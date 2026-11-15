"""Integrated regression tests for lineage and telemetry-to-OSC flow."""
import socket
import struct
import tempfile
import unittest
import uuid
from pathlib import Path

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher, serialize_osc_bundle
from hermes_lineage import HermesLineageLogger, validate_lineage_graph, verify_lineage_digests
from marcus_orchestrator_daemon import MarcusOrchestratorDaemon


class MockDispatcher:
    enabled = True
    def __init__(self, receipt=None):
        self.calls = []
        self.close_calls = 0
        self.receipt = receipt if receipt is not None else {"status": "SENT", "receiver_acknowledged": False}
    def transmit_to_reaper(self, result):
        self.calls.append(result)
        return self.receipt
    def close(self):
        self.close_calls += 1


class IntegratedLineageDaemonTests(unittest.TestCase):
    def feed(self, **overrides):
        data = {"parameter": "barometric_pressure", "value": 1013.25,
                "unit": "hPa", "missingness_state": "VALID"}
        data.update(overrides)
        return data

    def test_hash_input_is_retained_and_recomputed(self):
        logger = HermesLineageLogger()
        node_id = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        self.assertTrue(logger.verify_node_digest(node_id))
        self.assertEqual(verify_lineage_digests(logger.export_graph()), [])
        graph = logger.export_graph()
        graph["nodes"][0]["hash_input"]["data"]["x"] = 2
        self.assertTrue(any("digest mismatch" in e for e in verify_lineage_digests(graph)))
        self.assertFalse(logger.nodes[0]["jcs_canonicalized"])

    def test_unverified_status_cannot_be_promoted_without_verifier(self):
        logger = HermesLineageLogger()
        a = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        with self.assertRaises(ValueError):
            logger.add_edge(a, b, "mapping", verification_status="CRYPTOGRAPHICALLY_VERIFIED")
        logger.add_edge(a, b, "mapping")
        self.assertEqual(logger.edges[0]["verification_status"], "UNVERIFIED")

    def test_structural_validator_rejects_duplicate_unhashable_dangling_and_cycle(self):
        logger = HermesLineageLogger()
        a = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        graph = logger.export_graph()
        duplicate = {**graph, "nodes": [dict(graph["nodes"][0]), dict(graph["nodes"][0])]}
        self.assertTrue(any("duplicate node id" in e for e in validate_lineage_graph(duplicate)))
        malformed = logger.export_graph()
        malformed["nodes"][0]["node_id"] = []
        self.assertTrue(any("node_id" in e for e in validate_lineage_graph(malformed)))
        dangling = logger.export_graph()
        dangling["edges"] = [{"edge_id": str(uuid.uuid4()), "source_node_id": [],
                               "target_node_id": {}, "transformation_operation": "bad"}]
        errors = validate_lineage_graph(dangling)
        self.assertTrue(any("dangling source" in e for e in errors))
        self.assertTrue(any("dangling target" in e for e in errors))
        logger.add_edge(a, b, "forward")
        cycle = logger.export_graph()
        cycle["edges"].append({"edge_id": str(uuid.uuid4()), "source_node_id": b,
                               "target_node_id": a, "transformation_operation": "back",
                               "verification_status": "UNVERIFIED"})
        self.assertTrue(any("cycle" in e for e in validate_lineage_graph(cycle)))

    def test_osc_bundle_has_standard_header_lengths_and_types(self):
        data = serialize_osc_bundle([
            {"address": "/edmonia/synth/tet_index", "value": 10},
            {"address": "/edmonia/synth/tet_frequency", "value": 541.66},
        ])
        self.assertEqual(data[:8], b"#bundle\x00")
        self.assertEqual(struct.unpack(">II", data[8:16]), (0, 1))
        length1 = struct.unpack(">I", data[16:20])[0]
        self.assertIn(b",i\x00\x00", data[20:20 + length1])
        offset = 20 + length1
        length2 = struct.unpack(">I", data[offset:offset + 4])[0]
        self.assertIn(b",f\x00\x00", data[offset + 4:offset + 4 + length2])

    def test_integrated_cycle_verifies_input_output_digests(self):
        logger, dispatcher = HermesLineageLogger(), MockDispatcher()
        daemon = MarcusOrchestratorDaemon(telemetry_provider=self.feed, source_kind="SIMULATED",
                                          lineage_logger=logger, dispatcher=dispatcher)
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["input_sha256_digest"], logger.digest_for_node(result["input_node_id"]))
        self.assertEqual(result["output_sha256_digest"], logger.digest_for_node(result["output_node_id"]))
        self.assertEqual(result["structural_status"], "STRUCTURALLY_VALID")
        self.assertEqual(result["payload_digest_status"], "PAYLOAD_DIGEST_MATCHED")
        self.assertFalse(result["source_authenticated"])
        self.assertFalse(result["receiver_confirmed"])
        self.assertEqual(len(dispatcher.calls), 1)

    def test_measured_zero_is_preserved_and_nonfinite_data_is_rejected(self):
        zero = MarcusOrchestratorDaemon(
            telemetry_provider=lambda: self.feed(value=0.0, missingness_state="MEASURED_ZERO"),
            source_kind="SIMULATED")
        self.assertEqual(zero.process_cycle()["status"], "PROCESSED")
        dispatcher = MockDispatcher()
        invalid = MarcusOrchestratorDaemon(telemetry_provider=lambda: self.feed(value=float("nan")),
                                           source_kind="SIMULATED", dispatcher=dispatcher)
        self.assertEqual(invalid.process_cycle()["status"], "INVALID_MEASUREMENT")
        self.assertEqual(dispatcher.calls, [])

    def test_invalid_lineage_blocks_dispatch(self):
        class InvalidLogger(HermesLineageLogger):
            def validate(self):
                return ["forced invalid graph"]
        dispatcher = MockDispatcher()
        result = MarcusOrchestratorDaemon(telemetry_provider=self.feed, source_kind="SIMULATED",
            dispatcher=dispatcher, lineage_logger=InvalidLogger()).process_cycle()
        self.assertEqual(result["status"], "LINEAGE_INVALID")
        self.assertEqual(dispatcher.calls, [])

    def test_persistence_failure_prevents_dispatch(self):
        class BrokenLogger(HermesLineageLogger):
            def save_to_file(self, path):
                raise ValueError("invalid archive")
        dispatcher = MockDispatcher()
        with tempfile.TemporaryDirectory() as temp_dir:
            daemon = MarcusOrchestratorDaemon(telemetry_provider=self.feed, source_kind="SIMULATED",
                dispatcher=dispatcher, lineage_logger=BrokenLogger(),
                lineage_path=Path(temp_dir) / "nested" / "lineage.json")
            result = daemon.process_cycle()
        self.assertEqual(result["status"], "PERSISTENCE_FAILED")
        self.assertEqual(dispatcher.calls, [])

    def test_bad_dispatch_receipt_is_not_trusted(self):
        for receipt in (None, {"status": "ACKED", "receiver_acknowledged": True}):
            dispatcher = MockDispatcher(receipt)
            result = MarcusOrchestratorDaemon(telemetry_provider=self.feed, source_kind="SIMULATED",
                dispatcher=dispatcher).process_cycle()
            self.assertEqual(result["delivery_status"], "SEND_FAILED")
            self.assertFalse(result["receiver_confirmed"])

    def test_loopback_udp_is_sent_but_not_acknowledged_and_close_is_idempotent(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        dispatcher = EdmoniaOSCDispatcher("127.0.0.1", receiver.getsockname()[1], enabled=True)
        receipt = dispatcher.transmit_to_reaper({"status": "PROCESSED", "sha256_digest": "c" * 64,
            "osc_packets": [{"address": "/edmonia/synth/tet_index", "value": 2}]})
        payload, _ = receiver.recvfrom(65535)
        receiver.close()
        self.assertTrue(payload.startswith(b"#bundle\x00"))
        self.assertEqual(receipt["status"], "SENT")
        self.assertFalse(receipt["receiver_acknowledged"])
        dispatcher.close()
        dispatcher.close()

    def test_invalid_graph_cannot_be_persisted(self):
        logger = HermesLineageLogger()
        node_id = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        logger.nodes[0]["hash_input"]["data"]["x"] = 99
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "graph.json"
            with self.assertRaises(ValueError):
                logger.save_to_file(str(target))
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
