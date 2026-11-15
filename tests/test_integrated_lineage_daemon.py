"""Integrated regression tests for lineage, OSC, and daemon interfaces."""
import socket
import struct
import tempfile
import unittest
import uuid
from pathlib import Path

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher, serialize_osc_bundle
from hermes_lineage import HermesLineageLogger, validate_lineage_graph
from marcus_orchestrator_daemon import MarcusOrchestratorDaemon


class ConcreteDispatcher:
    enabled = True
    def __init__(self, receipt=None):
        self.calls = []
        self.close_calls = 0
        self.receipt = receipt or {"status": "SENT", "receiver_acknowledged": False}
    def transmit_to_reaper(self, result):
        self.calls.append(result)
        return self.receipt
    def close(self):
        self.close_calls += 1


class CorrectedRuntimeAcceptanceTests(unittest.TestCase):
    def valid_feed(self, **overrides):
        data = {"parameter": "barometric_pressure", "value": 1013.25,
                "unit": "hPa", "missingness_state": "VALID"}
        data.update(overrides)
        return data

    def test_osc_bundle_binary_framing_and_types(self):
        data = serialize_osc_bundle([
            {"address": "/edmonia/synth/tet_index", "value": 10},
            {"address": "/edmonia/synth/tet_frequency", "value": 541.66},
        ])
        self.assertEqual(data[:8], b"#bundle\x00")
        self.assertEqual(struct.unpack(">II", data[8:16]), (0, 1))
        first_len = struct.unpack(">I", data[16:20])[0]
        self.assertIn(b",i\x00\x00", data[20:20 + first_len])
        second_offset = 20 + first_len
        second_len = struct.unpack(">I", data[second_offset:second_offset + 4])[0]
        self.assertIn(b",f\x00\x00", data[second_offset + 4:second_offset + 4 + second_len])

    def test_exact_input_and_output_digests_are_retained(self):
        logger = HermesLineageLogger()
        daemon = MarcusOrchestratorDaemon(telemetry_provider=self.valid_feed,
                                          source_kind="SIMULATED", lineage_logger=logger)
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["input_sha256_digest"], logger.digest_for_node(result["input_node_id"]))
        self.assertEqual(result["output_sha256_digest"], logger.digest_for_node(result["output_node_id"]))
        self.assertEqual(result["sha256_digest"], result["input_sha256_digest"])

    def test_measured_zero_is_preserved(self):
        daemon = MarcusOrchestratorDaemon(
            telemetry_provider=lambda: self.valid_feed(value=0.0, missingness_state="MEASURED_ZERO"),
            source_kind="SIMULATED")
        self.assertEqual(daemon.process_cycle()["status"], "PROCESSED")

    def test_invalid_payload_does_not_dispatch(self):
        dispatcher = ConcreteDispatcher()
        daemon = MarcusOrchestratorDaemon(
            telemetry_provider=lambda: self.valid_feed(value=float("nan")),
            source_kind="SIMULATED", dispatcher=dispatcher)
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "INVALID_MEASUREMENT")
        self.assertEqual(dispatcher.calls, [])

    def test_invalid_lineage_blocks_dispatch(self):
        class InvalidLogger(HermesLineageLogger):
            def validate(self):
                return ["deliberate structural validation failure"]
        dispatcher = ConcreteDispatcher()
        daemon = MarcusOrchestratorDaemon(telemetry_provider=self.valid_feed,
                                          source_kind="SIMULATED", dispatcher=dispatcher,
                                          lineage_logger=InvalidLogger())
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "LINEAGE_INVALID")
        self.assertEqual(result["delivery_status"], "BLOCKED_LINEAGE_INVALID")
        self.assertEqual(dispatcher.calls, [])

    def test_receipt_must_be_mapping_with_known_status(self):
        for receipt in (None, {"status": "ACKED", "receiver_acknowledged": True}):
            dispatcher = ConcreteDispatcher(receipt=receipt)
            daemon = MarcusOrchestratorDaemon(telemetry_provider=self.valid_feed,
                                              source_kind="SIMULATED", dispatcher=dispatcher)
            result = daemon.process_cycle()
            self.assertEqual(result["delivery_status"], "SEND_FAILED")
            self.assertFalse(result["receiver_confirmed"])

    def test_persistence_failure_blocks_dispatch_and_success_saves_receipt(self):
        dispatcher = ConcreteDispatcher()
        class BrokenLogger(HermesLineageLogger):
            def save_to_file(self, filepath):
                raise ValueError("invalid graph")
        with tempfile.TemporaryDirectory() as temp_dir:
            failing = MarcusOrchestratorDaemon(telemetry_provider=self.valid_feed,
                source_kind="SIMULATED", dispatcher=dispatcher, lineage_logger=BrokenLogger(),
                lineage_path=Path(temp_dir) / "lineage.json")
            result = failing.process_cycle()
            self.assertEqual(result["status"], "PERSISTENCE_FAILED")
            self.assertEqual(dispatcher.calls, [])

        dispatcher = ConcreteDispatcher()
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "new" / "nested" / "lineage.json"
            daemon = MarcusOrchestratorDaemon(telemetry_provider=self.valid_feed,
                source_kind="SIMULATED", dispatcher=dispatcher, lineage_path=target)
            result = daemon.process_cycle()
            self.assertEqual(result["lineage_persistence_status"], "SAVED_WITH_RECEIPT")
            self.assertTrue(target.is_file())

    def test_measured_missingness_and_units_are_preserved_or_quarantined(self):
        daemon = MarcusOrchestratorDaemon(
            telemetry_provider=lambda: self.valid_feed(value=0, missingness_state="UNAVAILABLE"),
            source_kind="SIMULATED")
        self.assertEqual(daemon.process_cycle()["status"], "QUARANTINED")
        daemon = MarcusOrchestratorDaemon(
            telemetry_provider=lambda: self.valid_feed(unit="kPa"), source_kind="SIMULATED")
        self.assertEqual(daemon.process_cycle()["status"], "QUARANTINED")

    def test_structural_status_does_not_claim_digest_or_source_authentication(self):
        logger = HermesLineageLogger()
        a, _ = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b, _ = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        logger.add_edge(a, b, "mapping")
        self.assertEqual(logger.validate(), [])
        graph = logger.export_graph()
        self.assertFalse(graph["nodes"][0]["jcs_canonicalized"])
        self.assertEqual(graph["edges"][0]["verification_status"], "UNVERIFIED")
        with self.assertRaises(ValueError):
            logger.add_edge(a, b, "mapping", status="CRYPTOGRAPHICALLY_VERIFIED")

    def test_graph_validation_handles_unhashable_ids_duplicates_dangling_and_cycles(self):
        logger = HermesLineageLogger()
        a, _ = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b, _ = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        base = logger.export_graph()
        malformed = {**base, "nodes": [dict(base["nodes"][0]), dict(base["nodes"][0])], "edges": []}
        self.assertTrue(any("duplicate node id" in e for e in validate_lineage_graph(malformed)))
        malformed = logger.export_graph()
        malformed["nodes"][0]["node_id"] = []
        errors = validate_lineage_graph(malformed)
        self.assertTrue(any("node_id" in e for e in errors))
        malformed = logger.export_graph()
        malformed["edges"] = [{"edge_id": str(uuid.uuid4()), "source_node_id": [],
                               "target_node_id": {}, "transformation_operation": "x"}]
        errors = validate_lineage_graph(malformed)
        self.assertTrue(any("dangling source" in e for e in errors))
        self.assertTrue(any("dangling target" in e for e in errors))
        logger.add_edge(a, b, "forward")
        graph = logger.export_graph()
        graph["edges"].append({"edge_id": str(uuid.uuid4()), "source_node_id": b,
                               "target_node_id": a, "transformation_operation": "back"})
        self.assertTrue(any("cycle" in e for e in validate_lineage_graph(graph)))
        logger.add_edge(a, a, "self") if False else None

    def test_digest_syntax_and_payload_serialization_errors(self):
        logger = HermesLineageLogger()
        node, _ = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        graph = logger.export_graph()
        graph["nodes"][0]["sha256_digest"] = "A" * 64
        self.assertEqual(validate_lineage_graph(graph), [])  # syntax valid; not recomputed
        graph["nodes"][0]["sha256_digest"] = "a" * 63
        self.assertTrue(any("sha256_digest" in e for e in validate_lineage_graph(graph)))
        graph["nodes"][0]["sha256_digest"] = "g" * 64
        self.assertTrue(any("sha256_digest" in e for e in validate_lineage_graph(graph)))
        for bad in ({"n": float("nan")}, {"n": float("inf")}, {"x": object()}):
            with self.assertRaises(ValueError):
                logger.add_node("IZORA", "TELEMETRY_ENVELOPE", bad)

    def test_persistence_rejects_invalid_graph(self):
        logger = HermesLineageLogger()
        logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        logger.nodes[0]["node_id"] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "invalid.json"
            with self.assertRaises(ValueError):
                logger.save_to_file(str(target))
            self.assertFalse(target.exists())

    def test_close_is_idempotent_in_daemon_and_dispatcher(self):
        dispatcher = ConcreteDispatcher()
        daemon = MarcusOrchestratorDaemon(dispatcher=dispatcher)
        daemon.stop_daemon()
        daemon.stop_daemon()
        self.assertEqual(dispatcher.close_calls, 1)
        real_dispatcher = EdmoniaOSCDispatcher()
        real_dispatcher.close()
        real_dispatcher.close()


if __name__ == "__main__":
    unittest.main()
