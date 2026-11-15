import socket
import struct
import tempfile
import unittest
from pathlib import Path

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher, serialize_osc_bundle
from hermes_lineage import HermesLineageLogger, validate_lineage_graph
from marcus_orchestrator_daemon import MarcusOrchestratorDaemon


class MockDispatcher:
    enabled = True
    def __init__(self):
        self.sent = []
        self.closed = False
    def transmit_to_reaper(self, result):
        self.sent.append(result)
        return {"status": "SENT", "receiver_acknowledged": False}
    def close(self):
        self.closed = True


class CorrectedOrchestratorTests(unittest.TestCase):
    def test_bundle_binary_framing_and_type_tags(self):
        data = serialize_osc_bundle([
            {"address": "/edmonia/synth/tet_index", "value": 10},
            {"address": "/edmonia/synth/tet_frequency", "value": 541.66},
        ])
        self.assertEqual(data[:8], b"#bundle\x00")
        self.assertEqual(struct.unpack(">II", data[8:16]), (0, 1))
        length1 = struct.unpack(">I", data[16:20])[0]
        msg1 = data[20:20 + length1]
        self.assertIn(b",i\x00\x00", msg1)
        pos2 = 20 + length1
        length2 = struct.unpack(">I", data[pos2:pos2 + 4])[0]
        msg2 = data[pos2 + 4:pos2 + 4 + length2]
        self.assertIn(b",f\x00\x00", msg2)

    def test_injected_cycle_retains_exact_input_digest_and_does_not_claim_ack(self):
        logger = HermesLineageLogger()
        dispatcher = MockDispatcher()
        daemon = MarcusOrchestratorDaemon(
            lambda: {"parameter": "barometric_pressure", "value": 1013.25,
                     "unit": "hPa", "missingness_state": "VALID"},
            source_kind="SIMULATED", dispatcher=dispatcher, lineage_logger=logger,
        )
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["source_kind"], "SIMULATED")
        self.assertEqual(result["sha256_digest"], logger.digest_for_node(result["input_node_id"]))
        self.assertFalse(result["receiver_confirmed"])
        self.assertEqual(result["delivery_status"], "SENT")
        self.assertFalse(result["jcs_canonicalized"])
        self.assertEqual(result["lineage_validation_errors"], [])

    def test_no_default_feed_and_invalid_data_classification(self):
        self.assertEqual(MarcusOrchestratorDaemon().process_cycle()["status"], "NO_FEED")
        readings = [
            {"parameter": "barometric_pressure", "value": float("nan"), "unit": "hPa", "missingness_state": "VALID"},
            {"parameter": "barometric_pressure", "value": 1013, "unit": "kPa", "missingness_state": "VALID"},
            {"parameter": "barometric_pressure", "value": 0, "unit": "hPa", "missingness_state": "UNAVAILABLE"},
        ]
        daemon = MarcusOrchestratorDaemon(lambda: readings.pop(0), source_kind="INJECTED")
        self.assertEqual(daemon.process_cycle()["status"], "INVALID_MEASUREMENT")
        self.assertEqual(daemon.process_cycle()["status"], "QUARANTINED")
        self.assertEqual(daemon.process_cycle()["status"], "QUARANTINED")

    def test_range_policy_is_explicit(self):
        daemon = MarcusOrchestratorDaemon(
            lambda: {"parameter": "barometric_pressure", "value": 1200,
                     "unit": "hPa", "missingness_state": "VALID"},
            source_kind="SIMULATED",
        )
        result = daemon.process_cycle()
        self.assertEqual(result["range_status"], "CLAMPED_HIGH")
        self.assertEqual(result["mapping_parameters"]["out_of_range_policy"], "clamp_and_report_range_status")
        self.assertEqual(result["delivery_status"], "NOT_SENT")

    def test_loopback_udp_reports_sent_not_acknowledged(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        port = receiver.getsockname()[1]
        dispatcher = EdmoniaOSCDispatcher("127.0.0.1", port, enabled=True)
        receipt = dispatcher.transmit_to_reaper({
            "status": "PROCESSED", "sha256_digest": "c" * 64,
            "osc_packets": [{"address": "/edmonia/synth/tet_index", "value": 3}],
        })
        datagram, _ = receiver.recvfrom(65535)
        receiver.close()
        self.assertEqual(receipt["status"], "SENT")
        self.assertFalse(receipt["receiver_acknowledged"])
        self.assertTrue(datagram.startswith(b"#bundle\x00"))
        dispatcher.close()

    def test_lineage_validation_catches_duplicates_dangling_bad_digest_and_cycle(self):
        logger = HermesLineageLogger()
        a, _ = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b, _ = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        logger.add_edge(a, b, "forward")
        graph = logger.export_graph()
        graph["edges"].append({
            "edge_id": graph["edges"][0]["edge_id"], "source_node_id": b,
            "target_node_id": a, "transformation_operation": "backward",
            "verification_status": "UNVERIFIED",
        })
        graph["nodes"][0]["sha256_digest"] = "invalid"
        errors = validate_lineage_graph(graph)
        self.assertTrue(any("duplicate edge id" in item for item in errors))
        self.assertTrue(any("sha256_digest" in item for item in errors))
        self.assertTrue(any("cycle" in item for item in errors))
        graph["edges"][0]["source_node_id"] = "missing"
        self.assertTrue(any("dangling source" in item for item in validate_lineage_graph(graph)))

    def test_lineage_persistence_creates_parent_directories(self):
        logger = HermesLineageLogger()
        logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "new" / "nested" / "lineage.json"
            logger.save_to_file(str(target))
            self.assertTrue(target.is_file())


if __name__ == "__main__":
    unittest.main()
