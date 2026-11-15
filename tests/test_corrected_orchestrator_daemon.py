import socket
import struct
import unittest
from datetime import datetime, timezone

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher, serialize_osc_bundle
from hermes_lineage import HermesLineageLogger, validate_lineage_graph
from marcus_orchestrator_daemon import MarcusOrchestratorDaemon


class FixedClock:
    def __call__(self):
        return datetime(2026, 11, 15, tzinfo=timezone.utc)


class MockDispatcher:
    enabled = True
    def __init__(self):
        self.calls = []
        self.closed = False
    def transmit_to_reaper(self, result):
        self.calls.append(result)
        return {"status": "SENT", "receiver_acknowledged": False}
    def close(self):
        self.closed = True


class CorrectedOrchestratorIntegrationTests(unittest.TestCase):
    def test_osc_bundle_has_bundle_header_lengths_and_typed_values(self):
        blob = serialize_osc_bundle([
            {"address": "/edmonia/synth/tet_index", "value": 5},
            {"address": "/edmonia/synth/tet_frequency", "value": 500.0},
        ])
        self.assertEqual(blob[:8], b"#bundle\x00")
        self.assertEqual(struct.unpack(">II", blob[8:16]), (0, 1))
        first_len = struct.unpack(">I", blob[16:20])[0]
        first = blob[20:20 + first_len]
        self.assertIn(b",i\x00\x00", first)
        second_offset = 20 + first_len
        second_len = struct.unpack(">I", blob[second_offset:second_offset + 4])[0]
        second = blob[second_offset + 4:second_offset + 4 + second_len]
        self.assertIn(b",f\x00\x00", second)

    def test_deterministic_injected_cycle_retains_exact_input_digest_and_source(self):
        feed = {"parameter": "barometric_pressure", "value": 1013.25,
                "unit": "hPa", "missingness_state": "VALID"}
        logger = HermesLineageLogger(clock=FixedClock())
        dispatcher = MockDispatcher()
        daemon = MarcusOrchestratorDaemon(lambda: feed, source_kind="SIMULATED",
                                          dispatcher=dispatcher, lineage_logger=logger,
                                          clock=lambda: 123.0)
        result = daemon.process_cycle()
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["source_kind"], "SIMULATED")
        self.assertEqual(result["sha256_digest"], logger.digest_for_node(result["input_node_id"]))
        self.assertEqual(result["delivery_status"], "SENT")
        self.assertFalse(result["receiver_confirmed"])
        self.assertEqual(len(dispatcher.calls), 1)
        self.assertEqual(logger.validate(), [])

    def test_no_feed_and_invalid_readings_do_not_dispatch(self):
        self.assertEqual(MarcusOrchestratorDaemon().process_cycle()["status"], "NO_FEED")
        cases = [
            {"parameter": "barometric_pressure", "value": float("nan"), "unit": "hPa", "missingness_state": "VALID"},
            {"parameter": "barometric_pressure", "value": 10.0, "unit": "kPa", "missingness_state": "VALID"},
            {"parameter": "barometric_pressure", "value": 0.0, "unit": "hPa", "missingness_state": "UNAVAILABLE"},
        ]
        dispatcher = MockDispatcher()
        daemon = MarcusOrchestratorDaemon(lambda: cases.pop(0), dispatcher=dispatcher)
        for _ in range(3):
            self.assertEqual(daemon.process_cycle()["status"], "QUARANTINED")
        self.assertEqual(dispatcher.calls, [])

    def test_loopback_udp_send_is_sent_not_receiver_confirmed(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        port = receiver.getsockname()[1]
        dispatcher = EdmoniaOSCDispatcher("127.0.0.1", port, enabled=True)
        receipt = dispatcher.transmit_to_reaper({
            "status": "PROCESSED", "sha256_digest": "b" * 64,
            "osc_packets": [{"address": "/edmonia/synth/tet_index", "value": 2}],
        })
        data, _ = receiver.recvfrom(65535)
        receiver.close()
        self.assertEqual(receipt["status"], "SENT")
        self.assertFalse(receipt["receiver_acknowledged"])
        self.assertTrue(data.startswith(b"#bundle\x00"))
        dispatcher.close()
        with self.assertRaises(RuntimeError):
            dispatcher.transmit_to_reaper({})

    def test_lineage_validator_detects_duplicate_dangling_bad_digest_and_cycle(self):
        logger = HermesLineageLogger(clock=FixedClock())
        a = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        logger.add_edge(a, b, "forward")
        graph = logger.export_graph()
        graph["edges"].append({"edge_id": graph["edges"][0]["edge_id"],
            "source_node_id": b, "target_node_id": a,
            "transformation_operation": "backward", "verification_status": "UNVERIFIED"})
        graph["nodes"][0]["sha256_digest"] = "bad"
        errors = validate_lineage_graph(graph)
        self.assertTrue(any("duplicate edge id" in e for e in errors))
        self.assertTrue(any("sha256_digest" in e for e in errors))
        self.assertTrue(any("cycle" in e for e in errors))
        graph["edges"][0]["source_node_id"] = "missing"
        self.assertTrue(any("dangling source" in e for e in validate_lineage_graph(graph)))

    def test_mapping_assumptions_are_explicit_and_unverified(self):
        result = MarcusOrchestratorDaemon(lambda: {
            "parameter": "barometric_pressure", "value": 1200.0,
            "unit": "hPa", "missingness_state": "VALID",
        }).process_cycle()
        self.assertEqual(result["mapping_parameters"]["out_of_range_policy"], "clamp")
        self.assertEqual(result["mapping_parameters"]["rounding_rule"], "Python_round_ties_to_even")
        self.assertFalse(result["jcs_canonicalized"])
        self.assertEqual(result["delivery_status"], "NOT_SENT")


if __name__ == "__main__":
    unittest.main()
