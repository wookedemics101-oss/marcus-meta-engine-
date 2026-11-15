import socket
import struct
import threading
import unittest

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher, serialize_osc_bundle
from hermes_lineage import HermesLineageLogger
from marcus_orchestrator import MarcusOrchestrator


class OSCDispatcherTests(unittest.TestCase):
    def test_local_loopback_send_uses_framed_bundle(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        port = receiver.getsockname()[1]
        got = []
        thread = threading.Thread(target=lambda: got.append(receiver.recvfrom(65535)[0]), daemon=True)
        thread.start()
        dispatcher = EdmoniaOSCDispatcher("127.0.0.1", port, enabled=True)
        receipt = dispatcher.transmit_to_reaper({
            "status": "PROCESSED", "sha256_digest": "a" * 64,
            "osc_packets": [
                {"address": "/edmonia/synth/tet_index", "value": 4},
                {"address": "/edmonia/synth/tet_frequency", "value": 500.0},
            ],
        })
        thread.join(1)
        receiver.close()
        self.assertEqual(receipt["message_count"], 2)
        self.assertFalse(receipt["receiver_acknowledged"])
        self.assertEqual(got[0][:8], b"#bundle\x00")
        self.assertEqual(struct.unpack(">II", got[0][8:16]), (0, 1))
        self.assertIn(b",i\x00\x00", got[0])
        self.assertIn(b",f\x00\x00", got[0])

    def test_dispatch_disabled_and_invalid_destinations(self):
        dispatcher = EdmoniaOSCDispatcher()
        with self.assertRaises(PermissionError):
            dispatcher.transmit_to_reaper({})
        with self.assertRaises(ValueError):
            EdmoniaOSCDispatcher("192.0.2.1")

    def test_bad_values_and_addresses_rejected(self):
        with self.assertRaises(ValueError):
            serialize_osc_bundle([{"address": "/bad", "value": 1}])
        with self.assertRaises(ValueError):
            serialize_osc_bundle([{"address": "/edmonia/synth/tet_frequency", "value": float("nan")}])


class OrchestratorTests(unittest.TestCase):
    def test_injected_feed_computes_result_without_network_by_default(self):
        result = MarcusOrchestrator().process_cycle_with_feed({
            "parameter": "barometric_pressure", "value": 1013.25,
            "unit": "hPa", "missingness_state": "VALID",
        })
        self.assertEqual(result["status"], "PROCESSED")
        self.assertIn("sha256_digest", result)
        self.assertFalse(result["jcs_canonicalized"])
        self.assertFalse(result["dispatch"]["sent"])
        self.assertEqual(result["lineage_edge_status"], "UNVERIFIED")

    def test_missing_data_and_wrong_units_are_quarantined(self):
        processor = MarcusOrchestrator()
        for state in ("UNAVAILABLE", "NON_DETECTION", "UNKNOWN"):
            result = processor.process_cycle_with_feed({
                "parameter": "barometric_pressure", "value": 1013,
                "unit": "hPa", "missingness_state": state,
            })
            self.assertEqual(result["status"], "QUARANTINED")
        self.assertEqual(processor.process_cycle_with_feed({
            "parameter": "barometric_pressure", "value": 1013,
            "unit": "kPa", "missingness_state": "VALID",
        })["status"], "QUARANTINED")

    def test_lineage_does_not_claim_verification_without_verifier(self):
        logger = HermesLineageLogger()
        a = logger.add_node("IZORA", "TELEMETRY_ENVELOPE", {"x": 1})
        b = logger.add_node("EDMONIA", "SONIFICATION_STREAM", {"y": 2})
        logger.add_edge(a, b, "mapping")
        graph = logger.export_graph()
        self.assertFalse(graph["nodes"][0]["jcs_canonicalized"])
        self.assertEqual(graph["edges"][0]["verification_status"], "UNVERIFIED")
        with self.assertRaises(ValueError):
            logger.add_edge(a, b, "mapping", status="CRYPTOGRAPHICALLY_VERIFIED")


if __name__ == "__main__":
    unittest.main()
