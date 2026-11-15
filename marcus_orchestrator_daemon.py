"""One-way injected-feed telemetry-to-OSC prototype; not closed-loop control."""
from __future__ import annotations

import math
import threading
from pathlib import Path
from typing import Any, Callable, Mapping

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher
from hermes_lineage import HermesLineageLogger


class MarcusOrchestratorDaemon:
    def __init__(self, telemetry_provider: Callable[[], Mapping[str, Any]] | None = None,
                 *, source_kind: str = "SIMULATED", poll_interval_sec: float = 2.0,
                 reaper_port: int = 8000, master_pitch_hz: float = 432.0,
                 tet_divisions: int = 20, dispatcher: EdmoniaOSCDispatcher | None = None,
                 lineage_logger: HermesLineageLogger | None = None,
                 lineage_path: str | Path | None = None) -> None:
        if isinstance(poll_interval_sec, bool) or not isinstance(poll_interval_sec, (int, float)) or not math.isfinite(poll_interval_sec) or poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be finite and positive")
        if source_kind not in {"SIMULATED", "INJECTED", "REAL_UNVERIFIED"}:
            raise ValueError("source_kind must explicitly identify feed provenance")
        if isinstance(master_pitch_hz, bool) or not isinstance(master_pitch_hz, (int, float)) or not math.isfinite(master_pitch_hz) or master_pitch_hz <= 0:
            raise ValueError("master_pitch_hz must be finite and positive")
        if isinstance(tet_divisions, bool) or not isinstance(tet_divisions, int) or tet_divisions < 1:
            raise ValueError("tet_divisions must be a positive integer")
        self.telemetry_provider = telemetry_provider
        self.source_kind = source_kind if telemetry_provider is not None else "NO_FEED_CONFIGURED"
        self.poll_interval = float(poll_interval_sec)
        self.master_pitch_hz = float(master_pitch_hz)
        self.tet_divisions = tet_divisions
        self.dispatcher = dispatcher
        self.lineage_logger = lineage_logger or HermesLineageLogger()
        self.lineage_path = Path(lineage_path) if lineage_path is not None else None
        self.is_running = False
        self._stop_event = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._dispatcher_closed = False

    def _close_dispatcher_once(self) -> None:
        with self._lifecycle_lock:
            if self.dispatcher is not None and not self._dispatcher_closed:
                self.dispatcher.close()
                self._dispatcher_closed = True

    def _persist_graph(self) -> None:
        if self.lineage_path is not None:
            self.lineage_logger.save_to_file(str(self.lineage_path))

    def process_cycle(self) -> dict[str, Any]:
        if self.telemetry_provider is None:
            return {"status": "NO_FEED", "source_kind": self.source_kind}
        try:
            raw = self.telemetry_provider()
        except Exception as exc:
            return {"status": "FEED_ERROR", "source_kind": self.source_kind,
                    "error_type": type(exc).__name__}
        if not isinstance(raw, Mapping):
            return {"status": "QUARANTINED", "reason": "provider result must be a mapping"}
        missingness = raw.get("missingness_state", "UNKNOWN")
        if missingness not in {"VALID", "MEASURED_ZERO", "NON_DETECTION", "UNAVAILABLE", "UNKNOWN"}:
            return {"status": "QUARANTINED", "reason": "unrecognized missingness classification"}
        if missingness not in {"VALID", "MEASURED_ZERO"}:
            return {"status": "QUARANTINED", "reason": f"missingness state is {missingness}"}
        if raw.get("parameter") != "barometric_pressure" or raw.get("unit") != "hPa":
            return {"status": "QUARANTINED", "reason": "only barometric_pressure in hPa is supported"}
        value = raw.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return {"status": "INVALID_MEASUREMENT", "reason": "value must be numeric"}
        if not math.isfinite(value):
            return {"status": "INVALID_MEASUREMENT", "reason": "value must be finite"}
        if (missingness == "MEASURED_ZERO") != (value == 0):
            return {"status": "QUARANTINED", "reason": "value conflicts with missingness classification"}

        input_record = {
            "parameter": "barometric_pressure", "value": value, "unit": "hPa",
            "missingness_state": missingness, "source_kind": self.source_kind,
            "source_reference": raw.get("source_reference"),
            "measurement_status": "SIMULATED" if self.source_kind == "SIMULATED" else "UNVERIFIED_SOURCE",
        }
        input_node_id = self.lineage_logger.add_node("IZORA", "TELEMETRY_ENVELOPE", input_record)
        input_digest = self.lineage_logger.digest_for_node(input_node_id)
        if not self.lineage_logger.verify_node_digest(input_node_id):
            return {"status": "DIGEST_MISMATCH", "input_node_id": input_node_id,
                    "verification_status": "PAYLOAD_DIGEST_MISMATCH"}

        pressure_min, pressure_max = 870.0, 1085.0
        range_status = "CLAMPED_LOW" if value < pressure_min else "CLAMPED_HIGH" if value > pressure_max else "IN_RANGE"
        normalized = max(0.0, min(1.0, (float(value) - pressure_min) / (pressure_max - pressure_min)))
        tet_index = int(round(normalized * (self.tet_divisions - 1)))
        frequency = self.master_pitch_hz * (2.0 ** (tet_index / self.tet_divisions))
        if not math.isfinite(frequency):
            return {"status": "INVALID_MAPPING", "reason": "frequency is not finite"}
        mapping_parameters = {
            "pressure_range_hpa": [pressure_min, pressure_max],
            "out_of_range_policy": "clamp_and_report_range_status",
            "rounding_rule": "Python_round_ties_to_even",
            "master_pitch_hz": self.master_pitch_hz,
            "tet_divisions": self.tet_divisions,
            "frequency_formula": "master_pitch_hz * 2 ** (tet_index / tet_divisions)",
            "mapping_status": range_status,
            "interpretation": "provisional_artistic_mapping_not_atmospheric_law",
        }
        output_record = {"tet_index": tet_index, "frequency_hz": frequency,
                         "mapping_parameters": mapping_parameters,
                         "output_status": "COMPUTED_MAPPING_NOT_DSP_MEASUREMENT"}
        output_node_id = self.lineage_logger.add_node("EDMONIA", "SONIFICATION_STREAM", output_record)
        output_digest = self.lineage_logger.digest_for_node(output_node_id)
        self.lineage_logger.add_edge(input_node_id, output_node_id,
            "EDMONIA_PRESSURE_TO_20TET_MAPPING", mapping_parameters, status="UNVERIFIED")

        structural_errors = self.lineage_logger.validate()
        digest_errors = self.lineage_logger.verify_all_digests()
        errors = structural_errors + digest_errors
        result: dict[str, Any] = {
            "status": "PROCESSED", "source_kind": self.source_kind,
            "measurement_status": input_record["measurement_status"],
            "output_status": output_record["output_status"],
            "range_status": range_status,
            "input_node_id": input_node_id, "output_node_id": output_node_id,
            "input_sha256_digest": input_digest, "output_sha256_digest": output_digest,
            "sha256_digest": input_digest,
            "digest_semantics": "deterministic_compact_sorted_python_json_not_rfc8785_jcs",
            "jcs_canonicalized": False,
            "structural_status": "STRUCTURALLY_VALID" if not structural_errors else "STRUCTURALLY_INVALID",
            "payload_digest_status": "PAYLOAD_DIGEST_MATCHED" if not digest_errors else "PAYLOAD_DIGEST_MISMATCH",
            "source_authenticated": False,
            "mapping_parameters": mapping_parameters,
            "tet_index": tet_index, "frequency_hz": frequency,
            "osc_packets": [
                {"address": "/edmonia/synth/tet_index", "value": tet_index},
                {"address": "/edmonia/synth/tet_frequency", "value": float(frequency)},
            ],
            "lineage_validation_errors": errors,
            "delivery_status": "NOT_SENT", "receiver_confirmed": False,
            "lineage_persistence_status": "IN_MEMORY_ONLY" if self.lineage_path is None else "PENDING",
        }
        if errors:
            result.update(status="LINEAGE_INVALID", delivery_status="BLOCKED_LINEAGE_INVALID")
            return result

        # When configured, saving the valid graph is required before dispatch.
        if self.lineage_path is not None:
            try:
                self._persist_graph()
                result["lineage_persistence_status"] = "SAVED_BEFORE_DISPATCH"
            except (OSError, ValueError) as exc:
                result.update(status="PERSISTENCE_FAILED", delivery_status="BLOCKED_PERSISTENCE_FAILURE",
                              lineage_persistence_status="FAILED",
                              lineage_persistence_error_type=type(exc).__name__)
                return result

        if self.dispatcher is not None and getattr(self.dispatcher, "enabled", False):
            try:
                receipt = self.dispatcher.transmit_to_reaper(result)
                if not isinstance(receipt, Mapping):
                    raise RuntimeError("Dispatcher returned an invalid receipt")
                delivery = receipt.get("status")
                if delivery not in {"SENT", "SEND_FAILED"}:
                    raise RuntimeError("Dispatcher returned an unknown status")
                result["delivery_status"] = delivery
                result["receiver_confirmed"] = delivery == "SENT" and receipt.get("receiver_acknowledged") is True
                result["dispatch_receipt"] = dict(receipt)
            except (OSError, ValueError, RuntimeError) as exc:
                result["delivery_status"] = "SEND_FAILED"
                result["dispatch_error_type"] = type(exc).__name__

        if result["delivery_status"] in {"SENT", "SEND_FAILED"}:
            audit_record = {"delivery_status": result["delivery_status"],
                            "receiver_confirmed": result["receiver_confirmed"],
                            "dispatch_receipt": result.get("dispatch_receipt")}
            audit_id = self.lineage_logger.add_node("MARCUS", "AUDIT_LOG", audit_record)
            self.lineage_logger.add_edge(output_node_id, audit_id, "OSC_DISPATCH_RECEIPT",
                                         {"one_way_dispatch": True}, status="UNVERIFIED")
            result["lineage_validation_errors"] = self.lineage_logger.validate() + self.lineage_logger.verify_all_digests()

        if self.lineage_path is not None:
            try:
                self._persist_graph()
                result["lineage_persistence_status"] = "SAVED_WITH_RECEIPT"
            except (OSError, ValueError) as exc:
                result["lineage_persistence_status"] = "FAILED_AFTER_DISPATCH"
                result["lineage_persistence_error_type"] = type(exc).__name__
                result["status"] = "PERSISTENCE_FAILED_AFTER_DISPATCH"
        return result

    def start_daemon(self) -> None:
        if self.is_running:
            raise RuntimeError("daemon is already running")
        self.is_running = True
        self._stop_event.clear()
        try:
            while not self._stop_event.is_set():
                self.process_cycle()
                self._stop_event.wait(self.poll_interval)
        finally:
            self.is_running = False
            self._close_dispatcher_once()

    def stop_daemon(self) -> None:
        self._stop_event.set()
        self.is_running = False
        self._close_dispatcher_once()
