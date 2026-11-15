"""One-way, injected-feed telemetry-to-OSC dispatcher prototype.

This is not a closed loop: there is no feedback receiver or receiver
acknowledgement protocol. When a lineage_path is configured, a structurally
valid graph is persisted before dispatch; persistence failure blocks dispatch.
Without a path, in-memory-only operation is explicitly reported.
"""
from __future__ import annotations

import math
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher
from hermes_lineage import HermesLineageLogger


class MarcusOrchestratorDaemon:
    def __init__(
        self,
        telemetry_provider: Callable[[], Mapping[str, Any]] | None = None,
        *,
        source_kind: str = "SIMULATED",
        poll_interval_sec: float = 2.0,
        reaper_port: int = 8000,
        master_pitch_hz: float = 432.0,
        tet_divisions: int = 20,
        dispatcher: EdmoniaOSCDispatcher | None = None,
        lineage_logger: HermesLineageLogger | None = None,
        lineage_path: str | Path | None = None,
    ) -> None:
        if isinstance(poll_interval_sec, bool) or not isinstance(poll_interval_sec, (int, float)) or not math.isfinite(poll_interval_sec) or poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be finite and positive")
        if source_kind not in {"SIMULATED", "INJECTED", "REAL_UNVERIFIED"}:
            raise ValueError("source_kind must explicitly identify feed provenance")
        if not math.isfinite(master_pitch_hz) or master_pitch_hz <= 0:
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
        """Process one provider result; never synthesizes or fetches a live feed."""
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
        if missingness == "MEASURED_ZERO" and value != 0:
            return {"status": "QUARANTINED", "reason": "MEASURED_ZERO requires a zero value"}
        if missingness == "VALID" and value == 0:
            return {"status": "QUARANTINED", "reason": "zero must be explicitly classified as MEASURED_ZERO"}

        input_record = {
            "parameter": "barometric_pressure", "value": value, "unit": "hPa",
            "missingness_state": missingness, "source_kind": self.source_kind,
            "source_reference": raw.get("source_reference"),
        }
        input_node_id, input_digest = self.lineage_logger.add_node(
            "IZORA", "TELEMETRY_ENVELOPE", input_record
        )
        # Retrieve through the node lookup as an explicit consistency check while
        # retaining compatibility with the current tuple-returning add_node API.
        input_digest = self.lineage_logger.digest_for_node(input_node_id)

        pressure_min, pressure_max = 870.0, 1085.0
        if value < pressure_min:
            range_status = "CLAMPED_LOW"
        elif value > pressure_max:
            range_status = "CLAMPED_HIGH"
        else:
            range_status = "IN_RANGE"
        normalized = max(0.0, min(1.0, (float(value) - pressure_min) / (pressure_max - pressure_min)))
        tet_index = int(round(normalized * (self.tet_divisions - 1)))
        frequency = self.master_pitch_hz * (2.0 ** (tet_index / self.tet_divisions))
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
        output_payload = {"tet_index": tet_index, "frequency_hz": frequency,
                          "mapping_parameters": mapping_parameters}
        output_node_id, _ = self.lineage_logger.add_node(
            "EDMONIA", "SONIFICATION_STREAM", output_payload
        )
        output_digest = self.lineage_logger.digest_for_node(output_node_id)
        self.lineage_logger.add_edge(input_node_id, output_node_id,
                                     "EDMONIA_PRESSURE_TO_20TET_MAPPING",
                                     mapping_parameters, status="UNVERIFIED")

        result: dict[str, Any] = {
            "status": "PROCESSED", "source_kind": self.source_kind,
            "range_status": range_status,
            "input_sha256_digest": input_digest,
            "output_sha256_digest": output_digest,
            "sha256_digest": input_digest,
            "digest_semantics": "deterministic_compact_sorted_python_json_not_rfc8785_jcs",
            "jcs_canonicalized": False,
            "input_node_id": input_node_id, "output_node_id": output_node_id,
            "tet_index": tet_index, "frequency_hz": frequency,
            "mapping_parameters": mapping_parameters,
            "osc_packets": [
                {"address": "/edmonia/synth/tet_index", "value": tet_index},
                {"address": "/edmonia/synth/tet_frequency", "value": float(frequency)},
            ],
            "delivery_status": "NOT_SENT", "receiver_confirmed": False,
            "lineage_persistence_status": "NOT_CONFIGURED",
        }

        errors = self.lineage_logger.validate()
        result["lineage_validation_errors"] = errors
        if errors:
            result["status"] = "LINEAGE_INVALID"
            result["delivery_status"] = "BLOCKED_LINEAGE_INVALID"
            return result

        # Configured persistence is mandatory before dispatch. In-memory-only
        # operation is permitted only when no lineage_path was requested.
        if self.lineage_path is not None:
            try:
                self._persist_graph()
                result["lineage_persistence_status"] = "SAVED_BEFORE_DISPATCH"
            except (OSError, ValueError) as exc:
                result["status"] = "PERSISTENCE_FAILED"
                result["delivery_status"] = "BLOCKED_PERSISTENCE_FAILURE"
                result["lineage_persistence_status"] = "FAILED"
                result["lineage_persistence_error_type"] = type(exc).__name__
                return result

        if self.dispatcher is not None and getattr(self.dispatcher, "enabled", False):
            try:
                receipt = self.dispatcher.transmit_to_reaper(result)
                if not isinstance(receipt, Mapping):
                    raise RuntimeError("Dispatcher returned an invalid receipt")
                delivery_status = receipt.get("status")
                if delivery_status not in {"SENT", "SEND_FAILED"}:
                    raise RuntimeError("Dispatcher returned an unknown status")
                result["delivery_status"] = delivery_status
                result["receiver_confirmed"] = (
                    delivery_status == "SENT" and receipt.get("receiver_acknowledged") is True
                )
                result["dispatch_receipt"] = dict(receipt)
            except (OSError, ValueError, RuntimeError) as exc:
                result["delivery_status"] = "SEND_FAILED"
                result["dispatch_error_type"] = type(exc).__name__

        # Store the receipt as an audit node when possible, still UNVERIFIED as
        # to source identity and transport-level receiver processing.
        if result["delivery_status"] in {"SENT", "SEND_FAILED"}:
            audit_payload = {
                "delivery_status": result["delivery_status"],
                "receiver_confirmed": result["receiver_confirmed"],
                "dispatch_receipt": result.get("dispatch_receipt"),
            }
            audit_id, _ = self.lineage_logger.add_node("MARCUS", "AUDIT_LOG", audit_payload)
            self.lineage_logger.add_edge(output_node_id, audit_id, "OSC_DISPATCH_RECEIPT",
                                         {"one_way_dispatch": True}, status="UNVERIFIED")
            result["lineage_validation_errors"] = self.lineage_logger.validate()

        if self.lineage_path is not None:
            try:
                self._persist_graph()
                result["lineage_persistence_status"] = "SAVED_WITH_RECEIPT"
            except (OSError, ValueError) as exc:
                result["lineage_persistence_status"] = "FAILED_AFTER_DISPATCH"
                result["lineage_persistence_error_type"] = type(exc).__name__
                if result["status"] == "PROCESSED":
                    result["status"] = "PERSISTENCE_FAILED_AFTER_DISPATCH"
        return result

    def start_daemon(self) -> None:
        """Run the polling loop only when explicitly invoked."""
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
        """Request stop; dispatcher shutdown is performed once by the loop cleanup."""
        self.is_running = False
        self._stop_event.set()
        if not self._running_loop_active():
            self._close_dispatcher_once()

    def _running_loop_active(self) -> bool:
        return self.is_running
