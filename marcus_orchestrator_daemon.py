"""Injected-dependency telemetry-to-OSC dispatch daemon prototype.

This is a one-way dispatch pipeline, not a closed loop. It has no default live
sensor provider and does not start when imported. Feed, clock, dispatcher,
lineage logger, and persistence path are injectable for deterministic testing.
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
    def __init__(self, feed_provider: Callable[[], Mapping[str, Any]] | None = None,
                 *, source_kind: str = "SIMULATED", poll_interval_sec: float = 2.0,
                 master_pitch_hz: float = 432.0, tet_divisions: int = 20,
                 dispatcher: EdmoniaOSCDispatcher | None = None,
                 lineage_logger: HermesLineageLogger | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 lineage_path: str | Path | None = None) -> None:
        if not math.isfinite(poll_interval_sec) or poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be finite and positive")
        if source_kind not in {"SIMULATED", "INJECTED", "REAL_UNVERIFIED"}:
            raise ValueError("source_kind must explicitly identify feed provenance")
        if not math.isfinite(master_pitch_hz) or master_pitch_hz <= 0:
            raise ValueError("master_pitch_hz must be finite and positive")
        if isinstance(tet_divisions, bool) or not isinstance(tet_divisions, int) or tet_divisions < 1:
            raise ValueError("tet_divisions must be a positive integer")
        self.feed_provider = feed_provider
        self.source_kind = source_kind if feed_provider is not None else "NO_FEED_CONFIGURED"
        self.poll_interval = poll_interval_sec
        self.master_pitch_hz = master_pitch_hz
        self.tet_divisions = tet_divisions
        self.dispatcher = dispatcher
        self.lineage_logger = lineage_logger or HermesLineageLogger()
        self.clock = clock
        self.lineage_path = Path(lineage_path) if lineage_path is not None else None
        self._stop_event = threading.Event()
        self._running = False

    def process_cycle(self) -> dict[str, Any]:
        """Fetch one injected record, map it, record lineage, optionally dispatch."""
        if self.feed_provider is None:
            return {"status": "NO_FEED", "source_kind": self.source_kind}
        try:
            raw = self.feed_provider()
        except Exception as exc:
            return {"status": "FEED_ERROR", "source_kind": self.source_kind,
                    "error": type(exc).__name__}
        if not isinstance(raw, Mapping):
            return {"status": "QUARANTINED", "reason": "feed provider must return a mapping"}
        missingness = raw.get("missingness_state", "UNKNOWN")
        if missingness not in {"VALID", "MEASURED_ZERO"}:
            return {"status": "QUARANTINED", "reason": f"missingness state is {missingness}"}
        if raw.get("parameter") != "barometric_pressure" or raw.get("unit") != "hPa":
            return {"status": "QUARANTINED", "reason": "only barometric_pressure in hPa is supported"}
        value = raw.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return {"status": "QUARANTINED", "reason": "value must be finite numeric telemetry"}
        if (missingness == "MEASURED_ZERO") != (value == 0):
            return {"status": "QUARANTINED", "reason": "value and missingness classification conflict"}

        input_record = {
            "parameter": "barometric_pressure", "value": value, "unit": "hPa",
            "missingness_state": missingness, "source_kind": self.source_kind,
            "source_reference": raw.get("source_reference"),
        }
        input_node_id = self.lineage_logger.add_node("IZORA", "TELEMETRY_ENVELOPE", input_record)
        input_digest = self.lineage_logger.digest_for_node(input_node_id)

        # Provisional engineering mapping; bounds, clamp, and Python round() are explicit.
        pressure_min, pressure_max = 870.0, 1085.0
        normalized = max(0.0, min(1.0, (float(value) - pressure_min) / (pressure_max - pressure_min)))
        tet_index = int(round(normalized * (self.tet_divisions - 1)))
        frequency = self.master_pitch_hz * (2.0 ** (tet_index / self.tet_divisions))
        mapping_parameters = {
            "pressure_range_hpa": [pressure_min, pressure_max],
            "out_of_range_policy": "clamp",
            "rounding_rule": "Python_round_ties_to_even",
            "master_pitch_hz": self.master_pitch_hz,
            "tet_divisions": self.tet_divisions,
            "formula": "f = master_pitch_hz * 2 ** (tet_index / tet_divisions)",
            "interpretation": "provisional_artistic_mapping_not_atmospheric_law",
        }
        output_payload = {"tet_index": tet_index, "frequency_hz": frequency,
                          "mapping_parameters": mapping_parameters}
        output_node_id = self.lineage_logger.add_node("EDMONIA", "SONIFICATION_STREAM", output_payload)
        self.lineage_logger.add_edge(input_node_id, output_node_id,
                                     "EDMONIA_PRESSURE_TO_20TET_MAPPING", mapping_parameters)
        result: dict[str, Any] = {
            "status": "PROCESSED", "source_kind": self.source_kind,
            "sha256_digest": input_digest,
            "digest_semantics": "sha256_of_deterministic_compact_sorted_python_json_not_rfc8785_jcs",
            "jcs_canonicalized": False,
            "input_node_id": input_node_id, "output_node_id": output_node_id,
            "tet_index": tet_index, "frequency_hz": frequency,
            "mapping_parameters": mapping_parameters,
            "osc_packets": [
                {"address": "/edmonia/synth/tet_index", "value": tet_index},
                {"address": "/edmonia/synth/tet_frequency", "value": frequency},
            ],
            "delivery_status": "NOT_SENT",
            "receiver_confirmed": False,
        }
        if self.dispatcher is not None and self.dispatcher.enabled:
            receipt = self.dispatcher.transmit_to_reaper(result)
            result["delivery_status"] = receipt["status"]
            result["receiver_confirmed"] = receipt["receiver_acknowledged"]
        if self.lineage_path is not None:
            try:
                self.lineage_logger.save_to_file(str(self.lineage_path))
            except OSError as exc:
                result["lineage_persistence_error"] = type(exc).__name__
        return result

    def start_daemon(self) -> None:
        """Run until stop_daemon; callers must opt in by invoking this method."""
        if self._running:
            raise RuntimeError("Daemon is already running")
        self._running = True
        self._stop_event.clear()
        try:
            while not self._stop_event.is_set():
                self.process_cycle()
                self._stop_event.wait(self.poll_interval)
        finally:
            self._running = False
            self.stop_daemon()

    def stop_daemon(self) -> None:
        self._stop_event.set()
        if self.dispatcher is not None:
            self.dispatcher.close()
