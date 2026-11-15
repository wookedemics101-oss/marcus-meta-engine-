"""Injected-input, one-cycle MARCUS orchestration prototype.

No background daemon, sensor polling, or network transmission starts here. A
caller supplies telemetry and may separately inject an explicitly enabled,
loopback-only OSC dispatcher. Mappings are artistic conventions, not causal
scientific interpretations.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping

from edmonia_osc_dispatcher import EdmoniaOSCDispatcher
from hermes_lineage import HermesLineageLogger


class MarcusOrchestrator:
    def __init__(self, *, master_pitch_hz: float = 432.0, tet_divisions: int = 20,
                 dispatcher: EdmoniaOSCDispatcher | None = None,
                 lineage_logger: HermesLineageLogger | None = None) -> None:
        if not math.isfinite(master_pitch_hz) or master_pitch_hz <= 0:
            raise ValueError("master_pitch_hz must be finite and positive")
        if isinstance(tet_divisions, bool) or not isinstance(tet_divisions, int) or tet_divisions < 1:
            raise ValueError("tet_divisions must be a positive integer")
        self.master_pitch_hz = master_pitch_hz
        self.tet_divisions = tet_divisions
        self.dispatcher = dispatcher
        self.lineage_logger = lineage_logger or HermesLineageLogger()

    @staticmethod
    def _digest(payload: Mapping[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def process_cycle_with_feed(self, raw_data: Mapping[str, Any]) -> dict[str, Any]:
        """Process one caller-supplied measurement; never polls a live feed."""
        parameter, unit = raw_data.get("parameter"), raw_data.get("unit")
        missingness = raw_data.get("missingness_state", "UNKNOWN")
        if missingness not in {"VALID", "MEASURED_ZERO", "NON_DETECTION", "UNAVAILABLE", "UNKNOWN"}:
            return {"status": "QUARANTINED", "reason": "Unrecognized missingness classification"}
        if missingness not in {"VALID", "MEASURED_ZERO"}:
            return {"status": "QUARANTINED", "reason": f"Missingness state is {missingness}"}
        if parameter != "barometric_pressure" or unit != "hPa":
            return {"status": "QUARANTINED", "reason": "Only barometric_pressure in hPa is supported"}
        value = raw_data.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return {"status": "QUARANTINED", "reason": "Measurement must be a finite number"}
        if missingness == "MEASURED_ZERO" and value != 0:
            return {"status": "QUARANTINED", "reason": "MEASURED_ZERO requires value zero"}
        if missingness == "VALID" and value == 0:
            return {"status": "QUARANTINED", "reason": "Zero must be explicitly classified as MEASURED_ZERO"}

        input_record = {
            "parameter": parameter,
            "value": value,
            "unit": unit,
            "missingness_state": missingness,
            "source_reference": raw_data.get("source_reference"),
        }
        input_node = self.lineage_logger.add_node("IZORA", "TELEMETRY_ENVELOPE", input_record)
        norm = max(0.0, min(1.0, (float(value) - 870.0) / (1085.0 - 870.0)))
        index = int(round(norm * (self.tet_divisions - 1)))
        frequency = self.master_pitch_hz * (2.0 ** (index / self.tet_divisions))
        output_record = {
            "tet_index": index,
            "frequency_hz": frequency,
            "master_pitch_hz": self.master_pitch_hz,
            "tuning_system": f"{self.tet_divisions}-TET",
            "mapping_range_hpa": [870.0, 1085.0],
            "mapping_interpretation": "artistic_mapping_not_physical_causation",
        }
        output_node = self.lineage_logger.add_node("EDMONIA", "SONIFICATION_STREAM", output_record)
        self.lineage_logger.add_edge(
            input_node, output_node, "EDMONIA_PRESSURE_TO_20TET_MAPPING",
            {"master_pitch_hz": self.master_pitch_hz, "tet_divisions": self.tet_divisions,
             "input_unit": "hPa", "mapping_range_hpa": [870.0, 1085.0]},
            status="UNVERIFIED",
        )
        packets = [
            {"address": "/edmonia/synth/tet_index", "value": index},
            {"address": "/edmonia/synth/tet_frequency", "value": frequency},
        ]
        result: dict[str, Any] = {
            "status": "PROCESSED",
            "sha256_digest": self._digest(input_record),
            "digest_semantics": "sha256_of_sorted_compact_python_json_not_rfc8785_jcs",
            "jcs_canonicalized": False,
            "tet_index": index,
            "frequency_hz": frequency,
            "osc_packets": packets,
            "lineage_edge_status": "UNVERIFIED",
            "dispatch": {"sent": False, "reason": "no dispatcher configured"},
        }
        if self.dispatcher is not None and self.dispatcher.enabled:
            result["dispatch"] = self.dispatcher.transmit_to_reaper(result)
        return result
