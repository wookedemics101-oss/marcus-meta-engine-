"""In-memory H.E.R.M.E.S. lineage records with honest verification labels.

The hash encoding here is deterministic Python JSON, not RFC 8785 JCS. The
record therefore sets jcs_canonicalized=False. Hashes identify the encoded
payload bytes; they do not prove source truth or transformation correctness.
"""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

_SUBSYSTEMS = {"FRED", "OCTAVIA", "DELTA", "HERMES", "IZORA", "EDE", "MARCUS", "ARCHITECT", "EDMONIA"}
_ARTIFACTS = {"TELEMETRY_ENVELOPE", "STATE_TRANSITION_MATRIX", "SONIFICATION_STREAM", "AUDIT_LOG", "SCHEMA_DEFINITION"}


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                           allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("Payload must be finite, JSON-serializable data") from exc
    return text.encode("utf-8")


class HermesLineageLogger:
    """Create lineage graph records; no claim of cryptographic verification."""
    def __init__(self, graph_id: str | None = None) -> None:
        self.graph_id = graph_id or str(uuid.uuid4())
        uuid.UUID(self.graph_id)
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self._node_ids: set[str] = set()

    def add_node(self, subsystem: str, artifact_type: str, payload: Mapping[str, Any]) -> str:
        if subsystem not in _SUBSYSTEMS or artifact_type not in _ARTIFACTS:
            raise ValueError("Unknown subsystem or artifact type")
        encoded = _json_bytes(payload)
        node_id = str(uuid.uuid4())
        self.nodes.append({
            "node_id": node_id,
            "subsystem": subsystem,
            "artifact_type": artifact_type,
            "sha256_digest": hashlib.sha256(encoded).hexdigest(),
            "jcs_canonicalized": False,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
        self._node_ids.add(node_id)
        return node_id

    def add_edge(self, source_node_id: str, target_node_id: str, operation: str,
                 parameters: Mapping[str, Any] | None = None,
                 status: str = "UNVERIFIED") -> str:
        if source_node_id not in self._node_ids or target_node_id not in self._node_ids:
            raise ValueError("Both edge endpoints must refer to existing graph nodes")
        if source_node_id == target_node_id:
            raise ValueError("Self-referential lineage edges are not allowed")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if status not in {"UNVERIFIED", "ROUND_TRIP_PASSED", "CRYPTOGRAPHICALLY_VERIFIED", "FAILED"}:
            raise ValueError("Unknown verification status")
        # Do not let a caller label a transformation cryptographically verified
        # merely because source and output payloads have hashes.
        if status == "CRYPTOGRAPHICALLY_VERIFIED":
            raise ValueError("Use an external verifier before recording cryptographic verification")
        params = dict(parameters or {})
        _json_bytes(params)
        edge_id = str(uuid.uuid4())
        self.edges.append({
            "edge_id": edge_id,
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "transformation_operation": operation,
            "parameters": params,
            "verification_status": status,
        })
        return edge_id

    def export_graph(self) -> dict[str, Any]:
        return {
            "graph_metadata": {
                "graph_id": self.graph_id,
                "version": "1.0.0",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "coordinator": "HERMES",
            },
            "nodes": list(self.nodes),
            "edges": list(self.edges),
        }

    def save_to_file(self, filepath: str) -> None:
        """Write the graph to an explicit path; callers own path/permissions."""
        with open(filepath, "wb") as stream:
            stream.write(_json_bytes(self.export_graph()))
