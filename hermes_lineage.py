"""H.E.R.M.E.S. lineage records and structural graph validation.

Hashes cover deterministic compact Python JSON serialization. This is not an
RFC 8785 JCS implementation, so nodes correctly declare jcs_canonicalized=False.
A hash does not establish source truth or transformation correctness.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

_SUBSYSTEMS = {"FRED", "OCTAVIA", "DELTA", "HERMES", "IZORA", "EDE", "MARCUS", "ARCHITECT", "EDMONIA"}
_ARTIFACTS = {"TELEMETRY_ENVELOPE", "STATE_TRANSITION_MATRIX", "SONIFICATION_STREAM", "AUDIT_LOG", "SCHEMA_DEFINITION"}
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("Payload must be finite, JSON-serializable data") from exc
    return text.encode("utf-8")


def validate_lineage_graph(graph: Mapping[str, Any]) -> list[str]:
    """Return structural errors for duplicate IDs, references, hashes, and cycles."""
    errors: list[str] = []
    try:
        nodes = graph["nodes"]
        edges = graph["edges"]
        if not isinstance(nodes, list) or not isinstance(edges, list):
            return ["nodes and edges must be arrays"]
    except (KeyError, TypeError):
        return ["graph must contain nodes and edges arrays"]

    node_ids: set[str] = set()
    for i, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            errors.append(f"nodes[{i}] must be an object")
            continue
        node_id = node.get("node_id")
        try:
            uuid.UUID(str(node_id))
        except (ValueError, TypeError, AttributeError):
            errors.append(f"nodes[{i}].node_id is not a UUID")
        if node_id in node_ids:
            errors.append(f"duplicate node id: {node_id}")
        node_ids.add(node_id)
        digest = node.get("sha256_digest")
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            errors.append(f"nodes[{i}].sha256_digest is invalid")

    edge_ids: set[str] = set()
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for i, edge in enumerate(edges):
        if not isinstance(edge, Mapping):
            errors.append(f"edges[{i}] must be an object")
            continue
        edge_id = edge.get("edge_id")
        try:
            uuid.UUID(str(edge_id))
        except (ValueError, TypeError, AttributeError):
            errors.append(f"edges[{i}].edge_id is not a UUID")
        if edge_id in edge_ids:
            errors.append(f"duplicate edge id: {edge_id}")
        edge_ids.add(edge_id)
        source, target = edge.get("source_node_id"), edge.get("target_node_id")
        if source not in node_ids:
            errors.append(f"edges[{i}] has dangling source reference")
        if target not in node_ids:
            errors.append(f"edges[{i}] has dangling target reference")
        if source in node_ids and target in node_ids:
            adjacency[source].append(target)

    # Three-color DFS detects directed cycles, including self-cycles.
    color: dict[str, int] = {node_id: 0 for node_id in node_ids}
    cycle_found = False
    def visit(node_id: str) -> None:
        nonlocal cycle_found
        color[node_id] = 1
        for target in adjacency.get(node_id, []):
            if color[target] == 1:
                cycle_found = True
            elif color[target] == 0:
                visit(target)
        color[node_id] = 2
    for node_id in node_ids:
        if color[node_id] == 0:
            visit(node_id)
    if cycle_found:
        errors.append("lineage graph contains a directed cycle")
    return errors


class HermesLineageLogger:
    def __init__(self, graph_id: str | None = None, clock=None) -> None:
        self.graph_id = graph_id or str(uuid.uuid4())
        uuid.UUID(self.graph_id)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self._node_ids: set[str] = set()
        self._payload_digests: dict[str, str] = {}

    def add_node(self, subsystem: str, artifact_type: str, payload: Mapping[str, Any]) -> str:
        if subsystem not in _SUBSYSTEMS or artifact_type not in _ARTIFACTS:
            raise ValueError("Unknown subsystem or artifact type")
        digest = hashlib.sha256(_json_bytes(payload)).hexdigest()
        node_id = str(uuid.uuid4())
        timestamp = self._clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        self.nodes.append({
            "node_id": node_id, "subsystem": subsystem, "artifact_type": artifact_type,
            "sha256_digest": digest, "jcs_canonicalized": False, "timestamp": timestamp,
        })
        self._node_ids.add(node_id)
        self._payload_digests[node_id] = digest
        return node_id

    def digest_for_node(self, node_id: str) -> str:
        """Return the exact payload digest retained for this node ID."""
        try:
            return self._payload_digests[node_id]
        except KeyError as exc:
            raise ValueError("Unknown lineage node") from exc

    def add_edge(self, source_node_id: str, target_node_id: str, operation: str,
                 parameters: Mapping[str, Any] | None = None,
                 status: str = "UNVERIFIED") -> str:
        if source_node_id not in self._node_ids or target_node_id not in self._node_ids:
            raise ValueError("Both edge endpoints must refer to existing graph nodes")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if status not in {"UNVERIFIED", "ROUND_TRIP_PASSED", "CRYPTOGRAPHICALLY_VERIFIED", "FAILED"}:
            raise ValueError("Unknown verification status")
        if status == "CRYPTOGRAPHICALLY_VERIFIED":
            raise ValueError("An external verifier is required to record cryptographic verification")
        params = dict(parameters or {})
        _json_bytes(params)
        edge_id = str(uuid.uuid4())
        self.edges.append({
            "edge_id": edge_id, "source_node_id": source_node_id, "target_node_id": target_node_id,
            "transformation_operation": operation, "parameters": params,
            "verification_status": status,
        })
        return edge_id

    def export_graph(self) -> dict[str, Any]:
        return {
            "graph_metadata": {
                "graph_id": self.graph_id, "version": "1.0.0",
                "timestamp": self._clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "coordinator": "HERMES",
            },
            "nodes": [dict(node) for node in self.nodes],
            "edges": [dict(edge) for edge in self.edges],
        }

    def validate(self) -> list[str]:
        return validate_lineage_graph(self.export_graph())

    def save_to_file(self, filepath: str) -> None:
        graph = self.export_graph()
        errors = validate_lineage_graph(graph)
        if errors:
            raise ValueError("Invalid lineage graph: " + "; ".join(errors))
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_json_bytes(graph))
