"""Structural lineage graph validation and deterministic hash recording.

Hashes use compact, sorted Python JSON serialization, not RFC 8785 JCS. A hash
alone does not prove source truth or transformation correctness.
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
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("payload must be finite JSON-serializable data") from exc


def validate_lineage_graph(graph: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    nodes, edges = graph.get("nodes"), graph.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return ["nodes and edges must be arrays"]

    node_ids: set[str] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            errors.append(f"nodes[{index}] must be an object")
            continue
        node_id = node.get("node_id")
        if not isinstance(node_id, str):
            errors.append(f"nodes[{index}].node_id must be a string UUID")
        else:
            try:
                uuid.UUID(node_id)
            except (ValueError, AttributeError):
                errors.append(f"nodes[{index}].node_id is invalid")
            if node_id in node_ids:
                errors.append(f"duplicate node id: {node_id}")
            node_ids.add(node_id)
        digest = node.get("sha256_digest")
        if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
            errors.append(f"nodes[{index}].sha256_digest is invalid")

    edge_ids: set[str] = set()
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for index, edge in enumerate(edges):
        if not isinstance(edge, Mapping):
            errors.append(f"edges[{index}] must be an object")
            continue
        edge_id = edge.get("edge_id")
        if not isinstance(edge_id, str):
            errors.append(f"edges[{index}].edge_id must be a string UUID")
        else:
            try:
                uuid.UUID(edge_id)
            except (ValueError, AttributeError):
                errors.append(f"edges[{index}].edge_id is invalid")
            if edge_id in edge_ids:
                errors.append(f"duplicate edge id: {edge_id}")
            edge_ids.add(edge_id)
        source, target = edge.get("source_node_id"), edge.get("target_node_id")
        source_ok = isinstance(source, str) and source in node_ids
        target_ok = isinstance(target, str) and target in node_ids
        if not source_ok:
            errors.append(f"edges[{index}] has dangling source reference")
        if not target_ok:
            errors.append(f"edges[{index}] has dangling target reference")
        if source_ok and target_ok:
            adjacency[source].append(target)

    # Three-color depth-first traversal detects cycles in the directed graph.
    colors = {node_id: 0 for node_id in node_ids}
    cyclic = False
    def visit(node_id: str) -> None:
        nonlocal cyclic
        colors[node_id] = 1
        for target in adjacency[node_id]:
            if colors[target] == 1:
                cyclic = True
            elif colors[target] == 0:
                visit(target)
        colors[node_id] = 2
    for node_id in node_ids:
        if colors[node_id] == 0:
            visit(node_id)
    if cyclic:
        errors.append("lineage graph contains a directed cycle")
    return errors


class HermesLineageLogger:
    def __init__(self, graph_id: str | None = None, clock=None) -> None:
        self.graph_id = graph_id or str(uuid.uuid4())
        uuid.UUID(self.graph_id)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self._payload_digests: dict[str, str] = {}

    def add_node(self, subsystem: str, artifact_type: str, payload: Mapping[str, Any]) -> tuple[str, str]:
        if subsystem not in _SUBSYSTEMS or artifact_type not in _ARTIFACTS:
            raise ValueError("unknown subsystem or artifact type")
        digest = hashlib.sha256(_json_bytes(payload)).hexdigest()
        node_id = str(uuid.uuid4())
        timestamp = self._clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        self.nodes.append({
            "node_id": node_id, "subsystem": subsystem, "artifact_type": artifact_type,
            "sha256_digest": digest, "jcs_canonicalized": False, "timestamp": timestamp,
        })
        self._payload_digests[node_id] = digest
        return node_id, digest

    def digest_for_node(self, node_id: str) -> str:
        try:
            return self._payload_digests[node_id]
        except KeyError as exc:
            raise ValueError("unknown lineage node") from exc

    def add_edge(self, source_node_id: str, target_node_id: str, operation: str,
                 parameters: Mapping[str, Any] | None = None, status: str = "UNVERIFIED") -> str:
        node_ids = {node["node_id"] for node in self.nodes}
        if source_node_id not in node_ids or target_node_id not in node_ids:
            raise ValueError("both edge endpoints must refer to existing nodes")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if status not in {"UNVERIFIED", "ROUND_TRIP_PASSED", "CRYPTOGRAPHICALLY_VERIFIED", "FAILED"}:
            raise ValueError("unknown verification status")
        if status == "CRYPTOGRAPHICALLY_VERIFIED":
            raise ValueError("external verification evidence is required for this status")
        params = dict(parameters or {})
        _json_bytes(params)
        edge_id = str(uuid.uuid4())
        self.edges.append({
            "edge_id": edge_id, "source_node_id": source_node_id,
            "target_node_id": target_node_id, "transformation_operation": operation,
            "parameters": params, "verification_status": status,
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
            raise ValueError("invalid lineage graph: " + "; ".join(errors))
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_json_bytes(graph))
