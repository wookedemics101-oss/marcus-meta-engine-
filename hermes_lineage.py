"""H.E.R.M.E.S. lineage records, structural validation, and digest checks.

Digests cover retained hash_input records serialized as compact, sorted Python
JSON. This is deterministic for the supported Python data, but is NOT RFC 8785
JCS. A matching digest verifies integrity relative to that retained input; it
does not authenticate the source or establish measurement truth or correctness.
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
_EDGE_STATES = {"UNVERIFIED", "ROUND_TRIP_PASSED", "FAILED"}
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def deterministic_json_bytes(payload: Any) -> bytes:
    """Serialize finite JSON data deterministically; this is not RFC 8785 JCS."""
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("value must be finite JSON-serializable data") from exc


def _valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def validate_lineage_graph(graph: Mapping[str, Any]) -> list[str]:
    """Check IDs, duplicates, digest syntax, references, and cycles safely."""
    errors: list[str] = []
    if not isinstance(graph, Mapping):
        return ["graph must be an object"]
    nodes, edges = graph.get("nodes"), graph.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return ["nodes and edges must be arrays"]

    node_ids: set[str] = set()
    for i, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            errors.append(f"nodes[{i}] must be an object")
            continue
        node_id = node.get("node_id")
        if not _valid_uuid(node_id):
            errors.append(f"nodes[{i}].node_id must be a valid string UUID")
        else:
            if node_id in node_ids:
                errors.append(f"duplicate node id: {node_id}")
            node_ids.add(node_id)
        digest = node.get("sha256_digest")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            errors.append(f"nodes[{i}].sha256_digest must be a 64-character hexadecimal string")
        if node.get("jcs_canonicalized") is not False:
            errors.append(f"nodes[{i}].jcs_canonicalized must be false for deterministic Python JSON")

    edge_ids: set[str] = set()
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for i, edge in enumerate(edges):
        if not isinstance(edge, Mapping):
            errors.append(f"edges[{i}] must be an object")
            continue
        edge_id = edge.get("edge_id")
        if not _valid_uuid(edge_id):
            errors.append(f"edges[{i}].edge_id must be a valid string UUID")
        else:
            if edge_id in edge_ids:
                errors.append(f"duplicate edge id: {edge_id}")
            edge_ids.add(edge_id)
        source, target = edge.get("source_node_id"), edge.get("target_node_id")
        source_ok = isinstance(source, str) and source in node_ids
        target_ok = isinstance(target, str) and target in node_ids
        if not source_ok:
            errors.append(f"edges[{i}] has dangling source reference")
        if not target_ok:
            errors.append(f"edges[{i}] has dangling target reference")
        if source_ok and target_ok:
            adjacency[source].append(target)
        if edge.get("verification_status") == "CRYPTOGRAPHICALLY_VERIFIED":
            errors.append(f"edges[{i}] cannot assert cryptographic verification without independent evidence")

    # Iterative three-color DFS avoids Python recursion limits on deep graphs.
    colors = {node_id: 0 for node_id in node_ids}
    has_cycle = False
    for root in node_ids:
        if colors[root] != 0:
            continue
        colors[root] = 1
        stack: list[tuple[str, Any]] = [(root, iter(adjacency[root]))]
        while stack:
            current, children = stack[-1]
            try:
                child = next(children)
            except StopIteration:
                colors[current] = 2
                stack.pop()
                continue
            if colors[child] == 1:
                has_cycle = True
            elif colors[child] == 0:
                colors[child] = 1
                stack.append((child, iter(adjacency[child])))
    if has_cycle:
        errors.append("lineage graph contains a directed cycle")
    return errors


def verify_lineage_digests(graph: Mapping[str, Any]) -> list[str]:
    """Recompute digests from retained hash_input; no source authentication implied."""
    errors: list[str] = []
    nodes = graph.get("nodes") if isinstance(graph, Mapping) else None
    if not isinstance(nodes, list):
        return ["nodes must be an array"]
    for i, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            errors.append(f"nodes[{i}] is not an object")
            continue
        hash_input = node.get("hash_input")
        digest = node.get("sha256_digest")
        if not isinstance(hash_input, Mapping):
            errors.append(f"nodes[{i}] is missing retained hash_input")
            continue
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            errors.append(f"nodes[{i}] has malformed digest")
            continue
        try:
            actual = hashlib.sha256(deterministic_json_bytes(hash_input)).hexdigest()
        except ValueError as exc:
            errors.append(f"nodes[{i}] hash_input cannot be serialized: {exc}")
            continue
        if actual != digest.lower():
            errors.append(f"nodes[{i}] payload digest mismatch")
    return errors


class HermesLineageLogger:
    """Create auditable lineage records without overstating verification."""
    def __init__(self, graph_id: str | None = None, clock=None) -> None:
        self.graph_id = graph_id or str(uuid.uuid4())
        if not _valid_uuid(self.graph_id):
            raise ValueError("graph_id must be a valid UUID")
        self.version = "1.0.0"
        self.coordinator = "HERMES"
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self._node_ids: set[str] = set()
        self._payload_digests: dict[str, str] = {}

    def _timestamp(self) -> str:
        return self._clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def add_node(self, subsystem: str, artifact_type: str, data: Mapping[str, Any]) -> str:
        """Add node and return its UUID; use digest_for_node() for its SHA-256."""
        if subsystem not in _SUBSYSTEMS or artifact_type not in _ARTIFACTS:
            raise ValueError("unknown subsystem or artifact type")
        if not isinstance(data, Mapping):
            raise ValueError("node data must be an object")
        node_id = str(uuid.uuid4())
        timestamp = self._timestamp()
        hash_input = {
            "node_id": node_id,
            "subsystem": subsystem,
            "artifact_type": artifact_type,
            "timestamp": timestamp,
            "data": dict(data),
        }
        digest = hashlib.sha256(deterministic_json_bytes(hash_input)).hexdigest()
        node_record = {
            "node_id": node_id,
            "subsystem": subsystem,
            "artifact_type": artifact_type,
            "sha256_digest": digest,
            "jcs_canonicalized": False,
            "timestamp": timestamp,
            "hash_input": hash_input,
        }
        deterministic_json_bytes(node_record)
        self.nodes.append(node_record)
        self._node_ids.add(node_id)
        self._payload_digests[node_id] = digest
        return node_id

    def digest_for_node(self, node_id: str) -> str:
        if not isinstance(node_id, str):
            raise ValueError("node ID must be a string")
        try:
            return self._payload_digests[node_id]
        except KeyError as exc:
            raise ValueError("unknown lineage node") from exc

    def verify_node_digest(self, node_id: str) -> bool:
        if not isinstance(node_id, str):
            return False
        for node in self.nodes:
            if node.get("node_id") == node_id:
                try:
                    actual = hashlib.sha256(deterministic_json_bytes(node["hash_input"])).hexdigest()
                except (KeyError, ValueError, TypeError):
                    return False
                return actual == node.get("sha256_digest") == self._payload_digests.get(node_id)
        return False

    def verify_all_digests(self) -> list[str]:
        return verify_lineage_digests(self.export_graph())

    def add_edge(self, source_node_id: str, target_node_id: str, operation: str,
                 parameters: Mapping[str, Any] | None = None, status: str = "UNVERIFIED",
                 **kwargs: Any) -> str:
        # Accept the previous named parameter spelling for compatibility.
        if "transformation_operation" in kwargs:
            operation = kwargs.pop("transformation_operation")
        if "verification_status" in kwargs:
            status = kwargs.pop("verification_status")
        if kwargs:
            raise TypeError(f"unexpected arguments: {', '.join(kwargs)}")
        if not isinstance(source_node_id, str) or not isinstance(target_node_id, str):
            raise ValueError("edge endpoint identifiers must be strings")
        if source_node_id not in self._node_ids or target_node_id not in self._node_ids:
            raise ValueError("Source or target node ID does not exist in the active lineage graph.")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if status == "CRYPTOGRAPHICALLY_VERIFIED":
            raise ValueError("independent cryptographic verification evidence is required")
        if status not in _EDGE_STATES:
            raise ValueError("unknown verification status")
        edge_parameters = dict(parameters or {})
        deterministic_json_bytes(edge_parameters)
        edge_id = str(uuid.uuid4())
        self.edges.append({
            "edge_id": edge_id,
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "transformation_operation": operation,
            "parameters": edge_parameters,
            "verification_status": status,
        })
        return edge_id

    def export_graph(self) -> dict[str, Any]:
        return {
            "graph_metadata": {
                "graph_id": self.graph_id,
                "version": self.version,
                "timestamp": self._timestamp(),
                "coordinator": self.coordinator,
                "serialization": "deterministic_python_json_not_rfc8785_jcs",
            },
            "nodes": [dict(node) for node in self.nodes],
            "edges": [dict(edge) for edge in self.edges],
        }

    def validate(self) -> list[str]:
        return validate_lineage_graph(self.export_graph())

    def validate_graph_integrity(self) -> bool:
        """Compatibility helper for structural validity only."""
        return not self.validate()

    def save_to_file(self, filepath: str) -> None:
        graph = self.export_graph()
        errors = validate_lineage_graph(graph) + verify_lineage_digests(graph)
        if errors:
            raise ValueError("invalid lineage graph: " + "; ".join(errors))
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(deterministic_json_bytes(graph))
