"""Opt-in, loopback-only OSC 1.0 bundle dispatcher with explicit lifecycle."""
from __future__ import annotations

import ipaddress
import math
import re
import socket
import struct
from typing import Any, Iterable, Mapping

_ALLOWED = frozenset({"/edmonia/synth/tet_index", "/edmonia/synth/tet_frequency"})
_DIGEST = re.compile(r"^[0-9a-fA-F]{64}$")
_MAX_DATAGRAM = 65_507


def _osc_string(text: str) -> bytes:
    raw = text.encode("utf-8") + b"\x00"
    return raw + b"\x00" * ((-len(raw)) % 4)


def _encode_message(packet: Mapping[str, Any], allowed: frozenset[str]) -> bytes:
    address, value = packet.get("address"), packet.get("value")
    if not isinstance(address, str) or address not in allowed:
        raise ValueError("OSC address is not in the allowlist")
    if "\x00" in address or any(ch.isspace() for ch in address):
        raise ValueError("Invalid OSC address")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("OSC values must be numeric, not boolean")
    if isinstance(value, int):
        if not -(2**31) <= value < 2**31:
            raise ValueError("OSC int32 value out of range")
        tags, encoded = ",i", struct.pack(">i", value)
    else:
        if not math.isfinite(value):
            raise ValueError("OSC float must be finite")
        try:
            encoded = struct.pack(">f", value)
        except (OverflowError, struct.error) as exc:
            raise ValueError("OSC float32 value out of range") from exc
        if not math.isfinite(struct.unpack(">f", encoded)[0]):
            raise ValueError("OSC float32 value out of range")
        tags = ",f"
    return _osc_string(address) + _osc_string(tags) + encoded


def serialize_osc_bundle(packets: Iterable[Mapping[str, Any]],
                         allowed_addresses: Iterable[str] = _ALLOWED) -> bytes:
    """Encode a standard OSC bundle with immediate timetag and length prefixes."""
    packet_list = list(packets)
    if not packet_list or len(packet_list) > 64:
        raise ValueError("Bundle must contain between 1 and 64 messages")
    allowed = frozenset(allowed_addresses)
    output = bytearray(b"#bundle\x00" + struct.pack(">II", 0, 1))
    for packet in packet_list:
        message = _encode_message(packet, allowed)
        output.extend(struct.pack(">I", len(message)))
        output.extend(message)
    if len(output) > _MAX_DATAGRAM:
        raise ValueError("OSC bundle exceeds UDP payload limit")
    return bytes(output)


class EdmoniaOSCDispatcher:
    """Send only to 127.0.0.1; disabled by default. No receiver ACK is implied."""
    def __init__(self, reaper_ip: str = "127.0.0.1", reaper_port: int = 8000,
                 *, enabled: bool = False, timeout_seconds: float = 1.0) -> None:
        try:
            address = ipaddress.ip_address(reaper_ip)
        except ValueError as exc:
            raise ValueError("Only IPv4 loopback 127.0.0.1 is supported") from exc
        if address != ipaddress.IPv4Address("127.0.0.1"):
            raise ValueError("Only IPv4 loopback 127.0.0.1 is supported")
        if isinstance(reaper_port, bool) or not isinstance(reaper_port, int) or not 1 <= reaper_port <= 65535:
            raise ValueError("Port must be an integer from 1 through 65535")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        self.reaper_ip, self.reaper_port = str(address), reaper_port
        self.enabled, self.timeout_seconds = enabled, timeout_seconds
        self._closed = False

    def transmit_to_reaper(self, processing_result: Mapping[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("Dispatcher is closed")
        if not self.enabled:
            raise PermissionError("OSC transmission is disabled; explicitly opt in")
        if processing_result.get("status") != "PROCESSED":
            raise ValueError("Only PROCESSED results may be dispatched")
        digest = processing_result.get("sha256_digest")
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise ValueError("A 64-character hexadecimal digest is required as metadata")
        packets = processing_result.get("osc_packets")
        if not isinstance(packets, list):
            raise ValueError("osc_packets must be a list")
        data = serialize_osc_bundle(packets)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.timeout_seconds)
            sent = sock.sendto(data, (self.reaper_ip, self.reaper_port))
        if sent != len(data):
            raise OSError("Incomplete UDP datagram send")
        return {
            "status": "SENT",
            "destination": f"{self.reaper_ip}:{self.reaper_port}",
            "message_count": len(packets),
            "byte_count": sent,
            "sha256_digest": digest.lower(),
            "digest_verified": False,
            "receiver_acknowledged": False,
        }

    def close(self) -> None:
        """Close lifecycle; sockets are per-send and already closed after each call."""
        self._closed = True
