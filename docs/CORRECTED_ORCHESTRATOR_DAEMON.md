# Corrected telemetry-to-OSC dispatcher prototype

`marcus_orchestrator_daemon.py` is an injected-dependency, one-way telemetry-to-OSC dispatcher. It is not a closed loop: there is no feedback receiver or acknowledgment protocol. No feed is configured by default, and importing the module starts no daemon.

The OSC sender uses standard OSC bundle framing, immediate timetag, message length prefixes, OSC int32 for `tet_index`, and float32 for frequency. Transmission is opt-in and limited to IPv4 loopback. `SENT` means the local OS accepted a UDP datagram; it does not mean REAPER received or applied it.

The default feed provenance label for an injected provider is `SIMULATED`; integrations must explicitly label provider provenance. This does not independently verify the source. Pressure bounds (870–1085 hPa), clamping, Python ties-to-even rounding, 432 Hz anchor, and the 20-TET formula are recorded as provisional mapping parameters, not atmospheric laws. Non-finite values and invalid units/missingness are quarantined.

Lineage digests use deterministic compact, sorted Python JSON serialization, not RFC 8785 JCS. The graph records `jcs_canonicalized: false`, retains the digest by exact node ID, defaults edges to `UNVERIFIED`, and can detect duplicate IDs, malformed hashes, dangling references, and directed cycles. A hash alone does not verify a transformation.

Run deterministic local tests with:

```sh
python -m unittest discover -s tests -v
```
