# Corrected one-way orchestration daemon

`marcus_orchestrator_daemon.py` accepts a caller-injected telemetry provider. There is no default live or simulated feed. Its flow is one-way telemetry → provisional mapping → lineage record → optional OSC dispatch; it is **not a closed loop** and it does not implement a feedback receiver.

The OSC dispatcher uses standard bundle framing and distinct integer/float OSC tags, is loopback-only and disabled by default. `SENT` means only that the local OS accepted the UDP datagram; `receiver_confirmed` remains false. Socket use is per-send and `close()` is an explicit lifecycle method.

The pressure range, clamping policy, Python ties-to-even rounding, 432 Hz reference, and TET formula are recorded as provisional mapping parameters. Range-clamped readings are labeled `CLAMPED_LOW` or `CLAMPED_HIGH`; non-finite values are `INVALID_MEASUREMENT`; missingness and unit issues are quarantined. No production authorization or atmospheric law is implied.

Lineage digests are computed using compact sorted Python JSON serialization, not RFC 8785 JCS. `jcs_canonicalized` is false, exact input-node digest is returned, and edges remain `UNVERIFIED`. The structural validator checks duplicate IDs, dangling references, digest syntax, and cycles. It does not verify scientific provenance or transformation correctness.

Run local tests with:

```sh
python -m unittest discover -s tests -v
```
