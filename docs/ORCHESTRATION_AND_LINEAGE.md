# Injected-input orchestration prototype

`marcus_orchestrator.py` implements one deterministic processing cycle for caller-supplied barometric-pressure telemetry. It does not poll a live IZORA feed, start a daemon, or automatically open a network connection. `edmonia_osc_dispatcher.py` is a separate opt-in, IPv4-loopback-only OSC 1.0 UDP sender.

## Run the tests

```sh
python -m unittest discover -s tests -v
```

The tests use only an ephemeral loopback UDP receiver. Passing them demonstrates local serialization and datagram receipt, not REAPER connectivity or runtime activation.

## Integrity and evidence limitations

- Missingness and units are checked; unknown/unavailable/non-detection values are quarantined.
- The 870–1085 hPa to 20-TET mapping is an explicit artistic mapping, not a physical or causal law.
- SHA-256 is calculated over compact, sorted Python JSON. This is **not asserted to be RFC 8785 JCS**; `jcs_canonicalized` is false.
- Lineage edges default to `UNVERIFIED`. Recording hashes does not verify transformation correctness or input provenance.
- A successful UDP send confirms only that the local OS accepted the datagram; there is no receiver acknowledgment.
