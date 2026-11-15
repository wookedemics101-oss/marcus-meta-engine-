# H.E.R.M.E.S. lineage verification semantics

Each node retains the exact hash input. SHA-256 is recomputable over that retained record using compact, sorted Python JSON serialization. This serializer is deterministic for supported inputs but is **not RFC 8785 JCS**. Matching a digest proves integrity relative to the retained bytes only; it does not authenticate the source or establish measurement truth or scientific validity.

Structural graph validity, payload digest matching, round-trip verification, cryptographic verification, and source authentication are distinct states. Edges default to `UNVERIFIED`. The logger refuses `CRYPTOGRAPHICALLY_VERIFIED` without independent verifier evidence. The telemetry-to-OSC pipeline remains one-way; no DSP feedback or closed-loop control is claimed.

The 432 Hz / 20-TET pressure mapping is an explicitly provisional artistic configuration, not an atmospheric law or production mapping authorization.

Run tests with:

```sh
python -m unittest discover -s tests -v
```
