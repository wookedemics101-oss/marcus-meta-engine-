# Changelog — M.A.R.C.U.S. Project Architecture

## [1.0.0-governance.1] — 2026-11-15

### Added
- **MARCUS-DIR-001**: Project-level principles-first operating directive in `docs/MARCUS_DIRECTIVE_001.md`.
- **Directive registry**: Machine-readable directive status in `config/directive-registry.json`.
- **Subsystem activation register**: Adoption, connection, execution, runtime activation, verification status, and evidence ledger for the nine subsystem entries in `config/subsystem-activation-register.json`.
- **Evidence-aware validation**: The activation-register schema now models per-subsystem verification statuses and SHA-256 evidence records, with a JCS indicator and a structural requirement for evidence when a subsystem is marked tested, verified, or failed.
- **H.E.R.M.E.S. envelope contract**: Initial structural schema for semantic context, missingness classifications, integrity metadata, and the declared 432 Hz / 20-TET project mapping convention in `schemas/hermes.semantic-envelope.v1.schema.json`.
- **Release note**: `releases/v1.0.0-governance.md` records the scope and limitations of this milestone.

### Changed
- Subsystems without test evidence are explicitly `PENDING_TEST` with an empty evidence ledger. Runtime activation remains `pending_confirmation`; project-level adoption is not evidence of live subsystem activation.

### Verification and limitations
- JSON Schemas express structural constraints only. They do not execute tests, canonicalize JSON, recompute SHA-256 digests, establish scientific validity, or confirm external runtime activation.
- No subsystem runtime, REAPER OSC connection, or telemetry test harness was executed as part of this documentation/schema milestone.
