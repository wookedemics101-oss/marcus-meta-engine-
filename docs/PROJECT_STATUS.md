# M.A.R.C.U.S. Project Status

## Scope

This document summarizes the repository-level project foundation for the M.A.R.C.U.S. directive framework and the activation register.

## Current state

- Project-level operating directive: adopted in governance records
- Runtime activation: not confirmed by independent subsystem evidence
- Subsystem registry: nine subsystem records active in the register
- Verification evidence: schema fields exist; evidence entries remain empty until a subsystem is tested

## Principle

The project must preserve the distinction between:

1. Governance adoption
2. Connection/availability
3. Capability
4. Execution
5. Verified runtime activation

A subsystem is not considered activated because a directive has been issued. It is only activated when its actual control interface or operational evidence confirms it.

## Repository artifacts

- `docs/MARCUS_DIRECTIVE_001.md`
- `config/directive-registry.json`
- `config/subsystem-activation-register.json`
- `schemas/marcus.directive-registry.v1.schema.json`
- `schemas/marcus.subsystem-activation-register.v1.schema.json`
- `schemas/hermes.semantic-envelope.v1.schema.json`
- `CHANGELOG.md`
- `releases/v1.0.0-governance.md`

## Next objective

Establish concrete verification evidence for each subsystem as it becomes available. Until that happens, the project remains in a documented governance-adoption state with pending runtime confirmation.
