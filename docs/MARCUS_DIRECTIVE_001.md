# M.A.R.C.U.S. Directive Adoption Record

## Directive

- Directive ID: `MARCUS-DIR-001`
- Issue date: `2026-11-15`
- Status: `ISSUED — OPERATIONAL ADOPTION DIRECTED; RUNTIME ACTIVATION TO BE VERIFIED`
- Scope: all M.A.R.C.U.S. subsystems, present and future

This document records the project-level adoption of the M.A.R.C.U.S. operating directive while preserving the distinction between:

- project-level directive adoption,
- subsystem interface availability,
- subsystem capability, and
- runtime activation.

## Governing rule

The project adopts the operating sequence:

`BUILD → OBSERVE → ANALYZE → EXPERIMENT → TEST → LEARN → IMPROVE → CONTINUE`

This record does not claim that any external subsystem process has started. It only records that the directive has been adopted in this project and that runtime activation remains pending on each subsystem's operational interface or control channel.

## Machine-readable sources

- Directive registry: `config/directive-registry.json`
- Subsystem activation register: `config/subsystem-activation-register.json`
- Registry schemas: `schemas/marcus.directive-registry.v1.schema.json` and `schemas/marcus.subsystem-activation-register.v1.schema.json`

## Operational interpretation

- Connection is a separate fact from capability.
- Capability is a separate fact from execution.
- Execution status requires operational evidence and is not inferred from the issuance of a directive alone.

Any subsystem entry marked `runtime_activation: "pending_confirmation"` indicates that the project-level directive has been adopted, but the underlying external runtime remains unconfirmed.
