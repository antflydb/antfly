<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHAGates — structural diagrams

Generated from [`AntflyHAGates.tla`](../../../storage/ha/AntflyHAGates.tla). 16 state variables, 0 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `role`

Domain: `primary`, `standby`, `former_primary`. No statically extractable guard/update transitions.

### `commitMode`

Domain: `async`, `remote_write`, `remote_apply`. No statically extractable guard/update transitions.

### `failurePolicy`

Domain: `block`, `fail_closed`, `degrade_to_async`. No statically extractable guard/update transitions.

### `readConsistency`

Domain: `stale_ok`, `at_least_lsn`, `primary`. No statically extractable guard/update transitions.

### `commitAction`

Domain: `acknowledge`, `wait_for_standby`, `reject`, `acknowledge_degraded`. No statically extractable guard/update transitions.

### `readAction`

Domain: `serve_standby`, `wait_for_apply`, `wait_for_metadata`, `route_to_primary`. No statically extractable guard/update transitions.

### `writeAction`

Domain: `allow_write`, `reject_write`, `wait_for_promotion`. No statically extractable guard/update transitions.

### `ownerAction`

Domain: `allow_owner_job`, `reject_owner_job`. No statically extractable guard/update transitions.

### `backgroundAction`

Domain: `run_mutating_runtime`, `suppress_mutating_runtime`. No statically extractable guard/update transitions.
