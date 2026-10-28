# Cleanup ownership preserves health operation and byte headroom

The shared API execution owner admits at most one retained transport cleanup
capsule. Its input/adapter bytes plus actual Capsule metadata may use at most
half the control byte reserve. Policies with only one control operation reserve
reject cleanup so health can use that reserve. The gate covers queued, running,
parked and stopped cleanup through actual owning finish; CPU parking does not
return it. Denial, invalid executor/deadline and allocation failure unwind the
gate and owner lease before returning an unpublished failure.

Checked runnable ABI version 3 marks trusted cleanup separately from ordinary
control roots, consumes another reserved byte without growing Start and rejects
legacy interpretation. Cleanup requires single-slot control, fail-fast initial
acquisition, an explicit independent deadline and no foreground cancellation
view. The owner enforces at most 250ms remaining on that deadline. Existing
ordinary/control calls retain their default behavior.

Native and pinned Nix/glibc GNU AArch64 pass all 35 ABI contracts and all 26
executed owner/descriptor/worker tests. Unchanged 26 RPC contracts are cached from
the prior successful uncached gate. Native completes 16 build steps; GNU 15.
The new owner regressions retain all four ordinary operations, admit and park one
cleanup, reject a second cleanup without changing accounting, and still admit
three health/control operations. They verify byte/deadline/allocation rollback,
small-policy health progress and gate/byte retention through denied reentry until
owning finish. Actual subprocess output/reaping gates remain green.
Native hashes were captured after completion and match GNU launch hashes.

Transport callback wiring remains required: acquire outside connection locks,
park only physical waits and resume after unlocking atomic frames. This gate does
not qualify current production Worker.Client/standalone startup. Inherited ABI 33
integration remains supplemental from ../worker-output-errors. Full production/
API, durable completion, quarantine, whole-node bounds and default-enablement
proof remain pending. Pooling is disabled. Compiler limits are not runtime bounds.
