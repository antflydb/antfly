# Fail-fast initial runnable acquisition

Initial runnable acquisition honors the existing admission allow_wait policy.
An unpublished caller that was queued and disallows waiting returns
ResourceTemporarilyUnavailable before any executor wait, releasing its operation,
byte and owner reservations. Immediate grants still succeed when ordinary CPU is
full and protected control CPU is available. Published reentry retains bounded
fair waiting even when initial acquisition disallowed waiting.

Runnable ABI version 2 forwards allow_wait through the checked API owner port.
It consumes one previously reserved byte without changing Start's size. The
version change rejects legacy interpretation; invalid boolean and reserved values
fail before borrowing executor state. Existing callers default to allowing waits.

Native and pinned Nix/glibc GNU AArch64 pass all 28 scheduler/admission and all
34 ABI contracts. Both real owner/worker suites execute all 49 tests, including
26 uncached RPC and six descriptor contracts, owner reservation/lease rollback,
actual subprocess output, cancellation/deadline stalls and real reaping.
Native uses 16 build steps; GNU uses 15. Native source hashes were captured after
completion and match GNU launch hashes. An isolated scheduler with only the new
fail-fast branch removed fails the strengthened regression: it waits until
RunnableWaitTimeout instead of rejecting ResourceTemporarilyUnavailable. Its
sources, hashes and expected terminal failure are preserved.

This is the acquisition prerequisite for independent cleanup CPU ownership,
not that transport wiring itself. Cleanup must also reserve bounded operations
and bytes without consuming health/control headroom, acquire outside connection
locks, park only physical waits and retain ownership through cleanup. The
inherited ABI 33/worker/provider/standalone integration remains supplemental from
../worker-output-errors/supplemental-production-candidate.patch.gz. Production
worker/standalone hashes are provenance only for these focused gates. Full
production/API, durability, whole-node bounds and default-enablement proof remain
required. Pooling is disabled. Compiler limits are not runtime resource bounds.
