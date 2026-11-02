# Shared node runnable owner foundation

`NodeOwner` owns a stable scheduler independently of API borrower lifetimes.
Closing denies new borrowers and closes queued scheduler work; live execution
retains its existing retirement owner. Destruction requires every borrower and
execution to have retired. All 14 selected core scheduler tests pass natively
and with pinned Nix/glibc GNU AArch64 Zig. `core.json` pins the tested core source
and commands; complete output is retained in the two `core-*.log.gz` files.
The Linux core compiler/test container had two CPUs and a 1 GiB memory/swap limit.
This small contract result is not a service pressure or whole-production gate.

The API candidate can borrow a trusted API-kernel-owned opaque node handle. It
routes execution grants and metrics to that owner's scheduler, releases its
borrow after its workers join, and preserves the shared owner when the API is
destroyed. The full API fixture tests two borrowers, retirement of one API while
another still uses the owner, protected control capacity, and closed/conflicting
configuration rejection. That gate is currently pending. Its initial compile
failure was an opaque-pointer comparison in the earlier provider assertion;
`initial-native-api.json` pins those failed bytes and the complete negative log
is retained. The assertion now casts explicitly and the gate is running again.

The combined candidate adds checked API-kernel owner factory/lifecycle
callbacks and standalone handle wiring; their full gates remain pending.
Inference and background entry ownership, inherited synchronous provider grants,
and mixed-load/default qualification remain required. Cross-archive consumers must use checked owner
callbacks; they must not dereference `NodeOwner` or transfer raw Zig errors.

The first executable API run passed 37 embedded tests, then exposed a teardown
bug: deferred borrower release read the API after its fields were poisoned.
The negative receipt pins those bytes and retains the complete output. The
corrected candidate captures the owner pointer before teardown and releases it
after joins without accessing the destroyed API. Its combined API gate is running.

The corrected combined native API gate now passes all 50 build steps and
78 tests (37 embedded, 41 coordinator), with zero skips or leaks. Its complete
output and source hashes are recorded in the shared-node-owner-abi native API
receipt. This includes the checked factory/wrapper fixtures, two API borrowers,
closed/conflicting configuration checks, provider checkpoints, and existing
HTTP/pgwire contracts. GNU API, separate archive execution and standalone
production startup/shutdown remain pending.
