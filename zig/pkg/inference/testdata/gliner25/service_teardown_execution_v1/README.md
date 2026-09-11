# Model-manager teardown execution evidence

This ledger preserves 42 exact receipt, log, helper and source files (2,007,202
bytes) from three CPU attempts and seven model-free child processes. The
manifest pins every copied byte. Neither models nor children were executed
while packaging it.

CPU v1 stopped before the selected teardown tests because an optional macOS
argv observation raised `sysctl(KERN_PROCARGS2)`. Owned-process cleanup passed.
CPU v2 reached six fixture compilation errors from missing `preferred_backends`
initializers; its separate 23-test prerequisite result is not teardown evidence.
CPU v3 passed nine of ten selected tests, with one expected skip for the
separately supervised child fixture. Its four optional argv observation errors
remain recorded; required process ownership, source identity and cleanup checks
passed.

The seven fresh children use the exact 36,647,768-byte CPU test executable,
SHA256 `05d650b47cd67804ac7d16e3308b0e0bda8ad501730142408115acf98afc7590`.
All exited 86 through the real process watchdog, were reaped, and required no
outer kill. Cache destruction, TTL eviction, admission eviction, retired-handle
release, shutdown and load rollback each enter synthetic blocked destruction
while holding a 64-byte admission lease. The escaped raw-session case has no
lease: it verifies that the independent monitor and driver IO survive manager
destruction, and makes no lease-order claim.

The blocked callbacks and 100 ms close deadline are private test fixtures. The
production deadline remains 30 seconds. The driver has a five-second child
bound, a five-second reap bound and 1 MiB per-stream limit. These results prove
ticket ordering, ownership and watchdog exit behavior; they do not prove
recovery from a real Metal driver hang or the actual published-small Metal TTL
eviction/reload path. The latter remains pending.

`source_snapshots` contains exactly three files from the verified CPU v3 source
archive: `model_manager.zig`, `backends/session.zig` and the unexecuted
`gliner_boundary_cache_lifecycle_test.zig`. The last file records the pending
published-model test's implementation, not successful execution. The full
archive and binary stay outside the repository; their identities are retained.
Build wrappers v3/v4, the unchanged supervision helper, the seven-child driver,
all failures and all successes are copied verbatim. This is not a complete
dependency closure or an upstream numerical oracle. Public availability and
release qualification remain false.

Verify the copied evidence without starting any process or requiring original
temporary paths:

```sh
python3 zig/pkg/inference/testdata/gliner25/service_teardown_execution_v1/verify.py
```

Further qualification should append a distinct versioned record. See
[`GLINER25_OPERATIONS.md`](../../../../../../docs/GLINER25_OPERATIONS.md) for the
operational contract and remaining published-model evidence.
