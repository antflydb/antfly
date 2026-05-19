# Storage Sim Workflow

The storage sim stack now has a shared workflow across LMDB, WAL, persistent storage, and the higher orchestration layers above persistent:

- randomized schedules in the storage-layer tests
- reducer/minimizer support for failing schedules
- checked-in replay fixtures under `pkg/antfly/src/storage/*_sim_fixtures/`
- generic fixture promotion through `zig build storage-fixture-promote`

## Targets

LMDB:

- default wrapper matrix on the Zig backend:
  `zig build storage-lmdb-test`
- wrapper matrix on the C oracle backend:
  `zig build storage-lmdb-test -Dlmdb_backend=c`
- replay-only fixture target:
  `zig build lmdb-replay-fixtures`

LMDB-specific code:

- randomized differential/crash schedules live in
  [lmdb.zig](lmdb.zig)
- reducer/minimizer support lives in
  [sim.zig](../lmdb/sim.zig)
- checked-in replay fixtures live under
  [lmdb_sim_fixtures](lmdb_sim_fixtures)

WAL:

- default WAL test matrix:
  `zig build wal-test`
- WAL matrix on the C oracle backend:
  `zig build wal-test -Dlmdb_backend=c`
- replay-only fixture target:
  `zig build wal-replay-fixtures`

Persistent:

- default persistent storage matrix:
  `zig build persistent-test`
- focused randomized workload target:
  `zig build persistent-sim-test`
- replay-only fixture target:
  `zig build persistent-replay-fixtures`
- soak-only target:
  `zig build persistent-sim-soak`

Index manager:

- default catalog/index manager matrix:
  `zig build index-manager-test`
- focused randomized workload target:
  `zig build index-manager-sim-test`
- replay-only fixture target:
  `zig build index-manager-replay-fixtures`

DB split orchestration:

- focused randomized workload target:
  `zig build db-split-sim-test`
- replay-only fixture target:
  `zig build db-split-replay-fixtures`

Soak-only targets:

- LMDB soak:
  `zig build lmdb-sim-soak`
- WAL soak:
  `zig build wal-sim-soak`
- Persistent soak:
  `zig build persistent-sim-soak`
- both:
  `zig build storage-sim-soak`

## Failure Workflow

When a randomized harness finds a failure, it reduces the schedule and writes a replayable artifact into `/tmp`.

Typical artifact prefixes:

- LMDB: `/tmp/antfly-lmdb-replay-*.fixture`
- WAL: `/tmp/antfly-wal-replay-*.fixture`
- Persistent: `/tmp/antfly-persistent-replay-*.fixture`
- Index manager: `/tmp/antfly-index-manager-replay-*.fixture`
- DB split: `/tmp/antfly-db-split-replay-*.fixture`

Promote one into the checked-in corpus with:

```sh
zig build storage-fixture-promote -- /tmp/antfly-...fixture
```

If you want to override the destination stem:

```sh
zig build storage-fixture-promote -- /tmp/antfly-...fixture custom-name
```

If the destination already exists and you want to replace it:

```sh
zig build storage-fixture-promote -- /tmp/antfly-...fixture --force
```

If you want the newest reduced artifact from `/tmp` without looking up the path:

```sh
zig build storage-fixture-promote -- --latest
```

Persistent example:

```sh
zig build storage-fixture-promote -- /tmp/antfly-persistent-replay-...fixture
```

Index manager example:

```sh
zig build storage-fixture-promote -- /tmp/antfly-index-manager-replay-...fixture
```

DB split example:

```sh
zig build storage-fixture-promote -- /tmp/antfly-db-split-replay-...fixture
```

Compatibility alias:

- `zig build lmdb-fixture-promote -- ...` still works for LMDB fixtures

## Fixture Layout

LMDB:

- `pkg/antfly/src/storage/lmdb_sim_fixtures/differential/`
- `pkg/antfly/src/storage/lmdb_sim_fixtures/crash/`

WAL:

- `pkg/antfly/src/storage/wal_sim_fixtures/replay/`
- `pkg/antfly/src/storage/wal_sim_fixtures/crash/`

Persistent:

- `pkg/antfly/src/storage/persistent_sim_fixtures/replay/`
- `pkg/antfly/src/storage/persistent_sim_fixtures/crash/`

Index manager:

- `pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures/replay/`
- `pkg/antfly/src/storage/db/catalog/index_manager_sim_fixtures/crash/`

DB split:

- `pkg/antfly/src/storage/db/db_sim_fixtures/replay/`

## Notes

- Zig is now the default durable-LSM DB backend. Keep `-Dlmdb_backend=c` around as the LMDB oracle path.
- `fixed_map` is intentionally excluded from the randomized reopen matrices
  because persisted `mm_address` values make those schedules host VM layout
  sensitive. Keep `fixed_map` coverage in targeted tests and explicit fixtures
  instead.
- WAL crash fixtures and crash-mode randomized checks only execute on the Zig backend. Under `-Dlmdb_backend=c`, those paths are skipped.
- Persistent crash fixtures and crash-mode randomized checks also only execute on the Zig backend, because they rely on the Zig LMDB publish-phase test hook in the main index environment.
- Index-manager crash fixtures and crash-mode randomized checks also only execute on the Zig backend, because they reuse the persistent publish-phase test hook under the text index.
- Persistent durability contract: once the internal WAL append returns, reopen should recover the committed persistent state even if the main index LMDB publish crashes before WAL truncation. The monotonic WAL LSN fix exists specifically to preserve that guarantee across truncate and reopen cycles.
