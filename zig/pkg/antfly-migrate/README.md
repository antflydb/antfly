# antfly-migrate

Standalone SQLite to Antfly Lite relational migration, using explicit JSON
plans and the public embedded C ABI. The executable is separate from `antfly`
and `antfly-lite` and is only installed by the explicit `migrate` build step.

Build `libantfly` first (`cd zig && zig build capi`), then from `zig/`:

```sh
zig build migrate
zig build migrate-test migrate-e2e
```

The small standalone build avoids loading the server build graph:

```sh
zig build --build-file pkg/antfly-migrate/build.zig migrate \
  -Dmigrate-antfly-lib-dir=/absolute/path/to/libantfly
```

`-Dsqlite-root=/path/to/sqlite` optionally selects SQLite headers/libraries.
Without it, SQLite development headers and `libsqlite3` must be on the system
search path. Zig 0.17 (matching the repository) and ABI 3 `libantfly` are required. The first
implementation targets macOS and Linux local POSIX filesystems.

Run a plan:

```sh
zig-out/bin/antfly-migrate migrate --plan migration.json --dry-run
zig-out/bin/antfly-migrate migrate --plan migration.json
```

For the standalone build the installed executable is in the package's own
`zig-out/bin`. Paths in the plan are relative to its JSON file. The target must
be new; the tool never overwrites it. Dry-run validates all selected source
rows and target column types in a temporary staged schema; it does not publish
a file. Stop the source application when migrating multiple databases/files.

Real-library acceptance tests:

```sh
ANTFLY_MIGRATE_BIN=/absolute/path/to/antfly-migrate \
  python3 -m unittest discover -s pkg/antfly-migrate/tests -v
```

See [the design and plan contract](../../../docs/design/sqlite-aflite-migration.md)
for snapshot consistency, conversions, verification, publication, and the
boundary between generic relational migration and Codex history reconstruction.
