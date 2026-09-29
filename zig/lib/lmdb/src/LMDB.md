# Standalone LMDB library

`root.zig` is a Zig implementation of LMDB's file format, transactions, cursors, and page management. It is a standalone library under `zig/lib/lmdb`; Antfly's database does not select LMDB as a storage backend.

The C implementation in `zig/lib/lmdb/mdb.c` and `midl.c` remains as a differential oracle. `lmdb.zig` supplies the shared Zig-facing wrapper used by the C-versus-Zig tests, and `lmdb_vopr.zig` and the fixtures in `zig/lib/lmdb/fixtures` cover replay and crash outcomes. The wrapper is test and benchmark infrastructure, not an Antfly storage adapter.

From `zig/`, use these focused targets:

- `zig build lmdb-test` for the Zig port's unit tests.
- `zig build antfly-storage-lmdb-test` for the standalone C/Zig wrapper and differential fixtures.
- `zig build lmdb-vopr-test` for the replayable differential campaign.
- `zig build lmdb-workload-soak` for the randomized workload soak.
- `zig build lmdb-bench` to build the standalone C and Zig benchmark binaries.

The library's build options choose the C or Zig implementation for those targets. They do not affect Antfly's runtime storage configuration.
