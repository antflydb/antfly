// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

const antfly = @import("antfly-zig");

pub fn main() !void {
    comptime {
        if (antfly.build_options.lmdb_enabled) @compileError("product package must not enable LMDB");
        if (@hasDecl(antfly.lmdb, "Environment")) @compileError("product package exports the LMDB wrapper");
        if (@hasDecl(antfly.lmdb_backend, "Backend")) @compileError("product package exports the LMDB adapter");
    }

    const OpenOptions = antfly.db.OpenOptions;
    const cases = [_]OpenOptions{
        .{ .primary_backend = .lmdb },
        .{ .index_backends = .{ .text_main_backend = .lmdb } },
        .{ .index_backends = .{ .dense_storage_backend = .lmdb } },
        .{ .index_backends = .{ .sparse_backend = .lmdb } },
        .{ .index_backends = .{ .graph_reverse_backend = .lmdb } },
        .{ .change_journal_backend = .lmdb },
    };
    for (cases) |options| {
        if (!antfly.db.hasLegacyLmdbBackendSelection(options)) return error.LegacyBackendWasMissed;
    }
    if (antfly.db.hasLegacyLmdbBackendSelection(OpenOptions{})) return error.LsmBackendWasRejected;
}
