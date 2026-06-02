// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const mem_backend_mod = @import("../mem_backend.zig");
const persistent_mod = @import("../persistent.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("sparse_stub.zig")
else
    @import("../../sparse/sparse.zig");
const graph_mod = @import("../../graph/graph.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const resource_manager_mod = @import("../resource_manager.zig");

const mib: u64 = 1024 * 1024;
const gib: u64 = 1024 * mib;

const primary_wal_soft_limit_segments: u64 = 8;
const primary_wal_hard_limit_segments: u64 = 32;
const primary_wal_soft_limit_bytes: u64 = 512 * mib;
const primary_wal_hard_limit_bytes: u64 = 2 * gib;

const index_wal_soft_limit_segments: u64 = 4;
const index_wal_hard_limit_segments: u64 = 16;
const index_wal_soft_limit_bytes: u64 = 256 * mib;
const index_wal_hard_limit_bytes: u64 = gib;

pub const PrimaryBackendKind = enum {
    lmdb,
    mem,
    lsm_memory,
    lsm,
};

pub const PrimaryBackend = union(enum) {
    lmdb,
    mem: mem_backend_mod.Options,
    lsm_memory: lsm_backend_mod.Options,
    lsm: lsm_backend_mod.Options,
};

pub const primary_lsm_options_default = lsm_backend_mod.Options{
    .flush_threshold_bytes = 32 * 1024 * 1024,
    .bulk_ingest_flush_threshold_bytes_multiplier = 8,
    .local_block_cache_enabled = false,
    .l0_soft_limit_runs = 32,
    .l0_hard_limit_runs = 128,
    .l0_soft_limit_bytes = 512 * 1024 * 1024,
    .l0_hard_limit_bytes = 2 * 1024 * 1024 * 1024,
    .wal_soft_limit_segments = primary_wal_soft_limit_segments,
    .wal_hard_limit_segments = primary_wal_hard_limit_segments,
    .wal_soft_limit_bytes = primary_wal_soft_limit_bytes,
    .wal_hard_limit_bytes = primary_wal_hard_limit_bytes,
};

pub const text_main_lsm_options_default = lsm_backend_mod.Options{
    .flush_threshold_bytes = 16 * 1024 * 1024,
    .bulk_ingest_flush_threshold_bytes_multiplier = 4,
    .local_block_cache_enabled = false,
    .l0_soft_limit_runs = 32,
    .l0_hard_limit_runs = 128,
    .l0_soft_limit_bytes = 256 * 1024 * 1024,
    .l0_hard_limit_bytes = 1024 * 1024 * 1024,
    .wal_soft_limit_segments = index_wal_soft_limit_segments,
    .wal_hard_limit_segments = index_wal_hard_limit_segments,
    .wal_soft_limit_bytes = index_wal_soft_limit_bytes,
    .wal_hard_limit_bytes = index_wal_hard_limit_bytes,
};

pub const text_wal_lsm_options_default = lsm_backend_mod.Options{
    .flush_threshold_bytes = 16 * 1024 * 1024,
    .bulk_ingest_flush_threshold_bytes_multiplier = 4,
    .local_block_cache_enabled = false,
    .l0_soft_limit_runs = 32,
    .l0_hard_limit_runs = 128,
    .l0_soft_limit_bytes = 256 * 1024 * 1024,
    .l0_hard_limit_bytes = 1024 * 1024 * 1024,
    .wal_soft_limit_segments = index_wal_soft_limit_segments,
    .wal_hard_limit_segments = index_wal_hard_limit_segments,
    .wal_soft_limit_bytes = index_wal_soft_limit_bytes,
    .wal_hard_limit_bytes = index_wal_hard_limit_bytes,
};

pub const dense_hbc_lsm_options_default = lsm_backend_mod.Options{
    .flush_threshold_bytes = 128 * 1024 * 1024,
    .bulk_ingest_flush_threshold_bytes_multiplier = 4,
    .local_block_cache_enabled = false,
    .compact_threshold_runs = 8,
    .l0_overlap_compact_threshold_runs = 2,
    .l0_soft_limit_runs = 32,
    .l0_hard_limit_runs = 128,
    .l0_soft_limit_bytes = 1024 * 1024 * 1024,
    .l0_hard_limit_bytes = 4 * 1024 * 1024 * 1024,
    .wal_soft_limit_segments = index_wal_soft_limit_segments,
    .wal_hard_limit_segments = index_wal_hard_limit_segments,
    .wal_soft_limit_bytes = index_wal_soft_limit_bytes,
    .wal_hard_limit_bytes = index_wal_hard_limit_bytes,
    // HBC mutation streams rewrite nodes/ranges/quantized payloads. Direct
    // sorted ingest is reserved for a true final-unique bulk builder.
    .direct_bulk_ingest = false,
    // Dense indexes are opened by write, query, status, and startup catch-up
    // handles. Keep obsolete run files long enough for those independent LSM
    // manifest snapshots to finish using them.
    .obsolete_retention_ns = 5 * std.time.ns_per_min,
};

pub const graph_reverse_lsm_options_default = lsm_backend_mod.Options{
    .flush_threshold_bytes = 16 * 1024 * 1024,
    .bulk_ingest_flush_threshold_bytes_multiplier = 4,
    .local_block_cache_enabled = false,
    .l0_soft_limit_runs = 32,
    .l0_hard_limit_runs = 128,
    .l0_soft_limit_bytes = 256 * 1024 * 1024,
    .l0_hard_limit_bytes = 1024 * 1024 * 1024,
    .wal_soft_limit_segments = index_wal_soft_limit_segments,
    .wal_hard_limit_segments = index_wal_hard_limit_segments,
    .wal_soft_limit_bytes = index_wal_soft_limit_bytes,
    .wal_hard_limit_bytes = index_wal_hard_limit_bytes,
};

pub const sparse_lsm_options_default = graph_reverse_lsm_options_default;

pub const IndexBackendOptions = struct {
    text_main_backend: persistent_mod.MainBackend = .lsm,
    dense_storage_backend: hbc_mod.StorageBackend = .lsm,
    sparse_backend: sparse_mod.SparseBackend = .lsm,
    graph_reverse_backend: graph_mod.ReverseBackend = .lsm,
    text_lsm_storage: ?lsm_backend_mod.Storage = null,
    dense_lsm_storage: ?lsm_backend_mod.Storage = null,
    sparse_lsm_storage: ?lsm_backend_mod.Storage = null,
    graph_lsm_storage: ?lsm_backend_mod.Storage = null,
    lsm_cache: ?*lsm_backend_mod.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    lsm_root_generation: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    text_main_lsm_options: lsm_backend_mod.Options = text_main_lsm_options_default,
    text_wal_lsm_options: lsm_backend_mod.Options = text_wal_lsm_options_default,
    dense_lsm_options: lsm_backend_mod.Options = dense_hbc_lsm_options_default,
    sparse_lsm_options: lsm_backend_mod.Options = sparse_lsm_options_default,
    graph_reverse_lsm_options: lsm_backend_mod.Options = graph_reverse_lsm_options_default,
};

pub const CoreOpenOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    primary_backend: PrimaryBackend = .{ .lsm = primary_lsm_options_default },
    storage: ?lsm_backend_mod.Storage = null,
    lsm_cache: ?*lsm_backend_mod.Cache = null,
    hbc_cache: ?*hbc_mod.Cache = null,
    lsm_root_generation: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    index_backends: IndexBackendOptions = .{},
};

pub const ResolvedOpenConfig = struct {
    primary_backend_kind: PrimaryBackendKind,
    primary_lsm_storage: ?lsm_backend_mod.Storage,
    index_backends: IndexBackendOptions,

    pub fn init(
        primary_backend: PrimaryBackend,
        storage_override: ?lsm_backend_mod.Storage,
        lsm_cache: ?*lsm_backend_mod.Cache,
        hbc_cache: ?*hbc_mod.Cache,
        lsm_root_generation: u64,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        overrides: IndexBackendOptions,
    ) ResolvedOpenConfig {
        const primary_backend_kind = primaryBackendKind(primary_backend);
        const primary_lsm_storage = resolvedPrimaryLsmStorage(primary_backend, storage_override);
        return .{
            .primary_backend_kind = primary_backend_kind,
            .primary_lsm_storage = primary_lsm_storage,
            .index_backends = indexBackendOptionsForPrimary(primary_backend_kind, primary_lsm_storage, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, overrides),
        };
    }
};

pub fn primaryBackendKind(primary_backend: PrimaryBackend) PrimaryBackendKind {
    return switch (primary_backend) {
        .lmdb => .lmdb,
        .mem => .mem,
        .lsm_memory => .lsm_memory,
        .lsm => .lsm,
    };
}

pub fn primaryBackendLsmStorage(primary_backend: PrimaryBackend) ?lsm_backend_mod.Storage {
    return switch (primary_backend) {
        .lsm => |opts| opts.storage,
        .lmdb, .mem, .lsm_memory => null,
    };
}

pub fn resolvedPrimaryLsmStorage(
    primary_backend: PrimaryBackend,
    storage_override: ?lsm_backend_mod.Storage,
) ?lsm_backend_mod.Storage {
    return storage_override orelse primaryBackendLsmStorage(primary_backend);
}

pub fn mergedLsmOptions(
    storage_override: ?lsm_backend_mod.Storage,
    cache_override: ?*lsm_backend_mod.Cache,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    no_sync: bool,
    backend_opts: lsm_backend_mod.Options,
) lsm_backend_mod.Options {
    var merged = backend_opts;
    merged.backend.read_only = backend_opts.backend.read_only;
    merged.backend.create_if_missing = backend_opts.backend.create_if_missing;
    merged.storage = storage_override orelse backend_opts.storage;
    merged.cache = cache_override orelse backend_opts.cache;
    if (resource_manager) |manager| {
        merged.resource_manager = manager;
        if (merged.cache) |cache| cache.attachResourceManager(manager);
    }
    if (backend_opts.backend.durability == .full and no_sync) {
        merged.backend.durability = .none;
    }
    return merged;
}

pub fn mergedIndexLsmOptions(
    storage_override: ?lsm_backend_mod.Storage,
    cache_override: ?*lsm_backend_mod.Cache,
    root_generation_override: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    store_opts: lsm_backend_mod.Options,
) lsm_backend_mod.Options {
    var merged = store_opts;
    merged.storage = storage_override orelse store_opts.storage;
    merged.cache = cache_override orelse store_opts.cache;
    if (root_generation_override != 0 and merged.root_generation == 0) {
        merged.root_generation = root_generation_override;
    }
    if (resource_manager) |manager| {
        merged.resource_manager = manager;
        if (merged.cache) |cache| cache.attachResourceManager(manager);
    }
    return merged;
}

pub fn splitLsmOptions(
    primary_backend: PrimaryBackend,
    storage_override: ?lsm_backend_mod.Storage,
    cache_override: ?*lsm_backend_mod.Cache,
) ?lsm_backend_mod.Options {
    return switch (primary_backend) {
        .lsm => |opts| blk: {
            var split_opts = mergedLsmOptions(storage_override, cache_override, null, true, opts);
            split_opts.backend.durability = .none;
            split_opts.background_executor = null;
            break :blk split_opts;
        },
        .lmdb, .mem, .lsm_memory => null,
    };
}

pub fn textMainBackendForPrimary(kind: PrimaryBackendKind) persistent_mod.MainBackend {
    return switch (kind) {
        .lmdb => .lsm,
        .mem => .lsm_memory,
        .lsm_memory => .lsm_memory,
        .lsm => .lsm,
    };
}

pub fn denseStorageBackendForPrimary(kind: PrimaryBackendKind) hbc_mod.StorageBackend {
    return switch (kind) {
        .lmdb, .mem, .lsm_memory, .lsm => .lsm,
    };
}

pub fn graphReverseBackendForPrimary(kind: PrimaryBackendKind) graph_mod.ReverseBackend {
    return switch (kind) {
        .lmdb => .lsm,
        .mem => .lsm_memory,
        .lsm_memory => .lsm_memory,
        .lsm => .lsm,
    };
}

pub fn sparseBackendForPrimary(kind: PrimaryBackendKind) sparse_mod.SparseBackend {
    return switch (kind) {
        .lmdb => .lsm,
        .mem => .lsm_memory,
        .lsm_memory => .lsm_memory,
        .lsm => .lsm,
    };
}

pub fn indexBackendOptionsForPrimary(
    kind: PrimaryBackendKind,
    primary_lsm_storage: ?lsm_backend_mod.Storage,
    lsm_cache: ?*lsm_backend_mod.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    overrides: IndexBackendOptions,
) IndexBackendOptions {
    return .{
        .text_main_backend = textMainBackendForPrimary(kind),
        .dense_storage_backend = denseStorageBackendForPrimary(kind),
        .sparse_backend = sparseBackendForPrimary(kind),
        .graph_reverse_backend = graphReverseBackendForPrimary(kind),
        .text_lsm_storage = overrides.text_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
        .dense_lsm_storage = overrides.dense_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
        .sparse_lsm_storage = overrides.sparse_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
        .graph_lsm_storage = overrides.graph_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
        .lsm_cache = overrides.lsm_cache orelse lsm_cache,
        .hbc_cache = overrides.hbc_cache orelse hbc_cache,
        .lsm_root_generation = if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
        .resource_manager = overrides.resource_manager orelse resource_manager,
        .text_main_lsm_options = mergedIndexLsmOptions(
            overrides.text_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
            overrides.lsm_cache orelse lsm_cache,
            if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
            overrides.resource_manager orelse resource_manager,
            overrides.text_main_lsm_options,
        ),
        .text_wal_lsm_options = mergedIndexLsmOptions(
            overrides.text_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
            overrides.lsm_cache orelse lsm_cache,
            if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
            overrides.resource_manager orelse resource_manager,
            overrides.text_wal_lsm_options,
        ),
        .dense_lsm_options = mergedIndexLsmOptions(
            overrides.dense_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
            overrides.lsm_cache orelse lsm_cache,
            if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
            overrides.resource_manager orelse resource_manager,
            overrides.dense_lsm_options,
        ),
        .sparse_lsm_options = mergedIndexLsmOptions(
            overrides.sparse_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
            overrides.lsm_cache orelse lsm_cache,
            if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
            overrides.resource_manager orelse resource_manager,
            overrides.sparse_lsm_options,
        ),
        .graph_reverse_lsm_options = mergedIndexLsmOptions(
            overrides.graph_lsm_storage orelse if (kind == .lsm) primary_lsm_storage else null,
            overrides.lsm_cache orelse lsm_cache,
            if (overrides.lsm_root_generation != 0) overrides.lsm_root_generation else lsm_root_generation,
            overrides.resource_manager orelse resource_manager,
            overrides.graph_reverse_lsm_options,
        ),
    };
}

test "index lsm profiles preserve current flush profiles" {
    const opts = IndexBackendOptions{};
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), opts.text_main_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), opts.text_wal_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(index_wal_soft_limit_segments, opts.text_main_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(index_wal_hard_limit_segments, opts.text_main_lsm_options.wal_hard_limit_segments);
    try std.testing.expectEqual(index_wal_soft_limit_bytes, opts.text_main_lsm_options.wal_soft_limit_bytes);
    try std.testing.expectEqual(index_wal_hard_limit_bytes, opts.text_main_lsm_options.wal_hard_limit_bytes);
    try std.testing.expectEqual(index_wal_soft_limit_segments, opts.text_wal_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(index_wal_hard_limit_segments, opts.text_wal_lsm_options.wal_hard_limit_segments);
    try std.testing.expectEqual(@as(usize, 8), opts.dense_lsm_options.flush_threshold);
    try std.testing.expectEqual(@as(u64, 128 * 1024 * 1024), opts.dense_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(@as(usize, 4), opts.dense_lsm_options.bulk_ingest_flush_threshold_bytes_multiplier);
    try std.testing.expectEqual(@as(usize, 8), opts.dense_lsm_options.compact_threshold_runs);
    try std.testing.expectEqual(@as(usize, 2), opts.dense_lsm_options.l0_overlap_compact_threshold_runs);
    try std.testing.expectEqual(@as(usize, 32), opts.dense_lsm_options.l0_soft_limit_runs);
    try std.testing.expectEqual(@as(usize, 128), opts.dense_lsm_options.l0_hard_limit_runs);
    try std.testing.expectEqual(@as(u64, 1024 * 1024 * 1024), opts.dense_lsm_options.l0_soft_limit_bytes);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024 * 1024), opts.dense_lsm_options.l0_hard_limit_bytes);
    try std.testing.expectEqual(index_wal_soft_limit_segments, opts.dense_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(index_wal_hard_limit_segments, opts.dense_lsm_options.wal_hard_limit_segments);
    try std.testing.expectEqual(index_wal_soft_limit_bytes, opts.dense_lsm_options.wal_soft_limit_bytes);
    try std.testing.expectEqual(index_wal_hard_limit_bytes, opts.dense_lsm_options.wal_hard_limit_bytes);
    try std.testing.expectEqual(false, opts.dense_lsm_options.direct_bulk_ingest);
    try std.testing.expect(opts.dense_lsm_options.obsolete_retention_ns > 0);
    try std.testing.expectEqual(sparse_mod.SparseBackend.lsm, opts.sparse_backend);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), opts.sparse_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(index_wal_soft_limit_segments, opts.sparse_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(index_wal_hard_limit_segments, opts.sparse_lsm_options.wal_hard_limit_segments);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), opts.graph_reverse_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(index_wal_soft_limit_segments, opts.graph_reverse_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(index_wal_hard_limit_segments, opts.graph_reverse_lsm_options.wal_hard_limit_segments);
    const primary_opts = primary_lsm_options_default;
    try std.testing.expectEqual(@as(u64, 32 * 1024 * 1024), primary_opts.flush_threshold_bytes);
    try std.testing.expectEqual(@as(usize, 32), primary_opts.l0_soft_limit_runs);
    try std.testing.expectEqual(primary_wal_soft_limit_segments, primary_opts.wal_soft_limit_segments);
    try std.testing.expectEqual(primary_wal_hard_limit_segments, primary_opts.wal_hard_limit_segments);
    try std.testing.expectEqual(primary_wal_soft_limit_bytes, primary_opts.wal_soft_limit_bytes);
    try std.testing.expectEqual(primary_wal_hard_limit_bytes, primary_opts.wal_hard_limit_bytes);
}

test "index lsm profiles inherit shared cache root generation and overrides" {
    const resolved = indexBackendOptionsForPrimary(.lsm, null, null, null, 9, null, .{
        .dense_lsm_options = .{
            .flush_threshold = 128,
            .wal_soft_limit_segments = 12,
            .wal_hard_limit_segments = 24,
            .wal_soft_limit_bytes = 768 * mib,
            .wal_hard_limit_bytes = 3 * gib,
        },
    });
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), resolved.text_main_lsm_options.flush_threshold_bytes);
    try std.testing.expectEqual(@as(usize, 128), resolved.dense_lsm_options.flush_threshold);
    try std.testing.expectEqual(@as(u64, 12), resolved.dense_lsm_options.wal_soft_limit_segments);
    try std.testing.expectEqual(@as(u64, 24), resolved.dense_lsm_options.wal_hard_limit_segments);
    try std.testing.expectEqual(@as(u64, 768 * mib), resolved.dense_lsm_options.wal_soft_limit_bytes);
    try std.testing.expectEqual(@as(u64, 3 * gib), resolved.dense_lsm_options.wal_hard_limit_bytes);
    try std.testing.expectEqual(@as(u64, 9), resolved.text_main_lsm_options.root_generation);
    try std.testing.expectEqual(@as(u64, 9), resolved.dense_lsm_options.root_generation);
    try std.testing.expectEqual(@as(u64, 9), resolved.sparse_lsm_options.root_generation);
    try std.testing.expect(resolved.dense_lsm_options.obsolete_retention_ns > 0);
}
