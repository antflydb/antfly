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
const platform = @import("antfly_platform");

const apply_rw_lock_mod = @import("apply_rw_lock.zig");
const apply_state = @import("derived/apply_state.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const db_config = @import("config.zig");
const db_core = @import("core.zig");
const derived_types = @import("derived/derived_types.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const doc_set = @import("doc_set.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const ha_types = @import("ha_types.zig");
const internal_keys = @import("../internal_keys.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mapper = @import("document_mapper.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const relational_store_mod = @import("relational_store.zig");
const resolution_runtime_mod = @import("resolution_runtime.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const shard_mod = @import("../shard.zig");
const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const schema_mod = @import("../schema.zig");
const ttl_mod = @import("../ttl.zig");
const types = @import("types.zig");
const artifact_ids = @import("artifact_ids.zig");
const platform_clock = @import("../../platform/clock.zig");

const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);

pub const IndexStatusSnapshot = struct {
    kind: types.IndexKind,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    updated_at_ns: u64 = 0,
};

pub fn buildDerivedBatch(
    alloc: Allocator,
    req: types.BatchRequest,
    extracted: []const mapper.ExtractedWrite,
    deleted_artifact_keys: []const []u8,
    changed_artifact_keys: []const []u8,
) !derived_types.DerivedBatch {
    var documents = try alloc.alloc(derived_types.DerivedDocument, req.writes.len);
    var initialized: usize = 0;
    errdefer {
        var tmp = derived_types.DerivedBatch{ .documents = documents[0..initialized] };
        derived_types.deinitDerivedBatch(alloc, &tmp);
    }

    for (req.writes, 0..) |write, i| {
        var targets = std.ArrayListUnmanaged(derived_types.DerivedTargetRef).empty;
        defer targets.deinit(alloc);

        if (extracted[i].cleaned_value != null) {
            try targets.append(alloc, .{
                .kind = .full_text,
                .index_name = try alloc.dupe(u8, "*"),
            });
        }
        for (extracted[i].dense_embeddings) |embedding| {
            try targets.append(alloc, .{
                .kind = .dense_vector,
                .index_name = try alloc.dupe(u8, embedding.index_name),
            });
        }
        for (extracted[i].sparse_embeddings) |embedding| {
            try targets.append(alloc, .{
                .kind = .sparse_vector,
                .index_name = try alloc.dupe(u8, embedding.index_name),
            });
        }
        for (extracted[i].mentioned_graph_indexes) |index_name| {
            try targets.append(alloc, .{
                .kind = .graph,
                .index_name = try alloc.dupe(u8, index_name),
            });
        }
        for (req.graph_writes) |graph_write| {
            if (std.mem.eql(u8, graph_write.source, write.key)) {
                try targets.append(alloc, .{
                    .kind = .graph,
                    .index_name = try alloc.dupe(u8, graph_write.index_name),
                });
            }
        }

        documents[i] = .{
            .key = try alloc.dupe(u8, write.key),
            .action = if (extracted[i].cleaned_value == null) .preserve_base_document else .upsert,
            .cleaned_value = null,
            .targets = try targets.toOwnedSlice(alloc),
        };
        initialized += 1;
    }

    var deleted_keys = try alloc.alloc([]const u8, req.deletes.len + deleted_artifact_keys.len);
    var deleted_initialized: usize = 0;
    errdefer {
        for (deleted_keys[0..deleted_initialized]) |key| alloc.free(key);
        alloc.free(deleted_keys);
    }
    for (req.deletes, 0..) |key, i| {
        deleted_keys[i] = try alloc.dupe(u8, key);
        deleted_initialized += 1;
    }
    for (deleted_artifact_keys) |key| {
        deleted_keys[deleted_initialized] = try alloc.dupe(u8, key);
        deleted_initialized += 1;
    }

    var overwritten_doc_keys_list = std.ArrayListUnmanaged([]const u8).empty;
    defer overwritten_doc_keys_list.deinit(alloc);
    for (req.writes, 0..) |write, i| {
        if (extracted[i].cleaned_value != null) {
            try overwritten_doc_keys_list.append(alloc, try alloc.dupe(u8, write.key));
        }
    }

    var changed_artifact_keys_list = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (changed_artifact_keys_list.items) |key| alloc.free(@constCast(key));
        changed_artifact_keys_list.deinit(alloc);
    }
    for (changed_artifact_keys) |key| {
        try changed_artifact_keys_list.append(alloc, try alloc.dupe(u8, key));
    }
    for (deleted_artifact_keys) |key| {
        if (!internal_keys.isAssetArtifactKey(key) and !internal_keys.isGraphEdgeArtifactKey(key)) continue;
        try changed_artifact_keys_list.append(alloc, try alloc.dupe(u8, key));
    }

    var graph_doc_clears = std.ArrayListUnmanaged(derived_types.DerivedGraphDocClear).empty;
    errdefer {
        for (graph_doc_clears.items) |clear| {
            alloc.free(clear.key);
            for (clear.index_names) |index_name| alloc.free(index_name);
            if (clear.index_names.len > 0) alloc.free(clear.index_names);
        }
        graph_doc_clears.deinit(alloc);
    }
    for (req.writes, 0..) |write, i| {
        if (extracted[i].mentioned_graph_indexes.len == 0) continue;
        var index_names = try alloc.alloc([]const u8, extracted[i].mentioned_graph_indexes.len);
        for (extracted[i].mentioned_graph_indexes, 0..) |index_name, j| {
            index_names[j] = try alloc.dupe(u8, index_name);
        }
        try graph_doc_clears.append(alloc, .{
            .key = try alloc.dupe(u8, write.key),
            .index_names = index_names,
        });
    }

    var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer {
        for (graph_writes.items) |write| {
            alloc.free(@constCast(write.index_name));
            alloc.free(@constCast(write.source));
            alloc.free(@constCast(write.target));
            alloc.free(@constCast(write.edge_type));
            if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
        }
        graph_writes.deinit(alloc);
    }
    for (extracted) |item| {
        for (item.graph_writes) |write| {
            try graph_writes.append(alloc, .{
                .index_name = try alloc.dupe(u8, write.index_name),
                .source = try alloc.dupe(u8, write.source),
                .target = try alloc.dupe(u8, write.target),
                .edge_type = try alloc.dupe(u8, write.edge_type),
                .weight = write.weight,
                .created_at = write.created_at,
                .updated_at = write.updated_at,
                .metadata_json = if (write.metadata_json.len > 0) try alloc.dupe(u8, write.metadata_json) else "",
            });
        }
    }
    for (req.graph_writes) |write| {
        try graph_writes.append(alloc, .{
            .index_name = try alloc.dupe(u8, write.index_name),
            .source = try alloc.dupe(u8, write.source),
            .target = try alloc.dupe(u8, write.target),
            .edge_type = try alloc.dupe(u8, write.edge_type),
            .weight = write.weight,
            .created_at = write.created_at,
            .updated_at = write.updated_at,
            .metadata_json = if (write.metadata_json.len > 0) try alloc.dupe(u8, write.metadata_json) else "",
        });
    }

    var graph_deletes = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
    errdefer {
        for (graph_deletes.items) |delete| {
            alloc.free(@constCast(delete.index_name));
            alloc.free(@constCast(delete.source));
            alloc.free(@constCast(delete.target));
            alloc.free(@constCast(delete.edge_type));
        }
        graph_deletes.deinit(alloc);
    }
    for (req.graph_deletes) |delete| {
        try graph_deletes.append(alloc, .{
            .index_name = try alloc.dupe(u8, delete.index_name),
            .source = try alloc.dupe(u8, delete.source),
            .target = try alloc.dupe(u8, delete.target),
            .edge_type = try alloc.dupe(u8, delete.edge_type),
        });
    }

    var dense_embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
    errdefer {
        for (dense_embeddings.items) |embedding| {
            alloc.free(embedding.index_name);
            if (embedding.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            if (embedding.vector.len > 0) alloc.free(embedding.vector);
        }
        dense_embeddings.deinit(alloc);
    }
    for (extracted) |item| {
        for (item.dense_embeddings) |embedding| {
            try dense_embeddings.append(alloc, .{
                .index_name = try alloc.dupe(u8, embedding.index_name),
                .doc_key = try alloc.dupe(u8, embedding.doc_key),
                .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
                .vector = if (embedding.artifact_key != null) &.{} else try alloc.dupe(f32, embedding.vector),
            });
        }
    }

    var sparse_embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
    errdefer {
        for (sparse_embeddings.items) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.indices.len > 0) alloc.free(embedding.indices);
            if (embedding.values.len > 0) alloc.free(embedding.values);
        }
        sparse_embeddings.deinit(alloc);
    }
    for (extracted) |item| {
        for (item.sparse_embeddings) |embedding| {
            try sparse_embeddings.append(alloc, .{
                .index_name = try alloc.dupe(u8, embedding.index_name),
                .doc_key = try alloc.dupe(u8, embedding.doc_key),
                .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
                .indices = try alloc.dupe(u32, embedding.indices),
                .values = try alloc.dupe(f32, embedding.values),
            });
        }
    }

    return .{
        .sequence = 0,
        .documents = documents,
        .deleted_keys = deleted_keys,
        .overwritten_doc_keys = try overwritten_doc_keys_list.toOwnedSlice(alloc),
        .changed_artifact_keys = try changed_artifact_keys_list.toOwnedSlice(alloc),
        .graph_doc_clears = try graph_doc_clears.toOwnedSlice(alloc),
        .dense_embeddings = try dense_embeddings.toOwnedSlice(alloc),
        .sparse_embeddings = try sparse_embeddings.toOwnedSlice(alloc),
        .generated_enrichment_refs = &.{},
        .graph_writes = try graph_writes.toOwnedSlice(alloc),
        .graph_deletes = try graph_deletes.toOwnedSlice(alloc),
    };
}

const index_status_prefix = "\x00\x00__metadata__:index_status:";
const index_status_magic: u64 = 0x3153544154584449; // "IDXTATS1" little-endian
const index_status_encoded_len = 8 * 8;

pub fn getenv(name: [*:0]const u8) ?[]const u8 {
    if (comptime builtin.os.tag == .freestanding) return null;
    return platform.env.getenv(name);
}

pub fn readEnvUsize(name: [:0]const u8, default_value: usize) usize {
    const raw = platform.env.getenvSlice(name) orelse return default_value;
    if (raw.len == 0) return default_value;
    return std.fmt.parseUnsigned(usize, raw, 10) catch default_value;
}

pub fn readEnvU64(name: [:0]const u8, default_value: u64) u64 {
    const raw = platform.env.getenvSlice(name) orelse return default_value;
    if (raw.len == 0) return default_value;
    return std.fmt.parseUnsigned(u64, raw, 10) catch default_value;
}

pub fn readOptionalEnvUsize(name: [:0]const u8) ?usize {
    const raw = platform.env.getenvSlice(name) orelse return null;
    if (raw.len == 0) return null;
    return std.fmt.parseUnsigned(usize, raw, 10) catch null;
}

pub fn profileDelta(after: u64, before: u64) u64 {
    return after -| before;
}

pub fn indexStatusKeyAlloc(alloc: std.mem.Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ index_status_prefix, index_name });
}

pub fn encodeIndexStatusSnapshot(status_snapshot: IndexStatusSnapshot, out: *[index_status_encoded_len]u8) void {
    var offset: usize = 0;
    inline for (.{
        index_status_magic,
        @as(u64, @intFromEnum(status_snapshot.kind)),
        status_snapshot.doc_count,
        status_snapshot.term_count,
        status_snapshot.edge_count,
        status_snapshot.node_count,
        status_snapshot.root_node,
        status_snapshot.updated_at_ns,
    }) |value| {
        std.mem.writeInt(u64, out[offset..][0..8], value, .little);
        offset += 8;
    }
}

pub fn decodeIndexStatusSnapshot(raw: []const u8) !IndexStatusSnapshot {
    if (raw.len != index_status_encoded_len) return error.InvalidIndexStatusSnapshot;
    var offset: usize = 0;
    const magic = std.mem.readInt(u64, raw[offset..][0..8], .little);
    offset += 8;
    if (magic != index_status_magic) return error.InvalidIndexStatusSnapshot;
    const kind_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
    offset += 8;
    const kind: types.IndexKind = switch (kind_raw) {
        @intFromEnum(types.IndexKind.full_text) => .full_text,
        @intFromEnum(types.IndexKind.dense_vector) => .dense_vector,
        @intFromEnum(types.IndexKind.sparse_vector) => .sparse_vector,
        @intFromEnum(types.IndexKind.graph) => .graph,
        @intFromEnum(types.IndexKind.algebraic) => .algebraic,
        else => return error.InvalidIndexStatusSnapshot,
    };
    return .{
        .kind = kind,
        .doc_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .term_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .edge_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .node_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .root_node = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .updated_at_ns = std.mem.readInt(u64, raw[offset..][0..8], .little),
    };
}

pub fn collectLiveIndexStatusSnapshot(index_manager: *index_manager_mod.IndexManager, index_name: []const u8) ?IndexStatusSnapshot {
    if (index_manager.textIndex(index_name)) |entry| {
        const text_snapshot = entry.snapshot();
        const term_count = textIndexTermCount(entry);
        return .{
            .kind = .full_text,
            .doc_count = text_snapshot.global_doc_count,
            .term_count = term_count,
            .updated_at_ns = platform.time.monotonicNs(),
        };
    }
    if (index_manager.denseIndex(index_name)) |entry| {
        const dense_stats = entry.index.stats();
        return .{
            .kind = .dense_vector,
            .doc_count = dense_stats.active_count,
            .node_count = dense_stats.node_count,
            .root_node = dense_stats.root_node,
            .updated_at_ns = platform.time.monotonicNs(),
        };
    }
    if (index_manager.sparseIndex(index_name)) |entry| {
        const sparse_stats = entry.index.stats();
        return .{
            .kind = .sparse_vector,
            .doc_count = sparse_stats.doc_count,
            .term_count = sparse_stats.term_count,
            .updated_at_ns = platform.time.monotonicNs(),
        };
    }
    if (index_manager.graphIndex(index_name)) |entry| {
        const graph_stats = entry.index.stats(index_manager.alloc) catch return null;
        return .{
            .kind = .graph,
            .doc_count = graph_stats.node_count,
            .edge_count = graph_stats.edge_count,
            .node_count = graph_stats.node_count,
            .updated_at_ns = platform.time.monotonicNs(),
        };
    }
    return null;
}

pub fn textIndexTermCount(entry: anytype) u64 {
    const snap = entry.acquireSnapshot();
    defer snap.release();
    var terms: u64 = 0;
    for (snap.segments) |*seg| {
        const layout = seg.layoutStats(true);
        terms +|= layout.inverted_one_hit_terms +| layout.inverted_postings_terms;
    }
    return terms;
}

pub fn saveIndexStatusSnapshots(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    updates: []const apply_state.AppliedSequenceUpdate,
) !void {
    if (updates.len == 0) return;

    const PendingStatusWrite = struct {
        key: []u8,
        value: [index_status_encoded_len]u8,
    };
    var pending = std.ArrayListUnmanaged(PendingStatusWrite).empty;
    defer {
        for (pending.items) |item| alloc.free(item.key);
        pending.deinit(alloc);
    }

    for (updates) |update| {
        const status_snapshot = collectLiveIndexStatusSnapshot(index_manager, update.index_name) orelse continue;
        const key = try indexStatusKeyAlloc(alloc, update.index_name);
        var encoded: [index_status_encoded_len]u8 = undefined;
        encodeIndexStatusSnapshot(status_snapshot, &encoded);
        errdefer alloc.free(key);
        try pending.append(alloc, .{
            .key = key,
            .value = encoded,
        });
    }

    if (pending.items.len == 0) return;
    var status_batch = try store.beginWriteBatch();
    errdefer status_batch.abort();
    for (pending.items) |item| try status_batch.put(item.key, &item.value);
    try status_batch.commit();
}

pub fn saveIndexStatusSnapshot(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    index_name: []const u8,
    sequence: u64,
) !void {
    return try saveIndexStatusSnapshots(alloc, store, index_manager, &[_]apply_state.AppliedSequenceUpdate{.{
        .index_name = index_name,
        .sequence = sequence,
    }});
}

pub fn loadIndexStatusSnapshot(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
) !?IndexStatusSnapshot {
    const key = try indexStatusKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return decodeIndexStatusSnapshot(raw) catch null;
}

pub fn applyIndexStatusSnapshot(item: *types.DBIndexStats, status_snapshot: IndexStatusSnapshot) void {
    if (status_snapshot.kind != item.kind) return;
    item.doc_count = status_snapshot.doc_count;
    item.term_count = status_snapshot.term_count;
    item.edge_count = status_snapshot.edge_count;
    item.node_count = status_snapshot.node_count;
    item.root_node = status_snapshot.root_node;
}

pub fn threadedIo() if (builtin.os.tag == .freestanding) void else std.Io.Threaded {
    if (builtin.os.tag == .freestanding) return;
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

const PrimaryStoreOpenPlan = union(enum) {
    lmdb: struct {
        map_size: usize,
        no_sync: bool,
        read_only: bool,
    },
    mem: mem_backend_mod.Options,
    lsm_memory: lsm_backend_mod.Options,
    lsm: lsm_backend_mod.Options,
};

fn primaryStoreOpenPlan(opts: db_config.CoreOpenOptions) PrimaryStoreOpenPlan {
    return switch (opts.primary_backend) {
        .lmdb => .{
            .lmdb = .{
                .map_size = opts.map_size,
                .no_sync = opts.no_sync,
                .read_only = opts.read_only,
            },
        },
        .mem => |mem_opts| .{ .mem = mem_opts },
        .lsm_memory => |lsm_opts| .{ .lsm_memory = db_config.mergedLsmOptions(opts.storage, opts.lsm_cache, opts.resource_manager, opts.no_sync, lsm_opts) },
        .lsm => |lsm_opts| .{ .lsm = db_config.mergedLsmOptions(opts.storage, opts.lsm_cache, opts.resource_manager, opts.no_sync, lsm_opts) },
    };
}

pub fn openPrimaryStore(alloc: Allocator, path: []const u8, opts: db_config.CoreOpenOptions) !db_core.OpenedPrimaryStore {
    if (opts.primary_runtime_store) |runtime_store| {
        return .{
            .store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store),
        };
    }

    const zpath = try alloc.dupeZ(u8, path);
    defer alloc.free(zpath);

    return switch (primaryStoreOpenPlan(opts)) {
        .lmdb => |lmdb_opts| .{
            .store = try docstore_mod.DocStore.open(alloc, zpath, .{
                .map_size = lmdb_opts.map_size,
                .no_sync = lmdb_opts.no_sync,
                .read_only = lmdb_opts.read_only,
            }),
        },
        .mem => |mem_opts| mem_blk: {
            const backend = try alloc.create(mem_backend_mod.Backend);
            errdefer alloc.destroy(backend);
            backend.* = mem_backend_mod.Backend.init(alloc, mem_opts);
            errdefer backend.close();

            var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
            errdefer runtime_store.deinit();

            break :mem_blk .{
                .store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store),
                .owner = .{ .mem = backend },
            };
        },
        .lsm_memory => |lsm_opts| lsm_mem_blk: {
            var handle = try lsm_backend_mod.BackendHandle.init(alloc, lsm_opts);
            errdefer handle.close();

            var runtime_store = try handle.backend.runtimeStore(alloc, .{ .name = "docs" });
            errdefer runtime_store.deinit();

            break :lsm_mem_blk .{
                .store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store),
                .owner = .{ .lsm = .{
                    .handle = handle,
                    .split_options = null,
                } },
            };
        },
        .lsm => |lsm_opts| lsm_blk: {
            var handle = try lsm_backend_mod.BackendHandle.open(alloc, path, lsm_opts);
            errdefer handle.close();

            var runtime_store = try handle.backend.runtimeStore(alloc, .{ .name = "docs" });
            errdefer runtime_store.deinit();

            break :lsm_blk .{
                .store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store),
                .owner = .{ .lsm = .{
                    .handle = handle,
                    .split_options = db_config.splitLsmOptions(opts.primary_backend, opts.storage, opts.lsm_cache),
                } },
            };
        },
    };
}

pub fn spinOrYield() void {
    if (builtin.os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        platform.time.yieldBriefly();
    }
}

pub fn sleepNs(ns: u64) void {
    platform.time.sleepNs(ns);
}

pub fn sleepPollInterval() void {
    sleepNs(10 * std.time.ns_per_ms);
}

pub fn yieldToBackground() void {
    var spins: usize = 0;
    while (spins < 16) : (spins += 1) {
        spinOrYield();
    }
    sleepPollInterval();
}

pub fn waitForAtomicU8(flag: *const std.atomic.Value(u8), expected: u8, max_attempts: usize) bool {
    var attempts: usize = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        if (flag.load(.monotonic) == expected) return true;
        spinOrYield();
    }
    return flag.load(.monotonic) == expected;
}

pub const QueryVisibilityChange = enum {
    invalidate,
    publish,
    publish_consistent,
};

pub const ReplayProgress = struct {
    sequence: u64 = 0,
    target_sequence: u64 = 0,
    scanned_entries: u64 = 0,
    applied_entries: u64 = 0,
    replay_scan_batches: u64 = 0,
    replay_hint_filter_skips: u64 = 0,
    active: bool = false,
};

pub const ReplayProgressHook = *const fn (ctx: *anyopaque, index_name: []const u8, progress: ReplayProgress) anyerror!void;

var bench_metrics_enabled_cache: std.atomic.Value(u8) = .init(0);
var async_index_profile_enabled_cache: std.atomic.Value(u8) = .init(0);

pub fn envBoolEnabled(raw: []const u8) bool {
    return !(std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no"));
}

pub fn waitForCachedBool(cache: *std.atomic.Value(u8)) bool {
    while (true) {
        switch (cache.load(.acquire)) {
            1 => return false,
            2 => return true,
            else => std.atomic.spinLoopHint(),
        }
    }
}

pub fn benchMetricsEnabled() bool {
    const cached = bench_metrics_enabled_cache.load(.monotonic);
    if (cached == 1 or cached == 2) return cached == 2;
    if (cached == 3) return waitForCachedBool(&bench_metrics_enabled_cache);
    if (bench_metrics_enabled_cache.cmpxchgStrong(0, 3, .acq_rel, .monotonic) != null) {
        return waitForCachedBool(&bench_metrics_enabled_cache);
    }
    if (comptime builtin.os.tag == .freestanding) {
        bench_metrics_enabled_cache.store(1, .release);
        return false;
    }
    const raw_z = platform.env.getenv("ANTFLY_BENCH_METRICS") orelse
        platform.env.getenv("ANTFLY_BENCH_BATCH_PROFILE") orelse {
        bench_metrics_enabled_cache.store(1, .release);
        return false;
    };
    const enabled = envBoolEnabled(raw_z);
    bench_metrics_enabled_cache.store(if (enabled) 2 else 1, .release);
    return enabled;
}

pub fn asyncIndexProfileEnabled() bool {
    const cached = async_index_profile_enabled_cache.load(.monotonic);
    if (cached == 1 or cached == 2) return cached == 2;
    if (cached == 3) return waitForCachedBool(&async_index_profile_enabled_cache);
    if (async_index_profile_enabled_cache.cmpxchgStrong(0, 3, .acq_rel, .monotonic) != null) {
        return waitForCachedBool(&async_index_profile_enabled_cache);
    }
    if (comptime builtin.os.tag == .freestanding) {
        async_index_profile_enabled_cache.store(1, .release);
        return false;
    }
    const enabled = if (platform.env.getenv("ANTFLY_ASYNC_INDEX_PROFILE")) |raw_z|
        envBoolEnabled(raw_z)
    else
        benchMetricsEnabled();
    async_index_profile_enabled_cache.store(if (enabled) 2 else 1, .release);
    return enabled;
}

pub fn shouldWriteTimestamp(key: []const u8) bool {
    return !isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key);
}

pub fn isMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        isSplitMetadataKey(key) or
        internal_keys.isTtlKey(key);
}

pub fn isSplitMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "splitstate:") or std.mem.startsWith(u8, key, "splitdelta:");
}

pub fn makeTimestampKey(alloc: Allocator, key: []const u8) ![]u8 {
    return try internal_keys.ttlKeyAlloc(alloc, key);
}

pub fn containsStoreWriteKey(list: []const docstore_mod.KVPair, key: []const u8) bool {
    for (list) |item| {
        if (std.mem.eql(u8, item.key, key)) return true;
    }
    return false;
}

pub const BorrowedGraphMaterializationBatch = struct {
    writes: []docstore_mod.KVPair = &.{},
    deletes: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        self.* = .{};
    }
};

pub fn filterChangedGraphMaterializationBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: []const docstore_mod.KVPair,
    deletes: []const []const u8,
) !BorrowedGraphMaterializationBatch {
    var changed_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    errdefer changed_writes.deinit(alloc);
    for (writes) |write| {
        if (try storeValueDiffers(alloc, store, write.key, write.value)) {
            try changed_writes.append(alloc, write);
        }
    }

    var changed_deletes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer changed_deletes.deinit(alloc);
    for (deletes) |key| {
        if (containsStoreWriteKey(writes, key)) continue;
        if (try storeContainsKey(alloc, store, key)) {
            try changed_deletes.append(alloc, key);
        }
    }

    return .{
        .writes = try changed_writes.toOwnedSlice(alloc),
        .deletes = try changed_deletes.toOwnedSlice(alloc),
    };
}

fn storeValueDiffers(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8, value: []const u8) !bool {
    const existing = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return true,
        else => return err,
    };
    defer alloc.free(existing);
    return !std.mem.eql(u8, existing, value);
}

fn storeContainsKey(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8) !bool {
    const existing = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    alloc.free(existing);
    return true;
}

pub fn ttlTimestampNsFromDocumentValue(
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    value_json: []const u8,
) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return null,
    };
    const ttl_value = root.get(schema.ttl_field) orelse return null;
    if (ttl_value == .null) return null;
    return try ttlTimestampNsFromJsonValue(ttl_value);
}

pub fn ttlTimestampNsFromJsonValue(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| blk: {
            if (integer < 0) return error.InvalidArgument;
            break :blk std.math.cast(u64, integer) orelse return error.InvalidArgument;
        },
        .number_string => |text| std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidArgument,
        .string => |text| ttlTimestampNsFromString(text),
        else => error.InvalidArgument,
    };
}

pub fn ttlTimestampNsFromString(text: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidArgument;
    if (std.fmt.parseInt(u64, trimmed, 10)) |ts| return ts else |_| {}
    if (try parseRfc3339ToNs(trimmed)) |ts| return ts;
    return error.InvalidArgument;
}

pub fn encodeTimestampValue(alloc: Allocator, timestamp_ns: u64) ![]u8 {
    const buf = try alloc.alloc(u8, 8);
    std.mem.writeInt(u64, buf[0..8], timestamp_ns, .little);
    return buf;
}

pub fn currentTimeNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

pub fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

pub fn QueryVisibilityHook(comptime DB: type) type {
    return struct {
        ptr: *anyopaque,
        table_name: []const u8,
        group_id: u64 = 0,
        db: ?*DB = null,
        on_change: *const fn (ptr: *anyopaque, table_name: []const u8, group_id: u64, db: ?*DB, change: QueryVisibilityChange) void,

        pub fn notify(self: @This(), change: QueryVisibilityChange) void {
            self.on_change(self.ptr, self.table_name, self.group_id, self.db, change);
        }
    };
}

pub fn AsyncContext(comptime DB: type) type {
    return struct {
        alloc: Allocator,
        io: ?std.Io = null,
        store: *docstore_mod.DocStore,
        applied_sequence_checkpoint_path: ?[]const u8 = null,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        allow_graph_materialization: bool = true,
        require_graph_resolution_contract: bool = false,
        query_visibility_hook: ?QueryVisibilityHook(DB) = null,
        text_merge_deferred: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        applied_sequence_mutex: std.atomic.Mutex = .unlocked,
        dense_finish_mutex: std.atomic.Mutex = .unlocked,
        active_dense_catch_up_sessions: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        active_external_dense_bulk_sessions: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        deferred_external_bulk_notify_sequence: AtomicU64 = AtomicU64.init(0),
        dense_bulk_session_scope: DenseBulkSessionScope = .auto,
        dense_maintenance_last_ns: std.StringHashMapUnmanaged(u64) = .empty,
        text_merge_runtime: ?*text_merge_runtime_mod.TextMergeRuntime = null,
        sparse_compaction_runtime: ?*sparse_compaction_runtime_mod.SparseCompactionRuntime = null,
        relational_base_rows: bool = false,
        resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
        promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
        applied_sequence_coalescer: AppliedSequenceCoalescer = .{},
        stats: AsyncContentionStats = .{},

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            self.applied_sequence_coalescer.deinit(alloc);
            var maintenance_it = self.dense_maintenance_last_ns.iterator();
            while (maintenance_it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
            self.dense_maintenance_last_ns.deinit(alloc);
        }
    };
}

pub fn BatchExecutionContext(comptime DB: type) type {
    return struct {
        alloc: Allocator,
        io: ?std.Io = null,
        store: *docstore_mod.DocStore,
        applied_sequence_checkpoint_path: ?[]const u8,
        shard_manager: *shard_mod.ShardManager,
        change_journal: *change_journal_mod.Journal,
        replay_source: replay_source_mod.Source,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        log_mutex: *std.atomic.Mutex,
        identity_namespace: doc_identity.Namespace,
        artifact_cleanup_maybe: ?*std.atomic.Value(bool) = null,
        executor: *derived_executor_mod.Executor,
        enrichment_runtime: ?*enrichment_runtime_mod.EnrichmentRuntime,
        resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
        promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
        async_context: ?*AsyncContext(DB) = null,
        dense_bulk_session_scope: DenseBulkSessionScope = .auto,
        relational_base_rows: bool = false,
        ha_async_effect_mirror: ?ha_types.AsyncEffectMirror = null,
        ha_async_batch_mirror: ?ha_types.AsyncBatchMirror = null,
        ha_async_metadata_mirror: ?ha_types.AsyncMetadataMirror = null,
        ha_write_gate: ?ha_types.WriteGate = null,
    };
}

pub fn EnrichmentAppendContext(comptime DB: type) type {
    return struct {
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        applied_sequence_checkpoint_path: ?[]const u8,
        shard_manager: *shard_mod.ShardManager,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        change_journal: *change_journal_mod.Journal,
        replay_source: replay_source_mod.Source,
        executor: *derived_executor_mod.Executor,
        async_context: ?*AsyncContext(DB),
        log_mutex: *std.atomic.Mutex,
        ha_async_effect_mirror: ?ha_types.AsyncEffectMirror = null,
        ha_async_batch_mirror: ?ha_types.AsyncBatchMirror = null,
        ha_async_metadata_mirror: ?ha_types.AsyncMetadataMirror = null,
        ha_write_gate: ?ha_types.WriteGate = null,
        resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
        promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,

        pub fn batchContext(self: *const @This()) BatchExecutionContext(DB) {
            return .{
                .alloc = self.alloc,
                .store = self.store,
                .applied_sequence_checkpoint_path = self.applied_sequence_checkpoint_path,
                .shard_manager = self.shard_manager,
                .change_journal = self.change_journal,
                .replay_source = self.replay_source,
                .index_manager = self.index_manager,
                .apply_mutex = self.apply_mutex,
                .log_mutex = self.log_mutex,
                .identity_namespace = doc_identity.default_namespace,
                .artifact_cleanup_maybe = null,
                .executor = self.executor,
                .io = if (self.async_context) |ctx| ctx.io else null,
                .async_context = self.async_context,
                .ha_async_effect_mirror = self.ha_async_effect_mirror,
                .ha_async_batch_mirror = self.ha_async_batch_mirror,
                .ha_async_metadata_mirror = self.ha_async_metadata_mirror,
                .ha_write_gate = self.ha_write_gate,
                .enrichment_runtime = null,
                .resolution_runtime = self.resolution_runtime,
                .promotion_runtime = self.promotion_runtime,
            };
        }
    };
}

pub fn TtlCleanupContext(comptime DB: type) type {
    return struct {
        batch: BatchExecutionContext(DB),
        grace_period_ns: u64,
    };
}

pub fn ReplayApplyContext(comptime DB: type) type {
    return struct {
        db: *DB,
        dense_bulk_session_scope: DenseBulkSessionScope = .auto,
    };
}

pub fn ReplayApplyContextBatch(comptime DB: type) type {
    return struct {
        batch: *const BatchExecutionContext(DB),
        dense_bulk_session_scope: DenseBulkSessionScope = .auto,
    };
}

pub fn decodeArtifactRefIfKnownAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => null,
        else => return err,
    };
}

pub fn appendUniqueOwnedKey(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8), key: []const u8) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, key)) return;
    }
    try list.append(alloc, try alloc.dupe(u8, key));
}

pub fn containsKey(list: []const []const u8, key: []const u8) bool {
    for (list) |existing| {
        if (std.mem.eql(u8, existing, key)) return true;
    }
    return false;
}

pub fn putLeakyJsonStringField(alloc: Allocator, obj: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try obj.put(alloc, key, .{ .string = try alloc.dupe(u8, value) });
}

pub fn putLeakyJsonU64Field(alloc: Allocator, obj: *std.json.ObjectMap, key: []const u8, value: u64) !void {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidArgument;
    try obj.put(alloc, key, .{ .integer = @intCast(value) });
}

pub fn jsonValueAtPath(root: std.json.Value, field_name: []const u8) ?std.json.Value {
    if (field_name.len == 0) return null;
    var current = root;
    var it = std.mem.splitScalar(u8, field_name, '.');
    while (it.next()) |segment| {
        if (segment.len == 0) return null;
        switch (current) {
            .object => |object| current = object.get(segment) orelse return null,
            else => return null,
        }
    }
    return current;
}

pub fn assetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
}

pub fn mentionGraphStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}\x1fresolution_mentions\x1f{s}", .{ source_artifact, resolution_artifact });
}

pub fn batchDocumentValueForGraphSource(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    store_writes: []const docstore_mod.KVPair,
    doc_key: []const u8,
    relational_base_rows: bool,
) !?[]u8 {
    const internal_doc_key = try graphSourceDocumentStoreKeyAlloc(alloc, doc_key, relational_base_rows);
    defer alloc.free(internal_doc_key);
    for (store_writes) |write| {
        if (std.mem.eql(u8, write.key, internal_doc_key)) {
            return if (relational_base_rows)
                try mapper.materializeRelationalRowValueAlloc(alloc, write.value)
            else
                try mapper.materializeDocumentValueAlloc(alloc, write.value);
        }
    }
    return try storeDocumentValueForGraphSource(alloc, store, doc_key, relational_base_rows);
}

pub fn storeDocumentValueForGraphSource(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    relational_base_rows: bool,
) !?[]u8 {
    const internal_doc_key = try graphSourceDocumentStoreKeyAlloc(alloc, doc_key, relational_base_rows);
    defer alloc.free(internal_doc_key);
    const raw = store.get(alloc, internal_doc_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    return if (raw) |value|
        if (relational_base_rows)
            try mapper.materializeOwnedRelationalRowValueAlloc(alloc, value)
        else
            try mapper.materializeOwnedDocumentValueAlloc(alloc, value)
    else
        null;
}

pub fn graphSourceDocumentStoreKeyAlloc(alloc: Allocator, doc_key: []const u8, relational_base_rows: bool) ![]u8 {
    return if (relational_base_rows)
        try relational_store_mod.rowKeyAlloc(alloc, doc_key)
    else
        try internal_keys.documentKeyAlloc(alloc, doc_key);
}

pub fn graphArtifactContentType(index_manager: *const index_manager_mod.IndexManager, artifact_name: []const u8) []const u8 {
    const cfg = index_manager.getEnrichment(.asset, artifact_name) orelse return "";
    return cfg.content_type;
}

pub fn graphAssetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.graph_asset_state_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, index_name);
    try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
}

pub fn encodeGraphAssetStateKeysAlloc(alloc: Allocator, writes: []const docstore_mod.KVPair) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendU32Big(&out, alloc, @intCast(writes.len));
    for (writes) |write| {
        try appendU32Big(&out, alloc, @intCast(write.key.len));
        try out.appendSlice(alloc, write.key);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn loadGraphAssetStateKeysAlloc(alloc: Allocator, store: *docstore_mod.DocStore, state_key: []const u8) !?[][]const u8 {
    const raw = store.get(alloc, state_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var pos: usize = 0;
    const count = readU32Big(raw, &pos) catch return null;
    const keys = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(keys);
    }
    for (keys) |*key| {
        const len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (len > raw.len - pos) return error.InvalidGraphAssetState;
        key.* = try alloc.dupe(u8, raw[pos..][0..len]);
        pos += len;
        initialized += 1;
    }
    return keys;
}

pub fn collectGraphArtifactsForDocIndex(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    index_name: []const u8,
) ![]docstore_mod.OwnedKVPair {
    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, doc_key, index_name);
    defer alloc.free(prefix);
    return try store.scanPrefix(alloc, prefix);
}

pub fn freeOwnedConstKeySlice(alloc: Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    if (keys.len > 0) alloc.free(keys);
}

fn readU32Big(bytes: []const u8, pos: *usize) !u32 {
    if (bytes.len - pos.* < @sizeOf(u32)) return error.EndOfStream;
    const value = std.mem.readInt(u32, bytes[pos.*..][0..@sizeOf(u32)], .big);
    pos.* += @sizeOf(u32);
    return value;
}

fn appendU32Big(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: u32) !void {
    const be = std.mem.nativeToBig(u32, value);
    try out.appendSlice(alloc, std.mem.asBytes(&be));
}

pub fn isBaseDocumentStoreKeyForMode(relational_base_rows: bool, key: []const u8) bool {
    return if (relational_base_rows)
        internal_keys.isRelationalRowKey(key)
    else
        internal_keys.isPrimaryDocumentKey(key);
}

pub fn buildOverwrittenDocKeys(
    alloc: Allocator,
    writes: []const types.BatchWrite,
    overwritten_flags: []const bool,
) ![]const []const u8 {
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }

    for (writes, 0..) |write, i| {
        if (!overwritten_flags[i]) continue;
        try keys.append(alloc, try alloc.dupe(u8, write.key));
    }

    return try keys.toOwnedSlice(alloc);
}

pub const ManagedSyncTargets = struct {
    full_text_indexes: []const []const u8 = &.{},
    all_indexes: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.full_text_indexes) |name| alloc.free(@constCast(name));
        if (self.full_text_indexes.len > 0) alloc.free(self.full_text_indexes);
        for (self.all_indexes) |name| alloc.free(@constCast(name));
        if (self.all_indexes.len > 0) alloc.free(self.all_indexes);
        self.* = undefined;
    }
};

pub fn cloneManagedSyncTargetsAll(
    alloc: Allocator,
    index_names: []const []const u8,
) !ManagedSyncTargets {
    var full_text_indexes = try alloc.alloc([]const u8, index_names.len);
    var initialized_full_text: usize = 0;
    errdefer {
        for (full_text_indexes[0..initialized_full_text]) |name| alloc.free(@constCast(name));
        if (full_text_indexes.len > 0) alloc.free(full_text_indexes);
    }
    for (index_names, 0..) |name, i| {
        full_text_indexes[i] = try alloc.dupe(u8, name);
        initialized_full_text += 1;
    }

    var all_indexes = try alloc.alloc([]const u8, index_names.len);
    var initialized_all: usize = 0;
    errdefer {
        for (all_indexes[0..initialized_all]) |name| alloc.free(@constCast(name));
        if (all_indexes.len > 0) alloc.free(all_indexes);
    }
    for (index_names, 0..) |name, i| {
        all_indexes[i] = try alloc.dupe(u8, name);
        initialized_all += 1;
    }

    return .{
        .full_text_indexes = full_text_indexes,
        .all_indexes = all_indexes,
    };
}

test "cloneManagedSyncTargetsAll duplicates names independently" {
    const alloc = std.testing.allocator;

    var targets = try cloneManagedSyncTargetsAll(alloc, &.{ "ft_a", "ft_b" });
    defer targets.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), targets.full_text_indexes.len);
    try std.testing.expectEqual(@as(usize, 2), targets.all_indexes.len);
    try std.testing.expectEqualStrings("ft_a", targets.full_text_indexes[0]);
    try std.testing.expectEqualStrings("ft_b", targets.full_text_indexes[1]);
    try std.testing.expectEqualStrings("ft_a", targets.all_indexes[0]);
    try std.testing.expectEqualStrings("ft_b", targets.all_indexes[1]);
    try std.testing.expect(@intFromPtr(targets.full_text_indexes.ptr) != @intFromPtr(targets.all_indexes.ptr));
}

pub fn beginDenseCatchUpSessionTracked(ctx: anytype, index_name: []const u8) !void {
    _ = index_name;
    ctx.text_merge_deferred.store(true, .release);
    ctx.stats.dense_catch_up.active.store(1, .monotonic);
    ctx.stats.dense_catch_up.phase.store(@intFromEnum(types.DenseCatchUpStats.Phase.replay), .monotonic);
    _ = ctx.active_dense_catch_up_sessions.fetchAdd(1, .monotonic);
}

pub fn finishDenseCatchUpSessionTracked(ctx: anytype, index_name: []const u8) void {
    var active = ctx.active_dense_catch_up_sessions.load(.monotonic);
    while (active != 0) {
        if (ctx.active_dense_catch_up_sessions.cmpxchgWeak(active, active - 1, .monotonic, .monotonic) == null) {
            if (active == 1) {
                ctx.stats.dense_catch_up.active.store(0, .monotonic);
                ctx.stats.dense_catch_up.phase.store(@intFromEnum(types.DenseCatchUpStats.Phase.idle), .monotonic);
                ctx.stats.dense_catch_up.bulk_finish_current_window.store(0, .monotonic);
                ctx.stats.dense_catch_up.bulk_finish_current_window_split_steps.store(0, .monotonic);
                ctx.stats.dense_catch_up.bulk_finish_deferred_leaf_splits.store(0, .monotonic);
                ctx.stats.dense_catch_up.bulk_finish_current_window_ns.store(0, .monotonic);
            }
            resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            return;
        }
        active = ctx.active_dense_catch_up_sessions.load(.monotonic);
    }
    std.log.warn("dense catch-up session finish without active session index={s}", .{index_name});
}

pub fn beginExternalDenseBulkSessionTracked(ctx: anytype) void {
    ctx.text_merge_deferred.store(true, .release);
    _ = ctx.active_external_dense_bulk_sessions.fetchAdd(1, .release);
}

pub fn finishExternalDenseBulkSessionTracked(ctx: anytype) void {
    var active = ctx.active_external_dense_bulk_sessions.load(.acquire);
    while (active != 0) {
        if (ctx.active_external_dense_bulk_sessions.cmpxchgWeak(active, active - 1, .acq_rel, .acquire) == null) {
            resumeDeferredBackgroundMaintenanceIfIdle(ctx);
            return;
        }
        active = ctx.active_external_dense_bulk_sessions.load(.acquire);
    }
    std.log.warn("dense external bulk session finish without active session", .{});
}

pub fn asyncContextHasActiveDenseBulkWork(ctx: anytype) bool {
    return ctx.active_dense_catch_up_sessions.load(.acquire) != 0 or
        ctx.active_external_dense_bulk_sessions.load(.acquire) != 0;
}

pub fn resumeDeferredBackgroundMaintenanceIfIdle(ctx: anytype) void {
    if (asyncContextHasActiveDenseBulkWork(ctx)) return;
    const deferred = ctx.text_merge_deferred.swap(false, .acq_rel);
    if (deferred) if (ctx.text_merge_runtime) |runtime| runtime.notify();
}

pub fn deferExternalBulkExecutorNotification(ctx: anytype, sync_level: types.SyncLevel, sequence: u64) bool {
    switch (sync_level) {
        .propose, .write, .enrichments => {},
        .full_text, .aknn, .full_index => return false,
    }
    if (ctx.active_external_dense_bulk_sessions.load(.acquire) == 0) return false;
    storeMaxAtomicU64(&ctx.deferred_external_bulk_notify_sequence, sequence);
    return true;
}

pub fn notifyExecutorForSyncLevelWithDenseBulkDeferral(
    async_context: anytype,
    executor: *derived_executor_mod.Executor,
    sync_level: types.SyncLevel,
    sequence: u64,
    sync_targets: ManagedSyncTargets,
) void {
    const deferred = switch (@typeInfo(@TypeOf(async_context))) {
        .optional => if (async_context) |ctx|
            deferExternalBulkExecutorNotification(ctx, sync_level, sequence)
        else
            false,
        .pointer => deferExternalBulkExecutorNotification(async_context, sync_level, sequence),
        else => false,
    };
    if (deferred) {
        executor.notifyExceptKind(sequence, .dense_vector);
        return;
    }
    notifyExecutorForSyncLevel(executor, sync_level, sequence, sync_targets);
}

pub fn notifyExecutorForSyncLevel(
    executor: *derived_executor_mod.Executor,
    sync_level: types.SyncLevel,
    sequence: u64,
    sync_targets: ManagedSyncTargets,
) void {
    switch (sync_level) {
        .full_text => executor.notifyIndexes(sequence, sync_targets.full_text_indexes),
        .propose, .write, .enrichments, .aknn, .full_index => executor.notifySequence(sequence),
    }
}

pub fn flushDeferredExternalBulkExecutorNotification(ctx: anytype, executor: *derived_executor_mod.Executor) void {
    const sequence = ctx.deferred_external_bulk_notify_sequence.swap(0, .acq_rel);
    if (sequence == 0) return;
    if (!executor.hasWorkers()) return;
    executor.notifySequence(sequence);
}

pub fn flushDeferredExternalBulkExecutorNotificationOrTarget(
    ctx: anytype,
    executor: *derived_executor_mod.Executor,
    target_sequence: u64,
) void {
    const deferred_sequence = ctx.deferred_external_bulk_notify_sequence.swap(0, .acq_rel);
    const sequence = @max(deferred_sequence, target_sequence);
    if (sequence == 0) return;
    if (!executor.hasWorkers()) return;
    executor.notifySequence(sequence);
}

pub fn denseApplyUsesLocalBulkSession(ctx: anytype, index_name: []const u8) bool {
    _ = index_name;
    if (ctx.dense_bulk_session_scope == .external) return false;
    if (ctx.active_external_dense_bulk_sessions.load(.acquire) != 0) return false;
    if (ctx.active_dense_catch_up_sessions.load(.acquire) != 0) return false;
    return true;
}

pub fn shouldDeferAppliedSequenceFlush(ctx: anytype, force: bool) bool {
    if (force) return false;
    return ctx.active_dense_catch_up_sessions.load(.monotonic) != 0;
}

test "async context dense catch-up session tracking suppresses local bulk sessions" {
    const TestDB = struct {};
    const TestAsyncContext = AsyncContext(TestDB);
    var apply_mutex: apply_rw_lock_mod.ApplyRwLock = .{};
    var ctx = TestAsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = &apply_mutex,
    };
    defer ctx.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 0), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(!shouldDeferAppliedSequenceFlush(&ctx, false));
    try std.testing.expect(denseApplyUsesLocalBulkSession(&ctx, "vec"));
    try beginDenseCatchUpSessionTracked(&ctx, "vec");
    try std.testing.expectEqual(@as(u32, 1), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) != 0);
    try std.testing.expect(asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expect(ctx.text_merge_deferred.load(.acquire));
    try std.testing.expect(shouldDeferAppliedSequenceFlush(&ctx, false));
    try std.testing.expect(!shouldDeferAppliedSequenceFlush(&ctx, true));
    try std.testing.expect(!denseApplyUsesLocalBulkSession(&ctx, "vec"));
    finishDenseCatchUpSessionTracked(&ctx, "vec");
    try std.testing.expectEqual(@as(u32, 0), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) == 0);
    try std.testing.expect(!ctx.text_merge_deferred.load(.acquire));
    try std.testing.expect(!shouldDeferAppliedSequenceFlush(&ctx, false));
    try std.testing.expect(denseApplyUsesLocalBulkSession(&ctx, "vec"));

    beginExternalDenseBulkSessionTracked(&ctx);
    try std.testing.expectEqual(@as(u32, 1), ctx.active_external_dense_bulk_sessions.load(.monotonic));
    try std.testing.expect(asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expect(ctx.text_merge_deferred.load(.acquire));
    try std.testing.expect(!denseApplyUsesLocalBulkSession(&ctx, "vec"));
    try std.testing.expect(deferExternalBulkExecutorNotification(&ctx, .write, 7));
    try std.testing.expectEqual(@as(u64, 7), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    try std.testing.expect(deferExternalBulkExecutorNotification(&ctx, .propose, 11));
    try std.testing.expectEqual(@as(u64, 11), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    try std.testing.expect(deferExternalBulkExecutorNotification(&ctx, .enrichments, 13));
    try std.testing.expectEqual(@as(u64, 13), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    try std.testing.expect(!deferExternalBulkExecutorNotification(&ctx, .full_text, 17));
    try std.testing.expectEqual(@as(u64, 13), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    try std.testing.expect(!deferExternalBulkExecutorNotification(&ctx, .aknn, 19));
    try std.testing.expectEqual(@as(u64, 13), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    try std.testing.expect(!deferExternalBulkExecutorNotification(&ctx, .full_index, 23));
    try std.testing.expectEqual(@as(u64, 13), ctx.deferred_external_bulk_notify_sequence.load(.monotonic));
    finishExternalDenseBulkSessionTracked(&ctx);
    try std.testing.expectEqual(@as(u32, 0), ctx.active_external_dense_bulk_sessions.load(.monotonic));
    try std.testing.expect(!asyncContextHasActiveDenseBulkWork(&ctx));
    try std.testing.expect(!ctx.text_merge_deferred.load(.acquire));
    try std.testing.expect(denseApplyUsesLocalBulkSession(&ctx, "vec"));
    try std.testing.expect(!deferExternalBulkExecutorNotification(&ctx, .write, 13));

    ctx.dense_bulk_session_scope = .external;
    try std.testing.expect(!denseApplyUsesLocalBulkSession(&ctx, "vec"));
}

test "async context dense catch-up session finish is idempotent when already closed" {
    const TestDB = struct {};
    const TestAsyncContext = AsyncContext(TestDB);
    var apply_mutex: apply_rw_lock_mod.ApplyRwLock = .{};
    var ctx = TestAsyncContext{
        .alloc = std.testing.allocator,
        .store = undefined,
        .index_manager = undefined,
        .apply_mutex = &apply_mutex,
    };
    defer ctx.deinit(std.testing.allocator);

    finishDenseCatchUpSessionTracked(&ctx, "vec");
    try std.testing.expectEqual(@as(u32, 0), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) == 0);

    try beginDenseCatchUpSessionTracked(&ctx, "vec");
    try beginDenseCatchUpSessionTracked(&ctx, "vec");
    finishDenseCatchUpSessionTracked(&ctx, "vec");
    try std.testing.expectEqual(@as(u32, 1), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) != 0);
    finishDenseCatchUpSessionTracked(&ctx, "vec");
    finishDenseCatchUpSessionTracked(&ctx, "vec");
    try std.testing.expectEqual(@as(u32, 0), ctx.active_dense_catch_up_sessions.load(.monotonic));
    try std.testing.expect(ctx.stats.dense_catch_up.active.load(.monotonic) == 0);
    try std.testing.expect(denseApplyUsesLocalBulkSession(&ctx, "vec"));
}

fn storeMaxAtomicU64(value: *AtomicU64, candidate: u64) void {
    var current = value.load(.acquire);
    while (candidate > current) {
        if (value.cmpxchgWeak(current, candidate, .acq_rel, .acquire) == null) return;
        current = value.load(.acquire);
    }
}

pub fn dupeConstDocIdsAlloc(alloc: Allocator, doc_ids: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, doc_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(out);
    }
    for (doc_ids, 0..) |doc_id, i| {
        out[i] = try alloc.dupe(u8, doc_id);
        initialized += 1;
    }
    return out;
}

pub fn docIdsForOrdinalsTxnAlloc(alloc: Allocator, txn: anytype, ordinals: []const doc_set.DocOrdinal) ![]const []const u8 {
    return (try docIdsForOrdinalsAtGenerationTxnAlloc(alloc, txn, ordinals, null)) orelse error.InvalidDocIdentity;
}

pub fn docIdsForOrdinalsAtGenerationTxnAlloc(
    alloc: Allocator,
    txn: anytype,
    ordinals: []const doc_set.DocOrdinal,
    generation: ?u64,
) !?[]const []const u8 {
    const out = try alloc.alloc([]const u8, ordinals.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(out);
    }
    for (ordinals, 0..) |ordinal, i| {
        if (try doc_identity.lookupStateTxn(txn, ordinal)) |state| {
            const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
            if (!visible) {
                for (out[0..initialized]) |doc_id| alloc.free(@constCast(doc_id));
                alloc.free(out);
                return null;
            }
        } else {
            for (out[0..initialized]) |doc_id| alloc.free(@constCast(doc_id));
            alloc.free(out);
            return null;
        }
        out[i] = (try doc_identity.lookupDocIdTxn(alloc, txn, ordinal)) orelse return error.InvalidDocIdentity;
        initialized += 1;
    }
    return out;
}

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn notifyAsyncContextVisibilityHook(ptr: *anyopaque) void {
            const ctx: *AsyncContext(DB) = @ptrCast(@alignCast(ptr));
            if (ctx.query_visibility_hook) |hook| hook.notify(.invalidate);
        }

        pub fn lockApply(db: *DB) void {
            db.core.lockApply();
        }

        pub fn resolveWriteTimestampNs(self: *DB, fallback_timestamp_ns: u64, value_json: []const u8) !u64 {
            const schema = self.core.schema orelse return fallback_timestamp_ns;
            if (schema.ttl_duration_ns == 0) return fallback_timestamp_ns;
            return (try ttlTimestampNsFromDocumentValue(self.alloc, schema, value_json)) orelse fallback_timestamp_ns;
        }

        pub fn ttlDurationNs(self: *DB) u64 {
            return if (self.core.schema) |schema| schema.ttl_duration_ns else 0;
        }

        pub fn isExpiredDocumentKey(self: *DB, alloc: Allocator, key: []const u8) !bool {
            const duration_ns = Self.ttlDurationNs(self);
            if (duration_ns == 0) return false;
            const ts = try self.getTimestamp(alloc, key);
            if (ts == 0) return false;
            return ttl_mod.isExpired(ts, duration_ns, currentTimeNs());
        }

        pub fn hasActiveDenseBulkWork(self: *const DB) bool {
            return asyncContextHasActiveDenseBulkWork(self.async_context);
        }

        pub fn batchContext(self: *DB) BatchExecutionContext(DB) {
            const resources = self.core.batchExecutionResources();
            return .{
                .alloc = self.alloc,
                .store = resources.store,
                .applied_sequence_checkpoint_path = resources.applied_sequence_checkpoint_path,
                .shard_manager = resources.shard_manager,
                .change_journal = resources.change_journal,
                .replay_source = resources.replay_source,
                .index_manager = resources.index_manager,
                .apply_mutex = resources.apply_mutex,
                .log_mutex = resources.log_mutex,
                .identity_namespace = resources.identity_namespace,
                .artifact_cleanup_maybe = resources.artifact_cleanup_maybe,
                .executor = self.executor,
                .io = self.backend_runtime.io(),
                .enrichment_runtime = self.enrichment_runtime,
                .resolution_runtime = self.resolution_runtime,
                .promotion_runtime = self.promotion_runtime,
                .async_context = self.async_context,
                .relational_base_rows = self.relationalColumnsForStore() != null,
                .ha_async_effect_mirror = self.ha_async_effect_mirror,
                .ha_async_batch_mirror = self.ha_async_batch_mirror,
                .ha_async_metadata_mirror = self.ha_async_metadata_mirror,
                .ha_write_gate = self.ha_write_gate,
            };
        }

        pub fn resolveDocSetForIdsAlloc(self: *DB, alloc: Allocator, doc_ids: []const []const u8) !doc_set.ResolvedDocSet {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try Self.resolveDocSetForIdsNoLockAlloc(self, alloc, doc_ids);
        }

        pub fn resolveDocSetForIdsNoLockAlloc(self: *DB, alloc: Allocator, doc_ids: []const []const u8) !doc_set.ResolvedDocSet {
            return try Self.resolveDocSetForIdsNoLockAtGenerationAlloc(self, alloc, doc_ids, null);
        }

        pub fn resolveDocSetForIdsNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            const resolved = try doc_identity.resolvedDocSetForIdsAtGenerationTxn(alloc, &txn, doc_ids, generation);
            Self.recordResolvedDocSet(self, &resolved, doc_ids.len > 0 and switch (resolved) {
                .doc_keys => true,
                else => false,
            });
            return resolved;
        }

        pub fn resolvedDocFilterForIdsAlloc(
            self: *DB,
            include_positive: bool,
            include_doc_ids: []const []const u8,
            exclude_doc_ids: []const []const u8,
            generation: ?u64,
        ) !doc_set.ResolvedDocFilter {
            var filter = doc_set.ResolvedDocFilter{};
            errdefer filter.deinit(self.alloc);
            if (include_positive) {
                filter.include = try Self.resolveDocSetForIdsNoLockAtGenerationAlloc(self, self.alloc, include_doc_ids, generation);
            }
            if (exclude_doc_ids.len > 0) {
                filter.exclude = try Self.resolveDocSetForIdsNoLockAtGenerationAlloc(self, self.alloc, exclude_doc_ids, generation);
            }
            return filter;
        }

        pub fn docIdsForResolvedDocSetAlloc(self: *DB, alloc: Allocator, set: *const doc_set.ResolvedDocSet) !?[]const []const u8 {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try Self.docIdsForResolvedDocSetNoLockAlloc(self, alloc, set);
        }

        pub fn docIdsForResolvedDocSetNoLockAlloc(self: *DB, alloc: Allocator, set: *const doc_set.ResolvedDocSet) !?[]const []const u8 {
            return try Self.docIdsForResolvedDocSetNoLockAtGenerationAlloc(self, alloc, set, null);
        }

        pub fn docIdsForResolvedDocSetNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) !?[]const []const u8 {
            switch (set.*) {
                .all => return null,
                .none => return try alloc.alloc([]const u8, 0),
                .doc_keys => |keys| return try dupeConstDocIdsAlloc(alloc, keys),
                .ordinals => |ordinals| {
                    var txn = try self.core.store.beginProbeTxn();
                    defer txn.abort();
                    return try docIdsForOrdinalsAtGenerationTxnAlloc(alloc, &txn, ordinals, generation);
                },
                .ordinal_bitmap => |*bitmap| {
                    var txn = try self.core.store.beginProbeTxn();
                    defer txn.abort();
                    var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
                    defer ordinals.deinit(alloc);
                    var iter = bitmap.iterator();
                    while (iter.next()) |ordinal| try ordinals.append(alloc, ordinal);
                    return try docIdsForOrdinalsAtGenerationTxnAlloc(alloc, &txn, ordinals.items, generation);
                },
            }
        }

        pub fn recordResolvedDocSet(self: *DB, set: *const doc_set.ResolvedDocSet, missing_ordinal_coverage: bool) void {
            self.doc_set_planning_stats.recordResolvedSet(set, missing_ordinal_coverage);
        }

        pub fn recordUnsupportedDocSetFilterShape(self: *DB) void {
            self.doc_set_planning_stats.recordUnsupportedFilterShape();
        }

        pub fn allDocsVisibleAtGeneration(self: *DB, generation: ?u64) !bool {
            const bench_profile = platform.env.getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
            const total_start_ns = if (bench_profile) platform.time.monotonicNs() else 0;
            var summary_ns: u64 = 0;
            var stats_ns: u64 = 0;
            const summary_start_ns = if (bench_profile) platform.time.monotonicNs() else 0;
            if (try Self.allDocsVisibleSummaryFastMaybe(self, generation)) |all_visible| {
                if (bench_profile) summary_ns = platform.time.monotonicNs() - summary_start_ns;
                if (bench_profile) {
                    std.log.info(
                        "antfly_bench_visibility_gate total_us={d} summary_us={d} stats_us={d} result={}",
                        .{ (platform.time.monotonicNs() - total_start_ns) / 1000, summary_ns / 1000, stats_ns / 1000, all_visible },
                    );
                }
                if (all_visible) return true;
            }
            if (bench_profile) summary_ns = platform.time.monotonicNs() - summary_start_ns;
            const stats_start_ns = if (bench_profile) platform.time.monotonicNs() else 0;
            const identity_stats = try doc_identity.fullStatsFromStore(self.core.store);
            if (bench_profile) stats_ns = platform.time.monotonicNs() - stats_start_ns;
            const generation_covers_all_creates = if (generation) |at|
                identity_stats.max_created_generation <= at
            else
                true;
            const result = identity_stats.complete and identity_stats.tombstone_ordinals == 0 and generation_covers_all_creates;
            if (bench_profile) {
                std.log.info(
                    "antfly_bench_visibility_gate total_us={d} summary_us={d} stats_us={d} result={}",
                    .{ (platform.time.monotonicNs() - total_start_ns) / 1000, summary_ns / 1000, stats_ns / 1000, result },
                );
            }
            return result;
        }

        pub fn allDocsVisibleSummaryFast(self: *DB, generation: ?u64) !bool {
            return (try Self.allDocsVisibleSummaryFastMaybe(self, generation)) orelse false;
        }

        pub fn allDocsVisibleSummaryFastMaybe(self: *DB, generation: ?u64) !?bool {
            if (self.identity_visibility_summary_cache) |summary| {
                return doc_identity.allVisibleFromSummary(summary, generation);
            }
            return try doc_identity.allVisibleFromSummaryFast(self.core.store, generation);
        }

        pub fn lookupLiveDocOrdinalsNoLock(
            self: *DB,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) ![]?doc_set.DocOrdinal {
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();

            const ordinals = try doc_identity.lookupOrdinalsTxnAlloc(alloc, &txn, doc_ids);
            errdefer alloc.free(ordinals);

            const all_visible = try Self.allDocsVisibleSummaryFast(self, generation);
            if (all_visible) return ordinals;

            for (ordinals) |*maybe_ordinal| {
                const ordinal = maybe_ordinal.* orelse continue;
                const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse {
                    maybe_ordinal.* = null;
                    continue;
                };
                const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
                if (!visible) maybe_ordinal.* = null;
            }
            return ordinals;
        }

        pub fn lookupLiveDocOrdinalNoLock(
            self: *DB,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) !?doc_set.DocOrdinal {
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, doc_id)) orelse return null;
            const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return null;
            const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
            if (!visible) return null;
            return ordinal;
        }

        pub fn annotateSearchHitOrdinalsNoLock(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            hits: []types.SearchHit,
        ) !void {
            for (hits) |*hit| {
                if (hit.doc_ordinal != null) continue;
                hit.doc_ordinal = try Self.lookupLiveDocOrdinalNoLock(self, alloc, hit.id, req.identity_read_generation);
            }
        }

        const Self = @This();
    };
}

pub fn atomicMaxU64(value: *AtomicU64, candidate: u64) void {
    var current = value.load(.monotonic);
    while (candidate > current) {
        current = value.cmpxchgWeak(current, candidate, .monotonic, .monotonic) orelse return;
    }
}

fn monotonicTimeNs() u64 {
    return platform.time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return monotonicTimeNs() - start_ns;
}

pub const ProfiledLock = struct {
    mutex: *std.atomic.Mutex,
    stats: *MutexContentionStats,
    hold_start_ns: u64,
    profiled: bool,

    pub fn unlock(self: *@This()) void {
        if (!self.profiled) {
            self.mutex.unlock();
            self.* = undefined;
            return;
        }
        const hold_ns = elapsedSince(self.hold_start_ns);
        self.mutex.unlock();
        _ = self.stats.hold_ns.fetchAdd(hold_ns, .monotonic);
        atomicMaxU64(&self.stats.max_hold_ns, hold_ns);
        self.* = undefined;
    }
};

pub const ProfiledApplyLock = struct {
    rw_lock: *apply_rw_lock_mod.ApplyRwLock,
    stats: *MutexContentionStats,
    hold_start_ns: u64,
    profiled: bool,

    pub fn unlock(self: *@This()) void {
        if (!self.profiled) {
            self.rw_lock.unlockExclusive();
            self.* = undefined;
            return;
        }
        const hold_ns = elapsedSince(self.hold_start_ns);
        self.rw_lock.unlockExclusive();
        _ = self.stats.hold_ns.fetchAdd(hold_ns, .monotonic);
        atomicMaxU64(&self.stats.max_hold_ns, hold_ns);
        self.* = undefined;
    }
};

pub fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
    _ = platform.sync.lockAtomic(mutex);
}

pub fn lockAtomicWithBackoffProfiled(
    mutex: *std.atomic.Mutex,
    stats: *MutexContentionStats,
    profile_enabled: bool,
) ProfiledLock {
    if (!profile_enabled) {
        lockAtomicWithBackoff(mutex);
        return .{
            .mutex = mutex,
            .stats = stats,
            .hold_start_ns = 0,
            .profiled = false,
        };
    }
    _ = stats.lock_calls.fetchAdd(1, .monotonic);
    if (mutex.tryLock()) {
        return .{
            .mutex = mutex,
            .stats = stats,
            .hold_start_ns = monotonicTimeNs(),
            .profiled = true,
        };
    }

    _ = stats.contended_calls.fetchAdd(1, .monotonic);
    const current_waiters = stats.current_waiters.fetchAdd(1, .monotonic) + 1;
    atomicMaxU64(&stats.max_waiters, current_waiters);
    defer _ = stats.current_waiters.fetchSub(1, .monotonic);

    const wait_start_ns = monotonicTimeNs();
    var attempts: usize = 0;
    var spin_loops: u64 = 0;
    var yield_loops: u64 = 0;
    var sleep_loops: u64 = 0;
    while (!mutex.tryLock()) : (attempts += 1) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            spin_loops += 1;
            continue;
        }
        if (attempts < 64) {
            std.atomic.spinLoopHint();
            spin_loops += 1;
            continue;
        }
        if (attempts < 128) {
            spinOrYield();
            yield_loops += 1;
            continue;
        }
        const backoff_step = @min(attempts - 128, 5);
        const sleep_ns = @min(@as(u64, 50_000) << @intCast(backoff_step), @as(u64, 1_000_000));
        sleepNs(sleep_ns);
        sleep_loops += 1;
    }

    const wait_ns = elapsedSince(wait_start_ns);
    _ = stats.spin_loops.fetchAdd(spin_loops, .monotonic);
    _ = stats.yield_loops.fetchAdd(yield_loops, .monotonic);
    _ = stats.sleep_loops.fetchAdd(sleep_loops, .monotonic);
    _ = stats.wait_ns.fetchAdd(wait_ns, .monotonic);
    atomicMaxU64(&stats.max_wait_ns, wait_ns);
    return .{
        .mutex = mutex,
        .stats = stats,
        .hold_start_ns = monotonicTimeNs(),
        .profiled = true,
    };
}

pub fn lockApplyWithBackoffProfiled(
    rw_lock: *apply_rw_lock_mod.ApplyRwLock,
    stats: *MutexContentionStats,
    profile_enabled: bool,
) ProfiledApplyLock {
    if (!profile_enabled) {
        rw_lock.lockExclusive();
        return .{
            .rw_lock = rw_lock,
            .stats = stats,
            .hold_start_ns = 0,
            .profiled = false,
        };
    }
    _ = stats.lock_calls.fetchAdd(1, .monotonic);
    if (rw_lock.tryLockExclusive()) {
        return .{
            .rw_lock = rw_lock,
            .stats = stats,
            .hold_start_ns = monotonicTimeNs(),
            .profiled = true,
        };
    }

    _ = stats.contended_calls.fetchAdd(1, .monotonic);
    const current_waiters = stats.current_waiters.fetchAdd(1, .monotonic) + 1;
    atomicMaxU64(&stats.max_waiters, current_waiters);
    defer _ = stats.current_waiters.fetchSub(1, .monotonic);

    const wait_start_ns = monotonicTimeNs();
    var attempts: usize = 0;
    var spin_loops: u64 = 0;
    var yield_loops: u64 = 0;
    var sleep_loops: u64 = 0;
    while (!rw_lock.tryLockExclusive()) : (attempts += 1) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            spin_loops += 1;
            continue;
        }
        if (attempts < 64) {
            std.atomic.spinLoopHint();
            spin_loops += 1;
            continue;
        }
        if (attempts < 128) {
            spinOrYield();
            yield_loops += 1;
            continue;
        }
        const backoff_step = @min(attempts - 128, 5);
        const sleep_ns = @min(@as(u64, 50_000) << @intCast(backoff_step), @as(u64, 1_000_000));
        sleepNs(sleep_ns);
        sleep_loops += 1;
    }

    const wait_ns = elapsedSince(wait_start_ns);
    _ = stats.spin_loops.fetchAdd(spin_loops, .monotonic);
    _ = stats.yield_loops.fetchAdd(yield_loops, .monotonic);
    _ = stats.sleep_loops.fetchAdd(sleep_loops, .monotonic);
    _ = stats.wait_ns.fetchAdd(wait_ns, .monotonic);
    atomicMaxU64(&stats.max_wait_ns, wait_ns);
    return .{
        .rw_lock = rw_lock,
        .stats = stats,
        .hold_start_ns = monotonicTimeNs(),
        .profiled = true,
    };
}

pub const DenseBulkSessionScope = enum {
    auto,
    external,
};

pub const DocSetPlanningRuntimeStats = struct {
    resolved_set_count: AtomicU64 = AtomicU64.init(0),
    all_set_count: AtomicU64 = AtomicU64.init(0),
    none_set_count: AtomicU64 = AtomicU64.init(0),
    doc_key_list_count: AtomicU64 = AtomicU64.init(0),
    ordinal_list_count: AtomicU64 = AtomicU64.init(0),
    ordinal_bitmap_count: AtomicU64 = AtomicU64.init(0),
    doc_key_list_docs: AtomicU64 = AtomicU64.init(0),
    ordinal_list_docs: AtomicU64 = AtomicU64.init(0),
    ordinal_bitmap_docs: AtomicU64 = AtomicU64.init(0),
    missing_ordinal_coverage_count: AtomicU64 = AtomicU64.init(0),
    bitmap_promotion_count: AtomicU64 = AtomicU64.init(0),
    unsupported_filter_shape_count: AtomicU64 = AtomicU64.init(0),
    stale_identity_generation_rejection_count: AtomicU64 = AtomicU64.init(0),

    pub fn recordResolvedSet(self: *@This(), set: *const doc_set.ResolvedDocSet, missing_ordinal_coverage: bool) void {
        _ = self.resolved_set_count.fetchAdd(1, .monotonic);
        switch (set.*) {
            .all => _ = self.all_set_count.fetchAdd(1, .monotonic),
            .none => _ = self.none_set_count.fetchAdd(1, .monotonic),
            .doc_keys => |keys| {
                _ = self.doc_key_list_count.fetchAdd(1, .monotonic);
                _ = self.doc_key_list_docs.fetchAdd(@intCast(keys.len), .monotonic);
            },
            .ordinals => |ordinals| {
                _ = self.ordinal_list_count.fetchAdd(1, .monotonic);
                _ = self.ordinal_list_docs.fetchAdd(@intCast(ordinals.len), .monotonic);
            },
            .ordinal_bitmap => |*bitmap| {
                _ = self.ordinal_bitmap_count.fetchAdd(1, .monotonic);
                _ = self.ordinal_bitmap_docs.fetchAdd(@intCast(bitmap.cardinality()), .monotonic);
                _ = self.bitmap_promotion_count.fetchAdd(1, .monotonic);
            },
        }
        if (missing_ordinal_coverage) _ = self.missing_ordinal_coverage_count.fetchAdd(1, .monotonic);
    }

    pub fn recordUnsupportedFilterShape(self: *@This()) void {
        _ = self.unsupported_filter_shape_count.fetchAdd(1, .monotonic);
    }

    pub fn recordStaleIdentityGenerationRejection(self: *@This()) void {
        _ = self.stale_identity_generation_rejection_count.fetchAdd(1, .monotonic);
    }

    pub fn snapshot(self: *@This()) types.DocSetPlanningStats {
        return .{
            .resolved_set_count = self.resolved_set_count.load(.monotonic),
            .all_set_count = self.all_set_count.load(.monotonic),
            .none_set_count = self.none_set_count.load(.monotonic),
            .doc_key_list_count = self.doc_key_list_count.load(.monotonic),
            .ordinal_list_count = self.ordinal_list_count.load(.monotonic),
            .ordinal_bitmap_count = self.ordinal_bitmap_count.load(.monotonic),
            .doc_key_list_docs = self.doc_key_list_docs.load(.monotonic),
            .ordinal_list_docs = self.ordinal_list_docs.load(.monotonic),
            .ordinal_bitmap_docs = self.ordinal_bitmap_docs.load(.monotonic),
            .missing_ordinal_coverage_count = self.missing_ordinal_coverage_count.load(.monotonic),
            .bitmap_promotion_count = self.bitmap_promotion_count.load(.monotonic),
            .unsupported_filter_shape_count = self.unsupported_filter_shape_count.load(.monotonic),
            .stale_identity_generation_rejection_count = self.stale_identity_generation_rejection_count.load(.monotonic),
        };
    }
};

test "doc set planning stats record ordinal bitmap promotion" {
    const alloc = std.testing.allocator;

    var ordinals: [doc_set.bitmap_min_cardinality]doc_set.DocOrdinal = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i);

    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &ordinals);
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinal_bitmap => |*bitmap| try std.testing.expectEqual(@as(usize, doc_set.bitmap_min_cardinality), bitmap.cardinality()),
        else => return error.ExpectedOrdinalBitmapDocSet,
    }

    var stats = DocSetPlanningRuntimeStats{};
    stats.recordResolvedSet(&resolved, false);

    const snapshot = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 1), snapshot.ordinal_bitmap_count);
    try std.testing.expectEqual(@as(u64, doc_set.bitmap_min_cardinality), snapshot.ordinal_bitmap_docs);
    try std.testing.expectEqual(@as(u64, 1), snapshot.bitmap_promotion_count);
}

pub const ForeignKeyRuntimeStats = struct {
    child_write_rejects: AtomicU64 = AtomicU64.init(0),
    parent_delete_rejects: AtomicU64 = AtomicU64.init(0),
    validation_runs: AtomicU64 = AtomicU64.init(0),
    dry_run_runs: AtomicU64 = AtomicU64.init(0),
    repair_runs: AtomicU64 = AtomicU64.init(0),
    scanned_child_rows: AtomicU64 = AtomicU64.init(0),
    referenced_child_rows: AtomicU64 = AtomicU64.init(0),
    scanned_ref_rows: AtomicU64 = AtomicU64.init(0),
    missing_parent_rows: AtomicU64 = AtomicU64.init(0),
    missing_ref_rows: AtomicU64 = AtomicU64.init(0),
    stale_ref_rows: AtomicU64 = AtomicU64.init(0),
    repaired_ref_rows: AtomicU64 = AtomicU64.init(0),
    deleted_stale_ref_rows: AtomicU64 = AtomicU64.init(0),

    pub fn recordChildWriteReject(self: *@This()) void {
        _ = self.child_write_rejects.fetchAdd(1, .monotonic);
    }

    pub fn recordParentDeleteReject(self: *@This()) void {
        _ = self.parent_delete_rejects.fetchAdd(1, .monotonic);
    }

    pub fn recordIntegrityReport(
        self: *@This(),
        mode: relational_store_mod.ForeignKeyIntegrityMode,
        report: relational_store_mod.ForeignKeyIntegrityReport,
    ) void {
        switch (mode) {
            .validate => _ = self.validation_runs.fetchAdd(1, .monotonic),
            .dry_run => _ = self.dry_run_runs.fetchAdd(1, .monotonic),
            .repair => _ = self.repair_runs.fetchAdd(1, .monotonic),
        }
        _ = self.scanned_child_rows.fetchAdd(report.scanned_child_rows, .monotonic);
        _ = self.referenced_child_rows.fetchAdd(report.referenced_child_rows, .monotonic);
        _ = self.scanned_ref_rows.fetchAdd(report.scanned_ref_rows, .monotonic);
        _ = self.missing_parent_rows.fetchAdd(report.missing_parent_rows, .monotonic);
        _ = self.missing_ref_rows.fetchAdd(report.missing_ref_rows, .monotonic);
        _ = self.stale_ref_rows.fetchAdd(report.stale_ref_rows, .monotonic);
        _ = self.repaired_ref_rows.fetchAdd(report.repaired_ref_rows, .monotonic);
        _ = self.deleted_stale_ref_rows.fetchAdd(report.deleted_stale_ref_rows, .monotonic);
    }

    pub fn snapshot(self: *@This()) types.ForeignKeyStats {
        return .{
            .child_write_rejects = self.child_write_rejects.load(.monotonic),
            .parent_delete_rejects = self.parent_delete_rejects.load(.monotonic),
            .validation_runs = self.validation_runs.load(.monotonic),
            .dry_run_runs = self.dry_run_runs.load(.monotonic),
            .repair_runs = self.repair_runs.load(.monotonic),
            .scanned_child_rows = self.scanned_child_rows.load(.monotonic),
            .referenced_child_rows = self.referenced_child_rows.load(.monotonic),
            .scanned_ref_rows = self.scanned_ref_rows.load(.monotonic),
            .missing_parent_rows = self.missing_parent_rows.load(.monotonic),
            .missing_ref_rows = self.missing_ref_rows.load(.monotonic),
            .stale_ref_rows = self.stale_ref_rows.load(.monotonic),
            .repaired_ref_rows = self.repaired_ref_rows.load(.monotonic),
            .deleted_stale_ref_rows = self.deleted_stale_ref_rows.load(.monotonic),
        };
    }
};

pub const applied_sequence_flush_interval_ns: u64 = 100 * std.time.ns_per_ms;

pub const AppliedSequenceCoalescer = struct {
    pending: std.StringHashMapUnmanaged(u64) = .empty,
    last_flush_ns: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.clearPending(alloc);
        self.pending.deinit(alloc);
        self.* = .{};
    }

    pub fn note(self: *@This(), alloc: Allocator, index_name: []const u8, sequence: u64) !void {
        const gop = try self.pending.getOrPut(alloc, index_name);
        if (gop.found_existing) {
            gop.value_ptr.* = @max(gop.value_ptr.*, sequence);
            return;
        }
        errdefer _ = self.pending.remove(index_name);
        gop.key_ptr.* = try alloc.dupe(u8, index_name);
        gop.value_ptr.* = sequence;
    }

    pub fn shouldFlush(self: *const @This(), now_ns: u64) bool {
        if (self.pending.count() == 0) return false;
        return self.last_flush_ns == 0 or now_ns -| self.last_flush_ns >= applied_sequence_flush_interval_ns;
    }

    pub fn clearPending(self: *@This(), alloc: Allocator) void {
        var it = self.pending.iterator();
        while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
        self.pending.clearRetainingCapacity();
    }

    pub fn removePending(self: *@This(), alloc: Allocator, index_name: []const u8) void {
        const removed = self.pending.fetchRemove(index_name) orelse return;
        alloc.free(@constCast(removed.key));
    }

    pub fn takePending(self: *@This(), index_name: []const u8) ?struct { owned_name: []const u8, sequence: u64 } {
        const removed = self.pending.fetchRemove(index_name) orelse return null;
        return .{
            .owned_name = @constCast(removed.key),
            .sequence = removed.value,
        };
    }
};

test "applied sequence coalescer keeps max sequence per index" {
    const alloc = std.testing.allocator;

    var coalescer = AppliedSequenceCoalescer{};
    defer coalescer.deinit(alloc);

    try coalescer.note(alloc, "dv_v1", 10);
    try coalescer.note(alloc, "dv_v1", 7);
    try coalescer.note(alloc, "ft_v1", 4);
    try coalescer.note(alloc, "dv_v1", 12);

    try std.testing.expectEqual(@as(u32, 2), coalescer.pending.count());
    try std.testing.expectEqual(@as(u64, 12), coalescer.pending.get("dv_v1").?);
    try std.testing.expectEqual(@as(u64, 4), coalescer.pending.get("ft_v1").?);
    try std.testing.expect(coalescer.shouldFlush(applied_sequence_flush_interval_ns));

    coalescer.clearPending(alloc);
    try std.testing.expectEqual(@as(u32, 0), coalescer.pending.count());
}

test "applied sequence coalescer takePending removes only requested index" {
    const alloc = std.testing.allocator;

    var coalescer = AppliedSequenceCoalescer{};
    defer coalescer.deinit(alloc);

    try coalescer.note(alloc, "dv_v1", 10);
    try coalescer.note(alloc, "ft_v1", 4);

    const removed = coalescer.takePending("dv_v1").?;
    defer alloc.free(removed.owned_name);
    try std.testing.expectEqualStrings("dv_v1", removed.owned_name);
    try std.testing.expectEqual(@as(u64, 10), removed.sequence);
    try std.testing.expectEqual(@as(u32, 1), coalescer.pending.count());
    try std.testing.expectEqual(@as(u64, 4), coalescer.pending.get("ft_v1").?);
    try std.testing.expect(coalescer.pending.get("dv_v1") == null);
}

pub const MutexContentionStats = struct {
    lock_calls: AtomicU64 = .init(0),
    contended_calls: AtomicU64 = .init(0),
    current_waiters: AtomicU64 = .init(0),
    max_waiters: AtomicU64 = .init(0),
    spin_loops: AtomicU64 = .init(0),
    yield_loops: AtomicU64 = .init(0),
    sleep_loops: AtomicU64 = .init(0),
    wait_ns: AtomicU64 = .init(0),
    max_wait_ns: AtomicU64 = .init(0),
    hold_ns: AtomicU64 = .init(0),
    max_hold_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.DBMutexStats {
        return .{
            .lock_calls = self.lock_calls.load(.monotonic),
            .contended_calls = self.contended_calls.load(.monotonic),
            .max_waiters = self.max_waiters.load(.monotonic),
            .spin_loops = self.spin_loops.load(.monotonic),
            .yield_loops = self.yield_loops.load(.monotonic),
            .sleep_loops = self.sleep_loops.load(.monotonic),
            .wait_ns = self.wait_ns.load(.monotonic),
            .max_wait_ns = self.max_wait_ns.load(.monotonic),
            .hold_ns = self.hold_ns.load(.monotonic),
            .max_hold_ns = self.max_hold_ns.load(.monotonic),
        };
    }
};

pub const AppliedSequenceContentionStats = struct {
    note_calls: AtomicU64 = .init(0),
    forced_flush_calls: AtomicU64 = .init(0),
    skipped_flush_calls: AtomicU64 = .init(0),
    flush_calls: AtomicU64 = .init(0),
    flushed_indexes: AtomicU64 = .init(0),
    sync_ns: AtomicU64 = .init(0),
    save_ns: AtomicU64 = .init(0),
    flush_ns: AtomicU64 = .init(0),
    max_flush_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.AppliedSequenceStats {
        return .{
            .note_calls = self.note_calls.load(.monotonic),
            .forced_flush_calls = self.forced_flush_calls.load(.monotonic),
            .skipped_flush_calls = self.skipped_flush_calls.load(.monotonic),
            .flush_calls = self.flush_calls.load(.monotonic),
            .flushed_indexes = self.flushed_indexes.load(.monotonic),
            .sync_ns = self.sync_ns.load(.monotonic),
            .save_ns = self.save_ns.load(.monotonic),
            .flush_ns = self.flush_ns.load(.monotonic),
            .max_flush_ns = self.max_flush_ns.load(.monotonic),
        };
    }
};

pub const DenseCatchUpContentionStats = struct {
    begin_calls: AtomicU64 = .init(0),
    finish_calls: AtomicU64 = .init(0),
    abort_calls: AtomicU64 = .init(0),
    active: AtomicU64 = .init(0),
    phase: std.atomic.Value(u8) = .init(@intFromEnum(types.DenseCatchUpStats.Phase.idle)),
    current_sequence: AtomicU64 = .init(0),
    current_target_sequence: AtomicU64 = .init(0),
    current_scanned_entries: AtomicU64 = .init(0),
    current_applied_entries: AtomicU64 = .init(0),
    replay_scan_batches: AtomicU64 = .init(0),
    replay_hint_filter_skips: AtomicU64 = .init(0),
    progress_updates: AtomicU64 = .init(0),
    bulk_finish_windows: AtomicU64 = .init(0),
    bulk_finish_split_steps: AtomicU64 = .init(0),
    bulk_finish_deferred_leaf_splits: AtomicU64 = .init(0),
    bulk_finish_current_window: AtomicU64 = .init(0),
    bulk_finish_current_window_split_steps: AtomicU64 = .init(0),
    bulk_finish_current_window_ns: AtomicU64 = .init(0),
    bulk_finish_max_window_ns: AtomicU64 = .init(0),
    finish_ns: AtomicU64 = .init(0),
    max_finish_ns: AtomicU64 = .init(0),
    finalize_ns: AtomicU64 = .init(0),
    max_finalize_ns: AtomicU64 = .init(0),
    maintenance_calls: AtomicU64 = .init(0),
    maintenance_steps: AtomicU64 = .init(0),
    maintenance_ns: AtomicU64 = .init(0),
    max_maintenance_ns: AtomicU64 = .init(0),
    manifest_writes: AtomicU64 = .init(0),
    manifest_ns: AtomicU64 = .init(0),
    write_pressure_compactions: AtomicU64 = .init(0),
    write_pressure_ns: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.DenseCatchUpStats {
        return .{
            .begin_calls = self.begin_calls.load(.monotonic),
            .finish_calls = self.finish_calls.load(.monotonic),
            .abort_calls = self.abort_calls.load(.monotonic),
            .active = self.active.load(.monotonic) != 0,
            .phase = @enumFromInt(self.phase.load(.monotonic)),
            .current_sequence = self.current_sequence.load(.monotonic),
            .current_target_sequence = self.current_target_sequence.load(.monotonic),
            .current_scanned_entries = self.current_scanned_entries.load(.monotonic),
            .current_applied_entries = self.current_applied_entries.load(.monotonic),
            .replay_scan_batches = self.replay_scan_batches.load(.monotonic),
            .replay_hint_filter_skips = self.replay_hint_filter_skips.load(.monotonic),
            .progress_updates = self.progress_updates.load(.monotonic),
            .bulk_finish_windows = self.bulk_finish_windows.load(.monotonic),
            .bulk_finish_split_steps = self.bulk_finish_split_steps.load(.monotonic),
            .bulk_finish_deferred_leaf_splits = self.bulk_finish_deferred_leaf_splits.load(.monotonic),
            .bulk_finish_current_window = self.bulk_finish_current_window.load(.monotonic),
            .bulk_finish_current_window_split_steps = self.bulk_finish_current_window_split_steps.load(.monotonic),
            .bulk_finish_current_window_ns = self.bulk_finish_current_window_ns.load(.monotonic),
            .bulk_finish_max_window_ns = self.bulk_finish_max_window_ns.load(.monotonic),
            .finish_ns = self.finish_ns.load(.monotonic),
            .max_finish_ns = self.max_finish_ns.load(.monotonic),
            .finalize_ns = self.finalize_ns.load(.monotonic),
            .max_finalize_ns = self.max_finalize_ns.load(.monotonic),
            .maintenance_calls = self.maintenance_calls.load(.monotonic),
            .maintenance_steps = self.maintenance_steps.load(.monotonic),
            .maintenance_ns = self.maintenance_ns.load(.monotonic),
            .max_maintenance_ns = self.max_maintenance_ns.load(.monotonic),
            .manifest_writes = self.manifest_writes.load(.monotonic),
            .manifest_ns = self.manifest_ns.load(.monotonic),
            .write_pressure_compactions = self.write_pressure_compactions.load(.monotonic),
            .write_pressure_ns = self.write_pressure_ns.load(.monotonic),
        };
    }
};

pub const StartupOpenStats = struct {
    wal_retention_known: std.atomic.Value(bool) = .init(false),
    wal_retained_segments: AtomicU64 = .init(0),
    wal_retained_bytes: AtomicU64 = .init(0),
    wal_checkpoint_oldest_retained_segment: AtomicU64 = .init(0),
    wal_checkpoint_covered_through_segment: AtomicU64 = .init(0),
    wal_checkpoint_current_segment: AtomicU64 = .init(0),
    wal_checkpoint_lag_segments: AtomicU64 = .init(0),
    wal_replay_retained_segments: AtomicU64 = .init(0),
    wal_replay_retained_bytes: AtomicU64 = .init(0),
    wal_replay_current_segment: AtomicU64 = .init(0),
    configured_indexes: std.atomic.Value(u32) = .init(0),
    configured_dense_indexes: std.atomic.Value(u32) = .init(0),
    configured_sparse_indexes: std.atomic.Value(u32) = .init(0),
    configured_full_text_indexes: std.atomic.Value(u32) = .init(0),
    configured_graph_indexes: std.atomic.Value(u32) = .init(0),
    opened_indexes: std.atomic.Value(u32) = .init(0),
    db_open_ns: AtomicU64 = .init(0),
    load_indexes_ns: AtomicU64 = .init(0),
    lsm_open_stores: AtomicU64 = .init(0),
    lsm_open_completed: AtomicU64 = .init(0),
    lsm_open_failed: AtomicU64 = .init(0),
    lsm_open_total_ns: AtomicU64 = .init(0),
    lsm_open_initializing_storage_ns: AtomicU64 = .init(0),
    lsm_open_manifest_ns: AtomicU64 = .init(0),
    lsm_open_ensuring_dirs_ns: AtomicU64 = .init(0),
    lsm_open_wal_replay_ns: AtomicU64 = .init(0),
    lsm_open_mounting_runs_ns: AtomicU64 = .init(0),
    lsm_open_loaded_runs: AtomicU64 = .init(0),
    lsm_open_obsolete_paths: AtomicU64 = .init(0),
    lsm_open_mutable_entries_after_replay: AtomicU64 = .init(0),
    lsm_open_immutable_memtables_after_replay: AtomicU64 = .init(0),
    wal_replay_records: AtomicU64 = .init(0),
    wal_replay_entries: AtomicU64 = .init(0),
    wal_replay_bytes: AtomicU64 = .init(0),
    wal_replay_ns: AtomicU64 = .init(0),
    wal_replay_truncated_tail_bytes: AtomicU64 = .init(0),

    pub fn snapshot(self: *const @This()) types.StartupCatchUpStats {
        return .{
            .wal_retention_known = self.wal_retention_known.load(.monotonic),
            .wal_retained_segments = self.wal_retained_segments.load(.monotonic),
            .wal_retained_bytes = self.wal_retained_bytes.load(.monotonic),
            .wal_checkpoint_oldest_retained_segment = self.wal_checkpoint_oldest_retained_segment.load(.monotonic),
            .wal_checkpoint_covered_through_segment = self.wal_checkpoint_covered_through_segment.load(.monotonic),
            .wal_checkpoint_current_segment = self.wal_checkpoint_current_segment.load(.monotonic),
            .wal_checkpoint_lag_segments = self.wal_checkpoint_lag_segments.load(.monotonic),
            .wal_replay_retained_segments = self.wal_replay_retained_segments.load(.monotonic),
            .wal_replay_retained_bytes = self.wal_replay_retained_bytes.load(.monotonic),
            .wal_replay_current_segment = self.wal_replay_current_segment.load(.monotonic),
            .configured_indexes = self.configured_indexes.load(.monotonic),
            .configured_dense_indexes = self.configured_dense_indexes.load(.monotonic),
            .configured_sparse_indexes = self.configured_sparse_indexes.load(.monotonic),
            .configured_full_text_indexes = self.configured_full_text_indexes.load(.monotonic),
            .configured_graph_indexes = self.configured_graph_indexes.load(.monotonic),
            .opened_indexes = self.opened_indexes.load(.monotonic),
            .db_open_ns = self.db_open_ns.load(.monotonic),
            .load_indexes_ns = self.load_indexes_ns.load(.monotonic),
            .lsm_open_stores = self.lsm_open_stores.load(.monotonic),
            .lsm_open_completed = self.lsm_open_completed.load(.monotonic),
            .lsm_open_failed = self.lsm_open_failed.load(.monotonic),
            .lsm_open_total_ns = self.lsm_open_total_ns.load(.monotonic),
            .lsm_open_initializing_storage_ns = self.lsm_open_initializing_storage_ns.load(.monotonic),
            .lsm_open_manifest_ns = self.lsm_open_manifest_ns.load(.monotonic),
            .lsm_open_ensuring_dirs_ns = self.lsm_open_ensuring_dirs_ns.load(.monotonic),
            .lsm_open_wal_replay_ns = self.lsm_open_wal_replay_ns.load(.monotonic),
            .lsm_open_mounting_runs_ns = self.lsm_open_mounting_runs_ns.load(.monotonic),
            .lsm_open_loaded_runs = self.lsm_open_loaded_runs.load(.monotonic),
            .lsm_open_obsolete_paths = self.lsm_open_obsolete_paths.load(.monotonic),
            .lsm_open_mutable_entries_after_replay = self.lsm_open_mutable_entries_after_replay.load(.monotonic),
            .lsm_open_immutable_memtables_after_replay = self.lsm_open_immutable_memtables_after_replay.load(.monotonic),
            .wal_replay_records = self.wal_replay_records.load(.monotonic),
            .wal_replay_entries = self.wal_replay_entries.load(.monotonic),
            .wal_replay_bytes = self.wal_replay_bytes.load(.monotonic),
            .wal_replay_ns = self.wal_replay_ns.load(.monotonic),
            .wal_replay_truncated_tail_bytes = self.wal_replay_truncated_tail_bytes.load(.monotonic),
        };
    }
};

pub const AsyncContentionStats = struct {
    apply_mutex: MutexContentionStats = .{},
    applied_sequence_mutex: MutexContentionStats = .{},
    dense_finish_mutex: MutexContentionStats = .{},
    applied_sequence: AppliedSequenceContentionStats = .{},
    startup: StartupOpenStats = .{},
    dense_catch_up: DenseCatchUpContentionStats = .{},

    pub fn snapshot(self: *const @This()) types.AsyncIndexingStats {
        return .{
            .apply_mutex = self.apply_mutex.snapshot(),
            .applied_sequence_mutex = self.applied_sequence_mutex.snapshot(),
            .dense_finish_mutex = self.dense_finish_mutex.snapshot(),
            .applied_sequence = self.applied_sequence.snapshot(),
            .startup = self.startup.snapshot(),
            .dense_catch_up = self.dense_catch_up.snapshot(),
        };
    }
};
