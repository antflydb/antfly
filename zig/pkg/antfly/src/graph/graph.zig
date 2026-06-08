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

//! KV-based graph index with backend-selectable reverse edge storage.
//!
//! Matches Go antfly's graph_index.go pattern:
//!   - Outgoing edges stored in main DocStore
//!   - Reverse index (incoming edges) in separate backing store
//!   - Edge value: [weight:f64 LE][created_at:u64 LE][updated_at:u64 LE][metadata_json]

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const platform_time = @import("../platform/time.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const backend_scan = @import("../storage/backend_scan.zig");
const docstore = @import("../storage/docstore.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const backfill_state_mod = @import("../storage/db/backfill_state.zig");
const supports_native_reverse_lmdb = builtin.os.tag != .freestanding;
const lmdb_backend = if (supports_native_reverse_lmdb) @import("../storage/lmdb_backend.zig") else struct {
    pub const Backend = struct {
        pub fn close(_: *@This()) void {}

        pub fn sync(_: *@This(), _: bool) !void {
            return error.UnsupportedPlatform;
        }
    };
};
const mem_backend = @import("../storage/mem_backend.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");

// ============================================================================
// Edge types
// ============================================================================

pub const EdgeDirection = enum { out, in, both };

pub const Edge = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64, // unix seconds
    updated_at: u64, // unix seconds
    metadata: []const u8, // raw JSON bytes
};

pub const BatchWrite = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64 = 1.0,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    metadata_json: []const u8 = "",
};

pub const BatchDelete = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
};

/// Encode edge value: [weight:f64 LE][created_at:u64 LE][updated_at:u64 LE][metadata]
pub fn encodeEdgeValue(buf: []u8, weight: f64, created_at: u64, updated_at: u64, metadata: []const u8) []const u8 {
    const weight_bits: u64 = @bitCast(weight);
    std.mem.writeInt(u64, buf[0..8], weight_bits, .little);
    std.mem.writeInt(u64, buf[8..16], created_at, .little);
    std.mem.writeInt(u64, buf[16..24], updated_at, .little);
    if (metadata.len > 0) {
        @memcpy(buf[24 .. 24 + metadata.len], metadata);
    }
    return buf[0 .. 24 + metadata.len];
}

/// Decode edge value from binary format.
pub fn decodeEdgeValue(data: []const u8) struct { weight: f64, created_at: u64, updated_at: u64, metadata: []const u8 } {
    const weight_bits = std.mem.readInt(u64, data[0..8], .little);
    const weight: f64 = @bitCast(weight_bits);
    const created_at = std.mem.readInt(u64, data[8..16], .little);
    const updated_at = std.mem.readInt(u64, data[16..24], .little);
    const metadata = if (data.len > 24) data[24..] else &[0]u8{};
    return .{ .weight = weight, .created_at = created_at, .updated_at = updated_at, .metadata = metadata };
}

const ParsedGraphEdgeKey = struct {
    source: []u8,
    index_name: []u8,
    edge_type: []u8,
    target: []u8,

    fn deinit(self: *ParsedGraphEdgeKey, alloc: Allocator) void {
        alloc.free(self.source);
        alloc.free(self.index_name);
        alloc.free(self.edge_type);
        alloc.free(self.target);
        self.* = undefined;
    }
};

const graph_index_edge_artifact_type = "graph_index";

fn edgeKeyAlloc(alloc: Allocator, source: []const u8, index_name: []const u8, edge_type: []const u8, target: []const u8) ![]u8 {
    return try graphIndexEdgeKeyAlloc(alloc, source, index_name, edge_type, target);
}

fn reverseEdgeKeyAlloc(alloc: Allocator, target: []const u8, index_name: []const u8, edge_type: []const u8, source: []const u8) ![]u8 {
    return try graphIndexEdgeKeyAlloc(alloc, target, index_name, edge_type, source);
}

fn edgePrefixAlloc(alloc: Allocator, source: []const u8, index_name: []const u8, edge_type: []const u8) ![]u8 {
    return try graphIndexEdgePrefixAlloc(alloc, source, index_name, edge_type);
}

fn reverseEdgePrefixAlloc(alloc: Allocator, target: []const u8, index_name: []const u8, edge_type: []const u8) ![]u8 {
    return try graphIndexEdgePrefixAlloc(alloc, target, index_name, edge_type);
}

fn graphIndexEdgePrefixAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, edge_type: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, graph_index_edge_artifact_type);
    try internal_keys.appendEncodedComponent(&list, alloc, index_name);
    try list.append(alloc, internal_keys.graph_edge_record_kind);
    if (edge_type.len > 0) try internal_keys.appendEncodedComponent(&list, alloc, edge_type);

    return try list.toOwnedSlice(alloc);
}

fn graphIndexEdgeKeyAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, edge_type: []const u8, target_doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, graph_index_edge_artifact_type);
    try internal_keys.appendEncodedComponent(&list, alloc, index_name);
    try list.append(alloc, internal_keys.graph_edge_record_kind);
    try internal_keys.appendEncodedComponent(&list, alloc, edge_type);
    try internal_keys.appendEncodedComponent(&list, alloc, target_doc_key);

    return try list.toOwnedSlice(alloc);
}

fn parseGraphIndexEdgeKeyAlloc(alloc: Allocator, key: []const u8) !?ParsedGraphEdgeKey {
    if (!internal_keys.isInternalUserKey(key)) return null;
    const doc_term = internal_keys.findComponentTerminator(key, 1) orelse return null;
    const doc_key = try internal_keys.decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != internal_keys.artifact_kind) {
        alloc.free(doc_key);
        return null;
    }
    pos += 1;

    if (!internal_keys.componentEquals(key, pos, graph_index_edge_artifact_type)) {
        alloc.free(doc_key);
        return null;
    }
    pos = (internal_keys.findComponentTerminator(key, pos) orelse {
        alloc.free(doc_key);
        return null;
    }) + 2;

    const index_term = internal_keys.findComponentTerminator(key, pos) orelse {
        alloc.free(doc_key);
        return null;
    };
    const index_name = try internal_keys.decodeBodyAlloc(alloc, key[pos..index_term]);
    errdefer alloc.free(index_name);
    pos = index_term + 2;

    if (pos >= key.len or key[pos] != internal_keys.graph_edge_record_kind) {
        alloc.free(doc_key);
        alloc.free(index_name);
        return null;
    }
    pos += 1;

    const edge_type_term = internal_keys.findComponentTerminator(key, pos) orelse {
        alloc.free(doc_key);
        alloc.free(index_name);
        return null;
    };
    const edge_type = try internal_keys.decodeBodyAlloc(alloc, key[pos..edge_type_term]);
    errdefer alloc.free(edge_type);
    pos = edge_type_term + 2;

    const target_term = internal_keys.findComponentTerminator(key, pos) orelse {
        alloc.free(doc_key);
        alloc.free(index_name);
        alloc.free(edge_type);
        return null;
    };
    if (target_term + 2 != key.len) {
        alloc.free(doc_key);
        alloc.free(index_name);
        alloc.free(edge_type);
        return null;
    }
    const target_doc_key = try internal_keys.decodeBodyAlloc(alloc, key[pos..target_term]);
    errdefer alloc.free(target_doc_key);

    return .{
        .source = doc_key,
        .index_name = index_name,
        .edge_type = edge_type,
        .target = target_doc_key,
    };
}

fn parseOutgoingEdgeKeyAlloc(alloc: Allocator, key: []const u8) !?ParsedGraphEdgeKey {
    return try parseGraphIndexEdgeKeyAlloc(alloc, key);
}

fn parseReverseEdgeKeyAlloc(alloc: Allocator, key: []const u8) !?ParsedGraphEdgeKey {
    var parsed = (try parseGraphIndexEdgeKeyAlloc(alloc, key)) orelse return null;
    errdefer parsed.deinit(alloc);
    return .{
        .source = parsed.target,
        .index_name = parsed.index_name,
        .edge_type = parsed.edge_type,
        .target = parsed.source,
    };
}

// ============================================================================
// GraphIndex
// ============================================================================

pub const TopologyMode = enum { graph, tree };

pub const EdgeTypeConfig = struct {
    name: []const u8,
    field_name: ?[]const u8 = null,
    topology: TopologyMode = .graph,
};

pub const GraphMetricKind = enum {
    pagerank,
    degree,
    eigenvector,
    hits_authority,
    hits_hub,
};

pub const GraphMetricRefreshMode = enum {
    background,
    manual,
};

pub const GraphMetricEdgeFilterMode = enum {
    all,
    types,
};

pub const GraphMetricEdgeFilter = struct {
    mode: GraphMetricEdgeFilterMode = .all,
    types: []const []const u8 = &.{},

    pub fn cloneAlloc(self: @This(), alloc: Allocator) !@This() {
        if (self.types.len == 0) return .{ .mode = self.mode };
        const types = try alloc.alloc([]const u8, self.types.len);
        var initialized: usize = 0;
        errdefer {
            for (types[0..initialized]) |edge_type| alloc.free(edge_type);
            alloc.free(types);
        }
        for (self.types, 0..) |edge_type, i| {
            types[i] = try alloc.dupe(u8, edge_type);
            initialized += 1;
        }
        return .{ .mode = self.mode, .types = types };
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.types) |edge_type| alloc.free(edge_type);
        if (self.types.len > 0) alloc.free(self.types);
        self.* = undefined;
    }
};

pub const GraphMetricConfig = struct {
    name: []const u8,
    kind: GraphMetricKind = .pagerank,
    damping: f64 = 0.85,
    tolerance: f64 = 0.000001,
    max_iterations: u32 = 50,
    refresh: GraphMetricRefreshMode = .background,
    edge_filter: GraphMetricEdgeFilter = .{},
};

pub fn freeGraphMetricConfigs(alloc: Allocator, configs: []GraphMetricConfig) void {
    for (configs) |*cfg| {
        alloc.free(cfg.name);
        cfg.edge_filter.deinit(alloc);
    }
    if (configs.len > 0) alloc.free(configs);
}

pub const GraphMetricValidationError = error{
    UnknownGraphMetricEdgeType,
    InvalidGraphMetricName,
    DuplicateGraphMetricName,
    InvalidGraphMetricEdgeFilter,
    DuplicateGraphMetricEdgeType,
};

pub fn validateGraphMetricEdgeFilters(
    edge_type_configs: []const EdgeTypeConfig,
    metric_configs: []const GraphMetricConfig,
) GraphMetricValidationError!void {
    for (metric_configs, 0..) |metric_cfg, i| {
        if (metric_cfg.name.len == 0) return error.InvalidGraphMetricName;
        for (metric_configs[0..i]) |prior| {
            if (std.mem.eql(u8, prior.name, metric_cfg.name)) return error.DuplicateGraphMetricName;
        }
        if (metric_cfg.edge_filter.mode == .all) {
            if (metric_cfg.edge_filter.types.len != 0) return error.InvalidGraphMetricEdgeFilter;
        } else {
            if (metric_cfg.edge_filter.types.len == 0) return error.InvalidGraphMetricEdgeFilter;
            for (metric_cfg.edge_filter.types, 0..) |edge_type, edge_type_i| {
                if (edge_type.len == 0) return error.InvalidGraphMetricEdgeFilter;
                for (metric_cfg.edge_filter.types[0..edge_type_i]) |prior_edge_type| {
                    if (std.mem.eql(u8, prior_edge_type, edge_type)) return error.DuplicateGraphMetricEdgeType;
                }
                if (edge_type_configs.len > 0 and !hasConfiguredEdgeType(edge_type_configs, edge_type)) return error.UnknownGraphMetricEdgeType;
            }
        }
    }
}

fn hasConfiguredEdgeType(edge_type_configs: []const EdgeTypeConfig, edge_type: []const u8) bool {
    for (edge_type_configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, edge_type)) return true;
    }
    return false;
}

pub const GraphIndexOptions = struct {
    map_size: usize = 64 * 1024 * 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    reverse_backend: ReverseBackend = .lsm,
    reverse_lsm_storage: ?lsm_backend.Storage = null,
    reverse_lsm_cache: ?*lsm_backend.Cache = null,
    reverse_lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    reverse_lsm_root_generation: u64 = 0,
    edge_type_configs: []const EdgeTypeConfig = &.{},
    metric_configs: []const GraphMetricConfig = &.{},
    rebuild_root_path: ?[]const u8 = null,
    algebraic_semiring_traversal: bool = false,
};

test "graph index defaults to lsm reverse backend" {
    const opts = GraphIndexOptions{};
    try std.testing.expectEqual(ReverseBackend.lsm, opts.reverse_backend);
}

test "graph index routes reverse lsm profile options" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    var rev_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-lsm-profile");
    defer cleanupTmp(store_path);
    const rev_path = tmpPath(&rev_buf, "rev-lsm-profile");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();

    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
        .reverse_backend = .lsm_memory,
        .reverse_lsm_options = .{ .flush_threshold = 91 },
    });
    defer graph.close();

    switch (graph.reverse_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 91), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

test "graph metric edge filter validation uses configured edge type metadata" {
    const edge_types = [_]EdgeTypeConfig{
        .{ .name = "cites" },
        .{ .name = "mentions" },
    };
    const valid_filter_types = [_][]const u8{ "cites", "mentions" };
    const invalid_filter_types = [_][]const u8{"related"};
    const valid_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .types, .types = &valid_filter_types },
    }};
    const invalid_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .types, .types = &invalid_filter_types },
    }};
    const empty_name_metrics = [_]GraphMetricConfig{.{
        .name = "",
    }};
    const duplicate_metrics = [_]GraphMetricConfig{
        .{ .name = "pagerank" },
        .{ .name = "pagerank", .kind = .degree },
    };
    const empty_filter_types = [_][]const u8{};
    const empty_filter_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .types, .types = &empty_filter_types },
    }};
    const blank_filter_types = [_][]const u8{""};
    const blank_filter_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .types, .types = &blank_filter_types },
    }};
    const duplicate_filter_types = [_][]const u8{ "cites", "cites" };
    const duplicate_filter_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .types, .types = &duplicate_filter_types },
    }};
    const all_with_types_metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .edge_filter = .{ .mode = .all, .types = &valid_filter_types },
    }};

    try validateGraphMetricEdgeFilters(&edge_types, &valid_metrics);
    try validateGraphMetricEdgeFilters(&.{}, &invalid_metrics);
    try std.testing.expectError(error.UnknownGraphMetricEdgeType, validateGraphMetricEdgeFilters(&edge_types, &invalid_metrics));
    try std.testing.expectError(error.InvalidGraphMetricName, validateGraphMetricEdgeFilters(&edge_types, &empty_name_metrics));
    try std.testing.expectError(error.DuplicateGraphMetricName, validateGraphMetricEdgeFilters(&edge_types, &duplicate_metrics));
    try std.testing.expectError(error.InvalidGraphMetricEdgeFilter, validateGraphMetricEdgeFilters(&edge_types, &empty_filter_metrics));
    try std.testing.expectError(error.InvalidGraphMetricEdgeFilter, validateGraphMetricEdgeFilters(&edge_types, &blank_filter_metrics));
    try std.testing.expectError(error.DuplicateGraphMetricEdgeType, validateGraphMetricEdgeFilters(&edge_types, &duplicate_filter_metrics));
    try std.testing.expectError(error.InvalidGraphMetricEdgeFilter, validateGraphMetricEdgeFilters(&edge_types, &all_with_types_metrics));
}

const reverse_rebuild_batch_size: usize = 1024;
pub var test_abort_reverse_rebuild_after_batches: ?usize = null;
const graph_meta_prefix = "meta:";
const graph_edge_count_key = "meta:edge_count";
const graph_node_count_key = "meta:node_count";
const graph_edge_generation_key = "meta:edge_generation";
const graph_metric_key_prefix = "meta:metric:";
const graph_metric_control_key_prefix = "meta:metric_control:";
const graph_metric_recent_event_limit = 8;
const default_graph_metric_deferred_cleanup_ms: u64 = 60_000;
const graph_metric_local_build_worker_id = "local";
const graph_metric_local_build_lease_ms: u64 = 300_000;

pub const ReverseBackend = enum {
    lmdb,
    mem,
    lsm_memory,
    lsm,
};

pub const GraphIndex = struct {
    alloc: Allocator,
    index_name: []const u8,
    outgoing_store: backend_erased.Store,
    outgoing_owner: ReverseStoreOwner,
    reverse_store: backend_erased.Store,
    reverse_owner: ReverseStoreOwner,
    edge_type_configs: []const EdgeTypeConfig,
    metric_configs: []const GraphMetricConfig,
    rebuild_root_path: ?[]u8,
    algebraic_semiring_traversal: bool,
    edge_count: u64,
    node_count: u64,
    edge_generation: u64,
    algebraic_traversal_attempt_count: u64,
    algebraic_traversal_proven_count: u64,
    algebraic_traversal_rejected_count: u64,
    algebraic_traversal_fallback_count: u64,
    algebraic_traversal_result_node_count: u64,

    pub const TreeTopologyViolation = error{TreeTopologyViolation};

    const ReverseStoreOwner = union(enum) {
        none,
        lmdb: *lmdb_backend.Backend,
        mem: *mem_backend.Backend,
        lsm: lsm_backend.BackendHandle,

        fn close(self: *ReverseStoreOwner, alloc: Allocator) void {
            switch (self.*) {
                .none => {},
                .lmdb => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .mem => |backend| {
                    backend.close();
                    alloc.destroy(backend);
                },
                .lsm => |*handle| handle.close(),
            }
            self.* = .none;
        }

        fn sync(self: *ReverseStoreOwner, force: bool) !void {
            switch (self.*) {
                .none, .mem => {},
                .lmdb => |backend| try backend.sync(force),
                .lsm => |*handle| try handle.backend.sync(force),
            }
        }

        fn ensureDurableEmptyManifest(self: *ReverseStoreOwner) !void {
            switch (self.*) {
                .none, .mem, .lmdb => {},
                .lsm => |*handle| {
                    if (handle.backend.options.backend.read_only) return;
                    if (handle.backend.manifest_backing != null) return;
                    if (handle.backend.runs.items.len != 0) return;
                    try handle.backend.persistManifest();
                },
            }
        }

        fn checkpointLsmWalAfterDurableBoundary(self: *ReverseStoreOwner) !void {
            switch (self.*) {
                .none, .mem, .lmdb => {},
                .lsm => |*handle| try handle.backend.checkpointWalAfterDurableBoundary(),
            }
        }
    };

    const OpenedReverseStore = struct {
        store: backend_erased.Store,
        owner: ReverseStoreOwner,
    };

    fn resolvedReverseLsmOptions(opts: GraphIndexOptions, memory_only: bool) lsm_backend.Options {
        var lsm_options = opts.reverse_lsm_options;
        lsm_options.backend.durability = if (memory_only or opts.no_sync) .none else lsm_options.backend.durability;
        if (!memory_only) lsm_options.storage = opts.reverse_lsm_storage orelse lsm_options.storage;
        lsm_options.cache = opts.reverse_lsm_cache orelse lsm_options.cache;
        if (opts.reverse_lsm_root_generation != 0 and lsm_options.root_generation == 0) {
            lsm_options.root_generation = opts.reverse_lsm_root_generation;
        }
        return lsm_options;
    }

    pub fn reverseStore(self: *GraphIndex) *backend_erased.Store {
        return &self.reverse_store;
    }

    pub fn checkpointLsmWalAfterDurableBoundary(self: *GraphIndex) !void {
        try self.reverse_owner.checkpointLsmWalAfterDurableBoundary();
    }

    fn beginWriteOutgoingBatch(self: *GraphIndex) !backend_erased.Batch {
        return try self.outgoing_store.beginBatch();
    }

    fn beginReadReverseTxn(self: *GraphIndex) !backend_erased.ReadTxn {
        return try self.reverse_store.beginRead();
    }

    fn beginWriteReverseTxn(self: *GraphIndex) !backend_erased.WriteTxn {
        return try self.reverse_store.beginWrite();
    }

    fn beginWriteReverseBatch(self: *GraphIndex) !backend_erased.Batch {
        return try self.reverse_store.beginBatch();
    }

    fn loadGraphCounters(store: *backend_erased.Store) !Stats {
        var txn = try store.beginRead();
        defer txn.abort();
        return .{
            .edge_count = try readU64OrZero(&txn, graph_edge_count_key),
            .node_count = try readU64OrZero(&txn, graph_node_count_key),
            .edge_generation = try readU64OrZero(&txn, graph_edge_generation_key),
        };
    }

    fn readU64OrZero(txn: anytype, key: []const u8) !u64 {
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return 0,
            else => return err,
        };
        if (raw.len < 8) return 0;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    fn putU64(txn: anytype, key: []const u8, value: u64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, value, .little);
        try txn.put(key, &buf);
    }

    fn graphNodeRefKeyAlloc(alloc: Allocator, node: []const u8) ![]u8 {
        return try std.fmt.allocPrint(alloc, "meta:node_ref:{s}", .{node});
    }

    fn adjustNodeRef(self: *GraphIndex, batch: anytype, node: []const u8, delta: i64) !void {
        const key = try graphNodeRefKeyAlloc(self.alloc, node);
        defer self.alloc.free(key);
        const current = try readU64OrZero(batch, key);
        if (delta > 0) {
            if (current == 0) self.node_count += 1;
            try putU64(batch, key, current + @as(u64, @intCast(delta)));
            return;
        }
        const dec: u64 = @intCast(-delta);
        const next = if (dec >= current) 0 else current - dec;
        if (current > 0 and next == 0) {
            self.node_count = if (self.node_count == 0) 0 else self.node_count - 1;
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        } else {
            try putU64(batch, key, next);
        }
    }

    fn accountReverseDelete(self: *GraphIndex, batch: anytype, source: []const u8, target: []const u8, rev_key: []const u8) !void {
        _ = batch.get(rev_key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        self.edge_count = if (self.edge_count == 0) 0 else self.edge_count - 1;
        try self.adjustNodeRef(batch, source, -1);
        try self.adjustNodeRef(batch, target, -1);
    }

    fn accountReverseInsert(self: *GraphIndex, batch: anytype, source: []const u8, target: []const u8, rev_key: []const u8) !void {
        _ = batch.get(rev_key) catch |err| switch (err) {
            error.NotFound => {
                self.edge_count += 1;
                try self.adjustNodeRef(batch, source, 1);
                try self.adjustNodeRef(batch, target, 1);
                return;
            },
            else => return err,
        };
    }

    fn persistGraphCounters(self: *GraphIndex, batch: anytype) !void {
        try putU64(batch, graph_edge_count_key, self.edge_count);
        try putU64(batch, graph_node_count_key, self.node_count);
        try putU64(batch, graph_edge_generation_key, self.edge_generation);
    }

    fn graphMetricKeyAlloc(self: *GraphIndex, parts: []const []const u8) ![]u8 {
        var list = std.ArrayListUnmanaged(u8).empty;
        defer list.deinit(self.alloc);
        try list.appendSlice(self.alloc, graph_metric_key_prefix);
        for (parts) |part| try internal_keys.appendEncodedComponent(&list, self.alloc, part);
        return try list.toOwnedSlice(self.alloc);
    }

    fn graphMetricControlKeyAlloc(self: *GraphIndex, parts: []const []const u8) ![]u8 {
        var list = std.ArrayListUnmanaged(u8).empty;
        defer list.deinit(self.alloc);
        try list.appendSlice(self.alloc, graph_metric_control_key_prefix);
        for (parts) |part| try internal_keys.appendEncodedComponent(&list, self.alloc, part);
        return try list.toOwnedSlice(self.alloc);
    }

    fn graphMetricScoreKeyAlloc(self: *GraphIndex, metric_name: []const u8, generation: u64, node: []const u8) ![]u8 {
        const generation_text = try std.fmt.allocPrint(self.alloc, "{d}", .{generation});
        defer self.alloc.free(generation_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "score", generation_text, node });
    }

    fn graphMetricScorePrefixAlloc(self: *GraphIndex, metric_name: []const u8, generation: u64) ![]u8 {
        const generation_text = try std.fmt.allocPrint(self.alloc, "{d}", .{generation});
        defer self.alloc.free(generation_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "score", generation_text });
    }

    fn graphMetricPublishedGenerationKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricKeyAlloc(&.{ metric_name, "published_generation" });
    }

    fn graphMetricDirtyGenerationKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricKeyAlloc(&.{ metric_name, "dirty_generation" });
    }

    fn graphMetricMaintenancePausedKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricControlKeyAlloc(&.{ metric_name, "maintenance_paused" });
    }

    fn graphMetricBuildLeaseKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricControlKeyAlloc(&.{ metric_name, "build_lease" });
    }

    fn graphMetricBuildJobKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricControlKeyAlloc(&.{ metric_name, "build_job" });
    }

    fn graphMetricFailureKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricControlKeyAlloc(&.{ metric_name, "last_failure" });
    }

    fn graphMetricEventSequenceKeyAlloc(self: *GraphIndex, metric_name: []const u8) ![]u8 {
        return try self.graphMetricKeyAlloc(&.{ metric_name, "event_sequence" });
    }

    fn graphMetricEventKeyAlloc(self: *GraphIndex, metric_name: []const u8, sequence: u64) ![]u8 {
        const sequence_text = try std.fmt.allocPrint(self.alloc, "{d}", .{sequence});
        defer self.alloc.free(sequence_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "event", sequence_text });
    }

    fn graphMetricMetaKeyAlloc(self: *GraphIndex, metric_name: []const u8, generation: u64) ![]u8 {
        const generation_text = try std.fmt.allocPrint(self.alloc, "{d}", .{generation});
        defer self.alloc.free(generation_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "meta", generation_text });
    }

    fn graphMetricMetaEdgeFilterKeyAlloc(self: *GraphIndex, metric_name: []const u8, generation: u64) ![]u8 {
        const generation_text = try std.fmt.allocPrint(self.alloc, "{d}", .{generation});
        defer self.alloc.free(generation_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "meta_edge_filter", generation_text });
    }

    fn graphMetricMetaConfigFingerprintKeyAlloc(self: *GraphIndex, metric_name: []const u8, generation: u64) ![]u8 {
        const generation_text = try std.fmt.allocPrint(self.alloc, "{d}", .{generation});
        defer self.alloc.free(generation_text);
        return try self.graphMetricKeyAlloc(&.{ metric_name, "meta_config_fingerprint", generation_text });
    }

    fn metricPublishedGeneration(self: *GraphIndex, txn: anytype, metric_name: []const u8) !u64 {
        const key = try self.graphMetricPublishedGenerationKeyAlloc(metric_name);
        defer self.alloc.free(key);
        return try readU64OrZero(txn, key);
    }

    fn metricDirtyGeneration(self: *GraphIndex, txn: anytype, metric_name: []const u8) !u64 {
        const key = try self.graphMetricDirtyGenerationKeyAlloc(metric_name);
        defer self.alloc.free(key);
        return try readU64OrZero(txn, key);
    }

    fn metricMaintenancePaused(self: *GraphIndex, txn: anytype, metric_name: []const u8) !bool {
        const key = try self.graphMetricMaintenancePausedKeyAlloc(metric_name);
        defer self.alloc.free(key);
        return (try readU64OrZero(txn, key)) != 0;
    }

    fn metricBuildLease(self: *GraphIndex, txn: anytype, metric_name: []const u8) !?GraphMetricBuildLease {
        const key = try self.graphMetricBuildLeaseKeyAlloc(metric_name);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return decodeGraphMetricBuildLease(raw);
    }

    fn metricBuildJob(self: *GraphIndex, txn: anytype, metric_name: []const u8) !?GraphMetricBuildJob {
        const key = try self.graphMetricBuildJobKeyAlloc(metric_name);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return decodeGraphMetricBuildJob(raw);
    }

    fn metricFailureDetail(self: *GraphIndex, txn: anytype, metric_name: []const u8) !?GraphMetricFailureDetail {
        const key = try self.graphMetricFailureKeyAlloc(metric_name);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeGraphMetricFailureDetailAlloc(self.alloc, raw);
    }

    fn acquireGraphMetricBuildLease(
        self: *GraphIndex,
        metric_name: []const u8,
        target_generation: u64,
    ) !void {
        const now_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();

        const key = try self.graphMetricBuildLeaseKeyAlloc(metric_name);
        defer self.alloc.free(key);
        if (batch.get(key)) |raw| {
            if (decodeGraphMetricBuildLease(raw)) |lease| {
                if (lease.lease_expires_at_ms > now_ms) return error.GraphMetricBuildAlreadyRunning;
            }
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        const lease = GraphMetricBuildLease{
            .job_id = graphMetricBuildJobId(metric_name, target_generation, now_ms),
            .target_generation = target_generation,
            .started_at_ms = now_ms,
            .lease_expires_at_ms = now_ms + graph_metric_local_build_lease_ms,
            .phase = .prepare_generation,
            .iteration = 0,
            .worker_id = graph_metric_local_build_worker_id,
        };
        const encoded = try self.alloc.alloc(u8, graphMetricBuildLeaseEncodedLen(lease));
        defer self.alloc.free(encoded);
        encodeGraphMetricBuildLease(lease, encoded);
        try batch.put(key, encoded);
        try self.putGraphMetricBuildJobInBatch(&batch, metric_name, .{
            .job_id = lease.job_id,
            .target_generation = target_generation,
            .score_generation = target_generation,
            .started_at_ms = lease.started_at_ms,
            .updated_at_ms = now_ms,
            .lease_expires_at_ms = lease.lease_expires_at_ms,
            .phase = lease.phase,
            .iteration = lease.iteration,
            .worker_id = lease.worker_id,
        });
        try batch.commit();
    }

    fn releaseGraphMetricBuildLease(self: *GraphIndex, metric_name: []const u8) !void {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const key = try self.graphMetricBuildLeaseKeyAlloc(metric_name);
        defer self.alloc.free(key);
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try batch.commit();
    }

    fn updateGraphMetricBuildLeaseProgress(
        self: *GraphIndex,
        metric_name: []const u8,
        phase: GraphMetricBuildPhase,
        iteration: u32,
    ) !void {
        try self.updateGraphMetricBuildLeaseProgressWithCursor(metric_name, phase, iteration, "", 0, 0);
    }

    fn updateGraphMetricBuildLeaseProgressWithCursor(
        self: *GraphIndex,
        metric_name: []const u8,
        phase: GraphMetricBuildPhase,
        iteration: u32,
        cursor: []const u8,
        completed_units: u64,
        total_units: u64,
    ) !void {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const key = try self.graphMetricBuildLeaseKeyAlloc(metric_name);
        defer self.alloc.free(key);
        const raw = batch.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        var lease = decodeGraphMetricBuildLease(raw) orelse return;
        lease.phase = phase;
        lease.iteration = iteration;
        const now_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
        const encoded = try self.alloc.alloc(u8, graphMetricBuildLeaseEncodedLen(lease));
        defer self.alloc.free(encoded);
        encodeGraphMetricBuildLease(lease, encoded);
        try batch.put(key, encoded);
        try self.putGraphMetricBuildJobInBatch(&batch, metric_name, .{
            .job_id = lease.job_id,
            .target_generation = lease.target_generation,
            .score_generation = lease.target_generation,
            .started_at_ms = lease.started_at_ms,
            .updated_at_ms = now_ms,
            .lease_expires_at_ms = lease.lease_expires_at_ms,
            .phase = phase,
            .iteration = iteration,
            .worker_id = lease.worker_id,
            .cursor = cursor,
            .completed_units = completed_units,
            .total_units = total_units,
        });
        try batch.commit();
    }

    fn completeGraphMetricBuildJob(self: *GraphIndex, metric_name: []const u8) !void {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const job = try self.metricBuildJob(&batch, metric_name) orelse {
            try batch.commit();
            return;
        };
        try self.putGraphMetricBuildJobInBatch(&batch, metric_name, .{
            .job_id = job.job_id,
            .target_generation = job.target_generation,
            .score_generation = job.score_generation,
            .started_at_ms = job.started_at_ms,
            .updated_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .lease_expires_at_ms = 0,
            .phase = .complete,
            .iteration = job.iteration,
            .worker_id = job.worker_id,
            .cursor = job.cursor,
            .completed_units = if (job.total_units != 0) job.total_units else job.completed_units,
            .total_units = job.total_units,
        });
        try batch.commit();
    }

    fn putGraphMetricBuildJobInBatch(
        self: *GraphIndex,
        batch: anytype,
        metric_name: []const u8,
        job: GraphMetricBuildJob,
    ) !void {
        const job_key = try self.graphMetricBuildJobKeyAlloc(metric_name);
        defer self.alloc.free(job_key);
        const encoded = try self.alloc.alloc(u8, graphMetricBuildJobEncodedLen(job));
        defer self.alloc.free(encoded);
        encodeGraphMetricBuildJob(job, encoded);
        try batch.put(job_key, encoded);
    }

    fn graphMetricLastEvent(self: *GraphIndex, txn: anytype, metric_name: []const u8) !?GraphMetricEvent {
        const sequence_key = try self.graphMetricEventSequenceKeyAlloc(metric_name);
        defer self.alloc.free(sequence_key);
        const sequence = try readU64OrZero(txn, sequence_key);
        if (sequence == 0) return null;
        const event_key = try self.graphMetricEventKeyAlloc(metric_name, sequence);
        defer self.alloc.free(event_key);
        const raw = txn.get(event_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return decodeGraphMetricEvent(sequence, raw);
    }

    fn graphMetricRecentEvents(
        self: *GraphIndex,
        txn: anytype,
        metric_name: []const u8,
        limit: usize,
    ) ![]GraphMetricEvent {
        if (limit == 0) return &.{};
        const sequence_key = try self.graphMetricEventSequenceKeyAlloc(metric_name);
        defer self.alloc.free(sequence_key);
        var sequence = try readU64OrZero(txn, sequence_key);
        if (sequence == 0) return &.{};

        const events = try self.alloc.alloc(GraphMetricEvent, @min(limit, sequence));
        var count: usize = 0;
        errdefer if (events.len > 0) self.alloc.free(events);
        while (sequence > 0 and count < limit) : (sequence -= 1) {
            const event_key = try self.graphMetricEventKeyAlloc(metric_name, sequence);
            defer self.alloc.free(event_key);
            const raw = txn.get(event_key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            const event = decodeGraphMetricEvent(sequence, raw) orelse continue;
            events[count] = event;
            count += 1;
        }
        return try self.alloc.realloc(events, count);
    }

    fn appendGraphMetricEvent(
        self: *GraphIndex,
        batch: anytype,
        metric_name: []const u8,
        event: GraphMetricEvent,
    ) !void {
        const sequence_key = try self.graphMetricEventSequenceKeyAlloc(metric_name);
        defer self.alloc.free(sequence_key);
        const sequence = (try readU64OrZero(batch, sequence_key)) + 1;
        try putU64(batch, sequence_key, sequence);

        const event_key = try self.graphMetricEventKeyAlloc(metric_name, sequence);
        defer self.alloc.free(event_key);
        var encoded: [graph_metric_event_encoded_len]u8 = undefined;
        var sequenced = event;
        sequenced.sequence = sequence;
        encodeGraphMetricEvent(sequenced, &encoded);
        try batch.put(event_key, &encoded);
        if (sequence > graph_metric_recent_event_limit) {
            const prune_key = try self.graphMetricEventKeyAlloc(metric_name, sequence - graph_metric_recent_event_limit);
            defer self.alloc.free(prune_key);
            batch.delete(prune_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn clearGraphMetricFailureInBatch(self: *GraphIndex, batch: anytype, metric_name: []const u8) !void {
        const failure_key = try self.graphMetricFailureKeyAlloc(metric_name);
        defer self.alloc.free(failure_key);
        batch.delete(failure_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn recordGraphMetricFailure(self: *GraphIndex, metric_name: []const u8, err: anyerror) !void {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const failure_key = try self.graphMetricFailureKeyAlloc(metric_name);
        defer self.alloc.free(failure_key);
        var retry_count: u64 = 1;
        if (batch.get(failure_key)) |raw| {
            if (try decodeGraphMetricFailureDetailAlloc(self.alloc, raw)) |detail| {
                retry_count = detail.retry_count + 1;
                detail.deinit(self.alloc);
            }
        } else |get_err| switch (get_err) {
            error.NotFound => {},
            else => return get_err,
        }
        const err_name = @errorName(err);
        const encoded = try self.alloc.alloc(u8, graph_metric_failure_detail_header_len + err_name.len);
        defer self.alloc.free(encoded);
        encodeGraphMetricFailureDetail(.{ .retry_count = retry_count, .last_error = err_name }, encoded);
        try batch.put(failure_key, encoded);
        const now_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
        if (try self.metricBuildJob(&batch, metric_name)) |job| {
            try self.putGraphMetricBuildJobInBatch(&batch, metric_name, .{
                .job_id = job.job_id,
                .target_generation = job.target_generation,
                .score_generation = job.score_generation,
                .started_at_ms = job.started_at_ms,
                .updated_at_ms = now_ms,
                .lease_expires_at_ms = job.lease_expires_at_ms,
                .phase = job.phase,
                .iteration = job.iteration,
                .retry_count = retry_count,
                .worker_id = job.worker_id,
                .last_error = err_name,
                .cursor = job.cursor,
                .completed_units = job.completed_units,
                .total_units = job.total_units,
            });
        }
        try self.appendGraphMetricEvent(&batch, metric_name, .{
            .kind = .failed,
            .at_ms = now_ms,
            .target_edge_generation = self.edge_generation,
            .published_generation = try self.metricPublishedGeneration(&batch, metric_name),
            .score_count = 0,
        });
        try batch.commit();
    }

    fn markMetricDirty(self: *GraphIndex, batch: anytype) !void {
        if (self.metric_configs.len == 0) return;
        for (self.metric_configs) |cfg| {
            const key = try self.graphMetricDirtyGenerationKeyAlloc(cfg.name);
            defer self.alloc.free(key);
            try putU64(batch, key, self.edge_generation);
        }
    }

    fn rememberNodeRefCount(self: *GraphIndex, counts: *std.StringHashMapUnmanaged(u64), node: []const u8) !void {
        const result = try counts.getOrPut(self.alloc, node);
        if (result.found_existing) {
            result.value_ptr.* += 1;
            return;
        }
        errdefer _ = counts.remove(node);
        result.key_ptr.* = try self.alloc.dupe(u8, node);
        result.value_ptr.* = 1;
    }

    fn rebuildCounterMetadata(self: *GraphIndex) !void {
        const prev_edge_count = self.edge_count;
        const prev_node_count = self.node_count;
        errdefer {
            self.edge_count = prev_edge_count;
            self.node_count = prev_node_count;
        }

        var read_txn = try self.beginReadReverseTxn();
        defer read_txn.abort();

        var meta_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (meta_keys.items) |key| self.alloc.free(key);
            meta_keys.deinit(self.alloc);
        }
        var node_refs = std.StringHashMapUnmanaged(u64).empty;
        defer {
            var key_it = node_refs.keyIterator();
            while (key_it.next()) |key| self.alloc.free(key.*);
            node_refs.deinit(self.alloc);
        }

        var edge_count: u64 = 0;
        var cur = try read_txn.openCursor();
        defer cur.close();
        var maybe_entry = try cur.first();
        while (maybe_entry) |entry| {
            if (std.mem.startsWith(u8, entry.key, graph_meta_prefix)) {
                if (!std.mem.startsWith(u8, entry.key, graph_metric_key_prefix)) {
                    try meta_keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
                }
            } else {
                edge_count += 1;
                if (try parseReverseEdgeKeyAlloc(self.alloc, entry.key)) |parsed_owned| {
                    var parsed = parsed_owned;
                    defer parsed.deinit(self.alloc);
                    try self.rememberNodeRefCount(&node_refs, parsed.source);
                    try self.rememberNodeRefCount(&node_refs, parsed.target);
                }
            }
            maybe_entry = try cur.next();
        }

        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        for (meta_keys.items) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }

        var node_count: u64 = 0;
        var refs_it = node_refs.iterator();
        while (refs_it.next()) |entry| {
            const ref_key = try graphNodeRefKeyAlloc(self.alloc, entry.key_ptr.*);
            defer self.alloc.free(ref_key);
            try putU64(&batch, ref_key, entry.value_ptr.*);
            node_count += 1;
        }

        self.edge_count = edge_count;
        self.node_count = node_count;
        try self.persistGraphCounters(&batch);
        try batch.commit();
    }

    fn openEdgeStore(alloc: Allocator, path: [*:0]const u8, opts: GraphIndexOptions) !OpenedReverseStore {
        switch (opts.reverse_backend) {
            .lmdb => {
                if (!supports_native_reverse_lmdb) return error.UnsupportedPlatform;
                const backend = try alloc.create(lmdb_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = try lmdb_backend.Backend.open(alloc, path, .{
                    .backend = .{
                        .durability = if (opts.no_sync) .none else .full,
                    },
                    .env = .{
                        .map_size = opts.map_size,
                        .no_sync = opts.no_sync,
                        .no_meta_sync = opts.no_meta_sync,
                        .no_tls = true,
                        .max_dbs = 1,
                    },
                });
                errdefer backend.close();

                var runtime = try backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{
                    .store = runtime,
                    .owner = .{ .lmdb = backend },
                };
            },
            .mem => {
                const backend = try alloc.create(mem_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = mem_backend.Backend.init(alloc, .{});
                errdefer backend.close();

                var runtime = try backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{
                    .store = runtime,
                    .owner = .{ .mem = backend },
                };
            },
            .lsm_memory => {
                var handle = try lsm_backend.BackendHandle.init(alloc, resolvedReverseLsmOptions(opts, true));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{
                    .store = runtime,
                    .owner = .{ .lsm = handle },
                };
            },
            .lsm => {
                var handle = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), resolvedReverseLsmOptions(opts, false));
                errdefer handle.close();

                var runtime = try handle.backend.runtimeStore(alloc, .{});
                errdefer runtime.deinit();
                return .{
                    .store = runtime,
                    .owner = .{ .lsm = handle },
                };
            },
        }
    }

    fn openReverseStore(alloc: Allocator, reverse_path: [*:0]const u8, opts: GraphIndexOptions) !OpenedReverseStore {
        return try openEdgeStore(alloc, reverse_path, opts);
    }

    /// Test/backward-compatible opener. The supplied store is ignored: graph
    /// edges live in private forward/reverse stores rooted under reverse_path.
    pub fn open(alloc: Allocator, main_store: anytype, reverse_path: [*:0]const u8, index_name: []const u8, opts: GraphIndexOptions) !GraphIndex {
        _ = main_store;
        const root = std.mem.span(reverse_path);
        const outgoing_raw = try std.fmt.allocPrint(alloc, "{s}/forward", .{root});
        defer alloc.free(outgoing_raw);
        const outgoing_path = try alloc.dupeZ(u8, outgoing_raw);
        defer alloc.free(outgoing_path);
        const reverse_raw = try std.fmt.allocPrint(alloc, "{s}/reverse", .{root});
        defer alloc.free(reverse_raw);
        const private_reverse_path = try alloc.dupeZ(u8, reverse_raw);
        defer alloc.free(private_reverse_path);
        return try openWithPrivateStores(alloc, outgoing_path, private_reverse_path, index_name, opts);
    }

    pub fn openWithPrivateStores(alloc: Allocator, outgoing_path: [*:0]const u8, reverse_path: [*:0]const u8, index_name: []const u8, opts: GraphIndexOptions) !GraphIndex {
        try validateGraphMetricEdgeFilters(opts.edge_type_configs, opts.metric_configs);

        var outgoing_store = try openEdgeStore(alloc, outgoing_path, opts);
        errdefer {
            outgoing_store.store.deinit();
            outgoing_store.owner.close(alloc);
        }
        var reverse_store = try openReverseStore(alloc, reverse_path, opts);
        errdefer {
            reverse_store.store.deinit();
            reverse_store.owner.close(alloc);
        }
        try outgoing_store.owner.ensureDurableEmptyManifest();
        try reverse_store.owner.ensureDurableEmptyManifest();
        const loaded_stats = try loadGraphCounters(&reverse_store.store);

        return .{
            .alloc = alloc,
            .index_name = index_name,
            .outgoing_store = outgoing_store.store,
            .outgoing_owner = outgoing_store.owner,
            .reverse_store = reverse_store.store,
            .reverse_owner = reverse_store.owner,
            .edge_type_configs = opts.edge_type_configs,
            .metric_configs = opts.metric_configs,
            .rebuild_root_path = if (opts.rebuild_root_path) |path| try alloc.dupe(u8, path) else null,
            .algebraic_semiring_traversal = opts.algebraic_semiring_traversal,
            .edge_count = loaded_stats.edge_count,
            .node_count = loaded_stats.node_count,
            .edge_generation = loaded_stats.edge_generation,
            .algebraic_traversal_attempt_count = 0,
            .algebraic_traversal_proven_count = 0,
            .algebraic_traversal_rejected_count = 0,
            .algebraic_traversal_fallback_count = 0,
            .algebraic_traversal_result_node_count = 0,
        };
    }

    pub fn close(self: *GraphIndex) void {
        self.outgoing_store.deinit();
        self.outgoing_owner.close(self.alloc);
        self.reverse_store.deinit();
        self.reverse_owner.close(self.alloc);
        if (self.rebuild_root_path) |path| self.alloc.free(path);
        self.* = undefined;
    }

    pub fn sync(self: *GraphIndex, force: bool) !void {
        try self.outgoing_owner.sync(force);
        try self.reverse_owner.sync(force);
    }

    pub fn syncReplayState(self: *GraphIndex) !void {
        try self.outgoing_owner.sync(false);
        try self.reverse_owner.sync(false);
    }

    pub fn supportsAlgebraicSemiringTraversal(self: *const GraphIndex) bool {
        return self.algebraic_semiring_traversal;
    }

    pub const AlgebraicTraversalRuntimeStats = struct {
        attempt_count: u64 = 0,
        proven_count: u64 = 0,
        rejected_count: u64 = 0,
        fallback_count: u64 = 0,
        result_node_count: u64 = 0,
    };

    pub fn noteAlgebraicTraversalAttempt(self: *GraphIndex) void {
        self.algebraic_traversal_attempt_count += 1;
    }

    pub fn noteAlgebraicTraversalProven(self: *GraphIndex, result_node_count: usize) void {
        self.algebraic_traversal_proven_count += 1;
        self.algebraic_traversal_result_node_count += @intCast(result_node_count);
    }

    pub fn noteAlgebraicTraversalRejected(self: *GraphIndex) void {
        self.algebraic_traversal_rejected_count += 1;
    }

    pub fn noteAlgebraicTraversalFallback(self: *GraphIndex) void {
        self.algebraic_traversal_fallback_count += 1;
    }

    pub fn algebraicTraversalRuntimeStats(self: *const GraphIndex) AlgebraicTraversalRuntimeStats {
        return .{
            .attempt_count = self.algebraic_traversal_attempt_count,
            .proven_count = self.algebraic_traversal_proven_count,
            .rejected_count = self.algebraic_traversal_rejected_count,
            .fallback_count = self.algebraic_traversal_fallback_count,
            .result_node_count = self.algebraic_traversal_result_node_count,
        };
    }

    pub const Stats = struct {
        edge_count: u64 = 0,
        node_count: u64 = 0,
        edge_generation: u64 = 0,
    };

    pub fn stats(self: *GraphIndex, alloc: Allocator) !Stats {
        _ = alloc;
        if (self.edge_count == 0 and self.node_count == 0) {
            const persisted = try loadGraphCounters(&self.reverse_store);
            if (persisted.edge_count != 0 or persisted.node_count != 0) {
                self.edge_count = persisted.edge_count;
                self.node_count = persisted.node_count;
                return persisted;
            }
        }
        return .{
            .edge_count = self.edge_count,
            .node_count = self.node_count,
        };
    }

    pub fn scanStats(self: *GraphIndex, alloc: Allocator) !Stats {
        var txn = try self.beginReadReverseTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var seen_nodes = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen_nodes.keyIterator();
            while (it.next()) |key| alloc.free(key.*);
            seen_nodes.deinit(alloc);
        }

        var first = (try cur.first()) orelse return .{};
        while (std.mem.startsWith(u8, first.key, graph_meta_prefix)) {
            first = (try cur.next()) orelse return .{};
        }
        var edge_count: u64 = 0;
        try rememberStatsNode(alloc, &seen_nodes, first.key);
        edge_count += 1;

        while (try cur.next()) |entry| {
            if (std.mem.startsWith(u8, entry.key, graph_meta_prefix)) continue;
            try rememberStatsNode(alloc, &seen_nodes, entry.key);
            edge_count += 1;
        }
        return .{
            .edge_count = edge_count,
            .node_count = seen_nodes.count(),
            .edge_generation = self.edge_generation,
        };
    }

    fn rememberStatsNode(
        alloc: Allocator,
        seen_nodes: *std.StringHashMapUnmanaged(void),
        key: []const u8,
    ) !void {
        var parsed = (try parseReverseEdgeKeyAlloc(alloc, key)) orelse return;
        defer parsed.deinit(alloc);
        try rememberStatsNodeValue(alloc, seen_nodes, parsed.source);
        try rememberStatsNodeValue(alloc, seen_nodes, parsed.target);
    }

    fn rememberStatsNodeValue(
        alloc: Allocator,
        seen_nodes: *std.StringHashMapUnmanaged(void),
        key: []const u8,
    ) !void {
        const result = try seen_nodes.getOrPut(alloc, key);
        if (result.found_existing) return;
        errdefer _ = seen_nodes.remove(key);
        result.key_ptr.* = try alloc.dupe(u8, key);
    }

    fn getTopologyMode(self: *const GraphIndex, edge_type: []const u8) TopologyMode {
        for (self.edge_type_configs) |cfg| {
            if (std.mem.eql(u8, cfg.name, edge_type)) return cfg.topology;
        }
        return .graph;
    }

    /// Add an edge (writes outgoing and reverse edge rows to private graph stores).
    /// Returns TreeTopologyViolation if the edge type has tree topology and
    /// the source already has an outgoing edge of that type to a different target.
    pub fn addEdge(
        self: *GraphIndex,
        source: []const u8,
        target: []const u8,
        edge_type: []const u8,
        weight: f64,
        created_at: u64,
        updated_at: u64,
        metadata: []const u8,
    ) !void {
        // Tree topology: source can have at most one outgoing edge of this type
        if (self.getTopologyMode(edge_type) == .tree) {
            const existing = try self.getEdges(self.alloc, source, edge_type, .out);
            defer freeEdges(self.alloc, existing);
            for (existing) |e| {
                if (!std.mem.eql(u8, e.target, target)) {
                    return TreeTopologyViolation.TreeTopologyViolation;
                }
            }
        }

        return try self.batchApply(&.{.{
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = weight,
            .created_at = created_at,
            .updated_at = updated_at,
            .metadata_json = metadata,
        }}, &.{});
    }

    pub fn batchApply(self: *GraphIndex, writes: []const BatchWrite, deletes: []const BatchDelete) !void {
        if (writes.len == 0 and deletes.len == 0) return;

        try self.validateTreeBatchWrites(writes, deletes);

        var main_batch = try self.beginWriteOutgoingBatch();
        errdefer main_batch.abort();

        var reverse_batch = try self.beginWriteReverseBatch();
        errdefer reverse_batch.abort();
        const prev_edge_count = self.edge_count;
        const prev_node_count = self.node_count;
        const prev_edge_generation = self.edge_generation;
        errdefer {
            self.edge_count = prev_edge_count;
            self.node_count = prev_node_count;
            self.edge_generation = prev_edge_generation;
        }

        for (deletes) |delete| {
            const out_key = try edgeKeyAlloc(self.alloc, delete.source, self.index_name, delete.edge_type, delete.target);
            defer self.alloc.free(out_key);
            main_batch.delete(out_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };

            const rev_key = try reverseEdgeKeyAlloc(self.alloc, delete.target, self.index_name, delete.edge_type, delete.source);
            defer self.alloc.free(rev_key);
            try self.accountReverseDelete(&reverse_batch, delete.source, delete.target, rev_key);
            reverse_batch.delete(rev_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }

        for (writes) |write| {
            var val_buf: [4096]u8 = undefined;
            const edge_val = encodeEdgeValue(&val_buf, write.weight, write.created_at, write.updated_at, write.metadata_json);

            const out_key = try edgeKeyAlloc(self.alloc, write.source, self.index_name, write.edge_type, write.target);
            defer self.alloc.free(out_key);
            try main_batch.put(out_key, edge_val);

            const rev_key = try reverseEdgeKeyAlloc(self.alloc, write.target, self.index_name, write.edge_type, write.source);
            defer self.alloc.free(rev_key);
            try self.accountReverseInsert(&reverse_batch, write.source, write.target, rev_key);
            try reverse_batch.put(rev_key, edge_val);
        }

        self.edge_generation += 1;
        try self.markMetricDirty(&reverse_batch);
        try self.persistGraphCounters(&reverse_batch);
        try main_batch.commit();
        try reverse_batch.commit();
    }

    /// Delete an edge (removes from both private graph stores).
    pub fn deleteEdge(self: *GraphIndex, source: []const u8, target: []const u8, edge_type: []const u8) !void {
        return try self.batchApply(&.{}, &.{.{
            .source = source,
            .target = target,
            .edge_type = edge_type,
        }});
    }

    /// Get edges connected to a key. Caller owns the returned slice and edge data.
    pub fn getEdges(self: *GraphIndex, alloc: Allocator, key: []const u8, edge_type: []const u8, direction: EdgeDirection) ![]Edge {
        var results = std.ArrayListUnmanaged(Edge).empty;
        errdefer {
            for (results.items) |e| freeEdge(alloc, e);
            results.deinit(alloc);
        }

        if (direction == .out or direction == .both) {
            try self.scanOutgoingEdges(alloc, &results, key, edge_type);
        }
        if (direction == .in or direction == .both) {
            try self.scanIncomingEdges(alloc, &results, key, edge_type);
        }

        const owned = try alloc.dupe(Edge, results.items);
        results.deinit(alloc);
        return owned;
    }

    fn scanOutgoingEdges(self: *GraphIndex, alloc: Allocator, results: *std.ArrayListUnmanaged(Edge), key: []const u8, edge_type: []const u8) !void {
        const prefix = try edgePrefixAlloc(alloc, key, self.index_name, edge_type);
        defer alloc.free(prefix);

        const pairs = try self.mainStoreScanPrefix(alloc, prefix);
        defer backend_scan.freeResults(alloc, pairs);

        for (pairs) |pair| {
            var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, pair.key)) orelse continue;
            defer parsed.deinit(alloc);
            const decoded = decodeEdgeValue(pair.value);
            try results.append(alloc, .{
                .source = try alloc.dupe(u8, parsed.source),
                .target = try alloc.dupe(u8, parsed.target),
                .edge_type = try alloc.dupe(u8, parsed.edge_type),
                .weight = decoded.weight,
                .created_at = decoded.created_at,
                .updated_at = decoded.updated_at,
                .metadata = try alloc.dupe(u8, decoded.metadata),
            });
        }
    }

    fn scanIncomingEdges(self: *GraphIndex, alloc: Allocator, results: *std.ArrayListUnmanaged(Edge), key: []const u8, edge_type: []const u8) !void {
        const prefix = try reverseEdgePrefixAlloc(alloc, key, self.index_name, edge_type);
        defer alloc.free(prefix);

        var txn = try self.beginReadReverseTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        const first = (try cur.seekAtOrAfter(prefix)) orelse return;

        if (std.mem.startsWith(u8, first.key, prefix)) {
            try appendReverseEdgeFromKV(alloc, results, first.key, first.value);
        } else {
            return;
        }

        while (try cur.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            try appendReverseEdgeFromKV(alloc, results, entry.key, entry.value);
        }
    }

    fn appendEdgeFromKV(alloc: Allocator, results: *std.ArrayListUnmanaged(Edge), key: []const u8, value: []const u8) !void {
        var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, key)) orelse return;
        defer parsed.deinit(alloc);
        try appendParsedEdge(alloc, results, parsed, value);
    }

    fn appendReverseEdgeFromKV(alloc: Allocator, results: *std.ArrayListUnmanaged(Edge), key: []const u8, value: []const u8) !void {
        var parsed = (try parseReverseEdgeKeyAlloc(alloc, key)) orelse return;
        defer parsed.deinit(alloc);
        try appendParsedEdge(alloc, results, parsed, value);
    }

    fn appendParsedEdge(alloc: Allocator, results: *std.ArrayListUnmanaged(Edge), parsed: ParsedGraphEdgeKey, value: []const u8) !void {
        const decoded = decodeEdgeValue(value);
        try results.append(alloc, .{
            .source = try alloc.dupe(u8, parsed.source),
            .target = try alloc.dupe(u8, parsed.target),
            .edge_type = try alloc.dupe(u8, parsed.edge_type),
            .weight = decoded.weight,
            .created_at = decoded.created_at,
            .updated_at = decoded.updated_at,
            .metadata = try alloc.dupe(u8, decoded.metadata),
        });
    }

    /// Delete all outgoing edges for a document (cleanup on doc deletion).
    pub fn deleteEdgesForDoc(self: *GraphIndex, doc_key: []const u8) !void {
        const edges = try self.getEdges(self.alloc, doc_key, "", .both);
        defer freeEdges(self.alloc, edges);

        var deletes = try self.alloc.alloc(BatchDelete, edges.len);
        defer self.alloc.free(deletes);
        for (edges, 0..) |edge, i| {
            deletes[i] = .{
                .source = edge.source,
                .target = edge.target,
                .edge_type = edge.edge_type,
            };
        }
        try self.batchApply(&.{}, deletes);
    }

    fn validateTreeBatchWrites(self: *GraphIndex, writes: []const BatchWrite, deletes: []const BatchDelete) !void {
        for (writes, 0..) |write, i| {
            if (self.getTopologyMode(write.edge_type) != .tree) continue;

            const existing = try self.getEdges(self.alloc, write.source, write.edge_type, .out);
            defer freeEdges(self.alloc, existing);

            for (existing) |edge| {
                if (containsBatchDelete(deletes, edge.source, edge.target, edge.edge_type)) continue;
                if (!std.mem.eql(u8, edge.target, write.target)) {
                    return TreeTopologyViolation.TreeTopologyViolation;
                }
            }

            for (writes[0..i]) |prior| {
                if (!std.mem.eql(u8, prior.source, write.source)) continue;
                if (!std.mem.eql(u8, prior.edge_type, write.edge_type)) continue;
                if (containsBatchDelete(deletes, prior.source, prior.target, prior.edge_type)) continue;
                if (!std.mem.eql(u8, prior.target, write.target)) {
                    return TreeTopologyViolation.TreeTopologyViolation;
                }
            }
        }
    }

    pub fn rebuildReverseFromOwnedOutgoingEdges(self: *GraphIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !usize {
        return try self.rebuildReverseFromOwnedOutgoingEdgesResume(alloc, lower, upper, null);
    }

    pub fn copyOwnedOutgoingEdgesTo(self: *GraphIndex, dest: *GraphIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !usize {
        const range_lower_owned = if (lower.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, lower) else null;
        defer if (range_lower_owned) |key| alloc.free(key);
        const range_upper_owned = if (upper.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, upper) else null;
        defer if (range_upper_owned) |key| alloc.free(key);
        const range_lower = range_lower_owned orelse "";
        const range_upper = range_upper_owned orelse "";

        const pairs = try self.mainStoreScanRange(alloc, range_lower, range_upper);
        defer backend_scan.freeResults(alloc, pairs);

        var batch = try dest.beginWriteOutgoingBatch();
        errdefer batch.abort();
        var copied: usize = 0;
        for (pairs) |pair| {
            var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, pair.key)) orelse continue;
            defer parsed.deinit(alloc);
            if (!std.mem.eql(u8, parsed.index_name, self.index_name)) continue;
            if (!std.mem.eql(u8, dest.index_name, self.index_name)) continue;
            try batch.put(pair.key, pair.value);
            copied += 1;
        }
        try batch.commit();
        return copied;
    }

    pub fn rebuildReverseFromOwnedOutgoingEdgesResume(
        self: *GraphIndex,
        alloc: Allocator,
        lower: []const u8,
        upper: []const u8,
        resume_from: ?[]const u8,
    ) !usize {
        const base_lower_owned = if (lower.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, lower) else null;
        defer if (base_lower_owned) |key| alloc.free(key);
        const range_upper_owned = if (upper.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, upper) else null;
        defer if (range_upper_owned) |key| alloc.free(key);
        const base_lower = base_lower_owned orelse "";
        const range_lower = if (resume_from) |key|
            if (key.len > 0 and std.mem.order(u8, key, base_lower) == .gt) key else base_lower
        else
            base_lower;
        const range_upper = range_upper_owned orelse "";

        const pairs = try self.mainStoreScanRange(alloc, range_lower, range_upper);
        defer backend_scan.freeResults(alloc, pairs);

        var rebuilt: usize = 0;
        var batch_count: usize = 0;
        var flushed_batches: usize = 0;
        var matching_edges: usize = 0;
        var txn = try self.beginWriteReverseTxn();
        var txn_active = true;
        errdefer if (txn_active) txn.abort();
        const rebuild_state = if (self.rebuild_root_path) |path| backfill_state_mod.RebuildState.init(path) else null;

        for (pairs) |pair| {
            if (resume_from) |resume_key| {
                if (resume_key.len > 0 and std.mem.order(u8, pair.key, resume_key) != .gt) continue;
            }
            var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, pair.key)) orelse continue;
            defer parsed.deinit(alloc);
            if (!std.mem.eql(u8, parsed.index_name, self.index_name)) continue;
            matching_edges += 1;

            const rev_key = try reverseEdgeKeyAlloc(alloc, parsed.target, self.index_name, parsed.edge_type, parsed.source);
            defer alloc.free(rev_key);
            try txn.put(rev_key, pair.value);
            rebuilt += 1;
            batch_count += 1;

            if (batch_count >= reverse_rebuild_batch_size) {
                try txn.commit();
                txn_active = false;
                if (rebuild_state) |state| try state.update(pair.key);
                flushed_batches += 1;
                if (@import("builtin").is_test) {
                    if (test_abort_reverse_rebuild_after_batches) |limit| {
                        if (flushed_batches >= limit) return error.TestInjectedBackfillFailure;
                    }
                }
                txn = try self.beginWriteReverseTxn();
                txn_active = true;
                batch_count = 0;
            }
        }

        try txn.commit();
        txn_active = false;
        if (rebuild_state) |state| try state.clear();
        try self.rebuildCounterMetadata();
        try self.checkpointLsmWalAfterDurableBoundary();
        return rebuilt;
    }

    pub fn pruneOwnedRange(self: *GraphIndex, alloc: Allocator, lower: []const u8, upper: []const u8) !usize {
        var removed: usize = 0;

        const range_lower_owned = if (lower.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, lower) else null;
        defer if (range_lower_owned) |key| alloc.free(key);
        const range_upper_owned = if (upper.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, upper) else null;
        defer if (range_upper_owned) |key| alloc.free(key);
        const range_lower = range_lower_owned orelse "";
        const range_upper = range_upper_owned orelse "";

        const owned_pairs = try self.mainStoreScanRange(alloc, range_lower, range_upper);
        defer backend_scan.freeResults(alloc, owned_pairs);

        var reverse_txn = try self.beginWriteReverseTxn();
        errdefer reverse_txn.abort();

        for (owned_pairs) |pair| {
            var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, pair.key)) orelse continue;
            defer parsed.deinit(alloc);
            if (!std.mem.eql(u8, parsed.index_name, self.index_name)) continue;

            const rev_key = try reverseEdgeKeyAlloc(alloc, parsed.target, self.index_name, parsed.edge_type, parsed.source);
            defer alloc.free(rev_key);
            reverse_txn.delete(rev_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            removed += 1;
        }

        var keys_to_delete = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (keys_to_delete.items) |key| alloc.free(key);
            keys_to_delete.deinit(alloc);
        }

        {
            var cur = try reverse_txn.openCursor();
            defer cur.close();

            if (try cur.seekAtOrAfter(range_lower)) |initial_entry| {
                var entry = initial_entry;
                while (true) {
                    if (range_upper.len > 0 and std.mem.order(u8, entry.key, range_upper) != .lt) break;
                    if (try parseReverseEdgeKeyAlloc(alloc, entry.key)) |parsed_owned| {
                        var parsed = parsed_owned;
                        defer parsed.deinit(alloc);
                        if (std.mem.eql(u8, parsed.index_name, self.index_name)) {
                            try keys_to_delete.append(alloc, try alloc.dupe(u8, entry.key));
                        }
                    }
                    entry = (try cur.next()) orelse break;
                }
            }
        }

        for (keys_to_delete.items) |key| {
            reverse_txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            removed += 1;
        }

        try reverse_txn.commit();
        try self.rebuildCounterMetadata();
        return removed;
    }

    fn mainStoreScanPrefix(self: *GraphIndex, alloc: Allocator, prefix: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanPrefix(alloc, &self.outgoing_store, prefix);
    }

    fn mainStoreScanRange(self: *GraphIndex, alloc: Allocator, lower: []const u8, upper: []const u8) ![]backend_scan.OwnedKVPair {
        return try backend_scan.scanRange(alloc, &self.outgoing_store, lower, upper);
    }

    fn containsBatchDelete(
        deletes: []const BatchDelete,
        source: []const u8,
        target: []const u8,
        edge_type: []const u8,
    ) bool {
        for (deletes) |delete| {
            if (!std.mem.eql(u8, delete.source, source)) continue;
            if (!std.mem.eql(u8, delete.target, target)) continue;
            if (!std.mem.eql(u8, delete.edge_type, edge_type)) continue;
            return true;
        }
        return false;
    }

    pub const GraphMetricState = enum {
        disabled,
        not_ready,
        fresh,
        stale,
        building,
        failed,
    };

    pub const GraphMetricBuildPhase = enum {
        idle,
        computing,
        publishing,
        complete,
        prepare_generation,
        scan_edges_and_out_degree,
        initialize_ranks,
        iterate_contributions,
        reduce_ranks,
        check_convergence,
        publish_generation,
        cleanup_old_generations,
    };

    pub const GraphMetricEventKind = enum(u8) {
        publish,
        delete,
        pause,
        @"resume",
        failed,
    };

    pub const GraphMetricEvent = struct {
        sequence: u64 = 0,
        kind: GraphMetricEventKind,
        at_ms: u64 = 0,
        target_edge_generation: u64 = 0,
        published_generation: u64 = 0,
        score_count: u64 = 0,
    };

    pub const GraphMetricStatus = struct {
        name: []const u8,
        state: GraphMetricState = .not_ready,
        phase: GraphMetricBuildPhase = .idle,
        edge_filter: GraphMetricEdgeFilter = .{},
        metadata_version: u32 = 0,
        maintenance_paused: bool = false,
        build_queued: bool = false,
        published_generation: u64 = 0,
        edge_generation: u64 = 0,
        target_edge_generation: u64 = 0,
        queued_generation: u64 = 0,
        building_generation: u64 = 0,
        build_job_id: u64 = 0,
        build_started_at_ms: u64 = 0,
        build_iteration: u32 = 0,
        build_lease_expires_at_ms: u64 = 0,
        build_worker_id: []const u8 = "",
        build_cursor: []const u8 = "",
        build_completed_units: u64 = 0,
        build_total_units: u64 = 0,
        retry_count: u64 = 0,
        last_error: []const u8 = "",
        progress: f64 = 0.0,
        converged: bool = false,
        iterations_completed: u32 = 0,
        delta: f64 = 0.0,
        computed_at_ms: u64 = 0,
        last_event: ?GraphMetricEvent = null,
        recent_events: []GraphMetricEvent = &.{},

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.name);
            self.edge_filter.deinit(alloc);
            if (self.build_worker_id.len > 0) alloc.free(self.build_worker_id);
            if (self.build_cursor.len > 0) alloc.free(self.build_cursor);
            if (self.last_error.len > 0) alloc.free(self.last_error);
            if (self.recent_events.len > 0) alloc.free(self.recent_events);
            self.* = undefined;
        }
    };

    pub const GraphMetricScore = struct {
        node: []const u8,
        score: f64,

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.node);
            self.* = undefined;
        }
    };

    const GraphMetricMeta = struct {
        schema_version: u32 = graph_metric_meta_schema_version,
        converged: bool = false,
        iterations_completed: u32 = 0,
        delta: f64 = 0.0,
        computed_at_ms: u64 = 0,
        config_fingerprint: u64 = 0,
        edge_filter: GraphMetricEdgeFilter = .{},
    };

    const GraphMetricBuildLease = struct {
        job_id: u64 = 0,
        target_generation: u64 = 0,
        started_at_ms: u64 = 0,
        lease_expires_at_ms: u64 = 0,
        phase: GraphMetricBuildPhase = .computing,
        iteration: u32 = 0,
        worker_id: []const u8 = graph_metric_local_build_worker_id,
    };

    const GraphMetricBuildJob = struct {
        job_id: u64 = 0,
        target_generation: u64 = 0,
        score_generation: u64 = 0,
        started_at_ms: u64 = 0,
        updated_at_ms: u64 = 0,
        lease_expires_at_ms: u64 = 0,
        phase: GraphMetricBuildPhase = .computing,
        iteration: u32 = 0,
        retry_count: u64 = 0,
        worker_id: []const u8 = graph_metric_local_build_worker_id,
        last_error: []const u8 = "",
        cursor: []const u8 = "",
        completed_units: u64 = 0,
        total_units: u64 = 0,
    };

    const GraphMetricFailureDetail = struct {
        retry_count: u64 = 0,
        last_error: []const u8 = "",

        fn deinit(self: GraphMetricFailureDetail, alloc: Allocator) void {
            if (self.last_error.len > 0) alloc.free(self.last_error);
        }
    };

    pub const graph_metric_meta_schema_version: u32 = 3;
    const graph_metric_meta_legacy_encoded_len = 8 + 8 + 8 + 8;
    const graph_metric_meta_v1_encoded_len = 8 + graph_metric_meta_legacy_encoded_len;
    const graph_metric_meta_encoded_len = graph_metric_meta_v1_encoded_len + 8;
    const graph_metric_edge_filter_header_len = 8 + 8;
    const graph_metric_build_lease_legacy_encoded_len = 8 + 8 + 8;
    const graph_metric_build_lease_v1_header_len = 8 + graph_metric_build_lease_legacy_encoded_len + 8 + 8;
    const graph_metric_build_lease_v2_header_len = graph_metric_build_lease_v1_header_len + 8;
    const graph_metric_build_lease_header_len = graph_metric_build_lease_v2_header_len + 8;
    const graph_metric_build_job_v1_header_len = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8;
    const graph_metric_build_job_v2_header_len = graph_metric_build_job_v1_header_len + 8 + 8;
    const graph_metric_build_job_header_len = graph_metric_build_job_v2_header_len + 8 + 8 + 8;
    const graph_metric_failure_detail_header_len = 8;
    const graph_metric_event_encoded_len = 8 + 8 + 8 + 8 + 8;

    fn encodeGraphMetricMeta(meta: GraphMetricMeta, out: *[graph_metric_meta_encoded_len]u8) void {
        var offset: usize = 0;
        inline for (.{
            @as(u64, meta.schema_version),
            if (meta.converged) @as(u64, 1) else @as(u64, 0),
            @as(u64, meta.iterations_completed),
            @as(u64, @bitCast(meta.delta)),
            meta.computed_at_ms,
            meta.config_fingerprint,
        }) |value| {
            std.mem.writeInt(u64, out[offset..][0..8], value, .little);
            offset += 8;
        }
    }

    fn decodeGraphMetricMeta(raw: []const u8) ?GraphMetricMeta {
        if (raw.len != graph_metric_meta_encoded_len and raw.len != graph_metric_meta_v1_encoded_len and raw.len != graph_metric_meta_legacy_encoded_len) return null;
        var offset: usize = 0;
        const has_schema_version = raw.len == graph_metric_meta_encoded_len or raw.len == graph_metric_meta_v1_encoded_len;
        const schema_version: u32 = if (has_schema_version) blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            if (value > graph_metric_meta_schema_version) return null;
            break :blk @intCast(value);
        } else 0;
        const converged = std.mem.readInt(u64, raw[offset..][0..8], .little) != 0;
        offset += 8;
        const iterations_completed: u32 = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
        offset += 8;
        const delta_bits = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const computed_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const config_fingerprint = if (raw.len == graph_metric_meta_encoded_len)
            std.mem.readInt(u64, raw[offset..][0..8], .little)
        else
            0;
        return .{
            .schema_version = schema_version,
            .converged = converged,
            .iterations_completed = iterations_completed,
            .delta = @bitCast(delta_bits),
            .computed_at_ms = computed_at_ms,
            .config_fingerprint = config_fingerprint,
        };
    }

    fn graphMetricEdgeFilterEncodedLen(filter: GraphMetricEdgeFilter) usize {
        var len: usize = graph_metric_edge_filter_header_len;
        for (filter.types) |edge_type| len += 8 + edge_type.len;
        return len;
    }

    fn encodeGraphMetricEdgeFilter(filter: GraphMetricEdgeFilter, out: []u8) void {
        std.debug.assert(out.len == graphMetricEdgeFilterEncodedLen(filter));
        std.mem.writeInt(u64, out[0..8], @intFromEnum(filter.mode), .little);
        std.mem.writeInt(u64, out[8..16], filter.types.len, .little);
        var offset: usize = graph_metric_edge_filter_header_len;
        for (filter.types) |edge_type| {
            std.mem.writeInt(u64, out[offset..][0..8], edge_type.len, .little);
            offset += 8;
            @memcpy(out[offset..][0..edge_type.len], edge_type);
            offset += edge_type.len;
        }
    }

    fn decodeGraphMetricEdgeFilterAlloc(alloc: Allocator, raw: []const u8) !?GraphMetricEdgeFilter {
        if (raw.len < graph_metric_edge_filter_header_len) return null;
        const mode_raw = std.mem.readInt(u64, raw[0..8], .little);
        const mode: GraphMetricEdgeFilterMode = switch (mode_raw) {
            @intFromEnum(GraphMetricEdgeFilterMode.all) => .all,
            @intFromEnum(GraphMetricEdgeFilterMode.types) => .types,
            else => return null,
        };
        const count: usize = @intCast(std.mem.readInt(u64, raw[8..16], .little));
        var offset: usize = graph_metric_edge_filter_header_len;
        if (mode == .all) {
            if (count != 0 or offset != raw.len) return null;
            return .{};
        }
        if (count == 0) return null;
        const types = try alloc.alloc([]const u8, count);
        var initialized: usize = 0;
        errdefer {
            for (types[0..initialized]) |edge_type| alloc.free(edge_type);
            alloc.free(types);
        }
        for (types) |*slot| {
            if (offset + 8 > raw.len) return error.InvalidGraphMetricEdgeFilterMetadata;
            const edge_type_len: usize = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
            offset += 8;
            if (edge_type_len == 0 or offset + edge_type_len > raw.len) return error.InvalidGraphMetricEdgeFilterMetadata;
            slot.* = try alloc.dupe(u8, raw[offset..][0..edge_type_len]);
            initialized += 1;
            offset += edge_type_len;
        }
        if (offset != raw.len) return error.InvalidGraphMetricEdgeFilterMetadata;
        return .{ .mode = .types, .types = types };
    }

    fn graphMetricBuildLeaseEncodedLen(lease: GraphMetricBuildLease) usize {
        return graph_metric_build_lease_header_len + lease.worker_id.len;
    }

    fn encodeGraphMetricBuildLease(lease: GraphMetricBuildLease, out: []u8) void {
        std.debug.assert(out.len == graphMetricBuildLeaseEncodedLen(lease));
        var offset: usize = 0;
        inline for (.{
            @as(u64, 3),
            lease.target_generation,
            lease.started_at_ms,
            lease.lease_expires_at_ms,
            @as(u64, @intFromEnum(lease.phase)),
            @as(u64, lease.iteration),
            lease.job_id,
            @as(u64, lease.worker_id.len),
        }) |value| {
            std.mem.writeInt(u64, out[offset..][0..8], value, .little);
            offset += 8;
        }
        @memcpy(out[offset..][0..lease.worker_id.len], lease.worker_id);
    }

    fn decodeGraphMetricBuildLease(raw: []const u8) ?GraphMetricBuildLease {
        if (raw.len == graph_metric_build_lease_legacy_encoded_len) {
            var offset: usize = 0;
            const target_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            const started_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            return .{
                .job_id = started_at_ms,
                .target_generation = target_generation,
                .started_at_ms = started_at_ms,
                .lease_expires_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little),
                .phase = .computing,
                .worker_id = graph_metric_local_build_worker_id,
            };
        }
        if (raw.len < graph_metric_build_lease_v1_header_len) return null;
        var offset: usize = 0;
        const version = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        if (version != 1 and version != 2 and version != 3) return null;
        const target_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const started_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const lease_expires_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const phase_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const phase: GraphMetricBuildPhase = switch (phase_raw) {
            @intFromEnum(GraphMetricBuildPhase.idle) => .idle,
            @intFromEnum(GraphMetricBuildPhase.computing) => .computing,
            @intFromEnum(GraphMetricBuildPhase.publishing) => .publishing,
            @intFromEnum(GraphMetricBuildPhase.complete) => .complete,
            @intFromEnum(GraphMetricBuildPhase.prepare_generation) => .prepare_generation,
            @intFromEnum(GraphMetricBuildPhase.scan_edges_and_out_degree) => .scan_edges_and_out_degree,
            @intFromEnum(GraphMetricBuildPhase.initialize_ranks) => .initialize_ranks,
            @intFromEnum(GraphMetricBuildPhase.iterate_contributions) => .iterate_contributions,
            @intFromEnum(GraphMetricBuildPhase.reduce_ranks) => .reduce_ranks,
            @intFromEnum(GraphMetricBuildPhase.check_convergence) => .check_convergence,
            @intFromEnum(GraphMetricBuildPhase.publish_generation) => .publish_generation,
            @intFromEnum(GraphMetricBuildPhase.cleanup_old_generations) => .cleanup_old_generations,
            else => return null,
        };
        const iteration: u32 = if (version == 2 or version == 3) blk: {
            if (raw.len < graph_metric_build_lease_v2_header_len) return null;
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            if (value > std.math.maxInt(u32)) return null;
            break :blk @intCast(value);
        } else 0;
        const job_id: u64 = if (version == 3) blk: {
            if (raw.len < graph_metric_build_lease_header_len) return null;
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        } else started_at_ms;
        const worker_id_len: usize = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
        offset += 8;
        if (worker_id_len == 0 or offset + worker_id_len != raw.len) return null;
        return .{
            .job_id = job_id,
            .target_generation = target_generation,
            .started_at_ms = started_at_ms,
            .lease_expires_at_ms = lease_expires_at_ms,
            .phase = phase,
            .iteration = iteration,
            .worker_id = raw[offset..][0..worker_id_len],
        };
    }

    fn graphMetricBuildJobEncodedLen(job: GraphMetricBuildJob) usize {
        return graph_metric_build_job_header_len + job.worker_id.len + job.last_error.len + job.cursor.len;
    }

    fn encodeGraphMetricBuildJob(job: GraphMetricBuildJob, out: []u8) void {
        std.debug.assert(out.len == graphMetricBuildJobEncodedLen(job));
        var offset: usize = 0;
        inline for (.{
            @as(u64, 3),
            job.job_id,
            job.target_generation,
            job.score_generation,
            job.started_at_ms,
            job.updated_at_ms,
            job.lease_expires_at_ms,
            @as(u64, @intFromEnum(job.phase)),
            @as(u64, job.iteration),
            job.retry_count,
            job.completed_units,
            job.total_units,
            @as(u64, job.worker_id.len),
            @as(u64, job.last_error.len),
            @as(u64, job.cursor.len),
        }) |value| {
            std.mem.writeInt(u64, out[offset..][0..8], value, .little);
            offset += 8;
        }
        @memcpy(out[offset..][0..job.worker_id.len], job.worker_id);
        offset += job.worker_id.len;
        @memcpy(out[offset..][0..job.last_error.len], job.last_error);
        offset += job.last_error.len;
        @memcpy(out[offset..][0..job.cursor.len], job.cursor);
    }

    fn decodeGraphMetricBuildJob(raw: []const u8) ?GraphMetricBuildJob {
        if (raw.len < graph_metric_build_job_v1_header_len) return null;
        var offset: usize = 0;
        const version = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        if (version != 1 and version != 2 and version != 3) return null;
        const job_id = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const target_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const score_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const started_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const updated_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const lease_expires_at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const phase_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const phase: GraphMetricBuildPhase = switch (phase_raw) {
            @intFromEnum(GraphMetricBuildPhase.idle) => .idle,
            @intFromEnum(GraphMetricBuildPhase.computing) => .computing,
            @intFromEnum(GraphMetricBuildPhase.publishing) => .publishing,
            @intFromEnum(GraphMetricBuildPhase.complete) => .complete,
            @intFromEnum(GraphMetricBuildPhase.prepare_generation) => .prepare_generation,
            @intFromEnum(GraphMetricBuildPhase.scan_edges_and_out_degree) => .scan_edges_and_out_degree,
            @intFromEnum(GraphMetricBuildPhase.initialize_ranks) => .initialize_ranks,
            @intFromEnum(GraphMetricBuildPhase.iterate_contributions) => .iterate_contributions,
            @intFromEnum(GraphMetricBuildPhase.reduce_ranks) => .reduce_ranks,
            @intFromEnum(GraphMetricBuildPhase.check_convergence) => .check_convergence,
            @intFromEnum(GraphMetricBuildPhase.publish_generation) => .publish_generation,
            @intFromEnum(GraphMetricBuildPhase.cleanup_old_generations) => .cleanup_old_generations,
            else => return null,
        };
        const iteration_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        if (iteration_raw > std.math.maxInt(u32)) return null;
        const retry_count = if (version >= 2) blk: {
            if (raw.len < graph_metric_build_job_v2_header_len) return null;
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        } else 0;
        const completed_units = if (version >= 3) blk: {
            if (raw.len < graph_metric_build_job_header_len) return null;
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        } else 0;
        const total_units = if (version >= 3) blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        } else 0;
        const worker_id_len: usize = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
        offset += 8;
        const last_error_len: usize = if (version >= 2) blk: {
            const value: usize = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
            offset += 8;
            break :blk value;
        } else 0;
        const cursor_len: usize = if (version >= 3) blk: {
            const value: usize = @intCast(std.mem.readInt(u64, raw[offset..][0..8], .little));
            offset += 8;
            break :blk value;
        } else 0;
        if (worker_id_len == 0 or offset + worker_id_len + last_error_len + cursor_len != raw.len) return null;
        const worker_id = raw[offset..][0..worker_id_len];
        offset += worker_id_len;
        const last_error = raw[offset..][0..last_error_len];
        offset += last_error_len;
        return .{
            .job_id = job_id,
            .target_generation = target_generation,
            .score_generation = score_generation,
            .started_at_ms = started_at_ms,
            .updated_at_ms = updated_at_ms,
            .lease_expires_at_ms = lease_expires_at_ms,
            .phase = phase,
            .iteration = @intCast(iteration_raw),
            .retry_count = retry_count,
            .worker_id = worker_id,
            .last_error = last_error,
            .cursor = raw[offset..][0..cursor_len],
            .completed_units = completed_units,
            .total_units = total_units,
        };
    }

    fn encodeGraphMetricFailureDetail(detail: GraphMetricFailureDetail, out: []u8) void {
        std.debug.assert(out.len == graph_metric_failure_detail_header_len + detail.last_error.len);
        std.mem.writeInt(u64, out[0..8], detail.retry_count, .little);
        @memcpy(out[graph_metric_failure_detail_header_len..], detail.last_error);
    }

    fn decodeGraphMetricFailureDetailAlloc(alloc: Allocator, raw: []const u8) !?GraphMetricFailureDetail {
        if (raw.len < graph_metric_failure_detail_header_len) return null;
        const retry_count = std.mem.readInt(u64, raw[0..8], .little);
        const last_error = try alloc.dupe(u8, raw[graph_metric_failure_detail_header_len..]);
        return .{ .retry_count = retry_count, .last_error = last_error };
    }

    fn encodeGraphMetricEvent(event: GraphMetricEvent, out: *[graph_metric_event_encoded_len]u8) void {
        var offset: usize = 0;
        inline for (.{
            @as(u64, @intFromEnum(event.kind)),
            event.at_ms,
            event.target_edge_generation,
            event.published_generation,
            event.score_count,
        }) |value| {
            std.mem.writeInt(u64, out[offset..][0..8], value, .little);
            offset += 8;
        }
    }

    fn decodeGraphMetricEvent(sequence: u64, raw: []const u8) ?GraphMetricEvent {
        if (raw.len != graph_metric_event_encoded_len) return null;
        var offset: usize = 0;
        const kind_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const kind: GraphMetricEventKind = switch (kind_raw) {
            @intFromEnum(GraphMetricEventKind.publish) => .publish,
            @intFromEnum(GraphMetricEventKind.delete) => .delete,
            @intFromEnum(GraphMetricEventKind.pause) => .pause,
            @intFromEnum(GraphMetricEventKind.@"resume") => .@"resume",
            @intFromEnum(GraphMetricEventKind.failed) => .failed,
            else => return null,
        };
        const at_ms = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const target_edge_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        const published_generation = std.mem.readInt(u64, raw[offset..][0..8], .little);
        offset += 8;
        return .{
            .sequence = sequence,
            .kind = kind,
            .at_ms = at_ms,
            .target_edge_generation = target_edge_generation,
            .published_generation = published_generation,
            .score_count = std.mem.readInt(u64, raw[offset..][0..8], .little),
        };
    }

    fn putF64(batch: anytype, key: []const u8, value: f64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(value), .little);
        try batch.put(key, &buf);
    }

    fn decodeF64(raw: []const u8) ?f64 {
        if (raw.len < 8) return null;
        const bits = std.mem.readInt(u64, raw[0..8], .little);
        return @bitCast(bits);
    }

    fn metricConfig(self: *const GraphIndex, metric_name: []const u8) ?GraphMetricConfig {
        for (self.metric_configs) |cfg| {
            if (std.mem.eql(u8, cfg.name, metric_name)) return cfg;
        }
        return null;
    }

    fn graphMetricEdgeAllowed(filter: GraphMetricEdgeFilter, edge_type: []const u8) bool {
        return switch (filter.mode) {
            .all => true,
            .types => blk: {
                for (filter.types) |allowed| {
                    if (std.mem.eql(u8, allowed, edge_type)) break :blk true;
                }
                break :blk false;
            },
        };
    }

    fn graphMetricEdgeFiltersEqual(a: GraphMetricEdgeFilter, b: GraphMetricEdgeFilter) bool {
        if (a.mode != b.mode or a.types.len != b.types.len) return false;
        for (a.types, b.types) |a_type, b_type| {
            if (!std.mem.eql(u8, a_type, b_type)) return false;
        }
        return true;
    }

    fn graphMetricConfigFingerprintHashU64(hasher: *std.hash.Wyhash, value: u64) void {
        var raw = value;
        hasher.update(std.mem.asBytes(&raw));
    }

    fn graphMetricConfigFingerprint(cfg: GraphMetricConfig) u64 {
        var hasher = std.hash.Wyhash.init(0);
        graphMetricConfigFingerprintHashU64(&hasher, @intFromEnum(cfg.kind));
        graphMetricConfigFingerprintHashU64(&hasher, @as(u64, @bitCast(cfg.damping)));
        graphMetricConfigFingerprintHashU64(&hasher, @as(u64, @bitCast(cfg.tolerance)));
        graphMetricConfigFingerprintHashU64(&hasher, cfg.max_iterations);
        graphMetricConfigFingerprintHashU64(&hasher, @intFromEnum(cfg.edge_filter.mode));
        graphMetricConfigFingerprintHashU64(&hasher, cfg.edge_filter.types.len);
        for (cfg.edge_filter.types) |edge_type| {
            graphMetricConfigFingerprintHashU64(&hasher, edge_type.len);
            hasher.update(edge_type);
        }
        return hasher.final();
    }

    fn graphMetricBuildJobId(metric_name: []const u8, target_generation: u64, started_at_ms: u64) u64 {
        var hasher = std.hash.Wyhash.init(0xA17F_6D3B_2C91_5E44);
        hasher.update(metric_name);
        graphMetricConfigFingerprintHashU64(&hasher, target_generation);
        graphMetricConfigFingerprintHashU64(&hasher, started_at_ms);
        const id = hasher.final();
        return if (id == 0) 1 else id;
    }

    fn graphMetricActiveBuildProgress(cfg: GraphMetricConfig, phase: GraphMetricBuildPhase, iteration: u32) f64 {
        return switch (phase) {
            .idle => 0.0,
            .prepare_generation => 0.01,
            .scan_edges_and_out_degree => 0.05,
            .initialize_ranks => 0.1,
            .computing => blk: {
                if (iteration == 0 or cfg.max_iterations == 0) break :blk 0.01;
                const progress = @as(f64, @floatFromInt(iteration)) / @as(f64, @floatFromInt(cfg.max_iterations));
                break :blk @min(0.98, @max(0.01, progress));
            },
            .iterate_contributions => blk: {
                if (iteration == 0 or cfg.max_iterations == 0) break :blk 0.1;
                const progress = @as(f64, @floatFromInt(iteration)) / @as(f64, @floatFromInt(cfg.max_iterations));
                break :blk @min(0.9, @max(0.1, progress));
            },
            .reduce_ranks => 0.92,
            .check_convergence => 0.95,
            .publishing, .publish_generation => 0.99,
            .cleanup_old_generations => 0.995,
            .complete => 1.0,
        };
    }

    const PageRankNode = struct {
        key: []u8,
        rank: f64,
        next_rank: f64 = 0.0,
        out_degree: u64 = 0,
    };

    const PageRankEdge = struct {
        source: usize,
        target: usize,
    };

    const DegreeNode = struct {
        key: []u8,
        degree: u64 = 0,
    };

    fn getOrPutPageRankNode(
        self: *GraphIndex,
        map: *std.StringHashMapUnmanaged(usize),
        nodes: *std.ArrayListUnmanaged(PageRankNode),
        key: []const u8,
    ) !usize {
        if (map.get(key)) |idx| return idx;
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        const idx = nodes.items.len;
        try nodes.append(self.alloc, .{ .key = owned, .rank = 0.0 });
        errdefer _ = nodes.pop();
        try map.put(self.alloc, owned, idx);
        return idx;
    }

    fn getOrPutDegreeNode(
        self: *GraphIndex,
        map: *std.StringHashMapUnmanaged(usize),
        nodes: *std.ArrayListUnmanaged(DegreeNode),
        key: []const u8,
    ) !usize {
        if (map.get(key)) |idx| return idx;
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        const idx = nodes.items.len;
        try nodes.append(self.alloc, .{ .key = owned });
        errdefer _ = nodes.pop();
        try map.put(self.alloc, owned, idx);
        return idx;
    }

    fn freePageRankNodes(self: *GraphIndex, nodes: []PageRankNode) void {
        for (nodes) |node| self.alloc.free(node.key);
    }

    fn freeDegreeNodes(self: *GraphIndex, nodes: []DegreeNode) void {
        for (nodes) |node| self.alloc.free(node.key);
    }

    fn collectPageRankGraph(
        self: *GraphIndex,
        filter: GraphMetricEdgeFilter,
        nodes: *std.ArrayListUnmanaged(PageRankNode),
        edges: *std.ArrayListUnmanaged(PageRankEdge),
    ) !void {
        var map = std.StringHashMapUnmanaged(usize).empty;
        defer map.deinit(self.alloc);

        var txn = try self.beginReadReverseTxn();
        defer txn.abort();
        var cur = try txn.openCursor();
        defer cur.close();
        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (std.mem.startsWith(u8, entry.key, graph_meta_prefix)) continue;
            var parsed = (try parseReverseEdgeKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer parsed.deinit(self.alloc);
            if (!std.mem.eql(u8, parsed.index_name, self.index_name)) continue;
            if (!graphMetricEdgeAllowed(filter, parsed.edge_type)) continue;
            const source_idx = try self.getOrPutPageRankNode(&map, nodes, parsed.source);
            const target_idx = try self.getOrPutPageRankNode(&map, nodes, parsed.target);
            nodes.items[source_idx].out_degree += 1;
            try edges.append(self.alloc, .{ .source = source_idx, .target = target_idx });
        }
    }

    fn collectDegreeGraph(
        self: *GraphIndex,
        filter: GraphMetricEdgeFilter,
        nodes: *std.ArrayListUnmanaged(DegreeNode),
    ) !void {
        var map = std.StringHashMapUnmanaged(usize).empty;
        defer map.deinit(self.alloc);

        var txn = try self.beginReadReverseTxn();
        defer txn.abort();
        var cur = try txn.openCursor();
        defer cur.close();
        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (std.mem.startsWith(u8, entry.key, graph_meta_prefix)) continue;
            var parsed = (try parseReverseEdgeKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer parsed.deinit(self.alloc);
            if (!std.mem.eql(u8, parsed.index_name, self.index_name)) continue;
            if (!graphMetricEdgeAllowed(filter, parsed.edge_type)) continue;
            const source_idx = try self.getOrPutDegreeNode(&map, nodes, parsed.source);
            const target_idx = try self.getOrPutDegreeNode(&map, nodes, parsed.target);
            nodes.items[source_idx].degree += 1;
            nodes.items[target_idx].degree += 1;
        }
    }

    fn deleteScoreGeneration(self: *GraphIndex, batch: anytype, metric_name: []const u8, generation: u64) !void {
        if (generation == 0) return;
        const prefix = try self.graphMetricScorePrefixAlloc(metric_name, generation);
        defer self.alloc.free(prefix);
        var keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (keys.items) |key| self.alloc.free(key);
            keys.deinit(self.alloc);
        }
        var cur = try batch.openCursor();
        defer cur.close();
        var entry_opt = try cur.seekAtOrAfter(prefix);
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            try keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
        }
        for (keys.items) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const meta_key = try self.graphMetricMetaKeyAlloc(metric_name, generation);
        defer self.alloc.free(meta_key);
        batch.delete(meta_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        const edge_filter_key = try self.graphMetricMetaEdgeFilterKeyAlloc(metric_name, generation);
        defer self.alloc.free(edge_filter_key);
        batch.delete(edge_filter_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        const config_fingerprint_key = try self.graphMetricMetaConfigFingerprintKeyAlloc(metric_name, generation);
        defer self.alloc.free(config_fingerprint_key);
        batch.delete(config_fingerprint_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    pub fn deleteGraphMetricMaterialization(self: *GraphIndex, metric_name: []const u8) !void {
        _ = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        const prefix = try self.graphMetricKeyAlloc(&.{metric_name});
        defer self.alloc.free(prefix);
        const control_prefix = try self.graphMetricControlKeyAlloc(&.{metric_name});
        defer self.alloc.free(control_prefix);

        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        var keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (keys.items) |key| self.alloc.free(key);
            keys.deinit(self.alloc);
        }
        var cur = try batch.openCursor();
        defer cur.close();
        var entry_opt = try cur.seekAtOrAfter(prefix);
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            try keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
        }
        entry_opt = try cur.seekAtOrAfter(control_prefix);
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, control_prefix)) break;
            try keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
        }
        for (keys.items) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        try self.appendGraphMetricEvent(&batch, metric_name, .{
            .kind = .delete,
            .at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .target_edge_generation = self.edge_generation,
            .published_generation = 0,
            .score_count = 0,
        });
        try batch.commit();
    }

    pub fn pauseGraphMetricMaintenance(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        _ = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const key = try self.graphMetricMaintenancePausedKeyAlloc(metric_name);
        defer self.alloc.free(key);
        try putU64(&batch, key, 1);
        try self.appendGraphMetricEvent(&batch, metric_name, .{
            .kind = .pause,
            .at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .target_edge_generation = self.edge_generation,
            .published_generation = try self.metricPublishedGeneration(&batch, metric_name),
            .score_count = 0,
        });
        try batch.commit();
        return try self.graphMetricStatus(metric_name);
    }

    pub fn resumeGraphMetricMaintenance(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        _ = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const key = try self.graphMetricMaintenancePausedKeyAlloc(metric_name);
        defer self.alloc.free(key);
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try self.appendGraphMetricEvent(&batch, metric_name, .{
            .kind = .@"resume",
            .at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .target_edge_generation = self.edge_generation,
            .published_generation = try self.metricPublishedGeneration(&batch, metric_name),
            .score_count = 0,
        });
        try batch.commit();
        return try self.graphMetricStatus(metric_name);
    }

    fn publishGraphMetricScores(
        self: *GraphIndex,
        metric_name: []const u8,
        target_generation: u64,
        scores: []const GraphMetricScore,
        meta: GraphMetricMeta,
    ) !GraphMetricStatus {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const prior_published = try self.metricPublishedGeneration(&batch, metric_name);
        for (scores) |score| {
            if (!std.math.isFinite(score.score)) return error.InvalidGraphMetricScore;
            const score_key = try self.graphMetricScoreKeyAlloc(metric_name, target_generation, score.node);
            defer self.alloc.free(score_key);
            try putF64(&batch, score_key, score.score);
        }
        const published_key = try self.graphMetricPublishedGenerationKeyAlloc(metric_name);
        defer self.alloc.free(published_key);
        try putU64(&batch, published_key, target_generation);
        const dirty_key = try self.graphMetricDirtyGenerationKeyAlloc(metric_name);
        defer self.alloc.free(dirty_key);
        try putU64(&batch, dirty_key, target_generation);
        const meta_key = try self.graphMetricMetaKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(meta_key);
        var meta_buf: [graph_metric_meta_encoded_len]u8 = undefined;
        encodeGraphMetricMeta(meta, &meta_buf);
        try batch.put(meta_key, &meta_buf);
        const edge_filter_key = try self.graphMetricMetaEdgeFilterKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(edge_filter_key);
        const edge_filter_encoded = try self.alloc.alloc(u8, graphMetricEdgeFilterEncodedLen(meta.edge_filter));
        defer self.alloc.free(edge_filter_encoded);
        encodeGraphMetricEdgeFilter(meta.edge_filter, edge_filter_encoded);
        try batch.put(edge_filter_key, edge_filter_encoded);
        const config_fingerprint_key = try self.graphMetricMetaConfigFingerprintKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(config_fingerprint_key);
        try putU64(&batch, config_fingerprint_key, meta.config_fingerprint);
        if (prior_published != 0 and prior_published != target_generation) {
            try self.deleteScoreGeneration(&batch, metric_name, prior_published);
        }
        try self.clearGraphMetricFailureInBatch(&batch, metric_name);
        try self.appendGraphMetricEvent(&batch, metric_name, .{
            .kind = .publish,
            .at_ms = meta.computed_at_ms,
            .target_edge_generation = target_generation,
            .published_generation = target_generation,
            .score_count = scores.len,
        });
        try batch.commit();
        return try self.graphMetricStatus(metric_name);
    }

    fn publishGraphMetricScorePair(
        self: *GraphIndex,
        first_metric_name: []const u8,
        first_scores: []const GraphMetricScore,
        second_metric_name: []const u8,
        second_scores: []const GraphMetricScore,
        target_generation: u64,
        first_meta: GraphMetricMeta,
        second_meta: GraphMetricMeta,
    ) !void {
        var batch = try self.beginWriteReverseBatch();
        errdefer batch.abort();
        const first_prior_published = try self.metricPublishedGeneration(&batch, first_metric_name);
        const second_prior_published = try self.metricPublishedGeneration(&batch, second_metric_name);
        try self.putGraphMetricScoresInBatch(&batch, first_metric_name, target_generation, first_scores);
        try self.putGraphMetricScoresInBatch(&batch, second_metric_name, target_generation, second_scores);
        try self.publishGraphMetricPointerInBatch(&batch, first_metric_name, target_generation, first_meta);
        try self.publishGraphMetricPointerInBatch(&batch, second_metric_name, target_generation, second_meta);
        try self.appendGraphMetricEvent(&batch, first_metric_name, .{
            .kind = .publish,
            .at_ms = first_meta.computed_at_ms,
            .target_edge_generation = target_generation,
            .published_generation = target_generation,
            .score_count = first_scores.len,
        });
        try self.appendGraphMetricEvent(&batch, second_metric_name, .{
            .kind = .publish,
            .at_ms = second_meta.computed_at_ms,
            .target_edge_generation = target_generation,
            .published_generation = target_generation,
            .score_count = second_scores.len,
        });
        if (first_prior_published != 0 and first_prior_published != target_generation) {
            try self.deleteScoreGeneration(&batch, first_metric_name, first_prior_published);
        }
        if (second_prior_published != 0 and second_prior_published != target_generation) {
            try self.deleteScoreGeneration(&batch, second_metric_name, second_prior_published);
        }
        try self.clearGraphMetricFailureInBatch(&batch, first_metric_name);
        try self.clearGraphMetricFailureInBatch(&batch, second_metric_name);
        try batch.commit();
    }

    fn putGraphMetricScoresInBatch(
        self: *GraphIndex,
        batch: anytype,
        metric_name: []const u8,
        target_generation: u64,
        scores: []const GraphMetricScore,
    ) !void {
        for (scores) |score| {
            if (!std.math.isFinite(score.score)) return error.InvalidGraphMetricScore;
            const score_key = try self.graphMetricScoreKeyAlloc(metric_name, target_generation, score.node);
            defer self.alloc.free(score_key);
            try putF64(batch, score_key, score.score);
        }
    }

    fn publishGraphMetricPointerInBatch(
        self: *GraphIndex,
        batch: anytype,
        metric_name: []const u8,
        target_generation: u64,
        meta: GraphMetricMeta,
    ) !void {
        const published_key = try self.graphMetricPublishedGenerationKeyAlloc(metric_name);
        defer self.alloc.free(published_key);
        try putU64(batch, published_key, target_generation);
        const dirty_key = try self.graphMetricDirtyGenerationKeyAlloc(metric_name);
        defer self.alloc.free(dirty_key);
        try putU64(batch, dirty_key, target_generation);
        const meta_key = try self.graphMetricMetaKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(meta_key);
        var meta_buf: [graph_metric_meta_encoded_len]u8 = undefined;
        encodeGraphMetricMeta(meta, &meta_buf);
        try batch.put(meta_key, &meta_buf);
        const edge_filter_key = try self.graphMetricMetaEdgeFilterKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(edge_filter_key);
        const edge_filter_encoded = try self.alloc.alloc(u8, graphMetricEdgeFilterEncodedLen(meta.edge_filter));
        defer self.alloc.free(edge_filter_encoded);
        encodeGraphMetricEdgeFilter(meta.edge_filter, edge_filter_encoded);
        try batch.put(edge_filter_key, edge_filter_encoded);
        const config_fingerprint_key = try self.graphMetricMetaConfigFingerprintKeyAlloc(metric_name, target_generation);
        defer self.alloc.free(config_fingerprint_key);
        try putU64(batch, config_fingerprint_key, meta.config_fingerprint);
    }

    fn oppositeHitsKind(kind: GraphMetricKind) ?GraphMetricKind {
        return switch (kind) {
            .hits_authority => .hits_hub,
            .hits_hub => .hits_authority,
            else => null,
        };
    }

    fn pairedHitsMetricConfig(self: *const GraphIndex, cfg: GraphMetricConfig) ?GraphMetricConfig {
        const opposite_kind = oppositeHitsKind(cfg.kind) orelse return null;
        for (self.metric_configs) |candidate| {
            if (candidate.kind != opposite_kind) continue;
            if (std.mem.eql(u8, candidate.name, cfg.name)) continue;
            if (candidate.max_iterations != cfg.max_iterations) continue;
            if (candidate.tolerance != cfg.tolerance) continue;
            if (!graphMetricEdgeFiltersEqual(candidate.edge_filter, cfg.edge_filter)) continue;
            return candidate;
        }
        return null;
    }

    pub fn runGraphMetric(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        try self.acquireGraphMetricBuildLease(metric_name, self.edge_generation);
        var release_needed = true;
        defer if (release_needed) self.releaseGraphMetricBuildLease(metric_name) catch {};

        var resolved = self.runGraphMetricResolved(cfg, metric_name) catch |err| {
            self.recordGraphMetricFailure(metric_name, err) catch {};
            self.releaseGraphMetricBuildLease(metric_name) catch {};
            release_needed = false;
            return err;
        };
        resolved.deinit(self.alloc);
        try self.completeGraphMetricBuildJob(metric_name);
        try self.releaseGraphMetricBuildLease(metric_name);
        release_needed = false;
        return try self.graphMetricStatus(metric_name);
    }

    fn runGraphMetricResolved(self: *GraphIndex, cfg: GraphMetricConfig, metric_name: []const u8) !GraphMetricStatus {
        return switch (cfg.kind) {
            .pagerank => try self.runPageRankMetric(metric_name),
            .degree => try self.runDegreeMetric(metric_name),
            .eigenvector => try self.runEigenvectorMetric(metric_name),
            .hits_authority, .hits_hub => try self.runHitsMetric(metric_name),
        };
    }

    pub fn runPageRankMetric(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        if (cfg.kind != .pagerank) return error.UnsupportedGraphMetric;

        var nodes = std.ArrayListUnmanaged(PageRankNode).empty;
        defer {
            self.freePageRankNodes(nodes.items);
            nodes.deinit(self.alloc);
        }
        var edges = std.ArrayListUnmanaged(PageRankEdge).empty;
        defer edges.deinit(self.alloc);
        try self.updateGraphMetricBuildLeaseProgress(metric_name, .scan_edges_and_out_degree, 0);
        try self.collectPageRankGraph(cfg.edge_filter, &nodes, &edges);

        const target_generation = self.edge_generation;
        const n = nodes.items.len;
        var converged = true;
        var iterations_completed: u32 = 0;
        var delta: f64 = 0.0;
        if (n > 0) {
            try self.updateGraphMetricBuildLeaseProgress(metric_name, .initialize_ranks, 0);
            const node_count_f = @as(f64, @floatFromInt(n));
            const initial = 1.0 / node_count_f;
            for (nodes.items) |*node| node.rank = initial;
            converged = false;

            var iter: u32 = 0;
            while (iter < cfg.max_iterations) : (iter += 1) {
                const base = (1.0 - cfg.damping) / node_count_f;
                var sink_mass: f64 = 0.0;
                for (nodes.items) |*node| {
                    node.next_rank = base;
                    if (node.out_degree == 0) sink_mass += node.rank;
                }
                const sink_contribution = cfg.damping * sink_mass / node_count_f;
                for (nodes.items) |*node| node.next_rank += sink_contribution;
                for (edges.items) |edge| {
                    const source = nodes.items[edge.source];
                    if (source.out_degree == 0) continue;
                    nodes.items[edge.target].next_rank += cfg.damping * source.rank / @as(f64, @floatFromInt(source.out_degree));
                }
                delta = 0.0;
                for (nodes.items) |*node| {
                    delta += @abs(node.next_rank - node.rank);
                    node.rank = node.next_rank;
                }
                iterations_completed = iter + 1;
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .iterate_contributions, iterations_completed);
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .check_convergence, iterations_completed);
                if (delta <= cfg.tolerance) {
                    converged = true;
                    break;
                }
            }
        }

        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        for (nodes.items) |node| {
            if (!std.math.isFinite(node.rank)) return error.InvalidGraphMetricScore;
        }

        const scores = try self.alloc.alloc(GraphMetricScore, nodes.items.len);
        defer self.alloc.free(scores);
        for (nodes.items, 0..) |node, i| {
            scores[i] = .{ .node = node.key, .score = node.rank };
        }

        try self.updateGraphMetricBuildLeaseProgress(metric_name, .publish_generation, iterations_completed);
        return try self.publishGraphMetricScores(metric_name, target_generation, scores, .{
            .converged = converged,
            .iterations_completed = iterations_completed,
            .delta = delta,
            .computed_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .config_fingerprint = graphMetricConfigFingerprint(cfg),
            .edge_filter = cfg.edge_filter,
        });
    }

    pub fn runDegreeMetric(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        if (cfg.kind != .degree) return error.UnsupportedGraphMetric;

        var nodes = std.ArrayListUnmanaged(DegreeNode).empty;
        defer {
            self.freeDegreeNodes(nodes.items);
            nodes.deinit(self.alloc);
        }
        try self.updateGraphMetricBuildLeaseProgress(metric_name, .scan_edges_and_out_degree, 0);
        try self.collectDegreeGraph(cfg.edge_filter, &nodes);

        const scores = try self.alloc.alloc(GraphMetricScore, nodes.items.len);
        defer self.alloc.free(scores);
        for (nodes.items, 0..) |node, i| {
            scores[i] = .{
                .node = node.key,
                .score = @floatFromInt(node.degree),
            };
        }

        try self.updateGraphMetricBuildLeaseProgress(metric_name, .publish_generation, 1);
        return try self.publishGraphMetricScores(metric_name, self.edge_generation, scores, .{
            .converged = true,
            .iterations_completed = 1,
            .delta = 0.0,
            .computed_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .config_fingerprint = graphMetricConfigFingerprint(cfg),
            .edge_filter = cfg.edge_filter,
        });
    }

    pub fn runEigenvectorMetric(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        if (cfg.kind != .eigenvector) return error.UnsupportedGraphMetric;

        var nodes = std.ArrayListUnmanaged(PageRankNode).empty;
        defer {
            self.freePageRankNodes(nodes.items);
            nodes.deinit(self.alloc);
        }
        var edges = std.ArrayListUnmanaged(PageRankEdge).empty;
        defer edges.deinit(self.alloc);
        try self.updateGraphMetricBuildLeaseProgress(metric_name, .scan_edges_and_out_degree, 0);
        try self.collectPageRankGraph(cfg.edge_filter, &nodes, &edges);

        const target_generation = self.edge_generation;
        const n = nodes.items.len;
        var converged = true;
        var iterations_completed: u32 = 0;
        var delta: f64 = 0.0;
        if (n > 0) {
            try self.updateGraphMetricBuildLeaseProgress(metric_name, .initialize_ranks, 0);
            const initial = 1.0 / @sqrt(@as(f64, @floatFromInt(n)));
            for (nodes.items) |*node| node.rank = initial;
            converged = false;

            var iter: u32 = 0;
            while (iter < cfg.max_iterations) : (iter += 1) {
                for (nodes.items) |*node| node.next_rank = 0.0;
                for (edges.items) |edge| {
                    nodes.items[edge.target].next_rank += nodes.items[edge.source].rank;
                }

                var norm_sq: f64 = 0.0;
                for (nodes.items) |node| norm_sq += node.next_rank * node.next_rank;
                const norm = @sqrt(norm_sq);
                if (norm > 0.0) {
                    for (nodes.items) |*node| node.next_rank /= norm;
                }

                delta = 0.0;
                for (nodes.items) |*node| {
                    delta += @abs(node.next_rank - node.rank);
                    node.rank = node.next_rank;
                }
                iterations_completed = iter + 1;
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .iterate_contributions, iterations_completed);
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .check_convergence, iterations_completed);
                if (delta <= cfg.tolerance) {
                    converged = true;
                    break;
                }
            }
        }

        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        for (nodes.items) |node| {
            if (!std.math.isFinite(node.rank)) return error.InvalidGraphMetricScore;
        }

        const scores = try self.alloc.alloc(GraphMetricScore, nodes.items.len);
        defer self.alloc.free(scores);
        for (nodes.items, 0..) |node, i| {
            scores[i] = .{ .node = node.key, .score = node.rank };
        }

        try self.updateGraphMetricBuildLeaseProgress(metric_name, .publish_generation, iterations_completed);
        return try self.publishGraphMetricScores(metric_name, target_generation, scores, .{
            .converged = converged,
            .iterations_completed = iterations_completed,
            .delta = delta,
            .computed_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .config_fingerprint = graphMetricConfigFingerprint(cfg),
            .edge_filter = cfg.edge_filter,
        });
    }

    fn normalizeVector(values: []f64) f64 {
        var norm_sq: f64 = 0.0;
        for (values) |value| norm_sq += value * value;
        const norm = @sqrt(norm_sq);
        if (norm > 0.0) {
            for (values) |*value| value.* /= norm;
        }
        return norm;
    }

    pub fn runHitsMetric(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        if (cfg.kind != .hits_authority and cfg.kind != .hits_hub) return error.UnsupportedGraphMetric;

        var nodes = std.ArrayListUnmanaged(PageRankNode).empty;
        defer {
            self.freePageRankNodes(nodes.items);
            nodes.deinit(self.alloc);
        }
        var edges = std.ArrayListUnmanaged(PageRankEdge).empty;
        defer edges.deinit(self.alloc);
        try self.updateGraphMetricBuildLeaseProgress(metric_name, .scan_edges_and_out_degree, 0);
        try self.collectPageRankGraph(cfg.edge_filter, &nodes, &edges);

        const target_generation = self.edge_generation;
        const n = nodes.items.len;
        var converged = true;
        var iterations_completed: u32 = 0;
        var delta: f64 = 0.0;
        var authorities: []f64 = &.{};
        var next_authorities: []f64 = &.{};
        var hubs: []f64 = &.{};
        var next_hubs: []f64 = &.{};
        defer {
            if (authorities.len > 0) self.alloc.free(authorities);
            if (next_authorities.len > 0) self.alloc.free(next_authorities);
            if (hubs.len > 0) self.alloc.free(hubs);
            if (next_hubs.len > 0) self.alloc.free(next_hubs);
        }
        if (n > 0) {
            try self.updateGraphMetricBuildLeaseProgress(metric_name, .initialize_ranks, 0);
            authorities = try self.alloc.alloc(f64, n);
            next_authorities = try self.alloc.alloc(f64, n);
            hubs = try self.alloc.alloc(f64, n);
            next_hubs = try self.alloc.alloc(f64, n);

            const initial = 1.0 / @sqrt(@as(f64, @floatFromInt(n)));
            for (0..n) |i| {
                authorities[i] = initial;
                hubs[i] = initial;
            }
            converged = false;

            var iter: u32 = 0;
            while (iter < cfg.max_iterations) : (iter += 1) {
                @memset(next_authorities, 0.0);
                @memset(next_hubs, 0.0);

                for (edges.items) |edge| {
                    next_authorities[edge.target] += hubs[edge.source];
                }
                _ = normalizeVector(next_authorities);

                for (edges.items) |edge| {
                    next_hubs[edge.source] += next_authorities[edge.target];
                }
                _ = normalizeVector(next_hubs);

                delta = 0.0;
                for (0..n) |i| {
                    delta += @abs(next_authorities[i] - authorities[i]);
                    delta += @abs(next_hubs[i] - hubs[i]);
                    authorities[i] = next_authorities[i];
                    hubs[i] = next_hubs[i];
                }
                iterations_completed = iter + 1;
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .iterate_contributions, iterations_completed);
                try self.updateGraphMetricBuildLeaseProgress(metric_name, .check_convergence, iterations_completed);
                if (delta <= cfg.tolerance) {
                    converged = true;
                    break;
                }
            }
        }

        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        const scores = try self.alloc.alloc(GraphMetricScore, nodes.items.len);
        defer self.alloc.free(scores);
        var pair_scores: []GraphMetricScore = &.{};
        defer if (pair_scores.len > 0) self.alloc.free(pair_scores);
        const pair_cfg = self.pairedHitsMetricConfig(cfg);
        if (pair_cfg != null) pair_scores = try self.alloc.alloc(GraphMetricScore, nodes.items.len);
        for (nodes.items, 0..) |node, i| {
            const score = switch (cfg.kind) {
                .hits_authority => authorities[i],
                .hits_hub => hubs[i],
                else => unreachable,
            };
            if (!std.math.isFinite(score)) return error.InvalidGraphMetricScore;
            scores[i] = .{ .node = node.key, .score = score };
            if (pair_cfg) |pair| {
                const pair_score = switch (pair.kind) {
                    .hits_authority => authorities[i],
                    .hits_hub => hubs[i],
                    else => unreachable,
                };
                if (!std.math.isFinite(pair_score)) return error.InvalidGraphMetricScore;
                pair_scores[i] = .{ .node = node.key, .score = pair_score };
            }
        }

        const meta = GraphMetricMeta{
            .converged = converged,
            .iterations_completed = iterations_completed,
            .delta = delta,
            .computed_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms),
            .config_fingerprint = graphMetricConfigFingerprint(cfg),
            .edge_filter = cfg.edge_filter,
        };
        if (pair_cfg) |pair| {
            var pair_meta = meta;
            pair_meta.config_fingerprint = graphMetricConfigFingerprint(pair);
            try self.updateGraphMetricBuildLeaseProgress(metric_name, .publish_generation, iterations_completed);
            try self.publishGraphMetricScorePair(metric_name, scores, pair.name, pair_scores, target_generation, meta, pair_meta);
            return try self.graphMetricStatus(metric_name);
        }
        try self.updateGraphMetricBuildLeaseProgress(metric_name, .publish_generation, iterations_completed);
        return try self.publishGraphMetricScores(metric_name, target_generation, scores, meta);
    }

    pub fn graphMetricStatus(self: *GraphIndex, metric_name: []const u8) !GraphMetricStatus {
        const cfg = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        var txn = try self.beginReadReverseTxn();
        defer txn.abort();
        const published_generation = try self.metricPublishedGeneration(&txn, metric_name);
        const dirty_generation = try self.metricDirtyGeneration(&txn, metric_name);
        const maintenance_paused = try self.metricMaintenancePaused(&txn, metric_name);
        const maybe_build_lease = try self.metricBuildLease(&txn, metric_name);
        const maybe_build_job = try self.metricBuildJob(&txn, metric_name);
        const maybe_failure_detail = try self.metricFailureDetail(&txn, metric_name);
        defer if (maybe_failure_detail) |detail| detail.deinit(self.alloc);
        const recent_events = try self.graphMetricRecentEvents(&txn, metric_name, graph_metric_recent_event_limit);
        errdefer if (recent_events.len > 0) self.alloc.free(recent_events);
        const last_event = if (recent_events.len > 0) recent_events[0] else null;
        var meta = GraphMetricMeta{ .schema_version = 0 };
        if (published_generation != 0) {
            const meta_key = try self.graphMetricMetaKeyAlloc(metric_name, published_generation);
            defer self.alloc.free(meta_key);
            if (txn.get(meta_key)) |raw| {
                meta = decodeGraphMetricMeta(raw) orelse .{ .schema_version = 0 };
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }
        }
        const name = try self.alloc.dupe(u8, metric_name);
        errdefer self.alloc.free(name);
        var edge_filter = try cfg.edge_filter.cloneAlloc(self.alloc);
        errdefer edge_filter.deinit(self.alloc);
        var has_stored_edge_filter = false;
        if (published_generation != 0) {
            const edge_filter_key = try self.graphMetricMetaEdgeFilterKeyAlloc(metric_name, published_generation);
            defer self.alloc.free(edge_filter_key);
            if (txn.get(edge_filter_key)) |raw| {
                if (try decodeGraphMetricEdgeFilterAlloc(self.alloc, raw)) |stored_edge_filter| {
                    edge_filter.deinit(self.alloc);
                    edge_filter = stored_edge_filter;
                    has_stored_edge_filter = true;
                }
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }
        }
        var has_stored_config_fingerprint = false;
        var stored_config_fingerprint: u64 = 0;
        if (published_generation != 0) {
            const config_fingerprint_key = try self.graphMetricMetaConfigFingerprintKeyAlloc(metric_name, published_generation);
            defer self.alloc.free(config_fingerprint_key);
            if (txn.get(config_fingerprint_key)) |raw| {
                if (raw.len == 8) {
                    stored_config_fingerprint = std.mem.readInt(u64, raw[0..8], .little);
                    has_stored_config_fingerprint = true;
                }
            } else |err| switch (err) {
                error.NotFound => {
                    if (meta.schema_version >= 3) {
                        stored_config_fingerprint = meta.config_fingerprint;
                        has_stored_config_fingerprint = true;
                    }
                },
                else => return err,
            }
        }
        const target_edge_generation = @max(dirty_generation, self.edge_generation);
        const now_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
        const active_build_lease = if (maybe_build_lease) |lease| lease.lease_expires_at_ms > now_ms else false;
        const building_generation = if (active_build_lease) maybe_build_lease.?.target_generation else 0;
        const build_job_id = if (active_build_lease) maybe_build_lease.?.job_id else 0;
        const build_started_at_ms = if (active_build_lease) maybe_build_lease.?.started_at_ms else 0;
        const build_iteration = if (active_build_lease) maybe_build_lease.?.iteration else 0;
        const build_lease_expires_at_ms = if (active_build_lease) maybe_build_lease.?.lease_expires_at_ms else 0;
        const active_build_worker_id = if (active_build_lease)
            try self.alloc.dupe(u8, maybe_build_lease.?.worker_id)
        else
            "";
        errdefer if (active_build_worker_id.len > 0) self.alloc.free(active_build_worker_id);
        const active_build_job = if (active_build_lease and maybe_build_job != null and maybe_build_job.?.job_id == build_job_id)
            maybe_build_job.?
        else
            null;
        const active_build_cursor = if (active_build_job != null and active_build_job.?.cursor.len > 0)
            try self.alloc.dupe(u8, active_build_job.?.cursor)
        else
            "";
        errdefer if (active_build_cursor.len > 0) self.alloc.free(active_build_cursor);
        const edge_filter_stale = has_stored_edge_filter and !graphMetricEdgeFiltersEqual(edge_filter, cfg.edge_filter);
        const config_fingerprint_stale = has_stored_config_fingerprint and stored_config_fingerprint != graphMetricConfigFingerprint(cfg);
        const base_state: GraphMetricState = if (last_event != null and last_event.?.kind == .failed and last_event.?.target_edge_generation == target_edge_generation)
            .failed
        else if (published_generation == 0)
            .not_ready
        else if (dirty_generation > published_generation or self.edge_generation > published_generation or edge_filter_stale or config_fingerprint_stale)
            .stale
        else
            .fresh;
        const failure_applies = base_state == .failed and maybe_failure_detail != null;
        const last_error = if (failure_applies)
            try self.alloc.dupe(u8, maybe_failure_detail.?.last_error)
        else
            "";
        errdefer if (last_error.len > 0) self.alloc.free(last_error);
        const state: GraphMetricState = if (active_build_lease) .building else base_state;
        const queued_generation: u64 = if (active_build_lease)
            if (target_edge_generation > building_generation) target_edge_generation else 0
        else switch (base_state) {
            .not_ready, .stale, .failed => target_edge_generation,
            else => 0,
        };
        const phase: GraphMetricBuildPhase = if (state == .fresh) .complete else .idle;
        const active_phase = if (active_build_lease) maybe_build_lease.?.phase else phase;
        return .{
            .name = name,
            .state = state,
            .phase = active_phase,
            .edge_filter = edge_filter,
            .metadata_version = meta.schema_version,
            .maintenance_paused = maintenance_paused,
            .build_queued = queued_generation != 0,
            .published_generation = published_generation,
            .edge_generation = self.edge_generation,
            .target_edge_generation = target_edge_generation,
            .queued_generation = queued_generation,
            .building_generation = building_generation,
            .build_job_id = build_job_id,
            .build_started_at_ms = build_started_at_ms,
            .build_iteration = build_iteration,
            .build_lease_expires_at_ms = build_lease_expires_at_ms,
            .build_worker_id = active_build_worker_id,
            .build_cursor = active_build_cursor,
            .build_completed_units = if (active_build_job) |job| job.completed_units else 0,
            .build_total_units = if (active_build_job) |job| job.total_units else 0,
            .retry_count = if (failure_applies) maybe_failure_detail.?.retry_count else 0,
            .last_error = last_error,
            .progress = if (active_build_lease) graphMetricActiveBuildProgress(cfg, active_phase, build_iteration) else if (phase == .complete) 1.0 else 0.0,
            .converged = meta.converged,
            .iterations_completed = meta.iterations_completed,
            .delta = meta.delta,
            .computed_at_ms = meta.computed_at_ms,
            .last_event = last_event,
            .recent_events = recent_events,
        };
    }

    fn graphMetricNodeFromScoreKeyAlloc(self: *GraphIndex, key: []const u8, prefix: []const u8) !?[]u8 {
        if (!std.mem.startsWith(u8, key, prefix)) return null;
        const pos = prefix.len;
        const term = internal_keys.findComponentTerminator(key, pos) orelse return null;
        if (term + 2 != key.len) return null;
        return try internal_keys.decodeBodyAlloc(self.alloc, key[pos..term]);
    }

    pub fn graphMetricTopK(self: *GraphIndex, metric_name: []const u8, limit: usize) ![]GraphMetricScore {
        if (limit == 0) return try self.alloc.alloc(GraphMetricScore, 0);
        _ = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        var txn = try self.beginReadReverseTxn();
        defer txn.abort();
        const generation = try self.metricPublishedGeneration(&txn, metric_name);
        if (generation == 0) return error.MetricNotReady;
        const prefix = try self.graphMetricScorePrefixAlloc(metric_name, generation);
        defer self.alloc.free(prefix);
        var scores = std.ArrayListUnmanaged(GraphMetricScore).empty;
        errdefer {
            for (scores.items) |*score| score.deinit(self.alloc);
            scores.deinit(self.alloc);
        }
        var cur = try txn.openCursor();
        defer cur.close();
        var entry_opt = try cur.seekAtOrAfter(prefix);
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            const score_value = decodeF64(entry.value) orelse continue;
            const node = (try self.graphMetricNodeFromScoreKeyAlloc(entry.key, prefix)) orelse continue;
            errdefer self.alloc.free(node);
            try scores.append(self.alloc, .{ .node = node, .score = score_value });
        }
        std.mem.sort(GraphMetricScore, scores.items, {}, struct {
            fn lessThan(_: void, a: GraphMetricScore, b: GraphMetricScore) bool {
                if (a.score == b.score) return std.mem.lessThan(u8, a.node, b.node);
                return a.score > b.score;
            }
        }.lessThan);
        const out_len = @min(limit, scores.items.len);
        const out = try self.alloc.dupe(GraphMetricScore, scores.items[0..out_len]);
        for (scores.items[out_len..]) |*score| score.deinit(self.alloc);
        scores.deinit(self.alloc);
        return out;
    }

    pub fn graphMetricScore(self: *GraphIndex, metric_name: []const u8, node: []const u8) !?f64 {
        _ = self.metricConfig(metric_name) orelse return error.MetricNotReady;
        var txn = try self.beginReadReverseTxn();
        defer txn.abort();
        const generation = try self.metricPublishedGeneration(&txn, metric_name);
        if (generation == 0) return null;
        const score_key = try self.graphMetricScoreKeyAlloc(metric_name, generation, node);
        defer self.alloc.free(score_key);
        const raw = txn.get(score_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return decodeF64(raw);
    }

    /// Free an edge's allocated fields.
    pub fn freeEdge(alloc: Allocator, edge: Edge) void {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }

    /// Free a slice of edges returned by getEdges.
    pub fn freeEdges(alloc: Allocator, edges: []Edge) void {
        for (edges) |e| freeEdge(alloc, e);
        alloc.free(edges);
    }
};

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@typeInfo(ptr.child) == .@"struct" and @hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        .@"struct" => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {},
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

// ============================================================================
// Tests
// ============================================================================

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-graph-{s}-{d}\x00", .{ label, ns }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "graph addEdge and getEdges out" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{});
    defer graph.close();

    try graph.addEdge("doc1", "doc2", "cites", 0.9, 1000, 1001, "{}");
    try graph.addEdge("doc1", "doc3", "cites", 0.5, 1000, 1001, "");

    const edges = try graph.getEdges(alloc, "doc1", "cites", .out);
    defer GraphIndex.freeEdges(alloc, edges);

    try std.testing.expectEqual(@as(usize, 2), edges.len);
    try std.testing.expectEqualStrings("doc1", edges[0].source);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), edges[0].weight, 0.001);
}

test "graph pagerank metric publishes top-k scores and status" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .max_iterations = 40,
        .tolerance = 0.0000001,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-c", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-d", "cites", 1.0, 0, 0, "");

    var status = try graph.graphMetricStatus("pagerank");
    defer status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.not_ready, status.state);
    try std.testing.expectEqual(@as(u32, 0), status.metadata_version);

    var published = try graph.runPageRankMetric("pagerank");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    try std.testing.expectEqual(@as(u32, GraphIndex.graph_metric_meta_schema_version), published.metadata_version);
    try std.testing.expect(published.published_generation > 0);
    try std.testing.expect(published.iterations_completed > 0);

    const top = try graph.graphMetricTopK("pagerank", 2);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 2), top.len);
    try std.testing.expectEqualStrings("doc-d", top[0].node);
    try std.testing.expect(top[0].score >= top[1].score);

    try graph.addEdge("doc-d", "doc-a", "cites", 1.0, 0, 0, "");
    var stale = try graph.graphMetricStatus("pagerank");
    defer stale.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.stale, stale.state);
}

test "graph metric dirty marker survives reopen and rebuilds later generation" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank-dirty-reopen");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank-dirty-reopen");
    defer cleanupTmp(rev_path);

    const metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .max_iterations = 40,
        .tolerance = 0.0000001,
        .refresh = .manual,
    }};

    var first_generation: u64 = 0;
    var dirty_generation: u64 = 0;
    var expected_top_node = try alloc.alloc(u8, 0);
    defer alloc.free(expected_top_node);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    {
        var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
        defer graph.close();

        try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
        try graph.addEdge("doc-b", "doc-c", "cites", 1.0, 0, 0, "");

        var published = try graph.runGraphMetric("pagerank");
        defer published.deinit(alloc);
        try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
        first_generation = published.published_generation;

        const top = try graph.graphMetricTopK("pagerank", 1);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 1), top.len);
        alloc.free(expected_top_node);
        expected_top_node = try alloc.dupe(u8, top[0].node);

        try graph.addEdge("doc-c", "doc-a", "cites", 1.0, 0, 0, "");
        dirty_generation = graph.edge_generation;
        var stale = try graph.graphMetricStatus("pagerank");
        defer stale.deinit(alloc);
        try std.testing.expectEqual(GraphIndex.GraphMetricState.stale, stale.state);
        try std.testing.expect(stale.build_queued);
        try std.testing.expectEqual(dirty_generation, stale.queued_generation);
        try std.testing.expectEqual(first_generation, stale.published_generation);
    }
    store.close();

    store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    var reopened_stale = try graph.graphMetricStatus("pagerank");
    defer reopened_stale.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.stale, reopened_stale.state);
    try std.testing.expect(reopened_stale.build_queued);
    try std.testing.expectEqual(dirty_generation, reopened_stale.edge_generation);
    try std.testing.expectEqual(dirty_generation, reopened_stale.queued_generation);
    try std.testing.expectEqual(first_generation, reopened_stale.published_generation);

    const stale_top = try graph.graphMetricTopK("pagerank", 1);
    defer {
        for (stale_top) |*score| score.deinit(alloc);
        alloc.free(stale_top);
    }
    try std.testing.expectEqual(@as(usize, 1), stale_top.len);
    try std.testing.expectEqualStrings(expected_top_node, stale_top[0].node);

    var rebuilt = try graph.runGraphMetric("pagerank");
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, rebuilt.state);
    try std.testing.expectEqual(dirty_generation, rebuilt.published_generation);
    try std.testing.expect(!rebuilt.build_queued);
}

test "graph metric status marks algorithm config drift stale" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank-config-drift");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank-config-drift");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .damping = 0.85,
        .max_iterations = 40,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-c", "cites", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("pagerank");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);

    @constCast(graph.metric_configs)[0].damping = 0.90;
    var stale = try graph.graphMetricStatus("pagerank");
    defer stale.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.stale, stale.state);
    try std.testing.expect(stale.build_queued);
    try std.testing.expectEqual(stale.edge_generation, stale.queued_generation);
}

test "graph metric failed rebuild preserves published generation and records event" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank-failure");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank-failure");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-c", "cites", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("pagerank");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    const published_generation = published.published_generation;

    @constCast(graph.metric_configs)[0].damping = std.math.nan(f64);
    try std.testing.expectError(error.InvalidGraphMetricScore, graph.runGraphMetric("pagerank"));

    var failed = try graph.graphMetricStatus("pagerank");
    defer failed.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expectEqual(published_generation, failed.published_generation);
    try std.testing.expectEqual(@as(u64, 1), failed.retry_count);
    try std.testing.expectEqualStrings("InvalidGraphMetricScore", failed.last_error);
    try std.testing.expect(failed.last_event != null);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.failed, failed.last_event.?.kind);
    try std.testing.expect(failed.recent_events.len >= 2);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.failed, failed.recent_events[0].kind);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.publish, failed.recent_events[1].kind);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const failed_job = try graph.metricBuildJob(&job_txn, "pagerank") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(@as(u64, 1), failed_job.retry_count);
        try std.testing.expectEqualStrings("InvalidGraphMetricScore", failed_job.last_error);
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.check_convergence, failed_job.phase);
        try std.testing.expectEqual(failed_job.target_generation, failed_job.score_generation);
    }

    const top = try graph.graphMetricTopK("pagerank", 1);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 1), top.len);

    @constCast(graph.metric_configs)[0].damping = 0.85;
    var recovered = try graph.runGraphMetric("pagerank");
    defer recovered.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, recovered.state);
    try std.testing.expectEqual(@as(u64, 0), recovered.retry_count);
    try std.testing.expectEqualStrings("", recovered.last_error);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const recovered_job = try graph.metricBuildJob(&job_txn, "pagerank") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.complete, recovered_job.phase);
        try std.testing.expectEqual(@as(u64, 0), recovered_job.retry_count);
        try std.testing.expectEqualStrings("", recovered_job.last_error);
    }
}

test "graph metric failed rebuild preserves published generation across reopen" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank-failure-reopen");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank-failure-reopen");
    defer cleanupTmp(rev_path);

    var metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .refresh = .manual,
    }};

    var published_generation: u64 = 0;
    var expected_top_node = try alloc.alloc(u8, 0);
    defer alloc.free(expected_top_node);
    var expected_top_score: f64 = 0.0;

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    {
        var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
        defer graph.close();

        try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
        try graph.addEdge("doc-b", "doc-c", "cites", 1.0, 0, 0, "");

        var published = try graph.runGraphMetric("pagerank");
        defer published.deinit(alloc);
        try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
        published_generation = published.published_generation;

        const before_failure_top = try graph.graphMetricTopK("pagerank", 1);
        defer {
            for (before_failure_top) |*score| score.deinit(alloc);
            alloc.free(before_failure_top);
        }
        try std.testing.expectEqual(@as(usize, 1), before_failure_top.len);
        alloc.free(expected_top_node);
        expected_top_node = try alloc.dupe(u8, before_failure_top[0].node);
        expected_top_score = before_failure_top[0].score;

        @constCast(graph.metric_configs)[0].damping = std.math.nan(f64);
        try std.testing.expectError(error.InvalidGraphMetricScore, graph.runGraphMetric("pagerank"));
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const failed_job = try graph.metricBuildJob(&job_txn, "pagerank") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(@as(u64, 1), failed_job.retry_count);
        try std.testing.expectEqualStrings("InvalidGraphMetricScore", failed_job.last_error);
    }
    store.close();

    metrics[0].damping = 0.85;
    store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    var failed = try graph.graphMetricStatus("pagerank");
    defer failed.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expectEqual(published_generation, failed.published_generation);
    try std.testing.expectEqual(@as(u64, 1), failed.retry_count);
    try std.testing.expectEqualStrings("InvalidGraphMetricScore", failed.last_error);
    try std.testing.expect(failed.build_queued);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.failed, failed.last_event.?.kind);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const failed_job = try graph.metricBuildJob(&job_txn, "pagerank") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(@as(u64, 1), failed_job.retry_count);
        try std.testing.expectEqualStrings("InvalidGraphMetricScore", failed_job.last_error);
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.check_convergence, failed_job.phase);
    }

    const after_reopen_top = try graph.graphMetricTopK("pagerank", 1);
    defer {
        for (after_reopen_top) |*score| score.deinit(alloc);
        alloc.free(after_reopen_top);
    }
    try std.testing.expectEqual(@as(usize, 1), after_reopen_top.len);
    try std.testing.expectEqualStrings(expected_top_node, after_reopen_top[0].node);
    try std.testing.expectApproxEqAbs(expected_top_score, after_reopen_top[0].score, 0.0000000001);

    var recovered = try graph.runGraphMetric("pagerank");
    defer recovered.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, recovered.state);
    try std.testing.expectEqual(@as(u64, 0), recovered.retry_count);
    try std.testing.expectEqualStrings("", recovered.last_error);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const recovered_job = try graph.metricBuildJob(&job_txn, "pagerank") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.complete, recovered_job.phase);
        try std.testing.expectEqual(@as(u64, 0), recovered_job.retry_count);
        try std.testing.expectEqualStrings("", recovered_job.last_error);
    }
}

test "graph metric status exposes queued and active local build lease" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-metric-lease");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-metric-lease");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");

    var queued = try graph.graphMetricStatus("degree");
    defer queued.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.not_ready, queued.state);
    try std.testing.expect(queued.build_queued);
    try std.testing.expectEqual(graph.edge_generation, queued.queued_generation);
    try std.testing.expectEqual(@as(u64, 0), queued.building_generation);

    try graph.acquireGraphMetricBuildLease("degree", graph.edge_generation);
    defer graph.releaseGraphMetricBuildLease("degree") catch {};

    var building = try graph.graphMetricStatus("degree");
    defer building.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.building, building.state);
    try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.prepare_generation, building.phase);
    try std.testing.expectEqual(graph.edge_generation, building.building_generation);
    try std.testing.expect(building.build_job_id != 0);
    try std.testing.expect(building.build_started_at_ms != 0);
    try std.testing.expectEqual(@as(u32, 0), building.build_iteration);
    try std.testing.expectEqual(@as(u64, 0), building.queued_generation);
    try std.testing.expectEqualStrings(graph_metric_local_build_worker_id, building.build_worker_id);
    try std.testing.expect(building.build_lease_expires_at_ms > 0);
    try std.testing.expectApproxEqAbs(@as(f64, 0.01), building.progress, 0.0001);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const active_job = try graph.metricBuildJob(&job_txn, "degree") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(building.build_job_id, active_job.job_id);
        try std.testing.expectEqual(graph.edge_generation, active_job.target_generation);
        try std.testing.expectEqual(graph.edge_generation, active_job.score_generation);
        try std.testing.expectEqual(building.build_started_at_ms, active_job.started_at_ms);
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.prepare_generation, active_job.phase);
        try std.testing.expectEqual(@as(u32, 0), active_job.iteration);
        try std.testing.expectEqualStrings(graph_metric_local_build_worker_id, active_job.worker_id);
    }

    try graph.updateGraphMetricBuildLeaseProgressWithCursor("degree", .computing, 5, "edge-page:0007", 7, 20);
    var iterating = try graph.graphMetricStatus("degree");
    defer iterating.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.building, iterating.state);
    try std.testing.expectEqual(building.build_job_id, iterating.build_job_id);
    try std.testing.expectEqual(building.build_started_at_ms, iterating.build_started_at_ms);
    try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.computing, iterating.phase);
    try std.testing.expectEqual(@as(u32, 5), iterating.build_iteration);
    try std.testing.expectEqualStrings("edge-page:0007", iterating.build_cursor);
    try std.testing.expectEqual(@as(u64, 7), iterating.build_completed_units);
    try std.testing.expectEqual(@as(u64, 20), iterating.build_total_units);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), iterating.progress, 0.0001);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const active_job = try graph.metricBuildJob(&job_txn, "degree") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(iterating.build_job_id, active_job.job_id);
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.computing, active_job.phase);
        try std.testing.expectEqual(@as(u32, 5), active_job.iteration);
        try std.testing.expectEqualStrings("edge-page:0007", active_job.cursor);
        try std.testing.expectEqual(@as(u64, 7), active_job.completed_units);
        try std.testing.expectEqual(@as(u64, 20), active_job.total_units);
        try std.testing.expect(active_job.updated_at_ms >= active_job.started_at_ms);
    }

    try std.testing.expectError(error.GraphMetricBuildAlreadyRunning, graph.runGraphMetric("degree"));

    try graph.releaseGraphMetricBuildLease("degree");

    const custom_started_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
    const custom_lease = GraphIndex.GraphMetricBuildLease{
        .job_id = 99,
        .target_generation = graph.edge_generation,
        .started_at_ms = custom_started_at_ms,
        .lease_expires_at_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms) + graph_metric_local_build_lease_ms,
        .phase = .publishing,
        .iteration = 17,
        .worker_id = "worker-a",
    };
    const custom_lease_key = try graph.graphMetricBuildLeaseKeyAlloc("degree");
    defer alloc.free(custom_lease_key);
    const custom_lease_encoded = try alloc.alloc(u8, GraphIndex.graphMetricBuildLeaseEncodedLen(custom_lease));
    defer alloc.free(custom_lease_encoded);
    GraphIndex.encodeGraphMetricBuildLease(custom_lease, custom_lease_encoded);
    var custom_lease_batch = try graph.beginWriteReverseBatch();
    errdefer custom_lease_batch.abort();
    try custom_lease_batch.put(custom_lease_key, custom_lease_encoded);
    try custom_lease_batch.commit();

    var custom_building = try graph.graphMetricStatus("degree");
    defer custom_building.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.building, custom_building.state);
    try std.testing.expectEqual(@as(u64, 99), custom_building.build_job_id);
    try std.testing.expectEqual(custom_started_at_ms, custom_building.build_started_at_ms);
    try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.publishing, custom_building.phase);
    try std.testing.expectEqual(@as(u32, 17), custom_building.build_iteration);
    try std.testing.expectEqualStrings("worker-a", custom_building.build_worker_id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.99), custom_building.progress, 0.0001);

    try graph.releaseGraphMetricBuildLease("degree");
    var published = try graph.runGraphMetric("degree");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    try std.testing.expect(!published.build_queued);
    try std.testing.expectEqual(@as(u64, 0), published.building_generation);
    try std.testing.expectEqual(@as(u64, 0), published.build_job_id);
    try std.testing.expectEqual(@as(u64, 0), published.build_started_at_ms);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const completed_job = try graph.metricBuildJob(&job_txn, "degree") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.complete, completed_job.phase);
        try std.testing.expectEqual(graph.edge_generation, completed_job.target_generation);
        try std.testing.expectEqual(graph.edge_generation, completed_job.score_generation);
        try std.testing.expectEqual(@as(u64, 0), completed_job.lease_expires_at_ms);
        try std.testing.expect(completed_job.updated_at_ms >= completed_job.started_at_ms);
    }
}

test "graph metric build lease survives reopen and expired lease can be reclaimed" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-metric-lease-reopen");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-metric-lease-reopen");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    const target_generation = graph.edge_generation;
    try graph.acquireGraphMetricBuildLease("degree", target_generation);
    try graph.updateGraphMetricBuildLeaseProgressWithCursor("degree", .computing, 5, "edge-page:0013", 13, 20);
    graph.close();

    graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    var active = try graph.graphMetricStatus("degree");
    try std.testing.expectEqual(GraphIndex.GraphMetricState.building, active.state);
    try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.computing, active.phase);
    try std.testing.expect(active.build_job_id != 0);
    const active_job_id = active.build_job_id;
    try std.testing.expect(active.build_started_at_ms != 0);
    const active_started_at_ms = active.build_started_at_ms;
    try std.testing.expectEqual(target_generation, active.building_generation);
    try std.testing.expectEqual(@as(u32, 5), active.build_iteration);
    try std.testing.expectEqualStrings("edge-page:0013", active.build_cursor);
    try std.testing.expectEqual(@as(u64, 13), active.build_completed_units);
    try std.testing.expectEqual(@as(u64, 20), active.build_total_units);
    try std.testing.expectEqualStrings(graph_metric_local_build_worker_id, active.build_worker_id);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), active.progress, 0.0001);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const active_job = try graph.metricBuildJob(&job_txn, "degree") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(active.build_job_id, active_job.job_id);
        try std.testing.expectEqual(active.build_started_at_ms, active_job.started_at_ms);
        try std.testing.expectEqual(target_generation, active_job.target_generation);
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.computing, active_job.phase);
        try std.testing.expectEqual(@as(u32, 5), active_job.iteration);
        try std.testing.expectEqualStrings("edge-page:0013", active_job.cursor);
        try std.testing.expectEqual(@as(u64, 13), active_job.completed_units);
        try std.testing.expectEqual(@as(u64, 20), active_job.total_units);
        try std.testing.expectEqualStrings(graph_metric_local_build_worker_id, active_job.worker_id);
    }
    active.deinit(alloc);

    const now_ms = @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
    const expired_lease = GraphIndex.GraphMetricBuildLease{
        .target_generation = target_generation,
        .started_at_ms = now_ms - 1,
        .lease_expires_at_ms = now_ms - 1,
        .phase = .computing,
        .iteration = 9,
        .worker_id = "expired-worker",
    };
    const expired_lease_key = try graph.graphMetricBuildLeaseKeyAlloc("degree");
    defer alloc.free(expired_lease_key);
    const expired_lease_encoded = try alloc.alloc(u8, GraphIndex.graphMetricBuildLeaseEncodedLen(expired_lease));
    defer alloc.free(expired_lease_encoded);
    GraphIndex.encodeGraphMetricBuildLease(expired_lease, expired_lease_encoded);
    var expired_batch = try graph.beginWriteReverseBatch();
    errdefer expired_batch.abort();
    try expired_batch.put(expired_lease_key, expired_lease_encoded);
    try expired_batch.commit();
    graph.close();

    graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();
    var expired = try graph.graphMetricStatus("degree");
    try std.testing.expectEqual(GraphIndex.GraphMetricState.not_ready, expired.state);
    try std.testing.expect(expired.build_queued);
    try std.testing.expectEqual(@as(u64, 0), expired.building_generation);
    try std.testing.expectEqual(@as(u64, 0), expired.build_job_id);
    try std.testing.expectEqual(@as(u64, 0), expired.build_started_at_ms);
    try std.testing.expectEqual(@as(u32, 0), expired.build_iteration);
    try std.testing.expectEqualStrings("", expired.build_worker_id);
    expired.deinit(alloc);

    var rebuilt = try graph.runGraphMetric("degree");
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, rebuilt.state);
    try std.testing.expect(!rebuilt.build_queued);
    try std.testing.expectEqual(@as(u64, 0), rebuilt.building_generation);
    try std.testing.expectEqual(@as(u64, 0), rebuilt.build_job_id);
    try std.testing.expectEqual(@as(u64, 0), rebuilt.build_started_at_ms);
    try std.testing.expect(active_job_id != 0);
    try std.testing.expect(active_started_at_ms != 0);
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        const completed_job = try graph.metricBuildJob(&job_txn, "degree") orelse return error.TestExpectedGraphMetricBuildJob;
        try std.testing.expectEqual(GraphIndex.GraphMetricBuildPhase.complete, completed_job.phase);
        try std.testing.expectEqual(target_generation, completed_job.target_generation);
        try std.testing.expectEqual(@as(u64, 0), completed_job.lease_expires_at_ms);
    }
}

test "graph pagerank metric edge filter limits typed score graph" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-pagerank-filter");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-pagerank-filter");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const filter_types = [_][]const u8{"cites"};
    const metrics = [_]GraphMetricConfig{.{
        .name = "pagerank",
        .kind = .pagerank,
        .max_iterations = 30,
        .refresh = .manual,
        .edge_filter = .{ .mode = .types, .types = &filter_types },
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-x", "doc-y", "related", 1.0, 0, 0, "");
    var published = try graph.runPageRankMetric("pagerank");
    defer published.deinit(alloc);

    const top = try graph.graphMetricTopK("pagerank", 10);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 2), top.len);
    for (top) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-x"));
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-y"));
    }
}

test "graph degree metric publishes total incident degree scores" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-degree");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-degree");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-c", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-d", "cites", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("degree");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    try std.testing.expectEqual(@as(u32, 1), published.iterations_completed);
    try std.testing.expect(published.converged);

    const top = try graph.graphMetricTopK("degree", 4);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 4), top.len);
    try std.testing.expectEqualStrings("doc-b", top[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), top[0].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), top[1].score, 0.001);
}

test "graph degree metric edge filter limits typed score graph" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-degree-filter");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-degree-filter");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const filter_types = [_][]const u8{"cites"};
    const metrics = [_]GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
        .refresh = .manual,
        .edge_filter = .{ .mode = .types, .types = &filter_types },
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-x", "doc-y", "related", 1.0, 0, 0, "");
    var published = try graph.runGraphMetric("degree");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphMetricEdgeFilterMode.types, published.edge_filter.mode);
    try std.testing.expectEqual(@as(usize, 1), published.edge_filter.types.len);
    try std.testing.expectEqualStrings("cites", published.edge_filter.types[0]);
    try std.testing.expectEqual(@as(u32, GraphIndex.graph_metric_meta_schema_version), published.metadata_version);

    @constCast(graph.metric_configs)[0].edge_filter = .{};
    var published_scope_status = try graph.graphMetricStatus("degree");
    defer published_scope_status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.stale, published_scope_status.state);
    try std.testing.expect(published_scope_status.build_queued);
    try std.testing.expectEqual(published_scope_status.edge_generation, published_scope_status.queued_generation);
    try std.testing.expectEqual(GraphMetricEdgeFilterMode.types, published_scope_status.edge_filter.mode);
    try std.testing.expectEqual(@as(usize, 1), published_scope_status.edge_filter.types.len);
    try std.testing.expectEqualStrings("cites", published_scope_status.edge_filter.types[0]);

    const top = try graph.graphMetricTopK("degree", 10);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 2), top.len);
    for (top) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-x"));
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-y"));
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), score.score, 0.001);
    }
}

test "graph metric materialization deletion clears scores and allows rebuild" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-metric-delete");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-metric-delete");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
        .refresh = .manual,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-c", "cites", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("degree");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    const published_generation = published.published_generation;

    const top = try graph.graphMetricTopK("degree", 3);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 3), top.len);

    try graph.deleteGraphMetricMaterialization("degree");
    var deleted_status = try graph.graphMetricStatus("degree");
    defer deleted_status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.not_ready, deleted_status.state);
    try std.testing.expectEqual(@as(u64, 0), deleted_status.published_generation);
    try std.testing.expectError(error.MetricNotReady, graph.graphMetricTopK("degree", 1));
    {
        var job_txn = try graph.beginReadReverseTxn();
        defer job_txn.abort();
        try std.testing.expect((try graph.metricBuildJob(&job_txn, "degree")) == null);
    }

    var paused_after_delete = try graph.pauseGraphMetricMaintenance("degree");
    defer paused_after_delete.deinit(alloc);
    try std.testing.expect(paused_after_delete.maintenance_paused);
    try graph.deleteGraphMetricMaterialization("degree");
    var deleted_paused_status = try graph.graphMetricStatus("degree");
    defer deleted_paused_status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.not_ready, deleted_paused_status.state);
    try std.testing.expect(!deleted_paused_status.maintenance_paused);

    var rebuilt = try graph.runGraphMetric("degree");
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, rebuilt.state);
    try std.testing.expectEqual(published_generation, rebuilt.published_generation);
    try std.testing.expect(rebuilt.recent_events.len >= 1);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.publish, rebuilt.recent_events[0].kind);
    const rebuilt_top = try graph.graphMetricTopK("degree", 3);
    defer {
        for (rebuilt_top) |*score| score.deinit(alloc);
        alloc.free(rebuilt_top);
    }
    try std.testing.expectEqual(@as(usize, 3), rebuilt_top.len);
    try std.testing.expectEqualStrings("doc-b", rebuilt_top[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rebuilt_top[0].score, 0.001);

    var last_status: ?GraphIndex.GraphMetricStatus = null;
    defer if (last_status) |*status| status.deinit(alloc);
    for (0..10) |_| {
        if (last_status) |*status| {
            status.deinit(alloc);
            last_status = null;
        }
        var paused = try graph.pauseGraphMetricMaintenance("degree");
        paused.deinit(alloc);
        last_status = try graph.resumeGraphMetricMaintenance("degree");
    }
    const retained = last_status.?;
    try std.testing.expectEqual(@as(usize, graph_metric_recent_event_limit), retained.recent_events.len);
    try std.testing.expectEqual(GraphIndex.GraphMetricEventKind.@"resume", retained.recent_events[0].kind);
    for (retained.recent_events[0 .. retained.recent_events.len - 1], retained.recent_events[1..]) |newer, older| {
        try std.testing.expect(newer.sequence > older.sequence);
    }
}

test "graph eigenvector metric publishes normalized centrality scores" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-eigenvector");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-eigenvector");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{.{
        .name = "eigenvector",
        .kind = .eigenvector,
        .refresh = .manual,
        .max_iterations = 100,
        .tolerance = 0.000001,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-c", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-b", "cites", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("eigenvector");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, published.state);
    try std.testing.expect(published.iterations_completed > 0);
    try std.testing.expect(published.converged or published.iterations_completed == 100);

    const top = try graph.graphMetricTopK("eigenvector", 3);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 3), top.len);
    try std.testing.expectEqualStrings("doc-b", top[0].node);
    try std.testing.expect(top[0].score > top[1].score);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), top[0].score, 0.001);
}

test "graph eigenvector metric edge filter limits typed score graph" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-eigenvector-filter");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-eigenvector-filter");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const filter_types = [_][]const u8{"cites"};
    const metrics = [_]GraphMetricConfig{.{
        .name = "eigenvector",
        .kind = .eigenvector,
        .refresh = .manual,
        .edge_filter = .{ .mode = .types, .types = &filter_types },
        .max_iterations = 20,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-a", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-b", "doc-b", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-x", "doc-y", "related", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("eigenvector");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphMetricEdgeFilterMode.types, published.edge_filter.mode);
    try std.testing.expectEqual(@as(usize, 1), published.edge_filter.types.len);
    try std.testing.expectEqualStrings("cites", published.edge_filter.types[0]);

    const top = try graph.graphMetricTopK("eigenvector", 10);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 2), top.len);
    for (top) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-x"));
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-y"));
    }
}

test "graph hits metrics publish authority and hub scores" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-hits");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-hits");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const metrics = [_]GraphMetricConfig{
        .{
            .name = "hits_authority",
            .kind = .hits_authority,
            .refresh = .manual,
            .max_iterations = 50,
            .tolerance = 0.000001,
        },
        .{
            .name = "hits_hub",
            .kind = .hits_hub,
            .refresh = .manual,
            .max_iterations = 50,
            .tolerance = 0.000001,
        },
    };
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-hub-a", "doc-authority", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-hub-b", "doc-authority", "cites", 1.0, 0, 0, "");

    var authority_status = try graph.runGraphMetric("hits_authority");
    defer authority_status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, authority_status.state);
    try std.testing.expect(authority_status.iterations_completed > 0);

    var hub_status = try graph.graphMetricStatus("hits_hub");
    defer hub_status.deinit(alloc);
    try std.testing.expectEqual(GraphIndex.GraphMetricState.fresh, hub_status.state);
    try std.testing.expect(hub_status.iterations_completed > 0);
    try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);

    const authorities = try graph.graphMetricTopK("hits_authority", 3);
    defer {
        for (authorities) |*score| score.deinit(alloc);
        alloc.free(authorities);
    }
    try std.testing.expectEqual(@as(usize, 3), authorities.len);
    try std.testing.expectEqualStrings("doc-authority", authorities[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), authorities[0].score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), authorities[1].score, 0.001);

    const hubs = try graph.graphMetricTopK("hits_hub", 3);
    defer {
        for (hubs) |*score| score.deinit(alloc);
        alloc.free(hubs);
    }
    try std.testing.expectEqual(@as(usize, 3), hubs.len);
    try std.testing.expect(hubs[0].score >= hubs[1].score);
    try std.testing.expect(hubs[1].score > hubs[2].score);
    try std.testing.expectApproxEqAbs(@as(f64, 0.707106), hubs[0].score, 0.001);
}

test "graph hits metric edge filter limits typed score graph" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-hits-filter");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-hits-filter");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    const filter_types = [_][]const u8{"cites"};
    const metrics = [_]GraphMetricConfig{.{
        .name = "hits_authority",
        .kind = .hits_authority,
        .refresh = .manual,
        .edge_filter = .{ .mode = .types, .types = &filter_types },
        .max_iterations = 20,
    }};
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{ .metric_configs = &metrics });
    defer graph.close();

    try graph.addEdge("doc-hub", "doc-authority", "cites", 1.0, 0, 0, "");
    try graph.addEdge("doc-x", "doc-y", "related", 1.0, 0, 0, "");

    var published = try graph.runGraphMetric("hits_authority");
    defer published.deinit(alloc);
    try std.testing.expectEqual(GraphMetricEdgeFilterMode.types, published.edge_filter.mode);
    try std.testing.expectEqual(@as(usize, 1), published.edge_filter.types.len);
    try std.testing.expectEqualStrings("cites", published.edge_filter.types[0]);

    const top = try graph.graphMetricTopK("hits_authority", 10);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 2), top.len);
    for (top) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-x"));
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc-y"));
    }
}

test "graph addEdge and getEdges in (reverse index)" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store2");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev2");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{});
    defer graph.close();

    try graph.addEdge("a", "b", "knows", 1.0, 100, 100, "");
    try graph.addEdge("c", "b", "knows", 0.8, 100, 100, "");

    // Query incoming edges to "b"
    const edges = try graph.getEdges(alloc, "b", "knows", .in);
    defer GraphIndex.freeEdges(alloc, edges);

    try std.testing.expectEqual(@as(usize, 2), edges.len);
    // Both should point to target "b"
    for (edges) |e| {
        try std.testing.expectEqualStrings("b", e.target);
    }
}

test "graph edge keys support arbitrary document ids and edge types" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-binary-ids");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-binary-ids");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g\x00:i:", .{});
    defer graph.close();

    const source = "doc\x00:i:\xff";
    const target = "\x00target:out:\xff";
    const edge_type = "rel:with\x00byte";
    try graph.addEdge(source, target, edge_type, 1.5, 10, 11, "{\"ok\":true}");

    const out_edges = try graph.getEdges(alloc, source, edge_type, .out);
    defer GraphIndex.freeEdges(alloc, out_edges);
    try std.testing.expectEqual(@as(usize, 1), out_edges.len);
    try std.testing.expectEqualStrings(source, out_edges[0].source);
    try std.testing.expectEqualStrings(target, out_edges[0].target);
    try std.testing.expectEqualStrings(edge_type, out_edges[0].edge_type);

    const in_edges = try graph.getEdges(alloc, target, edge_type, .in);
    defer GraphIndex.freeEdges(alloc, in_edges);
    try std.testing.expectEqual(@as(usize, 1), in_edges.len);
    try std.testing.expectEqualStrings(source, in_edges[0].source);
    try std.testing.expectEqualStrings(target, in_edges[0].target);

    const prefix = try edgePrefixAlloc(alloc, source, graph.index_name, edge_type);
    defer alloc.free(prefix);
    const pairs = try graph.mainStoreScanPrefix(alloc, prefix);
    defer backend_scan.freeResults(alloc, pairs);
    try std.testing.expectEqual(@as(usize, 1), pairs.len);
    var parsed = (try parseOutgoingEdgeKeyAlloc(alloc, pairs[0].key)).?;
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings(source, parsed.source);
    try std.testing.expectEqualStrings(target, parsed.target);
    try std.testing.expectEqualStrings(edge_type, parsed.edge_type);

    try graph.deleteEdge(source, target, edge_type);
    const deleted_edges = try graph.getEdges(alloc, source, edge_type, .out);
    defer GraphIndex.freeEdges(alloc, deleted_edges);
    try std.testing.expectEqual(@as(usize, 0), deleted_edges.len);
}

test "graph deleteEdge removes both directions" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store3");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev3");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{});
    defer graph.close();

    try graph.addEdge("x", "y", "rel", 1.0, 0, 0, "");
    try graph.deleteEdge("x", "y", "rel");

    const out_edges = try graph.getEdges(alloc, "x", "rel", .out);
    defer GraphIndex.freeEdges(alloc, out_edges);
    try std.testing.expectEqual(@as(usize, 0), out_edges.len);

    const in_edges = try graph.getEdges(alloc, "y", "rel", .in);
    defer GraphIndex.freeEdges(alloc, in_edges);
    try std.testing.expectEqual(@as(usize, 0), in_edges.len);
}

test "graph batchApply applies writes and deletes together" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-batch");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-batch");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "links", .{});
    defer graph.close();

    try graph.addEdge("a", "b", "knows", 1.0, 0, 0, "");
    try graph.batchApply(
        &.{
            .{ .source = "a", .target = "c", .edge_type = "knows", .weight = 0.5 },
            .{ .source = "d", .target = "a", .edge_type = "likes", .weight = 0.7 },
        },
        &.{
            .{ .source = "a", .target = "b", .edge_type = "knows" },
        },
    );

    const out_edges = try graph.getEdges(alloc, "a", "", .out);
    defer GraphIndex.freeEdges(alloc, out_edges);
    try std.testing.expectEqual(@as(usize, 1), out_edges.len);
    try std.testing.expectEqualStrings("c", out_edges[0].target);

    const in_edges = try graph.getEdges(alloc, "a", "likes", .in);
    defer GraphIndex.freeEdges(alloc, in_edges);
    try std.testing.expectEqual(@as(usize, 1), in_edges.len);
    try std.testing.expectEqualStrings("d", in_edges[0].source);
}

test "graph edge encoding round-trip" {
    var buf: [256]u8 = undefined;
    const encoded = encodeEdgeValue(&buf, 0.75, 1234567890, 1234567891, "{\"key\":\"val\"}");
    const decoded = decodeEdgeValue(encoded);

    try std.testing.expectApproxEqAbs(@as(f64, 0.75), decoded.weight, 0.001);
    try std.testing.expectEqual(@as(u64, 1234567890), decoded.created_at);
    try std.testing.expectEqual(@as(u64, 1234567891), decoded.updated_at);
    try std.testing.expectEqualStrings("{\"key\":\"val\"}", decoded.metadata);
}

test "graph getEdges with edge type filter" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store4");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev4");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    try graph.addEdge("n1", "n2", "likes", 1.0, 0, 0, "");
    try graph.addEdge("n1", "n3", "follows", 1.0, 0, 0, "");

    // Filter by "likes" only
    const likes = try graph.getEdges(alloc, "n1", "likes", .out);
    defer GraphIndex.freeEdges(alloc, likes);
    try std.testing.expectEqual(@as(usize, 1), likes.len);
    try std.testing.expectEqualStrings("n2", likes[0].target);

    // All edges (empty type)
    const all = try graph.getEdges(alloc, "n1", "", .out);
    defer GraphIndex.freeEdges(alloc, all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
}

test "graph deleteEdgesForDoc cleanup" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store5");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev5");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    try graph.addEdge("doc1", "doc2", "ref", 1.0, 0, 0, "");
    try graph.addEdge("doc1", "doc3", "ref", 0.5, 0, 0, "");

    try graph.deleteEdgesForDoc("doc1");

    const out = try graph.getEdges(alloc, "doc1", "", .out);
    defer GraphIndex.freeEdges(alloc, out);
    try std.testing.expectEqual(@as(usize, 0), out.len);

    // Reverse index should also be cleaned up
    const in2 = try graph.getEdges(alloc, "doc2", "", .in);
    defer GraphIndex.freeEdges(alloc, in2);
    try std.testing.expectEqual(@as(usize, 0), in2.len);
}

test "graph rebuildReverseFromOwnedOutgoingEdges reconstructs incoming index" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-rebuild");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-rebuild");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    var val_buf: [128]u8 = undefined;
    const edge_val = encodeEdgeValue(&val_buf, 1.0, 10, 11, "");
    const edge_key = try edgeKeyAlloc(alloc, "doc:m", "g", "ref", "doc:z");
    defer alloc.free(edge_key);
    {
        var batch = try graph.outgoing_store.beginBatch();
        errdefer batch.abort();
        try batch.put(edge_key, edge_val);
        try batch.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), try graph.rebuildReverseFromOwnedOutgoingEdges(alloc, "doc:m", ""));

    const incoming = try graph.getEdges(alloc, "doc:z", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
    try std.testing.expectEqualStrings("doc:m", incoming[0].source);
}

test "graph rebuildReverseFromOwnedOutgoingEdges respects split ownership bounds" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-rebuild-bounds");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-rebuild-bounds");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    var val_buf: [128]u8 = undefined;
    const edge_val = encodeEdgeValue(&val_buf, 1.0, 10, 11, "");

    const edge_a = try edgeKeyAlloc(alloc, "doc:a", "g", "ref", "doc:z");
    defer alloc.free(edge_a);
    const edge_m = try edgeKeyAlloc(alloc, "doc:m", "g", "ref", "doc:z");
    defer alloc.free(edge_m);
    const edge_t = try edgeKeyAlloc(alloc, "doc:t", "g", "ref", "doc:y");
    defer alloc.free(edge_t);
    {
        var batch = try graph.outgoing_store.beginBatch();
        errdefer batch.abort();
        try batch.put(edge_a, edge_val);
        try batch.put(edge_m, edge_val);
        try batch.put(edge_t, edge_val);
        try batch.commit();
    }

    try std.testing.expectEqual(@as(usize, 1), try graph.rebuildReverseFromOwnedOutgoingEdges(alloc, "doc:m", "doc:t"));

    const incoming_z = try graph.getEdges(alloc, "doc:z", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming_z);
    try std.testing.expectEqual(@as(usize, 1), incoming_z.len);
    try std.testing.expectEqualStrings("doc:m", incoming_z[0].source);

    const incoming_y = try graph.getEdges(alloc, "doc:y", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming_y);
    try std.testing.expectEqual(@as(usize, 0), incoming_y.len);
}

test "graph pruneOwnedRange removes reverse edges for removed split range" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    try graph.addEdge("doc:a", "doc:z", "ref", 1.0, 0, 0, "");
    try graph.addEdge("doc:z", "doc:y", "ref", 1.0, 0, 0, "");
    try graph.addEdge("doc:m", "doc:q", "ref", 1.0, 0, 0, "");

    _ = try graph.pruneOwnedRange(alloc, "doc:m", "");

    const incoming_z = try graph.getEdges(alloc, "doc:z", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming_z);
    try std.testing.expectEqual(@as(usize, 0), incoming_z.len);

    const incoming_y = try graph.getEdges(alloc, "doc:y", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming_y);
    try std.testing.expectEqual(@as(usize, 0), incoming_y.len);

    const incoming_q = try graph.getEdges(alloc, "doc:q", "ref", .in);
    defer GraphIndex.freeEdges(alloc, incoming_q);
    try std.testing.expectEqual(@as(usize, 0), incoming_q.len);
}

test "tree topology rejects second outgoing edge" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-tree1");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-tree1");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
        .edge_type_configs = &.{.{ .name = "parent", .topology = .tree }},
    });
    defer graph.close();

    // First edge OK
    try graph.addEdge("child", "parent1", "parent", 1.0, 0, 0, "");

    // Second edge to different target should fail
    const result = graph.addEdge("child", "parent2", "parent", 1.0, 0, 0, "");
    try std.testing.expectError(GraphIndex.TreeTopologyViolation.TreeTopologyViolation, result);

    // Only original edge should exist
    const edges = try graph.getEdges(alloc, "child", "parent", .out);
    defer GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("parent1", edges[0].target);
}

test "tree topology allows update to same target" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-tree2");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-tree2");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
        .edge_type_configs = &.{.{ .name = "parent", .topology = .tree }},
    });
    defer graph.close();

    // First edge
    try graph.addEdge("child", "parent1", "parent", 1.0, 0, 0, "");
    // Update to same target (different weight) should succeed
    try graph.addEdge("child", "parent1", "parent", 2.0, 0, 0, "");

    const edges = try graph.getEdges(alloc, "child", "parent", .out);
    defer GraphIndex.freeEdges(alloc, edges);
    // May have 2 entries since we don't deduplicate on update, but both point to same target
    for (edges) |e| {
        try std.testing.expectEqualStrings("parent1", e.target);
    }
}

test "graph mode allows multiple outgoing edges" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-tree3");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-tree3");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    // "parent" is tree, "likes" is graph (default)
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
        .edge_type_configs = &.{.{ .name = "parent", .topology = .tree }},
    });
    defer graph.close();

    // Graph-mode edge type allows multiple targets
    try graph.addEdge("user1", "user2", "likes", 1.0, 0, 0, "");
    try graph.addEdge("user1", "user3", "likes", 1.0, 0, 0, "");

    const edges = try graph.getEdges(alloc, "user1", "likes", .out);
    defer GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 2), edges.len);
}

test "graph reverse backend adapters expose txn cursor and batch operations" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-adapter");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-adapter");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    {
        var txn = try graph.beginWriteReverseTxn();
        errdefer txn.abort();
        try txn.put("k1", "v1");
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("k1", (try cur.start(.{})).?.key);
        try txn.commit();
    }

    {
        var txn = try graph.beginReadReverseTxn();
        defer txn.abort();
        try std.testing.expectEqualStrings("v1", try txn.get("k1"));
    }

    {
        var batch = try graph.beginWriteReverseBatch();
        errdefer batch.abort();
        try batch.put("k2", "v2");
        try std.testing.expectEqualStrings("v2", try batch.get("k2"));
        try batch.commit();
    }

    const summary = try graph.scanStats(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 2), summary.edge_count);
    try std.testing.expectEqual(@as(u64, 0), summary.node_count);
}

test "graph stats summary counts unique nodes from reverse edges" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "stats-summary-store");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "stats-summary-rev");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    try graph.addEdge("doc:a", "doc:b", "links", 1.0, 0, 0, "");
    try graph.addEdge("doc:b", "doc:c", "links", 1.0, 0, 0, "");

    const summary = try graph.stats(alloc);
    try std.testing.expectEqual(@as(u64, 2), summary.edge_count);
    try std.testing.expectEqual(@as(u64, 3), summary.node_count);
}

test "graph reverse store opens concrete txn and batch handles" {
    const alloc = std.testing.allocator;
    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-backend");
    defer cleanupTmp(store_path);
    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-backend");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();
    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{});
    defer graph.close();

    const reverse_store = graph.reverseStore();
    try std.testing.expect(reverse_store.capabilities().cursors);

    {
        var txn = try reverse_store.beginWrite();
        errdefer txn.abort();
        try txn.put("k3", "v3");
        try txn.commit();
    }

    {
        var txn = try reverse_store.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("v3", try txn.get("k3"));
    }

    {
        var batch = try reverse_store.beginBatch();
        errdefer batch.abort();
        try batch.put("k4", "v4");
        try batch.commit();
    }
}

test "graph reverse store persists on durable lsm backend across reopen" {
    const alloc = std.testing.allocator;

    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-lsm-reopen");
    defer cleanupTmp(store_path);

    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-lsm-reopen");
    defer cleanupTmp(rev_path);

    {
        var store = try docstore.DocStore.open(alloc, store_path, .{});
        defer store.close();

        var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
            .reverse_backend = .lsm,
        });
        defer graph.close();

        const now = platform_time.nowSeconds();
        try graph.addEdge("doc:a", "doc:b", "link", 1.0, now, now, "");
        try graph.sync(true);
    }

    {
        var store = try docstore.DocStore.open(alloc, store_path, .{});
        defer store.close();

        var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
            .reverse_backend = .lsm,
        });
        defer graph.close();

        const outgoing = try graph.getEdges(alloc, "doc:a", "link", .out);
        defer GraphIndex.freeEdges(alloc, outgoing);
        try std.testing.expectEqual(@as(usize, 1), outgoing.len);
        try std.testing.expectEqualStrings("doc:b", outgoing[0].target);

        const incoming = try graph.getEdges(alloc, "doc:b", "link", .in);
        defer GraphIndex.freeEdges(alloc, incoming);
        try std.testing.expectEqual(@as(usize, 1), incoming.len);
        try std.testing.expectEqualStrings("doc:a", incoming[0].source);
    }
}

test "graph reverse lsm durable boundary checkpoint retires retained wal" {
    const alloc = std.testing.allocator;

    var store_buf: [256]u8 = undefined;
    const store_path = tmpPath(&store_buf, "store-lsm-wal-checkpoint");
    defer cleanupTmp(store_path);

    var rev_buf: [256]u8 = undefined;
    const rev_path = tmpPath(&rev_buf, "rev-lsm-wal-checkpoint");
    defer cleanupTmp(rev_path);

    var store = try docstore.DocStore.open(alloc, store_path, .{});
    defer store.close();

    var graph = try GraphIndex.open(alloc, &store, rev_path, "g", .{
        .reverse_backend = .lsm,
        .reverse_lsm_options = .{ .flush_threshold = 1024 },
    });
    defer graph.close();

    const now = platform_time.nowSeconds();
    try graph.addEdge("doc:a", "doc:b", "link", 1.0, now, now, "");

    const before = switch (graph.reverse_owner) {
        .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        else => return error.ExpectedLsmOwner,
    };
    try std.testing.expect(before.wal_retained_bytes > 0);

    try graph.checkpointLsmWalAfterDurableBoundary();

    const after = switch (graph.reverse_owner) {
        .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        else => return error.ExpectedLsmOwner,
    };
    try std.testing.expectEqual(@as(u64, 0), after.wal_retained_bytes);
}
