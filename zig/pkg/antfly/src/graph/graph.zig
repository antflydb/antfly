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

const reverse_rebuild_batch_size: usize = 1024;
pub var test_abort_reverse_rebuild_after_batches: ?usize = null;
const graph_meta_prefix = "meta:";
const graph_edge_count_key = "meta:edge_count";
const graph_node_count_key = "meta:node_count";

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
    rebuild_root_path: ?[]u8,
    algebraic_semiring_traversal: bool,
    edge_count: u64,
    node_count: u64,
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
                try meta_keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
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
        const loaded_stats = try loadGraphCounters(&reverse_store.store);

        return .{
            .alloc = alloc,
            .index_name = index_name,
            .outgoing_store = outgoing_store.store,
            .outgoing_owner = outgoing_store.owner,
            .reverse_store = reverse_store.store,
            .reverse_owner = reverse_store.owner,
            .edge_type_configs = opts.edge_type_configs,
            .rebuild_root_path = if (opts.rebuild_root_path) |path| try alloc.dupe(u8, path) else null,
            .algebraic_semiring_traversal = opts.algebraic_semiring_traversal,
            .edge_count = loaded_stats.edge_count,
            .node_count = loaded_stats.node_count,
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
        errdefer {
            self.edge_count = prev_edge_count;
            self.node_count = prev_node_count;
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
