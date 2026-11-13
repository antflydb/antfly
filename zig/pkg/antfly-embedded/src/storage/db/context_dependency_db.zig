// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! DB integration for context-source dependencies (see
//! `enrichment/context_notices.zig` and `enrichment/context_dependency_store.zig`).
//!
//! * `stageForBatch` runs inside the primary commit and adds shadow updates,
//!   outbox notices, and dependent cleanup to the same atomic store batch.
//! * `drainOutbox` delivers committed notices after commit (and on demand):
//!   own-table notices apply locally; foreign ones go to the host dispatcher.
//!   Entries are removed only after a successful apply, so delivery survives
//!   crashes, restarts, leader transfer, and dispatcher failures.
//! * `applyNotices` performs indexed invalidation and turns rebuilds into
//!   ordinary generated-enrichment replay refs (the native work journal).

const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore_mod = @import("../docstore.zig");
const types = @import("types.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const ctx = @import("enrichment/context_sources.zig");
const store = @import("enrichment/context_dependency_store.zig");
const notices = @import("enrichment/context_notices.zig");
const internal_keys = @import("../internal_keys.zig");
const artifact_ids = @import("artifact_ids.zig");
const artifact_codec = @import("enrichment/artifact_codec.zig");

pub const shadows_complete_key = "\x00\x00__metadata__:context_dependency:shadows_complete";

/// Host delivery for notices whose dependent lives in another table/shard.
/// `deliver` must apply the notice durably (idempotently) on every shard of
/// the dependent table before returning success.
pub const Dispatcher = struct {
    ptr: *anyopaque,
    deliver: *const fn (ptr: *anyopaque, alloc: Allocator, dependent_table_id: []const u8, encoded_notice: []const u8) anyerror!void,
};

/// Store `Reader` that serves point reads from a probe transaction and
/// prefix scans from a lazily opened current-scan transaction (the store's
/// probe snapshots do not expose cursors and scan snapshots no point reads).
pub const StoreReader = struct {
    const Txn = @typeInfo(@typeInfo(@TypeOf(docstore_mod.DocStore.beginProbeTxn)).@"fn".return_type.?).error_union.payload;

    primary: *docstore_mod.DocStore,
    probe: Txn,
    scan: ?Txn = null,

    pub fn init(primary: *docstore_mod.DocStore) !StoreReader {
        return .{ .primary = primary, .probe = try primary.beginProbeTxn() };
    }

    pub fn deinit(self: *StoreReader) void {
        if (self.scan) |*txn| txn.abort();
        self.probe.abort();
        self.* = undefined;
    }

    pub fn reader(self: *StoreReader) store.Reader {
        return .{ .ptr = self, .get_fn = getImpl, .scan_fn = scanImpl };
    }

    fn getImpl(p: *anyopaque, alloc: Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *StoreReader = @ptrCast(@alignCast(p));
        const value = self.probe.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try alloc.dupe(u8, value);
    }

    fn scanImpl(p: *anyopaque, lower: []const u8, upper: []const u8, visitor: store.Reader.Visitor) anyerror!void {
        const self: *StoreReader = @ptrCast(@alignCast(p));
        if (self.scan == null) self.scan = try self.primary.beginCurrentScanTxn();
        var cursor = try self.scan.?.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(lower);
        while (entry) |e| {
            if (!std.mem.lessThan(u8, e.key, upper)) return;
            if (!try visitor.visit_fn(visitor.ptr, e.key, e.value)) return;
            entry = try cursor.next();
        }
    }
};

fn isUserDocumentKey(key: []const u8) bool {
    return key.len != 0 and key[0] != 0;
}

/// Add context-dependency effects of a primary batch to its store writes.
pub fn stageForBatch(
    alloc: Allocator,
    primary: *docstore_mod.DocStore,
    manager: anytype,
    sequence: u64,
    writes: []const types.BatchWrite,
    deletes: []const []const u8,
    graph_writes: []const types.GraphEdgeWrite,
    graph_deletes: []const types.GraphEdgeDelete,
    /// Field-derived graph edge artifacts written or removed by this batch.
    graph_artifact_keys: []const []const u8,
    store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    delete_keys: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]u8),
) !void {
    if (!manager.hasContextDependents()) return;
    if (writes.len == 0 and deletes.len == 0 and graph_writes.len == 0 and graph_deletes.len == 0 and graph_artifact_keys.len == 0) return;
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();

    var txn_reader = try StoreReader.init(primary);
    defer txn_reader.deinit();
    const reader = txn_reader.reader();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();

    if (!specs.isEmpty()) {
        var changes = std.ArrayListUnmanaged(notices.RowChange).empty;
        defer changes.deinit(alloc);
        var rows_arena = std.heap.ArenaAllocator.init(alloc);
        defer rows_arena.deinit();
        for (writes) |w| if (isUserDocumentKey(w.key)) {
            // Logical row: document fields plus the generated artifacts read
            // by dependents (unchanged by this write, so read as committed).
            const next = if (specs.artifact_names.len == 0) w.value else try logicalRowAlloc(rows_arena.allocator(), reader, manager, w.key, w.value, specs.artifact_names);
            try changes.append(alloc, .{ .key = w.key, .next = next });
        };
        for (deletes) |k| if (isUserDocumentKey(k)) try changes.append(alloc, .{ .key = k, .next = null });
        const complete = try shadowsComplete(alloc, reader, specs.fields_digest);
        const staged = try notices.planStageNotices(alloc, reader, &plan, &specs, manager.context_table_id, sequence, 0, changes.items, complete);
        var edges = std.ArrayListUnmanaged(notices.EdgeChange).empty;
        defer edges.deinit(alloc);
        inline for (.{ graph_writes, graph_deletes }) |list| for (list) |e| try edges.append(alloc, .{ .graph_index = e.index_name, .source = e.source, .target = e.target, .edge_type = e.edge_type });
        if (specs.generated_edges) try appendDerivedEdgesAlloc(rows_arena.allocator(), &edges, alloc, graph_artifact_keys);
        _ = try notices.planStageEdgeNotices(alloc, &plan, &specs, manager.context_table_id, sequence, staged.next_ordinal, edges.items);
    }
    // A deleted dependent retires its live subscriptions and lineage.
    if (manager.hasAssetContextSources()) {
        manager.lockContextDependencies();
        defer manager.unlockContextDependencies();
        for (deletes) |k| {
            if (!isUserDocumentKey(k)) continue;
            for (declared) |d| {
                if (d.dependent_table_id.len != 0) continue;
                try store.planCleanup(alloc, reader, &plan, k, d.artifact, true);
            }
        }
    }

    try owned_keys.ensureUnusedCapacity(alloc, plan.puts.items.len);
    try owned_values.ensureUnusedCapacity(alloc, plan.puts.items.len);
    try store_writes.ensureUnusedCapacity(alloc, plan.puts.items.len);
    try owned_delete_keys.ensureUnusedCapacity(alloc, plan.deletes.items.len);
    try delete_keys.ensureUnusedCapacity(alloc, plan.deletes.items.len);
    for (plan.puts.items) |e| {
        const key = try alloc.dupe(u8, e.key);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, e.value);
        owned_keys.appendAssumeCapacity(key);
        owned_values.appendAssumeCapacity(value);
        store_writes.appendAssumeCapacity(.{ .key = key, .value = value });
    }
    for (plan.deletes.items) |k| {
        const key = try alloc.dupe(u8, k);
        owned_delete_keys.appendAssumeCapacity(key);
        delete_keys.appendAssumeCapacity(key);
    }
}

// ---------------------------------------------------------------------------
// Generated data as context input
// ---------------------------------------------------------------------------

/// `{"<artifact>": value|null}` for a row's generated artifacts. Asset bytes
/// render as parsed JSON (or a JSON string), chunk streams as an array of
/// payloads in chunk order, and document embeddings as a vector.
pub fn artifactValuesJsonAlloc(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, doc_key: []const u8, names: []const []const u8) ![]u8 {
    var reader = try StoreReader.init(primary);
    defer reader.deinit();
    return artifactValuesFromReaderAlloc(alloc, reader.reader(), manager, doc_key, names);
}

fn appendPayloadValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), raw: []const u8) !void {
    const public = try @import("../hierarchy_navigation.zig").publicArtifactPayloadAlloc(alloc, raw);
    defer alloc.free(public);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    if (std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), public, .{})) |value| {
        try ctx.appendCanonicalValue(alloc, out, value);
    } else |_| {
        if (std.unicode.utf8ValidateSlice(public)) try ctx.appendJsonString(alloc, out, public) else try out.appendSlice(alloc, "null");
    }
}

fn artifactValuesFromReaderAlloc(alloc: Allocator, reader: store.Reader, manager: anytype, doc_key: []const u8, names: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    for (names, 0..) |name, i| {
        if (i != 0) try out.append(alloc, ',');
        try ctx.appendJsonString(alloc, &out, name);
        try out.append(alloc, ':');
        const start = out.items.len;
        if (manager.getEnrichment(.asset, name) != null) {
            const key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", name);
            defer alloc.free(key);
            if (try reader.get(alloc, key)) |raw| {
                defer alloc.free(raw);
                try appendPayloadValue(alloc, &out, raw);
            }
        } else if (manager.getEnrichment(.embedding, name) != null) {
            const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, name);
            defer alloc.free(key);
            if (try reader.get(alloc, key)) |raw| {
                defer alloc.free(raw);
                if (artifact_codec.denseEmbeddingVectorView(raw) catch null) |vector| {
                    try out.append(alloc, '[');
                    for (vector, 0..) |f, j| {
                        if (j != 0) try out.append(alloc, ',');
                        try out.print(alloc, "{d}", .{f});
                    }
                    try out.append(alloc, ']');
                }
            }
        } else if (manager.getEnrichment(.chunk, name) != null) {
            const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", name);
            defer alloc.free(prefix);
            const record_prefix = try std.mem.concat(alloc, u8, &.{ prefix, &.{internal_keys.chunk_record_kind} });
            defer alloc.free(record_prefix);
            const Collect = struct {
                alloc: Allocator,
                out: *std.ArrayListUnmanaged(u8),
                count: usize = 0,
                fn visit(p: *anyopaque, _: []const u8, value: []const u8) anyerror!bool {
                    const self: *@This() = @ptrCast(@alignCast(p));
                    if (self.count != 0) try self.out.append(self.alloc, ',');
                    try appendPayloadValue(self.alloc, self.out, value);
                    self.count += 1;
                    return self.count < 4096;
                }
            };
            try out.append(alloc, '[');
            var collect = Collect{ .alloc = alloc, .out = &out };
            try reader.scanPrefix(alloc, record_prefix, .{ .ptr = &collect, .visit_fn = Collect.visit });
            try out.append(alloc, ']');
        }
        if (out.items.len == start) try out.appendSlice(alloc, "null");
    }
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

/// Document JSON plus `_artifacts` (the read artifacts), as one logical row.
fn logicalRowAlloc(a: Allocator, reader: store.Reader, manager: anytype, doc_key: []const u8, doc_json: []const u8, names: []const []const u8) ![]const u8 {
    var row = std.json.parseFromSliceLeaky(std.json.Value, a, doc_json, .{}) catch return doc_json;
    if (row != .object) return doc_json;
    const values = try artifactValuesFromReaderAlloc(a, reader, manager, doc_key, names);
    try row.object.put(a, "_artifacts", try std.json.parseFromSliceLeaky(std.json.Value, a, values, .{}));
    return std.json.Stringify.valueAlloc(a, row, .{});
}

/// Graph edge artifacts (field-derived or generated) as edge changes.
fn appendDerivedEdgesAlloc(a: Allocator, edges: *std.ArrayListUnmanaged(notices.EdgeChange), list_alloc: Allocator, keys: []const []const u8) !void {
    for (keys) |key| {
        if (!internal_keys.isGraphEdgeArtifactKey(key)) continue;
        const identity = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(a, key)) orelse continue;
        try edges.append(list_alloc, .{
            .graph_index = identity.index_name,
            .source = if (identity.logical_source.len != 0) identity.logical_source else identity.doc_key,
            .target = identity.target_doc_key,
            .edge_type = identity.edge_type,
            .artifact_derived = true,
        });
    }
}

var marker_counter = std.atomic.Value(u64).init(0);

pub const pending_prefix = "\x00\x00__metadata__:context_dependency:generated_pending:";
const reconcile_cursor_key = "\x00\x00__metadata__:context_dependency:reconcile_cursor";

fn readsGenerated(manager: anytype, batch: anytype) bool {
    if (!manager.hasContextDependents() or !manager.contextReadsGenerated()) return false;
    return batch.changed_artifact_keys.len != 0 or batch.deleted_keys.len != 0 or batch.graph_writes.len != 0;
}

/// Durable intent written before a generated record that may change context
/// inputs. Cleared when its notices are staged; a leftover marker (crash in
/// between) triggers a reconcile that compares shadows with current values.
pub fn markGeneratedPending(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, batch: anytype) !?u64 {
    if (!readsGenerated(manager, batch)) return null;
    // Unique per marker within the process; addresses vary across processes.
    const n = marker_counter.fetchAdd(1, .monotonic);
    const token = std.hash.Wyhash.hash(@intFromPtr(primary) ^ @intFromPtr(&marker_counter), std.mem.asBytes(&n));
    const key = try std.fmt.allocPrint(alloc, "{s}{x:0>16}", .{ pending_prefix, token });
    defer alloc.free(key);
    try primary.put(key, "");
    return token;
}

/// Stage notices for artifacts and generated edges changed by a committed
/// generated record. Next values are the shadow's document fields with the
/// current artifacts substituted (an artifact change never changes fields).
pub fn stageGenerated(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, sequence: u64, batch: anytype, token: ?u64) !void {
    const t = token orelse return;
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    var reader_state = try StoreReader.init(primary);
    defer reader_state.deinit();
    const reader = reader_state.reader();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (!specs.isEmpty() and (specs.artifact_names.len != 0 or specs.generated_edges)) {
        var docs = std.StringArrayHashMapUnmanaged(void).empty;
        var edge_keys = std.ArrayListUnmanaged([]const u8).empty;
        inline for (.{ batch.changed_artifact_keys, batch.deleted_keys }) |keys| for (keys) |key| {
            if (internal_keys.isGraphEdgeArtifactKey(key)) {
                try edge_keys.append(a, key);
                continue;
            }
            const ref = (artifact_ids.decodeArtifactRefAlloc(a, key) catch null) orelse continue;
            for (specs.artifact_names) |name| if (std.mem.eql(u8, name, ref.name)) {
                try docs.put(a, ref.document_id, {});
                break;
            };
        };
        var changes = std.ArrayListUnmanaged(notices.RowChange).empty;
        for (docs.keys()) |doc| {
            const values = try artifactValuesFromReaderAlloc(a, reader, manager, doc, specs.artifact_names);
            const shadow = try reader.get(a, try notices.shadowKeyAlloc(a, doc));
            var base: std.json.Value = .{ .object = .empty };
            if (shadow) |raw| if (raw.len >= 32 and std.mem.eql(u8, raw[0..32], &specs.fields_digest)) {
                base = std.json.parseFromSliceLeaky(std.json.Value, a, raw[32..], .{}) catch base;
            };
            if (shadow == null) {
                // No shadow yet: take the document itself.
                const doc_key = try internal_keys.documentKeyAlloc(a, doc);
                if (try reader.get(a, doc_key)) |raw| base = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch base;
            }
            if (base != .object) continue;
            try base.object.put(a, "_artifacts", try std.json.parseFromSliceLeaky(std.json.Value, a, values, .{}));
            try changes.append(a, .{ .key = doc, .next = try std.json.Stringify.valueAlloc(a, base, .{}) });
        }
        const complete = try shadowsComplete(alloc, reader, specs.fields_digest);
        const staged = try notices.planStageNotices(alloc, reader, &plan, &specs, manager.context_table_id, sequence, 0x4000_0000, changes.items, complete);
        if (specs.generated_edges) {
            var edges = std.ArrayListUnmanaged(notices.EdgeChange).empty;
            defer edges.deinit(alloc);
            try appendDerivedEdgesAlloc(a, &edges, alloc, edge_keys.items);
            for (batch.graph_writes) |e| try edges.append(alloc, .{ .graph_index = e.index_name, .source = e.source, .target = e.target, .edge_type = e.edge_type, .artifact_derived = true });
            _ = try notices.planStageEdgeNotices(alloc, &plan, &specs, manager.context_table_id, sequence, staged.next_ordinal, edges.items);
        }
    }
    const key = try std.fmt.allocPrint(alloc, "{s}{x:0>16}", .{ pending_prefix, t });
    defer alloc.free(key);
    try plan.delete(key);
    try commitPlan(alloc, primary, &plan);
}

pub fn hasGeneratedPending(alloc: Allocator, primary: *docstore_mod.DocStore) !bool {
    var reader = try StoreReader.init(primary);
    defer reader.deinit();
    const Probe = struct {
        found: bool = false,
        fn visit(p: *anyopaque, _: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            self.found = true;
            return false;
        }
    };
    var probe = Probe{};
    try reader.reader().scanPrefix(alloc, pending_prefix, .{ .ptr = &probe, .visit_fn = Probe.visit });
    return probe.found;
}

/// One reconcile page after an interrupted generated record: recompute
/// logical rows (document plus artifacts) and stage exactly the differences
/// from their shadows. With `done`, the pending markers are cleared.
pub fn reconcilePage(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, sequence: u64, rows: []const notices.RowChange, done: bool) !void {
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    var reader_state = try StoreReader.init(primary);
    defer reader_state.deinit();
    const reader = reader_state.reader();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var changes = std.ArrayListUnmanaged(notices.RowChange).empty;
    for (rows) |row| {
        const raw = row.next orelse continue;
        try changes.append(arena.allocator(), .{ .key = row.key, .next = if (specs.artifact_names.len == 0) raw else try logicalRowAlloc(arena.allocator(), reader, manager, row.key, raw, specs.artifact_names) });
    }
    if (!specs.isEmpty()) _ = try notices.planStageNotices(alloc, reader, &plan, &specs, manager.context_table_id, sequence, 0x6000_0000, changes.items, true);
    if (done) {
        const Collect = struct {
            plan: *store.Plan,
            fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const self: *@This() = @ptrCast(@alignCast(p));
                try self.plan.delete(key);
                return true;
            }
        };
        var collect = Collect{ .plan = &plan };
        try reader.scanPrefix(alloc, pending_prefix, .{ .ptr = &collect, .visit_fn = Collect.visit });
        try plan.delete(reconcile_cursor_key);
    } else if (rows.len != 0) {
        try plan.put(reconcile_cursor_key, rows[rows.len - 1].key);
    }
    try commitPlan(alloc, primary, &plan);
}

pub fn reconcileCursorAlloc(alloc: Allocator, primary: *docstore_mod.DocStore) !?[]u8 {
    var reader = try StoreReader.init(primary);
    defer reader.deinit();
    return reader.reader().get(alloc, reconcile_cursor_key);
}

fn shadowsComplete(alloc: Allocator, reader: store.Reader, fields_digest: ctx.Digest) !bool {
    const raw = (try reader.get(alloc, shadows_complete_key)) orelse return false;
    defer alloc.free(raw);
    return raw.len == 32 and std.mem.eql(u8, raw, &fields_digest);
}

pub fn shadowsCompleteForCurrent(alloc: Allocator, reader: store.Reader, manager: anytype) !bool {
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    if (specs.isEmpty()) return true;
    return shadowsComplete(alloc, reader, specs.fields_digest);
}

/// Mark shadows complete for the current field set (after a full backfill).
pub fn markShadowsComplete(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype) !void {
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    try primary.put(shadows_complete_key, &specs.fields_digest);
}

/// Backfill covering shadows for one page of existing rows.
pub fn backfillShadowPage(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, rows: []const notices.RowChange) !usize {
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    if (specs.isEmpty()) return 0;
    var txn_reader = try StoreReader.init(primary);
    defer txn_reader.deinit();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var logical = std.ArrayListUnmanaged(notices.RowChange).empty;
    for (rows) |row| {
        const raw = row.next orelse continue;
        try logical.append(arena.allocator(), .{ .key = row.key, .next = if (specs.artifact_names.len == 0) raw else try logicalRowAlloc(arena.allocator(), txn_reader.reader(), manager, row.key, raw, specs.artifact_names) });
    }
    const written = try notices.planBackfillShadows(alloc, txn_reader.reader(), &plan, &specs, logical.items);
    try commitPlan(alloc, primary, &plan);
    return written;
}

pub const ApplySummary = struct {
    rebuild_refs: []enrichment_types.GeneratedEnrichmentRef = &.{},
    invalidated: usize = 0,
    debounced: usize = 0,
    stale_only: usize = 0,

    pub fn deinit(self: *ApplySummary, alloc: Allocator) void {
        for (self.rebuild_refs) |ref| enrichment_types.freeGeneratedRef(alloc, ref);
        alloc.free(self.rebuild_refs);
        self.* = undefined;
    }
};

fn appendRef(alloc: Allocator, refs: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef), artifact: []const u8, doc_key: []const u8) !void {
    for (refs.items) |r| if (std.mem.eql(u8, r.artifact_name, artifact) and std.mem.eql(u8, r.doc_key, doc_key)) return;
    const ref = try enrichment_types.cloneGeneratedRef(alloc, .{ .kind = .asset, .index_name = artifact, .artifact_name = artifact, .doc_key = doc_key });
    errdefer enrichment_types.freeGeneratedRef(alloc, ref);
    try refs.append(alloc, ref);
}

/// Parses dependents' declarations once per apply call to evaluate residual
/// notices against each subscriber's parameters.
fn SourceCache(comptime Manager: type) type {
    return struct {
        alloc: Allocator,
        manager: Manager,
        configs: std.StringHashMapUnmanaged(ctx.Config) = .empty,

        fn deinit(self: *@This()) void {
            var it = self.configs.valueIterator();
            while (it.next()) |c| c.deinit();
            self.configs.deinit(self.alloc);
        }

        fn sources(self: *@This()) notices.SourceLookup {
            return .{ .ptr = self, .lookup_fn = lookupFn };
        }

        fn lookupFn(p: *anyopaque, artifact: []const u8, spec: ctx.Digest) ?*const ctx.Source {
            const self: *@This() = @ptrCast(@alignCast(p));
            if (self.configs.getPtr(artifact)) |c| return c.findSpec(spec);
            const entry = self.manager.getEnrichment(.asset, artifact) orelse return null;
            if (entry.context_sources_json.len == 0) return null;
            const config = ctx.parseStoredJson(self.alloc, entry.context_sources_json) catch return null;
            self.configs.put(self.alloc, entry.name, config) catch {
                var c = config;
                c.deinit();
                return null;
            };
            return self.configs.getPtr(artifact).?.findSpec(spec);
        }
    };
}

/// Apply encoded notices against this DB's subscriptions in one batch and
/// return the rebuilds to journal. Remaining fan-out is re-staged in this
/// DB's outbox as a continuation (bounded work per call).
pub fn applyNotices(
    alloc: Allocator,
    primary: *docstore_mod.DocStore,
    manager: anytype,
    encoded: []const []const u8,
    now_ms: u64,
    range_lower: []const u8,
    range_upper: []const u8,
) !ApplySummary {
    // All read-modify-write of dependency state holds this lock, so planning
    // from a probe snapshot and committing one atomic batch is linearizable.
    manager.lockContextDependencies();
    defer manager.unlockContextDependencies();
    var txn_reader = try StoreReader.init(primary);
    defer txn_reader.deinit();
    const reader = txn_reader.reader();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    var refs = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef).empty;
    errdefer {
        for (refs.items) |ref| enrichment_types.freeGeneratedRef(alloc, ref);
        refs.deinit(alloc);
    }
    var summary = ApplySummary{};
    var lookup = SourceCache(@TypeOf(manager)){ .alloc = alloc, .manager = manager };
    defer lookup.deinit();
    for (encoded, 0..) |raw, i| {
        var decoded = try notices.decodeNoticeAlloc(alloc, raw);
        defer decoded.deinit();
        var applied = try notices.planApplyNoticeWithSources(alloc, reader, &plan, decoded.notice, now_ms, range_lower, range_upper, notices.default_fanout_page, lookup.sources());
        defer applied.deinit(alloc);
        summary.invalidated += applied.rebuild.items.len + applied.debounced + applied.stale_only;
        summary.debounced += applied.debounced;
        summary.stale_only += applied.stale_only;
        for (applied.rebuild.items) |t| try appendRef(alloc, &refs, t.artifact, t.doc_key);
        if (applied.continuation) |cont| {
            // Same sequence keeps the continuation ordered before later notices.
            var h = std.hash.Wyhash.init(i);
            h.update(cont);
            const key = notices.outboxKey(decoded.notice.sequence, @truncate(h.final() | 0x8000_0000));
            try plan.put(&key, cont);
        }
    }
    // Debounced rebuilds that are due join this journal record.
    const due = try store.planDrainDebounced(alloc, reader, &plan, now_ms, notices.default_fanout_page);
    defer store.freeTargets(alloc, due);
    for (due) |t| try appendRef(alloc, &refs, t.artifact, t.doc_key);
    try commitPlan(alloc, primary, &plan);
    summary.rebuild_refs = try refs.toOwnedSlice(alloc);
    return summary;
}

/// Drain due debounced rebuilds without new notices.
pub fn drainDebounced(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, now_ms: u64) !ApplySummary {
    return applyNotices(alloc, primary, manager, &.{}, now_ms, "", "");
}

pub const DrainResult = struct {
    delivered: usize = 0,
    local: usize = 0,
    remaining: bool = false,
};

/// Read up to `limit` outbox entries in sequence order. Local (own-table)
/// entries are returned for the caller to apply; foreign entries are handed
/// to `dispatcher`. Successfully delivered foreign entries are deleted here;
/// the caller deletes local ones after applying them.
pub fn readOutbox(alloc: Allocator, primary: *docstore_mod.DocStore, limit: usize) ![]notices.OutboxEntry {
    var txn_reader = try StoreReader.init(primary);
    defer txn_reader.deinit();
    return notices.readOutboxAlloc(alloc, txn_reader.reader(), limit);
}

pub fn isLocalNotice(alloc: Allocator, raw: []const u8, own_table_id: []const u8) !bool {
    var decoded = try notices.decodeNoticeAlloc(alloc, raw);
    defer decoded.deinit();
    const dep = decoded.notice.dependent_table_id;
    return dep.len == 0 or std.mem.eql(u8, dep, own_table_id);
}

pub fn dependentOf(alloc: Allocator, raw: []const u8) ![]u8 {
    var decoded = try notices.decodeNoticeAlloc(alloc, raw);
    defer decoded.deinit();
    return alloc.dupe(u8, decoded.notice.dependent_table_id);
}

pub fn deleteOutboxKeys(primary: *docstore_mod.DocStore, keys: []const []const u8) !void {
    if (keys.len == 0) return;
    try primary.putBatch(&.{}, keys);
}

fn commitPlan(alloc: Allocator, primary: *docstore_mod.DocStore, plan: *const store.Plan) !void {
    if (plan.puts.items.len == 0 and plan.deletes.items.len == 0) return;
    const writes = try alloc.alloc(docstore_mod.KVPair, plan.puts.items.len);
    defer alloc.free(writes);
    for (plan.puts.items, writes) |e, *w| w.* = .{ .key = e.key, .value = e.value };
    try primary.putBatch(writes, plan.deletes.items);
}
