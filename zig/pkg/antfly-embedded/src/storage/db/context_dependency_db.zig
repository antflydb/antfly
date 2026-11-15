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
const command_mod = @import("enrichment/context_dependency_command.zig");
const named = @import("../named_callback.zig");

pub const shadows_complete_key = "\x00\x00__metadata__:context_dependency:shadows_complete";

/// Host delivery for notices whose dependent lives in another table/shard.
/// `deliver` must apply the notice durably (idempotently) on every shard of
/// the dependent table before returning success.
pub const Dispatcher = struct {
    const Methods = struct { deliver: *const fn (ptr: *anyopaque, alloc: Allocator, dependent_table_id: []const u8, encoded_notice: []const u8) anyerror!void };
    const NativeBoundary = named.Boundary(Methods);
    /// Translates errors by name when the host lives in another compiled
    /// unit (the server over the storage engine boundary).
    callback_dispatch: named.Dispatch = NativeBoundary.local_dispatch,
    ptr: *anyopaque,
    deliver: @FieldType(Methods, "deliver"),

    pub fn deliverNotice(self: Dispatcher, alloc: Allocator, dependent_table_id: []const u8, encoded_notice: []const u8) !void {
        return NativeBoundary.call("deliver", self.callback_dispatch, self.deliver, .{ self.ptr, alloc, dependent_table_id, encoded_notice });
    }
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
    const Pass = struct {
        manager: @TypeOf(manager),
        specs: *const notices.SourceSpecs,
        rows: []const notices.RowChange,
        sequence: u64,
        done: bool,
        fn run(p: *anyopaque, a: Allocator, reader: store.Reader, plan: *store.Plan) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(p));
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var changes = std.ArrayListUnmanaged(notices.RowChange).empty;
            for (self.rows) |row| {
                const raw = row.next orelse continue;
                try changes.append(arena.allocator(), .{ .key = row.key, .next = if (self.specs.artifact_names.len == 0) raw else try logicalRowAlloc(arena.allocator(), reader, self.manager, row.key, raw, self.specs.artifact_names) });
            }
            if (!self.specs.isEmpty()) _ = try notices.planStageNotices(a, reader, plan, self.specs, self.manager.context_table_id, self.sequence, 0x6000_0000, changes.items, true);
            if (self.done) {
                try plan.delete(reconcile_cursor_key);
            } else if (self.rows.len != 0) {
                try plan.put(reconcile_cursor_key, self.rows[self.rows.len - 1].key);
            }
        }
    };
    var pass = Pass{ .manager = manager, .specs = &specs, .rows = rows, .sequence = sequence, .done = done };
    try planAndCommit(alloc, primary, manager, .{ .ptr = &pass, .plan_fn = Pass.run });
    if (!done) return;
    // Pending markers are replica-local crash intents (never replicated, so
    // never part of a replicated read set): clear them locally.
    var reader_state = try StoreReader.init(primary);
    defer reader_state.deinit();
    const Collect = struct {
        alloc: Allocator,
        keys: std.ArrayListUnmanaged([]u8) = .empty,
        fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, key));
            return true;
        }
    };
    var collect = Collect{ .alloc = alloc };
    defer {
        for (collect.keys.items) |k| alloc.free(k);
        collect.keys.deinit(alloc);
    }
    try reader_state.reader().scanPrefix(alloc, pending_prefix, .{ .ptr = &collect, .visit_fn = Collect.visit });
    if (collect.keys.items.len != 0) try primary.putBatch(&.{}, @ptrCast(collect.keys.items));
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
    if (manager.context_replication == null) return primary.put(shadows_complete_key, &specs.fields_digest);
    const Pass = struct {
        digest: ctx.Digest,
        fn run(p: *anyopaque, _: Allocator, _: store.Reader, plan: *store.Plan) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(p));
            try plan.put(shadows_complete_key, &self.digest);
        }
    };
    var pass = Pass{ .digest = specs.fields_digest };
    try planAndCommit(alloc, primary, manager, .{ .ptr = &pass, .plan_fn = Pass.run });
}

/// Backfill covering shadows for one page of existing rows.
pub fn backfillShadowPage(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, rows: []const notices.RowChange) !usize {
    return backfillShadowPageWithCursor(alloc, primary, manager, rows, null, null);
}

/// One backfill page; `cursor_key`/`cursor` (when given) are written in the
/// same commit, so a replicated owner advances shadows and cursor together.
pub fn backfillShadowPageWithCursor(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, rows: []const notices.RowChange, cursor_key: ?[]const u8, cursor: ?[]const u8) !usize {
    const declared = try manager.contextDeclarationsAlloc(alloc);
    defer alloc.free(declared);
    var specs = try notices.buildSourceSpecs(alloc, manager.context_table_id, declared);
    defer specs.deinit();
    if (specs.isEmpty()) return 0;
    const Pass = struct {
        manager: @TypeOf(manager),
        specs: *const notices.SourceSpecs,
        rows: []const notices.RowChange,
        cursor_key: ?[]const u8,
        cursor: ?[]const u8,
        written: usize = 0,
        fn run(p: *anyopaque, a: Allocator, reader: store.Reader, plan: *store.Plan) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(p));
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var logical = std.ArrayListUnmanaged(notices.RowChange).empty;
            for (self.rows) |row| {
                const raw = row.next orelse continue;
                try logical.append(arena.allocator(), .{ .key = row.key, .next = if (self.specs.artifact_names.len == 0) raw else try logicalRowAlloc(arena.allocator(), reader, self.manager, row.key, raw, self.specs.artifact_names) });
            }
            self.written = try notices.planBackfillShadows(a, reader, plan, self.specs, logical.items);
            if (self.cursor_key) |key| {
                if (self.cursor) |value| try plan.put(key, value) else try plan.delete(key);
            }
        }
    };
    var pass = Pass{ .manager = manager, .specs = &specs, .rows = rows, .cursor_key = cursor_key, .cursor = cursor };
    try planAndCommit(alloc, primary, manager, .{ .ptr = &pass, .plan_fn = Pass.run });
    return pass.written;
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
            return .{ .ptr = self, .lookup_fn = lookupFn, .row_filter = @import("context_source_db_reader.zig").row_filter };
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
    return applyNoticesConsuming(alloc, primary, manager, encoded, &.{}, now_ms, range_lower, range_upper);
}

/// Apply notices and, in the same commit, delete the store keys they were
/// read from (a replicated dependent's inbox).
pub fn applyNoticesConsuming(
    alloc: Allocator,
    primary: *docstore_mod.DocStore,
    manager: anytype,
    encoded: []const []const u8,
    consumed_keys: []const []const u8,
    now_ms: u64,
    range_lower: []const u8,
    range_upper: []const u8,
) !ApplySummary {
    const Pass = struct {
        manager: @TypeOf(manager),
        encoded: []const []const u8,
        consumed: []const []const u8,
        now_ms: u64,
        lower: []const u8,
        upper: []const u8,
        refs: std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef) = .empty,
        summary: ApplySummary = .{},

        fn reset(p: *anyopaque, a: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(p));
            for (self.refs.items) |ref| enrichment_types.freeGeneratedRef(a, ref);
            self.refs.clearRetainingCapacity();
            self.summary = .{};
        }

        fn run(p: *anyopaque, a: Allocator, reader: store.Reader, plan: *store.Plan) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(p));
            var lookup = SourceCache(@TypeOf(self.manager)){ .alloc = a, .manager = self.manager };
            defer lookup.deinit();
            for (self.encoded, 0..) |raw, i| {
                var decoded = try notices.decodeNoticeAlloc(a, raw);
                defer decoded.deinit();
                var applied = try notices.planApplyNoticeWithSources(a, reader, plan, decoded.notice, self.now_ms, self.lower, self.upper, notices.default_fanout_page, lookup.sources());
                defer applied.deinit(a);
                self.summary.invalidated += applied.rebuild.items.len + applied.debounced + applied.stale_only;
                self.summary.debounced += applied.debounced;
                self.summary.stale_only += applied.stale_only;
                for (applied.rebuild.items) |t| try appendRef(a, &self.refs, t.artifact, t.doc_key);
                if (applied.continuation) |cont| {
                    // Same sequence keeps the continuation ordered before later
                    // notices. Replicated owners re-deliver to their own inbox.
                    if (self.manager.context_replication != null) {
                        try plan.putOwned(try command_mod.inboxKeyAlloc(a, cont), try a.dupe(u8, cont));
                    } else {
                        var h = std.hash.Wyhash.init(i);
                        h.update(cont);
                        const key = notices.outboxKey(decoded.notice.sequence, @truncate(h.final() | 0x8000_0000));
                        try plan.put(&key, cont);
                    }
                }
            }
            for (self.consumed) |key| try plan.delete(key);
            // Debounced rebuilds that are due join this journal record.
            const due = try store.planDrainDebounced(a, reader, plan, self.now_ms, notices.default_fanout_page);
            defer store.freeTargets(a, due);
            for (due) |t| try appendRef(a, &self.refs, t.artifact, t.doc_key);
        }
    };
    var pass = Pass{ .manager = manager, .encoded = encoded, .consumed = consumed_keys, .now_ms = now_ms, .lower = range_lower, .upper = range_upper };
    errdefer {
        Pass.reset(&pass, alloc);
        pass.refs.deinit(alloc);
    }
    try planAndCommit(alloc, primary, manager, .{ .ptr = &pass, .plan_fn = Pass.run, .reset_fn = Pass.reset });
    var summary = pass.summary;
    summary.rebuild_refs = try pass.refs.toOwnedSlice(alloc);
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

/// Acknowledge delivered outbox entries. Replicated owners propose content
/// acknowledgements (every replica deletes its identical entries, whatever
/// their local keys); `values` are the delivered notice bytes.
pub fn ackOutbox(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, keys: []const []const u8, values: []const []const u8) !void {
    if (keys.len == 0) return;
    const replication = manager.context_replication orelse return deleteOutboxKeys(primary, keys);
    if (!replication.isOwner()) return error.ContextDependencyNotOwner;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const acks = try a.alloc([]const u8, values.len);
    for (values, acks) |v, *d| d.* = try command_mod.ackDigestHexAlloc(a, v);
    manager.lockContextProposals();
    defer manager.unlockContextProposals();
    try replication.propose(alloc, .{ .acks = acks });
}

/// Replicated (Raft) owner hooks.
pub const Replication = struct {
    const Methods = struct { propose: *const fn (ptr: *anyopaque, alloc: Allocator, command: command_mod.Command) anyerror!void };
    const NativeBoundary = named.Boundary(Methods);
    callback_dispatch: named.Dispatch = NativeBoundary.local_dispatch,
    ptr: *anyopaque,
    /// Propose `command` to this table's group and return once it is applied
    /// on this replica (the leader).
    propose_fn: *const fn (ptr: *anyopaque, alloc: Allocator, command: command_mod.Command) anyerror!void,
    /// Whether this replica currently leads the group.
    is_owner_fn: *const fn (ptr: *anyopaque) bool,

    pub fn propose(self: Replication, alloc: Allocator, command: command_mod.Command) !void {
        return NativeBoundary.call("propose", self.callback_dispatch, self.propose_fn, .{ self.ptr, alloc, command });
    }

    pub fn isOwner(self: Replication) bool {
        return self.is_owner_fn(self.ptr);
    }
};

/// One planning pass against a reader. On a replicated owner the pass may
/// run more than once (a stale read set re-plans); `reset_fn` discards the
/// outputs of a superseded pass.
pub const Planner = struct {
    ptr: *anyopaque,
    plan_fn: *const fn (ptr: *anyopaque, alloc: Allocator, reader: store.Reader, plan: *store.Plan) anyerror!void,
    reset_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator) void = null,
};

pub const Committer = struct {
    ptr: *anyopaque,
    commit_fn: *const fn (ptr: *anyopaque, alloc: Allocator, planner: Planner) anyerror!void,

    pub fn commit(self: Committer, alloc: Allocator, planner: Planner) !void {
        return self.commit_fn(self.ptr, alloc, planner);
    }
};

pub const max_plan_attempts: usize = 4;

/// Plan and commit one context-dependency change. Locally: under the
/// dependency lock, as one atomic batch. Replicated: only on the leader, as
/// a proposal carrying the exact effects and the recorded read set, which
/// every replica validates at the same log position; a stale read set
/// re-plans. Followers never mutate dependency state.
pub fn planAndCommit(alloc: Allocator, primary: *docstore_mod.DocStore, manager: anytype, planner: Planner) !void {
    if (manager.context_replication) |replication| {
        if (!replication.isOwner()) return error.ContextDependencyNotOwner;
        manager.lockContextProposals();
        defer manager.unlockContextProposals();
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            if (attempt != 0) if (planner.reset_fn) |reset| reset(planner.ptr, alloc);
            var state = try StoreReader.init(primary);
            defer state.deinit();
            var recorder = command_mod.Recorder.init(alloc, state.reader());
            defer recorder.deinit();
            var plan = store.Plan.init(alloc);
            defer plan.deinit();
            try planner.plan_fn(planner.ptr, alloc, recorder.reader(), &plan);
            if (plan.puts.items.len == 0 and plan.deletes.items.len == 0) return;
            const command = try recorder.commandAlloc(&plan);
            try replication.propose(alloc, command);
            var check = try StoreReader.init(primary);
            defer check.deinit();
            if (try command_mod.wasApplied(alloc, check.reader(), command)) return;
            if (attempt + 1 >= max_plan_attempts) return error.ContextDependencyConflict;
        }
    }
    manager.lockContextDependencies();
    defer manager.unlockContextDependencies();
    var state = try StoreReader.init(primary);
    defer state.deinit();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    try planner.plan_fn(planner.ptr, alloc, state.reader(), &plan);
    try commitPlan(alloc, primary, &plan);
}

fn commitPlan(alloc: Allocator, primary: *docstore_mod.DocStore, plan: *const store.Plan) !void {
    if (plan.puts.items.len == 0 and plan.deletes.items.len == 0) return;
    const writes = try alloc.alloc(docstore_mod.KVPair, plan.puts.items.len);
    defer alloc.free(writes);
    for (plan.puts.items, writes) |e, *w| w.* = .{ .key = e.key, .value = e.value };
    try primary.putBatch(writes, plan.deletes.items);
}
