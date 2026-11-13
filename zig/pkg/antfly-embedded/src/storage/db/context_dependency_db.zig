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
    store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    delete_keys: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]u8),
) !void {
    if (!manager.hasContextDependents()) return;
    if (writes.len == 0 and deletes.len == 0) return;
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
        for (writes) |w| if (isUserDocumentKey(w.key)) try changes.append(alloc, .{ .key = w.key, .next = w.value });
        for (deletes) |k| if (isUserDocumentKey(k)) try changes.append(alloc, .{ .key = k, .next = null });
        const complete = try shadowsComplete(alloc, reader, specs.fields_digest);
        _ = try notices.planStageNotices(alloc, reader, &plan, &specs, manager.context_table_id, sequence, 0, changes.items, complete);
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
    const written = try notices.planBackfillShadows(alloc, txn_reader.reader(), &plan, &specs, rows);
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
    for (encoded, 0..) |raw, i| {
        var decoded = try notices.decodeNoticeAlloc(alloc, raw);
        defer decoded.deinit();
        var applied = try notices.planApplyNotice(alloc, reader, &plan, decoded.notice, now_ms, range_lower, range_upper, notices.default_fanout_page);
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
