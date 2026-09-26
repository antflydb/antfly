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

//! Receiver-owned preparation. The existing replicated checkpoint/page
//! appliers remain the only mutation and exact cleanup-prefix authority.
const std = @import("std");
const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
const wire = @import("online_merge_io_contract.zig");
const pages = @import("merge_page_contract.zig");
const merge = @import("merge_contract.zig");
const types = @import("types.zig");
const keys = @import("../internal_keys.zig");
const Allocator = std.mem.Allocator;
pub const max_scan_entries = 256;
pub const max_scan_key_bytes = 4 * pages.max_bytes;

/// Advisory recovery cursor, never a committed receiver receipt. A lost cache
/// restarts the bounded scan; completed pages still prove their exact prefix
/// under the native apply fence. No row payload or read snapshot is retained.
pub const Cache = struct {
    alloc: ?Allocator = null,
    receipt_digest: ?[32]u8 = null,
    after: ?[]u8 = null,
    deletes: std.ArrayList([]const u8) = .empty,
    bytes: usize = 0,
    pub fn clear(self: *Cache) void {
        const alloc = self.alloc orelse return;
        if (self.after) |value| alloc.free(value);
        for (self.deletes.items) |value| alloc.free(value);
        self.deletes.deinit(alloc);
        self.* = .{};
    }
};

fn context(scope: @import("online_source_contract.zig").Scope) types.MergeReplicationContext {
    return .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt };
}

fn encoded(alloc: Allocator, request: wire.Request, batch: ?types.BatchRequest) ![]u8 {
    return std.json.Stringify.valueAlloc(alloc, wire.Prepared{ .scope = request.scope, .request = batch }, .{});
}

pub fn executeJson(db: *DB, alloc: Allocator, request: wire.Request, cancellation: types.CancellationToken) ![]u8 {
    try request.validate();
    if (!db.core.identity_namespace.eql(request.scope.receiver_namespace)) return error.MergeCopyFenced;
    const io = db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
    const owner_cache = &db.online_merge_reader;
    try owner_cache.mutex.lock(io);
    defer owner_cache.mutex.unlock(io);
    const cache = &owner_cache.receiver;
    errdefer cache.clear();
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    const state_raw = txn.get(@import("merge_state.zig").key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var state = if (state_raw) |raw| try merge.decodeAlloc(alloc, raw) else null;
    defer if (state) |*value| value.deinit(alloc);
    try cancellation.check();
    if (request.operation == .checkpoint) return try checkpoint(db, alloc, request, state, &txn);
    if (request.operation != .cleanup) return error.InvalidArgument;
    const receipt = request.operation.cleanup;
    const current = state orelse return error.MergeCopyFenced;
    try validateState(request, current);
    if (current.phase != .accepting or !std.meta.eql(current.copy_attempt, request.scope.copy_attempt)) return error.MergeCopyFenced;
    const expected_receipt = try pages.encode(alloc, receipt);
    defer alloc.free(expected_receipt);
    const persisted = txn.get(pages.key) catch |err| switch (err) {
        error.NotFound => return error.MergePageSequenceGap,
        else => return err,
    };
    if (!std.mem.eql(u8, persisted, expected_receipt)) return error.MergePageSequenceGap;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(expected_receipt, &digest, .{});
    if (cache.receipt_digest == null or !std.mem.eql(u8, &cache.receipt_digest.?, &digest)) {
        cache.clear();
        cache.alloc = db.alloc;
        cache.receipt_digest = digest;
    }
    const merged = current.merged_range orelse return error.MergeCopyFenced;
    const donor: types.ByteRange = if (!std.mem.eql(u8, merged.start, current.receiver_base_range.start)) .{ .start = merged.start, .end = current.receiver_base_range.start } else .{ .start = current.receiver_base_range.end, .end = merged.end };
    if (receipt.phase == .cleanup_integrity) {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const cleaned = try @import("online_integrity_shadow.zig").cleanup(scratch.allocator(), &txn, donor, receipt.cursor);
        try cancellation.check();
        var batch: types.BatchRequest = .{
            .merge_replication = context(request.scope),
            .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = .cleanup_integrity, .after = receipt.cursor, .next = if (cleaned.effects.len != 0) cleaned.effects[cleaned.effects.len - 1].key else "", .exhausted = cleaned.exhausted, .digest = @splat(0), .integrity = cleaned.effects },
        };
        batch.merge_page.?.digest = pages.commandDigest(batch);
        try pages.validateRequest(batch);
        return encoded(alloc, request, batch);
    }
    const lower = if (receipt.cursor.len != 0) try keys.documentKeyAlloc(alloc, receipt.cursor) else try keys.documentRangeLowerAlloc(alloc, donor.start);
    defer alloc.free(lower);
    const upper = if (donor.end.len != 0) try keys.documentRangeLowerAlloc(alloc, donor.end) else null;
    defer if (upper) |value| alloc.free(value);
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    cursor.setUpperBound(upper);
    var entry = try cursor.seekAtOrAfter(cache.after orelse lower);
    var visited: usize = 0;
    var scanned_bytes: usize = 0;
    var exhausted = true;
    while (entry) |row| : (entry = try cursor.next()) {
        try cancellation.check();
        if (upper) |bound| if (std.mem.order(u8, row.key, bound) != .lt) break;
        if (cache.after) |after| if (std.mem.order(u8, row.key, after) != .gt) continue;
        if (visited >= max_scan_entries or (visited > 0 and scanned_bytes >= max_scan_key_bytes)) return encoded(alloc, request, null);
        if (row.key.len > 2 * pages.max_cursor_bytes) return error.InvalidMergePage;
        visited += 1;
        scanned_bytes +|= row.key.len;
        if (keys.isStoredDocumentRowKey(row.key)) {
            const logical = (try keys.decodeStoredDocumentRowKeyAlloc(db.alloc, row.key)).?;
            var owned = true;
            defer if (owned) db.alloc.free(logical);
            if (receipt.cursor.len == 0 or std.mem.order(u8, logical, receipt.cursor) == .gt) {
                if (cache.deletes.items.len == pages.max_rows or (cache.deletes.items.len > 0 and logical.len > pages.max_bytes -| cache.bytes)) {
                    exhausted = false;
                    break;
                }
                if (logical.len > pages.max_cursor_bytes) return error.InvalidMergePage;
                try cache.deletes.append(db.alloc, logical);
                cache.bytes += logical.len;
                owned = false;
            }
        }
        const saved = try db.alloc.dupe(u8, row.key);
        if (cache.after) |old| db.alloc.free(old);
        cache.after = saved;
    }
    var batch: types.BatchRequest = .{ .merge_replication = context(request.scope), .deletes = cache.deletes.items, .merge_page = .{
        .source = receipt.source,
        .sequence = try std.math.add(u64, receipt.sequence, 1),
        .phase = .cleanup,
        .after = receipt.cursor,
        .next = if (cache.deletes.items.len != 0) cache.deletes.items[cache.deletes.items.len - 1] else "",
        .exhausted = exhausted,
        .digest = @splat(0),
    } };
    batch.merge_page.?.digest = pages.commandDigest(batch);
    try pages.validateRequest(batch);
    const result = try encoded(alloc, request, batch);
    cache.clear();
    return result;
}

fn validateState(request: wire.Request, state: merge.State) !void {
    if (state.transition_id != request.scope.fence.transition_id or state.donor_group_id != request.scope.fence.owner_group_id or state.receiver_group_id != request.scope.fence.peer_group_id) return error.MergeCopyFenced;
}

fn checkpoint(db: *DB, alloc: Allocator, request: wire.Request, state: ?merge.State, txn: anytype) ![]u8 {
    var command = request.operation.checkpoint;
    // Acceptance reserves the range, not a copy attempt. Only begin_copy may
    // bind that attempt and its source/page receipt atomically.
    if (command.kind == .accept) command.copy_attempt = .{};
    const base: types.ByteRange = .{ .start = command.receiver_base_start, .end = command.receiver_base_end };
    const merged: types.ByteRange = .{ .start = command.merged_start, .end = command.merged_end };
    if (!rangeValid(base) or !rangeValid(merged) or (std.mem.eql(u8, base.start, merged.start) == std.mem.eql(u8, base.end, merged.end)) or
        std.mem.order(u8, merged.start, base.start) == .gt or (merged.end.len != 0 and (base.end.len == 0 or std.mem.order(u8, merged.end, base.end) == .lt))) return error.InvalidMergePage;
    if (command.kind == .rollback) if (state) |current| {
        try validateState(request, current);
        if (current.copy_attempt.sequence == 0) command.copy_attempt = current.copy_attempt;
    };
    const fold = try merge.planCheckpointApply(alloc, if (state) |*current| current else null, db.getRange(), command);
    defer fold.deinit(alloc);
    try validateState(request, fold.state);
    if (command.kind != .accept and !std.meta.eql(fold.state.copy_attempt, command.copy_attempt)) return error.MergeCopyFenced;
    if (command.kind == .begin_copy or (command.kind == .accept and command.page_source != null and command.page_source.?.integrity != null)) {
        if (command.kind == .begin_copy) if (state) |current| if (current.copy_attempt.sequence != 0 and !std.meta.eql(current.copy_attempt, command.copy_attempt)) return error.MergeCopyFenced;
        const source = command.page_source orelse return error.InvalidMergePage;
        try source.validate();
        if (!source.namespace.eql(request.scope.fence.namespace) or source.retention == null or source.retention.?.epoch != request.scope.consumer_epoch or
            !(command.page_receiver_namespace orelse return error.InvalidMergePage).eql(request.scope.receiver_namespace)) return error.MergeCopyFenced;
    } else if (command.page_source != null or command.page_receiver_namespace != null) return error.InvalidMergePage;
    const raw_receipt = txn.get(pages.key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var receipt = if (raw_receipt) |raw| try pages.decode(alloc, raw) else null;
    defer if (receipt) |*value| value.deinit();
    if (command.kind == .bootstrap_complete or command.kind == .finalize) {
        const value = if (receipt) |parsed| parsed.value else return error.MergePageIncomplete;
        if (!value.matches(context(request.scope)) or value.phase != .complete or value.assembly != null or value.final_applied_index == 0 or command.bootstrap_applied_index != value.final_applied_index) return error.MergePageIncomplete;
    }
    _ = try pages.checkpointPlan(state, fold.state, command, if (receipt) |value| value.value else null);
    // Checkpoints use the same authenticated, exact-destination forwarding
    // authority as page payloads. The checkpoint itself still decides when
    // a copy attempt is bound (acceptance deliberately leaves it unbound).
    var replication = context(request.scope);
    replication.copy_attempt = command.copy_attempt;
    return encoded(alloc, request, .{ .merge_replication = replication, .merge_checkpoint = command });
}

fn rangeValid(range: types.ByteRange) bool {
    return range.end.len == 0 or std.mem.order(u8, range.start, range.end) == .lt;
}

test "relational index system online receiver bounds cleanup and uses shared checkpoint receipts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/receiver", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 11, .shard_id = 102, .range_id = 102 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"old\":1}" }, .{ .key = "c", .value = "{\"old\":2}" }, .{ .key = "m", .value = "{\"base\":true}" } } });
    // Hundreds of non-row keys must yield without a false EOF or loading the
    // artifacts' payloads; their physical continuation remains advisory.
    for (0..600) |i| {
        const artifact = try keys.chunkArtifactKeyAlloc(alloc, "b", "fixture", @intCast(i));
        defer alloc.free(artifact);
        try db.core.store.put(artifact, "opaque artifact");
    }
    try db.updateRange(.{ .start = "m", .end = "z" });
    var scope: @import("online_source_contract.zig").Scope = .{
        .fence = .{ .admission_epoch = 1, .transition_id = 100, .attempt = 1, .peer_group_id = 102, .owner_group_id = 101, .role = .merge_source, .namespace = .{ .table_id = 11, .shard_id = 101, .range_id = 101 }, .catalog_digest = @splat(7) },
        .receiver_namespace = db.core.identity_namespace,
        .consumer_epoch = 3,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    var control: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 100, .donor_group_id = 101, .receiver_group_id = 102, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z", .copy_attempt = scope.copy_attempt };
    const Apply = struct {
        fn run(owner: *DB, request: wire.Request) !void {
            const output = try executeJson(owner, std.testing.allocator, request, .none);
            defer std.testing.allocator.free(output);
            var parsed = try std.json.parseFromSlice(wire.Prepared, std.testing.allocator, output, .{});
            defer parsed.deinit();
            const batch = parsed.value.request orelse return error.TestExpectedEqual;
            const replication = batch.merge_replication orelse return error.TestExpectedEqual;
            const checkpoint_value = batch.merge_checkpoint orelse return error.TestExpectedEqual;
            try std.testing.expectEqualDeep(checkpoint_value.copy_attempt, replication.copy_attempt);
            try std.testing.expect(replication.identity_namespace.eql(request.scope.receiver_namespace));
            try owner.batch(batch);
        }
        fn page(owner: *DB, source: pages.Source, seq: u64, phase: pages.Phase, tail_finish: bool, scope_value: @import("online_source_contract.zig").Scope) !void {
            var batch: types.BatchRequest = .{ .merge_replication = context(scope_value), .merge_page = .{ .source = source, .sequence = seq, .phase = phase, .exhausted = true, .digest = @splat(0), .tail = if (tail_finish) .{ .finish = .{ .through_sequence = 40, .applied_index = 99, .cut_digest = @splat(9) } } else null } };
            batch.merge_page.?.digest = pages.commandDigest(batch);
            try owner.batch(batch);
        }
    };
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    const source: pages.Source = .{ .namespace = scope.fence.namespace, .pin_digest = @splat(8), .applied_index = 20, .retention = .{ .epoch = 3, .after_sequence = 40 } };
    control.kind = .begin_copy;
    control.page_source = source;
    control.page_receiver_namespace = scope.receiver_namespace;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    var receipt = (try db.mergeCopyPageStatus(alloc)).?;
    defer receipt.deinit();
    var pending: usize = 0;
    while (true) {
        const output = try executeJson(&db, alloc, .{ .scope = scope, .operation = .{ .cleanup = receipt.value } }, .none);
        defer alloc.free(output);
        var parsed = try std.json.parseFromSlice(wire.Prepared, alloc, output, .{});
        defer parsed.deinit();
        if (parsed.value.request) |batch| {
            try std.testing.expectEqual(@as(usize, 2), batch.deletes.len);
            try std.testing.expect(batch.merge_page.?.exhausted);
            try db.batch(batch);
            break;
        }
        pending += 1;
        try std.testing.expect(pending <= 4);
    }
    try std.testing.expect(pending >= 2);
    try std.testing.expectEqual(@as(?u64, 1), try @import("range_cardinality.zig").load(alloc, db.core.store));
    try std.testing.expectError(error.MergePageSequenceGap, executeJson(&db, alloc, .{ .scope = scope, .operation = .{ .cleanup = receipt.value } }, .none));
    const base = (try db.get(alloc, "m")).?;
    defer alloc.free(base);
    try std.testing.expectEqualStrings("{\"base\":true}", base);
    control.kind = .bootstrap_complete;
    control.page_source = null;
    control.page_receiver_namespace = null;
    control.bootstrap_applied_index = 99;
    try std.testing.expectError(error.MergePageIncomplete, executeJson(&db, alloc, .{ .scope = scope, .operation = .{ .checkpoint = control } }, .none));
    var copied: types.BatchRequest = .{ .writes = &.{.{ .key = "b", .value = "{\"copied\":true}" }}, .merge_replication = context(scope), .merge_page = .{ .source = source, .sequence = 2, .phase = .rows, .next = "b", .exhausted = true, .timestamps = &.{42}, .digest = @splat(0) } };
    copied.merge_page.?.digest = pages.commandDigest(copied);
    try db.batch(copied);
    try std.testing.expectEqual(@as(?u64, 2), try @import("range_cardinality.zig").load(alloc, db.core.store));
    try Apply.page(&db, source, 3, .artifacts, false, scope);
    try Apply.page(&db, source, 4, .tail, true, scope);
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    control.kind = .finalize;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    try std.testing.expectEqual(@as(?u64, 2), try @import("range_cardinality.zig").load(alloc, db.core.store));
    // A later transition may reserve the now enlarged base. Cancel before
    // begin_copy must use the durable zero attempt, not manufacture a copy.
    scope.fence.transition_id = 101;
    control.transition_id = 101;
    control.kind = .accept;
    control.bootstrap_applied_index = 0;
    control.receiver_base_start = "a";
    control.merged_start = "0";
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    // Base mutations remain live during copying. Cancellation must restore
    // their current count, not the count captured at initial acceptance.
    try db.batch(.{ .writes = &.{.{ .key = "n", .value = "{}" }} });
    control.kind = .rollback;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    try std.testing.expectEqual(@as(?u64, 3), try @import("range_cardinality.zig").load(alloc, db.core.store));
    try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
    scope.fence.transition_id = 102;
    control.transition_id = 102;
    control.kind = .accept;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    control.kind = .begin_copy;
    control.page_source = source;
    control.page_receiver_namespace = scope.receiver_namespace;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    var stale_attempt = control;
    stale_attempt.copy_attempt.sequence += 1;
    try std.testing.expectError(error.MergeCopyFenced, executeJson(&db, alloc, .{ .scope = scope, .operation = .{ .checkpoint = stale_attempt } }, .none));
    try Apply.page(&db, source, 1, .cleanup, false, scope);
    copied.writes = &.{.{ .key = "1", .value = "{}" }};
    copied.merge_replication = context(scope);
    copied.merge_page.?.next = "1";
    copied.merge_page.?.digest = pages.commandDigest(copied);
    try db.batch(copied);
    try db.batch(.{ .deletes = &.{ "m", "n" } });
    try std.testing.expectEqual(@as(?u64, 2), try @import("range_cardinality.zig").load(alloc, db.core.store));
    control.kind = .rollback;
    control.page_source = null;
    control.page_receiver_namespace = null;
    try Apply.run(&db, .{ .scope = scope, .operation = .{ .checkpoint = control } });
    try std.testing.expectEqual(@as(?u64, 1), try @import("range_cardinality.zig").load(alloc, db.core.store));
}
