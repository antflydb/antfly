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

const backend_types = @import("../../storage/backend_types.zig");
const db_mod = @import("../../storage/db/mod.zig");
const table_catalog = @import("../table_catalog.zig");
const table_write_core = @import("core.zig");
const table_write_managed_db = @import("managed_db.zig");

pub const GroupBatch = table_write_core.GroupBatch;
const drainManagedDbBeforeClose = table_write_managed_db.drainManagedDbBeforeClose;
const validateTableBatchAgainstCatalogSchema = table_write_managed_db.validateTableBatchAgainstCatalogSchema;
const validateTableBatchAgainstSchemaJson = table_write_managed_db.validateTableBatchAgainstSchemaJson;

pub const min_batch_ops: usize = 100;
pub const max_window_ops: usize = 25_000;
pub const max_hbc_leaf_splits_per_publish: usize = 256;
pub const write_coalesce_max_waiters: usize = 64;
pub const write_coalesce_max_ops: usize = 10_000;

// Client-side bulk loads often arrive as serial HTTP chunks. Finish implicit
// dense bulk ingest windows on max ops or idle, not elapsed open time, so an
// active upload does not start HBC replay/publish work mid-stream.
pub const max_idle_ns: u64 = 2 * std.time.ns_per_s;

pub const finish_options: backend_types.BulkIngestFinishOptions = .{
    .compact = false,
    .flush = true,
    .max_deferred_l0_runs = 64,
    .max_deferred_hbc_leaf_splits_per_publish = max_hbc_leaf_splits_per_publish,
};

pub fn shouldDrainManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    // Request latency for weak sync levels must not depend on derived replay.
    // Pending replay is durable in the journal and is resumed by later writes,
    // explicit catch-up, or bulk-session finish.
    return switch (sync_level) {
        .propose, .write, .enrichments => false,
        .full_text, .aknn, .full_index => true,
    };
}

pub fn shouldDrainCachedManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    return shouldDrainManagedDbAfterBatch(sync_level);
}

pub fn autoBulkIngestBatchOps(req: db_mod.types.BatchRequest) usize {
    _ = req;
    // Weak-sync writes are already durable in the primary store plus replay
    // journal. Opening a foreground HBC bulk session here suppresses dense
    // replay notifications for the entire active upload, so indexing only
    // becomes query-visible after the writer goes idle. Let the background
    // derived executor own dense bulk sessions and publish bounded windows.
    return 0;
}

pub fn autoBulkIngestGroupBatchOps(group: anytype, sync_level: db_mod.types.SyncLevel) usize {
    _ = group;
    _ = sync_level;
    return 0;
}

pub fn ensureGroupBatch(
    alloc: std.mem.Allocator,
    grouped: *std.ArrayListUnmanaged(GroupBatch),
    group_id: u64,
) !*GroupBatch {
    for (grouped.items) |*group| {
        if (group.group_id == group_id) return group;
    }
    try grouped.append(alloc, .{ .group_id = group_id });
    return &grouped.items[grouped.items.len - 1];
}

pub fn applyGroupBatchUnchecked(
    db: *db_mod.DB,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
    before_batch: ?*const fn () void,
) !void {
    if (before_batch) |hook| hook();
    try db.batch(.{
        .writes = group.writes.items,
        .deletes = group.deletes.items,
        .relational_identity_rewrites = group.relational_identity_rewrites.items,
        .transforms = group.transforms.items,
        .graph_writes = req.graph_writes,
        .graph_deletes = req.graph_deletes,
        .predicates = req.predicates,
        .timestamp_ns = req.timestamp_ns,
        .sync_level = req.sync_level,
    });
    if (shouldDrainManagedDbAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(db);
}

pub fn applyGroupBatch(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    db: *db_mod.DB,
    table_name: []const u8,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
    before_batch: ?*const fn () void,
) !void {
    try validateTableBatchAgainstCatalogSchema(alloc, catalog, db, table_name, group.writes.items, group.deletes.items, group.transforms.items);
    try applyGroupBatchUnchecked(db, group, req, before_batch);
}

pub fn applyGroupBatchWithSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: ?[]const u8,
    group: GroupBatch,
    req: db_mod.types.BatchRequest,
    before_batch: ?*const fn () void,
) !void {
    try validateTableBatchAgainstSchemaJson(alloc, db, schema_json, group.writes.items, group.deletes.items, group.transforms.items);
    try applyGroupBatchUnchecked(db, group, req, before_batch);
}

pub const WriteCoalesceQueue = struct {
    table_name: []u8,
    group_id: u64,
    draining: bool = false,
    entries: std.ArrayListUnmanaged(*WriteCoalesceEntry) = .empty,
};

pub const WriteCoalesceEntry = struct {
    group: GroupBatch = .{ .group_id = 0 },
    sync_level: db_mod.types.SyncLevel = .write,
    timestamp_ns: u64 = 0,
    done: bool = false,
    err: ?anyerror = null,
};

pub fn deinitWriteCoalesceQueues(
    alloc: std.mem.Allocator,
    queues: *std.ArrayListUnmanaged(WriteCoalesceQueue),
) void {
    for (queues.items) |*queue| {
        alloc.free(queue.table_name);
        queue.entries.deinit(alloc);
    }
    queues.deinit(alloc);
    queues.* = .empty;
}

pub fn findWriteCoalesceQueue(
    queues: []const WriteCoalesceQueue,
    table_name: []const u8,
    group_id: u64,
) ?usize {
    for (queues, 0..) |queue, index| {
        if (queue.group_id == group_id and std.mem.eql(u8, queue.table_name, table_name)) return index;
    }
    return null;
}

pub fn pruneWriteCoalesceQueue(
    alloc: std.mem.Allocator,
    queues: *std.ArrayListUnmanaged(WriteCoalesceQueue),
    index: usize,
) void {
    const queue = &queues.items[index];
    if (queue.draining or queue.entries.items.len > 0) return;
    alloc.free(queue.table_name);
    queue.entries.deinit(alloc);
    _ = queues.orderedRemove(index);
}

pub fn ensureWriteCoalesceQueue(
    alloc: std.mem.Allocator,
    queues: *std.ArrayListUnmanaged(WriteCoalesceQueue),
    table_name: []const u8,
    group_id: u64,
) !*WriteCoalesceQueue {
    if (findWriteCoalesceQueue(queues.items, table_name, group_id)) |index| return &queues.items[index];
    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    try queues.append(alloc, .{
        .table_name = owned_table_name,
        .group_id = group_id,
    });
    return &queues.items[queues.items.len - 1];
}

pub fn writeCoalesceQueueActive(queue: *const WriteCoalesceQueue) bool {
    return queue.draining or queue.entries.items.len != 0;
}

pub fn writeCoalesceQueueEntryCount(
    queues: []const WriteCoalesceQueue,
    table_name: []const u8,
    group_id: u64,
) usize {
    const index = findWriteCoalesceQueue(queues, table_name, group_id) orelse return 0;
    return queues[index].entries.items.len;
}

pub fn coalesceCompatibleEntry(first: *const WriteCoalesceEntry, candidate: *const WriteCoalesceEntry) bool {
    return first.sync_level == candidate.sync_level and first.timestamp_ns == candidate.timestamp_ns;
}

pub fn writeCoalesceEntryOps(entry: *const WriteCoalesceEntry) usize {
    return entry.group.writes.items.len + entry.group.deletes.items.len + entry.group.relational_identity_rewrites.items.len;
}

pub fn takeWriteCoalesceEntries(
    alloc: std.mem.Allocator,
    queue: *WriteCoalesceQueue,
    out_entries: *std.ArrayListUnmanaged(*WriteCoalesceEntry),
) !void {
    std.debug.assert(queue.entries.items.len > 0);
    const first = queue.entries.items[0];
    var take: usize = 0;
    var ops: usize = 0;
    while (take < queue.entries.items.len and take < write_coalesce_max_waiters) : (take += 1) {
        const candidate = queue.entries.items[take];
        if (!coalesceCompatibleEntry(first, candidate)) break;
        const candidate_ops = writeCoalesceEntryOps(candidate);
        if (take > 0 and ops + candidate_ops > write_coalesce_max_ops) break;
        ops += candidate_ops;
    }
    try out_entries.appendSlice(alloc, queue.entries.items[0..take]);
    std.mem.copyForwards(*WriteCoalesceEntry, queue.entries.items[0 .. queue.entries.items.len - take], queue.entries.items[take..]);
    queue.entries.items.len -= take;
}

pub fn failAndClearWriteCoalesceQueue(queue: *WriteCoalesceQueue, err: anyerror) void {
    for (queue.entries.items) |entry| {
        entry.err = err;
        entry.done = true;
    }
    queue.entries.clearRetainingCapacity();
    queue.draining = false;
}

pub fn coalescedEntryBatchRequest(entry: *const WriteCoalesceEntry) db_mod.types.BatchRequest {
    return .{
        .sync_level = entry.sync_level,
        .timestamp_ns = entry.timestamp_ns,
    };
}

pub fn totalCoalescedWrites(entries: []const *WriteCoalesceEntry) usize {
    var total: usize = 0;
    for (entries) |entry| total += entry.group.writes.items.len;
    return total;
}

pub fn totalCoalescedDeletes(entries: []const *WriteCoalesceEntry) usize {
    var total: usize = 0;
    for (entries) |entry| total += entry.group.deletes.items.len;
    return total;
}

pub fn cloneWriteCoalesceGroupBatch(
    alloc: std.mem.Allocator,
    group: GroupBatch,
) !GroupBatch {
    var cloned = GroupBatch{ .group_id = group.group_id };
    errdefer freeWriteCoalesceGroupBatch(alloc, &cloned);

    try cloned.writes.ensureTotalCapacity(alloc, group.writes.items.len);
    for (group.writes.items) |write| {
        const key = try alloc.dupe(u8, write.key);
        const value = alloc.dupe(u8, write.value) catch |err| {
            alloc.free(key);
            return err;
        };
        cloned.writes.appendAssumeCapacity(.{ .key = key, .value = value });
    }

    try cloned.deletes.ensureTotalCapacity(alloc, group.deletes.items.len);
    for (group.deletes.items) |key| {
        const owned_key = try alloc.dupe(u8, key);
        cloned.deletes.appendAssumeCapacity(owned_key);
    }

    try cloned.relational_identity_rewrites.ensureTotalCapacity(alloc, group.relational_identity_rewrites.items.len);
    for (group.relational_identity_rewrites.items) |rewrite| {
        const old_key = try alloc.dupe(u8, rewrite.old_key);
        var old_key_owned = true;
        errdefer if (old_key_owned) alloc.free(old_key);
        const new_key = try alloc.dupe(u8, rewrite.new_key);
        var new_key_owned = true;
        errdefer if (new_key_owned) alloc.free(new_key);
        const value = try alloc.dupe(u8, rewrite.value);
        var value_owned = true;
        errdefer if (value_owned) alloc.free(value);
        cloned.relational_identity_rewrites.appendAssumeCapacity(.{
            .old_key = old_key,
            .new_key = new_key,
            .value = value,
        });
        old_key_owned = false;
        new_key_owned = false;
        value_owned = false;
    }

    return cloned;
}

pub fn freeWriteCoalesceGroupBatch(alloc: std.mem.Allocator, group: *GroupBatch) void {
    for (group.writes.items) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    for (group.deletes.items) |key| alloc.free(key);
    for (group.relational_identity_rewrites.items) |rewrite| {
        alloc.free(@constCast(rewrite.old_key));
        alloc.free(@constCast(rewrite.new_key));
        alloc.free(@constCast(rewrite.value));
    }
    group.deinit(alloc);
}

test "weak sync levels do not drain managed db after batch" {
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.propose));
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.write));
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.enrichments));
    try std.testing.expect(shouldDrainManagedDbAfterBatch(.full_text));
    try std.testing.expect(shouldDrainManagedDbAfterBatch(.aknn));
    try std.testing.expect(shouldDrainManagedDbAfterBatch(.full_index));
    try std.testing.expect(shouldDrainCachedManagedDbAfterBatch(.full_index));
}

test "write coalesce queue helpers reuse and prune idle queues" {
    const alloc = std.testing.allocator;
    var queues = std.ArrayListUnmanaged(WriteCoalesceQueue).empty;
    defer deinitWriteCoalesceQueues(alloc, &queues);

    const docs = try ensureWriteCoalesceQueue(alloc, &queues, "docs", 7001);
    try std.testing.expectEqual(@as(u64, 7001), docs.group_id);
    try std.testing.expectEqualStrings("docs", docs.table_name);

    const reused = try ensureWriteCoalesceQueue(alloc, &queues, "docs", 7001);
    try std.testing.expectEqual(@as(usize, 1), queues.items.len);
    try std.testing.expectEqual(docs, reused);

    const other = try ensureWriteCoalesceQueue(alloc, &queues, "docs", 7002);
    try std.testing.expectEqual(@as(usize, 2), queues.items.len);
    try std.testing.expectEqual(@as(u64, 7002), other.group_id);

    pruneWriteCoalesceQueue(alloc, &queues, 0);
    try std.testing.expectEqual(@as(usize, 1), queues.items.len);
    try std.testing.expectEqual(@as(?usize, null), findWriteCoalesceQueue(queues.items, "docs", 7001));
    try std.testing.expectEqual(@as(?usize, 0), findWriteCoalesceQueue(queues.items, "docs", 7002));
}

test "write coalesce queue helpers keep active queues and count entries" {
    const alloc = std.testing.allocator;
    var queues = std.ArrayListUnmanaged(WriteCoalesceQueue).empty;
    defer deinitWriteCoalesceQueues(alloc, &queues);

    const queue = try ensureWriteCoalesceQueue(alloc, &queues, "docs", 7001);
    var entry = WriteCoalesceEntry{};
    try queue.entries.append(alloc, &entry);

    try std.testing.expect(writeCoalesceQueueActive(queue));
    try std.testing.expectEqual(@as(usize, 1), writeCoalesceQueueEntryCount(queues.items, "docs", 7001));
    pruneWriteCoalesceQueue(alloc, &queues, 0);
    try std.testing.expectEqual(@as(usize, 1), queues.items.len);

    queue.entries.clearRetainingCapacity();
    queue.draining = true;
    try std.testing.expect(writeCoalesceQueueActive(queue));
    pruneWriteCoalesceQueue(alloc, &queues, 0);
    try std.testing.expectEqual(@as(usize, 1), queues.items.len);

    queue.draining = false;
    pruneWriteCoalesceQueue(alloc, &queues, 0);
    try std.testing.expectEqual(@as(usize, 0), queues.items.len);
}

test "write coalesce take helper enforces compatibility and operation limits" {
    const alloc = std.testing.allocator;

    var queue = WriteCoalesceQueue{
        .table_name = try alloc.dupe(u8, "docs"),
        .group_id = 7001,
    };
    defer {
        alloc.free(queue.table_name);
        queue.entries.deinit(alloc);
    }

    var first = WriteCoalesceEntry{ .timestamp_ns = 10 };
    defer first.group.deinit(alloc);
    try first.group.writes.appendNTimes(alloc, .{ .key = "", .value = "" }, 6000);

    var too_large = WriteCoalesceEntry{ .timestamp_ns = 10 };
    defer too_large.group.deinit(alloc);
    try too_large.group.writes.appendNTimes(alloc, .{ .key = "", .value = "" }, 5000);

    var incompatible = WriteCoalesceEntry{ .timestamp_ns = 11 };
    defer incompatible.group.deinit(alloc);
    try incompatible.group.writes.append(alloc, .{ .key = "", .value = "" });

    try queue.entries.append(alloc, &first);
    try queue.entries.append(alloc, &too_large);
    try queue.entries.append(alloc, &incompatible);

    var selected = std.ArrayListUnmanaged(*WriteCoalesceEntry).empty;
    defer selected.deinit(alloc);

    try takeWriteCoalesceEntries(alloc, &queue, &selected);
    try std.testing.expectEqual(@as(usize, 1), selected.items.len);
    try std.testing.expectEqual(&first, selected.items[0]);
    try std.testing.expectEqual(@as(usize, 2), queue.entries.items.len);

    selected.clearRetainingCapacity();
    try takeWriteCoalesceEntries(alloc, &queue, &selected);
    try std.testing.expectEqual(@as(usize, 1), selected.items.len);
    try std.testing.expectEqual(&too_large, selected.items[0]);
    try std.testing.expectEqual(@as(usize, 1), queue.entries.items.len);

    failAndClearWriteCoalesceQueue(&queue, error.TestExpectedError);
    try std.testing.expectEqual(@as(usize, 0), queue.entries.items.len);
    try std.testing.expect(incompatible.done);
    try std.testing.expectEqual(error.TestExpectedError, incompatible.err.?);
}
