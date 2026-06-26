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
const table_write_core = @import("core.zig");
const table_write_managed_db = @import("managed_db.zig");

pub const GroupBatch = table_write_core.GroupBatch;
const drainManagedDbBeforeClose = table_write_managed_db.drainManagedDbBeforeClose;

pub const min_batch_ops: usize = 100;
pub const max_window_ops: usize = 25_000;
pub const max_hbc_leaf_splits_per_publish: usize = 256;

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
        .full_text, .aknn, .full_index => false,
    };
}

pub fn shouldDrainCachedManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    _ = sync_level;
    return false;
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

pub fn coalesceCompatibleEntry(first: *const WriteCoalesceEntry, candidate: *const WriteCoalesceEntry) bool {
    return first.sync_level == candidate.sync_level and first.timestamp_ns == candidate.timestamp_ns;
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
}
