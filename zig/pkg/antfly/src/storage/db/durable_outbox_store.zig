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

//! Storage access to durable pending replication effects. Callers hold their
//! mutation/apply fence when reading or clearing. Publication and HA policy
//! are deliberately absent from this owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore = @import("../docstore.zig");
const outbox = @import("durable_outbox.zig");

pub const PendingPage = struct {
    legacy_batch: ?[]u8,
    legacy_replay: ?[]u8,
    legacy_schema: ?[]u8,
    entries: []docstore.OwnedKVPair,

    pub fn isEmpty(self: PendingPage) bool {
        return self.legacy_batch == null and self.legacy_replay == null and
            self.legacy_schema == null and self.entries.len == 0;
    }

    pub fn deinit(self: *PendingPage, alloc: Allocator) void {
        if (self.legacy_batch) |bytes| alloc.free(bytes);
        if (self.legacy_replay) |bytes| alloc.free(bytes);
        if (self.legacy_schema) |bytes| alloc.free(bytes);
        docstore.DocStore.freeResults(alloc, self.entries);
        self.* = undefined;
    }
};

/// Own one bounded page and all three rolling-upgrade singleton obligations.
/// Buffers survive releasing the apply fence so remote waits do not hold it.
pub fn readPending(alloc: Allocator, store: *docstore.DocStore) !PendingPage {
    const batch = try readOptional(alloc, store, outbox.replication_batch_outbox_key);
    errdefer if (batch) |bytes| alloc.free(bytes);
    const replay = try readOptional(alloc, store, outbox.replication_replay_outbox_key);
    errdefer if (replay) |bytes| alloc.free(bytes);
    const schema = try readOptional(alloc, store, outbox.replication_schema_outbox_key);
    errdefer if (schema) |bytes| alloc.free(bytes);
    return .{
        .legacy_batch = batch,
        .legacy_replay = replay,
        .legacy_schema = schema,
        .entries = try store.scanPrefixPage(alloc, outbox.replication_outbox_v2_prefix, null, outbox.replication_outbox_recovery_batch_size),
    };
}

/// Clear only the mutation which the replication adapter successfully
/// published. No prefix deletion or arbitrary internal key access is exposed.
pub fn clearPublished(store: *docstore.DocStore, key: []const u8) !void {
    if (!std.mem.eql(u8, key, outbox.replication_batch_outbox_key) and
        !std.mem.eql(u8, key, outbox.replication_replay_outbox_key) and
        !std.mem.eql(u8, key, outbox.replication_schema_outbox_key))
        _ = try outbox.durableReplicationOutboxKindFromKey(key);
    try store.putBatch(&.{}, &.{key});
}

fn readOptional(alloc: Allocator, store: *docstore.DocStore, key: []const u8) !?[]u8 {
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

test "storage.hot_standby durable outbox storage pages legacy and mutation obligations without broad deletion" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const value = try outbox.encodeDurableReplicationOutboxAlloc(alloc, 7, "pending");
    defer alloc.free(value);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = value },
        .{ .key = outbox.replication_replay_outbox_key, .value = value },
        .{ .key = outbox.replication_schema_outbox_key, .value = value },
        .{ .key = "unrelated", .value = "keep" },
    }, &.{});
    for (0..outbox.replication_outbox_recovery_batch_size + 3) |i| {
        const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .batch, @intCast(i + 1), 1, "pending");
        defer alloc.free(key);
        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
    }
    {
        var page = try readPending(alloc, &store);
        defer page.deinit(alloc);
        try std.testing.expect(!page.isEmpty());
        try std.testing.expectEqual(outbox.replication_outbox_recovery_batch_size, page.entries.len);
        try std.testing.expectEqualSlices(u8, value, page.legacy_batch.?);
        try std.testing.expectEqualSlices(u8, value, page.legacy_replay.?);
        try std.testing.expectEqualSlices(u8, value, page.legacy_schema.?);
        try clearPublished(&store, page.entries[0].key);
        try std.testing.expectError(error.NotFound, store.get(alloc, page.entries[0].key));
        const surviving = try store.get(alloc, page.entries[1].key);
        defer alloc.free(surviving);
        try std.testing.expectEqualSlices(u8, value, surviving);
        try std.testing.expectError(error.InvalidHAOutbox, clearPublished(&store, "unrelated"));
        try clearPublished(&store, outbox.replication_batch_outbox_key);
        try clearPublished(&store, outbox.replication_replay_outbox_key);
        try clearPublished(&store, outbox.replication_schema_outbox_key);
    }
    while (true) {
        var page = try readPending(alloc, &store);
        defer page.deinit(alloc);
        if (page.isEmpty()) break;
        for (page.entries) |entry| try clearPublished(&store, entry.key);
    }
    const surviving = try store.get(alloc, "unrelated");
    defer alloc.free(surviving);
    try std.testing.expectEqualStrings("keep", surviving);
}

test "storage.hot_standby durable outbox storage releases partial pages on allocation failure" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .schema, 9, 1, "pending");
    defer alloc.free(key);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = "legacy batch" },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = key, .value = "pending" },
    }, &.{});
    const Check = struct {
        fn read(allocator: Allocator, source: *docstore.DocStore) !void {
            var page = try readPending(allocator, source);
            defer page.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.read, .{&store});
}
