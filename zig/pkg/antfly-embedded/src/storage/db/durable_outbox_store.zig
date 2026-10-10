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
const admission = @import("../../common/read_admission.zig");

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

/// The apply fence keeps the legacy singletons and mutation page stable while
/// one admitted snapshot reads both. The original port is a synchronous borrow;
/// no returned buffer, cursor or reader retains it. Rejection never writes.
pub fn readPendingWithAdmission(alloc: Allocator, store: *docstore.DocStore, control: admission.Port) !PendingPage {
    try control.checkpoint();
    var txn = try store.beginReadTxnWithAdmission(.transient, control);
    defer txn.abort();
    const keys = [_][]const u8{ outbox.replication_batch_outbox_key, outbox.replication_replay_outbox_key, outbox.replication_schema_outbox_key };
    var values: [keys.len]?[]const u8 = undefined;
    try txn.getManySortedWithAdmission(&keys, &values, control);
    const batch = if (values[0]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (batch) |bytes| alloc.free(bytes);
    const replay = if (values[1]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (replay) |bytes| alloc.free(bytes);
    const schema = if (values[2]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (schema) |bytes| alloc.free(bytes);
    var cursor = try txn.openCursorWithAdmission(control);
    defer cursor.close();
    var entries: std.ArrayListUnmanaged(docstore.OwnedKVPair) = .empty;
    errdefer {
        for (entries.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        entries.deinit(alloc);
    }
    var next = try cursor.seekAtOrAfterWithAdmission(outbox.replication_outbox_v2_prefix, control);
    while (next) |entry| {
        try control.checkpoint();
        if (!std.mem.startsWith(u8, entry.key, outbox.replication_outbox_v2_prefix)) break;
        const pair = try copyEntryChecked(alloc, entry.key, entry.value, control);
        entries.append(alloc, pair) catch |err| {
            alloc.free(pair.key);
            alloc.free(pair.value);
            return err;
        };
        if (entries.items.len == outbox.replication_outbox_recovery_batch_size) break;
        next = try cursor.nextWithAdmission(control);
    }
    const owned = try entries.toOwnedSlice(alloc);
    errdefer docstore.DocStore.freeResults(alloc, owned);
    try control.checkpoint();
    return .{ .legacy_batch = batch, .legacy_replay = replay, .legacy_schema = schema, .entries = owned };
}

const copy_quantum_bytes = 64 * 1024;
fn copyChecked(alloc: Allocator, bytes: []const u8, control: admission.Port) ![]u8 {
    try control.checkpoint();
    const result = try alloc.alloc(u8, bytes.len);
    errdefer alloc.free(result);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try control.checkpoint();
        const length = @min(bytes.len - offset, copy_quantum_bytes);
        @memcpy(result[offset..][0..length], bytes[offset..][0..length]);
        offset += length;
    }
    try control.checkpoint();
    return result;
}

fn copyEntryChecked(alloc: Allocator, key: []const u8, value: []const u8, control: admission.Port) !docstore.OwnedKVPair {
    const owned_key = try copyChecked(alloc, key, control);
    errdefer alloc.free(owned_key);
    return .{ .key = owned_key, .value = try copyChecked(alloc, value, control) };
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
        pub fn read(allocator: Allocator, source: *docstore.DocStore) !void {
            var page = try readPending(allocator, source);
            defer page.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.read, .{&store});
}

test "durable outbox storage admitted page rejects every original checkpoint and preserves exact pending effects" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const large = try alloc.alloc(u8, 2 * copy_quantum_bytes + 17);
    defer alloc.free(large);
    @memset(large, 0x5a);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = large },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = "unrelated", .value = "keep" },
    }, &.{});
    for (0..outbox.replication_outbox_recovery_batch_size + 3) |i| {
        const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .batch, @intCast(i + 1), 1, "pending");
        defer alloc.free(key);
        try store.putBatch(&.{.{ .key = key, .value = if (i == 0) large else "pending" }}, &.{});
    }
    const State = struct {
        checks: usize = 0,
        fail_at: ?usize = null,
        rejection: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.fail_at == self.checks) return self.rejection;
        }
        fn wait(_: *anyopaque) !void {
            return error.UnexpectedProgressWait;
        }
    };
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
    const control = admission.Port.init(&payload);
    var expected = try readPending(alloc, &store);
    defer expected.deinit(alloc);
    {
        var page = try readPendingWithAdmission(alloc, &store, control);
        defer page.deinit(alloc);
        try std.testing.expectEqual(outbox.replication_outbox_recovery_batch_size, page.entries.len);
        try std.testing.expectEqualSlices(u8, expected.legacy_batch.?, page.legacy_batch.?);
        try std.testing.expectEqualSlices(u8, expected.legacy_replay.?, page.legacy_replay.?);
        try std.testing.expectEqualSlices(u8, expected.legacy_schema.?, page.legacy_schema.?);
        for (expected.entries, page.entries) |wanted, actual| {
            try std.testing.expectEqualSlices(u8, wanted.key, actual.key);
            try std.testing.expectEqualSlices(u8, wanted.value, actual.value);
        }
    }
    const successful_checks = state.checks;
    const snapshot_refs = backend.state.?.refs;
    try std.testing.expect(successful_checks > 2 * outbox.replication_outbox_recovery_batch_size);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |rejection| {
        for (1..successful_checks + 1) |fail_at| {
            state = .{ .fail_at = fail_at, .rejection = rejection };
            try std.testing.expectError(rejection, readPendingWithAdmission(alloc, &store, control));
            try std.testing.expectEqual(fail_at, state.checks);
            try std.testing.expectEqual(snapshot_refs, backend.state.?.refs);
        }
    }
    // Every failed read must release its physical reader/cursor and temporary
    // copies. The same store remains reusable, with no deletion or replay.
    state = .{};
    var retry = try readPendingWithAdmission(alloc, &store, control);
    defer retry.deinit(alloc);
    try std.testing.expectEqual(expected.entries.len, retry.entries.len);
    for (expected.entries, retry.entries) |wanted, actual| {
        try std.testing.expectEqualSlices(u8, wanted.key, actual.key);
        try std.testing.expectEqualSlices(u8, wanted.value, actual.value);
    }
    try std.testing.expectEqualSlices(u8, expected.legacy_batch.?, retry.legacy_batch.?);
    const unrelated = try store.get(alloc, "unrelated");
    defer alloc.free(unrelated);
    try std.testing.expectEqualStrings("keep", unrelated);
}

test "durable outbox storage admitted copies use finite byte quanta and release partial allocations" {
    const alloc = std.testing.allocator;
    const bytes = try alloc.alloc(u8, 2 * copy_quantum_bytes + 1);
    defer alloc.free(bytes);
    @memset(bytes, 0x6b);
    const State = struct {
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
    };
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const control = admission.Port.init(&payload);
    const result = try copyChecked(alloc, bytes, control);
    defer alloc.free(result);
    try std.testing.expectEqualSlices(u8, bytes, result);
    try std.testing.expectEqual(@as(usize, 5), state.checks);
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .row_policy, 11, 1, "pending");
    defer alloc.free(key);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = bytes },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = key, .value = bytes },
    }, &.{});
    const Check = struct {
        fn read(allocator: Allocator, source: *docstore.DocStore, port: admission.Port) !void {
            var page = try readPendingWithAdmission(allocator, source, port);
            defer page.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.read, .{ &store, control });
}

test "durable outbox storage admitted read preserves original rejection during held physical reader gates" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.putBatch(&.{.{ .key = outbox.replication_batch_outbox_key, .value = "pending" }}, &.{});
    const State = struct {
        checks: usize = 0,
        waits: usize = 0,
        reject: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= 32) return self.reject;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |rejection| {
        for (0..2) |gate| {
            if (gate == 0) store.payload_policy_mutex.lockUncancelable(std.testing.io) else try std.testing.expect(backend.mutex.tryLock());
            var held = true;
            defer if (held) {
                if (gate == 0) store.payload_policy_mutex.unlock(std.testing.io) else backend.mutex.unlock();
            };
            const refs = backend.state.?.refs;
            var state: State = .{ .reject = rejection };
            const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
            try std.testing.expectError(rejection, readPendingWithAdmission(alloc, &store, admission.Port.init(&payload)));
            try std.testing.expectEqual(@as(usize, 32), state.checks);
            try std.testing.expect(state.waits > 0);
            try std.testing.expectEqual(refs, backend.state.?.refs);
            if (gate == 0) {
                try std.testing.expect(!store.payload_policy_mutex.tryLock());
                store.payload_policy_mutex.unlock(std.testing.io);
            } else {
                try std.testing.expect(!backend.mutex.tryLock());
                backend.mutex.unlock();
            }
            held = false;
            // The caller's existing gate was neither unlocked nor stolen.
            // Healthy legacy recovery still observes the same obligation.
            var retry = try readPending(alloc, &store);
            defer retry.deinit(alloc);
            try std.testing.expectEqualStrings("pending", retry.legacy_batch.?);
            try std.testing.expectEqual(@as(usize, 0), retry.entries.len);
        }
    }
}
