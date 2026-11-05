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

//! Bounded, restartable cleanup of expiring job/memo records. Conditional
//! deletes cannot race a renewed/reclaimed job. Provider cursors are durable;
//! active early keys cannot starve expired records at the end of a prefix.
const std = @import("std");
const Store = @import("lake_index_store.zig").Store;
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const Progress = struct { version: u16 = 2, cursor: ?[]const u8 = null, after_key: ?[]const u8 = null };
pub fn collect(a: A, store: *Store, table: u64, kind: []const u8, context: Context) !void {
    try context.ensureActive();
    const bounded = @import("lake_cleanup_budget.zig").turnContext(context);
    collectTurn(a, store, table, kind, bounded) catch |err| {
        try context.ensureActive();
        if (err == error.LakeCleanupTurnYield) return;
        if ((err == error.Canceled or err == error.Cancelled or err == error.DeadlineExceeded) and @import("antfly_platform").time.monotonicNs() >= bounded.deadline_ns.?) return;
        return err;
    };
}
fn collectTurn(a: A, store: *Store, table: u64, kind: []const u8, context: Context) !void {
    try context.ensureActive();
    var client = store.opened.client;
    const types = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types;
    const checkpoint_token = types.contextCancellation(&context);
    var work = context;
    work.deadline_ns = context.deadline_ns.? -| std.time.ns_per_s;
    if (@import("antfly_platform").time.monotonicNs() >= work.deadline_ns.?) return;
    const token = types.contextCancellation(&work);
    const prefix = try std.fmt.allocPrint(a, "{s}{s}{s}/{d}/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", kind, table });
    defer a.free(prefix);
    const checkpoint = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/{d}/{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, kind });
    defer a.free(checkpoint);
    var previous = client.getObject(store.opened.bucket, checkpoint, .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = token }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => {
            try context.ensureActive();
            if (workExpired(work, err)) return error.LakeCleanupTurnYield;
            return err;
        },
    };
    defer if (previous) |*value| value.deinit(client.allocator);
    var parsed: ?std.json.Parsed(Progress) = if (previous) |value| try std.json.parseFromSlice(Progress, a, value.body, .{}) else null;
    defer if (parsed) |*value| value.deinit();
    const progress: Progress = if (parsed) |value| value.value else .{};
    if (progress.version != 2 or (progress.cursor != null and progress.after_key != null)) return error.InvalidExpiringLakeObject;
    if (progress.after_key) |key| if (!std.mem.startsWith(u8, key, prefix) or key.len > 4096) return error.InvalidExpiringLakeObject;
    var page = client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 128, .continuation_token = progress.cursor, .start_after = progress.after_key, .cancellation = token }) catch |err| {
        try context.ensureActive();
        if (workExpired(work, err)) return error.LakeCleanupTurnYield;
        return err;
    };
    defer page.deinit(client.allocator);
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    var processed: usize = 0;
    var last_key = progress.after_key;
    for (page.entries) |object| {
        try context.ensureActive();
        // Provider I/O uses an earlier deadline than the progress CAS. If an
        // in-flight operation reaches that boundary, retain the previous key
        // and checkpoint only records whose complete processing was observed.
        if (processed == 16 or @import("antfly_platform").time.monotonicNs() >= work.deadline_ns.?) break;
        if (!std.mem.startsWith(u8, object.key, prefix) or object.key.len > 4096) return error.InvalidExpiringLakeObject;
        collectRecord(a, client, store.opened.bucket, object.key, now, work) catch |err| {
            try context.ensureActive();
            if (workExpired(work, err)) break;
            return err;
        };
        processed += 1;
        last_key = object.key;
    }
    if (processed == 0 and page.entries.len != 0) return;
    if (previous == null and page.entries.len == 0) return;
    const complete_page = processed == page.entries.len;
    const bytes = try std.json.Stringify.valueAlloc(a, Progress{ .cursor = if (complete_page) page.next_continuation_token else null, .after_key = if (complete_page) null else last_key }, .{});
    defer a.free(bytes);
    if (previous) |value| if (std.mem.eql(u8, bytes, value.body)) return;
    var saved = client.compareAndPut(store.opened.bucket, checkpoint, bytes, if (previous) |value| value.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = checkpoint_token }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => return,
        else => return err,
    };
    saved.deinit(client.allocator);
}

fn workExpired(work: Context, err: anyerror) bool {
    return (err == error.Canceled or err == error.Cancelled or err == error.DeadlineExceeded) and @import("antfly_platform").time.monotonicNs() >= work.deadline_ns.?;
}
fn collectRecord(a: A, client_value: @import("objectstore").Client, bucket: []const u8, key: []const u8, now: u64, context: Context) !void {
    try context.ensureActive();
    var client = client_value;
    const token = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&context);
    var value = client.getObject(bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = 2 * 1024 * 1024, .cancellation = token }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return,
        else => return err,
    };
    defer value.deinit(client.allocator);
    var document = try std.json.parseFromSlice(std.json.Value, a, value.body, .{});
    defer document.deinit();
    if (document.value != .object) return error.InvalidExpiringLakeObject;
    const expires = document.value.object.get("expires_ms") orelse return;
    if (expires != .integer or expires.integer <= 0) return error.InvalidExpiringLakeObject;
    if (@as(u64, @intCast(expires.integer)) +| 30_000 >= now) return;
    if (document.value.object.get("lease_until_ms")) |lease| if (lease == .integer and lease.integer > 0 and @as(u64, @intCast(lease.integer)) > now) return;
    client.compareAndDelete(bucket, key, value.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = token }) catch |err| switch (err) {
        error.PreconditionFailed, error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
}

test "external lake expiring cleanup respects active leases and advances beyond one page" {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-expiring-state");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try Store.open(a, &config, null, false);
    defer store.deinit();
    var client = store.opened.client;
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    for (0..140) |i| {
        const key = try std.fmt.allocPrint(a, "{s}{s}recent-search/7/{d:0>4}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", i });
        defer a.free(key);
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .expires_ms = if (i < 128) now +| 60_000 else @as(u64, 1) }, .{});
        defer a.free(bytes);
        var written = try client.putObject(store.opened.bucket, key, bytes, .{});
        written.deinit(client.allocator);
    }
    const leased = try std.fmt.allocPrint(a, "{s}{s}recent-search/7/9999.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(leased);
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .expires_ms = @as(u64, 1), .lease_until_ms = now +| 60_000 }, .{});
    defer a.free(bytes);
    var written = try client.putObject(store.opened.bucket, leased, bytes, .{});
    written.deinit(client.allocator);
    const checkpoint_key = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/7/recent-search.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(checkpoint_key);
    const context: Context = .{ .io = std.testing.io };
    const metrics = @import("objectstore").operation_metrics;
    const get_index = @backingInt(metrics.Operation.get);
    const before = metrics.runtime.snapshot().calls[get_index];
    try collect(a, &store, 7, "recent-search", context);
    try std.testing.expect(metrics.runtime.snapshot().calls[get_index] - before <= 20);
    var checkpoint = try client.getObject(store.opened.bucket, checkpoint_key, .{});
    defer checkpoint.deinit(client.allocator);
    var checkpoint_state = try std.json.parseFromSlice(Progress, a, checkpoint.body, .{});
    defer checkpoint_state.deinit();
    try std.testing.expectEqual(@as(u16, 2), checkpoint_state.value.version);
    try std.testing.expect(checkpoint_state.value.after_key != null and checkpoint_state.value.cursor == null);
    try std.testing.expect(std.mem.endsWith(u8, checkpoint_state.value.after_key.?, "0015.json"));
    // Small turns must checkpoint past live records, including partial pages,
    // so an active early prefix cannot starve the expired records at its end.
    for (0..15) |_| try collect(a, &store, 7, "recent-search", context);
    const prefix = try std.fmt.allocPrint(a, "{s}{s}recent-search/7/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(prefix);
    var remaining = try client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 256 });
    defer remaining.deinit(client.allocator);
    try std.testing.expectEqual(@as(usize, 129), remaining.entries.len);
    var active = try client.getObject(store.opened.bucket, leased, .{});
    defer active.deinit(client.allocator);
}
