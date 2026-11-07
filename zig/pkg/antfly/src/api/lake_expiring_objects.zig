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
const Progress = struct { version: u16 = 2, cursor: ?[]const u8 = null, after_key: ?[]const u8 = null, retained: bool = false };
pub fn collect(a: A, store: *Store, table: u64, kind: []const u8, context: Context) !void {
    try context.ensureActive();
    const gc = @import("lake_query_gc.zig");
    const turn = @import("lake_cleanup_budget.zig").turnContext(context);
    var budget: gc.Budget = .{ .deletes = 128, .reads = 32 * 1024 * 1024, .work = 128, .deadline_ns = turn.deadline_ns.?, .cancellation = @import("lake_cleanup_budget.zig").contextToken(&context) };
    _ = gc.collectRecords(a, store, table, kind, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, &budget) catch |err| {
        try context.ensureActive();
        if (err == error.QueryGcBudgetExhausted or err == error.DeadlineExceeded or err == error.Canceled or err == error.Cancelled) return;
        return err;
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
