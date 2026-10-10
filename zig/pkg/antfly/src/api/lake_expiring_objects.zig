// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded, restartable cleanup of expiring job/memo records. Conditional
//! deletes cannot race a renewed/reclaimed job. Provider cursors are durable;
//! active early keys cannot starve expired records at the end of a prefix.
const std = @import("std");
const Store = @import("lake_index_store.zig").Store;
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const Progress = struct { version: u16 = 1, cursor: ?[]const u8 = null };
pub fn collect(a: A, store: *Store, table: u64, kind: []const u8, context: Context) !void {
    try context.ensureActive();
    var client = store.opened.client;
    const token: ?@import("objectstore").CancellationToken = if (context.cancellation) |cancel| .{ .ptr = cancel.ptr, .is_cancelled_fn = cancel.is_cancelled_fn } else null;
    const prefix = try std.fmt.allocPrint(a, "{s}{s}{s}/{d}/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", kind, table });
    defer a.free(prefix);
    const checkpoint = try std.fmt.allocPrint(a, "{s}{s}expiring-gc/{d}/{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, kind });
    defer a.free(checkpoint);
    var previous = client.getObject(store.opened.bucket, checkpoint, .{ .max_response_bytes = 16384, .cancellation = token }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (previous) |*value| value.deinit(client.allocator);
    var parsed: ?std.json.Parsed(Progress) = if (previous) |value| try std.json.parseFromSlice(Progress, a, value.body, .{}) else null;
    defer if (parsed) |*value| value.deinit();
    var page = try client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 128, .continuation_token = if (parsed) |value| value.value.cursor else null, .cancellation = token });
    defer page.deinit(client.allocator);
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    for (page.entries) |object| {
        try context.ensureActive();
        var value = client.getObject(store.opened.bucket, object.key, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = token }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => continue,
            else => return err,
        };
        defer value.deinit(client.allocator);
        var document = try std.json.parseFromSlice(std.json.Value, a, value.body, .{});
        defer document.deinit();
        if (document.value != .object) return error.InvalidExpiringLakeObject;
        const expires = document.value.object.get("expires_ms") orelse continue;
        if (expires != .integer or expires.integer <= 0) return error.InvalidExpiringLakeObject;
        if (@as(u64, @intCast(expires.integer)) +| 30_000 >= now) continue;
        if (document.value.object.get("lease_until_ms")) |lease| if (lease == .integer and lease.integer > 0 and @as(u64, @intCast(lease.integer)) > now) continue;
        client.deleteObject(store.opened.bucket, object.key, .{ .if_match_etag = value.metadata.etag orelse return error.MissingObjectEtag, .cancellation = token }) catch |err| switch (err) {
            error.PreconditionFailed, error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
            else => return err,
        };
    }
    const bytes = try std.json.Stringify.valueAlloc(a, Progress{ .cursor = page.next_continuation_token }, .{});
    defer a.free(bytes);
    var saved = client.putObject(store.opened.bucket, checkpoint, bytes, .{ .if_none_match = previous == null, .if_match_etag = if (previous) |value| value.metadata.etag orelse return error.MissingObjectEtag else null, .cancellation = token }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => return,
        else => return err,
    };
    saved.deinit(client.allocator);
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
    const context: Context = .{ .io = std.testing.io };
    try collect(a, &store, 7, "recent-search", context);
    try collect(a, &store, 7, "recent-search", context);
    const prefix = try std.fmt.allocPrint(a, "{s}{s}recent-search/7/", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/" });
    defer a.free(prefix);
    var remaining = try client.listObjects(store.opened.bucket, .{ .prefix = prefix, .max_keys = 256 });
    defer remaining.deinit(client.allocator);
    try std.testing.expectEqual(@as(usize, 129), remaining.entries.len);
    var active = try client.getObject(store.opened.bucket, leased, .{});
    defer active.deinit(client.allocator);
}
