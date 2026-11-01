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

//! Complete storage operations through opaque owner capabilities. No DB, index,
//! backend or native DB layout is imported by this consumer.
const std = @import("std");
const client = @import("storage/kernel_owner_client.zig");

fn open(path: []const u8, context: ?*anyopaque) !client.Owner {
    return client.Owner.open(.{
        .context = context,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = 1,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 1,
        .identity_range_id = 1,
        .schema_json = .fromSlice(
            \\{"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"}}}}}}
        ),
        .indexes_json = .fromSlice("{\"body_text\":{\"type\":\"full_text\"}}"),
    });
}

const initial_batch =
    \\{"inserts":{"a":{"body":"original"},"z":{"body":"original"}},"sync_level":"full_index"}
;

// Mutable source and response lifetimes, text indexing and retained generations
// all belong to the compiled provider. The test only owns encoded values.
test "opaque query owner retains text generation through later writes and profiles" {
    const a = std.testing.allocator;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const pin_path = try std.fmt.allocPrint(a, "{s}.query-pins", .{path});
    defer a.free(pin_path);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, pin_path) catch {};
    var owner = try open(path, null);
    defer owner.deinit();
    var batch = try owner.batchJson("docs", initial_batch);
    batch.deinit();
    const id: [64]u8 = @splat('e');
    const expires = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 300_000;
    const create = try std.json.Stringify.valueAlloc(a, .{
        ._native_cut = .{ .id = id[0..], .table_id = @as(u64, 7), .expires_ms = expires, .create = true },
        .full_text_index = "body_text",
        .full_text_search = .{ .match = "original", .field = "body" },
        .order_by = .{.{ .field = "_id" }},
        .limit = @as(u32, 1),
    }, .{});
    defer a.free(create);
    var first = try owner.queryJsonWithOptions("docs", create, .{ .execution = .{ .enabled = 1, .raw_search_result = 1 } });
    defer first.deinit();
    var updated = try owner.batchJson("docs",
        \\{"inserts":{"a":{"body":"changed"},"z":{"body":"changed"},"fresh":{"body":"new"}},"sync_level":"full_index"}
    );
    updated.deinit();
    for ([_]bool{ false, true }) |profile| {
        const request = try std.json.Stringify.valueAlloc(a, .{
            ._native_cut = .{ .id = id[0..], .table_id = @as(u64, 7), .expires_ms = expires },
            .full_text_index = "body_text",
            .full_text_search = .{ .match = "original", .field = "body" },
            .order_by = .{.{ .field = "_id" }},
            .search_after = .{"a"},
            .limit = @as(u32, 128),
            .profile = profile,
        }, .{});
        defer a.free(request);
        var response = try owner.queryJsonWithOptions("docs", request, .{ .execution = .{ .enabled = 1, .raw_search_result = 1 } });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(std.json.Value, a, response.bytes(), .{});
        defer parsed.deinit();
        const hits = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("z", hits[0].object.get("_id").?.string);
    }
}

test "opaque query owner preserves cancellation deadlines and subsequent queries" {
    const a = std.testing.allocator;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    var owner = try open(path, null);
    defer owner.deinit();
    var batch = try owner.batchJson("docs", initial_batch);
    batch.deinit();
    const query = "{\"full_text_index\":\"body_text\",\"full_text_search\":{\"match\":\"original\",\"field\":\"body\"},\"limit\":10}";
    try std.testing.expectError(error.Timeout, owner.queryJsonWithOptions("docs", query, .{ .execution_deadline_ns = 1 }));
    const Cancellation = struct {
        checks: usize = 0,
        fn requested(context: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.checks += 1;
            return @intFromBool(self.checks >= 2);
        }
    };
    var cancellation: Cancellation = .{};
    try std.testing.expectError(error.Cancelled, owner.queryJsonWithOptions("docs", query, .{ .cancellation_ctx = &cancellation, .cancellation_fn = Cancellation.requested }));
    try std.testing.expect(cancellation.checks >= 2);
    var response = try owner.queryJson("docs", query);
    defer response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, response.bytes(), "original") != null);
}

// The donor is closed before opening a different carrier DB. This exercises the
// repository-backed generation transfer from the former physical test root.
test "opaque query owner restores a remote retained text generation and continuation profiles" {
    const a = std.testing.allocator;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const base = try directory.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(base);
    var context: client.Context = .{};
    const setup = try std.json.Stringify.valueAlloc(a, .{ .config_json = "{}", .deployment = "standalone", .base_dir = base }, .{});
    defer a.free(setup);
    try context.configureNativeQueries(setup);
    defer context.deinit();
    const id: [64]u8 = @splat('f');
    const expires = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 300_000;
    const donor_path = try std.fmt.allocPrint(a, "{s}/donor", .{base});
    defer a.free(donor_path);
    {
        var donor = try open(donor_path, context.handle);
        defer donor.deinit();
        var batch = try donor.batchJson("docs", initial_batch);
        batch.deinit();
        const create = try std.json.Stringify.valueAlloc(a, .{
            ._native_cut = .{ .id = id[0..], .table_id = @as(u64, 7), .expires_ms = expires, .create = true },
            .full_text_index = "body_text",
            .full_text_search = .{ .match = "original", .field = "body" },
            .limit = @as(u32, 128),
        }, .{});
        defer a.free(create);
        var response = try donor.queryJsonWithOptions("docs", create, .{ .execution = .{ .enabled = 1, .raw_search_result = 1 } });
        response.deinit();
    }
    // Force repository restoration instead of accidentally reusing the donor's
    // local retained snapshot or document store.
    try std.Io.Dir.cwd().deleteTree(std.testing.io, donor_path);
    const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{donor_path});
    defer a.free(pins);
    try std.Io.Dir.cwd().deleteTree(std.testing.io, pins);
    const carrier_path = try std.fmt.allocPrint(a, "{s}/carrier", .{base});
    defer a.free(carrier_path);
    var carrier = try open(carrier_path, context.handle);
    defer carrier.deinit();
    var fresh = try carrier.batchJson("docs", "{\"inserts\":{\"fresh\":{\"body\":\"new generation\"}},\"sync_level\":\"full_index\"}");
    fresh.deinit();
    const token = "native2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    for ([_]bool{ false, true }) |profile| {
        const bytes = try std.json.Stringify.valueAlloc(a, .{
            ._native_cut = .{ .id = id[0..], .table_id = @as(u64, 7), .expires_ms = expires },
            .remote_snapshot = token,
            .full_text_index = "body_text",
            .full_text_search = .{ .match = "original", .field = "body" },
            .order_by = .{.{ .field = "_id" }},
            .search_after = .{"a"},
            .limit = @as(u32, 128),
            .profile = profile,
        }, .{});
        defer a.free(bytes);
        var response = try carrier.queryJsonWithOptions("docs", bytes, .{ .execution = .{ .enabled = 1, .raw_search_result = 1 } });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(std.json.Value, a, response.bytes(), .{});
        defer parsed.deinit();
        const result = parsed.value.object.get("responses").?.array.items[0];
        try std.testing.expectEqualStrings(token, result.object.get("remote_snapshot").?.string);
        const hits = result.object.get("hits").?.object.get("hits").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("z", hits[0].object.get("_id").?.string);
    }
}
