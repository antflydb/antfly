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
const domain = @import("../system_catalog/domain.zig");
const Cursor = struct {
    version: u8 = 1,
    database: []const u8,
    namespace: []const u8,
    prefix: ?[]const u8,
    revision: u64,
    legacy_membership: [32]u8,
    after_table_id: u64,
};

pub fn encode(alloc: std.mem.Allocator, request: domain.TableList, revision: u64, legacy_membership: [32]u8, after_table_id: u64) ![]u8 {
    const bytes = try std.json.Stringify.valueAlloc(alloc, Cursor{ .database = request.database, .namespace = request.namespace, .prefix = request.prefix, .revision = revision, .legacy_membership = legacy_membership, .after_table_id = after_table_id }, .{});
    defer alloc.free(bytes);
    const codec = std.base64.url_safe_no_pad.Encoder;
    const result = try alloc.alloc(u8, codec.calcSize(bytes.len));
    _ = codec.encode(result, bytes);
    return result;
}

/// The caller's arena owns cursor strings. Cursors select rows; they never
/// carry authorization, which is rechecked for every page.
pub fn apply(alloc: std.mem.Allocator, request: *domain.TableList, encoded: []const u8) !void {
    if (encoded.len > 4096) return error.InvalidCatalogName;
    const codec = std.base64.url_safe_no_pad.Decoder;
    const size = codec.calcSizeForSlice(encoded) catch return error.InvalidCatalogName;
    const bytes = try alloc.alloc(u8, size);
    codec.decode(bytes, encoded) catch return error.InvalidCatalogName;
    const cursor = std.json.parseFromSliceLeaky(Cursor, alloc, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogName,
    };
    if (cursor.version != 1 or !std.mem.eql(u8, request.database, cursor.database) or !std.mem.eql(u8, request.namespace, cursor.namespace) or !std.mem.eql(u8, request.prefix orelse "", cursor.prefix orelse "")) return error.InvalidCatalogName;
    if (cursor.after_table_id == 0) return error.InvalidCatalogName;
    request.after_table_id = cursor.after_table_id;
    request.revision = cursor.revision;
    request.legacy_membership = cursor.legacy_membership;
    if (request.limit == null) request.limit = 100;
}

test "system catalog cursor binds scope and uses opaque table identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const encoded = try encode(alloc, .{ .database = "analytics" }, 7, @splat(0), 42);
    var request: domain.TableList = .{ .database = "analytics" };
    try apply(alloc, &request, encoded);
    try std.testing.expectEqual(@as(u64, 42), request.after_table_id.?);
    try std.testing.expectEqual(@as(u64, 7), request.revision.?);
    var wrong: domain.TableList = .{};
    try std.testing.expectError(error.InvalidCatalogName, apply(alloc, &wrong, encoded));
    try std.testing.expectError(error.InvalidCatalogName, apply(alloc, &request, "!!!"));
}
