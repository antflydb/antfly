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

//! Optional internal routing metadata. Old peers ignore this header and route
//! normally. New peers require a matching catalog fence before consuming it;
//! the operation layer still validates that fence against local storage.
const std = @import("std");
const types = @import("../storage/db/types.zig");
const metadata = @import("../metadata/api.zig");
pub const header_name = "x-antfly-prepared-query-routing";
pub const Envelope = struct {
    version: u16 = 1,
    table_id: u64,
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    primary_text_index_name: ?[]const u8 = null,
};
pub fn encode(alloc: std.mem.Allocator, table: []const u8, req: types.SearchRequest) !?[]u8 {
    if (req.prepared_read_table_id == 0) return null;
    return try std.json.Stringify.valueAlloc(alloc, Envelope{ .table_id = req.prepared_read_table_id, .table_name = table, .index_name = req.index_name, .primary_text_index_name = req.primary_text_index_name }, .{});
}
pub fn apply(alloc: std.mem.Allocator, table: []const u8, encoded: ?[]const u8, fence_json: ?[]const u8, req: *types.SearchRequest) !bool {
    const bytes = encoded orelse return false;
    const fence_bytes = fence_json orelse return false;
    // Bound this header independently of request-body limits.
    if (bytes.len > 8192) return error.InvalidQueryRequest;
    var parsed = std.json.parseFromSlice(Envelope, alloc, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    const value = parsed.value;
    if (value.version != 1) return false;
    var fence = std.json.parseFromSlice(metadata.CatalogRouteFence, alloc, fence_bytes, .{}) catch return error.InvalidCatalogRouteFence;
    defer fence.deinit();
    try fence.value.validate();
    if (value.table_id == 0 or value.table_id != fence.value.table_id or !std.mem.eql(u8, table, value.table_name)) return error.InvalidCatalogRouteFence;
    const primary = if (value.primary_text_index_name) |name| try alloc.dupe(u8, name) else null;
    errdefer if (primary) |name| alloc.free(name);
    const index = if (req.index_name == null) if (value.index_name) |name| try alloc.dupe(u8, name) else null else null;
    if (req.primary_text_index_name) |name| alloc.free(name);
    req.primary_text_index_name = primary;
    if (index) |name| req.index_name = name;
    req.prepared_read_table_id = value.table_id;
    return true;
}

test "prepared query routing requires matching identity and preserves legacy fallback" {
    const alloc = std.testing.allocator;
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 4, .table_id = 7, .topology_epoch = 9, .route = .{ .group_id = 11, .range_id = 1, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } } };
    const fence_json = try std.json.Stringify.valueAlloc(alloc, fence, .{});
    defer alloc.free(fence_json);
    const encoded = (try encode(alloc, "table:7", .{ .prepared_read_table_id = 7, .index_name = "full_text_index_v2", .primary_text_index_name = "full_text_index_v2" })).?;
    defer alloc.free(encoded);
    var req: types.SearchRequest = .{};
    defer if (req.index_name) |name| alloc.free(name);
    defer if (req.primary_text_index_name) |name| alloc.free(name);
    try std.testing.expect(!try apply(alloc, "table:7", null, fence_json, &req));
    try std.testing.expect(!try apply(alloc, "table:7", encoded, null, &req));
    try std.testing.expectError(error.InvalidCatalogRouteFence, apply(alloc, "replacement", encoded, fence_json, &req));
    try std.testing.expectError(error.InvalidCatalogRouteFence, apply(alloc, "table:7", "{\"table_id\":8,\"table_name\":\"table:7\"}", fence_json, &req));
    try std.testing.expect(!try apply(alloc, "table:7", "{\"version\":2,\"table_id\":7,\"table_name\":\"table:7\"}", fence_json, &req));
    try std.testing.expect(try apply(alloc, "table:7", encoded, fence_json, &req));
    try std.testing.expectEqualStrings("full_text_index_v2", req.primary_text_index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v2", req.index_name.?);
    try std.testing.expectEqual(@as(u64, 7), req.prepared_read_table_id);
}

test "prepared query routing keeps a vector worker's selected retrieval index" {
    const alloc = std.testing.allocator;
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 4, .table_id = 7, .topology_epoch = 9, .route = .{ .group_id = 11, .range_id = 1, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } } };
    const fence_json = try std.json.Stringify.valueAlloc(alloc, fence, .{});
    defer alloc.free(fence_json);
    const encoded = (try encode(alloc, "docs", .{ .prepared_read_table_id = 7, .index_name = "text_v2", .primary_text_index_name = "text_v2" })).?;
    defer alloc.free(encoded);
    var req: types.SearchRequest = .{ .index_name = "dense" };
    defer if (req.primary_text_index_name) |name| alloc.free(name);
    try std.testing.expect(try apply(alloc, "docs", encoded, fence_json, &req));
    try std.testing.expectEqualStrings("dense", req.index_name.?);
    try std.testing.expectEqualStrings("text_v2", req.primary_text_index_name.?);
}
