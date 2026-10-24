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

//! AFAT binds this entire envelope. Routing headers must not supply authority
//! outside the signed body. Decode only with the admitted worker's allocator.
const std = @import("std");

pub const Envelope = struct {
    version: u16,
    query: []const u8,
    prepared_routing: ?[]const u8 = null,
    route_fence: ?[]const u8 = null,
};

pub fn encode(alloc: std.mem.Allocator, query: []const u8, prepared_routing: ?[]const u8, route_fence: ?[]const u8) ![]u8 {
    const envelope: Envelope = .{ .version = 1, .query = query, .prepared_routing = prepared_routing, .route_fence = route_fence };
    try validate(envelope);
    return std.json.Stringify.valueAlloc(alloc, envelope, .{});
}

pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Envelope) {
    const parsed = std.json.parseFromSlice(Envelope, alloc, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidQueryRequest;
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

fn validate(envelope: Envelope) !void {
    if (envelope.version != 1 or envelope.query.len == 0 or (envelope.prepared_routing != null and envelope.route_fence == null)) return error.InvalidQueryRequest;
}

test "internal query owner signed envelope preserves routing and rejects ambiguous versions" {
    const a = std.testing.allocator;
    const query = "{\"text\":\"quoted \\\" text\"}";
    const bytes = try encode(a, query, "{\"index_name\":\"text_v2\"}", "{\"catalog_revision\":4}");
    defer a.free(bytes);
    var parsed = try decode(a, bytes);
    defer parsed.deinit();
    try std.testing.expectEqualStrings(query, parsed.value.query);
    try std.testing.expectEqualStrings("{\"index_name\":\"text_v2\"}", parsed.value.prepared_routing.?);
    try std.testing.expectEqualStrings("{\"catalog_revision\":4}", parsed.value.route_fence.?);
    try std.testing.expectError(error.InvalidQueryRequest, encode(a, "{}", "prepared without fence", null));
    try std.testing.expectError(error.InvalidQueryRequest, encode(a, "", null, null));
    for ([_][]const u8{ "{\"version\":1,\"query\":\"{}\",\"prepared_routing\":\"unfenced\"}", "{}", "{\"version\":2,\"query\":\"{}\"}", "{\"query\":\"{}\"}", "{\"version\":1,\"query\":\"\"}", "{\"version\":1,\"query\":\"{}\",\"extra\":true}", "{\"version\":1,\"version\":1,\"query\":\"{}\"}" }) |invalid| try std.testing.expectError(error.InvalidQueryRequest, decode(a, invalid));
}
