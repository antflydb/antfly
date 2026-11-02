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

//! AFAT authenticates the whole envelope. No routing authority comes from
//! unsigned headers. Decode with the admitted worker's bounded allocator.
const std = @import("std");
const rpc = @import("retained_read_rpc.zig");

pub const Envelope = struct {
    version: u16,
    request: []const u8,
    route_fence: []const u8,
};

fn validate(value: Envelope) !void {
    if (value.version != 1 or value.request.len == 0 or value.route_fence.len == 0) return error.InvalidRetainedReadEnvelope;
}

pub fn encode(alloc: std.mem.Allocator, request: []const u8, route_fence: []const u8) ![]u8 {
    const value: Envelope = .{ .version = 1, .request = request, .route_fence = route_fence };
    try validate(value);
    if (request.len > rpc.max_request_bytes or route_fence.len > rpc.max_request_bytes) return error.InvalidRetainedReadEnvelope;
    // Bound writer capacity before allocating: escaped JSON can be much larger
    // than either input. The returned bytes have no allocator callbacks.
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = alloc, .limit = rpc.max_request_bytes };
    const bytes = std.json.Stringify.valueAlloc(budget.allocator(), value, .{}) catch |err| return if (budget.exhausted) error.InvalidRetainedReadEnvelope else err;
    errdefer alloc.free(bytes);
    if (bytes.len > rpc.max_request_bytes) return error.InvalidRetainedReadEnvelope;
    return bytes;
}

pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Envelope) {
    if (bytes.len > rpc.max_request_bytes) return error.InvalidRetainedReadEnvelope;
    const parsed = std.json.parseFromSlice(Envelope, alloc, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRetainedReadEnvelope;
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

/// Classification grants transport/control capacity only. Full authentication,
/// worker admission and route validation still precede resource access.
pub fn isControlBody(bytes: []const u8) bool {
    if (bytes.len > 16384) return false;
    var storage: [32768]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var parsed = decode(fixed.allocator(), bytes) catch return false;
    defer parsed.deinit();
    return rpc.isControlBody(parsed.value.request);
}

test "retained read RPC signed envelope binds routing and classifies only bounded cleanup" {
    const a = std.testing.allocator;
    const close = "{\"operation\":\"close\",\"schema_version\":5,\"token\":{\"incarnation\":1,\"slot\":0,\"sequence\":1}}";
    const bytes = try encode(a, close, "{\"catalog_revision\":4}");
    defer a.free(bytes);
    var parsed = try decode(a, bytes);
    defer parsed.deinit();
    try std.testing.expectEqualStrings(close, parsed.value.request);
    try std.testing.expectEqualStrings("{\"catalog_revision\":4}", parsed.value.route_fence);
    try std.testing.expect(isControlBody(bytes));
    const read = try encode(a, "{\"operation\":\"capture\",\"schema_version\":5}", "{}");
    defer a.free(read);
    try std.testing.expect(!isControlBody(read));
    try std.testing.expectError(error.InvalidRetainedReadEnvelope, encode(a, "{}", ""));
    for ([_][]const u8{ "{}", "{\"version\":2,\"request\":\"{}\",\"route_fence\":\"{}\"}", "{\"version\":1,\"request\":\"{}\"}", "{\"version\":1,\"request\":\"{}\",\"route_fence\":\"{}\",\"extra\":true}", "{\"version\":1,\"version\":1,\"request\":\"{}\",\"route_fence\":\"{}\"}" }) |invalid| {
        try std.testing.expectError(error.InvalidRetainedReadEnvelope, decode(a, invalid));
        try std.testing.expect(!isControlBody(invalid));
    }
    const escaped = try a.alloc(u8, 1 << 20);
    defer a.free(escaped);
    @memset(escaped, 0);
    var outer: @import("antfly_local_sources").sql_memory_budget = .{ .backing = a, .limit = rpc.max_request_bytes };
    try std.testing.expectError(error.InvalidRetainedReadEnvelope, encode(outer.allocator(), escaped, "{}"));
    try std.testing.expect(!outer.exhausted);
    try std.testing.expect(outer.peak <= rpc.max_request_bytes);
    try std.testing.expectEqual(@as(usize, 0), outer.live);
}
