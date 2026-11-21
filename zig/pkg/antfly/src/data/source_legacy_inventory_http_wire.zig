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

//! Correlated bounded read transport for private legacy work discovery.
//! Empty pages are not a coherent drainage or source activation certificate.
const std = @import("std");
pub const inventory = @import("antfly_local_sources").storage_source_legacy_inventory;
const scope = @import("antfly_local_sources").storage_source_scope;
const A = std.mem.Allocator;
pub const max_request_bytes = 8192;
pub const max_reply_bytes = inventory.max_observation_bytes + 8192;
pub const Request = struct {
    version: u8,
    request_id: [16]u8,
    query: inventory.Query,
    pub fn validate(self: Request) !void {
        try self.query.validate();
        if (self.version != 1 or std.mem.allEqual(u8, &self.request_id, 0)) return error.InvalidSourceAdmission;
    }
    pub fn digest(self: Request, a: A) !scope.Digest {
        const bytes = try encodeRequest(a, self);
        defer a.free(bytes);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:legacy-inventory-http:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8,
    request_digest: scope.Digest,
    observation: inventory.Observation,
    pub fn deinit(self: *Reply, a: A) void {
        self.observation.deinit(a);
        self.* = undefined;
    }
    pub fn validate(self: Reply, request: Request, expected: scope.Digest) !void {
        try request.validate();
        if (self.version != 1 or !std.meta.eql(self.request_digest, expected)) return error.SourceAdmissionCorrupt;
        try self.observation.validate(request.query);
    }
};
pub fn encodeRequest(a: A, request: Request) ![]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_request_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeRequest(a: A, bytes: []const u8) !Request {
    if (bytes.len == 0 or bytes.len > max_request_bytes) return error.InvalidSourceAdmission;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer raw.deinit();
    if (raw.value != .object) return error.InvalidSourceAdmission;
    const query_bytes = try std.json.Stringify.valueAlloc(a, raw.value.object.get("query") orelse return error.InvalidSourceAdmission, .{});
    defer a.free(query_bytes);
    _ = try inventory.decodeQuery(a, query_bytes);
    var parsed = std.json.parseFromValue(Request, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeReply(a: A, request: Request, reply: Reply) ![]u8 {
    try reply.validate(request, try request.digest(a));
    const bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeReply(a: A, request: Request, bytes: []const u8) !Reply {
    try request.validate();
    if (bytes.len == 0 or bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer raw.deinit();
    if (raw.value != .object or raw.value.object.count() != 3) return error.SourceAdmissionCorrupt;
    const version_value = raw.value.object.get("version") orelse return error.SourceAdmissionCorrupt;
    const digest_value = raw.value.object.get("request_digest") orelse return error.SourceAdmissionCorrupt;
    const observation_value = raw.value.object.get("observation") orelse return error.SourceAdmissionCorrupt;
    var version = std.json.parseFromValue(u8, a, version_value, .{}) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer version.deinit();
    var digest = std.json.parseFromValue(scope.Digest, a, digest_value, .{}) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer digest.deinit();
    const expected = try request.digest(a);
    if (version.value != 1 or !std.meta.eql(digest.value, expected)) return error.SourceAdmissionCorrupt;
    const observation_bytes = try std.json.Stringify.valueAlloc(a, observation_value, .{});
    defer a.free(observation_bytes);
    var reply: Reply = .{ .version = version.value, .request_digest = digest.value, .observation = try inventory.decodeObservation(a, observation_bytes, request.query) };
    errdefer reply.deinit(a);
    try reply.validate(request, expected);
    return reply;
}
pub const test_support = if (@import("builtin").is_test) struct {
    pub fn fixture() Request {
        const owner = @import("source_producer_control_http_wire.zig").test_support.fixture().owner;
        return .{ .version = 1, .request_id = @splat(8), .query = .{ .version = 1, .owner = owner, .after = null, .max_records = 2, .minimum_applied_index = 20 } };
    }
    pub fn reply(a: A, input: Request) !Reply {
        const items = try a.alloc(inventory.Item, 1);
        errdefer a.free(items);
        items[0] = .{ .transaction_id = @splat(50), .record_digest = @splat(51), .status = .pending, .prepared = true, .prepared_known = true, .coordinator = true, .coordinator_known = true, .intents_resolved = false, .intents_resolved_known = true };
        return .{ .version = 1, .request_digest = try input.digest(a), .observation = .{ .version = 1, .owner = input.query.owner, .cut = .{ .term = 1, .index = input.query.minimum_applied_index }, .page = .{ .closure = .{ .root = try scope.Basis.fromOwner(input.query.owner), .first_closed_at = .{ .term = 1, .index = 1 } }, .items = items, .scanned_records = 1, .next_after = items[0].transaction_id, .reached_end = true } } };
    }
} else struct {};

test "source legacy inventory wire binds nonce cursor record bound and complete native cut" {
    const a = std.testing.allocator;
    const input = test_support.fixture();
    const encoded = try encodeRequest(a, input);
    defer a.free(encoded);
    try std.testing.expectEqualDeep(input, try decodeRequest(a, encoded));
    var known = try test_support.reply(a, input);
    defer known.deinit(a);
    const bytes = try encodeReply(a, input, known);
    defer a.free(bytes);
    var decoded = try decodeReply(a, input, bytes);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(known, decoded);
    for (0..4) |n| {
        var wrong = input;
        switch (n) {
            0 => wrong.request_id[0] ^= 1,
            1 => wrong.query.after = @splat(1),
            2 => wrong.query.max_records += 1,
            3 => wrong.query.minimum_applied_index += 1,
            else => unreachable,
        }
        try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, wrong, bytes));
    }
    var raw = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer raw.deinit();
    _ = raw.value.object.getPtr("query").?.object.getPtr("owner").?.object.swapRemove("root_generation");
    const stripped = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    defer a.free(stripped);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, stripped));
}
