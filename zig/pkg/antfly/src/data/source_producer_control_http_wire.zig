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

//! One cumulative producer-intake closure submission. An accepted receipt is
//! not commitment; a qualified observation is state, never legacy drainage.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const policy = local.storage_source_producer_policy;
const Owner = local.storage_transaction_completion.RoutedOwner;
const scope = local.storage_source_scope;
const A = std.mem.Allocator;
pub const max_request_bytes = 4096;
pub const max_reply_bytes = 12288;
pub const Request = struct {
    version: u8,
    request_id: [16]u8,
    owner: Owner,
    pub fn validate(self: Request) !void {
        try self.owner.validate();
        if (self.version != 1 or std.mem.allEqual(u8, &self.request_id, 0)) return error.InvalidSourceAdmission;
    }
    pub fn digest(self: Request, a: A) !scope.Digest {
        const bytes = try encodeRequest(a, self);
        defer a.free(bytes);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-producer-control:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: scope.Digest,
    /// Accepted proposal identity; it may not have committed.
    receipt: ?scope.Cut = null,
    /// Qualified current state, not the original proposal's outcome.
    outcome: ?policy.Observation = null,
    pub fn closed(self: Reply) bool {
        return self.outcome != null and self.outcome.?.closure != null;
    }
    pub fn validate(self: Reply, request: Request, expected: scope.Digest) !void {
        try request.validate();
        if (self.version != 1 or !std.meta.eql(self.request_digest, expected)) return error.SourceAdmissionCorrupt;
        if (self.receipt) |receipt| {
            if (receipt.term == 0 or receipt.index == 0) return error.SourceAdmissionCorrupt;
            if (self.outcome) |observed| try observed.validate(.{ .version = 1, .owner = request.owner, .minimum_applied_index = receipt.index });
        } else if (self.outcome != null) return error.SourceAdmissionCorrupt;
    }
};
pub fn unknown(a: A, request: Request) !Reply {
    return .{ .request_digest = try request.digest(a) };
}
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
    for ([_][]const u8{ "version", "request_id", "owner" }) |field| if (!raw.value.object.contains(field)) return error.InvalidSourceAdmission;
    const owner = raw.value.object.get("owner").?;
    if (owner != .object) return error.InvalidSourceAdmission;
    for ([_][]const u8{ "metadata_group_id", "metadata_incarnation", "namespace", "table_binding", "root_generation", "group_id", "topology_epoch" }) |field| if (!owner.object.contains(field)) return error.InvalidSourceAdmission;
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
    if (raw.value != .object) return error.SourceAdmissionCorrupt;
    for ([_][]const u8{ "version", "request_digest", "receipt", "outcome" }) |field| if (!raw.value.object.contains(field)) return error.SourceAdmissionCorrupt;
    const observed = raw.value.object.get("outcome").?;
    if (observed != .null) {
        if (observed != .object) return error.SourceAdmissionCorrupt;
        for ([_][]const u8{ "version", "owner", "cut", "closure" }) |field| if (!observed.object.contains(field)) return error.SourceAdmissionCorrupt;
    }
    var parsed = std.json.parseFromValue(Reply, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(request, try request.digest(a));
    return parsed.value;
}

pub const test_support = if (@import("builtin").is_test) struct {
    pub fn fixture() Request {
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], 7, .big);
        std.mem.writeInt(u64, namespace[8..16], 17, .big);
        std.mem.writeInt(u64, namespace[16..24], 19, .big);
        return .{ .version = 1, .request_id = @splat(9), .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    }
    pub fn closedReply(a: A, request: Request) !Reply {
        return .{ .request_digest = try request.digest(a), .receipt = .{ .term = 1, .index = 20 }, .outcome = .{ .version = 1, .owner = request.owner, .cut = .{ .term = 1, .index = 20 }, .closure = .{ .root = try scope.Basis.fromOwner(request.owner), .first_closed_at = .{ .term = 1, .index = 10 } } } };
    }
} else struct {};

test "source producer control wire retains accepted identity without inventing outcomes" {
    const a = std.testing.allocator;
    const request = test_support.fixture();
    const bytes = try encodeRequest(a, request);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, bytes));
    var bad = request;
    bad.request_id = @splat(0);
    try std.testing.expectError(error.InvalidSourceAdmission, encodeRequest(a, bad));
    var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer raw.deinit();
    _ = raw.value.object.getPtr("owner").?.object.swapRemove("root_generation");
    const stripped = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    defer a.free(stripped);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, stripped));
    const extra = try std.fmt.allocPrint(a, "{s},\"drained\":true}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(extra);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, extra));
    const reply = try test_support.closedReply(a, request);
    const encoded = try encodeReply(a, request, reply);
    defer a.free(encoded);
    try std.testing.expect((try decodeReply(a, request, encoded)).closed());
    bad = request;
    bad.request_id[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, bad, encoded));
    var forged = reply;
    forged.outcome.?.cut.index = 19;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeReply(a, request, forged));
    forged = reply;
    forged.receipt = null;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeReply(a, request, forged));
    forged = reply;
    forged.outcome = null;
    const lost = try encodeReply(a, request, forged);
    defer a.free(lost);
    const pending = try decodeReply(a, request, lost);
    try std.testing.expect(!pending.closed());
    try std.testing.expectEqualDeep(reply.receipt, pending.receipt);
}
