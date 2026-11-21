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

//! Authenticated terminal recovery transport; accepted positions and exact
//! local outcomes remain separate from source ACK or global drainage.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const legacy = local.storage_source_legacy_terminal_ordered;
const scope = local.storage_source_scope;
const Owner = local.storage_transaction_completion.RoutedOwner;
const A = std.mem.Allocator;
pub const max_request_bytes = 16384;
pub const max_reply_bytes = 12288;
pub const Request = struct {
    version: u8,
    operation: enum { submit, recover },
    request_id: [16]u8,
    owner: Owner,
    command: legacy.policy.LegacyTerminalCommand,
    receipt: ?scope.Cut,
    minimum_applied_index: u64,
    pub fn validate(self: Request) !void {
        try self.command.validate();
        if (self.version != 1 or std.mem.allEqual(u8, &self.request_id, 0) or !std.meta.eql(self.owner, self.command.owner)) return error.InvalidSourceAdmission;
        switch (self.operation) {
            .submit => if (self.receipt != null or self.minimum_applied_index != 0) return error.InvalidSourceAdmission,
            .recover => {
                const receipt = self.receipt orelse return error.InvalidSourceAdmission;
                if (self.minimum_applied_index < receipt.index) return error.InvalidSourceAdmission;
                try self.query(receipt).validate();
            },
        }
    }
    pub fn query(self: Request, receipt: scope.Cut) legacy.Query {
        return .{ .version = 1, .owner = self.owner, .command = self.command, .entry = receipt, .minimum_applied_index = @max(self.minimum_applied_index, receipt.index) };
    }
    pub fn digest(self: Request, a: A) !scope.Digest {
        const bytes = try encodeRequest(a, self);
        defer a.free(bytes);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:legacy-terminal-http:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: scope.Digest,
    receipt: ?scope.Cut = null,
    outcome: ?legacy.Observation = null,
    pub fn confirmed(self: Reply) bool {
        return self.outcome != null and self.outcome.?.outcome != .superseded;
    }
    pub fn validate(self: Reply, a: A, request: Request, expected: scope.Digest) !void {
        try request.validate();
        if (self.version != 1 or !std.meta.eql(self.request_digest, expected)) return error.SourceAdmissionCorrupt;
        if (request.operation == .recover and !std.meta.eql(self.receipt, request.receipt)) return error.SourceAdmissionCorrupt;
        if (self.receipt) |receipt| {
            if (receipt.term == 0 or receipt.index == 0) return error.SourceAdmissionCorrupt;
            if (self.outcome) |observed| try observed.validate(a, request.query(receipt));
        } else if (self.outcome != null) return error.SourceAdmissionCorrupt;
    }
};
pub fn unknown(a: A, request: Request) !Reply {
    return .{ .request_digest = try request.digest(a), .receipt = request.receipt };
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
    for ([_][]const u8{ "version", "operation", "request_id", "owner", "command", "receipt", "minimum_applied_index" }) |field| if (!raw.value.object.contains(field)) return error.InvalidSourceAdmission;
    const owner = raw.value.object.get("owner").?;
    if (owner != .object) return error.InvalidSourceAdmission;
    for ([_][]const u8{ "version", "metadata_group_id", "metadata_incarnation", "namespace", "table_binding", "root_generation", "group_id", "topology_epoch" }) |field| if (!owner.object.contains(field)) return error.InvalidSourceAdmission;
    const command_bytes = try std.json.Stringify.valueAlloc(a, raw.value.object.get("command").?, .{});
    defer a.free(command_bytes);
    _ = try legacy.policy.decodeLegacyTerminalCommand(a, command_bytes);
    var parsed = std.json.parseFromValue(Request, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeReply(a: A, request: Request, reply: Reply) ![]u8 {
    try reply.validate(a, request, try request.digest(a));
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
    var parsed = std.json.parseFromValue(Reply, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const observed = raw.value.object.get("outcome").?;
    if (observed != .null) {
        const receipt = parsed.value.receipt orelse return error.SourceAdmissionCorrupt;
        const observation_bytes = try std.json.Stringify.valueAlloc(a, observed, .{});
        defer a.free(observation_bytes);
        _ = try legacy.decodeObservation(a, observation_bytes, request.query(receipt));
    }
    try parsed.value.validate(a, request, try request.digest(a));
    return parsed.value;
}
pub const test_support = if (@import("builtin").is_test) struct {
    pub fn fixture() Request {
        const owner = @import("source_producer_control_http_wire.zig").test_support.fixture().owner;
        return .{ .version = 1, .operation = .submit, .request_id = @splat(9), .owner = owner, .command = .{ .version = 1, .owner = owner, .proof = .{ .root = scope.Basis.fromOwner(owner) catch unreachable, .transaction_id = @splat(4), .record_digest = @splat(5) } }, .receipt = null, .minimum_applied_index = 0 };
    }
    pub fn knownReply(a: A, input: Request) !Reply {
        const entry: scope.Cut = input.receipt orelse .{ .term = 1, .index = 20 };
        return .{ .request_digest = try input.digest(a), .receipt = entry, .outcome = .{ .version = 1, .owner = input.owner, .command_digest = try legacy.fingerprint(a, input.command), .entry = entry, .cut = .{ .term = entry.term, .index = @max(entry.index, input.minimum_applied_index) }, .outcome = .{ .applied = .{ .kind = .aborted, .timestamp = 1001 } } } };
    }
} else struct {};

test "source legacy terminal wire separates submission recovery and ambiguous receipts" {
    const a = std.testing.allocator;
    const request = test_support.fixture();
    const bytes = try encodeRequest(a, request);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, bytes));
    var wrong = request;
    wrong.receipt = .{ .term = 1, .index = 20 };
    try std.testing.expectError(error.InvalidSourceAdmission, encodeRequest(a, wrong));
    wrong.operation = .recover;
    try std.testing.expectError(error.InvalidSourceAdmission, encodeRequest(a, wrong));
    wrong.minimum_applied_index = 20;
    const recovered_request = try encodeRequest(a, wrong);
    defer a.free(recovered_request);
    try std.testing.expectEqualDeep(wrong, try decodeRequest(a, recovered_request));
    const known = try test_support.knownReply(a, request);
    const reply = try encodeReply(a, request, known);
    defer a.free(reply);
    try std.testing.expect((try decodeReply(a, request, reply)).confirmed());
    var forged = known;
    forged.outcome.?.entry.index += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeReply(a, request, forged));
    wrong = request;
    wrong.request_id[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, wrong, reply));
    var recover = request;
    recover.operation = .recover;
    recover.receipt = known.receipt;
    recover.minimum_applied_index = known.receipt.?.index;
    const uncertain = try unknown(a, recover);
    try std.testing.expectEqualDeep(recover.receipt, uncertain.receipt);
    try std.testing.expect(uncertain.outcome == null);
    var bad_reply = uncertain;
    bad_reply.receipt = null;
    try std.testing.expectError(error.SourceAdmissionCorrupt, encodeReply(a, recover, bad_reply));
    const extra = try std.fmt.allocPrint(a, "{s},\"drained\":true}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(extra);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, extra));
    var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer raw.deinit();
    _ = raw.value.object.getPtr("command").?.object.getPtr("owner").?.object.swapRemove("root_generation");
    const stripped = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    defer a.free(stripped);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, stripped));
}
