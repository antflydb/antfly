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

//! Private lifecycle RPC contract. Submission accepts only an intent; recovery
//! carries an exact prior command and receipt. Unknown results never permit a
//! transport retry or inference of a row transaction's outcome.
const std = @import("std");
pub const ordered = @import("antfly_local_sources").storage_source_scope_ordered;
pub const control = ordered.control;
pub const max_request_bytes = 16 * 1024;
pub const max_reply_bytes = 24 * 1024;
const A = std.mem.Allocator;
pub const Request = struct {
    version: u8,
    /// Fresh correlation identity per RPC attempt; not a transaction outcome
    /// key or permission to retry an unknown mutation.
    request_id: [16]u8,
    operation: enum { submit, recover },
    intent: ?control.Intent = null,
    recovery: ?ordered.Recovery = null,
    pub fn validate(self: Request) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.request_id, 0)) return error.InvalidSourceAdmission;
        switch (self.operation) {
            .submit => {
                if (self.intent == null or self.recovery != null) return error.InvalidSourceAdmission;
                try self.intent.?.validate();
            },
            .recover => {
                if (self.intent != null or self.recovery == null) return error.InvalidSourceAdmission;
                try self.recovery.?.validate();
            },
        }
    }
    pub fn owner(self: Request) @FieldType(control.Intent, "owner") {
        return if (self.operation == .submit) self.intent.?.owner else self.recovery.?.command.intent.owner;
    }
    pub fn digest(self: Request, a: A) !control.scope.Digest {
        const bytes = try encodeRequest(a, self);
        defer a.free(bytes);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-scope-http-control:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: control.scope.Digest,
    command: ?control.Command = null,
    receipt: ?control.scope.Cut = null,
    outcome: ?ordered.Outcome = null,
    /// A known result can be rejected; inspect the outcome before acting.
    pub fn confirmed(self: Reply) bool {
        const command = self.command orelse return false;
        command.validate() catch return false;
        return self.version == 1 and self.receipt != null and self.receipt.?.term != 0 and self.receipt.?.index != 0 and self.outcome != null and self.outcome.? != .superseded;
    }
    pub fn validate(self: Reply, request: Request, expected: control.scope.Digest) !void {
        try request.validate();
        if (self.version != 1 or !std.meta.eql(self.request_digest, expected)) return error.SourceAdmissionCorrupt;
        if (self.command) |command| {
            command.validate() catch return error.SourceAdmissionCorrupt;
            const receipt = self.receipt orelse return error.SourceAdmissionCorrupt;
            if (receipt.term == 0 or receipt.index == 0) return error.SourceAdmissionCorrupt;
            if (request.operation == .submit and !std.meta.eql(command.intent, request.intent.?)) return error.SourceAdmissionCorrupt;
            if (self.outcome) |outcome| ordered.validateOutcome(command, receipt, outcome) catch return error.SourceAdmissionCorrupt;
        } else if (self.receipt != null or self.outcome != null) return error.SourceAdmissionCorrupt;
        if (request.operation == .recover) {
            if (self.command == null or self.receipt == null or !std.meta.eql(self.command.?, request.recovery.?.command) or !std.meta.eql(self.receipt.?, request.recovery.?.entry)) return error.SourceAdmissionCorrupt;
        }
    }
};
pub fn unknown(a: A, request: Request) !Reply {
    try request.validate();
    return .{ .request_digest = try request.digest(a), .command = if (request.recovery) |r| r.command else null, .receipt = if (request.recovery) |r| r.entry else null };
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
    var parsed = std.json.parseFromSlice(Request, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
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
    var parsed = std.json.parseFromSlice(Reply, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
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
        const owner: @FieldType(control.Intent, "owner") = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 };
        return .{ .version = 1, .request_id = @splat(9), .operation = .submit, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(3), .generation = 1, .binding = @splat(4), .checkpoint_owner = control.scope.Basis.fromOwner(owner) catch unreachable }, .minimum_metadata_index = 10, .action = .bind } };
    }
} else struct {};

test "source scope control wire separates metadata intent from exact recovery authority" {
    const a = std.testing.allocator;
    const request = test_support.fixture();
    const bytes = try encodeRequest(a, request);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, bytes));
    const command: control.Command = .{ .version = 1, .intent = request.intent.?, .metadata_cut = .{ .group_id = 1, .incarnation = request.owner().metadata_incarnation, .index = 11 } };
    const receipt: control.scope.Cut = .{ .term = 1, .index = 20 };
    var mixed = request;
    mixed.recovery = .{ .command = command, .entry = receipt, .minimum_applied_index = receipt.index };
    try std.testing.expectError(error.InvalidSourceAdmission, mixed.validate());
    const unconfirmed = try unknown(a, request);
    try std.testing.expect(!unconfirmed.confirmed());
    try unconfirmed.validate(request, try request.digest(a));
    const accepted: Reply = .{ .request_digest = try request.digest(a), .command = command, .receipt = receipt };
    try std.testing.expect(!accepted.confirmed());
    const reply_bytes = try encodeReply(a, request, accepted);
    defer a.free(reply_bytes);
    try std.testing.expectEqualDeep(accepted, try decodeReply(a, request, reply_bytes));
    var another_attempt = request;
    another_attempt.request_id[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, another_attempt, reply_bytes));
    another_attempt.request_id = @splat(0);
    try std.testing.expectError(error.InvalidSourceAdmission, another_attempt.validate());
    const recovery: Request = .{ .version = 1, .request_id = @splat(10), .operation = .recover, .recovery = .{ .command = command, .entry = receipt, .minimum_applied_index = receipt.index } };
    const unavailable = try unknown(a, recovery);
    try std.testing.expectEqualDeep(command, unavailable.command.?);
    try std.testing.expectEqualDeep(receipt, unavailable.receipt.?);
    var expired = unavailable;
    expired.outcome = .superseded;
    try std.testing.expect(!expired.confirmed());
    try expired.validate(recovery, try recovery.digest(a));
    var altered = expired;
    altered.command.?.metadata_cut.index += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, altered.validate(recovery, try recovery.digest(a)));
}

test "source scope control wire binds applied phase cut and correlation before confirmation" {
    const a = std.testing.allocator;
    const request = test_support.fixture();
    const command: control.Command = .{ .version = 1, .intent = request.intent.?, .metadata_cut = .{ .group_id = 1, .incarnation = request.owner().metadata_incarnation, .index = 11 } };
    const cut: control.scope.Cut = .{ .term = 1, .index = 20 };
    var reply: Reply = .{ .request_digest = try request.digest(a), .command = command, .receipt = cut, .outcome = .{ .applied = .{ .version = 1, .owner = request.owner(), .domain = command.intent.spec.domain(), .cut = cut, .header = .{ .root = try control.scope.Basis.fromOwner(request.owner()), .strict = true, .metadata_floor = 11, .domains = 1 }, .record = .{ .spec = command.intent.spec, .bound_metadata_index = 11 } } } };
    const bytes = try encodeReply(a, request, reply);
    defer a.free(bytes);
    try std.testing.expect((try decodeReply(a, request, bytes)).confirmed());
    reply.command.?.version = 2;
    const ordered_bytes = try encodeReply(a, request, reply);
    defer a.free(ordered_bytes);
    const ordered_reply = try decodeReply(a, request, ordered_bytes);
    try std.testing.expect(ordered_reply.confirmed());
    try std.testing.expectEqual(@as(u8, 2), ordered_reply.command.?.version);
    reply.outcome.?.applied.cut.term += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, reply.validate(request, try request.digest(a)));
    reply.outcome = .{ .rejected = .pending };
    try std.testing.expect(reply.confirmed());
    reply.request_digest[0] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, reply.validate(request, try request.digest(a)));
    const too_large = try a.alloc(u8, max_reply_bytes + 1);
    defer a.free(too_large);
    @memset(too_large, ' ');
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, request, too_large));
    const request_bytes = try encodeRequest(a, request);
    defer a.free(request_bytes);
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{request_bytes[0 .. request_bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeRequest(a, duplicate));
}
