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

//! Intent-only submission and owned, exact read-only retirement recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const control = local.storage_source_scope_retirement;
pub const ordered = local.storage_source_scope_retirement_ordered;
const scope = local.storage_source_scope;
const A = std.mem.Allocator;
pub const max_request_bytes = control.max_command_bytes + 8192;
pub const max_reply_bytes = control.max_command_bytes + ordered.max_record_bytes + 4096;
pub const Request = struct {
    version: u8,
    request_id: [16]u8,
    operation: enum { submit, recover },
    intent: ?control.Intent = null,
    coordinator_locator: ?[]const u8 = null,
    recovery: ?ordered.Query = null,
    pub fn validate(self: Request) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.request_id, 0)) return error.InvalidSourceAdmission;
        switch (self.operation) {
            .submit => {
                if (self.intent == null or self.coordinator_locator == null or self.recovery != null) return error.InvalidSourceAdmission;
                try self.intent.?.validate();
                const locator = self.coordinator_locator.?;
                if (locator.len == 0 or locator.len > local.storage_transaction_completion.max_locator_bytes) return error.InvalidSourceAdmission;
                const parsed = local.api_local_transaction_contract.parseParticipantRef(locator) orelse return error.InvalidSourceAdmission;
                if (parsed.group_id != self.intent.?.coordinator.owner.group_id or parsed.restore_staging_scope != null or parsed.restore_staging_plan_id != null) return error.InvalidSourceAdmission;
            },
            .recover => {
                if (self.intent != null or self.coordinator_locator != null or self.recovery == null) return error.InvalidSourceAdmission;
                try self.recovery.?.validate();
            },
        }
    }
    pub fn owner(self: Request) @FieldType(control.Intent, "owner") {
        return if (self.operation == .submit) self.intent.?.owner else self.recovery.?.command.intent.owner;
    }
    pub fn digest(self: Request, a: A) !scope.Digest {
        const bytes = try encodeRequest(a, self);
        defer a.free(bytes);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-retirement-http-control:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: scope.Digest,
    command: ?control.Command = null,
    receipt: ?scope.Cut = null,
    outcome: ?ordered.Recovery = null,
    pub fn confirmed(self: Reply) bool {
        return self.version == 1 and self.command != null and self.receipt != null and self.receipt.?.term != 0 and self.receipt.?.index != 0 and self.outcome != null and self.outcome.? != .superseded;
    }
    pub fn validate(self: Reply, a: A, request: Request) !void {
        try request.validate();
        if (self.version != 1 or !std.meta.eql(self.request_digest, try request.digest(a))) return error.SourceAdmissionCorrupt;
        if (self.command) |command| {
            _ = command.proof() catch return error.SourceAdmissionCorrupt;
            if (request.operation == .submit) {
                if (!std.meta.eql(command.intent, request.intent.?)) return error.SourceAdmissionCorrupt;
                const admission = command.observation.decision.?.admission.admission;
                const member = try admission.coordinatorMember();
                if (!std.mem.eql(u8, member.locator, request.coordinator_locator.?)) return error.SourceAdmissionCorrupt;
            }
            if (self.receipt) |cut| {
                if (cut.term == 0 or cut.index == 0) return error.SourceAdmissionCorrupt;
                if (self.outcome) |outcome| {
                    const bytes = try ordered.encodeOutcome(a, command, cut, outcome);
                    defer a.free(bytes);
                }
            } else if (self.outcome != null) return error.SourceAdmissionCorrupt;
        } else if (self.receipt != null or self.outcome != null) return error.SourceAdmissionCorrupt;
        if (request.recovery) |query| {
            if (self.command == null or self.receipt == null or !std.meta.eql(self.receipt.?, query.entry)) return error.SourceAdmissionCorrupt;
            if (!std.meta.eql(try ordered.fingerprint(a, self.command.?), try ordered.fingerprint(a, query.command))) return error.SourceAdmissionCorrupt;
        }
    }
};
pub const OwnedReply = std.json.Parsed(Reply);
pub fn encodeRequest(a: A, request: Request) ![]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_request_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeRequest(a: A, bytes: []const u8) !std.json.Parsed(Request) {
    if (bytes.len == 0 or bytes.len > max_request_bytes) return error.InvalidSourceAdmission;
    var parsed = std.json.parseFromSlice(Request, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub fn encodeReply(a: A, request: Request, reply: Reply) ![]u8 {
    try reply.validate(a, request);
    const bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeReply(a: A, request: Request, bytes: []const u8) !OwnedReply {
    try request.validate();
    if (bytes.len == 0 or bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    errdefer parsed.deinit();
    try parsed.value.validate(a, request);
    return parsed;
}
pub fn ownReply(a: A, request: Request, reply: Reply) !OwnedReply {
    const bytes = try encodeReply(a, request, reply);
    defer a.free(bytes);
    return decodeReply(a, request, bytes);
}
pub fn unknown(a: A, request: Request) !OwnedReply {
    return ownReply(a, request, .{ .request_digest = try request.digest(a), .command = if (request.recovery) |q| q.command else null, .receipt = if (request.recovery) |q| q.entry else null });
}
