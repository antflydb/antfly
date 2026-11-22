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

//! Bounded read-only grant observations across private compiled boundaries.
//! Request binding is mandatory; an observation is not a command receipt.
const std = @import("std");
const control = @import("history_consumer.zig");
pub const max_bytes = 4096;
pub const Request = struct {
    version: u8,
    authority: enum(u8) { native = 1, raft = 2 },
    identity: control.GrantIdentity,
    minimum_applied_index: u64 = 0,
    pub fn validate(self: Request) !void {
        try self.identity.validate();
        if (self.version != 1 or (self.authority == .native and self.minimum_applied_index != 0) or (self.authority == .raft and self.minimum_applied_index == 0)) return error.InvalidHistoryAdmission;
    }
    pub fn digest(self: Request) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:history-grant-observation:v1");
        hash.update(&.{ self.version, @backingInt(self.authority) });
        hash.update(&self.identity.namespace);
        hash.update(&self.identity.authority);
        hash.update(&self.identity.consumer);
        hash.update(&self.identity.definition);
        for ([_]u64{ self.identity.sequence, self.identity.policy_revision, self.minimum_applied_index }) |value| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        if (self.identity.include_bootstrap) hash.update("\x01");
        if (self.identity.include_cut) hash.update("\x02");
        if (self.identity.include_pressure) hash.update("\x03");
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8,
    request_digest: [32]u8,
    observation: control.GrantObservation,
    pub fn init(request: Request, observation: control.GrantObservation) !Reply {
        const value: Reply = .{ .version = 1, .request_digest = request.digest(), .observation = observation };
        try value.validate(request);
        return value;
    }
    pub fn validate(self: Reply, request: Request) !void {
        try request.validate();
        if (self.version != 1 or !std.mem.eql(u8, &self.request_digest, &request.digest())) return error.HistoryCorrupt;
        switch (self.observation) {
            .unbound => {},
            .absent => |floor| if (floor >= request.identity.sequence) return error.HistoryCorrupt,
            .retired => |floor| if (floor < request.identity.sequence) return error.HistoryCorrupt,
            .active, .other_grant => |value| {
                const consumer = value.consumer;
                if (!std.mem.eql(u8, &consumer.id, &request.identity.consumer) or consumer.epoch == 0 or consumer.expires_at_seconds == 0 or
                    consumer.policy_revision != request.identity.policy_revision or consumer.acknowledged < consumer.start or value.limit == 0 or value.grant == 0) return error.HistoryCorrupt;
                if ((value.grant == request.identity.sequence) != (self.observation == .active)) return error.HistoryCorrupt;
                if (value.holds_pressure_floor and (!request.identity.include_pressure or self.observation != .active))
                    return error.HistoryCorrupt;
                if (value.quiescent_latest) |latest| {
                    if (!request.identity.include_cut or self.observation != .active or latest < consumer.acknowledged) return error.HistoryCorrupt;
                }
                if (value.bootstrap) |pin| {
                    if (!request.identity.include_bootstrap or self.observation != .active or pin.expires_at_seconds == 0 or
                        pin.expires_at_seconds > consumer.expires_at_seconds or std.mem.allEqual(u8, &pin.digest, 0)) return error.HistoryCorrupt;
                }
            },
        }
    }
};
pub fn decodeRequest(a: std.mem.Allocator, raw: []const u8) !Request {
    if (raw.len > max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Request, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn decodeReply(a: std.mem.Allocator, raw: []const u8, request: Request) !Reply {
    if (raw.len > max_bytes) return error.HistoryCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(request);
    return parsed.value;
}

pub const test_support = if (@import("builtin").is_test) struct {
    pub fn contracts() !void {
        const a = std.testing.allocator;
        const maximum = std.math.maxInt(u64);
        var request: Request = .{ .version = 1, .authority = .raft, .minimum_applied_index = maximum, .identity = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .definition = @splat(4), .sequence = maximum, .policy_revision = maximum } };
        const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
        defer a.free(encoded);
        try std.testing.expectEqualDeep(request, try decodeRequest(a, encoded));
        var reply = try Reply.init(request, .{ .active = .{ .grant = maximum, .limit = maximum, .consumer = .{ .id = request.identity.consumer, .epoch = maximum, .start = maximum, .acknowledged = maximum, .expires_at_seconds = maximum, .policy_revision = maximum } } });
        const bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
        defer a.free(bytes);
        try std.testing.expectEqualDeep(reply, try decodeReply(a, bytes, request));
        inline for (.{ "namespace", "authority", "consumer", "definition" }) |field| {
            var wrong = request;
            @field(wrong.identity, field)[0] ^= 1;
            try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, bytes, wrong));
        }
        request.minimum_applied_index -= 1;
        try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, bytes, request));
        request.minimum_applied_index = 0;
        try std.testing.expectError(error.InvalidHistoryAdmission, request.validate());
        request.authority = .native;
        try request.validate();
        try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, bytes, request));
        reply = try Reply.init(request, .{ .absent = 0 });
        reply.observation = .{ .retired = 0 };
        try std.testing.expectError(error.HistoryCorrupt, reply.validate(request));
        reply.observation = .{ .absent = maximum };
        try std.testing.expectError(error.HistoryCorrupt, reply.validate(request));
        const original_request = try decodeRequest(a, encoded);
        var cut_request = original_request;
        cut_request.identity.include_cut = true;
        var cut_reply = try Reply.init(original_request, .{ .active = .{ .grant = maximum, .limit = maximum, .consumer = .{ .id = request.identity.consumer, .epoch = 1, .start = 0, .acknowledged = 1, .expires_at_seconds = maximum, .policy_revision = maximum } } });
        cut_reply.observation.active.quiescent_latest = 1;
        try std.testing.expectError(error.HistoryCorrupt, cut_reply.validate(original_request));
        cut_reply.request_digest = cut_request.digest();
        try cut_reply.validate(cut_request);
        cut_reply.observation.active.quiescent_latest = 0;
        try std.testing.expectError(error.HistoryCorrupt, cut_reply.validate(cut_request));
        cut_reply.observation.active.quiescent_latest = null;
        cut_reply.observation.active.holds_pressure_floor = true;
        try std.testing.expectError(error.HistoryCorrupt, cut_reply.validate(cut_request));
        cut_request.identity.include_pressure = true;
        cut_reply.request_digest = cut_request.digest();
        try cut_reply.validate(cut_request);
        try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, "{}", original_request));
        try std.testing.expectError(error.InvalidHistoryAdmission, decodeRequest(a, "{}"));
        const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{encoded[0 .. encoded.len - 1]});
        defer a.free(duplicate);
        try std.testing.expectError(error.InvalidHistoryAdmission, decodeRequest(a, duplicate));
        try std.testing.checkAllAllocationFailures(a, decodeAllocation, .{ encoded, bytes });
    }
} else struct {};

test "history grant wire binds authority identity and applied cut without truncating integers" {
    try test_support.contracts();
}

fn decodeAllocation(a: std.mem.Allocator, request: []const u8, reply: []const u8) !void {
    const decoded = try decodeRequest(a, request);
    _ = try decodeReply(a, reply, decoded);
}
