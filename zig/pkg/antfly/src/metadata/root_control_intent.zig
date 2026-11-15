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

//! Durable ownership of one exact root control attempt. The feed owner validates
//! the typed command and frozen catalog fence before preparing this record.
//! Persist begin BEFORE dispatch. A recovered dispatching record is ambiguous,
//! even when a later read observes no consumer. Only an accepted receipt may be
//! used for exact ordered-outcome recovery. This record grants no ACK authority.
const std = @import("std");
pub const Digest = [32]u8;
pub const Actor = [16]u8;
pub const Receipt = struct {
    term: u64,
    index: u64,
    pub fn validate(self: Receipt) !void {
        if (self.term == 0 or self.index == 0) return error.InvalidChangefeed;
    }
};
pub const Payload = struct {
    /// Exact private v1 submit body, validated by the typed root owner.
    command: []const u8,
    /// Exact original immutable route fence; recovery cannot silently re-route
    /// the old receipt to a different root incarnation.
    fence: []const u8,
    table: []const u8,
    pub fn validate(self: Payload) !void {
        if (self.command.len == 0 or self.command.len > 4096 or self.fence.len == 0 or self.fence.len > 16384 or self.table.len == 0 or self.table.len > 1024 or !std.unicode.utf8ValidateSlice(self.table) or std.mem.indexOfScalar(u8, self.table, 0) != null) return error.InvalidChangefeed;
    }
    pub fn identity(self: Payload) Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:root-control-intent:v1");
        inline for (.{ self.command, self.fence, self.table }) |bytes| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, bytes.len, .big);
            sha.update(&length);
            sha.update(bytes);
        }
        return sha.finalResult();
    }
};
/// Metadata effects of a result already validated by the typed root owner.
/// A proof is a private audit binding, not a public credential or authorization.
pub const Effect = union(enum) {
    enrolled: struct { consumer_epoch: u64, start: u64 },
    acknowledged: struct { consumer_epoch: u64, previous: u64, next: u64 },
    renewed: struct { consumer_epoch: u64, expires_at: u64 },
    retired: struct { sequence: u64 },
    maintenance,
    rejected: u8,
    pub fn validate(self: Effect) !void {
        switch (self) {
            .enrolled => |v| if (v.consumer_epoch == 0) return error.InvalidChangefeed,
            .acknowledged => |v| if (v.consumer_epoch == 0 or v.next < v.previous) return error.InvalidChangefeed,
            .renewed => |v| if (v.consumer_epoch == 0 or v.expires_at == 0) return error.InvalidChangefeed,
            .retired => |v| if (v.sequence == 0) return error.InvalidChangefeed,
            .rejected => |code| if (code == 0 or code > 7) return error.InvalidChangefeed,
            .maintenance => {},
        }
    }
};
pub const Settlement = struct {
    sequence: u64,
    actor: Actor,
    receipt: Receipt,
    request_digest: Digest,
    proof: Digest,
    effect: Effect,
    pub fn validate(self: Settlement) !void {
        try self.receipt.validate();
        try self.effect.validate();
        if (self.sequence == 0 or std.mem.allEqual(u8, &self.actor, 0) or std.mem.allEqual(u8, &self.request_digest, 0) or std.mem.allEqual(u8, &self.proof, 0)) return error.InvalidChangefeed;
    }
};
pub const State = struct {
    sequence: u64,
    ownership_epoch: u64,
    grant_sequence: u64,
    payload: Payload,
    digest: Digest,
    stage: enum { prepared, dispatching, accepted } = .prepared,
    /// Unique to the dispatching process attempt. Never reuse after restart.
    actor: Actor = @splat(0),
    receipt: ?Receipt = null,
    pub fn init(sequence: u64, ownership_epoch: u64, grant_sequence: u64, payload: Payload) !State {
        const result: State = .{ .sequence = sequence, .ownership_epoch = ownership_epoch, .grant_sequence = grant_sequence, .payload = payload, .digest = payload.identity() };
        try result.validate();
        return result;
    }
    pub fn validate(self: State) !void {
        try self.payload.validate();
        if (self.sequence == 0 or self.ownership_epoch == 0 or self.grant_sequence == 0 or self.sequence <= self.grant_sequence or !std.mem.eql(u8, &self.digest, &self.payload.identity())) return error.InvalidChangefeed;
        switch (self.stage) {
            .prepared => if (!std.mem.allEqual(u8, &self.actor, 0) or self.receipt != null) return error.InvalidChangefeed,
            .dispatching => if (std.mem.allEqual(u8, &self.actor, 0) or self.receipt != null) return error.InvalidChangefeed,
            .accepted => {
                if (std.mem.allEqual(u8, &self.actor, 0)) return error.InvalidChangefeed;
                try (self.receipt orelse return error.InvalidChangefeed).validate();
            },
        }
    }
    pub fn verifySettlement(self: State, settlement: Settlement) !void {
        try self.validate();
        try settlement.validate();
        if (self.stage != .accepted or self.sequence != settlement.sequence or
            !std.mem.eql(u8, &self.actor, &settlement.actor) or
            !std.mem.eql(u8, &self.digest, &settlement.request_digest) or
            !std.meta.eql(self.receipt.?, settlement.receipt)) return error.ChangefeedConflict;
    }
    pub fn begin(self: State, actor: Actor) !State {
        try self.validate();
        if (std.mem.allEqual(u8, &actor, 0)) return error.InvalidChangefeed;
        // Even the same actor cannot acquire a second dispatch permission.
        if (self.stage != .prepared) return error.ChangefeedConflict;
        var next = self;
        next.stage = .dispatching;
        next.actor = actor;
        return next;
    }
    pub fn accept(self: State, actor: Actor, receipt: Receipt) !State {
        try self.validate();
        try receipt.validate();
        if (!std.mem.eql(u8, &self.actor, &actor) or self.stage == .prepared) return error.ChangefeedConflict;
        if (self.receipt) |previous| {
            if (!std.meta.eql(previous, receipt)) return error.ChangefeedConflict;
            return self;
        }
        var next = self;
        next.stage = .accepted;
        next.receipt = receipt;
        return next;
    }
};

test "root control intent persists ambiguity and exact receipt without permitting redispatch" {
    const a = std.testing.allocator;
    const payload: Payload = .{ .command = "{\"version\":1}", .fence = "{\"root\":7}", .table = "docs" };
    try std.testing.expectError(error.InvalidChangefeed, State.init(10, 2, 10, payload));
    var state = try State.init(19, 2, 10, payload);
    try std.testing.expectError(error.InvalidChangefeed, state.begin(@splat(0)));
    state = try state.begin(@splat(1));
    const encoded = try std.json.Stringify.valueAlloc(a, state, .{});
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(State, a, encoded, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expectEqual(.dispatching, parsed.value.stage);
    try std.testing.expectError(error.ChangefeedConflict, parsed.value.begin(@splat(1)));
    try std.testing.expectError(error.ChangefeedConflict, parsed.value.begin(@splat(2)));
    try std.testing.expectError(error.ChangefeedConflict, parsed.value.accept(@splat(2), .{ .term = 1, .index = 20 }));
    const receipt: Receipt = .{ .term = std.math.maxInt(u64), .index = std.math.maxInt(u64) };
    state = try parsed.value.accept(@splat(1), receipt);
    try state.validate();
    try std.testing.expectEqualDeep(state, try state.accept(@splat(1), receipt));
    try std.testing.expectError(error.ChangefeedConflict, state.accept(@splat(1), .{ .term = 1, .index = 20 }));
    var forged = state;
    forged.payload.command = "{}";
    try std.testing.expectError(error.InvalidChangefeed, forged.validate());
    forged = state;
    forged.stage = .dispatching;
    try std.testing.expectError(error.InvalidChangefeed, forged.validate());
    forged = state;
    forged.actor = @splat(0);
    try std.testing.expectError(error.InvalidChangefeed, forged.validate());
}
