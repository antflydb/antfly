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

//! Bounded coordinator intents. Capability admission belongs to the metadata
//! proposal path; apply additionally checks durable membership-bound activation.
//! Root publication has a separate membership-bound capability. No sender
//! proof is carried: every receiver must independently verify its local cut.
const std = @import("std");
const r = @import("antfly_local_sources").system_catalog_relation_reconciliation;
const protocol = @import("topology_protocol.zig");
const incarnation = @import("antfly_local_sources").metadata_incarnation;
const magic = "AFRI01";
pub const max_encoded_bytes = magic.len + 1 + 2 * r.State.encoded_len + 1;
pub const Publication = struct { state: r.State, prior: ?r.Generation = null, activation: protocol.Activation };
pub const Command = union(enum(u8)) {
    adopt: protocol.Activation = 1,
    start: struct { next: r.State, prior: ?r.State = null } = 2,
    /// One bounded step from an already durable cut. Producers must observe
    /// its committed successor before submitting another step; chained future
    /// cuts inside the same Raft apply batch are deliberately not admitted.
    advance: r.State = 3,
    garbage: r.Retirement = 4,
    publish: Publication = 5,

    pub fn requiredDecoderVersion(self: @This()) u16 {
        return if (self == .publish) protocol.relation_publication_version else protocol.relation_reconciliation_version;
    }

    pub fn encodeAlloc(self: @This(), a: std.mem.Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        try out.appendSlice(a, magic);
        try out.append(a, @backingInt(std.meta.activeTag(self)));
        switch (self) {
            .adopt => |proof| {
                try validateActivation(proof);
                var version: [2]u8 = undefined;
                std.mem.writeInt(u16, &version, proof.version, .big);
                try out.appendSlice(a, &version);
                try out.appendSlice(a, &proof.incarnation);
                var count: [4]u8 = undefined;
                std.mem.writeInt(u32, &count, proof.member_count, .big);
                try out.appendSlice(a, &count);
                try out.appendSlice(a, &proof.membership_fingerprint);
            },
            .start => |request| {
                try validateStart(request.next, request.prior);
                try out.appendSlice(a, &(try request.next.encode()));
                try out.append(a, @intFromBool(request.prior != null));
                if (request.prior) |prior| try out.appendSlice(a, &(try prior.encode()));
            },
            .advance => |state| {
                try validateAdvance(state);
                try out.appendSlice(a, &(try state.encode()));
            },
            .garbage => |retirement| try out.appendSlice(a, &(try retirement.encode())),
            .publish => |request| {
                try validatePublication(request);
                try out.appendSlice(a, &(try request.state.encode()));
                try out.append(a, @intFromBool(request.prior != null));
                if (request.prior) |prior| try out.appendSlice(a, &(try prior.encode()));
                var version: [2]u8 = undefined;
                std.mem.writeInt(u16, &version, request.activation.version, .big);
                try out.appendSlice(a, &version);
                try out.appendSlice(a, &request.activation.incarnation);
                var count: [4]u8 = undefined;
                std.mem.writeInt(u32, &count, request.activation.member_count, .big);
                try out.appendSlice(a, &count);
                try out.appendSlice(a, &request.activation.membership_fingerprint);
            },
        }
        return out.toOwnedSlice(a);
    }
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len < magic.len + 1 or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidRelationReconciliationCommand;
        const payload = bytes[magic.len + 1 ..];
        switch (bytes[magic.len]) {
            1 => {
                if (payload.len != 2 + 32 + 4 + 32) return error.InvalidRelationReconciliationCommand;
                const proof: protocol.Activation = .{
                    .version = std.mem.readInt(u16, payload[0..2], .big),
                    .incarnation = payload[2..34].*,
                    .member_count = std.mem.readInt(u32, payload[34..38], .big),
                    .membership_fingerprint = payload[38..70].*,
                };
                try validateActivation(proof);
                return .{ .adopt = proof };
            },
            2 => {
                if (payload.len < r.State.encoded_len + 1) return error.InvalidRelationReconciliationCommand;
                const has_prior = payload[r.State.encoded_len];
                if (has_prior > 1 or payload.len != r.State.encoded_len + 1 + @as(usize, has_prior) * r.State.encoded_len) return error.InvalidRelationReconciliationCommand;
                const next = try r.State.decode(payload[0..r.State.encoded_len]);
                const prior = if (has_prior == 1) try r.State.decode(payload[r.State.encoded_len + 1 ..]) else null;
                try validateStart(next, prior);
                return .{ .start = .{ .next = next, .prior = prior } };
            },
            3 => {
                const state = r.State.decode(payload) catch return error.InvalidRelationReconciliationCommand;
                try validateAdvance(state);
                return .{ .advance = state };
            },
            4 => return .{ .garbage = r.Retirement.decode(payload) catch return error.InvalidRelationReconciliationCommand },
            5 => {
                if (payload.len < r.State.encoded_len + 1 + 70) return error.InvalidRelationReconciliationCommand;
                const has_prior = payload[r.State.encoded_len];
                if (has_prior > 1 or payload.len != r.State.encoded_len + 1 + @as(usize, has_prior) * r.Generation.encoded_len + 70) return error.InvalidRelationReconciliationCommand;
                const tail = payload[payload.len - 70 ..];
                const request: Publication = .{
                    .state = r.State.decode(payload[0..r.State.encoded_len]) catch return error.InvalidRelationReconciliationCommand,
                    .prior = if (has_prior == 1) r.Generation.decode(payload[r.State.encoded_len + 1 ..][0..r.Generation.encoded_len]) catch return error.InvalidRelationReconciliationCommand else null,
                    .activation = .{ .version = std.mem.readInt(u16, tail[0..2], .big), .incarnation = tail[2..34].*, .member_count = std.mem.readInt(u32, tail[34..38], .big), .membership_fingerprint = tail[38..70].* },
                };
                validatePublication(request) catch return error.InvalidRelationReconciliationCommand;
                return .{ .publish = request };
            },
            else => return error.InvalidRelationReconciliationCommand,
        }
    }
};

fn validatePublication(request: Publication) !void {
    _ = r.LiveRoot.initial(request.state) catch return error.InvalidRelationReconciliationCommand;
    if (request.activation.version != protocol.relation_publication_version or request.activation.member_count == 0 or
        !incarnation.isValid(request.activation.incarnation) or std.mem.allEqual(u8, &request.activation.membership_fingerprint, 0)) return error.InvalidRelationReconciliationCommand;
    var identity: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&identity, &request.activation.incarnation) catch return error.InvalidRelationReconciliationCommand;
    if (!std.mem.eql(u8, &identity, &request.state.epoch.incarnation)) return error.InvalidRelationReconciliationCommand;
    if (request.prior) |prior| {
        _ = prior.encode() catch return error.InvalidRelationReconciliationCommand;
        if (prior.group_id != request.state.group_id or std.mem.order(u8, &prior.job_id, &request.state.job_id) != .lt) return error.InvalidRelationReconciliationCommand;
    }
}

fn validateAdvance(state: r.State) !void {
    _ = try state.encode();
    if (state.failure != .none or state.phase == .ready or state.epoch.revision == 0) return error.InvalidRelationReconciliationCommand;
}
fn validateActivation(proof: protocol.Activation) !void {
    if (proof.version != protocol.relation_reconciliation_version or proof.member_count == 0 or
        !incarnation.isValid(proof.incarnation) or std.mem.allEqual(u8, &proof.membership_fingerprint, 0)) return error.InvalidRelationReconciliationCommand;
}
fn validateStart(next: r.State, prior: ?r.State) !void {
    _ = try next.encode();
    if (next.failure != .none or next.phase != .building or next.cursor_len != 0 or next.pass.rows != 0 or next.pass.claims != 0 or
        !std.mem.allEqual(u8, &next.pass.source_hash, 0) or !std.mem.allEqual(u8, &next.pass.claim_hash, 0)) return error.InvalidRelationReconciliationCommand;
    if (prior) |state| {
        _ = try state.encode();
        if (state.group_id != next.group_id) return error.InvalidRelationReconciliationCommand;
    }
    if (!std.mem.eql(u8, &next.job_id, &(try r.nextJobId(if (prior) |*state| state else null)))) return error.InvalidRelationReconciliationCommand;
}

test "system catalog relation namespace transaction coordinator intents are bounded canonical and allocation safe" {
    const a = std.testing.allocator;
    const proof: protocol.Activation = .{ .version = protocol.relation_reconciliation_version, .incarnation = "11111111111111111111111111111111".*, .member_count = 1, .membership_fingerprint = @splat(3) };
    const state = try r.State.init(41, try r.nextJobId(null), .{ .incarnation = @splat(1), .revision = 1 });
    const next = try r.State.init(41, try r.nextJobId(&state), state.epoch);
    for ([_]Command{ .{ .adopt = proof }, .{ .start = .{ .next = state } }, .{ .start = .{ .next = next, .prior = state } }, .{ .advance = state }, .{ .garbage = r.Retirement.init(r.Generation.of(&state)) } }) |command| {
        const bytes = try command.encodeAlloc(a);
        defer a.free(bytes);
        try std.testing.expect(bytes.len <= max_encoded_bytes);
        const decoded = try Command.decode(bytes);
        try std.testing.expect(std.meta.eql(command, decoded));
        for (0..bytes.len) |len| try std.testing.expectError(error.InvalidRelationReconciliationCommand, Command.decode(bytes[0..len]));
        const extra = try std.mem.concat(a, u8, &.{ bytes, "x" });
        defer a.free(extra);
        try std.testing.expectError(error.InvalidRelationReconciliationCommand, Command.decode(extra));
    }
    var bad = proof;
    bad.member_count = 0;
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .adopt = bad }).encodeAlloc(a));
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .start = .{ .next = next } }).encodeAlloc(a));
    var ready = state;
    ready.phase = .ready;
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .advance = ready }).encodeAlloc(a));
    var untracked = state;
    untracked.epoch.revision = 0;
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .advance = untracked }).encodeAlloc(a));
    var failed = state;
    failed.failure = .name_conflict;
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .advance = failed }).encodeAlloc(a));
    try std.testing.expectError(error.InvalidRelationReconciliationCommand, (Command{ .start = .{ .next = failed } }).encodeAlloc(a));
    const replacement = try (Command{ .start = .{ .next = next, .prior = failed } }).encodeAlloc(a);
    defer a.free(replacement);
    try std.testing.expect(std.meta.eql(failed, (try Command.decode(replacement)).start.prior.?));
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, command: Command) !void {
            const bytes = try command.encodeAlloc(alloc);
            defer alloc.free(bytes);
            _ = try Command.decode(bytes);
        }
    };
    // Force growth/shrink through allocation so the backing heap's ability to
    // resize in place cannot change the numbered allocation-fault schedule.
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{Command{ .start = .{ .next = next, .prior = state } }});
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{Command{ .advance = state }});
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{Command{ .garbage = r.Retirement.init(r.Generation.of(&state)) }});
}

test "system catalog relation publication command fences exact membership epoch and prior generation" {
    const a = std.testing.allocator;
    const activation: protocol.Activation = .{ .version = protocol.relation_publication_version, .incarnation = "01010101010101010101010101010101".*, .member_count = 3, .membership_fingerprint = @splat(9) };
    var first = try r.State.init(41, try r.nextJobId(null), .{ .incarnation = @splat(1), .revision = 1 });
    first.phase = .ready;
    var second = try r.State.init(41, try r.nextJobId(&first), first.epoch);
    second.phase = .ready;
    const requests: []const Command = &.{
        .{ .publish = .{ .state = first, .activation = activation } },
        .{ .publish = .{ .state = second, .prior = r.Generation.of(&first), .activation = activation } },
    };
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, command: Command) !void {
            const encoded = try command.encodeAlloc(alloc);
            defer alloc.free(encoded);
            try std.testing.expect(encoded.len <= max_encoded_bytes);
            try std.testing.expect(std.meta.eql(command, try Command.decode(encoded)));
        }
    };
    for (requests) |command| {
        try std.testing.expectEqual(protocol.relation_publication_version, command.requiredDecoderVersion());
        const encoded = try command.encodeAlloc(a);
        defer a.free(encoded);
        for (0..encoded.len) |len| try std.testing.expectError(error.InvalidRelationReconciliationCommand, Command.decode(encoded[0..len]));
        const extra = try std.mem.concat(a, u8, &.{ encoded, "x" });
        defer a.free(extra);
        try std.testing.expectError(error.InvalidRelationReconciliationCommand, Command.decode(extra));
        var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{command});
    }
    for (0..7) |fault| {
        var bad = requests[1];
        switch (fault) {
            0 => bad.publish.activation.version = protocol.relation_reconciliation_version,
            1 => bad.publish.activation.member_count = 0,
            2 => bad.publish.activation.membership_fingerprint = @splat(0),
            3 => bad.publish.activation.incarnation = "02020202020202020202020202020202".*,
            4 => bad.publish.prior = r.Generation.of(&second),
            5 => bad.publish.prior.?.group_id = 42,
            6 => bad.publish.state.phase = .building,
            else => unreachable,
        }
        try std.testing.expectError(error.InvalidRelationReconciliationCommand, bad.encodeAlloc(a));
    }
    try std.testing.expectEqual(protocol.relation_reconciliation_version, (Command{ .advance = try r.State.init(41, try r.nextJobId(null), first.epoch) }).requiredDecoderVersion());
}
