// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded coordinator intents. Capability admission belongs to the metadata
//! proposal path; apply additionally checks durable membership-bound activation.
//! These controls do not publish a root, adopt writers, or enable SQL serving.
const std = @import("std");
const r = @import("antfly_local_sources").system_catalog_relation_reconciliation;
const protocol = @import("topology_protocol.zig");
const incarnation = @import("antfly_local_sources").metadata_incarnation;
const magic = "AFRI01";
pub const max_encoded_bytes = magic.len + 1 + 2 * r.State.encoded_len + 1;
pub const Command = union(enum(u8)) {
    adopt: protocol.Activation = 1,
    start: struct { next: r.State, prior: ?r.State = null } = 2,

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
            else => return error.InvalidRelationReconciliationCommand,
        }
    }
};

fn validateActivation(proof: protocol.Activation) !void {
    if (proof.version != protocol.relation_reconciliation_version or proof.member_count == 0 or
        !incarnation.isValid(proof.incarnation) or std.mem.allEqual(u8, &proof.membership_fingerprint, 0)) return error.InvalidRelationReconciliationCommand;
}
fn validateStart(next: r.State, prior: ?r.State) !void {
    _ = try next.encode();
    if (next.phase != .building or next.cursor_len != 0 or next.pass.rows != 0 or next.pass.claims != 0 or
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
    for ([_]Command{ .{ .adopt = proof }, .{ .start = .{ .next = state } }, .{ .start = .{ .next = next, .prior = state } } }) |command| {
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
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, command: Command) !void {
            const bytes = try command.encodeAlloc(alloc);
            defer alloc.free(bytes);
            _ = try Command.decode(bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fault.run, .{Command{ .start = .{ .next = next, .prior = state } }});
}
