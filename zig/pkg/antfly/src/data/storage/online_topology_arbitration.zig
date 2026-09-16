// Copyright 2026 Antfly, Inc.
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

//! Shared raw/native Raft admission outcome. Reservation state is snapshotted;
//! per-entry rejection receipts share durable entry-identity retention.
const std = @import("std");
const source = @import("../../storage/db/online_source_contract.zig");
const topology = @import("../../storage/db/relational_integrity_topology_contract.zig");
pub const Rejection = @import("../../storage/data_raft_projection_wire.zig").TopologyRejection;
pub const Guard = struct {
    index: u64,
    action: union(enum) { ordinary: void, source: source.Command, revoke: topology.Fence },
};
pub const Reservation = struct {
    scope: source.Scope,
    released: bool = false,
    const checksum_offset = 2 + source.scope_encoded_size;
    pub const encoded_size = checksum_offset + 4;

    pub fn encode(self: Reservation) ![encoded_size]u8 {
        if (self.scope.authority != .raft) return error.InvalidOnlineTopologyReservation;
        var bytes: [encoded_size]u8 = undefined;
        bytes[0] = 2;
        bytes[1] = @intFromBool(self.released);
        @memcpy(bytes[2..checksum_offset], &try self.scope.encode());
        std.mem.writeInt(u32, bytes[checksum_offset..encoded_size], std.hash.Crc32.hash(bytes[0..checksum_offset]), .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Reservation {
        if (bytes.len != encoded_size or bytes[0] != 2 or bytes[1] > 1 or
            std.mem.readInt(u32, bytes[checksum_offset..encoded_size], .little) != std.hash.Crc32.hash(bytes[0..checksum_offset])) return error.InvalidOnlineTopologyReservation;
        const scope = source.Scope.decode(bytes[2..checksum_offset]) catch return error.InvalidOnlineTopologyReservation;
        if (scope.authority != .raft) return error.InvalidOnlineTopologyReservation;
        return .{ .scope = scope, .released = bytes[1] == 1 };
    }
};

pub fn reservationKey(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_group_online_topology:{d}", .{group_id});
}
pub fn rejectionKey(buf: []u8, group_id: u64, index: u64) ![]const u8 {
    const prefix = try rejectionPrefix(buf[0 .. buf.len - 8], group_id);
    std.mem.writeInt(u64, buf[prefix.len..][0..8], index, .big);
    return buf[0 .. prefix.len + 8];
}
pub fn rejectionPrefix(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_group_topology_rejection:{d}:", .{group_id});
}

pub fn decide(current: *?Reservation, guard: Guard, ordinary_active: bool) !?Rejection {
    if (guard.index == 0) return error.InvalidOnlineTopologyReservation;
    switch (guard.action) {
        .ordinary => if (current.*) |value| {
            if (!value.released) return .busy;
        },
        .revoke => |fence| if (current.*) |*value| {
            if (value.scope.fence.eql(fence)) value.released = true;
        },
        .source => |command| {
            const scope = command.scope();
            try scope.validate();
            if (scope.authority != .raft) return .scope_changed;
            if (command == .admit) {
                if (current.*) |value| {
                    if (std.meta.eql(value.scope, scope)) return if (value.released) .scope_changed else null;
                    if (!value.released) return .busy;
                    if (scope.fence.admission_epoch <= value.scope.fence.admission_epoch) return .scope_changed;
                }
                if (ordinary_active) return .busy;
                current.* = .{ .scope = scope };
            } else {
                const value = if (current.*) |*value| value else return .scope_changed;
                if (!std.meta.eql(value.scope, scope)) return .scope_changed;
                if (command == .release) value.released = true else if (value.released and command != .reclaim) return .scope_changed;
            }
        },
    }
    return null;
}
