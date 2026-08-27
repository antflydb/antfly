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

const std = @import("std");

/// Wire capability required to decode atomic table-topology transitions.
/// Version 2 replaces the unbounded explicit range-id list in table drops
/// with a fixed-size membership contract.
pub const current_version: u16 = 2;

/// Creating thousands of Raft groups is an operational workflow, not one
/// catalog request. Keep one create bounded in CPU, memory, and log growth.
pub const max_initial_ranges: u32 = 1024;

/// Canonical table definitions are copied into the Raft log and replication
/// messages. A small explicit ceiling prevents one request from monopolizing
/// the metadata runtime while retaining ample room for schemas and indexes.
pub const max_create_definition_bytes: usize = 2 * 1024 * 1024;

/// Defense in depth on the final encoded Raft command. This includes the
/// definition, generated range records, and codec overhead.
pub const max_transition_command_bytes: usize = 3 * 1024 * 1024;

pub const range_membership_digest_len = std.crypto.hash.sha2.Sha256.digest_length;

/// Fixed-size proof of the exact range ids owned by a table at admission.
/// The generation fence detects legitimate concurrent membership changes;
/// this digest additionally detects a missing or corrupt derived index.
pub const RangeMembership = struct {
    count: u64,
    digest: [range_membership_digest_len]u8,

    pub fn eql(lhs: RangeMembership, rhs: RangeMembership) bool {
        return lhs.count == rhs.count and
            std.crypto.timing_safe.eql(@TypeOf(lhs.digest), lhs.digest, rhs.digest);
    }
};

pub const RangeMembershipAccumulator = struct {
    count: u64 = 0,
    xor_digest: [range_membership_digest_len]u8 = [_]u8{0} ** range_membership_digest_len,

    pub fn add(self: *@This(), range_group_id: u64) !void {
        if (self.count == std.math.maxInt(u64)) return error.RangeMembershipOverflow;
        self.toggle(range_group_id);
        self.count += 1;
    }

    pub fn remove(self: *@This(), range_group_id: u64) !void {
        if (self.count == 0) return error.RangeMembershipUnderflow;
        self.toggle(range_group_id);
        self.count -= 1;
    }

    fn toggle(self: *@This(), range_group_id: u64) void {
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly-table-range-membership-entry-v1");
        var encoded: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &encoded, range_group_id, .little);
        hasher.update(&encoded);
        const contribution = hasher.finalResult();
        for (&self.xor_digest, contribution) |*byte, value| byte.* ^= value;
    }

    pub fn finish(self: @This(), table_id: u64) RangeMembership {
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly-table-range-membership-v1");
        var encoded: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &encoded, table_id, .little);
        hasher.update(&encoded);
        std.mem.writeInt(u64, &encoded, self.count, .little);
        hasher.update(&encoded);
        hasher.update(&self.xor_digest);
        return .{ .count = self.count, .digest = hasher.finalResult() };
    }
};

test "range membership is order independent and table scoped" {
    var lhs: RangeMembershipAccumulator = .{};
    try lhs.add(301);
    try lhs.add(302);
    var rhs: RangeMembershipAccumulator = .{};
    try rhs.add(302);
    try rhs.add(301);
    try std.testing.expect(lhs.finish(7).eql(rhs.finish(7)));
    try std.testing.expect(!lhs.finish(7).eql(rhs.finish(8)));
    try rhs.add(303);
    try std.testing.expect(!lhs.finish(7).eql(rhs.finish(7)));
}
