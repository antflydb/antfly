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
/// with a fixed-size membership contract. Version 3 fences create against the
/// replicated transition generation so recreated tables receive fresh data
/// group identities and stale drop cleanup cannot delete their storage.
/// Version 4 adds compare-and-set table preconditions to extension lifecycle
/// entries; leaders gate those entries on the same all-member capability.
pub const current_version: u16 = 4;
/// Minimum decoder capability required by the atomic create/drop wire format.
/// Later, unrelated metadata features must not unnecessarily stop table DDL
/// during a rolling upgrade.
pub const atomic_table_topology_version: u16 = 3;
/// Decoder capability required only when lifecycle entries carry table CAS
/// preconditions.
pub const extension_lifecycle_table_cas_version: u16 = 4;

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
/// Largest legacy v1 membership vector that could fit in a command accepted
/// by the topology proposal path. The decoder checks this before allocating,
/// so a corrupt persisted frame cannot turn a compact metadata entry into an
/// unbounded second allocation during apply.
pub const max_legacy_drop_range_count: usize = max_transition_command_bytes / @sizeOf(u64);

/// Exact post-commit cleanup contract returned by an in-process leader. Across
/// HTTP the receipt is deliberately O(1), and `group_ids` is empty: every Raft
/// replica owner independently stages a durable group-retirement intent at
/// replica-catalog removal. Older leaders' vectors are skipped without
/// materializing them, retaining wire compatibility with bounded memory.
pub const DropResult = struct {
    table_id: u64,
    expected_transition_generation: u64,
    group_ids: []u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.group_ids);
        self.* = undefined;
    }

    pub fn cleanupContract(self: @This()) DropCleanupContract {
        return .{
            .table_id = self.table_id,
            .expected_transition_generation = self.expected_transition_generation,
            .group_ids = self.group_ids,
        };
    }
};

/// Borrowed storage-cleanup view of a committed drop. Keeping ownership out of
/// the callback ABI lets request handlers retain and free the routed result.
pub const DropCleanupContract = struct {
    table_id: u64,
    expected_transition_generation: u64,
    group_ids: []const u64,
};

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
