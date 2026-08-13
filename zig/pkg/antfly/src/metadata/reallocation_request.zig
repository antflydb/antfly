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
const metadata_incarnation = @import("incarnation.zig");

/// Every metadata voter must advertise at least this version before a forced
/// reallocation request can be admitted. Older reconcilers do not understand
/// the causal acknowledgement barrier and may clear the request prematurely.
pub const barrier_protocol_version: u16 = 1;
pub const MembershipFingerprint = [std.crypto.hash.sha2.Sha256.digest_length]u8;
pub const zero_membership_fingerprint = [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;

pub const ReallocationRequestRecord = struct {
    request_id: u128,
    requested_at_ms: u64,
    /// A zero version denotes a request created before the durable membership
    /// barrier. Such a request remains valid, but metadata membership changes
    /// must fail closed until it is consumed.
    barrier_protocol_version: u16 = 0,
    metadata_incarnation: ?metadata_incarnation.MetadataClusterIncarnation = null,
    protected_metadata_member_count: u32 = 0,
    protected_metadata_membership_fingerprint: MembershipFingerprint = zero_membership_fingerprint,
};

pub fn generateRequestId(io: std.Io) !u128 {
    var request_id: u128 = 0;
    while (request_id == 0) try io.randomSecure(std.mem.asBytes(&request_id));
    return request_id;
}

pub fn isValid(record: ReallocationRequestRecord) bool {
    if (record.request_id == 0) return false;
    if (record.barrier_protocol_version == 0) {
        return record.metadata_incarnation == null and
            record.protected_metadata_member_count == 0 and
            std.mem.eql(u8, &record.protected_metadata_membership_fingerprint, &zero_membership_fingerprint);
    }
    const incarnation = record.metadata_incarnation orelse return false;
    return metadata_incarnation.isValid(incarnation) and
        record.protected_metadata_member_count != 0 and
        !std.mem.eql(u8, &record.protected_metadata_membership_fingerprint, &zero_membership_fingerprint);
}

/// Hashes one sorted, duplicate-free metadata membership. Callers construct
/// the canonical set before invoking this function so the digest is stable
/// across voters, learners, restarts, and configuration ordering.
pub fn membershipFingerprint(sorted_node_ids: []const u64) MembershipFingerprint {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-metadata-reallocation-membership-v1");
    var count_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &count_bytes, sorted_node_ids.len, .little);
    hasher.update(&count_bytes);
    for (sorted_node_ids) |node_id| {
        var node_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &node_bytes, node_id, .little);
        hasher.update(&node_bytes);
    }
    var digest: MembershipFingerprint = undefined;
    hasher.final(&digest);
    return digest;
}

pub fn protectsMembership(record: ReallocationRequestRecord, sorted_node_ids: []const u64) bool {
    if (record.barrier_protocol_version != barrier_protocol_version) return false;
    const member_count = std.math.cast(u32, sorted_node_ids.len) orelse return false;
    if (record.protected_metadata_member_count != member_count) return false;
    const fingerprint = membershipFingerprint(sorted_node_ids);
    return std.mem.eql(u8, &record.protected_metadata_membership_fingerprint, &fingerprint);
}

test "reallocation request membership contract is canonical and fail closed for legacy records" {
    const incarnation: metadata_incarnation.MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    const fingerprint = membershipFingerprint(&.{ 1, 2, 3 });
    const record: ReallocationRequestRecord = .{
        .request_id = 7,
        .requested_at_ms = 11,
        .barrier_protocol_version = barrier_protocol_version,
        .metadata_incarnation = incarnation,
        .protected_metadata_member_count = 3,
        .protected_metadata_membership_fingerprint = fingerprint,
    };
    try std.testing.expect(isValid(record));
    try std.testing.expect(protectsMembership(record, &.{ 1, 2, 3 }));
    try std.testing.expect(!protectsMembership(record, &.{ 1, 2, 4 }));
    try std.testing.expect(!protectsMembership(.{ .request_id = 7, .requested_at_ms = 11 }, &.{ 1, 2, 3 }));
}
