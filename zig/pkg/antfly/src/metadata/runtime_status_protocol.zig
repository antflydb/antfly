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

/// Runtime-status records are embedded in StoreRecord transitions. Only named
/// wire profiles are compatibility surfaces: V12 is the v0.2.0 profile, V15
/// is the positional profile, V16 adds inference diagnostics, V17 introduces
/// framed index records and native dense-storage capability, and V18 carries
/// the storage owner's applied schema epoch for indexless schema cutover. V19
/// adds media output counters; V20 adds durable source authority and the native
/// commit cut. Intermediate development
/// formats must never be advertised, negotiated, read, or written.
pub const v0_2_0_record_version: u16 = 12;
pub const positional_record_version: u16 = 15;
pub const previous_record_version: u16 = positional_record_version;
pub const inference_diagnostics_record_version: u16 = 16;
pub const framed_index_record_version: u16 = 17;
pub const schema_epoch_record_version: u16 = 18;
pub const media_output_record_version: u16 = 19;
pub const source_authority_record_version: u16 = 20;
pub const current_record_version: u16 = source_authority_record_version;
pub const legacy_record_version: u16 = v0_2_0_record_version;

pub const Profile = enum(u16) {
    released_v0_2_0 = v0_2_0_record_version,
    positional = positional_record_version,
    inference_diagnostics = inference_diagnostics_record_version,
    framed_index = framed_index_record_version,
    schema_epoch = schema_epoch_record_version,
    media_output = media_output_record_version,
    current = current_record_version,

    pub fn wireVersion(self: @This()) u16 {
        return @backingInt(self);
    }
};

pub fn profileForVersion(version: u16) ?Profile {
    return std.enums.fromInt(Profile, version);
}

/// Compatibility is a relationship between named wire profiles, not numeric
/// ordering. An unknown development or future version must never satisfy a
/// released profile merely because its integer is larger.
pub fn profileSatisfies(available_version: u16, required_version: u16) bool {
    const available = profileForVersion(available_version) orelse return false;
    const required = profileForVersion(required_version) orelse return false;
    return switch (required) {
        .released_v0_2_0 => true,
        .positional => available != .released_v0_2_0,
        .inference_diagnostics => available == .inference_diagnostics or available == .framed_index or available == .schema_epoch or available == .media_output or available == .current,
        .framed_index => available == .framed_index or available == .schema_epoch or available == .media_output or available == .current,
        .schema_epoch => available == .schema_epoch or available == .media_output or available == .current,
        .media_output => available == .media_output or available == .current,
        .current => available == .current,
    };
}

/// Returns the strongest named wire profile understood by both endpoints.
/// Version integers identify formats; compatibility is never inferred from
/// numeric ordering.
pub fn greatestCommonProfile(left_version: u16, right_version: u16) ?Profile {
    const candidates = [_]Profile{ .current, .media_output, .schema_epoch, .framed_index, .inference_diagnostics, .positional, .released_v0_2_0 };
    for (candidates) |candidate| {
        const version = candidate.wireVersion();
        if (profileSatisfies(left_version, version) and
            profileSatisfies(right_version, version))
        {
            return candidate;
        }
    }
    return null;
}

/// Facts already encoded by the positional V15 profile.
pub const repair_status_record_version: u16 = positional_record_version;
pub const native_restore_identity_record_version: u16 = positional_record_version;
pub const artifact_source_status_record_version: u16 = positional_record_version;
pub const artifact_source_failure_status_record_version: u16 = positional_record_version;
pub const publication_target_record_version: u16 = positional_record_version;

/// Native projection state is introduced atomically with the framed V17
/// profile. Semantic aliases keep call sites descriptive without creating
/// wire formats for every field.
pub const vector_projection_record_version: u16 = framed_index_record_version;
pub const dense_native_storage_record_version: u16 = framed_index_record_version;
pub const framed_index_status_record_version: u16 = framed_index_record_version;
pub const dense_native_capability_record_version: u16 = framed_index_record_version;

pub fn isSupported(version: u16) bool {
    return isNegotiable(version);
}

pub fn isNegotiable(version: u16) bool {
    return profileForVersion(version) != null;
}

/// The sender explicitly omits a new observation until all metadata decoders
/// are ready. Proposal admission independently rechecks readiness before the
/// first durable report activates the codec. Missing always means unknown.
pub fn projectSourceAuthority(reports: anytype, ready_version: u16) void {
    if (profileSatisfies(ready_version, source_authority_record_version)) return;
    for (reports) |*report| report.source_authority = null;
}

/// A late weaker probe must not erase a stronger proof for the same membership.
/// Membership replacement discards the prior proof; unknown profiles grant none.
pub fn mergeReadyVersion(same_membership: bool, previous: u16, observed: u16) u16 {
    if (!isNegotiable(observed)) return 0;
    if (same_membership and profileSatisfies(previous, observed)) return previous;
    return observed;
}

test "runtime status source authority readiness retains older-profile proofs only within their membership" {
    try std.testing.expectEqual(@as(u16, 18), mergeReadyVersion(true, 18, 17));
    try std.testing.expectEqual(source_authority_record_version, mergeReadyVersion(true, 18, source_authority_record_version));
    try std.testing.expectEqual(source_authority_record_version, mergeReadyVersion(true, source_authority_record_version, media_output_record_version));
    try std.testing.expectEqual(media_output_record_version, mergeReadyVersion(false, source_authority_record_version, media_output_record_version));
    try std.testing.expectEqual(@as(u16, 18), mergeReadyVersion(true, 21, 18));
    try std.testing.expectEqual(@as(u16, 0), mergeReadyVersion(true, source_authority_record_version, 21));
}

test "runtime status source authority projection requires a ready named profile" {
    const Report = struct { source_authority: ?u64 = 7 };
    inline for (.{ @as(u16, 0), @as(u16, 18), @as(u16, 19), @as(u16, 21), @as(u16, 65535) }) |version| {
        var reports = [_]Report{.{}};
        projectSourceAuthority(&reports, version);
        try std.testing.expect(reports[0].source_authority == null);
    }
    var reports = [_]Report{.{}};
    projectSourceAuthority(&reports, source_authority_record_version);
    try std.testing.expectEqual(@as(?u64, 7), reports[0].source_authority);
}

test "runtime status exposes only released compatibility profiles" {
    try std.testing.expect(!isSupported(1));
    try std.testing.expect(isSupported(v0_2_0_record_version));
    try std.testing.expect(isSupported(positional_record_version));
    try std.testing.expect(isSupported(current_record_version));
    try std.testing.expect(!isSupported(0));
    try std.testing.expect(!isSupported(13));
    try std.testing.expect(!isSupported(14));
    try std.testing.expect(isSupported(16));
    try std.testing.expect(isSupported(18));
    try std.testing.expect(isSupported(19));
    try std.testing.expect(isSupported(20));
    try std.testing.expect(!isSupported(21));

    try std.testing.expect(isNegotiable(v0_2_0_record_version));
    try std.testing.expect(isNegotiable(positional_record_version));
    try std.testing.expect(isNegotiable(current_record_version));
    try std.testing.expect(!isNegotiable(11));
    try std.testing.expect(!isNegotiable(13));
    try std.testing.expect(!isNegotiable(14));
    try std.testing.expect(isNegotiable(15));
    try std.testing.expect(isNegotiable(16));
    try std.testing.expectEqual(Profile.released_v0_2_0, profileForVersion(12).?);
    try std.testing.expectEqual(Profile.positional, profileForVersion(15).?);
    try std.testing.expectEqual(Profile.inference_diagnostics, profileForVersion(16).?);
    try std.testing.expectEqual(Profile.framed_index, profileForVersion(17).?);
    try std.testing.expectEqual(Profile.schema_epoch, profileForVersion(18).?);
    try std.testing.expectEqual(Profile.media_output, profileForVersion(19).?);
    try std.testing.expect(profileSatisfies(19, 18));
    try std.testing.expect(profileSatisfies(19, 19));
    try std.testing.expect(!profileSatisfies(18, 19));
    try std.testing.expect(!profileSatisfies(21, 19));
    try std.testing.expectEqual(Profile.schema_epoch, greatestCommonProfile(19, 18).?);
    try std.testing.expectEqual(Profile.media_output, greatestCommonProfile(19, 19).?);

    try std.testing.expect(profileSatisfies(12, 12));
    try std.testing.expect(profileSatisfies(15, 12));
    try std.testing.expect(profileSatisfies(15, 15));
    try std.testing.expect(profileSatisfies(16, 12));
    try std.testing.expect(profileSatisfies(16, 15));
    try std.testing.expect(profileSatisfies(16, 16));
    try std.testing.expect(!profileSatisfies(12, 15));
    try std.testing.expect(!profileSatisfies(13, 12));
    try std.testing.expect(!profileSatisfies(14, 15));
    try std.testing.expect(profileSatisfies(17, 15));
    try std.testing.expect(profileSatisfies(17, 16));
    try std.testing.expect(!profileSatisfies(16, 17));
    try std.testing.expect(profileSatisfies(18, 15));
    try std.testing.expect(profileSatisfies(18, 17));
    try std.testing.expect(!profileSatisfies(17, 18));
    try std.testing.expect(!profileSatisfies(15, 16));

    try std.testing.expectEqual(Profile.framed_index, greatestCommonProfile(17, 17).?);
    try std.testing.expectEqual(Profile.inference_diagnostics, greatestCommonProfile(16, 16).?);
    try std.testing.expectEqual(Profile.positional, greatestCommonProfile(16, 15).?);
    try std.testing.expectEqual(Profile.positional, greatestCommonProfile(15, 16).?);
    try std.testing.expectEqual(Profile.released_v0_2_0, greatestCommonProfile(16, 12).?);
    try std.testing.expectEqual(Profile.released_v0_2_0, greatestCommonProfile(15, 12).?);
    try std.testing.expectEqual(@as(?Profile, null), greatestCommonProfile(16, 14));
    try std.testing.expectEqual(Profile.inference_diagnostics, greatestCommonProfile(17, 16).?);
    try std.testing.expectEqual(Profile.inference_diagnostics, greatestCommonProfile(18, 16).?);

    try std.testing.expectEqual(Profile.schema_epoch, greatestCommonProfile(19, 18).?);
    try std.testing.expect(!profileSatisfies(18, 19));
    try std.testing.expect(profileSatisfies(19, 18));
    var rolling_common = Profile.current;
    for ([_]u16{ 16, 15, 16 }) |peer_version| {
        rolling_common = greatestCommonProfile(rolling_common.wireVersion(), peer_version).?;
    }
    try std.testing.expectEqual(Profile.positional, rolling_common);
}

const std = @import("std");

test "runtime status merged source authority negotiates above the shipped media profile" {
    try std.testing.expectEqual(Profile.current, profileForVersion(20).?);
    try std.testing.expect(profileSatisfies(20, 19));
    try std.testing.expect(profileSatisfies(20, 20));
    try std.testing.expect(!profileSatisfies(19, 20));
    try std.testing.expectEqual(Profile.media_output, greatestCommonProfile(20, 19).?);
    try std.testing.expectEqual(Profile.current, greatestCommonProfile(20, 20).?);
}
