// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy
// of the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations.

/// Runtime-status records are embedded in unframed StoreRecord transitions.
/// Only released wire profiles are compatibility surfaces. V12 is the
/// v0.2.0 profile; V15 is the current profile. Every other number belongs to
/// an unreleased development format and must not be advertised, negotiated,
/// read, or written.
pub const v0_2_0_record_version: u16 = 12;
pub const current_record_version: u16 = 15;

pub const Profile = enum(u16) {
    released_v0_2_0 = v0_2_0_record_version,
    current = current_record_version,

    pub fn wireVersion(self: @This()) u16 {
        return @intFromEnum(self);
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
        .current => available == .current,
    };
}

/// These facts form one current admission-safety profile. Keeping semantic
/// aliases makes call sites state why V15 is required without inventing
/// intermediate compatibility levels when new facts join that profile.
pub const repair_status_record_version: u16 = current_record_version;
pub const native_restore_identity_record_version: u16 = current_record_version;
pub const artifact_source_status_record_version: u16 = current_record_version;
pub const artifact_source_failure_status_record_version: u16 = current_record_version;
pub const publication_target_record_version: u16 = current_record_version;

pub fn isSupported(version: u16) bool {
    return isNegotiable(version);
}

pub fn isNegotiable(version: u16) bool {
    return profileForVersion(version) != null;
}

test "runtime status exposes only released compatibility profiles" {
    try std.testing.expect(!isSupported(1));
    try std.testing.expect(isSupported(v0_2_0_record_version));
    try std.testing.expect(isSupported(current_record_version));
    try std.testing.expect(!isSupported(0));
    try std.testing.expect(!isSupported(13));
    try std.testing.expect(!isSupported(14));
    try std.testing.expect(!isSupported(16));

    try std.testing.expect(isNegotiable(v0_2_0_record_version));
    try std.testing.expect(isNegotiable(current_record_version));
    try std.testing.expect(!isNegotiable(11));
    try std.testing.expect(!isNegotiable(13));
    try std.testing.expect(!isNegotiable(14));
    try std.testing.expectEqual(Profile.released_v0_2_0, profileForVersion(12).?);
    try std.testing.expectEqual(Profile.current, profileForVersion(15).?);

    try std.testing.expect(profileSatisfies(12, 12));
    try std.testing.expect(profileSatisfies(15, 12));
    try std.testing.expect(profileSatisfies(15, 15));
    try std.testing.expect(!profileSatisfies(12, 15));
    try std.testing.expect(!profileSatisfies(13, 12));
    try std.testing.expect(!profileSatisfies(14, 15));
    try std.testing.expect(!profileSatisfies(16, 15));
    try std.testing.expect(!profileSatisfies(15, 16));
}

const std = @import("std");
