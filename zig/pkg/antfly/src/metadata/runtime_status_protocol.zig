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
/// v0.2.0 profile; V15 is the current profile. The intervening development
/// numbers were never released and must not be advertised, negotiated, read,
/// or written.
pub const legacy_record_version: u16 = 12;
pub const current_record_version: u16 = 15;

/// These facts form one current admission-safety profile. Keeping semantic
/// aliases makes call sites state why V15 is required without inventing
/// intermediate compatibility levels when new facts join that profile.
pub const repair_status_record_version: u16 = current_record_version;
pub const native_restore_identity_record_version: u16 = current_record_version;
pub const artifact_source_status_record_version: u16 = current_record_version;
pub const artifact_source_failure_status_record_version: u16 = current_record_version;

pub fn isSupported(version: u16) bool {
    return version >= 1 and version <= legacy_record_version or
        version == current_record_version;
}

pub fn isNegotiable(version: u16) bool {
    return version == legacy_record_version or version == current_record_version;
}

pub fn profileForAdvertisement(version: u16) ?u16 {
    if (version == current_record_version) return current_record_version;
    if (version >= 1 and version <= legacy_record_version) return legacy_record_version;
    return null;
}

test "runtime status exposes only released compatibility profiles" {
    const std = @import("std");
    try std.testing.expect(isSupported(1));
    try std.testing.expect(isSupported(legacy_record_version));
    try std.testing.expect(isSupported(current_record_version));
    try std.testing.expect(!isSupported(0));
    try std.testing.expect(!isSupported(13));
    try std.testing.expect(!isSupported(14));
    try std.testing.expect(!isSupported(16));

    try std.testing.expect(isNegotiable(legacy_record_version));
    try std.testing.expect(isNegotiable(current_record_version));
    try std.testing.expect(!isNegotiable(11));
    try std.testing.expect(!isNegotiable(13));
    try std.testing.expect(!isNegotiable(14));
    try std.testing.expectEqual(legacy_record_version, profileForAdvertisement(1).?);
    try std.testing.expectEqual(legacy_record_version, profileForAdvertisement(12).?);
    try std.testing.expect(profileForAdvertisement(13) == null);
    try std.testing.expect(profileForAdvertisement(14) == null);
    try std.testing.expectEqual(current_record_version, profileForAdvertisement(15).?);
}
