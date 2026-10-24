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

//! Storage-independent schema admission contracts. Metadata, native owners and
//! backup manifests must use the same independent semantic and encoding axes.
//! Implementation support is not evidence that an owner installed a schema.
const std = @import("std");

pub const minimum_semantic_version: u32 = 11;
pub const current_semantic_version: u32 = 35;
pub const current_encoding_version: u32 = 35;

pub const ReaderCapabilities = struct {
    semantic_ceiling: u32 = 0,
    encoding_ceiling: u32 = 0,

    pub const current: ReaderCapabilities = .{
        .semantic_ceiling = current_semantic_version,
        .encoding_ceiling = current_encoding_version,
    };

    pub fn known(self: ReaderCapabilities) bool {
        return self.semantic_ceiling != 0 or self.encoding_ceiling != 0;
    }

    pub fn validForReporter(self: ReaderCapabilities, incarnation: u64) bool {
        if (!self.known()) return true;
        return incarnation != 0 and self.validProfile();
    }

    fn validProfile(self: ReaderCapabilities) bool {
        return self.semantic_ceiling >= minimum_semantic_version and self.semantic_ceiling <= current_semantic_version and
            self.encoding_ceiling >= minimum_semantic_version and self.encoding_ceiling <= current_encoding_version;
    }

    pub fn supports(self: ReaderCapabilities, requirements: ReaderRequirements) bool {
        return self.validProfile() and requirements.canDecodeFrame(self.encoding_ceiling, self.semantic_ceiling);
    }
};

/// The requirements of one immutable identity. Derivation from native schema
/// features belongs to the schema compiler, never to a client-supplied claim.
pub const ReaderRequirements = struct {
    semantic_version: u32,
    encoding_version: u32,

    pub fn valid(self: ReaderRequirements) bool {
        return self.semantic_version >= minimum_semantic_version and self.semantic_version <= current_semantic_version and
            self.encoding_version != 0 and self.encoding_version <= current_encoding_version;
    }

    pub fn canInterpret(self: ReaderRequirements, capability_ceiling: u32) bool {
        return self.semantic_version >= minimum_semantic_version and self.semantic_version <= capability_ceiling;
    }

    pub fn canDecodeFrame(self: ReaderRequirements, decoder_ceiling: u32, capability_ceiling: u32) bool {
        return self.encoding_version != 0 and self.encoding_version <= decoder_ceiling and self.canInterpret(capability_ceiling);
    }
};

/// Monotonic over every admitted schema, including retained historical layouts.
/// Zero/zero is an inactive floor, not an admissible schema identity.
pub const ReaderFloor = struct {
    semantic_version: u32 = 0,
    encoding_version: u32 = 0,

    pub fn active(self: ReaderFloor) bool {
        return self.semantic_version != 0 or self.encoding_version != 0;
    }

    pub fn valid(self: ReaderFloor) bool {
        return !self.active() or (ReaderRequirements{ .semantic_version = self.semantic_version, .encoding_version = self.encoding_version }).valid();
    }

    pub fn include(self: ReaderFloor, requirement: ReaderRequirements) !ReaderFloor {
        if (!self.valid() or !requirement.valid()) return error.InvalidSchemaReaderRequirements;
        return .{
            .semantic_version = @max(self.semantic_version, requirement.semantic_version),
            .encoding_version = @max(self.encoding_version, requirement.encoding_version),
        };
    }

    pub fn covers(self: ReaderFloor, requirement: ReaderRequirements) bool {
        return self.valid() and requirement.valid() and
            self.semantic_version >= requirement.semantic_version and self.encoding_version >= requirement.encoding_version;
    }

    pub fn admits(self: ReaderFloor, reader: ReaderCapabilities, reporter_incarnation: u64) bool {
        if (!self.valid() or !reader.validForReporter(reporter_incarnation)) return false;
        return !self.active() or reader.supports(.{ .semantic_version = self.semantic_version, .encoding_version = self.encoding_version });
    }
};

test "schema admission pure contract keeps semantic and physical requirements independent" {
    const requirement: ReaderRequirements = .{ .semantic_version = 30, .encoding_version = 17 };
    try std.testing.expect(requirement.valid());
    try std.testing.expect((ReaderCapabilities{ .semantic_ceiling = 30, .encoding_ceiling = 17 }).supports(requirement));
    try std.testing.expect(!(ReaderCapabilities{ .semantic_ceiling = 29, .encoding_ceiling = 31 }).supports(requirement));
    try std.testing.expect(!(ReaderCapabilities{ .semantic_ceiling = 31, .encoding_ceiling = 16 }).supports(requirement));
    const floor = try (try (ReaderFloor{}).include(requirement)).include(.{ .semantic_version = 13, .encoding_version = 31 });
    try std.testing.expectEqualDeep(ReaderFloor{ .semantic_version = 30, .encoding_version = 31 }, floor);
    try std.testing.expect(floor.covers(requirement));
    try std.testing.expect(!floor.admits(.{}, 7));
    try std.testing.expect(!floor.admits(.current, 0));
    try std.testing.expect(floor.admits(.current, 7));
    for ([_]ReaderRequirements{
        .{ .semantic_version = 0, .encoding_version = 0 },
        .{ .semantic_version = 10, .encoding_version = 31 },
        .{ .semantic_version = 30, .encoding_version = 0 },
        .{ .semantic_version = current_semantic_version + 1, .encoding_version = 31 },
        .{ .semantic_version = 30, .encoding_version = current_encoding_version + 1 },
    }) |invalid| {
        try std.testing.expect(!invalid.valid());
        try std.testing.expectError(error.InvalidSchemaReaderRequirements, floor.include(invalid));
        try std.testing.expect(!floor.covers(invalid));
    }
}
