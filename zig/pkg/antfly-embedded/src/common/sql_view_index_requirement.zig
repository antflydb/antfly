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

//! Immutable desired-index identity. Existence/readiness is proved by the
//! actual physical owner at the same retained primary read cut, not by this
//! serialized header alone. No callbacks or credentials cross this boundary.
const std = @import("std");
pub const max_indexes = 16;
pub const Kind = enum(u8) { full_text = 1 };
pub const Requirement = struct {
    name: []const u8,
    kind: Kind = .full_text,
    incarnation: u64,
    config_hash: u64,

    pub fn validate(self: Requirement) !void {
        if (self.incarnation == 0 or self.name.len == 0 or self.name.len > 128 or
            std.mem.indexOfScalar(u8, self.name, 0) != null or !std.unicode.utf8ValidateSlice(self.name)) return error.InvalidSqlViewRecord;
    }
    pub fn eql(self: Requirement, other: Requirement) bool {
        return self.kind == other.kind and self.incarnation == other.incarnation and
            self.config_hash == other.config_hash and std.mem.eql(u8, self.name, other.name);
    }
};
pub fn validate(requirements: []const Requirement) !void {
    if (requirements.len > max_indexes) return error.InvalidSqlViewRecord;
    for (requirements, 0..) |required, position| {
        try required.validate();
        for (requirements[0..position]) |prior| if (std.mem.eql(u8, prior.name, required.name)) return error.InvalidSqlViewRecord;
    }
}

test "SQL view index requirements reject duplicate names and distinguish same-config logical drop recreations" {
    const first: Requirement = .{ .name = "support_fts", .incarnation = 9, .config_hash = 33 };
    var next = first;
    next.incarnation += 1;
    try std.testing.expect(!first.eql(next));
    try std.testing.expectError(error.InvalidSqlViewRecord, validate(&.{ first, first }));
    next.incarnation = 0;
    try std.testing.expectError(error.InvalidSqlViewRecord, next.validate());
}

pub fn digest(requirements: []const Requirement) !?[32]u8 {
    try validate(requirements);
    if (requirements.len == 0) return null;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:sql-view-required-indexes:deferred-read-v2\x00");
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, requirements.len, .little);
    hash.update(&encoded);
    for (requirements) |entry| {
        std.mem.writeInt(u64, &encoded, entry.name.len, .little);
        hash.update(&encoded);
        hash.update(entry.name);
        hash.update(&.{@backingInt(entry.kind)});
        std.mem.writeInt(u64, &encoded, entry.incarnation, .little);
        hash.update(&encoded);
        std.mem.writeInt(u64, &encoded, entry.config_hash, .little);
        hash.update(&encoded);
    }
    return hash.finalResult();
}

pub fn requireDigest(expected: []const Requirement, actual: ?[32]u8) !void {
    if (!std.meta.eql(try digest(expected), actual)) return error.UnsupportedSqlExecution;
}
