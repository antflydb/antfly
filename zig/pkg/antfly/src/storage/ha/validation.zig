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

//! Shared HA input classifiers and primitive type predicates.
//!
//! Callers keep field-specific validation and error mapping local. These helpers
//! only classify missing/padded strings and provide reusable primitive checks so
//! CLI, runtime, admin, and operator-facing paths cannot drift.

const std = @import("std");

pub const StringValidation = enum {
    ok,
    missing,
    padded,
};

pub fn classifyString(value_or_null: ?[]const u8) StringValidation {
    const raw = value_or_null orelse return .missing;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == 0) return .missing;
    if (trimmed.len != raw.len) return .padded;
    return .ok;
}

pub fn isIdentifier(raw: []const u8) bool {
    if (raw.len == 0 or raw.len > 128) return false;
    for (raw) |byte| {
        if (!isIdentifierByte(byte)) return false;
    }
    return true;
}

pub fn isIdentifierByte(byte: u8) bool {
    return isEnvNameByte(byte) or byte == '-' or byte == '.' or byte == ':';
}

pub fn isEnvVarName(name: []const u8) bool {
    if (name.len == 0) return false;
    if (!isEnvNameFirstByte(name[0])) return false;
    for (name[1..]) |byte| {
        if (!isEnvNameByte(byte)) return false;
    }
    return true;
}

pub fn isEnvNameFirstByte(byte: u8) bool {
    return byte == '_' or (byte >= 'A' and byte <= 'Z') or (byte >= 'a' and byte <= 'z');
}

pub fn isEnvNameByte(byte: u8) bool {
    return isEnvNameFirstByte(byte) or (byte >= '0' and byte <= '9');
}

pub fn isNormalizedPath(path: []const u8) bool {
    if (path.len == 0) return false;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return false;
    if (std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) return false;
    if (std.mem.indexOf(u8, path, "//") != null) return false;
    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |part| {
        if (part.len == 0) continue;
        if (std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    }
    return true;
}

pub fn isAbsoluteNormalizedPath(path: []const u8) bool {
    return std.fs.path.isAbsolute(path) and isNormalizedPath(path);
}

test "storage.ha validation classifies missing padded and valid strings" {
    try std.testing.expectEqual(StringValidation.missing, classifyString(null));
    try std.testing.expectEqual(StringValidation.missing, classifyString(""));
    try std.testing.expectEqual(StringValidation.missing, classifyString(" \t\r\n"));
    try std.testing.expectEqual(StringValidation.padded, classifyString(" primary-a"));
    try std.testing.expectEqual(StringValidation.padded, classifyString("primary-a\n"));
    try std.testing.expectEqual(StringValidation.ok, classifyString("primary-a"));
}

test "storage.ha validation checks identifiers env names and normalized paths" {
    try std.testing.expect(isIdentifier("primary-a.1:zone"));
    try std.testing.expect(!isIdentifier("primary a"));
    try std.testing.expect(!isIdentifier(""));

    try std.testing.expect(isEnvVarName("ANTFLY_HA_ADMIN_TOKEN"));
    try std.testing.expect(isEnvVarName("_ANTFLY9"));
    try std.testing.expect(!isEnvVarName("9ANTFLY"));
    try std.testing.expect(!isEnvVarName("ANTFLY-HA"));

    try std.testing.expect(isAbsoluteNormalizedPath("/tmp/ha-primary.wal"));
    try std.testing.expect(!isAbsoluteNormalizedPath("ha/primary.wal"));
    try std.testing.expect(isNormalizedPath("ha/primary.wal"));
    try std.testing.expect(!isNormalizedPath("../ha-primary.wal"));
    try std.testing.expect(!isNormalizedPath("ha//primary.wal"));
    try std.testing.expect(!isNormalizedPath("."));
    try std.testing.expect(!isAbsoluteNormalizedPath("/tmp/../ha-primary.wal"));
}
