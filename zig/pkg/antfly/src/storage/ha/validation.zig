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

pub const HAStringValidation = enum {
    ok,
    missing,
    padded,
};

pub fn classifyHAString(value_or_null: ?[]const u8) HAStringValidation {
    const raw = value_or_null orelse return .missing;
    var start: usize = 0;
    while (start < raw.len and isASCIIWhitespace(raw[start])) : (start += 1) {}
    var end: usize = raw.len;
    while (end > start and isASCIIWhitespace(raw[end - 1])) : (end -= 1) {}
    if (start == end) return .missing;
    if (start != 0 or end != raw.len) return .padded;
    return .ok;
}

pub fn isIdentifier(raw: []const u8) bool {
    if (raw.len == 0 or raw.len > 128) return false;
    if (std.mem.eql(u8, raw, ".") or std.mem.eql(u8, raw, "..")) return false;
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

pub fn isAbsoluteNormalizedPathWithinRoot(path: []const u8, root: []const u8) bool {
    if (!isAbsoluteNormalizedPath(path)) return false;
    if (!isAbsoluteNormalizedPath(root)) return false;
    if (std.mem.eql(u8, root, "/")) return true;
    if (std.mem.eql(u8, path, root)) return true;
    if (!std.mem.startsWith(u8, path, root)) return false;
    return path.len > root.len and path[root.len] == '/';
}

pub fn parseURLNoHiddenWhitespace(raw: []const u8) !std.Uri {
    if (containsASCIIWhitespace(raw)) return error.URLContainsWhitespace;
    return std.Uri.parse(raw) catch error.InvalidURL;
}

pub fn isURLWithHostNoHiddenWhitespace(raw: []const u8) bool {
    const uri = parseURLNoHiddenWhitespace(raw) catch return false;
    return uri.scheme.len > 0 and uri.host != null;
}

pub fn isHTTPURLWithHostNoHiddenWhitespace(raw: []const u8) bool {
    const uri = parseURLNoHiddenWhitespace(raw) catch return false;
    if (uri.host == null) return false;
    return std.mem.eql(u8, uri.scheme, "http") or std.mem.eql(u8, uri.scheme, "https");
}

pub fn containsASCIIWhitespace(raw: []const u8) bool {
    for (raw) |byte| {
        if (isASCIIWhitespace(byte)) return true;
    }
    return false;
}

fn isASCIIWhitespace(byte: u8) bool {
    return switch (byte) {
        ' ', '\t', '\n', '\r', 0x0b, 0x0c => true,
        else => false,
    };
}

test "storage.ha validation classifies missing padded and valid strings" {
    try std.testing.expectEqual(HAStringValidation.missing, classifyHAString(null));
    try std.testing.expectEqual(HAStringValidation.missing, classifyHAString(""));
    try std.testing.expectEqual(HAStringValidation.missing, classifyHAString(" \t\r\n"));
    try std.testing.expectEqual(HAStringValidation.missing, classifyHAString("\x0b\x0c"));
    try std.testing.expectEqual(HAStringValidation.padded, classifyHAString(" primary-a"));
    try std.testing.expectEqual(HAStringValidation.padded, classifyHAString("primary-a\n"));
    try std.testing.expectEqual(HAStringValidation.padded, classifyHAString("\x0bprimary-a"));
    try std.testing.expectEqual(HAStringValidation.padded, classifyHAString("primary-a\x0c"));
    try std.testing.expectEqual(HAStringValidation.ok, classifyHAString("primary-a"));
}

test "storage.ha validation checks identifiers env names and normalized paths" {
    try std.testing.expect(isIdentifier("primary-a.1:zone"));
    try std.testing.expect(!isIdentifier("primary a"));
    try std.testing.expect(!isIdentifier(""));
    try std.testing.expect(!isIdentifier("."));
    try std.testing.expect(!isIdentifier(".."));

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

test "storage.ha validation checks paths are bounded by an allowed root" {
    try std.testing.expect(isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly/ha/primary.wal", "/var/lib/antfly"));
    try std.testing.expect(isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly", "/var/lib/antfly"));
    try std.testing.expect(isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly/ha/primary.wal", "/"));
    try std.testing.expect(!isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly2/ha/primary.wal", "/var/lib/antfly"));
    try std.testing.expect(!isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly/../primary.wal", "/var/lib/antfly"));
    try std.testing.expect(!isAbsoluteNormalizedPathWithinRoot("ha/primary.wal", "/var/lib/antfly"));
    try std.testing.expect(!isAbsoluteNormalizedPathWithinRoot("/var/lib/antfly/ha/primary.wal", "var/lib/antfly"));
}

test "storage.ha validation parses URLs and rejects hidden whitespace" {
    const uri = try parseURLNoHiddenWhitespace("https://primary.antfly.svc:8080/admin/v1/ha");
    try std.testing.expectEqualStrings("https", uri.scheme);
    try std.testing.expect(uri.host != null);
    try std.testing.expect(isURLWithHostNoHiddenWhitespace("http://127.0.0.1:8080"));
    try std.testing.expect(isHTTPURLWithHostNoHiddenWhitespace("https://primary.antfly.svc:8080"));
    try std.testing.expect(!isURLWithHostNoHiddenWhitespace("http://primary antfly.svc:8080"));
    try std.testing.expect(!isURLWithHostNoHiddenWhitespace("http://primary.antfly.svc:8080/\tadmin"));
    try std.testing.expect(!isURLWithHostNoHiddenWhitespace("not-a-url"));
    try std.testing.expect(!isURLWithHostNoHiddenWhitespace("file:///tmp/primary"));
    try std.testing.expect(!isHTTPURLWithHostNoHiddenWhitespace("ftp://primary.antfly.svc:8080"));
    try std.testing.expect(!isHTTPURLWithHostNoHiddenWhitespace("file:///tmp/primary"));
}
