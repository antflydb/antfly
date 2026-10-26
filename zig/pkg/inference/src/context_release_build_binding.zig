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

//! Minimal runtime binding for a parent-audited full release receipt. The exact
//! receipt SHA remains the source-build identity. Runtime parses only immutable
//! scope/status, executable identity and v2/v3 compiler profile; Python independently
//! audits the bound archive, toolchain, command, resource samples, logs and tests.
const std = @import("std");
const builtin = @import("builtin");

const Binary = struct { path: []const u8, sha256: []const u8, bytes: u64 };
const Receipt = struct {
    schema: []const u8,
    evidence_scope: []const u8,
    release_eligible: bool,
    serving_qualified: bool,
    source_verified_after_build: bool,
    binary: Binary,
    compiler_profile: ?std.json.Value = null,
};

const CompilerProfile = struct { strip_debug_info: bool, have_error_return_tracing: bool };

pub const Binding = struct { binary_sha256: [32]u8 };

pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !Binding {
    return parseWithCompilerProfile(allocator, bytes, .{
        .strip_debug_info = builtin.strip_debug_info,
        .have_error_return_tracing = builtin.have_error_return_tracing,
    });
}

fn parseWithCompilerProfile(allocator: std.mem.Allocator, bytes: []const u8, compiler: CompilerProfile) !Binding {
    var parsed = std.json.parseFromSlice(Receipt, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return error.InvalidContextReleaseBuildReceipt;
    defer parsed.deinit();
    const value = parsed.value;
    const v1 = std.mem.eql(u8, value.schema, "antfly.context-release-build.v1");
    const v2 = std.mem.eql(u8, value.schema, "antfly.context-release-build.v2");
    const v3 = std.mem.eql(u8, value.schema, "antfly.context-release-build.v3");
    if ((!v1 and !v2 and !v3) or
        !std.mem.eql(u8, value.evidence_scope, "full_context_release_build") or
        !value.release_eligible or value.serving_qualified or !value.source_verified_after_build or
        value.binary.bytes == 0 or !validRelative(value.binary.path))
        return error.InvalidContextReleaseBuildReceipt;
    if (v2 or v3) {
        const profile = value.compiler_profile orelse return error.InvalidContextReleaseBuildReceipt;
        if (profile != .object or profile.object.count() != 2)
            return error.InvalidContextReleaseBuildReceipt;
        const strip = profile.object.get("strip_debug_info") orelse return error.InvalidContextReleaseBuildReceipt;
        const tracing = profile.object.get("have_error_return_tracing") orelse return error.InvalidContextReleaseBuildReceipt;
        // V2/V3 are canonical stripped profiles. V3 removes the historical
        // build-option shape that Python audits; the runtime compiler contract
        // is intentionally identical. Require the actual executable's settings
        // as well as the parent-audited receipt's booleans.
        if (strip != .bool or tracing != .bool or !strip.bool or tracing.bool or
            strip.bool != compiler.strip_debug_info or tracing.bool != compiler.have_error_return_tracing)
            return error.InvalidContextReleaseBuildReceipt;
    }
    return .{ .binary_sha256 = digest32(value.binary.sha256) catch
        return error.InvalidContextReleaseBuildReceipt };
}

fn validRelative(value: []const u8) bool {
    if (value.len == 0 or value.len > 1024 or value[0] == '/') return false;
    var parts = std.mem.splitScalar(u8, value, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}

fn digest32(value: []const u8) ![32]u8 {
    if (value.len != 64) return error.InvalidDigest;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, value) catch return error.InvalidDigest;
    return result;
}

test "context full release runtime binding rejects historical and scoped receipts" {
    const good =
        \\{"schema":"antfly.context-release-build.v1","evidence_scope":"full_context_release_build","release_eligible":true,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7},"archive":{"ignored":true}}
    ;
    const binding = try parse(std.testing.allocator, good);
    try std.testing.expectEqual(@as(u8, 0x11), binding.binary_sha256[0]);
    const old =
        \\{"binary_sha256":"1111111111111111111111111111111111111111111111111111111111111111","source_verified_after_build":true}
    ;
    try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parse(std.testing.allocator, old));
    const scoped =
        \\{"schema":"antfly.context-release-build.v1","evidence_scope":"numerical_capture_and_metal_lifecycle_only","release_eligible":false,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7}}
    ;
    try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parse(std.testing.allocator, scoped));
}

const v2_test_prefix =
    \\{"schema":"antfly.context-release-build.v2","evidence_scope":"full_context_release_build","release_eligible":true,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7}
;
const v3_test_prefix =
    \\{"schema":"antfly.context-release-build.v3","evidence_scope":"full_context_release_build","release_eligible":true,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7}
;
const v2_test_profile =
    \\,"compiler_profile":{"strip_debug_info":true,"have_error_return_tracing":false}
;

test "context full release runtime binding v2 matches actual compiler profile" {
    const receipt = v2_test_prefix ++ v2_test_profile ++ "}";
    const binding = try parseWithCompilerProfile(std.testing.allocator, receipt, .{
        .strip_debug_info = true,
        .have_error_return_tracing = false,
    });
    try std.testing.expectEqual(@as(u8, 0x11), binding.binary_sha256[0]);
    if (builtin.strip_debug_info and !builtin.have_error_return_tracing) {
        _ = try parse(std.testing.allocator, receipt);
    } else {
        try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parse(std.testing.allocator, receipt));
    }
    for ([_]CompilerProfile{
        .{ .strip_debug_info = false, .have_error_return_tracing = false },
        .{ .strip_debug_info = false, .have_error_return_tracing = true },
        .{ .strip_debug_info = true, .have_error_return_tracing = true },
    }) |compiler| {
        try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parseWithCompilerProfile(std.testing.allocator, receipt, compiler));
    }
}

test "context full release runtime binding v3 matches stripped compiler profile" {
    const receipt = v3_test_prefix ++ v2_test_profile ++ "}";
    const binding = try parseWithCompilerProfile(std.testing.allocator, receipt, .{
        .strip_debug_info = true,
        .have_error_return_tracing = false,
    });
    try std.testing.expectEqual(@as(u8, 0x11), binding.binary_sha256[0]);
    if (builtin.strip_debug_info and !builtin.have_error_return_tracing) {
        _ = try parse(std.testing.allocator, receipt);
    } else {
        try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parse(std.testing.allocator, receipt));
    }
    for ([_]CompilerProfile{
        .{ .strip_debug_info = false, .have_error_return_tracing = false },
        .{ .strip_debug_info = false, .have_error_return_tracing = true },
        .{ .strip_debug_info = true, .have_error_return_tracing = true },
    }) |compiler| {
        try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parseWithCompilerProfile(std.testing.allocator, receipt, compiler));
    }
    try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parseWithCompilerProfile(std.testing.allocator, v3_test_prefix ++ "}", .{
        .strip_debug_info = true,
        .have_error_return_tracing = false,
    }));
}

test "context full release runtime binding rejects unknown future schema" {
    const receipt =
        \\{"schema":"antfly.context-release-build.v4","evidence_scope":"full_context_release_build","release_eligible":true,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7},"compiler_profile":{"strip_debug_info":true,"have_error_return_tracing":false}}
    ;
    try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parseWithCompilerProfile(std.testing.allocator, receipt, .{
        .strip_debug_info = true,
        .have_error_return_tracing = false,
    }));
}

test "context full release runtime binding v2 and v3 reject absent malformed and noncanonical profile" {
    const invalid_profiles = [_][]const u8{
        "",
        \\,"compiler_profile":null
        ,
        \\,"compiler_profile":[]
        ,
        \\,"compiler_profile":{"strip_debug_info":true}
        ,
        \\,"compiler_profile":{"strip_debug_info":1,"have_error_return_tracing":false}
        ,
        \\,"compiler_profile":{"strip_debug_info":true,"have_error_return_tracing":0}
        ,
        \\,"compiler_profile":{"strip_debug_info":"true","have_error_return_tracing":false}
        ,
        \\,"compiler_profile":{"strip_debug_info":false,"have_error_return_tracing":false}
        ,
        \\,"compiler_profile":{"strip_debug_info":true,"have_error_return_tracing":true}
        ,
        \\,"compiler_profile":{"strip_debug_info":true,"have_error_return_tracing":false,"extra":false}
        ,
    };
    for ([_][]const u8{ v2_test_prefix, v3_test_prefix }) |prefix| {
        for (invalid_profiles) |profile| {
            const receipt = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}}}", .{ prefix, profile });
            defer std.testing.allocator.free(receipt);
            try std.testing.expectError(error.InvalidContextReleaseBuildReceipt, parseWithCompilerProfile(std.testing.allocator, receipt, .{
                .strip_debug_info = true,
                .have_error_return_tracing = false,
            }));
        }
    }
}

test "context full release runtime binding v1 preserves unknown profile fields" {
    const receipt =
        \\{"schema":"antfly.context-release-build.v1","evidence_scope":"full_context_release_build","release_eligible":true,"serving_qualified":false,"source_verified_after_build":true,"binary":{"path":"context-tests","sha256":"1111111111111111111111111111111111111111111111111111111111111111","bytes":7},"compiler_profile":{"future_field":42}}
    ;
    _ = try parse(std.testing.allocator, receipt);
}
