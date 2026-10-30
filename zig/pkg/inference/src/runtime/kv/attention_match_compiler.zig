// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bind native host numerical compilation alongside the Metal compiler and OS.
const std = @import("std");
const builtin = @import("builtin");

pub const Profile = struct {
    zig_version: []const u8,
    zig_backend: []const u8,
    optimize: []const u8,
    arch: []const u8,
    os: []const u8,
    os_versions: []const u8,
    abi: []const u8,
    cpu_model: []const u8,
    cpu_features: []const u8,
    strip: bool,
    error_tracing: bool,

    pub fn current() Profile {
        return .{
            .zig_version = builtin.zig_version_string,
            .zig_backend = @tagName(builtin.zig_backend),
            .optimize = @tagName(builtin.mode),
            .arch = @tagName(builtin.cpu.arch),
            .os = @tagName(builtin.os.tag),
            .os_versions = if (comptime builtin.os.tag.versionRangeTag() == .semver)
                std.fmt.comptimePrint("{f}:{f}", .{ builtin.os.version_range.semver.min, builtin.os.version_range.semver.max })
            else
                @tagName(builtin.os.tag.versionRangeTag()),
            .abi = @tagName(builtin.abi),
            .cpu_model = builtin.cpu.model.name,
            .cpu_features = builtin.cpu.features.asBytes(),
            .strip = builtin.strip_debug_info,
            .error_tracing = builtin.have_error_return_tracing,
        };
    }
};

pub fn fingerprint(metal: []const u8, profile: Profile) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.native-whole-head.compiler.v2\n");
    const fields = .{ "zig_version", "zig_backend", "optimize", "arch", "os", "os_versions", "abi", "cpu_model", "cpu_features" };
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, metal.len, .little);
    hash.update(&size);
    hash.update(metal);
    inline for (fields) |field| {
        const value = @field(profile, field);
        std.mem.writeInt(u64, &size, value.len, .little);
        hash.update(&size);
        hash.update(value);
    }
    hash.update(&.{ @intFromBool(profile.strip), @intFromBool(profile.error_tracing) });
    return hash.finalResult();
}

test "native certificate compiler identity rejects changed host compilation and Metal compiler" {
    const profile = Profile.current();
    const expected = fingerprint("fixture Metal compiler", profile);
    inline for (.{ "zig_version", "zig_backend", "optimize", "arch", "os", "os_versions", "abi", "cpu_model", "cpu_features" }) |field| {
        var changed = profile;
        @field(changed, field) = "substituted compiler field";
        try std.testing.expect(!std.mem.eql(u8, &expected, &fingerprint("fixture Metal compiler", changed)));
    }
    inline for (.{ "strip", "error_tracing" }) |field| {
        var changed = profile;
        @field(changed, field) = !@field(changed, field);
        try std.testing.expect(!std.mem.eql(u8, &expected, &fingerprint("fixture Metal compiler", changed)));
    }
    try std.testing.expect(!std.mem.eql(u8, &expected, &fingerprint("other Metal compiler", profile)));
}

test "native certificate compiler identity length framing separates adjacent fields" {
    var left = Profile.current();
    var right = left;
    left.zig_version = "ab";
    left.zig_backend = "c";
    right.zig_version = "a";
    right.zig_backend = "bc";
    try std.testing.expect(!std.mem.eql(u8, &fingerprint("Metal", left), &fingerprint("Metal", right)));
}
