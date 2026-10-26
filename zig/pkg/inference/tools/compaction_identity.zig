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

//! Generate the context-compaction semantic source identity once as a cached
//! host build step. Target compilations import only the resulting 32 bytes.
const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;

const source_domain = "antfly.context-compaction.source-manifest.v1\x00";
const identity_version: u32 = 1;
const metal_begin_marker = "static void termite_metal_generated_pipeline_set_error";
const metal_end_marker = "\nint termite_metal_generated_pipeline_info_get(";
const registry_begin_marker = "// BEGIN RELEASE REGISTRY CONTENTS\n";
const registry_end_marker = "// END RELEASE REGISTRY CONTENTS\n";
const profile_tests_marker = "\ntest \"context compaction profiles reject";

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var hash = Sha256.init(.{});
    hash.update(source_domain);
    updateInt(&hash, u32, identity_version);

    var index: usize = 1;
    while (index < args.len) {
        const kind = args[index];
        index += 1;
        if (std.mem.eql(u8, kind, "--output")) {
            if (index + 1 != args.len) return error.InvalidArguments;
            var digest: [Sha256.digest_length]u8 = undefined;
            hash.final(&digest);
            try writeOutput(init, args[index], digest);
            return;
        }
        if (index + 2 > args.len) return error.InvalidArguments;
        const logical_name = args[index];
        const path = args[index + 1];
        index += 2;
        const source = try readSource(init, path);
        if (std.mem.eql(u8, kind, "source")) {
            updateSource(&hash, logical_name, source);
        } else if (std.mem.eql(u8, kind, "metal-section")) {
            try updateMetalSection(&hash, logical_name, source);
        } else if (std.mem.eql(u8, kind, "profile")) {
            try updateProfile(&hash, logical_name, source);
        } else return error.InvalidArguments;
    }
    return error.MissingOutput;
}

fn readSource(init: std.process.Init, path: []const u8) ![]const u8 {
    return std.Io.Dir.cwd().readFileAlloc(init.io, path, init.arena.allocator(), .limited(16 * 1024 * 1024));
}

fn writeOutput(init: std.process.Init, path: []const u8, digest: [32]u8) !void {
    var output: std.Io.Writer.Allocating = .init(init.arena.allocator());
    try output.writer.writeAll("pub const digest: [32]u8 = .{");
    for (digest, 0..) |byte, i| {
        if (i % 8 == 0) try output.writer.writeAll("\n    ");
        try output.writer.print("0x{x:0>2}, ", .{byte});
    }
    try output.writer.writeAll("\n};\n");
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = output.written() });
}

fn updateMetalSection(hash: *Sha256, logical_name: []const u8, source: []const u8) !void {
    const begin = try uniqueMarker(source, metal_begin_marker);
    const end = try uniqueMarker(source, metal_end_marker);
    if (end <= begin) return error.InvalidMarkerOrder;
    updateSource(hash, logical_name, source[begin..end]);
}

fn updateProfile(hash: *Sha256, logical_name: []const u8, source: []const u8) !void {
    const begin_marker = try uniqueMarker(source, registry_begin_marker);
    const begin = begin_marker + registry_begin_marker.len;
    const end = try uniqueMarker(source, registry_end_marker);
    const tests = try uniqueMarker(source, profile_tests_marker);
    if (end < begin or tests <= end) return error.InvalidMarkerOrder;

    updateInt(hash, u32, @intCast(logical_name.len));
    hash.update(logical_name);
    updateInt(hash, u64, @intCast(begin + tests - end));
    hash.update(source[0..begin]);
    hash.update(source[end..tests]);
}

fn uniqueMarker(source: []const u8, marker: []const u8) !usize {
    const first = std.mem.indexOf(u8, source, marker) orelse return error.MissingMarker;
    if (std.mem.indexOfPos(u8, source, first + marker.len, marker) != null) return error.DuplicateMarker;
    return first;
}

fn updateSource(hash: *Sha256, logical_name: []const u8, source: []const u8) void {
    updateInt(hash, u32, @intCast(logical_name.len));
    hash.update(logical_name);
    updateInt(hash, u64, @intCast(source.len));
    hash.update(source);
}

fn updateInt(hash: *Sha256, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hash.update(&bytes);
}

fn syntheticDigest(profile_source: []const u8, semantic_source: []const u8) ![32]u8 {
    var hash = Sha256.init(.{});
    hash.update(source_domain);
    updateInt(&hash, u32, identity_version);
    updateSource(&hash, "semantic.zig", semantic_source);
    try updateProfile(&hash, "runtime/kv/compaction_profile.zig", profile_source);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

test "context release registry contents do not change the source identity" {
    const prefix = "const semantic = 1;\n" ++ registry_begin_marker;
    const suffix = registry_end_marker ++ "pub fn select() void {}" ++ profile_tests_marker ++ " {}\n";
    const empty = prefix ++ "const release_profiles = &.{};\n" ++ suffix;
    const populated = prefix ++ "const release_profiles = &.{qualified};\n" ++ suffix;
    try std.testing.expectEqual(try syntheticDigest(empty, "fn compact() void {}"), try syntheticDigest(populated, "fn compact() void {}"));
}

test "context semantic source edits change the source identity" {
    const profile = "const semantic = 1;\n" ++ registry_begin_marker ++ "const release_profiles = &.{};\n" ++ registry_end_marker ++ "pub fn select() void {}" ++ profile_tests_marker ++ " {}\n";
    const before = try syntheticDigest(profile, "fn compact() void {}");
    const after = try syntheticDigest(profile, "fn compact() void { consume(); }");
    try std.testing.expect(!std.mem.eql(u8, &before, &after));
}

test "profile and metal markers are unique and ordered" {
    var hash = Sha256.init(.{});
    try std.testing.expectError(error.MissingMarker, updateProfile(&hash, "profile", "no markers"));
    try std.testing.expectError(error.DuplicateMarker, updateProfile(&hash, "profile", registry_begin_marker ++ registry_begin_marker ++ registry_end_marker ++ profile_tests_marker));
    try std.testing.expectError(error.InvalidMarkerOrder, updateProfile(&hash, "profile", registry_end_marker ++ registry_begin_marker ++ profile_tests_marker));
    try std.testing.expectError(error.MissingMarker, updateMetalSection(&hash, "metal", metal_begin_marker));
    try std.testing.expectError(error.InvalidMarkerOrder, updateMetalSection(&hash, "metal", metal_end_marker ++ metal_begin_marker));
}
