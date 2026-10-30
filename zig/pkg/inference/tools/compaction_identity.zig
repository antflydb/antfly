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
const metal_release_begin_marker = "// BEGIN METAL CHECKPOINT RELEASE REGISTRY\n";
const metal_release_end_marker = "// END METAL CHECKPOINT RELEASE REGISTRY\n";

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var hash = Sha256.init(.{});
    hash.update(source_domain);
    updateInt(&hash, u32, identity_version);
    var environment_keys: std.StringHashMapUnmanaged(void) = .empty;
    // Accelerate and other BLAS implementations can read their own thread
    // controls without passing through a runtime source helper.
    inline for (.{ "VECLIB_MAXIMUM_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS" }) |key|
        try environment_keys.put(allocator, key, {});

    var index: usize = 1;
    while (index < args.len) {
        const kind = args[index];
        index += 1;
        if (std.mem.eql(u8, kind, "--output")) {
            if (index + 1 != args.len) return error.InvalidArguments;
            var digest: [Sha256.digest_length]u8 = undefined;
            hash.final(&digest);
            try writeOutput(init, args[index], digest, &environment_keys);
            return;
        }
        if (index + 2 > args.len) return error.InvalidArguments;
        const logical_name = args[index];
        const path = args[index + 1];
        index += 2;
        const source = try readSource(init, path);
        if (std.mem.eql(u8, kind, "source")) {
            updateSource(&hash, logical_name, source);
            // Driver input/output paths do not configure the native backend.
            // Runtime sources include all their literal environment names,
            // including names passed through compile-time helper functions.
            if (!std.mem.startsWith(u8, logical_name, "context_") and
                !std.mem.startsWith(u8, logical_name, "backends/context_") and
                !std.mem.startsWith(u8, logical_name, "tools/") and
                !std.mem.startsWith(u8, logical_name, "build/") and
                !std.mem.startsWith(u8, logical_name, "tokenizer/"))
                try collectEnvironmentKeys(allocator, &environment_keys, source);
        } else if (std.mem.eql(u8, kind, "metal-section")) {
            try updateMetalSection(&hash, logical_name, source);
        } else if (std.mem.eql(u8, kind, "profile")) {
            try updateProfile(&hash, logical_name, source);
        } else if (std.mem.eql(u8, kind, "metal-release")) {
            try updateMetalRelease(&hash, logical_name, source);
        } else return error.InvalidArguments;
    }
    return error.MissingOutput;
}

/// Bind the complete authorization implementation while excluding only the
/// certificate table whose entries contain this digest.
fn updateMetalRelease(hash: *Sha256, logical_name: []const u8, source: []const u8) !void {
    const begin_marker = try uniqueMarker(source, metal_release_begin_marker);
    const begin = begin_marker + metal_release_begin_marker.len;
    const end = try uniqueMarker(source, metal_release_end_marker);
    if (end < begin) return error.InvalidMarkerOrder;

    updateInt(hash, u32, @intCast(logical_name.len));
    hash.update(logical_name);
    updateInt(hash, u64, @intCast(source.len - (end - begin)));
    hash.update(source[0..begin]);
    hash.update(source[end..]);
}

fn readSource(init: std.process.Init, path: []const u8) ![]const u8 {
    return std.Io.Dir.cwd().readFileAlloc(init.io, path, init.arena.allocator(), .limited(16 * 1024 * 1024));
}

fn writeOutput(init: std.process.Init, path: []const u8, digest: [32]u8, environment_keys: *const std.StringHashMapUnmanaged(void)) !void {
    var output: std.Io.Writer.Allocating = .init(init.arena.allocator());
    try output.writer.writeAll("pub const digest: [32]u8 = .{");
    for (digest, 0..) |byte, i| {
        if (i % 8 == 0) try output.writer.writeAll("\n    ");
        try output.writer.print("0x{x:0>2}, ", .{byte});
    }
    try output.writer.writeAll("\n};\n");
    var keys: std.ArrayList([]const u8) = .empty;
    var iterator = environment_keys.keyIterator();
    while (iterator.next()) |key| try keys.append(init.arena.allocator(), key.*);
    std.mem.sort([]const u8, keys.items, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.less);
    try output.writer.writeAll("pub const backend_environment_keys: []const [*:0]const u8 = &.{\n");
    for (keys.items) |key| try output.writer.print("    \"{s}\",\n", .{key});
    try output.writer.writeAll("};\n");
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = output.written() });
}

fn isBackendEnvironmentKey(key: []const u8) bool {
    if (!std.mem.startsWith(u8, key, "ANTFLY_") and !std.mem.startsWith(u8, key, "TERMITE_")) return false;
    for (key) |byte| if (!(byte >= 'A' and byte <= 'Z') and !(byte >= '0' and byte <= '9') and byte != '_') return false;
    inline for (.{ "ANTFLY_METAL_CHECKPOINT_", "ANTFLY_METAL_NATIVE_QUALITY_", "ANTFLY_METAL_QUALITY_" }) |prefix|
        if (std.mem.startsWith(u8, key, prefix)) return false;
    // These switches are certificate-owned typed Execution fields. Ignoring
    // their environment spelling lets serving apply the qualified settings
    // directly, without requiring qualification-only opt-ins in deployment.
    inline for (.{ "FORWARD_FRAME", "PRECOMPUTED_TAIL", "RETAIN_HIDDEN", "MATRIX_STAGE", "COALESCED_STAGE", "PLE_TAIL", "RESOURCE_BARRIERS", "CONCURRENT_FRAME" }) |suffix|
        if (std.mem.eql(u8, key, "ANTFLY_METAL_ATTENTION_MATCH_" ++ suffix)) return false;
    return true;
}

fn collectEnvironmentKeys(allocator: std.mem.Allocator, keys: *std.StringHashMapUnmanaged(void), source: []const u8) !void {
    var pieces = std.mem.tokenizeScalar(u8, source, '"');
    while (pieces.next()) |piece| if (isBackendEnvironmentKey(piece)) try keys.put(allocator, piece, {});
}

test "context compaction identity captures runtime environment names without driver or typed switches" {
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(std.testing.allocator);
    try collectEnvironmentKeys(
        std.testing.allocator,
        &keys,
        "\"TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH\" \"ANTFLY_METAL_Q8_OPTIMIZATIONS\" \"ANTFLY_METAL_Q8_OPTIMIZATIONS\" \"ANTFLY_METAL_CHECKPOINT_READY_FILE\" \"ANTFLY_METAL_NATIVE_QUALITY_INPUT\" \"ANTFLY_METAL_ATTENTION_MATCH_MATRIX_STAGE\" \"ANTFLY_lowercase\"",
    );
    try std.testing.expectEqual(@as(usize, 2), keys.count());
    try std.testing.expect(keys.contains("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH"));
    try std.testing.expect(keys.contains("ANTFLY_METAL_Q8_OPTIMIZATIONS"));
    try std.testing.expect(isBackendEnvironmentKey("ANTFLY_METAL_ATTENTION_MATCH_NEW_RUNTIME_SWITCH"));
    try std.testing.expect(isBackendEnvironmentKey("TERMITE_DISABLE_SLIDING_ATTENTION"));
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

fn syntheticMetalReleaseDigest(release_source: []const u8) ![32]u8 {
    var hash = Sha256.init(.{});
    hash.update(source_domain);
    updateInt(&hash, u32, identity_version);
    try updateMetalRelease(&hash, "runtime/kv/attention_match_release.zig", release_source);
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

test "attention_match Metal certificate enrollment is excluded but release semantics are bound" {
    const prefix = "pub fn validate() void {}\n" ++ metal_release_begin_marker;
    const suffix = metal_release_end_marker ++ "pub fn select() void {}\ntest \"attention_match release\" {}\n";
    const empty = prefix ++ "const certificates = &.{};\n" ++ suffix;
    const populated = prefix ++ "const certificates = &.{qualified};\n" ++ suffix;
    try std.testing.expectEqual(try syntheticMetalReleaseDigest(empty), try syntheticMetalReleaseDigest(populated));
    const changed = metal_release_end_marker ++ "pub fn select() void { enforce(); }\ntest \"attention_match release\" {}\n";
    try std.testing.expect(!std.mem.eql(u8, &try syntheticMetalReleaseDigest(empty), &try syntheticMetalReleaseDigest(prefix ++ "const certificates = &.{};\n" ++ changed)));
}

test "profile and metal markers are unique and ordered" {
    var hash = Sha256.init(.{});
    try std.testing.expectError(error.MissingMarker, updateProfile(&hash, "profile", "no markers"));
    try std.testing.expectError(error.DuplicateMarker, updateProfile(&hash, "profile", registry_begin_marker ++ registry_begin_marker ++ registry_end_marker ++ profile_tests_marker));
    try std.testing.expectError(error.InvalidMarkerOrder, updateProfile(&hash, "profile", registry_end_marker ++ registry_begin_marker ++ profile_tests_marker));
    try std.testing.expectError(error.MissingMarker, updateMetalSection(&hash, "metal", metal_begin_marker));
    try std.testing.expectError(error.InvalidMarkerOrder, updateMetalSection(&hash, "metal", metal_end_marker ++ metal_begin_marker));
    try std.testing.expectError(error.MissingMarker, updateMetalRelease(&hash, "release", "no markers"));
    try std.testing.expectError(error.DuplicateMarker, updateMetalRelease(&hash, "release", metal_release_begin_marker ++ metal_release_begin_marker ++ metal_release_end_marker));
    try std.testing.expectError(error.InvalidMarkerOrder, updateMetalRelease(&hash, "release", metal_release_end_marker ++ metal_release_begin_marker));
}
