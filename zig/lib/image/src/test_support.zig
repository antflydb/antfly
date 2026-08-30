// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const manifest_rel_path = "testdata/image/manifest.zon";
pub const max_manifest_bytes = 128 * 1024;
pub const max_fixture_bytes = 16 * 1024 * 1024;

pub const Manifest = struct {
    version: u32,
    results: struct {
        success: []const u8,
        known_unsupported: []const u8,
        invalid: []const u8,
    },
    fixtures: []Fixture,

    pub const Fixture = struct {
        path: []const u8,
        format: []const u8,
        result: []const u8,
        width: ?u32 = null,
        height: ?u32 = null,
        pixel_format: ?[]const u8 = null,
        frames: ?u32 = null,
        frame_delays_ms: []const u32 = &.{},
        pixel_hashes: []const []const u8 = &.{},
        tags: []const []const u8 = &.{},
        notes: []const u8 = "",
    };
};

pub fn loadManifest(alloc: Allocator, io: anytype) !Manifest {
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, manifest_rel_path, alloc, .limited(max_manifest_bytes));
    defer alloc.free(raw);
    const source = try alloc.dupeSentinel(u8, raw, 0);
    defer alloc.free(source);
    return try std.zon.parse.fromSliceAlloc(Manifest, alloc, source, null, .{});
}

pub fn freeManifest(alloc: Allocator, manifest: Manifest) void {
    std.zon.parse.free(alloc, manifest);
}

pub fn fixtureRepoPathAlloc(alloc: Allocator, fixture_rel_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "testdata/image/{s}", .{fixture_rel_path});
}

pub fn readFixtureAlloc(alloc: Allocator, io: anytype, fixture_rel_path: []const u8) ![]u8 {
    const repo_path = try fixtureRepoPathAlloc(alloc, fixture_rel_path);
    defer alloc.free(repo_path);
    return try std.Io.Dir.cwd().readFileAlloc(io, repo_path, alloc, .limited(max_fixture_bytes));
}

pub fn findFixture(manifest: Manifest, fixture_rel_path: []const u8) ?Manifest.Fixture {
    for (manifest.fixtures) |fixture| {
        if (std.mem.eql(u8, fixture.path, fixture_rel_path)) return fixture;
    }
    return null;
}

pub fn sha256HexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return try std.fmt.allocPrint(alloc, "{s}", .{std.fmt.bytesToHex(hash, .lower)});
}

test "image manifest fixture files exist" {
    const alloc = std.testing.allocator;
    const manifest = try loadManifest(alloc, std.testing.io);
    defer freeManifest(alloc, manifest);

    try std.testing.expect(manifest.version >= 1);
    try std.testing.expect(manifest.fixtures.len > 0);

    for (manifest.fixtures) |fixture| {
        const bytes = try readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(bytes);
        try std.testing.expect(bytes.len > 0);
    }
}

test "image manifest entries satisfy core invariants" {
    const alloc = std.testing.allocator;
    const manifest = try loadManifest(alloc, std.testing.io);
    defer freeManifest(alloc, manifest);

    for (manifest.fixtures) |fixture| {
        const is_success = std.mem.eql(u8, fixture.result, manifest.results.success);
        const is_known_unsupported = std.mem.eql(u8, fixture.result, manifest.results.known_unsupported);
        const is_invalid = std.mem.eql(u8, fixture.result, manifest.results.invalid);

        try std.testing.expect(is_success or is_known_unsupported or is_invalid);
        try std.testing.expect(fixture.path.len > 0);
        try std.testing.expect(fixture.format.len > 0);

        if (is_success) {
            try std.testing.expect(fixture.width != null);
            try std.testing.expect(fixture.height != null);
            try std.testing.expect(fixture.pixel_format != null);
            try std.testing.expect(fixture.frames != null);
            try std.testing.expect(fixture.frames.? >= 1);
            try std.testing.expectEqual(@as(usize, fixture.frames.?), fixture.pixel_hashes.len);
            if (fixture.frames.? == 1) {
                try std.testing.expectEqual(@as(usize, 0), fixture.frame_delays_ms.len);
            } else {
                try std.testing.expectEqual(@as(usize, fixture.frames.?), fixture.frame_delays_ms.len);
            }
        } else if (is_known_unsupported) {
            if (fixture.width) |width| try std.testing.expect(width >= 1);
            if (fixture.height) |height| try std.testing.expect(height >= 1);
            if (fixture.frames) |frames| {
                try std.testing.expect(frames >= 1);
                try std.testing.expect(fixture.pixel_format != null);
                if (fixture.pixel_hashes.len != 0) {
                    try std.testing.expectEqual(@as(usize, frames), fixture.pixel_hashes.len);
                }
                if (frames == 1) {
                    try std.testing.expectEqual(@as(usize, 0), fixture.frame_delays_ms.len);
                } else if (fixture.frame_delays_ms.len != 0) {
                    try std.testing.expectEqual(@as(usize, frames), fixture.frame_delays_ms.len);
                }
            } else {
                try std.testing.expectEqual(@as(usize, 0), fixture.frame_delays_ms.len);
                try std.testing.expectEqual(@as(usize, 0), fixture.pixel_hashes.len);
            }
        } else {
            try std.testing.expect(fixture.width == null);
            try std.testing.expect(fixture.height == null);
            try std.testing.expect(fixture.pixel_format == null);
            try std.testing.expect(fixture.frames == null);
            try std.testing.expectEqual(@as(usize, 0), fixture.frame_delays_ms.len);
            try std.testing.expectEqual(@as(usize, 0), fixture.pixel_hashes.len);
        }
    }
}
