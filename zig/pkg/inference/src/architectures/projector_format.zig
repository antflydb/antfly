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
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_mod = @import("../gguf/root.zig");
const compat = @import("../io/compat.zig");

pub const Kind = enum {
    unknown,
    termite_gemma3,
    clip_gemma4_image,
    clip_gemma4_audio,
    clip_gemma4_image_audio,
};

pub fn detectPath(allocator: std.mem.Allocator, projector_path: []const u8) !Kind {
    const raw = try c_file.readFile(allocator, projector_path);
    defer allocator.free(raw);
    return detectBytes(allocator, raw);
}

pub fn detectBytes(allocator: std.mem.Allocator, raw: []const u8) !Kind {
    var parsed = try gguf_format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    return detectFile(&parsed);
}

pub fn detectFile(file: *const gguf_format.File) Kind {
    const view = gguf_metadata.View.init(file);
    const arch = view.getString("general.architecture") orelse return .unknown;

    if (std.mem.eql(u8, arch, "termite-projector")) {
        const source_arch = view.getString("termite.projector.source_architecture") orelse return .unknown;
        if (std.mem.eql(u8, source_arch, "gemma3")) return .termite_gemma3;
        return .unknown;
    }

    if (!std.mem.eql(u8, arch, "clip")) return .unknown;

    const has_image = blk: {
        const projector_type = view.getString("clip.vision.projector_type") orelse break :blk false;
        break :blk std.mem.eql(u8, projector_type, "gemma4v");
    };
    const has_audio = blk: {
        const projector_type = view.getString("clip.audio.projector_type") orelse break :blk false;
        break :blk std.mem.eql(u8, projector_type, "gemma4a");
    };

    if (has_image and has_audio) return .clip_gemma4_image_audio;
    if (has_image) return .clip_gemma4_image;
    if (has_audio) return .clip_gemma4_audio;
    return .unknown;
}

pub fn isTermite(kind: Kind) bool {
    return kind == .termite_gemma3;
}

pub fn isClip(kind: Kind) bool {
    return switch (kind) {
        .clip_gemma4_image, .clip_gemma4_audio, .clip_gemma4_image_audio => true,
        else => false,
    };
}

test "detect termite gemma3 projector" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-projector-format-gemma3.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "termite-projector" } },
        .{ .key = "termite.projector.source_architecture", .value = .{ .string = "gemma3" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.termite_gemma3, try detectPath(allocator, path));
}

test "detect clip gemma4 image projector" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-projector-format-clip-image.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.clip_gemma4_image, try detectPath(allocator, path));
}

test "detect unknown projector metadata" {
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ "/tmp", "termite-projector-format-unknown.gguf" });
    defer allocator.free(path);
    defer compat.cwd().deleteFile(compat.io(), path) catch {};

    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "something-else" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });

    try std.testing.expectEqual(Kind.unknown, try detectPath(allocator, path));
}
