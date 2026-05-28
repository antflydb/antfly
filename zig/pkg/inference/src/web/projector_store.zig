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
const gguf_format = @import("../gguf/format.zig");
const gguf_mod = @import("../gguf/root.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");

pub const ProjectorStore = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    raw_bytes: []u8,
    parsed: gguf_format.File,
    kind: projector_format_mod.Kind,

    pub fn initOwnedBytes(
        allocator: std.mem.Allocator,
        name_hint: []const u8,
        gguf_bytes: []const u8,
    ) !*ProjectorStore {
        const self = try allocator.create(ProjectorStore);
        errdefer allocator.destroy(self);

        const raw_bytes = try allocator.dupe(u8, gguf_bytes);
        errdefer allocator.free(raw_bytes);

        var parsed = try gguf_format.parse(allocator, raw_bytes);
        errdefer parsed.deinit(allocator);

        const kind = projector_format_mod.detectFile(&parsed);
        if (kind == .unknown) return error.UnsupportedProjector;

        self.* = .{
            .allocator = allocator,
            .name = try allocator.dupe(u8, name_hint),
            .raw_bytes = raw_bytes,
            .parsed = parsed,
            .kind = kind,
        };
        return self;
    }

    pub fn deinit(self: *ProjectorStore) void {
        self.parsed.deinit(self.allocator);
        self.allocator.free(self.raw_bytes);
        self.allocator.free(self.name);
        self.allocator.destroy(self);
    }

    pub fn asCompatGgufStore(self: *ProjectorStore) tensor_store_mod.GgufStore {
        return .{
            .allocator = self.allocator,
            .path = self.name,
            .mmap_region = null,
            .owned_bytes = self.raw_bytes,
            .parsed = self.parsed,
        };
    }
};

fn projectorFixtureBytes(allocator: std.mem.Allocator, kind: projector_format_mod.Kind) ![]u8 {
    const metadata: []const gguf_mod.format.MetadataEntry = switch (kind) {
        .termite_gemma3 => &[_]gguf_mod.format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "termite-projector" } },
            .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
        },
        .clip_gemma4_image => &[_]gguf_mod.format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
        },
        .clip_gemma4_audio => &[_]gguf_mod.format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4a" } },
        },
        .clip_gemma4_image_audio => &[_]gguf_mod.format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
            .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4a" } },
        },
        else => &[_]gguf_mod.format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "unknown" } },
        },
    };

    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    return allocator.dupe(u8, layout.header_bytes);
}

test "initOwnedBytes detects termite projector and exposes compat store" {
    const allocator = std.testing.allocator;
    const bytes = try projectorFixtureBytes(allocator, .termite_gemma3);
    defer allocator.free(bytes);

    const store = try ProjectorStore.initOwnedBytes(allocator, "fixture.mmproj.gguf", bytes);
    defer store.deinit();

    try std.testing.expectEqual(projector_format_mod.Kind.termite_gemma3, store.kind);
    try std.testing.expectEqualStrings("fixture.mmproj.gguf", store.name);

    var compat = store.asCompatGgufStore();
    try std.testing.expectEqualStrings("fixture.mmproj.gguf", compat.path.?);
    try std.testing.expectEqual(projector_format_mod.Kind.termite_gemma3, projector_format_mod.detectFile(&compat.parsed));
}

test "initOwnedBytes rejects unsupported projector metadata" {
    const allocator = std.testing.allocator;
    const bytes = try projectorFixtureBytes(allocator, .unknown);
    defer allocator.free(bytes);

    try std.testing.expectError(error.UnsupportedProjector, ProjectorStore.initOwnedBytes(allocator, "unknown.gguf", bytes));
}
