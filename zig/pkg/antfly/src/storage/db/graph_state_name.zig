// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_ids = @import("artifact_ids.zig");
const types = @import("types.zig");

const magic = "GSN1";

pub const Role = enum {
    artifact,
    resolution_mentions,
    resolution_mention_artifacts,
};

pub const Identity = struct {
    source_artifact: []const u8,
    role: Role,
};

fn appendComponent(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: []const u8) !void {
    if (value.len > std.math.maxInt(u32)) return error.GraphStateNameComponentTooLarge;
    var len_bytes: [@sizeOf(u32)]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(value.len), .big);
    try list.appendSlice(alloc, &len_bytes);
    try list.appendSlice(alloc, value);
}

fn init(alloc: Allocator, source_artifact: []const u8) !std.ArrayListUnmanaged(u8) {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, magic);
    try appendComponent(&list, alloc, source_artifact);
    return list;
}

fn readComponent(state_name: []const u8, pos: *usize) ![]const u8 {
    if (state_name.len - pos.* < @sizeOf(u32)) return error.InvalidGraphStateName;
    const component_len = std.mem.readInt(u32, state_name[pos.*..][0..@sizeOf(u32)], .big);
    pos.* += @sizeOf(u32);
    if (component_len > state_name.len - pos.*) return error.InvalidGraphStateName;
    const component = state_name[pos.*..][0..component_len];
    pos.* += component_len;
    return component;
}

pub fn identity(state_name: []const u8) !?Identity {
    if (!std.mem.startsWith(u8, state_name, magic)) return null;
    var pos: usize = magic.len;
    const source_artifact = try readComponent(state_name, &pos);
    const role_name = try readComponent(state_name, &pos);
    const role = std.meta.stringToEnum(Role, role_name) orelse return error.InvalidGraphStateName;
    return .{ .source_artifact = source_artifact, .role = role };
}

pub fn sourceArtifact(state_name: []const u8) !?[]const u8 {
    return if (try identity(state_name)) |parsed| parsed.source_artifact else null;
}

/// Resolves a v0.2.0 separator-delimited state name to its source priority.
/// Longest-match selection is required because released source names may
/// themselves contain the legacy separator.
pub fn legacySourcePriority(state_name: []const u8, sources: anytype) ?usize {
    var best: ?usize = null;
    var best_len: usize = 0;
    for (sources, 0..) |source, i| {
        if (source.artifact_name.len < best_len or !std.mem.startsWith(u8, state_name, source.artifact_name)) continue;
        const boundary_matches = source.artifact_name.len == state_name.len or
            (source.artifact_name.len < state_name.len and state_name[source.artifact_name.len] == '\x1f');
        if (!boundary_matches) continue;
        if (best == null or source.artifact_name.len > best_len) {
            best = i;
            best_len = source.artifact_name.len;
        }
    }
    return best;
}

pub fn artifactAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
    var list = try init(alloc, artifact_ref.name);
    errdefer list.deinit(alloc);
    try appendComponent(&list, alloc, "artifact");
    try appendComponent(&list, alloc, @tagName(artifact_ref.kind));
    if (artifact_ref.unit_id) |unit_id| {
        try appendComponent(&list, alloc, "unit");
        try appendComponent(&list, alloc, unit_id);
    }
    if (artifact_ref.chunk_id) |chunk_id| {
        var chunk_bytes: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &chunk_bytes, chunk_id, .big);
        try appendComponent(&list, alloc, "chunk");
        try appendComponent(&list, alloc, &chunk_bytes);
    }
    if (artifact_ref.source) |source| {
        const source_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, .{
            .document_id = artifact_ref.document_id,
            .name = source.name,
            .kind = source.kind,
            .chunk_id = source.chunk_id,
            .unit_id = source.unit_id,
        });
        defer alloc.free(source_key);
        try appendComponent(&list, alloc, "source");
        try appendComponent(&list, alloc, source_key);
    }
    return try list.toOwnedSlice(alloc);
}

/// Returns the only released/pre-canonical state spelling for an artifact
/// member. It is used exclusively to read and retire migration aliases.
pub fn legacyArtifactAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
    const document_wide = artifact_ref.kind == .asset and artifact_ref.unit_id == null and
        artifact_ref.chunk_id == null and artifact_ref.source == null;
    if (document_wide) return try alloc.dupe(u8, artifact_ref.name);

    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, artifact_ref.name);
    try list.append(alloc, '\x1f');
    try list.appendSlice(alloc, @tagName(artifact_ref.kind));
    if (artifact_ref.unit_id) |unit_id| {
        try list.appendSlice(alloc, "\x1funit:");
        try list.appendSlice(alloc, unit_id);
    }
    if (artifact_ref.chunk_id) |chunk_id| {
        const chunk_component = try std.fmt.allocPrint(alloc, "\x1fchunk:{d}", .{chunk_id});
        defer alloc.free(chunk_component);
        try list.appendSlice(alloc, chunk_component);
    }
    return try list.toOwnedSlice(alloc);
}

pub fn mentionAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    var list = try init(alloc, source_artifact);
    errdefer list.deinit(alloc);
    try appendComponent(&list, alloc, "resolution_mentions");
    try appendComponent(&list, alloc, resolution_artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn legacyMentionAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}\x1fresolution_mentions\x1f{s}", .{ source_artifact, resolution_artifact });
}

pub fn mentionArtifactAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    var list = try init(alloc, source_artifact);
    errdefer list.deinit(alloc);
    try appendComponent(&list, alloc, "resolution_mention_artifacts");
    try appendComponent(&list, alloc, resolution_artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn legacyMentionArtifactAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}\x1fresolution_mention_artifacts\x1f{s}", .{ source_artifact, resolution_artifact });
}

test "canonical graph state names retain arbitrary source names" {
    const alloc = std.testing.allocator;
    const name = try mentionAlloc(alloc, "relations\x1fwest", "resolution:v1");
    defer alloc.free(name);
    try std.testing.expectEqualStrings("relations\x1fwest", (try sourceArtifact(name)).?);
    try std.testing.expectEqual(Role.resolution_mentions, (try identity(name)).?.role);
}

test "v0.2.0 graph state names prefer the longest source boundary" {
    const Source = struct { artifact_name: []const u8 };
    const sources = [_]Source{
        .{ .artifact_name = "relations" },
        .{ .artifact_name = "relations\x1fwest" },
        .{ .artifact_name = "relation" },
    };
    try std.testing.expectEqual(@as(?usize, 1), legacySourcePriority("relations\x1fwest\x1fresolution_mentions", &sources));
    try std.testing.expectEqual(@as(?usize, null), legacySourcePriority("relations-west", &sources));
}
