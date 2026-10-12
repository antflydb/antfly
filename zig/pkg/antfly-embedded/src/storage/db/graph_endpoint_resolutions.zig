// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Shared validation of landed resolution endpoints during graph replay.
const std = @import("std");

pub fn append(a: std.mem.Allocator, map: *std.json.ObjectMap, raw: []const u8) !void {
    const root = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidResolutionArtifact,
    };
    if (root != .object) return error.InvalidResolutionArtifact;
    const entities = root.object.get("entities") orelse return error.InvalidResolutionArtifact;
    if (entities != .array) return error.InvalidResolutionArtifact;
    for (entities.array.items) |entity| {
        if (entity != .object) return error.InvalidResolutionArtifact;
        const decision = entity.object.get("decision") orelse return error.InvalidResolutionArtifact;
        if (decision != .string) return error.InvalidResolutionArtifact;
        if (!std.mem.eql(u8, decision.string, "new") and !std.mem.eql(u8, decision.string, "match")) {
            if (std.mem.eql(u8, decision.string, "review") or std.mem.eql(u8, decision.string, "reject")) continue;
            return error.InvalidResolutionArtifact;
        }
        const local = entity.object.get("local_id") orelse return error.InvalidResolutionArtifact;
        const ref = entity.object.get("doc_ref") orelse return error.InvalidResolutionArtifact;
        if (local != .string or local.string.len == 0 or ref != .object) return error.InvalidResolutionArtifact;
        const key = ref.object.get("key") orelse return error.InvalidResolutionArtifact;
        if (key != .string or key.string.len == 0) return error.InvalidResolutionArtifact;
        if (ref.object.get("table")) |table| if (table != .string or table.string.len == 0) return error.InvalidResolutionArtifact;
        try map.put(a, local.string, ref);
    }
}

test "capi graph resolution endpoints reject corrupt artifacts and exclude provisional decisions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var map: std.json.ObjectMap = .empty;
    for ([_][]const u8{ "{", "[]", "{}", "{\"entities\":{}}", "{\"entities\":[{\"decision\":\"match\",\"local_id\":\"e0\",\"doc_ref\":{}}]}" }) |raw|
        try std.testing.expectError(error.InvalidResolutionArtifact, append(arena.allocator(), &map, raw));
    try append(arena.allocator(), &map, "{\"entities\":[{\"decision\":\"review\"},{\"decision\":\"reject\"},{\"decision\":\"match\",\"local_id\":\"e0\",\"doc_ref\":{\"key\":\"concept/canonical\",\"table\":\"entities\"}}]}");
    try std.testing.expectEqual(@as(usize, 1), map.count());
    try std.testing.expectEqualStrings("concept/canonical", map.get("e0").?.object.get("key").?.string);
}
