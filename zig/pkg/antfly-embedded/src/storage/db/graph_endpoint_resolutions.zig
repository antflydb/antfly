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

//! Isolate corrupt persisted resolution entries without inventing endpoints.
const std = @import("std");

fn parse(a: std.mem.Allocator, raw: []const u8) !?std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => {
            std.log.warn("graph replay ignored malformed resolution JSON: {s}", .{@errorName(err)});
            return null;
        },
    };
}
fn reference(value: std.json.Value) bool {
    if (value != .object) return false;
    const key = value.object.get("key") orelse return false;
    if (key != .string or key.string.len == 0) return false;
    if (value.object.get("table")) |table| if (table != .string or table.string.len == 0) return false;
    return true;
}
fn entry(entity: std.json.Value) !?struct { local: []const u8, ref: std.json.Value } {
    if (entity != .object) return error.InvalidResolutionArtifact;
    const decision = entity.object.get("decision") orelse return error.InvalidResolutionArtifact;
    if (decision != .string) return error.InvalidResolutionArtifact;
    if (std.mem.eql(u8, decision.string, "suppressed") or std.mem.eql(u8, decision.string, "review") or std.mem.eql(u8, decision.string, "reject")) return null;
    if (!std.mem.eql(u8, decision.string, "new") and !std.mem.eql(u8, decision.string, "match")) return error.InvalidResolutionArtifact;
    const local = entity.object.get("local_id") orelse return error.InvalidResolutionArtifact;
    const ref = entity.object.get("doc_ref") orelse return error.InvalidResolutionArtifact;
    if (local != .string or local.string.len == 0 or !reference(ref)) return error.InvalidResolutionArtifact;
    return .{ .local = local.string, .ref = ref };
}
pub fn append(a: std.mem.Allocator, map: *std.json.ObjectMap, raw: []const u8) !void {
    const root = (try parse(a, raw)) orelse return;
    const entities = if (root == .object) root.object.get("entities") else null;
    if (entities == null or entities.? != .array) {
        std.log.warn("graph replay ignored resolution artifact: entities must be an array", .{});
        return;
    }
    for (entities.?.array.items, 0..) |entity, i| {
        const resolved = entry(entity) catch {
            std.log.warn("graph replay ignored invalid resolution entry {d}", .{i});
            continue;
        };
        if (resolved) |value| try map.put(a, value.local, value.ref);
    }
}
/// Validate an already flattened endpoint map with the same isolation policy.
pub fn references(a: std.mem.Allocator, raw: []const u8) !std.json.Value {
    var map: std.json.ObjectMap = .empty;
    const root = (try parse(a, raw)) orelse return .{ .object = map };
    if (root != .object) {
        std.log.warn("graph replay ignored endpoint map: expected an object", .{});
        return .{ .object = map };
    }
    var it = root.object.iterator();
    while (it.next()) |item| {
        if (item.key_ptr.len == 0 or !reference(item.value_ptr.*)) {
            std.log.warn("graph replay ignored invalid endpoint reference", .{});
            continue;
        }
        try map.put(a, item.key_ptr.*, item.value_ptr.*);
    }
    return .{ .object = map };
}

test "capi graph resolution isolates corrupt entries and preserves valid siblings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var map: std.json.ObjectMap = .empty;
    for ([_][]const u8{ "{", "[]", "{}", "{\"entities\":{}}", "{\"entities\":[{\"decision\":\"match\",\"local_id\":\"e0\",\"doc_ref\":{}}]}" }) |raw|
        try append(arena.allocator(), &map, raw);
    try std.testing.expectEqual(@as(usize, 0), map.count());
    try append(arena.allocator(), &map, "{\"entities\":[{\"decision\":\"review\"},{\"decision\":\"match\",\"doc_ref\":{}},{\"decision\":\"match\",\"local_id\":\"e0\",\"doc_ref\":{\"key\":\"concept/canonical\",\"table\":\"entities\"}}]}");
    try std.testing.expectEqual(@as(usize, 1), map.count());
    try std.testing.expectEqualStrings("concept/canonical", map.get("e0").?.object.get("key").?.string);
}
