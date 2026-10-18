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

//! Promotion controls travel in the source's immutable intent, never in the
//! entity document. Provenance is bounded by distinct origin/spelling pairs,
//! rather than mentions. Curation uses a conditional destination write so a
//! concurrent source's alias contribution cannot be lost.
const std = @import("std");
const types = @import("types.zig");
pub const control_field = "_resolver_curation";
pub const Control = struct {
    scope: []const u8,
    owner: []const u8,
    alias: []const u8,
    revision: u64,
    curation_revision: u64 = 0,
    action: enum { add, remove, redirect },
    restore: bool = false,
    suppressed: ?bool = null,
};

pub fn merge(a: std.mem.Allocator, current: ?[]const u8, incoming: []const u8, key: []const u8) ![]u8 {
    const document = try std.json.parseFromSliceLeaky(std.json.Value, a, incoming, .{});
    if (document != .object) return error.InvalidEntityPromotionDocument;
    const existing = if (current) |bytes| try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{}) else std.json.Value{ .object = .empty };
    const instruction = try control(a, document);
    if (instruction) |value| if (isStale(existing, value)) return a.dupe(u8, current orelse "{}");
    var ops: std.ArrayListUnmanaged(types.TransformOp) = .empty;
    for ([_][]const u8{ "entity_type", "canonical_name" }) |field| {
        if (document.object.get(field)) |value| {
            if (value != .string) return error.InvalidEntityPromotionDocument;
            try ops.append(a, .{ .op = .set_on_insert, .path = field, .value_json = try std.json.Stringify.valueAlloc(a, value, .{}) });
        }
    }
    if (instruction == null) if (document.object.get("aliases")) |aliases| {
        if (aliases != .array) return error.InvalidEntityPromotionDocument;
        for (aliases.array.items) |alias| {
            if (alias != .string) return error.InvalidEntityPromotionDocument;
            try ops.append(a, .{ .op = .add_to_set, .path = "aliases", .value_json = try std.json.Stringify.valueAlloc(a, alias, .{}) });
        }
    };
    if (document.object.get("merged_into")) |redirect| {
        if (redirect == .string) {
            try ops.append(a, .{ .op = .set, .path = "merged_into", .value_json = try std.json.Stringify.valueAlloc(a, redirect, .{}) });
        } else if (redirect == .null) {
            try ops.append(a, .{ .op = .unset, .path = "merged_into" });
        } else return error.InvalidEntityPromotionDocument;
        const table = document.object.get("merged_into_table") orelse std.json.Value.null;
        if (table == .string) {
            try ops.append(a, .{ .op = .set, .path = "merged_into_table", .value_json = try std.json.Stringify.valueAlloc(a, table, .{}) });
        } else if (table == .null) try ops.append(a, .{ .op = .unset, .path = "merged_into_table" }) else return error.InvalidEntityPromotionDocument;
    }
    if (instruction) |value| try ops.appendSlice(a, try operations(a, existing, value));
    if (ops.items.len == 0) return error.InvalidEntityPromotionDocument;
    return (try @import("transform.zig").resolveDocumentTransform(a, current, .{ .key = key, .operations = ops.items, .upsert = true })) orelse return error.InvalidEntityPromotionDocument;
}

pub fn control(a: std.mem.Allocator, value: std.json.Value) !?Control {
    if (value != .object) return error.InvalidEntityPromotionDocument;
    const raw = value.object.get(control_field) orelse return null;
    const result = try std.json.parseFromSliceLeaky(Control, a, try std.json.Stringify.valueAlloc(a, raw, .{}), .{});
    for ([_][]const u8{ result.scope, result.owner }) |key| {
        if (key.len != 64) return error.InvalidEntityPromotionDocument;
        for (key) |byte| if (!std.ascii.isHex(byte)) return error.InvalidEntityPromotionDocument;
    }
    return result;
}

pub fn isStale(current: std.json.Value, instruction: Control) bool {
    if (current != .object) return false;
    const revisions = current.object.get("_resolver_curation_revisions") orelse return false;
    if (revisions != .object) return false;
    const revision = revisions.object.get(instruction.scope) orelse return false;
    if (revision == .integer) return instruction.curation_revision == 0 and revision.integer > 0 and @as(u64, @intCast(revision.integer)) > instruction.revision;
    if (revision != .object) return false;
    const curation = revision.object.get("curation_revision") orelse return false;
    const generation = revision.object.get("generation") orelse return false;
    if (curation != .integer or generation != .integer or curation.integer < 0 or generation.integer < 0) return false;
    const previous: u64 = @intCast(curation.integer);
    return previous > instruction.curation_revision or (previous == instruction.curation_revision and @as(u64, @intCast(generation.integer)) > instruction.revision);
}

pub fn operations(a: std.mem.Allocator, current: std.json.Value, instruction: Control) ![]types.TransformOp {
    var ops: std.ArrayListUnmanaged(types.TransformOp) = .empty;
    if (isStale(current, instruction)) return ops.items;
    const owner_path = try std.fmt.allocPrint(a, "_resolver_aliases.{s}", .{instruction.owner});
    try ops.append(a, .{ .op = .set, .path = try std.fmt.allocPrint(a, "_resolver_curation_revisions.{s}", .{instruction.scope}), .value_json = try std.json.Stringify.valueAlloc(a, .{ .curation_revision = instruction.curation_revision, .generation = instruction.revision }, .{}) });
    switch (instruction.action) {
        .add => {
            // Existing documents have unattributed legacy aliases. Preserve
            // them conservatively; newly promoted documents can reconcile
            // their complete managed alias set exactly.
            try ops.append(a, .{ .op = .set_on_insert, .path = "_resolver_aliases_version", .value_json = "1" });
            const ownership_path = try std.fmt.allocPrint(a, "_resolver_aliases_managed.{s}", .{instruction.owner});
            const owners = if (current == .object) current.object.get("_resolver_aliases_managed") else null;
            const previous = if (owners) |o| if (o == .object) o.object.get(instruction.owner) else null else null;
            var introduced = if (previous) |v| v == .bool and v.bool else true;
            if (previous == null and current == .object) if (current.object.get("aliases")) |aliases| {
                if (aliases == .array) for (aliases.array.items) |alias| if (alias == .string and std.mem.eql(u8, alias.string, instruction.alias)) {
                    introduced = false;
                    break;
                };
            };
            try ops.append(a, .{ .op = .set, .path = ownership_path, .value_json = if (introduced) "true" else "false" });
            try ops.append(a, .{ .op = .set, .path = owner_path, .value_json = try std.json.Stringify.valueAlloc(a, instruction.alias, .{}) });
            try ops.append(a, .{ .op = .add_to_set, .path = "aliases", .value_json = try std.json.Stringify.valueAlloc(a, instruction.alias, .{}) });
        },
        .remove => {
            const owners = if (current == .object) current.object.get("_resolver_aliases") else null;
            var owned = false;
            var shared = false;
            if (owners) |map| {
                if (map != .object) return error.InvalidEntityPromotionDocument;
                var it = map.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* != .string) return error.InvalidEntityPromotionDocument;
                    if (!std.mem.eql(u8, entry.value_ptr.string, instruction.alias)) continue;
                    if (std.mem.eql(u8, entry.key_ptr.*, instruction.owner)) owned = true else shared = true;
                }
            }
            const version = if (current == .object) current.object.get("_resolver_aliases_version") else null;
            const name = if (current == .object) current.object.get("canonical_name") else null;
            const canonical = if (name) |v| v == .string and std.mem.eql(u8, v.string, instruction.alias) else false;
            const managed = if (current == .object) current.object.get("_resolver_aliases_managed") else null;
            const contribution = if (managed) |m| if (m == .object) m.object.get(instruction.owner) else null else null;
            const introduced = if (contribution) |v| v == .bool and v.bool else false;
            if (owned and !shared and !canonical and (introduced or (version != null and version.? == .integer and version.?.integer == 1)))
                try ops.append(a, .{ .op = .pull, .path = "aliases", .value_json = try std.json.Stringify.valueAlloc(a, instruction.alias, .{}) });
            try ops.append(a, .{ .op = .unset, .path = owner_path });
            try ops.append(a, .{ .op = .unset, .path = try std.fmt.allocPrint(a, "_resolver_aliases_managed.{s}", .{instruction.owner}) });
        },
        .redirect => {},
    }
    if (instruction.restore) {
        try ops.append(a, .{ .op = .unset, .path = "merged_into" });
        try ops.append(a, .{ .op = .unset, .path = "merged_into_table" });
    }
    if (instruction.suppressed) |suppressed| try ops.append(a, .{
        .op = if (suppressed) .add_to_set else .pull,
        .path = "resolver_suppressed_by",
        .value_json = try std.json.Stringify.valueAlloc(a, instruction.scope, .{}),
    });
    return ops.items;
}

fn testDocument(a: std.mem.Allocator, instruction: Control) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, .{ .entity_type = "concept", .canonical_name = "Canonical", ._resolver_curation = instruction }, .{});
}

test "resolver curation alias ownership preserves other sources and fences stale promotion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var first: Control = .{ .scope = &@as([64]u8, @splat('a')), .owner = &@as([64]u8, @splat('b')), .alias = "Synonym", .revision = 1, .action = .add };
    const one = try merge(a, null, try testDocument(a, first), "entity");
    var second = first;
    second.owner = &@as([64]u8, @splat('c'));
    const two = try merge(a, one, try testDocument(a, second), "entity");
    first.action = .remove;
    first.revision = 2;
    const shared = try merge(a, two, try testDocument(a, first), "entity");
    const parsed_shared = try std.json.parseFromSliceLeaky(std.json.Value, a, shared, .{});
    try std.testing.expectEqual(@as(usize, 1), parsed_shared.object.get("aliases").?.array.items.len);
    second.action = .remove;
    second.revision = 3;
    const removed = try merge(a, shared, try testDocument(a, second), "entity");
    const parsed_removed = try std.json.parseFromSliceLeaky(std.json.Value, a, removed, .{});
    try std.testing.expectEqual(@as(usize, 0), parsed_removed.object.get("aliases").?.array.items.len);
    try std.testing.expect(!parsed_removed.object.contains(control_field));
    first.action = .add;
    first.revision = 1;
    try std.testing.expectEqualStrings(removed, try merge(a, removed, try testDocument(a, first), "entity"));
}

test "resolver curation preserves unattributed legacy aliases and explicitly restores redirects" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var instruction: Control = .{ .scope = &@as([64]u8, @splat('a')), .owner = &@as([64]u8, @splat('b')), .alias = "Synonym", .revision = 1, .action = .add };
    const legacy = try merge(a, "{\"canonical_name\":\"Canonical\",\"aliases\":[\"Synonym\"],\"merged_into\":\"other\"}", try testDocument(a, instruction), "entity");
    instruction.action = .remove;
    instruction.restore = true;
    instruction.revision = 2;
    const restored = try merge(a, legacy, try testDocument(a, instruction), "entity");
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, restored, .{});
    try std.testing.expect(!parsed.object.contains("merged_into"));
    try std.testing.expectEqual(@as(usize, 1), parsed.object.get("aliases").?.array.items.len);
}
