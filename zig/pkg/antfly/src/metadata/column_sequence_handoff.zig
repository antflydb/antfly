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

//! Immutable owned-sequence moves in shared table-generation publication.
//! Preparation grants no allocator authority. Metadata verifies the complete
//! indexed predecessor set again before changing any catalog or counter bytes.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const sequence = local.system_catalog_sequences;
const attributes = local.system_catalog_column_identities;
const columns = local.system_catalog_column_sequences;
const A = std.mem.Allocator;

pub const Request = struct { targets: []const @import("restore_staging.zig").Target, restart: bool };

pub const max_moves = 1024;
pub const Move = struct {
    before: domain.Resource,
    owner: sequence.OwnedBy,
    restart: bool,

    pub fn definition(self: Move, a: A) !sequence.Definition {
        if (self.before.kind != .sequence or self.before.id == 0 or self.before.parent_id == 0 or self.before.name.len == 0)
            return error.InvalidRestoreStaging;
        try self.owner.validate();
        const prior = try sequence.Descriptor.decode(a, self.before.source_json);
        if (prior.id != self.before.id) return error.InvalidRestoreStaging;
        var next = try prior.definition();
        const old = next.owned_by orelse return error.InvalidRestoreStaging;
        if (old.table_id == self.owner.table_id) return error.InvalidRestoreStaging;
        next.owned_by = self.owner;
        // Ownership transfer alone preserves cached arithmetic leases. RESTART
        // changes the generation, invalidating every unconsumed old lease.
        if (self.restart) next.generation = std.math.add(u64, next.generation, 1) catch return error.CatalogGenerationExhausted;
        try next.validate();
        return next;
    }
    pub fn resource(self: Move, a: A) !domain.Resource {
        var result = self.before;
        result.source_json = try (try sequence.Descriptor.init(result.id, try self.definition(a))).encode(a);
        return result;
    }
};

/// Rebind only descriptive default references. Attribute identities come from
/// the destination's native registry; ordinals are never interpreted as IDs.
pub fn rewriteSchema(a: A, schema_json: []const u8, old_table: u64, next: attributes.State, moves: []const Move) ![]const u8 {
    if (moves.len > max_moves or next.table_id == old_table) return error.InvalidRestoreStaging;
    const result = try rewriteOwnerReferences(a, schema_json, old_table, moves);
    try validateSchemaBindings(a, result, next, moves);
    return result;
}

/// Pure plan validation permits only these exact owner-reference substitutions
/// during TRUNCATE; unrelated schema changes still require a schema rewrite.
pub fn rewriteOwnerReferences(a: A, schema_json: []const u8, old_table: u64, moves: []const Move) ![]const u8 {
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
    if (root != .object) return error.InvalidRestoreStaging;
    const defaults = root.object.getPtr("column_defaults") orelse return schema_json;
    if (defaults.* != .array) return error.InvalidRestoreStaging;
    var changed = false;
    for (defaults.array.items) |*entry| {
        if (entry.* != .object) return error.InvalidRestoreStaging;
        const reference = entry.object.get("sequence") orelse continue;
        var prior = try columns.Binding.parse(a, reference);
        defer prior.deinit();
        if (prior.value.owner.table_id != old_table) return error.InvalidRestoreStaging;
        const move = for (moves) |value| {
            if (value.before.id == prior.value.sequence_id) break value;
        } else return error.InvalidRestoreStaging;
        const descriptor = try sequence.Descriptor.decode(a, move.before.source_json);
        if (!std.meta.eql((try descriptor.definition()).owned_by, @as(?sequence.OwnedBy, prior.value.owner))) return error.InvalidRestoreStaging;
        var binding = prior.value;
        binding.owner = move.owner;
        const encoded = try std.json.Stringify.valueAlloc(a, binding, .{});
        try entry.object.put(a, "sequence", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
        changed = true;
    }
    return if (changed) try std.json.Stringify.valueAlloc(a, root, .{}) else schema_json;
}

pub fn validateSchemaBindings(a: A, schema_json: []const u8, next: attributes.State, moves: []const Move) !void {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
    if (root != .object) return error.InvalidRestoreStaging;
    const defaults = root.object.get("column_defaults") orelse return;
    if (defaults != .array) return error.InvalidRestoreStaging;
    for (defaults.array.items) |entry| {
        if (entry != .object) return error.InvalidRestoreStaging;
        const reference = entry.object.get("sequence") orelse continue;
        const name = entry.object.get("column") orelse return error.InvalidRestoreStaging;
        if (name != .string) return error.InvalidRestoreStaging;
        var bound = try columns.Binding.parse(a, reference);
        defer bound.deinit();
        const move = for (moves) |value| {
            if (value.before.id == bound.value.sequence_id) break value;
        } else return error.InvalidRestoreStaging;
        const column = next.columnById(move.owner.column_id) orelse return error.InvalidRestoreStaging;
        if (!std.meta.eql(bound.value.owner, move.owner) or move.owner.table_id != next.table_id or
            bound.value.namespace_id != move.before.parent_id or !std.mem.eql(u8, column.name, name.string)) return error.InvalidRestoreStaging;
    }
}

test "SQL owned sequence handoff preserves CONTINUE leases and invalidates RESTART generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original: sequence.Definition = .{ .generation = 8, .cache = 4, .start = 100, .increment = 10, .owned_by = .{ .table_id = 17, .column_id = 3 } };
    const descriptor = try sequence.Descriptor.init(91, original);
    const move: Move = .{
        .before = .{ .kind = .sequence, .id = 91, .parent_id = 2, .name = "usage_records_id_seq", .source_json = try descriptor.encode(a) },
        .owner = .{ .table_id = 18, .column_id = 6 },
        .restart = false,
    };
    const continued = try move.definition(a);
    try std.testing.expectEqual(@as(u64, 8), continued.generation);
    try std.testing.expectEqualDeep(move.owner, continued.owned_by.?);
    var restarted = move;
    restarted.restart = true;
    try std.testing.expectEqual(@as(u64, 9), (try restarted.definition(a)).generation);
    const binding: columns.Binding = .{
        .generation = .always,
        .incarnation = @splat(1),
        .sequence_id = 91,
        .namespace_id = 2,
        .owner = original.owned_by.?,
        .target = .{ .database = "default", .namespace = "public", .table = move.before.name },
    };
    const schema_json = try std.json.Stringify.valueAlloc(a, .{ .column_defaults = .{.{ .column = "id", .sequence = binding }} }, .{});
    const next: attributes.State = .{ .format = 1, .table_id = 18, .epoch = .{ .version = 0, .digest = @splat(2) }, .next_id = 7, .columns = &.{.{ .id = 6, .name = "id", .signature = @splat(3) }} };
    const rewritten = try rewriteSchema(a, schema_json, 17, next, &.{move});
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, rewritten, .{});
    var parsed = try columns.Binding.parse(a, root.object.get("column_defaults").?.array.items[0].object.get("sequence").?);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(move.owner, parsed.value.owner);
    try std.testing.expectEqual(binding.sequence_id, parsed.value.sequence_id);
    try std.testing.expectEqualDeep(binding.incarnation, parsed.value.incarnation);
    try std.testing.expectError(error.InvalidRestoreStaging, rewriteSchema(a, schema_json, 17, next, &.{}));
}
