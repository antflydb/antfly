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

/// Rebind descriptive default references into an owned output. Scratch is
/// bounded to this call; callers never receive a borrowed/owned union by luck.
pub fn rewriteSchemaAlloc(backing: A, schema_json: []const u8, old_table: u64, next: attributes.State, moves: []const Move) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    if (try prepareOwnerReferenceRewrite(a, schema_json, old_table, next, moves)) |root| {
        try validateBindingsValue(a, root, next, moves);
        return std.json.Stringify.valueAlloc(backing, root, .{});
    }
    return backing.dupe(u8, schema_json);
}

/// Validation owns scratch, emits no persistent schema copy, and never grants
/// generation or counter authority. Keep unchanged bytes borrowed only inside
/// this call; the public rewrite function always returns owned output.
pub fn requireOwnerReferences(backing: A, before: []const u8, after: []const u8, old_table: u64, next: attributes.State, moves: []const Move) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const expected = if (try prepareOwnerReferenceRewrite(a, before, old_table, next, moves)) |root| block: {
        try validateBindingsValue(a, root, next, moves);
        break :block try std.json.Stringify.valueAlloc(a, root, .{});
    } else before;
    if (!std.mem.eql(u8, expected, after)) return error.InvalidRestoreStaging;
}

/// Pure plan validation permits only these exact owner-reference substitutions
/// during TRUNCATE; unrelated schema changes still require a schema rewrite.
fn prepareOwnerReferenceRewrite(a: A, schema_json: []const u8, old_table: u64, next: attributes.State, moves: []const Move) !?std.json.Value {
    if (old_table == 0 or next.table_id == old_table or moves.len > max_moves) return error.InvalidRestoreStaging;
    next.validate(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRestoreStaging;
    // An absent document schema has no default references. Preserve its exact
    // representation; it is not malformed JSON and must not become {}.
    if (schema_json.len == 0) return null;
    var root = std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRestoreStaging;
    if (root != .object) return error.InvalidRestoreStaging;
    const defaults = root.object.getPtr("column_defaults") orelse return null;
    if (defaults.* != .array) return error.InvalidRestoreStaging;
    var changed = false;
    for (defaults.array.items) |*entry| {
        if (entry.* != .object) return error.InvalidRestoreStaging;
        const reference = entry.object.get("sequence") orelse continue;
        var prior = try columns.Binding.parse(a, reference);
        defer prior.deinit();
        if (prior.value.owner.table_id != old_table) return error.InvalidRestoreStaging;
        const name = entry.object.get("column") orelse return error.InvalidRestoreStaging;
        if (name != .string) return error.InvalidRestoreStaging;
        const column = for (next.columns) |attribute| {
            if (std.mem.eql(u8, name.string, attribute.name)) break attribute;
        } else return error.InvalidRestoreStaging;
        const owner: sequence.OwnedBy = .{ .table_id = next.table_id, .column_id = column.id };
        // Only an internal identity dependency must follow an owned move.
        if (prior.value.generation != .serial) {
            const move = for (moves) |value| {
                if (value.before.id == prior.value.sequence_id) break value;
            } else return error.InvalidRestoreStaging;
            const descriptor = try sequence.Descriptor.decode(a, move.before.source_json);
            if (!std.meta.eql((try descriptor.definition()).owned_by, @as(?sequence.OwnedBy, prior.value.owner)) or !std.meta.eql(move.owner, owner)) return error.InvalidRestoreStaging;
        }
        var binding = prior.value;
        binding.owner = owner;
        const encoded = try std.json.Stringify.valueAlloc(a, binding, .{});
        try entry.object.put(a, "sequence", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
        changed = true;
    }
    return if (changed) root else null;
}

pub fn validateSchemaBindings(backing: A, schema_json: []const u8, next: attributes.State, moves: []const Move) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    next.validate(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRestoreStaging;
    if (schema_json.len == 0) return;
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, schema_json, .{ .parse_numbers = false });
    try validateBindingsValue(a, root, next, moves);
}

fn validateBindingsValue(a: A, root: std.json.Value, next: attributes.State, moves: []const Move) !void {
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
        if (bound.value.owner.table_id != next.table_id) return error.InvalidRestoreStaging;
        const column = next.columnById(bound.value.owner.column_id) orelse return error.InvalidRestoreStaging;
        if (!std.mem.eql(u8, column.name, name.string)) return error.InvalidRestoreStaging;
        if (bound.value.generation != .serial) {
            const move = for (moves) |value| {
                if (value.before.id == bound.value.sequence_id) break value;
            } else return error.InvalidRestoreStaging;
            if (!std.meta.eql(bound.value.owner, move.owner) or bound.value.namespace_id != move.before.parent_id) return error.InvalidRestoreStaging;
            if (!(try move.definition(a)).internal_identity) return error.InvalidRestoreStaging;
        }
    }
}

test "SQL owned sequence handoff preserves CONTINUE leases and invalidates RESTART generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original: sequence.Definition = .{ .generation = 8, .cache = 4, .start = 100, .increment = 10, .owned_by = .{ .table_id = 17, .column_id = 3 }, .internal_identity = true };
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
    const rewritten = try rewriteSchemaAlloc(a, schema_json, 17, next, &.{move});
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, rewritten, .{});
    var parsed = try columns.Binding.parse(a, root.object.get("column_defaults").?.array.items[0].object.get("sequence").?);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(move.owner, parsed.value.owner);
    try std.testing.expectEqual(binding.sequence_id, parsed.value.sequence_id);
    try std.testing.expectEqualDeep(binding.incarnation, parsed.value.incarnation);
    try std.testing.expectError(error.InvalidRestoreStaging, rewriteSchemaAlloc(a, schema_json, 17, next, &.{}));
}

test "SQL owned sequence handoff rebinds borrowed and unowned defaults without moving or restarting their counters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const binding: columns.Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 91, .namespace_id = 2, .owner = .{ .table_id = 17, .column_id = 3 }, .target = .{ .table = "external_counter" } };
    const schema_json = try std.json.Stringify.valueAlloc(a, .{ .column_defaults = .{.{ .column = "id", .sequence = binding }} }, .{});
    const next: attributes.State = .{ .format = 1, .table_id = 18, .epoch = .{ .version = 0, .digest = @splat(2) }, .next_id = 7, .columns = &.{.{ .id = 6, .name = "id", .signature = @splat(3) }} };
    const rewritten = try rewriteSchemaAlloc(a, schema_json, 17, next, &.{});
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, rewritten, .{});
    var parsed = (try columns.referenceForColumn(a, root, "id")).?;
    defer parsed.deinit();
    try std.testing.expectEqualDeep(sequence.OwnedBy{ .table_id = 18, .column_id = 6 }, parsed.value.owner);
    try std.testing.expectEqual(binding.sequence_id, parsed.value.sequence_id);
    try std.testing.expectEqual(binding.namespace_id, parsed.value.namespace_id);
    try std.testing.expectEqualDeep(binding.incarnation, parsed.value.incarnation);
    try std.testing.expectError(error.InvalidRestoreStaging, rewriteSchemaAlloc(a, schema_json, 19, next, &.{}));
}

fn ownerReferenceOwnership(a: A, before: []const u8, next: attributes.State) !void {
    const after = try rewriteSchemaAlloc(a, before, 17, next, &.{});
    defer a.free(after);
    if (before.len != 0) try std.testing.expect(after.ptr != before.ptr);
    try requireOwnerReferences(a, before, after, 17, next, &.{});
    try validateSchemaBindings(a, after, next, &.{});
}

test "SQL owned sequence handoff owns rewrite outputs and frees validation scratch on every allocation failure" {
    const a = std.testing.allocator;
    const next: attributes.State = .{ .format = 1, .table_id = 18, .epoch = .{ .version = 0, .digest = @splat(2) }, .next_id = 7, .columns = &.{.{ .id = 6, .name = "id", .signature = @splat(3) }} };
    const binding: columns.Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 91, .namespace_id = 2, .owner = .{ .table_id = 17, .column_id = 3 }, .target = .{ .table = "external_counter" } };
    const referenced = try std.json.Stringify.valueAlloc(a, .{ .column_defaults = .{.{ .column = "id", .sequence = binding }} }, .{});
    defer a.free(referenced);
    for ([_][]const u8{ "", "{}", referenced }) |before| {
        try ownerReferenceOwnership(a, before, next);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, ownerReferenceOwnership, .{ before, next });
    }
    try std.testing.expectError(error.InvalidRestoreStaging, requireOwnerReferences(a, referenced, "{}", 17, next, &.{}));
}

test "SQL owned sequence handoff preserves absent document schemas and rejects malformed or substituted defaults" {
    const a = std.testing.allocator;
    const next: attributes.State = .{ .format = 1, .table_id = 18, .epoch = .{ .version = 0, .digest = @splat(2) }, .next_id = 1, .columns = &.{} };
    const absent = try rewriteSchemaAlloc(a, "", 17, next, &.{});
    defer a.free(absent);
    try std.testing.expectEqualStrings("", absent);
    try requireOwnerReferences(a, "", "", 17, next, &.{});
    try validateSchemaBindings(a, "", next, &.{});
    try std.testing.expectError(error.InvalidRestoreStaging, requireOwnerReferences(a, "", "{}", 17, next, &.{}));
    try std.testing.expectError(error.InvalidRestoreStaging, requireOwnerReferences(a, "{}", "", 17, next, &.{}));
    try std.testing.expectError(error.InvalidRestoreStaging, requireOwnerReferences(a, "{", "", 17, next, &.{}));
}
