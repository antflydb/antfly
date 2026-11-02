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

//! Deterministic subtract-only survivor schemas. This selector carries no
//! authorization, owner membership, row proof or catalog publication authority.
const std = @import("std");
const counters = @import("column_sequences.zig");
const A = std.mem.Allocator;

pub const Relation = struct { table_id: u64, physical_name: []const u8 };
pub const Selection = struct {
    pub const max_relations = 128;
    pub const max_schema = 2 * 1024 * 1024;
    relations: []const Relation = &.{},
    sequences: []const counters.DefaultRetirement = &.{},

    pub fn validate(self: Selection) !void {
        if ((self.relations.len == 0 and self.sequences.len == 0) or self.relations.len > max_relations) return error.InvalidSchemaValidationJob;
        if (self.sequences.len != 0) try (counters.DefaultRetirementSet{ .members = self.sequences }).validate();
        var previous: u64 = 0;
        for (self.relations, 0..) |relation, index| {
            if (relation.table_id <= previous or relation.physical_name.len == 0 or relation.physical_name.len > 256 or
                std.mem.indexOfScalar(u8, relation.physical_name, 0) != null or !std.unicode.utf8ValidateSlice(relation.physical_name)) return error.InvalidSchemaValidationJob;
            for (self.relations[0..index]) |prior| if (std.mem.eql(u8, prior.physical_name, relation.physical_name)) return error.InvalidSchemaValidationJob;
            previous = relation.table_id;
        }
    }

    pub fn fingerprint(self: Selection) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/dependent-object-retirement/v1\x00");
        hashInteger(&hash, self.relations.len);
        for (self.relations) |relation| {
            hashInteger(&hash, relation.table_id);
            hashInteger(&hash, relation.physical_name.len);
            hash.update(relation.physical_name);
        }
        hashInteger(&hash, self.sequences.len);
        for (self.sequences) |sequence| hash.update(&try sequence.fingerprint());
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn requireSuccessor(self: Selection, a: A, table_id: u64, before: []const u8, after: []const u8) !void {
        if (after.len == 0 or after.len > max_schema) return error.InvalidSchemaValidationJob;
        const expected = try self.schemaAlloc(a, table_id, before);
        defer a.free(expected);
        var left = try std.json.parseFromSlice(std.json.Value, a, expected, .{ .parse_numbers = false });
        defer left.deinit();
        var right = try std.json.parseFromSlice(std.json.Value, a, after, .{ .parse_numbers = false });
        defer right.deinit();
        if (!@import("../schema/json_shape.zig").equal(left.value, right.value, 0)) return error.InvalidSchemaValidationJob;
    }

    /// Derive one successor epoch for this surviving table. Work depends on
    /// declarations and cohort membership, never user rows. Empty dependency
    /// arrays use the same omitted representation as ordinary DROP DEFAULT.
    pub fn schemaAlloc(self: Selection, backing: A, table_id: u64, before: []const u8) ![]u8 {
        try self.validate();
        if (table_id == 0 or before.len == 0 or before.len > max_schema) return error.InvalidSchemaValidationJob;
        for (self.relations) |relation| if (relation.table_id == table_id) return error.InvalidSchemaValidationJob;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var root = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{ .allocate = .alloc_always, .parse_numbers = false });
        counters.advanceSchemaVersion(&root) catch return error.InvalidSchemaValidationJob;
        var removed: usize = if (self.sequences.len != 0)
            try (counters.DefaultRetirementSet{ .members = self.sequences }).removeFromOwnedValue(a, table_id, &root)
        else
            0;
        if (self.relations.len != 0) if (root.object.getPtr("foreign_keys")) |foreign| {
            if (foreign.* != .array or foreign.array.items.len > 1024) return error.InvalidSchemaValidationJob;
            var parents: std.StringHashMapUnmanaged(void) = .empty;
            try parents.ensureTotalCapacity(a, @intCast(self.relations.len));
            for (self.relations) |relation| parents.putAssumeCapacity(relation.physical_name, {});
            var kept: usize = 0;
            var retired: usize = 0;
            for (foreign.array.items) |entry| {
                if (entry != .object) return error.InvalidSchemaValidationJob;
                const parent = entry.object.get("parent_table") orelse return error.InvalidSchemaValidationJob;
                if (parent != .string) return error.InvalidSchemaValidationJob;
                if (parents.contains(parent.string)) {
                    retired += 1;
                    continue;
                }
                foreign.array.items[kept] = entry;
                kept += 1;
            }
            if (retired != 0) {
                foreign.array.items.len = kept;
                if (kept == 0) _ = root.object.orderedRemove("foreign_keys");
                removed += retired;
            }
        };
        if (removed == 0) return error.CatalogGenerationChanged;
        return std.json.Stringify.valueAlloc(backing, root, .{});
    }
};

fn hashInteger(hash: *std.crypto.hash.Blake3, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}

fn exercise(a: A, mode: []const u8) !void {
    const one: counters.DefaultRetirement = .{ .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1) };
    var two = one;
    two.sequence_id = 12;
    const selection: Selection = .{ .relations = &.{.{ .table_id = 101, .physical_name = "retired_parent" }}, .sequences = &.{ one, two } };
    const first: counters.Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 11, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "first_seq" } };
    var second = first;
    second.sequence_id = 12;
    second.owner.column_id = 4;
    var retained = first;
    retained.sequence_id = 13;
    retained.owner.column_id = 5;
    retained.generation = .always;
    const before = try std.json.Stringify.valueAlloc(a, .{
        .version = 7,
        .storage_mode = mode,
        .marker = .{ .preserved = .{ 3, 1, 2 } },
        .foreign_keys = .{ .{ .name = "retired_fk", .parent_table = "retired_parent" }, .{ .name = "retained_fk", .parent_table = "other_parent" } },
        .column_defaults = .{ .{ .column = "first", .sequence = first }, .{ .column = "second", .sequence = second }, .{ .column = "unrelated_identity", .sequence = retained }, .{ .column = "label", .expression = .{ .literal = "ready" } } },
    }, .{});
    defer a.free(before);
    const after = try selection.schemaAlloc(a, 7, before);
    defer a.free(after);
    try selection.requireSuccessor(a, 7, before, after);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, after, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 8), parsed.value.object.get("version").?.integer);
    try std.testing.expectEqualStrings(mode, parsed.value.object.get("storage_mode").?.string);
    const foreign = parsed.value.object.get("foreign_keys").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), foreign.len);
    try std.testing.expectEqualStrings("retained_fk", foreign[0].object.get("name").?.string);
    const defaults = parsed.value.object.get("column_defaults").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), defaults.len);
    try std.testing.expectEqualStrings("unrelated_identity", defaults[0].object.get("column").?.string);
    try std.testing.expectEqualStrings("label", defaults[1].object.get("column").?.string);
    var kept = (try counters.referenceForColumn(a, parsed.value, "unrelated_identity")).?;
    defer kept.deinit();
    try std.testing.expectEqualDeep(retained, kept.value);
    // Identity binding contributes to the commitment, independent of physical
    // source encodings or mutable counter allocations.
    const digest = try selection.fingerprint();
    var changed = one;
    changed.incarnation[0] ^= 1;
    const different: Selection = .{ .relations = selection.relations, .sequences = &.{ changed, two } };
    try std.testing.expect(!std.mem.eql(u8, &digest, &try different.fingerprint()));
}

test "dependent retirement combines FK and counter withdrawals in one epoch with owned allocation failures" {
    const a = std.testing.allocator;
    try exercise(a, "relational");
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, exercise, .{"relational"});
}

test "dependent retirement rejects unrelated weakened schemas forged membership and retired survivors" {
    const a = std.testing.allocator;
    const selection: Selection = .{ .relations = &.{.{ .table_id = 101, .physical_name = "retired_parent" }} };
    const before = "{\"version\":1,\"marker\":true,\"foreign_keys\":[{\"parent_table\":\"retired_parent\"}]}";
    try std.testing.expectError(error.InvalidSchemaValidationJob, selection.requireSuccessor(a, 7, before, "{\"version\":2,\"marker\":false}"));
    try std.testing.expectError(error.InvalidSchemaValidationJob, selection.schemaAlloc(a, 101, before));
    try std.testing.expectError(error.CatalogGenerationChanged, selection.schemaAlloc(a, 7, "{\"version\":1,\"foreign_keys\":[]}"));
    try std.testing.expectError(error.InvalidSchemaValidationJob, (Selection{ .relations = &.{ .{ .table_id = 101, .physical_name = "retired_parent" }, .{ .table_id = 102, .physical_name = "retired_parent" } } }).validate());
    try std.testing.expectError(error.InvalidSchemaValidationJob, (Selection{}).validate());
    const after = try selection.schemaAlloc(a, 7, before);
    defer a.free(after);
    try std.testing.expectEqualStrings("{\"version\":2,\"marker\":true}", after);
}

test "dependent retirement preserves exact numeric defaults and rejects rounded postimages" {
    const a = std.testing.allocator;
    const selection: Selection = .{ .relations = &.{.{ .table_id = 101, .physical_name = "retired" }} };
    const before = "{\"version\":1,\"foreign_keys\":[{\"parent_table\":\"retired\"}],\"column_defaults\":[{\"column\":\"precision\",\"expression\":{\"literal\":0.123456789012345678901234567890}}]}";
    const expected = "{\"version\":2,\"column_defaults\":[{\"column\":\"precision\",\"expression\":{\"literal\":0.123456789012345678901234567890}}]}";
    const after = try selection.schemaAlloc(a, 7, before);
    defer a.free(after);
    try std.testing.expectEqualStrings(expected, after);
    try selection.requireSuccessor(a, 7, before, expected);
    try std.testing.expectError(error.InvalidSchemaValidationJob, selection.requireSuccessor(a, 7, before, "{\"version\":2,\"column_defaults\":[{\"column\":\"precision\",\"expression\":{\"literal\":0.123456789012345678901234567891}}]}"));
}
