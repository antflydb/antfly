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

//! Owned sequence defaults are preparation effects, never scalar bytecode.
//! Names are diagnostic; immutable catalog/attribute identities grant binding.
const std = @import("std");
const sequence = @import("sequences.zig");
const domain = @import("domain.zig");
const A = std.mem.Allocator;

pub const Generation = @import("antfly_schema_openapi").RelationalColumnSequenceGeneration;

/// Compiler intent, consumed only inside the containing table publication.
/// Ingress cannot supply an allocated sequence or attribute identity.
pub const Creation = struct {
    column: []const u8,
    generation: Generation,
    options: sequence.Options,
    /// Source intent carries no allocated destination identity. Native admission
    /// resolves it only through the containing transaction's source guards.
    source: ?struct { table_id: u64, column_id: u64 } = null,

    pub fn validate(self: Creation) !void {
        try domain.validateTableName(self.column);
        if (self.options.owned_by != .unchanged or self.options.restart != null or self.options.integer == null)
            return error.InvalidSequenceDefinition;
        if (self.source) |source| {
            if (source.table_id == 0 or source.column_id == 0) return error.InvalidSequenceDefinition;
            // Source configuration is resolved natively, never compiler-owned.
            const width_only: sequence.Options = .{ .integer = self.options.integer };
            if (!std.meta.eql(self.options, width_only)) return error.InvalidSequenceDefinition;
        }
        _ = try sequence.Definition.prepare(null, self.options);
    }
};

/// The schema owns one compact, immutable reference. Neither a column ordinal
/// nor a caller-provided name can authorize an allocation from another table.
pub const Binding = struct {
    generation: Generation,
    incarnation: [16]u8,
    sequence_id: u64,
    namespace_id: u64,
    owner: sequence.OwnedBy,
    target: domain.Target,

    pub fn validate(self: Binding) !void {
        if (self.sequence_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0))
            return error.InvalidSequenceDefinition;
        try self.owner.validate();
        try self.target.validate();
    }

    /// Public identities are decimal strings, retaining every native u64 bit.
    /// The catalog incarnation is a fixed hexadecimal value, not a SQL OID.
    pub fn parse(backing: A, value: std.json.Value) !Owned {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const Wire = struct {
            generation: Generation,
            incarnation: []const u8,
            sequence_id: []const u8,
            namespace_id: []const u8,
            table_id: []const u8,
            column_id: []const u8,
            database: []const u8,
            namespace: []const u8,
            name: []const u8,
        };
        if (value != .object or value.object.count() != 9) return error.InvalidSequenceDefinition;
        for (value.object.values()) |cell| {
            if (cell != .string or cell.string.len > 1024) return error.InvalidSequenceDefinition;
        }
        const wire = try std.json.parseFromValueLeaky(Wire, a, value, .{ .allocate = .alloc_always });
        if (wire.incarnation.len != 32) return error.InvalidSequenceDefinition;
        var incarnation: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&incarnation, wire.incarnation) catch return error.InvalidSequenceDefinition;
        const result: Binding = .{
            .generation = wire.generation,
            .incarnation = incarnation,
            .sequence_id = try id(wire.sequence_id),
            .namespace_id = try id(wire.namespace_id),
            .owner = .{ .table_id = try id(wire.table_id), .column_id = try id(wire.column_id) },
            .target = .{ .database = wire.database, .namespace = wire.namespace, .table = wire.name },
        };
        try result.validate();
        return .{ .arena = arena, .value = result };
    }

    fn id(text: []const u8) !u64 {
        const value = std.fmt.parseInt(u64, text, 10) catch return error.InvalidSequenceDefinition;
        if (value == 0 or text.len == 0 or (text.len > 1 and text[0] == '0')) return error.InvalidSequenceDefinition;
        for (text) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidSequenceDefinition;
        return value;
    }

    pub fn jsonStringify(self: Binding, writer: anytype) !void {
        var ids: [4][20]u8 = undefined;
        try writer.write(.{
            .generation = self.generation,
            .incarnation = &std.fmt.bytesToHex(self.incarnation, .lower),
            .sequence_id = std.fmt.bufPrint(&ids[0], "{d}", .{self.sequence_id}) catch unreachable,
            .namespace_id = std.fmt.bufPrint(&ids[1], "{d}", .{self.namespace_id}) catch unreachable,
            .table_id = std.fmt.bufPrint(&ids[2], "{d}", .{self.owner.table_id}) catch unreachable,
            .column_id = std.fmt.bufPrint(&ids[3], "{d}", .{self.owner.column_id}) catch unreachable,
            .database = self.target.database,
            .namespace = self.target.namespace,
            .name = self.target.table,
        });
    }

    /// Typed durable envelopes use the same canonical string-ID wire as
    /// schema references. Do not let the default struct parser reinterpret
    /// the 32-character incarnation as a native 16-element byte array.
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !Binding {
        const value = try std.json.innerParse(std.json.Value, a, source, options);
        return jsonParseFromValue(a, value, options);
    }

    pub fn jsonParseFromValue(a: A, value: std.json.Value, _: std.json.ParseOptions) error{ OutOfMemory, UnexpectedToken }!Binding {
        var parsed = parse(a, value) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.UnexpectedToken,
        };
        defer parsed.deinit();
        var result = parsed.value;
        result.target.database = try a.dupe(u8, result.target.database);
        errdefer a.free(result.target.database);
        result.target.namespace = try a.dupe(u8, result.target.namespace);
        errdefer a.free(result.target.namespace);
        result.target.table = try a.dupe(u8, result.target.table);
        return result;
    }

    /// Copies only the diagnostic strings into the caller's owning region.
    pub fn cloneLeaky(self: Binding, a: A) !*const Binding {
        const binding = try a.create(Binding);
        binding.* = self;
        binding.target.database = try a.dupe(u8, self.target.database);
        binding.target.namespace = try a.dupe(u8, self.target.namespace);
        binding.target.table = try a.dupe(u8, self.target.table);
        return binding;
    }

    /// Renaming a sequence does not change the logical default expression.
    pub fn fingerprint(self: Binding) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.column-sequence-default.v1\x00");
        hash.update(&self.incarnation);
        hash.update(@tagName(self.generation));
        inline for (.{ self.sequence_id, self.namespace_id, self.owner.table_id, self.owner.column_id }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .big);
            hash.update(&bytes);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
};

/// Schema-only removal selector. This is an immutable identity, never a grant
/// to retire the referenced counter or bypass its complete owner barrier.
pub const DefaultRetirement = struct {
    sequence_id: u64,
    namespace_id: u64,
    incarnation: [16]u8,

    pub fn validate(self: @This()) !void {
        if (self.sequence_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSequenceDefinition;
    }
    pub fn fingerprint(self: @This()) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sequence-default-retirement/v1\x00");
        hash.update(&self.incarnation);
        inline for (.{ self.sequence_id, self.namespace_id }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .little);
            hash.update(&bytes);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }

    /// Remove only ordinary defaults referring to this exact catalog identity.
    /// All other declarations and their order are retained. Identity sequences
    /// are internal column dependencies and cannot be dropped independently.
    /// This function owns no rows, counter state, or publication authority.
    pub fn schemaAlloc(self: @This(), backing: A, table: u64, before: []const u8) ![]u8 {
        return (DefaultRetirementSet{ .members = &.{self} }).schemaAlloc(backing, table, before);
    }
};

/// Advance a schema epoch without coercing the other JSON number lexemes.
/// Only the version field is converted to an integer in this owned DOM.
pub fn advanceSchemaVersion(root: *std.json.Value) !void {
    if (root.* != .object) return error.InvalidSequenceDefinition;
    const value = root.object.getPtr("version") orelse return error.InvalidSequenceDefinition;
    const version: u32 = switch (value.*) {
        .integer => |number| std.math.cast(u32, number) orelse return error.InvalidSequenceDefinition,
        .number_string => |text| std.fmt.parseInt(u32, text, 10) catch return error.InvalidSequenceDefinition,
        else => return error.InvalidSequenceDefinition,
    };
    if (version == std.math.maxInt(u32)) return error.InvalidSequenceDefinition;
    value.* = .{ .integer = version + 1 };
}

/// One schema-only postimage for a bounded counter cohort. Canonical membership
/// gives native admission and owner receipts the same immutable commitment.
/// This derives data only; it grants no counter or table mutation authority.
pub const DefaultRetirementSet = struct {
    pub const max_members = 256;
    members: []const DefaultRetirement,

    pub fn validate(self: @This()) !void {
        if (self.members.len == 0 or self.members.len > max_members) return error.InvalidSequenceDefinition;
        var previous: u64 = 0;
        for (self.members) |member| {
            try member.validate();
            if (member.sequence_id <= previous) return error.InvalidSequenceDefinition;
            previous = member.sequence_id;
        }
    }
    pub fn fingerprint(self: @This()) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sequence-default-retirement-set/v1\x00");
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, @intCast(self.members.len), .little);
        hash.update(&count);
        for (self.members) |member| hash.update(&try member.fingerprint());
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
    fn find(self: @This(), id: u64) ?DefaultRetirement {
        var left: usize = 0;
        var right = self.members.len;
        while (left < right) {
            const middle = left + (right - left) / 2;
            if (self.members[middle].sequence_id < id) left = middle + 1 else right = middle;
        }
        return if (left < self.members.len and self.members[left].sequence_id == id) self.members[left] else null;
    }

    /// Parse each declaration once and advance the table epoch only once,
    /// regardless of how many cohort counters this particular table borrows.
    pub fn schemaAlloc(self: @This(), backing: A, table: u64, before: []const u8) ![]u8 {
        try self.validate();
        if (table == 0) return error.InvalidSequenceDefinition;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var root = try std.json.parseFromSliceLeaky(std.json.Value, a, before, .{ .allocate = .alloc_always, .parse_numbers = false });
        try advanceSchemaVersion(&root);
        if (try self.removeFromOwnedValue(a, table, &root) == 0) return error.CatalogGenerationChanged;
        return std.json.Stringify.valueAlloc(backing, root, .{});
    }

    /// Mutates a caller-owned schema DOM. A participant may reference only a
    /// subset of the cohort, or none when its other dependencies are retired.
    /// The caller owns version advancement and must discard the DOM on error.
    pub fn removeFromOwnedValue(self: @This(), a: A, table: u64, root: *std.json.Value) !usize {
        try self.validate();
        if (table == 0 or root.* != .object) return error.InvalidSequenceDefinition;
        const defaults = root.object.getPtr("column_defaults") orelse return 0;
        if (defaults.* != .array or defaults.array.items.len > 1024) return error.InvalidSequenceDefinition;
        var removed: usize = 0;
        var kept: usize = 0;
        for (defaults.array.items) |entry| {
            if (entry != .object) return error.InvalidSequenceDefinition;
            const ref = entry.object.get("sequence");
            if (ref) |value| {
                var binding = try Binding.parse(a, value);
                defer binding.deinit();
                if (binding.value.owner.table_id != table) return error.InvalidSequenceDefinition;
                if (self.find(binding.value.sequence_id)) |selected| {
                    if (binding.value.namespace_id != selected.namespace_id or !std.mem.eql(u8, &binding.value.incarnation, &selected.incarnation)) return error.InvalidSequenceDefinition;
                    if (binding.value.generation != .serial) return error.SqlDependentObjectsExist;
                    removed += 1;
                    continue;
                }
            }
            defaults.array.items[kept] = entry;
            kept += 1;
        }
        defaults.array.items.len = kept;
        // Match ordinary DROP DEFAULT's canonical empty representation.
        if (removed != 0 and kept == 0) _ = root.object.orderedRemove("column_defaults");
        return removed;
    }
};

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: Binding,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Actual immutable row-preparation scope, supplied by a pinned table view.
/// It is independent of the schema's untrusted/descriptive sequence reference.
pub const Scope = struct {
    table_id: u64,
    schema_version: u32,
    storage_name: []const u8,
};

pub const Provider = struct {
    ptr: *anyopaque,
    /// Resolve and reauthorize the current immutable identity/owner before
    /// consuming a cache value or admitting a durable counter reservation.
    allocate: *const fn (*anyopaque, A, Scope, []const u8, Binding) anyerror!i64,
};

/// Read the exact durable default of an attribute. A table/column identity
/// alone never authorizes an allocation; absent/expression defaults carry no
/// sequence authority. Duplicate declarations are corruption, not first wins.
pub fn referenceForColumn(a: A, schema: std.json.Value, column: []const u8) !?Owned {
    if (schema != .object) return error.InvalidSequenceDefinition;
    const defaults = schema.object.get("column_defaults") orelse return null;
    if (defaults != .array) return error.InvalidSequenceDefinition;
    var result: ?Owned = null;
    errdefer if (result) |*value| value.deinit();
    var found = false;
    for (defaults.array.items) |entry| {
        if (entry != .object) return error.InvalidSequenceDefinition;
        const name = entry.object.get("column") orelse return error.InvalidSequenceDefinition;
        if (name != .string) return error.InvalidSequenceDefinition;
        if (!std.mem.eql(u8, name.string, column)) continue;
        if (found) return error.InvalidSequenceDefinition;
        found = true;
        if (entry.object.get("sequence")) |value| {
            if (entry.object.count() != 2) return error.InvalidSequenceDefinition;
            result = try Binding.parse(a, value);
        }
    }
    return result;
}

test "column sequence defaults require exact unique persisted attribute references" {
    const a = std.testing.allocator;
    const binding: Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 11, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "source_id_seq" } };
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .column_defaults = .{ .{ .column = "id", .sequence = binding }, .{ .column = "label", .expression = .{ .literal = "ready" } } } }, .{});
    defer a.free(encoded);
    var schema = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer schema.deinit();
    var reference = (try referenceForColumn(a, schema.value, "id")).?;
    defer reference.deinit();
    try std.testing.expectEqualDeep(binding, reference.value);
    try std.testing.expect((try referenceForColumn(a, schema.value, "label")) == null);
    try std.testing.expect((try referenceForColumn(a, schema.value, "missing")) == null);
    const defaults = &schema.value.object.getPtr("column_defaults").?.array;
    try defaults.append(defaults.items[0]);
    try std.testing.expectError(error.InvalidSequenceDefinition, referenceForColumn(a, schema.value, "id"));
    try std.testing.expectError(error.InvalidSequenceDefinition, referenceForColumn(a, .null, "id"));
}

test "column sequence defaults require exact retirement identities and preserve unrelated declarations" {
    const a = std.testing.allocator;
    const selected: DefaultRetirement = .{ .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1) };
    const binding: Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 11, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "source_id_seq" } };
    var other = binding;
    other.sequence_id = 12;
    other.owner.column_id = 4;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .version = 7, .storage_mode = "relational", .marker = "retained", .column_defaults = .{ .{ .column = "id", .sequence = binding }, .{ .column = "other", .sequence = other }, .{ .column = "label", .expression = .{ .literal = "ready" } } } }, .{});
    defer a.free(encoded);
    const after = try selected.schemaAlloc(a, 7, encoded);
    defer a.free(after);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, after, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 8), parsed.value.object.get("version").?.integer);
    try std.testing.expectEqualStrings("retained", parsed.value.object.get("marker").?.string);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("column_defaults").?.array.items.len);
    var retained = (try referenceForColumn(a, parsed.value, "other")).?;
    defer retained.deinit();
    try std.testing.expectEqualDeep(other, retained.value);
    try std.testing.expectError(error.CatalogGenerationChanged, selected.schemaAlloc(a, 7, after));
    try std.testing.expectError(error.InvalidSequenceDefinition, selected.schemaAlloc(a, 8, encoded));
    try std.testing.expectError(error.InvalidSequenceDefinition, (DefaultRetirement{ .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(2) }).schemaAlloc(a, 7, encoded));
    var identity = binding;
    identity.generation = .always;
    const internal = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .column_defaults = .{.{ .column = "id", .sequence = identity }} }, .{});
    defer a.free(internal);
    try std.testing.expectError(error.SqlDependentObjectsExist, selected.schemaAlloc(a, 7, internal));
}

test "column sequence retirement sets remove several counters in one epoch and preserve sparse unrelated defaults" {
    const a = std.testing.allocator;
    const one: DefaultRetirement = .{ .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1) };
    var two = one;
    two.sequence_id = 12;
    const selected: DefaultRetirementSet = .{ .members = &.{ one, two } };
    var binding: Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 11, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "source_id_seq" } };
    const first = binding;
    binding.sequence_id = 12;
    binding.owner.column_id = 4;
    const second = binding;
    binding.sequence_id = 13;
    binding.owner.column_id = 5;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .version = 7, .marker = "retained", .column_defaults = .{ .{ .column = "first", .sequence = first }, .{ .column = "second", .sequence = second }, .{ .column = "unrelated", .sequence = binding }, .{ .column = "label", .expression = .{ .literal = "ready" } } } }, .{});
    defer a.free(encoded);
    const after = try selected.schemaAlloc(a, 7, encoded);
    defer a.free(after);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, after, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 8), parsed.value.object.get("version").?.integer);
    try std.testing.expectEqualStrings("retained", parsed.value.object.get("marker").?.string);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("column_defaults").?.array.items.len);
    var retained = (try referenceForColumn(a, parsed.value, "unrelated")).?;
    defer retained.deinit();
    try std.testing.expectEqualDeep(binding, retained.value);
    // A participant may reference a subset of the cohort. Other tables prove
    // complete reverse membership at native admission, not by inventing cells.
    const sparse = try std.json.Stringify.valueAlloc(a, .{ .version = 2, .column_defaults = .{.{ .column = "first", .sequence = first }} }, .{});
    defer a.free(sparse);
    const sparse_after = try selected.schemaAlloc(a, 7, sparse);
    defer a.free(sparse_after);
    var sparse_parsed = try std.json.parseFromSlice(std.json.Value, a, sparse_after, .{});
    defer sparse_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 3), sparse_parsed.value.object.get("version").?.integer);
    try std.testing.expect(!sparse_parsed.value.object.contains("column_defaults"));
    try std.testing.expectError(error.CatalogGenerationChanged, selected.schemaAlloc(a, 7, after));
    try std.testing.expectError(error.InvalidSequenceDefinition, (DefaultRetirementSet{ .members = &.{ two, one } }).fingerprint());
    try std.testing.expectError(error.InvalidSequenceDefinition, (DefaultRetirementSet{ .members = &.{ one, one } }).validate());
    var moved = two;
    moved.namespace_id = 9;
    try std.testing.expectError(error.InvalidSequenceDefinition, (DefaultRetirementSet{ .members = &.{ one, moved } }).schemaAlloc(a, 7, encoded));
}

fn bindingJsonRoundTrip(a: A) !void {
    const original: Binding = .{ .generation = .always, .incarnation = @splat(0xab), .sequence_id = std.math.maxInt(u64), .namespace_id = 2, .owner = .{ .table_id = std.math.maxInt(u64) - 1, .column_id = 3 }, .target = .{ .table = "quoted_Ω_seq" } };
    const bytes = try std.json.Stringify.valueAlloc(a, original, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Binding, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(original, parsed.value);
    var tree = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer tree.deinit();
    var from_value = try std.json.parseFromValue(Binding, a, tree.value, .{});
    defer from_value.deinit();
    try std.testing.expectEqualDeep(original, from_value.value);
}

test "column sequence bindings round trip typed durable envelopes with exact IDs and allocation failures" {
    try bindingJsonRoundTrip(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, bindingJsonRoundTrip, .{});
}

test "column sequence retirement preserves unrelated numeric lexemes beyond floating precision" {
    const a = std.testing.allocator;
    const selected: DefaultRetirement = .{ .sequence_id = 11, .namespace_id = 2, .incarnation = @splat(1) };
    const binding: Binding = .{ .generation = .serial, .incarnation = @splat(1), .sequence_id = 11, .namespace_id = 2, .owner = .{ .table_id = 7, .column_id = 3 }, .target = .{ .table = "source_seq" } };
    const reference = try std.json.Stringify.valueAlloc(a, binding, .{});
    defer a.free(reference);
    const before = try std.fmt.allocPrint(a, "{{\"version\":7,\"column_defaults\":[{{\"column\":\"id\",\"sequence\":{s}}},{{\"column\":\"precision\",\"expression\":{{\"literal\":0.123456789012345678901234567890}}}}]}}", .{reference});
    defer a.free(before);
    const after = try selected.schemaAlloc(a, 7, before);
    defer a.free(after);
    try std.testing.expectEqualStrings("{\"version\":8,\"column_defaults\":[{\"column\":\"precision\",\"expression\":{\"literal\":0.123456789012345678901234567890}}]}", after);
}
