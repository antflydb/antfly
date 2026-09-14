// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Typed dependency expansion, shared by HTTP mutations and integrity workers.
//! A catalog supplies durable generation bindings; no client supplied state or
//! schema-version hash may manufacture a claim generation. The result must be
//! compiled on each claim owner and enlisted alongside the primary mutations in
//! the existing durable distributed transaction protocol.
const std = @import("std");
const registry = @import("../storage/db/schema_registry.zig");
const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
const tuples = @import("../storage/db/relational_index_keys.zig");
const native = @import("../storage/relational_index.zig");
pub const storage = @import("../storage/db/relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;

pub const UniqueBinding = struct {
    generation: storage.Generation,
    definition: native.UniqueConstraint,
};
pub const ForeignBinding = struct {
    generation: storage.Generation,
    parent_generation: storage.Generation,
    definition: native.ForeignKey,
    /// A pinned parent epoch proves exact type compatibility. The generation
    /// must identify this parent's declared unique key, in the same order.
    parent: registry.SchemaView,
    parent_unique: native.UniqueConstraint,
};
const BoundUnique = struct { generation: storage.Generation, definition: native.UniqueConstraint, tuple: tuples.TuplePlan };
const BoundForeign = struct { generation: storage.Generation, parent_generation: storage.Generation, definition: native.ForeignKey, tuple: tuples.TuplePlan };

pub const Mutation = struct {
    key: []const u8,
    before: ?codec.OrdinalRowView = null,
    after: ?codec.OrdinalRowView = null,
    repair: bool = false,
};

pub const RoutedCommand = struct { table_name: []const u8, command: storage.Command };
pub const ParentTransition = struct {
    table_name: []const u8,
    parent_key: []const u8,
    address: storage.Address,
    target_tuple: ?[]const u8,
};

pub const Expansion = struct {
    arena: std.heap.ArenaAllocator,
    commands: []const RoutedCommand,
    /// Parent transitions cannot be committed as ordinary row deletion. The
    /// coordinator first resolves incoming descriptors and either proves
    /// RESTRICT or schedules a gated, bounded parent-action job.
    parents: []const ParentTransition,
    pub fn deinit(self: *Expansion) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    view: registry.SchemaView,
    table_name: []const u8,
    uniques: []const BoundUnique,
    foreign: []const BoundForeign,

    pub fn init(alloc: Allocator, table_name: []const u8, view: registry.SchemaView, uniques: []const UniqueBinding, foreign: []const ForeignBinding) !Plan {
        if (view.storageMode() != .relational or table_name.len == 0 or uniques.len + foreign.len > 256) return error.InvalidIntegrityDefinition;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const bound_uniques = try owned.alloc(BoundUnique, uniques.len);
        for (uniques, bound_uniques) |binding, *bound| {
            const def = binding.definition;
            if (std.mem.allEqual(u8, &binding.generation, 0) or def.columns.len == 0 or def.expressions.len != 0 or
                def.include_columns.len != 0 or def.where.len != 0 or def.where_expressions.len != 0 or
                def.without_overlaps_period != null or def.deferrable or def.timing != .immediate) return error.UnsupportedIntegrityDefinition;
            var copied = def;
            copied.name = try owned.dupe(u8, def.name);
            copied.columns = try copyColumns(owned, def.columns);
            bound.* = .{ .generation = binding.generation, .definition = copied, .tuple = try bindTuple(owned, view, def.columns) };
        }
        const bound_foreign = try owned.alloc(BoundForeign, foreign.len);
        for (foreign, bound_foreign) |binding, *bound| {
            const def = binding.definition;
            if (std.mem.allEqual(u8, &binding.generation, 0) or std.mem.allEqual(u8, &binding.parent_generation, 0) or
                def.child_columns.len == 0 or def.child_columns.len != def.parent_columns.len or
                def.child_period != null or def.parent_period != null or def.match == .partial or def.deferrable or def.timing != .immediate)
                return error.UnsupportedIntegrityDefinition;
            if (!sameColumns(def.parent_columns, binding.parent_unique.columns)) return error.ForeignKeyTargetNotUnique;
            var parent_tuple = try bindTuple(owned, binding.parent, def.parent_columns);
            defer parent_tuple.deinit();
            const child_tuple = try bindTuple(owned, view, def.child_columns);
            for (child_tuple.keys, parent_tuple.keys) |child, parent| {
                if (child.column_type != parent.column_type) return error.ForeignKeyTypeMismatch;
            }
            var copied = def;
            copied.name = try owned.dupe(u8, def.name);
            copied.child_columns = try copyColumns(owned, def.child_columns);
            copied.parent_table = try owned.dupe(u8, def.parent_table);
            copied.parent_columns = try copyColumns(owned, def.parent_columns);
            bound.* = .{ .generation = binding.generation, .parent_generation = binding.parent_generation, .definition = copied, .tuple = child_tuple };
        }
        const owned_name = try owned.dupe(u8, table_name);
        return .{ .arena = arena, .view = view.clone(), .table_name = owned_name, .uniques = bound_uniques, .foreign = bound_foreign };
    }

    pub fn deinit(self: *Plan) void {
        self.view.release();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn expand(self: *const Plan, alloc: Allocator, mutations: []const Mutation) !Expansion {
        if (mutations.len > 4096) return error.TransactionTooLarge;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var commands = std.ArrayList(RoutedCommand).empty;
        var parents = std.ArrayList(ParentTransition).empty;
        const table_name = try owned.dupe(u8, self.table_name);
        for (mutations) |mutation| {
            const key = try owned.dupe(u8, mutation.key);
            if (mutation.after) |after| if (after.layout != self.view.physicalLayout() or
                after.table_schema.relational_columns.ptr != self.view.tableSchema().relational_columns.ptr) return error.PreparedGenerationChanged;
            for (self.uniques) |unique| {
                const before = try encode(owned, unique.tuple, mutation.before, !unique.definition.nulls_not_distinct, false);
                const after = try encode(owned, unique.tuple, mutation.after, !unique.definition.nulls_not_distinct, false);
                if (sameTuple(before, after) and !mutation.repair) {
                    if (after) |tuple| try commands.append(owned, .{ .table_name = table_name, .command = .{
                        .address = try storage.Address.init(unique.generation, tuple),
                        .operation = .{ .check_owner = .{ .parent_table = table_name, .parent_key = key } },
                    } });
                    continue;
                }
                if (!sameTuple(before, after)) if (before) |tuple| try parents.append(owned, .{ .table_name = table_name, .parent_key = key, .address = try storage.Address.init(unique.generation, tuple), .target_tuple = after });
                if (after) |tuple| try commands.append(owned, .{ .table_name = table_name, .command = .{
                    .address = try storage.Address.init(unique.generation, tuple),
                    .operation = .{ .establish = .{ .tuple = tuple, .parent_table = table_name, .parent_key = key, .schema_version = self.view.version() } },
                } });
            }
            for (self.foreign) |foreign| {
                const before = try encode(owned, foreign.tuple, mutation.before, true, foreign.definition.match == .full and !mutation.repair);
                const after = try encode(owned, foreign.tuple, mutation.after, true, foreign.definition.match == .full);
                const reference: storage.Reference = .{
                    .child_table = table_name,
                    .child_key = key,
                    .constraint_name = try owned.dupe(u8, foreign.definition.name),
                    .constraint_generation = foreign.generation,
                };
                const parent_table = try owned.dupe(u8, foreign.definition.parent_table);
                if (!sameTuple(before, after)) if (before) |tuple| try commands.append(owned, .{ .table_name = parent_table, .command = .{ .address = try storage.Address.init(foreign.parent_generation, tuple), .operation = if (mutation.repair) .{ .repair_detach = reference } else .{ .detach = reference } } });
                if (after) |tuple| try commands.append(owned, .{ .table_name = parent_table, .command = .{ .address = try storage.Address.init(foreign.parent_generation, tuple), .operation = .{ .attach = reference } } });
            }
            if (commands.items.len + parents.items.len > storage.max_commands) return error.TransactionTooLarge;
        }
        return .{ .arena = arena, .commands = try commands.toOwnedSlice(owned), .parents = try parents.toOwnedSlice(owned) };
    }
};

fn sameTuple(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |bytes| if (b) |other| std.mem.eql(u8, bytes, other) else false else b == null;
}

fn copyColumns(alloc: Allocator, columns: []const []const u8) ![]const []const u8 {
    const copied = try alloc.alloc([]const u8, columns.len);
    for (columns, copied) |column, *copy| copy.* = try alloc.dupe(u8, column);
    return copied;
}

fn sameColumns(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (!std.mem.eql(u8, left, right)) return false;
    return true;
}

fn bindTuple(alloc: Allocator, view: registry.SchemaView, columns: []const []const u8) !tuples.TuplePlan {
    const keys = try alloc.alloc(native.RelationalIndexKey, columns.len);
    for (columns, keys) |column, *key| key.* = .{ .column = column };
    return tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), keys);
}

fn encode(alloc: Allocator, current: tuples.TuplePlan, optional_row: ?codec.OrdinalRowView, skip_null: bool, match_full: bool) !?[]const u8 {
    const row = optional_row orelse return null;
    var source = if (row.layout == current.layout) current else try current.projectSource(alloc, row.table_schema, row.layout);
    defer if (row.layout != current.layout) source.deinit();
    var result = try source.encodeAlloc(alloc, row);
    if (skip_null and result.has_null) {
        defer result.deinit(alloc);
        if (match_full) {
            var nonnull = false;
            for (source.keys) |key| if (try row.findCell(key.ordinal)) |cell| {
                if (!cell.is_null) nonnull = true;
            };
            if (nonnull) return error.ForeignKeyMatchFullViolation;
        }
        return null;
    }
    return result.bytes;
}

test "distributed txn typed integrity expansion matches composite parent claim without float conversion" {
    const alloc = std.testing.allocator;
    const schema = @import("../storage/schema.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "tenant", .path = "tenant", .column_type = .string },
        .{ .name = "id", .path = "id", .column_type = .integer },
    };
    var view = registry.SchemaView{ .epoch = try registry.Epoch.createCloned(alloc, .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns }) };
    defer view.release();
    const unique: native.UniqueConstraint = .{ .name = "identity", .columns = &.{ "tenant", "id" } };
    const fk: native.ForeignKey = .{ .name = "parent", .child_columns = &.{ "tenant", "id" }, .parent_table = "parents", .parent_columns = &.{ "tenant", "id" }, .match = .full };
    var parent = try Plan.init(alloc, "parents", view, &.{.{ .generation = @splat(1), .definition = unique }}, &.{});
    defer parent.deinit();
    var child = try Plan.init(alloc, "children", view, &.{}, &.{.{ .generation = @splat(2), .parent_generation = @splat(1), .definition = fk, .parent = view, .parent_unique = unique }});
    defer child.deinit();
    const encoded = try codec.serializeOrdinal(alloc, 1, view.tableSchema().relational_columns, &.{
        .{ .ordinal = 0, .path = "tenant", .value_type = .bytes_val, .value = .{ .bytes_val = "Acme" } },
        .{ .ordinal = 1, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9_007_199_254_740_993 } },
    }, @splat(0));
    defer alloc.free(encoded);
    const row = try codec.ordinalRowView(encoded, view.tableSchema().*, view.physicalLayout());
    var parent_insert = try parent.expand(alloc, &.{.{ .key = "p1", .after = row }});
    defer parent_insert.deinit();
    var child_insert = try child.expand(alloc, &.{.{ .key = "c1", .after = row }});
    defer child_insert.deinit();
    try std.testing.expectEqual(@as(usize, 1), parent_insert.commands.len);
    try std.testing.expectEqual(@as(usize, 1), child_insert.commands.len);
    try std.testing.expectEqualDeep(parent_insert.commands[0].command.address, child_insert.commands[0].command.address);
    try std.testing.expectEqualStrings("parents", child_insert.commands[0].table_name);
    try std.testing.expectEqualStrings("children", child_insert.commands[0].command.operation.attach.child_table);
    var unchanged = try child.expand(alloc, &.{.{ .key = "c1", .before = row, .after = row }});
    defer unchanged.deinit();
    try std.testing.expectEqual(@as(usize, 1), unchanged.commands.len);
    var parent_delete = try parent.expand(alloc, &.{.{ .key = "p1", .before = row }});
    defer parent_delete.deinit();
    try std.testing.expectEqual(@as(usize, 1), parent_delete.parents.len);
    try std.testing.expectEqual(@as(usize, 0), parent_delete.commands.len);

    const partial = try codec.serializeOrdinal(alloc, 1, view.tableSchema().relational_columns, &.{
        .{ .ordinal = 0, .path = "tenant", .value_type = .bytes_val, .value = .{ .bytes_val = "Acme" } },
    }, @splat(0));
    defer alloc.free(partial);
    const partial_row = try codec.ordinalRowView(partial, view.tableSchema().*, view.physicalLayout());
    try std.testing.expectError(error.ForeignKeyMatchFullViolation, child.expand(alloc, &.{.{ .key = "c2", .after = partial_row }}));
}
