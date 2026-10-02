// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Canonical immutable constraint identities. Numeric schema epochs and
//! column ordinals are intentionally absent, so harmless layout evolution
//! retains the generation while comparison semantics cannot do so.
const std = @import("std");
const schema = @import("table_schema_impl.zig");
const native = @import("../storage/schema.zig");
const catalog = @import("../storage/db/relational_integrity_catalog.zig");

pub fn definitionFingerprints(alloc: std.mem.Allocator, public: schema.TableSchema, runtime: native.TableSchema) ![]catalog.Definition {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const arena = scratch.allocator();
    const uniques = try public.relationalUniqueDefinitions(arena);
    const foreign_keys = try public.relationalForeignKeyDefinitions(arena);
    // Constrained TTL is enforced by conditional, FK-aware transactions.
    // Until those commit, rows remain visible; a native-only owner without
    // the coordinator capability safely defers expiration.
    const definitions = try alloc.alloc(catalog.Definition, uniques.len + foreign_keys.len);
    errdefer alloc.free(definitions);
    var initialized: usize = 0;
    errdefer for (definitions[0..initialized]) |definition| alloc.free(definition.payload);
    var layout: ?@import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout = null;
    defer if (layout) |*value| value.deinit();
    for (uniques, definitions[0..uniques.len]) |unique, *definition| {
        if (unique.timing == .deferred and !unique.deferrable) return error.InvalidSchemaUpdateRequest;
        if (unique.keys.len == 0 and unique.where.len == 0 and !unique.deferrable) {
            definition.* = try definitionAlloc(alloc, .unique, unique.name, .{
                .domain = "antfly unique declaration v2 null witnesses",
                .name = unique.name,
                .columns = unique.columns,
                .column_types = try columnTypes(arena, runtime, unique.columns),
                .nulls_not_distinct = unique.nulls_not_distinct,
            });
            initialized += 1;
            continue;
        }
        if (layout == null) layout = try @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout.init(arena, runtime);
        const keys = if (unique.keys.len != 0) unique.keys else blk: {
            const values = try arena.alloc(@import("../storage/relational_index.zig").RelationalIndexKey, unique.columns.len);
            for (unique.columns, values) |column, *key| key.* = .{ .column = column };
            break :blk values;
        };
        var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(arena, runtime, &layout.?, keys);
        defer tuple.deinit();
        var predicate = if (unique.where.len != 0) try @import("../storage/db/relational_index_predicate.zig").Plan.init(arena, runtime, &layout.?, unique.where) else null;
        defer if (predicate) |*plan| plan.deinit();
        // Preserve generations of existing plain-column declarations. New
        // shapes use compiled identities, including referenced types and the
        // normalized predicate, rather than JSON spelling or column ordinals.
        definition.* = if (unique.deferrable) try definitionAlloc(alloc, .unique, unique.name, .{
            .domain = "antfly unique declaration v4 constraint timing",
            .name = unique.name,
            .tuple = tuple.fingerprint,
            .predicate = if (predicate) |plan| plan.identity else @as([32]u8, @splat(0)),
            .nulls_not_distinct = unique.nulls_not_distinct,
            .deferrable = unique.deferrable,
            .timing = @tagName(unique.timing),
        }) else try definitionAlloc(alloc, .unique, unique.name, .{
            .domain = "antfly unique declaration v3 typed keys and membership",
            .name = unique.name,
            .tuple = tuple.fingerprint,
            .predicate = if (predicate) |plan| plan.identity else @as([32]u8, @splat(0)),
            .nulls_not_distinct = unique.nulls_not_distinct,
        });
        initialized += 1;
    }
    for (foreign_keys, definitions[uniques.len..]) |foreign_key, *definition| {
        if (foreign_key.timing == .deferred and !foreign_key.deferrable) return error.InvalidSchemaUpdateRequest;
        if (foreign_key.on_delete == .set_null or foreign_key.on_update == .set_null) {
            for (foreign_key.child_columns) |name| {
                const column = for (runtime.relational_columns) |column| {
                    if (std.mem.eql(u8, column.name, name)) break column;
                } else return error.InvalidSchemaUpdateRequest;
                if (!column.allows_null) return error.InvalidSchemaUpdateRequest;
            }
        }
        definition.* = try definitionAlloc(alloc, .foreign_key, foreign_key.name, .{
            .domain = "antfly foreign key declaration v1",
            .name = foreign_key.name,
            .child_columns = foreign_key.child_columns,
            .column_types = try columnTypes(arena, runtime, foreign_key.child_columns),
            .parent_table = foreign_key.parent_table,
            .parent_columns = foreign_key.parent_columns,
            .on_delete = @tagName(foreign_key.on_delete),
            .on_update = @tagName(foreign_key.on_update),
            .timing = @tagName(foreign_key.timing),
            .deferrable = foreign_key.deferrable,
            .match = @tagName(foreign_key.match),
        });
        initialized += 1;
    }
    return definitions;
}

pub fn freeDefinitions(alloc: std.mem.Allocator, definitions: []const catalog.Definition) void {
    for (definitions) |definition| alloc.free(definition.payload);
    alloc.free(definitions);
}

/// Source projection shared by activation, retirement and the coordinator.
/// Expression operands and membership predicates are integrity dependencies.
pub fn uniqueFields(alloc: std.mem.Allocator, runtime: native.TableSchema, layout: *const @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout, unique: @import("../storage/relational_index.zig").UniqueConstraint) ![]const []const u8 {
    var fields: std.ArrayList([]const u8) = .empty;
    errdefer fields.deinit(alloc);
    try fields.appendSlice(alloc, unique.columns);
    if (unique.keys.len != 0) {
        var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(alloc, runtime, layout, unique.keys);
        defer tuple.deinit();
        for (tuple.keys) |key| {
            if (key.expression) |expression| {
                for (expression.plan.dependencies) |ordinal| try fields.append(alloc, runtime.relational_columns[ordinal].name);
            } else try fields.append(alloc, runtime.relational_columns[key.ordinal].name);
        }
    }
    for (unique.where) |condition| try fields.append(alloc, condition.field);
    return fields.toOwnedSlice(alloc);
}

fn columnTypes(alloc: std.mem.Allocator, runtime: native.TableSchema, names: []const []const u8) ![]const []const u8 {
    const types = try alloc.alloc([]const u8, names.len);
    for (names, types) |name, *kind| {
        const column = for (runtime.relational_columns) |column| {
            if (std.mem.eql(u8, column.name, name)) break column;
        } else return error.InvalidSchemaUpdateRequest;
        switch (column.column_type) {
            .string, .blob, .boolean, .datetime, .integer, .number => {},
            else => return error.InvalidSchemaUpdateRequest,
        }
        kind.* = @tagName(column.column_type);
    }
    return types;
}

fn definitionAlloc(alloc: std.mem.Allocator, kind: catalog.Kind, name: []const u8, value: anytype) !catalog.Definition {
    const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return .{ .kind = kind, .name = name, .fingerprint = result, .payload = bytes };
}

test "relational declarations own arrays and fingerprint logical identity" {
    const alloc = std.testing.allocator;
    var parsed = try schema.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["tenant","id"]}],"foreign_keys":[{"name":"parent","child_columns":["tenant","id"],"parent_table":"parents","parent_columns":["tenant","id"],"on_delete":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    const runtime = native.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "tenant", .path = "tenant", .column_type = .string },
        .{ .name = "id", .path = "id", .column_type = .integer },
    } };
    const first = try definitionFingerprints(alloc, parsed, runtime);
    defer freeDefinitions(alloc, first);
    var reordered = runtime;
    reordered.version = 2;
    reordered.relational_columns = &.{ runtime.relational_columns[1], runtime.relational_columns[0] };
    const second = try definitionFingerprints(alloc, parsed, reordered);
    defer freeDefinitions(alloc, second);
    try std.testing.expectEqualSlices(u8, &first[0].fingerprint, &second[0].fingerprint);
    try std.testing.expectEqualSlices(u8, &first[1].fingerprint, &second[1].fingerprint);
    parsed.ttl_duration_ns = 1;
    const ttl_definitions = try definitionFingerprints(alloc, parsed, runtime);
    defer freeDefinitions(alloc, ttl_definitions);
    try std.testing.expectEqualSlices(u8, &first[0].fingerprint, &ttl_definitions[0].fingerprint);
    @constCast(parsed.unique_constraints.?.value)[0].deferrable = true;
    const deferrable = try definitionFingerprints(alloc, parsed, runtime);
    defer freeDefinitions(alloc, deferrable);
    try std.testing.expect(!std.mem.eql(u8, &first[0].fingerprint, &deferrable[0].fingerprint));
    @constCast(parsed.unique_constraints.?.value)[0].timing = .deferred;
    const deferred = try definitionFingerprints(alloc, parsed, runtime);
    defer freeDefinitions(alloc, deferred);
    try std.testing.expect(!std.mem.eql(u8, &deferrable[0].fingerprint, &deferred[0].fingerprint));
    const moved = try definitionFingerprints(alloc, parsed, reordered);
    defer freeDefinitions(alloc, moved);
    try std.testing.expectEqualSlices(u8, &deferred[0].fingerprint, &moved[0].fingerprint);
}
