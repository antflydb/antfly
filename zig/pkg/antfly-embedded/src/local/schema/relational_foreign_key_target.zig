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

//! Cross-table declaration validation shared by catalog publication and API
//! admission. UNIQUE/FK declarations use binary typed tuples; a collated or
//! partial secondary index is deliberately not a substitute for a UNIQUE.
const std = @import("std");
const schema = @import("mod.zig");
const native = @import("../storage/schema.zig");

/// Same immutable definition identity used by storage generation allocation.
/// Declaration edits cannot discard existing claims by first erasing the
/// authoritative metadata schema while shard publication rejects retirement.
pub fn retains(alloc: std.mem.Allocator, previous_json: []const u8, next_json: []const u8) !bool {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var previous = try schema.parseValidatedTableSchema(owned, previous_json);
    defer previous.deinit(owned);
    var next = try schema.parseValidatedTableSchema(owned, next_json);
    defer next.deinit(owned);
    const declarations = @import("relational_declarations.zig");
    const old = try declarations.definitionFingerprints(owned, previous, try schema.deriveRelationalCheckLayout(owned, previous));
    const replacement = try declarations.definitionFingerprints(owned, next, try schema.deriveRelationalCheckLayout(owned, next));
    for (old) |definition| {
        const retained = for (replacement) |candidate| {
            if (definition.kind == candidate.kind and std.mem.eql(u8, definition.name, candidate.name) and
                std.mem.eql(u8, &definition.fingerprint, &candidate.fingerprint)) break true;
        } else false;
        if (!retained) return false;
    }
    return true;
}

pub fn validate(alloc: std.mem.Allocator, child_json: []const u8, parent_name: []const u8, parent_json: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var child = try schema.parseValidatedTableSchema(owned, child_json);
    defer child.deinit(owned);
    var parent = try schema.parseValidatedTableSchema(owned, parent_json);
    defer parent.deinit(owned);
    if (child.storage_mode != .relational or parent.storage_mode != .relational)
        return error.ForeignKeyTargetNotUnique;
    const child_layout = try schema.deriveRelationalCheckLayout(owned, child);
    const parent_layout = try schema.deriveRelationalCheckLayout(owned, parent);
    const uniques = try parent.relationalUniqueDefinitions(owned);
    for (try child.relationalForeignKeyDefinitions(owned)) |fk| {
        if (!std.mem.eql(u8, fk.parent_table, parent_name)) continue;
        const unique_found = for (uniques) |unique| {
            if (unique.keys.len != 0 or unique.where.len != 0 or unique.deferrable) continue;
            if (sameColumns(fk.parent_columns, unique.columns)) break true;
        } else false;
        if (!unique_found) return error.ForeignKeyTargetNotUnique;
        if (fk.match == .partial) try @import("relational_witness_indexes.zig").requireCoverage(parent, fk.parent_columns);
        if (fk.child_columns.len == 0 or fk.child_columns.len != fk.parent_columns.len)
            return error.ForeignKeyTypeMismatch;
        for (fk.child_columns, fk.parent_columns) |child_column, parent_column| {
            if (try columnType(child_layout, child_column) != try columnType(parent_layout, parent_column))
                return error.ForeignKeyTypeMismatch;
        }
    }
}

fn sameColumns(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (!std.mem.eql(u8, left, right)) return false;
    return true;
}

fn columnType(layout: native.TableSchema, name: []const u8) !native.RelationalColumnType {
    const column = for (layout.relational_columns) |candidate| {
        if (std.mem.eql(u8, candidate.name, name)) break candidate;
    } else return error.ForeignKeyTypeMismatch;
    return switch (column.column_type) {
        .string, .blob, .boolean, .datetime, .integer, .number => column.column_type,
        else => error.ForeignKeyTypeMismatch,
    };
}

test "relational integrity FK target validates ordered uniqueness types and self references" {
    try testTargetContract();
}

test "distributed txn MATCH PARTIAL DDL pins selectable parent support for every non-null mask" {
    const alloc = std.testing.allocator;
    const child =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["a","b"],"parent_table":"parents","parent_columns":["a","b"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const parent =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["a","b"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    try std.testing.expectError(error.ForeignKeyPartialSupportIndexRequired, validate(alloc, child, "parents", parent));
    const supported = (try @import("relational_witness_indexes.zig").ensureCoverage(alloc, parent, &.{ "a", "b" })).?;
    defer alloc.free(supported);
    try validate(alloc, child, "parents", supported);
    // The identical validator runs on incoming dependencies under the parent
    // metadata CAS, so removing the support definitions cannot race admission.
    try std.testing.expectError(error.ForeignKeyPartialSupportIndexRequired, validate(alloc, child, "parents", parent));
}

pub fn testTargetContract() !void {
    const child =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["tenant","id"],"parent_table":"parents","parent_columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const parent =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try validate(std.testing.allocator, child, "parents", parent);
    const reversed = try std.mem.replaceOwned(u8, std.testing.allocator, parent, "[\"tenant\",\"id\"]", "[\"id\",\"tenant\"]");
    defer std.testing.allocator.free(reversed);
    try std.testing.expectError(error.ForeignKeyTargetNotUnique, validate(std.testing.allocator, child, "parents", reversed));
    const mismatch = try std.mem.replaceOwned(u8, std.testing.allocator, parent, "integer", "number");
    defer std.testing.allocator.free(mismatch);
    try std.testing.expectError(error.ForeignKeyTypeMismatch, validate(std.testing.allocator, child, "parents", mismatch));
    const self_schema = try std.mem.replaceOwned(u8, std.testing.allocator, child, "\"foreign_keys\":", "\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"tenant\",\"id\"]}],\"foreign_keys\":");
    defer std.testing.allocator.free(self_schema);
    try validate(std.testing.allocator, self_schema, "parents", self_schema);
}
