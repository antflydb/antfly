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

//! Exact declaration-only FK validation transitions. Preparing or verifying
//! this candidate proves no historical data, parent claim or publication cut.
const std = @import("std");
const Value = std.json.Value;
const equal = @import("json_shape.zig").equal;
pub const max_constraints = 256;

fn sameExcept(a: Value, b: Value, ignored: []const []const u8) bool {
    if (a != .object or b != .object) return false;
    for ([_]bool{ false, true }) |reverse| {
        const left = if (reverse) b else a;
        const right = if (reverse) a else b;
        var fields = left.object.iterator();
        while (fields.next()) |field| {
            const skip = for (ignored) |name| {
                if (std.mem.eql(u8, field.key_ptr.*, name)) break true;
            } else false;
            if (skip) continue;
            const other = right.object.get(field.key_ptr.*) orelse return false;
            if (!equal(field.value_ptr.*, other, 0)) return false;
        }
    }
    return true;
}
fn unvalidated(rule: Value) !bool {
    if (rule != .object) return error.InvalidSchemaUpdateRequest;
    const flag = rule.object.get("not_valid") orelse return false;
    if (flag != .bool) return error.InvalidSchemaUpdateRequest;
    return flag.bool;
}
fn nameOf(rule: Value) ![]const u8 {
    if (rule != .object) return error.InvalidSchemaUpdateRequest;
    const name = rule.object.get("name") orelse return error.InvalidSchemaUpdateRequest;
    if (name != .string or name.string.len == 0) return error.InvalidSchemaUpdateRequest;
    return name.string;
}
fn rules(value: Value) ![]Value {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    const list = value.object.get("foreign_keys") orelse return &.{};
    if (list != .array or list.array.items.len > max_constraints) return error.InvalidSchemaUpdateRequest;
    return list.array.items;
}
fn selected(names: []const []const u8, name: []const u8) bool {
    for (names) |candidate| if (std.mem.eql(u8, name, candidate)) return true;
    return false;
}
fn requireNames(names: []const []const u8) !void {
    if (names.len == 0 or names.len > max_constraints) return error.InvalidSchemaUpdateRequest;
    var bytes: usize = 0;
    for (names, 0..) |name, i| {
        bytes = std.math.add(usize, bytes, name.len) catch return error.InvalidSchemaUpdateRequest;
        if (name.len == 0 or bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSchemaUpdateRequest;
        for (names[0..i]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidSchemaUpdateRequest;
    }
}
pub const Summary = struct { changed: usize = 0 };

/// The owner separately authenticates the immutable schema epochs and full
/// source/parent cohort. Only the selected NOT VALID bits may be cleared.
pub fn verifyTransition(before: Value, after: Value, names: []const []const u8) !Summary {
    try requireNames(names);
    if (!sameExcept(before, after, &.{ "version", "foreign_keys" })) return error.InvalidSchemaUpdateRequest;
    const old = try rules(before);
    const next = try rules(after);
    if (old.len != next.len) return error.InvalidSchemaUpdateRequest;
    var found: usize = 0;
    var result: Summary = .{};
    for (old, next, 0..) |prior, current, i| {
        const name = try nameOf(prior);
        for (old[0..i]) |earlier| if (std.mem.eql(u8, name, try nameOf(earlier))) return error.InvalidSchemaUpdateRequest;
        if (!selected(names, name)) {
            if (!equal(prior, current, 0)) return error.InvalidSchemaUpdateRequest;
            continue;
        }
        found += 1;
        if (!sameExcept(prior, current, &.{"not_valid"}) or try unvalidated(current)) return error.InvalidSchemaUpdateRequest;
        if (try unvalidated(prior)) result.changed += 1;
    }
    if (found != names.len) return error.SqlConstraintNotFound;
    return result;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    schema: Value,
    summary: Summary,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prepare(a: std.mem.Allocator, before: Value, names: []const []const u8) !Prepared {
    try requireNames(names);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const bytes = try std.json.Stringify.valueAlloc(owned, before, .{});
    var after = try std.json.parseFromSliceLeaky(Value, owned, bytes, .{ .parse_numbers = false });
    const entries = try rules(after);
    for (names) |name| {
        const entry = for (entries) |*rule| {
            if (std.mem.eql(u8, try nameOf(rule.*), name)) break rule;
        } else {
            for ([_][]const u8{ "checks", "unique_constraints" }) |kind| {
                if (after.object.get(kind)) |others| {
                    if (others != .array) return error.InvalidSchemaUpdateRequest;
                    for (others.array.items) |rule| if (std.mem.eql(u8, try nameOf(rule), name)) return error.SqlWrongObjectType;
                }
            }
            return error.SqlConstraintNotFound;
        };
        if (try unvalidated(entry.*)) try entry.object.put(owned, "not_valid", .{ .bool = false });
    }
    const summary = try verifyTransition(before, after, names);
    return .{ .arena = arena, .schema = after, .summary = summary };
}

const fixture =
    \\{"version":1,"foreign_keys":[{"name":"fk","child_columns":["tenant","id"],"parent_table":"parent","parent_columns":["tenant","id"],"not_valid":true,"match":"full"}],"checks":[{"name":"check","expression":{"op":"literal","value":true}}]}
;

test "foreign key validation candidate owns exact validity-only changes without historical authority" {
    const a = std.testing.allocator;
    var old = try std.json.parseFromSlice(Value, a, fixture, .{});
    defer old.deinit();
    var candidate = try prepare(a, old.value, &.{"fk"});
    defer candidate.deinit();
    try std.testing.expectEqual(@as(usize, 1), candidate.summary.changed);
    const next = &candidate.schema.object.get("foreign_keys").?.array.items[0];
    try std.testing.expect(!try unvalidated(next.*));
    try std.testing.expect(try unvalidated(old.value.object.get("foreign_keys").?.array.items[0]));
    var no_op = try prepare(a, candidate.schema, &.{"fk"});
    defer no_op.deinit();
    try std.testing.expectEqual(@as(usize, 0), no_op.summary.changed);
    try std.testing.expectError(error.SqlWrongObjectType, prepare(a, old.value, &.{"check"}));
    try std.testing.expectError(error.SqlConstraintNotFound, prepare(a, old.value, &.{"missing"}));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, prepare(a, old.value, &.{ "fk", "fk" }));
    try next.object.put(candidate.arena.allocator(), "parent_table", .{ .string = "different" });
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, verifyTransition(old.value, candidate.schema, &.{"fk"}));
}
fn allocationCase(a: std.mem.Allocator) !void {
    var old = try std.json.parseFromSlice(Value, std.testing.allocator, fixture, .{});
    defer old.deinit();
    var result = try prepare(a, old.value, &.{"fk"});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.summary.changed);
}
test "foreign key validation candidate is allocation atomic" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}

/// Prepare catalog bytes from independently parsed before/after declarations.
/// The returned update retains all native generations. This establishes exact
/// schema/catalog identity only; it supplies no source, parent or install proof.
pub const CatalogPreparation = struct {
    alloc: std.mem.Allocator,
    before: @import("mod.zig").PreparedSchemaDefinition,
    after: @import("mod.zig").PreparedSchemaDefinition,
    catalog: @import("../storage/db/relational_integrity_catalog.zig").Update,

    pub fn deinit(self: *@This()) void {
        self.before.deinit(self.alloc);
        self.after.deinit(self.alloc);
        self.catalog.deinit();
        self.* = undefined;
    }
};

pub fn prepareCatalog(a: std.mem.Allocator, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, names: []const []const u8) !@import("../storage/db/relational_integrity_catalog.zig").Update {
    var prepared = try prepareCatalogOwned(a, table_id, before_json, after_json, predecessor, names);
    prepared.before.deinit(a);
    prepared.after.deinit(a);
    return prepared.catalog;
}

/// Retain both compiled declarations for owner preparation. Consumers borrow
/// these immutable definitions instead of parsing/compiling the schemas again.
pub fn prepareCatalogOwned(a: std.mem.Allocator, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, names: []const []const u8) !CatalogPreparation {
    const api = @import("mod.zig");
    const declarations = @import("relational_declarations.zig");
    const catalog = @import("../storage/db/relational_integrity_catalog.zig");
    const native_schema = @import("../storage/schema.zig");
    const limits = @import("../storage/db/relational_schema_owner_contract.zig");
    if (table_id == 0 or before_json.len == 0 or after_json.len == 0 or before_json.len > limits.max_schema or after_json.len > limits.max_schema or predecessor.len == 0 or predecessor.len > catalog.max_catalog_bytes) return error.InvalidSchemaUpdateRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(Value, scratch, before_json, .{ .parse_numbers = false });
    const next = try std.json.parseFromSliceLeaky(Value, scratch, after_json, .{ .parse_numbers = false });
    const summary = try verifyTransition(old, next, names);
    if (summary.changed != names.len) return error.InvalidSchemaUpdateRequest;
    var before = try api.PreparedSchemaDefinition.init(a, before_json);
    errdefer before.deinit(a);
    var after = try api.PreparedSchemaDefinition.init(a, after_json);
    errdefer after.deinit(a);
    if (before.runtime_schema.storage_mode != .relational or after.runtime_schema.storage_mode != .relational or after.runtime_schema.version != (std.math.add(u32, before.runtime_schema.version, 1) catch return error.InvalidSchemaUpdateRequest)) return error.InvalidSchemaUpdateRequest;
    var previous = try catalog.decode(a, predecessor);
    defer previous.deinit();
    const coverage: declarations.CatalogCoverage = if (std.mem.allEqual(u8, &previous.checks_digest, 0)) .legacy else .checks_v1;
    var compiled = try declarations.compileCatalogWithCoverage(a, before.public_schema, table_id, predecessor, coverage);
    defer compiled.deinit();
    if (!std.mem.eql(u8, predecessor, compiled.value)) return error.IntegrityCatalogChanged;
    const definitions = try declarations.definitionFingerprints(a, after.public_schema, after.runtime_schema);
    defer declarations.freeDefinitions(a, definitions);
    const bytes = try native_schema.serializeSchema(a, after.runtime_schema);
    defer a.free(bytes);
    const digest = @import("../storage/db/relational_integrity_contract.zig").hash(bytes);
    const update = try catalog.prepareForeignKeyValidation(a, predecessor, try catalog.incarnationFromTableId(table_id), after.runtime_schema.version, digest, definitions, names, previous.checks_digest);
    return .{ .alloc = a, .before = before, .after = after, .catalog = update };
}

test "foreign key validation declaration preparation preserves real compiler generations and rejects unproven catalog inputs" {
    const a = std.testing.allocator;
    const api = @import("mod.zig");
    const declarations = @import("relational_declarations.zig");
    const before_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"parent","parent_columns":["id"],"not_valid":true}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const after_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"parent","parent_columns":["id"],"not_valid":false}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    var before = try api.PreparedSchemaDefinition.init(a, before_json);
    defer before.deinit(a);
    var predecessor = try declarations.compileCatalog(a, before.public_schema, 71, null);
    defer predecessor.deinit();
    var prepared = try prepareCatalog(a, 71, before_json, after_json, predecessor.value, &.{"fk"});
    defer prepared.deinit();
    try std.testing.expectEqual(predecessor.catalog.next_generation, prepared.catalog.next_generation);
    for ([_]@import("../storage/db/relational_integrity_catalog.zig").Kind{ .unique, .foreign_key }, [_][]const u8{ "pk", "fk" }) |kind, name| {
        try std.testing.expectEqualSlices(u8, &predecessor.catalog.find(kind, name).?.generation, &prepared.catalog.find(kind, name).?.generation);
    }
    var after = try api.PreparedSchemaDefinition.init(a, after_json);
    defer after.deinit(a);
    var recovered = try declarations.compileCatalog(a, after.public_schema, 71, prepared.value);
    defer recovered.deinit();
    try std.testing.expectEqualSlices(u8, prepared.value, recovered.value);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, prepareCatalog(a, 71, before_json, before_json, predecessor.value, &.{"fk"}));
    try std.testing.expectError(error.IntegrityCatalogIncarnationMismatch, prepareCatalog(a, 72, before_json, after_json, predecessor.value, &.{"fk"}));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, prepareCatalog(a, 71, after_json, before_json, prepared.value, &.{"fk"}));
}
