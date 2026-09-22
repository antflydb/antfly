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

//! Owned MATCH PARTIAL support uses ordinary schema indexes and their durable
//! build/drop jobs. Parent CAS precedes child publication; metadata admission
//! rechecks dependencies, so concurrent cleanup causes a retry, never an
//! unindexed admitted dependency. Unreferenced definitions are recoverable
//! after a failed request without an in-memory cleanup receipt.
const std = @import("std");
const Allocator = std.mem.Allocator;
const schema = @import("../schema/mod.zig");
const support = @import("../schema/relational_witness_indexes.zig");
const records = @import("../common/topology_records.zig");
const tables_api = @import("tables.zig");
const wire = @import("antfly_schema_openapi");

pub const Parent = struct { before: records.TableRecord, after: records.TableRecord };
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    schema_json: []const u8,
    parents: []const Parent,
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn reserved(name: []const u8) bool {
    return std.mem.startsWith(u8, name, support.owned_prefix);
}

pub fn validateArtifactNames(alloc: Allocator, json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidCreateTableRequest;
    for (parsed.value.object.keys()) |name| if (reserved(name)) return error.ReservedForeignKeySupportIndex;
}

pub fn needed(alloc: Allocator, proposed: []const u8, before: []const u8) !bool {
    for ([_][]const u8{ proposed, before }) |json| {
        if (json.len == 0) continue;
        var parsed = try schema.parseValidatedTableSchema(alloc, json);
        defer parsed.deinit(alloc);
        if (parsed.foreign_keys) |keys| for (keys.value) |fk| if ((fk.match orelse .simple) == .partial) return true;
        if (parsed.relational_indexes) |indexes| for (indexes.value) |index| if (reserved(index.name)) return true;
    }
    return false;
}

fn canonical(alloc: Allocator, value: std.json.Value) ![]const u8 {
    const parsed = try std.json.parseFromValue(wire.RelationalIndexDefinition, alloc, value, .{});
    return std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
}

/// Admit the owned namespace before fetching metadata or changing parents.
/// Callers validate the complete proposed schema through needed() first.
pub fn validatePublicSchema(alloc: Allocator, before: []const u8, proposed: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    _ = try preserveOwned(arena.allocator(), before, proposed);
}

/// Public replacements may omit server-owned definitions, but cannot forge
/// or edit them. Carry them forward independently of the user representation.
fn preserveOwned(alloc: Allocator, before: []const u8, proposed: []const u8) ![]const u8 {
    const old = try std.json.parseFromSlice(std.json.Value, alloc, if (before.len == 0) "{}" else before, .{ .parse_numbers = false });
    var next = try std.json.parseFromSlice(std.json.Value, alloc, proposed, .{ .parse_numbers = false });
    if (old.value != .object or next.value != .object) return error.InvalidSchemaUpdateRequest;
    var owned = std.StringHashMapUnmanaged(std.json.Value).empty;
    if (old.value.object.get("relational_indexes")) |indexes| if (indexes != .null) {
        for (indexes.array.items) |definition| {
            const name = definition.object.get("name").?.string;
            if (reserved(name)) try owned.put(alloc, name, definition);
        }
    };
    var definitions: std.array_list.Managed(std.json.Value) = .init(alloc);
    if (next.value.object.get("relational_indexes")) |indexes| if (indexes != .null) {
        for (indexes.array.items) |definition| {
            const name = definition.object.get("name").?.string;
            if (reserved(name)) {
                const previous = owned.get(name) orelse return error.ReservedForeignKeySupportIndex;
                if (!std.mem.eql(u8, try canonical(alloc, previous), try canonical(alloc, definition))) return error.ReservedForeignKeySupportIndex;
                _ = owned.remove(name);
            }
            try definitions.append(definition);
        }
    };
    if (old.value.object.get("relational_indexes")) |indexes| if (indexes != .null) {
        for (indexes.array.items) |definition| {
            if (owned.contains(definition.object.get("name").?.string)) try definitions.append(definition);
        }
    };
    if (definitions.items.len != 0 or next.value.object.contains("relational_indexes"))
        try next.value.object.put(alloc, "relational_indexes", .{ .array = definitions });
    return std.json.Stringify.valueAlloc(alloc, next.value, .{});
}

pub fn prepare(alloc: Allocator, tables: []const records.TableRecord, child_name: []const u8, proposed: []const u8, before: []const u8) !Plan {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var child = try schema.parseValidatedTableSchema(owned, proposed);
    defer child.deinit(owned);
    var child_json = try preserveOwned(owned, before, proposed);
    var parents = std.ArrayList(Parent).empty;
    for (try child.relationalForeignKeyDefinitions(owned)) |fk| {
        if (fk.match != .partial) continue;
        if (std.mem.eql(u8, child_name, fk.parent_table)) {
            if (try support.ensureCoverage(owned, child_json, fk.parent_columns)) |updated| child_json = updated;
            continue;
        }
        const parent = for (tables) |table| {
            if (std.mem.eql(u8, table.name, fk.parent_table)) break table;
        } else return error.ForeignKeyParentTableNotFound;
        if (parent.relational_retirement_json.len != 0 or parent.restore_backup_id.len != 0) return error.TableTransitionActive;
        const position = for (parents.items, 0..) |item, i| {
            if (item.before.table_id == parent.table_id) break i;
        } else null;
        const current = if (position) |i| parents.items[i].after.schema_json else parent.schema_json;
        if (try support.ensureCoverage(owned, current, fk.parent_columns)) |updated| {
            const replacement = try tables_api.applySchemaUpdateRecord(owned, &parent, updated);
            if (position) |i| parents.items[i].after = replacement else try parents.append(owned, .{ .before = parent, .after = replacement });
        }
    }
    // Validate every target before making the first external schema change.
    for (try child.relationalForeignKeyDefinitions(owned)) |fk| {
        const parent_json = if (std.mem.eql(u8, child_name, fk.parent_table)) child_json else blk: {
            for (parents.items) |parent| if (std.mem.eql(u8, parent.before.name, fk.parent_table)) break :blk parent.after.schema_json;
            for (tables) |table| if (std.mem.eql(u8, table.name, fk.parent_table)) break :blk table.schema_json;
            return error.ForeignKeyParentTableNotFound;
        };
        try @import("../schema/relational_foreign_key_target.zig").validate(owned, child_json, fk.parent_table, parent_json);
    }
    return .{ .arena = arena, .schema_json = child_json, .parents = try parents.toOwnedSlice(owned) };
}

/// One parent per maintenance turn. The final exact metadata CAS repeats
/// incoming current/read-schema validation against its current transaction.
pub fn cleanup(alloc: Allocator, tables: []const records.TableRecord, parent: records.TableRecord) !?records.TableRecord {
    if (parent.schema_json.len == 0 or parent.relational_retirement_json.len != 0 or parent.restore_backup_id.len != 0) return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, owned, parent.schema_json, .{ .parse_numbers = false });
    const indexes = parsed.value.object.get("relational_indexes") orelse return null;
    if (indexes == .null) return null;
    const has_owned = for (indexes.array.items) |definition| {
        if (reserved(definition.object.get("name").?.string)) break true;
    } else false;
    if (!has_owned) return null;
    var referenced = std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)).empty;
    // Decode only dependency descriptors, once per catalog row, and release
    // each temporary parse immediately. Never repeat a full schema/compiler
    // pass for each support column.
    const Dependencies = struct { foreign_keys: ?[]const struct { parent_table: []const u8, parent_columns: []const []const u8, match: ?wire.ForeignKeyMatch = null } = null };
    for (tables) |table| for ([_][]const u8{ table.schema_json, table.read_schema_json }) |json| {
        if (json.len == 0) continue;
        var candidate = try std.json.parseFromSlice(Dependencies, alloc, json, .{ .ignore_unknown_fields = true });
        defer candidate.deinit();
        for (candidate.value.foreign_keys orelse &.{}) |fk| {
            if ((fk.match orelse .simple) != .partial or !std.mem.eql(u8, fk.parent_table, parent.name)) continue;
            for (fk.parent_columns) |column| {
                const entry = try referenced.getOrPut(owned, column);
                if (!entry.found_existing) {
                    entry.key_ptr.* = try owned.dupe(u8, column);
                    entry.value_ptr.* = .empty;
                }
                for (fk.parent_columns) |other| if (!std.mem.eql(u8, other, column) and !entry.value_ptr.contains(other))
                    try entry.value_ptr.put(owned, try owned.dupe(u8, other), {});
            }
        }
    };
    var kept: std.array_list.Managed(std.json.Value) = .init(owned);
    var changed = false;
    for (indexes.array.items) |definition| {
        const name = definition.object.get("name").?.string;
        if (!reserved(name)) {
            try kept.append(definition);
            continue;
        }
        const index = try std.json.parseFromValue(wire.RelationalIndexDefinition, owned, definition, .{});
        if (index.value.keys.len != 1 or index.value.keys[0].column == null or !std.mem.eql(u8, name, &support.supportName(index.value.keys[0].column.?))) return error.InvalidIntegrityDefinition;
        const column = index.value.keys[0].column.?;
        if (referenced.get(column)) |covers| {
            const existing = index.value.include_columns orelse &.{};
            const same = same_covers: {
                if (existing.len != covers.count()) break :same_covers false;
                for (existing) |covered| if (!covers.contains(covered)) break :same_covers false;
                break :same_covers true;
            };
            if (same) {
                try kept.append(definition);
            } else {
                // Shared support follows the union of live dependencies.
                // Retired cover columns must not pin unrelated later DDL.
                const columns = try owned.alloc([]const u8, covers.count());
                var iterator = covers.keyIterator();
                var i: usize = 0;
                while (iterator.next()) |covered| : (i += 1) columns[i] = covered.*;
                std.mem.sort([]const u8, columns, {}, struct {
                    fn less(_: void, a: []const u8, b: []const u8) bool {
                        return std.mem.lessThan(u8, a, b);
                    }
                }.less);
                var next = index.value;
                next.include_columns = columns;
                const encoded = try std.json.Stringify.valueAlloc(owned, next, .{ .emit_null_optional_fields = false });
                const value = try std.json.parseFromSlice(std.json.Value, owned, encoded, .{});
                try kept.append(value.value);
                changed = true;
            }
        } else changed = true;
    }
    if (!changed) return null;
    try parsed.value.object.put(owned, "relational_indexes", .{ .array = kept });
    return try tables_api.applySchemaUpdateRecord(alloc, &parent, try std.json.Stringify.valueAlloc(owned, parsed.value, .{}));
}

test "distributed txn owned witness DDL resumes parent preparation and cleans failed child creation without weakening dependency races" {
    const alloc = std.testing.allocator;
    const parent_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["a","b"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const child_json =
        \\{"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["a","b"],"parent_table":"parents","parent_columns":["a","b"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const parent: records.TableRecord = .{ .table_id = 1, .name = "parents", .placement_role = "data", .schema_json = parent_json };
    const nullable_json = try std.mem.replaceOwned(u8, alloc, parent_json, "\"version\":1,", "\"version\":1,\"relational_indexes\":null,\"foreign_keys\":null,");
    defer alloc.free(nullable_json);
    var nullable_parent = parent;
    nullable_parent.schema_json = nullable_json;
    // The authoritative schema parser forbids explicit-null declaration
    // arrays; automatic support must not silently normalize invalid input.
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, prepare(alloc, &.{nullable_parent}, "children", child_json, ""));
    var plan = try prepare(alloc, &.{parent}, "children", child_json, "");
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 1), plan.parents.len);
    try std.testing.expectEqual(@as(u32, 2), try tables_api.schemaVersion(plan.parents[0].after.schema_json));
    const published_parent = plan.parents[0].after;
    // Lost response/restart: a second plan reuses the exact existing support.
    var resumed = try prepare(alloc, &.{published_parent}, "children", child_json, "");
    defer resumed.deinit();
    try std.testing.expectEqual(@as(usize, 0), resumed.parents.len);
    // Failed child create leaves no declaration. Cleanup is derivable from
    // durable catalog state rather than a process-local rollback callback.
    const empty_document: records.TableRecord = .{ .table_id = 3, .name = "documents", .schema_json = "{}" };
    const collected = (try cleanup(alloc, &.{ published_parent, empty_document }, published_parent)).?;
    defer @import("../metadata/table_manager.zig").freeTable(alloc, collected);
    var child: records.TableRecord = .{ .table_id = 2, .name = "children", .placement_role = "data", .schema_json = plan.schema_json };
    try std.testing.expect((try cleanup(alloc, &.{ published_parent, child }, published_parent)) == null);
    // A child winning after the collector's snapshot causes authoritative
    // metadata admission to reject the stale support-drop candidate.
    try std.testing.expectError(error.ForeignKeyPartialSupportIndexRequired, @import("../schema/relational_foreign_key_target.zig").validate(alloc, child.schema_json, "parents", collected.schema_json));
    child.read_schema_json = child.schema_json;
    child.schema_json = "{}";
    try std.testing.expect((try cleanup(alloc, &.{ published_parent, child }, published_parent)) == null);
    // Public replacements omit server-owned indexes safely, but cannot mint
    // them or change their immutable definitions.
    var preserved = try prepare(alloc, &.{published_parent}, "parents", parent_json, published_parent.schema_json);
    defer preserved.deinit();
    var parsed = try schema.parseValidatedTableSchema(alloc, preserved.schema_json);
    defer parsed.deinit(alloc);
    try support.requireCoverage(parsed, &.{ "a", "b" });
    try std.testing.expectError(error.ReservedForeignKeySupportIndex, prepare(alloc, &.{}, "forged", published_parent.schema_json, ""));
    const forged_artifact = try std.fmt.allocPrint(alloc, "{{\"{s}\":{{}}}}", .{support.supportName("a")});
    defer alloc.free(forged_artifact);
    try std.testing.expectError(error.ReservedForeignKeySupportIndex, validateArtifactNames(alloc, forged_artifact));
    const self_json = try std.mem.replaceOwned(u8, alloc, child_json, "\"foreign_keys\":", "\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"a\",\"b\"]}],\"foreign_keys\":");
    defer alloc.free(self_json);
    var self = try prepare(alloc, &.{}, "parents", self_json, "");
    defer self.deinit();
    try std.testing.expectEqual(@as(usize, 0), self.parents.len);
    var self_schema = try schema.parseValidatedTableSchema(alloc, self.schema_json);
    defer self_schema.deinit(alloc);
    try support.requireCoverage(self_schema, &.{ "a", "b" });
    const extra_unique = try std.mem.replaceOwned(u8, alloc, parent_json, "\"unique_constraints\":[", "\"unique_constraints\":[{\"name\":\"single\",\"columns\":[\"a\"]},");
    defer alloc.free(extra_unique);
    var shared_parent = parent;
    shared_parent.schema_json = extra_unique;
    var shared = try prepare(alloc, &.{shared_parent}, "children", child_json, "");
    defer shared.deinit();
    const narrower = try std.mem.replaceOwned(u8, alloc, child_json, "[\"a\",\"b\"]", "[\"a\"]");
    defer alloc.free(narrower);
    const remaining: records.TableRecord = .{ .table_id = 4, .name = "remaining", .schema_json = narrower };
    const trimmed = (try cleanup(alloc, &.{ shared.parents[0].after, remaining }, shared.parents[0].after)).?;
    defer @import("../metadata/table_manager.zig").freeTable(alloc, trimmed);
    var trimmed_schema = try schema.parseValidatedTableSchema(alloc, trimmed.schema_json);
    defer trimmed_schema.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), trimmed_schema.relational_indexes.?.value.len);
    const covers: []const []const u8 = trimmed_schema.relational_indexes.?.value[0].include_columns orelse &.{};
    try std.testing.expectEqual(@as(usize, 0), covers.len);
    try support.requireCoverage(trimmed_schema, &.{"a"});
}
