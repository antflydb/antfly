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

//! Bounded dependency closure for an unpublished named-type retirement.
//! This planner reads no rows and grants no publication authority. Its exact
//! postimages belong to one staged rewrite/type-retirement transaction.
const std = @import("std");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const max_tables = 128;
pub const max_bytes = 16 * 1024 * 1024;
const max_columns = 65535;
const max_nodes = 1_048_576;

pub const Input = struct { name: []const u8, schema_json: []const u8 };
pub const CascadeObject = struct {
    kind: enum { column, default, constraint, index },
    name: []const u8,
};
pub const Table = struct {
    name: []const u8,
    schema_json: []const u8,
    dropped_columns: []const []const u8,
    cascade_objects: []const CascadeObject,
    changed: bool,
};
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    tables: []const Table,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn init(backing: A, inputs: []const Input, type_id: u64) !Prepared {
        if (type_id == 0 or inputs.len == 0 or inputs.len > max_tables) return error.InvalidSqlNamedType;
        var region = std.heap.ArenaAllocator.init(backing);
        errdefer region.deinit();
        const a = region.allocator();
        var size: usize = 0;
        for (inputs) |input| {
            size +|= input.name.len +| input.schema_json.len;
            if (input.name.len == 0 or input.schema_json.len == 0 or size > max_bytes) return error.SqlProgramLimitExceeded;
        }
        const states = try a.alloc(State, inputs.len);
        var names: std.StringHashMapUnmanaged(usize) = .empty;
        for (inputs, states, 0..) |input, *state, ordinal| {
            if ((try names.getOrPut(a, input.name)).found_existing) return error.InvalidSqlBackendResponse;
            names.getPtr(input.name).?.* = ordinal;
            state.* = try State.init(a, input.schema_json, type_id);
        }
        const tables = try a.alloc(Table, inputs.len);
        size = 0;
        for (inputs, states, tables) |input, *state, *table| {
            try state.retire(a, states, names, type_id);
            const encoded = if (state.changed) try std.json.Stringify.valueAlloc(a, state.root, .{}) else try a.dupe(u8, input.schema_json);
            size +|= encoded.len;
            if (size > max_bytes) return error.SqlProgramLimitExceeded;
            table.* = .{ .name = try a.dupe(u8, input.name), .schema_json = encoded, .dropped_columns = try a.dupe([]const u8, state.queue.items), .changed = state.changed, .cascade_objects = state.cascade_objects.items };
        }
        return .{ .arena = region, .tables = tables };
    }
};

fn number(value: Json) !u64 {
    return switch (value) {
        .integer => |n| if (n >= 0) @intCast(n) else error.InvalidSqlBackendResponse,
        .number_string => |encoded| std.fmt.parseInt(u64, encoded, 10) catch error.InvalidSqlBackendResponse,
        else => error.InvalidSqlBackendResponse,
    };
}
fn text(value: Json) ![]const u8 {
    return if (value == .string) value.string else error.InvalidSqlBackendResponse;
}
const Edge = struct { source: usize, target: usize };
const State = struct {
    root: Json,
    columns: std.StringHashMapUnmanaged(usize) = .empty,
    removed: []bool = &.{},
    queue: std.ArrayList([]const u8) = .empty,
    changed: bool = false,
    cascade_objects: std.ArrayList(CascadeObject) = .empty,
    relational: bool = false,
    remaining: usize = max_nodes,

    fn checkpoint(self: *@This()) !void {
        if (self.remaining == 0) return error.SqlProgramLimitExceeded;
        self.remaining -= 1;
    }
    fn drop(self: *@This(), a: A, name: []const u8) !void {
        const ordinal = self.columns.get(name) orelse return error.InvalidSqlBackendResponse;
        if (self.removed[ordinal]) return;
        self.removed[ordinal] = true;
        try self.queue.append(a, name);
    }
    fn row(self: *@This()) !*Json {
        const kind = try text(self.root.object.get("default_type") orelse return error.InvalidSqlBackendResponse);
        const documents = self.root.object.getPtr("document_schemas") orelse return error.InvalidSqlBackendResponse;
        if (documents.* != .object) return error.InvalidSqlBackendResponse;
        const document = documents.object.getPtr(kind) orelse return error.InvalidSqlBackendResponse;
        if (document.* != .object) return error.InvalidSqlBackendResponse;
        const value = document.object.getPtr("schema") orelse return error.InvalidSqlBackendResponse;
        if (value.* != .object) return error.InvalidSqlBackendResponse;
        return value;
    }
    fn init(a: A, encoded: []const u8, type_id: u64) !State {
        var state: State = .{ .root = try std.json.parseFromSliceLeaky(Json, a, encoded, .{ .parse_numbers = false, .allocate = .alloc_always }) };
        if (state.root != .object) return error.InvalidSqlBackendResponse;
        const mode = state.root.object.get("storage_mode") orelse return state;
        if (mode != .string) return error.InvalidSqlBackendResponse;
        state.relational = std.mem.eql(u8, mode.string, "relational");
        if (!state.relational) return state;
        const fields = (try state.row()).object.get("properties") orelse return error.InvalidSqlBackendResponse;
        if (fields != .object or fields.object.count() > max_columns) return error.SqlProgramLimitExceeded;
        state.removed = try a.alloc(bool, fields.object.count());
        @memset(state.removed, false);
        for (fields.object.keys(), 0..) |name, i| try state.columns.put(a, name, i);
        for (fields.object.keys(), fields.object.values()) |name, property| {
            if (property != .object) return error.InvalidSqlBackendResponse;
            if (property.object.get("x-antfly-sql-type-id")) |id| if (try number(id) == type_id) try state.drop(a, name);
        }
        // Build the reverse dependency graph once. Queue propagation visits
        // every generated edge at most once, including reversed declaration
        // order and arbitrarily long chains under the column/node bounds.
        var edges: std.ArrayList(Edge) = .empty;
        if (state.root.object.get("generated_columns")) |generated| {
            if (generated != .array) return error.InvalidSqlBackendResponse;
            for (generated.array.items) |entry| {
                if (entry != .object) return error.InvalidSqlBackendResponse;
                const name = try text(entry.object.get("column") orelse return error.InvalidSqlBackendResponse);
                const target = state.columns.get(name) orelse return error.InvalidSqlBackendResponse;
                const program = entry.object.get("expression") orelse return error.InvalidSqlBackendResponse;
                if (try state.expression(a, program, type_id, target, &edges, 0)) try state.drop(a, name);
            }
        }
        const counts = try a.alloc(usize, state.removed.len);
        @memset(counts, 0);
        for (edges.items) |edge| counts[edge.source] += 1;
        const offsets = try a.alloc(usize, counts.len + 1);
        offsets[0] = 0;
        for (counts, 0..) |count, i| offsets[i + 1] = offsets[i] + count;
        const next = try a.dupe(usize, offsets[0..counts.len]);
        const targets = try a.alloc(usize, edges.items.len);
        for (edges.items) |edge| {
            targets[next[edge.source]] = edge.target;
            next[edge.source] += 1;
        }
        var head: usize = 0;
        while (head < state.queue.items.len) : (head += 1) {
            const source = state.columns.get(state.queue.items[head]).?;
            for (targets[offsets[source]..offsets[source + 1]]) |target| try state.drop(a, fields.object.keys()[target]);
        }
        return state;
    }

    // Only typed IR fields grant dependencies. JSON literal payloads, names,
    // descriptions and arbitrary strings never become column references.
    fn expression(self: *@This(), a: A, value: Json, type_id: u64, target: ?usize, edges: ?*std.ArrayList(Edge), depth: usize) anyerror!bool {
        try self.checkpoint();
        if (depth > 16 or value != .object) return error.InvalidSqlBackendResponse;
        const op = try text(value.object.get("op") orelse return error.InvalidSqlBackendResponse);
        var affected = if (value.object.get("named_type_id")) |id| try number(id) == type_id else false;
        if (std.mem.eql(u8, op, "literal")) return affected;
        if (std.mem.eql(u8, op, "column")) {
            const name = try text(value.object.get("column") orelse return error.InvalidSqlBackendResponse);
            const ordinal = self.columns.get(name) orelse return error.InvalidSqlBackendResponse;
            if (target) |output| if (edges) |graph| try graph.append(a, .{ .source = ordinal, .target = output });
            return affected or self.removed[ordinal];
        }
        if (value.object.get("args")) |args| {
            if (args != .array) return error.InvalidSqlBackendResponse;
            for (args.array.items) |arg| affected = try self.expression(a, arg, type_id, target, edges, depth + 1) or affected;
        }
        return affected;
    }
    fn namesAffected(self: *const @This(), value: Json) !bool {
        if (value != .array) return error.InvalidSqlBackendResponse;
        for (value.array.items) |entry| if (self.columns.get(try text(entry))) |ordinal| {
            if (self.removed[ordinal]) return true;
        };
        return false;
    }
    fn declarationAffected(self: *@This(), a: A, entry: Json, type_id: u64, depth: usize) anyerror!bool {
        try self.checkpoint();
        if (depth > 64 or entry != .object) return error.InvalidSqlBackendResponse;
        for ([_][]const u8{ "column", "without_overlaps_column" }) |key| if (entry.object.get(key)) |value| {
            if (self.columns.get(try text(value))) |ordinal| if (self.removed[ordinal]) return true;
        };
        for ([_][]const u8{ "columns", "include_columns" }) |key| if (entry.object.get(key)) |value| {
            if (try self.namesAffected(value)) return true;
        };
        if (entry.object.get("expression")) |expr| if (try self.expression(a, expr, type_id, null, null, 0)) return true;
        for ([_][]const u8{ "keys", "where" }) |key| if (entry.object.get(key)) |parts| {
            if (parts != .array) return error.InvalidSqlBackendResponse;
            for (parts.array.items) |part| if (try self.declarationAffected(a, part, type_id, depth + 1)) return true;
        };
        return false;
    }
    fn collectExpressionTypes(self: *@This(), a: A, value: Json, ids: *std.AutoHashMapUnmanaged(u64, void), depth: usize) anyerror!void {
        try self.checkpoint();
        if (depth > 16 or value != .object) return error.InvalidSqlBackendResponse;
        const op = try text(value.object.get("op") orelse return error.InvalidSqlBackendResponse);
        if (value.object.get("named_type_id")) |id| try ids.put(a, try number(id), {});
        if (std.mem.eql(u8, op, "literal")) return;
        if (value.object.get("args")) |args| {
            if (args != .array) return error.InvalidSqlBackendResponse;
            for (args.array.items) |arg| try self.collectExpressionTypes(a, arg, ids, depth + 1);
        }
    }
    fn collectDeclarationTypes(self: *@This(), a: A, entry: Json, ids: *std.AutoHashMapUnmanaged(u64, void), depth: usize) anyerror!void {
        try self.checkpoint();
        if (depth > 64 or entry != .object) return error.InvalidSqlBackendResponse;
        if (entry.object.get("expression")) |program| try self.collectExpressionTypes(a, program, ids, 0);
        for ([_][]const u8{ "keys", "where" }) |key| if (entry.object.get(key)) |parts| {
            if (parts != .array) return error.InvalidSqlBackendResponse;
            for (parts.array.items) |part| try self.collectDeclarationTypes(a, part, ids, depth + 1);
        };
    }
    fn retire(self: *@This(), a: A, states: []State, tables: std.StringHashMapUnmanaged(usize), type_id: u64) !void {
        if (!self.relational) return;
        for (self.queue.items) |name| try self.cascade_objects.append(a, .{ .kind = .column, .name = name });
        var index_names: std.StringHashMapUnmanaged(void) = .empty;
        for ([_][]const u8{ "generated_columns", "column_defaults", "checks", "unique_constraints", "relational_indexes" }) |key| {
            const entries = self.root.object.getPtr(key) orelse continue;
            if (entries.* != .array) return error.InvalidSqlBackendResponse;
            var write: usize = 0;
            for (entries.array.items) |entry| {
                if (try self.declarationAffected(a, entry, type_id, 0)) {
                    // Dependencies owned by a removed column disappear with
                    // that column. Independent expressions are separate
                    // PostgreSQL CASCADE objects; index-backed UNIQUE occurs
                    // in two executable directories but is one SQL object.
                    if (!std.mem.eql(u8, key, "generated_columns") and !try self.declarationAffected(a, entry, 0, 0)) {
                        const is_default = std.mem.eql(u8, key, "column_defaults");
                        const is_index = std.mem.eql(u8, key, "relational_indexes");
                        const name = try text(entry.object.get(if (is_default) "column" else "name") orelse return error.InvalidSqlBackendResponse);
                        const indexed = is_index or std.mem.eql(u8, key, "unique_constraints");
                        const duplicate = if (indexed) (try index_names.getOrPut(a, name)).found_existing else false;
                        if (!duplicate) try self.cascade_objects.append(a, .{ .kind = if (is_default) .default else if (is_index) .index else .constraint, .name = name });
                    }
                    self.changed = true;
                    continue;
                }
                entries.array.items[write] = entry;
                write += 1;
            }
            entries.array.items.len = write;
        }
        if (self.root.object.getPtr("foreign_keys")) |entries| {
            if (entries.* != .array) return error.InvalidSqlBackendResponse;
            var write: usize = 0;
            for (entries.array.items) |entry| {
                if (entry != .object) return error.InvalidSqlBackendResponse;
                const child = entry.object.get("child_columns") orelse return error.InvalidSqlBackendResponse;
                const parent_name = try text(entry.object.get("parent_table") orelse return error.InvalidSqlBackendResponse);
                const own_column = try self.namesAffected(child);
                var affected = own_column;
                if (tables.get(parent_name)) |parent| affected = try states[parent].namesAffected(entry.object.get("parent_columns") orelse return error.InvalidSqlBackendResponse) or affected;
                if (affected) {
                    if (!own_column) try self.cascade_objects.append(a, .{ .kind = .constraint, .name = try text(entry.object.get("name") orelse return error.InvalidSqlBackendResponse) });
                    self.changed = true;
                    continue;
                }
                entries.array.items[write] = entry;
                write += 1;
            }
            entries.array.items.len = write;
        }
        const document = try self.row();
        const fields = document.object.getPtr("properties") orelse return error.InvalidSqlBackendResponse;
        for (self.queue.items) |name| {
            if (std.mem.eql(u8, name, "_id")) return error.UnsupportedSqlShape;
            _ = fields.object.orderedRemove(name);
            self.changed = true;
        }
        if (document.object.getPtr("required")) |required| {
            if (required.* != .array) return error.InvalidSqlBackendResponse;
            var write: usize = 0;
            for (required.array.items) |entry| {
                const ordinal = self.columns.get(try text(entry)) orelse return error.InvalidSqlBackendResponse;
                if (self.removed[ordinal]) continue;
                required.array.items[write] = entry;
                write += 1;
            }
            required.array.items.len = write;
        }
        var live_types: std.AutoHashMapUnmanaged(u64, void) = .empty;
        for (fields.object.values()) |property| {
            if (property != .object) return error.InvalidSqlBackendResponse;
            if (property.object.get("x-antfly-sql-type-id")) |id| try live_types.put(a, try number(id), {});
        }
        // A retained CHECK/default/index may reference an unrelated named
        // type without any surviving column of that type. Its executable
        // registry must live as long as that expression, too. Literal JSON
        // payloads remain opaque throughout this walk.
        for ([_][]const u8{ "generated_columns", "column_defaults", "checks", "unique_constraints", "relational_indexes" }) |key| {
            if (self.root.object.get(key)) |entries| {
                if (entries != .array) return error.InvalidSqlBackendResponse;
                for (entries.array.items) |entry| try self.collectDeclarationTypes(a, entry, &live_types, 0);
            }
        }
        if (self.root.object.getPtr("named_types")) |directory| {
            if (directory.* != .array) return error.InvalidSqlBackendResponse;
            var write: usize = 0;
            for (directory.array.items) |entry| {
                if (entry != .object) return error.InvalidSqlBackendResponse;
                const identity = entry.object.get("identity") orelse return error.InvalidSqlBackendResponse;
                if (identity != .object) return error.InvalidSqlBackendResponse;
                const retained_id = try number(identity.object.get("id") orelse return error.InvalidSqlBackendResponse);
                if (retained_id == type_id or !live_types.contains(retained_id)) {
                    self.changed = true;
                    continue;
                }
                directory.array.items[write] = entry;
                write += 1;
            }
            directory.array.items.len = write;
        }
        if (self.changed) {
            const version = if (self.root.object.get("version")) |value| try number(value) else 0;
            if (version >= std.math.maxInt(u32)) return error.SqlLimitExceeded;
            try self.root.object.put(a, "version", .{ .integer = @intCast(version + 1) });
        }
    }
};

const retirement_parent =
    \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"required":["id","state","caption","caption2"],"properties":{"id":{"type":"integer"},"state":{"type":"sql_enum","x-antfly-sql-type-id":17},"caption":{"type":"keyword"},"caption2":{"type":"keyword"},"payload":{"type":"json","nullable":true},"spare":{"type":"sql_enum","x-antfly-sql-type-id":19,"nullable":true}}}}},"generated_columns":[{"column":"caption2","expression":{"op":"concat","args":[{"op":"column","column":"caption"},{"op":"literal","type":"string","value":" state "}]}},{"column":"caption","expression":{"op":"cast","type":"string","args":[{"op":"column","column":"state"}]}}],"column_defaults":[{"column":"state","expression":{"op":"literal","type":"enumeration","named_type_id":17,"value":"queued"}},{"column":"payload","expression":{"op":"literal","type":"json","value":{"op":"column","column":"state","named_type_id":17,"number":12345678901234567890.1234567890123456789}}}],"checks":[{"name":"state_check","expression":{"op":"eq","args":[{"op":"column","column":"state"},{"op":"literal","type":"enumeration","named_type_id":17,"value":"queued"}]}},{"name":"opaque_check","expression":{"op":"is_not_null","args":[{"op":"literal","type":"json","value":{"op":"column","column":"state","named_type_id":17}}]}}],"unique_constraints":[{"name":"pk","columns":["id"]},{"name":"state_key","columns":["state"]},{"name":"caption_key","columns":["caption"]}],"relational_indexes":[{"name":"by_state","keys":[{"column":"state"}]},{"name":"by_id","keys":[{"column":"id"}],"include_columns":["payload"]}]}
;
const retirement_child =
    \\{"version":0,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"properties":{"id":{"type":"integer"},"link":{"type":"keyword","nullable":true}}}}},"foreign_keys":[{"name":"derived_parent","child_columns":["link"],"parent_table":"parent","parent_columns":["caption"]}],"unique_constraints":[{"name":"child_pk","columns":["id"]}]}
;
const retirement_document =
    \\{"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"payload":{"type":"json","const":{"x-antfly-sql-type-id":17,"column":"state"}}}}}}}
;

fn retirementClosure(backing: A) !void {
    const native = @import("../schema/mod.zig");
    const types = @import("../system_catalog/named_types.zig");
    const layouts = @import("../common/sql_named_type_layout.zig");
    var source_region = std.heap.ArenaAllocator.init(backing);
    defer source_region.deinit();
    const a = source_region.allocator();
    var labels = try types.Owned.create(a, &.{ "queued", "done" });
    defer labels.deinit();
    const records = [_]types.Record{
        .{ .identity = try types.Identity.init(17, 2, 1, @splat(8)), .definition = labels.definition, .owner = .auth_disabled },
        .{ .identity = try types.Identity.init(19, 2, 1, @splat(9)), .definition = labels.definition, .owner = .auth_disabled },
    };
    var parent = try std.json.parseFromSliceLeaky(Json, a, retirement_parent, .{ .parse_numbers = false });
    try layouts.canonicalizeSchemaLeaky(a, &parent, &records);
    const before = try std.json.Stringify.valueAlloc(a, parent, .{});
    const inputs = [_]Input{ .{ .name = "parent", .schema_json = before }, .{ .name = "child", .schema_json = retirement_child }, .{ .name = "document", .schema_json = retirement_document } };
    var prepared = try Prepared.init(backing, &inputs, 17);
    defer prepared.deinit();
    const result = prepared.tables;
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expect(result[0].changed and result[1].changed and !result[2].changed);
    try std.testing.expectEqual(@as(usize, 3), result[0].cascade_objects.len);
    try std.testing.expectEqual(@as(usize, 1), result[1].cascade_objects.len);
    try std.testing.expectEqualDeep(CascadeObject{ .kind = .constraint, .name = "derived_parent" }, result[1].cascade_objects[0]);
    try std.testing.expectEqual(@as(usize, 0), result[2].cascade_objects.len);
    var after = try std.json.parseFromSlice(Json, backing, result[0].schema_json, .{ .parse_numbers = false });
    defer after.deinit();
    const fields = try @import("schema_columns.zig").properties(after.value);
    for ([_][]const u8{ "state", "caption", "caption2" }) |name| try std.testing.expect(!fields.object.contains(name));
    for ([_][]const u8{ "id", "payload", "spare" }) |name| try std.testing.expect(fields.object.contains(name));
    try std.testing.expectEqual(@as(usize, 3), result[0].dropped_columns.len);
    try std.testing.expectEqual(@as(u64, 5), try number(after.value.object.get("version").?));
    try std.testing.expectEqual(@as(usize, 0), after.value.object.get("generated_columns").?.array.items.len);
    for ([_][]const u8{ "checks", "column_defaults", "unique_constraints", "relational_indexes", "named_types" }) |key| try std.testing.expectEqual(@as(usize, 1), after.value.object.get(key).?.array.items.len);
    const retained_default = after.value.object.get("column_defaults").?.array.items[0].object.get("expression").?.object.get("value").?;
    try std.testing.expectEqualStrings("12345678901234567890.1234567890123456789", retained_default.object.get("number").?.number_string);
    try std.testing.expectEqualStrings("state", retained_default.object.get("column").?.string);
    try std.testing.expectEqual(@as(u64, 19), try number(after.value.object.get("named_types").?.array.items[0].object.get("identity").?.object.get("id").?));
    var child = try std.json.parseFromSlice(Json, backing, result[1].schema_json, .{});
    defer child.deinit();
    try std.testing.expectEqual(@as(usize, 0), child.value.object.get("foreign_keys").?.array.items.len);
    try std.testing.expect((try @import("schema_columns.zig").properties(child.value)).object.contains("link"));
    try std.testing.expectEqualStrings(retirement_document, result[2].schema_json);
    // Both predecessor and successor are real executable native schemas.
    // The planner itself never compiles schemas under a mutation lock.
    var predecessor = try native.PreparedSchemaDefinition.init(backing, before);
    defer predecessor.deinit(backing);
    var successor = try native.PreparedSchemaDefinition.init(backing, result[0].schema_json);
    defer successor.deinit(backing);
    try std.testing.expectEqual(@as(usize, 1), successor.runtime_schema.named_types.len);
}

test "SQL named type retirement closes generated and incoming FK dependencies without matching opaque JSON" {
    try retirementClosure(std.testing.allocator);
}

test "SQL named type retirement owns bounded candidates and unwinds every allocation failure" {
    const Run = struct {
        fn run(a: A) !void {
            var prepared = try Prepared.init(a, &.{ .{ .name = "parent", .schema_json = retirement_parent }, .{ .name = "child", .schema_json = retirement_child } }, 17);
            defer prepared.deinit();
        }
    };
    try Run.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
    try std.testing.expectError(error.InvalidSqlNamedType, Prepared.init(std.testing.allocator, &.{}, 17));
    try std.testing.expectError(error.InvalidSqlBackendResponse, Prepared.init(std.testing.allocator, &.{ .{ .name = "duplicate", .schema_json = retirement_child }, .{ .name = "duplicate", .schema_json = retirement_child } }, 17));
}
