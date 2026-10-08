// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Immutable schema derivations only. Authorization and current catalog identity
//! are resolved for every request. Content addressing naturally invalidates on
//! schema changes, including table-name reuse. No cached authorization exists.
const std = @import("std");
const catalog = @import("antfly_local_sources").sql_catalog;
const schema = @import("antfly_local_sources").schema_mod;
const native_schema = @import("antfly_local_sources").storage_schema;
const Budget = @import("antfly_local_sources").sql_memory_budget;

const Entry = struct {
    digest: [32]u8,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    version: u32 = 0,
    storage_mode: @FieldType(catalog.Table, "storage_mode") = .relational,
    external_base_source: ?@import("antfly_local_sources").serverless_external_source_schema_binding.OwnedExternalTableBinding = null,
    columns: []const catalog.Column = &.{},
    indexes: []const catalog.Index = &.{},
    constraints: []const catalog.Constraint = &.{},
    refs: usize = 0,
    used: u64 = 0,
};
const Slot = struct { entry: ?*Entry = null, building: bool = false };
pub const Cache = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    slots: [32]Slot = @splat(.{}),
    clock: u64 = 0,
    builds: usize = 0,
    // Every slot, including in-flight builds, reserves this bound. Maximum
    // backing allocation is 32 MiB plus entry metadata, regardless of churn.
    const entry_budget = 1 << 20;

    pub fn init(allocator: std.mem.Allocator) Cache {
        return .{ .allocator = allocator };
    }
    // Owner drains requests before destruction.
    pub fn deinit(self: *Cache) void {
        std.debug.assert(self.builds == 0);
        for (self.slots) |slot| if (slot.entry) |entry| {
            std.debug.assert(entry.refs == 0);
            self.destroy(entry);
        };
        self.* = undefined;
    }
    fn destroy(self: *Cache, entry: *Entry) void {
        entry.arena.deinit();
        std.debug.assert(entry.budget.live == 0);
        self.allocator.destroy(entry);
    }
    pub fn resolve(self: *Cache, io: std.Io, alloc: std.mem.Allocator, json: []const u8, id: u64, physical_name: []const u8) !catalog.Table {
        if (json.len > entry_budget) return error.SqlLimitExceeded;
        const entry = try self.acquire(io, json);
        defer {
            self.mutex.lockUncancelable(io);
            entry.refs -= 1;
            self.mutex.unlock(io);
        }
        // Copy only compact immutable columns to the request arena. Schema JSON
        // parsing/validation and runtime schema derivation happen only on miss.
        const columns = try alloc.alloc(catalog.Column, entry.columns.len);
        for (entry.columns, columns) |column, *out| {
            out.* = column;
            out.name = try alloc.dupe(u8, column.name);
            out.path = try alloc.dupe(u8, column.path);
        }
        const indexes = try alloc.alloc(catalog.Index, entry.indexes.len);
        for (entry.indexes, indexes) |index, *out| {
            const names = try alloc.alloc([]const u8, index.columns.len);
            for (index.columns, names) |name, *copy| copy.* = try alloc.dupe(u8, name);
            out.* = .{ .name = try alloc.dupe(u8, index.name), .columns = names };
        }
        const constraints = try alloc.alloc(catalog.Constraint, entry.constraints.len);
        for (entry.constraints, constraints) |constraint, *out| {
            out.* = constraint;
            out.name = try alloc.dupe(u8, constraint.name);
        }
        return .{ .external_base_source = if (entry.external_base_source) |source| try @import("antfly_local_sources").serverless_external_source_schema_binding.cloneAlloc(alloc, source) else null, .id = id, .physical_name = physical_name, .schema_version = entry.version, .storage_mode = entry.storage_mode, .columns = columns, .indexes = indexes, .constraints = constraints };
    }
    fn acquire(self: *Cache, io: std.Io, json: []const u8) !*Entry {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(json, &digest, .{});
        try self.mutex.lock(io);
        var locked = true;
        defer if (locked) self.mutex.unlock(io);
        self.clock +%= 1;
        var candidate: ?usize = null;
        for (&self.slots, 0..) |*slot, index| {
            if (slot.building) continue;
            if (slot.entry) |entry| {
                if (std.mem.eql(u8, &digest, &entry.digest)) {
                    entry.refs += 1;
                    entry.used = self.clock;
                    return entry;
                }
                if (entry.refs == 0 and (candidate == null or (self.slots[candidate.?].entry != null and entry.used < self.slots[candidate.?].entry.?.used))) candidate = index;
            } else candidate = index;
        }
        if (self.builds >= 4) return error.SqlPlanCacheBusy;
        const index = candidate orelse return error.SqlPlanCacheBusy;
        const victim = self.slots[index].entry;
        self.slots[index] = .{ .building = true };
        self.builds += 1;
        self.mutex.unlock(io);
        locked = false;
        errdefer {
            self.mutex.lockUncancelable(io);
            self.slots[index] = .{};
            self.builds -= 1;
            self.mutex.unlock(io);
        }
        if (victim) |old| self.destroy(old);
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .digest = digest, .budget = .{ .backing = self.allocator, .limit = entry_budget }, .arena = undefined };
        entry.arena = std.heap.ArenaAllocator.init(entry.budget.allocator());
        errdefer entry.arena.deinit();
        derive(entry, json) catch |err| {
            if (err == error.OutOfMemory and entry.budget.exhausted) return error.SqlLimitExceeded;
            return err;
        };
        try io.checkCancel();
        self.mutex.lockUncancelable(io);
        locked = true;
        // Concurrent requests may derive the same content. Publish one entry;
        // destroy the loser before releasing its reserved slot.
        for (self.slots) |slot| if (slot.entry) |winner| {
            if (std.mem.eql(u8, &digest, &winner.digest)) {
                winner.refs += 1;
                winner.used = self.clock;
                self.mutex.unlock(io);
                locked = false;
                self.destroy(entry);
                self.mutex.lockUncancelable(io);
                self.slots[index] = .{};
                self.builds -= 1;
                self.mutex.unlock(io);
                return winner;
            }
        };
        entry.refs = 1;
        entry.used = self.clock;
        self.slots[index] = .{ .entry = entry };
        self.builds -= 1;
        return entry;
    }
};

fn derive(entry: *Entry, json: []const u8) !void {
    var scratch = std.heap.ArenaAllocator.init(entry.budget.allocator());
    defer scratch.deinit();
    const alloc = scratch.allocator();
    const owned = entry.arena.allocator();
    const parsed = try schema.parseValidatedTableSchema(alloc, json);
    entry.constraints = try catalog.Constraint.derive(owned, parsed);
    if (parsed.storage_mode == .document) {
        entry.storage_mode = .document;
        entry.version = parsed.version;
        entry.columns = try @import("antfly_local_sources").sql_document_row.deriveColumns(owned, parsed);
        return;
    }
    const native = try schema.deriveRuntimeTableSchema(alloc, parsed);
    defer native_schema.freeSchema(alloc, native);
    if (native.storage_mode != .relational) return error.UnsupportedSqlExecution;
    var generated = std.StringHashMap(void).init(alloc);
    if (parsed.generated_columns) |items| {
        if (items.value != .array) return error.InvalidSqlBackendResponse;
        for (items.value.array.items) |item| {
            if (item != .object) return error.InvalidSqlBackendResponse;
            const name = item.object.get("column") orelse return error.InvalidSqlBackendResponse;
            if (name != .string) return error.InvalidSqlBackendResponse;
            try generated.put(name.string, {});
        }
    }
    const columns = try owned.alloc(catalog.Column, native.relational_columns.len);
    for (native.relational_columns, columns) |column, *out| out.* = .{
        .name = try owned.dupe(u8, column.name),
        .path = try owned.dupe(u8, column.path),
        .nullable = !column.required or column.allows_null,
        .generated = generated.contains(column.name),
        .element_type = column.sql_element_type,
        .type = @import("antfly_local_sources").sql_document_row.relationalType(parsed, column.name, switch (column.column_type) {
            .string => .string,
            .integer => .integer,
            .number => .number,
            .boolean => .boolean,
            .datetime => .datetime,
            .json => .json,
            .sql_array => .array,
            else => return error.UnsupportedSqlExecution,
        }),
    };
    entry.columns = columns;
    if (native.external_base_source) |source| {
        entry.external_base_source = try @import("antfly_local_sources").serverless_external_source_schema_binding.cloneAlloc(owned, source);
        entry.version = native.version;
        return;
    }
    var indexes: std.ArrayList(catalog.Index) = .empty;
    if (parsed.relational_indexes) |declarations| for (declarations.value) |index| {
        if (index.where != null or index.keys.len == 0 or index.keys.len > 32) continue;
        const names = try owned.alloc([]const u8, index.keys.len);
        var direct = true;
        for (index.keys, names) |key, *name| {
            const column = key.column orelse {
                direct = false;
                break;
            };
            name.* = try owned.dupe(u8, column);
        }
        if (!direct) continue;
        try indexes.append(owned, .{ .name = try owned.dupe(u8, index.name), .columns = names });
    };
    entry.indexes = try indexes.toOwnedSlice(owned);
    entry.version = native.version;
}

test "SQL schema cache reuses immutable layouts without caching table identity" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const first = try cache.resolve(std.testing.io, arena.allocator(), json, 1, "one");
    const second = try cache.resolve(std.testing.io, arena.allocator(), json, 2, "two");
    try std.testing.expectEqual(@as(u64, 2), second.id);
    try std.testing.expectEqualStrings("two", second.physical_name);
    try std.testing.expectEqual(first.columns[0].type, second.columns[0].type);
    var entries: usize = 0;
    for (cache.slots) |slot| if (slot.entry != null) {
        entries += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), entries);
    const changed =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    const newer = try cache.resolve(std.testing.io, arena.allocator(), changed, 1, "one");
    try std.testing.expectEqual(@as(u32, 2), newer.schema_version);
    try std.testing.expectEqual(@as(u32, 1), first.schema_version);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.string, newer.columns[0].type);
    const padding: [40]u8 = @splat(' ');
    for (1..40) |count| {
        const variant = try std.fmt.allocPrint(arena.allocator(), "{s}{s}", .{ json, padding[0..count] });
        _ = try cache.resolve(std.testing.io, arena.allocator(), variant, 4, "four");
    }
    // Copies remain valid after the corresponding immutable entry is evicted.
    try std.testing.expectEqualStrings("id", first.columns[0].name);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.integer, first.columns[0].type);
    const schemaless = try cache.resolve(std.testing.io, arena.allocator(), "{}", 3, "three");
    try std.testing.expectEqual(.document, schemaless.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), schemaless.columns.len);
    try std.testing.expectEqualStrings("_id", (try schemaless.column("_id")).name);
    try std.testing.expectEqual(@as(usize, 0), cache.builds);
}

test "SQL schema cache pins direct total index candidates with the layout" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"label_idx","keys":[{"column":"label"}]},{"name":"label_tenant_idx","keys":[{"column":"label"},{"column":"tenant"}]},{"name":"partial_idx","keys":[{"column":"label"}],"where":[{"column":"label","op":"eq","value":"ready"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"},"tenant":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const table = try cache.resolve(std.testing.io, arena.allocator(), json, 1, "physical");
    try std.testing.expectEqual(@as(usize, 2), table.indexes.len);
    try std.testing.expectEqualStrings("label_idx", table.indexes[0].name);
    try std.testing.expectEqualStrings("label", table.indexes[0].columns[0]);
    try std.testing.expectEqualStrings("label_tenant_idx", table.indexes[1].name);
    try std.testing.expectEqualStrings("label", table.indexes[1].columns[0]);
    try std.testing.expectEqualStrings("tenant", table.indexes[1].columns[1]);
}

test "SQL schema cache owns named constraint kinds across eviction and allocation faults" {
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"selected_key","columns":["id"]},{"name":"later_key","columns":["id"],"deferrable":true}],"relational_indexes":[{"name":"access_only","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const Faults = struct {
        fn run(a: std.mem.Allocator, bytes: []const u8) !void {
            var cache = Cache.init(a);
            defer cache.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const table = try cache.resolve(std.testing.io, arena.allocator(), bytes, 1, "rows");
            try std.testing.expectEqual(@as(usize, 2), table.constraints.len);
            try std.testing.expectEqualStrings("selected_key", table.constraints[0].name);
            try std.testing.expectEqual(.unique, table.constraints[0].kind);
            try std.testing.expect(!table.constraints[0].deferrable);
            try std.testing.expect(table.constraints[1].deferrable);
            // Release every cache-owned schema; request descriptors must own
            // their names, not borrow the temporary parser or cached arena.
            for (&cache.slots) |*slot| if (slot.entry) |entry| {
                cache.destroy(entry);
                slot.* = .{};
            };
            try std.testing.expectEqualStrings("selected_key", table.constraints[0].name);
            try std.testing.expectEqualStrings("later_key", table.constraints[1].name);
        }
    };
    try Faults.run(std.testing.allocator, json);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{json});
}

test "SQL schema cache document shapes are declared stable unions" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const table = try cache.resolve(std.testing.io, arena.allocator(),
        \\{"version":3,"storage_mode":"document","document_schemas":{"a":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"mixed":{"type":"integer"},"only_a":{"type":"string"},"nested":{"type":"object"}},"required":["id","mixed"]}},"b":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"mixed":{"type":"string"}},"required":["id","mixed"]}}}}
    , 7, "documents");
    try std.testing.expectEqual(.document, table.storage_mode);
    try std.testing.expectEqual(@as(u32, 3), table.schema_version);
    try std.testing.expectEqual(@as(usize, 4), table.columns.len);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.integer, (try table.column("id")).type);
    try std.testing.expect(!(try table.column("id")).nullable);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.json, (try table.column("mixed")).type);
    try std.testing.expect((try table.column("only_a")).nullable);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.json, (try table.column("nested")).type);
    try std.testing.expectError(error.UndefinedColumn, table.column("sampled_from_a_row"));
}

test "SQL schema cache preserves logical UUID over physical keywords" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc);
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const table = try cache.resolve(std.testing.io, arena.allocator(),
        \\{"version":0,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"u":{"type":"keyword","format":"uuid"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    , 7, "items");
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.uuid, (try table.column("u")).type);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_ast.ColumnType.string, (try table.column("label")).type);
}

test "SQL schema cache preserves every declared scalar builtin width through DDL" {
    const alloc = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    var cache = Cache.init(alloc);
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var create = try sources.sql_compiler.compile(a, "CREATE TABLE widths (t text, s smallint, i integer, b bigint, r real, d double precision, flag boolean, u uuid, j jsonb)", .{});
    defer create.deinit();
    const schema_json = try sources.sql_ddl_runtime.createSchemaAlloc(a, create.statement.create_table);
    const table = try cache.resolve(std.testing.io, a, schema_json, 7, "widths");
    const expected = [_]struct { name: []const u8, kind: sources.sql_array_value.ElementType, oid: u32 }{
        .{ .name = "t", .kind = .text, .oid = 25 },
        .{ .name = "s", .kind = .int16, .oid = 21 },
        .{ .name = "i", .kind = .int32, .oid = 23 },
        .{ .name = "b", .kind = .int64, .oid = 20 },
        .{ .name = "r", .kind = .float32, .oid = 700 },
        .{ .name = "d", .kind = .float64, .oid = 701 },
        .{ .name = "flag", .kind = .boolean, .oid = 16 },
        .{ .name = "u", .kind = .uuid, .oid = 2950 },
        .{ .name = "j", .kind = .jsonb, .oid = 3802 },
    };
    for (expected) |entry| {
        const column = try table.column(entry.name);
        try std.testing.expectEqual(entry.kind, column.element_type.?);
        try std.testing.expectEqual(entry.oid, column.element_type.?.oid());
    }
}
