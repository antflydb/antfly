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

const Layout = struct {
    version: u32 = 0,
    storage_mode: @FieldType(catalog.Table, "storage_mode") = .relational,
    version_field: ?[]const u8 = null,
    external_base_source: ?@import("antfly_local_sources").serverless_external_source_schema_binding.OwnedExternalTableBinding = null,
    columns: []const catalog.Column = &.{},
    indexes: []const catalog.Index = &.{},
    index_definitions: []const catalog.IndexDefinition = &.{},
    constraints: []const catalog.Constraint = &.{},

    fn table(self: Layout, id: u64, physical_name: []const u8) catalog.Table {
        return .{ .id = id, .physical_name = physical_name, .schema_version = self.version, .storage_mode = self.storage_mode, .version_field = self.version_field, .external_base_source = self.external_base_source, .columns = self.columns, .indexes = self.indexes, .index_definitions = self.index_definitions, .constraints = self.constraints };
    }
};
const Entry = struct {
    digest: [32]u8,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    layout: Layout = .{},
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
        return self.resolveWithEntryLimit(io, alloc, json, id, physical_name, entry_budget);
    }
    fn resolveWithEntryLimit(self: *Cache, io: std.Io, alloc: std.mem.Allocator, json: []const u8, id: u64, physical_name: []const u8, requested_limit: usize) !catalog.Table {
        const limit = @min(entry_budget, requested_limit);
        const cached = if (json.len <= limit) self.acquire(io, alloc, json, limit) catch |err| switch (err) {
            error.SchemaCacheEntryTooLarge => null,
            else => return err,
        } else null;
        const entry = cached orelse {
            // Cache admission is optional. The statement allocator remains the
            // authority for layouts that exceed the compact retention budget.
            try io.checkCancel();
            var layout: Layout = .{};
            try derive(&layout, json, alloc, alloc);
            try io.checkCancel();
            return layout.table(id, physical_name);
        };
        defer {
            self.mutex.lockUncancelable(io);
            entry.refs -= 1;
            self.mutex.unlock(io);
        }
        // Copy only compact immutable columns to the request arena. Schema JSON
        // parsing/validation and runtime schema derivation happen only on miss.
        const columns = try alloc.alloc(catalog.Column, entry.layout.columns.len);
        for (entry.layout.columns, columns) |column, *out| {
            out.* = column;
            out.name = try alloc.dupe(u8, column.name);
            out.path = try alloc.dupe(u8, column.path);
        }
        const indexes = try alloc.alloc(catalog.Index, entry.layout.indexes.len);
        for (entry.layout.indexes, indexes) |index, *out| {
            const names = try alloc.alloc([]const u8, index.columns.len);
            for (index.columns, names) |name, *copy| copy.* = try alloc.dupe(u8, name);
            out.* = .{ .name = try alloc.dupe(u8, index.name), .columns = names };
        }
        const constraints = try alloc.alloc(catalog.Constraint, entry.layout.constraints.len);
        const definitions = try alloc.alloc(catalog.IndexDefinition, entry.layout.index_definitions.len);
        for (entry.layout.index_definitions, definitions) |definition, *out| out.* = try definition.cloneLeaky(alloc);
        for (entry.layout.constraints, constraints) |constraint, *out| {
            out.* = constraint;
            out.name = try alloc.dupe(u8, constraint.name);
        }
        return .{ .external_base_source = if (entry.layout.external_base_source) |source| try @import("antfly_local_sources").serverless_external_source_schema_binding.cloneAlloc(alloc, source) else null, .id = id, .physical_name = physical_name, .schema_version = entry.layout.version, .storage_mode = entry.layout.storage_mode, .version_field = if (entry.layout.version_field) |field| try alloc.dupe(u8, field) else null, .columns = columns, .indexes = indexes, .index_definitions = definitions, .constraints = constraints };
    }
    fn acquire(self: *Cache, io: std.Io, scratch: std.mem.Allocator, json: []const u8, limit: usize) !*Entry {
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
        entry.* = .{ .digest = digest, .budget = .{ .backing = self.allocator, .limit = limit }, .arena = undefined };
        entry.arena = std.heap.ArenaAllocator.init(entry.budget.allocator());
        errdefer entry.arena.deinit();
        derive(&entry.layout, json, scratch, entry.arena.allocator()) catch |err| {
            if (err == error.OutOfMemory and entry.budget.exhausted) return error.SchemaCacheEntryTooLarge;
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

test "SQL schema cache owns custom version fields through cache hits and owner release" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var cache = Cache.init(a);
    var live = true;
    defer if (live) cache.deinit();
    const json = "{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"doc\",\"ttl\":{\"duration\":\"1s\",\"field\":\"expires\"},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{},\"additionalProperties\":true}}}}";
    var tables: [2]catalog.Table = undefined;
    for (&tables) |*table| table.* = try cache.resolve(std.testing.io, arena.allocator(), json, 7, "docs");
    for (cache.slots) |slot| if (slot.entry) |entry| for (tables) |table| {
        try std.testing.expect(table.version_field.?.ptr != entry.layout.version_field.?.ptr);
    };
    cache.deinit();
    live = false;
    var root: std.json.ObjectMap = .empty;
    try root.put(arena.allocator(), "expires", .{ .integer = 123 });
    for (tables) |table| {
        try std.testing.expectEqualStrings("expires", table.version_field.?);
        try std.testing.expectEqual(@as(u64, 123), try table.preparedVersion(.{ .object = root }, 456));
        try std.testing.expectEqual(@as(u64, 456), try table.preparedVersion(.{ .object = .empty }, 456));
    }
}

fn derive(layout: *Layout, json: []const u8, temporary: std.mem.Allocator, owned: std.mem.Allocator) !void {
    // Parsing and runtime schema construction are statement work, not retained
    // cache data. Never let their geometric arena growth consume a cache slot.
    var scratch = std.heap.ArenaAllocator.init(temporary);
    defer scratch.deinit();
    const alloc = scratch.allocator();
    const parsed = try schema.parseValidatedTableSchema(alloc, json);
    layout.version_field = if (parsed.ttl_duration_ns != 0) try owned.dupe(u8, parsed.ttl_field) else null;
    layout.constraints = try catalog.Constraint.derive(owned, parsed);
    if (parsed.storage_mode == .document) {
        layout.storage_mode = .document;
        layout.version = parsed.version;
        layout.columns = try @import("antfly_local_sources").sql_document_row.deriveColumns(owned, parsed);
        return;
    }
    const native = try schema.deriveRuntimeTableSchema(alloc, parsed);
    defer native_schema.freeSchema(alloc, native);
    layout.columns = try @import("antfly_local_sources").sql_document_row.deriveRelationalColumnsLeaky(owned, alloc, parsed, native);
    if (native.external_base_source) |source| {
        layout.external_base_source = try @import("antfly_local_sources").serverless_external_source_schema_binding.cloneAlloc(owned, source);
        layout.version = native.version;
        return;
    }
    var indexes: std.ArrayList(catalog.Index) = .empty;
    layout.index_definitions = try catalog.IndexDefinition.deriveLeaky(owned, alloc, parsed, native);
    if (parsed.relational_indexes) |declarations| for (declarations.value) |index| {
        // SQL DDL emits an empty predicate list for total indexes. Presence
        // alone is not partiality; only a nonempty predicate needs implication.
        if ((if (index.where) |where| where.len != 0 else false) or index.keys.len == 0 or index.keys.len > 32) continue;
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
    layout.indexes = try indexes.toOwnedSlice(owned);
    layout.version = native.version;
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

test "SQL schema cache retains SQL-lowered total indexes without admitting partial or expression access" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var candidate_schema = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"}},"additionalProperties":false}}}}
    , .{});
    for ([_][]const u8{
        "CREATE INDEX direct_label ON items(label)",
        "CREATE INDEX partial_label ON items(label) WHERE label='ready'",
        "CREATE INDEX expression_label ON items(lower(label))",
    }, 0..) |statement, ordinal| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(a, statement, .{});
        defer compiled.deinit();
        try std.testing.expect(try @import("antfly_local_sources").sql_schema_ddl.apply(a, &candidate_schema, compiled.statement.catalog_ddl));
        const bytes = try std.json.Stringify.valueAlloc(a, candidate_schema, .{});
        for ([_]bool{ false, true }) |uncached| {
            const table = if (uncached)
                try cache.resolveWithEntryLimit(std.testing.io, a, bytes, 1, "physical", 1)
            else
                try cache.resolve(std.testing.io, a, bytes, 1, "physical");
            try std.testing.expectEqual(@as(usize, 1), table.indexes.len);
            try std.testing.expectEqualStrings("direct_label", table.indexes[0].name);
            try std.testing.expectEqualStrings("label", table.indexes[0].columns[0]);
            try std.testing.expectEqual(ordinal + 1, table.index_definitions.len);
            if (ordinal >= 1) {
                const partial = table.index_definitions[1];
                try std.testing.expectEqualStrings("partial_label", partial.name);
                try std.testing.expectEqualStrings("label", partial.keys[0].column);
                try std.testing.expectEqualStrings("label", partial.where[0].column);
                try std.testing.expectEqual(.eq, partial.where[0].op);
                try std.testing.expectEqualStrings("ready", partial.where[0].value.?.string);
            }
            if (ordinal >= 2) {
                const expression = table.index_definitions[2];
                try std.testing.expectEqualStrings("expression_label", expression.name);
                try std.testing.expectEqualStrings("lower_ascii", expression.keys[0].expression.?.object.get("op").?.string);
                try std.testing.expectEqualStrings("label", expression.keys[0].expression.?.object.get("args").?.array.items[0].object.get("column").?.string);
            }
        }
    }
}

test "SQL schema cache owns complete index shapes across eviction fallback and allocation faults" {
    const json =
        \\{"version":7,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"mixed","keys":[{"column":"tenant","direction":"desc","nulls":"first"},{"expression":{"op":"lower_ascii","args":[{"op":"column","column":"label"}]},"result_type":"string","collation":"binary","direction":"asc","nulls":"last"}],"include_columns":["label"],"where":[{"column":"tenant","op":"gte","value":9007199254740993},{"column":"label","op":"is_not_null"}]},{"name":"empty","keys":[{"column":"label"}],"where":[{"constant":false}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"label":{"type":"keyword"},"tenant":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const Faults = struct {
        fn check(a: std.mem.Allocator, table: catalog.Table) !void {
            try std.testing.expectEqual(@as(u32, 7), table.schema_version);
            // Neither declaration is a direct-total probe, even when all
            // physical keys happen to be direct columns (constant FALSE).
            try std.testing.expectEqual(@as(usize, 0), table.indexes.len);
            try std.testing.expectEqual(@as(usize, 2), table.index_definitions.len);
            const mixed = table.index_definitions[0];
            try std.testing.expectEqualStrings("mixed", mixed.name);
            try std.testing.expectEqualStrings("tenant", mixed.keys[0].column);
            try std.testing.expectEqual(.desc, mixed.keys[0].direction);
            try std.testing.expectEqual(.first, mixed.keys[0].nulls);
            try std.testing.expect(mixed.keys[0].expression == null);
            const expression = mixed.keys[1];
            try std.testing.expectEqualStrings("", expression.column);
            try std.testing.expectEqual(.string, expression.result_type.?);
            try std.testing.expectEqualStrings("binary", expression.collation.?);
            try std.testing.expectEqual(.asc, expression.direction);
            try std.testing.expectEqual(.last, expression.nulls);
            const tree = expression.expression.?;
            try std.testing.expectEqualStrings("lower_ascii", tree.object.get("op").?.string);
            try std.testing.expectEqualStrings("label", tree.object.get("args").?.array.items[0].object.get("column").?.string);
            try std.testing.expectEqualDeep(expression.identity.?, try expression.bindIdentity(a, try table.expressionSchemaLeaky(a)));
            try std.testing.expectEqualStrings("label", mixed.include_columns[0]);
            try std.testing.expectEqualStrings("tenant", mixed.where[0].column);
            try std.testing.expectEqual(.gte, mixed.where[0].op);
            try std.testing.expectEqualStrings("9007199254740993", mixed.where[0].value.?.number_string);
            try std.testing.expect(mixed.where[1].value == null);
            try std.testing.expectEqual(false, table.index_definitions[1].where[0].constant.?);
        }
        fn run(a: std.mem.Allocator, bytes: []const u8) !void {
            var cache = Cache.init(a);
            defer cache.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const first = try cache.resolve(std.testing.io, alloc, bytes, 1, "rows");
            const hit = try cache.resolve(std.testing.io, alloc, bytes, 2, "other_rows");
            const fallback = try cache.resolveWithEntryLimit(std.testing.io, alloc, bytes, 3, "fallback", 1);
            // Destroy source buffers too: both descriptor paths must own
            // strings, exact operands, nested expression objects and arrays.
            for (&cache.slots) |*slot| if (slot.entry) |entry| {
                cache.destroy(entry);
                slot.* = .{};
            };
            try check(alloc, first);
            try check(alloc, hit);
            try check(alloc, fallback);
            try std.testing.expectEqual(@as(u64, 2), hit.id);
            try std.testing.expectEqualStrings("other_rows", hit.physical_name);
        }
    };
    try Faults.run(std.testing.allocator, json);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{json});
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

test "SQL schema cache excludes index owned uniqueness from named constraint identity" {
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row",
        \\"unique_constraints":[{"name":"named_key","columns":["id"]},{"name":"access_key","columns":["id"],"origin":"index"}],
        \\"relational_indexes":[{"name":"access_key","keys":[{"column":"id"}],"description":"operator edited"}],
        \\"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const Faults = struct {
        fn run(a: std.mem.Allocator, bytes: []const u8) !void {
            var cache = Cache.init(a);
            defer cache.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const table = try cache.resolve(std.testing.io, arena.allocator(), bytes, 1, "rows");
            try std.testing.expectEqual(@as(usize, 1), table.constraints.len);
            try std.testing.expectEqualStrings("named_key", table.constraints[0].name);
            try std.testing.expectEqual(@as(usize, 1), table.indexes.len);
            try std.testing.expectEqualStrings("access_key", table.indexes[0].name);
            // Catalog clones must keep both namespaces independent of parser
            // retirement and cache eviction.
            for (&cache.slots) |*slot| if (slot.entry) |entry| {
                cache.destroy(entry);
                slot.* = .{};
            };
            try std.testing.expectEqualStrings("named_key", table.constraints[0].name);
            try std.testing.expectEqualStrings("access_key", table.indexes[0].name);
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

test "external lake SQL schema cache declines oversized retained layouts without limiting statements" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var properties: std.json.ObjectMap = .empty;
    const property = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"type\":\"integer\"}", .{});
    for (0..64) |i| try properties.put(alloc, try std.fmt.allocPrint(alloc, "v{d}", .{i}), property);
    const json = try std.json.Stringify.valueAlloc(alloc, .{ .storage_mode = "relational", .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = std.json.Value{ .object = properties }, .additionalProperties = false } } } }, .{});
    try std.testing.expect(json.len < 2048);
    for ([_]usize{ 128, 2048 }) |limit| {
        var cache = Cache.init(a);
        defer cache.deinit();
        for (0..2) |_| {
            const table = try cache.resolveWithEntryLimit(std.testing.io, alloc, json, 7, "wide", limit);
            try std.testing.expectEqual(@as(usize, 64), table.columns.len);
            try std.testing.expectEqual(@as(u64, 7), table.id);
            try std.testing.expectEqual(@as(usize, 0), cache.builds);
            for (cache.slots) |slot| try std.testing.expect(slot.entry == null and !slot.building);
        }
    }
}

test "external lake SQL schema cache separates wide source derivation scratch from retained data" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var properties: std.json.ObjectMap = .empty;
    const property = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"type\":\"integer\"}", .{});
    try properties.put(alloc, "k", property);
    for (0..128) |i| try properties.put(alloc, try std.fmt.allocPrint(alloc, "v{d}", .{i}), property);
    const path: [1024]u8 = @splat('a');
    for ([_]usize{ 128, 1024 }) |length| {
        var cache = Cache.init(a);
        defer cache.deinit();
        const uri = try std.fmt.allocPrint(alloc, "file:///lake/{s}", .{path[0..length]});
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .version = 0,
            .storage_mode = "relational",
            .base_source = .{ .kind = "external", .table_id = "wide-join", .format = "parquet", .uri = uri, .schema_fingerprint = "parquet-schema:a7513e16cfbb0a57e754f680c8a5694dc29b127c69ff3d0dd3e6990b174291d8" },
            .default_type = "row",
            .enforce_types = true,
            .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = std.json.Value{ .object = properties }, .required = [_][]const u8{}, .additionalProperties = false } } },
        }, .{});
        const table = try cache.resolve(std.testing.io, alloc, json, 1, "wide");
        try std.testing.expectEqual(@as(usize, 129), table.columns.len);
        try std.testing.expectEqualStrings(uri, table.external_base_source.?.source_uri);
        var entries: usize = 0;
        for (cache.slots) |slot| if (slot.entry) |entry| {
            entries += 1;
            try std.testing.expect(entry.budget.peak < 128 * 1024);
        };
        try std.testing.expectEqual(@as(usize, 1), entries);
    }
}
