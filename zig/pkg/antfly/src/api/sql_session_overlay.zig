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

//! Bounded merge of an immutable statement read view and staged primary rows.
//! Owns at most one native page; staged state is borrowed from the serialized
//! statement. Native rows shadowed by any staged write/delete never leak out.
const std = @import("std");
const catalog = @import("antfly_local_sources").sql_catalog;
const transactions = @import("transactions.zig");
const Json = std.json.Value;

/// Read dependencies are not postimages. Only writes to this physical table
/// require a primary-order merge, but every matching entry still fences the
/// schema cut even when it contains predicates alone. Inspect the complete
/// cut before returning so later stale entries cannot be hidden by a write.
pub fn needsMerge(staged: *const transactions.OwnedTransactionCommitRequest, table: catalog.Table) !bool {
    var writes = false;
    for (staged.tables) |entry| {
        if (!std.mem.eql(u8, staged.physicalName(entry.table_name), table.physical_name)) continue;
        if (entry.schema_version != null and entry.schema_version != table.schema_version) return error.CatalogGenerationChanged;
        if (entry.relational_schema_version != null and entry.relational_schema_version != table.schema_version) return error.CatalogGenerationChanged;
        writes = writes or entry.batch.writes.len != 0 or entry.batch.deletes.len != 0 or entry.txn_writes.len != 0;
    }
    return writes;
}

pub fn open(alloc: std.mem.Allocator, native: catalog.Cursor, staged: *const transactions.OwnedTransactionCommitRequest, table: catalog.Table, request: catalog.Scan, row_filter_json: ?[]const u8) !catalog.Cursor {
    const cursor = try alloc.create(Cursor);
    cursor.* = .{ .alloc = alloc, .native = native, .arena = std.heap.ArenaAllocator.init(alloc), .page_arena = std.heap.ArenaAllocator.init(alloc) };
    errdefer {
        cursor.arena.deinit();
        cursor.page_arena.deinit();
        alloc.destroy(cursor);
    }
    const arena = cursor.arena.allocator();
    const projection_type = @import("antfly_local_sources").sql_document_row.Projection;
    // A staged full postimage is parsed once, but only scan-demanded fields
    // need typed preparation. Scalar-only reads must not decode every untouched
    // array in a wide table. Conditions add their own inputs to this directory.
    var names: std.ArrayList([]const u8) = .empty;
    try names.appendSlice(arena, request.fields);
    for (request.conditions) |condition| try names.append(arena, condition.column);
    const full_projection = try projection_type.init(arena, table, names.items);
    const full_layout = try full_projection.pageLayout(arena);
    const selected_projection = try projection_type.init(arena, table, request.fields);
    const selected_layout = try selected_projection.pageLayout(arena);
    var row_filter = if (row_filter_json) |filter| try @import("antfly_local_sources").search_pattern_filter.PreparedPatternFilter.init(alloc, filter) else null;
    defer if (row_filter) |*filter| filter.deinit();
    var numeric_context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = arena };
    const conditions = try prepareConditions(arena, table, request.conditions, &numeric_context);
    var rows: std.ArrayList(catalog.Row) = .empty;
    for (staged.tables) |entry| {
        if (!std.mem.eql(u8, staged.physicalName(entry.table_name), table.physical_name)) continue;
        if (entry.schema_version != null and entry.schema_version != table.schema_version) return error.CatalogGenerationChanged;
        if (entry.relational_schema_version != null and entry.relational_schema_version != table.schema_version) return error.CatalogGenerationChanged;
        for (entry.batch.deletes) |key| try cursor.shadowed.put(arena, key, {});
        for (entry.batch.writes) |write| {
            try cursor.shadowed.put(arena, write.key, {});
            if (request.primary_key) |key| if (!std.mem.eql(u8, key, write.key)) continue;
            if (request.after) |after| if (std.mem.order(u8, write.key, after) != .gt) continue;
            const value = try std.json.parseFromSliceLeaky(Json, arena, write.value, .{ .parse_numbers = false });
            if (value != .object) return error.InvalidSqlBackendResponse;
            if (row_filter) |*filter| if (!try filter.matchesJson(arena, write.key, value)) continue;
            var object: std.json.ObjectMap = .empty;
            var nulls: std.ArrayList(bool) = .empty;
            for (full_projection.columns) |column| {
                if (!value.object.contains(column.path)) continue;
                const raw = value.object.get(column.path) orelse .null;
                const json_null = if (table.storage_mode == .document) column.type == .json and value.object.contains(column.path) else for (write.json_null_fields) |field| {
                    if (std.mem.eql(u8, field, column.path)) break true;
                } else false;
                try nulls.append(arena, raw == .null and !json_null);
                const typed = if (raw == .null or column.type == .array or column.element_type == .numeric) raw else try @import("antfly_local_sources").sql_describe.coerce(raw, column.type);
                try object.put(arena, column.path, typed);
            }
            const observed = for (entry.predicates.items) |predicate| {
                if (std.mem.eql(u8, predicate.key, write.key)) break predicate;
            } else return error.InvalidSqlBackendResponse;
            const row = try full_projection.adaptBorrowed(arena, full_layout, catalog.Row{ .id = write.key, .version = observed.expected_version, .value = .{ .object = object }, .sql_nulls = nulls.items, .expected_content_digest = observed.expected_content_digest, .document = if (request.include_document) value else null });
            if (try matches(row, conditions, &numeric_context)) {
                if (selected_projection.has_typed_cells) {
                    try rows.append(arena, try selected_projection.adaptBorrowed(arena, selected_layout, row));
                    continue;
                }
                var projected: std.json.ObjectMap = .empty;
                var projected_nulls: std.ArrayList(bool) = .empty;
                for (selected_projection.columns) |column| {
                    if (!try row.hasField(column.path)) continue;
                    const cell = try row.cell(column.path);
                    try projected.put(arena, column.path, cell.value);
                    try projected_nulls.append(arena, cell.sql_null);
                }
                try rows.append(arena, .{ .id = row.id, .version = row.version, .value = .{ .object = projected }, .sql_nulls = projected_nulls.items, .expected_content_digest = row.expected_content_digest, .document = row.document });
            }
        }
    }
    std.mem.sort(catalog.Row, rows.items, {}, struct {
        fn less(_: void, left: catalog.Row, right: catalog.Row) bool {
            return std.mem.lessThan(u8, left.id, right.id);
        }
    }.less);
    cursor.staged = rows.items;
    return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
}

const Numeric = @import("antfly_local_sources").sql_numeric_value;
const PreparedCondition = struct { condition: catalog.Condition, numeric: ?Numeric.Value = null };

/// Bind exact operands once, before staged row traversal. The cursor arena
/// owns coefficients; the same context meters preparation and every probe.
fn prepareConditions(a: std.mem.Allocator, table: catalog.Table, conditions: []const catalog.Condition, context: *Numeric.Context) ![]const PreparedCondition {
    const prepared = try a.alloc(PreparedCondition, conditions.len);
    for (conditions, prepared) |condition, *item| {
        item.* = .{ .condition = condition };
        const column = try table.column(condition.column);
        if (condition.op == .is_null or condition.op == .is_not_null or condition.value == .null or column.element_type != .numeric or column.type != .number) continue;
        const operand = @import("antfly_local_sources").sql_numeric_storage.fromJson(context, condition.value) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.SqlTypeMismatch,
            else => return err,
        };
        item.numeric = operand.value;
    }
    return prepared;
}

fn matches(row: catalog.Row, conditions: []const PreparedCondition, numeric_context: *Numeric.Context) !bool {
    for (conditions) |item| {
        const condition = item.condition;
        const cell = try row.cell(condition.column);
        if (condition.op == .is_null) {
            if (!cell.sql_null) return false;
            continue;
        }
        if (condition.op == .is_not_null) {
            if (cell.sql_null) return false;
            continue;
        }
        if (cell.sql_null or condition.value == .null) return false;
        if (cell.array != null) return error.UnsupportedSqlShape;
        const order = if (cell.numeric) |number| blk: {
            break :blk try Numeric.order(numeric_context, number.*, item.numeric orelse return error.InvalidSqlBackendResponse);
        } else try @import("antfly_local_sources").sql_scalar.compare(cell.value, condition.value);
        if (!switch (condition.op) {
            .eq => order == .eq,
            .neq => order != .eq,
            .lt => order == .lt,
            .lte => order != .gt,
            .gt => order == .gt,
            .gte => order != .lt,
            else => unreachable,
        }) return false;
    }
    return true;
}

const Cursor = struct {
    alloc: std.mem.Allocator,
    native: catalog.Cursor,
    arena: std.heap.ArenaAllocator,
    page_arena: std.heap.ArenaAllocator,
    shadowed: std.StringHashMapUnmanaged(void) = .empty,
    staged: []const catalog.Row = &.{},
    staged_index: usize = 0,
    page: ?catalog.Page = null,
    page_index: usize = 0,
    exhausted: bool = false,

    fn peek(self: *Cursor) !?catalog.Row {
        while (true) {
            if (self.page) |page| {
                while (self.page_index < page.rows.len) {
                    const row = page.rows[self.page_index];
                    if (!self.shadowed.contains(row.id)) return row;
                    self.page_index += 1;
                }
                self.exhausted = page.after == null;
                page.deinit();
                self.page = null;
                _ = self.page_arena.reset(.retain_capacity);
            }
            if (self.exhausted) return null;
            self.page = try self.native.next(self.native.ptr, self.page_arena.allocator(), 4096);
            self.page_index = 0;
        }
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        var rows: std.ArrayList(catalog.Row) = .empty;
        var layout: ?catalog.Row.TypedLayout = null;
        while (rows.items.len < limit) {
            const native = try self.peek();
            const staged: ?catalog.Row = if (self.staged_index < self.staged.len) self.staged[self.staged_index] else null;
            if (native == null and staged == null) break;
            const take_staged = staged != null and (native == null or std.mem.lessThan(u8, staged.?.id, native.?.id));
            const row = if (take_staged) staged.? else native.?;
            if (row.typed_cells) |cells| if (layout == null) {
                layout = try cells.layout.clone(alloc);
            };
            try rows.append(alloc, try row.cloneWithLayout(alloc, layout));
            if (take_staged) self.staged_index += 1 else self.page_index += 1;
        }
        const more = self.staged_index < self.staged.len or (try self.peek()) != null;
        return .{ .rows = rows.items, .after = if (more and rows.items.len != 0) rows.items[rows.items.len - 1].id else null };
    }

    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.page) |page| page.deinit();
        self.native.close(self.native.ptr);
        self.page_arena.deinit();
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

test "SQL session overlay merge admission uses physical identity and validates every schema fence" {
    const a = std.testing.allocator;
    var staged: transactions.OwnedTransactionCommitRequest = .{};
    defer staged.deinit(a);
    try staged.bind(a, "logical", "physical");
    const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 9, .columns = &.{} };
    var entries = [_]transactions.TableCommitRequest{
        .{ .table_name = @constCast("logical"), .schema_version = 9, .relational_schema_version = 9 },
        .{ .table_name = @constCast("other"), .schema_version = 1, .batch = .{ .deletes = @constCast(&[_][]const u8{"a"}) } },
    };
    staged.tables = &entries;
    // Entries borrow stack memory; only the separately owned bindings are freed.
    defer staged.tables = &.{};
    try std.testing.expect(!try needsMerge(&staged, table));
    entries[0].batch.deletes = entries[1].batch.deletes;
    try std.testing.expect(try needsMerge(&staged, table));
    entries[1].table_name = @constCast("physical");
    try std.testing.expectError(error.CatalogGenerationChanged, needsMerge(&staged, table));
    entries[1].schema_version = 9;
    entries[1].relational_schema_version = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, needsMerge(&staged, table));
    entries[1].relational_schema_version = 9;
    try std.testing.expect(try needsMerge(&staged, table));
    entries[0].batch.deletes = &.{};
    entries[1].batch.deletes = &.{};
    try std.testing.expect(!try needsMerge(&staged, table));
}

test "SQL session overlay owns typed array rows after native pages and cursor close" {
    const alloc = std.testing.allocator;
    var output = std.heap.ArenaAllocator.init(alloc);
    defer output.deinit();
    const Test = struct {
        delivered: bool = false,
        fn next(ptr: *anyopaque, a: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.delivered) return .{ .rows = &.{} };
            self.delivered = true;
            const arrays = @import("antfly_local_sources").sql_array_value;
            const scalar = @import("antfly_local_sources").sql_scalar;
            const dimensions = [_]arrays.Dimension{.{ .length = 1, .lower = -2 }};
            const elements = [_]scalar.Datum{scalar.Datum.json(.{ .string = "retained" })};
            const array = try arrays.Value.init(.text, &dimensions, &elements, .{});
            const layout = try catalog.Row.TypedLayout.init(a, &.{ "a", "missing" });
            const rows = try a.alloc(catalog.Row, 1);
            rows[0] = try catalog.Row.fromDatums(a, "key", layout, &.{ scalar.Datum.typedArray(&array), .{} });
            rows[0].typed_cells.?.presence = &.{ true, false };
            return .{ .rows = rows };
        }
        fn close(_: *anyopaque) void {}
    };
    var native: Test = .{};
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{}}");
    defer staged.deinit(alloc);
    const cursor = try open(alloc, .{ .ptr = &native, .next = Test.next, .close = Test.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .text },
        .{ .name = "missing", .path = "missing", .type = .string },
    } }, .{ .fields = &.{ "a", "missing" }, .limit = 1 }, null);
    const page = cursor.next(cursor.ptr, output.allocator(), 1) catch |err| {
        cursor.close(cursor.ptr);
        return err;
    };
    cursor.close(cursor.ptr);
    try std.testing.expect(!try page.rows[0].hasField("missing"));
    const array = (try page.rows[0].cell("a")).array.?;
    try std.testing.expectEqual(@as(i32, -2), array.dimensions[0].lower);
    try std.testing.expectEqualStrings("retained", array.elements[0].value.string);
}

test "SQL session overlay prepares staged arrays once and preserves omitted fields" {
    const a = std.testing.allocator;
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const Native = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    var native: u8 = 0;
    var staged = try transactions.parseCommitRequest(a,
        \\{"read_set":[{"table":"t","key":"key","version":"0"}],"tables":{"t":{"inserts":{"key":{"a":{"dimensions":[{"length":2,"lower_bound":-2}],"values":["9223372036854775807",null],"sql_nulls":[false,true]},"j":{"dimensions":[{"length":2,"lower_bound":1}],"values":[null,null],"sql_nulls":[false,true]}}}}}}
    );
    defer staged.deinit(a);
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "j", .path = "j", .type = .array, .element_type = .jsonb },
        .{ .name = "missing", .path = "missing", .type = .string },
    } };
    const cursor = try open(a, .{ .ptr = &native, .next = Native.next, .close = Native.close }, &staged, table, .{ .fields = &.{ "a", "j", "missing" }, .limit = 1 }, null);
    const page = cursor.next(cursor.ptr, output.allocator(), 1) catch |err| {
        cursor.close(cursor.ptr);
        return err;
    };
    cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expect(!try page.rows[0].hasField("missing"));
    const array = (try page.rows[0].cell("a")).array.?;
    try std.testing.expectEqual(@as(i32, -2), array.dimensions[0].lower);
    try std.testing.expectEqual(std.math.maxInt(i64), array.elements[0].value.integer);
    try std.testing.expect(array.elements[1].sql_null);
    const json = (try page.rows[0].cell("j")).array.?;
    try std.testing.expect(json.elements[0].value == .null);
    try std.testing.expect(!json.elements[0].sql_null);
    try std.testing.expect(json.elements[1].sql_null);
    const scalar = try open(a, .{ .ptr = &native, .next = Native.next, .close = Native.close }, &staged, table, .{ .fields = &.{"missing"}, .limit = 1 }, null);
    defer scalar.close(scalar.ptr);
    const narrow = try scalar.next(scalar.ptr, output.allocator(), 1);
    try std.testing.expectEqual(@as(usize, 1), narrow.rows.len);
    try std.testing.expect(narrow.rows[0].typed_cells == null);
    try std.testing.expect(!try narrow.rows[0].hasField("missing"));
    try std.testing.expect((try narrow.rows[0].cell("missing")).sql_null);
}

test "SQL session overlay NUMERIC staged rows retain exact values after cursor close without arrays" {
    const a = std.testing.allocator;
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const Native = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    var native: u8 = 0;
    var staged = try transactions.parseCommitRequest(a,
        \\{"read_set":[{"table":"t","key":"key","version":"0"}],"tables":{"t":{"inserts":{"key":{"n":"9007199254740993.1200","j":null,"absent":null}}}}}
    );
    defer staged.deinit(a);
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
        .{ .name = "j", .path = "j", .type = .json },
        .{ .name = "absent", .path = "absent", .type = .number, .element_type = .numeric },
        .{ .name = "missing", .path = "missing", .type = .number, .element_type = .numeric },
    } };
    const cursor = try open(a, .{ .ptr = &native, .next = Native.next, .close = Native.close }, &staged, table, .{
        .fields = &.{ "n", "absent", "missing" },
        .limit = 1,
        .conditions = &.{.{ .column = "n", .op = .eq, .value = .{ .number_string = "9007199254740993.12" } }},
    }, null);
    const page = cursor.next(cursor.ptr, output.allocator(), 1) catch |err| {
        cursor.close(cursor.ptr);
        return err;
    };
    cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    const row = page.rows[0];
    try std.testing.expect(try row.hasField("absent"));
    try std.testing.expect(!try row.hasField("missing"));
    try std.testing.expect((try row.cell("absent")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    const number = (try row.cell("n")).numeric.?;
    try std.testing.expectEqual(@as(u16, 4), number.scale);
    var ctx: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = output.allocator() };
    const text = try @import("antfly_local_sources").sql_numeric_value.format(&ctx, number.*);
    try std.testing.expectEqualStrings("9007199254740993.1200", text);
}

test "SQL session overlay NUMERIC conditions prepare once and share bounded allocation-free probe work" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
    } };
    var ctx: Numeric.Context = .{ .alloc = a };
    const conditions = try prepareConditions(a, table, &.{
        .{ .column = "n", .op = .gte, .value = .{ .number_string = "9007199254740993.12" } },
        .{ .column = "n", .op = .lt, .value = .{ .string = "9007199254740993.13" } },
    }, &ctx);
    var number = try Numeric.parse(&ctx, "9007199254740993.1200");
    defer number.deinit();
    const scalar = @import("antfly_local_sources").sql_scalar;
    const layout = try catalog.Row.TypedLayout.init(a, &.{"n"});
    const row = try catalog.Row.fromDatums(a, "key", layout, &.{scalar.Datum.typedNumeric(&number.value)});
    const start = ctx.remaining;
    var none = std.heap.FixedBufferAllocator.init(&.{});
    ctx.alloc = none.allocator();
    for (0..1000) |_| try std.testing.expect(try matches(row, conditions, &ctx));
    try std.testing.expect(ctx.remaining < start);
    ctx.remaining = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, matches(row, conditions, &ctx));
    ctx = .{ .alloc = a };
    try std.testing.expectError(error.SqlTypeMismatch, prepareConditions(a, table, &.{.{ .column = "n", .op = .eq, .value = .{ .float = 1.2 } }}, &ctx));
}

test "SQL session overlay merges pages and suppresses replaced and deleted rows" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const Test = struct {
        calls: usize = 0,
        closed: bool = false,
        fn next(ptr: *anyopaque, page_alloc: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls > 1) return .{ .rows = &.{} };
            const rows = try page_alloc.alloc(catalog.Row, 2);
            rows[0] = .{ .id = "a", .version = 1, .value = try std.json.parseFromSliceLeaky(Json, page_alloc, "{\"n\":1}", .{}) };
            rows[1] = .{ .id = "c", .version = 3, .value = try std.json.parseFromSliceLeaky(Json, page_alloc, "{\"n\":3}", .{}) };
            return .{ .rows = rows };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };
    var test_native: Test = .{};
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"1\"},{\"table\":\"t\",\"key\":\"b\",\"version\":\"0\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"n\":11},\"b\":{\"n\":2}},\"deletes\":[\"c\"]}}}");
    defer staged.deinit(alloc);
    const cursor = try open(alloc, .{ .ptr = &test_native, .next = Test.next, .close = Test.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} }, .{ .fields = &.{"n"}, .limit = 1 }, null);
    const first = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expectEqualStrings("a", first.rows[0].id);
    try std.testing.expect(first.after != null);
    const second = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expectEqualStrings("b", second.rows[0].id);
    try std.testing.expect(second.after == null);
    cursor.close(cursor.ptr);
    try std.testing.expect(test_native.closed);
    try std.testing.expectEqual(@as(usize, 1), test_native.calls);
    test_native = .{};
    const filtered = try open(alloc, .{ .ptr = &test_native, .next = Test.next, .close = Test.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} }, .{ .fields = &.{"n"}, .limit = 1 }, "{\"doc_id\":{\"ids\":[\"b\"]}}");
    defer filtered.close(filtered.ptr);
    const visible = try filtered.next(filtered.ptr, arena.allocator(), 1);
    try std.testing.expectEqual(@as(usize, 1), visible.rows.len);
    try std.testing.expectEqualStrings("b", visible.rows[0].id);
    try std.testing.expect(visible.after == null);
}

test "SQL document session overlay retains full postimage and original conflict digest" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const Empty = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    var state: u8 = 0;
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"7\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"j\":null,\"extra\":9007199254740993}}}}}");
    defer staged.deinit(alloc);
    staged.tables[0].schema_version = 2;
    staged.tables[0].predicates.items[0].expected_content_digest = @splat(42);
    const table = catalog.Table{ .id = 1, .physical_name = "t", .schema_version = 2, .storage_mode = .document, .columns = &.{ .{ .name = "j", .path = "j", .type = .json }, .{ .name = "missing", .path = "missing", .type = .json } } };
    const cursor = try open(alloc, .{ .ptr = &state, .next = Empty.next, .close = Empty.close }, &staged, table, .{ .fields = &.{ "j", "missing" }, .include_document = true, .limit = 1 }, null);
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expect(!(try page.rows[0].cell("j")).sql_null);
    try std.testing.expect((try page.rows[0].cell("missing")).sql_null);
    try std.testing.expectEqual(@as(u64, 7), page.rows[0].version);
    try std.testing.expectEqual(@as([32]u8, @splat(42)), page.rows[0].expected_content_digest.?);
    try std.testing.expectEqualStrings("9007199254740993", page.rows[0].document.?.object.get("extra").?.number_string);
    var wrong_epoch = table;
    wrong_epoch.schema_version = 3;
    try std.testing.expectError(error.CatalogGenerationChanged, open(alloc, .{ .ptr = &state, .next = Empty.next, .close = Empty.close }, &staged, wrong_epoch, .{ .fields = &.{}, .limit = 1 }, null));
}

test "SQL session overlay preserves JSON null independently of SQL NULL" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const Empty = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    var state: u8 = 0;
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"0\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"j\":null,\"n\":null}}}}}");
    defer staged.deinit(alloc);
    const fields = try alloc.alloc([]const u8, 1);
    fields[0] = try alloc.dupe(u8, "j");
    staged.tables[0].batch.writes[0].json_null_fields = fields;
    const cursor = try open(alloc, .{ .ptr = &state, .next = Empty.next, .close = Empty.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{ .{ .name = "j", .path = "j", .type = .json }, .{ .name = "n", .path = "n", .type = .json } } }, .{ .fields = &.{ "j", "n" }, .limit = 1 }, null);
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expect(!(try page.rows[0].cell("j")).sql_null);
    try std.testing.expect((try page.rows[0].cell("n")).sql_null);
}
