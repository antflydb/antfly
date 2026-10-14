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

const std = @import("std");
const ast = @import("antfly_local_sources").sql_ast;
const catalog = @import("antfly_local_sources").sql_catalog;
const compiler = @import("antfly_local_sources").sql_compiler;
const runtime = @import("antfly_local_sources").sql_runtime;
const describe = @import("antfly_local_sources").sql_describe;
const relation_binding = @import("antfly_local_sources").sql_relation_binding;

const Backend = struct {
    const Cursor = struct {
        owner: *Backend,
        request: catalog.StatementScan,
        offset: usize = 0,
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const key = self.request.request.primary_key orelse if (self.request.request.index_equality) |equality| equality.values[0].string else null;
            const saturated = self.request.request.index_equality != null and self.owner.merge_path == .saturated;
            const available: usize = if (self.owner.empty_target and self.request.table.id == 1) 0 else if (saturated) 17 else if (key != null) 1 else self.owner.row_count;
            if (self.offset == available) return .{ .rows = &.{} };
            const count = @min(limit, available - self.offset);
            const rows = try alloc.alloc(catalog.Row, count);
            const local = @import("antfly_local_sources");
            const target = self.request.table.id == 1;
            const array_value: std.json.Value = if (self.owner.arrays) blk: {
                const decoded = try local.sql_array_text.decodeLeaky(alloc, if (target) .int64 else .int16, if (target) "[-1:1]={9007199254740993,NULL,2}" else "[3:4]={3,NULL}", .{});
                break :blk try local.sql_array_wire.toJsonLeaky(alloc, decoded.value, .{});
            } else .null;
            const json_value: std.json.Value = if (self.owner.arrays) blk: {
                const decoded = try local.sql_array_text.decodeLeaky(alloc, .jsonb, "{\"null\",NULL}", .{});
                break :blk try local.sql_array_wire.toJsonLeaky(alloc, decoded.value, .{});
            } else .null;
            const projection = if (self.owner.arrays) try local.sql_document_row.Projection.init(alloc, self.request.table, self.request.request.fields) else null;
            defer if (projection) |p| p.deinit(alloc);
            const layout = if (projection) |p| try p.pageLayout(alloc) else null;
            for (rows, self.offset..) |*row, ordinal| {
                const i: usize = if (key) |identity| if (std.mem.eql(u8, identity, "b")) 1 else 0 else ordinal;
                const id = if (target or !self.owner.duplicates) (if (i == 0) "a" else if (i == 1) "b" else try std.fmt.allocPrint(alloc, "row{d}", .{i})) else "a";
                var values: std.json.ObjectMap = .empty;
                const fields = self.request.request.fields;
                for (fields) |field| {
                    if (self.owner.arrays and std.mem.eql(u8, field, "missing")) continue;
                    if (self.owner.arrays and (std.mem.eql(u8, field, "a") or std.mem.eql(u8, field, "j"))) {
                        try values.put(alloc, field, if (std.mem.eql(u8, field, "a")) array_value else json_value);
                        continue;
                    }
                    const value: std.json.Value = if (std.mem.eql(u8, field, "n")) .{ .integer = @intCast(i + 1) } else if (std.mem.eql(u8, field, "delta")) .{ .integer = @intCast((i + 1) * 10) } else if (std.mem.eql(u8, field, "id")) .{ .string = id } else if (std.mem.eql(u8, field, "cold")) .{ .string = "old" } else .null;
                    try values.put(alloc, field, value);
                }
                const flags = try alloc.alloc(bool, values.count());
                @memset(flags, false); // payload JSON null is not SQL NULL.
                row.* = .{ .id = id, .version = std.math.maxInt(u64) - 1, .expected_content_digest = if (self.request.request.include_primary_digest) @splat(9) else null, .value = .{ .object = values }, .sql_nulls = flags };
                if (self.request.request.include_document) row.document = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"n\":1,\"payload\":null,\"cold\":\"old\",\"undeclared\":42}", .{});
                if (projection) |p| {
                    if (row.document) |*document| {
                        try document.object.put(alloc, "a", array_value);
                        try document.object.put(alloc, "j", json_value);
                    }
                    row.* = try p.adaptBorrowed(alloc, layout, row.*);
                }
            }
            self.offset += count;
            self.owner.rows_read += count;
            return .{ .rows = rows, .after = if (self.offset == available) null else rows[rows.len - 1].id };
        }
    };
    duplicates: bool = false,
    document: bool = false,
    arrays: bool = false,
    empty_target: bool = false,
    cancel_at: ?usize = null,
    prepare_guard: enum { none, absence, conflict } = .none,
    merge_path: enum { none, point, index, saturated, not_ready } = .none,
    array_first: i64 = 3,
    array_lower: i32 = 3,
    array_null: bool = false,
    default_mode: bool = false,
    default_prepare_failure: bool = false,
    generated_mode: bool = false,
    deny_source: bool = false,
    returning_mode: bool = false,
    cold_source: bool = false,
    cold_width: usize = 0,
    source_resolves: usize = 0,
    inserting: bool = false,
    row_count: usize = 2,
    rows_read: usize = 0,
    checkpoints: usize = 0,
    captures: usize = 0,
    last_scan_count: usize = 0,
    closes: usize = 0,
    commits: usize = 0,
    writes: usize = 0,
    states: [8]Cursor = undefined,
    cursors: [8]catalog.Cursor = undefined,
    fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, name.table, "target")) {
            try std.testing.expect(action == .read_write);
            if (self.arrays) return .{ .id = 1, .physical_name = "target", .schema_version = 1, .storage_mode = if (self.document) .document else .relational, .indexes = if (self.merge_path != .none and self.merge_path != .point) &.{.{ .name = "id_index", .columns = &.{"id"} }} else &.{}, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "n", .path = "n", .type = .integer },
                .{ .name = "payload", .path = "payload", .type = .json },
                .{ .name = "cold", .path = "cold", .type = .string },
                .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
                .{ .name = "j", .path = "j", .type = .array, .element_type = .jsonb },
                .{ .name = "missing", .path = "missing", .type = .string },
            } };
            if (self.generated_mode) return .{ .id = 1, .physical_name = "target", .schema_version = 1, .storage_mode = if (self.document) .document else .relational, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .json }, .{ .name = "cold", .path = "cold", .type = .string }, .{ .name = "g", .path = "g", .type = .integer, .generated = true } } };
            return .{ .id = 1, .physical_name = "target", .schema_version = 1, .storage_mode = if (self.document) .document else .relational, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .json }, .{ .name = "cold", .path = "cold", .type = .string } } };
        }
        try std.testing.expectEqual(catalog.Action.read, action);
        self.source_resolves += 1;
        if (self.deny_source) return error.Forbidden;
        if (self.arrays) return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{
            .{ .name = "id", .path = "id", .type = .string }, .{ .name = "delta", .path = "delta", .type = .integer }, .{ .name = "a", .path = "a", .type = .array, .element_type = .int16 },
        } };
        if (self.cold_width != 0) {
            const columns = try alloc.alloc(catalog.Column, self.cold_width + 2);
            columns[0] = .{ .name = "id", .path = "id", .type = .string };
            columns[1] = .{ .name = "delta", .path = "delta", .type = .integer };
            for (columns[2..], 0..) |*column, index| {
                const field = try std.fmt.allocPrint(alloc, "cold{d}", .{index});
                column.* = .{ .name = field, .path = field, .type = .string };
            }
            return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = columns };
        }
        return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = if (self.cold_source) &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "delta", .path = "delta", .type = .integer }, .{ .name = "cold", .path = "cold", .type = .string } } else &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "delta", .path = "delta", .type = .integer } } };
    }
    fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (scans.len != 2 and scans.len != 3 and !((self.default_mode or self.returning_mode or self.merge_path != .none) and scans.len == 1)) return error.TestUnexpectedScanCount;
        if (self.merge_path == .not_ready and scans[0].request.index_equality != null) return error.RelationalIndexNotReady;
        self.captures += 1;
        self.last_scan_count = scans.len;
        for (scans, self.states[0..scans.len], self.cursors[0..scans.len]) |scan_, *state, *cursor| {
            if (self.cold_source) for (scan_.request.fields) |field| try std.testing.expect(!std.mem.startsWith(u8, field, "cold"));
            state.* = .{ .owner = self, .request = scan_ };
            cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            if (scan_.table.id == 1 and !self.returning_mode) {
                try std.testing.expect(scan_.request.include_primary_digest);
                if (!self.arrays) for (scan_.request.fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "cold"));
            }
        }
        return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = close };
    }
    fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.closes += 1;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedIndependentScan;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
        if (self.cancel_at == self.checkpoints) return error.Canceled;
    }
    fn prepare(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.prepare_guard != .none) {
            const normalized = try alloc.dupe(catalog.Mutation, mutations);
            for (normalized) |*mutation| switch (self.prepare_guard) {
                .absence => mutation.unique_absence = !mutation.unique_absence,
                .conflict => mutation.conflict_guard = &self.checkpoints,
                .none => unreachable,
            };
            return normalized;
        }
        if (!self.default_mode) return mutations;
        if (self.default_prepare_failure) return error.NativeDefaultFailed;
        const normalized = try alloc.dupe(catalog.Mutation, mutations);
        for (normalized) |*mutation| {
            var object: std.json.ObjectMap = .empty;
            const row = mutation.row orelse return error.TestUnexpectedResult;
            for (row.object.keys(), row.object.values()) |key, value| try object.put(alloc, key, value);
            try std.testing.expect(!object.contains("cold"));
            try object.put(alloc, "cold", .{ .string = "default" });
            if (self.generated_mode) {
                try std.testing.expect(!object.contains("g"));
                try object.put(alloc, "g", .{ .integer = object.get("n").?.integer * 2 });
            }
            mutation.row = .{ .object = object };
        }
        return normalized;
    }
    fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(self.captures, self.closes);
        for (mutations) |mutation| {
            if (self.inserting) {
                try std.testing.expectEqualStrings("fresh", mutation.key);
                try std.testing.expectEqual(@as(?u64, 0), mutation.expected_version);
                try std.testing.expect(mutation.unique_absence);
            } else {
                try std.testing.expectEqual(std.math.maxInt(u64) - 1, mutation.expected_version);
                try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(9)), &mutation.expected_content_digest.?);
            }
            if (mutation.row) |row| {
                if (self.arrays) {
                    const wire = @import("antfly_local_sources").sql_array_wire;
                    try std.testing.expect(!row.object.contains("missing"));
                    for (mutation.json_null_fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "a") and !std.mem.eql(u8, field, "j"));
                    if (self.array_null) try std.testing.expect(row.object.get("a").? == .null) else {
                        const decoded = try wire.decodeBorrowed(alloc, .int64, row.object.get("a").?, .{});
                        try std.testing.expectEqual(self.array_lower, decoded.value.dimensions[0].lower);
                        try std.testing.expectEqual(self.array_first, decoded.value.elements[0].value.integer);
                        try std.testing.expect(decoded.value.elements[1].sql_null);
                    }
                    const json = try wire.decodeBorrowed(alloc, .jsonb, row.object.get("j").?, .{});
                    try std.testing.expect(!json.value.elements[0].sql_null and json.value.elements[1].sql_null);
                }
                try std.testing.expectEqualStrings(if (self.default_mode) "default" else "new", row.object.get("cold").?.string);
                if (self.generated_mode) try std.testing.expectEqual(row.object.get("n").?.integer * 2, row.object.get("g").?.integer);
                try std.testing.expect(row.object.get("payload").? == .null);
                try std.testing.expectEqualStrings("payload", mutation.json_null_fields[0]);
                if (self.document and !self.inserting) try std.testing.expectEqual(@as(i64, 42), row.object.get("undeclared").?.integer);
            }
        }
        self.commits += 1;
        self.writes += mutations.len;
        return .committed;
    }
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .atomic_statement_read_set = self.merge_path != .none, .coordinated_point_reads = self.merge_path != .none, .coordinated_index_reads = self.merge_path != .none and self.merge_path != .point, .vtable = &.{ .resolve = resolve, .open_statement = open, .scan = scan, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepare, .checkpoint = checkpoint, .generate_row_id = generateId } };
    }
    fn generateId(_: *anyopaque, a: std.mem.Allocator) ![]const u8 {
        return a.dupe(u8, "fresh");
    }
};

test "SQL joined mutations retain arrays through coercion scratch retirement and typed DELETE RETURNING" {
    const local = @import("antfly_local_sources");
    for ([_]bool{ false, true }) |document| for ([_]struct { sql: []const u8, first: i64, lower: i32, parameters: []const std.json.Value = &.{}, whole_null: bool = false }{
        .{ .sql = "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 9007199254740993, .lower = -1 },
        .{ .sql = "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 3, .lower = 3 },
        .{ .sql = "UPDATE target SET (a,cold)=(SELECT s.* FROM (SELECT '[-1:1]={9007199254740993,NULL,2}'::bigint[] AS a,'new' AS label FROM source WHERE id='a') s) RETURNING a,j", .first = 9007199254740993, .lower = -1 },
        .{ .sql = "UPDATE target t SET a='[-1:1]={9007199254740993,NULL,2}',cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 9007199254740993, .lower = -1 },
        .{ .sql = "UPDATE target t SET a=$1,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 9223372036854775807, .lower = 5, .parameters = &.{.{ .string = "[5:6]={9223372036854775807,NULL}" }} },
        .{ .sql = "UPDATE target t SET a=ARRAY[1::smallint,NULL],cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 1, .lower = 1 },
        .{ .sql = "UPDATE target t SET a=NULL,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 0, .lower = 0, .whole_null = true },
        .{ .sql = "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING t.a,t.j", .first = 9007199254740993, .lower = -1 },
        .{ .sql = "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET n=t.n+s.delta,cold='new' RETURNING t.a,t.j,s.a", .first = 9007199254740993, .lower = -1 },
        .{ .sql = "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET a=s.a,cold='new' RETURNING t.a,t.j,s.a", .first = 3, .lower = 3 },
        .{ .sql = "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET a=$1,cold='new' RETURNING t.a,t.j,s.a", .first = 9223372036854775807, .lower = 5, .parameters = &.{.{ .string = "[5:6]={9223372036854775807,NULL}" }} },
        .{ .sql = "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET a=NULL,cold='new' RETURNING t.a,t.j,s.a", .first = 0, .lower = 0, .whole_null = true },
        .{ .sql = "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN DELETE RETURNING t.a,t.j,s.a", .first = 9007199254740993, .lower = -1 },
    }) |case| {
        var backend: Backend = .{ .document = document, .arrays = true, .array_first = case.first, .array_lower = case.lower, .array_null = case.whole_null };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var interface = backend.backend();
        if (std.mem.startsWith(u8, case.sql, "MERGE")) interface.atomic_statement_read_set = true;
        var result = try runtime.execute(std.testing.allocator, interface, &compiled, case.parameters, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 4), backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        for (result.output.rows, result.output.sql_nulls.?) |row, flags| {
            if (case.whole_null) {
                try std.testing.expect(row[0] == .null and flags[0]);
            } else {
                var array = try local.sql_array_wire.decode(std.testing.allocator, .int64, row[0], .{});
                defer array.deinit();
                try std.testing.expectEqual(case.lower, array.value.dimensions[0].lower);
                try std.testing.expectEqual(case.first, array.value.elements[0].value.integer);
                try std.testing.expect(array.value.elements[1].sql_null);
            }
            var json = try local.sql_array_wire.decode(std.testing.allocator, .jsonb, row[1], .{});
            defer json.deinit();
            try std.testing.expect(json.value.elements[0].value == .null);
            try std.testing.expect(!json.value.elements[0].sql_null and json.value.elements[1].sql_null);
            if (row.len == 3) {
                try std.testing.expectEqual(.int16, result.output.columns[2].element_type.?);
                var source = try local.sql_array_wire.decode(std.testing.allocator, .int16, row[2], .{});
                defer source.deinit();
                try std.testing.expectEqual(@as(i32, 3), source.value.dimensions[0].lower);
                try std.testing.expectEqual(@as(i64, 3), source.value.elements[0].value.integer);
                try std.testing.expect(source.value.elements[1].sql_null);
            }
        }
    };
}

test "SQL MERGE typed point and index candidates preserve arrays through fallback and RETURNING" {
    const local = @import("antfly_local_sources");
    for ([_]bool{ false, true }) |document| for ([_]@FieldType(Backend, "merge_path"){ .point, .index, .saturated, .not_ready }) |path| for ([_]bool{ false, true }) |deleting| {
        var backend: Backend = .{ .arrays = true, .document = document, .merge_path = path, .array_first = if (deleting) 9007199254740993 else 3, .array_lower = if (deleting) -1 else 3 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "MERGE INTO target t USING source s ON t.{s}=s.id WHEN MATCHED THEN {s} RETURNING t.a,t.j,s.a", .{ if (path == .point) "_id" else "id", if (deleting) "DELETE" else "UPDATE SET a=s.a,cold='new'" });
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expectEqual(backend.captures, backend.closes);
        try std.testing.expectEqual(@as(usize, if (path == .saturated) 3 else 2), backend.captures);
        for (result.output.rows) |row| {
            var target = try local.sql_array_wire.decode(std.testing.allocator, .int64, row[0], .{});
            defer target.deinit();
            try std.testing.expectEqual(backend.array_first, target.value.elements[0].value.integer);
            try std.testing.expectEqual(backend.array_lower, target.value.dimensions[0].lower);
            try std.testing.expect(target.value.elements[1].sql_null);
            var json = try local.sql_array_wire.decode(std.testing.allocator, .jsonb, row[1], .{});
            defer json.deinit();
            try std.testing.expect(!json.value.elements[0].sql_null and json.value.elements[1].sql_null);
            var source = try local.sql_array_wire.decode(std.testing.allocator, .int16, row[2], .{});
            defer source.deinit();
            try std.testing.expectEqual(@as(i64, 3), source.value.elements[0].value.integer);
            try std.testing.expectEqual(@as(i32, 3), source.value.dimensions[0].lower);
            try std.testing.expect(source.value.elements[1].sql_null);
        }
    };
}

test "SQL MERGE typed candidates unwind every allocation failure before publication" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator, path: @FieldType(Backend, "merge_path"), deleting: bool) !void {
            var backend: Backend = .{ .arrays = true, .merge_path = path, .array_first = if (deleting) 9007199254740993 else 3, .array_lower = if (deleting) -1 else 3 };
            const sql = try std.fmt.allocPrint(a, "MERGE INTO target t USING source s ON t.{s}=s.id WHEN MATCHED THEN {s} RETURNING t.a,t.j,s.a", .{ if (path == .point) "_id" else "id", if (deleting) "DELETE" else "UPDATE SET a=s.a,cold='new'" });
            defer a.free(sql);
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    for ([_]@FieldType(Backend, "merge_path"){ .point, .index, .saturated }) |path| {
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{ path, path == .index });
    }
}

test "SQL MERGE source-only typed arrays retain assignment and RETURNING domains" {
    const local = @import("antfly_local_sources");
    for ([_]bool{ false, true }) |document| for ([_]@FieldType(Backend, "merge_path"){ .none, .point, .index }) |path| {
        var backend: Backend = .{ .arrays = true, .document = document, .merge_path = path, .empty_target = true, .inserting = true, .row_count = 1 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "MERGE INTO target t USING source s ON t.{s}=s.id WHEN NOT MATCHED THEN INSERT (a,j,payload,cold) VALUES (s.a,'{{\"null\",NULL}}'::jsonb[],'null'::jsonb,'new') RETURNING t.a,t.j,s.a", .{if (path == .point or path == .none) "_id" else "id"});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var interface = backend.backend();
        interface.atomic_statement_read_set = true;
        var result = try runtime.execute(std.testing.allocator, interface, &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        var target = try local.sql_array_wire.decode(std.testing.allocator, .int64, result.output.rows[0][0], .{});
        defer target.deinit();
        var source = try local.sql_array_wire.decode(std.testing.allocator, .int16, result.output.rows[0][2], .{});
        defer source.deinit();
        try std.testing.expectEqual(@as(i64, 3), target.value.elements[0].value.integer);
        try std.testing.expectEqual(@as(i32, 3), target.value.dimensions[0].lower);
        try std.testing.expect(target.value.elements[1].sql_null);
        try std.testing.expectEqual(@as(i64, 3), source.value.elements[0].value.integer);
        try std.testing.expect(source.value.elements[1].sql_null);
    };
}

test "SQL MERGE refuses normalized guard changes before publication" {
    for ([_]@FieldType(Backend, "prepare_guard"){ .absence, .conflict }) |guard| {
        var backend: Backend = .{ .arrays = true, .merge_path = .point, .prepare_guard = guard };
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET a=s.a,cold='new' RETURNING t.a", .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlBackendResponse, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 }));
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
        try std.testing.expectEqual(backend.captures, backend.closes);
    }
}

test "SQL MERGE cancellation at every checkpoint releases typed captures before publication" {
    for ([_]@FieldType(Backend, "merge_path"){ .point, .index, .saturated, .not_ready }) |path| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "MERGE INTO target t USING source s ON t.{s}=s.id WHEN MATCHED THEN UPDATE SET a=s.a,cold='new' RETURNING t.a,t.j,s.a", .{if (path == .point) "_id" else "id"});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var baseline: Backend = .{ .arrays = true, .merge_path = path };
        var result = try runtime.execute(std.testing.allocator, baseline.backend(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), baseline.commits);
        for (1..baseline.checkpoints + 1) |checkpoint| {
            var backend: Backend = .{ .arrays = true, .merge_path = path, .cancel_at = checkpoint };
            try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 }));
            try std.testing.expectEqual(@as(usize, 0), backend.commits);
            try std.testing.expectEqual(backend.captures, backend.closes);
        }
        var limited: Backend = .{ .arrays = true, .merge_path = path };
        try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, limited.backend(), &compiled, &.{}, .{ .page_rows = 1, .mutation_rows = 1 }));
        try std.testing.expectEqual(@as(usize, 0), limited.commits);
        try std.testing.expectEqual(limited.captures, limited.closes);
    }
}

test "SQL joined RETURNING resolves ambiguous names before any source capture or write" {
    for ([_][]const u8{
        "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t._id=s.id RETURNING a",
        "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t._id=s.id RETURNING coalesce(a,NULL)",
        "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING a",
    }) |sql| {
        var backend: Backend = .{ .arrays = true };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.AmbiguousSqlColumn, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.captures);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL joined source RETURNING preserves PostgreSQL target images and source array domains" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    const fixture = try std.json.parseFromSlice(struct { entries: []const struct { sql: []const u8, target_first: []const u8, target_lower: i32, duplicates: bool = false } }, a, sources.sql_parity_fixtures.joined_returning_reference, .{});
    defer fixture.deinit();
    for (fixture.value.entries) |entry| for ([_]bool{ false, true }) |document| {
        var backend: Backend = .{ .arrays = true, .document = document, .duplicates = entry.duplicates, .array_first = try std.fmt.parseInt(i64, entry.target_first, 10), .array_lower = entry.target_lower };
        var compiled = try compiler.compile(a, entry.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, if (entry.duplicates) 1 else 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(backend.captures, backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        for (result.output.rows, result.output.sql_nulls.?) |row, flags| {
            try std.testing.expectEqualSlices(bool, &.{ false, false, false }, flags);
            var target = try sources.sql_array_wire.decode(a, .int64, row[0], .{});
            defer target.deinit();
            try std.testing.expectEqual(entry.target_lower, target.value.dimensions[0].lower);
            try std.testing.expectEqual(backend.array_first, target.value.elements[0].value.integer);
            try std.testing.expect(target.value.elements[1].sql_null);
            var json = try sources.sql_array_wire.decode(a, .jsonb, row[1], .{});
            defer json.deinit();
            try std.testing.expect(!json.value.elements[0].sql_null and json.value.elements[0].value == .null and json.value.elements[1].sql_null);
            var source = try sources.sql_array_wire.decode(a, .int16, row[2], .{});
            defer source.deinit();
            try std.testing.expectEqual(@as(i32, 3), source.value.dimensions[0].lower);
            try std.testing.expectEqual(@as(i64, 3), source.value.elements[0].value.integer);
            try std.testing.expect(source.value.elements[1].sql_null);
            try std.testing.expectEqual(.int16, result.output.columns[2].element_type.?);
        }
    };
}

test "SQL joined DELETE source RETURNING deduplicates fanout before mutation admission" {
    var backend: Backend = .{ .duplicates = true, .row_count = 256 };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING t._id,s.delta,s.delta+1", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1, .result_rows = 1, .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    const row = result.output.rows[0];
    try std.testing.expectEqualStrings("a", row[0].string);
    try std.testing.expectEqual(try std.fmt.parseInt(i64, row[1].string, 10) + 1, try std.fmt.parseInt(i64, row[2].string, 10));
}

test "SQL joined source RETURNING sees normalized generated values and keeps parameter identity" {
    var backend: Backend = .{ .default_mode = true, .generated_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold=DEFAULT,g=DEFAULT FROM source s WHERE t._id=s.id RETURNING t.n,t.g,t.cold,s.delta+$1", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 5 }}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    for (result.output.rows) |row| {
        const n = try std.fmt.parseInt(i64, row[0].string, 10);
        try std.testing.expectEqual(n * 2, try std.fmt.parseInt(i64, row[1].string, 10));
        try std.testing.expectEqualStrings("default", row[2].string);
        try std.testing.expectEqual(n + 5, try std.fmt.parseInt(i64, row[3].string, 10));
    }
}

test "SQL joined source RETURNING expands qualified stars without capturing cold source fields" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING s.*,t.n", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.columns.len);
    try std.testing.expectEqualStrings("id", result.output.columns[0].name);
    try std.testing.expectEqualStrings("delta", result.output.columns[1].name);
    for (result.output.rows) |row| try std.testing.expectEqualStrings(row[1].string, row[2].string);
}

test "SQL joined source RETURNING execution slots scale with dependencies not schema width" {
    var backend: Backend = .{ .cold_source = true, .cold_width = 128 };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const binding = try describe.bind(arena.allocator(), backend.backend(), &compiled, &.{});
    const joined = binding.joined_mutation.?;
    // Type inference and the dependency-pruned rebind share one schema epoch.
    try std.testing.expectEqual(@as(usize, 1), backend.source_resolves);
    try std.testing.expectEqual(@as(usize, 2), joined.returning_scope.len);
    try std.testing.expectEqual(@as(usize, 1), joined.returning_sources.len);
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 2), backend.source_resolves);
    for (result.output.rows) |row| try std.testing.expectEqualStrings(row[0].string, row[1].string);
}

test "SQL joined source RETURNING replays the bounded shared disk capture" {
    var backend: Backend = .{ .arrays = true, .array_first = 3, .array_lower = 3 };
    var interface = backend.backend();
    interface.execution_io = std.testing.io;
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t.id=s.id RETURNING t.a,s.a", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, interface, &compiled, &.{}, .{ .page_rows = 1, .spill_bytes = 16 << 20, .retained_bytes = 4 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    const local = @import("antfly_local_sources");
    for (result.output.rows) |row| {
        var source = try local.sql_array_wire.decode(std.testing.allocator, .int16, row[1], .{});
        defer source.deinit();
        try std.testing.expectEqual(@as(i32, 3), source.value.dimensions[0].lower);
        try std.testing.expect(source.value.elements[1].sql_null);
    }
}

test "SQL joined source RETURNING rejects normalization guard changes and errors before publication" {
    for ([_]@FieldType(Backend, "prepare_guard"){ .absence, .conflict }) |guard| {
        var backend: Backend = .{ .prepare_guard = guard };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta", .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlBackendResponse, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING t.n,1/(s.delta-10)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL joined source RETURNING unwinds allocation faults and cancellation before publication" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            for ([_][]const u8{
                "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t.id=s.id RETURNING t.a,t.j,s.a",
                "DELETE FROM target t USING source s WHERE t.id=s.id RETURNING t.a,t.j,s.a",
            }) |sql| {
                var backend: Backend = .{ .arrays = true, .array_first = 3, .array_lower = 3 };
                var compiled = try compiler.compile(a, sql, .{});
                defer compiled.deinit();
                var result = runtime.execute(a, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 }) catch |err| {
                    try std.testing.expectEqual(@as(usize, 0), backend.commits);
                    try std.testing.expectEqual(backend.captures, backend.closes);
                    return err;
                };
                defer result.deinit();
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var baseline: Backend = .{ .arrays = true, .array_first = 3, .array_lower = 3 };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t.id=s.id RETURNING t.a,t.j,s.a", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, baseline.backend(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    for (1..baseline.checkpoints + 1) |point| {
        var canceled: Backend = .{ .arrays = true, .cancel_at = point };
        try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, canceled.backend(), &compiled, &.{}, .{ .page_rows = 1 }));
        try std.testing.expectEqual(@as(usize, 0), canceled.commits);
        try std.testing.expectEqual(canceled.captures, canceled.closes);
    }
}

test "SQL joined array assignments reject explicit-only coercions before source capture" {
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "UPDATE target t SET a=ARRAY['1'],cold='new' FROM source s WHERE t._id=s.id RETURNING t.a", .failure = error.SqlAssignmentTypeMismatch },
        .{ .sql = "UPDATE target t SET a='{1,2}'::text,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a", .failure = error.SqlAssignmentTypeMismatch },
        .{ .sql = "UPDATE target t SET a=ARRAY[],cold='new' FROM source s WHERE t._id=s.id RETURNING t.a", .failure = error.UnknownSqlArrayType },
    }) |case| {
        var backend: Backend = .{ .arrays = true };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.captures);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL joined array mutations unwind every allocation failure before writer admission" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .arrays = true };
            var compiled = try compiler.compile(a, "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t._id=s.id RETURNING t.a,t.j", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "SQL UPDATE DEFAULT omits the old cell for native preparation" {
    for ([_]bool{ false, true }) |document| {
        for ([_][]const u8{
            "UPDATE target SET cold=DEFAULT RETURNING cold",
            "UPDATE target SET (n,cold)=ROW(n,DEFAULT) RETURNING cold",
        }) |sql| {
            var backend: Backend = .{ .document = document, .default_mode = true };
            var compiled = try compiler.compile(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
            try std.testing.expectEqual(@as(usize, 1), backend.captures);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
            try std.testing.expectEqualStrings("default", result.output.rows[0][0].string);
        }
    }
    var failing: Backend = .{ .default_mode = true, .default_prepare_failure = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold=DEFAULT RETURNING cold", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.NativeDefaultFailed, runtime.execute(std.testing.allocator, failing.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), failing.captures);
    try std.testing.expectEqual(@as(usize, 1), failing.closes);
    try std.testing.expectEqual(@as(usize, 0), failing.commits);
    var generated: Backend = .{ .default_mode = true, .generated_mode = true };
    var generated_sql = try compiler.compile(std.testing.allocator, "UPDATE target SET cold=DEFAULT,g=DEFAULT RETURNING cold,g", .{});
    defer generated_sql.deinit();
    var generated_result = try runtime.execute(std.testing.allocator, generated.backend(), &generated_sql, &.{}, .{});
    defer generated_result.deinit();
    try std.testing.expectEqual(@as(u64, 2), generated_result.output.rows_affected);
    try std.testing.expectEqualStrings("default", generated_result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("2", generated_result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), generated.commits);
    var invalid = try compiler.compile(std.testing.allocator, "UPDATE target SET g=3", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlGeneratedColumnWrite, runtime.execute(std.testing.allocator, generated.backend(), &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), generated.commits);
}

test "SQL mutation target binding reuses its pinned identity only for reads" {
    var backend: Backend = .{};
    const target_name: ast.Name = .{ .table = "target" };
    const pinned = try backend.backend().vtable.resolve(&backend, std.testing.allocator, target_name, .read_write);
    var adapter: relation_binding.TargetResolveAdapter = .{ .backend = backend.backend(), .table = pinned, .name = target_name };
    const binding = adapter.iface();
    const target = try binding.vtable.resolve(binding.ptr, std.testing.allocator, target_name, .read);
    try std.testing.expectEqual(pinned.id, target.id);
    try std.testing.expectEqual(pinned.schema_version, target.schema_version);
    const source = try binding.vtable.resolve(binding.ptr, std.testing.allocator, .{ .table = "source" }, .read);
    try std.testing.expectEqual(@as(u64, 2), source.id);
    for ([_]catalog.Action{ .write, .read_write, .admin }) |action| {
        try std.testing.expectError(error.UnsupportedSqlExecution, binding.vtable.resolve(binding.ptr, std.testing.allocator, target_name, action));
        try std.testing.expectError(error.UnsupportedSqlExecution, binding.vtable.resolve(binding.ptr, std.testing.allocator, .{ .table = "source" }, action));
    }
}

test "SQL joined mutations preserve one capture exact fences typed values and document fields" {
    for ([_]bool{ false, true }) |document| {
        var backend: Backend = .{ .document = document };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta, cold='new' FROM source s WHERE t._id=s.id RETURNING t.n", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("11", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("22", result.output.rows[1][0].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL joined RETURNING subqueries share prepared target and captured source scope" {
    const Fixture = struct { entries: []const struct { sql: []const u8, rows: []const []const ?[]const u8, scans: usize, read_rows: ?usize = null } };
    const fixture = try std.json.parseFromSlice(Fixture, std.testing.allocator, @import("antfly_local_sources").sql_parity_fixtures.joined_returning_subquery_reference, .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var backend: Backend = .{ .arrays = true, .array_first = 9007199254740993, .array_lower = -1, .returning_mode = true };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        for (result.output.columns, [_][]const u8{ "n", "delta", "d" }) |column_, name| {
            try std.testing.expectEqualStrings(name, column_.name);
            try std.testing.expectEqual(.integer, column_.type);
        }
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(case.rows.len, result.output.rows.len);
        for (case.rows, result.output.rows, result.output.sql_nulls.?) |expected, actual, nulls| {
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual, nulls) |wanted, value, is_null| {
                try std.testing.expectEqual(wanted == null, is_null);
                if (wanted) |text| try std.testing.expectEqualStrings(text, value.string);
            }
        }
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expectEqual(case.scans, backend.last_scan_count);
        try std.testing.expectEqual(case.read_rows orelse case.scans * 2, backend.rows_read);
    }
}

test "SQL joined RETURNING subqueries prune cold scope and scale by captured rows" {
    var previous_work: usize = 0;
    var previous_count: usize = 0;
    for ([_]usize{ 128, 512, 1024 }) |count| {
        var backend: Backend = .{ .returning_mode = true, .cold_source = true, .cold_width = 128, .row_count = count };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta,(SELECT x.delta FROM source x WHERE x.id=s.id)", .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const binding = try describe.bind(arena.allocator(), backend.backend(), &compiled, &.{});
        try std.testing.expectEqual(@as(usize, 3), binding.joined_mutation.?.returning_scope.len);
        try std.testing.expectEqual(@as(usize, 2), binding.joined_mutation.?.returning_sources.len);
        try std.testing.expectEqual(@as(usize, 1), backend.source_resolves);
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 17, .result_rows = count });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, count), result.output.rows_affected);
        try std.testing.expectEqual(3 * count, backend.rows_read);
        try std.testing.expect(backend.checkpoints < 100 * count);
        if (previous_count != 0) try std.testing.expect(backend.checkpoints <= previous_work * count / previous_count + 2 * count);
        previous_count = count;
        previous_work = backend.checkpoints;
        for (result.output.rows, 0..) |row, index| {
            try std.testing.expectEqual(@as(i64, @intCast((index + 1) * 11)), try std.fmt.parseInt(i64, row[0].string, 10));
            try std.testing.expectEqualStrings(row[1].string, row[2].string);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        std.debug.print("SQL joined RETURNING: targets={} rows={} checkpoints={} peak_bytes={}\n", .{ count, backend.rows_read, backend.checkpoints, result.peakMemoryBytes() });
    }
}

test "SQL joined RETURNING subqueries unwind allocation faults and every cancellation" {
    const sql = "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta,(SELECT x.delta FROM source x WHERE x.id=s.id)";
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .returning_mode = true };
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var result = runtime.execute(a, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 }) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), backend.commits);
                try std.testing.expectEqual(backend.captures, backend.closes);
                return err;
            };
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var backend: Backend = .{ .returning_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    var baseline = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
    defer baseline.deinit();
    for (1..backend.checkpoints + 1) |point| {
        var canceled: Backend = .{ .returning_mode = true, .cancel_at = point };
        try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, canceled.backend(), &compiled, &.{}, .{ .page_rows = 1 }));
        try std.testing.expectEqual(@as(usize, 0), canceled.commits);
        try std.testing.expectEqual(canceled.captures, canceled.closes);
    }
}

test "SQL joined RETURNING subqueries replay typed arrays and JSON nulls through disk and memory" {
    const local = @import("antfly_local_sources");
    for ([_]bool{ false, true }) |disk| for ([_]bool{ false, true }) |document| {
        var backend: Backend = .{ .arrays = true, .array_first = 3, .array_lower = 3, .returning_mode = true, .document = document };
        var interface = backend.backend();
        if (disk) interface.execution_io = std.testing.io;
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET a=s.a,cold='new' FROM source s WHERE t.id=s.id RETURNING t.a,t.j,(SELECT s.a),(SELECT x.a FROM source x WHERE x.id=s.id)", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, interface, &compiled, &.{}, .{ .page_rows = 1, .spill_bytes = 16 << 20, .retained_bytes = 4 << 20 });
        defer result.deinit();
        for (result.output.columns, [_]local.sql_array_value.ElementType{ .int64, .jsonb, .int16, .int16 }) |column_, element| {
            try std.testing.expectEqual(.array, column_.type);
            try std.testing.expectEqual(element, column_.element_type.?);
        }
        for (result.output.rows, result.output.sql_nulls.?) |row, flags| {
            for (flags) |flag| try std.testing.expect(!flag);
            for ([_]usize{ 0, 2, 3 }) |index| {
                var array = try local.sql_array_wire.decode(std.testing.allocator, if (index == 0) .int64 else .int16, row[index], .{});
                defer array.deinit();
                try std.testing.expectEqual(@as(i32, 3), array.value.dimensions[0].lower);
                try std.testing.expectEqual(@as(i64, 3), array.value.elements[0].value.integer);
                try std.testing.expect(array.value.elements[1].sql_null);
            }
            var json = try local.sql_array_wire.decode(std.testing.allocator, .jsonb, row[1], .{});
            defer json.deinit();
            try std.testing.expect(!json.value.elements[0].sql_null);
            try std.testing.expect(json.value.elements[0].value == .null);
            try std.testing.expect(json.value.elements[1].sql_null);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    };
}

test "SQL joined RETURNING subquery failures preserve atomic publication" {
    for ([_]struct { expression: []const u8, failure: anyerror }{
        .{ .expression = "(SELECT x.delta FROM source x)", .failure = error.SqlCardinalityViolation },
        .{ .expression = "(SELECT 1/(x.delta-x.delta) FROM source x WHERE x.id=s.id)", .failure = error.SqlDivisionByZero },
        .{ .expression = "(SELECT id)", .failure = error.AmbiguousSqlColumn },
        .{ .expression = "(SELECT s.absent)", .failure = error.UndefinedColumn },
        .{ .expression = "row_number() OVER ()+(SELECT s.delta)", .failure = error.UnsupportedSqlShape },
        .{ .expression = "sum(t.n)+(SELECT s.delta)", .failure = error.UnsupportedSqlShape },
    }) |case| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t.id=s.id RETURNING s.delta,{s}", .{case.expression});
        defer std.testing.allocator.free(sql);
        var backend: Backend = .{ .arrays = true, .returning_mode = true };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
        try std.testing.expectEqual(backend.captures, backend.closes);
    }
    for ([_]@FieldType(Backend, "prepare_guard"){ .absence, .conflict }) |guard| {
        var backend: Backend = .{ .returning_mode = true, .prepare_guard = guard };
        var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING s.delta,(SELECT s.delta)", .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlBackendResponse, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
        try std.testing.expectEqual(backend.captures, backend.closes);
    }
}

test "SQL joined RETURNING subqueries handle zero width empty targets and coherent fanout" {
    for ([_]bool{ false, true }) |empty| {
        var backend: Backend = .{ .returning_mode = true, .empty_target = empty };
        var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING (SELECT max(x.delta) FROM source x)", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, if (empty) 0 else 2), result.output.rows_affected);
        for (result.output.rows) |row| try std.testing.expectEqualStrings("20", row[0].string);
        try std.testing.expectEqual(@as(usize, if (empty) 0 else 1), backend.commits);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(backend.captures, backend.closes);
    }
    var backend: Backend = .{ .returning_mode = true, .duplicates = true };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id RETURNING s.delta,(SELECT s.delta)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqualStrings(result.output.rows[0][0].string, result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL joined RETURNING subqueries use generated postimages and shared parameter identity" {
    var backend: Backend = .{ .returning_mode = true, .default_mode = true, .generated_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta+$1,cold=DEFAULT,g=DEFAULT FROM source s WHERE t._id=s.id RETURNING t.g,(SELECT s.delta+t.g+$1)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("24", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("35", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("46", result.output.rows[1][0].string);
    try std.testing.expectEqualStrings("67", result.output.rows[1][1].string);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
}

test "SQL RETURNING relations share one cut and evaluate prepared postimages before commit" {
    const cases = [_]struct { sql: []const u8, first: []const []const []const u8, nulls: []const bool = &.{ false, false } }{
        .{ .sql = "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT delta FROM source WHERE id='a') AS x", .first = &.{ &.{ "11", "10" }, &.{ "12", "10" } } },
        .{ .sql = "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n) AS x", .first = &.{ &.{ "10", "10" }, &.{ "11", "" } }, .nulls = &.{ false, true } },
        .{ .sql = "DELETE FROM target RETURNING n,(SELECT delta FROM source WHERE id='a') AS x", .first = &.{ &.{ "1", "10" }, &.{ "2", "10" } } },
        .{ .sql = "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT n FROM target WHERE _id='a') AS previous", .first = &.{ &.{ "11", "1" }, &.{ "12", "1" } } },
    };
    for (cases) |case| {
        var backend: Backend = .{ .returning_mode = true };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        for (result.output.rows, result.output.sql_nulls.?, case.first, case.nulls) |row, flags, expected, is_null| {
            try std.testing.expectEqualStrings(expected[0], row[0].string);
            try std.testing.expectEqual(is_null, flags[1]);
            if (!is_null) try std.testing.expectEqualStrings(expected[1], row[1].string);
        }
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 4), backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL masked Apply RETURNING skips dead sources and keeps demanded failure atomic" {
    var dead: Backend = .{ .returning_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET n=n+10,cold='new' RETURNING n,CASE WHEN n<0 THEN (SELECT delta FROM source) ELSE 0 END", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, dead.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    for (result.output.rows) |row| try std.testing.expectEqualStrings("0", row[1].string);
    try std.testing.expectEqual(@as(usize, 2), dead.rows_read);
    try std.testing.expectEqual(@as(usize, 1), dead.captures);
    try std.testing.expectEqual(@as(usize, 1), dead.closes);
    try std.testing.expectEqual(@as(usize, 1), dead.commits);

    var demanded: Backend = .{ .returning_mode = true };
    var failing = try compiler.compile(std.testing.allocator, "UPDATE target SET n=n+10,cold='new' RETURNING n,CASE WHEN n=11 THEN (SELECT delta FROM source WHERE id='a') ELSE (SELECT delta FROM source) END", .{});
    defer failing.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, demanded.backend(), &failing, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), demanded.captures);
    try std.testing.expectEqual(@as(usize, 1), demanded.closes);
    try std.testing.expectEqual(@as(usize, 0), demanded.commits);

    var denied: Backend = .{ .returning_mode = true, .deny_source = true };
    try std.testing.expectError(error.Forbidden, runtime.execute(std.testing.allocator, denied.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), denied.captures);
    try std.testing.expectEqual(@as(usize, 0), denied.commits);
}

test "SQL RETURNING source errors cardinality and zero candidates never publish invalid images" {
    for ([_]struct { sql: []const u8, failure: anyerror }{
        .{ .sql = "UPDATE target SET cold='new' RETURNING (SELECT delta FROM source)", .failure = error.SqlCardinalityViolation },
        .{ .sql = "DELETE FROM target RETURNING (SELECT delta/0 FROM source WHERE id='a')", .failure = error.SqlDivisionByZero },
    }) |case| {
        var backend: Backend = .{ .returning_mode = true };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
    var empty: Backend = .{ .returning_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE n<0 RETURNING (SELECT delta FROM source)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, empty.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), result.output.rows.len);
    try std.testing.expectEqual(@as(usize, 1), empty.closes);
    try std.testing.expectEqual(@as(usize, 0), empty.commits);
}

test "SQL RETURNING relations consume generated images and INSERT source snapshots" {
    var generated: Backend = .{ .returning_mode = true, .default_mode = true, .generated_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=n+10,cold=DEFAULT,g=DEFAULT RETURNING g,(SELECT delta FROM source s WHERE s.delta=t.g-2) AS matched", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, generated.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("22", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("20", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("24", result.output.rows[1][0].string);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expectEqual(@as(usize, 1), generated.captures);
    try std.testing.expectEqual(@as(usize, 1), generated.commits);
    for ([_][]const u8{
        "INSERT INTO target(n,payload,cold) VALUES(9,'null'::jsonb,'new') RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
        "INSERT INTO target(n,payload,cold) SELECT delta,'null'::jsonb,'new' FROM source WHERE id='a' RETURNING n,(SELECT delta FROM source WHERE id='b') AS x",
    }, 0..) |sql, i| {
        var backend: Backend = .{ .returning_mode = true, .inserting = true };
        var insertion = try compiler.compile(std.testing.allocator, sql, .{});
        defer insertion.deinit();
        var inserted = try runtime.execute(std.testing.allocator, backend.backend(), &insertion, &.{}, .{});
        defer inserted.deinit();
        try std.testing.expectEqual(@as(u64, 1), inserted.output.rows_affected);
        try std.testing.expectEqualStrings(if (i == 0) "9" else "10", inserted.output.rows[0][0].string);
        try std.testing.expectEqualStrings(if (i == 0) "10" else "20", inserted.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1 + i), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL RETURNING correlated relation work scales by captured inputs not target fanout" {
    const count = 1024;
    var backend: Backend = .{ .returning_mode = true, .row_count = count };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n)", .{});
    defer compiled.deinit();
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = count, .page_rows = 17 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, count), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, count), result.output.rows.len);
    try std.testing.expectEqual(@as(usize, 2 * count), backend.rows_read);
    try std.testing.expect(backend.checkpoints < 40 * count);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    for (result.output.rows, result.output.sql_nulls.?, 0..) |row, flags, i| {
        const expected: i64 = @intCast(i + 10);
        try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, row[0].string, 10));
        try std.testing.expectEqual(@rem(expected, 10) != 0, flags[1]);
        if (!flags[1]) try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, row[1].string, 10));
    }
    std.debug.print("SQL RETURNING relation: targets={} source_rows={} captures=1 peak_bytes={} elapsed_ns={}\n", .{ count, backend.rows_read - count, result.peakMemoryBytes(), std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
}

test "SQL RETURNING relations unwind allocation faults across capture and prepared evaluation" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .returning_mode = true };
            defer {
                if (backend.captures != backend.closes) @panic("RETURNING capture leaked");
            }
            var compiled = try compiler.compile(a, "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL masked Apply correlated producers reuse captured inputs under mixed demand" {
    var previous_count: usize = 0;
    var previous_work: usize = 0;
    for ([_]usize{ 128, 512, 1024 }) |count| {
        var backend: Backend = .{ .returning_mode = true, .row_count = count };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=n+9,cold='new' RETURNING n,CASE WHEN n%2=0 THEN (SELECT delta FROM source s WHERE s.delta=t.n) ELSE -1 END", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = count, .page_rows = 17 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, count), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 2 * count), backend.rows_read);
        // Fixed iterator/query setup costs are included. Check multiple sizes so
        // a source replay per demanded target cannot masquerade as one I/O scan.
        try std.testing.expect(backend.checkpoints < 80 * count);
        if (previous_count != 0) try std.testing.expect(backend.checkpoints <= previous_work * count / previous_count + 2 * count);
        previous_count = count;
        previous_work = backend.checkpoints;
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        for (result.output.rows, result.output.sql_nulls.?, 0..) |row, flags, i| {
            const n: i64 = @intCast(i + 10);
            try std.testing.expectEqual(n, try std.fmt.parseInt(i64, row[0].string, 10));
            const demanded = @rem(n, 2) == 0;
            try std.testing.expectEqual(demanded and @rem(n, 10) != 0, flags[1]);
            if (!flags[1]) try std.testing.expectEqual(if (demanded) n else -1, try std.fmt.parseInt(i64, row[1].string, 10));
        }
        std.debug.print("SQL masked Apply: targets={} input_rows={} checkpoints={} peak_bytes={}\n", .{ count, backend.rows_read, backend.checkpoints, result.peakMemoryBytes() });
    }
}

test "SQL quantified Apply preserves keyed boundary work and one captured source" {
    for ([_]usize{ 128, 1024 }) |count| {
        var backend: Backend = .{ .returning_mode = true, .row_count = count };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=n,cold='new' RETURNING n,t.n < ANY (SELECT s.delta FROM source s WHERE s.id=t._id ORDER BY s.delta DESC LIMIT 1)", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = count, .page_rows = 17 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, count), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 2 * count), backend.rows_read);
        try std.testing.expect(backend.checkpoints < 100 * count);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        for (result.output.rows) |row| try std.testing.expect(row[1].bool);
        std.debug.print("SQL quantified Apply: targets={} native_rows={} checkpoints={} peak_bytes={}\n", .{ count, backend.rows_read, backend.checkpoints, result.peakMemoryBytes() });
    }
}

test "SQL quantified Apply unwinds allocation faults and every cancellation before commit" {
    const sql = "UPDATE target t SET n=n,cold='new' RETURNING t.n < ANY (SELECT s.delta FROM source s WHERE s.id=t._id ORDER BY s.delta DESC LIMIT 1)";
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .returning_mode = true };
            defer std.debug.assert(backend.captures == backend.closes);
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            for (result.output.rows) |row| try std.testing.expect(row[0].bool);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var backend: Backend = .{ .returning_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    var baseline = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer baseline.deinit();
    for (1..backend.checkpoints + 1) |point| {
        var canceled: Backend = .{ .returning_mode = true, .cancel_at = point };
        try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, canceled.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(canceled.captures, canceled.closes);
        try std.testing.expectEqual(@as(usize, 0), canceled.commits);
    }
}

test "SQL scalar cardinality stops before later value errors and never commits" {
    for ([_]struct { sql: []const u8, parameters: []const std.json.Value = &.{} }{
        .{ .sql = "UPDATE target SET cold='new' RETURNING (SELECT CASE WHEN delta=30 THEN 1/(delta-30) ELSE delta END FROM source LIMIT 1000)" },
        .{ .sql = "UPDATE target SET cold='new' RETURNING CASE WHEN TRUE THEN (SELECT CASE WHEN delta=30 THEN 1/(delta-30) ELSE delta END FROM source) ELSE 0 END" },
        .{ .sql = "UPDATE target SET cold='new' RETURNING (SELECT CASE WHEN delta=30 THEN 1/(delta-30) ELSE delta END FROM source LIMIT $1)", .parameters = &.{.{ .integer = 1000 }} },
        .{ .sql = "UPDATE target SET cold='new' RETURNING (SELECT CASE WHEN delta=30 THEN 1/(delta-30) ELSE delta END FROM source LIMIT $1)", .parameters = &.{.null} },
    }) |case| {
        var backend: Backend = .{ .returning_mode = true, .row_count = 3 };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &compiled, case.parameters, .{ .page_rows = 2 }));
        try std.testing.expectEqual(@as(usize, 5), backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL scalar cardinality value programs run only for demanded correlation matches" {
    var backend: Backend = .{ .returning_mode = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT 1/(s.delta-20) FROM source s WHERE s.delta=t.n)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqualStrings("0", result.output.rows[0][1].string);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expectEqual(@as(usize, 4), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
}

test "SQL scalar cardinality bounds million-row requests by actual demand" {
    const count = 4096;
    var backend: Backend = .{ .returning_mode = true, .row_count = count };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' RETURNING (SELECT delta FROM source LIMIT $1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1_000_000 }}, .{ .page_rows = 2, .result_rows = count }));
    try std.testing.expectEqual(@as(usize, count + 2), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL scalar cardinality reads two rows from an unestimated million-row source" {
    var backend: Backend = .{ .returning_mode = true, .row_count = 1_000_000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT delta FROM source LIMIT $1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1_000_000 }}, .{ .page_rows = 2 }));
    try std.testing.expectEqual(@as(usize, 2), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL global aggregate invocation constants reuse a captured empty or nonempty source" {
    for ([_]usize{ 0, 512 }) |count| {
        var backend: Backend = .{ .returning_mode = true, .cold_source = true, .row_count = count };
        var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
        var iface = backend.backend();
        iface.decision_provider = provider.provider();
        var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT o.x+COUNT(*) FROM source i) AS n,(SELECT ai_probability(CAST(o.x AS TEXT),'Refund?','local')+COUNT(*) FROM source i) AS p FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{}, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        for (result.output.rows, 1..) |row, index| {
            try std.testing.expectEqual(count + index, try std.fmt.parseInt(usize, row[0].string, 10));
            try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(count)) + 0.9, row[1].float, 0.00001);
        }
        try std.testing.expectEqual(@as(usize, 2), provider.calls);
        // Two physical source occurrences, each read once, not once per parent.
        try std.testing.expectEqual(count * 2, backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL sorted scalar producers execute only selected parents and retain cold scan pruning" {
    const cases = [_]struct { sql: []const u8, calls: usize, rows: usize, scans: usize = 512 }{
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Refund?','local')) FROM source o ORDER BY o.delta DESC LIMIT $1 OFFSET $2", .calls = 3, .rows = 2 },
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Refund?','local')) FROM source o ORDER BY o.delta DESC LIMIT 0 OFFSET $2", .calls = 0, .rows = 0, .scans = 0 },
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Refund?','local')) AS p FROM source o ORDER BY p LIMIT $1 OFFSET $2", .calls = 512, .rows = 2 },
        .{ .sql = "SELECT o.delta AS rank,(SELECT ai_probability(o.id,'Refund?','local')) FROM source o ORDER BY rank DESC LIMIT $1 OFFSET $2", .calls = 3, .rows = 2 },
        .{ .sql = "SELECT o.delta,(SELECT ai_probability(o.id,'Refund?','local')) FROM source o ORDER BY 1 DESC LIMIT $1 OFFSET $2", .calls = 3, .rows = 2 },
        .{ .sql = "SELECT ai_probability(o.id,'Sort?','local') AS rank,(SELECT ai_probability(o.id,'Output?','local')) FROM source o ORDER BY rank DESC LIMIT $1 OFFSET $2", .calls = 515, .rows = 2 },
        .{ .sql = "SELECT ai_probability(o.id,'Sort?','local') AS rank,(SELECT ai_probability(o.id,'Output?','local')) FROM source o WHERE o.delta>5000 ORDER BY rank DESC LIMIT $1 OFFSET $2", .calls = 15, .rows = 2 },
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Sort?','local')) AS rank,(SELECT ai_probability(o.id,'Output?','local')) FROM source o ORDER BY rank DESC LIMIT $1 OFFSET $2", .calls = 515, .rows = 2 },
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Output?','local')) FROM source o ORDER BY (SELECT ai_probability(o.id,'Sort?','local')) DESC LIMIT $1 OFFSET $2", .calls = 515, .rows = 2 },
        .{ .sql = "SELECT (SELECT ai_probability(o.id,'Refund?','local')) FROM source o LIMIT $1 OFFSET $2", .calls = 3, .rows = 2, .scans = 3 },
    };
    for (cases) |case| {
        var backend: Backend = .{ .returning_mode = true, .cold_source = true, .row_count = 512 };
        var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
        var iface = backend.backend();
        iface.decision_provider = provider.provider();
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{ .{ .integer = 2 }, .{ .integer = 1 } }, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        try std.testing.expectEqual(case.calls, provider.calls);
        try std.testing.expectEqual(case.scans, backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
        if (result.output.columns.len == 2 and result.output.columns[0].type == .integer) {
            try std.testing.expectEqualStrings("5110", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("5100", result.output.rows[1][0].string);
        }
    }
}

test "SQL transparent sorted selection forwards wide cold scopes without widening public limits" {
    var backend: Backend = .{ .returning_mode = true, .cold_source = true, .cold_width = 512 };
    var wildcard = try compiler.compile(std.testing.allocator, "SELECT * FROM source", .{});
    defer wildcard.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &wildcard, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.captures);
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT o.delta+1) FROM source o ORDER BY o.delta DESC LIMIT 1", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("21", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 2), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
}

test "SQL transparent sorted selection unwinds every allocation failure" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .returning_mode = true, .cold_source = true };
            defer if (backend.captures != backend.closes) @panic("selection capture leaked");
            var compiled = try compiler.compile(a, "SELECT (SELECT o.delta+1) AS rank,(SELECT o.delta+1) AS v FROM source o ORDER BY rank DESC LIMIT $1 OFFSET $2", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{ .{ .integer = 1 }, .{ .integer = 0 } }, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings("21", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("21", result.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL bounded scalar producers retain one capture and reusable correlation builds" {
    const count = 512;
    for ([_][]const u8{
        "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n ORDER BY s.delta DESC LIMIT 1)",
        "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n GROUP BY s.delta HAVING s.delta=t.n)",
    }) |sql| {
        var backend: Backend = .{ .returning_mode = true, .row_count = count };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = count, .page_rows = 17 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, count), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 2 * count), backend.rows_read);
        try std.testing.expect(backend.checkpoints < 60 * count);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        for (result.output.rows, result.output.sql_nulls.?, 0..) |row, flags, i| {
            const n: i64 = @intCast(i + 10);
            try std.testing.expectEqual(n, try std.fmt.parseInt(i64, row[0].string, 10));
            try std.testing.expectEqual(@rem(n, 10) != 0, flags[1]);
            if (!flags[1]) try std.testing.expectEqual(n, try std.fmt.parseInt(i64, row[1].string, 10));
        }
        std.debug.print("SQL bounded scalar: inputs={} checkpoints={} peak_bytes={}\n", .{ backend.rows_read, backend.checkpoints, result.peakMemoryBytes() });
    }
}

test "SQL masked Apply unwinds allocation faults across demanded and bypassed producers" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{ .returning_mode = true };
            defer {
                if (backend.captures != backend.closes) @panic("masked Apply capture leaked");
            }
            var compiled = try compiler.compile(a, "UPDATE target t SET n=n+9,cold='new' RETURNING n,CASE WHEN n=10 THEN (SELECT delta FROM source s WHERE s.delta=t.n ORDER BY s.delta DESC LIMIT $1) ELSE -1 END", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL target-only mutation subqueries share one captured relational plan" {
    // sql-0068: its EXPLAIN remains dry-run, while these ordinary mutations
    // prove the same source-aware plan executes under one snapshot and commit.
    for ([_]struct { sql: []const u8, tag: []const u8 }{
        .{ .sql = "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET cold='new' WHERE EXISTS (SELECT id FROM source WHERE source.id=target._id)", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET n=(SELECT delta FROM source WHERE source.id=target._id),cold='new'", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source WHERE id='a')", .tag = "UPDATE" },
        .{ .sql = "DELETE FROM target WHERE _id IN (SELECT id FROM source)", .tag = "DELETE" },
    }) |case| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.tag, result.output.command_tag);
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expect(backend.last_scan_count <= 3);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expectEqual(@as(usize, 2), backend.writes);
    }
}

test "SQL sorted wildcard projection pins one catalog identity and evaluates only the selected prefix" {
    for ([_]usize{ 0, 1, 130, 512 }) |offset| {
        var backend: Backend = .{ .returning_mode = true, .row_count = 512 };
        var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
        var iface = backend.backend();
        iface.decision_provider = provider.provider();
        var compiled = try compiler.compile(std.testing.allocator, "SELECT o.*,(SELECT ai_probability(o.id,'Output?','local')) AS p FROM source o ORDER BY 2 DESC LIMIT $1 OFFSET $2", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{ .{ .integer = 2 }, .{ .integer = @intCast(offset) } }, .{ .page_rows = 4 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (offset == 512) 0 else 2), result.output.rows.len);
        try std.testing.expectEqual(@min(offset + 2, 512), provider.calls);
        try std.testing.expectEqual(@as(usize, 1), backend.source_resolves);
        try std.testing.expectEqual(@as(usize, 512), backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqualStrings("id", result.output.columns[0].name);
        try std.testing.expectEqualStrings("delta", result.output.columns[1].name);
        for (result.output.rows, 0..) |row, index| {
            try std.testing.expectEqual((512 - offset - index) * 10, try std.fmt.parseInt(usize, row[1].string, 10));
            try std.testing.expectApproxEqAbs(@as(f64, 0.9), row[2].float, 0.00001);
        }
    }
}

test "SQL window input subqueries share keys and retain qualified input demand" {
    for ([_]bool{ false, true }) |filtered| {
        var backend: Backend = .{ .returning_mode = true, .row_count = 128 };
        var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
        var iface = backend.backend();
        iface.decision_provider = provider.provider();
        const sql = try std.fmt.allocPrint(std.testing.allocator, "SELECT o.delta,row_number() OVER w AS n,rank() OVER w AS r FROM source o {s} WINDOW w AS (PARTITION BY (SELECT ai_probability(o.id,'Window?','local')) ORDER BY o.delta DESC) ORDER BY o.delta DESC LIMIT 2", .{if (filtered) "WHERE o.delta>1260" else ""});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        try std.testing.expectEqual(@as(usize, if (filtered) 2 else 128), provider.calls);
        try std.testing.expectEqual(@as(usize, 128), backend.rows_read);
        try std.testing.expectEqual(@as(usize, 1), backend.source_resolves);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        for (result.output.rows, 0..) |row, index| {
            try std.testing.expectEqual((128 - index) * 10, try std.fmt.parseInt(usize, row[0].string, 10));
            try std.testing.expectEqual(index + 1, try std.fmt.parseInt(usize, row[1].string, 10));
            try std.testing.expectEqual(index + 1, try std.fmt.parseInt(usize, row[2].string, 10));
        }
    }
}

test "SQL phase outputs prune wide cold layouts and demand only sorted scalar results" {
    for ([_][]const u8{
        "SELECT o.id,SUM(o.delta) AS n,(SELECT ai_probability(o.id,'Output?','local')) AS p FROM source o GROUP BY o.id ORDER BY n DESC LIMIT 2",
        "SELECT o.id,row_number() OVER (ORDER BY o.delta DESC) AS n,(SELECT ai_probability(o.id,'Output?','local')) AS p FROM source o ORDER BY o.delta DESC LIMIT 2",
    }, 0..) |sql, shape| {
        var small_work: usize = 0;
        for ([_]usize{ 128, 1024 }) |count| {
            var backend: Backend = .{ .returning_mode = true, .cold_source = true, .cold_width = 1024, .row_count = count };
            var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
            var iface = backend.backend();
            iface.decision_provider = provider.provider();
            var compiled = try compiler.compile(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqual(@as(usize, 2), provider.calls);
            try std.testing.expectEqual(count, backend.rows_read);
            try std.testing.expectEqual(@as(usize, 1), backend.source_resolves);
            try std.testing.expectEqual(@as(usize, 1), backend.last_scan_count);
            try std.testing.expectEqual(@as(usize, 1), backend.captures);
            try std.testing.expectEqual(@as(usize, 1), backend.closes);
            for (result.output.rows, 0..) |row, index| {
                const expected = try std.fmt.allocPrint(std.testing.allocator, "row{d}", .{count - index - 1});
                defer std.testing.allocator.free(expected);
                try std.testing.expectEqualStrings(expected, row[0].string);
            }
            if (count == 128) small_work = backend.checkpoints else try std.testing.expect(backend.checkpoints < small_work * 16);
            std.debug.print("SQL phase output: shape={s} rows={} checkpoints={} peak_bytes={} output_calls=2\n", .{ if (shape == 0) "grouped" else "window", count, backend.checkpoints, result.peakMemoryBytes() });
        }
    }
}

test "SQL phase outputs cancel every checkpoint and release one captured read" {
    var compiled = try compiler.compile(std.testing.allocator, "SELECT o.id,row_number() OVER (ORDER BY o.delta DESC),(SELECT o.delta+1) FROM source o ORDER BY o.delta DESC LIMIT 1", .{});
    defer compiled.deinit();
    var baseline: Backend = .{ .returning_mode = true };
    var output = try runtime.execute(std.testing.allocator, baseline.backend(), &compiled, &.{}, .{});
    output.deinit();
    for (1..baseline.checkpoints + 1) |checkpoint| {
        var backend: Backend = .{ .returning_mode = true, .cancel_at = checkpoint };
        try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(backend.captures, backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL row subquery assigns positional values after bounded ordered source" {
    for ([_][]const u8{
        "UPDATE target SET (n,cold)=(SELECT delta AS amount,'new' AS label FROM source ORDER BY amount DESC LIMIT 1) RETURNING n,cold",
        "WITH s AS (SELECT delta FROM source) UPDATE target SET (n,cold)=(SELECT delta,'new' FROM s ORDER BY delta DESC LIMIT 1) RETURNING n,cold",
        "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
        "UPDATE target SET (n,cold)=(SELECT s.* FROM (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
        "UPDATE target SET (n,cold)=(SELECT s.*,'new' FROM (SELECT delta FROM source ORDER BY delta DESC LIMIT 1) s) RETURNING n,cold",
        "WITH s AS (SELECT delta,'new' AS label FROM source ORDER BY delta DESC LIMIT 1) UPDATE target SET (n,cold)=(SELECT * FROM s) RETURNING n,cold",
        "WITH \"$update_row_source_0\" AS (SELECT delta FROM source) UPDATE target SET (n,cold)=(SELECT delta,'new' FROM \"$update_row_source_0\" ORDER BY delta DESC LIMIT 1) RETURNING n,cold",
    }) |sql| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("20", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("new", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL row subquery schema bound width fails before capture or mutation" {
    for ([_][]const u8{
        "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta FROM source) s)",
        "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label,id FROM source) s)",
        "UPDATE target SET (n,cold)=(SELECT s.*,'extra' FROM (SELECT delta,'new' AS label FROM source) s)",
        "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta FROM source WHERE false) s)",
    }) |sql| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlSyntax, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.captures);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL row subquery count star has one output rather than zero projections" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET (n)=(SELECT COUNT(*) FROM source),cold='new' RETURNING n,cold", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    for (result.output.rows) |row| {
        try std.testing.expectEqualStrings("2", row[0].string);
        try std.testing.expectEqualStrings("new", row[1].string);
    }
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
}

test "SQL row subquery shares one correlated producer across typed assignments" {
    for ([_]struct { sql: []const u8, first: []const u8 = "10", last: []const u8 = "20" }{
        .{ .sql = "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source WHERE source.id=target._id) RETURNING n,cold" },
        .{ .sql = "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id ORDER BY s.delta DESC LIMIT 1) RETURNING n,cold" },
        .{ .sql = "UPDATE target t SET (n,cold)=(SELECT q.* FROM (SELECT s.delta,'new' AS label FROM source s WHERE s.id=t._id) q) RETURNING n,cold" },
        .{ .sql = "UPDATE target t SET (n,cold)=(SELECT s.delta+t.n,'new' FROM source s WHERE s.id=t._id) RETURNING n,cold", .first = "11", .last = "22" },
        .{ .sql = "UPDATE target t SET (n,cold)=(SELECT COUNT(*),'new' FROM source s WHERE s.id=t._id) RETURNING n,cold", .first = "1", .last = "1" },
    }) |case| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings(case.first, result.output.rows[0][0].string);
        try std.testing.expectEqualStrings(case.last, result.output.rows[1][0].string);
        for (result.output.rows) |row| try std.testing.expectEqualStrings("new", row[1].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL row subquery keyed correlation scales without source rescans or duplicate producers" {
    for ([_]usize{ 128, 1024 }) |count| {
        var backend: Backend = .{ .row_count = count };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id) RETURNING n,cold", .{});
        defer compiled.deinit();
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 17, .result_rows = count });
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, @intCast(count)), result.output.rows_affected);
        for (result.output.rows, 1..) |row, n| try std.testing.expectEqual(n * 10, try std.fmt.parseInt(usize, row[0].string, 10));
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expect(backend.rows_read <= count * 2);
        try std.testing.expect(backend.checkpoints <= count * 32);
        std.debug.print("SQL row producer: rows={} native_rows={} checkpoints={} peak_bytes={} elapsed_ns={}\n", .{ count, backend.rows_read, backend.checkpoints, result.peakMemoryBytes(), std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    }
}

test "SQL row subquery expanded width releases every allocation on failure" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{};
            defer std.debug.assert(backend.captures == backend.closes);
            var compiled = try compiler.compile(a, "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id) RETURNING n,cold", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL row subquery empty expanded source assigns SQL NULL to every target" {
    const Empty = struct {
        fn mutate(raw: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *Backend = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.captures, self.closes);
            try std.testing.expectEqual(@as(usize, 2), mutations.len);
            for (mutations) |mutation| {
                try std.testing.expect(mutation.row.?.object.get("n").? == .null);
                try std.testing.expect(mutation.row.?.object.get("cold").? == .null);
                try std.testing.expectEqual(std.math.maxInt(u64) - 1, mutation.expected_version);
                try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(9)), &mutation.expected_content_digest.?);
            }
            self.commits += 1;
            return .committed;
        }
    };
    var backend: Backend = .{};
    var iface = backend.backend();
    var vtable = iface.vtable.*;
    vtable.mutate = Empty.mutate;
    vtable.mutate_prepared = Empty.mutate;
    iface.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id AND false) RETURNING n,cold", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    for (result.output.rows, result.output.sql_nulls.?) |row, flags| for (row, flags) |cell, is_null| {
        try std.testing.expect(cell == .null and is_null);
    };
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
}

test "SQL mutation membership subquery scales by captured rows" {
    var backend: Backend = .{ .row_count = 1024 };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 17 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1024), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    try std.testing.expectEqual(@as(usize, 1024), backend.writes);
    try std.testing.expect(backend.rows_read <= 3 * backend.row_count);
}

test "SQL mutation subquery quota aborts before commit" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL mutation subquery requires source read authority before capture" {
    var backend: Backend = .{ .deny_source = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.Forbidden, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.captures);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL mutation scalar subquery rejects multiple rows before commit" {
    for ([_][]const u8{
        "UPDATE target SET n=(SELECT delta FROM source WHERE source.id=target._id),cold='new'",
        "UPDATE target SET (n,cold)=ROW((SELECT delta FROM source WHERE source.id=target._id),'new')",
        "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source)",
        "UPDATE target SET (n,cold)=(SELECT * FROM (SELECT delta,'new' AS label FROM source) s)",
        "UPDATE target t SET (n,cold)=(SELECT s.delta,'new' FROM source s WHERE s.id=t._id)",
    }) |sql| {
        var backend: Backend = .{ .duplicates = true };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL joined FROM mutations choose hash keys and preserve CTE target identity" {
    for ([_][]const u8{
        "UPDATE target t SET n=t.n+s.delta, cold='new' FROM source s WHERE t._id=s.id RETURNING t.n",
        "WITH target AS (SELECT id, delta FROM source) UPDATE target t SET n=t.n+s.delta, cold='new' FROM target s WHERE t._id=s.id RETURNING t.n",
        "WITH RECURSIVE input(id,delta) AS (SELECT id,delta FROM source UNION ALL SELECT id,delta FROM input WHERE FALSE) UPDATE target t SET n=t.n+s.delta,cold='new' FROM input s WHERE t._id=s.id RETURNING t.n",
    }) |sql| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const bound = try describe.bind(arena.allocator(), backend.backend(), &compiled, &.{});
        const relation = bound.joined_mutation.?.input.relation.?;
        try std.testing.expect(relation.root.operation == .join);
        try std.testing.expectEqual(@as(usize, 1), relation.root.operation.join.left_keys.len);
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("11", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("22", result.output.rows[1][0].string);
    }
}

test "SQL original prepared CTE UPDATE DELETE and MERGE capture before one mutation" {
    const alloc = std.testing.allocator;
    const corpus = try std.json.parseFromSlice(std.json.Value, alloc, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
    defer corpus.deinit();
    const Fixture = struct {
        const Self = @This();
        const Cursor = struct {
            owner: *Self,
            request: catalog.StatementScan,
            done: bool = false,
            fn next(ptr: *anyopaque, allocator: std.mem.Allocator, _: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.done) return .{ .rows = &.{} };
                self.done = true;
                if (self.owner.fail_read) return error.SourceReadFailed;
                const rows = try allocator.alloc(catalog.Row, 2);
                for (rows, 0..) |*row, i| {
                    var values: std.json.ObjectMap = .empty;
                    for (self.request.request.fields) |field| {
                        const value: std.json.Value = if (std.mem.eql(u8, field, "id")) .{ .string = if (i == 0) "u1" else "u2" } else if (std.mem.eql(u8, field, "status")) .{ .string = "open" } else .null;
                        try values.put(allocator, field, value);
                    }
                    row.* = .{ .id = if (i == 0) "row-u1" else "row-u2", .version = 1, .expected_content_digest = if (self.request.request.include_primary_digest) @splat(9) else null, .value = .{ .object = values } };
                    if (self.request.request.include_document) row.document = try std.json.parseFromSliceLeaky(std.json.Value, allocator, if (i == 0) "{\"id\":\"u1\",\"status\":\"open\"}" else "{\"id\":\"u2\",\"status\":\"open\"}", .{});
                }
                return .{ .rows = rows };
            }
        };
        states: [4]Cursor = undefined,
        cursors: [4]catalog.Cursor = undefined,
        captures: usize = 0,
        closes: usize = 0,
        commits: usize = 0,
        fail_read: bool = false,
        deleting: bool = false,
        expected_status: []const u8 = "done",
        atomic_read_set: bool = true,
        fn backend(self: *Self) catalog.Backend {
            return .{ .ptr = self, .atomic_statement_read_set = self.atomic_read_set, .vtable = &.{ .resolve = resolve, .scan = scan, .open_statement = open, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            try std.testing.expectEqualStrings("usage_records", name.table);
            return .{ .id = 1, .physical_name = "usage_records", .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "status", .path = "status", .type = .string } } };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedIndependentScan;
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expect(scans.len > 0 and scans.len <= self.states.len);
            self.captures += 1;
            for (scans, self.states[0..scans.len], self.cursors[0..scans.len]) |request, *state, *cursor| {
                state.* = .{ .owner = self, .request = request };
                cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            }
            return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = close };
        }
        fn close(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), self.closes);
            try std.testing.expectEqual(@as(usize, 2), mutations.len);
            for (mutations, 0..) |mutation, i| {
                try std.testing.expectEqualStrings(if (i == 0) "row-u1" else "row-u2", mutation.key);
                try std.testing.expectEqual(@as(u64, 1), mutation.expected_version);
                if (self.deleting) {
                    try std.testing.expect(mutation.row == null);
                } else try std.testing.expectEqualStrings(self.expected_status, mutation.row.?.object.get("status").?.string);
            }
            self.commits += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    for ([_]struct { id: []const u8, tag: []const u8, deleting: bool, expected_status: []const u8 }{
        .{ .id = "sql-0006", .tag = "UPDATE", .deleting = false, .expected_status = "done" },
        .{ .id = "sql-0007", .tag = "DELETE", .deleting = true, .expected_status = "" },
        .{ .id = "sql-0008", .tag = "MERGE", .deleting = false, .expected_status = "open" },
    }) |case| {
        const original = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, case.id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const separator = std.mem.indexOf(u8, original, " AS ") orelse return error.TestInvalidCorpusCase;
        var compiled = try compiler.compile(alloc, original[separator + " AS ".len ..], .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .deleting = case.deleting, .expected_status = case.expected_status };
        var result = try runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.tag, result.output.command_tag);
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        fixture = .{ .deleting = case.deleting, .expected_status = case.expected_status, .fail_read = true };
        try std.testing.expectError(error.SourceReadFailed, runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        if (std.mem.eql(u8, case.id, "sql-0008")) {
            fixture = .{ .atomic_read_set = false };
            try std.testing.expectError(error.SqlRangeTrackingRequired, runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{}));
            try std.testing.expectEqual(@as(usize, 0), fixture.captures);
            try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        }
    }
}

test "SQL joined mutations empty matches and limits never partially commit" {
    var backend: Backend = .{};
    var empty = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id AND FALSE", .{});
    defer empty.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &empty, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
    backend = .{};
    var limited = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id", .{});
    defer limited.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &limited, &.{}, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL joined mutation equality work scales with inputs not Cartesian candidates" {
    const benchmark = try std.testing.environ.contains(std.testing.allocator, "ANTFLY_SQL_JOINED_MUTATION_BENCHMARK");
    const rows: usize = if (benchmark) 4096 else 1024;
    const execution_alloc = if (benchmark) std.heap.smp_allocator else std.testing.allocator;
    var backend: Backend = .{ .row_count = rows };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id", .{});
    defer compiled.deinit();
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var result = try runtime.execute(execution_alloc, backend.backend(), &compiled, &.{}, .{ .mutation_rows = rows, .retained_bytes = if (benchmark) 64 << 20 else 8 << 20 });
    defer result.deinit();
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
    try std.testing.expectEqual(@as(u64, rows), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, rows * 2), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expect(backend.checkpoints < rows * 100);
    std.debug.print("SQL joined mutation: targets={d} source_rows={d} native_rows={d} captures={d} checkpoints={d} peak_bytes={d} elapsed_ns={d}\n", .{ rows, rows, backend.rows_read, backend.captures, backend.checkpoints, result.peakMemoryBytes(), elapsed });
}

test "SQL DELETE source fanout consumes one target mutation slot" {
    var backend: Backend = .{ .row_count = 256, .duplicates = true };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL joined UPDATE and DELETE retain one coherent source match per target" {
    var backend: Backend = .{ .duplicates = true };
    var update = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id RETURNING t.n,s.delta,s.delta+1", .{});
    defer update.deinit();
    var updated = try runtime.execute(std.testing.allocator, backend.backend(), &update, &.{}, .{ .mutation_rows = 1, .result_rows = 1 });
    defer updated.deinit();
    try std.testing.expectEqual(@as(u64, 1), updated.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    const row = updated.output.rows[0];
    try std.testing.expectEqualStrings(row[0].string, row[1].string);
    const chosen = try std.fmt.parseInt(i64, row[1].string, 10);
    try std.testing.expect(chosen == 10 or chosen == 20);
    try std.testing.expectEqual(chosen + 1, try std.fmt.parseInt(i64, row[2].string, 10));
    backend = .{ .duplicates = true };
    var deletion = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id", .{});
    defer deletion.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &deletion, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL joined mutations release every failed allocation without partial commits" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "UPDATE target t SET n=s.delta+$1, cold='new' FROM source s WHERE t._id=s.id", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{.{ .integer = 2 }}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
