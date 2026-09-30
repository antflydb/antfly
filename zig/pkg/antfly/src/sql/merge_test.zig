// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const merge_mutation = @import("merge_mutation.zig");
const runtime = @import("runtime.zig");

test "SQL original MERGE corpus admitted plans retain exact source SQL" {
    const Probe = struct {
        fn table(name: []const u8) !catalog.Table {
            if (std.mem.eql(u8, name, "usage_records")) return .{ .id = 1, .physical_name = name, .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
                .{ .name = "amount", .path = "amount", .type = .number },
            } };
            if (std.mem.eql(u8, name, "source_records")) return .{ .id = 2, .physical_name = name, .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
            } };
            if (std.mem.eql(u8, name, "archived_records")) return .{ .id = 3, .physical_name = name, .schema_version = 1, .columns = &.{
                .{ .name = "archive_id", .path = "archive_id", .type = .string },
                .{ .name = "archive_status", .path = "archive_status", .type = .string },
                .{ .name = "archive_amount", .path = "archive_amount", .type = .number },
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
            } };
            if (std.mem.eql(u8, name, "prices")) return .{ .id = 4, .physical_name = name, .schema_version = 1, .columns = &.{
                .{ .name = "sku", .path = "sku", .type = .string },
                .{ .name = "source_id", .path = "source_id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
            } };
            return error.TestUnexpectedTable;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            return table(name.table);
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    var probe: Probe = .{};
    var covered: usize = 0;
    for (parsed.value.object.get("entries").?.array.items) |entry| {
        const id = entry.object.get("id").?.string;
        const ordinal = if (std.mem.startsWith(u8, id, "sql-")) std.fmt.parseInt(usize, id[4..], 10) catch continue else continue;
        if (!((ordinal >= 579 and ordinal <= 591) or ordinal == 623)) continue;
        const sql = entry.object.get("sql").?.string;
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expect(compiled.statement == .merge);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const target = try Probe.table(compiled.statement.merge.table.table);
        const bound = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), target, &compiled, &.{});
        try std.testing.expectEqual(compiled.statement.merge.arms.len, bound.arms.len);
        covered += 1;
    }
    try std.testing.expectEqual(@as(usize, 14), covered);
}

test "SQL original cross-table MERGE executes matched and source-only rows atomically" {
    const Probe = struct {
        const Cursor = struct {
            table_id: u64,
            done: bool = false,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.done) return .{ .rows = &.{} };
                self.done = true;
                const count: usize = if (self.table_id == 1) 1 else 2;
                const rows = try alloc.alloc(catalog.Row, count);
                for (rows, 0..) |*row, index| {
                    var object: std.json.ObjectMap = .empty;
                    const id = if (index == 0) "a" else "b";
                    try object.put(alloc, "id", .{ .string = id });
                    try object.put(alloc, "status", .{ .string = if (self.table_id == 1) "old" else if (index == 0) "updated" else "new" });
                    row.* = .{ .id = if (self.table_id == 1) "row-a" else id, .version = 7, .expected_content_digest = @splat(4), .value = .{ .object = object } };
                }
                return .{ .rows = rows };
            }
        };
        cursors: [2]Cursor = undefined,
        handles: [2]catalog.Cursor = undefined,
        captures: usize = 0,
        commits: usize = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            if (std.mem.eql(u8, name.table, "usage_records")) {
                try std.testing.expectEqual(catalog.Action.read_write, action);
                return .{ .id = 1, .physical_name = "usage_records", .schema_version = 1, .columns = &.{
                    .{ .name = "id", .path = "id", .type = .string },
                    .{ .name = "status", .path = "status", .type = .string },
                } };
            }
            try std.testing.expectEqualStrings("source_records", name.table);
            try std.testing.expectEqual(catalog.Action.read, action);
            return .{ .id = 2, .physical_name = "source_records", .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
            } };
        }
        fn generate(_: *anyopaque, _: std.mem.Allocator) ![]const u8 {
            return "row-b";
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), scans.len);
            try std.testing.expectEqual(@as(u64, 1), scans[0].table.id);
            try std.testing.expectEqual(@as(u64, 2), scans[1].table.id);
            self.captures += 1;
            for (scans, &self.cursors, &self.handles) |request, *cursor, *handle| {
                cursor.* = .{ .table_id = request.table.id };
                handle.* = .{ .ptr = cursor, .next = Cursor.next, .close = closeCursor };
            }
            return .{ .ptr = self, .cursors = &self.handles, .close = close };
        }
        fn closeCursor(_: *anyopaque) void {}
        fn close(_: *anyopaque) void {}
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 1), table.id);
            try std.testing.expectEqual(@as(usize, 2), mutations.len);
            try std.testing.expectEqualStrings("row-a", mutations[0].key);
            try std.testing.expectEqual(@as(u64, 7), mutations[0].expected_version);
            try std.testing.expectEqual(@as(?[32]u8, @splat(4)), mutations[0].expected_content_digest);
            try std.testing.expectEqualStrings("a", mutations[0].row.?.object.get("id").?.string);
            try std.testing.expectEqualStrings("updated", mutations[0].row.?.object.get("status").?.string);
            try std.testing.expectEqualStrings("row-b", mutations[1].key);
            try std.testing.expectEqual(@as(u64, 0), mutations[1].expected_version);
            try std.testing.expectEqualStrings("b", mutations[1].row.?.object.get("id").?.string);
            try std.testing.expectEqualStrings("new", mutations[1].row.?.object.get("status").?.string);
            self.commits += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .atomic_statement_read_set = true, .coordinated_point_reads = true, .vtable = &.{ .generate_row_id = generate, .resolve = resolve, .open_statement = open, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    const sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0579")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    var probe: Probe = .{};
    var result = try runtime.execute(std.testing.allocator, probe.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), probe.captures);
    try std.testing.expectEqual(@as(usize, 1), probe.commits);
}

test "SQL MERGE candidate binding pins target and projects only referenced source fields" {
    const Probe = struct {
        source_resolves: usize = 0,
        point_enabled: bool = false,
        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("source", name.table);
            try std.testing.expectEqual(catalog.Action.read, action);
            self.source_resolves += 1;
            return .{ .id = 2, .physical_name = "source", .schema_version = @intCast(self.source_resolves), .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
                .{ .name = "delta", .path = "delta", .type = .integer },
                .{ .name = "cold", .path = "cold", .type = .string },
            } };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedCall;
        }
        fn open(_: *anyopaque, _: std.mem.Allocator, _: []const catalog.StatementScan) !catalog.StatementRead {
            return error.TestUnexpectedCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .coordinated_point_reads = self.point_enabled, .vtable = &.{ .resolve = resolve, .open_statement = open, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 7, .columns = &.{
        .{ .name = "n", .path = "n", .type = .integer },
        .{ .name = "cold", .path = "cold", .type = .string },
    } };
    var probe: Probe = .{};
    var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED AND s.status='go' THEN UPDATE SET n=t.n+s.delta WHEN NOT MATCHED THEN INSERT (n) VALUES (s.delta) RETURNING t.n", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const candidates = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), target, &compiled, &.{});
    try std.testing.expectEqual(@as(usize, 1), probe.source_resolves);
    try std.testing.expect(candidates.point_plan == null);
    const relation = candidates.input.relation.?;
    try std.testing.expect(relation.root.operation == .join);
    try std.testing.expectEqual(ast.JoinKind.right, relation.root.operation.join.kind);
    try std.testing.expectEqual(@as(usize, 1), relation.root.operation.join.left_keys.len);
    try std.testing.expectEqual(@as(usize, 2), relation.scans.len);
    const target_scan = relation.scans[0];
    try std.testing.expectEqual(@as(u64, 1), target_scan.table.id);
    try std.testing.expect(target_scan.request.include_primary_digest);
    const source_scan = relation.scans[1];
    try std.testing.expectEqual(@as(u64, 2), source_scan.table.id);
    for (source_scan.request.fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "cold"));
    try std.testing.expectEqual(@as(usize, 3), source_scan.request.fields.len);

    var deletion = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN DELETE", .{});
    defer deletion.deinit();
    const deletion_candidates = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), target, &deletion, &.{});
    try std.testing.expectEqual(@as(usize, 2), probe.source_resolves);
    try std.testing.expectEqual(@as(usize, 0), deletion_candidates.input.relation.?.scans[0].request.fields.len);
    try std.testing.expectEqual(@as(usize, 1), deletion_candidates.input.relation.?.scans[1].request.fields.len);
    var document_target = target;
    document_target.storage_mode = .document;
    const document_deletion = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), document_target, &deletion, &.{});
    try std.testing.expect(!document_deletion.input.relation.?.scans[0].request.include_document);
    const document_update = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), document_target, &compiled, &.{});
    try std.testing.expect(document_update.input.relation.?.scans[0].request.include_document);
    probe.point_enabled = true;
    const point_candidates = try merge_mutation.bindCandidates(arena.allocator(), probe.backend(), target, &compiled, &.{});
    try std.testing.expect(point_candidates.point_plan != null);
    try std.testing.expectEqual(@as(usize, 5), probe.source_resolves);
    try std.testing.expectEqual(point_candidates.input.relation.?.scans[1].table.schema_version, point_candidates.point_plan.?.source_input.relation.?.scans[0].table.schema_version);
}

test "SQL MERGE source-preserving candidates exclude target-only rows" {
    const Probe = struct {
        const Cursor = struct {
            table: u64,
            key: ?[]const u8 = null,
            index_key: ?[]const u8 = null,
            indexed: bool = false,
            composite: bool = false,
            index_saturated: bool = false,
            duplicate_source: bool = false,
            source_count: usize = 0,
            source_read: *usize,
            offset: usize = 0,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.source_count != 0 and self.table == 2) {
                    if (self.offset >= self.source_count) return .{ .rows = &.{} };
                    const count = @min(@as(usize, limit), self.source_count - self.offset);
                    const rows = try alloc.alloc(catalog.Row, count);
                    for (rows, 0..) |*row, local| {
                        const ordinal = self.offset + local;
                        const id = if (ordinal == 0) "a" else try std.fmt.allocPrint(alloc, "source-{d}", .{ordinal});
                        var fields: std.json.ObjectMap = .empty;
                        try fields.put(alloc, "id", .{ .string = id });
                        if (self.composite) try fields.put(alloc, "tenant", .{ .integer = 1 });
                        row.* = .{ .id = id, .version = 7, .expected_content_digest = @splat(1), .value = .{ .object = fields } };
                    }
                    self.offset += count;
                    self.source_read.* += count;
                    return .{ .rows = rows, .after = if (self.offset < self.source_count) "more" else null };
                }
                if (self.offset != 0) return .{ .rows = &.{} };
                self.offset = 1;
                const rows = try alloc.alloc(catalog.Row, if (self.table == 1) (if (self.index_key) |key| if (std.mem.eql(u8, key, "a")) (if (self.index_saturated) 17 else 1) else 0 else if (self.key) |key| @as(usize, @intFromBool(std.mem.eql(u8, key, "a"))) else 2) else 3);
                for (rows, 0..) |*row, index| {
                    const id = if (index == 0 or (self.table == 2 and self.duplicate_source and index == 1)) "a" else if (self.table == 1) "target-only" else if (index == 1) "source-only" else "null-source";
                    var fields: std.json.ObjectMap = .empty;
                    if (self.table == 1) {
                        try fields.put(alloc, "n", .{ .integer = @intCast(index + 1) });
                        if (self.indexed) try fields.put(alloc, "label", .{ .string = if (self.index_key != null and self.index_saturated) "a" else id });
                        if (self.composite) try fields.put(alloc, "tenant", .{ .integer = 1 });
                    } else {
                        try fields.put(alloc, "id", if (index == 2) .null else .{ .string = id });
                        if (self.composite) try fields.put(alloc, "tenant", .{ .integer = 1 });
                    }
                    row.* = .{ .id = id, .version = 7, .expected_content_digest = @splat(1), .value = .{ .object = fields } };
                }
                return .{ .rows = rows };
            }
        };
        cursors: [64]Cursor = undefined,
        handles: [64]catalog.Cursor = undefined,
        captures: usize = 0,
        point_scans: usize = 0,
        index_scans: usize = 0,
        commits: usize = 0,
        atomic: bool = false,
        point_enabled: bool = false,
        index_enabled: bool = true,
        indexed: bool = false,
        composite: bool = false,
        index_saturated: bool = false,
        index_not_ready: bool = false,
        duplicate_source: bool = false,
        source_count: usize = 0,
        source_read: usize = 0,
        source_read_before_fallback: usize = 0,
        source_opens: usize = 0,
        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, name.table, "target")) {
                try std.testing.expectEqual(catalog.Action.read_write, action);
                if (self.composite) return .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "label", .path = "label", .type = .string }, .{ .name = "tenant", .path = "tenant", .type = .integer } }, .indexes = &.{.{ .name = "label_tenant_idx", .columns = &.{ "label", "tenant" } }} };
                if (self.indexed) return .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "label", .path = "label", .type = .string } }, .indexes = &.{.{ .name = "label_idx", .columns = &.{"label"} }} };
                return .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
            }
            try std.testing.expectEqualStrings("source", name.table);
            try std.testing.expectEqual(catalog.Action.read, action);
            return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = if (self.composite) &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "tenant", .path = "tenant", .type = .integer } } else &.{.{ .name = "id", .path = "id", .type = .string }} };
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(scans.len > 0 and scans.len <= self.cursors.len);
            if (self.index_not_ready and scans[0].request.index_equality != null) return error.RelationalIndexNotReady;
            self.captures += 1;
            for (scans, self.cursors[0..scans.len], self.handles[0..scans.len]) |request, *cursor, *handle| {
                if (request.request.primary_key != null) self.point_scans += 1;
                if (request.request.index_equality != null) self.index_scans += 1;
                if (self.composite and request.request.index_equality != null) {
                    try std.testing.expectEqual(@as(usize, 2), request.request.index_equality.?.values.len);
                    try std.testing.expectEqual(@as(i64, 1), request.request.index_equality.?.values[1].integer);
                }
                if (self.source_count != 0 and request.table.id == 2) {
                    self.source_opens += 1;
                    if (self.source_opens == 2) self.source_read_before_fallback = self.source_read;
                }
                cursor.* = .{ .table = request.table.id, .key = request.request.primary_key, .index_key = if (request.request.index_equality) |probe| probe.values[0].string else null, .indexed = self.indexed, .composite = self.composite, .index_saturated = self.index_saturated, .duplicate_source = self.duplicate_source, .source_count = self.source_count, .source_read = &self.source_read };
                handle.* = .{ .ptr = cursor, .next = Cursor.next, .close = closeCursor };
            }
            return .{ .ptr = self, .cursors = self.handles[0..scans.len], .close = close };
        }
        fn closeCursor(_: *anyopaque) void {}
        fn close(_: *anyopaque) void {}
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.atomic) return error.TestUnexpectedCall;
            try std.testing.expectEqual(@as(u64, 1), table.id);
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqual(@as(u64, 7), mutations[0].expected_version);
            try std.testing.expectEqual(@as(i64, if (self.indexed) 2 else 4), mutations[0].row.?.object.get("n").?.integer);
            self.commits += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .atomic_statement_read_set = self.atomic, .coordinated_point_reads = self.atomic and self.point_enabled, .coordinated_index_reads = self.atomic and self.point_enabled and self.index_enabled, .vtable = &.{ .resolve = resolve, .open_statement = open, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
    var probe: Probe = .{};
    var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED AND CAST(NULL AS BOOLEAN) THEN UPDATE SET n=t.n/0 WHEN MATCHED AND s.id='never' THEN UPDATE SET n=t.n/0 WHEN MATCHED AND s.id='a' THEN UPDATE SET n=t.n+$1 WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN DO NOTHING", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const candidates = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &compiled, &.{});
    try std.testing.expectEqual(ast.ColumnType.integer, candidates.parameter_types[0].?);
    const parameters = &[_]std.json.Value{.{ .integer = 3 }};
    const context: runtime.Context = .{ .alloc = alloc, .arena = alloc, .backend = probe.backend(), .binding = candidates.input.*, .parameters = parameters, .limits = .{}, .typed_output = true };
    const output = try context.select(candidates.query);
    try std.testing.expectEqual(@as(usize, 1), probe.captures);
    try std.testing.expectEqual(@as(usize, 3), output.rows.len);
    try std.testing.expectEqualStrings("a", output.rows[0][0].string);
    try std.testing.expect(output.sql_nulls.?[1][0]);
    const source_id = for (candidates.query.columns, 0..) |projection, index| {
        if (std.mem.eql(u8, projection.field, "s\x00id")) break index;
    } else return error.TestMissingSourceIdentity;
    try std.testing.expectEqualStrings("source-only", output.rows[1][source_id].string);
    try std.testing.expect(output.sql_nulls.?[2][source_id]);
    const selection = try candidates.classifyRows(alloc, output.rows, output.sql_nulls.?, parameters);
    try std.testing.expectEqualSlices(?usize, &.{ 2, 4, 4 }, selection);
    try std.testing.expectError(error.SqlMutationCardinalityViolation, candidates.classifyRows(alloc, &.{ output.rows[0], output.rows[0] }, &.{ output.sql_nulls.?[0], output.sql_nulls.?[0] }, parameters));
    const mutations = try candidates.prepareMutations(alloc, probe.backend(), output.rows, output.sql_nulls.?, parameters, 3, 1024);
    try std.testing.expectEqual(@as(usize, 1), mutations.len);
    try std.testing.expectEqualStrings("a", mutations[0].key);
    try std.testing.expectEqual(@as(u64, 7), mutations[0].expected_version);
    try std.testing.expectEqual(@as(i64, 4), mutations[0].row.?.object.get("n").?.integer);
    try std.testing.expectError(error.SqlProgramLimitExceeded, candidates.prepareMutations(alloc, probe.backend(), output.rows, output.sql_nulls.?, parameters, 3, 1));
    for (output.rows, output.sql_nulls.?, 0..) |row, nulls, row_index| {
        const cells = try alloc.alloc(@import("scalar.zig").Datum, row.len);
        for (row, nulls, cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
        const selected = (try candidates.selectArm(alloc, cells, parameters)).?;
        try std.testing.expectEqual(if (row_index == 0) @as(usize, 2) else @as(usize, 4), selected);
        const values = try candidates.evaluateValues(alloc, selected, cells, parameters);
        if (row_index == 0) try std.testing.expectEqual(@as(i64, 4), values[0].value.integer) else try std.testing.expectEqual(@as(usize, 0), values.len);
    }
    var deletion = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN DELETE", .{});
    defer deletion.deinit();
    const delete_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &deletion, &.{});
    const delete_context: runtime.Context = .{ .alloc = alloc, .arena = alloc, .backend = probe.backend(), .binding = delete_plan.input.*, .parameters = &.{}, .limits = .{}, .typed_output = true };
    const delete_rows = try delete_context.select(delete_plan.query);
    const deleted = try delete_plan.prepareMutations(alloc, probe.backend(), delete_rows.rows, delete_rows.sql_nulls.?, &.{}, 3, 1024);
    try std.testing.expectEqual(@as(usize, 1), deleted.len);
    try std.testing.expectEqualStrings("a", deleted[0].key);
    try std.testing.expectEqual(@as(u64, 7), deleted[0].expected_version);
    try std.testing.expect(deleted[0].row == null);
    const before = probe.captures;
    try std.testing.expectError(error.SqlRangeTrackingRequired, runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{}));
    try std.testing.expectEqual(before, probe.captures);
    try std.testing.expectEqual(@as(usize, 0), probe.commits);
    probe.atomic = true;
    var merged = try runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{});
    defer merged.deinit();
    try std.testing.expectEqual(@as(u64, 1), merged.output.rows_affected);
    try std.testing.expectEqualStrings("MERGE", merged.output.command_tag);
    try std.testing.expectEqual(@as(usize, 1), probe.commits);
    const captures_before_points = probe.captures;
    probe.point_enabled = true;
    const bounded_points = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &compiled, &.{});
    try std.testing.expectEqual(@as(i64, 129), bounded_points.point_plan.?.source_query.limit.?.integer);
    try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), probe.point_scans);
    try std.testing.expectEqual(@as(usize, 1), probe.commits);
    var pointed = try runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{});
    defer pointed.deinit();
    try std.testing.expectEqual(@as(u64, 1), pointed.output.rows_affected);
    try std.testing.expectEqual(captures_before_points + 3, probe.captures);
    try std.testing.expectEqual(@as(usize, 2), probe.point_scans);
    try std.testing.expectEqual(@as(usize, 2), probe.commits);
    probe.duplicate_source = true;
    const scans_before_duplicate = probe.point_scans;
    try std.testing.expectError(error.SqlMutationCardinalityViolation, runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{}));
    try std.testing.expectEqual(scans_before_duplicate + 1, probe.point_scans);
    try std.testing.expectEqual(@as(usize, 2), probe.commits);
    probe.duplicate_source = false;
    for ([_]usize{ 128, 129, 130 }, 0..) |source_count, index| {
        probe.source_count = source_count;
        probe.source_read = 0;
        probe.source_read_before_fallback = 0;
        probe.source_opens = 0;
        const points_before = probe.point_scans;
        var boundary_merge = try runtime.execute(std.testing.allocator, probe.backend(), &compiled, parameters, .{ .mutation_rows = 256 });
        defer boundary_merge.deinit();
        try std.testing.expectEqual(@as(u64, 1), boundary_merge.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 3) + index, probe.commits);
        if (source_count == 128) {
            try std.testing.expectEqual(@as(usize, 1), probe.source_opens);
            try std.testing.expectEqual(@as(usize, 128), probe.source_read);
            try std.testing.expectEqual(points_before + 128, probe.point_scans);
        } else {
            try std.testing.expectEqual(@as(usize, 2), probe.source_opens);
            try std.testing.expectEqual(@as(usize, 129), probe.source_read_before_fallback);
            try std.testing.expectEqual(@as(usize, 129) + source_count, probe.source_read);
            try std.testing.expectEqual(points_before, probe.point_scans);
        }
    }
    probe.source_count = 0;
    probe.point_enabled = false;
    var returning = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET n=t.n+1 RETURNING t.n", .{});
    defer returning.deinit();
    try std.testing.expectError(error.UnsupportedSqlExecution, runtime.execute(std.testing.allocator, probe.backend(), &returning, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 5), probe.commits);
    if (try std.testing.environ.contains(std.testing.allocator, "ANTFLY_SQL_MERGE_CLASSIFY_BENCHMARK")) {
        var benchmark_sql = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED AND lower(s.id)='a' THEN DO NOTHING", .{});
        defer benchmark_sql.deinit();
        const benchmark_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &benchmark_sql, &.{});
        const benchmark_context: runtime.Context = .{ .alloc = alloc, .arena = alloc, .backend = probe.backend(), .binding = benchmark_plan.input.*, .parameters = &.{}, .limits = .{}, .typed_output = true };
        const benchmark_capture = try benchmark_context.select(benchmark_plan.query);
        const count = 10_000;
        const rows = try alloc.alloc([]const std.json.Value, count);
        const nulls = try alloc.alloc([]const bool, count);
        @memset(rows, benchmark_capture.rows[0]);
        @memset(nulls, benchmark_capture.sql_nulls.?[0]);

        var reused = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer reused.deinit();
        const reuse_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        const selections = try benchmark_plan.classifyRows(reused.allocator(), rows, nulls, &.{});
        const reuse_elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - reuse_start;
        for (selections) |selected_arm| try std.testing.expectEqual(@as(?usize, 0), selected_arm);

        var per_row = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer per_row.deinit();
        const old_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        const old_selections = try per_row.allocator().alloc(?usize, count);
        const cells = try per_row.allocator().alloc(@import("scalar.zig").Datum, rows[0].len);
        for (rows, nulls, old_selections) |row, flags, *selected_arm| {
            for (row, flags, cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
            var scratch = std.heap.ArenaAllocator.init(per_row.allocator());
            defer scratch.deinit();
            selected_arm.* = try benchmark_plan.selectArm(scratch.allocator(), cells, &.{});
        }
        const old_elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - old_start;
        for (old_selections) |selected_arm| try std.testing.expectEqual(@as(?usize, 0), selected_arm);
        std.debug.print("MERGE classify rows={d} reused_ns={d} per_row_ns={d} reused_bytes={d} per_row_bytes={d}\n", .{ count, reuse_elapsed, old_elapsed, reused.queryCapacity(), per_row.queryCapacity() });
    }
    probe.indexed = true;
    probe.point_enabled = true;
    var indexed_sql = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t.label=s.id WHEN MATCHED THEN UPDATE SET n=t.n+1 WHEN NOT MATCHED THEN DO NOTHING", .{});
    defer indexed_sql.deinit();
    const indexed_target = try Probe.resolve(&probe, alloc, .{ .table = "target" }, .read_write);
    probe.index_enabled = false;
    const staged_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), indexed_target, &indexed_sql, &.{});
    try std.testing.expect(staged_plan.point_plan == null);
    probe.index_enabled = true;
    const indexed_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), indexed_target, &indexed_sql, &.{});
    try std.testing.expectEqualStrings("label_idx", indexed_plan.point_plan.?.index_name.?);
    try std.testing.expectEqual(@as(i64, 33), indexed_plan.point_plan.?.source_query.limit.?.integer);
    var indexed_result = try runtime.execute(std.testing.allocator, probe.backend(), &indexed_sql, &.{}, .{});
    defer indexed_result.deinit();
    try std.testing.expectEqual(@as(u64, 1), indexed_result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 2), probe.index_scans);
    probe.duplicate_source = true;
    const indexed_commits = probe.commits;
    try std.testing.expectError(error.SqlMutationCardinalityViolation, runtime.execute(std.testing.allocator, probe.backend(), &indexed_sql, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 3), probe.index_scans);
    try std.testing.expectEqual(indexed_commits, probe.commits);
    probe.duplicate_source = false;
    probe.index_saturated = true;
    probe.source_count = 3;
    probe.source_opens = 0;
    var saturated = try runtime.execute(std.testing.allocator, probe.backend(), &indexed_sql, &.{}, .{});
    defer saturated.deinit();
    try std.testing.expectEqual(@as(u64, 1), saturated.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 2), probe.source_opens);
    try std.testing.expectEqual(indexed_commits + 1, probe.commits);
    probe.index_saturated = false;
    probe.index_not_ready = true;
    probe.source_opens = 0;
    var not_ready = try runtime.execute(std.testing.allocator, probe.backend(), &indexed_sql, &.{}, .{});
    defer not_ready.deinit();
    try std.testing.expectEqual(@as(u64, 1), not_ready.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 2), probe.source_opens);
    try std.testing.expectEqual(indexed_commits + 2, probe.commits);
    probe.index_not_ready = false;
    probe.source_count = 33;
    probe.source_opens = 0;
    const scans_before_crossover = probe.index_scans;
    var above_crossover = try runtime.execute(std.testing.allocator, probe.backend(), &indexed_sql, &.{}, .{});
    defer above_crossover.deinit();
    try std.testing.expectEqual(@as(u64, 1), above_crossover.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 2), probe.source_opens);
    try std.testing.expectEqual(scans_before_crossover, probe.index_scans);
    try std.testing.expectEqual(indexed_commits + 3, probe.commits);
    probe.source_count = 0;
    probe.composite = true;
    var composite_sql = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t.tenant=s.tenant AND t.label=s.id WHEN MATCHED THEN UPDATE SET n=t.n+1 WHEN NOT MATCHED THEN DO NOTHING", .{});
    defer composite_sql.deinit();
    const composite_target = try Probe.resolve(&probe, alloc, .{ .table = "target" }, .read_write);
    var incomplete_sql = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t.label=s.id WHEN MATCHED THEN UPDATE SET n=t.n+1", .{});
    defer incomplete_sql.deinit();
    const incomplete_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), composite_target, &incomplete_sql, &.{});
    try std.testing.expect(incomplete_plan.point_plan == null);
    const composite_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), composite_target, &composite_sql, &.{});
    try std.testing.expectEqualStrings("label_tenant_idx", composite_plan.point_plan.?.index_name.?);
    try std.testing.expectEqual(@as(usize, 2), composite_plan.point_plan.?.lookup_ordinals.len);
    const scans_before_composite = probe.index_scans;
    var composite_result = try runtime.execute(std.testing.allocator, probe.backend(), &composite_sql, &.{}, .{});
    defer composite_result.deinit();
    try std.testing.expectEqual(@as(u64, 1), composite_result.output.rows_affected);
    try std.testing.expectEqual(scans_before_composite + 2, probe.index_scans);
    probe.duplicate_source = true;
    const commits_before_duplicate_composite = probe.commits;
    const scans_before_duplicate_composite = probe.index_scans;
    try std.testing.expectError(error.SqlMutationCardinalityViolation, runtime.execute(std.testing.allocator, probe.backend(), &composite_sql, &.{}, .{}));
    try std.testing.expectEqual(scans_before_duplicate_composite + 1, probe.index_scans);
    try std.testing.expectEqual(commits_before_duplicate_composite, probe.commits);
}

test "SQL MERGE prepares generated insert identity and document postimage before commit" {
    const Probe = struct {
        generated: usize = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            try std.testing.expectEqualStrings("source", name.table);
            try std.testing.expectEqual(catalog.Action.read, action);
            return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "delta", .path = "delta", .type = .integer },
            } };
        }
        fn generate(ptr: *anyopaque, _: std.mem.Allocator) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.generated += 1;
            return "generated-key";
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .generate_row_id = generate, .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    const Cell = struct {
        fn put(candidates: merge_mutation.Candidates, row: []std.json.Value, flags: []bool, name: []const u8, value: std.json.Value) !void {
            for (candidates.query.columns, 0..) |projection, index| if (std.mem.eql(u8, projection.field, name)) {
                row[index] = value;
                flags[index] = false;
                return;
            };
            return error.TestMissingCandidateColumn;
        }
    };
    var probe: Probe = .{};
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .integer },
        .{ .name = "payload", .path = "payload", .type = .json },
    } };
    var insert = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN NOT MATCHED THEN INSERT (n) VALUES (s.delta)", .{});
    defer insert.deinit();
    const insert_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &insert, &.{});
    const insert_row = try alloc.alloc(std.json.Value, insert_plan.query.columns.len);
    const insert_flags = try alloc.alloc(bool, insert_row.len);
    @memset(insert_row, .null);
    @memset(insert_flags, true);
    try Cell.put(insert_plan, insert_row, insert_flags, "s\x00id", .{ .string = "s1" });
    try Cell.put(insert_plan, insert_row, insert_flags, "s\x00delta", .{ .integer = 5 });
    const inserted = try insert_plan.prepareMutations(alloc, probe.backend(), &.{insert_row}, &.{insert_flags}, &.{}, 1, 4096);
    try std.testing.expectEqual(@as(usize, 1), inserted.len);
    try std.testing.expectEqual(@as(usize, 1), probe.generated);
    try std.testing.expectEqualStrings("generated-key", inserted[0].key);
    try std.testing.expectEqual(@as(u64, 0), inserted[0].expected_version);
    try std.testing.expectEqual(@as(i64, 5), inserted[0].row.?.object.get("n").?.integer);
    try std.testing.expectError(error.DuplicateSqlRow, insert_plan.prepareMutations(alloc, probe.backend(), &.{ insert_row, insert_row }, &.{ insert_flags, insert_flags }, &.{}, 2, 4096));
    var default_insert = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN NOT MATCHED THEN INSERT (n) VALUES (DEFAULT)", .{});
    defer default_insert.deinit();
    const default_insert_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), target, &default_insert, &.{});
    const default_insert_row = try alloc.alloc(std.json.Value, default_insert_plan.query.columns.len);
    const default_insert_flags = try alloc.alloc(bool, default_insert_row.len);
    @memset(default_insert_row, .null);
    @memset(default_insert_flags, true);
    try Cell.put(default_insert_plan, default_insert_row, default_insert_flags, "s\x00id", .{ .string = "s1" });
    const default_inserted = try default_insert_plan.prepareMutations(alloc, probe.backend(), &.{default_insert_row}, &.{default_insert_flags}, &.{}, 1, 4096);
    try std.testing.expectEqual(@as(usize, 1), default_inserted.len);
    try std.testing.expectEqual(@as(usize, 0), default_inserted[0].row.?.object.count());

    var document_target = target;
    document_target.storage_mode = .document;
    var update = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET n=s.delta", .{});
    defer update.deinit();
    const update_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), document_target, &update, &.{});
    const update_row = try alloc.alloc(std.json.Value, update_plan.query.columns.len);
    const update_flags = try alloc.alloc(bool, update_row.len);
    @memset(update_row, .null);
    @memset(update_flags, true);
    try Cell.put(update_plan, update_row, update_flags, "t\x00_id", .{ .string = "a" });
    try Cell.put(update_plan, update_row, update_flags, "s\x00id", .{ .string = "a" });
    try Cell.put(update_plan, update_row, update_flags, "s\x00delta", .{ .integer = 5 });
    try Cell.put(update_plan, update_row, update_flags, "t\x00\x00mutation_version", .{ .string = "7" });
    const digest = std.fmt.bytesToHex([_]u8{1} ** 32, .lower);
    try Cell.put(update_plan, update_row, update_flags, "t\x00\x00mutation_digest", .{ .string = &digest });
    var previous: std.json.ObjectMap = .empty;
    try previous.put(alloc, "n", .{ .integer = 1 });
    try previous.put(alloc, "payload", .null);
    try previous.put(alloc, "unknown", .{ .integer = 42 });
    try Cell.put(update_plan, update_row, update_flags, "t\x00\x00mutation_document", .{ .object = previous });
    const updated = try update_plan.prepareMutations(alloc, probe.backend(), &.{update_row}, &.{update_flags}, &.{}, 1, 4096);
    try std.testing.expectEqual(@as(usize, 1), updated.len);
    try std.testing.expectEqual(@as(u64, 7), updated[0].expected_version);
    try std.testing.expectEqual(@as(i64, 5), updated[0].row.?.object.get("n").?.integer);
    try std.testing.expectEqual(@as(i64, 42), updated[0].row.?.object.get("unknown").?.integer);
    try std.testing.expect(updated[0].row.?.object.get("payload").? == .null);
    try std.testing.expectEqualStrings("payload", updated[0].json_null_fields[0]);
    var default_update = try compiler.compile(std.testing.allocator, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET n=DEFAULT", .{});
    defer default_update.deinit();
    const default_update_plan = try merge_mutation.bindCandidates(alloc, probe.backend(), document_target, &default_update, &.{});
    const default_update_row = try alloc.alloc(std.json.Value, default_update_plan.query.columns.len);
    const default_update_flags = try alloc.alloc(bool, default_update_row.len);
    @memset(default_update_row, .null);
    @memset(default_update_flags, true);
    try Cell.put(default_update_plan, default_update_row, default_update_flags, "t\x00_id", .{ .string = "a" });
    try Cell.put(default_update_plan, default_update_row, default_update_flags, "s\x00id", .{ .string = "a" });
    try Cell.put(default_update_plan, default_update_row, default_update_flags, "t\x00\x00mutation_version", .{ .string = "7" });
    try Cell.put(default_update_plan, default_update_row, default_update_flags, "t\x00\x00mutation_digest", .{ .string = &digest });
    try Cell.put(default_update_plan, default_update_row, default_update_flags, "t\x00\x00mutation_document", .{ .object = previous });
    const default_updated = try default_update_plan.prepareMutations(alloc, probe.backend(), &.{default_update_row}, &.{default_update_flags}, &.{}, 1, 4096);
    try std.testing.expectEqual(@as(usize, 1), default_updated.len);
    try std.testing.expect(default_updated[0].row.?.object.get("n") == null);
    try std.testing.expectEqual(@as(i64, 42), default_updated[0].row.?.object.get("unknown").?.integer);
}
