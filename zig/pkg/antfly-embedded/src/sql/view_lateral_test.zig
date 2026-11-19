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

const std = @import("std");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");

const Empty = struct {
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return error.TableNotFound;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.TableNotFound;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.Forbidden;
    }
    fn checkpoint(_: *anyopaque) !void {}
};

test "SQL JSON record projection releases every failed allocation" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
            var compiled = try compiler.compile(a, "SELECT r.tags,r.n FROM jsonb_to_record('{\"tags\":[\"x\",null,\"y\"],\"n\":\"9007199254740993.25\"}') AS r(tags TEXT[],n NUMERIC)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend, &compiled, &.{}, .{ .native_result = true });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rowCount());
            const row = result.output.native_rows.?[0];
            try std.testing.expect(row[0].array != null and row[1].numeric != null);
            try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "SQL materialized record streams retire only owned scopes on close failure and allocation faults" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            for ([_]bool{ false, true }) |early_close| {
                var marker: u8 = 0;
                const backend: catalog.Backend = .{ .ptr = &marker, .execution_io = std.testing.io, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
                var compiled = try compiler.compile(a,
                    \\WITH records AS MATERIALIZED (
                    \\ SELECT r.n FROM (VALUES('{"n":1}'::jsonb),('{"n":2}'::jsonb)) d(body),
                    \\ jsonb_to_record(d.body) AS r(n BIGINT))
                    \\ SELECT a.n,b.n FROM records a LEFT JOIN LATERAL(
                    \\ SELECT n FROM records b WHERE b.n=a.n LIMIT 1) b ON true
                , .{});
                defer compiled.deinit();
                const stream = (try @import("read_stream.zig").Stream.open(a, backend, &compiled, &.{}, .{})).?;
                defer stream.close();
                var seen: usize = 0;
                while (true) {
                    var page = try stream.next(1);
                    defer page.deinit();
                    seen += page.output.rowCount();
                    if (page.exhausted or early_close) break;
                }
                try std.testing.expectEqual(@as(usize, if (early_close) 1 else 2), seen);
            }
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .execution_io = std.testing.io, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
            var compiled = try compiler.compile(a, "SELECT 1 FROM jsonb_to_record('{\"n\":\"bad\"}') AS r(n BIGINT)", .{});
            defer compiled.deinit();
            const stream = (try @import("read_stream.zig").Stream.open(a, backend, &compiled, &.{}, .{})).?;
            defer stream.close();
            var page = stream.next(1) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqualStrings("22P02", @import("errors.zig").describe(err).code);
                return;
            };
            defer page.deinit();
            return error.ExpectedRecordConversionFailure;
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "SQL stored producer hash domains retain linear cardinality and bounded working memory" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const backend: catalog.Backend = .{ .ptr = &marker, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
    var prefix: std.Io.Writer.Allocating = .init(a);
    defer prefix.deinit();
    try prefix.writer.writeAll("WITH keys AS(SELECT n::BIGINT AS id FROM UNNEST(ARRAY[");
    const count = 2048;
    for (0..count) |i| {
        if (i != 0) try prefix.writer.writeByte(',');
        try prefix.writer.print("{d}", .{i});
    }
    try prefix.writer.writeAll("]) a(n)) SELECT COUNT(*) FROM keys l JOIN keys r ON ");
    for ([_][]const u8{ "l.id=r.id", "l.id::NUMERIC=r.id" }) |predicate| {
        const sql = try std.mem.concat(a, u8, &.{ prefix.written(), predicate });
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const started = @import("antfly_platform").time.monotonicNs();
        var result = try runtime.execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 8 * 1024 * 1024, .scan_rows = count * 4 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("2048", result.output.rows[0][0].string);
        std.debug.print("SQL producer hash domain={s} rows={d} elapsed_ns={d} peak_bytes={d}\n", .{ predicate, count, @import("antfly_platform").time.monotonicNs() -| started, result.state.budget.footprint() });
    }
}

test "SQL repeated stored record producers isolate correlated materializations" {
    const a = std.testing.allocator;
    var body = try compiler.compile(a,
        \\SELECT t.id,r.plan,(r.metrics->>'score')::numeric AS score
        \\ FROM (VALUES ('a','{"plan":"pro","metrics":{"score":60}}'::jsonb),
        \\ ('b','{"plan":"pro","metrics":{"score":20}}'::jsonb),
        \\ ('f','{"plan":"pro","metrics":{}}'::jsonb)) t(id,body),
        \\ jsonb_to_record(t.body) AS r(plan TEXT,metrics JSONB)
    , .{});
    defer body.deinit();
    const Fixture = struct {
        query: *const @import("ast.zig").Select,
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .execution_io = std.testing.io, .pinned_statement_snapshot = true, .binding_scope = @intFromPtr(self), .vtable = &.{ .resolve_read = read, .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
        }
        fn read(raw: *anyopaque, _: std.mem.Allocator, _: @import("ast.zig").Name) !catalog.ReadRelation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .view = .{ .id = 7, .generation = 1, .query = self.query, .backend = self.backend(), .security_barrier = false, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string, .element_type = .text },
                .{ .name = "plan", .path = "plan", .type = .string, .element_type = .text },
                .{ .name = "score", .path = "score", .type = .number, .element_type = .numeric },
            } } };
        }
    };
    var fixture: Fixture = .{ .query = &body.statement.select };
    var compiled = try compiler.compile(a, "SELECT d.id,latest.score FROM support_view d JOIN LATERAL(SELECT score FROM support_view s WHERE s.plan=d.plan AND s.score>0 ORDER BY s.score DESC LIMIT 1) latest ON true WHERE d.plan='pro' LIMIT 10", .{});
    defer compiled.deinit();
    const stream = (try @import("read_stream.zig").Stream.open(a, fixture.backend(), &compiled, &.{}, .{})).?;
    defer stream.close();
    var seen: usize = 0;
    while (true) {
        var page = try stream.next(1);
        defer page.deinit();
        for (page.output.rows) |row| try std.testing.expectEqualStrings("60", row[1].string);
        seen += page.output.rowCount();
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "SQL document view lateral numeric equality survives ordered LIMIT branches" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const backend: catalog.Backend = .{ .ptr = &marker, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = Empty.resolve, .scan = Empty.scan, .mutate = Empty.mutate, .checkpoint = Empty.checkpoint } };
    const prefix =
        \\WITH support_view AS (
        \\ SELECT t.id,r.plan,(r.metrics->>'score')::numeric AS score
        \\ FROM (VALUES ('a','{"plan":"pro","metrics":{"score":60}}'::jsonb),
        \\ ('b','{"plan":"pro","metrics":{"score":20}}'::jsonb),
        \\ ('f','{"plan":"pro","metrics":{}}'::jsonb)) t(id,body),
        \\ jsonb_to_record(t.body) AS r(plan TEXT,metrics JSONB))
    ;
    const Case = struct { sql: []const u8, rows: usize };
    for ([_]Case{
        .{ .sql = "SELECT id,score FROM support_view WHERE score=60", .rows = 1 },
        .{ .sql = "SELECT d.id,latest.score FROM support_view d JOIN LATERAL(SELECT score FROM support_view s WHERE s.plan=d.plan AND s.score>0 ORDER BY s.score DESC LIMIT 1) latest ON true WHERE d.plan='pro' LIMIT 10", .rows = 3 },
        .{ .sql = "SELECT d.id,latest.score FROM support_view d LEFT JOIN LATERAL(SELECT score FROM support_view s WHERE s.plan=d.plan AND s.score=60 ORDER BY s.score DESC LIMIT 1) latest ON true WHERE d.plan='pro' LIMIT 10", .rows = 3 },
        .{ .sql = "SELECT d.id,latest.score FROM support_view d LEFT JOIN LATERAL(SELECT score FROM support_view s WHERE s.plan=d.plan AND s.score=60 LIMIT 1) latest ON true WHERE d.plan='pro' LIMIT 10", .rows = 3 },
    }) |case| {
        const suffix = case.sql;
        const sql = try std.mem.concat(a, u8, &.{ prefix, suffix });
        defer a.free(sql);
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(a, backend, &compiled, &.{}, .{});
        defer result.deinit();
        if (result.output.rows.len != case.rows or (result.output.rows.len != 0 and result.output.rows[0][1] != .string))
            std.debug.print("lateral numeric mismatch sql={s} rows={any}\n", .{ suffix, result.output.rows });
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        for (result.output.rows) |row| {
            try std.testing.expect(row[1] == .string);
            try std.testing.expectEqualStrings("60", row[1].string);
        }
        const stream = (try @import("read_stream.zig").Stream.open(a, backend, &compiled, &.{}, .{})).?;
        defer stream.close();
        var seen: usize = 0;
        while (true) {
            var page = try stream.next(1);
            defer page.deinit();
            for (page.output.rows) |row| try std.testing.expectEqualStrings("60", row[1].string);
            seen += page.output.rowCount();
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(case.rows, seen);
    }
}
