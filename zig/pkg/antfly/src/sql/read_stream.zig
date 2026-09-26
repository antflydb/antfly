// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Pull execution for order-preserving scan/filter/projection plans. Binding
//! and parameters live once per cursor; native/evaluation/result pages share
//! one memory budget and are reclaimed before the next pull. Blocking plans
//! explicitly decline this path, never pretend that LIMIT is a continuation.
const std = @import("std");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const describe = @import("describe.zig");
const Budget = @import("memory_budget.zig");
const Json = std.json.Value;

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    output: runtime.Output,
    exhausted: bool,

    /// Pages must be released before closing their stream, whose shared memory
    /// admission owns their backing allocator.
    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const Fixture = struct {
    offset: usize = 0,
    count: usize = 10000,
    opened: usize = 0,
    closed: usize = 0,
    calls: usize = 0,
    cancel: bool = false,
    fn backend(self: *Fixture) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedStatelessScan;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedMutation;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        if (self.cancel) return error.QueryCanceled;
    }
    fn openScan(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.opened += 1;
        return .{ .ptr = self, .next = next, .close = close };
    }
    fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.calls += 1;
        const count = @min(limit, self.count - self.offset);
        const rows = try alloc.alloc(catalog.Row, count);
        for (rows, 0..) |*row, i| {
            var object: std.json.ObjectMap = .empty;
            try object.put(alloc, "n", .{ .integer = @intCast(self.offset + i) });
            row.* = .{ .id = "id", .version = 1, .value = .{ .object = object } };
        }
        self.offset += count;
        return .{ .rows = rows, .after = if (self.offset < self.count) try std.fmt.allocPrint(alloc, "{d}", .{self.offset}) else null };
    }
    fn close(raw: *anyopaque) void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.closed += 1;
    }
};

test "SQL pull stream releases pages and streams beyond materialized result limit" {
    var compiled = try compiler.compile(std.testing.allocator, "SELECT n + 1 AS value FROM docs", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const stream = (try Stream.open(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .result_rows = 2, .page_rows = 128, .retained_bytes = 256 * 1024 })).?;
    defer stream.close();
    var seen: usize = 0;
    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var first_page_ns: i96 = 0;
    while (true) {
        var page = try stream.next(73);
        defer page.deinit();
        if (seen == 0) {
            first_page_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
            try std.testing.expectEqual(@as(usize, 73), fixture.offset);
        }
        for (page.output.rows) |row| {
            seen += 1;
            try std.testing.expectEqual(@as(i64, @intCast(seen)), row[0].integer);
        }
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 10000), seen);
    try std.testing.expectEqual(@as(usize, 1), fixture.opened);
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
    try std.testing.expect(stream.budget.peak < 256 * 1024);
    std.debug.print("SQL pull stream: rows={d} peak_bytes={d} first_page_ns={d} elapsed_ns={d}\n", .{ seen, stream.budget.peak, first_page_ns, std.Io.Clock.awake.now(std.testing.io).nanoseconds - started });
}

test "SQL pull stream keeps one pinned policy setting across pages" {
    const settings = @import("setting_catalog.zig");
    const Owner = struct {
        value: []const u8 = "tenant-a",
        definition: settings.Definition = .{ .identity = .{ .id = 10, .generation = 1 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "tenant-a" } },
        fn load(ptr: *anyopaque, _: std.mem.Allocator, scope: settings.Scope) !settings.RawSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.definition.default = .{ .string = self.value };
            return .{ .scope = scope, .epoch = 2, .definitions = @as([*]const settings.Definition, @ptrCast(&self.definition))[0..1] };
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT current_setting('app.tenant') AS tenant FROM docs LIMIT 2", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .count = 2 };
    var owner: Owner = .{};
    var backend = fixture.backend();
    backend.setting_capture = .{ .owner = .{ .ptr = &owner, .load = Owner.load }, .scope = .{ .principal = "alice", .database = "main" } };
    const stream = (try Stream.open(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 1 })).?;
    defer stream.close();
    var first = try stream.next(1);
    defer first.deinit();
    try std.testing.expectEqualStrings("tenant-a", first.output.rows[0][0].string);
    owner.value = "tenant-b";
    var second = try stream.next(1);
    defer second.deinit();
    try std.testing.expectEqualStrings("tenant-a", second.output.rows[0][0].string);
}

test "SQL pull stream keeps offset and residual state across pulls and closes on cancellation" {
    var compiled = try compiler.compile(std.testing.allocator, "SELECT n FROM docs WHERE n % 2 = 0 LIMIT 9 OFFSET 3", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const stream = (try Stream.open(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{})).?;
    defer stream.close();
    {
        var page = try stream.next(2);
        defer page.deinit();
        try std.testing.expectEqual(@as(i64, 6), page.output.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 8), page.output.rows[1][0].integer);
    }
    fixture.cancel = true;
    try std.testing.expectError(error.QueryCanceled, stream.next(2));
    try std.testing.expectError(error.SqlStreamFailed, stream.next(2));
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
}

test "SQL pull stream pipelines aliased nested CTEs without eager source materialization" {
    var compiled = try compiler.compile(std.testing.allocator, "WITH q AS (SELECT n + 2 AS x FROM docs) SELECT q.x FROM q WHERE q.x > 5 LIMIT 9", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const stream = (try Stream.open(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .result_rows = 2, .page_rows = 32, .retained_bytes = 256 * 1024 })).?;
    defer stream.close();
    var seen: usize = 0;
    while (true) {
        var page = try stream.next(2);
        defer page.deinit();
        for (page.output.rows) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(seen + 6)), row[0].integer);
            seen += 1;
        }
        try std.testing.expect(fixture.offset < 100);
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 9), seen);
}

test "SQL pull stream quotas fail rather than silently truncate and blocking shapes decline" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT n FROM docs", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .scan_rows = 3 })).?;
    defer stream.close();
    try std.testing.expectError(error.SqlProgramLimitExceeded, stream.next(5));
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
    var blocking = try compiler.compile(std.testing.allocator, "SELECT n FROM docs ORDER BY n DESC", .{});
    defer blocking.deinit();
    try std.testing.expectEqual(null, try Stream.open(std.testing.allocator, fixture.backend(), &blocking, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), fixture.opened);
}

fn allocationScenario(alloc: std.mem.Allocator) !void {
    var compiled = try compiler.compile(alloc, "SELECT q.x FROM (SELECT n + 1 AS x FROM docs) q LIMIT 5", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const stream = (try Stream.open(alloc, fixture.backend(), &compiled, &.{}, .{})).?;
    defer stream.close();
    var page = try stream.next(5);
    defer page.deinit();
}

test "SQL pull stream unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
}

pub const Stream = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    settings: ?*@import("setting_catalog.zig").View = null,
    context: runtime.Context,
    cursor: ?catalog.Cursor = null,
    fields: []const []const u8,
    after: ?[]u8 = null,
    skip: usize,
    remaining: usize,
    visited: usize = 0,
    pages: usize = 0,
    emitted: usize = 0,
    exhausted: bool = false,
    failed: bool = false,

    /// Caller retains the compiled plan and backend until close. A null result
    /// means a blocking plan, which may use the bounded materializing executor.
    /// No row reads or mutations occur on that decline path.
    pub fn open(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: runtime.Limits) !?*Stream {
        if (compiled.statement != .select) return null;
        if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
        if (limits.page_rows == 0 or limits.page_rows > 4096 or limits.scan_rows == 0 or limits.scan_pages == 0) return error.InvalidSqlLimit;
        try backend.vtable.checkpoint(backend.ptr);
        const self = try alloc.create(Stream);
        errdefer alloc.destroy(self);
        self.budget = .{ .backing = alloc, .limit = limits.retained_bytes };
        self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer self.arena.deinit();
        self.settings = null;
        errdefer if (self.settings) |view| view.deinit();
        const arena = self.arena.allocator();
        var statement_backend = backend;
        if (backend.setting_capture) |capture| {
            const view = try arena.create(@import("setting_catalog.zig").View);
            view.* = try @import("setting_catalog.zig").View.capture(self.budget.allocator(), capture.owner, capture.scope, capture.overlay);
            self.settings = view;
            statement_backend.settings_view = view;
        }
        const binding = try describe.bind(arena, statement_backend, compiled, &.{});
        const statement = if (binding.relation) |relation| relation.statement else compiled.statement.select;
        if (binding.aggregate != null or binding.window != null or binding.table == null or
            statement.count_all or (binding.order_keys.len != 0 and !binding.primary_order))
        {
            if (self.settings) |view| view.deinit();
            self.arena.deinit();
            alloc.destroy(self);
            return null;
        }
        self.context = .{ .alloc = self.budget.allocator(), .arena = arena, .backend = statement_backend, .binding = binding, .parameters = &.{}, .limits = limits, .typed_output = true };
        const params = try arena.alloc(Json, parameters.len);
        for (parameters, params) |value, *out| out.* = try self.context.outputValue(value);
        self.context.parameters = params;
        const table = binding.table.?;
        const predicates = try self.context.conditions(table, statement.predicate);
        var fields: std.ArrayList([]const u8) = .empty;
        if (statement.columns.len == 0) {
            for (table.columns) |column| try fields.append(arena, column.path);
        } else {
            for (statement.columns) |projection| try fields.append(arena, if (projection.expression != null) "" else (try table.column(projection.field)).path);
        }
        var needed: std.ArrayList([]const u8) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (fields.items) |field| {
            if (field.len == 0 or std.mem.eql(u8, field, "_id")) continue;
            const slot = try seen.getOrPut(arena, field);
            if (!slot.found_existing) try needed.append(arena, field);
        }
        for (binding.scalars.required) |ordinal| {
            const field = binding.scalars.columns[ordinal].name;
            if (std.mem.eql(u8, field, "_id")) continue;
            const slot = try seen.getOrPut(arena, field);
            if (!slot.found_existing) try needed.append(arena, field);
        }
        self.fields = fields.items;
        self.skip = try self.context.count(statement.offset, 0);
        self.remaining = try self.context.count(statement.limit, std.math.maxInt(usize));
        if (self.skip > limits.scan_rows or (statement.limit != null and self.remaining > limits.scan_rows)) return error.SqlProgramLimitExceeded;
        self.after = null;
        self.visited = 0;
        self.pages = 0;
        self.emitted = 0;
        self.failed = false;
        self.exhausted = predicates.empty or self.remaining == 0;
        self.cursor = null;
        if (!self.exhausted) {
            if (binding.relation != null) {
                self.cursor = try @import("relation_runtime.zig").openCursor(self.context);
            } else {
                const open_scan = backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
                self.cursor = (try open_scan(backend.ptr, self.budget.allocator(), table, .{
                    .fields = needed.items,
                    .primary_order = binding.primary_order,
                    .primary_key = predicates.primary_key,
                    .conditions = predicates.terms.items,
                    .limit = limits.page_rows,
                })) orelse return error.SqlStatementSnapshotRequired;
            }
        }
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.cursor) |cursor| cursor.close(cursor.ptr);
        if (self.after) |after| self.budget.allocator().free(after);
        if (self.settings) |view| view.deinit();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.budget.backing.destroy(self);
    }

    pub fn next(self: *Stream, max_rows: u32) !Page {
        if (self.failed) return error.SqlStreamFailed;
        if (max_rows == 0 or max_rows > 4096) return error.InvalidSqlLimit;
        return self.pull(max_rows) catch |err| {
            self.failed = true;
            // A failed pull is terminal. Release native snapshots immediately;
            // a portal that remains named must not retain storage admission.
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlProgramLimitExceeded;
            return err;
        };
    }

    fn pull(self: *Stream, max_rows: u32) !Page {
        try self.context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer arena.deinit();
        const out = arena.allocator();
        var rows: std.ArrayList([]const Json) = .empty;
        var flags: std.ArrayList([]const bool) = .empty;
        var eval = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer eval.deinit();
        while (!self.exhausted and rows.items.len < max_rows) {
            try self.context.checkpoint();
            self.pages += 1;
            if (self.pages > self.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var page_arena = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer page_arena.deinit();
            const scratch = page_arena.allocator();
            const wanted: u32 = @intCast(@min(self.context.limits.page_rows, @min(self.remaining, max_rows - rows.items.len) +| self.skip));
            const cursor = self.cursor orelse return error.InvalidSqlBackendResponse;
            const page = try cursor.next(cursor.ptr, scratch, wanted);
            defer page.deinit();
            if (page.rows.len > wanted) return error.InvalidSqlBackendResponse;
            for (page.rows) |row| {
                try self.context.checkpoint();
                self.visited += 1;
                if (self.visited > self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                // Reset expression temporaries per row, not once per entire
                // result, so filters with large discarded values stay bounded.
                _ = eval.reset(.retain_capacity);
                const values = try self.context.binding.scalars.cells(eval.allocator(), row);
                if (!try self.context.binding.scalars.matches(eval.allocator(), values, self.context.parameters)) continue;
                if (self.skip != 0) {
                    self.skip -= 1;
                    continue;
                }
                const projected = try self.context.projectValues(eval.allocator(), row, self.fields, values);
                const cells = try out.alloc(Json, projected.len);
                const nulls = try out.alloc(bool, projected.len);
                var output_context = self.context;
                output_context.arena = out;
                for (projected, cells, nulls) |value, *cell, *is_null| {
                    cell.* = try output_context.outputValue(value.value);
                    is_null.* = value.sql_null;
                }
                try rows.append(out, cells);
                try flags.append(out, nulls);
                self.remaining -= 1;
                self.emitted += 1;
                if (self.remaining == 0) break;
            }
            if (page.after) |after| {
                if (self.after) |previous| if (std.mem.eql(u8, previous, after)) return error.InvalidSqlBackendResponse;
                const owned = try self.budget.allocator().dupe(u8, after);
                if (self.after) |previous| self.budget.allocator().free(previous);
                self.after = owned;
            }
            self.exhausted = self.remaining == 0 or page.after == null;
        }
        if (self.exhausted) {
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
        }
        return .{ .arena = arena, .exhausted = self.exhausted, .output = .{
            .columns = self.context.binding.columns,
            .rows = rows.items,
            .sql_nulls = flags.items,
            .command_tag = "SELECT",
        } };
    }
};
