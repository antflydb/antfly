// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Pull execution for scan/filter/projection plans and disk-spooled blocking
//! results. Binding and parameters live once per cursor; evaluation/result
//! pages share one memory budget and are reclaimed before the next pull.
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

const Spool = @import("result_cursor.zig").Cursor;

/// A stable owner for typed projected columns. HTTP/pgwire rows are gathered
/// only for the delivery page; smaller fetch sizes do not clone an execution
/// batch into a second pending row representation.
const PendingColumns = struct {
    backing: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    values: @import("typed_store.zig").Store,
    exhausted: bool = false,
    terminal_error: ?anyerror = null,
    fn create(a: std.mem.Allocator) !*PendingColumns {
        const self = try a.create(PendingColumns);
        self.* = .{ .backing = a, .arena = .init(a), .values = undefined };
        self.values = .init(self.arena.allocator());
        return self;
    }
    fn deinit(self: *PendingColumns) void {
        const a = self.backing;
        self.values.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};

pub const Stream = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    settings: ?*@import("setting_catalog.zig").View = null,
    context: runtime.Context,
    cursor: ?catalog.Cursor = null,
    spool: ?*Spool = null,
    fields: []const []const u8,
    request: catalog.Scan,
    after: ?[]u8 = null,
    skip: usize,
    remaining: usize,
    visited: usize = 0,
    pages: usize = 0,
    emitted: usize = 0,
    exhausted: bool = false,
    failed: bool = false,
    pending_columns: ?*PendingColumns = null,
    pending_index: usize = 0,
    delivery_error: ?anyerror = null,

    /// Caller retains the compiled plan and backend until close. A null result
    /// selects the bounded materializing executor for unsupported shapes or
    /// when spilling is unavailable. No row reads occur on that decline path.
    pub fn open(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: runtime.Limits) !?*Stream {
        if (compiled.statement != .select) return null;
        if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
        if (limits.execution_batch_rows == 0 or limits.execution_batch_rows > 4096 or limits.page_rows == 0 or limits.page_rows > 4096 or limits.page_bytes == 0 or limits.scan_rows == 0 or limits.scan_pages == 0) return error.InvalidSqlLimit;
        try backend.vtable.checkpoint(backend.ptr);
        const self = try alloc.create(Stream);
        errdefer alloc.destroy(self);
        self.budget = .{ .backing = alloc, .limit = limits.retained_bytes };
        self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer self.arena.deinit();
        self.spool = null;
        self.cursor = null;
        self.after = null;
        self.visited = 0;
        self.pages = 0;
        self.emitted = 0;
        self.failed = false;
        self.pending_columns = null;
        self.pending_index = 0;
        self.delivery_error = null;
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
        self.context = .{ .alloc = self.budget.allocator(), .arena = arena, .backend = @import("decision_eval.zig").scopedBackend(statement_backend, binding), .binding = binding, .parameters = &.{}, .limits = limits, .typed_output = true };
        const params = try arena.alloc(Json, parameters.len);
        for (parameters, params) |value, *out| out.* = try self.context.outputValue(value);
        self.context.parameters = params;
        try @import("decision_eval.zig").validateStatement(arena, statement_backend.decision_provider, binding, params);
        if (binding.aggregate != null or binding.window != null or binding.table == null or statement.count_all or (binding.order_keys.len != 0 and !binding.primary_order)) {
            const decisions = @import("decision_eval.zig");
            var external = false;
            for (binding.scalars.projections) |optional| if (optional) |*program| {
                external = external or decisions.hasExternal(program);
            };
            if (binding.aggregate) |bound| external = external or decisions.hasExternalPrograms(bound.outputs) or decisions.hasExternalPrograms(bound.orders);
            if (binding.window) |bound| external = external or decisions.hasExternalPrograms(bound.outputs) or decisions.hasExternalPrograms(bound.orders);
            if (external or (backend.execution_io == null and backend.spill_manager == null) or limits.spill_bytes == 0 or binding.table == null) {
                if (self.settings) |view| view.deinit();
                self.arena.deinit();
                alloc.destroy(self);
                return null;
            }
            const owner = try self.budget.allocator().create(Spool);
            errdefer self.budget.allocator().destroy(owner);
            owner.manager = .{ .alloc = self.budget.allocator(), .io = backend.execution_io orelse backend.spill_manager.?.io, .context = backend.ptr, .checkpoint = backend.vtable.checkpoint, .root = limits.spill_root, .max_bytes = limits.spill_bytes, .max_record_bytes = @max(@as(usize, 1024), @min(@as(usize, 4 * 1024 * 1024), limits.retained_bytes / 32)) };
            owner.shared = backend.spill_manager;
            const manager = owner.shared orelse &owner.manager;
            errdefer if (owner.shared == null) owner.manager.deinit();
            owner.a = self.budget.allocator();
            owner.width = binding.columns.len;
            owner.rows = try @import("spill.zig").Sequential.init(manager, @max(128, @min(32 * 1024, limits.retained_bytes / 64)));
            errdefer owner.rows.close();
            owner.index = 0;
            owner.sorted = null;
            owner.sorted_rows = &.{};
            owner.sorted_offset = 0;
            owner.sorted_count = 0;
            self.context.spill = manager;
            self.context.sink = .{ .ptr = owner, .append = Spool.append, .take_sorted = Spool.takeSorted };
            self.context.limits.result_rows = limits.scan_rows;
            const output = try self.context.select(compiled.statement.select);
            // Constant/count and bounded fallback paths return ordinary rows.
            for (output.rows, 0..) |row, index| {
                var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
                defer scratch.deinit();
                const values = try scratch.allocator().alloc(@import("scalar.zig").Datum, row.len);
                for (row, values, 0..) |value, *cell, column| cell.* = .{ .value = value, .sql_null = if (output.sql_nulls) |flags| flags[index][column] else value == .null };
                try Spool.append(owner, values);
            }
            self.spool = owner;
            self.fields = &.{};
            self.request = .{ .fields = &.{}, .limit = limits.page_rows };
            self.skip = 0;
            self.remaining = owner.count();
            self.exhausted = owner.count() == 0;
            self.context.sink = null;
            return self;
        }
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
        self.request = .{ .fields = needed.items, .primary_order = binding.primary_order, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = limits.page_rows };
        if (!self.exhausted) {
            if (binding.relation != null) {
                self.cursor = try @import("relation_runtime.zig").openCursor(self.context);
            } else {
                if (backend.vtable.open_scan) |open_scan| {
                    self.cursor = try open_scan(backend.ptr, self.budget.allocator(), table, self.request);
                }
                if (self.cursor == null and !backend.pinned_statement_snapshot) return error.SqlStatementSnapshotRequired;
            }
        }
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.pending_columns) |page| page.deinit();
        if (self.spool) |spool| spool.close();
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
            if (self.pending_columns) |page| page.deinit();
            self.pending_columns = null;
            if (self.spool) |spool| spool.close();
            self.spool = null;
            // A failed pull is terminal. Release native snapshots immediately;
            // a portal that remains named must not retain storage admission.
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlProgramLimitExceeded;
            return err;
        };
    }

    fn pullSpool(self: *Stream, spool: *Spool, max_rows: u32) !Page {
        var arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer arena.deinit();
        const a = arena.allocator();
        const count = @min(max_rows, spool.count() - spool.index);
        const rows = try a.alloc([]const Json, count);
        const flags = try a.alloc([]const bool, count);
        var bytes: usize = 0;
        var read: usize = 0;
        for (rows, flags) |*out, *bits| {
            const row = try spool.read(a);
            out.* = try a.alloc(Json, row.values.len);
            bits.* = try a.alloc(bool, row.values.len);
            for (row.values, @constCast(out.*), @constCast(bits.*)) |value, *cell, *flag| {
                cell.* = if (spool.ownsRead()) value.value else (try @import("operators.zig").cloneDatum(a, value)).value;
                flag.* = value.sql_null;
                bytes +|= try @import("operators.zig").datumBytes(value);
            }
            if (spool.sorted) |top| if (top.external == null) top.releaseFinishedRow(spool.sorted_offset + spool.index);
            spool.index += 1;
            read += 1;
            if (bytes >= self.context.limits.page_bytes) break;
        }
        self.emitted += read;
        self.exhausted = spool.index == spool.count();
        return .{ .arena = arena, .exhausted = self.exhausted, .output = .{ .columns = self.context.binding.columns, .rows = rows[0..read], .sql_nulls = flags[0..read], .command_tag = "SELECT" } };
    }
    fn columnProgram(self: *Stream, a: std.mem.Allocator, program: *const @import("scalar.zig").Program, page: catalog.ColumnPage) ![]const @import("scalar.zig").Datum {
        if (try @import("vector_eval.zig").evaluateColumnsScheduled(a, program, page, self.context.binding.scalars.columns, self.context.parameters, self.context.backend.execution_io)) |values| return values;
        const values = try a.alloc(@import("scalar.zig").Datum, page.selection.len);
        var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer scratch.deinit();
        for (values, 0..) |*value, index| {
            _ = scratch.reset(.free_all);
            const cells = try self.context.binding.scalars.columnCells(scratch.allocator(), page, index);
            value.* = try @import("operators.zig").cloneDatum(a, try self.context.evaluate(scratch.allocator(), program.*, cells));
        }
        return values;
    }
    fn executeColumns(self: *Stream, max_rows: u32) !*PendingColumns {
        const pending = try PendingColumns.create(self.budget.allocator());
        errdefer pending.deinit();
        self.fillColumns(pending, max_rows) catch |err| {
            if (pending.values.failed or pending.values.len == 0) return err;
            pending.terminal_error = err;
        };
        if (self.exhausted or pending.terminal_error != null) {
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
        }
        pending.exhausted = self.exhausted and pending.terminal_error == null;
        return pending;
    }
    fn fillColumns(self: *Stream, pending: *PendingColumns, max_rows: u32) !void {
        var output_bytes: usize = 0;
        while (!self.exhausted and pending.values.len < max_rows) {
            try self.context.checkpoint();
            self.pages += 1;
            if (self.pages > self.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer scratch.deinit();
            const a = scratch.allocator();
            const wanted: u32 = @intCast(@min(@min(self.context.limits.execution_batch_rows, @max(@as(usize, 1), self.context.limits.retained_bytes / (16 * 1024 + self.fields.len * @sizeOf(@import("scalar.zig").Datum) * 16))), @min(self.remaining, max_rows - pending.values.len) +| self.skip));
            const cursor = self.cursor.?;
            const page = try cursor.next_columns.?(cursor.ptr, a, wanted);
            if (page.selection.len > wanted or page.selection.len > self.context.limits.scan_rows -| self.visited) return error.SqlProgramLimitExceeded;
            self.visited += page.selection.len;
            const before = pending.values.len;
            const bytes = self.appendColumnPage(pending, a, page) catch |err| switch (err) {
                error.SqlDivisionByZero, error.SqlNumericOutOfRange, error.SqlTypeMismatch, error.SqlProgramLimitExceeded, error.InvalidSqlDateTime, error.SqlCardinalityViolation => if (pending.values.len == before) try self.appendScalarColumnPage(pending, a, page) else return err,
                else => return err,
            };
            output_bytes +|= bytes;
            self.exhausted = self.remaining == 0 or page.after == null;

            if (output_bytes >= @min(self.context.limits.page_bytes, @max(@as(usize, 1024), self.context.limits.retained_bytes / 16))) break;
        }
    }
    fn appendColumnPage(self: *Stream, pending: *PendingColumns, backing: std.mem.Allocator, page: catalog.ColumnPage) !usize {
        var attempt = std.heap.ArenaAllocator.init(backing);
        defer attempt.deinit();
        const a = attempt.allocator();
        var skip = self.skip;
        var output_bytes: usize = 0;
        const predicates = if (self.context.binding.scalars.predicate) |*program| try self.columnProgram(a, program, page) else null;
        var selection: std.ArrayList(usize) = .empty;
        for (page.selection, 0..) |physical, index| {
            if (predicates) |values| {
                if (values[index].sql_null) continue;
                if (values[index].value != .bool) return error.SqlTypeMismatch;
                if (!values[index].value.bool) continue;
            }
            if (skip != 0) {
                skip -= 1;
                continue;
            }
            try selection.append(a, physical);
        }
        const selected: catalog.ColumnPage = .{ .batch = page.batch, .selection = selection.items };
        const projections = try a.alloc(?[]const @import("scalar.zig").Datum, self.fields.len);
        @memset(projections, null);
        var programs: std.ArrayList(*const @import("scalar.zig").Program) = .empty;
        var slots: std.ArrayList(usize) = .empty;
        for (self.context.binding.scalars.projections, 0..) |*optional, index| if (optional.*) |*program| {
            try programs.append(a, program);
            try slots.append(a, index);
        };
        const evaluated = try @import("vector_eval.zig").evaluateColumnsManyScheduled(a, programs.items, selected, self.context.binding.scalars.columns, self.context.parameters, self.context.backend.execution_io);
        for (evaluated, programs.items, slots.items) |values, program, index| projections[index] = values orelse try self.columnProgram(a, program, selected);
        for (0..selection.items.len) |index| {
            const row = try a.alloc(@import("scalar.zig").Datum, self.fields.len);
            for (self.fields, self.context.binding.columns, row, 0..) |field, column, *value, ordinal| {
                const cell = if (projections[ordinal]) |values| values[index] else blk: {
                    const raw = try selected.cell(a, index, field);
                    break :blk @import("scalar.zig").Datum{ .value = raw.value, .sql_null = raw.sql_null };
                };
                value.* = .{ .value = try describe.coerceAlloc(a, cell.value, column.type), .sql_null = cell.sql_null };
                output_bytes +|= try @import("operators.zig").datumBytes(value.*);
            }
            _ = try pending.values.append(row);
            self.remaining -= 1;
            self.emitted += 1;
        }
        self.skip = skip;
        return output_bytes;
    }
    /// Recover row order only on a semantic vector failure. Ordinary batches
    /// keep their fused kernels; a failure retains the exact successful prefix.
    fn appendScalarColumnPage(self: *Stream, pending: *PendingColumns, backing: std.mem.Allocator, page: catalog.ColumnPage) !usize {
        var scratch = std.heap.ArenaAllocator.init(backing);
        defer scratch.deinit();
        var output_bytes: usize = 0;
        for (page.selection, 0..) |physical, index| {
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const cells = try self.context.binding.scalars.columnCells(a, page, index);
            if (self.context.binding.scalars.predicate) |program| {
                const accepted = try self.context.evaluate(a, program, cells);
                if (accepted.sql_null) continue;
                if (accepted.value != .bool) return error.SqlTypeMismatch;
                if (!accepted.value.bool) continue;
            }
            if (self.skip != 0) {
                self.skip -= 1;
                continue;
            }
            if (self.remaining == 0) break;
            const one: catalog.ColumnPage = .{ .batch = page.batch, .selection = &.{physical} };
            const row = try a.alloc(@import("scalar.zig").Datum, self.fields.len);
            for (self.fields, self.context.binding.columns, row, 0..) |field, column, *value, ordinal| {
                const cell = if (self.context.binding.scalars.projections[ordinal]) |program|
                    try self.context.evaluate(a, program, cells)
                else blk: {
                    const raw = try one.cell(a, 0, field);
                    break :blk @import("scalar.zig").Datum{ .value = raw.value, .sql_null = raw.sql_null };
                };
                value.* = .{ .value = try describe.coerceAlloc(a, cell.value, column.type), .sql_null = cell.sql_null };
                output_bytes +|= try @import("operators.zig").datumBytes(value.*);
            }
            _ = try pending.values.append(row);
            self.remaining -= 1;
            self.emitted += 1;
        }
        return output_bytes;
    }
    /// Evaluate native batches independently of HTTP/pgwire delivery sizes.
    /// The pending page owns projected values, so the scan can advance without
    /// exposing borrowed vectors to a portal or retaining an entire result.
    fn pullColumns(self: *Stream, max_rows: u32) !Page {
        if (self.delivery_error) |err| return err;
        var arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer arena.deinit();
        const a = arena.allocator();
        var rows: std.ArrayList([]const Json) = .empty;
        var flags: std.ArrayList([]const bool) = .empty;
        var bytes: usize = 0;
        while (rows.items.len < max_rows and !self.exhausted) {
            if (self.pending_columns == null) {
                self.pending_columns = self.executeColumns(self.context.limits.execution_batch_rows) catch |err| {
                    if (rows.items.len == 0) return err;
                    self.delivery_error = err;
                    if (self.cursor) |cursor| cursor.close(cursor.ptr);
                    self.cursor = null;
                    break;
                };
                self.pending_index = 0;
                // Execution may have reached EOF while delivery still has rows.
                self.exhausted = false;
            }
            const pending = self.pending_columns.?;
            if (self.pending_index == pending.values.len) if (pending.terminal_error) |err| {
                if (rows.items.len != 0) break;
                return err;
            };
            while (self.pending_index < pending.values.len and rows.items.len < max_rows) {
                const index = self.pending_index;
                const row = try a.alloc(Json, self.context.binding.columns.len);
                const nulls = try a.alloc(bool, self.context.binding.columns.len);
                for (row, nulls, 0..) |*out, *sql_null, column| {
                    const datum = try pending.values.cell(a, index, column);
                    sql_null.* = datum.sql_null;
                    out.* = (try @import("operators.zig").cloneDatum(a, datum)).value;
                    bytes +|= try @import("operators.zig").datumBytes(datum);
                }
                try rows.append(a, row);
                try flags.append(a, nulls);
                self.pending_index += 1;
                if (bytes >= self.context.limits.page_bytes) break;
            }
            if (self.pending_index == pending.values.len and pending.terminal_error == null) {
                self.exhausted = pending.exhausted;
                pending.deinit();
                self.pending_columns = null;
            }
            if (bytes >= self.context.limits.page_bytes) break;
        }
        return .{ .arena = arena, .exhausted = self.exhausted, .output = .{ .columns = self.context.binding.columns, .rows = rows.items, .sql_nulls = flags.items, .command_tag = "SELECT" } };
    }
    fn columnsEligible(self: *Stream) bool {
        const cursor = self.cursor orelse return false;
        if (cursor.next_columns == null) return false;
        const decisions = @import("decision_eval.zig");
        if (self.context.binding.scalars.predicate) |*program| if (decisions.hasExternal(program)) return false;
        for (self.context.binding.scalars.projections) |optional| if (optional) |*program| if (decisions.hasExternal(program)) return false;
        return true;
    }
    fn pull(self: *Stream, max_rows: u32) !Page {
        try self.context.checkpoint();
        if (self.spool) |spool| return self.pullSpool(spool, max_rows);
        if (self.delivery_error != null or self.pending_columns != null or self.columnsEligible()) return self.pullColumns(max_rows);
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
            const native_scratch = page_arena.allocator();
            const wanted: u32 = @intCast(@min(self.context.limits.page_rows, @min(self.remaining, max_rows - rows.items.len) +| self.skip));
            var request = self.request;
            request.limit = wanted;
            request.after = self.after;
            const page = if (self.cursor) |cursor| try cursor.next(cursor.ptr, native_scratch, wanted) else try self.context.backend.vtable.scan(self.context.backend.ptr, native_scratch, self.context.binding.table.?, request);
            defer page.deinit();
            if (page.rows.len > wanted) return error.InvalidSqlBackendResponse;
            if (self.visited + page.rows.len > self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
            var first: usize = 0;
            while (first < page.rows.len) {
                var decision_page = std.heap.ArenaAllocator.init(self.budget.allocator());
                defer decision_page.deinit();
                const scratch = decision_page.allocator();
                const chunk_cells = try @import("decision_eval.zig").rowPage(scratch, self.context.binding.scalars, page.rows[first..], self.context.limits.page_rows, self.context.limits.page_bytes);
                const chunk_rows = page.rows[first..][0..chunk_cells.len];
                const decisions = @import("decision_eval.zig");
                var external = if (self.context.binding.scalars.predicate) |*p| decisions.hasExternal(p) else false;
                for (self.context.binding.scalars.projections) |optional| if (optional) |*p| {
                    external = external or decisions.hasExternal(p);
                };
                const page_cells = if (external) chunk_cells else null;
                const predicate_values = if (page_cells) |values| if (self.context.binding.scalars.predicate) |*p| try decisions.evaluateBatch(scratch, self.context.backend.decision_provider, p, values, self.context.parameters) else null else null;
                var selected_cells: std.ArrayList([]const @import("scalar.zig").Datum) = .empty;
                const positions: []?usize = if (external) try scratch.alloc(?usize, chunk_rows.len) else @constCast(&.{});
                if (external) {
                    @memset(positions, null);
                    var skip = self.skip;
                    for (page_cells.?, 0..) |cells, index| {
                        if (predicate_values) |values| {
                            if (values[index].sql_null) continue;
                            if (values[index].value != .bool) return error.SqlTypeMismatch;
                            if (!values[index].value.bool) continue;
                        }
                        if (skip > 0) {
                            skip -= 1;
                            continue;
                        }
                        positions[index] = selected_cells.items.len;
                        try selected_cells.append(scratch, cells);
                    }
                }
                const projection_values = if (external) blk: {
                    const values = try scratch.alloc(?[]const @import("scalar.zig").Datum, self.context.binding.scalars.projections.len);
                    for (self.context.binding.scalars.projections, values) |optional, *value| value.* = if (optional) |*p| try decisions.evaluateBatch(scratch, self.context.backend.decision_provider, p, selected_cells.items, self.context.parameters) else null;
                    break :blk values;
                } else null;
                for (chunk_rows, 0..) |row, row_index| {
                    try self.context.checkpoint();
                    self.visited += 1;
                    if (self.visited > self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                    // Reset expression temporaries per row, not once per entire
                    // result, so filters with large discarded values stay bounded.
                    _ = eval.reset(.retain_capacity);
                    const values = if (page_cells) |cells| cells[row_index] else try self.context.binding.scalars.cells(eval.allocator(), row);
                    if (external) {
                        if (predicate_values) |predicates| if (predicates[row_index].sql_null or !predicates[row_index].value.bool) continue;
                    } else if (!try self.context.binding.scalars.matches(eval.allocator(), values, self.context.parameters)) continue;
                    if (self.skip != 0) {
                        self.skip -= 1;
                        continue;
                    }
                    const projected = if (projection_values) |projections| blk: {
                        const fields = try eval.allocator().alloc(@import("scalar.zig").Datum, self.fields.len);
                        for (self.fields, fields, 0..) |field, *cell, index| cell.* = if (index < projections.len and projections[index] != null)
                            projections[index].?[positions[row_index].?]
                        else typed: {
                            const stored = try row.cell(field);
                            break :typed .{ .value = try describe.coerce(stored.value, self.context.binding.columns[index].type), .sql_null = stored.sql_null };
                        };
                        break :blk fields;
                    } else try self.context.projectValues(eval.allocator(), row, self.fields, values);
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
                first += chunk_rows.len;
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

test "SQL decisions pull streaming batches predicates before offset and projections after offset" {
    const d = @import("../functions/decisions.zig");
    const Fake = struct {
        calls: usize = 0,
        max_batch: usize = 0,
        fn validate(_: *anyopaque, _: []const u8, questions: Json) !void {
            try d.validateQuestions(questions, d.capabilities(.antfly));
        }
        fn evaluate(ptr: *anyopaque, a: std.mem.Allocator, requests: []const d.Request) ![]const Json {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += requests.len;
            self.max_batch = @max(self.max_batch, requests.len);
            const values = try a.alloc(Json, requests.len);
            for (values) |*value| value.* = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"mock\",\"answers\":{\"answer\":{\"type\":\"noul\",\"noul\":0.9}},\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}", .{});
            return values;
        }
    };
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .count = 4 };
    var fake: Fake = .{};
    var backend = fixture.backend();
    backend.decision_provider = .{ .ptr = &fake, .validate_fn = Fake.validate, .evaluate_batch_fn = Fake.evaluate };
    var compiled = try compiler.compile(a, "SELECT ai_probability(CAST(n AS TEXT), 'Refund?', 'local') AS p FROM docs WHERE ai_probability(CAST(n AS TEXT), 'Refund?', 'local') > 0.8 LIMIT 2 OFFSET 1", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .page_rows = 4 })).?;
    defer stream.close();
    var page = try stream.next(2);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.output.rows.len);
    try std.testing.expectEqual(@as(usize, 5), fake.calls);
    try std.testing.expectEqual(@as(usize, 3), fake.max_batch);
}

test "SQL decision streams validate nested specifications before opening reads" {
    const a = std.testing.allocator;
    const queries = [_][]const u8{
        "WITH q AS (SELECT CASE WHEN FALSE THEN ai_decide(CAST(n AS TEXT), '{}', 'local') ELSE NULL END AS p FROM docs) SELECT p FROM q",
        "SELECT p FROM (SELECT ai_decide(CAST(n AS TEXT), $1::jsonb, 'local') AS p FROM docs LIMIT 0) q",
    };
    for (queries, 0..) |sql, i| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var mock: @import("decision_eval.zig").testing.Provider = .{};
        var backend = fixture.backend();
        backend.decision_provider = mock.provider();
        const parameters: []const Json = if (i == 0) &.{} else &.{.{ .object = .empty }};
        try std.testing.expectError(error.DecisionLimitExceeded, Stream.open(a, backend, &compiled, parameters, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.opened);
        try std.testing.expectEqual(@as(usize, 0), mock.calls);
    }
}

test "SQL decision pull pages honor byte limits without losing cursor rows" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .count = 8 };
    var provider: @import("decision_eval.zig").testing.Provider = .{};
    var backend = fixture.backend();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(a, "SELECT ai_probability(CAST(n AS TEXT),'Refund?','local') FROM docs LIMIT 5 OFFSET 1", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .page_rows = 4, .page_bytes = 1 })).?;
    defer stream.close();
    var first = try stream.next(3);
    defer first.deinit();
    var second = try stream.next(3);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 3), first.output.rows.len);
    try std.testing.expectEqual(@as(usize, 2), second.output.rows.len);
    try std.testing.expectEqual(@as(usize, 5), provider.calls);
    try std.testing.expectEqual(@as(usize, 1), provider.max_batch);
}

test "SQL decision pull streams carry trusted source routing" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .count = 2 };
    var provider: @import("decision_eval.zig").testing.Provider = .{ .expected_source = "docs" };
    var backend = fixture.backend();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(a, "SELECT ai_probability(CAST(n AS TEXT),'Refund?','local') FROM docs LIMIT 1", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
    defer stream.close();
    var page = try stream.next(1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
}

test "SQL blocking results transfer sorted operators and deliver bounded continuation pages" {
    for ([_][]const u8{
        "SELECT n FROM docs ORDER BY n DESC",
        "SELECT n % 37 AS k, count(*) AS c FROM docs GROUP BY n % 37 ORDER BY k",
        "SELECT n, row_number() OVER (ORDER BY n DESC) AS r FROM docs ORDER BY n DESC",
    }, [_]usize{ 1000, 37, 1000 }) |sql, expected| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .count = 1000 };
        var backend = fixture.backend();
        backend.execution_io = std.testing.io;
        const stream = (try Stream.open(std.heap.page_allocator, backend, &compiled, &.{}, .{ .result_rows = 2, .page_rows = 16, .retained_bytes = 256 * 1024 })).?;
        defer stream.close();
        try std.testing.expect(stream.spool != null);
        if (std.mem.indexOf(u8, sql, "row_number()") != null) {
            try std.testing.expect(stream.spool.?.sorted == null);
            try std.testing.expectEqual(expected, @as(usize, @intCast(stream.spool.?.rows.size)));
        } else {
            try std.testing.expect(stream.spool.?.sorted != null);
            try std.testing.expectEqual(@as(usize, 0), @as(usize, @intCast(stream.spool.?.rows.size)));
        }
        const reads = fixture.calls;
        var seen: usize = 0;
        while (true) {
            var result = try stream.next(7);
            defer result.deinit();
            try std.testing.expect(result.output.rows.len <= 7);
            if (expected == 1000) {
                for (result.output.rows) |row| {
                    try std.testing.expectEqual(@as(i64, @intCast(999 - seen)), row[0].integer);
                    seen += 1;
                }
            } else {
                seen += result.output.rows.len;
            }
            if (result.exhausted) break;
        }
        try std.testing.expectEqual(expected, seen);
        try std.testing.expectEqual(reads, fixture.calls);
        try std.testing.expect(stream.budget.peak <= 256 * 1024);
        fixture.cancel = true;
        try std.testing.expectError(error.QueryCanceled, stream.next(1));
        try std.testing.expect(stream.spool == null);
    }
}

const NativeColumns = struct {
    fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        const fixture: *Fixture = @ptrCast(@alignCast(raw));
        fixture.opened += 1;
        return .{ .ptr = raw, .next = Fixture.next, .next_columns = nextColumns, .close = Fixture.close };
    }
    fn nextColumns(raw: *anyopaque, alloc: std.mem.Allocator, wanted: u32) !catalog.ColumnPage {
        const fixture: *Fixture = @ptrCast(@alignCast(raw));
        fixture.calls += 1;
        const count = @min(wanted, fixture.count - fixture.offset);
        const values = try alloc.alloc(i64, count);
        const refs = try alloc.alloc(@import("../storage/rowsource/types.zig").RowRef, count);
        const selected = try alloc.alloc(usize, count);
        for (values, refs, selected, 0..) |*value, *ref, *index, i| {
            value.* = @intCast(fixture.offset + i);
            ref.* = .{ .relational_key = "r" };
            index.* = i;
        }
        const columns = try alloc.alloc(@import("../storage/rowsource/types.zig").ColumnVector, 1);
        columns[0] = .{ .name = "n", .values = .{ .i64 = values } };
        fixture.offset += count;
        return .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = refs, .columns = columns }, .selection = selected, .after = if (fixture.offset < fixture.count) "more" else null };
    }
};

test "SQL native execution batches drain small delivery pages without rescan" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .count = 1200 };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.open_scan = NativeColumns.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(a, "SELECT n + 1 FROM docs WHERE n >= 0", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 1024, .page_rows = 256 })).?;
    defer stream.close();
    var count: usize = 0;
    while (true) {
        var page = try stream.next(17);
        defer page.deinit();
        if (count == 0) {
            try std.testing.expectEqual(@as(usize, 1024), fixture.offset);
            try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        }
        for (page.output.rows) |row| {
            count += 1;
            try std.testing.expectEqual(@as(i64, @intCast(count)), row[0].integer);
        }
        try std.testing.expect(page.output.rows.len <= 17);
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 1200), count);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
}

test "SQL native execution batches retain successful prefixes before semantic errors" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT 1 / (1 - n) FROM docs",
        "SELECT n FROM docs WHERE 1 / (1 - n) > 0",
        "SELECT n, 1 / (1 - n) FROM docs",
    }) |sql| for ([_]bool{ false, true }) |native| {
        var fixture: Fixture = .{ .count = 2 };
        var backend = fixture.backend();
        var vtable = backend.vtable.*;
        if (native) vtable.open_scan = NativeColumns.open;
        backend.vtable = &vtable;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 1024, .page_rows = 256 })).?;
        defer stream.close();
        var first = try stream.next(1);
        defer first.deinit();
        try std.testing.expectEqual(@as(usize, 1), first.output.rows.len);
        try std.testing.expect(!first.exhausted);
        try std.testing.expectError(error.SqlDivisionByZero, stream.next(1));
        try std.testing.expectEqual(@as(usize, 1), fixture.closed);
    };
}

test "SQL native delivery retains prefixes across execution batch boundaries" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .count = 2 };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.open_scan = NativeColumns.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(a, "SELECT 1 / (1 - n) FROM docs", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 1, .page_rows = 256 })).?;
    defer stream.close();
    var page = try stream.next(2);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.output.rows.len);
    try std.testing.expectEqual(@as(i64, 1), page.output.rows[0][0].integer);
    try std.testing.expect(!page.exhausted);
    try std.testing.expectError(error.SqlDivisionByZero, stream.next(2));
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
}
