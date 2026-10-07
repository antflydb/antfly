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

//! Pull execution for scan/filter/projection plans and disk-spooled blocking
//! results. Binding and parameters live once per cursor; evaluation/result
//! pages share one memory budget and are reclaimed before the next pull.
const native_platform = @import("antfly_platform");
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

/// A bounded result lease. Payloads remain in retained execution columns until
/// the transport has flushed them; consumers release the lease before close.
pub const BatchPage = struct {
    arena: std.heap.ArenaAllocator,
    columns: []const describe.Column,
    values: @import("execution_batch.zig").Batch,
    exhausted: bool,
    lease: ?*PendingColumns = null,
    row_page: ?Page = null,
    spill_lease: ?Spool.Lease = null,

    pub fn deinit(self: *BatchPage) void {
        if (self.lease) |lease| lease.deinit();
        if (self.row_page) |*page| page.deinit();
        if (self.spill_lease) |lease| lease.deinit();
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
    ordered: bool = false,
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
    fn openScan(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.opened += 1;
        return .{ .order_satisfied = self.ordered and request.order.len == 1 and std.mem.eql(u8, request.order[0].column, "n") and !request.order[0].descending and !request.order[0].nulls_first, .ptr = self, .next = next, .close = close };
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
    const started = std.Io.Clock.awake.now(native_platform.testing.io).nanoseconds;
    var first_page_ns: i96 = 0;
    while (true) {
        var page = try stream.next(73);
        defer page.deinit();
        if (seen == 0) {
            first_page_ns = std.Io.Clock.awake.now(native_platform.testing.io).nanoseconds - started;
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
    std.debug.print("SQL pull stream: rows={d} peak_bytes={d} first_page_ns={d} elapsed_ns={d}\n", .{ seen, stream.budget.peak, first_page_ns, std.Io.Clock.awake.now(native_platform.testing.io).nanoseconds - started });
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
    /// Bounded projected blocks per producer. Credit follows the final delivery
    /// span, and can outlive the joined worker without borrowing its address.
    const Credit = struct {
        a: std.mem.Allocator,
        io: std.Io,
        ready: std.Io.Event = .unset,
        available: std.atomic.Value(usize) = .init(2),
        stopped: std.atomic.Value(bool) = .init(false),
        refs: std.atomic.Value(usize) = .init(1),
        fn create(a: std.mem.Allocator, io: std.Io) !*Credit {
            const self = try a.create(Credit);
            self.* = .{ .a = a, .io = io };
            self.ready.set(io);
            return self;
        }
        fn acquire(self: *Credit) !bool {
            while (true) {
                if (self.stopped.load(.acquire)) return false;
                const count = self.available.load(.acquire);
                if (count != 0) {
                    if (self.available.cmpxchgWeak(count, count - 1, .acq_rel, .acquire) == null) return true;
                    continue;
                }
                self.ready.reset();
                if (self.available.load(.acquire) == 0 and !self.stopped.load(.acquire)) try self.ready.wait(self.io);
            }
        }
        fn stop(self: *Credit) void {
            self.stopped.store(true, .release);
            self.ready.set(self.io);
        }
        fn release(self: *Credit) void {
            if (self.refs.fetchSub(1, .acq_rel) == 1) self.a.destroy(self);
        }
    };
    const Owner = struct {
        a: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        values: @import("typed_store.zig").Store,
        positions: std.ArrayList(usize) = .empty,
        errors: std.ArrayList(?anyerror) = .empty,
        refs: std.atomic.Value(usize) = .init(1),
        credit: ?*Credit = null,
    };
    owner: *Owner,
    values: *@import("typed_store.zig").Store,
    begin: usize = 0,
    end: ?usize = null,
    scan_rows: usize = 0,
    exhausted: bool = false,
    terminal_error: ?anyerror = null,
    fn create(a: std.mem.Allocator) !*PendingColumns {
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        const self = try a.create(PendingColumns);
        owner.* = .{ .a = a, .arena = .init(a), .values = undefined };
        owner.values = .init(owner.arena.allocator());
        self.* = .{ .owner = owner, .values = &owner.values };
        return self;
    }
    fn len(self: *const PendingColumns) usize {
        return (self.end orelse self.values.len) - self.begin;
    }
    fn view(self: *PendingColumns, begin: usize, end: usize) !*PendingColumns {
        const part = try self.owner.a.create(PendingColumns);
        _ = self.owner.refs.fetchAdd(1, .monotonic);
        part.* = .{ .owner = self.owner, .values = self.values, .begin = self.begin + begin, .end = self.begin + end };
        return part;
    }
    fn cell(self: *const PendingColumns, a: std.mem.Allocator, row: usize, column: usize) !@import("scalar.zig").Datum {
        return self.values.cell(a, self.begin + row, column);
    }
    fn reserveRecords(self: *PendingColumns, count: usize) !void {
        const a = self.owner.arena.allocator();
        try self.owner.positions.ensureUnusedCapacity(a, count);
        try self.owner.errors.ensureUnusedCapacity(a, count);
    }
    fn record(self: *PendingColumns, position: usize, failure: ?anyerror) void {
        self.owner.positions.appendAssumeCapacity(position);
        self.owner.errors.appendAssumeCapacity(failure);
    }
    fn credit(self: *PendingColumns, control: *Credit) void {
        std.debug.assert(self.owner.credit == null);
        _ = control.refs.fetchAdd(1, .monotonic);
        self.owner.credit = control;
    }
    fn deinit(self: *PendingColumns) void {
        const owner = self.owner;
        const a = owner.a;
        a.destroy(self);
        if (owner.refs.fetchSub(1, .acq_rel) == 1) {
            const control = owner.credit;
            owner.values.deinit();
            owner.arena.deinit();
            a.destroy(owner);
            // Free the payload before allowing its producer to allocate again.
            if (control) |lease| {
                _ = lease.available.fetchAdd(1, .release);
                lease.ready.set(lease.io);
                lease.release();
            }
        }
    }
};

// Workers publish owned column blocks. Only the ordered consumer admits scan
// progress, applies OFFSET/LIMIT and exposes deferred expression failures.
const ParallelScan = struct {
    a: std.mem.Allocator,
    io: std.Io,
    lanes: []Lane,
    storage: []Lane = &.{},
    lane: usize = 0,
    workers: [8]Worker = undefined,
    worker_count: usize = 0,
    const Worker = struct {
        pipeline: *ParallelScan,
        index: usize,
        credit: *PendingColumns.Credit,
        task: ?@import("parallel_scheduler.zig").Task(anyerror!void) = null,
        fn run(self: *Worker) anyerror!void {
            var index = self.index;
            while (index < self.pipeline.lanes.len) : (index += self.pipeline.worker_count) {
                const lane = &self.pipeline.lanes[index];
                lane.produce() catch |err| {
                    lane.failure = err;
                    lane.queue.close(self.pipeline.io);
                    return;
                };
                lane.queue.close(self.pipeline.io);
            }
        }
    };
    const Lane = struct {
        stream: Stream,
        credit: *PendingColumns.Credit,
        slots: [2]*PendingColumns = undefined,
        queue: std.Io.Queue(*PendingColumns) = undefined,
        current: ?*PendingColumns = null,
        index: usize = 0,
        consumed_scan: usize = 0,
        failure: ?anyerror = null,
        fn produce(self: *Lane) !void {
            const io = self.stream.context.backend.execution_io.?;
            while (!self.stream.exhausted) {
                if (!try self.credit.acquire()) return;
                const before = self.stream.visited;
                const pending = try self.stream.executeColumns(self.stream.context.limits.executionRows());
                pending.credit(self.credit);
                var owned = true;
                defer if (owned) pending.deinit();
                pending.scan_rows = self.stream.visited - before;
                const terminal = pending.terminal_error != null;
                _ = try self.queue.put(io, &.{pending}, 1);
                owned = false;
                if (terminal) break;
            }
        }
    };
    fn start(source: *Stream) !?*ParallelScan {
        const cursor = source.cursor orelse return null;
        const split = cursor.split_ordered orelse return null;
        const io = source.context.backend.execution_io orelse return null;
        if (source.context.limits.retained_bytes < 16 * 1024 * 1024 or source.remaining < 8192) return null;
        if ((cursor.estimated_rows orelse 0) < 8192 and (cursor.estimated_bytes orelse 0) < 2 * 1024 * 1024) return null;
        const a = source.budget.allocator();
        const maximum = if (cursor.ordered_split_bytes == 0) 16 else @min(16, (source.budget.limit -| source.budget.live) / 4 / cursor.ordered_split_bytes);
        if (maximum < 2) return null;
        const children = (try split(cursor.ptr, a, maximum)) orelse return null;
        defer a.free(children);
        if (children.len < 2) {
            for (children) |child| child.close(child.ptr);
            return null;
        }
        const fanout = @import("parallel_scheduler.zig").global().fanout(children.len, source.context.limits.retained_bytes / 2, 4 * 1024 * 1024);
        if (fanout < 2) {
            for (children) |child| child.close(child.ptr);
            return null;
        }
        const workspace = source.context.limits.retained_bytes / (2 * fanout);
        var moved: usize = 0;
        errdefer for (children[moved..]) |child| child.close(child.ptr);
        const self = try a.create(ParallelScan);
        self.* = .{ .a = a, .io = io, .lanes = &.{} };
        errdefer self.close();
        const lanes = try a.alloc(Lane, children.len);
        self.storage = lanes;
        for (self.workers[0..fanout], 0..) |*worker, index| {
            worker.* = .{ .pipeline = self, .index = index, .credit = try PendingColumns.Credit.create(a, io) };
            self.worker_count += 1;
        }
        for (children, lanes, 0..) |child, *lane, index| {
            const credit = self.workers[index % fanout].credit;
            lane.* = .{ .credit = credit, .stream = .{
                .budget = .{ .backing = a, .limit = workspace },
                .arena = undefined,
                .context = source.context,
                .cursor = child,
                .fields = source.fields,
                .request = source.request,
                .skip = 0,
                .remaining = std.math.maxInt(usize),
                .parallel_checked = true,
                .projected_alloc = a,
                .one_scan_page = true,
            } };
            lane.queue = .init(&lane.slots);
            lane.stream.arena = .init(lane.stream.budget.allocator());
            lane.stream.context.alloc = lane.stream.budget.allocator();
            lane.stream.context.arena = lane.stream.arena.allocator();
            // Scratch ownership is independent of the logical scan batch.
            // Only the ordered consumer charges page progress.
            lane.stream.context.limits.scan_pages = std.math.maxInt(usize);
            lane.stream.context.limits.scan_rows = std.math.maxInt(usize);
            moved += 1;
            self.lanes = lanes[0..moved];
        }
        // A producer needs a concurrent consumer for bounded backpressure.
        for (self.workers[0..self.worker_count]) |*worker| {
            worker.task = @import("parallel_scheduler.zig").global().submit(io, workspace, Worker.run, .{worker});
            if (worker.task == null) {
                self.close();
                return null;
            }
        }
        return self;
    }
    fn close(self: *ParallelScan) void {
        for (self.workers[0..self.worker_count]) |*worker| worker.credit.stop();
        for (self.lanes) |*lane| lane.queue.close(self.io);
        for (self.workers[0..self.worker_count]) |*worker| if (worker.task) |*task| {
            task.cancel(self.io) catch {};
        };
        for (self.lanes) |*lane| {
            if (lane.current) |page| page.deinit();
            while (true) {
                var page: [1]*PendingColumns = undefined;
                const count = lane.queue.getUncancelable(self.io, &page, 0) catch break;
                if (count == 0) break;
                page[0].deinit();
            }
            if (lane.stream.cursor) |cursor| cursor.close(cursor.ptr);
            lane.stream.arena.deinit();
            std.debug.assert(lane.stream.budget.live == 0);
        }
        for (self.workers[0..self.worker_count]) |*worker| worker.credit.release();
        self.a.free(self.storage);
        self.a.destroy(self);
    }
    fn advanceScan(source: *Stream, lane: *Lane, position: usize) !void {
        const delta = position - lane.consumed_scan;
        try source.admitRows(delta);
        lane.consumed_scan = position;
    }
    fn pull(self: *ParallelScan, source: *Stream, max_rows: u32) !*PendingColumns {
        while (self.lane < self.lanes.len and source.remaining != 0) {
            const lane = &self.lanes[self.lane];
            if (lane.current == null) {
                var item: [1]*PendingColumns = undefined;
                _ = lane.queue.get(self.io, &item, 1) catch |err| switch (err) {
                    error.Closed => {
                        if (lane.failure) |failure| return failure;
                        self.lane += 1;
                        continue;
                    },
                    else => return err,
                };
                lane.current = item[0];
                lane.index = 0;
                lane.consumed_scan = 0;
                if (item[0].scan_rows == 0) try source.admitEmptyPage();
            }
            const current = lane.current.?;
            var failure: ?anyerror = null;
            while (source.skip != 0 and lane.index < current.len()) {
                try advanceScan(source, lane, current.owner.positions.items[current.begin + lane.index]);
                lane.index += 1;
                source.skip -= 1;
            }
            const begin = lane.index;
            const end = @min(current.len(), begin + @min(max_rows, source.remaining));
            while (lane.index < end) {
                advanceScan(source, lane, current.owner.positions.items[current.begin + lane.index]) catch |err| {
                    failure = err;
                    break;
                };
                if (current.owner.errors.items[current.begin + lane.index]) |err| {
                    failure = err;
                    break;
                }
                lane.index += 1;
            }
            const part = try current.view(begin, lane.index);
            source.remaining -= part.len();
            source.emitted += part.len();
            if (source.remaining != 0 and failure == null and lane.index == current.len()) {
                advanceScan(source, lane, current.scan_rows) catch |err| {
                    failure = err;
                };
                failure = failure orelse current.terminal_error;
                current.deinit();
                lane.current = null;
            }
            part.terminal_error = failure;
            part.exhausted = failure == null and source.remaining == 0;
            if (part.len() != 0 or part.exhausted) return part;
            part.deinit();
            if (failure) |err| return err;
        }
        const empty = try PendingColumns.create(self.a);
        empty.exhausted = true;
        return empty;
    }
};

pub const Stream = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    settings: ?*@import("setting_catalog.zig").View = null,
    context: runtime.Context,
    cursor: ?catalog.Cursor = null,
    spool: ?*Spool = null,
    stream_manager: ?*@import("spill.zig").Manager = null,
    fields: []const []const u8,
    request: catalog.Scan,
    after: ?[]u8 = null,
    skip: usize,
    remaining: usize,
    visited: usize = 0,
    pages: usize = 0,
    empty_scan_pages: usize = 0,
    emitted: usize = 0,
    exhausted: bool = false,
    failed: bool = false,
    pending_columns: ?*PendingColumns = null,
    pending_index: usize = 0,
    delivery_error: ?anyerror = null,
    parallel: ?*ParallelScan = null,
    parallel_checked: bool = false,
    projected_alloc: ?std.mem.Allocator = null,
    one_scan_page: bool = false,

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
        self.stream_manager = null;
        self.cursor = null;
        self.after = null;
        self.visited = 0;
        self.pages = 0;
        self.empty_scan_pages = 0;
        self.emitted = 0;
        self.failed = false;
        self.pending_columns = null;
        self.pending_index = 0;
        self.delivery_error = null;
        self.parallel = null;
        self.parallel_checked = false;
        self.projected_alloc = null;
        self.one_scan_page = false;
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
        const requested_order = if (!binding.primary_order) try describe.scanOrder(arena, binding, statement) else &.{};
        var preferred: ?catalog.Cursor = null;
        errdefer if (preferred) |cursor| cursor.close(cursor.ptr);
        if (backend.vtable.supports_scan_order and binding.aggregate == null and binding.window == null and binding.relation == null and binding.table != null and !statement.count_all and requested_order.len != 0) {
            const table = binding.table.?;
            const predicates = try self.context.conditions(table, statement.predicate);
            var needed: std.StringHashMapUnmanaged(void) = .empty;
            if (statement.columns.len == 0) {
                for (table.columns) |column| try needed.put(arena, column.path, {});
            } else for (statement.columns) |projection| {
                if (projection.expression == null and !std.mem.eql(u8, projection.field, "_id")) try needed.put(arena, (try table.column(projection.field)).path, {});
            }
            for (binding.scalars.required) |ordinal| {
                const field = binding.scalars.columns[ordinal].name;
                if (!std.mem.eql(u8, field, "_id")) try needed.put(arena, field, {});
            }
            for (requested_order) |order| if (!std.mem.eql(u8, order.column, "_id")) {
                try needed.put(arena, order.column, {});
            };
            const names = try arena.alloc([]const u8, needed.count());
            var iterator = needed.keyIterator();
            for (names) |*name| name.* = iterator.next().?.*;
            const row_goal: ?u64 = if (statement.limit != null and predicates.complete) (try self.context.count(statement.offset, 0)) +| (try self.context.count(statement.limit, 0)) else null;
            const request: catalog.Scan = .{ .row_goal = row_goal, .fields = names, .order = requested_order, .conditions = predicates.terms.items, .primary_key = predicates.primary_key, .limit = limits.page_rows };
            if (backend.vtable.open_scan) |open_scan| {
                const candidate = try open_scan(backend.ptr, self.budget.allocator(), table, request);
                if (candidate) |cursor| {
                    if (cursor.order_satisfied) preferred = cursor else cursor.close(cursor.ptr);
                }
            }
        }
        if (binding.aggregate != null or binding.window != null or binding.table == null or statement.count_all or (binding.order_keys.len != 0 and !binding.primary_order and preferred == null)) {
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
            owner.* = .{
                .manager = .{ .alloc = self.budget.allocator(), .io = backend.execution_io orelse backend.spill_manager.?.io, .context = backend.ptr, .checkpoint = backend.vtable.checkpoint, .root = limits.spill_root, .max_bytes = limits.spill_bytes, .buffer_bytes = @min(4096, @max(128, limits.retained_bytes / 512)), .max_record_bytes = @max(@as(usize, 1024), @min(@as(usize, 4 * 1024 * 1024), limits.retained_bytes / 32)) },
                .shared = backend.spill_manager,
                .a = self.budget.allocator(),
                .width = binding.columns.len,
                .rows = undefined,
            };
            const manager = owner.shared orelse &owner.manager;
            owner.rows = @import("spill.zig").Sequential.init(manager, @max(128, @min(32 * 1024, limits.retained_bytes / 64))) catch |err| {
                if (owner.shared == null) owner.manager.deinit();
                self.budget.allocator().destroy(owner);
                return err;
            };
            errdefer owner.close();
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
        for (requested_order) |order| {
            if (std.mem.eql(u8, order.column, "_id")) continue;
            const slot = try seen.getOrPut(arena, order.column);
            if (!slot.found_existing) try needed.append(arena, order.column);
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
        self.cursor = preferred;
        self.request = .{ .row_goal = if (statement.limit != null and predicates.complete) self.skip +| self.remaining else null, .order = requested_order, .fields = needed.items, .primary_order = binding.primary_order, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = limits.page_rows };
        errdefer if (self.stream_manager) |manager| {
            manager.deinit();
            self.budget.allocator().destroy(manager);
        };
        if (!self.exhausted) {
            if (binding.relation != null) {
                if (limits.spill_bytes != 0) {
                    if (backend.spill_manager) |manager| {
                        self.context.spill = manager;
                    } else if (backend.execution_io) |io| {
                        const manager = try self.budget.allocator().create(@import("spill.zig").Manager);
                        manager.* = .{ .alloc = self.budget.allocator(), .io = io, .context = backend.ptr, .checkpoint = backend.vtable.checkpoint, .root = limits.spill_root, .max_bytes = limits.spill_bytes, .buffer_bytes = @min(4096, @max(128, limits.retained_bytes / 512)), .max_record_bytes = @max(@as(usize, 1024), @min(@as(usize, 4 * 1024 * 1024), limits.retained_bytes / 32)) };
                        self.stream_manager = manager;
                        self.context.spill = manager;
                    }
                }
                self.cursor = try @import("relation_runtime.zig").openCursor(self.context);
            } else {
                if (self.cursor == null) {
                    if (backend.vtable.open_scan) |open_scan| self.cursor = try open_scan(backend.ptr, self.budget.allocator(), table, self.request);
                }
                if (self.cursor == null and !backend.pinned_statement_snapshot) return error.SqlStatementSnapshotRequired;
            }
        }
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.parallel) |pipeline| pipeline.close();
        if (self.pending_columns) |page| page.deinit();
        if (self.spool) |spool| spool.close();
        if (self.cursor) |cursor| cursor.close(cursor.ptr);
        if (self.stream_manager) |manager| {
            manager.deinit();
            self.budget.allocator().destroy(manager);
        }
        if (self.after) |after| self.budget.allocator().free(after);
        if (self.settings) |view| view.deinit();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.budget.backing.destroy(self);
    }

    pub fn next(self: *Stream, max_rows: u32) !Page {
        if (self.failed) return error.SqlStreamFailed;
        if (max_rows == 0 or max_rows > 4096) return error.InvalidSqlLimit;
        return self.pull(max_rows) catch |err| return self.fail(err);
    }

    fn fail(self: *Stream, err: anyerror) anyerror {
        self.failed = true;
        if (self.parallel) |pipeline| {
            pipeline.close();
            self.parallel = null;
        }
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
    }

    pub fn nextBatch(self: *Stream, max_rows: u32) !BatchPage {
        if (self.failed) return error.SqlStreamFailed;
        if (max_rows == 0 or max_rows > 4096) return error.InvalidSqlLimit;
        return self.pullBatch(max_rows) catch |err| return self.fail(err);
    }

    fn pullBatch(self: *Stream, max_rows: u32) !BatchPage {
        try self.context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        errdefer arena.deinit();
        if (self.spool) |spool| {
            const lease = try spool.nextLease(arena.allocator(), max_rows, self.context.limits.page_bytes);
            self.emitted += lease.values.len();
            self.exhausted = spool.index == spool.count();
            return .{ .arena = arena, .columns = self.context.binding.columns, .values = lease.values, .exhausted = self.exhausted, .spill_lease = lease };
        }
        if (!(self.pending_columns != null or self.columnsEligible())) {
            var page = try self.pull(max_rows);
            errdefer page.deinit();
            const output = try arena.allocator().create(runtime.Output);
            output.* = page.output;
            const Rows = struct {
                fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) anyerror!@import("scalar.zig").Datum {
                    const out: *runtime.Output = @ptrCast(@alignCast(raw));
                    const value = out.rows[row][column];
                    return .{ .value = value, .sql_null = if (out.sql_nulls) |flags| flags[row][column] else value == .null };
                }
            };
            return .{ .arena = arena, .columns = page.output.columns, .values = .{ .reader = .{ .ptr = output, .read = Rows.cell, .count = page.output.rows.len, .width = page.output.columns.len } }, .exhausted = page.exhausted, .row_page = page };
        }
        if (self.delivery_error) |err| return err;
        while (true) {
            if (self.pending_columns == null) {
                if (self.exhausted) return .{ .arena = arena, .columns = self.context.binding.columns, .values = .{ .rows = &.{} }, .exhausted = true };
                self.pending_columns = try self.executeColumns(self.context.limits.executionRows());
                self.pending_index = 0;
                self.exhausted = false;
            }
            const pending = self.pending_columns.?;
            if (self.pending_index == pending.len()) {
                if (pending.terminal_error) |err| return err;
                self.exhausted = pending.exhausted;
                pending.deinit();
                self.pending_columns = null;
                continue;
            }
            const begin = self.pending_index;
            var end = begin;
            var bytes: usize = 0;
            while (end < pending.len() and end - begin < max_rows) {
                for (0..self.context.binding.columns.len) |column| bytes +|= try @import("operators.zig").datumBytes(try pending.cell(arena.allocator(), end, column));
                end += 1;
                if (bytes >= self.context.limits.page_bytes) break;
            }
            const lease = try pending.view(begin, end);
            self.pending_index = end;
            if (end == pending.len() and pending.terminal_error == null) {
                self.exhausted = pending.exhausted;
                pending.deinit();
                self.pending_columns = null;
            }
            return .{ .arena = arena, .columns = self.context.binding.columns, .values = .{ .retained = .{ .store = lease.values, .begin = lease.begin, .count = lease.len() } }, .exhausted = self.exhausted, .lease = lease };
        }
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
            // Cursor.next owns the scalar delivery boundary, including the
            // release policy after an earlier batch leased sorted rows.
            const values = (try spool.next(a)) orelse return error.InvalidSqlSpill;
            out.* = try a.alloc(Json, values.len);
            bits.* = try a.alloc(bool, values.len);
            for (values, @constCast(out.*), @constCast(bits.*)) |value, *cell, *flag| {
                cell.* = value.value;
                flag.* = value.sql_null;
                bytes +|= try @import("operators.zig").datumBytes(value);
            }
            read += 1;
            if (bytes >= self.context.limits.page_bytes) break;
        }
        self.emitted += read;
        self.exhausted = spool.index == spool.count();
        return .{ .arena = arena, .exhausted = self.exhausted, .output = .{ .columns = self.context.binding.columns, .rows = rows[0..read], .sql_nulls = flags[0..read], .command_tag = "SELECT" } };
    }
    fn columnProgram(self: *Stream, a: std.mem.Allocator, program: *const @import("scalar.zig").Program, page: catalog.ColumnPage) ![]const @import("scalar.zig").Datum {
        if (try @import("vector_eval.zig").evaluateDictionaryColumns(a, program, page, self.context.binding.scalars.columns, self.context.parameters)) |encoded| {
            const values = try a.alloc(@import("scalar.zig").Datum, encoded.len());
            for (values, 0..) |*value, index| value.* = try encoded.cell(a, index, 0);
            return values;
        }
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
    fn scanCapacity(self: *const Stream) usize {
        return @min(self.context.limits.executionRows(), @max(@as(usize, 1), self.context.limits.retained_bytes / (16 * 1024 + self.fields.len * @sizeOf(@import("scalar.zig").Datum) * 16)));
    }
    // Native pages are logical execution-sized work units. Splitting a range
    // or stopping a physical page at a row-group boundary cannot change quota.
    // Zero-progress backend pulls still cost one unit to bound empty loops.
    fn admitRows(self: *Stream, count: usize) !void {
        if (count > self.context.limits.scan_rows -| self.visited) return error.SqlProgramLimitExceeded;
        const visited = self.visited + count;
        const pages = visited / self.scanCapacity() + @intFromBool(visited % self.scanCapacity() != 0) +| self.empty_scan_pages;
        if (pages > self.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
        self.visited = visited;
        self.pages = pages;
    }
    fn admitEmptyPage(self: *Stream) !void {
        if (self.pages >= self.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
        self.empty_scan_pages += 1;
        self.pages += 1;
    }
    fn executeColumns(self: *Stream, max_rows: u32) !*PendingColumns {
        if (!self.parallel_checked) {
            self.parallel_checked = true;
            self.parallel = try ParallelScan.start(self);
        }
        if (self.parallel) |pipeline| {
            const pending = try pipeline.pull(self, max_rows);
            if (pending.exhausted) {
                pipeline.close();
                self.parallel = null;
                self.exhausted = true;
                if (self.cursor) |cursor| cursor.close(cursor.ptr);
                self.cursor = null;
            }
            return pending;
        }
        const pending = try PendingColumns.create(self.projected_alloc orelse self.budget.allocator());
        errdefer pending.deinit();
        self.fillColumns(pending, max_rows) catch |err| {
            if (pending.values.failed or pending.len() == 0) return err;
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
        while (!self.exhausted and pending.len() < max_rows) {
            try self.context.checkpoint();
            var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer scratch.deinit();
            const a = scratch.allocator();
            const capacity = self.scanCapacity();
            const credit = (self.context.limits.scan_pages -| self.empty_scan_pages) *| capacity -| self.visited;
            const wanted: u32 = @intCast(@min(@min(capacity, @min(credit, self.context.limits.scan_rows -| self.visited)), @min(self.remaining, max_rows - pending.len()) +| self.skip));
            if (wanted == 0) return error.SqlProgramLimitExceeded;
            const cursor = self.cursor.?;
            const page = try cursor.next_columns.?(cursor.ptr, a, wanted);
            if (page.selection.len > wanted) return error.InvalidSqlBackendResponse;
            if (page.selection.len == 0) try self.admitEmptyPage() else try self.admitRows(page.selection.len);
            const before = pending.len();
            const bytes = self.appendColumnPage(pending, a, page) catch |err| switch (err) {
                error.SqlDivisionByZero, error.SqlNumericOutOfRange, error.SqlTypeMismatch, error.SqlProgramLimitExceeded, error.InvalidSqlDateTime, error.SqlCardinalityViolation => if (pending.len() == before) try self.appendScalarColumnPage(pending, a, page) else return err,
                else => return err,
            };
            output_bytes +|= bytes;
            self.exhausted = self.remaining == 0 or page.after == null;

            if (self.one_scan_page or output_bytes >= @min(self.context.limits.page_bytes, @max(@as(usize, 1024), self.context.limits.retained_bytes / 16))) break;
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
        var positions: std.ArrayList(usize) = .empty;
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
            if (self.one_scan_page) try positions.append(a, index + 1);
        }
        var selected = page;
        selected.selection = selection.items;
        const projections = try a.alloc(?@import("execution_batch.zig").Batch, self.fields.len);
        @memset(projections, null);
        var programs: std.ArrayList(*const @import("scalar.zig").Program) = .empty;
        var slots: std.ArrayList(usize) = .empty;
        for (self.context.binding.scalars.projections, 0..) |*optional, index| if (optional.*) |*program| {
            try programs.append(a, program);
            try slots.append(a, index);
        };
        const evaluated = try @import("vector_eval.zig").evaluateColumnsEncodedMany(a, programs.items, selected, self.context.binding.scalars.columns, self.context.parameters, self.context.backend.execution_io);
        for (evaluated, programs.items, slots.items) |values, program, index| {
            if (values) |batch| {
                projections[index] = batch;
            } else {
                const vector = try self.columnProgram(a, program, selected);
                const vectors = try a.alloc([]const @import("scalar.zig").Datum, 1);
                vectors[0] = vector;
                projections[index] = .{ .vectors = .{ .values = vectors, .count = vector.len } };
            }
        }
        const failures = try a.alloc(?anyerror, selection.items.len);
        @memset(failures, null);
        const Projection = struct {
            stream: *Stream,
            page: catalog.ColumnPage,
            projections: []const ?@import("execution_batch.zig").Batch,
            failures: []?anyerror,
            fn dictionary(raw: *anyopaque, alloc: std.mem.Allocator, ordinal: usize) anyerror!?@import("execution_batch.zig").Batch {
                const projection_: *@This() = @ptrCast(@alignCast(raw));
                const source = projection_.projections[ordinal] orelse blk: {
                    const definition = [_]@import("scalar.zig").Column{.{ .name = projection_.stream.fields[ordinal], .type = projection_.stream.context.binding.columns[ordinal].type }};
                    const direct: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = projection_.page, .definitions = &definition } };
                    break :blk (direct.dictionaryColumn(alloc, 0) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        return null;
                    }) orelse return null;
                };
                if (source != .dictionary) return null;
                const values = try alloc.dupe(@import("scalar.zig").Datum, source.dictionary.values);
                for (values) |*value| {
                    value.value = describe.coerceAlloc(alloc, value.value, projection_.stream.context.binding.columns[ordinal].type) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        // Scalar delivery records the precise failing row.
                        return null;
                    };
                }
                return .{ .dictionary = .{ .values = values, .indices = source.dictionary.indices } };
            }
            fn cell(raw: *anyopaque, alloc: std.mem.Allocator, index: usize, ordinal: usize) anyerror!@import("scalar.zig").Datum {
                const projection_: *@This() = @ptrCast(@alignCast(raw));
                const cell_ = if (projection_.projections[ordinal]) |values| try values.cell(alloc, index, 0) else blk: {
                    const raw_ = try projection_.page.cell(alloc, index, projection_.stream.fields[ordinal]);
                    break :blk @import("scalar.zig").Datum{ .value = raw_.value, .sql_null = raw_.sql_null, .patterns = raw_.patterns };
                };
                const coerced = describe.coerceAlloc(alloc, cell_.value, projection_.stream.context.binding.columns[ordinal].type) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    projection_.failures[index] = projection_.failures[index] orelse err;
                    return .{};
                };
                const value: @import("scalar.zig").Datum = .{ .value = coerced, .sql_null = cell_.sql_null, .patterns = cell_.patterns };
                return value;
            }
        };
        var projection: Projection = .{ .stream = self, .page = selected, .projections = projections, .failures = failures };
        // Both metadata arrays are admitted before column state changes. A
        // complete batch is published only after all typed columns succeed.
        if (self.one_scan_page) try pending.reserveRecords(selection.items.len);
        const before = pending.len();
        try pending.values.appendBatch(.{ .reader = .{ .ptr = &projection, .read = Projection.cell, .read_dictionary = Projection.dictionary, .count = selection.items.len, .width = self.fields.len } });
        if (self.one_scan_page) for (positions.items, failures) |position, failure| pending.record(position, failure);
        for (before..pending.len()) |row| for (0..self.fields.len) |column| {
            output_bytes +|= try @import("operators.zig").datumBytes(try pending.values.cell(a, row, column));
        };
        if (!self.one_scan_page) for (failures, 0..) |failure, index| if (failure) |err| {
            pending.end = before + index;
            self.remaining -= index;
            self.emitted += index;
            return err;
        };
        self.remaining -= selection.items.len;
        self.emitted += selection.items.len;
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
            var one = page;
            one.selection = &.{physical};
            var projection_error: ?anyerror = null;
            const row = try a.alloc(@import("scalar.zig").Datum, self.fields.len);
            for (self.fields, self.context.binding.columns, row, 0..) |field, column, *value, ordinal| {
                const cell = if (self.context.binding.scalars.projections[ordinal]) |program|
                    self.context.evaluate(a, program, cells) catch |err| blk: {
                        if (!self.one_scan_page or err == error.OutOfMemory or err == error.QueryCanceled or err == error.DeadlineExceeded) return err;
                        projection_error = projection_error orelse err;
                        break :blk @import("scalar.zig").Datum{};
                    }
                else blk: {
                    const raw = try one.cell(a, 0, field);
                    break :blk @import("scalar.zig").Datum{ .value = raw.value, .sql_null = raw.sql_null };
                };
                value.* = .{ .value = describe.coerceAlloc(a, cell.value, column.type) catch |err| blk: {
                    if (!self.one_scan_page or err == error.OutOfMemory) return err;
                    projection_error = projection_error orelse err;
                    break :blk Json.null;
                }, .sql_null = cell.sql_null };
                output_bytes +|= try @import("operators.zig").datumBytes(value.*);
            }
            if (self.one_scan_page) try pending.reserveRecords(1);
            _ = try pending.values.append(row);
            if (self.one_scan_page) pending.record(index + 1, projection_error);
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
                self.pending_columns = self.executeColumns(self.context.limits.executionRows()) catch |err| {
                    if (rows.items.len == 0) return err;
                    self.delivery_error = err;
                    if (self.parallel) |pipeline| {
                        pipeline.close();
                        self.parallel = null;
                    }
                    if (self.cursor) |cursor| cursor.close(cursor.ptr);
                    self.cursor = null;
                    break;
                };
                self.pending_index = 0;
                // Execution may have reached EOF while delivery still has rows.
                self.exhausted = false;
            }
            const pending = self.pending_columns.?;
            if (self.pending_index == pending.len()) if (pending.terminal_error) |err| {
                if (rows.items.len != 0) break;
                return err;
            };
            while (self.pending_index < pending.len() and rows.items.len < max_rows) {
                const index = self.pending_index;
                const row = try a.alloc(Json, self.context.binding.columns.len);
                const nulls = try a.alloc(bool, self.context.binding.columns.len);
                for (row, nulls, 0..) |*out, *sql_null, column| {
                    const datum = try pending.cell(a, index, column);
                    sql_null.* = datum.sql_null;
                    out.* = (try @import("operators.zig").cloneDatum(a, datum)).value;
                    bytes +|= try @import("operators.zig").datumBytes(datum);
                }
                try rows.append(a, row);
                try flags.append(a, nulls);
                self.pending_index += 1;
                if (bytes >= self.context.limits.page_bytes) break;
            }
            if (self.pending_index == pending.len() and pending.terminal_error == null) {
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
    for ([_]bool{ false, true }) |typed| for ([_][]const u8{
        "SELECT n FROM docs ORDER BY n DESC",
        "SELECT n % 37 AS k, count(*) AS c FROM docs GROUP BY n % 37 ORDER BY k",
        "SELECT n, row_number() OVER (ORDER BY n DESC) AS r FROM docs ORDER BY n DESC",
    }, [_]usize{ 1000, 37, 1000 }) |sql, expected| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .count = 1000 };
        var backend = fixture.backend();
        backend.execution_io = native_platform.testing.io;
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
            if (typed) {
                var result = try stream.nextBatch(7);
                defer result.deinit();
                try std.testing.expect(result.values == .rows or result.values == .reader);
                try std.testing.expect(result.values.len() <= 7);
                for (0..result.values.len()) |row| {
                    if (expected == 1000) try std.testing.expectEqual(@as(i64, @intCast(999 - seen)), (try result.values.cell(result.arena.allocator(), row, 0)).value.integer);
                    seen += 1;
                }
                if (result.exhausted) break;
                continue;
            }
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
    };
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

const OrderedColumns = struct {
    children: [2]Fixture = .{ .{ .count = 5000 }, .{ .offset = 5000, .count = 10000 } },
    splits: usize = 0,
    closes: usize = 0,
    fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        return .{ .ptr = raw, .next = Fixture.next, .next_columns = unreachableColumns, .close = close, .split_ordered = split, .estimated_rows = 10000 };
    }
    fn unreachableColumns(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.ColumnPage {
        return error.UnexpectedSerialScan;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.closes += 1;
    }
    fn split(raw: *anyopaque, a: std.mem.Allocator, _: usize) !?[]catalog.Cursor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const cursors = try a.alloc(catalog.Cursor, 2);
        for (&self.children, cursors) |*child, *cursor| cursor.* = (try NativeColumns.open(child, a, undefined, .{ .fields = &.{}, .limit = 4096 })).?;
        self.splits += 1;
        return cursors;
    }
};
test "SQL ordered parallel scan evaluates complete pipelines with bounded delivery" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "SELECT n + 1 FROM docs WHERE n % 2 = 0", "SELECT n + 1 FROM docs", "SELECT 1 / (9999 - n) FROM docs" }) |sql| {
        var fixture: OrderedColumns = .{};
        var backend = Fixture.backend(undefined);
        var vtable = backend.vtable.*;
        vtable.open_scan = OrderedColumns.open;
        vtable.checkpoint = struct {
            fn check(_: *anyopaque) !void {}
        }.check;
        backend.ptr = &fixture;
        backend.vtable = &vtable;
        backend.execution_io = native_platform.testing.io;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
        defer stream.close();
        var seen: usize = 0;
        var failed = false;
        while (true) {
            var page = stream.next(137) catch |err| {
                try std.testing.expectEqual(error.SqlDivisionByZero, err);
                failed = true;
                break;
            };
            defer page.deinit();
            for (page.output.rows) |row| {
                if (!std.mem.startsWith(u8, sql, "SELECT 1 /")) try std.testing.expectEqual(@as(i64, @intCast(if (std.mem.indexOf(u8, sql, "WHERE") != null) seen * 2 + 1 else seen + 1)), row[0].integer);
                seen += 1;
            }
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.splits);
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        for (fixture.children) |child| try std.testing.expectEqual(@as(usize, 1), child.closed);
        try std.testing.expectEqual(@as(usize, if (failed) 9999 else if (std.mem.indexOf(u8, sql, "WHERE") != null) 5000 else 10000), seen);
        try std.testing.expect(stream.budget.peak <= stream.budget.limit);
    }
}

test "SQL ordered parallel scan admits bounded limits without charging speculative ranges" {
    const a = std.testing.allocator;
    var fixture: OrderedColumns = .{};
    var backend = Fixture.backend(undefined);
    var vtable = backend.vtable.*;
    vtable.open_scan = OrderedColumns.open;
    vtable.checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check;
    backend.ptr = &fixture;
    backend.vtable = &vtable;
    backend.execution_io = native_platform.testing.io;
    var compiled = try compiler.compile(a, "SELECT n FROM docs LIMIT 8192", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .scan_rows = 8192 })).?;
    defer stream.close();
    var count: usize = 0;
    while (true) {
        var page = try stream.next(137);
        defer page.deinit();
        for (page.output.rows) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(count)), row[0].integer);
            count += 1;
        }
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 8192), count);
    try std.testing.expectEqual(@as(usize, 8192), stream.visited);
    try std.testing.expectEqual(@as(usize, 1), fixture.splits);
}

test "SQL ordered block admission preserves offsets filtered progress and deferred failures" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, count: usize, scan_rows: usize, failure: ?anyerror = null };
    for ([_]Case{
        .{ .sql = "SELECT n FROM docs LIMIT 8192 OFFSET 1000", .count = 8192, .scan_rows = 9192 },
        .{ .sql = "SELECT 1 / (n - 1) FROM docs LIMIT 8192 OFFSET 2", .count = 8192, .scan_rows = 8194 },
        .{ .sql = "SELECT 1 / (9500 - n) FROM docs LIMIT 8192 OFFSET 1000", .count = 8192, .scan_rows = 9192 },
        .{ .sql = "SELECT 1 / (9000 - n) FROM docs LIMIT 8192 OFFSET 1000", .count = 8000, .scan_rows = 10000, .failure = error.SqlDivisionByZero },
        .{ .sql = "SELECT n FROM docs WHERE n % 10000 >= 5000 LIMIT 8192 OFFSET 2", .count = 4998, .scan_rows = 10000 },
        .{ .sql = "SELECT n FROM docs WHERE n % 2 = 0 LIMIT 8192", .count = 4500, .scan_rows = 9000, .failure = error.SqlProgramLimitExceeded },
        .{ .sql = "SELECT n FROM docs WHERE 1 / (1 - n) > 0 LIMIT 8192 OFFSET 2", .count = 0, .scan_rows = 10000, .failure = error.SqlDivisionByZero },
    }) |case| {
        var fixture: OrderedColumns = .{};
        var backend = Fixture.backend(undefined);
        var vtable = backend.vtable.*;
        vtable.open_scan = OrderedColumns.open;
        vtable.checkpoint = struct {
            fn check(_: *anyopaque) !void {}
        }.check;
        backend.ptr = &fixture;
        backend.vtable = &vtable;
        backend.execution_io = std.testing.io;
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .scan_rows = case.scan_rows })).?;
        defer stream.close();
        var count: usize = 0;
        var failure: ?anyerror = null;
        while (true) {
            var page = stream.next(137) catch |err| {
                failure = err;
                break;
            };
            defer page.deinit();
            if (std.mem.eql(u8, case.sql, "SELECT n FROM docs LIMIT 8192 OFFSET 1000")) for (page.output.rows, count..) |row, index| try std.testing.expectEqual(@as(i64, @intCast(index + 1000)), row[0].integer);
            count += page.output.rows.len;
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(case.failure, failure);
        try std.testing.expectEqual(case.count, count);
        try std.testing.expect(stream.visited <= case.scan_rows);
        try std.testing.expectEqual(@as(usize, 1), fixture.splits);
    }
}

fn ownedBlockScenario(a: std.mem.Allocator) !void {
    const root = try PendingColumns.create(a);
    var root_owned = true;
    defer if (root_owned) root.deinit();
    const credit = try PendingColumns.Credit.create(a, std.testing.io);
    try std.testing.expect(try credit.acquire());
    root.credit(credit);
    // Joining/stopping a producer releases its reference before delivery.
    credit.stop();
    credit.release();
    _ = try root.values.append(&.{@import("scalar.zig").Datum.json(.{ .string = "retained after producer exit" })});
    _ = try root.values.append(&.{@import("scalar.zig").Datum.json(.{ .integer = 9007199254740993 })});
    const first = try root.view(0, 1);
    defer first.deinit();
    const second = try root.view(1, 2);
    defer second.deinit();
    root.deinit();
    root_owned = false;
    try std.testing.expectEqualStrings("retained after producer exit", (try first.cell(a, 0, 0)).value.string);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try second.cell(a, 0, 0)).value.integer);
}
test "SQL transferred column views survive producer teardown and allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ownedBlockScenario, .{});
}

fn transferBenchmark(copy: bool, root: *PendingColumns) !struct { ns: i96, checksum: i64 } {
    const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var checksum: i64 = 0;
    for (0..16) |_| {
        const page = if (copy) blk: {
            const result = try PendingColumns.create(std.testing.allocator);
            errdefer result.deinit();
            var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer scratch.deinit();
            for (0..root.len()) |row| {
                _ = scratch.reset(.retain_capacity);
                _ = try result.values.append(try root.values.row(scratch.allocator(), row));
            }
            break :blk result;
        } else try root.view(0, root.len());
        defer page.deinit();
        for (0..page.len()) |row| checksum += (try page.cell(std.testing.allocator, row, 0)).value.integer;
    }
    return .{ .ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start, .checksum = checksum };
}
test "native pipeline refinements benchmark owned column transfer" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const root = try PendingColumns.create(std.testing.allocator);
    defer root.deinit();
    var values: [8]@import("scalar.zig").Datum = undefined;
    const payload: [384]u8 = @splat('x');
    @memset(values[1..], @import("scalar.zig").Datum.json(.{ .string = &payload }));
    for (0..4096) |index| {
        values[0] = @import("scalar.zig").Datum.json(.{ .integer = @intCast(index) });
        _ = try root.values.append(&values);
    }
    for (0..3) |sample| {
        const first = try transferBenchmark(sample % 2 != 0, root);
        const second = try transferBenchmark(sample % 2 == 0, root);
        const copied = if (sample % 2 == 0) second else first;
        const transferred = if (sample % 2 == 0) first else second;
        try std.testing.expectEqual(copied.checksum, transferred.checksum);
        std.debug.print("native_refinement {{\"case\":\"owned_column_transfer\",\"rows\":4096,\"width\":8,\"repeats\":16,\"sample\":{d},\"copy_ns\":{d},\"transfer_ns\":{d}}}\n", .{ sample, copied.ns, transferred.ns });
    }
}

fn workerMetadataScenario(a: std.mem.Allocator) !void {
    for ([_][]const u8{ "SELECT n FROM docs", "SELECT 1 / (128 - n) FROM docs" }) |sql| {
        var fixture: Fixture = .{ .count = 256 };
        var backend = fixture.backend();
        var vtable = backend.vtable.*;
        vtable.open_scan = NativeColumns.open;
        backend.vtable = &vtable;
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
        defer stream.close();
        stream.parallel_checked = true;
        stream.one_scan_page = true;
        const pending = try stream.executeColumns(256);
        defer pending.deinit();
        try std.testing.expectEqual(pending.len(), pending.owner.positions.items.len);
        try std.testing.expectEqual(pending.len(), pending.owner.errors.items.len);
        for (0..pending.len()) |row| _ = try pending.cell(a, row, 0);
        if (pending.terminal_error) |err| if (err == error.OutOfMemory) return err;
    }
}
test "SQL native worker metadata admits rows atomically across allocation failures" {
    try workerMetadataScenario(std.testing.allocator);
    // Arena resizing depends on heap placement. Force allocation on growth so
    // the fault sweep visits the same allocation sequence on every attempt.
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), workerMetadataScenario, .{});
}

const ManyOrderedColumns = struct {
    parent: Fixture = .{ .count = 10000 },
    children: [16]Fixture = undefined,
    splits: usize = 0,
    closes: usize = 0,
    fn open(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        return .{ .ptr = raw, .next = Fixture.next, .next_columns = nextColumns, .close = close, .split_ordered = split, .estimated_rows = 10000 };
    }
    fn nextColumns(raw: *anyopaque, a: std.mem.Allocator, wanted: u32) !catalog.ColumnPage {
        const fixture: *@This() = @ptrCast(@alignCast(raw));
        return NativeColumns.nextColumns(&fixture.parent, a, wanted);
    }
    fn close(raw: *anyopaque) void {
        const fixture: *@This() = @ptrCast(@alignCast(raw));
        fixture.closes += 1;
    }
    fn split(raw: *anyopaque, a: std.mem.Allocator, maximum: usize) !?[]catalog.Cursor {
        const fixture: *@This() = @ptrCast(@alignCast(raw));
        const count = @min(maximum, fixture.children.len);
        const cursors = try a.alloc(catalog.Cursor, count);
        for (fixture.children[0..count], cursors, 0..) |*child, *cursor, index| {
            child.* = .{ .offset = index * 10000 / count, .count = (index + 1) * 10000 / count };
            cursor.* = (try NativeColumns.open(child, a, undefined, .{ .fields = &.{}, .limit = 4096 })).?;
        }
        fixture.splits = count;
        return cursors;
    }
};
test "SQL native page quotas survive ordered task fragmentation and delivery size" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |parallel| for ([_]u32{ 17, 137 }) |delivery| {
        var fixture: ManyOrderedColumns = .{};
        var backend = Fixture.backend(undefined);
        var vtable = backend.vtable.*;
        vtable.open_scan = ManyOrderedColumns.open;
        vtable.checkpoint = struct {
            fn check(_: *anyopaque) !void {}
        }.check;
        backend.ptr = &fixture;
        backend.vtable = &vtable;
        backend.execution_io = if (parallel) std.testing.io else null;
        var compiled = try compiler.compile(a, "SELECT n FROM docs LIMIT 8192", .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .scan_pages = 3 })).?;
        defer stream.close();
        var count: usize = 0;
        while (true) {
            var page = try stream.next(delivery);
            defer page.deinit();
            for (page.output.rows, count..) |row, index| try std.testing.expectEqual(@as(i64, @intCast(index)), row[0].integer);
            count += page.output.rows.len;
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(@as(usize, 8192), count);
        try std.testing.expectEqual(@as(usize, 3), stream.pages);
        try std.testing.expectEqual(@as(usize, 8192), stream.visited);
        try std.testing.expectEqual(@as(usize, if (parallel) 16 else 0), fixture.splits);
    };
}

const QueryBenchAllocator = struct {
    count: std.atomic.Value(usize) = .init(0),
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const counter: *@This() = @ptrCast(@alignCast(raw));
        const value = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        _ = counter.count.fetchAdd(1, .monotonic);
        return value;
    }
    fn resize(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        return std.testing.allocator.rawResize(bytes, alignment, len, ra);
    }
    fn remap(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        return std.testing.allocator.rawRemap(bytes, alignment, len, ra);
    }
    fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        std.testing.allocator.rawFree(bytes, alignment, ra);
    }
};
const QueryBenchBackend = struct {
    wide_rows: usize = 100000,
    scans: [2]ManyOrderedColumns = .{ .{}, .{} },
    opened: usize = 0,
    fn open(raw: *anyopaque, a: std.mem.Allocator, table: catalog.Table, scan_: catalog.Scan) !?catalog.Cursor {
        const fixture: *@This() = @ptrCast(@alignCast(raw));
        const source = &fixture.scans[fixture.opened];
        fixture.opened += 1;
        var cursor = (try ManyOrderedColumns.open(source, a, table, scan_)).?;
        cursor.next_columns = columns;
        return cursor;
    }
    fn columns(raw: *anyopaque, a: std.mem.Allocator, wanted: u32) !catalog.ColumnPage {
        const fixture: *ManyOrderedColumns = @ptrCast(@alignCast(raw));
        var page = try ManyOrderedColumns.nextColumns(raw, a, wanted);
        if (page.after != null) page.after = try std.fmt.allocPrint(a, "{d}", .{fixture.parent.offset});
        return page;
    }
    fn check(_: *anyopaque) !void {}
    fn wideResolve(_: *anyopaque, _: std.mem.Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{
            .{ .name = "n", .path = "n", .type = .integer },
            .{ .name = "c1", .path = "c1", .type = .integer },
            .{ .name = "c2", .path = "c2", .type = .integer },
            .{ .name = "c3", .path = "c3", .type = .integer },
            .{ .name = "c4", .path = "c4", .type = .integer },
            .{ .name = "c5", .path = "c5", .type = .integer },
            .{ .name = "c6", .path = "c6", .type = .integer },
            .{ .name = "c7", .path = "c7", .type = .integer },
        } };
    }
    const Wide = struct {
        a: std.mem.Allocator,
        fixture: Fixture = .{ .count = 100000 },
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return error.UnexpectedScalarScan;
        }
        fn columns(raw: *anyopaque, a: std.mem.Allocator, wanted: u32) !catalog.ColumnPage {
            const cursor: *@This() = @ptrCast(@alignCast(raw));
            var page = try NativeColumns.nextColumns(&cursor.fixture, a, wanted);
            const values = try a.alloc(@import("../storage/rowsource/types.zig").ColumnVector, 8);
            for (values, [_][]const u8{ "n", "c1", "c2", "c3", "c4", "c5", "c6", "c7" }) |*column, name| column.* = .{ .name = name, .values = page.batch.columns[0].values };
            page.batch.columns = values;
            if (page.after != null) page.after = try std.fmt.allocPrint(a, "{d}", .{cursor.fixture.offset});
            return page;
        }
        fn close(raw: *anyopaque) void {
            const cursor: *@This() = @ptrCast(@alignCast(raw));
            cursor.a.destroy(cursor);
        }
    };
    fn wideOpen(raw: *anyopaque, a: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        const owner = try a.create(Wide);
        const fixture: *QueryBenchBackend = @ptrCast(@alignCast(raw));
        owner.* = .{ .a = a, .fixture = .{ .count = fixture.wide_rows } };
        return .{ .ptr = owner, .next = Wide.next, .next_columns = Wide.columns, .close = Wide.close, .estimated_rows = 100000 };
    }
    const Statement = struct {
        a: std.mem.Allocator,
        cursors: []catalog.Cursor,
        fn close(raw: *anyopaque) void {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            for (owner.cursors) |cursor| cursor.close(cursor.ptr);
            owner.a.free(owner.cursors);
            owner.a.destroy(owner);
        }
    };
    fn wideStatement(raw: *anyopaque, a: std.mem.Allocator, requests: []const catalog.StatementScan) !catalog.StatementRead {
        const owner = try a.create(Statement);
        errdefer a.destroy(owner);
        const cursors = try a.alloc(catalog.Cursor, requests.len);
        errdefer a.free(cursors);
        var opened: usize = 0;
        errdefer for (cursors[0..opened]) |cursor| cursor.close(cursor.ptr);
        for (requests, cursors) |request, *cursor| {
            cursor.* = (try wideOpen(raw, a, request.table, request.request)).?;
            opened += 1;
        }
        owner.* = .{ .a = a, .cursors = cursors };
        return .{ .ptr = owner, .cursors = cursors, .close = Statement.close };
    }
};
fn completeQueryBenchmark(sql: []const u8, expected: usize, retained: usize, wide: bool) !struct { ns: i96, allocations: usize, peak: usize, checksum: i64 } {
    var counter: QueryBenchAllocator = .{};
    const a = counter.allocator();
    var fixture: QueryBenchBackend = .{};
    var backend = Fixture.backend(undefined);
    var vtable = backend.vtable.*;
    vtable.open_scan = if (wide) QueryBenchBackend.wideOpen else QueryBenchBackend.open;
    if (wide) {
        vtable.resolve = QueryBenchBackend.wideResolve;
        vtable.open_statement = QueryBenchBackend.wideStatement;
    }
    vtable.checkpoint = QueryBenchBackend.check;
    backend.ptr = &fixture;
    backend.vtable = &vtable;
    backend.execution_io = std.testing.io;
    backend.pinned_statement_snapshot = true;
    var compiled = try compiler.compile(a, sql, .{});
    defer compiled.deinit();
    const before = counter.count.load(.monotonic);
    const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .retained_bytes = retained })).?;
    defer stream.close();
    var count: usize = 0;
    var checksum: i64 = 0;
    while (true) {
        var page = try stream.next(137);
        defer page.deinit();
        for (page.output.rows) |row| {
            for (row) |value| checksum += value.integer;
            count += 1;
        }
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(expected, count);
    if (wide) {
        // A one-row result spool is tiny; this confirms the join itself spilled.
        const spool = stream.spool.?;
        const manager = spool.shared orelse &spool.manager;
        try std.testing.expect(manager.written_bytes > 1024 * 1024);
    }
    return .{ .ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start, .allocations = counter.count.load(.monotonic) - before, .peak = stream.budget.peak, .checksum = checksum };
}
test "native pipeline refinements benchmark complete queries" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const Case = struct { name: []const u8, sql: []const u8, rows: usize, bytes: usize = 64 * 1024 * 1024, wide: bool = false };
    for ([_]Case{
        .{ .name = "query_narrow_limit", .sql = "SELECT n FROM docs LIMIT 8192 OFFSET 1000", .rows = 8192 },
        .{ .name = "query_wide_projection", .sql = "SELECT n, n + 1, n * 2, n % 7, n + 2, n - 1, n / 2, n + 3 FROM docs LIMIT 8192 OFFSET 1000", .rows = 8192 },
        .{ .name = "query_group_projection", .sql = "SELECT n % 100 AS bucket, COUNT(*), SUM(n), SUM(n + 1) FROM docs GROUP BY n % 100 ORDER BY bucket", .rows = 100 },
        .{ .name = "query_spilled_join", .sql = "SELECT SUM(a.n), SUM(a.c1), SUM(a.c2), SUM(a.c3), SUM(a.c4), SUM(a.c5), SUM(a.c6), SUM(a.c7), SUM(b.n), SUM(b.c1), SUM(b.c2), SUM(b.c3), SUM(b.c4), SUM(b.c5), SUM(b.c6), SUM(b.c7) FROM docs a JOIN docs b ON a.n = b.n", .rows = 1, .bytes = 16 * 1024 * 1024, .wide = true },
    }) |case| {
        var checksum: ?i64 = null;
        for (0..3) |sample| {
            const result = completeQueryBenchmark(case.sql, case.rows, case.bytes, case.wide) catch |err| {
                std.debug.print("query benchmark {s}: {any}\n", .{ case.name, err });
                return err;
            };
            if (checksum) |prior| try std.testing.expectEqual(prior, result.checksum);
            checksum = result.checksum;
            std.debug.print("native_refinement {{\"case\":\"{s}\",\"rows\":{d},\"sample\":{d},\"ns\":{d},\"allocations\":{d},\"peak_bytes\":{d},\"checksum\":{d}}}\n", .{ case.name, case.rows, sample, result.ns, result.allocations, result.peak, result.checksum });
        }
    }
}
test "SQL ordered scan fanout reserves workspace before cloning provider metadata" {
    const a = std.testing.allocator;
    var fixture: ManyOrderedColumns = .{};
    var backend = Fixture.backend(undefined);
    var vtable = backend.vtable.*;
    vtable.open_scan = ManyOrderedColumns.open;
    vtable.checkpoint = QueryBenchBackend.check;
    backend.ptr = &fixture;
    backend.vtable = &vtable;
    backend.execution_io = std.testing.io;
    var compiled = try compiler.compile(a, "SELECT n FROM docs LIMIT 8192", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
    defer stream.close();
    stream.cursor.?.ordered_split_bytes = 64 * 1024 * 1024;
    try std.testing.expectEqual(null, try ParallelScan.start(stream));
    try std.testing.expectEqual(@as(usize, 0), fixture.splits);
    try std.testing.expectEqual(@as(usize, 0), fixture.parent.offset);
}

test "SQL streaming relational columns preserve successful prefixes before residual errors" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |bounded| {
        var fixture: QueryBenchBackend = .{ .wide_rows = 16 };
        var backend = Fixture.backend(undefined);
        var vtable = backend.vtable.*;
        vtable.resolve = QueryBenchBackend.wideResolve;
        vtable.open_scan = QueryBenchBackend.wideOpen;
        vtable.open_statement = QueryBenchBackend.wideStatement;
        vtable.checkpoint = QueryBenchBackend.check;
        backend.ptr = &fixture;
        backend.vtable = &vtable;
        backend.execution_io = std.testing.io;
        backend.pinned_statement_snapshot = true;
        var compiled = try compiler.compile(a, if (bounded) "SELECT a.n FROM docs a JOIN docs b ON a.n = b.n AND 1 / (5 - a.n) >= 0 LIMIT 5" else "SELECT a.n FROM docs a JOIN docs b ON a.n = b.n AND 1 / (5 - a.n) >= 0", .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
        defer stream.close();
        var count: usize = 0;
        while (count < 5) {
            var page = try stream.next(2);
            defer page.deinit();
            for (page.output.rows, count..) |row, index| try std.testing.expectEqual(@as(i64, @intCast(index)), row[0].integer);
            count += page.output.rows.len;
            try std.testing.expect(page.output.rows.len != 0);
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(@as(usize, 5), count);
        if (!bounded) try std.testing.expectError(error.SqlDivisionByZero, stream.next(2));
    }
}

test "SQL streaming joins own bounded spill state through paged delivery" {
    const a = std.testing.allocator;
    var fixture: QueryBenchBackend = .{ .wide_rows = 8000 };
    var backend = Fixture.backend(undefined);
    var vtable = backend.vtable.*;
    vtable.resolve = QueryBenchBackend.wideResolve;
    vtable.open_scan = QueryBenchBackend.wideOpen;
    vtable.open_statement = QueryBenchBackend.wideStatement;
    vtable.checkpoint = QueryBenchBackend.check;
    backend.ptr = &fixture;
    backend.vtable = &vtable;
    backend.execution_io = std.testing.io;
    backend.pinned_statement_snapshot = true;
    var compiled = try compiler.compile(a, "SELECT a.n, a.c1, a.c2, a.c3, a.c4, a.c5, a.c6, a.c7, b.n, b.c1, b.c2, b.c3, b.c4, b.c5, b.c6, b.c7 FROM docs a JOIN docs b ON a.n = b.n", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 })).?;
    defer stream.close();
    try std.testing.expect(stream.stream_manager != null);
    var count: usize = 0;
    var checksum: i64 = 0;
    while (true) {
        var page = try stream.next(137);
        defer page.deinit();
        for (page.output.rows) |row| {
            for (row) |value| try std.testing.expectEqual(row[0].integer, value.integer);
            checksum += row[0].integer;
            count += 1;
        }
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 8000), count);
    try std.testing.expectEqual(@as(i64, 31996000), checksum);
    try std.testing.expect(stream.stream_manager.?.written_bytes != 0);
}

test "SQL retained delivery survives subsequent pulls and preserves deferred errors" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |native| {
        var fixture: Fixture = .{ .count = 40 };
        var backend = fixture.backend();
        var vtable = backend.vtable.*;
        if (native) vtable.open_scan = NativeColumns.open;
        backend.vtable = &vtable;
        var compiled = try compiler.compile(a, "SELECT n + 1 FROM docs", .{});
        defer compiled.deinit();
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 32, .page_rows = 32 })).?;
        defer stream.close();
        var first = try stream.nextBatch(17);
        defer first.deinit();
        try std.testing.expectEqual(@as(usize, 17), first.values.len());
        if (native) try std.testing.expect(first.values == .retained);
        var count: usize = 17;
        while (true) {
            var page = try stream.nextBatch(17);
            defer page.deinit();
            for (0..page.values.len()) |row| {
                count += 1;
                const value = try page.values.cell(a, row, 0);
                try std.testing.expectEqual(@as(i64, @intCast(count)), value.value.integer);
                try std.testing.expect(!value.sql_null);
            }
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(@as(usize, 40), count);
        try std.testing.expectEqual(@as(i64, 1), (try first.values.cell(a, 0, 0)).value.integer);
        try std.testing.expectEqual(@as(usize, 1), fixture.closed);
    }
    var fixture: Fixture = .{ .count = 2 };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.open_scan = NativeColumns.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(a, "SELECT 1 / (1 - n) FROM docs", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{})).?;
    defer stream.close();
    var first = try stream.nextBatch(1);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 1), first.values.len());
    try std.testing.expect(!first.exhausted);
    try std.testing.expectError(error.SqlDivisionByZero, stream.nextBatch(1));
    try std.testing.expectEqual(@as(i64, 1), (try first.values.cell(a, 0, 0)).value.integer);
}

fn retainedDeliveryAllocationScenario(a: std.mem.Allocator) !void {
    var fixture: Fixture = .{ .count = 40 };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.open_scan = NativeColumns.open;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(a, "SELECT n + 1 FROM docs", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .execution_batch_rows = 32, .page_rows = 32 })).?;
    defer stream.close();
    var count: usize = 0;
    while (true) {
        var page = try stream.nextBatch(7);
        defer page.deinit();
        for (0..page.values.len()) |row| {
            count += 1;
            try std.testing.expectEqual(@as(i64, @intCast(count)), (try page.values.cell(a, row, 0)).value.integer);
        }
        if (page.exhausted) break;
    }
    try std.testing.expectEqual(@as(usize, 40), count);
}

test "SQL retained delivery releases leases on every allocation failure" {
    // Initialize shared compiler/kernel caches before measuring allocations.
    try retainedDeliveryAllocationScenario(std.testing.allocator);
    // In-place arena growth depends on heap layout. Disable it so every
    // allocation failure is exercised with the same deterministic sequence.
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), retainedDeliveryAllocationScenario, .{});
}

test "SQL blocking result pages remain valid after terminal stream failure" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT n FROM docs ORDER BY n DESC",
        "SELECT n, row_number() OVER (ORDER BY n) FROM docs ORDER BY n",
    }, [_]i64{ 999, 0 }) |sql, expected| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .count = 1000 };
        var backend = fixture.backend();
        backend.execution_io = std.testing.io;
        const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 })).?;
        defer stream.close();
        var first = try stream.nextBatch(7);
        defer first.deinit();
        try std.testing.expectEqual(expected, (try first.values.cell(a, 0, 0)).value.integer);
        try std.testing.expectEqual(error.SqlDivisionByZero, stream.fail(error.SqlDivisionByZero));
        try std.testing.expect(stream.spool == null);
        try std.testing.expectError(error.SqlStreamFailed, stream.nextBatch(7));
        try std.testing.expectEqual(expected, (try first.values.cell(a, 0, 0)).value.integer);
    }
}

test "SQL public blocking stream mixes scalar pulls with live sorted batch leases" {
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "SELECT n FROM docs ORDER BY n DESC LIMIT 10 OFFSET 2", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .count = 12 };
    var backend = fixture.backend();
    backend.execution_io = std.testing.io;
    const stream = (try Stream.open(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024 })).?;
    defer stream.close();
    try std.testing.expect(stream.spool.?.sorted.?.external == null);
    var first = try stream.nextBatch(2);
    defer first.deinit();
    for (0..8) |index| {
        if (index % 2 == 0) {
            var page = try stream.next(1);
            defer page.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(7 - index)), page.output.rows[0][0].integer);
        } else {
            var page = try stream.nextBatch(1);
            defer page.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(7 - index)), (try page.values.cell(a, 0, 0)).value.integer);
        }
        try std.testing.expectEqual(@as(i64, 9), (try first.values.cell(a, 0, 0)).value.integer);
        try std.testing.expectEqual(@as(i64, 8), (try first.values.cell(a, 1, 0)).value.integer);
    }
    try std.testing.expect(stream.exhausted);
}

test "SQL pull stream reuses proven provider order and stops before scanning a large tail" {
    var fixture: Fixture = .{ .ordered = true };
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .supports_scan_order = true, .resolve = Fixture.resolve, .scan = Fixture.scan, .open_scan = Fixture.openScan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT n FROM docs WHERE n + 0 > 5 ORDER BY n LIMIT 3 OFFSET 2", .{});
    defer compiled.deinit();
    const stream = (try Stream.open(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 16 })).?;
    defer stream.close();
    try std.testing.expect(stream.spool == null);
    var page = try stream.next(3);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 3), page.output.rows.len);
    for (page.output.rows, 8..) |row, expected| try std.testing.expectEqual(@as(i64, @intCast(expected)), row[0].integer);
    try std.testing.expect(page.exhausted);
    try std.testing.expect(fixture.offset <= 16);
    try std.testing.expectEqual(@as(usize, 1), fixture.opened);
    try std.testing.expectEqual(@as(usize, 1), fixture.closed);
}
