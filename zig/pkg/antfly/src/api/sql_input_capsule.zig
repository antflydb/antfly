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

//! Stable decoded input, original runnable authority, and immutable accounting.
const std = @import("std");
const sources = @import("antfly_local_sources");
const scheduler = sources.common_request_admission.runnable_scheduler;
const admission = sources.common_bounded_admission;
const Budget = sources.sql_memory_budget;
const json_source = @import("sql_json_source.zig");
const accounting = @import("sql_parameter_accounting.zig");

/// A completed worker releases CPU before waking its network waiter. Closed
/// or denied-reentry roots already have no runnable grant and stay retained.
pub fn parkForHandoff(execution: *scheduler.Execution) void {
    std.debug.assert(!execution.child_pending and execution.borrowed == null);
    const entry = if (execution.entry) |*value| value else return;
    std.debug.assert(entry.children == 0);
    const owner = entry.scheduler orelse return;
    if (owner.state(entry) != .running) return;
    execution.park(.output_blocked) catch {};
}

pub const Request = struct {
    read_requirements: ?@import("antfly_local_sources").sql_read_requirements.Requirements = null,
    lake_visibility: ?enum { committed, accepted } = null,
    statement: ?[]const u8 = null,
    parameters: ?[]const std.json.Value = null,
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
    limit: ?i64 = null,
    session_id: ?[]const u8 = null,
    connection_id: ?[]const u8 = null,
};

/// This synchronous handler owns the transport/identity/allocator call
/// frame until every worker joins and all decoded/result borrows retire.
/// None of these scoped input handles may escape into a response body.
pub const ScopedFrame = struct {
    node: ?*scheduler.NodeOwner,
    pins: std.atomic.Value(usize) = .init(0),
    pub fn retain(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.pins.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) return error.InvalidRunnableTransition;
        errdefer self.pins.store(0, .release);
        if (self.node) |node| node.retain() catch return error.SqlWriteCapacityUnavailable;
    }
    pub fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const previous = self.pins.swap(0, .acq_rel);
        std.debug.assert(previous == 1);
        if (self.node) |node| node.release();
    }
};

/// Heap frame for a capsule which may outlive the handler stack. Transport
/// context retirement pins its input, cancellation callback and borrowed Io;
/// this frame separately pins the node until capsule/root metadata retires.
pub fn OwnedFrame(comptime Token: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        node: ?*scheduler.NodeOwner,
        cancellation: Token,
        references: std.atomic.Value(usize) = .init(1),
        pub fn create(allocator: std.mem.Allocator, node: ?*scheduler.NodeOwner, cancellation: Token) !*Self {
            if (node) |owner| owner.retain() catch return error.SqlWriteCapacityUnavailable;
            errdefer if (node) |owner| owner.release();
            const self = try allocator.create(Self);
            self.* = .{ .allocator = allocator, .node = node, .cancellation = cancellation };
            return self;
        }
        pub fn parents(self: *Self) Parents {
            return .{ .context = self, .retain = retainOpaque, .release = releaseOpaque, .context_retirement_allowed = true };
        }
        pub fn retainOpaque(raw: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const previous = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        }
        pub fn releaseOpaque(raw: *anyopaque) void {
            @as(*Self, @ptrCast(@alignCast(raw))).release();
        }
        pub fn release(self: *Self) void {
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            if (previous != 1) return;
            const allocator = self.allocator;
            const node = self.node;
            allocator.destroy(self);
            if (node) |owner| owner.release();
        }
    };
}

pub const DecodeJob = struct {
    input: *Capsule(Request),
    completion_io: std.Io,
    done: std.Io.Event = .unset,
    failure: ?anyerror = null,
    started: bool = false,
    pub fn run(self: *@This()) void {
        defer self.done.set(self.completion_io);
        defer parkForHandoff(&self.input.execution);
        self.runChecked() catch |err| {
            self.failure = err;
        };
    }
    fn runChecked(self: *@This()) !void {
        try self.input.start();
        self.started = true;
        try self.input.decode(.{ .parse_numbers = false });
    }
};

pub const Parents = struct {
    context_retirement_allowed: bool = false,
    context: *anyopaque,
    /// Pin input, allocator, I/O, clock and cancellation parents together.
    retain: *const fn (*anyopaque) anyerror!void,
    /// Release last, after decoded buffers, root and capsule metadata retire.
    release: *const fn (*anyopaque) void,
};

pub fn Capsule(comptime T: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        parents: Parents,
        input: []const u8,
        options: admission.Options,
        scheduler_owner: ?*scheduler.Scheduler,
        class: scheduler.Class,
        started: bool = false,
        execution: scheduler.Execution = .{},
        budget: Budget,
        parsed: ?std.json.Parsed(T) = null,
        captured: ?accounting.Captured = null,
        references: std.atomic.Value(usize) = .init(1),
        context_retained: bool = false,

        /// Both allocators must be charged, thread-safe and pinned by parents.
        /// Construct at the final address before the first scheduler admission.
        pub fn create(allocator: std.mem.Allocator, region_allocator: std.mem.Allocator, parents: Parents, owner: ?*scheduler.Scheduler, options: admission.Options, class: scheduler.Class, input: []const u8, max_decode_bytes: usize) !*Self {
            try parents.retain(parents.context);
            errdefer parents.release(parents.context);
            try options.check();
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            self.* = .{ .allocator = allocator, .parents = parents, .input = input, .options = options, .scheduler_owner = owner, .class = class, .budget = .{ .backing = region_allocator, .limit = max_decode_bytes } };
            self.options.retained_bytes = @max(options.retained_bytes, @as(u64, input.len) +| @sizeOf(Self));
            return self;
        }

        /// Start only on the admitted backend worker, never while its network
        /// waiter can hold a transport lock. No authority is refreshed here.
        pub fn start(self: *Self) !void {
            if (self.started) return error.InvalidRunnableTransition;
            try self.options.check();
            try self.execution.start(self.scheduler_owner, self.options, self.class);
            self.started = true;
        }

        /// One worker owns mutable decoding; publish borrows after it joins.
        pub fn decode(self: *Self, options: std.json.ParseOptions) !void {
            if (!self.started or self.parsed != null) return error.InvalidRunnableTransition;
            self.parsed = try json_source.parse(T, self.budget.allocator(), self.input, options, self.control());
        }

        /// Values are immutable borrows of this decoded region or pinned input.
        pub fn captureParameters(self: *Self, values: []const std.json.Value, statement_bytes: usize) !accounting.Captured {
            if (self.parsed == null or self.captured != null) return error.InvalidRunnableTransition;
            const captured = try accounting.Captured.capture(self.budget.allocator(), values, self.control());
            const bytes = @max(self.options.retained_bytes, captured.requestBytes(statement_bytes) +| @sizeOf(Self));
            try self.execution.growInputBytes(bytes);
            self.options.retained_bytes = bytes;
            self.captured = captured;
            return captured;
        }

        pub fn control(self: *Self) json_source.Control {
            return .{ .ptr = self, .check = check };
        }
        fn check(raw: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.options.check();
            try self.execution.checkpoint();
            try self.options.check();
        }

        /// The real H1/H2 context outlives its response writer and response
        /// allocation. All other parents must have heap-stable ownership;
        /// callers must never transfer a capsule backed by ScopedFrame here.
        pub fn retainUntilContextRetires(self: *Self, ctx: anytype) !void {
            if (!self.parents.context_retirement_allowed) return error.InvalidRunnableTransition;
            const key = "antfly.sql-input-owner";
            if (self.context_retained or ctx.getData(key) != null) return error.InvalidRunnableTransition;
            const previous = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
            errdefer self.release();
            parkForHandoff(&self.execution);
            try ctx.setData(key, self, releaseFromContext);
            self.context_retained = true;
        }
        fn releaseFromContext(raw: *anyopaque) void {
            @as(*Self, @ptrCast(@alignCast(raw))).release();
        }

        pub const Borrow = struct {
            capsule: ?*Self,
            pub fn value(self: *const Borrow) *const T {
                return &self.capsule.?.parsed.?.value;
            }
            pub fn release(self: *Borrow) void {
                const capsule = self.capsule orelse return;
                self.capsule = null;
                capsule.release();
            }
        };
        pub const TaskBorrow = struct {
            capsule: ?*Self,
            /// The waiter calls this after the task's physical future join,
            /// not when its callback sets done or drops a decoded-data borrow.
            pub fn releaseAfterJoin(self: *TaskBorrow) void {
                const capsule = self.capsule orelse return;
                self.capsule = null;
                capsule.release();
            }
        };
        /// Reserve before enqueue. Failed enqueue releases this handle too;
        /// successful enqueue retains it through joined task metadata cleanup.
        pub fn retainTask(self: *Self) TaskBorrow {
            _ = self.references.fetchAdd(1, .monotonic);
            return .{ .capsule = self };
        }
        /// This is a data/lifetime borrow, never a second CPU execution owner.
        /// Keep the acquiring handle alive until all borrowers are published.
        pub fn borrow(self: *Self) !Borrow {
            if (self.parsed == null) return error.InvalidRunnableTransition;
            _ = self.references.fetchAdd(1, .monotonic);
            return .{ .capsule = self };
        }
        pub fn release(self: *Self) void {
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            if (previous != 1) return;
            // Every execution child must join before its data borrow releases.
            // Catch broken caller ownership before freeing the decoded region.
            std.debug.assert(!self.execution.child_pending and self.execution.borrowed == null);
            if (self.execution.entry) |entry| std.debug.assert(entry.children == 0);
            const parents = self.parents;
            const allocator = self.allocator;
            if (self.parsed) |*parsed| parsed.deinit();
            std.debug.assert(self.budget.live == 0);
            self.execution.finish();
            allocator.destroy(self);
            parents.release(parents.context);
        }
    };
}

const TestParents = struct {
    budget: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 },
    owner: ?*scheduler.Scheduler = null,
    pins: usize = 0,
    released_after_retirement: bool = false,
    fn retain(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.pins += 1;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.pins == 1);
        self.pins -= 1;
        self.released_after_retirement = self.budget.live == 0 and if (self.owner) |owner| owner.stats().operations == 0 else true;
    }
    fn callbacks(self: *@This()) Parents {
        return .{ .context = self, .retain = retain, .release = release };
    }
};

test "SQL input capsule retains decoded region original root and parents through last data borrow" {
    const Input = struct { statement: []const u8, parameters: []const std.json.Value };
    var owner = try scheduler.Scheduler.init(.{ .slots = 2 });
    var parents: TestParents = .{ .owner = &owner };
    const capsule = try Capsule(Input).create(parents.budget.allocator(), parents.budget.allocator(), parents.callbacks(), &owner, .{ .io = std.testing.io }, .query, "{\"statement\":\"SELECT $1\",\"parameters\":[\"\\u03bb\",9007199254740993]}", 8 << 20);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try capsule.start();
    try capsule.decode(.{ .parse_numbers = false });
    const entry = &capsule.execution.entry.?;
    const captured = try capsule.captureParameters(capsule.parsed.?.value.parameters, capsule.parsed.?.value.statement.len);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expect(entry == &capsule.execution.entry.?);
    try std.testing.expectEqual(capsule.options.retained_bytes, owner.stats().retained_bytes);
    try std.testing.expect(captured.values.ptr == capsule.parsed.?.value.parameters.ptr);
    var borrow = try capsule.borrow();
    try capsule.execution.park(.output_blocked);
    capsule.release();
    try std.testing.expectEqual(@as(usize, 1), parents.pins);
    try std.testing.expect(parents.budget.live != 0);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqualStrings("SELECT $1", borrow.value().statement);
    try std.testing.expectEqualStrings("λ", borrow.value().parameters[0].string);
    try std.testing.expectEqualStrings("9007199254740993", borrow.value().parameters[1].number_string);
    borrow.release();
    try std.testing.expectEqual(@as(usize, 0), parents.pins);
    try std.testing.expect(parents.released_after_retirement);
    try std.testing.expectEqual(@as(usize, 0), parents.budget.live);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "SQL input capsule interrupted decode keeps original authority and retires region before root" {
    const Cancellation = struct {
        checks: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 10;
        }
    };
    const body = try std.testing.allocator.alloc(u8, 65538);
    defer std.testing.allocator.free(body);
    @memset(body, 'x');
    body[0] = '"';
    body[body.len - 1] = '"';
    var canceled: Cancellation = .{};
    var owner = try scheduler.Scheduler.init(.{ .slots = 2 });
    var parents: TestParents = .{ .owner = &owner };
    const capsule = try Capsule([]const u8).create(parents.budget.allocator(), parents.budget.allocator(), parents.callbacks(), &owner, .{ .io = std.testing.io, .cancellation = .{ .ptr = &canceled, .requested = Cancellation.requested } }, .query, body, 8 << 20);
    try capsule.start();
    canceled.checks = 0;
    try std.testing.expectError(error.Canceled, capsule.decode(.{}));
    try std.testing.expect(capsule.parsed == null);
    try std.testing.expectEqual(@as(usize, 0), capsule.budget.live);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), parents.pins);
    capsule.release();
    try std.testing.expect(parents.released_after_retirement);
}

test "SQL input capsule allocation failures retire parsed buffers metadata and parent pins" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var parents: TestParents = .{};
            defer std.debug.assert(parents.pins == 0);
            const capsule = try Capsule([]const std.json.Value).create(allocator, allocator, parents.callbacks(), null, .{ .io = std.testing.io }, .query, "[\"escaped\\nvalue\",[1,2,3],{\"key\":\"value\"}]", 8 << 20);
            defer capsule.release();
            try capsule.start();
            try capsule.decode(.{ .parse_numbers = false });
            _ = try capsule.captureParameters(capsule.parsed.?.value, 8);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL input capsule task pin survives callback completion until physical future join" {
    const Input = Capsule([]const u8);
    const Worker = struct {
        fn run(capsule: *Input) !void {
            try capsule.start();
            try capsule.decode(.{});
            try capsule.execution.park(.output_blocked);
        }
    };
    var owner = try scheduler.Scheduler.init(.{ .slots = 2 });
    var parents: TestParents = .{ .owner = &owner };
    const capsule = try Input.create(parents.budget.allocator(), parents.budget.allocator(), parents.callbacks(), &owner, .{ .io = std.testing.io }, .query, "\"retained worker input\"", 8 << 20);
    var task_pin = capsule.retainTask();
    var future = std.testing.io.concurrent(Worker.run, .{capsule}) catch |err| {
        task_pin.releaseAfterJoin();
        capsule.release();
        return err;
    };
    // The original caller can leave; the waiter's prepublished task handle
    // retains every input/clock/I/O/allocator parent until the future joins.
    capsule.release();
    const result = future.await(std.testing.io);
    defer task_pin.releaseAfterJoin();
    try result;
    try std.testing.expectEqual(@as(usize, 1), parents.pins);
    try std.testing.expect(parents.budget.live != 0);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqualStrings("retained worker input", capsule.parsed.?.value);
    task_pin.releaseAfterJoin();
    try std.testing.expect(parents.released_after_retirement);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "SQL input capsule refused decoded footprint stays charged and unpublished until physical retirement" {
    const count = 4096;
    const body = try std.testing.allocator.alloc(u8, count * 2 + 1);
    defer std.testing.allocator.free(body);
    body[0] = '[';
    for (0..count) |index| {
        body[index * 2 + 1] = '0';
        body[index * 2 + 2] = if (index + 1 == count) ']' else ',';
    }
    var owner = try scheduler.Scheduler.init(.{ .slots = 2, .max_bytes = 32 << 10, .control_bytes = 4 << 10 });
    var parents: TestParents = .{ .owner = &owner };
    const capsule = try Capsule([]const std.json.Value).create(parents.budget.allocator(), parents.budget.allocator(), parents.callbacks(), &owner, .{ .io = std.testing.io }, .query, body, 8 << 20);
    try capsule.start();
    const original_bytes = owner.stats().retained_bytes;
    try capsule.decode(.{ .parse_numbers = false });
    try std.testing.expectError(error.SchedulerFull, capsule.captureParameters(capsule.parsed.?.value, 8));
    try std.testing.expect(capsule.captured == null);
    try std.testing.expect(capsule.parsed != null);
    try std.testing.expect(capsule.budget.live != 0);
    try std.testing.expectEqual(original_bytes, owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    capsule.release();
    try std.testing.expect(parents.released_after_retirement);
}

test "SQL input capsule worker start preserves deadline fixed before enqueue" {
    const Clock = struct {
        var time: u64 = 0;
        fn now() u64 {
            return time;
        }
    };
    Clock.time = 0;
    defer Clock.time = 0;
    var owner = try scheduler.Scheduler.init(.{ .slots = 2 });
    var parents: TestParents = .{ .owner = &owner };
    const capsule = try Capsule([]const u8).create(parents.budget.allocator(), parents.budget.allocator(), parents.callbacks(), &owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 }, .query, "\"original deadline\"", 8 << 20);
    Clock.time = 100;
    try std.testing.expectError(error.DeadlineExceeded, capsule.start());
    try std.testing.expect(!capsule.started and capsule.parsed == null);
    try std.testing.expectEqual(@as(?u64, 100), capsule.options.deadline_ns);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    capsule.release();
    try std.testing.expect(parents.released_after_retirement);
}

test "HTTP SQL production decoder preserves scoped node pin original errors and parked worker handoff" {
    const Case = enum { valid, malformed, memory, deadline };
    const Clock = struct {
        var time: u64 = 0;
        fn now() u64 {
            return time;
        }
    };
    for (std.enums.values(Case)) |case| {
        Clock.time = 0;
        defer Clock.time = 0;
        var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
        defer node.deinit();
        var frame: ScopedFrame = .{ .node = &node };
        defer std.debug.assert(frame.pins.load(.acquire) == 0);
        var backing: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
        const body = if (case == .malformed) "{\"statement\":false}" else "{\"statement\":\"SELECT $1\",\"parameters\":[9007199254740993,\"\\u03bb\"],\"limit\":128}";
        const input = try Capsule(Request).create(backing.allocator(), backing.allocator(), .{ .context = &frame, .retain = ScopedFrame.retain, .release = ScopedFrame.release }, &node.scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 }, .query, body, if (case == .memory) 32 else 8 << 20);
        defer input.release();
        try std.testing.expectEqual(@as(usize, 1), frame.pins.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), node.borrowers);
        try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
        if (case == .deadline) Clock.time = 100;
        var job: DecodeJob = .{ .input = input, .completion_io = std.testing.io };
        var task_pin = input.retainTask();
        var future = std.testing.io.concurrent(DecodeJob.run, .{&job}) catch |err| {
            task_pin.releaseAfterJoin();
            return err;
        };
        job.done.waitUncancelable(std.testing.io);
        @import("protected_future.zig").wait(std.testing.io, &future);
        task_pin.releaseAfterJoin();
        try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
        try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, if (case == .deadline) 0 else 1), node.scheduler.stats().operations);
        if (case == .valid) {
            try std.testing.expect(job.failure == null and job.started);
            try std.testing.expectEqualStrings("SELECT $1", input.parsed.?.value.statement.?);
            try std.testing.expectEqualStrings("9007199254740993", input.parsed.?.value.parameters.?[0].number_string);
            try std.testing.expectEqualStrings("λ", input.parsed.?.value.parameters.?[1].string);
            try std.testing.expectEqual(@as(?i64, 128), input.parsed.?.value.limit);
            try input.execution.enter(.query);
            _ = try input.captureParameters(input.parsed.?.value.parameters.?, input.parsed.?.value.statement.?.len);
            parkForHandoff(&input.execution);
        } else {
            try std.testing.expectEqual(@as(?anyerror, switch (case) {
                .malformed => error.UnexpectedToken,
                .memory => error.OutOfMemory,
                .deadline => error.DeadlineExceeded,
                .valid => unreachable,
            }), job.failure);
            try std.testing.expect(input.parsed == null);
            try std.testing.expectEqual(@as(usize, 0), input.budget.live);
            try std.testing.expectEqual(case == .memory, input.budget.exhausted);
        }
    }
}

test "HTTP SQL scoped input frame rejects closed node without allocating metadata or retaining partial pins" {
    var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
    defer node.deinit();
    node.close();
    var frame: ScopedFrame = .{ .node = &node };
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Capsule(Request).create(backing.allocator(), backing.allocator(), .{ .context = &frame, .retain = ScopedFrame.retain, .release = ScopedFrame.release }, &node.scheduler, .{ .io = std.testing.io }, .query, "{}", 8 << 20));
    try std.testing.expectEqual(@as(usize, 0), frame.pins.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), node.borrowers);
    try std.testing.expectEqual(@as(usize, 0), backing.live);
}
