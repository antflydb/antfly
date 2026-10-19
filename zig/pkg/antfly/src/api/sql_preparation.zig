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

//! Borrow an existing preparation owner. No new admission, deadline or byte
//! charge is created; all callbacks retire before the local owner goes away.
const std = @import("std");
const sources = @import("antfly_local_sources");
const scheduler = sources.common_request_admission.runnable_scheduler;
const admission = sources.common_bounded_admission;
const cache = @import("../sql/plan_cache.zig");

/// Only compiler failures carry source diagnostics. Cache I/O cancellation,
/// admission failures and interrupted checkpoints retain runtime diagnostics.
/// An allocator failure has no SQL source position or parser message either.
pub fn hasCompilerDiagnostic(err: anyerror) bool {
    return switch (err) {
        error.InvalidSqlSyntax,
        error.UnsupportedSqlShape,
        error.SqlLimitExceeded,
        error.InvalidSqlParameter,
        error.InvalidSqlNumber,
        error.DuplicateSqlColumn,
        error.SqlInvalidUnicodeEscape,
        error.SqlInvalidTextEncoding,
        error.SqlInvalidParameterValue,
        error.SqlInvalidTextRepresentation,
        error.SqlNumericOutOfRange,
        => true,
        else => false,
    };
}

pub const Owner = struct {
    execution: *scheduler.Execution,
    options: admission.Options,
    failure: ?anyerror = null,
    waited_ns: u64 = 0,
    wait_started_ns: ?u64 = null,
    wait_limit_ns: ?u64 = null,

    pub fn control(self: *Owner) cache.Control {
        return .{
            .work = .{ .ptr = self, .checkpoint = checkpoint },
            .wait = if (self.execution.entry != null or self.execution.borrowed != null) wait else null,
        };
    }

    /// Copy setup metadata under the existing owner. The destination keeps its
    /// caller's allocator/resource charge; an interrupted copy frees it before
    /// returning the original request cancellation/deadline error.
    pub fn copy(self: *Owner, allocator: std.mem.Allocator, source: []const u8) ![]u8 {
        return self.copyElements(u8, allocator, source);
    }

    pub fn copyElements(self: *Owner, comptime T: type, allocator: std.mem.Allocator, source: []const T) ![]T {
        try self.checkpointChecked();
        const result = try allocator.alloc(T, source.len);
        errdefer allocator.free(result);
        @import("sql_parser").lexer.copyControlled(T, result, source, self.control().work) catch |err| return self.original(err);
        try self.checkpointChecked();
        return result;
    }

    /// Match parseUnsigned(u64, text, 10), mapping invalid text/range to the
    /// wire adapter's existing InvalidResult. Signs remain invalid and interior
    /// underscores remain accepted, including repeated separators.
    pub fn unsignedDecimal(self: *Owner, text: []const u8) !u64 {
        try self.check();
        if (text.len == 0 or text[0] == '_' or text[text.len - 1] == '_') return error.InvalidResult;
        var value: u64 = 0;
        for (text, 0..) |byte, index| {
            if (index % 1024 == 0) try self.check();
            if (byte == '_') continue;
            if (byte < '0' or byte > '9') return error.InvalidResult;
            value = std.math.mul(u64, value, 10) catch return error.InvalidResult;
            value = std.math.add(u64, value, byte - '0') catch return error.InvalidResult;
        }
        try self.check();
        return value;
    }

    pub fn original(self: *const Owner, err: anyerror) anyerror {
        return if (err == error.SqlPreparationInterrupted) self.failure orelse error.InvalidRunnableTransition else err;
    }

    pub fn check(self: *Owner) !void {
        try self.checkpointChecked();
    }

    fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.checkpointChecked() catch |err| {
            self.failure = err;
            return error.SqlPreparationInterrupted;
        };
    }

    fn checkpointChecked(self: *Owner) !void {
        try self.options.check();
        try self.execution.checkpoint();
        try self.options.check();
    }

    fn begin(raw: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try checkWaiting(self);
        try self.execution.park(.resource_blocked);
    }

    fn end(raw: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const class = try self.execution.retainedClass();
        try self.execution.enterWithCheck(class, .{ .context = self, .check = checkWaiting });
        try checkWaiting(self);
    }

    fn checkWaiting(raw: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.options.check();
        const started = self.wait_started_ns orelse return error.InvalidRunnableTransition;
        const limit = self.wait_limit_ns orelse return error.InvalidRunnableTransition;
        if (self.waited_ns +| (self.options.now() -| started) >= limit) return error.RunnableWaitTimeout;
    }

    fn wait(raw: *anyopaque, io: std.Io) error{SqlPreparationInterrupted}!void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.waitChecked(io) catch |err| {
            self.failure = err;
            return error.SqlPreparationInterrupted;
        };
    }

    fn waitChecked(self: *Owner, io: std.Io) !void {
        if (!self.options.allow_wait) return error.SqlPlanCacheBusy;
        // Borrowed owners need an explicit original-policy wait limit; do not
        // invent a new timeout or silently hold their CPU through contention.
        const entry = if (self.execution.entry) |*value| value else return error.ExecutionWaitUnsupported;
        const scheduler_owner = entry.scheduler orelse return error.InvalidRunnableTransition;
        self.wait_limit_ns = @as(u64, scheduler_owner.config.max_wait_ms) * std.time.ns_per_ms;
        self.wait_started_ns = self.options.now();
        defer {
            self.waited_ns +|= self.options.now() -| self.wait_started_ns.?;
            self.wait_started_ns = null;
        }
        // Poll availability without acquiring the cache mutex while CPU is
        // parked. Resume under the original owner before the next tryLock.
        try sources.sql_execution_wait.run(
            .{ .ptr = self, .begin = begin, .end = end },
            std.Io.sleep,
            .{ io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake },
        );
    }
};

test "SQL preparation cache contention preserves original owner and bounded waiting" {
    const Case = enum { available, canceled, deadline, wait_timeout, shutdown };
    const Clock = struct {
        var value: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return value.load(.acquire);
        }
    };
    const Job = struct {
        scheduler_owner: *scheduler.Scheduler,
        plan_cache: *cache.Cache,
        options: admission.Options,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.acquire() catch |err| {
                self.err = err;
            };
        }
        fn acquire(self: *@This()) !void {
            var execution: scheduler.Execution = .{};
            defer execution.finish();
            try execution.start(self.scheduler_owner, self.options, .query);
            var preparation: Owner = .{ .execution = &execution, .options = self.options };
            var diagnostic: sources.sql_compiler.Diagnostic = .{};
            var lease = self.plan_cache.acquireControlled(self.options.io, .{ .statement = "SELECT 1" }, &diagnostic, preparation.control()) catch |err| return preparation.original(err);
            defer lease.release(self.options.io);
        }
    };
    const io = std.testing.io;
    for (std.enums.values(Case)) |case| {
        Clock.value.store(0, .release);
        defer Clock.value.store(0, .release);
        var cancel: std.atomic.Value(bool) = .init(false);
        const Cancellation = struct {
            fn requested(raw: *const anyopaque) bool {
                return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
            }
        };
        var owner = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
        var plans = cache.Cache.init(std.testing.allocator, .{});
        defer plans.deinit(io);
        // Hold the real cache mutex, forcing the actual acquisition adapter
        // through its parked wait instead of calling a synthetic wait hook.
        try plans.mutex.lock(io);
        var locked = true;
        defer if (locked) plans.mutex.unlock(io);
        var job: Job = .{
            .scheduler_owner = &owner,
            .plan_cache = &plans,
            .options = .{
                .io = io,
                .native_now_ns = Clock.now,
                .deadline_ns = 10 * std.time.ns_per_s,
                .retained_bytes = 37,
                .cancellation = .{ .ptr = &cancel, .requested = Cancellation.requested },
            },
        };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
        while (owner.stats().resource_blocked != 1) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CachePreparationDidNotPark;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        const blocked = owner.stats();
        try std.testing.expectEqual(@as(usize, 1), blocked.operations);
        try std.testing.expectEqual(@as(u64, 37), blocked.retained_bytes);
        // Both ordinary work and reserved control must progress while cache
        // contention retains the first operation and its input charge.
        var peer: scheduler.Execution = .{};
        defer peer.finish();
        try peer.start(&owner, .{ .io = io }, .query);
        peer.finish();
        var control: scheduler.Execution = .{};
        defer control.finish();
        try control.start(&owner, .{ .io = io }, .control);
        control.finish();
        switch (case) {
            .available => {
                plans.mutex.unlock(io);
                locked = false;
            },
            .canceled => cancel.store(true, .release),
            .deadline => Clock.value.store(11 * std.time.ns_per_s, .release),
            .wait_timeout => Clock.value.store(6 * std.time.ns_per_s, .release),
            .shutdown => owner.close(),
        }
        future.await(io);
        if (locked) {
            plans.mutex.unlock(io);
            locked = false;
        }
        switch (case) {
            .available => try std.testing.expectEqual(@as(?anyerror, null), job.err),
            .canceled => try std.testing.expectEqual(error.Canceled, job.err.?),
            .deadline => try std.testing.expectEqual(error.DeadlineExceeded, job.err.?),
            .wait_timeout => try std.testing.expectEqual(error.RunnableWaitTimeout, job.err.?),
            .shutdown => try std.testing.expectEqual(error.SchedulerClosed, job.err.?),
        }
        const retired = owner.stats();
        try std.testing.expectEqual(@as(usize, 0), retired.operations);
        try std.testing.expectEqual(@as(usize, 0), retired.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), retired.retained_bytes);
        const cached = plans.stats(io);
        try std.testing.expectEqual(@as(usize, 0), cached.compiling);
        try std.testing.expectEqual(@as(usize, 0), cached.leases);
        try std.testing.expectEqual(@as(usize, @intFromBool(case == .available)), cached.entries);
    }
}

test "SQL preparation cache contention preserves fail-fast request policy" {
    const io = std.testing.io;
    var owner = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    const options: admission.Options = .{ .io = io, .allow_wait = false, .retained_bytes = 37 };
    try execution.start(&owner, options, .query);
    var preparation: Owner = .{ .execution = &execution, .options = options };
    var plans = cache.Cache.init(std.testing.allocator, .{});
    defer plans.deinit(io);
    try plans.mutex.lock(io);
    var diagnostic: sources.sql_compiler.Diagnostic = .{};
    const result = plans.acquireControlled(io, .{ .statement = "SELECT 1" }, &diagnostic, preparation.control());
    plans.mutex.unlock(io);
    try std.testing.expectError(error.SqlPreparationInterrupted, result);
    try std.testing.expectEqual(error.SqlPlanCacheBusy, preparation.original(error.SqlPreparationInterrupted));
    try std.testing.expectEqual(@as(u64, 0), preparation.waited_ns);
    const retained = owner.stats();
    try std.testing.expectEqual(@as(usize, 0), retained.resource_blocked);
    try std.testing.expectEqual(@as(usize, 1), retained.runnable_slots);
    try std.testing.expectEqual(@as(u64, 37), retained.retained_bytes);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), plans.stats(io).compiling);
}

test "SQL setup metadata copy retires interrupted destination under original request owner" {
    const Cancellation = struct {
        checks: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 8;
        }
    };
    const source: [16384]u8 = @splat('x');
    var cancellation: Cancellation = .{};
    const options: admission.Options = .{ .io = std.testing.io, .retained_bytes = source.len, .cancellation = .{ .ptr = &cancellation, .requested = Cancellation.requested } };
    var owner = try scheduler.Scheduler.init(.{ .slots = 2, .quantum_ms = 100, .checkpoint_budget = 64 });
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, options, .query);
    cancellation.checks = 0;
    var preparation: Owner = .{ .execution = &execution, .options = options };
    var budget: sources.sql_memory_budget = .{ .backing = std.testing.allocator, .limit = source.len };
    try std.testing.expectError(error.Canceled, preparation.copy(budget.allocator(), &source));
    try std.testing.expectEqual(@as(usize, source.len), budget.peak);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    // Physical destination retirement precedes release of the request owner.
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, source.len), owner.stats().retained_bytes);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "SQL setup unsigned decimal preserves wire parsing semantics and cancellation" {
    const options: admission.Options = .{ .io = std.testing.io };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(null, options, .query);
    var owner: Owner = .{ .execution = &execution, .options = options };
    for ([_][]const u8{ "0", "0001", "1_2_3", "1__2", "18446744073709551615" }) |text|
        try std.testing.expectEqual(try std.fmt.parseUnsigned(u64, text, 10), try owner.unsignedDecimal(text));
    for ([_][]const u8{ "", "+0", "-0", "_1", "1_", "1x", "0x1", "18446744073709551616" }) |text| {
        if (std.fmt.parseUnsigned(u64, text, 10)) |_| return error.TestUnexpectedResult else |_| {}
        try std.testing.expectError(error.InvalidResult, owner.unsignedDecimal(text));
    }
    const Cancellation = struct {
        checks: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks == 5;
        }
    };
    const zeros: [8192]u8 = @splat('0');
    var cancellation: Cancellation = .{};
    owner.options.cancellation = .{ .ptr = &cancellation, .requested = Cancellation.requested };
    try std.testing.expectError(error.Canceled, owner.unsignedDecimal(&zeros));
    try std.testing.expectEqual(@as(usize, 5), cancellation.checks);
}

test "SQL preparation publication contention retains compiled owner through bounded waits" {
    const Case = enum { available, canceled, deadline, wait_timeout, shutdown };
    const Clock = struct {
        var value: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return value.load(.acquire);
        }
    };
    const Probe = struct {
        scope: *Owner = undefined,
        io: std.Io,
        checks: usize = 0,
        pause_at: ?usize = null,
        compiled_ready: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        barrier_exited: bool = false,
        publication_contended: std.atomic.Value(bool) = .init(false),
        fn control(self: *@This()) cache.Control {
            return .{ .work = .{ .ptr = self, .checkpoint = checkpoint }, .wait = wait };
        }
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const original = self.scope.control();
            try original.work.checkpoint(original.work.ptr);
            self.checks += 1;
            if (self.pause_at == self.checks) {
                self.compiled_ready.store(true, .release);
                // Test synchronization uses the same bounded owner wait. The
                // compiled reservation stays physically retained throughout.
                while (!self.release.load(.acquire)) try original.wait.?(original.work.ptr, self.io);
                self.barrier_exited = true;
            }
        }
        fn wait(raw: *anyopaque, io: std.Io) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.barrier_exited) self.publication_contended.store(true, .release);
            const original = self.scope.control();
            try original.wait.?(original.work.ptr, io);
        }
    };
    const Cancellation = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Job = struct {
        scheduler_owner: *scheduler.Scheduler,
        plans: *cache.Cache,
        options: admission.Options,
        probe: *Probe,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.acquire() catch |err| {
                self.err = err;
            };
        }
        fn acquire(self: *@This()) !void {
            var execution: scheduler.Execution = .{};
            defer execution.finish();
            try execution.start(self.scheduler_owner, self.options, .query);
            var scope: Owner = .{ .execution = &execution, .options = self.options };
            self.probe.scope = &scope;
            var diagnostic: sources.sql_compiler.Diagnostic = .{};
            var lease = self.plans.acquireControlled(self.options.io, .{ .statement = "SELECT 'retained compiled value'" }, &diagnostic, self.probe.control()) catch |err| return scope.original(err);
            defer lease.release(self.options.io);
        }
    };
    const io = std.testing.io;
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var final_checkpoint: usize = 0;
    {
        var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
        var plans = cache.Cache.init(no_resize.allocator(), .{});
        defer plans.deinit(io);
        var probe: Probe = .{ .io = io };
        var job: Job = .{ .scheduler_owner = &scheduler_owner, .plans = &plans, .options = .{ .io = io }, .probe = &probe };
        try job.acquire();
        final_checkpoint = probe.checks;
        try std.testing.expect(final_checkpoint > 1);
    }
    for (std.enums.values(Case)) |case| {
        Clock.value.store(0, .release);
        defer Clock.value.store(0, .release);
        var cancel: std.atomic.Value(bool) = .init(false);
        var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
        var backing: sources.sql_memory_budget = .{ .backing = no_resize.allocator(), .limit = 32 << 20 };
        var plans = cache.Cache.init(backing.allocator(), .{});
        var cache_live = true;
        defer if (cache_live) plans.deinit(io);
        var probe: Probe = .{ .io = io, .pause_at = final_checkpoint };
        var job: Job = .{ .scheduler_owner = &scheduler_owner, .plans = &plans, .probe = &probe, .options = .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10 * std.time.ns_per_s, .retained_bytes = 37, .cancellation = .{ .ptr = &cancel, .requested = Cancellation.requested } } };
        var future = try io.concurrent(Job.run, .{&job});
        defer future.cancel(io);
        // Release the test barrier on any assertion/observation failure before
        // joining its worker; neither task nor retained memory can be stranded.
        defer probe.release.store(true, .release);
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
        while (!probe.compiled_ready.load(.acquire)) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CompiledPublicationBarrierNotReached;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try plans.mutex.lock(io);
        var locked = true;
        defer if (locked) plans.mutex.unlock(io);
        probe.release.store(true, .release);
        while (!probe.publication_contended.load(.acquire) or scheduler_owner.stats().resource_blocked != 1) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CompiledPublicationDidNotPark;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        const metadata = plans.buckets.len * @sizeOf(?*anyopaque);
        try std.testing.expectEqual(@as(usize, 1), plans.counters.compiling);
        const entry_type = @typeInfo(@typeInfo(@TypeOf(plans.oldest)).optional.child).pointer.child;
        try std.testing.expectEqual(metadata + plans.config.max_compile_bytes + @sizeOf(entry_type), plans.counters.bytes);
        try std.testing.expect(backing.live > metadata + @sizeOf(entry_type));
        try std.testing.expectEqual(@as(u64, 37), scheduler_owner.stats().retained_bytes);
        var peer: scheduler.Execution = .{};
        defer peer.finish();
        try peer.start(&scheduler_owner, .{ .io = io }, .query);
        peer.finish();
        var control: scheduler.Execution = .{};
        defer control.finish();
        try control.start(&scheduler_owner, .{ .io = io }, .control);
        control.finish();
        switch (case) {
            .available => {},
            .canceled => cancel.store(true, .release),
            .deadline => Clock.value.store(11 * std.time.ns_per_s, .release),
            .wait_timeout => Clock.value.store(6 * std.time.ns_per_s, .release),
            .shutdown => scheduler_owner.close(),
        }
        // Deferred reservation retirement needs the cache mutex even after
        // request authority expires. Unlock before joining physical cleanup.
        plans.mutex.unlock(io);
        locked = false;
        future.await(io);
        switch (case) {
            .available => try std.testing.expectEqual(@as(?anyerror, null), job.err),
            .canceled => try std.testing.expectEqual(error.Canceled, job.err.?),
            .deadline => try std.testing.expectEqual(error.DeadlineExceeded, job.err.?),
            .wait_timeout => try std.testing.expectEqual(error.RunnableWaitTimeout, job.err.?),
            .shutdown => try std.testing.expectEqual(error.SchedulerClosed, job.err.?),
        }
        const retired = scheduler_owner.stats();
        try std.testing.expectEqual(@as(usize, 0), retired.operations);
        try std.testing.expectEqual(@as(usize, 0), retired.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), retired.retained_bytes);
        const cached = plans.stats(io);
        try std.testing.expectEqual(@as(usize, 0), cached.compiling);
        try std.testing.expectEqual(@as(usize, 0), cached.leases);
        try std.testing.expectEqual(@as(usize, @intFromBool(case == .available)), cached.entries);
        try std.testing.expectEqual(backing.live, cached.bytes);
        plans.deinit(io);
        cache_live = false;
        try std.testing.expectEqual(@as(usize, 0), backing.live);
        std.debug.print("SQL compiled publication wait: case={s} owner_retired=true compiled_memory_retired=true\n", .{@tagName(case)});
    }
}

test "SQL preparation cache I/O cancellation preserves runtime diagnostics and retirement" {
    const Cancellation = struct {
        var checks: usize = 0;
        var cancel_at: usize = 0;
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            checks += 1;
            if (checks == cancel_at) return error.Canceled;
        }
    };
    const io = std.testing.io;
    var vtable = io.vtable.*;
    vtable.checkCancel = Cancellation.check;
    var cache_io = io;
    cache_io.vtable = &vtable;
    for ([_]usize{ 1, 2 }) |cancel_at| {
        Cancellation.checks = 0;
        Cancellation.cancel_at = cancel_at;
        var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 64 });
        const options: admission.Options = .{ .io = io, .retained_bytes = 17 };
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler_owner, options, .query);
        var scope: Owner = .{ .execution = &execution, .options = options };
        var backing: sources.sql_memory_budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
        var plans = cache.Cache.init(backing.allocator(), .{});
        var cache_live = true;
        defer if (cache_live) plans.deinit(io);
        var diagnostic: sources.sql_compiler.Diagnostic = .{};
        const failure = if (plans.acquireControlled(cache_io, .{ .statement = "SELECT 'compiled before cancellation'" }, &diagnostic, scope.control())) |value| blk: {
            var lease = value;
            lease.release(io);
            break :blk error.ExpectedCacheCancellation;
        } else |err| err;
        try std.testing.expectEqual(error.Canceled, failure);
        try std.testing.expectEqual(cancel_at, Cancellation.checks);
        try std.testing.expectEqual(@as(?anyerror, null), scope.failure);
        try std.testing.expectEqual(error.Canceled, scope.original(failure));
        try std.testing.expect(!hasCompilerDiagnostic(failure));
        // The untouched parser default must never replace the runtime message.
        try std.testing.expectEqualStrings("invalid SQL syntax", diagnostic.message);
        const safe = sources.sql_errors.describe(scope.original(failure));
        try std.testing.expectEqualStrings("57014", safe.code);
        try std.testing.expect(!std.mem.eql(u8, diagnostic.message, safe.message));
        const stats = plans.stats(io);
        try std.testing.expectEqual(@as(usize, 0), stats.compiling);
        try std.testing.expectEqual(@as(usize, 0), stats.entries);
        try std.testing.expectEqual(@as(usize, 0), stats.leases);
        try std.testing.expectEqual(plans.buckets.len * @sizeOf(?*anyopaque), stats.bytes);
        try std.testing.expectEqual(stats.bytes, backing.live);
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler_owner.stats().retained_bytes);
        plans.deinit(io);
        cache_live = false;
        try std.testing.expectEqual(@as(usize, 0), backing.live);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().retained_bytes);
    }
}
