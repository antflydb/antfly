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

//! A node-owned FIFO permit acquired before control RPC signing and HTTP task
//! creation. All queue entries are preallocated and charged to control memory.
const std = @import("std");
const common = @import("antfly_local_sources").common_request_admission;
const bounded = common.bounded_admission;
const scheduling = common.runnable_scheduler;
const http = @import("../raft/transport/http_common.zig");
const Wait = @import("antfly_runtime_abi").execution_wait.Boundary;
const clock = @import("antfly_platform").time;

pub const Options = struct {
    deadline_ns: u64,
    cancellation: ?*const http.RequestCancellation = null,
    execution_wait: ?Wait = null,
    now_ns: *const fn () u64 = clock.monotonicNs,
    fn check(self: Options) !void {
        if (self.cancellation) |signal| {
            if (signal.isCancelled()) return error.Canceled;
            if (signal.execution_deadline_ns) |deadline| if (self.now_ns() >= deadline) return error.DeadlineExceeded;
        }
        if (self.now_ns() >= self.deadline_ns) return error.DeadlineExceeded;
    }
};

pub const Gate = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    baseline: scheduling.Scheduler.ControlBaseline,
    admission: bounded.Controller,
    slots: []Slot,
    mutex: std.atomic.Mutex = .unlocked,
    calls: usize = 0,
    closed: bool = false,
    next_generation: u64 = 1,
    active_generation: u64 = 0,
    const Slot = struct { used: bool = false, entry: bounded.Controller.Entry = undefined };
    pub const max_wait_ns: u64 = 3 * std.time.ns_per_s;
    pub const Lease = struct {
        gate: *Gate,
        generation: u64,
        inner: bounded.Controller.Lease,
        pub fn release(self: *Lease) void {
            if (!self.inner.active) return;
            self.gate.lock();
            const owned = self.gate.active_generation == self.generation;
            if (owned) self.gate.active_generation = 0;
            self.gate.mutex.unlock();
            if (owned) self.inner.release() else self.inner.active = false;
        }
        pub fn borrow(self: *const Lease) http.BorrowedControlPermit {
            return .{ .contract = .of(Lease), .owner = self.gate, .generation = self.generation };
        }
    };
    pub fn validateBorrow(self: *const Gate, permit: http.BorrowedControlPermit) !void {
        if (permit.owner != @as(*const anyopaque, @ptrCast(self))) return error.InvalidArgument;
        if (!permit.contract.matches(.of(Lease))) return error.UnsupportedVersion;
        const mutable = @constCast(self);
        mutable.lock();
        defer mutable.mutex.unlock();
        if (permit.generation == 0 or permit.generation != self.active_generation) return error.InvalidArgument;
    }

    pub fn reservationBytes(capacity: usize) !u64 {
        if (capacity == 0 or std.math.cast(u32, capacity) == null) return error.InvalidArgument;
        return std.math.add(u64, @sizeOf(Gate), try std.math.mul(u64, capacity, @sizeOf(Slot)));
    }
    /// One entry for every admitted node operation. A caller may wait only
    /// under its existing synchronous execution authority; no helper is born
    /// just to wait for this permit. Uncontrolled callers remain fail-fast.
    pub fn create(allocator: std.mem.Allocator, io: std.Io, scheduler: *scheduling.Scheduler) !*Gate {
        const capacity = scheduler.config.max_operations;
        var baseline = try scheduler.reserveControlBaseline(try reservationBytes(capacity));
        errdefer baseline.release();
        const self = try allocator.create(Gate);
        errdefer allocator.destroy(self);
        const slots = try allocator.alloc(Slot, capacity);
        errdefer allocator.free(slots);
        for (slots) |*slot| slot.* = .{};
        self.* = .{ .allocator = allocator, .io = io, .baseline = baseline, .slots = slots, .admission = try bounded.Controller.initConfigured(1, .{ .max_queued_requests = @intCast(capacity), .max_queued_bytes = try std.math.mul(u64, capacity, @sizeOf(Slot)), .max_wait_ms = 3000 }) };
        return self;
    }
    pub fn reservedBytes(self: *const Gate) u64 {
        return self.baseline.bytes;
    }
    fn lock(self: *Gate) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn beginCall(self: *Gate) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ClientClosed;
        self.calls = std.math.add(usize, self.calls, 1) catch return error.OutboundCapacityUnavailable;
    }
    fn endCall(self: *Gate) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.calls != 0);
        self.calls -= 1;
    }
    fn reserveSlot(self: *Gate) !*Slot {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.ClientClosed;
        for (self.slots) |*slot| if (!slot.used) {
            slot.used = true;
            slot.entry = .{ .io = self.io, .bytes = @sizeOf(Slot) };
            return slot;
        };
        return error.OutboundCapacityUnavailable;
    }
    fn releaseSlot(self: *Gate, slot: *Slot) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(slot.used and slot.entry.state == .retired);
        slot.used = false;
    }
    fn failure(err: anyerror) anyerror {
        return switch (err) {
            error.AdmissionFull, error.AdmissionQueueFull, error.AdmissionBytesExhausted, error.AdmissionRequestTooLarge => error.OutboundCapacityUnavailable,
            error.AdmissionClosed => error.ClientClosed,
            else => err,
        };
    }
    fn tracked(self: *Gate, lease: bounded.Controller.Lease, options: Options) !Lease {
        var result = lease;
        result.clock = .{ .io = self.io, .native_now_ns = options.now_ns };
        result.started_ns = options.now_ns();
        self.lock();
        const unavailable = self.closed or self.next_generation == std.math.maxInt(u64);
        if (unavailable) {
            self.mutex.unlock();
            self.close();
            result.release();
            return error.ClientClosed;
        }
        std.debug.assert(self.active_generation == 0);
        const generation = self.next_generation;
        self.next_generation += 1;
        self.active_generation = generation;
        self.mutex.unlock();
        return .{ .gate = self, .generation = generation, .inner = result };
    }
    pub fn acquire(self: *Gate, options: Options) !Lease {
        return self.acquireInternal(options, false);
    }
    /// Only fixed, independently reserved node consumers may wait here without
    /// a runnable grant. Acquire before starting their bounded execution round.
    pub fn acquireIdle(self: *Gate, options: Options) !Lease {
        if (options.execution_wait != null) return error.InvalidArgument;
        return self.acquireInternal(options, true);
    }
    fn acquireInternal(self: *Gate, options: Options, idle: bool) !Lease {
        try options.check();
        try self.beginCall();
        defer self.endCall();
        if (self.admission.tryAcquireLease()) |value| {
            var lease = value;
            options.check() catch |err| {
                lease.release();
                return err;
            };
            return self.tracked(lease, options);
        }
        const wait = options.execution_wait;
        if (wait == null and !idle) return error.OutboundCapacityUnavailable;
        const slot = try self.reserveSlot();
        // enqueue failure never publishes this entry. Every published entry
        // must retire before its preallocated slot can be reused.
        const queued_started = options.now_ns();
        self.admission.enqueue(&slot.entry) catch |err| {
            slot.entry.state = .retired;
            self.releaseSlot(slot);
            return failure(err);
        };
        defer self.releaseSlot(slot);
        var retired = false;
        defer if (!retired) {
            if (self.admission.retire(&slot.entry, error.Canceled)) |value| {
                var abandoned = value;
                abandoned.release();
            } else |_| {}
        };
        if (!slot.entry.was_queued) {
            const invalid: ?anyerror = if (options.check()) |_| null else |err| err;
            const result = self.admission.retire(&slot.entry, invalid);
            retired = true;
            return self.tracked(result catch |err| return failure(err), options);
        }
        if (wait) |boundary| boundary.beginWait() catch |err| {
            if (self.admission.retire(&slot.entry, err)) |value| {
                var lease = value;
                lease.release();
            } else |_| {}
            retired = true;
            return err;
        };
        const queue_deadline = @min(options.deadline_ns, queued_started +| max_wait_ns);
        var invalid: ?anyerror = null;
        while (true) {
            options.check() catch |err| {
                invalid = err;
                break;
            };
            if (slot.entry.ready.isSet()) break;
            if (options.now_ns() >= queue_deadline) {
                invalid = error.AdmissionWaitTimeout;
                break;
            }
            const remaining = queue_deadline -| options.now_ns();
            slot.entry.ready.waitTimeout(self.io, .{ .duration = .{ .clock = .awake, .raw = .fromNanoseconds(@intCast(@min(remaining, 5 * std.time.ns_per_ms))) } }) catch |err| switch (err) {
                error.Timeout => {},
                else => {
                    invalid = err;
                    break;
                },
            };
        }
        const result = self.admission.retire(&slot.entry, invalid);
        retired = true;
        if (wait) |boundary| boundary.endWait() catch |err| {
            if (result) |value| {
                var lease = value;
                lease.release();
            } else |_| {}
            return err;
        };
        var lease = result catch |err| return failure(err);
        options.check() catch |err| {
            lease.release();
            return err;
        };
        return self.tracked(lease, options);
    }
    pub fn close(self: *Gate) void {
        self.lock();
        self.closed = true;
        self.mutex.unlock();
        self.admission.close();
    }
    /// A refused destruction keeps the complete owner and its reservation.
    /// The supervisor must join callers and release granted permits first.
    pub fn tryDestroy(self: *Gate) !void {
        self.close();
        self.lock();
        const busy = self.calls != 0;
        self.mutex.unlock();
        const stats = self.admission.stats();
        if (busy or stats.in_flight != 0 or stats.queued != 0 or stats.retiring_waiters != 0) return error.ControlTransportBusy;
        for (self.slots) |slot| std.debug.assert(!slot.used);
        const allocator = self.allocator;
        var baseline = self.baseline;
        allocator.free(self.slots);
        allocator.destroy(self);
        baseline.release();
    }
};

test "remote control admission reserves all queue metadata before allocation and refuses live destruction" {
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    for (0..2) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, Gate.create(failing.allocator(), std.testing.io, &scheduler));
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().control_retained_bytes);
    }
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    const reserved = gate.reservedBytes();
    try std.testing.expectEqual(try Gate.reservationBytes(8), reserved);
    try std.testing.expectEqual(reserved, scheduler.stats().control_retained_bytes);
    const deadline = clock.monotonicNs() + std.time.ns_per_s;
    var held = try gate.acquire(.{ .deadline_ns = deadline });
    try std.testing.expectError(error.OutboundCapacityUnavailable, gate.acquire(.{ .deadline_ns = deadline }));
    try std.testing.expectError(error.ControlTransportBusy, gate.tryDestroy());
    try std.testing.expectEqual(reserved, scheduler.stats().control_retained_bytes);
    try std.testing.expectError(error.ClientClosed, gate.acquire(.{ .deadline_ns = deadline }));
    held.release();
    try gate.tryDestroy();
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().control_retained_bytes);
}

test "remote control admission parks before granting and retires every failed wait handoff" {
    const Case = enum { grant, begin_denied, end_denied, canceled, expired, closed };
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
    };
    const Scope = struct {
        gate: *Gate,
        held: *Gate.Lease,
        scheduler: *scheduling.Scheduler,
        execution: *scheduling.Execution,
        signal: *http.RequestCancellation,
        case: Case,
        begins: usize = 0,
        ends: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.gate.mutex.tryLock());
            self.gate.mutex.unlock();
            try std.testing.expect(self.gate.admission.mutex.tryLock());
            self.gate.admission.mutex.unlock();
            self.begins += 1;
            if (self.case == .begin_denied) return error.Canceled;
            try self.execution.park(.resource_blocked);
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 200 }, .control);
            health.finish();
            switch (self.case) {
                .canceled => self.signal.cancel(),
                .expired => Clock.tick = 200,
                .closed => self.gate.close(),
                else => {},
            }
            if (self.case != .closed) self.held.release();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            if (self.case == .end_denied) return error.DeadlineExceeded;
            try self.execution.enter(.control);
        }
    };
    for (std.enums.values(Case)) |case| {
        Clock.tick = 100;
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
        defer scheduler.close();
        const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
        defer gate.tryDestroy() catch unreachable;
        var held = try gate.acquire(.{ .deadline_ns = 200, .now_ns = Clock.now });
        defer held.release();
        var execution: scheduling.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 200 }, .control);
        var signal: http.RequestCancellation = .{};
        var scope: Scope = .{ .gate = gate, .held = &held, .scheduler = &scheduler, .execution = &execution, .signal = &signal, .case = case };
        const result = gate.acquire(.{ .deadline_ns = 200, .cancellation = &signal, .now_ns = Clock.now, .execution_wait = .{ .ptr = &scope, .begin = Scope.begin, .end = Scope.end } });
        switch (case) {
            .grant => {
                var lease = try result;
                try std.testing.expectEqual(@as(usize, 1), gate.admission.stats().in_flight);
                try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
                lease.release();
            },
            .begin_denied, .canceled => try std.testing.expectError(error.Canceled, result),
            .end_denied, .expired => try std.testing.expectError(error.DeadlineExceeded, result),
            .closed => try std.testing.expectError(error.ClientClosed, result),
        }
        try std.testing.expectEqual(@as(usize, 1), scope.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_denied) 0 else 1), scope.ends);
        try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().queued);
        try std.testing.expectEqual(@as(u64, 0), gate.admission.stats().queued_bytes);
        for (gate.slots) |slot| try std.testing.expect(!slot.used);
        held.release();
        try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().in_flight);
    }
}

test "remote control admission rejects wrong owner contract stale and exhausted borrowed permits" {
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    const other = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer other.tryDestroy() catch unreachable;
    const deadline = clock.monotonicNs() + std.time.ns_per_s;
    var first = try gate.acquire(.{ .deadline_ns = deadline });
    const borrowed = first.borrow();
    try gate.validateBorrow(borrowed);
    try std.testing.expectError(error.InvalidArgument, other.validateBorrow(borrowed));
    var forged = borrowed;
    forged.contract.version +%= 1;
    forged.generation = std.math.maxInt(u64);
    try std.testing.expectError(error.UnsupportedVersion, gate.validateBorrow(forged));
    forged = borrowed;
    forged.generation += 1;
    try std.testing.expectError(error.InvalidArgument, gate.validateBorrow(forged));
    first.release();
    try std.testing.expectError(error.InvalidArgument, gate.validateBorrow(borrowed));
    var second = try gate.acquire(.{ .deadline_ns = deadline });
    try std.testing.expect(second.generation != borrowed.generation);
    try std.testing.expectError(error.InvalidArgument, gate.validateBorrow(borrowed));
    try gate.validateBorrow(second.borrow());
    second.release();
    gate.next_generation = std.math.maxInt(u64);
    try std.testing.expectError(error.ClientClosed, gate.acquire(.{ .deadline_ns = deadline }));
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().in_flight);
    try std.testing.expectError(error.InvalidArgument, gate.validateBorrow(borrowed));
}

test "remote control admission checked retirement callbacks preserve borrowed permit lifetime" {
    const core = @import("antfly_local_sources").common_read_admission.Retirement;
    const runtime = @import("antfly_runtime_abi");
    const Fixture = struct {
        gate: *Gate,
        parents: usize = 0,
        destroyed: usize = 0,
        polls: usize = 0,
        reconciles: usize = 0,
        var dispatches: usize = 0;
        fn retain(raw: *anyopaque, _: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parents += 1;
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parents -= 1;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed += 1;
        }
        fn poll(raw: *anyopaque, options: core.Poll) !core.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.gate.validateBorrow(options.control_permit orelse return error.InvalidArgument);
            self.polls += 1;
            return .needs_fence;
        }
        fn reconcile(raw: *anyopaque, options: core.Poll) !core.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.gate.validateBorrow(options.control_permit orelse return error.InvalidArgument);
            self.reconciles += 1;
            return .retired;
        }
        fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
            dispatches += 1;
            const resource: core.Resource = .{ .ptr = undefined, .poll = poll, .destroy = destroy };
            return resource.boundary_dispatch(contract, callback, args, output);
        }
    };
    Fixture.dispatches = 0;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    var fixture: Fixture = .{ .gate = gate };
    var registry = try core.Registry.init(std.testing.allocator, 1, 4096, 4096);
    const ticket = try registry.reserve(.{ .ptr = &fixture, .retain = Fixture.retain, .release = Fixture.release }, 0);
    try ticket.bind(.{ .ptr = &fixture, .poll = Fixture.poll, .reconcile = Fixture.reconcile, .destroy = Fixture.destroy, .boundary_dispatch = Fixture.dispatch });
    try ticket.publish();
    try ticket.abandon();
    const deadline = clock.monotonicNs() + std.time.ns_per_s;
    var first = try gate.acquireIdle(.{ .deadline_ns = deadline });
    defer first.release();
    const borrowed = first.borrow();
    var invalid = borrowed;
    invalid.contract.version +%= 1;
    try std.testing.expectEqual(core.Registry.Progress.pending, registry.pollOne(.{ .allocator = std.testing.allocator, .control_permit = invalid }));
    try std.testing.expectEqual(@as(usize, 0), fixture.polls);
    try std.testing.expectEqual(core.Registry.Progress.needs_fence, registry.pollOne(.{ .allocator = std.testing.allocator, .control_permit = borrowed }));
    first.release();
    var second = try gate.acquireIdle(.{ .deadline_ns = deadline });
    defer second.release();
    try std.testing.expectEqual(core.Registry.Progress.needs_fence, registry.reconcileOne(.{ .allocator = std.testing.allocator, .control_permit = borrowed }));
    try std.testing.expectEqual(@as(usize, 0), fixture.reconciles);
    try std.testing.expectEqual(@as(usize, 1), registry.snapshot().retained);
    try std.testing.expectEqual(@as(usize, 1), fixture.parents);
    try std.testing.expectEqual(@as(usize, 0), fixture.destroyed);
    try std.testing.expectEqual(core.Registry.Progress.retired, registry.reconcileOne(.{ .allocator = std.testing.allocator, .control_permit = second.borrow() }));
    try std.testing.expectEqual(@as(usize, 4), Fixture.dispatches);
    try std.testing.expectEqual(@as(usize, 1), fixture.polls);
    try std.testing.expectEqual(@as(usize, 1), fixture.reconciles);
    try std.testing.expectEqual(@as(usize, 1), fixture.destroyed);
    try std.testing.expectEqual(@as(usize, 0), fixture.parents);
    try std.testing.expectEqual(@as(usize, 0), registry.snapshot().retained);
    registry.close();
    try registry.deinit();
}

test "remote control admission precedes signing and keeps the original authenticated RPC budget" {
    const client_mod = @import("remote_attempt_client.zig");
    const attempts = @import("../common/remote_attempts.zig");
    const replies = @import("../common/remote_attempt_reply.zig");
    const wire = @import("../common/remote_attempt_wire.zig");
    const auth = @import("internal_service_auth.zig");
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
    };
    const Transport = struct {
        calls: usize = 0,
        remaining_ns: u64 = 0,
        expected_deadline: u64 = 200,
        fn execute(raw: *anyopaque, allocator: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const token = request.header(auth.header_name) orelse return error.TestMissingControlCredential;
            var fields = std.mem.splitScalar(u8, token, '.');
            _ = fields.next() orelse return error.TestMalformedControlCredential;
            const payload = fields.next() orelse return error.TestMalformedControlCredential;
            _ = fields.next() orelse return error.TestMalformedControlCredential;
            if (fields.next() != null) return error.TestMalformedControlCredential;
            const decoder = std.base64.url_safe_no_pad.Decoder;
            const decoded = try allocator.alloc(u8, try decoder.calcSizeForSlice(payload));
            defer allocator.free(decoded);
            try decoder.decode(decoded, payload);
            const Claims = struct { metadata: struct { remote_attempt: []const u8 } };
            var parsed = try std.json.parseFromSlice(Claims, allocator, decoded, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const context = try wire.Context.decode(parsed.value.metadata.remote_attempt);
            try std.testing.expectEqual(wire.Kind.status, context.kind);
            self.remaining_ns = context.remaining_ns;
            try std.testing.expectEqual(@as(?u64, self.expected_deadline), request.cancellation.?.execution_deadline_ns);
            return error.TestControlTransportReached;
        }
    };
    Clock.tick = 100;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    var permit = try gate.acquireIdle(.{ .deadline_ns = 200, .now_ns = Clock.now });
    defer permit.release();
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 64 }};
    var ledger = try attempts.Coordinator.init(7, 1, &slots, &destinations, 64);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    const targets = [_]client_mod.Target{.{ .base_uri = "http://127.0.0.1:1", .authority = authority }};
    var transport: Transport = .{};
    const executor: http.RequestExecutor = .{ .ptr = &transport, .vtable = &.{ .execute = Transport.execute } };
    var empty: [0]u8 = .{};
    var no_allocations = std.heap.FixedBufferAllocator.init(&empty);
    var client: client_mod.Client = .{ .ledger = &ledger, .store = undefined, .targets = &targets, .signing = .{ .secret = "control-admission-rpc-signing-secret-32", .issuer = "control-admission-test" }, .read_allocator = no_allocations.allocator(), .control_allocator = no_allocations.allocator(), .read_executor = executor, .control_executor = executor, .control_admission = gate, .control_rpc_timeout_ns = 20, .now_ns = Clock.now };
    const id: attempts.Id = .{ .coordinator = 7, .generation = 1, .operation = 9, .sequence = 1 };
    const sequence = ledger.next_sequence;
    try std.testing.expectError(error.OutboundCapacityUnavailable, client.status(0, id, 200));
    try std.testing.expectEqual(@as(usize, 0), transport.calls);
    var invalid = permit.borrow();
    invalid.contract.version +%= 1;
    client.control_permit = invalid;
    try std.testing.expectError(error.UnsupportedVersion, client.status(0, id, 200));
    try std.testing.expectEqual(@as(usize, 0), transport.calls);
    client.control_permit = permit.borrow();
    client.control_allocator = std.testing.allocator;
    Clock.tick = 150;
    client.control_rpc_timeout_ns = null;
    try std.testing.expectError(error.TestControlTransportReached, client.status(0, id, 200));
    try std.testing.expectEqual(@as(usize, 1), transport.calls);
    try std.testing.expectEqual(@as(u64, 50), transport.remaining_ns);
    client.control_rpc_timeout_ns = 20;
    transport.expected_deadline = 170;
    try std.testing.expectError(error.TestControlTransportReached, client.status(0, id, 200));
    try std.testing.expectEqual(@as(usize, 2), transport.calls);
    try std.testing.expectEqual(@as(u64, 20), transport.remaining_ns);
    try std.testing.expectEqual(sequence, ledger.next_sequence);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
    permit.release();
    client.control_allocator = no_allocations.allocator();
    try std.testing.expectError(error.InvalidArgument, client.status(0, id, 200));
    try std.testing.expectEqual(@as(usize, 2), transport.calls);
}
