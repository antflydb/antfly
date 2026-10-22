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

//! Revocable local lifetime for retained mutation candidates. No execution,
//! policy, route or voting authority is minted by a registration.
const std = @import("std");
const time = @import("antfly_platform").time;
const admission = @import("../../common/read_admission.zig");

pub const Owner = struct {
    alloc: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    closing: std.atomic.Value(bool) = .init(false),
    closed: std.atomic.Value(bool) = .init(false),
    active: std.atomic.Value(usize) = .init(0),
    first: ?*Node = null,
    pending: ?*Node = null,
    finishing: std.atomic.Value(bool) = .init(false),

    /// The metadata allocator must outlive the owner and every registration.
    /// DBs use a process allocator, independently of their runtime.
    pub fn create(alloc: std.mem.Allocator) !*Owner {
        const self = try alloc.create(Owner);
        self.* = .{ .alloc = alloc };
        return self;
    }
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) time.yieldNow();
    }
    fn ref(self: *Owner) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *Owner) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            std.debug.assert(self.closed.load(.acquire));
            self.alloc.destroy(self);
        }
    }
    pub fn enter(self: *Owner) !Guard {
        self.lock();
        defer self.mutex.unlock();
        if (self.closing.load(.acquire)) return error.Canceled;
        self.ref();
        _ = self.active.fetchAdd(1, .acq_rel);
        return .{ .owner = self };
    }
    pub fn register(self: *Owner, alloc: std.mem.Allocator, ptr: *anyopaque, retire: *const fn (*anyopaque) void) !*Node {
        const node = try alloc.create(Node);
        errdefer alloc.destroy(node);
        self.lock();
        defer self.mutex.unlock();
        if (self.closing.load(.acquire)) return error.Canceled;
        node.* = .{ .alloc = alloc, .owner = self, .ptr = ptr, .retire_fn = retire, .next = self.first };
        if (self.first) |first| first.previous = node;
        self.first = node;
        self.ref();
        return node;
    }
    /// Fence new work and perform ready retirement under the caller's running
    /// ownership. Never wait for active guards or another retirement callback.
    /// Partial progress remains owned by `pending` across every retry/error.
    pub fn tryClose(self: *Owner) bool {
        self.lock();
        self.closing.store(true, .release);
        if (self.closed.load(.acquire)) {
            self.mutex.unlock();
            return true;
        }
        if (self.finishing.load(.acquire) or self.active.load(.acquire) != 0) {
            self.mutex.unlock();
            return false;
        }
        self.finishing.store(true, .release);
        if (self.pending == null) {
            self.pending = self.first;
            self.first = null;
            var node = self.pending;
            while (node) |entry| : (node = entry.next) entry.linked = false;
        }
        self.mutex.unlock();
        defer self.finishing.store(false, .release);
        while (self.pending) |entry| {
            const winner = entry.beginRetirement();
            if (!winner and entry.state.load(.acquire) != .retired) return false;
            // No new guards can enter after the fence; detached nodes still
            // have their list reference until their callback completes.
            std.debug.assert(entry.active.load(.acquire) == 0);
            entry.finishRetirement(winner);
            self.pending = entry.next;
            entry.unref();
        }
        self.closed.store(true, .release);
        return true;
    }

    fn readyToClose(self: *Owner) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed.load(.acquire)) return true;
        if (self.finishing.load(.acquire) or self.active.load(.acquire) != 0) return false;
        if (self.pending) |entry| if (entry.state.load(.acquire) == .retiring) return false;
        return true;
    }

    /// Compatibility shutdown for a caller whose execution ownership cannot
    /// prevent the drained work from progressing. Scheduled callers use the
    /// explicit original boundary below; no ambient authority is inferred.
    pub fn close(self: *Owner) void {
        while (!self.tryClose()) time.yieldNow();
    }

    /// Lend only across an actual guard/retirement wait, then rejoin before
    /// invoking callbacks or releasing list references. The original finite
    /// lifetime bounds the wait. Every failure leaves fenced, resumable state.
    pub fn closeWithAdmission(self: *Owner, original: admission.Port) !void {
        self.ref();
        defer self.release();
        self.lock();
        self.closing.store(true, .release);
        self.mutex.unlock();
        var trace: admission.RejectionTrace = .{};
        const control = trace.bind(original).?;
        const boundary = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        while (true) {
            try control.checkpoint();
            if (self.tryClose()) return;
            try boundary.beginWait();
            var failure: ?anyerror = null;
            while (!self.readyToClose()) {
                control.checkLifetime() catch |cause| {
                    failure = trace.failure orelse cause;
                    break;
                };
                time.yieldNow();
            }
            // Rejoin exactly once even after cancellation while parked. No
            // callback/metadata retirement is attempted after failed reentry.
            boundary.endWait() catch |cause| {
                if (failure) |first| return first;
                return trace.failure orelse cause;
            };
            if (failure) |cause| return cause;
        }
    }
};

pub const Node = struct {
    const State = enum(u8) { live, retiring, retired };
    alloc: std.mem.Allocator,
    owner: *Owner,
    ptr: *anyopaque,
    retire_fn: *const fn (*anyopaque) void,
    refs: std.atomic.Value(usize) = .init(2), // caller and registry list
    active: std.atomic.Value(usize) = .init(0),
    state: std.atomic.Value(State) = .init(.live),
    linked: bool = true,
    previous: ?*Node = null,
    next: ?*Node = null,

    pub fn enter(self: *Node) !Guard {
        const owner = self.owner;
        owner.lock();
        defer owner.mutex.unlock();
        if (owner.closing.load(.acquire) or self.state.load(.acquire) != .live) return error.Canceled;
        owner.ref();
        _ = owner.active.fetchAdd(1, .acq_rel);
        _ = self.refs.fetchAdd(1, .monotonic);
        _ = self.active.fetchAdd(1, .acq_rel);
        return .{ .owner = owner, .node = self };
    }
    fn beginRetirement(self: *Node) bool {
        return self.state.cmpxchgStrong(.live, .retiring, .acq_rel, .acquire) == null;
    }
    fn finishRetirement(self: *Node, winner: bool) void {
        if (winner) {
            while (self.active.load(.acquire) != 0) time.yieldNow();
            self.retire_fn(self.ptr);
            self.state.store(.retired, .release);
        } else while (self.state.load(.acquire) != .retired) time.yieldNow();
    }
    fn unref(self: *Node) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            const owner = self.owner;
            self.alloc.destroy(self);
            owner.release();
        }
    }
    pub fn release(self: *Node) void {
        const owner = self.owner;
        owner.lock();
        const linked = self.linked;
        if (linked) {
            if (self.previous) |previous| previous.next = self.next else owner.first = self.next;
            if (self.next) |next| next.previous = self.previous;
            self.linked = false;
            // Close must also drain a cancellation detached from its list.
            _ = owner.active.fetchAdd(1, .acq_rel);
        }
        const winner = self.beginRetirement();
        owner.mutex.unlock();
        self.finishRetirement(winner);
        if (linked) {
            _ = owner.active.fetchSub(1, .acq_rel);
            self.unref();
        }
        self.unref(); // Caller reference.
    }
};

pub const Guard = struct {
    owner: *Owner,
    node: ?*Node = null,
    pub fn check(self: *const Guard) !void {
        if (self.owner.closing.load(.acquire)) return error.Canceled;
        if (self.node) |node| if (node.state.load(.acquire) != .live) return error.Canceled;
    }
    pub fn deinit(self: *Guard) void {
        const owner = self.owner;
        if (self.node) |node| {
            _ = node.active.fetchSub(1, .acq_rel);
            node.unref();
        }
        _ = owner.active.fetchSub(1, .acq_rel);
        owner.release();
        self.* = undefined;
    }
};

/// Call-local adapter preserves the caller's execution boundary, deadlines,
/// retirement capability and original rejection. Never stored in a candidate.
pub const Access = struct {
    guard: ?Guard = null,
    trace: admission.RejectionTrace = .{},
    original: ?admission.Port = null,
    adapter: admission.Adapter(*Access) = undefined,
    payload: admission.Payload = undefined,
    pub fn bind(self: *Access, original: ?admission.Port) ?admission.Port {
        self.original = self.trace.bind(original);
        // A legacy uncontrolled caller must not acquire new I/O/admission
        // requirements. Its guard still drains the whole synchronous call.
        if (self.guard == null or self.original == null) return self.original;
        self.adapter = .{ .budget = self };
        self.payload = self.adapter.payload();
        return .init(&self.payload);
    }
    pub fn deinit(self: *Access) void {
        if (self.guard) |*guard| guard.deinit();
        self.guard = null;
    }
    pub fn checkpoint(self: *Access) !void {
        if (self.guard) |*guard| try guard.check();
        if (self.original) |original| try original.checkpoint();
        if (self.guard) |*guard| try guard.check();
    }
    pub fn checkLifetime(self: *Access) !void {
        if (self.guard) |*guard| try guard.check();
        if (self.original) |original| try original.checkLifetime();
    }
    pub fn waitForProgress(self: *Access) !void {
        try self.checkLifetime();
        if (self.original) |original| try original.waitForProgress() else time.yieldNow();
        try self.checkLifetime();
    }
    pub fn deadlineNs(self: *Access) !?u64 {
        try self.checkLifetime();
        return if (self.original) |original| try original.deadlineNs() else null;
    }
    pub fn retirementBinding(self: *Access) !?admission.Retirement.Binding {
        try self.checkLifetime();
        return if (self.original) |original| try original.retirementBinding() else null;
    }
    pub fn remoteExecutor(self: *Access) !?@import("../../common/http/http_common.zig").RequestExecutor {
        try self.checkLifetime();
        return if (self.original) |original| try original.remoteExecutor() else null;
    }
    pub fn executionBoundary(self: *Access) ?@import("../../common/execution_wait.zig").Boundary {
        return if (self.original != null) self.trace.payload.execution_wait else null;
    }
};

const Probe = struct {
    calls: std.atomic.Value(usize) = .init(0),
    owner: *Owner,
    unlocked: bool = false,
    fn retire(ptr: *anyopaque) void {
        const self: *Probe = @ptrCast(@alignCast(ptr));
        self.unlocked = self.owner.mutex.tryLock();
        if (self.unlocked) self.owner.mutex.unlock();
        _ = self.calls.fetchAdd(1, .acq_rel);
    }
};

test "prepared lifetime closes idle registrations before caller destruction without holding its lock" {
    const owner = try Owner.create(std.testing.allocator);
    var probe: Probe = .{ .owner = owner };
    const node = try owner.register(std.testing.allocator, &probe, Probe.retire);
    owner.close();
    owner.close();
    try std.testing.expectEqual(@as(usize, 1), probe.calls.load(.acquire));
    try std.testing.expect(probe.unlocked);
    try std.testing.expectError(error.Canceled, node.enter());
    try std.testing.expectError(error.Canceled, owner.register(std.testing.allocator, &probe, Probe.retire));
    owner.release(); // The node alone now owns the closed registry metadata.
    node.release();
}

test "prepared lifetime drains active guards and preserves execution wait reentry" {
    const owner = try Owner.create(std.testing.allocator);
    defer owner.release();
    defer owner.close();
    const Hook = struct {
        checkpoints: usize = 0,
        begins: usize = 0,
        ends: usize = 0,
        fn checkpoint(ptr: *anyopaque) !void {
            const h: *@This() = @ptrCast(@alignCast(ptr));
            h.checkpoints += 1;
        }
        fn lifetime(_: *anyopaque) !void {}
        fn begin(ptr: *anyopaque) !void {
            const h: *@This() = @ptrCast(@alignCast(ptr));
            h.begins += 1;
        }
        fn end(ptr: *anyopaque) !void {
            const h: *@This() = @ptrCast(@alignCast(ptr));
            h.ends += 1;
        }
        fn close(scope: *Owner) void {
            scope.close();
        }
    };
    var hook: Hook = .{};
    var original: admission.Payload = .{ .ptr = &hook, .vtable = &.{ .checkpoint = Hook.checkpoint, .check_lifetime = Hook.lifetime }, .execution_wait = .{ .ptr = &hook, .begin = Hook.begin, .end = Hook.end } };
    var access: Access = .{ .guard = try owner.enter() };
    defer access.deinit();
    const port = access.bind(.init(&original)).?;
    const wait = (try port.executionWaitAfterLifetime()).?;
    try std.testing.expectEqual(@as(usize, 0), hook.checkpoints);
    try wait.beginWait();
    const thread = try std.Thread.spawn(.{}, Hook.close, .{owner});
    defer thread.join();
    defer access.deinit();
    while (!owner.closing.load(.acquire)) time.yieldNow();
    try std.testing.expect(!owner.closed.load(.acquire));
    try std.testing.expectError(error.Canceled, port.checkLifetime());
    try wait.endWait();
    try std.testing.expectEqual(@as(usize, 1), hook.begins);
    try std.testing.expectEqual(@as(usize, 1), hook.ends);
    access.deinit();
}

test "prepared lifetime racing close and caller retirement invokes cleanup exactly once" {
    for (0..32) |_| {
        const owner = try Owner.create(std.testing.allocator);
        defer owner.release();
        var probe: Probe = .{ .owner = owner };
        const node = try owner.register(std.testing.allocator, &probe, Probe.retire);
        const Close = struct {
            fn run(scope: *Owner) void {
                scope.close();
            }
        };
        const thread = try std.Thread.spawn(.{}, Close.run, .{owner});
        node.release();
        thread.join();
        try std.testing.expectEqual(@as(usize, 1), probe.calls.load(.acquire));
    }
}

test "prepared lifetime registration allocation failures and original rejections retain ownership" {
    const F = struct {
        fn run(a: std.mem.Allocator) !void {
            const owner = try Owner.create(a);
            defer owner.release();
            defer owner.close();
            var probe: Probe = .{ .owner = owner };
            const node = try owner.register(a, &probe, Probe.retire);
            defer node.release();
            var access: Access = .{ .guard = try node.enter() };
            defer access.deinit();
            const Fail = struct {
                fn check(_: *anyopaque) !void {
                    return error.NotFound;
                }
            };
            var payload: admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = Fail.check } };
            try std.testing.expectError(error.NotFound, access.bind(.init(&payload)).?.checkpoint());
            try std.testing.expectEqual(error.NotFound, access.trace.failure.?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "prepared lifetime execution drain preserves begin end failures and retryable retirement" {
    const Case = enum { success, begin_failure, end_failure, deadline_in_wait };
    const F = struct {
        case: Case,
        access: *Access,
        running: bool = true,
        begins: usize = 0,
        ends: usize = 0,
        callbacks: usize = 0,
        retired_parked: bool = false,
        now: u64 = 0,
        fn check(ptr: *anyopaque) !void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            if (f.now >= 1000) return error.DeadlineExceeded;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
        fn begin(ptr: *anyopaque) !void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.begins += 1;
            if (f.case == .begin_failure) return error.DeadlineExceeded;
            f.running = false;
            if (f.case == .deadline_in_wait) f.now = 1000 else f.access.deinit();
        }
        fn end(ptr: *anyopaque) !void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.ends += 1;
            if (f.case == .end_failure) return error.Canceled;
            f.running = true;
        }
        fn retire(ptr: *anyopaque) void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.callbacks += 1;
            f.retired_parked = !f.running;
        }
    };
    for (std.enums.values(Case)) |case| {
        const owner = try Owner.create(std.testing.allocator);
        defer owner.release();
        defer owner.close();
        var access: Access = .{ .guard = try owner.enter() };
        defer access.deinit();
        var f: F = .{ .case = case, .access = &access };
        const node = try owner.register(std.testing.allocator, &f, F.retire);
        defer node.release();
        var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
        const result = owner.closeWithAdmission(.init(&payload));
        switch (case) {
            .success => {
                try result;
                try std.testing.expectEqual(@as(usize, 1), f.callbacks);
            },
            .begin_failure, .end_failure, .deadline_in_wait => {
                if (case != .end_failure) try std.testing.expectError(error.DeadlineExceeded, result) else try std.testing.expectError(error.Canceled, result);
                try std.testing.expect(owner.closing.load(.acquire) and !owner.closed.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), f.callbacks);
                try std.testing.expectError(error.Canceled, node.enter());
                // The original cleanup owner retries; no replacement execution
                // or lease is acquired by the registry after reentry fails.
                access.deinit();
                f.running = true;
                owner.close();
                try std.testing.expectEqual(@as(usize, 1), f.callbacks);
            },
        }
        try std.testing.expect(!f.retired_parked);
        try std.testing.expectEqual(@as(usize, 1), f.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), f.ends);
    }
}

test "prepared lifetime execution drain retains a detached tail after competing retirement and wait failure" {
    const F = struct {
        slow: ?*Node = null,
        fast_started: std.atomic.Value(bool) = .init(false),
        slow_started: std.atomic.Value(bool) = .init(false),
        allow: std.atomic.Value(bool) = .init(false),
        fast_calls: usize = 0,
        slow_calls: usize = 0,
        fn fast(ptr: *anyopaque) void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.fast_calls += 1;
            f.fast_started.store(true, .release);
            while (!f.slow_started.load(.acquire)) time.yieldNow();
        }
        fn slowRetire(ptr: *anyopaque) void {
            const f: *@This() = @ptrCast(@alignCast(ptr));
            f.slow_calls += 1;
            f.slow_started.store(true, .release);
            while (!f.allow.load(.acquire)) time.yieldNow();
        }
        fn run(f: *@This()) void {
            while (!f.fast_started.load(.acquire) and !f.allow.load(.acquire)) time.yieldNow();
            f.slow.?.release();
        }
        fn fail(_: *anyopaque) !void {
            return error.NotFound;
        }
        fn end(_: *anyopaque) !void {}
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
    };
    const owner = try Owner.create(std.testing.allocator);
    defer owner.release();
    defer owner.close();
    var f: F = .{};
    f.slow = try owner.register(std.testing.allocator, &f, F.slowRetire);
    const fast = try owner.register(std.testing.allocator, &f, F.fast);
    defer fast.release();
    const thread = try std.Thread.spawn(.{}, F.run, .{&f});
    var joined = false;
    defer {
        f.allow.store(true, .release);
        if (!joined) thread.join();
    }
    try std.testing.expect(!owner.tryClose());
    try std.testing.expectEqual(@as(usize, 1), f.fast_calls);
    try std.testing.expect(owner.pending == f.slow);
    var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.fail, .end = F.end } };
    try std.testing.expectError(error.NotFound, owner.closeWithAdmission(.init(&payload)));
    try std.testing.expect(!owner.closed.load(.acquire));
    f.allow.store(true, .release);
    thread.join();
    joined = true;
    owner.close();
    try std.testing.expectEqual(@as(usize, 1), f.fast_calls);
    try std.testing.expectEqual(@as(usize, 1), f.slow_calls);
    try std.testing.expect(owner.pending == null);
}

test "prepared lifetime execution drain requires original finite wait metadata" {
    const owner = try Owner.create(std.testing.allocator);
    defer owner.release();
    defer owner.close();
    var probe: Probe = .{ .owner = owner };
    const node = try owner.register(std.testing.allocator, &probe, Probe.retire);
    defer node.release();
    const F = struct {
        fn check(_: *anyopaque) !void {}
    };
    var payload: admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check } };
    try std.testing.expectError(error.ExecutionWaitUnsupported, owner.closeWithAdmission(.init(&payload)));
    payload.execution_wait = .{ .ptr = &probe, .begin = F.check, .end = F.check };
    try std.testing.expectError(error.ExecutionWaitUnsupported, owner.closeWithAdmission(.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), probe.calls.load(.acquire));
    try std.testing.expect(owner.closing.load(.acquire) and !owner.closed.load(.acquire));
    owner.close();
    try std.testing.expectEqual(@as(usize, 1), probe.calls.load(.acquire));
}
