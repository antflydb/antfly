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
    /// Fence new work, drain active operations, and revoke idle registrations.
    /// Retirement callbacks never run under the registry lock.
    pub fn close(self: *Owner) void {
        self.lock();
        const already_closing = self.closing.swap(true, .acq_rel);
        self.mutex.unlock();
        if (already_closing) {
            while (!self.closed.load(.acquire)) time.yieldNow();
            return;
        }
        const head = while (true) {
            self.lock();
            if (self.active.load(.acquire) == 0) {
                const first = self.first;
                self.first = null;
                var node = first;
                while (node) |entry| : (node = entry.next) entry.linked = false;
                self.mutex.unlock();
                break first;
            }
            self.mutex.unlock();
            time.yieldNow();
        };
        var node = head;
        while (node) |entry| {
            const next = entry.next;
            entry.finishRetirement(entry.beginRetirement());
            entry.unref(); // Detached list reference.
            node = next;
        }
        self.closed.store(true, .release);
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
