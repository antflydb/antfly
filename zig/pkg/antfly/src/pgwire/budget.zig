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

//! Per-connection allocation cap shared by messages, prepared statements,
//! portals and backend response arenas. Parallel SQL workers share this cap;
//! admission, child allocation and reclamation are serialized together.
//! The budget must keep its address until all workers and response leases close.
const std = @import("std");

/// Aggregate allocations through all connection budgets on one listener.
/// The owner outlives joined sessions; backend-global plan/cache allocations
/// retain their own budgets and are not included in this counter.
pub const Shared = @import("../common/allocation_budget.zig").Shared;

/// Stable bounded owner shared by frontend allocations and independent cleanup.
/// Child allocators must support concurrent calls and outlive physical frees.
/// Limit and shared owner configuration remain fixed while work is live.
pub const Budget = struct {
    child: std.mem.Allocator,
    limit: usize,
    used: std.atomic.Value(usize) = .init(0),
    /// Optional independent aggregate retained-state charge.
    shared: ?*Shared = null,

    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn reserve(self: *Budget, bytes: usize) bool {
        var observed = self.used.load(.acquire);
        while (observed <= self.limit and bytes <= self.limit - observed) {
            if (self.used.cmpxchgWeak(observed, observed + bytes, .acq_rel, .acquire)) |changed| observed = changed else {
                if (self.shared) |shared| if (!shared.reserve(bytes)) {
                    _ = self.used.fetchSub(bytes, .acq_rel);
                    return false;
                };
                return true;
            }
        }
        return false;
    }

    fn release(self: *Budget, bytes: usize) void {
        const previous = self.used.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
        if (self.shared) |shared| shared.release(bytes);
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (!self.reserve(len)) return null;
        return self.child.rawAlloc(len, alignment, ret_addr) orelse {
            self.release(len);
            return null;
        };
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (!self.reserve(growth)) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) {
            self.release(growth);
            return false;
        }
        self.release(memory.len -| new_len);
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (!self.reserve(growth)) return null;
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            self.release(growth);
            return null;
        };
        self.release(memory.len -| new_len);
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.child.rawFree(memory, alignment, ret_addr);
        self.release(memory.len);
    }
};

test "pgwire memory admission bounds and reclaims connection storage" {
    var budget = Budget{ .child = std.testing.allocator, .limit = 16 };
    const alloc = budget.allocator();
    const first = try alloc.alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 5));
    alloc.free(first);
    const second = try alloc.alloc(u8, 16);
    alloc.free(second);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
}

test "pgwire shared session budget bounds aggregate connections and reclaims capacity" {
    var shared: Shared = .{ .child = std.testing.allocator, .limit = 20 };
    var first: Budget = .{ .child = shared.allocator(), .limit = 16 };
    var second: Budget = .{ .child = shared.allocator(), .limit = 16 };
    const a = try first.allocator().alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 9));
    const b = try second.allocator().alloc(u8, 8);
    try std.testing.expectEqual(@as(usize, 20), shared.used.load(.acquire));
    try std.testing.expect(!first.allocator().resize(a, 13));
    second.allocator().free(b);
    first.allocator().free(a);
    const c = try second.allocator().alloc(u8, 16);
    second.allocator().free(c);
    try std.testing.expectEqual(@as(usize, 0), shared.used.load(.acquire));
}

test "pgwire shared session budget rolls back a failed backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var shared: Shared = .{ .child = failing.allocator(), .limit = 20 };
    try std.testing.expectError(error.OutOfMemory, shared.allocator().alloc(u8, 12));
    try std.testing.expectEqual(@as(usize, 0), shared.used.load(.acquire));
}

test "pgwire retained budget independently charges aggregate and connection allocation" {
    var memory: Shared = .{ .child = std.testing.allocator, .limit = 256 };
    var retained: Shared = .{ .child = std.testing.allocator, .limit = 20 };
    var connection: Budget = .{ .child = memory.allocator(), .limit = 128 };
    var a: Budget = .{ .child = connection.allocator(), .limit = 16, .shared = &retained };
    var b: Budget = .{ .child = connection.allocator(), .limit = 16, .shared = &retained };
    const first = try a.allocator().alloc(u8, 12);
    try std.testing.expectError(error.OutOfMemory, b.allocator().alloc(u8, 9));
    const second = try b.allocator().alloc(u8, 8);
    try std.testing.expect(!a.allocator().resize(first, 13));
    // Transient response/frame memory remains available for errors and controls.
    const transient = try connection.allocator().alloc(u8, 32);
    try std.testing.expectEqual(@as(usize, 20), retained.used.load(.acquire));
    b.allocator().free(second);
    a.allocator().free(first);
    connection.allocator().free(transient);
    try std.testing.expectEqual(@as(usize, 0), retained.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
}

test "pgwire retained budget rolls back failed backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var retained: Shared = .{ .child = std.testing.allocator, .limit = 100 };
    var budget: Budget = .{ .child = failing.allocator(), .limit = 100, .shared = &retained };
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 20));
    try std.testing.expectEqual(@as(usize, 0), retained.used.load(.acquire));
}

test "pgwire shared session budget admits concurrent foreground and cleanup against one parent cap" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Fixture = struct {
        io: std.Io,
        parent: *Budget,
        retained: *Shared,
        attempted: std.atomic.Value(usize) = .init(0),
        admitted: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),
        violations: std.atomic.Value(usize) = .init(0),
        ready: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(self: *@This()) std.Io.Cancelable!void {
            var local: Budget = .{ .child = self.parent.allocator(), .limit = 40, .shared = self.retained };
            const alloc = local.allocator();
            const body = alloc.alloc(u8, 40) catch blk: {
                _ = self.rejected.fetchAdd(1, .monotonic);
                break :blk null;
            };
            if (body != null) _ = self.admitted.fetchAdd(1, .monotonic);
            if (self.attempted.fetchAdd(1, .acq_rel) == 7) self.ready.set(self.io);
            self.release.wait(self.io) catch |err| {
                if (body) |bytes| alloc.free(bytes);
                return err;
            };
            if (body) |bytes| alloc.free(bytes);
            if (local.used.load(.acquire) != 0) _ = self.violations.fetchAdd(1, .monotonic);
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var memory: Shared = .{ .child = std.heap.smp_allocator, .limit = 120 };
    var retained: Shared = .{ .child = std.heap.smp_allocator, .limit = 120 };
    var parent: Budget = .{ .child = memory.allocator(), .limit = 120 };
    var fixture: Fixture = .{ .io = io, .parent = &parent, .retained = &retained };
    var group: std.Io.Group = .init;
    defer {
        fixture.release.set(io);
        group.cancel(io);
    }
    for (0..8) |_| try group.concurrent(io, Fixture.run, .{&fixture});
    try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, 3), fixture.admitted.load(.acquire));
    try std.testing.expectEqual(@as(usize, 5), fixture.rejected.load(.acquire));
    try std.testing.expectEqual(@as(usize, 120), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 120), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 120), retained.stats().used_bytes);
    try std.testing.expectError(error.OutOfMemory, parent.allocator().alloc(u8, 1));
    fixture.release.set(io);
    try group.await(io);
    try std.testing.expectEqual(@as(usize, 0), fixture.violations.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), retained.stats().used_bytes);
    const reused = try parent.allocator().alloc(u8, 120);
    parent.allocator().free(reused);
}

test "pgwire shared session budget keeps cleanup charged through a stalled physical free" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Backing = struct {
        io: std.Io,
        ready: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
        }
        fn allocate(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            return std.heap.smp_allocator.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const protection = self.io.swapCancelProtection(.blocked);
            defer _ = self.io.swapCancelProtection(protection);
            self.ready.set(self.io);
            self.release.wait(self.io) catch unreachable;
            std.heap.smp_allocator.rawFree(bytes, alignment, ret);
        }
        fn cleanup(alloc: std.mem.Allocator, body: []u8) void {
            alloc.free(body);
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    var backing: Backing = .{ .io = io };
    var memory: Shared = .{ .child = backing.allocator(), .limit = 32 };
    var retained: Shared = .{ .child = std.heap.smp_allocator, .limit = 32 };
    var parent: Budget = .{ .child = memory.allocator(), .limit = 32 };
    var cleanup: Budget = .{ .child = parent.allocator(), .limit = 32, .shared = &retained };
    const alloc = cleanup.allocator();
    const body = try alloc.alloc(u8, 24);
    // Backing rejects growth after local, parent and aggregate admission.
    // All layers must roll back to the original physical allocation size.
    try std.testing.expect(!alloc.resize(body, 32));
    try std.testing.expect(alloc.remap(body, 32) == null);
    try std.testing.expectEqual(@as(usize, 24), cleanup.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 24), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 24), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 24), retained.stats().used_bytes);
    var task = try io.concurrent(Backing.cleanup, .{ alloc, body });
    defer {
        backing.release.set(io);
        task.cancel(io);
    }
    try backing.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, 24), cleanup.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 24), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 24), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 24), retained.stats().used_bytes);
    try std.testing.expectError(error.OutOfMemory, parent.allocator().alloc(u8, 9));
    // An unrelated frontend allocation does not wait behind the stalled free.
    const frontend = try parent.allocator().alloc(u8, 8);
    try std.testing.expectEqual(@as(usize, 32), parent.used.load(.acquire));
    var protected: Budget = .{ .child = std.heap.smp_allocator, .limit = 8 };
    const control = try protected.allocator().alloc(u8, 8);
    protected.allocator().free(control);
    backing.release.set(io);
    task.await(io);
    try std.testing.expectEqual(@as(usize, 0), cleanup.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), retained.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 8), parent.used.load(.acquire));
    parent.allocator().free(frontend);
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
}

test "pgwire nested connection budgets serialize concurrent allocation and reclamation" {
    var parent: Budget = .{ .child = std.testing.allocator, .limit = 2048 };
    var budget: Budget = .{ .child = parent.allocator(), .limit = 1024 };
    const Worker = struct {
        a: std.mem.Allocator,
        failure: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            for (0..1024) |_| {
                const initial = self.a.alloc(u8, 64) catch {
                    self.failure.store(true, .release);
                    return;
                };
                const grown = self.a.realloc(initial, 128) catch {
                    self.a.free(initial);
                    self.failure.store(true, .release);
                    return;
                };
                const shrunk = self.a.realloc(grown, 32) catch {
                    self.a.free(grown);
                    self.failure.store(true, .release);
                    return;
                };
                self.a.free(shrunk);
                if (self.a.alloc(u8, 2049)) |invalid| {
                    self.a.free(invalid);
                    self.failure.store(true, .release);
                    return;
                } else |err| {
                    if (err != error.OutOfMemory) {
                        self.failure.store(true, .release);
                        return;
                    }
                }
            }
        }
    };
    var worker: Worker = .{ .a = budget.allocator() };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    {
        defer for (threads[0..started]) |thread| thread.join();
        for (&threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, Worker.run, .{&worker});
            started += 1;
        }
    }
    try std.testing.expect(!worker.failure.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.acquire));
}
