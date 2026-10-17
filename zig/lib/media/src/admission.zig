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

//! Nonblocking shared admission. Share one Pool across request-local readers and
//! preparers. Tokens are move-only and release only after resource destruction.
const std = @import("std");
pub const Resources = struct {
    host_bytes: u64 = 0,
    device_bytes: u64 = 0,
    commands: u64 = 0,
};
pub const Pool = struct {
    limits: Resources,
    used: Resources = .{},
    high_water: Resources = .{},
    locked: std.atomic.Value(bool) = .init(false),
    fn lock(self: *Pool) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    fn unlock(self: *Pool) void {
        self.locked.store(false, .release);
    }
    /// Atomic across all dimensions; denial mutates no counters. The caller can
    /// retry through its scheduler, retaining its original cancellation deadline.
    pub fn acquire(self: *Pool, requested: Resources) !Token {
        self.lock();
        defer self.unlock();
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            if (@field(requested, name) > @field(self.limits, name) -| @field(self.used, name)) return error.SharedAdmissionExceeded;
        }
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            @field(self.used, name) += @field(requested, name);
            @field(self.high_water, name) = @max(@field(self.high_water, name), @field(self.used, name));
        }
        return .{ .pool = self, .resources = requested };
    }
    pub fn snapshot(self: *Pool) Resources {
        self.lock();
        defer self.unlock();
        return self.used;
    }
};
pub const Token = struct {
    pool: ?*Pool = null,
    resources: Resources = .{},
    /// Replace this reservation atomically. Growth denial retains the original
    /// token; shrinking releases only bytes whose owner has already freed them.
    pub fn resize(self: *Token, requested: Resources) !void {
        const pool = self.pool orelse {
            self.resources = requested;
            return;
        };
        pool.lock();
        defer pool.unlock();
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            const others = @field(pool.used, name) - @field(self.resources, name);
            if (@field(requested, name) > @field(pool.limits, name) -| others) return error.SharedAdmissionExceeded;
        }
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            @field(pool.used, name) = @field(pool.used, name) - @field(self.resources, name) + @field(requested, name);
            @field(pool.high_water, name) = @max(@field(pool.high_water, name), @field(pool.used, name));
        }
        self.resources = requested;
    }
    pub fn deinit(self: *Token) void {
        if (self.pool) |pool| {
            pool.lock();
            inline for (@typeInfo(Resources).@"struct".field_names) |name| {
                std.debug.assert(@field(pool.used, name) >= @field(self.resources, name));
                @field(pool.used, name) -= @field(self.resources, name);
            }
            pool.unlock();
        }
        self.* = .{};
    }
};
test "shared admission is atomic and retains outputs until their owner releases" {
    var pool = Pool{ .limits = .{ .host_bytes = 100, .device_bytes = 80, .commands = 2 } };
    var first = try pool.acquire(.{ .host_bytes = 70, .device_bytes = 50, .commands = 1 });
    defer first.deinit();
    try std.testing.expectError(error.SharedAdmissionExceeded, pool.acquire(.{ .host_bytes = 20, .device_bytes = 40 }));
    try std.testing.expectEqual(Resources{ .host_bytes = 70, .device_bytes = 50, .commands = 1 }, pool.snapshot());
    var second = try pool.acquire(.{ .host_bytes = 30, .device_bytes = 30, .commands = 1 });
    second.deinit();
    second.deinit();
    first.deinit();
    try std.testing.expectEqual(Resources{}, pool.snapshot());
    try std.testing.expectEqual(pool.limits, pool.high_water);
}

test "shared admission serializes competing native worker reservations" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const Worker = struct {
        fn run(pool: *Pool, successes: *std.atomic.Value(usize)) void {
            for (0..1000) |_| {
                var token = pool.acquire(.{ .host_bytes = 10, .commands = 1 }) catch {
                    std.atomic.spinLoopHint();
                    continue;
                };
                const used = pool.snapshot();
                std.debug.assert(used.host_bytes <= 20 and used.commands <= 2);
                _ = successes.fetchAdd(1, .monotonic);
                token.deinit();
            }
        }
    };
    var pool = Pool{ .limits = .{ .host_bytes = 20, .commands = 2 } };
    var successes = std.atomic.Value(usize).init(0);
    var workers: [4]std.Thread = undefined;
    var count: usize = 0;
    defer for (workers[0..count]) |worker| worker.join();
    for (&workers) |*worker| {
        worker.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, &successes });
        count += 1;
    }
    for (workers[0..count]) |worker| worker.join();
    count = 0;
    try std.testing.expect(successes.load(.monotonic) > 0);
    try std.testing.expectEqual(Resources{}, pool.snapshot());
}

test "shared admission resizing retains ownership on denial and releases parse workspace" {
    var pool = Pool{ .limits = .{ .host_bytes = 100, .device_bytes = 50 } };
    var parsing = try pool.acquire(.{ .host_bytes = 80 });
    defer parsing.deinit();
    try parsing.resize(.{ .host_bytes = 10 });
    var other = try pool.acquire(.{ .host_bytes = 60 });
    defer other.deinit();
    try std.testing.expectError(error.SharedAdmissionExceeded, parsing.resize(.{ .host_bytes = 50, .device_bytes = 20 }));
    try std.testing.expectEqual(Resources{ .host_bytes = 10 }, parsing.resources);
    try std.testing.expectEqual(Resources{ .host_bytes = 70 }, pool.snapshot());
    try parsing.resize(.{ .host_bytes = 40, .device_bytes = 20 });
    try std.testing.expectEqual(Resources{ .host_bytes = 100, .device_bytes = 20 }, pool.snapshot());
}

/// Stable allocator owner that admits every live allocation and potential
/// old/new moving-growth overlap before touching the backing allocator.
/// Keep this owner immovable while its allocator is in use. Free allocations
/// before releasing the token, or transfer allocations and token together to
/// the backing allocator after all temporary owners have been released.
pub const TrackedAllocator = struct {
    backing: std.mem.Allocator,
    reservation: @import("admission.zig").Token,
    limit: usize,
    live: usize = 0,
    denied: ?anyerror = null,
    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn reserve(self: *@This(), bytes: usize) bool {
        if (bytes > self.limit) {
            self.denied = error.ResourceLimitExceeded;
            return false;
        }
        self.reservation.resize(.{ .host_bytes = bytes }) catch |err| {
            self.denied = err;
            return false;
        };
        self.denied = null;
        return true;
    }
    fn restore(self: *@This(), bytes: usize) void {
        // Restoration only shrinks an existing reservation.
        self.reservation.resize(.{ .host_bytes = bytes }) catch unreachable;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const next = std.math.add(usize, self.live, len) catch {
            self.denied = error.ResourceLimitExceeded;
            return null;
        };
        if (!self.reserve(next)) return null;
        const result = self.backing.rawAlloc(len, alignment, ra) orelse {
            self.restore(self.live);
            self.denied = null;
            return null;
        };
        self.live = next;
        self.denied = null;
        return result;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (len <= bytes.len) {
            // Do not publish a smaller charge before the backing allocator has
            // actually shrunk; another request may consume the released room.
            if (!self.backing.rawResize(bytes, alignment, len, ra)) return false;
            self.live = self.live - bytes.len + len;
            self.restore(self.live);
            self.denied = null;
            return true;
        }
        const next = std.math.add(usize, self.live - bytes.len, len) catch {
            self.denied = error.ResourceLimitExceeded;
            return false;
        };
        if (!self.reserve(next)) return false;
        if (!self.backing.rawResize(bytes, alignment, len, ra)) {
            self.restore(self.live);
            return false;
        }
        self.live = next;
        self.denied = null;
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // Try explicit in-place resize before reserving a possible move.
        if (resize(raw, bytes, alignment, len, ra)) return bytes.ptr;
        const overlap = std.math.add(usize, self.live, len) catch {
            self.denied = error.ResourceLimitExceeded;
            return null;
        };
        if (!self.reserve(overlap)) return null;
        const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse {
            self.restore(self.live);
            return null;
        };
        self.live = self.live - bytes.len + len;
        self.restore(self.live);
        self.denied = null;
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.backing.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
        self.restore(self.live);
    }
};
