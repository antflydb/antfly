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

//! Bounded, cancellable coordination for overlapping source/configuration work.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Registry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Entry) = .empty,
    const Entry = struct { key: [32]u8, mutex: std.atomic.Mutex = .unlocked, refs: usize = 0 };
    pub const Lease = struct {
        registry: *Registry,
        allocator: std.mem.Allocator,
        entry: *Entry,
        pub fn deinit(self: Lease) void {
            self.entry.mutex.unlock();
            self.registry.release(self.allocator, self.entry);
        }
    };
    fn release(self: *Registry, a: std.mem.Allocator, entry: *Entry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        entry.refs -= 1;
        if (entry.refs == 0) {
            _ = self.entries.remove(entry.key);
            a.destroy(entry);
        }
    }
    pub fn acquire(self: *Registry, a: std.mem.Allocator, key: [32]u8, control: Control) !Lease {
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        const entry = blk: {
            defer self.mutex.unlock();
            if (self.entries.get(key)) |entry| {
                entry.refs += 1;
                break :blk entry;
            }
            if (self.entries.count() >= 256) return error.ResourceTemporarilyUnavailable;
            const entry = try a.create(Entry);
            errdefer a.destroy(entry);
            entry.* = .{ .key = key, .refs = 1 };
            try self.entries.put(a, key, entry);
            break :blk entry;
        };
        control.lockWithPoll(&entry.mutex, 100 * std.time.ns_per_us) catch |err| {
            self.release(a, entry);
            return err;
        };
        return .{ .registry = self, .allocator = a, .entry = entry };
    }
    pub fn deinit(self: *Registry, a: std.mem.Allocator) void {
        std.debug.assert(self.entries.count() == 0);
        self.entries.deinit(a);
    }
};

test "model aware media speaker flights serialize one identity and release cancelled waiters" {
    const a = std.testing.allocator;
    var registry: Registry = .{};
    defer registry.deinit(a);
    const owner = try registry.acquire(a, @splat(1), .{});
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks > 2) return error.Cancelled;
        }
    };
    var cancel: Cancel = .{};
    try std.testing.expectError(error.Cancelled, registry.acquire(a, @splat(1), .{ .ptr = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 1), owner.entry.refs);
    const independent = try registry.acquire(a, @splat(2), .{});
    independent.deinit();
    const Waiter = struct {
        registry: *Registry,
        entered: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            const lease = self.registry.acquire(std.testing.allocator, @splat(1), .{}) catch |err| {
                self.failure = err;
                return;
            };
            defer lease.deinit();
            self.entered.store(true, .release);
        }
    };
    var waiter = Waiter{ .registry = &registry };
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{&waiter});
    while (true) {
        while (!registry.mutex.tryLock()) std.atomic.spinLoopHint();
        const waiting = owner.entry.refs == 2;
        registry.mutex.unlock();
        if (waiting) break;
        std.atomic.spinLoopHint();
    }
    try std.testing.expect(!waiter.entered.load(.acquire));
    owner.deinit();
    thread.join();
    if (waiter.failure) |err| return err;
    try std.testing.expect(waiter.entered.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), registry.entries.count());
}
