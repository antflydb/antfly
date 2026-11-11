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

//! A service borrows storage reclamation only while each attached owner lives.
//! State belongs to the service, not to a compilation unit. Callbacks run
//! outside registry locks; detach drains invocations before storage teardown.
const std = @import("std");
const platform = @import("antfly_platform");

pub const Registry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    allocator: std.mem.Allocator = std.heap.page_allocator,
    entries: std.ArrayListUnmanaged(?*Attachment) = .empty,
    count: usize = 0,
    cursor: usize = 0,
    next_identity: u64 = 1,

    pub const Attachment = struct {
        registry: *Registry,
        allocator: std.mem.Allocator,
        context: *anyopaque,
        callback: *const fn (*anyopaque, u64) u64,
        slot: usize = 0,
        identity: u64 = 0,
        in_flight: std.atomic.Value(usize) = .init(0),

        pub fn detach(self: *Attachment) void {
            const registry = self.registry;
            platform.sync.lockYielding(&registry.mutex);
            std.debug.assert(registry.entries.items[self.slot] == self);
            registry.entries.items[self.slot] = null;
            registry.count -= 1;
            registry.mutex.unlock();
            while (self.in_flight.load(.acquire) != 0) platform.time.yieldBriefly();
            self.allocator.destroy(self);
        }
    };

    pub fn attach(self: *Registry, allocator: std.mem.Allocator, context: *anyopaque, callback: *const fn (*anyopaque, u64) u64) !*Attachment {
        const entry = try allocator.create(Attachment);
        entry.* = .{ .registry = self, .allocator = allocator, .context = context, .callback = callback };
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        errdefer allocator.destroy(entry);
        const slot = for (self.entries.items, 0..) |value, index| {
            if (value == null) break index;
        } else blk: {
            try self.entries.append(self.allocator, null);
            break :blk self.entries.items.len - 1;
        };
        entry.slot = slot;
        entry.identity = self.next_identity;
        self.next_identity = std.math.add(u64, self.next_identity, 1) catch return error.OwnerIdentityExhausted;
        self.entries.items[slot] = entry;
        self.count += 1;
        return entry;
    }

    pub fn reclaim(self: *Registry, target: u64) u64 {
        if (target == 0) return 0;
        // Snapshot identities and the slot extent, not callback pointers. Each
        // bounded batch takes leases under the mutex; detach may retire any
        // unselected owner immediately. New/reused slots wait for another pass.
        platform.sync.lockYielding(&self.mutex);
        const extent = self.entries.items.len;
        if (extent == 0) {
            self.mutex.unlock();
            return 0;
        }
        const start = self.cursor % extent;
        self.cursor = (start + 1) % extent;
        const cutoff = self.next_identity - 1;
        self.mutex.unlock();
        var visited: usize = 0;
        var reclaimed: u64 = 0;
        while (visited < extent and reclaimed < target) {
            var selected: [16]*Attachment = undefined;
            var count: usize = 0;
            platform.sync.lockYielding(&self.mutex);
            while (visited < extent and count < selected.len) : (visited += 1) {
                const entry = self.entries.items[(start + visited) % extent] orelse continue;
                if (entry.identity > cutoff) continue;
                _ = entry.in_flight.fetchAdd(1, .monotonic);
                selected[count] = entry;
                count += 1;
            }
            self.mutex.unlock();
            for (selected[0..count]) |entry| {
                if (reclaimed < target) reclaimed +|= entry.callback(entry.context, target - reclaimed);
                const previous = entry.in_flight.fetchSub(1, .release);
                std.debug.assert(previous > 0);
            }
        }
        return reclaimed;
    }

    pub fn deinit(self: *Registry) void {
        std.debug.assert(self.count == 0);
        self.entries.deinit(self.allocator);
    }
};

test "host memory owners rotate fairly and callbacks run without registry locks" {
    const Probe = struct {
        registry: *Registry,
        calls: usize = 0,
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.registry.mutex.tryLock());
            self.registry.mutex.unlock();
            self.calls += 1;
            return target;
        }
    };
    var registry = Registry{};
    defer registry.deinit();
    var probes: [20]Probe = undefined;
    var attachments: [20]*Registry.Attachment = undefined;
    for (&probes, &attachments) |*probe, *entry| {
        probe.* = .{ .registry = &registry };
        entry.* = try registry.attach(std.testing.allocator, probe, Probe.reclaim);
    }
    defer for (attachments) |entry| entry.detach();
    // Selection rotates even when the first owner satisfies the entire target.
    for (0..20) |_| try std.testing.expectEqual(@as(u64, 64), registry.reclaim(64));
    for (probes) |probe| try std.testing.expect(probe.calls > 0);
}

test "host memory owners detach drains callbacks before storage is destroyed" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Gate = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        detached: std.atomic.Value(bool) = .init(false),
        fn reclaim(raw: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) platform.time.yieldBriefly();
            return 1;
        }
        fn run(registry: *Registry) void {
            _ = registry.reclaim(1);
        }
        fn detach(entry: *Registry.Attachment, gate: *@This()) void {
            entry.detach();
            gate.detached.store(true, .release);
        }
    };
    var registry = Registry{};
    defer registry.deinit();
    var gate = Gate{};
    const entry = try registry.attach(std.testing.allocator, &gate, Gate.reclaim);
    const callback = try std.Thread.spawn(.{}, Gate.run, .{&registry});
    defer callback.join();
    defer gate.release.store(true, .release);
    while (!gate.entered.load(.acquire)) platform.time.yieldBriefly();
    const detach = try std.Thread.spawn(.{}, Gate.detach, .{ entry, &gate });
    defer {
        gate.release.store(true, .release);
        detach.join();
    }
    while (true) {
        platform.sync.lockYielding(&registry.mutex);
        const removed = registry.count == 0;
        registry.mutex.unlock();
        if (removed) break;
        platform.time.yieldBriefly();
    }
    try std.testing.expect(!gate.detached.load(.acquire));
    gate.release.store(true, .release);
}

test "host memory owners visit later batches when the first sixteen cannot reclaim" {
    const Probe = struct {
        calls: usize = 0,
        bytes: u64 = 0,
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return @min(target, self.bytes);
        }
    };
    var registry = Registry{ .allocator = std.testing.allocator };
    defer registry.deinit();
    var probes: [40]Probe = @splat(.{});
    probes[39].bytes = 1024;
    var entries: [40]*Registry.Attachment = undefined;
    for (&probes, &entries) |*probe, *entry| entry.* = try registry.attach(std.testing.allocator, probe, Probe.reclaim);
    defer for (entries) |entry| entry.detach();
    try std.testing.expectEqual(@as(u64, 64), registry.reclaim(64));
    for (probes) |probe| try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
