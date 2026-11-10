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
    head: ?*Attachment = null,
    tail: ?*Attachment = null,
    count: usize = 0,

    pub const Attachment = struct {
        registry: *Registry,
        allocator: std.mem.Allocator,
        context: *anyopaque,
        callback: *const fn (*anyopaque, u64) u64,
        next: ?*Attachment = null,
        in_flight: std.atomic.Value(usize) = .init(0),

        pub fn detach(self: *Attachment) void {
            const registry = self.registry;
            platform.sync.lockYielding(&registry.mutex);
            var previous: ?*Attachment = null;
            var cursor = registry.head;
            while (cursor) |entry| {
                if (entry == self) break;
                previous = entry;
                cursor = entry.next;
            }
            std.debug.assert(cursor == self);
            if (previous) |entry| entry.next = self.next else registry.head = self.next;
            if (registry.tail == self) registry.tail = previous;
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
        if (self.tail) |tail| tail.next = entry else self.head = entry;
        self.tail = entry;
        self.count += 1;
        return entry;
    }

    pub fn reclaim(self: *Registry, target: u64) u64 {
        if (target == 0) return 0;
        var selected: [16]*Attachment = undefined;
        platform.sync.lockYielding(&self.mutex);
        const count = @min(selected.len, self.count);
        var cursor = self.head;
        for (selected[0..count]) |*slot| {
            const entry = cursor.?;
            cursor = entry.next;
            _ = entry.in_flight.fetchAdd(1, .monotonic);
            slot.* = entry;
        }
        if (self.count > 1) {
            const first = self.head.?;
            self.head = first.next;
            first.next = null;
            self.tail.?.next = first;
            self.tail = first;
        }
        self.mutex.unlock();
        var reclaimed: u64 = 0;
        for (selected[0..count]) |entry| {
            if (reclaimed < target) reclaimed +|= entry.callback(entry.context, target - reclaimed);
            const previous = entry.in_flight.fetchSub(1, .release);
            std.debug.assert(previous > 0);
        }
        return reclaimed;
    }

    pub fn deinit(self: *Registry) void {
        std.debug.assert(self.head == null and self.tail == null and self.count == 0);
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
