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

//! Bounded native owners for the shared collector. Never retain a request's
//! client, credentials, allocator, cancellation token, or root-reference clock.
const std = @import("std");
const objects = @import("objectstore");
const Collector = @import("../serverless/wal/collector.zig").Collector;
const Wal = @import("../serverless/wal/indexed_object_store.zig").IndexedObjectStore;
const A = std.mem.Allocator;
pub const Registry = struct {
    const Entry = struct {
        key: [32]u8,
        users: usize = 0,
        used: u64 = 0,
        lock: std.atomic.Mutex = .unlocked,
        state: ?Collector.ResumeState = null,
    };
    lock: std.atomic.Mutex = .unlocked,
    entries: [32]?*Entry = @splat(null),
    tick: u64 = 0,

    pub fn deinit(self: *@This()) void {
        for (self.entries) |entry| if (entry) |value| {
            std.debug.assert(value.users == 0);
            std.heap.page_allocator.destroy(value);
        };
        self.* = .{};
    }
    fn acquire(self: *@This(), key: [32]u8) !?*Entry {
        @import("antfly_platform").sync.lockYielding(&self.lock);
        defer self.lock.unlock();
        self.tick +%= 1;
        var selected: ?usize = null;
        for (self.entries, 0..) |entry, i| {
            if (entry) |value| {
                if (std.mem.eql(u8, &value.key, &key)) {
                    value.users += 1;
                    value.used = self.tick;
                    return value;
                }
                if (value.users == 0 and (selected == null or self.entries[selected.?] != null and value.used < self.entries[selected.?].?.used)) selected = i;
            } else selected = i;
        }
        const slot = selected orelse return null;
        const value = try std.heap.page_allocator.create(Entry);
        if (self.entries[slot]) |old| std.heap.page_allocator.destroy(old);
        value.* = .{ .key = key, .users = 1, .used = self.tick };
        self.entries[slot] = value;
        return value;
    }
    pub fn step(self: *@This(), a: A, key: [32]u8, wal: *Wal, io: std.Io, namespace: []const u8, maximum: usize, token: ?objects.CancellationToken) !Collector.Result {
        const entry = (try self.acquire(key)) orelse return .{ .phase = .deferred };
        defer {
            @import("antfly_platform").sync.lockYielding(&self.lock);
            entry.users -= 1;
            self.lock.unlock();
        }
        while (!entry.lock.tryLock()) {
            if (token) |cancel| try cancel.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        defer entry.lock.unlock();
        const collector = try a.create(Collector);
        defer a.destroy(collector);
        collector.* = Collector.init(a, wal, io);
        collector.inventory_limit = @intCast(@min(maximum, 16));
        if (entry.state) |state| collector.restoreState(state);
        // Suspend even on an error: local grace must remain conservative, and
        // the next request reconciles the durable job before continuing.
        defer entry.state = collector.suspendState(namespace);
        return collector.step(namespace, token);
    }
};

test "external lake shared collector registry resumes across request-scoped WAL owners" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const Clock = struct {
        now: u64 = std.time.ns_per_s,
        fn read(raw: ?*const anyopaque) u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return self.now;
        }
    };
    var clock: Clock = .{};
    var registry: Registry = .{};
    defer registry.deinit();
    {
        var wal = try Wal.init(a, client, "db", "request-owned");
        defer wal.deinit();
        wal.managed_reads = true;
        wal.read_clock = .{ .ptr = &clock, .now_fn = Clock.read };
        _ = try wal.appendIdempotent("events", 0, "old", "one");
        _ = try wal.appendIdempotent("events", 0, "retained", "two");
        _ = try wal.truncatePrefix("events", 2);
        while (try wal.compactPrefixStep("events", 1)) {}
    }
    var finished = false;
    for (0..2000) |_| {
        var wal = try Wal.init(a, client, "db", "request-owned");
        defer wal.deinit();
        wal.managed_reads = true;
        wal.read_clock = .{ .ptr = &clock, .now_fn = Clock.read };
        const result = try registry.step(a, @splat(1), &wal, std.testing.io, "events", 1, null);
        clock.now += if (result.phase == .waiting) Collector.grace_ns else std.time.ns_per_s;
        if (result.phase == .finished) {
            finished = true;
            try std.testing.expectEqual(@as(?u64, 1), try wal.findIdempotent("events", 0, "old", "one"));
            break;
        }
    }
    try std.testing.expect(finished);
}
