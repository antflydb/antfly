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

//! Shared endpoint health across immutable DNS snapshots and delivery retries.
//! Quarantine failed peers; admit one recovery probe after each cooldown.
const std = @import("std");
pub const Routing = struct {
    const Entry = struct { key: ?[32]u8 = null, failures: u8 = 0, until_ns: u64 = 0, probing: bool = false, generation: u64 = 0 };
    entries: [256]Entry = @splat(.{}),
    cursor: usize = 0,
    generation: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    cooldown_ms: u32 = 1000,
    probe_ms: u32 = 60_000,
    fn lock(self: *Routing) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn hash(url: []const u8) [32]u8 {
        var key: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(url, &key, .{});
        return key;
    }
    fn find(self: *Routing, key: [32]u8) ?*Entry {
        for (&self.entries) |*entry| if (entry.key) |value| {
            if (std.meta.eql(value, key)) return entry;
        };
        return null;
    }
    pub const Admission = struct { kind: enum { blocked, healthy, probe }, generation: u64 = 0 };
    pub fn admit(self: *Routing, url: []const u8, now: u64) Admission {
        const key = hash(url);
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(key) orelse return .{ .kind = .healthy };
        if (entry.failures == 0) return .{ .kind = .healthy };
        if (entry.until_ns > now) return .{ .kind = .blocked };
        entry.probing = true;
        self.generation +%= 1;
        entry.generation = self.generation;
        entry.until_ns = now +| @as(u64, self.probe_ms) * std.time.ns_per_ms;
        return .{ .kind = .probe, .generation = entry.generation };
    }
    pub fn success(self: *Routing, url: []const u8) void {
        const key = hash(url);
        self.lock();
        defer self.mutex.unlock();
        if (self.find(key)) |entry| entry.* = .{};
    }
    pub fn abandon(self: *Routing, url: []const u8, admission: Admission) void {
        if (admission.kind != .probe) return;
        const key = hash(url);
        self.lock();
        defer self.mutex.unlock();
        if (self.find(key)) |entry| if (entry.probing and entry.generation == admission.generation) {
            entry.probing = false;
            entry.until_ns = 0;
        };
    }
    pub fn failure(self: *Routing, url: []const u8, now: u64) void {
        const key = hash(url);
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(key) orelse free: {
            for (&self.entries) |*candidate| if (candidate.key == null) break :free candidate;
            const candidate = &self.entries[self.cursor];
            self.cursor = (self.cursor + 1) % self.entries.len;
            candidate.* = .{};
            break :free candidate;
        };
        entry.key = key;
        entry.failures = @min(@as(u8, 6), entry.failures +| 1);
        entry.probing = false;
        const delay_ms: u64 = @min(@as(u64, 30_000), @as(u64, self.cooldown_ms) << @as(u6, @intCast(entry.failures - 1)));
        entry.until_ns = now +| delay_ms * std.time.ns_per_ms;
    }
};

test "distributed SQL endpoint quarantine permits one recovery probe and fences abandoned generations" {
    var routing: Routing = .{};
    const url = "http://worker:12390";
    try std.testing.expectEqual(.healthy, routing.admit(url, 0).kind);
    routing.failure(url, 0);
    try std.testing.expectEqual(.blocked, routing.admit(url, 1).kind);
    const first = routing.admit(url, std.time.ns_per_s);
    try std.testing.expectEqual(.probe, first.kind);
    for (0..100) |_| try std.testing.expectEqual(.blocked, routing.admit(url, std.time.ns_per_s).kind);
    // A cancelled probe releases its reservation without poisoning peer health.
    routing.abandon(url, first);
    const second = routing.admit(url, std.time.ns_per_s + 1);
    try std.testing.expectEqual(.probe, second.kind);
    routing.abandon(url, first);
    try std.testing.expectEqual(.blocked, routing.admit(url, std.time.ns_per_s + 2).kind);
    routing.failure(url, std.time.ns_per_s + 3);
    try std.testing.expectEqual(.blocked, routing.admit(url, 2 * std.time.ns_per_s).kind);
    routing.success(url);
    try std.testing.expectEqual(.healthy, routing.admit(url, 2 * std.time.ns_per_s).kind);
}
