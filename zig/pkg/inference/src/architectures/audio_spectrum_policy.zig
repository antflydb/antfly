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

//! Bounded timing policy: sample actual completed CPU preparation, never run
//! duplicate model work. Each route's first use is cold and excluded.
const std = @import("std");
const platform = @import("antfly_platform");
const Entry = struct { frames: usize = 0, generation: u64 = 0, last: u64 = 0, uses: u64 = 0, count: [2]u8 = .{ 0, 0 }, ns: [2]u64 = .{ 0, 0 } };
pub const Ticket = struct { slot: usize, generation: u64, frames: usize, fft: bool };
pub const Policy = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: [8]Entry = @splat(.{}),
    clock: u64 = 0,
    pub fn choose(self: *@This(), frames: usize) Ticket {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.clock +|= 1;
        var slot: usize = 0;
        for (&self.entries, 0..) |*e, i| {
            if (e.frames == frames) {
                slot = i;
                break;
            }
            if (e.last < self.entries[slot].last) slot = i;
        } else {
            self.entries[slot] = .{ .frames = frames, .generation = self.clock };
        }
        const e = &self.entries[slot];
        e.last = self.clock;
        e.uses +%= 1;
        const wins = @as(u128, e.ns[1]) * 100 < @as(u128, e.ns[0]) * 95;
        const fft = if (e.count[0] < 3) false else if (e.count[1] < 3) true else if (e.uses % 64 == 0) !wins else wins;
        return .{ .slot = slot, .generation = e.generation, .frames = frames, .fft = fft };
    }
    pub fn completed(self: *@This(), ticket: Ticket, ns: u64) void {
        if (ns == 0) return;
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const e = &self.entries[ticket.slot];
        if (e.generation != ticket.generation or e.frames != ticket.frames) return;
        const i: @TypeOf(ticket.slot) = @intFromBool(ticket.fft);
        if (e.count[i] > 0) e.ns[i] = if (e.count[i] == 1) ns else @intCast((@as(u128, e.ns[i]) * 3 + ns) / 4);
        e.count[i] +|= 1;
    }
};
pub var cpu: Policy = .{};
test "embeddinggemma2 spectrum timing excludes cold setup and retains faster completed CPU route" {
    var p = Policy{};
    for (0..6) |_| {
        const t = p.choose(200);
        p.completed(t, if (t.fft) 200 else 100);
    }
    try std.testing.expect(!p.choose(200).fft);
    var q = Policy{};
    for (0..6) |_| {
        const t = q.choose(200);
        q.completed(t, if (t.fft) 50 else 100);
    }
    try std.testing.expect(q.choose(200).fft);
    const stale = q.choose(200);
    for (1..20) |i| {
        const t = q.choose(i);
        q.completed(t, 10);
    }
    q.completed(stale, 999999);
    try std.testing.expectEqual(@as(usize, 8), q.entries.len);
}
