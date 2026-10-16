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

//! Bounded process-local calibration of completed vision cost per frame.
//! No waiting to fill a batch. Admission remains an independent hard ceiling.
const std = @import("std");
const widths = [_]usize{ 1, 2, 4, 8 };
const Cost = struct { samples: u32 = 0, nanos_per_frame: u64 = 0, pending: u64 = 0 };
const Entry = struct {
    key: u64 = 0,
    costs: [4]Cost = @splat(.{}),
    decisions: u64 = 0,
    fn choose(self: *Entry, cap: usize) usize {
        self.decisions +%= 1;
        // Two completed observations for every admissible candidate, followed
        // by sparse exploration so changing contention/hardware can be learned.
        var candidates: usize = 0;
        for (widths, 0..) |width, i| {
            if (width > cap) break;
            candidates += 1;
            if (self.costs[i].samples < 2) return width;
        }
        if (self.decisions % 32 == 0) return widths[@intCast((self.decisions / 32) % candidates)];
        var best: usize = 0;
        for (1..candidates) |i| {
            // Prefer the smaller allocation unless throughput improves >5%.
            if (@as(u128, self.costs[i].nanos_per_frame) * 105 < @as(u128, self.costs[best].nanos_per_frame) * 100) best = i;
        }
        return widths[best];
    }
    fn observe(self: *Entry, count: usize, elapsed: u64) void {
        const i = std.mem.indexOfScalar(usize, &widths, count) orelse return;
        const cost = &self.costs[i];
        const sample = @max(@as(u64, 1), elapsed / count);
        // Pair minima exclude one-off cold weight uploads and scheduling
        // stalls. Smoothed successive pairs still adapt to sustained changes.
        if (cost.samples % 2 == 0) {
            cost.pending = sample;
        } else {
            const warm = @min(cost.pending, sample);
            cost.nanos_per_frame = if (cost.samples == 1) warm else @intCast((@as(u128, cost.nanos_per_frame) * 3 + warm) / 4);
        }
        cost.samples +|= 1;
    }
};
var mutex: std.atomic.Mutex = .unlocked;
var entries: [64]Entry = @splat(.{});
var eviction: usize = 0;
fn entry(metal: bool, width: u32, height: u32) *Entry {
    const key = (@as(u64, width) << 32) | (@as(u64, height) << 1) | @intFromBool(metal);
    for (&entries) |*item| if (item.key == key) return item;
    const item = &entries[eviction];
    eviction = (eviction + 1) % entries.len;
    item.* = .{ .key = key };
    return item;
}
pub fn capacity(metal: bool, width: u32, height: u32, maximum: usize) usize {
    std.debug.assert(maximum >= 1);
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    return entry(metal, width, height).choose(@min(maximum, 8));
}
pub fn completed(metal: bool, width: u32, height: u32, count: usize, elapsed: u64) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    entry(metal, width, height).observe(count, elapsed);
}
test "embeddinggemma2 video batch policy learns completed throughput within admission" {
    var cpu = Entry{};
    for (widths) |width| for (0..2) |_| {
        try std.testing.expectEqual(width, cpu.choose(8));
        cpu.observe(width, width * (100 + width * 10));
    };
    try std.testing.expectEqual(@as(usize, 1), cpu.choose(8));
    var gpu = Entry{};
    for (widths) |width| for (0..2) |_| {
        _ = gpu.choose(8);
        gpu.observe(width, 800 + width * 100);
    };
    try std.testing.expectEqual(@as(usize, 8), gpu.choose(8));
    try std.testing.expect(gpu.choose(2) <= 2);
    for (0..128) |_| try std.testing.expect(gpu.choose(1) == 1);
}

test "embeddinggemma2 video batch calibration separates cold setup from warm cost" {
    var policy = Entry{};
    for (widths) |width| {
        policy.observe(width, 1_000_000 * width);
        policy.observe(width, (100 + width * 20) * width);
    }
    try std.testing.expectEqual(@as(usize, 1), policy.choose(8));
    // Sustained changes eventually replace the initial warm estimate.
    for (0..32) |_| policy.observe(1, 10_000);
    try std.testing.expect(policy.choose(8) > 1);
}
