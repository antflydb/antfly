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
    key: u128 = 0,
    costs: [8]Cost = @splat(.{}),
    decisions: u64 = 0,
    fn candidate(width: usize, cap: usize) bool {
        return width <= cap and (std.math.isPowerOfTwo(width) or width == cap);
    }
    fn select(self: *Entry, cap: usize, depth: bool) usize {
        std.debug.assert(cap >= 1 and cap <= 8);
        self.decisions +%= 1;
        var candidates: [5]usize = undefined;
        var count: usize = 0;
        for (1..cap + 1) |width| if (candidate(width, cap)) {
            candidates[count] = width;
            count += 1;
        };
        // Frame cohorts explore upwards; layer groups preserve their qualified
        // deepest cold-start policy. Include the exact admission ceiling.
        for (0..count) |offset| {
            const width = candidates[if (depth) count - 1 - offset else offset];
            if (self.costs[width - 1].samples < 2) return width;
        }
        if (self.decisions % 32 == 0) return candidates[@intCast((self.decisions / 32) % count)];
        var best = candidates[if (depth) count - 1 else 0];
        for (candidates[0..count]) |width| {
            if (@as(u128, self.costs[width - 1].nanos_per_frame) * 105 < @as(u128, self.costs[best - 1].nanos_per_frame) * 100) best = width;
        }
        return best;
    }
    fn choose(self: *Entry, cap: usize) usize {
        return self.select(cap, false);
    }
    fn chooseDepth(self: *Entry, cap: usize) usize {
        return self.select(cap, true);
    }
    fn observe(self: *Entry, count: usize, elapsed: u64) void {
        if (count == 0 or count > 8) return;
        const cost = &self.costs[count - 1];
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
fn entry(metal: bool, width: u32, height: u32, batch: usize, prepared: bool) *Entry {
    const key = (@as(u128, width) << 64) | (@as(u128, height) << 32) | (@as(u128, batch) << 2) | (@as(u128, @intFromBool(prepared)) << 1) | @intFromBool(metal);
    for (&entries) |*item| if (item.key == key) return item;
    const item = &entries[eviction];
    eviction = (eviction + 1) % entries.len;
    item.* = .{ .key = key };
    return item;
}
pub fn capacity(metal: bool, width: u32, height: u32, maximum: usize) usize {
    return capacityForPlan(metal, width, height, maximum, false);
}
pub fn capacityForPlan(metal: bool, width: u32, height: u32, maximum: usize, prepared: bool) usize {
    std.debug.assert(maximum >= 1);
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    return entry(metal, width, height, 0, prepared).choose(@min(maximum, 8));
}
pub fn completed(metal: bool, width: u32, height: u32, count: usize, elapsed: u64) void {
    completedForPlan(metal, width, height, count, elapsed, false);
}
pub fn completedForPlan(metal: bool, width: u32, height: u32, count: usize, elapsed: u64, prepared: bool) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    entry(metal, width, height, 0, prepared).observe(count, elapsed);
}
/// Test diagnostics exercise actual serving routes, not just policy simulation.
pub fn frameSamplesForTesting(prepared: bool) [8]u64 {
    std.debug.assert(@import("builtin").is_test);
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    var samples: [8]u64 = @splat(0);
    for (entries) |item| {
        const category: u32 = @truncate(item.key);
        if (category != (1 | (@as(u32, @intFromBool(prepared)) << 1))) continue;
        for (item.costs, &samples) |cost, *sum| sum.* += cost.samples;
    }
    return samples;
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

/// Separate observations for each frame cohort: increasing batch size changes
/// command amortization and scratch pressure, so layer depth is never global.
pub fn layerCapacity(width: u32, height: u32, batch: usize, maximum: usize) usize {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    return entry(true, width, height, batch, false).chooseDepth(@min(maximum, 8));
}
pub fn layerCompleted(width: u32, height: u32, batch: usize, group: usize, layers: usize, elapsed: u64) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    entry(true, width, height, batch, false).observe(group, @intCast(@as(u128, elapsed) * group / @max(layers, 1)));
}

test "embeddinggemma2 video layer policy is independent for every frame batch" {
    for (0..2) |_| {
        layerCompleted(99, 77, 1, 1, 8, 800);
        layerCompleted(99, 77, 1, 2, 8, 400);
        layerCompleted(99, 77, 4, 1, 8, 400);
        layerCompleted(99, 77, 4, 2, 8, 800);
    }
    try std.testing.expectEqual(@as(usize, 2), layerCapacity(99, 77, 1, 2));
    try std.testing.expectEqual(@as(usize, 1), layerCapacity(99, 77, 4, 2));
    try std.testing.expectEqual(@as(usize, 1), layerCapacity(99, 77, 1, 1));
}

test "embeddinggemma2 video depth calibration preserves qualified grouping on cold starts and ties" {
    var policy = Entry{};
    try std.testing.expectEqual(@as(usize, 2), policy.chooseDepth(2));
    for (0..2) |_| {
        policy.observe(1, 100);
        policy.observe(2, 202);
    }
    try std.testing.expectEqual(@as(usize, 2), policy.chooseDepth(2));
    for (0..32) |_| policy.observe(1, 50);
    try std.testing.expectEqual(@as(usize, 1), policy.chooseDepth(2));
}

test "embeddinggemma2 calibration learns exact admitted widths and preserves smaller limits" {
    for ([_]usize{ 3, 5, 6, 7 }) |cap| {
        var policy = Entry{};
        for (1..cap + 1) |width| if (Entry.candidate(width, cap)) {
            for (0..2) |_| {
                try std.testing.expectEqual(width, policy.choose(cap));
                policy.observe(width, 1000 + width * 100);
            }
        };
        try std.testing.expectEqual(cap, policy.choose(cap));
        for (0..40) |_| try std.testing.expect(policy.choose(2) <= 2);
    }
    // Production geometry 576x528 admits six frames, not a power of two.
    const projector = @import("../architectures/gemma4_projector.zig");
    try std.testing.expectEqual(@as(usize, 6), try projector.embeddingGemma2VideoPreparedCapacity(576, 528, 768 * 1024 * 1024));
}

test "embeddinggemma2 prepared calibration is isolated from legacy fallback timings" {
    for ([_]bool{ false, true }) |prepared| for ([_]usize{ 1, 2, 4, 6 }) |width| for (0..2) |_| completedForPlan(true, 97, 51, width, if (prepared) 1000 + width * 100 else width * width * 100, prepared);
    try std.testing.expectEqual(@as(usize, 6), capacityForPlan(true, 97, 51, 6, true));
    try std.testing.expectEqual(@as(usize, 1), capacityForPlan(true, 97, 51, 6, false));
}

test "embeddinggemma2 frame calibration learns partial ready cohorts without waiting for a larger batch" {
    var policy = Entry{};
    const geometry_cap = 6;
    const ready = 3;
    for ([_]usize{ 1, 2, 3 }) |width| for (0..2) |_| {
        try std.testing.expectEqual(width, policy.choose(@min(ready, geometry_cap)));
        policy.observe(width, if (width == 2) 100 else width * 300);
    };
    for (0..100) |_| {
        const width = policy.choose(@min(ready, geometry_cap));
        try std.testing.expect(width <= ready);
        policy.observe(width, if (width == 2) 100 else width * 300);
    }
    try std.testing.expectEqual(@as(u32, 0), policy.costs[3].samples);
    try std.testing.expectEqual(@as(usize, 4), policy.choose(geometry_cap));
    var pressure = Entry{};
    for ([_]usize{ 1, 2, 4, 5 }) |width| for (0..2) |_| pressure.observe(width, if (width == 2) 100 else width * 300);
    try std.testing.expectEqual(@as(usize, 6), pressure.choose(6));
    // After live admission rejects six but grants five, reselect under five
    // instead of repeatedly executing its slower clipped width.
    try std.testing.expectEqual(@as(usize, 2), pressure.choose(5));
}

// Window costs include preparation, vision, composition, completed backbone and
// output readback. Keep them separate from frame/layer kernel observations.
const WindowEntry = struct {
    cost: Entry = .{},
    retry_at: u64 = 0,
    declines: u6 = 0,
};
var window_entries: [64]WindowEntry = @splat(.{});
var window_eviction: usize = 0;
fn windowEntry(key: u128) *WindowEntry {
    for (&window_entries) |*item| if (item.cost.key == key) return item;
    const item = &window_entries[window_eviction];
    window_eviction = (window_eviction + 1) % window_entries.len;
    item.* = .{ .cost = .{ .key = key } };
    return item;
}
pub fn windowCapacity(key: u128, maximum: usize) usize {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    const item = windowEntry(key);
    return item.cost.chooseDepth(if (item.cost.decisions < item.retry_at) 1 else @min(maximum, 2));
}
pub fn windowCompleted(key: u128, count: usize, elapsed: u64) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    const item = windowEntry(key);
    item.cost.observe(count, elapsed);
    if (count > 1) {
        item.declines = 0;
        item.retry_at = 0;
    }
}

/// Back off unavailable cohorts without treating a failed admission as a
/// completed batch observation. Retry eventually as shared pressure changes.
pub fn windowDeclined(key: u128) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    defer mutex.unlock();
    const item = windowEntry(key);
    item.declines = @min(item.declines +| 1, 6);
    item.retry_at = item.cost.decisions +| (@as(u64, 1) << item.declines);
}

test "embeddinggemma2 window calibration learns completed CPU and GPU cost independently" {
    const cpu_key = std.math.maxInt(u128) - 1;
    const gpu_key = std.math.maxInt(u128) - 2;
    for (0..2) |_| {
        windowCompleted(cpu_key, 1, 100);
        windowCompleted(cpu_key, 2, 240);
        windowCompleted(gpu_key, 1, 100);
        windowCompleted(gpu_key, 2, 140);
    }
    try std.testing.expectEqual(@as(usize, 1), windowCapacity(cpu_key, 2));
    try std.testing.expectEqual(@as(usize, 2), windowCapacity(gpu_key, 2));
    try std.testing.expectEqual(@as(usize, 1), windowCapacity(gpu_key, 1));
}

test "embeddinggemma2 window admission backoff retries without learning failed batch cost" {
    const key = std.math.maxInt(u128) - 3;
    try std.testing.expectEqual(@as(usize, 2), windowCapacity(key, 2));
    windowDeclined(key);
    try std.testing.expectEqual(@as(usize, 1), windowCapacity(key, 2));
    try std.testing.expectEqual(@as(usize, 1), windowCapacity(key, 2));
    try std.testing.expectEqual(@as(usize, 2), windowCapacity(key, 2));
    windowCompleted(key, 2, 100);
    try std.testing.expectEqual(@as(usize, 2), windowCapacity(key, 2));
}
