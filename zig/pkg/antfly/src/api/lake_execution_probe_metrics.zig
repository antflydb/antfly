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

//! Qualification-root counters. No pointer to these values crosses an owner.
const std = @import("std");
pub const names = [_][]const u8{ "identity", "fragments", "spool", "aggregate", "ordered", "text", "sparse", "dense" };
pub const bulk_names = [_][]const u8{ "bulk_tee", "bulk_receipt", "dense_ingest", "dense_cluster", "dense_checkpoint", "dense_upload" };
pub const Metrics = struct {
    parallel_jobs: std.atomic.Value(u64) = .init(0),
    ns: [names.len]std.atomic.Value(u64) = @splat(.init(0)),
    calls: [names.len]std.atomic.Value(u64) = @splat(.init(0)),
    bulk_ns: [bulk_names.len]std.atomic.Value(u64) = @splat(.init(0)),
    bulk_calls: [bulk_names.len]std.atomic.Value(u64) = @splat(.init(0)),
    pub fn reset(self: *@This()) void {
        self.parallel_jobs.store(0, .monotonic);
        for (&self.ns, &self.calls) |*time, *calls| {
            time.store(0, .monotonic);
            calls.store(0, .monotonic);
        }
        for (&self.bulk_ns, &self.bulk_calls) |*time, *calls| {
            time.store(0, .monotonic);
            calls.store(0, .monotonic);
        }
    }
    pub fn record(self: *@This(), comptime name: []const u8, elapsed: u64) void {
        const index = comptime slot: {
            for (names ++ bulk_names, 0..) |candidate, slot_index| if (std.mem.eql(u8, name, candidate)) break :slot slot_index;
            @compileError("unknown physical family probe");
        };
        if (comptime index < names.len) {
            _ = self.ns[index].fetchAdd(elapsed, .monotonic);
            _ = self.calls[index].fetchAdd(1, .monotonic);
        } else {
            _ = self.bulk_ns[index - names.len].fetchAdd(elapsed, .monotonic);
            _ = self.bulk_calls[index - names.len].fetchAdd(1, .monotonic);
        }
    }
    pub fn times(self: *@This()) [names.len]u64 {
        var result: [names.len]u64 = undefined;
        for (&result, &self.ns) |*out, *value| out.* = value.load(.monotonic);
        return result;
    }
    pub fn counts(self: *@This()) [names.len]u64 {
        var result: [names.len]u64 = undefined;
        for (&result, &self.calls) |*out, *value| out.* = value.load(.monotonic);
        return result;
    }
};
pub fn decodedHits(cache: anytype) u64 {
    while (!cache.decoded.mutex.tryLock()) std.atomic.spinLoopHint();
    defer cache.decoded.mutex.unlock();
    return cache.decoded.hits;
}

test "parallel physical family probes preserve complete worker attribution" {
    var values: Metrics = .{};
    const Worker = struct {
        fn run(value: *Metrics) void {
            for (0..1000) |_| {
                value.record("aggregate", 7);
                value.record("dense", 5);
            }
        }
    };
    const first = try std.Thread.spawn(.{}, Worker.run, .{&values});
    const second = std.Thread.spawn(.{}, Worker.run, .{&values}) catch |err| {
        first.join();
        return err;
    };
    first.join();
    second.join();
    try std.testing.expectEqual(@as(u64, 14000), values.times()[3]);
    try std.testing.expectEqual(@as(u64, 10000), values.times()[7]);
    try std.testing.expectEqual(@as(u64, 2000), values.counts()[3]);
    try std.testing.expectEqual(@as(u64, 2000), values.counts()[7]);
    try std.testing.expectEqual(@as(u64, 0), values.counts()[5]);
}
