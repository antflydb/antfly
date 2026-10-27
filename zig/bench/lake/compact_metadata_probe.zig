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

//! Alternating production-code kernel comparison; see README.md for invocation.
const std = @import("std");
const before = @import("before");
const after = @import("after");
fn run(comptime T: type, name: []const u8, sample: usize) !void {
    const a = std.heap.smp_allocator;
    var memory: T.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var store = memory.store();
    store.prefix_keys = true;
    const width = 131000;
    const keys = try a.alloc([width]u8, 64);
    defer a.free(keys);
    var builder: T.SortedBuilder = .{ .alloc = a, .store = store, .target_bytes = T.bulk_target_page_bytes };
    defer builder.deinit();
    for (keys, 0..) |*key, i| {
        @memset(key, 'w');
        std.mem.writeInt(u64, key[width - 8 ..], i, .big);
        try builder.add(key, "v");
    }
    const root = (try builder.finish()).?;
    var cache: T.Cache = .{ .alloc = a, .underlying = store, .decoded = true, .max_bytes = 16 * 1024 * 1024 };
    defer cache.deinit();
    {
        var cursor = try T.Cursor.init(a, cache.store(), root, "", null);
        defer cursor.deinit();
        while (try cursor.nextView()) |_| {}
    }
    const input = [_][]const u8{ &keys[2], &keys[7], &keys[12], &keys[23], &keys[31], &keys[42], &keys[54], &keys[63] };
    const reads = memory.reads;
    for (0..6) |kernel| {
        const iterations: usize = if (kernel == 0 or kernel == 1 or kernel == 2) 256 else 32;
        var allocator = std.testing.FailingAllocator.init(a, .{});
        const ca = allocator.allocator();
        const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        for (0..iterations) |_| {
            switch (kernel) {
                0 => try std.testing.expectEqual(@as(u64, 47), try T.rankOf(ca, cache.store(), root, &keys[47])),
                1 => try std.testing.expectEqual(@as(u64, 64), try T.countRange(ca, cache.store(), root, "", null)),
                2 => try std.testing.expectEqual(@as(u64, 45), try T.countRange(ca, cache.store(), root, &keys[7], &keys[52])),
                3 => {
                    var ranks: [input.len]u64 = undefined;
                    try T.ranksOf(ca, cache.store(), root, &input, &ranks);
                    try std.testing.expectEqualSlices(u64, &.{ 2, 7, 12, 23, 31, 42, 54, 63 }, &ranks);
                },
                4 => {
                    const values = try T.lookupMany(ca, cache.store(), root, &input);
                    defer {
                        for (values) |entry| if (entry) |value| value.deinit(ca);
                        ca.free(values);
                    }
                    for (values, input) |value, key| try std.testing.expectEqualSlices(u8, key, value.?.key);
                },
                5 => {
                    var found: [input.len]bool = undefined;
                    try T.containsMany(ca, cache.store(), root, &input, &found);
                    for (found) |present| try std.testing.expect(present);
                },
                else => unreachable,
            }
        }
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
        try std.testing.expectEqual(allocator.allocated_bytes, allocator.freed_bytes);
        try std.testing.expectEqual(reads, memory.reads);
        std.debug.print("sample={} variant={s} kernel={} iterations={} elapsed_ns={} allocated_bytes={} allocations={} reads={}\n", .{ sample, name, kernel, iterations, elapsed, allocator.allocated_bytes, allocator.allocations, memory.reads - reads });
    }
}
test "compact_metadata_production_comparison" {
    for (0..3) |sample| {
        if (sample % 2 == 0) {
            try run(before, "before", sample);
            try run(after, "after", sample);
        } else {
            try run(after, "after", sample);
            try run(before, "before", sample);
        }
    }
}
