// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const antfly = @import("antfly_zig");
const G = antfly.graph.GraphIndex;

pub fn run(io: std.Io, out: anytype) !void {
    const a = std.heap.smp_allocator;
    for ([_]usize{ 4096, 16384, 65536 }) |count| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        var g = try G.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .lsm_memory });
        defer g.close();
        const writes = try temp.alloc(antfly.graph.BatchWrite, count);
        for (writes, 0..) |*w, i| w.* = .{ .source = "z", .target = try std.fmt.allocPrint(temp, "target-{d:0>8}", .{i}), .edge_type = "link" };
        try g.batchApply(writes, &.{});
        try g.fenceOwnedRange(a, "m", "");
        for ([_][]const u8{ writes[0].target, writes[count - 1].target }) |key| {
            var times: [5]u64 = undefined;
            for (0..6) |sample| {
                const start = std.Io.Clock.awake.now(io);
                {
                    var scan = g.nativeEdgeScan(key, &.{"link"}, .in);
                    defer scan.deinit(a);
                    if (try scan.nextPage(a, 1, 4096)) |page| {
                        G.freeEdges(a, page);
                        return error.InvalidBenchmarkResult;
                    }
                }
                if (sample > 0) times[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(temp, .{ .mode = "fenced_incoming_prefix", .edges = count, .key = key, .median_ns = times[2], .note = "LSM memory backend; includes snapshot/cursor setup; no disk/network model" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
        var times: [5]u64 = undefined;
        for (0..6) |sample| {
            const start = std.Io.Clock.awake.now(io);
            for (0..10000) |_| {
                const stats = g.operationalStats();
                if (stats.edge_count != count or !stats.counts_pending) return error.InvalidBenchmarkResult;
            }
            if (sample > 0) times[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(temp, .{ .mode = "operational_counts", .edges = count, .iterations = 10000, .median_batch_ns = times[2], .extra_allocation_bytes = 0 }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}
