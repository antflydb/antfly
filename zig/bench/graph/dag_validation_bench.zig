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

//! In-memory validation cost only: no durable authority, network, transaction
//! contention or artifact publication latency is represented by these numbers.
const std = @import("std");
const dag = @import("dag_validation");
const A = std.mem.Allocator;
const Shape = enum { deep, wide, replacement, shared_replacement };
const Clock = struct {
    io: std.Io,
    origin: std.Io.Timestamp,
    fn now(raw: *anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return @intCast(self.origin.durationTo(std.Io.Clock.awake.now(self.io)).toNanoseconds());
    }
};
const Reader = struct {
    shape: Shape,
    names: []const [8]u8,
    pub fn next(self: @This(), _: A, source: []const u8, after: ?[]const u8, _: *dag.Budget) !?dag.Row {
        const id: usize = @intCast(std.mem.readInt(u64, source[0..8], .big));
        const target: usize = switch (self.shape) {
            .deep => if (after == null and id + 1 < self.names.len) id + 1 else return null,
            .wide => blk: {
                if (id != 0) return null;
                const next_id: usize = if (after) |cursor| @intCast(std.mem.readInt(u64, cursor[0..8], .big) + 1) else 1;
                if (next_id >= self.names.len) return null;
                break :blk next_id;
            },
            .shared_replacement => blk: {
                if (after != null) return null;
                const width = (self.names.len - 2) / 2;
                if (id == 0) break :blk self.names.len - 1;
                if (id <= width) break :blk width + 1;
                if (id + 1 < self.names.len) break :blk id + 1;
                return null;
            },
            .replacement => blk: {
                if (after != null) return null;
                if (id == 0) break :blk self.names.len - 1;
                if (id + 1 < self.names.len) break :blk id + 1;
                return null;
            },
        };
        return .{ .key = &self.names[target], .target = &self.names[target] };
    }
};

pub fn main(init: std.process.Init) !void {
    var buffer: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &buffer);
    const a = init.gpa;
    inline for (.{ Shape.deep, .wide, .replacement, .shared_replacement }) |shape| {
        for ([_]usize{ 1_000, 10_000, 100_000 }) |size| {
            const count = if (shape == .shared_replacement) 2 * size + 2 else size;
            const names = try a.alloc([8]u8, count);
            defer a.free(names);
            for (names, 0..) |*name, i| std.mem.writeInt(u64, name, i, .big);
            const reader: Reader = .{ .shape = shape, .names = names };
            const shared_changes = try a.alloc(dag.Change, if (shape == .shared_replacement) size + 1 else 0);
            defer a.free(shared_changes);
            if (shape == .shared_replacement) {
                shared_changes[0] = .{ .source = &names[0], .target = &names[count - 1], .present = false };
                for (1..size + 1) |i| shared_changes[i] = .{ .source = &names[0], .target = &names[i], .present = true };
            }
            var samples: [7]u64 = undefined;
            var stats: dag.Stats = .{};
            for (0..samples.len + 1) |sample| {
                var clock: Clock = .{ .io = init.io, .origin = std.Io.Clock.awake.now(init.io) };
                var budget: dag.Budget = .{
                    .limits = .{ .nodes = count + 1, .edges = 3 * count, .requests = 3 * count, .memory = 128 * 1024 * 1024, .deadline_ns = 10 * std.time.ns_per_s },
                    .clock = .{ .context = &clock, .now = Clock.now },
                };
                var witness: dag.Witness = .{};
                const changes = [_]dag.Change{
                    .{ .source = &names[0], .target = &names[count - 1], .present = false },
                    .{ .source = &names[0], .target = &names[1], .present = true },
                };
                if (shape == .shared_replacement) {
                    try dag.validate(a, reader, shared_changes, &budget, &witness);
                } else if (shape == .replacement) {
                    try dag.validate(a, reader, &changes, &budget, &witness);
                } else try dag.validateExisting(a, reader, &.{&names[0]}, &.{}, &budget, &witness);
                const elapsed = Clock.now(&clock);
                if (sample != 0) samples[sample - 1] = elapsed;
                stats = budget.stats;
                if (stats.nodes != count) return error.InvalidBenchmarkResult;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(a, .{ .shape = @tagName(shape), .size = size, .median_ns = samples[samples.len / 2], .stats = stats, .scope = "validation kernel, in-memory accepted adjacency" }, .{});
            defer a.free(json);
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        }
    }
}
