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
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const inverted = antfly.inverted;

pub fn main() !void {
    const alloc = std.heap.page_allocator;

    const aligned_prefix = try buildSection(alloc, 1024, "prefix", null);
    defer alloc.free(aligned_prefix);
    const unaligned_prefix = try buildSection(alloc, 1000, "prefix", null);
    defer alloc.free(unaligned_prefix);
    const shifted_target = try buildSection(alloc, 256, "target", "shifted");
    defer alloc.free(shifted_target);

    const iterations: usize = 64;
    const aligned = try benchCase(alloc, "aligned", aligned_prefix, shifted_target, iterations);
    const unaligned = try benchCase(alloc, "unaligned", unaligned_prefix, shifted_target, iterations);

    std.debug.print(
        "{{\"iterations\":{d},\"aligned_ns_per_iter\":{d},\"unaligned_ns_per_iter\":{d},\"ratio\":{d}}}\n",
        .{
            iterations,
            aligned.ns_per_iter,
            unaligned.ns_per_iter,
            if (aligned.ns_per_iter == 0) @as(u64, 0) else @as(u64, @intFromFloat(@as(f64, @floatFromInt(unaligned.ns_per_iter)) / @as(f64, @floatFromInt(aligned.ns_per_iter)) * 1000.0)),
        },
    );
}

const BenchResult = struct {
    ns_per_iter: u64,
};

fn monotonicNs() u64 {
    return platform_time.monotonicNs();
}

fn benchCase(
    alloc: std.mem.Allocator,
    label: []const u8,
    prefix: []const u8,
    target: []const u8,
    iterations: usize,
) !BenchResult {
    const warmup = try inverted.mergeInvertedSections(alloc, &.{ prefix, target }, .{});
    defer alloc.free(warmup);
    try verifyMerged(label, alloc, warmup);

    const start_ns = monotonicNs();
    for (0..iterations) |_| {
        const merged = try inverted.mergeInvertedSections(alloc, &.{ prefix, target }, .{});
        alloc.free(merged);
    }
    const elapsed = monotonicNs() - start_ns;
    return .{ .ns_per_iter = @intCast(elapsed / iterations) };
}

fn verifyMerged(label: []const u8, alloc: std.mem.Allocator, merged: []const u8) !void {
    var reader = try inverted.InvertedIndexReader.init(alloc, merged);
    const shifted = reader.lookup("shifted") orelse return error.InvalidData;
    if (shifted.docFreq() != 256) {
        std.debug.print("verification failed for {s}\n", .{label});
        return error.InvalidData;
    }
}

fn buildSection(
    alloc: std.mem.Allocator,
    doc_count: usize,
    base_term: []const u8,
    extra_term: ?[]const u8,
) ![]u8 {
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    for (0..doc_count) |doc_id| {
        if (extra_term) |term| {
            try builder.addDocument(@intCast(doc_id), &.{
                .{ .term = base_term, .freq = 1, .norm = 8 },
                .{ .term = term, .freq = 2 + @as(u32, @intCast(doc_id % 3)), .norm = 12 },
            });
        } else {
            try builder.addDocument(@intCast(doc_id), &.{
                .{ .term = base_term, .freq = 1, .norm = 6 },
            });
        }
    }

    return builder.build();
}
