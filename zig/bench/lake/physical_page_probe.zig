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

//! Single physical page comparison; both variants use the production scanner.
const std = @import("std");
const before = @import("before").page;
const after = @import("after").page;
fn run(comptime P: type, name: []const u8, sample: usize, chunk: []const u8) !void {
    const a = std.heap.smp_allocator;
    var tracked = std.testing.FailingAllocator.init(a, .{});
    const iterations = 1024;
    // Validate the complete vector outside the timed interval.
    const oracle = try P.scanPlainI64ColumnChunkAlloc(a, chunk, .uncompressed);
    defer a.free(oracle);
    for (oracle, 0..) |value, i| try std.testing.expectEqual(-@as(i64, @intCast(i)), value);
    const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..iterations) |i| {
        const values = try P.scanPlainI64ColumnChunkAlloc(tracked.allocator(), chunk, .uncompressed);
        std.mem.doNotOptimizeAway(values.ptr);
        try std.testing.expectEqual(-@as(i64, @intCast(i % values.len)), values[i % values.len]);
        tracked.allocator().free(values);
    }
    const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
    std.debug.print("sample={} variant={s} iterations={} rows={} elapsed_ns={} allocated_bytes={}\n", .{ sample, name, iterations, oracle.len, elapsed, tracked.allocated_bytes });
}
fn fitsTwoVectors(comptime P: type, chunk: []const u8) !bool {
    const a = std.testing.allocator;
    const space = try a.alloc(u8, 2 * 65536);
    defer a.free(space);
    var fixed = std.heap.FixedBufferAllocator.init(space);
    const values = P.scanPlainI64ColumnChunkAlloc(fixed.allocator(), chunk, .uncompressed) catch |err| {
        if (err == error.OutOfMemory) return false;
        return err;
    };
    defer fixed.allocator().free(values);
    try std.testing.expectEqual(@as(usize, 8192), values.len);
    return true;
}
test "physical_page_production_comparison" {
    const a = std.testing.allocator;
    // Compact Thrift DATA_PAGE: 8192 PLAIN i64 values, 65536 payload bytes.
    const header = [_]u8{ 0x15, 0, 0x15, 0x80, 0x80, 8, 0x15, 0x80, 0x80, 8, 0x2c, 0x15, 0x80, 0x80, 1, 0x15, 0, 0x15, 6, 0x15, 6, 0, 0 };
    const chunk = try a.alloc(u8, header.len + 8192 * 8);
    defer a.free(chunk);
    @memcpy(chunk[0..header.len], &header);
    for (0..8192) |i| std.mem.writeInt(i64, chunk[header.len + i * 8 ..][0..8], -@as(i64, @intCast(i)), .little);
    try std.testing.expect(!try fitsTwoVectors(before, chunk));
    try std.testing.expect(try fitsTwoVectors(after, chunk));
    for (0..6) |sample| {
        if (sample % 2 == 0) {
            try run(before, "before", sample, chunk);
            try run(after, "after", sample, chunk);
        } else {
            try run(after, "after", sample, chunk);
            try run(before, "before", sample, chunk);
        }
    }
}
