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

//! Exact borrowed-input containment without scanning every field per value.
const std = @import("std");
const builtin = @import("builtin");

pub const Range = struct {
    start: usize,
    end: usize,

    pub fn from(bytes: []const u8) Range {
        const start = @intFromPtr(bytes.ptr);
        return .{ .start = start, .end = start +| bytes.len };
    }
};

pub const Index = struct {
    ranges: []Range,
    owned: bool,
    probes: if (builtin.is_test) usize else void = if (builtin.is_test) 0 else {},

    /// The caller's small buffer remains borrowed until deinit. Larger input
    /// sets use the caller's scratch allocator and release it after accounting.
    pub fn init(a: std.mem.Allocator, small: []Range, count: usize) !Index {
        return .{
            .ranges = if (count <= small.len) small[0..count] else try a.alloc(Range, count),
            .owned = count > small.len,
        };
    }

    pub fn deinit(self: *Index, a: std.mem.Allocator) void {
        if (self.owned) a.free(self.ranges);
        self.* = undefined;
    }

    /// Prefix maxima preserve containment in an original range. In particular,
    /// adjacent ranges do not incorrectly authorize a slice spanning both.
    pub fn finish(self: *Index) void {
        std.mem.sortUnstable(Range, self.ranges, {}, struct {
            fn lessThan(_: void, lhs: Range, rhs: Range) bool {
                return lhs.start < rhs.start;
            }
        }.lessThan);
        var end: usize = 0;
        for (self.ranges) |*range| {
            end = @max(end, range.end);
            range.end = end;
        }
    }

    pub fn contains(self: *Index, bytes: []const u8) bool {
        if (bytes.len == 0) return true;
        const start = @intFromPtr(bytes.ptr);
        var low: usize = 0;
        var high = self.ranges.len;
        while (low < high) {
            if (comptime builtin.is_test) self.probes += 1;
            const mid = low + (high - low) / 2;
            if (self.ranges[mid].start <= start) low = mid + 1 else high = mid;
        }
        return low != 0 and self.ranges[low - 1].end >= start and
            bytes.len <= self.ranges[low - 1].end - start;
    }
};

test "input alias ranges preserve nested, duplicate and adjacent containment" {
    const a = std.testing.allocator;
    var bytes: [64]u8 = undefined;
    var small: [5]Range = undefined;
    var index = try Index.init(a, &small, small.len);
    defer index.deinit(a);
    index.ranges[0] = Range.from(bytes[24..32]);
    index.ranges[1] = Range.from(bytes[8..24]);
    index.ranges[2] = Range.from(bytes[12..16]);
    index.ranges[3] = Range.from(bytes[8..24]);
    index.ranges[4] = Range.from(bytes[40..48]);
    index.finish();
    try std.testing.expect(index.contains(bytes[18..24]));
    try std.testing.expect(index.contains(bytes[12..24]));
    try std.testing.expect(index.contains(bytes[24..32]));
    try std.testing.expect(index.contains(bytes[0..0]));
    try std.testing.expect(!index.contains(bytes[20..28]));
    try std.testing.expect(!index.contains(bytes[32..40]));
    try std.testing.expect(!index.contains(bytes[0..1]));
}

test "input alias lookup work is logarithmic for wide documents and allocation failure releases ownership" {
    const Run = struct {
        fn run(a: std.mem.Allocator) !void {
            const count = 4096;
            var bytes: [count * 8]u8 = undefined;
            var small: [16]Range = undefined;
            var index = try Index.init(a, &small, count);
            defer index.deinit(a);
            for (index.ranges, 0..) |*range, i| range.* = Range.from(bytes[i * 8 ..][0..4]);
            index.finish();
            for (0..count) |i| {
                try std.testing.expect(index.contains(bytes[i * 8 ..][0..4]));
                try std.testing.expect(!index.contains(bytes[i * 8 + 4 ..][0..4]));
            }
            try std.testing.expect(index.probes <= 2 * count * 13);
        }
    };
    try Run.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}
