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

//! Bounded range planning shared by native document readers and pending values.
const std = @import("std");
pub const Range = struct { offset: u64, length: usize };
pub const maximum_ranges = 512;
pub const maximum_span = 256 * 1024;
pub const maximum_gap = 4096;

/// Preserve caller order while sorting, deduplicating and coalescing physical
/// ranges. One legal oversized range is isolated. Returned slices share the
/// caller's read-scope owner; partial results on failure must be discarded.
pub fn read(size: usize, ranges: []const Range, output: [][]const u8, context: anytype) !void {
    if (ranges.len != output.len or ranges.len > maximum_ranges) return error.InvalidBatch;
    var order: [maximum_ranges]usize = undefined;
    for (ranges, order[0..ranges.len], 0..) |range, *index, i| {
        if (range.offset > size or range.length > size - range.offset) return error.EndOfStream;
        index.* = i;
    }
    std.mem.sort(usize, order[0..ranges.len], ranges, struct {
        fn less(source: []const Range, left: usize, right: usize) bool {
            return source[left].offset < source[right].offset;
        }
    }.less);
    var padding: usize = 0;
    var first: usize = 0;
    while (first < ranges.len) {
        const begin: usize = @intCast(ranges[order[first]].offset);
        var end = begin + ranges[order[first]].length;
        var next = first + 1;
        while (next < ranges.len) : (next += 1) {
            const range = ranges[order[next]];
            const stop = @max(end, @as(usize, @intCast(range.offset)) + range.length);
            const gap = @as(usize, @intCast(range.offset)) -| end;
            if (gap > maximum_gap or stop - begin > maximum_span or gap > maximum_span - padding) break;
            padding += gap;
            end = stop;
        }
        const bytes = try context.read(begin, end - begin);
        if (bytes.len != end - begin) return error.EndOfStream;
        for (order[first..next]) |index| output[index] = bytes[@as(usize, @intCast(ranges[index].offset)) - begin ..][0..ranges[index].length];
        first = next;
    }
}
