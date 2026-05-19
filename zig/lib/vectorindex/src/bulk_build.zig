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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub const BulkBuildAlgo = types.BulkBuildAlgo;
pub const NodeSplitRange = types.NodeSplitRange;

pub const BulkBuildOptions = struct {
    algo: ?BulkBuildAlgo = null,
    skip_vector_store: bool = false,
};

pub const PreparedBulkBuildInput = struct {
    vector_id: u64,
    vector: []const f32,
    transformed: []const f32,
    metadata: []const u8,
};

pub fn initNodeSplitRangeFromInput(alloc: Allocator, input: PreparedBulkBuildInput) !NodeSplitRange {
    return .{
        .min_key = try alloc.dupe(u8, input.metadata),
        .max_key = try alloc.dupe(u8, input.metadata),
    };
}

pub fn extendNodeSplitRangeFromInput(alloc: Allocator, range: *NodeSplitRange, input: PreparedBulkBuildInput) !void {
    if (std.mem.order(u8, input.metadata, range.min_key) == .lt) {
        alloc.free(range.min_key);
        range.min_key = try alloc.dupe(u8, input.metadata);
    }
    if (std.mem.order(u8, input.metadata, range.max_key) == .gt) {
        alloc.free(range.max_key);
        range.max_key = try alloc.dupe(u8, input.metadata);
    }
}

pub fn mergeNodeSplitRanges(alloc: Allocator, left: ?NodeSplitRange, right: ?NodeSplitRange) !?NodeSplitRange {
    if (left == null and right == null) return null;
    if (left == null) return try right.?.clone(alloc);
    if (right == null) return try left.?.clone(alloc);

    return .{
        .min_key = if (std.mem.order(u8, left.?.min_key, right.?.min_key) == .lt)
            try alloc.dupe(u8, left.?.min_key)
        else
            try alloc.dupe(u8, right.?.min_key),
        .max_key = if (std.mem.order(u8, left.?.max_key, right.?.max_key) == .gt)
            try alloc.dupe(u8, left.?.max_key)
        else
            try alloc.dupe(u8, right.?.max_key),
    };
}

pub fn planBalancedGroupSizes(alloc: Allocator, total: usize, max_group_size: usize) ![]usize {
    if (total == 0) return try alloc.alloc(usize, 0);

    const effective_max = @max(@as(usize, 1), max_group_size);
    const group_count = std.math.divCeil(usize, total, effective_max) catch unreachable;
    const base = total / group_count;
    const remainder = total % group_count;

    const groups = try alloc.alloc(usize, group_count);
    for (groups, 0..) |*group, i| {
        group.* = base + @intFromBool(i < remainder);
    }
    return groups;
}

pub fn encodeNodeRange(alloc: Allocator, range: *const NodeSplitRange) ![]u8 {
    const total = 4 + range.min_key.len + 4 + range.max_key.len;
    var out = try alloc.alloc(u8, total);
    var pos: usize = 0;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(range.min_key.len), .little);
    pos += 4;
    @memcpy(out[pos .. pos + range.min_key.len], range.min_key);
    pos += range.min_key.len;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(range.max_key.len), .little);
    pos += 4;
    @memcpy(out[pos .. pos + range.max_key.len], range.max_key);
    return out;
}

pub fn decodeNodeRange(alloc: Allocator, data: []const u8) !NodeSplitRange {
    if (data.len < 8) return error.Corrupted;
    var pos: usize = 0;
    const min_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + min_len > data.len) return error.Corrupted;
    const min_key = try alloc.dupe(u8, data[pos .. pos + min_len]);
    errdefer alloc.free(min_key);
    pos += min_len;
    if (pos + 4 > data.len) return error.Corrupted;
    const max_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + max_len > data.len) return error.Corrupted;
    const max_key = try alloc.dupe(u8, data[pos .. pos + max_len]);
    return .{
        .min_key = min_key,
        .max_key = max_key,
    };
}
