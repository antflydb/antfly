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
const arithmetic = @import("arithmetic.zig");

pub const native_port_available = true;

pub const Node = struct {
    value: u32 = 0,
    encode_state: u32 = 0,
    decode_state: u32 = 0,
};

pub const LeafPath = struct {
    indices: [32]usize = undefined,
    len: u8 = 0,
};

pub const TagTree = struct {
    allocator: std.mem.Allocator,
    width: usize,
    height: usize,
    nodes: []Node,
    level_offsets: [32]usize = undefined,
    level_widths: [32]usize = undefined,
    level_heights: [32]usize = undefined,
    level_count: u8 = 0,

    pub fn init(allocator: std.mem.Allocator, width: usize, height: usize) !TagTree {
        if (width == 0 or height == 0) return error.InvalidTagTreeShape;

        var total_nodes: usize = 0;
        var level_widths: [32]usize = undefined;
        var level_heights: [32]usize = undefined;
        var level_offsets: [32]usize = undefined;
        var level_count: u8 = 0;
        var w = width;
        var h = height;
        while (true) {
            if (level_count >= 32) return error.TagTreeTooDeep;
            level_offsets[level_count] = total_nodes;
            level_widths[level_count] = w;
            level_heights[level_count] = h;
            total_nodes += w * h;
            level_count += 1;
            if (w == 1 and h == 1) break;
            w = (w + 1) / 2;
            h = (h + 1) / 2;
        }

        const nodes = try allocator.alloc(Node, total_nodes);
        @memset(nodes, .{});
        return .{
            .allocator = allocator,
            .width = width,
            .height = height,
            .nodes = nodes,
            .level_offsets = level_offsets,
            .level_widths = level_widths,
            .level_heights = level_heights,
            .level_count = level_count,
        };
    }

    pub fn deinit(self: *TagTree) void {
        self.allocator.free(self.nodes);
        self.* = undefined;
    }

    pub fn clear(self: *TagTree) void {
        @memset(self.nodes, .{});
    }

    pub fn setAllValues(self: *TagTree, value: u32) void {
        for (self.nodes) |*node| node.value = value;
    }

    pub fn resetEncodeState(self: *TagTree) void {
        for (self.nodes) |*node| node.encode_state = 0;
    }

    pub fn resetDecodeState(self: *TagTree) void {
        for (self.nodes) |*node| node.decode_state = 0;
    }

    pub fn copyFrom(self: *TagTree, other: *const TagTree) !void {
        if (self.width != other.width or
            self.height != other.height or
            self.level_count != other.level_count or
            self.nodes.len != other.nodes.len)
        {
            return error.InvalidTagTreeShape;
        }
        @memcpy(self.nodes, other.nodes);
        self.level_offsets = other.level_offsets;
        self.level_widths = other.level_widths;
        self.level_heights = other.level_heights;
    }

    pub fn nodeIndex(self: *const TagTree, level: u8, x: usize, y: usize) usize {
        return self.level_offsets[level] + y * self.level_widths[level] + x;
    }

    pub fn leafIndex(self: *const TagTree, x: usize, y: usize) usize {
        return self.nodeIndex(0, x, y);
    }

    pub fn setLeafValue(self: *TagTree, x: usize, y: usize, value: u32) !void {
        if (x >= self.width or y >= self.height) return error.TagTreeLeafOutOfBounds;
        self.nodes[self.leafIndex(x, y)].value = value;
        self.propagateMinima();
    }

    pub fn leafValue(self: *const TagTree, x: usize, y: usize) !u32 {
        if (x >= self.width or y >= self.height) return error.TagTreeLeafOutOfBounds;
        return self.nodes[self.leafIndex(x, y)].value;
    }

    pub fn rootValue(self: *const TagTree) u32 {
        return self.nodes[self.nodeIndex(self.level_count - 1, 0, 0)].value;
    }

    pub fn pathToRoot(self: *const TagTree, x: usize, y: usize) !LeafPath {
        if (x >= self.width or y >= self.height) return error.TagTreeLeafOutOfBounds;
        var path: LeafPath = .{};
        var px = x;
        var py = y;
        var level: u8 = 0;
        while (true) {
            path.indices[path.len] = self.nodeIndex(level, px, py);
            path.len += 1;
            if (level + 1 >= self.level_count) break;
            px /= 2;
            py /= 2;
            level += 1;
        }
        return path;
    }

    pub fn recordDecodedThreshold(self: *TagTree, x: usize, y: usize, threshold: u32) !void {
        const path = try self.pathToRoot(x, y);
        var i: usize = path.len;
        while (i > 0) {
            i -= 1;
            const index = path.indices[i];
            if (self.nodes[index].decode_state < threshold) self.nodes[index].decode_state = threshold;
        }
    }

    pub fn recordEncodedThreshold(self: *TagTree, x: usize, y: usize, threshold: u32) !void {
        const path = try self.pathToRoot(x, y);
        var i: usize = path.len;
        while (i > 0) {
            i -= 1;
            const index = path.indices[i];
            if (self.nodes[index].encode_state < threshold) self.nodes[index].encode_state = threshold;
        }
    }

    pub fn decodeBelowThreshold(self: *TagTree, reader: anytype, x: usize, y: usize, threshold: u32) !bool {
        const path = try self.pathToRoot(x, y);
        var low: u32 = 0;
        var i: usize = path.len;
        while (i > 0) {
            i -= 1;
            const index = path.indices[i];
            var state = self.nodes[index].decode_state;
            if (state < low) state = low;
            if (self.nodes[index].value != std.math.maxInt(u32) and state > self.nodes[index].value) {
                state = self.nodes[index].value;
            }
            if (state > self.nodes[index].decode_state) self.nodes[index].decode_state = state;
            if (self.nodes[index].value != std.math.maxInt(u32)) {
                low = self.nodes[index].value;
                continue;
            }
            while (state < threshold) {
                const bit = try reader.readBit();
                if (bit == 1) {
                    self.nodes[index].value = state;
                    break;
                }
                state += 1;
            }
            self.nodes[index].decode_state = state;
            low = if (self.nodes[index].value != std.math.maxInt(u32)) self.nodes[index].value else state;
        }
        return self.nodes[path.indices[0]].value < threshold;
    }

    pub fn decodeValue(self: *TagTree, reader: anytype, x: usize, y: usize, max_threshold: u32) !u32 {
        const path = try self.pathToRoot(x, y);
        var low: u32 = 0;
        var i: usize = path.len;
        while (i > 0) {
            i -= 1;
            const index = path.indices[i];
            var state = self.nodes[index].decode_state;
            if (state < low) state = low;
            if (self.nodes[index].value != std.math.maxInt(u32) and state > self.nodes[index].value) {
                state = self.nodes[index].value;
            }
            if (state > self.nodes[index].decode_state) self.nodes[index].decode_state = state;
            if (self.nodes[index].value != std.math.maxInt(u32)) {
                low = self.nodes[index].value;
                continue;
            }
            while (state < max_threshold) {
                const bit = try reader.readBit();
                if (bit == 1) {
                    self.nodes[index].value = state;
                    break;
                }
                state += 1;
                self.nodes[index].decode_state = state;
            }
            if (self.nodes[index].value == std.math.maxInt(u32)) return error.TagTreeValueOutOfRange;
            low = self.nodes[index].value;
        }
        return self.nodes[path.indices[0]].value;
    }

    fn propagateMinima(self: *TagTree) void {
        var level: u8 = 1;
        while (level < self.level_count) : (level += 1) {
            const width = self.level_widths[level];
            const height = self.level_heights[level];
            var y: usize = 0;
            while (y < height) : (y += 1) {
                var x: usize = 0;
                while (x < width) : (x += 1) {
                    const child_x = x * 2;
                    const child_y = y * 2;
                    var min_value = self.nodes[self.nodeIndex(level - 1, child_x, child_y)].value;
                    var yy = child_y;
                    while (yy < @min(child_y + 2, self.level_heights[level - 1])) : (yy += 1) {
                        var xx = child_x;
                        while (xx < @min(child_x + 2, self.level_widths[level - 1])) : (xx += 1) {
                            min_value = @min(min_value, self.nodes[self.nodeIndex(level - 1, xx, yy)].value);
                        }
                    }
                    self.nodes[self.nodeIndex(level, x, y)].value = min_value;
                }
            }
        }
    }
};

test "tag tree propagates minima to the root" {
    var tree = try TagTree.init(std.testing.allocator, 2, 2);
    defer tree.deinit();
    tree.clear();

    try tree.setLeafValue(0, 0, 5);
    try tree.setLeafValue(1, 0, 3);
    try tree.setLeafValue(0, 1, 9);
    try tree.setLeafValue(1, 1, 7);

    try std.testing.expectEqual(@as(u32, 3), tree.rootValue());
}

test "decodeBelowThreshold propagates decoded parent value not advanced state" {
    var tree = try TagTree.init(std.testing.allocator, 2, 1);
    defer tree.deinit();
    tree.setAllValues(std.math.maxInt(u32));

    const root = tree.nodeIndex(tree.level_count - 1, 0, 0);
    tree.nodes[root].value = 0;
    tree.nodes[root].decode_state = 3;

    var reader = arithmetic.PacketHeaderBitReader.init(&.{0x80});
    const value = try tree.decodeValue(&reader, 1, 0, 8);

    try std.testing.expectEqual(@as(u32, 0), value);
    try std.testing.expectEqual(@as(usize, 1), reader.consumedBytes());
}

test "tag tree path reaches the root" {
    var tree = try TagTree.init(std.testing.allocator, 3, 2);
    defer tree.deinit();

    const path = try tree.pathToRoot(2, 1);
    try std.testing.expect(path.len >= 2);
    try std.testing.expectEqual(tree.nodeIndex(tree.level_count - 1, 0, 0), path.indices[path.len - 1]);
}

test "tag tree decodes a one-by-one value across thresholds" {
    var tree = try TagTree.init(std.testing.allocator, 1, 1);
    defer tree.deinit();
    tree.clear();
    tree.setAllValues(std.math.maxInt(u32));

    var reader = arithmetic.BitReader.init(&.{0b01000000});
    try std.testing.expect(!(try tree.decodeBelowThreshold(&reader, 0, 0, 1)));
    try std.testing.expect(try tree.decodeBelowThreshold(&reader, 0, 0, 2));
    try std.testing.expectEqual(@as(u32, 1), try tree.leafValue(0, 0));
}

test "tag tree threshold state updates entire leaf path" {
    var tree = try TagTree.init(std.testing.allocator, 2, 1);
    defer tree.deinit();

    try tree.recordDecodedThreshold(1, 0, 4);
    const path = try tree.pathToRoot(1, 0);
    for (path.indices[0..path.len]) |index| {
        try std.testing.expectEqual(@as(u32, 4), tree.nodes[index].decode_state);
    }
}
