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

pub const ComponentRect = struct {
    min_x: usize,
    min_y: usize,
    max_x: usize,
    max_y: usize,
    area: usize,
};

pub fn findConnectedComponents(
    allocator: std.mem.Allocator,
    mask: []const bool,
    width: usize,
    height: usize,
    min_area: usize,
) ![]ComponentRect {
    if (mask.len != width * height) return error.InvalidMaskShape;

    const parent = try allocator.alloc(usize, mask.len);
    defer allocator.free(parent);
    const rank = try allocator.alloc(u8, mask.len);
    defer allocator.free(rank);

    for (0..mask.len) |i| {
        parent[i] = i;
        rank[i] = 0;
    }

    for (0..height) |y| {
        for (0..width) |x| {
            const idx = y * width + x;
            if (!mask[idx]) continue;
            if (x > 0 and mask[idx - 1]) unionSets(parent, rank, idx, idx - 1);
            if (y > 0 and mask[idx - width]) unionSets(parent, rank, idx, idx - width);
        }
    }

    var components = std.AutoHashMapUnmanaged(usize, ComponentRect).empty;
    defer components.deinit(allocator);

    for (0..height) |y| {
        for (0..width) |x| {
            const idx = y * width + x;
            if (!mask[idx]) continue;

            const root = find(parent, idx);
            const entry = try components.getOrPut(allocator, root);
            if (!entry.found_existing) {
                entry.value_ptr.* = .{
                    .min_x = x,
                    .min_y = y,
                    .max_x = x,
                    .max_y = y,
                    .area = 0,
                };
            }

            entry.value_ptr.min_x = @min(entry.value_ptr.min_x, x);
            entry.value_ptr.min_y = @min(entry.value_ptr.min_y, y);
            entry.value_ptr.max_x = @max(entry.value_ptr.max_x, x);
            entry.value_ptr.max_y = @max(entry.value_ptr.max_y, y);
            entry.value_ptr.area += 1;
        }
    }

    var result = std.ArrayListUnmanaged(ComponentRect).empty;
    errdefer result.deinit(allocator);

    var it = components.valueIterator();
    while (it.next()) |comp| {
        if (comp.area >= min_area) try result.append(allocator, comp.*);
    }

    return try result.toOwnedSlice(allocator);
}

fn find(parent: []usize, x: usize) usize {
    var cur = x;
    while (parent[cur] != cur) {
        parent[cur] = parent[parent[cur]];
        cur = parent[cur];
    }
    return cur;
}

fn unionSets(parent: []usize, rank: []u8, a: usize, b: usize) void {
    var root_a = find(parent, a);
    var root_b = find(parent, b);
    if (root_a == root_b) return;

    if (rank[root_a] < rank[root_b]) std.mem.swap(usize, &root_a, &root_b);
    parent[root_b] = root_a;
    if (rank[root_a] == rank[root_b]) rank[root_a] += 1;
}

test "findConnectedComponents returns bounding boxes for foreground regions" {
    const allocator = std.testing.allocator;
    const mask = [_]bool{
        true,  true,  false, false,
        false, true,  false, true,
        false, false, false, true,
    };

    const comps = try findConnectedComponents(allocator, &mask, 4, 3, 1);
    defer allocator.free(comps);

    try std.testing.expectEqual(@as(usize, 2), comps.len);
    try std.testing.expect(comps[0].area > 0);
}

test "findConnectedComponents filters by minimum area" {
    const allocator = std.testing.allocator;
    const mask = [_]bool{
        true,  false, false,
        false, false, false,
        false, false, true,
    };

    const comps = try findConnectedComponents(allocator, &mask, 3, 3, 2);
    defer allocator.free(comps);

    try std.testing.expectEqual(@as(usize, 0), comps.len);
}
