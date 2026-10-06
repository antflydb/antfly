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

//! Contiguous keys and borrowed values for one synchronous document read.
const std = @import("std");

pub const Scratch = struct {
    alloc: std.mem.Allocator,
    memory: []align(alignment) u8,
    keys: [][]const u8,
    values: []?[]const u8,
    const alignment = @max(@alignOf([]const u8), @alignOf(?[]const u8));

    pub fn init(alloc: std.mem.Allocator, count: usize) !Scratch {
        comptime std.debug.assert(@sizeOf([]const u8) % @alignOf(?[]const u8) == 0);
        const key_bytes = try std.math.mul(usize, count, @sizeOf([]const u8));
        const value_bytes = try std.math.mul(usize, count, @sizeOf(?[]const u8));
        const memory = try alloc.alignedAlloc(u8, .fromByteUnits(alignment), try std.math.add(usize, key_bytes, value_bytes));
        const value_memory: []align(alignment) u8 = @alignCast(memory[key_bytes..]);
        const values = std.mem.bytesAsSlice(?[]const u8, value_memory);
        @memset(values, null);
        return .{ .alloc = alloc, .memory = memory, .keys = std.mem.bytesAsSlice([]const u8, memory[0..key_bytes]), .values = values };
    }

    pub fn deinit(self: *Scratch) void {
        self.alloc.free(self.memory);
        self.* = undefined;
    }
};

test "document read scratch keeps keys and nullable values independent in one allocation" {
    for ([_]usize{ 0, 1, 256 }) |count| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
        var scratch = try Scratch.init(failing.allocator(), count);
        defer scratch.deinit();
        for (scratch.keys, scratch.values) |*key, *value| {
            try std.testing.expect(value.* == null);
            key.* = "key";
            value.* = "body";
            try std.testing.expectEqualStrings("key", key.*);
            try std.testing.expectEqualStrings("body", value.*.?);
        }
        try std.testing.expectError(error.Overflow, Scratch.init(failing.allocator(), std.math.maxInt(usize)));
    }
}
