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

//! Exact caller-owned pthread mapping. Join the thread before releasing it.
const std = @import("std");
pub const Stack = struct {
    allocator: std.mem.Allocator,
    mapping: []u8,

    pub fn create(allocator: std.mem.Allocator, stack_size: usize) !Stack {
        if (comptime !std.Thread.use_pthreads) return error.UnsupportedPlatform;
        const page = std.heap.pageSize();
        const payload = (std.math.add(usize, @max(stack_size, 16 * 1024), page - 1) catch return error.OutOfMemory) & ~(page - 1);
        const length = std.math.add(usize, payload, std.math.mul(usize, page, 2) catch return error.OutOfMemory) catch return error.OutOfMemory;
        const bytes = allocator.rawAlloc(length, .fromByteUnits(page), @returnAddress()) orelse return error.OutOfMemory;
        const mapping = bytes[0..length];
        errdefer allocator.rawFree(mapping, .fromByteUnits(page), @returnAddress());
        if (std.c.mprotect(@ptrCast(@alignCast(bytes)), page, .{}) != 0 or
            std.c.mprotect(@ptrCast(@alignCast(bytes + length - page)), page, .{}) != 0) return error.SystemResources;
        return .{ .allocator = allocator, .mapping = mapping };
    }
    pub fn spawn(self: Stack, metadata: std.mem.Allocator, comptime function: anytype, args: anytype) !std.Thread {
        if (comptime std.Thread.use_pthreads and @hasField(std.Thread.SpawnConfig, "caller_stack")) {
            const page = std.heap.pageSize();
            const payload = self.mapping[page .. self.mapping.len - page];
            return std.Thread.spawn(.{ .stack_size = payload.len, .caller_stack = payload, .allocator = metadata }, function, args);
        } else return error.UnsupportedPlatform;
    }
    pub fn destroy(self: Stack) void {
        self.allocator.rawFree(self.mapping, .fromByteUnits(std.heap.pageSize()), @returnAddress());
    }
};
