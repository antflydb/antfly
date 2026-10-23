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

//! Global physical ownership follows backing allocations, including temporary
//! resize overlap. Retirement happens after the backing operation completes.
const std = @import("std");
const A = std.mem.Allocator;
pub const Allocator = struct {
    backing: A,
    physical: *std.atomic.Value(usize),
    limit: usize,
    pub fn allocator(self: *Allocator) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn reserve(self: *Allocator, size: usize) bool {
        return @import("budget.zig").Limits.reserve(self.physical, size, self.limit);
    }
    fn alloc(context: *anyopaque, size: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Allocator = @ptrCast(@alignCast(context));
        if (!self.reserve(size)) return null;
        return self.backing.rawAlloc(size, alignment, ra) orelse {
            _ = self.physical.fetchSub(size, .monotonic);
            return null;
        };
    }
    fn resize(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, size: usize, ra: usize) bool {
        const self: *Allocator = @ptrCast(@alignCast(context));
        const growth = size -| bytes.len;
        if (!self.reserve(growth)) return false;
        if (!self.backing.rawResize(bytes, alignment, size, ra)) {
            _ = self.physical.fetchSub(growth, .monotonic);
            return false;
        }
        _ = self.physical.fetchSub(bytes.len -| size, .monotonic);
        return true;
    }
    fn remap(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, size: usize, ra: usize) ?[*]u8 {
        const self: *Allocator = @ptrCast(@alignCast(context));
        // A relocating remap may hold old and new allocations simultaneously.
        // Reserve the entire destination until the backing remap retires old.
        if (!self.reserve(size)) return null;
        const result = self.backing.rawRemap(bytes, alignment, size, ra) orelse {
            _ = self.physical.fetchSub(size, .monotonic);
            return null;
        };
        _ = self.physical.fetchSub(bytes.len, .monotonic);
        return result;
    }
    fn free(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Allocator = @ptrCast(@alignCast(context));
        self.backing.rawFree(bytes, alignment, ra);
        _ = self.physical.fetchSub(bytes.len, .monotonic);
    }
};
