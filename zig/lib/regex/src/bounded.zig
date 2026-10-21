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

//! Allocator admission based on actual live requested bytes, including growth
//! capacity and temporary old/new buffers. The owner must keep this at a stable
//! address and distinguish quota denial from failure of its backing allocator.
const std = @import("std");
const A = std.mem.Allocator;
pub const Budget = struct {
    backing: A,
    limit: usize,
    live: usize = 0,
    peak: usize = 0,
    denied: bool = false,
    pub fn allocator(self: *Budget) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn reserve(self: *Budget, extra: usize) bool {
        if (extra <= self.limit - self.live) return true;
        self.denied = true;
        return false;
    }
    fn changed(self: *Budget, previous: usize, next: usize) void {
        self.live = self.live - previous + next;
        self.peak = @max(self.peak, self.live);
    }
    fn alloc(context: *anyopaque, size: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(context));
        if (!self.reserve(size)) return null;
        const bytes = self.backing.rawAlloc(size, alignment, ra) orelse return null;
        self.changed(0, size);
        return bytes;
    }
    fn resize(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, size: usize, ra: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(context));
        if (!self.reserve(size -| bytes.len) or !self.backing.rawResize(bytes, alignment, size, ra)) return false;
        self.changed(bytes.len, size);
        return true;
    }
    fn remap(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, size: usize, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(context));
        if (!self.reserve(size -| bytes.len)) return null;
        const result = self.backing.rawRemap(bytes, alignment, size, ra) orelse return null;
        self.changed(bytes.len, size);
        return result;
    }
    fn free(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Budget = @ptrCast(@alignCast(context));
        self.backing.rawFree(bytes, alignment, ra);
        self.changed(bytes.len, 0);
    }
};
