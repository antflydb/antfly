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

//! Early Client allocations share the storage parent's actual process ceiling.
//! The borrowed parent must outlive every allocation, including retained results.
const std = @import("std");
const Parent = @import("antfly_inference_bridge").MemoryParent;

pub const Owner = struct {
    backing: std.mem.Allocator,
    parent: Parent,
    references: std.atomic.Value(usize) = .init(1),

    pub fn create(backing: std.mem.Allocator, parent: *const Parent) !*Owner {
        if (parent.version != Parent.abi_version or parent.struct_size != @sizeOf(Parent)) return error.UnsupportedVersion;
        const attempt = parent.try_reserve_fn orelse return error.ResourceOwnerNotConfigured;
        if (attempt(parent.ptr, @sizeOf(Owner)) != .reserved) return error.ResourceTemporarilyUnavailable;
        errdefer parent.release_fn(parent.ptr, @sizeOf(Owner));
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .parent = parent.* };
        return self;
    }

    pub fn release(self: *Owner) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        const backing = self.backing;
        const parent = self.parent;
        backing.destroy(self);
        parent.release_fn(parent.ptr, @sizeOf(Owner));
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }

    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var count = self.references.load(.acquire);
        while (true) {
            if (count == 0 or count == std.math.maxInt(usize)) return null;
            count = self.references.cmpxchgWeak(count, count + 1, .acq_rel, .acquire) orelse break;
        }
        var retained = false;
        defer if (!retained) self.release();
        if (self.parent.try_reserve_fn.?(self.parent.ptr, len) != .reserved) return null;
        const memory = self.backing.rawAlloc(len, alignment, ra) orelse {
            self.parent.release_fn(self.parent.ptr, len);
            return null;
        };
        retained = true;
        return memory;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ra);
        self.parent.release_fn(self.parent.ptr, memory.len);
        self.release();
    }
};
