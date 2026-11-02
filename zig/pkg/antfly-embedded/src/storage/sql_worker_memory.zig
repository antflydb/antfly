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

//! Node-owned, prepaid executor heaps. The borrowed parent must outlive close.
//! Each role has a separate floor; foreground submissions cannot spend cleanup
//! or control memory. Physical allocation failure never publishes new credit.
const std = @import("std");
const Parent = @import("antfly_runtime_abi").memory_abi.Parent;

pub const Role = enum { execution, control, cleanup, bounded_read };
pub const Limits = struct {
    execution: usize = 1 << 20,
    control: usize = 1 << 20,
    cleanup: usize = 1 << 20,
    bounded_read: usize = 1 << 20,

    pub fn get(self: Limits, role: Role) usize {
        return switch (role) {
            inline else => |value| @field(self, @tagName(value)),
        };
    }
};

/// Stack mappings have separate floors: task allocations may not consume them.
pub const StackLimits = struct {
    execution: usize = 0,
    control: usize = 0,
    cleanup: usize = 0,
    bounded_read: usize = 0,
    pub fn get(self: StackLimits, role: Role) usize {
        return switch (role) {
            inline else => |value| @field(self, @tagName(value)),
        };
    }
};

pub const Bank = struct {
    backing: std.mem.Allocator,
    parent: Parent,
    charged: usize,
    segments: [4]Segment,
    stacks: [4]Segment,

    pub const Stats = struct { limit: usize, used: usize, peak: usize, rejected: usize };
    const Segment = struct {
        backing: std.mem.Allocator,
        limit: usize,
        pages_only: bool = false,
        used: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),

        fn allocator(self: *Segment) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *Segment = @ptrCast(@alignCast(raw));
            if (self.pages_only and (len % std.heap.pageSize() != 0 or alignment.toByteUnits() > std.heap.pageSize())) {
                _ = self.rejected.fetchAdd(1, .monotonic);
                return null;
            }
            var used = self.used.load(.acquire);
            while (used <= self.limit and len <= self.limit - used) {
                if (self.used.cmpxchgWeak(used, used + len, .acq_rel, .acquire)) |actual| {
                    used = actual;
                } else {
                    const bytes = self.backing.rawAlloc(len, alignment, ret) orelse {
                        _ = self.used.fetchSub(len, .acq_rel);
                        _ = self.rejected.fetchAdd(1, .monotonic);
                        return null;
                    };
                    var peak = self.peak.load(.acquire);
                    while (peak < used + len) peak = self.peak.cmpxchgWeak(peak, used + len, .acq_rel, .acquire) orelse break;
                    return bytes;
                }
            }
            _ = self.rejected.fetchAdd(1, .monotonic);
            return null;
        }
        // Allocator fallback must admit the physical old+new peak. In-place
        // growth/remap cannot silently bypass this ledger or its backing owner.
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *Segment = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ret);
            std.debug.assert(self.used.fetchSub(bytes.len, .acq_rel) >= bytes.len);
        }
    };

    pub fn create(backing: std.mem.Allocator, parent: Parent, limits: Limits) !*Bank {
        return createWithStacks(backing, parent, limits, .{});
    }
    pub fn createWithStacks(backing: std.mem.Allocator, parent: Parent, limits: Limits, stack_limits: StackLimits) !*Bank {
        if (!parent.valid()) return error.UnsupportedVersion;
        var charged: usize = @sizeOf(Bank);
        inline for (std.meta.tags(Role)) |role| {
            const limit = limits.get(role);
            if (limit == 0) return error.InvalidWorkerMemoryLimit;
            charged = std.math.add(usize, charged, limit) catch return error.InvalidWorkerMemoryLimit;
            charged = std.math.add(usize, charged, stack_limits.get(role)) catch return error.InvalidWorkerMemoryLimit;
        }
        if (!parent.reserve_fn(parent.ptr, charged)) return error.ResourceBudgetExceeded;
        errdefer parent.release_fn(parent.ptr, charged);
        const self = try backing.create(Bank);
        self.* = .{ .backing = backing, .parent = parent, .charged = charged, .segments = undefined, .stacks = undefined };
        inline for (std.meta.tags(Role)) |role| {
            self.segments[@backingInt(role)] = .{ .backing = backing, .limit = limits.get(role) };
            // Exact page mappings, without a libc stack cache. The complete
            // guard+payload mapping is admitted before physical allocation.
            self.stacks[@backingInt(role)] = .{ .backing = std.heap.page_allocator, .limit = stack_limits.get(role), .pages_only = true };
        }
        return self;
    }
    pub fn allocator(self: *Bank, role: Role) std.mem.Allocator {
        return self.segments[@backingInt(role)].allocator();
    }
    pub fn stackAllocator(self: *Bank, role: Role) std.mem.Allocator {
        return self.stacks[@backingInt(role)].allocator();
    }
    pub fn stackStats(self: *const Bank, role: Role) Stats {
        return segmentStats(&self.stacks[@backingInt(role)]);
    }
    pub fn stats(self: *const Bank, role: Role) Stats {
        return segmentStats(&self.segments[@backingInt(role)]);
    }
    fn segmentStats(segment: *const Segment) Stats {
        return .{ .limit = segment.limit, .used = segment.used.load(.acquire), .peak = segment.peak.load(.acquire), .rejected = segment.rejected.load(.monotonic) };
    }
    /// All executor futures and physical worker joins must precede this call.
    pub fn destroy(self: *Bank) void {
        for (&self.segments) |*segment| std.debug.assert(segment.used.load(.acquire) == 0);
        for (&self.stacks) |*segment| std.debug.assert(segment.used.load(.acquire) == 0);
        const parent = self.parent;
        const charged = self.charged;
        const backing = self.backing;
        backing.destroy(self);
        parent.release_fn(parent.ptr, charged);
    }
};
