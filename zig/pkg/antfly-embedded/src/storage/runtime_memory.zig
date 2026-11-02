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

//! Stable allocation authority for runtime metadata and exact owned stack maps.
//! The borrowed parent outlives physical executor joins and this owner.
const std = @import("std");
const Parent = @import("antfly_runtime_abi").memory_abi.Parent;

pub const Owner = struct {
    backing: std.mem.Allocator,
    parent: Parent,
    heap: Segment,
    stacks: Segment,
    pages: Segment,
    control_heap: Segment,
    control_stacks: Segment,
    raft_inbound_heap: Segment,
    raft_inbound_stacks: Segment,
    raft_outbound_heap: Segment,
    raft_outbound_stacks: Segment,
    charged: usize,
    heap_totals: Counter = .{},
    stack_totals: Counter = .{},

    pub const ControlLimits = struct { heap: usize, stacks: usize };
    pub const RaftDirection = enum { inbound, outbound };
    pub const RaftLimits = struct { inbound: ControlLimits, outbound: ControlLimits };
    pub const ExecutorStats = struct { heap: Stats, stacks: Stats, reserved_bytes: usize };

    pub const Stats = struct { used: usize, peak: usize, rejected: usize };
    const Counter = struct {
        used: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),
        fn add(self: *Counter, bytes: usize) void {
            const used = self.used.fetchAdd(bytes, .acq_rel) + bytes;
            var peak = self.peak.load(.acquire);
            while (peak < used) peak = self.peak.cmpxchgWeak(peak, used, .acq_rel, .acquire) orelse break;
        }
        fn stats(self: *const Counter) Stats {
            return .{ .used = self.used.load(.acquire), .peak = self.peak.load(.acquire), .rejected = self.rejected.load(.monotonic) };
        }
    };
    const Segment = struct {
        backing: std.mem.Allocator,
        parent: Parent,
        pages_only: bool = false,
        round_pages: bool = false,
        prepaid_limit: ?usize = null,
        totals: ?*Counter = null,
        used: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),

        fn allocator(self: *Segment) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *Segment = @ptrCast(@alignCast(raw));
            if (((self.pages_only or self.round_pages) and alignment.toByteUnits() > std.heap.pageSize()) or (self.pages_only and len % std.heap.pageSize() != 0)) {
                self.reject();
                return null;
            }
            const charged = self.chargeSize(len) orelse {
                self.reject();
                return null;
            };
            const used = if (self.prepaid_limit) |limit| blk: {
                var current = self.used.load(.acquire);
                while (current <= limit and charged <= limit - current) {
                    current = self.used.cmpxchgWeak(current, current + charged, .acq_rel, .acquire) orelse break :blk current + charged;
                }
                self.reject();
                return null;
            } else blk: {
                if (!self.parent.reserve_fn(self.parent.ptr, charged)) {
                    self.reject();
                    return null;
                }
                break :blk self.used.fetchAdd(charged, .acq_rel) + charged;
            };
            const bytes = self.backing.rawAlloc(charged, alignment, ret) orelse {
                _ = self.used.fetchSub(charged, .acq_rel);
                if (self.prepaid_limit == null) self.parent.release_fn(self.parent.ptr, charged);
                self.reject();
                return null;
            };
            var peak = self.peak.load(.acquire);
            while (peak < used) peak = self.peak.cmpxchgWeak(peak, used, .acq_rel, .acquire) orelse break;
            if (self.totals) |totals| totals.add(charged);
            return bytes;
        }
        // Reallocation must admit the physical old+new peak, including when the
        // backing allocator could grow in place or retain freed size classes.
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *Segment = @ptrCast(@alignCast(raw));
            const charged = self.chargeSize(bytes.len).?;
            self.backing.rawFree(bytes.ptr[0..charged], alignment, ret);
            std.debug.assert(self.used.fetchSub(charged, .acq_rel) >= charged);
            if (self.totals) |totals| std.debug.assert(totals.used.fetchSub(charged, .acq_rel) >= charged);
            if (self.prepaid_limit == null) self.parent.release_fn(self.parent.ptr, charged);
        }
        fn reject(self: *Segment) void {
            _ = self.rejected.fetchAdd(1, .monotonic);
            if (self.totals) |totals| _ = totals.rejected.fetchAdd(1, .monotonic);
        }
        fn chargeSize(self: *const Segment, len: usize) ?usize {
            if (!self.round_pages) return len;
            const page = std.heap.pageSize();
            return (std.math.add(usize, @max(len, 1), page - 1) catch return null) & ~(page - 1);
        }
        fn stats(self: *const Segment) Stats {
            return .{ .used = self.used.load(.acquire), .peak = self.peak.load(.acquire), .rejected = self.rejected.load(.monotonic) };
        }
    };

    pub fn create(backing: std.mem.Allocator, parent: Parent) !*Owner {
        return createWithControl(backing, parent, null);
    }
    /// Credit is reserved before physical owner allocation and held until all
    /// control futures and real worker joins have retired, including idle maps.
    pub fn createWithControl(backing: std.mem.Allocator, parent: Parent, control: ?ControlLimits) !*Owner {
        return createWithProgress(backing, parent, control, null);
    }
    pub fn createWithProgress(backing: std.mem.Allocator, parent: Parent, control: ?ControlLimits, raft: ?RaftLimits) !*Owner {
        if (!parent.valid()) return error.UnsupportedVersion;
        var charged: usize = @sizeOf(Owner);
        if (control) |limits| {
            if (limits.heap == 0) return error.InvalidControlMemoryLimit;
            charged = std.math.add(usize, charged, limits.heap) catch return error.InvalidControlMemoryLimit;
            charged = std.math.add(usize, charged, limits.stacks) catch return error.InvalidControlMemoryLimit;
        }
        if (raft) |limits| {
            inline for (.{ limits.inbound, limits.outbound }) |lane| {
                if (lane.heap == 0) return error.InvalidRaftMemoryLimit;
                charged = std.math.add(usize, charged, lane.heap) catch return error.InvalidRaftMemoryLimit;
                charged = std.math.add(usize, charged, lane.stacks) catch return error.InvalidRaftMemoryLimit;
            }
        }
        if (!parent.reserve_fn(parent.ptr, charged)) return error.ResourceBudgetExceeded;
        errdefer parent.release_fn(parent.ptr, charged);
        const self = try backing.create(Owner);
        self.* = .{
            .backing = backing,
            .parent = parent,
            .charged = charged,
            .heap = .{ .backing = backing, .parent = parent },
            .stacks = .{ .backing = std.heap.page_allocator, .parent = parent, .pages_only = true },
            .pages = .{ .backing = std.heap.page_allocator, .parent = parent, .round_pages = true },
            .control_heap = .{ .backing = backing, .parent = parent, .prepaid_limit = if (control) |limits| limits.heap else null },
            .control_stacks = .{ .backing = std.heap.page_allocator, .parent = parent, .pages_only = true, .prepaid_limit = if (control) |limits| limits.stacks else null },
            .raft_inbound_heap = .{ .backing = backing, .parent = parent, .prepaid_limit = if (raft) |limits| limits.inbound.heap else null },
            .raft_inbound_stacks = .{ .backing = std.heap.page_allocator, .parent = parent, .pages_only = true, .prepaid_limit = if (raft) |limits| limits.inbound.stacks else null },
            .raft_outbound_heap = .{ .backing = backing, .parent = parent, .prepaid_limit = if (raft) |limits| limits.outbound.heap else null },
            .raft_outbound_stacks = .{ .backing = std.heap.page_allocator, .parent = parent, .pages_only = true, .prepaid_limit = if (raft) |limits| limits.outbound.stacks else null },
        };
        self.heap.totals = &self.heap_totals;
        self.control_heap.totals = &self.heap_totals;
        self.stacks.totals = &self.stack_totals;
        self.control_stacks.totals = &self.stack_totals;
        self.raft_inbound_heap.totals = &self.heap_totals;
        self.raft_inbound_stacks.totals = &self.stack_totals;
        self.raft_outbound_heap.totals = &self.heap_totals;
        self.raft_outbound_stacks.totals = &self.stack_totals;
        return self;
    }
    pub fn controlAllocator(self: *Owner) std.mem.Allocator {
        return self.control_heap.allocator();
    }
    pub fn controlStackAllocator(self: *Owner) std.mem.Allocator {
        return self.control_stacks.allocator();
    }
    pub fn controlHeapStats(self: *const Owner) Stats {
        return self.control_heap.stats();
    }
    pub fn controlStackStats(self: *const Owner) Stats {
        return self.control_stacks.stats();
    }
    pub fn controlReservedBytes(self: *const Owner) usize {
        return (self.control_heap.prepaid_limit orelse 0) + (self.control_stacks.prepaid_limit orelse 0);
    }
    pub fn raftAllocator(self: *Owner, direction: RaftDirection) std.mem.Allocator {
        return switch (direction) {
            .inbound => self.raft_inbound_heap.allocator(),
            .outbound => self.raft_outbound_heap.allocator(),
        };
    }
    pub fn raftStackAllocator(self: *Owner, direction: RaftDirection) std.mem.Allocator {
        return switch (direction) {
            .inbound => self.raft_inbound_stacks.allocator(),
            .outbound => self.raft_outbound_stacks.allocator(),
        };
    }
    pub fn raftStats(self: *const Owner, direction: RaftDirection) ExecutorStats {
        const heap = switch (direction) {
            .inbound => &self.raft_inbound_heap,
            .outbound => &self.raft_outbound_heap,
        };
        const stacks = switch (direction) {
            .inbound => &self.raft_inbound_stacks,
            .outbound => &self.raft_outbound_stacks,
        };
        return .{ .heap = heap.stats(), .stacks = stacks.stats(), .reserved_bytes = (heap.prepaid_limit orelse 0) + (stacks.prepaid_limit orelse 0) };
    }
    pub fn allocator(self: *Owner) std.mem.Allocator {
        return self.heap.allocator();
    }
    pub fn stackAllocator(self: *Owner) std.mem.Allocator {
        return self.stacks.allocator();
    }
    pub fn pageAllocator(self: *Owner) std.mem.Allocator {
        return self.pages.allocator();
    }
    pub fn pageStats(self: *const Owner) Stats {
        return self.pages.stats();
    }
    pub fn heapStats(self: *const Owner) Stats {
        return self.heap_totals.stats();
    }
    pub fn stackStats(self: *const Owner) Stats {
        return self.stack_totals.stats();
    }
    pub fn destroy(self: *Owner) void {
        std.debug.assert(self.heap.used.load(.acquire) == 0);
        std.debug.assert(self.stacks.used.load(.acquire) == 0);
        std.debug.assert(self.pages.used.load(.acquire) == 0);
        std.debug.assert(self.control_heap.used.load(.acquire) == 0);
        std.debug.assert(self.control_stacks.used.load(.acquire) == 0);
        std.debug.assert(self.raft_inbound_heap.used.load(.acquire) == 0);
        std.debug.assert(self.raft_inbound_stacks.used.load(.acquire) == 0);
        std.debug.assert(self.raft_outbound_heap.used.load(.acquire) == 0);
        std.debug.assert(self.raft_outbound_stacks.used.load(.acquire) == 0);
        std.debug.assert(self.heap_totals.used.load(.acquire) == 0);
        std.debug.assert(self.stack_totals.used.load(.acquire) == 0);
        const parent = self.parent;
        const backing = self.backing;
        const charged = self.charged;
        backing.destroy(self);
        parent.release_fn(parent.ptr, charged);
    }
};
