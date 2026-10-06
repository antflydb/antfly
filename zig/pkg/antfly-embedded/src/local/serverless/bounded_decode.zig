// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Shared allocation-amplification guards for durable binary artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Limits = struct {
    max_artifact_bytes: usize = 256 * 1024 * 1024,
    max_allocation_bytes: usize = 256 * 1024 * 1024,
    max_elements: usize = 16 * 1024 * 1024,

    pub fn validate(self: Limits) !void {
        if (self.max_artifact_bytes == 0 or self.max_allocation_bytes == 0 or self.max_elements == 0) {
            return error.InvalidDecodeLimits;
        }
    }
};

pub const Budget = struct {
    remaining_allocation_bytes: usize,
    remaining_elements: usize,

    pub fn init(encoded_len: usize, limits: Limits) !Budget {
        try limits.validate();
        if (encoded_len > limits.max_artifact_bytes) return error.DecodedArtifactTooLarge;
        return .{
            .remaining_allocation_bytes = limits.max_allocation_bytes,
            .remaining_elements = limits.max_elements,
        };
    }

    /// Admit a serialized element count before allocating its destination
    /// slice. `min_encoded_bytes` is the smallest representation of one item,
    /// so forged counts are rejected even when the allocation budget is large.
    pub fn admitCount(
        self: *Budget,
        comptime T: type,
        raw_count: anytype,
        remaining_encoded_bytes: usize,
        min_encoded_bytes: usize,
    ) !usize {
        const count = try self.admitElements(raw_count, remaining_encoded_bytes, min_encoded_bytes);
        try self.admitAllocation(T, count);
        return count;
    }

    pub fn admitElements(
        self: *Budget,
        raw_count: anytype,
        remaining_encoded_bytes: usize,
        min_encoded_bytes: usize,
    ) !usize {
        const count = std.math.cast(usize, raw_count) orelse return error.DecodedArtifactTooLarge;
        if (count > self.remaining_elements) return error.DecodedArtifactTooLarge;
        if (min_encoded_bytes != 0) {
            const minimum_bytes = std.math.mul(usize, count, min_encoded_bytes) catch
                return error.InvalidEncodedCount;
            if (minimum_bytes > remaining_encoded_bytes) return error.InvalidEncodedCount;
        }
        self.remaining_elements -= count;
        return count;
    }

    pub fn admitAllocation(self: *Budget, comptime T: type, count: usize) !void {
        const allocation_bytes = std.math.mul(usize, count, @sizeOf(T)) catch
            return error.DecodedArtifactTooLarge;
        if (allocation_bytes > self.remaining_allocation_bytes) return error.DecodedArtifactTooLarge;
        self.remaining_allocation_bytes -= allocation_bytes;
    }

    pub fn admitBytes(self: *Budget, len: usize) !void {
        try self.admitAllocation(u8, len);
    }
};

/// Hard live-allocation admission for decoders whose wire format may contain
/// nested or variable-sized allocations. A forged count can make an allocator
/// request large, but it cannot reach the backing allocator once this limit is
/// exceeded. `limit_exceeded` lets callers distinguish configured admission
/// from a genuine backing-allocator failure.
pub const AllocationLimiter = struct {
    backing: Allocator,
    live_bytes: usize = 0,
    max_live_bytes: usize,
    limit_exceeded: bool = false,

    pub fn init(backing: Allocator, max_live_bytes: usize) !AllocationLimiter {
        if (max_live_bytes == 0) return error.InvalidDecodeLimits;
        return .{ .backing = backing, .max_live_bytes = max_live_bytes };
    }

    pub fn allocator(self: *AllocationLimiter) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn permitsGrowth(self: *AllocationLimiter, additional_bytes: usize) bool {
        if (additional_bytes <= self.max_live_bytes -| self.live_bytes) return true;
        self.limit_exceeded = true;
        return false;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *AllocationLimiter = @ptrCast(@alignCast(ctx));
        if (!self.permitsGrowth(len)) return null;
        const memory = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live_bytes += len;
        return memory;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *AllocationLimiter = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *AllocationLimiter = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return null;
        const remapped = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return remapped;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *AllocationLimiter = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.live_bytes -|= memory.len;
    }

    const vtable = Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

test "lake bounded decode rejects count amplification before allocation" {
    var budget = try Budget.init(16, .{ .max_artifact_bytes = 16, .max_allocation_bytes = 64, .max_elements = 8 });
    try std.testing.expectError(error.InvalidEncodedCount, budget.admitCount(u64, 4, 3, 1));

    var allocation_budget = try Budget.init(16, .{ .max_artifact_bytes = 16, .max_allocation_bytes = 16, .max_elements = 8 });
    try std.testing.expectError(error.DecodedArtifactTooLarge, allocation_budget.admitCount(u64, 3, 16, 1));
}

test "lake bounded decode allocator rejects live allocation amplification" {
    var limiter = try AllocationLimiter.init(std.testing.allocator, 16);
    const alloc = limiter.allocator();
    const bytes = try alloc.alloc(u8, 16);
    defer alloc.free(bytes);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    try std.testing.expect(limiter.limit_exceeded);
}
