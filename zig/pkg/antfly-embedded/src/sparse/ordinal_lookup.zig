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

const std = @import("std");
const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
/// Same-transaction native identity resolution. A missing block proof requests
/// the legacy point-lookup path; it must never be interpreted as an empty set.
pub const KeyFilter = struct {
    ptr: *anyopaque,
    allows: *const fn (*anyopaque, []const u8) anyerror!bool,
};
pub const Lookup = struct {
    ptr: *anyopaque,
    one: *const fn (*anyopaque, []const u8) anyerror!?u32,
    /// Sequential native identity window in the same pinned read transaction.
    /// False requests the exact legacy key predicate, never an empty selection.
    native_range: ?*const fn (*anyopaque, std.mem.Allocator, u32, u32, KeyFilter, *Bitmap) anyerror!bool = null,
    /// Bounded directory translation, when supported by this generation.
    bounded_block: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, u32, *const Bitmap, *Bitmap, *WorkBudget) anyerror!bool = null,
    block: *const fn (*anyopaque, std.mem.Allocator, []const u8, u32, *const Bitmap, *Bitmap) anyerror!bool,
};

/// Optional planning may spend at most this many authenticated physical
/// directory blocks or legacy identity point seeks. Exhaustion discards the
/// whole partial selection and retains the exact key predicate.
pub const WorkBudget = struct {
    blocks: usize = 4096,
    points: usize = 4096,
    pub fn takeBlock(self: *@This()) !void {
        if (self.blocks == 0) return error.OrdinalPlanningBudgetExceeded;
        self.blocks -= 1;
    }
    pub fn takePoint(self: *@This()) !void {
        if (self.points == 0) return error.OrdinalPlanningBudgetExceeded;
        self.points -= 1;
    }
};

/// Stable, query-owned allocator for optional compressed masks. The cap applies
/// to requested live bytes, including bitmap navigation and temporary clones.
/// Ordinary allocation failures still propagate; only an explicit cap rejection
/// permits the caller to abandon this optimization.
pub const MaskBudget = struct {
    backing: std.mem.Allocator,
    limit: usize,
    live: usize = 0,
    exhausted: bool = false,
    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn permits(self: *@This(), old: usize, len: usize) bool {
        // Resize/remap are optional allocator probes. If one is capped and a
        // subsequent permitted allocation fails in the backing allocator, that
        // ordinary failure must not inherit the earlier cap classification.
        self.exhausted = false;
        if (len <= old or len - old <= self.limit - self.live) return true;
        self.exhausted = true;
        return false;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.permits(0, len)) return null;
        const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return result;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.permits(bytes.len, len) or !self.backing.rawResize(bytes, alignment, len, ra)) return false;
        self.live = self.live - bytes.len + len;
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.permits(bytes.len, len)) return null;
        const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
        self.live = self.live - bytes.len + len;
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.backing.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
    pub fn destroy(self: *@This()) void {
        std.debug.assert(self.live == 0);
        self.backing.destroy(self);
    }
};

/// Owned masks in one pinned sparse generation. A null include admits every
/// ordinal; an empty include admits none. Exclusions never require constructing
/// the potentially archive-sized complement.
pub const Selection = struct {
    budget: ?*MaskBudget = null,
    /// Unmaterialized constraints remain exact candidate predicates.
    residual: bool = false,
    /// Complete physical membership can be translated in reached native windows.
    deferred: bool = false,
    include: ?Bitmap = null,
    exclude: ?Bitmap = null,
    pub fn initBounded(a: std.mem.Allocator, bytes: usize) !Selection {
        const budget = try a.create(MaskBudget);
        budget.* = .{ .backing = a, .limit = bytes };
        return .{ .budget = budget };
    }
    pub fn allocator(self: *const Selection) std.mem.Allocator {
        return self.budget.?.allocator();
    }
    pub fn prepareRead(self: *Selection) !void {
        if (self.include) |*bitmap| try bitmap.prepareRead();
        if (self.exclude) |*bitmap| try bitmap.prepareRead();
    }
    pub fn deinit(self: *Selection) void {
        if (self.include) |*bitmap| bitmap.deinit();
        if (self.exclude) |*bitmap| bitmap.deinit();
        if (self.budget) |budget| budget.destroy();
        self.* = undefined;
    }
};
