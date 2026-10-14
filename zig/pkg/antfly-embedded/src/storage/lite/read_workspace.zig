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

//! Stable store-owned scratch, exclusively leased by one read scope. Heap
//! residency and the borrowing query's admission are accounted independently.
const std = @import("std");
const native = @import("native.zig");
const resources = @import("../resource_manager.zig");
const Scratch = @import("../../segment_source.zig").Scratch;
const Allocator = std.mem.Allocator;

pub const Workspace = struct {
    backing: Allocator,
    budget: ?resources.BudgetedAllocator = null,
    query_budget: ?*resources.BudgetedAllocator = null,
    query_reservation: ?resources.BudgetedAllocator.BorrowedScratch = null,
    result_backing: ?Allocator = null,
    live: usize = 0,
    busy: bool = false,
    allocation_failed: bool = false,
    batch: Scratch,
    graph: Scratch,
    native_state: native.DocumentBatchReadState,
    resolved_scores: native.ResolvedScoreBatch = .{},
    score_key_storage: []align(@alignOf([]const u8)) u8 = &.{},
    score_plan_borrowed: bool = false,
    score_plan: @import("../prepared_score_plan.zig").PreparedScorePlan = .{},

    pub fn create(backing: Allocator, manager: ?*resources.ResourceManager) !*Workspace {
        const self = try backing.create(Workspace);
        self.* = .{ .backing = backing, .batch = undefined, .graph = undefined, .native_state = .{} };
        if (manager) |m| {
            self.budget = resources.BudgetedAllocator.initReclaiming(m, .lite_docstore_snapshot_cache, backing, 1);
            if (resources.BudgetedAllocator.fromAllocator(backing)) |owner| {
                if (owner.reservation.manager == m) {
                    if (owner.reservation.slice == .lite_docstore_snapshot_cache) self.budget = null else self.budget.?.reservation.host_charge = false;
                }
            }
        }
        const a = self.allocator();
        self.batch = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.graph = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.native_state.resolved = &self.resolved_scores;
        self.native_state.allocator = a;
        self.resolved_scores.promotion_allocator = self.optionalAllocator();
        self.native_state.path.allocator = a;
        return self;
    }

    pub fn destroy(self: *Workspace) void {
        const backing = self.backing;
        self.score_plan.deinit();
        self.native_state.deinit();
        self.batch.deinit();
        self.graph.deinit();
        self.allocator().free(self.score_key_storage);
        self.score_key_storage = &.{};
        if (self.budget) |*b| b.deinit();
        backing.destroy(self);
    }

    pub fn begin(self: *Workspace, query_budget: ?*resources.BudgetedAllocator) !void {
        self.query_budget = query_budget;
        self.admit(self.live) catch |err| {
            if (self.evictOptionalStorage()) {
                self.admit(self.live) catch |retry_err| {
                    self.query_budget = null;
                    return retry_err;
                };
            } else {
                self.query_budget = null;
                return err;
            }
        };
    }

    /// Failed speculative reads must not raise the streaming memory floor.
    /// Call only after the scope has finished using its working allocations.
    pub fn discardRetainedStorage(self: *Workspace) void {
        self.score_plan.deinit();
        self.native_state.deinit();
        self.batch.deinit();
        self.graph.deinit();
        self.allocator().free(self.score_key_storage);
        self.score_key_storage = &.{};
        self.allocation_failed = false;
        const a = self.allocator();
        self.batch = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.graph = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.native_state = .{ .allocator = a, .path = .{ .allocator = a }, .resolved = &self.resolved_scores };
    }

    pub fn end(self: *Workspace) void {
        self.score_plan_borrowed = false;
        if (self.allocation_failed) self.discardRetainedStorage();
        // No file or cache lease survives a query. Snapshot identity is numeric;
        // index frames retain independently owned storage and its allocator.
        self.native_state.detach();
        self.resetScratch(&self.batch);
        self.resetScratch(&self.graph);
        if (self.score_key_storage.len > 256 * 1024) {
            self.allocator().free(self.score_key_storage);
            self.score_key_storage = &.{};
        }
        if (self.query_reservation) |*r| r.release();
        self.query_reservation = null;
        self.query_budget = null;
        self.result_backing = null;
    }

    pub fn resetScoreBatch(self: *Workspace) void {
        self.score_plan_borrowed = false;
        self.native_state.detach();
        if (self.allocation_failed) {
            self.discardRetainedStorage();
            self.allocation_failed = false;
        } else {
            self.resetScratch(&self.batch);
            self.resetScratch(&self.graph);
        }
    }

    /// Exact-size encoder storage is independent of graph arena growth. The
    /// previous span has finished before growth; no caller owns these bytes.
    pub fn scoreKeyStorage(self: *Workspace, bytes: usize) ![]align(@alignOf([]const u8)) u8 {
        if (self.score_key_storage.len < bytes) {
            self.allocator().free(self.score_key_storage);
            self.score_key_storage = &.{};
            self.score_key_storage = try self.allocator().alignedAlloc(u8, .fromByteUnits(@alignOf([]const u8)), bytes);
        }
        return self.score_key_storage[0..bytes];
    }

    pub fn resetBatch(self: *Workspace) void {
        self.resetScratch(&self.batch);
    }
    fn resetScratch(self: *Workspace, scratch: *Scratch) void {
        // Arena consolidation is optional preheating after the required read.
        const child = scratch.arena.child_allocator;
        scratch.arena.child_allocator = self.optionalAllocator();
        defer scratch.arena.child_allocator = child;
        scratch.reset();
    }

    fn admit(self: *Workspace, bytes: usize) !void {
        if (self.query_budget) |b| {
            if (self.query_reservation) |*receipt| try receipt.growTo(bytes) else self.query_reservation = try b.borrowResidentScratch(bytes, self.residentManager());
        }
    }

    fn residentManager(self: *Workspace) ?*resources.ResourceManager {
        if (self.budget) |*b| return b.reservation.manager;
        if (resources.BudgetedAllocator.fromAllocator(self.backing)) |b| return b.reservation.manager;
        return null;
    }

    fn restoreAdmission(self: *Workspace) void {
        if (self.query_reservation) |*receipt| receipt.shrinkTo(self.live);
    }

    fn heap(self: *Workspace) Allocator {
        return if (self.budget) |*b| b.allocator() else self.backing;
    }
    // Arena growth must not depend on whether a failed speculative allocation
    // left a remappable heap block behind. Stable chunks give a clean retry the
    // same memory floor as a cold read. Native owned buffers still resize.
    fn arenaAllocator(self: *Workspace) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    pub fn allocator(self: *Workspace) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    /// Optional cache ownership must not poison otherwise successful scratch.
    pub fn optionalAllocator(self: *Workspace) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = optionalAlloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    /// Results remain query-owned; this adapter only evicts unused residency
    /// before retrying growth. Its context is stable for the exclusive lease.
    pub fn resultAllocator(self: *Workspace, backing: Allocator) Allocator {
        self.result_backing = backing;
        return .{ .ptr = self, .vtable = &.{ .alloc = resultAlloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = resultFree } };
    }
    fn resultAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        const backing = self.result_backing.?;
        if (backing.rawAlloc(len, alignment, ra)) |p| return p;
        if (!self.evictOptionalStorage()) return null;
        return backing.rawAlloc(len, alignment, ra);
    }
    fn resultFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        self.result_backing.?.rawFree(memory, alignment, ra);
    }
    fn evictOptionalStorage(self: *Workspace) bool {
        var released = false;
        if (!self.resolved_scores.borrowed and self.resolved_scores.storage != null) {
            self.resolved_scores.deinit(self.optionalAllocator());
            released = true;
        }
        if (!self.score_plan_borrowed and self.score_plan.storage != null) {
            self.score_plan.deinit();
            released = true;
        }
        return released;
    }
    fn optionalAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocImpl(ctx, len, alignment, ra, false);
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocImpl(ctx, len, alignment, ra, true);
    }
    fn allocImpl(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize, required: bool) ?[*]u8 {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        for (0..2) |attempt| {
            const next = std.math.add(usize, self.live, len) catch return null;
            self.admit(next) catch {
                if (required and attempt == 0 and self.evictOptionalStorage()) continue;
                if (required) self.allocation_failed = true;
                return null;
            };
            const p = self.heap().rawAlloc(len, alignment, ra) orelse {
                self.restoreAdmission();
                if (required and attempt == 0 and self.evictOptionalStorage()) continue;
                if (required) self.allocation_failed = true;
                return null;
            };
            self.live += len;
            return p;
        }
        unreachable;
    }
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        const next = std.math.add(usize, self.live - memory.len, len) catch return false;
        if (next > self.live) self.admit(next) catch return false;
        if (!self.heap().rawResize(memory, alignment, len, ra)) {
            self.restoreAdmission();
            return false;
        }
        self.live = next;
        self.restoreAdmission();
        return true;
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        const next = std.math.add(usize, self.live - memory.len, len) catch return null;
        if (next > self.live) self.admit(next) catch return null;
        const p = self.heap().rawRemap(memory, alignment, len, ra) orelse {
            self.restoreAdmission();
            return null;
        };
        self.live = next;
        self.restoreAdmission();
        return p;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        self.heap().rawFree(memory, alignment, ra);
        self.live -= memory.len;
        self.restoreAdmission();
    }
};

test "lite workspace optional cache evicts for required growth but protects borrowed references" {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    const w = try Workspace.create(a, &manager);
    defer w.destroy();
    var query = resources.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    try w.begin(&query);
    defer w.end();
    const scratch = try w.allocator().alloc(u8, 4096);
    defer w.allocator().free(scratch);
    w.resolved_scores.storage = try w.optionalAllocator().alloc(u8, 32 * 1024);
    manager.slices[@backingInt(resources.Slice.dense_search_working_set)].budget.hard_limit_bytes = w.live + 1024;
    w.resolved_scores.borrowed = true;
    try std.testing.expectError(error.OutOfMemory, w.allocator().alloc(u8, 8192));
    try std.testing.expect(w.resolved_scores.storage != null);
    w.allocation_failed = false;
    w.resolved_scores.borrowed = false;
    const required = try w.allocator().alloc(u8, 8192);
    defer w.allocator().free(required);
    try std.testing.expect(w.resolved_scores.storage == null);
    try std.testing.expect(!w.allocation_failed);
    try std.testing.expectEqual(@as(usize, 4096 + 8192), w.live);
}

test "lite workspace evicts idle plans and protects active plan ownership" {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    const w = try Workspace.create(a, &manager);
    defer w.destroy();
    var query = resources.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    try w.begin(&query);
    defer w.end();
    try std.testing.expect(try w.score_plan.prepare(w.allocator(), &.{"p/"}, &.{"a"}));
    manager.slices[@backingInt(resources.Slice.dense_search_working_set)].budget.hard_limit_bytes = w.live + 128;
    w.score_plan_borrowed = true;
    try std.testing.expectError(error.OutOfMemory, w.allocator().alloc(u8, 1024));
    try std.testing.expect(w.score_plan.matches(&.{"p/"}, &.{"a"}));
    w.allocation_failed = false;
    w.score_plan_borrowed = false;
    const bytes = try w.allocator().alloc(u8, 1024);
    defer w.allocator().free(bytes);
    try std.testing.expect(w.score_plan.storage == null);
    try std.testing.expect(!w.allocation_failed);
}
