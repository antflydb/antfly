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
    live: usize = 0,
    busy: bool = false,
    allocation_failed: bool = false,
    batch: Scratch,
    graph: Scratch,
    native_state: native.DocumentBatchReadState,
    resolved_scores: native.ResolvedScoreBatch = .{},
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
        self.native_state.path.allocator = a;
        return self;
    }

    pub fn destroy(self: *Workspace) void {
        const backing = self.backing;
        self.score_plan.deinit();
        self.native_state.deinit();
        self.batch.deinit();
        self.graph.deinit();
        if (self.budget) |*b| b.deinit();
        backing.destroy(self);
    }

    pub fn begin(self: *Workspace, query_budget: ?*resources.BudgetedAllocator) !void {
        self.query_budget = query_budget;
        self.admit(self.live) catch |err| {
            self.query_budget = null;
            return err;
        };
    }

    /// Failed speculative reads must not raise the streaming memory floor.
    /// Call only after the scope has finished using its working allocations.
    pub fn discardRetainedStorage(self: *Workspace) void {
        self.score_plan.deinit();
        self.native_state.deinit();
        self.batch.deinit();
        self.graph.deinit();
        self.allocation_failed = false;
        const a = self.allocator();
        self.batch = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.graph = Scratch.init(self.arenaAllocator(), 256 * 1024);
        self.native_state = .{ .allocator = a, .path = .{ .allocator = a }, .resolved = &self.resolved_scores };
    }

    pub fn end(self: *Workspace) void {
        if (self.allocation_failed) self.discardRetainedStorage();
        // No file or cache lease survives a query. Snapshot identity is numeric;
        // index frames retain independently owned storage and its allocator.
        self.native_state.detach();
        self.batch.reset();
        self.graph.reset();
        if (self.query_reservation) |*r| r.release();
        self.query_reservation = null;
        self.query_budget = null;
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
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Workspace = @ptrCast(@alignCast(ctx));
        self.admit(std.math.add(usize, self.live, len) catch return null) catch {
            self.allocation_failed = true;
            return null;
        };
        const p = self.heap().rawAlloc(len, alignment, ra) orelse {
            self.allocation_failed = true;
            self.restoreAdmission();
            return null;
        };
        self.live += len;
        return p;
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
