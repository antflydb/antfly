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

//! Stable handles shared by movable transactions. Retain at most sixteen small
//! nodes; concurrent overflow nodes are destroyed on return. No allocator or
//! cache callback runs while holding the free-list lock.
const std = @import("std");
const Allocator = std.mem.Allocator;
const resources = @import("../resource_manager.zig");
pub const MetadataPool = struct {
    const capacity = 1024;
    const alignment_bytes = 64;
    const Node = struct { next: ?*Node = null, payload: [capacity]u8 align(alignment_bytes) = undefined };
    backing: ?Allocator = null,
    budget: ?resources.BudgetedAllocator = null,
    mutex: std.atomic.Mutex = .unlocked,
    idle: ?*Node = null,
    idle_count: usize = 0,
    active: usize = 0,

    pub fn allocator(self: *@This(), backing: Allocator, manager: ?*resources.ResourceManager) Allocator {
        // Store owners initialize backing before publishing any read handle.
        lock(&self.mutex);
        if (self.backing == null) {
            self.backing = backing;
            if (manager) |m| {
                self.budget = resources.BudgetedAllocator.initReclaiming(m, .lite_docstore_snapshot_cache, backing, 1);
                if (resources.BudgetedAllocator.fromAllocator(backing)) |owner| {
                    if (owner.reservation.manager == m) {
                        if (owner.reservation.slice == .lite_docstore_snapshot_cache) self.budget = null else self.budget.?.reservation.host_charge = false;
                    }
                }
            }
        }
        self.mutex.unlock();
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn heap(self: *@This()) Allocator {
        return if (self.budget) |*budget| budget.threadSafeAllocator() else self.backing.?;
    }
    pub fn residentManager(self: *@This()) ?*resources.ResourceManager {
        if (self.budget) |*budget| return budget.reservation.manager;
        if (self.backing) |backing| if (resources.BudgetedAllocator.fromAllocator(backing)) |owner| return owner.reservation.manager;
        return null;
    }
    fn lock(mutex: *std.atomic.Mutex) void {
        while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn fits(len: usize, alignment: std.mem.Alignment) bool {
        return len <= capacity and alignment.toByteUnits() <= alignment_bytes;
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!fits(len, alignment)) return self.heap().rawAlloc(len, alignment, ra);
        lock(&self.mutex);
        const node = self.idle;
        if (node) |n| {
            self.idle = n.next;
            self.idle_count -= 1;
        }
        self.active += 1;
        self.mutex.unlock();
        const owned = node orelse self.heap().create(Node) catch {
            lock(&self.mutex);
            self.active -= 1;
            self.mutex.unlock();
            return null;
        };
        return &owned.payload;
    }
    fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, _: usize) bool {
        return fits(memory.len, alignment) and fits(len, alignment);
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        return if (resize(ctx, memory, alignment, len, ra)) memory.ptr else null;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!fits(memory.len, alignment)) return self.heap().rawFree(memory, alignment, ra);
        const payload: *align(alignment_bytes) [capacity]u8 = @ptrCast(@alignCast(memory.ptr));
        const node: *Node = @fieldParentPtr("payload", payload);
        lock(&self.mutex);
        self.active -= 1;
        const retain = self.idle_count < 16;
        if (retain) {
            node.next = self.idle;
            self.idle = node;
            self.idle_count += 1;
        }
        self.mutex.unlock();
        if (!retain) self.heap().destroy(node);
    }
    pub const Reclaimed = struct {
        physical: u64 = 0,
        logical: u64 = 0,
        pub fn total(self: @This()) u64 {
            return self.physical +| self.logical;
        }
    };
    pub fn reclaim(self: *@This(), target: u64) u64 {
        return self.reclaimDetailed(target).total();
    }
    /// Logical spare credit is admission relief, not freed node storage.
    pub fn reclaimDetailed(self: *@This(), target: u64) Reclaimed {
        // A nested backing allocator may request reclamation while our own
        // allocator is busy. Never reenter it through a blocking free.
        const budget = if (self.budget) |*owner| owner else null;
        if (budget) |owner| if (!owner.allocator_mutex.tryLock()) return .{};
        defer if (budget) |owner| owner.allocator_mutex.unlock();
        const a = if (budget) |owner| owner.allocator() else if (self.backing) |backing| backing else return .{};
        var freed: u64 = 0;
        while (freed < target) {
            if (!self.mutex.tryLock()) break;
            const node = self.idle;
            if (node) |n| {
                self.idle = n.next;
                self.idle_count -= 1;
            }
            self.mutex.unlock();
            const owned = node orelse break;
            a.destroy(owned);
            freed += @sizeOf(Node);
        }
        const logical = if (budget) |owner| owner.releaseUnusedCredit() else 0;
        return .{ .physical = freed, .logical = logical };
    }
    pub fn deinit(self: *@This()) void {
        std.debug.assert(self.active == 0);
        _ = self.reclaim(std.math.maxInt(u64));
        if (self.budget) |*budget| budget.deinit();
    }
};
