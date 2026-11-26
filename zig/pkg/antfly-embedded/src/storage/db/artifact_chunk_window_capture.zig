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

//! Budgeted and revision-bound replay state for a chunk member's window set.
const std = @import("std");
const Allocator = std.mem.Allocator;
const publication = @import("artifact_publication.zig");
const resource_manager_mod = @import("../resource_manager.zig");

// Preserve partial member scans across replay attempts. Each checkpoint
// owns budgeted storage so its reservation follows it while it waits.
pub const MediaWindowCapture = struct {
    backing: Allocator,
    arena: std.heap.ArenaAllocator,
    budget: ?resource_manager_mod.BudgetedAllocator,
    identity: publication.Digest,
    member: [32]u8,
    resume_key: std.ArrayList(u8) = .empty,
    records: std.ArrayList(publication.ArtifactSource) = .empty,
    key_bytes: usize = 0,
    complete: bool = false,

    pub fn create(backing: Allocator, manager: ?*resource_manager_mod.ResourceManager, cache: *MediaWindowCaptureCache, member: [32]u8, identity: publication.Digest) !*MediaWindowCapture {
        const scan = try backing.create(MediaWindowCapture);
        scan.* = .{
            .backing = backing,
            .arena = undefined,
            .budget = if (manager) |resources| cache.budget(resources, backing) else null,
            .identity = identity,
            .member = member,
        };
        scan.arena = .init(scan.allocator());
        return scan;
    }

    pub fn allocator(self: *MediaWindowCapture) Allocator {
        return if (self.budget) |*budget| budget.allocator() else self.backing;
    }

    pub fn admissionError(self: *MediaWindowCapture, err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
        return err;
    }

    pub fn setResumeKey(self: *MediaWindowCapture, key: []const u8) !void {
        self.resume_key.resize(self.allocator(), key.len) catch |err| return self.admissionError(err);
        @memcpy(self.resume_key.items, key);
    }

    pub fn appendGuard(self: *MediaWindowCapture, guard: publication.ArtifactSource) !void {
        const memory = self.arena.allocator();
        self.records.ensureUnusedCapacity(self.allocator(), 1) catch |err| return self.admissionError(err);
        var owned = guard;
        owned.key = memory.dupe(u8, guard.key) catch |err| return self.admissionError(err);
        self.records.appendAssumeCapacity(owned);
        self.key_bytes += owned.key.len;
    }

    pub fn deinit(self: *MediaWindowCapture) void {
        self.resume_key.deinit(self.allocator());
        self.records.deinit(self.allocator());
        self.arena.deinit();
        if (self.budget) |*budget| budget.deinit();
        self.backing.destroy(self);
    }
};

pub const MediaWindowCaptureCache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    // Oldest first. Several documents can yield in the same replay pass.
    saved: [32]*MediaWindowCapture = undefined,
    saved_count: usize = 0,

    reclaimer: ?struct { manager: *resource_manager_mod.ResourceManager, id: u64 } = null,

    pub fn register(self: *MediaWindowCaptureCache, manager: *resource_manager_mod.ResourceManager) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.reclaimer != null) return;
        self.reclaimer = .{ .manager = manager, .id = try manager.registerReclaimer(.relational_preparation_working_set, self, reclaim) };
    }

    pub fn reclaim(raw: *anyopaque, target: u64) u64 {
        const self: *MediaWindowCaptureCache = @ptrCast(@alignCast(raw));
        var released: u64 = 0;
        for (0..self.saved.len) |_| {
            if (released >= target or !self.mutex.tryLock()) break;
            const oldest = if (self.saved_count != 0) self.remove(0) else null;
            self.mutex.unlock();
            const scan = oldest orelse break;
            if (scan.budget) |*tracked| released +|= tracked.reservation.reservedBytes();
            scan.deinit();
        }
        return released;
    }

    fn remove(self: *MediaWindowCaptureCache, index: usize) *MediaWindowCapture {
        const previous = self.saved[index];
        self.saved_count -= 1;
        std.mem.copyForwards(*MediaWindowCapture, self.saved[index..self.saved_count], self.saved[index + 1 .. self.saved_count + 1]);
        return previous;
    }

    fn find(self: *MediaWindowCaptureCache, member: [32]u8) ?usize {
        for (self.saved[0..self.saved_count], 0..) |scan, index| {
            if (std.mem.eql(u8, &scan.member, &member)) return index;
        }
        return null;
    }

    pub fn take(self: *MediaWindowCaptureCache, member: [32]u8, identity: publication.Digest) ?*MediaWindowCapture {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const previous = if (self.find(member)) |index| self.remove(index) else null;
        self.mutex.unlock();
        if (previous) |old| {
            if (std.mem.eql(u8, &old.identity, &identity)) return old;
            old.deinit();
        }
        return null;
    }

    pub fn budget(self: *MediaWindowCaptureCache, manager: *resource_manager_mod.ResourceManager, backing: Allocator) resource_manager_mod.BudgetedAllocator {
        return resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, backing, 1)
            .withLocalReclaimer(self, reclaim);
    }

    /// Admit a suspended member without evicting another runnable member.
    /// When all slots are taken, new wide members retry after admitted
    /// members finish. They still release the replay worker at the boundary.
    pub fn saveIfSpace(self: *MediaWindowCaptureCache, scan: *MediaWindowCapture) bool {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const previous = if (self.find(scan.member)) |index| self.remove(index) else null;
        if (self.saved_count == self.saved.len) {
            self.mutex.unlock();
            return false;
        }
        self.saved[self.saved_count] = scan;
        self.saved_count += 1;
        self.mutex.unlock();
        if (previous) |old| old.deinit();
        return true;
    }

    pub fn deinit(self: *MediaWindowCaptureCache) void {
        if (self.reclaimer) |entry| entry.manager.unregisterReclaimer(entry.id);
        for (self.saved[0..self.saved_count]) |scan| scan.deinit();
    }
};
