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

//! Synchronous optional residency shared by allocator owners. Callbacks borrow
//! stable storage until unregister; they perform bounded nonblocking teardown.
const std = @import("std");
pub const Guard = struct {
    threadlocal var depth: usize = 0;
    threadlocal var locks: [128]?*std.atomic.Mutex = @splat(null);
    threadlocal var reclamation_count: usize = 0;
    const Ownership = enum { ordinary, reclamation, lendable };
    threadlocal var ownership: [locks.len]Ownership = @splat(.ordinary);
    pub fn active() bool {
        return depth != 0;
    }
    pub fn allowed() bool {
        return depth < locks.len;
    }
    pub fn held(mutex: *std.atomic.Mutex) bool {
        for (locks[0..@min(depth, locks.len)]) |locked| if (locked == mutex) return true;
        return false;
    }
    pub fn enter(mutex: *std.atomic.Mutex) void {
        push(mutex, .ordinary);
    }
    /// Record a mutex explicitly acquired for teardown by a reclamation scope.
    pub fn enterReclamation(mutex: *std.atomic.Mutex) void {
        std.debug.assert(allowed());
        push(mutex, .reclamation);
    }
    /// A stateless synchronizing adapter may lend its held lock to an
    /// audited reclaimer. The reclaimer must still check the entire free path.
    pub fn enterLendable(mutex: *std.atomic.Mutex) void {
        push(mutex, .lendable);
    }
    fn push(mutex: *std.atomic.Mutex, kind: Ownership) void {
        if (allowed()) {
            locks[depth] = mutex;
            ownership[depth] = kind;
            if (kind == .reclamation) reclamation_count += 1;
        }
        depth += 1;
    }
    pub fn leave() void {
        depth -= 1;
        if (depth < locks.len and ownership[depth] == .reclamation) reclamation_count -= 1;
    }
    pub fn reuse(mutex: *std.atomic.Mutex) bool {
        if (reclamation_count == 0) return false;
        return contains(mutex, .reclamation);
    }
    fn contains(mutex: *std.atomic.Mutex, kind: Ownership) bool {
        const count = @min(depth, locks.len);
        for (locks[0..count], ownership[0..count]) |locked, owned| {
            if (locked == mutex and owned == kind) return true;
        }
        return false;
    }
};

/// A checked lock receipt. Only audited stateless wrappers may lend a lock,
/// and only callers that understand that wrapper's backing may borrow it.
pub const ReclamationLock = struct {
    mutex: *std.atomic.Mutex,
    acquired: bool,

    pub fn tryAcquire(mutex: *std.atomic.Mutex, allow_borrow: bool) ?ReclamationLock {
        if (!Guard.allowed()) return null;
        const borrow = allow_borrow and Guard.contains(mutex, .lendable);
        if (!borrow and (Guard.held(mutex) or !mutex.tryLock())) return null;
        Guard.enterReclamation(mutex);
        return .{ .mutex = mutex, .acquired = !borrow };
    }

    pub fn deinit(self: ReclamationLock) void {
        Guard.leave();
        if (self.acquired) self.mutex.unlock();
    }
};

pub const Reclaimer = struct { ptr: *anyopaque, reclaim: *const fn (*anyopaque) bool, next: ?*Reclaimer = null };
pub const Registry = struct {
    mutex: std.atomic.Mutex = .unlocked,
    count: std.atomic.Value(usize) = .init(0),
    head: ?*Reclaimer = null,
    cursor: ?*Reclaimer = null,
    pub fn active(self: *Registry) bool {
        return self.count.load(.acquire) != 0;
    }
    pub fn register(self: *Registry, owner: *Reclaimer) void {
        @import("sync.zig").lockYielding(&self.mutex);
        defer self.mutex.unlock();
        owner.next = self.head;
        self.head = owner;
        _ = self.count.fetchAdd(1, .release);
    }
    pub fn unregister(self: *Registry, owner: *Reclaimer) void {
        @import("sync.zig").lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var link = &self.head;
        while (link.*) |current| {
            if (current == owner) {
                link.* = current.next;
                if (self.cursor == owner) self.cursor = current.next;
                owner.next = null;
                _ = self.count.fetchSub(1, .release);
                return;
            }
            link = &current.next;
        }
        unreachable;
    }
    pub fn reclaimOne(self: *Registry) bool {
        @import("sync.zig").lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const first = self.cursor orelse self.head orelse return false;
        var current = first;
        while (true) {
            const next = current.next orelse self.head.?;
            if (current.reclaim(current.ptr)) {
                self.cursor = next;
                return true;
            }
            current = next;
            if (current == first) return false;
        }
    }
};

/// A stable synchronized refundable quota owner. Allocator recognition is an
/// opt-in contract, not permission to allocate through the backing directly.
pub const Quota = struct {
    backing: std.mem.Allocator,
    live_bytes: usize = 0,
    peak_bytes: usize = 0,
    largest_rejected_growth: usize = 0,
    largest_backing_failure: usize = 0,
    max_live_bytes: usize,
    limit_exceeded: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    residency: Registry = .{},

    pub fn init(backing: std.mem.Allocator, maximum: usize) Quota {
        return .{ .backing = backing, .max_live_bytes = maximum };
    }
    pub fn deinit(self: *Quota) void {
        std.debug.assert(self.live_bytes == 0 and !self.residency.active());
    }
    pub fn owner(a: std.mem.Allocator) ?*Quota {
        if (a.vtable != &vtable) return null;
        return @ptrCast(@alignCast(a.ptr));
    }
    pub fn allocator(self: *Quota) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn lock(self: *Quota) void {
        @import("sync.zig").lockYielding(&self.mutex);
    }
    pub fn available(self: *Quota) usize {
        const held = Guard.held(&self.mutex);
        if (!held) self.lock();
        defer if (!held) self.mutex.unlock();
        return self.max_live_bytes -| self.live_bytes;
    }
    pub fn prepare(self: *Quota, growth: usize) void {
        if (growth == 0 or !Guard.allowed() or !self.residency.active()) return;
        while (growth > self.available()) if (!self.residency.reclaimOne()) return;
    }
    fn permits(self: *Quota, growth: usize) bool {
        if (growth <= self.max_live_bytes -| self.live_bytes) return true;
        self.limit_exceeded = true;
        self.largest_rejected_growth = @max(self.largest_rejected_growth, growth);
        return false;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Quota = @ptrCast(@alignCast(raw));
        self.prepare(len);
        self.lock();
        defer self.mutex.unlock();
        Guard.enter(&self.mutex);
        defer Guard.leave();
        if (!self.permits(len)) return null;
        const result = self.backing.rawAlloc(len, alignment, ra) orelse {
            self.largest_backing_failure = @max(self.largest_backing_failure, len);
            return null;
        };
        self.live_bytes += len;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        return result;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Quota = @ptrCast(@alignCast(raw));
        const growth = len -| bytes.len;
        self.prepare(growth);
        self.lock();
        defer self.mutex.unlock();
        Guard.enter(&self.mutex);
        defer Guard.leave();
        if (!self.permits(growth) or !self.backing.rawResize(bytes, alignment, len, ra)) return false;
        self.live_bytes = self.live_bytes - bytes.len + len;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Quota = @ptrCast(@alignCast(raw));
        const growth = len -| bytes.len;
        self.prepare(growth);
        self.lock();
        defer self.mutex.unlock();
        Guard.enter(&self.mutex);
        defer Guard.leave();
        if (!self.permits(growth)) return null;
        const result = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
        self.live_bytes = self.live_bytes - bytes.len + len;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Quota = @ptrCast(@alignCast(raw));
        const reuse = Guard.reuse(&self.mutex);
        if (!reuse) self.lock();
        defer if (!reuse) self.mutex.unlock();
        Guard.enter(&self.mutex);
        defer Guard.leave();
        self.backing.rawFree(bytes, alignment, ra);
        std.debug.assert(bytes.len <= self.live_bytes);
        self.live_bytes -= bytes.len;
    }
    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
};
