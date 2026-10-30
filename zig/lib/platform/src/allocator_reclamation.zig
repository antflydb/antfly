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
    threadlocal var reclamation_depth: usize = 0;
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
        if (depth < locks.len) locks[depth] = mutex;
        depth += 1;
    }
    pub fn leave() void {
        depth -= 1;
    }
    pub fn beginReclaim() void {
        reclamation_depth += 1;
    }
    pub fn endReclaim() void {
        reclamation_depth -= 1;
    }
    pub fn reuse(mutex: *std.atomic.Mutex) bool {
        return reclamation_depth != 0 and held(mutex);
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
