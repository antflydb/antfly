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

//! Same-owner reader fences, serialized by the connection's path-writer lease.
//! Other processes still see ordinary shared inode locks. Only a writer which
//! accounts for every known publication can suspend its own locks; an external
//! publication permanently poisons existing fences until their readers close.
const std = @import("std");
pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    /// Must outlive the last reader, not merely the writer. C ABI owners use
    /// the process-wide handle executor, which survives individual handles.
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    references: std.atomic.Value(usize) = .init(1),
    heap_owned: bool = false,
    readers: std.ArrayList(*Fence) = .empty,
    windows: usize = 0,
    known: ?struct { inode: u64, tip: [32]u8 } = null,

    pub const Fence = struct {
        file: std.Io.File,
        inode: u64,
        sequence: u64,
        /// Full graph scans also retain allocator/recovery metadata pages.
        metadata: bool = false,
        poisoned: bool = false,
        suspended: bool = false,
    };
    pub fn init(allocator: std.mem.Allocator, io: std.Io) Coordinator {
        return .{ .allocator = allocator, .io = io };
    }
    pub fn create(allocator: std.mem.Allocator, io: std.Io) !*Coordinator {
        const self = try allocator.create(Coordinator);
        self.* = init(allocator, io);
        self.heap_owned = true;
        return self;
    }
    pub fn release(self: *Coordinator) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.heap_owned);
        const allocator = self.allocator;
        self.deinit();
        allocator.destroy(self);
    }
    pub fn deinit(self: *Coordinator) void {
        std.debug.assert(self.readers.items.len == 0);
        self.readers.deinit(self.allocator);
    }
    /// The descriptor already holds its kernel shared lock. Registration may
    /// wait for a writer, which then treats this unregistered lock as external.
    pub fn register(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerWithScope(file, inode, sequence, tip, false);
    }
    pub fn registerFull(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8) !*Fence {
        return self.registerWithScope(file, inode, sequence, tip, true);
    }
    fn registerWithScope(self: *Coordinator, file: std.Io.File, inode: u64, sequence: u64, tip: [32]u8, metadata: bool) !*Fence {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const fence = try self.allocator.create(Fence);
        errdefer self.allocator.destroy(fence);
        // The shared lock may have blocked physical reuse before registration,
        // while an external writer promoted retirement. A delayed old reader
        // must never become eligible merely by arriving after that detection.
        const stale = if (self.known) |known| known.inode != inode or !std.mem.eql(u8, &known.tip, &tip) else false;
        fence.* = .{ .file = file, .inode = inode, .sequence = sequence, .metadata = metadata, .poisoned = stale };
        try self.readers.append(self.allocator, fence);
        _ = self.references.fetchAdd(1, .monotonic);
        if (self.known == null) self.known = .{ .inode = inode, .tip = tip };
        return fence;
    }
    pub fn unregister(self: *Coordinator, fence: *Fence) void {
        self.mutex.lockUncancelable(self.io);
        for (self.readers.items, 0..) |item, i| if (item == fence) {
            _ = self.readers.swapRemove(i);
            self.allocator.destroy(fence);
            self.mutex.unlock(self.io);
            self.release();
            return;
        };
        unreachable;
    }
    /// The path-writer lease must enclose this entire window, including
    /// restoration after exclusive reuse. Do not retain mutex across caller
    /// allocations: reclaiming an idle artifact may unregister its descriptor.
    pub fn begin(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool) ?u64 {
        return self.beginWithMetadata(inode, tip, reuse).data;
    }
    pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
    pub fn beginWithMetadata(self: *Coordinator, inode: u64, tip: [32]u8, reuse: bool) Frontier {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.windows += 1;
        if (self.known) |known| {
            if (known.inode != inode or !std.mem.eql(u8, &known.tip, &tip)) {
                for (self.readers.items) |fence| if (fence.inode == inode) {
                    fence.poisoned = true;
                };
            }
        }
        self.known = .{ .inode = inode, .tip = tip };
        var frontier: Frontier = .{};
        var can_suspend = reuse;
        for (self.readers.items) |fence| if (fence.inode == inode) {
            frontier.data = @min(frontier.data orelse fence.sequence, fence.sequence);
            if (fence.metadata) frontier.metadata = @min(frontier.metadata orelse fence.sequence, fence.sequence);
            can_suspend = can_suspend and !fence.poisoned;
        };
        if (can_suspend and self.windows == 1) for (self.readers.items) |fence| if (fence.inode == inode) {
            fence.file.unlock(self.io);
            fence.suspended = true;
        };
        return frontier;
    }
    pub fn end(self: *Coordinator) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        std.debug.assert(self.windows != 0);
        self.windows -= 1;
        if (self.windows != 0) return;
        // Losing a restored lock would expose live readers after the path
        // lease is released. Fail closed on an unexpected kernel-lock error.
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        for (self.readers.items) |fence| if (fence.suspended) {
            fence.file.lock(self.io, .shared) catch @panic("could not restore Lite reader fence");
            fence.suspended = false;
        };
    }
    /// Caller holds the path-writer lease and has read the current header.
    /// Fence readers captured before an unrecognized publication, then admit
    /// newly captured readers against this authoritative tip. Waiting until a
    /// writer window begins would poison those fresh readers too.
    pub fn observeCurrent(self: *Coordinator, inode: u64, tip: [32]u8) void {
        self.published(inode, tip, tip, false);
    }

    pub fn published(self: *Coordinator, inode: u64, previous_tip: [32]u8, tip: [32]u8, uncertain: bool) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const changed = if (self.known) |known| known.inode != inode or !std.mem.eql(u8, &known.tip, &previous_tip) else false;
        if (uncertain or changed) for (self.readers.items) |fence| if (fence.inode == inode) {
            fence.poisoned = true;
        };
        self.known = .{ .inode = inode, .tip = tip };
    }
};
