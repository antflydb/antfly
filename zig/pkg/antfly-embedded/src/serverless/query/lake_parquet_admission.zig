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

//! Shared live-byte admission for independently paged Parquet columns. Credits
//! follow retained result leases; neither cursor advancement nor cache hits
//! reset a request's residency. Encoded input waits only on transient readers.
const std = @import("std");
const A = std.mem.Allocator;
const Self = @This();
const Budget = @import("../../sql/memory_budget.zig");
const Context = @import("lake_read_context.zig").Context;
const RangeLease = @import("lake_range_io.zig").RangeLease;
a: A,
backing: @import("../../sql/parallel_scheduler.zig").LockedAllocator,
memory: Budget = undefined,
refs: std.atomic.Value(usize) = .init(1),
mutex: std.atomic.Mutex = .unlocked,
raw_limit: usize,
raw: usize = 0,
resident_limit: usize,
resident: usize = 0,
peak: usize = 0,
input_limit: usize,
input: usize = 0,
input_mutex: std.Io.Mutex = .init,
input_changed: std.Io.Condition = .init,

pub fn create(a: A, raw_limit: usize, struct_limit: usize, input_limit: usize) !*Self {
    const self = try a.create(Self);
    self.* = .{ .a = a, .backing = .{ .backing = a }, .raw_limit = raw_limit, .resident_limit = raw_limit +| struct_limit, .input_limit = input_limit };
    self.memory = .{ .backing = self.backing.allocator(), .limit = std.math.maxInt(usize), .admission = .{ .ptr = self, .reserve = reserveResident, .release = releaseResident } };
    return self;
}
pub fn retain(self: *Self) *Self {
    _ = self.refs.fetchAdd(1, .monotonic);
    return self;
}
pub fn release(self: *Self) void {
    if (self.refs.fetchSub(1, .acq_rel) != 1) return;
    std.debug.assert(self.raw == 0 and self.resident == 0 and self.input == 0 and self.memory.live == 0);
    self.a.destroy(self);
}
pub fn allocator(self: *Self) A {
    return self.memory.allocator();
}
fn lock(self: *Self) void {
    while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
}
fn reserveResident(raw: *anyopaque, bytes: usize) bool {
    const self: *Self = @ptrCast(@alignCast(raw));
    self.lock();
    defer self.mutex.unlock();
    if (bytes > self.resident_limit -| self.resident) return false;
    self.resident += bytes;
    self.peak = @max(self.peak, self.resident);
    return true;
}
fn releaseResident(raw: *anyopaque, bytes: usize) void {
    const self: *Self = @ptrCast(@alignCast(raw));
    self.lock();
    defer self.mutex.unlock();
    std.debug.assert(bytes <= self.resident);
    self.resident -= bytes;
}
pub const Credit = struct {
    owner: *Self,
    refs: std.atomic.Value(usize) = .init(1),
    raw: usize,
    resident: usize = 0,
    parent: ?*Credit,
    pub fn retain(self: *Credit) *Credit {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Credit) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const state = self.owner;
        state.lock();
        std.debug.assert(self.raw <= state.raw and self.resident <= state.resident);
        state.raw -= self.raw;
        state.resident -= self.resident;
        state.mutex.unlock();
        if (self.parent) |parent| parent.release();
        state.backing.allocator().destroy(self);
        state.release();
    }
    /// Cached allocations have a different lifetime and allocator. Charge their
    /// actual arena capacity before adoption, including an unbound dictionary.
    pub fn cached(self: *Credit, bytes: usize) !void {
        std.debug.assert(self.resident == 0);
        if (!reserveResident(self.owner, bytes)) return error.ParquetPageTooLarge;
        self.resident = bytes;
    }
    pub fn growRaw(self: *Credit, bytes: usize) !void {
        const state = self.owner;
        state.lock();
        defer state.mutex.unlock();
        const extra = bytes -| self.raw;
        if (extra > state.raw_limit -| state.raw) return error.ParquetPageTooLarge;
        state.raw += extra;
        self.raw += extra;
    }
};
pub fn claim(self: *Self, bytes: usize, parent: ?*Credit) !*Credit {
    self.lock();
    if (bytes > self.raw_limit -| self.raw) {
        self.mutex.unlock();
        return error.ParquetPageTooLarge;
    }
    self.raw += bytes;
    self.mutex.unlock();
    errdefer {
        self.lock();
        self.raw -= bytes;
        self.mutex.unlock();
    }
    const credit = try self.backing.allocator().create(Credit);
    credit.* = .{ .owner = self.retain(), .raw = bytes, .parent = if (parent) |p| p.retain() else null };
    return credit;
}
/// Compose request admission with the cache's existing loader admission. The
/// shared immutable item keeps its original allocator and cache reservation;
/// only this loading interval is charged to the initiating request.
pub const Loading = struct {
    owner: *Self,
    item: *@import("lake_decoded_cache.zig").Item,
    previous: @FieldType(Budget, "admission"),
    failed: bool = false,
    pub fn begin(self: *Loading, owner: *Self, item: *@import("lake_decoded_cache.zig").Item) !void {
        self.* = .{ .owner = owner, .item = item, .previous = item.budget.admission };
        if (!reserveResident(owner, item.budget.live)) return error.ParquetPageTooLarge;
        item.budget.admission = .{ .ptr = self, .reserve = reserve, .release = refund };
    }
    pub fn end(self: *Loading) void {
        releaseResident(self.owner, self.item.budget.live);
        self.item.budget.admission = self.previous;
        // Request rejection is final, never a retry through the cache's global
        // exclusive loader (which could otherwise bypass this request limit).
        if (self.failed) self.item.budget.admission_exhausted = false;
    }
    fn reserve(raw: *anyopaque, bytes: usize) bool {
        const self: *Loading = @ptrCast(@alignCast(raw));
        if (self.previous) |p| if (!p.reserve(p.ptr, bytes)) return false;
        if (!reserveResident(self.owner, bytes)) {
            if (self.previous) |p| p.release(p.ptr, bytes);
            self.failed = true;
            return false;
        }
        return true;
    }
    fn refund(raw: *anyopaque, bytes: usize) void {
        const self: *Loading = @ptrCast(@alignCast(raw));
        releaseResident(self.owner, bytes);
        if (self.previous) |p| p.release(p.ptr, bytes);
    }
};

/// Optional phases never wait while another parked phase owns their input
/// capacity. Required readers retain reserveInput's cooperative wait policy.
pub fn tryReserveInput(self: *Self, len: usize, context: Context) !bool {
    try context.ensureActive();
    if (len > self.input_limit) return error.ParquetPageTooLarge;
    if (context.io) |io| {
        try self.input_mutex.lock(io);
        defer self.input_mutex.unlock(io);
        try context.ensureActive();
        if (len > self.input_limit -| self.input) return false;
        self.input += len;
        return true;
    }
    if (len > self.input_limit -| self.input) return false;
    self.input += len;
    return true;
}

pub fn reserveInput(self: *Self, len: usize, context: Context) !void {
    try context.ensureActive();
    if (len > self.input_limit) return error.ParquetPageTooLarge;
    if (context.io) |io| {
        try self.input_mutex.lock(io);
        defer self.input_mutex.unlock(io);
        while (len > self.input_limit -| self.input) {
            try context.ensureActive();
            self.input_changed.waitTimeout(io, &self.input_mutex, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
        try context.ensureActive();
        self.input += len;
    } else {
        // Without an executor the cursor advances columns sequentially.
        if (len > self.input_limit -| self.input) return error.ParquetPageTooLarge;
        self.input += len;
    }
}
pub fn releaseInput(self: *Self, len: usize, context: Context) void {
    if (context.io) |io| {
        self.input_mutex.lockUncancelable(io);
        defer self.input_mutex.unlock(io);
        std.debug.assert(len <= self.input);
        self.input -= len;
        self.input_changed.broadcast(io);
    } else self.input -= len;
}
pub fn wrapInput(self: *Self, original: RangeLease, len: usize, context: Context) !RangeLease {
    const Owner = struct {
        state: *Self,
        original: RangeLease,
        len: usize,
        context: Context,
        fn release(raw: *anyopaque) void {
            const value: *@This() = @ptrCast(@alignCast(raw));
            const state = value.state;
            value.original.release();
            state.releaseInput(value.len, value.context);
            state.backing.allocator().destroy(value);
            state.release();
        }
    };
    const owner = try self.backing.allocator().create(Owner);
    owner.* = .{ .state = self.retain(), .original = original, .len = len, .context = context };
    return .{ .bytes = original.bytes, .owner = .{ .shared = .{ .ptr = owner, .release_fn = Owner.release } } };
}
