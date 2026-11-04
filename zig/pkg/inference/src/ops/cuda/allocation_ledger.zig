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

//! Allocation ownership survives a failed free and a retiring model wrapper.
//! Primary-context release alone is not evidence that physical bytes are gone.
const std = @import("std");
const accounting = @import("allocation_budget.zig");

pub var process: Ledger = .{ .allocator = std.heap.smp_allocator, .budget = &accounting.process_device_allocations };

pub const Token = struct { index: usize, generation: u64 };
const Record = struct {
    generation: u64 = 0,
    live: bool = false,
    primary: usize = 0,
    owner: u64 = 0,
    ptr: usize = 0,
    bytes: usize = 0,
    failed: bool = false,
    retired: bool = false,
    retryable: bool = false,
    next_free: ?usize = null,
};

pub const Ledger = struct {
    allocator: std.mem.Allocator,
    budget: *accounting.Budget,
    mutex: std.atomic.Mutex = .unlocked,
    records: std.ArrayListUnmanaged(Record) = .empty,
    free_head: ?usize = null,
    next_generation: u64 = 1,
    next_owner: u64 = 1,

    fn lock(self: *Ledger) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn deinit(self: *Ledger) void {
        for (self.records.items) |record| std.debug.assert(!record.live);
        self.records.deinit(self.allocator);
    }

    /// Reserve metadata and bytes before cuMemAlloc; host allocation failure
    /// cannot leave a real device pointer with no durable owner.
    pub fn reserve(self: *Ledger, primary: usize, owner: *u64, bytes: usize) !Token {
        self.lock();
        defer self.mutex.unlock();
        try self.budget.reserve(bytes);
        errdefer self.budget.release(bytes);
        if (self.next_generation == std.math.maxInt(u64) or self.next_owner == std.math.maxInt(u64)) return error.CudaMemoryLimitExceeded;
        const index = if (self.free_head) |index| blk: {
            self.free_head = self.records.items[index].next_free;
            break :blk index;
        } else blk: {
            try self.records.append(self.allocator, .{});
            break :blk self.records.items.len - 1;
        };
        if (owner.* == 0) {
            owner.* = self.next_owner;
            self.next_owner += 1;
        }
        const generation = self.next_generation;
        self.next_generation += 1;
        self.records.items[index] = .{ .live = true, .generation = generation, .primary = primary, .owner = owner.*, .bytes = bytes };
        return .{ .index = index, .generation = generation };
    }

    fn get(self: *Ledger, token: Token) *Record {
        const record = &self.records.items[token.index];
        std.debug.assert(record.live and record.generation == token.generation);
        return record;
    }

    /// A descriptor may outlive confirmed teardown. Its generation cannot
    /// free an allocation that reused this ledger slot or device address.
    pub fn isLive(self: *Ledger, token: Token) bool {
        self.lock();
        defer self.mutex.unlock();
        if (token.index >= self.records.items.len) return false;
        const record = self.records.items[token.index];
        return record.live and record.generation == token.generation;
    }

    pub fn publish(self: *Ledger, token: Token, ptr: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(ptr != 0);
        self.get(token).ptr = ptr;
    }

    fn releaseUnlocked(self: *Ledger, index: usize) usize {
        const record = &self.records.items[index];
        const bytes = record.bytes;
        self.budget.release(bytes);
        record.live = false;
        record.next_free = self.free_head;
        self.free_head = index;
        return bytes;
    }

    /// Used only after failed allocation rollback or a confirmed cuMemFree.
    pub fn release(self: *Ledger, token: Token) void {
        self.lock();
        defer self.mutex.unlock();
        _ = self.get(token);
        _ = self.releaseUnlocked(token.index);
    }

    pub fn failedFree(self: *Ledger, token: Token) void {
        self.lock();
        defer self.mutex.unlock();
        self.get(token).failed = true;
    }

    pub fn retire(self: *Ledger, owner: u64, safe_to_retry: bool) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.records.items) |*record| if (record.live and record.owner == owner) {
            record.retired = true;
            record.retryable = safe_to_retry;
        };
    }

    /// Caller owns primary-context lifecycle serialization and has made it
    /// current. Retired owners have drained their streams before retry.
    pub fn retryRetired(self: *Ledger, primary: usize, free: *const fn (usize) callconv(.c) c_int) void {
        self.lock();
        defer self.mutex.unlock();
        for (0..self.records.items.len) |index| {
            const record = self.records.items[index];
            if (!record.live or !record.retired or !record.retryable or record.primary != primary or record.ptr == 0) continue;
            if (free(record.ptr) == 0) _ = self.releaseUnlocked(index);
        }
    }

    /// Only a successful driver state query after release proving that this
    /// primary context is inactive can authorize reconciliation of all owners.
    pub fn confirmedPrimaryTeardown(self: *Ledger, primary: usize) void {
        self.lock();
        defer self.mutex.unlock();
        for (0..self.records.items.len) |index| {
            const record = self.records.items[index];
            if (record.live and record.primary == primary) _ = self.releaseUnlocked(index);
        }
    }

    pub fn outstanding(self: *Ledger) usize {
        self.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.records.items) |record| count += @intFromBool(record.live);
        return count;
    }
};

test "CUDA allocation ownership survives free failure until confirmed primary teardown and reload" {
    var budget = accounting.Budget{ .limit = 32 };
    var ledger = Ledger{ .allocator = std.testing.allocator, .budget = &budget };
    defer ledger.deinit();
    var owner: u64 = 0;
    const token = try ledger.reserve(7, &owner, 24);
    ledger.publish(token, 123);
    ledger.failedFree(token);
    ledger.retire(owner, false);
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, ledger.reserve(7, &owner, 16));
    // Other primary-context retainers keep the allocation charged.
    try std.testing.expectEqual(@as(usize, 1), ledger.outstanding());
    ledger.confirmedPrimaryTeardown(7);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    var reloaded: u64 = 0;
    const next = try ledger.reserve(9, &reloaded, 32);
    ledger.publish(next, 456);
    ledger.release(next);
}

test "CUDA retired allocation retries release only physically confirmed frees" {
    const Free = struct {
        var success = false;
        fn free(_: usize) callconv(.c) c_int {
            return if (success) 0 else 1;
        }
    };
    var budget = accounting.Budget{ .limit = 64 };
    var ledger = Ledger{ .allocator = std.testing.allocator, .budget = &budget };
    defer ledger.deinit();
    var owner: u64 = 0;
    const failed = try ledger.reserve(7, &owner, 24);
    ledger.publish(failed, 123);
    ledger.failedFree(failed);
    ledger.retire(owner, true);
    Free.success = false;
    ledger.retryRetired(7, Free.free);
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    Free.success = true;
    ledger.retryRetired(8, Free.free);
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    ledger.retryRetired(7, Free.free);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    const rollback = try ledger.reserve(7, &owner, 32);
    ledger.release(rollback);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
}
