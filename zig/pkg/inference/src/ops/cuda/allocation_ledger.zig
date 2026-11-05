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
const Owner = struct {
    id: u64 = 0,
    primary: usize = 0,
    device: c_int = 0,
    retained: bool = false,
    release_uncertain: bool = false,
    library: ?std.DynLib = null,
};

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
    owners: std.ArrayListUnmanaged(Owner) = .empty,
    free_head: ?usize = null,
    next_generation: u64 = 1,
    next_owner: u64 = 1,

    fn lock(self: *Ledger) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn deinit(self: *Ledger) void {
        for (self.records.items) |record| std.debug.assert(!record.live);
        for (self.owners.items) |owner| std.debug.assert(!owner.retained and owner.library == null);
        self.owners.deinit(self.allocator);
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
        if (owner.* == 0) {
            var slot: ?*Owner = null;
            for (self.owners.items) |*entry| if (entry.id == 0) {
                slot = entry;
                break;
            };
            if (slot == null) {
                try self.owners.append(self.allocator, .{});
                slot = &self.owners.items[self.owners.items.len - 1];
            }
            owner.* = self.next_owner;
            self.next_owner += 1;
            slot.?.* = .{ .id = owner.*, .primary = primary };
        }
        const index = if (self.free_head) |index| blk: {
            self.free_head = self.records.items[index].next_free;
            break :blk index;
        } else blk: {
            try self.records.append(self.allocator, .{});
            break :blk self.records.items.len - 1;
        };
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

    fn hasOwnerAllocations(self: *Ledger, owner: u64) bool {
        for (self.records.items) |record| if (record.live and record.owner == owner) return true;
        return false;
    }

    fn retainedOwner(self: *Ledger, owner: u64) bool {
        for (self.owners.items) |entry| if (entry.id == owner) return entry.retained;
        return false;
    }

    /// Transfer the retiring wrapper's existing CUDA retain to the ledger.
    /// No fallible allocation/retain is needed on this exceptional path: owner
    /// metadata was reserved before the first device allocation. The caller
    /// MUST skip cuDevicePrimaryCtxRelease when this returns true.
    pub fn retainPrimaryReference(self: *Ledger, owner: u64, device: c_int) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.owners.items) |*entry| if (entry.id != 0 and entry.id == owner) {
            if (!self.hasOwnerAllocations(owner)) {
                entry.* = .{};
                return false;
            }
            entry.retained = true;
            entry.device = device;
            return true;
        };
        return false;
    }

    /// Keep the driver library loaded for the transferred lifecycle reference.
    /// releaseResources may precede host cleanup; transfer the library only
    /// at the wrapper's final deinit, if quarantine still owns its reference.
    pub fn retainDriverLibrary(self: *Ledger, owner: u64, library: std.DynLib) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.owners.items) |*entry| if (entry.id != 0 and entry.id == owner and entry.retained) {
            std.debug.assert(entry.library == null);
            entry.library = library;
            return true;
        };
        return false;
    }

    /// Caller holds lifecycle serialization and its own live primary retain.
    /// Raw handles authorize retries ONLY for that retiring caller or an owner
    /// whose retain was transferred to quarantine without a lifetime gap.
    pub fn retryRetired(self: *Ledger, primary: usize, current_owner: u64, free: *const fn (usize) callconv(.c) c_int, release_primary: *const fn (c_int) callconv(.c) c_int) void {
        self.lock();
        defer self.mutex.unlock();
        for (0..self.records.items.len) |index| {
            const record = self.records.items[index];
            if (!record.live or !record.retired or !record.retryable or record.primary != primary or record.ptr == 0) continue;
            if (record.owner != current_owner and !self.retainedOwner(record.owner)) continue;
            if (free(record.ptr) == 0) _ = self.releaseUnlocked(index);
        }
        for (self.owners.items) |*owner| {
            if (!owner.retained or owner.release_uncertain or owner.primary != primary or self.hasOwnerAllocations(owner.id)) continue;
            // All device bytes are gone and the caller still holds its retain.
            // An error may report earlier asynchronous work; do not assume that
            // another release is safe. Quarantine the library until worker exit
            // rather than risk releasing a successor lifetime's reference.
            if (release_primary(owner.device) != 0) {
                owner.release_uncertain = true;
                continue;
            }
            if (owner.library) |*library| library.close();
            owner.* = .{};
        }
    }

    /// Only a successful driver state query after release proving that this
    /// primary context is inactive can authorize reconciliation of all owners.
    pub fn confirmedPrimaryTeardown(self: *Ledger, primary: usize) void {
        self.lock();
        defer self.mutex.unlock();
        // Fail closed in optimized builds too: an inactive-state observation
        // cannot authorize forgetting an owner whose retain is still held or
        // whose release outcome was uncertain.
        for (self.owners.items) |owner| if (owner.primary == primary and owner.retained) return;
        for (self.owners.items) |*owner| if (owner.primary == primary) {
            owner.* = .{};
        };
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
        fn release(_: c_int) callconv(.c) c_int {
            return 0;
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
    ledger.retryRetired(7, owner, Free.free, Free.release);
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    Free.success = true;
    ledger.retryRetired(8, owner, Free.free, Free.release);
    try std.testing.expectEqual(@as(usize, 24), budget.snapshot().live);
    ledger.retryRetired(7, owner, Free.free, Free.release);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    const rollback = try ledger.reserve(7, &owner, 32);
    ledger.release(rollback);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
}

test "CUDA quarantine requires a continuous retain and releases it only after every owned allocation" {
    const Mock = struct {
        var free_calls: usize = 0;
        var releases: usize = 0;
        var fail_release = true;
        fn free(_: usize) callconv(.c) c_int {
            free_calls += 1;
            return 0;
        }
        fn release(_: c_int) callconv(.c) c_int {
            releases += 1;
            return if (fail_release) 1 else 0;
        }
    };
    Mock.free_calls = 0;
    Mock.releases = 0;
    Mock.fail_release = true;
    var budget = accounting.Budget{ .limit = 64 };
    var ledger = Ledger{ .allocator = std.testing.allocator, .budget = &budget };
    defer ledger.deinit();
    var owner: u64 = 0;
    const first = try ledger.reserve(7, &owner, 24);
    const second = try ledger.reserve(7, &owner, 16);
    ledger.publish(first, 123);
    ledger.publish(second, 456);
    ledger.retire(owner, false);
    // Matching a handle by itself must never authorize a cross-owner retry.
    ledger.retryRetired(7, 0, Mock.free, Mock.release);
    try std.testing.expectEqual(@as(usize, 0), Mock.free_calls);
    try std.testing.expect(ledger.retainPrimaryReference(owner, 0));
    ledger.confirmedPrimaryTeardown(7);
    ledger.retryRetired(7, 0, Mock.free, Mock.release);
    // An unsuccessful stream drain remains quarantined, even with a pin.
    try std.testing.expectEqual(@as(usize, 40), budget.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), Mock.free_calls);
    // Test recovery supplies a confirmed drain; production never promotes a
    // failed drain and requires worker restart if it cannot establish one.
    ledger.retire(owner, true);
    ledger.retryRetired(8, 0, Mock.free, Mock.release);
    try std.testing.expectEqual(@as(usize, 0), Mock.free_calls);
    ledger.retryRetired(7, 0, Mock.free, Mock.release);
    try std.testing.expectEqual(@as(usize, 2), Mock.free_calls);
    try std.testing.expectEqual(@as(usize, 0), budget.snapshot().live);
    try std.testing.expect(ledger.retainedOwner(owner));
    Mock.fail_release = false;
    ledger.retryRetired(7, 0, Mock.free, Mock.release);
    try std.testing.expect(ledger.retainedOwner(owner));
    try std.testing.expectEqual(@as(usize, 1), Mock.releases);
    // Worker exit is the only recovery for an ambiguous reference release.
    // This local mock owns no real CUDA retain/library; discard its metadata
    // to simulate exit without calling release a second time.
    for (ledger.owners.items) |*entry| entry.* = .{};
}
