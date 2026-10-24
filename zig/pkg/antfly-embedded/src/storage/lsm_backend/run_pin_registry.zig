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

//! Retained path-pin ownership. Hashing/collision comparison/copy/retirement
//! yield outside the mutex. Its critical sections traverse at most 64 hash
//! branches and publish only fixed headers. Caller prepays the Operation header
//! and its planned additional bytes, registers retirement, and pins input and
//! registry through completion. Physical byte metrics include private and
//! detached ownership; global quota/parent-ledger integration is separate.
const std = @import("std");
const platform = @import("antfly_platform");
const admission = @import("../../common/read_admission.zig");
const Counter = platform.atomic.Value(u64);
const quota = @import("pin_quota.zig");
const resources = @import("../resource_manager.zig");
pub const quantum = 16 * 1024;
var default_epoch: Counter = .init(0);
var single_allocator: std.heap.DebugAllocator(.{ .thread_safe = false }) = .init;
const default_allocator = if (@import("builtin").single_threaded) single_allocator.allocator() else std.heap.smp_allocator;

const Record = struct {
    refs: Counter = .init(0), // File owners; release never takes the registry lock.
    index_refs: Counter = .init(1), // Index or provisional owner, plus cursor borrows.
    registry: *Registry,
    path_len: usize,
    group: ?*Group = null,
    previous: ?*Record = null,
    next: ?*Record = null,
    registered: bool = false, // Registry-mutex protected.
    quota_owner: ?*quota.Owner = null,
    fn allocate(registry: *Registry, length: usize) !*Record {
        const bytes = try std.math.add(usize, @sizeOf(Record), length);
        const raw = registry.allocator.rawAlloc(bytes, .of(Record), @returnAddress()) orelse return error.OutOfMemory;
        const pin: *Record = @ptrCast(@alignCast(raw));
        pin.* = .{ .registry = registry, .path_len = length };
        _ = registry.live_bytes.fetchAdd(bytes, .monotonic);
        return pin;
    }
    pub fn path(self: *Record) []u8 {
        return @as([*]u8, @ptrCast(self))[@sizeOf(Record)..][0..self.path_len];
    }
    fn borrow(self: *Record) *Record {
        _ = self.index_refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Record) void {
        const epoch = self.registry.epoch;
        const previous = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        // Concurrent forget can already have retired self. Only saved values
        // and the process/registry-lifetime epoch may be used after this drop.
        if (previous == 1) _ = epoch.fetchAdd(1, .release);
    }
    const Retirement = struct {
        registry: ?*Registry = null,
        bytes: ?[]u8 = null,
        offset: usize = 0,
        quota_owner: ?*quota.Owner = null,
        fn init(pin: *Record) @This() {
            const registry = pin.registry;
            const funding = pin.quota_owner;
            const bytes = @as([*]u8, @ptrCast(pin))[0 .. @sizeOf(Record) + pin.path_len];
            if (pin.index_refs.fetchSub(1, .acq_rel) != 1) return .{};
            std.debug.assert(!pin.registered and pin.refs.load(.acquire) == 0);
            return .{ .registry = registry, .bytes = bytes, .quota_owner = funding };
        }
        fn step(self: *@This(), original: ?admission.Port) !void {
            if (self.bytes) |bytes| {
                while (self.offset < bytes.len) {
                    try checkpoint(original);
                    const end = @min(bytes.len, self.offset + quantum);
                    @memset(bytes[self.offset..end], undefined);
                    self.offset = end;
                }
                try checkpoint(original);
                const registry = self.registry.?;
                registry.allocator.rawFree(bytes, .of(Record), @returnAddress());
                _ = registry.live_bytes.fetchSub(bytes.len, .release);
                if (self.quota_owner) |owner| owner.reportFreed(bytes.len);
                self.bytes = null;
            }
        }
    };
};
const Group = struct {
    registry: *Registry,
    refs: Counter = .init(1),
    hash: u64,
    generation: u64 = 0,
    head: ?*Record = null,
    registered: bool = false,
    quota_owner: ?*quota.Owner = null,
    fn borrow(self: *Group) *Group {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *Group) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(!self.registered and self.head == null);
        const registry = self.registry;
        const funding = self.quota_owner;
        registry.allocator.destroy(self);
        _ = registry.live_bytes.fetchSub(@sizeOf(Group), .release);
        if (funding) |owner| owner.reportFreed(@sizeOf(Group));
    }
};
const Link = union(enum) { group: *Group, branch: *Branch };
const Branch = struct { bit: u6 = 0, children: [2]?Link = @splat(null), quota_owner: ?*quota.Owner = null };
fn side(hash: u64, bit: u6) usize {
    return @intCast((hash >> (63 - bit)) & 1);
}
fn checkpoint(original: ?admission.Port) !void {
    if (original) |port| try port.checkpoint();
}

pub const Registry = struct {
    pub const Pin = Record;
    mutex: std.atomic.Mutex = .unlocked,
    allocator: std.mem.Allocator = default_allocator,
    epoch: *Counter = &default_epoch,
    root: ?Link = null,
    key_count: usize = 0,
    branch_count: usize = 0,
    live_bytes: Counter = .init(0),
    // Immutable after first use. Lower widths force collisions in qualification;
    // zero still supports every path through retained collision-list traversal.
    hash_bits: u7 = 64,
    fn lock(self: *Registry, original: ?admission.Port) !void {
        if (original) |port| try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, port) else platform.sync.lockYielding(&self.mutex);
    }
    fn leaf(self: *Registry, hash: u64) ?*Group {
        var link = self.root orelse return null;
        var visited: usize = 0;
        while (true) switch (link) {
            .group => |group| return group,
            .branch => |branch| {
                visited += 1;
                std.debug.assert(visited <= 64);
                link = branch.children[side(hash, branch.bit)].?;
            },
        };
    }
    fn findGroup(self: *Registry, hash: u64) ?*Group {
        const group = self.leaf(hash) orelse return null;
        return if (group.hash == hash) group else null;
    }
    fn insertGroup(self: *Registry, group: *Group, spare: *?*Branch) void {
        const other = self.leaf(group.hash) orelse {
            self.root = .{ .group = group };
            group.registered = true;
            return;
        };
        std.debug.assert(other.hash != group.hash);
        const bit: u6 = @intCast(@clz(group.hash ^ other.hash));
        var slot = &self.root;
        while (slot.*) |link| {
            if (std.meta.activeTag(link) == .group or link.branch.bit >= bit) break;
            slot = &link.branch.children[side(group.hash, link.branch.bit)];
        }
        const branch = spare.*.?;
        branch.bit = bit;
        const index = side(group.hash, bit);
        branch.children[index] = .{ .group = group };
        branch.children[1 - index] = slot.*;
        slot.* = .{ .branch = branch };
        spare.* = null;
        self.branch_count += 1;
        group.registered = true;
    }
    fn detachGroup(self: *Registry, group: *Group) ?*Branch {
        var slot = &self.root;
        var parent_slot: ?*?Link = null;
        var index: usize = 0;
        while (std.meta.activeTag(slot.*.?) == .branch) {
            parent_slot = slot;
            const branch = slot.*.?.branch;
            index = side(group.hash, branch.bit);
            slot = &branch.children[index];
        }
        std.debug.assert(slot.*.?.group == group and group.head == null);
        group.registered = false;
        if (parent_slot) |parent| {
            const branch = parent.*.?.branch;
            parent.* = branch.children[1 - index];
            self.branch_count -= 1;
            return branch;
        }
        self.root = null;
        return null;
    }
    /// Joined diagnostic only; caller already excludes index mutation. Ordinary
    /// lookups use Operation and never compare path bytes under the mutex.
    pub fn findLockedJoined(self: *Registry, path: []const u8) ?*Pin {
        var hash = std.hash.Wyhash.hash(0, path);
        if (self.hash_bits < 64) hash &= (@as(u64, 1) << @intCast(self.hash_bits)) - 1;
        const group = self.findGroup(hash) orelse return null;
        var pin = group.head;
        while (pin) |record| : (pin = record.next) if (std.mem.eql(u8, path, record.path())) return record;
        return null;
    }
    fn joined(self: *Registry, path: []const u8, mode: Mode) !?*Pin {
        while (true) {
            var operation = Operation.init(self, path, mode);
            defer operation.deinit();
            _ = operation.planWithAdmission(null) catch |err| {
                if (err == error.CompactionPlanningStale) continue;
                return err;
            };
            try operation.resumeWithAdmission(null);
            operation.publishWithAdmission(null) catch |err| {
                if (err == error.CompactionPlanningStale) continue;
                return err;
            };
            return if (mode == .retain) try operation.takePinWithAdmission(null) else if (operation.present) operation.current else null;
        }
    }
    pub fn retainPin(self: *Registry, path: []const u8) !*Pin {
        return (try self.joined(path, .retain)).?;
    }
    pub fn retain(self: *Registry, path: []const u8) !void {
        _ = try self.retainPin(path);
    }
    pub fn release(self: *Registry, path: []const u8) void {
        _ = self.joined(path, .release) catch unreachable;
    }
    pub fn isRetained(self: *Registry, path: []const u8) bool {
        return (self.joined(path, .query) catch unreachable) != null;
    }
    pub fn forget(self: *Registry, path: []const u8) void {
        _ = self.joined(path, .forget) catch unreachable;
    }
    /// Exclusive joined teardown; all operations and file owners have drained.
    pub fn deinitJoined(self: *Registry) void {
        while (self.leaf(0)) |group| {
            const pin = group.head.?;
            std.debug.assert(pin.refs.load(.acquire) == 0);
            self.forget(pin.path());
        }
        std.debug.assert(self.key_count == 0 and self.branch_count == 0 and self.live_bytes.load(.acquire) == 0);
    }
};

pub const Mode = enum { retain, release, query, forget };
/// Stable address required while active. No original port is stored. The
/// caller registers every partial owner and reserves plan's bound before resume.
pub const Operation = struct {
    registry: *Registry,
    path: []const u8,
    mode: Mode,
    hasher: std.hash.Wyhash = .init(0),
    hash_offset: usize = 0,
    hash: ?u64 = null,
    captured: bool = false,
    group: ?*Group = null,
    generation: u64 = 0,
    current: ?*Record = null,
    compare_offset: usize = 0,
    pending_drop: ?Record.Retirement = null,
    found: bool = false,
    bound: ?u64 = null,
    funder: ?*quota.Owner = null,
    credit: ?resources.ConstructionCredit = null,
    adopted: bool = false,
    new_pin: ?*Record = null,
    copy_offset: usize = 0,
    new_group: ?*Group = null,
    new_branch: ?*Branch = null,
    detached_pin: ?*Record = null,
    detached_group: ?*Group = null,
    detached_branch: ?*Branch = null,
    result: ?*Record = null,
    present: bool = false,
    ready: bool = false,
    committed: bool = false,
    taken: bool = false,
    retiring: bool = false,
    retired: bool = false,
    pub fn init(registry: *Registry, path: []const u8, mode: Mode) @This() {
        return .{ .registry = registry, .path = path, .mode = mode };
    }
    pub fn planWithAdmission(self: *@This(), original: ?admission.Port) !u64 {
        try checkpoint(original);
        if (self.retiring or self.retired or self.committed) return error.InvalidArgument;
        if (self.bound) |bound| return bound;
        if (self.registry.hash_bits > 64) return error.InvalidArgument;
        if (self.hash == null) {
            while (self.hash_offset < self.path.len) {
                try checkpoint(original);
                const end = @min(self.path.len, self.hash_offset + quantum);
                self.hasher.update(self.path[self.hash_offset..end]);
                self.hash_offset = end;
            }
            var hash = self.hasher.final();
            if (self.registry.hash_bits < 64) hash &= (@as(u64, 1) << @intCast(self.registry.hash_bits)) - 1;
            self.hash = hash;
        }
        if (!self.captured) {
            try self.registry.lock(original);
            defer self.registry.mutex.unlock();
            if (self.registry.findGroup(self.hash.?)) |group| {
                self.group = group.borrow();
                self.generation = group.generation;
                if (group.head) |head| self.current = head.borrow();
            }
            self.captured = true;
        }
        while (self.current) |pin| {
            if (self.pending_drop) |*drop| {
                try drop.step(original);
                self.pending_drop = null;
            }
            const key = pin.path();
            const length = @min(key.len, self.path.len);
            var mismatch = false;
            while (self.compare_offset < length) {
                try checkpoint(original);
                const end = @min(length, self.compare_offset + quantum);
                if (!std.mem.eql(u8, key[self.compare_offset..end], self.path[self.compare_offset..end])) {
                    mismatch = true;
                    break;
                }
                self.compare_offset = end;
            }
            if (!mismatch and key.len == self.path.len) {
                self.found = true;
                break;
            }
            {
                try self.registry.lock(original);
                defer self.registry.mutex.unlock();
                if (!self.group.?.registered or self.group.?.generation != self.generation) return error.CompactionPlanningStale;
                self.current = if (pin.next) |next| next.borrow() else null;
            }
            self.pending_drop = .init(pin);
            self.compare_offset = 0;
        }
        if (self.pending_drop) |*drop| {
            try drop.step(original);
            self.pending_drop = null;
        }
        var bytes: u64 = 0;
        if (self.mode == .retain and !self.found) {
            bytes = try std.math.add(u64, @sizeOf(Record), self.path.len);
            if (self.group == null) bytes = try std.math.add(u64, bytes, @sizeOf(Group) + @sizeOf(Branch));
        }
        self.bound = bytes;
        return bytes;
    }
    /// Retains the quota authority before reserving construction. It cannot
    /// close while this builder or any transferred allocation remains alive.
    pub fn resumeFundedWithAdmission(self: *@This(), owner: *quota.Owner, original: admission.Port) !void {
        try original.checkpoint();
        if (self.retiring or self.retired or self.committed) return error.InvalidArgument;
        const bytes = try self.planWithAdmission(original);
        if (bytes != 0) {
            if (self.funder == null) self.funder = try owner.acquireWithAdmission(original);
            if (self.funder.? != owner) return error.InvalidArgument;
            if (self.credit == null and !self.adopted) self.credit = try owner.manager.reserveConstructionWithAdmission(quota.slice, bytes, original);
        }
        try self.resumeWithAdmission(original);
    }
    pub fn publishFundedWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.retiring or self.retired or self.committed or !self.ready) return error.InvalidArgument;
        if (self.bound.? != 0 and !self.adopted) {
            const owner = self.funder orelse return error.ResourceBudgetExceeded;
            var bytes: u64 = 0;
            if (self.new_pin) |pin| bytes += @sizeOf(Record) + pin.path_len;
            if (self.new_group != null) bytes += @sizeOf(Group);
            if (self.new_branch != null) bytes += @sizeOf(Branch);
            try owner.adoptWithAdmission(&self.credit.?, bytes, original);
            // No fallible work between adoption and allocation provenance.
            if (self.new_pin) |pin| pin.quota_owner = owner.retainAllocation();
            if (self.new_group) |group| group.quota_owner = owner.retainAllocation();
            if (self.new_branch) |branch| branch.quota_owner = owner.retainAllocation();
            self.adopted = true;
            self.credit.?.retire();
            self.credit = null;
        }
        try self.publishWithAdmission(original);
    }
    pub fn resumeWithAdmission(self: *@This(), original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retiring or self.retired or self.committed) return error.InvalidArgument;
        if (self.bound == null or (self.funder != null and self.credit == null and !self.adopted)) return error.ResourceBudgetExceeded;
        if (self.ready) return;
        if (self.mode == .retain and !self.found) {
            if (self.new_pin == null) {
                try checkpoint(original);
                self.new_pin = try Record.allocate(self.registry, self.path.len);
            }
            while (self.copy_offset < self.path.len) {
                try checkpoint(original);
                const end = @min(self.path.len, self.copy_offset + quantum);
                @memcpy(self.new_pin.?.path()[self.copy_offset..end], self.path[self.copy_offset..end]);
                self.copy_offset = end;
            }
            if (self.group == null) {
                if (self.new_group == null) {
                    try checkpoint(original);
                    const group = try self.registry.allocator.create(Group);
                    group.* = .{ .registry = self.registry, .hash = self.hash.? };
                    _ = self.registry.live_bytes.fetchAdd(@sizeOf(Group), .monotonic);
                    self.new_group = group;
                }
                if (self.new_branch == null) {
                    try checkpoint(original);
                    const branch = try self.registry.allocator.create(Branch);
                    branch.* = .{};
                    _ = self.registry.live_bytes.fetchAdd(@sizeOf(Branch), .monotonic);
                    self.new_branch = branch;
                }
            }
        }
        try checkpoint(original);
        self.ready = true;
    }
    pub fn publishWithAdmission(self: *@This(), original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retiring or self.retired or self.committed or !self.ready) return error.InvalidArgument;
        if (self.funder != null and !self.adopted) return error.ResourceBudgetExceeded;
        while (true) {
            try self.registry.lock(original);
            {
                defer self.registry.mutex.unlock();
                if (self.found) {
                    const pin = self.current.?;
                    if (!pin.registered) return error.CompactionPlanningStale;
                    const count = pin.refs.load(.acquire);
                    switch (self.mode) {
                        .retain => {
                            if (count == std.math.maxInt(u64)) return error.Overflow;
                            // Other retains require this mutex. Direct drops
                            // can only reduce the count checked above.
                            _ = pin.refs.fetchAdd(1, .acq_rel);
                            self.result = pin;
                        },
                        .query => self.present = count != 0,
                        .release => if (count != 0) {
                            if (pin.refs.cmpxchgStrong(count, count - 1, .acq_rel, .acquire) != null) continue;
                            if (count == 1) _ = self.registry.epoch.fetchAdd(1, .release);
                        },
                        .forget => if (count == 0) {
                            const group = pin.group.?;
                            if (group.generation == std.math.maxInt(u64)) return error.Overflow;
                            if (pin.previous) |previous| previous.next = pin.next else group.head = pin.next;
                            if (pin.next) |next| next.previous = pin.previous;
                            pin.registered = false;
                            group.generation += 1;
                            self.registry.key_count -= 1;
                            self.detached_pin = pin; // Transfer list ownership.
                            if (group.head == null) {
                                self.detached_branch = self.registry.detachGroup(group);
                                self.detached_group = group; // Transfer root ownership.
                            }
                        },
                    }
                } else {
                    if (self.group) |group| {
                        if (!group.registered or group.generation != self.generation) return error.CompactionPlanningStale;
                    } else if (self.registry.findGroup(self.hash.?) != null) return error.CompactionPlanningStale;
                    if (self.mode == .retain) {
                        const group = self.group orelse self.new_group.?;
                        if (group.generation == std.math.maxInt(u64)) return error.Overflow;
                        const pin = self.new_pin.?;
                        pin.group = group;
                        pin.next = group.head;
                        if (group.head) |head| head.previous = pin;
                        group.head = pin;
                        group.generation += 1;
                        pin.registered = true;
                        pin.refs.store(1, .release);
                        if (self.group == null) {
                            self.registry.insertGroup(group, &self.new_branch);
                            self.new_group = null;
                        }
                        self.new_pin = null;
                        self.registry.key_count += 1;
                        self.result = pin;
                    }
                }
                self.committed = true;
                return;
            }
        }
    }
    pub fn takePinWithAdmission(self: *@This(), original: ?admission.Port) !*Registry.Pin {
        try checkpoint(original);
        if (!self.committed or self.mode != .retain or self.taken or self.retiring or self.retired) return error.InvalidArgument;
        const out = self.result.?;
        self.result = null;
        self.taken = true;
        return out;
    }
    fn retirePin(self: *@This(), slot: *?*Record, original: ?admission.Port) !void {
        if (slot.*) |pin| {
            self.pending_drop = .init(pin);
            slot.* = null;
        }
        if (self.pending_drop) |*drop| {
            try drop.step(original);
            self.pending_drop = null;
        }
    }
    fn freeBranch(self: *@This(), slot: *?*Branch, original: ?admission.Port) !void {
        if (slot.*) |branch| {
            try checkpoint(original);
            const funding = branch.quota_owner;
            self.registry.allocator.destroy(branch);
            _ = self.registry.live_bytes.fetchSub(@sizeOf(Branch), .release);
            if (funding) |owner| owner.reportFreed(@sizeOf(Branch));
            slot.* = null;
        }
    }
    pub fn retireWithAdmission(self: *@This(), original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retired) return;
        self.retiring = true;
        if (self.result) |pin| {
            try checkpoint(original);
            pin.release();
            self.result = null;
        }
        // A collision step may already own partial physical retirement.
        if (self.pending_drop) |*drop| {
            try drop.step(original);
            self.pending_drop = null;
        }
        try self.retirePin(&self.detached_pin, original);
        try self.retirePin(&self.new_pin, original);
        try self.retirePin(&self.current, original);
        try self.freeBranch(&self.detached_branch, original);
        try self.freeBranch(&self.new_branch, original);
        if (self.detached_group) |group| {
            try checkpoint(original);
            group.release();
            self.detached_group = null;
        }
        if (self.new_group) |group| {
            try checkpoint(original);
            group.release();
            self.new_group = null;
        }
        if (self.group) |group| {
            try checkpoint(original);
            group.release();
            self.group = null;
        }
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        if (self.funder) |owner| {
            if (original) |port| try owner.reconcileWithAdmission(port) else owner.reconcileJoined();
            owner.releaseBorrow();
            self.funder = null;
        }
        self.retired = true;
    }
    pub fn deinit(self: *@This()) void {
        self.retireWithAdmission(null) catch unreachable;
    }
};

const Fault = struct {
    checks: usize = 0,
    at: usize = std.math.maxInt(usize),
    failure: anyerror = error.Canceled,
    registry: ?*Registry = null,
    fn cpu(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.registry) |registry| {
            std.debug.assert(registry.mutex.tryLock());
            registry.mutex.unlock();
        }
        try check(raw);
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.checks;
        self.checks += 1;
        if (at == self.at) return self.failure;
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = cpu, .check_lifetime = check } };
    }
};
const Witness = struct {
    registry: *Registry,
    available: u64 = std.math.maxInt(u64),
    live: usize = 0,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.registry.live_bytes.load(.acquire) == self.live);
        std.debug.assert(self.available >= self.live + len);
        const at = self.calls;
        self.calls += 1;
        if (at == self.fail_at) return null;
        const bytes = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return bytes;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.registry.live_bytes.load(.acquire) == self.live);
        std.debug.assert(self.available >= self.live);
        std.testing.allocator.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
};
const Stage = enum { plan, construct, publish, take, retire };
fn exercise(paths: [3][]const u8, mode: Mode, present: bool, collisions: bool, stage: Stage, target: usize, abandon: bool) !usize {
    var registry: Registry = .{ .hash_bits = if (collisions) 0 else 64 };
    var witness: Witness = .{ .registry = &registry };
    registry.allocator = witness.allocator();
    defer registry.deinitJoined();
    const neighbor = try registry.retainPin(paths[1]);
    neighbor.release();
    var anchor: ?*Record = null;
    if (present) {
        anchor = try registry.retainPin(paths[0]);
        if (mode == .forget) anchor.?.release();
    }
    const baseline = witness.live;
    witness.available = baseline;
    var operation = Operation.init(&registry, if (present) paths[0] else paths[2], mode);
    defer operation.deinit();
    var fault: Fault = .{ .registry = &registry, .failure = switch (target % 3) {
        0 => error.Canceled,
        1 => error.DeadlineExceeded,
        else => error.FileNotFound,
    } };
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    if (stage != .plan) witness.available = baseline + try operation.planWithAdmission(original);
    if (stage == .publish or stage == .take or stage == .retire) try operation.resumeWithAdmission(original);
    if (stage == .take or stage == .retire) try operation.publishWithAdmission(original);
    fault.checks = 0;
    fault.at = target;
    var output: ?*Record = null;
    const result: anyerror!void = switch (stage) {
        .plan => blk: {
            const before = witness.calls;
            const bound = operation.planWithAdmission(original) catch |err| break :blk err;
            try std.testing.expectEqual(before, witness.calls);
            witness.available = baseline + bound;
            break :blk {};
        },
        .construct => operation.resumeWithAdmission(original),
        .publish => operation.publishWithAdmission(original),
        .take => blk: {
            output = operation.takePinWithAdmission(original) catch |err| break :blk err;
            break :blk {};
        },
        .retire => operation.retireWithAdmission(original),
    };
    const checks = fault.checks;
    if (target == std.math.maxInt(usize)) try result else try std.testing.expectError(fault.failure, result);
    if (!abandon and stage != .retire) {
        if (!operation.committed) {
            witness.available = baseline + try operation.planWithAdmission(original);
            try operation.resumeWithAdmission(original);
            try operation.publishWithAdmission(original);
        }
        if (mode == .retain and output == null) output = try operation.takePinWithAdmission(original);
        if (mode == .query) try std.testing.expectEqual(present, operation.present);
    }
    if (output) |pin| {
        try std.testing.expectEqualSlices(u8, operation.path, pin.path());
        pin.release();
    }
    if (abandon and target % 2 == 0) operation.deinit() else try operation.retireWithAdmission(original);
    if (present and mode != .forget and !(mode == .release and operation.committed)) anchor.?.release();
    fault.at = fault.checks;
    try std.testing.expectError(fault.failure, operation.retireWithAdmission(original));
    try operation.retireWithAdmission(original);
    registry.deinitJoined();
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    return checks;
}

test "controlled physical sync retained pin registry preserves every original planning publication take and retirement rejection" {
    const a = std.testing.allocator;
    var paths: [3][]const u8 = undefined;
    var initialized: usize = 0;
    defer for (paths[0..initialized]) |bytes| a.free(bytes);
    for (&paths, 0..) |*path, i| {
        const bytes = try a.alloc(u8, 2 * quantum + 11);
        @memset(bytes, 'k');
        bytes[bytes.len - 1] = @intCast(i);
        path.* = bytes;
        initialized += 1;
    }
    for ([_]Mode{ .retain, .release, .query, .forget }) |mode| {
        for ([_]bool{ false, true }) |present| {
            for ([_]bool{ false, true }) |collisions| {
                for ([_]Stage{ .plan, .construct, .publish, .take, .retire }) |stage| {
                    if (stage == .take and mode != .retain) continue;
                    const checks = try exercise(paths, mode, present, collisions, stage, std.math.maxInt(usize), false);
                    for (0..checks) |at| for ([_]bool{ false, true }) |abandon| {
                        _ = try exercise(paths, mode, present, collisions, stage, at, abandon);
                    };
                }
            }
        }
    }
}

test "controlled physical sync retained pin registry owns OOM partial construction and allocation free removal" {
    const a = std.testing.allocator;
    const path = try a.alloc(u8, 3 * quantum + 3);
    defer a.free(path);
    @memset(path, 'o');
    for ([_]bool{ false, true }) |abandon| {
        for (0..3) |at| {
            var registry: Registry = .{};
            var witness: Witness = .{ .registry = &registry, .available = 0, .fail_at = at };
            registry.allocator = witness.allocator();
            defer registry.deinitJoined();
            var operation = Operation.init(&registry, path, .retain);
            defer operation.deinit();
            var fault: Fault = .{};
            const payload = fault.payload();
            const original = admission.Port.init(&payload);
            witness.available = try operation.planWithAdmission(original);
            try std.testing.expectError(error.OutOfMemory, operation.resumeWithAdmission(original));
            try std.testing.expectEqual(@as(usize, 0), registry.key_count);
            if (!abandon) {
                try operation.resumeWithAdmission(original);
                try operation.publishWithAdmission(original);
                const pin = try operation.takePinWithAdmission(original);
                try std.testing.expectEqual(@intFromPtr(pin) + @sizeOf(Record), @intFromPtr(pin.path().ptr));
                pin.release();
            }
            try operation.retireWithAdmission(original);
            // Deny every allocation from here: query, path release and forget
            // must still reclaim all keys, group and branch headers.
            witness.fail_at = witness.calls;
            try std.testing.expect(!registry.isRetained(path));
            registry.release(path);
            registry.forget(path);
            try std.testing.expectEqual(@as(usize, 0), witness.live);
        }
    }
    // Abandonment never rereads borrowed input, including partially copied bytes.
    var registry: Registry = .{ .allocator = a };
    defer registry.deinitJoined();
    var borrowed: ?[]u8 = try a.dupe(u8, path);
    defer if (borrowed) |bytes| a.free(bytes);
    var operation = Operation.init(&registry, borrowed.?, .retain);
    defer operation.deinit();
    _ = try operation.planWithAdmission(null);
    try operation.resumeWithAdmission(null);
    a.free(borrowed.?);
    borrowed = null;
    try operation.retireWithAdmission(null);
    try std.testing.expectEqual(@as(u64, 0), registry.live_bytes.load(.acquire));
}

test "controlled physical sync pin registry collision publication fences removed owners and retains physical charge" {
    var registry: Registry = .{ .allocator = std.testing.allocator, .hash_bits = 0 };
    defer registry.deinitJoined();
    const first = try registry.retainPin("first");
    first.release();
    var stale = Operation.init(&registry, "new", .retain);
    defer stale.deinit();
    _ = try stale.planWithAdmission(null);
    try stale.resumeWithAdmission(null);
    const concurrent = try registry.retainPin("concurrent");
    concurrent.release();
    try std.testing.expectError(error.CompactionPlanningStale, stale.publishWithAdmission(null));
    try stale.retireWithAdmission(null);
    try std.testing.expect(registry.findLockedJoined("new") == null);
    var reader = Operation.init(&registry, "first", .query);
    defer reader.deinit();
    _ = try reader.planWithAdmission(null);
    try reader.resumeWithAdmission(null);
    const before = registry.live_bytes.load(.acquire);
    registry.forget("first");
    // The detached key remains physically charged to reader's retained borrow.
    try std.testing.expectEqual(before, registry.live_bytes.load(.acquire));
    const replacement = try registry.retainPin("first");
    defer replacement.release();
    try std.testing.expect(first != replacement);
    try std.testing.expectError(error.CompactionPlanningStale, reader.publishWithAdmission(null));
    try reader.retireWithAdmission(null);
    try std.testing.expect(registry.findLockedJoined("first") == replacement);
    // A cursor can also hold the final group after its last key is removed.
    var isolated: Registry = .{ .allocator = std.testing.allocator };
    defer isolated.deinitJoined();
    const sole = try isolated.retainPin("sole");
    sole.release();
    var old = Operation.init(&isolated, "sole", .query);
    defer old.deinit();
    _ = try old.planWithAdmission(null);
    try old.resumeWithAdmission(null);
    const retained = isolated.live_bytes.load(.acquire);
    isolated.forget("sole");
    try std.testing.expect(isolated.root == null);
    try std.testing.expectEqual(retained, isolated.live_bytes.load(.acquire));
    const newer = try isolated.retainPin("sole");
    defer newer.release();
    try std.testing.expect(sole != newer);
    try std.testing.expectError(error.CompactionPlanningStale, old.publishWithAdmission(null));
    try old.retireWithAdmission(null);
}

test "controlled physical sync pin registry lock contention uses original progress and lifetime checks" {
    const Control = struct {
        registry: *Registry,
        waits: usize = 0,
        fail_lifetime: bool = false,
        fn check(_: *anyopaque) !void {}
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_lifetime) return error.FileNotFound;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
            self.registry.mutex.unlock();
        }
    };
    var registry: Registry = .{ .allocator = std.testing.allocator };
    defer registry.deinitJoined();
    var control: Control = .{ .registry = &registry };
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.lifetime, .wait_for_progress = Control.wait } };
    const original = admission.Port.init(&payload);
    var operation = Operation.init(&registry, "progress", .retain);
    defer operation.deinit();
    try std.testing.expect(registry.mutex.tryLock());
    _ = try operation.planWithAdmission(original);
    try std.testing.expectEqual(@as(usize, 1), control.waits);
    try operation.resumeWithAdmission(original);
    control.fail_lifetime = true;
    try std.testing.expectError(error.FileNotFound, operation.publishWithAdmission(original));
    try std.testing.expect(registry.mutex.tryLock());
    registry.mutex.unlock();
    control.fail_lifetime = false;
    try operation.publishWithAdmission(original);
    (try operation.takePinWithAdmission(original)).release();
    try operation.retireWithAdmission(original);
}

const FundedWitness = struct {
    manager: *resources.ResourceManager,
    live: usize = 0,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn paid(self: *@This()) u64 {
        return self.manager.sliceStats(quota.slice).used_bytes - @sizeOf(quota.Owner);
    }
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.paid() >= self.live + len);
        const at = self.calls;
        self.calls += 1;
        if (at == self.fail_at) return null;
        const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return ptr;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.paid() >= self.live);
        std.testing.allocator.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
};
fn fundedExercise(path: []const u8, stage: enum { construct, publish, take, abort, close }, target: usize, abandon: bool) !usize {
    const a = std.testing.allocator;
    const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
    var manager_owned = true;
    defer if (manager_owned) manager.releaseOwned(a);
    var close: quota.Close = .{};
    defer close.deinitJoined();
    var creator = try quota.Creation.init(a, manager, &close, null);
    defer creator.deinitJoined();
    var fault: Fault = .{ .failure = switch (target % 3) {
        0 => error.Canceled,
        1 => error.DeadlineExceeded,
        else => error.FileNotFound,
    } };
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    try creator.resumeWithAdmission(original);
    const owner = try creator.takeWithAdmission(original);
    // Public/backend manager ownership ends; allocation and ledger operations
    // must continue using the quota authority's independent retained lifetime.
    manager.releaseOwned(a);
    manager_owned = false;
    var witness: FundedWitness = .{ .manager = manager };
    var registry: Registry = .{ .allocator = witness.allocator() };
    defer registry.deinitJoined();
    var operation = Operation.init(&registry, path, .retain);
    defer operation.deinit();
    if (stage != .construct) try operation.resumeFundedWithAdmission(owner, original);
    if (stage == .publish and target == std.math.maxInt(usize)) try std.testing.expectError(error.ResourceBudgetExceeded, operation.publishWithAdmission(original));
    if (stage == .take or stage == .abort or stage == .close) try operation.publishFundedWithAdmission(original);
    var pin: ?*Record = null;
    if (stage == .close) {
        (try operation.takePinWithAdmission(original)).release();
        try operation.retireWithAdmission(original);
        registry.deinitJoined();
        try owner.reconcileWithAdmission(original);
    }
    fault.checks = 0;
    fault.at = target;
    const result: anyerror!void = switch (stage) {
        .construct => operation.resumeFundedWithAdmission(owner, original),
        .publish => operation.publishFundedWithAdmission(original),
        .take => blk: {
            pin = operation.takePinWithAdmission(original) catch |err| break :blk err;
            break :blk {};
        },
        .abort => operation.retireWithAdmission(original),
        .close => close.resumeWithAdmission(original),
    };
    const checks = fault.checks;
    if (target == std.math.maxInt(usize)) try result else try std.testing.expectError(fault.failure, result);
    if (stage == .close) {
        try close.resumeWithAdmission(original);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(quota.slice).used_bytes);
        return checks;
    }
    if (!abandon and stage != .abort) {
        if (!operation.committed) {
            try operation.resumeFundedWithAdmission(owner, original);
            try operation.publishFundedWithAdmission(original);
        }
        if (pin == null) pin = try operation.takePinWithAdmission(original);
    }
    if (pin) |output| output.release();
    if (abandon and target % 2 == 0) operation.deinit() else try operation.retireWithAdmission(original);
    registry.deinitJoined();
    try owner.reconcileWithAdmission(original);
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(@as(u64, @sizeOf(quota.Owner)), close.usage);
    try close.resumeWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(quota.slice).used_bytes);
    return checks;
}

test "controlled physical sync funded registry retains every original construction adoption publication and close rejection after backend release" {
    const a = std.testing.allocator;
    const path = try a.alloc(u8, 2 * quantum + 13);
    defer a.free(path);
    @memset(path, 'f');
    inline for (.{ .construct, .publish, .take, .abort, .close }) |stage| {
        const checks = try fundedExercise(path, stage, std.math.maxInt(usize), false);
        for (0..checks) |at| for ([_]bool{ false, true }) |abandon| {
            _ = try fundedExercise(path, stage, at, abandon);
        };
    }
}

test "controlled physical sync funded registry refuses metadata quota before allocation and cannot bypass adoption" {
    const a = std.testing.allocator;
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(quota.slice)] = .{ .hard_limit_bytes = @sizeOf(quota.Owner) + 1 };
    const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a, .budgets = budgets });
    defer manager.releaseOwned(a);
    var close: quota.Close = .{};
    defer close.deinitJoined();
    var creator = try quota.Creation.init(a, manager, &close, null);
    defer creator.deinitJoined();
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    try creator.resumeWithAdmission(original);
    const owner = try creator.takeWithAdmission(original);
    var witness: FundedWitness = .{ .manager = manager };
    var registry: Registry = .{ .allocator = witness.allocator() };
    defer registry.deinitJoined();
    var operation = Operation.init(&registry, "quota-refusal", .retain);
    defer operation.deinit();
    try std.testing.expectError(error.ResourceBudgetExceeded, operation.resumeFundedWithAdmission(owner, original));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
    try std.testing.expectError(error.ResourceBudgetExceeded, operation.resumeWithAdmission(original));
    try operation.retireWithAdmission(original);
    try close.resumeWithAdmission(original);
}

test "controlled physical sync funded registry actual allocation failures retain prepaid cleanup through retry and abandonment" {
    const a = std.testing.allocator;
    const path = try a.alloc(u8, 2 * quantum + 13);
    defer a.free(path);
    @memset(path, 'o');
    // Missing-key construction allocates a record/path, group, then branch.
    for (0..3) |at| for ([_]bool{ false, true }) |abandon| {
        const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
        defer manager.releaseOwned(a);
        var close: quota.Close = .{};
        defer close.deinitJoined();
        var creator = try quota.Creation.init(a, manager, &close, null);
        defer creator.deinitJoined();
        var fault: Fault = .{};
        const payload = fault.payload();
        const original = admission.Port.init(&payload);
        try creator.resumeWithAdmission(original);
        const owner = try creator.takeWithAdmission(original);
        var witness: FundedWitness = .{ .manager = manager, .fail_at = at };
        var registry: Registry = .{ .allocator = witness.allocator() };
        defer registry.deinitJoined();
        var operation = Operation.init(&registry, path, .retain);
        defer operation.deinit();
        try std.testing.expectError(error.OutOfMemory, operation.resumeFundedWithAdmission(owner, original));
        try std.testing.expectEqual(at + 1, witness.calls);
        try std.testing.expectEqual(@as(usize, 0), registry.key_count);
        try std.testing.expect(witness.paid() >= witness.live);
        if (!abandon) {
            try operation.resumeFundedWithAdmission(owner, original);
            try operation.publishFundedWithAdmission(original);
            (try operation.takePinWithAdmission(original)).release();
        }
        try operation.retireWithAdmission(original);
        registry.deinitJoined();
        try owner.reconcileWithAdmission(original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try std.testing.expectEqual(@as(u64, @sizeOf(quota.Owner)), close.usage);
        try close.resumeWithAdmission(original);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(quota.slice).used_bytes);
    };
}
