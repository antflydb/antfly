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

//! Pre-WAL metadata credit. Group by the actual output partition, not by row:
//! a large ordinary batch must not reserve one manifest descriptor per row.
const std = @import("std");
const Key = struct { namespace: []const u8, namespace_present: bool, prefix: []const u8, domain: []const u8 };
const Context = struct {
    pub fn hash(_: @This(), key: Key) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.namespace_present));
        for ([_][]const u8{ key.namespace, key.prefix, key.domain }) |part| {
            hasher.update(std.mem.asBytes(&part.len));
            hasher.update(part);
        }
        return hasher.final();
    }
    pub fn eql(_: @This(), a: Key, b: Key) bool {
        return a.namespace_present == b.namespace_present and std.mem.eql(u8, a.namespace, b.namespace) and std.mem.eql(u8, a.prefix, b.prefix) and std.mem.eql(u8, a.domain, b.domain);
    }
};
const Group = struct { rows: u64 = 0, logical: u64 = 0, physical: u64 = 0, bound_bytes: u64 = 0 };
const Groups = std.HashMapUnmanaged(Key, Group, Context, 80);

pub fn estimate(allocator: std.mem.Allocator, options: anytype, root_len: usize, first: anytype, second: anytype) !u64 {
    var groups: Groups = .empty;
    defer groups.deinit(allocator);
    inline for (.{ first, second }) |state| {
        for (0..state.entryCount()) |i| {
            const entry = state.entryAt(i);
            const partitioned = options.run_partition_prefix_bytes != 0 or options.run_partition_key != null;
            const key = Key{
                .namespace = if (partitioned) entry.namespace_name orelse "" else "",
                .namespace_present = partitioned and entry.namespace_name != null,
                .prefix = entry.key[0..@min(options.run_partition_prefix_bytes, entry.key.len)],
                .domain = if (options.run_partition_key) |extract| extract(entry.key) else "",
            };
            const result = try groups.getOrPut(allocator, key);
            if (!result.found_existing) result.value_ptr.* = .{};
            const group = result.value_ptr;
            const names = entry.key.len + (if (entry.namespace_name) |name| name.len else 0);
            group.rows +|= 1;
            group.logical +|= 32 +| names +| entry.value.len;
            // Raw block payload, per-entry offsets, Bloom/prefix filters and
            // worst-case one-entry-block bounds. Compression falls back to
            // raw bytes when its output grows.
            group.physical +|= 256 +| 8 *| names +| 2 *| entry.value.len;
            group.bound_bytes = @max(group.bound_bytes, names);
        }
    }
    if (groups.count() == 0) return 0;
    var bytes: u64 = 128;
    var iterator = groups.valueIterator();
    while (iterator.next()) |group| {
        bytes +|= groupWire(options, root_len, group.*);
    }
    return bytes;
}

const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
pub var test_hook: ?*const fn (*OwnedEstimate, admission.Port) anyerror!void = null;
pub var test_retire_hook: ?*const fn (*OwnedEstimate, admission.Port) anyerror!void = null;

/// One synchronous planning attempt. Keys borrow only the producer's staged
/// entries during estimate(); retirement follows owned next pointers and never
/// dereferences those borrowed labels. The allocator and every partial table
/// survive rejection in the backend's existing preparation retirement queue.
pub const OwnedEstimate = struct {
    const Node = struct { next: ?*Node, hash: u64, key: Key, group: Group = .{} };
    const Slots = []?*Node;
    backing: std.mem.Allocator,
    budget: ?resources.BudgetedAllocator = null,
    nodes: ?*Node = null,
    count: usize = 0,
    slots: Slots = &.{},
    pending_slots: Slots = &.{},
    old_slots: Slots = &.{},
    old_offset: usize = 0,
    pending_offset: usize = 0,
    slots_offset: usize = 0,
    retired: bool = false,
    retiring: bool = false,

    pub fn init(backing: std.mem.Allocator, manager: ?*resources.ResourceManager) @This() {
        return .{ .backing = backing, .budget = if (manager) |m| resources.BudgetedAllocator.init(m, .lsm_table_builder_working_set, backing, 1) else null };
    }
    fn allocator(self: *@This()) std.mem.Allocator {
        return if (self.budget) |*budget| budget.threadSafeNoReclaimAllocator() else self.backing;
    }
    fn checkpoint(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
    }
    fn hashKey(self: *@This(), key: Key, original: admission.Port) !u64 {
        var hash = std.hash.Wyhash.init(0);
        hash.update(std.mem.asBytes(&key.namespace_present));
        for ([_][]const u8{ key.namespace, key.prefix, key.domain }) |part| {
            hash.update(std.mem.asBytes(&part.len));
            var offset: usize = 0;
            while (offset < part.len) {
                try self.checkpoint(original);
                const end = offset + @min(part.len - offset, 16 * 1024);
                hash.update(part[offset..end]);
                offset = end;
            }
        }
        return hash.final();
    }
    fn equal(self: *@This(), a: Key, b: Key, original: admission.Port) !bool {
        if (a.namespace_present != b.namespace_present) return false;
        for ([_][]const u8{ a.namespace, a.prefix, a.domain }, [_][]const u8{ b.namespace, b.prefix, b.domain }) |left, right| {
            if (left.len != right.len) return false;
            var offset: usize = 0;
            while (offset < left.len) {
                try self.checkpoint(original);
                const end = offset + @min(left.len - offset, 16 * 1024);
                if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
                offset = end;
            }
        }
        return true;
    }
    fn retirementCheckpoint(self: *@This(), original: ?admission.Port) !void {
        if (original) |port| {
            try port.checkpoint();
            if (comptime @import("builtin").is_test) if (self.retiring) if (test_retire_hook) |hook| try hook(self, port);
        }
    }
    fn retireSlots(self: *@This(), slots: *Slots, offset: *usize, credits: ?*usize, original: ?admission.Port) !bool {
        const bytes = std.mem.sliceAsBytes(slots.*);
        while (offset.* < bytes.len) {
            if (credits) |remaining| if (remaining.* == 0) return false;
            try self.retirementCheckpoint(original);
            if (credits) |remaining| remaining.* -= 1;
            const end = offset.* + @min(bytes.len - offset.*, 16 * 1024);
            @memset(bytes[offset.*..end], undefined);
            offset.* = end;
        }
        if (bytes.len != 0) {
            if (credits) |remaining| if (remaining.* == 0) return false;
            try self.retirementCheckpoint(original);
            if (credits) |remaining| remaining.* -= 1;
            self.allocator().rawFree(bytes, .of(?*Node), @returnAddress());
        }
        slots.* = &.{};
        offset.* = 0;
        return true;
    }
    fn grow(self: *@This(), original: admission.Port) !void {
        const capacity = if (self.slots.len == 0) 8 else try std.math.mul(usize, self.slots.len, 2);
        const bytes = try std.math.mul(usize, capacity, @sizeOf(?*Node));
        try self.checkpoint(original);
        const raw = self.allocator().rawAlloc(bytes, .of(?*Node), @returnAddress()) orelse return error.OutOfMemory;
        self.pending_slots = @as([*]?*Node, @ptrCast(@alignCast(raw)))[0..capacity];
        var offset: usize = 0;
        while (offset < capacity) {
            try self.checkpoint(original);
            const end = offset + @min(capacity - offset, (16 * 1024) / @sizeOf(?*Node));
            @memset(self.pending_slots[offset..end], null);
            offset = end;
        }
        var cursor = self.nodes;
        while (cursor) |node| : (cursor = node.next) {
            var slot: usize = @as(usize, @truncate(node.hash)) & (capacity - 1);
            while (self.pending_slots[slot] != null) {
                try self.checkpoint(original);
                slot = (slot + 1) & (capacity - 1);
            }
            try self.checkpoint(original);
            self.pending_slots[slot] = node;
        }
        self.old_slots = self.slots;
        self.slots = self.pending_slots;
        self.pending_slots = &.{};
        const complete = try self.retireSlots(&self.old_slots, &self.old_offset, null, original);
        std.debug.assert(complete);
    }
    fn group(self: *@This(), key: Key, original: admission.Port) !*Group {
        const hash = try self.hashKey(key, original);
        if (self.slots.len != 0) {
            var slot: usize = @as(usize, @truncate(hash)) & (self.slots.len - 1);
            while (self.slots[slot]) |node| {
                try self.checkpoint(original);
                if (node.hash == hash and try self.equal(node.key, key, original)) return &node.group;
                slot = (slot + 1) & (self.slots.len - 1);
            }
        }
        if (self.count >= self.slots.len / 2) try self.grow(original);
        try self.checkpoint(original);
        const node = try self.allocator().create(Node);
        node.* = .{ .next = self.nodes, .hash = hash, .key = key };
        self.nodes = node;
        self.count += 1;
        var slot: usize = @as(usize, @truncate(hash)) & (self.slots.len - 1);
        while (self.slots[slot] != null) {
            try self.checkpoint(original);
            slot = (slot + 1) & (self.slots.len - 1);
        }
        self.slots[slot] = node;
        return &node.group;
    }
    fn add(target: *Group, entry: anytype) void {
        const names = @as(u64, entry.key.len) +| if (entry.namespace_name) |name| name.len else @as(usize, 0);
        target.rows +|= 1;
        target.logical +|= 32 +| names +| entry.value.len;
        target.physical +|= 256 +| 8 *| names +| 2 *| entry.value.len;
        target.bound_bytes = @max(target.bound_bytes, names);
    }
    pub fn estimate(self: *@This(), options: anytype, root_len: usize, first: anytype, second: anytype, original: admission.Port) !u64 {
        try self.checkpoint(original);
        var unpartitioned: Group = .{};
        const partitioned = options.run_partition_prefix_bytes != 0 or options.run_partition_key != null;
        inline for (.{ first, second }) |state| {
            for (0..state.entryCount()) |i| {
                // The scalar path touches fixed metadata only. Bound its CPU
                // work by rows without dispatching authority for every field.
                if (partitioned or i % 64 == 0) try self.checkpoint(original);
                const entry = state.entryAt(i);
                if (!partitioned) {
                    add(&unpartitioned, entry);
                    continue;
                }
                const domain = if (options.run_partition_key != null) blk: {
                    const extract = options.run_partition_key_with_admission orelse return error.ExecutionWaitUnsupported;
                    break :blk try extract(entry.key, original);
                } else "";
                const target = try self.group(.{ .namespace = entry.namespace_name orelse "", .namespace_present = entry.namespace_name != null, .prefix = entry.key[0..@min(options.run_partition_prefix_bytes, entry.key.len)], .domain = domain }, original);
                add(target, entry);
            }
        }
        if (!partitioned) {
            try self.checkpoint(original);
            return if (unpartitioned.rows == 0) 0 else 128 +| groupWire(options, root_len, unpartitioned);
        }
        if (self.count == 0) return 0;
        var bytes: u64 = 128;
        var cursor = self.nodes;
        while (cursor) |node| : (cursor = node.next) {
            try self.checkpoint(original);
            bytes +|= groupWire(options, root_len, node.group);
        }
        return bytes;
    }
    pub fn retireStep(self: *@This(), credits: *usize, original: ?admission.Port) !bool {
        if (self.retired) return true;
        self.retiring = true;
        try self.retirementCheckpoint(original);
        // old_slots can already be partially poisoned by an interrupted grow.
        if (!try self.retireSlots(&self.old_slots, &self.old_offset, credits, original)) return false;
        if (!try self.retireSlots(&self.pending_slots, &self.pending_offset, credits, original)) return false;
        if (!try self.retireSlots(&self.slots, &self.slots_offset, credits, original)) return false;
        while (self.nodes) |node| {
            if (credits.* == 0) return false;
            try self.retirementCheckpoint(original);
            credits.* -= 1;
            self.nodes = node.next;
            self.allocator().destroy(node);
        }
        if (credits.* == 0) return false;
        try self.retirementCheckpoint(original);
        credits.* -= 1;
        if (self.budget) |*budget| {
            std.debug.assert(budget.live_bytes == 0);
            budget.deinit();
            self.budget = null;
        }
        self.retired = true;
        return true;
    }
    pub fn retireWithAdmission(self: *@This(), original: ?admission.Port) !void {
        var credits: usize = std.math.maxInt(usize);
        const complete = try self.retireStep(&credits, original);
        std.debug.assert(complete);
    }
};

fn groupWire(options: anytype, root_len: usize, group: Group) u64 {
    const logical_limit = @max(@as(u64, 1), @min(options.max_run_file_bytes, @import("../lsm/table_file.zig").max_entry_data_len));
    const physical_limit = @max(@as(u64, 1), @min(options.max_run_file_physical_bytes, @import("repository.zig").maxRunFileReadBytes()));
    // Greedy packing has at most 1 + 2*sum/capacity outputs. Account for
    // each independent splitting limit; tiny physical caps fall back to
    // the one-row bound. No assumption about input insertion order.
    var outputs = @min(group.rows, 1 +| (2 *| group.logical) / logical_limit);
    outputs = @min(group.rows, outputs +| if (physical_limit <= 64 * 1024) group.rows else (2 *| group.physical) / (physical_limit - 64 * 1024));
    if (options.max_run_file_entries != 0) outputs = @min(group.rows, outputs +| group.rows / options.max_run_file_entries);
    return outputs *| (192 +| root_len +| 2 *| group.bound_bytes);
}

/// Allocation-free credit planning over a stable sorted generation. Partition
/// identities must describe contiguous intervals, as required by Options.
/// Borrowed labels remain pinned by the generation until this cursor finishes;
/// no caller execution or allocator is retained.
pub const SortedEstimate = struct {
    index: usize = 0,
    group: Group = .{},
    key: ?Key = null,
    pending: ?Key = null,
    compare_part: usize = 0,
    compared: usize = 0,
    same: bool = true,
    bytes: u64 = 128,
    complete: bool = false,

    pub fn resumeWithAdmission(self: *@This(), options: anytype, root_len: usize, state: anytype, original: @import("../../common/read_admission.zig").Port) !u64 {
        try original.checkpoint();
        if (self.complete) return self.bytes;
        while (self.index < state.entryCount()) {
            try original.checkpoint();
            const entry = state.entryAt(self.index);
            if (self.pending == null) {
                const partitioned = options.run_partition_prefix_bytes != 0 or options.run_partition_key != null;
                const domain = if (options.run_partition_key != null) blk: {
                    const extract = options.run_partition_key_with_admission orelse return error.ExecutionWaitUnsupported;
                    break :blk try extract(entry.key, original);
                } else "";
                self.pending = .{
                    .namespace = if (partitioned) entry.namespace_name orelse "" else "",
                    .namespace_present = partitioned and entry.namespace_name != null,
                    .prefix = entry.key[0..@min(options.run_partition_prefix_bytes, entry.key.len)],
                    .domain = domain,
                };
                self.same = if (self.key) |key| key.namespace_present == self.pending.?.namespace_present else false;
                self.compare_part = 0;
                self.compared = 0;
            }
            if (self.key) |key| {
                const before = [_][]const u8{ key.namespace, key.prefix, key.domain };
                const after = [_][]const u8{ self.pending.?.namespace, self.pending.?.prefix, self.pending.?.domain };
                while (self.same and self.compare_part < 3) {
                    try original.checkpoint();
                    const a = before[self.compare_part];
                    const b = after[self.compare_part];
                    if (a.len != b.len) {
                        self.same = false;
                        break;
                    }
                    const end = self.compared + @min(a.len - self.compared, 16 * 1024);
                    if (!std.mem.eql(u8, a[self.compared..end], b[self.compared..end])) {
                        self.same = false;
                        break;
                    }
                    self.compared = end;
                    if (end == a.len) {
                        self.compare_part += 1;
                        self.compared = 0;
                    }
                }
            }
            try original.checkpoint();
            if (!self.same) {
                if (self.key != null) self.bytes +|= groupWire(options, root_len, self.group);
                self.group = .{};
                self.key = self.pending;
            }
            const names = @as(u64, entry.key.len) +| if (entry.namespace_name) |name| name.len else @as(usize, 0);
            self.group.rows +|= 1;
            self.group.logical +|= 32 +| names +| entry.value.len;
            self.group.physical +|= 256 +| 8 *| names +| 2 *| entry.value.len;
            self.group.bound_bytes = @max(self.group.bound_bytes, names);
            self.pending = null;
            self.index += 1;
        }
        try original.checkpoint();
        self.bytes = if (self.key != null) self.bytes +| groupWire(options, root_len, self.group) else 0;
        self.key = null;
        self.complete = true;
        return self.bytes;
    }
};
