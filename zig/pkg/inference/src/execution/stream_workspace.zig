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

//! A sealed physical workspace recipe for sequential stream submission.
//! The same offsets govern capacity admission and ordinary or captured
//! dispatch. Storage can overlap only after its previous consumer is enqueued
//! on the same stream. Backend owners retain and retire the actual allocation.
const std = @import("std");
// CUDA device addresses are 64-bit tokens, independent of host pointer width.
const DevicePointer = u64;
pub const limit = 128 * 1024 * 1024;
pub const alignment = 256;

pub const Layout = struct {
    bytes: usize,
    scratch_offset: usize,
    events: usize,
    recipe: u64,
    output: ?struct { offset: usize, bytes: usize },
    external_output: bool = false,
};
// A live allocation has a nonzero observed address or declared slot ID.

const Slot = u12;
const no_slot = std.math.maxInt(Slot);
const Allocation = struct {
    lifetime: extern union { live_id: u64, external_offset: u64 },
    bytes: u32,
    // Arena offsets are always aligned. Store the cold live-list link in
    // the spare bits so a cached recipe needs no separate index array.
    placement: packed struct(u32) { offset_blocks: u20, next_live: Slot },

    fn offset(self: Allocation) u32 {
        return @as(u32, self.placement.offset_blocks) * alignment;
    }
};
// A trace is kept per cached shape. Slot indices fit in the fixed plan and
// never need pointer-width storage or an independently padded flag.
const max_allocations = 2048;
const Event = packed struct(u16) {
    slot: u14,
    allocate: bool,
    operation_scratch: bool = false,
};

comptime {
    if (limit > std.math.maxInt(u32)) @compileError("stream workspace geometry overflow");

    if (max_allocations > no_slot or max_allocations - 1 > std.math.maxInt(u14)) @compileError("stream workspace event slot overflow");
    if (limit / alignment > std.math.maxInt(u20)) @compileError("stream workspace placement overflow");
}
pub const Origin = enum { observed, declared };

pub const Plan = struct {
    origin: Origin = .observed,
    allocations: [max_allocations]Allocation = undefined,
    events: [max_allocations * 2]Event = undefined,
    first_live: Slot = no_slot,
    count: usize = 0,
    event_count: usize = 0,
    declared_operation_scratch_intervals: usize = 0,
    peak: usize = 0,
    scratch_peak: usize = 0,
    external_scratch_savings_bytes: usize = 0,
    output: ?usize = null,
    recipe: u64 = 0,
    sealed: bool = false,
    valid: bool = true,
    fn event(self: *Plan, slot: usize, is_alloc: bool) void {
        if (self.event_count == self.events.len) {
            self.valid = false;
            return;
        }
        self.events[self.event_count] = .{ .slot = @intCast(slot), .allocate = is_alloc };
        self.event_count += 1;
    }
    pub fn allocate(self: *Plan, bytes: usize, pointer: DevicePointer) void {
        if (self.origin != .observed) {
            if (!self.sealed) self.valid = false;
            return;
        }
        self.allocateLive(bytes, pointer);
    }

    fn allocateLive(self: *Plan, bytes: usize, observed_address: ?u64) void {
        if (!self.valid or self.sealed) return;
        // Declared slot identities are monotonic and cannot duplicate a live
        // slot. Only observation needs to check every remaining live address.
        const live_id = observed_address orelse self.count + 1;
        if (self.count == self.allocations.len or bytes == 0 or bytes > limit or live_id == 0) {
            self.valid = false;
            return;
        }
        const extent = std.mem.alignForward(usize, bytes, alignment);
        const placement = self.findPlacement(false, extent, observed_address) orelse {
            self.valid = false;
            return;
        };
        const offset = placement.offset;
        const predecessor = placement.predecessor;
        self.allocations[self.count] = .{
            .bytes = @intCast(bytes),
            .placement = .{ .offset_blocks = @intCast(offset / alignment), .next_live = self.nextLive(predecessor) },
            .lifetime = .{ .live_id = live_id },
        };
        self.setNextLive(predecessor, @intCast(self.count));
        self.event(self.count, true);
        self.count += 1;
        self.peak = @max(self.peak, offset + extent);
    }
    fn findPlacement(self: *const Plan, comptime external: bool, extent: usize, observed_address: ?u64) ?struct { offset: usize, predecessor: Slot } {
        var offset: usize = 0;
        var previous: Slot = no_slot;
        var slot = self.first_live;
        var insert_after: ?Slot = null;
        while (slot != no_slot) {
            const allocation = self.allocations[slot];
            if (observed_address) |address| if (allocation.lifetime.live_id == address) return null;
            if (insert_after == null) {
                const start = if (external) @as(usize, @intCast(allocation.lifetime.external_offset)) else allocation.offset();
                if (offset + extent <= start) {
                    insert_after = previous;
                    if (observed_address == null) break;
                } else {
                    offset = start + std.mem.alignForward(usize, allocation.bytes, alignment);
                }
            }
            previous = slot;
            slot = allocation.placement.next_live;
        }
        if (offset > limit - extent) return null;
        return .{ .offset = offset, .predecessor = insert_after orelse previous };
    }

    pub fn release(self: *Plan, pointer: DevicePointer) void {
        if (!self.valid or self.sealed or pointer == 0) return;
        if (self.origin != .observed) {
            self.valid = false;
            return;
        }
        var previous: Slot = no_slot;
        var slot = self.first_live;
        while (slot != no_slot) {
            if (self.allocations[slot].lifetime.live_id == pointer) {
                self.unlinkLive(previous, slot);
                return;
            }
            previous = slot;
            slot = self.nextLive(slot);
        }
    }

    fn nextLive(self: *const Plan, predecessor: Slot) Slot {
        return if (predecessor == no_slot) self.first_live else self.allocations[predecessor].placement.next_live;
    }

    fn setNextLive(self: *Plan, predecessor: Slot, next: Slot) void {
        if (predecessor == no_slot) self.first_live = next else self.allocations[predecessor].placement.next_live = next;
    }

    fn releaseSlot(self: *Plan, target: Slot) void {
        self.unlinkLive(self.predecessorOf(target), target);
    }

    fn predecessorOf(self: *const Plan, target: Slot) Slot {
        var previous: Slot = no_slot;
        var slot = self.first_live;
        while (slot != target) {
            std.debug.assert(slot != no_slot);
            previous = slot;
            slot = self.nextLive(slot);
        }
        return previous;
    }

    fn unlinkLive(self: *Plan, predecessor: Slot, slot: Slot) void {
        self.setNextLive(predecessor, self.nextLive(slot));
        self.allocations[slot].lifetime.live_id = 0;
        self.event(slot, false);
    }

    fn allocateOperationScratch(self: *Plan, bytes: usize, address: ?DevicePointer) bool {
        const slot = self.count;
        self.allocateLive(bytes, address);
        if (!self.valid or self.count == slot) return false;
        self.events[self.event_count - 1].operation_scratch = true;
        self.declared_operation_scratch_intervals += 1;
        return true;
    }

    fn appendOperationScratch(self: *Plan, bytes: usize) bool {
        const slot = self.count;
        if (!self.allocateOperationScratch(bytes, null)) return false;
        self.releaseSlot(@intCast(slot));
        return self.valid;
    }

    /// Allocations receive slots in declaration order. A release names the
    /// slot of an earlier allocation. Only the published slot may escape.
    pub const Declaration = union(enum) { allocate: usize, release: u15 };

    /// Incremental declaration lets operation contracts compose directly into
    /// the destination plan without storing a second, flat event recipe.
    pub const Builder = struct {
        plan: *Plan,

        pub fn allocate(self: Builder, bytes: usize) !u15 {
            if (self.plan.origin != .declared) return error.InvalidExecutionPlan;
            const slot = self.plan.count;
            self.plan.allocateLive(bytes, null);
            if (!self.plan.valid or self.plan.count == slot) return error.InvalidExecutionPlan;
            return @intCast(slot);
        }

        pub fn release(self: Builder, slot: u15) !void {
            if (self.plan.origin != .declared or !self.plan.valid or self.plan.sealed or
                slot >= self.plan.count or self.plan.allocations[slot].lifetime.live_id == 0)
                return error.InvalidExecutionPlan;
            self.plan.releaseSlot(@intCast(slot));
            if (!self.plan.valid) return error.InvalidExecutionPlan;
        }

        /// Declare scratch that stays live across other scoped allocations.
        pub fn allocateScratch(self: Builder, bytes: usize) !u15 {
            if (self.plan.origin != .declared) return error.InvalidExecutionPlan;
            const slot = self.plan.count;
            if (!self.plan.allocateOperationScratch(bytes, null)) return error.InvalidExecutionPlan;
            return @intCast(slot);
        }

        /// An operation submits all of its scratch users before the next
        /// declared event. Its private offsets are relative to this interval.
        pub fn scratchInterval(self: Builder, bytes: usize) !void {
            if (self.plan.origin != .declared or !self.plan.appendOperationScratch(bytes)) return error.InvalidExecutionPlan;
        }

        pub fn finish(self: Builder, published_slot: ?u15) !void {
            if (self.plan.origin != .declared) return error.InvalidExecutionPlan;
            const published: ?u64 = if (published_slot) |slot| p: {
                if (slot >= self.plan.count or self.plan.allocations[slot].lifetime.live_id == 0) return error.InvalidExecutionPlan;
                break :p self.plan.allocations[slot].lifetime.live_id;
            } else null;
            if (!self.plan.seal(published)) return error.InvalidExecutionPlan;
        }
    };

    pub fn declarationBuilder(self: *Plan) !Builder {
        if (!self.valid or self.sealed or self.origin != .observed or self.event_count != 0 or self.count != 0)
            return error.InvalidExecutionPlan;
        self.origin = .declared;
        return .{ .plan = self };
    }

    pub fn declare(self: *Plan, declarations: []const Declaration, published_slot: ?u15) !void {
        const builder = try self.declarationBuilder();
        errdefer self.* = .{};
        for (declarations) |declaration| switch (declaration) {
            .allocate => |bytes| _ = try builder.allocate(bytes),
            .release => |slot| try builder.release(slot),
        };
        try builder.finish(published_slot);
    }

    /// Both ordinary execution and recording consume the same interval.
    pub fn declareScratch(self: *Plan, bytes: usize) !void {
        const builder = try self.declarationBuilder();
        errdefer self.* = .{};
        try builder.scratchInterval(bytes);
        try builder.finish(null);
    }

    pub fn finishNoOutput(self: *Plan) bool {
        return self.origin == .observed and self.seal(null);
    }

    pub fn finish(self: *Plan, pointer: DevicePointer) bool {
        return self.origin == .observed and self.seal(pointer);
    }

    fn seal(self: *Plan, expected: ?u64) bool {
        if (self.sealed) return false;
        var slot = self.first_live;
        while (slot != no_slot) : (slot = self.nextLive(slot)) {
            const allocation = &self.allocations[slot];
            if (expected != allocation.lifetime.live_id or self.output != null) self.valid = false;
            // The published layout keeps slot geometry, never a learning-run address.
            allocation.lifetime.live_id = 0;
            self.output = slot;
        }
        self.valid = self.valid and (expected == null) == (self.output == null);
        if (self.valid) if (self.output) |output| self.packExternalScratch(output);
        self.first_live = no_slot;
        self.sealed = self.valid;
        if (self.sealed) self.recipe = self.hashEvents(0);
        return self.valid;
    }

    fn packExternalScratch(self: *Plan, output: usize) void {
        const inline_peak = self.restoreExternalOffsets(output);
        self.scratch_peak = inline_peak;
        if (output + 1 == self.count) return;

        // Replay sealed lifetimes with the existing live-list links. Keep
        // the original offsets available when first-fit packing costs more.
        self.first_live = no_slot;
        var peak: usize = 0;
        for (self.events[0..self.event_count]) |entry| {
            if (entry.slot == output) continue;
            const slot: Slot = @intCast(entry.slot);
            if (entry.allocate) {
                const allocation = &self.allocations[slot];
                const extent = std.mem.alignForward(usize, allocation.bytes, alignment);
                const placement = self.findPlacement(true, extent, null) orelse break;
                const end = placement.offset + extent;
                if (end > inline_peak) break;
                allocation.lifetime.external_offset = placement.offset;
                allocation.placement.next_live = self.nextLive(placement.predecessor);
                self.setNextLive(placement.predecessor, slot);
                peak = @max(peak, end);
            } else self.setNextLive(self.predecessorOf(slot), self.nextLive(slot));
        } else {
            self.scratch_peak = peak;
            self.external_scratch_savings_bytes = inline_peak - peak;
            return;
        }
        _ = self.restoreExternalOffsets(output);
    }

    fn restoreExternalOffsets(self: *Plan, output: usize) usize {
        var peak: usize = 0;
        for (self.allocations[0..self.count], 0..) |*allocation, index| {
            if (index == output) continue;
            const offset = allocation.offset();
            allocation.lifetime.external_offset = offset;
            peak = @max(peak, offset + std.mem.alignForward(usize, allocation.bytes, alignment));
        }
        return peak;
    }

    pub fn layout(self: *const Plan, prefix_bytes: usize) !Layout {
        return self.layoutImpl(prefix_bytes, false);
    }

    /// Identify the sealed event schedule independently of its backing addresses.
    pub fn fingerprint(self: *const Plan, seed: u64) u64 {
        std.debug.assert(self.sealed);
        return if (seed == 0) self.recipe else self.hashEvents(seed);
    }

    fn hashEvents(self: *const Plan, seed: u64) u64 {
        var hash = std.hash.Wyhash.init(seed);
        for (self.events[0..self.event_count]) |entry| {
            const encoded: u16 = @bitCast(entry);
            hash.update(std.mem.asBytes(&encoded));
            const allocation = self.allocations[entry.slot];
            hash.update(std.mem.asBytes(&allocation.bytes));
            const offset = allocation.offset();
            hash.update(std.mem.asBytes(&offset));
        }
        return hash.final();
    }

    pub fn layoutWithExternalOutput(self: *const Plan, prefix_bytes: usize) !Layout {
        return self.layoutImpl(prefix_bytes, true);
    }

    fn layoutImpl(self: *const Plan, prefix_bytes: usize, external: bool) !Layout {
        if (!self.sealed or !self.valid or prefix_bytes % alignment != 0 or prefix_bytes > limit) return error.InvalidExecutionPlan;
        const output = if (self.output) |index| self.allocations[index] else null;
        const peak = if (external) self.scratch_peak else self.peak;
        const external_bytes = if (external) std.mem.alignForward(usize, (output orelse return error.InvalidExecutionPlan).bytes, alignment) else 0;
        if (peak > limit - prefix_bytes or external_bytes > limit - prefix_bytes - peak) return error.InvalidExecutionPlan;
        return .{
            .bytes = prefix_bytes + peak,
            .scratch_offset = prefix_bytes,
            .events = self.event_count,
            .recipe = self.recipe,
            .output = if (output) |region| .{ .offset = if (external) 0 else prefix_bytes + region.offset(), .bytes = region.bytes } else null,
            .external_output = external,
        };
    }
};
pub const Scope = struct {
    recipe: union(enum) { observing: *Plan, bound: *const Plan },
    base: DevicePointer = 0,
    empty_backing: bool = false,
    cursor: usize = 0,
    valid: bool = true,
    published_ptr: DevicePointer = 0,
    requires_published_output: bool = false,
    bound_start: DevicePointer = 0,
    bound_end: DevicePointer = 0,
    /// Observe ordinary device allocations while learning a cold recipe.
    /// This scope has no authority to return addresses from the plan.
    pub fn observe(plan: *Plan) Scope {
        std.debug.assert(!plan.sealed);
        return .{ .recipe = .{ .observing = plan } };
    }

    pub fn recordAllocation(self: *Scope, bytes: usize, pointer: DevicePointer) void {
        std.debug.assert(!self.isBound());
        self.recipe.observing.allocate(bytes, pointer);
    }

    /// Observe scratch that stays live across other scoped allocations.
    pub fn recordScratchAllocation(self: *Scope, bytes: usize, pointer: DevicePointer) void {
        std.debug.assert(!self.isBound());
        const plan = self.recipe.observing;
        std.debug.assert(plan.origin == .observed);
        _ = plan.allocateOperationScratch(bytes, pointer);
    }

    /// Add scratch for an operation that declares its size and makes no other
    /// scoped allocations before its final consumer is queued on this stream.
    /// Its actual backing can come from an eager command workspace.
    pub fn recordScratchInterval(self: *Scope, bytes: usize) void {
        std.debug.assert(!self.isBound());
        const plan = self.recipe.observing;
        if (plan.origin != .observed) {
            if (!plan.sealed) plan.valid = false;
            return;
        }
        _ = plan.appendOperationScratch(bytes);
    }

    /// Bind the funded byte range before recording or submitting any kernels.
    pub fn bind(plan: *const Plan, layout: Layout, backing: DevicePointer, backing_bytes: usize) !Scope {
        if (!std.meta.eql(layout, try plan.layoutImpl(layout.scratch_offset, layout.external_output))) return error.InvalidExecutionPlan;
        if (layout.bytes == 0) {
            if (backing != 0 or backing_bytes != 0) return error.InvalidExecutionPlan;
            return .{ .recipe = .{ .bound = plan }, .empty_backing = true, .requires_published_output = layout.external_output };
        }
        if (backing == 0 or backing % alignment != 0 or layout.bytes > backing_bytes or
            backing > @as(DevicePointer, std.math.maxInt(DevicePointer)) - @as(DevicePointer, backing_bytes)) return error.InvalidExecutionPlan;
        return .{ .recipe = .{ .bound = plan }, .base = backing + layout.scratch_offset, .bound_start = backing, .bound_end = backing + backing_bytes, .requires_published_output = layout.external_output };
    }

    pub inline fn isBound(self: *const Scope) bool {
        return self.recipe == .bound;
    }

    pub fn complete(self: *const Scope) bool {
        return self.isBound() and self.valid and self.cursor == self.recipe.bound.event_count;
    }

    /// Bind the actual backing span before execution or capture. The sealed
    /// event retains its exact write size even when the backing is larger.
    pub fn bindPublishedOutput(self: *Scope, pointer: DevicePointer, backing_bytes: usize) !void {
        if (!self.isBound()) return error.InvalidExecutionPlan;
        const slot = self.recipe.bound.output orelse return error.InvalidExecutionPlan;
        if (!self.valid or self.cursor != 0 or self.published_ptr != 0 or pointer % alignment != 0 or
            backing_bytes < self.recipe.bound.allocations[slot].bytes or !self.disjointBacking(pointer, backing_bytes)) return error.InvalidExecutionPlan;
        self.published_ptr = pointer;
    }

    pub fn disjointBacking(self: *const Scope, pointer: DevicePointer, bytes: usize) bool {
        if (!self.isBound() or (!self.empty_backing and self.bound_start == 0) or pointer == 0 or bytes == 0) return false;
        const end = std.math.add(DevicePointer, pointer, bytes) catch return false;
        return self.empty_backing or end <= self.bound_start or pointer >= self.bound_end;
    }

    pub fn owns(self: *const Scope, pointer: DevicePointer) bool {
        if (!self.isBound()) return false;
        const peak = if (self.requires_published_output) self.recipe.bound.scratch_peak else self.recipe.bound.peak;
        return (self.published_ptr != 0 and pointer == self.published_ptr) or
            (pointer >= self.base and pointer - self.base < peak);
    }
    fn offset(self: *const Scope, slot: u14) DevicePointer {
        const allocation = self.recipe.bound.allocations[slot];
        return if (self.requires_published_output) allocation.lifetime.external_offset else allocation.offset();
    }

    pub fn allocate(self: *Scope, bytes: usize) !DevicePointer {
        return self.allocateRole(bytes, false);
    }

    pub fn allocateScratch(self: *Scope, bytes: usize) !DevicePointer {
        return self.allocateRole(bytes, true);
    }

    fn allocateRole(self: *Scope, bytes: usize, operation_scratch: bool) !DevicePointer {
        // Observation scopes register real allocations through Plan.allocate.
        // Only an admitted backing may issue addresses from a sealed recipe.
        if (!self.isBound() or !self.valid or self.cursor >= self.recipe.bound.event_count or
            (self.requires_published_output and self.published_ptr == 0))
        {
            self.valid = false;
            return error.InvalidExecutionPlan;
        }
        const e = self.recipe.bound.events[self.cursor];
        if (!e.allocate or e.operation_scratch != operation_scratch or self.recipe.bound.allocations[e.slot].bytes != bytes) {
            self.valid = false;
            return error.InvalidExecutionPlan;
        }
        self.cursor += 1;
        if (self.published_ptr != 0 and self.recipe.bound.output == e.slot) return self.published_ptr;
        return self.base + self.offset(e.slot);
    }
    pub fn release(self: *Scope, pointer: DevicePointer) void {
        if (!self.isBound()) {
            self.recipe.observing.release(pointer);
            return;
        }
        if (!self.valid or self.cursor == self.recipe.bound.event_count) {
            self.valid = false;
            return;
        }
        const e = self.recipe.bound.events[self.cursor];
        if (e.allocate or self.base + self.offset(e.slot) != pointer) {
            self.valid = false;
            return;
        }
        self.cursor += 1;
    }
};

test "stream workspace rejects live buffers and event drift" {
    const p = try std.testing.allocator.create(Plan);
    defer std.testing.allocator.destroy(p);
    p.* = .{};
    p.allocate(256, 1000);
    try std.testing.expect(!p.finishNoOutput());
    p.* = .{};
    p.allocate(256, 1000);
    p.allocate(512, 2000);
    p.release(1000);
    p.allocate(256, 3000);
    p.release(2000);
    p.release(3000);
    try std.testing.expect(p.finishNoOutput());
    try std.testing.expectEqual(@as(usize, 768), p.peak);
    var open_plan: Plan = .{};
    var observing = Scope.observe(&open_plan);
    try std.testing.expect(!observing.owns(0x1000));
    try std.testing.expectError(error.InvalidExecutionPlan, observing.bindPublishedOutput(0x1000, alignment));
    try std.testing.expectError(error.InvalidExecutionPlan, observing.allocate(256));
    try std.testing.expectEqual(@as(usize, 0), observing.cursor);
    var scope = Scope{ .recipe = .{ .bound = p }, .base = 10000 };
    try std.testing.expect(!scope.disjointBacking(0x1000, alignment));
    const a = try scope.allocate(256);
    const b = try scope.allocate(512);
    scope.release(a);
    const c = try scope.allocate(256);
    try std.testing.expectEqual(a, c);
    scope.release(b);
    scope.release(c);
    try std.testing.expect(scope.complete());
    try std.testing.expectError(error.InvalidExecutionPlan, scope.allocate(256));
    try std.testing.expect(!scope.complete());
    var changed = Scope{ .recipe = .{ .bound = p }, .base = 10000 };
    _ = try changed.allocate(256);
    try std.testing.expectError(error.InvalidExecutionPlan, changed.allocate(256));
}

test "stream workspace rejects duplicate live addresses and accepts retired address reuse" {
    var plan: Plan = .{};
    plan.allocate(256, 1024);
    plan.allocate(256, 1024);
    try std.testing.expect(!plan.finish(1024));
    plan = .{};
    plan.allocate(256, 1024);
    plan.release(1024);
    plan.allocate(256, 1024);
    try std.testing.expect(plan.finish(1024));
}

test "sealed workspace identity excludes learning addresses and distinguishes geometry" {
    var plan: Plan = .{};
    plan.allocate(256, 1024);
    try std.testing.expect(plan.finish(1024));
    const identity = plan.fingerprint(17);
    plan = .{};
    plan.allocate(256, 2048);
    try std.testing.expect(plan.finish(2048));
    try std.testing.expectEqual(identity, plan.fingerprint(17));
    plan = .{};
    plan.allocate(512, 2048);
    try std.testing.expect(plan.finish(2048));
    try std.testing.expect(identity != plan.fingerprint(17));
}

test "declared scratch retains its role independently of allocation geometry" {
    var declared: Plan = .{};
    var learned: Plan = .{};
    try std.testing.expectError(error.InvalidExecutionPlan, declared.declareScratch(0));
    try declared.declareScratch(257);
    try std.testing.expectError(error.InvalidExecutionPlan, declared.declareScratch(64));
    learned.allocate(257, 1024);
    learned.release(1024);
    try std.testing.expect(learned.finishNoOutput());
    const scratch = try declared.layout(0);
    const ordinary = try learned.layout(0);
    try std.testing.expectEqual(ordinary.bytes, scratch.bytes);
    try std.testing.expect(learned.fingerprint(17) != declared.fingerprint(17));
    var wrong_role = try Scope.bind(&declared, scratch, 0x1000, scratch.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, wrong_role.allocate(257));
    try std.testing.expectEqual(@as(usize, 0), wrong_role.cursor);
    var wrong_scratch = try Scope.bind(&learned, ordinary, 0x1000, ordinary.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, wrong_scratch.allocateScratch(257));
    try std.testing.expectEqual(@as(usize, 0), wrong_scratch.cursor);
    var bound = try Scope.bind(&declared, scratch, 0x1000, scratch.bytes);
    bound.release(try bound.allocateScratch(257));
    try std.testing.expect(bound.complete());
}

test "declared CUDA workspace reuses dead slots and redirects its sole published output" {
    var plan: Plan = .{};
    const recipe: []const Plan.Declaration = &.{
        .{ .allocate = 64 },
        .{ .allocate = 192 },
        .{ .release = 0 },
        .{ .allocate = 128 },
        .{ .release = 1 },
    };
    try std.testing.expectError(error.InvalidExecutionPlan, plan.declare(recipe, 0));
    try std.testing.expectError(error.InvalidExecutionPlan, plan.declare(recipe, null));
    try plan.declare(recipe, 2);
    const layout = try plan.layoutWithExternalOutput(alignment);
    var scope = try Scope.bind(&plan, layout, 0x1000, layout.bytes);
    try scope.bindPublishedOutput(0x3000, alignment);
    const first = try scope.allocate(64);
    const second = try scope.allocate(192);
    scope.release(first);
    try std.testing.expectEqual(@as(DevicePointer, 0x3000), try scope.allocate(128));
    scope.release(second);
    try std.testing.expect(scope.complete());

    var learned: Plan = .{};
    learned.allocate(64, 0x1000);
    learned.allocate(192, 0x2000);
    learned.release(0x1000);
    learned.allocate(128, 0x1000);
    learned.release(0x2000);
    try std.testing.expect(learned.finish(0x1000));
    try std.testing.expectEqual(try learned.layoutWithExternalOutput(alignment), layout);
    try std.testing.expectEqual(learned.fingerprint(0), plan.fingerprint(0));
    try std.testing.expectEqual(Origin.observed, learned.origin);
    try std.testing.expectEqual(Origin.declared, plan.origin);

    var composed: Plan = .{};
    const builder = try composed.declarationBuilder();
    const intermediate = try builder.allocate(64);
    const residual = try builder.allocate(192);
    try builder.release(intermediate);
    const publication = try builder.allocate(128);
    try builder.release(residual);
    try builder.finish(publication);
    try std.testing.expectEqual(layout, try composed.layoutWithExternalOutput(alignment));
    try std.testing.expectEqual(plan.fingerprint(0), composed.fingerprint(0));
}

test "an operation can declare scratch inside an observed workspace" {
    var plan: Plan = .{};
    var scope = Scope.observe(&plan);
    scope.recordAllocation(64, 0x1000);
    scope.recordScratchInterval(512);
    scope.release(0x1000);
    scope.recordAllocation(128, 0x1000);
    try std.testing.expect(plan.finish(0x1000));
    try std.testing.expectEqual(Origin.observed, plan.origin);
    try std.testing.expectEqual(@as(usize, 1), plan.declared_operation_scratch_intervals);

    var declared: Plan = .{};
    const builder = try declared.declarationBuilder();
    const declared_input = try builder.allocate(64);
    try builder.scratchInterval(512);
    try builder.release(declared_input);
    const publication = try builder.allocate(128);
    try builder.finish(publication);
    try std.testing.expectEqual(Origin.declared, declared.origin);
    try std.testing.expectEqual(plan.declared_operation_scratch_intervals, declared.declared_operation_scratch_intervals);
    try std.testing.expectEqual(try plan.layout(0), try declared.layout(0));

    const layout = try plan.layout(0);
    var bound = try Scope.bind(&plan, layout, 0x2000, layout.bytes);
    const input = try bound.allocate(64);
    const scratch = try bound.allocateScratch(512);
    try std.testing.expectEqual(@as(DevicePointer, 0x2100), scratch);
    bound.release(scratch);
    bound.release(input);
    try std.testing.expectEqual(input, try bound.allocate(128));
    try std.testing.expect(bound.complete());
}

test "observed operation scratch cannot alias a concurrently live token" {
    var plan: Plan = .{};
    var observing = Scope.observe(&plan);
    observing.recordScratchAllocation(256, 0x1000);
    observing.recordAllocation(4, 0x2000);
    observing.release(0x1000);
    try std.testing.expect(plan.finish(0x2000));

    var declared: Plan = .{};
    const builder = try declared.declarationBuilder();
    const scratch_slot = try builder.allocateScratch(256);
    const token_slot = try builder.allocate(4);
    try builder.release(scratch_slot);
    try builder.finish(token_slot);
    const layout = try declared.layout(0);
    try std.testing.expectEqual(try plan.layout(0), layout);
    var bound = try Scope.bind(&declared, layout, 0x3000, layout.bytes);
    const scratch = try bound.allocateScratch(256);
    const token = try bound.allocate(4);
    try std.testing.expect(token >= scratch + 256);
    bound.release(scratch);
    try std.testing.expect(bound.complete());
}

test "stream workspace keeps declared slots separate from observed addresses" {
    var plan: Plan = .{};
    const builder = try plan.declarationBuilder();
    try std.testing.expectError(error.InvalidExecutionPlan, plan.declarationBuilder());
    try std.testing.expect(!plan.finishNoOutput());
    const slot = try builder.allocate(64);
    plan.release(@as(u64, slot) + 1);
    try std.testing.expectError(error.InvalidExecutionPlan, builder.finish(slot));

    plan = .{};
    try std.testing.expectError(error.InvalidExecutionPlan, builder.allocate(64));
    const fresh = try plan.declarationBuilder();
    plan.allocate(64, 0x1000);
    try std.testing.expectError(error.InvalidExecutionPlan, fresh.finish(null));
}

test "CUDA arena preserves device addresses wider than a 32-bit host pointer" {
    var plan: Plan = .{};
    const address: u64 = 0x1_0000_1000;
    plan.allocate(256, address);
    try std.testing.expect(plan.finish(address));
    var scope: Scope = .{ .recipe = .{ .bound = &plan }, .base = address };
    const allocated = try scope.allocate(256);
    try std.testing.expectEqual(address, allocated);
    try std.testing.expect(scope.owns(allocated));
}

test "stream workspace redirects only the sealed publication slot" {
    var plan: Plan = .{};
    plan.allocate(64, 0x1000);
    plan.release(0x1000);
    plan.allocate(64, 0x2000);
    try std.testing.expect(plan.finish(0x2000));
    for (plan.allocations[0..plan.count]) |allocation| try std.testing.expectEqual(@as(DevicePointer, 0), allocation.lifetime.live_id);
    const layout = try plan.layout(alignment);
    var scope = try Scope.bind(&plan, layout, 0x1000, layout.bytes + alignment);
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1000, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1100, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1200, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x2000, 32));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0xf00, 512));
    try scope.bindPublishedOutput(0x2000, alignment);
    try std.testing.expectEqual(@as(DevicePointer, 0x1100), try scope.allocate(64));
    scope.release(0x1100);
    try std.testing.expectEqual(@as(DevicePointer, 0x2000), try scope.allocate(64));
    try std.testing.expect(scope.complete());
    scope.release(0x2000);
    try std.testing.expect(!scope.complete());
}

test "stream workspace binds the exact sealed layout and retains its published span" {
    var plan: Plan = .{};
    try std.testing.expectError(error.InvalidExecutionPlan, plan.layout(0));
    plan.allocate(4, 1000);
    try std.testing.expect(plan.finish(1000));
    const layout = try plan.layout(alignment);
    try std.testing.expectEqual(2 * alignment, layout.bytes);
    try std.testing.expectEqual(alignment, layout.output.?.offset);
    try std.testing.expectEqual(@as(usize, 4), layout.output.?.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, plan.layout(1));
    try std.testing.expectError(error.InvalidExecutionPlan, Scope.bind(&plan, layout, 0x1000, layout.bytes - 1));
    var scope = try Scope.bind(&plan, layout, 0x1000, layout.bytes);
    try std.testing.expectEqual(@as(DevicePointer, 0x1100), try scope.allocate(4));
    plan.release(1000);
    try std.testing.expectEqualDeep(layout, try plan.layout(alignment));
}

test "stream workspace refuses to bind a layout admitted for different events" {
    var admitted: Plan = .{};
    try admitted.declareScratch(1024);
    const layout_ = try admitted.layout(0);
    var replacement: Plan = .{};
    try replacement.declareScratch(1023);
    const changed = try replacement.layout(0);
    try std.testing.expectEqual(layout_.bytes, changed.bytes);
    try std.testing.expectEqual(layout_.events, changed.events);
    try std.testing.expect(layout_.recipe != changed.recipe);
    try std.testing.expectError(error.InvalidExecutionPlan, Scope.bind(&replacement, layout_, 0x1000, layout_.bytes));
    _ = try Scope.bind(&admitted, layout_, 0x1000, layout_.bytes);
}

test "stream workspace can exclude its terminal output from the scratch backing" {
    var plan: Plan = .{};
    plan.allocate(256, 0x1000);
    plan.allocate(256, 0x1100);
    plan.release(0x1000);
    try std.testing.expect(plan.finish(0x1100));
    const layout = try plan.layoutWithExternalOutput(alignment);
    try std.testing.expectEqual(2 * alignment, layout.bytes);
    try std.testing.expectEqual(@as(usize, 256), layout.output.?.bytes);
    var unbound = try Scope.bind(&plan, layout, 0x2000, layout.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, unbound.allocate(256));
    try std.testing.expectEqual(@as(usize, 0), unbound.cursor);
    var scope = try Scope.bind(&plan, layout, 0x2000, layout.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x2100, 256));
    try scope.bindPublishedOutput(0x3000, 256);
    const scratch = try scope.allocate(256);
    try std.testing.expectEqual(@as(DevicePointer, 0x2100), scratch);
    try std.testing.expectEqual(@as(DevicePointer, 0x3000), try scope.allocate(256));
    scope.release(scratch);
    try std.testing.expect(!scope.owns(0x2200));
    try std.testing.expect(scope.complete());
}

test "stream workspace keeps the shared layout when a separate output would exceed the limit" {
    var plan: Plan = .{};
    plan.allocate(64, 0x1000);
    plan.release(0x1000);
    plan.allocate(64, 0x2000);
    try std.testing.expect(plan.finish(0x2000));
    const prefix = limit - alignment;
    try std.testing.expectError(error.InvalidExecutionPlan, plan.layoutWithExternalOutput(prefix));
    const shared = try plan.layout(prefix);
    try std.testing.expectEqual(limit, shared.bytes);
    try std.testing.expectEqual(prefix, shared.output.?.offset);
}

test "stream workspace binds a declared zero-scratch recipe without a device address" {
    var plan: Plan = .{};
    try plan.declare(&.{}, null);
    const layout = try plan.layout(0);
    try std.testing.expectEqual(@as(usize, 0), layout.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, Scope.bind(&plan, layout, alignment, alignment));
    var scope = try Scope.bind(&plan, layout, 0, 0);
    try std.testing.expect(scope.complete());
    try std.testing.expect(!scope.owns(alignment));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.allocate(alignment));
    try std.testing.expect(!scope.complete());
}

test "declared stream workspace can publish into its only backing" {
    var plan: Plan = .{};
    try plan.declare(&.{.{ .allocate = 64 }}, 0);
    const layout = try plan.layoutWithExternalOutput(0);
    try std.testing.expectEqual(@as(usize, 0), layout.bytes);
    var scope = try Scope.bind(&plan, layout, 0, 0);
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0, alignment));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1000, 63));
    try scope.bindPublishedOutput(0x1000, alignment);
    try std.testing.expectEqual(@as(DevicePointer, 0x1000), try scope.allocate(64));
    try std.testing.expect(scope.complete());
    try std.testing.expect(scope.owns(0x1000));
}

test "stream workspace chooses the earliest gap among out-of-order slots" {
    var plan: Plan = .{};
    try plan.declare(&.{
        .{ .allocate = 512 },
        .{ .allocate = 256 },
        .{ .release = 0 },
        .{ .allocate = 256 },
        .{ .allocate = 256 },
        .{ .release = 1 },
        .{ .release = 3 },
        .{ .release = 2 },
    }, null);
    try std.testing.expectEqual(@as(usize, 768), plan.peak);
    try std.testing.expectEqual(@as(u32, 0), plan.allocations[2].offset());
    try std.testing.expectEqual(@as(u32, 256), plan.allocations[3].offset());
}

test "external workspace packs scratch across a live inline publication" {
    var plan: Plan = .{};
    try plan.declare(&.{
        .{ .allocate = 256 },
        .{ .allocate = 2048 },
        .{ .release = 0 },
        .{ .allocate = 512 },
        .{ .release = 2 },
    }, 1);
    const inline_layout = try plan.layout(0);
    const external = try plan.layoutWithExternalOutput(0);
    try std.testing.expectEqual(@as(usize, 2816), inline_layout.bytes);
    try std.testing.expectEqual(@as(usize, 512), external.bytes);
    try std.testing.expectEqual(@as(usize, 2304), plan.external_scratch_savings_bytes);
    var scope = try Scope.bind(&plan, external, 0x1000, external.bytes);
    try scope.bindPublishedOutput(0x2000, 2048);
    try std.testing.expectEqual(@as(u64, 0x1000), try scope.allocate(256));
    try std.testing.expectEqual(@as(u64, 0x2000), try scope.allocate(2048));
    scope.release(0x1000);
    try std.testing.expectEqual(@as(u64, 0x1000), try scope.allocate(512));
    scope.release(0x1000);
    try std.testing.expect(scope.complete());
}

test "external workspace retains inline offsets when repacking increases peak" {
    var plan: Plan = .{};
    try plan.declare(&.{
        .{ .allocate = 5 * alignment },
        .{ .allocate = alignment },
        .{ .release = 0 },
        .{ .allocate = 3 * alignment },
        .{ .allocate = 3 * alignment },
        .{ .release = 2 },
        .{ .allocate = 4 * alignment },
        .{ .release = 3 },
        .{ .release = 4 },
    }, 1);
    const layout = try plan.layoutWithExternalOutput(0);
    try std.testing.expectEqual(9 * alignment, layout.bytes);
    try std.testing.expectEqual(@as(usize, 0), plan.external_scratch_savings_bytes);
    var scope = try Scope.bind(&plan, layout, 0x1000, layout.bytes);
    try scope.bindPublishedOutput(0x2000, alignment);
    const first = try scope.allocate(5 * alignment);
    _ = try scope.allocate(alignment);
    scope.release(first);
    const second = try scope.allocate(3 * alignment);
    const third = try scope.allocate(3 * alignment);
    try std.testing.expectEqual(@as(u64, 0x1600), third);
    scope.release(second);
    const fourth = try scope.allocate(4 * alignment);
    scope.release(third);
    scope.release(fourth);
    try std.testing.expect(scope.complete());
}
