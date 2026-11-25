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
    output: ?struct { offset: usize, bytes: usize },
    external_output: bool = false,
};
// A nonzero address denotes a live allocation during learning. The workspace limit bounds the geometry.

const Allocation = struct { pointer: DevicePointer, bytes: u32, offset: u32 };
// A trace is kept per cached shape. Slot indices fit in the fixed plan and
// never need pointer-width storage or an independently padded flag.
const max_allocations = 2048;
const Event = packed struct(u16) {
    slot: u15,
    allocate: bool,
};

comptime {
    if (limit > std.math.maxInt(u32)) @compileError("stream workspace geometry overflow");

    if (max_allocations - 1 > std.math.maxInt(u15)) @compileError("stream workspace event slot overflow");
}
pub const Plan = struct {
    allocations: [max_allocations]Allocation = undefined,
    events: [max_allocations * 2]Event = undefined,
    count: usize = 0,
    event_count: usize = 0,
    peak: usize = 0,
    scratch_peak: usize = 0,
    output: ?usize = null,
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
        if (!self.valid or self.sealed) return;
        if (self.count == self.allocations.len or bytes == 0 or bytes > limit or pointer == 0) {
            self.valid = false;
            return;
        }
        const extent = std.mem.alignForward(usize, bytes, alignment);
        var offset: usize = 0;
        while (true) {
            var next = offset;
            for (self.allocations[0..self.count]) |a| {
                if (a.pointer == 0) continue;
                if (a.pointer == pointer) {
                    self.valid = false;
                    return;
                }
                const end = a.offset + std.mem.alignForward(usize, a.bytes, alignment);
                if (offset < end and a.offset < offset + extent) next = @max(next, end);
            }
            if (next == offset) break;
            offset = next;
        }
        if (offset > limit - extent) {
            self.valid = false;
            return;
        }
        self.allocations[self.count] = .{ .bytes = @intCast(bytes), .offset = @intCast(offset), .pointer = pointer };
        self.event(self.count, true);
        self.count += 1;
        self.peak = @max(self.peak, offset + extent);
    }
    pub fn release(self: *Plan, pointer: DevicePointer) void {
        if (!self.valid or self.sealed) return;
        for (self.allocations[0..self.count], 0..) |*a, i| if (pointer != 0 and a.pointer == pointer) {
            a.pointer = 0;
            self.event(i, false);
            return;
        };
    }
    /// Declare one non-escaping interval on an empty plan. Both ordinary
    /// execution and recording consume the same allocate/release events.
    pub fn declareScratch(self: *Plan, bytes: usize) !void {
        if (!self.valid or self.sealed or self.event_count != 0 or self.count != 0 or bytes == 0 or bytes > limit)
            return error.InvalidExecutionPlan;
        self.allocations[0] = .{ .pointer = 0, .bytes = @intCast(bytes), .offset = 0 };
        self.count = 1;
        self.event(0, true);
        self.event(0, false);
        self.peak = std.mem.alignForward(usize, bytes, alignment);
        if (!self.finishNoOutput()) return error.InvalidExecutionPlan;
    }

    pub fn finishNoOutput(self: *Plan) bool {
        return self.seal(null);
    }

    pub fn finish(self: *Plan, pointer: DevicePointer) bool {
        return self.seal(pointer);
    }

    fn seal(self: *Plan, expected: ?DevicePointer) bool {
        if (self.sealed) return false;
        for (self.allocations[0..self.count], 0..) |*allocation, index| {
            if (allocation.pointer == 0) continue;
            if (expected != allocation.pointer or self.output != null) self.valid = false;
            // The published layout keeps slot geometry, never a learning-run address.
            allocation.pointer = 0;
            self.output = index;
        }
        self.valid = self.valid and (expected == null) == (self.output == null);
        if (self.valid) for (self.allocations[0..self.count], 0..) |allocation, index| {
            if (self.output == index) continue;
            self.scratch_peak = @max(self.scratch_peak, allocation.offset + std.mem.alignForward(usize, allocation.bytes, alignment));
        };
        self.sealed = self.valid;
        return self.valid;
    }

    pub fn layout(self: *const Plan, prefix_bytes: usize) !Layout {
        return self.layoutImpl(prefix_bytes, false);
    }

    /// Identify the sealed event schedule independently of its backing addresses.
    pub fn fingerprint(self: *const Plan, seed: u64) u64 {
        std.debug.assert(self.sealed);
        var hash = std.hash.Wyhash.init(seed);
        for (self.events[0..self.event_count]) |entry| {
            const encoded: u16 = @bitCast(entry);
            hash.update(std.mem.asBytes(&encoded));
            const allocation = self.allocations[entry.slot];
            hash.update(std.mem.asBytes(&allocation.bytes));
            hash.update(std.mem.asBytes(&allocation.offset));
        }
        return hash.final();
    }

    pub fn layoutWithExternalOutput(self: *const Plan, prefix_bytes: usize) !Layout {
        return self.layoutImpl(prefix_bytes, true);
    }

    fn layoutImpl(self: *const Plan, prefix_bytes: usize, external: bool) !Layout {
        if (!self.sealed or !self.valid or self.peak == 0 or prefix_bytes % alignment != 0 or prefix_bytes > limit) return error.InvalidExecutionPlan;
        const output = if (self.output) |index| self.allocations[index] else null;
        const peak = if (external) self.scratch_peak else self.peak;
        const external_bytes = if (external) std.mem.alignForward(usize, (output orelse return error.InvalidExecutionPlan).bytes, alignment) else 0;
        if (peak > limit - prefix_bytes or external_bytes > limit - prefix_bytes - peak) return error.InvalidExecutionPlan;
        return .{
            .bytes = prefix_bytes + peak,
            .scratch_offset = prefix_bytes,
            .events = self.event_count,
            .output = if (output) |region| .{ .offset = if (external) 0 else prefix_bytes + region.offset, .bytes = region.bytes } else null,
            .external_output = external,
        };
    }
};
pub const Scope = struct {
    plan: *Plan,
    base: DevicePointer = 0,
    cursor: usize = 0,
    valid: bool = true,
    published_ptr: DevicePointer = 0,
    requires_published_output: bool = false,
    bound_start: DevicePointer = 0,
    bound_end: DevicePointer = 0,
    /// Bind the funded byte range before recording or submitting any kernels.
    pub fn bind(plan: *Plan, layout: Layout, backing: DevicePointer, backing_bytes: usize) !Scope {
        if (backing == 0 or backing % alignment != 0 or layout.bytes > backing_bytes or
            backing > @as(DevicePointer, std.math.maxInt(DevicePointer)) - @as(DevicePointer, backing_bytes) or
            !std.meta.eql(layout, try plan.layoutImpl(layout.scratch_offset, layout.external_output))) return error.InvalidExecutionPlan;
        return .{ .plan = plan, .base = backing + layout.scratch_offset, .bound_start = backing, .bound_end = backing + backing_bytes, .requires_published_output = layout.external_output };
    }

    pub inline fn isBound(self: *const Scope) bool {
        return self.base != 0;
    }

    pub fn complete(self: *const Scope) bool {
        return self.isBound() and self.valid and self.cursor == self.plan.event_count;
    }

    /// Bind the actual backing span before execution or capture. The sealed
    /// event retains its exact write size even when the backing is larger.
    pub fn bindPublishedOutput(self: *Scope, pointer: DevicePointer, backing_bytes: usize) !void {
        const slot = self.plan.output orelse return error.InvalidExecutionPlan;
        if (!self.valid or self.cursor != 0 or self.published_ptr != 0 or pointer % alignment != 0 or
            backing_bytes < self.plan.allocations[slot].bytes or !self.disjointBacking(pointer, backing_bytes)) return error.InvalidExecutionPlan;
        self.published_ptr = pointer;
    }

    pub fn disjointBacking(self: *const Scope, pointer: DevicePointer, bytes: usize) bool {
        if (self.bound_start == 0 or pointer == 0 or bytes == 0) return false;
        const end = std.math.add(DevicePointer, pointer, bytes) catch return false;
        return end <= self.bound_start or pointer >= self.bound_end;
    }

    pub fn owns(self: *const Scope, pointer: DevicePointer) bool {
        const peak = if (self.requires_published_output) self.plan.scratch_peak else self.plan.peak;
        return self.isBound() and ((self.published_ptr != 0 and pointer == self.published_ptr) or
            (pointer >= self.base and pointer - self.base < peak));
    }
    pub fn allocate(self: *Scope, bytes: usize) !DevicePointer {
        if (!self.valid or self.cursor == self.plan.event_count) {
            self.valid = false;
            return error.AudioPlanChanged;
        }
        if (self.requires_published_output and self.published_ptr == 0) {
            self.valid = false;
            return error.AudioPlanChanged;
        }
        const e = self.plan.events[self.cursor];
        if (!e.allocate or self.plan.allocations[e.slot].bytes != bytes) {
            self.valid = false;
            return error.AudioPlanChanged;
        }
        self.cursor += 1;
        if (self.published_ptr != 0 and self.plan.output == e.slot) return self.published_ptr;
        return self.base + self.plan.allocations[e.slot].offset;
    }
    pub fn release(self: *Scope, pointer: DevicePointer) void {
        if (!self.isBound()) {
            self.plan.release(pointer);
            return;
        }
        if (!self.valid or self.cursor == self.plan.event_count) {
            self.valid = false;
            return;
        }
        const e = self.plan.events[self.cursor];
        if (e.allocate or self.base + self.plan.allocations[e.slot].offset != pointer) {
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
    var scope = Scope{ .plan = p, .base = 10000 };
    const a = try scope.allocate(256);
    const b = try scope.allocate(512);
    scope.release(a);
    const c = try scope.allocate(256);
    try std.testing.expectEqual(a, c);
    scope.release(b);
    scope.release(c);
    try std.testing.expect(scope.complete());
    try std.testing.expectError(error.AudioPlanChanged, scope.allocate(256));
    try std.testing.expect(!scope.complete());
    var changed = Scope{ .plan = p, .base = 10000 };
    _ = try changed.allocate(256);
    try std.testing.expectError(error.AudioPlanChanged, changed.allocate(256));
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

test "declared scratch is identical to a sealed allocation interval" {
    var declared: Plan = .{};
    var learned: Plan = .{};
    try std.testing.expectError(error.InvalidExecutionPlan, declared.declareScratch(0));
    try declared.declareScratch(257);
    try std.testing.expectError(error.InvalidExecutionPlan, declared.declareScratch(64));
    learned.allocate(257, 1024);
    learned.release(1024);
    try std.testing.expect(learned.finishNoOutput());
    try std.testing.expectEqual(try learned.layout(0), try declared.layout(0));
    try std.testing.expectEqual(learned.fingerprint(17), declared.fingerprint(17));
}

test "CUDA arena preserves device addresses wider than a 32-bit host pointer" {
    var plan: Plan = .{};
    const address: u64 = 0x1_0000_1000;
    plan.allocate(256, address);
    try std.testing.expect(plan.finish(address));
    var scope: Scope = .{ .plan = &plan, .base = address };
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
    for (plan.allocations[0..plan.count]) |allocation| try std.testing.expectEqual(@as(DevicePointer, 0), allocation.pointer);
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
    try std.testing.expectError(error.AudioPlanChanged, unbound.allocate(256));
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
