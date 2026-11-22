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
};
const Allocation = struct { bytes: usize, offset: usize, pointer: DevicePointer, live: bool = true };
const Event = struct { slot: usize, allocate: bool };
pub const Plan = struct {
    allocations: [2048]Allocation = undefined,
    events: [4096]Event = undefined,
    count: usize = 0,
    event_count: usize = 0,
    peak: usize = 0,
    output: ?usize = null,
    sealed: bool = false,
    valid: bool = true,
    fn event(self: *Plan, slot: usize, is_alloc: bool) void {
        if (self.event_count == self.events.len) {
            self.valid = false;
            return;
        }
        self.events[self.event_count] = .{ .slot = slot, .allocate = is_alloc };
        self.event_count += 1;
    }
    pub fn allocate(self: *Plan, bytes: usize, pointer: DevicePointer) void {
        if (!self.valid or self.sealed) return;
        if (self.count == self.allocations.len or bytes > limit) {
            self.valid = false;
            return;
        }
        const extent = std.mem.alignForward(usize, bytes, alignment);
        var offset: usize = 0;
        while (true) {
            var next = offset;
            for (self.allocations[0..self.count]) |a| if (a.live and offset < a.offset + std.mem.alignForward(usize, a.bytes, alignment) and a.offset < offset + extent) {
                next = @max(next, a.offset + std.mem.alignForward(usize, a.bytes, alignment));
            };
            if (next == offset) break;
            offset = next;
        }
        if (offset > limit - extent) {
            self.valid = false;
            return;
        }
        self.allocations[self.count] = .{ .bytes = bytes, .offset = offset, .pointer = pointer };
        self.event(self.count, true);
        self.count += 1;
        self.peak = @max(self.peak, offset + extent);
    }
    pub fn release(self: *Plan, pointer: DevicePointer) void {
        if (!self.valid or self.sealed) return;
        for (self.allocations[0..self.count], 0..) |*a, i| if (a.live and a.pointer == pointer) {
            a.live = false;
            self.event(i, false);
            return;
        };
    }
    pub fn finishNoOutput(self: *Plan) bool {
        return self.seal(null);
    }

    pub fn finish(self: *Plan, pointer: DevicePointer) bool {
        return self.seal(pointer);
    }

    fn seal(self: *Plan, expected: ?DevicePointer) bool {
        if (self.sealed) return false;
        for (self.allocations[0..self.count], 0..) |allocation, index| {
            if (!allocation.live) continue;
            if (expected != allocation.pointer or self.output != null) self.valid = false;
            self.output = index;
        }
        self.valid = self.valid and (expected == null) == (self.output == null);
        self.sealed = self.valid;
        return self.valid;
    }

    pub fn layout(self: *const Plan, prefix_bytes: usize) !Layout {
        if (!self.sealed or !self.valid or self.peak == 0 or prefix_bytes % alignment != 0 or
            prefix_bytes > limit or self.peak > limit - prefix_bytes) return error.InvalidExecutionPlan;
        const output = if (self.output) |index| self.allocations[index] else null;
        return .{
            .bytes = prefix_bytes + self.peak,
            .scratch_offset = prefix_bytes,
            .events = self.event_count,
            .output = if (output) |region| .{ .offset = prefix_bytes + region.offset, .bytes = region.bytes } else null,
        };
    }
};
pub const Scope = struct {
    plan: *Plan,
    base: DevicePointer = 0,
    cursor: usize = 0,
    valid: bool = true,
    // A caller-owned output can outlive individual arena allocations without
    // joining the ordinary temporary cache. Only its explicit producer may
    // borrow it; release remains valid through this complete stream scope.
    output_ptr: DevicePointer = 0,
    output_bytes: usize = 0,
    output_active: bool = false,
    output_taken: bool = false,
    published_ptr: DevicePointer = 0,
    bound_start: DevicePointer = 0,
    bound_end: DevicePointer = 0,
    /// Bind the funded byte range before recording or submitting any kernels.
    pub fn bind(plan: *Plan, layout: Layout, backing: DevicePointer, backing_bytes: usize) !Scope {
        if (backing == 0 or backing % alignment != 0 or layout.bytes > backing_bytes or
            backing > @as(DevicePointer, std.math.maxInt(DevicePointer)) - @as(DevicePointer, backing_bytes) or
            !std.meta.eql(layout, try plan.layout(layout.scratch_offset))) return error.InvalidExecutionPlan;
        return .{ .plan = plan, .base = backing + layout.scratch_offset, .bound_start = backing, .bound_end = backing + backing_bytes };
    }

    pub fn complete(self: *const Scope) bool {
        return self.base != 0 and self.valid and self.cursor == self.plan.event_count;
    }

    /// Ordinary execution may redirect the sealed output slot to independently
    /// owned storage. Capture continues to use the stable arena address.
    pub fn bindPublishedOutput(self: *Scope, pointer: DevicePointer, bytes: usize) !void {
        const slot = self.plan.output orelse return error.InvalidExecutionPlan;
        if (!self.valid or self.cursor != 0 or self.published_ptr != 0 or pointer % alignment != 0 or
            bytes != self.plan.allocations[slot].bytes or !self.disjointBacking(pointer, bytes)) return error.InvalidExecutionPlan;
        self.published_ptr = pointer;
    }

    pub fn disjointBacking(self: *const Scope, pointer: DevicePointer, bytes: usize) bool {
        if (self.bound_start == 0 or pointer == 0 or bytes == 0) return false;
        const end = std.math.add(DevicePointer, pointer, bytes) catch return false;
        return end <= self.bound_start or pointer >= self.bound_end;
    }

    pub fn owns(self: *const Scope, pointer: DevicePointer) bool {
        return self.base != 0 and ((self.published_ptr != 0 and pointer == self.published_ptr) or
            (pointer >= self.base and pointer - self.base < self.plan.peak));
    }
    pub fn allocate(self: *Scope, bytes: usize) !DevicePointer {
        if (!self.valid or self.cursor == self.plan.event_count) return error.AudioPlanChanged;
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
        if (self.base == 0) {
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

test "LightOnOCR3 CUDA closed execution arena rejects live buffers and event drift" {
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
    var changed = Scope{ .plan = p, .base = 10000 };
    _ = try changed.allocate(256);
    try std.testing.expectError(error.AudioPlanChanged, changed.allocate(256));
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
    const layout = try plan.layout(alignment);
    var scope = try Scope.bind(&plan, layout, 0x1000, layout.bytes + alignment);
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1000, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1100, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x1200, 64));
    try std.testing.expectError(error.InvalidExecutionPlan, scope.bindPublishedOutput(0x2000, 32));
    try scope.bindPublishedOutput(0x2000, 64);
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
