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

//! A bounded allocation/free trace for stream-owned graph arenas. Offsets
//! overlap only after the preceding consumer is submitted on the same stream.
const std = @import("std");
pub const limit = 128 * 1024 * 1024;
const Allocation = struct { bytes: usize, offset: usize, pointer: usize, live: bool = true };
const Event = struct { slot: usize, allocate: bool };
pub const Plan = struct {
    allocations: [2048]Allocation = undefined,
    events: [4096]Event = undefined,
    count: usize = 0,
    event_count: usize = 0,
    peak: usize = 0,
    output: usize = 0,
    valid: bool = true,
    fn event(self: *Plan, slot: usize, is_alloc: bool) void {
        if (self.event_count == self.events.len) {
            self.valid = false;
            return;
        }
        self.events[self.event_count] = .{ .slot = slot, .allocate = is_alloc };
        self.event_count += 1;
    }
    pub fn allocate(self: *Plan, bytes: usize, pointer: usize) void {
        if (!self.valid) return;
        if (self.count == self.allocations.len or bytes > limit) {
            self.valid = false;
            return;
        }
        const extent = std.mem.alignForward(usize, bytes, 256);
        var offset: usize = 0;
        while (true) {
            var next = offset;
            for (self.allocations[0..self.count]) |a| if (a.live and offset < a.offset + std.mem.alignForward(usize, a.bytes, 256) and a.offset < offset + extent) {
                next = @max(next, a.offset + std.mem.alignForward(usize, a.bytes, 256));
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
    pub fn release(self: *Plan, pointer: usize) void {
        if (!self.valid) return;
        for (self.allocations[0..self.count], 0..) |*a, i| if (a.live and a.pointer == pointer) {
            a.live = false;
            self.event(i, false);
            return;
        };
    }
    pub fn finishNoOutput(self: *Plan) bool {
        for (self.allocations[0..self.count]) |a| if (a.live) {
            self.valid = false;
        };
        return self.valid;
    }
    pub fn finish(self: *Plan, pointer: usize) bool {
        var live: usize = 0;
        for (self.allocations[0..self.count], 0..) |a, i| if (a.live) {
            live += 1;
            if (a.pointer == pointer) self.output = i else self.valid = false;
        };
        self.valid = self.valid and live == 1;
        return self.valid;
    }
};
pub const Scope = struct {
    plan: *Plan,
    base: usize = 0,
    cursor: usize = 0,
    valid: bool = true,
    // A caller-owned output can outlive individual arena allocations without
    // joining the ordinary temporary cache. Only its explicit producer may
    // borrow it; release remains valid through this complete stream scope.
    output_ptr: usize = 0,
    output_bytes: usize = 0,
    output_active: bool = false,
    output_taken: bool = false,
    pub fn owns(self: *const Scope, pointer: usize) bool {
        return self.base != 0 and pointer >= self.base and pointer - self.base < self.plan.peak;
    }
    pub fn allocate(self: *Scope, bytes: usize) !usize {
        if (!self.valid or self.cursor == self.plan.event_count) return error.AudioPlanChanged;
        const e = self.plan.events[self.cursor];
        if (!e.allocate or self.plan.allocations[e.slot].bytes != bytes) {
            self.valid = false;
            return error.AudioPlanChanged;
        }
        self.cursor += 1;
        return self.base + self.plan.allocations[e.slot].offset;
    }
    pub fn release(self: *Scope, pointer: usize) void {
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
    try std.testing.expect(scope.valid and scope.cursor == p.event_count);
    var changed = Scope{ .plan = p, .base = 10000 };
    _ = try changed.allocate(256);
    try std.testing.expectError(error.AudioPlanChanged, changed.allocate(256));
}
