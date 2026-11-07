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

//! Independent admission metadata. Pins preserve controller lifetime only;
//! they confer no authority to admit work after a model or manager closes.
//! No manager arena, model, provider, or offline IO is retained here.
const std = @import("std");
const platform = @import("antfly_platform");
const memory = @import("memory.zig");

pub const Domain = struct {
    allocator: std.mem.Allocator,
    controller: memory.AdmissionController = .{},
    references: std.atomic.Value(usize) = .init(1),

    /// Native targets use libc; other targets use their independent process
    /// heap. In particular this never borrows a model manager's allocator.
    pub fn create() !*Domain {
        return createWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()));
    }
    fn createWithAllocator(allocator: std.mem.Allocator) !*Domain {
        const self = try allocator.create(Domain);
        self.* = .{ .allocator = allocator, .controller = .{ .metadata_allocator = allocator } };
        return self;
    }
    pub fn retain(self: *Domain) *Domain {
        var current = self.references.load(.monotonic);
        while (true) {
            if (current == 0 or current == std.math.maxInt(usize)) @panic("invalid admission domain reference");
            current = self.references.cmpxchgWeak(current, current + 1, .monotonic, .monotonic) orelse return self;
        }
    }
    pub fn release(self: *Domain) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        // The final pin follows actual lease retirement, not merely a logical
        // output drop. AdmissionController rejects any active resources here.
        self.controller.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }
    pub fn pin(self: *Domain) Pin {
        return .{ .owner = self.retain() };
    }
};

pub const Pin = struct {
    owner: *Domain,
    pub fn retain(self: Pin) Pin {
        return self.owner.pin();
    }
    pub fn release(self: Pin) void {
        self.owner.release();
    }
    pub fn controller(self: Pin) *memory.AdmissionController {
        return &self.owner.controller;
    }
};

fn ownedControllerCase(allocator: std.mem.Allocator) !void {
    const owner = try Domain.createWithAllocator(allocator);
    var owner_live = true;
    defer if (owner_live) owner.release();
    const pin = owner.pin();
    defer pin.release();
    var lease = try pin.controller().tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false);
    defer lease.release();
    owner.release();
    owner_live = false;
    try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_scratch_bytes);
    const retained = pin.retain();
    retained.release();
    lease.release();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, pin.controller().snapshot());
}
test "independent admission domain pins preserve exact live leases after original owner release and unwind every allocation failure" {
    try ownedControllerCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, ownedControllerCase, .{});
}
