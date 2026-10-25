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

const std = @import("std");
const platform = @import("antfly_platform");
const resources = @import("../resource_manager.zig");

/// Cache ownership and cursor ownership are independent. Eviction drops only
/// the cache reference; outstanding scans retain immutable bytes.
pub const SharedBytes = struct {
    allocator: std.mem.Allocator,
    refs: platform.atomic.Value(usize) = .init(1),
    bytes: []u8,
    /// Assigned before publication; credit follows the payload, not cache membership.
    reservation: ?@import("../resource_manager.zig").Reservation = null,
    retirement: resources.PayloadRetirement = .{},
    cache_admitted: bool = false,
    result_pins_allowed: bool = true,
    retirement_offset: usize = 0,
    data_retired: bool = false,

    pub fn create(allocator: std.mem.Allocator, bytes: []u8) !*SharedBytes {
        const self = try allocator.create(SharedBytes);
        self.* = .{ .allocator = allocator, .bytes = bytes };
        return self;
    }
    pub fn registerRetirementWithAdmission(self: *SharedBytes, construction: *resources.ConstructionCredit, control: @import("../../common/read_admission.zig").Port) !void {
        try construction.manager.registerResumablePayloadRetirementWithAdmission(&self.retirement, &self.reservation, construction, self, .{ .advance = retireBytes }, destroyRetired, control);
    }
    pub fn enableRetirementWithAdmission(self: *SharedBytes, control: @import("../../common/read_admission.zig").Port) !void {
        if (self.retirement.credit != null) return;
        if (self.reservation) |*credit| try credit.manager.registerExistingResumablePayloadRetirementWithAdmission(&self.retirement, credit, self, .{ .advance = retireBytes }, destroyRetired, control);
    }
    /// One byte quantum per admitted manager pass. The last reference has
    /// already transferred ownership, so no reader can observe poisoned bytes.
    fn retireBytes(raw: *anyopaque, control: ?@import("../../common/read_admission.zig").Port) !bool {
        const self: *SharedBytes = @ptrCast(@alignCast(raw));
        if (control) |original| try original.checkpoint();
        if (self.data_retired) return true;
        while (self.retirement_offset < self.bytes.len) {
            if (control) |original| try original.checkpoint();
            const end = self.retirement_offset + @min(self.bytes.len - self.retirement_offset, 16 * 1024);
            @memset(self.bytes[self.retirement_offset..end], undefined);
            self.retirement_offset = end;
            if (control != null and end < self.bytes.len) return false;
        }
        if (control) |original| try original.checkpoint();
        if (self.bytes.len != 0) self.allocator.rawFree(self.bytes, .of(u8), @returnAddress());
        self.bytes = &.{};
        self.data_retired = true;
        return true;
    }
    fn destroyRetired(raw: *anyopaque) void {
        const self: *SharedBytes = @ptrCast(@alignCast(raw));
        std.debug.assert(self.data_retired);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
    pub fn retain(self: *SharedBytes) *SharedBytes {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *SharedBytes) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            if (self.retirement.credit != null) {
                self.retirement.publish();
                return;
            }
            const allocator = self.allocator;
            allocator.free(self.bytes);
            var reservation = self.reservation;
            allocator.destroy(self);
            if (reservation) |*credit| credit.release();
        }
    }
};

test "local block lease survives cache eviction without a payload copy" {
    const a = std.testing.allocator;
    const cache = try SharedBytes.create(a, try a.dupe(u8, "immutable decoded block"));
    const cursor = cache.retain();
    const original = cache.bytes.ptr;
    cache.release();
    defer cursor.release();
    try std.testing.expectEqual(original, cursor.bytes.ptr);
    try std.testing.expectEqualStrings("immutable decoded block", cursor.bytes);
}

const RetirementProbe = struct {
    calls: usize = 0,
    reject_at: usize = std.math.maxInt(usize),
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.calls;
        self.calls += 1;
        if (at == self.reject_at) return error.DeadlineExceeded;
    }
    fn payload(self: *@This()) @import("../../common/read_admission.zig").Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .wait_for_progress = check } };
    }
};
fn exerciseRegisteredBytes(target: usize, upgrade: bool) !usize {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    var probe: RetirementProbe = .{};
    const payload = probe.payload();
    const original = admission.Port.init(&payload);
    const length = 2 * 16 * 1024 + 7;
    const cost = @sizeOf(SharedBytes) + length;
    var construction = try manager.reserveConstructionWithAdmission(.lsm_block_table_cache, cost, original);
    defer construction.retire();
    const Witness = struct {
        header_freed: bool = false,
        data_freed: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (memory.len == @sizeOf(SharedBytes)) self.header_freed = true else self.data_freed = true;
            std.testing.allocator.rawFree(memory, alignment, ra);
        }
    };
    var witness: Witness = .{};
    const backing = witness.allocator();
    const bytes = try backing.alloc(u8, length);
    const owner = try SharedBytes.create(backing, bytes);
    if (upgrade) {
        owner.reservation = construction.transfer();
        try owner.enableRetirementWithAdmission(original);
    } else try owner.registerRetirementWithAdmission(&construction, original);
    const reader = owner.retain();
    owner.release();
    try std.testing.expectEqual(@as(usize, 0), try manager.drainPayloadRetirementWithAdmission(original));
    try std.testing.expectEqual(@as(usize, 0), reader.retirement_offset);
    reader.release();
    probe.calls = 0;
    probe.reject_at = if (target == 0) std.math.maxInt(usize) else target - 1;
    var rejected = false;

    var calls: usize = 0;
    var prior: usize = 0;
    while (!witness.header_freed) {
        const result = manager.drainPayloadRetirementWithAdmission(original);
        if (result) |_| {} else |err| {
            try std.testing.expectEqual(error.DeadlineExceeded, err);
            rejected = true;
            calls = probe.calls;
            probe.reject_at = std.math.maxInt(usize);
        }
        if (!witness.header_freed) {
            try std.testing.expect(owner.retirement_offset >= prior);
            try std.testing.expect(owner.retirement_offset - prior <= 16 * 1024);
            prior = owner.retirement_offset;
            try std.testing.expectEqual(@as(u64, cost), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        }
    }
    const drain_calls = probe.calls;
    if (target != 0) try std.testing.expect(rejected);
    probe.reject_at = std.math.maxInt(usize);
    try std.testing.expect(witness.data_freed);
    try manager.drainConstructionCleanupWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
    return if (target == 0) drain_calls else calls;
}

test "controlled physical sync resumable payload decoded bytes retain every rejected quantum and physical charge" {
    for ([_]bool{ false, true }) |upgrade| {
        const calls = try exerciseRegisteredBytes(0, upgrade);
        for (1..calls + 1) |target| _ = try exerciseRegisteredBytes(target, upgrade);
    }
}
