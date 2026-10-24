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

    pub fn create(allocator: std.mem.Allocator, bytes: []u8) !*SharedBytes {
        const self = try allocator.create(SharedBytes);
        self.* = .{ .allocator = allocator, .bytes = bytes };
        return self;
    }
    pub fn registerRetirementWithAdmission(self: *SharedBytes, construction: *resources.ConstructionCredit, control: @import("../../common/read_admission.zig").Port) !void {
        try construction.manager.registerPayloadRetirementWithAdmission(&self.retirement, &self.reservation, construction, self, destroyRetired, control);
    }
    pub fn enableRetirementWithAdmission(self: *SharedBytes, control: @import("../../common/read_admission.zig").Port) !void {
        if (self.retirement.credit != null) return;
        if (self.reservation) |*credit| try credit.manager.registerExistingPayloadRetirementWithAdmission(&self.retirement, credit, self, destroyRetired, control);
    }
    fn destroyRetired(raw: *anyopaque) void {
        const self: *SharedBytes = @ptrCast(@alignCast(raw));
        const allocator = self.allocator;
        allocator.free(self.bytes);
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
