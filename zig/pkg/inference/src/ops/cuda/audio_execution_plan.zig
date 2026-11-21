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

//! Warm allocation/lifetime trace. Stable offsets reuse only storage whose
//! last consumer has been submitted on the same stream. No request is rerun
//! for planning; capture validates the complete allocation/free event sequence.
const std = @import("std");
const arena = @import("execution_arena.zig");
pub const limit = arena.limit;
pub const Plan = arena.Plan;
pub const Scope = arena.Scope;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
const Quota = @import("cache_quota.zig").Quota;
const retirement = @import("../../execution/resource_retirement.zig");
const platform = @import("antfly_platform");

/// Independently allocated pure metadata. No CUDA/model/Io/epoch is captured.
/// Both this exact backing and its old Domain quota live through actual B.
pub const Owner = struct {
    allocator: std.mem.Allocator,
    plan: Plan = .{},
    quota: Quota = .none,
    retiring: bool = false,
    node: retirement.Node = undefined,

    pub fn createOutsideExecution(a: std.mem.Allocator) !*Owner {
        const self = try a.create(Owner);
        self.* = .{ .allocator = a };
        return self;
    }
    pub fn destroyUnusedOutsideExecution(self: *Owner) void {
        std.debug.assert(!self.retiring and self.quota == .none);
        self.allocator.destroy(self);
    }
    pub fn validateCleanup(self: *const Owner, current: Cleanup) !void {
        if (self.retiring) return error.AudioPlanAlreadyRetired;
        try self.quota.validateCleanup(current);
        if (!current.outside_execution) {
            const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (try epoch.isSealed()) return error.RetirementEpochSealed;
        }
    }
    fn destroyOutside(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var quota = self.quota;
        const a = self.allocator;
        a.destroy(self); // actual metadata death precedes external refund
        quota.releaseOutsideExecution();
    }
    pub fn retire(self: *Owner, current: Cleanup) !void {
        try self.validateCleanup(current);
        self.retiring = true;
        errdefer self.retiring = false;
        if (current.outside_execution) {
            destroyOutside(self);
        } else {
            const boundary = try current.boundary.?.boundary();
            self.node = .{ .context = self, .release_fn = destroyOutside };
            try boundary.register(boundary.context, boundary.nonce, &self.node);
        }
    }
};
pub const Entry = struct { key: u64 = 0, stamp: u64 = 0, warmed: bool = false, disabled: bool = false, plan: ?*Plan = null, owner: ?*Owner = null };
pub const Cache = struct {
    entries: [8]Entry = @splat(.{}),
    clock: u64 = 0,
    fn retireEntry(entry: *Entry, current: Cleanup) !void {
        if (entry.owner) |owner| try owner.validateCleanup(current);
        const previous = entry.*;
        entry.* = .{}; // consume before any callback-capable outside death
        if (previous.owner) |owner| {
            owner.retire(current) catch |err| {
                entry.* = previous; // registration failed before transfer
                return err;
            };
        } else if (previous.plan) |plan| std.heap.c_allocator.destroy(plan);
    }
    pub fn getWithCleanup(self: *Cache, key: u64, current: Cleanup) !*Entry {
        var oldest: usize = 0;
        for (&self.entries, 0..) |*entry, index| {
            if (entry.stamp != 0 and entry.key == key) {
                self.clock +|= 1;
                entry.stamp = self.clock;
                return entry;
            }
            if (entry.stamp < self.entries[oldest].stamp) oldest = index;
        }
        const entry = &self.entries[oldest];
        // Actual numerical use always supplies current B. Existing raw callers
        // may keep raw metadata, but cannot retire a managed owner without it.
        if (entry.owner) |owner| try owner.validateCleanup(current);
        var previous = entry.*;
        self.clock +|= 1;
        entry.* = .{ .key = key, .stamp = self.clock };
        retireEntry(&previous, current) catch |err| {
            entry.* = previous; // no callback/transfer happened on error
            return err;
        };
        if (entry.key != key or entry.stamp == 0) return error.AudioPlanChanged;
        return entry;
    }
    pub fn get(self: *Cache, key: u64) *Entry {
        return self.getWithCleanup(key, .{}) catch platform.inference_process_supervisor.restartWorker();
    }
    pub fn validateCleanup(self: *const Cache, current: Cleanup) !void {
        for (self.entries) |entry| if (entry.owner) |owner| {
            if (entry.plan != &owner.plan) return error.InvalidAudioPlanOwner;
            try owner.validateCleanup(current);
        };
    }
    pub fn clearWithCleanup(self: *Cache, current: Cleanup) !void {
        try self.validateCleanup(current);
        // Detach the whole cache before outside supplier reentry. An actual
        // boundary registration allocates nothing after the validation above.
        var previous = self.*;
        self.* = .{};
        for (&previous.entries) |*entry| retireEntry(entry, current) catch platform.inference_process_supervisor.restartWorker();
    }
    pub fn clearLegacy(self: *Cache) !void {
        for (self.entries) |entry| if (entry.owner != null) return error.MissingCurrentCleanupEpoch;
        try self.clearWithCleanup(.{});
    }
    pub fn clear(self: *Cache) void {
        self.clearLegacy() catch platform.inference_process_supervisor.restartWorker();
    }
};
test "embeddinggemma2 CUDA audio lifetime plan reuses dead regions and rejects allocation drift" {
    const a = std.testing.allocator;
    const p = try a.create(Plan);
    defer a.destroy(p);
    p.* = .{};
    p.allocate(256, 1000);
    p.allocate(512, 2000);
    p.release(1000);
    p.allocate(128, 3000);
    p.release(2000);
    try std.testing.expect(p.finish(3000));
    try std.testing.expectEqual(@as(usize, 768), p.peak);
    var s = Scope{ .plan = p, .base = 10000 };
    try std.testing.expectEqual(@as(usize, 10000), try s.allocate(256));
    try std.testing.expectEqual(@as(usize, 10256), try s.allocate(512));
    s.release(10000);
    try std.testing.expectEqual(@as(usize, 10000), try s.allocate(128));
    s.release(10256);
    try std.testing.expect(s.valid and s.cursor == p.event_count);
    var wrong = Scope{ .plan = p, .base = 10000 };
    try std.testing.expectError(error.AudioPlanChanged, wrong.allocate(128));
    p.* = .{};
    p.allocate(limit + 1, 1);
    try std.testing.expect(!p.valid);
    p.* = .{};
    p.allocate(256, 1);
    p.allocate(256, 2);
    try std.testing.expect(!p.finish(2));
}
