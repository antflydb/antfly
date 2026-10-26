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

//! Owned host/device compaction transaction. The serving owner must serialize
//! prepare/commit with append/decode (or keep a pinned immutable source and bump
//! generation on every mutation). This module does not enable a backend route.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const engine = @import("attention_match_engine.zig");
const Control = @import("attention_match.zig").Control;

/// Device owns its allocations and implements synchronize() !void and deinit().
/// deinit must safely retire any queued work, including after a failed fence.
/// Backend.upload(state, max_bytes, control) returns an independently owned
/// Device; it must admit original + replacement + workspace before allocation,
/// reject unsupported bias/coverage semantics and never mutate the live device.
pub fn Cache(comptime Device: type) type {
    return struct {
        const Self = @This();
        pub const Entry = struct {
            host: checkpoint.Owned,
            device: Device,
            pub fn deinit(self: *@This()) void {
                // Uploads can borrow the host arrays until the device retires.
                self.device.deinit();
                self.host.deinit();
                self.* = undefined;
            }
        };
        pub const Pending = struct {
            entry: ?Entry,
            generation: u64,
            pub fn deinit(self: *@This()) void {
                if (self.entry) |*entry| entry.deinit();
                self.entry = null;
            }
            pub fn exportTo(self: *const @This(), limits: checkpoint.Limits, sink: anytype, control: Control) !void {
                const entry = self.entry orelse return error.InvalidKvCheckpoint;
                try checkpoint.exportTo(entry.host.state, limits, sink, control);
            }
        };
        current: Entry,
        generation: u64 = 0,

        pub fn deinit(self: *Self) void {
            self.current.deinit();
            self.* = undefined;
        }

        pub fn prepare(self: *const Self, allocator: std.mem.Allocator, batches: []const engine.ReferenceBatch, layout: engine.Layout, options: engine.Options, backend: anytype, max_device_bytes: usize) !Pending {
            const generation = self.generation;
            if (generation == std.math.maxInt(u64)) return error.StaleKvCompaction;
            var host = try engine.prepare(allocator, self.current.host.state, batches, layout, options);
            errdefer host.deinit();
            var device: Device = try backend.upload(host.state, max_device_bytes, options.fit.control);
            errdefer device.deinit();
            try options.fit.control.check();
            if (generation != self.generation) return error.StaleKvCompaction;
            return .{ .entry = .{ .host = host, .device = device }, .generation = generation };
        }

        /// All fallible work precedes publication. Returns the still-owned old
        /// entry so the serving owner can update its borrowed views before
        /// retiring it. No device/host pointers are freed during the swap.
        pub fn commit(self: *Self, pending: *Pending, limits: checkpoint.Limits, control: Control) !Entry {
            const next = if (pending.entry) |*entry| entry else return error.InvalidKvCheckpoint;
            const guard: checkpoint.Store = .{ .current = self.current.host, .generation = self.generation };
            try guard.validateCompaction(next.host.state, pending.generation, limits);
            try control.check();
            try next.device.synchronize();
            try control.check();
            // The owner must keep the lock through this point. Recheck for
            // accidental reentrant mutation by cancellation/fence callbacks.
            if (pending.generation != self.generation) return error.StaleKvCompaction;
            const previous = self.current;
            self.current = next.*;
            self.generation += 1;
            pending.entry = null;
            return previous;
        }
    };
}

const TestDevice = struct {
    destroyed: *usize,
    fail_fence: bool = false,
    fn synchronize(self: *@This()) !void {
        if (self.fail_fence) return error.DeviceFenceFailed;
    }
    fn deinit(self: *@This()) void {
        self.destroyed.* += 1;
    }
};
const TestBackend = struct {
    destroyed: *usize,
    fail_upload: bool = false,
    fn upload(self: @This(), _: checkpoint.State, _: usize, _: Control) !TestDevice {
        if (self.fail_upload) return error.DeviceUploadFailed;
        return .{ .destroyed = self.destroyed };
    }
};
const fixture: checkpoint.State = .{
    .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
    .logical_length = 4,
    .tokens = &.{ 0, 1, 0, 1 },
    .logits = &.{ 0.1, 0.2 },
    .layer_donors = &.{0},
    .donors = &.{.{ .layer = 0, .heads = &.{.{ .dim = 1, .positions = &.{ 0, 1, 2, 3 }, .coverage_ends = &.{ 1, 2, 3, 4 }, .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 3, 3, 3, 3 }, .bias = &.{ 0, 0, 0, 0 } }} }},
};
const test_batches = [_]engine.ReferenceBatch{.{ .consumer_layer = 0, .query_head = 0, .train = &.{0}, .heldout = &.{1}, .train_positions = &.{4}, .heldout_positions = &.{5} }};
const test_layout: engine.Layout = .{ .query_heads_by_layer = &.{1}, .scales_by_donor = &.{1}, .softcaps_by_donor = &.{0} };
fn initial(allocator: std.mem.Allocator, destroyed: *usize) !Cache(TestDevice) {
    var owned = try checkpoint.cloneMetadata(allocator, fixture);
    errdefer owned.deinit();
    @constCast(owned.state.donors[0].heads)[0] = try checkpoint.cloneHead(allocator, fixture.donors[0].heads[0]);
    return .{ .current = .{ .host = owned, .device = .{ .destroyed = destroyed } } };
}

test "attention_match transaction preserves live and candidate ownership across upload fence cancellation and stale failures" {
    const a = std.testing.allocator;
    var destroyed: usize = 0;
    var cache = try initial(a, &destroyed);
    defer cache.deinit();
    const original = cache.current.host.state.donors[0].heads[0].values.ptr;
    var backend: TestBackend = .{ .destroyed = &destroyed, .fail_upload = true };
    try std.testing.expectError(error.DeviceUploadFailed, cache.prepare(a, &test_batches, test_layout, .{ .recent_rows = 0 }, backend, 1024));
    try std.testing.expectEqual(original, cache.current.host.state.donors[0].heads[0].values.ptr);
    backend.fail_upload = false;
    var pending = try cache.prepare(a, &test_batches, test_layout, .{ .recent_rows = 0 }, backend, 1024);
    defer pending.deinit();
    pending.entry.?.device.fail_fence = true;
    try std.testing.expectError(error.DeviceFenceFailed, cache.commit(&pending, .{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), destroyed);
    try std.testing.expectEqual(@as(u64, 0), cache.generation);
    pending.entry.?.device.fail_fence = false;
    const Cancel = struct {
        count: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == 2) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    try std.testing.expectError(error.CompactionCancelled, cache.commit(&pending, .{}, .{ .context = &cancel, .check_fn = Cancel.check }));
    cache.generation = 1;
    try std.testing.expectError(error.StaleKvCompaction, cache.commit(&pending, .{}, .{}));
    try std.testing.expectEqual(original, cache.current.host.state.donors[0].heads[0].values.ptr);
    try std.testing.expect(pending.entry != null);
    try std.testing.expectEqual(@as(usize, 0), destroyed);
}

test "attention_match transaction publishes host and device together and defers old cache retirement" {
    var destroyed: usize = 0;
    var cache = try initial(std.testing.allocator, &destroyed);
    defer cache.deinit();
    const old_values = cache.current.host.state.donors[0].heads[0].values.ptr;
    var pending = try cache.prepare(std.testing.allocator, &test_batches, test_layout, .{ .recent_rows = 0 }, TestBackend{ .destroyed = &destroyed }, 1024);
    defer pending.deinit();
    var retired = try cache.commit(&pending, .{}, .{});
    try std.testing.expectEqual(@as(usize, 0), destroyed);
    try std.testing.expectEqual(old_values, retired.host.state.donors[0].heads[0].values.ptr);
    try std.testing.expectEqual(@as(u64, 1), cache.generation);
    try std.testing.expectEqual(@as(usize, 1), cache.current.host.state.donors[0].heads[0].positions.len);
    try std.testing.expect(pending.entry == null);
    try std.testing.expectError(error.InvalidKvCheckpoint, cache.commit(&pending, .{}, .{}));
    retired.deinit();
    try std.testing.expectEqual(@as(usize, 1), destroyed);
}

fn allocationFailure(allocator: std.mem.Allocator) !void {
    var destroyed: usize = 0;
    var cache = try initial(allocator, &destroyed);
    defer cache.deinit();
    var pending = try cache.prepare(allocator, &test_batches, test_layout, .{ .recent_rows = 0 }, TestBackend{ .destroyed = &destroyed }, 1024);
    defer pending.deinit();
    var retired = try cache.commit(&pending, .{}, .{});
    retired.deinit();
}
test "attention_match transaction unwinds every host allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}
