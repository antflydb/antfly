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

//! Bounded, non-reentrant handoff of immutable producer proposals. Admission
//! is pending, never publication success. Durable producer work/receipts own
//! recovery; this queue borrows no DB, cache lease, or worker pointer.
const std = @import("std");
pub const Namespace = [24]u8;
pub const Port = struct {
    ptr: *anyopaque,
    enqueue_fn: *const fn (*anyopaque, u64, Namespace, []const u8) anyerror!void,
    pub fn enqueue(self: Port, group_id: u64, namespace: Namespace, command: []const u8) !void {
        try self.enqueue_fn(self.ptr, group_id, namespace, command);
    }
};
pub const Queue = struct {
    const capacity = 8;
    pub const Class = enum { producer, control };
    const Slot = struct { occupied: bool = false, group_id: u64 = 0, digest: [32]u8 = @splat(0), class: Class = .producer, namespace: Namespace = @splat(0), baseline: bool = false, reserved: bool = false };
    mutex: std.Io.Mutex = .init,
    slots: [capacity]Slot = @splat(.{}),
    bytes: usize = 0,
    producer_bytes: usize = 0,
    jobs: usize = 0,
    max_bytes: usize = @import("antfly_local_sources").storage_db_artifact_publication_transport_codec.max_encoded_bytes + 16 * 1024 * 1024,
    /// Baseline/activation controls must progress while large authored outputs
    /// occupy the producer lane. These reservations are admission headroom,
    /// not additional memory beyond max_bytes.
    control_slots: usize = 2,
    control_bytes: usize = 16 * 1024 * 1024,
    pub const Job = struct { alloc: std.mem.Allocator, group_id: u64, namespace: Namespace, command: []u8, slot: usize };

    /// Null means the proposal (or an earlier baseline for the same owner) is
    /// already queued. It never certifies a committed result.
    pub fn reserve(self: *Queue, io: std.Io, alloc: std.mem.Allocator, group_id: u64, namespace: Namespace, command: []const u8) !?*Job {
        return self.reserveClass(io, alloc, group_id, namespace, command, .producer);
    }

    pub fn reserveClass(self: *Queue, io: std.Io, alloc: std.mem.Allocator, group_id: u64, namespace: Namespace, command: []const u8, class: Class) !?*Job {
        if (group_id == 0 or std.mem.allEqual(u8, &namespace, 0) or command.len == 0) return error.InvalidBatchRequest;
        const reserved = class == .control and self.control_slots != 0 and command.len <= self.control_bytes / self.control_slots;
        const baseline = class == .control and @import("antfly_local_sources").storage_db_artifact_publication_transport_codec.baselineAdmissionHint(command);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&namespace);
        hash.update(command);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        self.mutex.lockUncancelable(io);
        const slot = blk: {
            defer self.mutex.unlock(io);
            var free: ?usize = null;
            var producer_jobs: usize = 0;
            for (self.slots, 0..) |entry, i| {
                if (entry.occupied and entry.group_id == group_id) {
                    if (std.mem.eql(u8, &entry.digest, &digest) or (baseline and entry.baseline and std.mem.eql(u8, &entry.namespace, &namespace))) return null;
                }
                if (entry.occupied and !entry.reserved) producer_jobs += 1;
                if (!entry.occupied and free == null) free = i;
            }
            const index = free orelse return error.ResourceLimitExceeded;
            if (command.len > self.max_bytes -| self.bytes) return error.ResourceLimitExceeded;
            if (!reserved) {
                if (producer_jobs >= capacity -| self.control_slots) return error.ResourceLimitExceeded;
                // A deliberately smaller test/deployment cap may disable
                // byte headroom explicitly; never silently overcommit it.
                if (command.len > (self.max_bytes -| self.control_bytes) -| self.producer_bytes) return error.ResourceLimitExceeded;
            }
            // Large control identities borrow shared capacity; the reservation
            // remains available to ordinary small control messages.
            self.slots[index] = .{ .occupied = true, .group_id = group_id, .digest = digest, .class = class, .namespace = namespace, .baseline = baseline, .reserved = reserved };
            self.bytes += command.len;
            if (!reserved) self.producer_bytes += command.len;
            self.jobs += 1;
            break :blk index;
        };
        errdefer self.releaseSlot(io, slot, command.len);
        const job = try alloc.create(Job);
        errdefer alloc.destroy(job);
        job.* = .{ .alloc = alloc, .group_id = group_id, .namespace = namespace, .command = try alloc.dupe(u8, command), .slot = slot };
        return job;
    }
    fn releaseSlot(self: *Queue, io: std.Io, slot: usize, bytes: usize) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.assert(self.slots[slot].occupied);
        if (!self.slots[slot].reserved) self.producer_bytes -= bytes;
        self.slots[slot] = .{};
        self.bytes -= bytes;
        self.jobs -= 1;
    }
    pub fn release(self: *Queue, io: std.Io, job: *Job) void {
        const slot = job.slot;
        const len = job.command.len;
        const alloc = job.alloc;
        alloc.free(job.command);
        alloc.destroy(job);
        self.releaseSlot(io, slot, len);
    }
    pub fn deinit(self: *Queue) void {
        std.debug.assert(self.jobs == 0 and self.bytes == 0 and self.producer_bytes == 0);
    }
};

test "ordered artifact inventory publication queue owns bounded bytes and exact attempt dedup" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{ .max_bytes = 8, .control_bytes = 0 };
    defer queue.deinit();
    var bytes: [4]u8 = "test".*;
    const first = (try queue.reserve(io, alloc, 2, @splat(1), &bytes)).?;
    try std.testing.expect((try queue.reserve(io, alloc, 2, @splat(1), &bytes)) == null);
    bytes[0] = 'b';
    try std.testing.expectEqualStrings("test", first.command);
    const second = (try queue.reserve(io, alloc, 2, @splat(1), &bytes)).?;
    try std.testing.expectError(error.ResourceLimitExceeded, queue.reserve(io, alloc, 3, @splat(1), &bytes));
    queue.release(io, first);
    queue.release(io, second);
    try std.testing.expectEqual(@as(usize, 0), queue.bytes);
}

test "ordered artifact inventory publication queue reserves control slots under producer saturation" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit();
    var jobs: [8]*Queue.Job = undefined;
    var count: usize = 0;
    defer for (jobs[0..count]) |job| queue.release(io, job);
    for (0..6) |i| {
        jobs[count] = (try queue.reserve(io, alloc, i + 1, @splat(1), "producer")).?;
        count += 1;
    }
    try std.testing.expectError(error.ResourceLimitExceeded, queue.reserve(io, alloc, 7, @splat(1), "producer"));
    // A duplicate must remain a no-op even when its lane is saturated.
    try std.testing.expect((try queue.reserve(io, alloc, 1, @splat(1), "producer")) == null);
    for (0..2) |i| {
        jobs[count] = (try queue.reserveClass(io, alloc, i + 7, @splat(1), "baseline", .control)).?;
        count += 1;
    }
    try std.testing.expectError(error.ResourceLimitExceeded, queue.reserveClass(io, alloc, 9, @splat(1), "baseline", .control));
}

test "ordered artifact inventory publication queue byte headroom survives release and allocation failures" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const io = std.testing.io;
            var queue: Queue = .{ .max_bytes = 100, .control_bytes = 20 };
            defer queue.deinit();
            const first = (try queue.reserve(io, alloc, 1, @splat(1), &@as([80]u8, @splat(1)))).?;
            defer queue.release(io, first);
            try std.testing.expectError(error.ResourceLimitExceeded, queue.reserve(io, alloc, 2, @splat(1), "x"));
            {
                const control = (try queue.reserveClass(io, alloc, 2, @splat(1), &@as([10]u8, @splat(2)), .control)).?;
                defer queue.release(io, control);
                const second = (try queue.reserveClass(io, alloc, 3, @splat(1), &@as([10]u8, @splat(3)), .control)).?;
                defer queue.release(io, second);
                try std.testing.expectEqual(@as(usize, 100), queue.bytes);
            }
            try std.testing.expectEqual(@as(usize, 80), queue.producer_bytes);
            try std.testing.expectEqual(@as(usize, 80), queue.bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "privacy large control identities borrow shared queue capacity without exceeding its cap" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{ .max_bytes = 100, .control_bytes = 20 };
    defer queue.deinit();
    const large = (try queue.reserveClass(io, a, 1, @splat(1), &@as([60]u8, @splat(1)), .control)).?;
    defer queue.release(io, large);
    try std.testing.expectError(error.ResourceLimitExceeded, queue.reserve(io, a, 2, @splat(2), &@as([41]u8, @splat(2))));
    const small = (try queue.reserveClass(io, a, 3, @splat(3), &@as([20]u8, @splat(3)), .control)).?;
    defer queue.release(io, small);
    try std.testing.expectEqual(@as(usize, 80), queue.bytes);
}

test "privacy publication queue coalesces changing baseline watermarks until apply" {
    const a = std.testing.allocator;
    const codec = @import("antfly_local_sources").storage_db_artifact_publication_transport_codec;
    const publication = @import("antfly_local_sources").storage_db_artifact_publication;
    var command: publication.Command = .{ .mode = .baseline, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .baseline = .{ .observed_term = 1, .observed_index = 1, .expected_cursor = "", .next_cursor = "b", .upper_bound = "c", .row_keys = &.{}, .at_end = false } };
    command.publication_digest = command.digest();
    const first = try codec.encodeAlloc(a, command);
    defer a.free(first);
    command.baseline.?.observed_index = 2;
    command.publication_digest = command.digest();
    const second = try codec.encodeAlloc(a, command);
    defer a.free(second);
    var queue: Queue = .{};
    const job = (try queue.reserveClass(std.testing.io, a, 1, command.namespace, first, .control)).?;
    try std.testing.expect((try queue.reserveClass(std.testing.io, a, 1, command.namespace, second, .control)) == null);
    try std.testing.expectEqual(@as(usize, 1), queue.jobs);
    queue.release(std.testing.io, job);
    const retry = (try queue.reserveClass(std.testing.io, a, 1, command.namespace, second, .control)).?;
    defer queue.release(std.testing.io, retry);
}

test "privacy large controls preserve small control slots under saturation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{ .max_bytes = 100, .control_bytes = 2 };
    defer queue.deinit();
    var jobs: [8]*Queue.Job = undefined;
    var count: usize = 0;
    defer for (jobs[0..count]) |job| queue.release(io, job);
    for (0..6) |i| {
        jobs[count] = (try queue.reserveClass(io, a, i + 1, @splat(1), "large", .control)).?;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 30), queue.producer_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, queue.reserveClass(io, a, 7, @splat(1), "large", .control));
    for (0..2) |i| {
        jobs[count] = (try queue.reserveClass(io, a, i + 7, @splat(1), "s", .control)).?;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 32), queue.bytes);
    try std.testing.expectEqual(@as(usize, 30), queue.producer_bytes);
}
