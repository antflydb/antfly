// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Storage-owned expiry observations; the API coordinator owns FK actions and
//! distributed commit. Borrowed slices remain valid only for the callback.
const std = @import("std");
pub const Candidate = struct {
    key: []const u8,
    row_version: u64,
    ttl_timestamp_ns: u64,
    /// SHA256 of exact stored primary bytes: a TTL timestamp is not a mutation revision.
    expected_content_digest: [32]u8,
};
pub const Request = struct {
    table_id: u64,
    group_id: u64,
    schema_version: u32,
    ttl_duration_ns: u64,
    ttl_field: []const u8,
    observed_at_unix_ns: u64,
    grace_period_ns: u64,
    candidates: []const Candidate,
};
pub const Port = struct {
    ptr: *anyopaque,
    expire_fn: *const fn (*anyopaque, Request) anyerror!u32,
    pub fn expire(self: Port, request: Request) !u32 {
        return self.expire_fn(self.ptr, request);
    }
};

/// Bounded ownership transfer from a native TTL worker to the server's existing
/// background job lane. Native callbacks must never reenter the managed DB cache:
/// cache close may be waiting for that very TTL worker to finish.
pub const Queue = struct {
    pub const capacity = 64;
    const Slot = struct { table_id: u64 = 0, group_id: u64 = 0 };
    mutex: std.Io.Mutex = .init,
    slots: [capacity]Slot = @splat(.{}),
    bytes: usize = 0,
    jobs: usize = 0,
    max_jobs: usize = capacity,
    max_bytes: usize = 8 * 1024 * 1024,

    pub const Job = struct {
        request: Request,
        alloc: std.mem.Allocator,
        candidates: []Candidate,
        buffer: []u8,
        slot: usize,
        admitted_bytes: usize,
    };

    pub fn reserve(self: *Queue, io: std.Io, alloc: std.mem.Allocator, request: Request) !*Job {
        if (request.table_id == 0 or request.group_id == 0 or
            request.candidates.len == 0 or request.candidates.len > 128)
            return error.InvalidTtlObservation;
        var buffer_bytes = request.ttl_field.len;
        for (request.candidates) |candidate| {
            buffer_bytes = std.math.add(usize, buffer_bytes, candidate.key.len) catch
                return error.CoordinatedTtlBackpressure;
        }
        const admitted_bytes = std.math.add(usize, buffer_bytes, @sizeOf(Job) + request.candidates.len * @sizeOf(Candidate)) catch
            return error.CoordinatedTtlBackpressure;
        self.mutex.lockUncancelable(io);
        const slot = blk: {
            defer self.mutex.unlock(io);
            if (self.jobs >= @min(capacity, self.max_jobs) or
                admitted_bytes > self.max_bytes -| self.bytes)
                return error.CoordinatedTtlBackpressure;
            var free_slot: ?usize = null;
            for (self.slots, 0..) |entry, index| {
                if (entry.table_id == request.table_id and entry.group_id == request.group_id)
                    return error.CoordinatedTtlBackpressure;
                if (entry.table_id == 0 and free_slot == null) free_slot = index;
            }
            const index = free_slot orelse return error.CoordinatedTtlBackpressure;
            self.slots[index] = .{ .table_id = request.table_id, .group_id = request.group_id };
            self.jobs += 1;
            self.bytes += admitted_bytes;
            break :blk index;
        };
        errdefer self.releaseSlot(io, slot, admitted_bytes);
        const job = try alloc.create(Job);
        errdefer alloc.destroy(job);
        const candidates = try alloc.alloc(Candidate, request.candidates.len);
        errdefer alloc.free(candidates);
        const buffer = try alloc.alloc(u8, buffer_bytes);
        errdefer alloc.free(buffer);
        @memcpy(buffer[0..request.ttl_field.len], request.ttl_field);
        var offset = request.ttl_field.len;
        for (request.candidates, candidates) |source, *target| {
            target.* = source;
            @memcpy(buffer[offset..][0..source.key.len], source.key);
            target.key = buffer[offset..][0..source.key.len];
            offset += source.key.len;
        }
        var owned = request;
        owned.ttl_field = buffer[0..request.ttl_field.len];
        owned.candidates = candidates;
        job.* = .{ .request = owned, .alloc = alloc, .candidates = candidates, .buffer = buffer, .slot = slot, .admitted_bytes = admitted_bytes };
        return job;
    }

    fn releaseSlot(self: *Queue, io: std.Io, slot: usize, bytes: usize) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.assert(self.slots[slot].table_id != 0);
        self.slots[slot] = .{};
        self.jobs -= 1;
        self.bytes -= bytes;
    }

    pub fn release(self: *Queue, io: std.Io, job: *Job) void {
        const slot = job.slot;
        const bytes = job.admitted_bytes;
        const alloc = job.alloc;
        alloc.free(job.buffer);
        alloc.free(job.candidates);
        alloc.destroy(job);
        self.releaseSlot(io, slot, bytes);
    }

    /// Called only after native callbacks and the server job owner have drained.
    pub fn deinit(self: *Queue) void {
        std.debug.assert(self.jobs == 0 and self.bytes == 0);
    }
};

test "coordinated ttl queue owns bounded per-group observations and releases admission" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit();
    var key = [_]u8{ 'a', 0, 'b' };
    var field = [_]u8{ 't', 's' };
    const candidates = [_]Candidate{.{ .key = &key, .row_version = 1, .ttl_timestamp_ns = 1, .expected_content_digest = @splat(9) }};
    var request: Request = .{ .table_id = 10, .group_id = 11, .schema_version = 1, .ttl_duration_ns = 1, .ttl_field = &field, .observed_at_unix_ns = 3, .grace_period_ns = 0, .candidates = &candidates };
    const first = try queue.reserve(io, alloc, request);
    key[0] = 'z';
    field[0] = 'x';
    try std.testing.expectEqualStrings("a\x00b", first.request.candidates[0].key);
    try std.testing.expectEqualStrings("ts", first.request.ttl_field);
    try std.testing.expectError(error.CoordinatedTtlBackpressure, queue.reserve(io, alloc, request));
    request.group_id += 1;
    queue.max_jobs = 1;
    try std.testing.expectError(error.CoordinatedTtlBackpressure, queue.reserve(io, alloc, request));
    queue.max_jobs = Queue.capacity;
    queue.max_bytes = queue.bytes;
    try std.testing.expectError(error.CoordinatedTtlBackpressure, queue.reserve(io, alloc, request));
    queue.release(io, first);
    queue.max_bytes = 8 * 1024 * 1024;
    const next = try queue.reserve(io, alloc, request);
    queue.release(io, next);
    try std.testing.expectEqual(@as(usize, 0), queue.jobs);
}

test "coordinated ttl queue allocation failures release reserved admission" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: std.mem.Allocator) !void {
            var queue: Queue = .{};
            defer queue.deinit();
            const candidates = [_]Candidate{.{ .key = "row", .row_version = 1, .ttl_timestamp_ns = 1, .expected_content_digest = @splat(8) }};
            const job = try queue.reserve(std.testing.io, alloc, .{ .table_id = 1, .group_id = 2, .schema_version = 3, .ttl_duration_ns = 1, .ttl_field = "expires", .observed_at_unix_ns = 9, .grace_period_ns = 0, .candidates = &candidates });
            queue.release(std.testing.io, job);
        }
    }.run, .{});
}
