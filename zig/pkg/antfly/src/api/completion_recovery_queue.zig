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

//! Bounded owned handoff out of native maintenance; no DB lease is retained.
const std = @import("std");
pub const wire = @import("antfly_local_sources").storage_history_recovery_wire;
pub const Request = wire.Request;
pub const Port = struct {
    ptr: *anyopaque,
    enqueue_fn: *const fn (*anyopaque, Request) anyerror!void,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const VTable = struct { enqueue: @FieldType(Port, "enqueue_fn") };
    const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
    pub fn enqueue(self: Port, request: Request) !void {
        return Boundary.call("enqueue", self.boundary_dispatch, self.enqueue_fn, .{ self.ptr, request });
    }
};
pub const Queue = struct {
    const capacity = 32;
    mutex: std.Io.Mutex = .init,
    slots: [capacity]?*Job = @splat(null),
    pub const Job = struct { allocator: std.mem.Allocator, request: Request, locator: []u8 };
    pub fn reserve(self: *Queue, io: std.Io, a: std.mem.Allocator, request: Request) !?*Job {
        try request.validate();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var available: ?usize = null;
        for (self.slots, 0..) |slot, index| {
            if (slot) |job| {
                if (std.mem.eql(u8, &job.request.identity(), &request.identity())) return null;
            } else if (available == null) {
                available = index;
            }
        }
        const index = available orelse return error.ResourceBudgetExceeded;
        const job = try a.create(Job);
        errdefer a.destroy(job);
        const locator = try a.dupe(u8, request.coordinator_locator);
        job.* = .{ .allocator = a, .request = request, .locator = locator };
        job.request.coordinator_locator = locator;
        self.slots[index] = job;
        return job;
    }
    pub fn release(self: *Queue, io: std.Io, job: *Job) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (&self.slots) |*slot| if (slot.* == job) {
            slot.* = null;
            job.allocator.free(job.locator);
            job.allocator.destroy(job);
            return;
        };
        unreachable;
    }
    pub fn deinit(self: *Queue) void {
        for (self.slots) |slot| std.debug.assert(slot == null);
    }
};

test "data raft ordered history completion scheduling owns deduplicated bounded jobs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var queue: Queue = .{};
    defer queue.deinit();
    var locator = "table2:00000004:docs:9".*;
    const request: Request = .{ .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 3, .group_id = 9, .topology_epoch = 4 }, .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_locator = &locator, .resolution_timestamp_ns = 100 };
    const job = (try queue.reserve(io, a, request)).?;
    @memset(&locator, 'x');
    try std.testing.expectEqualStrings("table2:00000004:docs:9", job.request.coordinator_locator);
    try std.testing.expect((try queue.reserve(io, a, request)) == null);
    var rest: [31]*Queue.Job = undefined;
    for (&rest, 0..) |*value, i| {
        var next = request;
        next.transaction_id[0] = @intCast(i + 10);
        value.* = (try queue.reserve(io, a, next)).?;
    }
    var overflow = request;
    overflow.transaction_id[0] = 99;
    try std.testing.expectError(error.ResourceBudgetExceeded, queue.reserve(io, a, overflow));
    for (rest) |value| queue.release(io, value);
    queue.release(io, job);
    const retry = (try queue.reserve(io, a, overflow)).?;
    queue.release(io, retry);
}
