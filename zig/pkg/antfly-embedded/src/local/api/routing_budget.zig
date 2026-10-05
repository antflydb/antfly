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

pub const std = @import("std");
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_time = @import("antfly_platform").time;
pub const runtime_io_abi = @import("antfly_runtime_abi").io_abi;

/// One absolute monotonic budget shared by snapshot capture and all CPU-side
/// routing work that follows it. The periodic checkpoint keeps large catalog
/// scans interruptible without putting a clock read on every range.
pub const RoutingBudget = struct {
    deadline_ns: ?u64 = null,
    io: ?runtime_io_abi.Borrow = null,
    cancellation: @import("antfly_cancellation").CancellationToken = .none,

    const checkpoint_stride: usize = 64;

    pub fn init(deadline_ns: ?u64) RoutingBudget {
        return .{ .deadline_ns = deadline_ns };
    }

    pub fn initIo(deadline_ns: ?u64, io: ?std.Io) RoutingBudget {
        return .{ .deadline_ns = deadline_ns, .io = if (io) |value| runtime_io_abi.Borrow.init(&value) else null };
    }

    pub fn nowNs(self: RoutingBudget) u64 {
        const borrow = self.io orelse return platform_time.monotonicNs();
        var receiver = borrow.receive() catch @panic("incompatible routing clock ABI");
        return @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds));
    }

    /// Translate a deadline into this budget's clock without extending it.
    /// Threaded .awake and native MONOTONIC have different epochs on Darwin.
    pub fn deadlineFrom(self: RoutingBudget, source: RoutingBudget) ?u64 {
        const deadline = source.deadline_ns orelse return null;
        if (self.io) |target| {
            if (source.io) |origin| {
                if (target.userdata == origin.userdata and target.vtable == origin.vtable and target.dispatch == origin.dispatch)
                    return deadline;
            }
        } else if (source.io == null) return deadline;
        // Sample the destination first so time spent translating cannot
        // extend the caller's budget. Expired budgets remain expired.
        const target_now = self.nowNs();
        return target_now +| (deadline -| source.nowNs());
    }

    pub fn sleepNs(self: RoutingBudget, duration_ns: u64) !void {
        if (self.io) |borrow| {
            var receiver = try borrow.receive();
            try receiver.io().sleep(.fromNanoseconds(duration_ns), .awake);
        } else {
            platform_clock.Clock.real().sleepMs(@max(@as(u64, 1), duration_ns / std.time.ns_per_ms));
        }
    }

    pub fn checkpoint(self: RoutingBudget) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| {
            if (self.nowNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
        }
    }

    pub fn checkpointIndex(self: RoutingBudget, index: usize) !void {
        if (index % checkpoint_stride == 0) try self.checkpoint();
    }
};

pub const RouteBudget = struct {
    clock: RoutingBudget = .{},
    cancellation: ?@import("antfly_cancellation").CancellationToken = null,

    pub fn fromRequest(request: anytype) RouteBudget {
        return .{
            .clock = .{
                .deadline_ns = request.execution_deadline_ns,
                .io = if (@hasField(@TypeOf(request), "execution_io")) request.execution_io else null,
            },
            .cancellation = request.cancellation,
        };
    }

    pub fn fromTimeoutMs(timeout_ms: ?u32) RouteBudget {
        return .{ .clock = .{ .deadline_ns = if (timeout_ms) |ms| @import("antfly_platform").time.monotonicNs() +| @as(u64, ms) * std.time.ns_per_ms else null } };
    }

    pub fn remainingTimeoutMs(self: RouteBudget) !?u32 {
        try self.check();
        const deadline = self.clock.deadline_ns orelse return null;
        const remaining = deadline -| self.clock.nowNs();
        if (remaining == 0) return error.Timeout;
        return @intCast(@min(std.math.maxInt(u32), (remaining +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms));
    }

    pub fn check(self: RouteBudget) !void {
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.clock.deadline_ns) |deadline| if (self.clock.nowNs() >= deadline) return error.Timeout;
    }
};
