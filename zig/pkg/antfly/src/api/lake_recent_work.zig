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

//! Node-wide admission and singleflight for recent-vector recovery. No caller
//! allocator, cancellation callback or table metadata is retained by the gate.
const std = @import("std");
const local = @import("antfly_local_sources");
const Context = local.serverless_query_lake_read_context.Context;
pub const Gate = struct {
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    flights: [64]?[32]u8 = @splat(null),
    // Each builder has 256 MiB working admission plus a 64 MiB map writer.
    // Both foreground and background callers share this 640 MiB ceiling.
    admission: local.storage_resource_manager.DenseWorkAdmission.Queue = .{ .capacity = 2 },
    pub const Flight = struct {
        gate: *Gate,
        io: std.Io,
        slot: usize,
        pub fn deinit(self: Flight) void {
            self.gate.mutex.lockUncancelable(self.io);
            defer self.gate.mutex.unlock(self.io);
            self.gate.flights[self.slot] = null;
            self.gate.changed.broadcast(self.io);
        }
    };
    pub const Builder = local.storage_resource_manager.DenseWorkAdmission.Queue.Lease;
    fn wait(self: *Gate, io: std.Io) !void {
        self.changed.waitTimeout(io, &self.mutex, .{ .duration = .{ .raw = .fromMilliseconds(25), .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => {},
            else => return err,
        };
    }
    pub fn claim(self: *Gate, key: [32]u8, context: Context) !Flight {
        const io = context.io orelse return error.UnsupportedQueryRequest;
        while (true) {
            try context.ensureActive();
            try self.mutex.lock(io);
            var free: ?usize = null;
            var busy = false;
            for (self.flights, 0..) |entry, i| {
                if (entry) |active| {
                    if (std.mem.eql(u8, &active, &key)) busy = true;
                } else if (free == null) free = i;
            }
            if (!busy) if (free) |slot| {
                self.flights[slot] = key;
                self.mutex.unlock(io);
                return .{ .gate = self, .io = io, .slot = slot };
            };
            self.wait(io) catch |err| {
                self.mutex.unlock(io);
                return err;
            };
            self.mutex.unlock(io);
        }
    }
    pub fn admit(self: *Gate, context: Context) !Builder {
        try context.ensureActive();
        const Probe = struct {
            fn canceled(raw: *const anyopaque) bool {
                return @as(*const Context, @ptrCast(@alignCast(raw))).isCancelled();
            }
        };
        var lease = self.admission.acquire(context.io, .{ .ptr = &context, .is_cancelled = Probe.canceled }) catch |err| {
            try context.ensureActive();
            return err;
        };
        errdefer lease.release();
        try context.ensureActive();
        return lease;
    }
};
pub var process: Gate = .{};

test "external lake bounded WAL recovery admission bounds builders and returns slots after cancellation" {
    var gate: Gate = .{ .admission = .{ .capacity = 1 } };
    const context: Context = .{ .io = std.testing.io };
    const first = try gate.claim(@splat(1), context);
    var builder = try gate.admit(context);
    var expired = context;
    expired.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, gate.claim(@splat(1), expired));
    try std.testing.expectError(error.DeadlineExceeded, gate.admit(expired));
    first.deinit();
    builder.release();
    const next = try gate.claim(@splat(1), context);
    defer next.deinit();
    var admitted = try gate.admit(context);
    admitted.release();
    try std.testing.expectEqual(@as(usize, 1), gate.admission.peak);
    try std.testing.expectEqual(@as(usize, 0), gate.admission.active);
}

test "external lake bounded WAL recovery scratch releases partition memory and preserves parent quota" {
    const a = std.testing.allocator;
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
    var retained = std.heap.ArenaAllocator.init(memory.allocator());
    defer retained.deinit();
    for (0..32) |_| {
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(retained.allocator()));
        const temporary = try scratch.allocator().alloc(u8, 1024 * 1024);
        @memset(temporary, 1);
        const root = try retained.allocator().alloc(u8, 64);
        @memset(root, 2);
        scratch.deinit();
        try std.testing.expect(memory.live < 128 * 1024);
    }
    try std.testing.expect(memory.peak < memory.limit);
    var small: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 };
    var region = std.heap.ArenaAllocator.init(small.allocator());
    defer region.deinit();
    try std.testing.expectError(error.OutOfMemory, local.sql_memory_budget.reclaimingAllocator(region.allocator()).alloc(u8, 128 * 1024));
}

test "external lake bounded WAL recovery singleflight wakes followers without retaining request owners" {
    const io = std.testing.io;
    var gate: Gate = .{};
    const key: [32]u8 = @splat(1);
    const first = try gate.claim(key, .{ .io = io });
    var first_active = true;
    defer if (first_active) first.deinit();
    const Worker = struct {
        gate: *Gate,
        io: std.Io,
        entered: std.Io.Event = .unset,
        finished: std.Io.Event = .unset,
        fn run(self: *@This()) !void {
            self.entered.set(self.io);
            const owned = try self.gate.claim(@splat(1), .{ .io = self.io });
            defer owned.deinit();
            self.finished.set(self.io);
        }
    };
    var worker: Worker = .{ .gate = &gate, .io = io };
    var task = try io.concurrent(Worker.run, .{&worker});
    defer task.cancel(io) catch {};
    try worker.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try std.testing.expectError(error.Timeout, worker.finished.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }));
    first.deinit();
    first_active = false;
    try task.await(io);
    for (gate.flights) |active| try std.testing.expect(active == null);
}
