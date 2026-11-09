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

//! Admission for detached maintenance. Callers hold their owner gate while
//! starting/finishing activity; a manual drain yields that gate until the
//! detached cohort releases its snapshots, before taking the path-writer lease.
const std = @import("std");
pub const Activity = struct {
    count: usize = 0,
    idle: @import("antfly_platform").sync.BroadcastCondition = .init,
    pub fn begin(self: *Activity) void {
        self.count += 1;
    }
    pub fn finish(self: *Activity, io: std.Io) void {
        std.debug.assert(self.count != 0);
        self.count -= 1;
        if (self.count == 0) self.idle.broadcast(io);
    }
    /// Return holding the same gate. Never wait while blocking a publisher
    /// which needs that gate to finish and release its retained read cut.
    pub fn wait(self: *Activity, io: std.Io, gate: anytype) void {
        while (self.count != 0) {
            // A new cohort can start before a notified waiter resumes. An
            // epoch condition tolerates that transition; resetting an Event
            // here invalidates waiters still returning from its previous set.
            self.idle.waitUncancelable(io, gate);
        }
    }
};

test "manual maintenance yields its owner gate until the detached cohort finishes" {
    const Worker = struct {
        fn run(activity: *Activity, gate: *std.Io.Mutex, io: std.Io) void {
            gate.lockUncancelable(io);
            defer gate.unlock(io);
            activity.finish(io);
            std.debug.assert(activity.count == 1);
            activity.finish(io);
        }
    };
    const io = std.testing.io;
    var gate: std.Io.Mutex = .init;
    var activity: Activity = .{};
    gate.lockUncancelable(io);
    activity.begin();
    activity.begin();
    const worker = try std.Thread.spawn(.{}, Worker.run, .{ &activity, &gate, io });
    activity.wait(io, &gate);
    gate.unlock(io);
    worker.join();
    try std.testing.expectEqual(@as(usize, 0), activity.count);
    activity.begin();
    activity.finish(io);
    gate.lockUncancelable(io);
    activity.wait(io, &gate);
    gate.unlock(io);
}

test "manual maintenance survives a new cohort before notified waiters resume" {
    const Worker = struct {
        fn run(activity: *Activity, gate: *std.Io.Mutex, io: std.Io) void {
            gate.lockUncancelable(io);
            activity.wait(io, gate);
            std.debug.assert(activity.count == 0);
            gate.unlock(io);
        }
    };
    const io = std.testing.io;
    var gate: std.Io.Mutex = .init;
    var activity: Activity = .{};
    gate.lockUncancelable(io);
    activity.begin();
    gate.unlock(io);
    var workers: [4]std.Thread = undefined;
    for (&workers) |*worker| worker.* = try std.Thread.spawn(.{}, Worker.run, .{ &activity, &gate, io });
    // Wait until every waiter has enrolled, rather than relying on a sleep.
    while (true) {
        gate.lockUncancelable(io);
        if (activity.idle.waiters == workers.len) break;
        gate.unlock(io);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // The gate prevents a notified waiter from checking its predicate before
    // the next cohort starts. Its old notification must remain safe to return.
    activity.finish(io);
    activity.begin();
    gate.unlock(io);
    try io.sleep(.fromMilliseconds(1), .awake);
    gate.lockUncancelable(io);
    activity.finish(io);
    gate.unlock(io);
    for (workers) |worker| worker.join();
    try std.testing.expectEqual(@as(usize, 0), activity.count);
    try std.testing.expectEqual(@as(u32, 0), activity.idle.waiters);
}
