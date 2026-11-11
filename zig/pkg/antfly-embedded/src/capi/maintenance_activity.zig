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
    const Waiter = struct {
        ready: std.Io.Event = .unset,
        next: ?*Waiter = null,
    };
    count: usize = 0,
    waiters: ?*Waiter = null,
    pub fn begin(self: *Activity) void {
        self.count += 1;
    }
    pub fn finish(self: *Activity, io: std.Io) void {
        std.debug.assert(self.count != 0);
        self.count -= 1;
        if (self.count == 0) {
            // Each waiter owns a one-shot notification. A successor cohort can
            // begin before woken callers reacquire the gate; resetting a shared
            // Event then invalidates their still-pending waits.
            var pending = self.waiters;
            self.waiters = null;
            while (pending) |waiter| {
                pending = waiter.next;
                waiter.ready.set(io);
            }
        }
    }
    /// Return holding the same gate. Never wait while blocking a publisher
    /// which needs that gate to finish and release its retained read cut.
    pub fn wait(self: *Activity, io: std.Io, gate: anytype) void {
        while (self.count != 0) {
            var waiter: Waiter = .{ .next = self.waiters };
            self.waiters = &waiter;
            gate.unlock(io);
            waiter.ready.waitUncancelable(io);
            gate.lockUncancelable(io);
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

test "successor maintenance cohorts cannot reset pending drain notifications" {
    const Worker = struct {
        fn run(activity: *Activity, gate: *std.Io.Mutex, io: std.Io) void {
            gate.lockUncancelable(io);
            defer gate.unlock(io);
            // Start a new cohort before the first waiter can reacquire its gate.
            // Its one-shot notification must remain valid throughout.
            activity.finish(io);
            activity.begin();
            activity.finish(io);
        }
    };
    const io = std.testing.io;
    var gate: std.Io.Mutex = .init;
    var activity: Activity = .{};
    for (0..128) |_| {
        gate.lockUncancelable(io);
        activity.begin();
        const worker = try std.Thread.spawn(.{}, Worker.run, .{ &activity, &gate, io });
        activity.wait(io, &gate);
        gate.unlock(io);
        worker.join();
        try std.testing.expectEqual(@as(usize, 0), activity.count);
        try std.testing.expect(activity.waiters == null);
    }
}
