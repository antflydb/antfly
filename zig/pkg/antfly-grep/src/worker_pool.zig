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

//! Bounded persistent worker leases. A lease joins its invocation, not its
//! operating-system thread. Separate file/provider pools prevent nested waits.
const std = @import("std");
pub const Handle = union(enum) {
    native: std.Thread,
    pooled: *Pool.Slot,
    pub fn join(self: Handle) void {
        switch (self) {
            .native => |thread| thread.join(),
            .pooled => |slot| {
                const pool = slot.owner;
                pool.mutex.lockUncancelable(pool.io);
                defer pool.mutex.unlock(pool.io);
                while (!slot.done) pool.changed.waitUncancelable(pool.io, &pool.mutex);
                slot.leased = false;
            },
        }
    }
};
pub const Pool = struct {
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    stopped: bool = false,
    slots: [32]Slot = @splat(.{}),
    const Slot = struct {
        owner: *Pool = undefined,
        thread: ?std.Thread = null,
        leased: bool = false,
        done: bool = true,
        context: ?*anyopaque = null,
        call: *const fn (*anyopaque) void = undefined,
    };
    pub fn launch(self: *Pool, comptime worker: anytype, context: anytype) !Handle {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (&self.slots) |*slot| {
            if (slot.leased) continue;
            slot.owner = self;
            slot.leased = true;
            slot.done = false;
            slot.context = context;
            slot.call = struct {
                fn call(raw: *anyopaque) void {
                    worker(@as(@TypeOf(context), @ptrCast(@alignCast(raw))));
                }
            }.call;
            errdefer {
                slot.leased = false;
                slot.done = true;
                slot.context = null;
            }
            if (slot.thread == null) slot.thread = try std.Thread.spawn(.{}, run, .{slot});
            self.changed.broadcast(self.io);
            return .{ .pooled = slot };
        }
        // Explicit high concurrency remains supported without retaining an
        // unbounded number of idle stacks in a long-lived process.
        return .{ .native = try std.Thread.spawn(.{}, worker, .{context}) };
    }
    fn run(slot: *Slot) void {
        const self = slot.owner;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (true) {
            while (slot.context == null and !self.stopped) self.changed.waitUncancelable(self.io, &self.mutex);
            if (self.stopped) return;
            const context = slot.context.?;
            const call = slot.call;
            self.mutex.unlock(self.io);
            call(context);
            self.mutex.lockUncancelable(self.io);
            slot.context = null;
            slot.done = true;
            self.changed.broadcast(self.io);
        }
    }
    pub fn deinit(self: *Pool) void {
        self.mutex.lockUncancelable(self.io);
        for (self.slots) |slot| std.debug.assert(!slot.leased);
        self.stopped = true;
        self.changed.broadcast(self.io);
        self.mutex.unlock(self.io);
        for (self.slots) |slot| if (slot.thread) |thread| thread.join();
    }
};
test "worker leases join before stack arguments are released and reuse threads" {
    var pool: Pool = .{ .io = std.testing.io };
    defer pool.deinit();
    const Work = struct {
        fn run(value: *usize) void {
            value.* += 1;
        }
    };
    var count: usize = 0;
    for (0..100) |_| {
        const handle = try pool.launch(Work.run, &count);
        handle.join();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
    try std.testing.expect(pool.slots[0].thread != null and pool.slots[1].thread == null);
}
