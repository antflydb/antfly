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

const std = @import("std");
const builtin = @import("builtin");
const runtime = @import("../background_runtime.zig");
/// Local restart admission and retry supervision. The runtime owner drains the
/// durable lane before freeing this owner or its borrowed operation context.
pub const Owner = struct {
    state: std.atomic.Value(u8) = .init(0),
    pub const Port = struct {
        ptr: *anyopaque,
        lane: runtime.DurableJobLane,
        owner_id: u64,
        io: ?std.Io,
        closing: *const std.atomic.Value(bool),
        wanted: *const fn (*anyopaque) bool,
        attempt: *const fn (*anyopaque) anyerror!bool,
        name: []const u8,
        unavailable: anyerror = error.MaintenanceRuntimeUnavailable,
    };
    const Work = struct {
        owner: *Owner,
        port: Port,
        fn run(ptr: *anyopaque) !void {
            const work: *@This() = @ptrCast(@alignCast(ptr));
            try work.owner.run(work.port);
        }
        fn deinit(ptr: *anyopaque) void {
            const work: *@This() = @ptrCast(@alignCast(ptr));
            std.heap.page_allocator.destroy(work);
        }
    };
    pub fn schedule(self: *Owner, port: Port) void {
        if (port.closing.load(.acquire) or !port.wanted(port.ptr)) return;
        if (self.state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) {
            _ = self.state.cmpxchgStrong(1, 2, .acq_rel, .acquire);
            return;
        }
        const work = std.heap.page_allocator.create(Work) catch {
            self.state.store(0, .release);
            return;
        };
        work.* = .{ .owner = self, .port = port };
        port.lane.submit(.{ .owner_id = port.owner_id, .class = .maintenance, .ptr = work, .run = Work.run, .deinit = Work.deinit }) catch |err| {
            std.heap.page_allocator.destroy(work);
            self.state.store(0, .release);
            std.log.warn("{s} runtime restart was not scheduled err={s}", .{ port.name, @errorName(err) });
        };
    }
    fn settled(self: *Owner) bool {
        if (self.state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return true;
        return self.state.cmpxchgStrong(2, 1, .acq_rel, .acquire) != null;
    }
    pub fn run(self: *Owner, port: Port) !void {
        var retries: usize = 0;
        while (true) {
            if (port.closing.load(.acquire)) {
                self.state.store(0, .release);
                return;
            }
            if (!port.wanted(port.ptr)) {
                if (self.settled()) return;
                retries = 0;
                continue;
            }
            var start_error: ?anyerror = null;
            const running = port.attempt(port.ptr) catch |err| blk: {
                start_error = err;
                break :blk false;
            };
            if (running) {
                if (self.settled()) return;
                retries = 0;
                continue;
            }
            retries += 1;
            if (start_error) |err| {
                if (retries == 1 or std.math.isPowerOfTwo(retries)) std.log.warn("{s} runtime restart retry attempt={} err={s}", .{ port.name, retries, @errorName(err) });
            }
            if (port.io) |io| {
                const shift: u6 = @intCast(@min(retries - 1, 5));
                const delay_ms: i64 = if (builtin.is_test) 1 else @min(@as(i64, 25) << shift, 1000);
                io.sleep(.fromMilliseconds(delay_ms), .awake) catch {};
            } else {
                self.state.store(0, .release);
                return start_error orelse port.unavailable;
            }
            if (retries >= 8) {
                self.state.store(0, .release);
                self.schedule(port);
                return;
            }
        }
    }
};

test "runtime restart owner coalesces a notification while an attempt is running" {
    const F = struct {
        owner: *Owner,
        closing: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn wanted(_: *anyopaque) bool {
            return true;
        }
        fn attempt(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) self.owner.schedule(self.port());
            return true;
        }
        fn port(self: *@This()) Owner.Port {
            return .{ .ptr = self, .lane = undefined, .owner_id = 1, .io = null, .closing = &self.closing, .wanted = wanted, .attempt = attempt, .name = "test" };
        }
    };
    var owner: Owner = .{};
    var f: F = .{ .owner = &owner };
    owner.state.store(1, .release);
    try owner.run(f.port());
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(u8, 0), owner.state.load(.acquire));
    f.closing.store(true, .release);
    owner.schedule(f.port());
    try std.testing.expectEqual(@as(u8, 0), owner.state.load(.acquire));
}

test "runtime restart owner releases admission when its operation fails without io" {
    const F = struct {
        fn wanted(_: *anyopaque) bool {
            return true;
        }
        fn attempt(_: *anyopaque) !bool {
            return error.InjectedStartFailure;
        }
    };
    var closing: std.atomic.Value(bool) = .init(false);
    var owner: Owner = .{};
    owner.state.store(1, .release);
    try std.testing.expectError(error.InjectedStartFailure, owner.run(.{
        .ptr = &closing,
        .lane = undefined,
        .owner_id = 1,
        .io = null,
        .closing = &closing,
        .wanted = F.wanted,
        .attempt = F.attempt,
        .name = "test",
    }));
    try std.testing.expectEqual(@as(u8, 0), owner.state.load(.acquire));
}
