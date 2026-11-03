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

const std = @import("std");
const queue = @import("writer_queue.zig");
const path_wait = @import("path_lock_wait.zig");

/// Per-process, per-path closed-lease telemetry. Coordination and kernel wait
/// time precede acquisition; hold time includes registration cleanup and ends
/// when the safety flock closes. Snapshots are observational under concurrency.
pub const Stats = struct {
    const Counter = std.atomic.Value(u64);
    leases: Counter = .init(0),
    admission_ns: Counter = .init(0),
    admission_max_ns: Counter = .init(0),
    hold_ns: Counter = .init(0),
    hold_max_ns: Counter = .init(0),
    hold_log2_ns: [64]Counter = @splat(.init(0)),

    pub const Snapshot = struct {
        leases: u64 = 0,
        admission_ns: u64 = 0,
        admission_max_ns: u64 = 0,
        hold_ns: u64 = 0,
        hold_max_ns: u64 = 0,
        hold_log2_ns: [64]u64 = @splat(0),
    };

    fn record(self: *Stats, admission_ns: u64, hold_ns: u64) void {
        _ = self.admission_ns.fetchAdd(admission_ns, .monotonic);
        _ = self.admission_max_ns.fetchMax(admission_ns, .monotonic);
        _ = self.hold_ns.fetchAdd(hold_ns, .monotonic);
        _ = self.hold_max_ns.fetchMax(hold_ns, .monotonic);
        _ = self.hold_log2_ns[std.math.log2_int(u64, @max(1, hold_ns))].fetchAdd(1, .monotonic);
        _ = self.leases.fetchAdd(1, .release);
    }

    pub fn snapshot(self: *const Stats) Snapshot {
        var result: Snapshot = .{
            .leases = self.leases.load(.acquire),
            .admission_ns = self.admission_ns.load(.monotonic),
            .admission_max_ns = self.admission_max_ns.load(.monotonic),
            .hold_ns = self.hold_ns.load(.monotonic),
            .hold_max_ns = self.hold_max_ns.load(.monotonic),
        };
        for (&result.hold_log2_ns, 0..) |*count, index| count.* = self.hold_log2_ns[index].load(.monotonic);
        return result;
    }
};

pub const Lease = struct {
    file: std.Io.File,
    ticket: ?queue.Ticket = null,
    admission_ns: u64 = 0,
    acquired_ns: i96 = 0,
    stats: ?*Stats = null,

    pub fn close(self: *Lease, io: std.Io) void {
        self.file.close(io);
        const released = std.Io.Clock.awake.now(io).nanoseconds;
        if (self.ticket) |*ticket| ticket.close(io);
        if (self.stats) |stats| stats.record(self.admission_ns, @intCast(@max(0, released - self.acquired_ns)));
        self.* = undefined;
    }
};

/// Consumes an unlocked safety descriptor on every path. Queue ownership
/// outlives the safety flock, including when a timed kernel wait is cancelled.
pub fn lock(a: std.mem.Allocator, io: std.Io, unlocked: std.Io.File, safety_path: []const u8, kind: queue.Kind, deadline: ?i96) !Lease {
    const began = std.Io.Clock.awake.now(io).nanoseconds;
    var file = unlocked;
    var file_owned = true;
    var ticket: ?queue.Ticket = null;
    var transferred = false;
    defer if (!transferred) {
        if (file_owned) file.close(io);
        if (ticket) |*owned| owned.close(io);
    };
    ticket = try queue.enroll(a, io, safety_path, kind, deadline, kind == .exclusive);
    if (ticket) |*owned| try owned.waitTurn(io, deadline);
    try io.checkCancel();
    if (ticket) |owned| if (owned.waited) if (deadline) |limit| {
        if (std.Io.Clock.awake.now(io).nanoseconds >= limit) return error.FileBusy;
    };
    const file_kind: std.Io.File.Lock = if (kind == .exclusive) .exclusive else .shared;
    if (!try file.tryLock(io, file_kind)) {
        const limit = deadline orelse return error.WouldBlock;
        if (std.Io.Clock.awake.now(io).nanoseconds >= limit) return error.FileBusy;
        // lockUntil joins both tasks before returning and consumes its input
        // descriptor on failure. Never close it a second time in that case.
        file_owned = false;
        file = try path_wait.lockUntil(file, io, file_kind, limit);
        file_owned = true;
    }
    try io.checkCancel();
    const acquired = std.Io.Clock.awake.now(io).nanoseconds;
    transferred = true;
    return .{
        .file = file,
        .ticket = ticket,
        .admission_ns = @intCast(@max(0, acquired - began)),
        .acquired_ns = acquired,
    };
}

test "path admission timeout releases its ticket and never the existing safety holder" {
    const io = std.testing.io;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const initial = try tmp.dir.createFile(io, "database.lock", .{ .read = true });
    const path = try tmp.dir.realPathFileAlloc(io, "database.lock", a);
    defer a.free(path);
    var first = try lock(a, io, initial, path, .exclusive, null);
    var stats: Stats = .{};
    first.stats = &stats;
    var first_open = true;
    defer if (first_open) first.close(io);
    const second = try tmp.dir.openFile(io, "database.lock", .{ .mode = .read_write });
    try std.testing.expectError(error.FileBusy, lock(a, io, second, path, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_ms));
    const probe = try tmp.dir.openFile(io, "database.lock", .{ .mode = .read_write });
    defer probe.close(io);
    try std.testing.expect(!try probe.tryLock(io, .exclusive));
    first.close(io);
    first_open = false;
    try std.testing.expectEqual(@as(u64, 1), stats.snapshot().leases);
    try std.testing.expect(stats.snapshot().hold_ns >= stats.snapshot().hold_max_ns);
    const third = try tmp.dir.openFile(io, "database.lock", .{ .mode = .read_write });
    var next = try lock(a, io, third, path, .exclusive, null);
    defer next.close(io);
    try std.testing.expect(!try probe.tryLock(io, .exclusive));
}

test "path admission preserves legacy flock safety while its queue is empty" {
    const io = std.testing.io;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const legacy = try tmp.dir.createFile(io, "database.lock", .{ .read = true, .lock = .exclusive });
    var legacy_open = true;
    defer if (legacy_open) legacy.close(io);
    const path = try tmp.dir.realPathFileAlloc(io, "database.lock", a);
    defer a.free(path);
    const contender = try tmp.dir.openFile(io, "database.lock", .{ .mode = .read_write });
    try std.testing.expectError(error.FileBusy, lock(a, io, contender, path, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_ms));
    legacy.close(io);
    legacy_open = false;
    const after = try tmp.dir.openFile(io, "database.lock", .{ .mode = .read_write });
    var next = try lock(a, io, after, path, .exclusive, null);
    defer next.close(io);
}
