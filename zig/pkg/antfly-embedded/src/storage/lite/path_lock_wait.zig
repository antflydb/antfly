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

const LockResult = struct {
    result: std.Io.File.LockError!void,
    acquired_ns: i96,
};

fn acquire(file: std.Io.File, io: std.Io, kind: std.Io.File.Lock) LockResult {
    file.lock(io, kind) catch |err| return .{ .result = err, .acquired_ns = 0 };
    return .{ .result = {}, .acquired_ns = std.Io.Clock.awake.now(io).nanoseconds };
}

fn expire(io: std.Io, deadline_ns: i96) std.Io.Cancelable!void {
    const remaining = deadline_ns - std.Io.Clock.awake.now(io).nanoseconds;
    if (remaining > 0) try io.sleep(.fromNanoseconds(remaining), .awake);
}

/// Consumes the unlocked descriptor, returning it only on success. In
/// particular, cancellation can race a successful kernel lock: join both
/// tasks before closing the descriptor so neither a lock nor a waiter escapes.
pub fn lockUntil(file: std.Io.File, io: std.Io, kind: std.Io.File.Lock, deadline_ns: i96) !std.Io.File {
    errdefer file.close(io);
    const Event = union(enum) { locked: LockResult, expired: std.Io.Cancelable!void };
    var buffer: [2]Event = undefined;
    var select = std.Io.Select(Event).init(io, &buffer);
    defer select.cancelDiscard();
    try select.concurrent(.locked, acquire, .{ file, io, kind });
    try select.concurrent(.expired, expire, .{ io, deadline_ns });
    switch (try select.await()) {
        .locked => |result| {
            try result.result;
            if (result.acquired_ns > deadline_ns) return error.FileBusy;
            return file;
        },
        .expired => |result| {
            try result;
            return error.FileBusy;
        },
    }
}

fn releaseLater(file: std.Io.File, io: std.Io) void {
    io.sleep(.fromMilliseconds(10), .awake) catch {};
    file.close(io);
}

test "lite path lock deadline closes the waiting descriptor" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const holder = try tmp.dir.createFile(io, "lock", .{ .read = true, .lock = .exclusive });
    const waiter = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    try std.testing.expectError(error.FileBusy, lockUntil(waiter, io, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + 20 * std.time.ns_per_ms));
    holder.close(io);
    const probe = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    defer probe.close(io);
    try std.testing.expect(try probe.tryLock(io, .exclusive));
}

test "lite path lock queued acquisition retains exclusive ownership" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const holder = try tmp.dir.createFile(io, "lock", .{ .read = true, .lock = .exclusive });
    var release = try std.Io.concurrent(io, releaseLater, .{ holder, io });
    defer release.cancel(io);
    const waiter = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    const held = try lockUntil(waiter, io, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    const probe = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    defer probe.close(io);
    try std.testing.expect(!try probe.tryLock(io, .exclusive));
    held.close(io);
    try std.testing.expect(try probe.tryLock(io, .exclusive));
}

test "lite path lock cancellation joins the kernel waiter" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const holder = try tmp.dir.createFile(io, "lock", .{ .read = true, .lock = .exclusive });
    const waiter = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    var pending = try std.Io.concurrent(io, lockUntil, .{ waiter, io, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s });
    try io.sleep(.fromMilliseconds(10), .awake);
    const result = pending.cancel(io);
    // Cancellation may arrive before the task starts; either way it owns and
    // closes the descriptor before returning to the caller.
    try std.testing.expectError(error.Canceled, result);
    holder.close(io);
    const probe = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    defer probe.close(io);
    try std.testing.expect(try probe.tryLock(io, .exclusive));
}

test "lite path lock exhausted executor cancels an already queued lock" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer threaded.deinit();
    const io = threaded.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const holder = try tmp.dir.createFile(io, "lock", .{ .read = true, .lock = .exclusive });
    const waiter = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    try std.testing.expectError(error.ConcurrencyUnavailable, lockUntil(waiter, io, .exclusive, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s));
    holder.close(io);
    const probe = try tmp.dir.openFile(io, "lock", .{ .mode = .read_write });
    defer probe.close(io);
    try std.testing.expect(try probe.tryLock(io, .exclusive));
}
