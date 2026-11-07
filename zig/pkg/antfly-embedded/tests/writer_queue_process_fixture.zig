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
const queue = @import("writer_queue");
const path_wait = @import("writer_queue").path_lock_wait;
const platform = @import("antfly_platform");

fn announce(io: std.Io, event: []const u8, round: usize) !void {
    var buffer: [64]u8 = undefined;
    try std.Io.File.stdout().writeStreamingAll(io, try std.fmt.bufPrint(&buffer, "{s} {d}\n", .{ event, round }));
}

fn run(init: std.process.Init) !void {
    var arguments = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer arguments.deinit();
    _ = arguments.next();
    const path = arguments.next() orelse return error.MissingPath;
    const kind: std.Io.File.Lock = if (std.mem.eql(u8, arguments.next() orelse return error.MissingKind, "read")) .shared else .exclusive;
    const timeout_ms = try std.fmt.parseUnsigned(u64, arguments.next() orelse "5000", 10);
    const rounds = try std.fmt.parseUnsigned(usize, arguments.next() orelse "1", 10);
    for (0..rounds) |round| {
        {
            var turn: ?queue.Turn = null;
            defer if (turn) |*owned| owned.release(init.io);
            var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .read = true, .truncate = false });
            var file_owned = true;
            defer if (file_owned) file.close(init.io);
            const deadline = std.Io.Clock.awake.now(init.io).nanoseconds + @as(i96, timeout_ms) * std.time.ns_per_ms;
            if (kind == .exclusive) {
                var writer_queue = try queue.Queue.open(init.gpa, init.io, path);
                var queue_owned = true;
                defer if (queue_owned) writer_queue.close(init.io);
                var tail = try writer_queue.lockTail(init.io, deadline);
                var tail_held = true;
                defer if (tail_held) tail.release(init.io);
                var ticket = try writer_queue.enqueue(init.io, &tail);
                tail.release(init.io);
                tail_held = false;
                announce(init.io, "enrolled", round) catch |err| {
                    // An unlocked waiting ticket is skipped as dead, while
                    // successors still check every preceding live ticket.
                    ticket.file.close(init.io);
                    return err;
                };
                try writer_queue.awaitTurn(init.io, &ticket, deadline);
                turn = .{ .queue = writer_queue, .ticket = ticket };
                queue_owned = false;
            } else {
                // Production readers use the shared safety flock. Writer
                // ordering belongs to the queue, independently of readers.
                try announce(init.io, "enrolled", round);
            }
            if (!try file.tryLock(init.io, kind)) {
                file_owned = false;
                file = try path_wait.lockUntil(file, init.io, kind, deadline);
                file_owned = true;
            }
            try announce(init.io, "acquired", round);
            var command: [1]u8 = undefined;
            if (try std.Io.File.stdin().readStreaming(init.io, &.{&command}) == 0) return;
            if (command[0] != 'r') return error.InvalidCommand;
        }
        try announce(init.io, "released", round);
    }
}

pub fn main(init: std.process.Init) !void {
    run(init) catch |err| {
        if (err == error.FileBusy or err == error.WouldBlock) {
            try announce(init.io, "busy", 0);
            platform.process.exitImmediately(75);
        }
        return err;
    };
}
