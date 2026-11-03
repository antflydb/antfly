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
const path_wait = @import("path_lock_wait");
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
    const kind: queue.Kind = if (std.mem.eql(u8, arguments.next() orelse return error.MissingKind, "read")) .shared else .exclusive;
    const timeout_ms = try std.fmt.parseUnsigned(u64, arguments.next() orelse "5000", 10);
    const rounds = try std.fmt.parseUnsigned(usize, arguments.next() orelse "1", 10);
    for (0..rounds) |round| {
        {
            var file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .read = true, .truncate = false });
            var file_owned = true;
            var ticket: ?queue.Ticket = null;
            defer {
                if (file_owned) file.close(init.io);
                if (ticket) |*owned| owned.close(init.io);
            }
            const deadline = std.Io.Clock.awake.now(init.io).nanoseconds + @as(i96, timeout_ms) * std.time.ns_per_ms;
            // Create even for read actors so the cohort case exercises the
            // production queue, rather than the sidecar-free reader fallback.
            ticket = try queue.enroll(init.gpa, init.io, path, kind, deadline, true);
            if (ticket == null) return error.FileLocksUnsupported;
            try announce(init.io, "enrolled", round);
            try ticket.?.waitTurn(init.io, deadline);
            const file_kind: std.Io.File.Lock = if (kind == .shared) .shared else .exclusive;
            if (!try file.tryLock(init.io, file_kind)) {
                file_owned = false;
                file = try path_wait.lockUntil(file, init.io, file_kind, deadline);
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
