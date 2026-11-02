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
const fence = @import("platform").process_generation_fence;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    const executable = args.next() orelse return error.MissingExecutable;
    const mode = args.next() orelse return error.MissingMode;
    if (std.mem.eql(u8, mode, "child")) {
        // Deliberately ignore parent death. The locked descriptor was already
        // inherited at exec, before this child performs any initialization.
        var byte: [1]u8 = undefined;
        if (try std.Io.File.stdin().readStreaming(init.io, &.{&byte}) != 1) return;
        try std.Io.File.stdout().writeStreamingAll(init.io, "child-ready\n");
        while (true) {
            _ = std.Io.File.stdin().readStreaming(init.io, &.{&byte}) catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };
        }
    }
    const path = args.next() orelse return error.MissingDirectory;
    const dir = try std.Io.Dir.cwd().openDir(init.io, path, .{});
    defer dir.close(init.io);
    var lease = fence.Lease.acquire(init.io, dir, "inference-worker-generation.lock") catch |err| switch (err) {
        error.ProcessGenerationBusy => std.process.exit(17),
        else => return err,
    };
    defer lease.close(init.io);
    if (std.mem.eql(u8, mode, "probe")) return;
    if (std.mem.eql(u8, mode, "close-twice")) {
        lease.close(init.io);
        lease.close(init.io);
        var next = try fence.Lease.acquire(init.io, dir, "inference-worker-generation.lock");
        next.close(init.io);
        return;
    }
    const worker_lifetime = std.mem.eql(u8, mode, "parent-lifetime");
    const child_executable = if (worker_lifetime) args.next() orelse return error.MissingWorkerFixture else executable;
    var child = try std.process.spawn(init.io, .{
        .argv = &.{ child_executable, if (worker_lifetime) "owner-loss" else "child" },
        .stdin = if (worker_lifetime) .pipe else .inherit,
        .stdout = .inherit,
        .stderr = .inherit,
        .inherit_files = &.{lease.inheritedFile()},
    });
    errdefer child.kill(init.io);
    if (std.mem.eql(u8, mode, "parent-close")) lease.close(init.io);
    var buffer: [64]u8 = undefined;
    const line = try std.fmt.bufPrint(&buffer, "child {d}\n", .{child.id.?});
    try std.Io.File.stdout().writeStreamingAll(init.io, line);
    const term = try child.wait(init.io);
    switch (term) {
        .exited => |code| if (code != 0) return error.ChildFailed,
        else => return error.ChildFailed,
    }
}
