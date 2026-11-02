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

//! Bounded credential reads. Only physical open/read operations lend CPU;
//! allocation, growth, parsing and ownership transfer remain on the caller.
const std = @import("std");
const Control = @import("httpx").socket.WriteControl;

fn open(io: std.Io, path: []const u8) !std.Io.File {
    return std.Io.Dir.cwd().openFile(io, path, .{});
}
fn discard(file: std.Io.File, args: anytype) void {
    file.close(args[0]);
}

pub fn readAlloc(alloc: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize, control: ?Control) ![]u8 {
    const wait = control orelse return std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bytes));
    var rejected = false;
    const file = try wait.withOwnedWait(&rejected, open, .{ io, path }, discard);
    defer file.close(io);
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(alloc);
    var buffer: [16 * 1024]u8 = undefined;
    var streaming = false;
    while (true) {
        const room = max_bytes - bytes.items.len;
        const dest = buffer[0..@min(buffer.len, room +| 1)];
        const vectors: []const []u8 = &.{dest};
        const count = if (streaming)
            try wait.withWait(std.Io.File.readStreaming, .{ file, io, vectors })
        else
            wait.withWait(std.Io.File.readPositional, .{ file, io, vectors, @as(u64, bytes.items.len) }) catch |err| switch (err) {
                error.Unseekable => {
                    streaming = true;
                    continue;
                },
                else => return err,
            };
        if (count == 0) break;
        if (count > room) return error.StreamTooLong;
        try bytes.appendSlice(alloc, dest[0..count]);
    }
    const result = try bytes.toOwnedSlice(alloc);
    errdefer alloc.free(result);
    try wait.check();
    return result;
}
