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

//! Supplied-Io model artifact operations. Only physical I/O lends the current
//! synchronous execution grant; allocation, hashing and parsing stay runnable.
const std = @import("std");
const execution = @import("../execution_control.zig");

fn discard(file: std.Io.File, args: anytype) void {
    file.close(args[1]);
}

pub fn open(io: std.Io, path: []const u8) !std.Io.File {
    return execution.ownedPhysicalWait(std.Io.Dir.openFile, .{ std.Io.Dir.cwd(), io, path, std.Io.Dir.OpenFileOptions{} }, discard);
}

pub fn stat(io: std.Io, path: []const u8, options: std.Io.Dir.StatFileOptions) !std.Io.File.Stat {
    return execution.physicalWait(std.Io.Dir.statFile, .{ std.Io.Dir.cwd(), io, path, options });
}

pub fn access(dir: std.Io.Dir, io: std.Io, path: []const u8) !void {
    return execution.physicalWait(std.Io.Dir.access, .{ dir, io, path, std.Io.Dir.AccessOptions{} });
}

pub fn realPathAlloc(a: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const count = try execution.physicalWait(std.Io.Dir.realPathFile, .{ std.Io.Dir.cwd(), io, path, &buffer });
    return a.dupe(u8, buffer[0..count]);
}

pub fn read(file: std.Io.File, io: std.Io, buffer: []u8) !usize {
    return execution.physicalWait(std.Io.File.readStreaming, .{ file, io, &.{buffer} }) catch |err| switch (err) {
        error.EndOfStream => 0,
        else => return err,
    };
}

pub fn readAlloc(a: std.mem.Allocator, io: std.Io, path: []const u8, limit: usize) ![]u8 {
    const file = try open(io, path);
    defer file.close(io);
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(a);
    var buffer: [64 * 1024]u8 = undefined;
    while (true) {
        const count = try read(file, io, &buffer);
        if (count == 0) break;
        if (count > limit -| output.items.len) return error.StreamTooLong;
        try output.appendSlice(a, buffer[0..count]);
    }
    return output.toOwnedSlice(a);
}

pub fn write(io: std.Io, path: []const u8, bytes: []const u8) !void {
    const file = try execution.ownedPhysicalWait(std.Io.Dir.createFile, .{ std.Io.Dir.cwd(), io, path, std.Io.Dir.CreateFileOptions{} }, discard);
    defer file.close(io);
    try execution.physicalWait(std.Io.File.writeStreamingAll, .{ file, io, bytes });
}
