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

//! Model artifact discovery: directory waits lend only physical work. Paths
//! and traversal frames are allocated after checked CPU return. Every opened
//! directory remains owned through refusal, allocation failure and early stop.
const std = @import("std");
const execution = @import("../execution_control.zig");
const Dir = std.Io.Dir;

pub const Walker = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    frames: std.ArrayList(Frame) = .empty,
    path: std.ArrayList(u8) = .empty,
    const Frame = struct { iterator: Dir.Iterator, prefix_len: usize };

    pub fn init(alloc: std.mem.Allocator, io: std.Io, root: []const u8) !Walker {
        const directory = try open(Dir.cwd(), io, root);
        errdefer directory.close(io);
        var self: Walker = .{ .alloc = alloc, .io = io };
        try self.frames.append(alloc, .{ .iterator = directory.iterate(), .prefix_len = 0 });
        return self;
    }

    fn discard(directory: Dir, args: anytype) void {
        directory.close(args[1]);
    }
    fn open(parent: Dir, io: std.Io, name: []const u8) !Dir {
        return execution.ownedPhysicalWait(Dir.openDir, .{ parent, io, name, Dir.OpenOptions{ .iterate = true } }, discard);
    }

    /// Borrowed until next() or deinit(). Descend only with enter(), so callers
    /// retain their existing hidden-directory and artifact selection policies.
    pub fn next(self: *Walker) !?Dir.Walker.Entry {
        try (execution.InferenceExecutionControl{}).check();
        while (self.frames.items.len != 0) {
            const frame = &self.frames.items[self.frames.items.len - 1];
            // Iterator.next uses its fixed buffer and allocates no paths.
            const entry = try execution.physicalWait(Dir.Iterator.next, .{ &frame.iterator, self.io }) orelse {
                self.closeTop();
                continue;
            };
            self.path.shrinkRetainingCapacity(frame.prefix_len);
            const name_start = frame.prefix_len + @intFromBool(frame.prefix_len != 0);
            if (frame.prefix_len != 0) try self.path.append(self.alloc, std.fs.path.sep);
            try self.path.appendSlice(self.alloc, entry.name);
            try self.path.append(self.alloc, 0);
            return .{
                .dir = frame.iterator.reader.dir,
                .basename = self.path.items[name_start .. self.path.items.len - 1 :0],
                .path = self.path.items[0 .. self.path.items.len - 1 :0],
                .kind = entry.kind,
            };
        }
        return null;
    }

    pub fn enter(self: *Walker, entry: Dir.Walker.Entry) !void {
        std.debug.assert(entry.kind == .directory);
        const directory = try open(entry.dir, self.io, entry.basename);
        errdefer directory.close(self.io);
        try self.frames.append(self.alloc, .{ .iterator = directory.iterateAssumeFirstIteration(), .prefix_len = self.path.items.len - 1 });
    }

    fn closeTop(self: *Walker) void {
        const frame = self.frames.pop().?;
        frame.iterator.reader.dir.close(self.io);
    }
    pub fn deinit(self: *Walker) void {
        while (self.frames.items.len != 0) self.closeTop();
        self.frames.deinit(self.alloc);
        self.path.deinit(self.alloc);
        self.* = undefined;
    }
};
