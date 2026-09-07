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

//! Private backing for archive schema history. Only a compact offset directory
//! stays in memory; serialized schemas spill to a task-owned file on hosts.
//! Freestanding callers have no filesystem and use the same interface in memory.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

pub const Spool = struct {
    pub const Ref = struct { offset: u64, len: usize };
    alloc: std.mem.Allocator,
    io_impl: ?*std.Io.Threaded = null,
    file: ?std.Io.File = null,
    path: ?[]u8 = null,
    size: u64 = 0,
    memory: std.ArrayListUnmanaged(u8) = .empty,

    pub fn deinit(self: *Spool) void {
        if (comptime builtin.os.tag != .freestanding) if (self.io_impl) |impl| {
            if (self.file) |file| file.close(impl.io());
            if (self.path) |path| {
                std.Io.Dir.cwd().deleteFile(impl.io(), path) catch {};
                self.alloc.free(path);
            }
            impl.deinit();
            self.alloc.destroy(impl);
        };
        self.memory.deinit(self.alloc);
        self.* = undefined;
    }

    fn ensureFile(self: *Spool) !void {
        if (self.file != null) return;
        const impl = try self.alloc.create(std.Io.Threaded);
        errdefer self.alloc.destroy(impl);
        impl.* = std.Io.Threaded.init(self.alloc, .{});
        errdefer impl.deinit();
        const io = impl.io();
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const directory = platform.env.getenv("TMPDIR") orelse platform.env.getenv("TEMP") orelse "/tmp";
        const path = try std.fmt.allocPrint(self.alloc, "{s}/antfly-schema-{x}.tmp", .{ directory, random });
        errdefer self.alloc.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        self.io_impl = impl;
        self.path = path;
        self.file = file;
        // POSIX permits unlinking an open file: cancellation and process death
        // then reclaim it without requiring an orphan-file scavenger.
        if (comptime builtin.os.tag != .windows) {
            std.Io.Dir.cwd().deleteFile(io, path) catch return;
            self.alloc.free(path);
            self.path = null;
        }
    }

    pub fn append(self: *Spool, bytes: []const u8) !Ref {
        const next = try std.math.add(u64, self.size, bytes.len);
        if (comptime builtin.os.tag == .freestanding) {
            try self.memory.appendSlice(self.alloc, bytes);
        } else {
            try self.ensureFile();
            try self.file.?.writePositionalAll(self.io_impl.?.io(), bytes, self.size);
        }
        const result: Ref = .{ .offset = self.size, .len = bytes.len };
        self.size = next;
        return result;
    }

    pub fn read(self: *Spool, alloc: std.mem.Allocator, ref: Ref) ![]u8 {
        const bytes = try alloc.alloc(u8, ref.len);
        errdefer alloc.free(bytes);
        if (comptime builtin.os.tag == .freestanding) {
            @memcpy(bytes, self.memory.items[@intCast(ref.offset)..][0..ref.len]);
        } else {
            if (try self.file.?.readPositionalAll(self.io_impl.?.io(), bytes, ref.offset) != bytes.len)
                return error.InvalidMetadataBatch;
        }
        return bytes;
    }
};
