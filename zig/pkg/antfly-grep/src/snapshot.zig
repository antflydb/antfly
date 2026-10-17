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

//! Bounded heap snapshot for searches requiring random access. Large decoded
//! streams spill into an owner-only, immediately unlinked temporary file.
const std = @import("std");
const Mapping = @import("filesystem.zig").Mapping;
const A = std.mem.Allocator;
pub const Snapshot = struct {
    bytes: []const u8,
    owned: ?[]u8 = null,
    mapping: ?Mapping = null,
    pub fn read(a: A, io: std.Io, input: *std.Io.Reader, limit: usize, temporary_dir: ?[]const u8) !Snapshot {
        if (@import("builtin").os.tag == .windows) {
            const bytes = try input.allocRemaining(a, .limited(limit));
            return .{ .bytes = bytes, .owned = bytes };
        }
        var memory: std.ArrayList(u8) = .empty;
        defer memory.deinit(a);
        var buffer: [65536]u8 = undefined;
        while (memory.items.len <= 1024 * 1024) {
            const n = try input.readSliceShort(&buffer);
            if (n > limit - memory.items.len) return error.StreamTooLong;
            try memory.appendSlice(a, buffer[0..n]);
            if (n < buffer.len) {
                const bytes = try memory.toOwnedSlice(a);
                return .{ .bytes = bytes, .owned = bytes };
            }
        }
        var nonce: [16]u8 = undefined;
        io.random(&nonce);
        var name_buffer: [80]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buffer, "antfly-grep-{s}.tmp", .{std.fmt.bytesToHex(nonce, .lower)});
        const path = try std.fs.path.join(a, &.{ temporary_dir orelse "/tmp", name });
        defer a.free(path);
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        defer file.close(io);
        // No pathname survives interruption; only this open descriptor can
        // access the source snapshot after creation.
        std.Io.Dir.cwd().deleteFile(io, path) catch |err| {
            std.Io.Dir.cwd().deleteFile(io, path) catch {};
            return err;
        };
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(memory.items);
        var size = memory.items.len;
        memory.deinit(a);
        memory = .empty;
        while (true) {
            const n = input.stream(&writer.interface, .limited(limit - size)) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            size += n;
            if (size == limit) {
                _ = input.peekByte() catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                return error.StreamTooLong;
            }
        }
        try writer.interface.flush();
        const mapping = try Mapping.open(io, file, size);
        return .{ .bytes = mapping.memory, .mapping = mapping };
    }
    pub fn deinit(self: *Snapshot, a: A, io: std.Io) void {
        if (self.owned) |bytes| a.free(bytes);
        if (self.mapping) |*mapping| mapping.deinit(io);
        self.* = undefined;
    }
};
