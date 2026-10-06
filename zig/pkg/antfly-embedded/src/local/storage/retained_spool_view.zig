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

//! Stable, bounded file-backed source for an authenticated retained frame.
const std = @import("std");
const retained = @import("retained_frame.zig");
const native = @import("db/native_backup.zig");

pub const Source = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    descriptor: []u8,
    cache: retained.View.ChunkCache,
    view: retained.View,

    pub fn open(alloc: std.mem.Allocator, io: std.Io, path: []const u8, descriptor_path: []const u8, sequence: u64, digest: [32]u8, total: u32) !*Source {
        const self = try alloc.create(Source);
        errdefer alloc.destroy(self);
        const descriptor = try native.readFileAlloc(alloc, io, descriptor_path, 272 * 1024);
        errdefer alloc.free(descriptor);
        const buffer = try alloc.alloc(u8, retained.chunk_bytes);
        errdefer alloc.free(buffer);
        const file = if (std.fs.path.isAbsolute(path)) try std.Io.Dir.openFileAbsolute(io, path, .{}) else try std.Io.Dir.cwd().openFile(io, path, .{});
        errdefer file.close(io);
        self.* = .{ .alloc = alloc, .io = io, .file = file, .descriptor = descriptor, .cache = .{ .bytes = buffer }, .view = undefined };
        self.view = try retained.View.fromDescriptor(descriptor, sequence, .{ .context = self, .read_chunk = readChunk, .corruption_error = error.RestoreSpoolCorrupt });
        if (self.view.total != total or !std.mem.eql(u8, &self.view.descriptor_digest, &digest)) return error.RetainedEffectsCorrupt;
        return self;
    }

    fn readChunk(ptr: *anyopaque, _: u64, ordinal: u32, out: []u8) !usize {
        const self: *Source = @ptrCast(@alignCast(ptr));
        return self.file.readPositionalAll(self.io, out, @as(u64, ordinal) * retained.chunk_bytes);
    }

    pub fn destroy(self: *Source) void {
        const alloc = self.alloc;
        self.file.close(self.io);
        alloc.free(self.descriptor);
        alloc.free(self.cache.bytes);
        alloc.destroy(self);
    }
};
