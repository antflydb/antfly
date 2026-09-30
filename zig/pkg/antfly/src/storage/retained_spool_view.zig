// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
