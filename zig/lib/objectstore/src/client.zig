// Copyright 2026 Antfly, Inc.
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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const file_transfer_chunk_bytes: u64 = 8 * 1024 * 1024;
const fallback_upload_limit_bytes: u64 = 64 * 1024 * 1024;
const max_list_page_keys: u32 = 10_000;

pub const Client = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        bucket_exists: *const fn (*anyopaque, []const u8) anyerror!bool,
        make_bucket: *const fn (*anyopaque, []const u8) anyerror!void,
        put_object: *const fn (*anyopaque, Allocator, []const u8, []const u8, []const u8, types.PutOptions) anyerror!types.PutResult,
        put_file: ?*const fn (*anyopaque, Allocator, std.Io, []const u8, []const u8, []const u8, types.PutOptions) anyerror!types.PutResult = null,
        get_object: *const fn (*anyopaque, Allocator, []const u8, []const u8, types.GetOptions) anyerror!types.GetResult,
        get_object_attributes: *const fn (*anyopaque, Allocator, []const u8, []const u8) anyerror!types.ObjectAttributes,
        stat_object: *const fn (*anyopaque, Allocator, []const u8, []const u8) anyerror!types.ObjectMetadata,
        delete_object: *const fn (*anyopaque, []const u8, []const u8, types.DeleteOptions) anyerror!void,
        list_objects: *const fn (*anyopaque, Allocator, []const u8, types.ListOptions) anyerror!types.ListResult,
    };

    pub fn deinit(self: *Client) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn bucketExists(self: *Client, bucket: []const u8) !bool {
        return try self.vtable.bucket_exists(self.ptr, bucket);
    }

    pub fn makeBucket(self: *Client, bucket: []const u8) !void {
        try self.vtable.make_bucket(self.ptr, bucket);
    }

    pub fn putObject(self: *Client, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        return try self.vtable.put_object(self.ptr, self.allocator, bucket, key, body, opts);
    }

    pub fn putFile(self: *Client, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        return try self.putFileWithIo(io_impl.io(), bucket, key, src_path, opts);
    }

    pub fn putFileWithIo(self: *Client, io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        if (self.vtable.put_file) |put_file| return try put_file(self.ptr, self.allocator, io, bucket, key, src_path, opts);
        const file = try openFilePath(io, src_path);
        defer file.close(io);
        const stat = try file.stat(io);
        if (stat.size > fallback_upload_limit_bytes) return error.StreamingUploadUnsupported;
        const body = try self.allocator.alloc(u8, @intCast(stat.size));
        defer self.allocator.free(body);
        if (try file.readPositionalAll(io, body, 0) != body.len) return error.SourceFileChanged;
        var extra: [1]u8 = undefined;
        if (try file.readPositionalAll(io, &extra, stat.size) != 0) return error.SourceFileChanged;
        return try self.putObject(bucket, key, body, opts);
    }

    pub fn getObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        return try self.vtable.get_object(self.ptr, self.allocator, bucket, key, opts);
    }

    pub fn getFile(self: *Client, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        return try self.getFileWithIo(io_impl.io(), bucket, key, dest_path, opts);
    }

    pub fn getFileWithIo(self: *Client, io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        if (opts.version_id == null and opts.range == null and opts.if_match_etag == null and opts.part_number == null) {
            return try self.getWholeFileWithIo(io, bucket, key, dest_path);
        }
        var object = try self.getObject(bucket, key, opts);
        defer object.deinit(self.allocator);
        try ensureParentDir(io, dest_path);
        try writeFileAtomically(io, dest_path, object.body);
    }

    fn getWholeFileWithIo(self: *Client, io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8) !void {
        var meta = try self.statObject(bucket, key);
        defer meta.deinit(self.allocator);
        try ensureParentDir(io, dest_path);
        const tmp_path = try tempPathAlloc(dest_path, io);
        defer std.heap.page_allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};

        {
            var file = try createFilePath(io, tmp_path);
            defer file.close(io);
            var buffer: [4096]u8 = undefined;
            var writer = file.writer(io, &buffer);
            var offset: u64 = 0;
            while (offset < meta.content_length) {
                const length = @min(file_transfer_chunk_bytes, meta.content_length - offset);
                var part = try self.getObject(bucket, key, .{
                    .range = .{ .offset = offset, .length = length },
                    .if_match_etag = meta.etag,
                });
                defer part.deinit(self.allocator);
                if (part.body.len != length) return error.ShortObjectRead;
                try writer.interface.writeAll(part.body);
                offset += length;
            }
            try writer.end();
        }
        try renameFilePath(io, tmp_path, dest_path);
    }

    pub fn getObjectAttributes(self: *Client, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        return try self.vtable.get_object_attributes(self.ptr, self.allocator, bucket, key);
    }

    pub fn statObject(self: *Client, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        return try self.vtable.stat_object(self.ptr, self.allocator, bucket, key);
    }

    pub fn deleteObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        try self.vtable.delete_object(self.ptr, bucket, key, opts);
    }

    pub fn listObjects(self: *Client, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        if (opts.max_keys == 0) return error.InvalidPageSize;
        if (opts.max_keys > max_list_page_keys) return error.PageSizeTooLarge;
        if (opts.start_after != null and opts.continuation_token != null) return error.AmbiguousContinuation;
        return try self.vtable.list_objects(self.ptr, self.allocator, bucket, opts);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    try std.Io.Dir.cwd().createDirPath(io, parent);
}

fn openFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
}

fn writeFileAtomically(io: std.Io, path: []const u8, contents: []const u8) !void {
    const tmp_path = try tempPathAlloc(path, io);
    defer std.heap.page_allocator.free(tmp_path);
    errdefer deleteFilePath(io, tmp_path) catch {};

    {
        var file = try createFilePath(io, tmp_path);
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    try renameFilePath(io, tmp_path, path);
}

fn tempPathAlloc(path: []const u8, io: std.Io) ![]u8 {
    return try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-objectstore-{d}", .{ path, uniqueNs(io) });
}

fn createFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
}

fn deleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.deleteFileAbsolute(io, path)
    else
        try std.Io.Dir.cwd().deleteFile(io, path);
}

fn renameFilePath(io: std.Io, source: []const u8, destination: []const u8) !void {
    if (std.fs.path.isAbsolute(destination))
        try std.Io.Dir.renameAbsolute(source, destination, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), source, std.Io.Dir.cwd(), destination, io);
}

var unique_counter: std.atomic.Value(u64) = .init(0);

fn uniqueNs(io: std.Io) u64 {
    const now = std.Io.Timestamp.now(io, .awake);
    return @as(u64, @intCast(now.toNanoseconds())) +% unique_counter.fetchAdd(1, .monotonic);
}
