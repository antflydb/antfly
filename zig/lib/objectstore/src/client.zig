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
        bucket_exists: *const fn (*anyopaque, []const u8, types.BucketOptions) anyerror!bool,
        make_bucket: *const fn (*anyopaque, []const u8, types.BucketOptions) anyerror!void,
        put_object: *const fn (*anyopaque, Allocator, []const u8, []const u8, []const u8, types.PutOptions) anyerror!types.PutResult,
        put_file: ?*const fn (*anyopaque, Allocator, std.Io, []const u8, []const u8, []const u8, types.PutOptions) anyerror!types.PutResult = null,
        get_file: ?*const fn (*anyopaque, Allocator, std.Io, []const u8, []const u8, []const u8) anyerror!void = null,
        get_file_with_options: ?*const fn (*anyopaque, Allocator, std.Io, []const u8, []const u8, []const u8, types.GetOptions) anyerror!void = null,
        get_prefix: ?*const fn (*anyopaque, Allocator, std.Io, []const u8, []const u8, []const u8) anyerror!usize = null,
        get_object: *const fn (*anyopaque, Allocator, []const u8, []const u8, types.GetOptions) anyerror!types.GetResult,
        get_object_attributes: *const fn (*anyopaque, Allocator, []const u8, []const u8) anyerror!types.ObjectAttributes,
        stat_object: *const fn (*anyopaque, Allocator, []const u8, []const u8) anyerror!types.ObjectMetadata,
        stat_object_with_options: ?*const fn (*anyopaque, Allocator, []const u8, []const u8, types.StatOptions) anyerror!types.ObjectMetadata = null,
        delete_object: *const fn (*anyopaque, []const u8, []const u8, types.DeleteOptions) anyerror!void,
        list_objects: *const fn (*anyopaque, Allocator, []const u8, types.ListOptions) anyerror!types.ListResult,
        list_object_versions: ?*const fn (*anyopaque, Allocator, []const u8, types.ListObjectVersionsOptions) anyerror!types.ListObjectVersionsResult = null,
    };

    pub fn deinit(self: *Client) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn bucketExists(self: *Client, bucket: []const u8) !bool {
        return try self.bucketExistsWithOptions(bucket, .{});
    }

    pub fn bucketExistsWithOptions(self: *Client, bucket: []const u8, opts: types.BucketOptions) !bool {
        if (opts.cancellation) |token| try token.check();
        return try self.vtable.bucket_exists(self.ptr, bucket, opts);
    }

    pub fn makeBucket(self: *Client, bucket: []const u8) !void {
        try self.makeBucketWithOptions(bucket, .{});
    }

    pub fn makeBucketWithOptions(self: *Client, bucket: []const u8, opts: types.BucketOptions) !void {
        if (opts.cancellation) |token| try token.check();
        try self.vtable.make_bucket(self.ptr, bucket, opts);
    }

    pub fn putObject(self: *Client, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        if (opts.cancellation) |token| try token.check();
        return try self.vtable.put_object(self.ptr, self.allocator, bucket, key, body, opts);
    }

    pub fn putFile(self: *Client, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        return try self.putFileWithIo(io_impl.io(), bucket, key, src_path, opts);
    }

    pub fn putFileWithIo(self: *Client, io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        if (opts.cancellation) |token| try token.check();
        if (self.vtable.put_file) |put_file| {
            return try put_file(self.ptr, self.allocator, io, bucket, key, src_path, opts);
        }
        const file = try openFilePath(io, src_path);
        defer file.close(io);
        const stat = try file.stat(io);
        if (stat.size > fallback_upload_limit_bytes) return error.StreamingUploadUnsupported;
        const body = try self.allocator.alloc(u8, @intCast(stat.size));
        defer self.allocator.free(body);
        if (try file.readPositionalAll(io, body, 0) != body.len) return error.SourceFileChanged;
        var extra: [1]u8 = undefined;
        if (try file.readPositionalAll(io, &extra, stat.size) != 0) return error.SourceFileChanged;
        if (opts.cancellation) |token| try token.check();
        return try self.putObject(bucket, key, body, opts);
    }

    pub fn getObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        if (opts.cancellation) |token| try token.check();
        var result = try self.vtable.get_object(self.ptr, self.allocator, bucket, key, opts);
        errdefer result.deinit(self.allocator);
        if (opts.cancellation) |token| try token.check();
        return result;
    }

    pub fn getFile(self: *Client, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        return try self.getFileWithIo(io_impl.io(), bucket, key, dest_path, opts);
    }

    pub fn getFileWithIo(self: *Client, io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        if (opts.version_id == null and
            opts.range == null and
            opts.if_match_etag == null and
            opts.part_number == null and
            opts.max_response_bytes == null)
        {
            return try self.getWholeFileWithIo(io, bucket, key, dest_path, opts.cancellation);
        }
        var object = try self.getObject(bucket, key, opts);
        defer object.deinit(self.allocator);
        try ensureParentDir(io, dest_path);
        try writeFileAtomically(io, dest_path, object.body);
    }

    fn getWholeFileWithIo(
        self: *Client,
        io: std.Io,
        bucket: []const u8,
        key: []const u8,
        dest_path: []const u8,
        cancellation: ?types.CancellationToken,
    ) !void {
        if (cancellation) |token| try token.check();
        if (self.vtable.get_file_with_options) |get_file_with_options| {
            return try get_file_with_options(self.ptr, self.allocator, io, bucket, key, dest_path, .{ .cancellation = cancellation });
        }
        if (cancellation == null) {
            if (self.vtable.get_file) |get_file| return try get_file(self.ptr, self.allocator, io, bucket, key, dest_path);
        }
        var meta = try self.statObjectWithOptions(bucket, key, .{ .cancellation = cancellation });
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
                    .max_response_bytes = @intCast(length),
                    .cancellation = cancellation,
                });
                defer part.deinit(self.allocator);
                if (part.body.len != length) return error.ShortObjectRead;
                try writer.interface.writeAll(part.body);
                offset += length;
            }
            try writer.end();
        }
        if (cancellation) |token| try token.check();
        try renameFilePath(io, tmp_path, dest_path);
    }

    pub fn getObjectAttributes(self: *Client, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        return try self.vtable.get_object_attributes(self.ptr, self.allocator, bucket, key);
    }

    /// Returns null when the provider has no native prefix transfer. A
    /// non-null count is the number of files atomically published below dest.
    pub fn getPrefixWithIo(self: *Client, io: std.Io, bucket: []const u8, prefix: []const u8, dest_path: []const u8) !?usize {
        const get_prefix = self.vtable.get_prefix orelse return null;
        return try get_prefix(self.ptr, self.allocator, io, bucket, prefix, dest_path);
    }

    pub fn statObject(self: *Client, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        return try self.statObjectWithOptions(bucket, key, .{});
    }

    pub fn statObjectWithOptions(self: *Client, bucket: []const u8, key: []const u8, opts: types.StatOptions) !types.ObjectMetadata {
        if (opts.cancellation) |token| try token.check();
        var metadata = if (self.vtable.stat_object_with_options) |stat_with_options|
            try stat_with_options(self.ptr, self.allocator, bucket, key, opts)
        else
            try self.vtable.stat_object(self.ptr, self.allocator, bucket, key);
        errdefer metadata.deinit(self.allocator);
        if (opts.cancellation) |token| try token.check();
        return metadata;
    }

    pub fn deleteObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        if (opts.cancellation) |token| try token.check();
        try self.vtable.delete_object(self.ptr, bucket, key, opts);
        if (opts.cancellation) |token| try token.check();
    }

    pub fn listObjects(self: *Client, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        if (opts.max_keys == 0) return error.InvalidPageSize;
        if (opts.max_keys > max_list_page_keys) return error.PageSizeTooLarge;
        if (opts.start_after != null and opts.continuation_token != null) return error.AmbiguousContinuation;
        if (opts.cancellation) |token| try token.check();
        var result = try self.vtable.list_objects(self.ptr, self.allocator, bucket, opts);
        errdefer result.deinit(self.allocator);
        if (opts.cancellation) |token| try token.check();
        return result;
    }

    pub fn listObjectVersions(self: *Client, bucket: []const u8, opts: types.ListObjectVersionsOptions) !types.ListObjectVersionsResult {
        if (opts.max_keys == 0) return error.InvalidPageSize;
        if (opts.max_keys > max_list_page_keys) return error.PageSizeTooLarge;
        if (opts.version_id_marker != null and opts.key_marker == null)
            return error.InvalidVersionPagination;
        const list_versions = self.vtable.list_object_versions orelse
            return error.ObjectVersionListingUnsupported;
        return try list_versions(self.ptr, self.allocator, bucket, opts);
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
