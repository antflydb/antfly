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

pub const Client = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        bucket_exists: *const fn (*anyopaque, []const u8) anyerror!bool,
        make_bucket: *const fn (*anyopaque, []const u8) anyerror!void,
        put_object: *const fn (*anyopaque, Allocator, []const u8, []const u8, []const u8, types.PutOptions) anyerror!types.PutResult,
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
        const body = try readFileAlloc(self.allocator, src_path);
        defer self.allocator.free(body);
        return try self.putObject(bucket, key, body, opts);
    }

    pub fn getObject(self: *Client, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        return try self.vtable.get_object(self.ptr, self.allocator, bucket, key, opts);
    }

    pub fn getFile(self: *Client, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        var object = try self.getObject(bucket, key, opts);
        defer object.deinit(self.allocator);
        try ensureParentDir(dest_path);
        try writeFileAtomically(dest_path, object.body);
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
        return try self.vtable.list_objects(self.ptr, self.allocator, bucket, opts);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-objectstore-{d}", .{ path, uniqueNs() });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.renameAbsolute(tmp_path, path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    } else {
        std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    }
}

fn uniqueNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}
