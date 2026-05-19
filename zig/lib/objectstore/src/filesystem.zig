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
const client_mod = @import("client.zig");
const types = @import("types.zig");

const multipart_part_size: usize = 5 * 1024 * 1024;

pub const FilesystemClient = struct {
    alloc: Allocator,
    root_dir: []u8,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FilesystemClient {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try std.Io.Dir.cwd().createDirPath(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FilesystemClient) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn client(self: *FilesystemClient) client_mod.Client {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn bucketExists(self: *FilesystemClient, bucket: []const u8) bool {
        const path = bucketRootAlloc(self.alloc, self.root_dir, bucket) catch return false;
        defer self.alloc.free(path);
        return fileExists(path);
    }

    fn makeBucket(self: *FilesystemClient, bucket: []const u8) !void {
        const objects_root = try objectRootAlloc(self.alloc, self.root_dir, bucket);
        defer self.alloc.free(objects_root);
        const metadata_root = try metadataRootAlloc(self.alloc, self.root_dir, bucket);
        defer self.alloc.free(metadata_root);

        var io_impl = threadedIo();
        defer io_impl.deinit();
        try std.Io.Dir.cwd().createDirPath(io_impl.io(), objects_root);
        try std.Io.Dir.cwd().createDirPath(io_impl.io(), metadata_root);
    }

    fn putObject(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        try self.makeBucket(bucket);

        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        const metadata_path = try metadataPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(metadata_path);

        if (fileExists(object_path)) {
            var current = try self.statObject(alloc, bucket, key);
            defer current.deinit(alloc);
            if (opts.if_none_match) return error.PreconditionFailed;
            if (opts.if_match_etag) |expected| {
                if (current.etag == null or !std.mem.eql(u8, current.etag.?, expected)) return error.PreconditionFailed;
            }
        } else if (opts.if_match_etag != null) {
            return error.PreconditionFailed;
        }

        try ensureParentDir(object_path);
        try writeFileAtomically(object_path, body);
        try ensureParentDir(metadata_path);
        try writeOptionalStringAtomically(metadata_path, opts.content_type orelse "");

        return .{
            .etag = try sha256HexAlloc(alloc, body),
        };
    }

    fn getObject(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        _ = opts.version_id;
        var meta = try self.statObject(alloc, bucket, key);
        errdefer meta.deinit(alloc);

        if (opts.if_match_etag) |expected| {
            if (meta.etag == null or !std.mem.eql(u8, meta.etag.?, expected)) return error.PreconditionFailed;
        }

        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        const raw = try readFileAlloc(alloc, object_path);
        errdefer alloc.free(raw);

        const part_range = if (opts.part_number) |part_number|
            try computePartRange(raw.len, part_number)
        else
            null;

        const body = if (opts.range) |range|
            try dupeRangeAlloc(alloc, raw, range.offset, range.length)
        else if (part_range) |range|
            try alloc.dupe(u8, raw[range.start..range.end])
        else
            try alloc.dupe(u8, raw);
        alloc.free(raw);

        meta.content_length = @intCast(body.len);
        return .{
            .body = body,
            .metadata = meta,
        };
    }

    fn getObjectAttributes(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        var meta = try self.statObject(alloc, bucket, key);
        defer meta.deinit(alloc);

        const part_count = partCount(meta.content_length);
        const parts = try alloc.alloc(types.ObjectPart, part_count);
        errdefer alloc.free(parts);

        var remaining = meta.content_length;
        for (parts, 0..) |*part, idx| {
            const size = @min(remaining, multipart_part_size);
            remaining -= size;
            part.* = .{
                .part_number = @intCast(idx + 1),
                .size = size,
                .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
            };
        }

        return .{
            .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
            .content_length = meta.content_length,
            .content_type = if (meta.content_type) |value| try alloc.dupe(u8, value) else null,
            .parts = parts,
        };
    }

    fn statObject(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        const metadata_path = try metadataPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(metadata_path);

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const file_stat = try std.Io.Dir.cwd().statFile(io_impl.io(), object_path, .{});
        const body = try readFileAlloc(alloc, object_path);
        defer alloc.free(body);
        const content_type = try readOptionalStringAlloc(alloc, metadata_path);

        return .{
            .bucket = try alloc.dupe(u8, bucket),
            .key = try alloc.dupe(u8, key),
            .etag = try sha256HexAlloc(alloc, body),
            .content_length = @intCast(file_stat.size),
            .content_type = content_type,
            .last_modified_unix_ms = file_stat.mtime.toMilliseconds(),
        };
    }

    fn deleteObject(self: *FilesystemClient, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const object_path = try objectPathAlloc(self.alloc, self.root_dir, bucket, key);
        defer self.alloc.free(object_path);
        const metadata_path = try metadataPathAlloc(self.alloc, self.root_dir, bucket, key);
        defer self.alloc.free(metadata_path);

        if (opts.if_match_etag) |expected| {
            var meta = try self.statObject(self.alloc, bucket, key);
            defer meta.deinit(self.alloc);
            if (meta.etag == null or !std.mem.eql(u8, meta.etag.?, expected)) return error.PreconditionFailed;
        }

        try deleteFile(object_path);
        deleteFile(metadata_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    fn listObjects(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const root = try objectRootAlloc(alloc, self.root_dir, bucket);
        defer alloc.free(root);
        if (!fileExists(root)) {
            return .{
                .entries = try alloc.alloc(types.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            };
        }

        var io_impl = threadedIo();
        defer io_impl.deinit();
        var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), root, .{ .iterate = true });
        defer dir.close(io_impl.io());

        var walker = try dir.walk(alloc);
        defer walker.deinit();

        var entries = std.ArrayListUnmanaged(types.ListEntry).empty;
        var prefixes = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
            for (prefixes.items) |prefix| alloc.free(prefix);
            prefixes.deinit(alloc);
        }

        const continuation = opts.continuation_token orelse opts.start_after;
        var count: u32 = 0;
        while (try walker.next(io_impl.io())) |entry| {
            if (entry.kind != .file) continue;
            const key = entry.path;
            if (!std.mem.startsWith(u8, key, opts.prefix)) continue;
            if (continuation) |token| {
                if (std.mem.order(u8, key, token) != .gt) continue;
            }

            if (!opts.recursive and opts.delimiter.len > 0 and key.len > opts.prefix.len) {
                if (std.mem.indexOf(u8, key[opts.prefix.len..], opts.delimiter)) |delimiter_offset| {
                    const prefix_end = opts.prefix.len + delimiter_offset + opts.delimiter.len;
                    const common_prefix = key[0..prefix_end];
                    if (!containsPrefix(prefixes.items, common_prefix)) {
                        if (count >= opts.max_keys) break;
                        try prefixes.append(alloc, try alloc.dupe(u8, common_prefix));
                        count += 1;
                    }
                    continue;
                }
            }

            if (count >= opts.max_keys) break;
            var meta = try self.statObject(alloc, bucket, key);
            defer meta.deinit(alloc);
            try entries.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
                .size = meta.content_length,
                .last_modified_unix_ms = meta.last_modified_unix_ms,
            });
            count += 1;
        }

        std.mem.sort(types.ListEntry, entries.items, {}, lessEntry);
        std.mem.sort([]u8, prefixes.items, {}, lessPrefix);
        return .{
            .entries = try entries.toOwnedSlice(alloc),
            .common_prefixes = try prefixes.toOwnedSlice(alloc),
        };
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = erasedBucketExists,
        .make_bucket = erasedMakeBucket,
        .put_object = erasedPutObject,
        .get_object = erasedGetObject,
        .get_object_attributes = erasedGetObjectAttributes,
        .stat_object = erasedStatObject,
        .delete_object = erasedDeleteObject,
        .list_objects = erasedListObjects,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedBucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return self.bucketExists(bucket);
    }

    fn erasedMakeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        try self.makeBucket(bucket);
    }

    fn erasedPutObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.putObject(alloc, bucket, key, body, opts);
    }

    fn erasedGetObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.getObject(alloc, bucket, key, opts);
    }

    fn erasedGetObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.getObjectAttributes(alloc, bucket, key);
    }

    fn erasedStatObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.statObject(alloc, bucket, key);
    }

    fn erasedDeleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        try self.deleteObject(bucket, key, opts);
    }

    fn erasedListObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.listObjects(alloc, bucket, opts);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn deleteFile(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-fs-objectstore-{d}", .{ path, uniqueNs() });
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

fn writeOptionalStringAtomically(path: []const u8, value: []const u8) !void {
    try writeFileAtomically(path, value);
}

fn readOptionalStringAlloc(alloc: Allocator, path: []const u8) !?[]u8 {
    const raw = readFileAlloc(alloc, path) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    if (raw.len == 0) {
        alloc.free(raw);
        return null;
    }
    return raw;
}

fn dupeRangeAlloc(alloc: Allocator, bytes: []const u8, offset: u64, maybe_len: ?u64) ![]u8 {
    if (offset > bytes.len) return error.InvalidRange;
    const start: usize = @intCast(offset);
    const end = if (maybe_len) |len|
        @min(bytes.len, start + @as(usize, @intCast(len)))
    else
        bytes.len;
    return try alloc.dupe(u8, bytes[start..end]);
}

fn computePartRange(total_len: usize, part_number: u32) !struct { start: usize, end: usize } {
    if (part_number == 0) return error.InvalidPartNumber;
    const start = (part_number - 1) * multipart_part_size;
    if (start >= total_len) return error.InvalidPartNumber;
    return .{
        .start = start,
        .end = @min(total_len, start + multipart_part_size),
    };
}

fn partCount(content_length: u64) usize {
    if (content_length == 0) return 1;
    return @intCast((content_length + multipart_part_size - 1) / multipart_part_size);
}

fn sha256HexAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const out = try alloc.alloc(u8, 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn bucketRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket });
}

fn objectRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket, "objects" });
}

fn metadataRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket, "metadata" });
}

fn objectPathAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8, key: []const u8) ![]u8 {
    const object_root = try objectRootAlloc(alloc, root_dir, bucket);
    defer alloc.free(object_root);
    return try std.fs.path.join(alloc, &.{ object_root, key });
}

fn metadataPathAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8, key: []const u8) ![]u8 {
    const metadata_root = try metadataRootAlloc(alloc, root_dir, bucket);
    defer alloc.free(metadata_root);
    const basename = try std.fmt.allocPrint(alloc, "{s}.content_type", .{key});
    defer alloc.free(basename);
    return try std.fs.path.join(alloc, &.{ metadata_root, basename });
}

fn lessEntry(_: void, lhs: types.ListEntry, rhs: types.ListEntry) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn lessPrefix(_: void, lhs: []u8, rhs: []u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

fn containsPrefix(prefixes: []const []u8, needle: []const u8) bool {
    for (prefixes) |prefix| {
        if (std.mem.eql(u8, prefix, needle)) return true;
    }
    return false;
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn uniqueNs() u64 {
    return nowNs();
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-objectstore-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "filesystem client supports bucket/object lifecycle and file helpers" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "lifecycle");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();

    try client.makeBucket("docs");
    try std.testing.expect(try client.bucketExists("docs"));

    var put = try client.putObject("docs", "nested/a.txt", "alpha", .{ .content_type = "text/plain" });
    defer put.deinit(alloc);

    var got = try client.getObject("docs", "nested/a.txt", .{});
    defer got.deinit(alloc);
    try std.testing.expectEqualStrings("alpha", got.body);
    try std.testing.expectEqualStrings("text/plain", got.metadata.content_type.?);

    var attrs = try client.getObjectAttributes("docs", "nested/a.txt");
    defer attrs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), attrs.parts.len);

    var listed = try client.listObjects("docs", .{ .prefix = "nested/" });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);

    const src_path = try std.fs.path.join(alloc, &.{ std.mem.span(path), "source.txt" });
    defer alloc.free(src_path);
    try writeFileAtomically(src_path, "beta");
    var file_put = try client.putFile("docs", "nested/b.txt", src_path, .{ .content_type = "text/plain" });
    defer file_put.deinit(alloc);

    const dst_path = try std.fs.path.join(alloc, &.{ std.mem.span(path), "download", "b.txt" });
    defer alloc.free(dst_path);
    try client.getFile("docs", "nested/b.txt", dst_path, .{});
    const downloaded = try readFileAlloc(alloc, dst_path);
    defer alloc.free(downloaded);
    try std.testing.expectEqualStrings("beta", downloaded);
}
