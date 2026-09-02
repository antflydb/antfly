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
const max_content_type_bytes: usize = 16 * 1024;
const object_magic = "AFOBJ001";
const object_header_len = object_magic.len + @sizeOf(u64) + @sizeOf(u32) + 64;
const stale_staging_age_ns: i96 = 24 * 60 * 60 * std.time.ns_per_s;
const ObjectRange = struct { start: usize, end: usize };

pub const FilesystemClient = struct {
    alloc: Allocator,
    root_dir: []u8,
    io: std.Io,
    io_impl: ?*std.Io.Threaded,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FilesystemClient {
        const io_impl = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(io_impl);
        io_impl.* = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        return try initWithIoOwned(alloc, root_dir, io_impl.io(), io_impl);
    }

    pub fn initWithIo(alloc: Allocator, root_dir: []const u8, io: std.Io) !FilesystemClient {
        return try initWithIoOwned(alloc, root_dir, io, null);
    }

    fn initWithIoOwned(alloc: Allocator, root_dir: []const u8, io: std.Io, io_impl: ?*std.Io.Threaded) !FilesystemClient {
        try std.Io.Dir.cwd().createDirPath(io, root_dir);
        try cleanupStaleStagingFiles(alloc, io, root_dir, stale_staging_age_ns);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
            .io = io,
            .io_impl = io_impl,
        };
    }

    pub fn deinit(self: *FilesystemClient) void {
        self.alloc.free(self.root_dir);
        if (self.io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
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
        return fileExists(self.io, path);
    }

    fn makeBucket(self: *FilesystemClient, bucket: []const u8) !void {
        const objects_root = try objectRootAlloc(self.alloc, self.root_dir, bucket);
        defer self.alloc.free(objects_root);
        const locks_root = try lockRootAlloc(self.alloc, self.root_dir, bucket);
        defer self.alloc.free(locks_root);
        const staging_root = try stagingRootAlloc(self.alloc, self.root_dir, bucket);
        defer self.alloc.free(staging_root);
        try std.Io.Dir.cwd().createDirPath(self.io, objects_root);
        try std.Io.Dir.cwd().createDirPath(self.io, locks_root);
        try std.Io.Dir.cwd().createDirPath(self.io, staging_root);
    }

    fn putObject(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        if (opts.cancellation) |token| try token.check();
        try self.makeBucket(bucket);

        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        var object_lock = try lockObject(self.io, alloc, self.root_dir, bucket, key);
        defer object_lock.deinit();
        if (fileExists(self.io, object_path)) {
            var current = try self.statObject(alloc, bucket, key);
            defer current.deinit(alloc);
            if (opts.if_none_match) return error.PreconditionFailed;
            if (opts.if_match_etag) |expected| {
                if (current.etag == null or !std.mem.eql(u8, current.etag.?, expected)) return error.PreconditionFailed;
            }
        } else if (opts.if_match_etag != null) {
            return error.PreconditionFailed;
        }

        try ensureParentDir(self.io, object_path);
        const etag = try sha256HexAlloc(alloc, body);
        errdefer alloc.free(etag);
        const staging_path = try stagingPathAlloc(alloc, self.root_dir, bucket);
        defer alloc.free(staging_path);
        try writeObjectAtomically(self.io, object_path, staging_path, body, etag, opts.content_type orelse "", opts.cancellation);

        return .{
            .etag = etag,
        };
    }

    fn putFile(self: *FilesystemClient, alloc: Allocator, source_io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        if (opts.cancellation) |token| try token.check();
        try self.makeBucket(bucket);
        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        var object_lock = try lockObject(self.io, alloc, self.root_dir, bucket, key);
        defer object_lock.deinit();
        if (fileExists(self.io, object_path)) {
            var current = try self.statObject(alloc, bucket, key);
            defer current.deinit(alloc);
            if (opts.if_none_match) return error.PreconditionFailed;
            if (opts.if_match_etag) |expected| {
                if (current.etag == null or !std.mem.eql(u8, current.etag.?, expected)) return error.PreconditionFailed;
            }
        } else if (opts.if_match_etag != null) {
            return error.PreconditionFailed;
        }

        try ensureParentDir(self.io, object_path);
        const staging_path = try stagingPathAlloc(alloc, self.root_dir, bucket);
        defer alloc.free(staging_path);
        const etag = try writeObjectFileAtomically(
            alloc,
            self.io,
            source_io,
            object_path,
            staging_path,
            src_path,
            opts.content_type orelse "",
            opts.cancellation,
        );
        return .{ .etag = etag };
    }

    fn getFile(self: *FilesystemClient, alloc: Allocator, destination_io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8) !void {
        return try self.getFileWithOptions(alloc, destination_io, bucket, key, dest_path, .{});
    }

    fn getFileWithOptions(
        self: *FilesystemClient,
        alloc: Allocator,
        destination_io: std.Io,
        bucket: []const u8,
        key: []const u8,
        dest_path: []const u8,
        opts: types.GetOptions,
    ) !void {
        if (opts.cancellation) |token| try token.check();
        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        const source = try openFilePath(self.io, object_path);
        defer source.close(self.io);
        const source_stat = try source.stat(self.io);
        var header = try readObjectHeader(alloc, self.io, source, source_stat.size);
        defer header.deinit(alloc);
        try streamVerifiedObjectToFile(
            alloc,
            self.io,
            destination_io,
            source,
            header,
            dest_path,
            opts.cancellation,
        );
    }

    fn getPrefix(self: *FilesystemClient, alloc: Allocator, destination_io: std.Io, bucket: []const u8, prefix: []const u8, dest_path: []const u8) !usize {
        try validatePrefix(prefix);
        const object_root = try objectRootAlloc(alloc, self.root_dir, bucket);
        defer alloc.free(object_root);
        const trimmed_prefix = if (prefix.len == 0) prefix else prefix[0 .. prefix.len - 1];
        const prefix_root = if (trimmed_prefix.len == 0)
            try alloc.dupe(u8, object_root)
        else
            try std.fs.path.join(alloc, &.{ object_root, trimmed_prefix });
        defer alloc.free(prefix_root);
        var dir = std.Io.Dir.cwd().openDir(self.io, prefix_root, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer dir.close(self.io);
        var walker = try dir.walk(alloc);
        defer walker.deinit();

        var count: usize = 0;
        while (try walker.next(self.io)) |entry| {
            if (entry.kind == .directory) continue;
            if (entry.kind != .file) return error.CorruptObjectNamespace;
            const key = if (prefix.len == 0)
                try alloc.dupe(u8, entry.path)
            else
                try std.fmt.allocPrint(alloc, "{s}{s}", .{ prefix, entry.path });
            defer alloc.free(key);
            const destination = try std.fs.path.join(alloc, &.{ dest_path, entry.path });
            defer alloc.free(destination);
            try self.getFile(alloc, destination_io, bucket, key, destination);
            count += 1;
        }
        return count;
    }

    fn getObject(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, key: []const u8, opts: types.GetOptions) !types.GetResult {
        if (opts.cancellation) |token| try token.check();
        if (opts.version_id != null) return error.VersioningUnsupported;
        if (opts.range != null and opts.part_number != null) return error.AmbiguousRange;
        var meta = try self.statObject(alloc, bucket, key);
        errdefer meta.deinit(alloc);

        if (opts.if_match_etag) |expected| {
            if (meta.etag == null or !std.mem.eql(u8, meta.etag.?, expected)) return error.PreconditionFailed;
        }

        const object_path = try objectPathAlloc(alloc, self.root_dir, bucket, key);
        defer alloc.free(object_path);
        const total_len = std.math.cast(usize, meta.content_length) orelse return error.ObjectTooLarge;
        const part_range = if (opts.part_number) |part_number|
            try computePartRange(total_len, part_number)
        else
            null;

        const requested: ObjectRange = if (opts.range) |range|
            try resolveRange(meta.content_length, range.offset, range.length)
        else if (part_range) |range|
            .{ .start = range.start, .end = range.end }
        else
            .{ .start = 0, .end = total_len };
        if (opts.max_response_bytes) |limit| {
            if (requested.end - requested.start > limit) return error.ResponseTooLarge;
        }
        const body = try readObjectRangeAlloc(
            self.io,
            alloc,
            object_path,
            requested.start,
            requested.end,
            meta.etag.?,
            opts.cancellation,
        );

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
        const file = try openFilePath(self.io, object_path);
        defer file.close(self.io);
        const file_stat = try file.stat(self.io);
        var header = try readObjectHeader(alloc, self.io, file, file_stat.size);
        defer header.deinit(alloc);

        return .{
            .bucket = try alloc.dupe(u8, bucket),
            .key = try alloc.dupe(u8, key),
            .etag = try alloc.dupe(u8, &header.etag),
            .checksum = .{
                .algorithm = .sha256_hex,
                .value = try alloc.dupe(u8, &header.etag),
            },
            .content_length = header.content_length,
            .content_type = if (header.content_type.len == 0) null else try alloc.dupe(u8, header.content_type),
            .last_modified_unix_ms = file_stat.mtime.toMilliseconds(),
        };
    }

    fn deleteObject(self: *FilesystemClient, bucket: []const u8, key: []const u8, opts: types.DeleteOptions) !void {
        if (opts.cancellation) |token| try token.check();
        if (opts.version_id != null) return error.VersioningUnsupported;
        const object_path = try objectPathAlloc(self.alloc, self.root_dir, bucket, key);
        defer self.alloc.free(object_path);
        var object_lock = try lockObject(self.io, self.alloc, self.root_dir, bucket, key);
        defer object_lock.deinit();
        if (opts.if_match_etag) |expected| {
            var meta = try self.statObject(self.alloc, bucket, key);
            defer meta.deinit(self.alloc);
            if (meta.etag == null or !std.mem.eql(u8, meta.etag.?, expected)) return error.PreconditionFailed;
        }

        if (opts.cancellation) |token| try token.check();
        try deleteFile(self.io, object_path);
    }

    fn listObjects(self: *FilesystemClient, alloc: Allocator, bucket: []const u8, opts: types.ListOptions) !types.ListResult {
        if (opts.cancellation) |token| try token.check();
        const root = try objectRootAlloc(alloc, self.root_dir, bucket);
        defer alloc.free(root);
        if (!fileExists(self.io, root)) {
            return .{
                .entries = try alloc.alloc(types.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            };
        }

        var dir = try std.Io.Dir.cwd().openDir(self.io, root, .{ .iterate = true });
        defer dir.close(self.io);

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

        const candidate_limit: usize = @as(usize, opts.max_keys) + 1;
        var candidates = std.PriorityQueue(ListCandidate, void, candidateMaxOrder).initContext({});
        defer {
            for (candidates.items) |candidate| alloc.free(candidate.name);
            candidates.deinit(alloc);
        }
        var retained = std.StringHashMapUnmanaged(void).empty;
        defer retained.deinit(alloc);
        const continuation = opts.continuation_token orelse opts.start_after;
        while (try walker.next(self.io)) |entry| {
            if (opts.cancellation) |token| try token.check();
            if (entry.kind != .file) continue;
            if (!std.mem.startsWith(u8, entry.path, opts.prefix)) continue;
            var candidate_name: []const u8 = entry.path;
            var is_prefix = false;
            if (!opts.recursive and opts.delimiter.len > 0 and entry.path.len > opts.prefix.len) {
                if (std.mem.indexOf(u8, entry.path[opts.prefix.len..], opts.delimiter)) |delimiter_offset| {
                    const prefix_end = opts.prefix.len + delimiter_offset + opts.delimiter.len;
                    candidate_name = entry.path[0..prefix_end];
                    is_prefix = true;
                }
            }
            if (continuation) |token| {
                if (std.mem.order(u8, candidate_name, token) != .gt) continue;
            }
            if (retained.contains(candidate_name)) continue;
            if (candidates.items.len >= candidate_limit) {
                const largest = candidates.peek().?;
                if (std.mem.order(u8, candidate_name, largest.name) != .lt) continue;
                const evicted = candidates.pop().?;
                _ = retained.remove(evicted.name);
                alloc.free(evicted.name);
            }
            const owned_name = try alloc.dupe(u8, candidate_name);
            retained.put(alloc, owned_name, {}) catch |err| {
                alloc.free(owned_name);
                return err;
            };
            candidates.push(alloc, .{ .name = owned_name, .is_prefix = is_prefix }) catch |err| {
                _ = retained.remove(owned_name);
                alloc.free(owned_name);
                return err;
            };
        }
        std.mem.sort(ListCandidate, candidates.items, {}, candidateLessThan);
        const result_count = @min(@as(usize, opts.max_keys), candidates.items.len);
        for (candidates.items[0..result_count]) |candidate| {
            if (candidate.is_prefix) {
                try prefixes.append(alloc, try alloc.dupe(u8, candidate.name));
                continue;
            }
            var meta = try self.statObject(alloc, bucket, candidate.name);
            defer meta.deinit(alloc);
            try entries.append(alloc, .{
                .key = try alloc.dupe(u8, candidate.name),
                .etag = if (meta.etag) |value| try alloc.dupe(u8, value) else null,
                .size = meta.content_length,
                .last_modified_unix_ms = meta.last_modified_unix_ms,
            });
        }

        return .{
            .entries = try entries.toOwnedSlice(alloc),
            .common_prefixes = try prefixes.toOwnedSlice(alloc),
            .next_continuation_token = if (candidates.items.len > result_count and result_count > 0)
                try alloc.dupe(u8, candidates.items[result_count - 1].name)
            else
                null,
        };
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = erasedBucketExists,
        .make_bucket = erasedMakeBucket,
        .put_object = erasedPutObject,
        .put_file = erasedPutFile,
        .get_file = erasedGetFile,
        .get_file_with_options = erasedGetFileWithOptions,
        .get_prefix = erasedGetPrefix,
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

    fn erasedBucketExists(ptr: *anyopaque, bucket: []const u8, opts: types.BucketOptions) !bool {
        if (opts.cancellation) |token| try token.check();
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return self.bucketExists(bucket);
    }

    fn erasedMakeBucket(ptr: *anyopaque, bucket: []const u8, opts: types.BucketOptions) !void {
        if (opts.cancellation) |token| try token.check();
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        try self.makeBucket(bucket);
    }

    fn erasedPutObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.putObject(alloc, bucket, key, body, opts);
    }

    fn erasedPutFile(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, key: []const u8, src_path: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.putFile(alloc, io, bucket, key, src_path, opts);
    }

    fn erasedGetFile(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8) !void {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.getFile(alloc, io, bucket, key, dest_path);
    }

    fn erasedGetFileWithOptions(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, key: []const u8, dest_path: []const u8, opts: types.GetOptions) !void {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.getFileWithOptions(alloc, io, bucket, key, dest_path, opts);
    }

    fn erasedGetPrefix(ptr: *anyopaque, alloc: Allocator, io: std.Io, bucket: []const u8, prefix: []const u8, dest_path: []const u8) !usize {
        const self: *FilesystemClient = @ptrCast(@alignCast(ptr));
        return try self.getPrefix(alloc, io, bucket, prefix, dest_path);
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

fn fileExists(io: std.Io, path: []const u8) bool {
    _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return true;
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn deleteFile(io: std.Io, path: []const u8) !void {
    try std.Io.Dir.cwd().deleteFile(io, path);
}

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    try std.Io.Dir.cwd().createDirPath(io, parent);
}

fn cleanupStaleStagingFiles(alloc: Allocator, io: std.Io, root_dir: []const u8, minimum_age_ns: i96) !void {
    const buckets_root = try std.fs.path.join(alloc, &.{ root_dir, "buckets" });
    defer alloc.free(buckets_root);
    var buckets_dir = std.Io.Dir.cwd().openDir(io, buckets_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer buckets_dir.close(io);
    const now = std.Io.Timestamp.now(io, .real);
    const cutoff_ns = now.toNanoseconds() - minimum_age_ns;

    var buckets = buckets_dir.iterate();
    while (try buckets.next(io)) |bucket| {
        if (bucket.kind != .directory) continue;
        const staging_path = try std.fs.path.join(alloc, &.{ buckets_root, bucket.name, "staging" });
        defer alloc.free(staging_path);
        var staging_dir = std.Io.Dir.cwd().openDir(io, staging_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer staging_dir.close(io);
        var staging = staging_dir.iterate();
        while (try staging.next(io)) |entry| {
            if (entry.kind != .file or
                !std.mem.startsWith(u8, entry.name, "upload-") or
                !std.mem.endsWith(u8, entry.name, ".tmp")) continue;
            const stat = staging_dir.statFile(io, entry.name, .{}) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            if (stat.mtime.toNanoseconds() > cutoff_ns) continue;
            var file = staging_dir.openFile(io, entry.name, .{ .mode = .read_write }) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            const locked = file.tryLock(io, .exclusive) catch |err| {
                file.close(io);
                return err;
            };
            if (!locked) {
                file.close(io);
                continue;
            }
            const locked_stat = file.stat(io) catch |err| {
                file.unlock(io);
                file.close(io);
                return err;
            };
            if (locked_stat.mtime.toNanoseconds() > cutoff_ns) {
                file.unlock(io);
                file.close(io);
                continue;
            }
            staging_dir.deleteFile(io, entry.name) catch |err| switch (err) {
                error.FileNotFound => {},
                else => {
                    file.unlock(io);
                    file.close(io);
                    return err;
                },
            };
            file.unlock(io);
            file.close(io);
        }
    }
}

const ObjectLock = struct {
    io: std.Io,
    file: std.Io.File,

    fn deinit(self: *ObjectLock) void {
        self.file.unlock(self.io);
        self.file.close(self.io);
        self.* = undefined;
    }
};

fn lockObject(io: std.Io, alloc: Allocator, root_dir: []const u8, bucket: []const u8, key: []const u8) !ObjectLock {
    const path = try lockPathAlloc(alloc, root_dir, bucket, key);
    defer alloc.free(path);
    try ensureParentDir(io, path);

    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().createFile(io, path, .{});
    errdefer file.close(io);
    // Conditional object mutations require an inter-process lock. Propagate
    // FileLocksUnsupported rather than silently weakening compare-and-swap.
    try file.lock(io, .exclusive);
    return .{ .io = io, .file = file };
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

const ObjectHeader = struct {
    content_length: u64,
    content_type: []u8,
    etag: [64]u8,
    data_offset: u64,

    fn deinit(self: *ObjectHeader, alloc: Allocator) void {
        alloc.free(self.content_type);
        self.* = undefined;
    }
};

fn openFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
}

fn writeObjectAtomically(
    io: std.Io,
    path: []const u8,
    tmp_path: []const u8,
    body: []const u8,
    etag: []const u8,
    content_type: []const u8,
    cancellation: ?types.CancellationToken,
) !void {
    if (cancellation) |token| try token.check();
    if (etag.len != 64 or content_type.len > max_content_type_bytes) return error.InvalidObjectMetadata;

    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        var file = if (std.fs.path.isAbsolute(tmp_path))
            try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
        else
            try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        try file.lock(io, .exclusive);
        defer file.unlock(io);

        var fixed: [object_header_len]u8 = undefined;
        encodeObjectHeader(&fixed, @intCast(body.len), content_type.len, etag);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(&fixed);
        try writer.interface.writeAll(content_type);
        const cancellation_chunk_bytes = 1024 * 1024;
        var offset: usize = 0;
        while (offset < body.len) {
            if (cancellation) |token| try token.check();
            const end = @min(body.len, offset + cancellation_chunk_bytes);
            try writer.interface.writeAll(body[offset..end]);
            offset = end;
        }
        try writer.end();
        if (cancellation) |token| try token.check();
        try file.sync(io);
    }

    if (cancellation) |token| try token.check();
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
}

fn writeObjectFileAtomically(
    alloc: Allocator,
    io: std.Io,
    source_io: std.Io,
    path: []const u8,
    tmp_path: []const u8,
    src_path: []const u8,
    content_type: []const u8,
    cancellation: ?types.CancellationToken,
) ![]u8 {
    if (cancellation) |token| try token.check();
    if (content_type.len > max_content_type_bytes) return error.InvalidObjectMetadata;
    const source = try openFilePath(source_io, src_path);
    defer source.close(source_io);
    const source_stat = try source.stat(source_io);

    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    var output = if (std.fs.path.isAbsolute(tmp_path))
        try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
    var output_open = true;
    defer if (output_open) output.close(io);
    try output.lock(io, .exclusive);
    var output_locked = true;
    defer if (output_locked) output.unlock(io);

    var placeholder: [object_header_len]u8 = @splat(0);
    var writer_buf: [64 * 1024]u8 = undefined;
    var writer = output.writer(io, &writer_buf);
    try writer.interface.writeAll(&placeholder);
    try writer.interface.writeAll(content_type);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var read_buf: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < source_stat.size) {
        if (cancellation) |token| try token.check();
        const wanted: usize = @intCast(@min(source_stat.size - offset, read_buf.len));
        const n = try source.readPositionalAll(source_io, read_buf[0..wanted], offset);
        if (n != wanted) return error.SourceFileChanged;
        hasher.update(read_buf[0..n]);
        try writer.interface.writeAll(read_buf[0..n]);
        offset += n;
    }
    var extra: [1]u8 = undefined;
    if (try source.readPositionalAll(source_io, &extra, offset) != 0) return error.SourceFileChanged;
    const final_source_stat = try source.stat(source_io);
    if (final_source_stat.size != source_stat.size or !std.meta.eql(final_source_stat.mtime, source_stat.mtime))
        return error.SourceFileChanged;
    try writer.end();
    if (cancellation) |token| try token.check();

    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    const etag = try digestHexAlloc(alloc, &digest);
    errdefer alloc.free(etag);
    encodeObjectHeader(&placeholder, source_stat.size, content_type.len, etag);
    try output.writePositionalAll(io, &placeholder, 0);
    try output.sync(io);
    output.unlock(io);
    output_locked = false;
    output.close(io);
    output_open = false;

    if (cancellation) |token| try token.check();
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    return etag;
}

fn encodeObjectHeader(out: *[object_header_len]u8, content_length: u64, content_type_len: usize, etag: []const u8) void {
    std.debug.assert(etag.len == 64);
    @memcpy(out[0..object_magic.len], object_magic);
    std.mem.writeInt(u64, out[object_magic.len..][0..8], content_length, .little);
    std.mem.writeInt(u32, out[object_magic.len + 8 ..][0..4], @intCast(content_type_len), .little);
    @memcpy(out[object_magic.len + 12 ..], etag);
}

fn readObjectHeader(alloc: Allocator, io: std.Io, file: std.Io.File, file_size: u64) !ObjectHeader {
    if (file_size < object_header_len) return error.CorruptObject;
    var fixed: [object_header_len]u8 = undefined;
    if (try file.readPositionalAll(io, &fixed, 0) != fixed.len) return error.CorruptObject;
    if (!std.mem.eql(u8, fixed[0..object_magic.len], object_magic)) return error.UnsupportedObjectFormat;

    const content_length = std.mem.readInt(u64, fixed[object_magic.len..][0..8], .little);
    const content_type_len = std.mem.readInt(u32, fixed[object_magic.len + 8 ..][0..4], .little);
    if (content_type_len > max_content_type_bytes) return error.CorruptObject;
    const data_offset = std.math.add(u64, object_header_len, content_type_len) catch return error.CorruptObject;
    const expected_size = std.math.add(u64, data_offset, content_length) catch return error.CorruptObject;
    if (expected_size != file_size) return error.CorruptObject;

    const content_type = try alloc.alloc(u8, content_type_len);
    errdefer alloc.free(content_type);
    if (content_type.len > 0 and try file.readPositionalAll(io, content_type, object_header_len) != content_type.len)
        return error.CorruptObject;
    var etag: [64]u8 = undefined;
    @memcpy(&etag, fixed[object_magic.len + 12 ..]);
    return .{
        .content_length = content_length,
        .content_type = content_type,
        .etag = etag,
        .data_offset = data_offset,
    };
}

fn resolveRange(total_len: u64, offset: u64, maybe_len: ?u64) !ObjectRange {
    if (offset > total_len or total_len > std.math.maxInt(usize)) return error.InvalidRange;
    const end_u64 = if (maybe_len) |len|
        @min(total_len, std.math.add(u64, offset, len) catch total_len)
    else
        total_len;
    return .{ .start = @intCast(offset), .end = @intCast(end_u64) };
}

fn readObjectRangeAlloc(
    io: std.Io,
    alloc: Allocator,
    path: []const u8,
    start: usize,
    end: usize,
    expected_etag: []const u8,
    cancellation: ?types.CancellationToken,
) ![]u8 {
    if (cancellation) |token| try token.check();
    if (end < start) return error.InvalidRange;
    const file = try openFilePath(io, path);
    defer file.close(io);
    const stat = try file.stat(io);
    var header = try readObjectHeader(alloc, io, file, stat.size);
    defer header.deinit(alloc);
    if (!std.mem.eql(u8, &header.etag, expected_etag)) return error.PreconditionFailed;
    if (end > header.content_length) return error.InvalidRange;

    const body = try alloc.alloc(u8, end - start);
    errdefer alloc.free(body);
    const file_offset = std.math.add(u64, header.data_offset, start) catch return error.InvalidRange;
    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < body.len) {
        if (cancellation) |token| try token.check();
        const chunk_len = @min(cancellation_chunk_bytes, body.len - copied);
        const chunk = body[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, file_offset + copied) != chunk.len) return error.CorruptObject;
        copied += chunk.len;
    }
    if (cancellation) |token| try token.check();
    return body;
}

fn streamVerifiedObjectToFile(
    alloc: Allocator,
    source_io: std.Io,
    destination_io: std.Io,
    source: std.Io.File,
    header: ObjectHeader,
    dest_path: []const u8,
    cancellation: ?types.CancellationToken,
) !void {
    if (cancellation) |token| try token.check();
    try ensureParentDir(destination_io, dest_path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-objectstore-download-{d}", .{ dest_path, uniqueNs() });
    defer alloc.free(tmp_path);
    errdefer deleteFilePath(destination_io, tmp_path) catch {};

    var output = try createFilePath(destination_io, tmp_path);
    var output_open = true;
    defer if (output_open) output.close(destination_io);
    var writer_buf: [64 * 1024]u8 = undefined;
    var writer = output.writer(destination_io, &writer_buf);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var read_buf: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < header.content_length) {
        if (cancellation) |token| try token.check();
        const wanted: usize = @intCast(@min(header.content_length - offset, read_buf.len));
        const file_offset = std.math.add(u64, header.data_offset, offset) catch return error.CorruptObject;
        const n = try source.readPositionalAll(source_io, read_buf[0..wanted], file_offset);
        if (n != wanted) return error.CorruptObject;
        hasher.update(read_buf[0..n]);
        try writer.interface.writeAll(read_buf[0..n]);
        offset += n;
    }
    try writer.end();
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    var actual_etag: [64]u8 = undefined;
    digestHex(&actual_etag, &digest);
    if (!std.mem.eql(u8, &actual_etag, &header.etag)) return error.ChecksumMismatch;
    if (cancellation) |token| try token.check();
    try output.sync(destination_io);
    if (cancellation) |token| try token.check();
    output.close(destination_io);
    output_open = false;
    try renameFilePath(destination_io, tmp_path, dest_path);
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
    if (content_length == 0) return 0;
    return @intCast((content_length + multipart_part_size - 1) / multipart_part_size);
}

fn sha256HexAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    return try digestHexAlloc(alloc, &digest);
}

fn digestHexAlloc(alloc: Allocator, digest: *const [32]u8) ![]u8 {
    const out = try alloc.alloc(u8, 64);
    digestHex(out, digest);
    return out;
}

fn digestHex(out: []u8, digest: *const [32]u8) void {
    std.debug.assert(out.len == 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

fn bucketRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    try validateBucket(bucket);
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket });
}

fn objectRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    try validateBucket(bucket);
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket, "objects" });
}

fn lockRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    try validateBucket(bucket);
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket, "locks" });
}

fn stagingRootAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    try validateBucket(bucket);
    return try std.fs.path.join(alloc, &.{ root_dir, "buckets", bucket, "staging" });
}

fn stagingPathAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8) ![]u8 {
    const staging_root = try stagingRootAlloc(alloc, root_dir, bucket);
    defer alloc.free(staging_root);
    const basename = try std.fmt.allocPrint(alloc, "upload-{d}.tmp", .{uniqueNs()});
    defer alloc.free(basename);
    return try std.fs.path.join(alloc, &.{ staging_root, basename });
}

fn lockPathAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8, key: []const u8) ![]u8 {
    const lock_root = try lockRootAlloc(alloc, root_dir, bucket);
    defer alloc.free(lock_root);
    // A bounded stripe set avoids retaining one lock inode for every object.
    // Hash collisions only reduce write concurrency; they do not weaken CAS.
    const stripe = std.hash.Wyhash.hash(0, key) & 0xfff;
    const basename = try std.fmt.allocPrint(alloc, "stripe-{x}.lock", .{stripe});
    defer alloc.free(basename);
    return try std.fs.path.join(alloc, &.{ lock_root, basename });
}

fn objectPathAlloc(alloc: Allocator, root_dir: []const u8, bucket: []const u8, key: []const u8) ![]u8 {
    try validateKey(key);
    const object_root = try objectRootAlloc(alloc, root_dir, bucket);
    defer alloc.free(object_root);
    return try std.fs.path.join(alloc, &.{ object_root, key });
}

fn validateBucket(bucket: []const u8) !void {
    if (bucket.len == 0 or std.mem.eql(u8, bucket, ".") or std.mem.eql(u8, bucket, ".."))
        return error.InvalidBucket;
    if (std.mem.indexOfAny(u8, bucket, "/\\\x00") != null) return error.InvalidBucket;
}

fn validateKey(key: []const u8) !void {
    if (key.len == 0 or std.fs.path.isAbsolute(key) or std.mem.indexOfScalar(u8, key, 0) != null)
        return error.InvalidObjectKey;
    var segments = std.mem.splitScalar(u8, key, '/');
    while (segments.next()) |segment| {
        if (segment.len == 0 or std.mem.eql(u8, segment, ".") or std.mem.eql(u8, segment, "..") or
            std.mem.indexOfScalar(u8, segment, '\\') != null)
            return error.InvalidObjectKey;
    }
}

fn validatePrefix(prefix: []const u8) !void {
    if (prefix.len == 0) return;
    if (!std.mem.endsWith(u8, prefix, "/")) return error.InvalidObjectPrefix;
    const trimmed = prefix[0 .. prefix.len - 1];
    if (trimmed.len == 0) return error.InvalidObjectPrefix;
    try validateKey(trimmed);
}

const ListCandidate = struct {
    name: []u8,
    is_prefix: bool,
};

fn candidateMaxOrder(_: void, lhs: ListCandidate, rhs: ListCandidate) std.math.Order {
    return std.mem.order(u8, rhs.name, lhs.name);
}

fn candidateLessThan(_: void, lhs: ListCandidate, rhs: ListCandidate) bool {
    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
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
    try std.testing.expectEqual(types.ObjectChecksumAlgorithm.sha256_hex, got.metadata.checksum.?.algorithm);
    try std.testing.expectEqualStrings(put.etag.?, got.metadata.checksum.?.value);

    var ranged = try client.getObject("docs", "nested/a.txt", .{ .range = .{ .offset = 1, .length = 3 } });
    defer ranged.deinit(alloc);
    try std.testing.expectEqualStrings("lph", ranged.body);
    try std.testing.expectEqual(@as(u64, 3), ranged.metadata.content_length);

    try std.testing.expectError(error.PreconditionFailed, client.putObject(
        "docs",
        "nested/a.txt",
        "replacement",
        .{ .if_match_etag = "stale" },
    ));

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

test "filesystem whole-file download honors cancellation before publication" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "download-cancellation");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();
    var put = try client.putObject("bucket", "backup/segment", "payload", .{});
    put.deinit(alloc);

    const State = struct {
        checks: usize = 0,

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 5;
        }
    };
    var state = State{};
    const destination = try std.fs.path.join(alloc, &.{ std.mem.span(path), "restore", "segment" });
    defer alloc.free(destination);
    try std.testing.expectError(error.Canceled, client.getFile("bucket", "backup/segment", destination, .{
        .cancellation = types.CancellationToken.fromCallback(&state, State.isCancelled),
    }));
    try std.testing.expect(state.checks >= 5);
    try std.testing.expect(!fileExists(fs.io, destination));
}

test "filesystem upload cancellation prevents atomic publication" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "upload-cancellation");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();

    const State = struct {
        checks: usize = 0,

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 4;
        }
    };
    var state = State{};
    try std.testing.expectError(error.Canceled, client.putObject(
        "bucket",
        "backup/segment",
        "payload",
        .{ .cancellation = types.CancellationToken.fromCallback(&state, State.isCancelled) },
    ));
    try std.testing.expect(state.checks >= 4);
    const object_path = try objectPathAlloc(alloc, fs.root_dir, "bucket", "backup/segment");
    defer alloc.free(object_path);
    try std.testing.expect(!fileExists(fs.io, object_path));
}

test "filesystem client rejects paths that escape its root" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "path-safety");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();

    try std.testing.expectError(error.InvalidBucket, client.makeBucket("../outside"));
    try std.testing.expectError(error.InvalidObjectKey, client.putObject("docs", "../outside", "no", .{}));
    try std.testing.expectError(error.InvalidObjectKey, client.putObject("docs", "nested//outside", "no", .{}));
    try std.testing.expectError(error.InvalidObjectKey, client.putObject("docs", "/outside", "no", .{}));
    try std.testing.expectError(error.InvalidPageSize, client.listObjects("docs", .{ .max_keys = 0 }));
    try std.testing.expectError(error.AmbiguousContinuation, client.listObjects("docs", .{
        .start_after = "a",
        .continuation_token = "b",
    }));
}

test "filesystem pagination retains only the requested ordered window" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "bounded-pagination");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();
    const keys = [_][]const u8{ "logs/c/2", "logs/a/2", "logs/b/1", "logs/a/1", "logs/c/1" };
    for (keys) |key| {
        var result = try client.putObject("bucket", key, key, .{});
        result.deinit(alloc);
    }

    var first = try client.listObjects("bucket", .{ .prefix = "logs/", .max_keys = 2 });
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), first.entries.len);
    try std.testing.expectEqualStrings("logs/a/1", first.entries[0].key);
    try std.testing.expectEqualStrings("logs/a/2", first.entries[1].key);
    try std.testing.expectEqualStrings("logs/a/2", first.next_continuation_token.?);

    var second = try client.listObjects("bucket", .{
        .prefix = "logs/",
        .max_keys = 2,
        .continuation_token = first.next_continuation_token,
    });
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), second.entries.len);
    try std.testing.expectEqualStrings("logs/b/1", second.entries[0].key);
    try std.testing.expectEqualStrings("logs/c/1", second.entries[1].key);

    var collapsed = try client.listObjects("bucket", .{
        .prefix = "logs/",
        .recursive = false,
        .max_keys = 2,
    });
    defer collapsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), collapsed.common_prefixes.len);
    try std.testing.expectEqualStrings("logs/a/", collapsed.common_prefixes[0]);
    try std.testing.expectEqualStrings("logs/b/", collapsed.common_prefixes[1]);
    try std.testing.expectEqualStrings("logs/b/", collapsed.next_continuation_token.?);
}

test "filesystem whole-file download verifies payload checksum before publication" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "download-integrity");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();
    var put = try client.putObject("bucket", "backup/segment", "integrity", .{});
    put.deinit(alloc);

    const object_path = try objectPathAlloc(alloc, fs.root_dir, "bucket", "backup/segment");
    defer alloc.free(object_path);
    var raw = try std.Io.Dir.openFileAbsolute(fs.io, object_path, .{ .mode = .read_write });
    try raw.writePositionalAll(fs.io, "X", object_header_len);
    raw.close(fs.io);

    const destination = try std.fs.path.join(alloc, &.{ std.mem.span(path), "restore", "segment" });
    defer alloc.free(destination);
    try std.testing.expectError(error.ChecksumMismatch, client.getFile("bucket", "backup/segment", destination, .{}));
    try std.testing.expect(!fileExists(fs.io, destination));
}

test "filesystem native prefix download walks once and confines descendants" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "prefix-download");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    var client = fs.client();
    defer client.deinit();
    const objects = [_][2][]const u8{
        .{ "backup/a", "alpha" },
        .{ "backup/nested/b", "beta" },
        .{ "backup-sibling/c", "wrong" },
    };
    for (objects) |object| {
        var put = try client.putObject("bucket", object[0], object[1], .{});
        put.deinit(alloc);
    }

    const destination = try std.fs.path.join(alloc, &.{ std.mem.span(path), "restore" });
    defer alloc.free(destination);
    const count = (try client.getPrefixWithIo(fs.io, "bucket", "backup/", destination)).?;
    try std.testing.expectEqual(@as(usize, 2), count);
    const alpha_path = try std.fs.path.join(alloc, &.{ destination, "a" });
    defer alloc.free(alpha_path);
    const alpha = try readFileAlloc(alloc, alpha_path);
    defer alloc.free(alpha);
    try std.testing.expectEqualStrings("alpha", alpha);
    const beta_path = try std.fs.path.join(alloc, &.{ destination, "nested", "b" });
    defer alloc.free(beta_path);
    const beta = try readFileAlloc(alloc, beta_path);
    defer alloc.free(beta);
    try std.testing.expectEqualStrings("beta", beta);
    const sibling_path = try std.fs.path.join(alloc, &.{ destination, "c" });
    defer alloc.free(sibling_path);
    try std.testing.expect(!fileExists(fs.io, sibling_path));
}

test "filesystem staging cleanup removes abandoned files and preserves locked uploads" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "staging-cleanup");
    defer cleanupTmp(path);

    var fs = try FilesystemClient.init(alloc, std.mem.span(path));
    defer fs.deinit();
    try fs.makeBucket("bucket");
    const staging_path = try stagingPathAlloc(alloc, fs.root_dir, "bucket");
    defer alloc.free(staging_path);
    const object_lookalike = try objectPathAlloc(alloc, fs.root_dir, "bucket", "staging/upload-user-object.tmp");
    defer alloc.free(object_lookalike);
    try ensureParentDir(fs.io, object_lookalike);
    var object_file = try std.Io.Dir.createFileAbsolute(fs.io, object_lookalike, .{ .truncate = true });
    object_file.close(fs.io);
    var staged = try std.Io.Dir.createFileAbsolute(fs.io, staging_path, .{ .truncate = true });
    try staged.writePositionalAll(fs.io, "partial", 0);
    try staged.sync(fs.io);
    try staged.lock(fs.io, .exclusive);

    try cleanupStaleStagingFiles(alloc, fs.io, fs.root_dir, -std.time.ns_per_s);
    try std.testing.expect(fileExists(fs.io, staging_path));
    staged.unlock(fs.io);
    staged.close(fs.io);

    try cleanupStaleStagingFiles(alloc, fs.io, fs.root_dir, -std.time.ns_per_s);
    try std.testing.expect(!fileExists(fs.io, staging_path));
    try std.testing.expect(fileExists(fs.io, object_lookalike));
}
