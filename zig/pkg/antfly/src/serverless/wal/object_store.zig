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

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const objectstore = @import("objectstore");
const wal_types = @import("types.zig");
const wal_store = @import("store.zig");
const remote_uri = @import("../remote_uri.zig");
const object_store_support = @import("../object_store_support.zig");

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    client: objectstore.Client,
    fs_client: ?*objectstore.FilesystemClient = null,
    gcs_client: ?*objectstore.Gcs.JsonApiClient = null,
    s3_client: ?*objectstore.S3.Client = null,
    owns_client: bool = true,
    bucket: []u8,
    prefix: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return try initRemoteUriWithS3Options(alloc, uri, null);
    }

    pub fn initRemoteUriWithS3Options(
        alloc: std.mem.Allocator,
        uri: []const u8,
        s3_options: ?object_store_support.S3Options,
    ) !ObjectStore {
        var parsed = try remote_uri.parseAlloc(alloc, uri);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        return switch (parsed) {
            .file => |path| blk: {
                const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
                defer alloc.free(file_uri);
                break :blk try initFileUri(alloc, file_uri);
            },
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix),
            .s3 => |value| try initS3UriWithOptions(alloc, value.bucket, value.prefix, s3_options),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        const path = try remote_uri.filePathFromUriAlloc(alloc, uri);
        defer alloc.free(path);
        const fs = try alloc.create(objectstore.FilesystemClient);
        errdefer alloc.destroy(fs);
        fs.* = try objectstore.FilesystemClient.init(alloc, path);

        var owned_client = fs.client();
        if (!(try owned_client.bucketExists("serverless-wal"))) try owned_client.makeBucket("serverless-wal");
        return .{
            .alloc = alloc,
            .client = owned_client,
            .fs_client = fs,
            .bucket = try alloc.dupe(u8, "serverless-wal"),
            .prefix = try alloc.dupe(u8, ""),
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        const gcs = try alloc.create(objectstore.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        const cfg = try objectstore.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        gcs.* = try objectstore.Gcs.JsonApiClient.init(alloc, cfg);

        var owned_client = gcs.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .gcs_client = gcs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return try initS3UriWithOptions(alloc, bucket, prefix, null);
    }

    pub fn initS3UriWithOptions(
        alloc: std.mem.Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: ?object_store_support.S3Options,
    ) !ObjectStore {
        const s3 = try alloc.create(objectstore.S3.Client);
        errdefer alloc.destroy(s3);
        const cfg = try object_store_support.s3ConfigAlloc(alloc, options);
        s3.* = try objectstore.S3.Client.init(alloc, cfg);

        var owned_client = s3.client();
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .s3_client = s3,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: objectstore.Client, bucket: []const u8, prefix: []const u8) !ObjectStore {
        var owned_client = client;
        if (!(try owned_client.bucketExists(bucket))) try owned_client.makeBucket(bucket);
        return .{
            .alloc = alloc,
            .client = owned_client,
            .owns_client = false,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.fs_client) |fs| self.alloc.destroy(fs);
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    pub fn walStore(self: *ObjectStore) wal_store.WalStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn append(self: *ObjectStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);

        var current = try self.tryReadLog(self.alloc, key);
        defer if (current) |*value| {
            self.alloc.free(value.body);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        const next_lsn: u64 = if (current) |value| (try lastLsn(value.body)) + 1 else 1;
        const encoded = try encodeRecordAlloc(self.alloc, next_lsn, timestamp_ns, payload);
        defer self.alloc.free(encoded);

        const combined = if (current) |value|
            try std.mem.concat(self.alloc, u8, &.{ value.body, encoded })
        else
            try self.alloc.dupe(u8, encoded);
        defer self.alloc.free(combined);

        var result = try self.client.putObject(self.bucket, key, combined, .{
            .content_type = "application/octet-stream",
            .if_none_match = current == null,
            .if_match_etag = if (current) |value| value.etag else null,
        });
        defer result.deinit(self.alloc);
        return next_lsn;
    }

    pub fn appendIdempotent(
        self: *ObjectStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
    ) !u64 {
        if (operation_id.len == 0 or operation_id.len > std.math.maxInt(u16))
            return error.InvalidWalOperationId;
        if (payload.len > idempotent_payload_max) return error.WalRecordTooLarge;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var current = try self.tryReadLog(self.alloc, key);
        defer if (current) |*value| {
            self.alloc.free(value.body);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        if (current) |value| {
            if (try findOperation(value.body, operation_id)) |existing| {
                if (existing.timestamp_ns != timestamp_ns or !std.mem.eql(u8, existing.payload, payload))
                    return error.WalIdempotencyConflict;
                return existing.lsn;
            }
        }

        const next_lsn: u64 = if (current) |value| (try lastLsn(value.body)) + 1 else 1;
        const encoded = try encodeIdempotentRecordAlloc(
            self.alloc,
            next_lsn,
            timestamp_ns,
            payload,
            operation_id,
        );
        defer self.alloc.free(encoded);
        const combined = if (current) |value|
            try std.mem.concat(self.alloc, u8, &.{ value.body, encoded })
        else
            try self.alloc.dupe(u8, encoded);
        defer self.alloc.free(combined);

        var result = try self.client.putObject(self.bucket, key, combined, .{
            .content_type = "application/octet-stream",
            .if_none_match = current == null,
            .if_match_etag = if (current) |value| value.etag else null,
        });
        defer result.deinit(self.alloc);
        return next_lsn;
    }

    pub fn readFromAlloc(self: *ObjectStore, alloc: std.mem.Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const key = try logKeyAlloc(alloc, self.prefix, namespace);
        defer alloc.free(key);

        var result = self.client.getObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return try alloc.alloc(wal_types.Record, 0),
            else => return err,
        };
        defer result.deinit(alloc);
        return try decodeRecordsFromAlloc(alloc, result.body, start_lsn);
    }

    pub fn latestLsn(self: *ObjectStore, namespace: []const u8) !u64 {
        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer result.deinit(self.alloc);
        return try lastLsn(result.body);
    }

    pub fn truncatePrefix(self: *ObjectStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);

        var current = try self.tryReadLog(self.alloc, key);
        defer if (current) |*value| {
            self.alloc.free(value.body);
            if (value.etag) |etag| self.alloc.free(etag);
        };
        if (current == null) return 0;

        const records = try decodeRecordsFromAlloc(self.alloc, current.?.body, 1);
        defer wal_types.freeRecords(self.alloc, records);

        var kept = std.ArrayListUnmanaged(wal_types.Record).empty;
        defer {
            for (kept.items) |record| self.alloc.free(record.payload);
            kept.deinit(self.alloc);
        }

        var removed: u64 = 0;
        for (records) |record| {
            if (record.lsn < keep_from_lsn) {
                removed += 1;
                continue;
            }
            try kept.append(self.alloc, .{
                .lsn = record.lsn,
                .timestamp_ns = record.timestamp_ns,
                .payload = try self.alloc.dupe(u8, record.payload),
                .operation_id = if (record.operation_id) |operation_id|
                    try self.alloc.dupe(u8, operation_id)
                else
                    null,
            });
        }

        if (kept.items.len == 0) {
            try self.client.deleteObject(self.bucket, key, .{
                .if_match_etag = current.?.etag,
            });
            return removed;
        }

        const encoded = try encodeRecordsAlloc(self.alloc, kept.items);
        defer self.alloc.free(encoded);
        var result = try self.client.putObject(self.bucket, key, encoded, .{
            .content_type = "application/octet-stream",
            .if_match_etag = current.?.etag,
        });
        defer result.deinit(self.alloc);
        return removed;
    }

    const CurrentLog = struct {
        body: []u8,
        etag: ?[]u8,
    };

    fn tryReadLog(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !?CurrentLog {
        var result = self.client.getObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        return .{
            .body = try alloc.dupe(u8, result.body),
            .etag = if (result.metadata.etag) |value| try alloc.dupe(u8, value) else null,
        };
    }

    const vtable: wal_store.WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
        .append_idempotent = erasedAppendIdempotent,
        .read_from_alloc = erasedReadFromAlloc,
        .latest_lsn = erasedLatestLsn,
        .truncate_prefix = erasedTruncatePrefix,
    };

    fn erasedDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedAppend(ptr: *anyopaque, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.append(namespace, timestamp_ns, payload);
    }

    fn erasedAppendIdempotent(
        ptr: *anyopaque,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
    ) !u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.appendIdempotent(namespace, timestamp_ns, payload, operation_id);
    }

    fn erasedReadFromAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.readFromAlloc(alloc, namespace, start_lsn);
    }

    fn erasedLatestLsn(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.latestLsn(namespace);
    }

    fn erasedTruncatePrefix(ptr: *anyopaque, namespace: []const u8, keep_from_lsn: u64) !u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.truncatePrefix(namespace, keep_from_lsn);
    }
};

fn encodeRecordAlloc(alloc: std.mem.Allocator, lsn: u64, timestamp_ns: u64, payload: []const u8) ![]u8 {
    // The high length bit identifies records carrying an operation ID. Keep
    // newly written legacy records below that boundary so both encodings are
    // unambiguous to the shared parser.
    if (payload.len > idempotent_payload_max) return error.WalRecordTooLarge;
    const buf = try alloc.alloc(u8, 8 + 8 + 4 + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u64, buf[pos..][0..8], lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], timestamp_ns, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(payload.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..payload.len], payload);
    return buf;
}

const idempotent_record_flag: u32 = @as(u32, 1) << 31;
const idempotent_payload_max: usize = idempotent_record_flag - 1;

fn encodeIdempotentRecordAlloc(
    alloc: std.mem.Allocator,
    lsn: u64,
    timestamp_ns: u64,
    payload: []const u8,
    operation_id: []const u8,
) ![]u8 {
    if (payload.len > idempotent_payload_max) return error.WalRecordTooLarge;
    if (operation_id.len == 0 or operation_id.len > std.math.maxInt(u16))
        return error.InvalidWalOperationId;
    const buf = try alloc.alloc(u8, 8 + 8 + 4 + 2 + operation_id.len + payload.len);
    var pos: usize = 0;
    std.mem.writeInt(u64, buf[pos..][0..8], lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], timestamp_ns, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], idempotent_record_flag | @as(u32, @intCast(payload.len)), .little);
    pos += 4;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(operation_id.len), .little);
    pos += 2;
    @memcpy(buf[pos..][0..operation_id.len], operation_id);
    pos += operation_id.len;
    @memcpy(buf[pos..][0..payload.len], payload);
    return buf;
}

fn encodeRecordsAlloc(alloc: std.mem.Allocator, records: []const wal_types.Record) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (records) |record| {
        const encoded = if (record.operation_id) |operation_id|
            try encodeIdempotentRecordAlloc(alloc, record.lsn, record.timestamp_ns, record.payload, operation_id)
        else
            try encodeRecordAlloc(alloc, record.lsn, record.timestamp_ns, record.payload);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    return try out.toOwnedSlice(alloc);
}

const ParsedRecord = struct {
    lsn: u64,
    timestamp_ns: u64,
    payload: []const u8,
    operation_id: ?[]const u8,
    next_cursor: usize,
};

fn parseRecord(raw: []const u8, start: usize) !ParsedRecord {
    var cursor = start;
    if (cursor + 8 + 8 + 4 > raw.len) return error.InvalidWal;
    const lsn = std.mem.readInt(u64, raw[cursor..][0..8], .little);
    cursor += 8;
    const timestamp_ns = std.mem.readInt(u64, raw[cursor..][0..8], .little);
    cursor += 8;
    const length_tag = std.mem.readInt(u32, raw[cursor..][0..4], .little);
    cursor += 4;
    const idempotent = length_tag & idempotent_record_flag != 0;
    const payload_len: usize = @intCast(length_tag & ~idempotent_record_flag);
    var operation_id: ?[]const u8 = null;
    if (idempotent) {
        if (cursor + 2 > raw.len) return error.InvalidWal;
        const operation_id_len = std.mem.readInt(u16, raw[cursor..][0..2], .little);
        cursor += 2;
        if (operation_id_len == 0 or cursor + operation_id_len > raw.len) return error.InvalidWal;
        operation_id = raw[cursor .. cursor + operation_id_len];
        cursor += operation_id_len;
    }
    if (cursor + payload_len > raw.len) return error.InvalidWal;
    const payload = raw[cursor .. cursor + payload_len];
    cursor += payload_len;
    return .{
        .lsn = lsn,
        .timestamp_ns = timestamp_ns,
        .payload = payload,
        .operation_id = operation_id,
        .next_cursor = cursor,
    };
}

fn decodeRecordsFromAlloc(alloc: std.mem.Allocator, raw: []const u8, start_lsn: u64) ![]wal_types.Record {
    var cursor: usize = 0;
    var out = std.ArrayListUnmanaged(wal_types.Record).empty;
    errdefer {
        wal_types.freeRecords(alloc, out.items);
        out = .empty;
    }

    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        if (parsed.lsn >= start_lsn) {
            try out.append(alloc, .{
                .lsn = parsed.lsn,
                .timestamp_ns = parsed.timestamp_ns,
                .payload = try alloc.dupe(u8, parsed.payload),
                .operation_id = if (parsed.operation_id) |operation_id|
                    try alloc.dupe(u8, operation_id)
                else
                    null,
            });
        }
        cursor = parsed.next_cursor;
    }

    return try out.toOwnedSlice(alloc);
}

fn lastLsn(raw: []const u8) !u64 {
    var cursor: usize = 0;
    var latest: u64 = 0;
    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        latest = parsed.lsn;
        cursor = parsed.next_cursor;
    }
    return latest;
}

fn findOperation(raw: []const u8, operation_id: []const u8) !?ParsedRecord {
    var cursor: usize = 0;
    while (cursor < raw.len) {
        const parsed = try parseRecord(raw, cursor);
        if (parsed.operation_id) |candidate| {
            if (std.mem.eql(u8, candidate, operation_id)) return parsed;
        }
        cursor = parsed.next_cursor;
    }
    return null;
}

fn logKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/wal.log", .{namespace});
    return try std.fmt.allocPrint(alloc, "{s}/{s}/wal.log", .{ prefix, namespace });
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

test "objectstore-backed wal store appends and truncates over file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "wal");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
    var store = impl.walStore();
    defer store.deinit();

    _ = try store.append("docs", 10, "one");
    _ = try store.append("docs", 20, "two");
    const records = try store.readFromAlloc("docs", 2);
    defer wal_types.freeRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 2), records[0].lsn);
    try std.testing.expectEqual(@as(u64, 1), try store.truncatePrefix("docs", 2));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-object-wal-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
