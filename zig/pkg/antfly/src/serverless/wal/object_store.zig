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

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const objectstore = @import("objectstore");
const wal_types = @import("types.zig");
const wal_store = @import("store.zig");
const remote_uri = @import("antfly_local_sources").serverless_remote_uri;
const object_store_support = @import("antfly_local_sources").serverless_object_store_support;

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    client: objectstore.Client,
    fs_client: ?*objectstore.FilesystemClient = null,
    gcs_client: ?*objectstore.Gcs.JsonApiClient = null,
    s3_client: ?*objectstore.S3.Client = null,
    owns_client: bool = true,
    indexed: ?*@import("indexed_object_store.zig").IndexedObjectStore = null,
    layout: std.atomic.Value(u8) = .init(0),
    bucket: []u8,
    prefix: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    const Layout = enum(u8) { legacy = 1, indexed = 2, managed_indexed = 3 };

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

    fn formatKey(self: *ObjectStore) ![]u8 {
        return if (self.prefix.len == 0) self.alloc.dupe(u8, ".antfly/WAL_FORMAT") else std.fmt.allocPrint(self.alloc, "{s}/.antfly/WAL_FORMAT", .{self.prefix});
    }
    fn readLayout(self: *ObjectStore) !?Layout {
        const key = try self.formatKey();
        defer self.alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = 32 }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(self.alloc);
        if (std.mem.eql(u8, result.body, "antfly-wal:legacy:1")) return .legacy;
        if (std.mem.eql(u8, result.body, "antfly-wal:indexed:1")) return .indexed;
        if (std.mem.eql(u8, result.body, "antfly-wal:indexed-managed:1")) return .managed_indexed;
        return error.UnsupportedWalLayout;
    }
    fn ensureLayout(self: *ObjectStore, wanted: Layout, create: bool) !void {
        const cached = self.layout.load(.acquire);
        if (cached != 0) {
            if (cached != @backingInt(wanted)) return error.WalLayoutMismatch;
            return;
        }
        if (try self.readLayout()) |selected| {
            if (selected != wanted) return error.WalLayoutMismatch;
            self.layout.store(@backingInt(selected), .release);
            return;
        }
        if (!create) return;
        const key = try self.formatKey();
        defer self.alloc.free(key);
        const value = switch (wanted) {
            .legacy => "antfly-wal:legacy:1",
            .indexed => "antfly-wal:indexed:1",
            .managed_indexed => "antfly-wal:indexed-managed:1",
        };
        var published = self.client.compareAndPut(self.bucket, key, value, null, .{}) catch |err| {
            // The immutable selection may have committed before an ACK was
            // lost, or another compatible opener may have selected it first.
            if (try self.readLayout()) |selected| {
                if (selected != wanted) return error.WalLayoutMismatch;
                self.layout.store(@backingInt(selected), .release);
                return;
            }
            return err;
        };
        defer published.deinit(self.alloc);
        self.layout.store(@backingInt(wanted), .release);
    }
    pub fn selectLegacy(self: *ObjectStore) !void {
        try self.ensureLayout(.legacy, false);
    }

    /// Explicit format selection. Existing legacy WALs must be migrated into a
    /// new prefix; activation may never silently hide acknowledged records.
    pub fn enableIndexed(self: *ObjectStore, options: struct { read_state: bool = false, document_index: bool = false, inline_tail: bool = true, managed_collection: bool = false }) !void {
        if (self.indexed != null) return error.WalAlreadyConfigured;
        const selected = try self.readLayout();
        if (selected == .legacy) return error.LegacyWalMigrationRequired;
        if (options.managed_collection and selected == .indexed) return error.ManagedWalMigrationRequired;
        const wanted: Layout = if (options.managed_collection or selected == .managed_indexed) .managed_indexed else .indexed;
        if (wanted == .managed_indexed and !@import("indexed_object_store.zig").IndexedObjectStore.managedClockAvailable()) return error.ManagedWalClockUnavailable;
        if (selected) |layout| self.layout.store(@backingInt(layout), .release);
        if (selected == null) {
            var cursor: ?[]u8 = null;
            defer if (cursor) |value| self.alloc.free(value);
            const listing_prefix = if (self.prefix.len == 0) try self.alloc.dupe(u8, "") else try std.fmt.allocPrint(self.alloc, "{s}/", .{self.prefix});
            defer self.alloc.free(listing_prefix);
            // A deleted current name does not prove a pristine versioned
            // prefix. Older unbounded readers may still hold historical roots.
            if (wanted == .managed_indexed and try self.client.supportsObjectVersions(self.bucket, null)) {
                var versions = try self.client.listObjectVersions(self.bucket, .{ .prefix = listing_prefix, .max_keys = 1 });
                defer versions.deinit(self.alloc);
                if (versions.entries.len != 0 or versions.is_truncated) return error.ManagedWalMigrationRequired;
            }
            while (true) {
                var page = try self.client.listObjects(self.bucket, .{ .prefix = listing_prefix, .continuation_token = cursor, .max_keys = 1000 });
                defer page.deinit(self.alloc);
                for (page.entries) |item| {
                    if (std.mem.endsWith(u8, item.key, "/wal.log")) return error.LegacyWalMigrationRequired;
                    if (wanted == .managed_indexed) return error.ManagedWalMigrationRequired;
                }
                if (page.next_continuation_token == null) break;
                const next = try self.alloc.dupe(u8, page.next_continuation_token orelse return error.InvalidObjectListing);
                if (cursor) |value| self.alloc.free(value);
                cursor = next;
            }
        }
        try self.ensureLayout(wanted, true);
        const indexed = try self.alloc.create(@import("indexed_object_store.zig").IndexedObjectStore);
        errdefer self.alloc.destroy(indexed);
        indexed.* = try @import("indexed_object_store.zig").IndexedObjectStore.init(self.alloc, self.client, self.bucket, self.prefix);
        indexed.read_state_enabled = options.read_state;
        indexed.document_index_enabled = options.document_index;
        indexed.inline_tail_enabled = options.inline_tail;
        indexed.managed_reads = wanted == .managed_indexed;
        self.indexed = indexed;
    }
    pub fn deinit(self: *ObjectStore) void {
        if (self.indexed) |value| {
            value.deinit();
            self.alloc.destroy(value);
        }
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
            .vtable = if (self.indexed != null) &indexed_vtable else &vtable,
        };
    }

    pub fn append(self: *ObjectStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        if (self.indexed) |value| return value.append(namespace, timestamp_ns, payload);
        try self.ensureLayout(.legacy, true);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);

        var current = try self.tryReadLog(self.alloc, key);
        defer if (current) |*value| {
            self.alloc.free(value.body);
            if (value.revision) |revision| freeRevision(self.alloc, revision);
        };

        const next_lsn: u64 = if (current) |value| (try lastLsn(value.body)) + 1 else 1;
        const encoded = try encodeRecordAlloc(self.alloc, next_lsn, timestamp_ns, payload);
        defer self.alloc.free(encoded);

        const combined = if (current) |value|
            try std.mem.concat(self.alloc, u8, &.{ value.body, encoded })
        else
            try self.alloc.dupe(u8, encoded);
        defer self.alloc.free(combined);

        var result = try self.client.compareAndPut(self.bucket, key, combined, if (current) |value| value.revision else null, .{
            .content_type = "application/octet-stream",
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
        return (try self.appendIdempotentMaybeLatest(namespace, timestamp_ns, payload, operation_id, null)).?;
    }

    pub fn appendIdempotentIfLatest(
        self: *ObjectStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        return try self.appendIdempotentMaybeLatest(
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
    }

    fn appendIdempotentMaybeLatest(
        self: *ObjectStore,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: ?u64,
    ) !?u64 {
        if (self.indexed) |value| {
            if (expected_latest_lsn) |expected| return value.appendIdempotentIfLatest(namespace, timestamp_ns, payload, operation_id, expected);
            return try value.appendIdempotent(namespace, timestamp_ns, payload, operation_id);
        }
        try self.ensureLayout(.legacy, true);
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
            if (value.revision) |revision| freeRevision(self.alloc, revision);
        };

        if (current) |value| {
            if (try findOperation(value.body, operation_id)) |existing| {
                if (existing.timestamp_ns != timestamp_ns or !std.mem.eql(u8, existing.payload, payload))
                    return error.WalIdempotencyConflict;
                return existing.lsn;
            }
            // An update without an ETag would turn the promised conditional
            // append into an unconditional whole-log replacement. Fail closed
            // on providers that omit the version token.
            if (value.revision == null) return error.MissingObjectEtag;
        }

        const next_lsn: u64 = if (current) |value| (try lastLsn(value.body)) + 1 else 1;
        if (expected_latest_lsn) |expected| {
            if (next_lsn - 1 != expected) return null;
        }
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

        var result = self.client.compareAndPut(self.bucket, key, combined, if (current) |value| value.revision else null, .{
            .content_type = "application/octet-stream",
        }) catch |err| switch (err) {
            error.PreconditionFailed => if (expected_latest_lsn != null) return null else return err,
            else => return err,
        };
        defer result.deinit(self.alloc);
        return next_lsn;
    }

    pub fn readFromAlloc(self: *ObjectStore, alloc: std.mem.Allocator, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        if (self.indexed) |value| {
            var wal = value.walStore();
            wal.allocator = alloc;
            return wal.readFromAlloc(namespace, start_lsn);
        }
        try self.ensureLayout(.legacy, false);
        const key = try logKeyAlloc(alloc, self.prefix, namespace);
        defer alloc.free(key);

        var result = self.client.getObject(self.bucket, key, .{ .skip_metadata_probe = true }) catch |err| switch (err) {
            error.FileNotFound => return try alloc.alloc(wal_types.Record, 0),
            else => return err,
        };
        defer result.deinit(alloc);
        return try decodeRecordsFromAlloc(alloc, result.body, start_lsn);
    }

    pub fn latestLsn(self: *ObjectStore, namespace: []const u8) !u64 {
        if (self.indexed) |value| return value.latestLsn(namespace);
        try self.ensureLayout(.legacy, false);
        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{ .skip_metadata_probe = true }) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer result.deinit(self.alloc);
        return try lastLsn(result.body);
    }

    pub fn truncatePrefix(self: *ObjectStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        if (self.indexed) |value| return value.truncatePrefix(namespace, keep_from_lsn);
        try self.ensureLayout(.legacy, true);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const key = try logKeyAlloc(self.alloc, self.prefix, namespace);
        defer self.alloc.free(key);

        var current = try self.tryReadLog(self.alloc, key);
        defer if (current) |*value| {
            self.alloc.free(value.body);
            if (value.revision) |revision| freeRevision(self.alloc, revision);
        };
        if (current == null) return 0;

        const records = try decodeRecordsFromAlloc(self.alloc, current.?.body, 1);
        defer wal_types.freeRecords(self.alloc, records);

        var kept = std.ArrayListUnmanaged(wal_types.Record).empty;
        defer {
            for (kept.items) |record| {
                self.alloc.free(record.payload);
                if (record.operation_id) |operation_id| self.alloc.free(operation_id);
            }
            kept.deinit(self.alloc);
        }

        var removed: u64 = 0;
        for (records) |record| {
            if (record.lsn < keep_from_lsn) {
                removed += 1;
                continue;
            }
            const cloned = try cloneRecordAlloc(self.alloc, record);
            kept.append(self.alloc, cloned) catch |err| {
                self.alloc.free(cloned.payload);
                if (cloned.operation_id) |operation_id| self.alloc.free(operation_id);
                return err;
            };
        }

        if (kept.items.len == 0) {
            try self.client.compareAndDelete(self.bucket, key, current.?.revision orelse return error.MissingObjectRevision, .{});
            return removed;
        }

        const encoded = try encodeRecordsAlloc(self.alloc, kept.items);
        defer self.alloc.free(encoded);
        var result = try self.client.compareAndPut(self.bucket, key, encoded, current.?.revision orelse return error.MissingObjectRevision, .{
            .content_type = "application/octet-stream",
        });
        defer result.deinit(self.alloc);
        return removed;
    }

    const CurrentLog = struct {
        body: []u8,
        revision: ?objectstore.ObjectRevision,
    };

    fn tryReadLog(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !?CurrentLog {
        var result = self.client.getObject(self.bucket, key, .{ .skip_metadata_probe = true }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        const body = try alloc.dupe(u8, result.body);
        errdefer alloc.free(body);
        return .{
            .body = body,
            .revision = if (result.metadata.writeRevision()) |value| try value.clone(alloc) else null,
        };
    }

    const indexed_vtable: wal_store.WalStore.VTable = blk: {
        var value = vtable;
        value.read_snapshot = erasedSnapshot;
        value.read_point_alloc = erasedPoint;
        value.read_range_alloc = erasedRange;
        value.append_batch = erasedUnkeyedBatch;
        value.append_batch_idempotent = erasedBatch;
        break :blk value;
    };
    fn erasedSnapshot(ptr: *anyopaque, ns: []const u8) !wal_types.ReadSnapshot {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return self.indexed.?.readSnapshot(ns);
    }
    fn erasedPoint(ptr: *anyopaque, a: std.mem.Allocator, ns: []const u8, id: []const u8, range: wal_types.ReadRange) !wal_types.PointRead {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return self.indexed.?.readPointAlloc(a, ns, id, range);
    }
    fn erasedRange(ptr: *anyopaque, a: std.mem.Allocator, ns: []const u8, range: wal_types.ReadRange) ![]wal_types.Record {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return self.indexed.?.readRangeAlloc(a, ns, range);
    }
    fn erasedUnkeyedBatch(ptr: *anyopaque, ns: []const u8, timestamp: u64, payloads: []const []const u8) !wal_types.BatchResult {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return self.indexed.?.appendBatch(ns, timestamp, payloads);
    }
    fn erasedBatch(ptr: *anyopaque, ns: []const u8, timestamp: u64, payloads: []const []const u8, id: []const u8) !wal_types.BatchResult {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return self.indexed.?.appendBatchIdempotent(ns, timestamp, payloads, id);
    }

    const vtable: wal_store.WalStore.VTable = .{
        .deinit = erasedDeinit,
        .append = erasedAppend,
        .append_idempotent = erasedAppendIdempotent,
        .append_idempotent_if_latest = erasedAppendIdempotentIfLatest,
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

    fn erasedAppendIdempotentIfLatest(
        ptr: *anyopaque,
        namespace: []const u8,
        timestamp_ns: u64,
        payload: []const u8,
        operation_id: []const u8,
        expected_latest_lsn: u64,
    ) !?u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.appendIdempotentIfLatest(
            namespace,
            timestamp_ns,
            payload,
            operation_id,
            expected_latest_lsn,
        );
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

const record_codec = @import("record_codec.zig");
pub const encodeRecordAlloc = record_codec.encodeRecordAlloc;
pub const idempotent_record_flag = record_codec.idempotent_record_flag;
pub const idempotent_payload_max = record_codec.idempotent_payload_max;
pub const encodeIdempotentRecordAlloc = record_codec.encodeIdempotentRecordAlloc;
pub const encodeRecordsAlloc = record_codec.encodeRecordsAlloc;
pub const ParsedRecord = record_codec.ParsedRecord;
pub const parseRecord = record_codec.parseRecord;
pub const decodeRecordsFromAlloc = record_codec.decodeRecordsFromAlloc;
pub const lastLsn = record_codec.lastLsn;
pub const findOperation = record_codec.findOperation;

fn cloneRecordAlloc(alloc: std.mem.Allocator, record: wal_types.Record) !wal_types.Record {
    const payload = try alloc.dupe(u8, record.payload);
    errdefer alloc.free(payload);
    return .{
        .lsn = record.lsn,
        .timestamp_ns = record.timestamp_ns,
        .payload = payload,
        .operation_id = if (record.operation_id) |operation_id|
            try alloc.dupe(u8, operation_id)
        else
            null,
    };
}

fn logKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/wal.log", .{namespace});
    return try std.fmt.allocPrint(alloc, "{s}/{s}/wal.log", .{ prefix, namespace });
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn freeRevision(alloc: std.mem.Allocator, revision: objectstore.ObjectRevision) void {
    var owned = revision;
    owned.deinit(alloc);
}

test "serverless objectstore WAL retained record cloning is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var payload = "payload".*;
            var operation_id = "operation".*;
            const cloned = try cloneRecordAlloc(alloc, .{
                .lsn = 1,
                .timestamp_ns = 2,
                .payload = payload[0..],
                .operation_id = operation_id[0..],
            });
            defer {
                alloc.free(cloned.payload);
                if (cloned.operation_id) |owned_operation_id| alloc.free(owned_operation_id);
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "serverless objectstore-backed WAL conditionally appends and truncates over file uri" {
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
    try std.testing.expectEqual(
        @as(?u64, null),
        try store.appendIdempotentIfLatest("docs", 30, "stale", "enrich/1", 1),
    );
    try std.testing.expectEqual(
        @as(?u64, 3),
        try store.appendIdempotentIfLatest("docs", 30, "derived", "enrich/1", 2),
    );
    try std.testing.expectEqual(@as(u64, 4), try store.append("docs", 40, "user"));
    try std.testing.expectEqual(
        @as(?u64, null),
        try store.appendIdempotentIfLatest("docs", 50, "stale-two", "enrich/2", 3),
    );
    try std.testing.expectEqual(@as(u64, 4), try store.latestLsn("docs"));
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

test "serverless indexed WAL selection preserves transport ownership capabilities and rejects legacy prefixes" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var store = try ObjectStore.initWithClient(a, client, "db", "new");
    defer store.deinit();
    try store.enableIndexed(.{ .read_state = true, .document_index = true });
    var wal = store.walStore();
    const payload = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = "value" });
    defer a.free(payload);
    try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("docs", 1, payload, "operation"));
    try std.testing.expectEqual(@as(u64, 1), try wal.appendIdempotent("docs", 1, payload, "operation"));
    try std.testing.expectEqual(@as(u64, 1), (try wal.readSnapshot("docs")).?.latest_lsn);
    const point = try wal.readPointAlloc("docs", "one", .{ .start_lsn = 1, .end_lsn = 1 });
    defer if (point.record) |record| {
        a.free(record.payload);
        if (record.operation_id) |id| a.free(id);
    };
    try std.testing.expect(point.indexed);
    try std.testing.expectEqualStrings(payload, point.record.?.payload);
    var legacy = try ObjectStore.initWithClient(a, client, "db", "old");
    defer legacy.deinit();
    _ = try legacy.append("docs", 1, "acknowledged");
    try std.testing.expectError(error.LegacyWalMigrationRequired, legacy.enableIndexed(.{}));
    try std.testing.expectEqual(@as(u64, 1), try legacy.latestLsn("docs"));
}

test "serverless multiprovider WAL format selection survives lost acknowledgement and bounds reopen" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var backing = memory.client();
    try backing.makeBucket("db");
    var faults = objectstore.ScriptedFaultClient.init(a, backing);
    defer faults.deinit();
    faults.commitNextPutThenFail(error.InjectedFormatAckLoss);
    var selected = try ObjectStore.initWithClient(a, faults.client(), "db", "selected");
    defer selected.deinit();
    try selected.enableIndexed(.{});
    var legacy = try ObjectStore.initWithClient(a, backing, "db", "selected");
    defer legacy.deinit();
    try std.testing.expectError(error.WalLayoutMismatch, legacy.selectLegacy());
    try std.testing.expectError(error.WalLayoutMismatch, legacy.latestLsn("docs"));
    try std.testing.expectError(error.WalLayoutMismatch, legacy.append("docs", 1, "must-not-shadow"));
    memory.resetOperationCount();
    var reopened = try ObjectStore.initWithClient(a, backing, "db", "selected");
    defer reopened.deinit();
    try reopened.enableIndexed(.{});
    try std.testing.expectEqual(@as(u64, 2), memory.operationCount()); // bucket + immutable format, no LIST
    var malformed = try backing.putObject("db", "future/.antfly/WAL_FORMAT", "antfly-wal:future:8", .{});
    malformed.deinit(a);
    var future = try ObjectStore.initWithClient(a, backing, "db", "future");
    defer future.deinit();
    try std.testing.expectError(error.UnsupportedWalLayout, future.enableIndexed(.{}));
    try std.testing.expectError(error.UnsupportedWalLayout, future.selectLegacy());
}

test "serverless multiprovider managed WAL activates only on a new prefix and persists its contract with collection disabled" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var backing = memory.client();
    try backing.makeBucket("db");
    var old = try ObjectStore.initWithClient(a, backing, "db", "old");
    defer old.deinit();
    try old.enableIndexed(.{});
    var upgrade = try ObjectStore.initWithClient(a, backing, "db", "old");
    defer upgrade.deinit();
    try std.testing.expectError(error.ManagedWalMigrationRequired, upgrade.enableIndexed(.{ .managed_collection = true }));
    var object = try backing.putObject("db", "unmarked/wal-v1/unknown/HEAD", "unmanaged", .{});
    object.deinit(a);
    var unmarked = try ObjectStore.initWithClient(a, backing, "db", "unmarked");
    defer unmarked.deinit();
    try std.testing.expectError(error.ManagedWalMigrationRequired, unmarked.enableIndexed(.{ .managed_collection = true }));
    try std.testing.expectError(error.FileNotFound, backing.getObject("db", "unmarked/.antfly/WAL_FORMAT", .{}));

    var faults = objectstore.ScriptedFaultClient.init(a, backing);
    defer faults.deinit();
    faults.commitNextPutThenFail(error.InjectedManagedSelectionAckLoss);
    var selected = try ObjectStore.initWithClient(a, faults.client(), "db", "managed");
    defer selected.deinit();
    try selected.enableIndexed(.{ .managed_collection = true });
    try std.testing.expect(selected.indexed.?.managed_reads);
    _ = try selected.append("docs", 1, "alpha");
    var reopened = try ObjectStore.initWithClient(a, backing, "db", "managed");
    defer reopened.deinit();
    // Turning off collection cannot turn off an already persisted contract.
    try reopened.enableIndexed(.{});
    try std.testing.expect(reopened.indexed.?.managed_reads);
    const records = try reopened.indexed.?.readRangeAlloc(a, "docs", .{ .start_lsn = 1, .end_lsn = 1 });
    defer wal_types.freeRecords(a, records);
    try std.testing.expectEqualStrings("alpha", records[0].payload);
    var legacy = try ObjectStore.initWithClient(a, backing, "db", "managed");
    defer legacy.deinit();
    try std.testing.expectError(error.WalLayoutMismatch, legacy.selectLegacy());
}

test "serverless multiprovider concurrent layout selection cannot create a shadow legacy WAL" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    var backing = memory.client();
    try backing.makeBucket("db");
    var indexed = try ObjectStore.initWithClient(a, backing, "db", "race");
    defer indexed.deinit();
    const Race = struct {
        store: *ObjectStore,
        activated: bool = false,
        fn claim(raw: *anyopaque, _: []const u8, key: []const u8, _: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.endsWith(u8, key, "/WAL_FORMAT")) return false;
            self.store.enableIndexed(.{}) catch return true;
            self.activated = true;
            return true;
        }
    };
    var race: Race = .{ .store = &indexed };
    var faults = objectstore.ScriptedFaultClient.init(a, backing);
    defer faults.deinit();
    faults.put_filter = .{ .ptr = &race, .matches = Race.claim };
    var legacy = try ObjectStore.initWithClient(a, faults.client(), "db", "race");
    defer legacy.deinit();
    try std.testing.expectError(error.WalLayoutMismatch, legacy.append("docs", 1, "must-not-shadow"));
    try std.testing.expect(race.activated);
    try std.testing.expectError(error.FileNotFound, backing.getObject("db", "race/docs/wal.log", .{}));
    try std.testing.expectEqual(@as(u64, 0), try indexed.latestLsn("docs"));
}
