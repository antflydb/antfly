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
const object_storage = @import("../../storage/object_storage.zig");
const manifest_types = @import("types.zig");
const manifest_codec = @import("codec.zig");
const manifest_store = @import("store.zig");
const object_store_support = @import("../object_store_support.zig");
const platform_clock = @import("../../platform/clock.zig");

const winner_visibility_attempts: usize = 5;
const winner_visibility_initial_backoff_ms: u64 = 1;
const winner_visibility_max_backoff_ms: u64 = 8;

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    opened: object_store_support.OpenedObjectStore,
    clock: platform_clock.Clock,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return try initRemoteUriWithS3Options(alloc, uri, null);
    }

    pub fn initRemoteUriWithS3Options(
        alloc: std.mem.Allocator,
        uri: []const u8,
        s3_options: ?object_store_support.S3Options,
    ) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initRemoteUriWithS3Options(alloc, uri, "serverless-manifests", s3_options),
            .clock = platform_clock.Clock.real(),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initFileUri(alloc, uri, "serverless-manifests"),
            .clock = platform_clock.Clock.real(),
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initGcsUri(alloc, bucket, prefix),
            .clock = platform_clock.Clock.real(),
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initS3Uri(alloc, bucket, prefix),
            .clock = platform_clock.Clock.real(),
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, bucket, prefix),
            .clock = platform_clock.Clock.real(),
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        self.opened.deinit();
        self.* = undefined;
    }

    pub fn manifestStore(self: *ObjectStore) manifest_store.ManifestStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn put(self: *ObjectStore, manifest: manifest_types.Manifest) !void {
        const key = try manifestKeyAlloc(self.alloc, self.opened.prefix, manifest.namespace, manifest.version);
        defer self.alloc.free(key);
        const encoded = try manifest_codec.encodeAlloc(self.alloc, manifest);
        defer self.alloc.free(encoded);

        if (try self.tryGetEncoded(self.alloc, key)) |existing| {
            defer self.alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded)) return error.ManifestVersionAlreadyExists;
            return;
        }

        var result = self.opened.client.putObject(self.opened.bucket, key, encoded, .{
            .content_type = "application/octet-stream",
            .if_none_match = true,
        }) catch |err| switch (err) {
            error.PreconditionFailed => {
                // Immutable manifests are keyed by namespace and version. Two
                // publishers may both observe a missing key before one wins
                // the conditional create. Treat the loser as idempotent only
                // when the winner published exactly the same manifest.
                const existing = (try self.getWinnerAfterPrecondition(self.alloc, key)) orelse
                    return error.PreconditionFailed;
                defer self.alloc.free(existing);
                if (!std.mem.eql(u8, existing, encoded)) return error.ManifestVersionAlreadyExists;
                return;
            },
            else => return err,
        };
        defer result.deinit(self.alloc);
    }

    pub fn getAlloc(self: *ObjectStore, alloc: std.mem.Allocator, namespace: []const u8, version: u64) !manifest_types.Manifest {
        const key = try manifestKeyAlloc(alloc, self.opened.prefix, namespace, version);
        defer alloc.free(key);
        var result = try self.opened.client.getObject(self.opened.bucket, key, .{});
        defer result.deinit(alloc);
        return try manifest_codec.decodeAlloc(alloc, result.body);
    }

    pub fn setHead(self: *ObjectStore, namespace: []const u8, version: u64) !void {
        const key = try headKeyAlloc(self.alloc, self.opened.prefix, namespace);
        defer self.alloc.free(key);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{version});
        defer self.alloc.free(payload);
        var result = try self.opened.client.putObject(self.opened.bucket, key, payload, .{ .content_type = "text/plain" });
        defer result.deinit(self.alloc);
    }

    pub fn getHead(self: *ObjectStore, namespace: []const u8) !u64 {
        const key = try headKeyAlloc(self.alloc, self.opened.prefix, namespace);
        defer self.alloc.free(key);
        var result = try self.opened.client.getObject(self.opened.bucket, key, .{});
        defer result.deinit(self.alloc);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, result.body, " \t\r\n"), 10);
    }

    pub fn compareAndSwapHead(self: *ObjectStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const manifest_key = try manifestKeyAlloc(self.alloc, self.opened.prefix, namespace, version);
        defer self.alloc.free(manifest_key);
        var meta = self.opened.client.statObject(self.opened.bucket, manifest_key) catch return error.ManifestVersionNotFound;
        defer meta.deinit(self.alloc);

        const head_key = try headKeyAlloc(self.alloc, self.opened.prefix, namespace);
        defer self.alloc.free(head_key);

        const current = self.tryReadHead(self.alloc, head_key) catch |err| switch (err) {
            error.FileNotFound => null,
            error.PreconditionFailed => return false,
            else => return err,
        };
        defer if (current) |*value| self.alloc.free(value.etag);

        if ((if (current) |value| value.version else null) != expected) return false;

        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{version});
        defer self.alloc.free(payload);

        var result = self.opened.client.putObject(self.opened.bucket, head_key, payload, .{
            .content_type = "text/plain",
            .if_none_match = current == null,
            .if_match_etag = if (current) |value| value.etag else null,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return false,
            else => return err,
        };
        defer result.deinit(self.alloc);
        return true;
    }

    pub fn listVersionsAlloc(self: *ObjectStore, alloc: std.mem.Allocator, namespace: []const u8) ![]u64 {
        return try self.listVersionsAllocWithPageSize(alloc, namespace, 1000);
    }

    fn listVersionsAllocWithPageSize(self: *ObjectStore, alloc: std.mem.Allocator, namespace: []const u8, page_size: u32) ![]u64 {
        if (page_size == 0) return error.InvalidPageSize;
        const prefix = try manifestsPrefixAlloc(alloc, self.opened.prefix, namespace);
        defer alloc.free(prefix);

        var versions = std.ArrayListUnmanaged(u64).empty;
        defer versions.deinit(alloc);
        var continuation_token: ?[]u8 = null;
        defer if (continuation_token) |token| alloc.free(token);
        while (true) {
            var listed = try self.opened.client.listObjects(self.opened.bucket, .{
                .prefix = prefix,
                .recursive = true,
                .max_keys = page_size,
                .continuation_token = continuation_token,
            });
            defer listed.deinit(alloc);
            var next_token = if (listed.next_continuation_token) |token| try alloc.dupe(u8, token) else null;
            errdefer if (next_token) |token| alloc.free(token);
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) return error.InvalidManifestKey;
                const version = parseVersionFromManifestKey(entry.key) catch continue;
                try versions.append(alloc, version);
            }
            if (continuation_token != null and next_token != null and std.mem.eql(u8, continuation_token.?, next_token.?)) {
                return error.InvalidContinuationToken;
            }
            if (continuation_token) |token| alloc.free(token);
            continuation_token = next_token;
            next_token = null;
            if (continuation_token == null) break;
        }
        std.mem.sort(u64, versions.items, {}, std.sort.asc(u64));
        return try versions.toOwnedSlice(alloc);
    }

    pub fn deleteVersion(self: *ObjectStore, namespace: []const u8, version: u64) !void {
        const current_head = self.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current_head != null and current_head.? == version) return error.CannotDeleteHead;

        const key = try manifestKeyAlloc(self.alloc, self.opened.prefix, namespace, version);
        defer self.alloc.free(key);
        try self.opened.client.deleteObject(self.opened.bucket, key, .{});
    }

    const HeadValue = struct {
        version: u64,
        etag: []u8,
    };

    fn tryReadHead(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !HeadValue {
        var result = try self.opened.client.getObject(self.opened.bucket, key, .{});
        defer result.deinit(alloc);
        if (result.metadata.etag) |etag| {
            return .{
                .version = try std.fmt.parseInt(u64, std.mem.trim(u8, result.body, " \t\r\n"), 10),
                .etag = try alloc.dupe(u8, etag),
            };
        }

        var metadata = try self.opened.client.statObject(self.opened.bucket, key);
        defer metadata.deinit(alloc);
        const stat_etag = metadata.etag orelse return error.MissingObjectEtag;
        var verified = try self.opened.client.getObject(self.opened.bucket, key, .{ .if_match_etag = stat_etag });
        defer verified.deinit(alloc);
        return .{
            .version = try std.fmt.parseInt(u64, std.mem.trim(u8, verified.body, " \t\r\n"), 10),
            .etag = try alloc.dupe(u8, stat_etag),
        };
    }

    fn tryGetEncoded(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        var result = self.opened.client.getObject(self.opened.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        return try alloc.dupe(u8, result.body);
    }

    fn getWinnerAfterPrecondition(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        // Native backends are read-after-write consistent. Keep host adapter
        // tolerance bounded so publication cannot block indefinitely or accept
        // an unverified winner after a conditional-create race.
        var backoff_ms = winner_visibility_initial_backoff_ms;
        for (0..winner_visibility_attempts) |attempt| {
            if (try self.tryGetEncoded(alloc, key)) |existing| return existing;
            if (attempt + 1 == winner_visibility_attempts) return null;
            self.clock.sleepMs(backoff_ms);
            backoff_ms = @min(backoff_ms * 2, winner_visibility_max_backoff_ms);
        }
        unreachable;
    }

    const vtable: manifest_store.ManifestStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .set_head = erasedSetHead,
        .get_head = erasedGetHead,
        .compare_and_swap_head = erasedCompareAndSwapHead,
        .list_versions_alloc = erasedListVersionsAlloc,
        .delete_version = erasedDeleteVersion,
    };

    fn erasedDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, manifest: manifest_types.Manifest) !void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        try self.put(manifest);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, namespace: []const u8, version: u64) !manifest_types.Manifest {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, namespace, version);
    }

    fn erasedSetHead(ptr: *anyopaque, namespace: []const u8, version: u64) !void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        try self.setHead(namespace, version);
    }

    fn erasedGetHead(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getHead(namespace);
    }

    fn erasedCompareAndSwapHead(ptr: *anyopaque, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapHead(namespace, expected, version);
    }

    fn erasedListVersionsAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, namespace: []const u8) ![]u64 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.listVersionsAlloc(alloc, namespace);
    }

    fn erasedDeleteVersion(ptr: *anyopaque, namespace: []const u8, version: u64) !void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        try self.deleteVersion(namespace, version);
    }
};

fn manifestsPrefixAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/manifests/", .{namespace});
    return try std.fmt.allocPrint(alloc, "{s}/{s}/manifests/", .{ prefix, namespace });
}

fn manifestKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8, version: u64) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/manifests/{d}.bin", .{ namespace, version });
    return try std.fmt.allocPrint(alloc, "{s}/{s}/manifests/{d}.bin", .{ prefix, namespace, version });
}

fn headKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8, namespace: []const u8) ![]u8 {
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}/HEAD", .{namespace});
    return try std.fmt.allocPrint(alloc, "{s}/{s}/HEAD", .{ prefix, namespace });
}

fn parseVersionFromManifestKey(key: []const u8) !u64 {
    const slash = std.mem.lastIndexOfScalar(u8, key, '/') orelse return error.InvalidManifestKey;
    const file_name = key[slash + 1 ..];
    if (!std.mem.endsWith(u8, file_name, ".bin")) return error.InvalidManifestKey;
    return try std.fmt.parseInt(u64, file_name[0 .. file_name.len - 4], 10);
}

const ConditionalCreateRaceClient = struct {
    backing: *object_storage.MemoryObjectStorage,
    winner_body: []const u8,
    injected: bool = false,
    hidden_reads_after_publish: usize = 0,
    omit_get_etag: bool = false,
    omit_stat_etag: bool = false,

    fn client(self: *@This()) object_storage.ObjectStorage {
        return .{
            .allocator = std.testing.allocator,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn backingClient(self: *@This(), alloc: std.mem.Allocator) object_storage.ObjectStorage {
        var client_impl = self.backing.client();
        client_impl.allocator = alloc;
        return client_impl;
    }

    fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}

    fn bucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(std.testing.allocator);
        return try client_impl.bucketExists(bucket);
    }

    fn makeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(std.testing.allocator);
        try client_impl.makeBucket(bucket);
    }

    fn putObject(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        opts: object_storage.PutOptions,
    ) !object_storage.PutResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(alloc);
        if (opts.if_none_match and !self.injected) {
            self.injected = true;
            var winner = try client_impl.putObject(bucket, key, self.winner_body, opts);
            winner.deinit(alloc);
            return error.PreconditionFailed;
        }
        return try client_impl.putObject(bucket, key, body, opts);
    }

    fn getObject(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        bucket: []const u8,
        key: []const u8,
        opts: object_storage.GetOptions,
    ) !object_storage.GetResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.injected and self.hidden_reads_after_publish > 0) {
            self.hidden_reads_after_publish -= 1;
            return error.FileNotFound;
        }
        var client_impl = self.backingClient(alloc);
        var result = try client_impl.getObject(bucket, key, opts);
        if (self.omit_get_etag) {
            if (result.metadata.etag) |etag| alloc.free(etag);
            result.metadata.etag = null;
        }
        return result;
    }

    fn getObjectAttributes(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8) !object_storage.ObjectAttributes {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(alloc);
        return try client_impl.getObjectAttributes(bucket, key);
    }

    fn statObject(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8) !object_storage.ObjectMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(alloc);
        var metadata = try client_impl.statObject(bucket, key);
        if (self.omit_stat_etag) {
            if (metadata.etag) |etag| alloc.free(etag);
            metadata.etag = null;
        }
        return metadata;
    }

    fn deleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: object_storage.DeleteOptions) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(std.testing.allocator);
        try client_impl.deleteObject(bucket, key, opts);
    }

    fn listObjects(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, opts: object_storage.ListOptions) !object_storage.ListResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var client_impl = self.backingClient(alloc);
        return try client_impl.listObjects(bucket, opts);
    }

    const vtable: object_storage.ObjectStorage.VTable = .{
        .deinit = deinit,
        .bucket_exists = bucketExists,
        .make_bucket = makeBucket,
        .put_object = putObject,
        .get_object = getObject,
        .get_object_attributes = getObjectAttributes,
        .stat_object = statObject,
        .delete_object = deleteObject,
        .list_objects = listObjects,
    };
};

test "objectstore-backed manifest store supports publish and list" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "manifests");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
    var store = impl.manifestStore();
    defer store.deinit();

    var manifest = manifest_types.Manifest{
        .namespace = try std.testing.allocator.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 10,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{},
        .artifacts = try std.testing.allocator.alloc(manifest_types.ArtifactRef, 0),
    };
    defer manifest.deinit(std.testing.allocator);

    try store.put(manifest);
    try store.put(manifest);
    manifest.built_at_ns = 11;
    try std.testing.expectError(error.ManifestVersionAlreadyExists, store.put(manifest));
    manifest.built_at_ns = 10;
    try std.testing.expect(try store.compareAndSwapHead("docs", null, 1));
    manifest.version = 2;
    try store.put(manifest);
    manifest.version = 3;
    try store.put(manifest);
    const versions = try impl.listVersionsAllocWithPageSize(std.testing.allocator, "docs", 2);
    defer std.testing.allocator.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, versions);
}

test "manifest head CAS verifies a stat ETag when GET omits it" {
    var backing = object_storage.MemoryObjectStorage.init(std.testing.allocator);
    defer backing.deinit();
    var adapter = ConditionalCreateRaceClient{
        .backing = &backing,
        .winner_body = "",
        .injected = true,
        .omit_get_etag = true,
    };
    var store = try ObjectStore.initWithClient(std.testing.allocator, adapter.client(), "manifests", "");
    defer store.deinit();

    var manifest = manifest_types.Manifest{
        .namespace = try std.testing.allocator.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 10,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{},
        .artifacts = try std.testing.allocator.alloc(manifest_types.ArtifactRef, 0),
    };
    defer manifest.deinit(std.testing.allocator);

    try store.put(manifest);
    try store.setHead("docs", 1);
    manifest.version = 2;
    try store.put(manifest);
    try std.testing.expect(try store.compareAndSwapHead("docs", 1, 2));

    manifest.version = 3;
    try store.put(manifest);
    adapter.omit_stat_etag = true;
    try std.testing.expectError(error.MissingObjectEtag, store.compareAndSwapHead("docs", 2, 3));
    try std.testing.expectEqual(@as(u64, 2), try store.getHead("docs"));
}

test "objectstore-backed manifest store resolves conditional create races by content" {
    var manifest = manifest_types.Manifest{
        .namespace = try std.testing.allocator.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 10,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{},
        .artifacts = try std.testing.allocator.alloc(manifest_types.ArtifactRef, 0),
    };
    defer manifest.deinit(std.testing.allocator);

    const encoded = try manifest_codec.encodeAlloc(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    {
        var backing = object_storage.MemoryObjectStorage.init(std.testing.allocator);
        defer backing.deinit();
        var race = ConditionalCreateRaceClient{
            .backing = &backing,
            .winner_body = encoded,
            .hidden_reads_after_publish = 2,
        };
        var impl = try ObjectStore.initWithClient(std.testing.allocator, race.client(), "manifests", "");
        var clock = platform_clock.ManualClock{};
        impl.clock = clock.clock();
        var store = impl.manifestStore();
        defer store.deinit();

        try store.put(manifest);
        try std.testing.expect(race.injected);
        try std.testing.expectEqual(@as(usize, 0), race.hidden_reads_after_publish);
    }

    var conflicting = manifest;
    conflicting.built_at_ns = 11;
    const conflicting_encoded = try manifest_codec.encodeAlloc(std.testing.allocator, conflicting);
    defer std.testing.allocator.free(conflicting_encoded);
    {
        var backing = object_storage.MemoryObjectStorage.init(std.testing.allocator);
        defer backing.deinit();
        var race = ConditionalCreateRaceClient{
            .backing = &backing,
            .winner_body = conflicting_encoded,
            .hidden_reads_after_publish = 1,
        };
        var impl = try ObjectStore.initWithClient(std.testing.allocator, race.client(), "manifests", "");
        var clock = platform_clock.ManualClock{};
        impl.clock = clock.clock();
        var store = impl.manifestStore();
        defer store.deinit();

        try std.testing.expectError(error.ManifestVersionAlreadyExists, store.put(manifest));
        try std.testing.expect(race.injected);
        try std.testing.expectEqual(@as(usize, 0), race.hidden_reads_after_publish);
    }

    {
        var backing = object_storage.MemoryObjectStorage.init(std.testing.allocator);
        defer backing.deinit();
        var race = ConditionalCreateRaceClient{
            .backing = &backing,
            .winner_body = encoded,
            .hidden_reads_after_publish = winner_visibility_attempts,
        };
        var impl = try ObjectStore.initWithClient(std.testing.allocator, race.client(), "manifests", "");
        var clock = platform_clock.ManualClock{};
        impl.clock = clock.clock();
        var store = impl.manifestStore();
        defer store.deinit();

        try std.testing.expectError(error.PreconditionFailed, store.put(manifest));
        try std.testing.expect(race.injected);
        try std.testing.expectEqual(@as(usize, 0), race.hidden_reads_after_publish);
    }
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-object-manifests-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
