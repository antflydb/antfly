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

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    opened: object_store_support.OpenedObjectStore,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initRemoteUri(alloc, uri, "serverless-manifests"),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initFileUri(alloc, uri, "serverless-manifests"),
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initGcsUri(alloc, bucket, prefix),
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initS3Uri(alloc, bucket, prefix),
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, bucket, prefix),
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

        var result = try self.opened.client.putObject(self.opened.bucket, key, encoded, .{
            .content_type = "application/octet-stream",
            .if_none_match = true,
        });
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
            else => return err,
        };
        defer if (current) |*value| if (value.etag) |etag| self.alloc.free(etag);

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
        const prefix = try manifestsPrefixAlloc(alloc, self.opened.prefix, namespace);
        defer alloc.free(prefix);
        var listed = try self.opened.client.listObjects(self.opened.bucket, .{ .prefix = prefix, .recursive = true });
        defer listed.deinit(alloc);

        var versions = std.ArrayListUnmanaged(u64).empty;
        defer versions.deinit(alloc);
        for (listed.entries) |entry| {
            const version = parseVersionFromManifestKey(entry.key) catch continue;
            try versions.append(alloc, version);
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
        etag: ?[]u8,
    };

    fn tryReadHead(self: *ObjectStore, alloc: std.mem.Allocator, key: []const u8) !HeadValue {
        var result = try self.opened.client.getObject(self.opened.bucket, key, .{});
        defer result.deinit(alloc);
        return .{
            .version = try std.fmt.parseInt(u64, std.mem.trim(u8, result.body, " \t\r\n"), 10),
            .etag = if (result.metadata.etag) |value| try alloc.dupe(u8, value) else null,
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
    try std.testing.expect(try store.compareAndSwapHead("docs", null, 1));
    const versions = try store.listVersionsAlloc("docs");
    defer std.testing.allocator.free(versions);
    try std.testing.expectEqualSlices(u64, &.{1}, versions);
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
