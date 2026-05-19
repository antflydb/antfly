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
const objectstore = @import("objectstore");

const Allocator = std.mem.Allocator;

pub const ObjectStorage = objectstore.Client;
pub const ObjectMetadata = objectstore.ObjectMetadata;
pub const PutOptions = objectstore.PutOptions;
pub const GetOptions = objectstore.GetOptions;
pub const DeleteOptions = objectstore.DeleteOptions;
pub const ListOptions = objectstore.ListOptions;
pub const ByteRange = objectstore.ByteRange;
pub const PutResult = objectstore.PutResult;
pub const GetResult = objectstore.GetResult;
pub const ObjectPart = objectstore.ObjectPart;
pub const ObjectAttributes = objectstore.ObjectAttributes;
pub const ListEntry = objectstore.ListEntry;
pub const ListResult = objectstore.ListResult;

pub const FilesystemObjectStorage = objectstore.FilesystemClient;
pub const MemoryObjectStorage = objectstore.MemoryClient;
pub const S3 = objectstore.S3;
pub const Gcs = objectstore.Gcs;

/// Thin wrapper for host-provided object storage callbacks.
/// Hosts can provide an object-oriented blob implementation without exposing
/// the underlying transport details to serverless or future external segment paths.
pub const HostObjectStorage = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const objectstore.Client.VTable,

    pub fn init(allocator: Allocator, ptr: *anyopaque, vtable: *const objectstore.Client.VTable) HostObjectStorage {
        return .{
            .allocator = allocator,
            .ptr = ptr,
            .vtable = vtable,
        };
    }

    pub fn objectStorage(self: HostObjectStorage) ObjectStorage {
        return .{
            .allocator = self.allocator,
            .ptr = self.ptr,
            .vtable = self.vtable,
        };
    }
};

test "host object storage delegates through callbacks" {
    var backing = MemoryObjectStorage.init(std.testing.allocator);
    defer backing.deinit();

    const HostContext = struct {
        backing: *MemoryObjectStorage,

        fn deinit(_: Allocator, ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self;
        }

        fn bucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            return try client.bucketExists(bucket);
        }

        fn makeBucket(ptr: *anyopaque, bucket: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            try client.makeBucket(bucket);
        }

        fn putObject(
            ptr: *anyopaque,
            alloc: Allocator,
            bucket: []const u8,
            key: []const u8,
            body: []const u8,
            opts: PutOptions,
        ) !PutResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            client.allocator = alloc;
            return try client.putObject(bucket, key, body, opts);
        }

        fn getObject(
            ptr: *anyopaque,
            alloc: Allocator,
            bucket: []const u8,
            key: []const u8,
            opts: GetOptions,
        ) !GetResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            client.allocator = alloc;
            return try client.getObject(bucket, key, opts);
        }

        fn getObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !ObjectAttributes {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            client.allocator = alloc;
            return try client.getObjectAttributes(bucket, key);
        }

        fn statObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !ObjectMetadata {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            client.allocator = alloc;
            return try client.statObject(bucket, key);
        }

        fn deleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: DeleteOptions) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            try client.deleteObject(bucket, key, opts);
        }

        fn listObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, opts: ListOptions) !ListResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var client = self.backing.client();
            client.allocator = alloc;
            return try client.listObjects(bucket, opts);
        }
    };

    const host_vtable: ObjectStorage.VTable = .{
        .deinit = HostContext.deinit,
        .bucket_exists = HostContext.bucketExists,
        .make_bucket = HostContext.makeBucket,
        .put_object = HostContext.putObject,
        .get_object = HostContext.getObject,
        .get_object_attributes = HostContext.getObjectAttributes,
        .stat_object = HostContext.statObject,
        .delete_object = HostContext.deleteObject,
        .list_objects = HostContext.listObjects,
    };

    var host_ctx = HostContext{ .backing = &backing };
    var storage = HostObjectStorage.init(std.testing.allocator, &host_ctx, &host_vtable).objectStorage();

    try storage.makeBucket("bucket");

    var put = try storage.putObject("bucket", "docs/a.txt", "alpha", .{ .content_type = "text/plain" });
    defer put.deinit(std.testing.allocator);

    var got = try storage.getObject("bucket", "docs/a.txt", .{});
    defer got.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("alpha", got.body);

    var listed = try storage.listObjects("bucket", .{ .prefix = "docs/" });
    defer listed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), listed.entries.len);
    try std.testing.expectEqualStrings("docs/a.txt", listed.entries[0].key);
}
