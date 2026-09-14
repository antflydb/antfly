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

//! Request-local read capability for discovery under publication authority.
//! Checkpoints bracket every remote operation, renewing the namespace lease
//! throughout multi-page listing/footer discovery. Ownership stays with caller.
const std = @import("std");
const storage = @import("../../storage/object_storage.zig");
const Token = @import("../../common/cancellation.zig").CancellationToken;
const Allocator = std.mem.Allocator;

pub const ReadAuthority = struct {
    inner: storage.ObjectStorage,
    cancellation: Token,

    pub fn client(self: *@This()) storage.ObjectStorage {
        return .{ .allocator = self.inner.allocator, .ptr = self, .vtable = &.{
            .deinit = deinit,
            .bucket_exists = exists,
            .make_bucket = make,
            .put_object = put,
            .get_object = get,
            .get_object_attributes = attributes,
            .stat_object = stat,
            .stat_object_with_options = statOptions,
            .delete_object = delete,
            .list_objects = list,
        } };
    }
    fn selfFrom(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(_: Allocator, _: *anyopaque) void {}
    fn make(_: *anyopaque, _: []const u8, _: storage.BucketOptions) !void {
        return error.ExternalTableReadOnly;
    }
    fn put(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: storage.PutOptions) !storage.PutResult {
        return error.ExternalTableReadOnly;
    }
    fn delete(_: *anyopaque, _: []const u8, _: []const u8, _: storage.DeleteOptions) !void {
        return error.ExternalTableReadOnly;
    }
    fn exists(ptr: *anyopaque, bucket: []const u8, options: storage.BucketOptions) !bool {
        const self = selfFrom(ptr);
        try self.cancellation.check();
        const result = try self.inner.bucketExistsWithOptions(bucket, options);
        try self.cancellation.check();
        return result;
    }
    fn get(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
        const self = selfFrom(ptr);
        try self.cancellation.check();
        var inner = self.inner;
        inner.allocator = alloc;
        var result = try inner.getObject(bucket, key, options);
        errdefer result.deinit(alloc);
        try self.cancellation.check();
        return result;
    }
    fn attributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectAttributes {
        const self = selfFrom(ptr);
        try self.cancellation.check();
        var inner = self.inner;
        inner.allocator = alloc;
        var result = try inner.getObjectAttributes(bucket, key);
        errdefer result.deinit(alloc);
        try self.cancellation.check();
        return result;
    }
    fn stat(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectMetadata {
        return statOptions(ptr, alloc, bucket, key, .{});
    }
    fn statOptions(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: @import("objectstore").StatOptions) !storage.ObjectMetadata {
        const self = selfFrom(ptr);
        try self.cancellation.check();
        var inner = self.inner;
        inner.allocator = alloc;
        var result = try inner.statObjectWithOptions(bucket, key, options);
        errdefer result.deinit(alloc);
        try self.cancellation.check();
        return result;
    }
    fn list(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, options: storage.ListOptions) !storage.ListResult {
        const self = selfFrom(ptr);
        try self.cancellation.check();
        var inner = self.inner;
        inner.allocator = alloc;
        var result = try inner.listObjects(bucket, options);
        errdefer result.deinit(alloc);
        try self.cancellation.check();
        return result;
    }
};

test "serverless external discovery checks authority around each object read" {
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    var client = memory.client();
    defer client.deinit();
    var put = try client.putObject("bucket", "key", "body", .{});
    defer put.deinit(a);
    const State = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(ptr: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.checks += 1;
            if (self.checks == self.fail_at) return error.WorkLeaseLost;
        }
    };
    var state: State = .{};
    var authority: ReadAuthority = .{ .inner = client, .cancellation = .{ .ptr = &state, .check_fn = State.check } };
    var guarded = authority.client();
    var got = try guarded.getObject("bucket", "key", .{});
    defer got.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), state.checks);
    state.fail_at = 4;
    try std.testing.expectError(error.WorkLeaseLost, guarded.getObject("bucket", "key", .{}));
}
