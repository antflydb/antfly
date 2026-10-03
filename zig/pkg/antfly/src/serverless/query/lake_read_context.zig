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

//! A serving request's deadline and cancellation reach object providers through
//! one owner. No inventory or object request may refresh outside that context.
const std = @import("std");
const storage = @import("../../storage/object_storage.zig");
const Allocator = std.mem.Allocator;

pub const Context = struct {
    deadline_ns: ?u64 = null,
    cancellation: ?storage.CancellationToken = null,
    pub fn ensureActive(self: Context) !void {
        if (self.cancellation) |token| try token.check();
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
    }
};

pub const Store = struct {
    base: storage.ObjectStorage,
    context: Context,
    pub fn client(self: *Store, alloc: Allocator) storage.ObjectStorage {
        return .{ .allocator = alloc, .ptr = self, .vtable = &.{ .deinit = deinit, .bucket_exists = bucketExists, .make_bucket = makeBucket, .put_object = putObject, .get_object = getObject, .get_object_attributes = attributes, .stat_object = statObject, .stat_object_with_options = statObjectWithOptions, .delete_object = deleteObject, .list_objects = listObjects } };
    }
    fn from(raw: *anyopaque) *Store {
        return @ptrCast(@alignCast(raw));
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Store = @ptrCast(@alignCast(raw));
        self.context.ensureActive() catch return true;
        return false;
    }
    fn token(self: *Store) storage.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
    fn deinit(_: Allocator, _: *anyopaque) void {}
    fn makeBucket(_: *anyopaque, _: []const u8, _: storage.BucketOptions) !void {
        return error.ExternalLakeReadOnly;
    }
    fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: storage.PutOptions) !storage.PutResult {
        return error.ExternalLakeReadOnly;
    }
    fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: storage.DeleteOptions) !void {
        return error.ExternalLakeReadOnly;
    }
    fn bucketExists(raw: *anyopaque, bucket: []const u8, options: storage.BucketOptions) !bool {
        const self = from(raw);
        try self.context.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        const result = self.base.bucketExistsWithOptions(bucket, opts) catch |err| {
            try self.context.ensureActive();
            return err;
        };
        try self.context.ensureActive();
        return result;
    }
    fn getObject(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
        const self = from(raw);
        try self.context.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        var base = self.base;
        base.allocator = alloc;
        var result = base.getObject(bucket, key, opts) catch |err| {
            try self.context.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.context.ensureActive();
        return result;
    }
    fn statObject(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectMetadata {
        return statObjectWithOptions(raw, alloc, bucket, key, .{});
    }
    fn statObjectWithOptions(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: @import("objectstore").StatOptions) !storage.ObjectMetadata {
        const self = from(raw);
        try self.context.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        var base = self.base;
        base.allocator = alloc;
        var result = base.statObjectWithOptions(bucket, key, opts) catch |err| {
            try self.context.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.context.ensureActive();
        return result;
    }
    fn attributes(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectAttributes {
        const self = from(raw);
        try self.context.ensureActive();
        var base = self.base;
        base.allocator = alloc;
        var result = try base.getObjectAttributes(bucket, key);
        errdefer result.deinit(alloc);
        try self.context.ensureActive();
        return result;
    }
    fn listObjects(raw: *anyopaque, alloc: Allocator, bucket: []const u8, options: storage.ListOptions) !storage.ListResult {
        const self = from(raw);
        try self.context.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        var base = self.base;
        base.allocator = alloc;
        var result = base.listObjects(bucket, opts) catch |err| {
            try self.context.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.context.ensureActive();
        return result;
    }
};

test "external lake object requests reject cancellation and expired deadlines before I/O" {
    const alloc = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var signal = std.atomic.Value(bool).init(true);
    var store: Store = .{ .base = memory.client(), .context = .{ .cancellation = storage.CancellationToken.fromAtomic(&signal) } };
    var client = store.client(alloc);
    try std.testing.expectError(error.Canceled, client.getObject("bucket", "part", .{}));
    try std.testing.expectError(error.Canceled, client.listObjects("bucket", .{}));
    signal.store(false, .release);
    store.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, client.statObject("bucket", "part"));
}
