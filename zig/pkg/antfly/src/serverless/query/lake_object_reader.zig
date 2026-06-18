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

//! Object-storage backed range reader for lake scans.

const std = @import("std");
const lake_range_io = @import("lake_range_io.zig");
const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
const object_storage = @import("../../storage/object_storage.zig");

const Allocator = std.mem.Allocator;

pub const ObjectStorageRangeReader = struct {
    client: object_storage.ObjectStorage,

    pub fn init(client: object_storage.ObjectStorage) ObjectStorageRangeReader {
        return .{ .client = client };
    }

    pub fn parquetReader(self: *@This()) lake_parquet_rowgroup.ObjectRangeReader {
        return .{
            .ctx = self,
            .read_range_alloc = readRangeAlloc,
            .read_planned_range_alloc = readPlannedRangeAlloc,
        };
    }

    fn readRangeAlloc(
        ctx: *anyopaque,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var client = self.client;
        client.allocator = alloc;
        var result = try client.getObject(bucket, key, .{
            .range = .{ .offset = offset, .length = len },
        });
        defer result.deinit(alloc);
        if (result.body.len != len) return error.InvalidLakeRangeRead;
        return try alloc.dupe(u8, result.body);
    }

    fn readPlannedRangeAlloc(
        ctx: *anyopaque,
        alloc: Allocator,
        read: lake_range_io.RangeRead,
    ) ![]u8 {
        try read.validate();
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var client = self.client;
        client.allocator = alloc;
        const len: usize = std.math.cast(usize, read.range.len) orelse return error.InvalidLakeRangeRead;
        var result = try client.getObject(read.object.bucket, read.object.key, .{
            .range = .{ .offset = read.range.offset, .length = len },
            .if_match_etag = if (read.object.version.etag.len == 0) null else read.object.version.etag,
            .version_id = if (read.object.version.version_id.len == 0) null else read.object.version.version_id,
        });
        defer result.deinit(alloc);
        if (result.body.len != len) return error.InvalidLakeRangeRead;
        return try alloc.dupe(u8, result.body);
    }
};

test "object storage range reader delegates lake parquet range reads" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();

    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "events/part-a.parquet", "0123456789abcdef", .{});
    defer put.deinit(alloc);

    var range_reader = ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    const bytes = try parquet_reader.readAlloc(alloc, "bucket", "events/part-a.parquet", 4, 6);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);
}

test "object storage range reader enforces planned object etag" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();

    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "events/part-a.parquet", "0123456789abcdef", .{});
    defer put.deinit(alloc);

    var range_reader = ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    const read = lake_range_io.RangeRead{
        .object = .{
            .bucket = "bucket",
            .key = "events/part-a.parquet",
            .byte_len = 16,
            .version = .{ .etag = put.etag.? },
        },
        .range = .{ .offset = 4, .len = 6 },
        .purpose = .parquet_footer,
    };

    const bytes = try parquet_reader.readPlannedAlloc(alloc, read);
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("456789", bytes);

    try client.deleteObject("bucket", "events/part-a.parquet", .{});
    var overwritten = try client.putObject("bucket", "events/part-a.parquet", "stale-object-body", .{});
    defer overwritten.deinit(alloc);
    try std.testing.expectError(error.PreconditionFailed, parquet_reader.readPlannedAlloc(alloc, read));
}
