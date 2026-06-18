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
        if (result.body.len != len) return error.ShortLakeRangeRead;
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
