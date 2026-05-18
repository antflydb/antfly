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
const Allocator = std.mem.Allocator;

pub const RemoteUri = union(enum) {
    file: []u8,
    gcs: BucketPath,
    s3: BucketPath,
};

pub const BucketPath = struct {
    bucket: []u8,
    prefix: []u8,

    pub fn deinit(self: *BucketPath, alloc: Allocator) void {
        alloc.free(self.bucket);
        alloc.free(self.prefix);
        self.* = undefined;
    }
};

pub fn parseAlloc(alloc: Allocator, uri: []const u8) !RemoteUri {
    if (std.mem.startsWith(u8, uri, "file://")) {
        return .{ .file = try filePathFromUriAlloc(alloc, uri) };
    }
    if (std.mem.startsWith(u8, uri, "gs://")) {
        return .{ .gcs = try bucketPathFromGsUriAlloc(alloc, uri) };
    }
    if (std.mem.startsWith(u8, uri, "s3://")) {
        return .{ .s3 = try bucketPathFromS3UriAlloc(alloc, uri) };
    }
    return error.UnsupportedRemoteUri;
}

pub fn filePathFromUriAlloc(alloc: Allocator, uri: []const u8) ![]u8 {
    const prefix = "file://";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.UnsupportedRemoteUri;
    if (uri.len == prefix.len) return error.InvalidRemoteUri;
    return try alloc.dupe(u8, uri[prefix.len..]);
}

pub fn bucketPathFromGsUriAlloc(alloc: Allocator, uri: []const u8) !BucketPath {
    return try bucketPathFromSchemeAlloc(alloc, uri, "gs://");
}

pub fn bucketPathFromS3UriAlloc(alloc: Allocator, uri: []const u8) !BucketPath {
    return try bucketPathFromSchemeAlloc(alloc, uri, "s3://");
}

fn bucketPathFromSchemeAlloc(alloc: Allocator, uri: []const u8, prefix: []const u8) !BucketPath {
    if (!std.mem.startsWith(u8, uri, prefix)) return error.UnsupportedRemoteUri;
    const rest = uri[prefix.len..];
    if (rest.len == 0) return error.InvalidRemoteUri;

    const slash = std.mem.indexOfScalar(u8, rest, '/');
    const bucket = if (slash) |idx| rest[0..idx] else rest;
    if (bucket.len == 0) return error.InvalidRemoteUri;
    const object_prefix = if (slash) |idx| trimLeftSlash(rest[idx + 1 ..]) else "";

    return .{
        .bucket = try alloc.dupe(u8, bucket),
        .prefix = try alloc.dupe(u8, object_prefix),
    };
}

test "remote uri parses file scheme" {
    const alloc = std.testing.allocator;
    const path = try filePathFromUriAlloc(alloc, "file:///tmp/antfly");
    defer alloc.free(path);
    try std.testing.expectEqualStrings("/tmp/antfly", path);
}

test "remote uri parses gs scheme" {
    const alloc = std.testing.allocator;
    var bucket_path = try bucketPathFromGsUriAlloc(alloc, "gs://serverless-bucket/manifests/prod");
    defer bucket_path.deinit(alloc);
    try std.testing.expectEqualStrings("serverless-bucket", bucket_path.bucket);
    try std.testing.expectEqualStrings("manifests/prod", bucket_path.prefix);
}

test "remote uri parses s3 scheme" {
    const alloc = std.testing.allocator;
    var bucket_path = try bucketPathFromS3UriAlloc(alloc, "s3://serverless-bucket/manifests/prod");
    defer bucket_path.deinit(alloc);
    try std.testing.expectEqualStrings("serverless-bucket", bucket_path.bucket);
    try std.testing.expectEqualStrings("manifests/prod", bucket_path.prefix);
}

test "remote uri parse alloc returns tagged result" {
    const alloc = std.testing.allocator;
    var parsed = try parseAlloc(alloc, "gs://bucket/path");
    switch (parsed) {
        .file => |value| alloc.free(value),
        .gcs => |*value| value.deinit(alloc),
        .s3 => |*value| value.deinit(alloc),
    }
}

fn trimLeftSlash(value: []const u8) []const u8 {
    var idx: usize = 0;
    while (idx < value.len and value[idx] == '/') : (idx += 1) {}
    return value[idx..];
}
