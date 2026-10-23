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

pub const IntegrationBackend = enum {
    s3,
    gs,

    pub fn scheme(self: IntegrationBackend) []const u8 {
        return switch (self) {
            .s3 => "s3",
            .gs => "gs",
        };
    }

    pub fn enableEnv(self: IntegrationBackend) []const u8 {
        return switch (self) {
            .s3 => "OBJECTSTORE_S3_INTEGRATION",
            .gs => "OBJECTSTORE_GCS_INTEGRATION",
        };
    }

    pub fn bucketEnv(self: IntegrationBackend) []const u8 {
        return switch (self) {
            .s3 => "OBJECTSTORE_S3_TEST_BUCKET",
            .gs => "OBJECTSTORE_GCS_TEST_BUCKET",
        };
    }
};

pub fn requireEnabled(comptime backend: IntegrationBackend) !void {
    const env_name = backend.enableEnv();
    if (!envEnabled(env_name)) {
        std.debug.print("skipping serverless {s} integration test: set {s}=1 to enable\n", .{ backend.scheme(), env_name });
        return error.SkipZigTest;
    }
}

pub fn requiredBucketOwned(alloc: std.mem.Allocator, comptime backend: IntegrationBackend) ![]u8 {
    const env_name = backend.bucketEnv();
    const env_name_z = try alloc.dupeSentinel(u8, env_name, 0);
    defer alloc.free(env_name_z);
    const value_z = std.c.getenv(env_name_z.ptr) orelse {
        std.debug.print("skipping serverless {s} integration test: missing env {s}\n", .{ backend.scheme(), env_name });
        return error.SkipZigTest;
    };
    return try alloc.dupe(u8, std.mem.span(value_z));
}

pub fn makeNamespaceUris(
    alloc: std.mem.Allocator,
    comptime backend: IntegrationBackend,
    bucket: []const u8,
    prefix_root: []const u8,
) !NamespaceUris {
    return .{
        .artifacts = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/artifacts", .{ backend.scheme(), bucket, prefix_root }),
        .manifests = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/manifests", .{ backend.scheme(), bucket, prefix_root }),
        .wal = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/wal", .{ backend.scheme(), bucket, prefix_root }),
        .progress = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/progress", .{ backend.scheme(), bucket, prefix_root }),
        .catalog = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/catalog", .{ backend.scheme(), bucket, prefix_root }),
    };
}

pub const NamespaceUris = struct {
    artifacts: []u8,
    manifests: []u8,
    wal: []u8,
    progress: []u8,
    catalog: []u8,

    pub fn deinit(self: *NamespaceUris, alloc: std.mem.Allocator) void {
        alloc.free(self.artifacts);
        alloc.free(self.manifests);
        alloc.free(self.wal);
        alloc.free(self.progress);
        alloc.free(self.catalog);
        self.* = undefined;
    }
};

pub fn integrationNonce() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn envEnabled(env_name: []const u8) bool {
    const env_name_z = std.heap.page_allocator.dupeSentinel(u8, env_name, 0) catch return false;
    defer std.heap.page_allocator.free(env_name_z);
    const value_z = std.c.getenv(env_name_z.ptr) orelse return false;
    const value = std.mem.span(value_z);
    return value.len > 0 and !std.mem.eql(u8, value, "0") and !std.mem.eql(u8, value, "false");
}
