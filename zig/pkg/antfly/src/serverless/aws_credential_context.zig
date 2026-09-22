// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const Allocator = std.mem.Allocator;
const objectstore = @import("objectstore");
const bedrock = @import("../inference/bedrock.zig");

pub const AwsCredentialContext = struct {
    alloc: Allocator,
    http: @import("httpx").Client,
    cache: bedrock.CredentialCache = .{},
    region: []u8,
    source: bedrock.CredentialSource,

    pub fn init(alloc: Allocator, region: []const u8, source: bedrock.CredentialSource, io: std.Io) !AwsCredentialContext {
        const owned_region = try alloc.dupe(u8, region);
        return .{
            .alloc = alloc,
            .http = @import("httpx").Client.init(alloc, io),
            .region = owned_region,
            .source = source,
        };
    }

    pub fn deinit(self: *AwsCredentialContext) void {
        self.cache.deinit(self.alloc);
        self.http.deinit();
        self.alloc.free(self.region);
        self.* = undefined;
    }

    pub fn provider(self: *AwsCredentialContext) objectstore.S3.CredentialProvider {
        return .{ .ptr = self, .get_fn = get };
    }

    fn get(ptr: *anyopaque, alloc: Allocator) anyerror!objectstore.S3.DynamicCredentials {
        const self: *AwsCredentialContext = @ptrCast(@alignCast(ptr));
        _ = alloc;
        const lease = try self.cache.getLeaseForSource(self.alloc, &self.http, self.region, self.source);
        const credentials = lease.credentials();
        return .{
            .access_key_id = @constCast(credentials.access_key_id),
            .secret_access_key = @constCast(credentials.secret_access_key),
            .session_token = if (credentials.session_token) |value| @constCast(value) else null,
            .ownership = .{ .borrowed = .{
                .ctx = lease.releaseContext(),
                .release = bedrock.CredentialCache.Lease.releaseOpaque,
            } },
        };
    }
};
