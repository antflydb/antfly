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
