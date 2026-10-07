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
const Allocator = std.mem.Allocator;
const objectstore = @import("objectstore");
const aws = @import("antfly_credentials").aws;

pub const AwsCredentialContext = struct {
    alloc: Allocator,
    http: @import("httpx").Client,
    cache: aws.CredentialCache = .{},
    region: []u8,
    source: aws.CredentialSource,

    pub fn init(alloc: Allocator, region: []const u8, source: aws.CredentialSource, io: std.Io) !AwsCredentialContext {
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
                .release = aws.CredentialCache.Lease.releaseOpaque,
            } },
        };
    }
};
