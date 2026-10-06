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
pub const data_uri = @import("antfly_data_uri");

pub const DownloadedContent = struct {
    content_type: []u8,
    data: []u8,

    pub fn deinit(self: *DownloadedContent, alloc: std.mem.Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.data);
        self.* = undefined;
    }
};

pub const HttpError = struct {
    status: u16,
    message: []const u8,
};

pub const DownloadOutcome = union(enum) {
    ok: DownloadedContent,
    http_error: HttpError,
};

pub const ContentSecurityConfig = struct {
    allowed_hosts: ?[]const []u8 = null,
    block_private_ips: ?bool = null,
    nat64_prefixes: ?[]const []u8 = null,
    max_download_size_bytes: ?u64 = null,
    download_timeout_seconds: ?u32 = null,
    max_image_dimension: ?u32 = null,
    allowed_paths: ?[]const []u8 = null,
    user_agent: ?[]u8 = null,
};

pub const RemoteContentConfig = struct {
    security: ?ContentSecurityConfig = null,
    default_s3: ?[]u8 = null,
    s3: EmptyCredentialMap = .{},
    http: EmptyCredentialMap = .{},
};

pub const EmptyCredentialMap = struct {
    pub fn count(_: EmptyCredentialMap) usize {
        return 0;
    }
};

pub fn dataUriDecodedSize(uri: []const u8) !usize {
    return (try data_uri.parseRequired(uri)).decodedSize();
}

pub fn downloadContentOutcomeAllocWithHeaders(
    alloc: std.mem.Allocator,
    uri: []const u8,
    security: *const ContentSecurityConfig,
    headers: ?[]const u8,
    content_type_hint: ?[]const u8,
) anyerror!DownloadOutcome {
    _ = alloc;
    _ = uri;
    _ = security;
    _ = headers;
    _ = content_type_hint;
    return error.UnsupportedPlatform;
}
