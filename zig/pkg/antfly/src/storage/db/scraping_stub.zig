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
    _ = uri;
    return 0;
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
