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

//! Immutable remote-source identity checks with no transport or OS dependencies.
const std = @import("std");
pub const VersionKind = enum { strong_etag, s3_version_id, file_sha256 };
pub fn validateReference(uri: []const u8, version: []const u8, kind: VersionKind, length: u64) !std.Uri {
    if (length == 0 or version.len == 0 or version.len > 4096 or uri.len > 16 * 1024) return error.InvalidSourceReference;
    if (kind == .strong_etag) {
        if (version.len < 2 or version[0] != '"' or version[version.len - 1] != '"') return error.ImmutableVersionRequired;
        for (version[1 .. version.len - 1]) |byte| if (byte < 0x21 or byte == 0x22 or byte == 0x7f) return error.ImmutableVersionRequired;
    } else if (kind == .file_sha256) {
        if (version.len != 64) return error.ImmutableVersionRequired;
        for (version) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.ImmutableVersionRequired;
    } else if (std.mem.eql(u8, version, "null")) return error.ImmutableVersionRequired;
    const parsed = try std.Uri.parse(uri);
    if (parsed.user != null or parsed.password != null or parsed.fragment != null) return error.InvalidSourceReference;
    if (std.ascii.eqlIgnoreCase(parsed.scheme, "http") or std.ascii.eqlIgnoreCase(parsed.scheme, "https")) {
        if (kind != .strong_etag) return error.InvalidSourceReference;
        try validateHttpUri(parsed);
    } else if (std.ascii.eqlIgnoreCase(parsed.scheme, "s3")) {
        if (kind == .file_sha256) return error.InvalidSourceReference;
        try validateS3Uri(parsed, .{});
    } else if (std.ascii.eqlIgnoreCase(parsed.scheme, "file")) {
        if (kind != .file_sha256 or parsed.port != null or parsed.query != null or !std.mem.startsWith(u8, parsed.path.percent_encoded, "/")) return error.InvalidSourceReference;
        if (parsed.host) |host| if (host.percent_encoded.len != 0 and !std.ascii.eqlIgnoreCase(host.percent_encoded, "localhost")) return error.InvalidHost;
    } else return error.UnsupportedUrlScheme;
    return parsed;
}
pub fn validateHttpUri(uri: std.Uri) !void {
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "http") and !std.ascii.eqlIgnoreCase(uri.scheme, "https")) return error.UnsupportedUrlScheme;
    if (uri.host == null or uri.user != null or uri.password != null) return error.InvalidHost;
}
pub fn validateS3Uri(uri: std.Uri, options: struct { complete: bool = true }) !void {
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "s3")) return error.UnsupportedUrlScheme;
    const host = uri.host orelse return error.InvalidS3Url;
    if (host.percent_encoded.len == 0) return error.InvalidS3Url;
    if (options.complete and std.mem.trimStart(u8, uri.path.percent_encoded, "/").len == 0) return error.InvalidS3Url;
}

test "immutable source identities validate without transport or allocation" {
    _ = try validateReference("https://example.org/video.mp4", "\"v1\"", .strong_etag, 1024);
    _ = try validateReference("s3://bucket/video.mp4", "v1", .s3_version_id, 1024);
    try std.testing.expectError(error.ImmutableVersionRequired, validateReference("https://example.org/a", "W/\"v1\"", .strong_etag, 1));
    try std.testing.expectError(error.ImmutableVersionRequired, validateReference("s3://bucket/a", "null", .s3_version_id, 1));
    try std.testing.expectError(error.InvalidSourceReference, validateReference("https://user@example.org/a", "\"v1\"", .strong_etag, 1));
    try std.testing.expectError(error.InvalidSourceReference, validateReference("https://example.org/a#fragment", "\"v1\"", .strong_etag, 1));
    try std.testing.expectError(error.InvalidSourceReference, validateReference("https://example.org/a", "v1", .s3_version_id, 1));
    try std.testing.expectError(error.InvalidS3Url, validateReference("s3://bucket/", "v1", .s3_version_id, 1));
    try std.testing.expectError(error.InvalidSourceReference, validateReference("https://example.org/a", "\"v1\"", .strong_etag, 0));
}

test "local source identity requires SHA256 and a local absolute URI" {
    const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    _ = try validateReference("file:///video%20file.mp4", digest, .file_sha256, 10);
    _ = try validateReference("file://localhost/video.mp4", digest, .file_sha256, 10);
    try std.testing.expectError(error.InvalidHost, validateReference("file://remote/video.mp4", digest, .file_sha256, 10));
    for ([_][]const u8{ "file:relative.mp4", "file:///video.mp4?x=1", "file:///video.mp4#fragment", "https://example.org/video", "s3://bucket/video" }) |uri| {
        try std.testing.expectError(error.InvalidSourceReference, validateReference(uri, digest, .file_sha256, 10));
    }
    try std.testing.expectError(error.ImmutableVersionRequired, validateReference("file:///video", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .file_sha256, 10));
    try std.testing.expectError(error.ImmutableVersionRequired, validateReference("file:///video", "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG", .file_sha256, 10));
    try std.testing.expectError(error.InvalidSourceReference, validateReference("file:///video", "\"v1\"", .strong_etag, 10));
}
