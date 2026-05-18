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

pub const Routes = struct {
    pub const raft_batch = "/raft/v1/batch";
    pub const snapshot_upload = "/raft/v1/snapshot/upload";
    pub const snapshot_fetch = "/raft/v1/snapshot/fetch";
    pub const health = "/raft/v1/health";

    pub fn join(alloc: std.mem.Allocator, base: []const u8, path: []const u8) ![]u8 {
        const total_len, const trimmed_base, const needs_separator = joinParts(base, path);
        const joined = try alloc.alloc(u8, total_len);
        errdefer alloc.free(joined);
        writeJoined(joined, trimmed_base, path, needs_separator);
        return joined;
    }

    pub fn joinInto(buf: []u8, base: []const u8, path: []const u8) error{NoSpace}![]u8 {
        const total_len, const trimmed_base, const needs_separator = joinParts(base, path);
        if (total_len > buf.len) return error.NoSpace;
        writeJoined(buf[0..total_len], trimmed_base, path, needs_separator);
        return buf[0..total_len];
    }

    fn joinParts(base: []const u8, path: []const u8) struct { usize, []const u8, bool } {
        const trimmed_base = if (std.mem.endsWith(u8, base, "/") and std.mem.startsWith(u8, path, "/"))
            base[0 .. base.len - 1]
        else
            base;
        const needs_separator = !std.mem.endsWith(u8, trimmed_base, "/") and !std.mem.startsWith(u8, path, "/");
        const total_len = trimmed_base.len + @intFromBool(needs_separator) + path.len;
        return .{ total_len, trimmed_base, needs_separator };
    }

    fn writeJoined(joined: []u8, trimmed_base: []const u8, path: []const u8, needs_separator: bool) void {
        @memcpy(joined[0..trimmed_base.len], trimmed_base);
        var offset = trimmed_base.len;
        if (needs_separator) {
            joined[offset] = '/';
            offset += 1;
        }
        @memcpy(joined[offset .. offset + path.len], path);
    }

    fn appendSuffix(alloc: std.mem.Allocator, prefix: []const u8, suffix: []const u8) ![]u8 {
        const out = try alloc.alloc(u8, prefix.len + 1 + suffix.len);
        errdefer alloc.free(out);
        @memcpy(out[0..prefix.len], prefix);
        out[prefix.len] = '/';
        @memcpy(out[prefix.len + 1 ..], suffix);
        return out;
    }

    pub fn snapshotUploadPath(alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        return try appendSuffix(alloc, snapshot_upload, snapshot_id);
    }

    pub fn snapshotFetchPath(alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        return try appendSuffix(alloc, snapshot_fetch, snapshot_id);
    }

    pub fn matchSnapshotUpload(path: []const u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, path, snapshot_upload)) return null;
        const suffix = path[snapshot_upload.len..];
        if (suffix.len < 2 or suffix[0] != '/') return null;
        return suffix[1..];
    }

    pub fn matchSnapshotFetch(path: []const u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, path, snapshot_fetch)) return null;
        const suffix = path[snapshot_fetch.len..];
        if (suffix.len < 2 or suffix[0] != '/') return null;
        return suffix[1..];
    }
};

test "transport routes compile" {
    try std.testing.expect(Routes.raft_batch.len > 0);
}

test "transport routes join and parse snapshot paths" {
    const upload = try Routes.snapshotUploadPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(upload);
    try std.testing.expectEqualStrings("/raft/v1/snapshot/upload/snap-1", upload);
    try std.testing.expectEqualStrings("snap-1", Routes.matchSnapshotUpload(upload).?);

    const fetch = try Routes.snapshotFetchPath(std.testing.allocator, "snap-2");
    defer std.testing.allocator.free(fetch);
    try std.testing.expectEqualStrings("snap-2", Routes.matchSnapshotFetch(fetch).?);
}
