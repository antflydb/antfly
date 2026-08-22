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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const sha256_checksum_len: usize = std.crypto.hash.sha2.Sha256.digest_length * 2;
pub const sha256_artifact_id_prefix = "sha256:";

/// Returns the checksum portion of a canonical content-addressed artifact ID.
/// Artifact stores use this before any filesystem or object-store access so a
/// malformed ID cannot select an arbitrary cache key or silently weaken
/// payload verification.
pub fn sha256ChecksumFromArtifactId(artifact_id: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, artifact_id, sha256_artifact_id_prefix)) {
        return error.InvalidArtifactId;
    }
    const checksum = artifact_id[sha256_artifact_id_prefix.len..];
    try validateSha256Checksum(checksum);
    return checksum;
}

pub fn validateSha256Checksum(checksum: []const u8) !void {
    if (checksum.len != sha256_checksum_len) return error.InvalidArtifactId;
    for (checksum) |byte| {
        if (!isLowerHex(byte)) return error.InvalidArtifactId;
    }
}

pub fn validateSha256ArtifactIdentity(artifact_id: []const u8, checksum: []const u8) !void {
    try validateSha256Checksum(checksum);
    const id_checksum = try sha256ChecksumFromArtifactId(artifact_id);
    if (!std.mem.eql(u8, id_checksum, checksum)) return error.InvalidArtifactId;
}

fn isLowerHex(byte: u8) bool {
    return (byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f');
}

pub const ArtifactMetadata = struct {
    artifact_id: []u8,
    byte_len: u64,
    checksum: []u8,

    pub fn deinit(self: *ArtifactMetadata, alloc: Allocator) void {
        alloc.free(self.artifact_id);
        alloc.free(self.checksum);
        self.* = undefined;
    }
};

pub const ArtifactStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        put: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        get_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror![]u8,
        get_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror![]u8 = null,
        get_range_alloc: *const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror![]u8,
        get_range_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, u64, usize, CancellationToken) anyerror![]u8 = null,
        stat: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        stat_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
    };

    pub fn deinit(self: *ArtifactStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn put(self: *ArtifactStore, contents: []const u8) !ArtifactMetadata {
        return try self.vtable.put(self.ptr, self.allocator, contents);
    }

    pub fn getAlloc(self: *ArtifactStore, artifact_id: []const u8) ![]u8 {
        return try self.getAllocWithCancellation(artifact_id, .none);
    }

    pub fn getAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const payload = if (self.vtable.get_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, self.allocator, artifact_id, cancellation)
        else
            try self.vtable.get_alloc(self.ptr, self.allocator, artifact_id);
        errdefer self.allocator.free(payload);
        try cancellation.check();
        return payload;
    }

    pub fn getRangeAlloc(self: *ArtifactStore, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        return try self.getRangeAllocWithCancellation(artifact_id, offset, len, .none);
    }

    pub fn getRangeAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const payload = if (self.vtable.get_range_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, self.allocator, artifact_id, offset, len, cancellation)
        else
            try self.vtable.get_range_alloc(self.ptr, self.allocator, artifact_id, offset, len);
        errdefer self.allocator.free(payload);
        try cancellation.check();
        return payload;
    }

    pub fn stat(self: *ArtifactStore, artifact_id: []const u8) !ArtifactMetadata {
        return try self.statWithCancellation(artifact_id, .none);
    }

    pub fn statWithCancellation(self: *ArtifactStore, artifact_id: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        try cancellation.check();
        var metadata = if (self.vtable.stat_with_cancellation) |stat_with_cancellation|
            try stat_with_cancellation(self.ptr, self.allocator, artifact_id, cancellation)
        else
            try self.vtable.stat(self.ptr, self.allocator, artifact_id);
        errdefer metadata.deinit(self.allocator);
        try cancellation.check();
        return metadata;
    }

    pub fn delete(self: *ArtifactStore, artifact_id: []const u8) !void {
        try self.vtable.delete(self.ptr, artifact_id);
    }
};

test "artifact identities require canonical matching sha256 values" {
    const checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const artifact_id = sha256_artifact_id_prefix ++ checksum;
    try validateSha256ArtifactIdentity(artifact_id, checksum);
    try std.testing.expectEqualStrings(checksum, try sha256ChecksumFromArtifactId(artifact_id));

    try std.testing.expectError(error.InvalidArtifactId, sha256ChecksumFromArtifactId("sha256:abcd"));
    try std.testing.expectError(
        error.InvalidArtifactId,
        sha256ChecksumFromArtifactId("sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"),
    );
    try std.testing.expectError(
        error.InvalidArtifactId,
        validateSha256ArtifactIdentity(
            artifact_id,
            "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
    );
}
