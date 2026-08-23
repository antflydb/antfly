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
        return try self.getAllocWithCancellationUsingAllocator(self.allocator, artifact_id, cancellation);
    }

    pub fn getAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const payload = if (self.vtable.get_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, result_alloc, artifact_id, cancellation)
        else
            try self.vtable.get_alloc(self.ptr, result_alloc, artifact_id);
        errdefer result_alloc.free(payload);
        try cancellation.check();
        return payload;
    }

    /// Loads one content-addressed artifact without trusting either the backing
    /// store or its metadata to honor the manifest contract. The declared size
    /// bounds the transport allocation and the digest is checked before bytes
    /// can reach a decoder or cache.
    pub fn getVerifiedAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getVerifiedAllocWithCancellationUsingAllocator(
            self.allocator,
            artifact_id,
            expected_byte_len,
            expected_checksum,
            cancellation,
        );
    }

    pub fn getVerifiedAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch
            return error.ArtifactIntegrityMismatch;
        const expected_len = std.math.cast(usize, expected_byte_len) orelse
            return error.ArtifactTooLarge;

        {
            var metadata = try self.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
            defer metadata.deinit(result_alloc);
            if (!std.mem.eql(u8, metadata.artifact_id, artifact_id) or
                metadata.byte_len != expected_byte_len or
                !std.mem.eql(u8, metadata.checksum, expected_checksum))
            {
                return error.ArtifactIntegrityMismatch;
            }
        }

        if (expected_len == 0) {
            const payload = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(payload);
            try validatePayloadSha256WithCancellation(payload, expected_checksum, cancellation);
            return payload;
        }

        const payload = try self.getRangeAllocWithCancellationUsingAllocator(
            result_alloc,
            artifact_id,
            0,
            expected_len,
            cancellation,
        );
        errdefer result_alloc.free(payload);
        if (payload.len != expected_len) return error.ArtifactIntegrityMismatch;
        try validatePayloadSha256WithCancellation(payload, expected_checksum, cancellation);
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
        return try self.getRangeAllocWithCancellationUsingAllocator(self.allocator, artifact_id, offset, len, cancellation);
    }

    pub fn getRangeAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        if (len == 0) {
            _ = try sha256ChecksumFromArtifactId(artifact_id);
            var metadata = try self.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
            defer metadata.deinit(result_alloc);
            if (offset > metadata.byte_len) return error.InvalidRange;
            const payload = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(payload);
            try cancellation.check();
            return payload;
        }
        const payload = if (self.vtable.get_range_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, result_alloc, artifact_id, offset, len, cancellation)
        else
            try self.vtable.get_range_alloc(self.ptr, result_alloc, artifact_id, offset, len);
        errdefer result_alloc.free(payload);
        try cancellation.check();
        return payload;
    }

    pub fn stat(self: *ArtifactStore, artifact_id: []const u8) !ArtifactMetadata {
        return try self.statWithCancellation(artifact_id, .none);
    }

    pub fn statWithCancellation(self: *ArtifactStore, artifact_id: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        return try self.statWithCancellationUsingAllocator(self.allocator, artifact_id, cancellation);
    }

    pub fn statWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) !ArtifactMetadata {
        try cancellation.check();
        var metadata = if (self.vtable.stat_with_cancellation) |stat_with_cancellation|
            try stat_with_cancellation(self.ptr, result_alloc, artifact_id, cancellation)
        else
            try self.vtable.stat(self.ptr, result_alloc, artifact_id);
        errdefer metadata.deinit(result_alloc);
        try cancellation.check();
        return metadata;
    }

    pub fn delete(self: *ArtifactStore, artifact_id: []const u8) !void {
        try self.vtable.delete(self.ptr, artifact_id);
    }
};

pub fn validatePayloadSha256WithCancellation(
    payload: []const u8,
    expected_checksum: []const u8,
    cancellation: CancellationToken,
) !void {
    validateSha256Checksum(expected_checksum) catch return error.ArtifactIntegrityMismatch;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    const cancellation_chunk_bytes = 1024 * 1024;
    var offset: usize = 0;
    while (offset < payload.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, payload.len - offset);
        hasher.update(payload[offset..][0..chunk_len]);
        offset += chunk_len;
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
    try cancellation.check();
}

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

test "serverless verified empty artifacts avoid invalid cloud ranges" {
    const alloc = std.testing.allocator;
    const empty_checksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const empty_artifact_id = sha256_artifact_id_prefix ++ empty_checksum;

    const State = struct {
        range_calls: usize = 0,

        fn deinit(_: Allocator, _: *anyopaque) void {}

        fn put(_: *anyopaque, _: Allocator, _: []const u8) anyerror!ArtifactMetadata {
            return error.UnexpectedCall;
        }

        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) anyerror![]u8 {
            return error.UnexpectedCall;
        }

        fn getRangeAlloc(
            raw: *anyopaque,
            _: Allocator,
            _: []const u8,
            _: u64,
            _: usize,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.range_calls += 1;
            return error.UnexpectedRangeRequest;
        }

        fn stat(raw: *anyopaque, result_alloc: Allocator, artifact_id: []const u8) anyerror!ArtifactMetadata {
            _ = raw;
            if (!std.mem.eql(u8, artifact_id, empty_artifact_id)) return error.FileNotFound;
            const artifact_id_copy = try result_alloc.dupe(u8, empty_artifact_id);
            errdefer result_alloc.free(artifact_id_copy);
            return .{
                .artifact_id = artifact_id_copy,
                .byte_len = 0,
                .checksum = try result_alloc.dupe(u8, empty_checksum),
            };
        }

        fn delete(_: *anyopaque, _: []const u8) anyerror!void {
            return error.UnexpectedCall;
        }
    };

    const vtable = ArtifactStore.VTable{
        .deinit = State.deinit,
        .put = State.put,
        .get_alloc = State.getAlloc,
        .get_range_alloc = State.getRangeAlloc,
        .stat = State.stat,
        .delete = State.delete,
    };
    var state = State{};
    var store = ArtifactStore{
        .allocator = alloc,
        .ptr = &state,
        .vtable = &vtable,
    };

    const payload = try store.getVerifiedAllocWithCancellation(
        empty_artifact_id,
        0,
        empty_checksum,
        .none,
    );
    defer alloc.free(payload);
    try std.testing.expectEqual(@as(usize, 0), payload.len);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);

    const empty_range = try store.getRangeAllocWithCancellationUsingAllocator(
        alloc,
        empty_artifact_id,
        0,
        0,
        .none,
    );
    defer alloc.free(empty_range);
    try std.testing.expectEqual(@as(usize, 0), empty_range.len);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);
    try std.testing.expectError(
        error.InvalidArtifactId,
        store.getRangeAllocWithCancellationUsingAllocator(alloc, "not-an-artifact", 0, 0, .none),
    );
    try std.testing.expectError(
        error.InvalidRange,
        store.getRangeAllocWithCancellationUsingAllocator(alloc, empty_artifact_id, 1, 0, .none),
    );
    try std.testing.expectError(
        error.FileNotFound,
        store.getRangeAllocWithCancellationUsingAllocator(
            alloc,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            0,
            0,
            .none,
        ),
    );
}
