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

test "serverless scoped artifact identities validate every routing component" {
    const a = std.testing.allocator;
    const scope: UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const checksum = "a" ** 64;
    const id = try scope.artifactId(checksum);
    try std.testing.expectEqualStrings(checksum, try sha256ChecksumFromArtifactId(&id));
    try std.testing.expectEqual(scope, (try uploadScopeFromArtifactId(&id)).?);
    const suffix = try storageSuffixAlloc(a, &id);
    defer a.free(suffix);
    try std.testing.expectEqualStrings("graph/" ++ "01" ** 32 ++ "/" ++ "02" ** 16 ++ "/" ++ checksum, suffix);
    for ([_]usize{ 0, 7, 71, 78, 142, 143, 174 }) |offset| {
        var corrupt = id;
        corrupt[offset] = '/';
        try std.testing.expectError(error.InvalidArtifactId, sha256ChecksumFromArtifactId(&corrupt));
        try std.testing.expectError(error.InvalidArtifactId, storageSuffixAlloc(a, &corrupt));
    }
    try std.testing.expectError(error.InvalidArtifactUploadScope, (UploadScope{ .domain = @splat(0), .attempt = scope.attempt }).artifactId(checksum));
    try std.testing.expectError(error.InvalidArtifactUploadScope, (UploadScope{ .domain = scope.domain, .attempt = @splat(0) }).artifactId(checksum));
}

pub fn chargeReadBudget(remaining: *u64, amount: u64) !void {
    if (amount > remaining.*) return error.ArtifactReadBudgetExceeded;
    remaining.* -= amount;
}
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const sha256_checksum_len: usize = std.crypto.hash.sha2.Sha256.digest_length * 2;
pub const sha256_artifact_id_prefix = "sha256:";

/// A publication attempt owns newly uploaded pages, while immutable references
/// may reuse pages belonging to older attempts. Attempt identities must never
/// be reused after loss of publication authority. Scoped object names are also
/// the durable inventory: late uploads remain discoverable after a failed build.
pub const UploadScope = struct {
    domain: [32]u8,
    attempt: [16]u8,

    pub fn validate(self: UploadScope) !void {
        if (std.mem.allEqual(u8, &self.domain, 0) or self.fencingToken() == 0 or std.mem.allEqual(u8, self.attempt[8..16], 0))
            return error.InvalidArtifactUploadScope;
    }

    pub fn fencingToken(self: UploadScope) u64 {
        return std.mem.readInt(u64, self.attempt[0..8], .big);
    }

    pub fn forPublication(domain: [32]u8, token: u64, io: std.Io) !UploadScope {
        var scope = UploadScope{ .domain = domain, .attempt = undefined };
        std.mem.writeInt(u64, scope.attempt[0..8], token, .big);
        while (true) {
            io.random(scope.attempt[8..16]);
            if (!std.mem.allEqual(u8, scope.attempt[8..16], 0)) break;
        }
        try scope.validate();
        return scope;
    }

    pub fn artifactId(self: UploadScope, checksum: []const u8) ![175]u8 {
        try self.validate();
        try validateSha256Checksum(checksum);
        var id: [175]u8 = undefined;
        @memcpy(id[0..7], "sha256:");
        @memcpy(id[7..71], checksum);
        @memcpy(id[71..78], ":graph:");
        @memcpy(id[78..142], &std.fmt.bytesToHex(&self.domain, .lower));
        id[142] = ':';
        @memcpy(id[143..175], &std.fmt.bytesToHex(&self.attempt, .lower));
        return id;
    }
};

pub fn uploadScopeFromArtifactId(id: []const u8) !?UploadScope {
    if ((id.len != 71 and id.len != 175) or !std.mem.startsWith(u8, id, "sha256:")) return error.InvalidArtifactId;
    try validateSha256Checksum(id[7..71]);
    if (id.len == 71) return null;
    if (id.len != 175 or !std.mem.eql(u8, id[71..78], ":graph:") or id[142] != ':') return error.InvalidArtifactId;
    try validateSha256Checksum(id[78..142]);
    for (id[143..175]) |byte| if (!isLowerHex(byte)) return error.InvalidArtifactId;
    var scope: UploadScope = undefined;
    _ = std.fmt.hexToBytes(&scope.domain, id[78..142]) catch return error.InvalidArtifactId;
    _ = std.fmt.hexToBytes(&scope.attempt, id[143..175]) catch return error.InvalidArtifactId;
    scope.validate() catch return error.InvalidArtifactId;
    return scope;
}

/// Validates the complete identity before deriving a filesystem/object key.
pub fn storageSuffixAlloc(alloc: Allocator, id: []const u8) ![]u8 {
    const checksum = try sha256ChecksumFromArtifactId(id);
    if (try uploadScopeFromArtifactId(id)) |_| return std.fmt.allocPrint(alloc, "graph/{s}/{s}/{s}", .{ id[78..142], id[143..175], checksum });
    return std.fmt.allocPrint(alloc, "sha256/{s}/{s}", .{ checksum[0..2], checksum[2..] });
}

pub const ScopedUploadVisitor = struct {
    ptr: *anyopaque,
    /// ID is borrowed only for this call. Enumeration is namespace-local and
    /// bounded; visitors must not assume a snapshot of concurrent late uploads.
    visit: *const fn (*anyopaque, UploadScope, []const u8) anyerror!void,
};

/// Parse only canonical objects below graph/<domain>/. Temporary local files
/// are not artifact identities and are deliberately excluded from this API.
pub fn visitScopedSuffix(domain: [32]u8, suffix: []const u8, visitor: ScopedUploadVisitor) !void {
    if (suffix.len != 97 or suffix[32] != '/') return;
    for (suffix[0..32]) |byte| if (!isLowerHex(byte)) return error.InvalidArtifactId;
    try validateSha256Checksum(suffix[33..97]);
    var scope: UploadScope = .{ .domain = domain, .attempt = undefined };
    _ = std.fmt.hexToBytes(&scope.attempt, suffix[0..32]) catch return error.InvalidArtifactId;
    const id = try scope.artifactId(suffix[33..97]);
    try visitor.visit(visitor.ptr, scope, &id);
}

/// Returns the checksum portion of a canonical content-addressed artifact ID.
/// Artifact stores use this before any filesystem or object-store access so a
/// malformed ID cannot select an arbitrary cache key or silently weaken
/// payload verification.
pub fn sha256ChecksumFromArtifactId(artifact_id: []const u8) ![]const u8 {
    _ = try uploadScopeFromArtifactId(artifact_id);
    return artifact_id[sha256_artifact_id_prefix.len..71];
}

pub fn validateSha256Checksum(checksum: []const u8) !void {
    if (checksum.len != sha256_checksum_len) return error.InvalidArtifactId;
    for (checksum) |byte| {
        if (!isLowerHex(byte)) return error.InvalidArtifactId;
    }
}

pub fn sha256DigestFromChecksum(checksum: []const u8) ![std.crypto.hash.sha2.Sha256.digest_length]u8 {
    try validateSha256Checksum(checksum);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    _ = std.fmt.hexToBytes(&digest, checksum) catch return error.InvalidArtifactId;
    return digest;
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
    /// A borrowed publication-local capability. All newly created artifacts,
    /// including flat/search segments, belong to this fenced attempt. Reused
    /// immutable references keep their original identities.
    upload_scope: ?UploadScope = null,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        put: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        put_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        get_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror![]u8,
        get_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror![]u8 = null,
        get_range_alloc: *const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror![]u8,
        get_range_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, u64, usize, CancellationToken) anyerror![]u8 = null,
        get_verified_range_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, u64, usize, CancellationToken) anyerror![]u8 = null,
        get_verified_range_alloc_with_budget: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, u64, usize, CancellationToken, *u64) anyerror![]u8 = null,
        stat: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        stat_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        verify_content: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, CancellationToken) anyerror!void = null,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        put_scoped: ?*const fn (*anyopaque, Allocator, UploadScope, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        visit_scoped_uploads: ?*const fn (*anyopaque, [32]u8, ScopedUploadVisitor, CancellationToken) anyerror!void = null,
        cleanup_retired_scoped_temporaries: ?*const fn (*anyopaque, [32]u8, u64, CancellationToken) anyerror!void = null,
    };

    pub fn deinit(self: *ArtifactStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn put(self: *ArtifactStore, contents: []const u8) !ArtifactMetadata {
        return try self.putWithCancellation(contents, .none);
    }

    pub fn putScoped(self: *ArtifactStore, scope: UploadScope, contents: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        try scope.validate();
        if (self.upload_scope) |authority| {
            if (!std.mem.eql(u8, &scope.domain, &authority.domain) or scope.fencingToken() != authority.fencingToken())
                return error.InvalidArtifactUploadScope;
        }
        try cancellation.check();
        const put_scoped = self.vtable.put_scoped orelse return error.ArtifactUploadScopesUnsupported;
        var metadata = try put_scoped(self.ptr, self.allocator, scope, contents, cancellation);
        errdefer metadata.deinit(self.allocator);
        const expected = try scope.artifactId(metadata.checksum);
        if (metadata.byte_len != contents.len or !std.mem.eql(u8, &expected, metadata.artifact_id)) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return metadata;
    }

    pub fn visitScopedUploads(self: *ArtifactStore, domain: [32]u8, visitor: ScopedUploadVisitor, cancellation: CancellationToken) !void {
        if (std.mem.allEqual(u8, &domain, 0)) return error.InvalidArtifactUploadScope;
        try cancellation.check();
        const visit = self.vtable.visit_scoped_uploads orelse return error.ArtifactUploadScopesUnsupported;
        try visit(self.ptr, domain, visitor, cancellation);
    }

    /// Only a collector that has fenced all older publications may call this.
    /// Backends without local staging files need no extra cleanup operation.
    pub fn cleanupRetiredScopedTemporaries(self: *ArtifactStore, domain: [32]u8, cutoff: u64, cancellation: CancellationToken) !void {
        if (cutoff == 0 or std.mem.allEqual(u8, &domain, 0)) return error.InvalidArtifactUploadScope;
        try cancellation.check();
        if (self.vtable.cleanup_retired_scoped_temporaries) |cleanup| try cleanup(self.ptr, domain, cutoff, cancellation);
    }

    pub fn putWithCancellation(self: *ArtifactStore, contents: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        if (self.upload_scope) |scope| return self.putScoped(scope, contents, cancellation);
        try cancellation.check();
        var metadata = if (self.vtable.put_with_cancellation) |put_with_cancellation|
            try put_with_cancellation(self.ptr, self.allocator, contents, cancellation)
        else
            try self.vtable.put(self.ptr, self.allocator, contents);
        errdefer metadata.deinit(self.allocator);
        try cancellation.check();
        return metadata;
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

    /// Loads a range from the exact object or file identity authenticated by
    /// `expected_checksum`. Native backends pin the provider generation, ETag,
    /// or open file identity across verification and reading. The fallback is
    /// retained for test and custom stores, but production stores should
    /// implement the vtable entry so replacement cannot race a range read.
    pub fn getVerifiedRangeAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch
            return error.ArtifactIntegrityMismatch;
        const end = std.math.add(u64, offset, std.math.cast(u64, len) orelse return error.InvalidRange) catch
            return error.InvalidRange;
        if (end > expected_byte_len) return error.InvalidRange;
        if (len == 0) {
            try self.verifyContentWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                cancellation,
            );
            const empty = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(empty);
            try cancellation.check();
            return empty;
        }

        const payload = if (self.vtable.get_verified_range_alloc_with_cancellation) |get_verified_range|
            try get_verified_range(
                self.ptr,
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                offset,
                len,
                cancellation,
            )
        else blk: {
            try self.verifyContentWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                cancellation,
            );
            break :blk try self.getRangeAllocWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                offset,
                len,
                cancellation,
            );
        };
        errdefer result_alloc.free(payload);
        if (payload.len != len) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return payload;
    }

    /// A shared read allowance covers both the requested range and any cold
    /// full-content authentication. Native backends charge only cache misses.
    pub fn getVerifiedRangeAllocWithBudget(
        self: *ArtifactStore,
        alloc: Allocator,
        artifact_id: []const u8,
        byte_len: u64,
        checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
        remaining: *u64,
    ) ![]u8 {
        try cancellation.check();
        try validateSha256ArtifactIdentity(artifact_id, checksum);
        const end = std.math.add(u64, offset, len) catch return error.InvalidRange;
        if (end > byte_len) return error.InvalidRange;
        try chargeReadBudget(remaining, len);
        const bytes = if (self.vtable.get_verified_range_alloc_with_budget) |read|
            try read(self.ptr, alloc, artifact_id, byte_len, checksum, offset, len, cancellation, remaining)
        else blk: {
            try chargeReadBudget(remaining, byte_len);
            break :blk try self.getVerifiedRangeAllocWithCancellationUsingAllocator(alloc, artifact_id, byte_len, checksum, offset, len, cancellation);
        };
        errdefer alloc.free(bytes);
        if (bytes.len != len) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return bytes;
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

    /// Verifies a content-addressed artifact with bounded memory. Backends may
    /// provide a native streaming verifier; the portable fallback hashes
    /// bounded ranges and therefore never allocates the full artifact.
    pub fn verifyContentWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch return error.ArtifactIntegrityMismatch;

        // A backend verifier owns the complete identity, length, and content
        // check. Calling stat here as well would duplicate provider HEADs on
        // every verification and defeat backend identity caches.
        if (self.vtable.verify_content) |verify_content| {
            try verify_content(self.ptr, result_alloc, artifact_id, expected_byte_len, expected_checksum, cancellation);
            return;
        }

        // Portable backends without a native verifier first validate the
        // declared metadata, then hash bounded ranges below.
        var metadata = try self.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
        defer metadata.deinit(result_alloc);
        if (metadata.byte_len != expected_byte_len or
            !std.mem.eql(u8, metadata.artifact_id, artifact_id) or
            !std.mem.eql(u8, metadata.checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        const chunk_bytes: usize = 8 * 1024 * 1024;
        var offset: u64 = 0;
        while (offset < expected_byte_len) {
            try cancellation.check();
            const remaining = expected_byte_len - offset;
            const len: usize = @intCast(@min(remaining, chunk_bytes));
            const chunk = try self.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
            defer result_alloc.free(chunk);
            if (chunk.len != len) return error.ArtifactIntegrityMismatch;
            hasher.update(chunk);
            offset += len;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
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
