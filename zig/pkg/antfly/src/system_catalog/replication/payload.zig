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

//! Immutable CDC intent custody references. The source/P journals authenticate
//! these descriptors; provider certificates remain dynamically verified facts.
const std = @import("std");
const artifacts = @import("../../serverless/artifacts/store.zig");
pub const inline_bytes = 256 * 1024;
pub const chunk_bytes = 4 * 1024 * 1024;
pub const max_manifest_bytes = 16 * 1024 * 1024;
pub const Digest = [32]u8;
/// Source birth and upload token are already durable before any immutable
/// publication. Token ownership is independent of the transaction Head;
/// canceled/expired attempts remain visible to native custody inventory.
pub fn uploadDomain(source: anytype, metadata_group: u64, metadata_incarnation: [32]u8) !Digest {
    try source.validate();
    if (metadata_group == 0 or std.mem.allEqual(u8, &metadata_incarnation, 0)) return error.InvalidArtifactUploadScope;
    var hashing = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).initHasher(std.crypto.hash.sha2.Sha256.init(.{}), &.{});
    try hashing.writer.writeAll("antfly:replication-private-payload-domain:v2\x00");
    var group_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &group_bytes, metadata_group, .little);
    try hashing.writer.writeAll(&group_bytes);
    try hashing.writer.writeAll(&metadata_incarnation);
    try std.json.Stringify.value(source, .{}, &hashing.writer);
    var domain: [32]u8 = undefined;
    hashing.hasher.final(&domain);
    return domain;
}

pub fn uploadScope(source: anytype, metadata_group: u64, metadata_incarnation: [32]u8, upload_token: u64, txn_id: [16]u8) !artifacts.UploadScope {
    try source.validate();
    if (metadata_group == 0 or upload_token == 0 or std.mem.allEqual(u8, &metadata_incarnation, 0) or std.mem.allEqual(u8, &txn_id, 0)) return error.InvalidArtifactUploadScope;
    const domain = try uploadDomain(source, metadata_group, metadata_incarnation);
    var nonce: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&txn_id, &nonce, .{});
    var attempt: [16]u8 = undefined;
    std.mem.writeInt(u64, attempt[0..8], upload_token, .big);
    @memcpy(attempt[8..16], nonce[0..8]);
    const scope: artifacts.UploadScope = .{ .domain = domain, .attempt = attempt };
    try scope.validate();
    return scope;
}

pub const Retained = @import("antfly_local_sources").storage_artifact_identity.Retained;

pub const Payload = union(enum) {
    inline_json: []const u8,
    retained: Retained,

    pub fn byteLength(self: Payload) u64 {
        return switch (self) {
            .inline_json => |bytes| bytes.len,
            .retained => |ref| ref.byte_length,
        };
    }
    pub fn digest(self: Payload) Digest {
        return switch (self) {
            .inline_json => |bytes| hash: {
                var out: Digest = undefined;
                std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
                break :hash out;
            },
            .retained => |ref| ref.content_digest,
        };
    }
    pub fn validate(self: Payload) !void {
        switch (self) {
            .inline_json => |bytes| if (bytes.len == 0 or bytes.len > inline_bytes) return error.ArtifactTooLarge,
            .retained => |ref| try ref.validate(),
        }
    }
};
pub const Chunk = struct {
    artifact_id: [175]u8,
    checksum: Digest,
    bytes: u32,
};
pub const Manifest = struct {
    version: u8 = 1,
    scope: artifacts.UploadScope,
    byte_length: u64,
    content_digest: Digest,
    chunks: []const Chunk,

    pub fn validate(self: Manifest, ref: Retained) !void {
        try ref.validate();
        if (self.version != 1 or !std.meta.eql(self.scope, ref.scope) or self.byte_length != ref.byte_length or
            !std.mem.eql(u8, &self.content_digest, &ref.content_digest) or self.chunks.len != ref.chunk_count) return error.ArtifactIntegrityMismatch;
        var total: u64 = 0;
        for (self.chunks, 0..) |chunk, index| {
            if (chunk.bytes == 0 or chunk.bytes > chunk_bytes or (index + 1 != self.chunks.len and chunk.bytes != chunk_bytes)) return error.ArtifactIntegrityMismatch;
            const id = try self.scope.artifactId(&std.fmt.bytesToHex(&chunk.checksum, .lower));
            if (!std.mem.eql(u8, &id, &chunk.artifact_id)) return error.ArtifactIntegrityMismatch;
            total = std.math.add(u64, total, chunk.bytes) catch return error.ArtifactIntegrityMismatch;
        }
        if (total != self.byte_length) return error.ArtifactIntegrityMismatch;
    }
};

/// A stable, owned reader for a native-admitted retained payload. The caller
/// supplies the scope from its native observation, including the original
/// READY scope when an intent retains an older execution context. Physical
/// integrity does not by itself attest the decoded logical contents.
///
/// Memory is bounded by the manifest, one verified chunk and the reader buffer.
/// The chunk arena retains its capacity between objects, even when the supplied
/// backing allocator is itself an arena. Decoders must call finish before any
/// proof can be minted; hashing prefetched bytes alone is not consumption.
/// Parse a bounded retained payload without copying the full encoded document.
/// Returned strings are owned across chunk reloads.
pub fn readJson(
    comptime T: type,
    allocator: std.mem.Allocator,
    store: *artifacts.ArtifactStore,
    reference: Retained,
    expected_scope: artifacts.UploadScope,
    cancellation: @import("antfly_cancellation").CancellationToken,
    max_bytes: usize,
) !std.json.Parsed(T) {
    if (reference.byte_length > max_bytes) return error.InvalidReplicationPayloadAdmission;
    const retained = try RetainedReader.create(allocator, store, reference, expected_scope, cancellation);
    defer retained.destroy();
    var scanner = std.json.Reader.init(allocator, &retained.reader);
    defer scanner.deinit();
    const parsed = std.json.parseFromTokenSource(T, allocator, &scanner, .{
        .allocate = .alloc_always,
        .max_value_len = max_bytes,
        .parse_numbers = false,
    }) catch |err| return if (err == error.ReadFailed) retained.readFailure() else err;
    errdefer parsed.deinit();
    try retained.finish();
    return parsed;
}

const Intent = @import("antfly_local_sources").storage_db_types.TransactionIntentRequest;
pub const FrozenIntent = struct {
    transaction: [16]u8,
    topology_epoch: u64,
    intent: Intent,

    pub fn validate(self: FrozenIntent) !void {
        if (std.mem.allEqual(u8, &self.transaction, 0) or self.topology_epoch == 0 or
            self.intent.external_decision_owner != null or self.intent.external_activation or self.intent.replication_source_permit != null or
            self.intent.publication_capture != null or self.intent.document_revision_handoff != null)
            return error.InvalidReplicationPayloadAdmission;
    }
};

pub fn encodeIntent(a: std.mem.Allocator, frozen: FrozenIntent) ![]u8 {
    try frozen.validate();
    return std.json.Stringify.valueAlloc(a, frozen, .{});
}

/// Authenticate a complete participant frame before attaching live authority.
/// Typed decoding preserves opaque keys and keeps only one member in memory.
pub fn readIntent(
    a: std.mem.Allocator,
    store: *artifacts.ArtifactStore,
    reference: Retained,
    scope: artifacts.UploadScope,
    permit: @import("antfly_local_sources").api_replication_source_apply.Permit,
    cancellation: @import("antfly_cancellation").CancellationToken,
) !std.json.Parsed(FrozenIntent) {
    const maximum = @import("antfly_local_sources").storage_db_table_catalog.default_transaction_admission_bytes;
    const parsed = try readJson(FrozenIntent, a, store, reference, scope, cancellation, maximum);
    errdefer parsed.deinit();
    const frozen = parsed.value;
    try frozen.validate();
    if (frozen.topology_epoch != permit.topology_epoch or frozen.intent.schema_version != permit.schema_version or
        !std.mem.eql(u8, &frozen.transaction, &permit.reference.txn_id)) return error.InvalidReplicationPayloadAdmission;
    return parsed;
}

pub const RetainedReader = struct {
    allocator: std.mem.Allocator,
    store: *artifacts.ArtifactStore,
    cancellation: @import("antfly_cancellation").CancellationToken,
    reference: Retained,
    manifest_arena: std.heap.ArenaAllocator,
    chunk_arena: std.heap.ArenaAllocator,
    manifest: Manifest = undefined,
    next_chunk: usize = 0,
    chunk: []const u8 = &.{},
    chunk_offset: usize = 0,
    emitted: u64 = 0,
    hasher: std.crypto.hash.sha2.Sha256 = .init(.{}),
    cause: ?anyerror = null,
    buffer: [8192]u8 = undefined,
    reader: std.Io.Reader = undefined,

    pub fn create(
        allocator: std.mem.Allocator,
        store: *artifacts.ArtifactStore,
        reference: Retained,
        expected_scope: artifacts.UploadScope,
        cancellation: @import("antfly_cancellation").CancellationToken,
    ) !*RetainedReader {
        try cancellation.check();
        try reference.validate();
        try expected_scope.validate();
        if (!std.meta.eql(reference.scope, expected_scope)) return error.ArtifactIntegrityMismatch;
        const self = try allocator.create(RetainedReader);
        self.* = .{
            .allocator = allocator,
            .store = store,
            .cancellation = cancellation,
            .reference = reference,
            .manifest_arena = .init(allocator),
            .chunk_arena = .init(allocator),
        };
        errdefer self.destroy();
        const manifest_allocator = self.manifest_arena.allocator();
        const raw = try store.getVerifiedAllocWithCancellationUsingAllocator(
            manifest_allocator,
            &reference.artifact_id,
            reference.manifest_bytes,
            &std.fmt.bytesToHex(&reference.manifest_digest, .lower),
            cancellation,
        );
        self.manifest = try std.json.parseFromSliceLeaky(Manifest, manifest_allocator, raw, .{
            .allocate = .alloc_if_needed,
            .ignore_unknown_fields = false,
            .max_value_len = max_manifest_bytes,
        });
        try self.manifest.validate(reference);
        self.reader = .{ .vtable = &.{ .stream = stream }, .buffer = &self.buffer, .seek = 0, .end = 0 };
        return self;
    }

    pub fn destroy(self: *RetainedReader) void {
        const allocator = self.allocator;
        self.chunk_arena.deinit();
        self.manifest_arena.deinit();
        allocator.destroy(self);
    }

    /// Preserve the actual transport/cancellation/integrity error hidden by
    /// std.Io.Reader's closed ReadFailed error set.
    pub fn readFailure(self: *const RetainedReader) anyerror {
        return self.cause orelse error.ReadFailed;
    }

    /// Require exact decoder consumption as well as exact physical integrity.
    /// A decoder that stops after a valid prefix cannot authenticate a payload.
    pub fn finish(self: *const RetainedReader) !void {
        try self.cancellation.check();
        if (self.cause) |err| return err;
        if (self.reader.seek != self.reader.end or self.emitted != self.reference.byte_length or
            self.next_chunk != self.manifest.chunks.len or self.chunk.len != 0)
            return error.ArtifactIntegrityMismatch;
        var hashing = self.hasher;
        var digest: Digest = undefined;
        hashing.final(&digest);
        if (!std.mem.eql(u8, &digest, &self.reference.content_digest)) return error.ArtifactIntegrityMismatch;
    }

    fn loadNext(self: *RetainedReader) !void {
        try self.cancellation.check();
        if (self.next_chunk == self.manifest.chunks.len) return error.EndOfStream;
        if (!self.chunk_arena.reset(.retain_capacity)) return error.OutOfMemory;
        const descriptor = self.manifest.chunks[self.next_chunk];
        self.chunk = try self.store.getVerifiedAllocWithCancellationUsingAllocator(
            self.chunk_arena.allocator(),
            &descriptor.artifact_id,
            descriptor.bytes,
            &std.fmt.bytesToHex(&descriptor.checksum, .lower),
            self.cancellation,
        );
        self.chunk_offset = 0;
        self.next_chunk += 1;
    }

    fn stream(reader: *std.Io.Reader, writer: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *RetainedReader = @alignCast(@fieldParentPtr("reader", reader));
        if (self.cause != null) return error.ReadFailed;
        self.cancellation.check() catch |err| {
            self.cause = err;
            return error.ReadFailed;
        };
        if (limit == .nothing) return 0;
        if (self.chunk.len == 0) self.loadNext() catch |err| {
            if (err == error.EndOfStream) return error.EndOfStream;
            self.cause = err;
            return error.ReadFailed;
        };
        const bytes = limit.sliceConst(self.chunk[self.chunk_offset..]);
        const written = try writer.write(bytes);
        self.hasher.update(bytes[0..written]);
        self.emitted += written;
        self.chunk_offset += written;
        if (self.chunk_offset == self.chunk.len) {
            self.chunk = &.{};
            self.chunk_offset = 0;
        }
        return written;
    }
};

/// Publish chunks first and publish their immutable manifest last. A retry can
/// reuse identical chunks under the same durable upload scope.
pub fn retain(a: std.mem.Allocator, store: *artifacts.ArtifactStore, scope: artifacts.UploadScope, body: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) !Retained {
    return retainInternal(a, store, scope, body, cancellation, false);
}

pub fn retainTestPayload(a: std.mem.Allocator, store: *artifacts.ArtifactStore, scope: artifacts.UploadScope, body: []const u8, false_content_digest: bool) !Retained {
    if (!false_content_digest) return retain(a, store, scope, body, .none);
    return retainInternal(a, store, scope, body, .none, true);
}

fn retainInternal(a: std.mem.Allocator, store: *artifacts.ArtifactStore, scope: artifacts.UploadScope, body: []const u8, cancellation: @import("antfly_cancellation").CancellationToken, corrupt: bool) !Retained {
    if (body.len == 0 or body.len > 64 * 1024 * 1024) return error.InvalidReplicationPayloadAdmission;
    try cancellation.check();
    var chunks: std.ArrayList(Chunk) = .empty;
    defer chunks.deinit(a);
    var offset: usize = 0;
    while (offset < body.len) {
        const end = offset + @min(chunk_bytes, body.len - offset);
        const bytes = body[offset..end];
        var metadata = try store.putScoped(scope, bytes, cancellation);
        defer metadata.deinit(store.allocator);
        var chunk: Chunk = .{ .artifact_id = undefined, .checksum = undefined, .bytes = @intCast(bytes.len) };
        if (chunk.artifact_id.len != metadata.artifact_id.len) return error.ArtifactIntegrityMismatch;
        @memcpy(&chunk.artifact_id, metadata.artifact_id);
        std.crypto.hash.sha2.Sha256.hash(bytes, &chunk.checksum, .{});
        try chunks.append(a, chunk);
        offset = end;
    }
    var content_digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &content_digest, .{});
    if (corrupt) content_digest[0] ^= 1;
    const manifest: Manifest = .{ .scope = scope, .byte_length = body.len, .content_digest = content_digest, .chunks = chunks.items };
    const encoded = try std.json.Stringify.valueAlloc(a, manifest, .{});
    defer a.free(encoded);
    var metadata = try store.putScoped(scope, encoded, cancellation);
    defer metadata.deinit(store.allocator);
    var result: Retained = .{
        .scope = scope,
        .artifact_id = undefined,
        .manifest_digest = undefined,
        .manifest_bytes = @intCast(encoded.len),
        .byte_length = body.len,
        .content_digest = content_digest,
        .chunk_count = @intCast(chunks.items.len),
    };
    @memcpy(&result.artifact_id, metadata.artifact_id);
    std.crypto.hash.sha2.Sha256.hash(encoded, &result.manifest_digest, .{});
    try result.validate();
    return result;
}

test "retained replication payload authenticates full decoder consumption and original upload scope" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("replication-retained-reader");
    defer directory.cleanup();
    var fs = try @import("../../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: artifacts.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const body = "a valid prefix is not the complete retained payload";
    const reference = try retainTestPayload(a, &store, scope, body, false);
    var wrong_scope = scope;
    wrong_scope.attempt[15] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, RetainedReader.create(a, &store, reference, wrong_scope, .none));
    const reader = try RetainedReader.create(a, &store, reference, scope, .none);
    defer reader.destroy();
    try std.testing.expectEqual(body[0], try reader.reader.takeByte());
    // The complete object has already been verified and prefetched, but most
    // bytes remain unconsumed in the reader's own buffer.
    try std.testing.expectEqual(reference.byte_length, reader.emitted);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, reader.finish());
    var tail: [body.len - 1]u8 = undefined;
    try reader.reader.readSliceAll(&tail);
    try std.testing.expectEqualStrings(body[1..], &tail);
    try reader.finish();
    try reader.finish();
    try std.testing.expectError(error.EndOfStream, reader.reader.takeByte());
    const Probe = struct {
        fn run(allocator: std.mem.Allocator, actual: *artifacts.ArtifactStore, ref: Retained) !void {
            const held = try RetainedReader.create(allocator, actual, ref, ref.scope, .none);
            defer held.destroy();
            var buffer: [body.len]u8 = undefined;
            held.reader.readSliceAll(&buffer) catch |err| return if (err == error.ReadFailed) held.readFailure() else err;
            try held.finish();
        }
    };
    // Immutable reads make allocation-failure retries safe. Every held manifest,
    // chunk arena and stable reader allocation must be released on every cut.
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ &store, reference });
}

test "retained replication payload JSON preserves exact numbers and rejects trailing data" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("replication-retained-json");
    defer directory.cleanup();
    var fs = try @import("../../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: artifacts.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const bytes = "{\"data\":9007199254740993}";
    const valid = try retainTestPayload(a, &store, scope, bytes, false);
    const decoded = try readJson(std.json.Value, a, &store, valid, scope, .none, bytes.len);
    defer decoded.deinit();
    try std.testing.expectEqualStrings("9007199254740993", decoded.value.object.get("data").?.number_string);
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, readJson(std.json.Value, a, &store, valid, scope, .none, bytes.len - 1));
    const forged = try retainTestPayload(a, &store, scope, bytes, true);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, readJson(std.json.Value, a, &store, forged, scope, .none, bytes.len));
    const trailing = try retainTestPayload(a, &store, scope, bytes ++ "{}", false);
    try std.testing.expectError(error.SyntaxError, readJson(std.json.Value, a, &store, trailing, scope, .none, bytes.len + 2));

    const codec = @import("snapshot_codec.zig");
    const page = try retainTestPayload(
        a,
        &store,
        scope,
        "{\"version\":2,\"relation_id\":7,\"offset\":2,\"rows\":[{\"value\":{\"data\":9007199254740993},\"json_null_fields\":[]}]}",
        false,
    );
    const relation: @import("antfly_local_sources").system_catalog_logical_replication_origin.Relation = .{
        .id = 7,
        .namespace = "public",
        .name = "records",
        .replica_identity = .default,
        .columns = &.{.{ .name = "data", .type_oid = 20, .type_modifier = -1, .key = true }},
    };
    var entry: codec.CaptureEntry = .{ .relation_id = 7, .offset = 2, .count = 1, .retained = page };
    const verified = try entry.read(a, &store, scope, relation, .none);
    defer verified.deinit();
    try std.testing.expectEqualStrings("9007199254740993", verified.value.rows[0].value.object.get("data").?.number_string);
    entry.offset += 1;
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, entry.read(a, &store, scope, relation, .none));
}

test "retained replication payload streams verified chunks and rejects aggregate digest forgery" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("replication-retained-chunks");
    defer directory.cleanup();
    var fs = try @import("../../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: artifacts.UploadScope = .{ .domain = @splat(3), .attempt = @splat(4) };
    const body = try a.alloc(u8, chunk_bytes + 17);
    defer a.free(body);
    for (body, 0..) |*byte, i| byte.* = @truncate(i);
    for ([_]bool{ false, true }) |false_digest| {
        const reference = try retainTestPayload(a, &store, scope, body, false_digest);
        const reader = try RetainedReader.create(a, &store, reference, scope, .none);
        defer reader.destroy();
        var buffer: [4096]u8 = undefined;
        var offset: usize = 0;
        while (offset < body.len) {
            const count = @min(buffer.len, body.len - offset);
            try reader.reader.readSliceAll(buffer[0..count]);
            try std.testing.expectEqualSlices(u8, body[offset..][0..count], buffer[0..count]);
            offset += count;
        }
        if (false_digest) {
            try std.testing.expectError(error.ArtifactIntegrityMismatch, reader.finish());
        } else try reader.finish();
    }
    const reference = try retainTestPayload(a, &store, scope, body, false);
    var canceled: std.atomic.Value(bool) = .init(false);
    const cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled);
    const held = try RetainedReader.create(a, &store, reference, scope, cancellation);
    defer held.destroy();
    var buffer: [4096]u8 = undefined;
    for (0..chunk_bytes / buffer.len) |_| try held.reader.readSliceAll(&buffer);
    canceled.store(true, .release);
    try std.testing.expectError(error.ReadFailed, held.reader.takeByte());
    try std.testing.expect(held.readFailure() == error.Canceled);
    try std.testing.expectError(error.Canceled, held.finish());
}

test "Source artifact custody upload domains fence actual metadata group" {
    const origin = @import("antfly_local_sources").system_catalog_logical_replication_origin;
    const identity: origin.Identity = .{ .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } }, .origin_generation = 1, .provider_identity = @splat(3), .recipe_digest = @splat(4), .slot_name = "actual_slot", .roster_digest = @splat(5) };
    const first = try uploadScope(identity, 1, @splat('1'), 1, @splat(6));
    const second = try uploadScope(identity, 2, @splat('1'), 1, @splat(6));
    try std.testing.expect(!std.meta.eql(first, second));
    try std.testing.expectEqualDeep(first, try uploadScope(identity, 1, @splat('1'), 1, @splat(6)));
    try std.testing.expectError(error.InvalidArtifactUploadScope, uploadScope(identity, 0, @splat('1'), 1, @splat(6)));
}
