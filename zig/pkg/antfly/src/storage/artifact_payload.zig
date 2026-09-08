// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Common source-payload boundary. Primary transactions select visibility;
//! implementations prepare immutable payloads before primary commit.
const std = @import("std");
const codec = @import("db/enrichment/artifact_codec.zig");
const keys = @import("internal_keys.zig");
const Allocator = std.mem.Allocator;

pub const Stats = struct {
    directory_bytes_written: u64 = 0,
    directory_entries: u64 = 0,
    directory_publications: u64 = 0,
    directory_publication_deferrals: u64 = 0,
    directory_hits: u64 = 0,
    directory_misses: u64 = 0,
    source_segments: u64 = 0,
    ownership_index_collections: u64 = 0,
    ownership_index_entries_scanned: u64 = 0,
    prepare_requests: u64 = 0,
    prepare_lock_wait_ns: u64 = 0,
    outer_db_batch_lock_wait_ns: u64 = 0,
    decode_outside_lock_ns: u64 = 0,
    snapshot_read_ns: u64 = 0,
    cache_reclaimed_bytes: u64 = 0,
    retired_ann_references_skipped: u64 = 0,

    location_cache_hits: u64 = 0,
    location_cache_misses: u64 = 0,
    location_cache_bytes: u64 = 0,
    source_shards: u64 = 0,
    collection_steps: u64 = 0,
    collection_pending_bytes: u64 = 0,
    collection_mark_ns: u64 = 0,
    collection_mark_steps: u64 = 0,
    collection_mark_budget_yields: u64 = 0,
    collection_mark_busy_deferrals: u64 = 0,
    collection_mark_outside_lock_ns: u64 = 0,
    collection_mark_merge_ns: u64 = 0,
    collection_mark_max_merge_ns: u64 = 0,
    collection_mark_rows: u64 = 0,
    collection_mark_max_step_rows: u64 = 0,
    collection_mark_max_step_ns: u64 = 0,
    collection_plan_ns: u64 = 0,
    collection_locked_ns: u64 = 0,
    collection_max_locked_ns: u64 = 0,
    collection_setup_ns: u64 = 0,
    collection_max_setup_ns: u64 = 0,
    collection_max_plan_ns: u64 = 0,
    collection_copy_ns: u64 = 0,
    collection_max_copy_ns: u64 = 0,
    collection_publish_ns: u64 = 0,
    collection_max_publish_ns: u64 = 0,
    collection_active_scan_turns: u64 = 0,
    collection_active_scan_pause_ns: u64 = 0,
    collection_rescued_payloads: u64 = 0,
    deduplicated_reappend_payloads: u64 = 0,
    deduplicated_reappend_bytes: u64 = 0,
    checkpoint_receipt_hits: u64 = 0,
    checkpoint_inventory_restores: u64 = 0,
    checkpoint_receipt_bytes_written: u64 = 0,
    prepare_batches: u64 = 0,
    preparation_ns: u64 = 0,
    durable_append_ns: u64 = 0,
    checkpoint_ns: u64 = 0,
    prepared_payloads: u64 = 0,
    prepared_payload_bytes: u64 = 0,
    wal_bytes_written: u64 = 0,
    active_sessions: u64 = 0,
    resolved_payloads: u64 = 0,
    resolved_bytes: u64 = 0,
    active_wal_bytes: u64 = 0,
    immutable_block_bytes: u64 = 0,
    live_payloads_at_collection: u64 = 0,
    live_payload_bytes_at_collection: u64 = 0,
    collections: u64 = 0,
    collection_deferrals: u64 = 0,
    collection_bytes_read: u64 = 0,
    collection_bytes_written: u64 = 0,
    unresolved_primary_commits: u64 = 0,
    heap_bytes: u64 = 0,
    retained_payloads: u64 = 0,
    retained_payload_bytes: u64 = 0,
    unreferenced_payload_bytes_at_collection: u64 = 0,
    checkpoint_bytes_read: u64 = 0,
    checkpoint_bytes_written: u64 = 0,
};

pub const magic = "AFVREF01";
pub const Digest = [32]u8;
pub const reference_len = magic.len + codec.header_len + 4 + @sizeOf(Digest);
pub const ownership_prefix = "\x00\x00__metadata__:source_vector_owner:";
pub const ownership_epoch_key = "\x00\x00__metadata__:source_vector_owner_epoch";
pub fn ownershipEnabled() bool {
    const raw = if (@import("builtin").link_libc) std.c.getenv("ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX") else null;
    return if (raw) |value| std.mem.eql(u8, std.mem.span(value), "1") else false;
}
pub const reference_epoch_key = "\x00\x00__metadata__:source_vector_reference_epoch";

pub fn isEmbeddingKey(key: []const u8) bool {
    return keys.isEmbeddingArtifactKey(key) or keys.isDerivedEmbeddingArtifactKey(key);
}

pub fn isReference(value: []const u8) bool {
    return std.mem.startsWith(u8, value, magic);
}

/// Primary-owned metadata. Reading it neither resolves nor certifies the
/// external payload. Returned values have no borrowed storage lifetime.
pub const Metadata = struct {
    header: codec.Header,
    dense_dimensions: ?u32,
    reference: ?Reference,

    pub fn decode(value: []const u8) !Metadata {
        if (isReference(value)) {
            const reference = try Reference.decode(value);
            return .{
                .header = try codec.decodeHeaderPrefix(&reference.header),
                .dense_dimensions = reference.dims,
                .reference = reference,
            };
        }
        const header = try codec.decodeHeader(value);
        return .{
            .header = header,
            .dense_dimensions = if (header.kind == .dense_embedding) try codec.decodeDenseEmbeddingDims(value) else null,
            .reference = null,
        };
    }

    pub fn sourceHash(self: Metadata) ?u64 {
        return if (self.header.flags.has_source_hash) self.header.source_hash else null;
    }
};

pub const Reference = struct {
    header: [codec.header_len]u8,
    dims: u32,
    digest: Digest,

    pub fn forArtifact(key: []const u8, value: []const u8) !Reference {
        const dims = try codec.decodeDenseEmbeddingDims(value);
        if (dims == 0) return error.InvalidVectorDimensions;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly-exact-artifact-v1");
        var len: [8]u8 = undefined;
        std.mem.writeInt(u64, &len, key.len, .little);
        hasher.update(&len);
        hasher.update(key);
        hasher.update(value);
        var digest: Digest = undefined;
        hasher.final(&digest);
        return .{ .header = value[0..codec.header_len].*, .dims = dims, .digest = digest };
    }

    pub fn encode(self: Reference) [reference_len]u8 {
        var result: [reference_len]u8 = undefined;
        @memcpy(result[0..magic.len], magic);
        @memcpy(result[magic.len..][0..codec.header_len], &self.header);
        std.mem.writeInt(u32, result[magic.len + codec.header_len ..][0..4], self.dims, .little);
        @memcpy(result[reference_len - 32 ..], &self.digest);
        return result;
    }

    pub fn decode(value: []const u8) !Reference {
        if (value.len != reference_len or !isReference(value)) return error.InvalidVectorReference;
        const header = value[magic.len..][0..codec.header_len];
        const dims = std.mem.readInt(u32, value[magic.len + codec.header_len ..][0..4], .little);
        // Validate the envelope without manufacturing a full vector allocation.
        if (!std.mem.eql(u8, header[0..codec.magic.len], &codec.magic) or
            std.mem.readInt(u16, header[8..10], .little) != codec.codec_version or
            header[10] != @intFromEnum(codec.Kind.dense_embedding) or dims == 0 or
            @as(u64, dims) * 4 + 4 != std.mem.readInt(u32, header[codec.header_len - 4 ..][0..4], .little))
            return error.InvalidVectorReference;
        return .{ .header = header.*, .dims = dims, .digest = value[reference_len - 32 ..][0..32].* };
    }

    pub fn reconstruct(self: Reference, alloc: Allocator, key: []const u8, vector_bytes: []const u8) ![]u8 {
        if (vector_bytes.len != @as(usize, self.dims) * 4) return error.InvalidVectorReference;
        const result = try alloc.alloc(u8, codec.header_len + 4 + vector_bytes.len);
        errdefer alloc.free(result);
        @memcpy(result[0..codec.header_len], &self.header);
        std.mem.writeInt(u32, result[codec.header_len..][0..4], self.dims, .little);
        @memcpy(result[codec.header_len + 4 ..], vector_bytes);
        const actual = try forArtifact(key, result);
        if (!std.mem.eql(u8, &actual.digest, &self.digest)) return error.VectorReferenceIdentityMismatch;
        return result;
    }
};

pub const Prepared = struct {
    reference: Reference,
    artifact: []const u8,
};

pub const Store = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        retain: *const fn (*anyopaque) void,
        release: *const fn (*anyopaque) void,
        prepare: *const fn (*anyopaque, []const Prepared) anyerror!void,
        resolve: *const fn (*anyopaque, Allocator, []const u8, Reference) anyerror![]u8,
        unresolved_commit: ?*const fn (*anyopaque) void = null,
    };

    pub fn retain(self: Store) void {
        self.vtable.retain(self.ptr);
    }
    pub fn release(self: Store) void {
        self.vtable.release(self.ptr);
    }
};

/// One primary transaction's payload ownership and returned-value lifetime.
/// Allocation remains proportional to payloads actually accessed by the caller.
pub const Session = struct {
    alloc: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    arena: std.heap.ArenaAllocator,
    store: Store,
    prepared: std.ArrayListUnmanaged(Prepared) = .empty,
    durable: bool = false,
    prepared_once: bool = false,
    committed: bool = false,
    reference_mutated: bool = false,
    reference_epoch_staged: bool = false,
    primary_commit_attempted: bool = false,
    ownership_failed: bool = false,

    pub fn create(alloc: Allocator, store: Store) !*Session {
        const self = try alloc.create(Session);
        store.retain();
        self.* = .{ .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .store = store };
        return self;
    }
    pub fn retain(self: *Session) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *Session) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        if ((self.prepared_once or (self.reference_mutated and self.primary_commit_attempted)) and !self.committed) {
            if (self.store.vtable.unresolved_commit) |unresolved| unresolved(self.store.ptr);
        }
        self.arena.deinit();
        self.store.release();
        self.alloc.destroy(self);
    }
    pub fn put(self: *Session, key: []const u8, value: []const u8) ![]const u8 {
        if (!isEmbeddingKey(key)) return value;
        self.reference_mutated = true;
        if (isReference(value)) return error.PhysicalVectorReferenceNotWritable;
        const header = codec.decodeHeader(value) catch return value;
        if (header.kind != .dense_embedding) return value;
        const reference = try Reference.forArtifact(key, value);
        const alloc = self.arena.allocator();
        const artifact = try alloc.dupe(u8, value);
        try self.prepared.append(alloc, .{ .reference = reference, .artifact = artifact });
        self.durable = false;
        return try alloc.dupe(u8, &reference.encode());
    }
    pub fn get(self: *Session, key: []const u8, value: []const u8) ![]const u8 {
        return self.getAlloc(self.arena.allocator(), key, value);
    }
    pub fn getAlloc(self: *Session, alloc: Allocator, key: []const u8, value: []const u8) ![]const u8 {
        if (!isEmbeddingKey(key) or !isReference(value)) return value;
        const reference = try Reference.decode(value);
        for (self.prepared.items) |pending| {
            if (std.mem.eql(u8, &pending.reference.digest, &reference.digest)) {
                const actual = try Reference.forArtifact(key, pending.artifact);
                if (!std.mem.eql(u8, &actual.digest, &reference.digest)) return error.VectorReferenceIdentityMismatch;
                return pending.artifact;
            }
        }
        return try self.store.vtable.resolve(self.store.ptr, alloc, key, reference);
    }
    pub fn prepareCommit(self: *Session) !void {
        if (self.durable or self.prepared.items.len == 0) return;
        try self.store.vtable.prepare(self.store.ptr, self.prepared.items);
        self.durable = true;
        self.prepared_once = true;
    }

    /// Materialized ownership changes use the primary transaction and its
    /// existing replay WAL. No independent journal can outlive its checkpoint.
    /// Called only after the physical artifact mutation succeeded.
    pub fn recordOwnership(self: *Session, txn: anytype, key: []const u8, value: ?[]const u8) !void {
        if (!ownershipEnabled() or !isEmbeddingKey(key)) return;
        errdefer self.ownership_failed = true;
        var owner_key: [ownership_prefix.len + 32]u8 = undefined;
        @memcpy(owner_key[0..ownership_prefix.len], ownership_prefix);
        std.crypto.hash.sha2.Sha256.hash(key, owner_key[ownership_prefix.len..], .{});
        if (value) |raw| {
            if (isReference(raw)) {
                const reference = try Reference.decode(raw);
                var encoded: [36]u8 = undefined;
                @memcpy(encoded[0..32], &reference.digest);
                std.mem.writeInt(u32, encoded[32..36], reference.dims, .little);
                return txn.put(&owner_key, &encoded);
            }
        }
        txn.delete(&owner_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    /// Commit this invalidation with the reference changes themselves. A
    /// delete-only transaction also invalidates a cached liveness proof.
    pub fn stageReferenceEpoch(self: *Session, txn: anytype) !void {
        if (self.ownership_failed) return error.VectorOwnershipMutationFailed;
        if (!self.reference_mutated or self.reference_epoch_staged) return;
        const previous = txn.get(reference_epoch_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const epoch: u64 = if (previous) |raw| blk: {
            if (raw.len != 8) return error.InvalidVectorReferenceEpoch;
            break :blk std.mem.readInt(u64, raw[0..8], .little);
        } else 0;
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, try std.math.add(u64, epoch, 1), .little);
        if (ownershipEnabled()) {
            const prior_owner = txn.get(ownership_epoch_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            const owner_epoch: ?u64 = if (prior_owner) |raw|
                if (raw.len == 8) std.mem.readInt(u64, raw[0..8], .little) else null
            else if (epoch == 0) 0 else null;
            // A disabled interval invalidates coverage. Never certify a
            // partial index when enabling this experiment on existing data.
            if (owner_epoch == epoch) try txn.put(ownership_epoch_key, &bytes);
        }
        try txn.put(reference_epoch_key, &bytes);
        self.reference_epoch_staged = true;
    }
};

test "vector references preserve source envelope and separate model artifacts" {
    const alloc = std.testing.allocator;
    const a = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-a");
    defer alloc.free(a);
    const b = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model-b");
    defer alloc.free(b);
    const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, 99, &.{ 1, 2, 3 });
    defer alloc.free(artifact);
    const first = try Reference.forArtifact(a, artifact);
    const second = try Reference.forArtifact(b, artifact);
    const inline_metadata = try Metadata.decode(artifact);
    const reference_metadata = try Metadata.decode(&first.encode());
    try std.testing.expectEqual(inline_metadata.header, reference_metadata.header);
    try std.testing.expectEqual(inline_metadata.dense_dimensions, reference_metadata.dense_dimensions);
    try std.testing.expectEqual(@as(?u64, 99), reference_metadata.sourceHash());
    try std.testing.expectError(error.InvalidArtifactPayload, codec.decodeHeader(&first.header));
    try std.testing.expectError(error.InvalidVectorReference, Metadata.decode(first.encode()[0 .. reference_len - 1]));
    try std.testing.expect(!std.mem.eql(u8, &first.digest, &second.digest));
    const decoded = try Reference.decode(&first.encode());
    const restored = try decoded.reconstruct(alloc, a, artifact[codec.header_len + 4 ..]);
    defer alloc.free(restored);
    try std.testing.expectEqualSlices(u8, artifact, restored);
    try std.testing.expectError(error.VectorReferenceIdentityMismatch, decoded.reconstruct(alloc, b, artifact[codec.header_len + 4 ..]));
    var corrupt = first.encode();
    corrupt[magic.len + codec.header_len] ^= 1;
    try std.testing.expectError(error.InvalidVectorReference, Reference.decode(&corrupt));
}
