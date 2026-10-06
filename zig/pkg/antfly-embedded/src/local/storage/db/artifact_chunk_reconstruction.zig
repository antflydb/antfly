// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Bounded off-lock reconstruction of an existing ordinal stream. Checkpoints
//! bind the catalog epoch, exact scope, and physical stream revision. Discovery
//! is not publication: callers must order and CAS checkpoint/final-manifest
//! installation, never treat a completed scan as accepted producer output.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const keys = @import("../internal_keys.zig");
const time = @import("antfly_platform").time;

pub const Scope = struct { document: []const u8, producer: []const u8, unit: ?[]const u8 = null };
pub const checkpoint_len = 245;

pub const Checkpoint = struct {
    authority: publication.Authority,
    scope_digest: publication.Digest,
    stream_revision: ?publication.Position,
    manifest: chunks.Manifest,

    pub fn encode(self: Checkpoint) ![checkpoint_len]u8 {
        var raw: [checkpoint_len]u8 = @splat(0);
        @memcpy(raw[0..4], "ACR1");
        @memcpy(raw[4..28], &self.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.authority.epoch, .little);
        @memcpy(raw[36..68], &self.authority.catalog_digest);
        @memcpy(raw[68..100], &self.scope_digest);
        if (self.stream_revision) |revision| @memcpy(raw[100..133], &try revision.encode());
        @memcpy(raw[133..213], &self.manifest.encode());
        std.crypto.hash.sha2.Sha256.hash(raw[0..213], raw[213..245], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Checkpoint {
        if (raw.len != checkpoint_len or !std.mem.eql(u8, raw[0..4], "ACR1")) return error.ArtifactCatalogCorrupt;
        var checksum: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..213], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[213..245])) return error.ArtifactCatalogCorrupt;
        const result: Checkpoint = .{
            .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* },
            .scope_digest = raw[68..100].*,
            .stream_revision = if (std.mem.allEqual(u8, raw[100..133], 0)) null else publication.Position.decode(raw[100..133]) catch return error.ArtifactCatalogCorrupt,
            .manifest = try chunks.Manifest.decode(raw[133..213]),
        };
        if (result.authority.epoch == 0 or std.mem.allEqual(u8, &result.authority.namespace, 0) or std.mem.allEqual(u8, &result.authority.catalog_digest, 0)) return error.ArtifactCatalogCorrupt;
        if (result.stream_revision) |revision| revision.requireNamespace(publication.namespaceFromBytes(result.authority.namespace)) catch return error.ArtifactCatalogCorrupt;
        return result;
    }

    /// Required in the final writer as well as before a resumed read. A
    /// mutation anywhere in this stream invalidates every prior scan page.
    pub fn requireCurrent(self: Checkpoint, txn: anytype, manifest_key: []const u8) !void {
        const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(current, self.authority)) return error.ArtifactCatalogDrift;
        var scope_digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(manifest_key, &scope_digest, .{});
        if (!std.mem.eql(u8, &self.scope_digest, &scope_digest)) return error.InvalidBatchRequest;
        if (!std.meta.eql(self.stream_revision, try publication.artifactRevision(txn, current.namespace, manifest_key))) return error.EnrichmentSourceChanged;
    }
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    manifest_key: []const u8,
    previous: Checkpoint,
    next: Checkpoint,
    at_end: bool,
    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Limits = struct {
    rows: usize = 128,
    bytes: usize = 64 * 1024,
    /// Discovery yields on time as well as size. A receiver verifies the
    /// sender's actual bounded row count without a machine-dependent deadline.
    time_budget_ns: ?u64 = 2 * std.time.ns_per_ms,
};

/// No snapshot survives this call. At most one oversized row advances a page;
/// otherwise preparation is bounded by rows, payload bytes, or 2 ms. Scope
/// prefixes exclude other units and other producers even with binary names.
pub fn prepare(alloc: std.mem.Allocator, store: anytype, scope: Scope, checkpoint: ?Checkpoint, limits: Limits) !?Page {
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    return scan(alloc, &read, scope, checkpoint, limits);
}

/// The ordered verifier and discovery use the same scan over one pinned cut.
/// Supplying an existing snapshot also keeps checkpoint CAS and stream rows
/// from being sampled across unrelated transactions.
pub fn scan(alloc: std.mem.Allocator, txn: anytype, scope: Scope, checkpoint: ?Checkpoint, limits: Limits) !?Page {
    if (limits.rows == 0 or limits.rows > 128 or limits.bytes == 0 or limits.bytes > 64 * 1024) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const manifest_key = try chunks.scopedKeyAlloc(owned, scope.document, scope.producer, scope.unit);
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    const existing = txn.get(manifest_key) catch |err| if (err == error.NotFound) null else return err;
    if (existing) |raw| {
        _ = try chunks.Manifest.decode(raw);
        arena.deinit();
        return null;
    }
    // A selected immutable generation is a different physical layout, not an
    // empty legacy prefix. Its publisher must retain that generation's head.
    const head_key = try owned.dupe(u8, manifest_key);
    head_key[keys.findComponentTerminator(head_key, 1).? + 2] = keys.producer_generation_head_kind;
    const head = txn.get(head_key) catch |err| if (err == error.NotFound) null else return err;
    if (head != null) return error.OnlineMergeArtifactTailsUnsupported;
    var scope_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(manifest_key, &scope_digest, .{});
    const previous = checkpoint orelse Checkpoint{
        .authority = authority,
        .scope_digest = scope_digest,
        .stream_revision = try publication.artifactRevision(txn, authority.namespace, manifest_key),
        .manifest = chunks.Builder.init().finish(),
    };
    try previous.requireCurrent(txn, manifest_key);
    var builder = try chunks.Builder.fromCheckpoint(previous.manifest);
    const start = if (scope.unit) |unit|
        try keys.documentUnitChunkArtifactKeyAlloc(owned, scope.document, scope.producer, unit, builder.count)
    else
        try keys.chunkArtifactKeyAlloc(owned, scope.document, scope.producer, builder.count);
    const prefix = start[0 .. start.len - 4];
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(start);
    const started = time.monotonicNs();
    var rows: usize = 0;
    var bytes: usize = 0;
    var at_end = true;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (rows != 0 and (rows >= limits.rows or bytes >= limits.bytes or item.key.len +| item.value.len > limits.bytes -| bytes or
            (if (limits.time_budget_ns) |budget| time.monotonicNs() -| started >= budget else false)))
        {
            at_end = false;
            break;
        }
        if (item.key.len != prefix.len + 4 or std.mem.readInt(u32, item.key[item.key.len - 4 ..][0..4], .big) != builder.count) return error.ArtifactCatalogCorrupt;
        try builder.append(builder.count, item.value);
        rows += 1;
        bytes +|= item.key.len +| item.value.len;
        entry = try cursor.next();
    }
    var next = previous;
    next.manifest = builder.finish();
    return .{ .arena = arena, .manifest_key = manifest_key, .previous = previous, .next = next, .at_end = at_end };
}
