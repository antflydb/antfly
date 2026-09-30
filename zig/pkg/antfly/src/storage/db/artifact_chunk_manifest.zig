// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Complete ordinal chunk sets, independent of physical scans or worker state.
//! A publication replaces one inventory and every changed member atomically.
//! This is an output-set certificate, not an entire-document completion claim.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const publication = @import("artifact_publication.zig");
const ids = @import("artifact_ids.zig");
pub const encoded_len = 80;

pub fn keyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    return scopedKeyAlloc(alloc, document, producer, null);
}

pub fn scopedKeyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, unit: ?[]const u8) ![]u8 {
    if (document.len == 0 or producer.len == 0) return error.InvalidBatchRequest;
    var result: std.ArrayListUnmanaged(u8) = .empty;
    errdefer result.deinit(alloc);
    try keys.appendDocumentPrefix(&result, alloc, document);
    try result.append(alloc, keys.producer_stream_manifest_kind);
    try keys.appendEncodedComponent(&result, alloc, producer);
    if (unit) |id| {
        if (id.len == 0) return error.InvalidBatchRequest;
        try result.append(alloc, keys.document_unit_record_kind);
        try keys.appendEncodedComponent(&result, alloc, id);
    }
    return result.toOwnedSlice(alloc);
}

/// Preserve encoded binary components directly. The physical mutation hook
/// needs one short allocation, not decoding every component of every chunk
/// into temporary strings. Embeddings and neighboring units are excluded.
pub fn keyForMemberAlloc(alloc: std.mem.Allocator, member: []const u8) !?[]u8 {
    if (!keys.isChunkArtifactRecordKey(member)) return null;
    const document_end = keys.findComponentTerminator(member, 1).? + 2;
    const name_start = keys.findComponentTerminator(member, document_end + 1).? + 2;
    const scope_end = member.len - 5;
    const result = try alloc.alloc(u8, document_end + 1 + scope_end - name_start);
    @memcpy(result[0..document_end], member[0..document_end]);
    result[document_end] = keys.producer_stream_manifest_kind;
    @memcpy(result[document_end + 1 ..], member[name_start..scope_end]);
    return result;
}

pub fn isKey(key: []const u8) bool {
    return isScopeKey(key, keys.producer_stream_manifest_kind);
}

/// Shared canonical scope grammar for inventories and generation-head guards.
/// Generation members/state have additional suffixes and are not scope keys.
pub fn isScopeKey(key: []const u8, kind_byte: u8) bool {
    if (!keys.isInternalUserKey(key)) return false;
    const doc_end = keys.findComponentTerminator(key, 1) orelse return false;
    const kind = doc_end + 2;
    if (doc_end == 1 or kind >= key.len or key[kind] != kind_byte) return false;
    const name_end = keys.findComponentTerminator(key, kind + 1) orelse return false;
    if (name_end == kind + 1) return false;
    const scope = name_end + 2;
    if (scope == key.len) return true;
    if (key[scope] != keys.document_unit_record_kind) return false;
    const unit_end = keys.findComponentTerminator(key, scope + 1) orelse return false;
    return unit_end > scope + 1 and unit_end + 2 == key.len;
}

pub const Manifest = struct {
    count: u32,
    payload_bytes: u64,
    content_digest: publication.Digest,

    pub fn encode(self: Manifest) [encoded_len]u8 {
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..4], "ACM2");
        std.mem.writeInt(u32, raw[4..8], self.count, .little);
        std.mem.writeInt(u64, raw[8..16], self.payload_bytes, .little);
        @memcpy(raw[16..48], &self.content_digest);
        std.crypto.hash.sha2.Sha256.hash(raw[0..48], raw[48..80], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Manifest {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "ACM2")) return error.ArtifactCatalogCorrupt;
        var checksum: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..48], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[48..80])) return error.ArtifactCatalogCorrupt;
        const value: Manifest = .{ .count = std.mem.readInt(u32, raw[4..8], .little), .payload_bytes = std.mem.readInt(u64, raw[8..16], .little), .content_digest = raw[16..48].* };
        if (value.count == 0 and value.payload_bytes != 0) return error.ArtifactCatalogCorrupt;
        if (value.count == 0 and !std.mem.eql(u8, &value.content_digest, &Builder.init().finish().content_digest)) return error.ArtifactCatalogCorrupt;
        return value;
    }
};

/// Streaming construction retains no payloads or per-member hashes. Ordinals
/// are canonical and contiguous, including when provider batches are sliced.
pub const Builder = struct {
    digest: publication.Digest,
    count: u32 = 0,
    payload_bytes: u64 = 0,

    pub fn init() Builder {
        var digest: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash("antfly-chunk-output-set-v2\x00", &digest, .{});
        return .{ .digest = digest };
    }
    /// A portable fixed-size checkpoint, independent of stdlib hash internals.
    /// The caller must bind it to the exact stream mutation revision/authority;
    /// this digest alone is neither a reconstruction nor completion certificate.
    pub fn fromCheckpoint(checkpoint: Manifest) !Builder {
        _ = try Manifest.decode(&checkpoint.encode());
        return .{ .digest = checkpoint.content_digest, .count = checkpoint.count, .payload_bytes = checkpoint.payload_bytes };
    }
    pub fn append(self: *Builder, ordinal: u32, payload: []const u8) !void {
        if (ordinal != self.count) return error.InvalidBatchRequest;
        const next = std.math.add(u32, self.count, 1) catch return error.TransactionTooLarge;
        const bytes = std.math.add(u64, self.payload_bytes, payload.len) catch return error.TransactionTooLarge;
        var framing: [12]u8 = undefined;
        std.mem.writeInt(u32, framing[0..4], ordinal, .little);
        std.mem.writeInt(u64, framing[4..12], payload.len, .little);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&self.digest);
        hash.update(&framing);
        hash.update(payload);
        hash.final(&self.digest);
        self.count = next;
        self.payload_bytes = bytes;
    }
    pub fn finish(self: Builder) Manifest {
        return .{ .count = self.count, .payload_bytes = self.payload_bytes, .content_digest = self.digest };
    }
};

/// One owned encoding shared by persistence, projection, and publication. JSON
/// construction uses a reusable per-row scratch arena; only keys and encoded
/// payloads survive preparation. Failed preparation publishes nothing.
pub const PreparedRows = struct {
    alloc: std.mem.Allocator,
    mutations: []publication.Mutation,
    /// Projection-only preparation is not a complete stored output set.
    manifest: ?Manifest,

    pub fn init(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, source_field: []const u8, chunks: []const @import("../../chunking/chunk.zig").Chunk) !PreparedRows {
        return prepare(alloc, document, producer, source_field, chunks, false);
    }

    pub fn initProjection(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, source_field: []const u8, chunks: []const @import("../../chunking/chunk.zig").Chunk) !PreparedRows {
        return prepare(alloc, document, producer, source_field, chunks, true);
    }

    fn prepare(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, source_field: []const u8, chunks: []const @import("../../chunking/chunk.zig").Chunk, text_only: bool) !PreparedRows {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const rows = try alloc.alloc(publication.Mutation, chunks.len);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |row| {
                if (row.key.len != 0) alloc.free(row.key);
                if (row.value) |value| alloc.free(value);
            }
            alloc.free(rows);
        }
        var builder = Builder.init();
        for (chunks, rows, 0..) |chunk, *row, ordinal| {
            if (chunk.chunk_id != ordinal) return error.InvalidBatchRequest;
            if (text_only and !chunk.isText()) {
                row.* = .{ .family = .document_artifact, .key = "", .value = null, .source_index = 0 };
                initialized += 1;
                continue;
            }
            // Retaining scratch can consolidate arena buffers. Propagate a
            // failed consolidation instead of hiding admission exhaustion;
            // reset before the next row, never after the final useful row.
            if (!scratch.reset(.retain_capacity)) return error.OutOfMemory;
            const temporary = scratch.allocator();
            var obj = std.json.ObjectMap.empty;
            try obj.put(temporary, "_parent_doc_key", .{ .string = document });
            try obj.put(temporary, "_artifact_name", .{ .string = producer });
            try obj.put(temporary, "_source_field", .{ .string = source_field });
            try @import("../../chunking/chunk.zig").appendArtifactFields(temporary, &obj, source_field, chunk, true);
            const payload = try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
            errdefer alloc.free(payload);
            if (!text_only) try builder.append(chunk.chunk_id, payload);
            row.* = .{ .family = .document_artifact, .key = try keys.chunkArtifactKeyAlloc(alloc, document, producer, chunk.chunk_id), .value = payload, .source_index = 0 };
            initialized += 1;
        }
        return .{ .alloc = alloc, .mutations = rows, .manifest = if (text_only) null else builder.finish() };
    }

    /// Transfer to an independently owned replay document without copying the
    /// payload. Call only after persistence/publication consumed the full set.
    pub fn takeRow(self: *PreparedRows, ordinal: usize) publication.Mutation {
        const row = self.mutations[ordinal];
        std.debug.assert(row.value != null);
        self.mutations[ordinal].key = "";
        self.mutations[ordinal].value = null;
        return row;
    }

    pub fn deinit(self: *PreparedRows) void {
        for (self.mutations) |row| {
            if (row.key.len != 0) self.alloc.free(row.key);
            if (row.value) |value| self.alloc.free(value);
        }
        self.alloc.free(self.mutations);
        self.* = undefined;
    }
};

test "ordered artifact inventory chunk preparation owns canonical rows through allocation faults" {
    const alloc = std.testing.allocator;
    const Chunk = @import("../../chunking/chunk.zig").Chunk;
    const chunks = [_]Chunk{
        .{ .chunk_id = 0, .text = @constCast("hello"), .start_offset = 0, .end_offset = 5 },
        .{ .chunk_id = 1, .mime_type = "image/png", .data = @constCast("\x00\xff") },
    };
    var rows = try PreparedRows.init(alloc, "doc", "chunks", "body", &chunks);
    defer rows.deinit();
    try std.testing.expectEqual(@as(u32, 2), rows.manifest.?.count);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, rows.mutations[0].value.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("hello", parsed.value.object.get("body").?.string);
    try std.testing.expectEqualStrings("doc", parsed.value.object.get("_parent_doc_key").?.string);
    try validateReplacement(alloc, "doc", "chunks", Builder.init().finish(), rows.manifest.?, rows.mutations);
    const Check = struct {
        fn run(a: std.mem.Allocator, input: []const Chunk) !void {
            var prepared = try PreparedRows.init(a, "doc", "chunks", "body", input);
            defer prepared.deinit();
            var projection = try PreparedRows.initProjection(a, "doc", "chunks", "body", input);
            defer projection.deinit();
            try std.testing.expect(projection.manifest == null);
            try std.testing.expectEqualStrings(prepared.mutations[0].value.?, projection.mutations[0].value.?);
            try std.testing.expect(projection.mutations[1].value == null);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{@as([]const Chunk, &chunks)});
    const moved = rows.takeRow(0);
    defer alloc.free(moved.key);
    defer alloc.free(moved.value.?);
    try std.testing.expect(rows.mutations[0].value == null);
    try std.testing.expectError(error.InvalidBatchRequest, PreparedRows.init(alloc, "doc", "chunks", "body", chunks[1..]));
}

/// Validate the complete replacement, including retirement of the old tail.
/// `previous` must come from the manifest read in the writer transaction, not
/// a worker's guessed count. No artifact body/prefix scan occurs at commit.
/// This bounded atomic-publication validator complements streamed construction;
/// larger output sets need the staged-generation publication path.
pub fn validateReplacement(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, previous: Manifest, next: Manifest, effects: []const publication.Mutation) !void {
    return validateScopedReplacement(alloc, document, producer, null, previous, next, effects);
}

/// Units own independent ordinal spaces. Bind the unit even for an empty
/// replacement: another unit's matching count/hash cannot retire this scope.
pub fn validateScopedReplacement(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, unit: ?[]const u8, previous: Manifest, next: Manifest, effects: []const publication.Mutation) !void {
    if (document.len == 0 or producer.len == 0 or (unit != null and unit.?.len == 0)) return error.InvalidBatchRequest;
    const length = @max(previous.count, next.count);
    if (length > publication.max_mutations or effects.len != length) return error.InvalidBatchRequest;
    const ordered = try alloc.alloc(?[]const u8, length);
    defer alloc.free(ordered);
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(alloc, length);
    defer seen.deinit(alloc);
    for (effects) |effect| {
        if (effect.family != .document_artifact) return error.InvalidBatchRequest;
        var identity = (try ids.decodeArtifactRefAlloc(alloc, effect.key)) orelse return error.InvalidBatchRequest;
        defer identity.deinit(alloc);
        if (identity.kind != .chunk or !std.mem.eql(u8, identity.document_id, document) or !std.mem.eql(u8, identity.name, producer)) return error.InvalidBatchRequest;
        if (unit) |selected| {
            if (identity.unit_id == null or !std.mem.eql(u8, identity.unit_id.?, selected)) return error.InvalidBatchRequest;
        } else if (identity.unit_id != null) return error.InvalidBatchRequest;
        const ordinal = identity.chunk_id orelse return error.InvalidBatchRequest;
        if (ordinal >= length or seen.isSet(ordinal) or (ordinal < next.count) != (effect.value != null)) return error.InvalidBatchRequest;
        seen.set(ordinal);
        ordered[ordinal] = effect.value;
    }
    var builder = Builder.init();
    for (ordered[0..next.count], 0..) |value, ordinal| try builder.append(@intCast(ordinal), value orelse return error.InvalidBatchRequest);
    if (!std.meta.eql(builder.finish(), next)) return error.InvalidBatchRequest;
}

/// Heavy payload validation runs before apply. The final writer does one
/// manifest point-read and installs the inventory in the SAME transaction as
/// its already-prepared member effects. Missing inventory is a baseline gap,
/// never permission to assume an existing stream has no chunks.
pub const PreparedReplacement = struct {
    alloc: std.mem.Allocator,
    key: []u8,
    previous: [encoded_len]u8,
    next: [encoded_len]u8,

    pub fn init(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, previous: Manifest, next: Manifest, effects: []const publication.Mutation) !PreparedReplacement {
        return initScoped(alloc, document, producer, null, previous, next, effects);
    }
    pub fn initScoped(alloc: std.mem.Allocator, document: []const u8, producer: []const u8, unit: ?[]const u8, previous: Manifest, next: Manifest, effects: []const publication.Mutation) !PreparedReplacement {
        try validateScopedReplacement(alloc, document, producer, unit, previous, next, effects);
        return .{ .alloc = alloc, .key = try scopedKeyAlloc(alloc, document, producer, unit), .previous = previous.encode(), .next = next.encode() };
    }
    pub fn deinit(self: *PreparedReplacement) void {
        self.alloc.free(self.key);
        self.* = undefined;
    }
    pub fn requireCurrent(self: *const PreparedReplacement, txn: anytype) !void {
        const current = txn.get(self.key) catch |err| {
            if (err == error.NotFound) return error.ArtifactCoverageBaselinePending;
            return err;
        };
        _ = try Manifest.decode(current);
        if (!std.mem.eql(u8, current, &self.previous)) return error.EnrichmentSourceChanged;
    }
    pub fn stage(self: *const PreparedReplacement, txn: anytype) !void {
        try self.requireCurrent(txn);
        try txn.put(self.key, &self.next);
    }
};

test "ordered artifact inventory unit chunk manifests isolate sibling ordinal spaces and retire exact tails" {
    const alloc = std.testing.allocator;
    const unit = "unit\x00\xff";
    const zero = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", unit, 0);
    defer alloc.free(zero);
    const one = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", unit, 1);
    defer alloc.free(one);
    var old = Builder.init();
    try old.append(0, "old zero");
    try old.append(1, "old one");
    var next = Builder.init();
    try next.append(0, "new zero");
    const effects = [_]publication.Mutation{
        .{ .family = .document_artifact, .key = zero, .value = "new zero", .source_index = 0 },
        .{ .family = .document_artifact, .key = one, .value = null, .source_index = 0 },
    };
    var prepared = try PreparedReplacement.initScoped(alloc, "doc", "chunks", unit, old.finish(), next.finish(), &effects);
    defer prepared.deinit();
    const expected_key = try scopedKeyAlloc(alloc, "doc", "chunks", unit);
    defer alloc.free(expected_key);
    try std.testing.expectEqualStrings(expected_key, prepared.key);
    try std.testing.expectError(error.InvalidBatchRequest, validateReplacement(alloc, "doc", "chunks", old.finish(), next.finish(), &effects));
    try std.testing.expectError(error.InvalidBatchRequest, validateScopedReplacement(alloc, "doc", "chunks", "sibling", old.finish(), next.finish(), &effects));
    try std.testing.expectError(error.InvalidBatchRequest, validateScopedReplacement(alloc, "doc", "chunks", unit, old.finish(), next.finish(), effects[0..1]));
    try std.testing.expectError(error.InvalidBatchRequest, validateScopedReplacement(alloc, "doc", "chunks", "", Builder.init().finish(), Builder.init().finish(), &.{}));
    const Check = struct {
        fn run(a: std.mem.Allocator, previous: Manifest, desired: Manifest, mutations: []const publication.Mutation) !void {
            var result = try PreparedReplacement.initScoped(a, "doc", "chunks", unit, previous, desired, mutations);
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ old.finish(), next.finish(), @as([]const publication.Mutation, &effects) });
}

test "ordered artifact inventory chunk manifests validate complete replacement and exact retirement" {
    const alloc = std.testing.allocator;
    const manifest_key = try keyAlloc(alloc, "doc\x00\xff", "chunks");
    defer alloc.free(manifest_key);
    try std.testing.expect(isKey(manifest_key));
    try std.testing.expectEqual(@import("../artifact_footprint.zig").Family.generated, @import("../artifact_footprint.zig").classify(manifest_key).?);
    try std.testing.expect(publication.requiresOrderedMaterialization(manifest_key));
    try std.testing.expect(!isKey(manifest_key[0 .. manifest_key.len - 1]));
    try std.testing.expect((try ids.decodeArtifactRefAlloc(alloc, manifest_key)) == null);
    var old = Builder.init();
    try old.append(0, "old zero");
    try old.append(1, "old one");
    try old.append(2, "old two");
    var next = Builder.init();
    try next.append(0, "new\xff");
    try std.testing.expectError(error.InvalidBatchRequest, next.append(2, "gap"));
    const encoded = next.finish().encode();
    try std.testing.expectEqualDeep(next.finish(), try Manifest.decode(&encoded));
    var corrupt = encoded;
    corrupt[17] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Manifest.decode(&corrupt));
    var owned_keys: [3][]u8 = undefined;
    var initialized: usize = 0;
    defer for (owned_keys[0..initialized]) |key| alloc.free(key);
    for (&owned_keys, 0..) |*key, ordinal| {
        key.* = try keys.chunkArtifactKeyAlloc(alloc, "doc\x00\xff", "chunks", @intCast(ordinal));
        initialized += 1;
    }
    var effects = [_]publication.Mutation{
        .{ .family = .document_artifact, .key = owned_keys[2], .value = null, .source_index = 0 },
        .{ .family = .document_artifact, .key = owned_keys[0], .value = "new\xff", .source_index = 0 },
        .{ .family = .document_artifact, .key = owned_keys[1], .value = null, .source_index = 0 },
    };
    try validateReplacement(alloc, "doc\x00\xff", "chunks", old.finish(), next.finish(), &effects);
    try std.testing.expectError(error.InvalidBatchRequest, validateReplacement(alloc, "doc\x00\xff", "chunks", old.finish(), next.finish(), effects[0..2]));
    effects[2].key = owned_keys[2];
    try std.testing.expectError(error.InvalidBatchRequest, validateReplacement(alloc, "doc\x00\xff", "chunks", old.finish(), next.finish(), &effects));
    effects[2].key = owned_keys[1];
    effects[1].value = "different";
    try std.testing.expectError(error.InvalidBatchRequest, validateReplacement(alloc, "doc\x00\xff", "chunks", old.finish(), next.finish(), &effects));
    effects[1].value = "new\xff";
    try std.testing.checkAllAllocationFailures(alloc, validateReplacement, .{ "doc\x00\xff", "chunks", old.finish(), next.finish(), &effects });
    const Check = struct {
        fn prepare(a: std.mem.Allocator, previous: Manifest, desired: Manifest, mutations: []const publication.Mutation) !void {
            var prepared = try PreparedReplacement.init(a, "doc\x00\xff", "chunks", previous, desired, mutations);
            defer prepared.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.prepare, .{ old.finish(), next.finish(), &effects });
    var prepared = try PreparedReplacement.init(alloc, "doc\x00\xff", "chunks", old.finish(), next.finish(), &effects);
    defer prepared.deinit();
    const Txn = struct {
        raw: ?[encoded_len]u8 = null,
        writes: usize = 0,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return if (self.raw) |*raw| raw else error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, raw: []const u8) !void {
            self.raw = raw[0..encoded_len].*;
            self.writes += 1;
        }
    };
    var txn: Txn = .{};
    try std.testing.expectError(error.ArtifactCoverageBaselinePending, prepared.stage(&txn));
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    txn.raw = old.finish().encode();
    try prepared.stage(&txn);
    try std.testing.expectEqualDeep(next.finish(), try Manifest.decode(&txn.raw.?));
    try std.testing.expectError(error.EnrichmentSourceChanged, prepared.stage(&txn));
    try std.testing.expectEqual(@as(usize, 1), txn.writes);
}

test "ordered artifact inventory chunk stream checkpoints resume exact binary unit sets" {
    const alloc = std.testing.allocator;
    var complete = Builder.init();
    var resumed = Builder.init();
    for (0..257) |ordinal| {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u64, &payload, ordinal, .little);
        try complete.append(@intCast(ordinal), &payload);
        try resumed.append(@intCast(ordinal), &payload);
        if (ordinal % 17 == 0) resumed = try Builder.fromCheckpoint(try Manifest.decode(&resumed.finish().encode()));
    }
    try std.testing.expectEqualDeep(complete.finish(), resumed.finish());
    var invalid = Builder.init().finish();
    invalid.content_digest[0] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Builder.fromCheckpoint(invalid));
    for ([_]?[]const u8{ null, "unit\x00\xff" }) |unit| {
        const manifest = try scopedKeyAlloc(alloc, "doc\x00\xff", "producer\x00", unit);
        defer alloc.free(manifest);
        try std.testing.expect(isKey(manifest));
        try std.testing.expect(!isKey(manifest[0 .. manifest.len - 1]));
        const member = if (unit) |id| try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc\x00\xff", "producer\x00", id, 256) else try keys.chunkArtifactKeyAlloc(alloc, "doc\x00\xff", "producer\x00", 256);
        defer alloc.free(member);
        const derived = (try keyForMemberAlloc(alloc, member)).?;
        defer alloc.free(derived);
        try std.testing.expectEqualStrings(manifest, derived);
        try std.testing.expect((try keyForMemberAlloc(alloc, manifest)) == null);
        const Check = struct {
            fn run(a: std.mem.Allocator, value: []const u8) !void {
                const key = (try keyForMemberAlloc(a, value)).?;
                defer a.free(key);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Check.run, .{member});
    }
}
