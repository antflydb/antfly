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

//! Receiver-local acceptance for explicit vector ingress. Preparation borrows
//! immutable postimages through commit; physical writes observe exact bytes.
//! An origin flag, imported record, or old source position cannot grant it.
const platform = @import("antfly_platform");
const std = @import("std");

const publication = @import("artifact_publication.zig");
const keys = @import("../internal_keys.zig");
const codec = @import("enrichment/artifact_codec.zig");
const prefix = "\x00\x00__artifact_publication__:authored:";
pub const Key = [prefix.len + 24 + 8 + 16 + 32]u8;
const record_len = 237;
const checksum_offset = record_len - 32;

fn digest(bytes: []const u8) publication.Digest {
    var result: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}

fn key(authority: publication.Authority, root: u128, artifact: []const u8) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, result[prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, result[prefix.len + 32 ..][0..16], root, .big);
    @memcpy(result[result.len - 32 ..], &digest(artifact));
    return result;
}

fn recordDigest(selected: *const Key, raw: []const u8) publication.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:authored-acceptance:v2:");
    hash.update(selected);
    hash.update(raw);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

const Vector = struct {
    document: []const u8,
    artifact: []const u8,
    primary: []const u8,
    ttl: []const u8,
    source_digest: publication.Digest,
    output_digest: publication.Digest,
    timestamp: u64,
};
const Postimage = struct { bytes: []const u8, value_digest: publication.Digest, matched: bool = false };

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    vectors: []const Vector,
    postimages: std.StringHashMapUnmanaged(Postimage),

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn remember(self: *Prepared, alloc: std.mem.Allocator, selected: []const u8, bytes: []const u8) !publication.Digest {
        if (self.postimages.get(selected)) |existing| return existing.value_digest;
        const value_digest = digest(bytes);
        try self.postimages.put(alloc, selected, .{ .bytes = bytes, .value_digest = value_digest });
        return value_digest;
    }

    /// `authored` comes ONLY from explicit ingress extraction, never from a
    /// scan of arbitrary stored/imported bytes. Both slices outlive commit.
    /// Last postimages win, including repeated row keys in the input batch.
    pub fn init(alloc: std.mem.Allocator, root: u128, authored: anytype, writes: anytype) !Prepared {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var result: Prepared = .{ .arena = undefined, .root = root, .vectors = &.{}, .postimages = .empty };
        if (root != 0 and authored.len != 0) {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            var final: std.StringHashMapUnmanaged([]const u8) = .empty;
            for (writes) |write| try final.put(scratch.allocator(), write.key, write.value);
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            var vectors: std.ArrayListUnmanaged(Vector) = .empty;
            for (authored) |write| {
                if (seen.contains(write.key)) continue;
                const value = final.get(write.key) orelse continue;
                const header = try codec.decodeHeader(value);
                if (!header.flags.authored) continue;
                const parsed = (try keys.parseEmbeddingArtifactKeyAlloc(a, write.key)) orelse return error.InvalidBatchRequest;
                const document_key = try keys.documentKeyAlloc(a, parsed.doc_key);
                const row_key = try keys.relationalRowKeyAlloc(a, parsed.doc_key);
                if (final.contains(document_key) and final.contains(row_key)) return error.EnrichmentSourceChanged;
                const primary = if (final.contains(document_key)) document_key else row_key;
                const source = final.get(primary) orelse continue;
                const ttl = try keys.ttlKeyAlloc(a, parsed.doc_key);
                const timestamp_bytes = final.get(ttl) orelse continue;
                if (timestamp_bytes.len != 8) return error.InvalidBatchRequest;
                const timestamp = std.mem.readInt(u64, timestamp_bytes[0..8], .little);
                if (timestamp == 0) return error.InvalidBatchRequest;
                try seen.put(scratch.allocator(), write.key, {});
                const source_digest = try result.remember(a, primary, source);
                _ = try result.remember(a, ttl, timestamp_bytes);
                const output_digest = try result.remember(a, write.key, value);
                try vectors.append(a, .{ .document = parsed.doc_key, .artifact = write.key, .primary = primary, .ttl = ttl, .source_digest = source_digest, .output_digest = output_digest, .timestamp = timestamp });
            }
            result.vectors = try vectors.toOwnedSlice(a);
        }
        result.arena = arena;
        return result;
    }

    /// A writer retry starts fresh. Merely preparing or enqueueing a batch is
    /// not evidence that any of its postimages reached the transaction.
    pub fn reset(self: *Prepared) void {
        var values = self.postimages.valueIterator();
        while (values.next()) |value| value.matched = false;
    }

    pub fn participant(self: *Prepared) @import("../commit_participant.zig").Participant {
        const Bridge = struct {
            fn reset(ptr: *anyopaque) void {
                const prepared: *Prepared = @ptrCast(@alignCast(ptr));
                prepared.reset();
            }
            fn observe(ptr: *anyopaque, selected: []const u8, value: ?[]const u8) void {
                const prepared: *Prepared = @ptrCast(@alignCast(ptr));
                prepared.observe(selected, value);
            }
            fn stage(ptr: *anyopaque, view: @import("../commit_participant.zig").View, encoded_position: []const u8) !void {
                const prepared: *Prepared = @ptrCast(@alignCast(ptr));
                const authority = (try publication.authority(view)) orelse return error.ArtifactCatalogDrift;
                const position = try publication.Position.decode(encoded_position);
                try position.requireNamespace(publication.namespaceFromBytes(authority.namespace));
                try prepared.stage(view, authority, position);
            }
        };
        return .{ .ptr = self, .reset = Bridge.reset, .observe = Bridge.observe, .stage = Bridge.stage };
    }

    pub fn observe(self: *Prepared, selected: []const u8, value: ?[]const u8) void {
        if (self.postimages.getPtr(selected)) |expected|
            expected.matched = if (value) |actual| std.mem.eql(u8, expected.bytes, actual) else false;
    }

    /// Called ONLY by input capture after it stamps revisions for the actual
    /// physical transaction. No payload hashing or reads in the commit hook.
    pub fn stage(self: *const Prepared, txn: anytype, authority: publication.Authority, position: publication.Position) !void {
        if (self.root == 0) return;
        const encoded_position = try position.encode();
        for (self.vectors) |vector| {
            if (!self.postimages.get(vector.primary).?.matched or !self.postimages.get(vector.ttl).?.matched or
                !self.postimages.get(vector.artifact).?.matched) continue;
            var raw: [record_len]u8 = undefined;
            @memcpy(raw[0..4], "ANA2");
            @memcpy(raw[4..36], &authority.catalog_digest);
            @memcpy(raw[36..68], &digest(vector.document));
            @memcpy(raw[68..100], &vector.source_digest);
            std.mem.writeInt(u64, raw[100..108], vector.timestamp, .little);
            @memcpy(raw[108..141], &encoded_position);
            @memcpy(raw[141..173], &vector.output_digest);
            const revision_key = publication.artifactRevisionKey(authority.namespace, vector.artifact);
            @memcpy(raw[173..205], revision_key[revision_key.len - 32 ..]);
            const selected = key(authority, self.root, vector.artifact);
            @memcpy(raw[checksum_offset..], &recordDigest(&selected, raw[0..checksum_offset]));
            try txn.put(&selected, &raw);
        }
    }
};

pub const Accepted = struct {
    source: publication.Source,
    output_digest: publication.Digest,
    reference: Key,
    record_digest: publication.Digest,
    record_bytes: usize = record_len,

    pub fn matchesSource(self: Accepted, source: publication.Source) bool {
        return self.source.exists == source.exists and
            std.mem.eql(u8, self.source.document_key, source.document_key) and
            std.mem.eql(u8, &self.source.content_digest, &source.content_digest) and
            self.source.timestamp == source.timestamp and
            std.meta.eql(self.source.input_position, source.input_position);
    }
};

pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, prefix, 48, 48, if (root == 0) null else &identity);
}

/// Reads local acceptance, not transferable provenance. The caller retains
/// the pinned snapshot and document slice; adoption must issue new evidence.
pub fn readCurrent(txn: anytype, root: u128, document: []const u8, artifact: []const u8) !?Accepted {
    if (root == 0) return null;
    const authority = (try publication.authority(txn)) orelse return null;
    const selected = key(authority, root, artifact);
    const raw = txn.get(&selected) catch |err| if (err == error.NotFound) return null else return err;
    try validateRecord(&selected, raw);
    const revision_key = publication.artifactRevisionKey(authority.namespace, artifact);
    if (!std.mem.eql(u8, raw[173..205], revision_key[revision_key.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, raw[4..36], &authority.catalog_digest) or !std.mem.eql(u8, raw[36..68], &digest(document))) return null;
    const position = try publication.Position.decode(raw[108..141]);
    const input = (try publication.inputRevision(txn, authority.namespace, document)) orelse return null;
    const output = (try publication.artifactRevision(txn, authority.namespace, artifact)) orelse return null;
    if (!std.meta.eql(position, input) or !std.meta.eql(position, output)) return null;
    return .{ .source = .{ .document_key = document, .content_digest = raw[68..100].*, .timestamp = std.mem.readInt(u64, raw[100..108], .little), .input_position = position }, .output_digest = raw[141..173].*, .reference = selected, .record_digest = raw[checksum_offset..][0..32].* };
}

fn validateRecord(selected: *const Key, raw: []const u8) !void {
    if (raw.len != record_len or !std.mem.eql(u8, raw[0..4], "ANA2") or
        !std.mem.eql(u8, raw[checksum_offset..], &recordDigest(selected, raw[0..checksum_offset]))) return error.ArtifactCatalogCorrupt;
    const position = publication.Position.decode(raw[108..141]) catch return error.ArtifactCatalogCorrupt;
    try position.requireNamespace(publication.namespaceFromBytes(selected[prefix.len..][0..24].*));
}

const gc_cursor_key = "\x00\x00__artifact_publication__:authored-gc";
const GcCursor = [181]u8;

fn mutationStamp(txn: anytype, authority: publication.Authority) !publication.Digest {
    const owners = @import("../source_authority.zig");
    const owner = (try owners.load(txn)) orelse return error.OnlineSourceScopeChanged;
    if (!std.mem.eql(u8, &owner.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    _ = try owners.require(txn, owner.kind, authority.namespace);
    var raw: [owners.encoded_size + 17]u8 = @splat(0);
    @memcpy(raw[0..owners.encoded_size], &owner.encode());
    if (owner.kind == .raft) {
        const marker = txn.get(&keys.ordered_document_applied_entry_key) catch |err| if (err == error.NotFound) null else return err;
        if (marker) |value| {
            if (value.len != 16) return error.ArtifactCatalogCorrupt;
            const position: publication.Position = .{ .raft = .{ .term = std.mem.readInt(u64, value[0..8], .little), .index = std.mem.readInt(u64, value[8..16], .little) } };
            position.validate() catch return error.ArtifactCatalogCorrupt;
            raw[owners.encoded_size] = 1;
            @memcpy(raw[owners.encoded_size + 1 ..], value);
        }
    }
    return digest(&raw);
}

fn encodeGcCursor(authority: publication.Authority, root: u128, after: [32]u8, stamp: publication.Digest, complete: bool) GcCursor {
    var raw: GcCursor = undefined;
    @memcpy(raw[0..4], "AGC2");
    @memcpy(raw[4..28], &authority.namespace);
    std.mem.writeInt(u64, raw[28..36], authority.epoch, .big);
    @memcpy(raw[36..68], &authority.catalog_digest);
    std.mem.writeInt(u128, raw[68..84], root, .big);
    @memcpy(raw[84..116], &after);
    @memcpy(raw[116..148], &stamp);
    raw[148] = @intFromBool(complete);
    @memcpy(raw[149..181], &digest(raw[0..149]));
    return raw;
}

fn loadGcCursor(txn: anytype) !?GcCursor {
    const raw = txn.get(gc_cursor_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != @sizeOf(GcCursor) or !std.mem.eql(u8, raw[0..4], "AGC2") or raw[148] > 1 or
        (raw[148] == 1 and !std.mem.allEqual(u8, raw[84..116], 0)) or
        !std.mem.eql(u8, raw[149..181], &digest(raw[0..149]))) return error.ArtifactCatalogCorrupt;
    return raw[0..@sizeOf(GcCursor)].*;
}

fn outputIsCurrent(txn: anytype, authority: publication.Authority, record: []const u8) !bool {
    var revision_key = publication.artifactRevisionKey(authority.namespace, "");
    @memcpy(revision_key[revision_key.len - 32 ..], record[173..205]);
    const raw = txn.get(&revision_key) catch |err| if (err == error.NotFound) return false else return err;
    const position = publication.Position.decode(raw) catch return error.ArtifactCatalogCorrupt;
    try position.requireNamespace(publication.namespaceFromBytes(authority.namespace));
    return std.mem.eql(u8, raw, record[108..141]);
}

pub const RetirementLimits = struct { visits: usize = 128, bytes: usize = 64 * 1024 };
pub const PreparedRetirement = struct {
    arena: std.heap.ArenaAllocator,
    authority: publication.Authority,
    root: u128,
    sweep_stamp: publication.Digest,
    expected_cursor: ?GcCursor,
    next_cursor: ?GcCursor,
    candidates: []const Candidate,
    visits: usize,
    pub const Candidate = struct { selected: Key, record: [record_len]u8 };

    pub fn deinit(self: *PreparedRetirement) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn atEnd(self: *const PreparedRetirement) bool {
        return self.next_cursor == null;
    }

    pub fn needsWrite(self: *const PreparedRetirement) bool {
        return self.candidates.len != 0 or self.expected_cursor == null or !std.mem.eql(u8, &self.expected_cursor.?, &self.nextValue());
    }

    fn nextValue(self: *const PreparedRetirement) GcCursor {
        return self.next_cursor orelse encodeGcCursor(self.authority, self.root, @splat(0), self.sweep_stamp, true);
    }

    /// Final writer repeats exact-record and output-revision checks. A renewal
    /// racing preparation must never lose its current certificate. Source-only
    /// changes deliberately do not retire an artifact's authoring evidence.
    pub fn stage(self: *const PreparedRetirement, txn: anytype, actual_root: u128) !bool {
        if (actual_root != self.root) return error.EnrichmentSourceChanged;
        const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(authority, self.authority)) return error.ArtifactCatalogDrift;
        const cursor = try loadGcCursor(txn);
        if (!std.meta.eql(cursor, self.expected_cursor)) return error.EnrichmentSourceChanged;
        for (self.candidates) |candidate| {
            const current = txn.get(&candidate.selected) catch |err| if (err == error.NotFound) continue else return err;
            if (!std.mem.eql(u8, current, &candidate.record)) continue;
            if (try outputIsCurrent(txn, authority, &candidate.record)) continue;
            try txn.delete(&candidate.selected);
        }
        const next = self.nextValue();
        if (cursor == null or !std.mem.eql(u8, &cursor.?, &next)) {
            try txn.put(gc_cursor_key, &next);
        }
        return self.atEnd() and std.mem.eql(u8, &self.sweep_stamp, &try mutationStamp(txn, authority));
    }
};

/// One cold, bounded pass over local certificates, not vector payloads. The
/// durable cursor advances across LIVE records too, so a live prefix cannot
/// starve cleanup after it. Completed sweeps are cached only at the same source
/// clock. A mutation during a sweep forces another pass, including keys before
/// its cursor, without restarting every partial page under write load.
pub fn prepareRetirementPage(alloc: std.mem.Allocator, store: anytype, root: u128, limits: RetirementLimits) !?PreparedRetirement {
    if (root == 0) return null;
    if (limits.visits == 0 or limits.visits > 128 or limits.bytes == 0 or limits.bytes > 64 * 1024) return error.InvalidArgument;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const authority = (try publication.authority(&read)) orelse {
        arena.deinit();
        return null;
    };
    const expected = try loadGcCursor(&read);
    const current_stamp = try mutationStamp(&read, authority);
    var sweep_stamp = current_stamp;
    var start = key(authority, root, "");
    const scope = start[0 .. start.len - 32];
    var has_after = false;
    if (expected) |previous| {
        const bound = encodeGcCursor(authority, root, previous[84..116].*, previous[116..148].*, previous[148] == 1);
        if (std.mem.eql(u8, &previous, &bound)) {
            if (previous[148] == 1) {
                if (std.mem.eql(u8, previous[116..148], &current_stamp))
                    return .{ .arena = arena, .authority = authority, .root = root, .sweep_stamp = current_stamp, .expected_cursor = expected, .next_cursor = null, .candidates = &.{}, .visits = 0 };
            } else {
                @memcpy(start[start.len - 32 ..], previous[84..116]);
                sweep_stamp = previous[116..148].*;
                has_after = true;
            }
        }
    }
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(if (has_after) &start else scope);
    if (has_after) if (entry) |item| if (std.mem.eql(u8, item.key, &start)) {
        entry = try cursor.next();
    };
    var candidates: std.ArrayListUnmanaged(PreparedRetirement.Candidate) = .empty;
    var visits: usize = 0;
    var bytes: usize = 0;
    var after: [32]u8 = undefined;
    const deadline = platform.time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, scope)) {
            entry = null;
            break;
        }
        if (item.key.len != @sizeOf(Key)) return error.ArtifactCatalogCorrupt;
        if (visits != 0 and (visits >= limits.visits or bytes +| @sizeOf(PreparedRetirement.Candidate) > limits.bytes or
            platform.time.monotonicNs() >= deadline)) break;
        const selected = item.key[0..@sizeOf(Key)].*;
        try validateRecord(&selected, item.value);
        if (!try outputIsCurrent(&read, authority, item.value))
            try candidates.append(arena.allocator(), .{ .selected = selected, .record = item.value[0..record_len].* });
        visits += 1;
        bytes += @sizeOf(PreparedRetirement.Candidate);
        after = selected[selected.len - 32 ..].*;
        entry = try cursor.next();
    }
    return .{ .arena = arena, .authority = authority, .root = root, .sweep_stamp = sweep_stamp, .expected_cursor = expected, .next_cursor = if (entry != null) encodeGcCursor(authority, root, after, sweep_stamp, false) else null, .candidates = candidates.items, .visits = visits };
}

pub fn collectCurrentPage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    var page = (try prepareRetirementPage(alloc, store, root, .{})) orelse return true;
    defer page.deinit();
    if (page.needsWrite()) {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const complete = page.stage(&txn, root) catch |err| {
            if (err == error.EnrichmentSourceChanged) {
                txn.abort();
                return false;
            }
            return err;
        };
        try txn.commit();
        return complete;
    }
    return page.atEnd();
}

fn commitRetirementForTest(store: anytype, page: *const PreparedRetirement, root: u128) !void {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    _ = try page.stage(&txn, root);
    try txn.commit();
}

test "ordered artifact inventory authored retirement resumes past live prefixes and fences renewal" {
    const alloc = std.testing.allocator;
    const docstore = @import("../docstore.zig");
    const owner = @import("../source_authority.zig");
    const authority: publication.Authority = .{ .namespace = @splat(7), .epoch = 1, .catalog_digest = @splat(3) };
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPath(platform.testing.io, &path_buffer);
    const path = try alloc.dupeSentinel(u8, path_buffer[0..path_len], 0);
    defer alloc.free(path);
    var store = try docstore.DocStore.open(alloc, path, .{});
    defer store.close();
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        var txn = batch.asTxn();
        try owner.bind(&txn, .native, authority.namespace);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try batch.commit();
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const primary = try keys.documentKeyAlloc(a, "doc");
    const ttl = try keys.ttlKeyAlloc(a, "doc");
    const vector = try codec.encodeAuthoredDenseEmbeddingAlloc(a, &.{ 1, 2 });
    const count = 257;
    const live_prefix = 129;
    const Descriptor = struct {
        artifact: []const u8,
        certificate: Key,
        fn less(_: void, left: @This(), right: @This()) bool {
            return std.mem.lessThan(u8, &left.certificate, &right.certificate);
        }
    };
    const ordered = try a.alloc(Descriptor, count);
    const writes = try a.alloc(docstore.KVPair, count + 2);
    writes[0] = .{ .key = primary, .value = "{}" };
    writes[1] = .{ .key = ttl, .value = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } };
    for (ordered, 0..) |*item, i| {
        const name = try std.fmt.allocPrint(a, "vector-{d}", .{i});
        const artifact = try keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", name);
        item.* = .{ .artifact = artifact, .certificate = key(authority, 41, artifact) };
        writes[i + 2] = .{ .key = artifact, .value = vector };
    }
    std.mem.sort(Descriptor, ordered, {}, Descriptor.less);
    var ingress = try Prepared.init(alloc, 41, writes[2..], writes);
    defer ingress.deinit();
    try store.putBatchWithReplayAndParticipant(null, writes, &.{}, null, .{}, ingress.participant());
    const deletes = try a.alloc([]const u8, count - live_prefix);
    for (ordered[live_prefix..], deletes) |item, *deleted| deleted.* = item.artifact;
    try store.putBatch(&.{}, deletes);
    // Primary-only changes invalidate current acceptance, but are not proof
    // that a still-current vector's original authoring evidence is garbage.
    try store.put(primary, "{\"changed\":true}");
    var first = (try prepareRetirementPage(alloc, &store, 41, .{ .visits = 17 })).?;
    defer first.deinit();
    var competing = (try prepareRetirementPage(alloc, &store, 41, .{ .visits = 17 })).?;
    defer competing.deinit();
    try std.testing.expect(first.visits > 0 and first.visits <= 17);
    try std.testing.expectEqual(@as(usize, 0), first.candidates.len);
    try std.testing.expect(!first.atEnd() and first.needsWrite());
    try std.testing.expectError(error.EnrichmentSourceChanged, commitRetirementForTest(&store, &first, 42));
    try commitRetirementForTest(&store, &first, 41);
    try std.testing.expectError(error.EnrichmentSourceChanged, commitRetirementForTest(&store, &competing, 41));
    var visited = first.visits;
    // Only the cursor is durable; no plan, callback or read handle survives.
    store.close();
    store = try docstore.DocStore.open(alloc, path, .{});
    var pinned = try store.beginReadTxn();
    defer pinned.abort();
    var renewed: ?Key = null;
    var finished = false;
    for (0..512) |_| {
        var page = (try prepareRetirementPage(alloc, &store, 41, .{ .visits = 17 })).?;
        defer page.deinit();
        try std.testing.expect(page.visits <= 17);
        if (renewed == null and page.candidates.len != 0) {
            const selected = page.candidates[0].selected;
            {
                var txn = try store.beginWriteTxn();
                defer txn.abort();
                _ = try page.stage(&txn, 41);
                try std.testing.expectError(error.NotFound, txn.get(&selected));
            }
            {
                var read = try store.beginReadTxn();
                defer read.abort();
                try std.testing.expectEqualDeep(page.expected_cursor, try loadGcCursor(&read));
                _ = try read.get(&selected);
            }
            var artifact: ?[]const u8 = null;
            for (ordered) |item| if (std.mem.eql(u8, &item.certificate, &selected)) {
                artifact = item.artifact;
                break;
            };
            const refreshed = [_]docstore.KVPair{ writes[0], writes[1], .{ .key = artifact.?, .value = vector } };
            var renewal = try Prepared.init(alloc, 41, refreshed[2..], &refreshed);
            defer renewal.deinit();
            try store.putBatchWithReplayAndParticipant(null, &refreshed, &.{}, null, .{}, renewal.participant());
            renewed = selected;
        }
        if (page.needsWrite()) try commitRetirementForTest(&store, &page, 41);
        visited += page.visits;
        if (page.atEnd()) {
            finished = true;
            break;
        }
    }
    try std.testing.expect(finished and renewed != null);
    try std.testing.expectEqual(@as(usize, count), visited);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const completed = (try loadGcCursor(&read)).?;
        try std.testing.expectEqual(@as(u8, 1), completed[148]);
        // Renewal happened behind the cursor: this sweep cannot certify the
        // new mutation clock, even though all its original keys were visited.
        try std.testing.expect(!std.mem.eql(u8, completed[116..148], &try mutationStamp(&read, authority)));
        for (ordered, 0..) |item, i| {
            // A pinned reader still sees every retired certificate.
            _ = try pinned.get(&item.certificate);
            if (i < live_prefix or std.mem.eql(u8, &item.certificate, &renewed.?)) {
                _ = try read.get(&item.certificate);
            } else try std.testing.expectError(error.NotFound, read.get(&item.certificate));
        }
    }
    var settled = false;
    for (0..512) |_| {
        if (try collectCurrentPage(alloc, &store, 41)) {
            settled = true;
            break;
        }
    }
    try std.testing.expect(settled);
    var cached = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
    defer cached.deinit();
    try std.testing.expectEqual(@as(usize, 0), cached.visits);
    try std.testing.expectEqual(@as(usize, 0), cached.candidates.len);
    try std.testing.expect(cached.atEnd() and !cached.needsWrite());
    // A new source mutation invalidates the completed sweep in O(1), without
    // discarding still-live authoring provenance.
    try store.put(primary, "{\"changed\":2}");
    var bytes_page = (try prepareRetirementPage(alloc, &store, 41, .{ .bytes = 1 })).?;
    defer bytes_page.deinit();
    try std.testing.expectEqual(@as(usize, 1), bytes_page.visits);
    try std.testing.expect(!bytes_page.atEnd());
    try std.testing.expectError(error.InvalidArgument, prepareRetirementPage(alloc, &store, 41, .{ .visits = 0 }));
}

test "ordered artifact inventory authored retirement cache binds owner clock epoch and physical root" {
    const alloc = std.testing.allocator;
    const docstore = @import("../docstore.zig");
    const owner = @import("../source_authority.zig");
    const authority: publication.Authority = .{ .namespace = @splat(7), .epoch = 1, .catalog_digest = @splat(3) };
    for ([_]owner.Kind{ .native, .raft }) |kind| {
        var tmp = platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const path_len = try tmp.dir.realPath(platform.testing.io, &path_buffer);
        const path = try alloc.dupeSentinel(u8, path_buffer[0..path_len], 0);
        defer alloc.free(path);
        var store = try docstore.DocStore.open(alloc, path, .{});
        defer store.close();
        var activate: publication.Command = .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        {
            var batch = try store.beginWriteBatch();
            errdefer batch.abort();
            var txn = batch.asTxn();
            try owner.bind(&txn, kind, authority.namespace);
            try publication.stageAuthority(&txn, activate);
            try batch.commit();
        }
        try std.testing.expect(try collectCurrentPage(alloc, &store, 41));
        {
            var cached = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
            defer cached.deinit();
            try std.testing.expect(cached.atEnd() and !cached.needsWrite());
        }
        const primary = try keys.documentKeyAlloc(alloc, "doc");
        defer alloc.free(primary);
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 1, .little);
        const writes = [_]docstore.KVPair{ .{ .key = primary, .value = "{}" }, .{ .key = &keys.ordered_document_applied_entry_key, .value = &marker } };
        try store.putBatch(writes[0..@as(usize, if (kind == .raft) 2 else 1)], &.{});
        {
            var invalidated = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
            defer invalidated.deinit();
            try std.testing.expect(invalidated.atEnd() and invalidated.needsWrite());
            try commitRetirementForTest(&store, &invalidated, 41);
        }
        {
            var cached = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
            defer cached.deinit();
            try std.testing.expect(!cached.needsWrite());
            var other_root = (try prepareRetirementPage(alloc, &store, 42, .{})).?;
            defer other_root.deinit();
            try std.testing.expect(other_root.needsWrite());
            // A changed authority fences an already prepared retirement page.
            activate.authority_epoch += 1;
            var batch = try store.beginWriteBatch();
            errdefer batch.abort();
            var txn = batch.asTxn();
            try publication.stageAuthority(&txn, activate);
            try batch.commit();
            try std.testing.expectError(error.ArtifactCatalogDrift, commitRetirementForTest(&store, &other_root, 42));
        }
        {
            var new_epoch = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
            defer new_epoch.deinit();
            try std.testing.expect(new_epoch.needsWrite());
        }
        try std.testing.expect(try collectCurrentPage(alloc, &store, 41));
        store.close();
        store = try docstore.DocStore.open(alloc, path, .{});
        var reopened = (try prepareRetirementPage(alloc, &store, 41, .{})).?;
        defer reopened.deinit();
        try std.testing.expect(!reopened.needsWrite() and reopened.visits == 0);
    }
}

test "ordered artifact inventory authored acceptance requires exact physical writer and postimages" {
    const alloc = std.testing.allocator;
    const docstore = @import("../docstore.zig");
    const owner = @import("../source_authority.zig");
    const namespace: publication.Namespace = @splat(7);
    const authority: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = @splat(3) };
    for ([_]owner.Kind{ .native, .raft }) |kind| {
        var tmp = platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const path_len = try tmp.dir.realPath(platform.testing.io, &path_buffer);
        const path = try alloc.dupeSentinel(u8, path_buffer[0..path_len], 0);
        defer alloc.free(path);
        var store = try docstore.DocStore.open(alloc, path, .{});
        defer store.close();
        {
            var batch = try store.beginWriteBatch();
            errdefer batch.abort();
            var txn = batch.asTxn();
            try owner.bind(&txn, kind, namespace);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try batch.commit();
        }
        const primary = try keys.documentKeyAlloc(alloc, "doc");
        defer alloc.free(primary);
        const ttl = try keys.ttlKeyAlloc(alloc, "doc");
        defer alloc.free(ttl);
        const artifact = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "vector");
        defer alloc.free(artifact);
        const value = if (kind == .native)
            try codec.encodeAuthoredDenseEmbeddingAlloc(alloc, &.{ 1, 2 })
        else
            try codec.encodeAuthoredSparseEmbeddingAlloc(alloc, &.{ 3, 8 }, &.{ 1, 2 });
        defer alloc.free(value);
        var timestamp: [8]u8 = undefined;
        std.mem.writeInt(u64, &timestamp, 17, .little);
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 1, .little);
        const writes = [_]docstore.KVPair{ .{ .key = primary, .value = "{}" }, .{ .key = ttl, .value = &timestamp }, .{ .key = artifact, .value = value }, .{ .key = &keys.ordered_document_applied_entry_key, .value = &marker } };
        const actual = writes[0..@as(usize, if (kind == .raft) 4 else 3)];
        // Merely writing authored bytes through generic/import ingress does
        // not create acceptance, even with valid physical input revisions.
        try store.putBatch(actual, &.{});
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) == null);
        }
        var prepared = try Prepared.init(alloc, 41, writes[2..3], actual);
        defer prepared.deinit();
        {
            var batch = try store.beginWriteBatch();
            defer batch.abort();
            try batch.put(primary, "uncommitted");
            try std.testing.expectError(error.InvalidBatch, batch.setCommitParticipant(prepared.participant()));
        }
        {
            var batch = try store.beginWriteBatch();
            defer batch.abort();
            try batch.setCommitParticipant(prepared.participant());
            try std.testing.expectError(error.InvalidBatch, batch.setCommitParticipant(prepared.participant()));
            var txn = batch.asTxn();
            const view = @import("../commit_participant.zig").View.from(&txn);
            try std.testing.expectError(error.InvalidCommitMetadata, view.put(primary, "forbidden"));
        }
        std.mem.writeInt(u64, marker[8..16], 2, .little);
        try store.putBatchWithReplayAndParticipant(null, actual, &.{}, null, .{}, prepared.participant());
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const accepted = (try readCurrent(&read, 41, "doc", artifact)) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualSlices(u8, &digest("{}"), &accepted.source.content_digest);
            try std.testing.expectEqualSlices(u8, &digest(value), &accepted.output_digest);
            try std.testing.expectEqual(@as(u64, 17), accepted.source.timestamp);
            try std.testing.expect((try readCurrent(&read, 42, "doc", artifact)) == null);
            try std.testing.expect((try readCurrent(&read, 41, "other", artifact)) == null);
        }
        {
            const certificate = try store.get(alloc, &key(authority, 41, artifact));
            defer alloc.free(certificate);
            try store.put(&key(authority, 42, artifact), certificate);
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.ArtifactCatalogCorrupt, readCurrent(&read, 42, "doc", artifact));
        }
        var collected = false;
        for (0..8) |_| {
            if (try collectObsoletePage(alloc, &store, 41)) {
                collected = true;
                break;
            }
        }
        try std.testing.expect(collected);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) != null);
            try std.testing.expect((try readCurrent(&read, 42, "doc", artifact)) == null);
        }
        // An aborted attempt must not leak observed postimages into a retry.
        prepared.reset();
        for (actual) |write| prepared.observe(write.key, write.value);
        std.mem.writeInt(u64, marker[8..16], 3, .little);
        const partial = [_]docstore.KVPair{ writes[2], writes[3] };
        try store.putBatchWithReplayAndParticipant(null, partial[0..@as(usize, if (kind == .raft) 2 else 1)], &.{}, null, .{}, prepared.participant());
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) == null);
        }
        std.mem.writeInt(u64, marker[8..16], 4, .little);
        try store.putBatchWithReplayAndParticipant(null, actual, &.{}, null, .{}, prepared.participant());
        // Primary-only replacement invalidates acceptance without scanning
        // vectors or rewriting their acceptance records.
        std.mem.writeInt(u64, marker[8..16], 5, .little);
        const changed = [_]docstore.KVPair{ .{ .key = primary, .value = "{\"changed\":true}" }, writes[3] };
        try store.putBatch(changed[0..@as(usize, if (kind == .raft) 2 else 1)], &.{});
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) == null);
        }
        // Coordinated resolution crosses backend type erasure. Its decision,
        // row writes and acceptance must still share the same physical commit.
        const transactions = @import("../transactions.zig");
        var manager = try transactions.TxnManager.init(alloc, &store);
        defer manager.deinit();
        const transaction_id: transactions.TxnId = @splat(9);
        try manager.initTransaction(transaction_id, 100);
        std.mem.writeInt(u64, marker[8..16], 6, .little);
        var failing: FailingParticipant = .{ .inner = prepared.participant() };
        try std.testing.expectError(error.InjectedAcceptanceFailure, manager.resolveIntentsWithExtraBatch(transaction_id, .committed, 101, .{
            .writes = actual,
            .commit_participant = failing.participant(),
        }));
        try std.testing.expectEqual(transactions.TxnStatus.pending, try manager.getTransactionStatus(transaction_id));
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualSlices(u8, changed[0].value, try read.get(primary));
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) == null);
        }
        const resolved = try manager.resolveIntentsWithExtraBatch(transaction_id, .committed, 101, .{
            .writes = actual,
            .commit_participant = prepared.participant(),
        });
        try std.testing.expect(resolved.applied);
        try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(transaction_id));
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) != null);
        }
        // Lost-reply retry after a newer primary write cannot replay old
        // postimages or attach their participant to the terminal decision.
        std.mem.writeInt(u64, marker[8..16], 7, .little);
        try store.putBatch(changed[0..@as(usize, if (kind == .raft) 2 else 1)], &.{});
        prepared.reset();
        const retried = try manager.resolveIntentsWithExtraBatch(transaction_id, .committed, 101, .{
            .writes = actual,
            .commit_participant = prepared.participant(),
        });
        try std.testing.expect(!retried.applied);
        try std.testing.expect(!prepared.postimages.get(primary).?.matched);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualSlices(u8, changed[0].value, try read.get(primary));
            try std.testing.expect((try readCurrent(&read, 41, "doc", artifact)) == null);
        }
    }
}

const FailingParticipant = struct {
    inner: @import("../commit_participant.zig").Participant,
    fn reset(ptr: *anyopaque) void {
        const self: *FailingParticipant = @ptrCast(@alignCast(ptr));
        self.inner.reset(self.inner.ptr);
    }
    fn observe(ptr: *anyopaque, selected: []const u8, value: ?[]const u8) void {
        const self: *FailingParticipant = @ptrCast(@alignCast(ptr));
        self.inner.observe(self.inner.ptr, selected, value);
    }
    fn stage(ptr: *anyopaque, view: @import("../commit_participant.zig").View, stamp: []const u8) !void {
        const self: *FailingParticipant = @ptrCast(@alignCast(ptr));
        try self.inner.stage(self.inner.ptr, view, stamp);
        return error.InjectedAcceptanceFailure;
    }
    fn participant(self: *FailingParticipant) @import("../commit_participant.zig").Participant {
        return .{ .ptr = self, .reset = reset, .observe = observe, .stage = stage };
    }
};

fn testPreparedAllocations(alloc: std.mem.Allocator) !void {
    const KV = @import("../docstore.zig").KVPair;
    const primary = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(primary);
    const ttl = try keys.ttlKeyAlloc(alloc, "doc");
    defer alloc.free(ttl);
    const first = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "dense");
    defer alloc.free(first);
    const second = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "sparse");
    defer alloc.free(second);
    const dense = try codec.encodeAuthoredDenseEmbeddingAlloc(alloc, &.{ 1, 2 });
    defer alloc.free(dense);
    const sparse = try codec.encodeAuthoredSparseEmbeddingAlloc(alloc, &.{ 3, 8 }, &.{ 1, 2 });
    defer alloc.free(sparse);
    const writes = [_]KV{ .{ .key = primary, .value = "first" }, .{ .key = ttl, .value = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } }, .{ .key = first, .value = dense }, .{ .key = second, .value = sparse }, .{ .key = primary, .value = "last" }, .{ .key = first, .value = dense } };
    const authored = [_]KV{ writes[2], writes[3], writes[5] };
    var prepared = try Prepared.init(alloc, 41, &authored, &writes);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.vectors.len);
    try std.testing.expectEqual(@as(u32, 4), prepared.postimages.count());
    for (prepared.vectors) |vector| try std.testing.expectEqualSlices(u8, &digest("last"), &vector.source_digest);
    prepared.reset();
    for (writes) |write| prepared.observe(write.key, write.value);
    try std.testing.expect(prepared.postimages.get(primary).?.matched);
    prepared.observe(primary, "first");
    try std.testing.expect(!prepared.postimages.get(primary).?.matched);
    prepared.observe(primary, "last");
    try std.testing.expect(prepared.postimages.get(primary).?.matched);
    prepared.observe(first, null);
    try std.testing.expect(!prepared.postimages.get(first).?.matched);
    prepared.reset();
    var checks = prepared.postimages.valueIterator();
    while (checks.next()) |check| try std.testing.expect(!check.matched);
}

test "ordered artifact inventory authored preparation is deduplicated and allocation safe" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testPreparedAllocations, .{});
}
