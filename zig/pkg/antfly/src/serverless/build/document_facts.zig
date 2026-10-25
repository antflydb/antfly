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

//! Source-fenced document point index and exact scheduling summaries. Body
//! blobs have independent immutable identities: rewriting a flat document
//! segment cannot invalidate every routing entry in this copy-on-write tree.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("../graph_segment/page_tree.zig");
const page_store = @import("../graph_segment/page_store.zig");
const artifacts = @import("../artifacts/store.zig");
const refs = @import("antfly_local_sources").serverless_manifest_artifact_ref;

pub const max_document_id_bytes = 256 * 1024;
pub const max_pending_key_bytes = max_document_id_bytes + 8;

pub fn pendingOrderKeyAlloc(alloc: Allocator, lsn: u64, id: []const u8) ![]u8 {
    if (id.len == 0 or id.len > max_document_id_bytes) return error.InvalidDocumentFact;
    const key = try alloc.alloc(u8, 8 + id.len);
    std.mem.writeInt(u64, key[0..8], lsn, .big);
    @memcpy(key[8..], id);
    return key;
}

pub const BodyRef = struct {
    digest: [32]u8,
    attempt: [16]u8,
    bytes: u64,

    pub fn identity(self: BodyRef, domain: [32]u8) ![175]u8 {
        return (artifacts.UploadScope{ .domain = domain, .attempt = self.attempt }).artifactId(&std.fmt.bytesToHex(&self.digest, .lower));
    }
};

pub const Fact = struct {
    body: BodyRef,
    body_has_dependencies: bool = false,
    media_audit: ?BodyRef = null,
    last_lsn: u64,
    last_timestamp_ns: u64,
    /// chunk_preview, chunk_embeddings, rerank_terms.
    present: u3 = 0,
    /// lexical_sparse, chunk_preview, chunk_embeddings, rerank_terms.
    pending: u4 = 0,

    pub const encoded_bytes = 136;
    pub fn encode(self: Fact) [encoded_bytes]u8 {
        var bytes = @as([encoded_bytes]u8, @splat(0));
        @memcpy(bytes[0..32], &self.body.digest);
        @memcpy(bytes[32..48], &self.body.attempt);
        std.mem.writeInt(u64, bytes[48..56], self.body.bytes, .little);
        std.mem.writeInt(u64, bytes[56..64], self.last_lsn, .little);
        std.mem.writeInt(u64, bytes[64..72], self.last_timestamp_ns, .little);
        bytes[72] = self.present;
        bytes[73] = self.pending;
        bytes[74] = @intFromBool(self.body_has_dependencies);
        if (self.media_audit) |audit| {
            bytes[75] = 1;
            @memcpy(bytes[80..112], &audit.digest);
            @memcpy(bytes[112..128], &audit.attempt);
            std.mem.writeInt(u64, bytes[128..136], audit.bytes, .little);
        }
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Fact {
        if ((bytes.len != 80 and bytes.len != encoded_bytes) or bytes[72] > 7 or bytes[73] > 15 or
            bytes[74] > 1 or bytes[75] > 1 or !std.mem.allEqual(u8, bytes[76..80], 0)) return error.InvalidDocumentFact;
        if (bytes[75] == 1 and bytes.len != encoded_bytes) return error.InvalidDocumentFact;
        if (bytes.len == encoded_bytes and bytes[75] == 0 and !std.mem.allEqual(u8, bytes[80..], 0)) return error.InvalidDocumentFact;
        return .{
            .body = .{ .digest = bytes[0..32].*, .attempt = bytes[32..48].*, .bytes = std.mem.readInt(u64, bytes[48..56], .little) },
            .body_has_dependencies = bytes[74] == 1,
            .media_audit = if (bytes[75] == 1) .{ .digest = bytes[80..112].*, .attempt = bytes[112..128].*, .bytes = std.mem.readInt(u64, bytes[128..136], .little) } else null,
            .last_lsn = std.mem.readInt(u64, bytes[56..64], .little),
            .last_timestamp_ns = std.mem.readInt(u64, bytes[64..72], .little),
            .present = @intCast(bytes[72]),
            .pending = @intCast(bytes[73]),
        };
    }
    fn bits(self: Fact) u7 {
        return @as(u7, self.present) | (@as(u7, self.pending) << 3);
    }
};

pub const Root = struct {
    domain: [32]u8,
    policy_fingerprint: [32]u8,
    wal_end_lsn: u64 = 0,
    page: ?tree.Ref = null,
    /// Ready-to-process documents ordered by (WAL LSN, document ID). New WAL
    /// arrivals cannot extend an existing scan cycle's immutable upper bound.
    pending_pages: [4]?tree.Ref = @splat(null),
    audit_page: ?tree.Ref = null,
    document_count: u64 = 0,
    /// Presence counters followed by pending counters, in Fact bit order.
    counts: [7]u64 = @splat(0),

    pub const encoded_bytes = 576;
    pub const metadata_version = 8;
    pub fn eql(a: Root, b: Root) bool {
        return std.mem.eql(u8, &a.encode(), &b.encode());
    }
    pub fn encode(self: Root) [encoded_bytes]u8 {
        var out = @as([encoded_bytes]u8, @splat(0));
        @memcpy(out[0..8], "AFDFACT8");
        @memcpy(out[16..48], &self.domain);
        @memcpy(out[48..80], &self.policy_fingerprint);
        std.mem.writeInt(u64, out[80..88], self.wal_end_lsn, .little);
        std.mem.writeInt(u64, out[88..96], self.document_count, .little);
        for (self.counts, 0..) |count, i| std.mem.writeInt(u64, out[96 + i * 8 ..][0..8], count, .little);
        const pages = [_]?tree.Ref{self.page} ++ self.pending_pages ++ [_]?tree.Ref{self.audit_page};
        for (pages, 0..) |maybe_page, i| if (maybe_page) |page| {
            out[8] |= @as(u8, 1) << @intCast(i);
            const slot = out[152 + i * 64 ..][0..64];
            @memcpy(slot[0..32], &page.digest);
            @memcpy(slot[32..48], &page.attempt);
            std.mem.writeInt(u32, slot[48..52], page.bytes, .little);
            slot[52] = page.height;
            std.mem.writeInt(u64, slot[56..64], page.records, .little);
        };
        return out;
    }
    pub fn decode(bytes: []const u8) !Root {
        const legacy = bytes.len == 512 and std.mem.eql(u8, bytes[0..8], "AFDFACT3");
        if (!legacy and (bytes.len != encoded_bytes or (!std.mem.eql(u8, bytes[0..8], "AFDFACT4") and !std.mem.eql(u8, bytes[0..8], "AFDFACT5") and !std.mem.eql(u8, bytes[0..8], "AFDFACT6") and !std.mem.eql(u8, bytes[0..8], "AFDFACT7") and !std.mem.eql(u8, bytes[0..8], "AFDFACT8")))) return error.InvalidDocumentFactsRoot;
        if (bytes[8] > (if (legacy) @as(u8, 31) else 63) or !std.mem.allEqual(u8, bytes[9..16], 0) or
            !std.mem.allEqual(u8, bytes[if (legacy) @as(usize, 472) else 536..], 0)) return error.InvalidDocumentFactsRoot;
        var root = Root{
            .domain = bytes[16..48].*,
            .policy_fingerprint = bytes[48..80].*,
            .wal_end_lsn = std.mem.readInt(u64, bytes[80..88], .little),
            .document_count = std.mem.readInt(u64, bytes[88..96], .little),
        };
        for (&root.counts, 0..) |*count, i| {
            count.* = std.mem.readInt(u64, bytes[96 + i * 8 ..][0..8], .little);
            if (count.* > root.document_count) return error.InvalidDocumentFactsRoot;
        }
        for (0..if (legacy) @as(usize, 5) else 6) |i| {
            const slot = bytes[152 + i * 64 ..][0..64];
            const count = if (i == 0 or i == 5) root.document_count else root.counts[2 + i];
            if (bytes[8] & (@as(u8, 1) << @intCast(i)) == 0) {
                if (count != 0 or !std.mem.allEqual(u8, slot, 0)) return error.InvalidDocumentFactsRoot;
                continue;
            }
            if (!std.mem.allEqual(u8, slot[53..56], 0)) return error.InvalidDocumentFactsRoot;
            const page: tree.Ref = .{
                .digest = slot[0..32].*,
                .attempt = slot[32..48].*,
                .bytes = std.mem.readInt(u32, slot[48..52], .little),
                .height = slot[52],
                .records = std.mem.readInt(u64, slot[56..64], .little),
            };
            try page.validate();
            if (page.records != count) return error.InvalidDocumentFactsRoot;
            if (i == 0) root.page = page else if (i == 5) root.audit_page = page else root.pending_pages[i - 1] = page;
        }
        return root;
    }
};

pub fn lookup(alloc: Allocator, store: tree.Store, root: Root, id: []const u8) !?Fact {
    if (!std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    var cursor = try tree.Cursor.init(alloc, store, root.page, id, null);
    defer cursor.deinit();
    const record = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, record.key, id)) return null;
    const fact = try Fact.decode(record.value);
    if (fact.last_lsn > root.wal_end_lsn) return error.InvalidDocumentFact;
    return fact;
}

pub const PendingCursor = struct {
    cursor: tree.Cursor,
    stage_bit: u4,
    wal_end_lsn: u64,
    pub fn deinit(self: *PendingCursor) void {
        self.cursor.deinit();
    }
    /// Borrowed until next/deinit, with the same lifetime as a tree record.
    pub const Record = struct { key: []const u8, order_key: []const u8, value: []const u8 };

    pub fn next(self: *PendingCursor) !?Record {
        const record = (try self.cursor.next()) orelse return null;
        const fact = try Fact.decode(record.value);
        if ((self.stage_bit != 0 and fact.pending & self.stage_bit == 0) or fact.last_lsn > self.wal_end_lsn or
            record.key.len <= 8 or record.key.len > max_pending_key_bytes or
            std.mem.readInt(u64, record.key[0..8], .big) != fact.last_lsn) return error.InvalidDocumentFact;
        return .{ .key = record.key[8..], .order_key = record.key, .value = record.value };
    }
};

pub fn pendingCursor(alloc: Allocator, store: tree.Store, root: Root, stage_index: usize, lower: []const u8) !PendingCursor {
    if (stage_index >= 4) return error.InvalidEnrichmentStage;
    if (!std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    const page = root.pending_pages[stage_index];
    if ((if (page) |ref| ref.records else @as(u64, 0)) != root.counts[3 + stage_index]) return error.InvalidDocumentFactsRoot;
    return .{
        .cursor = try tree.Cursor.init(alloc, store, page, lower, null),
        .stage_bit = @as(u4, 1) << @intCast(stage_index),
        .wal_end_lsn = root.wal_end_lsn,
    };
}

pub fn pendingCursorAtRank(alloc: Allocator, store: tree.Store, root: Root, stage_index: usize, rank: u64) !PendingCursor {
    if (stage_index >= 4) return error.InvalidEnrichmentStage;
    if (!std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    const page = root.pending_pages[stage_index];
    if ((if (page) |ref| ref.records else @as(u64, 0)) != root.counts[3 + stage_index]) return error.InvalidDocumentFactsRoot;
    return .{
        .cursor = try tree.Cursor.initAtRank(alloc, store, page, rank),
        .stage_bit = @as(u4, 1) << @intCast(stage_index),
        .wal_end_lsn = root.wal_end_lsn,
    };
}

/// All facts ordered by WAL position. A captured upper key excludes later
/// arrivals regardless of their document IDs, across publication HEAD changes.
pub fn mediaCursor(a: Allocator, store: tree.Store, root: Root, lower: []const u8, rank: ?u64) !PendingCursor {
    if ((if (root.audit_page) |page| page.records else @as(u64, 0)) != root.document_count) return error.InvalidDocumentFactsRoot;
    if (!std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    if (lower.len != 0 and (lower.len <= 8 or lower.len > max_pending_key_bytes)) return error.InvalidEnrichmentStageProgress;
    return .{ .cursor = if (rank) |value| try tree.Cursor.initAtRank(a, store, root.audit_page, value) else try tree.Cursor.init(a, store, root.audit_page, lower, null), .stage_bit = 0, .wal_end_lsn = root.wal_end_lsn };
}

pub const Replacement = struct { id: []const u8, value: ?Fact };
pub const Plan = struct {
    alloc: Allocator,
    source: Root,
    next: Root,
    changes: []tree.Mutation,
    /// Own ordering keys; values borrow the corresponding primary changes.
    pending_changes: [5][]tree.Mutation,

    pub fn deinit(self: *Plan) void {
        for (self.pending_changes) |changes| {
            for (changes) |change| self.alloc.free(change.key);
            self.alloc.free(changes);
        }
        for (self.changes) |change| {
            self.alloc.free(change.key);
            if (change.value) |value| self.alloc.free(value);
        }
        self.alloc.free(self.changes);
        self.* = undefined;
    }
    pub fn publish(self: *const Plan, store: tree.Store, current: Root) !Root {
        if (!current.eql(self.source)) return error.DocumentFactsSourceChanged;
        if (!std.mem.eql(u8, &store.domain, &current.domain)) return error.GraphPageDomainMismatch;
        var next = self.next;
        next.page = try tree.apply(self.alloc, store, current.page, self.changes);
        for (&next.pending_pages, current.pending_pages, self.pending_changes[0..4], 0..) |*page, prior, changes, i| {
            // Identical pending sets often cover every enabled stage during
            // bootstrap. Share their immutable result, not repeated PUTs.
            const shared = for (0..i) |j| {
                if (std.meta.eql(prior, current.pending_pages[j]) and sameChanges(changes, self.pending_changes[j])) break j;
            } else null;
            page.* = if (shared) |j| next.pending_pages[j] else try tree.apply(self.alloc, store, prior, changes);
        }
        const shared_audit = for (0..4) |i| {
            if (std.meta.eql(current.audit_page, current.pending_pages[i]) and sameChanges(self.pending_changes[4], self.pending_changes[i])) break i;
        } else null;
        next.audit_page = if (shared_audit) |i| next.pending_pages[i] else try tree.apply(self.alloc, store, current.audit_page, self.pending_changes[4]);
        _ = try Root.decode(&next.encode());
        return next;
    }
};

fn sameChanges(a: []const tree.Mutation, b: []const tree.Mutation) bool {
    if (a.len != b.len) return false;
    for (a, b) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs.key, rhs.key)) return false;
        if ((lhs.value == null) != (rhs.value == null)) return false;
        if (lhs.value) |value| if (!std.mem.eql(u8, value, rhs.value.?)) return false;
    }
    return true;
}

pub fn planAlloc(alloc: Allocator, store: tree.Store, source: Root, replacements: []const Replacement, next_wal_lsn: u64) !Plan {
    if (next_wal_lsn < source.wal_end_lsn) return error.DocumentFactsSourceChanged;
    if (!std.mem.eql(u8, &store.domain, &source.domain)) return error.GraphPageDomainMismatch;
    var changes = std.ArrayListUnmanaged(tree.Mutation).empty;
    errdefer {
        for (changes.items) |change| {
            alloc.free(change.key);
            if (change.value) |value| alloc.free(value);
        }
        changes.deinit(alloc);
    }
    var next = source;
    var pending: [5]std.ArrayListUnmanaged(tree.Mutation) = @splat(.empty);
    defer for (&pending) |*items| {
        for (items.items) |change| alloc.free(change.key);
        items.deinit(alloc);
    };
    next.wal_end_lsn = next_wal_lsn;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (replacements) |replacement| {
        try store.check(store.ptr);
        if (replacement.id.len == 0 or replacement.id.len > max_document_id_bytes) return error.InvalidDocumentFact;
        const slot = try seen.getOrPut(alloc, replacement.id);
        if (slot.found_existing) return error.DuplicateDocumentFactReplacement;
        const prior = try lookup(alloc, store, source, replacement.id);
        if (prior) |fact| {
            if (next.document_count == 0) return error.InvalidDocumentFactsRoot;
            next.document_count -= 1;
            for (&next.counts, 0..) |*count, i| if (fact.bits() & (@as(u7, 1) << @intCast(i)) != 0) {
                if (count.* == 0) return error.InvalidDocumentFactsRoot;
                count.* -= 1;
            };
        }
        if (replacement.value) |fact| {
            if (fact.last_lsn > next_wal_lsn) return error.DocumentFactsSourceChanged;
            next.document_count = std.math.add(u64, next.document_count, 1) catch return error.InvalidDocumentFactsRoot;
            for (&next.counts, 0..) |*count, i| if (fact.bits() & (@as(u7, 1) << @intCast(i)) != 0) {
                count.* += 1;
            };
        }
        const value = value: {
            const key = try alloc.dupe(u8, replacement.id);
            errdefer alloc.free(key);
            const bytes = if (replacement.value) |fact| try alloc.dupe(u8, fact.encode()[0..if (fact.media_audit == null) @as(usize, 80) else Fact.encoded_bytes]) else null;
            errdefer if (bytes) |owned| alloc.free(owned);
            try changes.append(alloc, .{ .key = key, .value = bytes });
            break :value bytes;
        };
        if (prior) |fact| if (replacement.value == null or fact.last_lsn != replacement.value.?.last_lsn)
            try appendPendingChange(alloc, &pending[4], fact.last_lsn, replacement.id, null);
        if (replacement.value) |fact| try appendPendingChange(alloc, &pending[4], fact.last_lsn, replacement.id, value);
        // The main change now owns these bytes, including on a later error.
        for (0..4) |i| {
            const items = &pending[i];
            const bit = @as(u4, 1) << @intCast(i);
            const was_pending = if (prior) |fact| fact.pending & bit != 0 else false;
            const is_pending = if (replacement.value) |fact| fact.pending & bit != 0 else false;
            if (was_pending and (!is_pending or prior.?.last_lsn != replacement.value.?.last_lsn))
                try appendPendingChange(alloc, items, prior.?.last_lsn, replacement.id, null);
            if (is_pending)
                try appendPendingChange(alloc, items, replacement.value.?.last_lsn, replacement.id, value);
        }
    }
    std.mem.sort(tree.Mutation, changes.items, {}, struct {
        fn less(_: void, a: tree.Mutation, b: tree.Mutation) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.less);
    for (&pending) |*items| std.mem.sort(tree.Mutation, items.items, {}, struct {
        fn less(_: void, a: tree.Mutation, b: tree.Mutation) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.less);
    var pending_changes: [5][]tree.Mutation = @splat(&.{});
    errdefer for (pending_changes) |items| {
        for (items) |change| alloc.free(change.key);
        alloc.free(items);
    };
    for (&pending_changes, &pending) |*owned, *items| owned.* = try items.toOwnedSlice(alloc);
    return .{ .alloc = alloc, .source = source, .next = next, .changes = try changes.toOwnedSlice(alloc), .pending_changes = pending_changes };
}

fn appendPendingChange(alloc: Allocator, changes: *std.ArrayListUnmanaged(tree.Mutation), lsn: u64, id: []const u8, value: ?[]const u8) !void {
    const key = try pendingOrderKeyAlloc(alloc, lsn, id);
    errdefer alloc.free(key);
    try changes.append(alloc, .{ .key = key, .value = value });
}

pub const body_header_bytes = 16;

pub fn bodyDigest(bytes: []const u8) [32]u8 {
    var header: [body_header_bytes]u8 = undefined;
    @memcpy(header[0..8], "AFDBODY1");
    std.mem.writeInt(u64, header[8..16], bytes.len, .little);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&header);
    hash.update(bytes);
    return hash.finalResult();
}

/// Only flagged bodies need this dependency walk. Ordinary documents retain
/// the existing metadata-only GC path. Links stay in the namespace's fact-body
/// domain and every referenced envelope is length/digest verified before use.
pub fn mediaCheckpointParent(a: Allocator, body: []const u8) !?BodyRef {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const state = parsed.value.object.get("_enrichment") orelse return null;
    if (state != .object) return null;
    const group = state.object.get("chunk_embeddings_checkpoint") orelse return null;
    if (group != .object) return error.InvalidDocumentBody;
    const version = group.object.get("format") orelse return error.InvalidDocumentBody;
    if (version != .integer or (version.integer != 1 and version.integer != 2)) return error.InvalidDocumentBody;
    const parent = group.object.get("parent") orelse return null;
    if (parent == .null) return null;
    return try std.json.parseFromValueLeaky(BodyRef, parsed.arena.allocator(), parent, .{});
}

pub fn putBody(alloc: Allocator, pages: *page_store.PageStore, bytes: []const u8) !BodyRef {
    try pages.cancellation.check();
    const len = std.math.add(usize, bytes.len, body_header_bytes) catch return error.ArtifactTooLarge;
    if (len > pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
    const encoded = try alloc.alloc(u8, len);
    defer alloc.free(encoded);
    @memcpy(encoded[0..8], "AFDBODY1");
    std.mem.writeInt(u64, encoded[8..16], bytes.len, .little);
    @memcpy(encoded[16..], bytes);
    return putEncoded(pages, encoded);
}

fn putEncoded(pages: *page_store.PageStore, bytes: []const u8) !BodyRef {
    if (bytes.len > pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
    pages.remaining_write_bytes.* -= bytes.len;
    var metadata = try pages.artifacts.putScoped(.{ .domain = pages.domain, .attempt = pages.attempt }, bytes, pages.cancellation);
    defer metadata.deinit(pages.artifacts.allocator);
    const ref = BodyRef{ .digest = try artifacts.sha256DigestFromChecksum(metadata.checksum), .attempt = pages.attempt, .bytes = bytes.len };
    if (metadata.byte_len != bytes.len or !std.mem.eql(u8, metadata.artifact_id, &try ref.identity(pages.domain))) return error.ArtifactIntegrityMismatch;
    try artifacts.validatePayloadSha256WithCancellation(bytes, metadata.checksum, pages.cancellation);
    return ref;
}

/// Compact checkpoint payloads live inside the authenticated body envelope.
/// The base is always a plain published source body, so reconstruction never
/// walks the output ancestry or accumulates another copy of the source.
pub const CheckpointBody = struct {
    base: BodyRef,
    patch: std.json.Value,
};
pub const checkpoint_magic = "AFDCP001";
pub fn isCheckpointBody(bytes: []const u8) bool {
    return std.mem.startsWith(u8, bytes, checkpoint_magic);
}
pub fn parseCheckpointBody(a: Allocator, bytes: []const u8) !std.json.Parsed(CheckpointBody) {
    if (!isCheckpointBody(bytes)) return error.InvalidDocumentBody;
    return std.json.parseFromSlice(CheckpointBody, a, bytes[checkpoint_magic.len..], .{});
}
pub const media_fields = [_][]const u8{ "_enrichment", "chunk_embeddings", "chunk_preview", "media_outputs", "transcript", "transcript_spans" };
pub fn encodeCheckpointBodyAlloc(a: Allocator, base: BodyRef, document: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, document, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentBody;
    const scratch = parsed.arena.allocator();
    var patch = std.json.Value{ .object = .empty };
    for (media_fields) |name| if (parsed.value.object.get(name)) |value| try patch.object.put(scratch, name, value);
    const encoded = try std.json.Stringify.valueAlloc(a, CheckpointBody{ .base = base, .patch = patch }, .{});
    defer a.free(encoded);
    return std.mem.concat(a, u8, &.{ checkpoint_magic, encoded });
}
pub fn resolveCheckpointBodyAlloc(a: Allocator, pages: *page_store.PageStore, bytes: []const u8) ![]u8 {
    return resolveCheckpointSelectedAlloc(a, pages, bytes, true);
}
/// Enrichment resumes the compact cursor directly; ordinary readers and
/// builders materialize repair overlays without losing healthy vector rows.
pub fn resolveCheckpointStateAlloc(a: Allocator, pages: *page_store.PageStore, bytes: []const u8) ![]u8 {
    return resolveCheckpointSelectedAlloc(a, pages, bytes, false);
}
fn resolveCheckpointSelectedAlloc(a: Allocator, pages: *page_store.PageStore, bytes: []const u8, materialize: bool) ![]u8 {
    var descriptor = try parseCheckpointBody(a, bytes);
    defer descriptor.deinit();
    // Nested bases are forbidden: unrelated stages/upserts collapse to a new
    // plain snapshot before the next media checkpoint starts.
    const source = try readStoredBodyAlloc(a, pages, descriptor.value.base);
    defer a.free(source);
    if (isCheckpointBody(source)) return error.InvalidDocumentBody;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, source, .{});
    defer parsed.deinit();
    if (parsed.value != .object or descriptor.value.patch != .object) return error.InvalidDocumentBody;
    var baseline = std.json.Value{ .object = .empty };
    for (media_fields[1..]) |name| if (parsed.value.object.get(name)) |value| try baseline.object.put(parsed.arena.allocator(), name, value);
    for (media_fields) |name| _ = parsed.value.object.swapRemove(name);
    var it = descriptor.value.patch.object.iterator();
    while (it.next()) |entry| {
        var allowed = false;
        for (media_fields) |name| if (std.mem.eql(u8, name, entry.key_ptr.*)) {
            allowed = true;
            break;
        };
        if (!allowed) return error.InvalidDocumentBody;
        try parsed.value.object.put(parsed.arena.allocator(), entry.key_ptr.*, entry.value_ptr.*);
    }
    const repair = repairCheckpoint(parsed.value);
    if (repair) {
        const checkpoint = @import("../enrichment/media_checkpoint.zig");
        if (try checkpoint.get(parsed.arena.allocator(), parsed.value)) |group| if (group.repair_base) |base| {
            if (!std.meta.eql(base, descriptor.value.base)) return error.InvalidMediaCheckpoint;
            if (materialize) {
                try checkpoint.expandRepairView(parsed.arena.allocator(), .{ .allocator = a, .pages = pages, .previous = base, .key_digest = group.key_digest }, &parsed.value, baseline);
            } else {
                // A resuming worker borrows the original accepted receipts for
                // selective masks. The group cursor owns the repaired prefix.
                var fields = baseline.object.iterator();
                while (fields.next()) |entry| try parsed.value.object.put(parsed.arena.allocator(), entry.key_ptr.*, entry.value_ptr.*);
            }
        };
    }
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}
fn repairCheckpoint(document: std.json.Value) bool {
    if (document != .object) return false;
    const state = document.object.get("_enrichment") orelse return false;
    if (state != .object) return false;
    const group = state.object.get("chunk_embeddings_checkpoint") orelse return false;
    if (group != .object) return false;
    if (group.object.get("repair_base")) |base| if (base != .null) return true;
    if (group.object.get("format")) |format| return format == .integer and format.integer == 2;
    return false;
}

pub fn readBodyAlloc(a: Allocator, pages: *page_store.PageStore, ref: BodyRef) ![]u8 {
    const bytes = try readStoredBodyAlloc(a, pages, ref);
    if (!isCheckpointBody(bytes)) return bytes;
    defer a.free(bytes);
    return resolveCheckpointBodyAlloc(a, pages, bytes);
}
/// Closure needs only output pages; it must never rehydrate their source.
pub fn readCheckpointPageAlloc(a: Allocator, pages: *page_store.PageStore, ref: BodyRef) ![]u8 {
    const bytes = try readStoredBodyAlloc(a, pages, ref);
    if (!isCheckpointBody(bytes)) return bytes; // pre-compact-format ancestors
    defer a.free(bytes);
    var descriptor = try parseCheckpointBody(a, bytes);
    defer descriptor.deinit();
    return std.json.Stringify.valueAlloc(a, descriptor.value.patch, .{});
}

pub fn readStoredBodyAlloc(alloc: Allocator, pages: *page_store.PageStore, ref: BodyRef) ![]u8 {
    const id = try ref.identity(pages.domain);
    const len = std.math.cast(usize, ref.bytes) orelse return error.ArtifactReadBudgetExceeded;
    const bytes = if (pages.read_cache) |cache|
        try cache.read(cache.ptr, alloc, pages.artifacts, .{ .kind = .document_facts, .artifact_id = &id, .checksum = id[7..71], .byte_len = ref.bytes }, 0, len, ref.digest, pages.cancellation, pages.remaining_read_bytes)
    else bytes: {
        try artifacts.chargeReadBudget(pages.remaining_read_bytes, ref.bytes);
        break :bytes try pages.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, &id, 0, len, pages.cancellation);
    };
    errdefer alloc.free(bytes);
    if (bytes.len != len) return error.ArtifactIntegrityMismatch;
    try artifacts.validatePayloadSha256WithCancellation(bytes, id[7..71], pages.cancellation);
    // The envelope is a type-domain separator. An arbitrary document body
    // must never alias a routing page and bypass its descendant GC walk.
    if (len < body_header_bytes or !std.mem.eql(u8, bytes[0..8], "AFDBODY1") or
        std.mem.readInt(u64, bytes[8..16], .little) != len - body_header_bytes) return error.InvalidDocumentBody;
    const body_len = len - body_header_bytes;
    std.mem.copyForwards(u8, bytes[0..body_len], bytes[body_header_bytes..]);
    return alloc.realloc(bytes, body_len);
}

pub fn loadRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef) !Root {
    if (source.kind != .document_facts or !((source.metadata_version == 3 and source.byte_len == 512) or ((source.metadata_version == 4 or source.metadata_version == 5 or source.metadata_version == 6 or source.metadata_version == 7 or source.metadata_version == Root.metadata_version) and source.byte_len == Root.encoded_bytes)))
        return error.InvalidDocumentFactsRoot;
    try artifacts.validateSha256ArtifactIdentity(source.artifact_id, source.checksum);
    const scope = (try artifacts.uploadScopeFromArtifactId(source.artifact_id)) orelse return error.InvalidDocumentFactsRoot;
    const bytes = if (pages.read_cache) |cache|
        try cache.read(cache.ptr, alloc, pages.artifacts, source, 0, source.byte_len, try artifacts.sha256DigestFromChecksum(source.checksum), pages.cancellation, pages.remaining_read_bytes)
    else bytes: {
        try artifacts.chargeReadBudget(pages.remaining_read_bytes, source.byte_len);
        break :bytes try pages.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, source.artifact_id, 0, source.byte_len, pages.cancellation);
    };
    defer alloc.free(bytes);
    try artifacts.validatePayloadSha256WithCancellation(bytes, source.checksum, pages.cancellation);
    const magic = switch (source.metadata_version) {
        3 => "AFDFACT3",
        4 => "AFDFACT4",
        5 => "AFDFACT5",
        6 => "AFDFACT6",
        7 => "AFDFACT7",
        8 => "AFDFACT8",
        else => return error.InvalidDocumentFactsRoot,
    };
    if (!std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidDocumentFactsRoot;
    const root = try Root.decode(bytes);
    if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.GraphPageDomainMismatch;
    if (!std.mem.allEqual(u8, &pages.domain, 0) and !std.mem.eql(u8, &root.domain, &pages.domain)) return error.GraphPageDomainMismatch;
    pages.domain = root.domain;
    return root;
}

pub fn publishRoot(alloc: Allocator, pages: *page_store.PageStore, root: Root) !refs.ArtifactRef {
    if (!std.mem.eql(u8, &root.domain, &pages.domain)) return error.GraphPageDomainMismatch;
    const bytes = root.encode();
    _ = try Root.decode(&bytes);
    const body = try putEncoded(pages, &bytes);
    const id = try body.identity(root.domain);
    const owned_id = try alloc.dupe(u8, &id);
    errdefer alloc.free(owned_id);
    return .{ .kind = .document_facts, .artifact_id = owned_id, .checksum = try alloc.dupe(u8, id[7..71]), .byte_len = bytes.len, .metadata_version = Root.metadata_version };
}

pub fn retainRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef, retained: *std.StringHashMapUnmanaged(void)) !void {
    if (retained.contains(source.artifact_id)) return;
    const root = try loadRoot(alloc, pages, source);
    var visitor = Reachability{ .alloc = alloc, .pages = pages, .retained = retained };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, false);
    for (root.pending_pages ++ [_]?tree.Ref{root.audit_page}) |maybe_page| if (maybe_page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, false);
    try visitor.mark(source.artifact_id);
}

pub fn reclaimRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef, retained: *const std.StringHashMapUnmanaged(void)) !usize {
    if (retained.contains(source.artifact_id)) return 0;
    const root = loadRoot(alloc, pages, source) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    var visitor = Reachability{ .alloc = alloc, .pages = pages, .retained = @constCast(retained), .reclaim = true };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, true);
    for (root.pending_pages ++ [_]?tree.Ref{root.audit_page}) |maybe_page| if (maybe_page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, true);
    try visitor.mark(source.artifact_id);
    return visitor.deleted;
}

const Reachability = struct {
    alloc: Allocator,
    pages: *page_store.PageStore,
    retained: *std.StringHashMapUnmanaged(void),
    reclaim: bool = false,
    deleted: usize = 0,

    fn mark(self: *@This(), id: []const u8) !void {
        if (self.retained.contains(id)) return;
        if (self.reclaim) {
            try self.pages.cancellation.check();
            self.pages.artifacts.delete(id) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            self.deleted += 1;
        } else {
            const owned = try self.alloc.dupe(u8, id);
            errdefer self.alloc.free(owned);
            try self.retained.put(self.alloc, owned, {});
        }
    }
    pub fn skip(self: *@This(), ref: tree.Ref) !bool {
        return self.retained.contains(&try page_store.PageStore.identity(self.pages.domain, ref));
    }
    pub fn visitRecord(self: *@This(), _: tree.Ref, _: []const u8, value: []const u8) !void {
        const fact = try Fact.decode(value);
        if (fact.media_audit) |audit| try self.mark(&try audit.identity(self.pages.domain));
        if (!fact.body_has_dependencies) return self.mark(&try fact.body.identity(self.pages.domain));
        // A legacy source snapshot can also be an output ancestor. Marking
        // it before walking that ancestry would mistake a source-only mark
        // for a completed dependency traversal (and skip or delete parents).
        var bases = std.ArrayList(BodyRef).empty;
        defer bases.deinit(self.alloc);
        var current: ?BodyRef = fact.body;
        while (current) |body| {
            const id = try body.identity(self.pages.domain);
            if (self.retained.contains(&id)) break;
            const bytes = readStoredBodyAlloc(self.alloc, self.pages, body) catch |err| switch (err) {
                error.FileNotFound => if (self.reclaim) break else return err,
                else => return err,
            };
            defer self.alloc.free(bytes);
            if (isCheckpointBody(bytes)) {
                var descriptor = try parseCheckpointBody(self.alloc, bytes);
                defer descriptor.deinit();
                try bases.append(self.alloc, descriptor.value.base);
                const patch = try std.json.Stringify.valueAlloc(self.alloc, descriptor.value.patch, .{});
                defer self.alloc.free(patch);
                current = try mediaCheckpointParent(self.alloc, patch);
            } else current = try mediaCheckpointParent(self.alloc, bytes);
            // Body links form a content-addressed DAG; count and byte budgets
            // remain owned by the retention operation, just like tree pages.
            try self.mark(&id);
        }
        for (bases.items) |base| try self.mark(&try base.identity(self.pages.domain));
    }
    pub fn visit(self: *@This(), ref: tree.Ref) !void {
        try self.mark(&try page_store.PageStore.identity(self.pages.domain, ref));
    }
};

test "serverless document facts update only touched paths and maintain exact source-fenced counters" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 10, .present = 5, .pending = 3 };
    var first = try planAlloc(a, store, empty, &.{ .{ .id = "a", .value = fact }, .{ .id = "b", .value = fact } }, 1);
    defer first.deinit();
    const root = try first.publish(store, empty);
    try std.testing.expectEqual(@as(usize, 2), memory.writes);
    try std.testing.expect(!std.meta.eql(root.page, root.pending_pages[0]));
    try std.testing.expectEqual(root.pending_pages[0], root.pending_pages[1]);
    try std.testing.expectEqual(@as(u64, 2), root.document_count);
    try std.testing.expectEqual([7]u64{ 2, 0, 2, 2, 2, 0, 0 }, root.counts);
    try std.testing.expect(root.eql(try Root.decode(&root.encode())));
    var v4 = root.encode();
    @memcpy(v4[0..8], "AFDFACT4");
    try std.testing.expect(root.eql(try Root.decode(&v4)));
    // Released v3 roots keep their five original trees readable. Migration
    // rebuilds the separate v4 audit tree from the immutable document facts.
    var legacy = root.encode()[0..512].*;
    @memcpy(legacy[0..8], "AFDFACT3");
    legacy[8] &= 31;
    @memset(legacy[472..], 0);
    const decoded_legacy = try Root.decode(&legacy);
    try std.testing.expectEqual(root.page, decoded_legacy.page);
    try std.testing.expectEqual(root.pending_pages, decoded_legacy.pending_pages);
    try std.testing.expectEqual(@as(?tree.Ref, null), decoded_legacy.audit_page);
    try std.testing.expectEqual(@as(u64, 2), decoded_legacy.document_count);
    var changed = fact;
    changed.present = 2;
    changed.pending = 12;
    var second = try planAlloc(a, store, root, &.{ .{ .id = "a", .value = changed }, .{ .id = "b", .value = null } }, 2);
    defer second.deinit();
    try std.testing.expectError(error.DocumentFactsSourceChanged, second.publish(store, empty));
    const updated = try second.publish(store, root);
    try std.testing.expectEqual([7]u64{ 0, 1, 0, 0, 0, 1, 1 }, updated.counts);
    try std.testing.expectEqual(@as(u64, 1), updated.document_count);
    try std.testing.expect((try lookup(a, store, updated, "b")) == null);
    try std.testing.expectEqual(@as(u3, 5), (try lookup(a, store, root, "a")).?.present);
    try std.testing.expectEqual(@as(u3, 2), (try lookup(a, store, updated, "a")).?.present);
    for (0..4) |stage| {
        var cursor = try pendingCursor(a, store, updated, stage, "");
        defer cursor.deinit();
        if (stage < 2) {
            try std.testing.expectEqual(null, try cursor.next());
        } else {
            try std.testing.expectEqualStrings("a", (try cursor.next()).?.key);
            try std.testing.expectEqual(null, try cursor.next());
        }
    }
    var invalid = updated.encode();
    // The pending subtree cardinality cannot disagree with its exact counter.
    std.mem.writeInt(u64, invalid[96 + 5 * 8 ..][0..8], 0, .little);
    try std.testing.expectError(error.InvalidDocumentFactsRoot, Root.decode(&invalid));
    invalid = updated.encode();
    invalid[7] = '2';
    try std.testing.expectError(error.InvalidDocumentFactsRoot, Root.decode(&invalid));
    try std.testing.expectError(error.DuplicateDocumentFactReplacement, planAlloc(a, store, root, &.{ .{ .id = "a", .value = fact }, .{ .id = "a", .value = null } }, 2));
}

test "serverless document facts pending indexes skip completed prefixes and refresh unchanged pending bits" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    const count = 4096;
    const ids = try a.alloc([8]u8, count);
    defer a.free(ids);
    const replacements = try a.alloc(Replacement, count);
    defer a.free(replacements);
    const completed = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 1 };
    for (ids, replacements, 0..) |*id, *replacement, i| {
        std.mem.writeInt(u64, id, i, .big);
        var fact = completed;
        if (i >= count - 2) fact.pending = 1;
        replacement.* = .{ .id = id, .value = fact };
    }
    var plan = try planAlloc(a, store, empty, replacements, 1);
    defer plan.deinit();
    const root = try plan.publish(store, empty);
    memory.reads = 0;
    var cursor = try pendingCursor(a, store, root, 0, "");
    defer cursor.deinit();
    try std.testing.expectEqualSlices(u8, &ids[count - 2], (try cursor.next()).?.key);
    try std.testing.expect(memory.reads <= root.pending_pages[0].?.height + 1);
    var changed = completed;
    changed.pending = 1;
    changed.last_lsn = 2;
    changed.body.digest = @splat(7);
    var update = try planAlloc(a, store, root, &.{.{ .id = &ids[count - 1], .value = changed }}, 2);
    defer update.deinit();
    const next = try update.publish(store, root);
    memory.reads = 0;
    var resumed = try pendingCursorAtRank(a, store, next, 0, 1);
    defer resumed.deinit();
    const record = (try resumed.next()).?;
    try std.testing.expectEqualSlices(u8, &ids[count - 1], record.key);
    try std.testing.expectEqual(changed, try Fact.decode(record.value));
    try std.testing.expect(memory.reads <= next.pending_pages[0].?.height + 1);
    try std.testing.expectEqual(null, try resumed.next());
}

test "serverless document facts allocation failures preserve source and release plan ownership" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const source = Root{ .domain = memory.store().domain, .policy_fingerprint = @splat(1) };
    const Exercise = struct {
        fn run(alloc: Allocator, store: tree.Store, empty: Root) !void {
            const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 1 }, .last_lsn = 1, .last_timestamp_ns = 0, .present = 7, .pending = 15 };
            var other = fact;
            other.pending = 1;
            var plan = try planAlloc(alloc, store, empty, &.{ .{ .id = "a", .value = fact }, .{ .id = "b", .value = other } }, 1);
            defer plan.deinit();
            const root = try plan.publish(store, empty);
            var next = try planAlloc(alloc, store, root, &.{ .{ .id = "a", .value = null }, .{ .id = "c", .value = fact } }, 2);
            defer next.deinit();
            const updated = try next.publish(store, root);
            try std.testing.expectEqual(root.counts, updated.counts);
            try std.testing.expect((try lookup(alloc, store, root, "a")) != null);
            try std.testing.expect((try lookup(alloc, store, updated, "a")) == null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Exercise.run, .{ memory.store(), source });
}

test "serverless document facts pending cursors reject wrong-stage future-source and forged ordering entries" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    var fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 1 }, .last_lsn = 1, .last_timestamp_ns = 0 };
    for (0..4) |case| {
        fact.pending = if (case == 0) 0 else 1;
        fact.last_lsn = if (case == 1) 2 else 1;
        const value = fact.encode();
        const key = try pendingOrderKeyAlloc(a, if (case == 2) 0 else fact.last_lsn, "a");
        defer a.free(key);
        const page = (try tree.apply(a, store, null, &.{.{ .key = if (case == 3) key[0..8] else key, .value = &value }})).?;
        const root = Root{ .domain = store.domain, .policy_fingerprint = @splat(1), .wal_end_lsn = 1, .page = page, .pending_pages = .{ page, null, null, null }, .document_count = 1, .counts = .{ 0, 0, 0, 1, 0, 0, 0 } };
        var cursor = try pendingCursor(a, store, root, 0, "");
        defer cursor.deinit();
        try std.testing.expectError(error.InvalidDocumentFact, cursor.next());
    }
}

test "serverless document facts LSN ordering freezes membership despite middle-key arrivals and replaces old pending keys" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    const old = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 1, .pending = 15 };
    var tail = old;
    tail.last_lsn = 2;
    var initial = try planAlloc(a, store, empty, &.{ .{ .id = "a", .value = old }, .{ .id = "z", .value = tail } }, 2);
    defer initial.deinit();
    const source = try initial.publish(store, empty);
    const bound = try pendingOrderKeyAlloc(a, 2, "z");
    defer a.free(bound);
    var middle = old;
    middle.last_lsn = 3;
    var changed = old;
    changed.last_lsn = 4;
    changed.body.digest = @splat(7);
    var update = try planAlloc(a, store, source, &.{ .{ .id = "m", .value = middle }, .{ .id = "a", .value = changed } }, 4);
    defer update.deinit();
    for (update.pending_changes) |changes| try std.testing.expectEqual(@as(usize, 3), changes.len);
    const current = try update.publish(store, source);
    for (0..4) |stage| {
        var cursor = try pendingCursor(a, store, current, stage, "");
        defer cursor.deinit();
        const first = (try cursor.next()).?;
        try std.testing.expectEqualStrings("z", first.key);
        try std.testing.expectEqualSlices(u8, bound, first.order_key);
        const second = (try cursor.next()).?;
        try std.testing.expectEqualStrings("m", second.key);
        try std.testing.expect(std.mem.order(u8, second.order_key, bound) == .gt);
        const third = (try cursor.next()).?;
        try std.testing.expectEqualStrings("a", third.key);
        try std.testing.expectEqual(changed, try Fact.decode(third.value));
        try std.testing.expectEqual(null, try cursor.next());
        var resumed = try pendingCursor(a, store, current, stage, bound);
        defer resumed.deinit();
        try std.testing.expectEqualStrings("z", (try resumed.next()).?.key);
    }
}

test "serverless document facts pending ordering preserves maximum accepted document identifiers" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    const id = try a.alloc(u8, max_document_id_bytes);
    defer a.free(id);
    @memset(id, 'k');
    const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 1, .pending = 1 };
    var initial = try planAlloc(a, store, empty, &.{.{ .id = id, .value = fact }}, 1);
    defer initial.deinit();
    const current = try initial.publish(store, empty);
    var cursor = try pendingCursor(a, store, current, 0, "");
    defer cursor.deinit();
    const record = (try cursor.next()).?;
    try std.testing.expectEqualSlices(u8, id, record.key);
    try std.testing.expectEqual(@as(usize, max_pending_key_bytes), record.order_key.len);
    try std.testing.expectEqual(fact, (try lookup(a, store, current, id)).?);
}

test "model aware media audit cycle keeps new arrivals beyond its captured boundary" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    var fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 10, .present = 5, .pending = 0 };
    var initial = try planAlloc(a, store, empty, &.{ .{ .id = "a", .value = fact }, .{ .id = "z", .value = fact } }, 1);
    defer initial.deinit();
    const original = try initial.publish(store, empty);
    var tail = try mediaCursor(a, store, original, "", 1);
    defer tail.deinit();
    const upper = try a.dupe(u8, (try tail.next()).?.order_key);
    defer a.free(upper);
    var first = try mediaCursor(a, store, original, "", null);
    defer first.deinit();
    const after = try a.dupe(u8, (try first.next()).?.order_key);
    defer a.free(after);
    fact.last_lsn = 2;
    var arrivals = try planAlloc(a, store, original, &.{.{ .id = "a0", .value = fact }}, 2);
    defer arrivals.deinit();
    const next_head = try arrivals.publish(store, original);
    var resumed = try mediaCursor(a, store, next_head, after, null);
    defer resumed.deinit();
    var next = (try resumed.next()).?;
    if (std.mem.eql(u8, next.order_key, after)) next = (try resumed.next()).?;
    // A finite cycle captured at WAL 1 must not admit the WAL 2 arrival.
    if (std.mem.order(u8, next.order_key, upper) != .gt) try std.testing.expectEqualStrings("z", next.key);
}

test "model aware media checkpoint bodies retain and reclaim their authenticated ancestry" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/checkpoint-retention", .{tmp.sub_path});
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    defer fs.deinit();
    var objects = fs.artifactStore();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &objects, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const parent = try putBody(a, &pages, "{\"_enrichment\":{\"chunk_embeddings_checkpoint\":{\"format\":1}}}");
    const child_bytes = try std.json.Stringify.valueAlloc(a, .{ ._enrichment = .{ .chunk_embeddings_checkpoint = .{ .format = 1, .parent = parent } } }, .{});
    defer a.free(child_bytes);
    const child = try putBody(a, &pages, child_bytes);
    const initial = Root{ .domain = pages.domain, .policy_fingerprint = @splat(2) };
    var first = try planAlloc(a, pages.store(), initial, &.{.{ .id = "doc", .value = .{ .body = parent, .body_has_dependencies = true, .last_lsn = 1, .last_timestamp_ns = 1 } }}, 1);
    defer first.deinit();
    const first_root = try first.publish(pages.store(), initial);
    const first_ref = try publishRoot(a, &pages, first_root);
    defer {
        a.free(first_ref.artifact_id);
        a.free(first_ref.checksum);
    }
    var second = try planAlloc(a, pages.store(), first_root, &.{.{ .id = "doc", .value = .{ .body = child, .body_has_dependencies = true, .last_lsn = 2, .last_timestamp_ns = 2 } }}, 2);
    defer second.deinit();
    const second_root = try second.publish(pages.store(), first_root);
    const second_ref = try publishRoot(a, &pages, second_root);
    defer {
        a.free(second_ref.artifact_id);
        a.free(second_ref.checksum);
    }
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var keys = retained.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        retained.deinit(a);
    }
    try retainRoot(a, &pages, second_ref, &retained);
    try std.testing.expect(retained.contains(&try parent.identity(pages.domain)));
    try std.testing.expect(retained.contains(&try child.identity(pages.domain)));
    _ = try reclaimRoot(a, &pages, first_ref, &retained);
    const old_body = try readBodyAlloc(a, &pages, parent);
    defer a.free(old_body);
    var empty: std.StringHashMapUnmanaged(void) = .empty;
    defer empty.deinit(a);
    try std.testing.expect(try reclaimRoot(a, &pages, second_ref, &empty) >= 3);
    try std.testing.expectError(error.FileNotFound, objects.getAlloc(&try parent.identity(pages.domain)));
}

test "model aware media serverless compact checkpoint bodies retain and reclaim shared source and output ancestry" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/checkpoint-retention", .{tmp.sub_path});
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    defer fs.deinit();
    var objects = fs.artifactStore();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &objects, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const ancestor = try putBody(a, &pages, "{\"source\":\"original input\"}");
    const legacy = try std.json.Stringify.valueAlloc(a, .{ .source = "original input", ._enrichment = .{ .chunk_embeddings_checkpoint = .{ .format = 1, .parent = ancestor } } }, .{});
    defer a.free(legacy);
    const base = try putBody(a, &pages, legacy);
    const first_page = try std.json.Stringify.valueAlloc(a, .{ ._enrichment = .{ .chunk_embeddings_checkpoint = .{ .format = 1, .parent = base } } }, .{});
    defer a.free(first_page);
    const parent_payload = try encodeCheckpointBodyAlloc(a, base, first_page);
    defer a.free(parent_payload);
    const parent = try putBody(a, &pages, parent_payload);
    const child_bytes = try std.json.Stringify.valueAlloc(a, .{ ._enrichment = .{ .chunk_embeddings_checkpoint = .{ .format = 1, .parent = parent } } }, .{});
    defer a.free(child_bytes);
    const child_payload = try encodeCheckpointBodyAlloc(a, base, child_bytes);
    defer a.free(child_payload);
    const child = try putBody(a, &pages, child_payload);
    const initial = Root{ .domain = pages.domain, .policy_fingerprint = @splat(2) };
    var first = try planAlloc(a, pages.store(), initial, &.{.{ .id = "doc", .value = .{ .body = parent, .body_has_dependencies = true, .last_lsn = 1, .last_timestamp_ns = 1 } }}, 1);
    defer first.deinit();
    const first_root = try first.publish(pages.store(), initial);
    const first_ref = try publishRoot(a, &pages, first_root);
    defer {
        a.free(first_ref.artifact_id);
        a.free(first_ref.checksum);
    }
    var second = try planAlloc(a, pages.store(), first_root, &.{.{ .id = "doc", .value = .{ .body = child, .body_has_dependencies = true, .last_lsn = 2, .last_timestamp_ns = 2 } }}, 2);
    defer second.deinit();
    const second_root = try second.publish(pages.store(), first_root);
    const second_ref = try publishRoot(a, &pages, second_root);
    defer {
        a.free(second_ref.artifact_id);
        a.free(second_ref.checksum);
    }
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var keys = retained.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        retained.deinit(a);
    }
    try retainRoot(a, &pages, second_ref, &retained);
    try std.testing.expect(retained.contains(&try ancestor.identity(pages.domain)));
    try std.testing.expect(retained.contains(&try base.identity(pages.domain)));
    try std.testing.expect(retained.contains(&try parent.identity(pages.domain)));
    try std.testing.expect(retained.contains(&try child.identity(pages.domain)));
    _ = try reclaimRoot(a, &pages, first_ref, &retained);
    const old_body = try readBodyAlloc(a, &pages, parent);
    defer a.free(old_body);
    var empty: std.StringHashMapUnmanaged(void) = .empty;
    defer empty.deinit(a);
    try std.testing.expect(try reclaimRoot(a, &pages, second_ref, &empty) >= 3);
    try std.testing.expectError(error.FileNotFound, objects.getAlloc(&try parent.identity(pages.domain)));
    try std.testing.expectError(error.FileNotFound, objects.getAlloc(&try base.identity(pages.domain)));
    try std.testing.expectError(error.FileNotFound, objects.getAlloc(&try ancestor.identity(pages.domain)));
}

test "serverless document facts decode legacy records and fence witness extensions" {
    const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 128 }, .last_lsn = 1, .last_timestamp_ns = 2 };
    var bytes = fact.encode();
    try std.testing.expectEqualDeep(fact, try Fact.decode(bytes[0..80]));
    var witnessed = fact;
    witnessed.media_audit = .{ .digest = @splat(4), .attempt = @splat(2), .bytes = 512 };
    bytes = witnessed.encode();
    try std.testing.expectEqualDeep(witnessed, try Fact.decode(&bytes));
    try std.testing.expectError(error.InvalidDocumentFact, Fact.decode(bytes[0..80]));
    bytes[75] = 0;
    try std.testing.expectError(error.InvalidDocumentFact, Fact.decode(&bytes));
    bytes = fact.encode();
    bytes[75] = 2;
    try std.testing.expectError(error.InvalidDocumentFact, Fact.decode(&bytes));
    const root = Root{ .domain = @splat(1), .policy_fingerprint = @splat(2) };
    var encoded = root.encode();
    @memcpy(encoded[0..8], "AFDFACT7");
    try std.testing.expectEqualDeep(root, try Root.decode(&encoded));
}
