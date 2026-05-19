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
const ArrayList = std.ArrayList;

const backup_codec = @import("backup_codec.zig");
const internal_keys = @import("internal_keys.zig");
const docstore_mod = @import("docstore.zig");
const DocStore = docstore_mod.DocStore;
const KeyEncoder = docstore_mod.KeyEncoder;
const KVPair = docstore_mod.KVPair;
const OwnedKVPair = docstore_mod.OwnedKVPair;

/// Target batch size in bytes before flushing a document/embedding/edge batch.
const batch_target_bytes: usize = 4 * 1024 * 1024;

// ============================================================================
// Export
// ============================================================================

/// Export all portable data from the DocStore into AFB format.
/// The caller provides an allocator for temporary buffers. The output is
/// appended to `out`.
pub fn exportPortable(alloc: Allocator, store: *DocStore, out: *ArrayList(u8)) !void {
    const pairs = try store.scanRange(alloc, "", "");
    defer DocStore.freeResults(alloc, pairs);

    // Write file header
    const backup_id = [_]u8{0} ** 16; // zero UUID for now
    try backup_codec.writeHeader(out, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0, // timestamp filled by caller if needed
        .backup_id = backup_id,
        .table_count = 1,
        .shard_count = 1,
    });

    // Cluster manifest
    try backup_codec.writeBlock(out, alloc, .cluster_manifest, "{}");

    // Table manifest
    try backup_codec.writeBlock(out, alloc, .table_manifest, "{}");

    // Shard header
    const shard_hdr = try backup_codec.encodeShardHeader(alloc, .{
        .table_name = "",
        .shard_id = 0,
        .start_key = "",
        .end_key = "",
    });
    defer alloc.free(shard_hdr);
    try backup_codec.writeBlock(out, alloc, .shard_header, shard_hdr);

    // Classify and batch all keys
    var doc_batch = std.ArrayListUnmanaged(backup_codec.DocumentEntry).empty;
    defer doc_batch.deinit(alloc);
    var doc_batch_bytes: usize = 0;

    // Embeddings keyed by index name
    var emb_batches = std.StringHashMapUnmanaged(EmbeddingBatch).empty;
    defer {
        var it = emb_batches.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        emb_batches.deinit(alloc);
    }

    // Edges keyed by index name
    var edge_batches = std.StringHashMapUnmanaged(EdgeBatch).empty;
    defer {
        var eit = edge_batches.iterator();
        while (eit.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        edge_batches.deinit(alloc);
    }

    var counts = Counts{};

    for (pairs) |kv| {
        // Binary internal keys (0x01 prefix)
        if (internal_keys.isInternalUserKey(kv.key)) {
            if (internal_keys.isPrimaryDocumentKey(kv.key)) {
                const user_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, kv.key)) orelse continue;
                defer alloc.free(user_key);

                try doc_batch.append(alloc, .{
                    .key = try alloc.dupe(u8, user_key),
                    .value_flags = 0,
                    .value = try alloc.dupe(u8, kv.value),
                    .timestamp_ns = 0,
                });
                doc_batch_bytes += user_key.len + kv.value.len;

                if (doc_batch_bytes >= batch_target_bytes) {
                    try flushDocBatch(alloc, out, &doc_batch, &counts);
                    doc_batch_bytes = 0;
                }
            } else if (internal_keys.isEmbeddingArtifactKey(kv.key)) {
                try collectEmbedding(alloc, &emb_batches, kv.key, kv.value);
            }
            // Skip: TTL, summary, chunk, derived embedding keys
            continue;
        }

        // Colon-delimited keys — check for outgoing edges
        if (KeyEncoder.isEdgeKey(kv.key)) {
            // Only export outgoing edges (ending with ":o")
            if (kv.key.len >= 2 and kv.key[kv.key.len - 1] == 'o' and kv.key[kv.key.len - 2] == ':') {
                const parsed = KeyEncoder.parseEdgeKey(kv.key) orelse continue;
                const idx_name = try alloc.dupe(u8, parsed.index_name);
                const gop = try edge_batches.getOrPut(alloc, idx_name);
                if (!gop.found_existing) {
                    gop.value_ptr.* = EdgeBatch.init();
                } else {
                    alloc.free(idx_name);
                }
                try gop.value_ptr.entries.append(alloc, .{
                    .source_key = try alloc.dupe(u8, parsed.source),
                    .target_key = try alloc.dupe(u8, parsed.target),
                    .edge_type = try alloc.dupe(u8, parsed.edge_type),
                    .value = try alloc.dupe(u8, kv.value),
                });
            }
            // Skip incoming edges (":i" suffix)
        }
        // Skip any other colon-delimited keys (summaries, enrichments, etc.)
    }

    // Flush remaining documents
    if (doc_batch.items.len > 0) {
        try flushDocBatch(alloc, out, &doc_batch, &counts);
    }

    // Flush embedding batches
    {
        var it = emb_batches.iterator();
        while (it.next()) |entry| {
            const batch = entry.value_ptr;
            if (batch.entries.items.len == 0) continue;
            const dim: u16 = if (batch.entries.items.len > 0) batch.dimension else 0;
            const encoded = try backup_codec.encodeEmbeddingBatch(alloc, entry.key_ptr.*, dim, batch.entries.items);
            defer alloc.free(encoded);
            try backup_codec.writeBlock(out, alloc, .embedding_batch, encoded);
            counts.embeddings += batch.entries.items.len;
        }
    }

    // Flush edge batches
    {
        var eit = edge_batches.iterator();
        while (eit.next()) |entry| {
            const batch = entry.value_ptr;
            if (batch.entries.items.len == 0) continue;
            const encoded = try backup_codec.encodeEdgeBatch(alloc, entry.key_ptr.*, batch.entries.items);
            defer alloc.free(encoded);
            try backup_codec.writeBlock(out, alloc, .edge_batch, encoded);
            counts.edges += batch.entries.items.len;
        }
    }

    // Shard footer
    const shard_footer = backup_codec.encodeShardFooter(.{
        .shard_id = 0,
        .document_count = counts.documents,
        .embedding_count = counts.embeddings,
        .edge_count = counts.edges,
        .transaction_count = 0,
    });
    try backup_codec.writeBlock(out, alloc, .shard_footer, &shard_footer);

    // File footer
    const file_footer = backup_codec.encodeFileFooter(.{
        .table_count = 1,
        .shard_count = 1,
        .total_documents = counts.documents,
        .total_bytes = out.items.len,
    });
    try backup_codec.writeBlock(out, alloc, .file_footer, &file_footer);
}

const Counts = struct {
    documents: u64 = 0,
    embeddings: u64 = 0,
    edges: u64 = 0,
};

const EmbeddingBatch = struct {
    entries: std.ArrayListUnmanaged(backup_codec.EmbeddingEntry),
    dimension: u16,

    fn init() EmbeddingBatch {
        return .{
            .entries = .empty,
            .dimension = 0,
        };
    }

    fn deinit(self: *EmbeddingBatch, alloc: Allocator) void {
        for (self.entries.items) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.vector);
        }
        self.entries.deinit(alloc);
    }
};

const EdgeBatch = struct {
    entries: std.ArrayListUnmanaged(backup_codec.EdgeEntry),

    fn init() EdgeBatch {
        return .{ .entries = .empty };
    }

    fn deinit(self: *EdgeBatch, alloc: Allocator) void {
        for (self.entries.items) |e| {
            alloc.free(e.source_key);
            alloc.free(e.target_key);
            alloc.free(e.edge_type);
            alloc.free(e.value);
        }
        self.entries.deinit(alloc);
    }
};

fn flushDocBatch(
    alloc: Allocator,
    out: *ArrayList(u8),
    batch: *std.ArrayListUnmanaged(backup_codec.DocumentEntry),
    counts: *Counts,
) !void {
    const encoded = try backup_codec.encodeDocumentBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try backup_codec.writeBlock(out, alloc, .document_batch, encoded);
    counts.documents += batch.items.len;

    // Free owned entry data
    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

/// Parse an embedding artifact's JSON value and collect into the appropriate batch.
fn collectEmbedding(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(EmbeddingBatch),
    key: []const u8,
    value: []const u8,
) !void {
    const parsed_key = (try internal_keys.parseEmbeddingArtifactKeyAlloc(alloc, key)) orelse return;
    defer alloc.free(parsed_key.doc_key);
    defer alloc.free(parsed_key.artifact_name);

    // Parse JSON value: {"dims": N, "vector": [...]}
    const EmbPayload = struct {
        dims: u32,
        vector: []f32,
    };
    const json_parsed = std.json.parseFromSlice(EmbPayload, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return; // skip malformed embeddings
    defer json_parsed.deinit();

    const idx_name = try alloc.dupe(u8, parsed_key.artifact_name);
    const gop = try batches.getOrPut(alloc, idx_name);
    if (!gop.found_existing) {
        gop.value_ptr.* = EmbeddingBatch.init();
        gop.value_ptr.dimension = @intCast(json_parsed.value.dims);
    } else {
        alloc.free(idx_name);
    }

    const vec = try alloc.dupe(f32, json_parsed.value.vector);
    try gop.value_ptr.entries.append(alloc, .{
        .doc_key = try alloc.dupe(u8, parsed_key.doc_key),
        .hash_id = 0, // Zig doesn't store hash_id in embedding values
        .vector = vec,
    });
}

// ============================================================================
// Import
// ============================================================================

/// Import AFB data into the DocStore.
pub fn importPortable(alloc: Allocator, store: *DocStore, data: []const u8) !void {
    var reader = backup_codec.SliceReader.init(data);
    _ = try reader.readHeader();

    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);

        switch (block.block_type) {
            .document_batch => try importDocumentBatch(alloc, store, block.payload),
            .embedding_batch => try importEmbeddingBatch(alloc, store, block.payload),
            .edge_batch => try importEdgeBatch(alloc, store, block.payload),
            // Skip: sparse, summary, chunk, transaction (rebuilt by enrichment)
            .cluster_manifest, .table_manifest, .shard_header, .shard_footer, .file_footer => {},
            else => {},
        }
    }
}

fn importDocumentBatch(alloc: Allocator, store: *DocStore, payload: []const u8) !void {
    const entries = try backup_codec.decodeDocumentBatch(alloc, payload);
    defer {
        for (entries) |e| {
            alloc.free(e.key);
            alloc.free(e.value);
        }
        alloc.free(entries);
    }

    // Build KV pairs with internal keys
    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |k| alloc.free(k);
        owned_keys.deinit(alloc);
    }

    for (entries) |e| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, e.key);
        try owned_keys.append(alloc, store_key);
        try writes.append(alloc, .{ .key = store_key, .value = e.value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importEmbeddingBatch(alloc: Allocator, store: *DocStore, payload: []const u8) !void {
    const result = try backup_codec.decodeEmbeddingBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.vector);
        }
        alloc.free(result.entries);
    }

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |k| alloc.free(k);
        owned_keys.deinit(alloc);
    }
    var owned_vals = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_vals.items) |v| alloc.free(v);
        owned_vals.deinit(alloc);
    }

    for (result.entries) |e| {
        const store_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, e.doc_key, result.index_name);
        try owned_keys.append(alloc, store_key);

        // Encode as JSON: {"dims": N, "vector": [...]}
        const EmbPayload = struct {
            dims: u32,
            vector: []const f32,
        };
        const json_val = try std.json.Stringify.valueAlloc(alloc, EmbPayload{
            .dims = result.dimension,
            .vector = e.vector,
        }, .{});
        try owned_vals.append(alloc, json_val);
        try writes.append(alloc, .{ .key = store_key, .value = json_val });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importEdgeBatch(alloc: Allocator, store: *DocStore, payload: []const u8) !void {
    const result = try decodeEdgeBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |e| {
            alloc.free(e.source_key);
            alloc.free(e.target_key);
            alloc.free(e.edge_type);
            alloc.free(e.value);
        }
        alloc.free(result.entries);
    }

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |k| alloc.free(k);
        owned_keys.deinit(alloc);
    }

    for (result.entries) |e| {
        // Build colon-delimited edge key
        const key_len = e.source_key.len + 3 + result.index_name.len + 5 + e.edge_type.len + 1 + e.target_key.len + 2;
        const edge_key = try alloc.alloc(u8, key_len);
        const written = std.fmt.bufPrint(edge_key, "{s}:i:{s}:out:{s}:{s}:o", .{
            e.source_key, result.index_name, e.edge_type, e.target_key,
        }) catch unreachable;
        const owned_key = try alloc.dupe(u8, written);
        alloc.free(edge_key);
        try owned_keys.append(alloc, owned_key);
        try writes.append(alloc, .{ .key = owned_key, .value = e.value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

/// Decode an edge batch payload (mirrors backup_codec.encodeEdgeBatch).
fn decodeEdgeBatch(alloc: Allocator, data: []const u8) !struct {
    index_name: []u8,
    entries: []backup_codec.EdgeEntry,
} {
    if (data.len < 4) return error.BatchTooShort;
    var off: usize = 0;

    const name_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + name_len > data.len) return error.Truncated;
    const index_name = try alloc.dupe(u8, data[off..][0..name_len]);
    errdefer alloc.free(index_name);
    off += name_len;

    if (off + 4 > data.len) return error.Truncated;
    const count = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    var entries = try std.ArrayListUnmanaged(backup_codec.EdgeEntry).initCapacity(alloc, count);
    errdefer {
        for (entries.items) |e| {
            alloc.free(e.source_key);
            alloc.free(e.target_key);
            alloc.free(e.edge_type);
            alloc.free(e.value);
        }
        entries.deinit(alloc);
    }

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > data.len) return error.Truncated;
        const src_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + src_len > data.len) return error.Truncated;
        const source_key = try alloc.dupe(u8, data[off..][0..src_len]);
        off += src_len;

        if (off + 4 > data.len) return error.Truncated;
        const tgt_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + tgt_len > data.len) return error.Truncated;
        const target_key = try alloc.dupe(u8, data[off..][0..tgt_len]);
        off += tgt_len;

        if (off + 4 > data.len) return error.Truncated;
        const etype_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + etype_len > data.len) return error.Truncated;
        const edge_type = try alloc.dupe(u8, data[off..][0..etype_len]);
        off += etype_len;

        if (off + 4 > data.len) return error.Truncated;
        const val_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + val_len > data.len) return error.Truncated;
        const value = try alloc.dupe(u8, data[off..][0..val_len]);
        off += val_len;

        try entries.append(alloc, .{
            .source_key = source_key,
            .target_key = target_key,
            .edge_type = edge_type,
            .value = value,
        });
    }

    return .{
        .index_name = index_name,
        .entries = try entries.toOwnedSlice(alloc),
    };
}

// ============================================================================
// Tests
// ============================================================================

fn openTestStore(alloc: Allocator, tmp: *std.testing.TmpDir) !DocStore {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    return DocStore.open(alloc, path_z, .{});
}

test "exportPortable empty store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try openTestStore(alloc, &tmp);
    defer store.close();

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);

    try exportPortable(alloc, &store, &out);

    // Should produce a valid AFB file
    try std.testing.expect(backup_codec.isAfbFormat(out.items));
    try std.testing.expect(out.items.len > backup_codec.header_size);
}

test "export and import documents round trip" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_keys = [_][]const u8{ "doc1", "doc2", "doc3" };
    const doc_vals = [_][]const u8{
        "{\"id\":\"doc1\",\"title\":\"Hello\"}",
        "{\"id\":\"doc2\",\"title\":\"World\"}",
        "{\"id\":\"doc3\",\"title\":\"Test\"}",
    };

    // Write documents using internal key encoding
    for (doc_keys, doc_vals) |dk, dv| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, dk);
        defer alloc.free(store_key);
        try src.putBatch(&.{.{ .key = store_key, .value = dv }}, &.{});
    }

    // Export
    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    // Import into fresh store
    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    // Verify all documents
    for (doc_keys, doc_vals) |dk, expected| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, dk);
        defer alloc.free(store_key);
        const val = try dst.get(alloc, store_key);
        defer alloc.free(val);
        try std.testing.expectEqualStrings(expected, val);
    }
}

test "export and import embeddings round trip" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    // Write a document
    const doc_store_key = try internal_keys.documentKeyAlloc(alloc, "emb-doc");
    defer alloc.free(doc_store_key);
    try src.putBatch(&.{.{ .key = doc_store_key, .value = "{\"id\":\"emb-doc\"}" }}, &.{});

    // Write an embedding artifact (JSON value)
    const emb_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "emb-doc", "my_index");
    defer alloc.free(emb_key);
    const emb_val = "{\"dims\":4,\"vector\":[0.1,0.2,0.3,0.4]}";
    try src.putBatch(&.{.{ .key = emb_key, .value = emb_val }}, &.{});

    // Export
    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    // Import into fresh store
    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    // Verify document
    {
        const val = try dst.get(alloc, doc_store_key);
        defer alloc.free(val);
        try std.testing.expectEqualStrings("{\"id\":\"emb-doc\"}", val);
    }

    // Verify embedding
    {
        const restored_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "emb-doc", "my_index");
        defer alloc.free(restored_key);
        const val = try dst.get(alloc, restored_key);
        defer alloc.free(val);

        // Parse restored JSON
        const EmbPayload = struct { dims: u32, vector: []f32 };
        const parsed = try std.json.parseFromSlice(EmbPayload, alloc, val, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u32, 4), parsed.value.dims);
        try std.testing.expectEqual(@as(usize, 4), parsed.value.vector.len);
        try std.testing.expectApproxEqAbs(@as(f32, 0.1), parsed.value.vector[0], 1e-6);
        try std.testing.expectApproxEqAbs(@as(f32, 0.4), parsed.value.vector[3], 1e-6);
    }
}

test "export and import edges round trip" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    // Write source and target documents
    for ([_][]const u8{ "alice", "bob" }) |dk| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, dk);
        defer alloc.free(store_key);
        const val = try std.fmt.allocPrint(alloc, "{{\"id\":\"{s}\"}}", .{dk});
        defer alloc.free(val);
        try src.putBatch(&.{.{ .key = store_key, .value = val }}, &.{});
    }

    // Write an outgoing edge: alice -> bob
    var edge_buf: [256]u8 = undefined;
    const edge_key = KeyEncoder.makeEdgeKey(&edge_buf, "alice", "social", "follows", "bob");
    const edge_val = "{}";
    try src.putBatch(&.{.{ .key = edge_key, .value = edge_val }}, &.{});

    // Export
    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    // Import into fresh store
    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    // Verify edge
    {
        const restored_edge_key = KeyEncoder.makeEdgeKey(&edge_buf, "alice", "social", "follows", "bob");
        const val = try dst.get(alloc, restored_edge_key);
        defer alloc.free(val);
        try std.testing.expectEqualStrings("{}", val);
    }
}

test "export skips derived data" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    // Write a document
    const doc_key = try internal_keys.documentKeyAlloc(alloc, "skip-doc");
    defer alloc.free(doc_key);
    try src.putBatch(&.{.{ .key = doc_key, .value = "{\"id\":\"skip-doc\"}" }}, &.{});

    // Write a summary artifact (should be skipped)
    const summary_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "skip-doc", "summary", "my_summary");
    defer alloc.free(summary_key);
    try src.putBatch(&.{.{ .key = summary_key, .value = "some summary text" }}, &.{});

    // Write an incoming edge (should be skipped)
    var edge_buf: [256]u8 = undefined;
    const rev_key = KeyEncoder.makeReverseEdgeKey(&edge_buf, "skip-doc", "social", "follows", "other");
    try src.putBatch(&.{.{ .key = rev_key, .value = "{}" }}, &.{});

    // Export
    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    // Import into fresh store
    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    // Document should exist
    {
        const val = try dst.get(alloc, doc_key);
        defer alloc.free(val);
        try std.testing.expectEqualStrings("{\"id\":\"skip-doc\"}", val);
    }

    // Summary should NOT exist
    {
        const val = dst.get(alloc, summary_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        try std.testing.expectEqual(null, val);
    }

    // Incoming edge should NOT exist
    {
        const val = dst.get(alloc, rev_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        try std.testing.expectEqual(null, val);
    }
}
