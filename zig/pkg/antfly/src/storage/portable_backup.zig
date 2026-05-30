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
const doc_identity = @import("db/doc_identity.zig");
const enrichment_artifact_codec = @import("db/enrichment/artifact_codec.zig");
const DocStore = docstore_mod.DocStore;
const KeyEncoder = docstore_mod.KeyEncoder;
const KVPair = docstore_mod.KVPair;

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
    const backup_id = @as([16]u8, @splat(0)); // zero UUID for now
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

    var identity_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer identity_batch.deinit(alloc);
    var identity_batch_bytes: usize = 0;

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
        if (kv.key.len > 0 and kv.key[0] == internal_keys.identity_namespace) {
            try identity_batch.append(alloc, .{
                .key = try alloc.dupe(u8, kv.key),
                .value = try alloc.dupe(u8, kv.value),
            });
            identity_batch_bytes += kv.key.len + kv.value.len;
            if (identity_batch_bytes >= batch_target_bytes) {
                try flushIdentityBatch(alloc, out, &identity_batch);
                identity_batch_bytes = 0;
            }
            continue;
        }

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
            } else if (internal_keys.isGraphEdgeArtifactKey(kv.key)) {
                try collectGraphEdgeArtifact(alloc, &edge_batches, kv.key, kv.value);
            } else if (try parseStandaloneGraphIndexEdgeKeyAlloc(alloc, kv.key)) |parsed| {
                defer parsed.deinit(alloc);
                try appendEdgeBatchEntry(alloc, &edge_batches, parsed.index_name, parsed.source, parsed.target, parsed.edge_type, kv.value);
            }
            // Skip: TTL, summary, chunk, derived embedding keys
            continue;
        }

        // Colon-delimited keys — check for outgoing edges
        if (KeyEncoder.isEdgeKey(kv.key)) {
            // Only export outgoing edges (ending with ":o")
            if (kv.key.len >= 2 and kv.key[kv.key.len - 1] == 'o' and kv.key[kv.key.len - 2] == ':') {
                const parsed = KeyEncoder.parseEdgeKey(kv.key) orelse continue;
                try appendEdgeBatchEntry(alloc, &edge_batches, parsed.index_name, parsed.source, parsed.target, parsed.edge_type, kv.value);
            }
            // Skip incoming edges (":i" suffix)
        }
        // Skip any other colon-delimited keys (summaries, enrichments, etc.)
    }

    // Flush remaining documents
    if (doc_batch.items.len > 0) {
        try flushDocBatch(alloc, out, &doc_batch, &counts);
    }
    if (identity_batch.items.len > 0) {
        try flushIdentityBatch(alloc, out, &identity_batch);
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

const ParsedStandaloneGraphEdgeKey = struct {
    source: []u8,
    index_name: []u8,
    edge_type: []u8,
    target: []u8,

    fn deinit(self: ParsedStandaloneGraphEdgeKey, alloc: Allocator) void {
        alloc.free(self.source);
        alloc.free(self.index_name);
        alloc.free(self.edge_type);
        alloc.free(self.target);
    }
};

fn parseStandaloneGraphIndexEdgeKeyAlloc(alloc: Allocator, key: []const u8) !?ParsedStandaloneGraphEdgeKey {
    if (!internal_keys.isInternalUserKey(key)) return null;
    const doc_term = internal_keys.findComponentTerminator(key, 1) orelse return null;
    const source = try internal_keys.decodeBodyAlloc(alloc, key[1..doc_term]);
    var source_owned = true;
    defer if (source_owned) alloc.free(source);

    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != internal_keys.artifact_kind) return null;
    pos += 1;

    if (!internal_keys.componentEquals(key, pos, "graph_index")) return null;
    pos = (internal_keys.findComponentTerminator(key, pos) orelse return null) + 2;

    const index_term = internal_keys.findComponentTerminator(key, pos) orelse return null;
    const index_name = try internal_keys.decodeBodyAlloc(alloc, key[pos..index_term]);
    var index_owned = true;
    defer if (index_owned) alloc.free(index_name);
    pos = index_term + 2;

    if (pos >= key.len or key[pos] != internal_keys.graph_edge_record_kind) return null;
    pos += 1;

    const edge_type_term = internal_keys.findComponentTerminator(key, pos) orelse return null;
    const edge_type = try internal_keys.decodeBodyAlloc(alloc, key[pos..edge_type_term]);
    var edge_type_owned = true;
    defer if (edge_type_owned) alloc.free(edge_type);
    pos = edge_type_term + 2;

    const target_term = internal_keys.findComponentTerminator(key, pos) orelse return null;
    if (target_term + 2 != key.len) return null;
    const target = try internal_keys.decodeBodyAlloc(alloc, key[pos..target_term]);
    errdefer alloc.free(target);

    source_owned = false;
    index_owned = false;
    edge_type_owned = false;

    return .{
        .source = source,
        .index_name = index_name,
        .edge_type = edge_type,
        .target = target,
    };
}

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

fn flushIdentityBatch(
    alloc: Allocator,
    out: *ArrayList(u8),
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
) !void {
    const encoded = try backup_codec.encodeKeyValueBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try backup_codec.writeBlock(out, alloc, .doc_identity_batch, encoded);

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

fn collectGraphEdgeArtifact(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(EdgeBatch),
    key: []const u8,
    value: []const u8,
) !void {
    const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, key)) orelse return;
    defer {
        alloc.free(parsed.doc_key);
        alloc.free(parsed.index_name);
        alloc.free(parsed.edge_type);
        alloc.free(parsed.target_doc_key);
    }

    try appendEdgeBatchEntry(alloc, batches, parsed.index_name, parsed.doc_key, parsed.target_doc_key, parsed.edge_type, value);
}

fn appendEdgeBatchEntry(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(EdgeBatch),
    index_name: []const u8,
    source_key: []const u8,
    target_key: []const u8,
    edge_type: []const u8,
    value: []const u8,
) !void {
    const idx_name = try alloc.dupe(u8, index_name);
    const gop = try batches.getOrPut(alloc, idx_name);
    if (!gop.found_existing) {
        gop.value_ptr.* = EdgeBatch.init();
    } else {
        alloc.free(idx_name);
    }

    try gop.value_ptr.entries.append(alloc, .{
        .source_key = try alloc.dupe(u8, source_key),
        .target_key = try alloc.dupe(u8, target_key),
        .edge_type = try alloc.dupe(u8, edge_type),
        .value = try alloc.dupe(u8, value),
    });
}

// ============================================================================
// Import
// ============================================================================

pub const ImportOptions = struct {
    identity_namespace: ?doc_identity.Namespace = null,
    prefer_existing_identity_namespace: bool = false,
};

/// Import AFB data into the DocStore.
pub fn importPortable(alloc: Allocator, store: *DocStore, data: []const u8) !void {
    return try importPortableWithOptions(alloc, store, data, .{});
}

pub fn importPortableWithOptions(alloc: Allocator, store: *DocStore, data: []const u8, opts: ImportOptions) !void {
    var reader = backup_codec.SliceReader.init(data);
    _ = try reader.readHeader();
    var imported_identity = false;

    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);

        switch (block.block_type) {
            .document_batch => try importDocumentBatch(alloc, store, block.payload),
            .doc_identity_batch => {
                try importIdentityBatch(alloc, store, block.payload);
                imported_identity = true;
            },
            .embedding_batch => try importEmbeddingBatch(alloc, store, block.payload),
            .edge_batch => try importEdgeBatch(alloc, store, block.payload),
            // Skip: sparse, summary, chunk, transaction (rebuilt by enrichment)
            .cluster_manifest, .table_manifest, .shard_header, .shard_footer, .file_footer => {},
            else => {},
        }
    }

    if (imported_identity) {
        try doc_identity.validateStoreAlloc(alloc, store);
        try validateImportedIdentityNamespace(store, opts);
    }
}

fn validateImportedIdentityNamespace(store: *DocStore, opts: ImportOptions) !void {
    const expected = opts.identity_namespace orelse return;
    if (opts.prefer_existing_identity_namespace) return;
    const stored = (try doc_identity.loadNamespaceFromStore(store)) orelse return error.IdentityNamespaceMismatch;
    if (!stored.eql(expected)) return error.IdentityNamespaceMismatch;
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

fn importIdentityBatch(alloc: Allocator, store: *DocStore, payload: []const u8) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer {
        for (entries) |e| {
            alloc.free(e.key);
            alloc.free(e.value);
        }
        alloc.free(entries);
    }

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);

    for (entries) |e| {
        if (e.key.len == 0 or e.key[0] != internal_keys.identity_namespace) {
            return error.InvalidDocIdentityBatch;
        }
        try writes.append(alloc, .{ .key = e.key, .value = e.value });
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
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }

    for (result.entries) |e| {
        const owned_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, e.source_key, result.index_name, e.edge_type, e.target_key);
        errdefer alloc.free(owned_key);
        const owned_value = try graphArtifactValueFromPortableEdgeValueAlloc(alloc, e.value);
        errdefer alloc.free(owned_value);
        try owned.append(alloc, owned_key);
        try owned.append(alloc, owned_value);
        try writes.append(alloc, .{ .key = owned_key, .value = owned_value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn graphArtifactValueFromPortableEdgeValueAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    if (enrichment_artifact_codec.decodeHeader(value)) |header| {
        if (header.kind == .graph_edge) return try alloc.dupe(u8, value);
    } else |_| {}

    if (value.len >= 24) {
        const weight = @as(f64, @bitCast(std.mem.readInt(u64, value[0..][0..8], .little)));
        const created_at = std.mem.readInt(u64, value[8..][0..8], .little);
        const updated_at = std.mem.readInt(u64, value[16..][0..8], .little);
        return try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, weight, created_at, updated_at, value[24..]);
    }

    return try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 1.0, 0, 0, value);
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
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    return DocStore.open(alloc, path_z, .{});
}

fn freeAllocatedKVPairs(alloc: Allocator, pairs: *std.ArrayListUnmanaged(KVPair)) void {
    for (pairs.items) |pair| {
        alloc.free(pair.key);
        alloc.free(pair.value);
    }
    pairs.deinit(alloc);
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

test "export and import preserves doc identity metadata" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    for ([_][]const u8{ "doc:a", "doc:b" }) |doc_id| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
        defer alloc.free(store_key);
        try src.putBatch(&.{.{ .key = store_key, .value = "{\"body\":\"identity\"}" }}, &.{});
    }

    var initial_identity = std.ArrayListUnmanaged(KVPair).empty;
    defer freeAllocatedKVPairs(alloc, &initial_identity);
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &src,
        7,
        9,
        10,
        &initial_identity,
        &.{ "doc:a", "doc:b" },
        &.{},
    );
    try src.putBatch(initial_identity.items, &.{});

    var tombstone_identity = std.ArrayListUnmanaged(KVPair).empty;
    defer freeAllocatedKVPairs(alloc, &tombstone_identity);
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &src,
        7,
        9,
        11,
        &tombstone_identity,
        &.{},
        &.{"doc:b"},
    );
    try src.putBatch(tombstone_identity.items, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    var txn = try dst.beginProbeTxn();
    defer txn.abort();

    const namespace = (try doc_identity.loadNamespaceTxn(&txn)) orelse return error.TestExpectedEqual;
    try std.testing.expect(namespace.eql(.{ .table_id = 7, .shard_id = 9 }));
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 1), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:a"));
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"));

    const doc_b = (try doc_identity.lookupDocIdTxn(alloc, &txn, 2)) orelse return error.TestExpectedEqual;
    defer alloc.free(doc_b);
    try std.testing.expectEqualStrings("doc:b", doc_b);

    const state_b = (try doc_identity.lookupStateTxn(&txn, 2)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 10), state_b.created_generation);
    try std.testing.expectEqual(@as(u64, 11), state_b.deleted_generation.?);
    try std.testing.expectEqual(@as(?doc_identity.DocOrdinal, 2), try doc_identity.lookupCanonicalOrdinalTxn(&txn, state_b.canonical_doc_id));

    const stats = try doc_identity.fullStatsFromStore(&dst);
    try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 3), stats.next_ordinal);
    try std.testing.expectEqual(@as(u64, 2), stats.allocated_ordinals);
    try std.testing.expectEqual(@as(u64, 1), stats.live_ordinals);
    try std.testing.expectEqual(@as(u64, 1), stats.tombstone_ordinals);
}

test "import rejects doc identity metadata with invalid canonical ids" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_id = "doc:corrupt";
    const store_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
    defer alloc.free(store_key);
    try src.putBatch(&.{.{ .key = store_key, .value = "{\"body\":\"identity\"}" }}, &.{});

    var identity_writes = std.ArrayListUnmanaged(KVPair).empty;
    defer freeAllocatedKVPairs(alloc, &identity_writes);
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        &src,
        7,
        9,
        10,
        &identity_writes,
        &.{doc_id},
        &.{},
    );
    try src.putBatch(identity_writes.items, &.{});

    const state_key = internal_keys.identityOrdinalStateKey(1);
    var corrupt_state: [25]u8 = undefined;
    std.mem.writeInt(u64, corrupt_state[0..8], 0xdead_beef, .big);
    std.mem.writeInt(u64, corrupt_state[8..16], 10, .big);
    corrupt_state[16] = 0;
    @memset(corrupt_state[17..25], 0);
    try src.putBatch(&.{.{ .key = state_key[0..], .value = corrupt_state[0..] }}, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try std.testing.expectError(error.InvalidDocIdentity, importPortable(alloc, &dst, out.items));
}

test "import rejects doc identity namespace mismatch unless preserving existing namespace" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const source_namespace = doc_identity.Namespace{ .table_id = 17, .shard_id = 1701, .range_id = 17001 };
    const target_namespace = doc_identity.Namespace{ .table_id = 17, .shard_id = 1702, .range_id = 17002 };
    const doc_id = "doc:portable-identity";
    const store_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
    defer alloc.free(store_key);
    try src.putBatch(&.{.{ .key = store_key, .value = "{\"body\":\"identity\"}" }}, &.{});

    var identity_writes = std.ArrayListUnmanaged(KVPair).empty;
    defer freeAllocatedKVPairs(alloc, &identity_writes);
    try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        &src,
        source_namespace,
        10,
        &identity_writes,
        &.{doc_id},
        &.{},
    );
    try src.putBatch(identity_writes.items, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    {
        var tmp_dst = std.testing.tmpDir(.{});
        defer tmp_dst.cleanup();
        var dst = try openTestStore(alloc, &tmp_dst);
        defer dst.close();
        try std.testing.expectError(error.IdentityNamespaceMismatch, importPortableWithOptions(alloc, &dst, out.items, .{
            .identity_namespace = target_namespace,
        }));
    }

    {
        var tmp_dst = std.testing.tmpDir(.{});
        defer tmp_dst.cleanup();
        var dst = try openTestStore(alloc, &tmp_dst);
        defer dst.close();
        try importPortableWithOptions(alloc, &dst, out.items, .{
            .identity_namespace = target_namespace,
            .prefer_existing_identity_namespace = true,
        });
        const restored_namespace = (try doc_identity.loadNamespaceFromStore(&dst)) orelse return error.TestExpectedEqual;
        try std.testing.expect(restored_namespace.eql(source_namespace));
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

test "export and import graph edge artifacts round trip with arbitrary ids" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    // Write source and target documents
    const source_doc = "alice\x00:i:\xff";
    const target_doc = "\x00bob:out:\xff";
    for ([_][]const u8{ source_doc, target_doc }) |dk| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, dk);
        defer alloc.free(store_key);
        try src.putBatch(&.{.{ .key = store_key, .value = "{}" }}, &.{});
    }

    const edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, source_doc, "social\x00idx", "follows:fast", target_doc);
    defer alloc.free(edge_key);
    const edge_val = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 2.5, 11, 22, "{\"ok\":true}");
    defer alloc.free(edge_val);
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

    // Verify edge restored under the structured graph artifact key, not a colon key.
    {
        const restored_edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, source_doc, "social\x00idx", "follows:fast", target_doc);
        defer alloc.free(restored_edge_key);
        const val = try dst.get(alloc, restored_edge_key);
        defer alloc.free(val);
        var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, val);
        defer decoded.deinit(alloc);
        try std.testing.expectApproxEqAbs(@as(f64, 2.5), decoded.weight, 0.001);
        try std.testing.expectEqual(@as(u64, 11), decoded.created_at);
        try std.testing.expectEqual(@as(u64, 22), decoded.updated_at);
        try std.testing.expectEqualStrings("{\"ok\":true}", decoded.metadata_json);
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
