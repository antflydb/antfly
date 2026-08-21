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
const backend_erased = @import("backend_erased.zig");
const internal_keys = @import("internal_keys.zig");
const docstore_mod = @import("docstore.zig");
const mem_backend = @import("mem_backend.zig");
const doc_identity = @import("db/doc_identity.zig");
const relational_store = @import("db/relational_store.zig");
const storage_schema = @import("schema.zig");
const public_table_schema = @import("../schema/mod.zig");
const db_types = @import("db/types.zig");
const artifact_ids = @import("db/artifact_ids.zig");
const enrichment_artifact_codec = @import("db/enrichment/artifact_codec.zig");
const DocStore = docstore_mod.DocStore;
const KeyEncoder = docstore_mod.KeyEncoder;
const KVPair = docstore_mod.KVPair;

/// Target batch size in bytes before flushing a document/embedding/edge batch.
const batch_target_bytes: usize = 4 * 1024 * 1024;
const resolution_public_id_prefix = "af1:resolution:";

const ResolutionArtifactRef = struct {
    doc_key: []u8,
    artifact_name: []u8,

    fn deinit(self: *ResolutionArtifactRef, alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.artifact_name);
        self.* = undefined;
    }
};

// ============================================================================
// Export
// ============================================================================

const PortableOutput = struct {
    writer: *std.Io.Writer,
    bytes_written: u64 = 0,

    fn writeHeader(self: *PortableOutput, header: backup_codec.FileHeader) !void {
        try backup_codec.writeHeaderTo(self.writer, header);
        self.bytes_written += backup_codec.header_size;
    }

    fn writeBlock(self: *PortableOutput, block_type: backup_codec.BlockType, payload: []const u8) !void {
        try backup_codec.writeBlockTo(self.writer, block_type, payload);
        self.bytes_written += backup_codec.block_envelope_overhead + payload.len;
    }
};

/// Export all portable data from the DocStore into AFB format.
/// The caller provides an allocator for temporary buffers. The output is
/// appended to `out`.
pub fn exportPortable(alloc: Allocator, store: *DocStore, out: *ArrayList(u8)) !void {
    var allocating = std.Io.Writer.Allocating.fromArrayList(alloc, out);
    defer out.* = allocating.toArrayList();
    try exportPortableToWriter(alloc, store, &allocating.writer);
}

pub fn exportPortableToWriter(alloc: Allocator, store: *DocStore, sink_writer: *std.Io.Writer) !void {
    var out: PortableOutput = .{ .writer = sink_writer };
    var scan = try store.beginReadTxn();
    defer scan.abort();

    // Write file header
    const backup_id = [_]u8{0} ** 16; // zero UUID for now
    try out.writeHeader(.{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0, // timestamp filled by caller if needed
        .backup_id = backup_id,
        .table_count = 1,
        .shard_count = 1,
    });

    // Cluster manifest
    try out.writeBlock(.cluster_manifest, "{}");

    // Table manifest
    try out.writeBlock(.table_manifest, "{}");

    // Shard header
    const shard_hdr = try backup_codec.encodeShardHeader(alloc, .{
        .table_name = "",
        .shard_id = 0,
        .start_key = "",
        .end_key = "",
    });
    defer alloc.free(shard_hdr);
    try out.writeBlock(.shard_header, shard_hdr);

    // Classify and batch all keys
    var doc_batch = std.ArrayListUnmanaged(backup_codec.DocumentEntry).empty;
    defer {
        for (doc_batch.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        doc_batch.deinit(alloc);
    }
    var doc_batch_bytes: usize = 0;

    var identity_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer deinitKeyValueBatch(alloc, &identity_batch);
    var identity_batch_bytes: usize = 0;

    var metadata_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer deinitKeyValueBatch(alloc, &metadata_batch);
    var metadata_batch_bytes: usize = 0;

    var chunk_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer deinitKeyValueBatch(alloc, &chunk_batch);
    var chunk_batch_bytes: usize = 0;

    var artifact_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer deinitKeyValueBatch(alloc, &artifact_batch);
    var artifact_batch_bytes: usize = 0;

    var resolution_batch = std.ArrayListUnmanaged(backup_codec.KeyValueEntry).empty;
    defer deinitKeyValueBatch(alloc, &resolution_batch);
    var resolution_batch_bytes: usize = 0;

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

    // Sparse embeddings keyed by index name
    var sparse_batches = std.StringHashMapUnmanaged(SparseBatch).empty;
    defer {
        var it = sparse_batches.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        sparse_batches.deinit(alloc);
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
    var derived_batch_bytes: usize = 0;

    var counts = Counts{};

    var cursor = try scan.openCursor();
    defer cursor.close();
    var scan_entry = try cursor.first();
    while (scan_entry) |kv| : (scan_entry = try cursor.next()) {
        if (isPortableMetadataKey(kv.key)) {
            try metadata_batch.append(alloc, .{
                .key = try alloc.dupe(u8, kv.key),
                .value = try alloc.dupe(u8, kv.value),
            });
            metadata_batch_bytes += kv.key.len + kv.value.len;
            if (metadata_batch_bytes >= batch_target_bytes) {
                try flushMetadataBatch(alloc, &out, &metadata_batch);
                metadata_batch_bytes = 0;
            }
            continue;
        }

        if (kv.key.len > 0 and kv.key[0] == internal_keys.identity_namespace) {
            try identity_batch.append(alloc, .{
                .key = try alloc.dupe(u8, kv.key),
                .value = try alloc.dupe(u8, kv.value),
            });
            identity_batch_bytes += kv.key.len + kv.value.len;
            if (identity_batch_bytes >= batch_target_bytes) {
                try flushIdentityBatch(alloc, &out, &identity_batch);
                identity_batch_bytes = 0;
            }
            continue;
        }

        // Binary internal keys (0x01 prefix)
        if (internal_keys.isInternalUserKey(kv.key)) {
            if (internal_keys.isStoredDocumentRowKey(kv.key)) {
                const relational_row = internal_keys.isRelationalRowKey(kv.key);
                if (relational_row) try relational_store.validateValue(kv.value);
                const user_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, kv.key)) orelse continue;
                defer alloc.free(user_key);
                const owned_value = try alloc.dupe(u8, kv.value);
                var owned_value_pending = true;
                errdefer if (owned_value_pending) alloc.free(owned_value);
                const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, user_key);
                defer alloc.free(timestamp_key);
                const timestamp_value = scan.get(timestamp_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };

                const owned_key = try alloc.dupe(u8, user_key);
                var owned_key_pending = true;
                errdefer if (owned_key_pending) alloc.free(owned_key);
                try doc_batch.append(alloc, .{
                    .key = owned_key,
                    .value_flags = if (relational_row) backup_codec.doc_value_flag_relational_row else 0,
                    .value = owned_value,
                    .timestamp_ns = if (timestamp_value) |value|
                        if (value.len >= 8) std.mem.readInt(u64, value[0..8], .little) else 0
                    else
                        0,
                });
                owned_key_pending = false;
                owned_value_pending = false;
                doc_batch_bytes += user_key.len + owned_value.len;

                if (doc_batch_bytes >= batch_target_bytes) {
                    try flushDocBatch(alloc, &out, &doc_batch, &counts);
                    doc_batch_bytes = 0;
                }
            } else if (internal_keys.isChunkArtifactRecordKey(kv.key)) {
                try appendChunkArtifactEntry(alloc, &chunk_batch, kv.key, kv.value);
                chunk_batch_bytes += kv.key.len + kv.value.len;
                if (chunk_batch_bytes >= batch_target_bytes) {
                    try flushChunkBatch(alloc, &out, &chunk_batch);
                    chunk_batch_bytes = 0;
                }
            } else if (internal_keys.isEmbeddingArtifactKey(kv.key)) {
                try collectEmbedding(alloc, &emb_batches, &sparse_batches, kv.key, kv.value);
                derived_batch_bytes += kv.key.len + kv.value.len;
            } else if (internal_keys.isGraphEdgeArtifactKey(kv.key)) {
                try collectGraphEdgeArtifact(alloc, &edge_batches, kv.key, kv.value);
                derived_batch_bytes += kv.key.len + kv.value.len;
            } else if (try parseStandaloneGraphIndexEdgeKeyAlloc(alloc, kv.key)) |parsed| {
                defer parsed.deinit(alloc);
                try appendEdgeBatchEntry(alloc, &edge_batches, parsed.index_name, parsed.source, parsed.target, parsed.edge_type, kv.value);
                derived_batch_bytes += kv.key.len + kv.value.len;
            } else if (try appendResolutionArtifactEntry(alloc, &resolution_batch, kv.key, kv.value)) {
                resolution_batch_bytes += kv.key.len + kv.value.len;
                if (resolution_batch_bytes >= batch_target_bytes) {
                    try flushKeyValueBlock(alloc, &out, &resolution_batch, .resolution_batch);
                    resolution_batch_bytes = 0;
                }
            } else if (try appendPortableArtifactEntry(alloc, &artifact_batch, kv.key, kv.value, .asset)) {
                artifact_batch_bytes += kv.key.len + kv.value.len;
                if (artifact_batch_bytes >= batch_target_bytes) {
                    try flushKeyValueBlock(alloc, &out, &artifact_batch, .artifact_batch);
                    artifact_batch_bytes = 0;
                }
            }
            if (derived_batch_bytes >= batch_target_bytes) {
                try flushDerivedBatches(alloc, &out, &emb_batches, &sparse_batches, &edge_batches, &counts);
                derived_batch_bytes = 0;
            }
            // Skip: TTL, summary, and derived embedding keys
            continue;
        }

        // Colon-delimited keys — check for outgoing edges
        if (KeyEncoder.isEdgeKey(kv.key)) {
            // Only export outgoing edges (ending with ":o")
            if (kv.key.len >= 2 and kv.key[kv.key.len - 1] == 'o' and kv.key[kv.key.len - 2] == ':') {
                const parsed = KeyEncoder.parseEdgeKey(kv.key) orelse continue;
                try appendEdgeBatchEntry(alloc, &edge_batches, parsed.index_name, parsed.source, parsed.target, parsed.edge_type, kv.value);
                derived_batch_bytes += kv.key.len + kv.value.len;
                if (derived_batch_bytes >= batch_target_bytes) {
                    try flushDerivedBatches(alloc, &out, &emb_batches, &sparse_batches, &edge_batches, &counts);
                    derived_batch_bytes = 0;
                }
            }
            // Skip incoming edges (":i" suffix)
        }
        // Skip any other colon-delimited keys (summaries, enrichments, etc.)
    }

    // Flush remaining documents
    if (doc_batch.items.len > 0) {
        try flushDocBatch(alloc, &out, &doc_batch, &counts);
    }
    if (identity_batch.items.len > 0) {
        try flushIdentityBatch(alloc, &out, &identity_batch);
    }
    if (metadata_batch.items.len > 0) {
        try flushMetadataBatch(alloc, &out, &metadata_batch);
    }
    if (chunk_batch.items.len > 0) {
        try flushChunkBatch(alloc, &out, &chunk_batch);
    }
    if (artifact_batch.items.len > 0) {
        try flushKeyValueBlock(alloc, &out, &artifact_batch, .artifact_batch);
    }
    if (resolution_batch.items.len > 0) {
        try flushKeyValueBlock(alloc, &out, &resolution_batch, .resolution_batch);
    }

    try flushDerivedBatches(alloc, &out, &emb_batches, &sparse_batches, &edge_batches, &counts);

    // Shard footer
    const shard_footer = backup_codec.encodeShardFooter(.{
        .shard_id = 0,
        .document_count = counts.documents,
        .embedding_count = counts.embeddings,
        .edge_count = counts.edges,
        .transaction_count = 0,
    });
    try out.writeBlock(.shard_footer, &shard_footer);

    // File footer
    const file_footer = backup_codec.encodeFileFooter(.{
        .table_count = 1,
        .shard_count = 1,
        .total_documents = counts.documents,
        .total_bytes = out.bytes_written,
    });
    try out.writeBlock(.file_footer, &file_footer);
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

const SparseBatch = struct {
    entries: std.ArrayListUnmanaged(backup_codec.SparseEntry),

    fn init() SparseBatch {
        return .{ .entries = .empty };
    }

    fn deinit(self: *SparseBatch, alloc: Allocator) void {
        for (self.entries.items) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.indices);
            alloc.free(e.values);
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

fn flushDerivedBatches(
    alloc: Allocator,
    out: *PortableOutput,
    dense: *std.StringHashMapUnmanaged(EmbeddingBatch),
    sparse: *std.StringHashMapUnmanaged(SparseBatch),
    edges: *std.StringHashMapUnmanaged(EdgeBatch),
    counts: *Counts,
) !void {
    var dense_it = dense.iterator();
    while (dense_it.next()) |entry| {
        const batch = entry.value_ptr;
        if (batch.entries.items.len == 0) continue;
        const encoded = try backup_codec.encodeEmbeddingBatch(alloc, entry.key_ptr.*, batch.dimension, batch.entries.items);
        defer alloc.free(encoded);
        try out.writeBlock(.embedding_batch, encoded);
        counts.embeddings += batch.entries.items.len;
        for (batch.entries.items) |item| {
            alloc.free(item.doc_key);
            alloc.free(item.vector);
        }
        batch.entries.clearRetainingCapacity();
    }

    var sparse_it = sparse.iterator();
    while (sparse_it.next()) |entry| {
        const batch = entry.value_ptr;
        if (batch.entries.items.len == 0) continue;
        const encoded = try backup_codec.encodeSparseBatch(alloc, entry.key_ptr.*, batch.entries.items);
        defer alloc.free(encoded);
        try out.writeBlock(.sparse_batch, encoded);
        counts.embeddings += batch.entries.items.len;
        for (batch.entries.items) |item| {
            alloc.free(item.doc_key);
            alloc.free(item.indices);
            alloc.free(item.values);
        }
        batch.entries.clearRetainingCapacity();
    }

    var edge_it = edges.iterator();
    while (edge_it.next()) |entry| {
        const batch = entry.value_ptr;
        if (batch.entries.items.len == 0) continue;
        const encoded = try backup_codec.encodeEdgeBatch(alloc, entry.key_ptr.*, batch.entries.items);
        defer alloc.free(encoded);
        try out.writeBlock(.edge_batch, encoded);
        counts.edges += batch.entries.items.len;
        for (batch.entries.items) |item| {
            alloc.free(item.source_key);
            alloc.free(item.target_key);
            alloc.free(item.edge_type);
            alloc.free(item.value);
        }
        batch.entries.clearRetainingCapacity();
    }
}

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
    out: *PortableOutput,
    batch: *std.ArrayListUnmanaged(backup_codec.DocumentEntry),
    counts: *Counts,
) !void {
    const encoded = try backup_codec.encodeDocumentBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try out.writeBlock(.document_batch, encoded);
    counts.documents += batch.items.len;

    // Free owned entry data
    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

fn timestampValueAlloc(alloc: Allocator, timestamp_ns: u64) ![]u8 {
    const value = try alloc.alloc(u8, 8);
    std.mem.writeInt(u64, value[0..8], timestamp_ns, .little);
    return value;
}

fn flushIdentityBatch(
    alloc: Allocator,
    out: *PortableOutput,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
) !void {
    const encoded = try backup_codec.encodeKeyValueBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try out.writeBlock(.doc_identity_batch, encoded);

    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

fn flushMetadataBatch(
    alloc: Allocator,
    out: *PortableOutput,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
) !void {
    const encoded = try backup_codec.encodeKeyValueBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try out.writeBlock(.metadata_batch, encoded);

    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

fn flushChunkBatch(
    alloc: Allocator,
    out: *PortableOutput,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
) !void {
    const encoded = try backup_codec.encodeKeyValueBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try out.writeBlock(.chunk_batch, encoded);

    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

fn flushKeyValueBlock(
    alloc: Allocator,
    out: *PortableOutput,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
    block_type: backup_codec.BlockType,
) !void {
    const encoded = try backup_codec.encodeKeyValueBatch(alloc, batch.items);
    defer alloc.free(encoded);
    try out.writeBlock(block_type, encoded);

    for (batch.items) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    batch.clearRetainingCapacity();
}

fn deinitKeyValueBatch(alloc: Allocator, batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry)) void {
    for (batch.items) |entry| {
        alloc.free(entry.key);
        alloc.free(entry.value);
    }
    batch.deinit(alloc);
}

fn isPortableMetadataKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "\x00\x00__metadata__:schema") or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:schema_v") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:schema_json") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:indexes") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:enrichments") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:resolvers");
}

fn appendChunkArtifactEntry(
    alloc: Allocator,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
    key: []const u8,
    value: []const u8,
) !void {
    var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, key)) orelse return;
    defer artifact_ref.deinit(alloc);
    if (artifact_ref.kind != .chunk) return;

    const public_id = try artifact_ids.artifactPublicIdAlloc(alloc, artifact_ref);
    errdefer alloc.free(public_id);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try batch.append(alloc, .{
        .key = public_id,
        .value = owned_value,
    });
}

fn appendPortableArtifactEntry(
    alloc: Allocator,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
    key: []const u8,
    value: []const u8,
    allowed_kind: db_types.ArtifactKind,
) !bool {
    var artifact_ref = (artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => return false,
        else => return err,
    }) orelse return false;
    defer artifact_ref.deinit(alloc);
    if (artifact_ref.kind != allowed_kind) return false;

    const public_id = try artifact_ids.artifactPublicIdAlloc(alloc, artifact_ref);
    errdefer alloc.free(public_id);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try batch.append(alloc, .{
        .key = public_id,
        .value = owned_value,
    });
    return true;
}

fn appendResolutionArtifactEntry(
    alloc: Allocator,
    batch: *std.ArrayListUnmanaged(backup_codec.KeyValueEntry),
    key: []const u8,
    value: []const u8,
) !bool {
    const parsed = (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, key)) orelse return false;
    defer {
        alloc.free(parsed.doc_key);
        alloc.free(parsed.artifact_name);
    }

    const public_id = try resolutionPublicIdAlloc(alloc, parsed.doc_key, parsed.artifact_name);
    errdefer alloc.free(public_id);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    try batch.append(alloc, .{
        .key = public_id,
        .value = owned_value,
    });
    return true;
}

fn resolutionPublicIdAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, resolution_public_id_prefix);
    try appendBase64UrlComponent(alloc, &out, doc_key);
    try out.append(alloc, ':');
    try appendBase64UrlComponent(alloc, &out, artifact_name);
    return try out.toOwnedSlice(alloc);
}

fn decodeResolutionPublicIdAlloc(alloc: Allocator, public_id: []const u8) !?ResolutionArtifactRef {
    if (!std.mem.startsWith(u8, public_id, resolution_public_id_prefix)) return null;

    const body = public_id[resolution_public_id_prefix.len..];
    const separator = std.mem.indexOfScalar(u8, body, ':') orelse return error.InvalidBackupRequest;
    if (std.mem.indexOfScalar(u8, body[separator + 1 ..], ':') != null) return error.InvalidBackupRequest;

    return .{
        .doc_key = try decodeBase64UrlComponentAlloc(alloc, body[0..separator]),
        .artifact_name = try decodeBase64UrlComponentAlloc(alloc, body[separator + 1 ..]),
    };
}

fn appendBase64UrlComponent(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const encoded_len = encoder.calcSize(bytes.len);
    const start = out.items.len;
    try out.resize(alloc, start + encoded_len);
    _ = encoder.encode(out.items[start .. start + encoded_len], bytes);
}

fn decodeBase64UrlComponentAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const decoded_len = decoder.calcSizeForSlice(encoded) catch return error.InvalidBackupRequest;
    const out = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(out);
    decoder.decode(out, encoded) catch return error.InvalidBackupRequest;
    return out;
}

/// Parse an embedding artifact value and collect into the appropriate batch.
fn collectEmbedding(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(EmbeddingBatch),
    sparse_batches: *std.StringHashMapUnmanaged(SparseBatch),
    key: []const u8,
    value: []const u8,
) !void {
    const parsed_key = (try internal_keys.parseEmbeddingArtifactKeyAlloc(alloc, key)) orelse return;
    defer alloc.free(parsed_key.doc_key);
    defer alloc.free(parsed_key.artifact_name);

    if (enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, value)) |vector| {
        try appendDenseEmbedding(alloc, batches, parsed_key.artifact_name, parsed_key.doc_key, vector);
        return;
    } else |_| {}

    if (enrichment_artifact_codec.decodeSparseEmbeddingAlloc(alloc, value)) |sparse| {
        try appendSparseEmbedding(alloc, sparse_batches, parsed_key.artifact_name, parsed_key.doc_key, sparse);
        return;
    } else |_| {}

    {
        // Legacy imported portable data used JSON: {"dims": N, "vector": [...]}.
        const EmbPayload = struct {
            dims: u32,
            vector: []f32,
        };
        const json_parsed = std.json.parseFromSlice(EmbPayload, alloc, value, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return; // skip malformed embeddings
        defer json_parsed.deinit();
        const vector = try alloc.dupe(f32, json_parsed.value.vector);
        try appendDenseEmbedding(alloc, batches, parsed_key.artifact_name, parsed_key.doc_key, vector);
    }
}

fn appendDenseEmbedding(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(EmbeddingBatch),
    index_name: []const u8,
    doc_key: []const u8,
    vector: []f32,
) !void {
    errdefer alloc.free(vector);

    const idx_name = try alloc.dupe(u8, index_name);
    const gop = try batches.getOrPut(alloc, idx_name);
    if (!gop.found_existing) {
        gop.value_ptr.* = EmbeddingBatch.init();
        gop.value_ptr.dimension = @intCast(vector.len);
    } else {
        alloc.free(idx_name);
    }

    const owned_doc_key = try alloc.dupe(u8, doc_key);
    errdefer alloc.free(owned_doc_key);
    try gop.value_ptr.entries.append(alloc, .{
        .doc_key = owned_doc_key,
        .hash_id = 0, // Zig doesn't store hash_id in embedding values
        .vector = vector,
    });
}

fn appendSparseEmbedding(
    alloc: Allocator,
    batches: *std.StringHashMapUnmanaged(SparseBatch),
    index_name: []const u8,
    doc_key: []const u8,
    sparse: enrichment_artifact_codec.SparseEmbedding,
) !void {
    var owned_sparse = sparse;
    errdefer owned_sparse.deinit(alloc);

    const idx_name = try alloc.dupe(u8, index_name);
    const gop = try batches.getOrPut(alloc, idx_name);
    if (!gop.found_existing) {
        gop.value_ptr.* = SparseBatch.init();
    } else {
        alloc.free(idx_name);
    }

    const owned_doc_key = try alloc.dupe(u8, doc_key);
    errdefer alloc.free(owned_doc_key);
    try gop.value_ptr.entries.append(alloc, .{
        .doc_key = owned_doc_key,
        .hash_id = 0,
        .indices = owned_sparse.indices,
        .values = owned_sparse.values,
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
    pub const EmbeddingSourceField = struct {
        index_name: []const u8,
        field_name: []const u8,
    };

    identity_namespace: ?doc_identity.Namespace = null,
    prefer_existing_identity_namespace: bool = false,
    import_derived_indexes: bool = true,
    embedding_source_fields: []const EmbeddingSourceField = &.{},
};

/// Import AFB data into the DocStore.
pub fn importPortable(alloc: Allocator, store: *DocStore, data: []const u8) !void {
    return try importPortableWithOptions(alloc, store, data, .{});
}

pub fn validatePortable(alloc: Allocator, data: []const u8) !void {
    try validatePortableImportBlocks(alloc, data, .{});
}

/// Fully validated portable import held outside the live target generation.
/// Publishing streams the staged keys through one target write transaction,
/// so memory/backend failures abort without exposing a prefix of the archive.
pub const PortableImportStage = struct {
    alloc: Allocator,
    backend: *mem_backend.Backend,
    runtime_store: backend_erased.Store,
    store: DocStore,

    pub fn init(alloc: Allocator, data: []const u8, opts: ImportOptions) !PortableImportStage {
        const backend = try alloc.create(mem_backend.Backend);
        errdefer alloc.destroy(backend);
        backend.* = mem_backend.Backend.init(alloc, .{});
        errdefer backend.close();
        var runtime_store = try backend.runtimeStore(alloc, .{ .name = "portable-restore-stage" });
        errdefer runtime_store.deinit();
        var store = try DocStore.openRuntime(alloc, &runtime_store);
        errdefer store.close();
        try importPortableWithOptions(alloc, &store, data, opts);
        return .{
            .alloc = alloc,
            .backend = backend,
            .runtime_store = runtime_store,
            .store = store,
        };
    }

    pub fn deinit(self: *PortableImportStage) void {
        self.store.close();
        self.runtime_store.deinit();
        self.backend.close();
        self.alloc.destroy(self.backend);
        self.* = undefined;
    }

    pub fn reassignIdentityNamespace(self: *PortableImportStage, alloc: Allocator, namespace: doc_identity.Namespace) !void {
        try doc_identity.reassignNamespaceAlloc(alloc, &self.store, namespace);
    }

    pub fn publish(self: *PortableImportStage, target: *DocStore) !void {
        var write_txn = try target.beginWriteTxn();
        errdefer write_txn.abort();
        const Publish = struct {
            txn: *DocStore.Txn,

            fn copy(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!DocStore.ScanAction {
                const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                try state.txn.put(key, value);
                return .@"continue";
            }
        };
        var state = Publish{ .txn = &write_txn };
        try self.store.scanWithContext("", "", .{}, &state, Publish.copy);
        try write_txn.commit();
    }
};

pub fn importPortableWithOptions(alloc: Allocator, store: *DocStore, data: []const u8, opts: ImportOptions) !void {
    var validation_reader = backup_codec.SliceReader.init(data);
    try validatePortableImportReader(alloc, &validation_reader, opts);

    var reader = backup_codec.SliceReader.init(data);
    const imported_identity = try importPortablePrimaryBlocks(alloc, store, &reader);

    try finishPortableIdentityImport(alloc, store, opts, imported_identity);
    if (opts.import_derived_indexes) {
        var derived_reader = backup_codec.SliceReader.init(data);
        try importPortableDerivedBlocks(alloc, store, &derived_reader, opts);
    }
}

/// Imports a portable archive without materializing the file. Validation and
/// the two dependency-ordered import passes use positional reads over the same
/// borrowed descriptor, keeping peak archive memory bounded to one block.
pub fn importPortableFileWithOptions(
    alloc: Allocator,
    store: *DocStore,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
    opts: ImportOptions,
) !void {
    // The three passes must observe one immutable archive generation. Locking
    // is fail-closed; the final stat also detects writers that ignore advisory
    // locks on a shared filesystem.
    try file.lock(io, .shared);
    defer file.unlock(io);
    const initial_stat = try file.stat(io);
    if (initial_stat.size != file_size) return error.SourceFileChanged;

    var validation_reader = backup_codec.FileReader.init(io, file, file_size);
    try validatePortableImportReader(alloc, &validation_reader, opts);

    var reader = backup_codec.FileReader.init(io, file, file_size);
    const imported_identity = try importPortablePrimaryBlocks(alloc, store, &reader);
    try finishPortableIdentityImport(alloc, store, opts, imported_identity);
    if (opts.import_derived_indexes) {
        var derived_reader = backup_codec.FileReader.init(io, file, file_size);
        try importPortableDerivedBlocks(alloc, store, &derived_reader, opts);
    }
    const final_stat = try file.stat(io);
    if (final_stat.size != initial_stat.size or !std.meta.eql(final_stat.mtime, initial_stat.mtime))
        return error.SourceFileChanged;
}

pub fn importPortableFile(alloc: Allocator, store: *DocStore, io: std.Io, file: std.Io.File, file_size: u64) !void {
    return try importPortableFileWithOptions(alloc, store, io, file, file_size, .{});
}

fn importPortablePrimaryBlocks(alloc: Allocator, store: *DocStore, reader: anytype) !bool {
    _ = try reader.readHeader();
    var imported_identity = false;

    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);

        switch (block.block_type) {
            .document_batch => try importDocumentBatch(alloc, store, block.payload),
            .doc_identity_batch => {
                try importIdentityBatch(alloc, store, block.payload);
                imported_identity = true;
            },
            .metadata_batch => try importMetadataBatch(alloc, store, block.payload),
            // Skip: derived indexes in the first pass; they are restored after documents.
            .cluster_manifest, .table_manifest, .shard_header, .shard_footer, .file_footer => {},
            else => {},
        }
    }
    return imported_identity;
}

fn finishPortableIdentityImport(alloc: Allocator, store: *DocStore, opts: ImportOptions, imported_identity: bool) !void {
    if (imported_identity) {
        try doc_identity.validateStoreAlloc(alloc, store);
        try validateImportedIdentityNamespace(store, opts);
    }
}

fn importPortableDerivedBlocks(alloc: Allocator, store: *DocStore, reader: anytype, opts: ImportOptions) !void {
    _ = try reader.readHeader();
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);

        switch (block.block_type) {
            .chunk_batch => try importChunkBatch(alloc, store, block.payload),
            .artifact_batch => try importPublicArtifactBatch(alloc, store, block.payload, .asset),
            .resolution_batch => try importResolutionArtifactBatch(alloc, store, block.payload),
            .embedding_batch => try importEmbeddingBatch(alloc, store, block.payload, opts.embedding_source_fields),
            .sparse_batch => try importSparseBatch(alloc, store, block.payload, opts.embedding_source_fields),
            .edge_batch => try importEdgeBatch(alloc, store, block.payload),
            else => {},
        }
    }
}

fn validatePortableImportBlocks(alloc: Allocator, data: []const u8, opts: ImportOptions) !void {
    var reader = backup_codec.SliceReader.init(data);
    return try validatePortableImportReader(alloc, &reader, opts);
}

fn validatePortableImportReader(alloc: Allocator, reader: anytype, opts: ImportOptions) !void {
    const header = try reader.readHeader();
    var archive: PortableArchiveValidation = .{ .format_version = header.format_version };
    defer archive.deinit(alloc);
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        try validatePortableImportBlockPayload(alloc, block.block_type, block.payload, opts, &archive);
    }
    try archive.finish(alloc);

    // Metadata can legally follow document batches. Rewind for a bounded-memory
    // second pass now that the authoritative schema is known, rather than
    // retaining every row from a large archive.
    reader.reset();
    _ = try reader.readHeader();
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type == .document_batch) {
            try validateDocumentBatchAgainstArchiveSchema(alloc, block.payload, archive);
        }
    }
}

const PortableArchiveValidation = struct {
    format_version: u32,
    saw_document_rows: bool = false,
    saw_relational_rows: bool = false,
    runtime_schema: ?storage_schema.TableSchema = null,
    public_schema: ?public_table_schema.ParsedTableSchema = null,

    fn deinit(self: *PortableArchiveValidation, alloc: Allocator) void {
        if (self.runtime_schema) |schema| storage_schema.freeSchema(alloc, schema);
        if (self.public_schema) |*schema| schema.deinit(alloc);
        self.* = undefined;
    }

    fn finish(self: *const PortableArchiveValidation, alloc: Allocator) !void {
        if (self.saw_document_rows and self.saw_relational_rows) return error.InvalidBackupRequest;
        if (self.saw_relational_rows and self.format_version < 2) return error.InvalidBackupRequest;

        const runtime_relational = if (self.runtime_schema) |schema| schema.storage_mode == .relational else false;
        const public_relational = if (self.public_schema) |schema| schema.storage_mode == .relational else false;
        if (self.public_schema != null and self.runtime_schema != null and runtime_relational != public_relational)
            return error.InvalidBackupRequest;
        if (public_relational and self.runtime_schema == null) return error.InvalidBackupRequest;
        if (self.public_schema) |public_schema| {
            if (self.runtime_schema) |runtime_schema| {
                const derived = public_table_schema.deriveRuntimeTableSchema(alloc, public_schema) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidBackupRequest,
                };
                defer storage_schema.freeSchema(alloc, derived);
                const schemas_match = storage_schema.schemasEqual(alloc, runtime_schema, derived) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidBackupRequest,
                };
                if (!schemas_match) return error.InvalidBackupRequest;
            }
        }
        if (self.saw_relational_rows and !runtime_relational) return error.InvalidBackupRequest;
        if (self.saw_document_rows and runtime_relational) return error.InvalidBackupRequest;
    }
};

fn validatePortableImportBlockPayload(
    alloc: Allocator,
    block_type: backup_codec.BlockType,
    payload: []const u8,
    opts: ImportOptions,
    archive: *PortableArchiveValidation,
) !void {
    switch (block_type) {
        .document_batch => try validateDocumentBatchPayload(alloc, payload, archive),
        .doc_identity_batch => try validateIdentityBatchPayload(alloc, payload),
        .metadata_batch => try validateMetadataBatchPayload(alloc, payload, archive),
        .chunk_batch => if (opts.import_derived_indexes) try validatePublicArtifactBatchPayload(alloc, payload, .chunk),
        .artifact_batch => if (opts.import_derived_indexes) try validatePublicArtifactBatchPayload(alloc, payload, .asset),
        .resolution_batch => if (opts.import_derived_indexes) try validateResolutionArtifactBatchPayload(alloc, payload),
        .embedding_batch => if (opts.import_derived_indexes) try validateEmbeddingBatchPayload(alloc, payload),
        .sparse_batch => if (opts.import_derived_indexes) try validateSparseBatchPayload(alloc, payload),
        .edge_batch => if (opts.import_derived_indexes) try validateEdgeBatchPayload(alloc, payload),
        .shard_header => {
            const header = try backup_codec.decodeShardHeader(alloc, payload);
            alloc.free(header.table_name);
            alloc.free(header.start_key);
            alloc.free(header.end_key);
        },
        .shard_footer => _ = try backup_codec.decodeShardFooter(payload),
        .file_footer => _ = try backup_codec.decodeFileFooter(payload),
        .cluster_manifest, .table_manifest => {},
        .summary_batch, .transaction_batch => {},
        else => {},
    }
}

fn validateDocumentBatchPayload(alloc: Allocator, payload: []const u8, archive: *PortableArchiveValidation) !void {
    const entries = try backup_codec.decodeDocumentBatch(alloc, payload);
    defer {
        for (entries) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        alloc.free(entries);
    }
    for (entries) |entry| {
        try validatePortableDocumentEntry(entry);
        if (entry.value_flags & backup_codec.doc_value_flag_relational_row != 0)
            archive.saw_relational_rows = true
        else
            archive.saw_document_rows = true;
    }
}

fn validatePortableDocumentEntry(entry: backup_codec.DocumentEntry) !void {
    if (entry.value_flags & ~backup_codec.doc_value_known_flags != 0) return error.InvalidBackupRequest;
    if (entry.value_flags & backup_codec.doc_value_flag_relational_row == 0) return;
    if (entry.value_flags & backup_codec.doc_value_flag_compressed != 0) return error.InvalidBackupRequest;
    relational_store.validateValue(entry.value) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
}

fn validateIdentityBatchPayload(alloc: Allocator, payload: []const u8) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);
    for (entries) |entry| {
        if (entry.key.len == 0 or entry.key[0] != internal_keys.identity_namespace) {
            return error.InvalidDocIdentityBatch;
        }
    }
}

fn validateMetadataBatchPayload(alloc: Allocator, payload: []const u8, archive: *PortableArchiveValidation) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);
    for (entries) |entry| {
        if (!isPortableMetadataKey(entry.key)) return error.InvalidMetadataBatch;
        if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema")) {
            if (archive.runtime_schema != null) return error.InvalidMetadataBatch;
            archive.runtime_schema = storage_schema.deserializeSchema(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
        } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema_json")) {
            if (archive.public_schema != null) return error.InvalidMetadataBatch;
            archive.public_schema = public_table_schema.parseValidatedTableSchema(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
        }
    }
}

fn validateDocumentBatchAgainstArchiveSchema(
    alloc: Allocator,
    payload: []const u8,
    archive: PortableArchiveValidation,
) !void {
    if (!archive.saw_relational_rows) return;
    const runtime_schema = archive.runtime_schema orelse return error.InvalidBackupRequest;
    const entries = try backup_codec.decodeDocumentBatch(alloc, payload);
    defer {
        for (entries) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        alloc.free(entries);
    }
    for (entries) |entry| {
        if (entry.value_flags & backup_codec.doc_value_flag_relational_row == 0) continue;
        const logical = relational_store.decodeValueAlloc(alloc, entry.value) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBackupRequest,
        };
        defer alloc.free(logical);
        if (archive.public_schema) |schema| {
            public_table_schema.validateWritesAgainstTableSchema(alloc, schema, &.{db_types.BatchWrite{
                .key = entry.key,
                .value = logical,
            }}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
        }
        const canonical = relational_store.encodeValueAlloc(alloc, logical, runtime_schema.relational_columns) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBackupRequest,
        };
        alloc.free(canonical);
    }
}

fn validatePublicArtifactBatchPayload(alloc: Allocator, payload: []const u8, allowed_kind: db_types.ArtifactKind) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);
    for (entries) |entry| {
        var artifact_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, entry.key)) orelse return error.InvalidBackupRequest;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != allowed_kind) return error.InvalidBackupRequest;
    }
}

fn validateResolutionArtifactBatchPayload(alloc: Allocator, payload: []const u8) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);
    for (entries) |entry| {
        var artifact_ref = (try decodeResolutionPublicIdAlloc(alloc, entry.key)) orelse return error.InvalidBackupRequest;
        defer artifact_ref.deinit(alloc);
    }
}

fn validateEmbeddingBatchPayload(alloc: Allocator, payload: []const u8) !void {
    const result = try backup_codec.decodeEmbeddingBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |entry| {
            alloc.free(entry.doc_key);
            alloc.free(entry.vector);
        }
        alloc.free(result.entries);
    }
}

fn validateSparseBatchPayload(alloc: Allocator, payload: []const u8) !void {
    const result = try backup_codec.decodeSparseBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |entry| {
            alloc.free(entry.doc_key);
            alloc.free(entry.indices);
            alloc.free(entry.values);
        }
        alloc.free(result.entries);
    }
}

fn validateEdgeBatchPayload(alloc: Allocator, payload: []const u8) !void {
    const result = try decodeEdgeBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |entry| {
            alloc.free(entry.source_key);
            alloc.free(entry.target_key);
            alloc.free(entry.edge_type);
            alloc.free(entry.value);
        }
        alloc.free(result.entries);
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
        try validatePortableDocumentEntry(e);
        const relational_row = e.value_flags & backup_codec.doc_value_flag_relational_row != 0;
        const store_key = if (relational_row)
            try internal_keys.relationalRowKeyAlloc(alloc, e.key)
        else
            try internal_keys.documentKeyAlloc(alloc, e.key);
        try owned_keys.append(alloc, store_key);
        const value = if (!relational_row and e.value_flags & backup_codec.doc_value_flag_compressed != 0)
            try backup_codec.decompressZstd(alloc, e.value)
        else
            try alloc.dupe(u8, e.value);
        try owned_keys.append(alloc, value);
        try writes.append(alloc, .{ .key = store_key, .value = value });
        if (e.timestamp_ns != 0) {
            const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, e.key);
            try owned_keys.append(alloc, timestamp_key);
            const timestamp_value = try timestampValueAlloc(alloc, e.timestamp_ns);
            try owned_keys.append(alloc, timestamp_value);
            try writes.append(alloc, .{ .key = timestamp_key, .value = timestamp_value });
        }
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

fn importMetadataBatch(alloc: Allocator, store: *DocStore, payload: []const u8) !void {
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
        if (!isPortableMetadataKey(e.key)) return error.InvalidMetadataBatch;
        try writes.append(alloc, .{ .key = e.key, .value = e.value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importChunkBatch(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);

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

    for (entries) |entry| {
        var artifact_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, entry.key)) orelse return error.InvalidBackupRequest;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != .chunk) return error.InvalidBackupRequest;

        const store_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, artifact_ref);
        var store_key_owned = true;
        errdefer if (store_key_owned) alloc.free(store_key);
        try owned_keys.append(alloc, store_key);
        store_key_owned = false;

        const store_value = try alloc.dupe(u8, entry.value);
        var store_value_owned = true;
        errdefer if (store_value_owned) alloc.free(store_value);
        try owned_vals.append(alloc, store_value);
        store_value_owned = false;

        try writes.append(alloc, .{
            .key = store_key,
            .value = store_value,
        });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importPublicArtifactBatch(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
    allowed_kind: db_types.ArtifactKind,
) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);

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

    for (entries) |entry| {
        var artifact_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, entry.key)) orelse return error.InvalidBackupRequest;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != allowed_kind) return error.InvalidBackupRequest;

        const store_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, artifact_ref);
        var store_key_owned = true;
        errdefer if (store_key_owned) alloc.free(store_key);
        try owned_keys.append(alloc, store_key);
        store_key_owned = false;

        const store_value = try alloc.dupe(u8, entry.value);
        var store_value_owned = true;
        errdefer if (store_value_owned) alloc.free(store_value);
        try owned_vals.append(alloc, store_value);
        store_value_owned = false;

        try writes.append(alloc, .{
            .key = store_key,
            .value = store_value,
        });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importResolutionArtifactBatch(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);

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

    for (entries) |entry| {
        var artifact_ref = (try decodeResolutionPublicIdAlloc(alloc, entry.key)) orelse return error.InvalidBackupRequest;
        defer artifact_ref.deinit(alloc);

        const store_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, artifact_ref.doc_key, artifact_ref.artifact_name);
        var store_key_owned = true;
        errdefer if (store_key_owned) alloc.free(store_key);
        try owned_keys.append(alloc, store_key);
        store_key_owned = false;

        const store_value = try alloc.dupe(u8, entry.value);
        var store_value_owned = true;
        errdefer if (store_value_owned) alloc.free(store_value);
        try owned_vals.append(alloc, store_value);
        store_value_owned = false;

        try writes.append(alloc, .{
            .key = store_key,
            .value = store_value,
        });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn freeKeyValueEntries(alloc: Allocator, entries: []backup_codec.KeyValueEntry) void {
    for (entries) |entry| {
        alloc.free(entry.key);
        alloc.free(entry.value);
    }
    alloc.free(entries);
}

fn importEmbeddingBatch(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
    source_fields: []const ImportOptions.EmbeddingSourceField,
) !void {
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

        const source_hash = try embeddingSourceHashForDocument(alloc, store, e.doc_key, result.index_name, source_fields);
        const artifact_value = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, source_hash, e.vector);
        try owned_vals.append(alloc, artifact_value);
        try writes.append(alloc, .{ .key = store_key, .value = artifact_value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn importSparseBatch(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
    source_fields: []const ImportOptions.EmbeddingSourceField,
) !void {
    const result = try backup_codec.decodeSparseBatch(alloc, payload);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.indices);
            alloc.free(e.values);
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

        const source_hash = try embeddingSourceHashForDocument(alloc, store, e.doc_key, result.index_name, source_fields);
        const artifact_value = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, source_hash, e.indices, e.values);
        try owned_vals.append(alloc, artifact_value);
        try writes.append(alloc, .{ .key = store_key, .value = artifact_value });
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
    }
}

fn embeddingSourceHashForDocument(
    alloc: Allocator,
    store: *DocStore,
    doc_key: []const u8,
    index_name: []const u8,
    source_fields: []const ImportOptions.EmbeddingSourceField,
) !?u64 {
    var field_name: ?[]const u8 = null;
    for (source_fields) |field| {
        if (std.mem.eql(u8, field.index_name, index_name)) {
            field_name = field.field_name;
            break;
        }
    }
    const field = field_name orelse return null;

    const store_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
    defer alloc.free(store_key);
    const raw_doc = store.get(alloc, store_key) catch |err| switch (err) {
        error.KeyNotFound => return null,
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw_doc);

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw_doc, .{}) catch return null;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return null,
    };
    const source_value = root.get(field) orelse return null;
    const source_text = switch (source_value) {
        .string => |text| text,
        else => return null,
    };
    return enrichment_artifact_codec.hashSource(source_text);
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
    const path_z = try alloc.dupeZ(u8, path);
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

test "portable backup round trips relational rows and schema metadata" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
        .{ .name = "count", .path = "count", .column_type = .integer, .required = true },
    };
    const packed_row = try relational_store.encodeValueAlloc(alloc, "{\"id\":\"a\",\"count\":9007199254740993}", &columns);
    defer alloc.free(packed_row);
    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
    defer alloc.free(row_key);
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"}},"required":["id","count"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const derived_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, derived_schema);
    const encoded_schema = try storage_schema.serializeSchema(alloc, derived_schema);
    defer alloc.free(encoded_schema);
    const timestamp_key = try internal_keys.ttlKeyAlloc(alloc, "row:a");
    defer alloc.free(timestamp_key);
    var timestamp_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &timestamp_value, 1234, .little);
    try src.putBatch(&.{
        .{ .key = row_key, .value = packed_row },
        .{ .key = timestamp_key, .value = &timestamp_value },
        .{ .key = "\x00\x00__metadata__:schema", .value = encoded_schema },
        .{ .key = "\x00\x00__metadata__:schema_json", .value = schema_json },
    }, &.{});

    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try exportPortable(alloc, &src, &portable);
    try validatePortable(alloc, portable.items);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, portable.items);

    const restored_row = try dst.get(alloc, row_key);
    defer alloc.free(restored_row);
    try std.testing.expectEqualSlices(u8, packed_row, restored_row);
    try relational_store.validateValue(restored_row);
    const restored_json = try relational_store.decodeValueAlloc(alloc, restored_row);
    defer alloc.free(restored_json);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"count\":9007199254740993}", restored_json);
    const legacy_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(legacy_key);
    try std.testing.expectError(error.NotFound, dst.get(alloc, legacy_key));

    const restored_schema = try dst.get(alloc, "\x00\x00__metadata__:schema_json");
    defer alloc.free(restored_schema);
    try std.testing.expectEqualStrings(schema_json, restored_schema);
    const restored_timestamp = try dst.get(alloc, timestamp_key);
    defer alloc.free(restored_timestamp);
    try std.testing.expectEqual(@as(u64, 1234), std.mem.readInt(u64, restored_timestamp[0..8], .little));
}

test "portable backup rejects ambiguous or corrupt relational document entries" {
    const alloc = std.testing.allocator;
    const columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
    };
    const packed_row = try relational_store.encodeValueAlloc(alloc, "{\"id\":\"a\"}", &columns);
    defer alloc.free(packed_row);

    try std.testing.expectError(error.InvalidBackupRequest, validatePortableDocumentEntry(.{
        .key = "row:a",
        .value_flags = 0x80,
        .value = packed_row,
        .timestamp_ns = 0,
    }));
    try std.testing.expectError(error.InvalidBackupRequest, validatePortableDocumentEntry(.{
        .key = "row:a",
        .value_flags = backup_codec.doc_value_flag_compressed | backup_codec.doc_value_flag_relational_row,
        .value = packed_row,
        .timestamp_ns = 0,
    }));
    try std.testing.expectError(error.InvalidBackupRequest, validatePortableDocumentEntry(.{
        .key = "row:a",
        .value_flags = backup_codec.doc_value_flag_relational_row,
        .value = "AROW-corrupt",
        .timestamp_ns = 0,
    }));
}

test "portable backup binds relational rows to archived runtime and public schemas" {
    const alloc = std.testing.allocator;
    const columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
        .{ .name = "status", .path = "status", .column_type = .string, .required = true },
    };
    const public_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","enum":["active"]}},"required":["id","status"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, public_schema);
    defer parsed_schema.deinit(alloc);
    const derived_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, derived_schema);
    const runtime_schema = try storage_schema.serializeSchema(alloc, derived_schema);
    defer alloc.free(runtime_schema);
    const metadata_payload = try backup_codec.encodeKeyValueBatch(alloc, &.{
        .{ .key = "\x00\x00__metadata__:schema", .value = runtime_schema },
        .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
    });
    defer alloc.free(metadata_payload);

    const invalid_row = try relational_store.encodeValueAlloc(alloc, "{\"id\":\"a\",\"status\":\"inactive\"}", &columns);
    defer alloc.free(invalid_row);
    const invalid_row_payload = try backup_codec.encodeDocumentBatch(alloc, &.{.{
        .key = "row:a",
        .value_flags = backup_codec.doc_value_flag_relational_row,
        .value = invalid_row,
        .timestamp_ns = 0,
    }});
    defer alloc.free(invalid_row_payload);

    var archive = std.ArrayList(u8).empty;
    defer archive.deinit(alloc);
    try backup_codec.writeHeader(&archive, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    // Exercise metadata-after-documents ordering as emitted by small exports.
    try backup_codec.writeBlock(&archive, alloc, .document_batch, invalid_row_payload);
    try backup_codec.writeBlock(&archive, alloc, .metadata_batch, metadata_payload);
    try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));

    archive.clearRetainingCapacity();
    try backup_codec.writeHeader(&archive, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    const document_payload = try backup_codec.encodeDocumentBatch(alloc, &.{.{
        .key = "row:a",
        .value_flags = 0,
        .value = "{\"id\":\"a\",\"status\":\"active\"}",
        .timestamp_ns = 0,
    }});
    defer alloc.free(document_payload);
    try backup_codec.writeBlock(&archive, alloc, .metadata_batch, metadata_payload);
    try backup_codec.writeBlock(&archive, alloc, .document_batch, document_payload);
    try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));

    var mismatched_schema = derived_schema;
    mismatched_schema.version += 1;
    const mismatched_runtime = try storage_schema.serializeSchema(alloc, mismatched_schema);
    defer alloc.free(mismatched_runtime);
    const mismatched_metadata = try backup_codec.encodeKeyValueBatch(alloc, &.{
        .{ .key = "\x00\x00__metadata__:schema", .value = mismatched_runtime },
        .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
    });
    defer alloc.free(mismatched_metadata);
    archive.clearRetainingCapacity();
    try backup_codec.writeHeader(&archive, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    try backup_codec.writeBlock(&archive, alloc, .metadata_batch, mismatched_metadata);
    try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));
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

    // The server restore path reads the same format positionally from disk and
    // must produce an identical store without allocating the whole archive.
    const archive_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/roundtrip.afb", .{tmp_src.sub_path});
    defer alloc.free(archive_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var archive = try std.Io.Dir.cwd().createFile(io, archive_path, .{ .truncate = true });
    var writer_buffer: [4096]u8 = undefined;
    var archive_writer = archive.writer(io, &writer_buffer);
    try exportPortableToWriter(alloc, &src, &archive_writer.interface);
    try archive_writer.end();
    try archive.sync(io);
    const archive_size = (try archive.stat(io)).size;
    archive.close(io);
    archive = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer archive.close(io);

    var tmp_file_dst = std.testing.tmpDir(.{});
    defer tmp_file_dst.cleanup();
    var file_dst = try openTestStore(alloc, &tmp_file_dst);
    defer file_dst.close();
    try importPortableFile(alloc, &file_dst, io, archive, archive_size);
    for (doc_keys, doc_vals) |dk, expected| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, dk);
        defer alloc.free(store_key);
        const val = try file_dst.get(alloc, store_key);
        defer alloc.free(val);
        try std.testing.expectEqualStrings(expected, val);
    }
}

test "file import restores Go cross-backend portable fixture" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("testdata/cross_backend_v1.afb");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const archive_path = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/go-cross-backend.afb",
        .{tmp.sub_path},
    );
    defer alloc.free(archive_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = archive_path,
        .data = fixture,
    });
    var archive = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer archive.close(io);

    var store = try openTestStore(alloc, &tmp);
    defer store.close();
    try importPortableFileWithOptions(
        alloc,
        &store,
        io,
        archive,
        fixture.len,
        .{ .import_derived_indexes = true },
    );

    const expected_docs = [_]struct {
        key: []const u8,
        title: []const u8,
        born: []const u8,
    }{
        .{ .key = "albert-einstein", .title = "Albert Einstein", .born = "1879" },
        .{ .key = "alan-turing", .title = "Alan Turing", .born = "1912" },
        .{ .key = "ada-lovelace", .title = "Ada Lovelace", .born = "1815" },
        .{ .key = "marie-curie", .title = "Marie Curie", .born = "1867" },
        .{ .key = "nikola-tesla", .title = "Nikola Tesla", .born = "1856" },
    };
    for (expected_docs) |expected| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, expected.key);
        defer alloc.free(store_key);
        const value = try store.get(alloc, store_key);
        defer alloc.free(value);
        try std.testing.expect(std.mem.indexOf(u8, value, expected.title) != null);
        try std.testing.expect(std.mem.indexOf(u8, value, expected.born) != null);
    }
}

test "file import restores production Go portable fixture" {
    const alloc = std.testing.allocator;
    // This golden is generated by DBImpl.exportPortableWithOptions in the Go
    // production package. Its document block is compressed, so this exercises
    // the real producer path and Zig's compressed streaming importer together.
    const fixture = @embedFile("testdata/production_portable_v1.afb");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const archive_path = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/go-production-portable.afb",
        .{tmp.sub_path},
    );
    defer alloc.free(archive_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = archive_path,
        .data = fixture,
    });
    var archive = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer archive.close(io);

    var store = try openTestStore(alloc, &tmp);
    defer store.close();
    try importPortableFile(alloc, &store, io, archive, fixture.len);

    const expected_docs = [_]struct {
        key: []const u8,
        title: []const u8,
    }{
        .{ .key = "prod-alpha", .title = "Alpha" },
        .{ .key = "prod-beta", .title = "Beta" },
        .{ .key = "prod-gamma", .title = "Gamma" },
    };
    for (expected_docs) |expected| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, expected.key);
        defer alloc.free(store_key);
        const value = try store.get(alloc, store_key);
        defer alloc.free(value);
        try std.testing.expect(std.mem.indexOf(u8, value, expected.title) != null);
    }
}

test "file import rejects oversized portable blocks before allocation" {
    const alloc = std.testing.allocator;
    var encoded: ArrayList(u8) = .empty;
    defer encoded.deinit(alloc);
    try backup_codec.writeHeader(&encoded, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = @splat(0),
        .table_count = 1,
        .shard_count = 1,
    });
    var env: [6]u8 = undefined;
    env[0] = @intFromEnum(backup_codec.BlockType.document_batch);
    env[1] = 0;
    std.mem.writeInt(u32, env[2..6], backup_codec.max_block_payload_bytes + 1, .little);
    try encoded.appendSlice(alloc, &env);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const archive_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/oversized.afb", .{tmp.sub_path});
    defer alloc.free(archive_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var archive = try std.Io.Dir.cwd().createFile(io, archive_path, .{ .truncate = true });
    try archive.writePositionalAll(io, encoded.items, 0);
    try archive.sync(io);
    archive.close(io);
    archive = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer archive.close(io);

    var store = try openTestStore(alloc, &tmp);
    defer store.close();
    try std.testing.expectError(
        error.BackupBlockTooLarge,
        importPortableFile(alloc, &store, io, archive, encoded.items.len),
    );
}

test "import preflights full portable envelope before mutating destination" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const store_key = try internal_keys.documentKeyAlloc(alloc, "doc:truncated-import");
    defer alloc.free(store_key);
    try src.putBatch(&.{.{ .key = store_key, .value = "{\"title\":\"must not import\"}" }}, &.{});

    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try exportPortable(alloc, &src, &portable);
    try std.testing.expect(portable.items.len > backup_codec.header_size);
    const truncated = portable.items[0 .. portable.items.len - 1];

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();

    try std.testing.expectError(error.EndOfStream, importPortable(alloc, &dst, truncated));
    try std.testing.expectError(error.NotFound, dst.get(alloc, store_key));
}

test "import preflights logical block payloads before mutating destination" {
    const alloc = std.testing.allocator;

    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try backup_codec.writeHeader(&portable, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });

    const good_doc = try backup_codec.encodeDocumentBatch(alloc, &.{
        .{
            .key = "doc:valid-before-malformed",
            .value_flags = 0,
            .value = "{\"title\":\"must not import\"}",
            .timestamp_ns = 0,
        },
    });
    defer alloc.free(good_doc);
    try backup_codec.writeBlock(&portable, alloc, .document_batch, good_doc);

    const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
    try backup_codec.writeBlock(&portable, alloc, .document_batch, &malformed_doc_payload);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();

    const store_key = try internal_keys.documentKeyAlloc(alloc, "doc:valid-before-malformed");
    defer alloc.free(store_key);
    try std.testing.expectError(error.Truncated, importPortable(alloc, &dst, portable.items));
    try std.testing.expectError(error.NotFound, dst.get(alloc, store_key));
}

test "export and import documents preserve timestamps" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_key = "doc:ttl";
    const timestamp_ns: u64 = 1_700_000_000_000_000_123;
    const store_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
    defer alloc.free(store_key);
    const ttl_key = try internal_keys.ttlKeyAlloc(alloc, doc_key);
    defer alloc.free(ttl_key);
    const ttl_value = try timestampValueAlloc(alloc, timestamp_ns);
    defer alloc.free(ttl_value);
    try src.putBatch(&.{
        .{ .key = store_key, .value = "{\"id\":\"doc:ttl\"}" },
        .{ .key = ttl_key, .value = ttl_value },
    }, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var saw_timestamp = false;
    var reader = backup_codec.SliceReader.init(out.items);
    _ = try reader.readHeader();
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type != .document_batch) continue;
        const entries = try backup_codec.decodeDocumentBatch(alloc, block.payload);
        defer {
            for (entries) |entry| {
                alloc.free(entry.key);
                alloc.free(entry.value);
            }
            alloc.free(entries);
        }
        for (entries) |entry| {
            if (!std.mem.eql(u8, entry.key, doc_key)) continue;
            try std.testing.expectEqual(timestamp_ns, entry.timestamp_ns);
            saw_timestamp = true;
        }
    }
    try std.testing.expect(saw_timestamp);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    const restored_ttl = try dst.get(alloc, ttl_key);
    defer alloc.free(restored_ttl);
    try std.testing.expectEqual(@as(usize, 8), restored_ttl.len);
    try std.testing.expectEqual(timestamp_ns, std.mem.readInt(u64, restored_ttl[0..8], .little));
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

test "export and import preserves portable schema and catalog metadata" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const public_schema = "{\"version\":1}";
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, public_schema);
    defer parsed_schema.deinit(alloc);
    const derived_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, derived_schema);
    const runtime_schema = try storage_schema.serializeSchema(alloc, derived_schema);
    defer alloc.free(runtime_schema);
    const portable_entries = [_]KVPair{
        .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
        .{ .key = "\x00\x00__metadata__:schema", .value = runtime_schema },
        .{ .key = "\x00\x00__metadata__:schema_v1", .value = "runtime-schema-v1" },
        .{ .key = "\x00\x00__metadata__:indexes", .value = "[{\"name\":\"ft\",\"kind\":\"full_text\",\"config_json\":\"{}\"}]" },
        .{ .key = "\x00\x00__metadata__:enrichments", .value = "[]" },
        .{ .key = "\x00\x00__metadata__:resolvers", .value = "[]" },
    };
    try src.putBatch(&portable_entries, &.{});
    try src.putBatch(&.{.{ .key = "\x00\x00__metadata__:text_field_analyzers:ft", .value = "{}" }}, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    for (portable_entries) |entry| {
        const restored = try dst.get(alloc, entry.key);
        defer alloc.free(restored);
        try std.testing.expectEqualStrings(entry.value, restored);
    }
    try std.testing.expectError(error.NotFound, dst.get(alloc, "\x00\x00__metadata__:text_field_analyzers:ft"));
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

        const vector = try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, val);
        defer alloc.free(vector);
        try std.testing.expectEqual(@as(usize, 4), vector.len);
        try std.testing.expectApproxEqAbs(@as(f32, 0.1), vector[0], 1e-6);
        try std.testing.expectApproxEqAbs(@as(f32, 0.4), vector[3], 1e-6);
    }
}

test "export and import sparse embeddings round trip" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_store_key = try internal_keys.documentKeyAlloc(alloc, "sparse-doc");
    defer alloc.free(doc_store_key);
    try src.putBatch(&.{.{ .key = doc_store_key, .value = "{\"id\":\"sparse-doc\",\"body\":\"alpha beta\"}" }}, &.{});

    const sparse_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "sparse-doc", "sparse_idx");
    defer alloc.free(sparse_key);
    const sparse_val = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(
        alloc,
        null,
        &.{ 2, 9, 17 },
        &.{ 0.5, 1.25, 0.75 },
    );
    defer alloc.free(sparse_val);
    try src.putBatch(&.{.{ .key = sparse_key, .value = sparse_val }}, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    const val = try dst.get(alloc, sparse_key);
    defer alloc.free(val);

    var decoded = try enrichment_artifact_codec.decodeSparseEmbeddingAlloc(alloc, val);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(u32, &.{ 2, 9, 17 }, decoded.indices);
    try std.testing.expectEqual(@as(usize, 3), decoded.values.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.values[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.25), decoded.values[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), decoded.values[2], 1e-6);
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

test "export and import chunk artifacts round trip with public artifact ids" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_key = "doc:chunk-source";
    const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, doc_key, "body_chunks_v1", 7);
    defer alloc.free(chunk_key);
    const unit_chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, "asset_chunks_v1", "unit:alpha", 3);
    defer alloc.free(unit_chunk_key);
    const chunk_value = "{\"body\":\"chunk seven\",\"_chunk_id\":7}";
    const unit_chunk_value = "{\"body\":\"unit chunk three\",\"_chunk_id\":3}";

    try src.putBatch(&.{
        .{ .key = chunk_key, .value = chunk_value },
        .{ .key = unit_chunk_key, .value = unit_chunk_value },
    }, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var saw_chunk_batch = false;
    var reader = backup_codec.SliceReader.init(out.items);
    _ = try reader.readHeader();
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type != .chunk_batch) continue;
        saw_chunk_batch = true;
        const entries = try backup_codec.decodeKeyValueBatch(alloc, block.payload);
        defer freeKeyValueEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        for (entries) |entry| {
            try std.testing.expect(std.mem.startsWith(u8, entry.key, "af1:chunk:"));
        }
    }
    try std.testing.expect(saw_chunk_batch);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    const restored_chunk = try dst.get(alloc, chunk_key);
    defer alloc.free(restored_chunk);
    try std.testing.expectEqualStrings(chunk_value, restored_chunk);

    const restored_unit_chunk = try dst.get(alloc, unit_chunk_key);
    defer alloc.free(restored_unit_chunk);
    try std.testing.expectEqualStrings(unit_chunk_value, restored_unit_chunk);
}

test "export and import asset artifacts round trip with public artifact ids" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const doc_key = "doc:asset-source";
    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", "document_units_v1");
    defer alloc.free(asset_key);
    const unit_asset_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, doc_key, "document_units_v1", "page:000001");
    defer alloc.free(unit_asset_key);
    const asset_value = "{\"units\":[\"page:000001\"]}";
    const unit_asset_value = "{\"text\":\"page one text\",\"page\":1}";

    try src.putBatch(&.{
        .{ .key = asset_key, .value = asset_value },
        .{ .key = unit_asset_key, .value = unit_asset_value },
    }, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var saw_artifact_batch = false;
    var reader = backup_codec.SliceReader.init(out.items);
    _ = try reader.readHeader();
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type != .artifact_batch) continue;
        saw_artifact_batch = true;
        const entries = try backup_codec.decodeKeyValueBatch(alloc, block.payload);
        defer freeKeyValueEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        for (entries) |entry| {
            try std.testing.expect(std.mem.startsWith(u8, entry.key, "af1:asset:"));
        }
    }
    try std.testing.expect(saw_artifact_batch);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    const restored_asset = try dst.get(alloc, asset_key);
    defer alloc.free(restored_asset);
    try std.testing.expectEqualStrings(asset_value, restored_asset);

    const restored_unit_asset = try dst.get(alloc, unit_asset_key);
    defer alloc.free(restored_unit_asset);
    try std.testing.expectEqualStrings(unit_asset_value, restored_unit_asset);
}

test "export and import resolution artifacts round trip with public artifact ids" {
    const alloc = std.testing.allocator;

    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:mention-source", "people_resolver_v1");
    defer alloc.free(resolution_key);
    const resolution_value =
        \\{"mentions":[{"text":"Ada","entity_id":"entity:ada","confidence":0.98}]}
    ;

    try src.putBatch(&.{.{ .key = resolution_key, .value = resolution_value }}, &.{});

    var out: ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try exportPortable(alloc, &src, &out);

    var saw_resolution_batch = false;
    var reader = backup_codec.SliceReader.init(out.items);
    _ = try reader.readHeader();
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type != .resolution_batch) continue;
        saw_resolution_batch = true;
        const entries = try backup_codec.decodeKeyValueBatch(alloc, block.payload);
        defer freeKeyValueEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expect(std.mem.startsWith(u8, entries[0].key, resolution_public_id_prefix));
    }
    try std.testing.expect(saw_resolution_batch);

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortable(alloc, &dst, out.items);

    const restored_resolution = try dst.get(alloc, resolution_key);
    defer alloc.free(restored_resolution);
    try std.testing.expectEqualStrings(resolution_value, restored_resolution);
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
