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
const platform_time = @import("antfly_platform").time;

const backup_codec = @import("backup_codec.zig");
const backup_bundle = @import("backup_bundle.zig");
const internal_keys = @import("internal_keys.zig");
const docstore_mod = @import("docstore.zig");
const doc_identity = @import("db/doc_identity.zig");
const relational_store = @import("db/relational_store.zig");
const relational_row_codec = @import("db/algebraic/relational_row_codec.zig");
const table_catalog = @import("db/table_catalog.zig");
const storage_schema = @import("schema.zig");
const SchemaSpool = @import("schema_spool.zig").Spool;
const public_table_schema = @import("../schema/mod.zig");
const db_types = @import("db/types.zig");
const artifact_ids = @import("db/artifact_ids.zig");
const enrichment_artifact_codec = @import("db/enrichment/artifact_codec.zig");
const index_manager = @import("db/catalog/index_manager.zig");
const enrichment_catalog = @import("db/catalog/enrichment_catalog.zig");
const resolver_catalog = @import("db/catalog/resolver_catalog.zig");
const DocStore = docstore_mod.DocStore;
const KeyEncoder = docstore_mod.KeyEncoder;
const KVPair = docstore_mod.KVPair;

/// Target batch size in bytes before flushing a document/embedding/edge batch.
const batch_target_bytes: usize = 4 * 1024 * 1024;
const portable_metadata_prefix = "\x00\x00__metadata__:";
const resolution_public_id_prefix = "af1:resolution:";
const schema_version_prefix = "\x00\x00__metadata__:schema_v";

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

const PortableObject = struct {
    block_type: backup_codec.BlockType,
    size_bytes: u64,
    sha256: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    path_buffer: [48]u8,
    path_len: u8,

    fn logicalPath(self: *const @This()) []const u8 {
        return self.path_buffer[0..self.path_len];
    }
};

const PortableBlob = struct {
    sha256: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    size_bytes: u64,
};

const PortableOutputMode = union(enum) {
    inventory: *std.ArrayListUnmanaged(PortableObject),
    bundle: struct {
        expected: []const PortableObject,
        next_ordinal: *usize,
        blobs: []const PortableBlob,
        included: []const bool,
        emitted: []bool,
        footer: *std.ArrayListUnmanaged(backup_bundle.FooterIndexEntry),
        /// The private file-backed spool is the exact byte stream replayed by
        /// bundle emission. Per-block CRC still detects accidental spool
        /// corruption, and the inventory SHA-256 is embedded for strict restore
        /// verification, so hashing the same private bytes twice is redundant.
        trust_inventory_digest: bool,
    },
};

const PortableOutput = struct {
    alloc: Allocator,
    writer: ?*std.Io.Writer = null,
    mode: PortableOutputMode,
    bytes_written: u64 = 0,
    bundle_offset: u64 = 0,

    fn writeHeader(self: *PortableOutput, header: backup_codec.FileHeader) !void {
        // AFB2 owns the physical file header. Keep the logical AFB1 header in
        // the byte count so portable file-footer accounting remains stable
        // across the inventory and emission passes.
        self.bytes_written += backup_codec.header_size;
        switch (self.mode) {
            .inventory => if (self.writer) |writer| try backup_codec.writeHeaderTo(writer, header),
            .bundle => {},
        }
    }

    fn writeBlock(self: *PortableOutput, block_type: backup_codec.BlockType, payload: []const u8) !void {
        self.bytes_written += backup_codec.block_envelope_overhead + payload.len;
        switch (self.mode) {
            .inventory => |objects| {
                var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
                if (objects.items.len >= backup_bundle.max_objects) return error.BackupManifestTooLarge;
                var object: PortableObject = .{
                    .block_type = block_type,
                    .size_bytes = payload.len,
                    .sha256 = digest,
                    .path_buffer = undefined,
                    .path_len = 0,
                };
                const path = try std.fmt.bufPrint(&object.path_buffer, "portable/{d:0>8}.block", .{objects.items.len});
                object.path_len = @intCast(path.len);
                try objects.append(self.alloc, object);
                if (self.writer) |writer| try backup_codec.writeBlockTo(writer, block_type, payload);
            },
            .bundle => |state| {
                if (state.next_ordinal.* >= state.expected.len) return error.NonDeterministicBackupCapture;
                const ordinal = state.next_ordinal.*;
                const expected = state.expected[ordinal];
                if (expected.block_type != block_type or expected.size_bytes != payload.len)
                    return error.NonDeterministicBackupCapture;
                const digest = expected.sha256;
                if (!state.trust_inventory_digest) {
                    var observed: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(payload, &observed, .{});
                    if (!std.crypto.timing_safe.eql(@TypeOf(digest), digest, observed))
                        return error.NonDeterministicBackupCapture;
                }
                const blob_index = portableBlobIndex(state.blobs, digest) orelse
                    return error.NonDeterministicBackupCapture;
                state.next_ordinal.* += 1;
                if (state.emitted[blob_index] or !state.included[blob_index]) return;
                state.emitted[blob_index] = true;
                const sink = self.writer orelse return error.InvalidBackupWriter;
                try state.footer.append(self.alloc, .{
                    .sha256 = digest,
                    .header_offset = self.bundle_offset,
                    .stored_size_bytes = payload.len,
                });
                const digest_hex = std.fmt.bytesToHex(digest, .lower);
                const blob_path = try std.fmt.allocPrint(self.alloc, "blobs/{s}", .{digest_hex});
                defer self.alloc.free(blob_path);
                const header_payload = try backup_bundle.encodeBlobHeaderAlloc(self.alloc, .{
                    .ordinal = @intCast(blob_index),
                    .logical_path = blob_path,
                    .role = "portable_blob",
                    .logical_size_bytes = payload.len,
                    .stored_size_bytes = payload.len,
                    .sha256 = digest,
                });
                defer self.alloc.free(header_payload);
                try backup_codec.writeBlockTo(sink, .blob_header, header_payload);
                self.bundle_offset += backup_codec.block_envelope_overhead + header_payload.len;
                var offset: usize = 0;
                while (offset < payload.len) {
                    const end = @min(payload.len, offset + backup_bundle.native_chunk_target_bytes);
                    const chunk_payload = try backup_bundle.encodeBlobChunkAlloc(self.alloc, .{
                        .ordinal = @intCast(blob_index),
                        .offset = offset,
                        .bytes = payload[offset..end],
                    });
                    defer self.alloc.free(chunk_payload);
                    try backup_codec.writeBlockTo(sink, .blob_chunk, chunk_payload);
                    self.bundle_offset += backup_codec.block_envelope_overhead + chunk_payload.len;
                    offset = end;
                }
            },
        }
    }
};

fn portableBlobIndex(blobs: []const PortableBlob, digest: [std.crypto.hash.sha2.Sha256.digest_length]u8) ?usize {
    var low: usize = 0;
    var high: usize = blobs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, &blobs[mid].sha256, &digest)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return mid,
        }
    }
    return null;
}

/// Export all portable data from the DocStore into AFB format.
/// The caller provides an allocator for temporary buffers. The output is
/// appended to `out`.
pub fn exportPortable(alloc: Allocator, store: *DocStore, out: *ArrayList(u8)) !void {
    var allocating = std.Io.Writer.Allocating.fromArrayList(alloc, out);
    defer out.* = allocating.toArrayList();
    try exportPortableToWriter(alloc, store, &allocating.writer);
}

pub fn exportPortableToWriter(alloc: Allocator, store: *DocStore, sink_writer: *std.Io.Writer) !void {
    return try exportPortableToWriterWithOptions(alloc, store, sink_writer, .{});
}

pub const ExportOptions = struct {
    header_backup_id: [16]u8 = [_]u8{0} ** 16,
    backup_id: []const u8 = "",
    table_name: []const u8 = "",
    created_at_unix_ns: i64 = 0,
    capture: backup_bundle.CaptureMode = .full,
    /// Optional bounded-disk staging stream. Production file exports provide
    /// this so the immutable store snapshot is classified and encoded once;
    /// callers without filesystem authority retain the deterministic two-pass
    /// compatibility path.
    spool: ?struct { io: std.Io, file: std.Io.File } = null,
};

pub fn exportPortableToWriterWithOptions(
    alloc: Allocator,
    store: *DocStore,
    sink_writer: *std.Io.Writer,
    options: ExportOptions,
) !void {
    const snapshot_mode: backup_bundle.SnapshotMode = switch (options.capture) {
        .full => .full,
        .delta => |base| blk: {
            try base.validate(alloc);
            if (base.manifest.representation != .portable or
                !std.mem.eql(u8, options.backup_id, base.manifest.backup_id) or
                !std.mem.eql(u8, options.table_name, base.manifest.table_name))
                return error.BackupBaseMismatch;
            break :blk .delta;
        },
    };
    const parent_manifest_sha256: ?[]const u8 = switch (options.capture) {
        .full => null,
        .delta => |base| base.manifest_sha256,
    };
    var scan = try store.beginReadTxn();
    defer scan.abort();

    var objects = std.ArrayListUnmanaged(PortableObject).empty;
    defer objects.deinit(alloc);
    var inventory_out: PortableOutput = .{ .alloc = alloc, .mode = .{ .inventory = &objects } };
    if (options.spool) |spool| {
        var spool_buffer: [64 * 1024]u8 = undefined;
        var spool_writer = spool.file.writer(spool.io, &spool_buffer);
        inventory_out.writer = &spool_writer.interface;
        try exportPortableSnapshot(alloc, &scan, &inventory_out);
        try spool_writer.end();
        inventory_out.writer = null;
    } else {
        try exportPortableSnapshot(alloc, &scan, &inventory_out);
    }

    const descriptors = try alloc.alloc(backup_bundle.ObjectDescriptor, objects.items.len);
    defer alloc.free(descriptors);
    var digest_strings = try alloc.alloc([std.crypto.hash.sha2.Sha256.digest_length * 2]u8, objects.items.len);
    defer alloc.free(digest_strings);
    for (objects.items, 0..) |*object, index| {
        digest_strings[index] = std.fmt.bytesToHex(object.sha256, .lower);
        descriptors[index] = .{
            .logical_path = object.logicalPath(),
            .role = @tagName(object.block_type),
            .size_bytes = object.size_bytes,
            .sha256 = &digest_strings[index],
        };
    }
    var unique_blobs = std.ArrayListUnmanaged(PortableBlob).empty;
    defer unique_blobs.deinit(alloc);
    try unique_blobs.ensureTotalCapacity(alloc, objects.items.len);
    for (objects.items) |object| unique_blobs.appendAssumeCapacity(.{
        .sha256 = object.sha256,
        .size_bytes = object.size_bytes,
    });
    std.mem.sort(PortableBlob, unique_blobs.items, {}, struct {
        fn lessThan(_: void, lhs: PortableBlob, rhs: PortableBlob) bool {
            return std.mem.order(u8, &lhs.sha256, &rhs.sha256) == .lt;
        }
    }.lessThan);
    var unique_len: usize = 0;
    for (unique_blobs.items) |candidate| {
        if (unique_len > 0 and std.mem.eql(u8, &unique_blobs.items[unique_len - 1].sha256, &candidate.sha256)) {
            if (unique_blobs.items[unique_len - 1].size_bytes != candidate.size_bytes)
                return error.NonDeterministicBackupCapture;
            continue;
        }
        unique_blobs.items[unique_len] = candidate;
        unique_len += 1;
    }
    unique_blobs.items.len = unique_len;
    const blob_descriptors = try alloc.alloc(backup_bundle.BlobDescriptor, unique_blobs.items.len);
    defer alloc.free(blob_descriptors);
    var blob_digest_strings = try alloc.alloc([std.crypto.hash.sha2.Sha256.digest_length * 2]u8, unique_blobs.items.len);
    defer alloc.free(blob_digest_strings);
    for (unique_blobs.items, 0..) |blob, index| {
        blob_digest_strings[index] = std.fmt.bytesToHex(blob.sha256, .lower);
        blob_descriptors[index] = .{
            .sha256 = &blob_digest_strings[index],
            .logical_size_bytes = blob.size_bytes,
            .stored_size_bytes = blob.size_bytes,
            .included = switch (options.capture) {
                .full => true,
                .delta => |base| !base.containsBlob(&blob_digest_strings[index]),
            },
        };
    }
    const manifest = try backup_bundle.encodeManifestAlloc(alloc, .{
        .backup_id = options.backup_id,
        .table_name = options.table_name,
        .representation = .portable,
        .mode = snapshot_mode,
        .parent_manifest_sha256 = parent_manifest_sha256,
        .created_at_unix_ns = options.created_at_unix_ns,
        .compatibility = .{ .storage_engine = "logical" },
        .objects = descriptors,
        .blobs = blob_descriptors,
    });
    defer alloc.free(manifest);

    try backup_codec.writeHeaderTo(sink_writer, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = options.created_at_unix_ns,
        .backup_id = options.header_backup_id,
        .table_count = 1,
        .shard_count = 1,
    });
    try backup_codec.writeBlockTo(sink_writer, .bundle_manifest, manifest);

    var footer = std.ArrayListUnmanaged(backup_bundle.FooterIndexEntry).empty;
    defer footer.deinit(alloc);
    var next_ordinal: usize = 0;
    const emitted = try alloc.alloc(bool, unique_blobs.items.len);
    defer alloc.free(emitted);
    @memset(emitted, false);
    const included = try alloc.alloc(bool, blob_descriptors.len);
    defer alloc.free(included);
    for (blob_descriptors, 0..) |blob, index| included[index] = blob.included;
    var bundle_out: PortableOutput = .{
        .alloc = alloc,
        .writer = sink_writer,
        .mode = .{ .bundle = .{
            .expected = objects.items,
            .next_ordinal = &next_ordinal,
            .blobs = unique_blobs.items,
            .included = included,
            .emitted = emitted,
            .footer = &footer,
            .trust_inventory_digest = options.spool != null,
        } },
        .bundle_offset = backup_codec.header_size + backup_codec.block_envelope_overhead + manifest.len,
    };
    if (options.spool) |spool| {
        var spool_reader = backup_codec.FileReader.init(spool.io, spool.file, inventory_out.bytes_written);
        _ = try spool_reader.readHeader();
        while (spool_reader.hasRemaining()) {
            const block = try spool_reader.readBlock(alloc);
            defer alloc.free(block.payload);
            try bundle_out.writeBlock(block.block_type, block.payload);
        }
        _ = try spool_reader.verifiedFingerprint();
    } else {
        try exportPortableSnapshot(alloc, &scan, &bundle_out);
    }
    if (next_ordinal != objects.items.len) return error.NonDeterministicBackupCapture;
    const footer_payload = try backup_bundle.encodeFooterIndexAlloc(alloc, footer.items);
    defer alloc.free(footer_payload);
    const footer_offset = bundle_out.bundle_offset;
    try backup_codec.writeBlockTo(sink_writer, .footer_index, footer_payload);
    const trailer = backup_bundle.encodeTrailer(.{
        .footer_offset = footer_offset,
        .footer_payload_size = footer_payload.len,
    });
    try sink_writer.writeAll(&trailer);
}

fn exportPortableSnapshot(alloc: Allocator, scan: *DocStore.Txn, out: *PortableOutput) !void {
    const backup_id = [_]u8{0} ** 16; // zero UUID for now
    try out.writeHeader(.{
        .format_version = backup_codec.legacy_format_version,
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
    var schema_cache: PortableArchiveValidation = .{
        .format_version = backup_codec.format_version,
        .snapshot = scan,
        .snapshot_alloc = alloc,
    };
    defer schema_cache.deinit(alloc);

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
    // AFB2 is manifest-first regardless of the backend's physical key order.
    // The snapshot transaction is immutable, so a metadata-only cursor pass
    // produces a stable layout registry without buffering user rows. The
    // second pass streams each row exactly once into its output block.
    var scan_entry = try cursor.seekAtOrAfter(portable_metadata_prefix);
    while (scan_entry) |kv| : (scan_entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, portable_metadata_prefix)) break;
        if (!isPortableMetadataKey(kv.key)) continue;
        if (std.mem.startsWith(u8, kv.key, schema_version_prefix)) {
            const layout = try storage_schema.deserializeSchema(alloc, kv.value);
            defer storage_schema.freeSchema(alloc, layout);
            const expected_key = try storage_schema.schemaVersionKeyAlloc(alloc, layout.version);
            defer alloc.free(expected_key);
            if (!std.mem.eql(u8, expected_key, kv.key)) return error.InvalidSchema;
            if (schema_cache.layouts.contains(layout.version)) return error.InvalidSchema;
            // The immutable source snapshot already owns the serialized bytes.
            // Export needs only a directory and the same bounded decoded cache
            // as restore, not a second copy of every layout.
            try schema_cache.layouts.putNoClobber(alloc, layout.version, .{ .offset = 0, .len = 0 });
        }
        try metadata_batch.append(alloc, .{
            .key = try alloc.dupe(u8, kv.key),
            .value = try alloc.dupe(u8, kv.value),
        });
        metadata_batch_bytes += kv.key.len + kv.value.len;
        if (metadata_batch_bytes >= batch_target_bytes) {
            try flushMetadataBatch(alloc, out, &metadata_batch);
            metadata_batch_bytes = 0;
        }
    }
    try flushMetadataBatch(alloc, out, &metadata_batch);
    metadata_batch_bytes = 0;

    scan_entry = try cursor.first();
    while (scan_entry) |kv| : (scan_entry = try cursor.next()) {
        if (isPortableMetadataKey(kv.key)) continue;

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
            if (internal_keys.isStoredDocumentRowKey(kv.key)) {
                const relational_row = internal_keys.isRelationalRowKey(kv.key);
                if (relational_row) {
                    const schema_version = try relational_store.rowSchemaVersion(kv.value);
                    const layout = (try schema_cache.layoutForVersion(schema_version)) orelse return error.InvalidSchema;
                    try relational_store.validateValueForSchemaAndLayout(kv.value, layout.schema.*, layout.physical);
                }
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
                    try flushDocBatch(alloc, out, &doc_batch, &counts);
                    doc_batch_bytes = 0;
                }
            } else if (internal_keys.isChunkArtifactRecordKey(kv.key)) {
                try appendChunkArtifactEntry(alloc, &chunk_batch, kv.key, kv.value);
                chunk_batch_bytes += kv.key.len + kv.value.len;
                if (chunk_batch_bytes >= batch_target_bytes) {
                    try flushChunkBatch(alloc, out, &chunk_batch);
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
                    try flushKeyValueBlock(alloc, out, &resolution_batch, .resolution_batch);
                    resolution_batch_bytes = 0;
                }
            } else if (try appendPortableArtifactEntry(alloc, &artifact_batch, kv.key, kv.value, .asset)) {
                artifact_batch_bytes += kv.key.len + kv.value.len;
                if (artifact_batch_bytes >= batch_target_bytes) {
                    try flushKeyValueBlock(alloc, out, &artifact_batch, .artifact_batch);
                    artifact_batch_bytes = 0;
                }
            }
            if (derived_batch_bytes >= batch_target_bytes) {
                try flushDerivedBatches(alloc, out, &emb_batches, &sparse_batches, &edge_batches, &counts);
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
                    try flushDerivedBatches(alloc, out, &emb_batches, &sparse_batches, &edge_batches, &counts);
                    derived_batch_bytes = 0;
                }
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
    if (chunk_batch.items.len > 0) {
        try flushChunkBatch(alloc, out, &chunk_batch);
    }
    if (artifact_batch.items.len > 0) {
        try flushKeyValueBlock(alloc, out, &artifact_batch, .artifact_batch);
    }
    if (resolution_batch.items.len > 0) {
        try flushKeyValueBlock(alloc, out, &resolution_batch, .resolution_batch);
    }

    try flushDerivedBatches(alloc, out, &emb_batches, &sparse_batches, &edge_batches, &counts);

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

pub fn isPortableMetadataKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "\x00\x00__metadata__:schema") or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:schema_v") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:schema_json") or
        std.mem.startsWith(u8, key, public_table_schema.versioned_schema_key_prefix) or
        std.mem.eql(u8, key, table_catalog.key) or
        std.mem.eql(u8, key, "\x00\x00__metadata__:indexes") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:enrichments") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:resolvers");
}

/// True only for durable keys that a validated portable import may create.
/// Publication rollback uses this ownership boundary so target-local routing,
/// split, replay, and control metadata are never mistaken for staged data.
pub fn isPortableStoreKey(key: []const u8) bool {
    if (key.len == 0) return false;
    return key[0] == internal_keys.user_namespace or
        key[0] == internal_keys.identity_namespace or
        isPortableMetadataKey(key);
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

/// Exact parent content provider for portable AFB2 deltas. Each returned
/// allocation is one logical portable block and is rehashed before use.
pub const PortableBaseBlobSource = struct {
    manifest_sha256: []const u8,
    context: *anyopaque,
    read_alloc: *const fn (
        context: *anyopaque,
        alloc: Allocator,
        sha256: []const u8,
        logical_size_bytes: u64,
        limit: usize,
    ) anyerror![]u8,
};

fn readAndLimitAfb2Trailer(comptime RawReader: type, raw: *RawReader) !backup_bundle.Trailer {
    if (RawReader == backup_codec.SliceReader) {
        if (raw.data.len < backup_codec.header_size + backup_bundle.trailer_size)
            return error.InvalidBundleFooter;
        const bundle_size: u64 = @intCast(raw.data.len);
        const encoded = raw.data[raw.data.len - backup_bundle.trailer_size ..][0..backup_bundle.trailer_size].*;
        const trailer = try backup_bundle.decodeTrailer(&encoded, bundle_size);
        raw.data = raw.data[0 .. raw.data.len - backup_bundle.trailer_size];
        return trailer;
    }
    if (RawReader == backup_codec.FileReader) {
        if (raw.size < backup_codec.header_size + backup_bundle.trailer_size)
            return error.InvalidBundleFooter;
        var encoded: [backup_bundle.trailer_size]u8 = undefined;
        if (try raw.file.readPositionalAll(raw.io, &encoded, raw.size - backup_bundle.trailer_size) != encoded.len)
            return error.InvalidBundleFooter;
        const trailer = try backup_bundle.decodeTrailer(&encoded, raw.size);
        raw.size -= backup_bundle.trailer_size;
        return trailer;
    }
    @compileError("AFB2 portable reader requires a positional slice or file reader");
}

/// Presents both the released AFB1 typed stream and an AFB2 portable object
/// bundle as the same logical sequence of portable blocks. AFB2 is verified
/// object-by-object, so file restore retains the one-block memory bound.
fn PortableArchiveReader(comptime RawReader: type) type {
    return struct {
        const Self = @This();

        raw: *RawReader,
        header: backup_codec.FileHeader,
        manifest: ?backup_bundle.ParsedManifest = null,
        manifest_pending: bool,
        next_ordinal: u32 = 0,
        done: bool = false,
        indexed: bool = false,
        blob_offsets: []?backup_bundle.FooterIndexEntry = &.{},
        base: ?PortableBaseBlobSource = null,
        trailer: ?backup_bundle.Trailer = null,
        logical_hasher: std.crypto.hash.Blake3 = std.crypto.hash.Blake3.init(.{}),
        completed_pass_fingerprint: ?backup_codec.FileReader.Fingerprint = null,
        generation_changed: bool = false,

        fn init(raw: *RawReader) !Self {
            return initWithBase(raw, null);
        }

        fn initWithBase(raw: *RawReader, base: ?PortableBaseBlobSource) !Self {
            const header = try raw.readHeader();
            if (header.format_version == backup_codec.legacy_format_version and base != null)
                return error.UnexpectedBackupBase;
            const trailer = if (header.format_version == backup_codec.format_version)
                try readAndLimitAfb2Trailer(RawReader, raw)
            else
                null;
            var self: Self = .{
                .raw = raw,
                .header = header,
                .manifest_pending = header.format_version == backup_codec.format_version,
                .base = base,
                .trailer = trailer,
            };
            self.observeHeader(header);
            return self;
        }

        fn deinit(self: *Self, alloc: Allocator) void {
            if (self.manifest) |*manifest| manifest.deinit();
            if (self.blob_offsets.len > 0) alloc.free(self.blob_offsets);
            self.* = undefined;
        }

        pub fn readHeader(self: *Self) !backup_codec.FileHeader {
            return self.header;
        }

        /// Rewinds the logical archive without retaining decoded rows. AFB2
        /// uses positional reads for objects, so its pass fingerprint belongs
        /// here rather than to the physical framing reader.
        pub fn reset(self: *Self, alloc: Allocator) !void {
            if (self.header.format_version == backup_codec.legacy_format_version) {
                self.raw.reset();
                self.header = try self.raw.readHeader();
                return;
            }
            if (!self.done) return error.EndOfStream;
            const fingerprint = self.currentLogicalFingerprint();
            if (self.completed_pass_fingerprint) |expected| {
                if (!std.mem.eql(u8, &expected, &fingerprint)) self.generation_changed = true;
            } else {
                self.completed_pass_fingerprint = fingerprint;
            }
            if (self.manifest) |*manifest| manifest.deinit();
            self.manifest = null;
            if (self.blob_offsets.len > 0) alloc.free(self.blob_offsets);
            self.blob_offsets = &.{};
            self.raw.reset();
            const header = try self.raw.readHeader();
            self.header = header;
            self.manifest_pending = true;
            self.next_ordinal = 0;
            self.done = false;
            self.indexed = false;
            self.logical_hasher = std.crypto.hash.Blake3.init(.{});
            self.observeHeader(header);
        }

        pub fn verifiedFingerprint(self: *const Self) !backup_codec.FileReader.Fingerprint {
            if (RawReader != backup_codec.FileReader)
                @compileError("portable file fingerprints require backup_codec.FileReader");
            if (self.header.format_version == backup_codec.legacy_format_version)
                return try self.raw.verifiedFingerprint();
            if (!self.done) return error.EndOfStream;
            if (self.generation_changed) return error.SourceFileChanged;
            const fingerprint = self.currentLogicalFingerprint();
            if (self.completed_pass_fingerprint) |expected| {
                if (!std.mem.eql(u8, &expected, &fingerprint)) return error.SourceFileChanged;
                return expected;
            }
            return fingerprint;
        }

        pub fn verifyFingerprint(self: *const Self, expected: backup_codec.FileReader.Fingerprint) !void {
            const actual = try self.verifiedFingerprint();
            if (!std.mem.eql(u8, &expected, &actual)) return error.SourceFileChanged;
        }

        fn observeHeader(self: *Self, header: backup_codec.FileHeader) void {
            var encoded: [44]u8 = undefined;
            std.mem.writeInt(u32, encoded[0..4], header.format_version, .little);
            std.mem.writeInt(u32, encoded[4..8], header.flags, .little);
            std.mem.writeInt(i64, encoded[8..16], header.created_at_ns, .little);
            @memcpy(encoded[16..32], &header.backup_id);
            std.mem.writeInt(u32, encoded[32..36], header.table_count, .little);
            std.mem.writeInt(u32, encoded[36..40], header.shard_count, .little);
            std.mem.writeInt(u32, encoded[40..44], 0x48445232, .little);
            self.logical_hasher.update(&encoded);
        }

        fn observeLogicalBlock(self: *Self, block_type: backup_codec.BlockType, payload: []const u8) void {
            const tag = [_]u8{@intFromEnum(block_type)};
            var len: [8]u8 = undefined;
            std.mem.writeInt(u64, &len, @intCast(payload.len), .little);
            self.logical_hasher.update(&tag);
            self.logical_hasher.update(&len);
            self.logical_hasher.update(payload);
        }

        fn currentLogicalFingerprint(self: *const Self) backup_codec.FileReader.Fingerprint {
            var fingerprint: backup_codec.FileReader.Fingerprint = undefined;
            self.logical_hasher.final(&fingerprint);
            return fingerprint;
        }

        pub fn hasRemaining(self: *const Self) bool {
            return if (self.header.format_version == backup_codec.legacy_format_version)
                self.raw.hasRemaining()
            else
                !self.done;
        }

        pub fn readBlock(self: *Self, alloc: Allocator) !backup_codec.Block {
            if (self.header.format_version == backup_codec.legacy_format_version)
                return try self.raw.readBlock(alloc);
            if (self.done) return error.EndOfStream;

            if (self.manifest_pending) {
                const manifest_block = try self.raw.readBlock(alloc);
                errdefer alloc.free(manifest_block.payload);
                if (manifest_block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
                var manifest = try backup_bundle.parseManifest(alloc, manifest_block.payload);
                errdefer manifest.deinit();
                try backup_bundle.validateReadablePayloadFeatures(manifest.value);
                if (manifest.value.representation != .portable or manifest.value.objects.len == 0 or
                    manifest.value.blobs.len == 0)
                    return error.BackupArtifactFormatMismatch;
                switch (manifest.value.mode) {
                    .full => if (self.base != null) return error.UnexpectedBackupBase,
                    .delta => {
                        const supplied = self.base orelse return error.BackupBaseRequired;
                        try backup_bundle.validateSha256(supplied.manifest_sha256);
                        if (!std.mem.eql(
                            u8,
                            manifest.value.parent_manifest_sha256.?,
                            supplied.manifest_sha256,
                        )) return error.BackupBaseMismatch;
                    },
                }
                self.manifest = manifest;
                self.manifest_pending = false;
                self.observeLogicalBlock(manifest_block.block_type, manifest_block.payload);
                return manifest_block;
            }

            const manifest = &(self.manifest orelse return error.InvalidBackupManifest).value;
            if (self.next_ordinal >= manifest.objects.len) return error.IncompleteBackupInventory;
            const descriptor = manifest.objects[self.next_ordinal];
            const blob_index = backup_bundle.blobIndex(manifest.blobs, descriptor.sha256) orelse
                return error.IncompleteBackupInventory;
            try self.ensureIndex(alloc);
            const block_type = std.meta.stringToEnum(backup_codec.BlockType, descriptor.role) orelse
                return error.InvalidBackupManifest;
            switch (block_type) {
                .bundle_manifest, .blob_header, .blob_chunk, .footer_index => return error.InvalidBackupManifest,
                else => {},
            }
            const payload = if (manifest.blobs[blob_index].included) blk: {
                const footer_entry = self.blob_offsets[blob_index] orelse
                    return error.IncompleteBackupInventory;
                break :blk try self.readBlobAt(alloc, blob_index, footer_entry.header_offset);
            } else try self.readBaseBlob(alloc, blob_index);
            self.next_ordinal += 1;
            if (self.next_ordinal == manifest.objects.len) self.done = true;
            self.observeLogicalBlock(block_type, payload);
            return .{ .block_type = block_type, .payload = payload };
        }

        /// Scans and verifies physical blobs once, retaining only the bounded
        /// digest-to-offset index. Logical objects can then be replayed in path
        /// order without retaining an archive-sized payload cache.
        fn ensureIndex(self: *Self, alloc: Allocator) !void {
            if (self.indexed) return;
            const manifest = &(self.manifest orelse return error.InvalidBackupManifest).value;
            const trailer = self.trailer orelse return error.InvalidBundleFooter;
            const offsets = try alloc.alloc(?backup_bundle.FooterIndexEntry, manifest.blobs.len);
            errdefer alloc.free(offsets);
            @memset(offsets, null);
            var cursor = self.raw.*;
            cursor.pos = @intCast(trailer.footer_offset);
            const raw_footer = try cursor.readBlock(alloc);
            defer alloc.free(raw_footer.payload);
            if (raw_footer.block_type != .footer_index or
                raw_footer.payload.len != trailer.footer_payload_size or cursor.hasRemaining())
                return error.InvalidBundleFooter;
            const footer = try backup_bundle.decodeFooterIndexAlloc(alloc, raw_footer.payload);
            defer alloc.free(footer);
            for (footer) |entry| {
                const digest_hex = std.fmt.bytesToHex(entry.sha256, .lower);
                const blob_index = backup_bundle.blobIndex(manifest.blobs, &digest_hex) orelse
                    return error.InvalidBundleFooter;
                const blob = manifest.blobs[blob_index];
                if (!blob.included or offsets[blob_index] != null or
                    entry.header_offset >= trailer.footer_offset or
                    entry.stored_size_bytes != blob.stored_size_bytes)
                    return error.InvalidBundleFooter;
                offsets[blob_index] = entry;
            }
            for (manifest.blobs, offsets) |blob, entry| {
                if (blob.included != (entry != null)) return error.IncompleteBackupInventory;
            }
            self.blob_offsets = offsets;
            self.indexed = true;
        }

        fn readBlobAt(self: *Self, alloc: Allocator, blob_index: usize, header_offset: u64) ![]u8 {
            const manifest = &(self.manifest orelse return error.InvalidBackupManifest).value;
            const blob = manifest.blobs[blob_index];
            var cursor = self.raw.*;
            cursor.pos = @intCast(header_offset);
            const raw_header = try cursor.readBlock(alloc);
            defer alloc.free(raw_header.payload);
            if (raw_header.block_type != .blob_header) return error.InvalidBackupManifest;
            var blob_header = try backup_bundle.decodeBlobHeader(alloc, raw_header.payload);
            defer blob_header.deinit(alloc);
            const digest_hex = std.fmt.bytesToHex(blob_header.sha256, .lower);
            const expected_path = try std.fmt.allocPrint(alloc, "blobs/{s}", .{digest_hex});
            defer alloc.free(expected_path);
            if (blob_header.ordinal != blob_index or blob_header.compression != .none or
                !std.mem.eql(u8, blob.sha256, &digest_hex) or
                !std.mem.eql(u8, blob_header.logical_path, expected_path) or
                !std.mem.eql(u8, blob_header.role, "portable_blob") or
                blob_header.logical_size_bytes != blob.logical_size_bytes or
                blob_header.stored_size_bytes != blob.stored_size_bytes or
                blob.stored_size_bytes > backup_codec.max_block_payload_bytes)
                return error.BackupArtifactIntegrityMismatch;
            const payload = try alloc.alloc(u8, @intCast(blob.stored_size_bytes));
            errdefer alloc.free(payload);
            var written: usize = 0;
            while (written < payload.len) {
                const raw_chunk = try cursor.readBlock(alloc);
                defer alloc.free(raw_chunk.payload);
                if (raw_chunk.block_type != .blob_chunk) return error.InvalidBackupManifest;
                const chunk = try backup_bundle.decodeBlobChunk(raw_chunk.payload);
                if (chunk.ordinal != blob_header.ordinal or chunk.offset != written or
                    chunk.bytes.len > payload.len - written)
                    return error.InvalidNativeFileChunk;
                @memcpy(payload[written..][0..chunk.bytes.len], chunk.bytes);
                written += chunk.bytes.len;
            }
            if (@as(u64, @intCast(cursor.pos)) > (self.trailer orelse return error.InvalidBundleFooter).footer_offset)
                return error.InvalidBundleFooter;
            var actual: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(payload, &actual, .{});
            if (!std.crypto.timing_safe.eql(@TypeOf(actual), actual, blob_header.sha256))
                return error.BackupArtifactIntegrityMismatch;
            return payload;
        }

        fn readBaseBlob(self: *Self, alloc: Allocator, blob_index: usize) ![]u8 {
            const blob = (self.manifest orelse return error.InvalidBackupManifest).value.blobs[blob_index];
            const source = self.base orelse return error.BackupBaseRequired;
            if (blob.logical_size_bytes > backup_codec.max_block_payload_bytes)
                return error.BackupBlockTooLarge;
            const payload = try source.read_alloc(
                source.context,
                alloc,
                blob.sha256,
                blob.logical_size_bytes,
                backup_codec.max_block_payload_bytes,
            );
            errdefer alloc.free(payload);
            if (payload.len != blob.logical_size_bytes) return error.BackupArtifactIntegrityMismatch;
            var actual: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(payload, &actual, .{});
            const actual_hex = std.fmt.bytesToHex(actual, .lower);
            if (!std.mem.eql(u8, &actual_hex, blob.sha256))
                return error.BackupArtifactIntegrityMismatch;
            return payload;
        }
    };
}

pub const default_schema_cache_bytes: usize = 16 * 1024 * 1024;

pub const ImportOptions = struct {
    pub const EmbeddingSourceField = struct {
        index_name: []const u8,
        field_name: []const u8,
    };

    identity_namespace: ?doc_identity.Namespace = null,
    prefer_existing_identity_namespace: bool = false,
    import_derived_indexes: bool = true,
    embedding_source_fields: []const EmbeddingSourceField = &.{},
    bundle_base: ?PortableBaseBlobSource = null,
    /// The destination is disposable and cannot become visible until this
    /// function succeeds. This enables validate-and-apply in one streaming
    /// pass; any error is handled by discarding the staging generation.
    unpublished_staging: bool = false,
    progress_context: ?*anyopaque = null,
    progress_fn: ?*const fn (?*anyopaque, ImportProgress) void = null,
    cancel_context: ?*anyopaque = null,
    is_cancelled_fn: ?*const fn (?*anyopaque) bool = null,
    /// Decoded historical epochs, not total archive history. A single oversized
    /// epoch may exceed this budget while its row is being validated.
    schema_cache_bytes: usize = default_schema_cache_bytes,
};

pub const ImportProgress = struct {
    blocks_processed: u64,
    rows_validated: u64,
    payload_bytes_processed: u64,
    elapsed_ns: u64,
    rows_per_second: u64,
};

/// Import AFB data into the DocStore.
pub fn importPortable(alloc: Allocator, store: *DocStore, data: []const u8) !void {
    return try importPortableWithOptions(alloc, store, data, .{});
}

pub fn validatePortable(alloc: Allocator, data: []const u8) !void {
    try validatePortableImportBlocks(alloc, data, .{});
}

/// Validate the cross-catalog invariant required when an archive represents a
/// complete database image. Generic portable imports intentionally remain
/// usable for identity-free data migration; production restore entry points
/// must call this after importing and before publishing their generation.
pub fn validateCompleteDatabaseImageAlloc(alloc: Allocator, store: *DocStore) !void {
    try doc_identity.validatePrimaryDocumentCoverageAlloc(alloc, store);

    const runtime_schema = try storage_schema.loadSchema(store, alloc);
    defer if (runtime_schema) |schema| storage_schema.freeSchema(alloc, schema);
    const schema_json = store.get(alloc, "\x00\x00__metadata__:schema_json") catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (schema_json) |value| alloc.free(value);

    // A public schema is never a standalone durable contract: it is the source
    // from which the executable runtime schema is derived. Direct compiled
    // document schemas may legitimately omit schema_json, while relational
    // schemas require it for the constraints that the packed column catalog
    // cannot represent on its own.
    if (schema_json != null and runtime_schema == null) return error.InvalidBackupRequest;
    if (runtime_schema) |schema| {
        if (schema.storage_mode == .relational and schema_json == null) return error.InvalidBackupRequest;
        if (schema_json) |value| {
            var public_schema = public_table_schema.parseValidatedTableSchema(alloc, value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            defer public_schema.deinit(alloc);
            const derived = public_table_schema.deriveRuntimeTableSchema(alloc, public_schema) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            defer storage_schema.freeSchema(alloc, derived);
            const schemas_match = storage_schema.schemasEqual(alloc, schema, derived) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            if (!schemas_match) return error.InvalidBackupRequest;
        }
    }
}

/// Visits the verified logical portable blocks of either AFB1 or AFB2. This is
/// the single framing boundary for metadata inspection, import, and tooling;
/// callers never need to know whether records were direct AFB1 blocks or AFB2
/// digest-addressed objects.
pub fn visitPortableBlocks(
    alloc: Allocator,
    data: []const u8,
    context: anytype,
    comptime visit: fn (@TypeOf(context), backup_codec.BlockType, []const u8) anyerror!void,
) !void {
    return try visitPortableBlocksWithBase(alloc, data, null, context, visit);
}

pub fn visitPortableBlocksWithBase(
    alloc: Allocator,
    data: []const u8,
    base: ?PortableBaseBlobSource,
    context: anytype,
    comptime visit: fn (@TypeOf(context), backup_codec.BlockType, []const u8) anyerror!void,
) !void {
    var raw = backup_codec.SliceReader.init(data);
    var reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&raw, base);
    defer reader.deinit(alloc);
    _ = try reader.readHeader();
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        try visit(context, block.block_type, block.payload);
    }
}

pub fn importPortableWithOptions(alloc: Allocator, store: *DocStore, data: []const u8, opts: ImportOptions) !void {
    if (opts.unpublished_staging) {
        var raw = backup_codec.SliceReader.init(data);
        var reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&raw, opts.bundle_base);
        defer reader.deinit(alloc);
        const imported_identity = try validateAndImportPortableStagingReader(alloc, store, &reader, opts);
        return try finishPortableIdentityImport(alloc, store, opts, imported_identity);
    }
    var validation_raw = backup_codec.SliceReader.init(data);
    var validation_reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&validation_raw, opts.bundle_base);
    defer validation_reader.deinit(alloc);
    try validatePortableImportReader(alloc, &validation_reader, opts);

    var raw = backup_codec.SliceReader.init(data);
    var reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&raw, opts.bundle_base);
    defer reader.deinit(alloc);
    const imported_identity = try importPortablePrimaryBlocks(alloc, store, &reader);

    try finishPortableIdentityImport(alloc, store, opts, imported_identity);
    if (opts.import_derived_indexes) {
        var derived_raw = backup_codec.SliceReader.init(data);
        var derived_reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&derived_raw, opts.bundle_base);
        defer derived_reader.deinit(alloc);
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
    // Restore admission must never wait behind an archive writer while the
    // caller may already hold a destination-wide restore lock.
    if (!try file.tryLock(io, .shared)) return error.WriterLocked;
    defer file.unlock(io);
    const initial_stat = try file.stat(io);
    if (initial_stat.size != file_size) return error.SourceFileChanged;

    if (opts.unpublished_staging) {
        var raw = backup_codec.FileReader.init(io, file, file_size);
        var reader = try PortableArchiveReader(backup_codec.FileReader).initWithBase(&raw, opts.bundle_base);
        defer reader.deinit(alloc);
        const imported_identity = try validateAndImportPortableStagingReader(alloc, store, &reader, opts);
        try finishPortableIdentityImport(alloc, store, opts, imported_identity);
        const final_stat = try file.stat(io);
        if (final_stat.size != initial_stat.size or
            !std.meta.eql(final_stat.mtime, initial_stat.mtime) or
            !std.meta.eql(final_stat.ctime, initial_stat.ctime))
            return error.SourceFileChanged;
        return;
    }

    var validation_raw = backup_codec.FileReader.init(io, file, file_size);
    var validation_reader = try PortableArchiveReader(backup_codec.FileReader).initWithBase(&validation_raw, opts.bundle_base);
    defer validation_reader.deinit(alloc);
    try validatePortableImportReader(alloc, &validation_reader, opts);
    const validated_fingerprint = try validation_reader.verifiedFingerprint();

    var raw = backup_codec.FileReader.init(io, file, file_size);
    var reader = try PortableArchiveReader(backup_codec.FileReader).initWithBase(&raw, opts.bundle_base);
    defer reader.deinit(alloc);
    const imported_identity = try importPortablePrimaryBlocks(alloc, store, &reader);
    try reader.verifyFingerprint(validated_fingerprint);
    try finishPortableIdentityImport(alloc, store, opts, imported_identity);
    if (opts.import_derived_indexes) {
        var derived_raw = backup_codec.FileReader.init(io, file, file_size);
        var derived_reader = try PortableArchiveReader(backup_codec.FileReader).initWithBase(&derived_raw, opts.bundle_base);
        defer derived_reader.deinit(alloc);
        try importPortableDerivedBlocks(alloc, store, &derived_reader, opts);
        try derived_reader.verifyFingerprint(validated_fingerprint);
    }
    const final_stat = try file.stat(io);
    if (final_stat.size != initial_stat.size or
        !std.meta.eql(final_stat.mtime, initial_stat.mtime) or
        !std.meta.eql(final_stat.ctime, initial_stat.ctime))
        return error.SourceFileChanged;
}

/// AFB2 restore path for a disposable destination. Framing, manifest digests,
/// semantic validation, canonical row validation, and store application happen
/// block-by-block without retaining or rereading the archive. Derived objects
/// are safe to apply inline because the exporter orders metadata and base rows
/// before them and the generation remains unpublished until completion.
fn validateAndImportPortableStagingReader(
    alloc: Allocator,
    store: *DocStore,
    reader: anytype,
    opts: ImportOptions,
) !bool {
    const header = try reader.readHeader();
    if (header.format_version < backup_codec.format_version) {
        // Preserve AFB1 compatibility; its metadata ordering was unspecified.
        try reader.reset(alloc);
        try validatePortableImportReader(alloc, reader, opts);
        try reader.reset(alloc);
        const imported_identity = try importPortablePrimaryBlocks(alloc, store, reader);
        if (opts.import_derived_indexes) {
            try reader.reset(alloc);
            try importPortableDerivedBlocks(alloc, store, reader, opts);
        }
        return imported_identity;
    }

    var archive: PortableArchiveValidation = .{
        .format_version = header.format_version,
        .schema_cache_bytes = opts.schema_cache_bytes,
        .staging_store = store,
        .snapshot_alloc = alloc,
    };
    defer archive.deinit(alloc);
    var imported_identity = false;
    var block_index: usize = 0;
    var saw_bundle_manifest = false;
    var payload_bytes: u64 = 0;
    const restore_started_ns = platform_time.monotonicNs();
    while (reader.hasRemaining()) {
        if (opts.is_cancelled_fn) |is_cancelled| if (is_cancelled(opts.cancel_context))
            return error.Canceled;
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block_index == 0 and block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
        if (block.block_type == .bundle_manifest) {
            if (saw_bundle_manifest or block_index != 0) return error.InvalidBackupManifest;
            var manifest = try backup_bundle.parseManifest(alloc, block.payload);
            defer manifest.deinit();
            if (manifest.value.representation != .portable) return error.BackupArtifactFormatMismatch;
            saw_bundle_manifest = true;
        }
        if (block.block_type == .metadata_batch and archive.saw_document_block)
            return error.InvalidBackupManifest;
        switch (block.block_type) {
            .document_batch => {
                try validateAndImportDocumentBatchPayload(alloc, store, block.payload, &archive);
                archive.saw_document_block = true;
            },
            .doc_identity_batch => {
                // The importer validates the identity namespace while applying
                // the same decoded entries to unpublished staging.
                try importIdentityBatch(alloc, store, block.payload);
                imported_identity = true;
            },
            .metadata_batch => try validateAndImportMetadataBatchPayload(alloc, store, block.payload, &archive),
            .chunk_batch => if (opts.import_derived_indexes) try importChunkBatch(alloc, store, block.payload),
            .artifact_batch => if (opts.import_derived_indexes) try importPublicArtifactBatch(alloc, store, block.payload, .asset),
            .resolution_batch => if (opts.import_derived_indexes) try importResolutionArtifactBatch(alloc, store, block.payload),
            .embedding_batch => if (opts.import_derived_indexes) try importEmbeddingBatch(alloc, store, block.payload, opts.embedding_source_fields),
            .sparse_batch => if (opts.import_derived_indexes) try importSparseBatch(alloc, store, block.payload, opts.embedding_source_fields),
            .edge_batch => if (opts.import_derived_indexes) try importEdgeBatch(alloc, store, block.payload),
            .shard_header, .shard_footer, .file_footer => try validatePortableImportBlockPayload(alloc, block.block_type, block.payload, opts, &archive),
            .bundle_manifest, .cluster_manifest, .table_manifest, .summary_batch, .transaction_batch => {},
            else => {},
        }
        block_index += 1;
        payload_bytes +|= block.payload.len;
        if (opts.progress_fn) |progress| {
            const elapsed_ns = platform_time.monotonicNs() - restore_started_ns;
            const scaled_rows = std.math.mul(u64, archive.document_row_count, std.time.ns_per_s) catch std.math.maxInt(u64);
            progress(opts.progress_context, .{
                .blocks_processed = block_index,
                .rows_validated = archive.document_row_count,
                .payload_bytes_processed = payload_bytes,
                .elapsed_ns = elapsed_ns,
                .rows_per_second = if (elapsed_ns == 0) 0 else scaled_rows / elapsed_ns,
            });
        }
    }
    if (!saw_bundle_manifest) return error.InvalidBackupManifest;
    try archive.finish(alloc);
    return imported_identity;
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
    var raw = backup_codec.SliceReader.init(data);
    var reader = try PortableArchiveReader(backup_codec.SliceReader).initWithBase(&raw, opts.bundle_base);
    defer reader.deinit(alloc);
    return try validatePortableImportReader(alloc, &reader, opts);
}

fn validatePortableImportReader(alloc: Allocator, reader: anytype, opts: ImportOptions) !void {
    const header = try reader.readHeader();
    var archive: PortableArchiveValidation = .{
        .format_version = header.format_version,
        .schema_cache_bytes = opts.schema_cache_bytes,
    };
    defer archive.deinit(alloc);
    var block_index: usize = 0;
    var saw_bundle_manifest = false;
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (header.format_version == backup_codec.format_version and block_index == 0 and
            block.block_type != .bundle_manifest) return error.InvalidBackupManifest;
        if (block.block_type == .bundle_manifest) {
            if (saw_bundle_manifest or block_index != 0) return error.InvalidBackupManifest;
            var manifest = try backup_bundle.parseManifest(alloc, block.payload);
            defer manifest.deinit();
            if (manifest.value.representation != .portable) return error.BackupArtifactFormatMismatch;
            saw_bundle_manifest = true;
        }
        if (header.format_version == backup_codec.format_version and
            block.block_type == .metadata_batch and archive.saw_document_block)
            return error.InvalidBackupManifest;
        try validatePortableImportBlockPayload(alloc, block.block_type, block.payload, opts, &archive);
        if (block.block_type == .document_batch) archive.saw_document_block = true;
        block_index += 1;
    }
    if (header.format_version == backup_codec.format_version and !saw_bundle_manifest)
        return error.InvalidBackupManifest;
    try archive.finish(alloc);
}

const ArchiveSchemaLayout = struct {
    schema: storage_schema.TableSchema,
    physical: relational_row_codec.PhysicalLayout,

    fn initOwned(alloc: Allocator, schema: storage_schema.TableSchema) !ArchiveSchemaLayout {
        return .{ .schema = schema, .physical = try relational_row_codec.PhysicalLayout.init(alloc, schema) };
    }

    fn deinit(self: *ArchiveSchemaLayout, alloc: Allocator) void {
        self.physical.deinit();
        storage_schema.freeSchema(alloc, self.schema);
        self.* = undefined;
    }
};

const PortableArchiveValidation = struct {
    format_version: u32,
    schema_cache_bytes: usize = default_schema_cache_bytes,
    spool: ?SchemaSpool = null,
    snapshot: ?*DocStore.Txn = null,
    staging_store: ?*DocStore = null,
    snapshot_alloc: ?Allocator = null,
    decoded: std.AutoHashMapUnmanaged(u32, DecodedEpoch) = .empty,
    transient_epoch: ?DecodedEpoch = null,
    admission: @import("db/schema_cache_admission.zig").Admission = .{},
    decoded_bytes: usize = 0,
    decode_clock: u64 = 0,
    saw_document_rows: bool = false,
    saw_relational_rows: bool = false,
    saw_document_block: bool = false,
    document_row_count: u64 = 0,
    runtime_schema: ?storage_schema.TableSchema = null,
    runtime_physical_layout: ?relational_row_codec.PhysicalLayout = null,
    layouts: std.AutoHashMapUnmanaged(u32, SchemaSpool.Ref) = .empty,
    active_public_validator: ?public_table_schema.CompiledTableValidator = null,
    active_public_digest: ?[std.crypto.hash.Blake3.digest_length]u8 = null,
    public_validators: std.AutoHashMapUnmanaged(u32, SchemaSpool.Ref) = .empty,
    public_digests: std.AutoHashMapUnmanaged(u32, [std.crypto.hash.Blake3.digest_length]u8) = .empty,
    table_catalog: ?table_catalog.Catalog = null,
    saw_index_catalog: bool = false,
    saw_enrichment_catalog: bool = false,
    saw_resolver_catalog: bool = false,

    const DecodedEpoch = struct {
        version: u32,
        arena: *std.heap.ArenaAllocator,
        layout: ArchiveSchemaLayout,
        validator: ?public_table_schema.CompiledTableValidator,
        access: u64,

        fn deinit(self: *DecodedEpoch, alloc: Allocator) void {
            self.arena.deinit();
            alloc.destroy(self.arena);
        }
    };

    fn retainSchema(self: *PortableArchiveValidation, alloc: Allocator, bytes: []const u8) !SchemaSpool.Ref {
        // Metadata can arrive in any order within the manifest section. Drop
        // compiled entries so a newly arrived public validator is never missed.
        self.clearDecoded(alloc);
        // In AFB2 staging, metadata is durably applied before any row. Reuse
        // those records instead of duplicating history into a scratch file.
        if (self.staging_store != null) return .{ .offset = 0, .len = 0 };
        if (self.spool == null) self.spool = .{ .alloc = alloc };
        return try self.spool.?.append(bytes);
    }

    fn clearDecoded(self: *PortableArchiveValidation, alloc: Allocator) void {
        if (self.transient_epoch) |*epoch| epoch.deinit(alloc);
        self.transient_epoch = null;
        var it = self.decoded.valueIterator();
        while (it.next()) |epoch| epoch.deinit(alloc);
        self.decoded.clearRetainingCapacity();
        self.decoded_bytes = 0;
    }

    fn epochForVersion(self: *PortableArchiveValidation, version: u32) !?*DecodedEpoch {
        const alloc = if (self.spool) |spool| spool.alloc else self.snapshot_alloc orelse return null;
        if (self.transient_epoch) |*epoch| if (epoch.version != version) {
            epoch.deinit(alloc);
            self.transient_epoch = null;
        };
        self.admission.record(version);
        self.decode_clock +%= 1;
        if (self.decoded.getPtr(version)) |epoch| {
            epoch.access = self.decode_clock;
            return epoch;
        }
        if (self.transient_epoch) |*epoch| if (epoch.version == version) return epoch;
        const ref = self.layouts.get(version) orelse return null;
        const arena = try alloc.create(std.heap.ArenaAllocator);
        errdefer alloc.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const arena_alloc = arena.allocator();
        const encoded = if (self.snapshot) |snapshot| blk: {
            const key = try storage_schema.schemaVersionKeyAlloc(alloc, version);
            defer alloc.free(key);
            const value = snapshot.get(key) catch |err| switch (err) {
                error.NotFound => return error.InvalidSchema,
                else => return err,
            };
            break :blk try alloc.dupe(u8, value);
        } else if (self.staging_store) |store| blk: {
            const key = try storage_schema.schemaVersionKeyAlloc(alloc, version);
            defer alloc.free(key);
            break :blk try store.get(alloc, key);
        } else try self.spool.?.read(alloc, ref);
        defer alloc.free(encoded);
        const schema = try storage_schema.deserializeSchema(arena_alloc, encoded);
        if (schema.version != version) return error.InvalidSchema;
        const layout = try ArchiveSchemaLayout.initOwned(arena_alloc, schema);
        var validator: ?public_table_schema.CompiledTableValidator = null;
        if (self.public_validators.get(version)) |public_ref| {
            const json = if (self.staging_store) |store| blk: {
                const key = try public_table_schema.versionedSchemaKeyAlloc(alloc, version);
                defer alloc.free(key);
                break :blk try store.get(alloc, key);
            } else try self.spool.?.read(alloc, public_ref);
            defer alloc.free(json);
            validator = try public_table_schema.CompiledTableValidator.init(arena_alloc, json);
        }
        const cost = arena.queryCapacity();
        const incoming: DecodedEpoch = .{ .version = version, .arena = arena, .layout = layout, .validator = validator, .access = self.decode_clock };
        if (cost > self.schema_cache_bytes) {
            // Oversized epochs are borrowed working state, never residents.
            // Do not flush a useful cache to serve one unusually wide schema.
            self.transient_epoch = incoming;
            return &self.transient_epoch.?;
        }
        while (self.decoded.count() > 0 and self.decoded_bytes +| cost > self.schema_cache_bytes) {
            var oldest: ?u32 = null;
            var oldest_access: u64 = std.math.maxInt(u64);
            var it = self.decoded.iterator();
            while (it.next()) |entry| {
                const frequency = self.admission.frequency(entry.key_ptr.*);
                const candidate_frequency = if (oldest) |candidate| self.admission.frequency(candidate) else 255;
                if (oldest == null or frequency < candidate_frequency or
                    (frequency == candidate_frequency and entry.value_ptr.access < oldest_access))
                {
                    oldest = entry.key_ptr.*;
                    oldest_access = entry.value_ptr.access;
                }
            }
            if (!self.admission.admits(version, oldest.?)) {
                if (self.transient_epoch) |*previous| previous.deinit(alloc);
                self.transient_epoch = incoming;
                return &self.transient_epoch.?;
            }
            var evicted = self.decoded.fetchRemove(oldest.?).?.value;
            self.decoded_bytes -= evicted.arena.queryCapacity();
            evicted.deinit(alloc);
        }
        try self.decoded.put(alloc, version, incoming);
        self.decoded_bytes += cost;
        return self.decoded.getPtr(version).?;
    }

    fn deinit(self: *PortableArchiveValidation, alloc: Allocator) void {
        if (self.runtime_physical_layout) |*layout| layout.deinit();
        if (self.runtime_schema) |schema| storage_schema.freeSchema(alloc, schema);
        self.clearDecoded(alloc);
        self.decoded.deinit(alloc);
        if (self.spool) |*spool| spool.deinit();
        self.layouts.deinit(alloc);
        if (self.active_public_validator) |*validator| validator.deinit(alloc);
        self.public_validators.deinit(alloc);
        self.public_digests.deinit(alloc);
        self.* = undefined;
    }

    fn validatorForVersion(
        self: *PortableArchiveValidation,
        version: ?u32,
    ) !?*const public_table_schema.CompiledTableValidator {
        if (version) |schema_version| {
            if (try self.epochForVersion(schema_version)) |epoch|
                if (epoch.validator) |*validator| return validator;
            if (self.active_public_validator) |*validator|
                if (validator.schema.version == schema_version) return validator;
            return null;
        }
        return if (self.active_public_validator) |*validator| validator else null;
    }

    fn layoutForVersion(self: *PortableArchiveValidation, version: u32) !?struct {
        schema: *const storage_schema.TableSchema,
        physical: *const relational_row_codec.PhysicalLayout,
    } {
        if (try self.epochForVersion(version)) |epoch| return .{
            .schema = &epoch.layout.schema,
            .physical = &epoch.layout.physical,
        };
        if (self.runtime_schema) |*schema| if (schema.version == version) {
            const physical = if (self.runtime_physical_layout) |*layout| layout else return null;
            return .{ .schema = schema, .physical = physical };
        };
        return null;
    }

    fn finish(self: *PortableArchiveValidation, alloc: Allocator) !void {
        if (self.saw_document_rows and self.saw_relational_rows) return error.InvalidBackupRequest;
        if (self.saw_relational_rows and self.format_version < 2) return error.InvalidBackupRequest;

        const runtime_relational = if (self.runtime_schema) |schema| schema.storage_mode == .relational else false;
        const public_relational = if (self.active_public_validator) |validator| validator.schema.storage_mode == .relational else false;
        if (self.active_public_validator != null and self.runtime_schema != null and runtime_relational != public_relational)
            return error.InvalidBackupRequest;
        if (self.active_public_validator != null and self.runtime_schema == null) return error.InvalidBackupRequest;
        if (self.table_catalog) |catalog| {
            if (catalog.reconciled and (catalog.row_count != 0) != (self.document_row_count != 0))
                return error.InvalidBackupRequest;
            if (self.runtime_schema) |runtime_schema| {
                if (!catalog.mode_initialized or catalog.storage_mode != runtime_schema.storage_mode or
                    catalog.active_schema_version != runtime_schema.version)
                    return error.InvalidBackupRequest;
            }
        }
        if (self.active_public_validator) |validator| {
            if (self.runtime_schema) |runtime_schema| {
                const derived = public_table_schema.deriveRuntimeTableSchema(alloc, validator.schema) catch |err| switch (err) {
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
            if (self.public_digests.get(validator.schema.version)) |versioned_digest| {
                if (!std.mem.eql(u8, &versioned_digest, &self.active_public_digest.?))
                    return error.InvalidBackupRequest;
            }
        }
        var versioned_validators = self.public_validators.iterator();
        while (versioned_validators.next()) |entry| {
            const layout = (try self.layoutForVersion(entry.key_ptr.*)) orelse return error.InvalidBackupRequest;
            const validator = (try self.validatorForVersion(entry.key_ptr.*)) orelse return error.InvalidBackupRequest;
            const derived = public_table_schema.deriveRuntimeTableSchema(alloc, validator.schema) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            defer storage_schema.freeSchema(alloc, derived);
            const schemas_match = storage_schema.schemasEqual(alloc, layout.schema.*, derived) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            if (!schemas_match) return error.InvalidBackupRequest;
        }
        if (self.saw_relational_rows and !runtime_relational) return error.InvalidBackupRequest;
        if (self.saw_document_rows and runtime_relational) return error.InvalidBackupRequest;
    }
};

test "portable archive accepts long history with a bounded decoded working set" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var source = try openTestStore(alloc, &source_tmp);
    defer source.close();
    var archive: PortableArchiveValidation = .{
        .format_version = backup_codec.format_version,
        .schema_cache_bytes = 1024,
    };
    defer archive.deinit(alloc);
    for (0..4100) |version| {
        const encoded = try storage_schema.serializeSchema(alloc, .{ .version = @intCast(version) });
        defer alloc.free(encoded);
        const key = try storage_schema.schemaVersionKeyAlloc(alloc, @intCast(version));
        defer alloc.free(key);
        try validateMetadataEntries(alloc, &.{.{ .key = key, .value = encoded }}, &archive);
        try source.put(key, encoded);
    }
    try std.testing.expectEqual(@as(usize, 4100), archive.layouts.count());
    for (0..4100) |version| {
        const layout = (try archive.layoutForVersion(@intCast(version))).?;
        try std.testing.expectEqual(@as(u32, @intCast(version)), layout.schema.version);
        try std.testing.expect(archive.decoded_bytes <= archive.schema_cache_bytes);
    }
    // Fault a previously evicted epoch and keep duplicate detection independent
    // of residency.
    try std.testing.expectEqual(@as(u32, 0), (try archive.layoutForVersion(0)).?.schema.version);
    const encoded = try storage_schema.serializeSchema(alloc, .{ .version = 0 });
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidMetadataBatch, validateMetadataEntries(alloc, &.{.{
        .key = "\x00\x00__metadata__:schema_v0",
        .value = encoded,
    }}, &archive));

    // Exercise the public exporter and both import modes, not just the cache.
    const active = try storage_schema.serializeSchema(alloc, .{ .version = 4099 });
    defer alloc.free(active);
    try source.put("\x00\x00__metadata__:schema", active);
    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try exportPortable(alloc, &source, &portable);
    for ([_]bool{ false, true }) |staged| {
        var destination_tmp = std.testing.tmpDir(.{});
        defer destination_tmp.cleanup();
        var destination = try openTestStore(alloc, &destination_tmp);
        defer destination.close();
        try importPortableWithOptions(alloc, &destination, portable.items, .{
            .unpublished_staging = staged,
            .import_derived_indexes = false,
            .schema_cache_bytes = 1024,
        });
        const restored = try storage_schema.loadSchemaVersion(&destination, alloc, 0);
        defer if (restored) |schema| storage_schema.freeSchema(alloc, schema);
        try std.testing.expectEqual(@as(u32, 0), restored.?.version);
    }
}

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
        .bundle_manifest, .cluster_manifest, .table_manifest, .summary_batch, .transaction_batch => {},
        else => {},
    }
}

fn validateDocumentBatchPayload(alloc: Allocator, payload: []const u8, archive: *PortableArchiveValidation) !void {
    const entries = try backup_codec.decodeDocumentBatchBorrowed(alloc, payload);
    defer alloc.free(entries);
    var validation_arena = std.heap.ArenaAllocator.init(alloc);
    defer validation_arena.deinit();
    for (entries) |entry| {
        try validatePortableDocumentEntryAgainstArchive(validation_arena.allocator(), entry, archive);
        _ = validation_arena.reset(.retain_capacity);
        archive.document_row_count = std.math.add(u64, archive.document_row_count, 1) catch return error.InvalidBackupRequest;
        if (entry.value_flags & backup_codec.doc_value_flag_relational_row != 0)
            archive.saw_relational_rows = true
        else
            archive.saw_document_rows = true;
    }
}

fn validateAndImportDocumentBatchPayload(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
    archive: *PortableArchiveValidation,
) !void {
    const entries = try backup_codec.decodeDocumentBatchBorrowed(alloc, payload);
    defer alloc.free(entries);
    var validation_arena = std.heap.ArenaAllocator.init(alloc);
    defer validation_arena.deinit();
    for (entries) |entry| {
        try validatePortableDocumentEntryAgainstArchive(validation_arena.allocator(), entry, archive);
        _ = validation_arena.reset(.retain_capacity);
        archive.document_row_count = std.math.add(u64, archive.document_row_count, 1) catch
            return error.InvalidBackupRequest;
        if (entry.value_flags & backup_codec.doc_value_flag_relational_row != 0)
            archive.saw_relational_rows = true
        else
            archive.saw_document_rows = true;
    }
    try importDecodedDocumentEntries(alloc, store, entries);
}

fn validatePortableDocumentEntryAgainstArchive(
    alloc: Allocator,
    entry: backup_codec.DocumentEntry,
    archive: *PortableArchiveValidation,
) !void {
    try validatePortableDocumentEntry(entry);
    if (entry.value_flags & backup_codec.doc_value_flag_relational_row == 0) return;
    const row_version = relational_store.rowSchemaVersion(entry.value) catch return error.InvalidBackupRequest;
    const layout = (try archive.layoutForVersion(row_version)) orelse return error.InvalidBackupRequest;
    if (try archive.validatorForVersion(row_version)) |validator| {
        if (!validator.restore.full_root) {
            // Canonical decoding discharges required/null/type constraints and
            // authenticates the logical hash. Archive finish verifies the
            // public/runtime schema binding before staging can be published.
            relational_store.validateCanonicalValueForSchemaAndLayout(alloc, entry.value, layout.schema.*, layout.physical) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            const row = try relational_row_codec.ordinalRowViewTrusted(entry.value, layout.schema.*, layout.physical);
            validator.validateRelationalRestoreFields(alloc, row) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidBackupRequest,
            };
            return;
        }
        var logical = relational_store.validateCanonicalAndMaterializeRootForSchemaAndLayoutAlloc(
            alloc,
            entry.value,
            layout.schema.*,
            layout.physical,
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBackupRequest,
        };
        defer logical.deinit(alloc);
        validator.validateValue(alloc, &logical.root) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBackupRequest,
        };
    } else if (archive.active_public_validator != null) {
        return error.InvalidBackupRequest;
    } else {
        relational_store.validateCanonicalValueForSchemaAndLayout(
            alloc,
            entry.value,
            layout.schema.*,
            layout.physical,
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBackupRequest,
        };
    }
}

/// Context-free envelope validation used again at the mutation boundary after
/// the archive-wide, schema-aware preflight has succeeded.
fn validatePortableDocumentEntry(entry: backup_codec.DocumentEntry) !void {
    if (entry.value_flags & ~backup_codec.doc_value_known_flags != 0) return error.InvalidBackupRequest;
    if (entry.value_flags & backup_codec.doc_value_flag_relational_row == 0) return;
    if (entry.value_flags & backup_codec.doc_value_flag_compressed != 0) return error.InvalidBackupRequest;
    _ = relational_store.rowSchemaVersion(entry.value) catch return error.InvalidBackupRequest;
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
    return try validateMetadataEntries(alloc, entries, archive);
}

fn validateMetadataEntries(
    alloc: Allocator,
    entries: []const backup_codec.KeyValueEntry,
    archive: *PortableArchiveValidation,
) !void {
    for (entries) |entry| {
        if (!isPortableMetadataKey(entry.key)) return error.InvalidMetadataBatch;
        if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema")) {
            if (archive.runtime_schema != null) return error.InvalidMetadataBatch;
            const schema = storage_schema.deserializeSchema(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            var schema_owned = true;
            errdefer if (schema_owned) storage_schema.freeSchema(alloc, schema);
            archive.runtime_physical_layout = relational_row_codec.PhysicalLayout.init(alloc, schema) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            archive.runtime_schema = schema;
            schema_owned = false;
        } else if (std.mem.startsWith(u8, entry.key, schema_version_prefix)) {
            const schema = storage_schema.deserializeSchema(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            const suffix = entry.key[schema_version_prefix.len..];
            const key_version = std.fmt.parseInt(u32, suffix, 10) catch {
                storage_schema.freeSchema(alloc, schema);
                return error.InvalidMetadataBatch;
            };
            if (key_version != schema.version) {
                storage_schema.freeSchema(alloc, schema);
                return error.InvalidMetadataBatch;
            }
            defer storage_schema.freeSchema(alloc, schema);
            const expected_key = try storage_schema.schemaVersionKeyAlloc(alloc, schema.version);
            defer alloc.free(expected_key);
            if (!std.mem.eql(u8, entry.key, expected_key)) return error.InvalidMetadataBatch;
            // Validate even layouts with no rows, but do not retain their
            // decoded allocation graph for the lifetime of the archive.
            var physical = try relational_row_codec.PhysicalLayout.init(alloc, schema);
            defer physical.deinit();
            if (archive.layouts.contains(schema.version)) return error.InvalidMetadataBatch;
            const ref = try archive.retainSchema(alloc, entry.value);
            try archive.layouts.putNoClobber(alloc, schema.version, ref);
        } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema_json")) {
            if (archive.active_public_validator != null) return error.InvalidMetadataBatch;
            archive.active_public_validator = public_table_schema.CompiledTableValidator.init(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
            std.crypto.hash.Blake3.hash(entry.value, &digest, .{});
            archive.active_public_digest = digest;
        } else if (std.mem.startsWith(u8, entry.key, public_table_schema.versioned_schema_key_prefix)) {
            const suffix = entry.key[public_table_schema.versioned_schema_key_prefix.len..];
            const key_version = std.fmt.parseInt(u32, suffix, 10) catch return error.InvalidMetadataBatch;
            const expected_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, key_version);
            defer alloc.free(expected_key);
            if (!std.mem.eql(u8, entry.key, expected_key)) return error.InvalidMetadataBatch;
            var validator = public_table_schema.CompiledTableValidator.init(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            if (validator.schema.version != key_version) {
                validator.deinit(alloc);
                return error.InvalidMetadataBatch;
            }
            defer validator.deinit(alloc);
            if (archive.public_validators.contains(key_version)) return error.InvalidMetadataBatch;
            const ref = try archive.retainSchema(alloc, entry.value);
            try archive.public_validators.putNoClobber(alloc, key_version, ref);
            var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
            std.crypto.hash.Blake3.hash(entry.value, &digest, .{});
            try archive.public_digests.putNoClobber(alloc, key_version, digest);
        } else if (std.mem.eql(u8, entry.key, table_catalog.key)) {
            if (archive.table_catalog != null) return error.InvalidMetadataBatch;
            archive.table_catalog = table_catalog.Catalog.decode(entry.value) catch return error.InvalidMetadataBatch;
        } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:indexes")) {
            if (archive.saw_index_catalog) return error.InvalidMetadataBatch;
            index_manager.validateSerializedCatalog(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            archive.saw_index_catalog = true;
        } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:enrichments")) {
            if (archive.saw_enrichment_catalog) return error.InvalidMetadataBatch;
            const configs = enrichment_catalog.deserializeCatalog(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            for (configs) |*config| config.deinit(alloc);
            alloc.free(configs);
            archive.saw_enrichment_catalog = true;
        } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:resolvers")) {
            if (archive.saw_resolver_catalog) return error.InvalidMetadataBatch;
            const configs = resolver_catalog.deserializeCatalog(alloc, entry.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMetadataBatch,
            };
            for (configs) |*config| config.deinit(alloc);
            alloc.free(configs);
            archive.saw_resolver_catalog = true;
        }
    }
}

fn validateAndImportMetadataBatchPayload(
    alloc: Allocator,
    store: *DocStore,
    payload: []const u8,
    archive: *PortableArchiveValidation,
) !void {
    const entries = try backup_codec.decodeKeyValueBatch(alloc, payload);
    defer freeKeyValueEntries(alloc, entries);
    try validateMetadataEntries(alloc, entries, archive);

    const writes = try alloc.alloc(KVPair, entries.len);
    defer alloc.free(writes);
    for (entries, 0..) |entry, i| writes[i] = .{ .key = entry.key, .value = entry.value };
    if (writes.len > 0) try store.putBatch(writes, &.{});
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
    const entries = try backup_codec.decodeDocumentBatchBorrowed(alloc, payload);
    defer alloc.free(entries);

    try importDecodedDocumentEntries(alloc, store, entries);
}

fn importDecodedDocumentEntries(
    alloc: Allocator,
    store: *DocStore,
    entries: []const backup_codec.DocumentEntry,
) !void {
    // Build KV pairs with internal keys. Decoded entry values already remain
    // alive through putBatch, so only decompressed values need another buffer.
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
        const value = if (!relational_row and e.value_flags & backup_codec.doc_value_flag_compressed != 0) blk: {
            try owned_keys.ensureUnusedCapacity(alloc, 1);
            const decompressed = try backup_codec.decompressZstd(alloc, e.value);
            owned_keys.appendAssumeCapacity(decompressed);
            break :blk decompressed;
        } else e.value;
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
        if (header.kind == .graph_edge) {
            var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, value);
            defer decoded.deinit(alloc);
            return try enrichment_artifact_codec.encodePortableUnboundGraphEdgeAlloc(
                alloc,
                decoded.weight,
                decoded.created_at,
                decoded.updated_at,
                decoded.metadata_json,
            );
        }
    } else |_| {}

    if (value.len >= 24) {
        const weight = @as(f64, @bitCast(std.mem.readInt(u64, value[0..][0..8], .little)));
        const created_at = std.mem.readInt(u64, value[8..][0..8], .little);
        const updated_at = std.mem.readInt(u64, value[16..][0..8], .little);
        return try enrichment_artifact_codec.encodePortableUnboundGraphEdgeAlloc(alloc, weight, created_at, updated_at, value[24..]);
    }

    return try enrichment_artifact_codec.encodePortableUnboundGraphEdgeAlloc(alloc, 1.0, 0, 0, value);
}

test "portable graph conversion accepts generation-less v1 edge artifacts" {
    const alloc = std.testing.allocator;
    const legacy =
        "AFENRCH\x00" ++
        "\x01\x00\x05\x00" ++
        "\x00\x00\x00\x00\x00\x00\x00\x00" ++
        "\x23\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\x00\xf8\x3f" ++
        "\x0a\x00\x00\x00\x00\x00\x00\x00" ++
        "\x14\x00\x00\x00\x00\x00\x00\x00" ++
        "\x07\x00\x00\x00{\"k\":1}";

    const portable = try graphArtifactValueFromPortableEdgeValueAlloc(alloc, legacy);
    defer alloc.free(portable);
    try std.testing.expect(enrichment_artifact_codec.isPortableUnboundGraphEdge(portable));
    var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, portable);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), decoded.generation);
    try std.testing.expectEqual(@as(f64, 1.5), decoded.weight);
    try std.testing.expectEqualStrings("{\"k\":1}", decoded.metadata_json);
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

    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
    defer alloc.free(row_key);
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"}},"required":["id","count"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const derived_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, derived_schema);
    const packed_row = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"a\",\"count\":9007199254740993}", derived_schema);
    defer alloc.free(packed_row);
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
        .{ .key = "\x00\x00__metadata__:schema_v1", .value = encoded_schema },
        .{ .key = "\x00\x00__metadata__:schema_json", .value = schema_json },
    }, &.{});

    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try exportPortable(alloc, &src, &portable);
    try validatePortable(alloc, portable.items);

    // The production file path stages encoded logical blocks on bounded disk,
    // avoiding a second store scan while preserving byte-for-byte output.
    const spool_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/portable.spool", .{tmp_src.sub_path});
    defer alloc.free(spool_path);
    var spool_io_impl = std.Io.Threaded.init(alloc, .{});
    defer spool_io_impl.deinit();
    const spool_io = spool_io_impl.io();
    var spool_file = try std.Io.Dir.cwd().createFile(spool_io, spool_path, .{ .read = true, .truncate = true });
    defer spool_file.close(spool_io);
    var spooled: ArrayList(u8) = .empty;
    defer spooled.deinit(alloc);
    var spooled_writer = std.Io.Writer.Allocating.fromArrayList(alloc, &spooled);
    defer spooled = spooled_writer.toArrayList();
    try exportPortableToWriterWithOptions(alloc, &src, &spooled_writer.writer, .{
        .spool = .{ .io = spool_io, .file = spool_file },
    });
    try std.testing.expectEqualSlices(u8, portable.items, spooled_writer.written());

    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    const Progress = struct {
        last: ?ImportProgress = null,

        fn update(ctx: ?*anyopaque, progress: ImportProgress) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.last = progress;
        }
    };
    var progress = Progress{};
    try importPortableWithOptions(alloc, &dst, portable.items, .{
        .unpublished_staging = true,
        .schema_cache_bytes = 1,
        .progress_context = &progress,
        .progress_fn = Progress.update,
    });
    try std.testing.expect(progress.last != null);
    try std.testing.expect(progress.last.?.blocks_processed > 0);
    try std.testing.expectEqual(@as(u64, 1), progress.last.?.rows_validated);
    try std.testing.expect(progress.last.?.payload_bytes_processed > 0);

    const restored_row = try dst.get(alloc, row_key);
    defer alloc.free(restored_row);
    try std.testing.expectEqualSlices(u8, packed_row, restored_row);
    try relational_store.validateValueForSchema(restored_row, derived_schema);
    const restored_json = try relational_store.decodeValueForSchemaAlloc(alloc, restored_row, derived_schema);
    defer alloc.free(restored_json);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"count\":9007199254740993}", restored_json);
    const legacy_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(legacy_key);
    try std.testing.expectError(error.NotFound, dst.get(alloc, legacy_key));

    var tmp_cancelled = std.testing.tmpDir(.{});
    defer tmp_cancelled.cleanup();
    var cancelled_dst = try openTestStore(alloc, &tmp_cancelled);
    defer cancelled_dst.close();
    const Cancellation = struct {
        fn requested(_: ?*anyopaque) bool {
            return true;
        }
    };
    try std.testing.expectError(error.Canceled, importPortableWithOptions(alloc, &cancelled_dst, portable.items, .{
        .unpublished_staging = true,
        .is_cancelled_fn = Cancellation.requested,
    }));
    try std.testing.expectError(error.NotFound, cancelled_dst.get(alloc, row_key));

    const restored_schema = try dst.get(alloc, "\x00\x00__metadata__:schema_json");
    defer alloc.free(restored_schema);
    try std.testing.expectEqualStrings(schema_json, restored_schema);
    const restored_timestamp = try dst.get(alloc, timestamp_key);
    defer alloc.free(restored_timestamp);
    try std.testing.expectEqual(@as(u64, 1234), std.mem.readInt(u64, restored_timestamp[0..8], .little));
}

test "portable restore validates historical rows with their public schema epoch" {
    const alloc = std.testing.allocator;
    const schema_v1_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"note":{"type":"keyword"}},"required":["id","note"],"additionalProperties":false}}}}
    ;
    var parsed_v1 = try public_table_schema.parseValidatedTableSchema(alloc, schema_v1_json);
    defer parsed_v1.deinit(alloc);
    const runtime_v1 = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_v1);
    defer storage_schema.freeSchema(alloc, runtime_v1);
    var parsed_v2 = try public_table_schema.parseValidatedTableSchema(alloc, schema_v2_json);
    defer parsed_v2.deinit(alloc);
    const runtime_v2 = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_v2);
    var runtime_v2_owned = true;
    defer if (runtime_v2_owned) storage_schema.freeSchema(alloc, runtime_v2);

    const packed_v1 = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"old\"}", runtime_v1);
    defer alloc.free(packed_v1);
    var archive: PortableArchiveValidation = .{
        .format_version = 2,
        .runtime_schema = runtime_v2,
        .runtime_physical_layout = try relational_row_codec.PhysicalLayout.init(alloc, runtime_v2),
        .active_public_validator = try public_table_schema.CompiledTableValidator.init(alloc, schema_v2_json),
    };
    runtime_v2_owned = false;
    defer archive.deinit(alloc);
    const encoded_v1 = try storage_schema.serializeSchema(alloc, runtime_v1);
    defer alloc.free(encoded_v1);
    try archive.layouts.put(alloc, 1, try archive.retainSchema(alloc, encoded_v1));

    const entry = backup_codec.DocumentEntry{
        .key = "row:old",
        .value_flags = backup_codec.doc_value_flag_relational_row,
        .value = packed_v1,
        .timestamp_ns = 0,
    };
    try std.testing.expectError(error.InvalidBackupRequest, validatePortableDocumentEntryAgainstArchive(alloc, entry, &archive));
    try archive.public_validators.put(alloc, 1, try archive.retainSchema(alloc, schema_v1_json));
    try validatePortableDocumentEntryAgainstArchive(alloc, entry, &archive);
}

test "relational restore plans avoid scalar materialization and retain residual validation" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"},"active":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var validator = try public_table_schema.CompiledTableValidator.init(alloc, schema_json);
    defer validator.deinit(alloc);
    try std.testing.expect(!validator.restore.full_root);
    try std.testing.expectEqual(@as(usize, 0), validator.restore.properties.len);
    const runtime = try public_table_schema.deriveRuntimeTableSchema(alloc, validator.schema);
    defer storage_schema.freeSchema(alloc, runtime);
    var layout = try relational_row_codec.PhysicalLayout.init(alloc, runtime);
    defer layout.deinit();
    const bytes = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"large scalar strings need no owned copy\",\"count\":7,\"active\":true}", runtime);
    defer alloc.free(bytes);
    try relational_store.validateCanonicalValueForSchemaAndLayout(alloc, bytes, runtime, &layout);
    const row = try relational_row_codec.ordinalRowViewTrusted(bytes, runtime, &layout);
    var no_allocations = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try validator.validateRelationalRestoreFields(no_allocations.allocator(), row);
    try std.testing.expectEqual(@as(usize, 0), no_allocations.alloc_index);

    const residual_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer","minimum":5},"active":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var residual = try public_table_schema.CompiledTableValidator.init(alloc, residual_json);
    defer residual.deinit(alloc);
    try std.testing.expect(!residual.restore.full_root);
    try std.testing.expectEqualSlices(usize, &.{1}, residual.restore.properties);
    try residual.validateRelationalRestoreFields(alloc, row);
    const invalid = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"a\",\"count\":4}", runtime);
    defer alloc.free(invalid);
    try relational_store.validateCanonicalValueForSchemaAndLayout(alloc, invalid, runtime, &layout);
    try std.testing.expectError(error.InvalidBatchRequest, residual.validateRelationalRestoreFields(alloc, try relational_row_codec.ordinalRowViewTrusted(invalid, runtime, &layout)));
}

test "relational restore plans retain root-sensitive validation" {
    const alloc = std.testing.allocator;
    const keywords = [_][]const u8{
        "\"minProperties\":1",
        "\"maxProperties\":1",
        "\"propertyNames\":{\"pattern\":\"^[ab]$\"}",
    };
    for (keywords) |keyword| {
        const json = try std.fmt.allocPrint(alloc,
            \\{{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"a":{{"type":"string"}},"b":{{"type":"string"}}}},"additionalProperties":false,{s}}}}}}}}}
        , .{keyword});
        defer alloc.free(json);
        var validator = try public_table_schema.CompiledTableValidator.init(alloc, json);
        defer validator.deinit(alloc);
        try std.testing.expect(validator.restore.full_root);
    }
}

test "complete relational database image requires its public schema contract" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try openTestStore(alloc, &tmp);
    defer store.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"keyword","enum":["active"]}},"required":["status"],"additionalProperties":false}}}}
    ;
    var parsed = try public_table_schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    const encoded = try storage_schema.serializeSchema(alloc, runtime_schema);
    defer alloc.free(encoded);
    try store.put("\x00\x00__metadata__:schema", encoded);

    try std.testing.expectError(error.InvalidBackupRequest, validateCompleteDatabaseImageAlloc(alloc, &store));
    try store.put("\x00\x00__metadata__:schema_json", schema_json);
    try validateCompleteDatabaseImageAlloc(alloc, &store);
}

test "complete database image rejects a public document schema without runtime schema" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try openTestStore(alloc, &tmp);
    defer store.close();

    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}}}
    ;
    try store.put("\x00\x00__metadata__:schema_json", schema_json);
    try std.testing.expectError(error.InvalidBackupRequest, validateCompleteDatabaseImageAlloc(alloc, &store));

    var parsed = try public_table_schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    const encoded = try storage_schema.serializeSchema(alloc, runtime_schema);
    defer alloc.free(encoded);
    try store.put("\x00\x00__metadata__:schema", encoded);
    try validateCompleteDatabaseImageAlloc(alloc, &store);
}

test "portable archive validation rejects any public schema without its runtime companion" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}}}
    ;
    const public_validator = try public_table_schema.CompiledTableValidator.init(alloc, schema_json);
    var validation: PortableArchiveValidation = .{
        .format_version = 2,
        .active_public_validator = public_validator,
    };
    defer validation.deinit(alloc);
    try std.testing.expectError(error.InvalidBackupRequest, validation.finish(alloc));
}

test "portable backup rejects ambiguous or corrupt relational document entries" {
    const alloc = std.testing.allocator;
    const columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
    };
    const table_schema: storage_schema.TableSchema = .{
        .version = 1,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    const packed_row = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"a\"}", table_schema);
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
    const public_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","enum":["active"]}},"required":["id","status"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, public_schema);
    defer parsed_schema.deinit(alloc);
    const derived_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, derived_schema);
    const runtime_schema = try storage_schema.serializeSchema(alloc, derived_schema);
    defer alloc.free(runtime_schema);
    const invalid_row = try relational_store.encodeValueForSchemaAlloc(alloc, "{\"id\":\"a\",\"status\":\"inactive\"}", derived_schema);
    defer alloc.free(invalid_row);
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try openTestStore(alloc, &tmp);
        defer store.close();
        const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
        defer alloc.free(row_key);
        const runtime_version_key = try storage_schema.schemaVersionKeyAlloc(alloc, derived_schema.version);
        defer alloc.free(runtime_version_key);
        const public_version_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, derived_schema.version);
        defer alloc.free(public_version_key);
        try store.putBatch(&.{
            .{ .key = row_key, .value = invalid_row },
            .{ .key = "\x00\x00__metadata__:schema", .value = runtime_schema },
            .{ .key = runtime_version_key, .value = runtime_schema },
            .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
            .{ .key = public_version_key, .value = public_schema },
        }, &.{});
        var archive: ArrayList(u8) = .empty;
        defer archive.deinit(alloc);
        try exportPortable(alloc, &store, &archive);
        try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));
    }

    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try openTestStore(alloc, &tmp);
        defer store.close();
        const document_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
        defer alloc.free(document_key);
        try store.putBatch(&.{
            .{ .key = document_key, .value = "{\"id\":\"a\",\"status\":\"active\"}" },
            .{ .key = "\x00\x00__metadata__:schema", .value = runtime_schema },
            .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
        }, &.{});
        var archive: ArrayList(u8) = .empty;
        defer archive.deinit(alloc);
        try exportPortable(alloc, &store, &archive);
        try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));
    }

    var mismatched_schema = derived_schema;
    mismatched_schema.version += 1;
    const mismatched_runtime = try storage_schema.serializeSchema(alloc, mismatched_schema);
    defer alloc.free(mismatched_runtime);
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try openTestStore(alloc, &tmp);
        defer store.close();
        try store.putBatch(&.{
            .{ .key = "\x00\x00__metadata__:schema", .value = mismatched_runtime },
            .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
        }, &.{});
        var archive: ArrayList(u8) = .empty;
        defer archive.deinit(alloc);
        try exportPortable(alloc, &store, &archive);
        try std.testing.expectError(error.InvalidBackupRequest, validatePortable(alloc, archive.items));
    }
}

const PortableBatchInspection = struct {
    alloc: Allocator,
    expected_type: backup_codec.BlockType,
    expected_prefix: []const u8,
    observed_count: usize = 0,

    fn visit(self: *@This(), block_type: backup_codec.BlockType, payload: []const u8) !void {
        if (block_type != self.expected_type) return;
        const entries = try backup_codec.decodeKeyValueBatch(self.alloc, payload);
        defer freeKeyValueEntries(self.alloc, entries);
        self.observed_count += entries.len;
        for (entries) |entry| {
            try std.testing.expect(std.mem.startsWith(u8, entry.key, self.expected_prefix));
        }
    }
};

const PortableTimestampInspection = struct {
    alloc: Allocator,
    expected_key: []const u8,
    expected_timestamp_ns: u64,
    found: bool = false,

    fn visit(self: *@This(), block_type: backup_codec.BlockType, payload: []const u8) !void {
        if (block_type != .document_batch) return;
        const entries = try backup_codec.decodeDocumentBatch(self.alloc, payload);
        defer {
            for (entries) |entry| {
                self.alloc.free(entry.key);
                self.alloc.free(entry.value);
            }
            self.alloc.free(entries);
        }
        for (entries) |entry| {
            if (!std.mem.eql(u8, entry.key, self.expected_key)) continue;
            try std.testing.expectEqual(self.expected_timestamp_ns, entry.timestamp_ns);
            self.found = true;
        }
    }
};

const TestPortableBaseContext = struct {
    data: []const u8,
};

fn readTestPortableBaseBlob(
    context_ptr: *anyopaque,
    alloc: Allocator,
    sha256: []const u8,
    logical_size_bytes: u64,
    limit: usize,
) ![]u8 {
    if (logical_size_bytes > limit) return error.BackupBlockTooLarge;
    const context: *TestPortableBaseContext = @ptrCast(@alignCast(context_ptr));
    var raw = backup_codec.SliceReader.init(context.data);
    var reader = try PortableArchiveReader(backup_codec.SliceReader).init(&raw);
    defer reader.deinit(alloc);
    _ = try reader.readHeader();
    while (reader.hasRemaining()) {
        const block = try reader.readBlock(alloc);
        if (block.block_type == .bundle_manifest) {
            alloc.free(block.payload);
            continue;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(block.payload, &digest, .{});
        const digest_hex = std.fmt.bytesToHex(digest, .lower);
        if (std.mem.eql(u8, &digest_hex, sha256)) return @constCast(block.payload);
        alloc.free(block.payload);
    }
    return error.BackupBlobMissing;
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

test "portable AFB2 delta resolves exact base and deduplicates physical blobs" {
    const alloc = std.testing.allocator;
    var tmp_src = std.testing.tmpDir(.{});
    defer tmp_src.cleanup();
    var src = try openTestStore(alloc, &tmp_src);
    defer src.close();

    const first_key = try internal_keys.documentKeyAlloc(alloc, "doc1");
    defer alloc.free(first_key);
    try src.putBatch(&.{.{ .key = first_key, .value = "{\"id\":\"doc1\"}" }}, &.{});
    var base_archive: ArrayList(u8) = .empty;
    defer base_archive.deinit(alloc);
    try exportPortable(alloc, &src, &base_archive);

    var base_raw = backup_codec.SliceReader.init(base_archive.items);
    _ = try base_raw.readHeader();
    const manifest_block = try base_raw.readBlock(alloc);
    defer alloc.free(manifest_block.payload);
    try std.testing.expectEqual(backup_codec.BlockType.bundle_manifest, manifest_block.block_type);
    var base_manifest = try backup_bundle.parseManifest(alloc, manifest_block.payload);
    defer base_manifest.deinit();
    // The logical cluster/table manifests both contain `{}`; they remain two
    // objects but share one physical digest-addressed record.
    try std.testing.expect(base_manifest.value.objects.len > base_manifest.value.blobs.len);
    var parent_digest_bytes: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(manifest_block.payload, &parent_digest_bytes, .{});
    const parent_digest = std.fmt.bytesToHex(parent_digest_bytes, .lower);
    const second_key = try internal_keys.documentKeyAlloc(alloc, "doc2");
    defer alloc.free(second_key);
    try src.putBatch(&.{.{ .key = second_key, .value = "{\"id\":\"doc2\"}" }}, &.{});
    var delta_archive: ArrayList(u8) = .empty;
    defer delta_archive.deinit(alloc);
    var delta_writer = std.Io.Writer.Allocating.fromArrayList(alloc, &delta_archive);
    try exportPortableToWriterWithOptions(alloc, &src, &delta_writer.writer, .{
        .capture = .{ .delta = .{
            .manifest_sha256 = &parent_digest,
            .manifest = base_manifest.value,
        } },
    });
    delta_archive = delta_writer.toArrayList();
    try std.testing.expectError(error.BackupBaseRequired, validatePortable(alloc, delta_archive.items));

    var base_context: TestPortableBaseContext = .{ .data = base_archive.items };
    var tmp_dst = std.testing.tmpDir(.{});
    defer tmp_dst.cleanup();
    var dst = try openTestStore(alloc, &tmp_dst);
    defer dst.close();
    try importPortableWithOptions(alloc, &dst, delta_archive.items, .{
        .bundle_base = .{
            .manifest_sha256 = &parent_digest,
            .context = &base_context,
            .read_alloc = readTestPortableBaseBlob,
        },
    });
    for ([_][]const u8{ "doc1", "doc2" }) |doc_key| {
        const store_key = try internal_keys.documentKeyAlloc(alloc, doc_key);
        defer alloc.free(store_key);
        const value = try dst.get(alloc, store_key);
        defer alloc.free(value);
        try std.testing.expect(std.mem.indexOf(u8, value, doc_key) != null);
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

test "file import reports a busy source without waiting for its writer" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const archive_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/locked.afb", .{tmp.sub_path});
    defer alloc.free(archive_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var writer = try std.Io.Dir.cwd().createFile(io, archive_path, .{ .read = true, .truncate = true });
    defer writer.close(io);
    try writer.writePositionalAll(io, "not-yet-complete", 0);
    try writer.lock(io, .exclusive);
    defer writer.unlock(io);

    var reader = try std.Io.Dir.cwd().openFile(io, archive_path, .{});
    defer reader.close(io);
    var store = try openTestStore(alloc, &tmp);
    defer store.close();
    try std.testing.expectError(
        error.WriterLocked,
        importPortableFile(alloc, &store, io, reader, "not-yet-complete".len),
    );
}

test "file import rejects oversized portable blocks before allocation" {
    const alloc = std.testing.allocator;
    var encoded: ArrayList(u8) = .empty;
    defer encoded.deinit(alloc);
    try backup_codec.writeHeader(&encoded, alloc, .{
        .format_version = backup_codec.legacy_format_version,
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

    try std.testing.expectError(error.InvalidBundleFooter, importPortable(alloc, &dst, truncated));
    try std.testing.expectError(error.NotFound, dst.get(alloc, store_key));
}

test "import preflights logical block payloads before mutating destination" {
    const alloc = std.testing.allocator;

    var portable: ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try backup_codec.writeHeader(&portable, alloc, .{
        .format_version = backup_codec.legacy_format_version,
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

test "import preflights durable catalogs before mutating destination" {
    const alloc = std.testing.allocator;
    const cases = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "\x00\x00__metadata__:indexes", .value = "not-an-index-catalog" },
        .{ .key = "\x00\x00__metadata__:enrichments", .value = "not-json" },
        .{ .key = "\x00\x00__metadata__:resolvers", .value = "not-json" },
    };
    for (cases) |case| {
        var tmp_src = std.testing.tmpDir(.{});
        defer tmp_src.cleanup();
        var src = try openTestStore(alloc, &tmp_src);
        defer src.close();
        const source_key = try internal_keys.documentKeyAlloc(alloc, "doc:must-not-publish");
        defer alloc.free(source_key);
        try src.putBatch(&.{
            .{ .key = source_key, .value = "{\"title\":\"staged only\"}" },
            .{ .key = case.key, .value = case.value },
        }, &.{});

        var portable: ArrayList(u8) = .empty;
        defer portable.deinit(alloc);
        try exportPortable(alloc, &src, &portable);

        var tmp_dst = std.testing.tmpDir(.{});
        defer tmp_dst.cleanup();
        var dst = try openTestStore(alloc, &tmp_dst);
        defer dst.close();
        const store_key = try internal_keys.documentKeyAlloc(alloc, "doc:must-not-publish");
        defer alloc.free(store_key);

        try std.testing.expectError(error.InvalidMetadataBatch, importPortable(alloc, &dst, portable.items));
        try std.testing.expectError(error.NotFound, dst.get(alloc, store_key));
    }
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

    var timestamp_inspection = PortableTimestampInspection{
        .alloc = alloc,
        .expected_key = doc_key,
        .expected_timestamp_ns = timestamp_ns,
    };
    try visitPortableBlocks(alloc, out.items, &timestamp_inspection, PortableTimestampInspection.visit);
    try std.testing.expect(timestamp_inspection.found);

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
    var empty_index_catalog: [12]u8 = undefined;
    @memcpy(empty_index_catalog[0..4], "AIDX");
    std.mem.writeInt(u32, empty_index_catalog[4..8], 2, .little);
    std.mem.writeInt(u32, empty_index_catalog[8..12], 0, .little);
    const portable_entries = [_]KVPair{
        .{ .key = "\x00\x00__metadata__:schema_json", .value = public_schema },
        .{ .key = "\x00\x00__metadata__:schema", .value = runtime_schema },
        .{ .key = "\x00\x00__metadata__:schema_v1", .value = runtime_schema },
        .{ .key = "\x00\x00__metadata__:indexes", .value = &empty_index_catalog },
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
    const edge_val = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 1, 2.5, 11, 22, "{\"ok\":true}");
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
        try std.testing.expect(enrichment_artifact_codec.isPortableUnboundGraphEdge(val));
        try std.testing.expectEqual(@as(u64, 0), decoded.generation);
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

    var batch_inspection = PortableBatchInspection{
        .alloc = alloc,
        .expected_type = .chunk_batch,
        .expected_prefix = "af1:chunk:",
    };
    try visitPortableBlocks(alloc, out.items, &batch_inspection, PortableBatchInspection.visit);
    try std.testing.expectEqual(@as(usize, 2), batch_inspection.observed_count);

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

    var batch_inspection = PortableBatchInspection{
        .alloc = alloc,
        .expected_type = .artifact_batch,
        .expected_prefix = "af1:asset:",
    };
    try visitPortableBlocks(alloc, out.items, &batch_inspection, PortableBatchInspection.visit);
    try std.testing.expectEqual(@as(usize, 2), batch_inspection.observed_count);

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

    var batch_inspection = PortableBatchInspection{
        .alloc = alloc,
        .expected_type = .resolution_batch,
        .expected_prefix = resolution_public_id_prefix,
    };
    try visitPortableBlocks(alloc, out.items, &batch_inspection, PortableBatchInspection.visit);
    try std.testing.expectEqual(@as(usize, 1), batch_inspection.observed_count);

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
