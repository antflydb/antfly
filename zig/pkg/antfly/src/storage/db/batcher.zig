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
const types = @import("types.zig");
const derived_types = @import("derived/derived_types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");

pub const ApplyFn = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool;

// Source records are often one document plus one derived write. Keep this high
// enough that the per-kind item budgets, not the WAL-record count, determine
// when large catch-up batches are applied.
const replay_batch_max_records: usize = 4096;
const dense_replay_batch_max_records: usize = 16 * 1024;
// Large dense replay windows amplify apply-lock hold time and were the first
// batches to trip the 1M ingest corruption surface. Keep dense replay batches
// materially smaller than the catch-up coalesce window so each flush stays
// bounded even when source records carry hundreds of embeddings.
const dense_replay_batch_max_embeddings: usize = 4 * 1024;
const sparse_replay_batch_max_embeddings: usize = 4096;
const text_replay_batch_max_documents: usize = 4096;
const graph_replay_batch_max_mutations: usize = 1024;

pub const ReplayBatcher = union(enum) {
    full_text: TextReplayAccumulator,
    algebraic: TextReplayAccumulator,
    dense_vector: DenseReplayAccumulator,
    sparse_vector: SparseReplayAccumulator,
    graph: GraphReplayAccumulator,

    pub fn init(alloc: Allocator, index_ref: index_manager_mod.ManagedIndexRef) ReplayBatcher {
        return switch (index_ref.kind) {
            .full_text => .{ .full_text = .{ .alloc = alloc, .target_kind = .full_text } },
            .algebraic => .{ .algebraic = .{ .alloc = alloc, .target_kind = .algebraic } },
            .dense_vector => .{ .dense_vector = .{ .alloc = alloc, .index_name = index_ref.name } },
            .sparse_vector => .{ .sparse_vector = .{ .alloc = alloc, .index_name = index_ref.name } },
            .graph => .{ .graph = .{ .alloc = alloc, .index_name = index_ref.name } },
        };
    }

    pub fn deinit(self: *ReplayBatcher) void {
        switch (self.*) {
            inline else => |*acc| acc.deinit(),
        }
    }

    pub fn empty(self: *const ReplayBatcher) bool {
        return switch (self.*) {
            inline else => |acc| acc.empty(),
        };
    }

    pub fn shouldBuffer(self: *const ReplayBatcher, batch: derived_types.DerivedBatch) bool {
        return switch (self.*) {
            inline else => |acc| acc.shouldBuffer(batch),
        };
    }

    pub fn appendBatch(self: *ReplayBatcher, batch: derived_types.DerivedBatch, sequence: u64) !void {
        switch (self.*) {
            inline else => |*acc| try acc.appendBatch(batch, sequence),
        }
    }

    pub fn shouldFlush(self: *const ReplayBatcher) bool {
        return switch (self.*) {
            inline else => |acc| acc.shouldFlush(),
        };
    }

    pub fn flush(self: *ReplayBatcher, ctx: *anyopaque, apply_fn: ApplyFn, index_ref: index_manager_mod.ManagedIndexRef) !usize {
        return switch (self.*) {
            inline else => |*acc| try acc.flush(ctx, apply_fn, index_ref),
        };
    }
};

const TextReplayAccumulator = struct {
    alloc: Allocator,
    target_kind: derived_types.DerivedTarget = .full_text,
    pre_deletes: std.StringHashMapUnmanaged(void) = .empty,
    documents: std.StringHashMapUnmanaged(derived_types.DerivedDocument) = .empty,
    source_records: usize = 0,
    document_count: usize = 0,
    last_sequence: u64 = 0,

    fn deinit(self: *TextReplayAccumulator) void {
        var delete_it = self.pre_deletes.iterator();
        while (delete_it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.pre_deletes.deinit(self.alloc);

        var docs_it = self.documents.iterator();
        while (docs_it.next()) |entry| deinitDerivedDocument(self.alloc, &entry.value_ptr.*);
        self.documents.deinit(self.alloc);

        self.* = .{ .alloc = self.alloc, .target_kind = self.target_kind };
    }

    fn empty(self: *const TextReplayAccumulator) bool {
        return self.source_records == 0;
    }

    fn shouldBuffer(self: *const TextReplayAccumulator, batch: derived_types.DerivedBatch) bool {
        if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return true;
        for (batch.documents) |doc| {
            if (documentHasAnyTargetKind(doc, self.target_kind)) return true;
        }
        return false;
    }

    fn shouldFlush(self: *const TextReplayAccumulator) bool {
        return self.source_records >= replay_batch_max_records or
            self.document_count >= text_replay_batch_max_documents;
    }

    fn appendBatch(self: *TextReplayAccumulator, batch: derived_types.DerivedBatch, sequence: u64) !void {
        for (batch.deleted_keys) |key| try self.recordDelete(key);
        for (batch.overwritten_doc_keys) |key| try self.recordDelete(key);
        for (batch.documents) |doc| {
            if (!documentHasAnyTargetKind(doc, .full_text)) continue;
            try self.recordDocument(doc);
        }
        self.source_records += 1;
        self.last_sequence = sequence;
    }

    fn recordDelete(self: *TextReplayAccumulator, key: []const u8) !void {
        if (self.documents.fetchRemove(key)) |removed| {
            var removed_value = removed.value;
            deinitDerivedDocument(self.alloc, &removed_value);
            self.document_count -= 1;
        }
        const gop = try self.pre_deletes.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.alloc.dupe(u8, key);
    }

    fn recordDocument(self: *TextReplayAccumulator, doc: derived_types.DerivedDocument) !void {
        var owned = try cloneDerivedDocument(self.alloc, doc);
        errdefer deinitDerivedDocument(self.alloc, &owned);

        const gop = try self.documents.getOrPut(self.alloc, doc.key);
        if (gop.found_existing) {
            deinitDerivedDocument(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned.key;
            self.document_count += 1;
        }
        gop.value_ptr.* = owned;
    }

    fn flush(self: *TextReplayAccumulator, ctx: *anyopaque, apply_fn: ApplyFn, index_ref: index_manager_mod.ManagedIndexRef) !usize {
        if (self.empty()) return 0;
        const source_records = self.source_records;
        var batch = try self.takeBatch();
        defer derived_types.deinitDerivedBatch(self.alloc, &batch);
        _ = try apply_fn(ctx, batch, index_ref);
        return source_records;
    }

    fn takeBatch(self: *TextReplayAccumulator) !derived_types.DerivedBatch {
        const delete_count = self.pre_deletes.count();
        const doc_count = self.documents.count();

        var overwritten_doc_keys = try self.alloc.alloc([]const u8, delete_count);
        errdefer self.alloc.free(overwritten_doc_keys);
        var documents = try self.alloc.alloc(derived_types.DerivedDocument, doc_count);
        errdefer self.alloc.free(documents);

        var delete_it = self.pre_deletes.iterator();
        var delete_index: usize = 0;
        while (delete_it.next()) |entry| : (delete_index += 1) overwritten_doc_keys[delete_index] = entry.key_ptr.*;

        var docs_it = self.documents.iterator();
        var doc_index: usize = 0;
        while (docs_it.next()) |entry| : (doc_index += 1) documents[doc_index] = entry.value_ptr.*;

        if (delete_count > 1) std.mem.sort([]const u8, overwritten_doc_keys, {}, lessThanString);
        if (doc_count > 1) std.mem.sort(derived_types.DerivedDocument, documents, {}, lessThanDocument);

        self.pre_deletes.deinit(self.alloc);
        self.pre_deletes = .empty;
        self.documents.deinit(self.alloc);
        self.documents = .empty;

        const sequence = self.last_sequence;
        self.source_records = 0;
        self.document_count = 0;
        self.last_sequence = 0;

        return .{
            .sequence = sequence,
            .documents = documents,
            .overwritten_doc_keys = overwritten_doc_keys,
        };
    }
};

const DenseReplayAccumulator = struct {
    alloc: Allocator,
    index_name: []const u8,
    pre_deletes: std.StringHashMapUnmanaged(void) = .empty,
    documents: std.StringHashMapUnmanaged(derived_types.DerivedDocument) = .empty,
    dense_embeddings: std.StringHashMapUnmanaged(derived_types.DerivedDenseEmbeddingWrite) = .empty,
    dense_vectors_by_parent: std.StringHashMapUnmanaged(std.ArrayListUnmanaged([]u8)) = .empty,
    source_records: usize = 0,
    dense_embedding_count: usize = 0,
    last_sequence: u64 = 0,

    fn deinit(self: *DenseReplayAccumulator) void {
        var delete_it = self.pre_deletes.iterator();
        while (delete_it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.pre_deletes.deinit(self.alloc);

        var docs_it = self.documents.iterator();
        while (docs_it.next()) |entry| deinitDerivedDocument(self.alloc, &entry.value_ptr.*);
        self.documents.deinit(self.alloc);

        var embeddings_it = self.dense_embeddings.iterator();
        while (embeddings_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            deinitDenseEmbeddingWrite(self.alloc, &entry.value_ptr.*);
        }
        self.dense_embeddings.deinit(self.alloc);

        var parent_it = self.dense_vectors_by_parent.iterator();
        while (parent_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            freeStringList(self.alloc, &entry.value_ptr.*);
        }
        self.dense_vectors_by_parent.deinit(self.alloc);

        self.* = .{ .alloc = self.alloc, .index_name = self.index_name };
    }

    fn empty(self: *const DenseReplayAccumulator) bool {
        return self.source_records == 0;
    }

    fn shouldBuffer(self: *const DenseReplayAccumulator, batch: derived_types.DerivedBatch) bool {
        if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0 or batch.documents.len > 0) return true;
        for (batch.dense_embeddings) |embedding| {
            if (std.mem.eql(u8, embedding.index_name, self.index_name)) return true;
        }
        return false;
    }

    fn shouldFlush(self: *const DenseReplayAccumulator) bool {
        return self.source_records >= dense_replay_batch_max_records or
            self.dense_embedding_count >= dense_replay_batch_max_embeddings;
    }

    fn appendBatch(self: *DenseReplayAccumulator, batch: derived_types.DerivedBatch, sequence: u64) !void {
        for (batch.deleted_keys) |key| try self.recordDelete(key);
        for (batch.overwritten_doc_keys) |key| try self.recordDelete(key);
        for (batch.documents) |doc| try self.recordDocument(doc);
        for (batch.dense_embeddings) |embedding| {
            if (!std.mem.eql(u8, embedding.index_name, self.index_name)) continue;
            try self.recordDenseEmbedding(embedding);
        }
        self.source_records += 1;
        self.last_sequence = sequence;
    }

    fn recordDelete(self: *DenseReplayAccumulator, key: []const u8) !void {
        if (self.documents.fetchRemove(key)) |removed| {
            var removed_value = removed.value;
            deinitDerivedDocument(self.alloc, &removed_value);
        }
        try self.removeDenseEmbeddingsForDoc(key);
        const gop = try self.pre_deletes.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.alloc.dupe(u8, key);
    }

    fn recordDocument(self: *DenseReplayAccumulator, doc: derived_types.DerivedDocument) !void {
        var owned = try cloneDerivedDocument(self.alloc, doc);
        errdefer deinitDerivedDocument(self.alloc, &owned);

        const gop = try self.documents.getOrPut(self.alloc, doc.key);
        if (gop.found_existing) {
            deinitDerivedDocument(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned.key;
        }
        gop.value_ptr.* = owned;
    }

    fn recordDenseEmbedding(self: *DenseReplayAccumulator, embedding: derived_types.DerivedDenseEmbeddingWrite) !void {
        var owned = try cloneDenseEmbeddingWrite(self.alloc, embedding);
        errdefer deinitDenseEmbeddingWrite(self.alloc, &owned);

        const owned_map_key = try self.alloc.dupe(u8, embedding.doc_key);
        errdefer self.alloc.free(owned_map_key);

        const parent_key = try self.resolveDenseParentDocKey(embedding);
        defer if (parent_key.owned) |owned_parent| self.alloc.free(owned_parent);

        const found_existing = self.dense_embeddings.contains(embedding.doc_key);
        if (!found_existing) try self.recordDenseVectorForParent(parent_key.key, embedding.doc_key);

        const gop = try self.dense_embeddings.getOrPut(self.alloc, owned_map_key);
        if (gop.found_existing) {
            self.alloc.free(owned_map_key);
            deinitDenseEmbeddingWrite(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned_map_key;
            self.dense_embedding_count += 1;
        }
        gop.value_ptr.* = owned;
    }

    fn recordDenseVectorForParent(self: *DenseReplayAccumulator, parent_key: []const u8, vector_key: []const u8) !void {
        const gop = try self.dense_vectors_by_parent.getOrPut(self.alloc, parent_key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, parent_key);
            gop.value_ptr.* = .empty;
        }
        const owned_vector_key = try self.alloc.dupe(u8, vector_key);
        errdefer self.alloc.free(owned_vector_key);
        try gop.value_ptr.append(self.alloc, owned_vector_key);
    }

    fn removeDenseEmbeddingsForDoc(self: *DenseReplayAccumulator, key: []const u8) !void {
        if (self.dense_vectors_by_parent.fetchRemove(key)) |removed_parent| {
            self.alloc.free(removed_parent.key);
            var vector_keys = removed_parent.value;
            defer freeStringList(self.alloc, &vector_keys);
            for (vector_keys.items) |vector_key| {
                if (self.dense_embeddings.fetchRemove(vector_key)) |removed| {
                    self.alloc.free(removed.key);
                    var removed_value = removed.value;
                    deinitDenseEmbeddingWrite(self.alloc, &removed_value);
                    self.dense_embedding_count -= 1;
                }
            }
        } else if (self.dense_embeddings.fetchRemove(key)) |removed| {
            self.alloc.free(removed.key);
            var removed_value = removed.value;
            deinitDenseEmbeddingWrite(self.alloc, &removed_value);
            self.dense_embedding_count -= 1;
        }
    }

    fn clearDenseParentMap(self: *DenseReplayAccumulator) void {
        var parent_it = self.dense_vectors_by_parent.iterator();
        while (parent_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            freeStringList(self.alloc, &entry.value_ptr.*);
        }
        self.dense_vectors_by_parent.deinit(self.alloc);
        self.dense_vectors_by_parent = .empty;
    }

    const ResolvedParentDocKey = struct {
        key: []const u8,
        owned: ?[]u8 = null,
    };

    fn resolveDenseParentDocKey(self: *DenseReplayAccumulator, embedding: derived_types.DerivedDenseEmbeddingWrite) !ResolvedParentDocKey {
        if (embedding.parent_doc_key) |key| return .{ .key = key };
        if (!internal_keys.isChunkArtifactRecordKey(embedding.doc_key)) return .{ .key = embedding.doc_key };
        const parent = (try internal_keys.decodeDocumentComponentAlloc(self.alloc, embedding.doc_key)) orelse return .{ .key = embedding.doc_key };
        return .{ .key = parent, .owned = parent };
    }

    fn freeStringList(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8)) void {
        for (list.items) |value| alloc.free(value);
        list.deinit(alloc);
    }

    fn flush(self: *DenseReplayAccumulator, ctx: *anyopaque, apply_fn: ApplyFn, index_ref: index_manager_mod.ManagedIndexRef) !usize {
        if (self.empty()) return 0;
        const source_records = self.source_records;
        var batch = try self.takeBatch();
        defer derived_types.deinitDerivedBatch(self.alloc, &batch);
        _ = try apply_fn(ctx, batch, index_ref);
        return source_records;
    }

    fn takeBatch(self: *DenseReplayAccumulator) !derived_types.DerivedBatch {
        const delete_count = self.pre_deletes.count();
        const doc_count = self.documents.count();
        const embedding_count = self.dense_embeddings.count();

        var overwritten_doc_keys = try self.alloc.alloc([]const u8, delete_count);
        errdefer self.alloc.free(overwritten_doc_keys);
        var documents = try self.alloc.alloc(derived_types.DerivedDocument, doc_count);
        errdefer self.alloc.free(documents);
        var dense_embeddings = try self.alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, embedding_count);
        errdefer self.alloc.free(dense_embeddings);

        var delete_it = self.pre_deletes.iterator();
        var delete_index: usize = 0;
        while (delete_it.next()) |entry| : (delete_index += 1) overwritten_doc_keys[delete_index] = entry.key_ptr.*;

        var docs_it = self.documents.iterator();
        var doc_index: usize = 0;
        while (docs_it.next()) |entry| : (doc_index += 1) documents[doc_index] = entry.value_ptr.*;

        var embeddings_it = self.dense_embeddings.iterator();
        var embedding_index: usize = 0;
        while (embeddings_it.next()) |entry| : (embedding_index += 1) {
            dense_embeddings[embedding_index] = entry.value_ptr.*;
            self.alloc.free(entry.key_ptr.*);
        }

        if (delete_count > 1) std.mem.sort([]const u8, overwritten_doc_keys, {}, lessThanString);
        if (doc_count > 1) std.mem.sort(derived_types.DerivedDocument, documents, {}, lessThanDocument);
        if (embedding_count > 1) std.mem.sort(derived_types.DerivedDenseEmbeddingWrite, dense_embeddings, {}, lessThanDenseEmbedding);

        self.pre_deletes.deinit(self.alloc);
        self.pre_deletes = .empty;
        self.documents.deinit(self.alloc);
        self.documents = .empty;
        self.dense_embeddings.deinit(self.alloc);
        self.dense_embeddings = .empty;
        self.clearDenseParentMap();

        const sequence = self.last_sequence;
        self.source_records = 0;
        self.dense_embedding_count = 0;
        self.last_sequence = 0;

        return .{
            .sequence = sequence,
            .documents = documents,
            .overwritten_doc_keys = overwritten_doc_keys,
            .dense_embeddings = dense_embeddings,
        };
    }
};

const SparseReplayAccumulator = struct {
    alloc: Allocator,
    index_name: []const u8,
    pre_deletes: std.StringHashMapUnmanaged(void) = .empty,
    documents: std.StringHashMapUnmanaged(derived_types.DerivedDocument) = .empty,
    sparse_embeddings: std.StringHashMapUnmanaged(derived_types.DerivedSparseEmbeddingWrite) = .empty,
    source_records: usize = 0,
    sparse_embedding_count: usize = 0,
    last_sequence: u64 = 0,

    fn deinit(self: *SparseReplayAccumulator) void {
        var delete_it = self.pre_deletes.iterator();
        while (delete_it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.pre_deletes.deinit(self.alloc);

        var docs_it = self.documents.iterator();
        while (docs_it.next()) |entry| deinitDerivedDocument(self.alloc, &entry.value_ptr.*);
        self.documents.deinit(self.alloc);

        var embeddings_it = self.sparse_embeddings.iterator();
        while (embeddings_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            deinitSparseEmbeddingWrite(self.alloc, &entry.value_ptr.*);
        }
        self.sparse_embeddings.deinit(self.alloc);

        self.* = .{ .alloc = self.alloc, .index_name = self.index_name };
    }

    fn empty(self: *const SparseReplayAccumulator) bool {
        return self.source_records == 0;
    }

    fn shouldBuffer(self: *const SparseReplayAccumulator, batch: derived_types.DerivedBatch) bool {
        if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0 or batch.documents.len > 0) return true;
        for (batch.sparse_embeddings) |embedding| {
            if (std.mem.eql(u8, embedding.index_name, self.index_name)) return true;
        }
        return false;
    }

    fn shouldFlush(self: *const SparseReplayAccumulator) bool {
        return self.source_records >= replay_batch_max_records or
            self.sparse_embedding_count >= sparse_replay_batch_max_embeddings;
    }

    fn appendBatch(self: *SparseReplayAccumulator, batch: derived_types.DerivedBatch, sequence: u64) !void {
        for (batch.deleted_keys) |key| try self.recordDelete(key);
        for (batch.overwritten_doc_keys) |key| try self.recordDelete(key);
        for (batch.documents) |doc| try self.recordDocument(doc);
        for (batch.sparse_embeddings) |embedding| {
            if (!std.mem.eql(u8, embedding.index_name, self.index_name)) continue;
            try self.recordSparseEmbedding(embedding);
        }
        self.source_records += 1;
        self.last_sequence = sequence;
    }

    fn recordDelete(self: *SparseReplayAccumulator, key: []const u8) !void {
        if (self.documents.fetchRemove(key)) |removed| {
            var removed_value = removed.value;
            deinitDerivedDocument(self.alloc, &removed_value);
        }
        try self.removeSparseEmbeddingsForDoc(key);
        const gop = try self.pre_deletes.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.alloc.dupe(u8, key);
    }

    fn recordDocument(self: *SparseReplayAccumulator, doc: derived_types.DerivedDocument) !void {
        var owned = try cloneDerivedDocument(self.alloc, doc);
        errdefer deinitDerivedDocument(self.alloc, &owned);

        const gop = try self.documents.getOrPut(self.alloc, doc.key);
        if (gop.found_existing) {
            deinitDerivedDocument(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned.key;
        }
        gop.value_ptr.* = owned;
    }

    fn recordSparseEmbedding(self: *SparseReplayAccumulator, embedding: derived_types.DerivedSparseEmbeddingWrite) !void {
        var owned = try cloneSparseEmbeddingWrite(self.alloc, embedding);
        errdefer deinitSparseEmbeddingWrite(self.alloc, &owned);

        const owned_map_key = try indexDocKeyAlloc(self.alloc, embedding.index_name, embedding.doc_key);
        errdefer self.alloc.free(owned_map_key);

        const gop = try self.sparse_embeddings.getOrPut(self.alloc, owned_map_key);
        if (gop.found_existing) {
            self.alloc.free(owned_map_key);
            deinitSparseEmbeddingWrite(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned_map_key;
            self.sparse_embedding_count += 1;
        }
        gop.value_ptr.* = owned;
    }

    fn removeSparseEmbeddingsForDoc(self: *SparseReplayAccumulator, key: []const u8) !void {
        var remove_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (remove_keys.items) |remove_key| self.alloc.free(remove_key);
            remove_keys.deinit(self.alloc);
        }

        var embeddings_it = self.sparse_embeddings.iterator();
        while (embeddings_it.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.doc_key, key)) continue;
            try remove_keys.append(self.alloc, try self.alloc.dupe(u8, entry.key_ptr.*));
        }

        for (remove_keys.items) |remove_key| {
            if (self.sparse_embeddings.fetchRemove(remove_key)) |removed| {
                self.alloc.free(removed.key);
                var removed_value = removed.value;
                deinitSparseEmbeddingWrite(self.alloc, &removed_value);
                self.sparse_embedding_count -= 1;
            }
        }
    }

    fn flush(self: *SparseReplayAccumulator, ctx: *anyopaque, apply_fn: ApplyFn, index_ref: index_manager_mod.ManagedIndexRef) !usize {
        if (self.empty()) return 0;
        const source_records = self.source_records;
        var batch = try self.takeBatch();
        defer derived_types.deinitDerivedBatch(self.alloc, &batch);
        _ = try apply_fn(ctx, batch, index_ref);
        return source_records;
    }

    fn takeBatch(self: *SparseReplayAccumulator) !derived_types.DerivedBatch {
        const delete_count = self.pre_deletes.count();
        const doc_count = self.documents.count();
        const embedding_count = self.sparse_embeddings.count();

        var overwritten_doc_keys = try self.alloc.alloc([]const u8, delete_count);
        errdefer self.alloc.free(overwritten_doc_keys);
        var documents = try self.alloc.alloc(derived_types.DerivedDocument, doc_count);
        errdefer self.alloc.free(documents);
        var sparse_embeddings = try self.alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, embedding_count);
        errdefer self.alloc.free(sparse_embeddings);

        var delete_it = self.pre_deletes.iterator();
        var delete_index: usize = 0;
        while (delete_it.next()) |entry| : (delete_index += 1) overwritten_doc_keys[delete_index] = entry.key_ptr.*;

        var docs_it = self.documents.iterator();
        var doc_index: usize = 0;
        while (docs_it.next()) |entry| : (doc_index += 1) documents[doc_index] = entry.value_ptr.*;

        var embeddings_it = self.sparse_embeddings.iterator();
        var embedding_index: usize = 0;
        while (embeddings_it.next()) |entry| : (embedding_index += 1) {
            sparse_embeddings[embedding_index] = entry.value_ptr.*;
            self.alloc.free(entry.key_ptr.*);
        }

        if (delete_count > 1) std.mem.sort([]const u8, overwritten_doc_keys, {}, lessThanString);
        if (doc_count > 1) std.mem.sort(derived_types.DerivedDocument, documents, {}, lessThanDocument);
        if (embedding_count > 1) std.mem.sort(derived_types.DerivedSparseEmbeddingWrite, sparse_embeddings, {}, lessThanSparseEmbedding);

        self.pre_deletes.deinit(self.alloc);
        self.pre_deletes = .empty;
        self.documents.deinit(self.alloc);
        self.documents = .empty;
        self.sparse_embeddings.deinit(self.alloc);
        self.sparse_embeddings = .empty;

        const sequence = self.last_sequence;
        self.source_records = 0;
        self.sparse_embedding_count = 0;
        self.last_sequence = 0;

        return .{
            .sequence = sequence,
            .documents = documents,
            .overwritten_doc_keys = overwritten_doc_keys,
            .sparse_embeddings = sparse_embeddings,
        };
    }
};

const GraphReplayAccumulator = struct {
    alloc: Allocator,
    index_name: []const u8,
    deleted_keys: std.StringHashMapUnmanaged(void) = .empty,
    doc_clears: std.StringHashMapUnmanaged(void) = .empty,
    graph_writes: std.StringHashMapUnmanaged(types.GraphEdgeWrite) = .empty,
    graph_deletes: std.StringHashMapUnmanaged(types.GraphEdgeDelete) = .empty,
    source_records: usize = 0,
    mutation_count: usize = 0,
    last_sequence: u64 = 0,

    fn deinit(self: *GraphReplayAccumulator) void {
        var delete_it = self.deleted_keys.iterator();
        while (delete_it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.deleted_keys.deinit(self.alloc);

        var clear_it = self.doc_clears.iterator();
        while (clear_it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.doc_clears.deinit(self.alloc);

        var writes_it = self.graph_writes.iterator();
        while (writes_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            deinitGraphWrite(self.alloc, &entry.value_ptr.*);
        }
        self.graph_writes.deinit(self.alloc);

        var deletes_it = self.graph_deletes.iterator();
        while (deletes_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            deinitGraphDelete(self.alloc, &entry.value_ptr.*);
        }
        self.graph_deletes.deinit(self.alloc);

        self.* = .{ .alloc = self.alloc, .index_name = self.index_name };
    }

    fn empty(self: *const GraphReplayAccumulator) bool {
        return self.source_records == 0;
    }

    fn shouldBuffer(self: *const GraphReplayAccumulator, batch: derived_types.DerivedBatch) bool {
        if (batch.deleted_keys.len > 0) return true;
        for (batch.graph_doc_clears) |clear| {
            for (clear.index_names) |index_name| {
                if (std.mem.eql(u8, index_name, self.index_name)) return true;
            }
        }
        for (batch.graph_writes) |write| {
            if (std.mem.eql(u8, write.index_name, self.index_name)) return true;
        }
        for (batch.graph_deletes) |delete| {
            if (std.mem.eql(u8, delete.index_name, self.index_name)) return true;
        }
        return false;
    }

    fn shouldFlush(self: *const GraphReplayAccumulator) bool {
        return self.source_records >= replay_batch_max_records or
            self.mutation_count >= graph_replay_batch_max_mutations;
    }

    fn appendBatch(self: *GraphReplayAccumulator, batch: derived_types.DerivedBatch, sequence: u64) !void {
        for (batch.deleted_keys) |key| try self.recordDocDelete(key);
        for (batch.graph_doc_clears) |clear| {
            var matches = false;
            for (clear.index_names) |index_name| {
                if (std.mem.eql(u8, index_name, self.index_name)) {
                    matches = true;
                    break;
                }
            }
            if (matches) try self.recordDocClear(clear.key);
        }
        for (batch.graph_deletes) |delete| {
            if (!std.mem.eql(u8, delete.index_name, self.index_name)) continue;
            try self.recordGraphDelete(delete);
        }
        for (batch.graph_writes) |write| {
            if (!std.mem.eql(u8, write.index_name, self.index_name)) continue;
            try self.recordGraphWrite(write);
        }
        self.source_records += 1;
        self.last_sequence = sequence;
    }

    fn recordDocDelete(self: *GraphReplayAccumulator, key: []const u8) !void {
        if (self.doc_clears.fetchRemove(key)) |removed| self.alloc.free(removed.key);
        try self.removeGraphMutationsForDoc(key);
        const gop = try self.deleted_keys.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.alloc.dupe(u8, key);
    }

    fn recordDocClear(self: *GraphReplayAccumulator, key: []const u8) !void {
        if (self.deleted_keys.contains(key)) return;
        try self.removeGraphMutationsForDoc(key);
        const gop = try self.doc_clears.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.alloc.dupe(u8, key);
    }

    fn recordGraphWrite(self: *GraphReplayAccumulator, write: types.GraphEdgeWrite) !void {
        const owned_key = try edgeKeyAlloc(self.alloc, write.source, write.target, write.edge_type);
        errdefer self.alloc.free(owned_key);

        if (self.graph_deletes.fetchRemove(owned_key)) |removed| {
            self.alloc.free(removed.key);
            var removed_value = removed.value;
            deinitGraphDelete(self.alloc, &removed_value);
            self.mutation_count -= 1;
        }

        var owned = try cloneGraphWrite(self.alloc, write);
        errdefer deinitGraphWrite(self.alloc, &owned);

        const gop = try self.graph_writes.getOrPut(self.alloc, owned_key);
        if (gop.found_existing) {
            self.alloc.free(owned_key);
            deinitGraphWrite(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned_key;
            self.mutation_count += 1;
        }
        gop.value_ptr.* = owned;
    }

    fn recordGraphDelete(self: *GraphReplayAccumulator, delete: types.GraphEdgeDelete) !void {
        if (self.deleted_keys.contains(delete.source) or self.deleted_keys.contains(delete.target) or
            self.doc_clears.contains(delete.source) or self.doc_clears.contains(delete.target))
        {
            return;
        }

        const owned_key = try edgeKeyAlloc(self.alloc, delete.source, delete.target, delete.edge_type);
        errdefer self.alloc.free(owned_key);

        if (self.graph_writes.fetchRemove(owned_key)) |removed| {
            self.alloc.free(removed.key);
            var removed_value = removed.value;
            deinitGraphWrite(self.alloc, &removed_value);
            self.mutation_count -= 1;
        }

        var owned = try cloneGraphDelete(self.alloc, delete);
        errdefer deinitGraphDelete(self.alloc, &owned);

        const gop = try self.graph_deletes.getOrPut(self.alloc, owned_key);
        if (gop.found_existing) {
            self.alloc.free(owned_key);
            deinitGraphDelete(self.alloc, gop.value_ptr);
        } else {
            gop.key_ptr.* = owned_key;
            self.mutation_count += 1;
        }
        gop.value_ptr.* = owned;
    }

    fn removeGraphMutationsForDoc(self: *GraphReplayAccumulator, key: []const u8) !void {
        var remove_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (remove_keys.items) |remove_key| self.alloc.free(remove_key);
            remove_keys.deinit(self.alloc);
        }

        var writes_it = self.graph_writes.iterator();
        while (writes_it.next()) |entry| {
            const write = entry.value_ptr.*;
            if (std.mem.eql(u8, write.source, key) or std.mem.eql(u8, write.target, key)) {
                try remove_keys.append(self.alloc, try self.alloc.dupe(u8, entry.key_ptr.*));
            }
        }
        for (remove_keys.items) |remove_key| {
            if (self.graph_writes.fetchRemove(remove_key)) |removed| {
                self.alloc.free(removed.key);
                var removed_value = removed.value;
                deinitGraphWrite(self.alloc, &removed_value);
                self.mutation_count -= 1;
            }
        }
        for (remove_keys.items) |remove_key| self.alloc.free(remove_key);
        remove_keys.clearRetainingCapacity();

        var deletes_it = self.graph_deletes.iterator();
        while (deletes_it.next()) |entry| {
            const delete = entry.value_ptr.*;
            if (std.mem.eql(u8, delete.source, key) or std.mem.eql(u8, delete.target, key)) {
                try remove_keys.append(self.alloc, try self.alloc.dupe(u8, entry.key_ptr.*));
            }
        }
        for (remove_keys.items) |remove_key| {
            if (self.graph_deletes.fetchRemove(remove_key)) |removed| {
                self.alloc.free(removed.key);
                var removed_value = removed.value;
                deinitGraphDelete(self.alloc, &removed_value);
                self.mutation_count -= 1;
            }
        }
    }

    fn flush(self: *GraphReplayAccumulator, ctx: *anyopaque, apply_fn: ApplyFn, index_ref: index_manager_mod.ManagedIndexRef) !usize {
        if (self.empty()) return 0;
        const source_records = self.source_records;
        var batch = try self.takeBatch();
        defer derived_types.deinitDerivedBatch(self.alloc, &batch);
        _ = try apply_fn(ctx, batch, index_ref);
        return source_records;
    }

    fn takeBatch(self: *GraphReplayAccumulator) !derived_types.DerivedBatch {
        const delete_count = self.deleted_keys.count();
        const clear_count = self.doc_clears.count();
        const write_count = self.graph_writes.count();
        const delete_edge_count = self.graph_deletes.count();

        var deleted_keys = try self.alloc.alloc([]const u8, delete_count);
        errdefer self.alloc.free(deleted_keys);
        var graph_doc_clears = try self.alloc.alloc(derived_types.DerivedGraphDocClear, clear_count);
        errdefer self.alloc.free(graph_doc_clears);
        var graph_writes = try self.alloc.alloc(types.GraphEdgeWrite, write_count);
        errdefer self.alloc.free(graph_writes);
        var graph_deletes = try self.alloc.alloc(types.GraphEdgeDelete, delete_edge_count);
        errdefer self.alloc.free(graph_deletes);

        var delete_it = self.deleted_keys.iterator();
        var delete_index: usize = 0;
        while (delete_it.next()) |entry| : (delete_index += 1) deleted_keys[delete_index] = entry.key_ptr.*;

        var clear_it = self.doc_clears.iterator();
        var clear_index: usize = 0;
        while (clear_it.next()) |entry| : (clear_index += 1) {
            const index_names = try self.alloc.alloc([]const u8, 1);
            index_names[0] = try self.alloc.dupe(u8, self.index_name);
            graph_doc_clears[clear_index] = .{
                .key = entry.key_ptr.*,
                .index_names = index_names,
            };
        }

        var writes_it = self.graph_writes.iterator();
        var write_index: usize = 0;
        while (writes_it.next()) |entry| : (write_index += 1) {
            graph_writes[write_index] = entry.value_ptr.*;
            self.alloc.free(entry.key_ptr.*);
        }

        var deletes_it = self.graph_deletes.iterator();
        var delete_edge_index: usize = 0;
        while (deletes_it.next()) |entry| : (delete_edge_index += 1) {
            graph_deletes[delete_edge_index] = entry.value_ptr.*;
            self.alloc.free(entry.key_ptr.*);
        }

        if (delete_count > 1) std.mem.sort([]const u8, deleted_keys, {}, lessThanString);
        if (clear_count > 1) std.mem.sort(derived_types.DerivedGraphDocClear, graph_doc_clears, {}, lessThanGraphClear);
        if (write_count > 1) std.mem.sort(types.GraphEdgeWrite, graph_writes, {}, lessThanGraphWrite);
        if (delete_edge_count > 1) std.mem.sort(types.GraphEdgeDelete, graph_deletes, {}, lessThanGraphDelete);

        self.deleted_keys.deinit(self.alloc);
        self.deleted_keys = .empty;
        self.doc_clears.deinit(self.alloc);
        self.doc_clears = .empty;
        self.graph_writes.deinit(self.alloc);
        self.graph_writes = .empty;
        self.graph_deletes.deinit(self.alloc);
        self.graph_deletes = .empty;

        const sequence = self.last_sequence;
        self.source_records = 0;
        self.mutation_count = 0;
        self.last_sequence = 0;

        return .{
            .sequence = sequence,
            .deleted_keys = deleted_keys,
            .graph_doc_clears = graph_doc_clears,
            .graph_writes = graph_writes,
            .graph_deletes = graph_deletes,
        };
    }
};

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

fn lessThanDocument(_: void, lhs: derived_types.DerivedDocument, rhs: derived_types.DerivedDocument) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn documentHasAnyTargetKind(doc: derived_types.DerivedDocument, kind: derived_types.DerivedTarget) bool {
    for (doc.targets) |target| {
        if (target.kind == kind) return true;
    }
    return false;
}

fn lessThanDenseEmbedding(_: void, lhs: derived_types.DerivedDenseEmbeddingWrite, rhs: derived_types.DerivedDenseEmbeddingWrite) bool {
    const index_cmp = std.mem.order(u8, lhs.index_name, rhs.index_name);
    if (index_cmp != .eq) return index_cmp == .lt;
    return std.mem.order(u8, lhs.doc_key, rhs.doc_key) == .lt;
}

fn lessThanSparseEmbedding(_: void, lhs: derived_types.DerivedSparseEmbeddingWrite, rhs: derived_types.DerivedSparseEmbeddingWrite) bool {
    const index_cmp = std.mem.order(u8, lhs.index_name, rhs.index_name);
    if (index_cmp != .eq) return index_cmp == .lt;
    return std.mem.order(u8, lhs.doc_key, rhs.doc_key) == .lt;
}

fn lessThanGraphClear(_: void, lhs: derived_types.DerivedGraphDocClear, rhs: derived_types.DerivedGraphDocClear) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn lessThanGraphWrite(_: void, lhs: types.GraphEdgeWrite, rhs: types.GraphEdgeWrite) bool {
    const source_cmp = std.mem.order(u8, lhs.source, rhs.source);
    if (source_cmp != .eq) return source_cmp == .lt;
    const target_cmp = std.mem.order(u8, lhs.target, rhs.target);
    if (target_cmp != .eq) return target_cmp == .lt;
    return std.mem.order(u8, lhs.edge_type, rhs.edge_type) == .lt;
}

fn lessThanGraphDelete(_: void, lhs: types.GraphEdgeDelete, rhs: types.GraphEdgeDelete) bool {
    const source_cmp = std.mem.order(u8, lhs.source, rhs.source);
    if (source_cmp != .eq) return source_cmp == .lt;
    const target_cmp = std.mem.order(u8, lhs.target, rhs.target);
    if (target_cmp != .eq) return target_cmp == .lt;
    return std.mem.order(u8, lhs.edge_type, rhs.edge_type) == .lt;
}

fn cloneDerivedDocument(alloc: Allocator, doc: derived_types.DerivedDocument) !derived_types.DerivedDocument {
    var targets = try alloc.alloc(derived_types.DerivedTargetRef, doc.targets.len);
    errdefer alloc.free(targets);
    var initialized_targets: usize = 0;
    errdefer {
        for (targets[0..initialized_targets]) |target| alloc.free(target.index_name);
    }
    for (doc.targets, 0..) |target, i| {
        targets[i] = .{
            .kind = target.kind,
            .index_name = try alloc.dupe(u8, target.index_name),
        };
        initialized_targets += 1;
    }
    return .{
        .key = try alloc.dupe(u8, doc.key),
        .action = doc.action,
        .cleaned_value = if (doc.cleaned_value) |value| try alloc.dupe(u8, value) else null,
        .targets = targets,
    };
}

fn deinitDerivedDocument(alloc: Allocator, doc: *derived_types.DerivedDocument) void {
    alloc.free(doc.key);
    if (doc.cleaned_value) |value| alloc.free(value);
    for (doc.targets) |target| alloc.free(target.index_name);
    if (doc.targets.len > 0) alloc.free(doc.targets);
    doc.* = undefined;
}

fn cloneDenseEmbeddingWrite(alloc: Allocator, embedding: derived_types.DerivedDenseEmbeddingWrite) !derived_types.DerivedDenseEmbeddingWrite {
    return .{
        .index_name = try alloc.dupe(u8, embedding.index_name),
        .parent_doc_key = if (embedding.parent_doc_key) |parent_doc_key| try alloc.dupe(u8, parent_doc_key) else null,
        .doc_key = try alloc.dupe(u8, embedding.doc_key),
        .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
        .vector = try alloc.dupe(f32, embedding.vector),
    };
}

fn deinitDenseEmbeddingWrite(alloc: Allocator, embedding: *derived_types.DerivedDenseEmbeddingWrite) void {
    alloc.free(embedding.index_name);
    if (embedding.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
    alloc.free(embedding.doc_key);
    if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
    if (embedding.vector.len > 0) alloc.free(embedding.vector);
    embedding.* = undefined;
}

fn cloneSparseEmbeddingWrite(alloc: Allocator, embedding: derived_types.DerivedSparseEmbeddingWrite) !derived_types.DerivedSparseEmbeddingWrite {
    return .{
        .index_name = try alloc.dupe(u8, embedding.index_name),
        .doc_key = try alloc.dupe(u8, embedding.doc_key),
        .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
        .indices = try alloc.dupe(u32, embedding.indices),
        .values = try alloc.dupe(f32, embedding.values),
    };
}

fn deinitSparseEmbeddingWrite(alloc: Allocator, embedding: *derived_types.DerivedSparseEmbeddingWrite) void {
    alloc.free(embedding.index_name);
    alloc.free(embedding.doc_key);
    if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
    if (embedding.indices.len > 0) alloc.free(embedding.indices);
    if (embedding.values.len > 0) alloc.free(embedding.values);
    embedding.* = undefined;
}

fn edgeKeyAlloc(alloc: Allocator, source: []const u8, target: []const u8, edge_type: []const u8) ![]u8 {
    return try tupleKeyAlloc(alloc, &.{ source, target, edge_type });
}

fn indexDocKeyAlloc(alloc: Allocator, index_name: []const u8, doc_key: []const u8) ![]u8 {
    return try tupleKeyAlloc(alloc, &.{ index_name, doc_key });
}

fn tupleKeyAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

test "replay batcher tuple map keys preserve embedded delimiters" {
    const alloc = std.testing.allocator;

    const sparse_a = try indexDocKeyAlloc(alloc, "idx\x00doc", "a");
    defer alloc.free(sparse_a);
    const sparse_b = try indexDocKeyAlloc(alloc, "idx", "doc\x00a");
    defer alloc.free(sparse_b);
    try std.testing.expect(!std.mem.eql(u8, sparse_a, sparse_b));

    const graph_a = try edgeKeyAlloc(alloc, "src\x00dst", "edge", "kind");
    defer alloc.free(graph_a);
    const graph_b = try edgeKeyAlloc(alloc, "src", "dst\x00edge", "kind");
    defer alloc.free(graph_b);
    try std.testing.expect(!std.mem.eql(u8, graph_a, graph_b));
}

fn cloneGraphWrite(alloc: Allocator, write: types.GraphEdgeWrite) !types.GraphEdgeWrite {
    return .{
        .index_name = try alloc.dupe(u8, write.index_name),
        .source = try alloc.dupe(u8, write.source),
        .target = try alloc.dupe(u8, write.target),
        .edge_type = try alloc.dupe(u8, write.edge_type),
        .weight = write.weight,
        .created_at = write.created_at,
        .updated_at = write.updated_at,
        .metadata_json = if (write.metadata_json.len > 0) try alloc.dupe(u8, write.metadata_json) else "",
    };
}

fn deinitGraphWrite(alloc: Allocator, write: *types.GraphEdgeWrite) void {
    alloc.free(@constCast(write.index_name));
    alloc.free(@constCast(write.source));
    alloc.free(@constCast(write.target));
    alloc.free(@constCast(write.edge_type));
    if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
    write.* = undefined;
}

fn cloneGraphDelete(alloc: Allocator, delete: types.GraphEdgeDelete) !types.GraphEdgeDelete {
    return .{
        .index_name = try alloc.dupe(u8, delete.index_name),
        .source = try alloc.dupe(u8, delete.source),
        .target = try alloc.dupe(u8, delete.target),
        .edge_type = try alloc.dupe(u8, delete.edge_type),
    };
}

fn deinitGraphDelete(alloc: Allocator, delete: *types.GraphEdgeDelete) void {
    alloc.free(@constCast(delete.index_name));
    alloc.free(@constCast(delete.source));
    alloc.free(@constCast(delete.target));
    alloc.free(@constCast(delete.edge_type));
    delete.* = undefined;
}
