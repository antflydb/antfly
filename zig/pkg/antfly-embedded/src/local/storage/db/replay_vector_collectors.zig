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

//! Local replay vector collection. Result owners distinguish cloned document
//! identities from borrowed vector payloads and retain the original allocation
//! length after tombstone filtering. Callers retain replay/apply admission.
const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const mapper = @import("document_mapper.zig");
const derived_types = @import("derived/derived_types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const docstore_mod = @import("../docstore.zig");
const artifact_ids = @import("artifact_ids.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_store = @import("relational_store.zig");
const lookup_key_scratch = @import("lookup_key_scratch.zig");
const document_read_scratch = @import("document_read_scratch.zig");
const sparse_mod = if (builtin.os.tag == .freestanding) @import("sparse_stub.zig") else @import("../../sparse/sparse.zig");
const document_collectors = @import("document_collectors.zig");
const CollectDocumentWritesOptions = document_collectors.CollectDocumentWritesOptions;
const replayDocumentKeyInRange = document_collectors.replayDocumentKeyInRange;
const replayDocumentIsDurablyDeleted = document_collectors.replayDocumentIsDurablyDeleted;
fn monotonicTimeNs() u64 {
    return @import("antfly_platform").time.monotonicNs();
}

pub const CollectSparseFieldWritesProfile = struct {
    scan_ns: u64 = 0,
    sort_ns: u64 = 0,
    read_ns: u64 = 0,
    extract_ns: u64 = 0,
    input_documents: usize = 0,
    pending_documents: usize = 0,
    output_writes: usize = 0,
    missing_required: usize = 0,
    inline_hits: usize = 0,
    store_hits: usize = 0,
    skipped_without_vector: usize = 0,
};

pub fn filterDeletedEmbeddingWrites(owned: anytype, deleted_keys: []const []const u8) !void {
    if (deleted_keys.len == 0 or owned.writes.len == 0) return;
    var deleted = std.StringHashMapUnmanaged(void).empty;
    defer deleted.deinit(owned.alloc);
    for (deleted_keys) |key| try deleted.put(owned.alloc, key, {});
    var kept: usize = 0;
    for (owned.writes) |write| {
        if (write.artifact_key) |key| if (deleted.contains(key)) {
            if (comptime @hasField(@TypeOf(owned.*), "owns_doc_keys")) {
                if (owned.owns_doc_keys) {
                    owned.alloc.free(@constCast(write.doc_key));
                    if (write.parent_doc_key) |parent| owned.alloc.free(@constCast(parent));
                }
            }
            continue;
        };
        owned.writes[kept] = write;
        kept += 1;
    }
    // Retain allocation_len for deinit; compaction needs no replacement buffer.
    owned.writes = owned.writes[0..kept];
}

pub const OwnedDenseEmbeddingWrites = struct {
    alloc: Allocator,
    owns_doc_keys: bool = false,
    writes: []mapper.DenseEmbeddingWrite = &.{},
    allocation_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.owns_doc_keys) {
            for (self.writes) |write| {
                self.alloc.free(@constCast(write.doc_key));
                if (write.parent_doc_key) |parent_doc_key| self.alloc.free(@constCast(parent_doc_key));
            }
        }
        if (self.allocation_len > 0) self.alloc.free(self.writes.ptr[0..self.allocation_len]);
        self.* = undefined;
    }
};

pub const OwnedEmbeddingArtifactWriteIdentity = struct {
    doc_key: []u8,
    parent_doc_key: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        if (self.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
        self.* = undefined;
    }
};

pub fn decodeEmbeddingArtifactWriteIdentityAlloc(
    alloc: Allocator,
    artifact_key: []const u8,
    expected_embedding_name: []const u8,
) !?OwnedEmbeddingArtifactWriteIdentity {
    if (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) |maybe_identity| {
        var identity = maybe_identity orelse return null;
        defer identity.deinit(alloc);
        if (!std.mem.eql(u8, identity.embedding_name, expected_embedding_name)) return null;

        const keys = identity.takeDocumentKeys();
        return .{ .doc_key = keys.doc_key, .parent_doc_key = keys.parent_doc_key };
    } else |err| switch (err) {
        error.InvalidInternalUserKey => {},
        else => return err,
    }

    if (try internal_keys.parseEmbeddingArtifactKeyView(artifact_key)) |identity| {
        if (!std.mem.eql(u8, identity.artifact_name, expected_embedding_name)) return null;
        return .{
            .doc_key = try alloc.dupe(u8, identity.doc_key),
        };
    }

    return null;
}

pub fn decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    index_ref: index_manager_mod.ManagedIndexRef,
    artifact_key: []const u8,
) !?OwnedEmbeddingArtifactWriteIdentity {
    if (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) |maybe_identity| {
        var identity = maybe_identity orelse return null;
        defer identity.deinit(alloc);
        if (!managedIndexConsumesEmbeddingName(index_manager, index_ref, identity.embedding_name)) return null;

        const keys = identity.takeDocumentKeys();
        return .{ .doc_key = keys.doc_key, .parent_doc_key = keys.parent_doc_key };
    } else |err| switch (err) {
        error.InvalidInternalUserKey => {},
        else => return err,
    }

    if (try internal_keys.parseEmbeddingArtifactKeyView(artifact_key)) |identity| {
        if (!managedIndexConsumesEmbeddingName(index_manager, index_ref, identity.artifact_name)) return null;
        return .{ .doc_key = try alloc.dupe(u8, identity.doc_key) };
    }
    return null;
}

pub const OwnedSparseEmbeddingWrites = struct {
    alloc: Allocator,
    owned_doc_keys: []const []const u8 = &.{},
    writes: []mapper.SparseEmbeddingWrite = &.{},
    allocation_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.owned_doc_keys) |doc_key| self.alloc.free(@constCast(doc_key));
        if (self.owned_doc_keys.len > 0) self.alloc.free(self.owned_doc_keys);
        if (self.allocation_len > 0) self.alloc.free(self.writes.ptr[0..self.allocation_len]);
        self.* = undefined;
    }
};

pub const OwnedSparseFieldWrites = struct {
    alloc: Allocator,
    items: []sparse_mod.SparseWrite = &.{},
    missing_required: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.items) |item| {
            self.alloc.free(@constCast(item.vec.indices));
            self.alloc.free(@constCast(item.vec.values));
        }
        if (self.items.len > 0) self.alloc.free(self.items);
        self.* = undefined;
    }
};

pub fn collectSparseFieldWritesProfiled(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: ?*index_manager_mod.IndexManager,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
    field_name: []const u8,
    opts: CollectDocumentWritesOptions,
    profile: ?*CollectSparseFieldWritesProfile,
) !OwnedSparseFieldWrites {
    const PendingDocumentWrite = struct {
        doc_key: []const u8,
        store_key: []const u8,
        inline_value: ?[]const u8,
    };

    var lookup_keys = lookup_key_scratch.Scratch.init(alloc, documents.len);
    defer lookup_keys.deinit();
    // Temporary descriptors cannot escape the synchronous read/apply below.
    var descriptor_buffer_storage: [4096]u8 align(@alignOf(std.c.max_align_t)) = undefined;
    var descriptor_buffer: std.heap.BufferFirstAllocator = .init(&descriptor_buffer_storage, alloc);
    const descriptor_alloc = descriptor_buffer.allocator();
    var pending = std.ArrayListUnmanaged(PendingDocumentWrite).empty;
    defer {
        pending.deinit(descriptor_alloc);
    }
    // A bounded small batch fits entirely on the stack. Reserve once so
    // append does not repeatedly enter the capacity-growth path. Larger
    // batches keep lazy growth proportional to the selected documents.
    if (documents.len <= 64) {
        comptime std.debug.assert(64 * @sizeOf(PendingDocumentWrite) <= 4096);
        try pending.ensureTotalCapacityPrecise(descriptor_alloc, documents.len);
    }

    var writes = std.ArrayListUnmanaged(sparse_mod.SparseWrite).empty;
    errdefer {
        for (writes.items) |item| {
            alloc.free(@constCast(item.vec.indices));
            alloc.free(@constCast(item.vec.values));
        }
        writes.deinit(alloc);
    }

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var missing_required: usize = 0;
    const trust_inline = opts.prefer_available_inline_values or
        if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
            store.nextReplaySequence(sequence + 1) == sequence + 1
        else
            false;

    if (profile) |p| p.input_documents = documents.len;
    const scan_start_ns = if (profile != null) monotonicTimeNs() else 0;
    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
        if (opts.skip_doc_keys) |skip_doc_keys| {
            if (skip_doc_keys.contains(doc.key)) continue;
        }
        if (trust_inline and doc.cleaned_value != null) {
            const extract_start_ns = if (profile != null) monotonicTimeNs() else 0;
            if (try mapper.extractSparseVectorField(alloc, doc.cleaned_value.?, field_name)) |raw_sparse_vec| {
                var sparse_vec = raw_sparse_vec;
                writes.append(alloc, .{
                    .doc_id = doc.key,
                    .vec = .{
                        .indices = sparse_vec.indices,
                        .values = sparse_vec.values,
                    },
                }) catch |err| {
                    sparse_vec.deinit(alloc);
                    return err;
                };
                if (profile) |p| p.output_writes += 1;
            } else if (profile) |p| {
                p.skipped_without_vector += 1;
            }
            if (profile) |p| {
                p.extract_ns += monotonicTimeNs() - extract_start_ns;
                p.inline_hits += 1;
            }
            continue;
        }
        try pending.append(descriptor_alloc, .{
            .doc_key = doc.key,
            .store_key = try lookup_keys.key(doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
    }
    if (profile) |p| {
        p.scan_ns = monotonicTimeNs() - scan_start_ns -| p.extract_ns;
        p.pending_documents = pending.items.len;
    }

    if (pending.items.len == 0) {
        return .{
            .alloc = alloc,
            .items = try writes.toOwnedSlice(alloc),
            .missing_required = missing_required,
        };
    }

    const SortContext = struct {};
    const sort_start_ns = if (profile != null) monotonicTimeNs() else 0;
    std.mem.sort(PendingDocumentWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingDocumentWrite, rhs: PendingDocumentWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);
    if (profile) |p| p.sort_ns = monotonicTimeNs() - sort_start_ns;

    var read_scratch = try document_read_scratch.Scratch.init(descriptor_alloc, pending.items.len);
    defer read_scratch.deinit();
    const read_keys = read_scratch.keys;
    const read_values = read_scratch.values;

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
    }
    const read_start_ns = if (profile != null) monotonicTimeNs() else 0;
    try txn.getManySorted(read_keys, read_values);
    if (profile) |p| p.read_ns = monotonicTimeNs() - read_start_ns;

    for (pending.items, 0..) |item, i| {
        var owned_logical_value: ?[]u8 = null;
        defer if (owned_logical_value) |owned| alloc.free(owned);
        const value = if (read_values[i]) |store_value| blk: {
            if (profile) |p| p.store_hits += 1;
            // Numeric outputs own their storage. Ordinary JSON only needs to
            // remain borrowed until extraction completes inside this txn.
            if (!internal_keys.isRelationalRowKey(item.store_key)) break :blk store_value;
            const logical = if (index_manager) |manager|
                try manager.materializeStoredValueAlloc(alloc, item.store_key, store_value)
            else
                try relational_store.materializeStoredValueAlloc(alloc, item.store_key, store_value);
            owned_logical_value = logical;
            break :blk logical;
        } else if (item.inline_value) |inline_value| blk: {
            if (profile) |p| p.inline_hits += 1;
            break :blk inline_value;
        } else {
            if (try replayDocumentIsDurablyDeleted(alloc, &txn, item.doc_key)) continue;
            missing_required += 1;
            continue;
        };
        const extract_start_ns = if (profile != null) monotonicTimeNs() else 0;
        if (try mapper.extractSparseVectorField(alloc, value, field_name)) |raw_sparse_vec| {
            var sparse_vec = raw_sparse_vec;
            writes.append(alloc, .{
                .doc_id = item.doc_key,
                .vec = .{
                    .indices = sparse_vec.indices,
                    .values = sparse_vec.values,
                },
            }) catch |err| {
                sparse_vec.deinit(alloc);
                return err;
            };
            if (profile) |p| p.output_writes += 1;
        } else if (profile) |p| {
            p.skipped_without_vector += 1;
        }
        if (profile) |p| p.extract_ns += monotonicTimeNs() - extract_start_ns;
    }
    if (profile) |p| {
        p.missing_required = missing_required;
        p.output_writes = writes.items.len;
    }

    return .{
        .alloc = alloc,
        .items = try writes.toOwnedSlice(alloc),
        .missing_required = missing_required,
    };
}

pub fn collectDenseEmbeddingWrites(alloc: Allocator, embeddings: []const derived_types.DerivedDenseEmbeddingWrite, index_name: []const u8) ![]mapper.DenseEmbeddingWrite {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    defer filtered.deinit(alloc);

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .parent_doc_key = embedding.parent_doc_key,
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .vector = if (embedding.artifact_key != null) &.{} else @constCast(embedding.vector),
        });
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn collectDenseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedDenseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    errdefer {
        for (filtered.items) |write| {
            alloc.free(@constCast(write.doc_key));
            if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
        }
        filtered.deinit(alloc);
    }

    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .dense_vector };
    for (artifact_keys) |artifact_key| {
        var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
        var identity_transferred = false;
        errdefer if (!identity_transferred) identity.deinit(alloc);
        try filtered.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = identity.doc_key,
            .parent_doc_key = identity.parent_doc_key,
            .artifact_key = @constCast(artifact_key),
            .vector = &.{},
        });
        identity_transferred = true;
    }

    const writes = try filtered.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn appendDenseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    out: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !void {
    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .dense_vector };
    for (artifact_keys) |artifact_key| {
        var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
        var identity_transferred = false;
        errdefer if (!identity_transferred) identity.deinit(alloc);
        try out.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = identity.doc_key,
            .parent_doc_key = identity.parent_doc_key,
            .artifact_key = @constCast(artifact_key),
            .vector = &.{},
        });
        identity_transferred = true;
    }
}

pub fn collectDenseEmbeddingWritesForBatch(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    embeddings: []const derived_types.DerivedDenseEmbeddingWrite,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedDenseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    errdefer {
        for (filtered.items) |write| {
            alloc.free(@constCast(write.doc_key));
            if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
        }
        filtered.deinit(alloc);
    }

    const uses_artifact_members = index_manager.denseIndexUsesArtifactMembers(index_name);
    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .dense_vector };
    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        if (uses_artifact_members) {
            const artifact_key = embedding.artifact_key orelse continue;
            var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
            var identity_transferred = false;
            errdefer if (!identity_transferred) identity.deinit(alloc);
            try filtered.append(alloc, .{
                .index_name = @constCast(embedding.index_name),
                .doc_key = identity.doc_key,
                .parent_doc_key = identity.parent_doc_key,
                .artifact_key = @constCast(artifact_key),
                .vector = &.{},
            });
            identity_transferred = true;
            continue;
        }
        const doc_key = try alloc.dupe(u8, embedding.doc_key);
        errdefer alloc.free(doc_key);
        var parent_doc_key = if (embedding.parent_doc_key) |parent_key| try alloc.dupe(u8, parent_key) else null;
        errdefer if (parent_doc_key) |owned_parent| alloc.free(owned_parent);
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = doc_key,
            .parent_doc_key = parent_doc_key,
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .vector = if (embedding.artifact_key != null) &.{} else @constCast(embedding.vector),
        });
        parent_doc_key = null;
    }
    try appendDenseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, artifact_keys, index_name);

    const writes = try filtered.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn collectSparseEmbeddingWrites(alloc: Allocator, embeddings: []const derived_types.DerivedSparseEmbeddingWrite, index_name: []const u8) ![]mapper.SparseEmbeddingWrite {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    defer filtered.deinit(alloc);

    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .indices = @constCast(embedding.indices),
            .values = @constCast(embedding.values),
        });
    }

    return try filtered.toOwnedSlice(alloc);
}

pub fn collectSparseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedSparseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    var owned_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (owned_doc_keys.items) |doc_key| alloc.free(@constCast(doc_key));
        owned_doc_keys.deinit(alloc);
        filtered.deinit(alloc);
    }

    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .sparse_vector };
    for (artifact_keys) |artifact_key| {
        var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
        defer identity.deinit(alloc);
        var doc_key = identity.doc_key;
        try filtered.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = doc_key,
            .artifact_key = @constCast(artifact_key),
            .indices = &.{},
            .values = &.{},
        });
        try owned_doc_keys.append(alloc, doc_key);
        identity.doc_key = identity.doc_key[0..0];
        doc_key = doc_key[0..0];
    }

    const writes = try filtered.toOwnedSlice(alloc);
    errdefer if (writes.len > 0) alloc.free(writes);
    const owned_keys = try owned_doc_keys.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owned_doc_keys = owned_keys,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn appendSparseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    out: *std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite),
    owned_doc_keys: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !void {
    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .sparse_vector };
    for (artifact_keys) |artifact_key| {
        var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
        defer identity.deinit(alloc);
        var doc_key = identity.doc_key;
        try out.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = doc_key,
            .artifact_key = @constCast(artifact_key),
            .indices = &.{},
            .values = &.{},
        });
        try owned_doc_keys.append(alloc, doc_key);
        identity.doc_key = identity.doc_key[0..0];
        doc_key = doc_key[0..0];
    }
}

pub fn collectSparseEmbeddingWritesForBatch(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    embeddings: []const derived_types.DerivedSparseEmbeddingWrite,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedSparseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    var owned_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (owned_doc_keys.items) |doc_key| alloc.free(@constCast(doc_key));
        owned_doc_keys.deinit(alloc);
        filtered.deinit(alloc);
    }

    const uses_artifact_members = index_manager.sparseIndexUsesArtifactMembers(index_name);
    const index_ref = index_manager_mod.ManagedIndexRef{ .name = index_name, .kind = .sparse_vector };
    for (embeddings) |embedding| {
        if (!std.mem.eql(u8, embedding.index_name, index_name)) continue;
        if (uses_artifact_members) {
            const artifact_key = embedding.artifact_key orelse continue;
            var identity = (try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, index_manager, index_ref, artifact_key)) orelse continue;
            defer identity.deinit(alloc);
            var doc_key = identity.doc_key;
            try filtered.append(alloc, .{
                .index_name = @constCast(embedding.index_name),
                .doc_key = doc_key,
                .artifact_key = @constCast(artifact_key),
                .indices = &.{},
                .values = &.{},
            });
            try owned_doc_keys.append(alloc, doc_key);
            identity.doc_key = identity.doc_key[0..0];
            doc_key = doc_key[0..0];
            continue;
        }
        try filtered.append(alloc, .{
            .index_name = @constCast(embedding.index_name),
            .doc_key = @constCast(embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| @constCast(artifact_key) else null,
            .indices = @constCast(embedding.indices),
            .values = @constCast(embedding.values),
        });
    }
    try appendSparseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, &owned_doc_keys, artifact_keys, index_name);

    const writes = try filtered.toOwnedSlice(alloc);
    errdefer if (writes.len > 0) alloc.free(writes);
    const owned_keys = try owned_doc_keys.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owned_doc_keys = owned_keys,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn denseEmbeddingDocKeySet(
    alloc: Allocator,
    embeddings: []const mapper.DenseEmbeddingWrite,
) !std.StringHashMapUnmanaged(void) {
    var set = std.StringHashMapUnmanaged(void){};
    errdefer set.deinit(alloc);
    for (embeddings) |embedding| {
        try set.put(alloc, embedding.doc_key, {});
    }
    return set;
}

pub fn sparseEmbeddingDocKeySet(
    alloc: Allocator,
    embeddings: []const mapper.SparseEmbeddingWrite,
) !std.StringHashMapUnmanaged(void) {
    var set = std.StringHashMapUnmanaged(void){};
    errdefer set.deinit(alloc);
    for (embeddings) |embedding| {
        try set.put(alloc, embedding.doc_key, {});
    }
    return set;
}

pub fn collectDenseReplayDeleteKeys(alloc: Allocator, deleted: []const []const u8, replacements: []const []const u8, overwritten: []const []const u8) ![]const []const u8 {
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer keys.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for ([_][]const []const u8{ deleted, replacements, overwritten }) |source| {
        for (source) |key| try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, key);
    }
    return try keys.toOwnedSlice(alloc);
}

fn managedIndexConsumesEmbeddingName(
    index_manager: *index_manager_mod.IndexManager,
    index_ref: index_manager_mod.ManagedIndexRef,
    embedding_name: []const u8,
) bool {
    return switch (index_ref.kind) {
        .dense_vector => index_manager.denseIndexConsumesEmbedding(index_ref.name, embedding_name),
        .sparse_vector => index_manager.sparseIndexConsumesEmbedding(index_ref.name, embedding_name),
        else => false,
    };
}

fn appendUniqueBorrowedKeyWithSet(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    seen: *std.StringHashMapUnmanaged(void),
    key: []const u8,
) !void {
    if (key.len == 0) return;
    // Preserve the normal insertion/probing path. At capacity, getOrPut can
    // fail while trying to grow even for an existing key; a duplicate still
    // needs no additional storage and may succeed under that pressure.
    const entry = seen.getOrPut(alloc, key) catch |err| {
        if (seen.contains(key)) return;
        return err;
    };
    if (entry.found_existing) return;
    errdefer _ = seen.remove(key);
    try out.append(alloc, key);
}

test "vector replay tombstones retire owned keys and preserve allocation length through compaction" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            const buffer = try alloc.alloc(mapper.DenseEmbeddingWrite, 2);
            var owned: OwnedDenseEmbeddingWrites = .{ .alloc = alloc, .owns_doc_keys = true, .writes = buffer[0..0], .allocation_len = buffer.len };
            defer owned.deinit();
            for ([_][]const u8{ "retired", "live" }, 0..) |key, i| {
                const doc_key = try alloc.dupe(u8, key);
                buffer[i] = .{ .index_name = @constCast("index"), .doc_key = doc_key, .artifact_key = @constCast(key), .vector = &.{} };
                owned.writes = buffer[0 .. i + 1];
            }
            try filterDeletedEmbeddingWrites(&owned, &.{"retired"});
            try std.testing.expectEqual(@as(usize, 2), owned.allocation_len);
            try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
            try std.testing.expectEqualStrings("live", owned.writes[0].doc_key);
            try filterDeletedEmbeddingWrites(&owned, &.{"live"});
            try std.testing.expectEqual(@as(usize, 0), owned.writes.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
