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

//! Transactional ownership transfer for public local results.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const artifact_ids = @import("artifact_ids.zig");
pub const Scan = struct {
    alloc: Allocator,
    include_documents: bool,
    hashes: std.ArrayListUnmanaged(types.ScanHash) = .empty,
    documents: std.ArrayListUnmanaged(types.ScanDocument) = .empty,

    pub fn deinit(collector: *@This()) void {
        for (collector.hashes.items) |*entry| entry.deinit(collector.alloc);
        collector.hashes.deinit(collector.alloc);
        for (collector.documents.items) |*document| document.deinit(collector.alloc);
        collector.documents.deinit(collector.alloc);
    }

    pub fn visit(raw_context: ?*anyopaque, entry: types.ScanVisitEntry) anyerror!void {
        const collector: *@This() = @ptrCast(@alignCast(raw_context orelse return error.InvalidArgument));
        try collector.hashes.ensureUnusedCapacity(collector.alloc, 1);
        if (collector.include_documents) try collector.documents.ensureUnusedCapacity(collector.alloc, 1);
        const hash_id = try collector.alloc.dupe(u8, entry.id);
        errdefer collector.alloc.free(hash_id);
        const row_cursor = if (entry.relational_cursor) |value| try collector.alloc.dupe(u8, value) else null;
        errdefer if (row_cursor) |value| collector.alloc.free(value);
        const json_null_fields = try types.cloneJsonNullFields(collector.alloc, entry.json_null_fields);
        errdefer {
            for (json_null_fields) |field| collector.alloc.free(field);
            collector.alloc.free(json_null_fields);
        }
        if (collector.include_documents) {
            const document_id = try collector.alloc.dupe(u8, entry.id);
            errdefer collector.alloc.free(document_id);
            const document_json = try collector.alloc.dupe(u8, entry.document_json orelse return error.InvalidState);
            errdefer collector.alloc.free(document_json);
            collector.documents.appendAssumeCapacity(.{ .id = document_id, .json = document_json });
        }
        collector.hashes.appendAssumeCapacity(.{
            .id = hash_id,
            .hash = entry.hash,
            .content_hash = entry.content_hash,
            .relational_schema_version = entry.relational_schema_version,
            .relational_cursor = row_cursor,
            .json_null_fields = json_null_fields,
        });
    }

    pub fn finish(self: *Scan) !types.ScanResult {
        var result: types.ScanResult = .{ .hashes = &.{}, .documents = &.{} };
        errdefer result.deinit(self.alloc);
        result.hashes = try self.hashes.toOwnedSlice(self.alloc);
        result.documents = try self.documents.toOwnedSlice(self.alloc);
        return result;
    }
};

pub fn externalizeArtifactWritesAlloc(alloc: Allocator, writes: []types.BatchWrite) ![]types.ArtifactWrite {
    var transferred: usize = 0;
    defer alloc.free(writes);
    errdefer for (writes[transferred..]) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    };
    const out = try alloc.alloc(types.ArtifactWrite, writes.len);
    errdefer {
        for (out[0..transferred]) |*write| write.deinit(alloc);
        alloc.free(out);
    }
    for (writes, 0..) |write, i| {
        var identity = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, write.key);
        defer identity.deinit(alloc);
        const id = try alloc.dupe(u8, identity.id);
        errdefer alloc.free(id);
        const ref = try identity.artifact_ref.?.clone(alloc);
        out[i] = .{ .id = id, .value = @constCast(write.value), .artifact_ref = ref };
        transferred += 1;
        alloc.free(@constCast(write.key));
    }
    return out;
}

/// Reserve first, then transfer only a fully initialized owned value.
pub fn appendArtifact(alloc: Allocator, out: *std.ArrayListUnmanaged(types.BatchWrite), key: []const u8, value: []const u8) !void {
    try out.ensureUnusedCapacity(alloc, 1);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    out.appendAssumeCapacity(.{ .key = owned_key, .value = owned_value });
}

pub fn appendDocument(alloc: Allocator, out: *std.ArrayListUnmanaged(types.EnrichmentDocumentWrite), key: []const u8, value: []const u8, names: []const []const u8) !void {
    try out.ensureUnusedCapacity(alloc, 1);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    const owned_names = try alloc.alloc([]u8, names.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_names[0..initialized]) |name| alloc.free(name);
        alloc.free(owned_names);
    }
    for (names, 0..) |name, i| {
        owned_names[i] = try alloc.dupe(u8, name);
        initialized += 1;
    }
    out.appendAssumeCapacity(.{ .key = owned_key, .value = owned_value, .target_index_names = owned_names });
}

pub fn appendDenseEmbeddingForConsumers(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite),
    doc_key: []const u8,
    parent_doc_key: ?[]const u8,
    artifact_key: []const u8,
    vector: []const f32,
    consumer_indexes: []const []const u8,
) !void {
    _ = parent_doc_key;
    var identity = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, artifact_key);
    defer identity.deinit(alloc);
    for (consumer_indexes) |name| {
        try out.ensureUnusedCapacity(alloc, 1);
        const owned_name = try alloc.dupe(u8, name);
        errdefer alloc.free(owned_name);
        const owned_doc = try alloc.dupe(u8, doc_key);
        errdefer alloc.free(owned_doc);
        const owned_id = try alloc.dupe(u8, identity.id);
        errdefer alloc.free(owned_id);
        var owned_ref = try identity.artifact_ref.?.clone(alloc);
        errdefer owned_ref.deinit(alloc);
        const owned_vector = try alloc.dupe(f32, vector);
        out.appendAssumeCapacity(.{ .index_name = owned_name, .doc_key = owned_doc, .artifact_id = owned_id, .artifact_ref = owned_ref, .vector = owned_vector });
    }
}

/// Unfinished lists stay with the caller. Adopted slices belong to this result
/// until every conversion succeeds and the complete result is returned.
pub fn finishEnrichments(alloc: Allocator, artifacts: *std.ArrayListUnmanaged(types.BatchWrite), documents: *std.ArrayListUnmanaged(types.EnrichmentDocumentWrite), dense: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite), failed: *std.ArrayListUnmanaged([]u8)) !types.ComputeEnrichmentsResult {
    var result: types.ComputeEnrichmentsResult = .{};
    errdefer result.deinit(alloc);
    result.artifact_writes = try externalizeArtifactWritesAlloc(alloc, try artifacts.toOwnedSlice(alloc));
    result.documents = try documents.toOwnedSlice(alloc);
    result.dense_embeddings = try dense.toOwnedSlice(alloc);
    result.failed_keys = try failed.toOwnedSlice(alloc);
    return result;
}

pub const Enrichments = struct {
    alloc: Allocator,
    artifacts: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    documents: std.ArrayListUnmanaged(types.EnrichmentDocumentWrite) = .empty,
    dense: std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite) = .empty,
    failed: std.ArrayListUnmanaged([]u8) = .empty,
    pub fn deinit(self: *Enrichments) void {
        for (self.artifacts.items) |write| {
            self.alloc.free(@constCast(write.key));
            self.alloc.free(@constCast(write.value));
        }
        self.artifacts.deinit(self.alloc);
        for (self.documents.items) |*doc| doc.deinit(self.alloc);
        self.documents.deinit(self.alloc);
        for (self.dense.items) |*embedding| embedding.deinit(self.alloc);
        self.dense.deinit(self.alloc);
        for (self.failed.items) |key| self.alloc.free(key);
        self.failed.deinit(self.alloc);
    }
    pub fn finish(self: *Enrichments) !types.ComputeEnrichmentsResult {
        return finishEnrichments(self.alloc, &self.artifacts, &self.documents, &self.dense, &self.failed);
    }
};

test "result collectors release scan rows across every allocation failure" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var scan: Scan = .{ .alloc = alloc, .include_documents = true };
            defer scan.deinit();
            for (0..3) |_| try Scan.visit(&scan, .{ .id = "doc", .hash = 7, .relational_cursor = "cursor", .json_null_fields = &.{"field"}, .document_json = "{\"field\":null}" });
            var result = try scan.finish();
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 3), result.hashes.len);
            try std.testing.expectEqualStrings("cursor", result.hashes[0].relational_cursor.?);
            try std.testing.expectEqual(@as(usize, 3), result.documents.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "result collectors adopt enrichment arrays atomically across every allocation failure" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var collector: Enrichments = .{ .alloc = alloc };
            defer collector.deinit();
            for (0..3) |_| {
                const key = try @import("../internal_keys.zig").artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
                defer alloc.free(key);
                try appendArtifact(alloc, &collector.artifacts, key, "caption");
            }
            try appendDocument(alloc, &collector.documents, "doc", "{}", &.{ "text", "other" });
            const artifact_key = try @import("../internal_keys.zig").artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
            defer alloc.free(artifact_key);
            try appendDenseEmbeddingForConsumers(alloc, &collector.dense, "doc", null, artifact_key, &.{ 1, 2, 3 }, &.{"dense"});
            try collector.failed.ensureUnusedCapacity(alloc, 1);
            collector.failed.appendAssumeCapacity(try alloc.dupe(u8, "failed"));
            var result = try collector.finish();
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 3), result.artifact_writes.len);
            try std.testing.expectEqualStrings("caption", result.artifact_writes[0].value);
            try std.testing.expectEqual(@as(usize, 1), result.documents.len);
            try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
            try std.testing.expectEqual(@as(usize, 1), result.failed_keys.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

/// Owns every partially constructed extraction and every transferred array.
pub const Extraction = struct {
    alloc: Allocator,
    cleaned: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    dense: std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite) = .empty,
    sparse: std.ArrayListUnmanaged(types.EnrichmentSparseEmbeddingWrite) = .empty,
    graph: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
    pub fn deinit(self: *Extraction) void {
        for (self.cleaned.items) |write| {
            self.alloc.free(@constCast(write.key));
            self.alloc.free(@constCast(write.value));
        }
        self.cleaned.deinit(self.alloc);
        for (self.dense.items) |*write| write.deinit(self.alloc);
        self.dense.deinit(self.alloc);
        for (self.sparse.items) |*write| write.deinit(self.alloc);
        self.sparse.deinit(self.alloc);
        for (self.graph.items) |*write| write.deinit(self.alloc);
        self.graph.deinit(self.alloc);
    }
    pub fn append(self: *Extraction, key: []const u8, extracted: anytype) !void {
        const alloc = self.alloc;
        if (extracted.cleaned_value) |value| try appendArtifact(alloc, &self.cleaned, key, value);
        for (extracted.dense_embeddings) |embedding| {
            try self.dense.ensureUnusedCapacity(alloc, 1);
            var identity = if (embedding.artifact_key) |artifact_key| try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, artifact_key) else null;
            defer if (identity) |*owned| owned.deinit(alloc);
            const name = try alloc.dupe(u8, embedding.index_name);
            errdefer alloc.free(name);
            const doc = try alloc.dupe(u8, embedding.doc_key);
            errdefer alloc.free(doc);
            const id = if (identity) |owned| try alloc.dupe(u8, owned.id) else null;
            errdefer if (id) |owned| alloc.free(owned);
            var ref = if (identity) |owned| try owned.artifact_ref.?.clone(alloc) else null;
            errdefer if (ref) |*owned| owned.deinit(alloc);
            const vector = try alloc.dupe(f32, embedding.vector);
            self.dense.appendAssumeCapacity(.{ .index_name = name, .doc_key = doc, .artifact_id = id, .artifact_ref = ref, .vector = vector });
        }
        for (extracted.sparse_embeddings) |embedding| {
            try self.sparse.ensureUnusedCapacity(alloc, 1);
            const name = try alloc.dupe(u8, embedding.index_name);
            errdefer alloc.free(name);
            const doc = try alloc.dupe(u8, embedding.doc_key);
            errdefer alloc.free(doc);
            const indices = try alloc.dupe(u32, embedding.indices);
            errdefer alloc.free(indices);
            const values = try alloc.dupe(f32, embedding.values);
            self.sparse.appendAssumeCapacity(.{ .index_name = name, .doc_key = doc, .indices = indices, .values = values });
        }
        for (extracted.graph_writes) |write| {
            try self.graph.ensureUnusedCapacity(alloc, 1);
            self.graph.appendAssumeCapacity(try write.cloneAlloc(alloc));
        }
    }
    pub fn finish(self: *Extraction) !types.ExtractEnrichmentsResult {
        var result: types.ExtractEnrichmentsResult = .{};
        errdefer result.deinit(self.alloc);
        result.cleaned_writes = try self.cleaned.toOwnedSlice(self.alloc);
        result.dense_embeddings = try self.dense.toOwnedSlice(self.alloc);
        result.sparse_embeddings = try self.sparse.toOwnedSlice(self.alloc);
        result.graph_writes = try self.graph.toOwnedSlice(self.alloc);
        return result;
    }
};

test "result collectors release complete graph identity and extraction transfers on OOM" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var collector: Extraction = .{ .alloc = alloc };
            defer collector.deinit();
            const mapper = @import("document_mapper.zig");
            var extracted: mapper.ExtractedWrite = .{ .cleaned_value = null, .graph_writes = &.{}, .mentioned_graph_indexes = &.{}, .dense_embeddings = &.{}, .sparse_embeddings = &.{} };
            const artifact_key = try @import("../internal_keys.zig").artifactNamedPrefixAlloc(alloc, "doc", "asset", "embedding");
            defer alloc.free(artifact_key);
            const dense = [_]mapper.DenseEmbeddingWrite{.{ .index_name = @constCast("dense"), .doc_key = @constCast("doc"), .artifact_key = artifact_key, .vector = @constCast(&[_]f32{ 1, 2 }) }};
            const sparse = [_]mapper.SparseEmbeddingWrite{.{ .index_name = @constCast("sparse"), .doc_key = @constCast("doc"), .indices = @constCast(&[_]u32{1}), .values = @constCast(&[_]f32{2}) }};
            const graph = [_]types.GraphEdgeWrite{.{ .index_name = "graph", .source = "a", .target = "b", .edge_type = "rel", .edge_id = "identity", .owner_document = "producer", .owner = "legacy", .metadata_json = "{}" }};
            extracted.cleaned_value = @constCast("{}");
            extracted.dense_embeddings = @constCast(&dense);
            extracted.sparse_embeddings = @constCast(&sparse);
            extracted.graph_writes = @constCast(&graph);
            for (0..3) |_| try collector.append("doc", extracted);
            var result = try collector.finish();
            defer result.deinit(alloc);
            try std.testing.expectEqualStrings("identity", result.graph_writes[0].edge_id);
            try std.testing.expectEqualStrings("producer", result.graph_writes[0].owner_document);
            try std.testing.expectEqualStrings("embedding", result.dense_embeddings[0].artifact_ref.?.name);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}
