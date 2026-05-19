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

pub const GeneratedEnrichmentKind = enum {
    dense_embedding,
    sparse_embedding,
    chunk_text,
};

pub const GeneratedEnrichmentRequest = struct {
    kind: GeneratedEnrichmentKind,
    index_name: []const u8,
    artifact_name: []const u8 = "",
    embedding_name: []const u8 = "",
    doc_key: []const u8,
    source_field: []const u8,
    /// Handlebars template to render document fields for embedding.
    /// When non-empty, the full document is rendered through this template
    /// instead of extracting a single source_field.
    source_template: []const u8 = "",
    expected_dims: u32 = 0,
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",
};

pub const GeneratedEnrichmentRef = struct {
    kind: GeneratedEnrichmentKind,
    index_name: []const u8,
    artifact_name: []const u8 = "",
    embedding_name: []const u8 = "",
    doc_key: []const u8,
};

pub const GeneratedEnrichmentRequestList = []const GeneratedEnrichmentRequest;

pub const LeaseRecord = struct {
    owner_id: []const u8,
    expires_at_ms: u64,
};

pub fn freeGeneratedRequest(alloc: Allocator, request: GeneratedEnrichmentRequest) void {
    alloc.free(request.index_name);
    if (request.artifact_name.len > 0) alloc.free(request.artifact_name);
    if (request.embedding_name.len > 0) alloc.free(request.embedding_name);
    alloc.free(request.doc_key);
    alloc.free(request.source_field);
    if (request.source_template.len > 0) alloc.free(request.source_template);
    if (request.chunker_json.len > 0) alloc.free(request.chunker_json);
}

pub fn cloneGeneratedRequest(alloc: Allocator, request: GeneratedEnrichmentRequest) !GeneratedEnrichmentRequest {
    return .{
        .kind = request.kind,
        .index_name = try alloc.dupe(u8, request.index_name),
        .artifact_name = if (request.artifact_name.len > 0) try alloc.dupe(u8, request.artifact_name) else "",
        .embedding_name = if (request.embedding_name.len > 0) try alloc.dupe(u8, request.embedding_name) else "",
        .doc_key = try alloc.dupe(u8, request.doc_key),
        .source_field = try alloc.dupe(u8, request.source_field),
        .source_template = if (request.source_template.len > 0) try alloc.dupe(u8, request.source_template) else "",
        .expected_dims = request.expected_dims,
        .chunk_size = request.chunk_size,
        .chunk_overlap = request.chunk_overlap,
        .chunker_json = if (request.chunker_json.len > 0) try alloc.dupe(u8, request.chunker_json) else "",
    };
}

pub fn deinitGeneratedRequests(alloc: Allocator, requests: []const GeneratedEnrichmentRequest) void {
    for (requests) |request| freeGeneratedRequest(alloc, request);
    alloc.free(requests);
}

pub fn freeGeneratedRef(alloc: Allocator, request: GeneratedEnrichmentRef) void {
    alloc.free(request.index_name);
    if (request.artifact_name.len > 0) alloc.free(request.artifact_name);
    if (request.embedding_name.len > 0) alloc.free(request.embedding_name);
    alloc.free(request.doc_key);
}

pub fn cloneGeneratedRef(alloc: Allocator, request: GeneratedEnrichmentRef) !GeneratedEnrichmentRef {
    return .{
        .kind = request.kind,
        .index_name = try alloc.dupe(u8, request.index_name),
        .artifact_name = if (request.artifact_name.len > 0) try alloc.dupe(u8, request.artifact_name) else "",
        .embedding_name = if (request.embedding_name.len > 0) try alloc.dupe(u8, request.embedding_name) else "",
        .doc_key = try alloc.dupe(u8, request.doc_key),
    };
}

pub fn deinitGeneratedRefs(alloc: Allocator, requests: []const GeneratedEnrichmentRef) void {
    for (requests) |request| freeGeneratedRef(alloc, request);
    alloc.free(requests);
}

pub fn cloneGeneratedRequests(alloc: Allocator, requests: []const GeneratedEnrichmentRequest) ![]GeneratedEnrichmentRequest {
    const cloned = try alloc.alloc(GeneratedEnrichmentRequest, requests.len);
    var initialized: usize = 0;
    errdefer {
        deinitGeneratedRequests(alloc, cloned[0..initialized]);
        alloc.free(cloned);
    }

    for (requests, 0..) |request, i| {
        cloned[i] = try cloneGeneratedRequest(alloc, request);
        initialized += 1;
    }
    return cloned;
}

pub fn cloneGeneratedRefs(alloc: Allocator, requests: []const GeneratedEnrichmentRef) ![]GeneratedEnrichmentRef {
    const cloned = try alloc.alloc(GeneratedEnrichmentRef, requests.len);
    var initialized: usize = 0;
    errdefer {
        deinitGeneratedRefs(alloc, cloned[0..initialized]);
        alloc.free(cloned);
    }

    for (requests, 0..) |request, i| {
        cloned[i] = try cloneGeneratedRef(alloc, request);
        initialized += 1;
    }
    return cloned;
}

pub fn requestToRef(alloc: Allocator, request: GeneratedEnrichmentRequest) !GeneratedEnrichmentRef {
    return cloneGeneratedRef(alloc, .{
        .kind = request.kind,
        .index_name = request.index_name,
        .artifact_name = request.artifact_name,
        .embedding_name = request.embedding_name,
        .doc_key = request.doc_key,
    });
}

pub fn requestArtifactName(request: GeneratedEnrichmentRequest) []const u8 {
    return if (request.artifact_name.len > 0) request.artifact_name else request.index_name;
}

pub fn requestEmbeddingName(request: GeneratedEnrichmentRequest) []const u8 {
    return if (request.embedding_name.len > 0) request.embedding_name else request.index_name;
}

pub fn refArtifactName(request: GeneratedEnrichmentRef) []const u8 {
    return if (request.artifact_name.len > 0) request.artifact_name else request.index_name;
}

pub fn refEmbeddingName(request: GeneratedEnrichmentRef) []const u8 {
    return if (request.embedding_name.len > 0) request.embedding_name else request.index_name;
}

pub fn requestMatchesRef(request: GeneratedEnrichmentRequest, ref: GeneratedEnrichmentRef) bool {
    if (request.kind != ref.kind) return false;
    if (!std.mem.eql(u8, request.doc_key, ref.doc_key)) return false;
    if (!std.mem.eql(u8, requestArtifactName(request), refArtifactName(ref))) return false;
    return switch (request.kind) {
        .chunk_text => true,
        .dense_embedding, .sparse_embedding => std.mem.eql(u8, requestEmbeddingName(request), refEmbeddingName(ref)),
    };
}

pub fn cloneLeaseRecord(alloc: Allocator, record: LeaseRecord) !LeaseRecord {
    return .{
        .owner_id = try alloc.dupe(u8, record.owner_id),
        .expires_at_ms = record.expires_at_ms,
    };
}

pub fn deinitLeaseRecord(alloc: Allocator, record: *LeaseRecord) void {
    alloc.free(record.owner_id);
    record.* = undefined;
}

test "generated enrichment request clone round trip" {
    const alloc = std.testing.allocator;

    const cloned = try cloneGeneratedRequests(alloc, &.{
        .{
            .kind = .dense_embedding,
            .index_name = "dv_v1",
            .artifact_name = "body_chunks_v1",
            .embedding_name = "body_dense_v1",
            .doc_key = "doc:a",
            .source_field = "body",
            .source_template = "{{title}} {{body}}",
            .expected_dims = 768,
            .chunk_size = 512,
            .chunk_overlap = 64,
            .chunker_json = "{\"provider\":\"antfly\",\"text\":{\"target_tokens\":512,\"overlap_tokens\":64}}",
        },
    });
    defer deinitGeneratedRequests(alloc, cloned);

    try std.testing.expectEqual(@as(usize, 1), cloned.len);
    try std.testing.expectEqualStrings("dv_v1", cloned[0].index_name);
    try std.testing.expectEqualStrings("body_chunks_v1", cloned[0].artifact_name);
    try std.testing.expectEqualStrings("body_dense_v1", cloned[0].embedding_name);
    try std.testing.expectEqualStrings("doc:a", cloned[0].doc_key);
    try std.testing.expectEqualStrings("body", cloned[0].source_field);
    try std.testing.expectEqualStrings("{{title}} {{body}}", cloned[0].source_template);
    try std.testing.expectEqual(@as(u32, 768), cloned[0].expected_dims);
    try std.testing.expectEqual(@as(u32, 512), cloned[0].chunk_size);
    try std.testing.expectEqual(@as(u32, 64), cloned[0].chunk_overlap);
    try std.testing.expectEqualStrings("{\"provider\":\"antfly\",\"text\":{\"target_tokens\":512,\"overlap_tokens\":64}}", cloned[0].chunker_json);
}

test "generated enrichment request clone without source_template" {
    const alloc = std.testing.allocator;

    const cloned = try cloneGeneratedRequests(alloc, &.{
        .{
            .kind = .dense_embedding,
            .index_name = "dv_v1",
            .doc_key = "doc:b",
            .source_field = "body",
            .expected_dims = 384,
        },
    });
    defer deinitGeneratedRequests(alloc, cloned);

    try std.testing.expectEqual(@as(usize, 1), cloned.len);
    try std.testing.expectEqualStrings("body", cloned[0].source_field);
    try std.testing.expectEqual(@as(usize, 0), cloned[0].source_template.len);
}
