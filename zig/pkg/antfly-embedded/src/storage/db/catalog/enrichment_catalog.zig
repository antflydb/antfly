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

const std = @import("std");
const Allocator = std.mem.Allocator;
const enrichment_types = @import("../enrichment/enrichment_types.zig");

pub const EnrichmentType = enum {
    chunk,
    asset,
    embedding,
};

pub const EnrichmentConfig = struct {
    name: []const u8,
    kind: EnrichmentType,
    source_field: []const u8,
    /// Handlebars template to render document fields for embedding.
    /// When non-empty, the full document is rendered through this template
    /// instead of extracting a single source_field.
    source_template: []const u8 = "",
    source_artifact_name: []const u8 = "",
    group_source_json: []const u8 = "",
    prior_context_json: []const u8 = "",
    embedding_input: enrichment_types.EmbeddingInput = .text,
    expected_dims: u32 = 0,
    vector_space: []const u8 = "",
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",
    full_text_index: bool = false,
    content_type: []const u8 = "",
    producer_json: []const u8 = "",
    /// Canonicalized neighbor-context configuration for asset producers.
    /// Empty means the producer input carries no graph adjacency sample.
    neighbor_context_json: []const u8 = "",
    execution_json: []const u8 = "",
    /// Graph shorthand owns this producer until explicitly upserted. Only
    /// unreferenced producers with this provenance may be removed with a graph.
    graph_inline_managed: ?bool = null,

    pub fn clone(alloc: Allocator, cfg: EnrichmentConfig) !EnrichmentConfig {
        var out = cfg;
        inline for (.{ "name", "source_field", "source_template", "source_artifact_name", "group_source_json", "prior_context_json", "vector_space", "chunker_json", "content_type", "producer_json", "neighbor_context_json", "execution_json" }) |field| @field(out, field) = "";
        errdefer out.deinit(alloc);
        inline for (.{ "name", "source_field", "source_template", "source_artifact_name", "group_source_json", "prior_context_json", "vector_space", "chunker_json", "content_type", "producer_json", "neighbor_context_json", "execution_json" }) |field| {
            const value = @field(cfg, field);
            if (value.len > 0) @field(out, field) = try alloc.dupe(u8, value);
        }
        return out;
    }

    pub fn deinit(self: *EnrichmentConfig, alloc: Allocator) void {
        if (self.group_source_json.len > 0) alloc.free(self.group_source_json);
        if (self.prior_context_json.len > 0) alloc.free(self.prior_context_json);
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.source_field));
        if (self.source_template.len > 0) alloc.free(@constCast(self.source_template));
        if (self.source_artifact_name.len > 0) alloc.free(@constCast(self.source_artifact_name));
        if (self.vector_space.len > 0) alloc.free(@constCast(self.vector_space));
        if (self.chunker_json.len > 0) alloc.free(@constCast(self.chunker_json));
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.producer_json.len > 0) alloc.free(@constCast(self.producer_json));
        if (self.neighbor_context_json.len > 0) alloc.free(@constCast(self.neighbor_context_json));
        if (self.execution_json.len > 0) alloc.free(@constCast(self.execution_json));
        self.* = undefined;
    }
};

pub fn serializeCatalog(alloc: Allocator, enrichments: []const EnrichmentConfig) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, enrichments, .{ .emit_null_optional_fields = false });
}

pub fn deserializeCatalog(alloc: Allocator, data: []const u8) ![]EnrichmentConfig {
    const parsed = try std.json.parseFromSlice([]EnrichmentConfig, alloc, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const out = try alloc.alloc(EnrichmentConfig, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
        alloc.free(out);
    }
    for (parsed.value, 0..) |cfg, i| {
        out[i] = try EnrichmentConfig.clone(alloc, cfg);
        initialized += 1;
    }
    return out;
}

test "enrichment catalog round trip" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "body_chunks_v1",
            .kind = .chunk,
            .source_field = "body",
            .source_template = "{{title}}\n{{body}}",
            .chunk_size = 512,
            .chunk_overlap = 64,
            .chunker_json = "{\"provider\":\"antfly\",\"text\":{\"target_tokens\":512,\"overlap_tokens\":64}}",
        },
        .{
            .name = "conceptualize_v1",
            .kind = .asset,
            .source_field = "name",
            .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\"}}",
            .neighbor_context_json = "{\"graph_index\":\"taxonomy\",\"edge_types\":[\"started_by\"],\"direction\":\"out\",\"limit\":8}",
        },
        .{
            .name = "body_dense_v1",
            .kind = .embedding,
            .source_field = "body",
            .source_template = "{{title}}\n{{body}}",
            .source_artifact_name = "body_chunks_v1",
            .expected_dims = 768,
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 3), decoded.len);
    try std.testing.expectEqual(.chunk, decoded[0].kind);
    try std.testing.expectEqualStrings("body_chunks_v1", decoded[0].name);
    try std.testing.expectEqualStrings("body", decoded[0].source_field);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", decoded[0].source_template);
    try std.testing.expectEqualStrings("{\"provider\":\"antfly\",\"text\":{\"target_tokens\":512,\"overlap_tokens\":64}}", decoded[0].chunker_json);
    try std.testing.expectEqual(@as(usize, 0), decoded[0].neighbor_context_json.len);
    try std.testing.expectEqual(.asset, decoded[1].kind);
    try std.testing.expectEqualStrings("conceptualize_v1", decoded[1].name);
    try std.testing.expectEqualStrings(
        "{\"graph_index\":\"taxonomy\",\"edge_types\":[\"started_by\"],\"direction\":\"out\",\"limit\":8}",
        decoded[1].neighbor_context_json,
    );
    try std.testing.expectEqual(.embedding, decoded[2].kind);
    try std.testing.expectEqualStrings("body_dense_v1", decoded[2].name);
    try std.testing.expectEqualStrings("body", decoded[2].source_field);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", decoded[2].source_template);
    try std.testing.expectEqualStrings("body_chunks_v1", decoded[2].source_artifact_name);
    try std.testing.expectEqual(@as(u32, 768), decoded[2].expected_dims);
}

test "enrichment catalog round trip without source_template" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "simple_chunk",
            .kind = .chunk,
            .source_field = "body",
            .chunk_size = 256,
            .chunk_overlap = 32,
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings("body", decoded[0].source_field);
    try std.testing.expectEqual(@as(usize, 0), decoded[0].source_template.len);
}
