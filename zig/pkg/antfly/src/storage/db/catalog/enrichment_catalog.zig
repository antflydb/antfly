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
    expected_dims: u32 = 0,
    chunk_size: u32 = 0,
    chunk_overlap: u32 = 0,
    chunker_json: []const u8 = "",
    content_type: []const u8 = "",
    producer_json: []const u8 = "",

    pub fn clone(alloc: Allocator, cfg: EnrichmentConfig) !EnrichmentConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .kind = cfg.kind,
            .source_field = try alloc.dupe(u8, cfg.source_field),
            .source_template = if (cfg.source_template.len > 0) try alloc.dupe(u8, cfg.source_template) else "",
            .source_artifact_name = if (cfg.source_artifact_name.len > 0) try alloc.dupe(u8, cfg.source_artifact_name) else "",
            .expected_dims = cfg.expected_dims,
            .chunk_size = cfg.chunk_size,
            .chunk_overlap = cfg.chunk_overlap,
            .chunker_json = if (cfg.chunker_json.len > 0) try alloc.dupe(u8, cfg.chunker_json) else "",
            .content_type = if (cfg.content_type.len > 0) try alloc.dupe(u8, cfg.content_type) else "",
            .producer_json = if (cfg.producer_json.len > 0) try alloc.dupe(u8, cfg.producer_json) else "",
        };
    }

    pub fn deinit(self: *EnrichmentConfig, alloc: Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.source_field));
        if (self.source_template.len > 0) alloc.free(@constCast(self.source_template));
        if (self.source_artifact_name.len > 0) alloc.free(@constCast(self.source_artifact_name));
        if (self.chunker_json.len > 0) alloc.free(@constCast(self.chunker_json));
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.producer_json.len > 0) alloc.free(@constCast(self.producer_json));
        self.* = undefined;
    }
};

pub fn serializeCatalog(alloc: Allocator, enrichments: []const EnrichmentConfig) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, enrichments, .{});
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

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqual(.chunk, decoded[0].kind);
    try std.testing.expectEqualStrings("body_chunks_v1", decoded[0].name);
    try std.testing.expectEqualStrings("body", decoded[0].source_field);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", decoded[0].source_template);
    try std.testing.expectEqualStrings("{\"provider\":\"antfly\",\"text\":{\"target_tokens\":512,\"overlap_tokens\":64}}", decoded[0].chunker_json);
    try std.testing.expectEqual(.embedding, decoded[1].kind);
    try std.testing.expectEqualStrings("body_dense_v1", decoded[1].name);
    try std.testing.expectEqualStrings("body", decoded[1].source_field);
    try std.testing.expectEqualStrings("{{title}}\n{{body}}", decoded[1].source_template);
    try std.testing.expectEqualStrings("body_chunks_v1", decoded[1].source_artifact_name);
    try std.testing.expectEqual(@as(u32, 768), decoded[1].expected_dims);
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
