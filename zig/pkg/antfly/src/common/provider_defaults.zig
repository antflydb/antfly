// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

pub const openai_origin = "https://api.openai.com";
pub const ollama_origin = "http://localhost:11434";
pub const cohere_origin = "https://api.cohere.com";
pub const gemini_v1beta_base = "https://generativelanguage.googleapis.com/v1beta";
pub const vertex_v1_base = "https://aiplatform.googleapis.com/v1";
pub const vertex_v1beta1_base = "https://aiplatform.googleapis.com/v1beta1";
pub const default_google_location = "us-central1";
pub const default_aws_region = "us-east-1";

/// Cohere Embed v2 accepts at most 96 texts per request. This applies to both
/// the direct Cohere API and Cohere models invoked through Bedrock.
pub const cohere_max_embedding_batch_size: usize = 96;
pub const vertex_default_max_embedding_batch_size: usize = 250;

/// Vertex generally accepts 250 embedding instances per prediction request,
/// but gemini-embedding-001 is restricted to one input. Keep this model-aware
/// wire limit shared by provider and managed batching so one managed chunk is
/// always exactly one upstream request.
pub fn vertexMaxEmbeddingBatchSize(model: []const u8) usize {
    const basename = std.fs.path.basename(model);
    return if (std.mem.eql(u8, basename, "gemini-embedding-001"))
        1
    else
        vertex_default_max_embedding_batch_size;
}

pub fn normalizedBase(configured: []const u8, default_value: []const u8) []const u8 {
    return std.mem.trimEnd(u8, if (configured.len > 0) configured else default_value, "/");
}

pub fn appendPathIfMissing(
    alloc: std.mem.Allocator,
    configured: []const u8,
    default_value: []const u8,
    suffix: []const u8,
) ![]u8 {
    const base = normalizedBase(configured, default_value);
    if (std.mem.endsWith(u8, base, suffix)) return try alloc.dupe(u8, base);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, suffix });
}

pub fn vertexRegionalV1BaseAlloc(
    alloc: std.mem.Allocator,
    configured: []const u8,
    location: []const u8,
) ![]u8 {
    if (configured.len > 0) return try alloc.dupe(u8, normalizedBase(configured, configured));
    return try std.fmt.allocPrint(alloc, "https://{s}-aiplatform.googleapis.com/v1", .{
        if (location.len > 0) location else default_google_location,
    });
}

pub fn bedrockRuntimeEndpointAlloc(
    alloc: std.mem.Allocator,
    configured: []const u8,
    region: []const u8,
) ![]u8 {
    if (configured.len > 0) return try alloc.dupe(u8, normalizedBase(configured, configured));
    return try std.fmt.allocPrint(alloc, "https://bedrock-runtime.{s}.amazonaws.com", .{
        if (region.len > 0) region else default_aws_region,
    });
}

test "provider endpoint defaults preserve explicit deployment endpoints" {
    const alloc = std.testing.allocator;
    const explicit = try vertexRegionalV1BaseAlloc(alloc, "https://private.example/v1/", "ignored");
    defer alloc.free(explicit);
    try std.testing.expectEqualStrings("https://private.example/v1", explicit);
    const regional = try vertexRegionalV1BaseAlloc(alloc, "", "europe-west4");
    defer alloc.free(regional);
    try std.testing.expectEqualStrings("https://europe-west4-aiplatform.googleapis.com/v1", regional);
}

test "shared provider limits match upstream APIs" {
    try std.testing.expectEqual(@as(usize, 96), cohere_max_embedding_batch_size);
    try std.testing.expectEqual(@as(usize, 1), vertexMaxEmbeddingBatchSize("gemini-embedding-001"));
    try std.testing.expectEqual(
        @as(usize, 1),
        vertexMaxEmbeddingBatchSize("publishers/google/models/gemini-embedding-001"),
    );
    try std.testing.expectEqual(
        @as(usize, 250),
        vertexMaxEmbeddingBatchSize("text-embedding-005"),
    );
}
