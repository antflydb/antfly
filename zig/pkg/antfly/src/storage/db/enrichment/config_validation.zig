// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const types = @import("../types.zig");
const asset_producer = @import("asset_producer.zig");
const document_extraction = @import("document_extraction.zig");
const json_helpers = @import("../../../api/json_helpers.zig");

const Allocator = std.mem.Allocator;

/// Producer documents are JSON values, not opaque byte strings. Keep this
/// comparison at the enrichment boundary so API admission, reconciliation,
/// and index installation all use the same equivalence relation.
pub fn producerJsonValuesEqual(alloc: Allocator, lhs: []const u8, rhs: []const u8) !bool {
    if (std.mem.eql(u8, lhs, rhs)) return true;
    if (lhs.len == 0 or rhs.len == 0) return false;
    var lhs_parsed = std.json.parseFromSlice(std.json.Value, alloc, lhs, .{}) catch
        return error.InvalidEnrichmentConfig;
    defer lhs_parsed.deinit();
    var rhs_parsed = std.json.parseFromSlice(std.json.Value, alloc, rhs, .{}) catch
        return error.InvalidEnrichmentConfig;
    defer rhs_parsed.deinit();
    return json_helpers.jsonValuesEqual(lhs_parsed.value, rhs_parsed.value);
}

/// Validates the context-free portion of a public enrichment definition.
/// Dependency edges are validated by the catalog-aware caller, while this
/// function is intentionally shared by API admission and local provisioning.
pub fn validatePublicConfig(alloc: Allocator, cfg: types.EnrichmentConfig) !void {
    if (cfg.name.len == 0 or (cfg.field.len == 0 and cfg.template.len == 0))
        return error.InvalidEnrichmentConfig;
    if (cfg.execution) |execution| {
        if (execution.batch_items) |items| if (items == 0)
            return error.InvalidEnrichmentExecutionConfig;
        if (execution.batch_bytes) |bytes| if (bytes == 0)
            return error.InvalidEnrichmentExecutionConfig;
        if (execution.max_document_pages) |pages| if (pages == 0)
            return error.InvalidEnrichmentExecutionConfig;
    }
    if (cfg.full_text_index and cfg.kind == .embedding)
        return error.InvalidEnrichmentConfig;
    if (cfg.vector_space.len > 0 and cfg.kind != .embedding)
        return error.InvalidEnrichmentConfig;
    switch (cfg.kind) {
        .chunk => if (cfg.chunk_size == 0 and cfg.chunker_json.len == 0)
            return error.InvalidEnrichmentConfig,
        .embedding => {},
        .asset => try validateAssetProducerConfig(alloc, cfg.producer_json),
    }
}

/// Parses every producer at admission time and applies the same deep
/// document-extraction validation used when a local index catalog is opened.
pub fn validateAssetProducerConfig(alloc: Allocator, raw: []const u8) !void {
    var producer = asset_producer.parseProducerConfig(alloc, raw) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidAssetProducerConfig,
    };
    defer producer.deinit(alloc);
    if (producer.type != .document_extraction) return;

    var extraction = try document_extraction.parseConfig(alloc, producer.config_json);
    defer extraction.deinit(alloc);
}

/// Expands the public `transcriber` enrichment shorthand into the
/// document-extraction producer it stands for: every recording the source
/// field points at takes the audio route and is transcribed with the given
/// speech-to-text provider. The provider object is passed through as-is so
/// the same validation that guards `producer_json` applies to it.
pub fn transcriberShorthandProducerJsonAlloc(alloc: Allocator, transcriber: std.json.Value) ![]u8 {
    if (transcriber != .object) return error.InvalidAssetProducerConfig;
    const provider = transcriber.object.get("provider") orelse return error.InvalidAssetProducerConfig;
    if (provider != .string or provider.string.len == 0) return error.InvalidAssetProducerConfig;
    return try std.json.Stringify.valueAlloc(alloc, .{
        .type = "document_extraction",
        .config = .{
            .transcription = .{ .enabled = true, .config = transcriber },
        },
    }, .{});
}

test "transcriber shorthand expands to a document extraction producer" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"provider":"antfly","model":"openai/whisper-base","language_code":"en","timestamps":true}
    , .{});
    defer parsed.deinit();
    const producer_json = try transcriberShorthandProducerJsonAlloc(alloc, parsed.value);
    defer alloc.free(producer_json);
    try std.testing.expectEqualStrings(
        "{\"type\":\"document_extraction\",\"config\":{\"transcription\":{\"enabled\":true,\"config\":{\"provider\":\"antfly\",\"model\":\"openai/whisper-base\",\"language_code\":\"en\",\"timestamps\":true}}}}",
        producer_json,
    );
    // The expansion is accepted by the same admission check as a hand-written producer.
    try validateAssetProducerConfig(alloc, producer_json);

    var missing_provider = try std.json.parseFromSlice(std.json.Value, alloc, "{\"model\":\"whisper-1\"}", .{});
    defer missing_provider.deinit();
    try std.testing.expectError(error.InvalidAssetProducerConfig, transcriberShorthandProducerJsonAlloc(alloc, missing_provider.value));
    try std.testing.expectError(error.InvalidAssetProducerConfig, transcriberShorthandProducerJsonAlloc(alloc, .{ .string = "antfly" }));
}

test "public enrichment validation rejects invalid execution and producer config" {
    try std.testing.expectError(error.InvalidEnrichmentExecutionConfig, validatePublicConfig(std.testing.allocator, .{
        .name = "chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 256,
        .execution = .{ .batch_items = 0 },
    }));
    try std.testing.expectError(error.InvalidAssetProducerConfig, validatePublicConfig(std.testing.allocator, .{
        .name = "units",
        .kind = .asset,
        .field = "url",
        .producer_json = "{",
    }));
    try std.testing.expectError(error.InvalidDocumentExtractionConfig, validatePublicConfig(std.testing.allocator, .{
        .name = "units",
        .kind = .asset,
        .field = "url",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"ocr\":{\"enabled\":true,\"render_dpi\":20,\"config\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    }));
    try std.testing.expectError(error.InvalidEnrichmentConfig, validatePublicConfig(std.testing.allocator, .{
        .name = "chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 256,
        .vector_space = "dense-v1",
    }));
}

test "producer JSON equality ignores object key order" {
    try std.testing.expect(try producerJsonValuesEqual(
        std.testing.allocator,
        "{\"provider\":\"antfly\",\"model\":\"embed-v1\"}",
        "{ \"model\" : \"embed-v1\", \"provider\" : \"antfly\" }",
    ));
    try std.testing.expect(!try producerJsonValuesEqual(
        std.testing.allocator,
        "{\"provider\":\"antfly\",\"model\":\"embed-v1\"}",
        "{\"provider\":\"antfly\",\"model\":\"embed-v2\"}",
    ));
}
