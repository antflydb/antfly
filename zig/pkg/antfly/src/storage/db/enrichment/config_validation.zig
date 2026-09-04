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
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"ocr\":{\"enabled\":true,\"render_dpi\":20,\"config\":{\"provider\":\"antfly\"}}}}",
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
