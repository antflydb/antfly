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

//! Durable inference preparation, deliberately separate from visible artifact
//! publication. A page and its cursor commit together under the source and
//! ownership fences. Restart never infers coverage from a partial artifact set.
const std = @import("std");
const chunker = @import("inference_chunker");
const Allocator = std.mem.Allocator;
pub const page_size = 64;

pub const Cursor = struct {
    format: u32 = 1,
    mutation_revision: u64 = 0,
    input_digest: [32]u8,
    source_sequence: u64 = 0,
    source_generation: u64 = 0,
    page_count: u32 = 0,
    covered_through_ordinal: u64 = 0,
    complete: bool = false,
    retiring: bool = false,
    settled: bool = false,
    acknowledged_consumers: []const []const u8 = &.{},
};

pub fn scope(alloc: Allocator, document: []const u8, producer: []const u8, field: []const u8) ![]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ document, producer, field }) |component| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, component.len, .little);
        hash.update(&length);
        hash.update(component);
    }
    return std.fmt.allocPrint(alloc, "\x00\x00__metadata__:media_pass:{s}", .{std.fmt.bytesToHex(hash.finalResult(), .lower)});
}

pub fn key(alloc: Allocator, prefix: []const u8, page: ?u32) ![]u8 {
    return if (page) |ordinal| std.fmt.allocPrint(alloc, "{s}:page:{d}", .{ prefix, ordinal }) else std.fmt.allocPrint(alloc, "{s}:cursor", .{prefix});
}

/// Checksummed records are private worker preparation, never chunk artifacts.
/// Vectors are held separately from descriptor recipes in the record. Media
/// payloads are never staged; replay always names the immutable original.
pub fn encodePage(alloc: Allocator, page: chunker.types.ChunkPage) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const chunks = try a.alloc(chunker.Chunk, page.chunks.len);
    const vectors = try a.alloc(?[]const f32, chunks.len);
    const outputs = try a.alloc(?[]const u8, chunks.len);
    for (page.chunks, chunks, vectors, outputs) |source, *recipe, *vector, *output| {
        recipe.* = source;
        recipe.data = null;
        const metadata = try a.create(chunker.types.ModelMetadata);
        metadata.* = source.metadata.?.*;
        vector.* = metadata.embedding;
        metadata.embedding = null;
        output.* = metadata.outputs_json;
        metadata.outputs_json = null;
        recipe.metadata = metadata;
    }
    var recipes = page;
    recipes.chunks = chunks;
    return encode(alloc, .{ .recipes = recipes, .vectors = vectors, .outputs = outputs });
}

pub fn decodePage(alloc: Allocator, raw: []const u8) !chunker.types.ChunkPage {
    const Stored = struct { recipes: chunker.types.ChunkPage, vectors: []const ?[]const f32, outputs: ?[]const ?[]const u8 = null };
    var parsed = try std.json.parseFromSlice(Stored, alloc, try payload(raw), .{});
    defer parsed.deinit();
    const source = parsed.value.recipes;
    if (parsed.value.vectors.len != source.chunks.len) return error.InvalidEnrichmentState;
    if (parsed.value.outputs) |values| if (values.len != source.chunks.len) return error.InvalidEnrichmentState;
    const chunks = try alloc.alloc(chunker.Chunk, source.chunks.len);
    var count: usize = 0;
    errdefer {
        for (chunks[0..count]) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    for (source.chunks, chunks, parsed.value.vectors, 0..) |recipe, *chunk, vector, index| {
        chunk.* = .{ .id = recipe.id, .mime_type = "", .start_char = recipe.start_char, .end_char = recipe.end_char };
        count += 1;
        chunk.mime_type = try alloc.dupe(u8, recipe.mime_type);
        chunk.owns_mime_type = true;
        if (recipe.data != null) return error.InvalidEnrichmentState;
        if (recipe.text) |text| {
            chunk.text = try alloc.dupe(u8, text);
            chunk.owns_text = true;
        }
        var metadata = (recipe.metadata orelse return error.InvalidEnrichmentState).*;
        if (metadata.embedding != null or metadata.outputs_json != null) return error.InvalidEnrichmentState;
        metadata.outputs_json = if (parsed.value.outputs) |values| values[index] else null;
        metadata.embedding = vector;
        chunk.metadata = try metadata.clone(alloc);
        chunk.owns_metadata = true;
    }
    const plan = try alloc.dupe(u8, source.plan_id);
    errdefer alloc.free(plan);
    const fingerprint = try alloc.dupe(u8, source.source_fingerprint);
    errdefer alloc.free(fingerprint);
    const version = try alloc.dupe(u8, source.source_version);
    return .{ .chunks = chunks, .plan_id = plan, .source_fingerprint = fingerprint, .source_version = version, .start_ordinal = source.start_ordinal, .covered_through_ordinal = source.covered_through_ordinal, .complete = source.complete };
}

pub fn encode(alloc: Allocator, value: anytype) ![]u8 {
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(json);
    const raw = try alloc.alloc(u8, 36 + json.len);
    @memcpy(raw[0..4], "AMP1");
    std.crypto.hash.sha2.Sha256.hash(json, raw[4..36], .{});
    @memcpy(raw[36..], json);
    return raw;
}

pub fn payload(raw: []const u8) ![]const u8 {
    if (raw.len < 36 or !std.mem.eql(u8, raw[0..4], "AMP1")) return error.InvalidEnrichmentState;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[36..], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[4..36])) return error.InvalidEnrichmentState;
    return raw[36..];
}

test "model aware media pass cursor is versioned and checksummed" {
    const a = std.testing.allocator;
    const raw = try encode(a, Cursor{ .input_digest = @splat(3), .page_count = 2, .covered_through_ordinal = 128 });
    defer a.free(raw);
    var parsed = try std.json.parseFromSlice(Cursor, a, try payload(raw), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 128), parsed.value.covered_through_ordinal);
    raw[raw.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidEnrichmentState, payload(raw));
}

pub fn position(alloc: Allocator, page: chunker.types.ChunkPage) !@import("../../../chunking/types.zig").Replay {
    const plan = try alloc.dupe(u8, page.plan_id);
    errdefer alloc.free(plan);
    const source = try alloc.dupe(u8, page.source_fingerprint);
    errdefer alloc.free(source);
    return .{ .plan_id = plan, .source_fingerprint = source, .source_version = try alloc.dupe(u8, page.source_version), .ordinal = page.covered_through_ordinal };
}

test "model aware media preparation owns descriptor recipes and separate vectors under allocation faults" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var metadata = chunker.types.ModelMetadata{ .descriptor = .{ .ordinal = 0, .input_tokens = 1, .coordinates = .{ .kind = .audio, .start_ns = 0, .end_ns = 100 } }, .inference_id = "plan:0", .source_fingerprint = "source", .source_version = "v1", .plan_id = "plan", .model = "audio", .model_identity = "identity", .task_type = "RETRIEVAL_DOCUMENT", .dimensions = 2, .policy = .{}, .embedding = &.{ 0.6, 0.8 }, .outputs_json = "[{\"name\":\"speech\",\"spans\":[{\"text\":\"hello\"}]}]" };
            var chunks = [_]chunker.Chunk{.{ .id = 0, .mime_type = "audio/wav", .data = "never persist decoded media", .metadata = &metadata }};
            const raw = try encodePage(a, .{ .chunks = &chunks, .plan_id = "plan", .source_fingerprint = "source", .source_version = "v1", .start_ordinal = 0, .covered_through_ordinal = 1, .complete = false });
            defer a.free(raw);
            try std.testing.expect(std.mem.indexOf(u8, raw, "never persist decoded media") == null);
            var page = try decodePage(a, raw);
            defer page.deinit(a);
            try std.testing.expectEqualSlices(f32, &.{ 0.6, 0.8 }, page.chunks[0].metadata.?.embedding.?);
            try std.testing.expectEqualStrings(metadata.outputs_json.?, page.chunks[0].metadata.?.outputs_json.?);
            try std.testing.expect(page.chunks[0].data == null);
            try std.testing.expect(!page.complete);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

/// Serving recipes retain computation identities and coverage, while private
/// pages and accepted assets own the vectors/transcripts/predictions.
pub fn releaseResultPayloads(a: Allocator, metadata: *chunker.types.ModelMetadata) !void {
    if (metadata.embedding) |vector| a.free(vector);
    metadata.embedding = null;
    if (metadata.outputs_json) |encoded| {
        var values = try std.json.parseFromSlice([]chunker.outputs.Result, a, encoded, .{});
        defer values.deinit();
        for (values.value) |*value| {
            value.embedding = null;
            value.spans = null;
            value.labels = null;
            value.speaker_spans = null;
        }
        const receipts = try std.json.Stringify.valueAlloc(a, values.value, .{ .emit_null_optional_fields = false });
        a.free(encoded);
        metadata.outputs_json = receipts;
    }
}

test "model aware media compacted speaker receipts retain dependency identity without timeline payloads" {
    const a = std.testing.allocator;
    const revision: [64]u8 = @splat('a');
    const output = chunker.outputs.Result{ .name = "voices", .kind = .diarize, .required = true, .model = "speaker", .model_identity = &revision, .model_revision = &revision, .speaker_model_revision = &revision, .recipe_id = &revision, .status = .complete, .start_ns = 0, .end_ns = 1, .speaker_scope = .source, .speaker_spans = &.{.{ .start_ns = 0, .end_ns = 1, .speaker_index = 0 }} };
    var metadata = chunker.types.ModelMetadata{ .descriptor = .{ .ordinal = 0, .input_tokens = 0, .coordinates = .{ .kind = .audio, .start_ns = 0, .end_ns = 1 } }, .inference_id = "plan:0", .source_fingerprint = "source", .source_version = "v1", .plan_id = "plan", .model = "speaker", .model_identity = &revision, .task_type = "DIARIZE", .dimensions = 0, .policy = .{}, .outputs_json = try std.json.Stringify.valueAlloc(a, &[_]chunker.outputs.Result{output}, .{ .emit_null_optional_fields = false }) };
    defer if (metadata.outputs_json) |bytes| a.free(bytes);
    try releaseResultPayloads(a, &metadata);
    const receipts = try std.json.parseFromSlice([]chunker.outputs.Result, a, metadata.outputs_json.?, .{});
    defer receipts.deinit();
    try std.testing.expect(receipts.value[0].speaker_spans == null);
    try std.testing.expect(!receipts.value[0].reusable());
    try std.testing.expectEqualStrings(&revision, receipts.value[0].speaker_model_revision.?);
    try std.testing.expectEqualStrings(&revision, receipts.value[0].recipe_id);
}
