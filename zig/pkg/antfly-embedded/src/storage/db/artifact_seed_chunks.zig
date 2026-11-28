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

//! Durable record of an accepted chunk-collection seed (#1791).
//!
//! A chunk producer has no input-identity record of its own: it re-chunks
//! every revision it processes. A seeded collection therefore records the
//! input it was accepted for (the producer's source text and chunker
//! configuration) together with the seeded chunks. Every chunk preparation
//! path (synchronous preparation and the replay worker) consults the record
//! before running the chunker: while the input is unchanged the seeded set is
//! the producer's output, so unrelated edits and replays keep it. A changed
//! input or chunker configuration no longer matches, and the chunker runs as
//! usual. A `null` seed deletes the record.
//!
//! The record lives in the document's asset-state family under a name no
//! enrichment can declare, so document deletion, backup and restore carry it
//! with the document.
const std = @import("std");
const Allocator = std.mem.Allocator;
const internal_keys = @import("../internal_keys.zig");
const artifact_seeds = @import("artifact_seeds.zig");

const magic = "antfly-seeded-chunks-v1\x00";
const record_name_prefix = "\x00seeded-chunks\x00";

pub fn keyAlloc(alloc: Allocator, doc_key: []const u8, chunk_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ record_name_prefix, chunk_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

/// Input identity of a chunk request: its source text and the chunker
/// configuration that would chunk it.
pub fn inputDigest(source_text: []const u8, request: anytype) [32]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ magic, source_text, request.chunker_json }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hasher.update(&length);
        hasher.update(part);
    }
    var sizes: [8]u8 = undefined;
    std.mem.writeInt(u32, sizes[0..4], request.chunk_size, .little);
    std.mem.writeInt(u32, sizes[4..8], request.chunk_overlap, .little);
    hasher.update(&sizes);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

pub fn encodeAlloc(alloc: Allocator, digest: [32]u8, items: []const std.json.Value) ![]u8 {
    const json = try std.json.Stringify.valueAlloc(alloc, items, .{});
    defer alloc.free(json);
    return try std.mem.concat(alloc, u8, &.{ magic, &digest, json });
}

/// The seeded chunk fields as chunker output.
pub fn chunksFromFieldsAlloc(comptime chunker: type, alloc: Allocator, fields: []const artifact_seeds.ChunkFields) ![]chunker.Chunk {
    const chunks = try alloc.alloc(chunker.Chunk, fields.len);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    for (fields, chunks) |field, *chunk| {
        chunk.* = .{
            .chunk_id = field.chunk_id,
            .text = try alloc.dupe(u8, field.content),
            .start_offset = field.start_char,
            .end_offset = field.end_char,
            .start_time_ms = field.start_time_ms,
            .end_time_ms = field.end_time_ms,
        };
        if (comptime @hasField(chunker.Chunk, "speaker_index")) chunk.speaker_index = field.speaker;
        initialized += 1;
    }
    return chunks;
}

/// The seeded chunks recorded for exactly this input, or null when the record
/// is absent, foreign, or was accepted for another input.
pub fn chunksForInputAlloc(comptime chunker: type, alloc: Allocator, record: ?[]const u8, source_text: []const u8, request: anytype) !?[]chunker.Chunk {
    const bytes = record orelse return null;
    if (bytes.len < magic.len + 32 or !std.mem.eql(u8, bytes[0..magic.len], magic)) return null;
    const expected = inputDigest(source_text, request);
    if (!std.mem.eql(u8, bytes[magic.len..][0..32], &expected)) return null;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, bytes[magic.len + 32 ..], .{ .parse_numbers = false }) catch return null;
    defer parsed.deinit();
    if (parsed.value != .array) return null;
    const fields = artifact_seeds.decodeChunksAlloc(alloc, parsed.value.array.items, source_text.len) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer alloc.free(fields);
    return try chunksFromFieldsAlloc(chunker, alloc, fields);
}

const vectors_magic = "antfly-staged-chunk-vectors-v1\x00";
const vectors_record_name_prefix = "\x00seeded-chunk-vectors\x00";

/// Seeded vectors of a staged chunk collection (above `max_seed_chunks`).
/// Its chunks publish on replay, so the write records the accepted vectors
/// with the chunk record they belong to and the embedder revision they were
/// accepted for. The replay worker uses them in place of embedding while
/// both still match.
pub fn vectorsKeyAlloc(alloc: Allocator, doc_key: []const u8, embedding_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ vectors_record_name_prefix, embedding_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

/// The digest a vectors record binds to: the exact chunk record bytes.
pub fn chunkRecordDigest(record: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(record, &digest, .{});
    return digest;
}

const vectors_header_len = vectors_magic.len + 32 + 1 + 32 + 4 + 4;

/// `vectors` are unsplit chunk vectors in strictly increasing chunk id
/// order, each `dims` long.
pub fn encodeVectorsAlloc(alloc: Allocator, chunk_record: [32]u8, revision: ?[32]u8, dims: u32, vectors: []const artifact_seeds.ChunkVector) ![]u8 {
    const entry_len = 4 + @as(usize, dims) * 4;
    const out = try alloc.alloc(u8, vectors_header_len + vectors.len * entry_len);
    errdefer alloc.free(out);
    var at: usize = 0;
    @memcpy(out[at..][0..vectors_magic.len], vectors_magic);
    at += vectors_magic.len;
    @memcpy(out[at..][0..32], &chunk_record);
    at += 32;
    out[at] = @intFromBool(revision != null);
    at += 1;
    @memcpy(out[at..][0..32], &(revision orelse @as([32]u8, @splat(0))));
    at += 32;
    std.mem.writeInt(u32, out[at..][0..4], dims, .little);
    at += 4;
    std.mem.writeInt(u32, out[at..][0..4], @intCast(vectors.len), .little);
    at += 4;
    for (vectors, 0..) |entry, i| {
        if (entry.window_index != null or entry.vector.len != dims) return error.InvalidArtifactSeed;
        if (i > 0 and vectors[i - 1].chunk_id >= entry.chunk_id) return error.InvalidArtifactSeed;
        std.mem.writeInt(u32, out[at..][0..4], entry.chunk_id, .little);
        at += 4;
        for (entry.vector) |value| {
            std.mem.writeInt(u32, out[at..][0..4], @bitCast(value), .little);
            at += 4;
        }
    }
    return out;
}

/// A vectors record that matches its chunk record and embedder revision.
pub const StagedVectors = struct {
    bytes: []const u8,
    dims: u32,
    count: u32,

    /// Validates `record` against the chunk record it binds to and the
    /// embedder revision of the replay; null when absent or stale.
    pub fn bind(record: ?[]const u8, chunk_record: ?[]const u8, revision: ?[32]u8, dims: u32) ?StagedVectors {
        const bytes = record orelse return null;
        const chunks = chunk_record orelse return null;
        if (bytes.len < vectors_header_len or !std.mem.eql(u8, bytes[0..vectors_magic.len], vectors_magic)) return null;
        var at: usize = vectors_magic.len;
        if (!std.mem.eql(u8, bytes[at..][0..32], &chunkRecordDigest(chunks))) return null;
        at += 32;
        const has_revision = bytes[at] != 0;
        at += 1;
        if (has_revision != (revision != null)) return null;
        if (revision) |expected| if (!std.mem.eql(u8, bytes[at..][0..32], &expected)) return null;
        at += 32;
        const recorded_dims = std.mem.readInt(u32, bytes[at..][0..4], .little);
        at += 4;
        const count = std.mem.readInt(u32, bytes[at..][0..4], .little);
        if (recorded_dims != dims or bytes.len != vectors_header_len + @as(usize, count) * (4 + @as(usize, dims) * 4)) return null;
        return .{ .bytes = bytes, .dims = dims, .count = count };
    }

    /// The seeded vector of `chunk_id`, copied, or null when none was seeded.
    pub fn vectorAlloc(self: StagedVectors, alloc: Allocator, chunk_id: u32) !?[]f32 {
        const entry_len = 4 + @as(usize, self.dims) * 4;
        var low: usize = 0;
        var high: usize = self.count;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const entry = self.bytes[vectors_header_len + middle * entry_len ..][0..entry_len];
            const id = std.mem.readInt(u32, entry[0..4], .little);
            if (id < chunk_id) {
                low = middle + 1;
            } else if (id > chunk_id) {
                high = middle;
            } else {
                const out = try alloc.alloc(f32, self.dims);
                for (out, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u32, entry[4 + i * 4 ..][0..4], .little));
                return out;
            }
        }
        return null;
    }
};

test "staged chunk vectors bind their chunk record and embedder revision" {
    const alloc = std.testing.allocator;
    var first = [_]f32{ 1, 2 };
    var second = [_]f32{ 3, 4 };
    const vectors = [_]artifact_seeds.ChunkVector{ .{ .chunk_id = 0, .window_index = null, .vector = &first }, .{ .chunk_id = 7, .window_index = null, .vector = &second } };
    const revision: [32]u8 = @splat(9);
    const record = try encodeVectorsAlloc(alloc, chunkRecordDigest("chunks"), revision, 2, &vectors);
    defer alloc.free(record);
    const bound = StagedVectors.bind(record, "chunks", revision, 2).?;
    const seven = (try bound.vectorAlloc(alloc, 7)).?;
    defer alloc.free(seven);
    try std.testing.expectEqualSlices(f32, &second, seven);
    try std.testing.expect((try bound.vectorAlloc(alloc, 3)) == null);
    try std.testing.expect(StagedVectors.bind(record, "other chunks", revision, 2) == null);
    try std.testing.expect(StagedVectors.bind(record, "chunks", null, 2) == null);
    try std.testing.expect(StagedVectors.bind(record, "chunks", revision, 3) == null);
    const unordered = [_]artifact_seeds.ChunkVector{ vectors[1], vectors[0] };
    try std.testing.expectError(error.InvalidArtifactSeed, encodeVectorsAlloc(alloc, chunkRecordDigest("chunks"), revision, 2, &unordered));
    var windowed = vectors;
    windowed[1].window_index = 1;
    try std.testing.expectError(error.InvalidArtifactSeed, encodeVectorsAlloc(alloc, chunkRecordDigest("chunks"), revision, 2, &windowed));
}

const partial_magic = "antfly-partial-extraction-v1\x00";
const partial_record_name_prefix = "\x00partial-extraction\x00";

/// Seeded pages of a partial document extraction, bound to the source URL and
/// extraction configuration they were accepted for. The extraction producer
/// extracts the document and uses these pages in place of its own.
pub fn partialExtractionKeyAlloc(alloc: Allocator, doc_key: []const u8, extraction_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ partial_record_name_prefix, extraction_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

pub fn encodePartialExtractionAlloc(alloc: Allocator, source_url: []const u8, config_json: []const u8, seed_json: []const u8) ![]u8 {
    const digest = extractionSourceDigest(source_url, config_json);
    return try std.mem.concat(alloc, u8, &.{ partial_magic, &digest, seed_json });
}

/// The canonical seed value when the record names exactly this source;
/// borrows from `record`.
pub fn partialExtractionSeed(record: ?[]const u8, source_url: []const u8, config_json: []const u8) ?[]const u8 {
    const bytes = record orelse return null;
    if (bytes.len <= partial_magic.len + 32 or !std.mem.eql(u8, bytes[0..partial_magic.len], partial_magic)) return null;
    const expected = extractionSourceDigest(source_url, config_json);
    if (!std.mem.eql(u8, bytes[partial_magic.len..][0..32], &expected)) return null;
    return bytes[partial_magic.len + 32 ..];
}

const extraction_magic = "antfly-seeded-extraction-v1\x00";
const extraction_record_name_prefix = "\x00seeded-extraction\x00";

/// Binding of a seeded document extraction to the source it was accepted for.
/// A seeded extraction's fingerprint covers the seed itself, which a later
/// replay cannot recompute without fetching; the binding names that
/// fingerprint for exactly this source URL and extraction configuration, so
/// replays reuse the accepted output instead of fetching. Any other URL or
/// configuration, or a regenerated state, no longer matches.
pub fn extractionKeyAlloc(alloc: Allocator, doc_key: []const u8, extraction_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ extraction_record_name_prefix, extraction_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

fn extractionSourceDigest(source_url: []const u8, config_json: []const u8) [32]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ extraction_magic, source_url, config_json }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hasher.update(&length);
        hasher.update(part);
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

pub fn encodeExtractionBindingAlloc(alloc: Allocator, source_url: []const u8, config_json: []const u8, fingerprint: []const u8) ![]u8 {
    const digest = extractionSourceDigest(source_url, config_json);
    return try std.mem.concat(alloc, u8, &.{ extraction_magic, &digest, fingerprint });
}

/// The accepted fingerprint when the binding names exactly this source;
/// borrows from `record`.
pub fn extractionBindingFingerprint(record: ?[]const u8, source_url: []const u8, config_json: []const u8) ?[]const u8 {
    const bytes = record orelse return null;
    if (bytes.len <= extraction_magic.len + 32 or !std.mem.eql(u8, bytes[0..extraction_magic.len], extraction_magic)) return null;
    const expected = extractionSourceDigest(source_url, config_json);
    if (!std.mem.eql(u8, bytes[extraction_magic.len..][0..32], &expected)) return null;
    return bytes[extraction_magic.len + 32 ..];
}

test "seeded extraction bindings name their exact source" {
    const alloc = std.testing.allocator;
    const record = try encodeExtractionBindingAlloc(alloc, "gs://a/b.pdf", "{}", "seed-sha256:ab");
    defer alloc.free(record);
    try std.testing.expectEqualStrings("seed-sha256:ab", extractionBindingFingerprint(record, "gs://a/b.pdf", "{}").?);
    try std.testing.expect(extractionBindingFingerprint(record, "gs://a/c.pdf", "{}") == null);
    try std.testing.expect(extractionBindingFingerprint(record, "gs://a/b.pdf", "{\"ocr\":true}") == null);
    try std.testing.expect(extractionBindingFingerprint(null, "gs://a/b.pdf", "{}") == null);
}

const unit_magic = "antfly-seeded-unit-chunks-v1\x00";
const asset_record_name_prefix = "\x00seeded-asset\x00";

/// Origin marker of a seeded asset: the digest of the asset state it was
/// accepted for. The asset counts as seeded only while its state still
/// matches; a later regeneration replaces the state and so the origin.
pub fn assetKeyAlloc(alloc: Allocator, doc_key: []const u8, asset_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ asset_record_name_prefix, asset_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

pub fn assetStateDigest(state_value: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(state_value, &digest, .{});
    return digest;
}

const unit_record_name_prefix = "\x00seeded-unit-chunks\x00";

/// Record of the accepted unit-scoped chunks of one extraction consumer.
pub fn unitKeyAlloc(alloc: Allocator, doc_key: []const u8, chunk_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, internal_keys.asset_state_kind);
    const name = try std.mem.concat(alloc, u8, &.{ unit_record_name_prefix, chunk_name });
    defer alloc.free(name);
    try internal_keys.appendEncodedComponent(&list, alloc, name);
    return try list.toOwnedSlice(alloc);
}

/// The chunker configuration a unit-scoped seed was accepted for.
pub fn configDigest(config: anytype) [32]u8 {
    return inputDigest(unit_magic, config);
}

pub const UnitSeed = struct {
    unit_id: []const u8,
    text: []const u8,
    items: []const std.json.Value,
};

/// Each unit's seeded chunks with the digest of the unit text they were
/// accepted for; units are matched by id and text when chunked again.
pub fn encodeUnitRecordAlloc(alloc: Allocator, config: anytype, units: []const UnitSeed) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var list = std.json.Array.init(a);
    for (units) |unit| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(unit.text, &digest, .{});
        var object = std.json.ObjectMap.empty;
        try object.put(a, "unit_id", .{ .string = unit.unit_id });
        try object.put(a, "text_sha256", .{ .string = try std.fmt.allocPrint(a, "{x}", .{&digest}) });
        var chunks = std.json.Array.init(a);
        try chunks.appendSlice(unit.items);
        try object.put(a, "chunks", .{ .array = chunks });
        try list.append(.{ .object = object });
    }
    var root_object = std.json.ObjectMap.empty;
    try root_object.put(a, "units", .{ .array = list });
    const json = try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = root_object }, .{});
    const digest = configDigest(config);
    return try std.mem.concat(alloc, u8, &.{ unit_magic, &digest, json });
}

/// The seeded chunks of `unit_id` when the record was accepted for exactly
/// this unit text and chunker configuration; null otherwise.
pub fn unitChunksForAlloc(comptime chunker: type, alloc: Allocator, record: ?[]const u8, unit_id: []const u8, unit_text: []const u8, config: anytype) !?[]chunker.Chunk {
    const bytes = record orelse return null;
    if (bytes.len < unit_magic.len + 32 or !std.mem.eql(u8, bytes[0..unit_magic.len], unit_magic)) return null;
    const expected = configDigest(config);
    if (!std.mem.eql(u8, bytes[unit_magic.len..][0..32], &expected)) return null;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, bytes[unit_magic.len + 32 ..], .{ .parse_numbers = false }) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const units = parsed.value.object.get("units") orelse return null;
    if (units != .array) return null;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(unit_text, &digest, .{});
    var hex: [64]u8 = undefined;
    _ = std.fmt.bufPrint(&hex, "{x}", .{&digest}) catch unreachable;
    for (units.array.items) |unit| {
        if (unit != .object) return null;
        const id = unit.object.get("unit_id") orelse return null;
        if (id != .string or !std.mem.eql(u8, id.string, unit_id)) continue;
        const text_digest = unit.object.get("text_sha256") orelse return null;
        if (text_digest != .string or !std.mem.eql(u8, text_digest.string, &hex)) return null;
        const chunks = unit.object.get("chunks") orelse return null;
        if (chunks != .array) return null;
        const fields = artifact_seeds.decodeUnitChunksAlloc(alloc, chunks.array.items, unit_id, unit_text.len) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return null,
        };
        defer alloc.free(fields);
        return try chunksFromFieldsAlloc(chunker, alloc, fields);
    }
    return null;
}

test "seeded unit chunk records bind each unit to its text" {
    const alloc = std.testing.allocator;
    const Chunker = struct {
        pub const Chunk = struct {
            chunk_id: u32,
            text: ?[]u8 = null,
            start_offset: ?u32 = null,
            end_offset: ?u32 = null,
            start_time_ms: ?f32 = null,
            end_time_ms: ?f32 = null,
            speaker_index: ?u8 = null,
            pub fn deinit(self: *Chunk, a: Allocator) void {
                if (self.text) |text| a.free(text);
            }
        };
    };
    const Config = struct { chunker_json: []const u8 = "", chunk_size: u32 = 8, chunk_overlap: u32 = 0 };
    var items = try std.json.parseFromSlice(std.json.Value, alloc, "[{\"_chunk_id\":0,\"_content\":\"Hi\",\"_parent_unit_id\":\"transcript:000001\",\"_speaker\":\"SPEAKER_02\"}]", .{});
    defer items.deinit();
    const record = try encodeUnitRecordAlloc(alloc, Config{}, &.{.{ .unit_id = "transcript:000001", .text = "Hi there", .items = items.value.array.items }});
    defer alloc.free(record);
    const chunks = (try unitChunksForAlloc(Chunker, alloc, record, "transcript:000001", "Hi there", Config{})).?;
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    try std.testing.expectEqual(@as(?u8, 2), chunks[0].speaker_index);
    try std.testing.expect((try unitChunksForAlloc(Chunker, alloc, record, "transcript:000001", "Hi then", Config{})) == null);
    try std.testing.expect((try unitChunksForAlloc(Chunker, alloc, record, "transcript:000002", "Hi there", Config{})) == null);
    try std.testing.expect((try unitChunksForAlloc(Chunker, alloc, record, "transcript:000001", "Hi there", Config{ .chunk_size = 4 })) == null);
}

test "seeded chunk records bind to their exact input" {
    const alloc = std.testing.allocator;
    const Chunker = struct {
        pub const Chunk = struct {
            chunk_id: u32,
            text: ?[]u8 = null,
            start_offset: ?u32 = null,
            end_offset: ?u32 = null,
            start_time_ms: ?f32 = null,
            end_time_ms: ?f32 = null,
            pub fn deinit(self: *Chunk, a: Allocator) void {
                if (self.text) |text| a.free(text);
            }
        };
    };
    const Request = struct { chunker_json: []const u8 = "", chunk_size: u32 = 8, chunk_overlap: u32 = 0 };
    var items = try std.json.parseFromSlice(std.json.Value, alloc, "[{\"_chunk_id\":0,\"_content\":\"ab\",\"_start_char\":0,\"_end_char\":2},{\"_chunk_id\":1,\"_content\":\"cd\"}]", .{});
    defer items.deinit();
    const record = try encodeAlloc(alloc, inputDigest("abcd", Request{}), items.value.array.items);
    defer alloc.free(record);

    const chunks = (try chunksForInputAlloc(Chunker, alloc, record, "abcd", Request{})).?;
    defer {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    }
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqualStrings("cd", chunks[1].text.?);
    try std.testing.expectEqual(@as(?u32, 2), chunks[0].end_offset);

    // A changed input or chunker configuration falls back to the chunker.
    try std.testing.expect((try chunksForInputAlloc(Chunker, alloc, record, "abce", Request{})) == null);
    try std.testing.expect((try chunksForInputAlloc(Chunker, alloc, record, "abcd", Request{ .chunk_size = 9 })) == null);
    try std.testing.expect((try chunksForInputAlloc(Chunker, alloc, "garbage", "abcd", Request{})) == null);
    try std.testing.expect((try chunksForInputAlloc(Chunker, alloc, null, "abcd", Request{})) == null);

    const key = try keyAlloc(alloc, "doc:a", "body_chunks_v1");
    defer alloc.free(key);
    try std.testing.expect(internal_keys.isRestoreArtifactKey(key));
}
