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

//! Artifact seeds: caller-supplied results for named enrichments.
//!
//! A write may carry `_artifacts.<name>` (assets and embeddings) and
//! `_chunks.<name>` (chunk collections) beside its source fields. The document
//! mapper strips both reserved fields from the primary row and keeps one
//! canonical, envelope-validated JSON copy with the extracted write. Producer
//! preparation then accepts each seed in place of the producer invocation, so
//! the seeded value is published through exactly the same artifact lifecycle
//! (input identity, coverage, consumer indexing, replication) as generated
//! output. See `docs/design/artifact-seeding.md`.
//!
//! This leaf owns only the public write shapes. It has no storage, catalog or
//! producer capability: envelope checks run at extraction, typed decoding runs
//! at acceptance once the target enrichment's kind and contract are known.
//! Writes accept exactly the documented shapes; descriptive read fields
//! (`artifact_id`, `status`, `dims`, ...) and internal routing fields
//! (`_parent_doc_key`, `_artifact_range_id`, `_chunk`, ...) fail visibly.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const artifacts_field = "_artifacts";
pub const chunks_field = "_chunks";

/// Upper bound on one seeded chunk collection inside a single atomic write.
/// Larger inventories belong to staged publication (`docs/design/media-pass.md`).
pub const max_seed_chunks: usize = 4095;
/// Upper bound on seeded artifact names per document and kind.
pub const max_seed_names: usize = 64;
pub const max_name_bytes: usize = 256;
pub const max_vector_dims: usize = 65536;

pub const Error = error{
    /// The seed does not match the documented write shape.
    InvalidArtifactSeed,
    /// The seed names an output that no enrichment declares, or targets it
    /// through the wrong reserved field (`_artifacts` versus `_chunks`).
    UnknownArtifactSeed,
    /// The target enrichment exists but this release cannot accept a seed for
    /// it (for example a context-dependent producer or a sparse embedding).
    ArtifactSeedNotAccepted,
    /// A seed-only transform names a document that does not exist. There are
    /// no pending seeds and no seed-only rows.
    ArtifactSeedDocumentNotFound,
    /// A seed-only transform must name the source version (`expected_version`)
    /// its seeds were computed from.
    ArtifactSeedSourceBindingRequired,
};

pub fn isSeedField(name: []const u8) bool {
    return std.mem.eql(u8, name, artifacts_field) or std.mem.eql(u8, name, chunks_field);
}

/// Returns whether a JSON object contains either reserved seed field.
pub fn hasSeedFields(object: std.json.ObjectMap) bool {
    return object.contains(artifacts_field) or object.contains(chunks_field);
}

/// Validates both reserved fields of `object` and returns their canonical
/// JSON, `{"_artifacts":{...},"_chunks":{...}}` with only present members, or
/// null when neither is present. The result is owned by `alloc`.
pub fn extractCanonicalAlloc(alloc: Allocator, object: std.json.ObjectMap) !?[]u8 {
    const artifacts = object.get(artifacts_field);
    const chunks = object.get(chunks_field);
    if (artifacts == null and chunks == null) return null;
    if (artifacts) |value| try validateArtifactsField(value);
    if (chunks) |value| try validateChunksField(value);
    if (artifacts != null and chunks != null) {
        // One write path per kind: a name may not be seeded through both.
        var it = artifacts.?.object.iterator();
        while (it.next()) |entry| {
            if (chunks.?.object.contains(entry.key_ptr.*)) return error.InvalidArtifactSeed;
        }
    }
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    var writer: std.json.Stringify = .{ .writer = &out.writer };
    writer.beginObject() catch return error.OutOfMemory;
    if (artifacts) |value| {
        writer.objectField(artifacts_field) catch return error.OutOfMemory;
        writer.write(value) catch return error.OutOfMemory;
    }
    if (chunks) |value| {
        writer.objectField(chunks_field) catch return error.OutOfMemory;
        writer.write(value) catch return error.OutOfMemory;
    }
    writer.endObject() catch return error.OutOfMemory;
    return try out.toOwnedSlice();
}

fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > max_name_bytes) return error.InvalidArtifactSeed;
    // Paths address `_artifacts.<name>` with exactly two segments.
    if (std.mem.indexOfScalar(u8, name, '.') != null) return error.InvalidArtifactSeed;
    if (!std.unicode.utf8ValidateSlice(name)) return error.InvalidArtifactSeed;
}

fn validateArtifactsField(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    if (value.object.count() > max_seed_names) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try validateName(entry.key_ptr.*);
        try validateArtifactEnvelope(entry.value_ptr.*);
    }
}

/// `null` deletes accepted output; otherwise `{value, source?, provenance?}`.
pub fn validateArtifactEnvelope(value: std.json.Value) !void {
    switch (value) {
        .null => return,
        .object => {},
        else => return error.InvalidArtifactSeed,
    }
    var has_value = false;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "value")) {
            has_value = true;
        } else if (std.mem.eql(u8, key, "source")) {
            try validateSource(entry.value_ptr.*);
        } else if (std.mem.eql(u8, key, "provenance")) {
            try validateProvenance(entry.value_ptr.*);
        } else return error.InvalidArtifactSeed;
    }
    if (!has_value) return error.InvalidArtifactSeed;
}

fn validateSource(value: std.json.Value) !void {
    if (value != .object or value.object.count() == 0) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const item = entry.value_ptr.*;
        if (item != .string or item.string.len == 0 or item.string.len > 512) return error.InvalidArtifactSeed;
        if (std.mem.eql(u8, key, "generation")) continue;
        if (std.mem.eql(u8, key, "sha256")) {
            if (item.string.len != 64) return error.InvalidArtifactSeed;
            for (item.string) |c| if (!std.ascii.isHex(c)) return error.InvalidArtifactSeed;
            continue;
        }
        if (std.mem.eql(u8, key, "source_hash")) {
            _ = parseSourceHash(item.string) orelse return error.InvalidArtifactSeed;
            continue;
        }
        return error.InvalidArtifactSeed;
    }
}

/// Parses the `xxh64:<hex>` form emitted by artifact reads.
pub fn parseSourceHash(text: []const u8) ?u64 {
    const prefix = "xxh64:";
    if (!std.mem.startsWith(u8, text, prefix)) return null;
    const digits = text[prefix.len..];
    if (digits.len == 0 or digits.len > 16) return null;
    return std.fmt.parseInt(u64, digits, 16) catch null;
}

fn validateProvenance(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (!std.mem.eql(u8, key, "producer") and !std.mem.eql(u8, key, "model") and !std.mem.eql(u8, key, "revision"))
            return error.InvalidArtifactSeed;
        const item = entry.value_ptr.*;
        if (item != .string or item.string.len > 512) return error.InvalidArtifactSeed;
    }
}

fn validateChunksField(value: std.json.Value) !void {
    if (value != .object) return error.InvalidArtifactSeed;
    if (value.object.count() > max_seed_names) return error.InvalidArtifactSeed;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try validateName(entry.key_ptr.*);
        try validateChunkCollection(entry.value_ptr.*);
    }
}

/// `null` deletes accepted output; otherwise the plain `_chunks` read array.
pub fn validateChunkCollection(value: std.json.Value) !void {
    switch (value) {
        .null => return,
        .array => {},
        else => return error.InvalidArtifactSeed,
    }
    if (value.array.items.len > max_seed_chunks) return error.InvalidArtifactSeed;
    var previous: ?u32 = null;
    for (value.array.items) |item| {
        const parsed = try parseChunkObject(item);
        if (previous) |prior| if (parsed.chunk_id <= prior) return error.InvalidArtifactSeed;
        previous = parsed.chunk_id;
    }
}

pub const ChunkFields = struct {
    chunk_id: u32,
    content: []const u8,
    start_char: ?u32 = null,
    end_char: ?u32 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
};

fn jsonU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |i| std.math.cast(u32, i) orelse error.InvalidArtifactSeed,
        .number_string => |s| std.fmt.parseInt(u32, s, 10) catch error.InvalidArtifactSeed,
        else => error.InvalidArtifactSeed,
    };
}

fn jsonF64(value: std.json.Value) !f64 {
    const number: f64 = switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| std.fmt.parseFloat(f64, s) catch return error.InvalidArtifactSeed,
        else => return error.InvalidArtifactSeed,
    };
    if (!std.math.isFinite(number)) return error.InvalidArtifactSeed;
    return number;
}

fn jsonF32(value: std.json.Value) !f32 {
    const wide = try jsonF64(value);
    const narrow: f32 = @floatCast(wide);
    if (!std.math.isFinite(narrow)) return error.InvalidArtifactSeed;
    return narrow;
}

fn jsonTimeMs(value: std.json.Value) !f32 {
    const ms = try jsonF32(value);
    if (ms < 0) return error.InvalidArtifactSeed;
    return ms;
}

pub fn parseChunkObject(value: std.json.Value) !ChunkFields {
    if (value != .object) return error.InvalidArtifactSeed;
    var chunk_id: ?u32 = null;
    var alias_id: ?u32 = null;
    var content: ?[]const u8 = null;
    var out: ChunkFields = .{ .chunk_id = 0, .content = "" };
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const item = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "_chunk_id")) {
            chunk_id = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_id")) {
            alias_id = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_content")) {
            if (item != .string) return error.InvalidArtifactSeed;
            content = item.string;
        } else if (std.mem.eql(u8, key, "_start_char")) {
            out.start_char = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_end_char")) {
            out.end_char = try jsonU32(item);
        } else if (std.mem.eql(u8, key, "_start_time_ms")) {
            out.start_time_ms = try jsonTimeMs(item);
        } else if (std.mem.eql(u8, key, "_end_time_ms")) {
            out.end_time_ms = try jsonTimeMs(item);
        } else if (std.mem.eql(u8, key, "_parent_unit_id") or std.mem.eql(u8, key, "_speaker")) {
            // Unit-scoped and diarized chunk seeds bind to a document
            // extraction manifest; they are not accepted by plain chunk
            // producers yet and must not be dropped silently.
            return error.ArtifactSeedNotAccepted;
        } else return error.InvalidArtifactSeed;
    }
    if (chunk_id == null and alias_id == null) return error.InvalidArtifactSeed;
    if (chunk_id != null and alias_id != null and chunk_id.? != alias_id.?) return error.InvalidArtifactSeed;
    out.chunk_id = chunk_id orelse alias_id.?;
    out.content = content orelse return error.InvalidArtifactSeed;
    if ((out.start_char == null) != (out.end_char == null)) return error.InvalidArtifactSeed;
    if (out.start_char) |start| if (start > out.end_char.?) return error.InvalidArtifactSeed;
    if ((out.start_time_ms == null) != (out.end_time_ms == null)) return error.InvalidArtifactSeed;
    if (out.start_time_ms) |start| if (start > out.end_time_ms.?) return error.InvalidArtifactSeed;
    return out;
}

/// One reserved-field entry. `delete` removes accepted output so the producer
/// regenerates it; omission leaves accepted output untouched.
pub const Entry = union(enum) {
    delete,
    /// `_artifacts.<name>` envelope.
    artifact: Envelope,
    /// `_chunks.<name>` array.
    chunks: []const std.json.Value,
};

pub const Envelope = struct {
    value: std.json.Value,
    source: ?Source = null,
};

pub const Source = struct {
    generation: ?[]const u8 = null,
    sha256: ?[]const u8 = null,
    source_hash: ?u64 = null,
};

/// Canonical seeds of one write, parsed for acceptance.
pub const Seeds = struct {
    parsed: std.json.Parsed(std.json.Value),

    pub fn parse(alloc: Allocator, canonical: []const u8) !Seeds {
        const parsed = std.json.parseFromSlice(std.json.Value, alloc, canonical, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidArtifactSeed,
        };
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArtifactSeed;
        // Canonical bytes may arrive from a durable intent or a peer; repeat
        // the envelope checks rather than trusting their producer.
        if (parsed.value.object.get(artifacts_field)) |value| try validateArtifactsField(value);
        if (parsed.value.object.get(chunks_field)) |value| try validateChunksField(value);
        var it = parsed.value.object.iterator();
        while (it.next()) |entry| if (!isSeedField(entry.key_ptr.*)) return error.InvalidArtifactSeed;
        return .{ .parsed = parsed };
    }

    pub fn deinit(self: *Seeds) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn artifact(self: *const Seeds, name: []const u8) ?Entry {
        const field = self.parsed.value.object.get(artifacts_field) orelse return null;
        const value = field.object.get(name) orelse return null;
        if (value == .null) return .delete;
        var envelope: Envelope = .{ .value = value.object.get("value").? };
        if (value.object.get("source")) |source| {
            var out: Source = .{};
            if (source.object.get("generation")) |item| out.generation = item.string;
            if (source.object.get("sha256")) |item| out.sha256 = item.string;
            if (source.object.get("source_hash")) |item| out.source_hash = parseSourceHash(item.string);
            envelope.source = out;
        }
        return .{ .artifact = envelope };
    }

    pub fn chunks(self: *const Seeds, name: []const u8) ?Entry {
        const field = self.parsed.value.object.get(chunks_field) orelse return null;
        const value = field.object.get(name) orelse return null;
        if (value == .null) return .delete;
        return .{ .chunks = value.array.items };
    }

    /// Either field, for callers that accept the output under its owning
    /// reserved field and must reject the other as a kind mismatch.
    pub fn any(self: *const Seeds, name: []const u8) ?Entry {
        return self.artifact(name) orelse self.chunks(name);
    }

    pub const Name = struct { name: []const u8, field: enum { artifacts, chunks } };

    /// Every seeded name, for the "names that match no enrichment" check.
    pub fn names(self: *const Seeds, alloc: Allocator) ![]Name {
        var out = std.ArrayListUnmanaged(Name).empty;
        errdefer out.deinit(alloc);
        if (self.parsed.value.object.get(artifacts_field)) |field| {
            var it = field.object.iterator();
            while (it.next()) |entry| try out.append(alloc, .{ .name = entry.key_ptr.*, .field = .artifacts });
        }
        if (self.parsed.value.object.get(chunks_field)) |field| {
            var it = field.object.iterator();
            while (it.next()) |entry| try out.append(alloc, .{ .name = entry.key_ptr.*, .field = .chunks });
        }
        return try out.toOwnedSlice(alloc);
    }
};

/// Dense vectors are arrays of finite numbers, as in `_embeddings`.
pub fn decodeDenseVectorAlloc(alloc: Allocator, value: std.json.Value, expected_dims: u32) ![]f32 {
    if (value != .array or value.array.items.len == 0 or value.array.items.len > max_vector_dims) return error.InvalidArtifactSeed;
    if (expected_dims != 0 and value.array.items.len != expected_dims) return error.InvalidArtifactSeed;
    const vector = try alloc.alloc(f32, value.array.items.len);
    errdefer alloc.free(vector);
    for (value.array.items, vector) |item, *slot| slot.* = try jsonF32(item);
    return vector;
}

pub const ChunkVector = struct {
    chunk_id: u32,
    /// Null means one vector for the whole chunk, stored unsplit (window 0).
    window_index: ?u32,
    vector: []f32,
};

pub fn freeChunkVectors(alloc: Allocator, vectors: []ChunkVector) void {
    for (vectors) |entry| alloc.free(entry.vector);
    alloc.free(vectors);
}

/// `[{chunk_id, window_index?, vector}]`, sorted by `(chunk_id, window)`.
/// A chunk's windows must be complete and contiguous from zero; an entry
/// without `window_index` must be that chunk's only vector.
pub fn decodeChunkVectorsAlloc(alloc: Allocator, value: std.json.Value, expected_dims: u32) ![]ChunkVector {
    if (value != .array or value.array.items.len > max_seed_chunks * 64) return error.InvalidArtifactSeed;
    var out = std.ArrayListUnmanaged(ChunkVector).empty;
    errdefer {
        for (out.items) |entry| alloc.free(entry.vector);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidArtifactSeed;
        var chunk_id: ?u32 = null;
        var window_index: ?u32 = null;
        var vector_value: ?std.json.Value = null;
        var it = item.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (std.mem.eql(u8, key, "chunk_id")) {
                chunk_id = try jsonU32(entry.value_ptr.*);
            } else if (std.mem.eql(u8, key, "window_index")) {
                window_index = try jsonU32(entry.value_ptr.*);
            } else if (std.mem.eql(u8, key, "vector")) {
                vector_value = entry.value_ptr.*;
            } else if (std.mem.eql(u8, key, "unit_id")) {
                return error.ArtifactSeedNotAccepted;
            } else return error.InvalidArtifactSeed;
        }
        const vector = try decodeDenseVectorAlloc(alloc, vector_value orelse return error.InvalidArtifactSeed, expected_dims);
        errdefer alloc.free(vector);
        if (out.items.len != 0) {
            // Consistent dimensions even when the contract leaves them open.
            if (out.items[0].vector.len != vector.len) return error.InvalidArtifactSeed;
        }
        try out.append(alloc, .{ .chunk_id = chunk_id orelse return error.InvalidArtifactSeed, .window_index = window_index, .vector = vector });
    }
    std.mem.sort(ChunkVector, out.items, {}, struct {
        fn less(_: void, a: ChunkVector, b: ChunkVector) bool {
            if (a.chunk_id != b.chunk_id) return a.chunk_id < b.chunk_id;
            return (a.window_index orelse 0) < (b.window_index orelse 0);
        }
    }.less);
    var i: usize = 0;
    while (i < out.items.len) {
        var j = i;
        while (j < out.items.len and out.items[j].chunk_id == out.items[i].chunk_id) : (j += 1) {}
        const group = out.items[i..j];
        if (group.len == 1 and group[0].window_index == null) {
            i = j;
            continue;
        }
        for (group, 0..) |entry, expected| {
            const window = entry.window_index orelse return error.InvalidArtifactSeed;
            if (window != expected) return error.InvalidArtifactSeed;
        }
        i = j;
    }
    return try out.toOwnedSlice(alloc);
}

/// Seeded chunks, borrowing their content from the parsed seeds. Callers map
/// them onto producer chunks. `source_len` bounds character spans when the
/// bound source text is known.
pub fn decodeChunksAlloc(alloc: Allocator, items: []const std.json.Value, source_len: ?usize) ![]ChunkFields {
    const out = try alloc.alloc(ChunkFields, items.len);
    errdefer alloc.free(out);
    var previous: ?u32 = null;
    for (items, out) |item, *slot| {
        const fields = try parseChunkObject(item);
        if (previous) |prior| if (fields.chunk_id <= prior) return error.InvalidArtifactSeed;
        previous = fields.chunk_id;
        if (source_len) |len| if (fields.end_char) |end| if (end > len) return error.InvalidArtifactSeed;
        slot.* = fields;
    }
    return out;
}

/// Asset seed bytes as stored for the enrichment's content type: a JSON
/// string for text assets, any JSON value for JSON assets. Text extraction
/// seeds may also use `{ "text": ... }`.
pub fn assetBytesAlloc(alloc: Allocator, value: std.json.Value, json_content: bool) ![]u8 {
    if (json_content) return try std.json.Stringify.valueAlloc(alloc, value, .{});
    return try alloc.dupe(u8, try assetText(value));
}

pub fn assetText(value: std.json.Value) ![]const u8 {
    switch (value) {
        .string => |text| return text,
        .object => |object| {
            if (object.count() != 1) return error.InvalidArtifactSeed;
            const text = object.get("text") orelse return error.InvalidArtifactSeed;
            if (text != .string) return error.InvalidArtifactSeed;
            return text.string;
        },
        else => return error.InvalidArtifactSeed,
    }
}

fn parseForTest(alloc: Allocator, json: []const u8) !std.json.Parsed(std.json.Value) {
    return std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
}

test "artifact seeds canonicalize both reserved fields and round-trip" {
    const alloc = std.testing.allocator;
    var doc = try parseForTest(alloc,
        \\{"url":"gs://a","_artifacts":{"extracted_text":{"value":{"text":"hi"},"source":{"generation":"17"},"provenance":{"producer":"legacy-ocr"}},"doc_vec":{"value":[0.5,1]},"gone":null},
        \\ "_chunks":{"body_chunks":[{"_chunk_id":0,"_content":"h","_start_char":0,"_end_char":1},{"_id":1,"_content":"i"}]}}
    );
    defer doc.deinit();
    const canonical = (try extractCanonicalAlloc(alloc, doc.value.object)).?;
    defer alloc.free(canonical);
    var seeds = try Seeds.parse(alloc, canonical);
    defer seeds.deinit();

    const text = seeds.artifact("extracted_text").?.artifact;
    try std.testing.expectEqualStrings("hi", try assetText(text.value));
    try std.testing.expectEqualStrings("17", text.source.?.generation.?);
    try std.testing.expect(seeds.artifact("gone").? == .delete);
    try std.testing.expect(seeds.artifact("missing") == null);
    try std.testing.expect(seeds.artifact("body_chunks") == null);

    const vector = try decodeDenseVectorAlloc(alloc, seeds.artifact("doc_vec").?.artifact.value, 2);
    defer alloc.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.5, 1 }, vector);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeDenseVectorAlloc(alloc, seeds.artifact("doc_vec").?.artifact.value, 3));

    const chunks = try decodeChunksAlloc(alloc, seeds.chunks("body_chunks").?.chunks, 2);
    defer alloc.free(chunks);
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqual(@as(u32, 1), chunks[1].chunk_id);
    try std.testing.expectEqualStrings("i", chunks[1].content);
    try std.testing.expectError(error.InvalidArtifactSeed, decodeChunksAlloc(alloc, seeds.chunks("body_chunks").?.chunks, 0));

    const names = try seeds.names(alloc);
    defer alloc.free(names);
    try std.testing.expectEqual(@as(usize, 4), names.len);
}

test "artifact seeds reject descriptive and internal read fields" {
    const alloc = std.testing.allocator;
    const rejected = [_][]const u8{
        \\{"_artifacts":{"a":{"value":"x","status":"ready"}}}
        ,
        \\{"_artifacts":{"a":{"artifact_id":"af1:asset:x","value":"x"}}}
        ,
        \\{"_artifacts":{"a":{"kind":"embedding_set","items":[]}}}
        ,
        \\{"_artifacts":{"a":{"source":{"generation":"1"}}}}
        ,
        \\{"_artifacts":{"a":"bare"}}
        ,
        \\{"_artifacts":{"a.b":{"value":"x"}}}
        ,
        \\{"_artifacts":{"a":{"value":"x","source":{"etag":"1"}}}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_parent_doc_key":"d"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_chunk":{"plan_id":"p"}}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":1,"_content":"x"},{"_chunk_id":1,"_content":"y"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_id":1,"_content":"x"}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0}]}}
        ,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_start_char":4,"_end_char":2}]}}
        ,
        \\{"_chunks":{"c":{"_chunk_id":0}}}
        ,
        \\{"_artifacts":{"c":{"value":"x"}},"_chunks":{"c":[]}}
    };
    for (rejected) |json| {
        var doc = try parseForTest(alloc, json);
        defer doc.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, extractCanonicalAlloc(alloc, doc.value.object));
    }
    var unit = try parseForTest(alloc,
        \\{"_chunks":{"c":[{"_chunk_id":0,"_content":"x","_parent_unit_id":"page:1"}]}}
    );
    defer unit.deinit();
    try std.testing.expectError(error.ArtifactSeedNotAccepted, extractCanonicalAlloc(alloc, unit.value.object));

    var plain = try parseForTest(alloc,
        \\{"title":"no seeds"}
    );
    defer plain.deinit();
    try std.testing.expect((try extractCanonicalAlloc(alloc, plain.value.object)) == null);
}

test "artifact seeds validate chunk vector windows" {
    const alloc = std.testing.allocator;
    var ok = try parseForTest(alloc,
        \\[{"chunk_id":1,"window_index":1,"vector":[3,4]},{"chunk_id":0,"vector":[1,2]},{"chunk_id":1,"window_index":0,"vector":[5,6]}]
    );
    defer ok.deinit();
    const vectors = try decodeChunkVectorsAlloc(alloc, ok.value, 2);
    defer freeChunkVectors(alloc, vectors);
    try std.testing.expectEqual(@as(usize, 3), vectors.len);
    try std.testing.expectEqual(@as(u32, 0), vectors[0].chunk_id);
    try std.testing.expectEqual(@as(?u32, 0), vectors[1].window_index);
    try std.testing.expectEqual(@as(f32, 5), vectors[1].vector[0]);

    const invalid = [_][]const u8{
        // Partial window set.
        \\[{"chunk_id":1,"window_index":1,"vector":[3,4]}]
        ,
        // Unsplit and windowed vectors for one chunk.
        \\[{"chunk_id":1,"vector":[3,4]},{"chunk_id":1,"window_index":1,"vector":[3,4]}]
        ,
        // Duplicate unsplit vectors.
        \\[{"chunk_id":1,"vector":[3,4]},{"chunk_id":1,"vector":[3,4]}]
        ,
        // Mixed dimensions.
        \\[{"chunk_id":0,"vector":[3,4]},{"chunk_id":1,"vector":[3]}]
        ,
        \\[{"chunk_id":0,"vector":[]}]
        ,
        \\[{"chunk_id":0,"vector":[1,2],"dims":2}]
    };
    for (invalid) |json| {
        var parsed = try parseForTest(alloc, json);
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidArtifactSeed, decodeChunkVectorsAlloc(alloc, parsed.value, 0));
    }
}

test "artifact seed asset values follow the content type" {
    const alloc = std.testing.allocator;
    var parsed = try parseForTest(alloc,
        \\{"text":"","json":{"label":"x","score":1},"wrapped":{"text":"t"}}
    );
    defer parsed.deinit();
    // Empty is a complete result, distinct from missing.
    const empty = try assetBytesAlloc(alloc, parsed.value.object.get("text").?, false);
    defer alloc.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    const json = try assetBytesAlloc(alloc, parsed.value.object.get("json").?, true);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{\"label\":\"x\",\"score\":1}", json);
    try std.testing.expectEqualStrings("t", try assetText(parsed.value.object.get("wrapped").?));
    try std.testing.expectError(error.InvalidArtifactSeed, assetBytesAlloc(alloc, parsed.value.object.get("json").?, false));
    try std.testing.expectEqual(@as(?u64, 0xabc), parseSourceHash("xxh64:abc"));
    try std.testing.expectEqual(@as(?u64, null), parseSourceHash("sha:abc"));
}
