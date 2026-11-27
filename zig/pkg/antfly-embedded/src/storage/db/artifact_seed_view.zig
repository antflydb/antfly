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

//! Seed view: accepted artifacts rendered in exactly the write shapes that
//! `artifact_seeds.zig` accepts (`artifact_view=seed`). Reading a document in
//! this view and writing the result back into another table publishes the
//! same outputs without invoking a producer. Vectors are included; descriptive
//! fields (`artifact_id`, `status`, `source_hash`, ...) and internal routing
//! fields are not. Document extraction assets export their extracted text
//! (`{"text": ...}`). Outputs that have no seed write shape (explicit external
//! vectors and unit-scoped extraction children) are omitted rather than
//! rendered in a shape writes would reject.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const artifact_ids = @import("artifact_ids.zig");
const codec = @import("enrichment/artifact_codec.zig");
const db_query_projection = @import("query/projection.zig");
const assetContentTypeIsJson = @import("document_collectors.zig").assetContentTypeIsJson;

/// Supplies the enrichment contract for asset outputs. Returns null for names
/// that no enrichment declares; `exportable` is false for producers whose
/// stored rows are not their seed shape (document extraction manifests).
pub const AssetContract = struct {
    ctx: *const anyopaque,
    lookup: *const fn (ctx: *const anyopaque, alloc: Allocator, name: []const u8) anyerror!?Asset,

    pub const Asset = struct {
        content_type: []const u8,
        mode: Mode,
    };

    pub const Mode = enum {
        /// The stored value bytes are the seed value.
        value,
        /// Document extraction: the seed is the units' text, `{"text": ...}`.
        extraction_text,
        /// No seed shape (e.g. a free `copy`).
        none,
    };
};

/// Stored regions and blocks without nulls or the positional reading order.
fn exportTextBoxes(a: Allocator, items: []const std.json.Value) !std.json.Value {
    var out = std.json.Array.init(a);
    for (items) |item| {
        if (item != .object) continue;
        var box = std.json.ObjectMap.empty;
        inline for (.{ "span", "bbox", "font_size", "bold", "italic" }) |field| {
            if (item.object.get(field)) |value| if (value != .null) try box.put(a, field, value);
        }
        try out.append(.{ .object = box });
    }
    return .{ .array = out };
}

/// Stored transcript phrases with their speaker as the write names it.
fn exportTranscriptSpans(a: Allocator, items: []const std.json.Value) !std.json.Value {
    var out = std.json.Array.init(a);
    for (items) |item| {
        if (item != .object) continue;
        var span = std.json.ObjectMap.empty;
        inline for (.{ "char_start", "char_end", "start_ms", "end_ms" }) |field| {
            if (item.object.get(field)) |value| try span.put(a, field, value);
        }
        if (item.object.get("speaker_index")) |value| if (value != .null) try span.put(a, "speaker", value);
        try out.append(.{ .object = span });
    }
    return .{ .array = out };
}

fn decodeRef(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => null,
        else => return err,
    };
}

fn floatArray(a: Allocator, vector: []const f32) !std.json.Value {
    var array = try std.json.Array.initCapacity(a, vector.len);
    for (vector) |value| array.appendAssumeCapacity(.{ .float = value });
    return .{ .array = array };
}

fn envelope(a: Allocator, value: std.json.Value) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try object.put(a, "value", value);
    return .{ .object = object };
}

const ChunkVector = struct { chunk_id: u32, window: u32, windowed: bool, vector: []f32, unit_id: ?[]const u8 = null };

fn sameChunk(x: ChunkVector, y: ChunkVector) bool {
    if (x.chunk_id != y.chunk_id) return false;
    if ((x.unit_id == null) != (y.unit_id == null)) return false;
    return x.unit_id == null or std.mem.eql(u8, x.unit_id.?, y.unit_id.?);
}

/// Returns `{ "_artifacts": {...}, "_chunks": {...} }` (only non-empty
/// members), owned by `alloc`, or null when the document has no exportable
/// output.
pub fn loadTxn(alloc: Allocator, doc_key: []const u8, txn: *docstore_mod.DocStore.Txn, contract: AssetContract) !?std.json.Value {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var artifacts = std.json.ObjectMap.empty;
    var chunk_sets = std.json.ObjectMap.empty;

    // Producer-backed assets: the stored value bytes are the seed value;
    // document extraction exports the text of its units in unit order.
    {
        const prefix = try internal_keys.artifactTypePrefixAlloc(a, doc_key, "asset");
        const rows = try docstore_mod.DocStore.scanPrefixTxn(a, txn, prefix);
        var extraction_text = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(u8)).empty;
        var extraction_units = std.StringArrayHashMapUnmanaged(std.json.Value).empty;
        var rich_extraction = false;
        for (rows) |row| {
            const ref = (try decodeRef(a, row.key)) orelse continue;
            if (ref.kind != .asset or ref.chunk_id != null) continue;
            if (@import("../../privacy/document.zig").isManagedArtifactName(ref.name)) continue;
            const asset = (try contract.lookup(contract.ctx, a, ref.name)) orelse continue;
            if (asset.mode == .extraction_text) {
                const text = try extraction_text.getOrPut(a, ref.name);
                if (!text.found_existing) text.value_ptr.* = .empty;
                const units = try extraction_units.getOrPut(a, ref.name);
                if (!units.found_existing) units.value_ptr.* = .{ .array = std.json.Array.init(a) };
                if (ref.unit_id == null) continue;
                const unit = std.json.parseFromSliceLeaky(std.json.Value, a, row.value, .{}) catch continue;
                const unit_text = if (unit == .object) unit.object.get("text") else null;
                if (unit_text == null or unit_text.? != .string) continue;
                if (text.value_ptr.items.len != 0) try text.value_ptr.appendSlice(a, "\n\n");
                try text.value_ptr.appendSlice(a, unit_text.?.string);
                // The rich form keeps each unit's type and page data.
                var exported = std.json.ObjectMap.empty;
                try exported.put(a, "text", unit_text.?);
                const unit_type = unit.object.get("unit_type");
                if (unit_type) |value| if (value == .string) try exported.put(a, "unit_type", value);
                if (unit.object.get("provenance")) |provenance| if (provenance == .object) {
                    inline for (.{ "page_number", "page_label", "page_bbox", "page_rotation" }) |field| {
                        if (provenance.object.get(field)) |value| if (value != .null) try exported.put(a, field, value);
                    }
                    // Layout and transcript phrases in their write shapes.
                    inline for (.{ "text_regions", "text_blocks" }) |field| {
                        if (provenance.object.get(field)) |value| if (value == .array and value.array.items.len > 0) {
                            try exported.put(a, field, try exportTextBoxes(a, value.array.items));
                        };
                    }
                    if (provenance.object.get("transcript_spans")) |value| if (value == .array and value.array.items.len > 0) {
                        try exported.put(a, "spans", try exportTranscriptSpans(a, value.array.items));
                    };
                };
                // The plain text route's single `document` unit is the text form.
                if (!(unit_type != null and unit_type.? == .string and std.mem.eql(u8, unit_type.?.string, "document")) or exported.count() > 2) rich_extraction = true;
                try units.value_ptr.array.append(.{ .object = exported });
                continue;
            }
            if (asset.mode != .value or ref.unit_id != null) continue;
            const value: std.json.Value = if (assetContentTypeIsJson(asset.content_type))
                std.json.parseFromSliceLeaky(std.json.Value, a, row.value, .{ .parse_numbers = false }) catch continue
            else
                .{ .string = row.value };
            try artifacts.put(a, ref.name, try envelope(a, value));
        }
        var texts = extraction_text.iterator();
        while (texts.next()) |entry| {
            var object = std.json.ObjectMap.empty;
            // One plain text unit exports as text; anything richer (pages,
            // typed or several units) exports its units.
            const units = extraction_units.get(entry.key_ptr.*).?;
            if (rich_extraction or units.array.items.len > 1) {
                try object.put(a, "units", units);
            } else {
                try object.put(a, "text", .{ .string = entry.value_ptr.items });
            }
            try artifacts.put(a, entry.key_ptr.*, try envelope(a, .{ .object = object }));
        }
    }

    // Document-level dense and sparse vectors. Explicit external vectors
    // round-trip through `_embeddings`; elided vectors have no seed shape.
    {
        const prefix = try internal_keys.artifactTypePrefixAlloc(a, doc_key, "embedding");
        const rows = try docstore_mod.DocStore.scanPrefixTxn(a, txn, prefix);
        for (rows) |row| {
            if (!internal_keys.isInternalUserKey(row.key)) continue;
            const ref = (try decodeRef(a, row.key)) orelse continue;
            if (ref.kind != .embedding or ref.source != null or ref.chunk_id != null) continue;
            const header = codec.decodeHeaderPrefix(row.value) catch continue;
            if (header.flags.authored) continue;
            if (header.kind == .sparse_embedding) {
                const sparse = codec.decodeSparseEmbeddingAlloc(a, row.value) catch continue;
                var indices = try std.json.Array.initCapacity(a, sparse.indices.len);
                for (sparse.indices) |index| indices.appendAssumeCapacity(.{ .integer = index });
                var object = std.json.ObjectMap.empty;
                try object.put(a, "indices", .{ .array = indices });
                try object.put(a, "values", try floatArray(a, sparse.values));
                try artifacts.put(a, ref.name, try envelope(a, .{ .object = object }));
                continue;
            }
            if (header.kind != .dense_embedding) continue;
            const vector = codec.decodeDenseEmbeddingAlloc(a, row.value) catch continue;
            try artifacts.put(a, ref.name, try envelope(a, try floatArray(a, vector)));
        }
    }

    // Chunk collections and their derived vectors, from the selected
    // generation only.
    {
        var cursor = try @import("artifact_chunk_cursor.zig").Cursor(docstore_mod.DocStore.Txn).open(a, txn, doc_key);
        defer cursor.close();
        while (try cursor.next()) |entry| {
            const ref = (try decodeRef(a, entry.key)) orelse continue;
            if (@import("../../privacy/document.zig").isManagedArtifactName(ref.name)) continue;
            var parsed = std.json.parseFromSliceLeaky(std.json.Value, a, entry.value, .{ .parse_numbers = false }) catch continue;
            if (parsed != .object) continue;
            // Unit-scoped chunks of a document extraction export their unit
            // and speaker; every other chunk names neither.
            const unit_id = ref.unit_id;
            if (unit_id == null and (parsed.object.get("_parent_unit_id") != null or parsed.object.get("_parent_unit_key") != null)) continue;
            const speaker = parsed.object.get("_speaker");
            try db_query_projection.normalizeChunkArtifactForQuery(a, &parsed);
            var chunk = std.json.ObjectMap.empty;
            try chunk.put(a, "_chunk_id", .{ .integer = ref.chunk_id orelse continue });
            const content = parsed.object.get("_content") orelse continue;
            if (content != .string) continue;
            try chunk.put(a, "_content", content);
            inline for (.{ "_start_char", "_end_char", "_start_time_ms", "_end_time_ms" }) |field| {
                if (parsed.object.get(field)) |value| switch (value) {
                    .integer, .float, .number_string => try chunk.put(a, field, value),
                    else => {},
                };
            }
            // Spans are optional, but a lone bound is not a write shape.
            if (chunk.contains("_start_char") != chunk.contains("_end_char")) {
                _ = chunk.swapRemove("_start_char");
                _ = chunk.swapRemove("_end_char");
            }
            if (chunk.contains("_start_time_ms") != chunk.contains("_end_time_ms")) {
                _ = chunk.swapRemove("_start_time_ms");
                _ = chunk.swapRemove("_end_time_ms");
            }
            if (unit_id) |id| {
                try chunk.put(a, "_parent_unit_id", .{ .string = id });
                if (speaker) |label| if (label == .string) try chunk.put(a, "_speaker", label);
            }
            const set = try chunk_sets.getOrPut(a, ref.name);
            if (!set.found_existing) set.value_ptr.* = .{ .array = std.json.Array.init(a) };
            try set.value_ptr.array.append(.{ .object = chunk });
        }

        var vectors = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(ChunkVector)).empty;
        const prefix = try internal_keys.artifactTypePrefixAlloc(a, doc_key, "chunk");
        var keys = try txn.openPhysicalKeyCursorAdapter();
        defer keys.close();
        var next = try keys.seekAtOrAfter(prefix);
        while (next) |entry| : (next = try keys.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            const base = (try internal_keys.derivedEmbeddingBaseKeyAlloc(a, entry.key)) orelse continue;
            if ((try @import("artifact_chunk_generation.zig").readMember(a, txn, base)) == null) continue;
            const ref = (try decodeRef(a, entry.key)) orelse continue;
            if (ref.kind != .embedding) continue;
            // Derived vectors name their chunk through the source reference;
            // vectors of extraction unit chunks also name the unit.
            const source = ref.source orelse continue;
            if (source.kind != .chunk) continue;
            const unit_id = source.unit_id orelse ref.unit_id;
            const chunk_id = source.chunk_id orelse continue;
            const window_index = source.window_index orelse ref.window_index;
            const payload = try txn.get(entry.key);
            const header = codec.decodeHeaderPrefix(payload) catch continue;
            if (header.flags.authored or header.kind != .dense_embedding) continue;
            const vector = codec.decodeDenseEmbeddingAlloc(a, payload) catch continue;
            const list = try vectors.getOrPut(a, try a.dupe(u8, ref.name));
            if (!list.found_existing) list.value_ptr.* = .empty;
            // Unit chunks are embedded whole: a windowed one has no seed shape.
            if (unit_id != null and window_index != null) continue;
            try list.value_ptr.append(a, .{ .chunk_id = chunk_id, .window = window_index orelse 0, .windowed = window_index != null, .vector = vector, .unit_id = unit_id });
        }
        var it = vectors.iterator();
        while (it.next()) |entry| {
            const items = entry.value_ptr.items;
            std.mem.sort(ChunkVector, items, {}, struct {
                fn less(_: void, x: ChunkVector, y: ChunkVector) bool {
                    const xu = x.unit_id orelse "";
                    const yu = y.unit_id orelse "";
                    if (!std.mem.eql(u8, xu, yu)) return std.mem.lessThan(u8, xu, yu);
                    if (x.chunk_id != y.chunk_id) return x.chunk_id < y.chunk_id;
                    return x.window < y.window;
                }
            }.less);
            var array = std.json.Array.init(a);
            for (items, 0..) |item, i| {
                // `window_index` appears exactly when the chunk has several
                // windows, matching the import rule for unsplit vectors.
                const several = (i > 0 and sameChunk(items[i - 1], item)) or
                    (i + 1 < items.len and sameChunk(items[i + 1], item));
                var object = std.json.ObjectMap.empty;
                if (item.unit_id) |id| try object.put(a, "parent_unit_id", .{ .string = id });
                try object.put(a, "chunk_id", .{ .integer = item.chunk_id });
                if (several or item.window != 0) try object.put(a, "window_index", .{ .integer = item.window });
                try object.put(a, "vector", try floatArray(a, item.vector));
                try array.append(.{ .object = object });
            }
            try artifacts.put(a, entry.key_ptr.*, try envelope(a, .{ .array = array }));
        }
    }

    if (artifacts.count() == 0 and chunk_sets.count() == 0) return null;
    var root = std.json.ObjectMap.empty;
    if (artifacts.count() != 0) try root.put(a, "_artifacts", .{ .object = artifacts });
    if (chunk_sets.count() != 0) try root.put(a, "_chunks", .{ .object = chunk_sets });
    return try db_query_projection.cloneJsonValue(alloc, .{ .object = root });
}
