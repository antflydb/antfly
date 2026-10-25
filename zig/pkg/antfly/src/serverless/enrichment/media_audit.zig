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

//! Bounded completion witnesses derived by publication from the exact body.
//! They contain source commitments and receipt identities, never vectors,
//! decoded media, transcripts, or caller-provided trusted completion flags.
const std = @import("std");
const chunking = @import("antfly_local_sources").chunking_mod;
const outputs = @import("inference_chunker").outputs;
const Allocator = std.mem.Allocator;
pub const max_bytes = 16 * 1024;
pub const Source = struct { field: []const u8, digest: [32]u8 };
pub const Witness = struct { format: u8 = 1, sources: []const Source, document: std.json.Value };

pub fn sourceDigest(a: Allocator, value: std.json.Value) ![32]u8 {
    const json = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(json);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(json, &digest, .{});
    return digest;
}

pub fn recipe(a: Allocator, config: chunking.types.Config, field: []const u8, digest: [32]u8, revision: ?[32]u8, version: u32) ![64]u8 {
    var semantic = config;
    semantic.max_chunks = 0;
    const canonical = try chunking.types.stringifyAlloc(a, semantic);
    defer a.free(canonical);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("serverless-media-recipe-v2");
    for ([_][]const u8{ canonical, field, &digest }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hash.update(&length);
        hash.update(part);
    }
    if (revision) |value| hash.update(&value);
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, version)));
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

/// Null means ordinary hydration is required. Unsupported/large witnesses do
/// not weaken validation or make an otherwise valid document unpublishable.
pub fn buildAlloc(a: Allocator, body: []const u8) !?[]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, a, body, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer parsed.deinit();
    const d = parsed.value;
    if (d != .object) return null;
    const state = d.object.get("_enrichment") orelse return null;
    if (state != .object or state.object.contains("chunk_embeddings_checkpoint")) return null;
    const format = state.object.get("chunk_embeddings_recipe_format") orelse return null;
    const version = state.object.get("chunk_embeddings_version") orelse return null;
    const saved = state.object.get("chunk_embeddings_recipe") orelse return null;
    if (format != .integer or format.integer != 2 or version != .integer or saved != .string) return null;
    if (state.object.get("chunk_embeddings_pending")) |pending| if (pending != .bool or pending.bool) return null;
    const vectors = d.object.get("chunk_embeddings") orelse return null;
    if (vectors != .array or vectors.array.items.len == 0) return null;
    const scratch = parsed.arena.allocator();
    var compact = std.json.Value{ .object = .empty };
    var compact_state = std.json.Value{ .object = .empty };
    try compact_state.object.put(scratch, "chunk_embeddings_version", version);
    try compact_state.object.put(scratch, "chunk_embeddings_recipe", saved);
    try compact.object.put(scratch, "_enrichment", compact_state);
    // Require a uniform boundary generation across every accepted vector.
    var boundary: ?std.json.Value = null;
    var boundary_json: ?[]const u8 = null;
    var transient = std.heap.ArenaAllocator.init(a);
    defer transient.deinit();
    for (vectors.array.items) |row| {
        _ = transient.reset(.retain_capacity);
        const temporary = transient.allocator();
        if (row != .object) return null;
        const descriptor = row.object.get("_chunk") orelse return null;
        if (descriptor != .object) return null;
        var reduced = std.json.Value{ .object = .empty };
        for ([_][]const u8{ "model", "model_identity", "model_revision", "task_type", "dimensions" }) |name| {
            try reduced.object.put(temporary, name, descriptor.object.get(name) orelse return null);
        }
        const json = try std.json.Stringify.valueAlloc(temporary, reduced, .{});
        if (boundary_json) |old| {
            if (!std.mem.eql(u8, old, json)) return null;
        } else {
            boundary_json = try scratch.dupe(u8, json);
            boundary = try std.json.parseFromSliceLeaky(std.json.Value, scratch, boundary_json.?, .{});
        }
    }
    var vector_rows = std.json.Value{ .array = std.array_list.Managed(std.json.Value).init(scratch) };
    var vector = std.json.Value{ .object = .empty };
    try vector.object.put(scratch, "_chunk", boundary.?);
    try vector_rows.array.append(vector);
    try compact.object.put(scratch, "chunk_embeddings", vector_rows);
    if (d.object.get("media_outputs")) |media| {
        if (media != .object) return null;
        const rows = media.object.get("chunks") orelse return null;
        var unique = std.StringHashMapUnmanaged(void).empty;
        var reduced_rows = std.ArrayList(outputs.Row).empty;
        if (rows != .array or rows.array.items.len == 0) return null;
        for (rows.array.items) |row| {
            _ = transient.reset(.retain_capacity);
            const temporary = transient.allocator();
            const value = std.json.parseFromValueLeaky(outputs.Row, temporary, row, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return null,
            };
            if (value.outputs.len > 16) return null;
            const receipts = try temporary.alloc(outputs.Result, value.outputs.len);
            for (receipts, value.outputs) |*dest, original| {
                if (!original.reusable()) return null;
                dest.* = original;
                dest.sub_index = 0;
                dest.start_ns = null;
                dest.end_ns = null;
                dest.code = null;
                dest.attempts = 0;
                dest.embedding = if (original.embedding != null) &.{} else null;
                dest.spans = if (original.spans != null) &.{} else null;
                dest.labels = if (original.labels != null) &.{} else null;
            }
            const json = try std.json.Stringify.valueAlloc(temporary, receipts, .{});
            const slot = try unique.getOrPut(scratch, json);
            if (!slot.found_existing) {
                if (unique.count() > 16) return null;
                const owned = try scratch.dupe(u8, json);
                slot.key_ptr.* = owned;
                try reduced_rows.append(scratch, .{ .chunk_id = 0, .outputs = try std.json.parseFromSliceLeaky([]outputs.Result, scratch, owned, .{}) });
            }
        }
        const json = try std.json.Stringify.valueAlloc(scratch, .{ .chunks = reduced_rows.items }, .{});
        try compact.object.put(scratch, "media_outputs", try std.json.parseFromSliceLeaky(std.json.Value, scratch, json, .{}));
    }
    var sources = std.ArrayList(Source).empty;
    var fields = d.object.iterator();
    while (fields.next()) |field| {
        // Mutable remote references need their own per-source observation and
        // cannot use the namespace generation snapshot as source evidence.
        const selected = chunking.chunk.modelSourceFromValue(field.value_ptr.*) catch continue;
        switch (selected) {
            .text, .media => {},
            else => continue,
        }
        if (sources.items.len == 64) return null;
        try sources.append(scratch, .{ .field = field.key_ptr.*, .digest = try sourceDigest(a, field.value_ptr.*) });
    }
    if (sources.items.len == 0) return null;
    const json = try std.json.Stringify.valueAlloc(a, Witness{ .sources = sources.items, .document = compact }, .{});
    if (json.len > max_bytes) {
        a.free(json);
        return null;
    }
    return json;
}

const test_document =
    \\{"clip":"source","_enrichment":{"chunk_embeddings_version":1,"chunk_embeddings_recipe":"recipe","chunk_embeddings_recipe_format":2},"chunk_embeddings":[{"_chunk":{"model":"m","model_identity":"id","model_revision":"r","task_type":"t","dimensions":1}}],"media_outputs":{"chunks":[{"chunk_id":0,"outputs":[{"name":"speech","kind":"transcribe","required":true,"model":"s","model_identity":"si","model_revision":"sr","recipe_id":"recipe-s","status":"complete","spans":[]}]}]}}
;

fn allocationProbe(a: Allocator) !void {
    const witness = (try buildAlloc(a, test_document)).?;
    defer a.free(witness);
}

test "serverless media audit witnesses release every allocation on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}

test "serverless media audit witnesses require all receipts and exclude remote source shortcuts" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, test_document, .{});
    defer parsed.deinit();
    const scratch = parsed.arena.allocator();
    const witness = (try buildAlloc(a, test_document)).?;
    defer a.free(witness);
    try std.testing.expect(witness.len < max_bytes);
    var decoded = try std.json.parseFromSlice(Witness, a, witness, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.value.sources.len);
    try std.testing.expectEqualStrings("clip", decoded.value.sources[0].field);
    var state = parsed.value.object.get("_enrichment").?;
    try state.object.put(scratch, "chunk_embeddings_pending", .{ .bool = true });
    try parsed.value.object.put(scratch, "_enrichment", state);
    const pending = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(pending);
    try std.testing.expectEqual(@as(?[]u8, null), try buildAlloc(a, pending));
    _ = state.object.swapRemove("chunk_embeddings_pending");
    try parsed.value.object.put(scratch, "_enrichment", state);
    const media = parsed.value.object.get("media_outputs").?;
    const rows = media.object.get("chunks").?;
    const row = rows.array.items[0];
    const receipts = row.object.get("outputs").?;
    var broken = receipts.array.items[0];
    _ = broken.object.swapRemove("spans");
    receipts.array.items[0] = broken;
    const missing = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(missing);
    try std.testing.expectEqual(@as(?[]u8, null), try buildAlloc(a, missing));
    // A source reference can have a stable declaration while its backing
    // object fails or changes; leave it to per-source provider observation.
    var original = try std.json.parseFromSlice(std.json.Value, a, test_document, .{});
    defer original.deinit();
    const remote = try std.json.parseFromSliceLeaky(std.json.Value, original.arena.allocator(),
        \\{"type":"source","uri":"https://example.com/audio","mime_type":"audio/wav","version":"\"v1\"","length":100}
    , .{});
    try original.value.object.put(original.arena.allocator(), "clip", remote);
    const referenced = try std.json.Stringify.valueAlloc(a, original.value, .{});
    defer a.free(referenced);
    try std.testing.expectEqual(@as(?[]u8, null), try buildAlloc(a, referenced));
}
