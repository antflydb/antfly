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

//! Immutable checkpoint groups are ordinary authenticated fact bodies. The
//! current WAL mutation carries one group and a link to the preceding body;
//! it never rewrites the acknowledged output prefix. The fact reachability
//! walker owns retention of these links under the ordinary manifest lease.
const std = @import("std");
const facts = @import("../build/document_facts.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const outputs = @import("inference_chunker").outputs;

pub const Context = struct { pages: *PageStore, previous: facts.BodyRef, key_digest: [32]u8 };
pub const Group = struct {
    format: u8 = 1,
    key_digest: [32]u8,
    recipe: []const u8,
    generation: [32]u8,
    plan_id: []const u8,
    start: u64,
    end: u64,
    parent: ?facts.BodyRef = null,
    vectors: []const std.json.Value,
    previews: []const std.json.Value,
    rows: []const outputs.Row,
};

pub fn get(a: std.mem.Allocator, document: std.json.Value) !?Group {
    if (document != .object) return null;
    const state = document.object.get("_enrichment") orelse return null;
    if (state != .object) return null;
    const raw = state.object.get("chunk_embeddings_checkpoint") orelse return null;
    const group = try std.json.parseFromValueLeaky(Group, a, raw, .{});
    if (group.format != 1 or group.start >= group.end or group.recipe.len != 64 or group.plan_id.len != 64 or (group.start == 0) != (group.parent == null)) return error.InvalidMediaCheckpoint;
    return group;
}

pub fn generations(targets: []const outputs.Target, current: []const outputs.Result) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("serverless-checkpoint-generations-v1");
    for (targets) |target| {
        var found = false;
        for (current) |result| if (std.mem.eql(u8, result.name, target.name)) {
            found = true;
            hash.update(result.recipe_id);
            break;
        };
        if (!found) return error.InvalidChunkOutput;
    }
    return hash.finalResult();
}

// This runs once at closure or recipe invalidation, rather than once per page.
// Validate a contiguous chain for this exact source/consumer/key before using
// any prior receipt. Hash-verified BodyRefs and document working limits bound
// both corrupted ancestry and oversized final materialization.
pub fn expand(a: std.mem.Allocator, context: Context, document: *std.json.Value) !void {
    const last = (try get(a, document.*)) orelse return;
    if (!std.mem.eql(u8, &last.key_digest, &context.key_digest)) return error.InvalidMediaCheckpoint;
    var groups = std.ArrayList(Group).empty;
    defer groups.deinit(a);
    var current = last;
    while (true) {
        try context.pages.cancellation.check();
        try groups.append(a, current);
        const parent = current.parent orelse break;
        const bytes = try facts.readBodyAlloc(a, context.pages, parent);
        defer a.free(bytes);
        const previous = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        const next = (try get(a, previous)) orelse return error.InvalidMediaCheckpoint;
        if (next.end != current.start or next.start >= current.start or !std.mem.eql(u8, &next.key_digest, &last.key_digest) or !std.mem.eql(u8, next.recipe, last.recipe) or !std.mem.eql(u8, next.plan_id, last.plan_id) or !std.mem.eql(u8, &next.generation, &last.generation)) return error.InvalidMediaCheckpoint;
        current = next;
    }
    var vectors = std.ArrayList(std.json.Value).empty;
    var previews = std.ArrayList(std.json.Value).empty;
    var rows = std.ArrayList(outputs.Row).empty;
    var i = groups.items.len;
    while (i != 0) {
        i -= 1;
        const group = groups.items[i];
        try vectors.appendSlice(a, group.vectors);
        try previews.appendSlice(a, group.previews);
        try rows.appendSlice(a, group.rows);
    }
    try document.object.put(a, "chunk_embeddings", .{ .array = vectors.toManaged(a) });
    try document.object.put(a, "chunk_preview", .{ .array = previews.toManaged(a) });
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .plan_id = last.plan_id, .chunks = rows.items }, .{ .emit_null_optional_fields = false });
    defer a.free(encoded);
    try document.object.put(a, "media_outputs", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{ .allocate = .alloc_always }));
    var state = document.object.get("_enrichment").?;
    _ = state.object.swapRemove("chunk_embeddings_checkpoint");
    try document.object.put(a, "_enrichment", state);
}

pub fn compact(a: std.mem.Allocator, context: Context, document: *std.json.Value, group: Group) !void {
    if (!std.mem.eql(u8, &group.key_digest, &context.key_digest)) return error.InvalidMediaCheckpoint;
    const encoded = try std.json.Stringify.valueAlloc(a, group, .{ .emit_null_optional_fields = false });
    defer a.free(encoded);
    var state = document.object.get("_enrichment").?;
    try state.object.put(a, "chunk_embeddings_checkpoint", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{ .allocate = .alloc_always }));
    try document.object.put(a, "_enrichment", state);
    for ([_][]const u8{ "chunk_embeddings", "chunk_preview", "media_outputs", "transcript", "transcript_spans" }) |name| _ = document.object.swapRemove(name);
}
