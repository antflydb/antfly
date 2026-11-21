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


//! Dependency reports shared by the embedded C ABI and the distributed
//! server's group-local reads: one document artifact's freshness and
//! retained lineage, and one shard's reverse lineage (impact) rows.

const std = @import("std");
const store = @import("enrichment/context_dependency_store.zig");
const ctx = @import("enrichment/context_sources.zig");

pub fn dependenciesJsonAlloc(alloc: std.mem.Allocator, db: anytype, doc_key: []const u8, artifact: []const u8, max_versions: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"freshness\":");
    var state = try db.contextDependencyState(alloc, doc_key, artifact);
    defer if (state) |*value| value.deinit(alloc);
    if (state) |value| {
        const fresh = try store.freshnessJsonAlloc(alloc, value);
        defer alloc.free(fresh);
        try out.appendSlice(alloc, fresh);
        try out.print(alloc, ",\"subscriptions\":{d}", .{value.tuples.len});
    } else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"versions\":[");
    var first = true;
    if (state) |value| {
        var version = value.published_version;
        var emitted: usize = 0;
        while (version > 0 and emitted < max_versions) : (version -= 1) {
            const manifest = (try db.contextDependencyManifest(alloc, doc_key, artifact, version)) orelse break;
            defer alloc.free(manifest);
            if (!first) try out.append(alloc, ',');
            first = false;
            try out.appendSlice(alloc, manifest);
            emitted += 1;
        }
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
}

/// One shard's impact rows for an input key: direct dependents confirmed
/// against retained manifests and, with `transitive`, the same-document
/// artifacts that consume them. `{"rows":[...],"complete":bool}`.
pub fn groupImpactJsonAlloc(alloc: std.mem.Allocator, db: anytype, own_identity: []const u8, input_identity: []const u8, key: []const u8, revision: ?store.Digest, limit: usize, transitive: bool, max_depth: u32) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"rows\":[");
    var emitted: usize = 0;
    var complete = true;
    var page = try db.contextImpact(alloc, input_identity, key, revision, "", limit);
    defer page.deinit(alloc);
    if (page.resume_after != null) complete = false;
    for (page.rows.items) |r| {
        if (emitted >= limit) {
            complete = false;
            break;
        }
        try appendImpactRow(alloc, &out, &emitted, own_identity, r.doc_key, r.artifact, r.version, r.current_version, @tagName(r.freshness), &.{r.artifact});
        if (!transitive) continue;
        if (!try appendDownstreamImpact(alloc, &out, &emitted, db.core.index_manager.enrichments.items, own_identity, r.doc_key, r.artifact, max_depth, limit)) complete = false;
    }
    try out.print(alloc, "],\"complete\":{}}}", .{complete});
    return out.toOwnedSlice(alloc);
}

/// Emit every same-document artifact that transitively consumes `root`, each
/// with its path. Returns false when the depth or row budget cut a branch
/// short (an existing consumer was left out).
pub fn appendDownstreamImpact(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), emitted: *usize, consumers: anytype, table_id: []const u8, doc_key: []const u8, root: []const u8, max_depth: u32, limit: usize) !bool {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const budget = if (emitted.* >= limit) 0 else limit - emitted.*;
    const walk = try downstreamImpactPaths(arena.allocator(), consumers, root, max_depth, budget);
    for (walk.paths) |path| try appendImpactRow(alloc, out, emitted, table_id, doc_key, path[path.len - 1], 0, true, "candidate", path);
    return walk.complete;
}

const DownstreamWalk = struct { paths: []const []const []const u8, complete: bool };

/// Breadth-first over every consumer: `consumers` items expose `.name` and
/// `.source_artifact_name`. Each path starts at `root`. `complete` is false
/// when an existing consumer lies beyond `max_depth` artifacts or past
/// `budget` paths. Allocates in `a` (an arena).
pub fn downstreamImpactPaths(a: std.mem.Allocator, consumers: anytype, root: []const u8, max_depth: u32, budget: usize) !DownstreamWalk {
    var paths = std.ArrayListUnmanaged([]const []const u8).empty;
    var queue = std.ArrayListUnmanaged([]const []const u8).empty;
    var visited = std.StringHashMapUnmanaged(void).empty;
    try visited.put(a, root, {});
    try queue.append(a, try a.dupe([]const u8, &.{root}));
    var head: usize = 0;
    while (head < queue.items.len) : (head += 1) {
        const node = queue.items[head];
        const frontier = node[node.len - 1];
        for (consumers) |entry| {
            if (!std.mem.eql(u8, entry.source_artifact_name, frontier)) continue;
            if (visited.contains(entry.name)) continue;
            if (node.len >= max_depth or paths.items.len >= budget) return .{ .paths = paths.items, .complete = false };
            try visited.put(a, entry.name, {});
            const path = try std.mem.concat(a, []const u8, &.{ node, &.{entry.name} });
            try paths.append(a, path);
            try queue.append(a, path);
        }
    }
    return .{ .paths = paths.items, .complete = true };
}

test "transitive impact follows every consumer of an artifact" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Consumer = struct { name: []const u8, source_artifact_name: []const u8 };
    // summary feeds both `tags` and `title`; `title` feeds `slug`.
    const consumers = [_]Consumer{
        .{ .name = "tags", .source_artifact_name = "summary" },
        .{ .name = "title", .source_artifact_name = "summary" },
        .{ .name = "slug", .source_artifact_name = "title" },
        .{ .name = "other", .source_artifact_name = "unrelated" },
    };
    const all = try downstreamImpactPaths(a, &consumers, "summary", 8, 100);
    try std.testing.expect(all.complete);
    try std.testing.expectEqual(@as(usize, 3), all.paths.len);
    try std.testing.expectEqualStrings("tags", all.paths[0][1]);
    try std.testing.expectEqualStrings("title", all.paths[1][1]);
    try std.testing.expectEqual(@as(usize, 3), all.paths[2].len);
    try std.testing.expectEqualStrings("slug", all.paths[2][2]);
    // Depth 2 reaches both direct consumers; `slug` is cut, so incomplete.
    const shallow = try downstreamImpactPaths(a, &consumers, "summary", 2, 100);
    try std.testing.expect(!shallow.complete);
    try std.testing.expectEqual(@as(usize, 2), shallow.paths.len);
    // A depth that covers everything is complete even at the boundary.
    try std.testing.expect((try downstreamImpactPaths(a, &consumers, "summary", 3, 100)).complete);
    // The row budget also reports incompleteness.
    try std.testing.expect(!(try downstreamImpactPaths(a, &consumers, "summary", 8, 2)).complete);
}

pub fn appendImpactRow(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), emitted: *usize, table_id: []const u8, doc_key: []const u8, artifact: []const u8, version: u64, current: bool, freshness: []const u8, path: []const []const u8) !void {
    if (emitted.* != 0) try out.append(alloc, ',');
    emitted.* += 1;
    try out.appendSlice(alloc, "{\"table_id\":");
    try ctx.appendJsonString(alloc, out, table_id);
    try out.appendSlice(alloc, ",\"key\":");
    try ctx.appendJsonString(alloc, out, doc_key);
    try out.appendSlice(alloc, ",\"artifact\":");
    try ctx.appendJsonString(alloc, out, artifact);
    if (version != 0) {
        try out.print(alloc, ",\"version\":{d},\"historical\":{}", .{ version, !current });
    } else try out.appendSlice(alloc, ",\"live_candidate\":true");
    try out.appendSlice(alloc, ",\"freshness\":");
    try ctx.appendJsonString(alloc, out, freshness);
    try out.appendSlice(alloc, ",\"path\":[");
    for (path, 0..) |p, i| {
        if (i != 0) try out.append(alloc, ',');
        try ctx.appendJsonString(alloc, out, p);
    }
    try out.appendSlice(alloc, "]}");
}

