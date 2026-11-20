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

//! Convert final visible graph-record changes into authority replacements.
//! Callers must resolve source precedence and validate the physical write set
//! before using this adapter.
const std = @import("std");
const authority = @import("../../graph/dag_authority.zig");
const edges = @import("graph_edge_types.zig");
const keys = @import("../internal_keys.zig");
const A = std.mem.Allocator;
const Replacement = authority.Contributions.Replacement;

/// Result strings and arrays belong to the caller's preparation arena.
pub fn collect(
    alloc: A,
    index: []const u8,
    edge_type: []const u8,
    writes: []const edges.GraphEdgeWrite,
    deletes: []const edges.GraphEdgeDelete,
) ![]const Replacement {
    var result: std.ArrayList(Replacement) = .empty;
    errdefer {
        for (result.items) |replacement| alloc.free(replacement.targets);
        result.deinit(alloc);
    }
    for (writes) |edge| {
        if (!matches(edge, index, edge_type)) continue;
        try result.ensureUnusedCapacity(alloc, 1);
        const targets = try alloc.alloc([]const u8, 1);
        targets[0] = edge.target;
        result.appendAssumeCapacity(.{ .owner = owner(edge), .source = edge.source, .targets = targets });
    }
    for (deletes) |edge| {
        if (!matches(edge, index, edge_type)) continue;
        try result.append(alloc, .{ .owner = owner(edge), .source = edge.source, .targets = &.{} });
    }
    std.mem.sort(Replacement, result.items, {}, lessThan);
    for (result.items, 0..) |replacement, i| {
        if (i != 0 and !lessThan({}, result.items[i - 1], replacement)) return error.InvalidDagMutation;
    }
    return result.toOwnedSlice(alloc);
}

const activation = @import("dag_activation_contract.zig");

/// Read accepted physical graph artifacts from a single fenced root. The
/// caller supplies the currently admitted catalog generation and owns the arena.
/// The cursor advances over skipped rows as well as matching relationships.
pub fn scanAccepted(alloc: A, txn: anytype, request: activation.ScanRequest, start: []const u8, end: []const u8) !activation.ScanPage {
    if (request.index_name.len == 0 or request.edge_type.len == 0 or request.generation == 0)
        return error.InvalidDagMutation;
    const current = (try authority.SourceFence.read(txn)) orelse return error.GraphConstraintNotReady;
    if (!std.meta.eql(current, request.fence)) return error.GraphConstraintAttemptConflict;

    // Read the generation from the same snapshot as the source fence.
    const catalog = (try @import("artifact_inventory.zig").load(alloc, txn)) orelse
        return error.GraphConstraintNotReady;
    defer catalog.deinit();
    if (!std.mem.eql(u8, &catalog.value.command.namespace, &request.namespace))
        return error.GraphConstraintRevisionChanged;
    var indexes = try @import("artifact_catalog_view.zig").Iterator.init(catalog.value.command.catalogs.indexes);
    const generation = while (try indexes.next()) |index| {
        if (index.kind == 3 and std.mem.eql(u8, index.name, request.index_name)) break index.generation;
    } else return error.GraphConstraintRevisionChanged;
    if (generation != request.generation) return error.GraphConstraintRevisionChanged;

    const lower = try keys.documentRangeLowerAlloc(alloc, start);
    const upper = if (end.len != 0)
        try keys.documentRangeLowerAlloc(alloc, end)
    else
        (try keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidDagAuthority;
    if (request.after) |after| {
        if (std.mem.order(u8, after, lower) == .lt or std.mem.order(u8, after, upper) != .lt)
            return error.InvalidDagMutation;
    }

    const max_rows = 256;
    const max_bytes = activation.max_source_page_bytes;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    cursor.setUpperBound(upper);
    var entry = try cursor.seekAtOrAfter(request.after orelse lower);
    if (entry) |row| {
        if (request.after) |after| if (std.mem.eql(u8, after, row.key)) {
            entry = try cursor.next();
        };
    }

    var result: std.ArrayList(Replacement) = .empty;
    var next: ?[]const u8 = null;
    var visited: usize = 0;
    var bytes: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (std.mem.order(u8, row.key, upper) != .lt) {
            entry = null;
            break;
        }
        const extent = row.key.len +| (if (keys.isGraphEdgeArtifactKey(row.key)) row.value.len else 0);
        if (visited == max_rows or (visited != 0 and extent > max_bytes - bytes)) break;
        if (extent > max_bytes) return error.GraphConstraintCapacity;
        visited += 1;
        bytes += extent;
        if (next) |previous| alloc.free(previous);
        next = try alloc.dupe(u8, row.key);
        if (try artifactReplacement(alloc, request.index_name, request.edge_type, request.generation, row.key, row.value, true)) |replacement|
            try result.append(alloc, replacement);
    }
    std.mem.sort(Replacement, result.items, {}, lessThan);
    return .{ .next = next, .exhausted = entry == null, .replacements = try result.toOwnedSlice(alloc) };
}

/// Translate a canonical physical relationship afterimage or retirement
/// preimage using the same identity as logical producer writes. The caller
/// supplies the fenced catalog generation and an allocator for the result.
pub fn artifactReplacement(
    alloc: A,
    index_name: []const u8,
    edge_type: []const u8,
    generation: u64,
    key: []const u8,
    value: []const u8,
    present: bool,
) !?Replacement {
    if (!keys.matchesGraphEdgeIndexName(key, index_name)) return null;
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const parsed = (try keys.parseGraphEdgeArtifactKeyAlloc(scratch.allocator(), key)) orelse return error.InvalidDagAuthority;
    if (!std.mem.eql(u8, parsed.edge_type, edge_type)) return null;
    const codec = @import("enrichment/artifact_codec.zig");
    const record = try codec.decodeGraphEdgeBorrowed(value);
    if (record.generation == 0 or codec.isPortableUnboundGraphEdge(value)) return error.GraphConstraintNotReady;
    if (record.generation != generation) return null;
    const identity: edges.GraphEdgeWrite = .{
        .index_name = parsed.index_name,
        .edge_type = parsed.edge_type,
        .source = parsed.source_node orelse parsed.doc_key,
        .target = parsed.target_doc_key,
        .owner = parsed.doc_key,
        .edge_id = parsed.edge_id,
    };
    const source = try alloc.dupe(u8, identity.source);
    errdefer alloc.free(source);
    if (!present) return .{ .owner = owner(identity), .source = source, .targets = &.{} };
    const targets = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(targets);
    targets[0] = try alloc.dupe(u8, identity.target);
    return .{ .owner = owner(identity), .source = source, .targets = targets };
}

fn matches(edge: anytype, index: []const u8, edge_type: []const u8) bool {
    return std.mem.eql(u8, index, edge.index_name) and std.mem.eql(u8, edge_type, edge.edge_type);
}

fn owner(edge: anytype) authority.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/dag-visible-relationship/v1");
    // A named relationship can have several different endpoint rows over its
    // lifetime. Each row has its own membership, so retiring the old endpoint
    // and installing the new one are distinct records in the final delta.
    for ([_][]const u8{ edge.producingDocument(), edge.target, edge.edge_id }) |part| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, part.len, .little);
        hash.update(&size);
        hash.update(part);
    }
    return hash.finalResult();
}

fn lessThan(_: void, left: Replacement, right: Replacement) bool {
    return switch (std.mem.order(u8, &left.owner, &right.owner)) {
        .lt => true,
        .gt => false,
        .eq => std.mem.order(u8, left.source, right.source) == .lt,
    };
}

test "DAG final graph effects retain relationship identity across publication and retirement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const writes: []const edges.GraphEdgeWrite = &.{
        .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner = "first" },
        .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner_document = "second", .edge_id = "x" },
        .{ .index_name = "g", .edge_type = "S", .source = "b", .target = "a" },
    };
    const added = try collect(alloc, "g", "R", writes, &.{});
    try std.testing.expectEqual(@as(usize, 2), added.len);
    try std.testing.expect(!std.mem.eql(u8, &added[0].owner, &added[1].owner));
    const delete: edges.GraphEdgeDelete = .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner = "first" };
    const removed = try collect(alloc, "g", "R", &.{}, &.{delete});
    try std.testing.expectEqual(@as(usize, 1), removed.len);
    try std.testing.expectEqualDeep(owner(writes[0]), removed[0].owner);
    try std.testing.expectEqual(@as(usize, 0), removed[0].targets.len);
    try std.testing.expectError(error.InvalidDagMutation, collect(alloc, "g", "R", writes, &.{delete}));
    const physical = try keys.graphRelationshipArtifactKeyAlloc(alloc, "second", "g", "R", "b", "a", "x");
    const value = try @import("enrichment/artifact_codec.zig").encodeGraphEdgeAlloc(alloc, null, 7, 1, 0, 0, "{}");
    const installed = (try artifactReplacement(alloc, "g", "R", 7, physical, value, true)).?;
    try std.testing.expectEqualDeep(owner(writes[1]), installed.owner);
    try std.testing.expectEqualStrings(writes[1].source, installed.source);
    try std.testing.expectEqualStrings(writes[1].target, installed.targets[0]);
    const retired = (try artifactReplacement(alloc, "g", "R", 7, physical, value, false)).?;
    try std.testing.expectEqualDeep(installed.owner, retired.owner);
    try std.testing.expectEqualStrings(installed.source, retired.source);
    try std.testing.expectEqual(@as(usize, 0), retired.targets.len);
    try std.testing.expect((try artifactReplacement(alloc, "g", "R", 8, physical, value, true)) == null);
}
