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
}
