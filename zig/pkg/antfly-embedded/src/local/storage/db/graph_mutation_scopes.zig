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

//! Point-addressed revision scopes fencing graph prefix phantoms. Extraction
//! uses keys only, so deletes/imports/cleanup need no previous-value lookup.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const graph = @import("online_graph_artifacts.zig");

/// Caller owns every returned key and the slice. A global contender embeds
/// its state owner as well as its physical edge owner; stamp both if distinct.
/// Canonical local contenders/manifests require a single physical owner.
pub fn countSentinelsAlloc(alloc: std.mem.Allocator, key: []const u8) ![][]u8 {
    if (!graph.isKey(key)) return error.InvalidGraphTransfer;
    const owner_end = keys.findComponentTerminator(key, 1) orelse return error.InvalidGraphTransfer;
    const owner = try keys.decodeBodyAlloc(alloc, key[1..owner_end]);
    defer alloc.free(owner);
    var index_start = owner_end + 3;
    if (keys.isGraphEdgeArtifactKey(key)) index_start = (keys.findComponentTerminator(key, index_start) orelse return error.InvalidGraphTransfer) + 2;
    const index_end = keys.findComponentTerminator(key, index_start) orelse return error.InvalidGraphTransfer;
    const index = try keys.decodeBodyAlloc(alloc, key[index_start..index_end]);
    defer alloc.free(index);
    const first = try keys.graphEdgeContenderCountKeyAlloc(alloc, owner, index);
    errdefer alloc.free(first);
    var second: ?[]u8 = null;
    errdefer if (second) |value| alloc.free(value);
    if (keys.isGraphGlobalEdgeContenderKey(key)) {
        const edge_start = index_end + 2 + 8;
        const edge_end = keys.findComponentTerminator(key, edge_start) orelse return error.InvalidGraphTransfer;
        const state_start = edge_end + 2 + 4;
        const state_end = keys.findComponentTerminator(key, state_start) orelse return error.InvalidGraphTransfer;
        const state = try keys.decodeBodyAlloc(alloc, key[state_start..state_end]);
        defer alloc.free(state);
        if (!keys.isGraphAssetStateRootKey(state) or !keys.matchesGraphAssetStateIndexName(state, index)) return error.InvalidGraphTransfer;
        const state_owner = (try keys.decodeDocumentComponentAlloc(alloc, state)) orelse return error.InvalidGraphTransfer;
        defer alloc.free(state_owner);
        if (!std.mem.eql(u8, owner, state_owner)) second = try keys.graphEdgeContenderCountKeyAlloc(alloc, state_owner, index);
    }
    const out = try alloc.alloc([]u8, if (second == null) 1 else 2);
    out[0] = first;
    if (second) |value| out[1] = value;
    return out;
}

test "ordered artifact inventory graph mutation scopes fence physical and embedded owners on tombstones" {
    const alloc = std.testing.allocator;
    const edge = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "physical", "g", "links", "target", "logical");
    defer alloc.free(edge);
    const state_prefix = try keys.graphAssetStateIndexPrefixAlloc(alloc, "other", "g");
    defer alloc.free(state_prefix);
    const state = try std.mem.concat(alloc, u8, &.{ state_prefix, "producer\x00\x00" });
    defer alloc.free(state);
    const global = try keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", 7, edge, 0, state);
    defer alloc.free(global);
    for ([_][]const u8{ edge, global }) |key| {
        const scopes = try countSentinelsAlloc(alloc, key);
        defer {
            for (scopes) |scope| alloc.free(scope);
            alloc.free(scopes);
        }
        try std.testing.expectEqual(@as(usize, if (key.ptr == edge.ptr) 1 else 2), scopes.len);
        const expected = try keys.graphEdgeContenderCountKeyAlloc(alloc, "physical", "g");
        defer alloc.free(expected);
        try std.testing.expectEqualSlices(u8, expected, scopes[0]);
        if (scopes.len == 2) {
            const other = try keys.graphEdgeContenderCountKeyAlloc(alloc, "other", "g");
            defer alloc.free(other);
            try std.testing.expectEqualSlices(u8, other, scopes[1]);
        }
    }
}
