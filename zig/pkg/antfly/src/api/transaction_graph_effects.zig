// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Owned graph deltas for staged transaction merging and savepoint cloning.
const std = @import("std");
const local = @import("antfly_local_sources");
const batch_api = local.api_batch;
const types = local.storage_db_types;
fn same(left: anytype, right: anytype) bool {
    inline for (.{ "index_name", "source", "target", "edge_type", "edge_id", "owner_document", "owner" }) |field| if (!std.mem.eql(u8, @field(left, field), @field(right, field))) return false;
    return true;
}
fn appendOwned(comptime T: type, a: std.mem.Allocator, list: *std.ArrayList(T), value: T) !void {
    var owned = try value.cloneAlloc(a);
    errdefer owned.deinit(a);
    try list.append(a, owned);
}
fn remove(comptime T: type, a: std.mem.Allocator, list: *std.ArrayList(T), target: anytype) void {
    var index: usize = 0;
    while (index < list.items.len) {
        if (same(list.items[index], target)) {
            var removed = list.orderedRemove(index);
            removed.deinit(a);
        } else index += 1;
    }
}
pub fn merge(a: std.mem.Allocator, batch: *batch_api.OwnedBatchRequest, incoming_writes: []const types.GraphEdgeWrite, incoming_deletes: []const types.GraphEdgeDelete) !void {
    if (incoming_writes.len == 0 and incoming_deletes.len == 0) return;
    var writes: std.ArrayList(types.GraphEdgeWrite) = .empty;
    defer {
        for (writes.items) |*edge| edge.deinit(a);
        writes.deinit(a);
    }
    var deletes: std.ArrayList(types.GraphEdgeDelete) = .empty;
    defer {
        for (deletes.items) |*edge| edge.deinit(a);
        deletes.deinit(a);
    }
    for (batch.graph_writes) |edge| try appendOwned(types.GraphEdgeWrite, a, &writes, edge);
    for (batch.graph_deletes) |edge| try appendOwned(types.GraphEdgeDelete, a, &deletes, edge);
    for (incoming_writes) |edge| {
        remove(types.GraphEdgeWrite, a, &writes, edge);
        remove(types.GraphEdgeDelete, a, &deletes, edge);
        try appendOwned(types.GraphEdgeWrite, a, &writes, edge);
    }
    for (incoming_deletes) |edge| {
        remove(types.GraphEdgeWrite, a, &writes, edge);
        remove(types.GraphEdgeDelete, a, &deletes, edge);
        try appendOwned(types.GraphEdgeDelete, a, &deletes, edge);
    }
    const next_writes = try writes.toOwnedSlice(a);
    errdefer {
        for (next_writes) |*edge| edge.deinit(a);
        a.free(next_writes);
    }
    const next_deletes = try deletes.toOwnedSlice(a);
    for (batch.graph_writes) |*edge| edge.deinit(a);
    for (batch.graph_deletes) |*edge| edge.deinit(a);
    a.free(batch.graph_writes);
    a.free(batch.graph_deletes);
    batch.graph_writes = next_writes;
    batch.graph_deletes = next_deletes;
}
