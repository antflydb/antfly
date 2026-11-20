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

//! Graph effects use reserved transaction intent addresses. Their envelopes
//! survive prepare/restart and are consumed by ordinary native batch commit.
const std = @import("std");
const types = @import("types.zig");
const txn = @import("../transactions.zig");
const A = std.mem.Allocator;
pub const prefix = "\x00\x00__metadata__:graph-transaction-effect:";
pub const Effect = union(enum) { write: types.GraphEdgeWrite, delete: types.GraphEdgeDelete };
const Envelope = struct { version: u8 = 1, effect: Effect };
pub fn isEffect(key: []const u8) bool {
    return std.mem.startsWith(u8, key, prefix);
}
pub fn address(a: A, effect: Effect) ![]const u8 {
    const json = switch (effect) {
        .write => |edge| try std.json.Stringify.valueAlloc(a, .{ edge.index_name, edge.source, edge.target, edge.edge_type, edge.edge_id, edge.owner_document, edge.owner }, .{}),
        .delete => |edge| try std.json.Stringify.valueAlloc(a, .{ edge.index_name, edge.source, edge.target, edge.edge_type, edge.edge_id, edge.owner_document, edge.owner }, .{}),
    };
    defer a.free(json);
    var digest: [32]u8 = undefined;
    @import("antfly_hash").Sha256.hash(json, &digest, .{});
    return std.fmt.allocPrint(a, "{s}{s}", .{ prefix, std.fmt.bytesToHex(digest, .lower) });
}
pub const Prepare = struct {
    owners: std.ArrayList(txn.WriteIntent) = .empty,
    policy_sources: std.ArrayList(txn.WriteIntent) = .empty,
    seen_owners: std.StringHashMapUnmanaged(void) = .empty,
    seen_policy_sources: std.StringHashMapUnmanaged(void) = .empty,
    include_policy_sources: bool,

    /// Effects are already typed at prepare. Capture dependencies before
    /// serializing the durable intent. Resolution still verifies the envelope.
    pub fn append(self: *Prepare, a: A, list_allocator: A, intents: *std.ArrayListUnmanaged(txn.WriteIntent), effect: Effect) !void {
        switch (effect) {
            inline else => |edge| {
                const owner = edge.producingDocument();
                if (!(try self.seen_owners.getOrPut(a, owner)).found_existing)
                    try self.owners.append(a, .{ .key = owner, .value = null });
                if (self.include_policy_sources) {
                    for ([_][]const u8{ owner, edge.source }) |document| {
                        if (!(try self.seen_policy_sources.getOrPut(a, document)).found_existing)
                            try self.policy_sources.append(a, .{ .key = document, .value = null });
                    }
                }
            },
        }
        try intents.append(list_allocator, .{
            .key = try address(a, effect),
            .value = try std.json.Stringify.valueAlloc(a, Envelope{ .effect = effect }, .{}),
        });
    }
};

pub fn decode(a: A, key: []const u8, value: []const u8) !Effect {
    if (!isEffect(key)) return error.InvalidTxnRecord;
    const envelope = try std.json.parseFromSliceLeaky(Envelope, a, value, .{});
    if (envelope.version != 1) return error.InvalidTxnRecord;
    const expected = try address(a, envelope.effect);
    defer a.free(expected);
    if (!std.mem.eql(u8, expected, key)) return error.InvalidTxnRecord;
    return envelope.effect;
}
pub const Batch = struct {
    writes: []const types.BatchWrite,
    deletes: []const []const u8,
    graph_writes: []const types.GraphEdgeWrite,
    graph_deletes: []const types.GraphEdgeDelete,
};
/// All returned buffers and parsed strings belong to the preparation arena.
pub fn materialize(a: A, intents: txn.IntentBatch) !Batch {
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    var deletes: std.ArrayList([]const u8) = .empty;
    var graph_writes: std.ArrayList(types.GraphEdgeWrite) = .empty;
    var graph_deletes: std.ArrayList(types.GraphEdgeDelete) = .empty;
    for (intents.writes) |write| {
        if (!isEffect(write.key)) {
            try writes.append(a, .{ .key = write.key, .value = write.value });
            continue;
        }
        switch (try decode(a, write.key, write.value)) {
            .write => |edge| try graph_writes.append(a, edge),
            .delete => |edge| try graph_deletes.append(a, edge),
        }
    }
    for (intents.deletes) |key| {
        if (isEffect(key)) return error.InvalidTxnRecord;
        try deletes.append(a, key);
    }
    return .{ .writes = try writes.toOwnedSlice(a), .deletes = try deletes.toOwnedSlice(a), .graph_writes = try graph_writes.toOwnedSlice(a), .graph_deletes = try graph_deletes.toOwnedSlice(a) };
}
