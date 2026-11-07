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

//! Owner-pinned graph adjacency over the same primary statement cut. Requests
//! contain no replacement snapshot or visibility selector: every page belongs
//! to the source and graph epochs captured together before apply resumes.
const std = @import("std");
pub const graph = @import("../graph/graph.zig");
pub const Proof = struct {
    source_sequence: u64,
    source_graph_sequence: u64,
    applied_sequence: u64,
    config_hash: u64,
    graph_generation: u64,
    reverse_generation: u64,
    owner_generation: u64,
    ownership_fingerprint: u64,
    ttl_read_at_ns: u64,

    pub fn validate(self: @This()) !void {
        if (self.source_graph_sequence > self.applied_sequence or self.applied_sequence > self.source_sequence or self.config_hash == 0 or self.graph_generation == 0) return error.SqlStatementSnapshotRequired;
    }
};
pub const Request = struct {
    index: []const u8,
    key: []const u8,
    edge_types: []const []const u8 = &.{},
    direction: graph.EdgeDirection = .out,
    cursor: ?graph.EdgeScanCursor = null,
    limits: graph.EdgePageLimits = .{ .max_edges = 128, .max_owned_bytes = 4 << 20 },
    execution_deadline_ns: ?u64 = null,
    cancellation: ?@import("antfly_cancellation").CancellationToken = null,

    pub fn check(self: @This()) !void {
        if (self.cancellation) |token| try token.check();
        if (self.execution_deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (self.index.len == 0 or self.index.len > 256 or self.key.len == 0 or self.key.len > 65536 or self.edge_types.len > 64) return error.InvalidQueryRequest;
        if (self.limits.max_edges == 0 or self.limits.max_edges > 10000 or self.limits.max_owned_bytes == 0 or self.limits.max_owned_bytes > 16 << 20 or self.limits.max_scanned_rows == 0 or self.limits.max_scanned_rows > 1000000) return error.InvalidQueryRequest;
    }
};
pub const Page = struct {
    proof: Proof,
    page: graph.EdgePage,
    /// Private paired-cut producer facts, one set per edge. Null is an older
    /// receiver and never certifies source ownership to a policy-aware reader.
    owner_documents: ?[][][]u8 = null,
    pub fn clone(self: @This(), alloc: std.mem.Allocator) !@This() {
        const edges = try alloc.alloc(graph.Edge, self.page.edges.len);
        var initialized: usize = 0;
        errdefer {
            for (edges[0..initialized]) |edge| graph.GraphIndex.freeEdge(alloc, edge);
            alloc.free(edges);
        }
        for (self.page.edges, edges) |edge, *out| {
            out.* = edge;
            const fields = .{ "source", "target", "edge_type", "edge_id", "owner_document", "metadata", "winner_key_hex" };
            var strings: usize = 0;
            errdefer inline for (fields, 0..) |field, position| {
                if (position < strings) alloc.free(@field(out, field));
            };
            inline for (fields) |field| {
                @field(out, field) = try alloc.dupe(u8, @field(edge, field));
                strings += 1;
            }
            initialized += 1;
        }
        const owners: ?[][][]u8 = if (self.owner_documents) |groups| copied: {
            if (groups.len != edges.len) return error.InvalidGraphSegment;
            const output = try alloc.alloc([][]u8, groups.len);
            var complete: usize = 0;
            errdefer {
                for (output[0..complete]) |group| {
                    for (group) |owner| alloc.free(owner);
                    alloc.free(group);
                }
                alloc.free(output);
            }
            for (groups, output) |group, *out| {
                const strings = try alloc.alloc([]u8, group.len);
                var count: usize = 0;
                errdefer {
                    for (strings[0..count]) |value| alloc.free(value);
                    alloc.free(strings);
                }
                for (group, strings) |value, *string| {
                    string.* = try alloc.dupe(u8, value);
                    count += 1;
                }
                out.* = strings;
                complete += 1;
            }
            break :copied output;
        } else null;
        errdefer if (owners) |groups| {
            for (groups) |group| {
                for (group) |owner| alloc.free(owner);
                alloc.free(group);
            }
            alloc.free(groups);
        };
        return .{ .proof = self.proof, .owner_documents = owners, .page = .{
            .edges = edges,
            .next_cursor = if (self.page.next_cursor) |cursor| try cursor.clone(alloc) else null,
            .owned_bytes = self.page.owned_bytes,
            .scanned_rows = self.page.scanned_rows,
            .byte_limit_hit = self.page.byte_limit_hit,
        } };
    }
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.page.deinit(alloc);
        if (self.owner_documents) |groups| {
            for (groups) |group| {
                for (group) |owner| alloc.free(owner);
                alloc.free(group);
            }
            alloc.free(groups);
        }
        self.* = undefined;
    }
};

test "retained graph source proof rejects lagging or future graph projection" {
    const good: Proof = .{ .source_sequence = 12, .source_graph_sequence = 10, .applied_sequence = 11, .config_hash = 7, .graph_generation = 1, .reverse_generation = 1, .owner_generation = 0, .ownership_fingerprint = 0, .ttl_read_at_ns = 99 };
    try good.validate();
    var lagging = good;
    lagging.applied_sequence = 9;
    try std.testing.expectError(error.SqlStatementSnapshotRequired, lagging.validate());
    var future = good;
    future.applied_sequence = 13;
    try std.testing.expectError(error.SqlStatementSnapshotRequired, future.validate());
    var unversioned = good;
    unversioned.graph_generation = 0;
    try std.testing.expectError(error.SqlStatementSnapshotRequired, unversioned.validate());
}

test "retained graph page copies every owned edge and continuation without leaking on allocation failure" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const owners: [1][]u8 = .{@constCast("producing:fact")};
            const groups: [1][][]u8 = .{@constCast(&owners)};
            const original: Page = .{ .owner_documents = @constCast(&groups), .proof = .{
                .source_sequence = 12,
                .source_graph_sequence = 10,
                .applied_sequence = 11,
                .config_hash = 7,
                .graph_generation = 1,
                .reverse_generation = 1,
                .owner_generation = 0,
                .ownership_fingerprint = 0,
                .ttl_read_at_ns = 99,
            }, .page = .{
                .edges = @constCast(&[_]graph.Edge{.{ .source = "root", .target = "old", .edge_type = "cites", .weight = 2.5, .created_at = 0, .updated_at = 0, .metadata = "{}" }}),
                .next_cursor = .{ .direction = .out, .type_index = 0, .edge_type = @constCast("cites"), .adjacent_key = @constCast("old") },
                .owned_bytes = 123,
                .scanned_rows = 1,
            } };
            var owned = try original.clone(alloc);
            defer owned.deinit(alloc);
            try std.testing.expectEqualDeep(original.proof, owned.proof);
            try std.testing.expectEqualStrings("old", owned.page.edges[0].target);
            try std.testing.expectEqual(@as(f64, 2.5), owned.page.edges[0].weight);
            try std.testing.expectEqualStrings("old", owned.page.next_cursor.?.adjacent_key);
            try std.testing.expectEqualStrings("producing:fact", owned.owner_documents.?[0][0]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
