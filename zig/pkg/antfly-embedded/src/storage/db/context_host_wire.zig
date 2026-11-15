// Copyright 2026 Antfly, Inc.
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

//! Wire vocabulary of the context-dependency host call (see
//! context_host_json.zig). Dependency-free so API-layer hosts can answer
//! without linking storage internals.

const std = @import("std");
const Allocator = std.mem.Allocator;
const resolution = @import("enrichment/context_resolution.zig");

pub const Op = enum(u32) {
    /// Request: command JSON. Response: empty once applied on this replica.
    propose = 1,
    /// Response: "1" when this replica leads the group.
    is_leader = 2,
    /// Request: `{"dependent","notice"}`.
    deliver = 3,
    /// Request: `{"table","key"}`. Response: `{"value":string|null}`.
    lookup = 4,
    /// Request: `{"table","envelope","offset","limit"}`. Response: page.
    query = 5,
    /// Request: `{"table","request","k"}`. Response: page.
    request = 6,
    /// Response: `{"table_id","inbound":[DeclaredConfig]}`.
    topology = 7,
    /// Request: `{"table","graph_index","node","edge_types","direction","limit","include_generated"}`.
    /// Response: `[{"edge_type","neighbor","outgoing"}]`.
    edges = 8,
    /// Request: `{"table","key","names"}`. Response: `{"<artifact>":value|null}`.
    artifacts = 9,
    /// Request: `{"table","envelope","after","limit"}`: the page after key
    /// `after` (exclusive; "" from the start). Response: page.
    scan = 10,
};

/// A group-local context read (lookup mode `context_read`), routed by key
/// to the group owning it.
pub const ReadRequest = struct {
    op: enum { edges, artifacts },
    key: []const u8,
    graph_index: []const u8 = "",
    edge_types: []const []const u8 = &.{},
    direction: @import("enrichment/context_sources.zig").GraphSpec.Direction = .out,
    limit: u32 = 0,
    include_generated: bool = false,
    names: []const []const u8 = &.{},
};

/// Page response: `{"rows":[{"key","value"}],"exhausted":bool}`.
pub const WirePage = struct {
    rows: []const struct { key: []const u8, value: []const u8 } = &.{},
    exhausted: bool = true,
};

/// Encode a resolution page for the wire (hosts answering query/request).
pub fn encodePageAlloc(alloc: Allocator, rows: []const resolution.Row, exhausted: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"rows\":[");
    for (rows, 0..) |row, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "{{\"key\":{f},\"value\":{f}}}", .{ std.json.fmt(row.key, .{}), std.json.fmt(row.value, .{}) });
    }
    try out.print(alloc, "],\"exhausted\":{}}}", .{exhausted});
    return out.toOwnedSlice(alloc);
}
