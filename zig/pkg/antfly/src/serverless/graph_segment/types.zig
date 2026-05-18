// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Edge = struct {
    neighbor_id: []u8,
    edge_type: []u8,
    weight: f32,

    pub fn deinit(self: *Edge, alloc: Allocator) void {
        alloc.free(self.neighbor_id);
        alloc.free(self.edge_type);
        self.* = undefined;
    }
};

pub const Adjacency = struct {
    node_id: []u8,
    out_edges: []Edge,
    in_edges: []Edge,

    pub fn deinit(self: *Adjacency, alloc: Allocator) void {
        alloc.free(self.node_id);
        for (self.out_edges) |*edge| edge.deinit(alloc);
        alloc.free(self.out_edges);
        for (self.in_edges) |*edge| edge.deinit(alloc);
        alloc.free(self.in_edges);
        self.* = undefined;
    }
};

pub const Segment = struct {
    adjacencies: []Adjacency,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        for (self.adjacencies) |*adjacency| adjacency.deinit(alloc);
        alloc.free(self.adjacencies);
        self.* = undefined;
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

test "graph segment types free owned storage" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .adjacencies = try alloc.alloc(Adjacency, 1),
    };
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, "doc-a"),
        .out_edges = try alloc.alloc(Edge, 1),
        .in_edges = try alloc.alloc(Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, "doc-b"),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    freeSegment(alloc, &segment);
}
