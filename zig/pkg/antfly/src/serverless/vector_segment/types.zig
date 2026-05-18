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
const vector_types = @import("antfly_vector").vector;

pub const Entry = struct {
    doc_id: []u8,
    vector: []f32,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.vector);
        self.* = undefined;
    }
};

pub const Cluster = struct {
    centroid: []f32,
    start_index: u32,
    entry_count: u32,
    routing_distance_min: f32 = 0,
    routing_distance_max: f32 = 0,
    routing_distance_avg: f32 = 0,
    quantized_offset: u32 = 0,
    quantized_len: u32 = 0,
    exact_entries_offset: u32 = 0,
    exact_entries_len: u32 = 0,
    quantized_set: []u8,
    exact_entries: []u8,

    pub fn deinit(self: *Cluster, alloc: Allocator) void {
        alloc.free(self.centroid);
        alloc.free(self.quantized_set);
        alloc.free(self.exact_entries);
        self.* = undefined;
    }
};

pub const Segment = struct {
    dims: u32,
    metric: vector_types.DistanceMetric = .cosine,
    base_probe_count: u32 = 2,
    shortlist_multiplier: u32 = 2,
    clusters: []Cluster,
    entries: []Entry,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        for (self.clusters) |*cluster| cluster.deinit(alloc);
        alloc.free(self.clusters);
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        self.* = undefined;
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

test "freeSegment releases owned vector segment entries" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .dims = 2,
        .metric = .cosine,
        .base_probe_count = 2,
        .shortlist_multiplier = 2,
        .clusters = try alloc.alloc(Cluster, 1),
        .entries = try alloc.alloc(Entry, 1),
    };
    segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 1.0, 2.0 }),
        .start_index = 0,
        .entry_count = 1,
        .routing_distance_min = 0.25,
        .routing_distance_max = 0.75,
        .routing_distance_avg = 0.5,
        .quantized_set = try alloc.dupe(u8, "quantized"),
        .exact_entries = try alloc.dupe(u8, "exact"),
    };
    segment.entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .vector = try alloc.dupe(f32, &.{ 1.0, 2.0 }),
    };
    freeSegment(alloc, &segment);
}
