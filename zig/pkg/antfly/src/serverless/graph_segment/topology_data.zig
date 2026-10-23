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

const std = @import("std");

pub const Edge = struct { source: u32, target: u32 };
pub const Topology = struct {
    node_ids: []const []const u8,
    edge_types: []const []const u8,
    string_bytes: []u8,
    edge_type_offsets: []const u32,
    edges: []const Edge,
    source_node_count: usize,
    source_edge_count: usize,
    retained_bytes: usize,
    type_checksums: []const [32]u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.node_ids);
        alloc.free(self.edge_types);
        alloc.free(self.string_bytes);
        alloc.free(self.edge_type_offsets);
        alloc.free(self.edges);
        alloc.free(self.type_checksums);
        self.* = undefined;
    }
};
