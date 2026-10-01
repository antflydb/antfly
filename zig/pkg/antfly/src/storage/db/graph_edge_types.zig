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

pub const GraphEdgeWrite = struct {
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64 = 1.0,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    ttl_created_ns: u64 = 0,
    metadata_json: []const u8 = "",
    /// Owning document for artifact-key routing, retirement, replacement
    /// manifests, and split ranges when it differs from the topological
    /// `source` (entity-sourced relations, zig/AUTOSCHEMA.md). Empty means
    /// `source` is the owner — the legacy shape every existing producer
    /// emits.
    owner: []const u8 = "",
};

pub const GraphEdgeDelete = struct {
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    /// Owning document of the durable edge artifact when it differs from the
    /// topological `source` (entity-sourced relations). Deleting such an edge
    /// must address the six-component artifact key owned by the producer;
    /// reconstructing a key from `source` alone would miss the row. Empty
    /// means `source` is the owner — the legacy shape.
    owner: []const u8 = "",
};
