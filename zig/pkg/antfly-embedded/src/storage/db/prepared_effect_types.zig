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

//! Plain generated-effect data shared by preparation, durable codecs and
//! materialization. Keep physical DB execution owners out of this import graph.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const derived_types = @import("derived/derived_types.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");

pub const DerivedCoverageOutcome = enum { produced, skipped, terminal_failed };

pub const PrecomputedCoverageOutcome = struct {
    index_name: []u8,
    doc_key: []u8,
    outcome: DerivedCoverageOutcome,

    pub fn deinit(self: @This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.doc_key);
    }
};

pub const PrecomputedGeneratedBatch = struct {
    artifact_writes: []types.BatchWrite = &.{},
    artifact_delete_keys: []const []const u8 = &.{},
    documents: []const derived_types.DerivedDocument = &.{},
    dense_embeddings: []const derived_types.DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const derived_types.DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},
    coverage_outcomes: []PrecomputedCoverageOutcome = &.{},

    pub fn deinit(self: *PrecomputedGeneratedBatch, alloc: Allocator) void {
        for (self.artifact_writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.artifact_delete_keys) |key| alloc.free(key);
        if (self.artifact_delete_keys.len > 0) alloc.free(self.artifact_delete_keys);

        var derived_batch = derived_types.DerivedBatch{
            .documents = self.documents,
            .dense_embeddings = self.dense_embeddings,
            .sparse_embeddings = self.sparse_embeddings,
            .generated_enrichment_refs = self.generated_enrichment_refs,
        };
        derived_types.deinitDerivedBatch(alloc, &derived_batch);
        for (self.coverage_outcomes) |outcome| outcome.deinit(alloc);
        if (self.coverage_outcomes.len > 0) alloc.free(self.coverage_outcomes);
        self.* = undefined;
    }
};
