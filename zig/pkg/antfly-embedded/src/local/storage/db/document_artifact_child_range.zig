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

//! Wire-safe child-range application payload shared with API dispatch.

const types = @import("types.zig");
const derived_types = @import("derived/derived_types.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");

pub const ApplyBatch = struct {
    artifact_writes: []const types.BatchWrite = &.{},
    artifact_delete_keys: []const []const u8 = &.{},
    documents: []const derived_types.DerivedDocument = &.{},
    dense_embeddings: []const derived_types.DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const derived_types.DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},
    sync_level: types.SyncLevel = .full_index,
};
