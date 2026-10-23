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

pub const pipeline = @import("pipeline.zig");
pub const worker = @import("worker.zig");
pub const operation_id = @import("operation_id.zig");

pub const BuiltinPipeline = pipeline.BuiltinPipeline;
pub const StageSpec = pipeline.StageSpec;
pub const builtinPipelineForPolicy = pipeline.builtinPipelineForPolicy;
pub const SparseEnricher = worker.SparseEnricher;
pub const EnrichmentRunStats = worker.EnrichmentRunStats;
pub const lexical_sparse_enrichment_version = worker.lexical_sparse_enrichment_version;
pub const chunk_preview_enrichment_version = worker.chunk_preview_enrichment_version;
pub const chunk_embeddings_enrichment_version = worker.chunk_embeddings_enrichment_version;
pub const rerank_terms_enrichment_version = worker.rerank_terms_enrichment_version;

test "serverless enrichment module compiles" {
    _ = pipeline;
    _ = worker;
    _ = BuiltinPipeline;
    _ = StageSpec;
    _ = builtinPipelineForPolicy;
    _ = SparseEnricher;
    _ = EnrichmentRunStats;
    _ = lexical_sparse_enrichment_version;
    _ = chunk_preview_enrichment_version;
    _ = chunk_embeddings_enrichment_version;
    _ = rerank_terms_enrichment_version;
}
