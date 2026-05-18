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

pub const pipeline = @import("pipeline.zig");
pub const worker = @import("worker.zig");

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
