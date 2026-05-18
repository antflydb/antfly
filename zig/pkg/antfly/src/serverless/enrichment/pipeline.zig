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

const catalog_types = @import("../catalog/types.zig");

pub const StageSpec = struct {
    stage: catalog_types.EnrichmentStage,
    pipeline_version: u32,
    model_preference: catalog_types.EnrichmentModelPreference,
    publish_min_pending_records: u64,
};

pub const BuiltinPipeline = struct {
    stages: [4]StageSpec = undefined,
    len: usize = 0,

    pub fn slice(self: *const BuiltinPipeline) []const StageSpec {
        return self.stages[0..self.len];
    }

    pub fn stageSpec(self: *const BuiltinPipeline, stage: catalog_types.EnrichmentStage) ?StageSpec {
        for (self.slice()) |spec| {
            if (spec.stage == stage) return spec;
        }
        return null;
    }
};

pub fn builtinPipelineForPolicy(policy: catalog_types.NamespacePolicy) BuiltinPipeline {
    var pipeline = BuiltinPipeline{};
    if (policy.enrichment_enabled) {
        pipeline.stages[pipeline.len] = .{
            .stage = .lexical_sparse,
            .pipeline_version = policy.enrichment_pipeline_version,
            .model_preference = policy.lexical_sparse_model_preference,
            .publish_min_pending_records = policy.enrichment_publish_min_pending_records,
        };
        pipeline.len += 1;
    }
    if (policy.chunk_preview_enabled) {
        pipeline.stages[pipeline.len] = .{
            .stage = .chunk_preview,
            .pipeline_version = policy.chunk_preview_pipeline_version,
            .model_preference = .deterministic_only,
            .publish_min_pending_records = policy.chunk_preview_publish_min_pending_records,
        };
        pipeline.len += 1;
    }
    if (policy.chunk_embeddings_enabled) {
        pipeline.stages[pipeline.len] = .{
            .stage = .chunk_embeddings,
            .pipeline_version = policy.chunk_embeddings_pipeline_version,
            .model_preference = policy.chunk_embeddings_model_preference,
            .publish_min_pending_records = policy.chunk_embeddings_publish_min_pending_records,
        };
        pipeline.len += 1;
    }
    if (policy.rerank_terms_enabled) {
        pipeline.stages[pipeline.len] = .{
            .stage = .rerank_terms,
            .pipeline_version = policy.rerank_terms_pipeline_version,
            .model_preference = .deterministic_only,
            .publish_min_pending_records = policy.rerank_terms_publish_min_pending_records,
        };
        pipeline.len += 1;
    }
    return pipeline;
}

test "builtin pipeline preserves serverless enrichment stage order" {
    const policy = catalog_types.NamespacePolicy{
        .enrichment_enabled = true,
        .chunk_preview_enabled = true,
        .chunk_embeddings_enabled = true,
        .rerank_terms_enabled = true,
    };
    const pipeline = builtinPipelineForPolicy(policy);
    try @import("std").testing.expectEqual(@as(usize, 4), pipeline.slice().len);
    try @import("std").testing.expectEqual(catalog_types.EnrichmentStage.lexical_sparse, pipeline.slice()[0].stage);
    try @import("std").testing.expectEqual(catalog_types.EnrichmentStage.chunk_preview, pipeline.slice()[1].stage);
    try @import("std").testing.expectEqual(catalog_types.EnrichmentStage.chunk_embeddings, pipeline.slice()[2].stage);
    try @import("std").testing.expectEqual(catalog_types.EnrichmentStage.rerank_terms, pipeline.slice()[3].stage);
}
