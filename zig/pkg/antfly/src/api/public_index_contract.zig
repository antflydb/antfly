// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

/// Public index kinds shared by request admission and response projection.
/// Keeping the field contract here prevents a field accepted on write from
/// being accidentally omitted on read, or an engine-owned field from leaking.
pub const Kind = enum {
    full_text,
    embeddings,
    graph,
    algebraic,
};

/// Closed object shapes used by CreatedIndex responses. Public response
/// projection must be driven by these positive contracts: stored catalog
/// documents can outlive request validation and may contain fields from older
/// or provider-specific representations.
pub const CreatedObjectShape = enum {
    unrestricted,
    provider,
    enrichments,
    enrichment,
    graph_source,
    graph_artifact,
    graph_resolvers,
    graph_resolver,
    index_execution,
    execution_policy,
};

pub fn parseKind(value: []const u8) ?Kind {
    if (std.mem.eql(u8, value, "full_text")) return .full_text;
    if (std.mem.eql(u8, value, "embeddings")) return .embeddings;
    if (std.mem.eql(u8, value, "graph")) return .graph;
    if (std.mem.eql(u8, value, "algebraic")) return .algebraic;
    return null;
}

pub fn isAllowedConfigField(kind: Kind, field: []const u8) bool {
    if (isCommonField(field)) return true;
    return switch (kind) {
        .full_text => std.mem.eql(u8, field, "mem_only") or
            std.mem.eql(u8, field, "field") or
            std.mem.eql(u8, field, "artifact_name"),
        .embeddings => std.mem.eql(u8, field, "coverage_policy") or
            std.mem.eql(u8, field, "external") or
            std.mem.eql(u8, field, "sparse") or
            std.mem.eql(u8, field, "dimension") or
            std.mem.eql(u8, field, "field") or
            std.mem.eql(u8, field, "embedding_name") or
            std.mem.eql(u8, field, "source_artifact_name") or
            std.mem.eql(u8, field, "template") or
            std.mem.eql(u8, field, "distance_metric") or
            std.mem.eql(u8, field, "mem_only") or
            std.mem.eql(u8, field, "embedder") or
            std.mem.eql(u8, field, "summarizer") or
            std.mem.eql(u8, field, "chunker") or
            std.mem.eql(u8, field, "top_k") or
            std.mem.eql(u8, field, "min_weight") or
            std.mem.eql(u8, field, "chunk_size") or
            std.mem.eql(u8, field, "execution"),
        .graph => std.mem.eql(u8, field, "summarizer") or
            std.mem.eql(u8, field, "template") or
            std.mem.eql(u8, field, "edge_types") or
            std.mem.eql(u8, field, "max_edges_per_document") or
            std.mem.eql(u8, field, "source") or
            std.mem.eql(u8, field, "artifact") or
            std.mem.eql(u8, field, "resolvers"),
        .algebraic => std.mem.eql(u8, field, "derive_from_schema"),
    };
}

pub fn isWriteOnlyConfigField(field: []const u8) bool {
    return std.ascii.eqlIgnoreCase(field, "producer_json");
}

/// Fields intentionally exposed by CreatedProviderConfig. Provider request
/// objects are extensible and may acquire new credentials without this API
/// layer knowing their names, so public responses must use a positive contract
/// rather than reflecting every field that does not look secret.
pub fn isAllowedCreatedProviderField(field: []const u8) bool {
    return std.mem.eql(u8, field, "provider") or
        std.mem.eql(u8, field, "model") or
        std.mem.eql(u8, field, "models") or
        std.mem.eql(u8, field, "project_id") or
        std.mem.eql(u8, field, "location") or
        std.mem.eql(u8, field, "region") or
        std.mem.eql(u8, field, "url") or
        std.mem.eql(u8, field, "api_url") or
        std.mem.eql(u8, field, "dimension") or
        std.mem.eql(u8, field, "dimensions") or
        std.mem.eql(u8, field, "input_type") or
        std.mem.eql(u8, field, "truncate") or
        std.mem.eql(u8, field, "strip_new_lines") or
        std.mem.eql(u8, field, "batch_size") or
        std.mem.eql(u8, field, "temperature") or
        std.mem.eql(u8, field, "max_tokens") or
        std.mem.eql(u8, field, "top_p") or
        std.mem.eql(u8, field, "top_k") or
        std.mem.eql(u8, field, "frequency_penalty") or
        std.mem.eql(u8, field, "presence_penalty") or
        std.mem.eql(u8, field, "timeout");
}

pub fn createdObjectShapeForRootField(kind: Kind, field: []const u8) CreatedObjectShape {
    if (std.mem.eql(u8, field, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, field, "summarizer")) return .provider;
    return switch (kind) {
        .embeddings => if (std.mem.eql(u8, field, "embedder"))
            .provider
        else if (std.mem.eql(u8, field, "execution"))
            .index_execution
        else
            .unrestricted,
        .graph => if (std.mem.eql(u8, field, "source"))
            .graph_source
        else if (std.mem.eql(u8, field, "artifact"))
            .graph_artifact
        else if (std.mem.eql(u8, field, "resolvers"))
            .graph_resolvers
        else
            .unrestricted,
        else => .unrestricted,
    };
}

pub fn createdObjectShapeForArrayItem(parent: CreatedObjectShape) CreatedObjectShape {
    return switch (parent) {
        .enrichments => .enrichment,
        .graph_resolvers => .graph_resolver,
        else => parent,
    };
}

pub fn createdValueMatchesShape(shape: CreatedObjectShape, value: std.json.Value) bool {
    return switch (shape) {
        .unrestricted => true,
        .enrichments, .graph_resolvers => value == .array,
        else => value == .object,
    };
}

pub fn createdObjectShapeForChild(parent: CreatedObjectShape, field: []const u8) CreatedObjectShape {
    return switch (parent) {
        .enrichment => if (std.mem.eql(u8, field, "execution")) .execution_policy else .unrestricted,
        .index_execution => if (isAllowedIndexExecutionField(field)) .execution_policy else .unrestricted,
        else => .unrestricted,
    };
}

pub fn isAllowedCreatedObjectField(shape: CreatedObjectShape, field: []const u8) bool {
    return switch (shape) {
        .unrestricted => true,
        .enrichments, .graph_resolvers => false,
        .provider => isAllowedCreatedProviderField(field),
        .enrichment => isAllowedCreatedEnrichmentField(field),
        .graph_source => isAllowedGraphArtifactSourceField(field),
        .graph_artifact => isAllowedCreatedGraphArtifactField(field),
        .graph_resolver => isAllowedGraphResolverField(field),
        .index_execution => isAllowedIndexExecutionField(field),
        .execution_policy => isAllowedExecutionPolicyField(field),
    };
}

pub fn isAllowedGraphArtifactSourceField(field: []const u8) bool {
    return std.mem.eql(u8, field, "kind") or
        std.mem.eql(u8, field, "artifact") or
        std.mem.eql(u8, field, "path") or
        std.mem.eql(u8, field, "format") or
        std.mem.eql(u8, field, "mention_edge_type");
}

pub fn isAllowedGraphArtifactRequestField(field: []const u8) bool {
    return isAllowedCreatedGraphArtifactField(field) or std.mem.eql(u8, field, "producer_json");
}

pub fn isAllowedCreatedGraphArtifactField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "kind") or
        std.mem.eql(u8, field, "field") or
        std.mem.eql(u8, field, "content_type");
}

pub fn isAllowedGraphResolverField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "table") or
        std.mem.eql(u8, field, "source_artifact") or
        std.mem.eql(u8, field, "source_artifact_kind") or
        std.mem.eql(u8, field, "resolution_artifact") or
        std.mem.eql(u8, field, "key_template") or
        std.mem.eql(u8, field, "type_must_match") or
        std.mem.eql(u8, field, "scorer_json") or
        std.mem.eql(u8, field, "candidate_search") or
        std.mem.eql(u8, field, "candidate_ann_index") or
        std.mem.eql(u8, field, "candidate_limit") or
        std.mem.eql(u8, field, "name_embedding") or
        std.mem.eql(u8, field, "name_embedding_dims") or
        std.mem.eql(u8, field, "fusion_combine") or
        std.mem.eql(u8, field, "fusion_trust") or
        std.mem.eql(u8, field, "fusion_prior") or
        std.mem.eql(u8, field, "fusion_prior_weight") or
        std.mem.eql(u8, field, "config_generation");
}

pub fn isAllowedCreatedEnrichmentField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "kind") or
        std.mem.eql(u8, field, "field") or
        std.mem.eql(u8, field, "template") or
        std.mem.eql(u8, field, "source_artifact_name") or
        std.mem.eql(u8, field, "expected_dims") or
        std.mem.eql(u8, field, "chunk_size") or
        std.mem.eql(u8, field, "chunk_overlap") or
        std.mem.eql(u8, field, "chunker_json") or
        std.mem.eql(u8, field, "full_text_index") or
        std.mem.eql(u8, field, "content_type") or
        std.mem.eql(u8, field, "execution");
}

pub fn isAllowedEnrichmentRequestField(field: []const u8) bool {
    return isAllowedCreatedEnrichmentField(field) or std.mem.eql(u8, field, "producer_json");
}

pub fn isAllowedIndexExecutionField(field: []const u8) bool {
    return std.mem.eql(u8, field, "chunking") or std.mem.eql(u8, field, "embedding");
}

pub fn isAllowedExecutionPolicyField(field: []const u8) bool {
    return std.mem.eql(u8, field, "batch_items") or std.mem.eql(u8, field, "batch_bytes");
}

fn isCommonField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "type") or
        std.mem.eql(u8, field, "description") or
        std.mem.eql(u8, field, "version") or
        std.mem.eql(u8, field, "enrichments");
}
