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

pub const max_artifact_sources = 64;

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
    artifact_sources,
    artifact_source,
    graph_sources,
    graph_source,
    graph_artifact,
    graph_artifact_producer_source,
    graph_nodes,
    graph_edge,
    graph_context,
    graph_algebraic_planning,
    graph_bounded_traversal,
    edge_types,
    edge_type,
    graph_resolvers,
    graph_resolver,
    chunker,
    chunker_text,
    chunker_audio,
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
            std.mem.eql(u8, field, "artifact_name") or
            std.mem.eql(u8, field, "sources"),
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
            std.mem.eql(u8, field, "execution") or
            std.mem.eql(u8, field, "sources"),
        .graph => std.mem.eql(u8, field, "summarizer") or
            std.mem.eql(u8, field, "template") or
            std.mem.eql(u8, field, "edge_types") or
            std.mem.eql(u8, field, "max_edges_per_document") or
            std.mem.eql(u8, field, "source") or
            std.mem.eql(u8, field, "sources") or
            std.mem.eql(u8, field, "artifact") or
            std.mem.eql(u8, field, "algebraic_planning") or
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
    if (std.mem.eql(u8, field, "sources")) return if (kind == .graph) .graph_sources else .artifact_sources;
    if (std.mem.eql(u8, field, "summarizer")) return .provider;
    return switch (kind) {
        .embeddings => if (std.mem.eql(u8, field, "embedder"))
            .provider
        else if (std.mem.eql(u8, field, "chunker"))
            .chunker
        else if (std.mem.eql(u8, field, "execution"))
            .index_execution
        else
            .unrestricted,
        .graph => if (std.mem.eql(u8, field, "source"))
            .graph_source
        else if (std.mem.eql(u8, field, "artifact"))
            .graph_artifact
        else if (std.mem.eql(u8, field, "algebraic_planning"))
            .graph_algebraic_planning
        else if (std.mem.eql(u8, field, "edge_types"))
            .edge_types
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
        .artifact_sources => .artifact_source,
        .graph_sources => .graph_source,
        .edge_types => .edge_type,
        .graph_resolvers => .graph_resolver,
        else => parent,
    };
}

pub fn createdValueMatchesShape(shape: CreatedObjectShape, value: std.json.Value) bool {
    return switch (shape) {
        .unrestricted => true,
        .enrichments, .artifact_sources, .graph_sources, .edge_types, .graph_resolvers => value == .array,
        else => value == .object and createdObjectHasRequiredFields(shape, value.object),
    };
}

fn createdObjectHasRequiredFields(shape: CreatedObjectShape, object: std.json.ObjectMap) bool {
    const required_fields: []const []const u8 = switch (shape) {
        .provider, .chunker => &.{"provider"},
        .enrichment => &.{ "name", "kind" },
        .artifact_source => &.{"artifact"},
        .graph_artifact => &.{ "name", "kind", "source" },
        .graph_artifact_producer_source => &.{ "type", "value" },
        .graph_source => &.{"artifact"},
        .edge_type => &.{"name"},
        .graph_resolver => &.{ "name", "table", "source_artifact", "resolution_artifact", "key_template" },
        .graph_bounded_traversal => &.{"law"},
        .unrestricted, .enrichments, .artifact_sources, .graph_sources, .edge_types, .graph_resolvers, .graph_nodes, .graph_edge, .graph_context, .graph_algebraic_planning, .chunker_text, .chunker_audio, .index_execution, .execution_policy => &.{},
    };
    for (required_fields) |field| {
        const value = object.get(field) orelse return false;
        if (value == .null or !createdFieldValueMatches(shape, field, value)) return false;
    }
    return true;
}

pub fn createdObjectShapeForChild(parent: CreatedObjectShape, field: []const u8) CreatedObjectShape {
    return switch (parent) {
        .enrichment => if (std.mem.eql(u8, field, "execution")) .execution_policy else .unrestricted,
        .chunker => if (std.mem.eql(u8, field, "text"))
            .chunker_text
        else if (std.mem.eql(u8, field, "audio"))
            .chunker_audio
        else
            .unrestricted,
        .index_execution => if (isAllowedIndexExecutionField(field)) .execution_policy else .unrestricted,
        .graph_artifact => if (std.mem.eql(u8, field, "source"))
            .graph_artifact_producer_source
        else if (std.mem.eql(u8, field, "execution"))
            .execution_policy
        else
            .unrestricted,
        .graph_source => if (std.mem.eql(u8, field, "nodes"))
            .graph_nodes
        else if (std.mem.eql(u8, field, "edge"))
            .graph_edge
        else if (std.mem.eql(u8, field, "context"))
            .graph_context
        else
            .unrestricted,
        .graph_algebraic_planning => if (std.mem.eql(u8, field, "bounded_traversal")) .graph_bounded_traversal else .unrestricted,
        else => .unrestricted,
    };
}

pub fn isAllowedCreatedObjectField(shape: CreatedObjectShape, field: []const u8) bool {
    return switch (shape) {
        .unrestricted => true,
        .enrichments, .artifact_sources, .graph_sources, .edge_types, .graph_resolvers => false,
        .provider => isAllowedCreatedProviderField(field),
        .enrichment => isAllowedCreatedEnrichmentField(field),
        .artifact_source => std.mem.eql(u8, field, "artifact"),
        .graph_source => isAllowedGraphArtifactSourceField(field),
        .graph_artifact => isAllowedCreatedGraphArtifactField(field),
        .graph_artifact_producer_source => std.mem.eql(u8, field, "type") or std.mem.eql(u8, field, "value"),
        .graph_nodes => isAllowedGraphNodeMappingField(field),
        .graph_edge => isAllowedGraphEdgeMappingField(field),
        .graph_context => isAllowedGraphContextField(field),
        .graph_algebraic_planning => std.mem.eql(u8, field, "bounded_traversal"),
        .graph_bounded_traversal => std.mem.eql(u8, field, "law"),
        .edge_type => isAllowedEdgeTypeField(field),
        .graph_resolver => isAllowedGraphResolverField(field),
        .chunker => isAllowedChunkerField(field),
        .chunker_text => isAllowedChunkerTextField(field),
        .chunker_audio => isAllowedChunkerAudioField(field),
        .index_execution => isAllowedIndexExecutionField(field),
        .execution_policy => isAllowedExecutionPolicyField(field),
    };
}

/// Verify the JSON representation of a public root field. This is used both
/// when admitting a request and when projecting catalog metadata, so corrupt
/// or legacy documents cannot turn a scalar field into an arbitrary object.
pub fn rootFieldValueMatches(kind: Kind, field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "type") or
        std.mem.eql(u8, field, "description")) return isString(value);
    if (std.mem.eql(u8, field, "version")) return isInteger(value);
    if (std.mem.eql(u8, field, "enrichments")) return value == .array;
    if (std.mem.eql(u8, field, "sources")) return value == .array;

    return switch (kind) {
        .full_text => if (std.mem.eql(u8, field, "mem_only"))
            isBool(value)
        else if (std.mem.eql(u8, field, "field") or std.mem.eql(u8, field, "artifact_name"))
            isNonEmptyString(value)
        else
            isString(value),
        .embeddings => if (std.mem.eql(u8, field, "external") or
            std.mem.eql(u8, field, "sparse") or
            std.mem.eql(u8, field, "mem_only"))
            isBool(value)
        else if (std.mem.eql(u8, field, "dimension") or
            std.mem.eql(u8, field, "top_k") or
            std.mem.eql(u8, field, "chunk_size"))
            isInteger(value)
        else if (std.mem.eql(u8, field, "min_weight"))
            isNumber(value)
        else if (std.mem.eql(u8, field, "embedder") or
            std.mem.eql(u8, field, "summarizer") or
            std.mem.eql(u8, field, "chunker") or
            std.mem.eql(u8, field, "execution"))
            value == .object
        else
            isString(value),
        .graph => if (std.mem.eql(u8, field, "max_edges_per_document"))
            isInteger(value)
        else if (std.mem.eql(u8, field, "edge_types") or std.mem.eql(u8, field, "resolvers"))
            value == .array
        else if (std.mem.eql(u8, field, "summarizer") or
            std.mem.eql(u8, field, "source") or
            std.mem.eql(u8, field, "artifact") or
            std.mem.eql(u8, field, "algebraic_planning"))
            value == .object
        else
            isString(value),
        .algebraic => isBool(value),
    };
}

/// Verify the JSON representation of a member in a closed CreatedIndex
/// object. `full_text_index` is the sole intentionally dynamic subtree.
pub fn createdFieldValueMatches(shape: CreatedObjectShape, field: []const u8, value: std.json.Value) bool {
    return switch (shape) {
        .unrestricted => true,
        .enrichments, .artifact_sources, .graph_sources, .edge_types, .graph_resolvers => false,
        .provider => providerFieldValueMatches(field, value),
        .enrichment => enrichmentFieldValueMatches(field, value),
        .artifact_source => isNonEmptyString(value),
        .graph_source => graphArtifactSourceFieldValueMatches(field, value),
        .graph_artifact => graphArtifactFieldValueMatches(field, value),
        .graph_artifact_producer_source => graphArtifactProducerSourceFieldValueMatches(field, value),
        .graph_nodes => graphNodeMappingFieldValueMatches(field, value),
        .graph_edge => graphEdgeMappingFieldValueMatches(field, value),
        .graph_context => isNonEmptyStringArray(value),
        .graph_algebraic_planning => value == .object,
        .graph_bounded_traversal => value == .string and std.mem.eql(u8, value.string, "provenance_semiring"),
        .edge_type => edgeTypeFieldValueMatches(field, value),
        .graph_resolver => graphResolverFieldValueMatches(field, value),
        .chunker => chunkerFieldValueMatches(field, value),
        .chunker_text => if (std.mem.eql(u8, field, "separator")) isString(value) else isInteger(value),
        .chunker_audio => isInteger(value),
        .index_execution => value == .object,
        .execution_policy => isInteger(value),
    };
}

fn providerFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "models")) return isStringArray(value);
    if (std.mem.eql(u8, field, "dimension") or
        std.mem.eql(u8, field, "dimensions") or
        std.mem.eql(u8, field, "batch_size") or
        std.mem.eql(u8, field, "max_tokens") or
        std.mem.eql(u8, field, "top_k") or
        std.mem.eql(u8, field, "timeout")) return isInteger(value);
    if (std.mem.eql(u8, field, "strip_new_lines")) return isBool(value);
    if (std.mem.eql(u8, field, "temperature") or
        std.mem.eql(u8, field, "top_p") or
        std.mem.eql(u8, field, "frequency_penalty") or
        std.mem.eql(u8, field, "presence_penalty")) return isNumber(value);
    return isString(value);
}

fn enrichmentFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "expected_dims") or
        std.mem.eql(u8, field, "chunk_size") or
        std.mem.eql(u8, field, "chunk_overlap")) return isInteger(value);
    if (std.mem.eql(u8, field, "full_text_index")) return isBool(value);
    if (std.mem.eql(u8, field, "execution")) return value == .object;
    if (std.mem.eql(u8, field, "vector_space")) return isNonEmptyString(value);
    return isString(value);
}

fn edgeTypeFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "max_weight") or std.mem.eql(u8, field, "min_weight")) return isNumber(value);
    if (std.mem.eql(u8, field, "allow_self_loops")) return isBool(value);
    if (std.mem.eql(u8, field, "required_metadata")) return isStringArray(value);
    return isString(value);
}

fn graphResolverFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "type_must_match")) return isBool(value);
    if (std.mem.eql(u8, field, "candidate_limit") or
        std.mem.eql(u8, field, "name_embedding_dims") or
        std.mem.eql(u8, field, "config_generation")) return isInteger(value);
    if (std.mem.eql(u8, field, "fusion_trust") or
        std.mem.eql(u8, field, "fusion_prior") or
        std.mem.eql(u8, field, "fusion_prior_weight")) return isNumber(value);
    return isString(value);
}

fn graphNodeMappingFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "model")) {
        return value == .string and
            (std.mem.eql(u8, value.string, "document") or std.mem.eql(u8, value.string, "external"));
    }
    return isString(value) or isNumber(value);
}

fn graphEdgeMappingFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "metadata")) return value == .object;
    return isString(value) or isNumber(value);
}

fn graphArtifactSourceFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "format"))
        return value == .string and
            (std.mem.eql(u8, value.string, "extraction_relation") or
                std.mem.eql(u8, value.string, "extraction_graph"));
    if (std.mem.eql(u8, field, "artifact")) return isNonEmptyString(value);
    if (std.mem.eql(u8, field, "nodes") or
        std.mem.eql(u8, field, "edge") or
        std.mem.eql(u8, field, "context")) return value == .object;
    return isString(value);
}

fn graphArtifactFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "kind"))
        return value == .string and std.mem.eql(u8, value.string, "asset");
    if (std.mem.eql(u8, field, "name")) return isNonEmptyString(value);
    if (std.mem.eql(u8, field, "source")) return value == .object;
    if (std.mem.eql(u8, field, "execution")) return value == .object;
    return isString(value);
}

fn graphArtifactProducerSourceFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "type"))
        return value == .string and
            (std.mem.eql(u8, value.string, "field") or std.mem.eql(u8, value.string, "template"));
    return isNonEmptyString(value);
}

fn chunkerFieldValueMatches(field: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, field, "store_chunks")) return isBool(value);
    if (std.mem.eql(u8, field, "full_text_index") or
        std.mem.eql(u8, field, "text") or
        std.mem.eql(u8, field, "audio")) return value == .object;
    if (std.mem.eql(u8, field, "max_chunks")) return isInteger(value);
    if (std.mem.eql(u8, field, "threshold")) return isNumber(value);
    return isString(value);
}

fn isString(value: std.json.Value) bool {
    return value == .string;
}

fn isNonEmptyString(value: std.json.Value) bool {
    return value == .string and value.string.len > 0;
}

fn isInteger(value: std.json.Value) bool {
    return value == .integer;
}

fn isNumber(value: std.json.Value) bool {
    return value == .integer or value == .float;
}

fn isBool(value: std.json.Value) bool {
    return value == .bool;
}

fn isStringArray(value: std.json.Value) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item != .string) return false;
    }
    return true;
}

fn isNonEmptyStringArray(value: std.json.Value) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0) return false;
    }
    return true;
}

pub fn isAllowedChunkerField(field: []const u8) bool {
    return std.mem.eql(u8, field, "api_url") or
        std.mem.eql(u8, field, "model") or
        std.mem.eql(u8, field, "provider") or
        std.mem.eql(u8, field, "store_chunks") or
        std.mem.eql(u8, field, "full_text_index") or
        std.mem.eql(u8, field, "max_chunks") or
        std.mem.eql(u8, field, "threshold") or
        std.mem.eql(u8, field, "text") or
        std.mem.eql(u8, field, "audio");
}

pub fn isAllowedChunkerTextField(field: []const u8) bool {
    return std.mem.eql(u8, field, "target_tokens") or
        std.mem.eql(u8, field, "overlap_tokens") or
        std.mem.eql(u8, field, "separator");
}

pub fn isAllowedChunkerAudioField(field: []const u8) bool {
    return std.mem.eql(u8, field, "window_duration_ms") or
        std.mem.eql(u8, field, "overlap_duration_ms");
}

pub fn isAllowedEdgeTypeField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "field") or
        std.mem.eql(u8, field, "topology") or
        std.mem.eql(u8, field, "max_weight") or
        std.mem.eql(u8, field, "min_weight") or
        std.mem.eql(u8, field, "allow_self_loops") or
        std.mem.eql(u8, field, "required_metadata");
}

pub fn isAllowedGraphArtifactSourceField(field: []const u8) bool {
    return std.mem.eql(u8, field, "artifact") or
        std.mem.eql(u8, field, "path") or
        std.mem.eql(u8, field, "format") or
        std.mem.eql(u8, field, "mention_edge_type") or
        std.mem.eql(u8, field, "nodes") or
        std.mem.eql(u8, field, "edge") or
        std.mem.eql(u8, field, "context");
}

pub fn isAllowedGraphNodeMappingField(field: []const u8) bool {
    return std.mem.eql(u8, field, "model") or
        std.mem.eql(u8, field, "source") or
        std.mem.eql(u8, field, "target");
}

pub fn isAllowedGraphEdgeMappingField(field: []const u8) bool {
    return std.mem.eql(u8, field, "type") or
        std.mem.eql(u8, field, "weight") or
        std.mem.eql(u8, field, "metadata");
}

pub fn isAllowedGraphContextField(field: []const u8) bool {
    return std.mem.eql(u8, field, "doc_fields");
}

pub fn isAllowedGraphArtifactRequestField(field: []const u8) bool {
    return isAllowedCreatedGraphArtifactField(field) or std.mem.eql(u8, field, "producer_json");
}

pub fn isAllowedCreatedGraphArtifactField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "kind") or
        std.mem.eql(u8, field, "source") or
        std.mem.eql(u8, field, "content_type") or
        std.mem.eql(u8, field, "execution");
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
        std.mem.eql(u8, field, "vector_space") or
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
