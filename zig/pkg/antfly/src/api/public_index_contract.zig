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

fn isCommonField(field: []const u8) bool {
    return std.mem.eql(u8, field, "name") or
        std.mem.eql(u8, field, "type") or
        std.mem.eql(u8, field, "description") or
        std.mem.eql(u8, field, "version") or
        std.mem.eql(u8, field, "enrichments");
}
