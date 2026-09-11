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

//! Dependency-light manifest artifact references shared by manifest codecs and
//! lake-native publication planning.

/// First manifest version carrying artifact materializer provenance.
/// The only manifest wire that may publish graph-metric artifacts. Serverless
/// has not shipped, so partial pre-release graph-metric layouts are rejected
/// instead of becoming a permanent compatibility surface.
pub const graph_metric_manifest_wire_version: u16 = 24;
pub const graph_metric_segment_wire_version: u16 = 10;

pub const GraphMetricMaterializationState = enum(u8) {
    ready = 0,
    rejected = 1,
};

pub const GraphMetricRejectionReason = enum(u8) {
    none = 0,
    build_budget_exceeded = 1,
};

pub const ArtifactKind = enum(u8) {
    text_segment = 1,
    vector_segment = 2,
    doc_values = 3,
    stored_fields = 4,
    mutation_segment = 5,
    document_segment = 6,
    sparse_segment = 7,
    graph_segment = 8,
    row_fragment = 9,
    row_fragment_stats = 10,
    algebraic_segment = 11,
    external_base_source = 12,
    graph_metric_segment = 13,
};

pub const ArtifactRef = struct {
    kind: ArtifactKind,
    name: []const u8 = &.{},
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
    /// Optional artifact-specific metadata schema version. Zero means the
    /// producing manifest predates persisted provenance.
    metadata_version: u16 = 0,
    published_generation: u64 = 0,
    edge_generation: u64 = 0,
    computed_at_ms: u64 = 0,
    /// Artifact-producing policy identity. Graph-metric refs persist this so
    /// catalog scheduling can detect stale materializations without fetching
    /// the object payload. Zero denotes a pre-v15 manifest.
    materializer_fingerprint: u64 = 0,
    /// Manifest-authenticated graph trailer; authenticates the directory and
    /// its fixed-size data-block checksums without whole-object cold reads.
    graph_topology_control_checksum: [32]u8 = @splat(0),
    /// Authenticated range metadata for the current graph-metric wire. Fixed-size
    /// digests avoid per-reference allocations and let point/status reads stay
    /// bounded without trusting object-store range responses.
    graph_metric_control_len: u32 = 0,
    graph_metric_routing_footer_len: u32 = 0,
    graph_metric_control_checksum: [32]u8 = @splat(0),
    // Authenticates the bounded routing root, which in turn authenticates the
    // primary point index. Top-K readers never need to fetch that point index.
    graph_metric_routing_checksum: [32]u8 = @splat(0),
    graph_metric_point_index_checksum: [32]u8 = @splat(0),
    graph_metric_config_fingerprint: u64 = 0,
    graph_metric_source_checksum: [32]u8 = @splat(0),
    /// Canonical selected unweighted connectivity, independent of source
    /// payload layout/weights. The metric control authenticates this identity;
    /// source_checksum binds the current publication to its graph artifact.
    graph_metric_topology_checksum: [32]u8 = @splat(0),
    graph_metric_materialization_state: GraphMetricMaterializationState = .ready,
    graph_metric_rejection_reason: GraphMetricRejectionReason = .none,
};

/// Distinct logical graphs/metrics may share one immutable payload. Every
/// physical/integrity field must agree; only names and per-reference provenance
/// may differ. Comparing the normalized whole struct also checks future fields.
pub fn areGraphArtifactAliases(a: ArtifactRef, b: ArtifactRef) bool {
    const std = @import("std");
    if (a.kind != b.kind or (a.kind != .graph_segment and a.kind != .graph_metric_segment) or
        (a.kind == .graph_metric_segment and a.metadata_version != graph_metric_segment_wire_version) or
        a.name.len == 0 or b.name.len == 0 or std.mem.eql(u8, a.name, b.name) or
        !std.mem.eql(u8, a.artifact_id, b.artifact_id) or !std.mem.eql(u8, a.checksum, b.checksum)) return false;
    var normalized = a;
    normalized.name = b.name;
    normalized.artifact_id = b.artifact_id;
    normalized.checksum = b.checksum;
    normalized.published_generation = b.published_generation;
    normalized.edge_generation = b.edge_generation;
    normalized.computed_at_ms = b.computed_at_ms;
    return std.meta.eql(normalized, b);
}

test "serverless graph metric aliases require identical immutable metadata" {
    const std = @import("std");
    const original = ArtifactRef{ .kind = .graph_metric_segment, .name = "1:a1:x", .artifact_id = "metric", .checksum = "checksum", .byte_len = 128, .metadata_version = graph_metric_segment_wire_version };
    var alias = original;
    alias.name = "1:b1:y";
    alias.published_generation = 3;
    alias.edge_generation = 2;
    alias.computed_at_ms = 1;
    try std.testing.expect(areGraphArtifactAliases(original, alias));
    try std.testing.expect(!areGraphArtifactAliases(original, original));
    alias.graph_metric_routing_checksum[0] = 1;
    try std.testing.expect(!areGraphArtifactAliases(original, alias));
    alias.graph_metric_routing_checksum[0] = 0;
    alias.graph_metric_config_fingerprint = 1;
    try std.testing.expect(!areGraphArtifactAliases(original, alias));
    alias.graph_metric_config_fingerprint = 0;
    alias.byte_len += 1;
    try std.testing.expect(!areGraphArtifactAliases(original, alias));
    alias.byte_len -= 1;
    alias.kind = .graph_segment;
    try std.testing.expect(!areGraphArtifactAliases(original, alias));
    var graph = original;
    graph.kind = .graph_segment;
    try std.testing.expect(areGraphArtifactAliases(graph, alias));
}

test "manifest artifact kinds include lake-native artifacts" {
    try @import("std").testing.expectEqual(@as(u8, 9), @intFromEnum(ArtifactKind.row_fragment));
    try @import("std").testing.expectEqual(@as(u8, 11), @intFromEnum(ArtifactKind.algebraic_segment));
    try @import("std").testing.expectEqual(@as(u8, 13), @intFromEnum(ArtifactKind.graph_metric_segment));
}
