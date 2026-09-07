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
pub const graph_metric_manifest_wire_version: u16 = 18;
pub const graph_metric_segment_wire_version: u16 = 9;

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
    graph_metric_materialization_state: GraphMetricMaterializationState = .ready,
    graph_metric_rejection_reason: GraphMetricRejectionReason = .none,
};

test "manifest artifact kinds include lake-native artifacts" {
    try @import("std").testing.expectEqual(@as(u8, 9), @intFromEnum(ArtifactKind.row_fragment));
    try @import("std").testing.expectEqual(@as(u8, 11), @intFromEnum(ArtifactKind.algebraic_segment));
    try @import("std").testing.expectEqual(@as(u8, 13), @intFromEnum(ArtifactKind.graph_metric_segment));
}
