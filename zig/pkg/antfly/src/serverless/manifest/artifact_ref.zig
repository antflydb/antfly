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
};

pub const ArtifactRef = struct {
    kind: ArtifactKind,
    name: []const u8 = &.{},
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
};

test "manifest artifact kinds include lake-native artifacts" {
    try @import("std").testing.expectEqual(@as(u8, 9), @intFromEnum(ArtifactKind.row_fragment));
    try @import("std").testing.expectEqual(@as(u8, 11), @intFromEnum(ArtifactKind.algebraic_segment));
}
