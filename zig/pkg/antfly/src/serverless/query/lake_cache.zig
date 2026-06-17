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

//! Lake-specific cache accounting. The generic query cache stores full objects,
//! ranges, and blocks; lake mode needs a stable accounting contract that
//! separates metadata/stats from scan payloads so future eviction policy can
//! protect serving-critical pruning and routing data.

const std = @import("std");
const artifact_ref = @import("../manifest/artifact_ref.zig");

pub const CacheClass = enum {
    row_fragment_data,
    row_fragment_stats,
    algebraic_segment,
    external_metadata,
    search_sidecar,
    other,

    pub fn isPinned(self: CacheClass) bool {
        return switch (self) {
            .row_fragment_stats, .external_metadata, .algebraic_segment => true,
            .row_fragment_data, .search_sidecar, .other => false,
        };
    }

    pub fn isPayload(self: CacheClass) bool {
        return switch (self) {
            .row_fragment_data, .search_sidecar => true,
            .row_fragment_stats, .external_metadata, .algebraic_segment, .other => false,
        };
    }
};

pub const Budget = struct {
    max_pinned_bytes: u64 = 0,
    max_payload_bytes: u64 = 0,
    max_total_bytes: u64 = 0,
};

pub const Accounting = struct {
    row_fragment_data_bytes: u64 = 0,
    row_fragment_stats_bytes: u64 = 0,
    algebraic_segment_bytes: u64 = 0,
    external_metadata_bytes: u64 = 0,
    search_sidecar_bytes: u64 = 0,
    other_bytes: u64 = 0,
    pinned_bytes: u64 = 0,
    payload_bytes: u64 = 0,
    total_bytes: u64 = 0,
    over_pinned_budget: bool = false,
    over_payload_budget: bool = false,
    over_total_budget: bool = false,

    pub fn bytesForClass(self: Accounting, class: CacheClass) u64 {
        return switch (class) {
            .row_fragment_data => self.row_fragment_data_bytes,
            .row_fragment_stats => self.row_fragment_stats_bytes,
            .algebraic_segment => self.algebraic_segment_bytes,
            .external_metadata => self.external_metadata_bytes,
            .search_sidecar => self.search_sidecar_bytes,
            .other => self.other_bytes,
        };
    }

    pub fn anyOverBudget(self: Accounting) bool {
        return self.over_pinned_budget or self.over_payload_budget or self.over_total_budget;
    }
};

pub fn classifyArtifact(kind: artifact_ref.ArtifactKind) CacheClass {
    return switch (kind) {
        .row_fragment => .row_fragment_data,
        .row_fragment_stats => .row_fragment_stats,
        .algebraic_segment => .algebraic_segment,
        .external_base_source => .external_metadata,
        .text_segment, .vector_segment, .sparse_segment, .graph_segment => .search_sidecar,
        .doc_values, .stored_fields, .mutation_segment, .document_segment => .other,
    };
}

pub fn accountArtifacts(
    artifacts: []const artifact_ref.ArtifactRef,
    budget: Budget,
) !Accounting {
    var out = Accounting{};
    for (artifacts) |artifact| {
        if (artifact.artifact_id.len == 0) return error.InvalidLakeCacheAccounting;
        const class = classifyArtifact(artifact.kind);
        try addBytes(&out, class, artifact.byte_len);
    }
    out.over_pinned_budget = budget.max_pinned_bytes != 0 and out.pinned_bytes > budget.max_pinned_bytes;
    out.over_payload_budget = budget.max_payload_bytes != 0 and out.payload_bytes > budget.max_payload_bytes;
    out.over_total_budget = budget.max_total_bytes != 0 and out.total_bytes > budget.max_total_bytes;
    return out;
}

fn addBytes(accounting: *Accounting, class: CacheClass, byte_len: u64) !void {
    switch (class) {
        .row_fragment_data => accounting.row_fragment_data_bytes += byte_len,
        .row_fragment_stats => accounting.row_fragment_stats_bytes += byte_len,
        .algebraic_segment => accounting.algebraic_segment_bytes += byte_len,
        .external_metadata => accounting.external_metadata_bytes += byte_len,
        .search_sidecar => accounting.search_sidecar_bytes += byte_len,
        .other => accounting.other_bytes += byte_len,
    }
    if (class.isPinned()) accounting.pinned_bytes += byte_len;
    if (class.isPayload()) accounting.payload_bytes += byte_len;
    accounting.total_bytes += byte_len;
}

test "lake cache accounting separates pinned metadata from payload bytes" {
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-1", .byte_len = 100, .checksum = "len:100" },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-1.stats", .byte_len = 10, .checksum = "len:10" },
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 20, .checksum = "len:20" },
        .{ .kind = .algebraic_segment, .artifact_id = "agg-1", .byte_len = 30, .checksum = "len:30" },
        .{ .kind = .vector_segment, .artifact_id = "vec-1", .byte_len = 40, .checksum = "len:40" },
    };

    const accounting = try accountArtifacts(&artifacts, .{});
    try std.testing.expectEqual(@as(u64, 100), accounting.bytesForClass(.row_fragment_data));
    try std.testing.expectEqual(@as(u64, 10), accounting.bytesForClass(.row_fragment_stats));
    try std.testing.expectEqual(@as(u64, 20), accounting.bytesForClass(.external_metadata));
    try std.testing.expectEqual(@as(u64, 30), accounting.bytesForClass(.algebraic_segment));
    try std.testing.expectEqual(@as(u64, 40), accounting.bytesForClass(.search_sidecar));
    try std.testing.expectEqual(@as(u64, 60), accounting.pinned_bytes);
    try std.testing.expectEqual(@as(u64, 140), accounting.payload_bytes);
    try std.testing.expectEqual(@as(u64, 200), accounting.total_bytes);
}

test "lake cache accounting reports budget pressure by lane" {
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-1", .byte_len = 100, .checksum = "len:100" },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-1.stats", .byte_len = 20, .checksum = "len:20" },
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 20, .checksum = "len:20" },
    };

    const accounting = try accountArtifacts(&artifacts, .{
        .max_pinned_bytes = 30,
        .max_payload_bytes = 200,
        .max_total_bytes = 130,
    });
    try std.testing.expect(accounting.over_pinned_budget);
    try std.testing.expect(!accounting.over_payload_budget);
    try std.testing.expect(accounting.over_total_budget);
    try std.testing.expect(accounting.anyOverBudget());
}
