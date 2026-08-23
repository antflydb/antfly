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

//! Adaptive promotion policy for Antfly-owned lake-native serving artifacts.

const std = @import("std");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const Thresholds = struct {
    repeated_scan_count: u32 = 3,
    repeated_projection_count: u32 = 4,
    repeated_aggregate_count: u32 = 3,
    min_scanned_rows: u64 = 1024,
    min_external_scan_rows: u64 = 4096,
    latency_ns: u64 = 50 * std.time.ns_per_ms,
};

pub const Observation = struct {
    source_kind: rowsource.SourceKind,
    scanned_rows: u64,
    returned_rows: u64,
    projected_column_count: u16,
    repeated_scan_count: u32 = 1,
    repeated_projection_count: u32 = 0,
    repeated_aggregate_count: u32 = 0,
    latency_ns: u64 = 0,
    has_group_by_fold: bool = false,
    source_already_antfly_owned: bool = false,
};

pub const RecommendationKind = enum {
    none,
    publish_row_fragment,
    promote_hot_projection,
    materialize_algebraic_segment,
    promote_external_to_row_fragment,
};

pub const Recommendation = struct {
    kind: RecommendationKind,
    reason: []const u8 = &.{},

    pub fn isActionable(self: Recommendation) bool {
        return self.kind != .none;
    }
};

pub fn recommend(observation: Observation, thresholds: Thresholds) Recommendation {
    if (observation.has_group_by_fold and
        observation.repeated_aggregate_count >= thresholds.repeated_aggregate_count and
        observation.scanned_rows >= thresholds.min_scanned_rows)
    {
        return .{
            .kind = .materialize_algebraic_segment,
            .reason = "repeated aggregate fold over enough rows",
        };
    }

    if (isExternal(observation.source_kind) and
        !observation.source_already_antfly_owned and
        observation.repeated_scan_count >= thresholds.repeated_scan_count and
        observation.scanned_rows >= thresholds.min_external_scan_rows)
    {
        return .{
            .kind = .promote_external_to_row_fragment,
            .reason = "repeated external scan justifies Antfly-owned fragment",
        };
    }

    if (observation.repeated_projection_count >= thresholds.repeated_projection_count and
        observation.projected_column_count > 0 and
        observation.scanned_rows >= thresholds.min_scanned_rows)
    {
        return .{
            .kind = .promote_hot_projection,
            .reason = "repeated projected scan justifies projection-local fragment",
        };
    }

    if (!observation.source_already_antfly_owned and
        observation.repeated_scan_count >= thresholds.repeated_scan_count and
        observation.latency_ns >= thresholds.latency_ns and
        observation.scanned_rows >= thresholds.min_scanned_rows)
    {
        return .{
            .kind = .publish_row_fragment,
            .reason = "repeated high-latency scan justifies row-fragment publication",
        };
    }

    return .{ .kind = .none };
}

fn isExternal(kind: rowsource.SourceKind) bool {
    return switch (kind) {
        .external_parquet, .external_iceberg, .external_lance => true,
        .relational_store, .json_materialized, .serverless_fragment => false,
    };
}

test "lake promotion policy prefers algebraic folds for repeated aggregates" {
    const recommendation = recommend(.{
        .source_kind = .serverless_fragment,
        .scanned_rows = 10_000,
        .returned_rows = 10,
        .projected_column_count = 2,
        .repeated_aggregate_count = 3,
        .has_group_by_fold = true,
        .source_already_antfly_owned = true,
    }, .{});

    try std.testing.expectEqual(RecommendationKind.materialize_algebraic_segment, recommendation.kind);
    try std.testing.expect(recommendation.isActionable());
}

test "lake promotion policy promotes repeated external scans into row fragments" {
    const recommendation = recommend(.{
        .source_kind = .external_iceberg,
        .scanned_rows = 20_000,
        .returned_rows = 1_000,
        .projected_column_count = 5,
        .repeated_scan_count = 3,
    }, .{});

    try std.testing.expectEqual(RecommendationKind.promote_external_to_row_fragment, recommendation.kind);
}

test "lake promotion policy leaves cold scans alone" {
    const recommendation = recommend(.{
        .source_kind = .external_parquet,
        .scanned_rows = 100,
        .returned_rows = 10,
        .projected_column_count = 2,
        .repeated_scan_count = 1,
    }, .{});

    try std.testing.expectEqual(RecommendationKind.none, recommendation.kind);
}
