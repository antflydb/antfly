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

const std = @import("std");
const antfly_client = @import("antfly-client");

/// Coverage is an integrity signal, but its completion semantics are policy
/// specific. External indexes are query-ready once replay is current even when
/// callers intentionally supplied vectors for only part of the source table.
pub fn coverageReady(status: antfly_client.types.DerivedCoverageStatus) bool {
    if (!status.observation_complete or status.config_mismatch_group_count != 0) return false;
    return status.policy == .external or status.complete;
}

pub fn embeddingIndexReady(stats: antfly_client.types.EmbeddingsIndexStats) bool {
    if (stats.@"error" != null) return false;
    if (stats.backfill_state) |state| {
        if (!std.mem.eql(u8, state, "ready")) return false;
    } else if (stats.rebuilding orelse true) {
        return false;
    }
    const status = stats.coverage orelse return false;
    return coverageReady(status);
}

fn makeCoverage(policy: antfly_client.types.DerivedCoverageStatusPolicy) antfly_client.types.DerivedCoverageStatus {
    return .{
        .policy = policy,
        .observation_complete = true,
        .observation_incomplete_reasons = &.{},
        .config_fingerprint = "0123456789abcdef",
        .summary_ready = true,
        .config_mismatch_group_count = 0,
        .source_total = 10,
        .produced = 5,
        .skipped = 0,
        .terminal_failed = 0,
        .covered = 5,
        .settled = 5,
        .uncovered = 5,
        .pending = 5,
        .complete = false,
        .healthy = false,
        .degraded = false,
    };
}

test "external readiness permits intentional partial coverage" {
    var external = makeCoverage(.external);
    try std.testing.expect(coverageReady(external));
    try std.testing.expect(embeddingIndexReady(.{
        .index_type = .embeddings,
        .backfill_state = "ready",
        .rebuilding = false,
        .coverage = external,
    }));

    var strict = makeCoverage(.strict);
    try std.testing.expect(!coverageReady(strict));
    strict.complete = true;
    strict.healthy = true;
    try std.testing.expect(coverageReady(strict));

    external.observation_complete = false;
    try std.testing.expect(!coverageReady(external));
    external.observation_complete = true;
    external.config_mismatch_group_count = 1;
    try std.testing.expect(!coverageReady(external));
}
