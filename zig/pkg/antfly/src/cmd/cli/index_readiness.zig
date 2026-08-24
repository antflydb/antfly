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

// Readiness is advisory for a query: keep its control-plane lookup bounded so
// an unhealthy status endpoint cannot hold the data-plane request hostage.
const semantic_readiness_timeout_ms: u64 = 1_500;
// Retrieval agents have their own server-side index selection and readiness
// checks. Keep the optional CLI advisory on a strict interactive latency bound
// and only run it when the caller explicitly selected indexes.
const retrieval_advisory_timeout_ms: u64 = 250;

/// Coverage is an integrity signal, but its completion semantics are policy
/// specific. External indexes are query-ready once replay is current even when
/// callers intentionally supplied vectors for only part of the source table.
pub fn coverageReady(status: antfly_client.types.DerivedCoverageStatus) bool {
    if (!status.observation_complete or status.config_mismatch_group_count != 0) return false;
    return status.policy == .external or status.complete;
}

pub fn embeddingIndexReady(stats: antfly_client.types.EmbeddingsIndexStats) bool {
    if (stats.readiness) |readiness| return readiness.state == .ready;
    if (stats.@"error" != null) return false;
    if (stats.backfill_state) |state| {
        if (!std.mem.eql(u8, state, "ready")) return false;
    } else if (stats.rebuilding orelse true) {
        return false;
    }
    const status = stats.coverage orelse return false;
    return coverageReady(status);
}

fn printEmbeddingReadinessWarning(
    table_name: []const u8,
    index: antfly_client.types.IndexStatus,
) void {
    const stats = switch (index.status) {
        .embeddings_index_stats => |value| value,
        else => {
            std.debug.print(
                "warning: semantic index {s} has no embeddings readiness status; results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
                .{ index.config.name, table_name, index.config.name },
            );
            return;
        },
    };
    if (embeddingIndexReady(stats)) return;

    const state = if (stats.readiness) |readiness|
        @tagName(readiness.state)
    else if (stats.coverage) |coverage|
        if (coverage.config_mismatch_group_count > 0) "config_mismatch" else stats.backfill_state orelse "not_ready"
    else if (stats.backfill_state != null and std.mem.eql(u8, stats.backfill_state.?, "ready"))
        "coverage_unavailable"
    else
        stats.backfill_state orelse if (stats.rebuilding orelse true) "running" else "not_ready";
    if (stats.coverage) |coverage| {
        std.debug.print(
            "warning: semantic index {s} is {s} (coverage {d}/{d}, complete={any}); results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
            .{ index.config.name, state, coverage.produced, coverage.source_total, coverage.complete, table_name, index.config.name },
        );
    } else if (stats.backfill_progress) |progress| {
        std.debug.print(
            "warning: semantic index {s} is {s} ({d:.1}%); results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
            .{ index.config.name, state, @max(0.0, @min(1.0, progress)) * 100.0, table_name, index.config.name },
        );
    } else {
        std.debug.print(
            "warning: semantic index {s} is {s}; results may be incomplete. Run `antfly index wait --table {s} --index {s}`.\n",
            .{ index.config.name, state, table_name, index.config.name },
        );
    }
}

fn selectedEarlier(selected: []const []const u8, index: usize) bool {
    for (selected[0..index]) |earlier| {
        if (std.mem.eql(u8, earlier, selected[index])) return true;
    }
    return false;
}

/// Best-effort semantic readiness advisory shared by direct queries and
/// retrieval-agent queries. It performs exactly one bounded list request and
/// accounts for every explicitly selected index without allocating or issuing
/// per-index requests.
pub fn warnIfSemanticIndexesAreNotReady(
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    selected_indexes: ?[]const []const u8,
) void {
    warnIfSemanticIndexesAreNotReadyWithTimeout(client, table_name, selected_indexes, semantic_readiness_timeout_ms);
}

pub fn warnIfSelectedSemanticIndexesAreNotReadyForRetrieval(
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    selected_indexes: ?[]const []const u8,
) void {
    if (selected_indexes == null) return;
    warnIfSemanticIndexesAreNotReadyWithTimeout(client, table_name, selected_indexes, retrieval_advisory_timeout_ms);
}

fn warnIfSemanticIndexesAreNotReadyWithTimeout(
    client: *antfly_client.AntflyClient,
    table_name: []const u8,
    selected_indexes: ?[]const []const u8,
    timeout_ms: u64,
) void {
    var resp = client.listIndexesResponseWithTimeout(table_name, timeout_ms) catch |err| {
        // Direct queries cancel their advisory as soon as the data-plane
        // response arrives. Cancellation is the healthy fast path, not a
        // user-facing readiness failure.
        if (err == error.Canceled or err == error.Cancelled) return;
        std.debug.print("warning: unable to verify semantic index readiness: {s}\n", .{@errorName(err)});
        return;
    };
    defer resp.deinit();
    const parsed = resp.data orelse {
        std.debug.print("warning: unable to verify semantic index readiness (HTTP {d})\n", .{resp.status_code});
        return;
    };

    if (selected_indexes) |selected| {
        for (selected, 0..) |name, selected_index| {
            if (selectedEarlier(selected, selected_index)) continue;
            var found: ?antfly_client.types.IndexStatus = null;
            for (parsed.value) |index| {
                if (std.mem.eql(u8, name, index.config.name)) {
                    found = index;
                    break;
                }
            }
            const index = found orelse {
                std.debug.print(
                    "warning: selected semantic index {s} was not found on table {s}; the query may fail.\n",
                    .{ name, table_name },
                );
                continue;
            };
            if (index.config.type != .embeddings) {
                std.debug.print(
                    "warning: selected semantic index {s} is type {s}, not embeddings; the query may fail.\n",
                    .{ name, @tagName(index.config.type) },
                );
                continue;
            }
            printEmbeddingReadinessWarning(table_name, index);
        }
        return;
    }

    var embedding_count: usize = 0;
    for (parsed.value) |index| {
        if (index.config.type != .embeddings) continue;
        embedding_count += 1;
        printEmbeddingReadinessWarning(table_name, index);
    }
    if (embedding_count == 0) {
        std.debug.print(
            "warning: table {s} has no embeddings indexes available for semantic search; the query may fail.\n",
            .{table_name},
        );
    }
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

test "semantic readiness selection deduplicates without allocation" {
    const selected = [_][]const u8{ "dense", "sparse", "dense" };
    try std.testing.expect(!selectedEarlier(&selected, 0));
    try std.testing.expect(!selectedEarlier(&selected, 1));
    try std.testing.expect(selectedEarlier(&selected, 2));
}
