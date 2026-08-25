// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Checked manifest of replayable Antfly VOPR adapters. Search-policy and CLI
//! implementation files are intentionally outside this manifest: their seeded
//! exploration PRNG and native worker threads choose complete histories but do
//! not execute inside, or contribute state to, a replayable world.

const std = @import("std");
const vopr = @import("vopr");

const Source = struct {
    path: []const u8,
    bytes: []const u8,
};

const metadata_sim_source = @embedFile("../metadata/sim_harness.zig");

fn region(comptime source: []const u8, comptime begin: []const u8, comptime end: []const u8) []const u8 {
    @setEvalBranchQuota(100_000);
    const start = std.mem.indexOf(u8, source, begin) orelse @compileError("determinism audit region start not found: " ++ begin);
    const tail = source[start..];
    const finish = std.mem.indexOf(u8, tail, end) orelse @compileError("determinism audit region end not found: " ++ end);
    return tail[0..finish];
}

const replayable_sources = [_]Source{
    .{ .path = "vopr/admission.zig", .bytes = @embedFile("admission.zig") },
    .{ .path = "vopr/auth_lifecycle.zig", .bytes = @embedFile("auth_lifecycle.zig") },
    .{ .path = "vopr/backfill_marker_discovery.zig", .bytes = @embedFile("backfill_marker_discovery.zig") },
    .{ .path = "vopr/capi_lite_lifecycle.zig", .bytes = @embedFile("capi_lite_lifecycle.zig") },
    .{ .path = "vopr/composed_query.zig", .bytes = @embedFile("composed_query.zig") },
    .{ .path = "vopr/config_extension_lifecycle.zig", .bytes = @embedFile("config_extension_lifecycle.zig") },
    .{ .path = "vopr/data_server.zig", .bytes = @embedFile("data_server.zig") },
    .{ .path = "vopr/db_index_races.zig", .bytes = @embedFile("db_index_races.zig") },
    .{ .path = "vopr/distributed_query.zig", .bytes = @embedFile("distributed_query.zig") },
    .{ .path = "vopr/domain_vopr.zig", .bytes = @embedFile("domain_vopr.zig") },
    .{ .path = "vopr/embedded_lite_lifecycle.zig", .bytes = @embedFile("embedded_lite_lifecycle.zig") },
    .{ .path = "vopr/external_lake.zig", .bytes = @embedFile("external_lake.zig") },
    .{ .path = "vopr/full_cluster.zig", .bytes = @embedFile("full_cluster.zig") },
    .{ .path = "vopr/generation_reranking.zig", .bytes = @embedFile("generation_reranking.zig") },
    .{ .path = "vopr/generation_lifecycle.zig", .bytes = @embedFile("generation_lifecycle.zig") },
    .{ .path = "vopr/http_disconnect.zig", .bytes = @embedFile("http_disconnect.zig") },
    .{ .path = "vopr/http_lifecycle.zig", .bytes = @embedFile("http_lifecycle.zig") },
    .{ .path = "vopr/media_runtime.zig", .bytes = @embedFile("media_runtime.zig") },
    .{ .path = "vopr/object_store.zig", .bytes = @embedFile("object_store.zig") },
    .{ .path = "vopr/parquet_cache.zig", .bytes = @embedFile("parquet_cache.zig") },
    .{ .path = "vopr/provider_boundaries.zig", .bytes = @embedFile("provider_boundaries.zig") },
    .{ .path = "vopr/provisioning_startup.zig", .bytes = @embedFile("provisioning_startup.zig") },
    .{ .path = "vopr/query_embedding_cache.zig", .bytes = @embedFile("query_embedding_cache.zig") },
    .{ .path = "vopr/replication_backfill.zig", .bytes = @embedFile("replication_backfill.zig") },
    .{ .path = "vopr/request_lifecycle.zig", .bytes = @embedFile("request_lifecycle.zig") },
    .{ .path = "vopr/resource_pressure.zig", .bytes = @embedFile("resource_pressure.zig") },
    .{ .path = "vopr/serverless_workflow.zig", .bytes = @embedFile("serverless_workflow.zig") },
    .{ .path = "vopr/supervision.zig", .bytes = @embedFile("supervision.zig") },
    .{ .path = "vopr/upgrade_compatibility.zig", .bytes = @embedFile("upgrade_compatibility.zig") },
    .{ .path = "raft/vopr.zig", .bytes = @embedFile("../raft/vopr.zig") },
    .{ .path = "storage/lsm_vopr.zig", .bytes = @embedFile("../storage/lsm_vopr.zig") },
    .{ .path = "storage/lmdb_vopr.zig", .bytes = @embedFile("../storage/lmdb_vopr.zig") },
    .{ .path = "storage/ha/vopr.zig", .bytes = @embedFile("../storage/ha/vopr.zig") },
    .{ .path = "storage/wal_vopr.zig", .bytes = @embedFile("../storage/wal_vopr.zig") },
    .{ .path = "storage/persistent_vopr.zig", .bytes = @embedFile("../storage/persistent_vopr.zig") },
    .{ .path = "storage/transaction_vopr.zig", .bytes = @embedFile("../storage/transaction_vopr.zig") },
    .{ .path = "storage/index_manager_vopr.zig", .bytes = @embedFile("../storage/index_manager_vopr.zig") },
    .{ .path = "storage/db_split_vopr.zig", .bytes = @embedFile("../storage/db_split_vopr.zig") },
    .{
        .path = "metadata/sim_harness.zig#DistributedDataVoprScenario",
        .bytes = region(
            metadata_sim_source,
            "pub const DistributedDataVoprCampaignConfig",
            "fn runAutomaticMergePublicTrafficScenario",
        ),
    },
    .{
        .path = "metadata/sim_harness.zig#MetadataVoprScenario",
        .bytes = region(
            metadata_sim_source,
            "const metadata_vopr_scenario_version",
            "test \"metadata VOPR",
        ),
    },
};

test "replayable Antfly VOPR sources pass the fail-closed determinism audit" {
    for (replayable_sources) |source| {
        if (vopr.determinism.firstFinding(source.bytes)) |finding| {
            std.debug.print("determinism violation {s}:{d}:{d} category={s} token={s}\n", .{
                source.path,
                finding.line,
                finding.column,
                @tagName(finding.category),
                finding.token,
            });
            return error.UncontrolledDeterminismSource;
        }
    }
}

test "determinism manifest covers every exported Antfly VOPR source" {
    const root_source = @embedFile("../root.zig");
    var lines = std.mem.splitScalar(u8, root_source, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "_vopr = @import(\"") == null) continue;
        const import_start = std.mem.indexOf(u8, line, "@import(\"").? + "@import(\"".len;
        const suffix = line[import_start..];
        const import_end = std.mem.indexOfScalar(u8, suffix, '"') orelse return error.InvalidVoprRootImport;
        const path = suffix[0..import_end];
        if (!std.mem.endsWith(u8, path, ".zig")) continue;
        if (std.mem.eql(u8, path, "vopr/determinism_audit.zig")) continue;
        var found = false;
        for (replayable_sources) |source| {
            if (std.mem.eql(u8, source.path, path)) {
                found = true;
                break;
            }
        }
        if (!found) {
            std.debug.print("exported VOPR source missing from determinism manifest: {s}\n", .{path});
            return error.VoprDeterminismManifestIncomplete;
        }
    }
}
