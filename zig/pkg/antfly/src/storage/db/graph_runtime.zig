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
const builtin = @import("builtin");

const graph_mod = @import("../../graph/graph.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const types = @import("types.zig");

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

test "db graph runtime prepared ownership waits for authoritative range commit across reopen" {
    const DB = @import("mod.zig").DB;
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const request = types.BatchRequest{ .split_transition = .{ .kind = .finalize, .transition_id = 1, .attempt_epoch = 1, .destination_group_id = 2, .split_key = "m" } };
    {
        var db = try DB.open(a, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "z", .target = "a", .edge_type = "link", .weight = 1 }}, .sync_level = .full_index });
        graph_mod.test_abort_ownership_before_range_commit = true;
        defer graph_mod.test_abort_ownership_before_range_commit = false;
        try std.testing.expectError(error.TestInjectedBackfillFailure, db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 }));
        try std.testing.expect((try db.raftAppliedEntry()) == null);
        const index = &db.core.index_manager.graphIndex("g").?.index;
        try std.testing.expect(index.ownershipTransitionPending());
        try std.testing.expect(!index.ownershipCleanupPending());
        try std.testing.expect(!try db.core.index_manager.runGraphOwnershipCleanupStep());
        const visible = try index.getEdges(a, "a", "link", .in);
        defer graph_mod.GraphIndex.freeEdges(a, visible);
        try std.testing.expectEqual(@as(usize, 1), visible.len);
    }
    var db = try DB.open(a, std.mem.span(path), .{});
    defer db.close();
    const index = &db.core.index_manager.graphIndex("g").?.index;
    try std.testing.expect(index.ownershipTransitionPending());
    try std.testing.expect(!index.ownershipCleanupPending());
    try std.testing.expectEqual(@as(u64, 1), (try index.stats(a)).edge_count);
    try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
    try std.testing.expect(index.ownershipCleanupPending());
    const hidden = try index.getEdges(a, "a", "link", .in);
    defer graph_mod.GraphIndex.freeEdges(a, hidden);
    try std.testing.expectEqual(@as(usize, 0), hidden.len);
    // Receipt replay does not need to wait for any physical cleanup.
    try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
    try std.testing.expectEqual(@as(u64, 1), index.edge_count);
    while (try db.core.index_manager.runGraphOwnershipCleanupStep()) {}
    try std.testing.expectEqual(@as(u64, 0), index.edge_count);
}

test "db graph runtime expansion waits for retirement before range and merge receipt commit" {
    const DB = @import("mod.zig").DB;
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const merge = types.BatchRequest{ .merge_checkpoint = .{
        .kind = .accept,
        .transition_id = 10,
        .donor_group_id = 2,
        .receiver_group_id = 1,
        .receiver_base_start = "",
        .receiver_base_end = "m",
        .merged_start = "",
        .merged_end = "",
    } };
    {
        var db = try DB.open(a, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "z", .target = "a", .edge_type = "link", .weight = 1 }}, .sync_level = .full_index });
        try db.batchRaftReplicatedApply(.{ .split_transition = .{ .kind = .finalize, .transition_id = 1, .attempt_epoch = 1, .destination_group_id = 2, .split_key = "m" } }, .{ .term = 1, .index = 1 });
        try std.testing.expectError(error.GraphMaintenanceInProgress, db.updateRange(.{ .start = "", .end = "" }));
        try std.testing.expectError(error.GraphMaintenanceInProgress, db.core.updateRange(.{ .start = "", .end = "" }));
        try std.testing.expectError(error.RaftApplyWriterUnavailable, db.batchRaftReplicatedApply(merge, .{ .term = 1, .index = 2 }));
        try std.testing.expectEqualStrings("m", db.getRange().end);
        try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
        const stats = try db.stats(a);
        defer types.freeDBStats(a, stats);
        try std.testing.expect(stats.indexes[0].graph_counts_pending);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].edge_count);
    }
    {
        var db = try DB.open(a, std.mem.span(path), .{});
        defer db.close();
        try std.testing.expectEqualStrings("m", db.getRange().end);
        const index = &db.core.index_manager.graphIndex("g").?.index;
        try std.testing.expect(index.ownershipCleanupPending());
        try std.testing.expectError(error.RaftApplyWriterUnavailable, db.batchRaftReplicatedApply(merge, .{ .term = 1, .index = 2 }));
        try db.runArtifactRepairMetadataMaintenanceUntilIdle();
        try std.testing.expect(!index.ownershipTransitionPending());
        try db.batchRaftReplicatedApply(merge, .{ .term = 1, .index = 2 });
        try std.testing.expectEqualStrings("", db.getRange().end);
        try std.testing.expectEqual(@as(u64, 2), (try db.raftAppliedEntry()).?.index);
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "z", .target = "b", .edge_type = "link", .weight = 1 }}, .sync_level = .full_index });
        const stats = try db.stats(a);
        defer types.freeDBStats(a, stats);
        try std.testing.expect(!stats.indexes[0].graph_counts_pending);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].edge_count);
    }
    var db = try DB.open(a, std.mem.span(path), .{});
    defer db.close();
    const incoming = try db.getEdges(a, "g", "b", "link", .in);
    defer graph_mod.GraphIndex.freeEdges(a, incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
    try std.testing.expectEqualStrings("z", incoming[0].source);
    const retired = try db.getEdges(a, "g", "a", "link", .in);
    defer graph_mod.GraphIndex.freeEdges(a, retired);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
}

test "db graph runtime repeated split defers behind cleanup without advancing its receipt" {
    const DB = @import("mod.zig").DB;
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    var db = try DB.open(a, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .graph_writes = &.{
        .{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "link", .weight = 1 },
        .{ .index_name = "g", .source = "i", .target = "hub", .edge_type = "link", .weight = 1 },
        .{ .index_name = "g", .source = "z", .target = "hub", .edge_type = "link", .weight = 1 },
    }, .sync_level = .full_index });
    const first = types.BatchRequest{ .split_transition = .{ .kind = .finalize, .transition_id = 1, .attempt_epoch = 1, .destination_group_id = 2, .split_key = "m" } };
    const second = types.BatchRequest{ .split_transition = .{ .kind = .finalize, .transition_id = 2, .attempt_epoch = 1, .destination_group_id = 3, .split_key = "h" } };
    try db.batchRaftReplicatedApply(first, .{ .term = 1, .index = 1 });
    try std.testing.expectError(error.RaftApplyWriterUnavailable, db.batchRaftReplicatedApply(second, .{ .term = 1, .index = 2 }));
    try std.testing.expectEqualStrings("m", db.getRange().end);
    try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
    const index = &db.core.index_manager.graphIndex("g").?.index;
    try std.testing.expectEqual(@as(u64, 2), (try index.stats(a)).edge_count);
    // The normal metadata scheduler also services graph-only indexes.
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try db.batchRaftReplicatedApply(second, .{ .term = 1, .index = 2 });
    try std.testing.expectEqualStrings("h", db.getRange().end);
    try std.testing.expectEqual(@as(u64, 2), (try db.raftAppliedEntry()).?.index);
    try std.testing.expectEqual(@as(u64, 1), (try index.stats(a)).edge_count);
    try db.runArtifactRepairMetadataMaintenanceUntilIdle();
    try std.testing.expectEqual(@as(u64, 1), index.edge_count);
}

test "db graph runtime replicated split fences topology before receipt and retires it after" {
    const DB = @import("mod.zig").DB;
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const request = types.BatchRequest{ .split_transition = .{ .kind = .finalize, .transition_id = 1, .attempt_epoch = 1, .destination_group_id = 2, .split_key = "m" } };
    var epoch: u64 = undefined;
    {
        var db = try DB.open(a, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\"}}}" });
        try db.batch(.{ .graph_writes = &.{
            .{ .index_name = "g", .source = "a", .target = "z", .edge_type = "link", .weight = 1 },
            .{ .index_name = "g", .source = "z", .target = "y", .edge_type = "link", .weight = 1 },
        }, .sync_level = .full_index });
        const index = &db.core.index_manager.graphIndex("g").?.index;
        var initial = try index.runGraphMetric("degree");
        defer initial.deinit(a);
        {
            graph_mod.test_abort_prune_after_forward_commit = true;
            defer graph_mod.test_abort_prune_after_forward_commit = false;
            // No edge pruning occurs on the Raft apply path.
            try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
            try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
            try std.testing.expect(index.ownershipCleanupPending());
            try std.testing.expectEqual(@as(u64, 2), index.edge_count);
            try std.testing.expectEqual(@as(u64, 1), (try index.stats(a)).edge_count);
            const hidden = try index.getEdges(a, "y", "link", .in);
            defer graph_mod.GraphIndex.freeEdges(a, hidden);
            try std.testing.expectEqual(@as(usize, 0), hidden.len);
            try std.testing.expectError(error.TestInjectedBackfillFailure, index.pruneOwnedRangePage());
            try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
        }
    }
    {
        // Reopen reconciles authoritative ownership before resuming cleanup.
        // The receipt and logical visibility do not depend on that drain.
        var db = try DB.open(a, std.mem.span(path), .{});
        defer db.close();
        try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
        try std.testing.expectEqualStrings("m", db.getRange().end);
        const index = &db.core.index_manager.graphIndex("g").?.index;
        try std.testing.expectEqual(@as(u64, 1), (try index.stats(a)).edge_count);
        var stale = try index.graphMetricStatus("degree");
        defer stale.deinit(a);
        try std.testing.expect(stale.state != .fresh);
        var fresh = try index.runGraphMetric("degree");
        defer fresh.deinit(a);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, fresh.state);
        try std.testing.expect((try index.graphMetricScore("degree", "y")) == null);
        // A retained source still owns its cross-range outgoing edge.
        const edges = try index.getEdges(a, "a", "link", .out);
        defer graph_mod.GraphIndex.freeEdges(a, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("z", edges[0].target);
        epoch = index.edge_generation;
        try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
        try std.testing.expectEqual(epoch, index.edge_generation);
    }
    var reopened = try DB.open(a, std.mem.span(path), .{});
    defer reopened.close();
    try std.testing.expectEqualStrings("m", reopened.getRange().end);
    try reopened.batchRaftReplicatedApply(request, .{ .term = 1, .index = 1 });
    try std.testing.expectEqual(epoch, reopened.core.index_manager.graphIndex("g").?.index.edge_generation);
    try std.testing.expectEqual(@as(u64, 1), (try reopened.raftAppliedEntry()).?.index);
}

test "db graph runtime helpers expose edges neighbors and shortest path" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
        },
        .sync_level = .full_index,
    });

    const edges = try db.getEdges(alloc, "citations", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    const neighbors = try db.getNeighbors(alloc, "citations", "a", "cites", .out);
    defer traversal_mod.freeOwnedResults(alloc, neighbors);
    try std.testing.expectEqual(@as(usize, 2), neighbors.len);

    const traversed = try db.traverseEdges(alloc, "citations", "a", .{
        .direction = .out,
        .edge_types = &.{"cites"},
        .max_depth = 2,
    });
    defer traversal_mod.freeOwnedResults(alloc, traversed);
    try std.testing.expectEqual(@as(usize, 3), traversed.len);

    const shortest = (try db.findShortestPath(alloc, "citations", "a", "d", &.{"cites"}, .out, .min_hops, 8, null, null)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 2), shortest.length);
    try std.testing.expectEqual(@as(usize, 3), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("d", shortest.nodes[2]);

    const algebraic_shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 8, null, null)).?;
    defer paths_mod.freePath(alloc, algebraic_shortest);
    try std.testing.expectEqual(@as(u32, 2), algebraic_shortest.length);
    try std.testing.expectEqual(@as(usize, 3), algebraic_shortest.nodes.len);
    try std.testing.expectEqualStrings("a", algebraic_shortest.nodes[0]);
    try std.testing.expectEqualStrings("b", algebraic_shortest.nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_shortest.nodes[2]);
    try std.testing.expectEqual(@as(usize, 2), algebraic_shortest.edges.len);
    try std.testing.expectEqualStrings("cites", algebraic_shortest.edges[0].edge_type);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 8, null, null);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 2), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[2]);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var found_alg_stats = false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "citations_alg")) continue;
        found_alg_stats = true;
        try std.testing.expect(item.algebraic_graph_traversal_attempt_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_proven_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_result_node_count > 0);
    }
    try std.testing.expect(found_alg_stats);
}

test "db graph runtime algebraic shortest path applies exact min-hop edge weight filters" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 0.5 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "e", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "e", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "d", .edge_type = "cites", .weight = 5.0 },
        },
        .sync_level = .full_index,
    });

    const shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 3), shortest.length);
    try std.testing.expectEqual(@as(usize, 4), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("c", shortest.nodes[1]);
    try std.testing.expectEqualStrings("e", shortest.nodes[2]);
    try std.testing.expectEqualStrings("d", shortest.nodes[3]);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 3), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("c", algebraic_k_one[0].nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[3]);
}

test "db graph runtime personalized graph metric seeds shape top-k and rerank ordering" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"rank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}},\"deg\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    // Seed cluster a<->b bridged through c to hub, which dominates global
    // PageRank with four in-edges.
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:a\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:f", .value = "{\"title\":\"zeta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" },
        },
        .sync_level = .full_index,
    });

    const seed_a = [_][]const u8{"doc:a"};
    const seed_f = [_][]const u8{"doc:f"};

    // Personalized rankings are computed fresh from the edge snapshot and
    // need no published generation. Seeding near the a<->b cluster keeps the
    // one-hop neighbor above the globally dominant hub.
    var seeded_a = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "rank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "rank",
                .top_k = 7,
                .freshness = .fresh,
                .seed_nodes = &seed_a,
                .damping = 0.9,
            },
        }},
        .limit = 0,
    });
    defer seeded_a.deinit();
    try std.testing.expectEqual(@as(usize, 1), seeded_a.graph_metric_results.len);
    const seeded_a_scores = seeded_a.graph_metric_results[0].scores;
    try std.testing.expectEqual(@as(usize, 7), seeded_a_scores.len);
    var position_b: usize = seeded_a_scores.len;
    var position_hub: usize = seeded_a_scores.len;
    for (seeded_a_scores, 0..) |score, i| {
        if (std.mem.eql(u8, score.node, "doc:b")) position_b = i;
        if (std.mem.eql(u8, score.node, "doc:hub")) position_hub = i;
    }
    try std.testing.expect(position_b < position_hub);

    // A different seed set concentrates mass elsewhere and flips the order.
    var seeded_f = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "rank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "rank",
                .top_k = 7,
                .freshness = .fresh,
                .seed_nodes = &seed_f,
            },
        }},
        .limit = 0,
    });
    defer seeded_f.deinit();
    const seeded_f_scores = seeded_f.graph_metric_results[0].scores;
    var f_position_b: usize = seeded_f_scores.len;
    var f_position_hub: usize = seeded_f_scores.len;
    for (seeded_f_scores, 0..) |score, i| {
        if (std.mem.eql(u8, score.node, "doc:b")) f_position_b = i;
        if (std.mem.eql(u8, score.node, "doc:hub")) f_position_hub = i;
    }
    try std.testing.expect(f_position_hub < f_position_b);

    // Global published ranking is dominated by the hub, unlike the seeded one.
    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "rank");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    var global = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "rank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "rank",
                .top_k = 1,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer global.deinit();
    try std.testing.expectEqualStrings("doc:hub", global.graph_metric_results[0].scores[0].node);

    // Seeded rerank blends the personalized column into search hits: with a
    // zero base weight, ordering is exactly the personalized metric order.
    var seeded_rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "rank",
            .freshness = .fresh,
            .base_weight = 0.0,
            .weight = 1.0,
            .seed_nodes = &seed_a,
            .damping = 0.9,
        },
        .limit = 7,
        .include_stored = false,
    });
    defer seeded_rerank.deinit();
    try std.testing.expectEqual(@as(usize, 7), seeded_rerank.hits.len);
    var rerank_position_b: usize = seeded_rerank.hits.len;
    var rerank_position_hub: usize = seeded_rerank.hits.len;
    for (seeded_rerank.hits, 0..) |hit, i| {
        if (std.mem.eql(u8, hit.id, "doc:b")) rerank_position_b = i;
        if (std.mem.eql(u8, hit.id, "doc:hub")) rerank_position_hub = i;
    }
    try std.testing.expect(rerank_position_b < rerank_position_hub);

    var global_rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "rank",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
        },
        .limit = 7,
        .include_stored = false,
    });
    defer global_rerank.deinit();
    try std.testing.expectEqualStrings("doc:hub", global_rerank.hits[0].id);

    // Personalization contract failures stay client errors: published
    // freshness with seeds, damping without seeds, and non-pagerank metrics.
    try std.testing.expectError(error.GraphMetricPersonalizationRequiresFresh, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "rank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "rank",
                .top_k = 1,
                .freshness = .published,
                .seed_nodes = &seed_a,
            },
        }},
        .limit = 0,
    }));
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "rank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "rank",
                .top_k = 1,
                .freshness = .fresh,
                .damping = 0.9,
            },
        }},
        .limit = 0,
    }));
    try std.testing.expectError(error.UnsupportedGraphMetric, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "deg",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "deg",
                .top_k = 1,
                .freshness = .fresh,
                .seed_nodes = &seed_a,
            },
        }},
        .limit = 0,
    }));
}
