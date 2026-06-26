const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

const Allocator = std.mem.Allocator;

const db_internal = @import("internal.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_query_mod = @import("../../graph/query.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mapper = @import("document_mapper.zig");
const promotion_runtime_mod = @import("promotion_runtime.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

pub const default_test_wait_attempts: usize = 100;
pub const slow_test_wait_attempts: usize = 500;

var temp_path_nonce: u64 = 0;

pub fn profileBenchTestsEnabled() bool {
    if (comptime builtin.os.tag == .freestanding) return false;
    return db_internal.getenv("ANTFLY_RUN_PROFILE_BENCH_TESTS") != null;
}

pub fn lockApply(db: anytype) void {
    db.core.lockApply();
}

pub fn stressDenseBackend() hbc_mod.StorageBackend {
    const raw = db_internal.getenv("ANTFLY_STRESS_DENSE_BACKEND") orelse return .lsm;
    if (std.ascii.eqlIgnoreCase(raw, "lmdb")) return .lmdb;
    return .lsm;
}

pub fn allocStressDenseDocJson(alloc: Allocator, dims: usize, doc_index: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"title\":\"dense\",\"_embeddings\":{\"dv_v1\":[");
    for (0..dims) |dim_index| {
        if (dim_index > 0) try out.append(alloc, ',');

        var value_buf: [64]u8 = undefined;
        const rendered = try std.fmt.bufPrint(&value_buf, "{d}", .{index_manager_mod.stressDenseValue(doc_index, dim_index)});
        try out.appendSlice(alloc, rendered);
    }
    try out.appendSlice(alloc, "]}}");

    const owned = try alloc.dupe(u8, out.items);
    out.deinit(alloc);
    return owned;
}

pub fn tempPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-db-test-";
    const ts = platform.time.monotonicNs();
    const pid: u32 = @intCast(std.posix.system.getpid());
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path = std.fmt.bufPrint(buf, "{s}{d}-{d}-{d}\x00", .{ base, pid, ts, nonce }) catch unreachable;
    return @ptrCast(path.ptr);
}

pub fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

pub fn cleanupSnapshotDirForPath(path: [*:0]const u8) void {
    var snapshots_buf: [512]u8 = undefined;
    const snapshots = std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(path)}) catch return;
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshots) catch {};
}

pub fn corruptNonEmptyFilesUnderDir(alloc: Allocator, root_path: []const u8) !usize {
    var io_impl = db_internal.threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    var root_dir = try std.Io.Dir.cwd().openDir(io, root_path, .{ .iterate = true });
    defer root_dir.close(io);

    var walker = try root_dir.walk(alloc);
    defer walker.deinit();

    var corrupted: usize = 0;
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_path, entry.path });
        defer alloc.free(full_path);
        const stat = try std.Io.Dir.cwd().statFile(io, full_path, .{});
        if (stat.size == 0) continue;

        const bytes = try alloc.alloc(u8, @intCast(stat.size));
        defer alloc.free(bytes);
        for (bytes, 0..) |*byte, i| {
            byte.* = @truncate((i *% 131) +% 17);
        }
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = full_path,
            .data = bytes,
        });
        corrupted += 1;
    }
    return corrupted;
}

pub fn cacheBlockHitsForBench(stats: anytype) u64 {
    var hits: u64 = 0;
    if (@hasField(@TypeOf(stats), "run_table_block")) {
        hits += stats.run_table_block.hits;
    }
    if (@hasField(@TypeOf(stats), "run_table_physical_block")) {
        hits += stats.run_table_physical_block.hits;
    }
    return hits;
}

/// A queued obsolete path only counts as reclaimable once no reader pins
/// the backend; transient background readers (index loads, status probes)
/// mask it as pinned_by_readers for a moment. Poll instead of asserting a
/// single snapshot so loaded CI machines don't flake.
pub fn expectObsoletePathsReclaimable(backend: *lsm_backend_mod.Backend, expected: u64) !void {
    var attempts: usize = 0;
    while (backend.snapshotMaintenanceStats().obsolete_paths_reclaimable != expected) : (attempts += 1) {
        if (attempts >= 2000) {
            return std.testing.expectEqual(expected, backend.snapshotMaintenanceStats().obsolete_paths_reclaimable);
        }
        db_internal.spinOrYield();
    }
}

pub fn expectRelationalTemporalPriceRow(
    alloc: Allocator,
    row_json: []const u8,
    sku: []const u8,
    valid_from: f64,
    valid_to: f64,
    price: f64,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    try std.testing.expectEqualStrings(sku, object.get("sku").?.string);
    try std.testing.expectEqual(valid_from, relational_store_mod.jsonNumberAsF64(object.get("valid_from").?) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(valid_to, relational_store_mod.jsonNumberAsF64(object.get("valid_to").?) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(price, relational_store_mod.jsonNumberAsF64(object.get("price").?) orelse return error.TestExpectedEqual);
}

pub fn expectRelationalTemporalPrimarySelectorPriceRow(
    alloc: Allocator,
    db: anytype,
    runtime_schema: schema_mod.TableSchema,
    sku: []const u8,
    point_json: []const u8,
    valid_from: f64,
    valid_to: f64,
    price: f64,
) !void {
    const primary_key = runtime_schema.primary_key orelse return error.TestExpectedEqual;
    const period_name = primary_key.without_overlaps_period orelse return error.TestExpectedEqual;
    var period: ?schema_mod.RelationalPeriod = null;
    for (runtime_schema.periods) |candidate| {
        if (std.mem.eql(u8, candidate.name, period_name)) {
            period = candidate;
            break;
        }
    }
    const resolved_period = period orelse return error.TestExpectedEqual;
    var start_column: ?schema_mod.RelationalColumn = null;
    for (runtime_schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.path, resolved_period.start_column)) {
            start_column = column;
            break;
        }
    }
    const resolved_start_column = start_column orelse return error.TestExpectedEqual;

    const tuple_row_json = try std.fmt.allocPrint(
        alloc,
        "{{\"sku\":{f},\"valid_from\":0,\"valid_to\":1}}",
        .{std.json.fmt(sku, .{})},
    );
    defer alloc.free(tuple_row_json);
    const tuple_row = try mapper.buildRelationalRowValueAlloc(alloc, tuple_row_json, runtime_schema.relational_columns);
    defer alloc.free(tuple_row);
    const tuple = try relational_store_mod.primaryKeyTupleValueAlloc(alloc, tuple_row, primary_key);
    defer alloc.free(tuple);
    const point = try relational_store_mod.temporalPeriodBoundBytesFromJsonAlloc(alloc, point_json, resolved_start_column);
    defer alloc.free(point);
    const owner = (try db.lookupRelationalTemporalUniqueOwner(
        alloc,
        relational_store_mod.primary_key_constraint_name,
        tuple,
        point,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(owner);
    const row = (try db.get(alloc, owner)) orelse return error.TestExpectedEqual;
    defer alloc.free(row);
    try expectRelationalTemporalPriceRow(alloc, row, sku, valid_from, valid_to, price);
}

pub fn verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads(
    comptime DB: type,
    alloc: Allocator,
    metric_name: []const u8,
    metric_kind: []const u8,
) !void {
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const graph_config = try std.fmt.allocPrint(
        alloc,
        "{{\"metrics\":{{\"{s}\":{{\"enabled\":true,\"kind\":\"{s}\",\"refresh\":\"manual\",\"max_iterations\":4,\"tolerance\":0.000001,\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
        .{ metric_name, metric_kind },
    );
    defer alloc.free(graph_config);
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = graph_config,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", metric_name);
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    const published_generation = refreshed.published_generation;
    try std.testing.expect(published_generation > 0);

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = metric_name,
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 1), initial.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    try std.testing.expectEqual(published_generation, initial.graph_metric_results[0].status.published_generation);
    try std.testing.expect(initial.graph_metric_results[0].scores.len > 0);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:new-hub", .value = "{\"title\":\"new hub\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const rebuilding_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        try std.testing.expect(target_generation > published_generation);
        var building = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", metric_name, target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);
        break :blk target_generation;
    };

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", metric_name, error.InvalidGraphMetricScore);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expectEqual(published_generation, failed.published_generation);
    try std.testing.expectEqual(rebuilding_generation, failed.target_edge_generation);

    var published_after_failure = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = metric_name,
                .top_k = 4,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_after_failure.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, published_after_failure.graph_metric_results[0].status.state);
    try std.testing.expectEqual(published_generation, published_after_failure.graph_metric_results[0].status.published_generation);
    try std.testing.expect(published_after_failure.graph_metric_results[0].scores.len > 0);
    for (published_after_failure.graph_metric_results[0].scores) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc:new-hub"));
        try std.testing.expect(std.math.isFinite(score.score));
    }

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = metric_name,
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings(metric_name, traversal_after_failure.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(traversal_after_failure.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expect(std.math.isFinite(traversal_after_failure.graph_results[0].nodes[0].metrics[0].score.?));
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_after_failure.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(published_generation, traversal_after_failure.graph_results[0].metric_status[0].published_generation);

    const published_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = metric_name,
        .freshness = .published,
    }};
    var published_order_query = published_graph_query;
    published_order_query.order_by = &published_metric_orders;
    var traversal_order_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_order_query }},
        .limit = 0,
    });
    defer traversal_order_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_order_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_order_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_order_after_failure.graph_results[0].metric_status[0].state);

    const published_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = metric_name,
        .op = .gte,
        .value = 0.0,
        .freshness = .published,
    }};
    var published_filter_query = published_graph_query;
    published_filter_query.where_metric = &published_metric_filters;
    var traversal_filter_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_filter_query }},
        .limit = 0,
    });
    defer traversal_filter_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_filter_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_filter_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_filter_after_failure.graph_results[0].metric_status[0].state);

    var rerank_after_failure = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = metric_name,
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_after_failure.deinit();
    try std.testing.expectEqual(@as(u32, 4), rerank_after_failure.total_hits);
    const rerank_status = rerank_after_failure.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    var saw_authority_score = false;
    var saw_new_hub_missing_score = false;
    for (rerank_after_failure.hits) |hit| {
        const details = hit.score_details orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(published_generation, details.published_generation);
        if (std.mem.eql(u8, hit.id, "doc:authority")) {
            saw_authority_score = details.metric_score != null;
        } else if (std.mem.eql(u8, hit.id, "doc:new-hub")) {
            saw_new_hub_missing_score = details.metric_score == null;
        }
    }
    try std.testing.expect(saw_authority_score);
    try std.testing.expect(saw_new_hub_missing_score);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = metric_name,
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = metric_name,
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = metric_name,
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = metric_name,
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = metric_name,
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

pub fn waitForSearchResult(alloc: Allocator, db: anytype, req: types.SearchRequest, min_hits: u32) !types.SearchResult {
    return waitForSearchResultWithAttempts(alloc, db, req, min_hits, default_test_wait_attempts);
}

pub fn waitForSearchResultWithAttempts(alloc: Allocator, db: anytype, req: types.SearchRequest, min_hits: u32, max_attempts: usize) !types.SearchResult {
    var last = try db.search(alloc, req);
    var attempts: usize = 0;
    while (last.total_hits < min_hits and attempts < max_attempts) : (attempts += 1) {
        last.deinit();
        db_internal.sleepPollInterval();
        last = try db.search(alloc, req);
    }
    if (last.total_hits < min_hits) {
        last.deinit();
        return error.Timeout;
    }
    return last;
}

pub fn waitForDenseSearchResult(alloc: Allocator, db: anytype, req: types.SearchRequest, min_hits: u32) !types.SearchResult {
    return waitForDenseSearchResultWithAttempts(alloc, db, req, min_hits, default_test_wait_attempts);
}

pub fn waitForDenseSearchResultWithAttempts(alloc: Allocator, db: anytype, req: types.SearchRequest, min_hits: u32, max_attempts: usize) !types.SearchResult {
    const dense = req.dense orelse return error.InvalidArgument;
    var last_profiled = try db.searchDenseProfiled(alloc, req, dense);
    var last = last_profiled.result;
    var attempts: usize = 0;
    while (last.total_hits < min_hits and attempts < max_attempts) : (attempts += 1) {
        last.deinit();
        db_internal.sleepPollInterval();
        last_profiled = try db.searchDenseProfiled(alloc, req, dense);
        last = last_profiled.result;
    }
    if (last.total_hits < min_hits) {
        last.deinit();
        return error.Timeout;
    }
    return last;
}

pub fn waitForDenseIndexResultsWithAttempts(index: *hbc_mod.HBCIndex, query: []const f32, k: usize, min_hits: usize, max_attempts: usize) !hbc_mod.SearchResults {
    var last = try index.searchWithRequest(.{
        .query = query,
        .k = k,
    });
    var attempts: usize = 0;
    while (last.getHits().len < min_hits and attempts < max_attempts) : (attempts += 1) {
        last.deinit();
        db_internal.sleepPollInterval();
        last = try index.searchWithRequest(.{
            .query = query,
            .k = k,
        });
    }
    if (last.getHits().len < min_hits) {
        last.deinit();
        return error.Timeout;
    }
    return last;
}

pub fn waitForAppliedSequenceAdvance(
    alloc: Allocator,
    db: anytype,
    index_name: []const u8,
    previous: u64,
) !u64 {
    var applied = try db.core.loadAppliedSequence(alloc, index_name);
    var attempts: usize = 0;
    while (applied <= previous and attempts < 100) : (attempts += 1) {
        db_internal.sleepPollInterval();
        applied = try db.core.loadAppliedSequence(alloc, index_name);
    }
    if (applied <= previous) return error.Timeout;
    return applied;
}

pub fn waitForRawDelete(alloc: Allocator, db: anytype, key: []const u8, max_attempts: usize) !void {
    var attempts: usize = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        const raw = try db.get(alloc, key);
        if (raw == null) return;
        alloc.free(raw.?);
        db_internal.sleepPollInterval();
    }
    return error.Timeout;
}

pub fn putDenseEmbeddingArtifactForTest(db: anytype, alloc: Allocator, artifact_key: []const u8, source_hash: ?u64, vector: []const f32) !void {
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, source_hash, vector);
    defer alloc.free(payload);
    try db.core.store.put(artifact_key, payload);
    try db.core.putArtifactPresenceMarker();
}

pub fn putSparseEmbeddingArtifactForTest(
    db: anytype,
    alloc: Allocator,
    artifact_key: []const u8,
    source_hash: ?u64,
    indices: []const u32,
    values: []const f32,
) !void {
    const payload = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, source_hash, indices, values);
    defer alloc.free(payload);
    try db.core.store.put(artifact_key, payload);
    try db.core.putArtifactPresenceMarker();
}

pub const CountingDenseEmbedder = struct {
    deterministic: embedder_mod.DeterministicDenseEmbedder = .{},
    calls: usize = 0,

    fn embedDense(ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *CountingDenseEmbedder = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return try embedder_mod.DeterministicDenseEmbedder.embedDense(&self.deterministic, alloc, embedding_name, text, dims);
    }

    pub fn interface(self: *CountingDenseEmbedder) embedder_mod.DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

pub const GateDenseEmbedder = struct {
    allowed_successes: std.atomic.Value(usize) = .init(1),
    successful_requests: std.atomic.Value(usize) = .init(0),
    total_requests: std.atomic.Value(usize) = .init(0),
    rate_limited_requests: std.atomic.Value(usize) = .init(0),

    fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
        if (needle.len == 0) return true;
        if (needle.len > haystack.len) return false;
        var i: usize = 0;
        while (i + needle.len <= haystack.len) : (i += 1) {
            var matched = true;
            for (needle, 0..) |needle_ch, j| {
                if (std.ascii.toLower(haystack[i + j]) != needle_ch) {
                    matched = false;
                    break;
                }
            }
            if (matched) return true;
        }
        return false;
    }

    fn vectorForText(text: []const u8) [3]f32 {
        if (containsIgnoreCase(text, "alpha") or containsIgnoreCase(text, "concept")) {
            return .{ 1.0, 0.0, 0.0 };
        }
        if (containsIgnoreCase(text, "beta")) {
            return .{ 0.0, 1.0, 0.0 };
        }
        if (containsIgnoreCase(text, "retrieval") or containsIgnoreCase(text, "semantic")) {
            return .{ 0.8, 0.2, 0.0 };
        }
        return .{ 0.0, 0.0, 1.0 };
    }

    fn embedDense(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *GateDenseEmbedder = @ptrCast(@alignCast(ptr));
        _ = self.total_requests.fetchAdd(1, .monotonic);
        const previous_successes = self.successful_requests.fetchAdd(1, .acq_rel);
        if (previous_successes >= self.allowed_successes.load(.acquire)) {
            _ = self.successful_requests.fetchSub(1, .acq_rel);
            _ = self.rate_limited_requests.fetchAdd(1, .monotonic);
            return error.EmbedRateLimited;
        }
        if (dims != 3) return error.InvalidVectorDimensions;
        const vector = try alloc.alloc(f32, 3);
        const values = vectorForText(text);
        @memcpy(vector, &values);
        return vector;
    }

    pub fn interface(self: *GateDenseEmbedder) embedder_mod.DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }

    pub fn allowAll(self: *GateDenseEmbedder) void {
        self.allowed_successes.store(std.math.maxInt(usize), .release);
    }

    pub fn snapshot(self: *GateDenseEmbedder) struct {
        total_requests: usize,
        rate_limited_requests: usize,
        successful_requests: usize,
    } {
        return .{
            .total_requests = self.total_requests.load(.acquire),
            .rate_limited_requests = self.rate_limited_requests.load(.acquire),
            .successful_requests = self.successful_requests.load(.acquire),
        };
    }
};

pub const CountingSparseEmbedder = struct {
    deterministic: embedder_mod.DeterministicSparseEmbedder = .{},
    calls: usize = 0,

    fn embedSparse(ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8) !embedder_mod.SparseEmbedding {
        const self: *CountingSparseEmbedder = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return try embedder_mod.DeterministicSparseEmbedder.embedSparse(&self.deterministic, alloc, embedding_name, text);
    }

    pub fn interface(self: *CountingSparseEmbedder) embedder_mod.SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

pub const TestTransactionRecoveryResolver = struct {
    pub fn resolve(_: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: transactions_mod.TxnStatus, _: u64) !void {}
};

pub const TxnResolverRecorder = struct {
    mutex: std.atomic.Mutex = .unlocked,
    calls: u32 = 0,

    pub fn resolve(ctx_ptr: *anyopaque, txn_id: transactions_mod.TxnId, participant: []const u8, status: transactions_mod.TxnStatus, commit_version: u64) anyerror!void {
        _ = txn_id;
        _ = status;
        _ = commit_version;
        if (!std.mem.eql(u8, participant, "remote")) return error.UnexpectedParticipant;
        const self: *TxnResolverRecorder = @ptrCast(@alignCast(ctx_ptr));
        _ = platform.sync.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.calls += 1;
    }
};

/// Embedder that returns a fixed vector for any text, so a backfilled mention
/// embedding deterministically matches a candidate's name_embedding.
pub const FixedVectorEmbedder = struct {
    pub fn interface(self: *FixedVectorEmbedder) embedder_mod.DenseEmbedder {
        return .{ .ptr = self, .dense_embed_fn = embed };
    }

    fn embed(ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
        _ = ptr;
        _ = embedding_name;
        _ = text;
        _ = dims;
        const v = try alloc.alloc(f32, 4);
        v[0] = 1.0;
        v[1] = 0.0;
        v[2] = 0.0;
        v[3] = 0.0;
        return v;
    }
};

/// Thread-safe capturing entity sink for the promotion integration test.
pub const FakePromotionSink = struct {
    const Upsert = struct { table: []u8, key: []u8, doc: []u8 };

    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    upserts: std.ArrayListUnmanaged(Upsert) = .empty,

    pub fn deinit(self: *FakePromotionSink) void {
        for (self.upserts.items) |u| {
            self.alloc.free(u.table);
            self.alloc.free(u.key);
            self.alloc.free(u.doc);
        }
        self.upserts.deinit(self.alloc);
    }

    pub fn sink(self: *FakePromotionSink) promotion_runtime_mod.EntitySink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = promotion_runtime_mod.EntitySink.VTable{ .upsert = upsertFn };

    fn upsertFn(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        _ = allocator;
        const self: *FakePromotionSink = @ptrCast(@alignCast(ptr));
        _ = platform.sync.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const t = try self.alloc.dupe(u8, table);
        errdefer self.alloc.free(t);
        const k = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(k);
        const d = try self.alloc.dupe(u8, doc_json);
        errdefer self.alloc.free(d);
        try self.upserts.append(self.alloc, .{ .table = t, .key = k, .doc = d });
    }

    pub fn findKey(self: *FakePromotionSink, key: []const u8) ?[]const u8 {
        _ = platform.sync.lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.upserts.items) |u| {
            if (std.mem.eql(u8, u.key, key)) return u.doc;
        }
        return null;
    }
};

pub const TestAssetProducer = struct {
    calls: usize = 0,
    generator_calls: usize = 0,
    reader_calls: usize = 0,
    transcriber_calls: usize = 0,
    extractor_calls: usize = 0,
    reader_output: ?[]const u8 = null,
    transcriber_output: ?[]const u8 = null,
    extractor_output: ?[]const u8 = null,

    pub fn producer(self: *@This()) asset_producer_mod.Producer {
        return .{
            .ptr = self,
            .vtable = &.{ .produce = produce },
        };
    }

    fn produce(ptr: *anyopaque, alloc: Allocator, request: asset_producer_mod.Request) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        switch (request.producer_type) {
            .copy => {},
            .document_extraction => {},
            .generator => self.generator_calls += 1,
            .reader => self.reader_calls += 1,
            .transcriber => self.transcriber_calls += 1,
            .extractor => self.extractor_calls += 1,
        }
        if (request.producer_type == .extractor) {
            if (self.extractor_output) |output| return try alloc.dupe(u8, output);
            return try std.fmt.allocPrint(alloc, "{{\"relations\":[{{\"type\":\"mentions\",\"target\":{{\"document_id\":{f}}}}}]}}", .{std.json.fmt(request.source_text, .{})});
        }
        if (request.producer_type == .reader) {
            if (self.reader_output) |output| return try alloc.dupe(u8, output);
        }
        if (request.producer_type == .transcriber) {
            if (self.transcriber_output) |output| return try alloc.dupe(u8, output);
        }
        return try std.fmt.allocPrint(alloc, "{s}:{s}", .{ @tagName(request.producer_type), request.source_text });
    }
};

pub fn SharedReadLockHold(comptime DB: type) type {
    return struct {
        db: *DB,
        acquired: std.atomic.Value(u8) = .init(0),
        release: std.atomic.Value(u8) = .init(0),

        pub fn run(self: *@This()) void {
            self.db.core.lockApplyShared();
            self.acquired.store(1, .monotonic);
            while (self.release.load(.monotonic) == 0) {
                db_internal.spinOrYield();
            }
            self.db.core.unlockApplyShared();
        }
    };
}

pub fn ConcurrentReadProbe(comptime DB: type) type {
    return struct {
        db: *DB,
        started: std.atomic.Value(u8) = .init(0),
        done: std.atomic.Value(u8) = .init(0),
        failed: std.atomic.Value(u8) = .init(0),

        pub fn runSearch(self: *@This()) void {
            self.started.store(1, .monotonic);
            var result = self.db.search(std.heap.c_allocator, .{
                .index_name = "ft_v1",
                .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
            }) catch {
                self.failed.store(1, .monotonic);
                return;
            };
            defer result.deinit();
            self.done.store(1, .monotonic);
        }

        pub fn runScan(self: *@This()) void {
            self.started.store(1, .monotonic);
            var result = self.db.scan(std.heap.c_allocator, "", "", .{
                .include_documents = true,
                .limit = 10,
            }) catch {
                self.failed.store(1, .monotonic);
                return;
            };
            defer result.deinit(std.heap.c_allocator);
            self.done.store(1, .monotonic);
        }
    };
}

pub fn ConcurrentWriteProbe(comptime DB: type) type {
    return struct {
        db: *DB,
        started: std.atomic.Value(u8) = .init(0),
        done: std.atomic.Value(u8) = .init(0),
        failed: std.atomic.Value(u8) = .init(0),

        pub fn runBatch(self: *@This()) void {
            self.started.store(1, .monotonic);
            self.db.batch(.{
                .writes = &.{
                    .{ .key = "doc:b", .value = "{\"title\":\"bravo\"}" },
                },
            }) catch {
                self.failed.store(1, .monotonic);
                return;
            };
            self.done.store(1, .monotonic);
        }
    };
}
