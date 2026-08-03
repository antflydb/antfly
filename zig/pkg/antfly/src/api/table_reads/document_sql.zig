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

const db_mod = @import("../../storage/db/mod.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const catalog_resources = @import("../catalog_resources.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const query_api = @import("../query.zig");
const document_sql_runtime = @import("../../sql/document_runtime.zig");
const sql_document = @import("../../sql/document.zig");
const raft_mod = @import("../../raft/mod.zig");
const core = @import("core.zig");
const cache = @import("cache.zig");
const remote_wire = @import("remote_wire.zig");
const platform_time = @import("antfly_platform").time;

pub const RuntimeSourceAdapter = struct {
    source: core.TableReadSource,
    target: catalog_resources.TableTarget,
    native_table_name: []const u8,
    public_table_name: []const u8,

    pub fn runtimeSource(self: *@This()) document_sql_runtime.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .lookup_catalog = lookupCatalog,
                .scan = scan,
                .scan_catalog = scanCatalog,
                .query = query,
                .query_catalog = queryCatalog,
                .algebraic_aggregate = algebraicAggregate,
                .algebraic_aggregate_catalog = algebraicAggregateCatalog,
            },
            .native_table_name = self.native_table_name,
            .public_table_name = self.public_table_name,
        };
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: document_sql_runtime.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.lookup(alloc, table_name, key, dbLookupOptions(opts), consistency);
    }

    fn lookupCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        key: []const u8,
        opts: document_sql_runtime.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.lookupCatalog(alloc, self.target, key, dbLookupOptions(opts), consistency);
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: document_sql_runtime.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.scan(alloc, table_name, from_key, to_key, dbScanOptions(opts), consistency);
    }

    fn scanCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        from_key: []const u8,
        to_key: []const u8,
        opts: document_sql_runtime.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.ScanResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.scanCatalog(alloc, self.target, from_key, to_key, dbScanOptions(opts), consistency);
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.QueryRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var owned_req = try parseRuntimeQueryRequestAlloc(alloc, table_name, req);
        defer owned_req.deinit(alloc);
        var result = (try self.source.query(alloc, table_name, owned_req.req, consistency)) orelse return null;
        const json = result.json;
        result = undefined;
        return .{ .json = json };
    }

    fn queryCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        req: document_sql_runtime.QueryRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.QueryResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var owned_req = try parseRuntimeQueryRequestAlloc(alloc, self.native_table_name, req);
        defer owned_req.deinit(alloc);
        var result = (try self.source.queryCatalog(alloc, self.target, owned_req.req, consistency)) orelse return null;
        const json = result.json;
        result = undefined;
        return .{ .json = json };
    }

    fn algebraicAggregate(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.documentAlgebraicAggregate(alloc, table_name, req, consistency);
    }

    fn algebraicAggregateCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.source.documentAlgebraicAggregateCatalog(alloc, self.target, req, consistency);
    }

    fn dbLookupOptions(_: document_sql_runtime.LookupOptions) db_mod.types.LookupOptions {
        return .{};
    }

    fn dbScanOptions(opts: document_sql_runtime.ScanOptions) db_mod.types.ScanOptions {
        return .{
            .include_documents = opts.include_documents,
            .include_all_fields = opts.include_all_fields,
            .limit = opts.limit,
        };
    }

    fn parseRuntimeQueryRequestAlloc(
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.QueryRequest,
    ) !query_api.OwnedQueryRequest {
        var owned = try query_api.parseQueryRequest(alloc, null, table_name, req.body_json);
        errdefer owned.deinit(alloc);
        if (req.index_name) |index_name| {
            if (owned.req.primary_text_index_name == null) {
                owned.req.primary_text_index_name = try alloc.dupe(u8, index_name);
            }
            if (owned.req.index_name == null) {
                owned.req.index_name = try alloc.dupe(u8, index_name);
            }
        }
        return owned;
    }
};

pub fn aggregationContextForDb(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    db: *db_mod.DB,
) !db_mod.aggregations.Context {
    const identity_read_generation = try currentIdentityReadGenerationForDb(req.identity_read_generation, db);
    return .{
        .index_manager = db.core.index_manager,
        .doc_store = db.core.store,
        .full_text_index_name = req.index_name,
        .algebraic_index_name = req.index_name,
        .algebraic_available = try algebraicIndexFreshEnoughForRequest(alloc, req, db),
        .runtime_schema = db.core.schema,
        .identity_read_generation = identity_read_generation,
    };
}

pub fn aggregationContextForCapturedResultDb(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    db: *db_mod.DB,
) !db_mod.aggregations.Context {
    const identity_read_generation = currentIdentityReadGenerationForDb(req.identity_read_generation, db) catch |err| switch (err) {
        // A complete result is already an immutable, exact aggregation input.
        // If background derived work advanced the DB after that search, do not
        // mix the captured rows with acceleration state from a newer identity
        // generation. Stored-row aggregations can still execute exactly; any
        // aggregation that requires index state remains unsupported and can be
        // retried by the caller against one coherent generation.
        error.IdentityReadGenerationChanged => return .{
            .identity_read_generation = req.identity_read_generation,
        },
        else => return err,
    };
    return .{
        .index_manager = db.core.index_manager,
        .doc_store = db.core.store,
        .full_text_index_name = req.index_name,
        .algebraic_index_name = req.index_name,
        .algebraic_available = try algebraicIndexFreshEnoughForRequest(alloc, req, db),
        .identity_read_generation = identity_read_generation,
    };
}

pub fn currentIdentityReadGenerationForDb(requested: ?u64, db: *db_mod.DB) !u64 {
    return try db.currentIdentityReadGenerationForRequest(requested);
}

pub fn algebraicIndexFreshEnoughForRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    db: *db_mod.DB,
) !bool {
    return try algebraicIndexFreshEnoughForName(alloc, req.index_name, db);
}

pub fn algebraicIndexFreshEnoughForName(
    alloc: std.mem.Allocator,
    index_name_opt: ?[]const u8,
    db: *db_mod.DB,
) !bool {
    // The request's index_name selects the text index; the algebraic index used
    // for aggregation pushdown is resolved independently (an explicit algebraic
    // index name, else the table's default algebraic index).
    const entry = db.core.index_manager.aggregationAlgebraicIndex(index_name_opt) orelse return false;
    if (entry.index.hasErrors()) return false;
    const target_sequence = db.core.nextDerivedSequence();
    var applied_sequence = try db.core.loadAppliedSequence(alloc, entry.config.name);
    if (db.executor.appliedSequence(entry.config.name)) |live_applied| {
        applied_sequence = @max(applied_sequence, live_applied);
    }
    return applied_sequence >= target_sequence;
}

pub fn canConsiderAlgebraicAggregations(req: db_mod.types.SearchRequest) bool {
    return req.full_text == null and
        req.filter_text == null and
        req.exclusion_text == null and
        req.exclusion_query_json.len == 0 and
        req.full_text_queries.len == 0 and
        req.dense == null and
        req.sparse == null and
        req.dense_queries.len == 0 and
        req.sparse_queries.len == 0 and
        req.graph_queries.len == 0 and
        req.merge_config == null and
        req.reranker == null and
        req.pruner == null and
        req.filter_prefix.len == 0 and
        req.filter_ids.len == 0 and
        req.exclude_ids.len == 0 and
        req.filter_doc_ids.len == 0 and
        !req.filter_doc_ids_positive and
        req.exclude_doc_ids.len == 0 and
        !remote_wire.searchRequestHasResolvedDocFilter(req) and
        req.distance_over == null and
        req.distance_under == null;
}

pub fn requestWithResultIdentityGeneration(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) db_mod.types.SearchRequest {
    var out = req;
    if (out.identity_read_generation == null) out.identity_read_generation = result.identity_read_generation;
    return out;
}

pub const default_aggregation_full_result_budget: u32 = 100_000;

pub fn aggregationFullResultBudgetFromRaw(raw: ?[*:0]u8) u32 {
    const value = raw orelse return default_aggregation_full_result_budget;
    const slice = std.mem.span(value);
    if (slice.len == 0) return default_aggregation_full_result_budget;
    const parsed = std.fmt.parseUnsigned(u32, slice, 10) catch return default_aggregation_full_result_budget;
    if (parsed == 0) return default_aggregation_full_result_budget;
    return @min(parsed, @as(u32, @intCast(db_mod.aggregations.max_aggregation_source_hits)));
}

pub fn aggregationFullResultBudget() u32 {
    return aggregationFullResultBudgetFromRaw(std.c.getenv("ANTFLY_AGGREGATION_FULL_RESULT_BUDGET\x00"));
}

pub fn checkQueryDeadline(req: db_mod.types.SearchRequest) !void {
    const deadline_ns = req.execution_deadline_ns orelse return;
    if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
}

pub fn identityGenerationForAggregationFullResultRerun(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) !?u64 {
    if (aggregationCanUseCurrentResult(req, result)) return req.identity_read_generation orelse result.identity_read_generation;
    return req.identity_read_generation orelse result.identity_read_generation orelse error.UnsupportedQueryRequest;
}

pub fn aggregationCanUseCurrentResult(req: db_mod.types.SearchRequest, result: db_mod.types.SearchResult) bool {
    if (result.total_hits_relation != .exact) return false;
    if (result.total_hits == 0) return true;
    return !req.count_only and result.hits.len == result.total_hits;
}

/// True when the first-pass search already materialized every matching document,
/// so its hits can be aggregated directly without a full-scan re-fetch. This
/// only holds when the request did not bound the result below the match count:
/// the page is complete (hits == total_hits) AND the caller asked for at least
/// as many hits as matched (the limit did not truncate). The hits must also
/// include stored data because scan-based aggregations read hit `stored_data`.
/// A `limit` of 0 (aggregation-only) never qualifies -- total_hits is then a
/// truncated 0.
pub fn aggregationFirstPassIsComplete(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) bool {
    if (result.total_hits_relation != .exact) return false;
    if (!req.include_stored) return false;
    if (req.limit == 0) return false;
    if (result.hits.len != result.total_hits) return false;
    // hits == total_hits but the page was filled to the limit: there may be more
    // matches the limit hid, so a full scan is still required.
    return result.hits.len < req.limit;
}

pub fn aggregationFullResultLimit(req: db_mod.types.SearchRequest, result: db_mod.types.SearchResult, operation: []const u8) !u32 {
    try checkQueryDeadline(req);
    const budget = aggregationFullResultBudget();
    // An inexact first page is a lower bound, not a safe allocation size.
    // Rerun up to the configured budget and require that execution to prove
    // completeness before computing aggregations.
    if (result.total_hits_relation != .exact) return budget;
    if (result.total_hits > budget) {
        std.log.warn("query aggregation full-result rerun budget exceeded operation={s} total_hits={d} budget={d}", .{
            operation,
            result.total_hits,
            budget,
        });
        return error.QueryCandidateBudgetExceeded;
    }
    return result.total_hits;
}

pub fn requireCompleteAggregationFullResult(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    operation: []const u8,
) !void {
    try checkQueryDeadline(req);
    if (aggregationCanUseCurrentResult(req, result)) return;
    std.log.warn("query aggregation bounded full-result rerun remained incomplete operation={s} relation={s} total_hits={d} returned_hits={d} budget={d}", .{
        operation,
        @tagName(result.total_hits_relation),
        result.total_hits,
        result.hits.len,
        aggregationFullResultBudget(),
    });
    if (result.total_hits_relation == .gte or result.total_hits >= aggregationFullResultBudget()) {
        return error.QueryCandidateBudgetExceeded;
    }
    return error.UnsupportedQueryRequest;
}

pub fn aggregationFullResultRequest(req: db_mod.types.SearchRequest, result: db_mod.types.SearchResult, operation: []const u8) !db_mod.types.SearchRequest {
    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result);
    const full_limit = try aggregationFullResultLimit(req, result, operation);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = full_limit;
    full_req.include_stored = true;
    full_req.count_only = false;
    return full_req;
}

/// Limit to use when re-fetching all matching documents for scan-based
/// aggregation. total_hits is unreliable (truncated to the page), so bound the
/// re-fetch by the shard's primary document count, which is an exact upper bound
/// on the number of matches. Falls back to the observed total_hits if the count
/// is unavailable.
pub fn aggregationFullScanLimit(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    result: db_mod.types.SearchResult,
) !u32 {
    const doc_count = db.primaryDocCount(alloc) catch return @max(result.total_hits, 1);
    const capped = std.math.cast(u32, doc_count) orelse std.math.maxInt(u32);
    return @max(capped, result.total_hits);
}

/// Re-fetch limit for scan-based aggregation across multiple groups, where a
/// single primary doc count is not available. An unbounded limit makes every
/// shard return all of its matching documents so the merge aggregates over the
/// full match set.
pub fn aggregationDistributedFullScanLimit(result: db_mod.types.SearchResult) u32 {
    return @max(result.total_hits, std.math.maxInt(u32));
}

test "aggregation context rejects non-current identity generation" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/aggregation-context-identity-generation", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"v\":1}" }},
        .sync_level = .write,
    });

    const current = db.core.nextDerivedSequence();
    const ctx = try aggregationContextForDb(alloc, .{ .identity_read_generation = current }, &db);
    try std.testing.expectEqual(@as(?u64, current), ctx.identity_read_generation);
    try std.testing.expectError(error.IdentityReadGenerationChanged, aggregationContextForDb(alloc, .{
        .identity_read_generation = current + 1,
    }, &db));

    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"v\":2}" }},
        .sync_level = .write,
    });
    const captured = try aggregationContextForCapturedResultDb(alloc, .{
        .identity_read_generation = current,
    }, &db);
    try std.testing.expectEqual(@as(?u64, current), captured.identity_read_generation);
    try std.testing.expect(captured.index_manager == null);
    try std.testing.expect(captured.doc_store == null);
    try std.testing.expect(!captured.algebraic_available);
}

test "aggregation completeness requires exact total relation" {
    const req = db_mod.types.SearchRequest{};
    try std.testing.expect(aggregationCanUseCurrentResult(req, .{
        .alloc = std.testing.allocator,
        .hits = &.{},
        .total_hits = 0,
        .total_hits_relation = .exact,
    }));
    try std.testing.expect(!aggregationCanUseCurrentResult(req, .{
        .alloc = std.testing.allocator,
        .hits = &.{},
        .total_hits = 0,
        .total_hits_relation = .gte,
    }));

    const hits = [_]db_mod.types.SearchHit{.{ .id = @constCast("doc:a") }};
    try std.testing.expect(aggregationCanUseCurrentResult(req, .{
        .alloc = std.testing.allocator,
        .hits = @constCast(hits[0..]),
        .total_hits = 1,
        .total_hits_relation = .exact,
    }));
    try std.testing.expect(!aggregationCanUseCurrentResult(req, .{
        .alloc = std.testing.allocator,
        .hits = @constCast(hits[0..]),
        .total_hits = 1,
        .total_hits_relation = .gte,
    }));
    try std.testing.expectEqual(aggregationFullResultBudget(), try aggregationFullResultLimit(.{}, .{
        .alloc = std.testing.allocator,
        .hits = @constCast(hits[0..]),
        .total_hits = 1,
        .total_hits_relation = .gte,
    }, "test"));
}

test "aggregation full-result rerun can reuse snapped result identity generation" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a") };
    var result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    };
    defer result.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, identityGenerationForAggregationFullResultRerun(.{}, result));
    try std.testing.expectEqual(@as(?u64, 9), try identityGenerationForAggregationFullResultRerun(.{ .identity_read_generation = 9 }, result));
    result.identity_read_generation = 11;
    try std.testing.expectEqual(@as(?u64, 11), try identityGenerationForAggregationFullResultRerun(.{}, result));
    try std.testing.expectEqual(@as(?u64, 9), try identityGenerationForAggregationFullResultRerun(.{ .identity_read_generation = 9 }, result));
    try std.testing.expectEqual(@as(?u64, 11), requestWithResultIdentityGeneration(.{}, result).identity_read_generation);
    try std.testing.expectEqual(@as(?u64, 9), requestWithResultIdentityGeneration(.{ .identity_read_generation = 9 }, result).identity_read_generation);

    const complete = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
    };
    try std.testing.expectEqual(@as(?u64, null), try identityGenerationForAggregationFullResultRerun(.{}, complete));
    try std.testing.expect(aggregationCanUseCurrentResult(.{}, complete));

    try std.testing.expect(!aggregationFirstPassIsComplete(.{ .include_stored = false, .limit = 10 }, result));
    try std.testing.expect(!aggregationFirstPassIsComplete(.{ .include_stored = true, .limit = 1 }, result));
    result.total_hits = 1;
    try std.testing.expect(aggregationFirstPassIsComplete(.{ .include_stored = true, .limit = 10 }, result));
    try std.testing.expect(aggregationCanUseCurrentResult(.{}, result));
    result.total_hits_relation = .gte;
    try std.testing.expect(!aggregationFirstPassIsComplete(.{ .include_stored = true, .limit = 10 }, result));
    try std.testing.expect(!aggregationCanUseCurrentResult(.{}, result));
    try std.testing.expectEqual(aggregationFullResultBudget(), try aggregationFullResultLimit(.{}, result, "test"));

    result.total_hits_relation = .exact;
    result.total_hits = default_aggregation_full_result_budget + 1;
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, aggregationFullResultLimit(.{}, result, "test"));

    result.total_hits = 1;
    const full_req = try aggregationFullResultRequest(.{
        .offset = 10,
        .limit = 5,
        .count_only = true,
        .include_stored = false,
    }, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 1,
        .total_hits_relation = .exact,
        .identity_read_generation = 99,
    }, "test");
    try std.testing.expectEqual(@as(u32, 0), full_req.offset);
    try std.testing.expectEqual(@as(u32, 1), full_req.limit);
    try std.testing.expect(!full_req.count_only);
    try std.testing.expect(full_req.include_stored);
    try std.testing.expectEqual(@as(?u64, 99), full_req.identity_read_generation);
}

test "aggregation full-scan limit uses primary count and distributed fallback is unbounded" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/aggregation-full-scan-limit", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"v\":1}" },
            .{ .key = "doc:b", .value = "{\"v\":2}" },
        },
        .sync_level = .write,
    });

    const result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 1,
    };
    try std.testing.expectEqual(@as(u32, 2), try aggregationFullScanLimit(alloc, &db, result));
    try std.testing.expectEqual(std.math.maxInt(u32), aggregationDistributedFullScanLimit(result));
}

pub fn aggregateProvisionedHostedLocal(
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: document_sql_runtime.AlgebraicAggregateRequest,
    consistency: raft_mod.ReadConsistency,
) !?document_sql_runtime.AlgebraicAggregateResponse {
    return aggregateLocal(
        replica_root_dir,
        catalog,
        requester,
        alloc,
        group_id,
        lsm_root_generation,
        backend_runtime,
        table_name,
        req,
        consistency,
    ) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try aggregateLocal(
            replica_root_dir,
            catalog,
            requester,
            alloc,
            group_id,
            lsm_root_generation,
            backend_runtime,
            table_name,
            req,
            .stale,
        ),
        else => err,
    };
}

fn aggregateLocal(
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: document_sql_runtime.AlgebraicAggregateRequest,
    consistency: raft_mod.ReadConsistency,
) !document_sql_runtime.AlgebraicAggregateResponse {
    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareScanWithConsistency(group_id, "", "", .{}, consistency);

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try cache.openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();

    return try sql_document.aggregateFromDbAlloc(alloc, &db, req);
}

pub fn aggregateProvisionedGroupsAlloc(
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    group_visible_root_generation: ?core.GroupVisibleRootGenerationSource,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: document_sql_runtime.AlgebraicAggregateRequest,
    consistency: raft_mod.ReadConsistency,
) !?document_sql_runtime.AlgebraicAggregateResponse {
    var shard_responses = std.ArrayListUnmanaged(document_sql_runtime.AlgebraicAggregateResponse).empty;
    defer {
        for (shard_responses.items) |*response| response.deinit(alloc);
        shard_responses.deinit(alloc);
    }

    var local_req = req;
    if (req.group_by != null) local_req.limit = null;

    for (group_ids) |group_id| {
        const root_generation = if (group_visible_root_generation) |source|
            source.visibleRootGenerationForGroup(group_id)
        else
            core.backend_current_root_generation;
        var response = (try aggregateProvisionedHostedLocal(
            replica_root_dir,
            catalog,
            requester,
            alloc,
            group_id,
            root_generation,
            backend_runtime,
            table_name,
            local_req,
            consistency,
        )) orelse return error.DocumentSqlIndexUnavailable;
        errdefer response.deinit(alloc);
        try shard_responses.append(alloc, response);
    }
    if (shard_responses.items.len == 0) return null;
    return try sql_document.mergeResponsesAlloc(alloc, req, shard_responses.items);
}
