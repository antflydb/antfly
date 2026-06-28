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
const table_catalog = @import("../table_catalog.zig");
const query_api = @import("../query.zig");
const document_sql_runtime = @import("document_sql_runtime.zig");
const sql_adapter_runtime = @import("../../sql/runtime.zig");
const raft_mod = @import("../../raft/mod.zig");
const core = @import("core.zig");
const cache = @import("cache.zig");
const remote_wire = @import("remote_wire.zig");

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

pub fn identityGenerationForAggregationFullResultRerun(
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
) !?u64 {
    if (!req.count_only and result.hits.len == result.total_hits) return req.identity_read_generation orelse result.identity_read_generation;
    if (result.total_hits == 0) return req.identity_read_generation orelse result.identity_read_generation;
    return req.identity_read_generation orelse result.identity_read_generation orelse error.UnsupportedQueryRequest;
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
    if (!req.include_stored) return false;
    if (req.limit == 0) return false;
    if (result.hits.len != result.total_hits) return false;
    // hits == total_hits but the page was filled to the limit: there may be more
    // matches the limit hid, so a full scan is still required.
    return result.hits.len < req.limit;
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
    try std.testing.expectError(error.UnsupportedQueryRequest, aggregationContextForDb(alloc, .{
        .identity_read_generation = current + 1,
    }, &db));
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

    try std.testing.expect(!aggregationFirstPassIsComplete(.{ .include_stored = false, .limit = 10 }, result));
    try std.testing.expect(!aggregationFirstPassIsComplete(.{ .include_stored = true, .limit = 1 }, result));
    result.total_hits = 1;
    try std.testing.expect(aggregationFirstPassIsComplete(.{ .include_stored = true, .limit = 10 }, result));
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

pub fn aggregateFromDbAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    req: document_sql_runtime.AlgebraicAggregateRequest,
) !document_sql_runtime.AlgebraicAggregateResponse {
    const entry = db.core.index_manager.algebraicIndex(req.index_name) orelse return error.DocumentSqlIndexUnavailable;

    if (req.group_by != null) {
        const entries = try entry.index.scanMaterializedExpressionEntriesForMaterialization(db.core.store, req.materialization_name);
        defer {
            for (entries) |*fold| fold.deinit(entry.index.alloc);
            entry.index.alloc.free(entries);
        }
        const output_count = if (req.limit) |limit| @min(entries.len, limit) else entries.len;
        const rows = try alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, output_count);
        errdefer alloc.free(rows);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |*row| row.deinit(alloc);
        }
        for (entries[0..output_count], 0..) |fold, i| {
            rows[i] = .{
                .group_json = try singleGroupJsonAlloc(alloc, &entry.index, fold.group_key),
                .value_json = try aggregateValueJsonAlloc(alloc, req.aggregate_op, fold.value),
                .raw_value = try alloc.dupe(u8, fold.value),
            };
            initialized += 1;
        }
        return .{
            .rows = rows,
            .total_groups = @intCast(entries.len),
        };
    }

    const empty_group = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{});
    defer alloc.free(empty_group);
    const raw = try entry.index.rawValueAlloc(db.core.store, req.materialization_name, empty_group);
    defer if (raw) |value| entry.index.alloc.free(value);
    const rows = try alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, 1);
    errdefer alloc.free(rows);
    rows[0] = .{
        .value_json = if (raw) |value|
            try aggregateValueJsonAlloc(alloc, req.aggregate_op, value)
        else
            try aggregateMissingValueJsonAlloc(alloc, req.aggregate_op),
        .raw_value = if (raw) |value| try alloc.dupe(u8, value) else null,
    };
    return .{
        .rows = rows,
        .total_groups = 1,
    };
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

    return try aggregateFromDbAlloc(alloc, &db, req);
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
    return try mergeResponsesAlloc(alloc, req, shard_responses.items);
}

fn singleGroupJsonAlloc(
    alloc: std.mem.Allocator,
    index: *const db_mod.algebraic.index.Index,
    group_key: []const u8,
) ![]u8 {
    const component = try db_mod.algebraic.token.componentAt(group_key, 0);
    if (component.next != group_key.len) return error.InvalidRowsRequest;
    return try index.scalarTokenJsonAlloc(alloc, component.payload);
}

fn aggregateMissingValueJsonAlloc(
    alloc: std.mem.Allocator,
    op: sql_adapter_runtime.DocumentAggregateOp,
) ![]u8 {
    return switch (op) {
        .count => try alloc.dupe(u8, "0"),
        .sum, .avg, .min, .max => try alloc.dupe(u8, "null"),
    };
}

fn aggregateValueJsonAlloc(
    alloc: std.mem.Allocator,
    op: sql_adapter_runtime.DocumentAggregateOp,
    raw: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    switch (op) {
        .count => try writer.print("{d}", .{try db_mod.algebraic.algebra.parseI64(raw)}),
        .sum, .min, .max => try writer.print("{d}", .{try db_mod.algebraic.algebra.parseF64(raw)}),
        .avg => {
            const avg = try db_mod.algebraic.algebra.parseAvg(raw);
            if (avg.count == 0) {
                try writer.writeAll("null");
            } else {
                try writer.print("{d}", .{avg.sum / @as(f64, @floatFromInt(avg.count))});
            }
        },
    }
    return try out.toOwnedSlice();
}

const MergedGroup = struct {
    group_json: ?[]u8 = null,
    raw_value: ?[]u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.group_json) |value| alloc.free(value);
        if (self.raw_value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn mergeResponsesAlloc(
    alloc: std.mem.Allocator,
    req: document_sql_runtime.AlgebraicAggregateRequest,
    responses: []const document_sql_runtime.AlgebraicAggregateResponse,
) !document_sql_runtime.AlgebraicAggregateResponse {
    var groups = std.ArrayListUnmanaged(MergedGroup).empty;
    defer {
        for (groups.items) |*group| group.deinit(alloc);
        groups.deinit(alloc);
    }

    for (responses) |response| {
        for (response.rows) |row| {
            if (row.raw_value == null and !missingRawValueAllowed(req, row.value_json)) {
                return error.UnsupportedQueryRequest;
            }
            try mergeRowAlloc(alloc, &groups, req.aggregate_op, row.group_json, row.raw_value);
        }
    }

    const output_count = if (req.limit) |limit| @min(groups.items.len, limit) else groups.items.len;
    const rows = try alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, output_count);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |*row| row.deinit(alloc);
    }

    for (groups.items[0..output_count], 0..) |group, i| {
        rows[i] = .{
            .group_json = if (group.group_json) |value| try alloc.dupe(u8, value) else null,
            .value_json = if (group.raw_value) |value|
                try aggregateValueJsonAlloc(alloc, req.aggregate_op, value)
            else
                try aggregateMissingValueJsonAlloc(alloc, req.aggregate_op),
            .raw_value = if (group.raw_value) |value| try alloc.dupe(u8, value) else null,
        };
        initialized += 1;
    }

    return .{
        .rows = rows,
        .total_groups = @intCast(groups.items.len),
    };
}

fn missingRawValueAllowed(req: document_sql_runtime.AlgebraicAggregateRequest, value_json: []const u8) bool {
    if (req.group_by != null) return false;
    return switch (req.aggregate_op) {
        .count => std.mem.eql(u8, value_json, "0"),
        .sum, .avg, .min, .max => std.mem.eql(u8, value_json, "null"),
    };
}

fn mergeRowAlloc(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(MergedGroup),
    op: sql_adapter_runtime.DocumentAggregateOp,
    group_json: ?[]const u8,
    raw_value: ?[]const u8,
) !void {
    for (groups.items) |*group| {
        if (optionalStringsEqual(group.group_json, group_json)) {
            if (raw_value) |right| {
                if (group.raw_value) |left| {
                    const merged = try mergeRawValueAlloc(alloc, op, left, right);
                    alloc.free(left);
                    group.raw_value = merged;
                } else {
                    group.raw_value = try alloc.dupe(u8, right);
                }
            } else if (op == .count and group.raw_value == null) {
                group.raw_value = try db_mod.algebraic.algebra.encodeI64Alloc(alloc, 0);
            }
            return;
        }
    }

    try groups.append(alloc, .{
        .group_json = if (group_json) |value| try alloc.dupe(u8, value) else null,
        .raw_value = if (raw_value) |value| try alloc.dupe(u8, value) else if (op == .count)
            try db_mod.algebraic.algebra.encodeI64Alloc(alloc, 0)
        else
            null,
    });
}

fn optionalStringsEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return std.mem.eql(u8, left.?, right.?);
}

fn mergeRawValueAlloc(
    alloc: std.mem.Allocator,
    op: sql_adapter_runtime.DocumentAggregateOp,
    left: []const u8,
    right: []const u8,
) ![]u8 {
    return switch (op) {
        .count => try db_mod.algebraic.algebra.encodeI64Alloc(
            alloc,
            try db_mod.algebraic.algebra.parseI64(left) + try db_mod.algebraic.algebra.parseI64(right),
        ),
        .sum => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            try db_mod.algebraic.algebra.parseF64(left) + try db_mod.algebraic.algebra.parseF64(right),
        ),
        .min => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            @min(try db_mod.algebraic.algebra.parseF64(left), try db_mod.algebraic.algebra.parseF64(right)),
        ),
        .max => try db_mod.algebraic.algebra.encodeF64Alloc(
            alloc,
            @max(try db_mod.algebraic.algebra.parseF64(left), try db_mod.algebraic.algebra.parseF64(right)),
        ),
        .avg => blk: {
            const left_avg = try db_mod.algebraic.algebra.parseAvg(left);
            const right_avg = try db_mod.algebraic.algebra.parseAvg(right);
            break :blk try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{
                .sum = left_avg.sum + right_avg.sum,
                .count = left_avg.count + right_avg.count,
            });
        },
    };
}

test "document algebraic aggregate fan-in merges raw grouped averages before applying limit" {
    const alloc = std.testing.allocator;
    const left_raw = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 10, .count = 1 });
    defer alloc.free(left_raw);
    const right_raw = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 12, .count = 1 });
    defer alloc.free(right_raw);
    var left_rows = [_]document_sql_runtime.AlgebraicAggregateRow{.{
        .group_json = try alloc.dupe(u8, "\"active\""),
        .value_json = try alloc.dupe(u8, "10"),
        .raw_value = try alloc.dupe(u8, left_raw),
    }};
    defer left_rows[0].deinit(alloc);
    var right_rows = [_]document_sql_runtime.AlgebraicAggregateRow{
        .{
            .group_json = try alloc.dupe(u8, "\"active\""),
            .value_json = try alloc.dupe(u8, "12"),
            .raw_value = try alloc.dupe(u8, right_raw),
        },
        .{
            .group_json = try alloc.dupe(u8, "\"archived\""),
            .value_json = try alloc.dupe(u8, "30"),
            .raw_value = try db_mod.algebraic.algebra.encodeAvgAlloc(alloc, .{ .sum = 30, .count = 1 }),
        },
    };
    defer {
        right_rows[0].deinit(alloc);
        right_rows[1].deinit(alloc);
    }
    const responses = [_]document_sql_runtime.AlgebraicAggregateResponse{
        .{ .rows = left_rows[0..], .total_groups = 1 },
        .{ .rows = right_rows[0..], .total_groups = 2 },
    };
    var merged = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "avg_by_status",
        .aggregate_op = .avg,
        .group_by = .{ .field = "status", .source_field = "status", .field_type = .keyword, .output = "status" },
        .limit = 1,
    }, responses[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), merged.total_groups);
    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings("\"active\"", merged.rows[0].group_json.?);
    try std.testing.expectEqualStrings("11", merged.rows[0].value_json);
}

test "document algebraic aggregate fan-in preserves empty scalar aggregate semantics" {
    const alloc = std.testing.allocator;
    var empty_sum_rows = [_]document_sql_runtime.AlgebraicAggregateRow{
        .{ .value_json = try alloc.dupe(u8, "null"), .raw_value = null },
        .{ .value_json = try alloc.dupe(u8, "null"), .raw_value = null },
    };
    defer {
        empty_sum_rows[0].deinit(alloc);
        empty_sum_rows[1].deinit(alloc);
    }
    const empty_sum_responses = [_]document_sql_runtime.AlgebraicAggregateResponse{
        .{ .rows = empty_sum_rows[0..1], .total_groups = 1 },
        .{ .rows = empty_sum_rows[1..2], .total_groups = 1 },
    };
    var merged_empty_sum = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "sum_all",
        .aggregate_op = .sum,
        .group_by = null,
        .limit = null,
    }, empty_sum_responses[0..]);
    defer merged_empty_sum.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged_empty_sum.rows.len);
    try std.testing.expectEqualStrings("null", merged_empty_sum.rows[0].value_json);
    try std.testing.expect(merged_empty_sum.rows[0].raw_value == null);

    var empty_count_rows = [_]document_sql_runtime.AlgebraicAggregateRow{
        .{ .value_json = try alloc.dupe(u8, "0"), .raw_value = null },
        .{ .value_json = try alloc.dupe(u8, "0"), .raw_value = null },
    };
    defer {
        empty_count_rows[0].deinit(alloc);
        empty_count_rows[1].deinit(alloc);
    }
    const empty_count_responses = [_]document_sql_runtime.AlgebraicAggregateResponse{
        .{ .rows = empty_count_rows[0..1], .total_groups = 1 },
        .{ .rows = empty_count_rows[1..2], .total_groups = 1 },
    };
    var merged_empty_count = try mergeResponsesAlloc(alloc, .{
        .index_name = "amount_alg",
        .materialization_name = "count_all",
        .aggregate_op = .count,
        .group_by = null,
        .limit = null,
    }, empty_count_responses[0..]);
    defer merged_empty_count.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged_empty_count.rows.len);
    try std.testing.expectEqualStrings("0", merged_empty_count.rows[0].value_json);
}
