// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

const db_mod = @import("../storage/db/mod.zig");
const relational_rows = @import("../storage/db/relational_rows.zig");
const raft_mod = @import("../raft/mod.zig");
const json_helpers = @import("../common/json_helpers.zig");
const regex_mod = @import("../search/regex.zig");
const document_sql_corpus = @import("document_sql_corpus.zig");
const document_write = @import("document_write.zig");
const expr_text = @import("expr/text.zig");
const sql_adapter = @import("document_plan.zig");
const sql_plan = @import("plan.zig");
const source_binding = @import("source_binding.zig");
const storage_schema = @import("../storage/schema.zig");
const tokenized = @import("tokenized.zig");

pub const LookupOptions = struct {};

pub const ScanOptions = struct {
    include_documents: bool = false,
    include_all_fields: bool = false,
    limit: u32 = 0,
};

pub const LookupResponse = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *LookupResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const ScanResponse = struct {
    ndjson: []u8,

    pub fn deinit(self: *ScanResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.ndjson);
        self.* = undefined;
    }
};

pub const QueryRequest = struct {
    body_json: []const u8,
    index_name: ?[]const u8 = null,
};

pub const OwnedQueryRequest = struct {
    body_json: []u8,
    index_name: ?[]const u8 = null,

    pub fn request(self: *const OwnedQueryRequest) QueryRequest {
        return .{
            .body_json = self.body_json,
            .index_name = self.index_name,
        };
    }

    pub fn deinit(self: *OwnedQueryRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.body_json);
        self.* = undefined;
    }
};

pub const QueryResponse = struct {
    json: []u8,

    pub fn deinit(self: *QueryResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const AlgebraicAggregateRequest = struct {
    index_name: []const u8,
    materialization_name: []const u8,
    aggregate_op: sql_adapter.DocumentAggregateOp,
    group_by: ?sql_adapter.DocumentAggregateGroupBy = null,
    limit: ?u32 = null,
};

pub const AlgebraicAggregateRow = struct {
    group_json: ?[]u8 = null,
    value_json: []u8,
    raw_value: ?[]u8 = null,

    pub fn deinit(self: *AlgebraicAggregateRow, alloc: std.mem.Allocator) void {
        if (self.group_json) |value| alloc.free(value);
        alloc.free(self.value_json);
        if (self.raw_value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const AlgebraicAggregateResponse = struct {
    rows: []AlgebraicAggregateRow,
    total_groups: u32,

    pub fn deinit(self: *AlgebraicAggregateResponse, alloc: std.mem.Allocator) void {
        for (self.rows) |*row| row.deinit(alloc);
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const RowsQueryResult = struct {
    rows: [][]const u8,
    total: u32,

    pub fn deinit(self: *RowsQueryResult, alloc: std.mem.Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const RowsAggregateResult = struct {
    rows: [][]const u8,
    total_groups: u32,

    pub fn deinit(self: *RowsAggregateResult, alloc: std.mem.Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    native_table_name: []const u8,
    public_table_name: []const u8,

    pub const VTable = struct {
        lookup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse,
        lookup_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse = null,
        scan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse,
        scan_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse = null,
        query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?QueryResponse,
        query_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?QueryResponse = null,
        algebraic_aggregate: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?AlgebraicAggregateResponse = null,
        algebraic_aggregate_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?AlgebraicAggregateResponse = null,
    };

    pub fn lookup(self: Source, alloc: std.mem.Allocator, table_name: []const u8, key: []const u8, opts: LookupOptions, consistency: raft_mod.ReadConsistency) !?LookupResponse {
        return try self.vtable.lookup(self.ptr, alloc, table_name, key, opts, consistency);
    }

    pub fn lookupCatalog(self: Source, alloc: std.mem.Allocator, key: []const u8, opts: LookupOptions, consistency: raft_mod.ReadConsistency) !?LookupResponse {
        const callback = self.vtable.lookup_catalog orelse return error.UnsupportedOperation;
        return try callback(self.ptr, alloc, key, opts, consistency);
    }

    pub fn scan(self: Source, alloc: std.mem.Allocator, table_name: []const u8, from_key: []const u8, to_key: []const u8, opts: ScanOptions, consistency: raft_mod.ReadConsistency) !?ScanResponse {
        return try self.vtable.scan(self.ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    pub fn scanCatalog(self: Source, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: ScanOptions, consistency: raft_mod.ReadConsistency) !?ScanResponse {
        const callback = self.vtable.scan_catalog orelse return error.UnsupportedOperation;
        return try callback(self.ptr, alloc, from_key, to_key, opts, consistency);
    }

    pub fn query(self: Source, alloc: std.mem.Allocator, table_name: []const u8, req: QueryRequest, consistency: raft_mod.ReadConsistency) !?QueryResponse {
        return try self.vtable.query(self.ptr, alloc, table_name, req, consistency);
    }

    pub fn queryCatalog(self: Source, alloc: std.mem.Allocator, req: QueryRequest, consistency: raft_mod.ReadConsistency) !?QueryResponse {
        const callback = self.vtable.query_catalog orelse return error.UnsupportedOperation;
        return try callback(self.ptr, alloc, req, consistency);
    }

    pub fn algebraicAggregate(self: Source, alloc: std.mem.Allocator, table_name: []const u8, req: AlgebraicAggregateRequest, consistency: raft_mod.ReadConsistency) !?AlgebraicAggregateResponse {
        const callback = self.vtable.algebraic_aggregate orelse return error.DocumentSqlIndexUnavailable;
        return try callback(self.ptr, alloc, table_name, req, consistency);
    }

    pub fn algebraicAggregateCatalog(self: Source, alloc: std.mem.Allocator, req: AlgebraicAggregateRequest, consistency: raft_mod.ReadConsistency) !?AlgebraicAggregateResponse {
        const callback = self.vtable.algebraic_aggregate_catalog orelse return error.UnsupportedOperation;
        return try callback(self.ptr, alloc, req, consistency);
    }
};

pub const BatchSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        batch: *const fn (ptr: *anyopaque, req: db_mod.types.BatchRequest) anyerror!void,
    };

    pub fn batch(self: BatchSink, req: db_mod.types.BatchRequest) !void {
        return try self.vtable.batch(self.ptr, req);
    }
};

pub fn dbBatchSink(db: *db_mod.DB) BatchSink {
    return .{
        .ptr = db,
        .vtable = &.{
            .batch = dbBatchSinkBatch,
        },
    };
}

fn dbBatchSinkBatch(ptr: *anyopaque, req: db_mod.types.BatchRequest) !void {
    const db: *db_mod.DB = @ptrCast(@alignCast(ptr));
    try db.batch(req);
}

fn appendJsonFieldName(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: []const u8) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

fn appendJsonFieldU32(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: u32) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

fn appendJsonFieldBool(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: bool) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

pub fn executeReadPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_adapter.DocumentReadPlan,
    consistency: raft_mod.ReadConsistency,
) !?RowsQueryResult {
    const native_table_name = source.native_table_name;
    const public_table_name = source.public_table_name;

    if (lowered.unnest) |unnest| {
        if (lowered.order_by) |order_by| {
            return try executeOrderedLoweredDocumentSqlUnnestReadPlanAlloc(alloc, source, native_table_name, public_table_name, lowered, unnest, order_by, consistency);
        }
        return try executeLoweredDocumentSqlUnnestReadPlanAlloc(alloc, source, native_table_name, public_table_name, lowered, unnest, consistency);
    }

    if (lowered.order_by) |order_by| {
        return try executeOrderedLoweredDocumentSqlReadPlanAlloc(alloc, source, native_table_name, public_table_name, lowered, order_by, consistency);
    }

    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
    }

    switch (lowered.producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lowered.lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lowered.lateral_subquery.?.join_kind != .left) continue;
                try rows.append(alloc, try documentSqlProjectedRowJsonWithLateralAlloc(alloc, id, lookup.json, lowered.projection, lateral_match));
                if (lowered.limit) |limit| {
                    if (rows.items.len >= limit) break;
                }
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse lowered.limit;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            try appendDocumentSqlRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, lowered.projection, query.residual_filter_json, lowered.lateral_subquery, lowered.limit, consistency, &rows);
            if (query.residual_filter_json != null and rows.items.len < (lowered.limit orelse std.math.maxInt(u32))) {
                const max_candidate_rows = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
                const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
                try documentSqlAdmitBoundedRowCount(total_hits, max_candidate_rows);
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = scan_plan.max_rows,
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                scanned += 1;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lowered.lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lowered.lateral_subquery.?.join_kind != .left) continue;
                try rows.append(alloc, try documentSqlProjectedRowJsonWithLateralAlloc(alloc, key_value.string, lookup.json, lowered.projection, lateral_match));
                if (lowered.limit) |limit| {
                    if (rows.items.len >= limit) break;
                }
            }
            if (scan_plan.residual_filter_json != null and lowered.limit != null and rows.items.len < lowered.limit.?) try documentSqlAdmitBoundedRowProbeCount(scanned, scan_plan.max_rows);
        },
    }

    const total: u32 = @intCast(rows.items.len);
    return .{
        .rows = try rows.toOwnedSlice(alloc),
        .total = total,
    };
}

fn freeDocumentCandidateIds(alloc: std.mem.Allocator, ids: []const []const u8) void {
    for (ids) |id| alloc.free(@constCast(id));
    if (ids.len > 0) alloc.free(ids);
}

fn appendDocumentCandidateIdAlloc(
    alloc: std.mem.Allocator,
    ids: *std.ArrayListUnmanaged([]const u8),
    id: []const u8,
) !void {
    const owned_id = try alloc.dupe(u8, id);
    errdefer alloc.free(owned_id);
    try ids.append(alloc, owned_id);
}

fn appendDocumentCandidateIdsFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    residual_filter_json: ?[]const u8,
    consistency: raft_mod.ReadConsistency,
    ids: *std.ArrayListUnmanaged([]const u8),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (residual_filter_json) |filter| {
            if (hit_value.object.get("_source")) |source_value| {
                if (source_value == .object) {
                    const doc_json = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
                    defer alloc.free(doc_json);
                    if (!try residualFilterMatchesAlloc(alloc, doc_json, filter)) continue;
                    try appendDocumentCandidateIdAlloc(alloc, ids, id_value.string);
                    continue;
                }
                if (source_value != .null) return error.InvalidRowsRequest;
            }

            var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
            defer lookup.deinit(alloc);
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        try appendDocumentCandidateIdAlloc(alloc, ids, id_value.string);
    }
}

fn collectDocumentProducerCandidateIdsAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    producer: sql_adapter.DocumentProducer,
    consistency: raft_mod.ReadConsistency,
) ![][]const u8 {
    var ids = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (ids.items) |id| alloc.free(@constCast(id));
        ids.deinit(alloc);
    }

    switch (producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                if (lookup_plan.residual_filter_json) |filter| {
                    var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                    defer lookup.deinit(alloc);
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentCandidateIdAlloc(alloc, &ids, id);
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, documentSqlIndexedCandidateProbeLimit(query_limit), query.residual_filter_json != null, false, consistency)) orelse return error.UnsupportedOperation;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowCount(total_hits, query_limit);
            try appendDocumentCandidateIdsFromQueryResponseAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                query_response.json,
                query.residual_filter_json,
                consistency,
                &ids,
            );
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = documentSqlBoundedScanProbeLimit(scan_plan.max_rows),
            }, consistency)) orelse return error.UnsupportedOperation;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                scanned += 1;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                if (scan_plan.residual_filter_json) |filter| {
                    var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                    defer lookup.deinit(alloc);
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentCandidateIdAlloc(alloc, &ids, key_value.string);
            }
            try documentSqlAdmitBoundedRowCount(scanned, scan_plan.max_rows);
        },
    }

    return try ids.toOwnedSlice(alloc);
}

fn cloneDocumentMutationTemplateOpsAlloc(
    alloc: std.mem.Allocator,
    operations: []const db_mod.types.TransformOp,
) ![]db_mod.types.TransformOp {
    var out = try alloc.alloc(db_mod.types.TransformOp, operations.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (out.len > 0) alloc.free(out);
    }

    for (operations) |op| {
        const path = try alloc.dupe(u8, op.path);
        errdefer alloc.free(path);
        const value_json = if (op.value_json) |value| try alloc.dupe(u8, value) else null;
        errdefer if (value_json) |value| alloc.free(value);
        out[initialized] = .{
            .op = op.op,
            .path = path,
            .value_json = value_json,
        };
        initialized += 1;
    }
    return out;
}

fn freeDocumentMutationTemplateOps(alloc: std.mem.Allocator, operations: []const db_mod.types.TransformOp) void {
    for (operations) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
    if (operations.len > 0) alloc.free(@constCast(operations));
}

fn documentProducerMutationVersionPredicatesAlloc(
    alloc: std.mem.Allocator,
    candidate_ids: []const []const u8,
    expected_version: ?u64,
) ![]db_mod.types.TransactionVersionPredicate {
    const version = expected_version orelse return &.{};
    if (candidate_ids.len == 0) return &.{};
    var predicates = try alloc.alloc(db_mod.types.TransactionVersionPredicate, candidate_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (predicates[0..initialized]) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates.len > 0) alloc.free(predicates);
    }
    for (candidate_ids) |id| {
        predicates[initialized] = .{
            .key = try alloc.dupe(u8, id),
            .expected_version = version,
        };
        initialized += 1;
    }
    return predicates;
}

fn appendDocumentMutationReturningRowFromJsonAlloc(
    alloc: std.mem.Allocator,
    returning_rows: *std.ArrayListUnmanaged([]const u8),
    key: []const u8,
    doc_json: []const u8,
    fields: []const sql_plan.DocumentWriteReturningField,
    version: ?u64,
) !void {
    if (fields.len == 0) return;
    const row = try documentConflictReturningRowFromJsonAlloc(alloc, key, doc_json, fields, version);
    errdefer alloc.free(@constCast(row));
    try returning_rows.append(alloc, row);
}

fn appendDocumentMutationReturningRowAfterOperationsAlloc(
    alloc: std.mem.Allocator,
    returning_rows: *std.ArrayListUnmanaged([]const u8),
    key: []const u8,
    doc_json: []const u8,
    operations: []const db_mod.types.TransformOp,
    fields: []const sql_plan.DocumentWriteReturningField,
    version: ?u64,
) !void {
    if (fields.len == 0) return;
    const row = try documentConflictReturningRowAfterOperationsAlloc(alloc, key, doc_json, operations, fields, version);
    errdefer alloc.free(@constCast(row));
    try returning_rows.append(alloc, row);
}

fn documentReturningFieldsHaveVersion(fields: []const sql_plan.DocumentWriteReturningField) bool {
    for (fields) |field| {
        if (field.kind == .version) return true;
    }
    return false;
}

fn documentReturningVersionOutputsAlloc(
    alloc: std.mem.Allocator,
    fields: []const sql_plan.DocumentWriteReturningField,
) ![][]const u8 {
    var outputs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (outputs.items) |output| alloc.free(@constCast(output));
        outputs.deinit(alloc);
    }
    for (fields) |field| {
        if (field.kind != .version) continue;
        try outputs.append(alloc, try alloc.dupe(u8, field.output));
    }
    return try outputs.toOwnedSlice(alloc);
}

fn appendDocumentReturningVersionKeyAlloc(
    alloc: std.mem.Allocator,
    keys: *std.ArrayListUnmanaged([]const u8),
    prewrite_versions: *std.ArrayListUnmanaged(u64),
    key: []const u8,
    prewrite_version: u64,
    fields: []const sql_plan.DocumentWriteReturningField,
) !void {
    if (!documentReturningFieldsHaveVersion(fields)) return;
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try keys.append(alloc, owned_key);
    errdefer _ = keys.pop();
    try prewrite_versions.append(alloc, prewrite_version);
}

fn freeDocumentReturningVersionKeyState(
    alloc: std.mem.Allocator,
    keys: *std.ArrayListUnmanaged([]const u8),
    outputs: ?[][]const u8,
) void {
    for (keys.items) |key| alloc.free(@constCast(key));
    keys.deinit(alloc);
    if (outputs) |owned_outputs| {
        for (owned_outputs) |output| alloc.free(@constCast(output));
        if (owned_outputs.len > 0) alloc.free(owned_outputs);
    }
}

fn documentSourceInsertProjectedJsonAlloc(
    alloc: std.mem.Allocator,
    source_doc: std.json.Value,
    assignments: []const sql_plan.DocumentSourceInsertAssignment,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    for (assignments) |assignment| {
        if (assignment.kind != .projection) continue;
        if (assignment.target_path.len == 0 or std.mem.indexOfAny(u8, assignment.target_path, "./") != null) return error.DocumentSqlWriteUnsupported;
        try appendJsonFieldName(alloc, &out, &first, assignment.target_path);
        if (documentSqlProjectedValue(source_doc, assignment.source_path)) |value| {
            try out.print(alloc, "{f}", .{std.json.fmt(value, .{})});
        } else {
            try out.appendSlice(alloc, "null");
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn documentSourceInsertContainsKey(writes: []const db_mod.types.BatchWrite, key: []const u8) bool {
    for (writes) |write| {
        if (std.mem.eql(u8, write.key, key)) return true;
    }
    return false;
}

fn documentSourceInsertContainsSourceId(source_ids: []const []const u8, id: []const u8) bool {
    for (source_ids) |source_id| {
        if (std.mem.eql(u8, source_id, id)) return true;
    }
    return false;
}

fn documentSourceInsertSourceIdLessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

pub fn materializeDocumentSourceInsertBatchAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentSourceInsert,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    const native_table_name = source.native_table_name;
    const public_table_name = source.public_table_name;
    const candidate_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, native_table_name, public_table_name, lowered.source_producer, consistency);
    defer freeDocumentCandidateIds(alloc, candidate_ids);
    if (lowered.target_id_mode == .generated_document_id) {
        std.mem.sort([]const u8, candidate_ids, {}, documentSourceInsertSourceIdLessThan);
    }

    var writes = try alloc.alloc(db_mod.types.BatchWrite, candidate_ids.len);
    var initialized: usize = 0;
    var source_ids_seen = std.ArrayListUnmanaged([]const u8).empty;
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_keys = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_prewrite = std.ArrayListUnmanaged(u64).empty;
    const source_returning_fields: []const sql_plan.DocumentWriteReturningField = if (lowered.conflict_write == null) lowered.returning_fields else &.{};
    var returning_version_outputs: ?[][]const u8 = try documentReturningVersionOutputsAlloc(alloc, source_returning_fields);
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (writes.len > 0) alloc.free(writes);
        source_ids_seen.deinit(alloc);
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
        freeDocumentReturningVersionKeyState(alloc, &returning_version_keys, returning_version_outputs);
        returning_version_prewrite.deinit(alloc);
    }

    for (candidate_ids) |id| {
        if (lowered.source_limit) |limit| {
            if (initialized >= limit) break;
        }
        var lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.source_table_name, id, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (documentSourceInsertContainsSourceId(source_ids_seen.items, id)) return error.DocumentSqlWriteDuplicateSource;
        try source_ids_seen.append(alloc, id);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        const key = switch (lowered.target_id_mode) {
            .source_identity => try alloc.dupe(u8, id),
            .generated_document_id => try document_write.generatedDocumentIdAlloc(alloc),
        };
        var key_transferred = false;
        errdefer if (!key_transferred) alloc.free(key);
        const value = try documentSourceInsertProjectedJsonAlloc(alloc, parsed.value, lowered.assignments);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value);
        if (documentSourceInsertContainsKey(writes[0..initialized], key)) return error.DocumentSqlWriteDuplicateSource;
        try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, key, value, source_returning_fields, null);
        try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, key, 0, source_returning_fields);
        writes[initialized] = .{
            .key = key,
            .value = value,
        };
        key_transferred = true;
        value_transferred = true;
        initialized += 1;
    }
    source_ids_seen.deinit(alloc);
    source_ids_seen = .empty;

    const writes_slice = if (initialized == writes.len) writes else try alloc.realloc(writes, initialized);
    writes = &.{};
    errdefer {
        for (writes_slice) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (writes_slice.len > 0) alloc.free(writes_slice);
    }
    const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
    returning_rows = .empty;
    errdefer {
        for (returning_rows_slice) |row| alloc.free(@constCast(row));
        if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
    }
    const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
    returning_version_keys = .empty;
    errdefer {
        for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
        if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
    }
    const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
    returning_version_prewrite = .empty;
    errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
    var returning_version_outputs_slice: [][]const u8 = &.{};
    if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
    returning_version_outputs = null;
    errdefer {
        for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
        if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
    }
    var source_batch = sql_plan.OwnedDocumentBatchRequest{
        .writes = writes_slice,
        .returning_rows = returning_rows_slice,
        .returning_version_keys = returning_version_keys_slice,
        .returning_version_prewrite = returning_version_prewrite_slice,
        .returning_version_outputs = returning_version_outputs_slice,
        .req = .{
            .writes = writes_slice,
            .sync_level = lowered.sync_level,
            .write_mode = .upsert,
        },
        .inserted = @intCast(writes_slice.len),
    };
    if (lowered.conflict_write) |conflict| {
        errdefer source_batch.deinit(alloc);
        var conflict_lowered = conflict;
        conflict_lowered.proposed_writes = source_batch.writes;
        conflict_lowered.returning_fields = lowered.returning_fields;
        conflict_lowered.sync_level = lowered.sync_level;
        const conflict_batch = try materializeDocumentConflictWriteBatchAlloc(alloc, source, conflict_lowered, consistency);
        source_batch.deinit(alloc);
        return conflict_batch;
    }
    return source_batch;
}

pub fn executeDocumentSourceInsertPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    sink: BatchSink,
    lowered: sql_plan.LoweredDocumentSourceInsert,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    var batch = try materializeDocumentSourceInsertBatchAlloc(alloc, source, lowered, consistency);
    errdefer batch.deinit(alloc);
    try sink.batch(batch.req);
    return batch;
}

pub fn materializeProducerMutationBatchAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentProducerMutation,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    const native_table_name = source.native_table_name;
    const public_table_name = source.public_table_name;
    const candidate_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, native_table_name, public_table_name, lowered.producer, consistency);
    var candidate_ids_transferred = false;
    errdefer if (!candidate_ids_transferred) freeDocumentCandidateIds(alloc, candidate_ids);

    const predicates = try documentProducerMutationVersionPredicatesAlloc(alloc, candidate_ids, lowered.expected_version);
    errdefer {
        for (predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates.len > 0) alloc.free(predicates);
    }
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
    }
    var returning_version_keys = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_prewrite = std.ArrayListUnmanaged(u64).empty;
    var returning_version_outputs: ?[][]const u8 = try documentReturningVersionOutputsAlloc(alloc, lowered.returning_fields);
    errdefer {
        freeDocumentReturningVersionKeyState(alloc, &returning_version_keys, returning_version_outputs);
        returning_version_prewrite.deinit(alloc);
    }

    switch (lowered.template) {
        .delete => {
            if (lowered.returning_fields.len != 0) {
                for (candidate_ids) |id| {
                    var lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, id, consistency)) orelse continue;
                    defer lookup.deinit(alloc);
                    try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, id, lookup.json, lowered.returning_fields, lookup.version);
                    try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, id, lookup.version, lowered.returning_fields);
                }
            }
            const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
            returning_rows = .empty;
            errdefer {
                for (returning_rows_slice) |row| alloc.free(@constCast(row));
                if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
            }
            const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
            returning_version_keys = .empty;
            errdefer {
                for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
                if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
            }
            const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
            returning_version_prewrite = .empty;
            errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
            var returning_version_outputs_slice: [][]const u8 = &.{};
            if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
            returning_version_outputs = null;
            errdefer {
                for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
                if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
            }
            candidate_ids_transferred = true;
            return .{
                .deletes = candidate_ids,
                .predicates = predicates,
                .returning_rows = returning_rows_slice,
                .returning_version_keys = returning_version_keys_slice,
                .returning_version_prewrite = returning_version_prewrite_slice,
                .returning_version_outputs = returning_version_outputs_slice,
                .req = .{
                    .deletes = candidate_ids,
                    .predicates = predicates,
                    .sync_level = lowered.sync_level,
                },
                .deleted = @intCast(candidate_ids.len),
            };
        },
        .transform => |operations_template| {
            if (candidate_ids.len == 0) {
                freeDocumentCandidateIds(alloc, candidate_ids);
                candidate_ids_transferred = true;
                const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
                returning_rows = .empty;
                const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
                returning_version_keys = .empty;
                const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
                returning_version_prewrite = .empty;
                var returning_version_outputs_slice: [][]const u8 = &.{};
                if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
                returning_version_outputs = null;
                return .{
                    .predicates = predicates,
                    .returning_rows = returning_rows_slice,
                    .returning_version_keys = returning_version_keys_slice,
                    .returning_version_prewrite = returning_version_prewrite_slice,
                    .returning_version_outputs = returning_version_outputs_slice,
                    .req = .{
                        .predicates = predicates,
                        .sync_level = lowered.sync_level,
                    },
                };
            }
            var transforms = try alloc.alloc(db_mod.types.DocumentTransform, candidate_ids.len);
            var initialized: usize = 0;
            errdefer {
                for (transforms[0..initialized]) |transform| {
                    alloc.free(@constCast(transform.key));
                    freeDocumentMutationTemplateOps(alloc, transform.operations);
                }
                if (transforms.len > 0) alloc.free(transforms);
            }
            for (candidate_ids) |id| {
                const transform_key = try alloc.dupe(u8, id);
                var key_transferred = false;
                errdefer if (!key_transferred) alloc.free(transform_key);
                const operations = try cloneDocumentMutationTemplateOpsAlloc(alloc, operations_template);
                var operations_transferred = false;
                errdefer if (!operations_transferred) freeDocumentMutationTemplateOps(alloc, operations);
                if (lowered.returning_fields.len != 0) {
                    var lookup_opt = try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, id, consistency);
                    if (lookup_opt) |*lookup| {
                        defer lookup.deinit(alloc);
                        try appendDocumentMutationReturningRowAfterOperationsAlloc(alloc, &returning_rows, id, lookup.json, operations, lowered.returning_fields, null);
                        try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, id, 0, lowered.returning_fields);
                    }
                }
                transforms[initialized] = .{
                    .key = transform_key,
                    .operations = operations,
                };
                key_transferred = true;
                operations_transferred = true;
                initialized += 1;
            }
            const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
            returning_rows = .empty;
            errdefer {
                for (returning_rows_slice) |row| alloc.free(@constCast(row));
                if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
            }
            const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
            returning_version_keys = .empty;
            errdefer {
                for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
                if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
            }
            const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
            returning_version_prewrite = .empty;
            errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
            var returning_version_outputs_slice: [][]const u8 = &.{};
            if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
            returning_version_outputs = null;
            errdefer {
                for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
                if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
            }
            freeDocumentCandidateIds(alloc, candidate_ids);
            candidate_ids_transferred = true;
            return .{
                .transforms = transforms,
                .predicates = predicates,
                .returning_rows = returning_rows_slice,
                .returning_version_keys = returning_version_keys_slice,
                .returning_version_prewrite = returning_version_prewrite_slice,
                .returning_version_outputs = returning_version_outputs_slice,
                .req = .{
                    .transforms = transforms,
                    .predicates = predicates,
                    .sync_level = lowered.sync_level,
                },
                .transformed = @intCast(transforms.len),
            };
        },
    }
}

pub fn executeProducerMutationPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    sink: BatchSink,
    lowered: sql_plan.LoweredDocumentProducerMutation,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    var batch = try materializeProducerMutationBatchAlloc(alloc, source, lowered, consistency);
    errdefer batch.deinit(alloc);
    try sink.batch(batch.req);
    return batch;
}

fn documentSqlLookupTableAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    table_name: []const u8,
    key: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    return try source.lookup(alloc, table_name, key, .{}, consistency);
}

const DocumentJoinedCandidate = struct {
    id: []const u8,
    join_value_json: []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.id));
        alloc.free(@constCast(self.join_value_json));
        self.* = undefined;
    }
};

fn freeDocumentJoinedCandidates(alloc: std.mem.Allocator, candidates: []DocumentJoinedCandidate) void {
    for (candidates) |*candidate| candidate.deinit(alloc);
    if (candidates.len > 0) alloc.free(candidates);
}

fn documentJoinedCandidateJoinValueJsonAlloc(
    alloc: std.mem.Allocator,
    id: []const u8,
    doc_json: []const u8,
    field: []const u8,
) !?[]const u8 {
    if (std.ascii.eqlIgnoreCase(field, "_id")) {
        return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = id }, .{});
    }

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    const value = documentSqlProjectedValue(parsed.value, field) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn collectDocumentJoinedCandidatesAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    table_name: []const u8,
    ids: []const []const u8,
    join_field: []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]DocumentJoinedCandidate {
    var candidates = std.ArrayListUnmanaged(DocumentJoinedCandidate).empty;
    errdefer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }

    for (ids) |id| {
        var lookup = (try documentSqlLookupTableAlloc(alloc, source, table_name, id, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        const join_value_json = (try documentJoinedCandidateJoinValueJsonAlloc(alloc, id, lookup.json, join_field)) orelse continue;
        errdefer alloc.free(@constCast(join_value_json));
        const owned_id = try alloc.dupe(u8, id);
        errdefer alloc.free(owned_id);
        try candidates.append(alloc, .{
            .id = owned_id,
            .join_value_json = join_value_json,
        });
    }

    return try candidates.toOwnedSlice(alloc);
}

fn documentJoinedSourceCandidateForTarget(
    source_candidates: []const DocumentJoinedCandidate,
    target: DocumentJoinedCandidate,
) ?DocumentJoinedCandidate {
    for (source_candidates) |source_candidate| {
        if (std.mem.eql(u8, source_candidate.join_value_json, target.join_value_json)) return source_candidate;
    }
    return null;
}

fn documentJoinedSourceCandidatesContainDuplicateJoinValue(source_candidates: []const DocumentJoinedCandidate, join_value_json: []const u8) bool {
    var seen = false;
    for (source_candidates) |source_candidate| {
        if (!std.mem.eql(u8, source_candidate.join_value_json, join_value_json)) continue;
        if (seen) return true;
        seen = true;
    }
    return false;
}

fn documentJoinedFilterPathForFieldAlloc(alloc: std.mem.Allocator, field: []const u8) ![]const u8 {
    if (field.len == 0) return error.DocumentSqlWriteUnsupported;
    if (field[0] == '/') return try alloc.dupe(u8, field);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '/');
    for (field) |ch| {
        try out.append(alloc, if (ch == '.') '/' else ch);
    }
    return try out.toOwnedSlice(alloc);
}

fn documentJoinedSourceLookupFilterJsonAlloc(
    alloc: std.mem.Allocator,
    source_field: []const u8,
    join_value_json: []const u8,
) ![]const u8 {
    const path = try documentJoinedFilterPathForFieldAlloc(alloc, source_field);
    defer alloc.free(path);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"term\":{\"path\":");
    try appendJsonString(alloc, &out, path);
    try out.appendSlice(alloc, ",\"value\":");
    try out.appendSlice(alloc, join_value_json);
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

fn collectDocumentJoinedSourceCandidateForTargetAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    source_table_name: []const u8,
    source_field: []const u8,
    target: DocumentJoinedCandidate,
    consistency: raft_mod.ReadConsistency,
) !?DocumentJoinedCandidate {
    const filter_json = try documentJoinedSourceLookupFilterJsonAlloc(alloc, source_field, target.join_value_json);
    defer alloc.free(filter_json);
    const query = sql_adapter.DocumentIndexQuery{
        .filter_query_json = filter_json,
        .max_candidate_rows = 2,
    };
    var response = (try documentSqlIndexQueryAlloc(alloc, source, source_table_name, source_table_name, query, documentSqlIndexedCandidateProbeLimit(2), false, false, consistency)) orelse return error.UnsupportedOperation;
    defer response.deinit(alloc);
    const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, response.json);
    if (total_hits == 0) return null;
    if (total_hits > 1) return error.DocumentSqlWriteDuplicateSource;

    var ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (ids.items) |id| alloc.free(@constCast(id));
        ids.deinit(alloc);
    }
    try appendDocumentCandidateIdsFromQueryResponseAlloc(
        alloc,
        source,
        source_table_name,
        source_table_name,
        response.json,
        null,
        consistency,
        &ids,
    );
    if (ids.items.len == 0) return null;
    if (ids.items.len > 1) return error.DocumentSqlWriteDuplicateSource;
    var candidates = try collectDocumentJoinedCandidatesAlloc(alloc, source, source_table_name, ids.items[0..1], source_field, consistency);
    defer {
        for (candidates) |*candidate| candidate.deinit(alloc);
        if (candidates.len > 0) alloc.free(candidates);
    }
    if (candidates.len == 0) return null;
    if (!std.mem.eql(u8, candidates[0].join_value_json, target.join_value_json)) return null;
    const out = candidates[0];
    candidates[0] = .{ .id = "", .join_value_json = "" };
    return out;
}

fn documentJoinedSourceAssignmentOpsAlloc(
    alloc: std.mem.Allocator,
    operations_template: []const db_mod.types.TransformOp,
    source_doc_json: []const u8,
    assignments: []const sql_plan.DocumentJoinedMutationSourceAssignment,
) ![]db_mod.types.TransformOp {
    var source_doc = std.json.parseFromSlice(std.json.Value, alloc, source_doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer source_doc.deinit();

    var operations = try alloc.alloc(db_mod.types.TransformOp, operations_template.len + assignments.len);
    var initialized: usize = 0;
    errdefer {
        for (operations[0..initialized]) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (operations.len > 0) alloc.free(operations);
    }

    for (operations_template) |op| {
        const path = try alloc.dupe(u8, op.path);
        errdefer alloc.free(path);
        const value_json = if (op.value_json) |value| try alloc.dupe(u8, value) else null;
        errdefer if (value_json) |value| alloc.free(value);
        operations[initialized] = .{
            .op = op.op,
            .path = path,
            .value_json = value_json,
        };
        initialized += 1;
    }

    for (assignments) |assignment| {
        const path = try alloc.dupe(u8, assignment.target_path);
        errdefer alloc.free(path);
        const value_json = if (documentSqlProjectedValue(source_doc.value, assignment.source_field)) |value|
            try std.json.Stringify.valueAlloc(alloc, value, .{})
        else
            try alloc.dupe(u8, "null");
        errdefer alloc.free(value_json);
        operations[initialized] = .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        };
        initialized += 1;
    }

    return operations;
}

fn documentJoinedMutationPredicatesAlloc(
    alloc: std.mem.Allocator,
    target_ids: []const []const u8,
    expected_version: ?u64,
) ![]db_mod.types.TransactionVersionPredicate {
    const version = expected_version orelse return &.{};
    if (target_ids.len == 0) return &.{};
    var predicates = try alloc.alloc(db_mod.types.TransactionVersionPredicate, target_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (predicates[0..initialized]) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates.len > 0) alloc.free(predicates);
    }
    for (target_ids) |id| {
        predicates[initialized] = .{
            .key = try alloc.dupe(u8, id),
            .expected_version = version,
        };
        initialized += 1;
    }
    return predicates;
}

pub fn materializeJoinedMutationBatchAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentJoinedMutation,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    if (lowered.join_keys.len != 1) return error.DocumentSqlWriteUnsupported;
    const join_key = lowered.join_keys[0];

    const target_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, source.native_table_name, source.public_table_name, lowered.target_producer, consistency);
    defer freeDocumentCandidateIds(alloc, target_ids);

    if (lowered.max_target_rows) |max_target_rows| {
        if (target_ids.len > max_target_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
    }
    const target_candidates = try collectDocumentJoinedCandidatesAlloc(alloc, source, lowered.table_name, target_ids, join_key.target_field, consistency);
    defer freeDocumentJoinedCandidates(alloc, target_candidates);

    var static_source_candidates: []DocumentJoinedCandidate = &.{};
    var static_source_candidates_initialized = false;
    defer if (static_source_candidates_initialized) freeDocumentJoinedCandidates(alloc, static_source_candidates);
    switch (lowered.source_producer) {
        .static => |producer| {
            const source_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, lowered.source_table_name, lowered.source_table_name, producer, consistency);
            defer freeDocumentCandidateIds(alloc, source_ids);
            if (lowered.max_source_rows) |max_source_rows| {
                if (source_ids.len > max_source_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
            }
            static_source_candidates = try collectDocumentJoinedCandidatesAlloc(alloc, source, lowered.source_table_name, source_ids, join_key.source_field, consistency);
            static_source_candidates_initialized = true;
            for (static_source_candidates) |source_candidate| {
                if (documentJoinedSourceCandidatesContainDuplicateJoinValue(static_source_candidates, source_candidate.join_value_json)) return error.DocumentSqlWriteDuplicateSource;
            }
        },
        .join_key_indexed_lookup => {},
    }

    var matched_ids = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (matched_ids.items) |id| alloc.free(@constCast(id));
        matched_ids.deinit(alloc);
    }
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
    }
    var returning_version_keys = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_prewrite = std.ArrayListUnmanaged(u64).empty;
    var returning_version_outputs: ?[][]const u8 = try documentReturningVersionOutputsAlloc(alloc, lowered.returning_fields);
    errdefer {
        freeDocumentReturningVersionKeyState(alloc, &returning_version_keys, returning_version_outputs);
        returning_version_prewrite.deinit(alloc);
    }

    switch (lowered.template) {
        .delete => {
            for (target_candidates) |target_candidate| {
                var owned_source_candidate: ?DocumentJoinedCandidate = null;
                defer if (owned_source_candidate) |*candidate| candidate.deinit(alloc);
                const source_candidate = switch (lowered.source_producer) {
                    .static => documentJoinedSourceCandidateForTarget(static_source_candidates, target_candidate) orelse continue,
                    .join_key_indexed_lookup => blk: {
                        owned_source_candidate = try collectDocumentJoinedSourceCandidateForTargetAlloc(alloc, source, lowered.source_table_name, join_key.source_field, target_candidate, consistency);
                        break :blk owned_source_candidate orelse continue;
                    },
                };
                var target_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, target_candidate.id, consistency)) orelse continue;
                defer target_lookup.deinit(alloc);
                var source_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.source_table_name, source_candidate.id, consistency)) orelse continue;
                defer source_lookup.deinit(alloc);
                try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, target_candidate.id, target_lookup.json, lowered.returning_fields, target_lookup.version);
                try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, target_candidate.id, target_lookup.version, lowered.returning_fields);
                const matched_id = try alloc.dupe(u8, target_candidate.id);
                errdefer alloc.free(matched_id);
                try matched_ids.append(alloc, matched_id);
            }
            const deletes = try matched_ids.toOwnedSlice(alloc);
            matched_ids = .empty;
            errdefer freeDocumentCandidateIds(alloc, deletes);
            const predicates = try documentJoinedMutationPredicatesAlloc(alloc, deletes, lowered.expected_version);
            errdefer {
                for (predicates) |predicate| alloc.free(@constCast(predicate.key));
                if (predicates.len > 0) alloc.free(predicates);
            }
            const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
            returning_rows = .empty;
            errdefer {
                for (returning_rows_slice) |row| alloc.free(@constCast(row));
                if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
            }
            const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
            returning_version_keys = .empty;
            errdefer {
                for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
                if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
            }
            const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
            returning_version_prewrite = .empty;
            errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
            var returning_version_outputs_slice: [][]const u8 = &.{};
            if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
            returning_version_outputs = null;
            errdefer {
                for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
                if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
            }
            return .{
                .deletes = deletes,
                .predicates = predicates,
                .returning_rows = returning_rows_slice,
                .returning_version_keys = returning_version_keys_slice,
                .returning_version_prewrite = returning_version_prewrite_slice,
                .returning_version_outputs = returning_version_outputs_slice,
                .req = .{
                    .deletes = deletes,
                    .predicates = predicates,
                    .sync_level = lowered.sync_level,
                },
                .deleted = @intCast(deletes.len),
            };
        },
        .transform => |operations_template| {
            var transforms = std.ArrayListUnmanaged(db_mod.types.DocumentTransform).empty;
            errdefer {
                for (transforms.items) |transform| {
                    alloc.free(@constCast(transform.key));
                    freeDocumentMutationTemplateOps(alloc, transform.operations);
                }
                transforms.deinit(alloc);
            }
            for (target_candidates) |target_candidate| {
                var owned_source_candidate: ?DocumentJoinedCandidate = null;
                defer if (owned_source_candidate) |*candidate| candidate.deinit(alloc);
                const source_candidate = switch (lowered.source_producer) {
                    .static => documentJoinedSourceCandidateForTarget(static_source_candidates, target_candidate) orelse continue,
                    .join_key_indexed_lookup => blk: {
                        owned_source_candidate = try collectDocumentJoinedSourceCandidateForTargetAlloc(alloc, source, lowered.source_table_name, join_key.source_field, target_candidate, consistency);
                        break :blk owned_source_candidate orelse continue;
                    },
                };
                var target_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, target_candidate.id, consistency)) orelse continue;
                defer target_lookup.deinit(alloc);
                var source_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.source_table_name, source_candidate.id, consistency)) orelse continue;
                defer source_lookup.deinit(alloc);

                const transform_key = try alloc.dupe(u8, target_candidate.id);
                var key_transferred = false;
                errdefer if (!key_transferred) alloc.free(transform_key);
                const operations = try documentJoinedSourceAssignmentOpsAlloc(alloc, operations_template, source_lookup.json, lowered.source_assignments);
                var operations_transferred = false;
                errdefer if (!operations_transferred) freeDocumentMutationTemplateOps(alloc, operations);
                try appendDocumentMutationReturningRowAfterOperationsAlloc(alloc, &returning_rows, target_candidate.id, target_lookup.json, operations, lowered.returning_fields, null);
                try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, target_candidate.id, 0, lowered.returning_fields);
                try transforms.append(alloc, .{
                    .key = transform_key,
                    .operations = operations,
                });
                key_transferred = true;
                operations_transferred = true;
                const matched_id = try alloc.dupe(u8, target_candidate.id);
                errdefer alloc.free(matched_id);
                try matched_ids.append(alloc, matched_id);
            }

            const transform_slice = try transforms.toOwnedSlice(alloc);
            transforms = .empty;
            errdefer {
                for (transform_slice) |transform| {
                    alloc.free(@constCast(transform.key));
                    freeDocumentMutationTemplateOps(alloc, transform.operations);
                }
                if (transform_slice.len > 0) alloc.free(transform_slice);
            }
            const matched_slice = try matched_ids.toOwnedSlice(alloc);
            matched_ids = .empty;
            defer freeDocumentCandidateIds(alloc, matched_slice);
            const predicates = try documentJoinedMutationPredicatesAlloc(alloc, matched_slice, lowered.expected_version);
            errdefer {
                for (predicates) |predicate| alloc.free(@constCast(predicate.key));
                if (predicates.len > 0) alloc.free(predicates);
            }
            const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
            returning_rows = .empty;
            errdefer {
                for (returning_rows_slice) |row| alloc.free(@constCast(row));
                if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
            }
            const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
            returning_version_keys = .empty;
            errdefer {
                for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
                if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
            }
            const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
            returning_version_prewrite = .empty;
            errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
            var returning_version_outputs_slice: [][]const u8 = &.{};
            if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
            returning_version_outputs = null;
            errdefer {
                for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
                if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
            }
            return .{
                .transforms = transform_slice,
                .predicates = predicates,
                .returning_rows = returning_rows_slice,
                .returning_version_keys = returning_version_keys_slice,
                .returning_version_prewrite = returning_version_prewrite_slice,
                .returning_version_outputs = returning_version_outputs_slice,
                .req = .{
                    .transforms = transform_slice,
                    .predicates = predicates,
                    .sync_level = lowered.sync_level,
                },
                .transformed = @intCast(transform_slice.len),
            };
        },
    }
}

pub fn executeJoinedMutationPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    sink: BatchSink,
    lowered: sql_plan.LoweredDocumentJoinedMutation,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    var batch = try materializeJoinedMutationBatchAlloc(alloc, source, lowered, consistency);
    errdefer batch.deinit(alloc);
    try sink.batch(batch.req);
    return batch;
}

const DocumentMergeScalarComparison = enum { lt, eq, gt };

fn documentMergeJsonNumericValue(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        else => null,
    };
}

fn documentMergeJsonCompare(left: std.json.Value, right: std.json.Value) ?DocumentMergeScalarComparison {
    if (documentMergeJsonNumericValue(left)) |left_num| {
        const right_num = documentMergeJsonNumericValue(right) orelse return null;
        if (left_num < right_num) return .lt;
        if (left_num > right_num) return .gt;
        return .eq;
    }
    if (left == .string and right == .string) {
        return switch (std.mem.order(u8, left.string, right.string)) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    if (left == .bool and right == .bool) {
        if (left.bool == right.bool) return .eq;
        return if (!left.bool and right.bool) .lt else .gt;
    }
    return null;
}

fn documentMergeObjectFieldOrNull(value: std.json.Value, field: []const u8) ?std.json.Value {
    if (value != .object) return null;
    return value.object.get(field);
}

fn documentMergeObjectField(value: std.json.Value, field: []const u8) !std.json.Value {
    return documentMergeObjectFieldOrNull(value, field) orelse return error.InvalidRowsRequest;
}

fn documentMergePredicateMatches(
    alloc: std.mem.Allocator,
    target_id: []const u8,
    target_doc: std.json.Value,
    target_version: u64,
    source_id: []const u8,
    source_doc: std.json.Value,
    source_version: u64,
    predicate: sql_plan.MergeArmPredicate,
) !bool {
    const side_doc = switch (predicate.side) {
        .target => target_doc,
        .source => source_doc,
    };
    const side_id = switch (predicate.side) {
        .target => target_id,
        .source => source_id,
    };
    const side_version = switch (predicate.side) {
        .target => target_version,
        .source => source_version,
    };
    const actual: std.json.Value = if (std.ascii.eqlIgnoreCase(predicate.field, "_id"))
        .{ .string = side_id }
    else if (std.ascii.eqlIgnoreCase(predicate.field, "_version"))
        .{ .integer = @intCast(side_version) }
    else if (std.ascii.eqlIgnoreCase(predicate.field, "_doc"))
        side_doc
    else
        documentMergeObjectFieldOrNull(side_doc, predicate.field) orelse .{ .null = {} };

    if (predicate.op == .is_null) return actual == .null;
    if (predicate.op == .is_not_null) return actual != .null;
    const value_json = predicate.value_json orelse return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    const cmp = documentMergeJsonCompare(actual, parsed.value) orelse return false;
    return switch (predicate.op) {
        .eq => cmp == .eq,
        .ne => cmp != .eq,
        else => return error.DocumentSqlWriteUnsupported,
    };
}

fn documentMergePredicatesMatch(
    alloc: std.mem.Allocator,
    target_id: []const u8,
    target_doc: std.json.Value,
    target_version: u64,
    source_id: []const u8,
    source_doc: std.json.Value,
    source_version: u64,
    predicates: []const sql_plan.MergeArmPredicate,
) !bool {
    for (predicates) |predicate| {
        if (!try documentMergePredicateMatches(alloc, target_id, target_doc, target_version, source_id, source_doc, source_version, predicate)) return false;
    }
    return true;
}

fn selectDocumentMergeMatchedArm(
    alloc: std.mem.Allocator,
    target_id: []const u8,
    target_doc: std.json.Value,
    target_version: u64,
    source_id: []const u8,
    source_doc: std.json.Value,
    source_version: u64,
    arms: []const sql_plan.DocumentMergeMatchedArm,
) !?sql_plan.DocumentMergeMatchedArm {
    for (arms) |arm| {
        if (try documentMergePredicatesMatch(alloc, target_id, target_doc, target_version, source_id, source_doc, source_version, arm.predicates)) return arm;
    }
    return null;
}

fn selectDocumentMergeNotMatchedArm(
    alloc: std.mem.Allocator,
    source_id: []const u8,
    source_doc: std.json.Value,
    source_version: u64,
    arms: []const sql_plan.DocumentMergeNotMatchedArm,
) !?sql_plan.DocumentMergeNotMatchedArm {
    for (arms) |arm| {
        if (try documentMergePredicatesMatch(alloc, "", .{ .null = {} }, 0, source_id, source_doc, source_version, arm.predicates)) return arm;
    }
    return null;
}

fn documentMergeCandidateForSource(
    targets: []const DocumentJoinedCandidate,
    source_candidate: DocumentJoinedCandidate,
) !?DocumentJoinedCandidate {
    var found: ?DocumentJoinedCandidate = null;
    for (targets) |target| {
        if (!std.mem.eql(u8, target.join_value_json, source_candidate.join_value_json)) continue;
        if (found != null) return error.DocumentSqlWriteDuplicateSource;
        found = target;
    }
    return found;
}

fn documentMergeRejectDuplicateSourceJoinValues(sources: []const DocumentJoinedCandidate) !void {
    for (sources, 0..) |source_candidate, i| {
        for (sources[0..i]) |previous| {
            if (std.mem.eql(u8, previous.join_value_json, source_candidate.join_value_json)) return error.DocumentSqlWriteDuplicateSource;
        }
    }
}

fn appendDocumentMergeVersionPredicateAlloc(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    key: []const u8,
    expected_version: u64,
) !void {
    const predicate_key = try alloc.dupe(u8, key);
    errdefer alloc.free(predicate_key);
    try predicates.append(alloc, .{
        .key = predicate_key,
        .expected_version = expected_version,
    });
}

pub fn materializeDocumentMergeMutationBatchAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentMergeMutation,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    if (lowered.join_keys.len != 1) return error.DocumentSqlWriteUnsupported;
    const join_key = lowered.join_keys[0];

    const source_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, lowered.source_table_name, lowered.source_table_name, lowered.source_producer, consistency);
    defer freeDocumentCandidateIds(alloc, source_ids);
    if (lowered.max_source_rows) |max_source_rows| {
        if (source_ids.len > max_source_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
    }
    const source_candidates = try collectDocumentJoinedCandidatesAlloc(alloc, source, lowered.source_table_name, source_ids, join_key.source_field, consistency);
    defer freeDocumentJoinedCandidates(alloc, source_candidates);
    try documentMergeRejectDuplicateSourceJoinValues(source_candidates);

    const target_ids = try collectDocumentProducerCandidateIdsAlloc(alloc, source, source.native_table_name, source.public_table_name, lowered.target_producer, consistency);
    defer freeDocumentCandidateIds(alloc, target_ids);
    if (lowered.max_target_rows) |max_target_rows| {
        if (target_ids.len > max_target_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
    }
    const target_candidates = try collectDocumentJoinedCandidatesAlloc(alloc, source, lowered.table_name, target_ids, join_key.target_field, consistency);
    defer freeDocumentJoinedCandidates(alloc, target_candidates);

    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    var transforms = std.ArrayListUnmanaged(db_mod.types.DocumentTransform).empty;
    var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_keys = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_prewrite = std.ArrayListUnmanaged(u64).empty;
    var returning_version_outputs: ?[][]const u8 = try documentReturningVersionOutputsAlloc(alloc, lowered.returning_fields);
    errdefer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
        for (deletes.items) |key| alloc.free(key);
        deletes.deinit(alloc);
        for (transforms.items) |transform| {
            alloc.free(@constCast(transform.key));
            freeDocumentMutationTemplateOps(alloc, transform.operations);
        }
        transforms.deinit(alloc);
        for (predicates.items) |predicate| alloc.free(@constCast(predicate.key));
        predicates.deinit(alloc);
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
        freeDocumentReturningVersionKeyState(alloc, &returning_version_keys, returning_version_outputs);
        returning_version_prewrite.deinit(alloc);
    }

    for (source_candidates) |source_candidate| {
        var source_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.source_table_name, source_candidate.id, consistency)) orelse continue;
        defer source_lookup.deinit(alloc);
        var parsed_source = std.json.parseFromSlice(std.json.Value, alloc, source_lookup.json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed_source.deinit();

        if (try documentMergeCandidateForSource(target_candidates, source_candidate)) |target_candidate| {
            var target_lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, target_candidate.id, consistency)) orelse continue;
            defer target_lookup.deinit(alloc);
            var parsed_target = std.json.parseFromSlice(std.json.Value, alloc, target_lookup.json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
            defer parsed_target.deinit();
            const arm = (try selectDocumentMergeMatchedArm(alloc, target_candidate.id, parsed_target.value, target_lookup.version, source_candidate.id, parsed_source.value, source_lookup.version, lowered.matched_arms)) orelse continue;
            if (arm.do_nothing) continue;
            switch (arm.template) {
                .delete => {
                    if (lowered.returning_fields.len != 0) {
                        try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, target_candidate.id, target_lookup.json, lowered.returning_fields, target_lookup.version);
                        try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, target_candidate.id, target_lookup.version, lowered.returning_fields);
                    }
                    try appendDocumentMergeVersionPredicateAlloc(alloc, &predicates, target_candidate.id, target_lookup.version);
                    try deletes.append(alloc, try alloc.dupe(u8, target_candidate.id));
                },
                .transform => |template| {
                    const key = try alloc.dupe(u8, target_candidate.id);
                    errdefer alloc.free(key);
                    const operations = try documentJoinedSourceAssignmentOpsAlloc(alloc, template, source_lookup.json, arm.source_assignments);
                    errdefer freeDocumentMutationTemplateOps(alloc, operations);
                    if (lowered.returning_fields.len != 0) {
                        try appendDocumentMutationReturningRowAfterOperationsAlloc(alloc, &returning_rows, target_candidate.id, target_lookup.json, operations, lowered.returning_fields, null);
                        try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, target_candidate.id, 0, lowered.returning_fields);
                    }
                    try appendDocumentMergeVersionPredicateAlloc(alloc, &predicates, target_candidate.id, target_lookup.version);
                    try transforms.append(alloc, .{
                        .key = key,
                        .operations = operations,
                    });
                },
            }
        } else {
            const arm = (try selectDocumentMergeNotMatchedArm(alloc, source_candidate.id, parsed_source.value, source_lookup.version, lowered.not_matched_arms)) orelse continue;
            if (arm.do_nothing) continue;
            if (arm.insert_source_document) {
                const key = try alloc.dupe(u8, source_candidate.id);
                errdefer alloc.free(key);
                const value = try alloc.dupe(u8, source_lookup.json);
                errdefer alloc.free(value);
                if (lowered.returning_fields.len != 0) {
                    try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, key, value, lowered.returning_fields, null);
                    try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, key, 0, lowered.returning_fields);
                }
                try writes.append(alloc, .{
                    .key = key,
                    .value = value,
                });
            } else if (arm.insert_assignments.len != 0) {
                const key = try alloc.dupe(u8, source_candidate.id);
                errdefer alloc.free(key);
                const value = try documentSourceInsertProjectedJsonAlloc(alloc, parsed_source.value, arm.insert_assignments);
                errdefer alloc.free(value);
                if (lowered.returning_fields.len != 0) {
                    try appendDocumentMutationReturningRowFromJsonAlloc(alloc, &returning_rows, key, value, lowered.returning_fields, null);
                    try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, key, 0, lowered.returning_fields);
                }
                try writes.append(alloc, .{
                    .key = key,
                    .value = value,
                });
            }
        }
    }

    const write_slice = try writes.toOwnedSlice(alloc);
    writes = .empty;
    errdefer {
        for (write_slice) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (write_slice.len > 0) alloc.free(write_slice);
    }
    const delete_slice = try deletes.toOwnedSlice(alloc);
    deletes = .empty;
    errdefer freeDocumentCandidateIds(alloc, delete_slice);
    const transform_slice = try transforms.toOwnedSlice(alloc);
    transforms = .empty;
    errdefer {
        for (transform_slice) |transform| {
            alloc.free(@constCast(transform.key));
            freeDocumentMutationTemplateOps(alloc, transform.operations);
        }
        if (transform_slice.len > 0) alloc.free(transform_slice);
    }
    const predicate_slice = try predicates.toOwnedSlice(alloc);
    predicates = .empty;
    errdefer {
        for (predicate_slice) |predicate| alloc.free(@constCast(predicate.key));
        if (predicate_slice.len > 0) alloc.free(predicate_slice);
    }
    const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
    returning_rows = .empty;
    errdefer {
        for (returning_rows_slice) |row| alloc.free(@constCast(row));
        if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
    }
    const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
    returning_version_keys = .empty;
    errdefer {
        for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
        if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
    }
    const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
    returning_version_prewrite = .empty;
    errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
    var returning_version_outputs_slice: [][]const u8 = &.{};
    if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
    returning_version_outputs = null;
    errdefer {
        for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
        if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
    }

    return .{
        .writes = write_slice,
        .deletes = delete_slice,
        .transforms = transform_slice,
        .predicates = predicate_slice,
        .returning_rows = returning_rows_slice,
        .returning_version_keys = returning_version_keys_slice,
        .returning_version_prewrite = returning_version_prewrite_slice,
        .returning_version_outputs = returning_version_outputs_slice,
        .req = .{
            .writes = write_slice,
            .deletes = delete_slice,
            .transforms = transform_slice,
            .predicates = predicate_slice,
            .sync_level = lowered.sync_level,
            .write_mode = .create_only,
        },
        .inserted = @intCast(write_slice.len),
        .deleted = @intCast(delete_slice.len),
        .transformed = @intCast(transform_slice.len),
    };
}

fn documentConflictPathSeparator(path: []const u8) u8 {
    return if (path.len > 0 and path[0] == '/') '/' else '.';
}

fn documentConflictPathSegmentCount(path: []const u8) !usize {
    if (path.len == 0) return error.DocumentSqlWriteUnsupported;
    const separator = documentConflictPathSeparator(path);
    var count: usize = 0;
    var pos: usize = if (separator == '/') 1 else 0;
    while (pos <= path.len) {
        const next = std.mem.indexOfScalarPos(u8, path, pos, separator) orelse path.len;
        if (next == pos) return error.DocumentSqlWriteUnsupported;
        count += 1;
        if (next == path.len) break;
        pos = next + 1;
    }
    return count;
}

fn documentConflictPathSegment(path: []const u8, depth: usize) ![]const u8 {
    const separator = documentConflictPathSeparator(path);
    var pos: usize = if (separator == '/') 1 else 0;
    var current: usize = 0;
    while (pos <= path.len) {
        const next = std.mem.indexOfScalarPos(u8, path, pos, separator) orelse path.len;
        if (next == pos) return error.DocumentSqlWriteUnsupported;
        if (current == depth) return path[pos..next];
        if (next == path.len) break;
        pos = next + 1;
        current += 1;
    }
    return error.DocumentSqlWriteUnsupported;
}

fn documentConflictProjectedValue(doc: std.json.Value, path: []const u8) !?std.json.Value {
    var current = doc;
    const count = try documentConflictPathSegmentCount(path);
    var depth: usize = 0;
    while (depth < count) : (depth += 1) {
        if (current != .object) return null;
        const segment = try documentConflictPathSegment(path, depth);
        current = current.object.get(segment) orelse return null;
    }
    return current;
}

fn appendDocumentConflictOperationFromProjectedValueAlloc(
    alloc: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
    target_path: []const u8,
    value: std.json.Value,
) !void {
    const path = try alloc.dupe(u8, target_path);
    errdefer alloc.free(path);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try out.writer.print("{f}", .{std.json.fmt(value, .{})});
    const value_json = try out.toOwnedSlice();
    errdefer alloc.free(value_json);
    try operations.append(alloc, .{
        .op = .set,
        .path = path,
        .value_json = value_json,
    });
}

fn cloneDocumentConflictOperationAlloc(alloc: std.mem.Allocator, op: db_mod.types.TransformOp) !db_mod.types.TransformOp {
    const path = try alloc.dupe(u8, op.path);
    errdefer alloc.free(path);
    const value_json = if (op.value_json) |value| try alloc.dupe(u8, value) else null;
    errdefer if (value_json) |value| alloc.free(value);
    return .{
        .op = op.op,
        .path = path,
        .value_json = value_json,
    };
}

fn documentConflictPredicateGroupPassesAlloc(
    alloc: std.mem.Allocator,
    existing_doc: std.json.Value,
    proposed_doc: std.json.Value,
    group: db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (group.conditions) |condition| {
        if (!(try relational_rows.expressionConditionMatchesWithSources(alloc, existing_doc, proposed_doc, null, condition, 0))) return false;
    }
    return true;
}

fn documentConflictGuardPassesAlloc(
    alloc: std.mem.Allocator,
    existing_doc: std.json.Value,
    proposed_doc: std.json.Value,
    lowered: sql_plan.LoweredDocumentConflictWrite,
) !bool {
    if (lowered.where_expression) |condition| {
        if (!(try relational_rows.expressionConditionMatchesWithSources(alloc, existing_doc, proposed_doc, null, condition, 0))) return false;
    }
    for (lowered.where_expressions) |condition| {
        if (!(try relational_rows.expressionConditionMatchesWithSources(alloc, existing_doc, proposed_doc, null, condition, 0))) return false;
    }
    if (lowered.where_any.len != 0) {
        var matched_any = false;
        for (lowered.where_any) |group| {
            if (try documentConflictPredicateGroupPassesAlloc(alloc, existing_doc, proposed_doc, group)) {
                matched_any = true;
                break;
            }
        }
        if (!matched_any) return false;
    }
    for (lowered.where_not) |group| {
        if (try documentConflictPredicateGroupPassesAlloc(alloc, existing_doc, proposed_doc, group)) return false;
    }
    return true;
}

fn appendDocumentConflictExpressionAssignmentOpsAlloc(
    alloc: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
    existing_doc: std.json.Value,
    proposed_doc: std.json.Value,
    assignments: []const db_mod.types.RelationalRowsExpressionAssignment,
) !void {
    for (assignments) |assignment| {
        const value_json = try relational_rows.expressionValueJsonWithSourcesAlloc(alloc, existing_doc, proposed_doc, null, assignment.expression, 0);
        errdefer alloc.free(value_json);
        const path = try alloc.dupe(u8, assignment.field);
        errdefer alloc.free(path);
        try operations.append(alloc, .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        });
    }
}

const DocumentConflictExistingRow = struct {
    key: []const u8,
    lookup: LookupResponse,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.key));
        self.lookup.deinit(alloc);
        self.* = undefined;
    }
};

fn documentConflictTargetValueJsonAlloc(
    alloc: std.mem.Allocator,
    doc_json: []const u8,
    target: sql_plan.DocumentConflictUniqueTarget,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const value = (try documentConflictProjectedValue(parsed.value, target.path)) orelse return error.InvalidRowsRequest;
    if (value == .null) return error.InvalidRowsRequest;
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn documentConflictUniqueExistingRowAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    table_name: []const u8,
    proposed: db_mod.types.BatchWrite,
    target: sql_plan.DocumentConflictUniqueTarget,
    consistency: raft_mod.ReadConsistency,
) !?DocumentConflictExistingRow {
    const value_json = try documentConflictTargetValueJsonAlloc(alloc, proposed.value, target);
    defer alloc.free(value_json);
    const filter_json = try documentJoinedSourceLookupFilterJsonAlloc(alloc, target.path, value_json);
    defer alloc.free(filter_json);
    const query = sql_adapter.DocumentIndexQuery{
        .filter_query_json = filter_json,
        .max_candidate_rows = 2,
    };
    var response = (try documentSqlIndexQueryAlloc(alloc, source, table_name, table_name, query, documentSqlIndexedCandidateProbeLimit(2), false, false, consistency)) orelse return error.UnsupportedOperation;
    defer response.deinit(alloc);
    const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, response.json);
    if (total_hits == 0) return null;
    if (total_hits > 1) return error.DocumentSqlWriteDuplicateSource;

    var ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (ids.items) |id| alloc.free(@constCast(id));
        ids.deinit(alloc);
    }
    try appendDocumentCandidateIdsFromQueryResponseAlloc(
        alloc,
        source,
        table_name,
        table_name,
        response.json,
        null,
        consistency,
        &ids,
    );
    if (ids.items.len == 0) return null;
    if (ids.items.len > 1) return error.DocumentSqlWriteDuplicateSource;
    const key = try alloc.dupe(u8, ids.items[0]);
    errdefer alloc.free(key);
    const lookup = (try documentSqlLookupTableAlloc(alloc, source, table_name, key, consistency)) orelse return null;
    return .{
        .key = key,
        .lookup = lookup,
    };
}

fn documentConflictExistingRowAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentConflictWrite,
    proposed: db_mod.types.BatchWrite,
    consistency: raft_mod.ReadConsistency,
) !?DocumentConflictExistingRow {
    switch (lowered.target) {
        .identity => {
            const lookup = (try documentSqlLookupTableAlloc(alloc, source, lowered.table_name, proposed.key, consistency)) orelse return null;
            const key = try alloc.dupe(u8, proposed.key);
            errdefer alloc.free(key);
            return .{
                .key = key,
                .lookup = lookup,
            };
        },
        .unique_field => |target| return try documentConflictUniqueExistingRowAlloc(
            alloc,
            source,
            lowered.table_name,
            proposed,
            target,
            consistency,
        ),
    }
}

fn documentConflictReturningRowFromValueAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc: std.json.Value,
    fields: []const sql_plan.DocumentWriteReturningField,
    version: ?u64,
) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    for (fields, 0..) |field, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(field.output, .{})});
        switch (field.kind) {
            .identity => try writer.print("{f}", .{std.json.fmt(key, .{})}),
            .document => try writer.print("{f}", .{std.json.fmt(doc, .{})}),
            .version => {
                if (version) |value| {
                    try writer.print("{d}", .{value});
                } else {
                    try writer.writeAll("null");
                }
            },
            .projection => {
                if (try documentConflictProjectedValue(doc, field.path)) |value| {
                    try writer.print("{f}", .{std.json.fmt(value, .{})});
                } else {
                    try writer.writeAll("null");
                }
            },
        }
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn documentConflictReturningRowFromJsonAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    fields: []const sql_plan.DocumentWriteReturningField,
    version: ?u64,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return try documentConflictReturningRowFromValueAlloc(alloc, key, parsed.value, fields, version);
}

fn documentConflictPutObjectFieldAlloc(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: *std.json.Value,
) !void {
    if (object.getPtr(key)) |existing| {
        json_helpers.deinitJsonValue(alloc, existing);
        existing.* = value.*;
        value.* = undefined;
        return;
    }
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try object.put(alloc, owned_key, value.*);
    value.* = undefined;
}

fn documentConflictApplySetValueAlloc(
    alloc: std.mem.Allocator,
    doc: *std.json.Value,
    path: []const u8,
    depth: usize,
    value: *std.json.Value,
) !void {
    if (doc.* != .object) return error.InvalidRowsRequest;
    const segment = try documentConflictPathSegment(path, depth);
    const last = depth + 1 == try documentConflictPathSegmentCount(path);
    if (last) return try documentConflictPutObjectFieldAlloc(alloc, &doc.object, segment, value);

    if (doc.object.getPtr(segment)) |child| {
        if (child.* != .object) return error.InvalidRowsRequest;
        return try documentConflictApplySetValueAlloc(alloc, child, path, depth + 1, value);
    }

    var nested = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &nested);
    try documentConflictPutObjectFieldAlloc(alloc, &doc.object, segment, &nested);
    const child = doc.object.getPtr(segment) orelse return error.InvalidRowsRequest;
    return try documentConflictApplySetValueAlloc(alloc, child, path, depth + 1, value);
}

fn documentConflictReturningRowAfterOperationsAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    operations: []const db_mod.types.TransformOp,
    fields: []const sql_plan.DocumentWriteReturningField,
    version: ?u64,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    for (operations) |op| {
        if (op.op != .set) return error.InvalidRowsRequest;
        const value_json = op.value_json orelse return error.InvalidRowsRequest;
        var parsed_value = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed_value.deinit();
        var cloned = try json_helpers.cloneJsonValue(alloc, parsed_value.value);
        errdefer json_helpers.deinitJsonValue(alloc, &cloned);
        try documentConflictApplySetValueAlloc(alloc, &parsed.value, op.path, 0, &cloned);
    }
    return try documentConflictReturningRowFromValueAlloc(alloc, key, parsed.value, fields, version);
}

pub fn materializeDocumentConflictWriteBatchAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_plan.LoweredDocumentConflictWrite,
    consistency: raft_mod.ReadConsistency,
) !sql_plan.OwnedDocumentBatchRequest {
    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    var transforms = std.ArrayListUnmanaged(db_mod.types.DocumentTransform).empty;
    var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_keys = std.ArrayListUnmanaged([]const u8).empty;
    var returning_version_prewrite = std.ArrayListUnmanaged(u64).empty;
    var returning_version_outputs: ?[][]const u8 = try documentReturningVersionOutputsAlloc(alloc, lowered.returning_fields);
    errdefer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
        for (transforms.items) |transform| {
            alloc.free(@constCast(transform.key));
            freeDocumentMutationTemplateOps(alloc, transform.operations);
        }
        transforms.deinit(alloc);
        for (predicates.items) |predicate| alloc.free(@constCast(predicate.key));
        predicates.deinit(alloc);
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
        freeDocumentReturningVersionKeyState(alloc, &returning_version_keys, returning_version_outputs);
        returning_version_prewrite.deinit(alloc);
    }

    for (lowered.proposed_writes) |proposed| {
        var existing = try documentConflictExistingRowAlloc(alloc, source, lowered, proposed, consistency);
        defer if (existing) |*value| value.deinit(alloc);
        if (existing == null) {
            const key = try alloc.dupe(u8, proposed.key);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, proposed.value);
            errdefer alloc.free(value);
            try writes.append(alloc, .{ .key = key, .value = value });
            const predicate_key = try alloc.dupe(u8, proposed.key);
            errdefer alloc.free(predicate_key);
            try predicates.append(alloc, .{ .key = predicate_key, .expected_version = 0 });
            if (lowered.returning_fields.len != 0) {
                const row = try documentConflictReturningRowFromJsonAlloc(alloc, proposed.key, proposed.value, lowered.returning_fields, null);
                errdefer alloc.free(@constCast(row));
                try returning_rows.append(alloc, row);
                try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, proposed.key, 0, lowered.returning_fields);
            }
            continue;
        }

        if (lowered.action == .nothing) continue;
        var parsed_existing = std.json.parseFromSlice(std.json.Value, alloc, existing.?.lookup.json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed_existing.deinit();
        if (parsed_existing.value != .object) return error.InvalidRowsRequest;
        var parsed_proposed = std.json.parseFromSlice(std.json.Value, alloc, proposed.value, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed_proposed.deinit();
        if (parsed_proposed.value != .object) return error.InvalidRowsRequest;

        if (!try documentConflictGuardPassesAlloc(alloc, parsed_existing.value, parsed_proposed.value, lowered)) continue;
        var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
        errdefer {
            freeDocumentMutationTemplateOps(alloc, operations.items);
            operations.deinit(alloc);
        }
        for (lowered.operations) |op| {
            try operations.append(alloc, try cloneDocumentConflictOperationAlloc(alloc, op));
        }
        if (lowered.source_assignments.len != 0) {
            for (lowered.source_assignments) |assignment| {
                const value = (try documentConflictProjectedValue(parsed_proposed.value, assignment.source_path)) orelse @as(std.json.Value, .{ .null = {} });
                try appendDocumentConflictOperationFromProjectedValueAlloc(alloc, &operations, assignment.target_path, value);
            }
        }
        try appendDocumentConflictExpressionAssignmentOpsAlloc(alloc, &operations, parsed_existing.value, parsed_proposed.value, lowered.expression_assignments);
        const operations_slice = try operations.toOwnedSlice(alloc);
        operations = .empty;
        errdefer freeDocumentMutationTemplateOps(alloc, operations_slice);
        const key = try alloc.dupe(u8, existing.?.key);
        errdefer alloc.free(key);
        if (lowered.returning_fields.len != 0) {
            const row = try documentConflictReturningRowAfterOperationsAlloc(alloc, existing.?.key, existing.?.lookup.json, operations_slice, lowered.returning_fields, null);
            errdefer alloc.free(@constCast(row));
            try returning_rows.append(alloc, row);
            try appendDocumentReturningVersionKeyAlloc(alloc, &returning_version_keys, &returning_version_prewrite, existing.?.key, 0, lowered.returning_fields);
        }
        try transforms.append(alloc, .{
            .key = key,
            .operations = operations_slice,
        });
        const predicate_key = try alloc.dupe(u8, existing.?.key);
        errdefer alloc.free(predicate_key);
        try predicates.append(alloc, .{ .key = predicate_key, .expected_version = existing.?.lookup.version });
    }

    const write_slice = try writes.toOwnedSlice(alloc);
    writes = .empty;
    errdefer {
        for (write_slice) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (write_slice.len > 0) alloc.free(write_slice);
    }
    const transform_slice = try transforms.toOwnedSlice(alloc);
    transforms = .empty;
    errdefer {
        for (transform_slice) |transform| {
            alloc.free(@constCast(transform.key));
            freeDocumentMutationTemplateOps(alloc, transform.operations);
        }
        if (transform_slice.len > 0) alloc.free(transform_slice);
    }
    const predicate_slice = try predicates.toOwnedSlice(alloc);
    predicates = .empty;
    errdefer {
        for (predicate_slice) |predicate| alloc.free(@constCast(predicate.key));
        if (predicate_slice.len > 0) alloc.free(predicate_slice);
    }
    const returning_rows_slice = try returning_rows.toOwnedSlice(alloc);
    returning_rows = .empty;
    errdefer {
        for (returning_rows_slice) |row| alloc.free(@constCast(row));
        if (returning_rows_slice.len > 0) alloc.free(returning_rows_slice);
    }
    const returning_version_keys_slice = try returning_version_keys.toOwnedSlice(alloc);
    returning_version_keys = .empty;
    errdefer {
        for (returning_version_keys_slice) |key| alloc.free(@constCast(key));
        if (returning_version_keys_slice.len > 0) alloc.free(returning_version_keys_slice);
    }
    const returning_version_prewrite_slice = try returning_version_prewrite.toOwnedSlice(alloc);
    returning_version_prewrite = .empty;
    errdefer if (returning_version_prewrite_slice.len > 0) alloc.free(returning_version_prewrite_slice);
    var returning_version_outputs_slice: [][]const u8 = &.{};
    if (returning_version_outputs) |outputs| returning_version_outputs_slice = outputs;
    returning_version_outputs = null;
    errdefer {
        for (returning_version_outputs_slice) |output| alloc.free(@constCast(output));
        if (returning_version_outputs_slice.len > 0) alloc.free(returning_version_outputs_slice);
    }

    return .{
        .writes = write_slice,
        .transforms = transform_slice,
        .predicates = predicate_slice,
        .returning_rows = returning_rows_slice,
        .returning_version_keys = returning_version_keys_slice,
        .returning_version_prewrite = returning_version_prewrite_slice,
        .returning_version_outputs = returning_version_outputs_slice,
        .req = .{
            .writes = write_slice,
            .transforms = transform_slice,
            .predicates = predicate_slice,
            .sync_level = lowered.sync_level,
            .write_mode = .upsert,
        },
        .inserted = @intCast(write_slice.len),
        .transformed = @intCast(transform_slice.len),
    };
}

fn documentSqlAdmitBoundedScanPayload(
    scan_plan: sql_adapter.BoundedDocumentScan,
    payload: []const u8,
) !void {
    const max_bytes = scan_plan.max_bytes orelse return;
    if (payload.len > max_bytes) return error.DocumentSqlBoundedScanByteCapExceeded;
}

fn documentSqlBoundedScanProbeLimit(max_rows: u32) u32 {
    if (max_rows == std.math.maxInt(u32)) return max_rows;
    return max_rows + 1;
}

fn documentSqlIndexedCandidateProbeLimit(max_rows: u32) u32 {
    return @max(max_rows, source_binding.default_document_sql_bounded_scan_rows);
}

fn documentSqlAdmitBoundedRowCount(row_count: u32, max_rows: u32) !void {
    if (row_count > max_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
}

fn documentSqlAdmitBoundedRowProbeCount(row_count: u32, max_rows: u32) !void {
    if (row_count >= max_rows) return error.DocumentSqlBoundedScanRowCapExceeded;
}

pub fn executeAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    lowered: sql_adapter.DocumentAlgebraicAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?RowsAggregateResult {
    const native_table_name = source.native_table_name;
    const public_table_name = source.public_table_name;

    if (lowered.index_name != null or lowered.materialization_name != null) {
        return try executeLoweredDocumentSqlAlgebraicAggregatePlanAlloc(
            alloc,
            source,
            native_table_name,
            lowered,
            consistency,
        );
    }

    if (lowered.group_by) |group_by| {
        return switch (lowered.aggregate.op) {
            .count => try executeLoweredDocumentSqlGroupedCountAggregatePlanAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                lowered,
                group_by,
                consistency,
            ),
            .sum, .avg, .min, .max => try executeLoweredDocumentSqlGroupedNumericAggregatePlanAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                lowered,
                group_by,
                consistency,
            ),
        };
    }

    if (lowered.aggregate.op != .count) {
        return try executeLoweredDocumentSqlNumericAggregatePlanAlloc(
            alloc,
            source,
            native_table_name,
            public_table_name,
            lowered,
            consistency,
        );
    }

    const count: u32 = if (lowered.candidate_producer) |producer| switch (producer) {
        .id_lookup => |lookup_plan| blk: {
            var total: u32 = 0;
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                total += 1;
            }
            break :blk total;
        },
        .indexed_query => |query| blk: {
            const query_limit = query.max_candidate_rows;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            if (query.residual_filter_json == null) break :blk try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            const max_candidate_rows = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowCount(total_hits, max_candidate_rows);
            var rows = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (rows.items) |row| alloc.free(@constCast(row));
                rows.deinit(alloc);
            }
            try appendDocumentSqlFullRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, consistency, &rows);
            var total: u32 = 0;
            for (rows.items) |row| {
                if (!try residualFilterMatchesAlloc(alloc, row, query.residual_filter_json.?)) continue;
                total += 1;
            }
            break :blk total;
        },
        .bounded_scan => |scan_plan| blk: {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = documentSqlBoundedScanProbeLimit(scan_plan.max_rows),
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var total: u32 = 0;
            var scanned_docs: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                scanned_docs += 1;
                try documentSqlAdmitBoundedRowCount(scanned_docs, scan_plan.max_rows);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                total += 1;
            }
            break :blk total;
        },
    } else return error.UnsupportedSqlShape;

    return try documentSqlCountAggregateResultAlloc(alloc, lowered.aggregate.output, count);
}

fn executeLoweredDocumentSqlAlgebraicAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    lowered: sql_adapter.DocumentAlgebraicAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?RowsAggregateResult {
    const index_name = lowered.index_name orelse return error.DocumentSqlIndexUnavailable;
    const materialization_name = lowered.materialization_name orelse return error.DocumentSqlIndexUnavailable;
    if (lowered.candidate_producer != null or lowered.filter_query_json != null) return error.DocumentSqlIndexUnavailable;

    const req: AlgebraicAggregateRequest = .{
        .index_name = index_name,
        .materialization_name = materialization_name,
        .aggregate_op = lowered.aggregate.op,
        .group_by = lowered.group_by,
        .limit = if (lowered.order_by == null and lowered.having.len == 0) lowered.limit else null,
    };
    var response = if (source.algebraicAggregateCatalog(alloc, req, consistency)) |result|
        result orelse return null
    else |err| switch (err) {
        error.UnsupportedOperation => (try source.algebraicAggregate(alloc, native_table_name, req, consistency)) orelse return null,
        else => return err,
    };
    defer response.deinit(alloc);

    if (lowered.group_by) |group_by| {
        return try documentSqlMaterializedGroupedAggregateResultAlloc(
            alloc,
            group_by.output,
            lowered.aggregate.output,
            response.rows,
            response.total_groups,
            lowered.having,
            lowered.order_by,
            lowered.limit,
        );
    }

    const value_json = if (response.rows.len > 0) response.rows[0].value_json else switch (lowered.aggregate.op) {
        .count => "0",
        .sum, .avg, .min, .max => "null",
    };
    return try documentSqlMaterializedScalarAggregateResultAlloc(alloc, lowered.aggregate.output, value_json);
}

const DocumentSqlNumericAggregate = struct {
    value: f64 = 0,
    count: u32 = 0,
    seen: bool = false,
};

fn executeLoweredDocumentSqlNumericAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentAlgebraicAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?RowsAggregateResult {
    const input = lowered.aggregate.input orelse return error.UnsupportedSqlShape;
    if (input.field_type != .numeric) return error.UnsupportedSqlShape;
    var aggregate = DocumentSqlNumericAggregate{};

    if (lowered.candidate_producer) |producer| switch (producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateFromDocJsonAlloc(alloc, &aggregate, lowered.aggregate.op, input.field, lookup.json);
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowProbeCount(total_hits, query_limit);
            var rows = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (rows.items) |row| alloc.free(@constCast(row));
                rows.deinit(alloc);
            }
            try appendDocumentSqlFullRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, consistency, &rows);
            for (rows.items) |row| {
                if (query.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, row, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateFromDocJsonAlloc(alloc, &aggregate, lowered.aggregate.op, input.field, row);
            }
        },
        .bounded_scan => |scan_plan| {
            native_match_all: {
                if (try documentSqlIndexQueryAlloc(
                    alloc,
                    source,
                    native_table_name,
                    public_table_name,
                    .{ .filter_query_json = "{\"match_all\":{}}", .max_candidate_rows = scan_plan.max_rows },
                    scan_plan.max_rows,
                    false,
                    false,
                    consistency,
                )) |query_response| {
                    var response = query_response;
                    defer response.deinit(alloc);
                    const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, response.json);
                    if (total_hits > scan_plan.max_rows) break :native_match_all;
                    var candidate_rows = std.ArrayListUnmanaged([]const u8).empty;
                    defer {
                        for (candidate_rows.items) |row| alloc.free(@constCast(row));
                        candidate_rows.deinit(alloc);
                    }
                    try appendDocumentSqlFullRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, response.json, consistency, &candidate_rows);
                    for (candidate_rows.items) |row| {
                        if (scan_plan.residual_filter_json) |filter| {
                            if (!try residualFilterMatchesAlloc(alloc, row, filter)) continue;
                        }
                        try appendDocumentSqlNumericAggregateFromDocJsonAlloc(alloc, &aggregate, lowered.aggregate.op, input.field, row);
                    }
                    return try documentSqlNumericAggregateResultAlloc(alloc, lowered.aggregate.output, lowered.aggregate.op, aggregate);
                }
            }

            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = documentSqlBoundedScanProbeLimit(scan_plan.max_rows),
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned_docs: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                scanned_docs += 1;
                try documentSqlAdmitBoundedRowCount(scanned_docs, scan_plan.max_rows);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateFromDocJsonAlloc(alloc, &aggregate, lowered.aggregate.op, input.field, lookup.json);
            }
        },
    } else return error.DocumentSqlRequiresBoundedScan;

    return try documentSqlNumericAggregateResultAlloc(alloc, lowered.aggregate.output, lowered.aggregate.op, aggregate);
}

fn executeLoweredDocumentSqlGroupedCountAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentAlgebraicAggregatePlan,
    group_by: sql_adapter.DocumentAggregateGroupBy,
    consistency: raft_mod.ReadConsistency,
) !?RowsAggregateResult {
    if (lowered.candidate_producer == null) return error.UnsupportedSqlShape;
    var groups = std.ArrayListUnmanaged(DocumentSqlCountGroup).empty;
    defer {
        for (groups.items) |*group| group.deinit(alloc);
        groups.deinit(alloc);
    }

    switch (lowered.candidate_producer.?) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlCountGroupFromDocJsonAlloc(alloc, &groups, group_by, lookup.json);
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            if (query.residual_filter_json != null) {
                const max_candidate_rows = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
                const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
                try documentSqlAdmitBoundedRowCount(total_hits, max_candidate_rows);
            }
            var rows = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (rows.items) |row| alloc.free(@constCast(row));
                rows.deinit(alloc);
            }
            try appendDocumentSqlFullRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, consistency, &rows);
            for (rows.items) |row| {
                if (query.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, row, filter)) continue;
                }
                try appendDocumentSqlCountGroupFromDocJsonAlloc(alloc, &groups, group_by, row);
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = documentSqlBoundedScanProbeLimit(scan_plan.max_rows),
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned_docs: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                scanned_docs += 1;
                try documentSqlAdmitBoundedRowCount(scanned_docs, scan_plan.max_rows);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlCountGroupFromDocJsonAlloc(alloc, &groups, group_by, lookup.json);
            }
        },
    }

    return try documentSqlGroupedCountAggregateResultAlloc(alloc, group_by.output, lowered.aggregate.output, groups.items, lowered.having, lowered.order_by, lowered.limit);
}

fn executeLoweredDocumentSqlGroupedNumericAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentAlgebraicAggregatePlan,
    group_by: sql_adapter.DocumentAggregateGroupBy,
    consistency: raft_mod.ReadConsistency,
) !?RowsAggregateResult {
    const input = lowered.aggregate.input orelse return error.UnsupportedSqlShape;
    if (input.field_type != .numeric) return error.UnsupportedSqlShape;
    if (lowered.candidate_producer == null) return error.DocumentSqlRequiresBoundedScan;
    var groups = std.ArrayListUnmanaged(DocumentSqlNumericAggregateGroup).empty;
    defer {
        for (groups.items) |*group| group.deinit(alloc);
        groups.deinit(alloc);
    }

    switch (lowered.candidate_producer.?) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateGroupFromDocJsonAlloc(alloc, &groups, group_by, input, lowered.aggregate.op, lookup.json);
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowProbeCount(total_hits, query_limit);
            var rows = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (rows.items) |row| alloc.free(@constCast(row));
                rows.deinit(alloc);
            }
            try appendDocumentSqlFullRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, consistency, &rows);
            for (rows.items) |row| {
                if (query.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, row, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateGroupFromDocJsonAlloc(alloc, &groups, group_by, input, lowered.aggregate.op, row);
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = documentSqlBoundedScanProbeLimit(scan_plan.max_rows),
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned_docs: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                scanned_docs += 1;
                try documentSqlAdmitBoundedRowCount(scanned_docs, scan_plan.max_rows);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlNumericAggregateGroupFromDocJsonAlloc(alloc, &groups, group_by, input, lowered.aggregate.op, lookup.json);
            }
        },
    }

    return try documentSqlGroupedNumericAggregateResultAlloc(alloc, group_by.output, lowered.aggregate.output, lowered.aggregate.op, groups.items, lowered.having, lowered.order_by, lowered.limit);
}

const DocumentSqlCountGroup = struct {
    key_json: []const u8,
    count: u32,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.key_json));
        self.* = undefined;
    }
};

fn appendDocumentSqlCountGroupFromDocJsonAlloc(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(DocumentSqlCountGroup),
    group_by: sql_adapter.DocumentAggregateGroupBy,
    doc_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const group_value = documentSqlProjectedValue(parsed.value, group_by.field) orelse std.json.Value{ .null = {} };
    const key_json = try std.json.Stringify.valueAlloc(alloc, group_value, .{});
    errdefer alloc.free(key_json);
    for (groups.items) |*group| {
        if (std.mem.eql(u8, group.key_json, key_json)) {
            group.count += 1;
            alloc.free(key_json);
            return;
        }
    }
    try groups.append(alloc, .{
        .key_json = key_json,
        .count = 1,
    });
}

const DocumentSqlNumericAggregateGroup = struct {
    key_json: []const u8,
    aggregate: DocumentSqlNumericAggregate = .{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.key_json));
        self.* = undefined;
    }
};

fn appendDocumentSqlNumericAggregateGroupFromDocJsonAlloc(
    alloc: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(DocumentSqlNumericAggregateGroup),
    group_by: sql_adapter.DocumentAggregateGroupBy,
    input: sql_adapter.DocumentAggregateInput,
    op: sql_adapter.DocumentAggregateOp,
    doc_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const group_value = documentSqlProjectedValue(parsed.value, group_by.field) orelse std.json.Value{ .null = {} };
    const key_json = try std.json.Stringify.valueAlloc(alloc, group_value, .{});
    errdefer alloc.free(key_json);
    const value = documentSqlProjectedValue(parsed.value, input.field);

    for (groups.items) |*group| {
        if (std.mem.eql(u8, group.key_json, key_json)) {
            alloc.free(key_json);
            if (value) |item| try appendDocumentSqlNumericAggregateValue(&group.aggregate, op, item);
            return;
        }
    }

    var aggregate = DocumentSqlNumericAggregate{};
    if (value) |item| try appendDocumentSqlNumericAggregateValue(&aggregate, op, item);
    try groups.append(alloc, .{
        .key_json = key_json,
        .aggregate = aggregate,
    });
}

const DocumentSqlSortKey = union(enum) {
    null,
    string: []u8,
    number: f64,
    boolean: bool,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .string => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

const OrderedDocumentSqlCandidate = struct {
    id: []u8,
    doc_json: []u8,
    lateral_branch_doc_json: ?[]u8 = null,
    sort_key: DocumentSqlSortKey,
    lateral_matched: bool = true,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.id);
        alloc.free(self.doc_json);
        if (self.lateral_branch_doc_json) |json| alloc.free(json);
        self.sort_key.deinit(alloc);
        self.* = undefined;
    }
};

const DocumentSqlLateralMatch = struct {
    matched: bool = true,
    branch_doc_json: ?[]const u8 = null,

    fn deinitOwned(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.branch_doc_json) |json| alloc.free(@constCast(json));
        self.* = undefined;
    }
};

const DocumentSqlBranchCandidate = struct {
    id: []u8,
    doc_json: []u8,
    sort_keys: []DocumentSqlSortKey,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.id);
        alloc.free(self.doc_json);
        for (self.sort_keys) |*sort_key| sort_key.deinit(alloc);
        if (self.sort_keys.len > 0) alloc.free(self.sort_keys);
        self.* = undefined;
    }
};

const OrderedDocumentSqlUnnestCandidate = struct {
    id: []u8,
    row_json: []const u8,
    sort_key: DocumentSqlSortKey,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.id);
        alloc.free(@constCast(self.row_json));
        self.sort_key.deinit(alloc);
        self.* = undefined;
    }
};

const DocumentSqlSortContext = struct {
    direction: sql_adapter.DocumentOrderDirection,
};

fn documentSqlCandidateLessThan(ctx: DocumentSqlSortContext, lhs: OrderedDocumentSqlCandidate, rhs: OrderedDocumentSqlCandidate) bool {
    const order = documentSqlSortKeyOrder(lhs.sort_key, rhs.sort_key);
    if (order == .eq) {
        return switch (ctx.direction) {
            .asc => std.mem.order(u8, lhs.id, rhs.id) == .lt,
            .desc => std.mem.order(u8, lhs.id, rhs.id) == .gt,
        };
    }
    return switch (ctx.direction) {
        .asc => order == .lt,
        .desc => order == .gt,
    };
}

fn documentSqlUnnestCandidateLessThan(ctx: DocumentSqlSortContext, lhs: OrderedDocumentSqlUnnestCandidate, rhs: OrderedDocumentSqlUnnestCandidate) bool {
    const order = documentSqlSortKeyOrder(lhs.sort_key, rhs.sort_key);
    if (order == .eq) {
        const id_order = std.mem.order(u8, lhs.id, rhs.id);
        if (id_order != .eq) {
            return switch (ctx.direction) {
                .asc => id_order == .lt,
                .desc => id_order == .gt,
            };
        }
        return switch (ctx.direction) {
            .asc => std.mem.order(u8, lhs.row_json, rhs.row_json) == .lt,
            .desc => std.mem.order(u8, lhs.row_json, rhs.row_json) == .gt,
        };
    }
    return switch (ctx.direction) {
        .asc => order == .lt,
        .desc => order == .gt,
    };
}

fn documentSqlSortKeyOrder(lhs: DocumentSqlSortKey, rhs: DocumentSqlSortKey) std.math.Order {
    switch (lhs) {
        .null => return if (rhs == .null) .eq else .gt,
        .string => |left| switch (rhs) {
            .null => return .lt,
            .string => |right| return std.mem.order(u8, left, right),
            else => return .lt,
        },
        .number => |left| switch (rhs) {
            .null => return .lt,
            .number => |right| return std.math.order(left, right),
            .string => return .gt,
            .boolean => return .lt,
        },
        .boolean => |left| switch (rhs) {
            .null => return .lt,
            .boolean => |right| return std.math.order(@intFromBool(left), @intFromBool(right)),
            else => return .gt,
        },
    }
}

fn executeOrderedLoweredDocumentSqlReadPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentReadPlan,
    order_by: sql_adapter.DocumentOrderBy,
    consistency: raft_mod.ReadConsistency,
) !?RowsQueryResult {
    var candidates = std.ArrayListUnmanaged(OrderedDocumentSqlCandidate).empty;
    errdefer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }

    switch (lowered.producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lowered.lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lowered.lateral_subquery.?.join_kind != .left) continue;
                try appendOrderedDocumentSqlCandidateAlloc(alloc, &candidates, id, lookup.json, order_by, lateral_match);
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = scan_plan.max_rows,
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                scanned += 1;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lowered.lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lowered.lateral_subquery.?.join_kind != .left) continue;
                try appendOrderedDocumentSqlCandidateAlloc(alloc, &candidates, key_value.string, lookup.json, order_by, lateral_match);
            }
            try documentSqlAdmitBoundedRowProbeCount(scanned, scan_plan.max_rows);
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowProbeCount(total_hits, query_limit);
            try appendOrderedDocumentSqlCandidatesFromQueryResponseAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                query_response.json,
                query.residual_filter_json,
                lowered.lateral_subquery,
                order_by,
                consistency,
                &candidates,
            );
        },
    }

    std.mem.sort(OrderedDocumentSqlCandidate, candidates.items, DocumentSqlSortContext{ .direction = order_by.direction }, documentSqlCandidateLessThan);

    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
    }
    const row_limit = lowered.limit orelse @as(u32, @intCast(candidates.items.len));
    for (candidates.items) |candidate| {
        if (rows.items.len >= row_limit) break;
        try rows.append(alloc, try documentSqlProjectedRowJsonWithLateralAlloc(alloc, candidate.id, candidate.doc_json, lowered.projection, .{
            .matched = candidate.lateral_matched,
            .branch_doc_json = candidate.lateral_branch_doc_json,
        }));
    }

    for (candidates.items) |*candidate| candidate.deinit(alloc);
    candidates.deinit(alloc);

    const total: u32 = @intCast(rows.items.len);
    return .{
        .rows = try rows.toOwnedSlice(alloc),
        .total = total,
    };
}

fn appendOrderedDocumentSqlCandidatesFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    residual_filter_json: ?[]const u8,
    lateral_subquery: ?sql_adapter.DocumentLateralSubquery,
    order_by: sql_adapter.DocumentOrderBy,
    consistency: raft_mod.ReadConsistency,
    candidates: *std.ArrayListUnmanaged(OrderedDocumentSqlCandidate),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (hit_value.object.get("_source")) |source_value| {
            if (source_value == .object) {
                const doc_json = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
                defer alloc.free(doc_json);
                if (residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, doc_json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, doc_json, lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lateral_subquery.?.join_kind != .left) continue;
                try appendOrderedDocumentSqlCandidateAlloc(alloc, candidates, id_value.string, doc_json, order_by, lateral_match);
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (residual_filter_json) |filter| {
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lateral_subquery, consistency);
        defer lateral_match.deinitOwned(alloc);
        if (!lateral_match.matched and lateral_subquery.?.join_kind != .left) continue;
        try appendOrderedDocumentSqlCandidateAlloc(alloc, candidates, id_value.string, lookup.json, order_by, lateral_match);
    }
}

fn appendOrderedDocumentSqlCandidateAlloc(
    alloc: std.mem.Allocator,
    candidates: *std.ArrayListUnmanaged(OrderedDocumentSqlCandidate),
    id: []const u8,
    doc_json: []const u8,
    order_by: sql_adapter.DocumentOrderBy,
    lateral_match: DocumentSqlLateralMatch,
) !void {
    const owned_id = try alloc.dupe(u8, id);
    errdefer alloc.free(owned_id);
    const owned_doc = try alloc.dupe(u8, doc_json);
    errdefer alloc.free(owned_doc);
    const owned_lateral_branch_doc = if (lateral_match.branch_doc_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (owned_lateral_branch_doc) |json| alloc.free(json);
    var sort_key = try documentSqlSortKeyAlloc(alloc, id, doc_json, order_by);
    errdefer sort_key.deinit(alloc);
    try candidates.append(alloc, .{
        .id = owned_id,
        .doc_json = owned_doc,
        .lateral_branch_doc_json = owned_lateral_branch_doc,
        .sort_key = sort_key,
        .lateral_matched = lateral_match.matched,
    });
}

fn documentSqlSortKeyAlloc(
    alloc: std.mem.Allocator,
    id: []const u8,
    doc_json: []const u8,
    order_by: sql_adapter.DocumentOrderBy,
) !DocumentSqlSortKey {
    if (std.mem.eql(u8, order_by.field, "_id")) return .{ .string = try alloc.dupe(u8, id) };
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const value = documentSqlProjectedValue(parsed.value, order_by.field) orelse return .null;
    return try documentSqlSortKeyFromValueAlloc(alloc, value, order_by.field_type);
}

fn documentSqlSortKeyFromValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    field_type: storage_schema.AntflyType,
) !DocumentSqlSortKey {
    if (value == .null) return .null;
    return switch (field_type) {
        .numeric => .{ .number = try documentSqlSortNumber(value) },
        .datetime => switch (value) {
            .integer, .float, .number_string => .{ .number = try documentSqlSortNumber(value) },
            .string => |text| .{ .string = try alloc.dupe(u8, text) },
            else => error.InvalidRowsRequest,
        },
        .boolean => switch (value) {
            .bool => |item| .{ .boolean = item },
            else => error.InvalidRowsRequest,
        },
        .keyword, .text, .search_as_you_type => switch (value) {
            .string => |text| .{ .string = try alloc.dupe(u8, text) },
            .integer, .float, .number_string => .{ .number = try documentSqlSortNumber(value) },
            .bool => |item| .{ .boolean = item },
            else => error.InvalidRowsRequest,
        },
        else => error.UnsupportedSqlShape,
    };
}

fn documentSqlSortNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |text| try std.fmt.parseFloat(f64, text),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlIndexQueryRequestAlloc(
    alloc: std.mem.Allocator,
    query: sql_adapter.DocumentIndexQuery,
    limit: ?u32,
    include_documents: bool,
    count_only: bool,
) !OwnedQueryRequest {
    if (query.native_query_json) |body| {
        const owned_body = try documentSqlNativeIndexQueryRequestBodyAlloc(alloc, body, limit, include_documents, count_only);
        errdefer alloc.free(owned_body);
        return .{
            .body_json = owned_body,
            .index_name = query.index_name,
        };
    }
    var out: std.ArrayListUnmanaged(u8) = .empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (query.full_text_query) |full_text| {
        try appendJsonFieldName(alloc, &out, &first, "full_text_search");
        try documentSqlAppendFullTextSearchAlloc(alloc, &out, full_text);
    } else if (query.filter_query_json != null) {
        try appendJsonFieldName(alloc, &out, &first, "full_text_search");
        try out.appendSlice(alloc, "{\"match_all\":{}}");
    }
    if (query.filter_query_json) |filter_json| {
        const native_filter_json = try documentSqlNativeFilterQueryJsonAlloc(alloc, filter_json);
        defer alloc.free(native_filter_json);
        try appendJsonFieldString(alloc, &out, &first, "_filter_query_json", native_filter_json);
    }
    if (limit) |value| {
        try appendJsonFieldU32(alloc, &out, &first, "limit", value);
    }
    if (include_documents) {
        try appendJsonFieldBool(alloc, &out, &first, "include_documents", true);
    }
    if (count_only) {
        try appendJsonFieldBool(alloc, &out, &first, "count_only", true);
    }
    try out.append(alloc, '}');
    const body = try out.toOwnedSlice(alloc);
    errdefer alloc.free(body);
    return .{
        .body_json = body,
        .index_name = query.index_name,
    };
}

fn documentSqlAppendFullTextSearchAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: []const u8,
) !void {
    if (documentSqlFullTextQueryLeadingField(query)) |field| {
        const text = std.mem.trim(u8, query[field.len + 1 ..], " \t\r\n");
        if (text.len > 0) {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, field);
            try out.append(alloc, '}');
            return;
        }
    }
    try out.appendSlice(alloc, "{\"query\":");
    try appendJsonString(alloc, out, query);
    try out.append(alloc, '}');
}

fn documentSqlFullTextQueryLeadingField(query: []const u8) ?[]const u8 {
    const colon = std.mem.indexOfScalar(u8, query, ':') orelse return null;
    if (colon == 0) return null;
    const field = std.mem.trim(u8, query[0..colon], " \t\r\n");
    if (field.len == 0 or field.len != colon) return null;
    for (field) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-' or ch == '.' or ch == '/')) return null;
    }
    return field;
}

fn documentSqlNativeIndexQueryRequestBodyAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    limit: ?u32,
    include_documents: bool,
    count_only: bool,
) ![]u8 {
    if (limit == null and !include_documents and !count_only) return try alloc.dupe(u8, body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    if (limit) |value| try putJsonObjectField(alloc, &parsed.value.object, "limit", .{ .integer = @intCast(value) });
    if (include_documents) try putJsonObjectField(alloc, &parsed.value.object, "include_documents", .{ .bool = true });
    if (count_only) try putJsonObjectField(alloc, &parsed.value.object, "count_only", .{ .bool = true });
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn putJsonObjectField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    name: []const u8,
    value: std.json.Value,
) !void {
    if (object.getPtr(name)) |slot| {
        slot.* = value;
        return;
    }
    try object.put(alloc, try alloc.dupe(u8, name), value);
}

fn documentSqlNativeFilterQueryJsonAlloc(
    alloc: std.mem.Allocator,
    filter_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{});
    defer parsed.deinit();

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    try documentSqlAppendNativeFilterValueAlloc(alloc, &out, parsed.value);
    return try out.toOwnedSlice(alloc);
}

fn documentSqlAppendNativeFilterValueAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
) anyerror!void {
    switch (value) {
        .object => |object| {
            if (try documentSqlAppendNativeFilterOperatorAlloc(alloc, out, object)) return;
            try out.append(alloc, '{');
            var first = true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (!first) try out.append(alloc, ',');
                first = false;

                const key = if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '/')
                    try documentSqlStorageFilterFieldAlloc(alloc, entry.key_ptr.*)
                else
                    try alloc.dupe(u8, entry.key_ptr.*);
                defer alloc.free(key);

                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');

                const field_identifier =
                    std.mem.eql(u8, entry.key_ptr.*, "field") or
                    std.mem.eql(u8, entry.key_ptr.*, "path");
                if (field_identifier and entry.value_ptr.* == .string) {
                    const field = try documentSqlStorageFilterFieldAlloc(alloc, entry.value_ptr.string);
                    defer alloc.free(field);
                    try appendJsonString(alloc, out, field);
                } else {
                    try documentSqlAppendNativeFilterValueAlloc(alloc, out, entry.value_ptr.*);
                }
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, i| {
                if (i > 0) try out.append(alloc, ',');
                try documentSqlAppendNativeFilterValueAlloc(alloc, out, item);
            }
            try out.append(alloc, ']');
        },
        else => {
            const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
}

fn documentSqlAppendNativeFilterOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    object: std.json.ObjectMap,
) anyerror!bool {
    if (object.count() != 1) return false;
    var it = object.iterator();
    const entry = it.next() orelse return false;
    const op = entry.key_ptr.*;
    if (std.mem.eql(u8, op, "conjuncts")) {
        try documentSqlAppendNativeBoolListOperatorAlloc(alloc, out, "must", entry.value_ptr.*);
        return true;
    }
    if (std.mem.eql(u8, op, "disjuncts")) {
        try documentSqlAppendNativeBoolListOperatorAlloc(alloc, out, "should", entry.value_ptr.*);
        return true;
    }
    if (entry.value_ptr.* != .object) return false;
    const spec = entry.value_ptr.object;

    if (std.mem.eql(u8, op, "term")) {
        if (spec.get("path") == null and spec.get("field") == null) {
            if (try documentSqlAppendNativeShorthandFieldValueOperatorAlloc(alloc, out, "term", spec)) return true;
            return false;
        }
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        const term_value = spec.get("value") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeFieldValueOperatorAlloc(alloc, out, "term", field, term_value);
        return true;
    }
    if (std.mem.eql(u8, op, "terms")) {
        if (spec.get("path") == null and spec.get("field") == null) {
            if (try documentSqlAppendNativeShorthandFieldValueOperatorAlloc(alloc, out, "terms", spec)) return true;
            return false;
        }
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        const terms_value = spec.get("values") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeFieldValueOperatorAlloc(alloc, out, "terms", field, terms_value);
        return true;
    }
    if (std.mem.eql(u8, op, "array_any")) {
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        const item_value = spec.get("value") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeExplicitFieldValueOperatorAlloc(alloc, out, "array_any", field, item_value);
        return true;
    }
    if (std.mem.eql(u8, op, "prefix")) {
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        const prefix_value = spec.get("value") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeFieldValueOperatorAlloc(alloc, out, "prefix", field, prefix_value);
        return true;
    }
    if (std.mem.eql(u8, op, "wildcard")) {
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        const pattern_value = spec.get("pattern") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeFieldValueOperatorAlloc(alloc, out, "wildcard", field, pattern_value);
        return true;
    }
    if (std.mem.eql(u8, op, "numeric_range") or std.mem.eql(u8, op, "date_range") or std.mem.eql(u8, op, "term_range")) {
        const path_value = spec.get("path") orelse spec.get("field") orelse return false;
        if (path_value != .string) return false;
        const field = try documentSqlStorageFilterFieldAlloc(alloc, path_value.string);
        defer alloc.free(field);
        try documentSqlAppendNativeRangeOperatorAlloc(alloc, out, op, field, spec);
        return true;
    }

    return false;
}

fn documentSqlAppendNativeShorthandFieldValueOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    op: []const u8,
    spec: std.json.ObjectMap,
) anyerror!bool {
    if (spec.count() != 1) return false;
    var it = spec.iterator();
    const entry = it.next() orelse return false;
    const field = try documentSqlStorageFilterFieldAlloc(alloc, entry.key_ptr.*);
    defer alloc.free(field);
    try documentSqlAppendNativeFieldValueOperatorAlloc(alloc, out, op, field, entry.value_ptr.*);
    return true;
}

fn documentSqlAppendNativeBoolListOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    bool_field: []const u8,
    value: std.json.Value,
) anyerror!void {
    try out.appendSlice(alloc, "{\"bool\":{");
    try appendJsonString(alloc, out, bool_field);
    try out.appendSlice(alloc, ":[");
    if (value == .array) {
        for (value.array.items, 0..) |item, i| {
            if (i > 0) try out.append(alloc, ',');
            try documentSqlAppendNativeFilterValueAlloc(alloc, out, item);
        }
    } else {
        try documentSqlAppendNativeFilterValueAlloc(alloc, out, value);
    }
    try out.append(alloc, ']');
    if (std.mem.eql(u8, bool_field, "should")) {
        try out.appendSlice(alloc, ",\"minimum_should_match\":1");
    }
    try out.appendSlice(alloc, "}}");
}

fn documentSqlAppendNativeFieldValueOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    op: []const u8,
    field: []const u8,
    value: std.json.Value,
) anyerror!void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, op);
    try out.appendSlice(alloc, ":{");
    try appendJsonString(alloc, out, field);
    try out.append(alloc, ':');
    try documentSqlAppendNativeFilterValueAlloc(alloc, out, value);
    try out.appendSlice(alloc, "}}");
}

fn documentSqlAppendNativeExplicitFieldValueOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    op: []const u8,
    field: []const u8,
    value: std.json.Value,
) anyerror!void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, op);
    try out.appendSlice(alloc, ":{");
    var first = true;
    try appendJsonFieldString(alloc, out, &first, "field", field);
    try appendJsonFieldName(alloc, out, &first, "value");
    try documentSqlAppendNativeFilterValueAlloc(alloc, out, value);
    try out.appendSlice(alloc, "}}");
}

fn documentSqlAppendNativeRangeOperatorAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    op: []const u8,
    field: []const u8,
    spec: std.json.ObjectMap,
) anyerror!void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, op);
    try out.appendSlice(alloc, ":{");
    var first = true;
    try appendJsonFieldString(alloc, out, &first, "field", field);
    var it = spec.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "path")) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try documentSqlAppendNativeFilterValueAlloc(alloc, out, entry.value_ptr.*);
    }
    try out.appendSlice(alloc, "}}");
}

fn documentSqlStorageFilterFieldAlloc(alloc: std.mem.Allocator, field: []const u8) ![]u8 {
    if (field.len == 0 or field[0] != '/') return try alloc.dupe(u8, field);
    if (field.len == 1) return error.InvalidRowsRequest;
    var out = try alloc.alloc(u8, field.len - 1);
    errdefer alloc.free(out);
    for (field[1..], 0..) |ch, i| {
        if (ch == '/') {
            if (i == 0 or i + 1 == out.len) return error.InvalidRowsRequest;
            out[i] = '.';
        } else {
            out[i] = ch;
        }
    }
    return out;
}

fn documentSqlLookupAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    key: []const u8,
    opts: LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    if (source.lookupCatalog(alloc, key, opts, consistency)) |result| {
        return result;
    } else |err| switch (err) {
        error.UnsupportedOperation => {},
        else => return err,
    }
    if (try source.lookup(alloc, native_table_name, key, opts, consistency)) |result| return result;
    if (std.mem.eql(u8, native_table_name, public_table_name)) return null;
    return try source.lookup(alloc, public_table_name, key, opts, consistency);
}

fn documentSqlScanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    if (source.scanCatalog(alloc, from_key, to_key, opts, consistency)) |result| {
        return result;
    } else |err| switch (err) {
        error.UnsupportedOperation => {},
        else => return err,
    }
    if (try source.scan(alloc, native_table_name, from_key, to_key, opts, consistency)) |result| return result;
    if (std.mem.eql(u8, native_table_name, public_table_name)) return null;
    return try source.scan(alloc, public_table_name, from_key, to_key, opts, consistency);
}

fn documentSqlIndexQueryAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    query: sql_adapter.DocumentIndexQuery,
    limit: ?u32,
    include_documents: bool,
    count_only: bool,
    consistency: raft_mod.ReadConsistency,
) !?QueryResponse {
    var query_req = try documentSqlIndexQueryRequestAlloc(alloc, query, limit, include_documents, count_only);
    defer query_req.deinit(alloc);
    if (source.queryCatalog(alloc, query_req.request(), consistency)) |result| {
        return result;
    } else |err| switch (err) {
        error.UnsupportedOperation => {},
        else => return err,
    }
    if (try source.query(alloc, native_table_name, query_req.request(), consistency)) |result| return result;
    if (std.mem.eql(u8, native_table_name, public_table_name)) return null;
    var fallback_req = try documentSqlIndexQueryRequestAlloc(alloc, query, limit, include_documents, count_only);
    defer fallback_req.deinit(alloc);
    return try source.query(alloc, public_table_name, fallback_req.request(), consistency);
}

fn documentSqlTotalHitsFromQueryResponse(alloc: std.mem.Allocator, response_json: []const u8) !u32 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const total_value = hits_value.object.get("total") orelse return error.InvalidRowsRequest;
    return switch (total_value) {
        .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.InvalidRowsRequest,
        .number_string => |text| try std.fmt.parseUnsigned(u32, text, 10),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlFirstSourceFromQueryResponseAlloc(alloc: std.mem.Allocator, response_json: []const u8) !?[]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;
    if (hit_items.array.items.len == 0) return null;
    const first_hit = hit_items.array.items[0];
    if (first_hit != .object) return error.InvalidRowsRequest;
    const source_value = first_hit.object.get("_source") orelse return error.InvalidRowsRequest;
    if (source_value == .null) return null;
    if (source_value != .object) return error.InvalidRowsRequest;
    return try std.json.Stringify.valueAlloc(alloc, source_value, .{});
}

fn documentSqlOrderedFirstSourceFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    response_json: []const u8,
    max_candidate_rows: u32,
    order_by: []const sql_adapter.DocumentOrderBy,
) !?[]const u8 {
    if (order_by.len == 0) return error.InvalidRowsRequest;
    try documentSqlAdmitBoundedRowCount(try documentSqlTotalHitsFromQueryResponse(alloc, response_json), max_candidate_rows);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;
    if (hit_items.array.items.len == 0) return null;

    var candidates = std.ArrayListUnmanaged(DocumentSqlBranchCandidate).empty;
    defer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }
    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        const source_value = hit_value.object.get("_source") orelse return error.InvalidRowsRequest;
        if (source_value != .object) return error.InvalidRowsRequest;
        var doc_json: ?[]u8 = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
        errdefer if (doc_json) |json| alloc.free(json);
        var owned_id: ?[]u8 = try alloc.dupe(u8, id_value.string);
        errdefer if (owned_id) |id| alloc.free(id);
        var sort_keys: ?[]DocumentSqlSortKey = try documentSqlSortKeysAlloc(alloc, id_value.string, doc_json.?, order_by);
        errdefer if (sort_keys) |keys| {
            for (keys) |*sort_key| sort_key.deinit(alloc);
            if (keys.len > 0) alloc.free(keys);
        };
        try candidates.append(alloc, .{
            .id = owned_id.?,
            .doc_json = doc_json.?,
            .sort_keys = sort_keys.?,
        });
        doc_json = null;
        owned_id = null;
        sort_keys = null;
    }
    if (candidates.items.len == 0) return null;
    std.mem.sort(DocumentSqlBranchCandidate, candidates.items, DocumentSqlBranchSortContext{ .order_by = order_by }, documentSqlBranchCandidateLessThan);
    const out = try alloc.dupe(u8, candidates.items[0].doc_json);
    return out;
}

const DocumentSqlBranchSortContext = struct {
    order_by: []const sql_adapter.DocumentOrderBy,
};

fn documentSqlBranchCandidateLessThan(ctx: DocumentSqlBranchSortContext, lhs: DocumentSqlBranchCandidate, rhs: DocumentSqlBranchCandidate) bool {
    for (ctx.order_by, 0..) |order_by, i| {
        const order = documentSqlSortKeyOrder(lhs.sort_keys[i], rhs.sort_keys[i]);
        if (order == .eq) continue;
        return switch (order_by.direction) {
            .asc => order == .lt,
            .desc => order == .gt,
        };
    }
    return std.mem.order(u8, lhs.id, rhs.id) == .lt;
}

fn documentSqlSortKeysAlloc(
    alloc: std.mem.Allocator,
    id: []const u8,
    doc_json: []const u8,
    order_by: []const sql_adapter.DocumentOrderBy,
) ![]DocumentSqlSortKey {
    const sort_keys = try alloc.alloc(DocumentSqlSortKey, order_by.len);
    errdefer alloc.free(sort_keys);
    var initialized: usize = 0;
    errdefer {
        for (sort_keys[0..initialized]) |*sort_key| sort_key.deinit(alloc);
    }
    for (order_by, 0..) |order, i| {
        sort_keys[i] = try documentSqlSortKeyAlloc(alloc, id, doc_json, order);
        initialized += 1;
    }
    return sort_keys;
}

fn documentSqlCountAggregateResultAlloc(
    alloc: std.mem.Allocator,
    output: []const u8,
    count: u32,
) !RowsAggregateResult {
    var row: std.Io.Writer.Allocating = .init(alloc);
    errdefer row.deinit();
    const writer = &row.writer;
    try writer.print("{{{f}:{d}}}", .{ std.json.fmt(output, .{}), count });
    const rows = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(rows);
    rows[0] = try row.toOwnedSlice();
    return .{
        .rows = rows,
        .total_groups = 1,
    };
}

fn documentSqlNumericAggregateResultAlloc(
    alloc: std.mem.Allocator,
    output: []const u8,
    op: sql_adapter.DocumentAggregateOp,
    aggregate: DocumentSqlNumericAggregate,
) !RowsAggregateResult {
    var row: std.Io.Writer.Allocating = .init(alloc);
    errdefer row.deinit();
    const writer = &row.writer;
    try writer.print("{{{f}:", .{std.json.fmt(output, .{})});
    if (aggregate.seen) {
        try writer.print("{d}", .{documentSqlNumericAggregateResultValue(aggregate, op)});
    } else {
        try writer.writeAll("null");
    }
    try writer.writeByte('}');
    const rows = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(rows);
    rows[0] = try row.toOwnedSlice();
    return .{
        .rows = rows,
        .total_groups = 1,
    };
}

const DocumentSqlCountGroupSortContext = struct {
    order_by: sql_adapter.DocumentAggregateOrderBy,
};

const DocumentSqlNumericAggregateGroupSortContext = struct {
    op: sql_adapter.DocumentAggregateOp,
    order_by: sql_adapter.DocumentAggregateOrderBy,
};

const DocumentSqlMaterializedAggregateSortContext = struct {
    order_by: sql_adapter.DocumentAggregateOrderBy,
};

fn filteredDocumentSqlCountGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []const DocumentSqlCountGroup,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
) ![]DocumentSqlCountGroup {
    var out = std.ArrayListUnmanaged(DocumentSqlCountGroup).empty;
    errdefer out.deinit(alloc);
    for (groups) |group| {
        if (documentSqlCountGroupMatchesHaving(group, having)) try out.append(alloc, group);
    }
    return try out.toOwnedSlice(alloc);
}

fn filteredDocumentSqlNumericAggregateGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []const DocumentSqlNumericAggregateGroup,
    op: sql_adapter.DocumentAggregateOp,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
) ![]DocumentSqlNumericAggregateGroup {
    var out = std.ArrayListUnmanaged(DocumentSqlNumericAggregateGroup).empty;
    errdefer out.deinit(alloc);
    for (groups) |group| {
        if (documentSqlNumericAggregateGroupMatchesHaving(group, op, having)) try out.append(alloc, group);
    }
    return try out.toOwnedSlice(alloc);
}

fn filteredDocumentSqlMaterializedAggregateRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []const AlgebraicAggregateRow,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
) ![]AlgebraicAggregateRow {
    var out = std.ArrayListUnmanaged(AlgebraicAggregateRow).empty;
    errdefer out.deinit(alloc);
    for (rows) |row| {
        if (documentSqlMaterializedAggregateRowMatchesHaving(row, having)) try out.append(alloc, row);
    }
    return try out.toOwnedSlice(alloc);
}

fn orderedDocumentSqlCountGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []const DocumentSqlCountGroup,
    order_by: sql_adapter.DocumentAggregateOrderBy,
) ![]DocumentSqlCountGroup {
    const out = try alloc.alloc(DocumentSqlCountGroup, groups.len);
    @memcpy(out, groups);
    std.mem.sort(DocumentSqlCountGroup, out, DocumentSqlCountGroupSortContext{ .order_by = order_by }, documentSqlCountGroupLessThan);
    return out;
}

fn orderedDocumentSqlNumericAggregateGroupsAlloc(
    alloc: std.mem.Allocator,
    groups: []const DocumentSqlNumericAggregateGroup,
    op: sql_adapter.DocumentAggregateOp,
    order_by: sql_adapter.DocumentAggregateOrderBy,
) ![]DocumentSqlNumericAggregateGroup {
    const out = try alloc.alloc(DocumentSqlNumericAggregateGroup, groups.len);
    @memcpy(out, groups);
    std.mem.sort(DocumentSqlNumericAggregateGroup, out, DocumentSqlNumericAggregateGroupSortContext{ .op = op, .order_by = order_by }, documentSqlNumericAggregateGroupLessThan);
    return out;
}

fn orderedDocumentSqlMaterializedAggregateRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []const AlgebraicAggregateRow,
    order_by: sql_adapter.DocumentAggregateOrderBy,
) ![]AlgebraicAggregateRow {
    const out = try alloc.alloc(AlgebraicAggregateRow, rows.len);
    @memcpy(out, rows);
    std.mem.sort(AlgebraicAggregateRow, out, DocumentSqlMaterializedAggregateSortContext{ .order_by = order_by }, documentSqlMaterializedAggregateRowLessThan);
    return out;
}

fn documentSqlCountGroupMatchesHaving(group: DocumentSqlCountGroup, having: []const sql_adapter.DocumentAggregateHavingPredicate) bool {
    for (having) |predicate| {
        if (!documentSqlCountGroupMatchesHavingPredicate(group, predicate)) return false;
    }
    return true;
}

fn documentSqlCountGroupMatchesHavingPredicate(group: DocumentSqlCountGroup, having: sql_adapter.DocumentAggregateHavingPredicate) bool {
    return switch (having.key) {
        .group => documentSqlAggregateHavingMatchesJson(group.key_json, having),
        .aggregate => blk: {
            var buf: [32]u8 = undefined;
            const value_json = std.fmt.bufPrint(&buf, "{d}", .{group.count}) catch return false;
            break :blk documentSqlAggregateHavingMatchesJson(value_json, having);
        },
    };
}

fn documentSqlNumericAggregateGroupMatchesHaving(
    group: DocumentSqlNumericAggregateGroup,
    op: sql_adapter.DocumentAggregateOp,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
) bool {
    for (having) |predicate| {
        if (!documentSqlNumericAggregateGroupMatchesHavingPredicate(group, op, predicate)) return false;
    }
    return true;
}

fn documentSqlNumericAggregateGroupMatchesHavingPredicate(
    group: DocumentSqlNumericAggregateGroup,
    op: sql_adapter.DocumentAggregateOp,
    having: sql_adapter.DocumentAggregateHavingPredicate,
) bool {
    return switch (having.key) {
        .group => documentSqlAggregateHavingMatchesJson(group.key_json, having),
        .aggregate => blk: {
            if (!group.aggregate.seen) break :blk documentSqlAggregateHavingMatchesJson("null", having);
            var buf: [64]u8 = undefined;
            const value_json = std.fmt.bufPrint(&buf, "{d}", .{documentSqlNumericAggregateResultValue(group.aggregate, op)}) catch return false;
            break :blk documentSqlAggregateHavingMatchesJson(value_json, having);
        },
    };
}

fn documentSqlMaterializedAggregateRowMatchesHaving(row: AlgebraicAggregateRow, having: []const sql_adapter.DocumentAggregateHavingPredicate) bool {
    for (having) |predicate| {
        if (!documentSqlMaterializedAggregateRowMatchesHavingPredicate(row, predicate)) return false;
    }
    return true;
}

fn documentSqlMaterializedAggregateRowMatchesHavingPredicate(row: AlgebraicAggregateRow, having: sql_adapter.DocumentAggregateHavingPredicate) bool {
    const value_json = switch (having.key) {
        .group => row.group_json orelse "null",
        .aggregate => row.value_json,
    };
    return documentSqlAggregateHavingMatchesJson(value_json, having);
}

fn documentSqlAggregateHavingMatchesJson(value_json: []const u8, having: sql_adapter.DocumentAggregateHavingPredicate) bool {
    const order = documentSqlJsonSortOrder(value_json, having.value_json, having.field_type);
    return switch (having.op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .gt => order == .gt,
        .gte => order == .gt or order == .eq,
        .lt => order == .lt,
        .lte => order == .lt or order == .eq,
    };
}

fn documentSqlCountGroupLessThan(ctx: DocumentSqlCountGroupSortContext, lhs: DocumentSqlCountGroup, rhs: DocumentSqlCountGroup) bool {
    const order = switch (ctx.order_by.key) {
        .group => documentSqlJsonSortOrder(lhs.key_json, rhs.key_json, ctx.order_by.field_type),
        .aggregate => std.math.order(lhs.count, rhs.count),
    };
    return documentSqlAggregateSortOrderLessThan(ctx.order_by.direction, order, lhs.key_json, rhs.key_json);
}

fn documentSqlNumericAggregateGroupLessThan(ctx: DocumentSqlNumericAggregateGroupSortContext, lhs: DocumentSqlNumericAggregateGroup, rhs: DocumentSqlNumericAggregateGroup) bool {
    const order = switch (ctx.order_by.key) {
        .group => documentSqlJsonSortOrder(lhs.key_json, rhs.key_json, ctx.order_by.field_type),
        .aggregate => documentSqlNumericAggregateSortOrder(lhs.aggregate, rhs.aggregate, ctx.op),
    };
    return documentSqlAggregateSortOrderLessThan(ctx.order_by.direction, order, lhs.key_json, rhs.key_json);
}

fn documentSqlMaterializedAggregateRowLessThan(ctx: DocumentSqlMaterializedAggregateSortContext, lhs: AlgebraicAggregateRow, rhs: AlgebraicAggregateRow) bool {
    const lhs_group = lhs.group_json orelse "null";
    const rhs_group = rhs.group_json orelse "null";
    const order = switch (ctx.order_by.key) {
        .group => documentSqlJsonSortOrder(lhs_group, rhs_group, ctx.order_by.field_type),
        .aggregate => documentSqlJsonSortOrder(lhs.value_json, rhs.value_json, ctx.order_by.field_type),
    };
    return documentSqlAggregateSortOrderLessThan(ctx.order_by.direction, order, lhs_group, rhs_group);
}

fn documentSqlNumericAggregateSortOrder(lhs: DocumentSqlNumericAggregate, rhs: DocumentSqlNumericAggregate, op: sql_adapter.DocumentAggregateOp) std.math.Order {
    if (!lhs.seen) return if (!rhs.seen) .eq else .gt;
    if (!rhs.seen) return .lt;
    return std.math.order(documentSqlNumericAggregateResultValue(lhs, op), documentSqlNumericAggregateResultValue(rhs, op));
}

fn documentSqlJsonSortOrder(lhs_json: []const u8, rhs_json: []const u8, field_type: storage_schema.AntflyType) std.math.Order {
    if (std.mem.eql(u8, lhs_json, "null")) return if (std.mem.eql(u8, rhs_json, "null")) .eq else .gt;
    if (std.mem.eql(u8, rhs_json, "null")) return .lt;
    return switch (field_type) {
        .numeric => blk: {
            const left = std.fmt.parseFloat(f64, lhs_json) catch break :blk std.mem.order(u8, lhs_json, rhs_json);
            const right = std.fmt.parseFloat(f64, rhs_json) catch break :blk std.mem.order(u8, lhs_json, rhs_json);
            break :blk std.math.order(left, right);
        },
        .boolean => blk: {
            const left = std.mem.eql(u8, lhs_json, "true");
            const right = std.mem.eql(u8, rhs_json, "true");
            break :blk std.math.order(@intFromBool(left), @intFromBool(right));
        },
        else => std.mem.order(u8, lhs_json, rhs_json),
    };
}

fn documentSqlAggregateSortOrderLessThan(
    direction: sql_adapter.DocumentOrderDirection,
    order: std.math.Order,
    lhs_tie: []const u8,
    rhs_tie: []const u8,
) bool {
    if (order == .eq) {
        const tie = std.mem.order(u8, lhs_tie, rhs_tie);
        return tie == .lt;
    }
    return switch (direction) {
        .asc => order == .lt,
        .desc => order == .gt,
    };
}

test "document SQL aggregate ordering uses stable ascending group tie break" {
    try std.testing.expect(documentSqlAggregateSortOrderLessThan(.asc, .eq, "\"active\"", "\"archived\""));
    try std.testing.expect(documentSqlAggregateSortOrderLessThan(.desc, .eq, "\"active\"", "\"archived\""));
    try std.testing.expect(!documentSqlAggregateSortOrderLessThan(.desc, .eq, "\"archived\"", "\"active\""));
    try std.testing.expect(documentSqlAggregateSortOrderLessThan(.desc, .gt, "\"active\"", "\"archived\""));
    try std.testing.expect(!documentSqlAggregateSortOrderLessThan(.desc, .lt, "\"active\"", "\"archived\""));
}

fn documentSqlGroupedCountAggregateResultAlloc(
    alloc: std.mem.Allocator,
    group_output: []const u8,
    aggregate_output: []const u8,
    groups: []const DocumentSqlCountGroup,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
    order_by: ?sql_adapter.DocumentAggregateOrderBy,
    limit: ?u32,
) !RowsAggregateResult {
    const filtered_groups = if (having.len > 0) try filteredDocumentSqlCountGroupsAlloc(alloc, groups, having) else null;
    defer if (filtered_groups) |items| alloc.free(items);
    const candidate_groups = filtered_groups orelse groups;
    const ordered_groups = if (order_by) |order| try orderedDocumentSqlCountGroupsAlloc(alloc, candidate_groups, order) else null;
    defer if (ordered_groups) |items| alloc.free(items);
    const result_groups = ordered_groups orelse candidate_groups;
    const output_count = if (limit) |value| @min(result_groups.len, value) else result_groups.len;
    const rows = try alloc.alloc([]const u8, output_count);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(@constCast(row));
    }
    for (result_groups[0..output_count], 0..) |group, i| {
        var row: std.Io.Writer.Allocating = .init(alloc);
        errdefer row.deinit();
        const writer = &row.writer;
        try writer.print("{{{f}:{s},{f}:{d}}}", .{
            std.json.fmt(group_output, .{}),
            group.key_json,
            std.json.fmt(aggregate_output, .{}),
            group.count,
        });
        rows[i] = try row.toOwnedSlice();
        initialized += 1;
    }
    return .{
        .rows = rows,
        .total_groups = @intCast(result_groups.len),
    };
}

fn documentSqlMaterializedScalarAggregateResultAlloc(
    alloc: std.mem.Allocator,
    aggregate_output: []const u8,
    value_json: []const u8,
) !RowsAggregateResult {
    var row: std.Io.Writer.Allocating = .init(alloc);
    errdefer row.deinit();
    try row.writer.print("{{{f}:{s}}}", .{
        std.json.fmt(aggregate_output, .{}),
        value_json,
    });
    const rows = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(rows);
    rows[0] = try row.toOwnedSlice();
    return .{
        .rows = rows,
        .total_groups = 1,
    };
}

fn documentSqlMaterializedGroupedAggregateResultAlloc(
    alloc: std.mem.Allocator,
    group_output: []const u8,
    aggregate_output: []const u8,
    materialized_rows: []const AlgebraicAggregateRow,
    total_groups: u32,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
    order_by: ?sql_adapter.DocumentAggregateOrderBy,
    limit: ?u32,
) !RowsAggregateResult {
    const filtered_rows = if (having.len > 0) try filteredDocumentSqlMaterializedAggregateRowsAlloc(alloc, materialized_rows, having) else null;
    defer if (filtered_rows) |items| alloc.free(items);
    const candidate_rows = filtered_rows orelse materialized_rows;
    const ordered_rows = if (order_by) |order| try orderedDocumentSqlMaterializedAggregateRowsAlloc(alloc, candidate_rows, order) else null;
    defer if (ordered_rows) |items| alloc.free(items);
    const result_rows = ordered_rows orelse candidate_rows;
    const output_count = if (limit) |value| @min(result_rows.len, value) else result_rows.len;
    const rows = try alloc.alloc([]const u8, output_count);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(@constCast(row));
    }
    for (result_rows[0..output_count], 0..) |materialized, i| {
        const group_json = materialized.group_json orelse return error.InvalidRowsRequest;
        var row: std.Io.Writer.Allocating = .init(alloc);
        errdefer row.deinit();
        try row.writer.print("{{{f}:{s},{f}:{s}}}", .{
            std.json.fmt(group_output, .{}),
            group_json,
            std.json.fmt(aggregate_output, .{}),
            materialized.value_json,
        });
        rows[i] = try row.toOwnedSlice();
        initialized += 1;
    }
    return .{
        .rows = rows,
        .total_groups = if (having.len == 0) total_groups else @intCast(candidate_rows.len),
    };
}

fn documentSqlGroupedNumericAggregateResultAlloc(
    alloc: std.mem.Allocator,
    group_output: []const u8,
    aggregate_output: []const u8,
    op: sql_adapter.DocumentAggregateOp,
    groups: []const DocumentSqlNumericAggregateGroup,
    having: []const sql_adapter.DocumentAggregateHavingPredicate,
    order_by: ?sql_adapter.DocumentAggregateOrderBy,
    limit: ?u32,
) !RowsAggregateResult {
    const filtered_groups = if (having.len > 0) try filteredDocumentSqlNumericAggregateGroupsAlloc(alloc, groups, op, having) else null;
    defer if (filtered_groups) |items| alloc.free(items);
    const candidate_groups = filtered_groups orelse groups;
    const ordered_groups = if (order_by) |order| try orderedDocumentSqlNumericAggregateGroupsAlloc(alloc, candidate_groups, op, order) else null;
    defer if (ordered_groups) |items| alloc.free(items);
    const result_groups = ordered_groups orelse candidate_groups;
    const output_count = if (limit) |value| @min(result_groups.len, value) else result_groups.len;
    const rows = try alloc.alloc([]const u8, output_count);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(@constCast(row));
    }
    for (result_groups[0..output_count], 0..) |group, i| {
        var row: std.Io.Writer.Allocating = .init(alloc);
        errdefer row.deinit();
        const writer = &row.writer;
        try writer.print("{{{f}:{s},{f}:", .{
            std.json.fmt(group_output, .{}),
            group.key_json,
            std.json.fmt(aggregate_output, .{}),
        });
        if (group.aggregate.seen) {
            try writer.print("{d}", .{documentSqlNumericAggregateResultValue(group.aggregate, op)});
        } else {
            try writer.writeAll("null");
        }
        try writer.writeByte('}');
        rows[i] = try row.toOwnedSlice();
        initialized += 1;
    }
    return .{
        .rows = rows,
        .total_groups = @intCast(result_groups.len),
    };
}

fn appendDocumentSqlNumericAggregateFromDocJsonAlloc(
    alloc: std.mem.Allocator,
    aggregate: *DocumentSqlNumericAggregate,
    op: sql_adapter.DocumentAggregateOp,
    field: []const u8,
    doc_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const value = documentSqlProjectedValue(parsed.value, field) orelse return;
    try appendDocumentSqlNumericAggregateValue(aggregate, op, value);
}

fn appendDocumentSqlNumericAggregateValue(
    aggregate: *DocumentSqlNumericAggregate,
    op: sql_adapter.DocumentAggregateOp,
    value: std.json.Value,
) !void {
    if (value == .null) return;
    const number = try documentSqlNumericValueAsF64(value);
    aggregate.count += 1;
    if (!aggregate.seen) {
        aggregate.value = number;
        aggregate.seen = true;
        return;
    }
    aggregate.value = switch (op) {
        .sum => aggregate.value + number,
        .avg => aggregate.value + number,
        .min => @min(aggregate.value, number),
        .max => @max(aggregate.value, number),
        .count => return error.UnsupportedSqlShape,
    };
}

fn documentSqlNumericAggregateResultValue(
    aggregate: DocumentSqlNumericAggregate,
    op: sql_adapter.DocumentAggregateOp,
) f64 {
    return switch (op) {
        .avg => aggregate.value / @as(f64, @floatFromInt(aggregate.count)),
        .sum, .min, .max => aggregate.value,
        .count => unreachable,
    };
}

fn documentSqlNumericValueAsF64(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |item| try std.fmt.parseFloat(f64, item),
        else => error.InvalidRowsRequest,
    };
}

fn executeOrderedLoweredDocumentSqlUnnestReadPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentReadPlan,
    unnest: sql_adapter.DocumentUnnest,
    order_by: sql_adapter.DocumentOrderBy,
    consistency: raft_mod.ReadConsistency,
) !?RowsQueryResult {
    var candidates = std.ArrayListUnmanaged(OrderedDocumentSqlUnnestCandidate).empty;
    errdefer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }

    switch (lowered.producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendOrderedDocumentSqlUnnestCandidatesAlloc(alloc, &candidates, id, lookup.json, lowered.projection, unnest, order_by);
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = scan_plan.max_rows,
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                scanned += 1;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendOrderedDocumentSqlUnnestCandidatesAlloc(alloc, &candidates, key_value.string, lookup.json, lowered.projection, unnest, order_by);
            }
            try documentSqlAdmitBoundedRowProbeCount(scanned, scan_plan.max_rows);
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try documentSqlAdmitBoundedRowProbeCount(total_hits, query_limit);
            try appendOrderedDocumentSqlUnnestCandidatesFromQueryResponseAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                query_response.json,
                query.residual_filter_json,
                lowered.projection,
                unnest,
                order_by,
                consistency,
                &candidates,
            );
        },
    }

    std.mem.sort(OrderedDocumentSqlUnnestCandidate, candidates.items, DocumentSqlSortContext{ .direction = order_by.direction }, documentSqlUnnestCandidateLessThan);

    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
    }
    const row_limit = lowered.limit orelse @as(u32, @intCast(candidates.items.len));
    for (candidates.items) |candidate| {
        if (rows.items.len >= row_limit) break;
        try rows.append(alloc, try alloc.dupe(u8, candidate.row_json));
    }

    for (candidates.items) |*candidate| candidate.deinit(alloc);
    candidates.deinit(alloc);

    const total: u32 = @intCast(rows.items.len);
    return .{
        .rows = try rows.toOwnedSlice(alloc),
        .total = total,
    };
}

fn executeLoweredDocumentSqlUnnestReadPlanAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    lowered: sql_adapter.DocumentReadPlan,
    unnest: sql_adapter.DocumentUnnest,
    consistency: raft_mod.ReadConsistency,
) !?RowsQueryResult {
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
    }

    switch (lowered.producer) {
        .id_lookup => |lookup_plan| {
            for (lookup_plan.ids) |id| {
                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (lookup_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlUnnestRowsAlloc(alloc, id, lookup.json, lowered.projection, unnest, lowered.limit, &rows);
                if (lowered.limit) |limit| {
                    if (rows.items.len >= limit) break;
                }
            }
        },
        .bounded_scan => |scan_plan| {
            var scan = (try documentSqlScanAlloc(alloc, source, native_table_name, public_table_name, "", "", .{
                .include_documents = false,
                .include_all_fields = false,
                .limit = scan_plan.max_rows,
            }, consistency)) orelse return null;
            defer scan.deinit(alloc);
            try documentSqlAdmitBoundedScanPayload(scan_plan, scan.ndjson);

            var scanned: u32 = 0;
            var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                scanned += 1;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;
                const key_value = parsed.value.object.get("key") orelse return error.InvalidRowsRequest;
                if (key_value != .string) return error.InvalidRowsRequest;

                var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, key_value.string, .{}, consistency)) orelse continue;
                defer lookup.deinit(alloc);
                if (scan_plan.residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
                }
                try appendDocumentSqlUnnestRowsAlloc(alloc, key_value.string, lookup.json, lowered.projection, unnest, lowered.limit, &rows);
                if (lowered.limit) |limit| {
                    if (rows.items.len >= limit) break;
                }
            }
            if (lowered.limit == null or rows.items.len < lowered.limit.?) try documentSqlAdmitBoundedRowProbeCount(scanned, scan_plan.max_rows);
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse lowered.limit orelse return error.DocumentSqlRequiresBoundedScan;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            const total_hits = try documentSqlTotalHitsFromQueryResponse(alloc, query_response.json);
            try appendDocumentSqlUnnestRowsFromQueryResponseAlloc(
                alloc,
                source,
                native_table_name,
                public_table_name,
                query_response.json,
                query.residual_filter_json,
                lowered.projection,
                unnest,
                lowered.limit,
                consistency,
                &rows,
            );
            const row_limit = lowered.limit orelse std.math.maxInt(u32);
            if (rows.items.len < row_limit) try documentSqlAdmitBoundedRowCount(total_hits, query_limit);
        },
    }

    const total: u32 = @intCast(rows.items.len);
    return .{
        .rows = try rows.toOwnedSlice(alloc),
        .total = total,
    };
}

fn appendDocumentSqlRowsFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    projection: []const sql_adapter.DocumentProjection,
    residual_filter_json: ?[]const u8,
    lateral_subquery: ?sql_adapter.DocumentLateralSubquery,
    row_limit: ?u32,
    consistency: raft_mod.ReadConsistency,
    rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (hit_value.object.get("_source")) |source_value| {
            if (source_value == .object) {
                const doc_json = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
                defer alloc.free(doc_json);
                if (residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, doc_json, filter)) continue;
                }
                var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, doc_json, lateral_subquery, consistency);
                defer lateral_match.deinitOwned(alloc);
                if (!lateral_match.matched and lateral_subquery.?.join_kind != .left) continue;
                try rows.append(alloc, try documentSqlProjectedParsedRowJsonWithLateralAlloc(alloc, id_value.string, source_value, doc_json, projection, lateral_match));
                if (row_limit) |limit| {
                    if (rows.items.len >= limit) return;
                }
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (residual_filter_json) |filter| {
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        var lateral_match = try documentSqlLateralSubqueryMatchesAlloc(alloc, source, native_table_name, public_table_name, lookup.json, lateral_subquery, consistency);
        defer lateral_match.deinitOwned(alloc);
        if (!lateral_match.matched and lateral_subquery.?.join_kind != .left) continue;
        try rows.append(alloc, try documentSqlProjectedRowJsonWithLateralAlloc(alloc, id_value.string, lookup.json, projection, lateral_match));
        if (row_limit) |limit| {
            if (rows.items.len >= limit) return;
        }
    }
}

fn appendDocumentSqlFullRowsFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    consistency: raft_mod.ReadConsistency,
    rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (hit_value.object.get("_source")) |source_value| {
            if (source_value == .object) {
                try rows.append(alloc, try std.json.Stringify.valueAlloc(alloc, source_value, .{}));
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        try rows.append(alloc, try alloc.dupe(u8, lookup.json));
    }
}

fn appendDocumentSqlFullRowsFromScanAlloc(
    alloc: std.mem.Allocator,
    ndjson: []const u8,
    rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var lines = std.mem.splitScalar(u8, ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        try rows.append(alloc, try std.json.Stringify.valueAlloc(alloc, parsed.value, .{}));
    }
}

fn documentSqlProjectedRowJsonAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    projection: []const sql_adapter.DocumentProjection,
) ![]const u8 {
    return try documentSqlProjectedRowJsonWithLateralAlloc(alloc, key, doc_json, projection, .{});
}

fn documentSqlProjectedRowJsonWithLateralAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    projection: []const sql_adapter.DocumentProjection,
    lateral_match: DocumentSqlLateralMatch,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    var branch_parsed = if (lateral_match.branch_doc_json) |branch_doc_json|
        try std.json.parseFromSlice(std.json.Value, alloc, branch_doc_json, .{ .allocate = .alloc_always })
    else
        null;
    defer if (branch_parsed) |*value| value.deinit();
    const lateral_row = if (branch_parsed) |value| value.value else null;
    return try documentSqlProjectedParsedRowJsonWithUnnestAndLateralAlloc(alloc, key, parsed.value, doc_json, projection, null, lateral_match, lateral_row);
}

fn documentSqlProjectedParsedRowJsonAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
) ![]const u8 {
    return try documentSqlProjectedParsedRowJsonWithUnnestAndLateralAlloc(alloc, key, row, full_doc_json, projection, null, .{}, null);
}

fn documentSqlProjectedParsedRowJsonWithLateralAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    lateral_match: DocumentSqlLateralMatch,
) ![]const u8 {
    var branch_parsed = if (lateral_match.branch_doc_json) |branch_doc_json|
        try std.json.parseFromSlice(std.json.Value, alloc, branch_doc_json, .{ .allocate = .alloc_always })
    else
        null;
    defer if (branch_parsed) |*value| value.deinit();
    const lateral_row = if (branch_parsed) |value| value.value else null;
    return try documentSqlProjectedParsedRowJsonWithUnnestAndLateralAlloc(alloc, key, row, full_doc_json, projection, null, lateral_match, lateral_row);
}

fn documentSqlProjectedParsedRowJsonWithUnnestAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest_value: ?std.json.Value,
) ![]const u8 {
    return try documentSqlProjectedParsedRowJsonWithUnnestAndLateralAlloc(alloc, key, row, full_doc_json, projection, unnest_value, .{}, null);
}

fn documentSqlProjectedParsedRowJsonWithUnnestAndLateralAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest_value: ?std.json.Value,
    lateral_match: DocumentSqlLateralMatch,
    lateral_row: ?std.json.Value,
) ![]const u8 {
    if (row != .object) return error.InvalidRowsRequest;
    if (lateral_row) |branch| {
        if (branch != .object) return error.InvalidRowsRequest;
    }
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    for (projection, 0..) |item, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(item.output, .{})});
        switch (item.kind) {
            .id => try writer.print("{f}", .{std.json.fmt(key, .{})}),
            .doc => {
                const doc_json = full_doc_json orelse return error.InvalidRowsRequest;
                try writer.writeAll(doc_json);
            },
            .field => {
                if (item.lateral and !lateral_match.matched) {
                    try writer.writeAll("null");
                } else if (documentSqlProjectedValue(if (item.lateral) (lateral_row orelse row) else row, item.field)) |value| {
                    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
                    defer alloc.free(value_json);
                    try writer.writeAll(value_json);
                } else {
                    try writer.writeAll("null");
                }
            },
            .scalar_text_cast => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlTextCastValueAlloc(alloc, value);
                defer alloc.free(text);
                try writer.print("{f}", .{std.json.fmt(text, .{})});
            },
            .scalar_numeric_cast => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const number = try documentSqlNumericCastValue(value);
                try writer.print("{d}", .{number});
            },
            .scalar_boolean_cast => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                try writer.writeAll(if (try documentSqlBooleanCastValue(value)) "true" else "false");
            },
            .scalar_datetime_cast => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                try writer.print("{d}", .{try documentSqlDatetimeCastValue(value)});
            },
            .temporal_date_utc => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const date = try documentSqlUtcDateFromDatetimeAlloc(alloc, value);
                defer alloc.free(date);
                try writer.print("{f}", .{std.json.fmt(date, .{})});
            },
            .array_append, .array_cat, .array_prepend, .array_remove, .array_replace => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const array_json = try documentSqlArrayTransformJsonAlloc(alloc, value, item.kind, item.pattern, item.numeric_operand);
                defer alloc.free(array_json);
                try writer.writeAll(array_json);
            },
            .array_cardinality => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                if (value != .array) return error.InvalidRowsRequest;
                try writer.print("{d}", .{value.array.items.len});
            },
            .array_position => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const position = try documentSqlArrayPosition(value, item.pattern);
                if (position) |index| {
                    try writer.print("{d}", .{index});
                } else {
                    try writer.writeAll("null");
                }
            },
            .array_positions => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const positions_json = try documentSqlArrayPositionsJsonAlloc(alloc, value, item.pattern);
                defer alloc.free(positions_json);
                try writer.writeAll(positions_json);
            },
            .json_typeof => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                try writer.print("{f}", .{std.json.fmt(documentSqlJsonTypeofName(value), .{})});
            },
            .json_array_length => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                switch (value) {
                    .null => try writer.writeAll("null"),
                    .array => |array| try writer.print("{d}", .{array.items.len}),
                    else => return error.InvalidRowsRequest,
                }
            },
            .numeric_abs => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const number = try documentSqlJsonNumber(value);
                try writer.print("{d}", .{@abs(number)});
            },
            .numeric_round, .numeric_trunc, .numeric_floor, .numeric_ceil, .numeric_sqrt, .numeric_sign => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const number = try documentSqlJsonNumber(value);
                const result = try documentSqlNumericUnaryResult(number, item.kind);
                try writer.print("{d}", .{result});
            },
            .numeric_arithmetic => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const number = try documentSqlJsonNumber(value);
                const operand = std.fmt.parseFloat(f64, item.numeric_operand) catch return error.InvalidRowsRequest;
                const result = switch (item.numeric_operator) {
                    .add => number + operand,
                    .sub => number - operand,
                    .mul => number * operand,
                    .div => if (operand == 0) return error.InvalidRowsRequest else number / operand,
                    .mod => if (operand == 0) return error.InvalidRowsRequest else number - @trunc(number / operand) * operand,
                    .power => std.math.pow(f64, number, operand),
                };
                if (!std.math.isFinite(result)) return error.InvalidRowsRequest;
                try writer.print("{d}", .{result});
            },
            .regexp_count, .regexp_instr, .regexp_substr => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                switch (item.kind) {
                    .regexp_count => {
                        const count = try documentSqlRegexpCountText(alloc, text, item.pattern);
                        try writer.print("{d}", .{count});
                    },
                    .regexp_instr => {
                        const position = try documentSqlRegexpInstrText(alloc, text, item.pattern);
                        try writer.print("{d}", .{position});
                    },
                    .regexp_substr => {
                        const matched = try documentSqlRegexpSubstrTextAlloc(alloc, text, item.pattern);
                        defer if (matched) |value_text| alloc.free(value_text);
                        if (matched) |value_text| {
                            try writer.print("{f}", .{std.json.fmt(value_text, .{})});
                        } else {
                            try writer.writeAll("null");
                        }
                    },
                    else => unreachable,
                }
            },
            .text_starts_with, .text_ends_with, .text_strpos => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                switch (item.kind) {
                    .text_starts_with => try writer.writeAll(if (std.mem.startsWith(u8, text, item.pattern)) "true" else "false"),
                    .text_ends_with => try writer.writeAll(if (std.mem.endsWith(u8, text, item.pattern)) "true" else "false"),
                    .text_strpos => {
                        const position = try documentSqlStrposTextCodepointPosition(text, item.pattern);
                        try writer.print("{d}", .{position});
                    },
                    else => unreachable,
                }
            },
            .text_split_part => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const index = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const part = try documentSqlSplitPartTextAlloc(alloc, text, item.pattern, index);
                defer alloc.free(part);
                try writer.print("{f}", .{std.json.fmt(part, .{})});
            },
            .text_substring => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const start = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const length: ?i64 = if (item.numeric_operand2.len == 0)
                    null
                else
                    std.fmt.parseInt(i64, item.numeric_operand2, 10) catch return error.InvalidRowsRequest;
                const substring = try documentSqlSubstringTextAlloc(alloc, text, start, length);
                defer alloc.free(substring);
                try writer.print("{f}", .{std.json.fmt(substring, .{})});
            },
            .text_overlay => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const start = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const length: ?i64 = if (item.numeric_operand2.len == 0)
                    null
                else
                    std.fmt.parseInt(i64, item.numeric_operand2, 10) catch return error.InvalidRowsRequest;
                const overlayed = try documentSqlOverlayTextAlloc(alloc, text, item.pattern, start, length);
                defer alloc.free(overlayed);
                try writer.print("{f}", .{std.json.fmt(overlayed, .{})});
            },
            .text_left, .text_right => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const count = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const sliced = try documentSqlLeftRightTextAlloc(alloc, text, count, item.kind == .text_left);
                defer alloc.free(sliced);
                try writer.print("{f}", .{std.json.fmt(sliced, .{})});
            },
            .text_repeat => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const count = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const repeated = try documentSqlRepeatTextAlloc(alloc, text, count);
                defer alloc.free(repeated);
                try writer.print("{f}", .{std.json.fmt(repeated, .{})});
            },
            .text_reverse => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const reversed = try documentSqlReverseTextAlloc(alloc, text);
                defer alloc.free(reversed);
                try writer.print("{f}", .{std.json.fmt(reversed, .{})});
            },
            .text_btrim, .text_ltrim, .text_rtrim => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const trimmed = documentSqlTrimText(text, item.kind);
                try writer.print("{f}", .{std.json.fmt(trimmed, .{})});
            },
            .text_ascii => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const first_codepoint = try documentSqlAsciiFirstCodepoint(text);
                try writer.print("{d}", .{first_codepoint});
            },
            .text_replace => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const replaced = try documentSqlReplaceTextAlloc(alloc, text, item.pattern, item.numeric_operand);
                defer alloc.free(replaced);
                try writer.print("{f}", .{std.json.fmt(replaced, .{})});
            },
            .text_nullif => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                if (std.mem.eql(u8, text, item.pattern)) {
                    try writer.writeAll("null");
                } else {
                    try writer.print("{f}", .{std.json.fmt(text, .{})});
                }
            },
            .text_concat_ws => {
                const left = try documentSqlOptionalProjectedStringValue(row, item.field);
                const right = try documentSqlOptionalProjectedStringValue(row, item.field2);
                const joined = try documentSqlConcatWsTextAlloc(alloc, item.pattern, left, right);
                defer alloc.free(joined);
                try writer.print("{f}", .{std.json.fmt(joined, .{})});
            },
            .text_array_to_string => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const joined = try documentSqlArrayToStringTextAlloc(alloc, value, item.pattern);
                defer alloc.free(joined);
                try writer.print("{f}", .{std.json.fmt(joined, .{})});
            },
            .text_string_to_array => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const array_json = try documentSqlStringToArrayJsonAlloc(alloc, text, item.pattern);
                defer alloc.free(array_json);
                try writer.writeAll(array_json);
            },
            .text_translate => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const translated = try documentSqlTranslateTextAlloc(alloc, text, item.pattern, item.numeric_operand);
                defer alloc.free(translated);
                try writer.print("{f}", .{std.json.fmt(translated, .{})});
            },
            .text_lpad, .text_rpad => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const width = std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const padded = try documentSqlPadTextAlloc(alloc, text, width, item.pattern, item.kind == .text_lpad);
                defer alloc.free(padded);
                try writer.print("{f}", .{std.json.fmt(padded, .{})});
            },
            .text_length => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                const text = try documentSqlFilterStringValue(value);
                const length = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
                try writer.print("{d}", .{length});
            },
            .text_octet_length, .text_bit_length => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                const text = try documentSqlFilterStringValue(value);
                const length = if (item.kind == .text_bit_length) text.len * 8 else text.len;
                try writer.print("{d}", .{length});
            },
            .text_initcap => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                const text = try documentSqlFilterStringValue(value);
                if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
                const titled = try documentSqlInitcapAsciiAlloc(alloc, text);
                defer alloc.free(titled);
                try writer.print("{f}", .{std.json.fmt(titled, .{})});
            },
            .text_md5 => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const digest = try expr_text.md5HexTextAlloc(alloc, text);
                defer alloc.free(digest);
                try writer.print("{f}", .{std.json.fmt(digest, .{})});
            },
            .text_soundex => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                if (value == .null) {
                    try writer.writeAll("null");
                    continue;
                }
                const text = try documentSqlFilterStringValue(value);
                const code = try expr_text.soundexTextAlloc(alloc, text);
                defer alloc.free(code);
                try writer.print("{f}", .{std.json.fmt(code, .{})});
            },
            .text_chr => {
                const codepoint = if (item.field.len > 0) blk: {
                    const value = documentSqlProjectedValue(row, item.field) orelse {
                        try writer.writeAll("null");
                        continue;
                    };
                    if (value == .null) {
                        try writer.writeAll("null");
                        continue;
                    }
                    break :blk try documentSqlIntegerCodepointFromJson(value);
                } else std.fmt.parseInt(i64, item.numeric_operand, 10) catch return error.InvalidRowsRequest;
                const encoded = try documentSqlChrTextAlloc(alloc, codepoint);
                defer alloc.free(encoded);
                try writer.print("{f}", .{std.json.fmt(encoded, .{})});
            },
            .text_lower, .text_upper => {
                const value = documentSqlProjectedValue(row, item.field) orelse {
                    try writer.writeAll("null");
                    continue;
                };
                const text = try documentSqlFilterStringValue(value);
                if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
                const folded = if (item.kind == .text_lower)
                    try std.ascii.allocLowerString(alloc, text)
                else
                    try std.ascii.allocUpperString(alloc, text);
                defer alloc.free(folded);
                try writer.print("{f}", .{std.json.fmt(folded, .{})});
            },
            .unnest_value => {
                const value = unnest_value orelse return error.InvalidRowsRequest;
                const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
                defer alloc.free(value_json);
                try writer.writeAll(value_json);
            },
        }
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn documentSqlLateralSubqueryMatchesAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    doc_json: []const u8,
    lateral_subquery: ?sql_adapter.DocumentLateralSubquery,
    consistency: raft_mod.ReadConsistency,
) !DocumentSqlLateralMatch {
    const lateral = lateral_subquery orelse return .{};
    if (lateral.branch_lookup_required) {
        return try documentSqlLateralBranchLookupMatchesAlloc(alloc, source, native_table_name, public_table_name, doc_json, lateral, consistency);
    }
    if (lateral.correlation_nullable or lateral.field_residuals.len > 0) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (lateral.correlation_nullable) {
            const correlation_value = documentSqlProjectedValue(parsed.value, lateral.correlation_path) orelse return .{ .matched = false };
            if (correlation_value == .null) return .{ .matched = false };
        }
        for (lateral.field_residuals) |residual| {
            if (!try documentSqlLateralFieldResidualMatches(parsed.value, residual)) return .{ .matched = false };
        }
    }
    const filter = lateral.residual_filter_json orelse return .{};
    return .{ .matched = try residualFilterMatchesAlloc(alloc, doc_json, filter) };
}

fn documentSqlLateralBranchLookupMatchesAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    doc_json: []const u8,
    lateral: sql_adapter.DocumentLateralSubquery,
    consistency: raft_mod.ReadConsistency,
) !DocumentSqlLateralMatch {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const correlation_value = documentSqlProjectedValue(parsed.value, lateral.correlation_path) orelse return .{ .matched = false };
    if (correlation_value == .null) return .{ .matched = false };

    const filter_json = try documentSqlLateralBranchFilterJsonAlloc(alloc, parsed.value, lateral, correlation_value);
    const branch_limit = if (lateral.branch_order_by.len > 0) documentSqlIndexedCandidateProbeLimit(1) else 1;
    var query = sql_adapter.DocumentIndexQuery{
        .filter_query_json = filter_json,
        .max_candidate_rows = branch_limit,
    };
    defer query.deinit(alloc);

    var response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, branch_limit, true, false, consistency)) orelse return .{ .matched = false };
    defer response.deinit(alloc);
    const branch_doc_json = if (lateral.branch_order_by.len > 0)
        (try documentSqlOrderedFirstSourceFromQueryResponseAlloc(alloc, response.json, branch_limit, lateral.branch_order_by)) orelse return .{ .matched = false }
    else
        (try documentSqlFirstSourceFromQueryResponseAlloc(alloc, response.json)) orelse return .{ .matched = false };
    return .{ .matched = true, .branch_doc_json = branch_doc_json };
}

fn documentSqlLateralBranchFilterJsonAlloc(
    alloc: std.mem.Allocator,
    outer_row: std.json.Value,
    lateral: sql_adapter.DocumentLateralSubquery,
    correlation_value: std.json.Value,
) ![]const u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"bool\":{\"filter\":[");
    try documentSqlAppendLateralTermFilterAlloc(alloc, &out, lateral.correlation_path, correlation_value);
    if (lateral.residual_filter_json) |filter| {
        try out.append(alloc, ',');
        try out.appendSlice(alloc, filter);
    }
    for (lateral.field_residuals) |residual| {
        const outer_value = documentSqlProjectedValue(outer_row, residual.outer_path) orelse {
            out.deinit(alloc);
            return try alloc.dupe(u8, "{\"match_none\":{}}");
        };
        if (outer_value == .null) {
            out.deinit(alloc);
            return try alloc.dupe(u8, "{\"match_none\":{}}");
        }
        try out.append(alloc, ',');
        try documentSqlAppendLateralFieldResidualFilterAlloc(alloc, &out, residual, outer_value);
    }
    try out.appendSlice(alloc, "]}}");
    return try out.toOwnedSlice(alloc);
}

fn documentSqlAppendLateralTermFilterAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    value: std.json.Value,
) !void {
    try out.appendSlice(alloc, "{\"term\":{\"path\":");
    try appendJsonString(alloc, out, path);
    try out.appendSlice(alloc, ",\"value\":");
    try documentSqlAppendJsonValueAlloc(alloc, out, value);
    try out.appendSlice(alloc, "}}");
}

fn documentSqlAppendLateralFieldResidualFilterAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    residual: sql_adapter.DocumentLateralFieldResidual,
    outer_value: std.json.Value,
) !void {
    switch (residual.op) {
        .eq => try documentSqlAppendLateralTermFilterAlloc(alloc, out, residual.branch_path, outer_value),
        .neq => {
            try out.appendSlice(alloc, "{\"bool\":{\"must_not\":[");
            try documentSqlAppendLateralTermFilterAlloc(alloc, out, residual.branch_path, outer_value);
            try out.appendSlice(alloc, "]}}");
        },
        .gt, .gte, .lt, .lte => {
            const range_op = switch (residual.field_type) {
                .numeric => "numeric_range",
                .datetime => "date_range",
                .keyword, .text, .html, .search_as_you_type => "term_range",
                else => return error.DocumentSqlLateralRequiresNativeProducer,
            };
            try out.append(alloc, '{');
            try appendJsonString(alloc, out, range_op);
            try out.appendSlice(alloc, ":{\"path\":");
            try appendJsonString(alloc, out, residual.branch_path);
            switch (residual.op) {
                .gt, .gte => {
                    try out.appendSlice(alloc, ",\"min\":");
                    try documentSqlAppendJsonValueAlloc(alloc, out, outer_value);
                    try out.appendSlice(alloc, ",\"inclusive_min\":");
                    try out.appendSlice(alloc, if (residual.op == .gte) "true" else "false");
                },
                .lt, .lte => {
                    try out.appendSlice(alloc, ",\"max\":");
                    try documentSqlAppendJsonValueAlloc(alloc, out, outer_value);
                    try out.appendSlice(alloc, ",\"inclusive_max\":");
                    try out.appendSlice(alloc, if (residual.op == .lte) "true" else "false");
                },
                .eq, .neq => unreachable,
            }
            try out.appendSlice(alloc, "}}");
        },
    }
}

fn documentSqlAppendJsonValueAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn documentSqlLateralFieldResidualMatches(
    row: std.json.Value,
    residual: sql_adapter.DocumentLateralFieldResidual,
) !bool {
    const branch_value = documentSqlProjectedValue(row, residual.branch_path) orelse return false;
    const outer_value = documentSqlProjectedValue(row, residual.outer_path) orelse return false;
    if (branch_value == .null or outer_value == .null) return false;
    return switch (residual.op) {
        .eq => documentSqlJsonValuesEqual(branch_value, outer_value),
        .neq => !documentSqlJsonValuesEqual(branch_value, outer_value),
        .gt, .gte, .lt, .lte => blk: {
            const comparison = documentSqlJsonCompare(branch_value, outer_value) orelse break :blk false;
            break :blk switch (residual.op) {
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                .eq, .neq => unreachable,
            };
        },
    };
}

fn documentSqlJsonCompare(left: std.json.Value, right: std.json.Value) ?DocumentMergeScalarComparison {
    return documentMergeJsonCompare(left, right);
}

fn appendOrderedDocumentSqlUnnestCandidatesAlloc(
    alloc: std.mem.Allocator,
    candidates: *std.ArrayListUnmanaged(OrderedDocumentSqlUnnestCandidate),
    key: []const u8,
    doc_json: []const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest: sql_adapter.DocumentUnnest,
    order_by: sql_adapter.DocumentOrderBy,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const array_value = documentSqlProjectedValue(parsed.value, unnest.field) orelse return;
    if (array_value != .array) return;

    var filter_value = if (unnest.filter_value_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_value) |*value| value.deinit();
    var filter_values = if (unnest.filter_values_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_values) |*value| value.deinit();
    var filter_range = if (unnest.filter_range_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_range) |*value| value.deinit();
    var filter_not_value = if (unnest.filter_not_value_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_not_value) |*value| value.deinit();
    var filter_not_values = if (unnest.filter_not_values_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_not_values) |*value| value.deinit();
    var filter_pattern = if (unnest.filter_pattern_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_pattern) |*value| value.deinit();

    if (unnest.filter_match_none) return;

    for (array_value.array.items) |item| {
        if (filter_value) |value| {
            if (!documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_values) |values| {
            if (!documentSqlJsonValueInArray(item, values.value)) continue;
        }
        if (filter_range) |range| {
            if (!try documentSqlUnnestItemRangeMatches(item, range.value)) continue;
        }
        if (filter_not_value) |value| {
            if (item == .null or documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_not_values) |values| {
            if (item == .null or documentSqlJsonValueInArray(item, values.value)) continue;
        }
        if (filter_pattern) |pattern| {
            if (!documentSqlUnnestItemPatternMatches(item, pattern.value, unnest.filter_pattern_case_insensitive)) continue;
        }
        if (unnest.filter_is_not_null and item == .null) continue;
        const owned_id = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_id);
        const row_json = try documentSqlProjectedParsedRowJsonWithUnnestAlloc(alloc, key, parsed.value, doc_json, projection, item);
        errdefer alloc.free(row_json);
        var sort_key = try documentSqlUnnestSortKeyAlloc(alloc, key, doc_json, item, unnest, order_by);
        errdefer sort_key.deinit(alloc);
        try candidates.append(alloc, .{
            .id = owned_id,
            .row_json = row_json,
            .sort_key = sort_key,
        });
    }
}

fn appendOrderedDocumentSqlUnnestCandidatesFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    residual_filter_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest: sql_adapter.DocumentUnnest,
    order_by: sql_adapter.DocumentOrderBy,
    consistency: raft_mod.ReadConsistency,
    candidates: *std.ArrayListUnmanaged(OrderedDocumentSqlUnnestCandidate),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (hit_value.object.get("_source")) |source_value| {
            if (source_value == .object) {
                const doc_json = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
                defer alloc.free(doc_json);
                if (residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, doc_json, filter)) continue;
                }
                try appendOrderedDocumentSqlUnnestCandidatesAlloc(alloc, candidates, id_value.string, doc_json, projection, unnest, order_by);
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (residual_filter_json) |filter| {
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        try appendOrderedDocumentSqlUnnestCandidatesAlloc(alloc, candidates, id_value.string, lookup.json, projection, unnest, order_by);
    }
}

fn documentSqlUnnestSortKeyAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    item: std.json.Value,
    unnest: sql_adapter.DocumentUnnest,
    order_by: sql_adapter.DocumentOrderBy,
) !DocumentSqlSortKey {
    if (std.ascii.eqlIgnoreCase(order_by.field, unnest.alias)) {
        return try documentSqlSortKeyFromValueAlloc(alloc, item, unnest.item_type);
    }
    return try documentSqlSortKeyAlloc(alloc, key, doc_json, order_by);
}

fn appendDocumentSqlUnnestRowsFromQueryResponseAlloc(
    alloc: std.mem.Allocator,
    source: Source,
    native_table_name: []const u8,
    public_table_name: []const u8,
    response_json: []const u8,
    residual_filter_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest: sql_adapter.DocumentUnnest,
    row_limit: ?u32,
    consistency: raft_mod.ReadConsistency,
    rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidRowsRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidRowsRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidRowsRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hits_value != .object) return error.InvalidRowsRequest;
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidRowsRequest;
    if (hit_items != .array) return error.InvalidRowsRequest;

    for (hit_items.array.items) |hit_value| {
        if (hit_value != .object) return error.InvalidRowsRequest;
        const id_value = hit_value.object.get("_id") orelse return error.InvalidRowsRequest;
        if (id_value != .string) return error.InvalidRowsRequest;
        if (hit_value.object.get("_source")) |source_value| {
            if (source_value == .object) {
                const doc_json = try std.json.Stringify.valueAlloc(alloc, source_value, .{});
                defer alloc.free(doc_json);
                if (residual_filter_json) |filter| {
                    if (!try residualFilterMatchesAlloc(alloc, doc_json, filter)) continue;
                }
                try appendDocumentSqlUnnestRowsAlloc(alloc, id_value.string, doc_json, projection, unnest, row_limit, rows);
                if (row_limit) |limit| {
                    if (rows.items.len >= limit) return;
                }
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (residual_filter_json) |filter| {
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        try appendDocumentSqlUnnestRowsAlloc(alloc, id_value.string, lookup.json, projection, unnest, row_limit, rows);
        if (row_limit) |limit| {
            if (rows.items.len >= limit) return;
        }
    }
}

fn appendDocumentSqlUnnestRowsAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    doc_json: []const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest: sql_adapter.DocumentUnnest,
    row_limit: ?u32,
    rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const array_value = documentSqlProjectedValue(parsed.value, unnest.field) orelse return;
    if (array_value != .array) return;

    var filter_value = if (unnest.filter_value_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_value) |*value| value.deinit();
    var filter_values = if (unnest.filter_values_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_values) |*value| value.deinit();
    var filter_range = if (unnest.filter_range_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_range) |*value| value.deinit();
    var filter_not_value = if (unnest.filter_not_value_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_not_value) |*value| value.deinit();
    var filter_not_values = if (unnest.filter_not_values_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_not_values) |*value| value.deinit();
    var filter_pattern = if (unnest.filter_pattern_json) |filter_json|
        try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{})
    else
        null;
    defer if (filter_pattern) |*value| value.deinit();

    if (unnest.filter_match_none) return;

    for (array_value.array.items) |item| {
        if (filter_value) |value| {
            if (!documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_values) |values| {
            if (!documentSqlJsonValueInArray(item, values.value)) continue;
        }
        if (filter_range) |range| {
            if (!try documentSqlUnnestItemRangeMatches(item, range.value)) continue;
        }
        if (filter_not_value) |value| {
            if (item == .null or documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_not_values) |values| {
            if (item == .null or documentSqlJsonValueInArray(item, values.value)) continue;
        }
        if (filter_pattern) |pattern| {
            if (!documentSqlUnnestItemPatternMatches(item, pattern.value, unnest.filter_pattern_case_insensitive)) continue;
        }
        if (unnest.filter_is_not_null and item == .null) continue;
        try rows.append(alloc, try documentSqlProjectedParsedRowJsonWithUnnestAlloc(alloc, key, parsed.value, doc_json, projection, item));
        if (row_limit) |limit| {
            if (rows.items.len >= limit) return;
        }
    }
}

fn documentSqlProjectedValue(row: std.json.Value, field: []const u8) ?std.json.Value {
    if (row != .object) return null;
    if (field.len == 0) return null;
    if (field[0] != '/') {
        if (row.object.get(field)) |value| return value;
        var current = row;
        var parts = std.mem.splitScalar(u8, field, '.');
        while (parts.next()) |part| {
            if (part.len == 0 or current != .object) return null;
            current = current.object.get(part) orelse return null;
        }
        return current;
    }

    var current = row;
    var parts = std.mem.splitScalar(u8, field[1..], '/');
    while (parts.next()) |part| {
        if (part.len == 0 or current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

pub fn residualFilterMatchesAlloc(
    alloc: std.mem.Allocator,
    doc_json: []const u8,
    filter_json: []const u8,
) !bool {
    var doc = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer doc.deinit();
    var filter = std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{}) catch return error.InvalidRowsRequest;
    defer filter.deinit();
    return try documentSqlFilterValueMatches(alloc, doc.value, filter.value);
}

fn documentSqlFilterValueMatches(
    alloc: std.mem.Allocator,
    doc: std.json.Value,
    filter: std.json.Value,
) anyerror!bool {
    if (filter != .object) return error.InvalidRowsRequest;
    if (filter.object.get("match_all") != null) return true;
    if (filter.object.get("match_none") != null) return false;
    if (filter.object.get("conjuncts")) |conjuncts| {
        return try documentSqlFilterConjunctionMatches(alloc, doc, conjuncts);
    }
    if (filter.object.get("disjuncts")) |disjuncts| {
        return try documentSqlFilterDisjunctionMatches(alloc, doc, disjuncts);
    }
    if (filter.object.get("bool")) |bool_value| {
        if (bool_value != .object) return error.InvalidRowsRequest;
        var has_clause = false;
        var matched_positive = false;
        if (bool_value.object.get("must")) |must_value| {
            has_clause = true;
            if (!try documentSqlFilterConjunctionMatches(alloc, doc, must_value)) return false;
            matched_positive = true;
        }
        if (bool_value.object.get("filter")) |filter_value| {
            has_clause = true;
            if (!try documentSqlFilterConjunctionMatches(alloc, doc, filter_value)) return false;
            matched_positive = true;
        }
        if (bool_value.object.get("should")) |should_value| {
            has_clause = true;
            const minimum_should_match = try documentSqlFilterMinimumShouldMatch(bool_value, if (matched_positive) 0 else 1);
            if (!try documentSqlFilterShouldMatches(alloc, doc, should_value, minimum_should_match)) return false;
        }
        if (bool_value.object.get("must_not")) |must_not_value| {
            has_clause = true;
            if (try documentSqlFilterDisjunctionMatches(alloc, doc, must_not_value)) return false;
        }
        if (!has_clause) return error.InvalidRowsRequest;
        return true;
    }
    if (filter.object.get("exists")) |exists| {
        const path = try documentSqlFilterPath(exists);
        return documentSqlProjectedValue(doc, path) != null;
    }
    if (filter.object.get("term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return documentSqlJsonValuesEqual(actual, field_value.value);
    }
    if (filter.object.get("text_lower_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiLowerEqualsAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("text_upper_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiUpperEqualsAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("scalar_text_cast_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlTextCastValueAlloc(alloc, actual);
        defer alloc.free(text);
        return std.mem.eql(u8, text, field_value.value.string);
    }
    if (filter.object.get("scalar_numeric_cast_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        if (actual == .null) return false;
        const number = try documentSqlNumericCastValue(actual);
        return try documentSqlNumericRangeValueMatches(number, range);
    }
    if (filter.object.get("scalar_boolean_cast_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .bool) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        return (try documentSqlBooleanCastValue(actual)) == field_value.value.bool;
    }
    if (filter.object.get("scalar_datetime_cast_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        if (actual == .null) return false;
        const timestamp = try documentSqlDatetimeCastValue(actual);
        return try documentSqlNumericRangeValueMatches(@floatFromInt(timestamp), range);
    }
    if (filter.object.get("text_nullif_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const nullif_value = try documentSqlFilterNamedValue(term, "nullif");
        if (nullif_value != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        if (std.mem.eql(u8, text, nullif_value.string)) return false;
        return std.mem.eql(u8, text, field_value.value.string);
    }
    if (filter.object.get("text_replace_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const needle = try documentSqlFilterNamedValue(term, "needle");
        if (needle != .string) return error.InvalidRowsRequest;
        const replacement = try documentSqlFilterNamedValue(term, "replacement");
        if (replacement != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const replaced = try documentSqlReplaceTextAlloc(alloc, text, needle.string, replacement.string);
        defer alloc.free(replaced);
        return std.mem.eql(u8, replaced, field_value.value.string);
    }
    if (filter.object.get("text_translate_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const from = try documentSqlFilterNamedValue(term, "from");
        if (from != .string) return error.InvalidRowsRequest;
        const to = try documentSqlFilterNamedValue(term, "to");
        if (to != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const translated = try documentSqlTranslateTextAlloc(alloc, text, from.string, to.string);
        defer alloc.free(translated);
        return std.mem.eql(u8, translated, field_value.value.string);
    }
    if (filter.object.get("text_concat_ws_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const path2_value = try documentSqlFilterNamedValue(term, "path2");
        if (path2_value != .string) return error.InvalidRowsRequest;
        const separator = try documentSqlFilterNamedValue(term, "separator");
        if (separator != .string) return error.InvalidRowsRequest;
        const left = try documentSqlOptionalProjectedStringValue(doc, field_value.path);
        const right = try documentSqlOptionalProjectedStringValue(doc, path2_value.string);
        const joined = try documentSqlConcatWsTextAlloc(alloc, separator.string, left, right);
        defer alloc.free(joined);
        return std.mem.eql(u8, joined, field_value.value.string);
    }
    if (filter.object.get("text_pad_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const side = try documentSqlFilterNamedValue(term, "side");
        if (side != .string) return error.InvalidRowsRequest;
        const width_value = try documentSqlFilterNamedValue(term, "width");
        if (width_value != .integer) return error.InvalidRowsRequest;
        const fill = try documentSqlFilterNamedValue(term, "fill");
        if (fill != .string) return error.InvalidRowsRequest;
        const left = if (std.mem.eql(u8, side.string, "left"))
            true
        else if (std.mem.eql(u8, side.string, "right"))
            false
        else
            return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const padded = try documentSqlPadTextAlloc(alloc, text, width_value.integer, fill.string, left);
        defer alloc.free(padded);
        return std.mem.eql(u8, padded, field_value.value.string);
    }
    if (filter.object.get("text_repeat_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const count_value = try documentSqlFilterNamedValue(term, "count");
        if (count_value != .integer) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const repeated = try documentSqlRepeatTextAlloc(alloc, text, count_value.integer);
        defer alloc.free(repeated);
        return std.mem.eql(u8, repeated, field_value.value.string);
    }
    if (filter.object.get("text_slice_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const side = try documentSqlFilterNamedValue(term, "side");
        if (side != .string) return error.InvalidRowsRequest;
        const count_value = try documentSqlFilterNamedValue(term, "count");
        if (count_value != .integer) return error.InvalidRowsRequest;
        const from_left = if (std.mem.eql(u8, side.string, "left"))
            true
        else if (std.mem.eql(u8, side.string, "right"))
            false
        else
            return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const sliced = try documentSqlLeftRightTextAlloc(alloc, text, count_value.integer, from_left);
        defer alloc.free(sliced);
        return std.mem.eql(u8, sliced, field_value.value.string);
    }
    if (filter.object.get("text_substring_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const start_value = try documentSqlFilterNamedValue(term, "start");
        if (start_value != .integer) return error.InvalidRowsRequest;
        const length_value = if (term == .object) term.object.get("length") else return error.InvalidRowsRequest;
        const length: ?i64 = if (length_value) |value| blk: {
            if (value != .integer) return error.InvalidRowsRequest;
            break :blk value.integer;
        } else null;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const substring = try documentSqlSubstringTextAlloc(alloc, text, start_value.integer, length);
        defer alloc.free(substring);
        return std.mem.eql(u8, substring, field_value.value.string);
    }
    if (filter.object.get("text_overlay_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const replacement = try documentSqlFilterNamedValue(term, "replacement");
        if (replacement != .string) return error.InvalidRowsRequest;
        const start_value = try documentSqlFilterNamedValue(term, "start");
        if (start_value != .integer) return error.InvalidRowsRequest;
        const length_value = if (term == .object) term.object.get("length") else return error.InvalidRowsRequest;
        const length: ?i64 = if (length_value) |value| blk: {
            if (value != .integer) return error.InvalidRowsRequest;
            break :blk value.integer;
        } else null;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const overlayed = try documentSqlOverlayTextAlloc(alloc, text, replacement.string, start_value.integer, length);
        defer alloc.free(overlayed);
        return std.mem.eql(u8, overlayed, field_value.value.string);
    }
    if (filter.object.get("text_split_part_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const delimiter = try documentSqlFilterNamedValue(term, "delimiter");
        if (delimiter != .string) return error.InvalidRowsRequest;
        const index_value = try documentSqlFilterNamedValue(term, "index");
        if (index_value != .integer) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const part = try documentSqlSplitPartTextAlloc(alloc, text, delimiter.string, index_value.integer);
        defer alloc.free(part);
        return std.mem.eql(u8, part, field_value.value.string);
    }
    if (filter.object.get("text_affix_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const side = try documentSqlFilterNamedValue(term, "side");
        if (side != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        if (std.mem.eql(u8, side.string, "prefix")) {
            return std.mem.startsWith(u8, text, field_value.value.string);
        }
        if (std.mem.eql(u8, side.string, "suffix")) {
            return std.mem.endsWith(u8, text, field_value.value.string);
        }
        return error.InvalidRowsRequest;
    }
    if (filter.object.get("text_lower_prefix")) |prefix| {
        const field_value = try documentSqlFilterFieldValue(prefix, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiLowerStartsWithAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("text_upper_prefix")) |prefix| {
        const field_value = try documentSqlFilterFieldValue(prefix, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiUpperStartsWithAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("text_lower_wildcard")) |wildcard| {
        const field_value = try documentSqlFilterFieldValue(wildcard, "pattern");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiLowerWildcardMatchesAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("text_upper_wildcard")) |wildcard| {
        const field_value = try documentSqlFilterFieldValue(wildcard, "pattern");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        if (!documentSqlAsciiOnly(field_value.value.string)) return error.UnsupportedSqlShape;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        return try documentSqlAsciiUpperWildcardMatchesAlloc(alloc, actual, field_value.value.string);
    }
    if (filter.object.get("text_length_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlTextLengthRangeMatches(actual, range);
    }
    if (filter.object.get("text_octet_length_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlTextByteLengthRangeMatches(actual, range, false);
    }
    if (filter.object.get("text_bit_length_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlTextByteLengthRangeMatches(actual, range, true);
    }
    if (filter.object.get("text_strpos_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const needle = try documentSqlFilterNamedValue(range, "needle");
        if (needle != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const position = try documentSqlStrposTextCodepointPosition(text, needle.string);
        return try documentSqlNumericRangeValueMatches(@floatFromInt(position), range);
    }
    if (filter.object.get("text_ascii_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const codepoint = try documentSqlAsciiFirstCodepoint(text);
        return try documentSqlNumericRangeValueMatches(@floatFromInt(codepoint), range);
    }
    if (filter.object.get("regexp_count_range")) |range| {
        return try documentSqlRegexpNumericRangeMatches(alloc, doc, range, .regexp_count);
    }
    if (filter.object.get("regexp_instr_range")) |range| {
        return try documentSqlRegexpNumericRangeMatches(alloc, doc, range, .regexp_instr);
    }
    if (filter.object.get("regexp_substr_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const pattern = try documentSqlFilterNamedValue(term, "pattern");
        if (pattern != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        const matched = try documentSqlRegexpSubstrTextAlloc(alloc, text, pattern.string);
        defer if (matched) |value_text| alloc.free(value_text);
        return if (matched) |value_text| std.mem.eql(u8, value_text, field_value.value.string) else false;
    }
    if (filter.object.get("text_chr_term")) |term| {
        if (term != .object) return error.InvalidRowsRequest;
        const expected = try documentSqlFilterNamedValue(term, "value");
        if (expected != .string) return error.InvalidRowsRequest;
        const path_value = term.object.get("path") orelse term.object.get("field");
        const codepoint_value = term.object.get("codepoint");
        if (path_value != null and codepoint_value != null) return error.InvalidRowsRequest;
        const codepoint = if (path_value) |path| blk: {
            if (path != .string) return error.InvalidRowsRequest;
            const actual = documentSqlProjectedValue(doc, path.string) orelse return false;
            if (actual == .null) return false;
            break :blk try documentSqlIntegerCodepointFromJson(actual);
        } else if (codepoint_value) |value| try documentSqlIntegerCodepointFromJson(value) else return error.InvalidRowsRequest;
        const encoded = try documentSqlChrTextAlloc(alloc, codepoint);
        defer alloc.free(encoded);
        return std.mem.eql(u8, encoded, expected.string);
    }
    if (filter.object.get("text_unary_term")) |term| {
        const field_value = try documentSqlFilterFieldValue(term, "value");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const operator = try documentSqlFilterNamedValue(term, "operator");
        if (operator != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        if (actual == .null) return false;
        const text = try documentSqlFilterStringValue(actual);
        if (std.mem.eql(u8, operator.string, "initcap")) {
            if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
            const titled = try documentSqlInitcapAsciiAlloc(alloc, text);
            defer alloc.free(titled);
            return std.mem.eql(u8, titled, field_value.value.string);
        }
        if (std.mem.eql(u8, operator.string, "md5")) {
            const digest = try expr_text.md5HexTextAlloc(alloc, text);
            defer alloc.free(digest);
            return std.mem.eql(u8, digest, field_value.value.string);
        }
        if (std.mem.eql(u8, operator.string, "soundex")) {
            const code = try expr_text.soundexTextAlloc(alloc, text);
            defer alloc.free(code);
            return std.mem.eql(u8, code, field_value.value.string);
        }
        if (std.mem.eql(u8, operator.string, "reverse")) {
            const reversed = try documentSqlReverseTextAlloc(alloc, text);
            defer alloc.free(reversed);
            return std.mem.eql(u8, reversed, field_value.value.string);
        }
        if (std.mem.eql(u8, operator.string, "btrim") or std.mem.eql(u8, operator.string, "ltrim") or std.mem.eql(u8, operator.string, "rtrim")) {
            const kind: sql_adapter.DocumentProjectionKind = if (std.mem.eql(u8, operator.string, "btrim"))
                .text_btrim
            else if (std.mem.eql(u8, operator.string, "ltrim"))
                .text_ltrim
            else
                .text_rtrim;
            const trimmed = documentSqlTrimText(text, kind);
            return std.mem.eql(u8, trimmed, field_value.value.string);
        }
        return error.InvalidRowsRequest;
    }
    if (filter.object.get("array_length_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlArrayLengthRangeMatches(actual, range);
    }
    if (filter.object.get("json_array_length_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlArrayLengthRangeMatches(actual, range);
    }
    if (filter.object.get("json_typeof_term")) |term| {
        const path = try documentSqlFilterPath(term);
        const expected = try documentSqlFilterNamedValue(term, "value");
        if (expected != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return std.mem.eql(u8, documentSqlJsonTypeofName(actual), expected.string);
    }
    if (filter.object.get("terms")) |terms| {
        const field_value = try documentSqlFilterFieldValue(terms, "values");
        if (field_value.value != .array) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        for (field_value.value.array.items) |expected| {
            if (documentSqlJsonValuesEqual(actual, expected)) return true;
        }
        return false;
    }
    if (filter.object.get("prefix")) |prefix| {
        const path = try documentSqlFilterPath(prefix);
        const expected = try documentSqlFilterNamedValue(prefix, "value");
        if (expected != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        const text = try documentSqlFilterStringValue(actual);
        return std.mem.startsWith(u8, text, expected.string);
    }
    if (filter.object.get("wildcard")) |wildcard| {
        const path = try documentSqlFilterPath(wildcard);
        const expected = try documentSqlFilterNamedValue(wildcard, "pattern");
        if (expected != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        const text = try documentSqlFilterStringValue(actual);
        return documentSqlWildcardMatches(expected.string, text);
    }
    if (filter.object.get("text_regex")) |regex| {
        const field_value = try documentSqlFilterFieldValue(regex, "pattern");
        if (field_value.value != .string) return error.InvalidRowsRequest;
        const actual = documentSqlProjectedValue(doc, field_value.path) orelse return false;
        const text = try documentSqlFilterStringValue(actual);
        const case_insensitive = documentSqlFilterBool(regex, "case_insensitive", false);
        return try documentSqlRegexMatchesAlloc(alloc, text, field_value.value.string, case_insensitive);
    }
    if (filter.object.get("numeric_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlNumericRangeMatches(actual, range);
    }
    if (filter.object.get("numeric_abs_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlNumericAbsRangeMatches(actual, range);
    }
    if (filter.object.get("numeric_arithmetic_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlNumericArithmeticRangeMatches(actual, range);
    }
    if (filter.object.get("numeric_unary_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlNumericUnaryRangeMatches(actual, range);
    }
    if (filter.object.get("date_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlStringRangeMatches(actual, range, "start", "end", "inclusive_start", "inclusive_end");
    }
    if (filter.object.get("term_range")) |range| {
        const path = try documentSqlFilterPath(range);
        const actual = documentSqlProjectedValue(doc, path) orelse return false;
        return try documentSqlStringRangeMatches(actual, range, "min", "max", "inclusive_min", "inclusive_max");
    }
    return error.InvalidRowsRequest;
}

fn documentSqlFilterConjunctionMatches(
    alloc: std.mem.Allocator,
    doc: std.json.Value,
    filter: std.json.Value,
) anyerror!bool {
    if (filter == .array) {
        for (filter.array.items) |item| {
            if (!try documentSqlFilterValueMatches(alloc, doc, item)) return false;
        }
        return true;
    }
    return try documentSqlFilterValueMatches(alloc, doc, filter);
}

fn documentSqlFilterShouldMatches(
    alloc: std.mem.Allocator,
    doc: std.json.Value,
    filter: std.json.Value,
    minimum_should_match: u32,
) anyerror!bool {
    var matches: u32 = 0;
    if (filter == .array) {
        for (filter.array.items) |item| {
            if (try documentSqlFilterValueMatches(alloc, doc, item)) matches += 1;
        }
    } else if (try documentSqlFilterValueMatches(alloc, doc, filter)) {
        matches += 1;
    }
    return matches >= minimum_should_match;
}

fn documentSqlFilterMinimumShouldMatch(bool_value: std.json.Value, default_value: u32) !u32 {
    if (bool_value != .object) return error.InvalidRowsRequest;
    const value = bool_value.object.get("minimum_should_match") orelse bool_value.object.get("min_should") orelse return default_value;
    return switch (value) {
        .integer => |item| if (item < 0) error.InvalidRowsRequest else @intCast(item),
        .number_string => |text| try std.fmt.parseUnsigned(u32, text, 10),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlFilterDisjunctionMatches(
    alloc: std.mem.Allocator,
    doc: std.json.Value,
    filter: std.json.Value,
) anyerror!bool {
    if (filter == .array) {
        for (filter.array.items) |item| {
            if (try documentSqlFilterValueMatches(alloc, doc, item)) return true;
        }
        return false;
    }
    return try documentSqlFilterValueMatches(alloc, doc, filter);
}

const DocumentSqlFilterFieldValue = struct {
    path: []const u8,
    value: std.json.Value,
};

fn documentSqlFilterFieldValue(value: std.json.Value, value_name: []const u8) !DocumentSqlFilterFieldValue {
    if (value != .object) return error.InvalidRowsRequest;
    if (value.object.get("path") orelse value.object.get("field")) |path| {
        if (path != .string) return error.InvalidRowsRequest;
        return .{
            .path = path.string,
            .value = try documentSqlFilterNamedValue(value, value_name),
        };
    }

    var it = value.object.iterator();
    const entry = it.next() orelse return error.InvalidRowsRequest;
    if (it.next() != null) return error.InvalidRowsRequest;
    return .{
        .path = entry.key_ptr.*,
        .value = entry.value_ptr.*,
    };
}

fn documentSqlFilterPath(value: std.json.Value) ![]const u8 {
    if (value != .object) return error.InvalidRowsRequest;
    const path = value.object.get("path") orelse value.object.get("field") orelse return error.InvalidRowsRequest;
    if (path != .string) return error.InvalidRowsRequest;
    return path.string;
}

fn documentSqlFilterNamedValue(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidRowsRequest;
    return value.object.get(name) orelse return error.InvalidRowsRequest;
}

fn documentSqlJsonValuesEqual(actual: std.json.Value, expected: std.json.Value) bool {
    return switch (expected) {
        .null => actual == .null,
        .bool => |value| actual == .bool and actual.bool == value,
        .integer, .float, .number_string => blk: {
            const left = documentSqlJsonNumber(actual) catch break :blk false;
            const right = documentSqlJsonNumber(expected) catch break :blk false;
            break :blk left == right;
        },
        .string => |value| blk: {
            const text = documentSqlFilterStringValue(actual) catch break :blk false;
            break :blk std.mem.eql(u8, text, value);
        },
        else => false,
    };
}

fn documentSqlJsonValueInArray(actual: std.json.Value, expected_values: std.json.Value) bool {
    if (expected_values != .array) return false;
    for (expected_values.array.items) |expected| {
        if (documentSqlJsonValuesEqual(actual, expected)) return true;
    }
    return false;
}

fn documentSqlUnnestItemRangeMatches(item: std.json.Value, filter: std.json.Value) !bool {
    if (filter != .object) return error.InvalidRowsRequest;
    if (filter.object.get("numeric_range")) |range| {
        if (!documentSqlJsonValueIsNumber(item)) return false;
        return try documentSqlNumericRangeMatches(item, range);
    }
    if (filter.object.get("date_range")) |range| {
        if (!documentSqlJsonValueIsStringLike(item)) return false;
        return try documentSqlStringRangeMatches(item, range, "start", "end", "inclusive_start", "inclusive_end");
    }
    if (filter.object.get("term_range")) |range| {
        if (!documentSqlJsonValueIsStringLike(item)) return false;
        return try documentSqlStringRangeMatches(item, range, "min", "max", "inclusive_min", "inclusive_max");
    }
    return error.InvalidRowsRequest;
}

fn documentSqlJsonValueIsNumber(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float, .number_string => true,
        else => false,
    };
}

fn documentSqlJsonValueIsStringLike(value: std.json.Value) bool {
    return switch (value) {
        .string, .number_string => true,
        else => false,
    };
}

fn documentSqlUnnestItemPatternMatches(item: std.json.Value, pattern: std.json.Value, case_insensitive: bool) bool {
    if (pattern != .string) return false;
    const text = documentSqlFilterStringValue(item) catch return false;
    return if (case_insensitive)
        documentSqlWildcardMatchesIgnoreCase(pattern.string, text)
    else
        documentSqlWildcardMatches(pattern.string, text);
}

fn documentSqlJsonNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |text| try std.fmt.parseFloat(f64, text),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlNumericCastValue(value: std.json.Value) !f64 {
    return switch (value) {
        .integer, .float, .number_string => try documentSqlJsonNumber(value),
        .string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidRowsRequest,
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlNumericUnaryResult(number: f64, kind: sql_adapter.DocumentProjectionKind) !f64 {
    const result = switch (kind) {
        .numeric_round => @round(number),
        .numeric_trunc => @trunc(number),
        .numeric_floor => @floor(number),
        .numeric_ceil => @ceil(number),
        .numeric_sqrt => if (number < 0) return error.InvalidRowsRequest else @sqrt(number),
        .numeric_sign => if (number < 0) @as(f64, -1) else if (number > 0) @as(f64, 1) else @as(f64, 0),
        else => unreachable,
    };
    if (!std.math.isFinite(result)) return error.InvalidRowsRequest;
    return result;
}

fn documentSqlTextCastValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |boolean| try alloc.dupe(u8, if (boolean) "true" else "false"),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlBooleanCastValue(value: std.json.Value) !bool {
    return switch (value) {
        .bool => |enabled| enabled,
        .string => |text| if (std.mem.eql(u8, text, "true"))
            true
        else if (std.mem.eql(u8, text, "false"))
            false
        else
            error.InvalidRowsRequest,
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlDatetimeCastValue(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @as(u64, @intCast(integer)) else error.InvalidRowsRequest,
        .number_string => |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidRowsRequest,
        .string => |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidRowsRequest,
        else => error.InvalidRowsRequest,
    };
}

const document_sql_nanoseconds_per_day: u64 = 86_400_000_000_000;

fn documentSqlUtcDateFromDatetimeAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    const timestamp_ns = try documentSqlDatetimeCastValue(value);
    const epoch_days = timestamp_ns / document_sql_nanoseconds_per_day;
    if (epoch_days > std.math.maxInt(i64)) return error.InvalidRowsRequest;
    const day = documentSqlUtcDateFromEpochDays(@intCast(epoch_days));
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}", .{ day.year, day.month, day.day });
}

const DocumentSqlUtcDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

fn documentSqlUtcDateFromEpochDays(epoch_days: i64) DocumentSqlUtcDate {
    const z = epoch_days + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    var year = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + if (mp < 10) @as(i64, 3) else @as(i64, -9);
    if (month <= 2) year += 1;
    return .{ .year = year, .month = month, .day = day };
}

fn documentSqlJsonTypeofName(value: std.json.Value) []const u8 {
    return switch (value) {
        .object => "object",
        .array => "array",
        .string => "string",
        .integer, .float, .number_string => "number",
        .bool => "boolean",
        .null => "null",
    };
}

fn documentSqlFilterStringValue(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        .number_string => |text| text,
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlOptionalProjectedStringValue(row: std.json.Value, field: []const u8) !?[]const u8 {
    const value = documentSqlProjectedValue(row, field) orelse return null;
    if (value == .null) return null;
    return try documentSqlFilterStringValue(value);
}

fn documentSqlConcatWsTextAlloc(
    alloc: std.mem.Allocator,
    separator: []const u8,
    left: ?[]const u8,
    right: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var wrote = false;
    if (left) |text| {
        try out.appendSlice(alloc, text);
        wrote = true;
    }
    if (right) |text| {
        if (wrote) try out.appendSlice(alloc, separator);
        try out.appendSlice(alloc, text);
    }
    return try out.toOwnedSlice(alloc);
}

fn documentSqlArrayToStringTextAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    separator: []const u8,
) ![]u8 {
    if (value != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var wrote = false;
    for (value.array.items) |item| {
        if (item == .null) continue;
        const text = try documentSqlFilterStringValue(item);
        if (wrote) try out.appendSlice(alloc, separator);
        try out.appendSlice(alloc, text);
        wrote = true;
    }
    return try out.toOwnedSlice(alloc);
}

fn documentSqlArrayPosition(value: std.json.Value, needle: []const u8) !?usize {
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items, 0..) |item, i| {
        if (item == .null) continue;
        const text = try documentSqlFilterStringValue(item);
        if (std.mem.eql(u8, text, needle)) return i + 1;
    }
    return null;
}

fn documentSqlArrayPositionsJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    needle: []const u8,
) ![]u8 {
    if (value != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    var wrote = false;
    for (value.array.items, 0..) |item, i| {
        if (item == .null) continue;
        const text = try documentSqlFilterStringValue(item);
        if (!std.mem.eql(u8, text, needle)) continue;
        if (wrote) try out.append(alloc, ',');
        const position_text = try std.fmt.allocPrint(alloc, "{d}", .{i + 1});
        defer alloc.free(position_text);
        try out.appendSlice(alloc, position_text);
        wrote = true;
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn documentSqlArrayTransformJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    kind: sql_adapter.DocumentProjectionKind,
    pattern: []const u8,
    replacement: []const u8,
) ![]u8 {
    if (value != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    var wrote = false;
    if (kind == .array_prepend) {
        try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, pattern);
    }
    for (value.array.items) |item| {
        if (item == .null) {
            try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, null);
            continue;
        }
        const text = try documentSqlFilterStringValue(item);
        switch (kind) {
            .array_append, .array_cat, .array_prepend => try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, text),
            .array_remove => if (!std.mem.eql(u8, text, pattern)) try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, text),
            .array_replace => try documentSqlAppendArrayTransformItem(
                alloc,
                &out,
                &wrote,
                if (std.mem.eql(u8, text, pattern)) replacement else text,
            ),
            else => unreachable,
        }
    }
    if (kind == .array_append) {
        try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, pattern);
    } else if (kind == .array_cat) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, pattern, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
        for (parsed.value.array.items) |item| {
            const text = try documentSqlFilterStringValue(item);
            try documentSqlAppendArrayTransformItem(alloc, &out, &wrote, text);
        }
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn documentSqlAppendArrayTransformItem(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    wrote: *bool,
    text: ?[]const u8,
) !void {
    if (wrote.*) try out.append(alloc, ',');
    if (text) |value| {
        try appendJsonString(alloc, out, value);
    } else {
        try out.appendSlice(alloc, "null");
    }
    wrote.* = true;
}

fn documentSqlStringToArrayJsonAlloc(
    alloc: std.mem.Allocator,
    text: []const u8,
    separator: []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    if (separator.len == 0) {
        try appendJsonString(alloc, &out, text);
    } else {
        var split = std.mem.splitSequence(u8, text, separator);
        var index: usize = 0;
        while (split.next()) |part| : (index += 1) {
            if (index > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, part);
        }
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn documentSqlAsciiLowerEqualsAlloc(alloc: std.mem.Allocator, actual: std.json.Value, expected: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const lowered = try std.ascii.allocLowerString(alloc, text);
    defer alloc.free(lowered);
    return std.mem.eql(u8, lowered, expected);
}

fn documentSqlAsciiUpperEqualsAlloc(alloc: std.mem.Allocator, actual: std.json.Value, expected: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const uppered = try std.ascii.allocUpperString(alloc, text);
    defer alloc.free(uppered);
    return std.mem.eql(u8, uppered, expected);
}

fn documentSqlAsciiLowerStartsWithAlloc(alloc: std.mem.Allocator, actual: std.json.Value, expected: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const lowered = try std.ascii.allocLowerString(alloc, text);
    defer alloc.free(lowered);
    return std.mem.startsWith(u8, lowered, expected);
}

fn documentSqlAsciiUpperStartsWithAlloc(alloc: std.mem.Allocator, actual: std.json.Value, expected: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const uppered = try std.ascii.allocUpperString(alloc, text);
    defer alloc.free(uppered);
    return std.mem.startsWith(u8, uppered, expected);
}

fn documentSqlAsciiLowerWildcardMatchesAlloc(alloc: std.mem.Allocator, actual: std.json.Value, pattern: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const lowered = try std.ascii.allocLowerString(alloc, text);
    defer alloc.free(lowered);
    return documentSqlWildcardMatches(pattern, lowered);
}

fn documentSqlAsciiUpperWildcardMatchesAlloc(alloc: std.mem.Allocator, actual: std.json.Value, pattern: []const u8) !bool {
    const text = try documentSqlFilterStringValue(actual);
    if (!documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const uppered = try std.ascii.allocUpperString(alloc, text);
    defer alloc.free(uppered);
    return documentSqlWildcardMatches(pattern, uppered);
}

fn documentSqlRegexMatchesAlloc(alloc: std.mem.Allocator, text: []const u8, pattern: []const u8, case_insensitive: bool) !bool {
    if (!case_insensitive) {
        var regex = regex_mod.compile(alloc, pattern) catch return error.InvalidRowsRequest;
        defer regex.deinit();
        return regex_mod.matchesCompiled(pattern, &regex, text);
    }
    if (!documentSqlAsciiOnly(pattern) or !documentSqlAsciiOnly(text)) return error.UnsupportedSqlShape;
    const folded_pattern = try std.ascii.allocLowerString(alloc, pattern);
    defer alloc.free(folded_pattern);
    const folded_text = try std.ascii.allocLowerString(alloc, text);
    defer alloc.free(folded_text);
    var regex = regex_mod.compile(alloc, folded_pattern) catch return error.InvalidRowsRequest;
    defer regex.deinit();
    return regex_mod.matchesCompiled(folded_pattern, &regex, folded_text);
}

const DocumentSqlRegexpMatchSpan = struct {
    start: usize,
    end: usize,
};

fn documentSqlRegexpCountText(alloc: std.mem.Allocator, source: []const u8, pattern: []const u8) !u64 {
    var compiled = regex_mod.compile(alloc, pattern) catch return error.InvalidRowsRequest;
    defer compiled.deinit();

    var count: u64 = 0;
    var cursor: usize = 0;
    while (cursor <= source.len) {
        const span = documentSqlRegexpFindLeftmostMatch(&compiled, source, cursor) orelse break;
        if (span.end <= span.start) return error.InvalidRowsRequest;
        count += 1;
        cursor = span.end;
    }
    return count;
}

fn documentSqlRegexpInstrText(alloc: std.mem.Allocator, source: []const u8, pattern: []const u8) !usize {
    var compiled = regex_mod.compile(alloc, pattern) catch return error.InvalidRowsRequest;
    defer compiled.deinit();

    const span = documentSqlRegexpFindLeftmostMatch(&compiled, source, 0) orelse return 0;
    if (span.end <= span.start) return error.InvalidRowsRequest;
    const codepoints_before = std.unicode.utf8CountCodepoints(source[0..span.start]) catch return error.InvalidRowsRequest;
    return codepoints_before + 1;
}

fn documentSqlRegexpSubstrTextAlloc(alloc: std.mem.Allocator, source: []const u8, pattern: []const u8) !?[]u8 {
    var compiled = regex_mod.compile(alloc, pattern) catch return error.InvalidRowsRequest;
    defer compiled.deinit();

    const span = documentSqlRegexpFindLeftmostMatch(&compiled, source, 0) orelse return null;
    if (span.end <= span.start) return error.InvalidRowsRequest;
    return try alloc.dupe(u8, source[span.start..span.end]);
}

fn documentSqlRegexpNumericRangeMatches(alloc: std.mem.Allocator, doc: std.json.Value, range: std.json.Value, kind: sql_adapter.DocumentProjectionKind) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const path = try documentSqlFilterPath(range);
    const pattern = try documentSqlFilterNamedValue(range, "pattern");
    if (pattern != .string) return error.InvalidRowsRequest;
    const actual = documentSqlProjectedValue(doc, path) orelse return false;
    if (actual == .null) return false;
    const text = try documentSqlFilterStringValue(actual);
    const value = switch (kind) {
        .regexp_count => @as(f64, @floatFromInt(try documentSqlRegexpCountText(alloc, text, pattern.string))),
        .regexp_instr => @as(f64, @floatFromInt(try documentSqlRegexpInstrText(alloc, text, pattern.string))),
        else => return error.InvalidRowsRequest,
    };
    return try documentSqlNumericRangeValueMatches(value, range);
}

fn documentSqlRegexpFindLeftmostMatch(compiled: *regex_mod.RegexAutomaton, text: []const u8, start_at: usize) ?DocumentSqlRegexpMatchSpan {
    if (start_at > text.len) return null;
    const start_limit: usize = if (compiled.anchored_start) 1 else text.len + 1;
    if (start_at >= start_limit) return null;
    var start = start_at;
    while (start < start_limit) : (start += 1) {
        if (documentSqlRegexpMatchEndFrom(compiled, text, start)) |end| {
            return .{ .start = start, .end = end };
        }
    }
    return null;
}

fn documentSqlRegexpMatchEndFrom(compiled: *regex_mod.RegexAutomaton, text: []const u8, start: usize) ?usize {
    const automaton = compiled.automaton();
    var state = automaton.start();
    var latest_match: ?usize = null;
    if (automaton.isMatch(state) and (!compiled.anchored_end or start == text.len)) latest_match = start;
    for (text[start..], 0..) |byte, offset| {
        state = automaton.accept(state, byte);
        if (!automaton.canMatch(state)) break;
        if (automaton.isMatch(state)) {
            const end = start + offset + 1;
            if (!compiled.anchored_end or end == text.len) latest_match = end;
        }
    }
    return latest_match;
}

fn documentSqlStrposTextCodepointPosition(text: []const u8, needle: []const u8) !usize {
    if (needle.len == 0) return 1;
    const byte_index = std.mem.indexOf(u8, text, needle) orelse return 0;
    const codepoints_before = std.unicode.utf8CountCodepoints(text[0..byte_index]) catch return error.InvalidRowsRequest;
    return codepoints_before + 1;
}

fn documentSqlSplitPartTextAlloc(alloc: std.mem.Allocator, text: []const u8, delimiter: []const u8, field_index: i64) ![]u8 {
    if (field_index == 0) return error.InvalidRowsRequest;
    if (delimiter.len == 0) return try alloc.dupe(u8, if (field_index == 1 or field_index == -1) text else "");

    if (field_index > 0) {
        var split = std.mem.splitSequence(u8, text, delimiter);
        var current: i64 = 1;
        while (split.next()) |part| : (current += 1) {
            if (current == field_index) return try alloc.dupe(u8, part);
        }
        return try alloc.dupe(u8, "");
    }

    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var split = std.mem.splitSequence(u8, text, delimiter);
    while (split.next()) |part| try parts.append(alloc, part);
    const from_end: usize = @intCast(-field_index);
    if (from_end == 0 or from_end > parts.items.len) return try alloc.dupe(u8, "");
    return try alloc.dupe(u8, parts.items[parts.items.len - from_end]);
}

fn documentSqlSubstringTextAlloc(alloc: std.mem.Allocator, text: []const u8, start_index: i64, length_count: ?i64) ![]u8 {
    if (start_index < 1) return error.InvalidRowsRequest;
    if (length_count) |count| if (count < 0) return error.InvalidRowsRequest;
    const start_codepoint: usize = @intCast(start_index - 1);
    const end_codepoint: ?usize = if (length_count) |count| start_codepoint + @as(usize, @intCast(count)) else null;
    const start_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, start_codepoint);
    const end_byte = if (end_codepoint) |index| try documentSqlUtf8ByteOffsetForCodepointIndex(text, index) else text.len;
    return try alloc.dupe(u8, text[start_byte..end_byte]);
}

fn documentSqlOverlayTextAlloc(alloc: std.mem.Allocator, text: []const u8, replacement: []const u8, start_index: i64, length_count: ?i64) ![]u8 {
    if (start_index < 1) return error.InvalidRowsRequest;
    if (length_count) |count| if (count < 0) return error.InvalidRowsRequest;
    const replacement_codepoints = std.unicode.utf8CountCodepoints(replacement) catch return error.InvalidRowsRequest;
    const start_codepoint: usize = @intCast(start_index - 1);
    const replaced_codepoints: usize = if (length_count) |count| @intCast(count) else replacement_codepoints;
    const end_codepoint = start_codepoint + replaced_codepoints;
    const start_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, start_codepoint);
    const end_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, end_codepoint);
    var out = try std.ArrayListUnmanaged(u8).initCapacity(alloc, start_byte + replacement.len + (text.len - end_byte));
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, text[0..start_byte]);
    try out.appendSlice(alloc, replacement);
    try out.appendSlice(alloc, text[end_byte..]);
    return try out.toOwnedSlice(alloc);
}

fn documentSqlLeftRightTextAlloc(alloc: std.mem.Allocator, text: []const u8, count: i64, from_left: bool) ![]u8 {
    const total_codepoints = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
    const abs_count: usize = if (count < 0) @intCast(-count) else @intCast(count);
    const slice_start: usize, const slice_end: usize = if (from_left) blk: {
        if (count >= 0) {
            break :blk .{ 0, @min(abs_count, total_codepoints) };
        }
        break :blk .{ 0, total_codepoints - @min(abs_count, total_codepoints) };
    } else blk: {
        if (count >= 0) {
            const kept = @min(abs_count, total_codepoints);
            break :blk .{ total_codepoints - kept, total_codepoints };
        }
        break :blk .{ @min(abs_count, total_codepoints), total_codepoints };
    };
    const start_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, slice_start);
    const end_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, slice_end);
    return try alloc.dupe(u8, text[start_byte..end_byte]);
}

fn documentSqlRepeatTextAlloc(alloc: std.mem.Allocator, text: []const u8, count: i64) ![]u8 {
    if (count < 0) return error.InvalidRowsRequest;
    const repeat_count: usize = @intCast(count);
    if (repeat_count == 0 or text.len == 0) return try alloc.dupe(u8, "");
    if (text.len > std.math.maxInt(usize) / repeat_count) return error.InvalidRowsRequest;
    const total_len = text.len * repeat_count;
    var out = try alloc.alloc(u8, total_len);
    var offset: usize = 0;
    while (offset < total_len) : (offset += text.len) {
        @memcpy(out[offset .. offset + text.len], text);
    }
    return out;
}

fn documentSqlReverseTextAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    const total_codepoints = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
    var out = try alloc.alloc(u8, text.len);
    var out_offset: usize = 0;
    var index = total_codepoints;
    while (index > 0) {
        index -= 1;
        const start_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, index);
        const end_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, index + 1);
        @memcpy(out[out_offset .. out_offset + (end_byte - start_byte)], text[start_byte..end_byte]);
        out_offset += end_byte - start_byte;
    }
    return out;
}

fn documentSqlTrimText(text: []const u8, kind: sql_adapter.DocumentProjectionKind) []const u8 {
    var start: usize = 0;
    var end: usize = text.len;
    if (kind == .text_btrim or kind == .text_ltrim) {
        while (start < end and text[start] == ' ') : (start += 1) {}
    }
    if (kind == .text_btrim or kind == .text_rtrim) {
        while (end > start and text[end - 1] == ' ') : (end -= 1) {}
    }
    return text[start..end];
}

fn documentSqlAsciiFirstCodepoint(text: []const u8) !u21 {
    if (text.len == 0) return 0;
    const width = std.unicode.utf8ByteSequenceLength(text[0]) catch return error.InvalidRowsRequest;
    if (width > text.len) return error.InvalidRowsRequest;
    return std.unicode.utf8Decode(text[0..width]) catch return error.InvalidRowsRequest;
}

fn documentSqlReplaceTextAlloc(alloc: std.mem.Allocator, text: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    if (needle.len == 0) return try alloc.dupe(u8, text);
    var replacement_count: usize = 0;
    var scan_start: usize = 0;
    while (std.mem.indexOf(u8, text[scan_start..], needle)) |relative_index| {
        replacement_count += 1;
        scan_start += relative_index + needle.len;
    }
    if (replacement_count == 0) return try alloc.dupe(u8, text);

    const removed_len = replacement_count * needle.len;
    const added_len = replacement_count * replacement.len;
    const total_len = text.len - removed_len + added_len;
    var out = try alloc.alloc(u8, total_len);
    var input_offset: usize = 0;
    var output_offset: usize = 0;
    while (std.mem.indexOf(u8, text[input_offset..], needle)) |relative_index| {
        const match_start = input_offset + relative_index;
        @memcpy(out[output_offset .. output_offset + (match_start - input_offset)], text[input_offset..match_start]);
        output_offset += match_start - input_offset;
        @memcpy(out[output_offset .. output_offset + replacement.len], replacement);
        output_offset += replacement.len;
        input_offset = match_start + needle.len;
    }
    @memcpy(out[output_offset..], text[input_offset..]);
    return out;
}

fn documentSqlTranslateTextAlloc(alloc: std.mem.Allocator, text: []const u8, from: []const u8, to: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var offset: usize = 0;
    while (offset < text.len) {
        const width = std.unicode.utf8ByteSequenceLength(text[offset]) catch return error.InvalidRowsRequest;
        if (offset + width > text.len) return error.InvalidRowsRequest;
        const codepoint = std.unicode.utf8Decode(text[offset .. offset + width]) catch return error.InvalidRowsRequest;
        if (try documentSqlTranslateSourceIndex(from, codepoint)) |source_index| {
            if (try documentSqlCodepointSliceAt(to, source_index)) |replacement| {
                try out.appendSlice(alloc, replacement);
            }
        } else {
            try out.appendSlice(alloc, text[offset .. offset + width]);
        }
        offset += width;
    }
    _ = try documentSqlTranslateSourceIndex(from, 0);
    _ = try documentSqlCodepointSliceAt(to, std.math.maxInt(usize));
    return try out.toOwnedSlice(alloc);
}

fn documentSqlTranslateSourceIndex(source: []const u8, target: u21) !?usize {
    var offset: usize = 0;
    var index: usize = 0;
    while (offset < source.len) : (index += 1) {
        const width = std.unicode.utf8ByteSequenceLength(source[offset]) catch return error.InvalidRowsRequest;
        if (offset + width > source.len) return error.InvalidRowsRequest;
        const codepoint = std.unicode.utf8Decode(source[offset .. offset + width]) catch return error.InvalidRowsRequest;
        if (codepoint == target) return index;
        offset += width;
    }
    return null;
}

fn documentSqlCodepointSliceAt(text: []const u8, target_index: usize) !?[]const u8 {
    var offset: usize = 0;
    var index: usize = 0;
    while (offset < text.len) : (index += 1) {
        const width = std.unicode.utf8ByteSequenceLength(text[offset]) catch return error.InvalidRowsRequest;
        if (offset + width > text.len) return error.InvalidRowsRequest;
        _ = std.unicode.utf8Decode(text[offset .. offset + width]) catch return error.InvalidRowsRequest;
        if (index == target_index) return text[offset .. offset + width];
        offset += width;
    }
    return null;
}

fn documentSqlPadTextAlloc(alloc: std.mem.Allocator, text: []const u8, width: i64, fill: []const u8, left: bool) ![]u8 {
    if (width < 0 or fill.len == 0) return error.InvalidRowsRequest;
    const target_codepoints: usize = @intCast(width);
    const text_codepoints = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
    if (text_codepoints >= target_codepoints) {
        const end_byte = try documentSqlUtf8ByteOffsetForCodepointIndex(text, target_codepoints);
        return try alloc.dupe(u8, text[0..end_byte]);
    }
    _ = std.unicode.utf8CountCodepoints(fill) catch return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const pad_codepoints = target_codepoints - text_codepoints;
    if (left) try documentSqlAppendFillCodepoints(alloc, &out, fill, pad_codepoints);
    try out.appendSlice(alloc, text);
    if (!left) try documentSqlAppendFillCodepoints(alloc, &out, fill, pad_codepoints);
    return try out.toOwnedSlice(alloc);
}

fn documentSqlAppendFillCodepoints(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), fill: []const u8, count: usize) !void {
    var remaining = count;
    while (remaining > 0) {
        var offset: usize = 0;
        while (offset < fill.len and remaining > 0) : (remaining -= 1) {
            const width = std.unicode.utf8ByteSequenceLength(fill[offset]) catch return error.InvalidRowsRequest;
            if (offset + width > fill.len) return error.InvalidRowsRequest;
            _ = std.unicode.utf8Decode(fill[offset .. offset + width]) catch return error.InvalidRowsRequest;
            try out.appendSlice(alloc, fill[offset .. offset + width]);
            offset += width;
        }
    }
}

fn documentSqlUtf8ByteOffsetForCodepointIndex(text: []const u8, codepoint_index: usize) !usize {
    var offset: usize = 0;
    var current: usize = 0;
    while (offset < text.len and current < codepoint_index) : (current += 1) {
        const width = std.unicode.utf8ByteSequenceLength(text[offset]) catch return error.InvalidRowsRequest;
        if (offset + width > text.len) return error.InvalidRowsRequest;
        _ = std.unicode.utf8Decode(text[offset .. offset + width]) catch return error.InvalidRowsRequest;
        offset += width;
    }
    return offset;
}

fn documentSqlAsciiOnly(text: []const u8) bool {
    for (text) |ch| {
        if (ch >= 0x80) return false;
    }
    return true;
}

fn documentSqlInitcapAsciiAlloc(alloc: std.mem.Allocator, text: []const u8) ![]u8 {
    var out = try alloc.dupe(u8, text);
    var in_word = false;
    for (out, 0..) |ch, index| {
        if (std.ascii.isAlphanumeric(ch)) {
            out[index] = if (!in_word) std.ascii.toUpper(ch) else std.ascii.toLower(ch);
            in_word = true;
        } else {
            in_word = false;
        }
    }
    return out;
}

fn documentSqlIntegerCodepointFromJson(value: std.json.Value) !i64 {
    const number = try documentSqlJsonNumber(value);
    if (!std.math.isFinite(number)) return error.InvalidRowsRequest;
    const truncated = @trunc(number);
    if (truncated != number) return error.InvalidRowsRequest;
    return @intFromFloat(number);
}

fn documentSqlChrTextAlloc(alloc: std.mem.Allocator, codepoint: i64) ![]u8 {
    if (codepoint < 0 or codepoint > std.math.maxInt(u21)) return error.InvalidRowsRequest;
    var buf: [4]u8 = undefined;
    const len = std.unicode.utf8Encode(@intCast(codepoint), &buf) catch return error.InvalidRowsRequest;
    return try alloc.dupe(u8, buf[0..len]);
}

fn documentSqlNumericRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const value = try documentSqlJsonNumber(actual);
    return try documentSqlNumericRangeValueMatches(value, range);
}

fn documentSqlNumericAbsRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const value = try documentSqlJsonNumber(actual);
    return try documentSqlNumericRangeValueMatches(@abs(value), range);
}

fn documentSqlNumericArithmeticRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const value = try documentSqlJsonNumber(actual);
    const operator = try documentSqlFilterNamedValue(range, "operator");
    if (operator != .string) return error.InvalidRowsRequest;
    const operand = try documentSqlJsonNumber(try documentSqlFilterNamedValue(range, "operand"));
    const computed = if (std.mem.eql(u8, operator.string, "add"))
        value + operand
    else if (std.mem.eql(u8, operator.string, "sub"))
        value - operand
    else if (std.mem.eql(u8, operator.string, "mul"))
        value * operand
    else if (std.mem.eql(u8, operator.string, "div"))
        if (operand == 0) return error.InvalidRowsRequest else value / operand
    else if (std.mem.eql(u8, operator.string, "mod"))
        if (operand == 0) return error.InvalidRowsRequest else value - @trunc(value / operand) * operand
    else if (std.mem.eql(u8, operator.string, "power"))
        std.math.pow(f64, value, operand)
    else
        return error.InvalidRowsRequest;
    if (!std.math.isFinite(computed)) return error.InvalidRowsRequest;
    return try documentSqlNumericRangeValueMatches(computed, range);
}

fn documentSqlNumericUnaryRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const value = try documentSqlJsonNumber(actual);
    const operator = try documentSqlFilterNamedValue(range, "operator");
    if (operator != .string) return error.InvalidRowsRequest;
    const kind = documentSqlNumericUnaryProjectionKind(operator.string) orelse return error.InvalidRowsRequest;
    const computed = try documentSqlNumericUnaryResult(value, kind);
    return try documentSqlNumericRangeValueMatches(computed, range);
}

fn documentSqlNumericUnaryProjectionKind(operator: []const u8) ?sql_adapter.DocumentProjectionKind {
    if (std.mem.eql(u8, operator, "round")) return .numeric_round;
    if (std.mem.eql(u8, operator, "trunc")) return .numeric_trunc;
    if (std.mem.eql(u8, operator, "floor")) return .numeric_floor;
    if (std.mem.eql(u8, operator, "ceil")) return .numeric_ceil;
    if (std.mem.eql(u8, operator, "sqrt")) return .numeric_sqrt;
    if (std.mem.eql(u8, operator, "sign")) return .numeric_sign;
    return null;
}

fn documentSqlTextLengthRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const text = try documentSqlFilterStringValue(actual);
    const length = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
    return try documentSqlNumericRangeValueMatches(@floatFromInt(length), range);
}

fn documentSqlTextByteLengthRangeMatches(actual: std.json.Value, range: std.json.Value, bits: bool) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const text = try documentSqlFilterStringValue(actual);
    const length = if (bits) text.len * 8 else text.len;
    return try documentSqlNumericRangeValueMatches(@floatFromInt(length), range);
}

fn documentSqlArrayLengthRangeMatches(actual: std.json.Value, range: std.json.Value) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    if (actual == .null) return false;
    if (actual != .array) return error.InvalidRowsRequest;
    return try documentSqlNumericRangeValueMatches(@floatFromInt(actual.array.items.len), range);
}

fn documentSqlNumericRangeValueMatches(value: f64, range: std.json.Value) !bool {
    if (range.object.get("min")) |min| {
        const bound = try documentSqlJsonNumber(min);
        const inclusive = documentSqlFilterBool(range, "inclusive_min", true);
        if (if (inclusive) value < bound else value <= bound) return false;
    }
    if (range.object.get("max")) |max| {
        const bound = try documentSqlJsonNumber(max);
        const inclusive = documentSqlFilterBool(range, "inclusive_max", false);
        if (if (inclusive) value > bound else value >= bound) return false;
    }
    return true;
}

fn documentSqlStringRangeMatches(
    actual: std.json.Value,
    range: std.json.Value,
    min_name: []const u8,
    max_name: []const u8,
    inclusive_min_name: []const u8,
    inclusive_max_name: []const u8,
) !bool {
    if (range != .object) return error.InvalidRowsRequest;
    const value = try documentSqlFilterStringValue(actual);
    if (range.object.get(min_name)) |min| {
        const bound = try documentSqlFilterStringValue(min);
        const order = std.mem.order(u8, value, bound);
        const inclusive = documentSqlFilterBool(range, inclusive_min_name, true);
        if (if (inclusive) order == .lt else order != .gt) return false;
    }
    if (range.object.get(max_name)) |max| {
        const bound = try documentSqlFilterStringValue(max);
        const order = std.mem.order(u8, value, bound);
        const inclusive = documentSqlFilterBool(range, inclusive_max_name, false);
        if (if (inclusive) order == .gt else order != .lt) return false;
    }
    return true;
}

fn documentSqlFilterBool(value: std.json.Value, name: []const u8, default_value: bool) bool {
    if (value != .object) return default_value;
    const item = value.object.get(name) orelse return default_value;
    return item == .bool and item.bool;
}

test "document sql filter-only index query requests include match_all base query" {
    const alloc = std.testing.allocator;
    var req = try documentSqlIndexQueryRequestAlloc(
        alloc,
        .{ .filter_query_json = "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}" },
        10,
        false,
        false,
    );
    defer req.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, req.body_json, "\"full_text_search\":{\"match_all\":{}}") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body_json, "\"_filter_query_json\":\"{\\\"term\\\":{\\\"status\\\":\\\"active\\\"}}\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body_json, "\"limit\":10") != null);
}

test "document sql native index query requests forward raw body" {
    const alloc = std.testing.allocator;
    var req = try documentSqlIndexQueryRequestAlloc(
        alloc,
        .{ .native_query_json = "{\"embeddings\":{\"docs_embedding_hnsw\":[0.1,0.2,0.3]},\"indexes\":[\"docs_embedding_hnsw\"],\"limit\":5}" },
        null,
        false,
        false,
    );
    defer req.deinit(alloc);

    try std.testing.expectEqualStrings("{\"embeddings\":{\"docs_embedding_hnsw\":[0.1,0.2,0.3]},\"indexes\":[\"docs_embedding_hnsw\"],\"limit\":5}", req.body_json);
    try std.testing.expect(std.mem.indexOf(u8, req.body_json, "\"full_text_search\"") == null);

    var capped_req = try documentSqlIndexQueryRequestAlloc(
        alloc,
        .{ .native_query_json = "{\"embeddings\":{\"docs_embedding_hnsw\":[0.1,0.2,0.3]},\"indexes\":[\"docs_embedding_hnsw\"],\"limit\":5}" },
        25,
        false,
        false,
    );
    defer capped_req.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, capped_req.body_json, "\"limit\":25") != null);
    try std.testing.expect(std.mem.indexOf(u8, capped_req.body_json, "\"limit\":5") == null);
}

test "document sql native derived-index producers execute equivalent native requests" {
    const alloc = std.testing.allocator;

    const schema = storage_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "body", .path = "body", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };

    const RecordingSource = struct {
        calls: usize = 0,
        last_table: ?[]u8 = null,
        last_body: ?[]u8 = null,

        fn deinit(self: *@This(), source_alloc: std.mem.Allocator) void {
            if (self.last_table) |value| source_alloc.free(value);
            if (self.last_body) |value| source_alloc.free(value);
            self.* = undefined;
        }

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn replaceOwned(
            slot: *?[]u8,
            source_alloc: std.mem.Allocator,
            value: []const u8,
        ) !void {
            if (slot.*) |old| source_alloc.free(old);
            slot.* = try source_alloc.dupe(u8, value);
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = lookup_alloc;
            _ = table_name;
            _ = key;
            _ = opts;
            _ = consistency;
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = scan_alloc;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return null;
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = consistency;
            self.calls += 1;
            try replaceOwned(&self.last_table, query_alloc, table_name);
            try replaceOwned(&self.last_body, query_alloc, req.body_json);
            return .{ .json = try query_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":1,\"hits\":[{\"_id\":\"doc:hit\",\"_source\":{\"status\":\"active\",\"rank\":7}}]}}]}") };
        }
    };

    const Case = struct {
        name: []const u8,
        indexes_json: []const u8,
        sql: []const u8,
        required_body_fragments: []const []const u8,
    };
    const cases = [_]Case{
        .{
            .name = "vector",
            .indexes_json = "{\"docs_embedding_hnsw\":{\"type\":\"aknn\",\"index_generation\":11,\"expected_index_generation\":11}}",
            .sql = "SELECT _id FROM docs WHERE antfly.vector_search(table_name => 'docs', index => 'docs_embedding_hnsw', vector => '[0.1,0.2,0.3]', limit => 5) LIMIT 5",
            .required_body_fragments = &.{ "\"embeddings\"", "\"docs_embedding_hnsw\"", "\"limit\":5" },
        },
        .{
            .name = "semantic",
            .indexes_json = "{\"docs_body_semantic\":{\"type\":\"semantic\",\"generation\":12,\"required_index_generation\":12}}",
            .sql = "SELECT _id FROM docs WHERE antfly.semantic_search(table_name => 'docs', index => 'docs_body_semantic', query => 'automatic embeddings', limit => 7) LIMIT 7",
            .required_body_fragments = &.{ "\"semantic_search\":\"automatic embeddings\"", "\"docs_body_semantic\"", "\"limit\":7" },
        },
        .{
            .name = "hybrid",
            .indexes_json = "{\"docs_body_fts\":{\"type\":\"full_text\"},\"docs_body_semantic\":{\"type\":\"semantic\",\"generation\":12,\"required_index_generation\":12},\"docs_hybrid\":{\"type\":\"hybrid\",\"index_generation\":13,\"expected_index_generation\":13}}",
            .sql = "SELECT _id FROM docs WHERE antfly.hybrid_search(table_name => 'docs', full_text_index => 'docs_body_fts', semantic_index => 'docs_body_semantic', field => 'body', query => 'hybrid refund', fusion => 'rrf', limit => 9) LIMIT 9",
            .required_body_fragments = &.{ "\"semantic_search\":\"hybrid refund\"", "\"full_text_search\"", "\"merge_config\"", "\"limit\":9" },
        },
        .{
            .name = "graph traverse",
            .indexes_json = "{\"docs_edge_graph\":{\"type\":\"graph\",\"index_generation\":14,\"expected_index_generation\":14}}",
            .sql = "SELECT _id FROM docs WHERE antfly.graph_traverse(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', max_depth => 2, limit => 11) LIMIT 11",
            .required_body_fragments = &.{ "\"graph_searches\"", "\"type\":\"traverse\"", "\"index_name\":\"docs_edge_graph\"", "\"limit\":11" },
        },
        .{
            .name = "graph shortest path",
            .indexes_json = "{\"docs_edge_graph\":{\"type\":\"graph\",\"index_generation\":14,\"expected_index_generation\":14}}",
            .sql = "SELECT _id FROM docs WHERE antfly.graph_shortest_path(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', target => 'doc:z', max_depth => 4, limit => 4) LIMIT 4",
            .required_body_fragments = &.{ "\"graph_searches\"", "\"type\":\"shortest_path\"", "\"target_nodes\"", "\"limit\":4" },
        },
        .{
            .name = "graph metric",
            .indexes_json = "{\"docs_pagerank\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\",\"index_generation\":15,\"expected_index_generation\":15}}",
            .sql = "SELECT _id FROM docs WHERE antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 2, limit => 2) LIMIT 2",
            .required_body_fragments = &.{ "\"graph_metric\"", "\"index\":\"docs_edge_graph\"", "\"metric\":\"pagerank\"", "\"limit\":2" },
        },
        .{
            .name = "graph metric rerank",
            .indexes_json = "{\"docs_body_fts\":{\"type\":\"full_text\"},\"docs_pagerank\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\",\"index_generation\":16,\"expected_index_generation\":16}}",
            .sql = "SELECT _id FROM docs WHERE antfly.graph_metric_rerank(table_name => 'docs', full_text_index => 'docs_body_fts', field => 'body', query => 'refund', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', limit => 6) LIMIT 6",
            .required_body_fragments = &.{ "\"full_text_search\"", "\"graph_metric_rerank\"", "\"index\":\"docs_edge_graph\"", "\"limit\":6" },
        },
    };

    for (cases) |case| {
        errdefer std.debug.print("document sql native runtime parity case failed: {s}\n", .{case.name});
        var capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(alloc, schema, case.indexes_json);
        defer source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);
        var parsed = try tokenized.ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        var lowered = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, capabilities);
        defer lowered.deinit(alloc);
        var source = RecordingSource{};
        defer source.deinit(alloc);

        var result = (try executeReadPlanAlloc(alloc, source.source(), lowered, .stale)).?;
        defer result.deinit(alloc);

        try std.testing.expectEqual(@as(usize, 1), source.calls);
        try std.testing.expectEqualStrings("docs", source.last_table.?);
        const body = source.last_body orelse return error.TestUnexpectedResult;
        for (case.required_body_fragments) |fragment| {
            try std.testing.expect(std.mem.indexOf(u8, body, fragment) != null);
        }
        try std.testing.expectEqual(@as(u32, 1), result.total);
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqualStrings("{\"_id\":\"doc:hit\"}", result.rows[0]);
    }
}

test "document sql native filter rewrite only maps field identifiers" {
    const alloc = std.testing.allocator;
    const native_filter = try documentSqlNativeFilterQueryJsonAlloc(
        alloc,
        "{\"bool\":{\"filter\":[{\"term\":{\"path\":\"/metadata/status\",\"value\":\"/active\"}},{\"terms\":{\"/tenant\":[\"/t1\"]}}]}}",
    );
    defer alloc.free(native_filter);

    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"term\":{\"metadata.status\":\"/active\"}},{\"terms\":{\"tenant\":[\"/t1\"]}}]}}",
        native_filter,
    );

    const native_array_filter = try documentSqlNativeFilterQueryJsonAlloc(
        alloc,
        "{\"array_any\":{\"path\":\"/tags\",\"value\":\"urgent\"}}",
    );
    defer alloc.free(native_array_filter);

    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"field\":\"tags\",\"value\":\"urgent\"}}",
        native_array_filter,
    );
}

test "document sql native filter rewrite canonicalizes row filter conjunctions" {
    const alloc = std.testing.allocator;
    const native_filter = try documentSqlNativeFilterQueryJsonAlloc(
        alloc,
        "{\"conjuncts\":[{\"term\":{\"path\":\"/status\",\"value\":\"active\"}},{\"term\":{\"tier\":\"gold\"}}]}",
    );
    defer alloc.free(native_filter);

    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"tier\":\"gold\"}}]}}",
        native_filter,
    );

    const native_disjunction = try documentSqlNativeFilterQueryJsonAlloc(
        alloc,
        "{\"disjuncts\":[{\"term\":{\"path\":\"/tier\",\"value\":\"gold\"}},{\"terms\":{\"status\":[\"trial\",\"active\"]}}]}",
    );
    defer alloc.free(native_disjunction);

    try std.testing.expectEqualStrings(
        "{\"bool\":{\"should\":[{\"term\":{\"tier\":\"gold\"}},{\"terms\":{\"status\":[\"trial\",\"active\"]}}],\"minimum_should_match\":1}}",
        native_disjunction,
    );
}

test "document SQL residual filters match corpus cases" {
    const alloc = std.testing.allocator;
    var corpus = try document_sql_corpus.parseDocumentSqlCorpusAlloc(alloc);
    defer corpus.deinit();
    for (corpus.value.residual_filter_cases) |case| {
        errdefer std.debug.print("document SQL residual corpus case failed: {s}\n", .{case.name});
        if (case.expected.matches) |expected| {
            if (case.expected.@"error" != null) return error.InvalidSqlCorpusFixture;
            try std.testing.expectEqual(expected, try residualFilterMatchesAlloc(alloc, case.document_json, case.filter_json));
        } else if (case.expected.@"error") |expected_error_name| {
            const expected_error = document_sql_corpus.errorValue(try document_sql_corpus.errorFromName(expected_error_name));
            try std.testing.expectError(expected_error, residualFilterMatchesAlloc(alloc, case.document_json, case.filter_json));
        } else {
            return error.InvalidSqlCorpusFixture;
        }
    }
}

test "document SQL residual scalar text cast term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":12}",
        "{\"scalar_text_cast_term\":{\"path\":\"/amount\",\"value\":\"12\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"active\":true}",
        "{\"scalar_text_cast_term\":{\"path\":\"/active\",\"value\":\"true\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"created_at\":123456}",
        "{\"scalar_text_cast_term\":{\"path\":\"/created_at\",\"value\":\"123456\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\"}",
        "{\"scalar_text_cast_term\":{\"path\":\"/status\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":13}",
        "{\"scalar_text_cast_term\":{\"path\":\"/amount\",\"value\":\"12\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":null}",
        "{\"scalar_text_cast_term\":{\"path\":\"/amount\",\"value\":\"12\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\"}",
        "{\"scalar_text_cast_term\":{\"path\":\"/amount\",\"value\":\"12\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"tags\":[\"a\"]}",
            "{\"scalar_text_cast_term\":{\"path\":\"/tags\",\"value\":\"a\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"amount\":12}",
            "{\"scalar_text_cast_term\":{\"path\":\"/amount\",\"value\":12}}",
        ),
    );
}

test "document SQL residual scalar numeric cast range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":12}",
        "{\"scalar_numeric_cast_range\":{\"path\":\"/amount\",\"min\":12,\"max\":12,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"encoded\":\"12.5\"}",
        "{\"scalar_numeric_cast_range\":{\"path\":\"/encoded\",\"min\":12,\"inclusive_min\":false,\"max\":13,\"inclusive_max\":false}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":11}",
        "{\"scalar_numeric_cast_range\":{\"path\":\"/amount\",\"min\":12,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":null}",
        "{\"scalar_numeric_cast_range\":{\"path\":\"/amount\",\"min\":12,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"12\"}",
        "{\"scalar_numeric_cast_range\":{\"path\":\"/amount\",\"min\":12,\"inclusive_min\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"encoded\":\"not-number\"}",
            "{\"scalar_numeric_cast_range\":{\"path\":\"/encoded\",\"min\":12,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"active\":true}",
            "{\"scalar_numeric_cast_range\":{\"path\":\"/active\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"amount\":12}",
            "{\"scalar_numeric_cast_range\":{\"path\":\"/amount\",\"min\":\"12\",\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual scalar boolean cast term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"active\":true}",
        "{\"scalar_boolean_cast_term\":{\"path\":\"/active\",\"value\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"encoded\":\"false\"}",
        "{\"scalar_boolean_cast_term\":{\"path\":\"/encoded\",\"value\":false}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"active\":false}",
        "{\"scalar_boolean_cast_term\":{\"path\":\"/active\",\"value\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"active\":null}",
        "{\"scalar_boolean_cast_term\":{\"path\":\"/active\",\"value\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"true\"}",
        "{\"scalar_boolean_cast_term\":{\"path\":\"/active\",\"value\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"encoded\":\"TRUE\"}",
            "{\"scalar_boolean_cast_term\":{\"path\":\"/encoded\",\"value\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"amount\":1}",
            "{\"scalar_boolean_cast_term\":{\"path\":\"/amount\",\"value\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"active\":true}",
            "{\"scalar_boolean_cast_term\":{\"path\":\"/active\",\"value\":\"true\"}}",
        ),
    );
}

test "document SQL residual scalar datetime cast range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"created_at\":123456}",
        "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":123456,\"max\":123456,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"encoded\":\"987654\"}",
        "{\"scalar_datetime_cast_range\":{\"path\":\"/encoded\",\"min\":987000,\"inclusive_min\":true,\"max\":988000,\"inclusive_max\":false}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"created_at\":123455}",
        "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":123456,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"created_at\":null}",
        "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":123456,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"123456\"}",
        "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":123456,\"inclusive_min\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"encoded\":\"2026-07-03T12:34:56Z\"}",
            "{\"scalar_datetime_cast_range\":{\"path\":\"/encoded\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"created_at\":-1}",
            "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":0,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"active\":true}",
            "{\"scalar_datetime_cast_range\":{\"path\":\"/active\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"created_at\":123456}",
            "{\"scalar_datetime_cast_range\":{\"path\":\"/created_at\",\"min\":\"123456\",\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual nullif term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\"}",
        "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"archived\"}",
        "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"pending\"}",
        "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"release\"}",
        "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":\"active\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\"}",
            "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":0,\"value\":\"active\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\"}",
            "{\"text_nullif_term\":{\"path\":\"/status\",\"nullif\":\"archived\",\"value\":true}}",
        ),
    );
}

test "document SQL residual replace term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ready-ready\"}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":\" \",\"value\":\"ready ready\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ready-ready\"}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"ready\",\"replacement\":\"\",\"value\":\"-\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ready-ready\"}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"\",\"replacement\":\"x\",\"value\":\"ready-ready\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"pending\"}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":\" \",\"value\":\"ready ready\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":\" \",\"value\":\"ready ready\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"ready-ready\"}",
        "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":\" \",\"value\":\"ready ready\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ready-ready\"}",
            "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":1,\"replacement\":\" \",\"value\":\"ready ready\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ready-ready\"}",
            "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":false,\"value\":\"ready ready\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ready-ready\"}",
            "{\"text_replace_term\":{\"path\":\"/status\",\"needle\":\"-\",\"replacement\":\" \",\"value\":42}}",
        ),
    );
}

test "document SQL residual translate term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc-de\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":\"xyz\",\"value\":\"xyz-de\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc-de\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abcd\",\"to\":\"XY\",\"value\":\"XY-e\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc-de\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"\",\"to\":\"x\",\"value\":\"abc-de\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"hé-hé\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"é\",\"to\":\"e\",\"value\":\"he-he\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"pending\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":\"xyz\",\"value\":\"xyz-de\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":\"xyz\",\"value\":\"xyz-de\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"abc-de\"}",
        "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":\"xyz\",\"value\":\"xyz-de\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc-de\"}",
            "{\"text_translate_term\":{\"path\":\"/status\",\"from\":1,\"to\":\"xyz\",\"value\":\"xyz-de\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc-de\"}",
            "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":false,\"value\":\"xyz-de\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc-de\"}",
            "{\"text_translate_term\":{\"path\":\"/status\",\"from\":\"abc\",\"to\":\"xyz\",\"value\":42}}",
        ),
    );
}

test "document SQL residual concat_ws term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\",\"next_status\":\"queued\"}",
        "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":\"active-queued\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\",\"next_status\":null}",
        "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":\"active\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"next_status\":\"queued\"}",
        "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":\"queued\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null,\"next_status\":null}",
        "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":\"\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\",\"next_status\":\"queued\"}",
        "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\":\",\"value\":\"active-queued\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\",\"next_status\":42}",
            "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":\"active-queued\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\",\"next_status\":\"queued\"}",
            "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":42,\"separator\":\"-\",\"value\":\"active-queued\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\",\"next_status\":\"queued\"}",
            "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":false,\"value\":\"active-queued\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"active\",\"next_status\":\"queued\"}",
            "{\"text_concat_ws_term\":{\"path\":\"/status\",\"path2\":\"/next_status\",\"separator\":\"-\",\"value\":42}}",
        ),
    );
}

test "document SQL residual pad term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\"0\",\"value\":\"00abc\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"right\",\"width\":6,\"fill\":\"-+\",\"value\":\"abc-+a\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\" \",\"value\":\"  abc\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"right\",\"width\":3,\"fill\":\"-\",\"value\":\"abc\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"hé\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":4,\"fill\":\"é\",\"value\":\"ééhé\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\"0\",\"value\":\"abc00\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\"0\",\"value\":\"00abc\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"abc\"}",
        "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\"0\",\"value\":\"00abc\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc\"}",
            "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"middle\",\"width\":5,\"fill\":\"0\",\"value\":\"00abc\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc\"}",
            "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":\"5\",\"fill\":\"0\",\"value\":\"00abc\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc\"}",
            "{\"text_pad_term\":{\"path\":\"/status\",\"side\":\"left\",\"width\":5,\"fill\":\"\",\"value\":\"00abc\"}}",
        ),
    );
}

test "document SQL residual split_part term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"first:middle:last\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":2,\"value\":\"middle\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"first:middle:last\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":-1,\"value\":\"last\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"release\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\"\",\"index\":1,\"value\":\"release\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"first:middle:last\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":9,\"value\":\"\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"first:middle:last\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":2,\"value\":\"last\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":2,\"value\":\"middle\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"first:middle:last\"}",
        "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":2,\"value\":\"middle\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"first:middle:last\"}",
            "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":0,\"value\":\"middle\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"first:middle:last\"}",
            "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":\"2\",\"value\":\"middle\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"first:middle:last\"}",
            "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":42,\"index\":2,\"value\":\"middle\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"first:middle:last\"}",
            "{\"text_split_part_term\":{\"path\":\"/status\",\"delimiter\":\":\",\"index\":2,\"value\":42}}",
        ),
    );
}

test "document SQL residual text affix term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"prefix\",\"value\":\"open\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"suffix\",\"value\":\"tail\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"prefix\",\"value\":\"\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"suffix\",\"value\":\"open\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"prefix\",\"value\":\"open\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"open-tail\"}",
        "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"prefix\",\"value\":\"open\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"open-tail\"}",
            "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"middle\",\"value\":\"open\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"open-tail\"}",
            "{\"text_affix_term\":{\"path\":\"/status\",\"side\":1,\"value\":\"open\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"open-tail\"}",
            "{\"text_affix_term\":{\"path\":\"/status\",\"side\":\"prefix\",\"value\":true}}",
        ),
    );
}

test "document SQL residual text position range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"opén-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"é\",\"min\":3,\"max\":3,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"tail\",\"min\":0,\"inclusive_min\":false}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"missing\",\"min\":0,\"max\":0,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"\",\"min\":1,\"max\":1,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"tail\",\"max\":4,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"tail\",\"min\":0,\"inclusive_min\":false}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"open-tail\"}",
        "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"tail\",\"min\":0,\"inclusive_min\":false}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"open-tail\"}",
            "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":1,\"min\":0,\"inclusive_min\":false}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"open-tail\"}",
            "{\"text_strpos_range\":{\"path\":\"/status\",\"needle\":\"tail\",\"min\":\"0\",\"inclusive_min\":false}}",
        ),
    );
}

test "document SQL residual text ascii range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"Active\"}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":65,\"max\":65,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"éclair\"}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":233,\"max\":233,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"\"}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":0,\"max\":0,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"Active\"}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":66,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":65,\"max\":65,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"Active\"}",
        "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":65,\"max\":65,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"Active\"}",
            "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":\"65\",\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":true}",
            "{\"text_ascii_range\":{\"path\":\"/status\",\"min\":65,\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual regexp function filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"a12 b34\"}",
        "{\"regexp_count_range\":{\"path\":\"/status\",\"pattern\":\"[0-9]+\",\"min\":2,\"max\":2,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc DEF\"}",
        "{\"regexp_instr_range\":{\"path\":\"/status\",\"pattern\":\"[A-Z]+\",\"min\":5,\"max\":5,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc DEF\"}",
        "{\"regexp_substr_term\":{\"path\":\"/status\",\"pattern\":\"[A-Z]+\",\"value\":\"DEF\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc DEF\"}",
        "{\"regexp_count_range\":{\"path\":\"/status\",\"pattern\":\"[0-9]+\",\"min\":1,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abc DEF\"}",
        "{\"regexp_substr_term\":{\"path\":\"/status\",\"pattern\":\"[0-9]+\",\"value\":\"123\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"regexp_count_range\":{\"path\":\"/status\",\"pattern\":\"[0-9]+\",\"min\":1,\"inclusive_min\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc DEF\"}",
            "{\"regexp_count_range\":{\"path\":\"/status\",\"pattern\":\"[\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc DEF\"}",
            "{\"regexp_count_range\":{\"path\":\"/status\",\"pattern\":42,\"min\":1,\"inclusive_min\":true}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abc DEF\"}",
            "{\"regexp_substr_term\":{\"path\":\"/status\",\"pattern\":\"[A-Z]+\",\"value\":42}}",
        ),
    );
}

test "document SQL residual chr term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":65}",
        "{\"text_chr_term\":{\"path\":\"/amount\",\"value\":\"A\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":65}",
        "{\"text_chr_term\":{\"codepoint\":233,\"value\":\"\\u00e9\"}}",
    ) == false);
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{}",
        "{\"text_chr_term\":{\"codepoint\":233,\"value\":\"\\u00e9\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":66}",
        "{\"text_chr_term\":{\"path\":\"/amount\",\"value\":\"A\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":null}",
        "{\"text_chr_term\":{\"path\":\"/amount\",\"value\":\"A\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":65}",
        "{\"text_chr_term\":{\"path\":\"/amount\",\"value\":\"A\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"amount\":\"65\"}",
            "{\"text_chr_term\":{\"path\":\"/amount\",\"value\":\"A\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{}",
            "{\"text_chr_term\":{\"codepoint\":65.5,\"value\":\"A\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{}",
            "{\"text_chr_term\":{\"codepoint\":1114112,\"value\":\"A\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"amount\":65}",
            "{\"text_chr_term\":{\"path\":\"/amount\",\"codepoint\":65,\"value\":\"A\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{}",
            "{\"text_chr_term\":{\"codepoint\":65,\"value\":65}}",
        ),
    );
}

test "document SQL residual text unary term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open tail\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"initcap\",\"value\":\"Open Tail\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"hello\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"md5\",\"value\":\"5d41402abc4b2a76b9719d911017c592\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"héllo\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"md5\",\"value\":\"9f6ec78061f7655b2782d3e5b8cd77a2\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"active\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"soundex\",\"value\":\"A231\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"héllo\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"reverse\",\"value\":\"olléh\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"  ready  \"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"btrim\",\"value\":\"ready\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"  ready  \"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"ltrim\",\"value\":\"ready  \"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"  ready  \"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"rtrim\",\"value\":\"  ready\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"open tail\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"initcap\",\"value\":\"Open tail\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"md5\",\"value\":\"5d41402abc4b2a76b9719d911017c592\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"hello\"}",
        "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"md5\",\"value\":\"5d41402abc4b2a76b9719d911017c592\"}}",
    ));
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"caf\xc3\xa9\"}",
            "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"initcap\",\"value\":\"Caf\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"hello\"}",
            "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"reverse\",\"value\":\"olleh\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"hello\"}",
            "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":1,\"value\":\"hello\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"hello\"}",
            "{\"text_unary_term\":{\"path\":\"/status\",\"operator\":\"md5\",\"value\":42}}",
        ),
    );
}

test "document SQL residual repeat term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ab\"}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":\"abab\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ab\"}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":0,\"value\":\"\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"hé\"}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":\"héhé\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"ab\"}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":\"ab\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":\"abab\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"ab\"}",
        "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":\"abab\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ab\"}",
            "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":-1,\"value\":\"\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ab\"}",
            "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":\"2\",\"value\":\"abab\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"ab\"}",
            "{\"text_repeat_term\":{\"path\":\"/status\",\"count\":2,\"value\":42}}",
        ),
    );
}

test "document SQL residual text slice term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":2,\"value\":\"ab\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"right\",\"count\":3,\"value\":\"def\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":-1,\"value\":\"abcde\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"héllo\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"right\",\"count\":4,\"value\":\"éllo\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"right\",\"count\":3,\"value\":\"abc\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":2,\"value\":\"ab\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"abcdef\"}",
        "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":2,\"value\":\"ab\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"middle\",\"count\":2,\"value\":\"ab\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":\"2\",\"value\":\"ab\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_slice_term\":{\"path\":\"/status\",\"side\":\"left\",\"count\":2,\"value\":42}}",
        ),
    );
}

test "document SQL residual substring term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":3,\"value\":\"bcd\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":4,\"value\":\"def\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"héllo\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":2,\"value\":\"él\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":1,\"length\":0,\"value\":\"\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":3,\"value\":\"abc\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":3,\"value\":\"bcd\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"abcdef\"}",
        "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":3,\"value\":\"bcd\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_substring_term\":{\"path\":\"/status\",\"start\":0,\"length\":3,\"value\":\"abc\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":-1,\"value\":\"\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_substring_term\":{\"path\":\"/status\",\"start\":\"2\",\"length\":3,\"value\":\"bcd\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_substring_term\":{\"path\":\"/status\",\"start\":2,\"length\":3,\"value\":42}}",
        ),
    );
}

test "document SQL residual overlay term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":3,\"value\":\"aXXef\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"YY\",\"start\":4,\"value\":\"abcYYf\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"!\",\"start\":3,\"length\":0,\"value\":\"ab!cdef\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"héllo\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"YY\",\"start\":2,\"length\":2,\"value\":\"hYYlo\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"abcdef\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":3,\"value\":\"abcXX\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":null}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":3,\"value\":\"aXXef\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"category\":\"abcdef\"}",
        "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":3,\"value\":\"aXXef\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":0,\"length\":3,\"value\":\"XXdef\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":-1,\"value\":\"\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":42,\"start\":2,\"length\":3,\"value\":\"aXXef\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":\"2\",\"length\":3,\"value\":\"aXXef\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":\"abcdef\"}",
            "{\"text_overlay_term\":{\"path\":\"/status\",\"replacement\":\"XX\",\"start\":2,\"length\":3,\"value\":42}}",
        ),
    );
}

test "document SQL residual array length range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"tags\":[\"a\",\"b\",\"c\"]}",
        "{\"array_length_range\":{\"path\":\"/tags\",\"min\":2,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"tags\":[\"a\"]}",
        "{\"array_length_range\":{\"path\":\"/tags\",\"min\":2,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"tags\":[\"a\",\"b\"]}",
        "{\"array_length_range\":{\"path\":\"/tags\",\"min\":2,\"max\":2,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"tags\":null}",
        "{\"array_length_range\":{\"path\":\"/tags\",\"min\":1,\"inclusive_min\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"tags\":\"a\"}",
            "{\"array_length_range\":{\"path\":\"/tags\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual JSON array length range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":[\"a\",\"b\",\"c\"]}}",
        "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":2,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":[\"a\"]}}",
        "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":2,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":[\"a\",\"b\"]}}",
        "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":2,\"max\":2,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":null}}",
        "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":1,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{}}",
        "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":1,\"inclusive_min\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"metadata\":{\"flags\":\"a\"}}",
            "{\"json_array_length_range\":{\"path\":\"/metadata/flags\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual JSON typeof term filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":[\"a\",\"b\"]}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/flags\",\"value\":\"array\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"billing\":{\"plan\":\"pro\"}}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/billing\",\"value\":\"object\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"source\":\"api\"}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/source\",\"value\":\"string\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"count\":3}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/count\",\"value\":\"number\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"active\":true}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/active\",\"value\":\"boolean\"}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"deleted_at\":null}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/deleted_at\",\"value\":\"null\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{\"flags\":\"not-array\"}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/flags\",\"value\":\"array\"}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"metadata\":{}}",
        "{\"json_typeof_term\":{\"path\":\"/metadata/flags\",\"value\":\"null\"}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"metadata\":{\"flags\":[]}}",
            "{\"json_typeof_term\":{\"path\":\"/metadata/flags\",\"value\":1}}",
        ),
    );
}

test "document SQL residual byte and bit length filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"caf\xc3\xa9\"}",
        "{\"text_octet_length_range\":{\"path\":\"/status\",\"min\":5,\"max\":5,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"caf\xc3\xa9\"}",
        "{\"text_octet_length_range\":{\"path\":\"/status\",\"min\":4,\"max\":4,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"caf\xc3\xa9\"}",
        "{\"text_bit_length_range\":{\"path\":\"/status\",\"min\":40,\"inclusive_min\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"status\":\"caf\xc3\xa9\"}",
        "{\"text_bit_length_range\":{\"path\":\"/status\",\"max\":39,\"inclusive_max\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"status\":42}",
            "{\"text_octet_length_range\":{\"path\":\"/status\",\"min\":1,\"inclusive_min\":true}}",
        ),
    );
}

test "document SQL residual numeric unary range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":8.6}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"round\",\"min\":9,\"max\":9,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":8.4}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"round\",\"min\":9,\"max\":9,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":-3.25}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"floor\",\"max\":-4,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":-3.25}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"sign\",\"min\":-1,\"max\":-1,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":16}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"sqrt\",\"max\":4,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"amount\":16}",
        "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"sqrt\",\"max\":4,\"inclusive_max\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"delta\":-1}",
            "{\"numeric_unary_range\":{\"path\":\"/delta\",\"operator\":\"sqrt\",\"max\":4,\"inclusive_max\":true}}",
        ),
    );
}

test "document SQL residual numeric power range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":3}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"power\",\"operand\":2,\"min\":9,\"max\":9,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":2}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"power\",\"operand\":2,\"min\":9,\"max\":9,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":4}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"power\",\"operand\":0.5,\"max\":2,\"inclusive_max\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"delta\":-3}",
            "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"power\",\"operand\":0.5,\"max\":2,\"inclusive_max\":true}}",
        ),
    );
}

test "document SQL residual numeric modulo range filters match rows" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":10}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"mod\",\"operand\":3,\"min\":1,\"max\":1,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":-10}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"mod\",\"operand\":3,\"min\":-1,\"max\":-1,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expect(!try residualFilterMatchesAlloc(
        alloc,
        "{\"delta\":11}",
        "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"mod\",\"operand\":3,\"min\":1,\"max\":1,\"inclusive_min\":true,\"inclusive_max\":true}}",
    ));
    try std.testing.expectError(
        error.InvalidRowsRequest,
        residualFilterMatchesAlloc(
            alloc,
            "{\"delta\":10}",
            "{\"numeric_arithmetic_range\":{\"path\":\"/delta\",\"operator\":\"mod\",\"operand\":0,\"min\":0,\"max\":0,\"inclusive_min\":true,\"inclusive_max\":true}}",
        ),
    );
}

test "document SQL producer mutation materializes bounded residual transform batch" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        last_scan_limit: u32 = 0,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            const json = if (std.mem.eql(u8, key, "doc:a"))
                "{\"status\":\"active\"}"
            else if (std.mem.eql(u8, key, "doc:b"))
                "{\"status\":\"inactive\"}"
            else if (std.mem.eql(u8, key, "doc:c"))
                "{\"status\":\"active\"}"
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = consistency;
            self.last_scan_limit = opts.limit;
            return .{ .ndjson = try scan_alloc.dupe(u8, "{\"key\":\"doc:a\"}\n{\"key\":\"doc:b\"}\n{\"key\":\"doc:c\"}\n") };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    var operations = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "/title",
        .value_json = "\"Ready\"",
    }};
    const residual_filter_json = "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}";

    const byte_capped_lowered = sql_plan.LoweredDocumentProducerMutation{
        .table_name = "docs",
        .producer = .{ .bounded_scan = .{
            .max_rows = 3,
            .max_bytes = 8,
            .residual_filter_json = residual_filter_json,
        } },
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
    };
    var byte_capped_source = MockSource{};
    try std.testing.expectError(
        error.DocumentSqlBoundedScanByteCapExceeded,
        materializeProducerMutationBatchAlloc(alloc, byte_capped_source.source(), byte_capped_lowered, .stale),
    );

    const lowered = sql_plan.LoweredDocumentProducerMutation{
        .table_name = "docs",
        .producer = .{ .bounded_scan = .{
            .max_rows = 3,
            .residual_filter_json = residual_filter_json,
        } },
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
        .expected_version = 7,
        .sync_level = .enrichments,
    };
    var source = MockSource{};
    var batch = try materializeProducerMutationBatchAlloc(alloc, source.source(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 4), source.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 2), batch.req.transforms.len);
    try std.testing.expectEqual(@as(usize, 2), batch.req.predicates.len);
    try std.testing.expectEqual(@as(u32, 2), batch.transformed);
    try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, batch.req.sync_level);
    try std.testing.expectEqualStrings("doc:a", batch.req.transforms[0].key);
    try std.testing.expectEqualStrings("doc:c", batch.req.transforms[1].key);
    for (batch.req.transforms) |transform| {
        try std.testing.expectEqual(@as(usize, 1), transform.operations.len);
        try std.testing.expectEqual(db_mod.types.TransformOpType.set, transform.operations[0].op);
        try std.testing.expectEqualStrings("/title", transform.operations[0].path);
        try std.testing.expectEqualStrings("\"Ready\"", transform.operations[0].value_json.?);
    }
    try std.testing.expectEqualStrings("doc:a", batch.req.predicates[0].key);
    try std.testing.expectEqual(@as(u64, 7), batch.req.predicates[0].expected_version);
    try std.testing.expectEqualStrings("doc:c", batch.req.predicates[1].key);
    try std.testing.expectEqual(@as(u64, 7), batch.req.predicates[1].expected_version);
}

test "document SQL producer mutation applies materialized native batch through sink" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            const json = if (std.mem.eql(u8, key, "doc:a"))
                "{\"status\":\"active\"}"
            else if (std.mem.eql(u8, key, "doc:b"))
                "{\"status\":\"inactive\"}"
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8, "") };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    const MockSink = struct {
        calls: usize = 0,
        transform_count: usize = 0,
        predicate_count: usize = 0,
        first_key: []const u8 = "",
        sync_level: db_mod.types.SyncLevel = .write,

        fn sink(self: *@This()) BatchSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                },
            };
        }

        fn batch(ptr: *anyopaque, req: db_mod.types.BatchRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.transform_count = req.transforms.len;
            self.predicate_count = req.predicates.len;
            self.sync_level = req.sync_level;
            if (req.transforms.len > 0) self.first_key = req.transforms[0].key;
        }
    };

    var operations = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "/title",
        .value_json = "\"Ready\"",
    }};
    const ids = [_][]const u8{ "doc:a", "doc:b" };
    const lowered = sql_plan.LoweredDocumentProducerMutation{
        .table_name = "docs",
        .producer = .{ .id_lookup = .{
            .ids = ids[0..],
            .residual_filter_json = "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        } },
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
        .expected_version = 9,
        .sync_level = .enrichments,
    };
    var source = MockSource{};
    var sink = MockSink{};
    var batch = try executeProducerMutationPlanAlloc(alloc, source.source(), sink.sink(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), sink.calls);
    try std.testing.expectEqual(@as(usize, 1), sink.transform_count);
    try std.testing.expectEqual(@as(usize, 1), sink.predicate_count);
    try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, sink.sync_level);
    try std.testing.expectEqualStrings("doc:a", sink.first_key);
    try std.testing.expectEqual(@as(usize, 1), batch.req.transforms.len);
    try std.testing.expectEqualStrings("doc:a", batch.req.transforms[0].key);
}

test "document SQL joined mutation materializes exact id source assignment update" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = opts;
            _ = consistency;
            if (!std.mem.eql(u8, table_name, "docs")) return null;
            if (!std.mem.eql(u8, key, "doc:a")) return null;
            return .{ .json = try lookup_alloc.dupe(u8, "{\"title\":\"Source Title\",\"status\":\"draft\"}"), .version = 3 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8, "") };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    var operations = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "status",
        .value_json = "\"ready\"",
    }};
    var join_keys = [_]sql_plan.DocumentJoinedMutationJoinKey{.{
        .target_field = "_id",
        .source_field = "_id",
    }};
    var assignments = [_]sql_plan.DocumentJoinedMutationSourceAssignment{.{
        .target_path = "title",
        .source_field = "title",
        .field_type = .text,
    }};
    const ids = [_][]const u8{"doc:a"};
    const lowered = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "docs",
        .target_producer = .{ .id_lookup = .{ .ids = ids[0..] } },
        .source_producer = .{ .static = .{ .id_lookup = .{ .ids = ids[0..] } } },
        .join_keys = join_keys[0..],
        .source_assignments = assignments[0..],
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
        .expected_version = 11,
        .max_target_rows = 1,
        .max_source_rows = 1,
        .duplicate_source_policy = .reject,
        .sync_level = .enrichments,
    };

    var source = MockSource{};
    var batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), batch.req.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), batch.req.predicates.len);
    try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, batch.req.sync_level);
    try std.testing.expectEqualStrings("doc:a", batch.req.transforms[0].key);
    try std.testing.expectEqual(@as(usize, 2), batch.req.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", batch.req.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"ready\"", batch.req.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("title", batch.req.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("\"Source Title\"", batch.req.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqualStrings("doc:a", batch.req.predicates[0].key);
    try std.testing.expectEqual(@as(u64, 11), batch.req.predicates[0].expected_version);
}

test "document SQL joined mutation handles exact id no-match duplicate source delete and sink" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = opts;
            _ = consistency;
            if (std.mem.eql(u8, table_name, "docs") and std.mem.eql(u8, key, "doc:a")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"title\":\"Target\"}"), .version = 5 };
            }
            if (std.mem.eql(u8, table_name, "source_docs") and std.mem.eql(u8, key, "doc:z")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"title\":\"Source\"}"), .version = 7 };
            }
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8, "") };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    const MockSink = struct {
        calls: usize = 0,
        delete_count: usize = 0,
        predicate_count: usize = 0,

        fn sink(self: *@This()) BatchSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                },
            };
        }

        fn batch(ptr: *anyopaque, req: db_mod.types.BatchRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.delete_count = req.deletes.len;
            self.predicate_count = req.predicates.len;
        }
    };

    var join_keys = [_]sql_plan.DocumentJoinedMutationJoinKey{.{
        .target_field = "_id",
        .source_field = "_id",
    }};
    const doc_a = [_][]const u8{"doc:a"};
    const doc_z = [_][]const u8{"doc:z"};
    const duplicate = [_][]const u8{ "doc:a", "doc:a" };

    const delete_lowered = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "docs",
        .target_producer = .{ .id_lookup = .{ .ids = doc_a[0..] } },
        .source_producer = .{ .static = .{ .id_lookup = .{ .ids = doc_a[0..] } } },
        .join_keys = join_keys[0..],
        .operation = .non_identity_delete,
        .template = .delete,
        .expected_version = 5,
        .max_target_rows = 1,
        .max_source_rows = 1,
    };

    var source = MockSource{};
    var sink = MockSink{};
    var delete_batch = try executeJoinedMutationPlanAlloc(alloc, source.source(), sink.sink(), delete_lowered, .stale);
    defer delete_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), sink.calls);
    try std.testing.expectEqual(@as(usize, 1), sink.delete_count);
    try std.testing.expectEqual(@as(usize, 1), sink.predicate_count);
    try std.testing.expectEqual(@as(u32, 1), delete_batch.deleted);
    try std.testing.expectEqualStrings("doc:a", delete_batch.req.deletes[0]);
    try std.testing.expectEqual(@as(u64, 5), delete_batch.req.predicates[0].expected_version);

    const source_missing = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "source_docs",
        .target_producer = .{ .id_lookup = .{ .ids = doc_a[0..] } },
        .source_producer = .{ .static = .{ .id_lookup = .{ .ids = doc_a[0..] } } },
        .join_keys = join_keys[0..],
        .operation = .non_identity_delete,
        .template = .delete,
        .expected_version = 5,
    };
    var source_missing_batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), source_missing, .stale);
    defer source_missing_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), source_missing_batch.req.deletes.len);
    try std.testing.expectEqual(@as(usize, 0), source_missing_batch.req.predicates.len);

    const target_missing = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "source_docs",
        .target_producer = .{ .id_lookup = .{ .ids = doc_z[0..] } },
        .source_producer = .{ .static = .{ .id_lookup = .{ .ids = doc_z[0..] } } },
        .join_keys = join_keys[0..],
        .operation = .non_identity_delete,
        .template = .delete,
        .expected_version = 5,
    };
    var target_missing_batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), target_missing, .stale);
    defer target_missing_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), target_missing_batch.req.deletes.len);
    try std.testing.expectEqual(@as(usize, 0), target_missing_batch.req.predicates.len);

    const duplicate_source = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "docs",
        .target_producer = .{ .id_lookup = .{ .ids = doc_a[0..] } },
        .source_producer = .{ .static = .{ .id_lookup = .{ .ids = duplicate[0..] } } },
        .join_keys = join_keys[0..],
        .operation = .non_identity_delete,
        .template = .delete,
        .max_source_rows = 2,
    };
    try std.testing.expectError(error.DocumentSqlWriteDuplicateSource, materializeJoinedMutationBatchAlloc(alloc, source.source(), duplicate_source, .stale));
}

test "document SQL merge materializes not matched source document insert" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = opts;
            _ = consistency;
            if (std.mem.eql(u8, table_name, "source_docs") and std.mem.eql(u8, key, "doc:new")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"title\":\"New\",\"status\":\"ready\"}"), .version = 3 };
            }
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8, "") };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    const ids = [_][]const u8{"doc:new"};
    var join_keys = [_]sql_plan.DocumentJoinedMutationJoinKey{.{
        .target_field = "_id",
        .source_field = "_id",
    }};
    var not_matched = [_]sql_plan.DocumentMergeNotMatchedArm{.{
        .insert_source_document = true,
    }};
    const lowered = sql_plan.LoweredDocumentMergeMutation{
        .table_name = "docs",
        .source_table_name = "source_docs",
        .target_producer = .{ .id_lookup = .{ .ids = ids[0..] } },
        .source_producer = .{ .id_lookup = .{ .ids = ids[0..] } },
        .join_keys = join_keys[0..],
        .not_matched_arms = not_matched[0..],
        .max_target_rows = 1,
        .max_source_rows = 1,
        .sync_level = .enrichments,
    };

    var source = MockSource{};
    var batch = try materializeDocumentMergeMutationBatchAlloc(alloc, source.source(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), batch.req.writes.len);
    try std.testing.expectEqual(db_mod.types.BatchWriteMode.create_only, batch.req.write_mode);
    try std.testing.expectEqual(db_mod.types.SyncLevel.enrichments, batch.req.sync_level);
    try std.testing.expectEqualStrings("doc:new", batch.req.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"New\",\"status\":\"ready\"}", batch.req.writes[0].value);
}

test "document SQL joined mutation materializes mapped-field indexed source and bounded target" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        duplicate_source: bool = false,
        target_no_match: bool = false,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = opts;
            _ = consistency;
            if (std.mem.eql(u8, table_name, "docs") and std.mem.eql(u8, key, "doc:target-a")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"account_id\":\"acct:1\",\"title\":\"Target A\"}"), .version = 5 };
            }
            if (std.mem.eql(u8, table_name, "docs") and std.mem.eql(u8, key, "doc:target-b")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"account_id\":\"acct:2\",\"title\":\"Target B\"}"), .version = 6 };
            }
            if (std.mem.eql(u8, table_name, "source_docs") and std.mem.eql(u8, key, "doc:source-a")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"account_id\":\"acct:1\",\"title\":\"Source A\"}"), .version = 7 };
            }
            if (std.mem.eql(u8, table_name, "source_docs") and std.mem.eql(u8, key, "doc:source-duplicate")) {
                return .{ .json = try lookup_alloc.dupe(u8, "{\"account_id\":\"acct:1\",\"title\":\"Source Duplicate\"}"), .version = 8 };
            }
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8,
                \\{"key":"doc:target-a"}
                \\{"key":"doc:target-b"}
                \\
            ) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = req;
            _ = consistency;
            if (std.mem.eql(u8, table_name, "docs")) {
                const target_response_json =
                    if (self.target_no_match)
                        \\{"responses":[{"hits":{"total":0,"hits":[]}}]}
                    else
                        \\{"responses":[{"hits":{"total":1,"hits":[{"_id":"doc:target-a"}]}}]}
                    ;
                return .{ .json = try query_alloc.dupe(u8, target_response_json) };
            }
            const response_json =
                if (self.duplicate_source)
                    \\{"responses":[{"hits":{"total":2,"hits":[{"_id":"doc:source-a"},{"_id":"doc:source-duplicate"}]}}]}
                else
                    \\{"responses":[{"hits":{"total":1,"hits":[{"_id":"doc:source-a"}]}}]}
                ;
            return .{ .json = try query_alloc.dupe(u8, response_json) };
        }
    };

    var operations = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "status",
        .value_json = "\"copied\"",
    }};
    var join_keys = [_]sql_plan.DocumentJoinedMutationJoinKey{.{
        .target_field = "account_id",
        .source_field = "account_id",
    }};
    var assignments = [_]sql_plan.DocumentJoinedMutationSourceAssignment{.{
        .target_path = "title",
        .source_field = "title",
        .field_type = .text,
    }};
    const lowered = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "source_docs",
        .target_producer = .{ .bounded_scan = .{ .max_rows = 5 } },
        .source_producer = .join_key_indexed_lookup,
        .join_keys = join_keys[0..],
        .source_assignments = assignments[0..],
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
        .max_target_rows = 5,
        .max_source_rows = 5,
    };

    var source = MockSource{};
    var batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), batch.req.transforms.len);
    try std.testing.expectEqualStrings("doc:target-a", batch.req.transforms[0].key);
    try std.testing.expectEqual(@as(usize, 2), batch.req.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", batch.req.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"copied\"", batch.req.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("title", batch.req.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("\"Source A\"", batch.req.transforms[0].operations[1].value_json.?);

    source.duplicate_source = true;
    try std.testing.expectError(error.DocumentSqlWriteDuplicateSource, materializeJoinedMutationBatchAlloc(alloc, source.source(), lowered, .stale));

    source.duplicate_source = false;
    const indexed_target_lowered = sql_plan.LoweredDocumentJoinedMutation{
        .table_name = "docs",
        .source_table_name = "source_docs",
        .target_producer = .{ .indexed_query = .{
            .filter_query_json = "{\"term\":{\"path\":\"/account_id\",\"value\":\"acct:1\"}}",
            .max_candidate_rows = 1,
        } },
        .source_producer = .join_key_indexed_lookup,
        .join_keys = join_keys[0..],
        .source_assignments = assignments[0..],
        .operation = .projection_write,
        .template = .{ .transform = operations[0..] },
        .max_target_rows = 1,
        .max_source_rows = 1,
    };
    var indexed_batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), indexed_target_lowered, .stale);
    defer indexed_batch.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), indexed_batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), indexed_batch.req.transforms.len);
    try std.testing.expectEqualStrings("doc:target-a", indexed_batch.req.transforms[0].key);

    source.target_no_match = true;
    var no_match_batch = try materializeJoinedMutationBatchAlloc(alloc, source.source(), indexed_target_lowered, .stale);
    defer no_match_batch.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), no_match_batch.transformed);
    try std.testing.expectEqual(@as(usize, 0), no_match_batch.req.transforms.len);
}

test "document SQL producer mutation materializes explicit delete batch" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = lookup_alloc;
            _ = table_name;
            _ = key;
            _ = opts;
            _ = consistency;
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            _ = ptr;
            _ = scan_alloc;
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return null;
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    const ids = [_][]const u8{ "doc:a", "doc:b" };
    const lowered = sql_plan.LoweredDocumentProducerMutation{
        .table_name = "docs",
        .producer = .{ .id_lookup = .{ .ids = ids[0..] } },
        .operation = .exact_id_delete,
        .template = .delete,
    };
    var source = MockSource{};
    var batch = try materializeProducerMutationBatchAlloc(alloc, source.source(), lowered, .stale);
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), batch.req.deletes.len);
    try std.testing.expectEqual(@as(u32, 2), batch.deleted);
    try std.testing.expectEqualStrings("doc:a", batch.req.deletes[0]);
    try std.testing.expectEqualStrings("doc:b", batch.req.deletes[1]);
}

test "document SQL bounded residual scan fails closed only when the scan cap is filled" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        scanned_rows: u32,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            if (!std.mem.eql(u8, key, "doc:a")) return null;
            return .{ .json = try lookup_alloc.dupe(u8, "{\"status\":\"active\"}"), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            const ndjson = if (self.scanned_rows == 0)
                ""
            else
                "{\"key\":\"doc:a\"}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    var projection = [_]sql_adapter.DocumentProjection{.{ .kind = .id, .output = "_id" }};
    const residual_filter_json = "{\"term\":{\"path\":\"/status\",\"value\":\"missing\"}}";

    const capped_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 1,
            .residual_filter_json = residual_filter_json,
        } },
        .limit = 1,
    };
    var capped_source = MockSource{ .scanned_rows = 1 };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeReadPlanAlloc(alloc, capped_source.source(), capped_plan, .stale),
    );

    const partial_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 2,
            .residual_filter_json = residual_filter_json,
        } },
        .limit = 1,
    };
    var partial_source = MockSource{ .scanned_rows = 1 };
    var partial = (try executeReadPlanAlloc(alloc, partial_source.source(), partial_plan, .stale)).?;
    defer partial.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), partial.total);
    try std.testing.expectEqual(@as(usize, 0), partial.rows.len);
}

test "document SQL materializes scalar text cast projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":12.5,\"active\":true,\"created_at\":\"2026-07-03T12:34:56Z\",\"status\":\"ready\",\"encoded\":\"7\",\"missing\":null,\"tags\":[\"hot\"]}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_text_cast, .field = "amount", .output = "amount_text" },
        .{ .kind = .scalar_text_cast, .field = "active", .output = "active_text" },
        .{ .kind = .scalar_text_cast, .field = "created_at", .output = "created_text" },
        .{ .kind = .scalar_text_cast, .field = "status", .output = "status_text" },
        .{ .kind = .scalar_text_cast, .field = "encoded", .output = "encoded_text" },
        .{ .kind = .scalar_text_cast, .field = "missing", .output = "missing_text" },
        .{ .kind = .scalar_text_cast, .field = "absent", .output = "absent_text" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"amount_text\":\"12.5\",\"active_text\":\"true\",\"created_text\":\"2026-07-03T12:34:56Z\",\"status_text\":\"ready\",\"encoded_text\":\"7\",\"missing_text\":null,\"absent_text\":null}", row);

    var wrong_type = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_text_cast, .field = "tags", .output = "tags_text" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, wrong_type[0..]));
}

test "document SQL materializes scalar numeric cast projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":12.5,\"encoded\":\"7\",\"description\":\"8.25\",\"missing\":null,\"active\":true}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_numeric_cast, .field = "amount", .output = "amount_num" },
        .{ .kind = .scalar_numeric_cast, .field = "encoded", .output = "encoded_num" },
        .{ .kind = .scalar_numeric_cast, .field = "description", .output = "description_num" },
        .{ .kind = .scalar_numeric_cast, .field = "missing", .output = "missing_num" },
        .{ .kind = .scalar_numeric_cast, .field = "absent", .output = "absent_num" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"amount_num\":12.5,\"encoded_num\":7,\"description_num\":8.25,\"missing_num\":null,\"absent_num\":null}", row);

    var invalid_text = try std.json.parseFromSlice(std.json.Value, alloc, "{\"encoded\":\"ready\"}", .{ .allocate = .alloc_always });
    defer invalid_text.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", invalid_text.value, null, projection[1..2]));

    var wrong_type = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_numeric_cast, .field = "active", .output = "active_num" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, wrong_type[0..]));
}

test "document SQL materializes scalar boolean cast projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"active\":true,\"disabled\":false,\"encoded_true\":\"true\",\"encoded_false\":\"false\",\"missing\":null,\"amount\":1}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_boolean_cast, .field = "active", .output = "active_bool" },
        .{ .kind = .scalar_boolean_cast, .field = "disabled", .output = "disabled_bool" },
        .{ .kind = .scalar_boolean_cast, .field = "encoded_true", .output = "encoded_true_bool" },
        .{ .kind = .scalar_boolean_cast, .field = "encoded_false", .output = "encoded_false_bool" },
        .{ .kind = .scalar_boolean_cast, .field = "missing", .output = "missing_bool" },
        .{ .kind = .scalar_boolean_cast, .field = "absent", .output = "absent_bool" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"active_bool\":true,\"disabled_bool\":false,\"encoded_true_bool\":true,\"encoded_false_bool\":false,\"missing_bool\":null,\"absent_bool\":null}", row);

    var invalid_text = try std.json.parseFromSlice(std.json.Value, alloc, "{\"encoded\":\"TRUE\"}", .{ .allocate = .alloc_always });
    defer invalid_text.deinit();
    var invalid_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_boolean_cast, .field = "encoded", .output = "encoded_bool" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", invalid_text.value, null, invalid_projection[0..]));

    var wrong_type = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_boolean_cast, .field = "amount", .output = "amount_bool" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, wrong_type[0..]));
}

test "document SQL materializes scalar datetime cast projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"created_at\":123456789,\"encoded\":\"987654321\",\"missing\":null,\"active\":true,\"negative\":-1,\"float_value\":1.5}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_datetime_cast, .field = "created_at", .output = "created_ts" },
        .{ .kind = .scalar_datetime_cast, .field = "encoded", .output = "encoded_ts" },
        .{ .kind = .scalar_datetime_cast, .field = "missing", .output = "missing_ts" },
        .{ .kind = .scalar_datetime_cast, .field = "absent", .output = "absent_ts" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"created_ts\":123456789,\"encoded_ts\":987654321,\"missing_ts\":null,\"absent_ts\":null}", row);

    var iso_text = try std.json.parseFromSlice(std.json.Value, alloc, "{\"encoded\":\"2026-07-03T12:34:56Z\"}", .{ .allocate = .alloc_always });
    defer iso_text.deinit();
    var text_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_datetime_cast, .field = "encoded", .output = "encoded_ts" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", iso_text.value, null, text_projection[0..]));

    var negative_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_datetime_cast, .field = "negative", .output = "negative_ts" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, negative_projection[0..]));

    var float_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_datetime_cast, .field = "float_value", .output = "float_ts" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, float_projection[0..]));

    var wrong_type = [_]sql_adapter.DocumentProjection{
        .{ .kind = .scalar_datetime_cast, .field = "active", .output = "active_ts" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, wrong_type[0..]));
}

test "document SQL materializes UTC date helper projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"epoch\":0,\"end_of_day\":86399999999999,\"next_day\":86400000000000,\"leap_day\":1709164800000000000,\"encoded\":\"1709251200000000000\",\"missing\":null,\"bad\":\"2026-01-01T00:00:00Z\"}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .temporal_date_utc, .field = "epoch", .output = "epoch_day" },
        .{ .kind = .temporal_date_utc, .field = "end_of_day", .output = "end_day" },
        .{ .kind = .temporal_date_utc, .field = "next_day", .output = "next_day" },
        .{ .kind = .temporal_date_utc, .field = "leap_day", .output = "leap_day" },
        .{ .kind = .temporal_date_utc, .field = "encoded", .output = "encoded_day" },
        .{ .kind = .temporal_date_utc, .field = "missing", .output = "missing_day" },
        .{ .kind = .temporal_date_utc, .field = "absent", .output = "absent_day" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"epoch_day\":\"1970-01-01\",\"end_day\":\"1970-01-01\",\"next_day\":\"1970-01-02\",\"leap_day\":\"2024-02-29\",\"encoded_day\":\"2024-03-01\",\"missing_day\":null,\"absent_day\":null}", row);

    var bad_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .temporal_date_utc, .field = "bad", .output = "bad_day" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, bad_projection[0..]));
}

test "document SQL materializes case-fold projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"Active\"}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_lower, .field = "status", .output = "status_lower" },
        .{ .kind = .text_upper, .field = "status", .output = "status_upper" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_lower\":\"active\",\"status_upper\":\"ACTIVE\"}", row);

    var non_ascii = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"CAF\xc3\x89\"}", .{ .allocate = .alloc_always });
    defer non_ascii.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_ascii.value, null, projection[0..]));
}

test "document SQL materializes initcap projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"ready FOR-review_2026\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_initcap, .field = "status", .output = "status_title" },
        .{ .kind = .text_initcap, .field = "missing", .output = "missing_title" },
        .{ .kind = .text_initcap, .field = "absent", .output = "absent_title" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_title\":\"Ready For-Review_2026\",\"missing_title\":null,\"absent_title\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));

    var non_ascii = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9llo\"}", .{ .allocate = .alloc_always });
    defer non_ascii.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_ascii.value, null, projection[0..1]));
}

test "document SQL materializes md5 projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"hello\",\"accent\":\"h\xc3\xa9llo\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_md5, .field = "status", .output = "status_md5" },
        .{ .kind = .text_md5, .field = "accent", .output = "accent_md5" },
        .{ .kind = .text_md5, .field = "missing", .output = "missing_md5" },
        .{ .kind = .text_md5, .field = "absent", .output = "absent_md5" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_md5\":\"5d41402abc4b2a76b9719d911017c592\",\"accent_md5\":\"9f6ec78061f7655b2782d3e5b8cd77a2\",\"missing_md5\":null,\"absent_md5\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes soundex projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"active\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_soundex, .field = "status", .output = "status_soundex" },
        .{ .kind = .text_soundex, .field = "missing", .output = "missing_soundex" },
        .{ .kind = .text_soundex, .field = "absent", .output = "absent_soundex" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_soundex\":\"A231\",\"missing_soundex\":null,\"absent_soundex\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes text length projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"caf\xc3\xa9\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_length, .field = "status", .output = "status_len" },
        .{ .kind = .text_octet_length, .field = "status", .output = "status_bytes" },
        .{ .kind = .text_bit_length, .field = "status", .output = "status_bits" },
        .{ .kind = .text_length, .field = "missing", .output = "missing_len" },
        .{ .kind = .text_octet_length, .field = "missing", .output = "missing_bytes" },
        .{ .kind = .text_length, .field = "absent", .output = "absent_len" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_len\":4,\"status_bytes\":5,\"status_bits\":40,\"missing_len\":null,\"missing_bytes\":null,\"absent_len\":null}", row);
}

test "document SQL materializes JSON typeof projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"metadata\":{\"flags\":[\"hot\"],\"source\":\"api\",\"count\":2,\"active\":true,\"missing\":null}}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .json_typeof, .field = "/metadata", .output = "metadata_type" },
        .{ .kind = .json_typeof, .field = "/metadata/flags", .output = "flags_type" },
        .{ .kind = .json_typeof, .field = "/metadata/source", .output = "source_type" },
        .{ .kind = .json_typeof, .field = "/metadata/count", .output = "count_type" },
        .{ .kind = .json_typeof, .field = "/metadata/active", .output = "active_type" },
        .{ .kind = .json_typeof, .field = "/metadata/missing", .output = "missing_type" },
        .{ .kind = .json_typeof, .field = "/metadata/absent", .output = "absent_type" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"metadata_type\":\"object\",\"flags_type\":\"array\",\"source_type\":\"string\",\"count_type\":\"number\",\"active_type\":\"boolean\",\"missing_type\":\"null\",\"absent_type\":null}", row);
}

test "document SQL materializes JSON array length projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"metadata\":{\"flags\":[\"hot\",\"new\"],\"events\":[],\"missing\":null,\"source\":\"api\"}}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .json_array_length, .field = "/metadata/flags", .output = "flag_count" },
        .{ .kind = .json_array_length, .field = "/metadata/events", .output = "event_count" },
        .{ .kind = .json_array_length, .field = "/metadata/missing", .output = "missing_count" },
        .{ .kind = .json_array_length, .field = "/metadata/absent", .output = "absent_count" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"flag_count\":2,\"event_count\":0,\"missing_count\":null,\"absent_count\":null}", row);

    var scalar_projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .json_array_length, .field = "/metadata/source", .output = "source_count" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, scalar_projection[0..]));
}

test "document SQL materializes numeric abs projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"delta\":-12.5,\"encoded\":\"-3.25\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_abs, .field = "delta", .output = "delta_abs" },
        .{ .kind = .numeric_abs, .field = "encoded", .output = "encoded_abs" },
        .{ .kind = .numeric_abs, .field = "missing", .output = "missing_abs" },
        .{ .kind = .numeric_abs, .field = "absent", .output = "absent_abs" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"delta_abs\":12.5,\"encoded_abs\":3.25,\"missing_abs\":null,\"absent_abs\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"delta\":\"ready\"}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes numeric unary projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":12.6,\"delta\":-3.25,\"zero\":0,\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_round, .field = "amount", .output = "amount_round" },
        .{ .kind = .numeric_trunc, .field = "delta", .output = "delta_trunc" },
        .{ .kind = .numeric_floor, .field = "delta", .output = "delta_floor" },
        .{ .kind = .numeric_ceil, .field = "delta", .output = "delta_ceil" },
        .{ .kind = .numeric_sqrt, .field = "amount", .output = "amount_sqrt" },
        .{ .kind = .numeric_sign, .field = "delta", .output = "delta_sign" },
        .{ .kind = .numeric_sign, .field = "zero", .output = "zero_sign" },
        .{ .kind = .numeric_round, .field = "missing", .output = "missing_round" },
        .{ .kind = .numeric_round, .field = "absent", .output = "absent_round" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"amount_round\":13,\"delta_trunc\":-3,\"delta_floor\":-4,\"delta_ceil\":-3,\"amount_sqrt\":3.5496478698597698,\"delta_sign\":-1,\"zero_sign\":0,\"missing_round\":null,\"absent_round\":null}", row);

    var bad_sqrt = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_sqrt, .field = "delta", .output = "bad_sqrt" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, bad_sqrt[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":\"ready\"}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes numeric arithmetic projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":10,\"delta\":-10,\"encoded\":\"8\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "plus_two", .numeric_operator = .add, .numeric_operand = "2" },
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "minus_three", .numeric_operator = .sub, .numeric_operand = "3" },
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "times_four", .numeric_operator = .mul, .numeric_operand = "4" },
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "divided", .numeric_operator = .div, .numeric_operand = "5" },
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "remainder", .numeric_operator = .mod, .numeric_operand = "3" },
        .{ .kind = .numeric_arithmetic, .field = "delta", .output = "negative_remainder", .numeric_operator = .mod, .numeric_operand = "3" },
        .{ .kind = .numeric_arithmetic, .field = "encoded", .output = "encoded_plus", .numeric_operator = .add, .numeric_operand = "0.5" },
        .{ .kind = .numeric_arithmetic, .field = "missing", .output = "missing_plus", .numeric_operator = .add, .numeric_operand = "2" },
        .{ .kind = .numeric_arithmetic, .field = "absent", .output = "absent_plus", .numeric_operator = .add, .numeric_operand = "2" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"plus_two\":12,\"minus_three\":7,\"times_four\":40,\"divided\":2,\"remainder\":1,\"negative_remainder\":-1,\"encoded_plus\":8.5,\"missing_plus\":null,\"absent_plus\":null}", row);

    var zero_divisor = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "bad_divide", .numeric_operator = .div, .numeric_operand = "0" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, zero_divisor[0..]));

    var zero_modulus = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "bad_mod", .numeric_operator = .mod, .numeric_operand = "0" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, zero_modulus[0..]));

    var malformed_operand = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "bad_operand", .numeric_operator = .add, .numeric_operand = "nan" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_operand[0..]));
}

test "document SQL materializes numeric power projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":4,\"delta\":-3,\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "amount", .output = "amount_squared", .numeric_operator = .power, .numeric_operand = "2" },
        .{ .kind = .numeric_arithmetic, .field = "delta", .output = "delta_squared", .numeric_operator = .power, .numeric_operand = "2" },
        .{ .kind = .numeric_arithmetic, .field = "missing", .output = "missing_power", .numeric_operator = .power, .numeric_operand = "2" },
        .{ .kind = .numeric_arithmetic, .field = "absent", .output = "absent_power", .numeric_operator = .power, .numeric_operand = "2" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"amount_squared\":16,\"delta_squared\":9,\"missing_power\":null,\"absent_power\":null}", row);

    var invalid_result = [_]sql_adapter.DocumentProjection{
        .{ .kind = .numeric_arithmetic, .field = "delta", .output = "bad_power", .numeric_operator = .power, .numeric_operand = "0.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, invalid_result[0..]));
}

test "document SQL materializes regexp projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"A1B22 caf\xc3\xa97\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .regexp_count, .field = "status", .output = "digit_groups", .pattern = "[0-9]+" },
        .{ .kind = .regexp_instr, .field = "status", .output = "first_digit_pos", .pattern = "[0-9]" },
        .{ .kind = .regexp_substr, .field = "status", .output = "first_lower", .pattern = "[a-z]+" },
        .{ .kind = .regexp_substr, .field = "status", .output = "no_match", .pattern = "z+" },
        .{ .kind = .regexp_count, .field = "missing", .output = "missing_count", .pattern = "[0-9]+" },
        .{ .kind = .regexp_instr, .field = "absent", .output = "absent_pos", .pattern = "[0-9]" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"digit_groups\":3,\"first_digit_pos\":2,\"first_lower\":\"caf\",\"no_match\":null,\"missing_count\":null,\"absent_pos\":null}", row);

    var bad_pattern = [_]sql_adapter.DocumentProjection{
        .{ .kind = .regexp_count, .field = "status", .output = "bad_pattern", .pattern = "[" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, bad_pattern[0..]));

    var empty_pattern = [_]sql_adapter.DocumentProjection{
        .{ .kind = .regexp_count, .field = "status", .output = "empty_pattern", .pattern = "" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, empty_pattern[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes text binary projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"op\xc3\xa9ned\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_starts_with, .field = "status", .output = "has_prefix", .pattern = "op" },
        .{ .kind = .text_ends_with, .field = "status", .output = "has_suffix", .pattern = "ed" },
        .{ .kind = .text_starts_with, .field = "status", .output = "bad_prefix", .pattern = "closed" },
        .{ .kind = .text_strpos, .field = "status", .output = "accent_pos", .pattern = "é" },
        .{ .kind = .text_strpos, .field = "status", .output = "missing_pos", .pattern = "z" },
        .{ .kind = .text_strpos, .field = "status", .output = "empty_pos", .pattern = "" },
        .{ .kind = .text_strpos, .field = "status", .output = "position_equivalent", .pattern = "ned" },
        .{ .kind = .text_starts_with, .field = "missing", .output = "missing_prefix", .pattern = "op" },
        .{ .kind = .text_ends_with, .field = "absent", .output = "absent_suffix", .pattern = "ed" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"has_prefix\":true,\"has_suffix\":true,\"bad_prefix\":false,\"accent_pos\":3,\"missing_pos\":0,\"empty_pos\":1,\"position_equivalent\":5,\"missing_prefix\":null,\"absent_suffix\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes split part projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"tenant:plan:region\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_split_part, .field = "status", .output = "first_part", .pattern = ":", .numeric_operand = "1" },
        .{ .kind = .text_split_part, .field = "status", .output = "middle_part", .pattern = ":", .numeric_operand = "2" },
        .{ .kind = .text_split_part, .field = "status", .output = "tail_part", .pattern = ":", .numeric_operand = "-1" },
        .{ .kind = .text_split_part, .field = "status", .output = "out_of_range", .pattern = ":", .numeric_operand = "9" },
        .{ .kind = .text_split_part, .field = "status", .output = "whole_value", .pattern = "", .numeric_operand = "1" },
        .{ .kind = .text_split_part, .field = "status", .output = "empty_delimiter_out_of_range", .pattern = "", .numeric_operand = "2" },
        .{ .kind = .text_split_part, .field = "missing", .output = "missing_part", .pattern = ":", .numeric_operand = "1" },
        .{ .kind = .text_split_part, .field = "absent", .output = "absent_part", .pattern = ":", .numeric_operand = "1" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"first_part\":\"tenant\",\"middle_part\":\"plan\",\"tail_part\":\"region\",\"out_of_range\":\"\",\"whole_value\":\"tenant:plan:region\",\"empty_delimiter_out_of_range\":\"\",\"missing_part\":null,\"absent_part\":null}", row);

    var zero_index = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_split_part, .field = "status", .output = "bad_part", .pattern = ":", .numeric_operand = "0" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, zero_index[0..]));

    var malformed_index = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_split_part, .field = "status", .output = "bad_part", .pattern = ":", .numeric_operand = "1.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_index[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes overlay projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9llo-world\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_overlay, .field = "status", .output = "middle", .pattern = "XX", .numeric_operand = "2", .numeric_operand2 = "4" },
        .{ .kind = .text_overlay, .field = "status", .output = "default_len", .pattern = "YY", .numeric_operand = "7" },
        .{ .kind = .text_overlay, .field = "status", .output = "inserted", .pattern = "!", .numeric_operand = "3", .numeric_operand2 = "0" },
        .{ .kind = .text_overlay, .field = "status", .output = "past_end", .pattern = "?", .numeric_operand = "99", .numeric_operand2 = "2" },
        .{ .kind = .text_overlay, .field = "missing", .output = "missing_overlay", .pattern = "X", .numeric_operand = "1", .numeric_operand2 = "1" },
        .{ .kind = .text_overlay, .field = "absent", .output = "absent_overlay", .pattern = "X", .numeric_operand = "1", .numeric_operand2 = "1" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"middle\":\"hXX-world\",\"default_len\":\"héllo-YYrld\",\"inserted\":\"hé!llo-world\",\"past_end\":\"héllo-world?\",\"missing_overlay\":null,\"absent_overlay\":null}", row);

    var zero_start = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_overlay, .field = "status", .output = "bad_overlay", .pattern = "X", .numeric_operand = "0", .numeric_operand2 = "1" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, zero_start[0..]));

    var malformed_length = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_overlay, .field = "status", .output = "bad_overlay", .pattern = "X", .numeric_operand = "1", .numeric_operand2 = "1.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_length[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes substring projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9llo-world\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_substring, .field = "status", .output = "middle", .numeric_operand = "2", .numeric_operand2 = "4" },
        .{ .kind = .text_substring, .field = "status", .output = "tail", .numeric_operand = "7" },
        .{ .kind = .text_substring, .field = "status", .output = "empty", .numeric_operand = "1", .numeric_operand2 = "0" },
        .{ .kind = .text_substring, .field = "status", .output = "past_end", .numeric_operand = "99", .numeric_operand2 = "2" },
        .{ .kind = .text_substring, .field = "missing", .output = "missing_part", .numeric_operand = "1", .numeric_operand2 = "2" },
        .{ .kind = .text_substring, .field = "absent", .output = "absent_part", .numeric_operand = "1", .numeric_operand2 = "2" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"middle\":\"éllo\",\"tail\":\"world\",\"empty\":\"\",\"past_end\":\"\",\"missing_part\":null,\"absent_part\":null}", row);

    var zero_start = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_substring, .field = "status", .output = "bad_part", .numeric_operand = "0", .numeric_operand2 = "2" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, zero_start[0..]));

    var malformed_length = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_substring, .field = "status", .output = "bad_part", .numeric_operand = "1", .numeric_operand2 = "1.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_length[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes left right projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9llo\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_left, .field = "status", .output = "prefix", .numeric_operand = "2" },
        .{ .kind = .text_right, .field = "status", .output = "suffix", .numeric_operand = "3" },
        .{ .kind = .text_left, .field = "status", .output = "without_tail", .numeric_operand = "-1" },
        .{ .kind = .text_right, .field = "status", .output = "without_head", .numeric_operand = "-2" },
        .{ .kind = .text_left, .field = "status", .output = "all_left", .numeric_operand = "99" },
        .{ .kind = .text_right, .field = "status", .output = "empty_right", .numeric_operand = "0" },
        .{ .kind = .text_left, .field = "missing", .output = "missing_part", .numeric_operand = "2" },
        .{ .kind = .text_right, .field = "absent", .output = "absent_part", .numeric_operand = "2" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"prefix\":\"hé\",\"suffix\":\"llo\",\"without_tail\":\"héll\",\"without_head\":\"llo\",\"all_left\":\"héllo\",\"empty_right\":\"\",\"missing_part\":null,\"absent_part\":null}", row);

    var malformed_count = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_left, .field = "status", .output = "bad_part", .numeric_operand = "1.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_count[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes repeat reverse projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9!\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_reverse, .field = "status", .output = "reversed" },
        .{ .kind = .text_repeat, .field = "status", .output = "doubled", .numeric_operand = "2" },
        .{ .kind = .text_repeat, .field = "status", .output = "empty_repeat", .numeric_operand = "0" },
        .{ .kind = .text_reverse, .field = "missing", .output = "missing_reverse" },
        .{ .kind = .text_repeat, .field = "absent", .output = "absent_repeat", .numeric_operand = "2" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"reversed\":\"!éh\",\"doubled\":\"hé!hé!\",\"empty_repeat\":\"\",\"missing_reverse\":null,\"absent_repeat\":null}", row);

    var malformed_count = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_repeat, .field = "status", .output = "bad_repeat", .numeric_operand = "1.5" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_count[0..]));

    var negative_count = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_repeat, .field = "status", .output = "bad_repeat", .numeric_operand = "-1" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, negative_count[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes trim projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"  h\xc3\xa9!  \",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_btrim, .field = "status", .output = "both" },
        .{ .kind = .text_ltrim, .field = "status", .output = "left" },
        .{ .kind = .text_rtrim, .field = "status", .output = "right" },
        .{ .kind = .text_btrim, .field = "missing", .output = "missing_trim" },
        .{ .kind = .text_ltrim, .field = "absent", .output = "absent_trim" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"both\":\"hé!\",\"left\":\"hé!  \",\"right\":\"  hé!\",\"missing_trim\":null,\"absent_trim\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes ascii projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"Active\",\"accent\":\"\xc3\xa9lan\",\"empty\":\"\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_ascii, .field = "status", .output = "status_code" },
        .{ .kind = .text_ascii, .field = "accent", .output = "accent_code" },
        .{ .kind = .text_ascii, .field = "empty", .output = "empty_code" },
        .{ .kind = .text_ascii, .field = "missing", .output = "missing_code" },
        .{ .kind = .text_ascii, .field = "absent", .output = "absent_code" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_code\":65,\"accent_code\":233,\"empty_code\":0,\"missing_code\":null,\"absent_code\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes chr projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":65,\"accent\":233,\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_chr, .field = "amount", .output = "amount_char" },
        .{ .kind = .text_chr, .field = "accent", .output = "accent_char" },
        .{ .kind = .text_chr, .output = "literal_char", .numeric_operand = "90" },
        .{ .kind = .text_chr, .field = "missing", .output = "missing_char" },
        .{ .kind = .text_chr, .field = "absent", .output = "absent_char" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"amount_char\":\"A\",\"accent_char\":\"é\",\"literal_char\":\"Z\",\"missing_char\":null,\"absent_char\":null}", row);

    var decimal_value = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":65.5}", .{ .allocate = .alloc_always });
    defer decimal_value.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", decimal_value.value, null, projection[0..1]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"amount\":\"65\"}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));

    var invalid_codepoint = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_chr, .output = "bad_char", .numeric_operand = "1114112" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, invalid_codepoint[0..]));
}

test "document SQL materializes replace projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"ready-ready\",\"accent\":\"h\xc3\xa9-h\xc3\xa9\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_replace, .field = "status", .output = "words", .pattern = "-", .numeric_operand = " " },
        .{ .kind = .text_replace, .field = "status", .output = "deleted", .pattern = "ready", .numeric_operand = "" },
        .{ .kind = .text_replace, .field = "status", .output = "unchanged", .pattern = "", .numeric_operand = "x" },
        .{ .kind = .text_replace, .field = "accent", .output = "accent_words", .pattern = "é", .numeric_operand = "e" },
        .{ .kind = .text_replace, .field = "missing", .output = "missing_replace", .pattern = "-", .numeric_operand = " " },
        .{ .kind = .text_replace, .field = "absent", .output = "absent_replace", .pattern = "-", .numeric_operand = " " },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"words\":\"ready ready\",\"deleted\":\"-\",\"unchanged\":\"ready-ready\",\"accent_words\":\"he-he\",\"missing_replace\":null,\"absent_replace\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes nullif projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"active\",\"empty\":\"\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_nullif, .field = "status", .output = "status_value", .pattern = "archived" },
        .{ .kind = .text_nullif, .field = "status", .output = "status_null", .pattern = "active" },
        .{ .kind = .text_nullif, .field = "empty", .output = "empty_null", .pattern = "" },
        .{ .kind = .text_nullif, .field = "missing", .output = "missing_null", .pattern = "" },
        .{ .kind = .text_nullif, .field = "absent", .output = "absent_null", .pattern = "" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_value\":\"active\",\"status_null\":null,\"empty_null\":null,\"missing_null\":null,\"absent_null\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes concat_ws projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"active\",\"next\":{\"status\":\"queued\"},\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_concat_ws, .field = "status", .field2 = "next.status", .output = "path", .pattern = "-" },
        .{ .kind = .text_concat_ws, .field = "status", .field2 = "missing", .output = "left_only", .pattern = "-" },
        .{ .kind = .text_concat_ws, .field = "missing", .field2 = "next.status", .output = "right_only", .pattern = "-" },
        .{ .kind = .text_concat_ws, .field = "missing", .field2 = "absent", .output = "empty", .pattern = "-" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"path\":\"active-queued\",\"left_only\":\"active\",\"right_only\":\"queued\",\"empty\":\"\"}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"active\",\"next\":{\"status\":42}}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes array_to_string projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"urgent\",null,\"vip\"],\"empty\":[],\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_array_to_string, .field = "tags", .output = "tag_list", .pattern = "," },
        .{ .kind = .text_array_to_string, .field = "empty", .output = "empty_list", .pattern = "," },
        .{ .kind = .text_array_to_string, .field = "missing", .output = "missing_list", .pattern = "," },
        .{ .kind = .text_array_to_string, .field = "absent", .output = "absent_list", .pattern = "," },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"tag_list\":\"urgent,vip\",\"empty_list\":\"\",\"missing_list\":null,\"absent_list\":null}", row);

    var non_array = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":\"urgent\"}", .{ .allocate = .alloc_always });
    defer non_array.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_array.value, null, projection[0..1]));

    var non_string_item = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"urgent\",42]}", .{ .allocate = .alloc_always });
    defer non_string_item.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_string_item.value, null, projection[0..1]));
}

test "document SQL materializes string_to_array projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"active,,queued\",\"body\":\"hello\",\"empty\":\"\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_string_to_array, .field = "status", .output = "status_parts", .pattern = "," },
        .{ .kind = .text_string_to_array, .field = "body", .output = "body_parts", .pattern = "" },
        .{ .kind = .text_string_to_array, .field = "empty", .output = "empty_parts", .pattern = "," },
        .{ .kind = .text_string_to_array, .field = "missing", .output = "missing_parts", .pattern = "," },
        .{ .kind = .text_string_to_array, .field = "absent", .output = "absent_parts", .pattern = "," },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"status_parts\":[\"active\",\"\",\"queued\"],\"body_parts\":[\"hello\"],\"empty_parts\":[\"\"],\"missing_parts\":null,\"absent_parts\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes cardinality projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"urgent\",null,\"vip\"],\"empty\":[],\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .array_cardinality, .field = "tags", .output = "tag_count" },
        .{ .kind = .array_cardinality, .field = "empty", .output = "empty_count" },
        .{ .kind = .array_cardinality, .field = "missing", .output = "missing_count" },
        .{ .kind = .array_cardinality, .field = "absent", .output = "absent_count" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"tag_count\":3,\"empty_count\":0,\"missing_count\":null,\"absent_count\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":\"urgent\"}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes array position projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"urgent\",\"vip\",\"urgent\",null],\"empty\":[],\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .array_position, .field = "tags", .output = "first_urgent", .pattern = "urgent" },
        .{ .kind = .array_position, .field = "tags", .output = "first_missing", .pattern = "missing" },
        .{ .kind = .array_positions, .field = "tags", .output = "urgent_positions", .pattern = "urgent" },
        .{ .kind = .array_positions, .field = "empty", .output = "empty_positions", .pattern = "urgent" },
        .{ .kind = .array_positions, .field = "missing", .output = "missing_positions", .pattern = "urgent" },
        .{ .kind = .array_position, .field = "absent", .output = "absent_position", .pattern = "urgent" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"first_urgent\":1,\"first_missing\":null,\"urgent_positions\":[1,3],\"empty_positions\":[],\"missing_positions\":null,\"absent_position\":null}", row);

    var non_array = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":\"urgent\"}", .{ .allocate = .alloc_always });
    defer non_array.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_array.value, null, projection[0..1]));

    var non_string_item = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"urgent\",42]}", .{ .allocate = .alloc_always });
    defer non_string_item.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_string_item.value, null, projection[2..3]));
}

test "document SQL materializes array transform projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"old\",null,\"keep\",\"old\"],\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .array_append, .field = "tags", .output = "appended", .pattern = "new" },
        .{ .kind = .array_prepend, .field = "tags", .output = "prepended", .pattern = "first" },
        .{ .kind = .array_cat, .field = "tags", .output = "catted", .pattern = "[\"cat\",\"dog\"]" },
        .{ .kind = .array_remove, .field = "tags", .output = "removed", .pattern = "old" },
        .{ .kind = .array_replace, .field = "tags", .output = "replaced", .pattern = "old", .numeric_operand = "new" },
        .{ .kind = .array_prepend, .field = "missing", .output = "missing_prepend", .pattern = "new" },
        .{ .kind = .array_append, .field = "missing", .output = "missing_append", .pattern = "new" },
        .{ .kind = .array_remove, .field = "absent", .output = "absent_remove", .pattern = "old" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"appended\":[\"old\",null,\"keep\",\"old\",\"new\"],\"prepended\":[\"first\",\"old\",null,\"keep\",\"old\"],\"catted\":[\"old\",null,\"keep\",\"old\",\"cat\",\"dog\"],\"removed\":[null,\"keep\"],\"replaced\":[\"new\",null,\"keep\",\"new\"],\"missing_prepend\":null,\"missing_append\":null,\"absent_remove\":null}", row);

    var non_array = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":\"old\"}", .{ .allocate = .alloc_always });
    defer non_array.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_array.value, null, projection[0..1]));

    var non_string_item = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tags\":[\"old\",42]}", .{ .allocate = .alloc_always });
    defer non_string_item.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", non_string_item.value, null, projection[3..4]));

    var malformed_literal = [_]sql_adapter.DocumentProjection{
        .{ .kind = .array_cat, .field = "tags", .output = "bad_cat", .pattern = "{\"bad\":true}" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_literal[0..]));
}

test "document SQL materializes translate projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"abc-de\",\"accent\":\"h\xc3\xa9-h\xc3\xa9\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_translate, .field = "status", .output = "mapped", .pattern = "abc", .numeric_operand = "xyz" },
        .{ .kind = .text_translate, .field = "status", .output = "deleted", .pattern = "abcd", .numeric_operand = "XY" },
        .{ .kind = .text_translate, .field = "status", .output = "unchanged", .pattern = "", .numeric_operand = "x" },
        .{ .kind = .text_translate, .field = "accent", .output = "accent_mapped", .pattern = "é", .numeric_operand = "e" },
        .{ .kind = .text_translate, .field = "missing", .output = "missing_translate", .pattern = "a", .numeric_operand = "b" },
        .{ .kind = .text_translate, .field = "absent", .output = "absent_translate", .pattern = "a", .numeric_operand = "b" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"mapped\":\"xyz-de\",\"deleted\":\"XY-e\",\"unchanged\":\"abc-de\",\"accent_mapped\":\"he-he\",\"missing_translate\":null,\"absent_translate\":null}", row);

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL materializes pad projections" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":\"h\xc3\xa9\",\"missing\":null}", .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_lpad, .field = "status", .output = "left_padded", .numeric_operand = "5", .pattern = "0" },
        .{ .kind = .text_rpad, .field = "status", .output = "right_padded", .numeric_operand = "6", .pattern = "-+" },
        .{ .kind = .text_lpad, .field = "status", .output = "default_left", .numeric_operand = "4", .pattern = " " },
        .{ .kind = .text_rpad, .field = "status", .output = "truncated", .numeric_operand = "1", .pattern = "-" },
        .{ .kind = .text_lpad, .field = "status", .output = "empty", .numeric_operand = "0", .pattern = "0" },
        .{ .kind = .text_lpad, .field = "missing", .output = "missing_pad", .numeric_operand = "5", .pattern = "0" },
        .{ .kind = .text_rpad, .field = "absent", .output = "absent_pad", .numeric_operand = "5", .pattern = "0" },
    };
    const row = try documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, projection[0..]);
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"left_padded\":\"000hé\",\"right_padded\":\"hé-+-+\",\"default_left\":\"  hé\",\"truncated\":\"h\",\"empty\":\"\",\"missing_pad\":null,\"absent_pad\":null}", row);

    var malformed_width = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_lpad, .field = "status", .output = "bad_pad", .numeric_operand = "1.5", .pattern = "0" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, malformed_width[0..]));

    var empty_fill = [_]sql_adapter.DocumentProjection{
        .{ .kind = .text_lpad, .field = "status", .output = "bad_pad", .numeric_operand = "5", .pattern = "" },
    };
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", parsed.value, null, empty_fill[0..]));

    var wrong_type = try std.json.parseFromSlice(std.json.Value, alloc, "{\"status\":42}", .{ .allocate = .alloc_always });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidRowsRequest, documentSqlProjectedParsedRowJsonAlloc(alloc, "doc:a", wrong_type.value, null, projection[0..1]));
}

test "document SQL ordered bounded residual scan fails closed only when top-k completeness is unknown" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        scanned_rows: u32,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            if (!std.mem.eql(u8, key, "doc:a")) return null;
            return .{ .json = try lookup_alloc.dupe(u8, "{\"status\":\"active\",\"rank\":2}"), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            const ndjson = if (self.scanned_rows == 0)
                ""
            else
                "{\"key\":\"doc:a\"}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .id, .output = "_id" },
        .{
            .kind = .field,
            .field = "/rank",
            .output = "rank",
        },
    };
    const residual_filter_json = "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}";
    const order_by = sql_adapter.DocumentOrderBy{
        .field = "/rank",
        .field_type = .numeric,
        .direction = .asc,
    };

    const capped_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 1,
            .residual_filter_json = residual_filter_json,
        } },
        .order_by = order_by,
        .limit = 1,
    };
    var capped_source = MockSource{ .scanned_rows = 1 };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeReadPlanAlloc(alloc, capped_source.source(), capped_plan, .stale),
    );

    const partial_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 2,
            .residual_filter_json = residual_filter_json,
        } },
        .order_by = order_by,
        .limit = 1,
    };
    var partial_source = MockSource{ .scanned_rows = 1 };
    var partial = (try executeReadPlanAlloc(alloc, partial_source.source(), partial_plan, .stale)).?;
    defer partial.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), partial.total);
    try std.testing.expectEqual(@as(usize, 1), partial.rows.len);
    try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"rank\":2}", partial.rows[0]);
}

test "document SQL bounded unnest scan fails closed when expansion cannot prove completeness" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        scanned_rows: u32,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            if (!std.mem.eql(u8, key, "doc:a")) return null;
            return .{ .json = try lookup_alloc.dupe(u8, "{\"tags\":[\"urgent\",\"vip\"]}"), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            const ndjson = if (self.scanned_rows == 0)
                ""
            else
                "{\"key\":\"doc:a\"}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    var projection = [_]sql_adapter.DocumentProjection{
        .{ .kind = .id, .output = "_id" },
        .{ .kind = .unnest_value, .output = "tag" },
    };
    const missing_tag_unnest = sql_adapter.DocumentUnnest{
        .field = "/tags",
        .alias = "tag",
        .item_type = .keyword,
        .filter_value_json = "\"missing\"",
    };
    const urgent_tag_unnest = sql_adapter.DocumentUnnest{
        .field = "/tags",
        .alias = "tag",
        .item_type = .keyword,
        .filter_value_json = "\"urgent\"",
    };

    const capped_no_match_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .unnest = missing_tag_unnest,
        .limit = 1,
    };
    var capped_source = MockSource{ .scanned_rows = 1 };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeReadPlanAlloc(alloc, capped_source.source(), capped_no_match_plan, .stale),
    );

    const partial_no_match_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .unnest = missing_tag_unnest,
        .limit = 1,
    };
    var partial_source = MockSource{ .scanned_rows = 1 };
    var partial = (try executeReadPlanAlloc(alloc, partial_source.source(), partial_no_match_plan, .stale)).?;
    defer partial.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), partial.total);
    try std.testing.expectEqual(@as(usize, 0), partial.rows.len);

    const match_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .unnest = urgent_tag_unnest,
        .limit = 1,
    };
    var match_source = MockSource{ .scanned_rows = 1 };
    var matched = (try executeReadPlanAlloc(alloc, match_source.source(), match_plan, .stale)).?;
    defer matched.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), matched.total);
    try std.testing.expectEqual(@as(usize, 1), matched.rows.len);
    try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"tag\":\"urgent\"}", matched.rows[0]);
}

test "document SQL bounded aggregate scan admits only lookup-backed document keys" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        overflow: bool = false,

        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            _ = ptr;
            _ = table_name;
            _ = opts;
            _ = consistency;
            const json = if (std.mem.eql(u8, key, "doc:a"))
                "{\"status\":\"active\",\"amount\":10}"
            else if (std.mem.eql(u8, key, "doc:b"))
                "{\"status\":\"archived\",\"amount\":20}"
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            const ndjson = if (self.overflow)
                "{\"key\":\"doc:a\"}\n{\"key\":\"doc:b\"}\n"
            else
                "{\"key\":\"__internal:index\"}\n{\"key\":\"doc:a\"}\n{\"key\":\"doc:b\"}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return null;
        }
    };

    const plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };

    var source = MockSource{};
    var result = (try executeAggregatePlanAlloc(alloc, source.source(), plan, .stale)).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), result.total_groups);
    try std.testing.expectEqualStrings("{\"total_amount\":30}", result.rows[0]);

    const min_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .min,
            .output = "min_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };

    var min_result = (try executeAggregatePlanAlloc(alloc, source.source(), min_plan, .stale)).?;
    defer min_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), min_result.total_groups);
    try std.testing.expectEqualStrings("{\"min_amount\":10}", min_result.rows[0]);

    const avg_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .avg,
            .output = "avg_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };

    var avg_result = (try executeAggregatePlanAlloc(alloc, source.source(), avg_plan, .stale)).?;
    defer avg_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), avg_result.total_groups);
    try std.testing.expectEqualStrings("{\"avg_amount\":15}", avg_result.rows[0]);

    const max_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .max,
            .output = "max_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };

    var max_result = (try executeAggregatePlanAlloc(alloc, source.source(), max_plan, .stale)).?;
    defer max_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), max_result.total_groups);
    try std.testing.expectEqualStrings("{\"max_amount\":20}", max_result.rows[0]);

    const grouped_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .count,
            .output = "row_count",
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };

    var grouped = (try executeAggregatePlanAlloc(alloc, source.source(), grouped_plan, .stale)).?;
    defer grouped.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), grouped.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"active\",\"row_count\":1}", grouped.rows[0]);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"row_count\":1}", grouped.rows[1]);

    const grouped_sum_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };

    var grouped_sum = (try executeAggregatePlanAlloc(alloc, source.source(), grouped_sum_plan, .stale)).?;
    defer grouped_sum.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), grouped_sum.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"active\",\"total_amount\":10}", grouped_sum.rows[0]);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"total_amount\":20}", grouped_sum.rows[1]);

    const grouped_max_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .max,
            .output = "max_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };

    var grouped_max = (try executeAggregatePlanAlloc(alloc, source.source(), grouped_max_plan, .stale)).?;
    defer grouped_max.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), grouped_max.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"active\",\"max_amount\":10}", grouped_max.rows[0]);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"max_amount\":20}", grouped_max.rows[1]);

    const grouped_avg_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 2 } },
        .aggregate = .{
            .op = .avg,
            .output = "avg_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };

    var grouped_avg = (try executeAggregatePlanAlloc(alloc, source.source(), grouped_avg_plan, .stale)).?;
    defer grouped_avg.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), grouped_avg.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"active\",\"avg_amount\":10}", grouped_avg.rows[0]);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"avg_amount\":20}", grouped_avg.rows[1]);

    var overflow_source = MockSource{ .overflow = true };
    const overflow_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeAggregatePlanAlloc(alloc, overflow_source.source(), overflow_plan, .stale),
    );

    const overflow_grouped_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .aggregate = .{
            .op = .count,
            .output = "row_count",
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeAggregatePlanAlloc(alloc, overflow_source.source(), overflow_grouped_plan, .stale),
    );

    const overflow_grouped_sum_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
    };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeAggregatePlanAlloc(alloc, overflow_source.source(), overflow_grouped_sum_plan, .stale),
    );

    const overflow_max_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .candidate_producer = .{ .bounded_scan = .{ .max_rows = 1 } },
        .aggregate = .{
            .op = .max,
            .output = "max_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
    };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        executeAggregatePlanAlloc(alloc, overflow_source.source(), overflow_max_plan, .stale),
    );
}

test "document SQL executes algebraic materialized aggregate rows" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        fn source(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .algebraic_aggregate = algebraicAggregate,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: QueryRequest,
            _: raft_mod.ReadConsistency,
        ) !?QueryResponse {
            return null;
        }

        fn algebraicAggregate(
            _: *anyopaque,
            aggregate_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: AlgebraicAggregateRequest,
            _: raft_mod.ReadConsistency,
        ) !?AlgebraicAggregateResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("alg", req.index_name);
            try std.testing.expectEqualStrings("sum_by_status", req.materialization_name);
            try std.testing.expectEqual(sql_adapter.DocumentAggregateOp.sum, req.aggregate_op);
            try std.testing.expect(req.group_by != null);
            const rows = try aggregate_alloc.alloc(AlgebraicAggregateRow, 2);
            rows[0] = .{
                .group_json = try aggregate_alloc.dupe(u8, "\"active\""),
                .value_json = try aggregate_alloc.dupe(u8, "22"),
            };
            rows[1] = .{
                .group_json = try aggregate_alloc.dupe(u8, "\"archived\""),
                .value_json = try aggregate_alloc.dupe(u8, "30"),
            };
            return .{
                .rows = rows,
                .total_groups = 2,
            };
        }
    };

    const plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .index_name = "alg",
        .materialization_name = "sum_by_status",
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
        .limit = 1,
    };

    var source = MockSource{};
    var result = (try executeAggregatePlanAlloc(alloc, source.source(), plan, .stale)).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), result.total_groups);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"status\":\"active\",\"total_amount\":22}", result.rows[0]);

    const ordered_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .index_name = "alg",
        .materialization_name = "sum_by_status",
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
        .order_by = .{
            .key = .aggregate,
            .field_type = .numeric,
            .direction = .desc,
        },
        .limit = 1,
    };

    var ordered_result = (try executeAggregatePlanAlloc(alloc, source.source(), ordered_plan, .stale)).?;
    defer ordered_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), ordered_result.total_groups);
    try std.testing.expectEqual(@as(usize, 1), ordered_result.rows.len);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"total_amount\":30}", ordered_result.rows[0]);

    var having_predicates = [_]sql_adapter.DocumentAggregateHavingPredicate{.{
        .key = .aggregate,
        .field_type = .numeric,
        .op = .gt,
        .value_json = "25",
    }};
    const having_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .index_name = "alg",
        .materialization_name = "sum_by_status",
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
        .having = having_predicates[0..],
        .order_by = .{
            .key = .aggregate,
            .field_type = .numeric,
            .direction = .desc,
        },
        .limit = 1,
    };

    var having_result = (try executeAggregatePlanAlloc(alloc, source.source(), having_plan, .stale)).?;
    defer having_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), having_result.total_groups);
    try std.testing.expectEqual(@as(usize, 1), having_result.rows.len);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"total_amount\":30}", having_result.rows[0]);

    var having_conjunction_predicates = [_]sql_adapter.DocumentAggregateHavingPredicate{
        .{
            .key = .aggregate,
            .field_type = .numeric,
            .op = .gt,
            .value_json = "20",
        },
        .{
            .key = .group,
            .field_type = .keyword,
            .op = .eq,
            .value_json = "\"archived\"",
        },
    };
    const having_conjunction_plan = sql_adapter.DocumentAlgebraicAggregatePlan{
        .table_name = "docs",
        .index_name = "alg",
        .materialization_name = "sum_by_status",
        .aggregate = .{
            .op = .sum,
            .output = "total_amount",
            .input = .{ .field = "/amount", .source_field = "amount", .field_type = .numeric },
        },
        .group_by = .{
            .field = "/status",
            .source_field = "status",
            .field_type = .keyword,
            .output = "status",
        },
        .having = having_conjunction_predicates[0..],
        .order_by = .{
            .key = .aggregate,
            .field_type = .numeric,
            .direction = .desc,
        },
        .limit = 1,
    };

    var having_conjunction_result = (try executeAggregatePlanAlloc(alloc, source.source(), having_conjunction_plan, .stale)).?;
    defer having_conjunction_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), having_conjunction_result.total_groups);
    try std.testing.expectEqual(@as(usize, 1), having_conjunction_result.rows.len);
    try std.testing.expectEqualStrings("{\"status\":\"archived\",\"total_amount\":30}", having_conjunction_result.rows[0]);
}

fn documentSqlWildcardMatches(pattern: []const u8, text: []const u8) bool {
    return documentSqlWildcardMatchesAt(pattern, 0, text, 0);
}

fn documentSqlWildcardMatchesIgnoreCase(pattern: []const u8, text: []const u8) bool {
    if (!documentSqlAsciiOnly(pattern) or !documentSqlAsciiOnly(text)) return false;
    return documentSqlWildcardMatchesIgnoreCaseAt(pattern, 0, text, 0);
}

fn documentSqlWildcardMatchesAt(pattern: []const u8, pattern_index: usize, text: []const u8, text_index: usize) bool {
    if (pattern_index == pattern.len) return text_index == text.len;
    if (pattern[pattern_index] == '*') {
        var next = text_index;
        while (next <= text.len) : (next += 1) {
            if (documentSqlWildcardMatchesAt(pattern, pattern_index + 1, text, next)) return true;
        }
        return false;
    }
    if (text_index == text.len) return false;
    if (pattern[pattern_index] == '?' or pattern[pattern_index] == text[text_index]) {
        return documentSqlWildcardMatchesAt(pattern, pattern_index + 1, text, text_index + 1);
    }
    return false;
}

fn documentSqlWildcardMatchesIgnoreCaseAt(pattern: []const u8, pattern_index: usize, text: []const u8, text_index: usize) bool {
    if (pattern_index == pattern.len) return text_index == text.len;
    if (pattern[pattern_index] == '*') {
        var next = text_index;
        while (next <= text.len) : (next += 1) {
            if (documentSqlWildcardMatchesIgnoreCaseAt(pattern, pattern_index + 1, text, next)) return true;
        }
        return false;
    }
    if (text_index == text.len) return false;
    if (pattern[pattern_index] == '?' or std.ascii.toLower(pattern[pattern_index]) == std.ascii.toLower(text[text_index])) {
        return documentSqlWildcardMatchesIgnoreCaseAt(pattern, pattern_index + 1, text, text_index + 1);
    }
    return false;
}
