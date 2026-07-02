// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

const db_mod = @import("../storage/db/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const document_sql_corpus = @import("document_sql_corpus.zig");
const sql_adapter = @import("document_plan.zig");
const sql_plan = @import("plan.zig");
const storage_schema = @import("../storage/schema.zig");

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
                try rows.append(alloc, try documentSqlProjectedRowJsonAlloc(alloc, id, lookup.json, lowered.projection));
                if (lowered.limit) |limit| {
                    if (rows.items.len >= limit) break;
                }
            }
        },
        .indexed_query => |query| {
            const query_limit = query.max_candidate_rows orelse lowered.limit;
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, false, false, consistency)) orelse return null;
            defer query_response.deinit(alloc);
            try appendDocumentSqlRowsFromQueryResponseAlloc(alloc, source, native_table_name, public_table_name, query_response.json, lowered.projection, query.residual_filter_json, lowered.limit, consistency, &rows);
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
                try rows.append(alloc, try documentSqlProjectedRowJsonAlloc(alloc, key_value.string, lookup.json, lowered.projection));
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
            var query_response = (try documentSqlIndexQueryAlloc(alloc, source, native_table_name, public_table_name, query, query_limit, query.residual_filter_json != null, false, consistency)) orelse return error.UnsupportedOperation;
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

    switch (lowered.template) {
        .delete => {
            candidate_ids_transferred = true;
            return .{
                .deletes = candidate_ids,
                .predicates = predicates,
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
                return .{
                    .predicates = predicates,
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
                errdefer alloc.free(transform_key);
                const operations = try cloneDocumentMutationTemplateOpsAlloc(alloc, operations_template);
                transforms[initialized] = .{
                    .key = transform_key,
                    .operations = operations,
                };
                initialized += 1;
            }
            freeDocumentCandidateIds(alloc, candidate_ids);
            candidate_ids_transferred = true;
            return .{
                .transforms = transforms,
                .predicates = predicates,
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
    sort_key: DocumentSqlSortKey,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.id);
        alloc.free(self.doc_json);
        self.sort_key.deinit(alloc);
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
                try appendOrderedDocumentSqlCandidateAlloc(alloc, &candidates, id, lookup.json, order_by);
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
                try appendOrderedDocumentSqlCandidateAlloc(alloc, &candidates, key_value.string, lookup.json, order_by);
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
        try rows.append(alloc, try documentSqlProjectedRowJsonAlloc(alloc, candidate.id, candidate.doc_json, lowered.projection));
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
                try appendOrderedDocumentSqlCandidateAlloc(alloc, candidates, id_value.string, doc_json, order_by);
                continue;
            }
            if (source_value != .null) return error.InvalidRowsRequest;
        }

        var lookup = (try documentSqlLookupAlloc(alloc, source, native_table_name, public_table_name, id_value.string, .{}, consistency)) orelse continue;
        defer lookup.deinit(alloc);
        if (residual_filter_json) |filter| {
            if (!try residualFilterMatchesAlloc(alloc, lookup.json, filter)) continue;
        }
        try appendOrderedDocumentSqlCandidateAlloc(alloc, candidates, id_value.string, lookup.json, order_by);
    }
}

fn appendOrderedDocumentSqlCandidateAlloc(
    alloc: std.mem.Allocator,
    candidates: *std.ArrayListUnmanaged(OrderedDocumentSqlCandidate),
    id: []const u8,
    doc_json: []const u8,
    order_by: sql_adapter.DocumentOrderBy,
) !void {
    const owned_id = try alloc.dupe(u8, id);
    errdefer alloc.free(owned_id);
    const owned_doc = try alloc.dupe(u8, doc_json);
    errdefer alloc.free(owned_doc);
    var sort_key = try documentSqlSortKeyAlloc(alloc, id, doc_json, order_by);
    errdefer sort_key.deinit(alloc);
    try candidates.append(alloc, .{
        .id = owned_id,
        .doc_json = owned_doc,
        .sort_key = sort_key,
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
        try out.appendSlice(alloc, "{\"query\":");
        try appendJsonString(alloc, &out, full_text);
        try out.append(alloc, '}');
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
                try rows.append(alloc, try documentSqlProjectedParsedRowJsonAlloc(alloc, id_value.string, source_value, doc_json, projection));
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
        try rows.append(alloc, try documentSqlProjectedRowJsonAlloc(alloc, id_value.string, lookup.json, projection));
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
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, doc_json, .{ .allocate = .alloc_always }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return try documentSqlProjectedParsedRowJsonWithUnnestAlloc(alloc, key, parsed.value, doc_json, projection, null);
}

fn documentSqlProjectedParsedRowJsonAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
) ![]const u8 {
    return try documentSqlProjectedParsedRowJsonWithUnnestAlloc(alloc, key, row, full_doc_json, projection, null);
}

fn documentSqlProjectedParsedRowJsonWithUnnestAlloc(
    alloc: std.mem.Allocator,
    key: []const u8,
    row: std.json.Value,
    full_doc_json: ?[]const u8,
    projection: []const sql_adapter.DocumentProjection,
    unnest_value: ?std.json.Value,
) ![]const u8 {
    if (row != .object) return error.InvalidRowsRequest;
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
                if (documentSqlProjectedValue(row, item.field)) |value| {
                    const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
                    defer alloc.free(value_json);
                    try writer.writeAll(value_json);
                } else {
                    try writer.writeAll("null");
                }
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

    for (array_value.array.items) |item| {
        if (filter_value) |value| {
            if (!documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_values) |values| {
            if (!documentSqlJsonValueInArray(item, values.value)) continue;
        }
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

    for (array_value.array.items) |item| {
        if (filter_value) |value| {
            if (!documentSqlJsonValuesEqual(item, value.value)) continue;
        }
        if (filter_values) |values| {
            if (!documentSqlJsonValueInArray(item, values.value)) continue;
        }
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

fn documentSqlJsonNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        .number_string => |text| try std.fmt.parseFloat(f64, text),
        else => error.InvalidRowsRequest,
    };
}

fn documentSqlFilterStringValue(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        .number_string => |text| text,
        else => error.InvalidRowsRequest,
    };
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

fn documentSqlAsciiOnly(text: []const u8) bool {
    for (text) |ch| {
        if (ch >= 0x80) return false;
    }
    return true;
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
        .having = &.{
            .{
                .key = .aggregate,
                .field_type = .numeric,
                .op = .gt,
                .value_json = "25",
            },
        },
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
        .having = &.{
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
        },
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
