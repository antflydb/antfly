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

const db_mod = @import("../storage/db/mod.zig");
const platform_time = @import("../platform/time.zig");
const runtime_schema = @import("../storage/schema.zig");

const physical_primary_key_prefix = "\x00antfly-rel-pk:";

pub const OwnedRowsBatchRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    transforms: []db_mod.types.DocumentTransform = &.{},
    predicates: []db_mod.types.TransactionVersionPredicate = &.{},
    returning_rows: [][]const u8 = &.{},
    req: db_mod.types.BatchRequest = .{},
    inserted: u32 = 0,
    deleted: u32 = 0,
    transformed: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(key);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        for (self.transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.transforms.len > 0) alloc.free(self.transforms);
        for (self.predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (self.predicates.len > 0) alloc.free(self.predicates);
        for (self.returning_rows) |row| alloc.free(@constCast(row));
        if (self.returning_rows.len > 0) alloc.free(self.returning_rows);
        self.* = undefined;
    }
};

pub const OwnedRowsGetRequest = struct {
    keys: []?[]const u8 = &.{},
    identities_json: [][]const u8 = &.{},
    include_physical_key: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.keys) |key| if (key) |value| alloc.free(value);
        if (self.keys.len > 0) alloc.free(self.keys);
        for (self.identities_json) |identity| alloc.free(identity);
        if (self.identities_json.len > 0) alloc.free(self.identities_json);
        self.* = undefined;
    }
};

pub const RowsQueryOrderDirection = db_mod.types.RelationalRowsQueryOrderDirection;
pub const RowsQueryOrderNullTest = db_mod.types.RelationalRowsQueryOrderNullTest;
pub const RowsQueryOrder = db_mod.types.RelationalRowsQueryOrder;
pub const OwnedRowsQueryRequest = db_mod.types.RelationalRowsQueryRequest;
pub const OwnedRowsQueryPlan = db_mod.types.RelationalRowsQueryPlan;
pub const OwnedRowsQueryResult = db_mod.types.RelationalRowsQueryResult;
pub const OwnedRowsAggregateRequest = db_mod.types.RelationalRowsAggregateRequest;
pub const OwnedRowsAggregatePlan = db_mod.types.RelationalRowsAggregatePlan;
pub const OwnedRowsWindowRequest = db_mod.types.RelationalRowsWindowRequest;
pub const OwnedRowsWindowPlan = db_mod.types.RelationalRowsWindowPlan;
pub const OwnedRowsJoinRequest = db_mod.types.RelationalRowsJoinRequest;
pub const OwnedRowsJoinPlan = db_mod.types.RelationalRowsJoinPlan;
pub const OwnedRowsLateralRequest = db_mod.types.RelationalRowsLateralRequest;
pub const OwnedRowsLateralPlan = db_mod.types.RelationalRowsLateralPlan;
pub const OwnedRowsMutationSourceResult = db_mod.types.RelationalRowsMutationSourceResult;

pub const OwnedRowsMutationSourceRequest = struct {
    req: db_mod.types.RelationalRowsMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.req.source.deinit(alloc);
        for (self.req.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (self.req.operations.len > 0) alloc.free(self.req.operations);
        for (self.req.returning) |field| alloc.free(@constCast(field));
        if (self.req.returning.len > 0) alloc.free(self.req.returning);
        for (self.req.returning_expressions) |projection| {
            alloc.free(@constCast(projection.output));
            freeRowsQueryExpression(alloc, projection.expression);
        }
        if (self.req.returning_expressions.len > 0) alloc.free(self.req.returning_expressions);
        self.* = undefined;
    }
};

pub const UniqueSelectorResolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8,
    resolve_primary: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool = null,
    lookup_primary: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?ResolvedPrimaryRow = null,

    pub fn resolveUnique(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) !?[]u8 {
        return try self.resolve(self.ptr, alloc, table_name, constraint_name, encoded_value);
    }

    pub fn primaryExists(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) !bool {
        const func = self.resolve_primary orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, physical_key);
    }

    pub fn lookupPrimary(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) !?ResolvedPrimaryRow {
        const func = self.lookup_primary orelse return error.UnsupportedRowsSelector;
        return try func(self.ptr, alloc, table_name, physical_key);
    }
};

pub const ResolvedPrimaryRow = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub fn parseRowsBatchRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsBatchRequest {
    return try parseRowsBatchRequestWithResolver(alloc, "", body, schema, null);
}

pub fn parseRowsBatchRequestWithResolver(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    schema: runtime_schema.TableSchema,
    unique_resolver: ?UniqueSelectorResolver,
) !OwnedRowsBatchRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    const operations_value = parsed.value.object.get("operations") orelse return error.InvalidRowsRequest;
    if (operations_value != .array) return error.InvalidRowsRequest;
    const sync_level = if (parsed.value.object.get("sync_level")) |value|
        db_mod.types.parsePublicSyncLevelJson(value) orelse return error.InvalidRowsRequest
    else
        db_mod.types.SyncLevel.propose;

    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    errdefer {
        freeWrites(alloc, writes.items);
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        freeDeletes(alloc, deletes.items);
        deletes.deinit(alloc);
    }
    var transforms = std.ArrayListUnmanaged(db_mod.types.DocumentTransform).empty;
    errdefer {
        freeTransforms(alloc, transforms.items);
        transforms.deinit(alloc);
    }
    var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
    errdefer {
        freePredicates(alloc, predicates.items);
        predicates.deinit(alloc);
    }
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        freeDeletes(alloc, returning_rows.items);
        returning_rows.deinit(alloc);
    }

    var inserted: u32 = 0;
    var deleted: u32 = 0;
    var transformed: u32 = 0;

    for (operations_value.array.items) |op_value| {
        if (op_value != .object) return error.InvalidRowsRequest;
        const op_text_value = op_value.object.get("op") orelse return error.InvalidRowsRequest;
        if (op_text_value != .string) return error.InvalidRowsRequest;
        const op_text = op_text_value.string;

        if (std.mem.eql(u8, op_text, "insert") or std.mem.eql(u8, op_text, "upsert")) {
            const row_value = op_value.object.get("row") orelse return error.InvalidRowsRequest;
            if (row_value != .object) return error.InvalidRowsRequest;
            if (op_value.object.get("on_conflict")) |conflict_value| {
                if (!std.mem.eql(u8, op_text, "insert")) return error.InvalidRowsRequest;
                try appendInsertWithConflictAlloc(
                    alloc,
                    table_name,
                    schema,
                    row_value,
                    op_value,
                    conflict_value,
                    unique_resolver orelse return error.UnsupportedRowsSelector,
                    &writes,
                    &transforms,
                    &predicates,
                    &returning_rows,
                    &inserted,
                    &transformed,
                );
            } else {
                try appendInsertAlloc(
                    alloc,
                    schema,
                    row_value,
                    std.mem.eql(u8, op_text, "insert"),
                    &writes,
                    &predicates,
                );
                try appendReturningProjectionAlloc(alloc, schema, &returning_rows, op_value, writes.items[writes.items.len - 1].value);
                inserted += 1;
            }
            continue;
        }

        if (std.mem.eql(u8, op_text, "delete")) {
            const key = (try physicalPrimaryKeyFromWhereAlloc(alloc, table_name, schema, op_value.object.get("where") orelse return error.InvalidRowsRequest, unique_resolver, false)) orelse return error.RowSelectorNotFound;
            var key_transferred = false;
            errdefer if (!key_transferred) alloc.free(key);
            var returning_base = try returningBaseRowForKey(alloc, table_name, key, unique_resolver, op_value);
            defer if (returning_base) |*row| row.deinit(alloc);
            try appendExpectedVersionPredicateAlloc(alloc, &predicates, op_value, key);
            if (returning_base) |row| {
                try appendVersionPredicateAlloc(alloc, &predicates, key, row.version);
                try appendReturningProjectionFromJsonAlloc(alloc, schema, &returning_rows, op_value, row.json);
            }
            try deletes.append(alloc, key);
            key_transferred = true;
            deleted += 1;
            continue;
        }

        if (std.mem.eql(u8, op_text, "update")) {
            const key = (try physicalPrimaryKeyFromWhereAlloc(alloc, table_name, schema, op_value.object.get("where") orelse return error.InvalidRowsRequest, unique_resolver, false)) orelse return error.RowSelectorNotFound;
            var key_transferred = false;
            errdefer if (!key_transferred) alloc.free(key);
            var operations = try updateTransformOperationsAlloc(alloc, schema, op_value);
            var operations_transferred = false;
            errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
            operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);
            const needs_planned_row = schemaHasGeneratedColumns(schema) or schema.checks.len != 0 or hasReturningProjection(op_value);
            var returning_base = if (needs_planned_row)
                try lookupBaseRowForKey(alloc, table_name, key, unique_resolver)
            else
                null;
            defer if (returning_base) |*row| row.deinit(alloc);
            try appendExpectedVersionPredicateAlloc(alloc, &predicates, op_value, key);
            if (returning_base) |row| {
                try appendVersionPredicateAlloc(alloc, &predicates, key, row.version);
                const projected_json = (try db_mod.transform.resolveDocumentTransform(alloc, row.json, .{ .key = key, .operations = operations })) orelse return error.RowSelectorNotFound;
                defer alloc.free(projected_json);
                const planned_json = try plannedExistingRelationalRowJsonAlloc(alloc, schema, projected_json);
                defer alloc.free(planned_json);
                if (schemaHasGeneratedColumns(schema)) operations = try extendOperationsWithGeneratedColumnsAlloc(alloc, operations, schema, planned_json);
                try appendReturningProjectionFromJsonAlloc(alloc, schema, &returning_rows, op_value, planned_json);
            }
            try transforms.append(alloc, .{ .key = key, .operations = operations });
            key_transferred = true;
            operations_transferred = true;
            transformed += 1;
            continue;
        }

        return error.InvalidRowsRequest;
    }

    const writes_owned = try writes.toOwnedSlice(alloc);
    errdefer {
        freeWrites(alloc, writes_owned);
        alloc.free(writes_owned);
    }
    const deletes_owned = try deletes.toOwnedSlice(alloc);
    errdefer {
        freeDeletes(alloc, deletes_owned);
        alloc.free(deletes_owned);
    }
    const transforms_owned = try transforms.toOwnedSlice(alloc);
    errdefer {
        freeTransforms(alloc, transforms_owned);
        alloc.free(transforms_owned);
    }
    const predicates_owned = try predicates.toOwnedSlice(alloc);
    errdefer {
        freePredicates(alloc, predicates_owned);
        alloc.free(predicates_owned);
    }
    const returning_rows_owned = try returning_rows.toOwnedSlice(alloc);
    errdefer {
        freeDeletes(alloc, returning_rows_owned);
        alloc.free(returning_rows_owned);
    }

    return .{
        .writes = writes_owned,
        .deletes = deletes_owned,
        .transforms = transforms_owned,
        .predicates = predicates_owned,
        .returning_rows = returning_rows_owned,
        .req = .{
            .writes = writes_owned,
            .deletes = deletes_owned,
            .transforms = transforms_owned,
            .predicates = predicates_owned,
            .sync_level = sync_level,
        },
        .inserted = inserted,
        .deleted = deleted,
        .transformed = transformed,
    };
}

pub fn parseRowsGetRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsGetRequest {
    return try parseRowsGetRequestWithResolver(alloc, "", body, schema, null);
}

pub fn parseRowsGetRequestWithResolver(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    schema: runtime_schema.TableSchema,
    unique_resolver: ?UniqueSelectorResolver,
) !OwnedRowsGetRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    const include_physical_key = if (parsed.value.object.get("include_physical_key")) |value| blk: {
        if (value != .bool) return error.InvalidRowsRequest;
        break :blk value.bool;
    } else false;

    const keys_value = parsed.value.object.get("keys") orelse return error.InvalidRowsRequest;
    if (keys_value != .array) return error.InvalidRowsRequest;

    var keys = std.ArrayListUnmanaged(?[]const u8).empty;
    errdefer {
        freeOptionalKeys(alloc, keys.items);
        keys.deinit(alloc);
    }
    var identities = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        freeDeletes(alloc, identities.items);
        identities.deinit(alloc);
    }

    for (keys_value.array.items) |selector| {
        const key = try physicalPrimaryKeyFromWhereAlloc(alloc, table_name, schema, selector, unique_resolver, true);
        var key_transferred = false;
        errdefer if (!key_transferred) if (key) |value| alloc.free(value);
        const identity_json = try identityResponseJsonAlloc(alloc, selector);
        var identity_transferred = false;
        errdefer if (!identity_transferred) alloc.free(identity_json);
        try keys.append(alloc, key);
        key_transferred = true;
        try identities.append(alloc, identity_json);
        identity_transferred = true;
    }

    const keys_owned = try keys.toOwnedSlice(alloc);
    errdefer {
        freeOptionalKeys(alloc, keys_owned);
        alloc.free(keys_owned);
    }
    const identities_owned = try identities.toOwnedSlice(alloc);
    errdefer {
        freeDeletes(alloc, identities_owned);
        alloc.free(identities_owned);
    }
    return .{
        .keys = keys_owned,
        .identities_json = identities_owned,
        .include_physical_key = include_physical_key,
    };
}

pub fn parseRowsQueryRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsQueryRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    const source_cte = try parseRowsQuerySourceCteAlloc(alloc, parsed.value.object.get("source_cte"));
    errdefer if (source_cte.len > 0) alloc.free(source_cte);

    const predicates = try parseRowsQueryPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeQueryPredicates(alloc, predicates);

    const or_predicates = try parseRowsQueryOrPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryPredicateGroups(alloc, or_predicates);

    const not_predicates = try parseRowsQueryNotPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryPredicateGroups(alloc, not_predicates);

    const array_any = try parseRowsQueryArrayAnyPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryArrayAnyPredicates(alloc, array_any);

    const array_contains = try parseRowsQueryArrayContainsPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryArrayContainsPredicates(alloc, array_contains);

    const array_eq = try parseRowsQueryArrayEqPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryArrayEqPredicates(alloc, array_eq);

    const in_predicates = try parseRowsQueryInPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryInPredicates(alloc, in_predicates);

    const json_contains = try parseRowsQueryJsonContainsPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryJsonContainsPredicates(alloc, json_contains);

    const json_path_eq = try parseRowsQueryJsonPathEqPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryJsonPathEqPredicates(alloc, json_path_eq);

    const json_path_exists = try parseRowsQueryJsonPathExistsPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryJsonPathExistsPredicates(alloc, json_path_exists);

    const select_parsed = try parseRowsQuerySelectAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer {
        for (select_parsed.fields) |field| alloc.free(field);
        if (select_parsed.fields.len > 0) alloc.free(select_parsed.fields);
    }

    const json_extract = try parseRowsQueryJsonExtractProjectionsAlloc(alloc, schema, parsed.value.object.get("json_extract"));
    errdefer freeRowsQueryJsonExtractProjections(alloc, json_extract);

    const array_length = try parseRowsQueryArrayLengthProjectionsAlloc(alloc, schema, parsed.value.object.get("array_length"));
    errdefer freeRowsQueryArrayLengthProjections(alloc, array_length);

    const coalesce = try parseRowsQueryCoalesceProjectionsAlloc(alloc, schema, parsed.value.object.get("coalesce"));
    errdefer freeRowsQueryCoalesceProjections(alloc, coalesce);

    const expressions = try parseRowsQueryExpressionProjectionsAlloc(alloc, schema, parsed.value.object.get("expressions"));
    errdefer freeRowsQueryExpressionProjections(alloc, expressions);

    const field_aliases = try parseRowsQueryFieldAliasProjectionsAlloc(alloc, schema, parsed.value.object.get("field_aliases"));
    errdefer freeRowsQueryFieldAliasProjections(alloc, field_aliases);

    const order_by = try parseRowsQueryOrderAlloc(alloc, schema, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    const row_claim = try parseRowsQueryRowClaimAlloc(alloc, parsed.value.object.get("row_claim"));
    errdefer if (row_claim) |claim| if (claim.owner_id.len > 0) alloc.free(claim.owner_id);

    const doc_key_range = try parseRowsQueryDocKeyRangeAlloc(alloc, parsed.value.object.get("doc_key_range"));
    errdefer if (doc_key_range) |range| {
        if (range.start.len > 0) alloc.free(range.start);
        if (range.end.len > 0) alloc.free(range.end);
    };

    return .{
        .source_cte = source_cte,
        .predicates = predicates,
        .array_any = array_any,
        .array_contains = array_contains,
        .array_eq = array_eq,
        .in_predicates = in_predicates,
        .json_contains = json_contains,
        .json_path_eq = json_path_eq,
        .json_path_exists = json_path_exists,
        .or_predicates = or_predicates,
        .not_predicates = not_predicates,
        .select = select_parsed.fields,
        .json_extract = json_extract,
        .array_length = array_length,
        .coalesce = coalesce,
        .field_aliases = field_aliases,
        .expressions = expressions,
        .select_all = select_parsed.all and json_extract.len == 0 and array_length.len == 0 and coalesce.len == 0 and field_aliases.len == 0 and expressions.len == 0,
        .order_by = order_by,
        .row_claim = row_claim,
        .doc_key_range = doc_key_range,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

pub fn parseRowsMutationSourceRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsMutationSourceRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    const op_value = parsed.value.object.get("op") orelse return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const kind: db_mod.types.RelationalRowsMutationKind = if (std.mem.eql(u8, op_value.string, "update"))
        .update
    else if (std.mem.eql(u8, op_value.string, "delete"))
        .delete
    else
        return error.InvalidRowsRequest;

    const source_value = parsed.value.object.get("source") orelse return error.InvalidRowsRequest;
    if (source_value != .object) return error.InvalidRowsRequest;
    const source_json = try jsonValueStringifyAlloc(alloc, source_value);
    defer alloc.free(source_json);
    var source = try parseRowsQueryRequest(alloc, source_json, schema);
    errdefer source.deinit(alloc);
    if (source.row_claim == null) return error.InvalidRowsRequest;
    if (source.source_cte.len != 0) return error.InvalidRowsRequest;

    var operations: []db_mod.types.TransformOp = &.{};
    if (kind == .update) {
        operations = try updateTransformOperationsAlloc(alloc, schema, parsed.value);
        errdefer freeTransformOps(alloc, operations);
        operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);
    } else if (parsed.value.object.get("patch") != null or parsed.value.object.get("increment") != null or parsed.value.object.get("json_set") != null or parsed.value.object.get("array_update") != null) {
        return error.InvalidRowsRequest;
    }

    const returning = try parseMutationSourceReturningAlloc(alloc, schema, parsed.value.object.get("returning"), parsed.value.object.get("returning_expressions"));
    errdefer {
        for (returning.fields) |field| alloc.free(field);
        if (returning.fields.len > 0) alloc.free(returning.fields);
        for (returning.expressions) |projection| {
            alloc.free(projection.output);
            freeRowsQueryExpression(alloc, projection.expression);
        }
        if (returning.expressions.len > 0) alloc.free(returning.expressions);
    }

    return .{ .req = .{
        .kind = kind,
        .source = source,
        .operations = operations,
        .returning = returning.fields,
        .returning_expressions = returning.expressions,
        .returning_all = returning.all,
    } };
}

pub fn parseRowsAggregateRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsAggregateRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var source: OwnedRowsQueryRequest = if (parsed.value.object.get("source")) |source_value| blk: {
        if (source_value != .object) return error.InvalidRowsRequest;
        const source_json = try jsonValueStringifyAlloc(alloc, source_value);
        defer alloc.free(source_json);
        break :blk try parseRowsQueryRequest(alloc, source_json, schema);
    } else .{};
    errdefer source.deinit(alloc);
    if (source.row_claim != null or source.doc_key_range != null) return error.InvalidRowsRequest;

    const group_by = try parseRowsAggregateGroupByAlloc(alloc, schema, parsed.value.object.get("group_by"));
    errdefer freeStringSlice(alloc, group_by);

    const aggregations = try parseRowsAggregateSpecsAlloc(alloc, schema, parsed.value.object.get("aggregations"));
    errdefer freeRowsAggregateSpecs(alloc, aggregations);

    const having_predicates = try parseRowsAggregateOutputPredicatesAlloc(alloc, parsed.value.object.get("having"));
    errdefer {
        freeQueryPredicates(alloc, having_predicates);
        if (having_predicates.len > 0) alloc.free(having_predicates);
    }

    const order_by = try parseRowsAggregateOutputOrderAlloc(alloc, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .source = source,
        .group_by = group_by,
        .aggregations = aggregations,
        .having_predicates = having_predicates,
        .order_by = order_by,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

pub fn parseRowsWindowRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsWindowRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var source: OwnedRowsQueryRequest = if (parsed.value.object.get("source")) |source_value| blk: {
        if (source_value != .object) return error.InvalidRowsRequest;
        const source_json = try jsonValueStringifyAlloc(alloc, source_value);
        defer alloc.free(source_json);
        break :blk try parseRowsQueryRequest(alloc, source_json, schema);
    } else .{};
    errdefer source.deinit(alloc);
    if (source.row_claim != null or source.doc_key_range != null) return error.InvalidRowsRequest;

    const windows = try parseRowsWindowSpecsAlloc(alloc, schema, parsed.value.object.get("windows"));
    errdefer freeRowsWindowSpecs(alloc, windows);

    const select_parsed = try parseRowsQuerySelectAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer freeStringSlice(alloc, select_parsed.fields);

    const order_by = try parseRowsAggregateOutputOrderAlloc(alloc, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .source = source,
        .windows = windows,
        .select = select_parsed.fields,
        .select_all = select_parsed.all,
        .order_by = order_by,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

pub fn parseRowsJoinRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsJoinRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var left = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("left") orelse return error.InvalidRowsRequest);
    errdefer left.deinit(alloc);
    var right = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("right") orelse return error.InvalidRowsRequest);
    errdefer right.deinit(alloc);

    const on = try parseRowsJoinOnAlloc(alloc, schema, parsed.value.object.get("on"));
    errdefer freeRowsJoinOn(alloc, on);

    const select = try parseRowsJoinProjectionsAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer freeRowsJoinProjections(alloc, select);

    const order_by = try parseRowsAggregateOutputOrderAlloc(alloc, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .left = left,
        .right = right,
        .on = on,
        .join_type = try parseRowsJoinType(parsed.value.object.get("join_type")),
        .select = select,
        .order_by = order_by,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

pub fn parseRowsLateralRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsLateralRequest {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var left = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("left") orelse return error.InvalidRowsRequest);
    errdefer left.deinit(alloc);
    var right = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("right") orelse return error.InvalidRowsRequest);
    errdefer right.deinit(alloc);
    if (right.limit == null) return error.InvalidRowsRequest;

    const correlations = try parseRowsLateralCorrelationsAlloc(alloc, schema, parsed.value.object.get("correlations"));
    errdefer freeRowsLateralCorrelations(alloc, correlations);

    const select = try parseRowsJoinProjectionsAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer freeRowsJoinProjections(alloc, select);

    const order_by = try parseRowsAggregateOutputOrderAlloc(alloc, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .left = left,
        .right = right,
        .correlations = correlations,
        .select = select,
        .order_by = order_by,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

pub fn parseRowsQueryPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsQueryPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema);
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    var query = try parseRowsQueryRequestFromValue(alloc, schema, parsed.value.object.get("query") orelse return error.InvalidRowsRequest);
    errdefer query.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, query);

    return .{
        .ctes = ctes,
        .query = query,
    };
}

pub fn parseRowsAggregatePlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsAggregatePlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema);
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    var aggregate = try parseRowsAggregateRequestFromValue(alloc, schema, parsed.value.object.get("aggregate") orelse return error.InvalidRowsRequest);
    errdefer aggregate.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, aggregate.source);

    return .{
        .ctes = ctes,
        .aggregate = aggregate,
    };
}

pub fn parseRowsWindowPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsWindowPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema);
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    var window = try parseRowsWindowRequestFromValue(alloc, schema, parsed.value.object.get("window") orelse return error.InvalidRowsRequest);
    errdefer window.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, window.source);

    return .{
        .ctes = ctes,
        .window = window,
    };
}

pub fn parseRowsJoinPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsJoinPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema);
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    var join = try parseRowsJoinRequestFromValue(alloc, schema, parsed.value.object.get("join") orelse return error.InvalidRowsRequest);
    errdefer join.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, join.left);
    try validateRowsQuerySourceCteReference(ctes, join.right);

    return .{
        .ctes = ctes,
        .join = join,
    };
}

pub fn parseRowsLateralPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsLateralPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema);
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    var lateral = try parseRowsLateralRequestFromValue(alloc, schema, parsed.value.object.get("lateral") orelse return error.InvalidRowsRequest);
    errdefer lateral.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, lateral.left);
    try validateRowsQuerySourceCteReference(ctes, lateral.right);

    return .{
        .ctes = ctes,
        .lateral = lateral,
    };
}

pub fn encodeRowsMutationSourceResponseAlloc(
    alloc: std.mem.Allocator,
    result: OwnedRowsMutationSourceResult,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"matched\":{d},\"staged\":{d}", .{ result.matched, result.staged });
    if (result.returning_rows.len > 0) {
        try writer.writeAll(",\"returning\":[");
        for (result.returning_rows, 0..) |row, i| {
            if (i > 0) try writer.writeByte(',');
            try writer.writeAll(row);
        }
        try writer.writeByte(']');
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn executeRowsQueryOnJsonRowsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    request: OwnedRowsQueryRequest,
    rows: []const []const u8,
) !OwnedRowsQueryResult {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (request.source_cte.len != 0) return error.UnsupportedRowsQuery;
    if (request.row_claim != null) return error.UnsupportedRowsQuery;
    if (request.doc_key_range != null) return error.UnsupportedRowsQuery;

    var candidates = std.ArrayListUnmanaged(QueryCandidate).empty;
    defer {
        for (candidates.items) |candidate| freeQueryOrderKeySlice(alloc, candidate.order_keys);
        candidates.deinit(alloc);
    }

    for (rows, 0..) |row_json, ordinal| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        if (!try queryRequestPredicatesPass(alloc, parsed.value, request)) continue;

        const order_keys = try queryOrderKeysAlloc(alloc, parsed.value, request.order_by);
        errdefer freeQueryOrderKeys(alloc, order_keys);
        try candidates.append(alloc, .{
            .row_json = row_json,
            .order_keys = order_keys,
            .ordinal = ordinal,
        });
    }

    if (request.order_by.len > 0) {
        std.sort.pdq(QueryCandidate, candidates.items, QuerySortContext{ .order_by = request.order_by }, queryCandidateLessThan);
    }

    const total: u32 = @intCast(candidates.items.len);
    const start = @min(@as(usize, request.offset), candidates.items.len);
    const limited_len: usize = if (request.limit) |limit|
        @min(@as(usize, limit), candidates.items.len - start)
    else
        candidates.items.len - start;

    var out_rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out_rows.items) |row| alloc.free(@constCast(row));
        out_rows.deinit(alloc);
    }
    for (candidates.items[start .. start + limited_len]) |candidate| {
        const projected = try projectRowsQueryRowAlloc(alloc, request, candidate.row_json);
        var projected_transferred = false;
        errdefer if (!projected_transferred) alloc.free(projected);
        try out_rows.append(alloc, projected);
        projected_transferred = true;
    }

    return .{
        .rows = try out_rows.toOwnedSlice(alloc),
        .total = total,
    };
}

pub fn encodeRowsBatchResponseAlloc(alloc: std.mem.Allocator, req: OwnedRowsBatchRequest) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"inserted\":{d},\"deleted\":{d},\"transformed\":{d}", .{
        req.inserted,
        req.deleted,
        req.transformed,
    });
    if (req.returning_rows.len > 0) {
        try writer.writeAll(",\"returning\":[");
        for (req.returning_rows, 0..) |row, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.writeAll(row);
        }
        try writer.writeByte(']');
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn encodeRowsQueryResponseAlloc(alloc: std.mem.Allocator, result: OwnedRowsQueryResult) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"total\":{d},\"rows\":[", .{result.total});
    for (result.rows, 0..) |row, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.writeAll(row);
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn encodeRowsGetResponseAlloc(
    alloc: std.mem.Allocator,
    rows: []const RowLookupResult,
    include_physical_key: bool,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"rows\":[");
    for (rows, 0..) |row, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.writeAll("{\"identity\":");
        try writer.writeAll(row.identity_json);
        if (row.found) {
            try writer.writeAll(",\"found\":true,\"row\":");
            try writer.writeAll(row.row_json.?);
            try writer.print(",\"version\":{d}", .{row.version.?});
        } else {
            try writer.writeAll(",\"found\":false");
        }
        if (include_physical_key) {
            if (row.physical_key) |physical_key| {
                try writer.print(",\"physical_key\":{f}", .{std.json.fmt(physical_key, .{})});
            } else {
                try writer.writeAll(",\"physical_key\":null");
            }
        }
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub const RowLookupResult = struct {
    identity_json: []const u8,
    physical_key: ?[]const u8 = null,
    found: bool,
    row_json: ?[]const u8 = null,
    version: ?u64 = null,
};

const ParsedRowsQuerySelect = struct {
    fields: []const []const u8 = &.{},
    all: bool = true,
};

const ParsedMutationSourceReturning = struct {
    fields: []const []const u8 = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},
    all: bool = false,
};

const QueryCandidate = struct {
    row_json: []const u8,
    order_keys: []QueryOrderKey = &.{},
    ordinal: usize,
};

const QueryOrderKey = union(enum) {
    missing,
    null,
    bool: bool,
    number: f64,
    string: []const u8,
};

const QuerySortContext = struct {
    order_by: []const RowsQueryOrder,
};

fn parseRowsAggregateGroupByAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_group_by: ?std.json.Value,
) ![]const []const u8 {
    const group_by_value = maybe_group_by orelse return &.{};
    if (group_by_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc([]const u8, group_by_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |field| alloc.free(field);
        alloc.free(out);
    }
    for (group_by_value.array.items) |field_value| {
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        out[initialized] = try alloc.dupe(u8, field_value.string);
        initialized += 1;
    }
    return out;
}

fn parseRowsAggregateSpecsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_aggregations: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsAggregateSpec {
    const aggregations_value = maybe_aggregations orelse return error.InvalidRowsRequest;
    if (aggregations_value != .array or aggregations_value.array.items.len == 0) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsAggregateSpec, aggregations_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |spec| freeRowsAggregateSpec(alloc, spec);
        alloc.free(out);
    }
    for (aggregations_value.array.items) |item| {
        out[initialized] = try parseRowsAggregateSpecAlloc(alloc, schema, item);
        initialized += 1;
    }
    return out;
}

fn parseRowsAggregateSpecAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsAggregateSpec {
    if (value != .object) return error.InvalidRowsRequest;
    const name_value = value.object.get("name") orelse return error.InvalidRowsRequest;
    const op_value = value.object.get("op") orelse return error.InvalidRowsRequest;
    if (name_value != .string or name_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const op = parseRowsAggregateOp(op_value.string) orelse return error.InvalidRowsRequest;

    const name = try alloc.dupe(u8, name_value.string);
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);

    const field_value = value.object.get("field");
    const expression_value = value.object.get("expr");
    if ((field_value != null) and (expression_value != null)) return error.InvalidRowsRequest;
    if (op != .count and field_value == null and expression_value == null) return error.InvalidRowsRequest;

    const field: ?[]const u8 = if (field_value) |field_json| blk: {
        if (field_json != .string or field_json.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_json.string) orelse return error.InvalidRowsRequest;
        try validateRowsAggregateFieldInput(op, column);
        break :blk try alloc.dupe(u8, field_json.string);
    } else null;
    var field_transferred = false;
    errdefer if (!field_transferred) if (field) |owned| alloc.free(owned);

    const expression: ?db_mod.types.RelationalRowsExpression = if (expression_value) |expr_json| blk: {
        const parsed = try parseRowsQueryExpressionAlloc(alloc, schema, expr_json);
        errdefer freeRowsQueryExpression(alloc, parsed);
        try validateRowsAggregateExpressionInput(alloc, schema, op, parsed);
        break :blk parsed;
    } else null;
    var expression_transferred = false;
    errdefer if (!expression_transferred) if (expression) |owned| freeRowsQueryExpression(alloc, owned);

    const distinct = try parseOptionalBool(value.object.get("distinct")) orelse false;
    const distinct_max_items = if (distinct)
        (try parseOptionalU32(value.object.get("distinct_max_items"))) orelse db_mod.types.default_relational_rows_aggregate_distinct_max_items
    else blk: {
        if (value.object.get("distinct_max_items") != null) return error.InvalidRowsRequest;
        break :blk 0;
    };
    if (distinct and distinct_max_items == 0) return error.InvalidRowsRequest;

    const array_max_items = if (op == .array_agg)
        (try parseOptionalU32(value.object.get("array_max_items"))) orelse db_mod.types.default_relational_rows_array_agg_max_items
    else blk: {
        if (value.object.get("array_max_items") != null) return error.InvalidRowsRequest;
        break :blk 0;
    };
    if (op == .array_agg and array_max_items == 0) return error.InvalidRowsRequest;

    const array_order_by = if (op == .array_agg)
        try parseRowsQueryOrderAlloc(alloc, schema, value.object.get("array_order_by"))
    else blk: {
        if (value.object.get("array_order_by") != null) return error.InvalidRowsRequest;
        break :blk @as([]RowsQueryOrder, &.{});
    };
    var array_order_transferred = false;
    errdefer if (!array_order_transferred) {
        freeRowsQueryOrder(alloc, array_order_by);
        if (array_order_by.len > 0) alloc.free(array_order_by);
    };

    const filter_predicates = try parseRowsQueryPredicatesAlloc(alloc, schema, value.object.get("filter"));
    errdefer {
        freeQueryPredicates(alloc, filter_predicates);
        if (filter_predicates.len > 0) alloc.free(filter_predicates);
    }

    const filter_expressions = try parseRowsAggregateFilterExpressionsAlloc(alloc, schema, value.object.get("filter_expressions"));
    errdefer freeRowsQueryExpressionConditions(alloc, filter_expressions);

    name_transferred = true;
    field_transferred = true;
    expression_transferred = true;
    array_order_transferred = true;
    return .{
        .name = name,
        .op = op,
        .field = field,
        .expression = expression,
        .distinct = distinct,
        .distinct_max_items = distinct_max_items,
        .array_max_items = array_max_items,
        .array_order_by = array_order_by,
        .filter_predicates = filter_predicates,
        .filter_expressions = filter_expressions,
    };
}

fn parseRowsAggregateOp(value: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.mem.eql(u8, value, "count")) return .count;
    if (std.mem.eql(u8, value, "sum")) return .sum;
    if (std.mem.eql(u8, value, "min")) return .min;
    if (std.mem.eql(u8, value, "max")) return .max;
    if (std.mem.eql(u8, value, "avg")) return .avg;
    if (std.mem.eql(u8, value, "array_agg")) return .array_agg;
    return null;
}

fn validateRowsAggregateFieldInput(
    op: db_mod.types.RelationalRowsAggregateOp,
    column: runtime_schema.RelationalColumn,
) !void {
    switch (op) {
        .count, .array_agg => if (column.field_type == .array or column.field_type == .json) return error.InvalidRowsRequest,
        .sum, .avg, .min, .max => if (column.field_type != .numeric) return error.InvalidRowsRequest,
    }
}

fn validateRowsAggregateExpressionInput(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    op: db_mod.types.RelationalRowsAggregateOp,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    switch (op) {
        .count, .array_agg => {},
        .sum, .avg, .min, .max => try validateRowsQueryNumericExpression(alloc, schema, expression),
    }
}

fn parseRowsAggregateFilterExpressionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_expressions: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const expressions_value = maybe_expressions orelse return &.{};
    if (expressions_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, expressions_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeRowsQueryExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (expressions_value.array.items) |item| {
        out[initialized] = try parseRowsQueryExpressionConditionAlloc(alloc, schema, item);
        initialized += 1;
    }
    return out;
}

fn parseRowsAggregateOutputPredicatesAlloc(
    alloc: std.mem.Allocator,
    maybe_having: ?std.json.Value,
) ![]const runtime_schema.RelationalCheck {
    const having_value = maybe_having orelse return &.{};
    if (having_value != .object) return error.InvalidRowsRequest;
    if (having_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        const out = try alloc.alloc(runtime_schema.RelationalCheck, all_value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            freeQueryPredicates(alloc, out[0..initialized]);
            alloc.free(out);
        }
        for (all_value.array.items) |atom| {
            out[initialized] = try parseRowsAggregateOutputPredicateAlloc(alloc, atom);
            initialized += 1;
        }
        return out;
    }
    const out = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    errdefer alloc.free(out);
    out[0] = try parseRowsAggregateOutputPredicateAlloc(alloc, having_value);
    return out;
}

fn parseRowsAggregateOutputPredicateAlloc(
    alloc: std.mem.Allocator,
    atom: std.json.Value,
) !runtime_schema.RelationalCheck {
    if (atom != .object) return error.InvalidRowsRequest;
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = atom.object.get("op") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const op = try parseRowsQueryPredicateOp(op_value.string);
    const value_json: ?[]const u8 = if (rowsQueryPredicateNeedsValue(op)) blk: {
        const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
        break :blk try jsonValueStringifyAlloc(alloc, value);
    } else blk: {
        if (atom.object.get("value") != null) return error.InvalidRowsRequest;
        break :blk null;
    };
    errdefer if (value_json) |value| alloc.free(value);
    return .{
        .name = "",
        .field = try alloc.dupe(u8, field_value.string),
        .op = op,
        .value_json = value_json,
    };
}

fn parseRowsAggregateOutputOrderAlloc(
    alloc: std.mem.Allocator,
    maybe_order: ?std.json.Value,
) ![]RowsQueryOrder {
    const order_value = maybe_order orelse return &.{};
    if (order_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(RowsQueryOrder, order_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeRowsQueryOrder(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (order_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const direction = if (item.object.get("direction")) |direction_value| blk: {
            if (direction_value != .string) return error.InvalidRowsRequest;
            if (std.mem.eql(u8, direction_value.string, "asc")) break :blk RowsQueryOrderDirection.asc;
            if (std.mem.eql(u8, direction_value.string, "desc")) break :blk RowsQueryOrderDirection.desc;
            return error.InvalidRowsRequest;
        } else RowsQueryOrderDirection.asc;
        out[initialized] = .{
            .field = try alloc.dupe(u8, field_value.string),
            .direction = direction,
        };
        initialized += 1;
    }
    return out;
}

fn parseRowsWindowSpecsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_windows: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsWindowSpec {
    const windows_value = maybe_windows orelse return error.InvalidRowsRequest;
    if (windows_value != .array or windows_value.array.items.len == 0) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsWindowSpec, windows_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |spec| freeRowsWindowSpec(alloc, spec);
        alloc.free(out);
    }
    for (windows_value.array.items) |item| {
        out[initialized] = try parseRowsWindowSpecAlloc(alloc, schema, item);
        initialized += 1;
    }
    return out;
}

fn parseRowsWindowSpecAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsWindowSpec {
    if (value != .object) return error.InvalidRowsRequest;
    const output_value = value.object.get("as") orelse value.object.get("output") orelse return error.InvalidRowsRequest;
    const function_value = value.object.get("function") orelse return error.InvalidRowsRequest;
    if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
    if (function_value != .string) return error.InvalidRowsRequest;
    const function = parseRowsWindowFunction(function_value.string) orelse return error.InvalidRowsRequest;

    const output = try alloc.dupe(u8, output_value.string);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);

    const partition_by = try parseRowsWindowPartitionByAlloc(alloc, schema, value.object.get("partition_by"));
    var partition_transferred = false;
    errdefer if (!partition_transferred) freeStringSlice(alloc, partition_by);

    const order_by = try parseRowsQueryOrderAlloc(alloc, schema, value.object.get("order_by"));
    var order_transferred = false;
    errdefer if (!order_transferred) {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    };
    if (order_by.len == 0) return error.InvalidRowsRequest;

    output_transferred = true;
    partition_transferred = true;
    order_transferred = true;
    return .{
        .output = output,
        .function = function,
        .partition_by = partition_by,
        .order_by = order_by,
    };
}

fn parseRowsWindowFunction(value: []const u8) ?db_mod.types.RelationalRowsWindowFunction {
    if (std.mem.eql(u8, value, "row_number")) return .row_number;
    return null;
}

fn parseRowsWindowPartitionByAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_partition_by: ?std.json.Value,
) ![]const []const u8 {
    const partition_value = maybe_partition_by orelse return &.{};
    if (partition_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc([]const u8, partition_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |field| alloc.free(field);
        alloc.free(out);
    }
    for (partition_value.array.items) |field_value| {
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        out[initialized] = try alloc.dupe(u8, field_value.string);
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinSourceAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsQueryRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const source_json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(source_json);
    var source = try parseRowsQueryRequest(alloc, source_json, schema);
    errdefer source.deinit(alloc);
    if (source.row_claim != null or source.doc_key_range != null) return error.InvalidRowsRequest;
    return source;
}

fn parseRowsJoinOnAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_on: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsJoinOn {
    const on_value = maybe_on orelse return error.InvalidRowsRequest;
    if (on_value != .array or on_value.array.items.len == 0) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsJoinOn, on_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |join_on| {
            alloc.free(join_on.left_field);
            alloc.free(join_on.right_field);
        }
        alloc.free(out);
    }
    for (on_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const left_value = item.object.get("left_field") orelse item.object.get("left") orelse return error.InvalidRowsRequest;
        const right_value = item.object.get("right_field") orelse item.object.get("right") orelse return error.InvalidRowsRequest;
        if (left_value != .string or left_value.string.len == 0) return error.InvalidRowsRequest;
        if (right_value != .string or right_value.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, left_value.string) orelse return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, right_value.string) orelse return error.InvalidRowsRequest;
        const left_field = try alloc.dupe(u8, left_value.string);
        var left_transferred = false;
        errdefer if (!left_transferred) alloc.free(left_field);
        const right_field = try alloc.dupe(u8, right_value.string);
        var right_transferred = false;
        errdefer if (!right_transferred) alloc.free(right_field);
        out[initialized] = .{
            .left_field = left_field,
            .right_field = right_field,
        };
        left_transferred = true;
        right_transferred = true;
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinType(maybe_type: ?std.json.Value) !db_mod.types.RelationalRowsJoinType {
    const type_value = maybe_type orelse return .inner;
    if (type_value != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, type_value.string, "inner")) return .inner;
    if (std.mem.eql(u8, type_value.string, "left")) return .left;
    return error.InvalidRowsRequest;
}

fn parseRowsJoinProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_select: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsJoinProjection {
    const select_value = maybe_select orelse return &.{};
    if (select_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsJoinProjection, select_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        alloc.free(out);
    }
    for (select_value.array.items) |item| {
        out[initialized] = try parseRowsJoinProjectionAlloc(alloc, schema, item);
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinProjectionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsJoinProjection {
    if (value != .object) return error.InvalidRowsRequest;
    const output_value = value.object.get("as") orelse value.object.get("output") orelse return error.InvalidRowsRequest;
    const side_value = value.object.get("side") orelse return error.InvalidRowsRequest;
    const field_value = value.object.get("field") orelse return error.InvalidRowsRequest;
    if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
    if (side_value != .string) return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    const side: db_mod.types.RelationalRowsJoinProjectionSide = if (std.mem.eql(u8, side_value.string, "left"))
        .left
    else if (std.mem.eql(u8, side_value.string, "right"))
        .right
    else
        return error.InvalidRowsRequest;
    const output = try alloc.dupe(u8, output_value.string);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);
    const field = try alloc.dupe(u8, field_value.string);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    output_transferred = true;
    field_transferred = true;
    return .{
        .output = output,
        .side = side,
        .field = field,
    };
}

fn parseRowsLateralCorrelationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_correlations: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsLateralCorrelation {
    const correlations_value = maybe_correlations orelse return error.InvalidRowsRequest;
    if (correlations_value != .array or correlations_value.array.items.len == 0) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsLateralCorrelation, correlations_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |correlation| {
            alloc.free(correlation.left_field);
            alloc.free(correlation.right_field);
        }
        alloc.free(out);
    }
    for (correlations_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const left_value = item.object.get("left_field") orelse item.object.get("left") orelse return error.InvalidRowsRequest;
        const right_value = item.object.get("right_field") orelse item.object.get("right") orelse return error.InvalidRowsRequest;
        if (left_value != .string or left_value.string.len == 0) return error.InvalidRowsRequest;
        if (right_value != .string or right_value.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, left_value.string) orelse return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, right_value.string) orelse return error.InvalidRowsRequest;
        const left_field = try alloc.dupe(u8, left_value.string);
        var left_transferred = false;
        errdefer if (!left_transferred) alloc.free(left_field);
        const right_field = try alloc.dupe(u8, right_value.string);
        var right_transferred = false;
        errdefer if (!right_transferred) alloc.free(right_field);
        out[initialized] = .{
            .left_field = left_field,
            .right_field = right_field,
        };
        left_transferred = true;
        right_transferred = true;
        initialized += 1;
    }
    return out;
}

fn parseRowsPlanEnvelope(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !std.json.Parsed(std.json.Value) {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return parsed;
}

fn parseRowsCtesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_ctes: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsCte {
    const ctes_value = maybe_ctes orelse return &.{};
    if (ctes_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsCte, ctes_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |cte| {
            var owned = cte;
            owned.deinit(alloc);
        }
        alloc.free(out);
    }

    for (ctes_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const name_value = item.object.get("name") orelse return error.InvalidRowsRequest;
        const query_value = item.object.get("query") orelse return error.InvalidRowsRequest;
        if (name_value != .string or name_value.string.len == 0) return error.InvalidRowsRequest;
        if (rowsCteNameExists(out[0..initialized], name_value.string)) return error.InvalidRowsRequest;

        const name = try alloc.dupe(u8, name_value.string);
        var name_transferred = false;
        errdefer if (!name_transferred) alloc.free(name);
        var query = try parseRowsQueryRequestFromValue(alloc, schema, query_value);
        errdefer query.deinit(alloc);
        if (query.row_claim != null or query.doc_key_range != null) return error.InvalidRowsRequest;
        if (query.source_cte.len != 0 and !rowsCteNameExists(out[0..initialized], query.source_cte)) return error.InvalidRowsRequest;

        out[initialized] = .{
            .name = name,
            .query = query,
        };
        name_transferred = true;
        initialized += 1;
    }
    return out;
}

fn rowsCteNameExists(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) bool {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return true;
    }
    return false;
}

fn validateRowsQuerySourceCteReference(
    ctes: []const db_mod.types.RelationalRowsCte,
    query: OwnedRowsQueryRequest,
) !void {
    if (query.source_cte.len == 0) return;
    if (!rowsCteNameExists(ctes, query.source_cte)) return error.InvalidRowsRequest;
}

fn parseRowsQueryRequestFromValue(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsQueryRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(json);
    return try parseRowsQueryRequest(alloc, json, schema);
}

fn parseRowsAggregateRequestFromValue(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsAggregateRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(json);
    return try parseRowsAggregateRequest(alloc, json, schema);
}

fn parseRowsWindowRequestFromValue(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsWindowRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(json);
    return try parseRowsWindowRequest(alloc, json, schema);
}

fn parseRowsJoinRequestFromValue(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsJoinRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(json);
    return try parseRowsJoinRequest(alloc, json, schema);
}

fn parseRowsLateralRequestFromValue(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsLateralRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(json);
    return try parseRowsLateralRequest(alloc, json, schema);
}

fn parseRowsQueryPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]runtime_schema.RelationalCheck {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;

    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        var out = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeQueryPredicates(alloc, out.items);
            out.deinit(alloc);
        }
        for (all_value.array.items) |atom| {
            if (rowsQueryPredicateAtomOpIsStructured(atom)) continue;
            try out.append(alloc, try parseRowsQueryPredicateAtomAlloc(alloc, schema, atom));
        }
        return try out.toOwnedSlice(alloc);
    }

    if (where_value.object.get("any") != null or where_value.object.get("not") != null) return &.{};

    if (where_value.object.get("field") != null or where_value.object.get("op") != null) {
        if (rowsQueryPredicateAtomOpIsStructured(where_value)) return &.{};
        const predicate = try parseRowsQueryPredicateAtomAlloc(alloc, schema, where_value);
        const out = try alloc.alloc(runtime_schema.RelationalCheck, 1);
        out[0] = predicate;
        return out;
    }

    var out = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    errdefer {
        freeQueryPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    var it = where_value.object.iterator();
    while (it.next()) |entry| {
        _ = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        const field = try alloc.dupe(u8, entry.key_ptr.*);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const value_json = try jsonValueStringifyAlloc(alloc, entry.value_ptr.*);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try out.append(alloc, .{
            .name = "",
            .field = field,
            .op = .eq,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryOrPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsPredicateGroup {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;
    const any_value = where_value.object.get("any") orelse return &.{};
    if (any_value != .array or any_value.array.items.len == 0) return error.InvalidRowsRequest;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, any_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            freeQueryPredicates(alloc, group.predicates);
            if (group.predicates.len > 0) alloc.free(group.predicates);
        }
        alloc.free(groups);
    }

    for (any_value.array.items) |branch| {
        const predicates = try parseRowsQueryPredicateGroupAlloc(alloc, schema, branch);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeQueryPredicates(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        if (predicates.len == 0) return error.InvalidRowsRequest;
        groups[initialized] = .{ .predicates = predicates };
        predicates_transferred = true;
        initialized += 1;
    }

    return groups;
}

fn parseRowsQueryNotPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsPredicateGroup {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;
    const not_value = where_value.object.get("not") orelse return &.{};
    if (not_value != .array or not_value.array.items.len == 0) return error.InvalidRowsRequest;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsPredicateGroup, not_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            freeQueryPredicates(alloc, group.predicates);
            if (group.predicates.len > 0) alloc.free(group.predicates);
        }
        alloc.free(groups);
    }

    for (not_value.array.items) |branch| {
        const predicates = try parseRowsQueryPredicateGroupAlloc(alloc, schema, branch);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeQueryPredicates(alloc, predicates);
            if (predicates.len > 0) alloc.free(predicates);
        };
        if (predicates.len == 0) return error.InvalidRowsRequest;
        groups[initialized] = .{ .predicates = predicates };
        predicates_transferred = true;
        initialized += 1;
    }

    return groups;
}

fn parseRowsQueryPredicateGroupAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    branch: std.json.Value,
) ![]runtime_schema.RelationalCheck {
    if (branch != .object) return error.InvalidRowsRequest;
    if (branch.object.get("all")) |all_value| {
        if (all_value != .array or all_value.array.items.len == 0) return error.InvalidRowsRequest;
        var out = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeQueryPredicates(alloc, out.items);
            out.deinit(alloc);
        }
        for (all_value.array.items) |atom| {
            if (rowsQueryPredicateAtomOpIsStructured(atom)) return error.InvalidRowsRequest;
            try out.append(alloc, try parseRowsQueryPredicateAtomAlloc(alloc, schema, atom));
        }
        return try out.toOwnedSlice(alloc);
    }
    if (rowsQueryPredicateAtomOpIsStructured(branch)) return error.InvalidRowsRequest;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    errdefer alloc.free(out);
    out[0] = try parseRowsQueryPredicateAtomAlloc(alloc, schema, branch);
    return out;
}

fn parseRowsQuerySourceCteAlloc(alloc: std.mem.Allocator, maybe_source_cte: ?std.json.Value) ![]const u8 {
    const value = maybe_source_cte orelse return "";
    if (value != .string or value.string.len == 0) return error.InvalidRowsRequest;
    return try alloc.dupe(u8, value.string);
}

fn rowsQueryPredicateAtomOpIsStructured(atom: std.json.Value) bool {
    if (atom != .object) return false;
    const op_value = atom.object.get("op") orelse return false;
    if (op_value != .string) return false;
    return std.mem.eql(u8, op_value.string, "array_any") or
        std.mem.eql(u8, op_value.string, "array_contains") or
        std.mem.eql(u8, op_value.string, "array_eq") or
        std.mem.eql(u8, op_value.string, "in") or
        std.mem.eql(u8, op_value.string, "not_in") or
        std.mem.eql(u8, op_value.string, "json_contains") or
        std.mem.eql(u8, op_value.string, "json_path_eq") or
        std.mem.eql(u8, op_value.string, "json_path_exists");
}

fn parseRowsQueryArrayAnyPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayAnyPredicate {
    return try parseRowsQueryStructuredPredicatesAlloc(db_mod.types.RelationalRowsArrayAnyPredicate, alloc, schema, maybe_where, "array_any", .array);
}

fn parseRowsQueryArrayContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayContainsPredicate {
    const predicates = try parseRowsQueryStructuredPredicatesAlloc(db_mod.types.RelationalRowsArrayContainsPredicate, alloc, schema, maybe_where, "array_contains", .array);
    errdefer freeRowsQueryArrayContainsPredicates(alloc, predicates);
    for (predicates) |predicate| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
    }
    return predicates;
}

fn parseRowsQueryArrayEqPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayEqPredicate {
    const predicates = try parseRowsQueryStructuredPredicatesAlloc(db_mod.types.RelationalRowsArrayEqPredicate, alloc, schema, maybe_where, "array_eq", .array);
    errdefer freeRowsQueryArrayEqPredicates(alloc, predicates);
    for (predicates) |predicate| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
    }
    return predicates;
}

fn parseRowsQueryInPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsInPredicate {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    errdefer {
        freeRowsQueryInPredicates(alloc, out.items);
        out.deinit(alloc);
    }

    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        for (all_value.array.items) |atom| {
            if (rowsQueryPredicateAtomOpEquals(atom, "in")) {
                try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, false));
            } else if (rowsQueryPredicateAtomOpEquals(atom, "not_in")) {
                try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, true));
            }
        }
    } else if (rowsQueryPredicateAtomOpEquals(where_value, "in")) {
        try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, where_value, false));
    } else if (rowsQueryPredicateAtomOpEquals(where_value, "not_in")) {
        try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, where_value, true));
    }

    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryInPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
    negated: bool,
) !db_mod.types.RelationalRowsInPredicate {
    if (atom != .object) return error.InvalidRowsRequest;
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type == .array or column.field_type == .json) return error.InvalidRowsRequest;
    if (value != .array) return error.InvalidRowsRequest;
    const field = try alloc.dupe(u8, field_value.string);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const values_json = try jsonValueStringifyAlloc(alloc, value);
    var values_transferred = false;
    errdefer if (!values_transferred) alloc.free(values_json);
    field_transferred = true;
    values_transferred = true;
    return .{ .field = field, .values_json = values_json, .negated = negated };
}

fn parseRowsQueryJsonContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonContainsPredicate {
    return try parseRowsQueryStructuredPredicatesAlloc(db_mod.types.RelationalRowsJsonContainsPredicate, alloc, schema, maybe_where, "json_contains", .json);
}

fn parseRowsQueryJsonPathEqPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonPathEqPredicate {
    return try parseRowsQueryJsonPathPredicatesAlloc(db_mod.types.RelationalRowsJsonPathEqPredicate, alloc, schema, maybe_where, "json_path_eq");
}

fn parseRowsQueryJsonPathExistsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonPathExistsPredicate {
    return try parseRowsQueryJsonPathPredicatesAlloc(db_mod.types.RelationalRowsJsonPathExistsPredicate, alloc, schema, maybe_where, "json_path_exists");
}

fn parseRowsQueryJsonPathPredicatesAlloc(
    comptime Predicate: type,
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
    op_name: []const u8,
) ![]Predicate {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(Predicate).empty;
    errdefer {
        for (out.items) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.path);
            if (comptime @hasField(Predicate, "value_json")) alloc.free(predicate.value_json);
        }
        out.deinit(alloc);
    }

    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        for (all_value.array.items) |atom| {
            if (!rowsQueryPredicateAtomOpEquals(atom, op_name)) continue;
            try out.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(Predicate, alloc, schema, atom, op_name));
        }
    } else if (rowsQueryPredicateAtomOpEquals(where_value, op_name)) {
        try out.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(Predicate, alloc, schema, where_value, op_name));
    }

    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryJsonPathPredicateAtomAlloc(
    comptime Predicate: type,
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
    op_name: []const u8,
) !Predicate {
    if (atom != .object) return error.InvalidRowsRequest;
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = atom.object.get("op") orelse return error.InvalidRowsRequest;
    const path_value = atom.object.get("path") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, op_name)) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type != .json) return error.InvalidRowsRequest;

    const field = try alloc.dupe(u8, field_value.string);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const path = try parseRowsQueryJsonPathAlloc(alloc, path_value);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(path);

    if (comptime @hasField(Predicate, "value_json")) {
        const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
        const value_json = try jsonValueStringifyAlloc(alloc, value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        field_transferred = true;
        path_transferred = true;
        value_transferred = true;
        return .{ .field = field, .path = path, .value_json = value_json };
    }

    field_transferred = true;
    path_transferred = true;
    return .{ .field = field, .path = path };
}

fn parseRowsQueryJsonPathAlloc(alloc: std.mem.Allocator, path_value: std.json.Value) ![]u8 {
    switch (path_value) {
        .string => |path| {
            if (path.len == 0) return error.InvalidRowsRequest;
            return try alloc.dupe(u8, path);
        },
        .array => |segments| {
            if (segments.items.len == 0) return error.InvalidRowsRequest;
            var out = std.Io.Writer.Allocating.init(alloc);
            errdefer out.deinit();
            const writer = &out.writer;
            for (segments.items, 0..) |segment, i| {
                if (segment != .string or segment.string.len == 0 or std.mem.indexOfScalar(u8, segment.string, '.') != null) return error.InvalidRowsRequest;
                if (i != 0) try writer.writeByte('.');
                try writer.writeAll(segment.string);
            }
            return try out.toOwnedSlice();
        },
        else => return error.InvalidRowsRequest,
    }
}

fn parseRowsQueryStructuredPredicatesAlloc(
    comptime Predicate: type,
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
    op_name: []const u8,
    expected_type: runtime_schema.AntflyType,
) ![]Predicate {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(Predicate).empty;
    errdefer {
        for (out.items) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        out.deinit(alloc);
    }

    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        for (all_value.array.items) |atom| {
            if (!rowsQueryPredicateAtomOpEquals(atom, op_name)) continue;
            try out.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(Predicate, alloc, schema, atom, op_name, expected_type));
        }
    } else if (rowsQueryPredicateAtomOpEquals(where_value, op_name)) {
        try out.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(Predicate, alloc, schema, where_value, op_name, expected_type));
    }

    return try out.toOwnedSlice(alloc);
}

fn rowsQueryPredicateAtomOpEquals(atom: std.json.Value, op_name: []const u8) bool {
    if (atom != .object) return false;
    const op_value = atom.object.get("op") orelse return false;
    return op_value == .string and std.mem.eql(u8, op_value.string, op_name);
}

fn parseRowsQueryStructuredPredicateAtomAlloc(
    comptime Predicate: type,
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
    op_name: []const u8,
    expected_type: runtime_schema.AntflyType,
) !Predicate {
    if (atom != .object) return error.InvalidRowsRequest;
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = atom.object.get("op") orelse return error.InvalidRowsRequest;
    const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, op_name)) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type != expected_type) return error.InvalidRowsRequest;

    const field = try alloc.dupe(u8, field_value.string);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try jsonValueStringifyAlloc(alloc, value);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);

    field_transferred = true;
    value_transferred = true;
    return .{
        .field = field,
        .value_json = value_json,
    };
}

fn parseRowsQueryPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
) !runtime_schema.RelationalCheck {
    if (atom != .object) return error.InvalidRowsRequest;
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = atom.object.get("op") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    const op = try parseRowsQueryPredicateOp(op_value.string);
    const value_json: ?[]const u8 = if (rowsQueryPredicateNeedsValue(op)) blk: {
        const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
        break :blk try jsonValueStringifyAlloc(alloc, value);
    } else blk: {
        if (atom.object.get("value") != null) return error.InvalidRowsRequest;
        break :blk null;
    };
    errdefer if (value_json) |value| alloc.free(value);
    const field = try alloc.dupe(u8, field_value.string);
    return .{
        .name = "",
        .field = field,
        .op = op,
        .value_json = value_json,
    };
}

fn parseRowsQueryPredicateOp(op_text: []const u8) !runtime_schema.RelationalCheckOp {
    if (std.mem.eql(u8, op_text, "is_null")) return .is_null;
    if (std.mem.eql(u8, op_text, "is_not_null")) return .is_not_null;
    if (std.mem.eql(u8, op_text, "eq")) return .eq;
    if (std.mem.eql(u8, op_text, "ne")) return .ne;
    if (std.mem.eql(u8, op_text, "gt")) return .gt;
    if (std.mem.eql(u8, op_text, "gte")) return .gte;
    if (std.mem.eql(u8, op_text, "lt")) return .lt;
    if (std.mem.eql(u8, op_text, "lte")) return .lte;
    return error.InvalidRowsRequest;
}

fn rowsQueryPredicateNeedsValue(op: runtime_schema.RelationalCheckOp) bool {
    return switch (op) {
        .is_null, .is_not_null => false,
        .eq, .ne, .gt, .gte, .lt, .lte => true,
    };
}

fn parseRowsQuerySelectAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_select: ?std.json.Value,
) !ParsedRowsQuerySelect {
    const select_value = maybe_select orelse return .{ .fields = &.{}, .all = true };
    if (select_value != .array or select_value.array.items.len == 0) return error.InvalidRowsRequest;
    if (select_value.array.items.len == 1 and select_value.array.items[0] == .string and std.mem.eql(u8, select_value.array.items[0].string, "*")) {
        return .{ .fields = &.{}, .all = true };
    }

    const fields = try alloc.alloc([]const u8, select_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (select_value.array.items) |field_value| {
        if (field_value != .string or field_value.string.len == 0 or std.mem.eql(u8, field_value.string, "*")) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        fields[initialized] = try alloc.dupe(u8, field_value.string);
        initialized += 1;
    }
    return .{ .fields = fields, .all = false };
}

fn parseMutationSourceReturningAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_returning: ?std.json.Value,
    maybe_expressions: ?std.json.Value,
) !ParsedMutationSourceReturning {
    const expressions = try parseRowsQueryExpressionProjectionsAlloc(alloc, schema, maybe_expressions);
    errdefer freeRowsQueryExpressionProjections(alloc, expressions);
    const returning_value = maybe_returning orelse return .{ .expressions = expressions };
    if (returning_value != .array or returning_value.array.items.len == 0) return error.InvalidRowsRequest;
    if (returning_value.array.items.len == 1 and returning_value.array.items[0] == .string and std.mem.eql(u8, returning_value.array.items[0].string, "*")) {
        if (expressions.len != 0) return error.InvalidRowsRequest;
        return .{ .fields = &.{}, .expressions = &.{}, .all = true };
    }

    const fields = try alloc.alloc([]const u8, returning_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (returning_value.array.items) |field_value| {
        if (field_value != .string or field_value.string.len == 0 or std.mem.eql(u8, field_value.string, "*")) return error.InvalidRowsRequest;
        fields[initialized] = try alloc.dupe(u8, field_value.string);
        initialized += 1;
    }
    return .{ .fields = fields, .expressions = expressions, .all = false };
}

fn parseRowsQueryJsonExtractProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_extract: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonExtractProjection {
    const extract_value = maybe_extract orelse return &.{};
    if (extract_value != .array) return error.InvalidRowsRequest;

    const projections = try alloc.alloc(db_mod.types.RelationalRowsJsonExtractProjection, extract_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
            alloc.free(projection.path);
        }
        alloc.free(projections);
    }

    for (extract_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        const path_value = item.object.get("path") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .json) return error.InvalidRowsRequest;
        const as_text = if (item.object.get("as_text")) |as_text_value| blk: {
            if (as_text_value != .bool) return error.InvalidRowsRequest;
            break :blk as_text_value.bool;
        } else false;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const field = try alloc.dupe(u8, field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const path = try parseRowsQueryJsonPathAlloc(alloc, path_value);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);

        projections[initialized] = .{
            .output = output,
            .field = field,
            .path = path,
            .as_text = as_text,
        };
        output_transferred = true;
        field_transferred = true;
        path_transferred = true;
        initialized += 1;
    }

    return projections;
}

fn parseRowsQueryArrayLengthProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_projection: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayLengthProjection {
    const projection_value = maybe_projection orelse return &.{};
    if (projection_value != .array) return error.InvalidRowsRequest;

    const projections = try alloc.alloc(db_mod.types.RelationalRowsArrayLengthProjection, projection_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        alloc.free(projections);
    }

    for (projection_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .array) return error.InvalidRowsRequest;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const field = try alloc.dupe(u8, field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);

        projections[initialized] = .{
            .output = output,
            .field = field,
        };
        output_transferred = true;
        field_transferred = true;
        initialized += 1;
    }

    return projections;
}

fn parseRowsQueryCoalesceProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_projection: ?std.json.Value,
) ![]db_mod.types.RelationalRowsCoalesceProjection {
    const projection_value = maybe_projection orelse return &.{};
    if (projection_value != .array) return error.InvalidRowsRequest;

    const projections = try alloc.alloc(db_mod.types.RelationalRowsCoalesceProjection, projection_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            alloc.free(projection.output);
            for (projection.operands) |operand| {
                switch (operand.kind) {
                    .field => if (operand.field.len > 0) alloc.free(operand.field),
                    .value => if (operand.value_json.len > 0) alloc.free(operand.value_json),
                }
            }
            if (projection.operands.len > 0) alloc.free(projection.operands);
        }
        alloc.free(projections);
    }

    for (projection_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const operands_value = item.object.get("operands") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
        if (operands_value != .array or operands_value.array.items.len == 0) return error.InvalidRowsRequest;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);

        const operands = try alloc.alloc(db_mod.types.RelationalRowsCoalesceOperand, operands_value.array.items.len);
        var operand_initialized: usize = 0;
        errdefer {
            for (operands[0..operand_initialized]) |operand| {
                switch (operand.kind) {
                    .field => if (operand.field.len > 0) alloc.free(operand.field),
                    .value => if (operand.value_json.len > 0) alloc.free(operand.value_json),
                }
            }
            alloc.free(operands);
        }

        for (operands_value.array.items) |operand_value| {
            if (operand_value != .object) return error.InvalidRowsRequest;
            const field_value = operand_value.object.get("field");
            const literal_value = operand_value.object.get("value");
            if ((field_value != null) == (literal_value != null)) return error.InvalidRowsRequest;
            if (field_value) |field| {
                if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
                _ = findRelationalColumn(schema.relational_columns, field.string) orelse return error.InvalidRowsRequest;
                operands[operand_initialized] = .{
                    .kind = .field,
                    .field = try alloc.dupe(u8, field.string),
                };
            } else if (literal_value) |literal| {
                const value_json = try std.json.Stringify.valueAlloc(alloc, literal, .{});
                operands[operand_initialized] = .{
                    .kind = .value,
                    .value_json = value_json,
                };
            }
            operand_initialized += 1;
        }

        projections[initialized] = .{
            .output = output,
            .operands = operands,
        };
        output_transferred = true;
        initialized += 1;
    }

    return projections;
}

fn parseRowsQueryExpressionProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_projection: ?std.json.Value,
) ![]db_mod.types.RelationalRowsExpressionProjection {
    const projection_value = maybe_projection orelse return &.{};
    if (projection_value != .array) return error.InvalidRowsRequest;

    const projections = try alloc.alloc(db_mod.types.RelationalRowsExpressionProjection, projection_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            alloc.free(projection.output);
            freeRowsQueryExpression(alloc, projection.expression);
        }
        alloc.free(projections);
    }

    for (projection_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const expression_value = item.object.get("expr") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const expression = try parseRowsQueryExpressionAlloc(alloc, schema, expression_value);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);

        projections[initialized] = .{
            .output = output,
            .expression = expression,
        };
        output_transferred = true;
        expression_transferred = true;
        initialized += 1;
    }

    return projections;
}

fn parseRowsQueryExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) anyerror!db_mod.types.RelationalRowsExpression {
    if (value != .object) return error.InvalidRowsRequest;
    const field_value = value.object.get("field");
    const literal_value = value.object.get("value");
    const op_value = value.object.get("op");
    const present_count: u8 = (if (field_value != null) @as(u8, 1) else 0) + (if (literal_value != null) @as(u8, 1) else 0) + (if (op_value != null) @as(u8, 1) else 0);
    if (present_count != 1) return error.InvalidRowsRequest;

    if (field_value) |field| {
        if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field.string) orelse return error.InvalidRowsRequest;
        return .{ .kind = .field, .field = try alloc.dupe(u8, field.string) };
    }
    if (literal_value) |literal| {
        return .{ .kind = .value, .value_json = try jsonValueStringifyAlloc(alloc, literal) };
    }

    const op = op_value.?;
    if (op != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, op.string, "case")) {
        return try parseRowsQueryCaseExpressionAlloc(alloc, schema, value);
    }

    const args_value = value.object.get("args") orelse return error.InvalidRowsRequest;
    if (args_value != .array or args_value.array.items.len == 0) return error.InvalidRowsRequest;
    const expression_kind: db_mod.types.RelationalRowsExpressionKind = if (std.mem.eql(u8, op.string, "coalesce"))
        .coalesce
    else if (std.mem.eql(u8, op.string, "lower"))
        .lower
    else if (std.mem.eql(u8, op.string, "concat"))
        .concat
    else if (std.mem.eql(u8, op.string, "nullif"))
        .nullif
    else if (std.mem.eql(u8, op.string, "add"))
        .add
    else if (std.mem.eql(u8, op.string, "sub"))
        .sub
    else if (std.mem.eql(u8, op.string, "mul"))
        .mul
    else if (std.mem.eql(u8, op.string, "div"))
        .div
    else if (std.mem.eql(u8, op.string, "cast"))
        .cast
    else if (std.mem.eql(u8, op.string, "json_extract"))
        .json_extract
    else if (std.mem.eql(u8, op.string, "array_length"))
        .array_length
    else
        return error.InvalidRowsRequest;
    if (expression_kind == .lower and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .nullif and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if (expression_kind == .add and args_value.array.items.len < 2) return error.InvalidRowsRequest;
    if (expression_kind == .sub and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if (expression_kind == .mul and args_value.array.items.len < 2) return error.InvalidRowsRequest;
    if (expression_kind == .div and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if (expression_kind == .cast and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .json_extract and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .array_length and args_value.array.items.len != 1) return error.InvalidRowsRequest;

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, args_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeRowsQueryExpression(alloc, operand);
        alloc.free(operands);
    }
    for (args_value.array.items) |arg| {
        operands[initialized] = try parseRowsQueryExpressionAlloc(alloc, schema, arg);
        initialized += 1;
    }
    const cast_type: ?db_mod.types.RelationalRowsExpressionCastType = if (expression_kind == .cast) blk: {
        const target_value = value.object.get("to") orelse return error.InvalidRowsRequest;
        if (target_value != .string) return error.InvalidRowsRequest;
        break :blk parseRowsQueryExpressionCastType(target_value.string) orelse return error.InvalidRowsRequest;
    } else null;
    const json_path: []const u8 = if (expression_kind == .json_extract) blk: {
        const path_value = value.object.get("path") orelse return error.InvalidRowsRequest;
        break :blk try parseRowsQueryJsonPathAlloc(alloc, path_value);
    } else "";
    errdefer if (json_path.len > 0) alloc.free(json_path);
    const json_as_text = if (expression_kind == .json_extract) blk: {
        const as_text_value = value.object.get("as_text") orelse break :blk false;
        if (as_text_value != .bool) return error.InvalidRowsRequest;
        break :blk as_text_value.bool;
    } else false;
    if (expression_kind == .json_extract) {
        const root = operands[0];
        if (root.kind == .field) {
            const column = findRelationalColumn(schema.relational_columns, root.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .json) return error.InvalidRowsRequest;
        }
    }
    if (expression_kind == .array_length) {
        const root = operands[0];
        if (root.kind == .field) {
            const column = findRelationalColumn(schema.relational_columns, root.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .array) return error.InvalidRowsRequest;
        }
    }
    const expression: db_mod.types.RelationalRowsExpression = .{ .kind = expression_kind, .operands = operands, .cast_type = cast_type, .json_path = json_path, .json_as_text = json_as_text };
    if (expression_kind == .add or expression_kind == .sub or expression_kind == .mul or expression_kind == .div) try validateRowsQueryNumericExpression(alloc, schema, expression);
    return expression;
}

fn parseRowsQueryExpressionCastType(text: []const u8) ?db_mod.types.RelationalRowsExpressionCastType {
    if (std.mem.eql(u8, text, "text")) return .text;
    if (std.mem.eql(u8, text, "numeric")) return .numeric;
    if (std.mem.eql(u8, text, "bool") or std.mem.eql(u8, text, "boolean")) return .bool;
    return null;
}

fn parseRowsQueryCaseExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) anyerror!db_mod.types.RelationalRowsExpression {
    const cases_value = value.object.get("cases") orelse return error.InvalidRowsRequest;
    if (cases_value != .array or cases_value.array.items.len == 0) return error.InvalidRowsRequest;
    const branches = try alloc.alloc(db_mod.types.RelationalRowsExpressionCaseBranch, cases_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (branches[0..initialized]) |branch| freeRowsQueryExpressionCaseBranch(alloc, branch);
        alloc.free(branches);
    }

    for (cases_value.array.items) |branch_value| {
        if (branch_value != .object) return error.InvalidRowsRequest;
        const when_value = branch_value.object.get("when") orelse return error.InvalidRowsRequest;
        const then_value = branch_value.object.get("then") orelse return error.InvalidRowsRequest;
        const when = try parseRowsQueryExpressionConditionAlloc(alloc, schema, when_value);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRowsQueryExpressionCondition(alloc, when);
        const then = try parseRowsQueryExpressionAlloc(alloc, schema, then_value);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRowsQueryExpression(alloc, then);
        branches[initialized] = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        initialized += 1;
    }

    const else_value = value.object.get("else") orelse return error.InvalidRowsRequest;
    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_transferred = false;
    errdefer {
        if (!fallback_transferred) alloc.free(fallback);
    }
    fallback[0] = try parseRowsQueryExpressionAlloc(alloc, schema, else_value);
    fallback_transferred = true;

    return .{
        .kind = .case,
        .case_branches = branches,
        .case_else = fallback,
    };
}

fn parseRowsQueryExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) anyerror!db_mod.types.RelationalRowsExpressionCondition {
    if (value != .object) return error.InvalidRowsRequest;
    const lhs_value = value.object.get("lhs") orelse return error.InvalidRowsRequest;
    const op_value = value.object.get("op") orelse return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const op = try parseRowsQueryPredicateOp(op_value.string);
    const rhs_needed = rowsQueryPredicateNeedsValue(op);
    const rhs_value = value.object.get("rhs");
    if (rhs_needed and rhs_value == null) return error.InvalidRowsRequest;
    if (!rhs_needed and rhs_value != null) return error.InvalidRowsRequest;

    const lhs = try parseRowsQueryExpressionAlloc(alloc, schema, lhs_value);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRowsQueryExpression(alloc, lhs);

    const rhs = if (rhs_value) |rhs_json| blk: {
        const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var out_transferred = false;
        errdefer if (!out_transferred) alloc.free(out);
        out[0] = try parseRowsQueryExpressionAlloc(alloc, schema, rhs_json);
        out_transferred = true;
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

fn validateRowsQueryNumericExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    switch (expression.kind) {
        .field => {
            const column = findRelationalColumn(schema.relational_columns, expression.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .numeric) return error.InvalidRowsRequest;
        },
        .value => {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null, .integer, .float, .number_string => {},
                else => return error.InvalidRowsRequest,
            }
        },
        .nullif => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpression(alloc, schema, operand);
        },
        .cast => {
            if (expression.operands.len != 1 or expression.cast_type != .numeric) return error.InvalidRowsRequest;
        },
        .add => {
            if (expression.operands.len < 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpression(alloc, schema, operand);
        },
        .mul => {
            if (expression.operands.len < 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpression(alloc, schema, operand);
        },
        .sub => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpression(alloc, schema, operand);
        },
        .div => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpression(alloc, schema, operand);
        },
        .case => {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidRowsRequest;
            for (expression.case_branches) |branch| try validateRowsQueryNumericExpression(alloc, schema, branch.then);
            try validateRowsQueryNumericExpression(alloc, schema, expression.case_else[0]);
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
        },
        .json_extract => return error.InvalidRowsRequest,
        else => return error.InvalidRowsRequest,
    }
}

fn parseRowsQueryFieldAliasProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_projection: ?std.json.Value,
) ![]db_mod.types.RelationalRowsFieldAliasProjection {
    const projection_value = maybe_projection orelse return &.{};
    if (projection_value != .array) return error.InvalidRowsRequest;

    const projections = try alloc.alloc(db_mod.types.RelationalRowsFieldAliasProjection, projection_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (projections[0..initialized]) |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        }
        alloc.free(projections);
    }

    for (projection_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const field = try alloc.dupe(u8, field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);

        projections[initialized] = .{
            .output = output,
            .field = field,
        };
        output_transferred = true;
        field_transferred = true;
        initialized += 1;
    }

    return projections;
}

fn parseRowsQueryOrderAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_order: ?std.json.Value,
) ![]RowsQueryOrder {
    const order_value = maybe_order orelse return &.{};
    if (order_value != .array) return error.InvalidRowsRequest;
    const orders = try alloc.alloc(RowsQueryOrder, order_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeRowsQueryOrder(alloc, orders[0..initialized]);
        alloc.free(orders);
    }
    for (order_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const field_value = item.object.get("field");
        const expression_value = item.object.get("expr");
        if ((field_value == null) == (expression_value == null)) return error.InvalidRowsRequest;
        const field = if (field_value) |value| blk: {
            if (value != .string or value.string.len == 0) return error.InvalidRowsRequest;
            _ = findRelationalColumn(schema.relational_columns, value.string) orelse return error.InvalidRowsRequest;
            break :blk try alloc.dupe(u8, value.string);
        } else "";
        var field_transferred = false;
        errdefer if (!field_transferred and field.len > 0) alloc.free(field);
        const expression = if (expression_value) |value| try parseRowsQueryExpressionAlloc(alloc, schema, value) else null;
        var expression_transferred = false;
        errdefer if (!expression_transferred) if (expression) |owned| freeRowsQueryExpression(alloc, owned);
        const null_test = if (item.object.get("null_test")) |null_test_value| blk: {
            if (null_test_value != .string) return error.InvalidRowsRequest;
            if (std.mem.eql(u8, null_test_value.string, "is_null")) break :blk RowsQueryOrderNullTest.is_null;
            if (std.mem.eql(u8, null_test_value.string, "is_not_null")) break :blk RowsQueryOrderNullTest.is_not_null;
            return error.InvalidRowsRequest;
        } else null;
        const direction = if (item.object.get("direction")) |direction_value| blk: {
            if (direction_value != .string) return error.InvalidRowsRequest;
            if (std.mem.eql(u8, direction_value.string, "asc")) break :blk RowsQueryOrderDirection.asc;
            if (std.mem.eql(u8, direction_value.string, "desc")) break :blk RowsQueryOrderDirection.desc;
            return error.InvalidRowsRequest;
        } else RowsQueryOrderDirection.asc;
        orders[initialized] = .{
            .field = field,
            .expression = expression,
            .direction = direction,
            .null_test = null_test,
        };
        field_transferred = true;
        expression_transferred = true;
        initialized += 1;
    }
    return orders;
}

fn parseOptionalU32(maybe_value: ?std.json.Value) !?u32 {
    const value = maybe_value orelse return null;
    if (value != .integer or value.integer < 0 or value.integer > std.math.maxInt(u32)) return error.InvalidRowsRequest;
    return @intCast(value.integer);
}

fn parseOptionalBool(maybe_value: ?std.json.Value) !?bool {
    const value = maybe_value orelse return null;
    if (value != .bool) return error.InvalidRowsRequest;
    return value.bool;
}

fn parseRowsQueryDocKeyRangeAlloc(
    alloc: std.mem.Allocator,
    maybe_range: ?std.json.Value,
) !?db_mod.types.RelationalRowsDocKeyRange {
    const range = maybe_range orelse return null;
    if (range != .object) return error.InvalidRowsRequest;

    const start = if (range.object.get("start")) |start_value| blk: {
        if (start_value != .string) return error.InvalidRowsRequest;
        break :blk try alloc.dupe(u8, start_value.string);
    } else try alloc.dupe(u8, "");
    errdefer alloc.free(start);

    const end = if (range.object.get("end")) |end_value| blk: {
        if (end_value != .string) return error.InvalidRowsRequest;
        break :blk try alloc.dupe(u8, end_value.string);
    } else try alloc.dupe(u8, "");
    errdefer alloc.free(end);

    if (start.len == 0 and end.len == 0) return error.InvalidRowsRequest;
    if (start.len > 0 and end.len > 0 and std.mem.order(u8, start, end) != .lt) return error.InvalidRowsRequest;

    return .{ .start = start, .end = end };
}

fn parseRowsQueryRowClaimAlloc(
    alloc: std.mem.Allocator,
    maybe_claim: ?std.json.Value,
) !?db_mod.types.RowClaimRequest {
    const claim = maybe_claim orelse return null;
    if (claim != .object) return error.InvalidRowsRequest;

    const mode = if (claim.object.get("mode")) |mode_value| blk: {
        if (mode_value != .string) return error.InvalidRowsRequest;
        if (std.mem.eql(u8, mode_value.string, "for_update")) break :blk db_mod.types.RowClaimMode.for_update;
        return error.InvalidRowsRequest;
    } else db_mod.types.RowClaimMode.for_update;

    const skip_locked = if (claim.object.get("skip_locked")) |skip_value| blk: {
        if (skip_value != .bool) return error.InvalidRowsRequest;
        break :blk skip_value.bool;
    } else false;

    const lease_ms = if (claim.object.get("lease_ms")) |lease_value| blk: {
        if (lease_value != .integer or lease_value.integer < 0) return error.InvalidRowsRequest;
        break :blk @as(u64, @intCast(lease_value.integer));
    } else @as(u64, 30_000);

    const owner_id = if (claim.object.get("owner_id")) |owner_value| blk: {
        if (owner_value != .string) return error.InvalidRowsRequest;
        break :blk try alloc.dupe(u8, owner_value.string);
    } else try alloc.dupe(u8, "");
    errdefer alloc.free(owner_id);

    const txn_value = claim.object.get("transaction_id") orelse claim.object.get("txn_id") orelse return error.InvalidRowsRequest;
    if (txn_value != .string) return error.InvalidRowsRequest;
    const txn_id = try parseTxnIdHex(txn_value.string);

    return .{
        .mode = mode,
        .skip_locked = skip_locked,
        .lease_ms = lease_ms,
        .owner_id = owner_id,
        .txn_id = txn_id,
    };
}

fn parseTxnIdHex(text: []const u8) !db_mod.types.TxnId {
    if (text.len != 32) return error.InvalidRowsRequest;
    var out: db_mod.types.TxnId = undefined;
    for (&out, 0..) |*byte, i| {
        const high = try hexNibble(text[i * 2]);
        const low = try hexNibble(text[i * 2 + 1]);
        byte.* = (high << 4) | low;
    }
    return out;
}

fn hexNibble(ch: u8) !u8 {
    if (ch >= '0' and ch <= '9') return ch - '0';
    if (ch >= 'a' and ch <= 'f') return 10 + ch - 'a';
    if (ch >= 'A' and ch <= 'F') return 10 + ch - 'A';
    return error.InvalidRowsRequest;
}

fn queryPredicatesPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicates: []const runtime_schema.RelationalCheck,
) !bool {
    for (predicates) |predicate| {
        if (!try relationalCheckPasses(alloc, row, predicate)) return false;
    }
    return true;
}

fn queryOrPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsPredicateGroup,
) !bool {
    if (groups.len == 0) return true;
    for (groups) |group| {
        if (try queryPredicatesPass(alloc, row, group.predicates)) return true;
    }
    return false;
}

fn queryNotPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsPredicateGroup,
) !bool {
    for (groups) |group| {
        if (try queryPredicatesPass(alloc, row, group.predicates)) return false;
    }
    return true;
}

fn queryRequestPredicatesPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    request: OwnedRowsQueryRequest,
) !bool {
    if (!try queryPredicatesPass(alloc, row, request.predicates)) return false;
    if (!try queryOrPredicateGroupsPass(alloc, row, request.or_predicates)) return false;
    if (!try queryNotPredicateGroupsPass(alloc, row, request.not_predicates)) return false;
    for (request.array_any) |predicate| {
        if (!try queryArrayAnyPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.array_contains) |predicate| {
        if (!try queryArrayContainsPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.array_eq) |predicate| {
        if (!try queryArrayEqPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.in_predicates) |predicate| {
        if (!try queryInPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.json_contains) |predicate| {
        if (!try queryJsonContainsPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.json_path_eq) |predicate| {
        if (!try queryJsonPathEqPredicatePasses(alloc, row, predicate)) return false;
    }
    for (request.json_path_exists) |predicate| {
        if (!queryJsonPathExistsPredicatePasses(row, predicate)) return false;
    }
    return true;
}

fn queryInPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsInPredicate,
) !bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return predicate.negated;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.values_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    if (wanted.value != .array) return error.InvalidRowsRequest;
    var found = false;
    for (wanted.value.array.items) |item| {
        if (jsonValuesEqual(actual.*, item)) {
            found = true;
            break;
        }
    }
    return if (predicate.negated) !found else found;
}

fn queryArrayAnyPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsArrayAnyPredicate,
) !bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return false;
    if (actual.* != .array) return false;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    for (actual.array.items) |item| {
        if (jsonValueContains(item, wanted.value) and jsonValueContains(wanted.value, item)) return true;
    }
    return false;
}

fn queryArrayContainsPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsArrayContainsPredicate,
) !bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return false;
    if (actual.* != .array) return false;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    if (wanted.value != .array) return error.InvalidRowsRequest;
    return jsonValueContains(actual.*, wanted.value);
}

fn queryArrayEqPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsArrayEqPredicate,
) !bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return false;
    if (actual.* != .array) return false;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    if (wanted.value != .array) return error.InvalidRowsRequest;
    return jsonValuesEqual(actual.*, wanted.value);
}

fn queryJsonContainsPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsJsonContainsPredicate,
) !bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return false;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    return jsonValueContains(actual.*, wanted.value);
}

fn queryJsonPathEqPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsJsonPathEqPredicate,
) !bool {
    const json_root = jsonValueAtPath(row, predicate.field) orelse return false;
    const actual = jsonValueAtPath(json_root.*, predicate.path) orelse return false;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    return jsonValuesEqual(actual.*, wanted.value);
}

fn queryJsonPathExistsPredicatePasses(
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsJsonPathExistsPredicate,
) bool {
    const json_root = jsonValueAtPath(row, predicate.field) orelse return false;
    return jsonValueAtPath(json_root.*, predicate.path) != null;
}

fn jsonValueContains(candidate: std.json.Value, wanted: std.json.Value) bool {
    return switch (wanted) {
        .object => |wanted_object| blk: {
            if (candidate != .object) break :blk false;
            var it = wanted_object.iterator();
            while (it.next()) |entry| {
                const candidate_value = candidate.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValueContains(candidate_value, entry.value_ptr.*)) break :blk false;
            }
            break :blk true;
        },
        .array => |wanted_array| blk: {
            if (candidate != .array) break :blk false;
            for (wanted_array.items) |wanted_item| {
                var found = false;
                for (candidate.array.items) |candidate_item| {
                    if (jsonValueContains(candidate_item, wanted_item)) {
                        found = true;
                        break;
                    }
                }
                if (!found) break :blk false;
            }
            break :blk true;
        },
        else => jsonScalarValuesEqual(candidate, wanted),
    };
}

fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    return switch (lhs) {
        .null => rhs == .null,
        .bool => |value| rhs == .bool and rhs.bool == value,
        .integer => |value| switch (rhs) {
            .integer => |other| other == value,
            .float => |other| @as(f64, @floatFromInt(value)) == other,
            else => false,
        },
        .float => |value| switch (rhs) {
            .integer => |other| value == @as(f64, @floatFromInt(other)),
            .float => |other| other == value,
            else => false,
        },
        .number_string => |value| switch (rhs) {
            .number_string => |other| std.mem.eql(u8, value, other),
            .string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .string => |value| switch (rhs) {
            .string => |other| std.mem.eql(u8, value, other),
            .number_string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .array => |array| blk: {
            if (rhs != .array or array.items.len != rhs.array.items.len) break :blk false;
            for (array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqual(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (rhs != .object or object.count() != rhs.object.count()) break :blk false;
            for (object.keys(), object.values()) |key, lhs_value| {
                const rhs_value = rhs.object.get(key) orelse break :blk false;
                if (!jsonValuesEqual(lhs_value, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn jsonScalarValuesEqual(candidate: std.json.Value, wanted: std.json.Value) bool {
    return switch (candidate) {
        .null => wanted == .null,
        .bool => |value| wanted == .bool and wanted.bool == value,
        .integer => |value| switch (wanted) {
            .integer => |other| value == other,
            .float => |other| @as(f64, @floatFromInt(value)) == other,
            else => false,
        },
        .float => |value| switch (wanted) {
            .integer => |other| value == @as(f64, @floatFromInt(other)),
            .float => |other| value == other,
            else => false,
        },
        .number_string => |value| switch (wanted) {
            .number_string => |other| std.mem.eql(u8, value, other),
            .string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .string => |value| switch (wanted) {
            .string => |other| std.mem.eql(u8, value, other),
            .number_string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        else => false,
    };
}

fn queryOrderKeysAlloc(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    order_by: []const RowsQueryOrder,
) ![]QueryOrderKey {
    if (order_by.len == 0) return &.{};
    const keys = try alloc.alloc(QueryOrderKey, order_by.len);
    var initialized: usize = 0;
    errdefer {
        freeQueryOrderKeys(alloc, keys[0..initialized]);
        alloc.free(keys);
    }
    for (order_by) |order| {
        keys[initialized] = try queryOrderKeyAlloc(alloc, row, order);
        initialized += 1;
    }
    return keys;
}

fn queryOrderKeyAlloc(alloc: std.mem.Allocator, row: std.json.Value, order: RowsQueryOrder) !QueryOrderKey {
    if (order.expression) |expression| {
        const value_json = try expressionValueJsonAlloc(alloc, row, expression);
        defer alloc.free(value_json);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        return try queryOrderKeyFromJsonValueAlloc(alloc, parsed.value, order.null_test);
    }
    if (order.field.len == 0) return error.InvalidRowsRequest;
    const value = jsonValueAtPath(row, order.field);
    return try queryOrderKeyFromJsonValueAlloc(alloc, if (value) |selected| selected.* else null, order.null_test);
}

fn queryOrderKeyFromJsonValueAlloc(alloc: std.mem.Allocator, maybe_value: ?std.json.Value, null_test: ?RowsQueryOrderNullTest) !QueryOrderKey {
    if (null_test) |test_value| {
        const is_null = maybe_value == null or maybe_value.? == .null;
        return .{ .bool = switch (test_value) {
            .is_null => is_null,
            .is_not_null => !is_null,
        } };
    }
    const selected = maybe_value orelse return .missing;
    return switch (selected) {
        .null => .null,
        .bool => |enabled| .{ .bool = enabled },
        .integer => |integer| .{ .number = @floatFromInt(integer) },
        .float => |float| .{ .number = float },
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        else => error.InvalidRowsRequest,
    };
}

fn queryCandidateLessThan(ctx: QuerySortContext, lhs: QueryCandidate, rhs: QueryCandidate) bool {
    for (ctx.order_by, 0..) |order, i| {
        const comparison = compareQueryOrderKeys(lhs.order_keys[i], rhs.order_keys[i]);
        if (comparison == .eq) continue;
        return switch (order.direction) {
            .asc => comparison == .lt,
            .desc => comparison == .gt,
        };
    }
    return lhs.ordinal < rhs.ordinal;
}

fn compareQueryOrderKeys(lhs: QueryOrderKey, rhs: QueryOrderKey) ScalarComparison {
    const left_rank = queryOrderKeyRank(lhs);
    const right_rank = queryOrderKeyRank(rhs);
    if (left_rank != right_rank) return if (left_rank < right_rank) .lt else .gt;
    return switch (lhs) {
        .missing, .null => .eq,
        .bool => |left| blk: {
            const right = rhs.bool;
            if (left == right) break :blk .eq;
            break :blk if (!left and right) .lt else .gt;
        },
        .number => |left| blk: {
            const right = rhs.number;
            if (left < right) break :blk .lt;
            if (left > right) break :blk .gt;
            break :blk .eq;
        },
        .string => |left| switch (std.mem.order(u8, left, rhs.string)) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        },
    };
}

fn queryOrderKeyRank(key: QueryOrderKey) u8 {
    return switch (key) {
        .bool => 0,
        .number => 1,
        .string => 2,
        .null => 3,
        .missing => 4,
    };
}

fn projectRowsQueryRowAlloc(
    alloc: std.mem.Allocator,
    request: OwnedRowsQueryRequest,
    row_json: []const u8,
) ![]u8 {
    if (request.select_all and request.json_extract.len == 0 and request.array_length.len == 0 and request.coalesce.len == 0 and request.field_aliases.len == 0 and request.expressions.len == 0) return try alloc.dupe(u8, row_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    if (request.select_all) {
        for (parsed.value.object.keys(), parsed.value.object.values()) |field, value| {
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(field, .{})});
            try std.json.Stringify.value(value, .{}, writer);
        }
    } else {
        for (request.select) |field| {
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(field, .{})});
            if (jsonValueAtPath(parsed.value, field)) |selected| {
                try std.json.Stringify.value(selected.*, .{}, writer);
            } else {
                try writer.writeAll("null");
            }
        }
    }
    for (request.json_extract) |projection| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        try writeJsonExtractProjectionValue(alloc, writer, parsed.value, projection);
    }
    for (request.array_length) |projection| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        try writeArrayLengthProjectionValue(writer, parsed.value, projection);
    }
    for (request.coalesce) |projection| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        try writeCoalesceProjectionValue(alloc, writer, parsed.value, projection);
    }
    for (request.field_aliases) |projection| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        if (jsonValueAtPath(parsed.value, projection.field)) |selected| {
            try std.json.Stringify.value(selected.*, .{}, writer);
        } else {
            try writer.writeAll("null");
        }
    }
    for (request.expressions) |projection| {
        if (queryProjectionOutputAlreadyRendered(request, projection.output)) continue;
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        const value_json = try expressionValueJsonAlloc(alloc, parsed.value, projection.expression);
        defer alloc.free(value_json);
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn queryProjectionOutputAlreadyRendered(request: OwnedRowsQueryRequest, output: []const u8) bool {
    for (request.select) |field| {
        if (std.mem.eql(u8, field, output)) return true;
    }
    for (request.json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (request.array_length) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (request.coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (request.field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    return false;
}

fn writeJsonExtractProjectionValue(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    row: std.json.Value,
    projection: db_mod.types.RelationalRowsJsonExtractProjection,
) !void {
    const json_root = jsonValueAtPath(row, projection.field) orelse {
        try writer.writeAll("null");
        return;
    };
    const selected = jsonValueAtPath(json_root.*, projection.path) orelse {
        try writer.writeAll("null");
        return;
    };
    if (!projection.as_text) {
        try std.json.Stringify.value(selected.*, .{}, writer);
        return;
    }
    try writeJsonExtractTextValue(alloc, writer, selected.*);
}

fn writeJsonExtractTextValue(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    value: std.json.Value,
) !void {
    switch (value) {
        .null => try writer.writeAll("null"),
        .string => |text| try writer.print("{f}", .{std.json.fmt(text, .{})}),
        else => {
            const text = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(text);
            try writer.print("{f}", .{std.json.fmt(text, .{})});
        },
    }
}

fn writeArrayLengthProjectionValue(
    writer: *std.Io.Writer,
    row: std.json.Value,
    projection: db_mod.types.RelationalRowsArrayLengthProjection,
) !void {
    const selected = jsonValueAtPath(row, projection.field) orelse {
        try writer.writeAll("null");
        return;
    };
    switch (selected.*) {
        .null => try writer.writeAll("null"),
        .array => |array| try writer.print("{d}", .{array.items.len}),
        else => return error.InvalidRowsRequest,
    }
}

fn writeCoalesceProjectionValue(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    row: std.json.Value,
    projection: db_mod.types.RelationalRowsCoalesceProjection,
) !void {
    for (projection.operands) |operand| {
        switch (operand.kind) {
            .field => {
                const selected = jsonValueAtPath(row, operand.field) orelse continue;
                if (selected.* == .null) continue;
                try std.json.Stringify.value(selected.*, .{}, writer);
                return;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, operand.value_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                try std.json.Stringify.value(parsed.value, .{}, writer);
                return;
            },
        }
    }
    try writer.writeAll("null");
}

fn expressionValueJsonAlloc(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror![]u8 {
    return switch (expression.kind) {
        .field => blk: {
            const selected = jsonValueAtPath(row, expression.field) orelse return try alloc.dupe(u8, "null");
            break :blk try std.json.Stringify.valueAlloc(alloc, selected.*, .{});
        },
        .value => try alloc.dupe(u8, expression.value_json),
        .coalesce => blk: {
            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonAlloc(alloc, row, operand);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch {
                    alloc.free(value_json);
                    return error.InvalidRowsRequest;
                };
                defer parsed.deinit();
                if (parsed.value == .null) {
                    alloc.free(value_json);
                    continue;
                }
                break :blk value_json;
            }
            break :blk try alloc.dupe(u8, "null");
        },
        .lower => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const lowered = try std.ascii.allocLowerString(alloc, text);
                    defer alloc.free(lowered);
                    break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = lowered }, .{});
                },
                else => return error.InvalidRowsRequest,
            }
        },
        .concat => blk: {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonAlloc(alloc, row, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                const text = try scalarJsonValueTextAlloc(alloc, parsed.value);
                defer alloc.free(text);
                try joined.appendSlice(alloc, text);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .nullif => blk: {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            const lhs_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            var lhs_transferred = false;
            errdefer if (!lhs_transferred) alloc.free(lhs_json);
            const rhs_json = try expressionValueJsonAlloc(alloc, row, expression.operands[1]);
            defer alloc.free(rhs_json);
            var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return error.InvalidRowsRequest;
            defer lhs.deinit();
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidRowsRequest;
            defer rhs.deinit();
            if (lhs.value != .null and rhs.value != .null and jsonValuesEqual(lhs.value, rhs.value)) {
                alloc.free(lhs_json);
                lhs_transferred = true;
                break :blk try alloc.dupe(u8, "null");
            }
            lhs_transferred = true;
            break :blk lhs_json;
        },
        .add, .sub, .mul, .div => blk: {
            if ((expression.kind == .add or expression.kind == .mul) and expression.operands.len < 2) return error.InvalidRowsRequest;
            if ((expression.kind == .sub or expression.kind == .div) and expression.operands.len != 2) return error.InvalidRowsRequest;
            const first_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            defer alloc.free(first_json);
            var first = std.json.parseFromSlice(std.json.Value, alloc, first_json, .{}) catch return error.InvalidRowsRequest;
            defer first.deinit();
            if (first.value == .null) break :blk try alloc.dupe(u8, "null");
            var result = try numericJsonValue(first.value);

            for (expression.operands[1..]) |operand| {
                const value_json = try expressionValueJsonAlloc(alloc, row, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
                const rhs = try numericJsonValue(parsed.value);
                result = switch (expression.kind) {
                    .add => result + rhs,
                    .sub => result - rhs,
                    .mul => result * rhs,
                    .div => if (rhs == 0) return error.InvalidRowsRequest else result / rhs,
                    else => unreachable,
                };
            }
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{result});
        },
        .cast => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const cast_type = expression.cast_type orelse return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            break :blk try castExpressionValueJsonAlloc(alloc, parsed.value, cast_type);
        },
        .json_extract => blk: {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidRowsRequest;
            const root_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            defer alloc.free(root_json);
            var root = std.json.parseFromSlice(std.json.Value, alloc, root_json, .{}) catch return error.InvalidRowsRequest;
            defer root.deinit();
            const selected = jsonValueAtPath(root.value, expression.json_path) orelse break :blk try alloc.dupe(u8, "null");
            if (!expression.json_as_text) break :blk try std.json.Stringify.valueAlloc(alloc, selected.*, .{});
            break :blk try jsonExtractTextValueJsonAlloc(alloc, selected.*);
        },
        .array_length => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonAlloc(alloc, row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .array => |array| break :blk try std.fmt.allocPrint(alloc, "{d}", .{array.items.len}),
                else => return error.InvalidRowsRequest,
            }
        },
        .case => blk: {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidRowsRequest;
            for (expression.case_branches) |branch| {
                if (try expressionConditionMatches(alloc, row, branch.when)) {
                    break :blk try expressionValueJsonAlloc(alloc, row, branch.then);
                }
            }
            break :blk try expressionValueJsonAlloc(alloc, row, expression.case_else[0]);
        },
    };
}

fn jsonExtractTextValueJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, "null"),
        .string => |text| try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = text }, .{}),
        else => blk: {
            const text = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(text);
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = text }, .{});
        },
    };
}

fn castExpressionValueJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    cast_type: db_mod.types.RelationalRowsExpressionCastType,
) ![]u8 {
    return switch (cast_type) {
        .text => blk: {
            const text = try scalarJsonValueTextAlloc(alloc, value);
            defer alloc.free(text);
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = text }, .{});
        },
        .numeric => blk: {
            const number = switch (value) {
                .integer, .float, .number_string => try numericJsonValue(value),
                .string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidRowsRequest,
                else => return error.InvalidRowsRequest,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{number});
        },
        .bool => blk: {
            const enabled = switch (value) {
                .bool => |enabled| enabled,
                .string => |text| if (std.mem.eql(u8, text, "true"))
                    true
                else if (std.mem.eql(u8, text, "false"))
                    false
                else
                    return error.InvalidRowsRequest,
                else => return error.InvalidRowsRequest,
            };
            break :blk try alloc.dupe(u8, if (enabled) "true" else "false");
        },
    };
}

fn expressionConditionMatches(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) anyerror!bool {
    const lhs_json = try expressionValueJsonAlloc(alloc, row, condition.lhs);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return error.InvalidRowsRequest;
    defer lhs.deinit();

    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .eq, .ne, .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return error.InvalidRowsRequest;
            const rhs_json = try expressionValueJsonAlloc(alloc, row, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidRowsRequest;
            defer rhs.deinit();
            const comparison = compareJsonScalars(lhs.value, rhs.value) orelse return error.InvalidRowsRequest;
            break :blk switch (condition.op) {
                .eq => comparison == .eq,
                .ne => comparison != .eq,
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                else => unreachable,
            };
        },
    };
}

fn numericJsonValue(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidRowsRequest,
        else => error.InvalidRowsRequest,
    };
}

fn freeQueryPredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.RelationalCheck) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value_json| alloc.free(value_json);
    }
}

fn freeRowsQueryPredicateGroups(alloc: std.mem.Allocator, groups: []const db_mod.types.RelationalRowsPredicateGroup) void {
    for (groups) |group| {
        freeQueryPredicates(alloc, group.predicates);
        if (group.predicates.len > 0) alloc.free(group.predicates);
    }
    if (groups.len > 0) alloc.free(groups);
}

fn freeRowsQueryArrayAnyPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayAnyPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryArrayContainsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryArrayEqPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryInPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsInPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.values_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonContainsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonPathEqPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
        alloc.free(predicate.value_json);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonPathExistsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonExtractProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsJsonExtractProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        alloc.free(projection.field);
        alloc.free(projection.path);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRowsQueryArrayLengthProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsArrayLengthProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        alloc.free(projection.field);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRowsQueryCoalesceProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsCoalesceProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        for (projection.operands) |operand| {
            switch (operand.kind) {
                .field => if (operand.field.len > 0) alloc.free(operand.field),
                .value => if (operand.value_json.len > 0) alloc.free(operand.value_json),
            }
        }
        if (projection.operands.len > 0) alloc.free(projection.operands);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRowsQueryExpression(alloc: std.mem.Allocator, expression: db_mod.types.RelationalRowsExpression) void {
    if (expression.field.len > 0) alloc.free(expression.field);
    if (expression.value_json.len > 0) alloc.free(expression.value_json);
    if (expression.json_path.len > 0) alloc.free(expression.json_path);
    for (expression.operands) |operand| freeRowsQueryExpression(alloc, operand);
    if (expression.operands.len > 0) alloc.free(expression.operands);
    for (expression.case_branches) |branch| freeRowsQueryExpressionCaseBranch(alloc, branch);
    if (expression.case_branches.len > 0) alloc.free(expression.case_branches);
    for (expression.case_else) |fallback| freeRowsQueryExpression(alloc, fallback);
    if (expression.case_else.len > 0) alloc.free(expression.case_else);
}

fn freeRowsQueryOrder(alloc: std.mem.Allocator, orders: []const RowsQueryOrder) void {
    for (orders) |order| {
        if (order.field.len > 0) alloc.free(order.field);
        if (order.expression) |expression| freeRowsQueryExpression(alloc, expression);
    }
}

fn freeRowsQueryExpressionCaseBranch(alloc: std.mem.Allocator, branch: db_mod.types.RelationalRowsExpressionCaseBranch) void {
    freeRowsQueryExpressionCondition(alloc, branch.when);
    freeRowsQueryExpression(alloc, branch.then);
}

fn freeRowsQueryExpressionCondition(alloc: std.mem.Allocator, condition: db_mod.types.RelationalRowsExpressionCondition) void {
    freeRowsQueryExpression(alloc, condition.lhs);
    for (condition.rhs) |rhs| freeRowsQueryExpression(alloc, rhs);
    if (condition.rhs.len > 0) alloc.free(condition.rhs);
}

fn freeRowsQueryExpressionConditions(alloc: std.mem.Allocator, conditions: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (conditions) |condition| freeRowsQueryExpressionCondition(alloc, condition);
    if (conditions.len > 0) alloc.free(conditions);
}

fn freeRowsAggregateSpecs(alloc: std.mem.Allocator, specs: []const db_mod.types.RelationalRowsAggregateSpec) void {
    for (specs) |spec| freeRowsAggregateSpec(alloc, spec);
    if (specs.len > 0) alloc.free(specs);
}

fn freeRowsAggregateSpec(alloc: std.mem.Allocator, spec: db_mod.types.RelationalRowsAggregateSpec) void {
    alloc.free(spec.name);
    if (spec.field) |field| alloc.free(field);
    if (spec.expression) |expression| freeRowsQueryExpression(alloc, expression);
    freeRowsQueryOrder(alloc, spec.array_order_by);
    if (spec.array_order_by.len > 0) alloc.free(spec.array_order_by);
    freeQueryPredicates(alloc, spec.filter_predicates);
    if (spec.filter_predicates.len > 0) alloc.free(spec.filter_predicates);
    freeRowsQueryExpressionConditions(alloc, spec.filter_expressions);
}

fn freeRowsWindowSpecs(alloc: std.mem.Allocator, specs: []const db_mod.types.RelationalRowsWindowSpec) void {
    for (specs) |spec| freeRowsWindowSpec(alloc, spec);
    if (specs.len > 0) alloc.free(specs);
}

fn freeRowsWindowSpec(alloc: std.mem.Allocator, spec: db_mod.types.RelationalRowsWindowSpec) void {
    alloc.free(spec.output);
    freeStringSlice(alloc, spec.partition_by);
    freeRowsQueryOrder(alloc, spec.order_by);
    if (spec.order_by.len > 0) alloc.free(spec.order_by);
}

fn freeRowsJoinOn(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJoinOn) void {
    for (predicates) |predicate| {
        alloc.free(predicate.left_field);
        alloc.free(predicate.right_field);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsJoinProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsJoinProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        alloc.free(projection.field);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRowsLateralCorrelations(alloc: std.mem.Allocator, correlations: []const db_mod.types.RelationalRowsLateralCorrelation) void {
    for (correlations) |correlation| {
        alloc.free(correlation.left_field);
        alloc.free(correlation.right_field);
    }
    if (correlations.len > 0) alloc.free(correlations);
}

fn freeRowsCtes(alloc: std.mem.Allocator, ctes: []const db_mod.types.RelationalRowsCte) void {
    for (ctes) |cte| {
        var owned = cte;
        owned.deinit(alloc);
    }
    if (ctes.len > 0) alloc.free(ctes);
}

fn freeRowsQueryExpressionProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsExpressionProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        freeRowsQueryExpression(alloc, projection.expression);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeRowsQueryFieldAliasProjections(alloc: std.mem.Allocator, projections: []const db_mod.types.RelationalRowsFieldAliasProjection) void {
    for (projections) |projection| {
        alloc.free(projection.output);
        alloc.free(projection.field);
    }
    if (projections.len > 0) alloc.free(projections);
}

fn freeQueryOrderKeys(alloc: std.mem.Allocator, keys: []const QueryOrderKey) void {
    for (keys) |key| switch (key) {
        .string => |text| alloc.free(text),
        else => {},
    };
}

fn freeQueryOrderKeySlice(alloc: std.mem.Allocator, keys: []const QueryOrderKey) void {
    freeQueryOrderKeys(alloc, keys);
    if (keys.len > 0) alloc.free(keys);
}

const ConflictAction = enum {
    update,
    nothing,
};

fn plannedRelationalRowJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
) ![]u8 {
    return try plannedRelationalRowJsonWithOptionsAlloc(alloc, schema, row_value, true);
}

fn plannedExistingRelationalRowJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    return try plannedRelationalRowJsonWithOptionsAlloc(alloc, schema, parsed.value, false);
}

fn plannedRelationalRowJsonWithOptionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    reject_generated_input: bool,
) ![]u8 {
    if (row_value != .object) return error.InvalidRowsRequest;
    const resolved_defaults = try alloc.alloc(?[]u8, schema.relational_columns.len);
    defer {
        for (resolved_defaults) |value| if (value) |owned| alloc.free(owned);
        alloc.free(resolved_defaults);
    }
    @memset(resolved_defaults, null);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;

    var it = row_value.object.iterator();
    while (it.next()) |entry| {
        const column = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        if (column.generated != null) {
            if (reject_generated_input) return error.InvalidRowsRequest;
            continue;
        }
        try appendJsonFieldValue(alloc, writer, &first, entry.key_ptr.*, entry.value_ptr.*);
    }

    for (schema.relational_columns, 0..) |column, column_index| {
        if (row_value.object.get(column.path) != null) continue;
        if (column.default_value) |default_value| {
            const value_json = try relationalDefaultValueJsonAlloc(alloc, default_value);
            resolved_defaults[column_index] = value_json;
            try appendRawJsonFieldValue(alloc, writer, &first, column.path, value_json);
        }
    }

    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        const value_json = try generatedColumnValueJsonAlloc(alloc, schema, row_value, resolved_defaults, generated);
        defer alloc.free(value_json);
        try appendRawJsonFieldValue(alloc, writer, &first, column.path, value_json);
    }

    try writer.writeByte('}');
    const planned = try out.toOwnedSlice();
    errdefer alloc.free(planned);
    try validateRelationalChecks(alloc, schema, planned);
    return planned;
}

pub fn relationalDefaultValueJsonAlloc(alloc: std.mem.Allocator, default_value: runtime_schema.RelationalDefaultValue) ![]u8 {
    return switch (default_value.kind) {
        .literal => try alloc.dupe(u8, default_value.value_json),
        .now_ns => try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
        .uuid_v4 => blk: {
            const uuid = try randomUuidV4String();
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(uuid[0..], .{})});
        },
    };
}

fn randomUuidV4String() ![36]u8 {
    var bytes: [16]u8 = undefined;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try io_impl.io().randomSecure(&bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    var out: [36]u8 = undefined;
    const hex = "0123456789abcdef";
    var src: usize = 0;
    var dst: usize = 0;
    while (src < bytes.len) : (src += 1) {
        if (dst == 8 or dst == 13 or dst == 18 or dst == 23) {
            out[dst] = '-';
            dst += 1;
        }
        out[dst] = hex[bytes[src] >> 4];
        out[dst + 1] = hex[bytes[src] & 0x0f];
        dst += 2;
    }
    return out;
}

fn schemaHasGeneratedColumns(schema: runtime_schema.TableSchema) bool {
    for (schema.relational_columns) |column| {
        if (column.generated != null) return true;
    }
    return false;
}

fn extendOperationsWithOnUpdateAlloc(
    alloc: std.mem.Allocator,
    operations: []db_mod.types.TransformOp,
    schema: runtime_schema.TableSchema,
) ![]db_mod.types.TransformOp {
    var update_count: usize = 0;
    for (schema.relational_columns) |column| {
        if (column.on_update_value != null) update_count += 1;
    }
    if (update_count == 0) return operations;

    const extended = try alloc.alloc(db_mod.types.TransformOp, operations.len + update_count);
    @memcpy(extended[0..operations.len], operations);
    alloc.free(operations);
    var index = operations.len;
    errdefer freeTransformOps(alloc, extended[operations.len..index]);

    for (schema.relational_columns) |column| {
        const on_update_value = column.on_update_value orelse continue;
        const value_json = try relationalDefaultValueJsonAlloc(alloc, on_update_value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, column.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        extended[index] = .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        };
        path_transferred = true;
        value_transferred = true;
        index += 1;
    }
    return extended;
}

fn appendJsonFieldValue(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    first: *bool,
    field: []const u8,
    value: std.json.Value,
) !void {
    if (!first.*) try writer.writeByte(',');
    first.* = false;
    try writer.print("{f}:", .{std.json.fmt(field, .{})});
    _ = alloc;
    try std.json.Stringify.value(value, .{}, writer);
}

fn appendRawJsonFieldValue(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    first: *bool,
    field: []const u8,
    value_json: []const u8,
) !void {
    if (!first.*) try writer.writeByte(',');
    first.* = false;
    try writer.print("{f}:", .{std.json.fmt(field, .{})});
    _ = alloc;
    try writer.writeAll(value_json);
}

fn generatedColumnValueJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    resolved_defaults: []const ?[]u8,
    generated: runtime_schema.RelationalGeneratedValue,
) ![]u8 {
    return switch (generated.op) {
        .lower => blk: {
            const field = generated.field orelse return error.InvalidRowsRequest;
            const source = try plannedStringFieldValueAlloc(alloc, schema, row_value, resolved_defaults, field);
            defer alloc.free(source);
            const lowered = try std.ascii.allocLowerString(alloc, source);
            defer alloc.free(lowered);
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(lowered, .{})});
        },
        .concat => blk: {
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (generated.fields, 0..) |field, i| {
                if (i != 0) try joined.appendSlice(alloc, generated.separator);
                const value = try plannedScalarFieldTextAlloc(alloc, schema, row_value, resolved_defaults, field);
                defer alloc.free(value);
                try joined.appendSlice(alloc, value);
            }
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(joined.items, .{})});
        },
    };
}

fn plannedStringFieldValueAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    resolved_defaults: []const ?[]u8,
    field: []const u8,
) ![]u8 {
    if (row_value.object.get(field)) |value| {
        if (value != .string) return error.InvalidRowsRequest;
        return try alloc.dupe(u8, value.string);
    }
    const column_index = findRelationalColumnIndex(schema.relational_columns, field) orelse return error.InvalidRowsRequest;
    const default_json = resolved_defaults[column_index] orelse return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, default_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .string) return error.InvalidRowsRequest;
    return try alloc.dupe(u8, parsed.value.string);
}

fn plannedScalarFieldTextAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    resolved_defaults: []const ?[]u8,
    field: []const u8,
) ![]u8 {
    if (row_value.object.get(field)) |value| return try scalarJsonValueTextAlloc(alloc, value);
    const column_index = findRelationalColumnIndex(schema.relational_columns, field) orelse return error.InvalidRowsRequest;
    const default_json = resolved_defaults[column_index] orelse return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, default_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    return try scalarJsonValueTextAlloc(alloc, parsed.value);
}

fn scalarJsonValueTextAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |integer| try std.fmt.allocPrint(alloc, "{d}", .{integer}),
        .float => |float| try std.fmt.allocPrint(alloc, "{d}", .{float}),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.InvalidRowsRequest,
    };
}

fn validateRelationalChecks(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
) !void {
    if (schema.checks.len == 0) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    for (schema.checks) |check| {
        if (!try relationalCheckPasses(alloc, parsed.value, check)) return error.InvalidRowsRequest;
    }
}

fn extendOperationsWithGeneratedColumnsAlloc(
    alloc: std.mem.Allocator,
    operations: []db_mod.types.TransformOp,
    schema: runtime_schema.TableSchema,
    planned_json: []const u8,
) ![]db_mod.types.TransformOp {
    var generated_count: usize = 0;
    for (schema.relational_columns) |column| {
        if (column.generated != null) generated_count += 1;
    }
    if (generated_count == 0) return operations;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, planned_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    const extended = try alloc.alloc(db_mod.types.TransformOp, operations.len + generated_count);
    @memcpy(extended[0..operations.len], operations);
    alloc.free(operations);
    var index = operations.len;
    errdefer freeTransformOps(alloc, extended[operations.len..index]);

    for (schema.relational_columns) |column| {
        if (column.generated == null) continue;
        const value = jsonValueAtPath(parsed.value, column.path) orelse return error.InvalidRowsRequest;
        const value_json = try jsonValueStringifyAlloc(alloc, value.*);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, column.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        extended[index] = .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        };
        path_transferred = true;
        value_transferred = true;
        index += 1;
    }
    return extended;
}

fn relationalCheckPasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    check: runtime_schema.RelationalCheck,
) !bool {
    const value = jsonValueAtPath(row, check.field);
    return switch (check.op) {
        .is_null => value == null or value.?.* == .null,
        .is_not_null => value != null and value.?.* != .null,
        .eq, .ne, .gt, .gte, .lt, .lte => blk: {
            const expected_json = check.value_json orelse return error.InvalidRowsRequest;
            var expected = std.json.parseFromSlice(std.json.Value, alloc, expected_json, .{}) catch return error.InvalidRowsRequest;
            defer expected.deinit();
            const actual = value orelse break :blk false;
            const comparison = compareJsonScalars(actual.*, expected.value) orelse return error.InvalidRowsRequest;
            break :blk switch (check.op) {
                .eq => comparison == .eq,
                .ne => comparison != .eq,
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                else => unreachable,
            };
        },
    };
}

const ScalarComparison = enum { lt, eq, gt };

fn compareJsonScalars(actual: std.json.Value, expected: std.json.Value) ?ScalarComparison {
    if (jsonNumericValue(actual)) |left| {
        const right = jsonNumericValue(expected) orelse return null;
        if (left < right) return .lt;
        if (left > right) return .gt;
        return .eq;
    }
    if (actual == .string and expected == .string) {
        const order = std.mem.order(u8, actual.string, expected.string);
        return switch (order) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    if (actual == .bool and expected == .bool) {
        if (actual.bool == expected.bool) return .eq;
        return if (!actual.bool and expected.bool) .lt else .gt;
    }
    if (actual == .null and expected == .null) return .eq;
    return null;
}

fn jsonNumericValue(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        else => null,
    };
}

fn appendInsertAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    require_absent: bool,
    writes: *std.ArrayListUnmanaged(db_mod.types.BatchWrite),
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
) !void {
    const row_json = try plannedRelationalRowJsonAlloc(alloc, schema, row_value);
    var row_json_transferred = false;
    errdefer if (!row_json_transferred) alloc.free(row_json);
    const key = try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
    var key_transferred = false;
    errdefer if (!key_transferred) alloc.free(key);
    try writes.append(alloc, .{ .key = key, .value = row_json });
    key_transferred = true;
    row_json_transferred = true;
    if (require_absent) {
        const predicate_key = try alloc.dupe(u8, key);
        var predicate_key_transferred = false;
        errdefer if (!predicate_key_transferred) alloc.free(predicate_key);
        try predicates.append(alloc, .{ .key = predicate_key, .expected_version = 0 });
        predicate_key_transferred = true;
    }
}

fn appendInsertWithConflictAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    row_value: std.json.Value,
    op_value: std.json.Value,
    conflict_value: std.json.Value,
    resolver: UniqueSelectorResolver,
    writes: *std.ArrayListUnmanaged(db_mod.types.BatchWrite),
    transforms: *std.ArrayListUnmanaged(db_mod.types.DocumentTransform),
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    returning_rows: *std.ArrayListUnmanaged([]const u8),
    inserted: *u32,
    transformed: *u32,
) !void {
    if (conflict_value != .object) return error.InvalidRowsRequest;
    const action = try parseConflictAction(conflict_value.object.get("action") orelse return error.InvalidRowsRequest);
    const target_value = conflict_value.object.get("target") orelse return error.InvalidRowsRequest;
    if (target_value != .object) return error.InvalidRowsRequest;

    const row_json = try plannedRelationalRowJsonAlloc(alloc, schema, row_value);
    defer alloc.free(row_json);

    const conflict_key = try conflictTargetPrimaryKeyAlloc(alloc, table_name, schema, row_json, target_value, resolver);
    defer if (conflict_key) |key| alloc.free(key);

    if (conflict_key) |key| {
        switch (action) {
            .nothing => return,
            .update => {
                var operations = try updateTransformOperationsAlloc(alloc, schema, conflict_value);
                var operations_transferred = false;
                errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
                operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);
                if (hasReturningProjection(op_value) or schemaHasGeneratedColumns(schema) or schema.checks.len != 0) {
                    const existing = (try resolver.lookupPrimary(alloc, table_name, key)) orelse return error.RowSelectorNotFound;
                    var existing_mut = existing;
                    defer existing_mut.deinit(alloc);
                    try appendVersionPredicateAlloc(alloc, predicates, key, existing.version);
                    const projected_json = (try db_mod.transform.resolveDocumentTransform(alloc, existing.json, .{ .key = key, .operations = operations })) orelse return error.RowSelectorNotFound;
                    defer alloc.free(projected_json);
                    const planned_json = try plannedExistingRelationalRowJsonAlloc(alloc, schema, projected_json);
                    defer alloc.free(planned_json);
                    if (schemaHasGeneratedColumns(schema)) operations = try extendOperationsWithGeneratedColumnsAlloc(alloc, operations, schema, planned_json);
                    try appendReturningProjectionFromJsonAlloc(alloc, schema, returning_rows, op_value, planned_json);
                }
                const transform_key = try alloc.dupe(u8, key);
                var key_transferred = false;
                errdefer if (!key_transferred) alloc.free(transform_key);
                try transforms.append(alloc, .{ .key = transform_key, .operations = operations });
                key_transferred = true;
                operations_transferred = true;
                transformed.* += 1;
                return;
            },
        }
    }

    try appendInsertAlloc(alloc, schema, row_value, true, writes, predicates);
    try appendReturningProjectionAlloc(alloc, schema, returning_rows, op_value, row_json);
    inserted.* += 1;
}

fn parseConflictAction(value: std.json.Value) !ConflictAction {
    if (value != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, value.string, "update")) return .update;
    if (std.mem.eql(u8, value.string, "nothing")) return .nothing;
    return error.InvalidRowsRequest;
}

fn conflictTargetPrimaryKeyAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
    target_value: std.json.Value,
    resolver: UniqueSelectorResolver,
) !?[]u8 {
    if (target_value.object.get("primary")) |primary_value| {
        if (primary_value != .bool or !primary_value.bool) return error.InvalidRowsRequest;
        const key = try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
        errdefer alloc.free(key);
        if (try resolver.primaryExists(alloc, table_name, key)) return key;
        alloc.free(key);
        return null;
    }

    if (target_value.object.get("unique")) |unique_value| {
        if (unique_value != .object) return error.InvalidRowsRequest;
        const name_value = unique_value.object.get("name") orelse return error.InvalidRowsRequest;
        if (name_value != .string) return error.InvalidRowsRequest;
        const constraint = findUniqueConstraint(schema.unique_constraints, name_value.string) orelse return error.InvalidRowsRequest;
        try validateConflictTargetPredicateMatchesConstraint(alloc, unique_value, constraint);
        const encoded_value = (try uniqueConstraintValueFromRowAlloc(alloc, schema, constraint, row_json)) orelse return null;
        defer alloc.free(encoded_value);
        return try resolver.resolveUnique(alloc, table_name, constraint.name, encoded_value);
    }

    return error.InvalidRowsRequest;
}

fn validateConflictTargetPredicateMatchesConstraint(
    alloc: std.mem.Allocator,
    unique_value: std.json.Value,
    constraint: runtime_schema.UniqueConstraint,
) !void {
    const where_value = unique_value.object.get("where");
    if (constraint.where.len == 0) {
        if (where_value != null) return error.InvalidRowsRequest;
        return;
    }
    const supplied = where_value orelse return error.InvalidRowsRequest;
    try validateUniquePredicateJsonMatches(alloc, supplied, constraint.where);
}

fn validateUniquePredicateJsonMatches(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    predicates: []const runtime_schema.UniquePredicate,
) !void {
    if (value != .object) return error.InvalidRowsRequest;
    const all_value = value.object.get("all") orelse return error.InvalidRowsRequest;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.InvalidRowsRequest;
    for (all_value.array.items, predicates) |item, predicate| {
        try validateUniquePredicateAtomJsonMatches(alloc, item, predicate);
    }
}

fn validateUniquePredicateAtomJsonMatches(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    predicate: runtime_schema.UniquePredicate,
) !void {
    if (value != .object) return error.InvalidRowsRequest;
    const field_value = value.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = value.object.get("op") orelse return error.InvalidRowsRequest;
    if (field_value != .string or !std.mem.eql(u8, field_value.string, predicate.field)) return error.InvalidRowsRequest;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, uniquePredicateOpToken(predicate.op))) return error.InvalidRowsRequest;

    const supplied_value = value.object.get("value");
    if (predicate.value_json) |expected_json| {
        const supplied = supplied_value orelse return error.InvalidRowsRequest;
        const supplied_json = try jsonValueStringifyAlloc(alloc, supplied);
        defer alloc.free(supplied_json);
        if (!std.mem.eql(u8, supplied_json, expected_json)) return error.InvalidRowsRequest;
    } else if (supplied_value != null) {
        return error.InvalidRowsRequest;
    }
}

fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

pub fn physicalPrimaryKeyFromWhereAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    where_value: std.json.Value,
    unique_resolver: ?UniqueSelectorResolver,
    missing_unique_ok: bool,
) !?[]u8 {
    if (where_value != .object) return error.InvalidRowsRequest;
    const primary_value = where_value.object.get("primary") orelse {
        if (where_value.object.get("unique")) |unique_value| {
            const resolver = unique_resolver orelse return error.UnsupportedRowsSelector;
            if (unique_value != .object) return error.InvalidRowsRequest;
            const name_value = unique_value.object.get("name") orelse return error.InvalidRowsRequest;
            if (name_value != .string) return error.InvalidRowsRequest;
            const values_value = unique_value.object.get("values") orelse return error.InvalidRowsRequest;
            if (values_value != .object) return error.InvalidRowsRequest;
            const constraint = findUniqueConstraint(schema.unique_constraints, name_value.string) orelse return error.InvalidRowsRequest;
            const encoded_value = try uniqueConstraintValueFromValuesAlloc(alloc, schema, constraint, values_value);
            defer alloc.free(encoded_value);
            return (try resolver.resolveUnique(alloc, table_name, constraint.name, encoded_value)) orelse {
                if (missing_unique_ok) return null;
                return error.RowSelectorNotFound;
            };
        }
        return error.InvalidRowsRequest;
    };
    if (primary_value != .object) return error.InvalidRowsRequest;
    return try physicalPrimaryKeyFromPrimaryValuesAlloc(alloc, schema, primary_value);
}

pub fn physicalPrimaryKeyFromRowJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
) ![]u8 {
    const primary_key = schema.primary_key orelse return error.InvalidRowsRequest;
    const selected_columns = try primaryKeyColumnsAlloc(alloc, schema, primary_key);
    defer alloc.free(selected_columns);
    const row_value = db_mod.document_mapper.buildRelationalRowValueAlloc(alloc, row_json, selected_columns) catch return error.InvalidRowsRequest;
    defer alloc.free(row_value);
    const tuple = db_mod.relational_store.primaryKeyTupleValueAlloc(alloc, row_value, primary_key) catch return error.InvalidRowsRequest;
    defer alloc.free(tuple);
    return try physicalPrimaryKeyFromTupleAlloc(alloc, tuple);
}

fn physicalPrimaryKeyFromPrimaryValuesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    primary_value: std.json.Value,
) ![]u8 {
    const primary_json = try jsonValueStringifyAlloc(alloc, primary_value);
    defer alloc.free(primary_json);
    return try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, primary_json);
}

fn physicalPrimaryKeyFromTupleAlloc(alloc: std.mem.Allocator, tuple: []const u8) ![]u8 {
    const encoded_len = std.base64.url_safe_no_pad.Encoder.calcSize(tuple.len);
    const out = try alloc.alloc(u8, physical_primary_key_prefix.len + encoded_len);
    @memcpy(out[0..physical_primary_key_prefix.len], physical_primary_key_prefix);
    _ = std.base64.url_safe_no_pad.Encoder.encode(out[physical_primary_key_prefix.len..], tuple);
    return out;
}

fn primaryKeyColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    primary_key: runtime_schema.PrimaryKey,
) ![]runtime_schema.RelationalColumn {
    var selected = try alloc.alloc(runtime_schema.RelationalColumn, primary_key.columns.len);
    errdefer alloc.free(selected);
    for (primary_key.columns, 0..) |column_name, i| {
        selected[i] = findRelationalColumn(schema.relational_columns, column_name) orelse return error.InvalidRowsRequest;
    }
    return selected;
}

fn uniqueConstraintColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    constraint: runtime_schema.UniqueConstraint,
) ![]runtime_schema.RelationalColumn {
    if (constraint.where.len != 0 or constraint.expressions.len != 0) return error.InvalidRowsRequest;
    var selected = try alloc.alloc(runtime_schema.RelationalColumn, constraint.columns.len);
    errdefer alloc.free(selected);
    for (constraint.columns, 0..) |column_name, i| {
        selected[i] = findRelationalColumn(schema.relational_columns, column_name) orelse return error.InvalidRowsRequest;
    }
    return selected;
}

fn findUniqueConstraint(constraints: []const runtime_schema.UniqueConstraint, name: []const u8) ?runtime_schema.UniqueConstraint {
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn findRelationalColumn(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?runtime_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, name) or std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn findRelationalColumnIndex(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?usize {
    for (columns, 0..) |column, i| {
        if (std.mem.eql(u8, column.path, name) or std.mem.eql(u8, column.name, name)) return i;
    }
    return null;
}

fn uniqueConstraintValueFromValuesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    constraint: runtime_schema.UniqueConstraint,
    values: std.json.Value,
) ![]u8 {
    const selected_columns = try uniqueConstraintColumnsAlloc(alloc, schema, constraint);
    defer alloc.free(selected_columns);
    const values_json = try jsonValueStringifyAlloc(alloc, values);
    defer alloc.free(values_json);
    const row_value = db_mod.document_mapper.buildRelationalRowValueAlloc(alloc, values_json, selected_columns) catch return error.InvalidRowsRequest;
    defer alloc.free(row_value);
    const tuple = (db_mod.relational_store.uniqueConstraintTupleValueAlloc(alloc, row_value, constraint) catch return error.InvalidRowsRequest) orelse return error.InvalidRowsRequest;
    return tuple;
}

fn uniqueConstraintValueFromRowAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    constraint: runtime_schema.UniqueConstraint,
    row_json: []const u8,
) !?[]u8 {
    const row_value = db_mod.document_mapper.buildRelationalRowValueAlloc(alloc, row_json, schema.relational_columns) catch return error.InvalidRowsRequest;
    defer alloc.free(row_value);
    return (db_mod.relational_store.uniqueConstraintTupleValueAlloc(alloc, row_value, constraint) catch return error.InvalidRowsRequest);
}

fn updateTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    op_value: std.json.Value,
) ![]db_mod.types.TransformOp {
    if (op_value != .object) return error.InvalidRowsRequest;
    var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer freeTransformOps(alloc, operations.items);

    var saw_mutation = false;
    if (op_value.object.get("patch")) |patch| {
        saw_mutation = true;
        if (patch != .object) return error.InvalidRowsRequest;
        try appendPatchTransformOperationsAlloc(alloc, schema.primary_key.?, patch, &operations);
    }
    if (op_value.object.get("increment")) |increment| {
        saw_mutation = true;
        if (increment != .object) return error.InvalidRowsRequest;
        try appendIncrementTransformOperationsAlloc(alloc, schema, increment, &operations);
    }
    if (op_value.object.get("json_set")) |json_set| {
        saw_mutation = true;
        try appendJsonSetTransformOperationsAlloc(alloc, schema, json_set, &operations);
    }
    if (op_value.object.get("array_update")) |array_update| {
        saw_mutation = true;
        try appendArrayUpdateTransformOperationsAlloc(alloc, schema, array_update, &operations);
    }
    if (!saw_mutation) return error.InvalidRowsRequest;

    return try operations.toOwnedSlice(alloc);
}

fn appendPatchTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    primary_key: runtime_schema.PrimaryKey,
    patch: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    var it = patch.object.iterator();
    while (it.next()) |entry| {
        if (primaryKeyContains(primary_key, entry.key_ptr.*)) return error.InvalidRowsRequest;
        const value_json = try jsonValueStringifyAlloc(alloc, entry.value_ptr.*);
        var value_json_transferred = false;
        errdefer if (!value_json_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, entry.key_ptr.*);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        });
        path_transferred = true;
        value_json_transferred = true;
    }
}

fn appendIncrementTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    increment: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    var it = increment.object.iterator();
    while (it.next()) |entry| {
        const column = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        if (column.field_type != .numeric) return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, entry.key_ptr.*)) return error.InvalidRowsRequest;
        }
        switch (entry.value_ptr.*) {
            .integer, .float, .number_string => {},
            else => return error.InvalidRowsRequest,
        }
        const value_json = try jsonValueStringifyAlloc(alloc, entry.value_ptr.*);
        var value_json_transferred = false;
        errdefer if (!value_json_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, column.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{
            .op = .inc,
            .path = path,
            .value_json = value_json,
        });
        path_transferred = true;
        value_json_transferred = true;
    }
}

fn appendJsonSetTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    json_set: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    if (json_set != .array) return error.InvalidRowsRequest;
    for (json_set.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .json) return error.InvalidRowsRequest;
        const path_value = item.object.get("path") orelse return error.InvalidRowsRequest;
        if (path_value != .array or path_value.array.items.len == 0) return error.InvalidRowsRequest;
        const value = item.object.get("value") orelse return error.InvalidRowsRequest;
        const transform_path = try jsonSetTransformPathAlloc(alloc, column.path, path_value);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(transform_path);
        const value_json = try jsonValueStringifyAlloc(alloc, value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try operations.append(alloc, .{
            .op = .set,
            .path = transform_path,
            .value_json = value_json,
        });
        path_transferred = true;
        value_transferred = true;
    }
}

fn appendArrayUpdateTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    array_update: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    if (array_update != .array) return error.InvalidRowsRequest;
    for (array_update.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .array) return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, field_value.string)) return error.InvalidRowsRequest;
        }
        const op_value = item.object.get("op") orelse return error.InvalidRowsRequest;
        if (op_value != .string) return error.InvalidRowsRequest;
        const op: db_mod.types.TransformOpType = if (std.mem.eql(u8, op_value.string, "append"))
            .push
        else if (std.mem.eql(u8, op_value.string, "remove"))
            .pull
        else if (std.mem.eql(u8, op_value.string, "add_to_set"))
            .add_to_set
        else
            return error.InvalidRowsRequest;
        const value = item.object.get("value") orelse return error.InvalidRowsRequest;
        const value_json = try jsonValueStringifyAlloc(alloc, value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, column.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{
            .op = op,
            .path = path,
            .value_json = value_json,
        });
        path_transferred = true;
        value_transferred = true;
    }
}

fn jsonSetTransformPathAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    path_value: std.json.Value,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll(field);
    for (path_value.array.items) |segment| {
        if (segment != .string or segment.string.len == 0) return error.InvalidRowsRequest;
        if (std.mem.indexOfScalar(u8, segment.string, '.') != null) return error.InvalidRowsRequest;
        try writer.writeByte('.');
        try writer.writeAll(segment.string);
    }
    return try out.toOwnedSlice();
}

fn primaryKeyContains(primary_key: runtime_schema.PrimaryKey, column: []const u8) bool {
    for (primary_key.columns) |component| {
        if (std.mem.eql(u8, component, column)) return true;
    }
    return false;
}

fn appendExpectedVersionPredicateAlloc(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    op_value: std.json.Value,
    key: []const u8,
) !void {
    const expected_version_value = op_value.object.get("expected_version") orelse return;
    const expected_version = try parseExpectedVersion(expected_version_value);
    const predicate_key = try alloc.dupe(u8, key);
    var predicate_key_transferred = false;
    errdefer if (!predicate_key_transferred) alloc.free(predicate_key);
    try predicates.append(alloc, .{
        .key = predicate_key,
        .expected_version = expected_version,
    });
    predicate_key_transferred = true;
}

fn appendVersionPredicateAlloc(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    key: []const u8,
    expected_version: u64,
) !void {
    const predicate_key = try alloc.dupe(u8, key);
    var predicate_key_transferred = false;
    errdefer if (!predicate_key_transferred) alloc.free(predicate_key);
    try predicates.append(alloc, .{
        .key = predicate_key,
        .expected_version = expected_version,
    });
    predicate_key_transferred = true;
}

fn returningBaseRowForKey(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    key: []const u8,
    unique_resolver: ?UniqueSelectorResolver,
    op_value: std.json.Value,
) !?ResolvedPrimaryRow {
    if (!hasReturningProjection(op_value)) return null;
    const resolver = unique_resolver orelse return error.UnsupportedRowsSelector;
    return (try resolver.lookupPrimary(alloc, table_name, key)) orelse return error.RowSelectorNotFound;
}

fn lookupBaseRowForKey(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    key: []const u8,
    unique_resolver: ?UniqueSelectorResolver,
) !ResolvedPrimaryRow {
    const resolver = unique_resolver orelse return error.UnsupportedRowsSelector;
    return (try resolver.lookupPrimary(alloc, table_name, key)) orelse return error.RowSelectorNotFound;
}

fn appendReturningProjectionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    returning_rows: *std.ArrayListUnmanaged([]const u8),
    op_value: std.json.Value,
    row_json: []const u8,
) !void {
    if (!hasReturningProjection(op_value)) return;
    try appendReturningProjectionFromJsonAlloc(alloc, schema, returning_rows, op_value, row_json);
}

fn appendReturningProjectionFromJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    returning_rows: *std.ArrayListUnmanaged([]const u8),
    op_value: std.json.Value,
    row_json: []const u8,
) !void {
    if (!hasReturningProjection(op_value)) return;
    const projected = try projectReturningRowAlloc(alloc, schema, op_value, row_json);
    var projected_transferred = false;
    errdefer if (!projected_transferred) alloc.free(projected);
    try returning_rows.append(alloc, projected);
    projected_transferred = true;
}

fn hasReturningProjection(op_value: std.json.Value) bool {
    if (op_value != .object) return false;
    return op_value.object.get("returning") != null or op_value.object.get("returning_expressions") != null;
}

fn projectReturningRowAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    op_value: std.json.Value,
    row_json: []const u8,
) ![]u8 {
    const returning_value = op_value.object.get("returning");
    const returning_expressions_value = op_value.object.get("returning_expressions");
    if (returning_value == null and returning_expressions_value == null) return error.InvalidRowsRequest;
    if (returning_value) |value| if (value != .array) return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    const expressions = try parseRowsQueryExpressionProjectionsAlloc(alloc, schema, returning_expressions_value);
    defer freeRowsQueryExpressionProjections(alloc, expressions);

    if (returning_value) |fields| {
        if (fields.array.items.len == 1 and fields.array.items[0] == .string and std.mem.eql(u8, fields.array.items[0].string, "*")) {
            if (expressions.len != 0) return error.InvalidRowsRequest;
            return try alloc.dupe(u8, row_json);
        }
    }

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    if (returning_value) |fields| {
        for (fields.array.items) |field_value| {
            if (field_value != .string) return error.InvalidRowsRequest;
            if (field_value.string.len == 0 or std.mem.eql(u8, field_value.string, "*")) return error.InvalidRowsRequest;
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(field_value.string, .{})});
            if (jsonValueAtPath(parsed.value, field_value.string)) |selected| {
                try std.json.Stringify.value(selected.*, .{}, writer);
            } else {
                try writer.writeAll("null");
            }
        }
    }
    for (expressions) |projection| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        const value_json = try expressionValueJsonAlloc(alloc, parsed.value, projection.expression);
        defer alloc.free(value_json);
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn jsonValueAtPath(root: std.json.Value, path: []const u8) ?*const std.json.Value {
    if (root != .object) return null;
    var current: *const std.json.Value = &root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current.* != .object) return null;
        current = current.object.getPtr(part) orelse return null;
    }
    return current;
}

fn parseExpectedVersion(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @intCast(integer) else error.InvalidRowsRequest,
        .string => |text| std.fmt.parseUnsigned(u64, text, 10) catch return error.InvalidRowsRequest,
        else => error.InvalidRowsRequest,
    };
}

fn identityResponseJsonAlloc(alloc: std.mem.Allocator, selector: std.json.Value) ![]u8 {
    if (selector != .object) return error.InvalidRowsRequest;
    if (selector.object.get("primary")) |primary_value| {
        const primary_json = try jsonValueStringifyAlloc(alloc, primary_value);
        defer alloc.free(primary_json);
        return try std.fmt.allocPrint(alloc, "{{\"primary\":{s}}}", .{primary_json});
    }
    if (selector.object.get("unique")) |unique_value| {
        const unique_json = try jsonValueStringifyAlloc(alloc, unique_value);
        defer alloc.free(unique_json);
        return try std.fmt.allocPrint(alloc, "{{\"unique\":{s}}}", .{unique_json});
    }
    return error.InvalidRowsRequest;
}

fn jsonValueStringifyAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try out.toOwnedSlice();
}

fn freeWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: []const []const u8) void {
    for (deletes) |key| alloc.free(@constCast(key));
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

fn freeOptionalKeys(alloc: std.mem.Allocator, keys: []const ?[]const u8) void {
    for (keys) |key| if (key) |value| alloc.free(@constCast(value));
}

fn freeTransformOps(alloc: std.mem.Allocator, operations: []const db_mod.types.TransformOp) void {
    for (operations) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
    if (operations.len > 0) alloc.free(@constCast(operations));
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []const db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        for (transform.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (transform.operations.len > 0) alloc.free(@constCast(transform.operations));
    }
}

fn freePredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.TransactionVersionPredicate) void {
    for (predicates) |predicate| alloc.free(@constCast(predicate.key));
}

test "relational rows batch derives deterministic physical primary keys" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"order_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["tenant_id","order_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","order_id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var batch = try parseRowsBatchRequest(
        std.testing.allocator,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"tenant_id\":\"t1\",\"order_id\":\"o9\",\"status\":\"open\"}},{\"op\":\"delete\",\"where\":{\"primary\":{\"tenant_id\":\"t1\",\"order_id\":\"o9\"}}}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.predicates.len);
    try std.testing.expectEqualStrings(batch.writes[0].key, batch.deletes[0]);
    try std.testing.expect(std.mem.startsWith(u8, batch.writes[0].key, physical_primary_key_prefix));
}

test "relational rows batch returning materializes defaults generated columns and checks" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_key":{"type":"keyword","generated":{"op":"lower","field":"email"}},"status":{"type":"keyword","default":"active"},"amount":{"type":"numeric","default":1}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"checks":[{"name":"amount_positive","field":"amount","op":"gte","value":0},{"name":"status_present","field":"status","op":"is_not_null"}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var batch = try parseRowsBatchRequest(
        std.testing.allocator,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"email\":\"ADA@EXAMPLE.TEST\"},\"returning\":[\"id\",\"email_key\",\"status\",\"amount\"]}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"email\":\"ADA@EXAMPLE.TEST\",\"status\":\"active\",\"amount\":1,\"email_key\":\"ada@example.test\"}", batch.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"email_key\":\"ada@example.test\",\"status\":\"active\",\"amount\":1}", batch.returning_rows[0]);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{
                .ptr = self,
                .resolve = resolve,
                .lookup_primary = lookupPrimary,
            };
        }

        fn resolve(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
        ) !?[]u8 {
            return null;
        }

        fn lookupPrimary(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            physical_key: []const u8,
        ) !?ResolvedPrimaryRow {
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expect(std.mem.startsWith(u8, physical_key, physical_primary_key_prefix));
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"email\":\"ADA@EXAMPLE.TEST\",\"email_key\":\"ada@example.test\",\"status\":\"active\",\"amount\":1}"),
                .version = 9,
            };
        }
    };

    var resolver = Resolver{};
    var update_batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"email\":\"GRACE@EXAMPLE.TEST\"},\"returning\":[\"email\",\"email_key\"]}]}",
        schema,
        resolver.iface(),
    );
    defer update_batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), update_batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 2), update_batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("email", update_batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"GRACE@EXAMPLE.TEST\"", update_batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("email_key", update_batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("\"grace@example.test\"", update_batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), update_batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 9), update_batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), update_batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"email\":\"GRACE@EXAMPLE.TEST\",\"email_key\":\"grace@example.test\"}", update_batch.returning_rows[0]);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"bad@example.test\",\"amount\":-1}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"email\":\"bad@example.test\",\"email_key\":\"user-supplied\"}}]}",
            schema,
        ),
    );
}

test "relational rows materializes server defaults once per planned row" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"request_id":{"type":"keyword","x-antfly-default":{"op":"uuid_v4"}},"request_id_lc":{"type":"keyword","generated":{"op":"lower","field":"request_id"}},"created_at_ns":{"type":"numeric","x-antfly-default":{"op":"now_ns"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"checks":[{"name":"created_at_positive","field":"created_at_ns","op":"gt","value":0}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var batch = try parseRowsBatchRequest(
        std.testing.allocator,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\"},\"returning\":[\"request_id\",\"request_id_lc\",\"created_at_ns\"]}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);

    var committed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, batch.writes[0].value, .{});
    defer committed.deinit();
    const committed_request_id = committed.value.object.get("request_id") orelse return error.TestExpectedEqual;
    const committed_request_id_lc = committed.value.object.get("request_id_lc") orelse return error.TestExpectedEqual;
    const committed_created_at = committed.value.object.get("created_at_ns") orelse return error.TestExpectedEqual;
    try expectUuidV4JsonString(committed_request_id);
    try std.testing.expectEqualStrings(committed_request_id.string, committed_request_id_lc.string);
    try std.testing.expect(committed_created_at.integer > 0);

    var returned = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings(committed_request_id.string, returned.value.object.get("request_id").?.string);
    try std.testing.expectEqualStrings(committed_request_id_lc.string, returned.value.object.get("request_id_lc").?.string);
    try std.testing.expectEqual(committed_created_at.integer, returned.value.object.get("created_at_ns").?.integer);
}

test "relational rows applies server on update policies" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"updated_at_ns":{"type":"numeric","x-antfly-on-update":{"op":"now_ns"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn lookupPrimary(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !?ResolvedPrimaryRow {
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"status\":\"old\",\"updated_at_ns\":1}"),
                .version = 23,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"active\"},\"returning\":[\"status\",\"updated_at_ns\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 2), batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("updated_at_ns", batch.transforms[0].operations[1].path);
    try std.testing.expectEqual(db_mod.types.TransformOpType.set, batch.transforms[0].operations[1].op);
    try std.testing.expectEqual(@as(u64, 23), batch.predicates[0].expected_version);

    var returned = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("active", returned.value.object.get("status").?.string);
    switch (returned.value.object.get("updated_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 1),
        .float => |value| try std.testing.expect(value > 1),
        else => return error.TestUnexpectedResult,
    }
}

fn expectUuidV4JsonString(value: std.json.Value) !void {
    if (value != .string) return error.TestExpectedEqual;
    const text = value.string;
    try std.testing.expectEqual(@as(usize, 36), text.len);
    try std.testing.expectEqual(@as(u8, '-'), text[8]);
    try std.testing.expectEqual(@as(u8, '-'), text[13]);
    try std.testing.expectEqual(@as(u8, '-'), text[18]);
    try std.testing.expectEqual(@as(u8, '-'), text[23]);
    try std.testing.expectEqual(@as(u8, '4'), text[14]);
    try std.testing.expect(text[19] == '8' or text[19] == '9' or text[19] == 'a' or text[19] == 'b');
    for (text, 0..) |ch, i| {
        if (i == 8 or i == 13 or i == 18 or i == 23) continue;
        try std.testing.expect((ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f'));
    }
}

test "relational rows unique selector resolves through owner lookup" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        calls: usize = 0,
        last_encoded: []u8 = &.{},

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.last_encoded.len > 0) alloc.free(self.last_encoded);
            self.* = undefined;
        }

        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolve };
        }

        fn resolve(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("users_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            if (self.last_encoded.len > 0) alloc.free(self.last_encoded);
            self.last_encoded = try alloc.dupe(u8, encoded_value);
            self.calls += 1;
            return try alloc.dupe(u8, "\x00antfly-rel-pk:test");
        }
    };

    var resolver = Resolver{};
    defer resolver.deinit(std.testing.allocator);
    var get_req = try parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}}],\"include_physical_key\":true}",
        schema,
        resolver.iface(),
    );
    defer get_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(usize, 1), get_req.keys.len);
    try std.testing.expect(get_req.keys[0] != null);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:test", get_req.keys[0].?);
    try std.testing.expectEqualStrings("{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}}", get_req.identities_json[0]);
    try std.testing.expect(get_req.include_physical_key);

    var update_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}},\"expected_version\":17,\"patch\":{\"status\":\"active\"}},{\"op\":\"delete\",\"where\":{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}},\"expected_version\":\"18\"}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), resolver.calls);
    try std.testing.expectEqual(@as(usize, 1), update_req.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), update_req.deletes.len);
    try std.testing.expectEqual(@as(usize, 2), update_req.predicates.len);
    try std.testing.expectEqualStrings(update_req.transforms[0].key, update_req.predicates[0].key);
    try std.testing.expectEqual(@as(u64, 17), update_req.predicates[0].expected_version);
    try std.testing.expectEqualStrings(update_req.deletes[0], update_req.predicates[1].key);
    try std.testing.expectEqual(@as(u64, 18), update_req.predicates[1].expected_version);
}

test "relational rows conflict target upsert resolves expression unique owner" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_lower_email_key","expressions":[{"op":"lower","field":"email"}]}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        existing: bool = true,
        calls: usize = 0,

        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolve };
        }

        fn resolve(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("users_lower_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            self.calls += 1;
            if (!self.existing) return null;
            return try std.testing.allocator.dupe(u8, "\x00antfly-rel-pk:existing");
        }
    };

    var resolver = Resolver{};
    var update_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"A@EXAMPLE.COM\",\"name\":\"Ann\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_lower_email_key\"}},\"action\":\"update\",\"patch\":{\"name\":\"Ada\"}}}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(usize, 0), update_req.writes.len);
    try std.testing.expectEqual(@as(usize, 1), update_req.transforms.len);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:existing", update_req.transforms[0].key);
    try std.testing.expectEqual(@as(u32, 0), update_req.inserted);
    try std.testing.expectEqual(@as(u32, 1), update_req.transformed);
    try std.testing.expectEqualStrings("name", update_req.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"Ada\"", update_req.transforms[0].operations[0].value_json.?);

    resolver.existing = false;
    var insert_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"email\":\"new@example.com\",\"name\":\"New\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_lower_email_key\"}},\"action\":\"update\",\"patch\":{\"name\":\"Ignored\"}}}]}",
        schema,
        resolver.iface(),
    );
    defer insert_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), insert_req.writes.len);
    try std.testing.expectEqual(@as(usize, 1), insert_req.predicates.len);
    try std.testing.expectEqual(@as(usize, 0), insert_req.transforms.len);
    try std.testing.expectEqual(@as(u32, 1), insert_req.inserted);
    try std.testing.expectEqual(@as(u64, 0), insert_req.predicates[0].expected_version);

    resolver.existing = true;
    var nothing_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u4\",\"email\":\"a@example.com\",\"name\":\"Noop\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_lower_email_key\"}},\"action\":\"nothing\"},\"returning\":[\"*\"]}]}",
        schema,
        resolver.iface(),
    );
    defer nothing_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), nothing_req.writes.len);
    try std.testing.expectEqual(@as(usize, 0), nothing_req.transforms.len);
    try std.testing.expectEqual(@as(usize, 0), nothing_req.predicates.len);
    try std.testing.expectEqual(@as(usize, 0), nothing_req.returning_rows.len);
    try std.testing.expectEqual(@as(u32, 0), nothing_req.inserted);
    try std.testing.expectEqual(@as(u32, 0), nothing_req.transformed);
}

test "relational rows conflict target upsert validates partial unique predicate" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"email","op":"is_not_null"},{"field":"status","op":"eq","value":"active"}]}}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        calls: usize = 0,

        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolve };
        }

        fn resolve(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("users_active_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            self.calls += 1;
            return try alloc.dupe(u8, "\x00antfly-rel-pk:active-owner");
        }
    };

    const conflict_where = "\"where\":{\"all\":[{\"field\":\"email\",\"op\":\"is_not_null\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}";
    var resolver = Resolver{};
    var update_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"a@example.test\",\"status\":\"active\",\"name\":\"Ann\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"," ++ conflict_where ++ "}},\"action\":\"update\",\"patch\":{\"name\":\"Ada\"}}}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(usize, 0), update_req.writes.len);
    try std.testing.expectEqual(@as(usize, 1), update_req.transforms.len);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:active-owner", update_req.transforms[0].key);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"}},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u4\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\",\"where\":{\"all\":[{\"field\":\"email\",\"op\":\"is_not_null\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"inactive\"}]}}},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );

    var inactive_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u5\",\"email\":\"a@example.test\",\"status\":\"inactive\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"," ++ conflict_where ++ "}},\"action\":\"nothing\"}}]}",
        schema,
        resolver.iface(),
    );
    defer inactive_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), inactive_req.writes.len);
    try std.testing.expectEqual(@as(usize, 0), inactive_req.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), inactive_req.predicates.len);
}

test "relational rows conflict target upsert resolves primary existence" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        exists: bool = true,

        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .resolve_primary = resolvePrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn resolvePrimary(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            physical_key: []const u8,
        ) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expect(std.mem.startsWith(u8, physical_key, physical_primary_key_prefix));
            return self.exists;
        }
    };

    var resolver = Resolver{};
    var update_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"name\":\"Ann\"},\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"update\",\"patch\":{\"name\":\"Ada\"}}}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), update_req.writes.len);
    try std.testing.expectEqual(@as(usize, 1), update_req.transforms.len);
    try std.testing.expectEqual(@as(u32, 1), update_req.transformed);

    resolver.exists = false;
    var insert_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"name\":\"New\"},\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"nothing\"}}]}",
        schema,
        resolver.iface(),
    );
    defer insert_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), insert_req.writes.len);
    try std.testing.expectEqual(@as(usize, 1), insert_req.predicates.len);
    try std.testing.expectEqual(@as(u32, 1), insert_req.inserted);
}

test "relational rows batch returning projects committed mutation images" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        lookup_calls: usize = 0,

        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn lookupPrimary(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            physical_key: []const u8,
        ) !?ResolvedPrimaryRow {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expect(std.mem.startsWith(u8, physical_key, physical_primary_key_prefix));
            self.lookup_calls += 1;
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"name\":\"Ada\",\"status\":\"active\"}"),
                .version = 42,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"name\":\"Grace\",\"status\":\"new\"},\"returning\":[\"id\",\"name\"],\"returning_expressions\":[{\"as\":\"name_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"name\"}]}}]},{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"disabled\"},\"returning\":[\"id\",\"status\"],\"returning_expressions\":[{\"as\":\"label\",\"expr\":{\"op\":\"concat\",\"args\":[{\"field\":\"id\"},{\"value\":\":\"},{\"field\":\"status\"}]}}]},{\"op\":\"delete\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"returning\":[\"*\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), resolver.lookup_calls);
    try std.testing.expectEqual(@as(usize, 3), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u2\",\"name\":\"Grace\",\"name_key\":\"grace\"}", batch.returning_rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"disabled\",\"label\":\"u1:disabled\"}", batch.returning_rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"name\":\"Ada\",\"status\":\"active\"}", batch.returning_rows[2]);
    try std.testing.expectEqual(@as(usize, 3), batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 0), batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(u64, 42), batch.predicates[1].expected_version);
    try std.testing.expectEqual(@as(u64, 42), batch.predicates[2].expected_version);

    const response = try encodeRowsBatchResponseAlloc(std.testing.allocator, batch);
    defer std.testing.allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"returning\":[{\"id\":\"u2\",\"name\":\"Grace\",\"name_key\":\"grace\"},{\"id\":\"u1\",\"status\":\"disabled\",\"label\":\"u1:disabled\"},{\"id\":\"u1\",\"name\":\"Ada\",\"status\":\"active\"}]") != null);
}

test "relational rows batch supports typed numeric increments" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn lookupPrimary(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
        ) !?ResolvedPrimaryRow {
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"amount\":5,\"status\":\"active\"}"),
                .version = 17,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"increment\":{\"amount\":2},\"returning\":[\"amount\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("2", batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 17), batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);

    var returned = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 7), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 7), value),
        else => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"increment\":{\"status\":1}}]}",
        schema,
        resolver.iface(),
    ));
}

test "relational rows batch supports typed array transforms" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn lookupPrimary(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
        ) !?ResolvedPrimaryRow {
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"tags\":[\"db\",\"old\",\"old\"],\"status\":\"active\"}"),
                .version = 19,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"array_update\":[{\"field\":\"tags\",\"op\":\"append\",\"value\":\"db\"},{\"field\":\"tags\",\"op\":\"remove\",\"value\":\"old\"},{\"field\":\"tags\",\"op\":\"add_to_set\",\"value\":\"zig\"}],\"returning\":[\"tags\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 3), batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.push, batch.transforms[0].operations[0].op);
    try std.testing.expectEqual(db_mod.types.TransformOpType.pull, batch.transforms[0].operations[1].op);
    try std.testing.expectEqual(db_mod.types.TransformOpType.add_to_set, batch.transforms[0].operations[2].op);
    try std.testing.expectEqualStrings("tags", batch.transforms[0].operations[0].path);
    try std.testing.expectEqual(@as(u64, 19), batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"tags\":[\"db\",\"db\",\"zig\"]}", batch.returning_rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"array_update\":[{\"field\":\"status\",\"op\":\"append\",\"value\":\"bad\"}]}]}",
        schema,
        resolver.iface(),
    ));
}

test "relational rows json_set updates declared json columns" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"billing":{"type":"object","properties":{"plan":{"type":"keyword"}}}}}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return error.InvalidRowsRequest;
        }

        fn lookupPrimary(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
        ) !?ResolvedPrimaryRow {
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"status\":\"active\",\"attrs\":{\"billing\":{\"plan\":\"free\"}}}"),
                .version = 9,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\"}],\"returning\":[\"attrs.billing.plan\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("attrs.billing.plan", batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pro\"", batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"attrs.billing.plan\":\"pro\"}", batch.returning_rows[0]);
    try std.testing.expectEqual(@as(usize, 1), batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 9), batch.predicates[0].expected_version);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"status\",\"path\":[\"nested\"],\"value\":\"bad\"}]}]}",
        schema,
        resolver.iface(),
    ));
}

test "relational rows query contract filters orders paginates and projects rows" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"tenant_id\",\"op\":\"eq\",\"value\":\"t1\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},{\"field\":\"amount\",\"op\":\"gt\",\"value\":3}],\"not\":[{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"archived\"}]}]},\"select\":[\"id\",\"status\",\"created_at\"],\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"offset\":1,\"limit\":2}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.not_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.not_predicates[0].predicates.len);

    const rows = [_][]const u8{
        "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"status\":\"open\",\"amount\":5,\"created_at\":20}",
        "{\"id\":\"u2\",\"tenant_id\":\"t1\",\"status\":\"closed\",\"amount\":9,\"created_at\":40}",
        "{\"id\":\"u3\",\"tenant_id\":\"t1\",\"status\":\"open\",\"amount\":7,\"created_at\":30}",
        "{\"id\":\"u4\",\"tenant_id\":\"t1\",\"status\":\"open\",\"amount\":11,\"created_at\":10}",
        "{\"id\":\"u5\",\"tenant_id\":\"t2\",\"status\":\"open\",\"amount\":12,\"created_at\":50}",
    };

    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 3), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"open\",\"created_at\":20}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u4\",\"status\":\"open\",\"created_at\":10}", result.rows[1]);

    const response = try encodeRowsQueryResponseAlloc(std.testing.allocator, result);
    defer std.testing.allocator.free(response);
    try std.testing.expectEqualStrings("{\"total\":3,\"rows\":[{\"id\":\"u1\",\"status\":\"open\",\"created_at\":20},{\"id\":\"u4\",\"status\":\"open\",\"created_at\":10}]}", response);
}

test "relational rows aggregate contract accepts typed expression inputs and filters" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"discount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","customer","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{\"where\":{\"field\":\"created_at\",\"op\":\"gte\",\"value\":10}},\"group_by\":[\"customer\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"},{\"name\":\"status_count\",\"op\":\"count\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"distinct\":true},{\"name\":\"net_amount\",\"op\":\"sum\",\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"amount\"},{\"field\":\"discount\"}]},\"filter\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"filter_expressions\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"value\":\"open\"}}]},{\"name\":\"statuses\",\"op\":\"array_agg\",\"field\":\"status\",\"array_max_items\":4,\"array_order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}]}],\"having\":{\"field\":\"net_amount\",\"op\":\"gt\",\"value\":0},\"order_by\":[{\"field\":\"net_amount\",\"direction\":\"desc\"}],\"limit\":5}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.source.predicates.len);
    try std.testing.expectEqualStrings("created_at", request.source.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.group_by.len);
    try std.testing.expectEqualStrings("customer", request.group_by[0]);
    try std.testing.expectEqual(@as(usize, 4), request.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, request.aggregations[0].op);
    try std.testing.expect(request.aggregations[0].field == null);
    try std.testing.expect(request.aggregations[0].expression == null);
    try std.testing.expect(request.aggregations[1].distinct);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.aggregations[1].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, request.aggregations[2].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.aggregations[2].expression.?.kind);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, request.aggregations[3].op);
    try std.testing.expectEqual(@as(u32, 4), request.aggregations[3].array_max_items);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[3].array_order_by.len);
    try std.testing.expectEqual(@as(usize, 1), request.having_predicates.len);
    try std.testing.expectEqualStrings("net_amount", request.having_predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.order_by.len);
    try std.testing.expectEqualStrings("net_amount", request.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 5), request.limit.?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"aggregations\":[{\"name\":\"bad\",\"op\":\"sum\",\"field\":\"status\"}]}",
        schema,
    ));
}

test "relational rows window contract accepts typed row number plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","tenant","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["tenant","id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"partition_by\":[\"tenant\"],\"order_by\":[{\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]},\"direction\":\"desc\"}]}],\"select\":[\"tenant\",\"id\",\"amount\"],\"order_by\":[{\"field\":\"row_num\",\"direction\":\"asc\"}],\"limit\":10}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.source.predicates.len);
    try std.testing.expectEqualStrings("status", request.source.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.windows.len);
    try std.testing.expectEqualStrings("row_num", request.windows[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.row_number, request.windows[0].function);
    try std.testing.expectEqual(@as(usize, 1), request.windows[0].partition_by.len);
    try std.testing.expectEqualStrings("tenant", request.windows[0].partition_by[0]);
    try std.testing.expectEqual(@as(usize, 1), request.windows[0].order_by.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.windows[0].order_by[0].expression.?.kind);
    try std.testing.expectEqual(@as(usize, 3), request.select.len);
    try std.testing.expect(!request.select_all);
    try std.testing.expectEqual(@as(usize, 1), request.order_by.len);
    try std.testing.expectEqualStrings("row_num", request.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 10), request.limit.?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"source\":{\"row_claim\":{\"owner_id\":\"worker\",\"lease_ms\":1000}},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"rank\",\"function\":\"rank\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
}

test "relational rows join contract accepts typed equality join plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"order\"}},\"right\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"customer\"}},\"join_type\":\"left\",\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"select\":[{\"as\":\"order_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"customer_name\",\"side\":\"right\",\"field\":\"name\"}],\"order_by\":[{\"field\":\"order_id\",\"direction\":\"asc\"}],\"limit\":25}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinType.left, request.join_type);
    try std.testing.expectEqual(@as(usize, 1), request.left.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.right.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.on.len);
    try std.testing.expectEqualStrings("customer_id", request.on[0].left_field);
    try std.testing.expectEqualStrings("id", request.on[0].right_field);
    try std.testing.expectEqual(@as(usize, 2), request.select.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, request.select[0].side);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, request.select[1].side);
    try std.testing.expectEqual(@as(usize, 1), request.order_by.len);
    try std.testing.expectEqualStrings("order_id", request.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 25), request.limit.?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{\"doc_key_range\":{\"start\":\"a\",\"end\":\"z\"}},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}]}",
        schema,
    ));
}

test "relational rows lateral contract accepts bounded correlated plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"customer\"}},\"right\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"order\"},\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":1},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}],\"select\":[{\"as\":\"customer_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_order_id\",\"side\":\"right\",\"field\":\"id\"}],\"order_by\":[{\"field\":\"customer_id\",\"direction\":\"asc\"}],\"limit\":10}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.left.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.right.predicates.len);
    try std.testing.expectEqual(@as(u32, 1), request.right.limit.?);
    try std.testing.expectEqual(@as(usize, 1), request.correlations.len);
    try std.testing.expectEqualStrings("id", request.correlations[0].left_field);
    try std.testing.expectEqualStrings("customer_id", request.correlations[0].right_field);
    try std.testing.expectEqual(@as(usize, 2), request.select.len);
    try std.testing.expectEqual(@as(usize, 1), request.order_by.len);
    try std.testing.expectEqual(@as(u32, 10), request.limit.?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}]}",
        schema,
    ));
}

test "relational rows cte plan contract accepts ordered typed subplans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","tenant","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["tenant","id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var query_plan = try parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\"]}},{\"name\":\"expensive_open_rows\",\"query\":{\"source_cte\":\"open_rows\",\"where\":{\"field\":\"amount\",\"op\":\"gt\",\"value\":10},\"select\":[\"id\",\"amount\"]}}],\"query\":{\"source_cte\":\"expensive_open_rows\",\"select\":[\"id\"],\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":2}}",
        schema,
    );
    defer query_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), query_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", query_plan.ctes[0].name);
    try std.testing.expectEqualStrings("", query_plan.ctes[0].query.source_cte);
    try std.testing.expectEqualStrings("expensive_open_rows", query_plan.ctes[1].name);
    try std.testing.expectEqualStrings("open_rows", query_plan.ctes[1].query.source_cte);
    try std.testing.expectEqualStrings("expensive_open_rows", query_plan.query.source_cte);
    try std.testing.expectEqual(@as(u32, 2), query_plan.query.limit.?);

    var aggregate_plan = try parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"aggregate\":{\"source\":{\"source_cte\":\"open_rows\"},\"group_by\":[\"tenant\"],\"aggregations\":[{\"name\":\"amount_sum\",\"op\":\"sum\",\"field\":\"amount\"}]}}",
        schema,
    );
    defer aggregate_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), aggregate_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", aggregate_plan.aggregate.source.source_cte);

    var window_plan = try parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"window\":{\"source\":{\"source_cte\":\"open_rows\"},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"partition_by\":[\"tenant\"],\"order_by\":[{\"field\":\"created_at\"}]}],\"select\":[\"tenant\",\"id\"]}}",
        schema,
    );
    defer window_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), window_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", window_plan.window.source.source_cte);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"early\",\"query\":{\"source_cte\":\"later\"}},{\"name\":\"later\",\"query\":{\"select\":[\"id\"]}}],\"query\":{\"source_cte\":\"early\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"row_claim\":{\"transaction_id\":\"00000000000000000000000000000000\"}}}],\"window\":{\"source\":{\"source_cte\":\"open_rows\"},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"created_at\"}]}]}}",
        schema,
    ));
}

test "relational rows query contract supports scalar or predicate groups" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"any\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"closed\"},{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},{\"field\":\"amount\",\"op\":\"gt\",\"value\":20}]}]},\"select\":[\"id\",\"status\"],\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), request.or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.or_predicates[0].predicates.len);
    try std.testing.expectEqual(@as(usize, 2), request.or_predicates[1].predicates.len);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"open\",\"amount\":12,\"created_at\":20}",
        "{\"id\":\"b\",\"status\":\"closed\",\"amount\":3,\"created_at\":40}",
        "{\"id\":\"c\",\"status\":\"open\",\"amount\":25,\"created_at\":10}",
        "{\"id\":\"d\",\"status\":\"disabled\",\"amount\":99,\"created_at\":50}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"status\":\"closed\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"status\":\"open\"}", result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"any\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"closed\"]}]}}",
        schema,
    ));
}

test "relational rows query contract supports shorthand equality and validates fields" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"rank":{"type":"numeric"},"expires_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"status\":\"ready\"},\"order_by\":[{\"field\":\"rank\"}],\"limit\":10}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"ready\",\"rank\":2}",
        "{\"id\":\"b\",\"status\":\"blocked\",\"rank\":1}",
        "{\"id\":\"c\",\"status\":\"ready\",\"rank\":1}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"status\":\"ready\",\"rank\":1}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"ready\",\"rank\":2}", result.rows[1]);

    var in_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"ready\",\"queued\"]},{\"field\":\"id\",\"op\":\"not_in\",\"value\":[\"a\"]}]},\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer in_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), in_request.in_predicates.len);
    try std.testing.expectEqualStrings("status", in_request.in_predicates[0].field);
    try std.testing.expect(!in_request.in_predicates[0].negated);
    try std.testing.expectEqualStrings("id", in_request.in_predicates[1].field);
    try std.testing.expect(in_request.in_predicates[1].negated);
    var in_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, in_request, rows[0..]);
    defer in_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), in_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"status\":\"ready\",\"rank\":1}", in_result.rows[0]);

    const nullable_rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"ready\",\"rank\":1,\"expires_at\":10}",
        "{\"id\":\"b\",\"status\":\"ready\",\"rank\":1,\"expires_at\":5}",
        "{\"id\":\"c\",\"status\":\"ready\",\"rank\":1,\"expires_at\":null}",
        "{\"id\":\"d\",\"status\":\"ready\",\"rank\":1}",
    };
    var null_order_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"order_by\":[{\"field\":\"expires_at\",\"null_test\":\"is_null\"},{\"field\":\"expires_at\"},{\"field\":\"id\"}]}",
        schema,
    );
    defer null_order_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), null_order_request.order_by.len);
    try std.testing.expectEqual(RowsQueryOrderNullTest.is_null, null_order_request.order_by[0].null_test.?);
    var null_order_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, null_order_request, nullable_rows[0..]);
    defer null_order_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 4), null_order_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"status\":\"ready\",\"rank\":1,\"expires_at\":5}", null_order_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"ready\",\"rank\":1,\"expires_at\":10}", null_order_result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"status\":\"ready\",\"rank\":1,\"expires_at\":null}", null_order_result.rows[2]);
    try std.testing.expectEqualStrings("{\"id\":\"d\",\"status\":\"ready\",\"rank\":1}", null_order_result.rows[3]);

    const expression_order_rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"Beta\",\"rank\":1}",
        "{\"id\":\"b\",\"status\":\"alpha\",\"rank\":2}",
        "{\"id\":\"c\",\"status\":\"beta\",\"rank\":0}",
    };
    var expression_order_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\",\"status\"],\"order_by\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}},{\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"rank\"},{\"value\":0}]},\"direction\":\"desc\"}]}",
        schema,
    );
    defer expression_order_request.deinit(std.testing.allocator);
    try std.testing.expect(expression_order_request.order_by[0].expression != null);
    var expression_order_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, expression_order_request, expression_order_rows[0..]);
    defer expression_order_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 3), expression_order_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"status\":\"alpha\"}", expression_order_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"Beta\"}", expression_order_result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"status\":\"beta\"}", expression_order_result.rows[2]);

    var ranged_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"doc_key_range\":{\"start\":\"row:b\",\"end\":\"row:d\"},\"limit\":10}",
        schema,
    );
    defer ranged_request.deinit(std.testing.allocator);
    const doc_key_range = ranged_request.doc_key_range orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("row:b", doc_key_range.start);
    try std.testing.expectEqualStrings("row:d", doc_key_range.end);
    try std.testing.expectError(error.UnsupportedRowsQuery, executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, ranged_request, &.{}));

    var cte_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"source_cte\":\"open_orders\",\"where\":{\"status\":\"ready\"},\"limit\":10}",
        schema,
    );
    defer cte_request.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("open_orders", cte_request.source_cte);
    try std.testing.expectError(error.UnsupportedRowsQuery, executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, cte_request, rows[0..]));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"doc_key_range\":{\"start\":\"row:d\",\"end\":\"row:b\"}}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"missing\"]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"missing\",\"op\":\"eq\",\"value\":\"x\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"order_by\":[{\"field\":\"missing\"}]}",
        schema,
    ));
}

test "relational rows query contract projects array lengths" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"array_length\":[{\"as\":\"tag_count\",\"field\":\"tags\"}],\"expressions\":[{\"as\":\"tag_count_expr\",\"expr\":{\"op\":\"array_length\",\"args\":[{\"field\":\"tags\"}]}}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.array_length.len);
    try std.testing.expectEqualStrings("tag_count", request.array_length[0].output);
    try std.testing.expectEqualStrings("tags", request.array_length[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.expressions.len);
    try std.testing.expectEqualStrings("tag_count_expr", request.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_length, request.expressions[0].expression.kind);
    try std.testing.expectEqualStrings("tags", request.expressions[0].expression.operands[0].field);
    try std.testing.expect(!request.select_all);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"tags\":[\"hot\",\"new\"]}",
        "{\"id\":\"b\",\"tags\":[]}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"tag_count\":2,\"tag_count_expr\":2}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"tag_count\":0,\"tag_count_expr\":0}", result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"array_length\":[{\"as\":\"bad\",\"field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"array_length\",\"args\":[{\"field\":\"id\"}]}}]}",
        schema,
    ));
}

test "relational rows query contract projects coalesce expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"display_name":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"field_aliases\":[{\"as\":\"user_id\",\"field\":\"id\"}],\"coalesce\":[{\"as\":\"name_or_email\",\"operands\":[{\"field\":\"display_name\"},{\"field\":\"email\"},{\"value\":\"unknown\"}]}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.coalesce.len);
    try std.testing.expectEqualStrings("name_or_email", request.coalesce[0].output);
    try std.testing.expectEqual(@as(usize, 3), request.coalesce[0].operands.len);
    try std.testing.expectEqual(@as(usize, 1), request.field_aliases.len);
    try std.testing.expectEqualStrings("user_id", request.field_aliases[0].output);
    try std.testing.expectEqualStrings("id", request.field_aliases[0].field);
    try std.testing.expect(!request.select_all);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"display_name\":\"Ada\",\"email\":\"ada@example.test\"}",
        "{\"id\":\"b\",\"display_name\":null,\"email\":\"b@example.test\"}",
        "{\"id\":\"c\"}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 3), result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"name_or_email\":\"Ada\",\"user_id\":\"a\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"name_or_email\":\"b@example.test\",\"user_id\":\"b\"}", result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"name_or_email\":\"unknown\",\"user_id\":\"c\"}", result.rows[2]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"coalesce\":[{\"as\":\"bad\",\"operands\":[{\"field\":\"missing\"}]}]}",
        schema,
    ));
}

test "relational rows query contract projects generic expression AST" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"display_name":{"type":"keyword"},"email":{"type":"keyword"},"score":{"type":"numeric"},"bonus":{"type":"numeric"},"penalty":{"type":"numeric"},"multiplier":{"type":"numeric"},"divisor":{"type":"numeric"},"attrs":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"name_or_email\",\"expr\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"display_name\"},{\"field\":\"email\"},{\"value\":\"unknown\"}]}},{\"as\":\"email_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}]}},{\"as\":\"label\",\"expr\":{\"op\":\"concat\",\"args\":[{\"field\":\"id\"},{\"value\":\"::\"},{\"field\":\"email\"}]}},{\"as\":\"email_without_b\",\"expr\":{\"op\":\"nullif\",\"args\":[{\"field\":\"email\"},{\"value\":\"b@example.test\"}]}},{\"as\":\"total_score\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"}]}},{\"as\":\"net_score\",\"expr\":{\"op\":\"sub\",\"args\":[{\"op\":\"add\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"}]},{\"field\":\"penalty\"}]}},{\"as\":\"scaled_score\",\"expr\":{\"op\":\"mul\",\"args\":[{\"field\":\"score\"},{\"field\":\"multiplier\"}]}},{\"as\":\"score_ratio\",\"expr\":{\"op\":\"div\",\"args\":[{\"field\":\"score\"},{\"field\":\"divisor\"}]}},{\"as\":\"email_bucket\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\"},\"then\":{\"value\":\"missing\"}},{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"eq\",\"rhs\":{\"value\":\"b@example.test\"}},\"then\":{\"value\":\"blocked\"}}],\"else\":{\"value\":\"ok\"}}},{\"as\":\"plan\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"attrs\"}],\"path\":[\"billing\",\"plan\"],\"as_text\":true}},{\"as\":\"flags\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"attrs\"}],\"path\":\"flags\"}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 11), request.expressions.len);
    try std.testing.expectEqualStrings("name_or_email", request.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, request.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 3), request.expressions[0].expression.operands.len);
    try std.testing.expectEqualStrings("email_key", request.expressions[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.expressions[1].expression.kind);
    try std.testing.expectEqual(@as(usize, 1), request.expressions[1].expression.operands.len);
    try std.testing.expectEqualStrings("label", request.expressions[2].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.concat, request.expressions[2].expression.kind);
    try std.testing.expectEqual(@as(usize, 3), request.expressions[2].expression.operands.len);
    try std.testing.expectEqualStrings("email_without_b", request.expressions[3].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.nullif, request.expressions[3].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.expressions[3].expression.operands.len);
    try std.testing.expectEqualStrings("total_score", request.expressions[4].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.expressions[4].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.expressions[4].expression.operands.len);
    try std.testing.expectEqualStrings("net_score", request.expressions[5].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.expressions[5].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.expressions[5].expression.operands[0].kind);
    try std.testing.expectEqualStrings("scaled_score", request.expressions[6].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.mul, request.expressions[6].expression.kind);
    try std.testing.expectEqualStrings("score_ratio", request.expressions[7].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.div, request.expressions[7].expression.kind);
    try std.testing.expectEqualStrings("email_bucket", request.expressions[8].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, request.expressions[8].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.expressions[8].expression.case_branches.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, request.expressions[8].expression.case_branches[0].when.op);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, request.expressions[8].expression.case_branches[1].when.op);
    try std.testing.expectEqual(@as(usize, 1), request.expressions[8].expression.case_else.len);
    try std.testing.expectEqualStrings("plan", request.expressions[9].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, request.expressions[9].expression.kind);
    try std.testing.expectEqualStrings("billing.plan", request.expressions[9].expression.json_path);
    try std.testing.expect(request.expressions[9].expression.json_as_text);
    try std.testing.expectEqualStrings("flags", request.expressions[10].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, request.expressions[10].expression.kind);
    try std.testing.expect(!request.expressions[10].expression.json_as_text);
    try std.testing.expect(!request.select_all);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"display_name\":\"Ada\",\"email\":\"ada@example.test\",\"score\":10,\"bonus\":5,\"penalty\":2,\"multiplier\":3,\"divisor\":5,\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}}",
        "{\"id\":\"b\",\"display_name\":null,\"email\":\"b@example.test\",\"score\":3,\"bonus\":4,\"penalty\":1,\"multiplier\":2,\"divisor\":2,\"attrs\":{\"billing\":{\"plan\":\"free\"},\"flags\":[\"blocked\"]}}",
        "{\"id\":\"c\"}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"name_or_email\":\"Ada\",\"email_key\":\"ada@example.test\",\"label\":\"a::ada@example.test\",\"email_without_b\":\"ada@example.test\",\"total_score\":15,\"net_score\":13,\"scaled_score\":30,\"score_ratio\":2,\"email_bucket\":\"ok\",\"plan\":\"pro\",\"flags\":[\"active\"]}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"name_or_email\":\"b@example.test\",\"email_key\":\"b@example.test\",\"label\":\"b::b@example.test\",\"email_without_b\":null,\"total_score\":7,\"net_score\":6,\"scaled_score\":6,\"score_ratio\":1.5,\"email_bucket\":\"blocked\",\"plan\":\"free\",\"flags\":[\"blocked\"]}", result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"name_or_email\":\"unknown\",\"email_key\":null,\"label\":\"c::\",\"email_without_b\":null,\"total_score\":null,\"net_score\":null,\"scaled_score\":null,\"score_ratio\":null,\"email_bucket\":\"missing\",\"plan\":null,\"flags\":null}", result.rows[2]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"field\":\"missing\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"},{\"value\":\"extra\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"concat\",\"args\":[]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"nullif\",\"args\":[{\"field\":\"email\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"score\"},{\"field\":\"email\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"score\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"mul\",\"args\":[{\"field\":\"score\"},{\"field\":\"email\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"div\",\"args\":[{\"field\":\"score\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"email\"}],\"path\":\"billing.plan\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"attrs\"}],\"path\":[]}}]}",
        schema,
    ));
}

test "relational rows query contract projects cast expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"score_text":{"type":"keyword"},"active":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"id_text\",\"expr\":{\"op\":\"cast\",\"to\":\"text\",\"args\":[{\"field\":\"id\"}]}},{\"as\":\"score_number\",\"expr\":{\"op\":\"cast\",\"to\":\"numeric\",\"args\":[{\"field\":\"score_text\"}]}},{\"as\":\"active_bool\",\"expr\":{\"op\":\"cast\",\"to\":\"bool\",\"args\":[{\"value\":\"true\"}]}},{\"as\":\"active_text\",\"expr\":{\"op\":\"cast\",\"to\":\"text\",\"args\":[{\"field\":\"active\"}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), request.expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.cast, request.expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.text, request.expressions[0].expression.cast_type.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.numeric, request.expressions[1].expression.cast_type.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.bool, request.expressions[2].expression.cast_type.?);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"score_text\":\"42.5\",\"active\":true}",
        "{\"id\":\"b\",\"score_text\":null,\"active\":false}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"id_text\":\"a\",\"score_number\":42.5,\"active_bool\":true,\"active_text\":\"true\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"id_text\":\"b\",\"score_number\":null,\"active_bool\":true,\"active_text\":\"false\"}", result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"cast\",\"to\":\"timestamp\",\"args\":[{\"field\":\"id\"}]}}]}",
        schema,
    ));
}

test "relational rows api query contract parses typed row claim request" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"rank":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"status\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"skip_locked\":true,\"lease_ms\":45000,\"owner_id\":\"session:7\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"},\"limit\":10}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    const claim = request.row_claim orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.RowClaimMode.for_update, claim.mode);
    try std.testing.expect(claim.skip_locked);
    try std.testing.expectEqual(@as(u64, 45_000), claim.lease_ms);
    try std.testing.expectEqualStrings("session:7", claim.owner_id);
    try std.testing.expectEqual(@as(u8, 0x00), claim.txn_id.?[0]);
    try std.testing.expectEqual(@as(u8, 0x11), claim.txn_id.?[1]);
    try std.testing.expectEqual(@as(u8, 0xff), claim.txn_id.?[15]);

    var alias_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"row_claim\":{\"txn_id\":\"00112233445566778899aabbccddeeff\"}}",
        schema,
    );
    defer alias_request.deinit(std.testing.allocator);
    try std.testing.expect(alias_request.row_claim != null);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"row_claim\":{\"transaction_id\":\"001122\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"row_claim\":{\"transaction_id\":\"00112233445566778899aabbccddeefg\"}}",
        schema,
    ));
}

test "relational rows mutation source contract parses claimed update plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"status\":\"ready\"},\"order_by\":[{\"field\":\"amount\",\"direction\":\"asc\"}],\"limit\":2,\"row_claim\":{\"mode\":\"for_update\",\"skip_locked\":true,\"owner_id\":\"session:mutation\",\"txn_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"id\",\"status\"],\"returning_expressions\":[{\"as\":\"amount_plus_one\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, request.req.kind);
    try std.testing.expectEqual(@as(usize, 1), request.req.source.predicates.len);
    try std.testing.expectEqualStrings("status", request.req.source.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.req.source.order_by.len);
    try std.testing.expectEqual(@as(u32, 2), request.req.source.limit.?);
    try std.testing.expect(request.req.source.row_claim != null);
    try std.testing.expect(request.req.source.row_claim.?.skip_locked);
    try std.testing.expectEqual(@as(usize, 1), request.req.operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.set, request.req.operations[0].op);
    try std.testing.expectEqualStrings("status", request.req.operations[0].path);
    try std.testing.expectEqualStrings("\"claimed\"", request.req.operations[0].value_json.?);
    try std.testing.expect(!request.req.returning_all);
    try std.testing.expectEqual(@as(usize, 2), request.req.returning.len);
    try std.testing.expectEqualStrings("id", request.req.returning[0]);
    try std.testing.expectEqualStrings("status", request.req.returning[1]);
    try std.testing.expectEqual(@as(usize, 1), request.req.returning_expressions.len);
    try std.testing.expectEqualStrings("amount_plus_one", request.req.returning_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.req.returning_expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.req.returning_expressions[0].expression.operands.len);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"delete\",\"source\":{\"where\":{\"status\":\"ready\"}}}",
        schema,
    ));

    var returning_rows = [_][]const u8{ "{\"id\":\"a\"}", "{\"id\":\"b\"}" };
    const response = try encodeRowsMutationSourceResponseAlloc(std.testing.allocator, .{
        .matched = 3,
        .staged = 2,
        .returning_rows = returning_rows[0..],
    });
    defer std.testing.allocator.free(response);
    try std.testing.expectEqualStrings("{\"matched\":3,\"staged\":2,\"returning\":[{\"id\":\"a\"},{\"id\":\"b\"}]}", response);
}

test "relational rows api query contract parses typed json filters" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"attrs":{"type":"json"},"rank":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},{\"field\":\"tags\",\"op\":\"array_any\",\"value\":\"hot\"},{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\",\"new\"]},{\"field\":\"tags\",\"op\":\"array_eq\",\"value\":[\"hot\",\"new\"]},{\"field\":\"attrs\",\"op\":\"json_contains\",\"value\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}},{\"field\":\"attrs\",\"op\":\"json_path_eq\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\"},{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"flags\"}]},\"select\":[\"id\",\"rank\"],\"json_extract\":[{\"as\":\"plan\",\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"as_text\":true},{\"as\":\"flags\",\"field\":\"attrs\",\"path\":\"flags\"}],\"order_by\":[{\"field\":\"rank\"}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.array_any.len);
    try std.testing.expectEqual(@as(usize, 1), request.array_contains.len);
    try std.testing.expectEqual(@as(usize, 1), request.array_eq.len);
    try std.testing.expectEqual(@as(usize, 1), request.json_contains.len);
    try std.testing.expectEqual(@as(usize, 1), request.json_path_eq.len);
    try std.testing.expectEqual(@as(usize, 1), request.json_path_exists.len);
    try std.testing.expectEqual(@as(usize, 2), request.json_extract.len);
    try std.testing.expectEqualStrings("tags", request.array_any[0].field);
    try std.testing.expectEqualStrings("\"hot\"", request.array_any[0].value_json);
    try std.testing.expectEqualStrings("tags", request.array_contains[0].field);
    try std.testing.expectEqualStrings("[\"hot\",\"new\"]", request.array_contains[0].value_json);
    try std.testing.expectEqualStrings("tags", request.array_eq[0].field);
    try std.testing.expectEqualStrings("[\"hot\",\"new\"]", request.array_eq[0].value_json);
    try std.testing.expectEqualStrings("attrs", request.json_contains[0].field);
    try std.testing.expectEqualStrings("attrs", request.json_path_eq[0].field);
    try std.testing.expectEqualStrings("billing.plan", request.json_path_eq[0].path);
    try std.testing.expectEqualStrings("\"pro\"", request.json_path_eq[0].value_json);
    try std.testing.expectEqualStrings("attrs", request.json_path_exists[0].field);
    try std.testing.expectEqualStrings("flags", request.json_path_exists[0].path);
    try std.testing.expectEqualStrings("plan", request.json_extract[0].output);
    try std.testing.expectEqualStrings("attrs", request.json_extract[0].field);
    try std.testing.expectEqualStrings("billing.plan", request.json_extract[0].path);
    try std.testing.expect(request.json_extract[0].as_text);
    try std.testing.expectEqualStrings("flags", request.json_extract[1].output);
    try std.testing.expect(!request.json_extract[1].as_text);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"active\",\"tags\":[\"hot\",\"new\"],\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\",\"beta\"]},\"rank\":2}",
        "{\"id\":\"b\",\"status\":\"active\",\"tags\":[\"cold\"],\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]},\"rank\":1}",
        "{\"id\":\"c\",\"status\":\"active\",\"tags\":[\"hot\"],\"attrs\":{\"billing\":{\"plan\":\"free\"},\"flags\":[\"active\"]},\"rank\":3}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"rank\":2,\"plan\":\"pro\",\"flags\":[\"active\",\"beta\"]}", result.rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"array_any\",\"value\":\"active\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"array_contains\",\"value\":[\"active\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"tags\",\"op\":\"array_eq\",\"value\":\"hot\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"tags\",\"op\":\"json_contains\",\"value\":[\"hot\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"json_path_eq\",\"path\":\"billing.plan\",\"value\":\"pro\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"json_extract\":[{\"as\":\"plan\",\"field\":\"status\",\"path\":\"billing.plan\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"json_extract\":[{\"as\":\"plan\",\"field\":\"attrs\",\"path\":[]}]}",
        schema,
    ));
}
