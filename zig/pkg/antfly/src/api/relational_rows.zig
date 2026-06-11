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

pub const OwnedRowsPlan = union(enum) {
    query: OwnedRowsQueryPlan,
    aggregate: OwnedRowsAggregatePlan,
    window: OwnedRowsWindowPlan,
    join: OwnedRowsJoinPlan,
    lateral: OwnedRowsLateralPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query => |*plan| plan.deinit(alloc),
            .aggregate => |*plan| plan.deinit(alloc),
            .window => |*plan| plan.deinit(alloc),
            .join => |*plan| plan.deinit(alloc),
            .lateral => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

const RowsPlanOperation = enum {
    query,
    aggregate,
    window,
    join,
    lateral,
};

pub const OwnedRowsMutationSourceRequest = struct {
    req: db_mod.types.RelationalRowsMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.req.source.deinit(alloc);
        for (self.req.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (self.req.operations.len > 0) alloc.free(self.req.operations);
        freeRowsExpressionAssignments(alloc, self.req.patch_expressions);
        freeRowsExpressionAssignments(alloc, self.req.increment_expressions);
        freeRowsJsonSetExpressionAssignments(alloc, self.req.json_set_expressions);
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

pub const OwnedRowsInsertSourceRequest = struct {
    req: db_mod.types.RelationalRowsInsertSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.req.source_table.len > 0) alloc.free(@constCast(self.req.source_table));
        self.req.source.deinit(alloc);
        freeRowsExpressionAssignments(alloc, self.req.assignments);
        if (self.req.on_conflict) |conflict| freeRowsOnConflict(alloc, conflict);
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

pub const OwnedRowsJoinedMutationSourceRequest = struct {
    req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.req.source_table.len > 0) alloc.free(@constCast(self.req.source_table));
        self.req.join.deinit(alloc);
        freeRowsQueryExpressionConditions(alloc, self.req.match_expression_predicates);
        freeRowsQueryExpressionPredicateGroups(alloc, self.req.match_expression_or_predicates);
        freeRowsQueryExpressionPredicateGroups(alloc, self.req.match_expression_not_predicates);
        freeRowsQueryExpressionArrayContainsPredicates(alloc, self.req.match_expression_array_contains);
        for (self.req.source_assignments) |assignment| {
            alloc.free(@constCast(assignment.field));
            alloc.free(@constCast(assignment.source_field));
        }
        if (self.req.source_assignments.len > 0) alloc.free(self.req.source_assignments);
        for (self.req.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (self.req.operations.len > 0) alloc.free(self.req.operations);
        freeRowsExpressionAssignments(alloc, self.req.patch_expressions);
        freeRowsExpressionAssignments(alloc, self.req.increment_expressions);
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

pub fn buildRowsInsertSourceBatchAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    source_rows: []const []const u8,
    conflict_resolver: ?UniqueSelectorResolver,
) !OwnedRowsBatchRequest {
    return try buildRowsInsertSourceBatchWithSchemasAlloc(alloc, table_name, schema, schema, req, source_rows, conflict_resolver);
}

pub fn buildRowsInsertSourceBatchWithSchemasAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    source_rows: []const []const u8,
    conflict_resolver: ?UniqueSelectorResolver,
) !OwnedRowsBatchRequest {
    const schema = target_schema;
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    db_mod.DB.validateRelationalRowsInsertSourceRequestWithSchemas(target_schema, source_schema, req) catch return error.InvalidRowsRequest;
    if (req.on_conflict != null) _ = conflict_resolver orelse return error.UnsupportedRowsSelector;

    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    var transforms = std.ArrayListUnmanaged(db_mod.types.DocumentTransform).empty;
    var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
    var returning_rows = std.ArrayListUnmanaged([]const u8).empty;
    var proposed_conflict_targets = std.StringHashMapUnmanaged(void).empty;
    errdefer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
        for (transforms.items) |transform| {
            alloc.free(@constCast(transform.key));
            freeTransformOps(alloc, transform.operations);
        }
        transforms.deinit(alloc);
        for (predicates.items) |predicate| alloc.free(@constCast(predicate.key));
        predicates.deinit(alloc);
        for (returning_rows.items) |row| alloc.free(@constCast(row));
        returning_rows.deinit(alloc);
    }
    defer {
        var keys = proposed_conflict_targets.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        proposed_conflict_targets.deinit(alloc);
    }

    for (source_rows) |source_row_json| {
        const row_json = try insertSourceAssignedRowJsonAlloc(alloc, req, source_row_json);
        defer alloc.free(row_json);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;

        if (req.on_conflict) |conflict| {
            const planned = try plannedRelationalRowJsonAlloc(alloc, schema, parsed.value);
            defer alloc.free(planned);
            const duplicate_key = try typedConflictTargetDuplicateKeyAlloc(alloc, schema, planned, conflict.target);
            var duplicate_key_transferred = false;
            errdefer if (!duplicate_key_transferred) if (duplicate_key) |key| alloc.free(key);
            if (duplicate_key) |key| {
                const gop = try proposed_conflict_targets.getOrPut(alloc, key);
                if (gop.found_existing) {
                    alloc.free(key);
                    if (conflict.action == .update) return error.InvalidRowsRequest;
                    continue;
                }
                duplicate_key_transferred = true;
            }
            const conflict_key = try typedConflictTargetPrimaryKeyAlloc(alloc, table_name, schema, planned, conflict.target, conflict_resolver.?);
            defer if (conflict_key) |key| alloc.free(key);
            if (conflict_key) |key| {
                switch (conflict.action) {
                    .nothing => continue,
                    .update => {
                        try appendTypedConflictUpdateAlloc(alloc, table_name, schema, req, planned, conflict, key, conflict_resolver.?, &transforms, &predicates, &returning_rows);
                        continue;
                    },
                }
            }
            try appendPlannedInsertAlloc(alloc, schema, planned, true, &writes, &predicates);
        } else {
            const planned = try plannedRelationalRowJsonAlloc(alloc, schema, parsed.value);
            defer alloc.free(planned);
            try appendPlannedInsertAlloc(alloc, schema, planned, true, &writes, &predicates);
        }
        if (req.returning_all or req.returning.len > 0 or req.returning_expressions.len > 0) {
            const projected = try insertSourceReturningProjectionAlloc(alloc, schema, req, writes.items[writes.items.len - 1].value);
            var projected_transferred = false;
            errdefer if (!projected_transferred) alloc.free(projected);
            try returning_rows.append(alloc, projected);
            projected_transferred = true;
        }
    }

    const writes_slice = try writes.toOwnedSlice(alloc);
    errdefer {
        for (writes_slice) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (writes_slice.len > 0) alloc.free(writes_slice);
    }
    const transforms_slice = try transforms.toOwnedSlice(alloc);
    errdefer {
        for (transforms_slice) |transform| {
            alloc.free(@constCast(transform.key));
            freeTransformOps(alloc, transform.operations);
        }
        if (transforms_slice.len > 0) alloc.free(transforms_slice);
    }
    const predicates_slice = try predicates.toOwnedSlice(alloc);
    errdefer {
        for (predicates_slice) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates_slice.len > 0) alloc.free(predicates_slice);
    }
    const returning_slice = try returning_rows.toOwnedSlice(alloc);
    errdefer {
        for (returning_slice) |row| alloc.free(@constCast(row));
        if (returning_slice.len > 0) alloc.free(returning_slice);
    }

    return .{
        .writes = writes_slice,
        .transforms = transforms_slice,
        .predicates = predicates_slice,
        .returning_rows = returning_slice,
        .req = .{
            .writes = writes_slice,
            .transforms = transforms_slice,
            .predicates = predicates_slice,
        },
        .inserted = @intCast(writes_slice.len),
        .transformed = @intCast(transforms_slice.len),
    };
}

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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "operations", "sync_level" });
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
        try validateRowsBatchOperationEnvelope(op_value, op_text);

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
            const needs_planned_row = hasMutationExpression(op_value) or schemaHasGeneratedColumns(schema) or schema.checks.len != 0 or hasReturningProjection(op_value);
            var returning_base = if (needs_planned_row)
                try lookupBaseRowForKey(alloc, table_name, key, unique_resolver)
            else
                null;
            defer if (returning_base) |*row| row.deinit(alloc);
            var parsed_existing: std.json.Parsed(std.json.Value) = undefined;
            var parsed_existing_loaded = false;
            defer if (parsed_existing_loaded) parsed_existing.deinit();
            const existing_row_value: ?std.json.Value = if (returning_base) |row| blk: {
                parsed_existing = std.json.parseFromSlice(std.json.Value, alloc, row.json, .{}) catch return error.InvalidRowsRequest;
                parsed_existing_loaded = true;
                if (parsed_existing.value != .object) return error.InvalidRowsRequest;
                break :blk parsed_existing.value;
            } else null;
            var operations = try updateTransformOperationsAlloc(alloc, schema, op_value, existing_row_value, null);
            var operations_transferred = false;
            errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
            operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);
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

    try validateRowsBatchTargetKeys(writes.items, deletes.items, transforms.items);

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

fn validateRowsBatchOperationEnvelope(op_value: std.json.Value, op_text: []const u8) !void {
    if (std.mem.eql(u8, op_text, "insert")) {
        try requireJsonObjectOnlyKeys(op_value.object, &.{ "op", "row", "on_conflict", "returning", "returning_expressions" });
    } else if (std.mem.eql(u8, op_text, "upsert")) {
        try requireJsonObjectOnlyKeys(op_value.object, &.{ "op", "row", "returning", "returning_expressions" });
    } else if (std.mem.eql(u8, op_text, "update")) {
        try requireJsonObjectOnlyKeys(op_value.object, &.{ "op", "where", "patch", "patch_expr", "increment", "increment_expr", "json_set", "array_update", "returning", "returning_expressions", "expected_version" });
    } else if (std.mem.eql(u8, op_text, "delete")) {
        try requireJsonObjectOnlyKeys(op_value.object, &.{ "op", "where", "returning", "returning_expressions", "expected_version" });
    } else {
        return error.InvalidRowsRequest;
    }
}

fn validateRowsBatchTargetKeys(
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    for (writes, 0..) |write, i| {
        for (writes[i + 1 ..]) |other| {
            if (std.mem.eql(u8, write.key, other.key)) return error.InvalidRowsRequest;
        }
        for (deletes) |key| {
            if (std.mem.eql(u8, write.key, key)) return error.InvalidRowsRequest;
        }
        for (transforms) |transform| {
            if (std.mem.eql(u8, write.key, transform.key)) return error.InvalidRowsRequest;
        }
    }

    for (deletes, 0..) |key, i| {
        for (deletes[i + 1 ..]) |other| {
            if (std.mem.eql(u8, key, other)) return error.InvalidRowsRequest;
        }
        for (transforms) |transform| {
            if (std.mem.eql(u8, key, transform.key)) return error.InvalidRowsRequest;
        }
    }

    for (transforms, 0..) |transform, i| {
        for (transforms[i + 1 ..]) |other| {
            if (std.mem.eql(u8, transform.key, other.key)) return error.InvalidRowsRequest;
        }
    }
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "keys", "include_physical_key" });

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

fn freeRowsQueryArrayAnyPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayAnyPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
}

fn freeRowsQueryAccessPredicateGroups(alloc: std.mem.Allocator, groups: []const db_mod.types.RelationalRowsAccessPredicateGroup) void {
    for (groups) |group| {
        for (group.predicates) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        if (group.predicates.len > 0) alloc.free(group.predicates);
        freeRowsQueryArrayAnyPredicatesNoSlice(alloc, group.array_any);
        if (group.array_any.len > 0) alloc.free(group.array_any);
        for (group.array_contains) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (group.array_contains.len > 0) alloc.free(group.array_contains);
        for (group.array_eq) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (group.array_eq.len > 0) alloc.free(group.array_eq);
        for (group.in_predicates) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.values_json);
        }
        if (group.in_predicates.len > 0) alloc.free(group.in_predicates);
        for (group.json_contains) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        }
        if (group.json_contains.len > 0) alloc.free(group.json_contains);
        for (group.json_path_eq) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.path);
            alloc.free(predicate.value_json);
        }
        if (group.json_path_eq.len > 0) alloc.free(group.json_path_eq);
        for (group.json_path_exists) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.path);
        }
        if (group.json_path_exists.len > 0) alloc.free(group.json_path_exists);
        for (group.text_patterns) |predicate| {
            alloc.free(predicate.field);
            alloc.free(predicate.pattern);
        }
        if (group.text_patterns.len > 0) alloc.free(group.text_patterns);
    }
    if (groups.len > 0) alloc.free(groups);
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "source_cte", "where", "expression_where", "expression_any", "expression_not", "expression_array_contains", "select", "json_extract", "array_length", "coalesce", "field_aliases", "expressions", "distinct_on", "order_by", "limit", "offset", "row_claim", "doc_key_range" });

    const source_cte = try parseRowsQuerySourceCteAlloc(alloc, parsed.value.object.get("source_cte"));
    errdefer if (source_cte.len > 0) alloc.free(source_cte);

    const predicates = try parseRowsQueryPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeQueryPredicates(alloc, predicates);

    const or_predicates = try parseRowsQueryOrPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryPredicateGroups(alloc, or_predicates);

    const not_predicates = try parseRowsQueryNotPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryPredicateGroups(alloc, not_predicates);

    const access_or_predicates = try parseRowsQueryAccessPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"), "any");
    errdefer freeRowsQueryAccessPredicateGroups(alloc, access_or_predicates);

    const access_not_predicates = try parseRowsQueryAccessPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("where"), "not");
    errdefer freeRowsQueryAccessPredicateGroups(alloc, access_not_predicates);

    const expression_predicates = try parseRowsQueryExpressionPredicatesAlloc(alloc, schema, parsed.value.object.get("expression_where"));
    errdefer freeRowsQueryExpressionConditions(alloc, expression_predicates);

    const expression_or_predicates = try parseRowsQueryExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("expression_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, expression_or_predicates);

    const expression_not_predicates = try parseRowsQueryExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("expression_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, expression_not_predicates);

    const expression_array_contains = try parseRowsQueryExpressionArrayContainsPredicatesAlloc(alloc, schema, parsed.value.object.get("expression_array_contains"));
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, expression_array_contains);

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

    const text_patterns = try parseRowsQueryTextPatternPredicatesAlloc(alloc, schema, parsed.value.object.get("where"));
    errdefer freeRowsQueryTextPatternPredicates(alloc, text_patterns);

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

    const distinct_on = try parseRowsQueryDistinctOnAlloc(alloc, schema, parsed.value.object.get("distinct_on"), order_by);
    errdefer {
        for (distinct_on) |field| alloc.free(field);
        if (distinct_on.len > 0) alloc.free(distinct_on);
    }

    const row_claim = try parseRowsQueryRowClaimAlloc(alloc, parsed.value.object.get("row_claim"));
    errdefer if (row_claim) |claim| if (claim.owner_id.len > 0) alloc.free(claim.owner_id);
    if (row_claim != null and distinct_on.len > 0) return error.InvalidRowsRequest;
    if (row_claim != null and source_cte.len != 0) return error.InvalidRowsRequest;

    const doc_key_range = try parseRowsQueryDocKeyRangeAlloc(alloc, parsed.value.object.get("doc_key_range"));
    errdefer if (doc_key_range) |range| {
        if (range.start.len > 0) alloc.free(range.start);
        if (range.end.len > 0) alloc.free(range.end);
    };

    try validateRowsQueryProjectionOutputs(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions);

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
        .text_patterns = text_patterns,
        .or_predicates = or_predicates,
        .not_predicates = not_predicates,
        .access_or_predicates = access_or_predicates,
        .access_not_predicates = access_not_predicates,
        .expression_predicates = expression_predicates,
        .expression_or_predicates = expression_or_predicates,
        .expression_not_predicates = expression_not_predicates,
        .expression_array_contains = expression_array_contains,
        .select = select_parsed.fields,
        .json_extract = json_extract,
        .array_length = array_length,
        .coalesce = coalesce,
        .field_aliases = field_aliases,
        .expressions = expressions,
        .select_all = select_parsed.all,
        .distinct_on = distinct_on,
        .order_by = order_by,
        .row_claim = row_claim,
        .doc_key_range = doc_key_range,
        .limit = try parseOptionalU32(parsed.value.object.get("limit")),
        .offset = (try parseOptionalU32(parsed.value.object.get("offset"))) orelse 0,
    };
}

fn parseRowsQueryDistinctOnAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_distinct_on: ?std.json.Value,
    order_by: []const RowsQueryOrder,
) ![]const []const u8 {
    const value = maybe_distinct_on orelse return &.{};
    if (value != .array or value.array.items.len == 0) return error.InvalidRowsRequest;
    if (order_by.len < value.array.items.len) return error.InvalidRowsRequest;

    const fields = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (value.array.items, 0..) |field_value, i| {
        if (field_value != .string or field_value.string.len == 0 or std.mem.eql(u8, field_value.string, "*")) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (order_by[i].expression != null or order_by[i].field.len == 0 or !std.mem.eql(u8, order_by[i].field, field_value.string)) return error.InvalidRowsRequest;
        fields[initialized] = try alloc.dupe(u8, field_value.string);
        initialized += 1;
    }
    return fields;
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "op", "source", "patch", "patch_expr", "increment", "increment_expr", "json_set", "array_update", "returning", "returning_expressions" });

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
    if (source.doc_key_range != null) return error.InvalidRowsRequest;

    var operations: []db_mod.types.TransformOp = &.{};
    var patch_expressions: []db_mod.types.RelationalRowsExpressionAssignment = &.{};
    var increment_expressions: []db_mod.types.RelationalRowsExpressionAssignment = &.{};
    var json_set_expressions: []db_mod.types.RelationalRowsJsonSetExpressionAssignment = &.{};
    if (kind == .update) {
        operations = try staticUpdateTransformOperationsAlloc(alloc, schema, parsed.value, true);
        errdefer freeTransformOps(alloc, operations);
        patch_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, schema, parsed.value.object.get("patch_expr"), false, false);
        errdefer freeRowsExpressionAssignments(alloc, patch_expressions);
        increment_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, schema, parsed.value.object.get("increment_expr"), true, false);
        errdefer freeRowsExpressionAssignments(alloc, increment_expressions);
        json_set_expressions = try parseRowsMutationJsonSetExpressionAssignmentsAlloc(alloc, schema, parsed.value.object.get("json_set"), false);
        errdefer freeRowsJsonSetExpressionAssignments(alloc, json_set_expressions);
        if (operations.len == 0 and patch_expressions.len == 0 and increment_expressions.len == 0 and json_set_expressions.len == 0) return error.InvalidRowsRequest;
        try validateRowsMutationUpdateTargetPaths(operations, patch_expressions, increment_expressions, json_set_expressions, &.{});
    } else if (parsed.value.object.get("patch") != null or parsed.value.object.get("patch_expr") != null or parsed.value.object.get("increment") != null or parsed.value.object.get("increment_expr") != null or parsed.value.object.get("json_set") != null or parsed.value.object.get("array_update") != null) {
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
        .patch_expressions = patch_expressions,
        .increment_expressions = increment_expressions,
        .json_set_expressions = json_set_expressions,
        .returning = returning.fields,
        .returning_expressions = returning.expressions,
        .returning_all = returning.all,
    } };
}

pub fn parseRowsInsertSourceRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsInsertSourceRequest {
    return try parseRowsInsertSourceRequestWithSchemas(alloc, body, schema, schema);
}

pub fn parseRowsInsertSourceRequestWithSchemas(
    alloc: std.mem.Allocator,
    body: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
) !OwnedRowsInsertSourceRequest {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidRowsRequest;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "op", "source_table", "source", "assignments", "on_conflict", "returning", "returning_expressions" });

    const op_value = parsed.value.object.get("op") orelse return error.InvalidRowsRequest;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, "insert")) return error.InvalidRowsRequest;

    const source_table = try parseRowsInsertSourceTableAlloc(alloc, parsed.value.object.get("source_table"));
    errdefer if (source_table.len > 0) alloc.free(source_table);

    const source_value = parsed.value.object.get("source") orelse return error.InvalidRowsRequest;
    if (source_value != .object) return error.InvalidRowsRequest;
    const source_json = try jsonValueStringifyAlloc(alloc, source_value);
    defer alloc.free(source_json);
    var source = try parseRowsQueryRequest(alloc, source_json, source_schema);
    errdefer source.deinit(alloc);
    if (source.row_claim != null) return error.InvalidRowsRequest;
    if (source.doc_key_range != null) return error.InvalidRowsRequest;
    if (source.source_cte.len != 0) return error.InvalidRowsRequest;

    const assignments = try parseRowsInsertSourceAssignmentsWithSchemasAlloc(alloc, target_schema, source_schema, parsed.value.object.get("assignments") orelse return error.InvalidRowsRequest);
    errdefer freeRowsExpressionAssignments(alloc, assignments);

    const on_conflict: ?db_mod.types.RelationalRowsOnConflict = if (parsed.value.object.get("on_conflict")) |on_conflict_value| blk: {
        break :blk try parseRowsOnConflictAlloc(alloc, target_schema, on_conflict_value);
    } else null;
    errdefer if (on_conflict) |conflict| freeRowsOnConflict(alloc, conflict);

    const returning = try parseMutationSourceReturningAlloc(alloc, target_schema, parsed.value.object.get("returning"), parsed.value.object.get("returning_expressions"));
    errdefer {
        for (returning.fields) |field| alloc.free(field);
        if (returning.fields.len > 0) alloc.free(returning.fields);
        for (returning.expressions) |projection| {
            alloc.free(projection.output);
            freeRowsQueryExpression(alloc, projection.expression);
        }
        if (returning.expressions.len > 0) alloc.free(returning.expressions);
    }

    const req: db_mod.types.RelationalRowsInsertSourceRequest = .{
        .source_table = source_table,
        .source = source,
        .assignments = assignments,
        .on_conflict = on_conflict,
        .returning = returning.fields,
        .returning_expressions = returning.expressions,
        .returning_all = returning.all,
    };
    db_mod.DB.validateRelationalRowsInsertSourceRequestWithSchemas(target_schema, source_schema, req) catch return error.InvalidRowsRequest;
    return .{ .req = req };
}

fn parseRowsInsertSourceTableAlloc(alloc: std.mem.Allocator, maybe_source_table: ?std.json.Value) ![]const u8 {
    const value = maybe_source_table orelse return "";
    if (value != .string) return error.InvalidRowsRequest;
    if (value.string.len == 0) return "";
    for (value.string) |c| {
        const valid = std.ascii.isAlphanumeric(c) or c == '_' or c == '-';
        if (!valid) return error.InvalidRowsRequest;
    }
    return try alloc.dupe(u8, value.string);
}

pub fn parseRowsJoinedMutationSourceRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsJoinedMutationSourceRequest {
    return try parseRowsJoinedMutationSourceRequestWithSchemas(alloc, body, schema, schema);
}

pub fn parseRowsJoinedMutationSourceRequestWithSchemas(
    alloc: std.mem.Allocator,
    body: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
) !OwnedRowsJoinedMutationSourceRequest {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidRowsRequest;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "op", "source_table", "target_side", "join", "match_expression_where", "match_expression_any", "match_expression_not", "match_expression_array_contains", "source_assignments", "patch", "patch_expr", "increment", "increment_expr", "returning", "returning_expressions" });

    const op_value = parsed.value.object.get("op") orelse return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const kind: db_mod.types.RelationalRowsMutationKind = if (std.mem.eql(u8, op_value.string, "update"))
        .update
    else if (std.mem.eql(u8, op_value.string, "delete"))
        .delete
    else
        return error.InvalidRowsRequest;

    const source_table = try parseRowsJoinedMutationSourceTableAlloc(alloc, parsed.value.object.get("source_table"));
    errdefer if (source_table.len > 0) alloc.free(source_table);

    const target_side = try parseRowsJoinSide(parsed.value.object.get("target_side") orelse return error.InvalidRowsRequest);
    const left_schema = if (target_side == .left) target_schema else source_schema;
    const right_schema = if (target_side == .left) source_schema else target_schema;
    var join = try parseRowsJoinedMutationJoinAllocWithSchemas(alloc, left_schema, right_schema, parsed.value.object.get("join") orelse return error.InvalidRowsRequest, target_side);
    errdefer join.deinit(alloc);

    const match_expression_predicates = try parseRowsJoinedMutationMatchExpressionPredicatesAlloc(alloc, target_schema, source_schema, parsed.value.object.get("match_expression_where"));
    errdefer freeRowsQueryExpressionConditions(alloc, match_expression_predicates);
    const match_expression_or_predicates = try parseRowsJoinedMutationMatchExpressionPredicateGroupsAlloc(alloc, target_schema, source_schema, parsed.value.object.get("match_expression_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_or_predicates);
    const match_expression_not_predicates = try parseRowsJoinedMutationMatchExpressionPredicateGroupsAlloc(alloc, target_schema, source_schema, parsed.value.object.get("match_expression_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_not_predicates);
    const match_expression_array_contains = try parseRowsJoinedMutationMatchExpressionArrayContainsPredicatesAlloc(alloc, target_schema, source_schema, parsed.value.object.get("match_expression_array_contains"));
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, match_expression_array_contains);

    var source_assignments: []const db_mod.types.RelationalRowsJoinedMutationFieldAssignment = &.{};
    var operations: []db_mod.types.TransformOp = &.{};
    var patch_expressions: []db_mod.types.RelationalRowsExpressionAssignment = &.{};
    var increment_expressions: []db_mod.types.RelationalRowsExpressionAssignment = &.{};
    if (kind == .update) {
        source_assignments = try parseRowsJoinedMutationFieldAssignmentsAllocWithSchemas(alloc, target_schema, source_schema, parsed.value.object.get("source_assignments"), target_side);
        errdefer freeRowsJoinedMutationFieldAssignments(alloc, source_assignments);
        operations = try staticUpdateTransformOperationsAlloc(alloc, target_schema, parsed.value, false);
        errdefer freeTransformOps(alloc, operations);
        patch_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, target_schema, parsed.value.object.get("patch_expr"), false, false);
        errdefer freeRowsExpressionAssignments(alloc, patch_expressions);
        increment_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, target_schema, parsed.value.object.get("increment_expr"), true, false);
        errdefer freeRowsExpressionAssignments(alloc, increment_expressions);
        if (source_assignments.len == 0 and operations.len == 0 and patch_expressions.len == 0 and increment_expressions.len == 0) return error.InvalidRowsRequest;
        try validateRowsMutationUpdateTargetPaths(operations, patch_expressions, increment_expressions, &.{}, source_assignments);
    } else if (parsed.value.object.get("source_assignments") != null or parsed.value.object.get("patch") != null or parsed.value.object.get("patch_expr") != null or parsed.value.object.get("increment") != null or parsed.value.object.get("increment_expr") != null) {
        return error.InvalidRowsRequest;
    }

    const returning = try parseJoinedMutationSourceReturningAlloc(alloc, target_schema, source_schema, parsed.value.object.get("returning"), parsed.value.object.get("returning_expressions"));
    errdefer freeParsedMutationSourceReturning(alloc, returning);

    return .{ .req = .{
        .kind = kind,
        .source_table = source_table,
        .target_side = target_side,
        .join = join,
        .match_expression_predicates = match_expression_predicates,
        .match_expression_or_predicates = match_expression_or_predicates,
        .match_expression_not_predicates = match_expression_not_predicates,
        .match_expression_array_contains = match_expression_array_contains,
        .source_assignments = source_assignments,
        .operations = operations,
        .patch_expressions = patch_expressions,
        .increment_expressions = increment_expressions,
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "source", "group_by", "group_expressions", "aggregations", "having", "having_expressions", "having_any", "having_not", "order_by", "limit", "offset" });

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

    const group_expressions = try parseRowsQueryExpressionProjectionsAlloc(alloc, schema, parsed.value.object.get("group_expressions"));
    errdefer freeRowsQueryExpressionProjections(alloc, group_expressions);

    const aggregations = try parseRowsAggregateSpecsAlloc(alloc, schema, parsed.value.object.get("aggregations"), group_by.len + group_expressions.len > 0);
    errdefer freeRowsAggregateSpecs(alloc, aggregations);

    const output_columns = try rowsAggregateOutputColumnsAlloc(alloc, schema, group_by, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);

    const having_predicates = try parseRowsAggregateOutputPredicatesAlloc(alloc, schema, group_by, group_expressions, aggregations, parsed.value.object.get("having"));
    errdefer {
        freeQueryPredicates(alloc, having_predicates);
        if (having_predicates.len > 0) alloc.free(having_predicates);
    }

    const having_expressions = try parseRowsAggregateOutputExpressionsAlloc(alloc, schema, group_by, group_expressions, aggregations, parsed.value.object.get("having_expressions"));
    errdefer freeRowsQueryExpressionConditions(alloc, having_expressions);

    const having_any = try parseRowsAggregateOutputExpressionGroupsAlloc(alloc, schema, group_by, group_expressions, aggregations, parsed.value.object.get("having_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, having_any);

    const having_not = try parseRowsAggregateOutputExpressionGroupsAlloc(alloc, schema, group_by, group_expressions, aggregations, parsed.value.object.get("having_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, having_not);

    const order_by = try parseRowsAggregateOutputOrderAlloc(alloc, schema, group_by, group_expressions, aggregations, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .source = source,
        .group_by = group_by,
        .group_expressions = group_expressions,
        .aggregations = aggregations,
        .having_predicates = having_predicates,
        .having_expressions = having_expressions,
        .having_any = having_any,
        .having_not = having_not,
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "source", "windows", "select", "order_by", "limit", "offset" });

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

    const output_columns = try rowsWindowOutputColumnsAlloc(alloc, schema, select_parsed.fields, select_parsed.all, windows);
    defer if (output_columns.len > 0) alloc.free(output_columns);

    const order_by = try parseRowsWindowOutputOrderAlloc(alloc, schema, select_parsed.fields, select_parsed.all, windows, parsed.value.object.get("order_by"));
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "left", "right", "on", "match_expression_where", "match_expression_any", "match_expression_not", "match_expression_array_contains", "join_type", "select", "order_by", "limit", "offset" });

    var left = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("left") orelse return error.InvalidRowsRequest);
    errdefer left.deinit(alloc);
    var right = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("right") orelse return error.InvalidRowsRequest);
    errdefer right.deinit(alloc);

    const on = try parseRowsJoinOnAlloc(alloc, schema, parsed.value.object.get("on"));
    errdefer freeRowsJoinOn(alloc, on);

    const match_expression_predicates = try parseRowsJoinMatchExpressionPredicatesAlloc(alloc, schema, parsed.value.object.get("match_expression_where"));
    errdefer freeRowsQueryExpressionConditions(alloc, match_expression_predicates);
    const match_expression_or_predicates = try parseRowsJoinMatchExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("match_expression_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_or_predicates);
    const match_expression_not_predicates = try parseRowsJoinMatchExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("match_expression_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_not_predicates);
    const match_expression_array_contains = try parseRowsJoinMatchExpressionArrayContainsPredicatesAlloc(alloc, schema, parsed.value.object.get("match_expression_array_contains"));
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, match_expression_array_contains);

    const select = try parseRowsJoinProjectionsAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer freeRowsJoinProjections(alloc, select);

    const order_by = try parseRowsJoinOutputOrderAlloc(alloc, schema, schema, select, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .left = left,
        .right = right,
        .on = on,
        .match_expression_predicates = match_expression_predicates,
        .match_expression_or_predicates = match_expression_or_predicates,
        .match_expression_not_predicates = match_expression_not_predicates,
        .match_expression_array_contains = match_expression_array_contains,
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
    try requireJsonObjectOnlyKeys(parsed.value.object, &.{ "left", "right", "correlations", "match_expression_where", "match_expression_any", "match_expression_not", "match_expression_array_contains", "select", "order_by", "limit", "offset" });

    var left = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("left") orelse return error.InvalidRowsRequest);
    errdefer left.deinit(alloc);
    var right = try parseRowsJoinSourceAlloc(alloc, schema, parsed.value.object.get("right") orelse return error.InvalidRowsRequest);
    errdefer right.deinit(alloc);
    if (right.limit == null) return error.InvalidRowsRequest;

    const correlations = try parseRowsLateralCorrelationsAlloc(alloc, schema, parsed.value.object.get("correlations"));
    errdefer freeRowsLateralCorrelations(alloc, correlations);

    const match_expression_predicates = try parseRowsJoinMatchExpressionPredicatesAlloc(alloc, schema, parsed.value.object.get("match_expression_where"));
    errdefer freeRowsQueryExpressionConditions(alloc, match_expression_predicates);
    const match_expression_or_predicates = try parseRowsJoinMatchExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("match_expression_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_or_predicates);
    const match_expression_not_predicates = try parseRowsJoinMatchExpressionPredicateGroupsAlloc(alloc, schema, parsed.value.object.get("match_expression_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, match_expression_not_predicates);
    const match_expression_array_contains = try parseRowsJoinMatchExpressionArrayContainsPredicatesAlloc(alloc, schema, parsed.value.object.get("match_expression_array_contains"));
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, match_expression_array_contains);

    const select = try parseRowsJoinProjectionsAlloc(alloc, schema, parsed.value.object.get("select"));
    errdefer freeRowsJoinProjections(alloc, select);

    const order_by = try parseRowsJoinOutputOrderAlloc(alloc, schema, schema, select, parsed.value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    return .{
        .left = left,
        .right = right,
        .correlations = correlations,
        .match_expression_predicates = match_expression_predicates,
        .match_expression_or_predicates = match_expression_or_predicates,
        .match_expression_not_predicates = match_expression_not_predicates,
        .match_expression_array_contains = match_expression_array_contains,
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
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema, "query");
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    const ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("ranges"));
    errdefer freeRowsDocKeyRanges(alloc, ranges);
    try rejectRowsPlanRangeFields(parsed.value.object, &.{ "left_ranges", "right_ranges" });
    var query = try parseRowsQueryRequestFromValue(alloc, schema, parsed.value.object.get("query") orelse return error.InvalidRowsRequest);
    errdefer query.deinit(alloc);
    if (query.row_claim != null or query.doc_key_range != null) return error.InvalidRowsRequest;
    try validateRowsQuerySourceCteReference(ctes, query);
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsQueryAgainstPlannedCteOutput(planned_ctes, query);

    return .{
        .ctes = ctes,
        .ranges = ranges,
        .query = query,
    };
}

pub fn parseRowsPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsPlan {
    const operation = try detectRowsPlanOperation(alloc, body, schema);
    return switch (operation) {
        .query => .{ .query = try parseRowsQueryPlanRequest(alloc, body, schema) },
        .aggregate => .{ .aggregate = try parseRowsAggregatePlanRequest(alloc, body, schema) },
        .window => .{ .window = try parseRowsWindowPlanRequest(alloc, body, schema) },
        .join => .{ .join = try parseRowsJoinPlanRequest(alloc, body, schema) },
        .lateral => .{ .lateral = try parseRowsLateralPlanRequest(alloc, body, schema) },
    };
}

pub fn parseRowsAggregatePlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsAggregatePlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema, "aggregate");
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    const ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("ranges"));
    errdefer freeRowsDocKeyRanges(alloc, ranges);
    try rejectRowsPlanRangeFields(parsed.value.object, &.{ "left_ranges", "right_ranges" });
    var aggregate = try parseRowsAggregateRequestFromValue(alloc, schema, parsed.value.object.get("aggregate") orelse return error.InvalidRowsRequest);
    errdefer aggregate.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, aggregate.source);
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsAggregateAgainstPlannedCteOutput(planned_ctes, aggregate);

    return .{
        .ctes = ctes,
        .ranges = ranges,
        .aggregate = aggregate,
    };
}

pub fn parseRowsWindowPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsWindowPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema, "window");
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    const ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("ranges"));
    errdefer freeRowsDocKeyRanges(alloc, ranges);
    try rejectRowsPlanRangeFields(parsed.value.object, &.{ "left_ranges", "right_ranges" });
    var window = try parseRowsWindowRequestFromValue(alloc, schema, parsed.value.object.get("window") orelse return error.InvalidRowsRequest);
    errdefer window.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, window.source);
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsWindowAgainstPlannedCteOutput(planned_ctes, window);

    return .{
        .ctes = ctes,
        .ranges = ranges,
        .window = window,
    };
}

pub fn parseRowsJoinPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsJoinPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema, "join");
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    const left_ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("left_ranges"));
    errdefer freeRowsDocKeyRanges(alloc, left_ranges);
    const right_ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("right_ranges"));
    errdefer freeRowsDocKeyRanges(alloc, right_ranges);
    try rejectRowsPlanRangeFields(parsed.value.object, &.{"ranges"});
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidRowsRequest;
    var join = try parseRowsJoinRequestFromValue(alloc, schema, parsed.value.object.get("join") orelse return error.InvalidRowsRequest);
    errdefer join.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, join.left);
    try validateRowsQuerySourceCteReference(ctes, join.right);
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsJoinAgainstPlannedCteOutput(planned_ctes, join);

    return .{
        .ctes = ctes,
        .left_ranges = left_ranges,
        .right_ranges = right_ranges,
        .join = join,
    };
}

pub fn parseRowsLateralPlanRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !OwnedRowsLateralPlan {
    var parsed = try parseRowsPlanEnvelope(alloc, body, schema, "lateral");
    defer parsed.deinit();

    const ctes = try parseRowsCtesAlloc(alloc, schema, parsed.value.object.get("ctes"));
    errdefer freeRowsCtes(alloc, ctes);
    const left_ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("left_ranges"));
    errdefer freeRowsDocKeyRanges(alloc, left_ranges);
    const right_ranges = try parseRowsPlanRangesAlloc(alloc, parsed.value.object.get("right_ranges"));
    errdefer freeRowsDocKeyRanges(alloc, right_ranges);
    try rejectRowsPlanRangeFields(parsed.value.object, &.{"ranges"});
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidRowsRequest;
    var lateral = try parseRowsLateralRequestFromValue(alloc, schema, parsed.value.object.get("lateral") orelse return error.InvalidRowsRequest);
    errdefer lateral.deinit(alloc);
    try validateRowsQuerySourceCteReference(ctes, lateral.left);
    try validateRowsQuerySourceCteReference(ctes, lateral.right);
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsLateralAgainstPlannedCteOutput(planned_ctes, lateral);

    return .{
        .ctes = ctes,
        .left_ranges = left_ranges,
        .right_ranges = right_ranges,
        .lateral = lateral,
    };
}

pub fn validateRowsQueryPlanCteOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: OwnedRowsQueryPlan,
) !void {
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, plan.ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsQueryAgainstPlannedCteOutput(planned_ctes, plan.query);
}

pub fn validateRowsAggregatePlanCteOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: OwnedRowsAggregatePlan,
) !void {
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, plan.ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsAggregateAgainstPlannedCteOutput(planned_ctes, plan.aggregate);
}

pub fn validateRowsWindowPlanCteOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: OwnedRowsWindowPlan,
) !void {
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, plan.ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsWindowAgainstPlannedCteOutput(planned_ctes, plan.window);
}

pub fn validateRowsJoinPlanCteOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: OwnedRowsJoinPlan,
) !void {
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, plan.ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsJoinAgainstPlannedCteOutput(planned_ctes, plan.join);
}

pub fn validateRowsLateralPlanCteOutputAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: OwnedRowsLateralPlan,
) !void {
    const planned_ctes = try planRowsCteOutputsAlloc(alloc, schema, plan.ctes);
    defer freeRowsPlannedCtes(alloc, planned_ctes);
    try validateRowsLateralAgainstPlannedCteOutput(planned_ctes, plan.lateral);
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
    try validateRowsQueryProjectionOutputs(
        schema,
        .{ .fields = request.select, .all = request.select_all },
        request.json_extract,
        request.array_length,
        request.coalesce,
        request.field_aliases,
        request.expressions,
    );

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

    var candidate_indexes = std.ArrayListUnmanaged(usize).empty;
    defer candidate_indexes.deinit(alloc);
    if (request.distinct_on.len > 0) {
        try appendRowsQueryDistinctOnIndexesAlloc(alloc, candidates.items, request.distinct_on, &candidate_indexes);
    } else {
        try candidate_indexes.ensureUnusedCapacity(alloc, candidates.items.len);
        for (0..candidates.items.len) |i| candidate_indexes.appendAssumeCapacity(i);
    }

    const total: u32 = @intCast(candidate_indexes.items.len);
    const start = @min(@as(usize, request.offset), candidate_indexes.items.len);
    const limited_len: usize = if (request.limit) |limit|
        @min(@as(usize, limit), candidate_indexes.items.len - start)
    else
        candidate_indexes.items.len - start;

    var out_rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out_rows.items) |row| alloc.free(@constCast(row));
        out_rows.deinit(alloc);
    }
    for (candidate_indexes.items[start .. start + limited_len]) |candidate_index| {
        const candidate = candidates.items[candidate_index];
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

fn appendRowsQueryDistinctOnIndexesAlloc(
    alloc: std.mem.Allocator,
    candidates: []const QueryCandidate,
    distinct_on: []const []const u8,
    out: *std.ArrayListUnmanaged(usize),
) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer {
        var keys = seen.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        seen.deinit(alloc);
    }

    for (candidates, 0..) |candidate, i| {
        const key = try rowsQueryDistinctOnKeyJsonAlloc(alloc, candidate.row_json, distinct_on);
        var key_transferred = false;
        errdefer if (!key_transferred) alloc.free(key);
        if (seen.contains(key)) {
            alloc.free(key);
            continue;
        }
        const gop = try seen.getOrPut(alloc, key);
        if (gop.found_existing) {
            alloc.free(key);
            continue;
        }
        key_transferred = true;
        try out.append(alloc, i);
    }
}

fn rowsQueryDistinctOnKeyJsonAlloc(
    alloc: std.mem.Allocator,
    row_json: []const u8,
    distinct_on: []const []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    for (distinct_on, 0..) |field, i| {
        if (i > 0) try writer.writeByte(',');
        const selected = jsonValueAtPath(parsed.value, field) orelse {
            try writer.writeAll("null");
            continue;
        };
        try std.json.Stringify.value(selected.*, .{}, writer);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
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

fn encodeRowsResultWithTotalFieldAlloc(
    alloc: std.mem.Allocator,
    total_field: []const u8,
    total: u32,
    rows: []const []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{{f}:{d},\"rows\":[", .{ std.json.fmt(total_field, .{}), total });
    for (rows, 0..) |row, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.writeAll(row);
    }
    try writer.writeAll("]}");
    return try out.toOwnedSlice();
}

pub fn encodeRowsAggregateResponseAlloc(
    alloc: std.mem.Allocator,
    result: db_mod.types.RelationalRowsAggregateResult,
) ![]u8 {
    return try encodeRowsResultWithTotalFieldAlloc(alloc, "total_groups", result.total_groups, result.rows);
}

pub fn encodeRowsWindowResponseAlloc(
    alloc: std.mem.Allocator,
    result: db_mod.types.RelationalRowsWindowResult,
) ![]u8 {
    return try encodeRowsResultWithTotalFieldAlloc(alloc, "total_rows", result.total_rows, result.rows);
}

pub fn encodeRowsJoinResponseAlloc(
    alloc: std.mem.Allocator,
    result: db_mod.types.RelationalRowsJoinResult,
) ![]u8 {
    return try encodeRowsResultWithTotalFieldAlloc(alloc, "total_rows", result.total_rows, result.rows);
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

fn freeParsedMutationSourceReturning(alloc: std.mem.Allocator, returning: ParsedMutationSourceReturning) void {
    for (returning.fields) |field| alloc.free(field);
    if (returning.fields.len > 0) alloc.free(returning.fields);
    for (returning.expressions) |projection| {
        alloc.free(projection.output);
        freeRowsQueryExpression(alloc, projection.expression);
    }
    if (returning.expressions.len > 0) alloc.free(returning.expressions);
}

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
    allow_empty: bool,
) ![]const db_mod.types.RelationalRowsAggregateSpec {
    const aggregations_value = maybe_aggregations orelse {
        if (allow_empty) return &.{};
        return error.InvalidRowsRequest;
    };
    if (aggregations_value != .array) return error.InvalidRowsRequest;
    if (aggregations_value.array.items.len == 0) {
        if (allow_empty) return &.{};
        return error.InvalidRowsRequest;
    }
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
    try requireJsonObjectOnlyKeys(value.object, &.{ "name", "op", "field", "expr", "distinct", "distinct_max_items", "array_max_items", "array_order_by", "filter", "filter_array_any", "filter_array_contains", "filter_array_eq", "filter_in", "filter_json_contains", "filter_json_path_eq", "filter_json_path_exists", "filter_text_patterns", "filter_expressions", "filter_expression_array_contains", "filter_any", "filter_not" });
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

    const filter_array_any = try parseRowsAggregateArrayAnyPredicatesAlloc(alloc, schema, value.object.get("filter_array_any"));
    errdefer freeRowsQueryArrayAnyPredicates(alloc, filter_array_any);

    const filter_array_contains = try parseRowsAggregateArrayContainsPredicatesAlloc(alloc, schema, value.object.get("filter_array_contains"));
    errdefer freeRowsQueryArrayContainsPredicates(alloc, filter_array_contains);

    const filter_array_eq = try parseRowsAggregateArrayEqPredicatesAlloc(alloc, schema, value.object.get("filter_array_eq"));
    errdefer freeRowsQueryArrayEqPredicates(alloc, filter_array_eq);

    const filter_in_predicates = try parseRowsAggregateInPredicatesAlloc(alloc, schema, value.object.get("filter_in"));
    errdefer freeRowsQueryInPredicates(alloc, filter_in_predicates);

    const filter_json_contains = try parseRowsAggregateJsonContainsPredicatesAlloc(alloc, schema, value.object.get("filter_json_contains"));
    errdefer freeRowsQueryJsonContainsPredicates(alloc, filter_json_contains);

    const filter_json_path_eq = try parseRowsAggregateJsonPathEqPredicatesAlloc(alloc, schema, value.object.get("filter_json_path_eq"));
    errdefer freeRowsQueryJsonPathEqPredicates(alloc, filter_json_path_eq);

    const filter_json_path_exists = try parseRowsAggregateJsonPathExistsPredicatesAlloc(alloc, schema, value.object.get("filter_json_path_exists"));
    errdefer freeRowsQueryJsonPathExistsPredicates(alloc, filter_json_path_exists);

    const filter_text_patterns = try parseRowsAggregateTextPatternPredicatesAlloc(alloc, schema, value.object.get("filter_text_patterns"));
    errdefer freeRowsQueryTextPatternPredicates(alloc, filter_text_patterns);

    const filter_expressions = try parseRowsAggregateFilterExpressionsAlloc(alloc, schema, value.object.get("filter_expressions"));
    errdefer freeRowsQueryExpressionConditions(alloc, filter_expressions);

    const filter_expression_array_contains = try parseRowsQueryExpressionArrayContainsPredicatesAlloc(alloc, schema, value.object.get("filter_expression_array_contains"));
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, filter_expression_array_contains);

    const filter_any = try parseRowsQueryExpressionPredicateGroupsAlloc(alloc, schema, value.object.get("filter_any"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, filter_any);

    const filter_not = try parseRowsQueryExpressionPredicateGroupsAlloc(alloc, schema, value.object.get("filter_not"));
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, filter_not);

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
        .filter_array_any = filter_array_any,
        .filter_array_contains = filter_array_contains,
        .filter_array_eq = filter_array_eq,
        .filter_in_predicates = filter_in_predicates,
        .filter_json_contains = filter_json_contains,
        .filter_json_path_eq = filter_json_path_eq,
        .filter_json_path_exists = filter_json_path_exists,
        .filter_text_patterns = filter_text_patterns,
        .filter_expressions = filter_expressions,
        .filter_expression_array_contains = filter_expression_array_contains,
        .filter_any = filter_any,
        .filter_not = filter_not,
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
        .count, .array_agg => {},
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

fn parseRowsQueryExpressionPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_expressions: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    return try parseRowsAggregateFilterExpressionsAlloc(alloc, schema, maybe_expressions);
}

fn parseRowsQueryExpressionPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_groups: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionPredicateGroup {
    const groups_value = maybe_groups orelse return &.{};
    if (groups_value != .array or groups_value.array.items.len == 0) return error.InvalidRowsRequest;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, groups_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            freeRowsQueryExpressionConditions(alloc, group.conditions);
        }
        alloc.free(groups);
    }

    for (groups_value.array.items) |branch| {
        if (branch != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(branch.object, &.{"all"});
        const all_value = branch.object.get("all") orelse return error.InvalidRowsRequest;
        const conditions = try parseRowsQueryExpressionPredicatesAlloc(alloc, schema, all_value);
        var conditions_transferred = false;
        errdefer if (!conditions_transferred) {
            freeRowsQueryExpressionConditions(alloc, conditions);
        };
        if (conditions.len == 0) return error.InvalidRowsRequest;
        groups[initialized] = .{ .conditions = conditions };
        conditions_transferred = true;
        initialized += 1;
    }

    return groups;
}

fn parseRowsQueryExpressionArrayContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_predicates: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionArrayContainsPredicate {
    const predicates_value = maybe_predicates orelse return &.{};
    if (predicates_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionArrayContainsPredicate, predicates_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| freeRowsQueryExpressionArrayContainsPredicate(alloc, predicate);
        alloc.free(out);
    }
    for (predicates_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "expr", "value" });
        const expr_value = item.object.get("expr") orelse return error.InvalidRowsRequest;
        const value = item.object.get("value") orelse return error.InvalidRowsRequest;
        if (value != .array) return error.InvalidRowsRequest;
        const expression = try parseRowsExpressionAlloc(alloc, schema, expr_value, false);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        if ((try rowsExpressionOutputType(alloc, schema, expression)) != .array) return error.InvalidRowsRequest;
        try validateRowsStringArrayValue(value);
        const value_json = try jsonValueStringifyAlloc(alloc, value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        out[initialized] = .{
            .expression = expression,
            .value_json = value_json,
        };
        expression_transferred = true;
        value_transferred = true;
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinMatchExpressionPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_expressions: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const expressions = try parseRowsJoinedMutationMatchExpressionPredicatesAlloc(alloc, schema, schema, maybe_expressions);
    errdefer freeRowsQueryExpressionConditions(alloc, expressions);
    try validateRowsJoinMatchExpressionConditionSources(expressions);
    return expressions;
}

fn parseRowsJoinMatchExpressionPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_groups: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionPredicateGroup {
    const groups = try parseRowsJoinedMutationMatchExpressionPredicateGroupsAlloc(alloc, schema, schema, maybe_groups);
    errdefer freeRowsQueryExpressionPredicateGroups(alloc, groups);
    for (groups) |group| try validateRowsJoinMatchExpressionConditionSources(group.conditions);
    return groups;
}

fn parseRowsJoinMatchExpressionArrayContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_predicates: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionArrayContainsPredicate {
    const predicates = try parseRowsJoinedMutationMatchExpressionArrayContainsPredicatesAlloc(alloc, schema, schema, maybe_predicates);
    errdefer freeRowsQueryExpressionArrayContainsPredicates(alloc, predicates);
    for (predicates) |predicate| try validateRowsJoinMatchExpressionSources(predicate.expression);
    return predicates;
}

fn validateRowsJoinMatchExpressionConditionSources(conditions: []const db_mod.types.RelationalRowsExpressionCondition) !void {
    for (conditions) |condition| {
        if (rowsExpressionConditionUsesFieldSource(condition, .existing) or
            rowsExpressionConditionUsesFieldSource(condition, .proposed))
        {
            return error.InvalidRowsRequest;
        }
    }
}

fn validateRowsJoinMatchExpressionSources(expression: db_mod.types.RelationalRowsExpression) !void {
    if (rowsExpressionUsesFieldSource(expression, .existing) or rowsExpressionUsesFieldSource(expression, .proposed)) {
        return error.InvalidRowsRequest;
    }
}

fn parseRowsJoinedMutationMatchExpressionPredicatesAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
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
        out[initialized] = try parseRowsExpressionConditionWithSourceSchemaAlloc(alloc, target_schema, source_schema, item, true);
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinedMutationMatchExpressionPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    maybe_groups: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionPredicateGroup {
    const groups_value = maybe_groups orelse return &.{};
    if (groups_value != .array or groups_value.array.items.len == 0) return error.InvalidRowsRequest;

    const groups = try alloc.alloc(db_mod.types.RelationalRowsExpressionPredicateGroup, groups_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |group| {
            freeRowsQueryExpressionConditions(alloc, group.conditions);
        }
        alloc.free(groups);
    }

    for (groups_value.array.items) |branch| {
        if (branch != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(branch.object, &.{"all"});
        const all_value = branch.object.get("all") orelse return error.InvalidRowsRequest;
        const conditions = try parseRowsJoinedMutationMatchExpressionPredicatesAlloc(alloc, target_schema, source_schema, all_value);
        var conditions_transferred = false;
        errdefer if (!conditions_transferred) {
            freeRowsQueryExpressionConditions(alloc, conditions);
        };
        if (conditions.len == 0) return error.InvalidRowsRequest;
        groups[initialized] = .{ .conditions = conditions };
        conditions_transferred = true;
        initialized += 1;
    }

    return groups;
}

fn parseRowsJoinedMutationMatchExpressionArrayContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    maybe_predicates: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionArrayContainsPredicate {
    const predicates_value = maybe_predicates orelse return &.{};
    if (predicates_value != .array) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionArrayContainsPredicate, predicates_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| freeRowsQueryExpressionArrayContainsPredicate(alloc, predicate);
        alloc.free(out);
    }
    for (predicates_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "expr", "value" });
        const expr_value = item.object.get("expr") orelse return error.InvalidRowsRequest;
        const value = item.object.get("value") orelse return error.InvalidRowsRequest;
        if (value != .array) return error.InvalidRowsRequest;
        const expression = try parseRowsExpressionWithSourceSchemaAlloc(alloc, target_schema, source_schema, expr_value, true);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        if ((try rowsExpressionOutputTypeWithSources(alloc, target_schema, source_schema, expression)) != .array) return error.InvalidRowsRequest;
        try validateRowsStringArrayValue(value);
        const value_json = try jsonValueStringifyAlloc(alloc, value);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        out[initialized] = .{
            .expression = expression,
            .value_json = value_json,
        };
        expression_transferred = true;
        value_transferred = true;
        initialized += 1;
    }
    return out;
}

fn validateRowsStringArrayValue(value: std.json.Value) !void {
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (item != .string) return error.InvalidRowsRequest;
    }
}

fn parseRowsAggregateOutputPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    maybe_having: ?std.json.Value,
) ![]const runtime_schema.RelationalCheck {
    const having_value = maybe_having orelse return &.{};
    if (having_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(having_value.object, &.{"all"});
    const all_value = having_value.object.get("all") orelse return error.InvalidRowsRequest;
    if (all_value != .array) return error.InvalidRowsRequest;
    const output_columns = try rowsAggregateOutputColumnsAlloc(alloc, schema, group_by, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    const out = try alloc.alloc(runtime_schema.RelationalCheck, all_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeQueryPredicates(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (all_value.array.items) |atom| {
        if (atom != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "value" });
        if (rowsQueryPredicateAtomOpIsStructured(atom)) return error.InvalidRowsRequest;
        out[initialized] = try parseRowsQueryPredicateAtomAlloc(alloc, output_schema, atom);
        initialized += 1;
    }
    return out;
}

fn parseRowsAggregateOutputExpressionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    maybe_expressions: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionCondition {
    const expressions_value = maybe_expressions orelse return &.{};
    if (expressions_value != .array) return error.InvalidRowsRequest;

    const output_columns = try rowsAggregateOutputColumnsAlloc(alloc, schema, group_by, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };

    const out = try alloc.alloc(db_mod.types.RelationalRowsExpressionCondition, expressions_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeRowsQueryExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (expressions_value.array.items) |item| {
        out[initialized] = try parseRowsQueryExpressionConditionAlloc(alloc, output_schema, item);
        initialized += 1;
    }
    return out;
}

fn parseRowsAggregateOutputExpressionGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    maybe_groups: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionPredicateGroup {
    const groups_value = maybe_groups orelse return &.{};
    const output_columns = try rowsAggregateOutputColumnsAlloc(alloc, schema, group_by, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseRowsQueryExpressionPredicateGroupsAlloc(alloc, output_schema, groups_value);
}

fn rowsAggregateOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) ![]runtime_schema.RelationalColumn {
    const total = group_by.len + group_expressions.len + aggregations.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer alloc.free(out);

    for (group_by) |field| {
        if (rowsAggregateOutputColumnExists(out[0..initialized], field)) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field) orelse return error.InvalidRowsRequest;
        out[initialized] = .{
            .name = field,
            .path = field,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }

    for (group_expressions) |projection| {
        if (rowsAggregateOutputColumnExists(out[0..initialized], projection.output)) return error.InvalidRowsRequest;
        out[initialized] = try rowsAggregateExpressionOutputColumn(alloc, schema, projection);
        initialized += 1;
    }

    for (aggregations) |aggregation| {
        if (rowsAggregateOutputColumnExists(out[0..initialized], aggregation.name)) return error.InvalidRowsRequest;
        out[initialized] = .{
            .name = aggregation.name,
            .path = aggregation.name,
            .field_type = rowsAggregateOutputType(aggregation),
            .array_item_type = if (aggregation.op == .array_agg) .keyword else null,
            .nullable = false,
        };
        initialized += 1;
    }
    return out;
}

fn rowsAggregateExpressionOutputColumn(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    projection: db_mod.types.RelationalRowsExpressionProjection,
) !runtime_schema.RelationalColumn {
    return .{
        .name = projection.output,
        .path = projection.output,
        .field_type = try rowsExpressionOutputType(alloc, schema, projection.expression),
        .nullable = true,
    };
}

fn rowsExpressionOutputType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) !runtime_schema.AntflyType {
    return try rowsExpressionOutputTypeWithSources(alloc, schema, null, expression);
}

fn rowsExpressionOutputTypeWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) !runtime_schema.AntflyType {
    switch (expression.kind) {
        .field => {
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, expression.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, expression.field) orelse return error.InvalidRowsRequest;
            return column.field_type;
        },
        .value => {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            return switch (parsed.value) {
                .integer, .float, .number_string => .numeric,
                .string => .keyword,
                .bool => .boolean,
                .array => .array,
                .object => .json,
                .null => .json,
            };
        },
        .now => return .datetime,
        .lower, .upper, .trim, .replace, .concat => return .keyword,
        .nullif => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            return try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, expression.operands[0]);
        },
        .length, .abs, .round, .floor, .ceil, .mul, .div, .array_length, .interval_ns => return .numeric,
        .add, .sub => {
            if (expression.operands.len > 0 and rowsExpressionContainsInterval(expression)) {
                return try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, expression.operands[0]);
            }
            return .numeric;
        },
        .coalesce => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            return try rowsExpressionOperandOutputTypeWithSources(alloc, schema, source_schema, expression.operands);
        },
        .greatest, .least => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            return try rowsExpressionOperandOutputTypeWithSources(alloc, schema, source_schema, expression.operands);
        },
        .case => {
            return try rowsCaseExpressionOutputTypeWithSources(alloc, schema, source_schema, expression.case_branches, expression.case_else);
        },
        .cast => return switch (expression.cast_type orelse return error.InvalidRowsRequest) {
            .text => .keyword,
            .numeric => .numeric,
            .bool => .boolean,
        },
        .json_extract => return if (expression.json_as_text) .keyword else .json,
        .string_to_array => return .array,
    }
}

fn rowsExpressionOperandOutputType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    operands: []const db_mod.types.RelationalRowsExpression,
) anyerror!runtime_schema.AntflyType {
    return try rowsExpressionOperandOutputTypeWithSources(alloc, schema, null, operands);
}

fn rowsExpressionOperandOutputTypeWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    operands: []const db_mod.types.RelationalRowsExpression,
) anyerror!runtime_schema.AntflyType {
    if (operands.len == 0) return error.InvalidRowsRequest;
    for (operands) |operand| {
        if (try rowsExpressionIsNullLiteral(alloc, operand)) continue;
        return try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, operand);
    }
    return .json;
}

fn rowsCaseExpressionOutputType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    branches: []const db_mod.types.RelationalRowsExpressionCaseBranch,
    fallback: []const db_mod.types.RelationalRowsExpression,
) anyerror!runtime_schema.AntflyType {
    return try rowsCaseExpressionOutputTypeWithSources(alloc, schema, null, branches, fallback);
}

fn rowsCaseExpressionOutputTypeWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    branches: []const db_mod.types.RelationalRowsExpressionCaseBranch,
    fallback: []const db_mod.types.RelationalRowsExpression,
) anyerror!runtime_schema.AntflyType {
    if (branches.len == 0 or fallback.len != 1) return error.InvalidRowsRequest;
    var result_type: ?runtime_schema.AntflyType = null;
    for (branches) |branch| {
        try rowsMergeCaseExpressionArmTypeWithSources(alloc, schema, source_schema, branch.then, &result_type);
    }
    try rowsMergeCaseExpressionArmTypeWithSources(alloc, schema, source_schema, fallback[0], &result_type);
    return result_type orelse .json;
}

fn rowsMergeCaseExpressionArmType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
    result_type: *?runtime_schema.AntflyType,
) anyerror!void {
    return try rowsMergeCaseExpressionArmTypeWithSources(alloc, schema, null, expression, result_type);
}

fn rowsMergeCaseExpressionArmTypeWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
    result_type: *?runtime_schema.AntflyType,
) anyerror!void {
    if (try rowsExpressionIsNullLiteral(alloc, expression)) return;
    const arm_type = try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, expression);
    if (result_type.*) |existing| {
        if (!rowsExpressionResultTypesCompatible(existing, arm_type)) return error.InvalidRowsRequest;
    } else {
        result_type.* = arm_type;
    }
}

fn rowsExpressionResultTypesCompatible(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    return rowsExpressionTypesComparable(lhs, rhs);
}

fn validateRowsExpressionOperandDomains(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    return try validateRowsExpressionOperandDomainsWithSources(alloc, schema, null, expression);
}

fn validateRowsExpressionOperandDomainsWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .coalesce => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            try validateRowsExpressionSameDomainOperandsWithSources(alloc, schema, source_schema, expression.operands, false);
        },
        .nullif => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            try validateRowsExpressionSameDomainOperandsWithSources(alloc, schema, source_schema, expression.operands, false);
        },
        .greatest, .least => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            try validateRowsExpressionSameDomainOperandsWithSources(alloc, schema, source_schema, expression.operands, true);
        },
        else => return error.InvalidRowsRequest,
    }
}

fn validateRowsExpressionSameDomainOperands(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    operands: []const db_mod.types.RelationalRowsExpression,
    require_orderable: bool,
) anyerror!void {
    return try validateRowsExpressionSameDomainOperandsWithSources(alloc, schema, null, operands, require_orderable);
}

fn validateRowsExpressionSameDomainOperandsWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    operands: []const db_mod.types.RelationalRowsExpression,
    require_orderable: bool,
) anyerror!void {
    var result_type: ?runtime_schema.AntflyType = null;
    for (operands) |operand| {
        if (try rowsExpressionIsNullLiteral(alloc, operand)) continue;
        const operand_type = try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, operand);
        if (result_type) |existing| {
            if (!rowsExpressionResultTypesCompatible(existing, operand_type)) return error.InvalidRowsRequest;
        } else {
            result_type = operand_type;
        }
    }
    if (require_orderable) {
        const operand_type = result_type orelse return;
        if (!rowsExpressionTypeIsOrderable(operand_type)) return error.InvalidRowsRequest;
    }
}

fn rowsAggregateOutputColumnExists(columns: []const runtime_schema.RelationalColumn, name: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

fn rowsAggregateOutputType(aggregation: db_mod.types.RelationalRowsAggregateSpec) runtime_schema.AntflyType {
    return switch (aggregation.op) {
        .array_agg => .array,
        .count, .sum, .min, .max, .avg => .numeric,
    };
}

fn parseRowsAggregateOutputOrderAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    group_by: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    maybe_order: ?std.json.Value,
) ![]RowsQueryOrder {
    if (maybe_order == null) return &.{};
    const output_columns = try rowsAggregateOutputColumnsAlloc(alloc, schema, group_by, group_expressions, aggregations);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseRowsQueryOrderAlloc(alloc, output_schema, maybe_order);
}

fn parseRowsWindowOutputOrderAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: []const []const u8,
    select_all: bool,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    maybe_order: ?std.json.Value,
) ![]RowsQueryOrder {
    if (maybe_order == null) return &.{};
    const output_columns = try rowsWindowOutputColumnsAlloc(alloc, schema, select, select_all, windows);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseRowsQueryOrderAlloc(alloc, output_schema, maybe_order);
}

fn rowsWindowOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    select: []const []const u8,
    select_all: bool,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
) ![]runtime_schema.RelationalColumn {
    const selected_count = if (select_all) schema.relational_columns.len else select.len;
    const total = selected_count + windows.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer alloc.free(out);

    if (select_all) {
        for (schema.relational_columns) |column| {
            if (rowsAggregateOutputColumnExists(out[0..initialized], column.name)) return error.InvalidRowsRequest;
            out[initialized] = .{
                .name = column.name,
                .path = column.path,
                .field_type = column.field_type,
                .array_item_type = column.array_item_type,
                .nullable = column.nullable,
            };
            initialized += 1;
        }
    } else {
        for (select) |field| {
            if (rowsAggregateOutputColumnExists(out[0..initialized], field)) return error.InvalidRowsRequest;
            const column = findRelationalColumn(schema.relational_columns, field) orelse return error.InvalidRowsRequest;
            out[initialized] = .{
                .name = field,
                .path = field,
                .field_type = column.field_type,
                .array_item_type = column.array_item_type,
                .nullable = column.nullable,
            };
            initialized += 1;
        }
    }

    for (windows) |window| {
        if (rowsAggregateOutputColumnExists(out[0..initialized], window.output)) return error.InvalidRowsRequest;
        out[initialized] = .{
            .name = window.output,
            .path = window.output,
            .field_type = try rowsWindowOutputType(alloc, schema, window),
            .nullable = true,
        };
        initialized += 1;
    }
    return out;
}

fn rowsWindowOutputType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    window: db_mod.types.RelationalRowsWindowSpec,
) !runtime_schema.AntflyType {
    return switch (window.function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .count, .sum, .avg => .numeric,
        .lag, .lead, .first_value, .last_value, .nth_value, .min, .max => try rowsExpressionOutputType(alloc, schema, window.value_expression orelse return error.InvalidRowsRequest),
    };
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
    try requireJsonObjectOnlyKeys(value.object, &.{ "as", "function", "partition_by", "order_by", "expr", "offset", "default", "frame" });
    const output_value = value.object.get("as") orelse return error.InvalidRowsRequest;
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

    const value_expression: ?db_mod.types.RelationalRowsExpression = if (value.object.get("expr")) |expr_value|
        try parseRowsQueryExpressionAlloc(alloc, schema, expr_value)
    else
        null;
    var value_expression_transferred = false;
    errdefer if (!value_expression_transferred) if (value_expression) |expr| freeRowsQueryExpression(alloc, expr);

    const offset = try parseRowsWindowOffset(value.object.get("offset"));
    const default_json = try parseRowsWindowDefaultJsonAlloc(alloc, value.object.get("default"));
    var default_transferred = false;
    errdefer if (!default_transferred) if (default_json.len > 0) alloc.free(default_json);
    const frame = try parseRowsWindowFrame(value.object.get("frame"));
    if (frame) |parsed_frame| try validateRowsWindowRangeOffsetOrder(alloc, schema, parsed_frame, order_by);

    switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => {
            if (value_expression != null or offset != 1 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .ntile => {
            if (value_expression != null or value.object.get("offset") == null or offset == 0 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .lag, .lead => {
            if (value_expression == null or offset == 0) return error.InvalidRowsRequest;
        },
        .first_value => {
            if (value_expression == null or offset != 1 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .last_value => {
            if (value_expression == null or offset != 1 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .nth_value => {
            if (value_expression == null or value.object.get("offset") == null or offset == 0 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .count => {
            if (offset != 1 or default_json.len > 0) return error.InvalidRowsRequest;
        },
        .sum, .avg, .min, .max => {
            const expression = value_expression orelse return error.InvalidRowsRequest;
            if (offset != 1 or default_json.len > 0) return error.InvalidRowsRequest;
            try validateRowsQueryNumericExpression(alloc, schema, expression);
        },
    }

    output_transferred = true;
    partition_transferred = true;
    order_transferred = true;
    value_expression_transferred = true;
    default_transferred = true;
    return .{
        .output = output,
        .function = function,
        .partition_by = partition_by,
        .order_by = order_by,
        .value_expression = value_expression,
        .offset = offset,
        .default_json = default_json,
        .frame = frame,
    };
}

fn parseRowsWindowFunction(value: []const u8) ?db_mod.types.RelationalRowsWindowFunction {
    if (std.mem.eql(u8, value, "row_number")) return .row_number;
    if (std.mem.eql(u8, value, "rank")) return .rank;
    if (std.mem.eql(u8, value, "dense_rank")) return .dense_rank;
    if (std.mem.eql(u8, value, "percent_rank")) return .percent_rank;
    if (std.mem.eql(u8, value, "cume_dist")) return .cume_dist;
    if (std.mem.eql(u8, value, "ntile")) return .ntile;
    if (std.mem.eql(u8, value, "lag")) return .lag;
    if (std.mem.eql(u8, value, "lead")) return .lead;
    if (std.mem.eql(u8, value, "first_value")) return .first_value;
    if (std.mem.eql(u8, value, "last_value")) return .last_value;
    if (std.mem.eql(u8, value, "nth_value")) return .nth_value;
    if (std.mem.eql(u8, value, "count")) return .count;
    if (std.mem.eql(u8, value, "sum")) return .sum;
    if (std.mem.eql(u8, value, "avg")) return .avg;
    if (std.mem.eql(u8, value, "min")) return .min;
    if (std.mem.eql(u8, value, "max")) return .max;
    return null;
}

fn parseRowsWindowFrame(maybe_frame: ?std.json.Value) !?db_mod.types.RelationalRowsWindowFrame {
    const value = maybe_frame orelse return null;
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "unit", "start", "start_offset", "end", "end_offset" });
    const unit_value = value.object.get("unit") orelse return error.InvalidRowsRequest;
    const start_value = value.object.get("start") orelse return error.InvalidRowsRequest;
    const end_value = value.object.get("end") orelse return error.InvalidRowsRequest;
    if (unit_value != .string or start_value != .string or end_value != .string) return error.InvalidRowsRequest;
    const unit = if (std.mem.eql(u8, unit_value.string, "rows"))
        db_mod.types.RelationalRowsWindowFrameUnit.rows
    else if (std.mem.eql(u8, unit_value.string, "range"))
        db_mod.types.RelationalRowsWindowFrameUnit.range
    else
        return error.InvalidRowsRequest;
    const start = parseRowsWindowFrameBound(start_value.string) orelse return error.InvalidRowsRequest;
    const end = parseRowsWindowFrameBound(end_value.string) orelse return error.InvalidRowsRequest;
    if (start == .unbounded_following or end == .unbounded_preceding) return error.InvalidRowsRequest;
    const start_offset = (try parseOptionalU32(value.object.get("start_offset"))) orelse 0;
    const end_offset = (try parseOptionalU32(value.object.get("end_offset"))) orelse 0;
    try validateRowsWindowFrame(.{
        .unit = unit,
        .start = start,
        .start_offset = start_offset,
        .end = end,
        .end_offset = end_offset,
    });
    return .{
        .unit = unit,
        .start = start,
        .start_offset = start_offset,
        .end = end,
        .end_offset = end_offset,
    };
}

fn parseRowsWindowFrameBound(value: []const u8) ?db_mod.types.RelationalRowsWindowFrameBound {
    if (std.mem.eql(u8, value, "unbounded_preceding")) return .unbounded_preceding;
    if (std.mem.eql(u8, value, "offset_preceding")) return .offset_preceding;
    if (std.mem.eql(u8, value, "current_row")) return .current_row;
    if (std.mem.eql(u8, value, "offset_following")) return .offset_following;
    if (std.mem.eql(u8, value, "unbounded_following")) return .unbounded_following;
    return null;
}

fn validateRowsWindowFrame(frame: db_mod.types.RelationalRowsWindowFrame) !void {
    try validateRowsWindowFrameBoundOffset(frame.start, frame.start_offset);
    try validateRowsWindowFrameBoundOffset(frame.end, frame.end_offset);
    if (rowsWindowFrameBoundOrdinal(frame.start, frame.start_offset) > rowsWindowFrameBoundOrdinal(frame.end, frame.end_offset)) return error.InvalidRowsRequest;
}

fn validateRowsWindowFrameBoundOffset(
    bound: db_mod.types.RelationalRowsWindowFrameBound,
    offset: u32,
) !void {
    switch (bound) {
        .offset_preceding, .offset_following => {
            if (offset == 0) return error.InvalidRowsRequest;
        },
        else => if (offset != 0) return error.InvalidRowsRequest,
    }
}

fn validateRowsWindowRangeOffsetOrder(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    frame: db_mod.types.RelationalRowsWindowFrame,
    order_by: []const db_mod.types.RelationalRowsQueryOrder,
) !void {
    if (frame.unit != .range) return;
    if (frame.start != .offset_preceding and frame.start != .offset_following and frame.end != .offset_preceding and frame.end != .offset_following) return;
    if (order_by.len != 1) return error.InvalidRowsRequest;
    const order = order_by[0];
    if (order.null_test != null) return error.InvalidRowsRequest;
    if (order.expression) |expression| {
        try validateRowsQueryNumericOrDatetimeExpression(alloc, schema, expression);
        return;
    }
    if (order.field.len == 0) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, order.field) orelse return error.InvalidRowsRequest;
    if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidRowsRequest;
}

fn rowsWindowFrameBoundOrdinal(bound: db_mod.types.RelationalRowsWindowFrameBound, offset: u32) i64 {
    return switch (bound) {
        .unbounded_preceding => std.math.minInt(i64),
        .offset_preceding => -@as(i64, @intCast(offset)),
        .current_row => 0,
        .offset_following => @as(i64, @intCast(offset)),
        .unbounded_following => std.math.maxInt(i64),
    };
}

fn parseRowsWindowOffset(maybe_offset: ?std.json.Value) !u32 {
    const value = maybe_offset orelse return 1;
    if (value != .integer or value.integer <= 0 or value.integer > std.math.maxInt(u32)) return error.InvalidRowsRequest;
    return @intCast(value.integer);
}

fn parseRowsWindowDefaultJsonAlloc(
    alloc: std.mem.Allocator,
    maybe_default: ?std.json.Value,
) ![]const u8 {
    const value = maybe_default orelse return &.{};
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
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

fn parseRowsJoinOutputOrderAlloc(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
    select: []const db_mod.types.RelationalRowsJoinProjection,
    maybe_order: ?std.json.Value,
) ![]RowsQueryOrder {
    if (maybe_order == null) return &.{};
    const output_columns = try rowsJoinOutputColumnsAlloc(alloc, left_schema, right_schema, select);
    defer if (output_columns.len > 0) alloc.free(output_columns);
    const output_schema: runtime_schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = output_columns,
    };
    return try parseRowsQueryOrderAlloc(alloc, output_schema, maybe_order);
}

fn rowsJoinOutputColumnsAlloc(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
    select: []const db_mod.types.RelationalRowsJoinProjection,
) ![]runtime_schema.RelationalColumn {
    if (select.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, select.len);
    var initialized: usize = 0;
    errdefer alloc.free(out);
    for (select) |projection| {
        if (rowsAggregateOutputColumnExists(out[0..initialized], projection.output)) return error.InvalidRowsRequest;
        const projection_schema = if (projection.side == .left) left_schema else right_schema;
        const column = findRelationalColumn(projection_schema.relational_columns, projection.field) orelse return error.InvalidRowsRequest;
        out[initialized] = .{
            .name = projection.output,
            .path = projection.output,
            .field_type = column.field_type,
            .array_item_type = column.array_item_type,
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    return out;
}

fn parseRowsJoinedMutationJoinAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) !db_mod.types.RelationalRowsJoinRequest {
    return try parseRowsJoinedMutationJoinAllocWithSchemas(alloc, schema, schema, value, target_side);
}

fn parseRowsJoinedMutationJoinAllocWithSchemas(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
    value: std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) !db_mod.types.RelationalRowsJoinRequest {
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "left", "right", "on", "join_type", "select", "order_by", "limit", "offset" });

    var left = try parseRowsJoinedMutationSourceSideAlloc(alloc, left_schema, value.object.get("left") orelse return error.InvalidRowsRequest);
    errdefer left.deinit(alloc);
    var right = try parseRowsJoinedMutationSourceSideAlloc(alloc, right_schema, value.object.get("right") orelse return error.InvalidRowsRequest);
    errdefer right.deinit(alloc);

    const target = if (target_side == .left) left else right;
    const other = if (target_side == .left) right else left;
    if (target.row_claim == null) return error.InvalidRowsRequest;
    if (other.row_claim != null) return error.InvalidRowsRequest;

    const on = try parseRowsJoinOnAllocWithSchemas(alloc, left_schema, right_schema, value.object.get("on"));
    errdefer freeRowsJoinOn(alloc, on);

    const select = try parseRowsJoinProjectionsAllocWithSchemas(alloc, left_schema, right_schema, value.object.get("select"));
    errdefer freeRowsJoinProjections(alloc, select);

    const order_schema = if (target_side == .left) left_schema else right_schema;
    const order_by = try parseRowsQueryOrderAlloc(alloc, order_schema, value.object.get("order_by"));
    errdefer {
        freeRowsQueryOrder(alloc, order_by);
        if (order_by.len > 0) alloc.free(order_by);
    }

    const join_type = try parseRowsJoinType(value.object.get("join_type"));
    if (join_type != .inner) return error.InvalidRowsRequest;

    return .{
        .left = left,
        .right = right,
        .on = on,
        .join_type = join_type,
        .select = select,
        .order_by = order_by,
        .limit = try parseOptionalU32(value.object.get("limit")),
        .offset = (try parseOptionalU32(value.object.get("offset"))) orelse 0,
    };
}

fn parseRowsJoinedMutationSourceTableAlloc(alloc: std.mem.Allocator, maybe_source_table: ?std.json.Value) ![]const u8 {
    const value = maybe_source_table orelse return "";
    if (value != .string) return error.InvalidRowsRequest;
    if (value.string.len == 0) return "";
    for (value.string) |c| {
        const valid = std.ascii.isAlphanumeric(c) or c == '_' or c == '-';
        if (!valid) return error.InvalidRowsRequest;
    }
    return try alloc.dupe(u8, value.string);
}

fn parseRowsJoinedMutationSourceSideAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !OwnedRowsQueryRequest {
    if (value != .object) return error.InvalidRowsRequest;
    const source_json = try jsonValueStringifyAlloc(alloc, value);
    defer alloc.free(source_json);
    var source = try parseRowsQueryRequest(alloc, source_json, schema);
    errdefer source.deinit(alloc);
    if (source.source_cte.len != 0 or source.doc_key_range != null) return error.InvalidRowsRequest;
    return source;
}

fn parseRowsJoinOnAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_on: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsJoinOn {
    return try parseRowsJoinOnAllocWithSchemas(alloc, schema, schema, maybe_on);
}

fn parseRowsJoinOnAllocWithSchemas(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "left_field", "right_field" });
        const left_value = item.object.get("left_field") orelse return error.InvalidRowsRequest;
        const right_value = item.object.get("right_field") orelse return error.InvalidRowsRequest;
        if (left_value != .string or left_value.string.len == 0) return error.InvalidRowsRequest;
        if (right_value != .string or right_value.string.len == 0) return error.InvalidRowsRequest;
        const left_column = findRelationalColumn(left_schema.relational_columns, left_value.string) orelse return error.InvalidRowsRequest;
        const right_column = findRelationalColumn(right_schema.relational_columns, right_value.string) orelse return error.InvalidRowsRequest;
        if (left_column.field_type != right_column.field_type) return error.InvalidRowsRequest;
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

fn parseRowsJoinSide(value: std.json.Value) !db_mod.types.RelationalRowsJoinProjectionSide {
    if (value != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, value.string, "left")) return .left;
    if (std.mem.eql(u8, value.string, "right")) return .right;
    return error.InvalidRowsRequest;
}

fn parseRowsJoinProjectionsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_select: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsJoinProjection {
    return try parseRowsJoinProjectionsAllocWithSchemas(alloc, schema, schema, maybe_select);
}

fn parseRowsJoinProjectionsAllocWithSchemas(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
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
        out[initialized] = try parseRowsJoinProjectionAllocWithSchemas(alloc, left_schema, right_schema, item);
        initialized += 1;
    }
    try validateRowsJoinProjectionOutputs(out[0..initialized]);
    return out;
}

fn validateRowsJoinProjectionOutputs(select: []const db_mod.types.RelationalRowsJoinProjection) !void {
    for (select, 0..) |projection, i| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        for (select[i + 1 ..]) |other| {
            if (std.mem.eql(u8, projection.output, other.output)) return error.InvalidRowsRequest;
        }
    }
}

fn parseRowsJoinProjectionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsJoinProjection {
    return try parseRowsJoinProjectionAllocWithSchemas(alloc, schema, schema, value);
}

fn parseRowsJoinProjectionAllocWithSchemas(
    alloc: std.mem.Allocator,
    left_schema: runtime_schema.TableSchema,
    right_schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsJoinProjection {
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "as", "side", "field" });
    const output_value = value.object.get("as") orelse return error.InvalidRowsRequest;
    const side_value = value.object.get("side") orelse return error.InvalidRowsRequest;
    const field_value = value.object.get("field") orelse return error.InvalidRowsRequest;
    if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;
    if (side_value != .string) return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    const side: db_mod.types.RelationalRowsJoinProjectionSide = if (std.mem.eql(u8, side_value.string, "left"))
        .left
    else if (std.mem.eql(u8, side_value.string, "right"))
        .right
    else
        return error.InvalidRowsRequest;
    const projection_schema = if (side == .left) left_schema else right_schema;
    _ = findRelationalColumn(projection_schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
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

fn parseRowsJoinedMutationFieldAssignmentsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_source_assignments: ?std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) ![]const db_mod.types.RelationalRowsJoinedMutationFieldAssignment {
    return try parseRowsJoinedMutationFieldAssignmentsAllocWithSchemas(alloc, schema, schema, maybe_source_assignments, target_side);
}

fn parseRowsJoinedMutationFieldAssignmentsAllocWithSchemas(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    maybe_source_assignments: ?std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) ![]const db_mod.types.RelationalRowsJoinedMutationFieldAssignment {
    if (maybe_source_assignments) |assignments_value| {
        return try parseRowsJoinedMutationFieldAssignmentArrayAllocWithSchemas(alloc, target_schema, source_schema, assignments_value, target_side);
    }
    return &.{};
}

fn parseRowsJoinedMutationFieldAssignmentArrayAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    assignments_value: std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) ![]const db_mod.types.RelationalRowsJoinedMutationFieldAssignment {
    return try parseRowsJoinedMutationFieldAssignmentArrayAllocWithSchemas(alloc, schema, schema, assignments_value, target_side);
}

fn parseRowsJoinedMutationFieldAssignmentArrayAllocWithSchemas(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    assignments_value: std.json.Value,
    target_side: db_mod.types.RelationalRowsJoinProjectionSide,
) ![]const db_mod.types.RelationalRowsJoinedMutationFieldAssignment {
    if (assignments_value != .array or assignments_value.array.items.len == 0) return error.InvalidRowsRequest;
    const out = try alloc.alloc(db_mod.types.RelationalRowsJoinedMutationFieldAssignment, assignments_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |assignment| {
            alloc.free(assignment.field);
            alloc.free(assignment.source_field);
        }
        alloc.free(out);
    }

    for (assignments_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "target_field", "side", "field" });
        const target_field_value = item.object.get("target_field") orelse return error.InvalidRowsRequest;
        if (target_field_value != .string or target_field_value.string.len == 0) return error.InvalidRowsRequest;
        const target_column = findRelationalColumn(target_schema.relational_columns, target_field_value.string) orelse return error.InvalidRowsRequest;
        const source_side = try parseRowsJoinSide(item.object.get("side") orelse return error.InvalidRowsRequest);
        if (source_side == target_side) return error.InvalidRowsRequest;
        const source_field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (source_field_value != .string or source_field_value.string.len == 0) return error.InvalidRowsRequest;
        const source_column = findRelationalColumn(source_schema.relational_columns, source_field_value.string) orelse return error.InvalidRowsRequest;
        if (source_column.field_type != target_column.field_type) return error.InvalidRowsRequest;

        const field = try alloc.dupe(u8, target_field_value.string);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const source_field = try alloc.dupe(u8, source_field_value.string);
        var source_field_transferred = false;
        errdefer if (!source_field_transferred) alloc.free(source_field);

        out[initialized] = .{
            .field = field,
            .source_side = source_side,
            .source_field = source_field,
        };
        field_transferred = true;
        source_field_transferred = true;
        initialized += 1;
    }
    return out;
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "left_field", "right_field" });
        const left_value = item.object.get("left_field") orelse return error.InvalidRowsRequest;
        const right_value = item.object.get("right_field") orelse return error.InvalidRowsRequest;
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
    expected_operation: []const u8,
) !std.json.Parsed(std.json.Value) {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    try validateRowsPlanEnvelopeObject(parsed.value.object, expected_operation);
    return parsed;
}

fn detectRowsPlanOperation(
    alloc: std.mem.Allocator,
    body: []const u8,
    schema: runtime_schema.TableSchema,
) !RowsPlanOperation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidRowsRequest;
    if (body.len == 0) return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return try rowsPlanOperationFromObject(parsed.value.object);
}

fn validateRowsPlanEnvelopeObject(object: std.json.ObjectMap, expected_operation: []const u8) !void {
    if (!isRowsPlanOperationField(expected_operation)) return error.InvalidRowsRequest;
    const operation = try rowsPlanOperationFromObject(object);
    if (!std.mem.eql(u8, @tagName(operation), expected_operation)) return error.InvalidRowsRequest;
}

fn rowsPlanOperationFromObject(object: std.json.ObjectMap) !RowsPlanOperation {
    var operation: ?RowsPlanOperation = null;
    var it = object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "ctes")) continue;
        if (isRowsPlanRangeField(key)) continue;
        if (std.mem.eql(u8, key, "query")) {
            if (operation != null) return error.InvalidRowsRequest;
            operation = .query;
            continue;
        }
        if (std.mem.eql(u8, key, "aggregate")) {
            if (operation != null) return error.InvalidRowsRequest;
            operation = .aggregate;
            continue;
        }
        if (std.mem.eql(u8, key, "window")) {
            if (operation != null) return error.InvalidRowsRequest;
            operation = .window;
            continue;
        }
        if (std.mem.eql(u8, key, "join")) {
            if (operation != null) return error.InvalidRowsRequest;
            operation = .join;
            continue;
        }
        if (std.mem.eql(u8, key, "lateral")) {
            if (operation != null) return error.InvalidRowsRequest;
            operation = .lateral;
            continue;
        }
        return error.InvalidRowsRequest;
    }
    return operation orelse error.InvalidRowsRequest;
}

fn isRowsPlanOperationField(key: []const u8) bool {
    return std.mem.eql(u8, key, "query") or
        std.mem.eql(u8, key, "aggregate") or
        std.mem.eql(u8, key, "window") or
        std.mem.eql(u8, key, "join") or
        std.mem.eql(u8, key, "lateral");
}

fn isRowsPlanRangeField(key: []const u8) bool {
    return std.mem.eql(u8, key, "ranges") or
        std.mem.eql(u8, key, "left_ranges") or
        std.mem.eql(u8, key, "right_ranges");
}

fn rejectRowsPlanRangeFields(object: std.json.ObjectMap, disallowed: []const []const u8) !void {
    for (disallowed) |key| {
        if (object.get(key) != null) return error.InvalidRowsRequest;
    }
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "name", "query", "max_rows", "max_bytes" });
        const name_value = item.object.get("name") orelse return error.InvalidRowsRequest;
        const query_value = item.object.get("query") orelse return error.InvalidRowsRequest;
        if (name_value != .string or name_value.string.len == 0) return error.InvalidRowsRequest;
        if (rowsCteNameExists(out[0..initialized], name_value.string)) return error.InvalidRowsRequest;
        const max_rows = try parseOptionalU32(item.object.get("max_rows"));
        const max_bytes = try parseOptionalU64(item.object.get("max_bytes"));

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
            .max_rows = max_rows,
            .max_bytes = max_bytes,
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

const RowsPlannedCte = struct {
    name: []const u8,
    output_fields: []const []const u8,
};

fn freeRowsPlannedCtes(alloc: std.mem.Allocator, planned_ctes: []RowsPlannedCte) void {
    for (planned_ctes) |cte| freeStringSlice(alloc, cte.output_fields);
    if (planned_ctes.len > 0) alloc.free(planned_ctes);
}

fn planRowsCteOutputsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    ctes: []const db_mod.types.RelationalRowsCte,
) ![]RowsPlannedCte {
    var planned = std.ArrayListUnmanaged(RowsPlannedCte).empty;
    errdefer {
        for (planned.items) |cte| freeStringSlice(alloc, cte.output_fields);
        planned.deinit(alloc);
    }
    for (ctes) |cte| {
        try validateRowsQueryAgainstPlannedCteOutput(planned.items, cte.query);
        const output_fields = try rowsPlannedQueryOutputFieldsAlloc(alloc, schema, planned.items, cte.query);
        errdefer freeStringSlice(alloc, output_fields);
        try planned.append(alloc, .{
            .name = cte.name,
            .output_fields = output_fields,
        });
    }
    return try planned.toOwnedSlice(alloc);
}

fn rowsPlannedQueryOutputFieldsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsQueryRequest,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }

    if (req.select_all) {
        if (req.source_cte.len != 0) {
            const source = findRowsPlannedCte(planned_ctes, req.source_cte) orelse return error.InvalidRowsRequest;
            for (source.output_fields) |field| try appendRowsOutputFieldAlloc(alloc, &fields, field);
        } else {
            for (schema.relational_columns) |column| {
                try appendRowsOutputFieldAlloc(alloc, &fields, column.name);
                if (!std.mem.eql(u8, column.path, column.name)) try appendRowsOutputFieldAlloc(alloc, &fields, column.path);
            }
        }
    } else {
        for (req.select) |field| try appendRowsOutputFieldAlloc(alloc, &fields, field);
    }
    for (req.json_extract) |projection| try appendRowsOutputFieldAlloc(alloc, &fields, projection.output);
    for (req.array_length) |projection| try appendRowsOutputFieldAlloc(alloc, &fields, projection.output);
    for (req.coalesce) |projection| try appendRowsOutputFieldAlloc(alloc, &fields, projection.output);
    for (req.field_aliases) |projection| try appendRowsOutputFieldAlloc(alloc, &fields, projection.output);
    for (req.expressions) |projection| {
        if (queryProjectionOutputAlreadyRendered(req, projection.output)) continue;
        try appendRowsOutputFieldAlloc(alloc, &fields, projection.output);
    }
    return try fields.toOwnedSlice(alloc);
}

fn findRowsPlannedCte(planned_ctes: []RowsPlannedCte, name: []const u8) ?RowsPlannedCte {
    for (planned_ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

fn rowsPlannedSourceCteOutputFields(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsQueryRequest,
) ?[]const []const u8 {
    if (req.source_cte.len == 0) return null;
    const source = findRowsPlannedCte(planned_ctes, req.source_cte) orelse return &.{};
    return source.output_fields;
}

fn validateRowsQueryAgainstPlannedCteOutput(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsQueryRequest,
) !void {
    const source_output = rowsPlannedSourceCteOutputFields(planned_ctes, req) orelse return;
    try validateRowsQueryAgainstCteOutput(req, source_output);
}

fn validateRowsAggregateAgainstPlannedCteOutput(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsAggregateRequest,
) !void {
    const source_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.source) orelse return;
    try validateRowsAggregateAgainstOutputFields(source_output, req);
}

fn validateRowsWindowAgainstPlannedCteOutput(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsWindowRequest,
) !void {
    const source_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.source) orelse return;
    try validateRowsWindowAgainstOutputFields(source_output, req);
}

fn validateRowsJoinAgainstPlannedCteOutput(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsJoinRequest,
) !void {
    const left_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.left);
    const right_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.right);
    try validateRowsJoinAgainstOutputFields(left_output, right_output, req);
}

fn validateRowsLateralAgainstPlannedCteOutput(
    planned_ctes: []RowsPlannedCte,
    req: OwnedRowsLateralRequest,
) !void {
    const left_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.left);
    const right_output = rowsPlannedSourceCteOutputFields(planned_ctes, req.right);
    try validateRowsLateralAgainstOutputFields(left_output, right_output, req);
}

fn appendRowsOutputFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged([]const u8),
    field: []const u8,
) !void {
    if (field.len == 0) return error.InvalidRowsRequest;
    if (rowsCteOutputCoversField(fields.items, field)) return;
    const owned = try alloc.dupe(u8, field);
    errdefer alloc.free(owned);
    try fields.append(alloc, owned);
}

fn validateRowsQueryAgainstCteOutput(
    req: OwnedRowsQueryRequest,
    output_fields: []const []const u8,
) !void {
    for (req.predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.array_any) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.array_contains) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.array_eq) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.in_predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.json_contains) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.json_path_eq) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.json_path_exists) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.text_patterns) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (req.or_predicates) |group| {
        for (group.predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    }
    for (req.not_predicates) |group| {
        for (group.predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    }
    for (req.access_or_predicates) |group| try validateRowsAccessPredicateGroupAgainstCteOutput(output_fields, group);
    for (req.access_not_predicates) |group| try validateRowsAccessPredicateGroupAgainstCteOutput(output_fields, group);
    for (req.expression_predicates) |condition| try validateRowsExpressionConditionAgainstCteOutput(output_fields, condition);
    for (req.expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateRowsExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateRowsExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.expression_array_contains) |predicate| try validateRowsExpressionAgainstCteOutput(output_fields, predicate.expression);
    if (!req.select_all) {
        for (req.select) |field| try validateRowsCteOutputField(output_fields, field);
    }
    for (req.distinct_on) |field| try validateRowsCteOutputField(output_fields, field);
    for (req.order_by) |order| try validateRowsQueryOrderAgainstCteOutput(output_fields, order);
    for (req.json_extract) |projection| try validateRowsCteOutputField(output_fields, projection.field);
    for (req.array_length) |projection| try validateRowsCteOutputField(output_fields, projection.field);
    for (req.coalesce) |projection| {
        for (projection.operands) |operand| {
            if (operand.kind == .field) try validateRowsCteOutputField(output_fields, operand.field);
        }
    }
    for (req.field_aliases) |projection| try validateRowsCteOutputField(output_fields, projection.field);
    for (req.expressions) |projection| try validateRowsExpressionAgainstCteOutput(output_fields, projection.expression);
}

fn validateRowsAccessPredicateGroupAgainstCteOutput(
    output_fields: []const []const u8,
    group: db_mod.types.RelationalRowsAccessPredicateGroup,
) !void {
    for (group.predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.array_any) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.array_contains) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.array_eq) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.in_predicates) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.json_contains) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.json_path_eq) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.json_path_exists) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
    for (group.text_patterns) |predicate| try validateRowsCteOutputField(output_fields, predicate.field);
}

fn validateRowsAggregateAgainstOutputFields(
    source_output: []const []const u8,
    req: OwnedRowsAggregateRequest,
) !void {
    for (req.group_by) |field| try validateRowsCteOutputField(source_output, field);
    for (req.group_expressions) |projection| try validateRowsExpressionAgainstCteOutput(source_output, projection.expression);
    for (req.aggregations) |aggregation| {
        if (aggregation.field) |field| try validateRowsCteOutputField(source_output, field);
        if (aggregation.expression) |expression| try validateRowsExpressionAgainstCteOutput(source_output, expression);
        for (aggregation.array_order_by) |order| try validateRowsQueryOrderAgainstCteOutput(source_output, order);
        for (aggregation.filter_predicates) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_array_any) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_array_contains) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_array_eq) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_in_predicates) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_json_contains) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_json_path_eq) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_json_path_exists) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_text_patterns) |predicate| try validateRowsCteOutputField(source_output, predicate.field);
        for (aggregation.filter_expressions) |condition| try validateRowsExpressionConditionAgainstCteOutput(source_output, condition);
        for (aggregation.filter_expression_array_contains) |predicate| try validateRowsExpressionAgainstCteOutput(source_output, predicate.expression);
        for (aggregation.filter_any) |group| {
            for (group.conditions) |condition| try validateRowsExpressionConditionAgainstCteOutput(source_output, condition);
        }
        for (aggregation.filter_not) |group| {
            for (group.conditions) |condition| try validateRowsExpressionConditionAgainstCteOutput(source_output, condition);
        }
    }
}

fn validateRowsWindowAgainstOutputFields(
    source_output: []const []const u8,
    req: OwnedRowsWindowRequest,
) !void {
    for (req.windows) |window| {
        for (window.partition_by) |field| try validateRowsCteOutputField(source_output, field);
        for (window.order_by) |order| try validateRowsQueryOrderAgainstCteOutput(source_output, order);
        if (window.value_expression) |expression| try validateRowsExpressionAgainstCteOutput(source_output, expression);
    }
    if (!req.select_all) {
        for (req.select) |field| try validateRowsCteOutputField(source_output, field);
    }
}

fn validateRowsJoinAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    req: OwnedRowsJoinRequest,
) !void {
    for (req.on) |join_on| {
        if (left_output) |fields| try validateRowsCteOutputField(fields, join_on.left_field);
        if (right_output) |fields| try validateRowsCteOutputField(fields, join_on.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (left_output) |fields| try validateRowsCteOutputField(fields, projection.field),
        .right => if (right_output) |fields| try validateRowsCteOutputField(fields, projection.field),
    };
    try validateRowsJoinMatchExpressionsAgainstOutputFields(left_output, right_output, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

fn validateRowsLateralAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    req: OwnedRowsLateralRequest,
) !void {
    for (req.correlations) |correlation| {
        if (left_output) |fields| try validateRowsCteOutputField(fields, correlation.left_field);
        if (right_output) |fields| try validateRowsCteOutputField(fields, correlation.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (left_output) |fields| try validateRowsCteOutputField(fields, projection.field),
        .right => if (right_output) |fields| try validateRowsCteOutputField(fields, projection.field),
    };
    try validateRowsJoinMatchExpressionsAgainstOutputFields(left_output, right_output, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

fn validateRowsJoinMatchExpressionsAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    predicates: []const db_mod.types.RelationalRowsExpressionCondition,
    any_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
    not_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
    array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) !void {
    for (predicates) |condition| try validateRowsJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    for (any_groups) |group| {
        for (group.conditions) |condition| try validateRowsJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    }
    for (not_groups) |group| {
        for (group.conditions) |condition| try validateRowsJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    }
    for (array_contains) |predicate| try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, predicate.expression);
}

fn validateRowsJoinMatchExpressionConditionAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, condition.lhs);
    for (condition.rhs) |expression| try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, expression);
}

fn validateRowsJoinMatchExpressionAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        switch (expression.field_source) {
            .source => if (right_output) |fields| try validateRowsCteOutputField(fields, expression.field),
            .row => if (left_output) |fields| try validateRowsCteOutputField(fields, expression.field),
            .existing, .proposed => return error.InvalidRowsRequest,
        }
    }
    for (expression.operands) |operand| try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, operand);
    for (expression.case_branches) |branch| {
        try validateRowsJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, branch.when);
        try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, branch.then);
    }
    for (expression.case_else) |fallback| try validateRowsJoinMatchExpressionAgainstOutputFields(left_output, right_output, fallback);
}

fn validateRowsQueryOrderAgainstCteOutput(
    output_fields: []const []const u8,
    order: RowsQueryOrder,
) !void {
    if (order.field.len > 0) try validateRowsCteOutputField(output_fields, order.field);
    if (order.expression) |expression| try validateRowsExpressionAgainstCteOutput(output_fields, expression);
}

fn validateRowsExpressionConditionAgainstCteOutput(
    output_fields: []const []const u8,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateRowsExpressionAgainstCteOutput(output_fields, condition.lhs);
    for (condition.rhs) |expression| try validateRowsExpressionAgainstCteOutput(output_fields, expression);
}

fn validateRowsExpressionAgainstCteOutput(
    output_fields: []const []const u8,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field and expression.field_source != .proposed) {
        try validateRowsCteOutputField(output_fields, expression.field);
    }
    for (expression.operands) |operand| try validateRowsExpressionAgainstCteOutput(output_fields, operand);
    for (expression.case_branches) |branch| {
        try validateRowsExpressionConditionAgainstCteOutput(output_fields, branch.when);
        try validateRowsExpressionAgainstCteOutput(output_fields, branch.then);
    }
    for (expression.case_else) |fallback| try validateRowsExpressionAgainstCteOutput(output_fields, fallback);
}

fn validateRowsCteOutputField(output_fields: []const []const u8, field: []const u8) !void {
    if (field.len == 0 or !rowsCteOutputCoversField(output_fields, field)) return error.InvalidRowsRequest;
}

fn rowsCteOutputCoversField(output_fields: []const []const u8, field: []const u8) bool {
    for (output_fields) |output| {
        if (std.mem.eql(u8, output, field)) return true;
        if (field.len > output.len and std.mem.startsWith(u8, field, output) and field[output.len] == '.') return true;
    }
    return false;
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
    try validateRowsWhereEnvelope(where_value);

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

    if (where_value.object.count() == 0) return &.{};
    return error.InvalidRowsRequest;
}

fn parseRowsQueryOrPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsPredicateGroup {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;
    try validateRowsWhereEnvelope(where_value);
    const any_value = where_value.object.get("any") orelse return &.{};
    if (any_value != .array or any_value.array.items.len == 0) return error.InvalidRowsRequest;
    for (any_value.array.items) |branch| {
        if (rowsQueryPredicateBranchHasStructuredAtom(branch)) return &.{};
    }

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
    try validateRowsWhereEnvelope(where_value);
    const not_value = where_value.object.get("not") orelse return &.{};
    if (not_value != .array or not_value.array.items.len == 0) return error.InvalidRowsRequest;
    for (not_value.array.items) |branch| {
        if (rowsQueryPredicateBranchHasStructuredAtom(branch)) return &.{};
    }

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
        try requireJsonObjectOnlyKeys(branch.object, &.{"all"});
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

fn validateRowsWhereEnvelope(where_value: std.json.Value) !void {
    if (where_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(where_value.object, &.{ "field", "op", "value", "path", "pattern", "case_insensitive", "negated", "all", "any", "not" });
    const has_atom_key = where_value.object.get("field") != null or
        where_value.object.get("op") != null or
        where_value.object.get("value") != null or
        where_value.object.get("path") != null or
        where_value.object.get("pattern") != null or
        where_value.object.get("case_insensitive") != null or
        where_value.object.get("negated") != null;
    const has_group_key = where_value.object.get("all") != null or where_value.object.get("any") != null or where_value.object.get("not") != null;
    if (!has_atom_key and !has_group_key) return error.InvalidRowsRequest;
    if (has_atom_key and has_group_key) return error.InvalidRowsRequest;
    if (has_atom_key) {
        const field_value = where_value.object.get("field") orelse return error.InvalidRowsRequest;
        const op_value = where_value.object.get("op") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        if (op_value != .string or op_value.string.len == 0) return error.InvalidRowsRequest;
    }
    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array or all_value.array.items.len == 0) return error.InvalidRowsRequest;
    }
    if (where_value.object.get("any")) |any_value| {
        if (any_value != .array or any_value.array.items.len == 0) return error.InvalidRowsRequest;
    }
    if (where_value.object.get("not")) |not_value| {
        if (not_value != .array or not_value.array.items.len == 0) return error.InvalidRowsRequest;
    }
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
        std.mem.eql(u8, op_value.string, "json_path_exists") or
        std.mem.eql(u8, op_value.string, "text_pattern");
}

fn rowsQueryPredicateBranchHasStructuredAtom(branch: std.json.Value) bool {
    if (branch != .object) return false;
    if (branch.object.get("all")) |all_value| {
        if (all_value != .array) return false;
        for (all_value.array.items) |atom| {
            if (rowsQueryPredicateAtomOpIsStructured(atom)) return true;
        }
        return false;
    }
    return rowsQueryPredicateAtomOpIsStructured(branch);
}

fn parseRowsQueryAccessPredicateGroupsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
    group_key: []const u8,
) ![]db_mod.types.RelationalRowsAccessPredicateGroup {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;
    try validateRowsWhereEnvelope(where_value);
    const group_value = where_value.object.get(group_key) orelse return &.{};
    if (group_value != .array or group_value.array.items.len == 0) return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup).empty;
    errdefer {
        freeRowsQueryAccessPredicateGroupsNoSlice(alloc, out.items);
        out.deinit(alloc);
    }
    for (group_value.array.items) |branch| {
        if (!rowsQueryPredicateBranchHasStructuredAtom(branch)) continue;
        try out.append(alloc, try parseRowsQueryAccessPredicateGroupAlloc(alloc, schema, branch));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryAccessPredicateGroupAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    branch: std.json.Value,
) !db_mod.types.RelationalRowsAccessPredicateGroup {
    if (branch != .object) return error.InvalidRowsRequest;
    var scalar = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
    var array_any = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate).empty;
    var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
    var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
    var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
    var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
    var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    errdefer {
        freeQueryPredicates(alloc, scalar.items);
        scalar.deinit(alloc);
        freeRowsQueryArrayAnyPredicatesNoSlice(alloc, array_any.items);
        array_any.deinit(alloc);
        freeRowsQueryArrayContainsPredicatesNoSlice(alloc, array_contains.items);
        array_contains.deinit(alloc);
        freeRowsQueryArrayEqPredicatesNoSlice(alloc, array_eq.items);
        array_eq.deinit(alloc);
        freeRowsQueryInPredicatesNoSlice(alloc, in_predicates.items);
        in_predicates.deinit(alloc);
        freeRowsQueryJsonContainsPredicatesNoSlice(alloc, json_contains.items);
        json_contains.deinit(alloc);
        freeRowsQueryJsonPathEqPredicatesNoSlice(alloc, json_path_eq.items);
        json_path_eq.deinit(alloc);
        freeRowsQueryJsonPathExistsPredicatesNoSlice(alloc, json_path_exists.items);
        json_path_exists.deinit(alloc);
        freeRowsQueryTextPatternPredicatesNoSlice(alloc, text_patterns.items);
        text_patterns.deinit(alloc);
    }
    if (branch.object.get("all")) |all_value| {
        try requireJsonObjectOnlyKeys(branch.object, &.{"all"});
        if (all_value != .array or all_value.array.items.len == 0) return error.InvalidRowsRequest;
        for (all_value.array.items) |atom| try appendRowsQueryAccessPredicateAtom(alloc, schema, atom, &scalar, &array_any, &array_contains, &array_eq, &in_predicates, &json_contains, &json_path_eq, &json_path_exists, &text_patterns);
    } else {
        try appendRowsQueryAccessPredicateAtom(alloc, schema, branch, &scalar, &array_any, &array_contains, &array_eq, &in_predicates, &json_contains, &json_path_eq, &json_path_exists, &text_patterns);
    }
    if (scalar.items.len == 0 and array_any.items.len == 0 and array_contains.items.len == 0 and array_eq.items.len == 0 and in_predicates.items.len == 0 and json_contains.items.len == 0 and json_path_eq.items.len == 0 and json_path_exists.items.len == 0 and text_patterns.items.len == 0) return error.InvalidRowsRequest;
    return .{
        .predicates = try scalar.toOwnedSlice(alloc),
        .array_any = try array_any.toOwnedSlice(alloc),
        .array_contains = try array_contains.toOwnedSlice(alloc),
        .array_eq = try array_eq.toOwnedSlice(alloc),
        .in_predicates = try in_predicates.toOwnedSlice(alloc),
        .json_contains = try json_contains.toOwnedSlice(alloc),
        .json_path_eq = try json_path_eq.toOwnedSlice(alloc),
        .json_path_exists = try json_path_exists.toOwnedSlice(alloc),
        .text_patterns = try text_patterns.toOwnedSlice(alloc),
    };
}

fn appendRowsQueryAccessPredicateAtom(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
    scalar: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    array_any: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate),
    array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
    array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
    in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
    json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
    json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
    json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
    text_patterns: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate),
) !void {
    if (rowsQueryPredicateAtomOpEquals(atom, "array_any")) {
        try array_any.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayAnyPredicate, alloc, schema, atom, "array_any", .array));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "array_contains")) {
        const predicate = try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayContainsPredicate, alloc, schema, atom, "array_contains", .array);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
        try array_contains.append(alloc, predicate);
    } else if (rowsQueryPredicateAtomOpEquals(atom, "array_eq")) {
        const predicate = try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayEqPredicate, alloc, schema, atom, "array_eq", .array);
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
        try array_eq.append(alloc, predicate);
    } else if (rowsQueryPredicateAtomOpEquals(atom, "in")) {
        try in_predicates.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, false));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "not_in")) {
        try in_predicates.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, true));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "json_contains")) {
        try json_contains.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsJsonContainsPredicate, alloc, schema, atom, "json_contains", .json));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "json_path_eq")) {
        try json_path_eq.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(db_mod.types.RelationalRowsJsonPathEqPredicate, alloc, schema, atom, "json_path_eq"));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "json_path_exists")) {
        try json_path_exists.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(db_mod.types.RelationalRowsJsonPathExistsPredicate, alloc, schema, atom, "json_path_exists"));
    } else if (rowsQueryPredicateAtomOpEquals(atom, "text_pattern")) {
        try text_patterns.append(alloc, try parseRowsQueryTextPatternPredicateAtomAlloc(alloc, schema, atom));
    } else {
        try scalar.append(alloc, try parseRowsQueryPredicateAtomAlloc(alloc, schema, atom));
    }
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
    try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "value" });
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type == .array or column.field_type == .json) return error.InvalidRowsRequest;
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (!rowsScalarValueMatches(column.field_type, item)) return error.InvalidRowsRequest;
    }
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

fn rowsScalarValueMatches(field_type: runtime_schema.AntflyType, value: std.json.Value) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => value == .string,
        .numeric => value == .integer or value == .float or value == .number_string,
        .datetime => value == .integer or value == .float or value == .number_string,
        .boolean => value == .bool,
        .geopoint => value == .array or value == .object,
        .json, .array, .embedding => false,
    };
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

fn parseRowsAggregateArrayAnyPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayAnyPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate).empty;
    errdefer {
        freeRowsQueryArrayAnyPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        try out.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayAnyPredicate, alloc, schema, atom, "array_any", .array));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateArrayContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayContainsPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
    errdefer {
        freeRowsQueryArrayContainsPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        const predicate = try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayContainsPredicate, alloc, schema, atom, "array_contains", .array);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        };
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
        try out.append(alloc, predicate);
        predicate_transferred = true;
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateArrayEqPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsArrayEqPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
    errdefer {
        freeRowsQueryArrayEqPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        const predicate = try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsArrayEqPredicate, alloc, schema, atom, "array_eq", .array);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) {
            alloc.free(predicate.field);
            alloc.free(predicate.value_json);
        };
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidRowsRequest;
        try out.append(alloc, predicate);
        predicate_transferred = true;
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateInPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsInPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    errdefer {
        freeRowsQueryInPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        if (rowsQueryPredicateAtomOpEquals(atom, "in")) {
            try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, false));
        } else if (rowsQueryPredicateAtomOpEquals(atom, "not_in")) {
            try out.append(alloc, try parseRowsQueryInPredicateAtomAlloc(alloc, schema, atom, true));
        } else {
            return error.InvalidRowsRequest;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateJsonContainsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonContainsPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
    errdefer {
        freeRowsQueryJsonContainsPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        try out.append(alloc, try parseRowsQueryStructuredPredicateAtomAlloc(db_mod.types.RelationalRowsJsonContainsPredicate, alloc, schema, atom, "json_contains", .json));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateJsonPathEqPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonPathEqPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
    errdefer {
        freeRowsQueryJsonPathEqPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        try out.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(db_mod.types.RelationalRowsJsonPathEqPredicate, alloc, schema, atom, "json_path_eq"));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateJsonPathExistsPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsJsonPathExistsPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
    errdefer {
        freeRowsQueryJsonPathExistsPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        try out.append(alloc, try parseRowsQueryJsonPathPredicateAtomAlloc(db_mod.types.RelationalRowsJsonPathExistsPredicate, alloc, schema, atom, "json_path_exists"));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsAggregateTextPatternPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_filters: ?std.json.Value,
) ![]db_mod.types.RelationalRowsTextPatternPredicate {
    const filters = maybe_filters orelse return &.{};
    if (filters != .array) return error.InvalidRowsRequest;
    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    errdefer {
        freeRowsQueryTextPatternPredicates(alloc, out.items);
        out.deinit(alloc);
    }
    for (filters.array.items) |atom| {
        if (!rowsQueryPredicateAtomOpEquals(atom, "text_pattern")) return error.InvalidRowsRequest;
        try out.append(alloc, try parseRowsQueryTextPatternPredicateAtomAlloc(alloc, schema, atom));
    }
    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryTextPatternPredicatesAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
) ![]db_mod.types.RelationalRowsTextPatternPredicate {
    const where_value = maybe_where orelse return &.{};
    if (where_value != .object) return error.InvalidRowsRequest;

    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    errdefer {
        freeRowsQueryTextPatternPredicates(alloc, out.items);
        out.deinit(alloc);
    }

    if (where_value.object.get("all")) |all_value| {
        if (all_value != .array) return error.InvalidRowsRequest;
        for (all_value.array.items) |atom| {
            if (!rowsQueryPredicateAtomOpEquals(atom, "text_pattern")) continue;
            try out.append(alloc, try parseRowsQueryTextPatternPredicateAtomAlloc(alloc, schema, atom));
        }
    } else if (rowsQueryPredicateAtomOpEquals(where_value, "text_pattern")) {
        try out.append(alloc, try parseRowsQueryTextPatternPredicateAtomAlloc(alloc, schema, where_value));
    }

    return try out.toOwnedSlice(alloc);
}

fn parseRowsQueryTextPatternPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
) !db_mod.types.RelationalRowsTextPatternPredicate {
    if (atom != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "pattern", "case_insensitive", "negated" });
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const pattern_value = atom.object.get("pattern") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (pattern_value != .string) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidRowsRequest;
    const case_insensitive = if (atom.object.get("case_insensitive")) |value| blk: {
        if (value != .bool) return error.InvalidRowsRequest;
        break :blk value.bool;
    } else false;
    const negated = if (atom.object.get("negated")) |value| blk: {
        if (value != .bool) return error.InvalidRowsRequest;
        break :blk value.bool;
    } else false;

    const field = try alloc.dupe(u8, field_value.string);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const pattern = try alloc.dupe(u8, pattern_value.string);
    var pattern_transferred = false;
    errdefer if (!pattern_transferred) alloc.free(pattern);
    field_transferred = true;
    pattern_transferred = true;
    return .{
        .field = field,
        .pattern = pattern,
        .case_insensitive = case_insensitive,
        .negated = negated,
    };
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
    if (comptime @hasField(Predicate, "value_json")) {
        try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "path", "value" });
    } else {
        try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "path" });
    }
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
    try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "value" });
    const field_value = atom.object.get("field") orelse return error.InvalidRowsRequest;
    const op_value = atom.object.get("op") orelse return error.InvalidRowsRequest;
    const value = atom.object.get("value") orelse return error.InvalidRowsRequest;
    if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, op_name)) return error.InvalidRowsRequest;
    const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
    if (column.field_type != expected_type) return error.InvalidRowsRequest;
    if (expected_type == .array) try validateRowsArrayPredicateValue(column, op_name, value);

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

fn validateRowsArrayPredicateValue(column: runtime_schema.RelationalColumn, op_name: []const u8, value: std.json.Value) !void {
    const item_type = column.array_item_type orelse return error.InvalidRowsRequest;
    if (std.mem.eql(u8, op_name, "array_any")) {
        if (!rowsArrayItemValueMatches(item_type, value)) return error.InvalidRowsRequest;
        return;
    }
    if (!std.mem.eql(u8, op_name, "array_contains") and !std.mem.eql(u8, op_name, "array_eq")) return error.InvalidRowsRequest;
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (!rowsArrayItemValueMatches(item_type, item)) return error.InvalidRowsRequest;
    }
}

fn parseRowsQueryPredicateAtomAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    atom: std.json.Value,
) !runtime_schema.RelationalCheck {
    if (atom != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(atom.object, &.{ "field", "op", "value" });
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
    if (std.mem.eql(u8, op_text, "is_distinct")) return .is_distinct;
    if (std.mem.eql(u8, op_text, "is_not_distinct")) return .is_not_distinct;
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
        .is_distinct, .is_not_distinct, .eq, .ne, .gt, .gte, .lt, .lte => true,
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

fn validateRowsQueryProjectionOutputs(
    schema: runtime_schema.TableSchema,
    select_parsed: ParsedRowsQuerySelect,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!select_parsed.all) {
        for (select_parsed.fields) |field| {
            if (field.len == 0) return error.InvalidRowsRequest;
            if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, field) > 1) return error.InvalidRowsRequest;
        }
    }
    for (json_extract) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
    for (array_length) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
    for (coalesce) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
    for (field_aliases) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (rowsQueryProjectionOutputCount(schema, select_parsed, json_extract, array_length, coalesce, field_aliases, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
}

fn rowsQueryProjectionOutputCount(
    schema: runtime_schema.TableSchema,
    select_parsed: ParsedRowsQuerySelect,
    json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection,
    array_length: []const db_mod.types.RelationalRowsArrayLengthProjection,
    coalesce: []const db_mod.types.RelationalRowsCoalesceProjection,
    field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    output: []const u8,
) usize {
    var count: usize = 0;
    if (!select_parsed.all) {
        for (select_parsed.fields) |field| {
            if (std.mem.eql(u8, field, output)) count += 1;
        }
    } else {
        for (schema.relational_columns) |column| {
            if (std.mem.eql(u8, column.name, output)) count += 1;
            if (!std.mem.eql(u8, column.path, column.name) and std.mem.eql(u8, column.path, output)) count += 1;
        }
    }
    for (json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (array_length) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

fn parseMutationSourceReturningAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_returning: ?std.json.Value,
    maybe_expressions: ?std.json.Value,
) !ParsedMutationSourceReturning {
    const expressions = try parseRowsQueryExpressionProjectionsAlloc(alloc, schema, maybe_expressions);
    errdefer freeRowsQueryExpressionProjections(alloc, expressions);
    const returning_value = maybe_returning orelse {
        try validateMutationReturningOutputs(schema, &.{}, false, expressions);
        return .{ .expressions = expressions };
    };
    if (returning_value != .array or returning_value.array.items.len == 0) return error.InvalidRowsRequest;
    if (returning_value.array.items.len == 1 and returning_value.array.items[0] == .string and std.mem.eql(u8, returning_value.array.items[0].string, "*")) {
        try validateMutationReturningOutputs(schema, &.{}, true, expressions);
        return .{ .fields = &.{}, .expressions = expressions, .all = true };
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
    try validateMutationReturningOutputs(schema, fields, false, expressions);
    return .{ .fields = fields, .expressions = expressions, .all = false };
}

fn parseJoinedMutationSourceReturningAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    maybe_returning: ?std.json.Value,
    maybe_expressions: ?std.json.Value,
) !ParsedMutationSourceReturning {
    const expressions = try parseJoinedMutationSourceReturningExpressionsAlloc(alloc, target_schema, source_schema, maybe_expressions);
    errdefer freeRowsQueryExpressionProjections(alloc, expressions);
    const returning_value = maybe_returning orelse {
        try validateMutationReturningOutputs(target_schema, &.{}, false, expressions);
        return .{ .expressions = expressions };
    };
    if (returning_value != .array or returning_value.array.items.len == 0) return error.InvalidRowsRequest;
    if (returning_value.array.items.len == 1 and returning_value.array.items[0] == .string and std.mem.eql(u8, returning_value.array.items[0].string, "*")) {
        try validateMutationReturningOutputs(target_schema, &.{}, true, expressions);
        return .{ .fields = &.{}, .expressions = expressions, .all = true };
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
    try validateMutationReturningOutputs(target_schema, fields, false, expressions);
    return .{ .fields = fields, .expressions = expressions, .all = false };
}

fn parseJoinedMutationSourceReturningExpressionsAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "expr" });
        const output_value = item.object.get("as") orelse return error.InvalidRowsRequest;
        const expression_value = item.object.get("expr") orelse return error.InvalidRowsRequest;
        if (output_value != .string or output_value.string.len == 0) return error.InvalidRowsRequest;

        const output = try alloc.dupe(u8, output_value.string);
        var output_transferred = false;
        errdefer if (!output_transferred) alloc.free(output);
        const expression = try parseJoinedMutationSourceReturningExpressionAlloc(alloc, target_schema, source_schema, expression_value);
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

fn parseJoinedMutationSourceReturningExpressionAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    value: std.json.Value,
) !db_mod.types.RelationalRowsExpression {
    if (jsonRowsExpressionUsesSourceField(value)) {
        const expression = try parseRowsExpressionAlloc(alloc, source_schema, value, true);
        errdefer freeRowsQueryExpression(alloc, expression);
        try validateJoinedMutationSourceReturningExpression(expression);
        return expression;
    }
    return try parseRowsMutationExpressionAlloc(alloc, target_schema, value);
}

fn jsonRowsExpressionUsesSourceField(value: std.json.Value) bool {
    return switch (value) {
        .object => |object| blk: {
            if (object.get("source")) |source| {
                if (source == .string and std.mem.eql(u8, source.string, "source")) break :blk true;
            }
            var fields = object.iterator();
            while (fields.next()) |entry| {
                if (jsonRowsExpressionUsesSourceField(entry.value_ptr.*)) break :blk true;
            }
            break :blk false;
        },
        .array => |array| blk: {
            for (array.items) |item| {
                if (jsonRowsExpressionUsesSourceField(item)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn validateJoinedMutationSourceReturningExpression(expression: db_mod.types.RelationalRowsExpression) anyerror!void {
    if (expression.kind == .field and expression.field_source != .source) return error.InvalidRowsRequest;
    for (expression.operands) |operand| try validateJoinedMutationSourceReturningExpression(operand);
    for (expression.case_branches) |branch| {
        try validateJoinedMutationSourceReturningCondition(branch.when);
        try validateJoinedMutationSourceReturningExpression(branch.then);
    }
    for (expression.case_else) |case_else| try validateJoinedMutationSourceReturningExpression(case_else);
}

fn validateJoinedMutationSourceReturningCondition(condition: db_mod.types.RelationalRowsExpressionCondition) anyerror!void {
    try validateJoinedMutationSourceReturningExpression(condition.lhs);
    for (condition.rhs) |rhs| try validateJoinedMutationSourceReturningExpression(rhs);
}

fn validateMutationReturningOutputs(
    schema: runtime_schema.TableSchema,
    fields: []const []const u8,
    returning_all: bool,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (returning_all and fields.len != 0) return error.InvalidRowsRequest;
    for (fields) |field| {
        if (field.len == 0) return error.InvalidRowsRequest;
        if (mutationReturningOutputCount(fields, expressions, field) > 1) return error.InvalidRowsRequest;
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (returning_all and findRelationalColumn(schema.relational_columns, projection.output) != null) return error.InvalidRowsRequest;
        if (mutationReturningOutputCount(fields, expressions, projection.output) > 1) return error.InvalidRowsRequest;
    }
}

fn mutationReturningOutputCount(
    fields: []const []const u8,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    output: []const u8,
) usize {
    var count: usize = 0;
    for (fields) |field| {
        if (std.mem.eql(u8, field, output)) count += 1;
    }
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "field", "path", "as_text" });
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "field" });
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "operands" });
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
            try requireJsonObjectOnlyKeys(operand_value.object, &.{ "field", "value" });
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "expr" });
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

fn requireJsonObjectOnlyKeys(object: std.json.ObjectMap, comptime allowed_keys: []const []const u8) !void {
    var it = object.iterator();
    while (it.next()) |entry| {
        var found = false;
        inline for (allowed_keys) |allowed| {
            if (std.mem.eql(u8, entry.key_ptr.*, allowed)) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidRowsRequest;
    }
}

fn parseRowsQueryExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) anyerror!db_mod.types.RelationalRowsExpression {
    return try parseRowsExpressionAlloc(alloc, schema, value, false);
}

fn parseRowsMutationExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
) anyerror!db_mod.types.RelationalRowsExpression {
    const expression = try parseRowsExpressionAlloc(alloc, schema, value, true);
    errdefer freeRowsQueryExpression(alloc, expression);
    if (rowsExpressionUsesFieldSource(expression, .source)) return error.InvalidRowsRequest;
    return expression;
}

fn rowsExpressionUsesFieldSource(
    expression: db_mod.types.RelationalRowsExpression,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) bool {
    if (expression.kind == .field and expression.field_source == field_source) return true;
    for (expression.operands) |operand| {
        if (rowsExpressionUsesFieldSource(operand, field_source)) return true;
    }
    for (expression.case_branches) |branch| {
        if (rowsExpressionConditionUsesFieldSource(branch.when, field_source)) return true;
        if (rowsExpressionUsesFieldSource(branch.then, field_source)) return true;
    }
    for (expression.case_else) |case_else| {
        if (rowsExpressionUsesFieldSource(case_else, field_source)) return true;
    }
    return false;
}

fn rowsExpressionConditionUsesFieldSource(
    condition: db_mod.types.RelationalRowsExpressionCondition,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) bool {
    if (rowsExpressionUsesFieldSource(condition.lhs, field_source)) return true;
    for (condition.rhs) |rhs| {
        if (rowsExpressionUsesFieldSource(rhs, field_source)) return true;
    }
    return false;
}

fn parseRowsExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpression {
    return try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, null, value, allow_mutation_sources);
}

fn parseRowsExpressionWithSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpression {
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "field", "source", "value", "op", "args", "to", "path", "as_text", "cases", "else" });
    const field_value = value.object.get("field");
    const literal_value = value.object.get("value");
    const op_value = value.object.get("op");
    const present_count: u8 = (if (field_value != null) @as(u8, 1) else 0) + (if (literal_value != null) @as(u8, 1) else 0) + (if (op_value != null) @as(u8, 1) else 0);
    if (present_count != 1) return error.InvalidRowsRequest;
    if (value.object.get("source") != null and field_value == null) return error.InvalidRowsRequest;

    if (field_value) |field| {
        try requireJsonObjectOnlyKeys(value.object, &.{ "field", "source" });
        if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
        const field_source = try parseRowsExpressionFieldSource(value.object.get("source"), allow_mutation_sources);
        const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, field_source);
        _ = findRelationalColumn(field_schema.relational_columns, field.string) orelse return error.InvalidRowsRequest;
        return .{ .kind = .field, .field = try alloc.dupe(u8, field.string), .field_source = field_source };
    }
    if (literal_value) |literal| {
        try requireJsonObjectOnlyKeys(value.object, &.{"value"});
        return .{ .kind = .value, .value_json = try jsonValueStringifyAlloc(alloc, literal) };
    }

    const op = op_value.?;
    if (op != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, op.string, "case")) {
        try requireJsonObjectOnlyKeys(value.object, &.{ "op", "cases", "else" });
        return try parseRowsCaseExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, value, allow_mutation_sources);
    }
    if (std.mem.eql(u8, op.string, "now")) {
        try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args" });
        if (value.object.get("args")) |args_value| {
            if (args_value != .array or args_value.array.items.len != 0) return error.InvalidRowsRequest;
        }
        return .{
            .kind = .now,
            .value_json = try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
        };
    }

    const args_value = value.object.get("args") orelse return error.InvalidRowsRequest;
    if (args_value != .array or args_value.array.items.len == 0) return error.InvalidRowsRequest;
    const expression_kind: db_mod.types.RelationalRowsExpressionKind = if (std.mem.eql(u8, op.string, "coalesce"))
        .coalesce
    else if (std.mem.eql(u8, op.string, "lower"))
        .lower
    else if (std.mem.eql(u8, op.string, "upper"))
        .upper
    else if (std.mem.eql(u8, op.string, "trim"))
        .trim
    else if (std.mem.eql(u8, op.string, "replace"))
        .replace
    else if (std.mem.eql(u8, op.string, "concat"))
        .concat
    else if (std.mem.eql(u8, op.string, "length"))
        .length
    else if (std.mem.eql(u8, op.string, "nullif"))
        .nullif
    else if (std.mem.eql(u8, op.string, "greatest"))
        .greatest
    else if (std.mem.eql(u8, op.string, "least"))
        .least
    else if (std.mem.eql(u8, op.string, "abs"))
        .abs
    else if (std.mem.eql(u8, op.string, "round"))
        .round
    else if (std.mem.eql(u8, op.string, "floor"))
        .floor
    else if (std.mem.eql(u8, op.string, "ceil"))
        .ceil
    else if (std.mem.eql(u8, op.string, "add"))
        .add
    else if (std.mem.eql(u8, op.string, "sub"))
        .sub
    else if (std.mem.eql(u8, op.string, "mul"))
        .mul
    else if (std.mem.eql(u8, op.string, "div"))
        .div
    else if (std.mem.eql(u8, op.string, "interval_ns"))
        .interval_ns
    else if (std.mem.eql(u8, op.string, "cast"))
        .cast
    else if (std.mem.eql(u8, op.string, "json_extract"))
        .json_extract
    else if (std.mem.eql(u8, op.string, "array_length"))
        .array_length
    else if (std.mem.eql(u8, op.string, "string_to_array"))
        .string_to_array
    else
        return error.InvalidRowsRequest;
    switch (expression_kind) {
        .cast => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args", "to" }),
        .json_extract => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args", "path", "as_text" }),
        else => try requireJsonObjectOnlyKeys(value.object, &.{ "op", "args" }),
    }
    if ((expression_kind == .lower or expression_kind == .upper or expression_kind == .trim or expression_kind == .length) and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .replace and args_value.array.items.len != 3) return error.InvalidRowsRequest;
    if (expression_kind == .nullif and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if ((expression_kind == .greatest or expression_kind == .least) and args_value.array.items.len == 0) return error.InvalidRowsRequest;
    if ((expression_kind == .abs or expression_kind == .round or expression_kind == .floor or expression_kind == .ceil) and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .add and args_value.array.items.len < 2) return error.InvalidRowsRequest;
    if (expression_kind == .sub and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if (expression_kind == .mul and args_value.array.items.len < 2) return error.InvalidRowsRequest;
    if (expression_kind == .div and args_value.array.items.len != 2) return error.InvalidRowsRequest;
    if (expression_kind == .interval_ns and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .cast and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .json_extract and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .array_length and args_value.array.items.len != 1) return error.InvalidRowsRequest;
    if (expression_kind == .string_to_array and args_value.array.items.len != 2) return error.InvalidRowsRequest;

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, args_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeRowsQueryExpression(alloc, operand);
        alloc.free(operands);
    }
    for (args_value.array.items) |arg| {
        operands[initialized] = try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, arg, allow_mutation_sources);
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
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, root.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, root.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .json) return error.InvalidRowsRequest;
        }
    }
    if (expression_kind == .array_length) {
        const root = operands[0];
        if (root.kind == .field) {
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, root.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, root.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .array) return error.InvalidRowsRequest;
        }
    }
    const expression: db_mod.types.RelationalRowsExpression = .{ .kind = expression_kind, .operands = operands, .cast_type = cast_type, .json_path = json_path, .json_as_text = json_as_text };
    if (expression_kind == .length or expression_kind == .abs or expression_kind == .round or expression_kind == .floor or expression_kind == .ceil or expression_kind == .add or expression_kind == .sub or expression_kind == .mul or expression_kind == .div or expression_kind == .interval_ns) try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, expression);
    if (expression_kind == .coalesce or expression_kind == .nullif or expression_kind == .greatest or expression_kind == .least) try validateRowsExpressionOperandDomainsWithSources(alloc, schema, source_schema, expression);
    if (expression_kind == .string_to_array) try validateRowsStringToArrayExpressionWithSources(alloc, schema, source_schema, expression);
    return expression;
}

fn rowsExpressionSchemaForFieldSource(
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    field_source: db_mod.types.RelationalRowsExpressionFieldSource,
) runtime_schema.TableSchema {
    if (field_source == .source) return source_schema orelse schema;
    return schema;
}

fn parseRowsExpressionFieldSource(
    maybe_source: ?std.json.Value,
    allow_mutation_sources: bool,
) !db_mod.types.RelationalRowsExpressionFieldSource {
    const source = maybe_source orelse return .row;
    if (source != .string) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, source.string, "row")) return .row;
    if (!allow_mutation_sources) return error.InvalidRowsRequest;
    if (std.mem.eql(u8, source.string, "existing")) return .existing;
    if (std.mem.eql(u8, source.string, "proposed")) return .proposed;
    if (std.mem.eql(u8, source.string, "source")) return .source;
    return error.InvalidRowsRequest;
}

fn parseRowsQueryExpressionCastType(text: []const u8) ?db_mod.types.RelationalRowsExpressionCastType {
    if (std.mem.eql(u8, text, "text")) return .text;
    if (std.mem.eql(u8, text, "numeric")) return .numeric;
    if (std.mem.eql(u8, text, "bool") or std.mem.eql(u8, text, "boolean")) return .bool;
    return null;
}

fn parseRowsCaseExpressionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpression {
    return try parseRowsCaseExpressionWithSourceSchemaAlloc(alloc, schema, null, value, allow_mutation_sources);
}

fn parseRowsCaseExpressionWithSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpression {
    try requireJsonObjectOnlyKeys(value.object, &.{ "op", "cases", "else" });
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
        try requireJsonObjectOnlyKeys(branch_value.object, &.{ "when", "then" });
        const when_value = branch_value.object.get("when") orelse return error.InvalidRowsRequest;
        const then_value = branch_value.object.get("then") orelse return error.InvalidRowsRequest;
        const when = try parseRowsExpressionConditionWithSourceSchemaAlloc(alloc, schema, source_schema, when_value, allow_mutation_sources);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRowsQueryExpressionCondition(alloc, when);
        const then = try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, then_value, allow_mutation_sources);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRowsQueryExpression(alloc, then);
        branches[initialized] = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        initialized += 1;
    }

    const else_value = value.object.get("else") orelse return error.InvalidRowsRequest;
    const fallback = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var fallback_initialized = false;
    var fallback_transferred = false;
    errdefer {
        if (!fallback_transferred) {
            if (fallback_initialized) freeRowsQueryExpression(alloc, fallback[0]);
            alloc.free(fallback);
        }
    }
    fallback[0] = try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, else_value, allow_mutation_sources);
    fallback_initialized = true;

    _ = try rowsCaseExpressionOutputTypeWithSources(alloc, schema, source_schema, branches[0..initialized], fallback);

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
    return try parseRowsExpressionConditionAlloc(alloc, schema, value, false);
}

fn parseRowsExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpressionCondition {
    return try parseRowsExpressionConditionWithSourceSchemaAlloc(alloc, schema, null, value, allow_mutation_sources);
}

fn parseRowsExpressionConditionWithSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    value: std.json.Value,
    allow_mutation_sources: bool,
) anyerror!db_mod.types.RelationalRowsExpressionCondition {
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "lhs", "op", "rhs" });
    const lhs_value = value.object.get("lhs") orelse return error.InvalidRowsRequest;
    const op_value = value.object.get("op") orelse return error.InvalidRowsRequest;
    if (op_value != .string) return error.InvalidRowsRequest;
    const op = try parseRowsQueryPredicateOp(op_value.string);
    const rhs_needed = rowsQueryPredicateNeedsValue(op);
    const rhs_value = value.object.get("rhs");
    if (rhs_needed and rhs_value == null) return error.InvalidRowsRequest;
    if (!rhs_needed and rhs_value != null) return error.InvalidRowsRequest;

    const lhs = try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, lhs_value, allow_mutation_sources);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRowsQueryExpression(alloc, lhs);

    const rhs = if (rhs_value) |rhs_json| blk: {
        const out = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var out_transferred = false;
        errdefer if (!out_transferred) alloc.free(out);
        out[0] = try parseRowsExpressionWithSourceSchemaAlloc(alloc, schema, source_schema, rhs_json, allow_mutation_sources);
        out_transferred = true;
        break :blk out;
    } else &.{};
    var rhs_transferred = false;
    errdefer if (!rhs_transferred and rhs.len > 0) {
        for (rhs) |expression| freeRowsQueryExpression(alloc, expression);
        alloc.free(rhs);
    };
    try validateRowsExpressionConditionTypesWithSources(alloc, schema, source_schema, lhs, op, rhs);

    lhs_transferred = true;
    rhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = op,
        .rhs = rhs,
    };
}

fn validateRowsExpressionConditionTypes(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: []const db_mod.types.RelationalRowsExpression,
) !void {
    return try validateRowsExpressionConditionTypesWithSources(alloc, schema, null, lhs, op, rhs);
}

fn validateRowsExpressionConditionTypesWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: []const db_mod.types.RelationalRowsExpression,
) !void {
    switch (op) {
        .is_null, .is_not_null => {
            if (rhs.len != 0) return error.InvalidRowsRequest;
            return;
        },
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            if (rhs.len != 1) return error.InvalidRowsRequest;
        },
    }
    const lhs_type = try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, lhs);
    const rhs_expression = rhs[0];
    if (try rowsExpressionIsNullLiteral(alloc, rhs_expression)) return;
    const rhs_type = try rowsExpressionOutputTypeWithSources(alloc, schema, source_schema, rhs_expression);
    if (!rowsExpressionTypesComparable(lhs_type, rhs_type)) return error.InvalidRowsRequest;
    switch (op) {
        .gt, .gte, .lt, .lte => if (!rowsExpressionTypeIsOrderable(lhs_type) or !rowsExpressionTypeIsOrderable(rhs_type)) return error.InvalidRowsRequest,
        else => {},
    }
}

fn rowsExpressionIsNullLiteral(alloc: std.mem.Allocator, expression: db_mod.types.RelationalRowsExpression) !bool {
    if (expression.kind != .value) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    return parsed.value == .null;
}

fn rowsExpressionTypesComparable(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    if (rowsExpressionTypeIsTextLike(lhs) and rowsExpressionTypeIsTextLike(rhs)) return true;
    if ((lhs == .datetime and rhs == .numeric) or (lhs == .numeric and rhs == .datetime)) return true;
    return lhs == rhs;
}

fn rowsExpressionTypeIsTextLike(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
        else => false,
    };
}

fn rowsExpressionTypeIsOrderable(field_type: runtime_schema.AntflyType) bool {
    return rowsExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime or field_type == .boolean;
}

fn validateRowsQueryNumericExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    return try validateRowsQueryNumericExpressionWithSources(alloc, schema, null, expression);
}

fn validateRowsQueryNumericExpressionWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .field => {
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, expression.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, expression.field) orelse return error.InvalidRowsRequest;
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
        .now => {},
        .coalesce => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .nullif => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .length => {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
        },
        .greatest, .least => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .abs, .round, .floor, .ceil => {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
        },
        .cast => {
            if (expression.operands.len != 1 or expression.cast_type != .numeric) return error.InvalidRowsRequest;
        },
        .add => {
            if (expression.operands.len < 2) return error.InvalidRowsRequest;
            if (rowsExpressionContainsInterval(expression)) {
                if (expression.operands.len != 2) return error.InvalidRowsRequest;
                const lhs_interval = expression.operands[0].kind == .interval_ns;
                const rhs_interval = expression.operands[1].kind == .interval_ns;
                if (lhs_interval == rhs_interval) return error.InvalidRowsRequest;
                if (lhs_interval) {
                    try validateRowsQueryIntervalExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
                    try validateRowsQueryNumericOrDatetimeExpressionWithSources(alloc, schema, source_schema, expression.operands[1]);
                } else {
                    try validateRowsQueryNumericOrDatetimeExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
                    try validateRowsQueryIntervalExpressionWithSources(alloc, schema, source_schema, expression.operands[1]);
                }
                return;
            }
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .mul => {
            if (expression.operands.len < 2) return error.InvalidRowsRequest;
            if (rowsExpressionContainsInterval(expression)) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .sub => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            if (rowsExpressionContainsInterval(expression)) {
                if (expression.operands[0].kind == .interval_ns or expression.operands[1].kind != .interval_ns) return error.InvalidRowsRequest;
                try validateRowsQueryNumericOrDatetimeExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
                try validateRowsQueryIntervalExpressionWithSources(alloc, schema, source_schema, expression.operands[1]);
                return;
            }
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .div => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            if (rowsExpressionContainsInterval(expression)) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .case => {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidRowsRequest;
            for (expression.case_branches) |branch| try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, branch.then);
            try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, expression.case_else[0]);
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
        },
        .interval_ns => try validateRowsQueryIntervalExpressionWithSources(alloc, schema, source_schema, expression),
        .json_extract => return error.InvalidRowsRequest,
        else => return error.InvalidRowsRequest,
    }
}

fn rowsExpressionContainsInterval(expression: db_mod.types.RelationalRowsExpression) bool {
    if (expression.kind == .interval_ns) return true;
    for (expression.operands) |operand| {
        if (rowsExpressionContainsInterval(operand)) return true;
    }
    return false;
}

fn validateRowsQueryIntervalExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    return try validateRowsQueryIntervalExpressionWithSources(alloc, schema, null, expression);
}

fn validateRowsQueryIntervalExpressionWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind != .interval_ns or expression.operands.len != 1) return error.InvalidRowsRequest;
    if (rowsExpressionContainsInterval(expression.operands[0])) return error.InvalidRowsRequest;
    try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
}

fn validateRowsQueryNumericOrDatetimeExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    return try validateRowsQueryNumericOrDatetimeExpressionWithSources(alloc, schema, null, expression);
}

fn validateRowsQueryNumericOrDatetimeExpressionWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .field => {
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, expression.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, expression.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidRowsRequest;
        },
        .now => {},
        else => try validateRowsQueryNumericExpressionWithSources(alloc, schema, source_schema, expression),
    }
}

fn validateRowsQueryTextExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    return try validateRowsQueryTextExpressionWithSources(alloc, schema, null, expression);
}

fn validateRowsQueryTextExpressionWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    switch (expression.kind) {
        .field => {
            const field_schema = rowsExpressionSchemaForFieldSource(schema, source_schema, expression.field_source);
            const column = findRelationalColumn(field_schema.relational_columns, expression.field) orelse return error.InvalidRowsRequest;
            if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidRowsRequest;
        },
        .value => {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null, .string => {},
                else => return error.InvalidRowsRequest,
            }
        },
        .coalesce => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .lower, .upper, .trim => {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
        },
        .replace => {
            if (expression.operands.len != 3) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .concat => {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
        },
        .nullif => {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            for (expression.operands) |operand| try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, operand);
        },
        .case => {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidRowsRequest;
            for (expression.case_branches) |branch| try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, branch.then);
            try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, expression.case_else[0]);
        },
        .cast => {
            if (expression.operands.len != 1 or expression.cast_type != .text) return error.InvalidRowsRequest;
        },
        .json_extract => {
            if (expression.operands.len != 1 or !expression.json_as_text) return error.InvalidRowsRequest;
        },
        else => return error.InvalidRowsRequest,
    }
}

fn validateRowsStringToArrayExpression(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    return try validateRowsStringToArrayExpressionWithSources(alloc, schema, null, expression);
}

fn validateRowsStringToArrayExpressionWithSources(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind != .string_to_array or expression.operands.len != 2) return error.InvalidRowsRequest;
    try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, expression.operands[0]);
    try validateRowsQueryTextExpressionWithSources(alloc, schema, source_schema, expression.operands[1]);
    try validateRowsStringToArrayDelimiterLiteral(alloc, expression.operands[1]);
}

fn validateRowsStringToArrayDelimiterLiteral(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind != .value) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, expression.value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    switch (parsed.value) {
        .null => {},
        .string => |delimiter| if (delimiter.len == 0) return error.InvalidRowsRequest,
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "as", "field" });
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "field", "expr", "null_test", "direction" });
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

fn parseOptionalU64(maybe_value: ?std.json.Value) !?u64 {
    const value = maybe_value orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidRowsRequest;
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
    try requireJsonObjectOnlyKeys(range.object, &.{ "start", "end" });

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

fn parseRowsPlanRangesAlloc(
    alloc: std.mem.Allocator,
    maybe_ranges: ?std.json.Value,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    const ranges_value = maybe_ranges orelse return &.{};
    if (ranges_value != .array or ranges_value.array.items.len == 0) return error.InvalidRowsRequest;

    const out = try alloc.alloc(db_mod.types.RelationalRowsDocKeyRange, ranges_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |range| {
            if (range.start.len > 0) alloc.free(range.start);
            if (range.end.len > 0) alloc.free(range.end);
        }
        alloc.free(out);
    }

    for (ranges_value.array.items) |range_value| {
        const parsed = (try parseRowsQueryDocKeyRangeAlloc(alloc, range_value)) orelse return error.InvalidRowsRequest;
        out[initialized] = parsed;
        initialized += 1;
    }
    try validateRowsPlanRanges(out);
    return out;
}

fn validateRowsPlanRanges(ranges: []const db_mod.types.RelationalRowsDocKeyRange) !void {
    var previous_end: ?[]const u8 = null;
    for (ranges, 0..) |range, i| {
        if (range.start.len == 0 and range.end.len == 0) return error.InvalidRowsRequest;
        if (range.start.len > 0 and range.end.len > 0 and std.mem.order(u8, range.start, range.end) != .lt) return error.InvalidRowsRequest;
        if (i > 0 and range.start.len == 0) return error.InvalidRowsRequest;
        if (previous_end) |end| {
            if (end.len == 0) return error.InvalidRowsRequest;
            if (std.mem.order(u8, range.start, end) == .lt) return error.InvalidRowsRequest;
        }
        previous_end = range.end;
    }
}

fn freeRowsDocKeyRanges(alloc: std.mem.Allocator, ranges: []const db_mod.types.RelationalRowsDocKeyRange) void {
    for (ranges) |range| {
        if (range.start.len > 0) alloc.free(range.start);
        if (range.end.len > 0) alloc.free(range.end);
    }
    if (ranges.len > 0) alloc.free(ranges);
}

fn parseRowsQueryRowClaimAlloc(
    alloc: std.mem.Allocator,
    maybe_claim: ?std.json.Value,
) !?db_mod.types.RowClaimRequest {
    const claim = maybe_claim orelse return null;
    if (claim != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(claim.object, &.{ "mode", "skip_locked", "lease_ms", "owner_id", "transaction_id" });

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

    const txn_value = claim.object.get("transaction_id") orelse return error.InvalidRowsRequest;
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

fn queryAccessPredicateGroupPasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    group: db_mod.types.RelationalRowsAccessPredicateGroup,
) !bool {
    if (!try queryPredicatesPass(alloc, row, group.predicates)) return false;
    for (group.array_any) |predicate| {
        if (!try queryArrayAnyPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.array_contains) |predicate| {
        if (!try queryArrayContainsPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.array_eq) |predicate| {
        if (!try queryArrayEqPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.in_predicates) |predicate| {
        if (!try queryInPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.json_contains) |predicate| {
        if (!try queryJsonContainsPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.json_path_eq) |predicate| {
        if (!try queryJsonPathEqPredicatePasses(alloc, row, predicate)) return false;
    }
    for (group.json_path_exists) |predicate| {
        if (!queryJsonPathExistsPredicatePasses(row, predicate)) return false;
    }
    for (group.text_patterns) |predicate| {
        if (!queryTextPatternPredicatePasses(row, predicate)) return false;
    }
    return true;
}

fn queryAccessOrPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsAccessPredicateGroup,
) !bool {
    if (groups.len == 0) return true;
    for (groups) |group| {
        if (try queryAccessPredicateGroupPasses(alloc, row, group)) return true;
    }
    return false;
}

fn queryAccessNotPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsAccessPredicateGroup,
) !bool {
    for (groups) |group| {
        if (try queryAccessPredicateGroupPasses(alloc, row, group)) return false;
    }
    return true;
}

fn queryExpressionOrPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    if (groups.len == 0) return true;
    for (groups) |group| {
        var group_passes = true;
        for (group.conditions) |condition| {
            if (!try expressionConditionMatches(alloc, row, condition)) {
                group_passes = false;
                break;
            }
        }
        if (group_passes) return true;
    }
    return false;
}

fn queryExpressionNotPredicateGroupsPass(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (groups) |group| {
        var group_passes = true;
        for (group.conditions) |condition| {
            if (!try expressionConditionMatches(alloc, row, condition)) {
                group_passes = false;
                break;
            }
        }
        if (group_passes) return false;
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
    if (!try queryAccessOrPredicateGroupsPass(alloc, row, request.access_or_predicates)) return false;
    if (!try queryAccessNotPredicateGroupsPass(alloc, row, request.access_not_predicates)) return false;
    for (request.expression_predicates) |condition| {
        if (!try expressionConditionMatches(alloc, row, condition)) return false;
    }
    if (!try queryExpressionOrPredicateGroupsPass(alloc, row, request.expression_or_predicates)) return false;
    if (!try queryExpressionNotPredicateGroupsPass(alloc, row, request.expression_not_predicates)) return false;
    for (request.expression_array_contains) |predicate| {
        if (!try queryExpressionArrayContainsPredicatePasses(alloc, row, predicate)) return false;
    }
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
    for (request.text_patterns) |predicate| {
        if (!queryTextPatternPredicatePasses(row, predicate)) return false;
    }
    return true;
}

fn queryExpressionArrayContainsPredicatePasses(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) !bool {
    const actual_json = try expressionValueJsonAlloc(alloc, row, predicate.expression);
    defer alloc.free(actual_json);
    var actual = std.json.parseFromSlice(std.json.Value, alloc, actual_json, .{}) catch return error.InvalidRowsRequest;
    defer actual.deinit();
    if (actual.value == .null) return false;
    if (actual.value != .array) return error.InvalidRowsRequest;
    var wanted = std.json.parseFromSlice(std.json.Value, alloc, predicate.value_json, .{}) catch return error.InvalidRowsRequest;
    defer wanted.deinit();
    if (wanted.value != .array) return error.InvalidRowsRequest;
    return jsonValueContains(actual.value, wanted.value);
}

fn queryTextPatternPredicatePasses(
    row: std.json.Value,
    predicate: db_mod.types.RelationalRowsTextPatternPredicate,
) bool {
    const actual = jsonValueAtPath(row, predicate.field) orelse return predicate.negated;
    if (actual.* != .string) return predicate.negated;
    const matched = sqlLikePatternMatches(actual.string, predicate.pattern, predicate.case_insensitive);
    return if (predicate.negated) !matched else matched;
}

fn sqlLikePatternMatches(text: []const u8, pattern: []const u8, case_insensitive: bool) bool {
    return sqlLikePatternMatchesFrom(text, pattern, case_insensitive, 0, 0);
}

fn sqlLikePatternMatchesFrom(
    text: []const u8,
    pattern: []const u8,
    case_insensitive: bool,
    text_index: usize,
    pattern_index: usize,
) bool {
    var ti = text_index;
    var pi = pattern_index;
    while (pi < pattern.len) {
        const token = pattern[pi];
        if (token == '%') {
            while (pi < pattern.len and pattern[pi] == '%') pi += 1;
            if (pi == pattern.len) return true;
            var next_ti = ti;
            while (next_ti <= text.len) : (next_ti += 1) {
                if (sqlLikePatternMatchesFrom(text, pattern, case_insensitive, next_ti, pi)) return true;
            }
            return false;
        }
        if (ti >= text.len) return false;
        if (token == '_') {
            ti += 1;
            pi += 1;
            continue;
        }
        if (token == '\\' and pi + 1 < pattern.len) {
            pi += 1;
        }
        if (!sqlLikeBytesEqual(text[ti], pattern[pi], case_insensitive)) return false;
        ti += 1;
        pi += 1;
    }
    return ti == text.len;
}

fn sqlLikeBytesEqual(lhs: u8, rhs: u8, case_insensitive: bool) bool {
    if (!case_insensitive) return lhs == rhs;
    return std.ascii.toLower(lhs) == std.ascii.toLower(rhs);
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
    return try expressionValueJsonWithSourcesAlloc(alloc, row, null, expression);
}

fn expressionValueJsonWithSourcesAlloc(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    proposed_row: ?std.json.Value,
    expression: db_mod.types.RelationalRowsExpression,
) anyerror![]u8 {
    return switch (expression.kind) {
        .field => blk: {
            const source_row = switch (expression.field_source) {
                .row, .existing, .source => row,
                .proposed => proposed_row orelse return error.InvalidRowsRequest,
            };
            const selected = jsonValueAtPath(source_row, expression.field) orelse return try alloc.dupe(u8, "null");
            break :blk try std.json.Stringify.valueAlloc(alloc, selected.*, .{});
        },
        .value => try alloc.dupe(u8, expression.value_json),
        .now => if (expression.value_json.len > 0)
            try alloc.dupe(u8, expression.value_json)
        else
            try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
        .coalesce => blk: {
            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, operand);
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
        .lower, .upper, .trim => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const transformed = switch (expression.kind) {
                        .lower => try std.ascii.allocLowerString(alloc, text),
                        .upper => try std.ascii.allocUpperString(alloc, text),
                        .trim => try alloc.dupe(u8, std.mem.trim(u8, text, &std.ascii.whitespace)),
                        else => unreachable,
                    };
                    defer alloc.free(transformed);
                    break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = transformed }, .{});
                },
                else => return error.InvalidRowsRequest,
            }
        },
        .replace => blk: {
            if (expression.operands.len != 3) return error.InvalidRowsRequest;
            const source_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(source_json);
            const needle_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[1]);
            defer alloc.free(needle_json);
            const replacement_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[2]);
            defer alloc.free(replacement_json);
            var source = std.json.parseFromSlice(std.json.Value, alloc, source_json, .{}) catch return error.InvalidRowsRequest;
            defer source.deinit();
            var needle = std.json.parseFromSlice(std.json.Value, alloc, needle_json, .{}) catch return error.InvalidRowsRequest;
            defer needle.deinit();
            var replacement = std.json.parseFromSlice(std.json.Value, alloc, replacement_json, .{}) catch return error.InvalidRowsRequest;
            defer replacement.deinit();
            if (source.value == .null or needle.value == .null or replacement.value == .null) break :blk try alloc.dupe(u8, "null");
            if (source.value != .string or needle.value != .string or replacement.value != .string) return error.InvalidRowsRequest;
            const transformed = try replaceTextAlloc(alloc, source.value.string, needle.value.string, replacement.value.string);
            defer alloc.free(transformed);
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = transformed }, .{});
        },
        .concat => blk: {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, operand);
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
            const lhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            var lhs_transferred = false;
            errdefer if (!lhs_transferred) alloc.free(lhs_json);
            const rhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[1]);
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
        .greatest, .least => blk: {
            if (expression.operands.len == 0) return error.InvalidRowsRequest;
            var best_json: ?[]u8 = null;
            errdefer if (best_json) |owned| alloc.free(owned);
            var best_value: ?std.json.Parsed(std.json.Value) = null;
            defer if (best_value) |*parsed| parsed.deinit();

            for (expression.operands) |operand| {
                const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, operand);
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
                var parsed_transferred = false;
                defer if (!parsed_transferred) parsed.deinit();
                if (parsed.value == .null) {
                    alloc.free(value_json);
                    value_transferred = true;
                    continue;
                }
                if (best_value) |*current| {
                    const comparison = compareJsonScalars(parsed.value, current.value) orelse return error.InvalidRowsRequest;
                    const replace = switch (expression.kind) {
                        .greatest => comparison == .gt,
                        .least => comparison == .lt,
                        else => unreachable,
                    };
                    if (!replace) {
                        alloc.free(value_json);
                        value_transferred = true;
                        continue;
                    }
                    current.deinit();
                    alloc.free(best_json.?);
                }
                best_json = value_json;
                best_value = parsed;
                value_transferred = true;
                parsed_transferred = true;
            }
            break :blk if (best_json) |owned| owned else try alloc.dupe(u8, "null");
        },
        .abs, .round, .floor, .ceil => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            const value = try numericJsonValue(parsed.value);
            const result = switch (expression.kind) {
                .abs => if (value < 0) -value else value,
                .round => @round(value),
                .floor => @floor(value),
                .ceil => @ceil(value),
                else => unreachable,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{result});
        },
        .length => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .string => |text| {
                    const len = std.unicode.utf8CountCodepoints(text) catch return error.InvalidRowsRequest;
                    break :blk try std.fmt.allocPrint(alloc, "{d}", .{len});
                },
                else => return error.InvalidRowsRequest,
            }
        },
        .add, .sub, .mul, .div => blk: {
            if ((expression.kind == .add or expression.kind == .mul) and expression.operands.len < 2) return error.InvalidRowsRequest;
            if ((expression.kind == .sub or expression.kind == .div) and expression.operands.len != 2) return error.InvalidRowsRequest;
            const first_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(first_json);
            var first = std.json.parseFromSlice(std.json.Value, alloc, first_json, .{}) catch return error.InvalidRowsRequest;
            defer first.deinit();
            if (first.value == .null) break :blk try alloc.dupe(u8, "null");
            var result = try numericJsonValue(first.value);

            for (expression.operands[1..]) |operand| {
                const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, operand);
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
        .interval_ns => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            break :blk try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
        },
        .cast => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const cast_type = expression.cast_type orelse return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            break :blk try castExpressionValueJsonAlloc(alloc, parsed.value, cast_type);
        },
        .json_extract => blk: {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidRowsRequest;
            const root_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(root_json);
            var root = std.json.parseFromSlice(std.json.Value, alloc, root_json, .{}) catch return error.InvalidRowsRequest;
            defer root.deinit();
            const selected = jsonValueAtPath(root.value, expression.json_path) orelse break :blk try alloc.dupe(u8, "null");
            if (!expression.json_as_text) break :blk try std.json.Stringify.valueAlloc(alloc, selected.*, .{});
            break :blk try jsonExtractTextValueJsonAlloc(alloc, selected.*);
        },
        .array_length => blk: {
            if (expression.operands.len != 1) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .array => |array| break :blk try std.fmt.allocPrint(alloc, "{d}", .{array.items.len}),
                else => return error.InvalidRowsRequest,
            }
        },
        .string_to_array => blk: {
            if (expression.operands.len != 2) return error.InvalidRowsRequest;
            const value_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[0]);
            defer alloc.free(value_json);
            const delimiter_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.operands[1]);
            defer alloc.free(delimiter_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            var delimiter = std.json.parseFromSlice(std.json.Value, alloc, delimiter_json, .{}) catch return error.InvalidRowsRequest;
            defer delimiter.deinit();
            if (parsed.value == .null or delimiter.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed.value != .string or delimiter.value != .string or delimiter.value.string.len == 0) return error.InvalidRowsRequest;
            break :blk try stringToArrayValueJsonAlloc(alloc, parsed.value.string, delimiter.value.string);
        },
        .case => blk: {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidRowsRequest;
            for (expression.case_branches) |branch| {
                if (try expressionConditionMatchesWithSources(alloc, row, proposed_row, branch.when)) {
                    break :blk try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, branch.then);
                }
            }
            break :blk try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, expression.case_else[0]);
        },
    };
}

fn stringToArrayValueJsonAlloc(
    alloc: std.mem.Allocator,
    text: []const u8,
    delimiter: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var split = std.mem.splitSequence(u8, text, delimiter);
    var first = true;
    while (split.next()) |part| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}", .{std.json.fmt(part, .{})});
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
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
    return try expressionConditionMatchesWithSources(alloc, row, null, condition);
}

fn expressionConditionMatchesWithSources(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    proposed_row: ?std.json.Value,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) anyerror!bool {
    const lhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, condition.lhs);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return error.InvalidRowsRequest;
    defer lhs.deinit();

    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .is_distinct, .is_not_distinct => blk: {
            if (condition.rhs.len != 1) return error.InvalidRowsRequest;
            const rhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidRowsRequest;
            defer rhs.deinit();
            const not_distinct = jsonValuesNotDistinct(lhs.value, rhs.value) orelse return error.InvalidRowsRequest;
            break :blk if (condition.op == .is_not_distinct) not_distinct else !not_distinct;
        },
        .eq, .ne => blk: {
            if (condition.rhs.len != 1) return error.InvalidRowsRequest;
            const rhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidRowsRequest;
            defer rhs.deinit();
            const equal = jsonValuesEqual(lhs.value, rhs.value);
            break :blk if (condition.op == .eq) equal else !equal;
        },
        .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return error.InvalidRowsRequest;
            const rhs_json = try expressionValueJsonWithSourcesAlloc(alloc, row, proposed_row, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidRowsRequest;
            defer rhs.deinit();
            const comparison = compareJsonScalars(lhs.value, rhs.value) orelse return error.InvalidRowsRequest;
            break :blk switch (condition.op) {
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

fn freeRowsQueryAccessPredicateGroupsNoSlice(alloc: std.mem.Allocator, groups: []const db_mod.types.RelationalRowsAccessPredicateGroup) void {
    for (groups) |group| {
        freeQueryPredicates(alloc, group.predicates);
        if (group.predicates.len > 0) alloc.free(group.predicates);
        freeRowsQueryArrayAnyPredicatesNoSlice(alloc, group.array_any);
        if (group.array_any.len > 0) alloc.free(group.array_any);
        freeRowsQueryArrayContainsPredicatesNoSlice(alloc, group.array_contains);
        if (group.array_contains.len > 0) alloc.free(group.array_contains);
        freeRowsQueryArrayEqPredicatesNoSlice(alloc, group.array_eq);
        if (group.array_eq.len > 0) alloc.free(group.array_eq);
        freeRowsQueryInPredicatesNoSlice(alloc, group.in_predicates);
        if (group.in_predicates.len > 0) alloc.free(group.in_predicates);
        freeRowsQueryJsonContainsPredicatesNoSlice(alloc, group.json_contains);
        if (group.json_contains.len > 0) alloc.free(group.json_contains);
        freeRowsQueryJsonPathEqPredicatesNoSlice(alloc, group.json_path_eq);
        if (group.json_path_eq.len > 0) alloc.free(group.json_path_eq);
        freeRowsQueryJsonPathExistsPredicatesNoSlice(alloc, group.json_path_exists);
        if (group.json_path_exists.len > 0) alloc.free(group.json_path_exists);
        freeRowsQueryTextPatternPredicatesNoSlice(alloc, group.text_patterns);
        if (group.text_patterns.len > 0) alloc.free(group.text_patterns);
    }
}

fn freeRowsQueryArrayAnyPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayAnyPredicate) void {
    freeRowsQueryArrayAnyPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryArrayContainsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    freeRowsQueryArrayContainsPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryArrayContainsPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
}

fn freeRowsQueryArrayEqPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    freeRowsQueryArrayEqPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryArrayEqPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
}

fn freeRowsQueryInPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsInPredicate) void {
    freeRowsQueryInPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryInPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsInPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.values_json);
    }
}

fn freeRowsQueryJsonContainsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    freeRowsQueryJsonContainsPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonContainsPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.value_json);
    }
}

fn freeRowsQueryJsonPathEqPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    freeRowsQueryJsonPathEqPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonPathEqPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
        alloc.free(predicate.value_json);
    }
}

fn freeRowsQueryJsonPathExistsPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    freeRowsQueryJsonPathExistsPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryJsonPathExistsPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.path);
    }
}

fn freeRowsQueryTextPatternPredicates(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsTextPatternPredicate) void {
    freeRowsQueryTextPatternPredicatesNoSlice(alloc, predicates);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryTextPatternPredicatesNoSlice(alloc: std.mem.Allocator, predicates: []const db_mod.types.RelationalRowsTextPatternPredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        alloc.free(predicate.pattern);
    }
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

fn freeRowsExpressionAssignments(alloc: std.mem.Allocator, assignments: []const db_mod.types.RelationalRowsExpressionAssignment) void {
    for (assignments) |assignment| {
        alloc.free(@constCast(assignment.field));
        freeRowsQueryExpression(alloc, assignment.expression);
    }
    if (assignments.len > 0) alloc.free(assignments);
}

fn freeRowsJsonSetExpressionAssignments(alloc: std.mem.Allocator, assignments: []const db_mod.types.RelationalRowsJsonSetExpressionAssignment) void {
    for (assignments) |assignment| {
        alloc.free(@constCast(assignment.field));
        for (assignment.path) |segment| alloc.free(@constCast(segment));
        if (assignment.path.len > 0) alloc.free(assignment.path);
        freeRowsQueryExpression(alloc, assignment.expression);
    }
    if (assignments.len > 0) alloc.free(assignments);
}

fn freeRowsOnConflict(alloc: std.mem.Allocator, conflict: db_mod.types.RelationalRowsOnConflict) void {
    if (conflict.target.unique_name.len > 0) alloc.free(@constCast(conflict.target.unique_name));
    freeQueryPredicates(alloc, conflict.target.unique_predicates);
    if (conflict.target.unique_predicates.len > 0) alloc.free(conflict.target.unique_predicates);
    freeTransformOps(alloc, conflict.operations);
    freeRowsExpressionAssignments(alloc, conflict.patch_expressions);
    freeRowsExpressionAssignments(alloc, conflict.increment_expressions);
    freeRowsJsonSetExpressionAssignments(alloc, conflict.json_set_expressions);
    if (conflict.where_expression) |condition| freeRowsQueryExpressionCondition(alloc, condition);
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

fn freeRowsQueryExpressionArrayContainsPredicate(
    alloc: std.mem.Allocator,
    predicate: db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) void {
    freeRowsQueryExpression(alloc, predicate.expression);
    alloc.free(predicate.value_json);
}

fn freeRowsQueryExpressionArrayContainsPredicates(
    alloc: std.mem.Allocator,
    predicates: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate,
) void {
    for (predicates) |predicate| freeRowsQueryExpressionArrayContainsPredicate(alloc, predicate);
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeRowsQueryExpressionConditions(alloc: std.mem.Allocator, conditions: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (conditions) |condition| freeRowsQueryExpressionCondition(alloc, condition);
    if (conditions.len > 0) alloc.free(conditions);
}

fn freeRowsQueryExpressionPredicateGroups(alloc: std.mem.Allocator, groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup) void {
    for (groups) |group| {
        freeRowsQueryExpressionConditions(alloc, group.conditions);
    }
    if (groups.len > 0) alloc.free(groups);
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
    freeRowsQueryArrayAnyPredicates(alloc, spec.filter_array_any);
    freeRowsQueryArrayContainsPredicates(alloc, spec.filter_array_contains);
    freeRowsQueryArrayEqPredicates(alloc, spec.filter_array_eq);
    freeRowsQueryInPredicates(alloc, spec.filter_in_predicates);
    freeRowsQueryJsonContainsPredicates(alloc, spec.filter_json_contains);
    freeRowsQueryJsonPathEqPredicates(alloc, spec.filter_json_path_eq);
    freeRowsQueryJsonPathExistsPredicates(alloc, spec.filter_json_path_exists);
    freeRowsQueryTextPatternPredicates(alloc, spec.filter_text_patterns);
    freeRowsQueryExpressionConditions(alloc, spec.filter_expressions);
    freeRowsQueryExpressionArrayContainsPredicates(alloc, spec.filter_expression_array_contains);
    freeRowsQueryExpressionPredicateGroups(alloc, spec.filter_any);
    freeRowsQueryExpressionPredicateGroups(alloc, spec.filter_not);
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
    if (spec.value_expression) |expression| freeRowsQueryExpression(alloc, expression);
    if (spec.default_json.len > 0) alloc.free(spec.default_json);
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

fn freeRowsJoinedMutationFieldAssignments(alloc: std.mem.Allocator, assignments: []const db_mod.types.RelationalRowsJoinedMutationFieldAssignment) void {
    for (assignments) |assignment| {
        alloc.free(assignment.field);
        alloc.free(assignment.source_field);
    }
    if (assignments.len > 0) alloc.free(assignments);
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

fn insertSourceAssignedRowJsonAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    source_row_json: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, source_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    for (req.assignments) |assignment| {
        const value_json = try expressionValueJsonAlloc(alloc, parsed.value, assignment.expression);
        defer alloc.free(value_json);
        try appendRawJsonFieldValue(alloc, writer, &first, assignment.field, value_json);
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn insertSourceReturningProjectionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    row_json: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    if (req.returning_all) {
        for (parsed.value.object.keys(), parsed.value.object.values()) |field, value| {
            try appendJsonFieldValue(alloc, writer, &first, field, value);
        }
    }
    for (req.returning) |field| {
        if (field.len == 0) return error.InvalidRowsRequest;
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(field, .{})});
        if (jsonValueAtPath(parsed.value, field)) |selected| {
            try std.json.Stringify.value(selected.*, .{}, writer);
        } else {
            try writer.writeAll("null");
        }
    }
    for (req.returning_expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(projection.output, .{})});
        const value_json = try expressionValueJsonAlloc(alloc, parsed.value, projection.expression);
        defer alloc.free(value_json);
        try writer.writeAll(value_json);
    }
    _ = schema;
    try writer.writeByte('}');
    return try out.toOwnedSlice();
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
        .lower, .upper => blk: {
            const field = generated.field orelse return error.InvalidRowsRequest;
            const source = try plannedStringFieldValueAlloc(alloc, schema, row_value, resolved_defaults, field);
            defer alloc.free(source);
            const folded = switch (generated.op) {
                .lower => try std.ascii.allocLowerString(alloc, source),
                .upper => try std.ascii.allocUpperString(alloc, source),
                .concat => unreachable,
            };
            defer alloc.free(folded);
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(folded, .{})});
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

fn replaceTextAlloc(alloc: std.mem.Allocator, source: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    if (needle.len == 0) return try alloc.dupe(u8, source);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var start: usize = 0;
    while (std.mem.indexOfPos(u8, source, start, needle)) |index| {
        try out.appendSlice(alloc, source[start..index]);
        try out.appendSlice(alloc, replacement);
        start = index + needle.len;
    }
    try out.appendSlice(alloc, source[start..]);
    return try out.toOwnedSlice(alloc);
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
        if (check.validation_state != .enforced) continue;
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
        .is_distinct, .is_not_distinct => blk: {
            const expected_json = check.value_json orelse return error.InvalidRowsRequest;
            var expected = std.json.parseFromSlice(std.json.Value, alloc, expected_json, .{}) catch return error.InvalidRowsRequest;
            defer expected.deinit();
            const actual: std.json.Value = if (value) |selected| selected.* else .null;
            const not_distinct = jsonValuesNotDistinct(actual, expected.value) orelse return error.InvalidRowsRequest;
            break :blk if (check.op == .is_not_distinct) not_distinct else !not_distinct;
        },
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

fn jsonValuesNotDistinct(actual: std.json.Value, expected: std.json.Value) ?bool {
    if (jsonScalarKind(actual) != null and jsonScalarKind(expected) != null) {
        if (jsonScalarKind(actual) != jsonScalarKind(expected)) return false;
    }
    const comparison = compareJsonScalars(actual, expected) orelse return null;
    return comparison == .eq;
}

const JsonScalarKind = enum { null, bool, numeric, string };

fn jsonScalarKind(value: std.json.Value) ?JsonScalarKind {
    return switch (value) {
        .null => .null,
        .bool => .bool,
        .integer, .float => .numeric,
        .string => .string,
        else => null,
    };
}

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

fn appendPlannedInsertAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
    require_absent: bool,
    writes: *std.ArrayListUnmanaged(db_mod.types.BatchWrite),
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
) !void {
    const row_json_owned = try alloc.dupe(u8, row_json);
    var row_json_transferred = false;
    errdefer if (!row_json_transferred) alloc.free(row_json_owned);
    const key = try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
    var key_transferred = false;
    errdefer if (!key_transferred) alloc.free(key);
    try writes.append(alloc, .{ .key = key, .value = row_json_owned });
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
    try requireJsonObjectOnlyKeys(conflict_value.object, &.{ "target", "action", "patch", "patch_expr", "increment", "increment_expr", "json_set", "array_update", "where_expression" });
    const action = try parseConflictAction(conflict_value.object.get("action") orelse return error.InvalidRowsRequest);
    const target_value = conflict_value.object.get("target") orelse return error.InvalidRowsRequest;
    if (target_value != .object) return error.InvalidRowsRequest;

    const row_json = try plannedRelationalRowJsonAlloc(alloc, schema, row_value);
    defer alloc.free(row_json);

    const conflict_key = try conflictTargetPrimaryKeyAlloc(alloc, table_name, schema, row_json, target_value, resolver);
    defer if (conflict_key) |key| alloc.free(key);

    if (conflict_key) |key| {
        switch (action) {
            .nothing => {
                if (hasConflictActionCondition(conflict_value)) return error.InvalidRowsRequest;
                return;
            },
            .update => {
                const needs_existing_row = hasMutationExpression(conflict_value) or hasConflictActionCondition(conflict_value) or hasReturningProjection(op_value) or schemaHasGeneratedColumns(schema) or schema.checks.len != 0;
                var existing: ?ResolvedPrimaryRow = if (needs_existing_row)
                    (try resolver.lookupPrimary(alloc, table_name, key)) orelse return error.RowSelectorNotFound
                else
                    null;
                defer if (existing) |*row| row.deinit(alloc);
                var parsed_existing: std.json.Parsed(std.json.Value) = undefined;
                var parsed_existing_loaded = false;
                defer if (parsed_existing_loaded) parsed_existing.deinit();
                const existing_row_value: ?std.json.Value = if (existing) |row| blk: {
                    parsed_existing = std.json.parseFromSlice(std.json.Value, alloc, row.json, .{}) catch return error.InvalidRowsRequest;
                    parsed_existing_loaded = true;
                    if (parsed_existing.value != .object) return error.InvalidRowsRequest;
                    break :blk parsed_existing.value;
                } else null;
                var parsed_proposed: std.json.Parsed(std.json.Value) = undefined;
                var parsed_proposed_loaded = false;
                defer if (parsed_proposed_loaded) parsed_proposed.deinit();
                const proposed_row_value: ?std.json.Value = if (hasMutationExpression(conflict_value) or hasConflictActionCondition(conflict_value)) blk: {
                    parsed_proposed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
                    parsed_proposed_loaded = true;
                    if (parsed_proposed.value != .object) return error.InvalidRowsRequest;
                    break :blk parsed_proposed.value;
                } else null;
                if (hasConflictActionCondition(conflict_value) and !try conflictActionConditionMatches(alloc, schema, conflict_value, existing_row_value orelse return error.InvalidRowsRequest, proposed_row_value)) return;
                var operations = try updateTransformOperationsAlloc(alloc, schema, conflict_value, existing_row_value, proposed_row_value);
                var operations_transferred = false;
                errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
                operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);
                if (existing) |row| {
                    try appendVersionPredicateAlloc(alloc, predicates, key, row.version);
                }
                if (existing) |row| {
                    const projected_json = (try db_mod.transform.resolveDocumentTransform(alloc, row.json, .{ .key = key, .operations = operations })) orelse return error.RowSelectorNotFound;
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
    if (target_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(target_value.object, &.{ "primary", "unique" });
    if (target_value.object.get("primary") != null and target_value.object.get("unique") != null) return error.InvalidRowsRequest;
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
        try requireJsonObjectOnlyKeys(unique_value.object, &.{ "name", "where" });
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

fn typedConflictTargetPrimaryKeyAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
    target: db_mod.types.RelationalRowsConflictTarget,
    resolver: UniqueSelectorResolver,
) !?[]u8 {
    switch (target.kind) {
        .primary => {
            const key = try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
            errdefer alloc.free(key);
            if (try resolver.primaryExists(alloc, table_name, key)) return key;
            alloc.free(key);
            return null;
        },
        .unique => {
            if (target.unique_name.len == 0) return error.InvalidRowsRequest;
            const constraint = findUniqueConstraint(schema.unique_constraints, target.unique_name) orelse return error.InvalidRowsRequest;
            const encoded_value = (try uniqueConstraintValueFromRowAlloc(alloc, schema, constraint, row_json)) orelse return null;
            defer alloc.free(encoded_value);
            return try resolver.resolveUnique(alloc, table_name, constraint.name, encoded_value);
        },
    }
}

fn typedConflictTargetDuplicateKeyAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    row_json: []const u8,
    target: db_mod.types.RelationalRowsConflictTarget,
) !?[]const u8 {
    switch (target.kind) {
        .primary => return try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json),
        .unique => {
            if (target.unique_name.len == 0) return error.InvalidRowsRequest;
            const constraint = findUniqueConstraint(schema.unique_constraints, target.unique_name) orelse return error.InvalidRowsRequest;
            const encoded_value = (try uniqueConstraintValueFromRowAlloc(alloc, schema, constraint, row_json)) orelse return null;
            defer alloc.free(encoded_value);
            return try std.mem.concat(alloc, u8, &.{ "unique\x00", constraint.name, "\x00", encoded_value });
        },
    }
}

fn appendTypedConflictUpdateAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    proposed_row_json: []const u8,
    conflict: db_mod.types.RelationalRowsOnConflict,
    key: []const u8,
    resolver: UniqueSelectorResolver,
    transforms: *std.ArrayListUnmanaged(db_mod.types.DocumentTransform),
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    returning_rows: *std.ArrayListUnmanaged([]const u8),
) !void {
    var existing = (try resolver.lookupPrimary(alloc, table_name, key)) orelse return error.RowSelectorNotFound;
    defer existing.deinit(alloc);

    var parsed_existing = std.json.parseFromSlice(std.json.Value, alloc, existing.json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_existing.deinit();
    if (parsed_existing.value != .object) return error.InvalidRowsRequest;
    var parsed_proposed = std.json.parseFromSlice(std.json.Value, alloc, proposed_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_proposed.deinit();
    if (parsed_proposed.value != .object) return error.InvalidRowsRequest;

    if (conflict.where_expression) |condition| {
        if (!try expressionConditionMatchesWithSources(alloc, parsed_existing.value, parsed_proposed.value, condition)) return;
    }

    var operations = try typedConflictUpdateOperationsAlloc(alloc, conflict, parsed_existing.value, parsed_proposed.value);
    var operations_transferred = false;
    errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
    operations = try extendOperationsWithOnUpdateAlloc(alloc, operations, schema);

    try appendVersionPredicateAlloc(alloc, predicates, key, existing.version);

    const projected_json = (try db_mod.transform.resolveDocumentTransform(alloc, existing.json, .{ .key = key, .operations = operations })) orelse return error.RowSelectorNotFound;
    defer alloc.free(projected_json);
    const planned_json = try plannedExistingRelationalRowJsonAlloc(alloc, schema, projected_json);
    defer alloc.free(planned_json);
    if (schemaHasGeneratedColumns(schema)) operations = try extendOperationsWithGeneratedColumnsAlloc(alloc, operations, schema, planned_json);

    if (req.returning_all or req.returning.len > 0 or req.returning_expressions.len > 0) {
        const projected = try insertSourceReturningProjectionAlloc(alloc, schema, req, planned_json);
        var projected_transferred = false;
        errdefer if (!projected_transferred) alloc.free(projected);
        try returning_rows.append(alloc, projected);
        projected_transferred = true;
    }

    const transform_key = try alloc.dupe(u8, key);
    var key_transferred = false;
    errdefer if (!key_transferred) alloc.free(transform_key);
    try transforms.append(alloc, .{ .key = transform_key, .operations = operations });
    key_transferred = true;
    operations_transferred = true;
}

fn typedConflictUpdateOperationsAlloc(
    alloc: std.mem.Allocator,
    conflict: db_mod.types.RelationalRowsOnConflict,
    existing_row: std.json.Value,
    proposed_row: std.json.Value,
) ![]db_mod.types.TransformOp {
    var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer {
        for (operations.items) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        operations.deinit(alloc);
    }

    for (conflict.operations) |op| {
        const path = try alloc.dupe(u8, op.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        const value_json: ?[]const u8 = if (op.value_json) |value| try alloc.dupe(u8, value) else null;
        var value_transferred = false;
        errdefer if (!value_transferred) if (value_json) |value| alloc.free(value);
        try operations.append(alloc, .{ .op = op.op, .path = path, .value_json = value_json });
        path_transferred = true;
        value_transferred = true;
    }

    for (conflict.patch_expressions) |assignment| {
        const value_json = try expressionValueJsonWithSourcesAlloc(alloc, existing_row, proposed_row, assignment.expression);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, assignment.field);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{ .op = .set, .path = path, .value_json = value_json });
        path_transferred = true;
        value_transferred = true;
    }

    for (conflict.increment_expressions) |assignment| {
        const value_json = try expressionValueJsonWithSourcesAlloc(alloc, existing_row, proposed_row, assignment.expression);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try validateIncrementExpressionValueJson(alloc, value_json);
        const path = try alloc.dupe(u8, assignment.field);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{ .op = .inc, .path = path, .value_json = value_json });
        path_transferred = true;
        value_transferred = true;
    }

    for (conflict.json_set_expressions) |assignment| {
        const transform_path = try jsonSetTypedTransformPathAlloc(alloc, assignment.field, assignment.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(transform_path);
        const value_json = try expressionValueJsonWithSourcesAlloc(alloc, existing_row, proposed_row, assignment.expression);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        try operations.append(alloc, .{ .op = .set, .path = transform_path, .value_json = value_json });
        path_transferred = true;
        value_transferred = true;
    }

    return try operations.toOwnedSlice(alloc);
}

fn jsonSetTypedTransformPathAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    path: []const []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll(field);
    for (path) |segment| {
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.InvalidRowsRequest;
        try writer.writeByte('.');
        try writer.writeAll(segment);
    }
    return try out.toOwnedSlice();
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
    try requireJsonObjectOnlyKeys(value.object, &.{"all"});
    const all_value = value.object.get("all") orelse return error.InvalidRowsRequest;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.InvalidRowsRequest;
    for (all_value.array.items, predicates) |item, predicate| {
        try validateUniquePredicateAtomJsonMatches(alloc, item, predicate);
    }
}

fn conflictActionConditionMatches(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    conflict_value: std.json.Value,
    existing_row: std.json.Value,
    proposed_row: ?std.json.Value,
) !bool {
    const condition_value = conflict_value.object.get("where_expression") orelse return true;
    const condition = try parseRowsExpressionConditionAlloc(alloc, schema, condition_value, true);
    defer freeRowsQueryExpressionCondition(alloc, condition);
    return try expressionConditionMatchesWithSources(alloc, existing_row, proposed_row, condition);
}

fn validateUniquePredicateAtomJsonMatches(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    predicate: runtime_schema.UniquePredicate,
) !void {
    if (value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(value.object, &.{ "field", "op", "value" });
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
    try requireJsonObjectOnlyKeys(where_value.object, &.{ "primary", "unique" });
    if (where_value.object.get("primary") != null and where_value.object.get("unique") != null) return error.InvalidRowsRequest;
    const primary_value = where_value.object.get("primary") orelse {
        if (where_value.object.get("unique")) |unique_value| {
            const resolver = unique_resolver orelse return error.UnsupportedRowsSelector;
            if (unique_value != .object) return error.InvalidRowsRequest;
            try requireJsonObjectOnlyKeys(unique_value.object, &.{ "name", "values" });
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
    if (constraint.expressions.len != 0) return error.InvalidRowsRequest;
    var selected = std.ArrayListUnmanaged(runtime_schema.RelationalColumn).empty;
    errdefer selected.deinit(alloc);
    for (constraint.columns) |column_name| {
        try appendUniqueConstraintSelectedColumn(alloc, &selected, schema, column_name);
    }
    for (constraint.where) |predicate| {
        try appendUniqueConstraintSelectedColumn(alloc, &selected, schema, predicate.field);
    }
    return try selected.toOwnedSlice(alloc);
}

fn appendUniqueConstraintSelectedColumn(
    alloc: std.mem.Allocator,
    selected: *std.ArrayListUnmanaged(runtime_schema.RelationalColumn),
    schema: runtime_schema.TableSchema,
    field: []const u8,
) !void {
    if (relationalColumnListContains(selected.items, field)) return;
    const column = findRelationalColumn(schema.relational_columns, field) orelse return error.InvalidRowsRequest;
    try selected.append(alloc, column);
}

fn relationalColumnListContains(columns: []const runtime_schema.RelationalColumn, field: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, field) or std.mem.eql(u8, column.name, field)) return true;
    }
    return false;
}

fn findUniqueConstraint(constraints: []const runtime_schema.UniqueConstraint, name: []const u8) ?runtime_schema.UniqueConstraint {
    for (constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
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
    existing_row: ?std.json.Value,
    proposed_row: ?std.json.Value,
) ![]db_mod.types.TransformOp {
    if (op_value != .object) return error.InvalidRowsRequest;
    var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer {
        for (operations.items) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        operations.deinit(alloc);
    }

    var saw_mutation = false;
    if (op_value.object.get("patch")) |patch| {
        saw_mutation = true;
        if (patch != .object) return error.InvalidRowsRequest;
        try appendPatchTransformOperationsAlloc(alloc, schema.primary_key.?, patch, &operations);
    }
    if (op_value.object.get("patch_expr")) |patch_expr| {
        saw_mutation = true;
        if (patch_expr != .object) return error.InvalidRowsRequest;
        const existing = existing_row orelse return error.InvalidRowsRequest;
        try appendPatchExpressionTransformOperationsAlloc(alloc, schema, existing, proposed_row, patch_expr, &operations);
    }
    if (op_value.object.get("increment")) |increment| {
        saw_mutation = true;
        if (increment != .object) return error.InvalidRowsRequest;
        try appendIncrementTransformOperationsAlloc(alloc, schema, increment, &operations);
    }
    if (op_value.object.get("increment_expr")) |increment_expr| {
        saw_mutation = true;
        if (increment_expr != .object) return error.InvalidRowsRequest;
        const existing = existing_row orelse return error.InvalidRowsRequest;
        try appendIncrementExpressionTransformOperationsAlloc(alloc, schema, existing, proposed_row, increment_expr, &operations);
    }
    if (op_value.object.get("json_set")) |json_set| {
        saw_mutation = true;
        try appendJsonSetTransformOperationsAlloc(alloc, schema, json_set, existing_row, proposed_row, false, &operations);
    }
    if (op_value.object.get("array_update")) |array_update| {
        saw_mutation = true;
        try appendArrayUpdateTransformOperationsAlloc(alloc, schema, array_update, &operations);
    }
    if (!saw_mutation) return error.InvalidRowsRequest;
    try validateRowsMutationUpdateTargetPaths(operations.items, &.{}, &.{}, &.{}, &.{});

    return try operations.toOwnedSlice(alloc);
}

fn staticUpdateTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    op_value: std.json.Value,
    skip_json_set_expressions: bool,
) ![]db_mod.types.TransformOp {
    if (op_value != .object) return error.InvalidRowsRequest;
    var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer {
        for (operations.items) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        operations.deinit(alloc);
    }

    if (op_value.object.get("patch")) |patch| {
        if (patch != .object) return error.InvalidRowsRequest;
        try appendPatchTransformOperationsAlloc(alloc, schema.primary_key.?, patch, &operations);
    }
    if (op_value.object.get("increment")) |increment| {
        if (increment != .object) return error.InvalidRowsRequest;
        try appendIncrementTransformOperationsAlloc(alloc, schema, increment, &operations);
    }
    if (op_value.object.get("json_set")) |json_set| {
        try appendJsonSetTransformOperationsAlloc(alloc, schema, json_set, null, null, skip_json_set_expressions, &operations);
    }
    if (op_value.object.get("array_update")) |array_update| {
        try appendArrayUpdateTransformOperationsAlloc(alloc, schema, array_update, &operations);
    }

    return try operations.toOwnedSlice(alloc);
}

fn parseRowsMutationExpressionAssignmentsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_assignments: ?std.json.Value,
    require_numeric: bool,
    allow_proposed_source: bool,
) ![]db_mod.types.RelationalRowsExpressionAssignment {
    const assignments_value = maybe_assignments orelse return &.{};
    if (assignments_value != .object) return error.InvalidRowsRequest;

    const assignments = try alloc.alloc(db_mod.types.RelationalRowsExpressionAssignment, assignments_value.object.count());
    var initialized: usize = 0;
    errdefer {
        for (assignments[0..initialized]) |assignment| {
            alloc.free(@constCast(assignment.field));
            freeRowsQueryExpression(alloc, assignment.expression);
        }
        alloc.free(assignments);
    }

    var it = assignments_value.object.iterator();
    while (it.next()) |entry| {
        const column = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, entry.key_ptr.*)) return error.InvalidRowsRequest;
        }
        if (require_numeric and column.field_type != .numeric) return error.InvalidRowsRequest;
        const expression = try parseRowsMutationExpressionAlloc(alloc, schema, entry.value_ptr.*);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        if (!allow_proposed_source and expressionUsesProposedSource(expression)) return error.InvalidRowsRequest;
        if (require_numeric or column.field_type == .numeric or column.field_type == .datetime) {
            try validateRowsQueryNumericExpression(alloc, schema, expression);
        } else if (column.field_type == .keyword or column.field_type == .text or column.field_type == .link) {
            try validateRowsQueryTextExpression(alloc, schema, expression);
        }
        const field = try alloc.dupe(u8, column.path);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        assignments[initialized] = .{
            .field = field,
            .expression = expression,
        };
        field_transferred = true;
        expression_transferred = true;
        initialized += 1;
    }

    return assignments;
}

fn parseRowsInsertSourceAssignmentsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    assignments_value: std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionAssignment {
    return try parseRowsInsertSourceAssignmentsWithSchemasAlloc(alloc, schema, schema, assignments_value);
}

fn parseRowsInsertSourceAssignmentsWithSchemasAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    assignments_value: std.json.Value,
) ![]const db_mod.types.RelationalRowsExpressionAssignment {
    if (assignments_value != .array or assignments_value.array.items.len == 0) return error.InvalidRowsRequest;

    const assignments = try alloc.alloc(db_mod.types.RelationalRowsExpressionAssignment, assignments_value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (assignments[0..initialized]) |assignment| {
            alloc.free(@constCast(assignment.field));
            freeRowsQueryExpression(alloc, assignment.expression);
        }
        alloc.free(assignments);
    }

    for (assignments_value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "target_field", "expr" });
        const target_value = item.object.get("target_field") orelse return error.InvalidRowsRequest;
        if (target_value != .string or target_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(target_schema.relational_columns, target_value.string) orelse return error.InvalidRowsRequest;
        for (assignments[0..initialized]) |existing| {
            if (std.mem.eql(u8, existing.field, column.path)) return error.InvalidRowsRequest;
        }

        const expression_value = item.object.get("expr") orelse return error.InvalidRowsRequest;
        const expression = try parseRowsExpressionAlloc(alloc, source_schema, expression_value, true);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        if (expressionUsesInsertSourceDisallowedMutationSource(expression)) return error.InvalidRowsRequest;
        try validateRowsInsertSourceAssignmentType(alloc, source_schema, column, expression);

        const field = try alloc.dupe(u8, column.path);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        assignments[initialized] = .{
            .field = field,
            .expression = expression,
        };
        field_transferred = true;
        expression_transferred = true;
        initialized += 1;
    }

    return assignments;
}

fn validateRowsInsertSourceAssignmentType(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const expression_type = try rowsExpressionOutputType(alloc, schema, expression);
    const compatible = if (column.field_type == .json or column.field_type == .array)
        expression_type == column.field_type
    else
        rowsExpressionTypesComparable(column.field_type, expression_type);
    if (!compatible) return error.InvalidRowsRequest;
}

fn expressionUsesInsertSourceDisallowedMutationSource(expression: db_mod.types.RelationalRowsExpression) bool {
    if (expression.kind == .field and (expression.field_source == .existing or expression.field_source == .proposed)) return true;
    for (expression.operands) |operand| if (expressionUsesInsertSourceDisallowedMutationSource(operand)) return true;
    for (expression.case_branches) |branch| {
        if (expressionConditionUsesInsertSourceDisallowedMutationSource(branch.when) or expressionUsesInsertSourceDisallowedMutationSource(branch.then)) return true;
    }
    for (expression.case_else) |fallback| if (expressionUsesInsertSourceDisallowedMutationSource(fallback)) return true;
    return false;
}

fn expressionConditionUsesInsertSourceDisallowedMutationSource(condition: db_mod.types.RelationalRowsExpressionCondition) bool {
    if (expressionUsesInsertSourceDisallowedMutationSource(condition.lhs)) return true;
    for (condition.rhs) |rhs| if (expressionUsesInsertSourceDisallowedMutationSource(rhs)) return true;
    return false;
}

fn parseRowsOnConflictAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    conflict_value: std.json.Value,
) !db_mod.types.RelationalRowsOnConflict {
    if (conflict_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(conflict_value.object, &.{ "target", "action", "patch", "patch_expr", "increment", "increment_expr", "json_set", "array_update", "where_expression" });
    const target = try parseRowsConflictTargetAlloc(alloc, schema, conflict_value.object.get("target") orelse return error.InvalidRowsRequest);
    errdefer freeRowsConflictTarget(alloc, target);

    const action = switch (try parseConflictAction(conflict_value.object.get("action") orelse return error.InvalidRowsRequest)) {
        .update => db_mod.types.RelationalRowsConflictAction.update,
        .nothing => db_mod.types.RelationalRowsConflictAction.nothing,
    };

    const operations = try staticUpdateTransformOperationsAlloc(alloc, schema, conflict_value, true);
    errdefer freeTransformOps(alloc, operations);
    const patch_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, schema, conflict_value.object.get("patch_expr"), false, true);
    errdefer freeRowsExpressionAssignments(alloc, patch_expressions);
    const increment_expressions = try parseRowsMutationExpressionAssignmentsAlloc(alloc, schema, conflict_value.object.get("increment_expr"), true, true);
    errdefer freeRowsExpressionAssignments(alloc, increment_expressions);
    const json_set_expressions = try parseRowsMutationJsonSetExpressionAssignmentsAlloc(alloc, schema, conflict_value.object.get("json_set"), true);
    errdefer freeRowsJsonSetExpressionAssignments(alloc, json_set_expressions);

    const where_expression: ?db_mod.types.RelationalRowsExpressionCondition = if (conflict_value.object.get("where_expression")) |condition_value|
        try parseRowsExpressionConditionAlloc(alloc, schema, condition_value, true)
    else
        null;
    errdefer if (where_expression) |condition| freeRowsQueryExpressionCondition(alloc, condition);

    if (action == .nothing) {
        if (operations.len != 0 or patch_expressions.len != 0 or increment_expressions.len != 0 or json_set_expressions.len != 0 or where_expression != null) return error.InvalidRowsRequest;
    } else if (operations.len == 0 and patch_expressions.len == 0 and increment_expressions.len == 0 and json_set_expressions.len == 0) {
        return error.InvalidRowsRequest;
    }

    try validateRowsMutationUpdateTargetPaths(operations, patch_expressions, increment_expressions, json_set_expressions, &.{});

    return .{
        .target = target,
        .action = action,
        .operations = operations,
        .patch_expressions = patch_expressions,
        .increment_expressions = increment_expressions,
        .json_set_expressions = json_set_expressions,
        .where_expression = where_expression,
    };
}

fn parseRowsConflictTargetAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    target_value: std.json.Value,
) !db_mod.types.RelationalRowsConflictTarget {
    if (target_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(target_value.object, &.{ "primary", "unique" });
    if (target_value.object.get("primary") != null and target_value.object.get("unique") != null) return error.InvalidRowsRequest;
    if (target_value.object.get("primary")) |primary_value| {
        if (primary_value != .bool or !primary_value.bool) return error.InvalidRowsRequest;
        return .{ .kind = .primary };
    }
    const unique_value = target_value.object.get("unique") orelse return error.InvalidRowsRequest;
    if (unique_value != .object) return error.InvalidRowsRequest;
    try requireJsonObjectOnlyKeys(unique_value.object, &.{ "name", "where" });
    const name_value = unique_value.object.get("name") orelse return error.InvalidRowsRequest;
    if (name_value != .string or name_value.string.len == 0) return error.InvalidRowsRequest;
    const constraint = findUniqueConstraint(schema.unique_constraints, name_value.string) orelse return error.InvalidRowsRequest;
    const unique_name = try alloc.dupe(u8, constraint.name);
    errdefer alloc.free(unique_name);
    const unique_predicates = try parseRowsConflictTargetPredicateAlloc(alloc, schema, unique_value.object.get("where"), constraint);
    errdefer {
        freeQueryPredicates(alloc, unique_predicates);
        if (unique_predicates.len > 0) alloc.free(unique_predicates);
    }
    return .{
        .kind = .unique,
        .unique_name = unique_name,
        .unique_predicates = unique_predicates,
    };
}

fn parseRowsConflictTargetPredicateAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_where: ?std.json.Value,
    constraint: runtime_schema.UniqueConstraint,
) ![]const runtime_schema.RelationalCheck {
    if (constraint.where.len == 0) {
        if (maybe_where != null) return error.InvalidRowsRequest;
        return &.{};
    }
    const where_value = maybe_where orelse return error.InvalidRowsRequest;
    const predicates = try parseRowsQueryPredicatesAlloc(alloc, schema, where_value);
    errdefer {
        freeQueryPredicates(alloc, predicates);
        if (predicates.len > 0) alloc.free(predicates);
    }
    try validateConflictTargetPredicatesMatchConstraint(predicates, constraint);
    return predicates;
}

fn validateConflictTargetPredicatesMatchConstraint(
    predicates: []const runtime_schema.RelationalCheck,
    constraint: runtime_schema.UniqueConstraint,
) !void {
    if (predicates.len != constraint.where.len) return error.InvalidRowsRequest;
    for (predicates, constraint.where) |actual, expected| {
        if (!std.mem.eql(u8, actual.field, expected.field)) return error.InvalidRowsRequest;
        if (!uniquePredicateOpMatchesRelationalCheck(actual.op, expected.op)) return error.InvalidRowsRequest;
        if (expected.value_json) |expected_json| {
            const actual_json = actual.value_json orelse return error.InvalidRowsRequest;
            if (!std.mem.eql(u8, actual_json, expected_json)) return error.InvalidRowsRequest;
        } else if (actual.value_json != null) {
            return error.InvalidRowsRequest;
        }
    }
}

fn uniquePredicateOpMatchesRelationalCheck(actual: runtime_schema.RelationalCheckOp, expected: runtime_schema.UniquePredicateOp) bool {
    return switch (expected) {
        .is_null => actual == .is_null,
        .is_not_null => actual == .is_not_null,
        .eq => actual == .eq,
        .ne => actual == .ne,
    };
}

fn freeRowsConflictTarget(alloc: std.mem.Allocator, target: db_mod.types.RelationalRowsConflictTarget) void {
    if (target.unique_name.len > 0) alloc.free(@constCast(target.unique_name));
    freeQueryPredicates(alloc, target.unique_predicates);
    if (target.unique_predicates.len > 0) alloc.free(target.unique_predicates);
}

fn parseRowsMutationJsonSetExpressionAssignmentsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    maybe_json_set: ?std.json.Value,
    allow_proposed_source: bool,
) ![]db_mod.types.RelationalRowsJsonSetExpressionAssignment {
    const json_set = maybe_json_set orelse return &.{};
    if (json_set != .array) return error.InvalidRowsRequest;

    var assignments = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonSetExpressionAssignment).empty;
    errdefer {
        for (assignments.items) |assignment| {
            alloc.free(@constCast(assignment.field));
            for (assignment.path) |segment| alloc.free(@constCast(segment));
            if (assignment.path.len > 0) alloc.free(assignment.path);
            freeRowsQueryExpression(alloc, assignment.expression);
        }
        assignments.deinit(alloc);
    }

    for (json_set.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "field", "path", "value", "expr" });
        if (item.object.get("value") != null) continue;
        const expr_value = item.object.get("expr") orelse return error.InvalidRowsRequest;

        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .json) return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, field_value.string)) return error.InvalidRowsRequest;
        }

        const path_value = item.object.get("path") orelse return error.InvalidRowsRequest;
        if (path_value != .array or path_value.array.items.len == 0) return error.InvalidRowsRequest;
        const path = try alloc.alloc([]const u8, path_value.array.items.len);
        var path_initialized: usize = 0;
        errdefer {
            for (path[0..path_initialized]) |segment| alloc.free(@constCast(segment));
            alloc.free(path);
        }
        for (path_value.array.items) |segment| {
            if (segment != .string or segment.string.len == 0) return error.InvalidRowsRequest;
            if (std.mem.indexOfScalar(u8, segment.string, '.') != null) return error.InvalidRowsRequest;
            path[path_initialized] = try alloc.dupe(u8, segment.string);
            path_initialized += 1;
        }

        const expression = try parseRowsMutationExpressionAlloc(alloc, schema, expr_value);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        if (!allow_proposed_source and expressionUsesProposedSource(expression)) return error.InvalidRowsRequest;

        const field = try alloc.dupe(u8, column.path);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);

        try assignments.append(alloc, .{
            .field = field,
            .path = path,
            .expression = expression,
        });
        field_transferred = true;
        expression_transferred = true;
    }

    return try assignments.toOwnedSlice(alloc);
}

fn validateRowsMutationUpdateTargetPaths(
    operations: []const db_mod.types.TransformOp,
    patch_expressions: []const db_mod.types.RelationalRowsExpressionAssignment,
    increment_expressions: []const db_mod.types.RelationalRowsExpressionAssignment,
    json_set_expressions: []const db_mod.types.RelationalRowsJsonSetExpressionAssignment,
    source_assignments: []const db_mod.types.RelationalRowsJoinedMutationFieldAssignment,
) !void {
    for (operations, 0..) |lhs, i| {
        for (operations[i + 1 ..]) |rhs| {
            if (rowsMutationOperationPathsConflict(lhs, rhs)) return error.InvalidRowsRequest;
        }
        for (patch_expressions) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.path, assignment.field)) return error.InvalidRowsRequest;
        }
        for (increment_expressions) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.path, assignment.field)) return error.InvalidRowsRequest;
        }
        for (json_set_expressions) |assignment| {
            if (rowsMutationDottedPathConflictsJsonSetPath(lhs.path, assignment.field, assignment.path)) return error.InvalidRowsRequest;
        }
        for (source_assignments) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.path, assignment.field)) return error.InvalidRowsRequest;
        }
    }
    for (patch_expressions, 0..) |lhs, i| {
        for (patch_expressions[i + 1 ..]) |rhs| {
            if (rowsMutationDottedPathsConflict(lhs.field, rhs.field)) return error.InvalidRowsRequest;
        }
        for (increment_expressions) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.field, assignment.field)) return error.InvalidRowsRequest;
        }
        for (json_set_expressions) |assignment| {
            if (rowsMutationDottedPathConflictsJsonSetPath(lhs.field, assignment.field, assignment.path)) return error.InvalidRowsRequest;
        }
        for (source_assignments) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.field, assignment.field)) return error.InvalidRowsRequest;
        }
    }
    for (increment_expressions, 0..) |lhs, i| {
        for (increment_expressions[i + 1 ..]) |rhs| {
            if (rowsMutationDottedPathsConflict(lhs.field, rhs.field)) return error.InvalidRowsRequest;
        }
        for (json_set_expressions) |assignment| {
            if (rowsMutationDottedPathConflictsJsonSetPath(lhs.field, assignment.field, assignment.path)) return error.InvalidRowsRequest;
        }
        for (source_assignments) |assignment| {
            if (rowsMutationDottedPathsConflict(lhs.field, assignment.field)) return error.InvalidRowsRequest;
        }
    }
    for (json_set_expressions, 0..) |lhs, i| {
        for (json_set_expressions[i + 1 ..]) |rhs| {
            if (rowsMutationJsonSetPathsConflict(lhs.field, lhs.path, rhs.field, rhs.path)) return error.InvalidRowsRequest;
        }
        for (source_assignments) |assignment| {
            if (rowsMutationDottedPathConflictsJsonSetPath(assignment.field, lhs.field, lhs.path)) return error.InvalidRowsRequest;
        }
    }
    for (source_assignments, 0..) |lhs, i| {
        for (source_assignments[i + 1 ..]) |rhs| {
            if (rowsMutationDottedPathsConflict(lhs.field, rhs.field)) return error.InvalidRowsRequest;
        }
    }
}

fn rowsMutationOperationPathsConflict(lhs: db_mod.types.TransformOp, rhs: db_mod.types.TransformOp) bool {
    if (!rowsMutationDottedPathsConflict(lhs.path, rhs.path)) return false;
    return !(rowsMutationTransformOpIsArrayUpdate(lhs.op) and rowsMutationTransformOpIsArrayUpdate(rhs.op) and std.mem.eql(u8, lhs.path, rhs.path));
}

fn rowsMutationTransformOpIsArrayUpdate(op: db_mod.types.TransformOpType) bool {
    return switch (op) {
        .push, .pull, .add_to_set, .pop => true,
        else => false,
    };
}

fn rowsMutationDottedPathsConflict(lhs: []const u8, rhs: []const u8) bool {
    if (std.mem.eql(u8, lhs, rhs)) return true;
    return rowsMutationDottedPathIsAncestor(lhs, rhs) or rowsMutationDottedPathIsAncestor(rhs, lhs);
}

fn rowsMutationDottedPathIsAncestor(parent: []const u8, child: []const u8) bool {
    return parent.len < child.len and
        std.mem.startsWith(u8, child, parent) and
        child[parent.len] == '.';
}

fn rowsMutationDottedPathConflictsJsonSetPath(path: []const u8, json_field: []const u8, json_path: []const []const u8) bool {
    if (rowsMutationDottedPathsConflict(path, json_field)) return true;
    if (path.len <= json_field.len + 1) return false;
    if (!std.mem.startsWith(u8, path, json_field) or path[json_field.len] != '.') return false;
    return rowsMutationJsonSegmentsConflictDottedPath(json_path, path[json_field.len + 1 ..]);
}

fn rowsMutationJsonSetPathsConflict(
    lhs_field: []const u8,
    lhs_path: []const []const u8,
    rhs_field: []const u8,
    rhs_path: []const []const u8,
) bool {
    if (!std.mem.eql(u8, lhs_field, rhs_field)) return rowsMutationDottedPathsConflict(lhs_field, rhs_field);
    const shared = @min(lhs_path.len, rhs_path.len);
    for (lhs_path[0..shared], rhs_path[0..shared]) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs, rhs)) return false;
    }
    return true;
}

fn rowsMutationJsonSegmentsConflictDottedPath(json_path: []const []const u8, dotted_path: []const u8) bool {
    if (json_path.len == 0 or dotted_path.len == 0) return false;
    var offset: usize = 0;
    for (json_path, 0..) |segment, i| {
        if (offset >= dotted_path.len) return true;
        if (!std.mem.startsWith(u8, dotted_path[offset..], segment)) return false;
        offset += segment.len;
        const dotted_done = offset == dotted_path.len;
        const json_done = i + 1 == json_path.len;
        if (!dotted_done and dotted_path[offset] != '.') return false;
        if (dotted_done or json_done) return true;
        offset += 1;
    }
    return offset == dotted_path.len;
}

fn expressionUsesProposedSource(expression: db_mod.types.RelationalRowsExpression) bool {
    if (expression.kind == .field and expression.field_source == .proposed) return true;
    for (expression.operands) |operand| if (expressionUsesProposedSource(operand)) return true;
    for (expression.case_branches) |branch| {
        if (expressionConditionUsesProposedSource(branch.when) or expressionUsesProposedSource(branch.then)) return true;
    }
    for (expression.case_else) |fallback| if (expressionUsesProposedSource(fallback)) return true;
    return false;
}

fn expressionConditionUsesProposedSource(condition: db_mod.types.RelationalRowsExpressionCondition) bool {
    if (expressionUsesProposedSource(condition.lhs)) return true;
    for (condition.rhs) |rhs| if (expressionUsesProposedSource(rhs)) return true;
    return false;
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

fn appendPatchExpressionTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    existing_row: std.json.Value,
    proposed_row: ?std.json.Value,
    patch_expr: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    var it = patch_expr.object.iterator();
    while (it.next()) |entry| {
        const column = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, entry.key_ptr.*)) return error.InvalidRowsRequest;
        }
        const expression = try parseRowsMutationExpressionAlloc(alloc, schema, entry.value_ptr.*);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        const value_json = try expressionValueJsonWithSourcesAlloc(alloc, existing_row, proposed_row, expression);
        var value_json_transferred = false;
        errdefer if (!value_json_transferred) alloc.free(value_json);
        const path = try alloc.dupe(u8, column.path);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(path);
        try operations.append(alloc, .{
            .op = .set,
            .path = path,
            .value_json = value_json,
        });
        path_transferred = true;
        value_json_transferred = true;
        expression_transferred = true;
        freeRowsQueryExpression(alloc, expression);
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

fn appendIncrementExpressionTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    existing_row: std.json.Value,
    proposed_row: ?std.json.Value,
    increment_expr: std.json.Value,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    var it = increment_expr.object.iterator();
    while (it.next()) |entry| {
        const column = findRelationalColumn(schema.relational_columns, entry.key_ptr.*) orelse return error.InvalidRowsRequest;
        if (column.field_type != .numeric) return error.InvalidRowsRequest;
        if (schema.primary_key) |primary_key| {
            if (primaryKeyContains(primary_key, entry.key_ptr.*)) return error.InvalidRowsRequest;
        }
        const expression = try parseRowsMutationExpressionAlloc(alloc, schema, entry.value_ptr.*);
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
        try validateRowsQueryNumericExpression(alloc, schema, expression);
        const value_json = try expressionValueJsonWithSourcesAlloc(alloc, existing_row, proposed_row, expression);
        var value_json_transferred = false;
        errdefer if (!value_json_transferred) alloc.free(value_json);
        try validateIncrementExpressionValueJson(alloc, value_json);
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
        expression_transferred = true;
        freeRowsQueryExpression(alloc, expression);
    }
}

fn validateIncrementExpressionValueJson(alloc: std.mem.Allocator, value_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    switch (parsed.value) {
        .integer, .float, .number_string => {},
        else => return error.InvalidRowsRequest,
    }
}

fn appendJsonSetTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    json_set: std.json.Value,
    existing_row: ?std.json.Value,
    proposed_row: ?std.json.Value,
    skip_expressions: bool,
    operations: *std.ArrayListUnmanaged(db_mod.types.TransformOp),
) !void {
    if (json_set != .array) return error.InvalidRowsRequest;
    for (json_set.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireJsonObjectOnlyKeys(item.object, &.{ "field", "path", "value", "expr" });
        const field_value = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field_value != .string or field_value.string.len == 0) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema.relational_columns, field_value.string) orelse return error.InvalidRowsRequest;
        if (column.field_type != .json) return error.InvalidRowsRequest;
        const path_value = item.object.get("path") orelse return error.InvalidRowsRequest;
        if (path_value != .array or path_value.array.items.len == 0) return error.InvalidRowsRequest;
        const value = item.object.get("value");
        const expr = item.object.get("expr");
        if ((value == null) == (expr == null)) return error.InvalidRowsRequest;
        if (expr != null and skip_expressions) continue;
        const transform_path = try jsonSetTransformPathAlloc(alloc, column.path, path_value);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(transform_path);
        const value_json = if (value) |literal|
            try jsonValueStringifyAlloc(alloc, literal)
        else blk: {
            const existing = existing_row orelse return error.InvalidRowsRequest;
            const expression = try parseRowsMutationExpressionAlloc(alloc, schema, expr.?);
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeRowsQueryExpression(alloc, expression);
            const out = try expressionValueJsonWithSourcesAlloc(alloc, existing, proposed_row, expression);
            expression_transferred = true;
            freeRowsQueryExpression(alloc, expression);
            break :blk out;
        };
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
        try requireJsonObjectOnlyKeys(item.object, &.{ "field", "op", "value" });
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
        try validateRowsArrayUpdateElement(column, value);
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

fn validateRowsArrayUpdateElement(column: runtime_schema.RelationalColumn, value: std.json.Value) !void {
    const item_type = column.array_item_type orelse return error.InvalidRowsRequest;
    if (!rowsArrayItemValueMatches(item_type, value)) return error.InvalidRowsRequest;
}

fn rowsArrayItemValueMatches(item_type: runtime_schema.AntflyType, value: std.json.Value) bool {
    return switch (item_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => value == .string,
        .numeric => value == .integer or value == .float or value == .number_string,
        .datetime => value == .integer or value == .float or value == .number_string,
        .boolean => value == .bool,
        .geopoint => value == .array or value == .object,
        .json => true,
        .array => value == .array,
        .embedding => false,
    };
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

fn hasIncrementExpression(op_value: std.json.Value) bool {
    if (op_value != .object) return false;
    return op_value.object.get("increment_expr") != null;
}

fn hasPatchExpression(op_value: std.json.Value) bool {
    if (op_value != .object) return false;
    return op_value.object.get("patch_expr") != null;
}

fn hasMutationExpression(op_value: std.json.Value) bool {
    return hasPatchExpression(op_value) or hasIncrementExpression(op_value) or hasJsonSetExpression(op_value);
}

fn hasJsonSetExpression(op_value: std.json.Value) bool {
    if (op_value != .object) return false;
    const json_set = op_value.object.get("json_set") orelse return false;
    if (json_set != .array) return false;
    for (json_set.array.items) |item| {
        if (item == .object and item.object.get("expr") != null) return true;
    }
    return false;
}

fn hasConflictActionCondition(op_value: std.json.Value) bool {
    if (op_value != .object) return false;
    return op_value.object.get("where_expression") != null;
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

    const returning_all = if (returning_value) |fields|
        fields.array.items.len == 1 and fields.array.items[0] == .string and std.mem.eql(u8, fields.array.items[0].string, "*")
    else
        false;
    try validateProjectReturningOutputs(returning_value, returning_all, expressions, parsed.value);
    if (returning_all and expressions.len == 0) return try alloc.dupe(u8, row_json);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    if (returning_all) {
        for (parsed.value.object.keys(), parsed.value.object.values()) |field, value| {
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(field, .{})});
            try std.json.Stringify.value(value, .{}, writer);
        }
    } else if (returning_value) |fields| {
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
        if (returning_all and parsed.value.object.get(projection.output) != null) return error.InvalidRowsRequest;
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

fn validateProjectReturningOutputs(
    returning_value: ?std.json.Value,
    returning_all: bool,
    expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    row: std.json.Value,
) !void {
    if (row != .object) return error.InvalidRowsRequest;
    if (returning_all) {
        for (expressions) |projection| {
            if (projection.output.len == 0) return error.InvalidRowsRequest;
            if (row.object.get(projection.output) != null) return error.InvalidRowsRequest;
            if (projectReturningExpressionOutputCount(expressions, projection.output) > 1) return error.InvalidRowsRequest;
        }
        return;
    }
    if (returning_value) |fields| {
        if (fields != .array) return error.InvalidRowsRequest;
        for (fields.array.items) |field_value| {
            if (field_value != .string or field_value.string.len == 0 or std.mem.eql(u8, field_value.string, "*")) return error.InvalidRowsRequest;
            if (projectReturningFieldOutputCount(fields.array.items, field_value.string) > 1) return error.InvalidRowsRequest;
            if (projectReturningExpressionOutputCount(expressions, field_value.string) > 0) return error.InvalidRowsRequest;
        }
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidRowsRequest;
        if (projectReturningExpressionOutputCount(expressions, projection.output) > 1) return error.InvalidRowsRequest;
        if (returning_value) |fields| {
            if (projectReturningFieldOutputCount(fields.array.items, projection.output) > 0) return error.InvalidRowsRequest;
        }
    }
}

fn projectReturningFieldOutputCount(fields: []const std.json.Value, output: []const u8) usize {
    var count: usize = 0;
    for (fields) |field_value| {
        if (field_value == .string and std.mem.eql(u8, field_value.string, output)) count += 1;
    }
    return count;
}

fn projectReturningExpressionOutputCount(expressions: []const db_mod.types.RelationalRowsExpressionProjection, output: []const u8) usize {
    var count: usize = 0;
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
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
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"tenant_id\":\"t1\",\"order_id\":\"o9\",\"status\":\"open\"}},{\"op\":\"delete\",\"where\":{\"primary\":{\"tenant_id\":\"t1\",\"order_id\":\"o10\"}}}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.predicates.len);
    try std.testing.expect(!std.mem.eql(u8, batch.writes[0].key, batch.deletes[0]));
    try std.testing.expect(std.mem.startsWith(u8, batch.writes[0].key, physical_primary_key_prefix));
    try std.testing.expect(std.mem.startsWith(u8, batch.deletes[0], physical_primary_key_prefix));
}

test "relational rows batch rejects duplicate physical row targets" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"status\":\"ready\"}},{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"status\":\"done\"}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"status\":\"ready\"}},{\"op\":\"delete\",\"where\":{\"primary\":{\"id\":\"u1\"}}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"ready\"}},{\"op\":\"delete\",\"where\":{\"primary\":{\"id\":\"u1\"}}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"ready\"}},{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"done\"}}]}",
            schema,
        ),
    );
}

test "relational rows batch returning materializes defaults generated columns and checks" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_key":{"type":"keyword","generated":{"op":"lower","field":"email"}},"email_upper_key":{"type":"keyword","generated":{"op":"upper","field":"email"}},"status":{"type":"keyword","default":"active"},"amount":{"type":"numeric","default":1}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"checks":[{"name":"amount_positive","field":"amount","op":"gte","value":0},{"name":"status_present","field":"status","op":"is_not_null"}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var batch = try parseRowsBatchRequest(
        std.testing.allocator,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"email\":\"Ada@Example.Test\"},\"returning\":[\"id\",\"email_key\",\"email_upper_key\",\"status\",\"amount\"]}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"email\":\"Ada@Example.Test\",\"status\":\"active\",\"amount\":1,\"email_key\":\"ada@example.test\",\"email_upper_key\":\"ADA@EXAMPLE.TEST\"}", batch.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"email_key\":\"ada@example.test\",\"email_upper_key\":\"ADA@EXAMPLE.TEST\",\"status\":\"active\",\"amount\":1}", batch.returning_rows[0]);

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
    try std.testing.expectEqual(@as(usize, 3), update_batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("email", update_batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"GRACE@EXAMPLE.TEST\"", update_batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("email_key", update_batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("\"grace@example.test\"", update_batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqualStrings("email_upper_key", update_batch.transforms[0].operations[2].path);
    try std.testing.expectEqualStrings("\"GRACE@EXAMPLE.TEST\"", update_batch.transforms[0].operations[2].value_json.?);
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
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u4\",\"email\":\"bad@example.test\"},\"patch\":{\"status\":\"ignored\"}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"upsert\",\"row\":{\"id\":\"u5\",\"email\":\"bad@example.test\"},\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"nothing\"}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"row\":{\"id\":\"u1\",\"email\":\"bad@example.test\"},\"patch\":{\"status\":\"ignored\"}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"delete\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"ignored\"}}]}",
            schema,
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"active\"},\"where_expression\":{\"lhs\":{\"field\":\"status\"},\"op\":\"eq\",\"rhs\":{\"value\":\"pending\"}}}]}",
            schema,
        ),
    );
}

test "relational rows ignore unvalidated checks for write validation" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"checks":[{"name":"amount_nonnegative_pending","field":"amount","op":"gte","value":0,"validation_state":"unvalidated"},{"name":"status_present","field":"status","op":"is_not_null"}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var batch = try parseRowsBatchRequest(
        std.testing.allocator,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"amount\":-1,\"status\":\"pending\"}}]}",
        schema,
    );
    defer batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), batch.writes.len);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequest(
            std.testing.allocator,
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"amount\":1}}]}",
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
            const suffix: []const u8 = switch (self.calls) {
                0 => "test",
                1 => "update",
                2 => "delete",
                else => "extra",
            };
            self.calls += 1;
            return try std.fmt.allocPrint(alloc, "\x00antfly-rel-pk:{s}", .{suffix});
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
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}},\"expected_version\":17,\"patch\":{\"status\":\"active\"}},{\"op\":\"delete\",\"where\":{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"grace@example.test\"}}},\"expected_version\":\"18\"}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), resolver.calls);
    try std.testing.expectEqual(@as(usize, 1), update_req.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), update_req.deletes.len);
    try std.testing.expectEqual(@as(usize, 2), update_req.predicates.len);
    try std.testing.expectEqualStrings(update_req.transforms[0].key, update_req.predicates[0].key);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:update", update_req.transforms[0].key);
    try std.testing.expectEqual(@as(u64, 17), update_req.predicates[0].expected_version);
    try std.testing.expectEqualStrings(update_req.deletes[0], update_req.predicates[1].key);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:delete", update_req.deletes[0]);
    try std.testing.expectEqual(@as(u64, 18), update_req.predicates[1].expected_version);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"},\"unknown\":true}}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"primary\":{\"id\":\"u1\"},\"unique\":{\"name\":\"users_email_key\",\"values\":{\"email\":\"ada@example.test\"}}}]}",
        schema,
        resolver.iface(),
    ));
}

test "relational rows partial unique selector includes predicate fields" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_active_email_key","columns":["email"],"where":{"all":[{"field":"email","op":"is_not_null"},{"field":"status","op":"eq","value":"active"}]}}]}
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

    var resolver = Resolver{};
    var get_req = try parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_active_email_key\",\"values\":{\"email\":\"ada@example.test\",\"status\":\"active\"}}}],\"include_physical_key\":true}",
        schema,
        resolver.iface(),
    );
    defer get_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:active-owner", get_req.keys[0].?);

    var update_req = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"unique\":{\"name\":\"users_active_email_key\",\"values\":{\"email\":\"ada@example.test\",\"status\":\"active\"}}},\"expected_version\":17,\"patch\":{\"status\":\"disabled\"}}]}",
        schema,
        resolver.iface(),
    );
    defer update_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:active-owner", update_req.transforms[0].key);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_active_email_key\",\"values\":{\"email\":\"ada@example.test\"}}}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_active_email_key\",\"values\":{\"email\":\"ada@example.test\",\"status\":\"inactive\"}}}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
}

test "relational rows partial unique selector supports null predicate fields" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"deleted_at":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_live_email_key","columns":["email"],"where":{"all":[{"field":"deleted_at","op":"is_null"}]}}]}
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
            try std.testing.expectEqualStrings("users_live_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            self.calls += 1;
            return try alloc.dupe(u8, "\x00antfly-rel-pk:live-owner");
        }
    };

    var resolver = Resolver{};
    var get_req = try parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_live_email_key\",\"values\":{\"email\":\"ada@example.test\",\"deleted_at\":null}}}]}",
        schema,
        resolver.iface(),
    );
    defer get_req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqualStrings("\x00antfly-rel-pk:live-owner", get_req.keys[0].?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsGetRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"keys\":[{\"unique\":{\"name\":\"users_live_email_key\",\"values\":{\"email\":\"ada@example.test\",\"deleted_at\":\"2026-01-01\"}}}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
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

test "relational rows conflict target upsert rejects unvalidated unique owner" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_lower_email_key","expressions":[{"op":"lower","field":"email"}],"validation_state":"unvalidated"}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolve };
        }

        fn resolve(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
        ) !?[]u8 {
            return error.TestUnexpectedResult;
        }
    };

    var resolver = Resolver{};
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"A@EXAMPLE.COM\",\"name\":\"Ann\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_lower_email_key\"}},\"action\":\"update\",\"patch\":{\"name\":\"Ada\"}}}]}",
            schema,
            resolver.iface(),
        ),
    );
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

    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u6\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\",\"where\":{\"all\":[{\"field\":\"email\",\"op\":\"is_not_null\",\"unknown\":true},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}}},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u7\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\",\"where\":{\"all\":[{\"field\":\"email\",\"op\":\"is_not_null\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}],\"unknown\":true}}},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u8\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"," ++ conflict_where ++ ",\"unknown\":true}},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u9\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"," ++ conflict_where ++ "},\"primary\":true},\"action\":\"nothing\"}}]}",
            schema,
            resolver.iface(),
        ),
    );
    try std.testing.expectError(
        error.InvalidRowsRequest,
        parseRowsBatchRequestWithResolver(
            std.testing.allocator,
            "users",
            "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u10\",\"email\":\"a@example.test\",\"status\":\"active\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"users_active_email_key\"," ++ conflict_where ++ "}},\"action\":\"nothing\",\"unknown\":true}}]}",
            schema,
            resolver.iface(),
        ),
    );
}

test "relational rows conflict target upsert supports typed increment expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8) !?[]u8 {
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("usage_records_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            return try alloc.dupe(u8, "\x00antfly-rel-pk:u1");
        }

        fn lookupPrimary(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !?ResolvedPrimaryRow {
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("\x00antfly-rel-pk:u1", physical_key);
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"email\":\"a@example.test\",\"amount\":5}"),
                .version = 21,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"a@example.test\",\"amount\":null},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"usage_records_email_key\"}},\"action\":\"update\",\"increment_expr\":{\"amount\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"amount\",\"source\":\"proposed\"},{\"value\":4}]}}},\"returning\":[\"amount\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("4", batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 21), batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"amount\":9}", batch.returning_rows[0]);
}

test "relational rows conflict target upsert supports typed patch expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"next_status":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    const Resolver = struct {
        fn iface(self: *@This()) UniqueSelectorResolver {
            return .{ .ptr = self, .resolve = resolveUnique, .lookup_primary = lookupPrimary };
        }

        fn resolveUnique(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, constraint_name: []const u8, encoded_value: []const u8) !?[]u8 {
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("usage_records_email_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            return try alloc.dupe(u8, "\x00antfly-rel-pk:u1");
        }

        fn lookupPrimary(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !?ResolvedPrimaryRow {
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("\x00antfly-rel-pk:u1", physical_key);
            return .{
                .json = try alloc.dupe(u8, "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"existing\"}"),
                .version = 22,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"a@example.test\",\"next_status\":null},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"usage_records_email_key\"}},\"action\":\"update\",\"patch_expr\":{\"status\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"next_status\",\"source\":\"proposed\"},{\"field\":\"status\",\"source\":\"existing\"},{\"value\":\"fallback\"}]}}},\"returning\":[\"status\"]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.set, batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("status", batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"existing\"", batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 22), batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"status\":\"existing\"}", batch.returning_rows[0]);

    var skipped = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"a@example.test\",\"next_status\":null},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"usage_records_email_key\"}},\"action\":\"update\",\"patch_expr\":{\"status\":{\"field\":\"next_status\",\"source\":\"proposed\"}},\"where_expression\":{\"lhs\":{\"field\":\"next_status\",\"source\":\"proposed\"},\"op\":\"is_not_null\"}},\"returning\":[\"status\"]}]}",
        schema,
        resolver.iface(),
    );
    defer skipped.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), skipped.writes.len);
    try std.testing.expectEqual(@as(usize, 0), skipped.transforms.len);
    try std.testing.expectEqual(@as(usize, 0), skipped.predicates.len);
    try std.testing.expectEqual(@as(usize, 0), skipped.returning_rows.len);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"email\":\"a@example.test\",\"next_status\":\"proposed\"},\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"usage_records_email_key\"}},\"action\":\"update\",\"patch\":{\"status\":\"literal\"},\"patch_expr\":{\"status\":{\"field\":\"next_status\",\"source\":\"proposed\"}}}}]}",
        schema,
        resolver.iface(),
    ));
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
            const row_json = if (self.lookup_calls == 0)
                "{\"id\":\"u1\",\"name\":\"Ada\",\"status\":\"active\"}"
            else
                "{\"id\":\"u3\",\"name\":\"Lin\",\"status\":\"inactive\"}";
            self.lookup_calls += 1;
            return .{
                .json = try alloc.dupe(u8, row_json),
                .version = 42,
            };
        }
    };

    var resolver = Resolver{};
    var batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"name\":\"Grace\",\"status\":\"new\"},\"returning\":[\"id\",\"name\"],\"returning_expressions\":[{\"as\":\"name_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"name\"}]}}]},{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"disabled\"},\"returning\":[\"id\",\"status\"],\"returning_expressions\":[{\"as\":\"label\",\"expr\":{\"op\":\"concat\",\"args\":[{\"field\":\"id\"},{\"value\":\":\"},{\"field\":\"status\"}]}}]},{\"op\":\"delete\",\"where\":{\"primary\":{\"id\":\"u3\"}},\"returning\":[\"*\"],\"returning_expressions\":[{\"as\":\"status_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}}]}]}",
        schema,
        resolver.iface(),
    );
    defer batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), resolver.lookup_calls);
    try std.testing.expectEqual(@as(usize, 3), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u2\",\"name\":\"Grace\",\"name_key\":\"grace\"}", batch.returning_rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"disabled\",\"label\":\"u1:disabled\"}", batch.returning_rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\",\"name\":\"Lin\",\"status\":\"inactive\",\"status_key\":\"inactive\"}", batch.returning_rows[2]);
    try std.testing.expectEqual(@as(usize, 3), batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 0), batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(u64, 42), batch.predicates[1].expected_version);
    try std.testing.expectEqual(@as(u64, 42), batch.predicates[2].expected_version);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"name\":\"Dup\"},\"returning\":[\"id\",\"id\"]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"name\":\"Dup\"},\"returning\":[\"id\"],\"returning_expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"name\"}]}}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"name\":\"Dup\"},\"returning\":[\"*\"],\"returning_expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"name\"}]}}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"patch\":{\"status\":\"disabled\"},\"patch_expr\":{\"status\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\",\"source\":\"existing\"}]}}}]}",
        schema,
        resolver.iface(),
    ));

    const response = try encodeRowsBatchResponseAlloc(std.testing.allocator, batch);
    defer std.testing.allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"returning\":[{\"id\":\"u2\",\"name\":\"Grace\",\"name_key\":\"grace\"},{\"id\":\"u1\",\"status\":\"disabled\",\"label\":\"u1:disabled\"},{\"id\":\"u3\",\"name\":\"Lin\",\"status\":\"inactive\",\"status_key\":\"inactive\"}]") != null);
}

test "relational rows batch supports typed numeric increments" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"bonus":{"type":"numeric"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
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

    var expression_batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"increment_expr\":{\"amount\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"bonus\",\"source\":\"existing\"},{\"value\":3}]}},\"returning\":[\"amount\"]}]}",
        schema,
        resolver.iface(),
    );
    defer expression_batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), expression_batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), expression_batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, expression_batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", expression_batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("3", expression_batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"amount\":8}", expression_batch.returning_rows[0]);

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
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"array_update\":[{\"field\":\"tags\",\"op\":\"append\",\"value\":\"db\",\"unknown\":true}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "usage_records",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"array_update\":[{\"field\":\"tags\",\"op\":\"append\",\"value\":3}]}]}",
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

    var expression_batch = try parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\",\"source\":\"existing\"}]}}],\"returning\":[\"attrs.billing.plan\"]}]}",
        schema,
        resolver.iface(),
    );
    defer expression_batch.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), expression_batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), expression_batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("attrs.billing.plan", expression_batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", expression_batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), expression_batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"attrs.billing.plan\":\"active\"}", expression_batch.returning_rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\"},{\"field\":\"attrs\",\"path\":[\"billing\"],\"expr\":{\"field\":\"status\",\"source\":\"existing\"}}]}]}",
        schema,
        resolver.iface(),
    ));

    var mutation_source_expression = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:json-set\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"expr\":{\"field\":\"status\",\"source\":\"existing\"}}]}",
        schema,
    );
    defer mutation_source_expression.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), mutation_source_expression.req.operations.len);
    try std.testing.expectEqual(@as(usize, 1), mutation_source_expression.req.json_set_expressions.len);
    try std.testing.expectEqualStrings("attrs", mutation_source_expression.req.json_set_expressions[0].field);
    try std.testing.expectEqual(@as(usize, 2), mutation_source_expression.req.json_set_expressions[0].path.len);
    try std.testing.expectEqualStrings("billing", mutation_source_expression.req.json_set_expressions[0].path[0]);
    try std.testing.expectEqualStrings("plan", mutation_source_expression.req.json_set_expressions[0].path[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"status\",\"path\":[\"nested\"],\"value\":\"bad\"}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\",\"unknown\":true}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"]}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsBatchRequestWithResolver(
        std.testing.allocator,
        "users",
        "{\"operations\":[{\"op\":\"update\",\"where\":{\"primary\":{\"id\":\"u1\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\",\"expr\":{\"field\":\"status\",\"source\":\"existing\"}}]}]}",
        schema,
        resolver.iface(),
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:json-set\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:json-set\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\",\"expr\":{\"field\":\"status\",\"source\":\"existing\"}}]}",
        schema,
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

    var distinct_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"tenant_id\",\"id\",\"created_at\"],\"distinct_on\":[\"tenant_id\"],\"order_by\":[{\"field\":\"tenant_id\",\"direction\":\"asc\"},{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":10}",
        schema,
    );
    defer distinct_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), distinct_request.distinct_on.len);
    try std.testing.expectEqualStrings("tenant_id", distinct_request.distinct_on[0]);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"tenant_id\",\"id\"],\"distinct_on\":[\"tenant_id\"],\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"distinct_on\":[\"tenant_id\"],\"order_by\":[{\"field\":\"tenant_id\"}],\"row_claim\":{\"mode\":\"for_update\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"source_cte\":\"ready_rows\",\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:cte\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}}",
        schema,
    ));

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

    var distinct_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, distinct_request, rows[0..]);
    defer distinct_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), distinct_result.total);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"t1\",\"id\":\"u2\",\"created_at\":40}", distinct_result.rows[0]);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"t2\",\"id\":\"u5\",\"created_at\":50}", distinct_result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"unknown\":true}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\",\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"any\":[{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}],\"unknown\":true}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"not\":[{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}],\"unknown\":true}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\",\"unknown\":true}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"doc_key_range\":{\"start\":\"row:a\",\"end\":\"row:z\",\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"order_by\":[{\"direction\":\"desc\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"order_by\":[{\"field\":\"created_at\",\"expr\":{\"field\":\"amount\"}}]}",
        schema,
    ));
}

test "relational rows aggregate contract accepts typed expression inputs and filters" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer":{"type":"keyword"},"status":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"discount":{"type":"numeric"},"created_at":{"type":"numeric"},"tags":{"type":"array","items":{"type":"keyword"}},"attrs":{"type":"json"}},"required":["id","customer","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{\"where\":{\"field\":\"created_at\",\"op\":\"gte\",\"value\":10}},\"group_by\":[\"customer\"],\"group_expressions\":[{\"as\":\"status_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}}],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"},{\"name\":\"status_count\",\"op\":\"count\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"distinct\":true},{\"name\":\"net_amount\",\"op\":\"sum\",\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"amount\"},{\"field\":\"discount\"}]},\"filter\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"filter_array_any\":[{\"field\":\"tags\",\"op\":\"array_any\",\"value\":\"hot\"}],\"filter_array_contains\":[{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\"]}],\"filter_array_eq\":[{\"field\":\"tags\",\"op\":\"array_eq\",\"value\":[\"hot\",\"new\"]}],\"filter_in\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"open\",\"pending\"]}],\"filter_json_contains\":[{\"field\":\"attrs\",\"op\":\"json_contains\",\"value\":{\"source\":\"api\"}}],\"filter_json_path_eq\":[{\"field\":\"attrs\",\"op\":\"json_path_eq\",\"path\":\"source\",\"value\":\"api\"}],\"filter_json_path_exists\":[{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"flags\"}],\"filter_text_patterns\":[{\"field\":\"status\",\"op\":\"text_pattern\",\"pattern\":\"op%\",\"case_insensitive\":true}],\"filter_expressions\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"value\":\"open\"}}],\"filter_expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"write\"]}],\"filter_any\":[{\"all\":[{\"lhs\":{\"field\":\"status\"},\"op\":\"eq\",\"rhs\":{\"value\":\"open\"}}]},{\"all\":[{\"lhs\":{\"op\":\"array_length\",\"args\":[{\"field\":\"tags\"}]},\"op\":\"gt\",\"rhs\":{\"value\":0}}]}],\"filter_not\":[{\"all\":[{\"lhs\":{\"field\":\"status\"},\"op\":\"eq\",\"rhs\":{\"value\":\"blocked\"}}]}]},{\"name\":\"amount_total\",\"op\":\"sum\",\"expr\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"amount\"},{\"value\":0}]}},{\"name\":\"tag_total\",\"op\":\"sum\",\"expr\":{\"op\":\"array_length\",\"args\":[{\"field\":\"tags\"}]}},{\"name\":\"attrs_seen\",\"op\":\"count\",\"field\":\"attrs\"},{\"name\":\"tag_sets\",\"op\":\"array_agg\",\"field\":\"tags\",\"array_max_items\":4},{\"name\":\"statuses\",\"op\":\"array_agg\",\"field\":\"status\",\"array_max_items\":4,\"array_order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}]}],\"having\":{\"all\":[{\"field\":\"net_amount\",\"op\":\"gt\",\"value\":0}]},\"having_expressions\":[{\"lhs\":{\"op\":\"sub\",\"args\":[{\"field\":\"net_amount\"},{\"field\":\"row_count\"}]},\"op\":\"gt\",\"rhs\":{\"value\":1}}],\"having_any\":[{\"all\":[{\"lhs\":{\"field\":\"status_key\"},\"op\":\"eq\",\"rhs\":{\"value\":\"open\"}}]},{\"all\":[{\"lhs\":{\"field\":\"row_count\"},\"op\":\"gt\",\"rhs\":{\"value\":5}}]}],\"having_not\":[{\"all\":[{\"lhs\":{\"field\":\"net_amount\"},\"op\":\"lt\",\"rhs\":{\"value\":0}}]}],\"order_by\":[{\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"net_amount\"},{\"field\":\"row_count\"}]},\"direction\":\"desc\"},{\"field\":\"status_key\",\"null_test\":\"is_null\"}],\"limit\":5}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.source.predicates.len);
    try std.testing.expectEqualStrings("created_at", request.source.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.group_by.len);
    try std.testing.expectEqualStrings("customer", request.group_by[0]);
    try std.testing.expectEqual(@as(usize, 1), request.group_expressions.len);
    try std.testing.expectEqualStrings("status_key", request.group_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.group_expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 8), request.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, request.aggregations[0].op);
    try std.testing.expect(request.aggregations[0].field == null);
    try std.testing.expect(request.aggregations[0].expression == null);
    try std.testing.expect(request.aggregations[1].distinct);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.aggregations[1].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, request.aggregations[2].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.aggregations[2].expression.?.kind);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_array_any.len);
    try std.testing.expectEqualStrings("tags", request.aggregations[2].filter_array_any[0].field);
    try std.testing.expectEqualStrings("\"hot\"", request.aggregations[2].filter_array_any[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_array_contains.len);
    try std.testing.expectEqualStrings("tags", request.aggregations[2].filter_array_contains[0].field);
    try std.testing.expectEqualStrings("[\"hot\"]", request.aggregations[2].filter_array_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_array_eq.len);
    try std.testing.expectEqualStrings("tags", request.aggregations[2].filter_array_eq[0].field);
    try std.testing.expectEqualStrings("[\"hot\",\"new\"]", request.aggregations[2].filter_array_eq[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_in_predicates.len);
    try std.testing.expectEqualStrings("status", request.aggregations[2].filter_in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"open\",\"pending\"]", request.aggregations[2].filter_in_predicates[0].values_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_json_contains.len);
    try std.testing.expectEqualStrings("attrs", request.aggregations[2].filter_json_contains[0].field);
    try std.testing.expectEqualStrings("{\"source\":\"api\"}", request.aggregations[2].filter_json_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_json_path_eq.len);
    try std.testing.expectEqualStrings("attrs", request.aggregations[2].filter_json_path_eq[0].field);
    try std.testing.expectEqualStrings("source", request.aggregations[2].filter_json_path_eq[0].path);
    try std.testing.expectEqualStrings("\"api\"", request.aggregations[2].filter_json_path_eq[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_json_path_exists.len);
    try std.testing.expectEqualStrings("attrs", request.aggregations[2].filter_json_path_exists[0].field);
    try std.testing.expectEqualStrings("flags", request.aggregations[2].filter_json_path_exists[0].path);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_text_patterns.len);
    try std.testing.expectEqualStrings("status", request.aggregations[2].filter_text_patterns[0].field);
    try std.testing.expectEqualStrings("op%", request.aggregations[2].filter_text_patterns[0].pattern);
    try std.testing.expect(request.aggregations[2].filter_text_patterns[0].case_insensitive);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_expressions.len);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, request.aggregations[2].filter_expression_array_contains[0].expression.kind);
    try std.testing.expectEqualStrings("[\"write\"]", request.aggregations[2].filter_expression_array_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 2), request.aggregations[2].filter_any.len);
    try std.testing.expectEqualStrings("status", request.aggregations[2].filter_any[0].conditions[0].lhs.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_length, request.aggregations[2].filter_any[1].conditions[0].lhs.kind);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[2].filter_not.len);
    try std.testing.expectEqualStrings("status", request.aggregations[2].filter_not[0].conditions[0].lhs.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, request.aggregations[3].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, request.aggregations[3].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, request.aggregations[4].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_length, request.aggregations[4].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, request.aggregations[5].op);
    try std.testing.expectEqualStrings("attrs", request.aggregations[5].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, request.aggregations[6].op);
    try std.testing.expectEqualStrings("tags", request.aggregations[6].field.?);
    try std.testing.expectEqual(@as(u32, 4), request.aggregations[6].array_max_items);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, request.aggregations[7].op);
    try std.testing.expectEqual(@as(u32, 4), request.aggregations[7].array_max_items);
    try std.testing.expectEqual(@as(usize, 1), request.aggregations[7].array_order_by.len);
    try std.testing.expectEqual(@as(usize, 1), request.having_predicates.len);
    try std.testing.expectEqualStrings("net_amount", request.having_predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.having_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.having_expressions[0].lhs.kind);
    try std.testing.expectEqualStrings("net_amount", request.having_expressions[0].lhs.operands[0].field);
    try std.testing.expectEqualStrings("row_count", request.having_expressions[0].lhs.operands[1].field);
    try std.testing.expectEqual(@as(usize, 2), request.having_any.len);
    try std.testing.expectEqualStrings("status_key", request.having_any[0].conditions[0].lhs.field);
    try std.testing.expectEqualStrings("row_count", request.having_any[1].conditions[0].lhs.field);
    try std.testing.expectEqual(@as(usize, 1), request.having_not.len);
    try std.testing.expectEqualStrings("net_amount", request.having_not[0].conditions[0].lhs.field);
    try std.testing.expectEqual(@as(usize, 2), request.order_by.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.order_by[0].expression.?.kind);
    try std.testing.expectEqual(RowsQueryOrderDirection.desc, request.order_by[0].direction);
    try std.testing.expectEqualStrings("status_key", request.order_by[1].field);
    try std.testing.expectEqual(RowsQueryOrderNullTest.is_null, request.order_by[1].null_test.?);
    try std.testing.expectEqual(@as(u32, 5), request.limit.?);

    var group_only = try parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}},\"group_by\":[\"customer\"],\"aggregations\":[],\"order_by\":[{\"field\":\"customer\"}]}",
        schema,
    );
    defer group_only.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), group_only.group_by.len);
    try std.testing.expectEqualStrings("customer", group_only.group_by[0]);
    try std.testing.expectEqual(@as(usize, 0), group_only.aggregations.len);
    try std.testing.expectEqual(@as(usize, 1), group_only.order_by.len);
    try std.testing.expectEqualStrings("customer", group_only.order_by[0].field);

    var group_only_omitted = try parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"group_by\":[\"customer\"]}",
        schema,
    );
    defer group_only_omitted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), group_only_omitted.group_by.len);
    try std.testing.expectEqual(@as(usize, 0), group_only_omitted.aggregations.len);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"aggregations\":[]}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"aggregations\":[{\"name\":\"bad\",\"op\":\"sum\",\"field\":\"status\"}]}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}],\"having\":{\"field\":\"row_count\",\"op\":\"gt\",\"value\":0}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}],\"having\":{\"all\":[{\"field\":\"amount\",\"op\":\"gt\",\"value\":0}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\",\"unknown\":true}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\",\"expression\":{\"field\":\"amount\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}],\"order_by\":[{\"field\":\"row_count\",\"direction\":\"desc\",\"unknown\":true}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\",\"filter_array_contains\":[{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\",3]}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\",\"filter_in\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"open\",3]}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\",\"filter_expressions\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"value\":3}}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\",\"customer\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\"],\"group_expressions\":[{\"as\":\"customer\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}}],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\"],\"aggregations\":[{\"name\":\"customer\",\"op\":\"count\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"},{\"name\":\"row_count\",\"op\":\"count\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregateRequest(
        std.testing.allocator,
        "{\"source\":{},\"group_by\":[\"customer\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"}],\"order_by\":[{\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"amount\"},{\"field\":\"row_count\"}]}}]}",
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
        "{\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"partition_by\":[\"tenant\"],\"order_by\":[{\"expr\":{\"op\":\"sub\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]},\"direction\":\"desc\"}]}],\"select\":[\"tenant\",\"id\",\"amount\"],\"order_by\":[{\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"row_num\"},{\"value\":1}]},\"direction\":\"asc\"}],\"limit\":10}",
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
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.order_by[0].expression.?.kind);
    try std.testing.expectEqual(@as(u32, 10), request.limit.?);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"source\":{\"row_claim\":{\"owner_id\":\"worker\",\"lease_ms\":1000}},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    var rank_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"rank\",\"function\":\"rank\",\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"dense_rank\",\"function\":\"dense_rank\",\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"prev_amount\",\"function\":\"lag\",\"expr\":{\"field\":\"amount\"},\"offset\":2,\"default\":0,\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"next_status\",\"function\":\"lead\",\"expr\":{\"field\":\"status\"},\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"first_amount\",\"function\":\"first_value\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"last_amount\",\"function\":\"last_value\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"rows\",\"start\":\"unbounded_preceding\",\"end\":\"unbounded_following\"}},{\"as\":\"partition_count\",\"function\":\"count\",\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"rows\",\"start\":\"offset_preceding\",\"start_offset\":1,\"end\":\"offset_following\",\"end_offset\":1}},{\"as\":\"partition_sum\",\"function\":\"sum\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"current_avg\",\"function\":\"avg\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"rows\",\"start\":\"current_row\",\"end\":\"current_row\"}},{\"as\":\"partition_min\",\"function\":\"min\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"partition_max\",\"function\":\"max\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    );
    defer rank_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 11), rank_request.windows.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.rank, rank_request.windows[0].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.dense_rank, rank_request.windows[1].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.lag, rank_request.windows[2].function);
    try std.testing.expectEqual(@as(u32, 2), rank_request.windows[2].offset);
    try std.testing.expectEqualStrings("0", rank_request.windows[2].default_json);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, rank_request.windows[2].value_expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.lead, rank_request.windows[3].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.first_value, rank_request.windows[4].function);
    try std.testing.expectEqualStrings("amount", rank_request.windows[4].value_expression.?.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.last_value, rank_request.windows[5].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameUnit.rows, rank_request.windows[5].frame.?.unit);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.unbounded_preceding, rank_request.windows[5].frame.?.start);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.unbounded_following, rank_request.windows[5].frame.?.end);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.count, rank_request.windows[6].function);
    try std.testing.expect(rank_request.windows[6].value_expression == null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.offset_preceding, rank_request.windows[6].frame.?.start);
    try std.testing.expectEqual(@as(u32, 1), rank_request.windows[6].frame.?.start_offset);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.offset_following, rank_request.windows[6].frame.?.end);
    try std.testing.expectEqual(@as(u32, 1), rank_request.windows[6].frame.?.end_offset);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.sum, rank_request.windows[7].function);
    try std.testing.expectEqualStrings("amount", rank_request.windows[7].value_expression.?.field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.avg, rank_request.windows[8].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.current_row, rank_request.windows[8].frame.?.start);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.min, rank_request.windows[9].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.max, rank_request.windows[10].function);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad_sum\",\"function\":\"sum\",\"expr\":{\"field\":\"status\"},\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));

    var relative_rank_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"percent_rank\",\"function\":\"percent_rank\",\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"cume_dist\",\"function\":\"cume_dist\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    );
    defer relative_rank_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), relative_rank_request.windows.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.percent_rank, relative_rank_request.windows[0].function);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.cume_dist, relative_rank_request.windows[1].function);
    try std.testing.expect(relative_rank_request.windows[0].value_expression == null);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad_percent\",\"function\":\"percent_rank\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));

    var ntile_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"amount_bucket\",\"function\":\"ntile\",\"offset\":2,\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    );
    defer ntile_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.ntile, ntile_request.windows[0].function);
    try std.testing.expectEqual(@as(u32, 2), ntile_request.windows[0].offset);
    try std.testing.expect(ntile_request.windows[0].value_expression == null);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad_ntile\",\"function\":\"ntile\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad_ntile\",\"function\":\"ntile\",\"expr\":{\"field\":\"amount\"},\"offset\":2,\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));

    var nth_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"second_amount\",\"function\":\"nth_value\",\"expr\":{\"field\":\"amount\"},\"offset\":2,\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"rows\",\"start\":\"current_row\",\"end\":\"unbounded_following\"}}]}",
        schema,
    );
    defer nth_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.nth_value, nth_request.windows[0].function);
    try std.testing.expectEqualStrings("amount", nth_request.windows[0].value_expression.?.field);
    try std.testing.expectEqual(@as(u32, 2), nth_request.windows[0].offset);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.current_row, nth_request.windows[0].frame.?.start);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"missing_n\",\"function\":\"nth_value\",\"expr\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));

    var current_start_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"tail_count\",\"function\":\"count\",\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"rows\",\"start\":\"current_row\",\"end\":\"unbounded_following\"}}]}",
        schema,
    );
    defer current_start_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.current_row, current_start_request.windows[0].frame.?.start);

    var range_offset_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"range_count\",\"function\":\"count\",\"order_by\":[{\"field\":\"amount\"}],\"frame\":{\"unit\":\"range\",\"start\":\"offset_preceding\",\"start_offset\":1,\"end\":\"current_row\"}}]}",
        schema,
    );
    defer range_offset_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameUnit.range, range_offset_request.windows[0].frame.?.unit);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameBound.offset_preceding, range_offset_request.windows[0].frame.?.start);

    var range_expression_order_request = try parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"range_count\",\"function\":\"count\",\"order_by\":[{\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}],\"frame\":{\"unit\":\"range\",\"start\":\"offset_preceding\",\"start_offset\":1,\"end\":\"current_row\"}}]}",
        schema,
    );
    defer range_expression_order_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, range_expression_order_request.windows[0].order_by[0].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFrameUnit.range, range_expression_order_request.windows[0].frame.?.unit);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad\",\"function\":\"count\",\"order_by\":[{\"field\":\"status\"}],\"frame\":{\"unit\":\"range\",\"start\":\"offset_preceding\",\"start_offset\":1,\"end\":\"current_row\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad\",\"function\":\"count\",\"order_by\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}}],\"frame\":{\"unit\":\"range\",\"start\":\"offset_preceding\",\"start_offset\":1,\"end\":\"current_row\"}}]}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bucket\",\"function\":\"ntile\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad\",\"function\":\"lag\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"output\":\"bad\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"bad\",\"function\":\"lag\",\"expression\":{\"field\":\"amount\"},\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}],\"select\":[\"id\"],\"order_by\":[{\"field\":\"status\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}],\"select\":[\"id\",\"id\"]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"id\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]}],\"select\":[\"id\"]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowRequest(
        std.testing.allocator,
        "{\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"amount\"}]},{\"as\":\"row_num\",\"function\":\"rank\",\"order_by\":[{\"field\":\"amount\"}]}]}",
        schema,
    ));
}

test "relational rows join contract accepts typed equality join plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"rank":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
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

    var rich_request = try parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{\"expression_any\":[{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"lt\",\"rhs\":{\"value\":3}}]},{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}]},\"right\":{\"expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"write\"]}]},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"select\":[{\"as\":\"order_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"customer_name\",\"side\":\"right\",\"field\":\"name\"}],\"order_by\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"customer_name\"}]},\"direction\":\"asc\"}],\"limit\":5}",
        schema,
    );
    defer rich_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), rich_request.left.expression_or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), rich_request.right.expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, rich_request.right.expression_array_contains[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 1), rich_request.on.len);
    try std.testing.expectEqual(@as(usize, 1), rich_request.order_by.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, rich_request.order_by[0].expression.?.kind);
    try std.testing.expectEqual(@as(u32, 5), rich_request.limit.?);

    var residual_request = try parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"order\"}},\"right\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"customer\"}},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"match_expression_where\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\",\"source\":\"source\"}]}}],\"match_expression_any\":[{\"all\":[{\"lhs\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"field\":\"amount\",\"source\":\"source\"}]},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}],\"match_expression_not\":[{\"all\":[{\"lhs\":{\"field\":\"name\",\"source\":\"source\"},\"op\":\"eq\",\"rhs\":{\"value\":\"blocked\"}}]}],\"match_expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\",\"source\":\"source\"},{\"value\":\" \"}]},\"value\":[\"read\"]}],\"select\":[{\"as\":\"order_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"customer_name\",\"side\":\"right\",\"field\":\"name\"}]}",
        schema,
    );
    defer residual_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_predicates.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.row, residual_request.match_expression_predicates[0].lhs.operands[0].field_source);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, residual_request.match_expression_predicates[0].rhs[0].operands[0].field_source);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_not_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, residual_request.match_expression_array_contains[0].expression.operands[0].field_source);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{\"doc_key_range\":{\"start\":\"a\",\"end\":\"z\"}},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left\":\"customer_id\",\"right\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"select\":[{\"output\":\"order_id\",\"side\":\"left\",\"field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"select\":[{\"as\":\"order_id\",\"side\":\"left\",\"field\":\"id\"}],\"order_by\":[{\"field\":\"amount\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"select\":[{\"as\":\"id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"id\",\"side\":\"right\",\"field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"match_expression_where\":[{\"lhs\":{\"field\":\"amount\",\"source\":\"existing\"},\"op\":\"gt\",\"rhs\":{\"value\":1}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"customer_id\",\"right_field\":\"id\"}],\"match_expression_where\":[{\"lhs\":{\"field\":\"amount\",\"source\":\"proposed\"},\"op\":\"gt\",\"rhs\":{\"value\":1}}]}",
        schema,
    ));
}

test "relational rows lateral contract accepts bounded correlated plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"rank":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
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

    var rich_request = try parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{\"expression_not\":[{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gte\",\"rhs\":{\"value\":3}},{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"lte\",\"rhs\":{\"value\":10}}]}]},\"right\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"order\"},\"expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"read\"]}],\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":2},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}],\"select\":[{\"as\":\"customer_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_order_id\",\"side\":\"right\",\"field\":\"id\"}],\"order_by\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"latest_order_id\"}]},\"direction\":\"desc\"},{\"field\":\"customer_id\",\"null_test\":\"is_not_null\"}],\"limit\":4}",
        schema,
    );
    defer rich_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), rich_request.left.expression_not_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), rich_request.right.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), rich_request.right.expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, rich_request.right.expression_array_contains[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 1), rich_request.right.order_by.len);
    try std.testing.expectEqual(@as(usize, 2), rich_request.order_by.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, rich_request.order_by[0].expression.?.kind);
    try std.testing.expectEqual(RowsQueryOrderNullTest.is_not_null, rich_request.order_by[1].null_test.?);
    try std.testing.expectEqual(@as(u32, 2), rich_request.right.limit.?);
    try std.testing.expectEqual(@as(u32, 4), rich_request.limit.?);

    var residual_request = try parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"customer\"}},\"right\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"order\"},\"limit\":2},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}],\"match_expression_where\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\",\"source\":\"source\"}]}}],\"match_expression_any\":[{\"all\":[{\"lhs\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"field\":\"amount\",\"source\":\"source\"}]},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}],\"match_expression_not\":[{\"all\":[{\"lhs\":{\"field\":\"name\",\"source\":\"source\"},\"op\":\"eq\",\"rhs\":{\"value\":\"blocked\"}}]}],\"match_expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\",\"source\":\"source\"},{\"value\":\" \"}]},\"value\":[\"read\"]}],\"select\":[{\"as\":\"customer_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_order_id\",\"side\":\"right\",\"field\":\"id\"}]}",
        schema,
    );
    defer residual_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_predicates.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.row, residual_request.match_expression_predicates[0].lhs.operands[0].field_source);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, residual_request.match_expression_predicates[0].rhs[0].operands[0].field_source);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_not_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), residual_request.match_expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, residual_request.match_expression_array_contains[0].expression.operands[0].field_source);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{\"limit\":1},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}],\"select\":[{\"as\":\"id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"id\",\"side\":\"right\",\"field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{\"doc_key_range\":{\"start\":\"a\",\"end\":\"z\"}},\"right\":{\"limit\":1},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{\"limit\":1},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"customer_id\"}],\"select\":[{\"as\":\"customer_id\",\"side\":\"left\",\"field\":\"id\"}],\"order_by\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralRequest(
        std.testing.allocator,
        "{\"left\":{},\"right\":{\"limit\":1},\"correlations\":[{\"left\":\"id\",\"right\":\"customer_id\"}]}",
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
        "{\"ctes\":[{\"name\":\"open_rows\",\"max_rows\":100,\"max_bytes\":4096,\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\"]}},{\"name\":\"expensive_open_rows\",\"query\":{\"source_cte\":\"open_rows\",\"where\":{\"field\":\"amount\",\"op\":\"gt\",\"value\":10},\"select\":[\"id\",\"amount\"]}}],\"query\":{\"source_cte\":\"expensive_open_rows\",\"select\":[\"id\"],\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":2}}",
        schema,
    );
    defer query_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), query_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", query_plan.ctes[0].name);
    try std.testing.expectEqual(@as(u32, 100), query_plan.ctes[0].max_rows.?);
    try std.testing.expectEqual(@as(u64, 4096), query_plan.ctes[0].max_bytes.?);
    try std.testing.expectEqualStrings("", query_plan.ctes[0].query.source_cte);
    try std.testing.expectEqualStrings("expensive_open_rows", query_plan.ctes[1].name);
    try std.testing.expectEqual(@as(?u32, null), query_plan.ctes[1].max_rows);
    try std.testing.expectEqual(@as(?u64, null), query_plan.ctes[1].max_bytes);
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

    var join_plan = try parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\"]}},{\"name\":\"expensive_open_rows\",\"query\":{\"source_cte\":\"open_rows\",\"where\":{\"field\":\"amount\",\"op\":\"gt\",\"value\":10},\"select\":[\"id\",\"tenant\",\"amount\"]}}],\"join\":{\"left\":{\"source_cte\":\"open_rows\"},\"right\":{\"source_cte\":\"expensive_open_rows\"},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"match_expression_where\":[{\"lhs\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"field\":\"amount\",\"source\":\"source\"}]},\"op\":\"gt\",\"rhs\":{\"value\":20}}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"right_id\",\"side\":\"right\",\"field\":\"id\"}],\"limit\":3}}",
        schema,
    );
    defer join_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), join_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", join_plan.join.left.source_cte);
    try std.testing.expectEqualStrings("expensive_open_rows", join_plan.join.right.source_cte);
    try std.testing.expectEqual(@as(usize, 1), join_plan.join.on.len);
    try std.testing.expectEqual(@as(usize, 1), join_plan.join.match_expression_predicates.len);
    try std.testing.expectEqual(@as(u32, 3), join_plan.join.limit.?);

    var lateral_plan = try parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\",\"created_at\"]}}],\"lateral\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}},\"right\":{\"source_cte\":\"open_rows\",\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"match_expression_where\":[{\"lhs\":{\"field\":\"amount\",\"source\":\"source\"},\"op\":\"gt\",\"rhs\":{\"field\":\"amount\"}}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_id\",\"side\":\"right\",\"field\":\"id\"}],\"limit\":2}}",
        schema,
    );
    defer lateral_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), lateral_plan.ctes.len);
    try std.testing.expectEqualStrings("open_rows", lateral_plan.lateral.right.source_cte);
    try std.testing.expectEqual(@as(u32, 1), lateral_plan.lateral.right.limit.?);
    try std.testing.expectEqual(@as(usize, 1), lateral_plan.lateral.correlations.len);
    try std.testing.expectEqual(@as(usize, 1), lateral_plan.lateral.match_expression_predicates.len);
    try std.testing.expectEqual(@as(u32, 2), lateral_plan.lateral.limit.?);

    var ranged_query_plan = try parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:m\"},{\"start\":\"row:m\",\"end\":\"row:z\"}],\"query\":{\"select\":[\"id\"],\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":5}}",
        schema,
    );
    defer ranged_query_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), ranged_query_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 2), ranged_query_plan.ranges.len);
    try std.testing.expectEqualStrings("row:a", ranged_query_plan.ranges[0].start);
    try std.testing.expectEqualStrings("row:z", ranged_query_plan.ranges[1].end);
    try std.testing.expectEqual(@as(u32, 5), ranged_query_plan.query.limit.?);

    var ranged_cte_query_plan = try parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\"]}}],\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"query\":{\"source_cte\":\"open_rows\",\"select\":[\"id\"],\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":2}}",
        schema,
    );
    defer ranged_cte_query_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_query_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_query_plan.ranges.len);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_query_plan.query.source_cte);

    var ranged_aggregate_plan = try parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"aggregate\":{\"group_by\":[\"tenant\"],\"aggregations\":[{\"name\":\"amount_sum\",\"op\":\"sum\",\"field\":\"amount\"}]}}",
        schema,
    );
    defer ranged_aggregate_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_aggregate_plan.ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_aggregate_plan.aggregate.group_by.len);

    var ranged_cte_aggregate_plan = try parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"aggregate\":{\"source\":{\"source_cte\":\"open_rows\"},\"group_by\":[\"tenant\"],\"aggregations\":[{\"name\":\"amount_sum\",\"op\":\"sum\",\"field\":\"amount\"}]}}",
        schema,
    );
    defer ranged_cte_aggregate_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_aggregate_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_aggregate_plan.ranges.len);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_aggregate_plan.aggregate.source.source_cte);

    var ranged_window_plan = try parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"window\":{\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"partition_by\":[\"tenant\"],\"order_by\":[{\"field\":\"created_at\"}]}],\"select\":[\"tenant\",\"id\"]}}",
        schema,
    );
    defer ranged_window_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_window_plan.ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_window_plan.window.windows.len);

    var ranged_cte_window_plan = try parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"window\":{\"source\":{\"source_cte\":\"open_rows\"},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"partition_by\":[\"tenant\"],\"order_by\":[{\"field\":\"created_at\"}]}],\"select\":[\"tenant\",\"id\"]}}",
        schema,
    );
    defer ranged_cte_window_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_window_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_window_plan.ranges.len);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_window_plan.window.source.source_cte);

    var ranged_join_plan = try parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:m\"}],\"right_ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"}],\"join\":{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"right_id\",\"side\":\"right\",\"field\":\"id\"}]}}",
        schema,
    );
    defer ranged_join_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_join_plan.left_ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_join_plan.right_ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_join_plan.join.on.len);

    var ranged_cte_join_plan = try parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\"]}},{\"name\":\"expensive_open_rows\",\"query\":{\"source_cte\":\"open_rows\",\"where\":{\"field\":\"amount\",\"op\":\"gt\",\"value\":10},\"select\":[\"id\",\"tenant\",\"amount\"]}}],\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:m\"}],\"right_ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"}],\"join\":{\"left\":{\"source_cte\":\"open_rows\"},\"right\":{\"source_cte\":\"expensive_open_rows\"},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"right_id\",\"side\":\"right\",\"field\":\"id\"}]}}",
        schema,
    );
    defer ranged_cte_join_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), ranged_cte_join_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_join_plan.left_ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_join_plan.right_ranges.len);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_join_plan.join.left.source_cte);
    try std.testing.expectEqualStrings("expensive_open_rows", ranged_cte_join_plan.join.right.source_cte);

    var ranged_lateral_plan = try parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:m\"}],\"right_ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"}],\"lateral\":{\"left\":{},\"right\":{\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_id\",\"side\":\"right\",\"field\":\"id\"}]}}",
        schema,
    );
    defer ranged_lateral_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_lateral_plan.left_ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_lateral_plan.right_ranges.len);
    try std.testing.expectEqual(@as(u32, 1), ranged_lateral_plan.lateral.right.limit.?);

    var ranged_cte_lateral_plan = try parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"},\"select\":[\"id\",\"tenant\",\"amount\",\"created_at\"]}}],\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:m\"}],\"right_ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"}],\"lateral\":{\"left\":{\"source_cte\":\"open_rows\"},\"right\":{\"source_cte\":\"open_rows\",\"order_by\":[{\"field\":\"created_at\",\"direction\":\"desc\"}],\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"},{\"as\":\"latest_id\",\"side\":\"right\",\"field\":\"id\"}]}}",
        schema,
    );
    defer ranged_cte_lateral_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_lateral_plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_lateral_plan.left_ranges.len);
    try std.testing.expectEqual(@as(usize, 1), ranged_cte_lateral_plan.right_ranges.len);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_lateral_plan.lateral.left.source_cte);
    try std.testing.expectEqualStrings("open_rows", ranged_cte_lateral_plan.lateral.right.source_cte);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"early\",\"query\":{\"source_cte\":\"later\"}},{\"name\":\"later\",\"query\":{\"select\":[\"id\"]}}],\"query\":{\"source_cte\":\"early\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"query\":{\"select\":[\"id\"]},\"aggregate\":{\"source\":{},\"aggregations\":[{\"name\":\"count\",\"op\":\"count\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"query\":{\"select\":[\"id\"]},\"unexpected\":true}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"max_rows\":-1,\"query\":{\"select\":[\"id\"]}}],\"query\":{\"source_cte\":\"open_rows\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"max_bytes\":-1,\"query\":{\"select\":[\"id\"]}}],\"query\":{\"source_cte\":\"open_rows\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"query\":{\"select\":[\"id\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"query\":{\"row_claim\":{\"transaction_id\":\"00000000000000000000000000000000\"}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"query\":{\"doc_key_range\":{\"start\":\"row:a\",\"end\":\"row:z\"}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\",\"unknown\":true}],\"query\":{\"select\":[\"id\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"},{\"start\":\"row:a\",\"end\":\"row:m\"}],\"query\":{\"select\":[\"id\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:n\"},{\"start\":\"row:m\",\"end\":\"row:z\"}],\"aggregate\":{\"aggregations\":[{\"name\":\"count\",\"op\":\"count\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\"},{\"start\":\"row:m\",\"end\":\"row:z\"}],\"window\":{\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"created_at\"}]}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"aggregate\":{\"source\":{\"source_cte\":\"open_rows\"},\"aggregations\":[{\"name\":\"count\",\"op\":\"count\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"join\":{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"right_ranges\":[{\"start\":\"row:z\",\"end\":\"row:a\"}],\"lateral\":{\"left\":{},\"right\":{\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:n\"},{\"start\":\"row:m\",\"end\":\"row:z\"}],\"right_ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"join\":{\"left\":{},\"right\":{},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"left_ranges\":[{\"start\":\"row:a\",\"end\":\"row:z\"}],\"right_ranges\":[{\"start\":\"row:m\",\"end\":\"row:z\"},{\"start\":\"row:a\",\"end\":\"row:m\"}],\"lateral\":{\"left\":{},\"right\":{\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"row_claim\":{\"transaction_id\":\"00000000000000000000000000000000\"}}}],\"window\":{\"source\":{\"source_cte\":\"open_rows\"},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"created_at\"}]}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"join\":{\"left\":{\"source_cte\":\"missing_rows\"},\"right\":{},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"open_rows\",\"query\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"open\"}}}],\"lateral\":{\"left\":{},\"right\":{\"source_cte\":\"missing_rows\",\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"query\":{\"source_cte\":\"ids_only\",\"where\":{\"field\":\"amount\",\"op\":\"gt\",\"value\":10}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsAggregatePlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"aggregate\":{\"source\":{\"source_cte\":\"ids_only\"},\"group_by\":[\"tenant\"],\"aggregations\":[{\"name\":\"count\",\"op\":\"count\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsWindowPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"window\":{\"source\":{\"source_cte\":\"ids_only\"},\"windows\":[{\"as\":\"row_num\",\"function\":\"row_number\",\"order_by\":[{\"field\":\"created_at\"}]}],\"select\":[\"id\"]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"join\":{\"left\":{\"source_cte\":\"ids_only\"},\"right\":{},\"on\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"left_id\",\"side\":\"left\",\"field\":\"id\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"lateral\":{\"left\":{},\"right\":{\"source_cte\":\"ids_only\",\"limit\":1},\"correlations\":[{\"left_field\":\"tenant\",\"right_field\":\"tenant\"}],\"select\":[{\"as\":\"latest_amount\",\"side\":\"right\",\"field\":\"amount\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"join\":{\"left\":{\"source_cte\":\"ids_only\"},\"right\":{},\"on\":[{\"left_field\":\"id\",\"right_field\":\"id\"}],\"match_expression_where\":[{\"lhs\":{\"field\":\"amount\"},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsLateralPlanRequest(
        std.testing.allocator,
        "{\"ctes\":[{\"name\":\"ids_only\",\"query\":{\"select\":[\"id\"]}}],\"lateral\":{\"left\":{},\"right\":{\"source_cte\":\"ids_only\",\"limit\":1},\"correlations\":[{\"left_field\":\"id\",\"right_field\":\"id\"}],\"match_expression_where\":[{\"lhs\":{\"field\":\"amount\",\"source\":\"source\"},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}}",
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

    var access_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"any\":[{\"field\":\"status\",\"op\":\"in\",\"value\":[\"closed\"]}]}}",
        schema,
    );
    defer access_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), access_request.or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), access_request.access_or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), access_request.access_or_predicates[0].in_predicates.len);
    try std.testing.expectEqualStrings("status", access_request.access_or_predicates[0].in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"closed\"]", access_request.access_or_predicates[0].in_predicates[0].values_json);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"all\":[{\"field\":\"amount\",\"op\":\"gt\",\"value\":20}]}}",
        schema,
    ));
}

test "relational rows query contract rejects shorthand equality and validates typed fields" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"rank":{"type":"numeric"},"expires_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"order_by\":[{\"field\":\"rank\"}],\"limit\":10}",
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

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"status\":\"ready\"}}",
        schema,
    ));

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

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"in\",\"value\":[\"ready\",3]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"rank\",\"op\":\"in\",\"value\":[1,\"bad\"]}}",
        schema,
    ));

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

    var expression_where_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"value\":\"beta\"}}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer expression_where_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), expression_where_request.expression_predicates.len);
    var expression_where_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, expression_where_request, expression_order_rows[0..]);
    defer expression_where_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), expression_where_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", expression_where_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", expression_where_result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]},\"op\":\"eq\",\"rhs\":{\"value\":3}}]}",
        schema,
    ));

    const arithmetic_rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"ready\",\"rank\":1}",
        "{\"id\":\"b\",\"status\":\"ready\",\"rank\":3}",
        "{\"id\":\"c\",\"status\":\"ready\",\"rank\":6}",
    };
    var arithmetic_expression_where_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gt\",\"rhs\":{\"value\":5}}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer arithmetic_expression_where_request.deinit(std.testing.allocator);
    var arithmetic_expression_where_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, arithmetic_expression_where_request, arithmetic_rows[0..]);
    defer arithmetic_expression_where_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), arithmetic_expression_where_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", arithmetic_expression_where_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", arithmetic_expression_where_result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gt\",\"rhs\":{\"value\":\"bad\"}}]}",
        schema,
    ));

    var expression_any_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_any\":[{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"lt\",\"rhs\":{\"value\":3}}]},{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gt\",\"rhs\":{\"value\":10}}]}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer expression_any_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), expression_any_request.expression_or_predicates.len);
    var expression_any_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, expression_any_request, arithmetic_rows[0..]);
    defer expression_any_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), expression_any_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", expression_any_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", expression_any_result.rows[1]);

    var expression_not_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_not\":[{\"all\":[{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"gte\",\"rhs\":{\"value\":3}},{\"lhs\":{\"op\":\"mul\",\"args\":[{\"field\":\"rank\"},{\"value\":2}]},\"op\":\"lte\",\"rhs\":{\"value\":10}}]}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer expression_not_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), expression_not_request.expression_not_predicates.len);
    var expression_not_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, expression_not_request, arithmetic_rows[0..]);
    defer expression_not_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), expression_not_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", expression_not_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", expression_not_result.rows[1]);

    const coalesce_rows = [_][]const u8{
        "{\"id\":\"a\",\"rank\":1}",
        "{\"id\":\"b\",\"status\":\"ready\",\"rank\":2}",
        "{\"id\":\"c\",\"status\":null,\"rank\":3}",
    };
    var coalesce_expression_where_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"status\"},{\"value\":\"missing\"}]},\"op\":\"eq\",\"rhs\":{\"value\":\"missing\"}}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer coalesce_expression_where_request.deinit(std.testing.allocator);
    var coalesce_expression_where_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, coalesce_expression_where_request, coalesce_rows[0..]);
    defer coalesce_expression_where_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), coalesce_expression_where_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", coalesce_expression_where_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", coalesce_expression_where_result.rows[1]);

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
        "{\"source_cte\":\"open_orders\",\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"limit\":10}",
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

    var expression_where_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"array_length\",\"args\":[{\"field\":\"tags\"}]},\"op\":\"gt\",\"rhs\":{\"value\":0}}],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer expression_where_request.deinit(std.testing.allocator);
    var expression_where_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, expression_where_request, rows[0..]);
    defer expression_where_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), expression_where_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", expression_where_result.rows[0]);

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
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"coalesce\":[{\"as\":\"display\",\"operands\":[{\"field\":\"display_name\"},{\"field\":\"email\"}]}],\"field_aliases\":[{\"as\":\"display\",\"field\":\"id\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}]}}]}",
        schema,
    ));

    var select_all_with_extra = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"*\"],\"expressions\":[{\"as\":\"email_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}]}}]}",
        schema,
    );
    defer select_all_with_extra.deinit(std.testing.allocator);
    try std.testing.expect(select_all_with_extra.select_all);
    var select_all_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, select_all_with_extra, rows[0..1]);
    defer select_all_result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"display_name\":\"Ada\",\"email\":\"ada@example.test\",\"email_key\":\"ada@example.test\"}", select_all_result.rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"*\"],\"expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}]}}]}",
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
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"name_or_email\",\"expr\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"display_name\"},{\"field\":\"email\"},{\"value\":\"unknown\"}]}},{\"as\":\"email_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}]}},{\"as\":\"label\",\"expr\":{\"op\":\"concat\",\"args\":[{\"field\":\"id\"},{\"value\":\"::\"},{\"field\":\"email\"}]}},{\"as\":\"email_without_b\",\"expr\":{\"op\":\"nullif\",\"args\":[{\"field\":\"email\"},{\"value\":\"b@example.test\"}]}},{\"as\":\"max_score\",\"expr\":{\"op\":\"greatest\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"},{\"field\":\"penalty\"}]}},{\"as\":\"min_score\",\"expr\":{\"op\":\"least\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"},{\"field\":\"penalty\"}]}},{\"as\":\"total_score\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"}]}},{\"as\":\"net_score\",\"expr\":{\"op\":\"sub\",\"args\":[{\"op\":\"add\",\"args\":[{\"field\":\"score\"},{\"field\":\"bonus\"}]},{\"field\":\"penalty\"}]}},{\"as\":\"scaled_score\",\"expr\":{\"op\":\"mul\",\"args\":[{\"field\":\"score\"},{\"field\":\"multiplier\"}]}},{\"as\":\"score_ratio\",\"expr\":{\"op\":\"div\",\"args\":[{\"field\":\"score\"},{\"field\":\"divisor\"}]}},{\"as\":\"email_bucket\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\"},\"then\":{\"value\":\"missing\"}},{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"eq\",\"rhs\":{\"value\":\"b@example.test\"}},\"then\":{\"value\":\"blocked\"}}],\"else\":{\"value\":\"ok\"}}},{\"as\":\"plan\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"attrs\"}],\"path\":[\"billing\",\"plan\"],\"as_text\":true}},{\"as\":\"flags\",\"expr\":{\"op\":\"json_extract\",\"args\":[{\"field\":\"attrs\"}],\"path\":\"flags\"}},{\"as\":\"score_gap_abs\",\"expr\":{\"op\":\"abs\",\"args\":[{\"op\":\"sub\",\"args\":[{\"field\":\"penalty\"},{\"field\":\"score\"}]}]}},{\"as\":\"rounded_literal\",\"expr\":{\"op\":\"round\",\"args\":[{\"value\":2.4}]}},{\"as\":\"floored_literal\",\"expr\":{\"op\":\"floor\",\"args\":[{\"value\":2.4}]}},{\"as\":\"ceiled_literal\",\"expr\":{\"op\":\"ceil\",\"args\":[{\"value\":2.4}]}},{\"as\":\"email_length\",\"expr\":{\"op\":\"length\",\"args\":[{\"field\":\"email\"}]}},{\"as\":\"trimmed_literal\",\"expr\":{\"op\":\"trim\",\"args\":[{\"value\":\"  padded  \"}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 20), request.expressions.len);
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
    try std.testing.expectEqualStrings("max_score", request.expressions[4].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.greatest, request.expressions[4].expression.kind);
    try std.testing.expectEqual(@as(usize, 3), request.expressions[4].expression.operands.len);
    try std.testing.expectEqualStrings("min_score", request.expressions[5].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.least, request.expressions[5].expression.kind);
    try std.testing.expectEqual(@as(usize, 3), request.expressions[5].expression.operands.len);
    try std.testing.expectEqualStrings("total_score", request.expressions[6].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.expressions[6].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.expressions[6].expression.operands.len);
    try std.testing.expectEqualStrings("net_score", request.expressions[7].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.expressions[7].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.expressions[7].expression.operands[0].kind);
    try std.testing.expectEqualStrings("scaled_score", request.expressions[8].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.mul, request.expressions[8].expression.kind);
    try std.testing.expectEqualStrings("score_ratio", request.expressions[9].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.div, request.expressions[9].expression.kind);
    try std.testing.expectEqualStrings("email_bucket", request.expressions[10].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, request.expressions[10].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), request.expressions[10].expression.case_branches.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, request.expressions[10].expression.case_branches[0].when.op);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, request.expressions[10].expression.case_branches[1].when.op);
    try std.testing.expectEqual(@as(usize, 1), request.expressions[10].expression.case_else.len);
    try std.testing.expectEqualStrings("plan", request.expressions[11].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, request.expressions[11].expression.kind);
    try std.testing.expectEqualStrings("billing.plan", request.expressions[11].expression.json_path);
    try std.testing.expect(request.expressions[11].expression.json_as_text);
    try std.testing.expectEqualStrings("flags", request.expressions[12].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, request.expressions[12].expression.kind);
    try std.testing.expect(!request.expressions[12].expression.json_as_text);
    try std.testing.expectEqualStrings("score_gap_abs", request.expressions[13].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.abs, request.expressions[13].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, request.expressions[13].expression.operands[0].kind);
    try std.testing.expectEqualStrings("rounded_literal", request.expressions[14].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.round, request.expressions[14].expression.kind);
    try std.testing.expectEqualStrings("floored_literal", request.expressions[15].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.floor, request.expressions[15].expression.kind);
    try std.testing.expectEqualStrings("ceiled_literal", request.expressions[16].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.ceil, request.expressions[16].expression.kind);
    try std.testing.expectEqualStrings("email_length", request.expressions[17].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.length, request.expressions[17].expression.kind);
    try std.testing.expectEqualStrings("trimmed_literal", request.expressions[18].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.trim, request.expressions[18].expression.kind);
    try std.testing.expectEqualStrings("replaced_literal", request.expressions[19].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.replace, request.expressions[19].expression.kind);
    try std.testing.expect(!request.select_all);

    var null_first_case = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"maybe_email\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\"},\"then\":{\"value\":null}}],\"else\":{\"field\":\"email\"}}}]}",
        schema,
    );
    defer null_first_case.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, null_first_case.expressions[0].expression.kind);

    var null_first_coalesce = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"score_or_zero\",\"expr\":{\"op\":\"coalesce\",\"args\":[{\"value\":null},{\"field\":\"score\"},{\"value\":0}]}}]}",
        schema,
    );
    defer null_first_coalesce.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, null_first_coalesce.expressions[0].expression.kind);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"bad_case\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"eq\",\"rhs\":{\"value\":3}},\"then\":{\"value\":\"bad\"}}],\"else\":{\"value\":\"ok\"}}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"bad_case\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\"},\"then\":{\"value\":\"missing\"}}],\"else\":{\"field\":\"score\"}}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"bad_coalesce\",\"expr\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"email\"},{\"field\":\"score\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"bad_nullif\",\"expr\":{\"op\":\"nullif\",\"args\":[{\"field\":\"email\"},{\"value\":3}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"bad_greatest\",\"expr\":{\"op\":\"greatest\",\"args\":[{\"field\":\"score\"},{\"value\":\"bad\"}]}}]}",
        schema,
    ));

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"display_name\":\"Ada\",\"email\":\"ada@example.test\",\"score\":10,\"bonus\":5,\"penalty\":2,\"multiplier\":3,\"divisor\":5,\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}}",
        "{\"id\":\"b\",\"display_name\":null,\"email\":\"b@example.test\",\"score\":3,\"bonus\":4,\"penalty\":1,\"multiplier\":2,\"divisor\":2,\"attrs\":{\"billing\":{\"plan\":\"free\"},\"flags\":[\"blocked\"]}}",
        "{\"id\":\"c\"}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"name_or_email\":\"Ada\",\"email_key\":\"ada@example.test\",\"label\":\"a::ada@example.test\",\"email_without_b\":\"ada@example.test\",\"max_score\":10,\"min_score\":2,\"total_score\":15,\"net_score\":13,\"scaled_score\":30,\"score_ratio\":2,\"email_bucket\":\"ok\",\"plan\":\"pro\",\"flags\":[\"active\"],\"score_gap_abs\":8,\"rounded_literal\":2,\"floored_literal\":2,\"ceiled_literal\":3,\"email_length\":16,\"trimmed_literal\":\"padded\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"name_or_email\":\"b@example.test\",\"email_key\":\"b@example.test\",\"label\":\"b::b@example.test\",\"email_without_b\":null,\"max_score\":4,\"min_score\":1,\"total_score\":7,\"net_score\":6,\"scaled_score\":6,\"score_ratio\":1.5,\"email_bucket\":\"blocked\",\"plan\":\"free\",\"flags\":[\"blocked\"],\"score_gap_abs\":2,\"rounded_literal\":2,\"floored_literal\":2,\"ceiled_literal\":3,\"email_length\":14,\"trimmed_literal\":\"padded\"}", result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"name_or_email\":\"unknown\",\"email_key\":null,\"label\":\"c::\",\"email_without_b\":null,\"max_score\":null,\"min_score\":null,\"total_score\":null,\"net_score\":null,\"scaled_score\":null,\"score_ratio\":null,\"email_bucket\":\"missing\",\"plan\":null,\"flags\":null,\"score_gap_abs\":null,\"rounded_literal\":2,\"floored_literal\":2,\"ceiled_literal\":3,\"email_length\":null,\"trimmed_literal\":\"padded\"}", result.rows[2]);

    var upper_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"email_upper\",\"expr\":{\"op\":\"upper\",\"args\":[{\"field\":\"email\"}]}}]}",
        schema,
    );
    defer upper_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), upper_request.expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.upper, upper_request.expressions[0].expression.kind);
    var upper_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, upper_request, rows[0..1]);
    defer upper_result.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"email_upper\":\"ADA@EXAMPLE.TEST\"}", upper_result.rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"field\":\"missing\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"field\":\"email\",\"to\":\"text\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"field\":\"email\",\"unknown\":true}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"email\"}],\"path\":\"extra\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"extra\":true,\"expr\":{\"field\":\"email\"}}]}",
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
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"case\",\"cases\":[{\"when\":{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\"},\"then\":{\"value\":\"missing\"},\"extra\":true}],\"else\":{\"value\":\"ok\"}}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expression_where\":[{\"lhs\":{\"field\":\"email\"},\"op\":\"is_null\",\"extra\":true}]}",
        schema,
    ));
}

test "relational rows query contract binds now expression once per typed plan" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"planned_at_ns\",\"expr\":{\"op\":\"now\"}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.now, request.expressions[0].expression.kind);
    const bound_now = try std.fmt.parseInt(u64, request.expressions[0].expression.value_json, 10);
    try std.testing.expect(bound_now > 0);

    const rows = [_][]const u8{
        "{\"id\":\"a\"}",
        "{\"id\":\"b\"}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    const expected_a = try std.fmt.allocPrint(std.testing.allocator, "{{\"id\":\"a\",\"planned_at_ns\":{d}}}", .{bound_now});
    defer std.testing.allocator.free(expected_a);
    const expected_b = try std.fmt.allocPrint(std.testing.allocator, "{{\"id\":\"b\",\"planned_at_ns\":{d}}}", .{bound_now});
    defer std.testing.allocator.free(expected_b);
    try std.testing.expectEqualStrings(expected_a, result.rows[0]);
    try std.testing.expectEqualStrings(expected_b, result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"now\",\"args\":[{\"value\":1}]}}]}",
        schema,
    ));
}

test "relational rows query contract projects interval arithmetic expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"datetime"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expressions\":[{\"as\":\"expires_at_ns\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"created_at_ns\"},{\"op\":\"interval_ns\",\"args\":[{\"value\":3600000000000}]}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.expressions.len);
    try std.testing.expectEqualStrings("expires_at_ns", request.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.interval_ns, request.expressions[0].expression.operands[1].kind);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"created_at_ns\":1000,\"amount\":5}",
        "{\"id\":\"b\",\"created_at_ns\":null,\"amount\":9}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"expires_at_ns\":3600000001000}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\",\"expires_at_ns\":null}", result.rows[1]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"mul\",\"args\":[{\"field\":\"amount\"},{\"op\":\"interval_ns\",\"args\":[{\"value\":1000}]}]}}]}",
        schema,
    ));
}

test "relational rows query contract projects string_to_array expressions" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"write\"]}],\"expressions\":[{\"as\":\"scope_parts\",\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), request.expression_array_contains.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, request.expression_array_contains[0].expression.kind);
    try std.testing.expectEqualStrings("[\"write\"]", request.expression_array_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), request.expressions.len);
    try std.testing.expectEqualStrings("scope_parts", request.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, request.expressions[0].expression.kind);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"write\"],\"extra\":true}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expression_array_contains\":[{\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"value\":[\"write\",3]}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expression_array_contains\":[{\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"scope\"}]},\"value\":[\"write\"]}]}",
        schema,
    ));
    try std.testing.expectEqual(@as(usize, 2), request.expressions[0].expression.operands.len);

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"scope\":\"read write admin\"}",
        "{\"id\":\"b\"}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"scope_parts\":[\"read\",\"write\",\"admin\"]}", result.rows[0]);

    var equality_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"select\":[\"id\"],\"expression_where\":[{\"lhs\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\" \"}]},\"op\":\"eq\",\"rhs\":{\"value\":[\"read\",\"write\",\"admin\"]}}]}",
        schema,
    );
    defer equality_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), equality_request.expression_predicates.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.string_to_array, equality_request.expression_predicates[0].lhs.kind);
    var equality_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, equality_request, rows[0..]);
    defer equality_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), equality_result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", equality_result.rows[0]);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"amount\"},{\"value\":\" \"}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":3}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"expressions\":[{\"as\":\"bad\",\"expr\":{\"op\":\"string_to_array\",\"args\":[{\"field\":\"scope\"},{\"value\":\"\"}]}}]}",
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
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"skip_locked\":true,\"lease_ms\":45000,\"owner_id\":\"session:7\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"},\"limit\":10}",
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
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"row_claim\":{\"transaction_id\":\"00112233445566778899aabbccddeeff\",\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"row_claim\":{\"txn_id\":\"00112233445566778899aabbccddeeff\"}}",
        schema,
    ));
}

test "relational rows mutation source contract parses claimed update plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"expires_at_ns":{"type":"datetime"},"attrs":{"type":"json"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"order_by\":[{\"field\":\"amount\",\"direction\":\"asc\"}],\"limit\":2,\"row_claim\":{\"mode\":\"for_update\",\"skip_locked\":true,\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"id\",\"status\"],\"returning_expressions\":[{\"as\":\"amount_plus_one\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
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

    var expression_request = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch_expr\":{\"status\":{\"op\":\"concat\",\"args\":[{\"field\":\"status\",\"source\":\"existing\"},{\"value\":\"-claimed\"}]},\"expires_at_ns\":{\"op\":\"add\",\"args\":[{\"field\":\"expires_at_ns\",\"source\":\"existing\"},{\"op\":\"interval_ns\",\"args\":[{\"value\":60000000000}]}]}},\"increment_expr\":{\"amount\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"amount\",\"source\":\"existing\"},{\"value\":1}]}},\"returning\":[\"status\",\"amount\"]}",
        schema,
    );
    defer expression_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), expression_request.req.operations.len);
    try std.testing.expectEqual(@as(usize, 2), expression_request.req.patch_expressions.len);
    try std.testing.expectEqualStrings("status", expression_request.req.patch_expressions[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.concat, expression_request.req.patch_expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.existing, expression_request.req.patch_expressions[0].expression.operands[0].field_source);
    try std.testing.expectEqualStrings("expires_at_ns", expression_request.req.patch_expressions[1].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, expression_request.req.patch_expressions[1].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.interval_ns, expression_request.req.patch_expressions[1].expression.operands[1].kind);
    try std.testing.expectEqual(@as(usize, 1), expression_request.req.increment_expressions.len);
    try std.testing.expectEqualStrings("amount", expression_request.req.increment_expressions[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, expression_request.req.increment_expressions[0].expression.kind);

    var all_returning_request = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"*\"],\"returning_expressions\":[{\"as\":\"amount_plus_one\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
        schema,
    );
    defer all_returning_request.deinit(std.testing.allocator);
    try std.testing.expect(all_returning_request.req.returning_all);
    try std.testing.expectEqual(@as(usize, 0), all_returning_request.req.returning.len);
    try std.testing.expectEqual(@as(usize, 1), all_returning_request.req.returning_expressions.len);
    try std.testing.expectEqualStrings("amount_plus_one", all_returning_request.req.returning_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, all_returning_request.req.returning_expressions[0].expression.kind);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"id\",\"id\"]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"id\"],\"returning_expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"returning\":[\"*\"],\"returning_expressions\":[{\"as\":\"id\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch_expr\":{\"status\":{\"field\":\"status\",\"source\":\"proposed\"}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"amount\":1},\"increment\":{\"amount\":1}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch_expr\":{\"amount\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\",\"source\":\"existing\"},{\"value\":1}]}},\"increment_expr\":{\"amount\":{\"value\":1}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"json_set\":[{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"value\":\"basic\"},{\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"expr\":{\"field\":\"status\",\"source\":\"existing\"}}]}",
        schema,
    ));

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"delete\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}}}",
        schema,
    ));

    var table_emptying_request = try parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"delete\",\"source\":{\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:truncate\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}}}",
        schema,
    );
    defer table_emptying_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, table_emptying_request.req.kind);
    try std.testing.expectEqual(@as(usize, 0), table_emptying_request.req.source.predicates.len);
    try std.testing.expectEqual(@as(usize, 0), table_emptying_request.req.source.order_by.len);
    try std.testing.expectEqual(@as(?u32, null), table_emptying_request.req.source.limit);
    try std.testing.expect(table_emptying_request.req.source.row_claim != null);
    try std.testing.expectEqualStrings("session:truncate", table_emptying_request.req.source.row_claim.?.owner_id);
    try std.testing.expectEqual(@as(usize, 0), table_emptying_request.req.returning.len);
    try std.testing.expectEqual(@as(usize, 0), table_emptying_request.req.returning_expressions.len);
    try std.testing.expect(!table_emptying_request.req.returning_all);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source\":{\"doc_key_range\":{\"start\":\"row:a\",\"end\":\"row:z\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:mutation\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"}}",
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

test "relational rows insert source contract parses typed source assignments" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"rows_source_id_key","columns":["source_id"]}]}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);
    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"archive_id":{"type":"keyword"},"archive_status":{"type":"keyword"},"archive_amount":{"type":"numeric"}},"required":["archive_id"],"additionalProperties":false}}},"primary_key":{"columns":["archive_id"]}}
    ;
    var parsed_source = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, source_schema_json);
    defer parsed_source.deinit(std.testing.allocator);
    const source_schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed_source);
    defer runtime_schema.freeSchema(std.testing.allocator, source_schema);

    var request = try parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":2},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"status\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"status\"}]}},{\"target_field\":\"amount\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}],\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"nothing\"},\"returning\":[\"id\",\"status\"],\"returning_expressions\":[{\"as\":\"amount_plus_one\",\"expr\":{\"op\":\"add\",\"args\":[{\"field\":\"amount\"},{\"value\":1}]}}]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.req.source.predicates.len);
    try std.testing.expectEqualStrings("status", request.req.source.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), request.req.source.order_by.len);
    try std.testing.expectEqual(@as(u32, 2), request.req.source.limit.?);
    try std.testing.expect(request.req.source.row_claim == null);
    try std.testing.expectEqual(@as(usize, 3), request.req.assignments.len);
    try std.testing.expectEqualStrings("id", request.req.assignments[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, request.req.assignments[0].expression.kind);
    try std.testing.expectEqualStrings("source_id", request.req.assignments[0].expression.field);
    try std.testing.expectEqualStrings("status", request.req.assignments[1].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.req.assignments[1].expression.kind);
    try std.testing.expectEqualStrings("amount", request.req.assignments[2].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, request.req.assignments[2].expression.kind);
    try std.testing.expect(request.req.on_conflict != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.primary, request.req.on_conflict.?.target.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.nothing, request.req.on_conflict.?.action);
    try std.testing.expect(!request.req.returning_all);
    try std.testing.expectEqual(@as(usize, 2), request.req.returning.len);
    try std.testing.expectEqualStrings("id", request.req.returning[0]);
    try std.testing.expectEqual(@as(usize, 1), request.req.returning_expressions.len);
    try std.testing.expectEqualStrings("amount_plus_one", request.req.returning_expressions[0].output);

    var unique_request = try parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"source_id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"status\",\"expr\":{\"field\":\"status\"}}],\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"rows_source_id_key\"}},\"action\":\"update\",\"patch_expr\":{\"status\":{\"op\":\"coalesce\",\"args\":[{\"field\":\"status\",\"source\":\"proposed\"},{\"field\":\"status\",\"source\":\"existing\"},{\"value\":\"fallback\"}]}}}}",
        schema,
    );
    defer unique_request.deinit(std.testing.allocator);
    try std.testing.expect(unique_request.req.on_conflict != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsConflictTargetKind.unique, unique_request.req.on_conflict.?.target.kind);
    try std.testing.expectEqualStrings("rows_source_id_key", unique_request.req.on_conflict.?.target.unique_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsConflictAction.update, unique_request.req.on_conflict.?.action);
    try std.testing.expectEqual(@as(usize, 1), unique_request.req.on_conflict.?.patch_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, unique_request.req.on_conflict.?.patch_expressions[0].expression.kind);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:insert-source\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"source_id\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{\"source_cte\":\"ready_rows\"},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"source_id\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"id\",\"expr\":{\"field\":\"status\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{},\"assignments\":[{\"target_field\":\"amount\",\"expr\":{\"field\":\"status\"}}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsInsertSourceRequest(
        std.testing.allocator,
        "{\"op\":\"insert\",\"source\":{},\"assignments\":[{\"target_field\":\"status\",\"expr\":{\"field\":\"status\",\"source\":\"proposed\"}}]}",
        schema,
    ));

    const cross_assignments = [_]db_mod.types.RelationalRowsExpressionAssignment{
        .{ .field = "id", .expression = .{ .kind = .field, .field = "archive_id", .field_source = .source } },
        .{ .field = "status", .expression = .{ .kind = .field, .field = "archive_status", .field_source = .source } },
        .{ .field = "amount", .expression = .{ .kind = .field, .field = "archive_amount", .field_source = .source } },
    };
    const cross_predicates = [_]runtime_schema.RelationalCheck{.{
        .name = "",
        .field = "archive_status",
        .op = .eq,
        .value_json = "\"ready\"",
    }};
    const cross_returning = [_][]const u8{ "id", "status" };
    const cross_req: db_mod.types.RelationalRowsInsertSourceRequest = .{
        .source_table = "archived_records",
        .source = .{ .predicates = cross_predicates[0..] },
        .assignments = cross_assignments[0..],
        .returning = cross_returning[0..],
    };
    try db_mod.DB.validateRelationalRowsInsertSourceRequestWithSchemas(schema, source_schema, cross_req);
    const source_rows = [_][]const u8{"{\"archive_id\":\"a1\",\"archive_status\":\"ready\",\"archive_amount\":7}"};
    var cross_batch = try buildRowsInsertSourceBatchWithSchemasAlloc(
        std.testing.allocator,
        "usage_records",
        schema,
        source_schema,
        cross_req,
        source_rows[0..],
        null,
    );
    defer cross_batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), cross_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"a1\",\"status\":\"ready\",\"amount\":7}", cross_batch.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), cross_batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a1\",\"status\":\"ready\"}", cross_batch.returning_rows[0]);
}

test "relational rows joined mutation source contract parses lockable join plans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"quantity":{"type":"numeric"},"source_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}],\"order_by\":[{\"field\":\"amount\",\"direction\":\"desc\"}],\"limit\":5},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}],\"patch\":{\"status\":\"synced\"},\"returning\":[\"id\",\"quantity\"]}",
        schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, request.req.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, request.req.target_side);
    try std.testing.expect(request.req.join.left.row_claim != null);
    try std.testing.expect(request.req.join.right.row_claim == null);
    try std.testing.expectEqual(@as(usize, 1), request.req.join.on.len);
    try std.testing.expectEqual(@as(u32, 5), request.req.join.limit.?);
    try std.testing.expectEqual(@as(usize, 1), request.req.source_assignments.len);
    try std.testing.expectEqualStrings("quantity", request.req.source_assignments[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, request.req.source_assignments[0].source_side);
    try std.testing.expectEqualStrings("quantity", request.req.source_assignments[0].source_field);
    try std.testing.expectEqual(@as(usize, 1), request.req.operations.len);
    try std.testing.expectEqualStrings("status", request.req.operations[0].path);
    try std.testing.expectEqual(@as(usize, 2), request.req.returning.len);

    var delete_request = try parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"delete\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"expired\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"returning\":[\"id\"]}",
        schema,
    );
    defer delete_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, delete_request.req.kind);
    try std.testing.expectEqual(@as(usize, 0), delete_request.req.source_assignments.len);
    try std.testing.expectEqual(@as(usize, 1), delete_request.req.returning.len);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_patch\":{\"quantity\":{\"side\":\"right\",\"field\":\"quantity\"}}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"status\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"},{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"amount\"}]}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}],\"patch\":{\"quantity\":7}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"delete\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"expired\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}]}",
        schema,
    ));

    var source_table_request = try parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source_table\":\"source_records\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}],\"returning\":[\"id\"]}",
        schema,
    );
    defer source_table_request.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("source_records", source_table_request.req.source_table);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"source_table\":\"../source_records\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"id\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"quantity\"}],\"returning\":[\"id\"]}",
        schema,
    ));
}

test "relational rows joined mutation source validates target and source schemas independently" {
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"source_id":{"type":"keyword"},"quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"source_pk":{"type":"keyword"},"source_status":{"type":"keyword"},"source_quantity":{"type":"numeric"}},"required":["source_pk"],"additionalProperties":false}}},"primary_key":{"columns":["source_pk"]}}
    ;
    var parsed_target = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, target_schema_json);
    defer parsed_target.deinit(std.testing.allocator);
    const target_schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed_target);
    defer runtime_schema.freeSchema(std.testing.allocator, target_schema);
    var parsed_source = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, source_schema_json);
    defer parsed_source.deinit(std.testing.allocator);
    const source_schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed_source);
    defer runtime_schema.freeSchema(std.testing.allocator, source_schema);

    var request = try parseRowsJoinedMutationSourceRequestWithSchemas(
        std.testing.allocator,
        "{\"op\":\"update\",\"source_table\":\"source_records\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"source_status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"source_pk\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"source_quantity\"}],\"returning\":[\"id\",\"quantity\"],\"returning_expressions\":[{\"as\":\"source_status_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"source_status\",\"source\":\"source\"}]}}]}",
        target_schema,
        source_schema,
    );
    defer request.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), request.req.join.right.predicates.len);
    try std.testing.expectEqualStrings("source_records", request.req.source_table);
    try std.testing.expectEqualStrings("source_status", request.req.join.right.predicates[0].field);
    try std.testing.expectEqualStrings("source_pk", request.req.join.on[0].right_field);
    try std.testing.expectEqualStrings("source_quantity", request.req.source_assignments[0].source_field);
    try std.testing.expectEqual(@as(usize, 1), request.req.returning_expressions.len);
    try std.testing.expectEqualStrings("source_status_key", request.req.returning_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, request.req.returning_expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, request.req.returning_expressions[0].expression.operands[0].field_source);
    try std.testing.expectEqualStrings("source_status", request.req.returning_expressions[0].expression.operands[0].field);

    var delete_request = try parseRowsJoinedMutationSourceRequestWithSchemas(
        std.testing.allocator,
        "{\"op\":\"delete\",\"source_table\":\"source_records\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"expired\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"source_status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"source_pk\"}]},\"returning\":[\"id\"],\"returning_expressions\":[{\"as\":\"deleted_source_status_key\",\"expr\":{\"op\":\"lower\",\"args\":[{\"field\":\"source_status\",\"source\":\"source\"}]}}]}",
        target_schema,
        source_schema,
    );
    defer delete_request.deinit(std.testing.allocator);

    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, delete_request.req.kind);
    try std.testing.expectEqual(@as(usize, 0), delete_request.req.source_assignments.len);
    try std.testing.expectEqual(@as(usize, 1), delete_request.req.returning.len);
    try std.testing.expectEqual(@as(usize, 1), delete_request.req.returning_expressions.len);
    try std.testing.expectEqualStrings("deleted_source_status_key", delete_request.req.returning_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, delete_request.req.returning_expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionFieldSource.source, delete_request.req.returning_expressions[0].expression.operands[0].field_source);
    try std.testing.expectEqualStrings("source_status", delete_request.req.returning_expressions[0].expression.operands[0].field);

    try std.testing.expectError(error.InvalidRowsRequest, parseRowsJoinedMutationSourceRequest(
        std.testing.allocator,
        "{\"op\":\"update\",\"target_side\":\"left\",\"join\":{\"left\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"session:joined\",\"transaction_id\":\"00112233445566778899aabbccddeeff\"}},\"right\":{\"where\":{\"field\":\"source_status\",\"op\":\"eq\",\"value\":\"source\"}},\"on\":[{\"left_field\":\"source_id\",\"right_field\":\"source_pk\"}]},\"source_assignments\":[{\"target_field\":\"quantity\",\"side\":\"right\",\"field\":\"source_quantity\"}],\"returning\":[\"id\",\"quantity\"]}",
        target_schema,
    ));
}

test "relational rows api query contract parses typed json filters" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"text"},"status":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"attrs":{"type":"json"},"rank":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed.deinit(std.testing.allocator);
    const schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(std.testing.allocator, parsed);
    defer runtime_schema.freeSchema(std.testing.allocator, schema);

    var request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},{\"field\":\"name\",\"op\":\"text_pattern\",\"pattern\":\"AL%\",\"case_insensitive\":true},{\"field\":\"tags\",\"op\":\"array_any\",\"value\":\"hot\"},{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\",\"new\"]},{\"field\":\"tags\",\"op\":\"array_eq\",\"value\":[\"hot\",\"new\"]},{\"field\":\"attrs\",\"op\":\"json_contains\",\"value\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}},{\"field\":\"attrs\",\"op\":\"json_path_eq\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\"},{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"flags\"}]},\"select\":[\"id\",\"rank\"],\"json_extract\":[{\"as\":\"plan\",\"field\":\"attrs\",\"path\":[\"billing\",\"plan\"],\"as_text\":true},{\"as\":\"flags\",\"field\":\"attrs\",\"path\":\"flags\"}],\"order_by\":[{\"field\":\"rank\"}]}",
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
    try std.testing.expectEqual(@as(usize, 1), request.text_patterns.len);
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
    try std.testing.expectEqualStrings("name", request.text_patterns[0].field);
    try std.testing.expectEqualStrings("AL%", request.text_patterns[0].pattern);
    try std.testing.expect(request.text_patterns[0].case_insensitive);
    try std.testing.expectEqualStrings("plan", request.json_extract[0].output);
    try std.testing.expectEqualStrings("attrs", request.json_extract[0].field);
    try std.testing.expectEqualStrings("billing.plan", request.json_extract[0].path);
    try std.testing.expect(request.json_extract[0].as_text);
    try std.testing.expectEqualStrings("flags", request.json_extract[1].output);
    try std.testing.expect(!request.json_extract[1].as_text);
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}],\"extra\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\",\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\"],\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"status\",\"op\":\"in\",\"value\":[\"active\"],\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"attrs\",\"op\":\"json_contains\",\"value\":{\"billing\":{\"plan\":\"pro\"}},\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"attrs\",\"op\":\"json_path_eq\",\"path\":[\"billing\",\"plan\"],\"value\":\"pro\",\"unknown\":true}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"flags\",\"value\":\"bad\"}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"name\",\"op\":\"text_pattern\",\"pattern\":\"AL%\",\"case_insensitive\":true,\"unknown\":true}}",
        schema,
    ));

    const rows = [_][]const u8{
        "{\"id\":\"a\",\"name\":\"Alice\",\"status\":\"active\",\"tags\":[\"hot\",\"new\"],\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\",\"beta\"]},\"rank\":2}",
        "{\"id\":\"b\",\"name\":\"Bob\",\"status\":\"active\",\"tags\":[\"cold\"],\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]},\"rank\":1}",
        "{\"id\":\"c\",\"name\":\"Ada\",\"status\":\"active\",\"tags\":[\"hot\"],\"attrs\":{\"billing\":{\"plan\":\"free\"},\"flags\":[\"active\"]},\"rank\":3}",
    };
    var result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, request, rows[0..]);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"rank\":2,\"plan\":\"pro\",\"flags\":[\"active\",\"beta\"]}", result.rows[0]);

    var access_any_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"any\":[{\"all\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"},{\"field\":\"attrs\",\"op\":\"json_contains\",\"value\":{\"billing\":{\"plan\":\"pro\"}}}]},{\"all\":[{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\"]},{\"field\":\"name\",\"op\":\"text_pattern\",\"pattern\":\"Ad%\"}]}]},\"select\":[\"id\"],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer access_any_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), access_any_request.or_predicates.len);
    try std.testing.expectEqual(@as(usize, 2), access_any_request.access_or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), access_any_request.access_or_predicates[0].predicates.len);
    try std.testing.expectEqual(@as(usize, 1), access_any_request.access_or_predicates[0].json_contains.len);
    try std.testing.expectEqual(@as(usize, 1), access_any_request.access_or_predicates[1].array_contains.len);
    try std.testing.expectEqual(@as(usize, 1), access_any_request.access_or_predicates[1].text_patterns.len);
    var access_any_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, access_any_request, rows[0..]);
    defer access_any_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 3), access_any_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", access_any_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", access_any_result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", access_any_result.rows[2]);

    var access_not_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"not\":[{\"all\":[{\"field\":\"attrs\",\"op\":\"json_path_exists\",\"path\":\"flags\"},{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"cold\"]}]}]},\"select\":[\"id\"],\"order_by\":[{\"field\":\"id\"}]}",
        schema,
    );
    defer access_not_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), access_not_request.not_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), access_not_request.access_not_predicates.len);
    var access_not_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, access_not_request, rows[0..]);
    defer access_not_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), access_not_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", access_not_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", access_not_result.rows[1]);

    var null_safe_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"is_not_distinct\",\"value\":null}]},\"select\":[\"id\"]}",
        schema,
    );
    defer null_safe_request.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), null_safe_request.predicates.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_not_distinct, null_safe_request.predicates[0].op);

    const null_safe_rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"active\"}",
        "{\"id\":\"b\",\"status\":null}",
        "{\"id\":\"c\"}",
        "{\"id\":\"d\",\"status\":\"inactive\"}",
    };
    var null_safe_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, null_safe_request, null_safe_rows[0..]);
    defer null_safe_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), null_safe_result.total);
    try std.testing.expectEqual(@as(usize, 2), null_safe_result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", null_safe_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", null_safe_result.rows[1]);

    var distinct_request = try parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"all\":[{\"field\":\"status\",\"op\":\"is_distinct\",\"value\":\"active\"}]},\"select\":[\"id\"]}",
        schema,
    );
    defer distinct_request.deinit(std.testing.allocator);
    var distinct_result = try executeRowsQueryOnJsonRowsAlloc(std.testing.allocator, schema, distinct_request, null_safe_rows[0..]);
    defer distinct_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 3), distinct_result.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", distinct_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"c\"}", distinct_result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"d\"}", distinct_result.rows[2]);

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
        "{\"where\":{\"field\":\"tags\",\"op\":\"array_any\",\"value\":3}}",
        schema,
    ));
    try std.testing.expectError(error.InvalidRowsRequest, parseRowsQueryRequest(
        std.testing.allocator,
        "{\"where\":{\"field\":\"tags\",\"op\":\"array_contains\",\"value\":[\"hot\",3]}}",
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
