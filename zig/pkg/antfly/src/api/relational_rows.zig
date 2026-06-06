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
const runtime_schema = @import("../storage/schema.zig");

const physical_primary_key_prefix = "\x00antfly-rel-pk:";

pub const OwnedRowsBatchRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    transforms: []db_mod.types.DocumentTransform = &.{},
    predicates: []db_mod.types.TransactionVersionPredicate = &.{},
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

pub const UniqueSelectorResolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8,

    pub fn resolveUnique(
        self: UniqueSelectorResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) !?[]u8 {
        return try self.resolve(self.ptr, alloc, table_name, constraint_name, encoded_value);
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
            const row_json = try jsonValueStringifyAlloc(alloc, row_value);
            var row_json_transferred = false;
            errdefer if (!row_json_transferred) alloc.free(row_json);
            const key = try physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_json);
            var key_transferred = false;
            errdefer if (!key_transferred) alloc.free(key);
            try writes.append(alloc, .{ .key = key, .value = row_json });
            key_transferred = true;
            row_json_transferred = true;
            if (std.mem.eql(u8, op_text, "insert")) {
                const predicate_key = try alloc.dupe(u8, key);
                var predicate_key_transferred = false;
                errdefer if (!predicate_key_transferred) alloc.free(predicate_key);
                try predicates.append(alloc, .{ .key = predicate_key, .expected_version = 0 });
                predicate_key_transferred = true;
            }
            inserted += 1;
            continue;
        }

        if (std.mem.eql(u8, op_text, "delete")) {
            const key = (try physicalPrimaryKeyFromWhereAlloc(alloc, table_name, schema, op_value.object.get("where") orelse return error.InvalidRowsRequest, unique_resolver, false)) orelse return error.RowSelectorNotFound;
            var key_transferred = false;
            errdefer if (!key_transferred) alloc.free(key);
            try appendExpectedVersionPredicateAlloc(alloc, &predicates, op_value, key);
            try deletes.append(alloc, key);
            key_transferred = true;
            deleted += 1;
            continue;
        }

        if (std.mem.eql(u8, op_text, "update")) {
            const key = (try physicalPrimaryKeyFromWhereAlloc(alloc, table_name, schema, op_value.object.get("where") orelse return error.InvalidRowsRequest, unique_resolver, false)) orelse return error.RowSelectorNotFound;
            var key_transferred = false;
            errdefer if (!key_transferred) alloc.free(key);
            const patch = op_value.object.get("patch") orelse return error.InvalidRowsRequest;
            if (patch != .object) return error.InvalidRowsRequest;
            const operations = try patchToTransformOperationsAlloc(alloc, schema.primary_key.?, patch);
            var operations_transferred = false;
            errdefer if (!operations_transferred) freeTransformOps(alloc, operations);
            try appendExpectedVersionPredicateAlloc(alloc, &predicates, op_value, key);
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

    return .{
        .writes = writes_owned,
        .deletes = deletes_owned,
        .transforms = transforms_owned,
        .predicates = predicates_owned,
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

pub fn encodeRowsBatchResponseAlloc(alloc: std.mem.Allocator, req: OwnedRowsBatchRequest) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{{\"inserted\":{d},\"deleted\":{d},\"transformed\":{d}}}", .{
        req.inserted,
        req.deleted,
        req.transformed,
    });
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

fn patchToTransformOperationsAlloc(
    alloc: std.mem.Allocator,
    primary_key: runtime_schema.PrimaryKey,
    patch: std.json.Value,
) ![]db_mod.types.TransformOp {
    var operations = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer freeTransformOps(alloc, operations.items);
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
    return try operations.toOwnedSlice(alloc);
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
