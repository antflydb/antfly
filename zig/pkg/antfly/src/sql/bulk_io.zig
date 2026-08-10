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

const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
};
const ddl_plan = @import("ddl_plan.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const usermgr = @import("../usermgr/mod.zig");

pub const BulkSqlIoOperation = enum {
    import_rows,
    export_rows,
};

pub const BulkSqlIoNativeRoute = enum {
    rows_batch,
    rows_query,
};

pub const BulkSqlIoStream = enum {
    stdin,
    stdout,
    file,
    program,
};

pub const BulkSqlIoCodec = enum {
    postgres_text,
    csv,
    postgres_binary,
};

pub const BulkSqlIoAuditAction = enum {
    copy_from,
    copy_to,
};

pub const BulkSqlIoExecutionPlan = struct {
    operation: BulkSqlIoOperation,
    native_route: BulkSqlIoNativeRoute,
    stream: BulkSqlIoStream,
    codec: BulkSqlIoCodec,
    endpoint_kind: ddl_plan.BulkIoEndpointKind = .stream,
    endpoint: []const u8,
    table_name: []const u8,
    columns: []const []const u8 = &.{},
    where_expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    required_resource_type: usermgr.ResourceType = .table,
    required_permission: usermgr.PermissionType,
    audit_action: BulkSqlIoAuditAction,
    requires_external_stream: bool = true,
    freeze: bool = false,
    on_error: ddl_plan.BulkIoOnErrorPolicy = .stop,
    reject_limit: ?usize = null,
    log_verbosity: ddl_plan.BulkIoLogVerbosity = .default,
    header: bool = false,
    delimiter: ?[]const u8 = null,
    quote: ?[]const u8 = null,
    escape: ?[]const u8 = null,
    null_marker: ?[]const u8 = null,
    default_marker: ?[]const u8 = null,
    encoding: ?[]const u8 = null,
    force_quote_all: bool = false,
    force_quote_columns: []const []const u8 = &.{},
    force_not_null_columns: []const []const u8 = &.{},
    force_null_columns: []const []const u8 = &.{},
};

const BulkSqlIoCsvField = struct {
    value: []const u8,
    quoted: bool = false,
};

pub fn executionPlanFromDdlPlan(plan: ddl_plan.BulkIoPlan) !BulkSqlIoExecutionPlan {
    const codec = try codecFromPlan(plan);
    switch (plan.direction) {
        .from => {
            const stream = switch (plan.endpoint_kind) {
                .stream => blk: {
                    if (!std.ascii.eqlIgnoreCase(plan.endpoint, "STDIN")) return error.UnsupportedSqlShape;
                    break :blk BulkSqlIoStream.stdin;
                },
                .file => BulkSqlIoStream.file,
                .program => BulkSqlIoStream.program,
            };
            return .{
                .operation = .import_rows,
                .native_route = .rows_batch,
                .stream = stream,
                .codec = codec,
                .endpoint_kind = plan.endpoint_kind,
                .endpoint = plan.endpoint,
                .table_name = plan.table_name,
                .columns = plan.columns,
                .where_expressions = plan.where_expressions,
                .required_permission = .write,
                .audit_action = .copy_from,
                .requires_external_stream = stream == .stdin,
                .freeze = plan.freeze,
                .on_error = plan.on_error,
                .reject_limit = plan.reject_limit,
                .log_verbosity = plan.log_verbosity,
                .header = plan.header,
                .delimiter = plan.delimiter,
                .quote = plan.quote,
                .escape = plan.escape,
                .null_marker = plan.null_marker,
                .default_marker = plan.default_marker,
                .encoding = plan.encoding,
                .force_quote_all = plan.force_quote_all,
                .force_quote_columns = plan.force_quote_columns,
                .force_not_null_columns = plan.force_not_null_columns,
                .force_null_columns = plan.force_null_columns,
            };
        },
        .to => {
            const stream = switch (plan.endpoint_kind) {
                .stream => blk: {
                    if (!std.ascii.eqlIgnoreCase(plan.endpoint, "STDOUT")) return error.UnsupportedSqlShape;
                    break :blk BulkSqlIoStream.stdout;
                },
                .file => BulkSqlIoStream.file,
                .program => BulkSqlIoStream.program,
            };
            return .{
                .operation = .export_rows,
                .native_route = .rows_query,
                .stream = stream,
                .codec = codec,
                .endpoint_kind = plan.endpoint_kind,
                .endpoint = plan.endpoint,
                .table_name = plan.table_name,
                .columns = plan.columns,
                .where_expressions = plan.where_expressions,
                .required_permission = .read,
                .audit_action = .copy_to,
                .requires_external_stream = stream == .stdout,
                .header = plan.header,
                .delimiter = plan.delimiter,
                .quote = plan.quote,
                .escape = plan.escape,
                .null_marker = plan.null_marker,
                .encoding = plan.encoding,
                .force_quote_all = plan.force_quote_all,
                .force_quote_columns = plan.force_quote_columns,
            };
        },
    }
}

fn codecFromPlan(plan: ddl_plan.BulkIoPlan) !BulkSqlIoCodec {
    const format = plan.format orelse return .postgres_text;
    if (std.ascii.eqlIgnoreCase(format, "csv")) return .csv;
    if (std.ascii.eqlIgnoreCase(format, "text")) return .postgres_text;
    if (std.ascii.eqlIgnoreCase(format, "binary")) return .postgres_binary;
    return error.UnsupportedSqlShape;
}

pub fn executionFingerprintAlloc(alloc: std.mem.Allocator, plan: BulkSqlIoExecutionPlan) ![]const u8 {
    if (plan.endpoint_kind == .stream) {
        return try std.fmt.allocPrint(
            alloc,
            "bulk_sql_io:op={s}:native={s}:stream={s}:codec={s}:auth={s}/{s}:audit={s}:table={s}:columns={d}:where_expr={d}:requires_stream={}",
            .{
                @tagName(plan.operation),
                @tagName(plan.native_route),
                @tagName(plan.stream),
                @tagName(plan.codec),
                plan.required_resource_type.slice(),
                plan.required_permission.slice(),
                @tagName(plan.audit_action),
                plan.table_name,
                plan.columns.len,
                plan.where_expressions.len,
                plan.requires_external_stream,
            },
        );
    }
    return try std.fmt.allocPrint(
        alloc,
        "bulk_sql_io:op={s}:native={s}:stream={s}:codec={s}:endpoint_kind={s}:endpoint={s}:auth={s}/{s}:audit={s}:table={s}:columns={d}:where_expr={d}:requires_stream={}",
        .{
            @tagName(plan.operation),
            @tagName(plan.native_route),
            @tagName(plan.stream),
            @tagName(plan.codec),
            @tagName(plan.endpoint_kind),
            plan.endpoint,
            plan.required_resource_type.slice(),
            plan.required_permission.slice(),
            @tagName(plan.audit_action),
            plan.table_name,
            plan.columns.len,
            plan.where_expressions.len,
            plan.requires_external_stream,
        },
    );
}

pub fn validateCsvOptions(plan: BulkSqlIoExecutionPlan) !void {
    const delimiter = try singleByteOption(plan.delimiter, ',');
    const quote = try singleByteOption(plan.quote, '"');
    _ = try singleByteOption(plan.escape, quote);
    if (delimiter == quote) return error.UnsupportedSqlShape;
}

fn validatePostgresTextOptions(plan: BulkSqlIoExecutionPlan) !void {
    const delimiter = try singleByteOption(plan.delimiter, '\t');
    if (delimiter == '\n' or delimiter == '\r') return error.UnsupportedSqlShape;
    if (plan.quote != null or plan.escape != null) return error.UnsupportedSqlShape;
    if (plan.force_quote_all or plan.force_quote_columns.len != 0) return error.UnsupportedSqlShape;
    if (plan.force_not_null_columns.len != 0 or plan.force_null_columns.len != 0) return error.UnsupportedSqlShape;
}

fn validatePostgresBinaryOptions(plan: BulkSqlIoExecutionPlan) !void {
    if (plan.header) return error.UnsupportedSqlShape;
    if (plan.delimiter != null or plan.quote != null or plan.escape != null) return error.UnsupportedSqlShape;
    if (plan.null_marker != null or plan.default_marker != null or plan.encoding != null) return error.UnsupportedSqlShape;
    if (plan.force_quote_all or plan.force_quote_columns.len != 0) return error.UnsupportedSqlShape;
    if (plan.force_not_null_columns.len != 0 or plan.force_null_columns.len != 0) return error.UnsupportedSqlShape;
}

fn validateCodecOptions(plan: BulkSqlIoExecutionPlan) !void {
    switch (plan.codec) {
        .csv => try validateCsvOptions(plan),
        .postgres_text => try validatePostgresTextOptions(plan),
        .postgres_binary => try validatePostgresBinaryOptions(plan),
    }
}

pub fn validatePlanForSchema(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
) !void {
    switch (plan.operation) {
        .import_rows => {
            if (plan.columns.len == 0) return error.UnsupportedSqlShape;
            try validateColumnListForSchema(alloc, schema, plan.columns, .reject_generated);
            try validateOptionColumns(alloc, schema, plan.columns, plan.force_not_null_columns);
            try validateOptionColumns(alloc, schema, plan.columns, plan.force_null_columns);
            if (plan.force_quote_all or plan.force_quote_columns.len != 0) return error.UnsupportedSqlShape;
        },
        .export_rows => {
            try validateColumnListForSchema(alloc, schema, plan.columns, .allow_generated);
            try validateOptionColumns(alloc, schema, plan.columns, plan.force_quote_columns);
            if (plan.force_not_null_columns.len != 0 or plan.force_null_columns.len != 0) return error.UnsupportedSqlShape;
        },
    }
    if (plan.codec == .postgres_binary) try validatePostgresBinaryColumns(schema, plan);
    for (plan.where_expressions) |condition| try validateExpressionConditionForSchema(schema, condition);
}

fn validatePostgresBinaryColumns(
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
) !void {
    if (plan.columns.len == 0) {
        for (schema.relational_columns) |column| try validatePostgresBinaryColumn(column);
        return;
    }
    for (plan.columns) |column_name| {
        const column = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
        try validatePostgresBinaryColumn(column);
    }
}

fn validatePostgresBinaryColumn(column: runtime_schema.RelationalColumn) !void {
    if (column.generated != null) return error.InvalidRowsRequest;
    switch (column.field_type) {
        .keyword, .text, .link, .html, .search_as_you_type, .datetime, .boolean => return,
        else => return error.UnsupportedSqlShape,
    }
}

pub fn importRowsBatchFromStdinAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    stdin_payload: []const u8,
) !relational_rows.OwnedRowsBatchRequest {
    return try importRowsBatchFromStdinWithDefaultContextAlloc(alloc, schema, plan, stdin_payload, .{});
}

pub fn importRowsBatchFromStdinWithDefaultContextAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    stdin_payload: []const u8,
    default_context: relational_rows.DefaultValueContext,
) !relational_rows.OwnedRowsBatchRequest {
    if (plan.operation != .import_rows or plan.native_route != .rows_batch or (plan.endpoint_kind != .stream and plan.endpoint_kind != .file and plan.endpoint_kind != .program)) return error.UnsupportedSqlShape;
    if (plan.endpoint_kind == .stream and plan.stream != .stdin) return error.UnsupportedSqlShape;
    if (plan.endpoint_kind == .file and plan.stream != .file) return error.UnsupportedSqlShape;
    if (plan.endpoint_kind == .program and plan.stream != .program) return error.UnsupportedSqlShape;
    if (plan.columns.len == 0) return error.UnsupportedSqlShape;
    try validateCodecOptions(plan);
    try validatePlanForSchema(alloc, schema, plan);
    if (plan.encoding) |encoding| {
        if (!std.ascii.eqlIgnoreCase(encoding, "UTF8") and !std.ascii.eqlIgnoreCase(encoding, "UTF-8")) {
            return error.UnsupportedSqlShape;
        }
    }
    if (plan.codec == .postgres_binary) return try importRowsBatchFromPostgresBinaryWithDefaultContextAlloc(alloc, schema, plan, stdin_payload, default_context);

    var body = std.ArrayList(u8).empty;
    defer body.deinit(alloc);
    try body.appendSlice(alloc, "{\"operations\":[");
    var appended: usize = 0;
    var skipped_header = false;
    var line_start: usize = 0;
    while (line_start <= stdin_payload.len) {
        const newline_offset = std.mem.indexOfScalar(u8, stdin_payload[line_start..], '\n');
        const line_end = if (newline_offset) |offset| line_start + offset else stdin_payload.len;
        var line = stdin_payload[line_start..line_end];
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        line_start = line_end + 1;

        if (line.len == 0 and line_end == stdin_payload.len) break;
        if (std.mem.eql(u8, line, "\\.")) break;
        if (line.len == 0) continue;
        if (plan.header and !skipped_header) {
            skipped_header = true;
            continue;
        }

        const fields = try parseRecordAlloc(alloc, line, plan);
        defer freeCsvFields(alloc, fields);
        if (fields.len != plan.columns.len) return error.InvalidRowsRequest;
        const row_json = try rowJsonFromCsvFieldsAlloc(alloc, schema, plan, fields);
        defer alloc.free(row_json);
        if (!try rowMatchesWhereAlloc(alloc, row_json, plan.where_expressions)) continue;

        if (appended != 0) try body.append(alloc, ',');
        try body.appendSlice(alloc, "{\"op\":\"insert\",\"row\":");
        try body.appendSlice(alloc, row_json);
        try body.append(alloc, '}');
        appended += 1;
    }
    try body.appendSlice(alloc, "]}");
    return try relational_rows.parseRowsBatchRequestWithResolverAndDefaultContext(alloc, plan.table_name, body.items, schema, null, default_context);
}

const postgres_binary_copy_signature = "PGCOPY\n\xff\r\n\x00";

fn importRowsBatchFromPostgresBinaryAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    payload: []const u8,
) !relational_rows.OwnedRowsBatchRequest {
    return try importRowsBatchFromPostgresBinaryWithDefaultContextAlloc(alloc, schema, plan, payload, .{});
}

fn importRowsBatchFromPostgresBinaryWithDefaultContextAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    payload: []const u8,
    default_context: relational_rows.DefaultValueContext,
) !relational_rows.OwnedRowsBatchRequest {
    if (payload.len < postgres_binary_copy_signature.len + 8) return error.InvalidRowsRequest;
    if (!std.mem.eql(u8, payload[0..postgres_binary_copy_signature.len], postgres_binary_copy_signature)) return error.InvalidRowsRequest;
    var pos: usize = postgres_binary_copy_signature.len;
    const flags = try readBinaryCopyU32(payload, &pos);
    if (flags != 0) return error.UnsupportedSqlShape;
    const extension_len = try readBinaryCopyI32(payload, &pos);
    if (extension_len < 0) return error.InvalidRowsRequest;
    const extension_len_usize: usize = @intCast(extension_len);
    if (pos + extension_len_usize > payload.len) return error.InvalidRowsRequest;
    pos += extension_len_usize;

    var body = std.ArrayList(u8).empty;
    defer body.deinit(alloc);
    try body.appendSlice(alloc, "{\"operations\":[");
    var appended: usize = 0;
    while (true) {
        const field_count = try readBinaryCopyI16(payload, &pos);
        if (field_count == -1) break;
        if (field_count < 0 or @as(usize, @intCast(field_count)) != plan.columns.len) return error.InvalidRowsRequest;

        const row_json = try rowJsonFromPostgresBinaryFieldsAlloc(alloc, schema, plan, payload, &pos, @intCast(field_count));
        defer alloc.free(row_json);
        if (!try rowMatchesWhereAlloc(alloc, row_json, plan.where_expressions)) continue;

        if (appended != 0) try body.append(alloc, ',');
        try body.appendSlice(alloc, "{\"op\":\"insert\",\"row\":");
        try body.appendSlice(alloc, row_json);
        try body.append(alloc, '}');
        appended += 1;
    }
    if (pos != payload.len) return error.InvalidRowsRequest;
    try body.appendSlice(alloc, "]}");
    return try relational_rows.parseRowsBatchRequestWithResolverAndDefaultContext(alloc, plan.table_name, body.items, schema, null, default_context);
}

fn rowJsonFromPostgresBinaryFieldsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    payload: []const u8,
    pos: *usize,
    field_count: usize,
) ![]u8 {
    var row = std.ArrayList(u8).empty;
    errdefer row.deinit(alloc);
    try row.append(alloc, '{');
    var emitted: usize = 0;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (0..field_count) |i| {
        const column_name = plan.columns[i];
        if (seen.contains(column_name)) return error.InvalidRowsRequest;
        try seen.put(alloc, column_name, {});
        const column = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
        const len = try readBinaryCopyI32(payload, pos);
        const value_bytes: ?[]const u8 = if (len == -1) null else blk: {
            if (len < 0) return error.InvalidRowsRequest;
            const len_usize: usize = @intCast(len);
            if (pos.* + len_usize > payload.len) return error.InvalidRowsRequest;
            const value = payload[pos.* .. pos.* + len_usize];
            pos.* += len_usize;
            break :blk value;
        };
        const value_json = try postgresBinaryValueJsonAlloc(alloc, column, value_bytes);
        defer alloc.free(value_json);
        if (emitted != 0) try row.append(alloc, ',');
        const key_json = try std.json.Stringify.valueAlloc(alloc, column_name, .{});
        defer alloc.free(key_json);
        try row.appendSlice(alloc, key_json);
        try row.append(alloc, ':');
        try row.appendSlice(alloc, value_json);
        emitted += 1;
    }
    try row.append(alloc, '}');
    return try row.toOwnedSlice(alloc);
}

fn postgresBinaryValueJsonAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value_bytes: ?[]const u8,
) ![]u8 {
    const bytes = value_bytes orelse return try alloc.dupe(u8, "null");
    switch (column.field_type) {
        .keyword, .text, .link, .html, .search_as_you_type, .datetime => {
            _ = std.unicode.utf8CountCodepoints(bytes) catch return error.InvalidRowsRequest;
            return try std.json.Stringify.valueAlloc(alloc, bytes, .{});
        },
        .boolean => {
            if (bytes.len != 1) return error.InvalidRowsRequest;
            if (bytes[0] == 0) return try alloc.dupe(u8, "false");
            if (bytes[0] == 1) return try alloc.dupe(u8, "true");
            return error.InvalidRowsRequest;
        },
        else => return error.UnsupportedSqlShape,
    }
}

pub fn exportRowsCsvToStdoutAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    result: db_mod.types.RelationalRowsQueryResult,
) ![]u8 {
    if (plan.operation != .export_rows or plan.native_route != .rows_query or (plan.stream != .stdout and plan.stream != .file and plan.stream != .program)) return error.UnsupportedSqlShape;
    try validateCodecOptions(plan);
    try validatePlanForSchema(alloc, schema, plan);
    if (plan.encoding) |encoding| {
        if (!std.ascii.eqlIgnoreCase(encoding, "UTF8") and !std.ascii.eqlIgnoreCase(encoding, "UTF-8")) {
            return error.UnsupportedSqlShape;
        }
    }

    const columns = try exportColumnsAlloc(alloc, schema, plan);
    defer alloc.free(columns);
    if (plan.codec == .postgres_binary) {
        return try exportRowsPostgresBinaryToStdoutAlloc(alloc, schema, plan, result, columns);
    }

    const delimiter = try singleByteOption(plan.delimiter, switch (plan.codec) {
        .csv => ',',
        .postgres_text => '\t',
        .postgres_binary => unreachable,
    });
    const quote = try singleByteOption(plan.quote, '"');
    const escape = try singleByteOption(plan.escape, quote);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;

    if (plan.header) {
        for (columns, 0..) |column_name, i| {
            if (i != 0) try writer.writeByte(delimiter);
            switch (plan.codec) {
                .csv => try appendCsvField(writer, column_name, delimiter, quote, escape, false),
                .postgres_text => try appendPostgresTextField(writer, column_name, delimiter),
                .postgres_binary => unreachable,
            }
        }
        try writer.writeByte('\n');
    }

    for (result.rows) |row_json| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        for (columns, 0..) |column_name, i| {
            if (i != 0) try writer.writeByte(delimiter);
            const value = parsed.value.object.get(column_name);
            if (value == null or value.? == .null) {
                switch (plan.codec) {
                    .csv => {
                        const force_quote = plan.force_quote_all or stringSliceContains(plan.force_quote_columns, column_name);
                        try appendCsvField(writer, nullMarker(plan), delimiter, quote, escape, force_quote);
                    },
                    .postgres_text => try writer.writeAll(nullMarker(plan)),
                    .postgres_binary => unreachable,
                }
                continue;
            }
            const field = try fieldFromJsonValueAlloc(alloc, value, plan);
            defer alloc.free(field);
            switch (plan.codec) {
                .csv => {
                    const force_quote = plan.force_quote_all or stringSliceContains(plan.force_quote_columns, column_name);
                    try appendCsvField(writer, field, delimiter, quote, escape, force_quote);
                },
                .postgres_text => try appendPostgresTextField(writer, field, delimiter),
                .postgres_binary => unreachable,
            }
        }
        try writer.writeByte('\n');
    }

    return try out.toOwnedSlice();
}

fn exportRowsPostgresBinaryToStdoutAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    result: db_mod.types.RelationalRowsQueryResult,
    columns: []const []const u8,
) ![]u8 {
    _ = plan;
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, postgres_binary_copy_signature);
    try appendBinaryCopyU32(alloc, &out, 0);
    try appendBinaryCopyU32(alloc, &out, 0);
    for (result.rows) |row_json| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        if (columns.len > std.math.maxInt(i16)) return error.InvalidRowsRequest;
        try appendBinaryCopyI16(alloc, &out, @intCast(columns.len));
        for (columns) |column_name| {
            const value = parsed.value.object.get(column_name) orelse std.json.Value.null;
            if (value == .null) {
                try appendBinaryCopyI32(alloc, &out, -1);
                continue;
            }
            const column = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
            const field = try postgresBinaryFieldBytesAlloc(alloc, column, value);
            defer alloc.free(field);
            if (field.len > std.math.maxInt(i32)) return error.InvalidRowsRequest;
            try appendBinaryCopyI32(alloc, &out, @intCast(field.len));
            try out.appendSlice(alloc, field);
        }
    }
    try appendBinaryCopyI16(alloc, &out, -1);
    return try out.toOwnedSlice(alloc);
}

fn postgresBinaryFieldBytesAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    value: std.json.Value,
) ![]u8 {
    switch (column.field_type) {
        .keyword, .text, .link, .html, .search_as_you_type, .datetime => return switch (value) {
            .string => |raw| try alloc.dupe(u8, raw),
            else => error.InvalidRowsRequest,
        },
        .boolean => return switch (value) {
            .bool => |raw| blk: {
                const out = try alloc.alloc(u8, 1);
                out[0] = if (raw) 1 else 0;
                break :blk out;
            },
            else => error.InvalidRowsRequest,
        },
        else => return error.UnsupportedSqlShape,
    }
}

fn readBinaryCopyI16(data: []const u8, pos: *usize) !i16 {
    if (pos.* + 2 > data.len) return error.InvalidRowsRequest;
    const value = std.mem.readInt(i16, data[pos.*..][0..2], .big);
    pos.* += 2;
    return value;
}

fn readBinaryCopyI32(data: []const u8, pos: *usize) !i32 {
    if (pos.* + 4 > data.len) return error.InvalidRowsRequest;
    const value = std.mem.readInt(i32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return value;
}

fn readBinaryCopyU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidRowsRequest;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return value;
}

fn appendBinaryCopyI16(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(u8),
    value: i16,
) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(i16, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendBinaryCopyI32(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(u8),
    value: i32,
) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendBinaryCopyU32(
    alloc: std.mem.Allocator,
    out: *std.ArrayList(u8),
    value: u32,
) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn exportColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
) ![]const []const u8 {
    const count = if (plan.columns.len == 0) schema.relational_columns.len else plan.columns.len;
    const columns = try alloc.alloc([]const u8, count);
    errdefer alloc.free(columns);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    if (plan.columns.len == 0) {
        for (schema.relational_columns, 0..) |column, i| {
            columns[i] = column.name;
            try seen.put(alloc, column.name, {});
        }
    } else {
        for (plan.columns, 0..) |column_name, i| {
            if (seen.contains(column_name)) return error.InvalidRowsRequest;
            _ = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
            try seen.put(alloc, column_name, {});
            columns[i] = column_name;
        }
    }
    return columns;
}

fn fieldFromJsonValueAlloc(
    alloc: std.mem.Allocator,
    maybe_value: ?std.json.Value,
    plan: BulkSqlIoExecutionPlan,
) ![]u8 {
    const value = maybe_value orelse return try alloc.dupe(u8, nullMarker(plan));
    return switch (value) {
        .null => try alloc.dupe(u8, nullMarker(plan)),
        .string => |raw| try alloc.dupe(u8, raw),
        .bool => |raw| try alloc.dupe(u8, if (raw) "true" else "false"),
        .integer, .float, .object, .array => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        else => error.UnsupportedSqlShape,
    };
}

fn appendCsvField(
    writer: anytype,
    field: []const u8,
    delimiter: u8,
    quote: u8,
    escape: u8,
    force_quote: bool,
) !void {
    var needs_quote = force_quote;
    for (field) |c| {
        if (c == delimiter or c == quote or c == escape or c == '\n' or c == '\r') {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        try writer.writeAll(field);
        return;
    }
    try writer.writeByte(quote);
    for (field) |c| {
        if (c == quote or c == escape) try writer.writeByte(escape);
        try writer.writeByte(c);
    }
    try writer.writeByte(quote);
}

fn appendPostgresTextField(writer: anytype, field: []const u8, delimiter: u8) !void {
    for (field) |c| {
        switch (c) {
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => if (delimiter == '\t') try writer.writeAll("\\t") else try writer.writeByte(c),
            '\x08' => try writer.writeAll("\\b"),
            '\x0c' => try writer.writeAll("\\f"),
            else => {
                if (c == delimiter) {
                    try writer.writeByte('\\');
                    try writer.writeByte(c);
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

fn parseRecordAlloc(
    alloc: std.mem.Allocator,
    line: []const u8,
    plan: BulkSqlIoExecutionPlan,
) ![]BulkSqlIoCsvField {
    return switch (plan.codec) {
        .csv => try parseCsvRecordAlloc(alloc, line, plan),
        .postgres_text => try parsePostgresTextRecordAlloc(alloc, line, plan),
        .postgres_binary => unreachable,
    };
}

fn parseCsvRecordAlloc(
    alloc: std.mem.Allocator,
    line: []const u8,
    plan: BulkSqlIoExecutionPlan,
) ![]BulkSqlIoCsvField {
    const delimiter = try singleByteOption(plan.delimiter, ',');
    const quote = try singleByteOption(plan.quote, '"');
    const escape = try singleByteOption(plan.escape, quote);
    var fields = std.ArrayListUnmanaged(BulkSqlIoCsvField).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field.value);
        fields.deinit(alloc);
    }
    var field = std.ArrayListUnmanaged(u8).empty;
    errdefer field.deinit(alloc);
    var in_quote = false;
    var field_quoted = false;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (in_quote) {
            if (c == quote) {
                if (i + 1 < line.len and line[i + 1] == quote) {
                    i += 1;
                    try field.append(alloc, quote);
                    continue;
                }
                in_quote = false;
                continue;
            }
            if (c == escape and i + 1 < line.len) {
                i += 1;
                try field.append(alloc, line[i]);
                continue;
            }
            try field.append(alloc, c);
            continue;
        }
        if (c == quote and field.items.len == 0) {
            in_quote = true;
            field_quoted = true;
            continue;
        }
        if (c == delimiter) {
            try appendCsvFieldAlloc(alloc, &fields, &field, field_quoted);
            field_quoted = false;
            continue;
        }
        try field.append(alloc, c);
    }
    if (in_quote) return error.InvalidRowsRequest;
    try appendCsvFieldAlloc(alloc, &fields, &field, field_quoted);
    return try fields.toOwnedSlice(alloc);
}

fn parsePostgresTextRecordAlloc(
    alloc: std.mem.Allocator,
    line: []const u8,
    plan: BulkSqlIoExecutionPlan,
) ![]BulkSqlIoCsvField {
    const delimiter = try singleByteOption(plan.delimiter, '\t');
    var fields = std.ArrayListUnmanaged(BulkSqlIoCsvField).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field.value);
        fields.deinit(alloc);
    }
    var field = std.ArrayListUnmanaged(u8).empty;
    errdefer field.deinit(alloc);
    const marker = nullMarker(plan);
    var field_start: usize = 0;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (c == delimiter) {
            try appendPostgresTextFieldAlloc(alloc, &fields, &field, line[field_start..i], marker);
            field_start = i + 1;
            continue;
        }
        if (c == '\\') {
            i += 1;
            if (i >= line.len) return error.InvalidRowsRequest;
            try appendPostgresTextEscapedByte(alloc, &field, line, &i);
            continue;
        }
        try field.append(alloc, c);
    }
    try appendPostgresTextFieldAlloc(alloc, &fields, &field, line[field_start..], marker);
    return try fields.toOwnedSlice(alloc);
}

fn appendPostgresTextFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(BulkSqlIoCsvField),
    field: *std.ArrayListUnmanaged(u8),
    raw_field: []const u8,
    null_marker: []const u8,
) !void {
    if (std.mem.eql(u8, raw_field, null_marker)) {
        field.deinit(alloc);
        field.* = .empty;
        try field.appendSlice(alloc, null_marker);
    }
    try appendCsvFieldAlloc(alloc, fields, field, false);
}

fn appendPostgresTextEscapedByte(
    alloc: std.mem.Allocator,
    field: *std.ArrayListUnmanaged(u8),
    line: []const u8,
    index: *usize,
) !void {
    const c = line[index.*];
    switch (c) {
        'b' => try field.append(alloc, '\x08'),
        'f' => try field.append(alloc, '\x0c'),
        'n' => try field.append(alloc, '\n'),
        'r' => try field.append(alloc, '\r'),
        't' => try field.append(alloc, '\t'),
        'v' => try field.append(alloc, '\x0b'),
        '0'...'7' => {
            var value: u16 = c - '0';
            var consumed: usize = 1;
            while (consumed < 3 and index.* + 1 < line.len and line[index.* + 1] >= '0' and line[index.* + 1] <= '7') : (consumed += 1) {
                index.* += 1;
                value = value * 8 + (line[index.*] - '0');
            }
            if (value > std.math.maxInt(u8)) return error.InvalidRowsRequest;
            try field.append(alloc, @intCast(value));
        },
        else => try field.append(alloc, c),
    }
}

fn appendCsvFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(BulkSqlIoCsvField),
    field: *std.ArrayListUnmanaged(u8),
    quoted: bool,
) !void {
    const value = try field.toOwnedSlice(alloc);
    errdefer alloc.free(value);
    try fields.append(alloc, .{
        .value = value,
        .quoted = quoted,
    });
    field.* = .empty;
}

fn freeCsvFields(alloc: std.mem.Allocator, fields: []const BulkSqlIoCsvField) void {
    for (fields) |field| alloc.free(field.value);
    if (fields.len > 0) alloc.free(fields);
}

fn rowJsonFromCsvFieldsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    fields: []const BulkSqlIoCsvField,
) ![]u8 {
    var row = std.ArrayList(u8).empty;
    errdefer row.deinit(alloc);
    try row.append(alloc, '{');
    var emitted: usize = 0;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (plan.columns, fields) |column_name, field| {
        if (seen.contains(column_name)) return error.InvalidRowsRequest;
        try seen.put(alloc, column_name, {});
        const column = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
        if (column.generated != null) return error.InvalidRowsRequest;
        const value_json = try csvValueJsonAlloc(alloc, column, field, plan);
        if (value_json == null) continue;
        defer alloc.free(value_json.?);
        if (emitted != 0) try row.append(alloc, ',');
        const key_json = try std.json.Stringify.valueAlloc(alloc, column_name, .{});
        defer alloc.free(key_json);
        try row.appendSlice(alloc, key_json);
        try row.append(alloc, ':');
        try row.appendSlice(alloc, value_json.?);
        emitted += 1;
    }
    try row.append(alloc, '}');
    return try row.toOwnedSlice(alloc);
}

fn csvValueJsonAlloc(
    alloc: std.mem.Allocator,
    column: runtime_schema.RelationalColumn,
    field: BulkSqlIoCsvField,
    plan: BulkSqlIoExecutionPlan,
) !?[]u8 {
    const raw_value = field.value;
    if (plan.default_marker) |marker| {
        if (std.mem.eql(u8, raw_value, marker)) return null;
    }
    const null_marker = nullMarker(plan);
    const forced_not_null = stringSliceContains(plan.force_not_null_columns, column.name);
    const forced_null = stringSliceContains(plan.force_null_columns, column.name);
    if (std.mem.eql(u8, raw_value, null_marker)) {
        if (field.quoted) {
            if (forced_null) return try alloc.dupe(u8, "null");
        } else if (!forced_not_null) {
            return try alloc.dupe(u8, "null");
        }
    }
    return switch (column.field_type) {
        .keyword, .text, .link, .blob, .html, .search_as_you_type, .datetime => try std.json.Stringify.valueAlloc(alloc, raw_value, .{}),
        .numeric => try numericJsonAlloc(alloc, raw_value),
        .boolean => try booleanJsonAlloc(alloc, raw_value),
        .json => try parsedJsonValueAlloc(alloc, raw_value, false),
        .array => try parsedJsonValueAlloc(alloc, raw_value, true),
        else => error.UnsupportedSqlShape,
    };
}

fn nullMarker(plan: BulkSqlIoExecutionPlan) []const u8 {
    return plan.null_marker orelse switch (plan.codec) {
        .csv => "",
        .postgres_text => "\\N",
        .postgres_binary => unreachable,
    };
}

fn numericJsonAlloc(alloc: std.mem.Allocator, raw_value: []const u8) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw_value, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    switch (parsed.value) {
        .integer, .float => return try alloc.dupe(u8, raw_value),
        else => return error.InvalidRowsRequest,
    }
}

fn booleanJsonAlloc(alloc: std.mem.Allocator, raw_value: []const u8) ![]u8 {
    if (std.ascii.eqlIgnoreCase(raw_value, "true") or std.mem.eql(u8, raw_value, "t") or std.mem.eql(u8, raw_value, "1")) {
        return try alloc.dupe(u8, "true");
    }
    if (std.ascii.eqlIgnoreCase(raw_value, "false") or std.mem.eql(u8, raw_value, "f") or std.mem.eql(u8, raw_value, "0")) {
        return try alloc.dupe(u8, "false");
    }
    return error.InvalidRowsRequest;
}

fn parsedJsonValueAlloc(alloc: std.mem.Allocator, raw_value: []const u8, require_array: bool) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw_value, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (require_array and parsed.value != .array) return error.InvalidRowsRequest;
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn rowMatchesWhereAlloc(
    alloc: std.mem.Allocator,
    row_json: []const u8,
    conditions: []const db_mod.types.RelationalRowsExpressionCondition,
) !bool {
    if (conditions.len == 0) return true;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    for (conditions) |condition| {
        if (!try expressionConditionMatchesAlloc(alloc, parsed.value.object, condition)) return false;
    }
    return true;
}

fn expressionConditionMatchesAlloc(
    alloc: std.mem.Allocator,
    row: std.json.ObjectMap,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) !bool {
    if (condition.lhs.kind != .field or condition.lhs.field_source != .row) return error.UnsupportedSqlShape;
    const lhs = row.get(condition.lhs.field) orelse std.json.Value.null;
    switch (condition.op) {
        .is_null => return lhs == .null,
        .is_not_null => return lhs != .null,
        else => {},
    }
    if (condition.rhs.len != 1 or condition.rhs[0].kind != .value) return error.UnsupportedSqlShape;
    var parsed_rhs = std.json.parseFromSlice(std.json.Value, alloc, condition.rhs[0].value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_rhs.deinit();
    const cmp = try compareJsonValues(alloc, lhs, parsed_rhs.value);
    return switch (condition.op) {
        .eq, .is_not_distinct => cmp == .eq,
        .ne, .is_distinct => cmp != .eq,
        .gt => cmp == .gt,
        .gte => cmp == .gt or cmp == .eq,
        .lt => cmp == .lt,
        .lte => cmp == .lt or cmp == .eq,
        .is_null, .is_not_null => unreachable,
    };
}

const ValueComparison = enum { lt, eq, gt };

fn compareJsonValues(alloc: std.mem.Allocator, lhs: std.json.Value, rhs: std.json.Value) !ValueComparison {
    if (jsonNumber(lhs)) |left_number| {
        if (jsonNumber(rhs)) |right_number| {
            if (left_number < right_number) return .lt;
            if (left_number > right_number) return .gt;
            return .eq;
        }
    }
    if (lhs == .string and rhs == .string) {
        const order = std.mem.order(u8, lhs.string, rhs.string);
        return switch (order) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    const lhs_json = try std.json.Stringify.valueAlloc(alloc, lhs, .{});
    defer alloc.free(lhs_json);
    const rhs_json = try std.json.Stringify.valueAlloc(alloc, rhs, .{});
    defer alloc.free(rhs_json);
    const order = std.mem.order(u8, lhs_json, rhs_json);
    return switch (order) {
        .lt => .lt,
        .eq => .eq,
        .gt => .gt,
    };
}

fn jsonNumber(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => null,
    };
}

const GeneratedColumnPolicy = enum {
    allow_generated,
    reject_generated,
};

fn validateColumnListForSchema(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
    generated_policy: GeneratedColumnPolicy,
) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (columns) |column_name| {
        if (seen.contains(column_name)) return error.InvalidRowsRequest;
        const column = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
        if (generated_policy == .reject_generated and column.generated != null) return error.InvalidRowsRequest;
        try seen.put(alloc, column_name, {});
    }
}

fn validateOptionColumns(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    copy_columns: []const []const u8,
    option_columns: []const []const u8,
) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (option_columns) |column_name| {
        if (seen.contains(column_name)) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema, column_name) orelse return error.InvalidRowsRequest;
        if (copy_columns.len != 0 and !stringSliceContains(copy_columns, column_name)) return error.InvalidRowsRequest;
        try seen.put(alloc, column_name, {});
    }
}

fn validateExpressionConditionForSchema(
    schema: runtime_schema.TableSchema,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) error{InvalidRowsRequest}!void {
    try validateExpressionForSchema(schema, condition.lhs);
    for (condition.rhs) |expression| try validateExpressionForSchema(schema, expression);
}

fn validateExpressionForSchema(
    schema: runtime_schema.TableSchema,
    expression: db_mod.types.RelationalRowsExpression,
) error{InvalidRowsRequest}!void {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidRowsRequest;
        _ = findRelationalColumn(schema, expression.field) orelse return error.InvalidRowsRequest;
    }
    for (expression.operands) |operand| try validateExpressionForSchema(schema, operand);
    for (expression.case_branches) |branch| {
        try validateExpressionConditionForSchema(schema, branch.when);
        try validateExpressionForSchema(schema, branch.then);
    }
    for (expression.case_else) |fallback| try validateExpressionForSchema(schema, fallback);
}

fn findRelationalColumn(schema: runtime_schema.TableSchema, column_name: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, column_name)) return column;
    }
    return null;
}

fn stringSliceContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try alloc.dupe(u8, out.written());
}

fn singleByteOption(option: ?[]const u8, default: u8) !u8 {
    if (option) |value| {
        if (value.len == 1) return value[0];
        return error.UnsupportedSqlShape;
    }
    return default;
}

test "sql adapter bulk io lowers COPY execution routes" {
    const alloc = std.testing.allocator;

    const columns = [_][]const u8{ "id", "status" };
    const where_rhs = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .value, .value_json = "\"active\"" },
    };
    const where_expressions = [_]db_mod.types.RelationalRowsExpressionCondition{
        .{
            .lhs = .{ .kind = .field, .field = "status" },
            .op = .eq,
            .rhs = where_rhs[0..],
        },
    };

    const copy_from_plan = ddl_plan.BulkIoPlan{
        .direction = .from,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint = "STDIN",
        .format = "csv",
        .where_expressions = where_expressions[0..],
    };
    const copy_from = try executionPlanFromDdlPlan(copy_from_plan);
    try std.testing.expectEqual(BulkSqlIoOperation.import_rows, copy_from.operation);
    try std.testing.expectEqual(BulkSqlIoNativeRoute.rows_batch, copy_from.native_route);
    try std.testing.expectEqual(BulkSqlIoStream.stdin, copy_from.stream);
    try std.testing.expectEqual(BulkSqlIoCodec.csv, copy_from.codec);
    try std.testing.expectEqual(ddl_plan.BulkIoEndpointKind.stream, copy_from.endpoint_kind);
    try std.testing.expectEqual(usermgr.PermissionType.write, copy_from.required_permission);
    try std.testing.expectEqual(BulkSqlIoAuditAction.copy_from, copy_from.audit_action);
    try std.testing.expect(copy_from.requires_external_stream);
    const copy_from_fingerprint = try executionFingerprintAlloc(alloc, copy_from);
    defer alloc.free(copy_from_fingerprint);
    try std.testing.expectEqualStrings("bulk_sql_io:op=import_rows:native=rows_batch:stream=stdin:codec=csv:auth=table/write:audit=copy_from:table=usage_records:columns=2:where_expr=1:requires_stream=true", copy_from_fingerprint);

    const copy_from_text = try executionPlanFromDdlPlan(.{
        .direction = .from,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint = "STDIN",
    });
    try std.testing.expectEqual(BulkSqlIoCodec.postgres_text, copy_from_text.codec);

    const copy_to = try executionPlanFromDdlPlan(.{
        .direction = .to,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint = "STDOUT",
        .format = "csv",
        .force_quote_all = true,
    });
    try std.testing.expectEqual(BulkSqlIoOperation.export_rows, copy_to.operation);
    try std.testing.expectEqual(BulkSqlIoNativeRoute.rows_query, copy_to.native_route);
    try std.testing.expectEqual(BulkSqlIoStream.stdout, copy_to.stream);
    try std.testing.expectEqual(usermgr.PermissionType.read, copy_to.required_permission);
    try std.testing.expectEqual(BulkSqlIoAuditAction.copy_to, copy_to.audit_action);
    const copy_to_fingerprint = try executionFingerprintAlloc(alloc, copy_to);
    defer alloc.free(copy_to_fingerprint);
    try std.testing.expectEqualStrings("bulk_sql_io:op=export_rows:native=rows_query:stream=stdout:codec=csv:auth=table/read:audit=copy_to:table=usage_records:columns=2:where_expr=0:requires_stream=true", copy_to_fingerprint);

    const copy_from_file = try executionPlanFromDdlPlan(.{
        .direction = .from,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint_kind = .file,
        .endpoint = "/tmp/usage.csv",
        .format = "csv",
    });
    try std.testing.expectEqual(BulkSqlIoStream.file, copy_from_file.stream);
    try std.testing.expect(!copy_from_file.requires_external_stream);
    const copy_from_file_fingerprint = try executionFingerprintAlloc(alloc, copy_from_file);
    defer alloc.free(copy_from_file_fingerprint);
    try std.testing.expectEqualStrings("bulk_sql_io:op=import_rows:native=rows_batch:stream=file:codec=csv:endpoint_kind=file:endpoint=/tmp/usage.csv:auth=table/write:audit=copy_from:table=usage_records:columns=2:where_expr=0:requires_stream=false", copy_from_file_fingerprint);

    const copy_from_program = try executionPlanFromDdlPlan(.{
        .direction = .from,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint_kind = .program,
        .endpoint = "cat /tmp/usage.csv",
    });
    try std.testing.expectEqual(BulkSqlIoStream.program, copy_from_program.stream);
    const copy_from_program_fingerprint = try executionFingerprintAlloc(alloc, copy_from_program);
    defer alloc.free(copy_from_program_fingerprint);
    try std.testing.expectEqualStrings("bulk_sql_io:op=import_rows:native=rows_batch:stream=program:codec=postgres_text:endpoint_kind=program:endpoint=cat /tmp/usage.csv:auth=table/write:audit=copy_from:table=usage_records:columns=2:where_expr=0:requires_stream=false", copy_from_program_fingerprint);

    try std.testing.expectError(error.UnsupportedSqlShape, executionPlanFromDdlPlan(.{
        .direction = .from,
        .table_name = "usage_records",
        .columns = columns[0..],
        .endpoint = "STDOUT",
    }));
}

test "sql adapter bulk io imports COPY rows into row batches" {
    const alloc = std.testing.allocator;

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .text, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .text, .nullable = true },
        .{ .name = "status_key", .path = "status_key", .field_type = .text, .nullable = true, .generated = .{ .op = .lower, .field = "status" } },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = true },
        .{ .name = "active", .path = "active", .field_type = .boolean, .nullable = true },
        .{ .name = "metadata", .path = "metadata", .field_type = .json, .nullable = true },
    };
    const primary_columns = [_][]const u8{"id"};
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .primary_key = .{ .columns = primary_columns[0..] },
    };
    const import_columns = [_][]const u8{ "id", "status", "amount", "active", "metadata" };
    const base_csv_plan = BulkSqlIoExecutionPlan{
        .operation = .import_rows,
        .native_route = .rows_batch,
        .stream = .stdin,
        .codec = .csv,
        .endpoint_kind = .stream,
        .endpoint = "STDIN",
        .table_name = "usage_records",
        .columns = import_columns[0..],
        .required_permission = .write,
        .audit_action = .copy_from,
        .header = true,
    };
    var csv_batch = try importRowsBatchFromStdinAlloc(
        alloc,
        schema,
        base_csv_plan,
        "id,status,amount,active,metadata\nu1,Ready,42,true,{\"source\":\"copy\"}\n\\.\n",
    );
    defer csv_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), csv_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"Ready\",\"amount\":42,\"active\":true,\"metadata\":{\"source\":\"copy\"},\"status_key\":\"ready\"}", csv_batch.writes[0].value);

    const text_rhs_json = try jsonStringifyAlloc(alloc, "line\nbreak");
    defer alloc.free(text_rhs_json);
    const text_rhs = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .value, .value_json = text_rhs_json },
    };
    const text_where = [_]db_mod.types.RelationalRowsExpressionCondition{
        .{
            .lhs = .{ .kind = .field, .field = "status" },
            .op = .eq,
            .rhs = text_rhs[0..],
        },
    };
    var text_plan = base_csv_plan;
    text_plan.codec = .postgres_text;
    text_plan.header = false;
    text_plan.default_marker = "DEFAULT";
    text_plan.where_expressions = text_where[0..];
    var text_batch = try importRowsBatchFromStdinAlloc(
        alloc,
        schema,
        text_plan,
        "u_text_1\tline\\nbreak\t7\ttrue\t{\"source\":\"copy_text\"}\nu_text_2\tclosed\tDEFAULT\tfalse\t\\N\n\\.\n",
    );
    defer text_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), text_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"u_text_1\",\"status\":\"line\\nbreak\",\"amount\":7,\"active\":true,\"metadata\":{\"source\":\"copy_text\"},\"status_key\":\"line\\nbreak\"}", text_batch.writes[0].value);

    const nullable_columns = [_][]const u8{ "id", "status", "amount" };
    var null_plan = base_csv_plan;
    null_plan.columns = nullable_columns[0..];
    null_plan.header = false;
    null_plan.null_marker = "";
    var null_batch = try importRowsBatchFromStdinAlloc(alloc, schema, null_plan, "u_empty_unquoted,,0\nu_empty_quoted,\"\",0\n");
    defer null_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), null_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"u_empty_unquoted\",\"status\":null,\"amount\":0,\"status_key\":null}", null_batch.writes[0].value);
    try std.testing.expectEqualStrings("{\"id\":\"u_empty_quoted\",\"status\":\"\",\"amount\":0,\"status_key\":\"\"}", null_batch.writes[1].value);

    const status_option_columns = [_][]const u8{"status"};
    var force_null_plan = null_plan;
    force_null_plan.force_null_columns = status_option_columns[0..];
    var force_null_batch = try importRowsBatchFromStdinAlloc(alloc, schema, force_null_plan, "u_force_null,\"\",0\n");
    defer force_null_batch.deinit(alloc);
    try std.testing.expectEqualStrings("{\"id\":\"u_force_null\",\"status\":null,\"amount\":0,\"status_key\":null}", force_null_batch.writes[0].value);

    var force_not_null_plan = null_plan;
    force_not_null_plan.force_not_null_columns = status_option_columns[0..];
    var force_not_null_batch = try importRowsBatchFromStdinAlloc(alloc, schema, force_not_null_plan, "u_force_not_null,,0\n");
    defer force_not_null_batch.deinit(alloc);
    try std.testing.expectEqualStrings("{\"id\":\"u_force_not_null\",\"status\":\"\",\"amount\":0,\"status_key\":\"\"}", force_not_null_batch.writes[0].value);

    var malformed_plan = base_csv_plan;
    malformed_plan.delimiter = "::";
    try std.testing.expectError(error.UnsupportedSqlShape, importRowsBatchFromStdinAlloc(alloc, schema, malformed_plan, "u_bad:ready\n"));

    var invalid_options = base_csv_plan;
    invalid_options.delimiter = "|";
    invalid_options.quote = "|";
    try std.testing.expectError(error.UnsupportedSqlShape, importRowsBatchFromStdinAlloc(alloc, schema, invalid_options, "u_bad|ready|0\n"));

    const duplicate_columns = [_][]const u8{ "id", "id" };
    var duplicate_plan = base_csv_plan;
    duplicate_plan.columns = duplicate_columns[0..];
    duplicate_plan.header = false;
    try std.testing.expectError(error.InvalidRowsRequest, importRowsBatchFromStdinAlloc(alloc, schema, duplicate_plan, "u_bad,u_bad_again\n"));

    const generated_columns = [_][]const u8{ "id", "status_key" };
    var generated_plan = base_csv_plan;
    generated_plan.columns = generated_columns[0..];
    generated_plan.header = false;
    try std.testing.expectError(error.InvalidRowsRequest, importRowsBatchFromStdinAlloc(alloc, schema, generated_plan, "u5,ready\n"));
}

test "sql adapter bulk io materializes sequence defaults through explicit resolver" {
    const alloc = std.testing.allocator;

    const columns = [_]runtime_schema.RelationalColumn{
        .{
            .name = "id",
            .path = "id",
            .field_type = .numeric,
            .nullable = false,
            .default_value = .{ .kind = .sequence_next, .value_json = "{\"sequence\":\"usage_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}" },
        },
        .{ .name = "status", .path = "status", .field_type = .text, .nullable = false },
    };
    const primary_columns = [_][]const u8{"id"};
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .primary_key = .{ .columns = primary_columns[0..] },
    };
    const import_columns = [_][]const u8{"status"};
    const plan = BulkSqlIoExecutionPlan{
        .operation = .import_rows,
        .native_route = .rows_batch,
        .stream = .stdin,
        .codec = .csv,
        .endpoint_kind = .stream,
        .endpoint = "STDIN",
        .table_name = "usage_records",
        .columns = import_columns[0..],
        .required_permission = .write,
        .audit_action = .copy_from,
    };

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        importRowsBatchFromStdinAlloc(alloc, schema, plan, "pending\n"),
    );

    const Resolver = struct {
        calls: usize = 0,

        fn iface(self: *@This()) relational_rows.SequenceDefaultResolver {
            return .{
                .ptr = self,
                .next_value_json_alloc = nextValueJsonAlloc,
            };
        }

        fn nextValueJsonAlloc(ptr: *anyopaque, inner_alloc: std.mem.Allocator, request: relational_rows.SequenceDefaultRequest) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_id_seq", request.sequence);
            try std.testing.expectEqualStrings("tenant", request.database);
            try std.testing.expectEqualStrings("billing", request.schema);
            self.calls += 1;
            return try std.fmt.allocPrint(inner_alloc, "{d}", .{@as(i64, 700) + @as(i64, @intCast(self.calls))});
        }
    };

    var resolver = Resolver{};
    var batch = try importRowsBatchFromStdinWithDefaultContextAlloc(
        alloc,
        schema,
        plan,
        "pending\nclosed\n",
        .{ .sequence_resolver = resolver.iface() },
    );
    defer batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
    try std.testing.expectEqual(@as(usize, 2), batch.writes.len);
    try std.testing.expectEqualStrings("{\"status\":\"pending\",\"id\":701}", batch.writes[0].value);
    try std.testing.expectEqualStrings("{\"status\":\"closed\",\"id\":702}", batch.writes[1].value);
}

test "sql adapter bulk io imports and exports COPY text csv and binary codecs" {
    const alloc = std.testing.allocator;

    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .text, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .text, .nullable = true },
        .{ .name = "note", .path = "note", .field_type = .text, .nullable = true },
    };
    const plan_columns = [_][]const u8{ "id", "status", "note" };
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .primary_key = .{ .columns = plan_columns[0..1] },
    };

    const csv_plan = BulkSqlIoExecutionPlan{
        .operation = .import_rows,
        .native_route = .rows_batch,
        .stream = .stdin,
        .codec = .csv,
        .endpoint_kind = .stream,
        .endpoint = "STDIN",
        .table_name = "usage_records",
        .columns = plan_columns[0..],
        .required_permission = .write,
        .audit_action = .copy_from,
    };
    var csv_batch = try importRowsBatchFromStdinAlloc(alloc, schema, csv_plan, "r1,,\"\"\n");
    defer csv_batch.deinit(alloc);
    try std.testing.expectEqualStrings("{\"id\":\"r1\",\"status\":null,\"note\":\"\"}", csv_batch.writes[0].value);

    const forced_columns = [_][]const u8{ "status", "note" };
    var forced_csv_plan = csv_plan;
    forced_csv_plan.force_not_null_columns = forced_columns[0..];
    forced_csv_plan.force_null_columns = forced_columns[0..];
    var forced_csv_batch = try importRowsBatchFromStdinAlloc(alloc, schema, forced_csv_plan, "r2,,\"\"\n");
    defer forced_csv_batch.deinit(alloc);
    try std.testing.expectEqualStrings("{\"id\":\"r2\",\"status\":\"\",\"note\":null}", forced_csv_batch.writes[0].value);

    var text_plan = csv_plan;
    text_plan.codec = .postgres_text;
    var text_batch = try importRowsBatchFromStdinAlloc(alloc, schema, text_plan, "r3\t\\N\tliteral\\\\N\nr4\tline\\nbreak\tplain\\ttab\n");
    defer text_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), text_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"r3\",\"status\":null,\"note\":\"literal\\\\N\"}", text_batch.writes[0].value);
    try std.testing.expectEqualStrings("{\"id\":\"r4\",\"status\":\"line\\nbreak\",\"note\":\"plain\\ttab\"}", text_batch.writes[1].value);

    var text_export_rows = [_][]const u8{
        "{\"id\":\"r3\",\"status\":null,\"note\":\"literal\\\\N\"}",
        "{\"id\":\"r4\",\"status\":\"line\\nbreak\",\"note\":\"plain\\ttab\"}",
    };
    const text_export_plan = BulkSqlIoExecutionPlan{
        .operation = .export_rows,
        .native_route = .rows_query,
        .stream = .stdout,
        .codec = .postgres_text,
        .endpoint_kind = .stream,
        .endpoint = "STDOUT",
        .table_name = "usage_records",
        .columns = plan_columns[0..],
        .required_permission = .read,
        .audit_action = .copy_to,
    };
    const text_export = try exportRowsCsvToStdoutAlloc(alloc, schema, text_export_plan, .{
        .rows = text_export_rows[0..],
        .total = 2,
    });
    defer alloc.free(text_export);
    try std.testing.expectEqualStrings("r3\t\\N\tliteral\\\\N\nr4\tline\\nbreak\tplain\\ttab\n", text_export);

    const BinaryCopy = struct {
        fn appendI16(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: i16) !void {
            var buf: [2]u8 = undefined;
            std.mem.writeInt(i16, &buf, value, .big);
            try out.appendSlice(allocator, &buf);
        }

        fn appendI32(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: i32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, value, .big);
            try out.appendSlice(allocator, &buf);
        }

        fn appendU32(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: u32) !void {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, value, .big);
            try out.appendSlice(allocator, &buf);
        }

        fn appendField(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: ?[]const u8) !void {
            const bytes = value orelse {
                try appendI32(allocator, out, -1);
                return;
            };
            try appendI32(allocator, out, @intCast(bytes.len));
            try out.appendSlice(allocator, bytes);
        }
    };
    var binary_payload = std.ArrayList(u8).empty;
    defer binary_payload.deinit(alloc);
    try binary_payload.appendSlice(alloc, postgres_binary_copy_signature);
    try BinaryCopy.appendU32(alloc, &binary_payload, 0);
    try BinaryCopy.appendU32(alloc, &binary_payload, 0);
    try BinaryCopy.appendI16(alloc, &binary_payload, 3);
    try BinaryCopy.appendField(alloc, &binary_payload, "b1");
    try BinaryCopy.appendField(alloc, &binary_payload, "ready");
    try BinaryCopy.appendField(alloc, &binary_payload, null);
    try BinaryCopy.appendI16(alloc, &binary_payload, 3);
    try BinaryCopy.appendField(alloc, &binary_payload, "b2");
    try BinaryCopy.appendField(alloc, &binary_payload, "done");
    try BinaryCopy.appendField(alloc, &binary_payload, "memo");
    try BinaryCopy.appendI16(alloc, &binary_payload, -1);

    var binary_import_plan = csv_plan;
    binary_import_plan.codec = .postgres_binary;
    var binary_batch = try importRowsBatchFromStdinAlloc(alloc, schema, binary_import_plan, binary_payload.items);
    defer binary_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), binary_batch.writes.len);
    try std.testing.expectEqualStrings("{\"id\":\"b1\",\"status\":\"ready\",\"note\":null}", binary_batch.writes[0].value);
    try std.testing.expectEqualStrings("{\"id\":\"b2\",\"status\":\"done\",\"note\":\"memo\"}", binary_batch.writes[1].value);

    const binary_export_plan = BulkSqlIoExecutionPlan{
        .operation = .export_rows,
        .native_route = .rows_query,
        .stream = .stdout,
        .codec = .postgres_binary,
        .endpoint_kind = .stream,
        .endpoint = "STDOUT",
        .table_name = "usage_records",
        .columns = plan_columns[0..],
        .required_permission = .read,
        .audit_action = .copy_to,
    };
    var binary_export_rows = [_][]const u8{
        binary_batch.writes[0].value,
        binary_batch.writes[1].value,
    };
    const binary_export = try exportRowsCsvToStdoutAlloc(alloc, schema, binary_export_plan, .{
        .rows = binary_export_rows[0..],
        .total = 2,
    });
    defer alloc.free(binary_export);
    try std.testing.expect(binary_export.len > postgres_binary_copy_signature.len);
    try std.testing.expectEqualSlices(u8, postgres_binary_copy_signature, binary_export[0..postgres_binary_copy_signature.len]);
    var binary_roundtrip_batch = try importRowsBatchFromStdinAlloc(alloc, schema, binary_import_plan, binary_export);
    defer binary_roundtrip_batch.deinit(alloc);
    try std.testing.expectEqualStrings(binary_batch.writes[0].value, binary_roundtrip_batch.writes[0].value);
    try std.testing.expectEqualStrings(binary_batch.writes[1].value, binary_roundtrip_batch.writes[1].value);
}
