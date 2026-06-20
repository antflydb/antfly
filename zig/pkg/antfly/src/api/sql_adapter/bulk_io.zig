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
const ddl_plan = @import("ddl_plan.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");
const usermgr = @import("../../usermgr/mod.zig");

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

fn validateCodecOptions(plan: BulkSqlIoExecutionPlan) !void {
    switch (plan.codec) {
        .csv => try validateCsvOptions(plan),
        .postgres_text => try validatePostgresTextOptions(plan),
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
    for (plan.where_expressions) |condition| try validateExpressionConditionForSchema(schema, condition);
}

pub fn importRowsBatchFromStdinAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    plan: BulkSqlIoExecutionPlan,
    stdin_payload: []const u8,
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
    return try relational_rows.parseRowsBatchRequest(alloc, body.items, schema);
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

    const delimiter = try singleByteOption(plan.delimiter, switch (plan.codec) {
        .csv => ',',
        .postgres_text => '\t',
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
            const field = try fieldFromJsonValueAlloc(alloc, value, plan);
            defer alloc.free(field);
            switch (plan.codec) {
                .csv => {
                    const force_quote = plan.force_quote_all or stringSliceContains(plan.force_quote_columns, column_name);
                    try appendCsvField(writer, field, delimiter, quote, escape, force_quote);
                },
                .postgres_text => try appendPostgresTextField(writer, field, delimiter),
            }
        }
        try writer.writeByte('\n');
    }

    return try out.toOwnedSlice();
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

fn singleByteOption(option: ?[]const u8, default: u8) !u8 {
    if (option) |value| {
        if (value.len == 1) return value[0];
        return error.UnsupportedSqlShape;
    }
    return default;
}
