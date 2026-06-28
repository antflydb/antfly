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
const antfly = @import("antfly-zig");
const cli = @import("cli/mod.zig");

const Allocator = std.mem.Allocator;
const LiteDb = antfly.lite.connection.Connection;
const catalog_resources = antfly.public_api.catalog_resources;
const metadata_api = antfly.metadata_api;
const metadata_table_manager = antfly.metadata.table_manager;
const metadata_transition_state = antfly.metadata.transition_state;
const raft_mod = antfly.raft;
const raft_reconciler = antfly.raft.reconciler;
const relational_rows = antfly.public_api.relational_rows;
const sql_adapter = antfly.public_api.sql_adapter;
const sql_adapter_runtime = antfly.public_api.sql_adapter_runtime;
const storage_schema = antfly.schema;
const table_catalog = antfly.public_api.table_catalog;
const table_reads = antfly.public_api.table_reads;
const table_writes = antfly.public_api.table_writes;
const tables_api = antfly.public_api.tables;

pub const max_sql_file_bytes = 64 * 1024 * 1024;
pub const max_repl_statement_bytes = 16 * 1024 * 1024;

pub const Session = struct {
    catalog: sql_adapter.OwnedSqlCatalogSession,

    pub fn init(alloc: Allocator, flags: cli.CatalogFlags) !Session {
        var catalog = try sql_adapter.OwnedSqlCatalogSession.fromSessionAlloc(alloc, catalog_resources.SqlCatalogSession.default());
        errdefer catalog.deinit(alloc);
        if (flags.database) |database| {
            try validateCatalogIdentifier(database);
            alloc.free(catalog.current_database_name);
            catalog.current_database_name = try alloc.dupe(u8, database);
        }
        if (flags.namespace) |namespace| {
            try validateCatalogIdentifier(namespace);
            for (catalog.search_path) |name| alloc.free(@constCast(name));
            if (catalog.search_path.len > 0) alloc.free(catalog.search_path);
            const search_path = try alloc.alloc([]const u8, 1);
            errdefer alloc.free(search_path);
            search_path[0] = try alloc.dupe(u8, namespace);
            catalog.search_path = search_path;
        }
        catalog.notification_session_id = 1;
        return .{ .catalog = catalog };
    }

    pub fn deinit(self: *Session, alloc: Allocator) void {
        self.catalog.deinit(alloc);
        self.* = undefined;
    }

    pub fn sessionId(self: *Session) u64 {
        if (self.catalog.notification_session_id == 0) self.catalog.notification_session_id = 1;
        return self.catalog.notification_session_id;
    }
};

pub fn runFromArgs(allocator: Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const path = args.next() orelse cli.fatal("database path is required", .{});
    try requireAflitePath(path);

    var command: ?[]const u8 = null;
    var file_path: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            command = args.next() orelse cli.fatal("{s} requires a SQL statement", .{arg});
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--file")) {
            file_path = args.next() orelse cli.fatal("{s} requires a path", .{arg});
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else {
            cli.fatal("unknown lite sql option: {s}", .{arg});
        }
    }

    if (command != null and file_path != null) {
        cli.fatal("use only one of -c/--command or -f/--file", .{});
    }

    var session = try Session.init(allocator, catalog);
    defer session.deinit(allocator);

    if (command) |sql| {
        if (!try executeSqlText(allocator, io, path, &session, sql, true)) return error.SqlCommandFailed;
        return;
    }

    if (file_path) |sql_path| {
        const sql = cli.readFileAlloc(io, allocator, sql_path, max_sql_file_bytes) catch |err| {
            cli.fatal("reading SQL file {s}: {}", .{ sql_path, err });
        };
        defer allocator.free(sql);
        if (!try executeSqlText(allocator, io, path, &session, sql, true)) return error.SqlCommandFailed;
        return;
    }

    return repl(allocator, io, path, &session);
}

pub fn executeSqlText(
    allocator: Allocator,
    io: std.Io,
    path: []const u8,
    session: *Session,
    sql_text: []const u8,
    execute_trailing: bool,
) !bool {
    var ok = true;
    var rest = sql_text;
    while (true) {
        if (firstStatementEnd(rest)) |end| {
            const statement = std.mem.trim(u8, rest[0..end], " \t\r\n");
            if (statement.len != 0 and !try executeOne(allocator, io, path, session, statement)) ok = false;
            rest = rest[end + 1 ..];
            continue;
        }

        const trailing = std.mem.trim(u8, rest, " \t\r\n");
        if (execute_trailing and trailing.len != 0) {
            if (!try executeOne(allocator, io, path, session, trailing)) ok = false;
        }
        return ok;
    }
}

pub fn repl(allocator: Allocator, io: std.Io, path: []const u8, session: *Session) !void {
    var stdin_buf: [8192]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buf);

    var statement = std.ArrayListUnmanaged(u8).empty;
    defer statement.deinit(allocator);

    while (true) {
        cli.writeStdout(io, if (statement.items.len == 0) "antfly-lite=> " else "antfly-lite-> ");
        const line_raw = (try stdin_reader.interface.takeDelimiter('\n')) orelse break;
        const line = std.mem.trim(u8, line_raw, "\r\n");
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (statement.items.len == 0 and (std.mem.eql(u8, trimmed, "\\q") or std.mem.eql(u8, trimmed, ".quit"))) break;
        if (trimmed.len == 0 and statement.items.len == 0) continue;

        if (statement.items.len + line.len + 1 > max_repl_statement_bytes) {
            statement.clearRetainingCapacity();
            std.debug.print("statement too large\n", .{});
            continue;
        }
        try statement.appendSlice(allocator, line);
        try statement.append(allocator, '\n');

        if (firstStatementEnd(statement.items) == null) continue;
        _ = try executeSqlText(allocator, io, path, session, statement.items, false);
        statement.clearRetainingCapacity();
    }

    const trailing = std.mem.trim(u8, statement.items, " \t\r\n");
    if (trailing.len != 0) {
        std.debug.print("discarding incomplete SQL statement\n", .{});
    }
}

fn executeOne(allocator: Allocator, io: std.Io, path: []const u8, session: *Session, sql: []const u8) !bool {
    var parsed_sql = sql_adapter.ParsedSql.initAlloc(allocator, sql) catch |err| {
        std.debug.print("SQL error: {}\n", .{err});
        return false;
    };
    defer parsed_sql.deinit(allocator);

    const open_mode: antfly.db.OpenMode = switch (parsed_sql.statement) {
        .read => .query_readonly,
        else => .writer,
    };
    var lite = try LiteDb.open(allocator, path, open_mode);
    defer lite.close();

    const body = executeParsedSqlJsonAlloc(allocator, &lite.db, session, &parsed_sql) catch |err| {
        std.debug.print("SQL error: {}\n", .{err});
        return false;
    };
    defer allocator.free(body);
    writeJsonLine(io, body);
    return true;
}

pub fn executeOneJsonAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, sql: []const u8) ![]u8 {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(allocator, sql);
    defer parsed_sql.deinit(allocator);
    return try executeParsedSqlJsonAlloc(allocator, db, session, &parsed_sql);
}

fn executeParsedSqlJsonAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    switch (parsed_sql.statement) {
        .read => return try executeReadAlloc(allocator, db, session, parsed_sql),
        .write => return try executeWriteAlloc(allocator, db, session, parsed_sql),
        else => return try executeNonRowAlloc(allocator, db, session, parsed_sql),
    }
}

fn executeNonRowAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    var catalog = try LiteSingleTableCatalog.fromStoredTableAlloc(allocator, db);
    defer catalog.deinit(allocator);

    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog.iface(),
        .session = session.catalog.session(),
    });
    defer logical_plan.deinit(allocator);

    return switch (logical_plan) {
        .table_ddl => try executeTableDdlLogicalPlanAlloc(allocator, db, session, &logical_plan.table_ddl),
        .session => try executeSessionLogicalPlanAlloc(allocator, session, logical_plan.session),
        .other_ddl => try executeOtherDdlLogicalPlanAlloc(allocator, session, logical_plan.other_ddl),
        else => error.UnsupportedSqlShape,
    };
}

fn executeTableDdlLogicalPlanAlloc(
    allocator: Allocator,
    db: *antfly.db.DB,
    session: *Session,
    table_plan: *sql_adapter.TableDdlLogicalPlan,
) ![]u8 {
    const table_name = try ddlTargetTableNameAlloc(allocator, table_plan.*, session.catalog.session());
    defer allocator.free(table_name);

    const existing_table = try loadLiteSqlTableRecordForTargetAlloc(allocator, db, table_name, session.catalog.session());
    defer if (existing_table) |table| metadata_table_manager.freeTable(allocator, table);
    const base_table = if (existing_table) |table| table else metadata_table_manager.TableRecord{
        .table_id = 1,
        .name = table_name,
        .database_name = session.catalog.session().currentDatabase(),
        .namespace_name = session.catalog.session().primarySearchPathNamespace(),
        .placement_role = "data",
        .desired_replica_count = 1,
    };

    var applied = try tables_api.applyTableDdlPlanToTableRecordWithSessionAlloc(
        allocator,
        &base_table,
        table_plan,
        session.catalog.session(),
    );
    defer applied.deinit(allocator);

    try db.applyLiteSqlTableRecord(allocator, applied.table);

    const response = NonRowResponse{
        .kind = "ddl",
        .statement_kind = "table_ddl",
        .session_id = session.sessionId(),
        .noop = applied.noop,
        .applied = applied,
    };
    return try std.json.Stringify.valueAlloc(allocator, response, .{ .whitespace = .indent_2 });
}

fn executeSessionLogicalPlanAlloc(
    allocator: Allocator,
    session: *Session,
    plan: sql_adapter.SessionCatalogPlan,
) ![]u8 {
    const notification_session_id = session.catalog.notification_session_id;
    var old = session.catalog;
    var updated = try sql_adapter.applyOwnedSessionCatalogPlanAlloc(allocator, old, plan);
    errdefer updated.deinit(allocator);
    updated.notification_session_id = notification_session_id;
    old.deinit(allocator);
    session.catalog = updated;

    var applied = try tables_api.emptyAppliedRelationalSqlDdlRecordAlloc(allocator);
    defer applied.deinit(allocator);
    applied.noop = true;

    const response = NonRowResponse{
        .kind = "session",
        .statement_kind = "session",
        .session_id = session.sessionId(),
        .noop = true,
        .applied = applied,
    };
    return try std.json.Stringify.valueAlloc(allocator, response, .{ .whitespace = .indent_2 });
}

fn executeOtherDdlLogicalPlanAlloc(
    allocator: Allocator,
    session: *Session,
    plan: sql_adapter.OtherDdlLogicalPlan,
) ![]u8 {
    switch (plan) {
        .adapter_noop => {},
        .moved => return error.UnsupportedSqlShape,
    }
    var applied = try tables_api.emptyAppliedRelationalSqlDdlRecordAlloc(allocator);
    defer applied.deinit(allocator);
    applied.noop = true;

    const response = NonRowResponse{
        .kind = "ddl",
        .statement_kind = "other_ddl",
        .session_id = session.sessionId(),
        .noop = true,
        .applied = applied,
    };
    return try std.json.Stringify.valueAlloc(allocator, response, .{ .whitespace = .indent_2 });
}

fn executeWriteAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    const target_table = try sql_adapter.writeTargetTableNameFromParsedSqlAlloc(allocator, parsed_sql);
    defer allocator.free(target_table);

    const table_record = (try loadLiteSqlTableRecordForTargetAlloc(allocator, db, target_table, session.catalog.session())) orelse return error.InvalidSqlCatalog;
    defer metadata_table_manager.freeTable(allocator, table_record);
    var catalog = LiteSingleTableCatalog.initBorrowed(table_record);
    const catalog_source = catalog.iface();

    var unique_resolver_ctx = DbUniqueSelectorResolverContext{ .db = db };
    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog_source,
        .session = session.catalog.session(),
        .write_options = .{
            .unique_resolver = unique_resolver_ctx.resolver(),
            .sync_level = try sql_adapter.sqlSyncLevelFromSession(session.catalog.session()),
        },
    });
    defer logical_plan.deinit(allocator);
    switch (logical_plan) {
        .catalog_write => {},
        else => return error.UnsupportedSqlShape,
    }

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, target_table, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var write_source = table_writes.BoundTableWriteSource.init(target_table, db);
    var lowered = try sql_adapter_runtime.lowerWritePlanWithLogicalPlanAndFunctionBindingsAlloc(
        allocator,
        parsed_sql,
        &logical_plan,
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(allocator);

    const rows_batch = switch (lowered) {
        .insert => |*insert| &insert.batch,
        .update => |*update| &update.batch,
        .delete => |*delete| &delete.batch,
        else => return error.UnsupportedSqlShape,
    };
    if (rows_batch.writes.len != 0 or rows_batch.deletes.len != 0 or rows_batch.transforms.len != 0 or rows_batch.predicates.len != 0) {
        _ = (try write_source.source().batch(allocator, target_table, rows_batch.req)) orelse return error.TableNotFound;
    }

    return try encodeRowsBatchResultAlloc(allocator, session.sessionId(), try liteStatementKindForParsedLogicalPlan(logical_plan, parsed_sql), rows_batch.*);
}

fn executeReadAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    var table_names = (try sql_adapter.readSourceTableNamesFromParsedSqlAlloc(allocator, parsed_sql)) orelse return error.UnsupportedSqlShape;
    defer table_names.deinit(allocator);

    const table_record = (try loadLiteSqlTableRecordForTargetAlloc(allocator, db, table_names.left, session.catalog.session())) orelse return error.InvalidSqlCatalog;
    defer metadata_table_manager.freeTable(allocator, table_record);
    var catalog = LiteSingleTableCatalog.initBorrowed(table_record);
    const catalog_source = catalog.iface();

    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog_source,
        .session = session.catalog.session(),
    });
    defer logical_plan.deinit(allocator);
    const statement_kind = switch (logical_plan) {
        .catalog_read => |catalog_read| try liteReadStatementKindForParsedCatalogRead(&catalog_read, parsed_sql),
        else => return error.UnsupportedSqlShape,
    };
    if (try executeAntflyQueryFunctionReadAlloc(allocator, db, session, parsed_sql, statement_kind)) |body| return body;

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, table_names.left, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var lowered = try sql_adapter_runtime.lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc(
        allocator,
        parsed_sql,
        &logical_plan,
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(allocator);

    var read_source = table_reads.BoundTableReadSource.init(table_names.left, 1, db, raft_mod.read_gate.noopReadableLeaseRequester());
    var result = (try table_reads.executeLoweredSqlReadPlanWithSessionAlloc(
        allocator,
        read_source.source(),
        catalog_source,
        session.catalog.session(),
        table_names.left,
        schema,
        lowered,
        .read_index,
    )) orelse return error.TableNotFound;
    defer result.deinit(allocator);

    return try encodeReadResultAlloc(allocator, session.sessionId(), try liteStatementKindForParsedLogicalPlan(logical_plan, parsed_sql), result);
}

fn executeAntflyQueryFunctionReadAlloc(
    allocator: Allocator,
    db: *antfly.db.DB,
    session: *Session,
    parsed_sql: *const sql_adapter.ParsedSql,
    statement_kind: sql_adapter.SqlReadStatementKind,
) !?[]u8 {
    var lowered = sql_adapter.lowerAntflyQueryFunctionReadParsedSqlAlloc(allocator, null, parsed_sql) catch |err| switch (err) {
        error.UnsupportedSqlShape => return null,
        else => return err,
    };
    defer lowered.deinit(allocator);

    var read_source = table_reads.BoundTableReadSource.init(lowered.table_name, 1, db, raft_mod.read_gate.noopReadableLeaseRequester());
    var query_response = (try read_source.source().query(allocator, lowered.table_name, lowered.request.req, .read_index)) orelse return error.TableNotFound;
    defer query_response.deinit(allocator);

    var rows = try sqlQueryFunctionRowsFromQueryResponseAlloc(allocator, query_response.json, lowered.projection_columns);
    defer rows.deinit(allocator);
    return try encodeReadResultAlloc(
        allocator,
        session.sessionId(),
        @tagName(statement_kind),
        .{ .document_query = rows },
    );
}

fn loadLiteSqlTableRecordForTargetAlloc(
    allocator: Allocator,
    db: *antfly.db.DB,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !?metadata_table_manager.TableRecord {
    if (try db.getLiteSqlTableRecordAlloc(allocator)) |table| return table;
    const schema_json = (try db.getSchemaJson(allocator)) orelse return null;
    defer allocator.free(schema_json);
    return try metadata_table_manager.cloneTable(allocator, .{
        .table_id = 1,
        .name = table_name,
        .database_name = session.currentDatabase(),
        .namespace_name = session.primarySearchPathNamespace(),
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
    });
}

const NonRowResponse = struct {
    kind: []const u8,
    statement_kind: []const u8,
    session_id: u64,
    noop: bool,
    applied: tables_api.AppliedRelationalSqlDdlRecord,
};

fn liteReadStatementKindForParsedCatalogRead(
    read: *const sql_adapter.CatalogLogicalReadPlan,
    parsed_sql: *const sql_adapter.ParsedSql,
) !sql_adapter.SqlReadStatementKind {
    const parsed_kind = parsed_sql.readStatementKindIncludingGeneratedAst();
    if (parsed_sql.generatedStatementKind() == .read) return parsed_kind orelse error.UnsupportedSqlShape;
    return read.statement.readKind() orelse parsed_kind orelse error.UnsupportedSqlShape;
}

fn liteWriteStatementKindForParsedCatalogWrite(
    write: *const sql_adapter.CatalogLogicalWritePlan,
    parsed_sql: *const sql_adapter.ParsedSql,
) !sql_adapter.SqlWriteStatementKind {
    const parsed_kind = parsed_sql.writeStatementKindIncludingGeneratedAst();
    if (parsed_sql.generatedStatementKind() == .dml) return parsed_kind orelse error.UnsupportedSqlShape;
    return write.statement.writeKind() orelse parsed_kind orelse error.UnsupportedSqlShape;
}

fn liteStatementKindForParsedLogicalPlan(plan: sql_adapter.LogicalSqlPlan, parsed_sql: *const sql_adapter.ParsedSql) ![]const u8 {
    return switch (plan) {
        .catalog_read => |read| @tagName(try liteReadStatementKindForParsedCatalogRead(&read, parsed_sql)),
        .catalog_write => |write| @tagName(try liteWriteStatementKindForParsedCatalogWrite(&write, parsed_sql)),
        .table_ddl => "table_ddl",
        .catalog_ddl => "catalog_ddl",
        .other_ddl => "other_ddl",
        .session => "session",
        .transaction => "transaction",
        .prepared_statement => "prepared_statement",
        .cursor => "cursor",
        .notification => "notification",
        .routine => "routine",
        .auth => "auth",
        .extension => "extension",
        .maintenance => "maintenance",
        .bulk_io => "bulk_io",
        .read => |kind| @tagName(kind),
        .write => |kind| @tagName(kind),
    };
}

fn encodeReadResultAlloc(allocator: Allocator, session_id: u64, statement_kind: []const u8, result: table_reads.LoweredSqlReadPlanResult) ![]u8 {
    const result_body = switch (result) {
        .query => |query| try relational_rows.encodeRowsQueryResponseAlloc(allocator, query),
        .document_query => |query| try relational_rows.encodeRowsQueryResponseAlloc(allocator, query),
        .set_operation => |query| try relational_rows.encodeRowsQueryResponseAlloc(allocator, query),
        .recursive_cte => |query| try relational_rows.encodeRowsQueryResponseAlloc(allocator, query),
        .aggregate => |aggregate| try relational_rows.encodeRowsAggregateResponseAlloc(allocator, aggregate),
        .window => |window| try relational_rows.encodeRowsWindowResponseAlloc(allocator, window),
        .join => |join| try relational_rows.encodeRowsJoinResponseAlloc(allocator, join),
        .lateral => |lateral| try relational_rows.encodeRowsJoinResponseAlloc(allocator, lateral),
    };
    defer allocator.free(result_body);

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print(
        "{{\"kind\":\"read\",\"session_id\":{d},\"statement_kind\":{f},\"result\":",
        .{ session_id, std.json.fmt(statement_kind, .{}) },
    );
    try writer.writeAll(result_body);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn encodeRowsBatchResultAlloc(allocator: Allocator, session_id: u64, statement_kind: []const u8, rows_batch: relational_rows.OwnedRowsBatchRequest) ![]u8 {
    const result_body = try relational_rows.encodeRowsBatchResponseAlloc(allocator, rows_batch);
    defer allocator.free(result_body);

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print(
        "{{\"kind\":\"write\",\"session_id\":{d},\"statement_kind\":{f},\"result\":",
        .{ session_id, std.json.fmt(statement_kind, .{}) },
    );
    try writer.writeAll(result_body);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn sqlQueryFunctionRowsFromQueryResponseAlloc(
    allocator: Allocator,
    response_json: []const u8,
    projection_columns: []const []const u8,
) !relational_rows.OwnedRowsQueryResult {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, response_json, .{ .allocate = .alloc_always }) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    const responses_value = parsed.value.object.get("responses") orelse return error.InvalidQueryRequest;
    if (responses_value != .array or responses_value.array.items.len == 0) return error.InvalidQueryRequest;
    const first_response = responses_value.array.items[0];
    if (first_response != .object) return error.InvalidQueryRequest;
    const hits_value = first_response.object.get("hits") orelse return error.InvalidQueryRequest;
    if (hits_value != .object) return error.InvalidQueryRequest;
    const total_value = hits_value.object.get("total") orelse return error.InvalidQueryRequest;
    const total = switch (total_value) {
        .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @as(u32, @intCast(value)) else return error.InvalidQueryRequest,
        .number_string => |text| std.fmt.parseUnsigned(u32, text, 10) catch return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    const hit_items = hits_value.object.get("hits") orelse return error.InvalidQueryRequest;
    if (hit_items != .array) return error.InvalidQueryRequest;

    const rows = try allocator.alloc([]const u8, hit_items.array.items.len);
    errdefer allocator.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| allocator.free(@constCast(row));
    }
    for (hit_items.array.items, 0..) |hit, i| {
        if (hit != .object) return error.InvalidQueryRequest;
        rows[i] = try sqlQueryFunctionHitRowAlloc(allocator, hit, projection_columns);
        initialized += 1;
    }
    return .{
        .rows = rows,
        .total = total,
    };
}

fn sqlQueryFunctionHitRowAlloc(
    allocator: Allocator,
    hit: std.json.Value,
    projection_columns: []const []const u8,
) ![]const u8 {
    if (projection_columns.len == 0) return try std.json.Stringify.valueAlloc(allocator, hit, .{});
    if (hit != .object) return error.InvalidQueryRequest;
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    for (projection_columns, 0..) |column, i| {
        if (i != 0) try writer.writeByte(',');
        try std.json.Stringify.value(column, .{}, writer);
        try writer.writeByte(':');
        if (hit.object.get(column)) |value| {
            try std.json.Stringify.value(value, .{}, writer);
        } else {
            try writer.writeAll("null");
        }
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

const LiteSingleTableCatalog = struct {
    table: ?metadata_table_manager.TableRecord = null,
    owned: bool = false,

    fn initBorrowed(table_record: metadata_table_manager.TableRecord) LiteSingleTableCatalog {
        return .{ .table = table_record };
    }

    fn fromStoredTableAlloc(allocator: Allocator, db: *antfly.db.DB) !LiteSingleTableCatalog {
        return .{
            .table = try db.getLiteSqlTableRecordAlloc(allocator),
            .owned = true,
        };
    }

    fn deinit(self: *@This(), allocator: Allocator) void {
        if (self.owned) {
            if (self.table) |table| metadata_table_manager.freeTable(allocator, table);
        }
        self.* = undefined;
    }

    fn iface(self: *LiteSingleTableCatalog) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *LiteSingleTableCatalog = @ptrCast(@alignCast(ptr));
        const tables = if (self.table) |*table|
            @as([*]metadata_table_manager.TableRecord, @ptrCast(table))[0..1]
        else
            @constCast((&[_]metadata_table_manager.TableRecord{})[0..]);
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables,
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const DbUniqueSelectorResolverContext = struct {
    db: *antfly.db.DB,

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolveUnique,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolveUnique(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
        return null;
    }

    fn primaryExists(ptr: *anyopaque, alloc: Allocator, _: []const u8, physical_key: []const u8) !bool {
        const self: *DbUniqueSelectorResolverContext = @ptrCast(@alignCast(ptr));
        var result = (try self.db.lookup(alloc, physical_key, .{})) orelse return false;
        defer result.deinit(alloc);
        return true;
    }

    fn lookupPrimary(ptr: *anyopaque, alloc: Allocator, _: []const u8, physical_key: []const u8) !?relational_rows.ResolvedPrimaryRow {
        const self: *DbUniqueSelectorResolverContext = @ptrCast(@alignCast(ptr));
        var result = (try self.db.lookup(alloc, physical_key, .{})) orelse return null;
        errdefer result.deinit(alloc);
        return .{
            .json = result.json,
            .version = try self.db.getTimestamp(alloc, physical_key),
        };
    }
};

fn ddlTargetTableNameAlloc(allocator: Allocator, plan: sql_adapter.TableDdlLogicalPlan, session: catalog_resources.SqlCatalogSession) ![]u8 {
    var target = try tables_api.relationalSqlDdlTargetForTablePlanWithSessionAlloc(allocator, plan, session);
    defer target.deinit(allocator);
    if (target.table_name.len == 0) return error.UnsupportedSqlShape;
    return try allocator.dupe(u8, target.table_name);
}

pub fn firstStatementEnd(sql: []const u8) ?usize {
    var i: usize = 0;
    var state: enum { normal, single_quote, double_quote, line_comment, block_comment, dollar_quote } = .normal;
    var dollar_delim: []const u8 = "";
    while (i < sql.len) {
        switch (state) {
            .normal => {
                if (sql[i] == ';') return i;
                if (sql[i] == '\'') {
                    state = .single_quote;
                    i += 1;
                    continue;
                }
                if (sql[i] == '"') {
                    state = .double_quote;
                    i += 1;
                    continue;
                }
                if (sql[i] == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
                    state = .line_comment;
                    i += 2;
                    continue;
                }
                if (sql[i] == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
                    state = .block_comment;
                    i += 2;
                    continue;
                }
                if (sql[i] == '$') {
                    if (dollarQuoteDelimiter(sql[i..])) |delim| {
                        dollar_delim = delim;
                        state = .dollar_quote;
                        i += delim.len;
                        continue;
                    }
                }
                i += 1;
            },
            .single_quote => {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2;
                    continue;
                }
                if (sql[i] == '\'') state = .normal;
                i += 1;
            },
            .double_quote => {
                if (sql[i] == '"' and i + 1 < sql.len and sql[i + 1] == '"') {
                    i += 2;
                    continue;
                }
                if (sql[i] == '"') state = .normal;
                i += 1;
            },
            .line_comment => {
                if (sql[i] == '\n') state = .normal;
                i += 1;
            },
            .block_comment => {
                if (sql[i] == '*' and i + 1 < sql.len and sql[i + 1] == '/') {
                    state = .normal;
                    i += 2;
                    continue;
                }
                i += 1;
            },
            .dollar_quote => {
                if (std.mem.startsWith(u8, sql[i..], dollar_delim)) {
                    state = .normal;
                    i += dollar_delim.len;
                    continue;
                }
                i += 1;
            },
        }
    }
    return null;
}

fn dollarQuoteDelimiter(sql: []const u8) ?[]const u8 {
    if (sql.len == 0 or sql[0] != '$') return null;
    var i: usize = 1;
    while (i < sql.len and sql[i] != '$') : (i += 1) {
        if (!std.ascii.isAlphanumeric(sql[i]) and sql[i] != '_') return null;
    }
    if (i >= sql.len or sql[i] != '$') return null;
    return sql[0 .. i + 1];
}

fn writeJsonLine(io: std.Io, json: []const u8) void {
    cli.writeStdout(io, json);
    cli.writeStdout(io, "\n");
}

fn validateCatalogIdentifier(name: []const u8) !void {
    if (name.len == 0) return error.InvalidSqlRequest;
    for (name) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_')) return error.InvalidSqlRequest;
    }
}

fn requireAflitePath(path: []const u8) !void {
    if (!std.mem.endsWith(u8, path, ".aflite")) {
        std.debug.print("lite database path must end with .aflite: {s}\n", .{path});
        return error.InvalidArguments;
    }
}

fn printUsage() void {
    std.debug.print(
        \\usage: antfly lite sql <db.aflite> [-c <sql> | -f <path>] [--database <name>] [--namespace <name>]
        \\
        \\Without -c or -f, starts a small psql-style REPL. End statements with
        \\a semicolon. Use \q or .quit to exit.
        \\
    , .{});
}

test "lite sql statement splitter ignores quoted semicolons" {
    try std.testing.expectEqual(@as(?usize, 35), firstStatementEnd("select ';' as semi, \"x;y\" from docs;"));
    try std.testing.expectEqual(@as(?usize, 22), firstStatementEnd("select $$a;b$$ as body;"));
    try std.testing.expectEqual(@as(?usize, null), firstStatementEnd("select 'unterminated;"));
}

test "lite sql reads legacy local schema metadata without table record" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-legacy", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(allocator, schema_json, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"row:a\",\"status\":\"open\",\"amount\":42}" }},
    });

    const stored_table_record = try db.getLiteSqlTableRecordAlloc(allocator);
    defer if (stored_table_record) |record| metadata_table_manager.freeTable(allocator, record);
    try std.testing.expect(stored_table_record == null);

    var session = try Session.init(allocator, .{});
    defer session.deinit(allocator);
    const body = try executeOneJsonAlloc(allocator, &db, &session, "SELECT id, amount FROM usage_records WHERE status = 'open';");
    defer allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"kind\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"id\":\"row:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"amount\":42") != null);
}

test "lite sql ddl updates catalog for subsequent statements" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-table-record", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try Session.init(allocator, .{});
    defer session.deinit(allocator);

    const ddl_body = try executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text);");
    defer allocator.free(ddl_body);
    var parsed_ddl = try std.json.parseFromSlice(std.json.Value, allocator, ddl_body, .{ .allocate = .alloc_always });
    defer parsed_ddl.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_ddl.value.object.get("kind").?.string);

    const read_body = try executeOneJsonAlloc(allocator, &db, &session, "SELECT id, status FROM usage_records;");
    defer allocator.free(read_body);
    try std.testing.expect(std.mem.indexOf(u8, read_body, "\"kind\":\"read\"") != null);

    try std.testing.expectError(error.InvalidSqlCatalog, executeOneJsonAlloc(allocator, &db, &session, "SELECT id, status FROM other_records;"));
    try std.testing.expectError(error.InvalidSqlCatalog, executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO other_records (id, status) VALUES ('row:a', 'open');"));
}

test "lite sql applies session plans before later logical planning" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-session-plan", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try Session.init(allocator, .{});
    defer session.deinit(allocator);

    const set_body = try executeOneJsonAlloc(allocator, &db, &session, "SET search_path TO tenant_schema;");
    defer allocator.free(set_body);
    var parsed_set = try std.json.parseFromSlice(std.json.Value, allocator, set_body, .{ .allocate = .alloc_always });
    defer parsed_set.deinit();
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("statement_kind").?.string);
    try std.testing.expectEqualStrings("tenant_schema", session.catalog.search_path[0]);

    const ddl_body = try executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text);");
    defer allocator.free(ddl_body);
    var parsed_ddl = try std.json.parseFromSlice(std.json.Value, allocator, ddl_body, .{ .allocate = .alloc_always });
    defer parsed_ddl.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_ddl.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("table_ddl", parsed_ddl.value.object.get("statement_kind").?.string);

    const table_record = (try db.getLiteSqlTableRecordAlloc(allocator)) orelse return error.TestUnexpectedResult;
    defer metadata_table_manager.freeTable(allocator, table_record);
    try std.testing.expectEqualStrings("tenant_schema", table_record.namespace_name);
}

test "lite sql antfly query functions use native document query path" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-query-functions", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();
    try db.applyLiteSqlTableRecord(allocator, .{
        .table_id = 1,
        .name = "docs",
        .database_name = "default",
        .namespace_name = "public",
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\",\"field\":\"title\"}}",
    });
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"archived\"}" },
        },
        .sync_level = .full_index,
    });

    var session = try Session.init(allocator, .{});
    defer session.deinit(allocator);
    const body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT * FROM antfly.full_text_search(table_name => 'docs', index => 'full_text_index_v0', field => 'title', query => 'alpha', limit => 5);",
    );
    defer allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("read", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", parsed.value.object.get("statement_kind").?.string);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("total").?.integer);
    const row = result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", row.get("_source").?.object.get("title").?.string);

    const projected_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, _score FROM antfly.full_text_search(table_name => 'docs', index => 'full_text_index_v0', field => 'title', query => 'alpha', limit => 5);",
    );
    defer allocator.free(projected_body);

    var projected = try std.json.parseFromSlice(std.json.Value, allocator, projected_body, .{ .allocate = .alloc_always });
    defer projected.deinit();
    const projected_result = projected.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), projected_result.get("total").?.integer);
    const projected_row = projected_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", projected_row.get("_id").?.string);
    try std.testing.expect(projected_row.get("_score") != null);
    try std.testing.expect(projected_row.get("_source") == null);
}

test "lite sql document table reads use typed document plan path" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"note":{"type":"keyword"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":true}}}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-document-table", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try Session.init(allocator, .{});
    defer session.deinit(allocator);

    try db.applyLiteSqlTableRecord(allocator, .{
        .table_id = 1,
        .name = "docs",
        .database_name = session.catalog.session().currentDatabase(),
        .namespace_name = session.catalog.session().primarySearchPathNamespace(),
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = "{\"typed_paths\":{\"keyword\":[\"metadata.plan\"]}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\",\"amount\":10,\"note\":null,\"metadata\":{\"plan\":\"pro\"},\"tags\":[\"urgent\",\"vip\"]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"archived\",\"amount\":20,\"metadata\":{\"plan\":\"free\"},\"tags\":[\"stale\"]}" },
        },
        .sync_level = .full_index,
    });

    const lookup_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, title, metadata->>'plan' AS plan FROM docs WHERE _id = 'doc:a';",
    );
    defer allocator.free(lookup_body);
    var lookup = try std.json.parseFromSlice(std.json.Value, allocator, lookup_body, .{ .allocate = .alloc_always });
    defer lookup.deinit();
    try std.testing.expectEqualStrings("read", lookup.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", lookup.value.object.get("statement_kind").?.string);
    const lookup_result = lookup.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), lookup_result.get("total").?.integer);
    const lookup_row = lookup_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", lookup_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", lookup_row.get("title").?.string);
    try std.testing.expectEqualStrings("pro", lookup_row.get("plan").?.string);

    const star_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT * FROM docs WHERE _id = 'doc:a';",
    );
    defer allocator.free(star_body);
    var star = try std.json.parseFromSlice(std.json.Value, allocator, star_body, .{ .allocate = .alloc_always });
    defer star.deinit();
    const star_result = star.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), star_result.get("total").?.integer);
    const star_row = star_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", star_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", star_row.get("title").?.string);
    try std.testing.expectEqualStrings("active", star_row.get("status").?.string);
    const star_doc = star_row.get("_doc").?.object;
    try std.testing.expectEqualStrings("alpha", star_doc.get("title").?.string);
    try std.testing.expectEqualStrings("pro", star_doc.get("metadata").?.object.get("plan").?.string);

    const bounded_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;",
    );
    defer allocator.free(bounded_body);
    var bounded = try std.json.parseFromSlice(std.json.Value, allocator, bounded_body, .{ .allocate = .alloc_always });
    defer bounded.deinit();
    const bounded_result = bounded.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), bounded_result.get("total").?.integer);
    const bounded_row = bounded_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", bounded_row.get("_id").?.string);
    try std.testing.expectEqualStrings("active", bounded_row.get("status").?.string);

    const scalar_ops_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, amount FROM docs WHERE status IN ('active', 'pending') AND title LIKE 'alp%' AND amount BETWEEN 5 AND 15 LIMIT 10;",
    );
    defer allocator.free(scalar_ops_body);
    var scalar_ops = try std.json.parseFromSlice(std.json.Value, allocator, scalar_ops_body, .{ .allocate = .alloc_always });
    defer scalar_ops.deinit();
    const scalar_ops_result = scalar_ops.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), scalar_ops_result.get("total").?.integer);
    const scalar_ops_row = scalar_ops_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", scalar_ops_row.get("_id").?.string);
    try std.testing.expectEqual(@as(i64, 10), scalar_ops_row.get("amount").?.integer);

    const null_predicate_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id FROM docs WHERE note IS NULL ORDER BY _id ASC LIMIT 10;",
    );
    defer allocator.free(null_predicate_body);
    var null_predicate = try std.json.parseFromSlice(std.json.Value, allocator, null_predicate_body, .{ .allocate = .alloc_always });
    defer null_predicate.deinit();
    const null_predicate_rows = null_predicate.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", null_predicate_rows[1].object.get("_id").?.string);

    const not_null_predicate_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id FROM docs WHERE status IS NOT NULL ORDER BY _id ASC LIMIT 10;",
    );
    defer allocator.free(not_null_predicate_body);
    var not_null_predicate = try std.json.parseFromSlice(std.json.Value, allocator, not_null_predicate_body, .{ .allocate = .alloc_always });
    defer not_null_predicate.deinit();
    const not_null_predicate_rows = not_null_predicate.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), not_null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", not_null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", not_null_predicate_rows[1].object.get("_id").?.string);

    const ordered_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, title FROM docs ORDER BY title DESC LIMIT 2;",
    );
    defer allocator.free(ordered_body);
    var ordered = try std.json.parseFromSlice(std.json.Value, allocator, ordered_body, .{ .allocate = .alloc_always });
    defer ordered.deinit();
    const ordered_rows = ordered.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), ordered_rows.len);
    try std.testing.expectEqualStrings("doc:b", ordered_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("beta", ordered_rows[0].object.get("title").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", ordered_rows[1].object.get("title").?.string);

    const unnest_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10;",
    );
    defer allocator.free(unnest_body);
    var unnest = try std.json.parseFromSlice(std.json.Value, allocator, unnest_body, .{ .allocate = .alloc_always });
    defer unnest.deinit();
    const unnest_result = unnest.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), unnest_result.get("total").?.integer);
    const unnest_row = unnest_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", unnest_row.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", unnest_row.get("tag").?.string);

    try std.testing.expectError(
        error.DocumentSqlWriteUnsupported,
        executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO docs (_id, _doc) VALUES ('doc:c', '{\"title\":\"gamma\"}');"),
    );
}
