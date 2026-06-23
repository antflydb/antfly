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
    var lite = try LiteDb.open(allocator, path, .writer);
    defer lite.close();

    const body = executeOneJsonAlloc(allocator, &lite.db, session, sql) catch |err| {
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

    if (parsed_sql.writeStatementKind() != null) return try executeWriteAlloc(allocator, db, session, &parsed_sql);
    if (parsed_sql.readStatementKind() != null) return try executeReadAlloc(allocator, db, session, &parsed_sql);
    return try executeDdlAlloc(allocator, db, session, &parsed_sql);
}

fn executeDdlAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    var plan = try sql_adapter.lowerDdlPlanParsedSqlAlloc(allocator, parsed_sql);
    defer plan.deinit(allocator);

    const table_name = try ddlTargetTableNameAlloc(allocator, plan, session.catalog.session());
    defer allocator.free(table_name);

    const existing_schema = try db.getSchemaJson(allocator);
    defer if (existing_schema) |schema_json| allocator.free(schema_json);
    const base_schema = existing_schema orelse "";
    var table_record = metadata_table_manager.TableRecord{
        .table_id = 1,
        .name = table_name,
        .database_name = session.catalog.session().currentDatabase(),
        .namespace_name = session.catalog.session().primarySearchPathNamespace(),
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = base_schema,
    };

    var applied = try tables_api.applyRelationalSqlDdlPlanToTableRecordWithSessionAlloc(
        allocator,
        &table_record,
        &plan,
        session.catalog.session(),
    );
    defer applied.deinit(allocator);

    try db.applyTableSchemaJson(allocator, applied.table.schema_json, .{});

    const response = DdlResponse{
        .session_id = session.sessionId(),
        .noop = applied.noop,
        .applied = applied,
    };
    return try std.json.Stringify.valueAlloc(allocator, response, .{ .whitespace = .indent_2 });
}

fn executeWriteAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    const target_table = try writeTargetTableNameAlloc(allocator, parsed_sql);
    defer allocator.free(target_table);

    const schema_json = (try db.getSchemaJson(allocator)) orelse return error.InvalidSqlCatalog;
    defer allocator.free(schema_json);
    var catalog = SingleTableCatalog.init(target_table, schema_json, session.catalog.session());
    const catalog_source = catalog.iface();

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, target_table, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var write_source = table_writes.BoundTableWriteSource.init(target_table, db);
    var unique_resolver_ctx = DbUniqueSelectorResolverContext{ .db = db };
    var lowered = try sql_adapter_runtime.lowerWritePlanWithCatalogParsedSqlAlloc(
        allocator,
        parsed_sql,
        schema,
        &.{},
        .{
            .unique_resolver = unique_resolver_ctx.resolver(),
            .sync_level = try sql_adapter.sqlSyncLevelFromSession(session.catalog.session()),
        },
        catalog_source,
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

    return try encodeRowsBatchResultAlloc(allocator, session.sessionId(), @tagName(parsed_sql.writeStatementKind().?), rows_batch.*);
}

fn executeReadAlloc(allocator: Allocator, db: *antfly.db.DB, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    const statement_kind = parsed_sql.readStatementKind() orelse return error.UnsupportedSqlShape;
    var table_names = (try sql_adapter.readSourceTableNamesFromParsedSqlAlloc(allocator, parsed_sql)) orelse return error.UnsupportedSqlShape;
    defer table_names.deinit(allocator);

    const schema_json = (try db.getSchemaJson(allocator)) orelse return error.InvalidSqlCatalog;
    defer allocator.free(schema_json);
    var catalog = SingleTableCatalog.init(table_names.left, schema_json, session.catalog.session());
    const catalog_source = catalog.iface();

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, table_names.left, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var lowered = try sql_adapter_runtime.lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
        allocator,
        parsed_sql,
        schema,
        &.{},
        catalog_source,
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

    return try encodeReadResultAlloc(allocator, session.sessionId(), @tagName(statement_kind), result);
}

const DdlResponse = struct {
    kind: []const u8 = "ddl",
    session_id: u64,
    noop: bool,
    applied: tables_api.AppliedRelationalSqlDdlRecord,
};

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

const SingleTableCatalog = struct {
    tables: [1]metadata_table_manager.TableRecord,

    fn init(table_name: []const u8, schema_json: []const u8, session: catalog_resources.SqlCatalogSession) SingleTableCatalog {
        return .{ .tables = .{.{
            .table_id = 1,
            .name = table_name,
            .database_name = session.currentDatabase(),
            .namespace_name = session.primarySearchPathNamespace(),
            .placement_role = "data",
            .desired_replica_count = 1,
            .schema_json = schema_json,
        }} };
    }

    fn iface(self: *SingleTableCatalog) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *SingleTableCatalog = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables[0..],
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

fn ddlTargetTableNameAlloc(allocator: Allocator, plan: sql_adapter.LoweredDdlPlan, session: catalog_resources.SqlCatalogSession) ![]u8 {
    var target = try tables_api.relationalSqlDdlTargetForPlanWithSessionAlloc(allocator, plan, session);
    defer target.deinit(allocator);
    if (target.table_name.len == 0) return error.UnsupportedSqlShape;
    return try allocator.dupe(u8, target.table_name);
}

fn writeTargetTableNameAlloc(allocator: Allocator, parsed_sql: *const sql_adapter.ParsedSql) ![]const u8 {
    const statement_kind = parsed_sql.writeStatementKind() orelse return error.UnsupportedSqlShape;
    const tokens = parsed_sql.items();
    const raw = parsed_sql.statement.raw();
    if (raw.token_start >= raw.token_end or raw.token_start >= tokens.len) return error.UnsupportedSqlShape;
    var pos = withFinalStatementIndex(tokens, raw.token_start, raw.token_end) orelse return error.UnsupportedSqlShape;
    switch (statement_kind) {
        .insert, .insert_source => {
            if (!tokens[pos].matchesKeywordTag(.insert)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos >= raw.token_end or !tokens[pos].matchesKeywordTag(.into)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.only)) pos += 1;
        },
        .update, .update_source, .update_joined_source => {
            if (!tokens[pos].matchesKeywordTag(.update)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.only)) pos += 1;
        },
        .delete, .delete_source, .delete_joined_source => {
            if (!tokens[pos].matchesKeywordTag(.delete)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos >= raw.token_end or !tokens[pos].matchesKeywordTag(.from)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.only)) pos += 1;
        },
        .truncate => {
            if (!tokens[pos].matchesKeywordTag(.truncate)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.table)) pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.only)) pos += 1;
        },
        .merge => {
            if (!tokens[pos].matchesKeywordTag(.merge)) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.into)) pos += 1;
            if (pos < raw.token_end and tokens[pos].matchesKeywordTag(.only)) pos += 1;
        },
    }
    if (pos >= raw.token_end or tokens[pos].kind != .identifier) return error.UnsupportedSqlShape;
    return try sql_adapter.normalizeSqlObjectIdentifierAlloc(allocator, tokens[pos].text);
}

fn withFinalStatementIndex(tokens: []const sql_adapter.Token, start: usize, end: usize) ?usize {
    if (start >= end or start >= tokens.len) return null;
    if (!tokens[start].matchesKeywordTag(.with)) return start;

    var index = start + 1;
    if (index < end and tokens[index].matchesKeywordTag(.recursive)) index += 1;
    while (true) {
        if (index >= end or tokens[index].kind != .identifier) return null;
        index += 1;
        if (index < end and tokens[index].kind == .lparen) {
            index = (findMatchingRParenIndex(tokens, index, end) orelse return null) + 1;
        }
        if (index >= end or !tokens[index].matchesKeywordTag(.as)) return null;
        index += 1;
        if (index < end and tokens[index].matchesKeywordTag(.not)) {
            if (index + 1 < end and tokens[index + 1].matchesKeywordTag(.materialized)) index += 2;
        } else if (index < end and tokens[index].matchesKeywordTag(.materialized)) {
            index += 1;
        }
        if (index >= end or tokens[index].kind != .lparen) return null;
        index = (findMatchingRParenIndex(tokens, index, end) orelse return null) + 1;
        if (index < end and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }
    if (index >= end or tokens[index].kind != .identifier) return null;
    return index;
}

fn findMatchingRParenIndex(tokens: []const sql_adapter.Token, lparen_index: usize, end: usize) ?usize {
    if (lparen_index >= end or tokens[lparen_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = lparen_index + 1;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
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
    try std.testing.expectEqual(@as(?usize, 40), firstStatementEnd("select ';' as semi, \"x;y\" from docs;"));
    try std.testing.expectEqual(@as(?usize, 22), firstStatementEnd("select $$a;b$$ as body;"));
    try std.testing.expectEqual(@as(?usize, null), firstStatementEnd("select 'unterminated;"));
}
