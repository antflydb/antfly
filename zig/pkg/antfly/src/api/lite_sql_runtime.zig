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
const Allocator = std.mem.Allocator;
const catalog_resources = @import("../metadata/catalog/resources.zig");
const table_record_mod = @import("../metadata/catalog/table_record.zig");
const relational_rows = @import("relational_rows.zig");
const storage_schema = @import("../storage/schema.zig");
const table_schema = @import("../schema/mod.zig");
const table_catalog = @import("../sql/catalog_source.zig");
const ddl_result = @import("../sql/ddl_result.zig");
const sql_schema_mutation = @import("../sql/schema_mutation.zig");
const lite_sql_source = @import("lite_sql_source.zig");
const lite_sql_value_ref = @import("lite_sql_value_ref.zig");
const ReadPlanResult = @import("table_reads/relational_rows.zig").LoweredSqlReadPlanResult;

// Lite owns orchestration, while the SQL package owns parsing and planning.
// Keep this list local so SQL does not expose a Lite-specific runtime facade.
const sql_adapter = struct {
    const binder = @import("../sql/binder.zig");
    const catalog_apply = @import("../sql/catalog_apply.zig");
    const ddl_plan = @import("../sql/ddl_plan.zig");
    const executor = @import("../sql/executor.zig");
    const lower_dml = @import("../sql/lower_dml.zig");
    const lower_select = @import("../sql/lower_select.zig");
    const plan = @import("../sql/plan.zig");
    const query_function = @import("../sql/query_function.zig");
    const statement_kind = @import("../sql/statement_kind.zig");
    const tokenized = @import("../sql/tokenized.zig");

    pub const CatalogLogicalReadPlan = binder.CatalogLogicalReadPlan;
    pub const CatalogLogicalWritePlan = binder.CatalogLogicalWritePlan;
    pub const LogicalSqlPlan = binder.LogicalSqlPlan;
    pub const OtherDdlLogicalPlan = binder.OtherDdlLogicalPlan;
    pub const TableDdlLogicalPlan = binder.TableDdlLogicalPlan;
    pub const SessionCatalogPlan = ddl_plan.SessionCatalogPlan;
    pub const OwnedSqlCatalogSession = catalog_apply.OwnedSqlCatalogSession;
    pub const ParsedSql = tokenized.ParsedSql;
    pub const SqlReadStatementKind = statement_kind.SqlReadStatementKind;
    pub const SqlWriteStatementKind = statement_kind.SqlWriteStatementKind;

    pub const applyOwnedSessionCatalogPlanAlloc = catalog_apply.applyOwnedSessionCatalogPlanAlloc;
    pub const lowerAntflyQueryFunctionReadParsedSqlAlloc = query_function.lowerAntflyQueryFunctionReadParsedSqlAlloc;
    pub const lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc = lower_select.lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc;
    pub const lowerWritePlanWithLogicalPlanAndFunctionBindingsAlloc = lower_dml.lowerWritePlanWithLogicalPlanAndFunctionBindingsAlloc;
    pub const parsedSqlHasGeneratedAntflyReadSource = query_function.parsedSqlHasGeneratedAntflyReadSource;
    pub const planParsedSqlWithSessionAlloc = executor.planParsedSqlWithSessionAlloc;
    pub const readSourceTableNamesFromParsedSqlAlloc = binder.readSourceTableNamesFromParsedSqlAlloc;
    pub const runtimeSchemaForCatalogTableWithSessionAlloc = binder.runtimeSchemaForCatalogTableWithSessionAlloc;
    pub const sqlSyncLevelFromSession = catalog_apply.sqlSyncLevelFromSession;
    pub const writeTargetTableNameFromParsedSqlAlloc = binder.writeTargetTableNameFromParsedSqlAlloc;
};

pub const max_sql_file_bytes = 64 * 1024 * 1024;
pub const max_repl_statement_bytes = 16 * 1024 * 1024;

pub const CatalogOptions = struct {
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
};

pub const Session = struct {
    catalog: sql_adapter.OwnedSqlCatalogSession,

    pub fn init(alloc: Allocator, flags: CatalogOptions) !Session {
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

pub fn statementIsReadOnly(allocator: Allocator, sql: []const u8) !bool {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(allocator, sql);
    defer parsed_sql.deinit(allocator);
    return parsed_sql.statement == .read;
}

pub fn executeOneWithSourceJsonAlloc(allocator: Allocator, source: lite_sql_source.Source, session: *Session, sql: []const u8) ![]u8 {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(allocator, sql);
    defer parsed_sql.deinit(allocator);
    return try executeParsedSqlJsonAlloc(allocator, source, session, &parsed_sql);
}

fn executeParsedSqlJsonAlloc(allocator: Allocator, source: lite_sql_source.Source, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    switch (parsed_sql.statement) {
        .read => return try executeReadAlloc(allocator, source, session, parsed_sql),
        .write => return try executeWriteAlloc(allocator, source, session, parsed_sql),
        else => switch (parsed_sql.generatedStatementKind() orelse .other) {
            .read => return try executeReadAlloc(allocator, source, session, parsed_sql),
            .dml => return try executeWriteAlloc(allocator, source, session, parsed_sql),
            else => return try executeNonRowAlloc(allocator, source, session, parsed_sql),
        },
    }
}

fn executeNonRowAlloc(allocator: Allocator, source: lite_sql_source.Source, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    var catalog = try LiteSingleTableCatalog.fromStoredTableAlloc(allocator, source);
    defer catalog.deinit(allocator);

    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog.iface(),
        .session = session.catalog.session(),
    });
    defer logical_plan.deinit(allocator);

    return switch (logical_plan) {
        .table_ddl => try executeTableDdlLogicalPlanAlloc(allocator, source, session, &logical_plan.table_ddl),
        .session => try executeSessionLogicalPlanAlloc(allocator, session, logical_plan.session),
        .other_ddl => try executeOtherDdlLogicalPlanAlloc(allocator, session, logical_plan.other_ddl),
        else => error.UnsupportedSqlShape,
    };
}

fn executeTableDdlLogicalPlanAlloc(
    allocator: Allocator,
    source: lite_sql_source.Source,
    session: *Session,
    table_plan: *sql_adapter.TableDdlLogicalPlan,
) ![]u8 {
    var applied: ddl_result.AppliedRelationalSqlDdlRecord = undefined;
    try source.applyTableDdlPlan(
        allocator,
        lite_sql_value_ref.Ref.from(sql_adapter.TableDdlLogicalPlan, .table_ddl, table_plan),
        session.catalog.session(),
        lite_sql_value_ref.OutRef.from(ddl_result.AppliedRelationalSqlDdlRecord, .ddl_result, &applied),
    );
    defer applied.deinit(allocator);

    try executeLiteLocalSecondaryIndexWorkAlloc(allocator, source, &applied);

    const response = NonRowResponse{
        .kind = "ddl",
        .statement_kind = "table_ddl",
        .session_id = session.sessionId(),
        .noop = applied.noop,
        .applied = applied,
    };
    return try std.json.Stringify.valueAlloc(allocator, response, .{ .whitespace = .indent_2 });
}

const LiteLocalSecondaryIndex = struct {
    name: []u8,
    generation: u64,
    access_method: storage_schema.RelationalIndexAccessMethod,
    schema_fingerprint: []u8,

    fn deinit(self: *@This(), allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.schema_fingerprint);
        self.* = undefined;
    }
};

fn executeLiteLocalSecondaryIndexWorkAlloc(
    allocator: Allocator,
    source: lite_sql_source.Source,
    applied: *ddl_result.AppliedRelationalSqlDdlRecord,
) !void {
    if (applied.dropped_table or applied.table.schema_json.len == 0) return;

    const indexes = try liteLocalBuildingSecondaryIndexesAlloc(allocator, applied.table.schema_json);
    defer {
        for (indexes) |*index| index.deinit(allocator);
        if (indexes.len > 0) allocator.free(indexes);
    }
    if (indexes.len == 0) return;

    for (indexes) |index| {
        try source.rebuildSecondaryIndex(index.name, index.generation);
    }

    var ready_schema_json = try allocator.dupe(u8, applied.table.schema_json);
    var ready_schema_transferred = false;
    errdefer if (!ready_schema_transferred) allocator.free(ready_schema_json);
    for (indexes) |index| {
        const next_schema_json = try sql_schema_mutation.schemaWithSecondaryIndexReadyCheckedAlloc(
            allocator,
            ready_schema_json,
            index.name,
            .{
                .generation = index.generation,
                .access_method = index.access_method,
                .schema_fingerprint = index.schema_fingerprint,
            },
        );
        allocator.free(ready_schema_json);
        ready_schema_json = next_schema_json;
    }

    allocator.free(applied.table.schema_json);
    applied.table.schema_json = ready_schema_json;
    ready_schema_transferred = true;
    try source.applyTable(allocator, applied.table);
}

fn liteLocalBuildingSecondaryIndexesAlloc(
    allocator: Allocator,
    schema_json: []const u8,
) ![]LiteLocalSecondaryIndex {
    var parsed_schema = try table_schema.parseValidatedTableSchema(allocator, schema_json);
    defer parsed_schema.deinit(allocator);
    const runtime_schema = try table_schema.deriveRuntimeTableSchema(allocator, parsed_schema);
    defer storage_schema.freeSchema(allocator, runtime_schema);

    var indexes = std.ArrayListUnmanaged(LiteLocalSecondaryIndex).empty;
    errdefer {
        for (indexes.items) |*index| index.deinit(allocator);
        indexes.deinit(allocator);
    }

    for (runtime_schema.relational_indexes) |index| {
        if (index.lifecycle != .building or index.generation == 0) continue;
        const schema_fingerprint = index.schema_fingerprint orelse return error.InvalidSchemaUpdateRequest;
        try indexes.append(allocator, .{
            .name = try allocator.dupe(u8, index.name),
            .generation = index.generation,
            .access_method = index.access_method,
            .schema_fingerprint = try allocator.dupe(u8, schema_fingerprint),
        });
    }

    // Retain support for schemas that only carry legacy column-owned indexes.
    for (runtime_schema.relational_columns) |column| {
        if (!column.indexed or column.index_lifecycle != .building or column.index_generation == 0) continue;
        const identity = column.index_name orelse column.name;
        const access_method = column.index_access_method orelse return error.InvalidSchemaUpdateRequest;
        const schema_fingerprint = column.index_schema_fingerprint orelse return error.InvalidSchemaUpdateRequest;
        var seen = false;
        for (indexes.items) |existing| {
            if (std.mem.eql(u8, existing.name, identity)) {
                seen = true;
                break;
            }
        }
        if (seen) continue;
        try indexes.append(allocator, .{
            .name = try allocator.dupe(u8, identity),
            .generation = column.index_generation,
            .access_method = access_method,
            .schema_fingerprint = try allocator.dupe(u8, schema_fingerprint),
        });
    }

    return try indexes.toOwnedSlice(allocator);
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

    var applied = try ddl_result.emptyAppliedRelationalSqlDdlRecordAlloc(allocator);
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
    var applied = try ddl_result.emptyAppliedRelationalSqlDdlRecordAlloc(allocator);
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

fn executeWriteAlloc(allocator: Allocator, source: lite_sql_source.Source, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    const target_table = try sql_adapter.writeTargetTableNameFromParsedSqlAlloc(allocator, parsed_sql);
    defer allocator.free(target_table);

    const table_record = (try loadLiteSqlTableRecordForTargetAlloc(allocator, source, target_table, session.catalog.session())) orelse return error.InvalidSqlCatalog;
    defer table_record_mod.freeTable(allocator, table_record);
    var catalog = LiteSingleTableCatalog.initBorrowed(table_record);
    const catalog_source = catalog.iface();

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, target_table, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var unique_resolver_ctx = SourceUniqueSelectorResolverContext{ .source = source };
    var scalar_default_resolver_ctx = SourceScalarSubqueryDefaultResolverContext{
        .source = source,
        .table_name = target_table,
        .schema = schema,
    };
    const default_context = relational_rows.DefaultValueContext{
        .scalar_subquery_resolver = scalar_default_resolver_ctx.resolver(),
    };
    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog_source,
        .session = session.catalog.session(),
        .write_options = .{
            .unique_resolver = unique_resolver_ctx.resolver(),
            .default_context = default_context,
            .sync_level = try sql_adapter.sqlSyncLevelFromSession(session.catalog.session()),
        },
    });
    defer logical_plan.deinit(allocator);
    switch (logical_plan) {
        .catalog_write => {},
        else => return error.UnsupportedSqlShape,
    }

    var lowered = try sql_adapter.lowerWritePlanWithLogicalPlanAndFunctionBindingsAlloc(
        allocator,
        parsed_sql,
        &logical_plan,
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(allocator);

    var owned_insert_source_batch: relational_rows.OwnedRowsBatchRequest = undefined;
    var owns_insert_source_batch = false;
    defer if (owns_insert_source_batch) owned_insert_source_batch.deinit(allocator);
    var lowered_statement_kind: ?[]const u8 = null;
    const rows_batch = switch (lowered) {
        .insert => |*insert| &insert.batch,
        .update => |*update| &update.batch,
        .delete => |*delete| &delete.batch,
        .insert_source => |insert_source| blk: {
            if (insert_source.ctes.len != 0) return error.UnsupportedSqlShape;
            if (insert_source.literal_source_rows.len == 0) return error.UnsupportedSqlShape;
            if (insert_source.insert_source.req.source_table.len != 0 and
                !std.mem.eql(u8, insert_source.insert_source.req.source_table, target_table))
            {
                return error.UnsupportedSqlShape;
            }
            const unique_resolver = unique_resolver_ctx.resolver();
            if (!(try source.buildInsertSourceBatchAlloc(
                allocator,
                target_table,
                lite_sql_value_ref.Ref.from(@TypeOf(schema), .table_schema, &schema),
                lite_sql_value_ref.Ref.from(@TypeOf(schema), .table_schema, &schema),
                lite_sql_value_ref.Ref.from(@TypeOf(insert_source), .insert_source, &insert_source),
                lite_sql_value_ref.Ref.from(@TypeOf(unique_resolver), .unique_selector_resolver, &unique_resolver),
                lite_sql_value_ref.Ref.from(@TypeOf(default_context), .default_value_context, &default_context),
                lite_sql_value_ref.OutRef.from(relational_rows.OwnedRowsBatchRequest, .rows_batch_result, &owned_insert_source_batch),
            ))) return error.TableNotFound;
            owns_insert_source_batch = true;
            lowered_statement_kind = "insert_source";
            break :blk &owned_insert_source_batch;
        },
        else => {
            if (schema.storage_mode == .document) return error.DocumentSqlWriteUnsupported;
            return error.UnsupportedSqlShape;
        },
    };
    if (rows_batch.writes.len != 0 or rows_batch.deletes.len != 0 or rows_batch.transforms.len != 0 or rows_batch.predicates.len != 0) {
        if (!(try source.batchRows(
            allocator,
            target_table,
            lite_sql_value_ref.Ref.from(@TypeOf(rows_batch.req), .batch_request, &rows_batch.req),
        ))) return error.TableNotFound;
    }

    return try encodeRowsBatchResultAlloc(
        allocator,
        session.sessionId(),
        lowered_statement_kind orelse try liteStatementKindForParsedLogicalPlan(logical_plan, parsed_sql),
        rows_batch.*,
    );
}

fn executeReadAlloc(allocator: Allocator, source: lite_sql_source.Source, session: *Session, parsed_sql: *const sql_adapter.ParsedSql) ![]u8 {
    if (try executeLiteAntflyQueryFunctionReadAlloc(allocator, source, session, parsed_sql)) |body| return body;

    var table_names = (try sql_adapter.readSourceTableNamesFromParsedSqlAlloc(allocator, parsed_sql)) orelse return error.UnsupportedSqlShape;
    defer table_names.deinit(allocator);

    const table_record = (try loadLiteSqlTableRecordForTargetAlloc(allocator, source, table_names.left, session.catalog.session())) orelse return error.InvalidSqlCatalog;
    defer table_record_mod.freeTable(allocator, table_record);
    var catalog = LiteSingleTableCatalog.initBorrowed(table_record);
    const catalog_source = catalog.iface();

    var logical_plan = try sql_adapter.planParsedSqlWithSessionAlloc(allocator, parsed_sql, .{
        .catalog = catalog_source,
        .session = session.catalog.session(),
    });
    defer logical_plan.deinit(allocator);
    switch (logical_plan) {
        .catalog_read => {},
        else => return error.UnsupportedSqlShape,
    }

    const schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(allocator, catalog_source, table_names.left, session.catalog.session());
    defer storage_schema.freeSchema(allocator, schema);

    var lowered = try sql_adapter.lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc(
        allocator,
        parsed_sql,
        &logical_plan,
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(allocator);

    var result: ReadPlanResult = undefined;
    if (!(try source.executeReadPlanAlloc(
        allocator,
        table_names.left,
        session.catalog.session(),
        lite_sql_value_ref.Ref.from(@TypeOf(schema), .table_schema, &schema),
        lite_sql_value_ref.Ref.from(@TypeOf(lowered), .read_plan, &lowered),
        lite_sql_value_ref.OutRef.from(ReadPlanResult, .read_plan_result, &result),
    ))) return error.TableNotFound;
    defer result.deinit(allocator);

    return try encodeReadResultAlloc(allocator, session.sessionId(), try liteStatementKindForParsedLogicalPlan(logical_plan, parsed_sql), result);
}

fn executeLiteAntflyQueryFunctionReadAlloc(
    allocator: Allocator,
    source: lite_sql_source.Source,
    session: *Session,
    parsed_sql: *const sql_adapter.ParsedSql,
) !?[]u8 {
    const statement_kind = parsed_sql.readStatementKindIncludingGeneratedAst() orelse {
        if (sql_adapter.parsedSqlHasGeneratedAntflyReadSource(parsed_sql)) return error.UnsupportedSqlShape;
        return null;
    };
    return try executeAntflyQueryFunctionReadAlloc(allocator, source, session, parsed_sql, statement_kind);
}

fn executeAntflyQueryFunctionReadAlloc(
    allocator: Allocator,
    source: lite_sql_source.Source,
    session: *Session,
    parsed_sql: *const sql_adapter.ParsedSql,
    statement_kind: sql_adapter.SqlReadStatementKind,
) !?[]u8 {
    var lowered = sql_adapter.lowerAntflyQueryFunctionReadParsedSqlAlloc(allocator, null, parsed_sql) catch |err| switch (err) {
        error.UnsupportedSqlShape => if (sql_adapter.parsedSqlHasGeneratedAntflyReadSource(parsed_sql)) return err else return null,
        else => return err,
    };
    defer lowered.deinit(allocator);

    const query_json = (try source.queryJsonAlloc(
        allocator,
        lowered.table_name,
        lite_sql_value_ref.Ref.from(@TypeOf(lowered.request.req), .search_request, &lowered.request.req),
    )) orelse return error.TableNotFound;
    defer allocator.free(query_json);

    var rows = try sqlQueryFunctionRowsFromQueryResponseAlloc(allocator, query_json, lowered.projection_columns);
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
    source: lite_sql_source.Source,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
) !?table_record_mod.TableRecord {
    return try source.loadTableAlloc(
        allocator,
        table_name,
        session.currentDatabase(),
        session.primarySearchPathNamespace(),
    );
}

const NonRowResponse = struct {
    kind: []const u8,
    statement_kind: []const u8,
    session_id: u64,
    noop: bool,
    applied: ddl_result.AppliedRelationalSqlDdlRecord,
};

fn liteReadStatementKindForParsedCatalogRead(
    read: *const sql_adapter.CatalogLogicalReadPlan,
    parsed_sql: *const sql_adapter.ParsedSql,
) !sql_adapter.SqlReadStatementKind {
    const parsed_kind = parsed_sql.readStatementKindIncludingGeneratedAst();
    if (parsed_sql.generatedStatementKind() == .read) return parsed_kind orelse read.statement.readKind() orelse .query;
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

fn encodeReadResultAlloc(allocator: Allocator, session_id: u64, statement_kind: []const u8, result: ReadPlanResult) ![]u8 {
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
    const total = try queryHitsTotalValueAsU32(total_value);
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

fn queryHitsTotalValueAsU32(total_value: std.json.Value) !u32 {
    return switch (total_value) {
        .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @as(u32, @intCast(value)) else error.InvalidQueryRequest,
        .number_string => |text| std.fmt.parseUnsigned(u32, text, 10) catch error.InvalidQueryRequest,
        .object => |object| blk: {
            const value = object.get("value") orelse return error.InvalidQueryRequest;
            break :blk try queryHitsTotalValueAsU32(value);
        },
        else => error.InvalidQueryRequest,
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
    table: ?table_record_mod.TableRecord = null,
    owned: bool = false,

    fn initBorrowed(table_record: table_record_mod.TableRecord) LiteSingleTableCatalog {
        return .{ .table = table_record };
    }

    fn fromStoredTableAlloc(allocator: Allocator, source: lite_sql_source.Source) !LiteSingleTableCatalog {
        return .{
            .table = try source.loadStoredTableAlloc(allocator),
            .owned = true,
        };
    }

    fn deinit(self: *@This(), allocator: Allocator) void {
        if (self.owned) {
            if (self.table) |table| table_record_mod.freeTable(allocator, table);
        }
        self.* = undefined;
    }

    fn iface(self: *LiteSingleTableCatalog) table_catalog.SqlCatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .snapshot_alloc = snapshotAlloc,
                .free_snapshot = freeSnapshot,
            },
        };
    }

    fn snapshotAlloc(ptr: *anyopaque, _: ?*const anyopaque, _: Allocator) !table_catalog.SqlCatalogSnapshot {
        const self: *LiteSingleTableCatalog = @ptrCast(@alignCast(ptr));
        const tables = if (self.table) |*table|
            @as([*]table_record_mod.TableRecord, @ptrCast(table))[0..1]
        else
            @constCast((&[_]table_record_mod.TableRecord{})[0..]);
        return .{ .tables = tables };
    }

    fn freeSnapshot(_: *anyopaque, _: ?*const anyopaque, _: Allocator, snapshot: *table_catalog.SqlCatalogSnapshot) void {
        snapshot.* = undefined;
    }
};

const SourceUniqueSelectorResolverContext = struct {
    source: lite_sql_source.Source,

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

    fn primaryExists(ptr: *anyopaque, alloc: Allocator, table_name: []const u8, physical_key: []const u8) !bool {
        const self: *SourceUniqueSelectorResolverContext = @ptrCast(@alignCast(ptr));
        var result = (try self.source.lookupAlloc(alloc, table_name, physical_key)) orelse return false;
        defer result.deinit(alloc);
        return true;
    }

    fn lookupPrimary(ptr: *anyopaque, alloc: Allocator, table_name: []const u8, physical_key: []const u8) !?relational_rows.ResolvedPrimaryRow {
        const self: *SourceUniqueSelectorResolverContext = @ptrCast(@alignCast(ptr));
        const result = (try self.source.lookupAlloc(alloc, table_name, physical_key)) orelse return null;
        return .{
            .json = result.json,
            .version = result.version,
        };
    }
};

const SourceScalarSubqueryDefaultResolverContext = struct {
    source: lite_sql_source.Source,
    table_name: []const u8,
    schema: storage_schema.TableSchema,

    fn resolver(self: *@This()) relational_rows.ScalarSubqueryDefaultResolver {
        return .{
            .ptr = self,
            .value_json_alloc = valueJsonAlloc,
        };
    }

    fn valueJsonAlloc(
        ptr: *anyopaque,
        allocator: Allocator,
        request: relational_rows.ScalarSubqueryDefaultRequest,
    ) ![]u8 {
        const self: *SourceScalarSubqueryDefaultResolverContext = @ptrCast(@alignCast(ptr));
        var plan = try relational_rows.scalarSubqueryDefaultPlanFromQueryJsonAlloc(allocator, request.query_json);
        defer plan.deinit(allocator);
        if (!std.mem.eql(u8, plan.table_name, self.table_name)) return error.UnsupportedSqlShape;

        const query_plan = relational_rows.OwnedRowsQueryPlan{ .query = plan.query };
        var result: relational_rows.OwnedRowsQueryResult = undefined;
        if (!(try self.source.rowsQueryPlanAlloc(
            allocator,
            self.table_name,
            lite_sql_value_ref.Ref.from(@TypeOf(self.schema), .table_schema, &self.schema),
            lite_sql_value_ref.Ref.from(@TypeOf(query_plan), .rows_query_plan, &query_plan),
            lite_sql_value_ref.OutRef.from(relational_rows.OwnedRowsQueryResult, .rows_query_result, &result),
        ))) return error.TableNotFound;
        defer result.deinit(allocator);
        if (result.rows.len > 1) return error.InvalidRowsRequest;
        if (result.rows.len == 0) return try allocator.dupe(u8, "null");

        var parsed = std.json.parseFromSlice(std.json.Value, allocator, result.rows[0], .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        const value = liteJsonValueAtPath(parsed.value, plan.output_field) orelse return try allocator.dupe(u8, "null");
        return try std.json.Stringify.valueAlloc(allocator, value.*, .{});
    }
};

fn liteJsonValueAtPath(root: std.json.Value, path: []const u8) ?*const std.json.Value {
    if (root != .object) return null;
    var current: *const std.json.Value = &root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current.* != .object) return null;
        current = current.object.getPtr(part) orelse return null;
    }
    return current;
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

fn validateCatalogIdentifier(name: []const u8) !void {
    if (name.len == 0) return error.InvalidSqlRequest;
    for (name) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_')) return error.InvalidSqlRequest;
    }
}

test "lite sql statement splitter ignores quoted semicolons" {
    try std.testing.expectEqual(@as(?usize, 35), firstStatementEnd("select ';' as semi, \"x;y\" from docs;"));
    try std.testing.expectEqual(@as(?usize, 22), firstStatementEnd("select $$a;b$$ as body;"));
    try std.testing.expectEqual(@as(?usize, null), firstStatementEnd("select 'unterminated;"));
}

test "lite sql detects generated Antfly query function reads before source binding" {
    const allocator = std.testing.allocator;

    var ordinary_read = try sql_adapter.ParsedSql.initAlloc(allocator, "SELECT id FROM usage_records;");
    defer ordinary_read.deinit(allocator);
    try std.testing.expect(!sql_adapter.parsedSqlHasGeneratedAntflyReadSource(&ordinary_read));

    var query_function_read = try sql_adapter.ParsedSql.initAlloc(
        allocator,
        "SELECT _id FROM antfly.full_text_search(table_name => 'docs', index => 'docs_body_fts', field => 'body', query => 'refund', limit => 5);",
    );
    defer query_function_read.deinit(allocator);
    try std.testing.expect(sql_adapter.parsedSqlHasGeneratedAntflyReadSource(&query_function_read));

    var graph_query_function_read = try sql_adapter.ParsedSql.initAlloc(
        allocator,
        "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b', max_results => 5, limit => 5);",
    );
    defer graph_query_function_read.deinit(allocator);
    try std.testing.expect(sql_adapter.parsedSqlHasGeneratedAntflyReadSource(&graph_query_function_read));
}
