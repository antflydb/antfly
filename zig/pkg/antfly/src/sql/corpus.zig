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
const sql_adapter = @This();

const binder = @import("binder.zig");
const classifier = @import("classifier.zig");
const db_mod = @import("../storage/db/mod.zig");
const diagnostics = @import("diagnostics.zig");
const ddl_plan = @import("ddl.zig");
const logical_ddl_plan = @import("logical_ddl_plan.zig");
const lower_expr = @import("lower_expr.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const parser = @import("parser.zig");
const plan_mod = @import("plan.zig");
const query_contract = @import("../api/query_contract.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const table_catalog = @import("../api/table_catalog.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

pub const SqlValue = value_mod.SqlValue;
pub const app_parity_default_schema_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"organization_id":{"type":"keyword"},"cloud_instance_id":{"type":"keyword"},"user_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"},"status":{"type":"keyword","default":"active"},"metric_type":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"},"rating_status":{"type":"keyword"},"product_family":{"type":"keyword"},"enabled":{"type":"boolean"},"amount":{"type":"numeric"},"quantity":{"type":"numeric"},"rated_quantity":{"type":"numeric"},"priority":{"type":"numeric"},"created_at":{"type":"numeric"},"updated_at_ns":{"type":"numeric"},"recorded_at":{"type":"numeric"},"expires_at":{"type":"numeric"},"billing_cycle_start":{"type":"numeric"},"bucket_start":{"type":"numeric"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
;
const LoweredAggregate = plan_mod.LoweredAggregate;
const LoweredAggregatePlan = plan_mod.LoweredAggregatePlan;
const LoweredJoin = plan_mod.LoweredJoin;
const LoweredLateralPlan = plan_mod.LoweredLateralPlan;
const LoweredReadPlan = plan_mod.LoweredReadPlan;
const LoweredRecursiveCteMemberPlan = plan_mod.LoweredRecursiveCteMemberPlan;
const LoweredRecursiveCtePlan = plan_mod.LoweredRecursiveCtePlan;
const LoweredSetOperationPlan = plan_mod.LoweredSetOperationPlan;
const LoweredWindowPlan = plan_mod.LoweredWindowPlan;
const aggregateDescendingPercentileCount = lower_expr.aggregateDescendingPercentileCount;
const aggregateFilterExpressionArrayCount = lower_expr.aggregateFilterExpressionArrayCount;
const aggregateFilterExpressionCount = lower_expr.aggregateFilterExpressionCount;
const aggregateFilterGroupCount = lower_expr.aggregateFilterGroupCount;
const aggregateFilterJsonAccessCount = lower_expr.aggregateFilterJsonAccessCount;
const aggregateFilterStructuredAccessCount = lower_expr.aggregateFilterStructuredAccessCount;
const aggregateInputExpressionCount = lower_expr.aggregateInputExpressionCount;
const aggregateModeCount = lower_expr.aggregateModeCount;
const aggregatePercentileArrayCount = lower_expr.aggregatePercentileArrayCount;
const expressionOrderCount = lower_expr.expressionOrderCount;
const sqlRowClaimFingerprintName = lower_expr.sqlRowClaimFingerprintName;
const sourceQueryUsesExtendedPredicates = lower_expr.sourceQueryUsesExtendedPredicates;
const windowDefaultCount = lower_expr.windowDefaultCount;
const windowFilterAccessCount = lower_expr.windowFilterAccessCount;
const windowFilterExpressionCount = lower_expr.windowFilterExpressionCount;
const windowFilterGroupCount = lower_expr.windowFilterGroupCount;
const windowFilterPredicateCount = lower_expr.windowFilterPredicateCount;
const windowFrameSignature = lower_expr.windowFrameSignature;
const windowValueExpressionCount = lower_expr.windowValueExpressionCount;

pub const UnsupportedPlanFamily = enum {
    query,
    read,
    ddl,
    write,
    insert,
    update,
    update_source,
    delete,
    update_joined_source,
    delete_joined_source,
    merge_mutation,
};

pub const AppParityCorpusPlanFamily = enum {
    ddl,
    query_function,
    read,
    query,
    aggregate,
    join,
    lateral,
    window,
    explain,
    relation_population,
    insert,
    insert_source,
    recursive_insert_source,
    update,
    delete,
    update_source,
    delete_source,
    truncate_source,
    update_joined_source,
    delete_joined_source,
    merge_mutation,
    adapter_noop_ddl,
    invalid_read,
    invalid_insert,
    invalid_update,
    invalid_delete,
    invalid_update_source,
    invalid_update_joined_source,
    unsupported,
    unsupported_read,
    unsupported_ddl,
    unsupported_write,
    unsupported_insert,
    unsupported_update,
    unsupported_update_source,
    unsupported_delete,
    unsupported_update_joined_source,
    unsupported_delete_joined_source,
    unsupported_merge_mutation,
};

pub const AppParityDdlTag = enum {
    create_table,
    table_clone,
    create_view,
    rename_view,
    drop_view,
    create_materialized_view,
    refresh_materialized_view,
    drop_materialized_view,
    relation_lifetime,
    create_enum_type,
    add_enum_value,
    drop_enum_type,
    create_domain,
    alter_domain,
    drop_domain,
    create_sequence,
    alter_sequence,
    drop_sequence,
    identity_allocator,
    create_schema_namespace,
    rename_schema_namespace,
    drop_schema_namespace,
    create_extension,
    alter_extension_update,
    drop_extension,
    create_function,
    drop_function,
    create_procedure,
    drop_procedure,
    call_procedure,
    create_role,
    alter_role,
    drop_role,
    grant_privilege,
    revoke_privilege,
    copy_from,
    copy_to,
    create_partitioned_table,
    create_table_partition,
    attach_table_partition,
    detach_table_partition,
    enable_row_security,
    disable_row_security,
    create_row_policy,
    alter_row_policy,
    drop_row_policy,
    create_database,
    alter_database,
    drop_database,
    create_tablespace,
    rename_tablespace,
    drop_tablespace,
    listen_notification,
    notify_notification,
    unlisten_notification,
    create_publication,
    alter_publication,
    drop_publication,
    create_subscription,
    alter_subscription,
    drop_subscription,
    create_collation,
    rename_collation,
    drop_collation,
    create_operator,
    drop_operator,
    create_aggregate,
    drop_aggregate,
    create_cast,
    drop_cast,
    vacuum_maintenance,
    analyze_maintenance,
    reindex_maintenance,
    cluster_maintenance,
    prepare_statement,
    execute_statement,
    deallocate_statement,
    prepare_transaction,
    commit_prepared,
    rollback_prepared,
    declare_cursor,
    fetch_cursor,
    close_cursor,
    savepoint_transaction,
    release_savepoint,
    rollback_to_savepoint,
    comment_metadata,
    table_lock,
    constraint_mode,
    transaction_mode,
    advisory_lock,
    set_search_path,
    set_setting,
    reset_search_path,
    reset_setting,
    show_search_path,
    discard_all,
    create_index,
    drop_index,
    drop_table,
    alter_table,
    create_update_policy,
};

pub const AppParityPlanSummary = struct {
    ddl_tag: ?AppParityDdlTag = null,
    table_name: ?[]const u8 = null,
    ctes: ?usize = null,
    predicates: ?usize = null,
    array_any: ?usize = null,
    in_predicates: ?usize = null,
    json_path_eq: ?usize = null,
    json_contains: ?usize = null,
    json_path_exists: ?usize = null,
    array_contains: ?usize = null,
    array_eq: ?usize = null,
    text_patterns: ?usize = null,
    access_or_predicates: ?usize = null,
    access_not_predicates: ?usize = null,
    expression_predicates: ?usize = null,
    expression_or_predicates: ?usize = null,
    expression_not_predicates: ?usize = null,
    expression_array_contains: ?usize = null,
    select: ?usize = null,
    select_all: ?bool = null,
    distinct_on: ?usize = null,
    order_by: ?usize = null,
    limit: ?u32 = null,
    offset: ?u32 = null,
    right_offset: ?u32 = null,
    group_by: ?usize = null,
    group_expressions: ?usize = null,
    aggregations: ?usize = null,
    filter_groups: ?usize = null,
    having: ?usize = null,
    having_expressions: ?usize = null,
    having_any: ?usize = null,
    having_not: ?usize = null,
    operations: ?usize = null,
    source_assignments: ?usize = null,
    patch_expressions: ?usize = null,
    increment_expressions: ?usize = null,
    json_set_expressions: ?usize = null,
    returning: ?usize = null,
    returning_all: ?bool = null,
    conflict_where: ?bool = null,
    join_on: ?usize = null,
    matched_predicates: ?usize = null,
    matched_delete: ?bool = null,
    matched_do_nothing: ?bool = null,
    not_matched_predicates: ?usize = null,
    not_matched_do_nothing: ?bool = null,
    join_select: ?usize = null,
    lateral_correlations: ?usize = null,
    windows: ?usize = null,
    row_claim_skip_locked: ?bool = null,
    temporal_periods: ?usize = null,
    temporal_primary_key: ?bool = null,
    temporal_unique: ?usize = null,
    temporal_foreign_keys: ?usize = null,
    explain_subject: ?[]const u8 = null,
    explain_inner_kind: ?[]const u8 = null,
    explain_options: ?bool = null,
    explain_analyze: ?bool = null,
    explain_buffers: ?bool = null,
    explain_timing: ?bool = null,
    explain_summary: ?bool = null,
    explain_settings: ?bool = null,
    explain_wal: ?bool = null,
};

pub fn summaryHasFields(summary: AppParityPlanSummary) bool {
    return summary.ddl_tag != null or
        summary.table_name != null or
        summary.ctes != null or
        summary.predicates != null or
        summary.array_any != null or
        summary.in_predicates != null or
        summary.json_path_eq != null or
        summary.json_contains != null or
        summary.json_path_exists != null or
        summary.array_contains != null or
        summary.array_eq != null or
        summary.text_patterns != null or
        summary.access_or_predicates != null or
        summary.access_not_predicates != null or
        summary.expression_predicates != null or
        summary.expression_or_predicates != null or
        summary.expression_not_predicates != null or
        summary.expression_array_contains != null or
        summary.select != null or
        summary.select_all != null or
        summary.distinct_on != null or
        summary.order_by != null or
        summary.limit != null or
        summary.offset != null or
        summary.right_offset != null or
        summary.group_by != null or
        summary.group_expressions != null or
        summary.aggregations != null or
        summary.filter_groups != null or
        summary.having != null or
        summary.having_expressions != null or
        summary.having_any != null or
        summary.having_not != null or
        summary.operations != null or
        summary.source_assignments != null or
        summary.patch_expressions != null or
        summary.increment_expressions != null or
        summary.json_set_expressions != null or
        summary.returning != null or
        summary.returning_all != null or
        summary.conflict_where != null or
        summary.join_on != null or
        summary.matched_predicates != null or
        summary.matched_delete != null or
        summary.matched_do_nothing != null or
        summary.not_matched_predicates != null or
        summary.not_matched_do_nothing != null or
        summary.join_select != null or
        summary.lateral_correlations != null or
        summary.windows != null or
        summary.row_claim_skip_locked != null or
        summary.temporal_periods != null or
        summary.temporal_primary_key != null or
        summary.temporal_unique != null or
        summary.temporal_foreign_keys != null or
        summary.explain_subject != null or
        summary.explain_inner_kind != null or
        summary.explain_options != null or
        summary.explain_analyze != null or
        summary.explain_buffers != null or
        summary.explain_timing != null or
        summary.explain_summary != null or
        summary.explain_settings != null or
        summary.explain_wal != null;
}

pub fn summaryHasNonTableFields(summary: AppParityPlanSummary) bool {
    var without_table = summary;
    without_table.table_name = null;
    return summaryHasFields(without_table);
}

pub const AppParityCorpusEntry = struct {
    name: []const u8,
    sql: []const u8,
    family: AppParityCorpusPlanFamily,
    params: []const SqlValue = &.{},
    summary: AppParityPlanSummary = .{},
    plan: []const u8 = "",
    classification_reason: []const u8 = "",
    apply_setup_sql: []const []const u8 = &.{},
    returning_rows: []const []const u8 = &.{},
    applied_plan: []const u8 = "",
    execution_plan: []const u8 = "",
    resolver_row_json: []const u8 = "",
    resolver_version: u64 = 0,
    resolver_exists: ?bool = null,
    source_schema_json: []const u8 = "",
    catalog_tables: []const AppParityCatalogTable = &.{},
};

pub const AppParityCatalogTable = struct {
    name: []const u8,
    schema_json: []const u8,
};

pub const AppParitySourceSchemaCatalog = struct {
    single_table: [1]metadata_table_manager.TableRecord = undefined,
    owned_tables: []metadata_table_manager.TableRecord = &.{},
    owned_source_table_name: []u8 = &.{},
    table_count: usize = 0,

    pub fn init(table_name: []const u8, source_schema_json: []const u8) @This() {
        return .{ .single_table = .{
            .{ .table_id = 90_001, .name = table_name, .placement_role = "data", .schema_json = source_schema_json },
        }, .table_count = 1 };
    }

    pub fn initSourceSchemaAlloc(alloc: std.mem.Allocator, table_name: []const u8, source_schema_json: []const u8) !@This() {
        const owned_table_name = try alloc.dupe(u8, table_name);
        errdefer alloc.free(owned_table_name);
        return .{
            .single_table = .{
                .{ .table_id = 90_001, .name = owned_table_name, .placement_role = "data", .schema_json = source_schema_json },
            },
            .owned_source_table_name = owned_table_name,
            .table_count = 1,
        };
    }

    pub fn initCatalogTablesAlloc(alloc: std.mem.Allocator, catalog_tables: []const AppParityCatalogTable) !@This() {
        if (catalog_tables.len == 0) return error.InvalidSqlCatalog;
        var records = try alloc.alloc(metadata_table_manager.TableRecord, catalog_tables.len);
        errdefer alloc.free(records);
        for (catalog_tables, 0..) |table, i| {
            records[i] = .{
                .table_id = 90_001 + @as(u64, @intCast(i)),
                .name = table.name,
                .placement_role = "data",
                .schema_json = table.schema_json,
            };
        }
        return .{ .owned_tables = records, .table_count = catalog_tables.len };
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.owned_tables.len > 0) alloc.free(self.owned_tables);
        if (self.owned_source_table_name.len > 0) alloc.free(self.owned_source_table_name);
        self.* = undefined;
    }

    pub fn iface(self: *@This()) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = self.tables(),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

    fn tables(self: *@This()) []metadata_table_manager.TableRecord {
        if (self.owned_tables.len > 0) return self.owned_tables;
        return self.single_table[0..self.table_count];
    }
};

pub fn appParityEntryHasCatalogSchemas(entry: AppParityCorpusEntry) bool {
    return entry.source_schema_json.len > 0 or entry.catalog_tables.len > 0;
}

pub fn appParityCatalogForEntryAlloc(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !?AppParitySourceSchemaCatalog {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, entry.sql);
    defer parsed_sql.deinit(alloc);
    return try appParityCatalogForEntryParsedSqlAlloc(alloc, entry, &parsed_sql);
}

pub fn appParityCatalogForEntryParsedSqlAlloc(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !?AppParitySourceSchemaCatalog {
    if (entry.source_schema_json.len > 0 and entry.catalog_tables.len > 0) return error.InvalidSqlCatalog;
    if (entry.catalog_tables.len > 0) {
        return try AppParitySourceSchemaCatalog.initCatalogTablesAlloc(alloc, entry.catalog_tables);
    }
    const source_table_name = (try appParitySourceTableNameParsedSqlAlloc(alloc, entry, parsed_sql)) orelse return null;
    defer alloc.free(@constCast(source_table_name));
    return try AppParitySourceSchemaCatalog.initSourceSchemaAlloc(alloc, source_table_name, entry.source_schema_json);
}

fn appParityBindingCoverageCatalogForEntryParsedSqlAlloc(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !?AppParitySourceSchemaCatalog {
    var table_names = try appParityBindingCoverageTableNamesAlloc(alloc, entry, parsed_sql) orelse return null;
    defer table_names.deinit(alloc);
    var catalog_tables = std.ArrayListUnmanaged(AppParityCatalogTable).empty;
    defer catalog_tables.deinit(alloc);
    try appendAppParityBindingCoverageCatalogTable(alloc, &catalog_tables, entry, table_names.target);
    if (table_names.source) |source| {
        if (!std.mem.eql(u8, table_names.target, source)) {
            try appendAppParityBindingCoverageCatalogTable(alloc, &catalog_tables, entry, source);
        }
    }
    return try AppParitySourceSchemaCatalog.initCatalogTablesAlloc(alloc, catalog_tables.items);
}

const AppParityBindingCoverageTableNames = struct {
    target: []const u8,
    source: ?[]const u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.target));
        if (self.source) |source| alloc.free(@constCast(source));
        self.* = undefined;
    }
};

fn appParityBindingCoverageTableNamesAlloc(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !?AppParityBindingCoverageTableNames {
    if (appParityPreparedInnerStatementRange(parsed_sql)) |inner| {
        var inner_parsed = try tokenized.ParsedSql.initChildStatementAlloc(alloc, parsed_sql, inner.start, inner.end);
        defer inner_parsed.deinit(alloc);
        return try appParityBindingCoverageTableNamesAlloc(alloc, entry, &inner_parsed);
    }

    switch (entry.family) {
        .read, .query, .aggregate, .join, .lateral, .window => {
            const resolved = (try binder.readSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) orelse return null;
            var tables = resolved;
            defer tables.deinit(alloc);
            return .{
                .target = try alloc.dupe(u8, tables.left),
                .source = try alloc.dupe(u8, tables.source),
            };
        },
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => {
            const target = try binder.writeTargetTableNameFromParsedSqlAlloc(alloc, parsed_sql);
            errdefer alloc.free(target);
            const source = try appParityBindingCoverageWriteSourceTableNameAlloc(alloc, entry, parsed_sql);
            errdefer if (source) |name| alloc.free(@constCast(name));
            return .{
                .target = target,
                .source = source,
            };
        },
        else => return null,
    }
}

const AppParityPreparedInnerStatementRange = struct {
    start: usize,
    end: usize,
};

fn appParityPreparedInnerStatementRange(parsed_sql: *const tokenized.ParsedSql) ?AppParityPreparedInnerStatementRange {
    const generated_raw = parsed_sql.generated_statement orelse return null;
    const ast = generated_raw.ast orelse return null;
    const prepared = switch (ast) {
        .prepared => |prepared_ast| prepared_ast,
        else => return null,
    };
    if (prepared.kind != .prepare) return null;
    const inner = prepared.inner_statement_tokens orelse return null;
    return .{ .start = inner.start, .end = inner.end };
}

fn appParityBindingCoverageWriteSourceTableNameAlloc(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !?[]const u8 {
    switch (entry.family) {
        .insert_source => {
            if (try binder.insertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved| {
                var tables = resolved;
                defer tables.deinit(alloc);
                return try alloc.dupe(u8, tables.source);
            }
            return null;
        },
        .recursive_insert_source => {
            if (try binder.recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved| {
                var tables = resolved;
                defer tables.deinit(alloc);
                return try alloc.dupe(u8, tables.source);
            }
            return null;
        },
        .update_joined_source, .delete_joined_source, .merge_mutation => {
            if (try binder.joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) |resolved| {
                var tables = resolved;
                defer tables.deinit(alloc);
                return try alloc.dupe(u8, tables.source);
            }
            return null;
        },
        else => return null,
    }
}

fn appendAppParityBindingCoverageCatalogTable(
    alloc: std.mem.Allocator,
    catalog_tables: *std.ArrayListUnmanaged(AppParityCatalogTable),
    entry: AppParityCorpusEntry,
    table_name: []const u8,
) !void {
    for (catalog_tables.items) |table| {
        if (std.mem.eql(u8, table.name, table_name)) return;
    }
    try catalog_tables.append(alloc, .{
        .name = table_name,
        .schema_json = appParityBindingCoverageSchemaJsonForTable(entry, table_name),
    });
}

fn appParityBindingCoverageSchemaJsonForTable(entry: AppParityCorpusEntry, table_name: []const u8) []const u8 {
    for (entry.catalog_tables) |table| {
        if (std.mem.eql(u8, table.name, table_name)) return table.schema_json;
    }
    if (entry.source_schema_json.len > 0 and !std.mem.eql(u8, table_name, "usage_records")) return entry.source_schema_json;
    return app_parity_default_schema_json;
}

pub fn appParitySourceTableNameAlloc(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !?[]const u8 {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, entry.sql);
    defer parsed_sql.deinit(alloc);
    return try appParitySourceTableNameParsedSqlAlloc(alloc, entry, &parsed_sql);
}

pub fn appParitySourceTableNameParsedSqlAlloc(
    alloc: std.mem.Allocator,
    entry: AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
) !?[]const u8 {
    if (entry.source_schema_json.len == 0) return null;

    switch (entry.family) {
        .insert_source => {
            var tables = (try binder.insertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) orelse return error.InvalidSqlCatalog;
            defer tables.deinit(alloc);
            return try alloc.dupe(u8, tables.source);
        },
        .recursive_insert_source => {
            var tables = (try binder.recursiveInsertSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) orelse return error.InvalidSqlCatalog;
            defer tables.deinit(alloc);
            return try alloc.dupe(u8, tables.source);
        },
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => {
            var tables = (try binder.joinedWriteSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) orelse return error.InvalidSqlCatalog;
            defer tables.deinit(alloc);
            return try alloc.dupe(u8, tables.source);
        },
        .read,
        .join,
        .lateral,
        => {
            var tables = (try binder.readSourceTableNamesFromParsedSqlAlloc(alloc, parsed_sql)) orelse return error.InvalidSqlCatalog;
            defer tables.deinit(alloc);
            return try alloc.dupe(u8, tables.source);
        },
        else => return error.InvalidSqlCatalog,
    }
}

pub const app_parity_fixture_format: u64 = 1;
pub const app_parity_coverage_fixture_format: u64 = 1;
pub const app_parity_coverage_regression_requirement_fixture_format: u64 = 1;
pub const app_parity_native_requirement_fixture_format: u64 = 1;
pub const app_parity_resolved_requirement_fixture_format: u64 = 1;
pub const app_parity_summary_assertion_fixture_format: u64 = 1;
pub const app_parity_summary_regression_fixture_format: u64 = 1;
pub const app_parity_source_corpus_format: u64 = 1;
pub const sql_adapter_edge_case_fixture_format: u64 = 1;
pub const sql_adapter_edge_coverage_fixture_format: u64 = 1;

pub const AppParityFixtureRoot = struct {
    fixture_format: u64,
    source_sha256: []const u8,
    source_entry_count: usize,
    entry_count: usize,
    skipped_entries: []const []const u8,
    schema_json: []const u8,
    entries: []const std.json.Value,
};

pub const AppParitySourceCorpusRoot = struct {
    source_format: u64,
    entries: []const AppParityCorpusEntry,
};

pub const AppParityCoverageRequirementsRoot = struct {
    coverage_format: u64,
    required: []const []const u8,
};

pub const AppParityCoverageRegressionRequirementsRoot = struct {
    coverage_format: u64,
    required: []const []const u8,
};

pub const AppParityNativeRequirementRoot = struct {
    requirement_format: u64,
    required: []const []const u8,
};

pub const AppParityResolvedRequirement = struct {
    reason: []const u8,
    coverage: []const []const u8,
};

pub const AppParityResolvedRequirementRoot = struct {
    resolution_format: u64,
    resolved: []const AppParityResolvedRequirement,
};

pub const AppParityNativeRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParityNativeRequirementRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeNativeRequirementRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub const AppParityResolvedRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParityResolvedRequirementRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeResolvedRequirementRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub const AppParityCoverageRegressionRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParityCoverageRegressionRequirementsRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeCoverageRegressionRequirementsRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub const AppParitySummaryAssertionRequirementsRoot = struct {
    assertion_format: u64,
    required: []const []const u8,
};

pub const AppParitySummaryAssertionRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParitySummaryAssertionRequirementsRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeSummaryAssertionRequirementsRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub const AppParityExternalSourceCorpus = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParitySourceCorpusRoot,
    source_sha256: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.source_sha256);
        freeSourceCorpusRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub fn parseAppParityExternalSourceCorpusAlloc(alloc: std.mem.Allocator) !AppParityExternalSourceCorpus {
    const source_json = @embedFile("../api/fixtures/sql_api_parity_source_corpus.json");
    const source_sha256 = try sourceCorpusSha256HexAlloc(alloc, source_json);
    errdefer alloc.free(source_sha256);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
    errdefer parsed.deinit();

    const root = try parseSourceCorpusRootAlloc(alloc, parsed.value);
    errdefer freeSourceCorpusRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
        .source_sha256 = source_sha256,
    };
}

pub const AppParityCoverageRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: AppParityCoverageRequirementsRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeCoverageRequirementsRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub fn parseAppParityCoverageRequirementsAlloc(alloc: std.mem.Allocator) !AppParityCoverageRequirements {
    const coverage_json = @embedFile("../api/fixtures/sql_api_required_coverage.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, coverage_json, .{});
    errdefer parsed.deinit();

    const root = try parseCoverageRequirementsRootAlloc(alloc, parsed.value);
    errdefer freeCoverageRequirementsRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

pub fn parseNativeRequirementRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !AppParityNativeRequirementRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "requirement_format", "required" });
    const requirement_format = try fixtureJsonOptionalU64(root, "requirement_format", 0);
    if (requirement_format != app_parity_native_requirement_fixture_format) return error.TestUnexpectedResult;
    const required = try parseFixtureStringListAlloc(alloc, root, "required");
    errdefer if (required.len > 0) alloc.free(required);

    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    for (required, 0..) |name, i| {
        const reason = diagnostics.classificationReasonFromToken(name) orelse return error.TestUnexpectedResult;
        if (name.len == 0 or seen.contains(name) or !diagnostics.classificationReasonIsUnsupportedRequirement(reason)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, required[i - 1], name)) return error.TestUnexpectedResult;
        try seen.put(alloc, name, {});
    }

    return .{
        .requirement_format = requirement_format,
        .required = required,
    };
}

pub fn freeNativeRequirementRoot(
    alloc: std.mem.Allocator,
    root: AppParityNativeRequirementRoot,
) void {
    if (root.required.len > 0) alloc.free(root.required);
}

pub fn parseAppParityNativeRequirementsAlloc(alloc: std.mem.Allocator) !AppParityNativeRequirements {
    const requirement_json = @embedFile("../api/fixtures/sql_api_required_native_requirements.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, requirement_json, .{});
    errdefer parsed.deinit();

    const root = try parseNativeRequirementRootAlloc(alloc, parsed.value);
    errdefer freeNativeRequirementRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

pub fn parseResolvedRequirementRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !AppParityResolvedRequirementRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "resolution_format", "resolved" });
    const resolution_format = try fixtureJsonOptionalU64(root, "resolution_format", 0);
    if (resolution_format != app_parity_resolved_requirement_fixture_format) return error.TestUnexpectedResult;
    const resolved_values = switch (root.get("resolved") orelse return error.TestUnexpectedResult) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    if (resolved_values.items.len == 0) return error.TestUnexpectedResult;

    var resolved = std.ArrayListUnmanaged(AppParityResolvedRequirement).empty;
    errdefer {
        for (resolved.items) |item| if (item.coverage.len > 0) alloc.free(item.coverage);
        resolved.deinit(alloc);
    }
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    for (resolved_values.items, 0..) |item_value, i| {
        const item_object = try fixtureJsonObject(item_value);
        try fixtureRequireOnlyKeys(item_object, &.{ "reason", "coverage" });
        const reason = try fixtureJsonOptionalString(item_object, "reason", "");
        const parsed_reason = diagnostics.classificationReasonFromToken(reason) orelse return error.TestUnexpectedResult;
        if (reason.len == 0 or seen.contains(reason) or !diagnostics.classificationReasonIsUnsupportedRequirement(parsed_reason)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, resolved.items[i - 1].reason, reason)) return error.TestUnexpectedResult;

        const coverage = try parseFixtureStringListAlloc(alloc, item_object, "coverage");
        errdefer if (coverage.len > 0) alloc.free(coverage);
        if (coverage.len == 0) return error.TestUnexpectedResult;
        var coverage_seen = std.StringHashMapUnmanaged(void){};
        defer coverage_seen.deinit(alloc);
        for (coverage, 0..) |name, coverage_index| {
            if (name.len == 0 or coverage_seen.contains(name) or !appParityCoverageRequirementKnown(name)) return error.TestUnexpectedResult;
            if (coverage_index > 0 and !std.mem.lessThan(u8, coverage[coverage_index - 1], name)) return error.TestUnexpectedResult;
            try coverage_seen.put(alloc, name, {});
        }

        try seen.put(alloc, reason, {});
        try resolved.append(alloc, .{
            .reason = reason,
            .coverage = coverage,
        });
    }

    return .{
        .resolution_format = resolution_format,
        .resolved = try resolved.toOwnedSlice(alloc),
    };
}

pub fn freeResolvedRequirementRoot(
    alloc: std.mem.Allocator,
    root: AppParityResolvedRequirementRoot,
) void {
    for (root.resolved) |item| {
        if (item.coverage.len > 0) alloc.free(item.coverage);
    }
    if (root.resolved.len > 0) alloc.free(root.resolved);
}

pub fn parseAppParityResolvedRequirementsAlloc(alloc: std.mem.Allocator) !AppParityResolvedRequirements {
    const requirement_json = @embedFile("../api/fixtures/sql_api_resolved_native_requirements.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, requirement_json, .{});
    errdefer parsed.deinit();

    const root = try parseResolvedRequirementRootAlloc(alloc, parsed.value);
    errdefer freeResolvedRequirementRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

const app_parity_summary_regression_assertions = [_][]const u8{
    "access_matches",
    "aggregate_matches_plan",
    "allows_access",
    "allows_aggregate",
    "allows_conflict_where",
    "allows_full_query_output",
    "allows_join_on",
    "allows_join_select",
    "allows_lateral",
    "allows_merge_arm",
    "allows_mutation_transform",
    "allows_pagination",
    "allows_predicate",
    "allows_returning",
    "allows_row_claim",
    "allows_source_assignments",
    "allows_window",
    "conflict_where_matches_false",
    "ddl_predicate_matches",
    "ddl_select_matches",
    "explain_plan_has_read_kind",
    "explain_plan_has_write_kind",
    "explain_write_inner_insert",
    "explain_write_inner_merge",
    "full_query_output_matches",
    "join_on_matches_1",
    "join_select_matches_1",
    "lateral_matches",
    "merge_arm_matches_plan",
    "operations_matches_2",
    "operations_matches_3",
    "pagination_matches",
    "plan_analyze_true_token",
    "plan_costs_present",
    "plan_format_present",
    "plan_verbose_present",
    "predicate_matches",
    "read_plan_has_query_prefix",
    "returning_all_matches_true",
    "returning_matches_1",
    "row_claim_matches",
    "select_matches",
    "source_assignments_match_2",
    "transform_matches_plan",
    "window_matches_1",
};

fn appParitySummaryRegressionAssertionKnown(name: []const u8) bool {
    for (app_parity_summary_regression_assertions) |known| {
        if (std.mem.eql(u8, name, known)) return true;
    }
    return false;
}

pub fn parseSummaryAssertionRequirementsRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !AppParitySummaryAssertionRequirementsRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "assertion_format", "required" });
    const assertion_format = try fixtureJsonOptionalU64(root, "assertion_format", 0);
    if (assertion_format != app_parity_summary_assertion_fixture_format) return error.TestUnexpectedResult;
    const required = try parseFixtureStringListAlloc(alloc, root, "required");
    errdefer if (required.len > 0) alloc.free(required);
    if (required.len == 0) return error.TestUnexpectedResult;

    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    for (required, 0..) |name, i| {
        if (name.len == 0 or seen.contains(name) or !appParitySummaryRegressionAssertionKnown(name)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, required[i - 1], name)) return error.TestUnexpectedResult;
        try seen.put(alloc, name, {});
    }
    for (app_parity_summary_regression_assertions) |known| {
        if (!seen.contains(known)) return error.TestUnexpectedResult;
    }

    return .{
        .assertion_format = assertion_format,
        .required = required,
    };
}

pub fn freeSummaryAssertionRequirementsRoot(
    alloc: std.mem.Allocator,
    root: AppParitySummaryAssertionRequirementsRoot,
) void {
    if (root.required.len > 0) alloc.free(root.required);
}

pub fn parseAppParitySummaryAssertionRequirementsAlloc(alloc: std.mem.Allocator) !AppParitySummaryAssertionRequirements {
    const assertion_json = @embedFile("../api/fixtures/sql_api_summary_required_assertions.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, assertion_json, .{});
    errdefer parsed.deinit();

    const root = try parseSummaryAssertionRequirementsRootAlloc(alloc, parsed.value);
    errdefer freeSummaryAssertionRequirementsRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

pub const SqlAdapterEdgeCaseAction = enum {
    select,
    update,
    delete,
    insert,
    ddl,
    classify_write,
    write_plan,
};

pub const SqlAdapterEdgeCaseDdlTag = enum {
    create_table,
};

pub const SqlAdapterEdgeCase = struct {
    name: []const u8,
    action: SqlAdapterEdgeCaseAction,
    sql: []const u8,
    coverage: []const []const u8 = &.{},
    expected_error: []const u8 = "",
    expected_table: ?[]const u8 = null,
    expected_predicates: ?usize = null,
    expected_first_predicate_field: ?[]const u8 = null,
    expected_first_predicate_value_json: ?[]const u8 = null,
    expected_transformed: ?u32 = null,
    expected_write_kind: ?classifier.SqlWriteStatementKind = null,
    expected_inserted: ?u32 = null,
    expected_ddl_tag: ?SqlAdapterEdgeCaseDdlTag = null,
    expected_if_not_exists: ?bool = null,
    omit_resolver: bool = false,
    params: []const SqlValue = &.{},
};

pub const SqlAdapterEdgeCaseRoot = struct {
    edge_case_format: u64,
    cases: []const SqlAdapterEdgeCase,
};

pub const SqlAdapterEdgeCaseCoverageRequirementsRoot = struct {
    coverage_format: u64,
    required: []const []const u8,
};

pub const SqlAdapterEdgeCaseCoverageRequirements = struct {
    parsed: std.json.Parsed(std.json.Value),
    root: SqlAdapterEdgeCaseCoverageRequirementsRoot,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeSqlAdapterEdgeCaseCoverageRequirementsRoot(alloc, self.root);
        self.parsed.deinit();
    }
};

pub const SqlAdapterEdgeCaseCoverage = struct {
    action_classify_write: bool = false,
    action_ddl: bool = false,
    action_delete: bool = false,
    action_insert: bool = false,
    action_select: bool = false,
    action_update: bool = false,
    action_write_plan: bool = false,
    aggregate_duplicate_output_name_rejection: bool = false,
    cte_write_classification: bool = false,
    cte_write_plan_rejection: bool = false,
    expected_error_invalid_sql_catalog: bool = false,
    expected_error_unsupported_rows_query: bool = false,
    expected_error_unsupported_rows_selector: bool = false,
    expected_error_unsupported_sql_shape: bool = false,
    expression_conflict_target_rejection: bool = false,
    fail_closed_unterminated_comment: bool = false,
    malformed_placeholder_suffix: bool = false,
    mutation_source_boundary: bool = false,
    point_lowerer_boundary: bool = false,
    preserved_comments_success: bool = false,
    typed_create_table_ddl: bool = false,
    typed_insert_batch: bool = false,
    typed_select_predicate: bool = false,
    typed_update_mutation: bool = false,
    write_kind_delete: bool = false,
    write_kind_delete_source: bool = false,
    write_kind_delete_joined_source: bool = false,
    write_kind_update: bool = false,
    write_kind_update_source: bool = false,
    write_kind_update_joined_source: bool = false,

    pub fn observe(self: *@This(), edge_case: SqlAdapterEdgeCase) !void {
        for (edge_case.coverage) |name| {
            try sqlAdapterEdgeCaseCoverageSet(self, name);
        }
    }
};

pub const AppParityFixtureEncodedEntry = struct {
    entry: AppParityCorpusEntry,
    applied_plan: []const u8 = "",
};

pub const AppParityFixtureGateMode = union(enum) {
    none,
    check: []const u8,
    promote: []const u8,
};

pub fn fixtureJsonObject(value: std.json.Value) !std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.TestUnexpectedResult,
    };
}

fn fixtureStringIn(field: []const u8, allowed: []const []const u8) bool {
    for (allowed) |item| {
        if (std.mem.eql(u8, field, item)) return true;
    }
    return false;
}

pub fn fixtureRequireOnlyKeys(object: std.json.ObjectMap, allowed: []const []const u8) !void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!fixtureStringIn(entry.key_ptr.*, allowed)) return error.TestUnexpectedResult;
    }
}

pub fn fixtureJsonString(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalString(object: std.json.ObjectMap, field: []const u8, default: []const u8) ![]const u8 {
    return if (object.get(field)) |value| try fixtureJsonString(value) else default;
}

pub fn fixtureJsonOptionalStringField(object: std.json.ObjectMap, field: []const u8) !?[]const u8 {
    return if (object.get(field)) |value| try fixtureJsonString(value) else null;
}

pub fn fixtureJsonOptionalBool(object: std.json.ObjectMap, field: []const u8) !?bool {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .bool => |flag| flag,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalUsize(object: std.json.ObjectMap, field: []const u8) !?usize {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalU32(object: std.json.ObjectMap, field: []const u8) !?u32 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .integer => |number| if (number >= 0 and number <= std.math.maxInt(u32)) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalU64(object: std.json.ObjectMap, field: []const u8, default: u64) !u64 {
    const value = object.get(field) orelse return default;
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn sourceCorpusSha256HexAlloc(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const out = try alloc.alloc(u8, digest.len * 2);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn validateFixtureSourceSha256(text: []const u8) !void {
    if (text.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return error.TestUnexpectedResult;
    for (text) |ch| {
        const lower = ch >= 'a' and ch <= 'f';
        const digit = ch >= '0' and ch <= '9';
        if (!lower and !digit) return error.TestUnexpectedResult;
    }
}

pub fn parseFixtureRootAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParityFixtureRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "fixture_format", "source_sha256", "source_entry_count", "entry_count", "skipped_entries", "schema_json", "entries" });
    const fixture_format = try fixtureJsonOptionalU64(root, "fixture_format", 0);
    if (fixture_format != app_parity_fixture_format) return error.TestUnexpectedResult;
    const source_sha256 = try fixtureJsonString(root.get("source_sha256") orelse return error.TestUnexpectedResult);
    try validateFixtureSourceSha256(source_sha256);
    const source_entry_count = try fixtureJsonOptionalUsize(root, "source_entry_count") orelse return error.TestUnexpectedResult;
    const entry_count = try fixtureJsonOptionalUsize(root, "entry_count") orelse return error.TestUnexpectedResult;
    const skipped_entries = try parseFixtureStringListAlloc(alloc, root, "skipped_entries");
    errdefer if (skipped_entries.len > 0) alloc.free(skipped_entries);
    const schema_json = try fixtureJsonString(root.get("schema_json") orelse return error.TestUnexpectedResult);
    const entries = switch (root.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entries.len == 0) return error.TestUnexpectedResult;
    if (entry_count != entries.len) return error.TestUnexpectedResult;
    if (source_entry_count != entries.len + skipped_entries.len) return error.TestUnexpectedResult;
    return .{
        .fixture_format = fixture_format,
        .source_sha256 = source_sha256,
        .source_entry_count = source_entry_count,
        .entry_count = entry_count,
        .skipped_entries = skipped_entries,
        .schema_json = schema_json,
        .entries = entries,
    };
}

pub fn freeFixtureRoot(alloc: std.mem.Allocator, root: AppParityFixtureRoot) void {
    if (root.skipped_entries.len > 0) alloc.free(root.skipped_entries);
}

pub fn parseFixtureSummary(value: ?std.json.Value) !AppParityPlanSummary {
    if (value == null) return .{};
    const object = try fixtureJsonObject(value.?);
    try fixtureRequireOnlyKeys(object, &.{
        "ddl_tag",
        "table_name",
        "ctes",
        "predicates",
        "array_any",
        "in_predicates",
        "json_path_eq",
        "json_contains",
        "json_path_exists",
        "array_contains",
        "array_eq",
        "text_patterns",
        "access_or_predicates",
        "access_not_predicates",
        "expression_predicates",
        "expression_or_predicates",
        "expression_not_predicates",
        "expression_array_contains",
        "select",
        "select_all",
        "distinct_on",
        "order_by",
        "limit",
        "offset",
        "right_offset",
        "group_by",
        "group_expressions",
        "aggregations",
        "filter_groups",
        "having",
        "having_expressions",
        "having_any",
        "having_not",
        "operations",
        "source_assignments",
        "patch_expressions",
        "increment_expressions",
        "json_set_expressions",
        "returning",
        "returning_all",
        "conflict_where",
        "join_on",
        "matched_predicates",
        "matched_delete",
        "matched_do_nothing",
        "not_matched_predicates",
        "not_matched_do_nothing",
        "join_select",
        "lateral_correlations",
        "windows",
        "row_claim_skip_locked",
        "temporal_periods",
        "temporal_primary_key",
        "temporal_unique",
        "temporal_foreign_keys",
        "explain_subject",
        "explain_inner_kind",
        "explain_options",
        "explain_analyze",
        "explain_buffers",
        "explain_timing",
        "explain_summary",
        "explain_settings",
        "explain_wal",
    });
    return .{
        .ddl_tag = if (object.get("ddl_tag")) |tag_value| std.meta.stringToEnum(AppParityDdlTag, try fixtureJsonString(tag_value)) orelse return error.TestUnexpectedResult else null,
        .table_name = try fixtureJsonOptionalStringField(object, "table_name"),
        .ctes = try fixtureJsonOptionalUsize(object, "ctes"),
        .predicates = try fixtureJsonOptionalUsize(object, "predicates"),
        .array_any = try fixtureJsonOptionalUsize(object, "array_any"),
        .in_predicates = try fixtureJsonOptionalUsize(object, "in_predicates"),
        .json_path_eq = try fixtureJsonOptionalUsize(object, "json_path_eq"),
        .json_contains = try fixtureJsonOptionalUsize(object, "json_contains"),
        .json_path_exists = try fixtureJsonOptionalUsize(object, "json_path_exists"),
        .array_contains = try fixtureJsonOptionalUsize(object, "array_contains"),
        .array_eq = try fixtureJsonOptionalUsize(object, "array_eq"),
        .text_patterns = try fixtureJsonOptionalUsize(object, "text_patterns"),
        .access_or_predicates = try fixtureJsonOptionalUsize(object, "access_or_predicates"),
        .access_not_predicates = try fixtureJsonOptionalUsize(object, "access_not_predicates"),
        .expression_predicates = try fixtureJsonOptionalUsize(object, "expression_predicates"),
        .expression_or_predicates = try fixtureJsonOptionalUsize(object, "expression_or_predicates"),
        .expression_not_predicates = try fixtureJsonOptionalUsize(object, "expression_not_predicates"),
        .expression_array_contains = try fixtureJsonOptionalUsize(object, "expression_array_contains"),
        .select = try fixtureJsonOptionalUsize(object, "select"),
        .select_all = try fixtureJsonOptionalBool(object, "select_all"),
        .distinct_on = try fixtureJsonOptionalUsize(object, "distinct_on"),
        .order_by = try fixtureJsonOptionalUsize(object, "order_by"),
        .limit = try fixtureJsonOptionalU32(object, "limit"),
        .offset = try fixtureJsonOptionalU32(object, "offset"),
        .right_offset = try fixtureJsonOptionalU32(object, "right_offset"),
        .group_by = try fixtureJsonOptionalUsize(object, "group_by"),
        .group_expressions = try fixtureJsonOptionalUsize(object, "group_expressions"),
        .aggregations = try fixtureJsonOptionalUsize(object, "aggregations"),
        .filter_groups = try fixtureJsonOptionalUsize(object, "filter_groups"),
        .having = try fixtureJsonOptionalUsize(object, "having"),
        .having_expressions = try fixtureJsonOptionalUsize(object, "having_expressions"),
        .having_any = try fixtureJsonOptionalUsize(object, "having_any"),
        .having_not = try fixtureJsonOptionalUsize(object, "having_not"),
        .operations = try fixtureJsonOptionalUsize(object, "operations"),
        .source_assignments = try fixtureJsonOptionalUsize(object, "source_assignments"),
        .patch_expressions = try fixtureJsonOptionalUsize(object, "patch_expressions"),
        .increment_expressions = try fixtureJsonOptionalUsize(object, "increment_expressions"),
        .json_set_expressions = try fixtureJsonOptionalUsize(object, "json_set_expressions"),
        .returning = try fixtureJsonOptionalUsize(object, "returning"),
        .returning_all = try fixtureJsonOptionalBool(object, "returning_all"),
        .conflict_where = try fixtureJsonOptionalBool(object, "conflict_where"),
        .join_on = try fixtureJsonOptionalUsize(object, "join_on"),
        .matched_predicates = try fixtureJsonOptionalUsize(object, "matched_predicates"),
        .matched_delete = try fixtureJsonOptionalBool(object, "matched_delete"),
        .matched_do_nothing = try fixtureJsonOptionalBool(object, "matched_do_nothing"),
        .not_matched_predicates = try fixtureJsonOptionalUsize(object, "not_matched_predicates"),
        .not_matched_do_nothing = try fixtureJsonOptionalBool(object, "not_matched_do_nothing"),
        .join_select = try fixtureJsonOptionalUsize(object, "join_select"),
        .lateral_correlations = try fixtureJsonOptionalUsize(object, "lateral_correlations"),
        .windows = try fixtureJsonOptionalUsize(object, "windows"),
        .row_claim_skip_locked = try fixtureJsonOptionalBool(object, "row_claim_skip_locked"),
        .temporal_periods = try fixtureJsonOptionalUsize(object, "temporal_periods"),
        .temporal_primary_key = try fixtureJsonOptionalBool(object, "temporal_primary_key"),
        .temporal_unique = try fixtureJsonOptionalUsize(object, "temporal_unique"),
        .temporal_foreign_keys = try fixtureJsonOptionalUsize(object, "temporal_foreign_keys"),
        .explain_subject = try fixtureJsonOptionalStringField(object, "explain_subject"),
        .explain_inner_kind = try fixtureJsonOptionalStringField(object, "explain_inner_kind"),
        .explain_options = try fixtureJsonOptionalBool(object, "explain_options"),
        .explain_analyze = try fixtureJsonOptionalBool(object, "explain_analyze"),
        .explain_buffers = try fixtureJsonOptionalBool(object, "explain_buffers"),
        .explain_timing = try fixtureJsonOptionalBool(object, "explain_timing"),
        .explain_summary = try fixtureJsonOptionalBool(object, "explain_summary"),
        .explain_settings = try fixtureJsonOptionalBool(object, "explain_settings"),
        .explain_wal = try fixtureJsonOptionalBool(object, "explain_wal"),
    };
}

pub fn parseFixtureStringListAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    field: []const u8,
) ![]const []const u8 {
    const value = object.get(field) orelse return &.{};
    const array = switch (value) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    var strings = std.ArrayListUnmanaged([]const u8).empty;
    errdefer strings.deinit(alloc);
    for (array.items) |item| {
        try strings.append(alloc, try fixtureJsonString(item));
    }
    return try strings.toOwnedSlice(alloc);
}

fn parseAppParityCatalogTablesAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
) ![]const AppParityCatalogTable {
    const value = object.get("catalog_tables") orelse return &.{};
    const array = switch (value) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    if (array.items.len == 0) return error.TestUnexpectedResult;
    var tables = std.ArrayListUnmanaged(AppParityCatalogTable).empty;
    errdefer tables.deinit(alloc);
    for (array.items) |item| {
        const table_object = try fixtureJsonObject(item);
        try fixtureRequireOnlyKeys(table_object, &.{ "name", "schema_json" });
        const name = try fixtureJsonString(table_object.get("name") orelse return error.TestUnexpectedResult);
        const schema_json = try fixtureJsonString(table_object.get("schema_json") orelse return error.TestUnexpectedResult);
        if (name.len == 0 or schema_json.len == 0) return error.TestUnexpectedResult;
        try tables.append(alloc, .{ .name = name, .schema_json = schema_json });
    }
    return try tables.toOwnedSlice(alloc);
}

pub fn parseFixtureSqlValue(value: std.json.Value) !SqlValue {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{ "null", "bool", "integer", "float", "string", "json" });
    var it = object.iterator();
    const entry = it.next() orelse return error.TestUnexpectedResult;
    if (it.next() != null) return error.TestUnexpectedResult;
    if (std.mem.eql(u8, entry.key_ptr.*, "null")) return .null;
    if (std.mem.eql(u8, entry.key_ptr.*, "bool")) {
        return switch (entry.value_ptr.*) {
            .bool => |flag| .{ .bool = flag },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "integer")) {
        return switch (entry.value_ptr.*) {
            .integer => |number| .{ .integer = number },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "float")) {
        return switch (entry.value_ptr.*) {
            .integer => |number| .{ .float = @floatFromInt(number) },
            .float => |number| .{ .float = number },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "string")) {
        return .{ .string = try fixtureJsonString(entry.value_ptr.*) };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "json")) {
        return .{ .json = try fixtureJsonString(entry.value_ptr.*) };
    }
    return error.TestUnexpectedResult;
}

pub fn parseFixtureSqlValuesAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
) ![]const SqlValue {
    const value = object.get("params") orelse return &.{};
    const array = switch (value) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    var params = std.ArrayListUnmanaged(SqlValue).empty;
    errdefer params.deinit(alloc);
    for (array.items) |item| {
        try params.append(alloc, try parseFixtureSqlValue(item));
    }
    return try params.toOwnedSlice(alloc);
}

pub fn parseFixtureEntryAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParityCorpusEntry {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{
        "name",
        "sql",
        "family",
        "params",
        "summary",
        "plan",
        "classification_reason",
        "apply_setup_sql",
        "returning_rows",
        "applied_plan",
        "execution_plan",
        "resolver_row_json",
        "resolver_version",
        "resolver_exists",
        "source_schema_json",
        "catalog_tables",
    });
    const family_text = try fixtureJsonString(object.get("family") orelse return error.TestUnexpectedResult);
    const family = std.meta.stringToEnum(AppParityCorpusPlanFamily, family_text) orelse return error.TestUnexpectedResult;
    const plan = try fixtureJsonOptionalString(object, "plan", "");
    const summary = normalizeFixtureSummary(
        family,
        plan,
        try parseFixtureSummary(object.get("summary")),
    );
    return .{
        .name = try fixtureJsonString(object.get("name") orelse return error.TestUnexpectedResult),
        .sql = try fixtureJsonString(object.get("sql") orelse return error.TestUnexpectedResult),
        .family = family,
        .params = try parseFixtureSqlValuesAlloc(alloc, object),
        .summary = summary,
        .plan = plan,
        .classification_reason = try fixtureJsonOptionalString(object, "classification_reason", ""),
        .apply_setup_sql = try parseFixtureStringListAlloc(alloc, object, "apply_setup_sql"),
        .returning_rows = try parseFixtureStringListAlloc(alloc, object, "returning_rows"),
        .applied_plan = try fixtureJsonOptionalString(object, "applied_plan", ""),
        .execution_plan = try fixtureJsonOptionalString(object, "execution_plan", ""),
        .resolver_row_json = try fixtureJsonOptionalString(object, "resolver_row_json", ""),
        .resolver_version = try fixtureJsonOptionalU64(object, "resolver_version", 0),
        .resolver_exists = try fixtureJsonOptionalBool(object, "resolver_exists"),
        .source_schema_json = try fixtureJsonOptionalString(object, "source_schema_json", ""),
        .catalog_tables = try parseAppParityCatalogTablesAlloc(alloc, object),
    };
}

pub fn parseSourceCorpusRootAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParitySourceCorpusRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "source_format", "entries" });
    const source_format = try fixtureJsonOptionalU64(root, "source_format", 0);
    if (source_format != app_parity_source_corpus_format) return error.TestUnexpectedResult;

    const entry_values = switch (root.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entry_values.len == 0) return error.TestUnexpectedResult;

    var entries = std.ArrayListUnmanaged(AppParityCorpusEntry).empty;
    errdefer {
        for (entries.items) |entry| freeFixtureEntry(alloc, entry);
        entries.deinit(alloc);
    }
    var seen_names = std.StringHashMapUnmanaged(void){};
    defer seen_names.deinit(alloc);

    for (entry_values) |entry_value| {
        const entry = try parseFixtureEntryAlloc(alloc, entry_value);
        errdefer freeFixtureEntry(alloc, entry);
        if (entry.name.len == 0 or seen_names.contains(entry.name)) return error.TestUnexpectedResult;
        var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, entry.sql) catch return error.TestUnexpectedResult;
        defer parsed_sql.deinit(alloc);
        try validateSourceCorpusEntryMetadataParsedSql(entry, &parsed_sql);
        try validateSourceCorpusEntryJsonPayloadsParsedSql(alloc, entry, &parsed_sql);
        try seen_names.put(alloc, entry.name, {});
        try entries.append(alloc, entry);
    }

    return .{
        .source_format = source_format,
        .entries = try entries.toOwnedSlice(alloc),
    };
}

pub fn freeSourceCorpusRoot(alloc: std.mem.Allocator, root: AppParitySourceCorpusRoot) void {
    for (root.entries) |entry| freeFixtureEntry(alloc, entry);
    alloc.free(root.entries);
}

fn normalizeFixtureSummary(
    family: AppParityCorpusPlanFamily,
    plan: []const u8,
    summary: AppParityPlanSummary,
) AppParityPlanSummary {
    var normalized = summary;
    if (family == .update_joined_source and
        normalized.source_assignments == null and
        normalized.patch_expressions != null and
        planHasNonZeroToken(plan, ":source_assignments=") and
        !planHasNonZeroToken(plan, ":patch_expr="))
    {
        normalized.source_assignments = normalized.patch_expressions;
        normalized.patch_expressions = null;
    }
    return normalized;
}

pub fn freeFixtureEntry(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) void {
    if (entry.params.len > 0) alloc.free(entry.params);
    if (entry.apply_setup_sql.len > 0) alloc.free(entry.apply_setup_sql);
    if (entry.returning_rows.len > 0) alloc.free(entry.returning_rows);
    if (entry.catalog_tables.len > 0) alloc.free(entry.catalog_tables);
}

fn appParityCoverageFlag(coverage: AppParityCorpusCoverage, name: []const u8) !bool {
    inline for (std.meta.fields(AppParityCorpusCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            if (field.type != bool) return error.TestUnexpectedResult;
            return @field(coverage, field.name);
        }
    }
    return error.TestUnexpectedResult;
}

fn appParityCoverageRequirementKnown(name: []const u8) bool {
    inline for (std.meta.fields(AppParityCorpusCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            return field.type == bool or field.type == usize;
        }
    }
    return false;
}

pub fn appParityCoverageRequirementSatisfied(coverage: AppParityCorpusCoverage, name: []const u8) !bool {
    inline for (std.meta.fields(AppParityCorpusCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            if (field.type == bool) return @field(coverage, field.name);
            if (field.type == usize) return @field(coverage, field.name) > 0;
            return error.TestUnexpectedResult;
        }
    }
    return error.TestUnexpectedResult;
}

pub fn parseCoverageRequirementsRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !AppParityCoverageRequirementsRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "coverage_format", "required" });
    const coverage_format = try fixtureJsonOptionalU64(root, "coverage_format", 0);
    if (coverage_format != app_parity_coverage_fixture_format) return error.TestUnexpectedResult;
    const required = try parseFixtureStringListAlloc(alloc, root, "required");
    errdefer if (required.len > 0) alloc.free(required);
    if (required.len == 0) return error.TestUnexpectedResult;

    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    for (required, 0..) |name, i| {
        if (name.len == 0 or seen.contains(name) or !appParityCoverageRequirementKnown(name)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, required[i - 1], name)) return error.TestUnexpectedResult;
        try seen.put(alloc, name, {});
    }
    inline for (std.meta.fields(AppParityCorpusCoverage)) |field| {
        if ((field.type == bool or field.type == usize) and !seen.contains(field.name)) {
            return error.TestUnexpectedResult;
        }
    }

    return .{
        .coverage_format = coverage_format,
        .required = required,
    };
}

pub fn freeCoverageRequirementsRoot(
    alloc: std.mem.Allocator,
    root: AppParityCoverageRequirementsRoot,
) void {
    if (root.required.len > 0) alloc.free(root.required);
}

pub fn parseCoverageRegressionRequirementsRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !AppParityCoverageRegressionRequirementsRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "coverage_format", "required" });
    const coverage_format = try fixtureJsonOptionalU64(root, "coverage_format", 0);
    if (coverage_format != app_parity_coverage_regression_requirement_fixture_format) return error.TestUnexpectedResult;
    const required = try parseFixtureStringListAlloc(alloc, root, "required");
    errdefer if (required.len > 0) alloc.free(required);
    if (required.len == 0) return error.TestUnexpectedResult;

    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    for (required, 0..) |name, i| {
        if (name.len == 0 or seen.contains(name) or !appParityCoverageRequirementKnown(name)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, required[i - 1], name)) return error.TestUnexpectedResult;
        try seen.put(alloc, name, {});
    }

    return .{
        .coverage_format = coverage_format,
        .required = required,
    };
}

pub fn freeCoverageRegressionRequirementsRoot(
    alloc: std.mem.Allocator,
    root: AppParityCoverageRegressionRequirementsRoot,
) void {
    if (root.required.len > 0) alloc.free(root.required);
}

pub fn parseAppParityCoverageRegressionRequirementsAlloc(alloc: std.mem.Allocator) !AppParityCoverageRegressionRequirements {
    const coverage_json = @embedFile("../api/fixtures/sql_api_coverage_regression_required_buckets.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, coverage_json, .{});
    errdefer parsed.deinit();

    const root = try parseCoverageRegressionRequirementsRootAlloc(alloc, parsed.value);
    errdefer freeCoverageRegressionRequirementsRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

pub fn parseSqlAdapterEdgeCaseRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !SqlAdapterEdgeCaseRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "edge_case_format", "cases" });
    const edge_case_format = try fixtureJsonOptionalU64(root, "edge_case_format", 0);
    if (edge_case_format != sql_adapter_edge_case_fixture_format) return error.TestUnexpectedResult;
    const case_values = switch (root.get("cases") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (case_values.len == 0) return error.TestUnexpectedResult;

    var cases = std.ArrayListUnmanaged(SqlAdapterEdgeCase).empty;
    errdefer {
        for (cases.items) |case| freeSqlAdapterEdgeCase(alloc, case);
        cases.deinit(alloc);
    }
    var seen_names = std.StringHashMapUnmanaged(void){};
    defer seen_names.deinit(alloc);

    for (case_values) |case_value| {
        const edge_case = try parseSqlAdapterEdgeCaseAlloc(alloc, case_value);
        errdefer freeSqlAdapterEdgeCase(alloc, edge_case);
        if (edge_case.name.len == 0 or edge_case.sql.len == 0 or seen_names.contains(edge_case.name)) {
            return error.TestUnexpectedResult;
        }
        try validateSqlAdapterEdgeCaseCoverage(edge_case);
        try seen_names.put(alloc, edge_case.name, {});
        try cases.append(alloc, edge_case);
    }

    return .{
        .edge_case_format = edge_case_format,
        .cases = try cases.toOwnedSlice(alloc),
    };
}

pub fn freeSqlAdapterEdgeCaseRoot(
    alloc: std.mem.Allocator,
    root: SqlAdapterEdgeCaseRoot,
) void {
    for (root.cases) |case| freeSqlAdapterEdgeCase(alloc, case);
    if (root.cases.len > 0) alloc.free(root.cases);
}

fn sqlAdapterEdgeCaseCoverageRequirementKnown(name: []const u8) bool {
    inline for (std.meta.fields(SqlAdapterEdgeCaseCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) return field.type == bool;
    }
    return false;
}

fn sqlAdapterEdgeCaseCoverageSet(coverage: *SqlAdapterEdgeCaseCoverage, name: []const u8) !void {
    inline for (std.meta.fields(SqlAdapterEdgeCaseCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            if (field.type != bool) return error.TestUnexpectedResult;
            @field(coverage.*, field.name) = true;
            return;
        }
    }
    return error.TestUnexpectedResult;
}

fn sqlAdapterEdgeCaseActionCoverageName(action: SqlAdapterEdgeCaseAction) []const u8 {
    return switch (action) {
        .classify_write => "action_classify_write",
        .ddl => "action_ddl",
        .delete => "action_delete",
        .insert => "action_insert",
        .select => "action_select",
        .update => "action_update",
        .write_plan => "action_write_plan",
    };
}

fn sqlAdapterEdgeCaseExpectedErrorCoverageName(expected_error: []const u8) !?[]const u8 {
    if (expected_error.len == 0) return null;
    if (std.mem.eql(u8, expected_error, "invalid_sql_catalog")) return "expected_error_invalid_sql_catalog";
    if (std.mem.eql(u8, expected_error, "unsupported_rows_query")) return "expected_error_unsupported_rows_query";
    if (std.mem.eql(u8, expected_error, "unsupported_rows_selector")) return "expected_error_unsupported_rows_selector";
    if (std.mem.eql(u8, expected_error, "unsupported_sql_shape")) return "expected_error_unsupported_sql_shape";
    return error.TestUnexpectedResult;
}

fn sqlAdapterEdgeCaseWriteKindCoverageName(kind: classifier.SqlWriteStatementKind) []const u8 {
    return switch (kind) {
        .delete => "write_kind_delete",
        .delete_source => "write_kind_delete_source",
        .delete_joined_source => "write_kind_delete_joined_source",
        .update => "write_kind_update",
        .update_source => "write_kind_update_source",
        .update_joined_source => "write_kind_update_joined_source",
        else => "",
    };
}

fn stringListContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

fn validateSqlAdapterEdgeCaseCoverage(edge_case: SqlAdapterEdgeCase) !void {
    if (edge_case.coverage.len == 0) return error.TestUnexpectedResult;
    for (edge_case.coverage, 0..) |name, i| {
        if (name.len == 0 or !sqlAdapterEdgeCaseCoverageRequirementKnown(name)) return error.TestUnexpectedResult;
        if (i > 0 and !std.mem.lessThan(u8, edge_case.coverage[i - 1], name)) return error.TestUnexpectedResult;
    }

    if (!stringListContains(edge_case.coverage, sqlAdapterEdgeCaseActionCoverageName(edge_case.action))) {
        return error.TestUnexpectedResult;
    }
    if (try sqlAdapterEdgeCaseExpectedErrorCoverageName(edge_case.expected_error)) |expected_error_coverage| {
        if (!stringListContains(edge_case.coverage, expected_error_coverage)) return error.TestUnexpectedResult;
    }
    if (edge_case.expected_write_kind) |kind| {
        const coverage_name = sqlAdapterEdgeCaseWriteKindCoverageName(kind);
        if (coverage_name.len == 0 or !stringListContains(edge_case.coverage, coverage_name)) {
            return error.TestUnexpectedResult;
        }
    }
}

fn sqlAdapterEdgeCaseCoverageRequirementSatisfied(
    coverage: SqlAdapterEdgeCaseCoverage,
    name: []const u8,
) !bool {
    inline for (std.meta.fields(SqlAdapterEdgeCaseCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            if (field.type == bool) return @field(coverage, field.name);
            return error.TestUnexpectedResult;
        }
    }
    return error.TestUnexpectedResult;
}

pub fn parseSqlAdapterEdgeCaseCoverageRequirementsRootAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !SqlAdapterEdgeCaseCoverageRequirementsRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "coverage_format", "required" });
    const coverage_format = try fixtureJsonOptionalU64(root, "coverage_format", 0);
    if (coverage_format != sql_adapter_edge_coverage_fixture_format) return error.TestUnexpectedResult;
    const required = try parseFixtureStringListAlloc(alloc, root, "required");
    errdefer if (required.len > 0) alloc.free(required);
    if (required.len == 0) return error.TestUnexpectedResult;

    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    for (required, 0..) |name, i| {
        if (name.len == 0 or seen.contains(name) or !sqlAdapterEdgeCaseCoverageRequirementKnown(name)) {
            return error.TestUnexpectedResult;
        }
        if (i > 0 and !std.mem.lessThan(u8, required[i - 1], name)) return error.TestUnexpectedResult;
        try seen.put(alloc, name, {});
    }
    inline for (std.meta.fields(SqlAdapterEdgeCaseCoverage)) |field| {
        if (field.type == bool and !seen.contains(field.name)) {
            return error.TestUnexpectedResult;
        }
    }

    return .{
        .coverage_format = coverage_format,
        .required = required,
    };
}

pub fn freeSqlAdapterEdgeCaseCoverageRequirementsRoot(
    alloc: std.mem.Allocator,
    root: SqlAdapterEdgeCaseCoverageRequirementsRoot,
) void {
    if (root.required.len > 0) alloc.free(root.required);
}

pub fn parseSqlAdapterEdgeCaseCoverageRequirementsAlloc(alloc: std.mem.Allocator) !SqlAdapterEdgeCaseCoverageRequirements {
    const coverage_json = @embedFile("../api/fixtures/sql_api_adapter_edge_required_coverage.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, coverage_json, .{});
    errdefer parsed.deinit();

    const root = try parseSqlAdapterEdgeCaseCoverageRequirementsRootAlloc(alloc, parsed.value);
    errdefer freeSqlAdapterEdgeCaseCoverageRequirementsRoot(alloc, root);

    return .{
        .parsed = parsed,
        .root = root,
    };
}

pub fn expectSqlAdapterEdgeCaseCoverageRequirements(
    coverage: SqlAdapterEdgeCaseCoverage,
    required: []const []const u8,
) !void {
    if (required.len == 0) return error.TestUnexpectedResult;
    for (required) |name| {
        if (!try sqlAdapterEdgeCaseCoverageRequirementSatisfied(coverage, name)) {
            std.debug.print("missing sql adapter edge coverage: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

fn parseSqlAdapterEdgeCaseAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !SqlAdapterEdgeCase {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{
        "name",
        "action",
        "coverage",
        "expected_error",
        "expected_table",
        "expected_predicates",
        "expected_first_predicate_field",
        "expected_first_predicate_value_json",
        "expected_transformed",
        "expected_write_kind",
        "expected_inserted",
        "expected_ddl_tag",
        "expected_if_not_exists",
        "omit_resolver",
        "params",
        "sql",
    });
    const action_text = try fixtureJsonString(object.get("action") orelse return error.TestUnexpectedResult);
    const action = std.meta.stringToEnum(SqlAdapterEdgeCaseAction, action_text) orelse return error.TestUnexpectedResult;
    const coverage = try parseFixtureStringListAlloc(alloc, object, "coverage");
    errdefer if (coverage.len > 0) alloc.free(coverage);
    const params = try parseFixtureSqlValuesAlloc(alloc, object);
    errdefer if (params.len > 0) alloc.free(params);
    return .{
        .name = try fixtureJsonString(object.get("name") orelse return error.TestUnexpectedResult),
        .action = action,
        .sql = try fixtureJsonString(object.get("sql") orelse return error.TestUnexpectedResult),
        .coverage = coverage,
        .expected_error = try fixtureJsonOptionalString(object, "expected_error", ""),
        .expected_table = try fixtureJsonOptionalStringField(object, "expected_table"),
        .expected_predicates = try fixtureJsonOptionalUsize(object, "expected_predicates"),
        .expected_first_predicate_field = try fixtureJsonOptionalStringField(object, "expected_first_predicate_field"),
        .expected_first_predicate_value_json = try fixtureJsonOptionalStringField(object, "expected_first_predicate_value_json"),
        .expected_transformed = try fixtureJsonOptionalU32(object, "expected_transformed"),
        .expected_write_kind = if (try fixtureJsonOptionalStringField(object, "expected_write_kind")) |kind|
            std.meta.stringToEnum(classifier.SqlWriteStatementKind, kind) orelse return error.TestUnexpectedResult
        else
            null,
        .expected_inserted = try fixtureJsonOptionalU32(object, "expected_inserted"),
        .expected_ddl_tag = if (try fixtureJsonOptionalStringField(object, "expected_ddl_tag")) |tag|
            std.meta.stringToEnum(SqlAdapterEdgeCaseDdlTag, tag) orelse return error.TestUnexpectedResult
        else
            null,
        .expected_if_not_exists = try fixtureJsonOptionalBool(object, "expected_if_not_exists"),
        .omit_resolver = (try fixtureJsonOptionalBool(object, "omit_resolver")) orelse false,
        .params = params,
    };
}

fn freeSqlAdapterEdgeCase(
    alloc: std.mem.Allocator,
    edge_case: SqlAdapterEdgeCase,
) void {
    if (edge_case.coverage.len > 0) alloc.free(edge_case.coverage);
    if (edge_case.params.len > 0) alloc.free(edge_case.params);
}

pub fn expectAppParityCoverageRequirements(
    coverage: AppParityCorpusCoverage,
    required: []const []const u8,
) !void {
    if (required.len == 0) return error.TestUnexpectedResult;
    for (required) |name| {
        if (!try appParityCoverageRequirementSatisfied(coverage, name)) {
            std.debug.print("missing app parity coverage: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

fn recordCoverageRegressionBuckets(
    alloc: std.mem.Allocator,
    seen_buckets: *std.StringHashMapUnmanaged(void),
    buckets: []const []const u8,
) !void {
    for (buckets) |name| {
        if (!appParityCoverageRequirementKnown(name)) return error.TestUnexpectedResult;
        try seen_buckets.put(alloc, name, {});
    }
}

fn checkCoverageRegressionCase(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    seen_buckets: *std.StringHashMapUnmanaged(void),
) !void {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{ "name", "entries", "expect_true", "expect_false" });

    const entries = switch (object.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entries.len == 0) return error.TestUnexpectedResult;

    const expect_true = try parseFixtureStringListAlloc(alloc, object, "expect_true");
    defer {
        if (expect_true.len > 0) alloc.free(expect_true);
    }
    const expect_false = try parseFixtureStringListAlloc(alloc, object, "expect_false");
    defer {
        if (expect_false.len > 0) alloc.free(expect_false);
    }
    if (expect_true.len == 0 and expect_false.len == 0) return error.TestUnexpectedResult;
    try recordCoverageRegressionBuckets(alloc, seen_buckets, expect_true);
    try recordCoverageRegressionBuckets(alloc, seen_buckets, expect_false);

    var coverage = AppParityCorpusCoverage{};
    for (entries) |entry_value| {
        const entry = try parseFixtureEntryAlloc(alloc, entry_value);
        defer freeFixtureEntry(alloc, entry);
        try coverage.observe(alloc, entry);
    }

    for (expect_true) |name| {
        try std.testing.expect(try appParityCoverageFlag(coverage, name));
    }
    for (expect_false) |name| {
        try std.testing.expect(!try appParityCoverageFlag(coverage, name));
    }
}

fn recordSummaryRegressionAssertions(
    alloc: std.mem.Allocator,
    seen_assertions: *std.StringHashMapUnmanaged(void),
    assertions: []const []const u8,
) !void {
    for (assertions) |name| {
        if (!appParitySummaryRegressionAssertionKnown(name)) return error.TestUnexpectedResult;
        try seen_assertions.put(alloc, name, {});
    }
}

fn checkSummaryRegressionCase(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    seen_assertions: *std.StringHashMapUnmanaged(void),
) !void {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{ "name", "entry", "expect_true", "expect_false" });

    const entry_value = object.get("entry") orelse return error.TestUnexpectedResult;
    const entry = try parseFixtureEntryAlloc(alloc, entry_value);
    defer freeFixtureEntry(alloc, entry);

    const expect_true = try parseFixtureStringListAlloc(alloc, object, "expect_true");
    defer {
        if (expect_true.len > 0) alloc.free(expect_true);
    }
    const expect_false = try parseFixtureStringListAlloc(alloc, object, "expect_false");
    defer {
        if (expect_false.len > 0) alloc.free(expect_false);
    }
    if (expect_true.len == 0 and expect_false.len == 0) return error.TestUnexpectedResult;
    try recordSummaryRegressionAssertions(alloc, seen_assertions, expect_true);
    try recordSummaryRegressionAssertions(alloc, seen_assertions, expect_false);

    for (expect_true) |name| {
        try std.testing.expect(try appParitySummaryRegressionAssertion(entry, name));
    }
    for (expect_false) |name| {
        try std.testing.expect(!try appParitySummaryRegressionAssertion(entry, name));
    }
}

fn appParitySummaryRegressionAssertion(entry: AppParityCorpusEntry, name: []const u8) !bool {
    if (std.mem.eql(u8, name, "explain_plan_has_write_kind")) return corpusExplainSubjectMatches(entry, "write");
    if (std.mem.eql(u8, name, "explain_plan_has_read_kind")) return corpusExplainSubjectMatches(entry, "read");
    if (std.mem.eql(u8, name, "explain_write_inner_insert")) return corpusExplainInnerKindMatches(entry, "write", "insert");
    if (std.mem.eql(u8, name, "explain_write_inner_merge")) return corpusExplainInnerKindMatches(entry, "write", "merge_mutation");
    if (std.mem.eql(u8, name, "read_plan_has_query_prefix")) return corpusReadPlanHasPrefix(entry, "read:query:");
    if (std.mem.eql(u8, name, "plan_analyze_true_token")) return planHasExactBoolToken(entry.plan, ":analyze=", true);
    if (std.mem.eql(u8, name, "plan_format_present")) return planHasStringToken(entry.plan, ":format=");
    if (std.mem.eql(u8, name, "plan_verbose_present")) return planUsizeTokenValue(entry.plan, ":verbose=") != null;
    if (std.mem.eql(u8, name, "plan_costs_present")) return planUsizeTokenValue(entry.plan, ":costs=") != null;

    if (std.mem.eql(u8, name, "operations_matches_3")) return corpusFixtureOperationsSummaryMatchesPlan(entry, 3);
    if (std.mem.eql(u8, name, "operations_matches_2")) return corpusFixtureOperationsSummaryMatchesPlan(entry, 2);
    if (std.mem.eql(u8, name, "allows_returning")) return corpusFixtureAllowsReturningSummary(entry);
    if (std.mem.eql(u8, name, "returning_matches_1")) return corpusFixtureReturningSummaryMatchesPlan(entry, 1);
    if (std.mem.eql(u8, name, "returning_all_matches_true")) return corpusFixtureReturningAllSummaryMatchesPlan(entry, true);
    if (std.mem.eql(u8, name, "allows_conflict_where")) return corpusFixtureAllowsConflictWhereSummary(entry);
    if (std.mem.eql(u8, name, "conflict_where_matches_false")) return corpusFixtureConflictWhereSummaryMatchesPlan(entry, false);
    if (std.mem.eql(u8, name, "allows_mutation_transform")) return corpusFixtureAllowsMutationTransformSummary(entry);
    if (std.mem.eql(u8, name, "transform_matches_plan")) return corpusFixtureTransformSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_source_assignments")) return corpusFixtureAllowsSourceAssignmentsSummary(entry);
    if (std.mem.eql(u8, name, "source_assignments_match_2")) return corpusFixtureSourceAssignmentsSummaryMatchesPlan(entry, 2);
    if (std.mem.eql(u8, name, "allows_merge_arm")) return corpusFixtureAllowsMergeArmSummary(entry);
    if (std.mem.eql(u8, name, "merge_arm_matches_plan")) return corpusFixtureMergeArmSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_aggregate")) return corpusFixtureAllowsAggregateSummary(entry);
    if (std.mem.eql(u8, name, "aggregate_matches_plan")) return corpusFixtureAggregateSummaryMatchesPlan(entry);

    if (std.mem.eql(u8, name, "ddl_select_matches")) return corpusFixtureDdlSelectSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "ddl_predicate_matches")) return corpusFixtureDdlPredicateSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_predicate")) return corpusFixtureAllowsPredicateSummary(entry);
    if (std.mem.eql(u8, name, "predicate_matches")) return corpusFixturePredicateSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_access")) return corpusFixtureAllowsAccessSummary(entry);
    if (std.mem.eql(u8, name, "access_matches")) return corpusFixtureAccessSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "select_matches")) return corpusFixtureSelectSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_full_query_output")) return corpusFixtureAllowsFullQueryOutputSummary(entry);
    if (std.mem.eql(u8, name, "full_query_output_matches")) return corpusFixtureFullQueryOutputSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_pagination")) return corpusFixtureAllowsPaginationSummary(entry);
    if (std.mem.eql(u8, name, "pagination_matches")) return corpusFixturePaginationSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_row_claim")) return corpusFixtureAllowsRowClaimSummary(entry);
    if (std.mem.eql(u8, name, "row_claim_matches")) return corpusFixtureRowClaimSummaryMatchesPlan(entry);
    if (std.mem.eql(u8, name, "allows_join_select")) return corpusFixtureAllowsJoinSelectSummary(entry);
    if (std.mem.eql(u8, name, "join_select_matches_1")) return corpusFixtureJoinSelectSummaryMatchesPlan(entry, 1);
    if (std.mem.eql(u8, name, "allows_join_on")) return corpusFixtureAllowsJoinOnSummary(entry);
    if (std.mem.eql(u8, name, "join_on_matches_1")) return corpusFixtureJoinOnSummaryMatchesPlan(entry, 1);
    if (std.mem.eql(u8, name, "allows_window")) return corpusFixtureAllowsWindowSummary(entry);
    if (std.mem.eql(u8, name, "window_matches_1")) return corpusFixtureWindowSummaryMatchesPlan(entry, 1);
    if (std.mem.eql(u8, name, "allows_lateral")) return corpusFixtureAllowsLateralSummary(entry);
    if (std.mem.eql(u8, name, "lateral_matches")) return corpusFixtureLateralSummaryMatchesPlan(entry);

    return error.TestUnexpectedResult;
}

fn corpusExplainSubjectMatches(entry: AppParityCorpusEntry, expected: []const u8) bool {
    if (entry.summary.explain_subject) |subject| {
        return std.mem.eql(u8, subject, expected);
    }
    return explainPlanHasKind(entry.plan, expected);
}

fn corpusExplainOptionsEnabled(entry: AppParityCorpusEntry) bool {
    if (entry.summary.explain_options) |enabled| return enabled;
    return planHasStringToken(entry.plan, ":format=") or
        planUsizeTokenValue(entry.plan, ":verbose=") != null or
        planUsizeTokenValue(entry.plan, ":costs=") != null;
}

fn corpusExplainBoolValue(entry: AppParityCorpusEntry, summary_value: ?bool, token: []const u8, default: bool) bool {
    if (summary_value) |value| return value;
    return (planUsizeTokenValue(entry.plan, token) orelse @intFromBool(default)) == 1;
}

fn corpusExplainInnerKindMatches(entry: AppParityCorpusEntry, expected_subject: []const u8, expected_inner_kind: []const u8) bool {
    if (entry.summary.explain_subject) |subject| {
        if (!std.mem.eql(u8, subject, expected_subject)) return false;
        const inner_kind = entry.summary.explain_inner_kind orelse return false;
        return std.mem.eql(u8, inner_kind, expected_inner_kind);
    }
    if (!std.mem.eql(u8, expected_subject, "write")) return false;
    if (std.mem.eql(u8, expected_inner_kind, "insert")) {
        return corpusExplainWriteInnerHasPrefix(entry, ":inner=insert:");
    }
    if (std.mem.eql(u8, expected_inner_kind, "merge_mutation")) {
        return corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:");
    }
    return false;
}

fn fixtureWriteObjectComma(writer: anytype, first: *bool) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeAll(",\n");
    }
}

fn fixtureWriteStringField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: []const u8) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {f}", .{ indent, std.json.fmt(name, .{}), std.json.fmt(value, .{}) });
}

fn fixtureWriteBoolField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: bool) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {}", .{ indent, std.json.fmt(name, .{}), value });
}

fn fixtureWriteU64Field(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: u64) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {d}", .{ indent, std.json.fmt(name, .{}), value });
}

fn fixtureWriteUsizeSummaryField(writer: anytype, first: *bool, name: []const u8, value: ?usize) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {d}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteU32SummaryField(writer: anytype, first: *bool, name: []const u8, value: ?u32) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {d}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteBoolSummaryField(writer: anytype, first: *bool, name: []const u8, value: ?bool) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteStringListField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, values: []const []const u8) !void {
    if (values.len == 0) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: [", .{ indent, std.json.fmt(name, .{}) });
    for (values, 0..) |value, i| {
        if (i > 0) try writer.writeAll(", ");
        try writer.print("{f}", .{std.json.fmt(value, .{})});
    }
    try writer.writeByte(']');
}

fn fixtureWriteCatalogTablesField(writer: anytype, first: *bool, indent: []const u8, tables: []const AppParityCatalogTable) !void {
    if (tables.len == 0) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}\"catalog_tables\": [", .{indent});
    for (tables, 0..) |table, i| {
        if (i > 0) try writer.writeAll(", ");
        try writer.print(
            "{{\"name\": {f}, \"schema_json\": {f}}}",
            .{ std.json.fmt(table.name, .{}), std.json.fmt(table.schema_json, .{}) },
        );
    }
    try writer.writeByte(']');
}

fn fixtureWriteSqlValue(writer: anytype, value: SqlValue) !void {
    switch (value) {
        .null => try writer.writeAll("{\"null\": true}"),
        .bool => |actual| try writer.print("{{\"bool\": {}}}", .{actual}),
        .integer => |actual| try writer.print("{{\"integer\": {d}}}", .{actual}),
        .float => |actual| try writer.print("{{\"float\": {d}}}", .{actual}),
        .string => |actual| try writer.print("{{\"string\": {f}}}", .{std.json.fmt(actual, .{})}),
        .json => |actual| try writer.print("{{\"json\": {f}}}", .{std.json.fmt(actual, .{})}),
    }
}

fn fixtureWriteParamsField(writer: anytype, first: *bool, indent: []const u8, params: []const SqlValue) !void {
    if (params.len == 0) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}\"params\": [", .{indent});
    for (params, 0..) |param, i| {
        if (i > 0) try writer.writeAll(", ");
        try fixtureWriteSqlValue(writer, param);
    }
    try writer.writeByte(']');
}

fn fixtureWriteSummaryField(writer: anytype, first: *bool, summary: AppParityPlanSummary) !void {
    if (!summaryHasFields(summary)) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.writeAll("      \"summary\": {\n");
    var summary_first = true;
    if (summary.ddl_tag) |tag| try fixtureWriteStringField(writer, &summary_first, "        ", "ddl_tag", @tagName(tag));
    if (summary.table_name) |table_name| try fixtureWriteStringField(writer, &summary_first, "        ", "table_name", table_name);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "ctes", summary.ctes);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "predicates", summary.predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_any", summary.array_any);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "in_predicates", summary.in_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_path_eq", summary.json_path_eq);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_contains", summary.json_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_path_exists", summary.json_path_exists);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_contains", summary.array_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_eq", summary.array_eq);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "text_patterns", summary.text_patterns);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "access_or_predicates", summary.access_or_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "access_not_predicates", summary.access_not_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_predicates", summary.expression_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_or_predicates", summary.expression_or_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_not_predicates", summary.expression_not_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_array_contains", summary.expression_array_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "select", summary.select);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "select_all", summary.select_all);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "distinct_on", summary.distinct_on);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "order_by", summary.order_by);
    try fixtureWriteU32SummaryField(writer, &summary_first, "limit", summary.limit);
    try fixtureWriteU32SummaryField(writer, &summary_first, "offset", summary.offset);
    try fixtureWriteU32SummaryField(writer, &summary_first, "right_offset", summary.right_offset);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "group_by", summary.group_by);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "group_expressions", summary.group_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "aggregations", summary.aggregations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "filter_groups", summary.filter_groups);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having", summary.having);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_expressions", summary.having_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_any", summary.having_any);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_not", summary.having_not);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "operations", summary.operations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "source_assignments", summary.source_assignments);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "patch_expressions", summary.patch_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "increment_expressions", summary.increment_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_set_expressions", summary.json_set_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "returning", summary.returning);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "returning_all", summary.returning_all);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "conflict_where", summary.conflict_where);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "join_on", summary.join_on);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "matched_predicates", summary.matched_predicates);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "matched_delete", summary.matched_delete);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "matched_do_nothing", summary.matched_do_nothing);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "not_matched_predicates", summary.not_matched_predicates);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "not_matched_do_nothing", summary.not_matched_do_nothing);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "join_select", summary.join_select);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "lateral_correlations", summary.lateral_correlations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "windows", summary.windows);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "row_claim_skip_locked", summary.row_claim_skip_locked);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_periods", summary.temporal_periods);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "temporal_primary_key", summary.temporal_primary_key);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_unique", summary.temporal_unique);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_foreign_keys", summary.temporal_foreign_keys);
    if (summary.explain_subject) |subject| try fixtureWriteStringField(writer, &summary_first, "        ", "explain_subject", subject);
    if (summary.explain_inner_kind) |kind| try fixtureWriteStringField(writer, &summary_first, "        ", "explain_inner_kind", kind);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_options", summary.explain_options);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_analyze", summary.explain_analyze);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_buffers", summary.explain_buffers);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_timing", summary.explain_timing);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_summary", summary.explain_summary);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_settings", summary.explain_settings);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "explain_wal", summary.explain_wal);
    try writer.writeAll("\n      }");
}

pub fn fixtureJsonAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    source_sha256: []const u8,
    source_entry_count: usize,
    entries: []const AppParityFixtureEncodedEntry,
    skipped_entries: []const []const u8,
) ![]u8 {
    try validateFixtureSourceSha256(source_sha256);
    var entries_out: std.Io.Writer.Allocating = .init(alloc);
    errdefer entries_out.deinit();
    const entries_writer = &entries_out.writer;
    for (entries, 0..) |encoded, i| {
        const entry = encoded.entry;
        if (i > 0) try entries_writer.writeByte(',');
        try entries_writer.writeAll("\n    {\n");
        var first = true;
        try fixtureWriteStringField(entries_writer, &first, "      ", "name", entry.name);
        try fixtureWriteStringField(entries_writer, &first, "      ", "family", @tagName(entry.family));
        try fixtureWriteSummaryField(entries_writer, &first, entry.summary);
        try fixtureWriteStringField(entries_writer, &first, "      ", "plan", entry.plan);
        if (entry.classification_reason.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "classification_reason", entry.classification_reason);
        try fixtureWriteStringListField(entries_writer, &first, "      ", "apply_setup_sql", entry.apply_setup_sql);
        try fixtureWriteStringListField(entries_writer, &first, "      ", "returning_rows", entry.returning_rows);
        if (encoded.applied_plan.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "applied_plan", encoded.applied_plan);
        if (entry.execution_plan.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "execution_plan", entry.execution_plan);
        if (entry.resolver_row_json.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "resolver_row_json", entry.resolver_row_json);
        if (entry.resolver_version != 0) try fixtureWriteU64Field(entries_writer, &first, "      ", "resolver_version", entry.resolver_version);
        if (entry.resolver_exists) |exists| try fixtureWriteBoolField(entries_writer, &first, "      ", "resolver_exists", exists);
        if (entry.source_schema_json.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "source_schema_json", entry.source_schema_json);
        try fixtureWriteCatalogTablesField(entries_writer, &first, "      ", entry.catalog_tables);
        try fixtureWriteParamsField(entries_writer, &first, "      ", entry.params);
        try fixtureWriteStringField(entries_writer, &first, "      ", "sql", entry.sql);
        try entries_writer.writeAll("\n    }");
    }
    const entries_json = try entries_out.toOwnedSlice();
    defer alloc.free(entries_json);

    var skipped_out: std.Io.Writer.Allocating = .init(alloc);
    errdefer skipped_out.deinit();
    const skipped_writer = &skipped_out.writer;
    for (skipped_entries, 0..) |name, i| {
        if (i > 0) try skipped_writer.writeByte(',');
        try skipped_writer.writeAll("\n    ");
        try skipped_writer.print("{f}", .{std.json.fmt(name, .{})});
    }
    const skipped_json = try skipped_out.toOwnedSlice();
    defer alloc.free(skipped_json);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print(
        "{{\n  \"fixture_format\": {},\n  \"source_sha256\": {f},\n  \"source_entry_count\": {},\n  \"entry_count\": {},\n  \"skipped_entries\": [",
        .{ app_parity_fixture_format, std.json.fmt(source_sha256, .{}), source_entry_count, entries.len },
    );
    try writer.writeAll(skipped_json);
    try writer.print(
        "\n  ],\n  \"schema_json\": {f},\n  \"entries\": [",
        .{std.json.fmt(schema_json, .{})},
    );
    try writer.writeAll(entries_json);
    try writer.writeAll("\n  ]\n}\n");
    return try out.toOwnedSlice();
}

pub fn fixtureGateModeFromPaths(promote_path: ?[]const u8, check_path: ?[]const u8) !AppParityFixtureGateMode {
    if (promote_path != null and check_path != null) return error.TestUnexpectedResult;
    if (promote_path) |path| return .{ .promote = path };
    if (check_path) |path| return .{ .check = path };
    return .none;
}

fn fixtureGateEnvPathAlloc(alloc: std.mem.Allocator, name: []const u8) !?[]u8 {
    const view = std.testing.environ.block.view();
    for (view.slice) |entry| {
        const text = std.mem.span(entry);
        const eq = std.mem.indexOfScalar(u8, text, '=') orelse continue;
        if (std.mem.eql(u8, text[0..eq], name)) return try alloc.dupe(u8, text[eq + 1 ..]);
    }
    if (comptime @hasDecl(std.process.Environ.Block, "global")) {
        const process_env: std.process.Environ = .{ .block = .global };
        return process_env.getAlloc(alloc, name) catch |err| switch (err) {
            error.EnvironmentVariableMissing => null,
            else => return err,
        };
    }
    var index: usize = 0;
    while (std.c.environ[index]) |entry| : (index += 1) {
        const text = std.mem.span(entry);
        const eq = std.mem.indexOfScalar(u8, text, '=') orelse continue;
        if (std.mem.eql(u8, text[0..eq], name)) return try alloc.dupe(u8, text[eq + 1 ..]);
    }
    return null;
}

pub fn fixtureGateModeFromEnvAlloc(alloc: std.mem.Allocator) !AppParityFixtureGateMode {
    const promote_path = try fixtureGateEnvPathAlloc(alloc, "ANTFLY_SQL_API_PARITY_FIXTURE_PROMOTE");
    errdefer if (promote_path) |path| alloc.free(path);
    const check_path = try fixtureGateEnvPathAlloc(alloc, "ANTFLY_SQL_API_PARITY_FIXTURE_CHECK");
    errdefer if (check_path) |path| alloc.free(path);
    return fixtureGateModeFromPaths(promote_path, check_path);
}

pub fn freeFixtureGateMode(alloc: std.mem.Allocator, mode: AppParityFixtureGateMode) void {
    switch (mode) {
        .none => {},
        .check, .promote => |path| alloc.free(path),
    }
}

pub fn checkOrPromoteFixtureJson(
    alloc: std.mem.Allocator,
    mode: AppParityFixtureGateMode,
    encoded: []const u8,
) !void {
    switch (mode) {
        .none => return,
        .check => |path| {
            const existing = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(encoded.len + 1));
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded)) {
                std.debug.print("SQL/API parity fixture is stale: {s}\nrun `zig build sql-api-parity-fixture-promote` from zig/\n", .{path});
                return error.TestUnexpectedResult;
            }
        },
        .promote => |path| {
            var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
            defer file.close(std.testing.io);

            var file_buf: [4096]u8 = undefined;
            var writer = file.writer(std.testing.io, &file_buf);
            try writer.interface.writeAll(encoded);
            try writer.end();
        },
    }
}

pub fn corpusUnsupportedPlanFamily(family: AppParityCorpusPlanFamily) ?UnsupportedPlanFamily {
    return switch (family) {
        .unsupported => .query,
        .unsupported_read => .read,
        .unsupported_ddl => .ddl,
        .unsupported_write => .write,
        .unsupported_insert => .insert,
        .unsupported_update => .update,
        .unsupported_update_source => .update_source,
        .unsupported_delete => .delete,
        .unsupported_update_joined_source => .update_joined_source,
        .unsupported_delete_joined_source => .delete_joined_source,
        .unsupported_merge_mutation => .merge_mutation,
        else => null,
    };
}

pub fn corpusPlanFamilyIsUnsupported(family: AppParityCorpusPlanFamily) bool {
    return corpusUnsupportedPlanFamily(family) != null;
}

pub fn corpusPlanFamilyIsInvalid(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .invalid_read,
        .invalid_insert,
        .invalid_update,
        .invalid_delete,
        .invalid_update_source,
        .invalid_update_joined_source,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyNeedsReason(family: AppParityCorpusPlanFamily) bool {
    return family == .adapter_noop_ddl or corpusPlanFamilyIsUnsupported(family) or corpusPlanFamilyIsInvalid(family);
}

pub fn corpusStableReasonToken(reason: []const u8) bool {
    return diagnostics.classificationReasonTokenIsKnown(reason);
}

pub fn corpusReasonHasNativeRequirement(reason: []const u8) bool {
    const diagnostic_reason = diagnostics.classificationReasonFromToken(reason) orelse return false;
    _ = diagnostics.nativeExecutionRequirement(diagnostic_reason);
    return true;
}

const InvalidPlanFamily = enum {
    read,
    insert,
    update,
    delete,
    update_source,
    update_joined_source,
};

fn invalidPlanMatchesReason(
    plan: []const u8,
    family: InvalidPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    const prefix = switch (family) {
        .read => "invalid:read:reason=",
        .insert => "invalid:insert:reason=",
        .update => "invalid:update:reason=",
        .delete => "invalid:delete:reason=",
        .update_source => "invalid:update_source:reason=",
        .update_joined_source => "invalid:update_joined_source:reason=",
    };
    return std.mem.startsWith(u8, plan, prefix) and
        std.mem.eql(u8, plan[prefix.len..], @tagName(reason));
}

pub fn corpusPlanMatchesReason(
    family: AppParityCorpusPlanFamily,
    plan: []const u8,
    reason: []const u8,
) bool {
    const diagnostic_reason = diagnostics.classificationReasonFromToken(reason) orelse return false;
    switch (family) {
        .adapter_noop_ddl => return adapterNoopPlanMatchesReason(plan, "ddl", diagnostic_reason),
        .invalid_read => return invalidPlanMatchesReason(plan, .read, diagnostic_reason),
        .invalid_insert => return invalidPlanMatchesReason(plan, .insert, diagnostic_reason),
        .invalid_update => return invalidPlanMatchesReason(plan, .update, diagnostic_reason),
        .invalid_delete => return invalidPlanMatchesReason(plan, .delete, diagnostic_reason),
        .invalid_update_source => return invalidPlanMatchesReason(plan, .update_source, diagnostic_reason),
        .invalid_update_joined_source => return invalidPlanMatchesReason(plan, .update_joined_source, diagnostic_reason),
        else => if (corpusUnsupportedPlanFamily(family)) |unsupported_family| {
            return unsupportedPlanMatchesReason(plan, unsupported_family, diagnostic_reason);
        } else return true,
    }
}

pub fn corpusPlanMatchesFamily(family: AppParityCorpusPlanFamily, plan: []const u8) bool {
    if (corpusUnsupportedPlanFamily(family)) |unsupported_family| {
        return unsupportedPlanMatchesFamily(plan, unsupported_family);
    }

    const prefix = switch (family) {
        .ddl => "ddl:",
        .query_function => "query_function:",
        .read => "read:",
        .query => "query:",
        .aggregate => "aggregate:",
        .join => "join:",
        .lateral => "lateral:",
        .window => "window:",
        .explain => "explain:",
        .relation_population => "relation_population:",
        .insert => "insert:",
        .insert_source => "insert_source:",
        .recursive_insert_source => "recursive_insert_source:",
        .update => "update:",
        .delete => "delete:",
        .update_source => "update_source:",
        .delete_source => "delete_source:",
        .truncate_source => "truncate_source:",
        .update_joined_source => "update_joined_source:",
        .delete_joined_source => "delete_joined_source:",
        .merge_mutation => if (std.mem.startsWith(u8, plan, "recursive_merge_mutation:")) return true else "merge_mutation:",
        .adapter_noop_ddl => "adapter_noop:ddl:",
        .invalid_read => "invalid:read:",
        .invalid_insert => "invalid:insert:",
        .invalid_update => "invalid:update:",
        .invalid_delete => "invalid:delete:",
        .invalid_update_source => "invalid:update_source:",
        .invalid_update_joined_source => "invalid:update_joined_source:",
        .unsupported,
        .unsupported_read,
        .unsupported_ddl,
        .unsupported_write,
        .unsupported_insert,
        .unsupported_update,
        .unsupported_update_source,
        .unsupported_delete,
        .unsupported_update_joined_source,
        .unsupported_delete_joined_source,
        .unsupported_merge_mutation,
        => unreachable,
    };
    return std.mem.startsWith(u8, plan, prefix);
}

pub fn corpusFixtureFamilyNeedsTableSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .explain,
        .relation_population,
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .ddl,
        .query_function,
        .read,
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .explain,
        .relation_population,
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSourceSchema(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .read,
        .join,
        .lateral,
        .insert_source,
        .recursive_insert_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSetupSql(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .unsupported_ddl,
        .adapter_noop_ddl,
        => false,
        else => true,
    };
}

pub fn corpusFixtureFamilyAllowsReturningRows(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .insert,
        .update,
        .delete,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsResolverHint(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .insert,
        .update,
        .delete,
        .query_function,
        .invalid_insert,
        .invalid_update,
        .unsupported_insert,
        .unsupported_update,
        .unsupported_delete,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsOperationsSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .ddl,
        .explain,
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .update_source,
        .update_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusExplainWriteInnerHasPrefix(entry: AppParityCorpusEntry, inner_prefix: []const u8) bool {
    const inner_token = ":inner=";
    if (!std.mem.startsWith(u8, inner_prefix, inner_token)) return false;
    const expected = planRootKindFromExactPrefix(inner_prefix[inner_token.len..]) orelse return false;
    return entry.family == .explain and
        explainPlanHasKind(entry.plan, "write") and
        explainPlanInnerHasRootKind(entry.plan, expected);
}

pub fn corpusReadPlanHasPrefix(entry: AppParityCorpusEntry, read_prefix: []const u8) bool {
    const expected = readPlanKindFromExactPrefix(read_prefix) orelse return false;
    return (entry.family == .read and readPlanHasKind(entry.plan, expected)) or
        (entry.family == .explain and
            explainPlanHasKind(entry.plan, "read") and
            explainPlanInnerReadHasKind(entry.plan, expected));
}

fn planRootKindFromExactPrefix(prefix: []const u8) ?[]const u8 {
    const end = std.mem.indexOfScalar(u8, prefix, ':') orelse return null;
    if (end == 0 or end + 1 != prefix.len) return null;
    return prefix[0..end];
}

fn readPlanKindFromExactPrefix(prefix: []const u8) ?[]const u8 {
    const root = "read:";
    if (!std.mem.startsWith(u8, prefix, root)) return null;
    const sub_start = root.len;
    const sub_end = std.mem.indexOfScalarPos(u8, prefix, sub_start, ':') orelse return null;
    if (sub_end == sub_start or sub_end + 1 != prefix.len) return null;
    return prefix[sub_start..sub_end];
}

pub fn corpusOptionalZeroSummaryMatchesPlan(plan_text: []const u8, token_text: []const u8, expected: usize) bool {
    return switch (scanUsizeToken(plan_text, token_text)) {
        .value => |value| value == expected,
        .absent => expected == 0,
        .invalid => false,
    };
}

pub fn corpusOptionalBool01SummaryMatchesPlan(plan_text: []const u8, token_text: []const u8, expected: bool) bool {
    const value = planUsizeTokenValue(plan_text, token_text) orelse return !expected;
    return value == @intFromBool(expected);
}

pub fn corpusFixtureHasAccessSummary(summary: AppParityPlanSummary) bool {
    return summary.array_any != null or
        summary.in_predicates != null or
        summary.json_path_eq != null or
        summary.json_contains != null or
        summary.json_path_exists != null or
        summary.array_contains != null or
        summary.array_eq != null or
        summary.text_patterns != null or
        summary.access_or_predicates != null or
        summary.access_not_predicates != null or
        summary.expression_predicates != null or
        summary.expression_or_predicates != null or
        summary.expression_not_predicates != null or
        summary.expression_array_contains != null;
}

pub fn corpusFixtureHasTemporalDdlSummary(entry: AppParityCorpusEntry) bool {
    return entry.summary.temporal_periods != null or
        entry.summary.temporal_primary_key != null or
        entry.summary.temporal_unique != null or
        entry.summary.temporal_foreign_keys != null;
}

pub fn corpusFixturePlanMatchesSourceTable(entry: AppParityCorpusEntry, source_table_name: []const u8) bool {
    return switch (entry.family) {
        .insert_source => planHasExactStringToken(entry.plan, ":source_table=", source_table_name),
        .recursive_insert_source => planHasExactStringToken(entry.plan, ":source_table=", source_table_name) or
            planHasExactStringToken(entry.plan, ":anchor_table=", source_table_name),
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => planHasExactStringToken(entry.plan, ":source=", source_table_name),
        .read => planHasExactStringToken(entry.plan, ":right=", source_table_name) or
            setOperationPlanHasRightTable(entry.plan, source_table_name),
        .join,
        .lateral,
        => planHasExactStringToken(entry.plan, ":right=", source_table_name),
        else => false,
    };
}

fn setOperationPlanHasRightTable(plan: []const u8, source_table_name: []const u8) bool {
    const token = ":right=right:table=";
    const index = std.mem.indexOf(u8, plan, token) orelse return false;
    const value_start = index + token.len;
    const value_end = std.mem.indexOfScalarPos(u8, plan, value_start, ':') orelse plan.len;
    return std.mem.eql(u8, plan[value_start..value_end], source_table_name);
}

pub fn corpusFixtureSqlParameterCoverageMatchesAlloc(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !bool {
    var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, entry.sql) catch return false;
    defer parsed_sql.deinit(alloc);
    return corpusFixtureSqlParameterCoverageMatchesParsedSql(entry, &parsed_sql);
}

pub fn corpusFixtureSqlParameterCoverageMatchesParsedSql(entry: AppParityCorpusEntry, parsed_sql: *const tokenized.ParsedSql) bool {
    if (entry.family == .ddl and entry.summary.ddl_tag == .prepare_statement) {
        if (entry.params.len != 0) return false;
        const prepared_params = planUsizeTokenValue(entry.plan, ":params=") orelse return false;
        return sqlParameterCoverageMatchesParsedSql(parsed_sql, prepared_params);
    }
    return sqlParameterCoverageMatchesParsedSql(parsed_sql, entry.params.len);
}

pub fn sqlParameterCoverageMatchesParsedSql(parsed_sql: *const tokenized.ParsedSql, param_count: usize) bool {
    return sqlParameterCoverageMatchesTokens(parsed_sql.items(), param_count);
}

pub fn corpusDdlFixtureRequiresAppliedPlan(entry: AppParityCorpusEntry) !bool {
    if (entry.family != .ddl) return false;
    if (entry.summary.temporal_foreign_keys) |temporal_foreign_keys| {
        if (temporal_foreign_keys > 0) return false;
    }
    return switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
        .drop_table => true,
        .create_view, .rename_view, .drop_view => false,
        .create_materialized_view, .refresh_materialized_view, .drop_materialized_view => false,
        .relation_lifetime => false,
        .create_enum_type, .add_enum_value, .drop_enum_type => false,
        .create_domain, .alter_domain, .drop_domain => false,
        .create_sequence, .alter_sequence, .drop_sequence => false,
        .identity_allocator => false,
        .create_schema_namespace, .rename_schema_namespace, .drop_schema_namespace => false,
        .create_extension, .alter_extension_update, .drop_extension => false,
        .create_function, .drop_function, .create_procedure, .drop_procedure, .call_procedure => false,
        .create_role, .alter_role, .drop_role, .grant_privilege, .revoke_privilege => false,
        .copy_from, .copy_to => false,
        .prepare_transaction, .commit_prepared, .rollback_prepared => false,
        .create_partitioned_table, .create_table_partition, .attach_table_partition, .detach_table_partition => false,
        .enable_row_security, .disable_row_security, .create_row_policy, .alter_row_policy, .drop_row_policy => false,
        .create_database, .alter_database, .drop_database => false,
        .create_tablespace, .rename_tablespace, .drop_tablespace => false,
        .listen_notification, .notify_notification, .unlisten_notification => false,
        .create_publication, .alter_publication, .drop_publication => false,
        .create_subscription, .alter_subscription, .drop_subscription => false,
        .create_collation, .rename_collation, .drop_collation => false,
        .create_operator, .drop_operator => false,
        .create_aggregate, .drop_aggregate => false,
        .create_cast, .drop_cast => false,
        .vacuum_maintenance, .analyze_maintenance, .reindex_maintenance, .cluster_maintenance => false,
        .prepare_statement, .execute_statement, .deallocate_statement => false,
        .declare_cursor, .fetch_cursor, .close_cursor => false,
        .savepoint_transaction, .release_savepoint, .rollback_to_savepoint => false,
        .set_search_path, .set_setting, .reset_search_path, .reset_setting, .show_search_path, .discard_all => false,
        .comment_metadata => true,
        .table_lock, .constraint_mode, .transaction_mode, .advisory_lock => false,
        .create_table,
        .table_clone,
        .create_index,
        .drop_index,
        .alter_table,
        .create_update_policy,
        => true,
    };
}

pub fn corpusDdlFixtureAppliesFromEmptyCatalog(entry: AppParityCorpusEntry) !bool {
    if (entry.family != .ddl) return false;
    return switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
        .create_table => !planHasExactBoolToken(entry.plan, ":if_not_exists=", true) and
            !planHasExactBoolToken(entry.plan, ":replace=", true),
        else => false,
    };
}

pub fn corpusFixtureAllowsExecutionPlan(entry: AppParityCorpusEntry) bool {
    if (entry.family != .ddl) return false;
    return switch (entry.summary.ddl_tag orelse return false) {
        .copy_from, .copy_to => true,
        .prepare_transaction, .commit_prepared, .rollback_prepared => true,
        else => false,
    };
}

pub fn corpusFixtureRequiresExecutionPlan(entry: AppParityCorpusEntry) bool {
    return corpusFixtureAllowsExecutionPlan(entry);
}

pub fn corpusFixtureExecutionPlanIsStructured(entry: AppParityCorpusEntry) bool {
    if (entry.execution_plan.len == 0) return true;
    if (!corpusFixtureAllowsExecutionPlan(entry)) return false;
    return bulkSqlIoExecutionPlanIsStructured(entry.execution_plan) or
        preparedTransactionRecoveryPlanIsStructured(entry.execution_plan) or
        unsupportedPlanMatchesReason(entry.execution_plan, .ddl, .bulk_io_plan);
}

const AppParityCorpusMetadataMode = enum {
    source,
    generated_fixture,
};

fn validateCorpusMetadataCoreParsedSql(
    entry: AppParityCorpusEntry,
    mode: AppParityCorpusMetadataMode,
    parsed_sql: *const tokenized.ParsedSql,
) !void {
    if (entry.name.len == 0 or entry.sql.len == 0 or entry.plan.len == 0) return error.TestUnexpectedResult;
    if (!corpusPlanMatchesFamily(entry.family, entry.plan)) return error.TestUnexpectedResult;
    if (corpusFixtureFamilyNeedsReason(entry.family) and entry.classification_reason.len == 0) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureFamilyNeedsReason(entry.family) and entry.classification_reason.len > 0) {
        return error.TestUnexpectedResult;
    }
    if (entry.classification_reason.len > 0 and !corpusStableReasonToken(entry.classification_reason)) {
        return error.TestUnexpectedResult;
    }
    if (entry.classification_reason.len > 0 and !corpusReasonHasNativeRequirement(entry.classification_reason)) {
        return error.TestUnexpectedResult;
    }
    if (corpusFixtureFamilyNeedsReason(entry.family) and
        !corpusPlanMatchesReason(entry.family, entry.plan, entry.classification_reason))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.family == .ddl and entry.summary.ddl_tag == null) return error.TestUnexpectedResult;
    if (entry.summary.ddl_tag != null and entry.family != .ddl) return error.TestUnexpectedResult;
    if (corpusFixtureFamilyNeedsTableSummary(entry.family) and entry.summary.table_name == null) {
        return error.TestUnexpectedResult;
    }
    if (summaryHasFields(entry.summary) and !corpusFixtureFamilyAllowsSummary(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.family == .relation_population and summaryHasNonTableFields(entry.summary)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.ctes) |ctes| {
        if (!planHasExactUsizeToken(entry.plan, ":ctes=", ctes)) return error.TestUnexpectedResult;
    }
    if (entry.summary.operations != null and !corpusFixtureFamilyAllowsOperationsSummary(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.operations) |operations| {
        if (!corpusFixtureOperationsSummaryMatchesPlan(entry, operations)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.returning != null or entry.summary.returning_all != null) and
        !corpusFixtureAllowsReturningSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.returning) |returning| {
        if (!corpusFixtureReturningSummaryMatchesPlan(entry, returning)) return error.TestUnexpectedResult;
    }
    if (entry.summary.returning_all) |returning_all| {
        if (!corpusFixtureReturningAllSummaryMatchesPlan(entry, returning_all)) return error.TestUnexpectedResult;
    }
    if (entry.summary.conflict_where != null and !corpusFixtureAllowsConflictWhereSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.conflict_where) |conflict_where| {
        if (!corpusFixtureConflictWhereSummaryMatchesPlan(entry, conflict_where)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.patch_expressions != null or
        entry.summary.increment_expressions != null or
        entry.summary.json_set_expressions != null) and
        !corpusFixtureAllowsMutationTransformSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureTransformSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.source_assignments != null and !corpusFixtureAllowsSourceAssignmentsSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.source_assignments) |source_assignments| {
        if (!corpusFixtureSourceAssignmentsSummaryMatchesPlan(entry, source_assignments)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.matched_predicates != null or
        entry.summary.matched_delete != null or
        entry.summary.matched_do_nothing != null or
        entry.summary.not_matched_predicates != null or
        entry.summary.not_matched_do_nothing != null) and
        !corpusFixtureAllowsMergeArmSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureMergeArmSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if ((entry.summary.group_by != null or
        entry.summary.group_expressions != null or
        entry.summary.aggregations != null or
        entry.summary.filter_groups != null or
        entry.summary.having != null or
        entry.summary.having_expressions != null or
        entry.summary.having_any != null or
        entry.summary.having_not != null) and
        !corpusFixtureAllowsAggregateSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureAggregateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureDdlSelectSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureDdlPredicateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.predicates != null and !corpusFixtureAllowsPredicateSummary(entry)) return error.TestUnexpectedResult;
    if (!corpusFixturePredicateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (corpusFixtureHasAccessSummary(entry.summary) and !corpusFixtureAllowsAccessSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureAccessSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureSelectSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.join_select != null and !corpusFixtureAllowsJoinSelectSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.join_select) |join_select| {
        if (!corpusFixtureJoinSelectSummaryMatchesPlan(entry, join_select)) return error.TestUnexpectedResult;
    }
    if (entry.summary.join_on != null and !corpusFixtureAllowsJoinOnSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.join_on) |join_on| {
        if (!corpusFixtureJoinOnSummaryMatchesPlan(entry, join_on)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.lateral_correlations != null or entry.summary.right_offset != null) and
        !corpusFixtureAllowsLateralSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureLateralSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.windows != null and !corpusFixtureAllowsWindowSummary(entry)) return error.TestUnexpectedResult;
    if (entry.summary.windows) |windows| {
        if (!corpusFixtureWindowSummaryMatchesPlan(entry, windows)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.select_all != null or entry.summary.distinct_on != null) and
        !corpusFixtureAllowsFullQueryOutputSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureFullQueryOutputSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if ((entry.summary.order_by != null or entry.summary.limit != null or entry.summary.offset != null) and
        !corpusFixtureAllowsPaginationSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixturePaginationSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.row_claim_skip_locked != null and !corpusFixtureAllowsRowClaimSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureRowClaimSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (corpusFixtureHasTemporalDdlSummary(entry)) {
        if (entry.family != .ddl) return error.TestUnexpectedResult;
        if (entry.summary.temporal_periods) |periods| {
            if (!planHasExactUsizeToken(entry.plan, ":periods=", periods) and
                !planHasExactUsizeToken(entry.plan, ":add_period=", periods))
            {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_primary_key) |has_temporal_primary_key| {
            if (!planHasExactBoolToken(entry.plan, ":temporal_pk=", has_temporal_primary_key)) {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_unique) |temporal_unique| {
            if (!planHasExactUsizeToken(entry.plan, ":temporal_unique=", temporal_unique) and
                !(temporal_unique == 1 and planHasExactBoolToken(entry.plan, ":temporal_unique=", true)))
            {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_foreign_keys) |temporal_foreign_keys| {
            if (!planHasExactUsizeToken(entry.plan, ":temporal_fk=", temporal_foreign_keys)) {
                return error.TestUnexpectedResult;
            }
        }
    }
    if (entry.applied_plan.len > 0 and entry.family != .ddl) return error.TestUnexpectedResult;
    if (entry.applied_plan.len > 0 and !appliedPlanIsStructured(entry.applied_plan)) return error.TestUnexpectedResult;
    if (!corpusFixtureExecutionPlanIsStructured(entry)) return error.TestUnexpectedResult;
    if (entry.apply_setup_sql.len > 0 and !corpusFixtureFamilyAllowsSetupSql(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (mode == .generated_fixture and entry.family == .ddl and entry.apply_setup_sql.len > 0 and entry.applied_plan.len == 0) {
        return error.TestUnexpectedResult;
    }
    for (entry.apply_setup_sql) |setup_sql| {
        if (setup_sql.len == 0) return error.TestUnexpectedResult;
    }
    if (entry.source_schema_json.len > 0 and entry.catalog_tables.len > 0) {
        return error.TestUnexpectedResult;
    }
    if (appParityEntryHasCatalogSchemas(entry) and !corpusFixtureFamilyAllowsSourceSchema(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureCatalogTablesAreValid(entry)) return error.TestUnexpectedResult;
    if (entry.returning_rows.len > 0 and !corpusFixtureFamilyAllowsReturningRows(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.returning_rows.len > 0) {
        if (entry.summary.returning == null or entry.summary.returning.? != entry.returning_rows.len) {
            return error.TestUnexpectedResult;
        }
        if (!planHasExactUsizeToken(entry.plan, ":returning_rows=", entry.returning_rows.len)) {
            return error.TestUnexpectedResult;
        }
    }
    const has_resolver_hint = entry.resolver_row_json.len > 0 or
        entry.resolver_version != 0 or
        entry.resolver_exists != null;
    if (has_resolver_hint and !corpusFixtureFamilyAllowsResolverHint(entry.family)) return error.TestUnexpectedResult;
    if (has_resolver_hint and
        (entry.family == .insert or entry.family == .invalid_insert or entry.family == .unsupported_insert) and
        !corpusParsedSqlHasOnConflictTokens(parsed_sql))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.resolver_row_json.len > 0 and entry.resolver_version == 0) return error.TestUnexpectedResult;
    if (entry.resolver_row_json.len == 0 and entry.resolver_version != 0) return error.TestUnexpectedResult;
    if (entry.resolver_exists == false and
        (entry.resolver_row_json.len > 0 or entry.resolver_version != 0))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.resolver_exists == true and entry.resolver_row_json.len == 0) return error.TestUnexpectedResult;
    if (mode == .generated_fixture and try corpusDdlFixtureRequiresAppliedPlan(entry)) {
        if (entry.applied_plan.len == 0) return error.TestUnexpectedResult;
    }
    if (mode == .generated_fixture and corpusFixtureRequiresExecutionPlan(entry)) {
        if (entry.execution_plan.len == 0) return error.TestUnexpectedResult;
    }
}

pub fn validateSourceCorpusEntryMetadata(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
    var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, entry.sql) catch return error.TestUnexpectedResult;
    defer parsed_sql.deinit(alloc);
    return validateSourceCorpusEntryMetadataParsedSql(entry, &parsed_sql);
}

pub fn validateSourceCorpusEntryMetadataParsedSql(entry: AppParityCorpusEntry, parsed_sql: *const tokenized.ParsedSql) !void {
    return validateCorpusMetadataCoreParsedSql(entry, .source, parsed_sql);
}

pub fn validateFixtureMetadataCore(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
    var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, entry.sql) catch return error.TestUnexpectedResult;
    defer parsed_sql.deinit(alloc);
    return validateFixtureMetadataCoreParsedSql(entry, &parsed_sql);
}

pub fn validateFixtureMetadataCoreParsedSql(entry: AppParityCorpusEntry, parsed_sql: *const tokenized.ParsedSql) !void {
    return validateCorpusMetadataCoreParsedSql(entry, .generated_fixture, parsed_sql);
}

fn corpusParsedSqlHasOnConflictTokens(parsed_sql: *const tokenized.ParsedSql) bool {
    return appParityTokensHaveKeywordSequence(parsed_sql.items(), &.{ .on, .conflict });
}

fn validateSourceCorpusEntryJsonPayloads(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
    if (!(try corpusFixtureSqlParameterCoverageMatchesAlloc(alloc, entry))) return error.TestUnexpectedResult;
    try validateSourceCorpusEntryJsonPayloadsOnlyAlloc(alloc, entry);
}

fn validateSourceCorpusEntryJsonPayloadsParsedSql(alloc: std.mem.Allocator, entry: AppParityCorpusEntry, parsed_sql: *const tokenized.ParsedSql) !void {
    if (!corpusFixtureSqlParameterCoverageMatchesParsedSql(entry, parsed_sql)) return error.TestUnexpectedResult;
    try validateSourceCorpusEntryJsonPayloadsOnlyAlloc(alloc, entry);
}

fn validateSourceCorpusEntryJsonPayloadsOnlyAlloc(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
    for (entry.returning_rows) |row_json| {
        if (!(try fixtureJsonTextIsObjectAlloc(alloc, row_json))) return error.TestUnexpectedResult;
    }
    if (entry.resolver_row_json.len > 0 and !(try fixtureJsonTextIsObjectAlloc(alloc, entry.resolver_row_json))) {
        return error.TestUnexpectedResult;
    }
    for (entry.catalog_tables) |table| {
        if (!(try fixtureJsonTextIsObjectAlloc(alloc, table.schema_json))) return error.TestUnexpectedResult;
    }
}

pub fn fixtureSchemaJsonIsRelationalTableAlloc(alloc: std.mem.Allocator, text: []const u8) !bool {
    var parsed = schema_api.parseValidatedTableSchema(alloc, text) catch return false;
    defer parsed.deinit(alloc);
    const schema = schema_api.deriveRuntimeTableSchema(alloc, parsed) catch return false;
    defer runtime_schema.freeSchema(alloc, schema);
    return schema.storage_mode == .relational and schema.primary_key != null;
}

fn corpusFixtureCatalogTablesAreValid(entry: AppParityCorpusEntry) bool {
    if (entry.catalog_tables.len == 0) return true;
    for (entry.catalog_tables, 0..) |table, i| {
        if (table.name.len == 0 or table.schema_json.len == 0) return false;
        if (!corpusFixturePlanMatchesSourceTable(entry, table.name)) return false;
        var j: usize = i + 1;
        while (j < entry.catalog_tables.len) : (j += 1) {
            if (std.mem.eql(u8, table.name, entry.catalog_tables[j].name)) return false;
        }
    }
    return true;
}

pub fn fixtureJsonTextIsObjectAlloc(alloc: std.mem.Allocator, text: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, text, .{}) catch return false;
    defer parsed.deinit();
    return parsed.value == .object;
}

pub fn corpusFixtureDdlOperationsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_table,
        .relation_lifetime,
        => planUsizeOptionalTokenSumMatches(entry.plan, &.{ ":unique=", ":fk=", ":checks=" }, expected),
        .alter_table,
        .alter_domain,
        .alter_database,
        .alter_sequence,
        => planHasExactUsizeToken(entry.plan, ":ops=", expected),
        .create_sequence => planHasExactUsizeToken(entry.plan, ":options=", expected),
        .identity_allocator => (planBoolTokenUsize(entry.plan, ":primary=") orelse return false) == expected,
        .create_function,
        .create_procedure,
        .drop_function,
        .drop_procedure,
        .call_procedure,
        => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .alter_role => (planNonNoneStringTokenUsize(entry.plan, ":setting=") orelse return false) == expected,
        .grant_privilege,
        .revoke_privilege,
        => planHasExactUsizeToken(entry.plan, ":privileges=", expected),
        .copy_from,
        .copy_to,
        => planHasExactUsizeToken(entry.plan, ":columns=", expected),
        .create_partitioned_table => planHasExactUsizeToken(entry.plan, ":keys=", expected),
        .create_table_partition,
        .attach_table_partition,
        .detach_table_partition,
        => expected == 0,
        .enable_row_security,
        .disable_row_security,
        .create_row_policy,
        .alter_row_policy,
        .drop_row_policy,
        .create_update_policy,
        => expected == 1,
        .create_database,
        .drop_database,
        .create_tablespace,
        .rename_tablespace,
        .drop_tablespace,
        .listen_notification,
        .unlisten_notification,
        .alter_subscription,
        .drop_subscription,
        .drop_publication,
        .create_schema_namespace,
        .rename_schema_namespace,
        .drop_schema_namespace,
        .create_extension,
        .alter_extension_update,
        .drop_extension,
        .create_cast,
        .drop_cast,
        .deallocate_statement,
        .declare_cursor,
        .close_cursor,
        .savepoint_transaction,
        .release_savepoint,
        .rollback_to_savepoint,
        .set_search_path,
        .set_setting,
        .reset_search_path,
        .reset_setting,
        .show_search_path,
        .discard_all,
        => expected == 0,
        .create_publication => planHasExactUsizeToken(entry.plan, ":tables=", expected),
        .alter_publication => planHasExactUsizeToken(entry.plan, ":add_tables=", expected),
        .create_subscription => planHasExactUsizeToken(entry.plan, ":publications=", expected),
        .notify_notification => (planBoolTokenUsize(entry.plan, ":payload=") orelse return false) == expected,
        .create_collation,
        .create_operator,
        .create_aggregate,
        => planHasExactUsizeToken(entry.plan, ":options=", expected),
        .drop_operator,
        .drop_aggregate,
        => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .vacuum_maintenance => planBoolTokenSumMatches(entry.plan, &.{ ":full=", ":freeze=", ":verbose=", ":analyze=" }, expected),
        .analyze_maintenance => (planUsizeTokenValue(entry.plan, ":columns=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":verbose=") orelse return false) == expected,
        .reindex_maintenance => (planBoolTokenUsize(entry.plan, ":concurrently=") orelse return false) == expected,
        .cluster_maintenance => (planNonNoneStringTokenUsize(entry.plan, ":index=") orelse return false) == expected,
        .prepare_statement => planHasExactUsizeToken(entry.plan, ":params=", expected),
        .prepare_transaction, .commit_prepared, .rollback_prepared => expected == 1,
        .execute_statement => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .comment_metadata => (planBoolTokenUsize(entry.plan, ":comment=") orelse return false) == expected,
        .table_lock => planHasExactUsizeToken(entry.plan, ":tables=", expected),
        .constraint_mode => (planUsizeTokenValue(entry.plan, ":constraints=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":all=") orelse return false) == expected,
        .transaction_mode => planNonNoneStringTokenSumMatches(entry.plan, &.{ ":isolation=", ":access=", ":deferrable=" }, expected),
        .advisory_lock => planHasExactUsizeToken(entry.plan, ":keys=", expected),
        .fetch_cursor => planHasExactUsizeToken(entry.plan, ":count=", expected) or expected == 0,
        .create_index,
        .drop_index,
        .drop_table,
        .create_view,
        .rename_view,
        .drop_view,
        .create_materialized_view,
        .refresh_materialized_view,
        .drop_materialized_view,
        .table_clone,
        .create_enum_type,
        .add_enum_value,
        .drop_enum_type,
        .create_domain,
        .drop_domain,
        .drop_sequence,
        .create_role,
        .drop_role,
        .rename_collation,
        .drop_collation,
        => false,
    };
}

pub fn corpusFixtureOperationsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .ddl => corpusFixtureDdlOperationsSummaryMatchesPlan(entry, expected),
        .insert,
        .update,
        .update_source,
        .update_joined_source,
        => planHasExactUsizeToken(entry.plan, ":ops=", expected),
        .insert_source,
        .recursive_insert_source,
        => planHasExactUsizeToken(entry.plan, ":assignments=", expected),
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":matched_update=", expected),
        .explain => (corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") and planHasExactUsizeToken(entry.plan, ":assignments=", expected)) or
            (corpusExplainWriteInnerHasPrefix(entry, ":inner=recursive_insert_source:") and planHasExactUsizeToken(entry.plan, ":assignments=", expected)) or
            (corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") and planHasExactUsizeToken(entry.plan, ":ops=", expected)) or
            (corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:") and planHasExactUsizeToken(entry.plan, ":matched_update=", expected)),
        else => false,
    };
}

pub fn corpusFixtureAllowsReturningSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert,
        .insert_source,
        .recursive_insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        .explain => explainPlanHasKind(entry.plan, "write"),
        else => false,
    };
}

pub fn corpusFixtureReturningSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .insert,
        .update,
        .delete,
        => planHasExactUsizeToken(entry.plan, ":returning_rows=", expected),
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => planHasExactUsizeToken(entry.plan, ":returning=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":returning_rows=", expected) or
            planHasExactUsizeToken(entry.plan, ":returning=", expected),
        else => false,
    };
}

pub fn corpusFixtureReturningAllSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: bool) bool {
    return corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":returning_all=", expected);
}

pub fn corpusFixtureAllowsConflictWhereSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert,
        .insert_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"),
        else => false,
    };
}

pub fn corpusFixtureConflictWhereSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: bool) bool {
    return corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":conflict_where=", expected);
}

pub fn corpusFixtureAllowsMutationTransformSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert_source,
        .update_source,
        .update_joined_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureAllowsSourceAssignmentsSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .update_joined_source => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureSourceAssignmentsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .update_joined_source,
        .explain,
        => planHasExactUsizeToken(entry.plan, ":source_assignments=", expected),
        else => false,
    };
}

pub fn corpusFixtureTransformSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.patch_expressions) |patch_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_patch_expr="
        else
            ":patch_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, patch_expressions)) return false;
    }
    if (entry.summary.increment_expressions) |increment_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_increment_expr="
        else
            ":increment_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, increment_expressions)) return false;
    }
    if (entry.summary.json_set_expressions) |json_set_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_json_set_expr="
        else
            ":json_set_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, json_set_expressions)) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsMergeArmSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .merge_mutation => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:"),
        else => false,
    };
}

pub fn corpusFixtureMergeArmSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.matched_predicates) |matched_predicates| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_pred=", matched_predicates)) return false;
    }
    if (entry.summary.matched_delete) |matched_delete| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_delete=", @intFromBool(matched_delete))) return false;
    }
    if (entry.summary.matched_do_nothing) |matched_do_nothing| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_noop=", @intFromBool(matched_do_nothing))) return false;
    }
    if (entry.summary.not_matched_predicates) |not_matched_predicates| {
        if (!planHasExactUsizeToken(entry.plan, ":not_matched_pred=", not_matched_predicates)) return false;
    }
    if (entry.summary.not_matched_do_nothing) |not_matched_do_nothing| {
        if (!planHasExactUsizeToken(entry.plan, ":not_matched_noop=", @intFromBool(not_matched_do_nothing))) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsAggregateSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .aggregate => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:aggregate:"),
        else => false,
    };
}

pub fn corpusFixtureAggregateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.group_by) |group_by| {
        if (!planHasExactUsizeToken(entry.plan, ":group=", group_by)) return false;
    }
    if (entry.summary.group_expressions) |group_expressions| {
        if (!planHasExactUsizeToken(entry.plan, ":group_expr=", group_expressions)) return false;
    }
    if (entry.summary.aggregations) |aggregations| {
        if (!planHasExactUsizeToken(entry.plan, ":aggs=", aggregations)) return false;
    }
    if (entry.summary.filter_groups) |filter_groups| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":filter_groups=", filter_groups)) return false;
    }
    if (entry.summary.having) |having| {
        if (!planHasExactUsizeToken(entry.plan, ":having=", having)) return false;
    }
    if (entry.summary.having_expressions) |having_expressions| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_expr=", having_expressions)) return false;
    }
    if (entry.summary.having_any) |having_any| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_any=", having_any)) return false;
    }
    if (entry.summary.having_not) |having_not| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_not=", having_not)) return false;
    }
    return true;
}

pub fn corpusFixtureDdlSelectSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.select orelse return true;
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_table,
        .relation_lifetime,
        .create_partitioned_table,
        => planHasExactUsizeToken(entry.plan, ":columns=", expected),
        .identity_allocator => (planUsizeTokenValue(entry.plan, ":columns=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":primary=") orelse return false) == expected,
        .create_index => planUsizeOptionalTokenSumMatches(entry.plan, &.{ ":columns=", ":expr=", ":generated_expr=" }, expected),
        .create_view,
        .create_materialized_view,
        => planHasExactUsizeToken(entry.plan, ":fields=", expected),
        .create_enum_type => planHasExactUsizeToken(entry.plan, ":values=", expected),
        .create_aggregate => planHasExactUsizeToken(entry.plan, ":args=", expected),
        else => false,
    };
}

pub fn corpusFixtureDdlPredicateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.predicates orelse return true;
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_index => planHasExactUsizeToken(entry.plan, ":where=", expected),
        .create_domain => planHasExactUsizeToken(entry.plan, ":checks=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsPredicateSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .ddl,
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusReadPlanHasPrefix(entry, "read:set_operation:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusReadPlanHasPrefix(entry, "read:set_operation:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureSidePredicateSummaryMatchesPlan(plan: []const u8, expected: usize) bool {
    return planUsizeTokenSumMatches(plan, &.{ ":left_pred=", ":right_pred=" }, expected);
}

pub fn corpusFixturePredicateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.predicates orelse return true;
    return switch (entry.family) {
        .ddl => true,
        .query => planHasExactUsizeToken(entry.plan, ":pred=", expected),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, ":source_pred=", expected),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, ":pred=", expected)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, ":source_pred=", expected)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, ":pred=", expected) or
            planHasExactUsizeToken(entry.plan, ":source_pred=", expected) or
            corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsAccessSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusReadPlanHasPrefix(entry, "read:set_operation:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureSideAccessSummaryMatchesPlan(
    plan: []const u8,
    left_token: []const u8,
    right_token: []const u8,
    expected: usize,
) bool {
    return planUsizeOptionalTokenSumMatches(plan, &.{ left_token, right_token }, expected);
}

fn corpusFixtureSideTextPatternSummaryMatchesPlan(plan: []const u8, expected: usize) bool {
    return planUsizeOptionalTokenSumMatches(plan, &.{ ":left_text=", ":right_text=" }, expected) or
        planUsizeOptionalTokenSumMatches(plan, &.{ ":left_text_pattern=", ":right_text_pattern=" }, expected);
}

fn corpusFixtureAccessSummaryFieldMatchesPlan(
    entry: AppParityCorpusEntry,
    expected: ?usize,
    row_token: []const u8,
    source_token: []const u8,
    left_token: []const u8,
    right_token: []const u8,
) bool {
    const value = expected orelse return true;
    return switch (entry.family) {
        .query => planHasExactUsizeToken(entry.plan, row_token, value),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, source_token, value),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, row_token, value)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, source_token, value)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, row_token, value) or
            planHasExactUsizeToken(entry.plan, source_token, value) or
            corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value),
        else => false,
    };
}

fn corpusFixtureTextPatternSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const value = entry.summary.text_patterns orelse return true;
    return switch (entry.family) {
        .query => planHasExactUsizeToken(entry.plan, ":text_pattern=", value),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, ":text_pattern=", value)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, ":text_pattern=", value) or
            planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value) or
            corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value),
        else => false,
    };
}

pub fn corpusFixtureAccessSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const summary = entry.summary;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_any, ":array_any=", ":source_array_any=", ":left_array_any=", ":right_array_any=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.in_predicates, ":in=", ":source_in=", ":left_in=", ":right_in=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_path_eq, ":json_eq=", ":source_json_eq=", ":left_json_eq=", ":right_json_eq=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_contains, ":json_contains=", ":source_json_contains=", ":left_json_contains=", ":right_json_contains=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_path_exists, ":json_exists=", ":source_json_exists=", ":left_json_exists=", ":right_json_exists=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_contains, ":array_contains=", ":source_array_contains=", ":left_array_contains=", ":right_array_contains=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_eq, ":array_eq=", ":source_array_eq=", ":left_array_eq=", ":right_array_eq=")) return false;
    if (!corpusFixtureTextPatternSummaryMatchesPlan(entry)) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.access_or_predicates, ":access_or=", ":source_access_or=", ":left_access_or=", ":right_access_or=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.access_not_predicates, ":access_not=", ":source_access_not=", ":left_access_not=", ":right_access_not=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_predicates, ":expr_pred=", ":source_expr_pred=", ":left_expr_pred=", ":right_expr_pred=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_or_predicates, ":expr_or=", ":source_expr_or=", ":left_expr_or=", ":right_expr_or=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_not_predicates, ":expr_not=", ":source_expr_not=", ":left_expr_not=", ":right_expr_not=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_array_contains, ":expr_array=", ":source_expr_array=", ":left_expr_array=", ":right_expr_array=")) return false;
    return true;
}

pub fn corpusFixtureSelectSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.select orelse return true;
    return switch (entry.family) {
        .ddl => true,
        .query,
        .read,
        .window,
        => if (entry.family == .read and corpusReadPlanHasPrefix(entry, "read:set_operation:"))
            planAllUsizeTokensMatch(entry.plan, ":select=", expected)
        else
            planHasExactUsizeToken(entry.plan, ":select=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":select=", expected) or
            planHasExactUsizeToken(entry.plan, ":not_matched_insert=", expected),
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":not_matched_insert=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsJoinSelectSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .join,
        .lateral,
        => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"),
        else => false,
    };
}

pub fn corpusFixtureJoinSelectSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return planHasExactUsizeToken(entry.plan, ":select=", expected);
}

pub fn corpusFixtureAllowsJoinOnSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .join,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:join:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:"),
        else => false,
    };
}

pub fn corpusFixtureJoinOnSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":match=", expected),
        .join,
        .update_joined_source,
        .delete_joined_source,
        .read,
        => planHasExactUsizeToken(entry.plan, ":on=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":on=", expected) or
            planHasExactUsizeToken(entry.plan, ":match=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsLateralSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .lateral => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:lateral:"),
        else => false,
    };
}

pub fn corpusFixtureLateralSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.lateral_correlations) |correlations| {
        if (!planHasExactUsizeToken(entry.plan, ":corr=", correlations)) return false;
    }
    if (entry.summary.right_offset) |right_offset| {
        if (!planHasExactUsizeToken(entry.plan, ":right_offset=", @intCast(right_offset))) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsWindowSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .window => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:window:"),
        else => false,
    };
}

pub fn corpusFixtureWindowSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return planHasExactUsizeToken(entry.plan, ":windows=", expected);
}

pub fn corpusFixtureAllowsFullQueryOutputSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:"),
        else => false,
    };
}

pub fn corpusFixtureFullQueryOutputSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.select_all) |select_all| {
        if (!corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":select_all=", select_all)) return false;
    }
    if (entry.summary.distinct_on) |distinct_on| {
        if (!planHasExactUsizeToken(entry.plan, ":distinct_on=", distinct_on)) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsPaginationSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusReadPlanHasPrefix(entry, "read:set_operation:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureUsesSourcePagination(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:"),
        else => false,
    };
}

pub fn corpusFixturePaginationSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const source_pagination = corpusFixtureUsesSourcePagination(entry);
    const result_pagination = corpusFixtureUsesSetOperationResultPagination(entry);
    if (entry.summary.order_by) |order_by| {
        const token_text = if (source_pagination) ":source_order=" else if (result_pagination) ":result_order=" else ":order=";
        if (!planHasExactUsizeToken(entry.plan, token_text, order_by)) return false;
    }
    if (entry.summary.limit) |limit| {
        const token_text = if (source_pagination) ":source_limit=" else if (result_pagination) ":result_limit=" else ":limit=";
        if (!planHasExactUsizeToken(entry.plan, token_text, limit)) return false;
    }
    if (entry.summary.offset) |offset| {
        const token_text = if (source_pagination) ":source_offset=" else if (result_pagination) ":result_offset=" else ":offset=";
        if (!planHasExactUsizeToken(entry.plan, token_text, offset)) return false;
    }
    return true;
}

fn corpusFixtureUsesSetOperationResultPagination(entry: AppParityCorpusEntry) bool {
    return corpusReadPlanHasPrefix(entry, "read:set_operation:");
}

pub fn corpusFixtureAllowsRowClaimSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .join,
        .lateral,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureRowClaimSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const skip_locked = entry.summary.row_claim_skip_locked orelse return true;
    if (skip_locked) {
        return planHasExactStringToken(entry.plan, ":claim=", "skip_locked") or
            planHasExactStringToken(entry.plan, ":claim=", "no_key_update_skip_locked");
    }
    return planHasExactStringToken(entry.plan, ":claim=", "locked") or
        planHasExactStringToken(entry.plan, ":claim=", "nowait") or
        planHasExactStringToken(entry.plan, ":claim=", "no_key_update") or
        planHasExactStringToken(entry.plan, ":claim=", "no_key_update_nowait");
}

pub fn unsupportedPlanFamilyToken(family: UnsupportedPlanFamily) []const u8 {
    return @tagName(family);
}

pub fn unsupportedFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "unsupported:{s}:requires={s}", .{
        unsupportedPlanFamilyToken(family),
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "adapter_noop:{s}:reason={s}", .{
        family,
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn boolFingerprintValue(value: bool) u8 {
    return if (value) 1 else 0;
}

pub fn appendNonZeroU32FingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: u32,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendNonZeroUsizeFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: usize,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendNamedNonZeroUsizeFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    prefix: []const u8,
    label: []const u8,
    value: usize,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}_{s}={d}", .{ owned_base, prefix, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendTrueBoolFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: bool,
) ![]u8 {
    if (!value) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}=1", .{ owned_base, label });
    alloc.free(owned_base);
    return out;
}

pub fn appendBoolFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: bool,
) ![]u8 {
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, boolFingerprintValue(value) });
    alloc.free(owned_base);
    return out;
}

pub fn appendStringFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: []const u8,
) ![]u8 {
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={s}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

fn appendSelectAllExtraOutputsFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]u8 {
    if (!query.select_all) return owned_base;
    var fingerprint = owned_base;
    for (query.json_extract, 0..) |projection, index| {
        const label = try std.fmt.allocPrint(alloc, "select_all_json{d}", .{index});
        defer alloc.free(label);
        fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, label, projection.output);
    }
    for (query.array_length, 0..) |projection, index| {
        const label = try std.fmt.allocPrint(alloc, "select_all_array_len{d}", .{index});
        defer alloc.free(label);
        fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, label, projection.output);
    }
    for (query.coalesce, 0..) |projection, index| {
        const label = try std.fmt.allocPrint(alloc, "select_all_coalesce{d}", .{index});
        defer alloc.free(label);
        fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, label, projection.output);
    }
    for (query.field_aliases, 0..) |projection, index| {
        const label = try std.fmt.allocPrint(alloc, "select_all_alias{d}", .{index});
        defer alloc.free(label);
        fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, label, projection.output);
    }
    for (query.expressions, 0..) |projection, index| {
        const label = try std.fmt.allocPrint(alloc, "select_all_expr{d}", .{index});
        defer alloc.free(label);
        fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, label, projection.output);
    }
    return fingerprint;
}

pub fn appParityLimitValue(limit: ?u32) i64 {
    return if (limit) |value| @intCast(value) else -1;
}

pub fn appParityBoolValue(value: bool) u8 {
    return sql_adapter.boolFingerprintValue(value);
}

pub fn sqlJoinTypeFingerprintName(join_type: db_mod.types.RelationalRowsJoinType) []const u8 {
    return switch (join_type) {
        .inner => "inner",
        .left => "left",
    };
}

pub fn queryFingerprintAlloc(alloc: std.mem.Allocator, family: []const u8, table_name: []const u8, query: db_mod.types.RelationalRowsQueryRequest, ctes: usize) ![]u8 {
    const claim = if (query.row_claim) |claim_value| sqlRowClaimFingerprintName(claim_value) else "none";
    const order_expr = expressionOrderCount(query.order_by);
    const distinct_on_count = query.distinct_on.len + query.distinct_on_expressions.len;
    if (distinct_on_count > 0) {
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:distinct_on={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.expression_predicates.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                distinct_on_count,
                query.order_by.len,
                order_expr,
                appParityLimitValue(query.limit),
                claim,
            },
        ), query);
    }
    if (ctes > 0 or query.source_cte.len > 0) {
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:source_cte={d}:pred={d}:array_any={d}:expr_pred={d}:expr_or={d}:expr_not={d}:expr_array={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
            .{
                family,
                table_name,
                ctes,
                @as(u8, if (query.source_cte.len > 0) 1 else 0),
                query.predicates.len,
                query.array_any.len,
                query.expression_predicates.len,
                query.expression_or_predicates.len,
                query.expression_not_predicates.len,
                query.expression_array_contains.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                appParityLimitValue(query.limit),
                claim,
            },
        ), query);
    }
    if (query.array_any.len > 0 or query.expression_or_predicates.len > 0 or query.expression_not_predicates.len > 0) {
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:array_any={d}:expr_pred={d}:expr_or={d}:expr_not={d}:expr_array={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.array_any.len,
                query.expression_predicates.len,
                query.expression_or_predicates.len,
                query.expression_not_predicates.len,
                query.expression_array_contains.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                appParityLimitValue(query.limit),
                claim,
            },
        ), query);
    }
    if (query.expression_array_contains.len > 0) {
        if (query.limit) |limit| {
            return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
                alloc,
                "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:expr_array={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
                .{
                    family,
                    table_name,
                    ctes,
                    query.predicates.len,
                    query.expression_predicates.len,
                    query.expression_array_contains.len,
                    query.json_path_eq.len,
                    query.or_predicates.len,
                    query.not_predicates.len,
                    query.select.len,
                    query.expressions.len,
                    query.field_aliases.len,
                    query.order_by.len,
                    order_expr,
                    limit,
                    claim,
                },
            ), query);
        }
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:expr_array={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit=none:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.expression_predicates.len,
                query.expression_array_contains.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                claim,
            },
        ), query);
    }
    if (query.text_patterns.len > 0) {
        if (query.limit) |limit| {
            return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
                alloc,
                "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:json_eq={d}:text_pattern={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
                .{
                    family,
                    table_name,
                    ctes,
                    query.predicates.len,
                    query.expression_predicates.len,
                    query.json_path_eq.len,
                    query.text_patterns.len,
                    query.or_predicates.len,
                    query.not_predicates.len,
                    query.select.len,
                    query.expressions.len,
                    query.field_aliases.len,
                    query.order_by.len,
                    order_expr,
                    limit,
                    claim,
                },
            ), query);
        }
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:json_eq={d}:text_pattern={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit=none:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.expression_predicates.len,
                query.json_path_eq.len,
                query.text_patterns.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                claim,
            },
        ), query);
    }
    if (query.limit) |limit| {
        return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.expression_predicates.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                limit,
                claim,
            },
        ), query);
    }
    return try appendSelectAllExtraOutputsFingerprintAlloc(alloc, try std.fmt.allocPrint(
        alloc,
        "{s}:table={s}:ctes={d}:pred={d}:expr_pred={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit=none:claim={s}",
        .{
            family,
            table_name,
            ctes,
            query.predicates.len,
            query.expression_predicates.len,
            query.json_path_eq.len,
            query.or_predicates.len,
            query.not_predicates.len,
            query.select.len,
            query.expressions.len,
            query.field_aliases.len,
            query.order_by.len,
            order_expr,
            claim,
        },
    ), query);
}

pub fn appendQueryAccessPathFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]u8 {
    var fingerprint = owned_base;
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "in", query.in_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "json_contains", query.json_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "json_exists", query.json_path_exists.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "array_contains", query.array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "array_eq", query.array_eq.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "access_or", query.access_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "access_not", query.access_not_predicates.len);
    return fingerprint;
}

pub fn appendSourceQueryAccessPathFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]u8 {
    var fingerprint = owned_base;
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_array_any", query.array_any.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_in", query.in_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_json_eq", query.json_path_eq.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_json_contains", query.json_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_json_exists", query.json_path_exists.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_array_contains", query.array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_array_eq", query.array_eq.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_text_pattern", query.text_patterns.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_access_or", query.access_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_access_not", query.access_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_expr_pred", query.expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_expr_or", query.expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_expr_not", query.expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_expr_array", query.expression_array_contains.len);
    return fingerprint;
}

pub fn appendSourceQueryAccessOnlyFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]u8 {
    var fingerprint = owned_base;
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_json_contains", query.json_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_json_exists", query.json_path_exists.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_array_contains", query.array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_array_eq", query.array_eq.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_text_pattern", query.text_patterns.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_access_or", query.access_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_access_not", query.access_not_predicates.len);
    return fingerprint;
}

pub fn appendSideQueryAccessOnlyFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    prefix: []const u8,
    query: db_mod.types.RelationalRowsQueryRequest,
) ![]u8 {
    var fingerprint = owned_base;
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "in", query.in_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "json_contains", query.json_contains.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "json_exists", query.json_path_exists.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "array_contains", query.array_contains.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "array_eq", query.array_eq.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "text_pattern", query.text_patterns.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "access_or", query.access_or_predicates.len);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "access_not", query.access_not_predicates.len);
    return fingerprint;
}

pub fn appendCteAccessPathFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    ctes: []const db_mod.types.RelationalRowsCte,
) ![]u8 {
    var fingerprint = owned_base;
    for (ctes, 0..) |cte, i| {
        const prefix = try std.fmt.allocPrint(alloc, "cte{d}", .{i});
        defer alloc.free(prefix);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "array_any", cte.query.array_any.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "in", cte.query.in_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "json_eq", cte.query.json_path_eq.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "json_contains", cte.query.json_contains.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "json_exists", cte.query.json_path_exists.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "array_contains", cte.query.array_contains.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "array_eq", cte.query.array_eq.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "text_pattern", cte.query.text_patterns.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "access_or", cte.query.access_or_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "access_not", cte.query.access_not_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "expr_pred", cte.query.expression_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "expr_or", cte.query.expression_or_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "expr_not", cte.query.expression_not_predicates.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "expr_array", cte.query.expression_array_contains.len);
        fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, prefix, "alias", cte.query.field_aliases.len);
    }
    return fingerprint;
}

pub const TransformOpFingerprintCounts = struct {
    set: usize = 0,
    set_on_insert: usize = 0,
    unset: usize = 0,
    inc: usize = 0,
    push: usize = 0,
    pull: usize = 0,
    add_to_set: usize = 0,
    pop: usize = 0,
    mul: usize = 0,
    min: usize = 0,
    max: usize = 0,
    current_date: usize = 0,
    rename: usize = 0,
};

pub fn transformOpFingerprintCounts(transforms: []const db_mod.types.DocumentTransform) TransformOpFingerprintCounts {
    var counts: TransformOpFingerprintCounts = .{};
    for (transforms) |transform| {
        for (transform.operations) |operation| switch (operation.op) {
            .set => counts.set += 1,
            .set_on_insert => counts.set_on_insert += 1,
            .unset => counts.unset += 1,
            .inc => counts.inc += 1,
            .push => counts.push += 1,
            .pull => counts.pull += 1,
            .add_to_set => counts.add_to_set += 1,
            .pop => counts.pop += 1,
            .mul => counts.mul += 1,
            .min => counts.min += 1,
            .max => counts.max += 1,
            .current_date => counts.current_date += 1,
            .rename => counts.rename += 1,
        };
    }
    return counts;
}

pub fn appendTransformOpFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    transforms: []const db_mod.types.DocumentTransform,
) ![]u8 {
    const counts = transformOpFingerprintCounts(transforms);
    var fingerprint = owned_base;
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_set", counts.set);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_set_on_insert", counts.set_on_insert);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_unset", counts.unset);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_inc", counts.inc);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_push", counts.push);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_pull", counts.pull);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_add_to_set", counts.add_to_set);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_pop", counts.pop);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_mul", counts.mul);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_min", counts.min);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_max", counts.max);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_current_date", counts.current_date);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "op_rename", counts.rename);
    return fingerprint;
}

pub fn unsupportedPlanMatchesFamily(plan: []const u8, family: UnsupportedPlanFamily) bool {
    const prefix = "unsupported:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const family_token = unsupportedPlanFamilyToken(family);
    const rest = plan[prefix.len..];
    return std.mem.startsWith(u8, rest, family_token) and
        rest.len > family_token.len and
        rest[family_token.len] == ':';
}

pub fn unsupportedPlanMatchesReason(
    plan: []const u8,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return false;
    if (!unsupportedPlanMatchesFamily(plan, family)) return false;
    return planHasExactStringToken(plan, ":requires=", diagnostics.classificationReasonToken(reason));
}

pub fn adapterNoopPlanMatchesReason(
    plan: []const u8,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return false;
    const prefix = "adapter_noop:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const rest = plan[prefix.len..];
    if (!std.mem.startsWith(u8, rest, family) or rest.len <= family.len or rest[family.len] != ':') return false;
    return planHasExactStringToken(plan, ":reason=", diagnostics.classificationReasonToken(reason));
}

pub const PlanStringTokenScan = union(enum) {
    absent,
    value: []const u8,
    invalid,
};

pub fn aggregateFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredAggregate) ![]u8 {
    const source = lowered.aggregate.source;
    const filter_groups = aggregateFilterGroupCount(lowered.aggregate.aggregations);
    const filter_expression_arrays = aggregateFilterExpressionArrayCount(lowered.aggregate.aggregations);
    const filter_json_access = aggregateFilterJsonAccessCount(lowered.aggregate.aggregations);
    const filter_structured_access = aggregateFilterStructuredAccessCount(lowered.aggregate.aggregations);
    if (lowered.aggregate.having_expressions.len > 0 or lowered.aggregate.having_any.len > 0 or lowered.aggregate.having_not.len > 0 or filter_groups > 0 or filter_expression_arrays > 0 or filter_json_access > 0 or filter_structured_access > 0) {
        const base = try std.fmt.allocPrint(
            alloc,
            "aggregate:table={s}:source_pred={d}:source_array_any={d}:source_expr_pred={d}:source_expr_or={d}:source_expr_not={d}:source_expr_array={d}:source_json_eq={d}:group={d}:group_expr={d}:aggs={d}:agg_expr={d}:filter_expr={d}:filter_groups={d}:having={d}:having_expr={d}:having_any={d}:having_not={d}:order={d}:limit={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.array_any.len,
                source.expression_predicates.len,
                source.expression_or_predicates.len,
                source.expression_not_predicates.len,
                source.expression_array_contains.len,
                source.json_path_eq.len,
                lowered.aggregate.group_by.len,
                lowered.aggregate.group_expressions.len,
                lowered.aggregate.aggregations.len,
                aggregateInputExpressionCount(lowered.aggregate.aggregations),
                aggregateFilterExpressionCount(lowered.aggregate.aggregations),
                filter_groups,
                lowered.aggregate.having_predicates.len,
                lowered.aggregate.having_expressions.len,
                lowered.aggregate.having_any.len,
                lowered.aggregate.having_not.len,
                lowered.aggregate.order_by.len,
                appParityLimitValue(lowered.aggregate.limit),
            },
        );
        var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "order_expr", expressionOrderCount(lowered.aggregate.order_by));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "filter_expr_array", filter_expression_arrays);
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "filter_json", filter_json_access);
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "filter_structured", filter_structured_access);
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_desc", aggregateDescendingPercentileCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_array", aggregatePercentileArrayCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "mode", aggregateModeCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_in", source.in_predicates.len);
        return try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, source);
    }
    if (sourceQueryUsesExtendedPredicates(source)) {
        const base = try std.fmt.allocPrint(
            alloc,
            "aggregate:table={s}:source_pred={d}:source_array_any={d}:source_expr_pred={d}:source_expr_or={d}:source_expr_not={d}:source_expr_array={d}:source_json_eq={d}:group={d}:group_expr={d}:aggs={d}:agg_expr={d}:filter_expr={d}:having={d}:order={d}:limit={d}",
            .{
                lowered.table_name,
                source.predicates.len,
                source.array_any.len,
                source.expression_predicates.len,
                source.expression_or_predicates.len,
                source.expression_not_predicates.len,
                source.expression_array_contains.len,
                source.json_path_eq.len,
                lowered.aggregate.group_by.len,
                lowered.aggregate.group_expressions.len,
                lowered.aggregate.aggregations.len,
                aggregateInputExpressionCount(lowered.aggregate.aggregations),
                aggregateFilterExpressionCount(lowered.aggregate.aggregations),
                lowered.aggregate.having_predicates.len,
                lowered.aggregate.order_by.len,
                appParityLimitValue(lowered.aggregate.limit),
            },
        );
        var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "order_expr", expressionOrderCount(lowered.aggregate.order_by));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_desc", aggregateDescendingPercentileCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_array", aggregatePercentileArrayCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "mode", aggregateModeCount(lowered.aggregate.aggregations));
        fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_in", source.in_predicates.len);
        return try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, source);
    }
    const base = try std.fmt.allocPrint(
        alloc,
        "aggregate:table={s}:source_pred={d}:source_json_eq={d}:group={d}:group_expr={d}:aggs={d}:agg_expr={d}:filter_expr={d}:having={d}:order={d}:limit={d}",
        .{
            lowered.table_name,
            source.predicates.len,
            source.json_path_eq.len,
            lowered.aggregate.group_by.len,
            lowered.aggregate.group_expressions.len,
            lowered.aggregate.aggregations.len,
            aggregateInputExpressionCount(lowered.aggregate.aggregations),
            aggregateFilterExpressionCount(lowered.aggregate.aggregations),
            lowered.aggregate.having_predicates.len,
            lowered.aggregate.order_by.len,
            appParityLimitValue(lowered.aggregate.limit),
        },
    );
    var fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, base, "order_expr", expressionOrderCount(lowered.aggregate.order_by));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_desc", aggregateDescendingPercentileCount(lowered.aggregate.aggregations));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "percentile_array", aggregatePercentileArrayCount(lowered.aggregate.aggregations));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "mode", aggregateModeCount(lowered.aggregate.aggregations));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "source_in", source.in_predicates.len);
    return try appendSourceQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, source);
}

pub fn aggregatePlanFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredAggregatePlan) ![]u8 {
    const aggregate = LoweredAggregate{
        .table_name = lowered.table_name,
        .aggregate = lowered.plan.aggregate,
    };
    var fingerprint = try aggregateFingerprintAlloc(alloc, aggregate);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "ctes", lowered.plan.ctes.len);
    fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, lowered.plan.ctes);
    return fingerprint;
}

pub fn joinFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredJoin) ![]u8 {
    const left = lowered.join.left;
    const right = lowered.join.right;
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "join:type={s}:left={s}:right={s}:left_pred={d}:left_array_any={d}:left_expr_pred={d}:left_expr_or={d}:left_expr_not={d}:left_expr_array={d}:left_json_eq={d}:left_text={d}:right_pred={d}:right_array_any={d}:right_expr_pred={d}:right_expr_or={d}:right_expr_not={d}:right_expr_array={d}:right_json_eq={d}:right_text={d}:on={d}:select={d}:order={d}:order_expr={d}:limit={d}",
        .{
            sqlJoinTypeFingerprintName(lowered.join.join_type),
            lowered.left_table_name,
            lowered.right_table_name,
            left.predicates.len,
            left.array_any.len,
            left.expression_predicates.len,
            left.expression_or_predicates.len,
            left.expression_not_predicates.len,
            left.expression_array_contains.len,
            left.json_path_eq.len,
            left.text_patterns.len,
            right.predicates.len,
            right.array_any.len,
            right.expression_predicates.len,
            right.expression_or_predicates.len,
            right.expression_not_predicates.len,
            right.expression_array_contains.len,
            right.json_path_eq.len,
            right.text_patterns.len,
            lowered.join.on.len,
            lowered.join.select.len,
            lowered.join.order_by.len,
            expressionOrderCount(lowered.join.order_by),
            appParityLimitValue(lowered.join.limit),
        },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_pred", lowered.join.on_expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_or", lowered.join.on_expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_not", lowered.join.on_expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "on_expr_array", lowered.join.on_expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_pred", lowered.join.match_expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_or", lowered.join.match_expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_not", lowered.join.match_expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_array", lowered.join.match_expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "ctes", lowered.ctes.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left_source_cte", if (lowered.join.left.source_cte.len > 0) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right_source_cte", if (lowered.join.right.source_cte.len > 0) 1 else 0);
    fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, lowered.ctes);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "left", left);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "right", right);
    return fingerprint;
}

pub fn lateralFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredLateralPlan) ![]u8 {
    const left = lowered.plan.lateral.left;
    const right = lowered.plan.lateral.right;
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "lateral:left={s}:right={s}:ctes={d}:left_pred={d}:left_array_any={d}:left_expr_pred={d}:left_expr_or={d}:left_expr_not={d}:left_expr_array={d}:left_json_eq={d}:left_text={d}:right_pred={d}:right_array_any={d}:right_expr_pred={d}:right_expr_or={d}:right_expr_not={d}:right_expr_array={d}:right_json_eq={d}:right_text={d}:right_order={d}:right_order_expr={d}:right_limit={d}:corr={d}:select={d}:order={d}:order_expr={d}:limit={d}",
        .{
            lowered.left_table_name,
            lowered.right_table_name,
            lowered.plan.ctes.len,
            left.predicates.len,
            left.array_any.len,
            left.expression_predicates.len,
            left.expression_or_predicates.len,
            left.expression_not_predicates.len,
            left.expression_array_contains.len,
            left.json_path_eq.len,
            left.text_patterns.len,
            right.predicates.len,
            right.array_any.len,
            right.expression_predicates.len,
            right.expression_or_predicates.len,
            right.expression_not_predicates.len,
            right.expression_array_contains.len,
            right.json_path_eq.len,
            right.text_patterns.len,
            right.order_by.len,
            expressionOrderCount(right.order_by),
            appParityLimitValue(right.limit),
            lowered.plan.lateral.correlations.len,
            lowered.plan.lateral.select.len,
            lowered.plan.lateral.order_by.len,
            expressionOrderCount(lowered.plan.lateral.order_by),
            appParityLimitValue(lowered.plan.lateral.limit),
        },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left_source_cte", if (left.source_cte.len > 0) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "right_source_cte", if (right.source_cte.len > 0) 1 else 0);
    fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, lowered.plan.ctes);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "left", left);
    fingerprint = try appendSideQueryAccessOnlyFingerprintAlloc(alloc, fingerprint, "right", right);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_pred", lowered.plan.lateral.match_expression_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_or", lowered.plan.lateral.match_expression_or_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_not", lowered.plan.lateral.match_expression_not_predicates.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_array", lowered.plan.lateral.match_expression_array_contains.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "match_expr_or_lower", predicateGroupsExpressionKindCount(lowered.plan.lateral.match_expression_or_predicates, .lower));
    return fingerprint;
}

pub fn readFingerprintWithPrefixAlloc(
    alloc: std.mem.Allocator,
    owned_fingerprint: []u8,
    family: []const u8,
) ![]u8 {
    defer alloc.free(owned_fingerprint);
    return try std.fmt.allocPrint(alloc, "read:{s}:{s}", .{ family, owned_fingerprint });
}

fn expressionContainsKind(
    expression: db_mod.types.RelationalRowsExpression,
    kind: db_mod.types.RelationalRowsExpressionKind,
) bool {
    if (expression.kind == kind) return true;
    for (expression.operands) |operand| {
        if (expressionContainsKind(operand, kind)) return true;
    }
    for (expression.case_branches) |branch| {
        if (conditionContainsExpressionKind(branch.when, kind)) return true;
        if (expressionContainsKind(branch.then, kind)) return true;
    }
    for (expression.case_else) |case_else| {
        if (expressionContainsKind(case_else, kind)) return true;
    }
    return false;
}

fn conditionContainsExpressionKind(
    condition: db_mod.types.RelationalRowsExpressionCondition,
    kind: db_mod.types.RelationalRowsExpressionKind,
) bool {
    if (expressionContainsKind(condition.lhs, kind)) return true;
    for (condition.rhs) |rhs| {
        if (expressionContainsKind(rhs, kind)) return true;
    }
    return false;
}

fn predicateGroupsExpressionKindCount(
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
    kind: db_mod.types.RelationalRowsExpressionKind,
) usize {
    var count: usize = 0;
    for (groups) |group| {
        for (group.conditions) |condition| {
            if (conditionContainsExpressionKind(condition, kind)) {
                count += 1;
                break;
            }
        }
    }
    return count;
}

fn windowValueExpressionKindCount(
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    kind: db_mod.types.RelationalRowsExpressionKind,
) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.value_expression) |expression| {
            if (expressionContainsKind(expression, kind)) count += 1;
        }
    }
    return count;
}

fn windowFunctionCount(
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    function: db_mod.types.RelationalRowsWindowFunction,
) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.function == function) count += 1;
    }
    return count;
}

fn windowScalarMinMaxCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    return windowFunctionCount(windows, .min) + windowFunctionCount(windows, .max);
}

fn windowBooleanAggregateFunctionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    return windowFunctionCount(windows, .bool_or) + windowFunctionCount(windows, .bool_and);
}

pub fn windowFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredWindowPlan) ![]u8 {
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "window:table={s}:ctes={d}:source_cte={d}:source_pred={d}:windows={d}:window_expr={d}:window_default={d}:window_frame_sig={d}:select={d}:order={d}:limit={d}",
        .{
            lowered.table_name,
            lowered.plan.ctes.len,
            @as(u8, if (lowered.plan.window.source.source_cte.len > 0) 1 else 0),
            lowered.plan.window.source.predicates.len,
            lowered.plan.window.windows.len,
            windowValueExpressionCount(lowered.plan.window.windows),
            windowDefaultCount(lowered.plan.window.windows),
            windowFrameSignature(lowered.plan.window.windows),
            lowered.plan.window.select.len,
            lowered.plan.window.order_by.len,
            appParityLimitValue(lowered.plan.window.limit),
        },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "order_expr", expressionOrderCount(lowered.plan.window.order_by));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_filter", windowFilterPredicateCount(lowered.plan.window.windows));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_filter_expr", windowFilterExpressionCount(lowered.plan.window.windows));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_filter_access", windowFilterAccessCount(lowered.plan.window.windows));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_filter_groups", windowFilterGroupCount(lowered.plan.window.windows));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_expr_mod", windowValueExpressionKindCount(lowered.plan.window.windows, .mod));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_scalar_minmax", windowScalarMinMaxCount(lowered.plan.window.windows));
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "window_bool_agg", windowBooleanAggregateFunctionCount(lowered.plan.window.windows));
    fingerprint = try appendSourceQueryAccessPathFingerprintAlloc(alloc, fingerprint, lowered.plan.window.source);
    fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, lowered.plan.ctes);
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", lowered.plan.window.offset);
    return fingerprint;
}

pub fn readPlanFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredReadPlan) ![]u8 {
    return switch (lowered) {
        .query => |query| blk: {
            var fingerprint = try queryFingerprintAlloc(alloc, "query", query.table_name, query.plan.query, query.plan.ctes.len);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", query.plan.query.offset);
            fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "select_all", query.plan.query.select_all);
            fingerprint = try appendQueryAccessPathFingerprintAlloc(alloc, fingerprint, query.plan.query);
            fingerprint = try appendCteAccessPathFingerprintAlloc(alloc, fingerprint, query.plan.ctes);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "query");
        },
        .document_query => try alloc.dupe(u8, "document_query"),
        .document_aggregate => try alloc.dupe(u8, "document_aggregate"),
        .set_operation => |set_operation| blk: {
            const fingerprint = try setOperationFingerprintAlloc(alloc, set_operation);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "set_operation");
        },
        .recursive_cte => |recursive_cte| blk: {
            const fingerprint = try recursiveCteFingerprintAlloc(alloc, recursive_cte);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "recursive_cte");
        },
        .aggregate => |aggregate| blk: {
            var fingerprint = try aggregatePlanFingerprintAlloc(alloc, aggregate);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", aggregate.plan.aggregate.offset);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "aggregate");
        },
        .join => |join| blk: {
            var fingerprint = try joinFingerprintAlloc(alloc, join);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", join.join.offset);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "join");
        },
        .lateral => |lateral| blk: {
            var fingerprint = try lateralFingerprintAlloc(alloc, lateral);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "right_offset", lateral.plan.lateral.right.offset);
            fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", lateral.plan.lateral.offset);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "lateral");
        },
        .window => |window| blk: {
            const fingerprint = try windowFingerprintAlloc(alloc, window);
            break :blk try readFingerprintWithPrefixAlloc(alloc, fingerprint, "window");
        },
    };
}

pub fn queryFunctionFingerprintAlloc(
    alloc: std.mem.Allocator,
    lowered: query_contract.OwnedQueryRequest,
) ![]u8 {
    const req = lowered.req;
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "query_function:text={d}:full_text_queries={d}:dense={d}:sparse={d}:graph_search={d}:graph_metric={d}:fields={d}:limit={d}",
        .{
            @as(usize, if (req.full_text != null) 1 else 0),
            req.full_text_queries.len,
            req.dense_queries.len + @as(usize, if (req.dense != null) 1 else 0),
            req.sparse_queries.len + @as(usize, if (req.sparse != null) 1 else 0),
            req.graph_queries.len,
            req.graph_metric_queries.len,
            lowered.fields.len + req.fields.len,
            req.limit,
        },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "primary_text_index", if (req.primary_text_index_name != null) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "graph_metric_rerank", if (req.graph_metric_rerank != null) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "merge", if (req.merge_config != null) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "reranker", if (req.reranker != null) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "filter_json", if (req.filter_query_json.len > 0) 1 else 0);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "doc_filter_bindings", req.doc_filter_bindings.len);
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", req.offset);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "count", req.count_only);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "profile", req.profile);
    return fingerprint;
}

pub fn recursiveCteFingerprintAlloc(alloc: std.mem.Allocator, recursive_cte: LoweredRecursiveCtePlan) ![]u8 {
    const anchor = try queryFingerprintAlloc(alloc, "anchor", recursive_cte.anchor.table_name, recursive_cte.anchor.plan.query, recursive_cte.anchor.plan.ctes.len);
    defer alloc.free(anchor);
    const member = try recursiveCteMemberFingerprintAlloc(alloc, recursive_cte.recursive_member);
    defer alloc.free(member);
    return try std.fmt.allocPrint(
        alloc,
        "recursive_cte:name={s}:op={s}:anchor={s}:member={s}:outputs={d}:self_ref={}:max_rows={d}:max_bytes={d}:spill_after={d}",
        .{
            recursive_cte.cte_name,
            @tagName(recursive_cte.operation),
            anchor,
            member,
            recursive_cte.output_columns.len,
            recursive_cte.recursive_member_references_cte,
            recursive_cte.max_rows orelse 0,
            recursive_cte.max_bytes orelse 0,
            recursive_cte.spill_after_bytes orelse 0,
        },
    );
}

pub fn recursiveCteMemberFingerprintAlloc(alloc: std.mem.Allocator, member: LoweredRecursiveCteMemberPlan) ![]u8 {
    return switch (member) {
        .join => |join| try std.fmt.allocPrint(
            alloc,
            "recursive_member_join:type={s}:left={s}:right={s}:on={d}:projections={d}",
            .{ sqlJoinTypeFingerprintName(join.join_type), join.left_table_name, join.right_table_name, join.on.len, join.projections.len },
        ),
    };
}

pub fn setOperationFingerprintAlloc(alloc: std.mem.Allocator, set_operation: LoweredSetOperationPlan) ![]u8 {
    const left = try queryFingerprintAlloc(alloc, "left", set_operation.left.table_name, set_operation.left.plan.query, set_operation.left.plan.ctes.len);
    defer alloc.free(left);
    const right = try queryFingerprintAlloc(alloc, "right", set_operation.right.table_name, set_operation.right.plan.query, set_operation.right.plan.ctes.len);
    defer alloc.free(right);
    var fingerprint = try std.fmt.allocPrint(
        alloc,
        "set_operation:op={s}:left={s}:right={s}",
        .{ @tagName(set_operation.operation), left, right },
    );
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "result_output", set_operation.output_columns.len);
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "result_order", set_operation.order_by.len);
    if (set_operation.limit) |limit| fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "result_limit", limit);
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "result_offset", set_operation.offset);
    return fingerprint;
}

pub const PlanFingerprintView = struct {
    plan: []const u8,

    pub fn init(plan: []const u8) PlanFingerprintView {
        return .{ .plan = plan };
    }

    pub fn scanStringToken(self: PlanFingerprintView, token: []const u8) PlanStringTokenScan {
        var start: usize = 0;
        var found: ?[]const u8 = null;
        while (std.mem.indexOfPos(u8, self.plan, start, token)) |index| {
            if (!self.tokenStartsAtBoundary(index, token)) {
                start = index + 1;
                continue;
            }
            const value_start = index + token.len;
            var value_end = value_start;
            while (value_end < self.plan.len and self.plan[value_end] != ':') : (value_end += 1) {}
            if (found != null) return .invalid;
            found = self.plan[value_start..value_end];
            start = value_end + 1;
        }
        if (found) |value| return .{ .value = value };
        return .absent;
    }

    pub fn scanUsizeToken(self: PlanFingerprintView, token: []const u8) PlanUsizeTokenScan {
        return switch (self.scanStringToken(token)) {
            .absent => .absent,
            .invalid => .invalid,
            .value => |value| .{ .value = parseWholeUsizeTokenValue(value) orelse return .invalid },
        };
    }

    pub fn hasNonZeroUsizeTokenNamePrefix(self: PlanFingerprintView, name_prefix: []const u8) bool {
        var segment_start: usize = 0;
        var found_non_zero = false;
        while (segment_start < self.plan.len) {
            const segment_end = std.mem.indexOfScalarPos(u8, self.plan, segment_start, ':') orelse self.plan.len;
            const segment = self.plan[segment_start..segment_end];
            if (std.mem.indexOfScalar(u8, segment, '=')) |equals_index| {
                if (std.mem.startsWith(u8, segment[0..equals_index], name_prefix)) {
                    const value = parseWholeUsizeTokenValue(segment[equals_index + 1 ..]) orelse return false;
                    found_non_zero = found_non_zero or value > 0;
                }
            }
            segment_start = segment_end + 1;
        }
        return found_non_zero;
    }

    fn tokenStartsAtBoundary(self: PlanFingerprintView, index: usize, token: []const u8) bool {
        if (token.len > 0 and token[0] == ':') return index < self.plan.len and self.plan[index] == ':';
        if (token.len > 0 and token[0] == '_') return index > 0 and self.segmentStartBefore(index) < index;
        return index == 0 or self.plan[index - 1] == ':';
    }

    fn segmentStartBefore(self: PlanFingerprintView, index: usize) usize {
        var start = index;
        while (start > 0 and self.plan[start - 1] != ':') start -= 1;
        return start;
    }
};

pub fn scanStringToken(plan: []const u8, token: []const u8) PlanStringTokenScan {
    return PlanFingerprintView.init(plan).scanStringToken(token);
}

pub fn planHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| std.mem.eql(u8, value, expected),
        .absent, .invalid => false,
    };
}

pub fn planHasStringToken(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| value.len > 0,
        .absent, .invalid => false,
    };
}

pub fn planTokenAbsent(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .absent => true,
        .value, .invalid => false,
    };
}

pub fn planHasAnyExactStringToken(plan: []const u8, token: []const u8, expected_values: []const []const u8) bool {
    for (expected_values) |expected| {
        if (planHasExactStringToken(plan, token, expected)) return true;
    }
    return false;
}

pub fn parseDelimitedUsizeToken(plan: []const u8, value_start: usize) ?usize {
    const value_end = std.mem.indexOfScalarPos(u8, plan, value_start, ':') orelse plan.len;
    return parseWholeUsizeTokenValue(plan[value_start..value_end]);
}

fn parseWholeUsizeTokenValue(value_text: []const u8) ?usize {
    var pos: usize = 0;
    if (value_text.len == 0 or value_text[0] < '0' or value_text[0] > '9') return null;
    var value: usize = 0;
    while (pos < value_text.len and value_text[pos] >= '0' and value_text[pos] <= '9') : (pos += 1) {
        value = value * 10 + @as(usize, value_text[pos] - '0');
    }
    if (pos != value_text.len) return null;
    return value;
}

pub const PlanUsizeTokenScan = union(enum) {
    absent,
    value: usize,
    invalid,
};

pub fn scanUsizeToken(plan: []const u8, token: []const u8) PlanUsizeTokenScan {
    return PlanFingerprintView.init(plan).scanUsizeToken(token);
}

pub fn planUsizeTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent, .invalid => null,
    };
}

pub fn planHasNonZeroToken(plan: []const u8, token: []const u8) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value > 0,
        .absent, .invalid => false,
    };
}

pub fn planHasNonZeroUsizeTokenNamePrefix(plan: []const u8, name_prefix: []const u8) bool {
    return PlanFingerprintView.init(plan).hasNonZeroUsizeTokenNamePrefix(name_prefix);
}

pub fn planHasExactUsizeToken(plan: []const u8, token: []const u8, expected: usize) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value == expected,
        .absent, .invalid => false,
    };
}

pub fn planAllUsizeTokensMatch(plan: []const u8, token: []const u8, expected: usize) bool {
    var start: usize = 0;
    var found = false;
    while (std.mem.indexOfPos(u8, plan, start, token)) |index| {
        const parsed = parseDelimitedUsizeToken(plan, index + token.len) orelse return false;
        if (parsed != expected) return false;
        found = true;
        start = index + token.len;
    }
    return found;
}

pub fn planUsizeOptionalTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent => 0,
        .invalid => null,
    };
}

pub fn planBoolTokenValue(plan: []const u8, token: []const u8) ?bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk true;
            if (std.mem.eql(u8, value, "false")) break :blk false;
            break :blk null;
        },
        .absent, .invalid => null,
    };
}

pub fn planBoolTokenUsize(plan: []const u8, token: []const u8) ?usize {
    return switch (scanStringToken(plan, token)) {
        .absent => 0,
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk 1;
            if (std.mem.eql(u8, value, "false")) break :blk 0;
            break :blk null;
        },
        .invalid => null,
    };
}

pub fn planHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    const value = planBoolTokenValue(plan, token) orelse return false;
    return value == expected;
}

pub fn planUsizeTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        const value = planUsizeTokenValue(plan, token) orelse return false;
        sum += value;
    }
    return sum == expected;
}

pub fn planUsizeOptionalTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planUsizeOptionalTokenValue(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planNonNoneStringTokenUsize(plan: []const u8, token: []const u8) ?usize {
    return switch (scanStringToken(plan, token)) {
        .absent => 0,
        .value => |value| if (std.mem.eql(u8, value, "none")) 0 else 1,
        .invalid => null,
    };
}

pub fn planNonNoneStringTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planNonNoneStringTokenUsize(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planBoolTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planBoolTokenUsize(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planHasAnyNonZeroToken(plan: []const u8, tokens: []const []const u8) bool {
    for (tokens) |token| {
        if (planHasNonZeroToken(plan, token)) return true;
    }
    return false;
}

pub fn planHasRootKind(plan: []const u8, expected: []const u8) bool {
    const root_end = std.mem.indexOfScalar(u8, plan, ':') orelse plan.len;
    return std.mem.eql(u8, plan[0..root_end], expected);
}

pub fn planHasRootSubKind(plan: []const u8, root: []const u8, expected: []const u8) bool {
    const root_end = std.mem.indexOfScalar(u8, plan, ':') orelse return false;
    if (!std.mem.eql(u8, plan[0..root_end], root)) return false;
    const sub_start = root_end + 1;
    const sub_end = std.mem.indexOfScalarPos(u8, plan, sub_start, ':') orelse plan.len;
    return std.mem.eql(u8, plan[sub_start..sub_end], expected);
}

pub fn readPlanHasKind(plan: []const u8, expected: []const u8) bool {
    return planHasRootSubKind(plan, "read", expected);
}

pub fn mergePlanIsTyped(plan: []const u8) bool {
    return planHasRootKind(plan, "merge_mutation") or planHasRootKind(plan, "recursive_merge_mutation");
}

pub fn mergePlanIsRecursive(plan: []const u8) bool {
    return planHasRootKind(plan, "recursive_merge_mutation");
}

pub fn explainPlanHasKind(plan: []const u8, expected: []const u8) bool {
    return planHasExactStringToken(plan, "explain:kind=", expected);
}

pub fn explainPlanInnerHasRootKind(plan: []const u8, expected: []const u8) bool {
    const inner = explainPlanInnerFingerprint(plan) orelse return false;
    return planHasRootKind(inner, expected);
}

pub fn explainPlanInnerReadHasKind(plan: []const u8, expected: []const u8) bool {
    const inner = explainPlanInnerFingerprint(plan) orelse return false;
    return readPlanHasKind(inner, expected);
}

fn explainPlanInnerFingerprint(plan: []const u8) ?[]const u8 {
    const inner_token = ":inner=";
    const inner_index = std.mem.indexOf(u8, plan, inner_token) orelse return null;
    if (std.mem.indexOfPos(u8, plan, inner_index + inner_token.len, inner_token) != null) return null;
    return plan[inner_index + inner_token.len ..];
}

pub fn planHasTrailingRowExpressionFragment(plan: []const u8, fragment: []const u8) bool {
    const expr_token = ":expr=";
    const expr_index = std.mem.indexOf(u8, plan, expr_token) orelse return false;
    if (std.mem.indexOfPos(u8, plan, expr_index + expr_token.len, expr_token) != null) return false;
    if (!PlanFingerprintView.init(plan).tokenStartsAtBoundary(expr_index, expr_token)) return false;
    var index = expr_index + expr_token.len;
    const expr_start = index;
    if (!consumeRowRewriteExpressionFingerprint(plan, &index)) return false;
    if (index != plan.len) return false;
    return std.mem.indexOf(u8, plan[expr_start..index], fragment) != null;
}

pub fn bulkSqlIoExecutionPlanIsStructured(plan: []const u8) bool {
    if (!planHasRootKind(plan, "bulk_sql_io")) return false;
    const has_endpoint_kind = planHasStringToken(plan, ":endpoint_kind=");
    const has_endpoint = planHasStringToken(plan, ":endpoint=");
    if (has_endpoint_kind != has_endpoint) return false;
    return planHasAnyExactStringToken(plan, ":op=", &.{ "import_rows", "export_rows" }) and
        planHasAnyExactStringToken(plan, ":native=", &.{ "rows_batch", "rows_query" }) and
        planHasAnyExactStringToken(plan, ":stream=", &.{ "stdin", "stdout", "file", "program" }) and
        planHasAnyExactStringToken(plan, ":codec=", &.{ "csv", "postgres_text", "postgres_binary" }) and
        planHasAnyExactStringToken(plan, ":auth=", &.{ "table/read", "table/write" }) and
        planHasAnyExactStringToken(plan, ":audit=", &.{ "copy_from", "copy_to" }) and
        planHasStringToken(plan, ":table=") and
        planUsizeTokenValue(plan, ":columns=") != null and
        planUsizeTokenValue(plan, ":where_expr=") != null and
        planBoolTokenValue(plan, ":requires_stream=") != null and
        (!has_endpoint_kind or planHasAnyExactStringToken(plan, ":endpoint_kind=", &.{ "file", "program" }));
}

pub fn bulkSqlIoExecutionPlanHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    return bulkSqlIoExecutionPlanIsStructured(plan) and planHasExactStringToken(plan, token, expected);
}

pub fn bulkSqlIoExecutionPlanHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    return bulkSqlIoExecutionPlanIsStructured(plan) and planHasExactBoolToken(plan, token, expected);
}

pub fn preparedTransactionRecoveryPlanIsStructured(plan: []const u8) bool {
    return planHasRootKind(plan, "prepared_txn_recovery") and
        planHasAnyExactStringToken(plan, ":op=", &.{ "register_prepared", "resolve_commit", "resolve_rollback" }) and
        planHasStringToken(plan, ":gid=") and
        planHasAnyExactStringToken(plan, ":audit=", &.{ "prepare", "commit", "rollback" }) and
        planBoolTokenValue(plan, ":requires_coordinator=") != null;
}

pub fn preparedTransactionRecoveryPlanHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    return preparedTransactionRecoveryPlanIsStructured(plan) and planHasExactStringToken(plan, token, expected);
}

pub fn joinedSourcePlanHasCounts(plan: []const u8, right_predicates: usize, join_keys: usize) bool {
    return planHasExactUsizeToken(plan, ":right_pred=", right_predicates) and
        planHasExactUsizeToken(plan, ":on=", join_keys);
}

pub fn writePlanHasCounts(plan: []const u8, writes: usize, transforms: usize) bool {
    return planHasExactUsizeToken(plan, ":writes=", writes) and
        planHasExactUsizeToken(plan, ":transforms=", transforms);
}

pub fn appliedPlanIsStructured(plan: []const u8) bool {
    if (std.mem.startsWith(u8, plan, "applied:drop_table:")) {
        var drop_index: usize = 0;
        if (!consumeLiteral(plan, &drop_index, "applied:drop_table:rebuild=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        if (!consumeLiteral(plan, &drop_index, ":validation=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        if (!consumeLiteral(plan, &drop_index, ":rewrite=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        if (!consumeLiteral(plan, &drop_index, ":work_items=")) return false;
        const work_item_count = consumeUsizeValue(plan, &drop_index) orelse return false;
        if (!consumeLiteral(plan, &drop_index, ":work=")) return false;
        if (!consumeAppliedWorkItems(plan, &drop_index, work_item_count)) return false;
        return drop_index == plan.len;
    }

    var index: usize = 0;
    if (!consumeLiteral(plan, &index, "applied:rebuild=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":validation=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":rewrite=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":building_indexes=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_unique=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_fk=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_check=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":update_policy=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":work_items=")) return false;
    const work_item_count = consumeUsizeValue(plan, &index) orelse return false;
    if (!consumeLiteral(plan, &index, ":work=")) return false;
    if (!consumeAppliedWorkItems(plan, &index, work_item_count)) return false;
    if (index == plan.len) return true;
    if (!consumeLiteral(plan, &index, ":comments=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    return index == plan.len;
}

pub fn appliedPlanHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    return appliedPlanIsStructured(plan) and planHasExactBoolToken(plan, token, expected);
}

pub fn appliedPlanHasExactUsizeToken(plan: []const u8, token: []const u8, expected: usize) bool {
    return appliedPlanIsStructured(plan) and planHasExactUsizeToken(plan, token, expected);
}

pub fn appliedPlanHasRowImageRewriteExpression(plan: []const u8) bool {
    if (!appliedPlanIsStructured(plan)) return false;
    const literal = "rewrite/table/row_images(target=";
    var start: usize = 0;
    while (std.mem.indexOfPos(u8, plan, start, literal)) |index| {
        if (index > 0 and plan[index - 1] != '=' and plan[index - 1] != ',') {
            start = index + 1;
            continue;
        }
        var value_index = index + literal.len;
        if (consumeAppliedRewriteExpression(plan, &value_index)) return true;
        start = index + 1;
    }
    return false;
}

fn consumeLiteral(text: []const u8, index: *usize, literal: []const u8) bool {
    if (index.* > text.len) return false;
    if (text.len - index.* < literal.len) return false;
    if (!std.mem.eql(u8, text[index.* .. index.* + literal.len], literal)) return false;
    index.* += literal.len;
    return true;
}

fn consumeBool(text: []const u8, index: *usize) bool {
    if (consumeLiteral(text, index, "true")) return true;
    if (consumeLiteral(text, index, "false")) return true;
    return false;
}

fn consumeUsize(text: []const u8, index: *usize) bool {
    const start = index.*;
    while (index.* < text.len and text[index.*] >= '0' and text[index.*] <= '9') : (index.* += 1) {}
    return index.* > start;
}

fn consumeUsizeValue(text: []const u8, index: *usize) ?usize {
    const start = index.*;
    if (!consumeUsize(text, index)) return null;
    return std.fmt.parseInt(usize, text[start..index.*], 10) catch null;
}

fn consumeAppliedWorkItems(text: []const u8, index: *usize, expected_count: usize) bool {
    if (expected_count == 0) return consumeLiteral(text, index, "none");

    var count: usize = 0;
    while (count < expected_count) : (count += 1) {
        if (!(consumeLiteral(text, index, "rebuild/table/derived_artifacts") or
            consumeLiteral(text, index, "validate/table/constraints")))
        {
            if (!consumeLiteral(text, index, "rewrite/table/row_images")) return false;
            if (consumeLiteral(text, index, "(target=")) {
                if (!consumeAppliedRewriteExpression(text, index)) return false;
            } else if (consumeLiteral(text, index, "(row_plan=")) {
                if (!consumeAppliedRowRewritePlan(text, index)) return false;
            }
        }
        if (count + 1 < expected_count and !consumeLiteral(text, index, ",")) return false;
    }
    return true;
}

fn consumeAppliedRewriteExpression(text: []const u8, index: *usize) bool {
    if (!consumeIdentifierValue(text, index)) return false;
    if (!consumeLiteral(text, index, ":expr=")) return false;
    if (!consumeRowRewriteExpressionFingerprint(text, index)) return false;
    return consumeLiteral(text, index, ")");
}

fn consumeAppliedRowRewritePlan(text: []const u8, index: *usize) bool {
    var count: usize = 0;
    while (true) : (count += 1) {
        if (consumeLiteral(text, index, ":rename(")) {
            if (!consumeRowRewritePath(text, index)) return false;
            if (!consumeLiteral(text, index, "->")) return false;
            if (!consumeRowRewritePath(text, index)) return false;
            if (!consumeLiteral(text, index, ")")) return false;
        } else if (consumeLiteral(text, index, ":drop(")) {
            if (!consumeRowRewritePath(text, index)) return false;
            if (!consumeLiteral(text, index, ")")) return false;
        } else {
            break;
        }
    }
    return count > 0 and consumeLiteral(text, index, ")");
}

fn consumeRowRewritePath(text: []const u8, index: *usize) bool {
    const start = index.*;
    while (index.* < text.len) : (index.* += 1) {
        const ch = text[index.*];
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '.' or ch == '[' or ch == ']')) break;
    }
    return index.* > start;
}

fn consumeIdentifierValue(text: []const u8, index: *usize) bool {
    const start = index.*;
    while (index.* < text.len and (std.ascii.isAlphanumeric(text[index.*]) or text[index.*] == '_')) : (index.* += 1) {}
    return index.* > start;
}

fn consumeRowRewriteExpressionFingerprint(text: []const u8, index: *usize) bool {
    const start = index.*;
    var depth: usize = 0;
    while (index.* < text.len) : (index.* += 1) {
        switch (text[index.*]) {
            '[' => depth += 1,
            ']' => {
                if (depth == 0) return false;
                depth -= 1;
            },
            ')' => if (depth == 0) break,
            ',' => if (depth == 0) return false,
            ':' => if (depth == 0) return false,
            else => |ch| {
                if (!(std.ascii.isAlphanumeric(ch) or
                    ch == '_' or ch == '-' or ch == '+' or ch == '|' or ch == '[' or ch == ']' or ch == '=' or ch == '.'))
                {
                    return false;
                }
            },
        }
    }
    return index.* > start and depth == 0;
}

pub fn sqlTokensHaveParameterIndex(tokens: []const tokenized.Token, expected: usize) bool {
    for (tokens) |token| {
        const param_index = sqlParameterIndexFromToken(token) orelse continue;
        if (param_index == expected) return true;
    }
    return false;
}

pub fn sqlParameterCoverageMatchesTokens(tokens: []const tokenized.Token, param_count: usize) bool {
    var saw_parameter = false;
    var max_index: usize = 0;
    for (tokens) |token| {
        const param_index = sqlParameterIndexFromToken(token) orelse continue;
        if (param_index == 0 or param_index > param_count) return false;
        saw_parameter = true;
        max_index = @max(max_index, param_index);
    }
    if (param_count == 0) return !saw_parameter;
    if (!saw_parameter or max_index != param_count) return false;

    for (1..param_count + 1) |param_index| {
        if (!sqlTokensHaveParameterIndex(tokens, param_index)) return false;
    }
    return true;
}

fn sqlParameterIndexFromToken(token: tokenized.Token) ?usize {
    if (token.kind != .placeholder) return null;
    const text = token.text;
    if (text.len < 2 or text[0] != '$' or !std.ascii.isDigit(text[1])) return null;

    var index: usize = 1;
    var value: usize = 0;
    while (index < text.len and std.ascii.isDigit(text[index])) : (index += 1) {
        value = value * 10 + (text[index] - '0');
    }
    return value;
}

test "sql adapter corpus parses fixture entries and owns allocated slices" {
    const alloc = std.testing.allocator;
    const fixture_entry_json =
        \\{
        \\  "name": "joined update",
        \\  "family": "update_joined_source",
        \\  "summary": {
        \\    "table_name": "usage_records",
        \\    "patch_expressions": 2
        \\  },
        \\  "plan": "update_joined_source:table=usage_records:source_assignments=2",
        \\  "classification_reason": "",
        \\  "apply_setup_sql": ["CREATE TABLE usage_records (id text PRIMARY KEY)"],
        \\  "returning_rows": ["{\"id\":\"u1\"}"],
        \\  "applied_plan": "",
        \\  "resolver_row_json": "",
        \\  "resolver_version": 9,
        \\  "resolver_exists": true,
        \\  "source_schema_json": "",
        \\  "params": [
        \\    {"integer": 42},
        \\    {"string": "queued"},
        \\    {"float": 2.5},
        \\    {"json": "{\"enabled\":true}"},
        \\    {"null": true}
        \\  ],
        \\  "sql": "UPDATE usage_records SET quantity = $1"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_entry_json, .{});
    defer parsed.deinit();

    const entry = try parseFixtureEntryAlloc(alloc, parsed.value);
    defer freeFixtureEntry(alloc, entry);
    try std.testing.expectEqualStrings("joined update", entry.name);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.update_joined_source, entry.family);
    try std.testing.expectEqual(@as(?usize, 2), entry.summary.source_assignments);
    try std.testing.expectEqual(@as(?usize, null), entry.summary.patch_expressions);
    try std.testing.expectEqual(@as(usize, 5), entry.params.len);
    try std.testing.expectEqual(@as(i64, 42), entry.params[0].integer);
    try std.testing.expectEqualStrings("queued", entry.params[1].string);
    try std.testing.expectEqual(@as(usize, 1), entry.apply_setup_sql.len);
    try std.testing.expectEqual(@as(usize, 1), entry.returning_rows.len);
    try std.testing.expectEqual(@as(u64, 9), entry.resolver_version);
    try std.testing.expectEqual(@as(?bool, true), entry.resolver_exists);
}

test "sql adapter corpus parses fixture root metadata and owns skipped list" {
    const alloc = std.testing.allocator;
    const source_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const fixture_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        \\  "source_entry_count": 2,
        \\  "entry_count": 1,
        \\  "skipped_entries": ["unsupported recursive cte"],
        \\  "schema_json": "{\"version\":1}",
        \\  "entries": [
        \\    {
        \\      "name": "read",
        \\      "family": "read",
        \\      "plan": "read:table=usage_records",
        \\      "sql": "SELECT * FROM usage_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();

    const root = try parseFixtureRootAlloc(alloc, parsed.value);
    defer freeFixtureRoot(alloc, root);
    try std.testing.expectEqual(app_parity_fixture_format, root.fixture_format);
    try std.testing.expectEqualStrings(source_sha256, root.source_sha256);
    try std.testing.expectEqual(@as(usize, 2), root.source_entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.skipped_entries.len);
    try std.testing.expectEqualStrings("unsupported recursive cte", root.skipped_entries[0]);
    try std.testing.expectEqualStrings("{\"version\":1}", root.schema_json);
    try std.testing.expectEqual(@as(usize, 1), root.entries.len);

    const missing_digest_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 1,
        \\  "entry_count": 1,
        \\  "skipped_entries": [],
        \\  "schema_json": "{\"version\":1}",
        \\  "entries": [{"name": "read", "family": "read", "plan": "read:table=usage_records", "sql": "SELECT * FROM usage_records"}]
        \\}
    ;
    var parsed_missing_digest = try std.json.parseFromSlice(std.json.Value, alloc, missing_digest_json, .{});
    defer parsed_missing_digest.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureRootAlloc(alloc, parsed_missing_digest.value));
}

test "sql adapter corpus parses source corpus root entries" {
    const alloc = std.testing.allocator;
    const source_json = @embedFile("../api/fixtures/sql_api_parity_source_corpus.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
    defer parsed.deinit();

    const root = try parseSourceCorpusRootAlloc(alloc, parsed.value);
    defer freeSourceCorpusRoot(alloc, root);

    try std.testing.expectEqual(app_parity_source_corpus_format, root.source_format);
    try std.testing.expect(root.entries.len > 0);
    try std.testing.expectEqualStrings("prepare statement protocol plan", root.entries[0].name);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.ddl, root.entries[0].family);
    try std.testing.expectEqual(AppParityDdlTag.prepare_statement, root.entries[0].summary.ddl_tag.?);
}

fn expectSourceCorpusNativeRequirements(
    alloc: std.mem.Allocator,
    entries: []const AppParityCorpusEntry,
    required: []const []const u8,
    resolved: []const AppParityResolvedRequirement,
) !void {
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    for (entries) |entry| {
        if (entry.classification_reason.len == 0) continue;
        if (corpusPlanFamilyIsInvalid(entry.family)) continue;
        const reason = diagnostics.classificationReasonFromToken(entry.classification_reason) orelse return error.TestUnexpectedResult;
        if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) continue;
        if (!stringListContains(required, entry.classification_reason) and !resolvedRequirementContains(resolved, entry.classification_reason)) {
            std.debug.print("unlisted source corpus native requirement: {s}\n", .{entry.classification_reason});
            return error.TestUnexpectedResult;
        }
        try seen.put(alloc, entry.classification_reason, {});
    }

    for (required) |name| {
        if (!seen.contains(name)) {
            std.debug.print("missing source corpus native requirement: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

test "sql adapter source corpus covers required native requirement classifications" {
    const alloc = std.testing.allocator;
    var source = try parseAppParityExternalSourceCorpusAlloc(alloc);
    defer source.deinit(alloc);
    var requirements = try parseAppParityNativeRequirementsAlloc(alloc);
    defer requirements.deinit(alloc);
    var resolved = try parseAppParityResolvedRequirementsAlloc(alloc);
    defer resolved.deinit(alloc);

    try expectSourceCorpusNativeRequirements(alloc, source.root.entries, requirements.root.required, resolved.root.resolved);
}

test "sql adapter corpus validates native requirement manifest" {
    const alloc = std.testing.allocator;
    var requirements = try parseAppParityNativeRequirementsAlloc(alloc);
    defer requirements.deinit(alloc);
    try std.testing.expectEqual(app_parity_native_requirement_fixture_format, requirements.root.requirement_format);

    const unknown_json =
        \\{
        \\  "requirement_format": 1,
        \\  "required": [
        \\    "aggregate_duplicate_output_name",
        \\    "not_a_requirement"
        \\  ]
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseNativeRequirementRootAlloc(alloc, parsed_unknown.value));

    const noop_json =
        \\{
        \\  "requirement_format": 1,
        \\  "required": [
        \\    "aggregate_duplicate_output_name",
        \\    "session_setting"
        \\  ]
        \\}
    ;
    var parsed_noop = try std.json.parseFromSlice(std.json.Value, alloc, noop_json, .{});
    defer parsed_noop.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseNativeRequirementRootAlloc(alloc, parsed_noop.value));

    const unsorted_json =
        \\{
        \\  "requirement_format": 1,
        \\  "required": [
        \\    "bulk_io_plan",
        \\    "aggregate_duplicate_output_name"
        \\  ]
        \\}
    ;
    var parsed_unsorted = try std.json.parseFromSlice(std.json.Value, alloc, unsorted_json, .{});
    defer parsed_unsorted.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseNativeRequirementRootAlloc(alloc, parsed_unsorted.value));

    const entries = [_]AppParityCorpusEntry{
        .{
            .name = "unsupported bulk",
            .family = .unsupported_ddl,
            .classification_reason = "bulk_io_plan",
            .plan = "unsupported:ddl:requires=bulk_io_plan",
            .sql = "COPY usage_records FROM STDIN",
        },
    };
    try std.testing.expectError(
        error.TestUnexpectedResult,
        expectSourceCorpusNativeRequirements(alloc, &entries, &.{ "aggregate_duplicate_output_name", "bulk_io_plan" }, &.{}),
    );
    try std.testing.expectError(
        error.TestUnexpectedResult,
        expectSourceCorpusNativeRequirements(alloc, &entries, &.{"aggregate_duplicate_output_name"}, &.{}),
    );
    try std.testing.expectError(
        error.TestUnexpectedResult,
        expectSourceCorpusNativeRequirements(
            alloc,
            &entries,
            &.{"aggregate_duplicate_output_name"},
            &.{.{ .reason = "bulk_io_plan", .coverage = &.{"ddl_copy_from_execution_contract"} }},
        ),
    );
}

fn expectSourceCorpusResolvedRequirements(
    coverage: AppParityCorpusCoverage,
    resolved: []const AppParityResolvedRequirement,
    emit_diagnostics: bool,
) !void {
    if (resolved.len == 0) return error.TestUnexpectedResult;
    for (resolved) |item| {
        if (item.coverage.len == 0) return error.TestUnexpectedResult;
        for (item.coverage) |name| {
            if (!try appParityCoverageRequirementSatisfied(coverage, name)) {
                if (emit_diagnostics) {
                    std.debug.print("missing resolved native requirement coverage: {s} -> {s}\n", .{ item.reason, name });
                }
                return error.TestUnexpectedResult;
            }
        }
    }
}

fn resolvedRequirementContains(resolved: []const AppParityResolvedRequirement, reason: []const u8) bool {
    for (resolved) |item| {
        if (std.mem.eql(u8, item.reason, reason)) return true;
    }
    return false;
}

fn expectNativeRequirementPolicyComplete(
    unresolved: []const []const u8,
    resolved: []const AppParityResolvedRequirement,
    emit_diagnostics: bool,
) !void {
    if (resolved.len == 0) return error.TestUnexpectedResult;
    inline for (std.meta.fields(diagnostics.SqlAdapterClassificationReason)) |field| {
        const reason: diagnostics.SqlAdapterClassificationReason = @enumFromInt(field.value);
        if (diagnostics.classificationReasonIsUnsupportedRequirement(reason)) {
            const name = diagnostics.classificationReasonToken(reason);
            const is_unresolved = stringListContains(unresolved, name);
            const is_resolved = resolvedRequirementContains(resolved, name);
            if (is_unresolved == is_resolved) {
                if (emit_diagnostics) {
                    std.debug.print("native requirement policy must classify exactly once: {s}\n", .{name});
                }
                return error.TestUnexpectedResult;
            }
        }
    }
}

test "sql adapter source corpus covers resolved native requirements with positive typed plans" {
    const alloc = std.testing.allocator;
    var source = try parseAppParityExternalSourceCorpusAlloc(alloc);
    defer source.deinit(alloc);
    var resolved = try parseAppParityResolvedRequirementsAlloc(alloc);
    defer resolved.deinit(alloc);

    var coverage = AppParityCorpusCoverage{};
    for (source.root.entries) |entry| {
        try coverage.observe(alloc, entry);
    }
    try expectSourceCorpusResolvedRequirements(coverage, resolved.root.resolved, true);
}

test "sql adapter native requirement manifests classify every stable requirement" {
    const alloc = std.testing.allocator;
    var unresolved = try parseAppParityNativeRequirementsAlloc(alloc);
    defer unresolved.deinit(alloc);
    var resolved = try parseAppParityResolvedRequirementsAlloc(alloc);
    defer resolved.deinit(alloc);

    try expectNativeRequirementPolicyComplete(unresolved.root.required, resolved.root.resolved, true);

    const overlapping = [_]AppParityResolvedRequirement{.{
        .reason = "bulk_io_plan",
        .coverage = &.{"ddl_copy_from_execution_contract"},
    }};
    try std.testing.expectError(
        error.TestUnexpectedResult,
        expectNativeRequirementPolicyComplete(unresolved.root.required, &overlapping, false),
    );

    const missing_unresolved = [_][]const u8{"bulk_io_plan"};
    const missing_resolved = [_]AppParityResolvedRequirement{.{
        .reason = "set_operation_plan",
        .coverage = &.{"read_set_operation_order_limit"},
    }};
    try std.testing.expectError(
        error.TestUnexpectedResult,
        expectNativeRequirementPolicyComplete(&missing_unresolved, &missing_resolved, false),
    );
}

test "sql adapter corpus validates resolved native requirement manifest" {
    const alloc = std.testing.allocator;
    var resolved = try parseAppParityResolvedRequirementsAlloc(alloc);
    defer resolved.deinit(alloc);
    try std.testing.expectEqual(app_parity_resolved_requirement_fixture_format, resolved.root.resolution_format);
    try std.testing.expect(resolved.root.resolved.len > 0);

    const unknown_reason_json =
        \\{
        \\  "resolution_format": 1,
        \\  "resolved": [
        \\    {"reason": "not_a_reason", "coverage": ["read_recursive_cte_stream_plan"]}
        \\  ]
        \\}
    ;
    var parsed_unknown_reason = try std.json.parseFromSlice(std.json.Value, alloc, unknown_reason_json, .{});
    defer parsed_unknown_reason.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseResolvedRequirementRootAlloc(alloc, parsed_unknown_reason.value));

    const noop_reason_json =
        \\{
        \\  "resolution_format": 1,
        \\  "resolved": [
        \\    {"reason": "session_setting", "coverage": ["read_recursive_cte_stream_plan"]}
        \\  ]
        \\}
    ;
    var parsed_noop_reason = try std.json.parseFromSlice(std.json.Value, alloc, noop_reason_json, .{});
    defer parsed_noop_reason.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseResolvedRequirementRootAlloc(alloc, parsed_noop_reason.value));

    const unknown_coverage_json =
        \\{
        \\  "resolution_format": 1,
        \\  "resolved": [
        \\    {"reason": "recursive_cte_stream_plan", "coverage": ["not_a_coverage_bucket"]}
        \\  ]
        \\}
    ;
    var parsed_unknown_coverage = try std.json.parseFromSlice(std.json.Value, alloc, unknown_coverage_json, .{});
    defer parsed_unknown_coverage.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseResolvedRequirementRootAlloc(alloc, parsed_unknown_coverage.value));

    const unsorted_reason_json =
        \\{
        \\  "resolution_format": 1,
        \\  "resolved": [
        \\    {"reason": "set_operation_plan", "coverage": ["read_set_operation_order_limit"]},
        \\    {"reason": "recursive_cte_stream_plan", "coverage": ["read_recursive_cte_stream_plan"]}
        \\  ]
        \\}
    ;
    var parsed_unsorted_reason = try std.json.parseFromSlice(std.json.Value, alloc, unsorted_reason_json, .{});
    defer parsed_unsorted_reason.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseResolvedRequirementRootAlloc(alloc, parsed_unsorted_reason.value));

    const unsorted_coverage_json =
        \\{
        \\  "resolution_format": 1,
        \\  "resolved": [
        \\    {"reason": "set_operation_plan", "coverage": ["read_set_operation_order_limit", "read_set_operation_cross_table_source_schema_classifier"]}
        \\  ]
        \\}
    ;
    var parsed_unsorted_coverage = try std.json.parseFromSlice(std.json.Value, alloc, unsorted_coverage_json, .{});
    defer parsed_unsorted_coverage.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseResolvedRequirementRootAlloc(alloc, parsed_unsorted_coverage.value));

    try std.testing.expectError(error.TestUnexpectedResult, expectSourceCorpusResolvedRequirements(.{}, resolved.root.resolved, false));
}

test "sql adapter source corpus rejects duplicate entry names" {
    const alloc = std.testing.allocator;
    const source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "duplicate source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "show_search_path"},
        \\      "plan": "ddl:session:show_search_path",
        \\      "sql": "SHOW search_path"
        \\    },
        \\    {
        \\      "name": "duplicate source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "discard_all"},
        \\      "plan": "ddl:session:discard_all",
        \\      "sql": "DISCARD ALL"
        \\    }
        \\  ]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed.value));
}

test "sql adapter source corpus validates entry metadata while allowing derived applied plans" {
    const alloc = std.testing.allocator;
    const valid_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "comment metadata source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "comment_metadata", "table_name": "usage_records"},
        \\      "plan": "ddl:comment:on=table:table=usage_records",
        \\      "apply_setup_sql": ["CREATE TABLE usage_records (id text PRIMARY KEY)"],
        \\      "sql": "COMMENT ON TABLE usage_records IS 'runtime records'"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_valid = try std.json.parseFromSlice(std.json.Value, alloc, valid_source_json, .{});
    defer parsed_valid.deinit();
    const valid_root = try parseSourceCorpusRootAlloc(alloc, parsed_valid.value);
    defer freeSourceCorpusRoot(alloc, valid_root);
    try std.testing.expectEqual(@as(usize, 1), valid_root.entries.len);

    const invalid_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "bad family plan",
        \\      "family": "query",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "insert:table=usage_records",
        \\      "sql": "SELECT id FROM usage_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_invalid = try std.json.parseFromSlice(std.json.Value, alloc, invalid_source_json, .{});
    defer parsed_invalid.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_invalid.value));
}

test "sql adapter source corpus validates catalog table metadata" {
    const alloc = std.testing.allocator;
    const valid_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "catalog backed set op",
        \\      "family": "read",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "read:set_operation:set_operation:op=except:left=left:table=usage_records:right=right:table=archived_records",
        \\      "catalog_tables": [{"name": "archived_records", "schema_json": "{\"version\":1}"}],
        \\      "sql": "SELECT id FROM usage_records EXCEPT SELECT id FROM archived_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_valid = try std.json.parseFromSlice(std.json.Value, alloc, valid_source_json, .{});
    defer parsed_valid.deinit();
    const valid_root = try parseSourceCorpusRootAlloc(alloc, parsed_valid.value);
    defer freeSourceCorpusRoot(alloc, valid_root);
    try std.testing.expectEqual(@as(usize, 1), valid_root.entries[0].catalog_tables.len);
    try std.testing.expect(appParityEntryHasCatalogSchemas(valid_root.entries[0]));
    try std.testing.expectEqualStrings("archived_records", valid_root.entries[0].catalog_tables[0].name);

    const duplicate_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "duplicate catalog tables",
        \\      "family": "read",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "read:set_operation:set_operation:op=except:left=left:table=usage_records:right=right:table=archived_records",
        \\      "catalog_tables": [
        \\        {"name": "archived_records", "schema_json": "{\"version\":1}"},
        \\        {"name": "archived_records", "schema_json": "{\"version\":1}"}
        \\      ],
        \\      "sql": "SELECT id FROM usage_records EXCEPT SELECT id FROM archived_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_duplicate = try std.json.parseFromSlice(std.json.Value, alloc, duplicate_source_json, .{});
    defer parsed_duplicate.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_duplicate.value));

    const mixed_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "mixed catalog forms",
        \\      "family": "read",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "read:set_operation:set_operation:op=except:left=left:table=usage_records:right=right:table=archived_records",
        \\      "source_schema_json": "{\"version\":1}",
        \\      "catalog_tables": [{"name": "archived_records", "schema_json": "{\"version\":1}"}],
        \\      "sql": "SELECT id FROM usage_records EXCEPT SELECT id FROM archived_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_mixed = try std.json.parseFromSlice(std.json.Value, alloc, mixed_source_json, .{});
    defer parsed_mixed.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_mixed.value));

    const stale_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "stale catalog table",
        \\      "family": "read",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "read:set_operation:set_operation:op=except:left=left:table=usage_records:right=right:table=archived_records",
        \\      "catalog_tables": [{"name": "missing_records", "schema_json": "{\"version\":1}"}],
        \\      "sql": "SELECT id FROM usage_records EXCEPT SELECT id FROM archived_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_stale = try std.json.parseFromSlice(std.json.Value, alloc, stale_source_json, .{});
    defer parsed_stale.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_stale.value));
}

test "sql adapter source corpus validates deterministic json payloads" {
    const alloc = std.testing.allocator;
    const invalid_returning_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "array returning row",
        \\      "family": "insert",
        \\      "summary": {"table_name": "usage_records", "returning": 1},
        \\      "plan": "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        \\      "returning_rows": ["[\"u1\"]"],
        \\      "sql": "INSERT INTO usage_records (id) VALUES ('u1') RETURNING id"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_returning = try std.json.parseFromSlice(std.json.Value, alloc, invalid_returning_json, .{});
    defer parsed_returning.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_returning.value));

    const invalid_resolver_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "array resolver row",
        \\      "family": "insert",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=0:returning_expr=0:op_set=1",
        \\      "resolver_row_json": "[\"u1\"]",
        \\      "resolver_version": 7,
        \\      "resolver_exists": true,
        \\      "sql": "INSERT INTO usage_records (id, status) VALUES ('u1', 'new') ON CONFLICT (id) DO UPDATE SET status = 'new'"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_resolver = try std.json.parseFromSlice(std.json.Value, alloc, invalid_resolver_json, .{});
    defer parsed_resolver.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_resolver.value));
}

test "sql adapter corpus encodes fixture roots and entries" {
    const alloc = std.testing.allocator;
    const entries = [_]AppParityFixtureEncodedEntry{.{
        .entry = .{
            .name = "insert returning",
            .family = .insert,
            .summary = .{
                .table_name = "usage_records",
                .returning = 1,
                .returning_all = true,
            },
            .plan = "insert:table=usage_records:returning_rows=1:returning_all=1",
            .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY)"},
            .returning_rows = &.{"{\"id\":\"u1\"}"},
            .resolver_row_json = "{\"id\":\"u1\"}",
            .resolver_version = 7,
            .resolver_exists = true,
            .source_schema_json = "{\"source\":true}",
            .params = &.{.{ .string = "u1" }},
            .sql = "INSERT INTO usage_records (id) VALUES ($1) RETURNING *",
        },
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false",
    }};
    const skipped = [_][]const u8{"unsupported generated expression"};
    const source_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const encoded = try fixtureJsonAlloc(
        alloc,
        "{\"version\":1}",
        source_sha256,
        2,
        &entries,
        &skipped,
    );
    defer alloc.free(encoded);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    const root = try parseFixtureRootAlloc(alloc, parsed.value);
    defer freeFixtureRoot(alloc, root);
    try std.testing.expectEqualStrings(source_sha256, root.source_sha256);
    try std.testing.expectEqual(@as(usize, 2), root.source_entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.entry_count);
    try std.testing.expectEqualStrings("unsupported generated expression", root.skipped_entries[0]);
    try std.testing.expectEqualStrings("{\"version\":1}", root.schema_json);

    const entry = try parseFixtureEntryAlloc(alloc, root.entries[0]);
    defer freeFixtureEntry(alloc, entry);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.insert, entry.family);
    try std.testing.expectEqualStrings("usage_records", entry.summary.table_name.?);
    try std.testing.expectEqual(@as(?usize, 1), entry.summary.returning);
    try std.testing.expectEqual(@as(?bool, true), entry.summary.returning_all);
    try std.testing.expectEqualStrings("applied:rebuild=false:validation=false:rewrite=false", entry.applied_plan);
    try std.testing.expectEqual(@as(usize, 1), entry.params.len);
    try std.testing.expectEqualStrings("u1", entry.params[0].string);
}

test "sql adapter corpus owns fixture gate mode selection" {
    try std.testing.expect((try fixtureGateModeFromPaths(null, null)) == .none);
    switch (try fixtureGateModeFromPaths("fixture.json", null)) {
        .promote => |path| try std.testing.expectEqualStrings("fixture.json", path),
        else => return error.TestUnexpectedResult,
    }
    switch (try fixtureGateModeFromPaths(null, "fixture.json")) {
        .check => |path| try std.testing.expectEqualStrings("fixture.json", path),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectError(error.TestUnexpectedResult, fixtureGateModeFromPaths("promote.json", "check.json"));
}

test "sql adapter corpus detects empty and non-table fixture summaries" {
    try std.testing.expect(!summaryHasFields(.{}));
    try std.testing.expect(!summaryHasNonTableFields(.{ .table_name = "usage_records" }));
    try std.testing.expect(summaryHasFields(.{ .table_name = "usage_records" }));
    try std.testing.expect(summaryHasFields(.{ .predicates = 1 }));
    try std.testing.expect(summaryHasNonTableFields(.{ .table_name = "usage_records", .predicates = 1 }));
    try std.testing.expect(summaryHasNonTableFields(.{ .ddl_tag = .create_table }));
    try std.testing.expect(summaryHasNonTableFields(.{ .temporal_foreign_keys = 1 }));
}

test "sql adapter corpus owns fixture family policies" {
    try std.testing.expectEqual(UnsupportedPlanFamily.query, corpusUnsupportedPlanFamily(.unsupported).?);
    try std.testing.expectEqual(UnsupportedPlanFamily.update_joined_source, corpusUnsupportedPlanFamily(.unsupported_update_joined_source).?);
    try std.testing.expect(corpusPlanFamilyIsUnsupported(.unsupported_delete));
    try std.testing.expect(!corpusPlanFamilyIsUnsupported(.delete));

    try std.testing.expect(corpusFixtureFamilyNeedsReason(.adapter_noop_ddl));
    try std.testing.expect(corpusFixtureFamilyNeedsReason(.unsupported_write));
    try std.testing.expect(!corpusFixtureFamilyNeedsReason(.insert));
    try std.testing.expect(corpusStableReasonToken("multi_table_generation_barrier"));
    try std.testing.expect(!corpusStableReasonToken("future_unknown_reason"));
    try std.testing.expect(corpusReasonHasNativeRequirement("recursive_cte_stream_plan"));
    try std.testing.expect(corpusReasonHasNativeRequirement("set_operation_plan"));
    try std.testing.expect(!corpusReasonHasNativeRequirement("future_unknown_reason"));
    try std.testing.expect(corpusPlanMatchesReason(.unsupported_write, "unsupported:write:requires=multi_table_generation_barrier", "multi_table_generation_barrier"));
    try std.testing.expect(!corpusPlanMatchesReason(.unsupported_write, "unsupported:write:requires=session_setting", "session_setting"));
    try std.testing.expect(corpusPlanMatchesReason(.adapter_noop_ddl, "adapter_noop:ddl:reason=session_setting", "session_setting"));
    try std.testing.expect(corpusPlanMatchesFamily(.insert_source, "insert_source:table=usage_records"));
    try std.testing.expect(!corpusPlanMatchesFamily(.insert_source, "insert:table=usage_records"));

    try std.testing.expect(corpusFixtureFamilyNeedsTableSummary(.update_source));
    try std.testing.expect(!corpusFixtureFamilyNeedsTableSummary(.ddl));
    try std.testing.expect(corpusFixtureFamilyAllowsSummary(.join));
    try std.testing.expect(!corpusFixtureFamilyAllowsSummary(.unsupported_read));
    try std.testing.expect(corpusFixtureAllowsConflictWhereSummary(.{ .name = "insert conflict", .family = .insert, .plan = "insert:table=usage_records:conflict_where=1", .sql = "INSERT INTO usage_records VALUES ('1') ON CONFLICT (id) WHERE status = 'active' DO NOTHING" }));
    try std.testing.expect(corpusFixtureAllowsConflictWhereSummary(.{ .name = "insert source conflict", .family = .insert_source, .plan = "insert_source:table=usage_records:conflict_where=1", .sql = "INSERT INTO usage_records SELECT * FROM usage_sources ON CONFLICT (id) WHERE status = 'active' DO NOTHING" }));
    try std.testing.expect(!corpusFixtureAllowsConflictWhereSummary(.{ .name = "delete source conflict", .family = .delete_source, .plan = "delete_source:table=usage_records:source_pred=1", .sql = "DELETE FROM usage_records WHERE status = 'closed'" }));
    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(std.testing.allocator, .{
        .name = "delete source conflict summary",
        .family = .delete_source,
        .summary = .{ .table_name = "usage_records", .conflict_where = true },
        .plan = "delete_source:table=usage_records:source_pred=1:source_order=0:source_limit=-1:claim=locked:returning=0:returning_expr=0:returning_all=0",
        .sql = "DELETE FROM usage_records WHERE status = 'closed'",
    }));

    try std.testing.expect(corpusReadPlanHasPrefix(.{ .name = "read query", .family = .read, .plan = "read:query:table=usage_records", .sql = "SELECT * FROM usage_records" }, "read:query:"));
    try std.testing.expect(corpusReadPlanHasPrefix(.{ .name = "explain read", .family = .explain, .plan = "explain:kind=read:inner=read:query:table=usage_records", .sql = "EXPLAIN SELECT * FROM usage_records" }, "read:query:"));
    try std.testing.expect(!corpusReadPlanHasPrefix(.{ .name = "read aggregate", .family = .read, .plan = "read:aggregate:table=usage_records", .sql = "SELECT count(*) FROM usage_records" }, "read:query:"));
    try std.testing.expect(!corpusReadPlanHasPrefix(.{ .name = "read query extra", .family = .read, .plan = "read:query_extra:table=usage_records", .sql = "SELECT * FROM usage_records" }, "read:query:"));
    try std.testing.expect(!corpusReadPlanHasPrefix(.{ .name = "malformed read prefix", .family = .read, .plan = "read:query:table=usage_records", .sql = "SELECT * FROM usage_records" }, "read:query"));
    try std.testing.expect(corpusExplainWriteInnerHasPrefix(.{ .name = "explain write", .family = .explain, .plan = "explain:kind=write:inner=insert:table=usage_records", .sql = "EXPLAIN INSERT INTO usage_records VALUES ('1')" }, ":inner=insert:"));
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(.{ .name = "explain write extra", .family = .explain, .plan = "explain:kind=write:inner=insert_source:table=usage_records", .sql = "EXPLAIN INSERT INTO usage_records SELECT * FROM usage_records" }, ":inner=insert:"));
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(.{ .name = "malformed explain prefix", .family = .explain, .plan = "explain:kind=write:inner=insert:table=usage_records", .sql = "EXPLAIN INSERT INTO usage_records VALUES ('1')" }, ":inner=insert"));
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(.{ .name = "explain read insert token", .family = .explain, .plan = "explain:kind=read:inner=insert:table=usage_records", .sql = "EXPLAIN SELECT * FROM usage_records" }, ":inner=insert:"));

    var parsed_expression_selector = try tokenized.ParsedSql.initAlloc(std.testing.allocator, "UPDATE usage_records SET status = 'disabled' WHERE email = $1 AND tenant_id = 't1' AND status = 'active' RETURNING id, status");
    defer parsed_expression_selector.deinit(std.testing.allocator);
    try std.testing.expect(appParityPointWriteHasExpressionPartialUniqueSelector(.{
        .name = "renamed fixture still proves selector coverage",
        .family = .update,
        .plan = "update:table=usage_records:transforms=1:ops=1:returning_rows=1:returning_expr=0:op_set=1",
        .apply_setup_sql = &.{
            "CREATE TABLE usage_records (id uuid PRIMARY KEY, tenant_id text, email text, status text);",
            "CREATE UNIQUE INDEX usage_records_active_tenant_email_key ON usage_records (email) WHERE concat_ws(':', tenant_id, status) = 't1:active';",
            "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_active_tenant_email_key;",
        },
        .resolver_row_json = "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"email\":\"a@example.test\",\"status\":\"active\"}",
        .sql = parsed_expression_selector.sql(),
    }, parsed_expression_selector.items(), try appParitySetupSqlSummaryAlloc(std.testing.allocator, &.{
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, tenant_id text, email text, status text);",
        "CREATE UNIQUE INDEX usage_records_active_tenant_email_key ON usage_records (email) WHERE concat_ws(':', tenant_id, status) = 't1:active';",
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_active_tenant_email_key;",
    }), .update));
    try std.testing.expect(!appParityPointWriteHasExpressionPartialUniqueSelector(.{
        .name = "field partial selector does not prove expression selector coverage",
        .family = .update,
        .plan = "update:table=usage_records:transforms=1:ops=1:returning_rows=1:returning_expr=0:op_set=1",
        .apply_setup_sql = &.{
            "CREATE TABLE usage_records (id uuid PRIMARY KEY, email text, status text);",
            "CREATE UNIQUE INDEX usage_records_active_email_key ON usage_records (email) WHERE status = 'active';",
            "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_active_email_key;",
        },
        .resolver_row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"active\"}",
        .sql = parsed_expression_selector.sql(),
    }, parsed_expression_selector.items(), try appParitySetupSqlSummaryAlloc(std.testing.allocator, &.{
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, email text, status text);",
        "CREATE UNIQUE INDEX usage_records_active_email_key ON usage_records (email) WHERE status = 'active';",
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_active_email_key;",
    }), .update));

    var parsed_expression_assignment = try tokenized.ParsedSql.initAlloc(std.testing.allocator, "UPDATE usage_records SET status = lower(status) WHERE id = 'u1' RETURNING status");
    defer parsed_expression_assignment.deinit(std.testing.allocator);
    try std.testing.expect(appParityTokensHaveSetFunctionAssignment(parsed_expression_assignment.items(), "status", "lower"));
    var parsed_returning_expression_only = try tokenized.ParsedSql.initAlloc(std.testing.allocator, "UPDATE usage_records SET status = 'active' WHERE id = 'u1' RETURNING lower(status)");
    defer parsed_returning_expression_only.deinit(std.testing.allocator);
    try std.testing.expect(!appParityTokensHaveSetFunctionAssignment(parsed_returning_expression_only.items(), "status", "lower"));

    try std.testing.expect(corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records", ":having_expr=", 0));
    try std.testing.expect(corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records:having_expr=2", ":having_expr=", 2));
    try std.testing.expect(!corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records:having_expr=2", ":having_expr=", 0));
    try std.testing.expect(corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records", ":select_all=", false));
    try std.testing.expect(corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records:select_all=1", ":select_all=", true));
    try std.testing.expect(!corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records:select_all=0", ":select_all=", true));

    try std.testing.expect(corpusFixtureHasAccessSummary(.{ .json_contains = 1 }));
    try std.testing.expect(!corpusFixtureHasAccessSummary(.{ .table_name = "usage_records" }));
    try std.testing.expect(corpusFixtureHasTemporalDdlSummary(.{ .name = "temporal ddl", .family = .ddl, .plan = "ddl:create_table:table=usage_records", .sql = "CREATE TABLE usage_records (id text)", .summary = .{ .temporal_foreign_keys = 1 } }));
    try std.testing.expect(!corpusFixtureHasTemporalDdlSummary(.{ .name = "ordinary ddl", .family = .ddl, .plan = "ddl:create_table:table=usage_records", .sql = "CREATE TABLE usage_records (id text)", .summary = .{ .ddl_tag = .create_table } }));
    try std.testing.expect(corpusFixturePlanMatchesSourceTable(
        .{ .name = "insert source", .family = .insert_source, .plan = "insert_source:table=usage_records:source_table=usage_sources", .sql = "INSERT INTO usage_records SELECT * FROM usage_sources" },
        "usage_sources",
    ));
    try std.testing.expect(!corpusFixturePlanMatchesSourceTable(
        .{ .name = "insert source mismatch", .family = .insert_source, .plan = "insert_source:table=usage_records:source_table=usage_sources", .sql = "INSERT INTO usage_records SELECT * FROM usage_sources" },
        "other_sources",
    ));
    try std.testing.expect(corpusFixturePlanMatchesSourceTable(
        .{ .name = "recursive insert source", .family = .recursive_insert_source, .plan = "recursive_insert_source:cte=source_rows:insert=insert_source:table=archived_records:source_table=usage_records", .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archived_records SELECT id FROM source_rows" },
        "usage_records",
    ));
    try std.testing.expect(corpusFixturePlanMatchesSourceTable(
        .{ .name = "recursive insert source anchor", .family = .recursive_insert_source, .plan = "recursive_insert_source:cte=source_rows:anchor_table=usage_records:insert=insert_source:table=archived_records:source_cte=1", .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archived_records SELECT id FROM source_rows" },
        "usage_records",
    ));
    try std.testing.expect(corpusFixturePlanMatchesSourceTable(
        .{ .name = "set operation", .family = .read, .plan = "read:set_operation:set_operation:op=union_all:left=left:table=usage_records:right=right:table=archived_records", .sql = "SELECT id FROM usage_records UNION ALL SELECT id FROM archived_records" },
        "archived_records",
    ));
    const alloc = std.testing.allocator;

    try std.testing.expect(try corpusFixtureSqlParameterCoverageMatchesAlloc(alloc, .{
        .name = "query params",
        .family = .query,
        .plan = "query:table=usage_records",
        .sql = "SELECT id FROM usage_records WHERE tenant_id = $1",
        .params = &.{.{ .string = "tenant-a" }},
    }));
    try std.testing.expect(try corpusFixtureSqlParameterCoverageMatchesAlloc(alloc, .{
        .name = "prepare params",
        .family = .ddl,
        .summary = .{ .ddl_tag = .prepare_statement },
        .plan = "ddl:prepare:params=2",
        .sql = "PREPARE lookup AS SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $2",
    }));
    try std.testing.expect(!(try corpusFixtureSqlParameterCoverageMatchesAlloc(alloc, .{
        .name = "prepare missing param",
        .family = .ddl,
        .summary = .{ .ddl_tag = .prepare_statement },
        .plan = "ddl:prepare:params=2",
        .sql = "PREPARE lookup AS SELECT id FROM usage_records WHERE tenant_id = $1",
    })));
}

test "sql adapter corpus rejects malformed fixture root metadata" {
    const alloc = std.testing.allocator;
    const mismatched_count_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 1,
        \\  "entry_count": 2,
        \\  "skipped_entries": [],
        \\  "schema_json": "{}",
        \\  "entries": [
        \\    {"name": "read", "family": "read", "plan": "read:table=usage_records", "sql": "SELECT * FROM usage_records"}
        \\  ]
        \\}
    ;
    var parsed_count = try std.json.parseFromSlice(std.json.Value, alloc, mismatched_count_json, .{});
    defer parsed_count.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureRootAlloc(alloc, parsed_count.value));

    const unknown_root_key_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 0,
        \\  "entry_count": 0,
        \\  "skipped_entries": [],
        \\  "schema_json": "{}",
        \\  "entries": [],
        \\  "unexpected": true
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_root_key_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureRootAlloc(alloc, parsed_unknown.value));
}

test "sql adapter corpus rejects fixture unknown keys and malformed scalars" {
    const alloc = std.testing.allocator;
    const unknown_key_json =
        \\{
        \\  "name": "bad",
        \\  "family": "read",
        \\  "plan": "read:table=usage_records",
        \\  "sql": "SELECT * FROM usage_records",
        \\  "unexpected": true
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_key_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureEntryAlloc(alloc, parsed_unknown.value));

    const object = try fixtureJsonObject(parsed_unknown.value);
    try std.testing.expectError(error.TestUnexpectedResult, fixtureJsonOptionalUsize(object, "unexpected"));
}

test "sql adapter corpus fingerprints unsupported and adapter no-op reasons" {
    const alloc = std.testing.allocator;
    const unsupported = try unsupportedFingerprintAlloc(alloc, .write, .multi_table_generation_barrier);
    defer alloc.free(unsupported);
    try std.testing.expectEqualStrings("unsupported:write:requires=multi_table_generation_barrier", unsupported);
    try std.testing.expect(unsupportedPlanMatchesReason(unsupported, .write, .multi_table_generation_barrier));
    try std.testing.expect(!unsupportedPlanMatchesReason(unsupported, .write, .session_setting));
    try std.testing.expect(!unsupportedPlanMatchesReason("unsupported:write:requires=multi_table_generation_barrier_extra", .write, .multi_table_generation_barrier));

    const noop = try adapterNoopFingerprintAlloc(alloc, "ddl", .session_setting);
    defer alloc.free(noop);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=session_setting", noop);
    try std.testing.expect(adapterNoopPlanMatchesReason(noop, "ddl", .session_setting));
    try std.testing.expect(!adapterNoopPlanMatchesReason(noop, "ddl", .set_operation_plan));
    try std.testing.expect(!adapterNoopPlanMatchesReason("adapter_noop:ddl:reason=session_setting_extra", "ddl", .session_setting));

    try std.testing.expectError(error.UnsupportedSqlShape, unsupportedFingerprintAlloc(alloc, .write, .session_setting));
    try std.testing.expectError(error.UnsupportedSqlShape, adapterNoopFingerprintAlloc(alloc, "ddl", .set_operation_plan));
}

test "sql adapter corpus appends owned fingerprint fields" {
    const alloc = std.testing.allocator;

    var fingerprint = try alloc.dupe(u8, "query:table=usage_records");
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "pred", 2);
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", 4);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr", 1);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "select_all", true);
    fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "verbose", false);
    fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, "claim", "skip_locked");
    defer alloc.free(fingerprint);

    try std.testing.expectEqualStrings(
        "query:table=usage_records:pred=2:offset=4:left_expr=1:select_all=1:verbose=0:claim=skip_locked",
        fingerprint,
    );

    var unchanged = try alloc.dupe(u8, "query:table=usage_records");
    unchanged = try appendNonZeroUsizeFingerprintAlloc(alloc, unchanged, "pred", 0);
    unchanged = try appendTrueBoolFingerprintAlloc(alloc, unchanged, "select_all", false);
    defer alloc.free(unchanged);
    try std.testing.expectEqualStrings("query:table=usage_records", unchanged);
}

test "sql adapter corpus fingerprints select all extra output labels from plan structure" {
    const alloc = std.testing.allocator;

    var query: db_mod.types.RelationalRowsQueryRequest = .{
        .select_all = true,
        .field_aliases = &.{.{ .field = "status", .output = "status_2" }},
        .expressions = &.{.{
            .output = "status_3",
            .expression = .{ .kind = .lower, .field = "status" },
        }},
        .limit = 10,
    };
    const limited = try queryFingerprintAlloc(alloc, "query", "usage_records", query, 0);
    defer alloc.free(limited);
    try std.testing.expect(planHasExactStringToken(limited, ":select_all_alias0=", "status_2"));
    try std.testing.expect(planHasExactStringToken(limited, ":select_all_expr0=", "status_3"));

    query.limit = null;
    query.text_patterns = &.{.{
        .field = "status",
        .pattern = "active%",
    }};
    const text_pattern = try queryFingerprintAlloc(alloc, "query", "usage_records", query, 0);
    defer alloc.free(text_pattern);
    try std.testing.expect(planHasExactStringToken(text_pattern, ":select_all_alias0=", "status_2"));
    try std.testing.expect(planHasExactStringToken(text_pattern, ":select_all_expr0=", "status_3"));
}

test "sql adapter corpus string token matching is exact and unique" {
    const plan = "query:table=usage_records:claim=no_key_update_nowait:limit=none";
    try std.testing.expect(planHasExactStringToken(plan, ":claim=", "no_key_update_nowait"));
    try std.testing.expect(!planHasExactStringToken(plan, ":claim=", "no_key_update"));
    try std.testing.expect(planHasStringToken(plan, ":limit="));
    try std.testing.expect(planTokenAbsent(plan, ":offset="));
    try std.testing.expect(planHasAnyExactStringToken(plan, ":claim=", &.{
        "no_key_update",
        "no_key_update_nowait",
    }));

    const duplicate = "read:query:table=usage_records:table=usage_records";
    try std.testing.expect(!planHasExactStringToken(duplicate, ":table=", "usage_records"));
    try std.testing.expect(!planHasStringToken(duplicate, ":table="));
}

test "sql adapter corpus numeric and bool token matching is exact and unique" {
    const plan = "applied:rebuild=true:validation=false:rewrite=false:unvalidated_unique=10:unvalidated_fk=1:expr=0:generated_expr=1:kind=none:setting=search_path";
    try std.testing.expect(planHasExactBoolToken(plan, "rebuild=", true));
    try std.testing.expect(planHasExactBoolToken(plan, "validation=", false));
    try std.testing.expectEqual(@as(?usize, 10), planUsizeTokenValue(plan, "unvalidated_unique="));
    try std.testing.expect(planHasExactUsizeToken(plan, "unvalidated_fk=", 1));
    try std.testing.expect(planHasNonZeroToken(plan, "unvalidated_unique="));
    try std.testing.expect(planHasNonZeroUsizeTokenNamePrefix(plan, "unvalidated_"));
    try std.testing.expectEqual(@as(?usize, 0), planUsizeOptionalTokenValue(plan, "missing="));
    try std.testing.expect(planUsizeTokenSumMatches(plan, &.{ "unvalidated_unique=", "unvalidated_fk=" }, 11));
    try std.testing.expect(planUsizeOptionalTokenSumMatches(plan, &.{ "unvalidated_fk=", "missing=", "generated_expr=" }, 2));
    try std.testing.expect(planBoolTokenSumMatches(plan, &.{ "rebuild=", "validation=", "rewrite=" }, 1));
    try std.testing.expect(planNonNoneStringTokenSumMatches(plan, &.{ "kind=", "setting=" }, 1));
    try std.testing.expect(planHasAnyNonZeroToken(plan, &.{ "expr=", "generated_expr=" }));
    try std.testing.expect(!planHasAnyNonZeroToken(plan, &.{ "expr=", "missing=" }));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=10x", "pred=", 10));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=1:pred=1", "pred=", 1));
    try std.testing.expect(!planHasExactBoolToken("ddl:replace=true_extra", "replace=", true));
}

test "sql adapter corpus cte coverage tokens are exact" {
    const malformed = "query:table=usage_records:ctes=1:cte0_expr_pred=2x";
    try std.testing.expect(!planHasNonZeroUsizeTokenNamePrefix(malformed, "cte0_"));
    try std.testing.expect(!planHasNonZeroUsizeTokenNamePrefix(malformed, "cte0_expr_"));
    try std.testing.expect(!planHasNonZeroUsizeTokenNamePrefix("query:table=usage_records:ctes=1:cte0_expr_pred=0", "cte0_expr_"));
    try std.testing.expect(planHasNonZeroUsizeTokenNamePrefix("query:table=usage_records:ctes=1:cte0_expr_pred=2", "cte0_expr_"));

    var coverage = AppParityCorpusCoverage{};
    try coverage.observe(std.testing.allocator, .{
        .name = "malformed cte expression predicate token",
        .sql = "WITH active_usage AS (SELECT id FROM usage_records WHERE lower(status) = 'active') SELECT id FROM active_usage",
        .family = .query,
        .plan = malformed,
    });
    try std.testing.expect(!coverage.query_cte_structured_access);
    try std.testing.expect(!coverage.query_cte_expression_access);

    try coverage.observe(std.testing.allocator, .{
        .name = "valid cte expression predicate token",
        .sql = "WITH active_usage AS (SELECT id FROM usage_records WHERE lower(status) = 'active') SELECT id FROM active_usage",
        .family = .query,
        .plan = "query:table=usage_records:ctes=1:cte0_expr_pred=2",
    });
    try std.testing.expect(coverage.query_cte_structured_access);
    try std.testing.expect(coverage.query_cte_expression_access);
}

test "sql adapter corpus plan predicates are exact and structured" {
    const applied = "applied:rebuild=true:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=10:unvalidated_fk=1:unvalidated_check=0:update_policy=0:work_items=1:work=rebuild/table/derived_artifacts";
    try std.testing.expect(appliedPlanIsStructured(applied));
    try std.testing.expect(appliedPlanHasExactBoolToken(applied, "rebuild=", true));
    try std.testing.expect(appliedPlanHasExactBoolToken(applied, "validation=", false));
    try std.testing.expect(appliedPlanHasExactUsizeToken(applied, "unvalidated_unique=", 10));
    try std.testing.expect(!appliedPlanHasExactUsizeToken(applied, "unvalidated_unique=", 1));
    try std.testing.expect(!appliedPlanHasExactBoolToken("applied:rebuild=true:rewrite=false", "rebuild=", true));
    try std.testing.expect(appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true:work_items=3:work=rebuild/table/derived_artifacts,validate/table/constraints,rewrite/table/row_images"));
    try std.testing.expect(appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true:work_items=3:work=rebuild/table/derived_artifacts,validate/table/constraints,rewrite/table/row_images(row_plan=:rename(status->state):drop(legacy_status))"));
    try std.testing.expect(!appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true:work_items=3:work=rebuild/table/derived_artifacts,validate/table/constraints"));
    try std.testing.expect(!appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true:work_items=0:work=none:extra=1"));

    const explain = "explain:kind=write:analyze=false:inner=insert:table=usage_records:writes=1:transforms=0";
    try std.testing.expect(explainPlanHasKind(explain, "write"));
    try std.testing.expect(!explainPlanHasKind(explain, "read"));
    try std.testing.expect(explainPlanInnerHasRootKind(explain, "insert"));
    try std.testing.expect(!explainPlanInnerHasRootKind("explain:kind=write:inner=insert:inner=update:", "insert"));
    try std.testing.expect(!explainPlanInnerHasRootKind("explain:kind=write:inner=insert_source:table=usage_records", "insert"));

    try std.testing.expect(writePlanHasCounts("insert:table=usage_records:writes=2:transforms=1", 2, 1));
    try std.testing.expect(!writePlanHasCounts("insert:table=usage_records:writes=2:transforms=1", 1, 1));
    try std.testing.expect(joinedSourcePlanHasCounts("update_joined_source:table=usage_records:right_pred=1:on=2", 1, 2));
    try std.testing.expect(!joinedSourcePlanHasCounts("update_joined_source:table=usage_records:right_pred=1:on=2", 0, 2));
}

test "sql adapter corpus detects create-table empty-catalog applicability with exact bool tokens" {
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create-table-empty-catalog-applies",
        .sql = "CREATE TABLE usage_records (id TEXT PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=false:pk=1",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create-table-if-not-exists-empty-catalog-skips",
        .sql = "CREATE TABLE IF NOT EXISTS usage_records (id TEXT PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=true:pk=1",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create-table-replace-empty-catalog-skips",
        .sql = "CREATE OR REPLACE TABLE usage_records (id TEXT PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=false:replace=true:pk=1",
    }));
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create-table-if-not-exists-extra-token-still-applies",
        .sql = "CREATE TABLE IF NOT EXISTS usage_records (id TEXT PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=true_extra:pk=1",
    }));
}

test "sql adapter corpus exact-token helpers reject ddl submode suffixes" {
    const comment = "ddl:comment:kind=table_extra:object=users:comment=true";
    try std.testing.expect(!planHasExactStringToken(comment, ":kind=", "table"));
    try std.testing.expect(planHasExactStringToken(comment, ":kind=", "table_extra"));

    const transaction = "ddl:transaction_control:kind=transaction_mode:starter=start_transaction_extra:isolation=serializable:access=none:deferrable=none";
    try std.testing.expect(!planHasExactStringToken(transaction, ":starter=", "start_transaction"));
    try std.testing.expect(planHasExactStringToken(transaction, ":starter=", "start_transaction_extra"));

    const population = "relation_population:mode=create_table_as_extra:target=usage_archive:lifetime=durable:if_not_exists=false:source=read:query:query:table=usage_records";
    try std.testing.expect(!planHasExactStringToken(population, "relation_population:mode=", "create_table_as"));
    try std.testing.expect(planHasExactStringToken(population, "relation_population:mode=", "create_table_as_extra"));
}

test "sql adapter corpus data-driven summary regressions" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("../api/fixtures/sql_api_summary_regressions.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();
    var required_assertions = try parseAppParitySummaryAssertionRequirementsAlloc(alloc);
    defer required_assertions.deinit(alloc);

    const root = try fixtureJsonObject(parsed.value);
    try fixtureRequireOnlyKeys(root, &.{ "summary_format", "cases" });
    const summary_format = try fixtureJsonOptionalU64(root, "summary_format", 0);
    if (summary_format != app_parity_summary_regression_fixture_format) return error.TestUnexpectedResult;
    const cases = switch (root.get("cases") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (cases.len == 0) return error.TestUnexpectedResult;
    var seen_assertions = std.StringHashMapUnmanaged(void){};
    defer seen_assertions.deinit(alloc);
    for (cases) |regression_case| {
        try checkSummaryRegressionCase(alloc, regression_case, &seen_assertions);
    }
    for (required_assertions.root.required) |name| {
        if (!seen_assertions.contains(name)) {
            std.debug.print("missing summary regression assertion coverage: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

test "sql adapter corpus validates summary assertion requirements" {
    const alloc = std.testing.allocator;
    var required_assertions = try parseAppParitySummaryAssertionRequirementsAlloc(alloc);
    defer required_assertions.deinit(alloc);
    try std.testing.expectEqual(app_parity_summary_assertion_fixture_format, required_assertions.root.assertion_format);
    try std.testing.expect(required_assertions.root.required.len > 0);

    const incomplete_json =
        \\{
        \\  "assertion_format": 1,
        \\  "required": ["access_matches"]
        \\}
    ;
    var parsed_incomplete = try std.json.parseFromSlice(std.json.Value, alloc, incomplete_json, .{});
    defer parsed_incomplete.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSummaryAssertionRequirementsRootAlloc(alloc, parsed_incomplete.value));

    const unknown_json =
        \\{
        \\  "assertion_format": 1,
        \\  "required": [
        \\    "access_matches",
        \\    "not_a_summary_assertion"
        \\  ]
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSummaryAssertionRequirementsRootAlloc(alloc, parsed_unknown.value));
}

test "sql adapter corpus data-driven coverage regressions" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("../api/fixtures/sql_api_coverage_regressions.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();
    var required_buckets = try parseAppParityCoverageRegressionRequirementsAlloc(alloc);
    defer required_buckets.deinit(alloc);

    const root = try fixtureJsonObject(parsed.value);
    try fixtureRequireOnlyKeys(root, &.{ "coverage_format", "cases" });
    const coverage_format = try fixtureJsonOptionalU64(root, "coverage_format", 0);
    if (coverage_format != app_parity_coverage_fixture_format) return error.TestUnexpectedResult;
    const cases = switch (root.get("cases") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (cases.len == 0) return error.TestUnexpectedResult;
    var seen_buckets = std.StringHashMapUnmanaged(void){};
    defer seen_buckets.deinit(alloc);
    for (cases) |regression_case| {
        try checkCoverageRegressionCase(alloc, regression_case, &seen_buckets);
    }
    for (required_buckets.root.required) |name| {
        if (!seen_buckets.contains(name)) {
            std.debug.print("missing coverage regression bucket: {s}\n", .{name});
            return error.TestUnexpectedResult;
        }
    }
}

test "sql adapter corpus validates coverage regression bucket requirements" {
    const alloc = std.testing.allocator;
    var required_buckets = try parseAppParityCoverageRegressionRequirementsAlloc(alloc);
    defer required_buckets.deinit(alloc);
    try std.testing.expectEqual(app_parity_coverage_regression_requirement_fixture_format, required_buckets.root.coverage_format);
    try std.testing.expect(required_buckets.root.required.len > 0);

    const unknown_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": [
        \\    "aggregate_distinct_group_projection",
        \\    "not_a_coverage_bucket"
        \\  ]
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseCoverageRegressionRequirementsRootAlloc(alloc, parsed_unknown.value));

    const unsorted_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": [
        \\    "conflict_do_nothing_returning_all",
        \\    "aggregate_distinct_group_projection"
        \\  ]
        \\}
    ;
    var parsed_unsorted = try std.json.parseFromSlice(std.json.Value, alloc, unsorted_json, .{});
    defer parsed_unsorted.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseCoverageRegressionRequirementsRootAlloc(alloc, parsed_unsorted.value));
}

test "sql adapter corpus parses data-driven coverage requirements" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("../api/fixtures/sql_api_required_coverage.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();

    const root = try parseCoverageRequirementsRootAlloc(alloc, parsed.value);
    defer freeCoverageRequirementsRoot(alloc, root);
    try std.testing.expectEqual(app_parity_coverage_fixture_format, root.coverage_format);
    try std.testing.expect(root.required.len > 0);

    var coverage = AppParityCorpusCoverage{
        .query = true,
        .deterministic_returning_rows = 1,
    };
    try std.testing.expect(try appParityCoverageRequirementSatisfied(coverage, "query"));
    try std.testing.expect(try appParityCoverageRequirementSatisfied(coverage, "deterministic_returning_rows"));
    coverage.deterministic_returning_rows = 0;
    try std.testing.expect(!try appParityCoverageRequirementSatisfied(coverage, "deterministic_returning_rows"));

    const invalid_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": ["query", "not_a_real_coverage_flag"]
        \\}
    ;
    var parsed_invalid = try std.json.parseFromSlice(std.json.Value, alloc, invalid_json, .{});
    defer parsed_invalid.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseCoverageRequirementsRootAlloc(alloc, parsed_invalid.value));

    const unsorted_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": ["query", "aggregate"]
        \\}
    ;
    var parsed_unsorted = try std.json.parseFromSlice(std.json.Value, alloc, unsorted_json, .{});
    defer parsed_unsorted.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseCoverageRequirementsRootAlloc(alloc, parsed_unsorted.value));

    const incomplete_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": ["aggregate"]
        \\}
    ;
    var parsed_incomplete = try std.json.parseFromSlice(std.json.Value, alloc, incomplete_json, .{});
    defer parsed_incomplete.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseCoverageRequirementsRootAlloc(alloc, parsed_incomplete.value));
}

test "sql adapter corpus parses adapter edge case fixtures" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("../api/fixtures/sql_api_adapter_edge_cases.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();

    const root = try parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed.value);
    defer freeSqlAdapterEdgeCaseRoot(alloc, root);
    try std.testing.expectEqual(sql_adapter_edge_case_fixture_format, root.edge_case_format);
    try std.testing.expect(root.cases.len > 0);

    const valid_json =
        \\{
        \\  "edge_case_format": 1,
        \\  "cases": [
        \\    {
        \\      "name": "valid edge",
        \\      "action": "classify_write",
        \\      "coverage": ["action_classify_write", "write_kind_update"],
        \\      "sql": "UPDATE users SET organization_id = $1",
        \\      "expected_write_kind": "update",
        \\      "params": [{"string": "o1"}]
        \\    }
        \\  ]
        \\}
    ;
    var parsed_valid = try std.json.parseFromSlice(std.json.Value, alloc, valid_json, .{});
    defer parsed_valid.deinit();
    const valid_root = try parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed_valid.value);
    defer freeSqlAdapterEdgeCaseRoot(alloc, valid_root);
    try std.testing.expectEqual(SqlAdapterEdgeCaseAction.classify_write, valid_root.cases[0].action);
    try std.testing.expectEqual(classifier.SqlWriteStatementKind.update, valid_root.cases[0].expected_write_kind.?);
    try std.testing.expectEqual(@as(usize, 1), valid_root.cases[0].params.len);

    const duplicate_json =
        \\{
        \\  "edge_case_format": 1,
        \\  "cases": [
        \\    {"name": "dup", "action": "select", "coverage": ["action_select"], "sql": "SELECT id FROM users"},
        \\    {"name": "dup", "action": "select", "coverage": ["action_select"], "sql": "SELECT id FROM users"}
        \\  ]
        \\}
    ;
    var parsed_duplicate = try std.json.parseFromSlice(std.json.Value, alloc, duplicate_json, .{});
    defer parsed_duplicate.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed_duplicate.value));

    const invalid_kind_json =
        \\{
        \\  "edge_case_format": 1,
        \\  "cases": [
        \\    {
        \\      "name": "bad kind",
        \\      "action": "classify_write",
        \\      "coverage": ["action_classify_write"],
        \\      "sql": "UPDATE users SET organization_id = 'o1'",
        \\      "expected_write_kind": "not_a_write_kind"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_invalid_kind = try std.json.parseFromSlice(std.json.Value, alloc, invalid_kind_json, .{});
    defer parsed_invalid_kind.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed_invalid_kind.value));

    const invalid_coverage_json =
        \\{
        \\  "edge_case_format": 1,
        \\  "cases": [
        \\    {
        \\      "name": "bad coverage",
        \\      "action": "select",
        \\      "coverage": ["expected_error_unsupported_sql_shape", "action_select"],
        \\      "expected_error": "unsupported_sql_shape",
        \\      "sql": "SELECT id FROM users WHERE id = $1abc"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_invalid_coverage = try std.json.parseFromSlice(std.json.Value, alloc, invalid_coverage_json, .{});
    defer parsed_invalid_coverage.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSqlAdapterEdgeCaseRootAlloc(alloc, parsed_invalid_coverage.value));
}

test "sql adapter corpus validates adapter edge case coverage requirements" {
    const alloc = std.testing.allocator;
    var required_coverage = try parseSqlAdapterEdgeCaseCoverageRequirementsAlloc(alloc);
    defer required_coverage.deinit(alloc);
    try std.testing.expectEqual(sql_adapter_edge_coverage_fixture_format, required_coverage.root.coverage_format);
    try std.testing.expect(required_coverage.root.required.len > 0);

    const incomplete_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": ["action_select"]
        \\}
    ;
    var parsed_incomplete = try std.json.parseFromSlice(std.json.Value, alloc, incomplete_json, .{});
    defer parsed_incomplete.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSqlAdapterEdgeCaseCoverageRequirementsRootAlloc(alloc, parsed_incomplete.value));

    const unknown_json =
        \\{
        \\  "coverage_format": 1,
        \\  "required": [
        \\    "action_classify_write",
        \\    "not_a_coverage_bucket"
        \\  ]
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSqlAdapterEdgeCaseCoverageRequirementsRootAlloc(alloc, parsed_unknown.value));
}

fn appParityTokensHaveIdentifier(tokens: []const tokenized.Token, identifier: []const u8) bool {
    for (tokens) |token| {
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, identifier)) return true;
    }
    return false;
}

fn appParityTokensHaveIdentifierPrefix(tokens: []const tokenized.Token, prefix: []const u8) bool {
    for (tokens) |token| {
        if (token.kind == .identifier and std.ascii.startsWithIgnoreCase(token.text, prefix)) return true;
    }
    return false;
}

fn appParityTokensHaveKeyword(tokens: []const tokenized.Token, keyword: token_mod.TokenKeyword) bool {
    for (tokens) |token| {
        if (token.matchesKeywordTag(keyword)) return true;
    }
    return false;
}

fn appParityTokensHaveKeywordSequence(tokens: []const tokenized.Token, keywords: []const token_mod.TokenKeyword) bool {
    if (keywords.len == 0) return true;
    if (tokens.len < keywords.len) return false;
    var start: usize = 0;
    while (start + keywords.len <= tokens.len) : (start += 1) {
        var offset: usize = 0;
        while (offset < keywords.len and tokens[start + offset].matchesKeywordTag(keywords[offset])) : (offset += 1) {}
        if (offset == keywords.len) return true;
    }
    return false;
}

fn appParityTokensHaveKeywordsInOrder(tokens: []const tokenized.Token, keywords: []const token_mod.TokenKeyword) bool {
    if (keywords.len == 0) return true;
    var next: usize = 0;
    for (tokens) |token| {
        if (token.matchesKeywordTag(keywords[next])) {
            next += 1;
            if (next == keywords.len) return true;
        }
    }
    return false;
}

fn appParityTokensHaveKeywordThenKind(tokens: []const tokenized.Token, keyword: token_mod.TokenKeyword, kind: token_mod.TokenKind) bool {
    if (tokens.len < 2) return false;
    var index: usize = 0;
    while (index + 1 < tokens.len) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword) and tokens[index + 1].kind == kind) return true;
    }
    return false;
}

fn appParityTokensStartWithKeyword(tokens: []const tokenized.Token, keyword: token_mod.TokenKeyword) bool {
    return tokens.len > 0 and tokens[0].matchesKeywordTag(keyword);
}

fn appParityTokensHaveConflictConstraint(tokens: []const tokenized.Token, name: []const u8) bool {
    if (tokens.len < 5) return false;
    var index: usize = 0;
    while (index + 4 < tokens.len) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.on) and
            tokens[index + 1].matchesKeywordTag(.conflict) and
            tokens[index + 2].matchesKeywordTag(.on) and
            tokens[index + 3].matchesKeywordTag(.constraint) and
            tokens[index + 4].kind == .identifier and
            std.ascii.eqlIgnoreCase(tokens[index + 4].text, name))
        {
            return true;
        }
    }
    return false;
}

const AppParityConflictTargetRange = struct {
    start: usize,
    end: usize,
    after: usize,
};

fn appParityConflictTargetRange(tokens: []const tokenized.Token) ?AppParityConflictTargetRange {
    if (tokens.len < 4) return null;
    var index: usize = 0;
    while (index + 3 < tokens.len) : (index += 1) {
        if (!tokens[index].matchesKeywordTag(.on) or
            !tokens[index + 1].matchesKeywordTag(.conflict) or
            tokens[index + 2].kind != .lparen)
        {
            continue;
        }
        const close_index = parser.findMatchingRParenIndex(tokens, index + 2) orelse return null;
        return .{
            .start = index + 3,
            .end = close_index,
            .after = close_index + 1,
        };
    }
    return null;
}

fn appParityConflictTargetHasIdentifier(tokens: []const tokenized.Token, identifier: []const u8) bool {
    const target = appParityConflictTargetRange(tokens) orelse return false;
    return appParityTokensHaveIdentifier(tokens[target.start..target.end], identifier);
}

fn appParityConflictTargetHasFunctionCall(tokens: []const tokenized.Token, name: []const u8) bool {
    const target = appParityConflictTargetRange(tokens) orelse return false;
    return appParityTokensHaveFunctionCall(tokens[target.start..target.end], name);
}

fn appParityConflictTargetHasWhere(tokens: []const tokenized.Token) bool {
    const target = appParityConflictTargetRange(tokens) orelse return false;
    return target.after < tokens.len and tokens[target.after].matchesKeywordTag(.where);
}

fn appParityTokensHaveFunctionCall(tokens: []const tokenized.Token, name: []const u8) bool {
    if (tokens.len < 2) return false;
    var index: usize = 0;
    while (index + 1 < tokens.len) : (index += 1) {
        if (tokens[index + 1].kind == .lparen and
            tokens[index].kind == .identifier and
            std.ascii.eqlIgnoreCase(tokens[index].text, name))
        {
            return true;
        }
    }
    return false;
}

fn appParityTokensHaveSetFunctionAssignment(tokens: []const tokenized.Token, field: []const u8, function_name: []const u8) bool {
    if (tokens.len < 6) return false;
    var index: usize = 0;
    while (index + 5 < tokens.len) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.set) and
            tokens[index + 1].kind == .identifier and
            std.ascii.eqlIgnoreCase(tokens[index + 1].text, field) and
            tokens[index + 2].kind == .eq and
            tokens[index + 3].kind == .identifier and
            std.ascii.eqlIgnoreCase(tokens[index + 3].text, function_name) and
            tokens[index + 4].kind == .lparen)
        {
            return parser.findMatchingRParenIndex(tokens, index + 4) != null;
        }
    }
    return false;
}

fn appParityTokensHaveFunctionCallWithKeyword(tokens: []const tokenized.Token, name: []const u8, keyword: token_mod.TokenKeyword) bool {
    if (tokens.len < 4) return false;
    var index: usize = 0;
    while (index + 1 < tokens.len) : (index += 1) {
        if (tokens[index + 1].kind != .lparen or
            tokens[index].kind != .identifier or
            !std.ascii.eqlIgnoreCase(tokens[index].text, name))
        {
            continue;
        }
        const close_index = parser.findMatchingRParenIndex(tokens, index + 1) orelse return false;
        for (tokens[index + 2 .. close_index]) |token| {
            if (token.matchesKeywordTag(keyword)) return true;
        }
    }
    return false;
}

fn appParityTokensHaveFunctionCallWithLiteral(tokens: []const tokenized.Token, name: []const u8, literal: []const u8) bool {
    if (tokens.len < 4) return false;
    var index: usize = 0;
    while (index + 1 < tokens.len) : (index += 1) {
        if (tokens[index + 1].kind != .lparen or
            tokens[index].kind != .identifier or
            !std.ascii.eqlIgnoreCase(tokens[index].text, name))
        {
            continue;
        }
        const close_index = parser.findMatchingRParenIndex(tokens, index + 1) orelse return false;
        for (tokens[index + 2 .. close_index]) |token| {
            if (token.kind == .string and std.mem.eql(u8, token.text, literal)) return true;
        }
    }
    return false;
}

fn appParityTokensHaveStringLiteral(tokens: []const tokenized.Token, literal: []const u8) bool {
    for (tokens) |token| {
        if (token.kind == .string and std.mem.eql(u8, token.text, literal)) return true;
    }
    return false;
}

fn appParityTokensHaveStringLiteralContaining(tokens: []const tokenized.Token, needle: []const u8) bool {
    for (tokens) |token| {
        if (token.kind == .string and std.mem.indexOf(u8, token.text, needle) != null) return true;
    }
    return false;
}

fn appParityTokensHaveKind(tokens: []const tokenized.Token, kind: token_mod.TokenKind) bool {
    for (tokens) |token| {
        if (token.kind == kind) return true;
    }
    return false;
}

fn appParityTokensHaveKindSequence(tokens: []const tokenized.Token, kinds: []const token_mod.TokenKind) bool {
    if (kinds.len == 0) return true;
    if (tokens.len < kinds.len) return false;
    var start: usize = 0;
    while (start + kinds.len <= tokens.len) : (start += 1) {
        var offset: usize = 0;
        while (offset < kinds.len and tokens[start + offset].kind == kinds[offset]) : (offset += 1) {}
        if (offset == kinds.len) return true;
    }
    return false;
}

fn appParityParsedSqlHasComputedPattern(parsed_sql: *const tokenized.ParsedSql) bool {
    const tokens = parsed_sql.items();
    return appParityTokensHaveIdentifier(tokens, "lower") and
        (appParityTokensHaveKeyword(tokens, .like) or appParityTokensHaveKeyword(tokens, .ilike));
}

const AppParitySetupSqlSummary = struct {
    create_table_email_unique_constraint: bool = false,
    alter_add_email_unique_constraint: bool = false,
    create_unique_index: bool = false,
    partial_unique_index: bool = false,
    validated_constraint: bool = false,
    renamed_usage_records_primary_constraint: bool = false,
    expression_unique_lower_upper: bool = false,
    mixed_expression_unique_tenant_lower: bool = false,
    partial_expression_unique_concat_ws: bool = false,
    partial_expression_unique_amount_inequality: bool = false,

    fn observePlan(self: *@This(), plan: binder.LogicalSqlPlan) void {
        switch (plan) {
            .table_ddl => |table_plan| switch (table_plan) {
                .create_table => |create| {
                    for (create.unique_constraints) |constraint| self.observeCreateTableUniqueConstraint(constraint);
                },
                .create_index => |create| self.observeCreateIndex(create),
                .alter_table => |alter| {
                    for (alter.operations) |operation| switch (operation) {
                        .add_unique_constraint => |constraint| self.observeAlterUniqueConstraint(constraint),
                        .rename_constraint => |rename| {
                            if (std.ascii.eqlIgnoreCase(rename.old_name, "usage_records_pkey") and
                                std.ascii.eqlIgnoreCase(rename.new_name, "usage_records_id_pk"))
                            {
                                self.renamed_usage_records_primary_constraint = true;
                            }
                        },
                        .validate_constraint => self.validated_constraint = true,
                        else => {},
                    };
                },
                else => {},
            },
            else => {},
        }
    }

    fn observeCreateTableUniqueConstraint(self: *@This(), constraint: runtime_schema.UniqueConstraint) void {
        if (appParityStringSliceContainsIdentifier(constraint.columns, "email")) {
            self.create_table_email_unique_constraint = true;
        }
    }

    fn observeAlterUniqueConstraint(self: *@This(), constraint: runtime_schema.UniqueConstraint) void {
        if (appParityStringSliceContainsIdentifier(constraint.columns, "email")) {
            self.alter_add_email_unique_constraint = true;
        }
    }

    fn observeCreateIndex(self: *@This(), create: ddl_plan.CreateIndexPlan) void {
        if (!create.unique) return;
        self.create_unique_index = true;
        const partial = create.where.len > 0 or create.where_expressions.len > 0;
        self.partial_unique_index = self.partial_unique_index or partial;

        for (create.expressions) |expression| {
            switch (expression.op) {
                .lower, .upper => {
                    self.expression_unique_lower_upper = true;
                    if (expression.op == .lower and appParityStringSliceContainsIdentifier(create.columns, "tenant_id")) {
                        self.mixed_expression_unique_tenant_lower = true;
                    }
                },
                .expression => if (expression.expression) |row_expression| {
                    self.expression_unique_lower_upper = self.expression_unique_lower_upper or
                        appParityExpressionContainsKind(row_expression, .lower) or
                        appParityExpressionContainsKind(row_expression, .upper);
                    self.mixed_expression_unique_tenant_lower = self.mixed_expression_unique_tenant_lower or
                        (appParityStringSliceContainsIdentifier(create.columns, "tenant_id") and appParityExpressionContainsKind(row_expression, .lower));
                },
                else => {},
            }
        }
        if (create.generated_expression) |generated| {
            self.expression_unique_lower_upper = self.expression_unique_lower_upper or
                generated.op == .lower or generated.op == .upper or
                (generated.expression != null and
                    (appParityExpressionContainsKind(generated.expression.?, .lower) or
                        appParityExpressionContainsKind(generated.expression.?, .upper)));
            self.mixed_expression_unique_tenant_lower = self.mixed_expression_unique_tenant_lower or
                (appParityStringSliceContainsIdentifier(create.columns, "tenant_id") and
                    (generated.op == .lower or
                        (generated.expression != null and appParityExpressionContainsKind(generated.expression.?, .lower))));
        }

        if (partial) {
            for (create.where_expressions) |condition| {
                self.partial_expression_unique_concat_ws = self.partial_expression_unique_concat_ws or
                    appParityExpressionConditionContainsKind(condition, .concat_ws);
                self.partial_expression_unique_amount_inequality = self.partial_expression_unique_amount_inequality or
                    appParityExpressionConditionHasFieldComparison(condition, "amount", &.{ .gt, .gte, .lt, .lte });
            }
        }
    }
};

fn appParitySetupSqlSummaryAlloc(alloc: std.mem.Allocator, setup_sql: []const []const u8) !AppParitySetupSqlSummary {
    var summary: AppParitySetupSqlSummary = .{};
    for (setup_sql) |sql| {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
        defer parsed_sql.deinit(alloc);
        var plan = try logical_ddl_plan.parseLogicalDdlPlanAlloc(alloc, &parsed_sql, .{});
        defer plan.deinit(alloc);
        summary.observePlan(plan);
    }
    return summary;
}

fn appParityStringSliceContainsIdentifier(values: []const []const u8, value: []const u8) bool {
    for (values) |item| {
        if (std.ascii.eqlIgnoreCase(item, value)) return true;
    }
    return false;
}

fn appParityExpressionConditionContainsKind(
    condition: db_mod.types.RelationalRowsExpressionCondition,
    kind: db_mod.types.RelationalRowsExpressionKind,
) bool {
    if (appParityExpressionContainsKind(condition.lhs, kind)) return true;
    for (condition.rhs) |rhs| {
        if (appParityExpressionContainsKind(rhs, kind)) return true;
    }
    return false;
}

fn appParityExpressionContainsKind(
    expression: db_mod.types.RelationalRowsExpression,
    kind: db_mod.types.RelationalRowsExpressionKind,
) bool {
    if (expression.kind == kind) return true;
    for (expression.operands) |operand| {
        if (appParityExpressionContainsKind(operand, kind)) return true;
    }
    for (expression.case_branches) |branch| {
        if (appParityExpressionConditionContainsKind(branch.when, kind) or
            appParityExpressionContainsKind(branch.then, kind))
        {
            return true;
        }
    }
    for (expression.case_else) |fallback| {
        if (appParityExpressionContainsKind(fallback, kind)) return true;
    }
    return false;
}

fn appParityExpressionConditionHasFieldComparison(
    condition: db_mod.types.RelationalRowsExpressionCondition,
    field: []const u8,
    ops: []const runtime_schema.RelationalCheckOp,
) bool {
    if (!appParityExpressionContainsField(condition.lhs, field)) return false;
    for (ops) |op| {
        if (condition.op == op) return true;
    }
    return false;
}

fn appParityExpressionContainsField(expression: db_mod.types.RelationalRowsExpression, field: []const u8) bool {
    if (expression.kind == .field and std.ascii.eqlIgnoreCase(expression.field, field)) return true;
    for (expression.operands) |operand| {
        if (appParityExpressionContainsField(operand, field)) return true;
    }
    for (expression.case_branches) |branch| {
        if (appParityExpressionConditionHasField(branch.when, field) or
            appParityExpressionContainsField(branch.then, field))
        {
            return true;
        }
    }
    for (expression.case_else) |fallback| {
        if (appParityExpressionContainsField(fallback, field)) return true;
    }
    return false;
}

fn appParityExpressionConditionHasField(condition: db_mod.types.RelationalRowsExpressionCondition, field: []const u8) bool {
    if (appParityExpressionContainsField(condition.lhs, field)) return true;
    for (condition.rhs) |rhs| {
        if (appParityExpressionContainsField(rhs, field)) return true;
    }
    return false;
}

fn appParityPointWriteHasExpressionPartialUniqueSelector(
    entry: AppParityCorpusEntry,
    sql_tokens: []const tokenized.Token,
    setup_summary: AppParitySetupSqlSummary,
    family: AppParityCorpusPlanFamily,
) bool {
    if (entry.family != family or entry.apply_setup_sql.len == 0 or entry.resolver_row_json.len == 0) return false;
    if (!setup_summary.create_unique_index or !setup_summary.partial_unique_index or !setup_summary.validated_constraint) return false;
    if (!appParityTokensHaveIdentifier(sql_tokens, "email")) return false;

    const point_write_matches = switch (family) {
        .update => corpusPlanMatchesFamily(.update, entry.plan) and
            planHasExactStringToken(entry.plan, "update:table=", "usage_records") and
            planHasNonZeroToken(entry.plan, ":ops="),
        .delete => corpusPlanMatchesFamily(.delete, entry.plan) and
            planHasExactStringToken(entry.plan, "delete:table=", "usage_records") and
            planHasNonZeroToken(entry.plan, ":deletes="),
        else => false,
    };
    if (!point_write_matches) return false;

    const concat_ws_partial = setup_summary.partial_expression_unique_concat_ws and
        appParityTokensHaveIdentifier(sql_tokens, "tenant_id") and
        appParityTokensHaveIdentifier(sql_tokens, "status");
    const inequality_partial = setup_summary.partial_expression_unique_amount_inequality and
        appParityTokensHaveIdentifier(sql_tokens, "amount");
    return concat_ws_partial or inequality_partial;
}

pub const AppParityCorpusCoverage = struct {
    ddl: bool = false,
    ddl_table_clone: bool = false,
    ddl_view_create: bool = false,
    ddl_view_create_replace: bool = false,
    ddl_view_create_if_not_exists: bool = false,
    ddl_view_rename: bool = false,
    ddl_view_drop: bool = false,
    ddl_view_drop_cascade: bool = false,
    ddl_materialized_view_create: bool = false,
    ddl_materialized_view_create_replace: bool = false,
    ddl_materialized_view_create_if_not_exists: bool = false,
    ddl_materialized_view_create_no_data: bool = false,
    ddl_materialized_view_refresh: bool = false,
    ddl_materialized_view_refresh_concurrently: bool = false,
    ddl_materialized_view_refresh_no_data: bool = false,
    ddl_materialized_view_drop: bool = false,
    ddl_materialized_view_drop_cascade: bool = false,
    ddl_relation_lifetime_temporary: bool = false,
    ddl_relation_lifetime_unlogged: bool = false,
    ddl_enum_type_create: bool = false,
    ddl_enum_type_add_value: bool = false,
    ddl_enum_type_add_value_if_not_exists: bool = false,
    ddl_enum_type_add_value_position: bool = false,
    ddl_enum_type_drop: bool = false,
    ddl_enum_type_drop_cascade: bool = false,
    ddl_domain_create: bool = false,
    ddl_domain_create_default: bool = false,
    ddl_domain_create_not_null: bool = false,
    ddl_domain_alter: bool = false,
    ddl_domain_drop: bool = false,
    ddl_domain_drop_cascade: bool = false,
    ddl_sequence_create: bool = false,
    ddl_sequence_create_if_not_exists: bool = false,
    ddl_sequence_create_typed_owned: bool = false,
    ddl_sequence_alter: bool = false,
    ddl_sequence_alter_if_exists: bool = false,
    ddl_sequence_alter_typed_owned: bool = false,
    ddl_sequence_drop: bool = false,
    ddl_sequence_drop_cascade: bool = false,
    ddl_identity_allocator_serial: bool = false,
    ddl_identity_allocator_generated: bool = false,
    ddl_identity_allocator_generated_options: bool = false,
    ddl_schema_namespace_create: bool = false,
    ddl_schema_namespace_create_if_not_exists: bool = false,
    ddl_schema_namespace_rename: bool = false,
    ddl_schema_namespace_drop: bool = false,
    ddl_schema_namespace_drop_cascade: bool = false,
    ddl_extension_create: bool = false,
    ddl_extension_create_if_not_exists: bool = false,
    ddl_extension_create_quoted_sql_name: bool = false,
    ddl_extension_create_version: bool = false,
    ddl_extension_update: bool = false,
    ddl_extension_update_latest: bool = false,
    ddl_extension_update_version: bool = false,
    ddl_extension_drop: bool = false,
    ddl_extension_drop_cascade: bool = false,
    ddl_function_create: bool = false,
    ddl_function_replace: bool = false,
    ddl_function_volatility: bool = false,
    ddl_function_security: bool = false,
    ddl_function_external_security: bool = false,
    ddl_function_null_input: bool = false,
    ddl_function_cost: bool = false,
    ddl_function_rows: bool = false,
    ddl_function_parallel: bool = false,
    ddl_function_leakproof: bool = false,
    ddl_function_window: bool = false,
    ddl_function_support: bool = false,
    ddl_function_transform: bool = false,
    ddl_function_setting: bool = false,
    ddl_function_sql_expression_concat_body: bool = false,
    ddl_function_sql_expression_body: bool = false,
    ddl_function_sql_expression_multi_arg_body: bool = false,
    ddl_function_sql_expression_named_arg_body: bool = false,
    ddl_function_sql_expression_nested_body: bool = false,
    ddl_function_sql_expression_minmax_body: bool = false,
    ddl_function_trigger_perform_body: bool = false,
    ddl_function_trigger_return_new: bool = false,
    ddl_function_trigger_return_null: bool = false,
    ddl_function_trigger_return_old: bool = false,
    ddl_function_drop: bool = false,
    ddl_function_drop_cascade: bool = false,
    ddl_procedure_create: bool = false,
    ddl_procedure_noop_body: bool = false,
    ddl_procedure_perform_body: bool = false,
    ddl_procedure_drop: bool = false,
    ddl_procedure_drop_cascade: bool = false,
    ddl_role_create: bool = false,
    ddl_role_alter: bool = false,
    ddl_role_alter_database_scope: bool = false,
    ddl_role_alter_reset: bool = false,
    ddl_role_alter_current_setting: bool = false,
    ddl_role_alter_runtime_setting: bool = false,
    ddl_role_alter_runtime_reset: bool = false,
    ddl_role_drop: bool = false,
    ddl_privilege_grant: bool = false,
    ddl_privilege_revoke: bool = false,
    ddl_copy_binary_execution_contract: bool = false,
    ddl_copy_from: bool = false,
    ddl_copy_from_execution_contract: bool = false,
    ddl_copy_file_endpoint: bool = false,
    ddl_copy_from_text_execution_contract: bool = false,
    ddl_copy_default_marker: bool = false,
    ddl_copy_header: bool = false,
    ddl_copy_delimiter: bool = false,
    ddl_copy_escape: bool = false,
    ddl_copy_encoding: bool = false,
    ddl_copy_force_quote: bool = false,
    ddl_copy_force_not_null: bool = false,
    ddl_copy_force_null: bool = false,
    ddl_copy_freeze: bool = false,
    ddl_copy_log_verbosity: bool = false,
    ddl_copy_null_marker: bool = false,
    ddl_copy_oids_false_noop: bool = false,
    ddl_copy_on_error_ignore: bool = false,
    ddl_copy_program_endpoint: bool = false,
    ddl_copy_reject_limit: bool = false,
    ddl_copy_quote: bool = false,
    ddl_copy_to: bool = false,
    ddl_copy_to_execution_contract: bool = false,
    ddl_copy_to_text_execution_contract: bool = false,
    ddl_copy_where_expression: bool = false,
    ddl_partition_create_parent: bool = false,
    ddl_partition_create_child: bool = false,
    ddl_partition_attach: bool = false,
    ddl_partition_detach: bool = false,
    ddl_row_security_enable: bool = false,
    ddl_row_security_disable: bool = false,
    ddl_row_security_create_policy: bool = false,
    ddl_row_security_conjunction_policy: bool = false,
    ddl_row_security_disjunction_policy: bool = false,
    ddl_row_security_check_policy: bool = false,
    ddl_row_security_expression_policy: bool = false,
    ddl_row_security_literal_policy: bool = false,
    ddl_row_security_targeted_policy: bool = false,
    ddl_row_security_alter_policy: bool = false,
    ddl_row_security_drop_policy: bool = false,
    ddl_database_create: bool = false,
    ddl_database_alter: bool = false,
    ddl_database_alter_setting: bool = false,
    ddl_database_drop: bool = false,
    ddl_database_drop_if_exists: bool = false,
    ddl_database_drop_force: bool = false,
    ddl_tablespace_create: bool = false,
    ddl_tablespace_create_location: bool = false,
    ddl_tablespace_rename: bool = false,
    ddl_tablespace_drop: bool = false,
    ddl_tablespace_drop_if_exists: bool = false,
    ddl_notification_listen: bool = false,
    ddl_notification_notify: bool = false,
    ddl_notification_unlisten: bool = false,
    ddl_publication_create: bool = false,
    ddl_publication_create_all_tables: bool = false,
    ddl_publication_create_table_list: bool = false,
    ddl_publication_alter: bool = false,
    ddl_publication_alter_add_table: bool = false,
    ddl_publication_drop: bool = false,
    ddl_publication_drop_if_exists: bool = false,
    ddl_subscription_create: bool = false,
    ddl_subscription_create_multi_publication: bool = false,
    ddl_subscription_alter: bool = false,
    ddl_subscription_alter_enable: bool = false,
    ddl_subscription_alter_disable: bool = false,
    ddl_subscription_drop: bool = false,
    ddl_subscription_drop_if_exists: bool = false,
    ddl_collation_create: bool = false,
    ddl_collation_create_options: bool = false,
    ddl_collation_rename: bool = false,
    ddl_collation_drop: bool = false,
    ddl_collation_drop_if_exists: bool = false,
    ddl_operator_create: bool = false,
    ddl_operator_create_options: bool = false,
    ddl_operator_drop: bool = false,
    ddl_operator_drop_args: bool = false,
    ddl_operator_drop_if_exists: bool = false,
    ddl_aggregate_create: bool = false,
    ddl_aggregate_create_args: bool = false,
    ddl_aggregate_create_options: bool = false,
    ddl_aggregate_drop: bool = false,
    ddl_aggregate_drop_args: bool = false,
    ddl_aggregate_drop_if_exists: bool = false,
    ddl_cast_create: bool = false,
    ddl_cast_create_assignment: bool = false,
    ddl_cast_create_function: bool = false,
    ddl_cast_drop: bool = false,
    ddl_cast_drop_if_exists: bool = false,
    ddl_vacuum_maintenance: bool = false,
    ddl_vacuum_maintenance_options: bool = false,
    ddl_analyze_maintenance: bool = false,
    ddl_analyze_maintenance_columns: bool = false,
    ddl_analyze_maintenance_verbose: bool = false,
    ddl_reindex_maintenance: bool = false,
    ddl_reindex_maintenance_concurrently: bool = false,
    ddl_reindex_maintenance_index_target: bool = false,
    ddl_cluster_maintenance: bool = false,
    ddl_cluster_maintenance_index: bool = false,
    ddl_cluster_maintenance_verbose: bool = false,
    ddl_prepare_statement: bool = false,
    ddl_prepare_statement_read_subject: bool = false,
    ddl_prepare_statement_write_subject: bool = false,
    ddl_prepare_statement_params: bool = false,
    ddl_prepare_statement_ddl_family: bool = false,
    ddl_prepare_statement_insert_family: bool = false,
    ddl_prepare_statement_read_family: bool = false,
    ddl_prepare_statement_insert_source_family: bool = false,
    ddl_prepare_statement_truncate_family: bool = false,
    ddl_prepare_statement_update_family: bool = false,
    ddl_prepare_statement_delete_family: bool = false,
    ddl_prepare_statement_merge_family: bool = false,
    ddl_prepare_cte_write_statement: bool = false,
    ddl_prepare_recursive_cte_read_statement: bool = false,
    ddl_prepare_recursive_cte_write_statement: bool = false,
    ddl_prepared_transaction_commit: bool = false,
    ddl_prepared_transaction_prepare: bool = false,
    ddl_prepared_transaction_recovery_contract: bool = false,
    ddl_prepared_transaction_rollback: bool = false,
    ddl_execute_statement: bool = false,
    ddl_execute_statement_args: bool = false,
    ddl_deallocate_statement: bool = false,
    ddl_deallocate_statement_all: bool = false,
    ddl_declare_cursor: bool = false,
    ddl_declare_cursor_binary_hold_scroll: bool = false,
    ddl_declare_cursor_read_subject: bool = false,
    ddl_fetch_cursor: bool = false,
    ddl_fetch_cursor_absolute: bool = false,
    ddl_fetch_cursor_all: bool = false,
    ddl_fetch_cursor_backward: bool = false,
    ddl_fetch_cursor_count: bool = false,
    ddl_fetch_cursor_first: bool = false,
    ddl_fetch_cursor_forward: bool = false,
    ddl_fetch_cursor_last: bool = false,
    ddl_fetch_cursor_prior: bool = false,
    ddl_fetch_cursor_relative: bool = false,
    ddl_close_cursor: bool = false,
    ddl_close_cursor_all: bool = false,
    ddl_savepoint_transaction: bool = false,
    ddl_release_savepoint: bool = false,
    ddl_rollback_to_savepoint: bool = false,
    ddl_comment_table: bool = false,
    ddl_comment_column: bool = false,
    ddl_comment_index: bool = false,
    ddl_comment_constraint: bool = false,
    ddl_table_lock: bool = false,
    ddl_table_lock_access_exclusive: bool = false,
    ddl_table_lock_multi_table: bool = false,
    ddl_table_lock_share_row_exclusive: bool = false,
    ddl_constraint_mode: bool = false,
    ddl_constraint_mode_all: bool = false,
    ddl_constraint_mode_named: bool = false,
    ddl_constraint_mode_deferred: bool = false,
    ddl_constraint_mode_immediate: bool = false,
    ddl_set_transaction_mode: bool = false,
    ddl_start_transaction_mode: bool = false,
    ddl_begin_transaction_mode: bool = false,
    ddl_transaction_isolation: bool = false,
    ddl_transaction_read_only: bool = false,
    ddl_transaction_read_write: bool = false,
    ddl_transaction_deferrable_true: bool = false,
    ddl_transaction_deferrable_false: bool = false,
    ddl_advisory_lock: bool = false,
    ddl_advisory_unlock: bool = false,
    ddl_advisory_lock_two_keys: bool = false,
    read: bool = false,
    read_query: bool = false,
    read_recursive_cte_stream_plan: bool = false,
    read_aggregate: bool = false,
    read_join: bool = false,
    read_lateral: bool = false,
    read_window: bool = false,
    read_cte_query_expression: bool = false,
    read_cte_aggregate_expression: bool = false,
    read_cte_window_expression: bool = false,
    read_graph_table_function_cte_join: bool = false,
    read_graph_table_function_inline_join: bool = false,
    read_join_cross_table_source_schema_classifier: bool = false,
    read_lateral_cross_table_source_schema_classifier: bool = false,
    read_set_operation_cross_table_except_classifier: bool = false,
    read_set_operation_cross_table_intersect_classifier: bool = false,
    read_set_operation_cross_table_source_schema_classifier: bool = false,
    read_window_duplicate_output_label: bool = false,
    query: bool = false,
    query_select_all_disambiguated_outputs: bool = false,
    aggregate: bool = false,
    join: bool = false,
    lateral: bool = false,
    window: bool = false,
    explain: bool = false,
    explain_options: bool = false,
    explain_analyze: bool = false,
    explain_buffers: bool = false,
    explain_timing_disabled: bool = false,
    explain_summary_disabled: bool = false,
    explain_settings: bool = false,
    explain_wal: bool = false,
    explain_write: bool = false,
    relation_population_select_into: bool = false,
    relation_population_select_into_temporary: bool = false,
    relation_population_select_into_unlogged: bool = false,
    relation_population_create_table_as: bool = false,
    relation_population_create_table_as_no_data: bool = false,
    insert: bool = false,
    insert_source: bool = false,
    recursive_insert_source: bool = false,
    insert_source_expression_assignment: bool = false,
    insert_source_regexp_expression_assignment: bool = false,
    insert_source_computed_pattern_source: bool = false,
    insert_source_expression_or_source: bool = false,
    insert_source_expression_not_source: bool = false,
    insert_source_returning_all_expression: bool = false,
    insert_source_conflict_default_update: bool = false,
    insert_source_conflict_json_set_expression: bool = false,
    insert_source_conflict_regexp_expression: bool = false,
    insert_source_conflict_boolean_is_not_guard: bool = false,
    update: bool = false,
    delete: bool = false,
    update_source: bool = false,
    delete_source: bool = false,
    truncate_source: bool = false,
    update_joined_source: bool = false,
    update_joined_source_cte_mutation: bool = false,
    delete_joined_source: bool = false,
    delete_joined_source_cte_mutation: bool = false,
    adapter_noop_ddl: bool = false,
    unsupported_read: bool = false,
    unsupported_ddl: bool = false,
    unsupported_ddl_copy_wrong_stream_endpoint: bool = false,
    unsupported_ddl_copy_unsupported_options: bool = false,
    ddl_temporal_fk_delete_set_null_action: bool = false,
    ddl_temporal_fk_delete_cascade_action: bool = false,
    ddl_temporal_fk_update_cascade_action: bool = false,
    ddl_system_versioned_table: bool = false,
    query_function: bool = false,
    query_function_full_text: bool = false,
    query_function_semantic: bool = false,
    query_function_vector: bool = false,
    query_function_graph_search: bool = false,
    query_function_graph_traverse: bool = false,
    query_function_graph_shortest_path: bool = false,
    query_function_graph_k_shortest_paths: bool = false,
    query_function_graph_metric: bool = false,
    query_function_graph_metric_rerank: bool = false,
    query_function_hybrid: bool = false,
    query_function_hybrid_sources_json: bool = false,
    query_function_hybrid_source_helpers: bool = false,
    invalid_insert: bool = false,
    invalid_duplicate_row_batch_target: bool = false,
    invalid_duplicate_conflict_update_target: bool = false,
    invalid_expression_conflict_target: bool = false,
    invalid_named_conflict_target: bool = false,
    invalid_update: bool = false,
    invalid_duplicate_update_target: bool = false,
    invalid_update_multi_output_subquery_selector: bool = false,
    invalid_delete: bool = false,
    invalid_delete_multi_output_subquery_selector: bool = false,
    unsupported_insert: bool = false,
    invalid_read_row_lock_target: bool = false,
    invalid_update_source_row_lock_mode: bool = false,
    invalid_update_source_row_lock_target: bool = false,
    invalid_update_joined_source_row_lock_target: bool = false,
    query_calendar_interval_expression: bool = false,
    unsupported_read_set_operation_output_shape: bool = false,
    read_row_lock_nowait: bool = false,
    read_row_lock_share: bool = false,
    read_row_lock_key_share: bool = false,
    query_row_lock_no_key_update: bool = false,
    merge_mutation_cte: bool = false,
    merge_mutation_data_modifying_cte: bool = false,
    merge_mutation_typed_plan: bool = false,
    merge_mutation_default_expressions: bool = false,
    truncate_multi_table_generation_barrier: bool = false,
    truncate_cascade_generation_barrier: bool = false,
    truncate_continue_identity: bool = false,
    truncate_restart_identity: bool = false,
    update_source_claim_nowait: bool = false,
    update_source_claim_no_key_update: bool = false,
    update_identity_rewrite: bool = false,
    insert_source_cross_table_source_schema: bool = false,
    joined_source_cross_table_source_schema: bool = false,
    read_join_cross_table_source_schema: bool = false,
    read_lateral_cross_table_source_schema: bool = false,
    read_set_operation_cross_table_except: bool = false,
    read_set_operation_cross_table_intersect: bool = false,
    read_set_operation_cross_table_source_schema: bool = false,
    merge_cross_table_source_schema: bool = false,
    scalar_membership: bool = false,
    boolean_is_predicate: bool = false,
    boolean_is_not_predicate: bool = false,
    boolean_unknown_predicate: bool = false,
    postfix_null_test_predicate: bool = false,
    expression_postfix_null_test_predicate: bool = false,
    json_access_path: bool = false,
    array_access_path: bool = false,
    text_pattern: bool = false,
    query_access_or_predicates: bool = false,
    query_array_overlap_access_or: bool = false,
    query_access_not_predicates: bool = false,
    expression_predicate: bool = false,
    query_computed_pattern_predicate: bool = false,
    mixed_scalar_expression_or: bool = false,
    expression_order: bool = false,
    query_order_using_operator: bool = false,
    aggregate_order_using_operator: bool = false,
    join_order_using_operator: bool = false,
    lateral_order_using_operator: bool = false,
    window_order_using_operator: bool = false,
    update_source_order_using_operator: bool = false,
    delete_source_order_using_operator: bool = false,
    query_fixed_interval_expression: bool = false,
    query_mixed_interval_expression: bool = false,
    query_now_expression: bool = false,
    query_current_timestamp_expression: bool = false,
    query_current_timestamp_precision_expression: bool = false,
    query_current_date_expression: bool = false,
    query_uuid_generation_expression: bool = false,
    query_uuid_generate_v4_expression: bool = false,
    cte_stream: bool = false,
    cte_query: bool = false,
    cte_aggregate: bool = false,
    cte_window: bool = false,
    catalog_setup_sql: bool = false,
    catalog_tables_fixture_metadata: bool = false,
    applied_catalog_plan: bool = false,
    applied_catalog_rebuild: bool = false,
    applied_catalog_validation: bool = false,
    applied_catalog_rewrite: bool = false,
    deterministic_returning_rows: usize = 0,
    deterministic_insert_returning_rows: bool = false,
    deterministic_update_returning_rows: bool = false,
    deterministic_delete_returning_rows: bool = false,
    insert_typed_datetime_literal: bool = false,
    returning_all_insert: bool = false,
    returning_all_update: bool = false,
    returning_all_delete: bool = false,
    returning_all_update_source: bool = false,
    returning_all_delete_source: bool = false,
    returning_all_update_joined_source: bool = false,
    returning_all_delete_joined_source: bool = false,
    conflict_do_nothing_returning_all: bool = false,
    conflict_do_update: bool = false,
    conflict_default_update: bool = false,
    conflict_coalesce_existing_update: bool = false,
    conflict_numeric_expression_update: bool = false,
    conflict_case_expression_update: bool = false,
    conflict_current_timestamp_precision: bool = false,
    conflict_current_date_update: bool = false,
    conflict_uuid_generation_update: bool = false,
    conflict_text_expression_update: bool = false,
    conflict_octet_length_expression_update: bool = false,
    conflict_bit_length_expression_update: bool = false,
    conflict_regexp_replace_expression_update: bool = false,
    conflict_regexp_match_expression_update: bool = false,
    conflict_regexp_count_expression_update: bool = false,
    conflict_regexp_instr_expression_update: bool = false,
    conflict_regexp_substr_expression_update: bool = false,
    conflict_jsonb_update: bool = false,
    conflict_jsonb_concat_update: bool = false,
    conflict_guard_where: bool = false,
    conflict_guard_where_skip: bool = false,
    conflict_returning_expression: bool = false,
    conflict_interval_update: bool = false,
    conflict_mixed_interval_update: bool = false,
    conflict_row_assignment: bool = false,
    conflict_row_assignment_default: bool = false,
    conflict_row_assignment_constructor: bool = false,
    conflict_boolean_expression_update: bool = false,
    update_source_boolean_expression_update: bool = false,
    update_joined_source_boolean_expression_update: bool = false,
    multi_row_insert: bool = false,
    multi_row_conflict_do_nothing: bool = false,
    multi_row_conflict_do_nothing_duplicate_target: bool = false,
    write_plan_insert_op_set: bool = false,
    write_plan_insert_op_inc: bool = false,
    write_plan_update_op_set: bool = false,
    write_plan_update_op_push: bool = false,
    write_plan_update_op_pull: bool = false,
    point_update_jsonb: bool = false,
    point_update_jsonb_concat: bool = false,
    point_update_array: bool = false,
    point_update_uuid_generation: bool = false,
    point_update_patch_expression: bool = false,
    update_source_claim_skip_locked: bool = false,
    update_source_pagination: bool = false,
    update_source_nullable_pagination: bool = false,
    update_source_boolean_is_not_predicate: bool = false,
    update_source_returning_expression: bool = false,
    point_update_expression_partial_unique_selector: bool = false,
    point_delete_expression_partial_unique_selector: bool = false,
    delete_source_fetch_pagination: bool = false,
    delete_source_nullable_pagination: bool = false,
    delete_source_boolean_unknown_predicate: bool = false,
    delete_source_returning_expression: bool = false,
    joined_source_ordered_pagination: bool = false,
    joined_source_expression_predicate: bool = false,
    joined_source_expression_group: bool = false,
    joined_source_expression_array: bool = false,
    joined_source_returning_expression: bool = false,
    joined_source_returning_source_field: bool = false,
    joined_source_returning_source_expression: bool = false,
    update_joined_source_returning_source_expression: bool = false,
    delete_joined_source_returning_source_expression: bool = false,
    update_joined_source_non_primary_semijoin: bool = false,
    delete_joined_source_non_primary_semijoin: bool = false,
    update_joined_source_correlated_semijoin: bool = false,
    delete_joined_source_correlated_semijoin: bool = false,
    update_joined_source_correlated_filtered_semijoin: bool = false,
    delete_joined_source_correlated_filtered_semijoin: bool = false,
    update_joined_source_semijoin_match_expression: bool = false,
    delete_joined_source_semijoin_match_expression: bool = false,
    update_joined_source_exists_semijoin: bool = false,
    delete_joined_source_exists_semijoin: bool = false,
    update_joined_source_exists_match_expression: bool = false,
    delete_joined_source_exists_match_expression: bool = false,
    update_joined_source_row_value_semijoin: bool = false,
    delete_joined_source_row_value_semijoin: bool = false,
    update_joined_source_modulo_expression: bool = false,
    update_joined_source_regexp_expression: bool = false,
    delete_joined_source_regexp_expression: bool = false,
    update_joined_source_array_expression: bool = false,
    delete_joined_source_array_expression: bool = false,
    update_joined_source_json_expression: bool = false,
    delete_joined_source_json_expression: bool = false,
    update_joined_source_row_assignment: bool = false,
    update_joined_source_row_assignment_default: bool = false,
    update_joined_source_row_assignment_constructor: bool = false,
    update_source_patch_expression: bool = false,
    update_source_increment_expression: bool = false,
    update_source_modulo_expression: bool = false,
    update_source_regexp_replace_expression: bool = false,
    update_source_regexp_match_expression: bool = false,
    update_source_regexp_count_expression: bool = false,
    update_source_regexp_instr_expression: bool = false,
    update_source_regexp_substr_expression: bool = false,
    update_source_row_assignment: bool = false,
    update_source_row_assignment_default: bool = false,
    update_source_row_assignment_constructor: bool = false,
    schema_additive_unique_conflict_target: bool = false,
    schema_default_primary_named_conflict_target: bool = false,
    schema_custom_primary_named_conflict_target: bool = false,
    schema_unique_conflict_target: bool = false,
    schema_partial_unique_conflict_target: bool = false,
    schema_expression_unique_conflict_target: bool = false,
    schema_mixed_expression_unique_conflict_target: bool = false,
    schema_nulls_not_distinct_unique: bool = false,
    schema_rich_expression_secondary_index: bool = false,
    schema_system_versioned_table: bool = false,
    schema_temporal_numrange_insert: bool = false,
    schema_temporal_daterange_insert: bool = false,
    schema_temporal_open_daterange_insert: bool = false,
    schema_temporal_lower_open_daterange_insert: bool = false,
    schema_temporal_numrange_constructor_insert: bool = false,
    schema_temporal_daterange_constructor_insert: bool = false,
    schema_temporal_inclusive_daterange_constructor_insert: bool = false,
    schema_temporal_inclusive_daterange_literal_insert: bool = false,
    schema_temporal_lower_exclusive_daterange_constructor_insert: bool = false,
    schema_temporal_lower_exclusive_daterange_literal_insert: bool = false,
    schema_temporal_tsrange_insert: bool = false,
    schema_temporal_tsrange_constructor_insert: bool = false,
    schema_temporal_tstzrange_insert: bool = false,
    schema_temporal_tstzrange_constructor_insert: bool = false,
    schema_temporal_range_bound_query: bool = false,
    schema_temporal_range_contains_query: bool = false,
    schema_temporal_range_overlap_query: bool = false,
    schema_temporal_inclusive_daterange_overlap_query: bool = false,
    schema_temporal_unique_conflict_upsert: bool = false,
    schema_temporal_fk_ddl: bool = false,
    schema_temporal_portion_update: bool = false,
    schema_temporal_portion_delete: bool = false,
    schema_temporal_range_column_portion_update: bool = false,
    schema_temporal_range_column_portion_delete: bool = false,
    migration_equivalent_data_backfill_delete: bool = false,
    migration_equivalent_data_backfill_insert: bool = false,
    migration_equivalent_data_backfill_update: bool = false,
    migration_equivalent_schema_metadata: bool = false,
    migration_equivalent_schema_rebuild: bool = false,
    migration_equivalent_schema_rewrite: bool = false,
    migration_equivalent_schema_validation: bool = false,
    to_jsonb_value_wrapper: bool = false,
    to_jsonb_dynamic_expression: bool = false,
    update_source_json_set_expression: bool = false,
    update_joined_source_json_set_expression: bool = false,
    query_substring_expression: bool = false,
    query_overlay_expression: bool = false,
    query_translate_expression: bool = false,
    query_split_part_expression: bool = false,
    query_strpos_expression: bool = false,
    query_left_right_expression: bool = false,
    query_trim_variant_expression: bool = false,
    query_regexp_replace_expression: bool = false,
    query_regexp_substr_expression: bool = false,
    query_regexp_match_expression: bool = false,
    query_regexp_count_expression: bool = false,
    query_regexp_instr_expression: bool = false,
    query_pad_expression: bool = false,
    query_repeat_expression: bool = false,
    query_reverse_expression: bool = false,
    query_initcap_expression: bool = false,
    query_text_length_expression: bool = false,
    query_bit_length_expression: bool = false,
    query_md5_expression: bool = false,
    query_concat_ws_expression: bool = false,
    query_nullif_expression: bool = false,
    query_extremum_expression: bool = false,
    query_nullable_pagination: bool = false,
    query_json_build_object_expression: bool = false,
    query_to_jsonb_expression: bool = false,
    query_convert_from_jsonb_expression: bool = false,
    query_cardinality_expression: bool = false,
    query_array_position_expression: bool = false,
    query_array_positions_expression: bool = false,
    query_array_element_transform_expression: bool = false,
    query_array_to_string_expression: bool = false,
    query_string_to_array_expression: bool = false,
    query_starts_with_expression: bool = false,
    query_ends_with_expression: bool = false,
    query_ascii_chr_expression: bool = false,
    query_modulo_expression: bool = false,
    aggregate_modulo_expression: bool = false,
    aggregate_octet_length_expression: bool = false,
    aggregate_bit_length_expression: bool = false,
    aggregate_scalar_minmax: bool = false,
    aggregate_regexp_numeric_expression: bool = false,
    aggregate_regexp_text_expression: bool = false,
    query_date_trunc_expression: bool = false,
    query_date_bin_expression: bool = false,
    query_typed_datetime_literal_expression: bool = false,
    query_date_part_expression: bool = false,
    query_date_part_epoch_expression: bool = false,
    conflict_date_bin_update: bool = false,
    conflict_typed_datetime_literal_update: bool = false,
    query_nested_case_fold_text_expression: bool = false,
    conflict_nested_text_expression_update: bool = false,
    ddl_create_table: bool = false,
    ddl_inline_named_column_constraints: bool = false,
    ddl_temporal_table: bool = false,
    ddl_replace_table: bool = false,
    ddl_create_index: bool = false,
    ddl_create_covering_index: bool = false,
    ddl_create_covering_generated_index: bool = false,
    ddl_create_covering_gin_index: bool = false,
    ddl_drop_index: bool = false,
    ddl_drop_table: bool = false,
    ddl_drop_table_cascade: bool = false,
    ddl_alter_table: bool = false,
    ddl_add_column_default_rewrite: bool = false,
    ddl_create_update_policy: bool = false,
    ddl_drop_update_policy: bool = false,
    ddl_add_unvalidated_unique: bool = false,
    ddl_add_unvalidated_fk: bool = false,
    ddl_add_unvalidated_check: bool = false,
    ddl_add_deferrable_primary_key: bool = false,
    ddl_add_deferrable_unique_constraint: bool = false,
    ddl_validate_constraint: bool = false,
    ddl_drop_constraint: bool = false,
    ddl_drop_column: bool = false,
    ddl_alter_column_default: bool = false,
    ddl_drop_column_default: bool = false,
    ddl_alter_column_not_null: bool = false,
    ddl_drop_column_not_null: bool = false,
    ddl_alter_column_type: bool = false,
    ddl_alter_column_rewrite_expression: bool = false,
    ddl_rename_column: bool = false,
    ddl_rename_constraint: bool = false,
    adapter_noop_transaction: bool = false,
    adapter_noop_transaction_commit: bool = false,
    adapter_noop_transaction_rollback: bool = false,
    adapter_noop_session: bool = false,
    adapter_noop_session_probe: bool = false,
    adapter_noop_schema_namespace: bool = false,
    adapter_noop_extension: bool = false,
    session_set_search_path: bool = false,
    session_set_search_path_local: bool = false,
    session_set_search_path_multi_namespace: bool = false,
    session_set_app_setting: bool = false,
    session_set_runtime_setting: bool = false,
    session_reset_search_path: bool = false,
    session_reset_app_setting: bool = false,
    session_show_search_path: bool = false,
    session_discard: bool = false,
    query_distinct_on: bool = false,
    query_cte_chain: bool = false,
    query_cte_structured_access: bool = false,
    query_cte_expression_access: bool = false,
    query_set_operation_order_limit: bool = false,
    read_set_operation_order_limit: bool = false,
    set_operation_fetch_tail: bool = false,
    set_operation_null_pagination_tail: bool = false,
    cte_set_operation_tail: bool = false,
    set_operation_numeric_range_disjoint: bool = false,
    set_operation_expression_numeric_range_disjoint: bool = false,
    aggregate_offset: bool = false,
    aggregate_input_expression: bool = false,
    aggregate_percentile_cont: bool = false,
    aggregate_percentile_disc: bool = false,
    aggregate_percentile_desc: bool = false,
    aggregate_percentile_nulls: bool = false,
    aggregate_percentile_array: bool = false,
    aggregate_mode: bool = false,
    aggregate_duplicate_output_label: bool = false,
    aggregate_group_expression: bool = false,
    aggregate_group_expression_alias: bool = false,
    aggregate_having_expression: bool = false,
    aggregate_having_any: bool = false,
    aggregate_boolean_having_predicate: bool = false,
    aggregate_boolean_is_not_having: bool = false,
    aggregate_filter_expression: bool = false,
    aggregate_computed_pattern_filter: bool = false,
    aggregate_filter_groups: bool = false,
    aggregate_boolean_is_not_filter: bool = false,
    aggregate_boolean_unknown_filter: bool = false,
    aggregate_distinct_json_array_expression: bool = false,
    aggregate_distinct_group_projection: bool = false,
    aggregate_cte_expression_access: bool = false,
    join_structured_side_access: bool = false,
    join_on_side_predicate: bool = false,
    join_on_preserved_side_predicate: bool = false,
    join_on_computed_predicate: bool = false,
    join_computed_pattern_side_filter: bool = false,
    join_expression_order: bool = false,
    join_offset: bool = false,
    lateral_structured_side_access: bool = false,
    lateral_computed_pattern_side_filter: bool = false,
    lateral_subquery_match_expression: bool = false,
    lateral_subquery_match_expression_or: bool = false,
    lateral_subquery_function_match_expression_or: bool = false,
    lateral_subquery_match_expression_not: bool = false,
    lateral_subquery_match_expression_array: bool = false,
    lateral_expression_order: bool = false,
    lateral_right_offset: bool = false,
    window_rich_functions: bool = false,
    window_source_membership: bool = false,
    window_mixed_order: bool = false,
    window_expression_order: bool = false,
    window_boolean_aggregate_functions: bool = false,
    window_cte: bool = false,
    window_cte_expression_access: bool = false,
    window_offset: bool = false,
    window_frame_signature: bool = false,
    window_aggregate_filter: bool = false,
    window_computed_pattern_filter: bool = false,
    window_scalar_minmax: bool = false,
    window_modulo_expression: bool = false,
    joined_source_computed_pattern_filter: bool = false,
    parameterized_query: bool = false,
    parameterized_aggregate: bool = false,
    parameterized_join: bool = false,
    parameterized_lateral: bool = false,
    parameterized_window: bool = false,
    parameterized_insert: bool = false,
    parameterized_update: bool = false,
    parameterized_delete: bool = false,
    parameterized_update_source: bool = false,
    parameterized_delete_source: bool = false,
    parameterized_update_joined_source: bool = false,
    parameterized_delete_joined_source: bool = false,
    bound_catalog_object_schema_generation: bool = false,
    bound_catalog_object_table_id: bool = false,
    bound_catalog_read_source: bool = false,
    bound_catalog_read_target: bool = false,
    bound_catalog_write_insert_source: bool = false,
    bound_catalog_write_joined_source: bool = false,
    bound_catalog_write_target: bool = false,

    fn observeMigrationEquivalentDataBackfill(self: *@This(), entry: AppParityCorpusEntry) void {
        self.migration_equivalent_data_backfill_insert =
            self.migration_equivalent_data_backfill_insert or
            (corpusPlanMatchesFamily(.insert_source, entry.plan) and
                planHasStringToken(entry.plan, ":source_table=") and
                planUsizeTokenValue(entry.plan, ":assignments=") != null);
        self.migration_equivalent_data_backfill_update =
            self.migration_equivalent_data_backfill_update or
            ((corpusPlanMatchesFamily(.update_source, entry.plan) and
                planHasNonZeroToken(entry.plan, ":ops=")) or
                (corpusPlanMatchesFamily(.update_joined_source, entry.plan) and
                    planHasStringToken(entry.plan, ":source=") and
                    planHasNonZeroToken(entry.plan, ":on=") and
                    planHasNonZeroToken(entry.plan, ":ops=")));
        self.migration_equivalent_data_backfill_delete =
            self.migration_equivalent_data_backfill_delete or
            ((corpusPlanMatchesFamily(.delete_source, entry.plan) and
                planHasStringToken(entry.plan, ":claim=") and
                planHasStringToken(entry.plan, ":returning=")) or
                (corpusPlanMatchesFamily(.delete_joined_source, entry.plan) and
                    planHasStringToken(entry.plan, ":source=") and
                    planHasNonZeroToken(entry.plan, ":on=") and
                    planHasStringToken(entry.plan, ":returning=")));
    }

    fn observeBoundCatalogLogicalPlan(self: *@This(), plan: binder.LogicalSqlPlan) void {
        switch (plan) {
            .catalog_read => |read| {
                for (read.bound_objects) |object| {
                    self.observeBoundCatalogObjectIdentity(object);
                    switch (object.role) {
                        .target => self.bound_catalog_read_target = true,
                        .source => self.bound_catalog_read_source = true,
                        else => {},
                    }
                }
            },
            .catalog_write => |write| {
                for (write.bound_objects) |object| {
                    self.observeBoundCatalogObjectIdentity(object);
                    switch (object.role) {
                        .target => self.bound_catalog_write_target = true,
                        .insert_source => self.bound_catalog_write_insert_source = true,
                        .joined_source => self.bound_catalog_write_joined_source = true,
                        else => {},
                    }
                }
            },
            else => {},
        }
    }

    fn observeBoundCatalogObjectIdentity(self: *@This(), object: binder.BoundCatalogObject) void {
        self.bound_catalog_object_table_id = self.bound_catalog_object_table_id or object.table_id != 0;
        self.bound_catalog_object_schema_generation = self.bound_catalog_object_schema_generation or object.schema_generation != 0;
    }

    fn observeBoundCatalogFacts(self: *@This(), alloc: std.mem.Allocator, entry: AppParityCorpusEntry, parsed_sql: *const tokenized.ParsedSql) !void {
        var catalog = (try appParityBindingCoverageCatalogForEntryParsedSqlAlloc(alloc, entry, parsed_sql)) orelse return;
        defer catalog.deinit(alloc);
        switch (entry.family) {
            .read, .query, .aggregate, .join, .lateral, .window => {
                var bound = binder.bindReadPlanCatalogStatementAlloc(alloc, parsed_sql, catalog.iface()) catch |err| switch (err) {
                    error.InvalidSqlCatalog, error.TableNotFound, error.UnsupportedSqlShape => return,
                    else => return err,
                };
                defer bound.deinit(alloc);
                var logical = try binder.logicalReadPlanFromBoundStatement(&bound);
                defer logical.deinit(alloc);
                self.observeBoundCatalogLogicalPlan(logical);
            },
            .insert,
            .insert_source,
            .recursive_insert_source,
            .update,
            .delete,
            .update_source,
            .delete_source,
            .truncate_source,
            .update_joined_source,
            .delete_joined_source,
            .merge_mutation,
            => {
                var bound = binder.bindWritePlanCatalogStatementAlloc(alloc, parsed_sql, .{}, catalog.iface()) catch |err| switch (err) {
                    error.InvalidSqlCatalog, error.TableNotFound, error.UnsupportedSqlShape => return,
                    else => return err,
                };
                defer bound.deinit(alloc);
                var logical = try binder.logicalWritePlanFromBoundStatement(&bound);
                defer logical.deinit(alloc);
                self.observeBoundCatalogLogicalPlan(logical);
            },
            else => {},
        }
    }

    pub fn observe(self: *@This(), alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, entry.sql);
        defer parsed_sql.deinit(alloc);
        const sql_tokens = parsed_sql.items();

        const uses_cte_stream = sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") or sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=");
        const uses_returning_all = sql_adapter.planHasNonZeroToken(entry.plan, ":returning_all=");
        const uses_conflict_where = sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_where=");
        const uses_insert_conflict = entry.family == .insert and appParityTokensHaveKeywordSequence(sql_tokens, &.{ .on, .conflict });
        const uses_multi_row_insert = entry.family == .insert and appParityTokensHaveKindSequence(sql_tokens, &.{ .rparen, .comma, .lparen });
        const uses_computed_pattern = appParityParsedSqlHasComputedPattern(&parsed_sql);
        const is_update_joined_source = entry.family == .update_joined_source;
        const is_delete_joined_source = entry.family == .delete_joined_source;
        const is_joined_source = is_update_joined_source or is_delete_joined_source;
        const applied_rebuild = entry.applied_plan.len > 0 and sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", true);
        const applied_validation = entry.applied_plan.len > 0 and sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", true);
        const applied_rewrite = entry.applied_plan.len > 0 and sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", true);
        const setup_summary = try appParitySetupSqlSummaryAlloc(alloc, entry.apply_setup_sql);
        try self.observeBoundCatalogFacts(alloc, entry, &parsed_sql);
        if (entry.params.len > 0) {
            switch (entry.family) {
                .query => self.parameterized_query = true,
                .aggregate => self.parameterized_aggregate = true,
                .join => self.parameterized_join = true,
                .lateral => self.parameterized_lateral = true,
                .window => self.parameterized_window = true,
                .insert => self.parameterized_insert = true,
                .update => self.parameterized_update = true,
                .delete => self.parameterized_delete = true,
                .update_source => self.parameterized_update_source = true,
                .delete_source => self.parameterized_delete_source = true,
                .update_joined_source => self.parameterized_update_joined_source = true,
                .delete_joined_source => self.parameterized_delete_joined_source = true,
                else => {},
            }
        }
        if (entry.family == .query_function) {
            self.query_function = true;
            self.query_function_full_text = self.query_function_full_text or
                sql_adapter.planHasNonZeroToken(entry.plan, ":text=");
            self.query_function_semantic = self.query_function_semantic or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.semantic_search") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":dense="));
            self.query_function_vector = self.query_function_vector or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.vector_search") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":dense="));
            self.query_function_graph_search = self.query_function_graph_search or
                sql_adapter.planHasNonZeroToken(entry.plan, ":graph_search=");
            self.query_function_graph_traverse = self.query_function_graph_traverse or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.graph_traverse") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":graph_search="));
            self.query_function_graph_shortest_path = self.query_function_graph_shortest_path or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.graph_shortest_path") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":graph_search="));
            self.query_function_graph_k_shortest_paths = self.query_function_graph_k_shortest_paths or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.graph_k_shortest_paths") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":graph_search="));
            self.query_function_graph_metric = self.query_function_graph_metric or
                sql_adapter.planHasNonZeroToken(entry.plan, ":graph_metric=");
            self.query_function_graph_metric_rerank = self.query_function_graph_metric_rerank or
                sql_adapter.planHasNonZeroToken(entry.plan, ":graph_metric_rerank=");
            self.query_function_hybrid = self.query_function_hybrid or
                (sql_adapter.planHasNonZeroToken(entry.plan, ":merge=") and
                    (sql_adapter.planHasNonZeroToken(entry.plan, ":text=") or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":dense=") or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":graph_metric_rerank=")));
            self.query_function_hybrid_sources_json = self.query_function_hybrid_sources_json or
                (appParityTokensHaveIdentifier(sql_tokens, "sources_json") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":merge="));
            self.query_function_hybrid_source_helpers = self.query_function_hybrid_source_helpers or
                (appParityTokensHaveIdentifier(sql_tokens, "antfly.source") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":merge="));
        }
        self.to_jsonb_value_wrapper = self.to_jsonb_value_wrapper or appParityTokensHaveIdentifier(sql_tokens, "to_jsonb");
        self.to_jsonb_dynamic_expression = self.to_jsonb_dynamic_expression or
            (appParityTokensHaveIdentifier(sql_tokens, "to_jsonb") and
                (appParityTokensHaveIdentifier(sql_tokens, "lower") or appParityTokensHaveIdentifierPrefix(sql_tokens, "excluded.")));
        self.update_source_json_set_expression = self.update_source_json_set_expression or
            (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":json_set_expr="));
        self.update_joined_source_json_set_expression = self.update_joined_source_json_set_expression or
            (entry.family == .update_joined_source and sql_adapter.planHasNonZeroToken(entry.plan, ":json_set_expr="));
        self.point_update_jsonb = self.point_update_jsonb or (entry.family == .update and appParityTokensHaveIdentifierPrefix(sql_tokens, "jsonb_"));
        self.point_update_jsonb_concat = self.point_update_jsonb_concat or (entry.family == .update and
            appParityTokensHaveIdentifier(sql_tokens, "metadata") and
            appParityTokensHaveKind(sql_tokens, .pipe_concat) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.point_update_array = self.point_update_array or (entry.family == .update and appParityTokensHaveIdentifierPrefix(sql_tokens, "array_"));
        self.write_plan_insert_op_set = self.write_plan_insert_op_set or (entry.family == .insert and sql_adapter.planHasNonZeroToken(entry.plan, ":op_set="));
        self.write_plan_insert_op_inc = self.write_plan_insert_op_inc or (entry.family == .insert and sql_adapter.planHasNonZeroToken(entry.plan, ":op_inc="));
        self.write_plan_update_op_set = self.write_plan_update_op_set or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_set="));
        self.write_plan_update_op_push = self.write_plan_update_op_push or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_push="));
        self.write_plan_update_op_pull = self.write_plan_update_op_pull or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_pull="));
        self.point_update_uuid_generation = self.point_update_uuid_generation or (entry.family == .update and appParityTokensHaveIdentifier(sql_tokens, "gen_random_uuid"));
        self.point_update_patch_expression = self.point_update_patch_expression or
            (entry.family == .update and
                planHasExactStringToken(entry.plan, "update:table=", "usage_records") and
                planHasNonZeroToken(entry.plan, ":op_set=") and
                appParityTokensHaveSetFunctionAssignment(sql_tokens, "status", "lower"));
        self.update_source_claim_skip_locked = self.update_source_claim_skip_locked or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "skip_locked", "no_key_update_skip_locked" }));
        self.update_source_claim_nowait = self.update_source_claim_nowait or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "nowait", "no_key_update_nowait" }));
        self.update_source_claim_no_key_update = self.update_source_claim_no_key_update or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "no_key_update", "no_key_update_nowait", "no_key_update_skip_locked" }));
        self.update_source_pagination = self.update_source_pagination or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":source_offset="));
        self.update_source_nullable_pagination = self.update_source_nullable_pagination or (entry.family == .update_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .limit, .null, .offset, .null }) and
            sql_adapter.planHasExactStringToken(entry.plan, ":source_limit=", "-1") and
            sql_adapter.planTokenAbsent(entry.plan, ":source_offset="));
        self.update_source_returning_expression = self.update_source_returning_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.schema_temporal_numrange_insert = self.schema_temporal_numrange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "[1,10)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "price_intervals") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_daterange_insert = self.schema_temporal_daterange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "[2025-01-01,2025-07-01)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_open_daterange_insert = self.schema_temporal_open_daterange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "[2026-01-01,)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_open_daterange_insert = self.schema_temporal_lower_open_daterange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "(,2026-01-01)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_numrange_constructor_insert = self.schema_temporal_numrange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "numrange") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "price_intervals") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_daterange_constructor_insert = self.schema_temporal_daterange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "daterange") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_inclusive_daterange_constructor_insert = self.schema_temporal_inclusive_daterange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "daterange") and
            appParityTokensHaveStringLiteral(sql_tokens, "[]") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_inclusive_daterange_literal_insert = self.schema_temporal_inclusive_daterange_literal_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "2025-02-01]") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_exclusive_daterange_constructor_insert = self.schema_temporal_lower_exclusive_daterange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "daterange") and
            appParityTokensHaveStringLiteral(sql_tokens, "(]") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_exclusive_daterange_literal_insert = self.schema_temporal_lower_exclusive_daterange_literal_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "(2025-01-01,") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tsrange_insert = self.schema_temporal_tsrange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "[2025-01-01 00:00:00,2025-01-02 00:00:00)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "local_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tsrange_constructor_insert = self.schema_temporal_tsrange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "tsrange") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "local_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tstzrange_insert = self.schema_temporal_tstzrange_insert or (entry.family == .insert and
            appParityTokensHaveStringLiteralContaining(sql_tokens, "[2025-01-01T01:30:00+01:30,2025-01-02T00:00:00Z)") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "published_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tstzrange_constructor_insert = self.schema_temporal_tstzrange_constructor_insert or (entry.family == .insert and
            appParityTokensHaveFunctionCall(sql_tokens, "tstzrange") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "published_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_bound_query = self.schema_temporal_range_bound_query or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveFunctionCall(sql_tokens, "upper") and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.schema_temporal_range_contains_query = self.schema_temporal_range_contains_query or (entry.family == .query and
            appParityTokensHaveKind(sql_tokens, .at_contains) and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_range_overlap_query = self.schema_temporal_range_overlap_query or (entry.family == .query and
            appParityTokensHaveKind(sql_tokens, .range_overlap) and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_inclusive_daterange_overlap_query = self.schema_temporal_inclusive_daterange_overlap_query or (entry.family == .query and
            appParityTokensHaveKind(sql_tokens, .range_overlap) and
            appParityTokensHaveFunctionCall(sql_tokens, "daterange") and
            appParityTokensHaveStringLiteral(sql_tokens, "[]") and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_unique_conflict_upsert = self.schema_temporal_unique_conflict_upsert or (entry.family == .insert and
            appParityTokensHaveConflictConstraint(sql_tokens, "prices_sku_time_key") and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "prices") and
            sql_adapter.planHasExactUsizeToken(entry.plan, ":transforms=", 1) and
            entry.apply_setup_sql.len > 0 and
            entry.resolver_row_json.len > 0);
        self.query_set_operation_order_limit = self.query_set_operation_order_limit or (entry.family == .query and
            appParityTokensHaveKeyword(sql_tokens, .@"union") and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .order, .by }) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order=") and
            sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "5"));
        self.read_set_operation_order_limit = self.read_set_operation_order_limit or (entry.family == .read and
            (appParityTokensHaveKeyword(sql_tokens, .intersect) or
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"union", .all })) and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .order, .by }) and
            (sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") or sql_adapter.readPlanHasKind(entry.plan, "set_operation")) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":result_order=") and
            sql_adapter.planHasExactStringToken(entry.plan, ":result_limit=", "5"));
        self.set_operation_fetch_tail = self.set_operation_fetch_tail or
            ((entry.family == .query or entry.family == .read) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"union", .all }) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .fetch, .first }) and
                (sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "1") or
                    sql_adapter.planHasExactStringToken(entry.plan, ":result_limit=", "1")));
        self.set_operation_null_pagination_tail = self.set_operation_null_pagination_tail or
            ((entry.family == .query or entry.family == .read) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"union", .all }) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .limit, .null, .offset, .null }) and
                sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "none") and
                sql_adapter.planTokenAbsent(entry.plan, ":offset="));
        self.cte_set_operation_tail = self.cte_set_operation_tail or
            ((entry.family == .query or entry.family == .read) and
                appParityTokensStartWithKeyword(sql_tokens, .with) and
                appParityTokensHaveIdentifier(sql_tokens, "scoped") and
                appParityTokensHaveKeyword(sql_tokens, .@"union") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.set_operation_numeric_range_disjoint = self.set_operation_numeric_range_disjoint or
            ((entry.family == .query or entry.family == .read) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"union", .all }) and
                appParityTokensHaveIdentifier(sql_tokens, "amount") and
                (appParityTokensHaveKind(sql_tokens, .lt) or appParityTokensHaveKind(sql_tokens, .lte)) and
                (appParityTokensHaveKind(sql_tokens, .gt) or appParityTokensHaveKind(sql_tokens, .gte)) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.set_operation_expression_numeric_range_disjoint = self.set_operation_expression_numeric_range_disjoint or
            ((entry.family == .query or entry.family == .read) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"union", .all }) and
                appParityTokensHaveIdentifier(sql_tokens, "amount") and
                appParityTokensHaveIdentifier(sql_tokens, "quantity") and
                appParityTokensHaveKind(sql_tokens, .plus) and
                (appParityTokensHaveKind(sql_tokens, .lt) or appParityTokensHaveKind(sql_tokens, .lte)) and
                (appParityTokensHaveKind(sql_tokens, .gt) or appParityTokensHaveKind(sql_tokens, .gte)) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or="));
        self.schema_temporal_fk_ddl = self.schema_temporal_fk_ddl or (entry.family == .ddl and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal_fk=") and
            appParityTokensHaveKeyword(sql_tokens, .period) and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .foreign, .key }));
        self.schema_system_versioned_table = self.schema_system_versioned_table or (entry.family == .ddl and
            sql_adapter.planHasExactUsizeToken(entry.plan, ":system_versioned=", 1) and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .system, .versioning }));
        self.schema_nulls_not_distinct_unique = self.schema_nulls_not_distinct_unique or (entry.family == .ddl and
            appParityTokensHaveKeyword(sql_tokens, .unique) and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .nulls, .not, .distinct }));
        self.schema_rich_expression_secondary_index = self.schema_rich_expression_secondary_index or (entry.family == .ddl and
            entry.summary.ddl_tag == .create_index and
            sql_adapter.planHasExactStringToken(entry.plan, ":generated_op=", "expression"));
        self.schema_temporal_portion_update = self.schema_temporal_portion_update or (entry.family == .update_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .portion, .of }) and
            sql_adapter.planHasExactStringToken(entry.plan, "update_source:table=", "prices") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_portion_delete = self.schema_temporal_portion_delete or (entry.family == .delete_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .portion, .of }) and
            sql_adapter.planHasExactStringToken(entry.plan, "delete_source:table=", "prices") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_column_portion_update = self.schema_temporal_range_column_portion_update or (entry.family == .update_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .portion, .of }) and
            appParityTokensHaveIdentifier(sql_tokens, "valid_at") and
            sql_adapter.planHasExactStringToken(entry.plan, "update_source:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_column_portion_delete = self.schema_temporal_range_column_portion_delete or (entry.family == .delete_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .portion, .of }) and
            appParityTokensHaveIdentifier(sql_tokens, "valid_at") and
            sql_adapter.planHasExactStringToken(entry.plan, "delete_source:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.update_source_row_assignment = self.update_source_row_assignment or (entry.family == .update_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_row_assignment_default = self.update_source_row_assignment_default or (entry.family == .update_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            appParityTokensHaveKeyword(sql_tokens, .default) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_row_assignment_constructor = self.update_source_row_assignment_constructor or (entry.family == .update_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            appParityTokensHaveFunctionCall(sql_tokens, "row") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_boolean_is_not_predicate = self.update_source_boolean_is_not_predicate or (entry.family == .update_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_or=") and
            (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .true }) or
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .false })));
        self.delete_source_fetch_pagination = self.delete_source_fetch_pagination or (entry.family == .delete_source and
            appParityTokensHaveKeyword(sql_tokens, .fetch) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_offset="));
        self.delete_source_nullable_pagination = self.delete_source_nullable_pagination or (entry.family == .delete_source and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .limit, .null, .offset, .null }) and
            sql_adapter.planHasExactStringToken(entry.plan, ":source_limit=", "-1") and
            sql_adapter.planTokenAbsent(entry.plan, ":source_offset="));
        self.delete_source_boolean_unknown_predicate = self.delete_source_boolean_unknown_predicate or (entry.family == .delete_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_pred=") and
            (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .unknown }) or
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .unknown })));
        self.delete_source_returning_expression = self.delete_source_returning_expression or (entry.family == .delete_source and sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.joined_source_ordered_pagination = self.joined_source_ordered_pagination or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":limit=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":offset="));
        self.joined_source_expression_predicate = self.joined_source_expression_predicate or (is_joined_source and
            (sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_or=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_not=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_array=")));
        self.joined_source_computed_pattern_filter = self.joined_source_computed_pattern_filter or (is_joined_source and
            uses_computed_pattern and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred="));
        self.joined_source_expression_group = self.joined_source_expression_group or (is_joined_source and
            (sql_adapter.planHasNonZeroToken(entry.plan, "_expr_or=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_not=")));
        self.joined_source_expression_array = self.joined_source_expression_array or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_array="));
        self.joined_source_returning_expression = self.joined_source_returning_expression or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.joined_source_returning_source_field = self.joined_source_returning_source_field or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            appParityTokensHaveKeyword(sql_tokens, .returning) and
            appParityTokensHaveIdentifierPrefix(sql_tokens, "source."));
        self.joined_source_returning_source_expression = self.joined_source_returning_source_expression or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            appParityTokensHaveKeyword(sql_tokens, .returning) and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifierPrefix(sql_tokens, "source."));
        self.update_joined_source_returning_source_expression = self.update_joined_source_returning_source_expression or (entry.family == .update_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            appParityTokensHaveKeyword(sql_tokens, .returning) and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifierPrefix(sql_tokens, "source."));
        self.delete_joined_source_returning_source_expression = self.delete_joined_source_returning_source_expression or (entry.family == .delete_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            appParityTokensHaveKeyword(sql_tokens, .returning) and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifierPrefix(sql_tokens, "source."));
        self.update_joined_source_non_primary_semijoin = self.update_joined_source_non_primary_semijoin or (is_update_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .in, .select, .from }) and
            appParityTokensHaveIdentifier(sql_tokens, "organization_id") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 1));
        self.delete_joined_source_non_primary_semijoin = self.delete_joined_source_non_primary_semijoin or (is_delete_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .in, .select, .from }) and
            appParityTokensHaveIdentifier(sql_tokens, "organization_id") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 1));
        self.update_joined_source_correlated_semijoin = self.update_joined_source_correlated_semijoin or (is_update_joined_source and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.delete_joined_source_correlated_semijoin = self.delete_joined_source_correlated_semijoin or (is_delete_joined_source and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.update_joined_source_correlated_filtered_semijoin = self.update_joined_source_correlated_filtered_semijoin or (is_update_joined_source and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.organization_id") and
            appParityTokensHaveStringLiteral(sql_tokens, "o1") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 2));
        self.delete_joined_source_correlated_filtered_semijoin = self.delete_joined_source_correlated_filtered_semijoin or (is_delete_joined_source and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.organization_id") and
            appParityTokensHaveStringLiteral(sql_tokens, "o1") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 2));
        self.update_joined_source_semijoin_match_expression = self.update_joined_source_semijoin_match_expression or (is_update_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.delete_joined_source_semijoin_match_expression = self.delete_joined_source_semijoin_match_expression or (is_delete_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.update_joined_source_exists_semijoin = self.update_joined_source_exists_semijoin or (is_update_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .where, .exists }) and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.organization_id") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.id") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 1));
        self.delete_joined_source_exists_semijoin = self.delete_joined_source_exists_semijoin or (is_delete_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .where, .exists }) and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.organization_id") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.id") and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 1));
        self.update_joined_source_exists_match_expression = self.update_joined_source_exists_match_expression or (is_update_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .where, .exists }) and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.delete_joined_source_exists_match_expression = self.delete_joined_source_exists_match_expression or (is_delete_joined_source and
            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .where, .exists }) and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveIdentifier(sql_tokens, "archived_records.status") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.status") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.update_joined_source_row_value_semijoin = self.update_joined_source_row_value_semijoin or (is_update_joined_source and
            appParityTokensHaveKeyword(sql_tokens, .where) and
            appParityTokensHaveKindSequence(sql_tokens, &.{ .lparen, .identifier, .comma, .identifier, .rparen }) and
            appParityTokensHaveKeyword(sql_tokens, .in) and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.delete_joined_source_row_value_semijoin = self.delete_joined_source_row_value_semijoin or (is_delete_joined_source and
            appParityTokensHaveKeyword(sql_tokens, .where) and
            appParityTokensHaveKindSequence(sql_tokens, &.{ .lparen, .identifier, .comma, .identifier, .rparen }) and
            appParityTokensHaveKeyword(sql_tokens, .in) and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.update_joined_source_modulo_expression = self.update_joined_source_modulo_expression or (is_update_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "mod") and
            appParityTokensHaveKind(sql_tokens, .percent) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_regexp_expression = self.update_joined_source_regexp_expression or (is_update_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_like") and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_regexp_expression = self.delete_joined_source_regexp_expression or (is_delete_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_like") and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_array_expression = self.update_joined_source_array_expression or (is_update_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "array_append") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_position") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_to_string") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_array_expression = self.delete_joined_source_array_expression or (is_delete_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "array_position") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_to_string") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_json_expression = self.update_joined_source_json_expression or (is_update_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "jsonb_build_object") and
            appParityTokensHaveStringLiteral(sql_tokens, "status") and
            appParityTokensHaveFunctionCall(sql_tokens, "to_jsonb") and
            appParityTokensHaveFunctionCall(sql_tokens, "jsonb_extract_path_text") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_json_expression = self.delete_joined_source_json_expression or (is_delete_joined_source and
            appParityTokensHaveFunctionCall(sql_tokens, "jsonb_build_object") and
            appParityTokensHaveStringLiteral(sql_tokens, "source") and
            appParityTokensHaveFunctionCall(sql_tokens, "to_jsonb") and
            appParityTokensHaveFunctionCall(sql_tokens, "jsonb_extract_path_text") and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_row_assignment = self.update_joined_source_row_assignment or (is_update_joined_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_row_assignment_default = self.update_joined_source_row_assignment_default or (is_update_joined_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            appParityTokensHaveKeyword(sql_tokens, .default) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_row_assignment_constructor = self.update_joined_source_row_assignment_constructor or (is_update_joined_source and
            appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
            appParityTokensHaveFunctionCall(sql_tokens, "row") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_boolean_expression_update = self.update_joined_source_boolean_expression_update or (is_update_joined_source and
            appParityTokensHaveKeyword(sql_tokens, .set) and
            appParityTokensHaveIdentifier(sql_tokens, "enabled") and
            appParityTokensHaveIdentifier(sql_tokens, "usage_records.enabled") and
            appParityTokensHaveIdentifier(sql_tokens, "source.enabled") and
            appParityTokensHaveKeyword(sql_tokens, .@"or") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_patch_expression = self.update_source_patch_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_boolean_expression_update = self.update_source_boolean_expression_update or (entry.family == .update_source and
            appParityTokensHaveKeyword(sql_tokens, .set) and
            appParityTokensHaveIdentifier(sql_tokens, "enabled") and
            appParityTokensHaveKeyword(sql_tokens, .@"or") and
            appParityTokensHaveKeyword(sql_tokens, .false) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_increment_expression = self.update_source_increment_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":increment_expr="));
        self.update_source_modulo_expression = self.update_source_modulo_expression or (entry.family == .update_source and
            appParityTokensHaveFunctionCall(sql_tokens, "mod") and
            appParityTokensHaveKind(sql_tokens, .percent) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_source_regexp_replace_expression = self.update_source_regexp_replace_expression or (entry.family == .update_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_replace") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_match_expression = self.update_source_regexp_match_expression or (entry.family == .update_source and
            (appParityTokensHaveFunctionCall(sql_tokens, "regexp_like") or
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_match")) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_count_expression = self.update_source_regexp_count_expression or (entry.family == .update_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_instr_expression = self.update_source_regexp_instr_expression or (entry.family == .update_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_substr_expression = self.update_source_regexp_substr_expression or (entry.family == .update_source and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.observeMigrationEquivalentDataBackfill(entry);
        self.catalog_setup_sql = self.catalog_setup_sql or entry.apply_setup_sql.len > 0;
        self.catalog_tables_fixture_metadata = self.catalog_tables_fixture_metadata or entry.catalog_tables.len > 0;
        if (entry.applied_plan.len > 0) {
            self.applied_catalog_plan = true;
            self.applied_catalog_rebuild = self.applied_catalog_rebuild or applied_rebuild;
            self.applied_catalog_validation = self.applied_catalog_validation or applied_validation;
            self.applied_catalog_rewrite = self.applied_catalog_rewrite or applied_rewrite;
            if (entry.family == .ddl) {
                self.migration_equivalent_schema_metadata = self.migration_equivalent_schema_metadata or (!applied_rebuild and !applied_validation and !applied_rewrite);
                self.migration_equivalent_schema_rebuild = self.migration_equivalent_schema_rebuild or applied_rebuild;
                self.migration_equivalent_schema_validation = self.migration_equivalent_schema_validation or applied_validation;
                self.migration_equivalent_schema_rewrite = self.migration_equivalent_schema_rewrite or applied_rewrite;
            }
        }
        switch (entry.family) {
            .ddl => self.ddl = true,
            .query_function => {},
            .query => {
                self.query = true;
                self.cte_query = self.cte_query or uses_cte_stream;
                self.query_distinct_on = self.query_distinct_on or sql_adapter.planHasNonZeroToken(entry.plan, ":distinct_on=");
                self.query_cte_chain = self.query_cte_chain or sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 2);
                self.query_cte_structured_access = self.query_cte_structured_access or
                    sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_");
                self.query_cte_expression_access = self.query_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_array="))));
                self.query_computed_pattern_predicate = self.query_computed_pattern_predicate or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=");
            },
            .aggregate => {
                self.aggregate = true;
                self.cte_aggregate = self.cte_aggregate or uses_cte_stream;
                self.aggregate_offset = self.aggregate_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
                self.aggregate_input_expression = self.aggregate_input_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr=");
                self.aggregate_modulo_expression = self.aggregate_modulo_expression or (appParityTokensHaveFunctionCall(sql_tokens, "sum") and
                    appParityTokensHaveFunctionCall(sql_tokens, "mod") and
                    appParityTokensHaveKind(sql_tokens, .percent) and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_octet_length_expression = self.aggregate_octet_length_expression or (appParityTokensHaveFunctionCall(sql_tokens, "octet_length") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_bit_length_expression = self.aggregate_bit_length_expression or (appParityTokensHaveFunctionCall(sql_tokens, "bit_length") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_scalar_minmax = self.aggregate_scalar_minmax or (appParityTokensHaveFunctionCall(sql_tokens, "min") and
                    appParityTokensHaveFunctionCall(sql_tokens, "max") and
                    appParityTokensHaveFunctionCall(sql_tokens, "lower") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_regexp_numeric_expression = self.aggregate_regexp_numeric_expression or (appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
                    appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_regexp_text_expression = self.aggregate_regexp_text_expression or (appParityTokensHaveFunctionCallWithKeyword(sql_tokens, "count", .distinct) and
                    appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_percentile_cont = self.aggregate_percentile_cont or
                    appParityTokensHaveFunctionCall(sql_tokens, "percentile_cont");
                self.aggregate_percentile_disc = self.aggregate_percentile_disc or
                    appParityTokensHaveFunctionCall(sql_tokens, "percentile_disc");
                self.aggregate_percentile_desc = self.aggregate_percentile_desc or
                    (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .within, .group }) and
                        appParityTokensHaveKeyword(sql_tokens, .desc));
                self.aggregate_percentile_nulls = self.aggregate_percentile_nulls or
                    (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .within, .group }) and
                        appParityTokensHaveIdentifier(sql_tokens, "nulls"));
                self.aggregate_percentile_array = self.aggregate_percentile_array or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":percentile_array=");
                self.aggregate_mode = self.aggregate_mode or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":mode=");
                self.aggregate_duplicate_output_label = self.aggregate_duplicate_output_label or
                    (appParityTokensHaveFunctionCall(sql_tokens, "count") and
                        appParityTokensHaveKeyword(sql_tokens, .as) and
                        appParityTokensHaveIdentifier(sql_tokens, "customer_id") and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":group=", 1) and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":aggs=", 1));
                self.aggregate_group_expression = self.aggregate_group_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":group_expr=");
                self.aggregate_group_expression_alias = self.aggregate_group_expression_alias or (sql_adapter.planHasNonZeroToken(entry.plan, ":group_expr=") and
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .group, .by }) and
                    appParityTokensHaveIdentifier(sql_tokens, "status_key"));
                self.aggregate_having_expression = self.aggregate_having_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":having_expr=");
                self.aggregate_having_any = self.aggregate_having_any or sql_adapter.planHasNonZeroToken(entry.plan, ":having_any=");
                self.aggregate_boolean_having_predicate = self.aggregate_boolean_having_predicate or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":having=") and
                        (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .true }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .false }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .unknown }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .not, .unknown }));
                self.aggregate_boolean_is_not_having = self.aggregate_boolean_is_not_having or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":having_any=") and
                        (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .not, .true }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .having, .is, .not, .false }));
                self.aggregate_filter_expression = self.aggregate_filter_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":filter_expr=");
                self.aggregate_computed_pattern_filter = self.aggregate_computed_pattern_filter or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":filter_expr=");
                self.aggregate_filter_groups = self.aggregate_filter_groups or sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=");
                self.aggregate_boolean_is_not_filter = self.aggregate_boolean_is_not_filter or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=") and
                        (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .filter, .where, .is, .not, .true }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .filter, .where, .is, .not, .false }));
                self.aggregate_boolean_unknown_filter = self.aggregate_boolean_unknown_filter or
                    (sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=") or sql_adapter.planHasNonZeroToken(entry.plan, ":aggs=")) and
                        (appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .filter, .where, .is, .unknown }) or
                            appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .filter, .where, .is, .not, .unknown }));
                self.aggregate_distinct_json_array_expression = self.aggregate_distinct_json_array_expression or
                    appParityTokensHaveFunctionCallWithKeyword(sql_tokens, "array_agg", .distinct) and
                        appParityTokensHaveKind(sql_tokens, .arrow_json) and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr=");
                self.aggregate_distinct_group_projection = self.aggregate_distinct_group_projection or
                    (sql_adapter.planHasNonZeroToken(entry.plan, ":group=") and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":aggs=", 0));
                self.aggregate_cte_expression_access = self.aggregate_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array="))));
            },
            .join => {
                self.join = true;
                self.join_structured_side_access = self.join_structured_side_access or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":left_json_contains=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":right_json_exists=");
                self.join_on_side_predicate = self.join_on_side_predicate or
                    appParityTokensHaveIdentifier(sql_tokens, "o.customer_id") and
                        appParityTokensHaveIdentifier(sql_tokens, "c.id") and
                        appParityTokensHaveIdentifier(sql_tokens, "c.kind") and
                        appParityTokensHaveStringLiteral(sql_tokens, "customer");
                self.join_on_preserved_side_predicate = self.join_on_preserved_side_predicate or
                    appParityTokensHaveIdentifier(sql_tokens, "o.customer_id") and
                        appParityTokensHaveIdentifier(sql_tokens, "c.id") and
                        appParityTokensHaveIdentifier(sql_tokens, "o.kind") and
                        appParityTokensHaveStringLiteral(sql_tokens, "order") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":on_expr_pred=");
                self.join_on_computed_predicate = self.join_on_computed_predicate or
                    appParityTokensHaveIdentifier(sql_tokens, "o.customer_id") and
                        appParityTokensHaveIdentifier(sql_tokens, "c.id") and
                        appParityTokensHaveFunctionCall(sql_tokens, "lower") and
                        appParityTokensHaveIdentifier(sql_tokens, "o.kind") and
                        appParityTokensHaveIdentifier(sql_tokens, "c.kind") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":on_expr_pred=");
                self.join_computed_pattern_side_filter = self.join_computed_pattern_side_filter or
                    uses_computed_pattern and
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":left_expr_pred=") or
                            sql_adapter.planHasNonZeroToken(entry.plan, ":right_expr_pred="));
                self.join_expression_order = self.join_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.join_offset = self.join_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
            },
            .lateral => {
                self.lateral = true;
                self.lateral_structured_side_access = self.lateral_structured_side_access or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":left_json_contains=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":right_json_exists=");
                self.lateral_computed_pattern_side_filter = self.lateral_computed_pattern_side_filter or
                    uses_computed_pattern and
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":left_expr_pred=") or
                            sql_adapter.planHasNonZeroToken(entry.plan, ":right_expr_pred="));
                self.lateral_subquery_match_expression = self.lateral_subquery_match_expression or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred=");
                self.lateral_subquery_match_expression_or = self.lateral_subquery_match_expression_or or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_or=");
                self.lateral_subquery_function_match_expression_or = self.lateral_subquery_function_match_expression_or or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_or_lower=");
                self.lateral_subquery_match_expression_not = self.lateral_subquery_match_expression_not or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_not=");
                self.lateral_subquery_match_expression_array = self.lateral_subquery_match_expression_array or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_array=");
                self.lateral_expression_order = self.lateral_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.lateral_right_offset = self.lateral_right_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":right_offset=");
            },
            .window => {
                self.window = true;
                self.cte_window = self.cte_window or uses_cte_stream;
                self.window_rich_functions = self.window_rich_functions or sql_adapter.planHasNonZeroToken(entry.plan, ":windows=");
                self.window_source_membership = self.window_source_membership or sql_adapter.planHasNonZeroToken(entry.plan, ":source_in=");
                self.window_mixed_order = self.window_mixed_order or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":windows=");
                self.window_expression_order = self.window_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.window_modulo_expression = self.window_modulo_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":window_expr_mod=");
                self.window_scalar_minmax = self.window_scalar_minmax or sql_adapter.planHasNonZeroToken(entry.plan, ":window_scalar_minmax=");
                self.window_boolean_aggregate_functions = self.window_boolean_aggregate_functions or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_bool_agg=") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter=") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr=");
                self.window_cte = self.window_cte or uses_cte_stream;
                self.window_cte_expression_access = self.window_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array="))));
                self.window_offset = self.window_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
                self.window_frame_signature = self.window_frame_signature or sql_adapter.planHasNonZeroToken(entry.plan, ":window_frame_sig=");
                self.window_aggregate_filter = self.window_aggregate_filter or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_access=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_groups=");
                self.window_computed_pattern_filter = self.window_computed_pattern_filter or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr=");
            },
            .explain => {
                self.explain = true;
                self.explain_write = self.explain_write or corpusExplainSubjectMatches(entry, "write");
                self.explain_options = self.explain_options or corpusExplainOptionsEnabled(entry);
                self.explain_analyze = self.explain_analyze or corpusExplainBoolValue(entry, entry.summary.explain_analyze, ":analyze=", false);
                self.explain_buffers = self.explain_buffers or corpusExplainBoolValue(entry, entry.summary.explain_buffers, ":buffers=", false);
                self.explain_timing_disabled = self.explain_timing_disabled or !corpusExplainBoolValue(entry, entry.summary.explain_timing, ":timing=", true);
                self.explain_summary_disabled = self.explain_summary_disabled or !corpusExplainBoolValue(entry, entry.summary.explain_summary, ":summary=", true);
                self.explain_settings = self.explain_settings or corpusExplainBoolValue(entry, entry.summary.explain_settings, ":settings=", false);
                self.explain_wal = self.explain_wal or corpusExplainBoolValue(entry, entry.summary.explain_wal, ":wal=", false);
            },
            .relation_population => {
                self.relation_population_select_into = self.relation_population_select_into or
                    sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "select_into");
                self.relation_population_select_into_temporary = self.relation_population_select_into_temporary or
                    (sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "select_into") and
                        sql_adapter.planHasExactStringToken(entry.plan, ":lifetime=", "temporary"));
                self.relation_population_select_into_unlogged = self.relation_population_select_into_unlogged or
                    (sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "select_into") and
                        sql_adapter.planHasExactStringToken(entry.plan, ":lifetime=", "unlogged"));
                self.relation_population_create_table_as = self.relation_population_create_table_as or
                    sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "create_table_as");
                self.relation_population_create_table_as_no_data = self.relation_population_create_table_as_no_data or
                    (sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "create_table_as") and
                        sql_adapter.planHasExactBoolToken(entry.plan, ":populate=", false));
            },
            .insert => self.insert = true,
            .insert_source => self.insert_source = true,
            .recursive_insert_source => self.recursive_insert_source = true,
            .update => {
                self.update = true;
                self.update_identity_rewrite = self.update_identity_rewrite or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":identity_rewrites=");
            },
            .delete => self.delete = true,
            .update_source => self.update_source = true,
            .delete_source => self.delete_source = true,
            .truncate_source => self.truncate_source = true,
            .update_joined_source => {
                self.update_joined_source = true;
                self.update_joined_source_cte_mutation = self.update_joined_source_cte_mutation or
                    (appParityTokensStartWithKeyword(sql_tokens, .with) and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 1));
            },
            .delete_joined_source => {
                self.delete_joined_source = true;
                self.delete_joined_source_cte_mutation = self.delete_joined_source_cte_mutation or
                    (appParityTokensStartWithKeyword(sql_tokens, .with) and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 1));
            },
            .merge_mutation => {
                self.merge_mutation_typed_plan = self.merge_mutation_typed_plan or sql_adapter.mergePlanIsTyped(entry.plan);
                const recursive_merge = sql_adapter.mergePlanIsRecursive(entry.plan);
                self.merge_mutation_cte = self.merge_mutation_cte or
                    recursive_merge or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=");
                self.merge_mutation_data_modifying_cte = self.merge_mutation_data_modifying_cte or
                    sql_adapter.planHasExactUsizeToken(entry.plan, ":data_ctes=", 1) and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":data_cte_update=", 1) and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":data_cte_returning=");
            },
            .adapter_noop_ddl => self.adapter_noop_ddl = true,
            .invalid_read => {
                self.invalid_read_row_lock_target = self.invalid_read_row_lock_target or
                    (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .update, .of }) and
                        appParityTokensHaveIdentifier(sql_tokens, "archived_records"));
            },
            .invalid_insert => {
                self.invalid_insert = true;
                self.invalid_duplicate_row_batch_target = self.invalid_duplicate_row_batch_target or std.mem.eql(u8, entry.classification_reason, "duplicate_row_batch_target");
                self.invalid_duplicate_conflict_update_target = self.invalid_duplicate_conflict_update_target or std.mem.eql(u8, entry.classification_reason, "duplicate_conflict_update_target");
                self.invalid_expression_conflict_target = self.invalid_expression_conflict_target or std.mem.eql(u8, entry.classification_reason, "invalid_expression_conflict_target");
                self.invalid_named_conflict_target = self.invalid_named_conflict_target or std.mem.eql(u8, entry.classification_reason, "invalid_named_conflict_target");
            },
            .invalid_update => {
                self.invalid_update = true;
                self.invalid_duplicate_update_target = self.invalid_duplicate_update_target or std.mem.eql(u8, entry.classification_reason, "duplicate_update_target");
                self.invalid_update_multi_output_subquery_selector = self.invalid_update_multi_output_subquery_selector or std.mem.eql(u8, entry.classification_reason, "multi_output_subquery_update_selector");
            },
            .invalid_delete => {
                self.invalid_delete = true;
                self.invalid_delete_multi_output_subquery_selector = self.invalid_delete_multi_output_subquery_selector or std.mem.eql(u8, entry.classification_reason, "multi_output_subquery_delete_selector");
            },
            .invalid_update_source => {
                self.invalid_update_source_row_lock_mode = self.invalid_update_source_row_lock_mode or
                    (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .share }));
                self.invalid_update_source_row_lock_target = self.invalid_update_source_row_lock_target or
                    (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .update, .of }) and
                        appParityTokensHaveIdentifier(sql_tokens, "archived_records"));
            },
            .invalid_update_joined_source => {
                self.invalid_update_joined_source_row_lock_target = self.invalid_update_joined_source_row_lock_target or
                    (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"for", .update, .of }) and
                        appParityTokensHaveIdentifier(sql_tokens, "source"));
            },
            .unsupported => {},
            .unsupported_read => self.unsupported_read = true,
            .unsupported_ddl => self.unsupported_ddl = true,
            .unsupported_write => {},
            .unsupported_insert => self.unsupported_insert = true,
            .unsupported_update => {},
            .unsupported_update_source => {},
            .unsupported_delete => {},
            .unsupported_update_joined_source => {},
            .unsupported_delete_joined_source => {},
            .unsupported_merge_mutation => {},
            .read => {
                const is_read_query = sql_adapter.readPlanHasKind(entry.plan, "query");
                const is_read_aggregate = sql_adapter.readPlanHasKind(entry.plan, "aggregate");
                const is_read_join = sql_adapter.readPlanHasKind(entry.plan, "join");
                const is_read_lateral = sql_adapter.readPlanHasKind(entry.plan, "lateral");
                const is_read_recursive_cte = sql_adapter.readPlanHasKind(entry.plan, "recursive_cte");
                const is_read_set_operation = sql_adapter.readPlanHasKind(entry.plan, "set_operation");
                const is_read_window = sql_adapter.readPlanHasKind(entry.plan, "window");
                const has_cte_expression =
                    sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_");
                const has_read_query_expression =
                    has_cte_expression or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_not=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_array=");
                const has_read_source_expression =
                    has_cte_expression or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array=");
                self.read = true;
                self.read_query = self.read_query or is_read_query;
                self.read_recursive_cte_stream_plan = self.read_recursive_cte_stream_plan or is_read_recursive_cte;
                self.read_aggregate = self.read_aggregate or is_read_aggregate;
                self.read_join = self.read_join or is_read_join;
                self.read_lateral = self.read_lateral or is_read_lateral;
                self.read_window = self.read_window or is_read_window;
                self.read_window_duplicate_output_label = self.read_window_duplicate_output_label or
                    (is_read_window and
                        appParityTokensHaveFunctionCall(sql_tokens, "row_number") and
                        appParityTokensHaveKeyword(sql_tokens, .over) and
                        appParityTokensHaveKeyword(sql_tokens, .as) and
                        appParityTokensHaveIdentifier(sql_tokens, "id"));
                self.read_join_cross_table_source_schema_classifier = self.read_join_cross_table_source_schema_classifier or
                    (is_read_join and
                        appParityEntryHasCatalogSchemas(entry) and
                        sql_adapter.planHasExactStringToken(entry.plan, ":right=", "customer_records"));
                self.read_graph_table_function_cte_join = self.read_graph_table_function_cte_join or
                    (is_read_join and
                        appParityTokensStartWithKeyword(sql_tokens, .with) and
                        appParityTokensHaveIdentifier(sql_tokens, "antfly.graph_match") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":right_source_cte="));
                self.read_graph_table_function_inline_join = self.read_graph_table_function_inline_join or
                    (is_read_join and
                        appParityTokensHaveKeyword(sql_tokens, .join) and
                        appParityTokensHaveIdentifier(sql_tokens, "antfly.graph_match") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":right_source_cte="));
                self.read_lateral_cross_table_source_schema_classifier = self.read_lateral_cross_table_source_schema_classifier or
                    (is_read_lateral and
                        appParityEntryHasCatalogSchemas(entry) and
                        sql_adapter.planHasExactStringToken(entry.plan, ":right=", "balance_records"));
                self.read_set_operation_cross_table_source_schema_classifier = self.read_set_operation_cross_table_source_schema_classifier or
                    (is_read_set_operation and
                        appParityEntryHasCatalogSchemas(entry) and
                        setOperationPlanHasRightTable(entry.plan, "archived_records"));
                self.read_set_operation_cross_table_except_classifier = self.read_set_operation_cross_table_except_classifier or
                    (is_read_set_operation and
                        appParityEntryHasCatalogSchemas(entry) and
                        sql_adapter.planHasExactStringToken(entry.plan, "set_operation:op=", "except") and
                        setOperationPlanHasRightTable(entry.plan, "archived_records"));
                self.read_set_operation_cross_table_intersect_classifier = self.read_set_operation_cross_table_intersect_classifier or
                    (is_read_set_operation and
                        appParityEntryHasCatalogSchemas(entry) and
                        sql_adapter.planHasExactStringToken(entry.plan, "set_operation:op=", "intersect") and
                        setOperationPlanHasRightTable(entry.plan, "archived_records"));
                self.read_cte_query_expression = self.read_cte_query_expression or
                    (is_read_query and has_read_query_expression);
                self.read_cte_aggregate_expression = self.read_cte_aggregate_expression or
                    (is_read_aggregate and has_read_source_expression);
                self.read_cte_window_expression = self.read_cte_window_expression or
                    (is_read_window and has_read_source_expression);
            },
        }
        if (entry.family == .unsupported_read) {
            self.unsupported_read_set_operation_output_shape = self.unsupported_read_set_operation_output_shape or
                (std.mem.eql(u8, entry.classification_reason, "set_operation_output_shape") and
                    appParityTokensHaveKeyword(sql_tokens, .intersect));
        } else if (entry.family == .merge_mutation) {
            self.merge_mutation_default_expressions = self.merge_mutation_default_expressions or
                (appParityTokensHaveKeyword(sql_tokens, .default) and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":matched_update_expr=") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":not_matched_insert_expr="));
        } else if (entry.family == .recursive_insert_source) {
            self.recursive_insert_source = self.recursive_insert_source or
                (sql_adapter.planHasRootKind(entry.plan, "recursive_insert_source") and
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .with, .recursive }) and
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .insert, .into }));
        } else if (entry.family == .truncate_source) {
            self.truncate_continue_identity = self.truncate_continue_identity or
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .@"continue", .identity }) and
                    sql_adapter.planHasExactStringToken(entry.plan, "truncate_source:table=", "usage_records"));
            self.truncate_restart_identity = self.truncate_restart_identity or
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .restart, .identity }) and
                    sql_adapter.planHasExactUsizeToken(entry.plan, ":restart_identity=", 1));
            self.truncate_multi_table_generation_barrier = self.truncate_multi_table_generation_barrier or
                (appParityTokensHaveIdentifier(sql_tokens, "archived_records") and
                    sql_adapter.planHasExactUsizeToken(entry.plan, ":additional_tables=", 1));
            self.truncate_cascade_generation_barrier = self.truncate_cascade_generation_barrier or
                (appParityTokensHaveKeyword(sql_tokens, .cascade) and
                    sql_adapter.planHasExactUsizeToken(entry.plan, ":cascade=", 1));
        } else if (entry.family == .unsupported_ddl) {
            self.unsupported_ddl_copy_wrong_stream_endpoint = self.unsupported_ddl_copy_wrong_stream_endpoint or
                (std.mem.eql(u8, entry.classification_reason, "bulk_io_plan") and
                    appParityTokensStartWithKeyword(sql_tokens, .copy) and
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .to, .stdin }));
            self.unsupported_ddl_copy_unsupported_options = self.unsupported_ddl_copy_unsupported_options or
                (std.mem.eql(u8, entry.classification_reason, "bulk_io_plan") and
                    appParityTokensStartWithKeyword(sql_tokens, .copy) and
                    appParityTokensHaveKeyword(sql_tokens, .oids));
        }
        if (entry.family == .ddl) {
            switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
                .create_table => {
                    self.ddl_create_table = true;
                    self.ddl_inline_named_column_constraints = self.ddl_inline_named_column_constraints or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":pk=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":unique=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":fk=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":checks=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":pk_named=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":unique_named=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":fk_named=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":check_named=", 1) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_unique=", 0) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_fk=", 0) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_check=", 0));
                    self.ddl_temporal_table = self.ddl_temporal_table or sql_adapter.planHasNonZeroToken(entry.plan, ":periods=");
                    self.ddl_system_versioned_table = self.ddl_system_versioned_table or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":system_versioned=", 1) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .system, .versioning }));
                    self.ddl_temporal_fk_delete_set_null_action = self.ddl_temporal_fk_delete_set_null_action or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":temporal_fk=", 1) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .on, .delete, .set, .null }));
                    self.ddl_temporal_fk_delete_cascade_action = self.ddl_temporal_fk_delete_cascade_action or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":temporal_fk=", 1) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .on, .delete, .cascade }));
                    self.ddl_temporal_fk_update_cascade_action = self.ddl_temporal_fk_update_cascade_action or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":temporal_fk=", 1) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .on, .update, .cascade }));
                    self.ddl_replace_table = self.ddl_replace_table or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                },
                .table_clone => self.ddl_table_clone = true,
                .create_view => {
                    self.ddl_view_create = true;
                    self.ddl_view_create_replace = self.ddl_view_create_replace or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                    self.ddl_view_create_if_not_exists = self.ddl_view_create_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                },
                .rename_view => self.ddl_view_rename = true,
                .drop_view => {
                    self.ddl_view_drop = true;
                    self.ddl_view_drop_cascade = self.ddl_view_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_materialized_view => {
                    self.ddl_materialized_view_create = true;
                    self.ddl_materialized_view_create_replace = self.ddl_materialized_view_create_replace or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                    self.ddl_materialized_view_create_if_not_exists = self.ddl_materialized_view_create_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                    self.ddl_materialized_view_create_no_data = self.ddl_materialized_view_create_no_data or sql_adapter.planHasExactBoolToken(entry.plan, ":populate=", false);
                },
                .refresh_materialized_view => {
                    self.ddl_materialized_view_refresh = true;
                    self.ddl_materialized_view_refresh_concurrently = self.ddl_materialized_view_refresh_concurrently or sql_adapter.planHasExactBoolToken(entry.plan, ":concurrently=", true);
                    self.ddl_materialized_view_refresh_no_data = self.ddl_materialized_view_refresh_no_data or sql_adapter.planHasExactBoolToken(entry.plan, ":populate=", false);
                },
                .drop_materialized_view => {
                    self.ddl_materialized_view_drop = true;
                    self.ddl_materialized_view_drop_cascade = self.ddl_materialized_view_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .relation_lifetime => {
                    self.ddl_relation_lifetime_temporary = self.ddl_relation_lifetime_temporary or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "temporary");
                    self.ddl_relation_lifetime_unlogged = self.ddl_relation_lifetime_unlogged or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "unlogged");
                },
                .create_enum_type => self.ddl_enum_type_create = true,
                .add_enum_value => {
                    self.ddl_enum_type_add_value = true;
                    self.ddl_enum_type_add_value_if_not_exists = self.ddl_enum_type_add_value_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                    self.ddl_enum_type_add_value_position = self.ddl_enum_type_add_value_position or
                        sql_adapter.planHasAnyExactStringToken(entry.plan, ":position=", &.{ "before", "after" });
                },
                .drop_enum_type => {
                    self.ddl_enum_type_drop = true;
                    self.ddl_enum_type_drop_cascade = self.ddl_enum_type_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_domain => {
                    self.ddl_domain_create = true;
                    self.ddl_domain_create_default = self.ddl_domain_create_default or sql_adapter.planHasExactBoolToken(entry.plan, ":default=", true);
                    self.ddl_domain_create_not_null = self.ddl_domain_create_not_null or sql_adapter.planHasExactBoolToken(entry.plan, ":not_null=", true);
                },
                .alter_domain => self.ddl_domain_alter = true,
                .drop_domain => {
                    self.ddl_domain_drop = true;
                    self.ddl_domain_drop_cascade = self.ddl_domain_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_sequence => {
                    self.ddl_sequence_create = true;
                    self.ddl_sequence_create_if_not_exists = self.ddl_sequence_create_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                    self.ddl_sequence_create_typed_owned = self.ddl_sequence_create_typed_owned or
                        (appParityTokensHaveKeyword(sql_tokens, .as) and
                            appParityTokensHaveIdentifier(sql_tokens, "bigint") and
                            appParityTokensHaveIdentifier(sql_tokens, "owned") and
                            appParityTokensHaveKeyword(sql_tokens, .by));
                },
                .alter_sequence => {
                    self.ddl_sequence_alter = true;
                    self.ddl_sequence_alter_if_exists = self.ddl_sequence_alter_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                    self.ddl_sequence_alter_typed_owned = self.ddl_sequence_alter_typed_owned or
                        (appParityTokensHaveKeyword(sql_tokens, .as) and
                            appParityTokensHaveIdentifier(sql_tokens, "integer") and
                            appParityTokensHaveIdentifier(sql_tokens, "owned") and
                            appParityTokensHaveKeyword(sql_tokens, .by) and
                            appParityTokensHaveIdentifier(sql_tokens, "none"));
                },
                .drop_sequence => {
                    self.ddl_sequence_drop = true;
                    self.ddl_sequence_drop_cascade = self.ddl_sequence_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .identity_allocator => {
                    self.ddl_identity_allocator_serial = self.ddl_identity_allocator_serial or sql_adapter.planHasAnyExactStringToken(entry.plan, ":kind=", &.{ "serial", "bigserial" });
                    self.ddl_identity_allocator_generated = self.ddl_identity_allocator_generated or sql_adapter.planHasAnyExactStringToken(entry.plan, ":kind=", &.{ "generated_by_default", "generated_always" });
                    self.ddl_identity_allocator_generated_options = self.ddl_identity_allocator_generated_options or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "generated_always") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":options="));
                },
                .create_schema_namespace => {
                    self.ddl_schema_namespace_create = true;
                    self.ddl_schema_namespace_create_if_not_exists = self.ddl_schema_namespace_create_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                },
                .rename_schema_namespace => self.ddl_schema_namespace_rename = true,
                .drop_schema_namespace => {
                    self.ddl_schema_namespace_drop = true;
                    self.ddl_schema_namespace_drop_cascade = self.ddl_schema_namespace_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_extension => {
                    self.ddl_extension_create = true;
                    self.ddl_extension_create_if_not_exists = self.ddl_extension_create_if_not_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_not_exists=", true);
                    self.ddl_extension_create_quoted_sql_name = self.ddl_extension_create_quoted_sql_name or sql_adapter.planHasExactStringToken(entry.plan, ":extension=", "uuid-ossp");
                    self.ddl_extension_create_version = self.ddl_extension_create_version or sql_adapter.planHasStringToken(entry.plan, ":version=");
                },
                .alter_extension_update => {
                    self.ddl_extension_update = true;
                    self.ddl_extension_update_latest = self.ddl_extension_update_latest or sql_adapter.planHasExactStringToken(entry.plan, ":version=", "latest");
                    self.ddl_extension_update_version = self.ddl_extension_update_version or
                        (sql_adapter.planHasStringToken(entry.plan, ":version=") and
                            !sql_adapter.planHasExactStringToken(entry.plan, ":version=", "latest"));
                },
                .drop_extension => {
                    self.ddl_extension_drop = true;
                    self.ddl_extension_drop_cascade = self.ddl_extension_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_function => {
                    self.ddl_function_create = true;
                    self.ddl_function_replace = self.ddl_function_replace or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                    self.ddl_function_volatility = self.ddl_function_volatility or sql_adapter.planHasStringToken(entry.plan, ":volatility=");
                    self.ddl_function_security = self.ddl_function_security or sql_adapter.planHasStringToken(entry.plan, ":security=");
                    self.ddl_function_external_security = self.ddl_function_external_security or
                        (sql_adapter.planHasStringToken(entry.plan, ":security=") and
                            appParityTokensHaveIdentifier(sql_tokens, "external") and
                            appParityTokensHaveIdentifier(sql_tokens, "security"));
                    self.ddl_function_null_input = self.ddl_function_null_input or sql_adapter.planHasStringToken(entry.plan, ":null_input=");
                    self.ddl_function_cost = self.ddl_function_cost or sql_adapter.planHasStringToken(entry.plan, ":cost=");
                    self.ddl_function_rows = self.ddl_function_rows or sql_adapter.planHasStringToken(entry.plan, ":rows=");
                    self.ddl_function_parallel = self.ddl_function_parallel or sql_adapter.planHasStringToken(entry.plan, ":parallel=");
                    self.ddl_function_leakproof = self.ddl_function_leakproof or sql_adapter.planHasExactBoolToken(entry.plan, ":leakproof=", true);
                    self.ddl_function_window = self.ddl_function_window or sql_adapter.planHasExactBoolToken(entry.plan, ":window=", true);
                    self.ddl_function_support = self.ddl_function_support or sql_adapter.planHasStringToken(entry.plan, ":support=");
                    self.ddl_function_transform = self.ddl_function_transform or sql_adapter.planHasNonZeroToken(entry.plan, ":transforms=");
                    self.ddl_function_setting = self.ddl_function_setting or sql_adapter.planHasNonZeroToken(entry.plan, ":settings=");
                    self.ddl_function_sql_expression_body = self.ddl_function_sql_expression_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression"));
                    self.ddl_function_sql_expression_concat_body = self.ddl_function_sql_expression_concat_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "concat_ws["));
                    self.ddl_function_sql_expression_multi_arg_body = self.ddl_function_sql_expression_multi_arg_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "arg2"));
                    self.ddl_function_sql_expression_named_arg_body = self.ddl_function_sql_expression_named_arg_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "lower[field[source:arg1]]") and
                            appParityTokensHaveIdentifier(sql_tokens, "status_text") and
                            appParityTokensHaveIdentifier(sql_tokens, "text") and
                            appParityTokensHaveStringLiteralContaining(sql_tokens, "lower(status_text)"));
                    self.ddl_function_sql_expression_nested_body = self.ddl_function_sql_expression_nested_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "concat_ws[") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "lower[field[source:arg1]]") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "coalesce[field[source:arg2]+"));
                    self.ddl_function_sql_expression_minmax_body = self.ddl_function_sql_expression_minmax_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "sql_expression") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "expression") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "greatest[") and
                            sql_adapter.planHasTrailingRowExpressionFragment(entry.plan, "least["));
                    self.ddl_function_trigger_return_new = self.ddl_function_trigger_return_new or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_trigger") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "trigger_return_new"));
                    self.ddl_function_trigger_perform_body = self.ddl_function_trigger_perform_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_trigger") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "trigger_return_new") and
                            sql_adapter.planHasStringToken(entry.plan, ":perform_args="));
                    self.ddl_function_trigger_return_null = self.ddl_function_trigger_return_null or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_trigger") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "trigger_return_null"));
                    self.ddl_function_trigger_return_old = self.ddl_function_trigger_return_old or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_trigger") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "trigger_return_old"));
                },
                .drop_function => {
                    self.ddl_function_drop = true;
                    self.ddl_function_drop_cascade = self.ddl_function_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .create_procedure => {
                    self.ddl_procedure_create = true;
                    self.ddl_procedure_noop_body = self.ddl_procedure_noop_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_procedure") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "procedure_noop"));
                    self.ddl_procedure_perform_body = self.ddl_procedure_perform_body or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":body=", "plpgsql_procedure") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":hook=", "procedure_noop") and
                            sql_adapter.planHasStringToken(entry.plan, ":perform_args="));
                },
                .drop_procedure => {
                    self.ddl_procedure_drop = true;
                    self.ddl_procedure_drop_cascade = self.ddl_procedure_drop_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .call_procedure => {},
                .create_role => self.ddl_role_create = true,
                .alter_role => {
                    self.ddl_role_alter = true;
                    self.ddl_role_alter_database_scope = self.ddl_role_alter_database_scope or sql_adapter.planHasStringToken(entry.plan, ":database=");
                    self.ddl_role_alter_reset = self.ddl_role_alter_reset or sql_adapter.planHasExactStringToken(entry.plan, ":operation=", "reset");
                    self.ddl_role_alter_current_setting = self.ddl_role_alter_current_setting or
                        sql_adapter.planHasExactStringToken(entry.plan, ":value_source=", "current_setting");
                    self.ddl_role_alter_runtime_setting = self.ddl_role_alter_runtime_setting or
                        sql_adapter.planHasExactStringToken(entry.plan, ":setting_kind=", "runtime") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":operation=", "set");
                    self.ddl_role_alter_runtime_reset = self.ddl_role_alter_runtime_reset or
                        sql_adapter.planHasExactStringToken(entry.plan, ":setting_kind=", "runtime") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":operation=", "reset");
                },
                .drop_role => self.ddl_role_drop = true,
                .grant_privilege => self.ddl_privilege_grant = true,
                .revoke_privilege => self.ddl_privilege_revoke = true,
                .copy_from => {
                    self.ddl_copy_from = true;
                    self.ddl_copy_binary_execution_contract = self.ddl_copy_binary_execution_contract or
                        sql_adapter.planHasExactStringToken(entry.plan, ":format=", "binary") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":codec=", "postgres_binary");
                    self.ddl_copy_from_execution_contract = self.ddl_copy_from_execution_contract or
                        sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":op=", "import_rows") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":native=", "rows_batch") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":stream=", "stdin");
                    self.ddl_copy_file_endpoint = self.ddl_copy_file_endpoint or
                        sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":stream=", "file") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":endpoint_kind=", "file");
                    self.ddl_copy_from_text_execution_contract = self.ddl_copy_from_text_execution_contract or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":format=", "text") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":codec=", "postgres_text"));
                    self.ddl_copy_default_marker = self.ddl_copy_default_marker or sql_adapter.planHasExactStringToken(entry.plan, ":default_marker_hex=", "6e2f61");
                    self.ddl_copy_header = self.ddl_copy_header or sql_adapter.planHasExactBoolToken(entry.plan, ":header=", true);
                    self.ddl_copy_delimiter = self.ddl_copy_delimiter or sql_adapter.planHasExactStringToken(entry.plan, ":delimiter_hex=", "2c");
                    self.ddl_copy_escape = self.ddl_copy_escape or sql_adapter.planHasExactStringToken(entry.plan, ":escape_hex=", "21");
                    self.ddl_copy_encoding = self.ddl_copy_encoding or sql_adapter.planHasExactStringToken(entry.plan, ":encoding_hex=", "55544638");
                    self.ddl_copy_force_not_null = self.ddl_copy_force_not_null or sql_adapter.planHasExactUsizeToken(entry.plan, ":force_not_null_columns=", 2);
                    self.ddl_copy_force_null = self.ddl_copy_force_null or sql_adapter.planHasExactUsizeToken(entry.plan, ":force_null_columns=", 1);
                    self.ddl_copy_freeze = self.ddl_copy_freeze or sql_adapter.planHasExactBoolToken(entry.plan, ":freeze=", true);
                    self.ddl_copy_log_verbosity = self.ddl_copy_log_verbosity or sql_adapter.planHasExactStringToken(entry.plan, ":log_verbosity=", "verbose");
                    self.ddl_copy_null_marker = self.ddl_copy_null_marker or sql_adapter.planHasExactStringToken(entry.plan, ":null_marker_hex=", "empty");
                    self.ddl_copy_oids_false_noop = self.ddl_copy_oids_false_noop or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .oids, .false }) and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":op=", "import_rows") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":native=", "rows_batch");
                    self.ddl_copy_on_error_ignore = self.ddl_copy_on_error_ignore or sql_adapter.planHasExactStringToken(entry.plan, ":on_error=", "ignore");
                    self.ddl_copy_program_endpoint = self.ddl_copy_program_endpoint or
                        appParityTokensHaveKeyword(sql_tokens, .program) and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":stream=", "program") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":endpoint_kind=", "program");
                    self.ddl_copy_reject_limit = self.ddl_copy_reject_limit or sql_adapter.planHasExactUsizeToken(entry.plan, ":reject_limit=", 10);
                    self.ddl_copy_quote = self.ddl_copy_quote or sql_adapter.planHasExactStringToken(entry.plan, ":quote_hex=", "22");
                    self.ddl_copy_where_expression = self.ddl_copy_where_expression or sql_adapter.planHasExactUsizeToken(entry.plan, ":where_expressions=", 1);
                },
                .copy_to => {
                    self.ddl_copy_to = true;
                    self.ddl_copy_binary_execution_contract = self.ddl_copy_binary_execution_contract or
                        sql_adapter.planHasExactStringToken(entry.plan, ":format=", "binary") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":codec=", "postgres_binary");
                    self.ddl_copy_to_execution_contract = self.ddl_copy_to_execution_contract or
                        sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":op=", "export_rows") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":native=", "rows_query") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":stream=", "stdout");
                    self.ddl_copy_to_text_execution_contract = self.ddl_copy_to_text_execution_contract or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":format=", "text") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":codec=", "postgres_text"));
                    self.ddl_copy_program_endpoint = self.ddl_copy_program_endpoint or
                        appParityTokensHaveKeyword(sql_tokens, .program) and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":stream=", "program") and
                            sql_adapter.bulkSqlIoExecutionPlanHasExactStringToken(entry.execution_plan, ":endpoint_kind=", "program");
                    self.ddl_copy_force_quote = self.ddl_copy_force_quote or sql_adapter.planHasExactStringToken(entry.plan, ":force_quote=", "all");
                },
                .create_partitioned_table => self.ddl_partition_create_parent = true,
                .create_table_partition => self.ddl_partition_create_child = true,
                .attach_table_partition => self.ddl_partition_attach = true,
                .detach_table_partition => self.ddl_partition_detach = true,
                .enable_row_security => self.ddl_row_security_enable = true,
                .disable_row_security => self.ddl_row_security_disable = true,
                .create_row_policy => {
                    self.ddl_row_security_create_policy = true;
                    self.ddl_row_security_conjunction_policy = self.ddl_row_security_conjunction_policy or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "and");
                    self.ddl_row_security_disjunction_policy = self.ddl_row_security_disjunction_policy or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "or");
                    self.ddl_row_security_check_policy = self.ddl_row_security_check_policy or sql_adapter.planHasStringToken(entry.plan, ":check=");
                    self.ddl_row_security_expression_policy = self.ddl_row_security_expression_policy or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "expression");
                    self.ddl_row_security_literal_policy = self.ddl_row_security_literal_policy or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "literal_eq");
                    self.ddl_row_security_targeted_policy = self.ddl_row_security_targeted_policy or sql_adapter.planHasStringToken(entry.plan, ":roles=");
                },
                .alter_row_policy => {
                    self.ddl_row_security_alter_policy = true;
                    self.ddl_row_security_check_policy = self.ddl_row_security_check_policy or sql_adapter.planHasStringToken(entry.plan, ":check=");
                },
                .drop_row_policy => self.ddl_row_security_drop_policy = true,
                .create_database => self.ddl_database_create = true,
                .alter_database => {
                    self.ddl_database_alter = true;
                    self.ddl_database_alter_setting = self.ddl_database_alter_setting or sql_adapter.planHasNonZeroToken(entry.plan, ":ops=");
                },
                .drop_database => {
                    self.ddl_database_drop = true;
                    self.ddl_database_drop_if_exists = self.ddl_database_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                    self.ddl_database_drop_force = self.ddl_database_drop_force or sql_adapter.planHasExactBoolToken(entry.plan, ":force=", true);
                },
                .create_tablespace => {
                    self.ddl_tablespace_create = true;
                    self.ddl_tablespace_create_location = self.ddl_tablespace_create_location or sql_adapter.planHasExactBoolToken(entry.plan, ":location=", true);
                },
                .rename_tablespace => self.ddl_tablespace_rename = true,
                .drop_tablespace => {
                    self.ddl_tablespace_drop = true;
                    self.ddl_tablespace_drop_if_exists = self.ddl_tablespace_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .listen_notification => self.ddl_notification_listen = true,
                .notify_notification => self.ddl_notification_notify = true,
                .unlisten_notification => self.ddl_notification_unlisten = true,
                .create_publication => {
                    self.ddl_publication_create = true;
                    self.ddl_publication_create_all_tables = self.ddl_publication_create_all_tables or sql_adapter.planHasExactBoolToken(entry.plan, ":all=", true);
                    self.ddl_publication_create_table_list = self.ddl_publication_create_table_list or sql_adapter.planHasNonZeroToken(entry.plan, ":tables=");
                },
                .alter_publication => {
                    self.ddl_publication_alter = true;
                    self.ddl_publication_alter_add_table = self.ddl_publication_alter_add_table or sql_adapter.planHasNonZeroToken(entry.plan, ":add_tables=");
                },
                .drop_publication => {
                    self.ddl_publication_drop = true;
                    self.ddl_publication_drop_if_exists = self.ddl_publication_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .create_subscription => {
                    self.ddl_subscription_create = true;
                    self.ddl_subscription_create_multi_publication = self.ddl_subscription_create_multi_publication or
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":publications=", 2);
                },
                .alter_subscription => {
                    self.ddl_subscription_alter = true;
                    self.ddl_subscription_alter_enable = self.ddl_subscription_alter_enable or sql_adapter.planHasExactBoolToken(entry.plan, ":enabled=", true);
                    self.ddl_subscription_alter_disable = self.ddl_subscription_alter_disable or sql_adapter.planHasExactBoolToken(entry.plan, ":enabled=", false);
                },
                .drop_subscription => {
                    self.ddl_subscription_drop = true;
                    self.ddl_subscription_drop_if_exists = self.ddl_subscription_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .create_collation => {
                    self.ddl_collation_create = true;
                    self.ddl_collation_create_options = self.ddl_collation_create_options or sql_adapter.planHasNonZeroToken(entry.plan, ":options=");
                },
                .rename_collation => self.ddl_collation_rename = true,
                .drop_collation => {
                    self.ddl_collation_drop = true;
                    self.ddl_collation_drop_if_exists = self.ddl_collation_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .create_operator => {
                    self.ddl_operator_create = true;
                    self.ddl_operator_create_options = self.ddl_operator_create_options or sql_adapter.planHasNonZeroToken(entry.plan, ":options=");
                },
                .drop_operator => {
                    self.ddl_operator_drop = true;
                    self.ddl_operator_drop_args = self.ddl_operator_drop_args or sql_adapter.planHasNonZeroToken(entry.plan, ":args=");
                    self.ddl_operator_drop_if_exists = self.ddl_operator_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .create_aggregate => {
                    self.ddl_aggregate_create = true;
                    self.ddl_aggregate_create_args = self.ddl_aggregate_create_args or sql_adapter.planHasNonZeroToken(entry.plan, ":args=");
                    self.ddl_aggregate_create_options = self.ddl_aggregate_create_options or sql_adapter.planHasNonZeroToken(entry.plan, ":options=");
                },
                .drop_aggregate => {
                    self.ddl_aggregate_drop = true;
                    self.ddl_aggregate_drop_args = self.ddl_aggregate_drop_args or sql_adapter.planHasNonZeroToken(entry.plan, ":args=");
                    self.ddl_aggregate_drop_if_exists = self.ddl_aggregate_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .create_cast => {
                    self.ddl_cast_create = true;
                    self.ddl_cast_create_assignment = self.ddl_cast_create_assignment or sql_adapter.planHasExactBoolToken(entry.plan, ":assignment=", true);
                    self.ddl_cast_create_function = self.ddl_cast_create_function or sql_adapter.planHasStringToken(entry.plan, ":function=");
                },
                .drop_cast => {
                    self.ddl_cast_drop = true;
                    self.ddl_cast_drop_if_exists = self.ddl_cast_drop_if_exists or sql_adapter.planHasExactBoolToken(entry.plan, ":if_exists=", true);
                },
                .vacuum_maintenance => {
                    self.ddl_vacuum_maintenance = true;
                    self.ddl_vacuum_maintenance_options = self.ddl_vacuum_maintenance_options or
                        (sql_adapter.planHasExactBoolToken(entry.plan, ":full=", true) and
                            sql_adapter.planHasExactBoolToken(entry.plan, ":verbose=", true));
                },
                .analyze_maintenance => {
                    self.ddl_analyze_maintenance = true;
                    self.ddl_analyze_maintenance_columns = self.ddl_analyze_maintenance_columns or sql_adapter.planHasNonZeroToken(entry.plan, ":columns=");
                    self.ddl_analyze_maintenance_verbose = self.ddl_analyze_maintenance_verbose or sql_adapter.planHasExactBoolToken(entry.plan, ":verbose=", true);
                },
                .reindex_maintenance => {
                    self.ddl_reindex_maintenance = true;
                    self.ddl_reindex_maintenance_concurrently = self.ddl_reindex_maintenance_concurrently or sql_adapter.planHasExactBoolToken(entry.plan, ":concurrently=", true);
                    self.ddl_reindex_maintenance_index_target = self.ddl_reindex_maintenance_index_target or sql_adapter.planHasExactStringToken(entry.plan, ":target=", "index");
                },
                .cluster_maintenance => {
                    self.ddl_cluster_maintenance = true;
                    self.ddl_cluster_maintenance_index = self.ddl_cluster_maintenance_index or
                        !sql_adapter.planHasExactStringToken(entry.plan, ":index=", "");
                    self.ddl_cluster_maintenance_verbose = self.ddl_cluster_maintenance_verbose or sql_adapter.planHasExactBoolToken(entry.plan, ":verbose=", true);
                },
                .prepare_statement => {
                    self.ddl_prepare_statement = true;
                    self.ddl_prepare_statement_read_subject = self.ddl_prepare_statement_read_subject or sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "read");
                    self.ddl_prepare_statement_write_subject = self.ddl_prepare_statement_write_subject or sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "write");
                    self.ddl_prepare_statement_params = self.ddl_prepare_statement_params or sql_adapter.planHasNonZeroToken(entry.plan, ":params=");
                    self.ddl_prepare_statement_ddl_family = self.ddl_prepare_statement_ddl_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "ddl");
                    self.ddl_prepare_statement_insert_family = self.ddl_prepare_statement_insert_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "insert");
                    self.ddl_prepare_statement_read_family = self.ddl_prepare_statement_read_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "read");
                    self.ddl_prepare_statement_insert_source_family = self.ddl_prepare_statement_insert_source_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "insert_source");
                    self.ddl_prepare_statement_truncate_family = self.ddl_prepare_statement_truncate_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "truncate");
                    self.ddl_prepare_statement_update_family = self.ddl_prepare_statement_update_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "update");
                    self.ddl_prepare_statement_delete_family = self.ddl_prepare_statement_delete_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "delete");
                    self.ddl_prepare_statement_merge_family = self.ddl_prepare_statement_merge_family or sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "merge");
                    self.ddl_prepare_cte_write_statement = self.ddl_prepare_cte_write_statement or
                        appParityTokensStartWithKeyword(sql_tokens, .prepare) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .as, .with }) and
                            sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "write");
                    self.ddl_prepare_recursive_cte_read_statement = self.ddl_prepare_recursive_cte_read_statement or
                        appParityTokensStartWithKeyword(sql_tokens, .prepare) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .as, .with, .recursive }) and
                            sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "read") and
                            sql_adapter.planHasExactStringToken(entry.plan, ":statement=", "read");
                    self.ddl_prepare_recursive_cte_write_statement = self.ddl_prepare_recursive_cte_write_statement or
                        appParityTokensStartWithKeyword(sql_tokens, .prepare) and
                            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .as, .with, .recursive }) and
                            sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "write");
                },
                .prepare_transaction => {
                    self.ddl_prepared_transaction_prepare = true;
                    self.ddl_prepared_transaction_recovery_contract = self.ddl_prepared_transaction_recovery_contract or
                        sql_adapter.preparedTransactionRecoveryPlanHasExactStringToken(entry.execution_plan, ":op=", "register_prepared");
                },
                .commit_prepared => {
                    self.ddl_prepared_transaction_commit = true;
                    self.ddl_prepared_transaction_recovery_contract = self.ddl_prepared_transaction_recovery_contract or
                        sql_adapter.preparedTransactionRecoveryPlanHasExactStringToken(entry.execution_plan, ":op=", "resolve_commit");
                },
                .rollback_prepared => {
                    self.ddl_prepared_transaction_rollback = true;
                    self.ddl_prepared_transaction_recovery_contract = self.ddl_prepared_transaction_recovery_contract or
                        sql_adapter.preparedTransactionRecoveryPlanHasExactStringToken(entry.execution_plan, ":op=", "resolve_rollback");
                },
                .execute_statement => {
                    self.ddl_execute_statement = true;
                    self.ddl_execute_statement_args = self.ddl_execute_statement_args or sql_adapter.planHasNonZeroToken(entry.plan, ":args=");
                },
                .deallocate_statement => {
                    self.ddl_deallocate_statement = true;
                    self.ddl_deallocate_statement_all = self.ddl_deallocate_statement_all or sql_adapter.planHasExactBoolToken(entry.plan, ":all=", true);
                },
                .declare_cursor => {
                    self.ddl_declare_cursor = true;
                    self.ddl_declare_cursor_binary_hold_scroll = self.ddl_declare_cursor_binary_hold_scroll or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":scroll=", "scroll") and
                            sql_adapter.planHasExactBoolToken(entry.plan, ":binary=", true) and
                            sql_adapter.planHasExactBoolToken(entry.plan, ":hold=", true));
                    self.ddl_declare_cursor_read_subject = self.ddl_declare_cursor_read_subject or sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "read");
                },
                .fetch_cursor => {
                    self.ddl_fetch_cursor = true;
                    self.ddl_fetch_cursor_absolute = self.ddl_fetch_cursor_absolute or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "absolute");
                    self.ddl_fetch_cursor_all = self.ddl_fetch_cursor_all or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "all");
                    self.ddl_fetch_cursor_backward = self.ddl_fetch_cursor_backward or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "backward");
                    self.ddl_fetch_cursor_count = self.ddl_fetch_cursor_count or sql_adapter.planHasNonZeroToken(entry.plan, ":count=");
                    self.ddl_fetch_cursor_first = self.ddl_fetch_cursor_first or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "first");
                    self.ddl_fetch_cursor_forward = self.ddl_fetch_cursor_forward or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "forward");
                    self.ddl_fetch_cursor_last = self.ddl_fetch_cursor_last or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "last");
                    self.ddl_fetch_cursor_prior = self.ddl_fetch_cursor_prior or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "prior");
                    self.ddl_fetch_cursor_relative = self.ddl_fetch_cursor_relative or sql_adapter.planHasExactStringToken(entry.plan, ":direction=", "relative");
                },
                .close_cursor => {
                    self.ddl_close_cursor = true;
                    self.ddl_close_cursor_all = self.ddl_close_cursor_all or sql_adapter.planHasExactBoolToken(entry.plan, ":all=", true);
                },
                .savepoint_transaction => self.ddl_savepoint_transaction = true,
                .release_savepoint => self.ddl_release_savepoint = true,
                .rollback_to_savepoint => self.ddl_rollback_to_savepoint = true,
                .comment_metadata => {
                    self.ddl_comment_table = self.ddl_comment_table or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "table");
                    self.ddl_comment_column = self.ddl_comment_column or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "column");
                    self.ddl_comment_index = self.ddl_comment_index or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "index");
                    self.ddl_comment_constraint = self.ddl_comment_constraint or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "constraint");
                },
                .table_lock => {
                    self.ddl_table_lock = true;
                    self.ddl_table_lock_access_exclusive = self.ddl_table_lock_access_exclusive or sql_adapter.planHasExactStringToken(entry.plan, ":mode=", "access_exclusive");
                    self.ddl_table_lock_multi_table = self.ddl_table_lock_multi_table or sql_adapter.planHasExactUsizeToken(entry.plan, ":tables=", 2);
                    self.ddl_table_lock_share_row_exclusive = self.ddl_table_lock_share_row_exclusive or sql_adapter.planHasExactStringToken(entry.plan, ":mode=", "share_row_exclusive");
                },
                .constraint_mode => {
                    self.ddl_constraint_mode = true;
                    self.ddl_constraint_mode_all = self.ddl_constraint_mode_all or sql_adapter.planHasExactBoolToken(entry.plan, ":all=", true);
                    self.ddl_constraint_mode_named = self.ddl_constraint_mode_named or sql_adapter.planHasNonZeroToken(entry.plan, ":constraints=");
                    self.ddl_constraint_mode_deferred = self.ddl_constraint_mode_deferred or sql_adapter.planHasExactStringToken(entry.plan, ":mode=", "deferred");
                    self.ddl_constraint_mode_immediate = self.ddl_constraint_mode_immediate or sql_adapter.planHasExactStringToken(entry.plan, ":mode=", "immediate");
                },
                .transaction_mode => {
                    self.ddl_set_transaction_mode = self.ddl_set_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "set_transaction");
                    self.ddl_start_transaction_mode = self.ddl_start_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "start_transaction");
                    self.ddl_begin_transaction_mode = self.ddl_begin_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "begin");
                    self.ddl_transaction_isolation = self.ddl_transaction_isolation or !sql_adapter.planHasExactStringToken(entry.plan, ":isolation=", "none");
                    self.ddl_transaction_read_only = self.ddl_transaction_read_only or sql_adapter.planHasExactStringToken(entry.plan, ":access=", "read_only");
                    self.ddl_transaction_read_write = self.ddl_transaction_read_write or sql_adapter.planHasExactStringToken(entry.plan, ":access=", "read_write");
                    self.ddl_transaction_deferrable_true = self.ddl_transaction_deferrable_true or sql_adapter.planHasExactStringToken(entry.plan, ":deferrable=", "true");
                    self.ddl_transaction_deferrable_false = self.ddl_transaction_deferrable_false or sql_adapter.planHasExactStringToken(entry.plan, ":deferrable=", "false");
                },
                .advisory_lock => {
                    self.ddl_advisory_lock = self.ddl_advisory_lock or sql_adapter.planHasExactStringToken(entry.plan, ":action=", "lock");
                    self.ddl_advisory_unlock = self.ddl_advisory_unlock or sql_adapter.planHasExactStringToken(entry.plan, ":action=", "unlock");
                    self.ddl_advisory_lock_two_keys = self.ddl_advisory_lock_two_keys or sql_adapter.planHasExactUsizeToken(entry.plan, ":keys=", 2);
                },
                .set_search_path => {
                    self.session_set_search_path = true;
                    self.session_set_search_path_local = self.session_set_search_path_local or sql_adapter.planHasExactBoolToken(entry.plan, ":local=", true);
                    self.session_set_search_path_multi_namespace = self.session_set_search_path_multi_namespace or
                        (sql_adapter.planUsizeTokenValue(entry.plan, ":namespaces=") orelse 0) > 1;
                },
                .set_setting => {
                    self.session_set_app_setting = self.session_set_app_setting or
                        sql_adapter.planHasExactStringToken(entry.plan, ":setting_kind=", "app");
                    self.session_set_runtime_setting = self.session_set_runtime_setting or
                        sql_adapter.planHasExactStringToken(entry.plan, ":setting_kind=", "runtime");
                },
                .reset_search_path => self.session_reset_search_path = true,
                .reset_setting => self.session_reset_app_setting = self.session_reset_app_setting or
                    sql_adapter.planHasExactStringToken(entry.plan, ":setting_kind=", "app"),
                .show_search_path => self.session_show_search_path = true,
                .discard_all => self.session_discard = true,
                .create_index => {
                    self.ddl_create_index = true;
                    self.ddl_create_covering_index = self.ddl_create_covering_index or sql_adapter.planHasNonZeroToken(entry.plan, ":include=");
                    self.ddl_create_covering_generated_index = self.ddl_create_covering_generated_index or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":generated_expr=") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":include=");
                    self.ddl_create_covering_gin_index = self.ddl_create_covering_gin_index or
                        sql_adapter.planHasExactStringToken(entry.plan, ":method=", "gin") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":include=");
                },
                .drop_index => self.ddl_drop_index = true,
                .drop_table => {
                    self.ddl_drop_table = true;
                    self.ddl_drop_table_cascade = self.ddl_drop_table_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .alter_table => {
                    self.ddl_alter_table = true;
                    self.ddl_add_column_default_rewrite = self.ddl_add_column_default_rewrite or
                        appParityTokensHaveKeywordsInOrder(sql_tokens, &.{ .add, .column, .default }) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", true) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", true) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", true);
                    self.ddl_add_unvalidated_unique = self.ddl_add_unvalidated_unique or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_unique=", 1);
                    self.ddl_add_unvalidated_fk = self.ddl_add_unvalidated_fk or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_fk=", 1);
                    self.ddl_add_unvalidated_check = self.ddl_add_unvalidated_check or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_check=", 1);
                    self.ddl_add_deferrable_primary_key = self.ddl_add_deferrable_primary_key or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":pk_deferrable=") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":pk_deferred=");
                    self.ddl_add_deferrable_unique_constraint = self.ddl_add_deferrable_unique_constraint or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":unique_deferrable=") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":unique_deferred=");
                    self.ddl_validate_constraint = self.ddl_validate_constraint or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .validate, .constraint });
                    self.ddl_drop_constraint = self.ddl_drop_constraint or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .constraint });
                    self.ddl_drop_column = self.ddl_drop_column or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .column });
                    self.ddl_alter_column_default = self.ddl_alter_column_default or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .set, .default }) or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .default });
                    self.ddl_drop_column_default = self.ddl_drop_column_default or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .default }) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false);
                    self.ddl_alter_column_not_null = self.ddl_alter_column_not_null or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .set, .not, .null }) or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .not, .null });
                    self.ddl_drop_column_not_null = self.ddl_drop_column_not_null or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .not, .null }) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false);
                    self.ddl_alter_column_type = self.ddl_alter_column_type or
                        appParityTokensHaveKeyword(sql_tokens, .type) or
                        appParityTokensHaveKeywordSequence(sql_tokens, &.{ .set, .data, .type });
                    self.ddl_alter_column_rewrite_expression = self.ddl_alter_column_rewrite_expression or
                        sql_adapter.planHasNonZeroToken(entry.plan, ":alter_type_rewrite_expr=") and
                            sql_adapter.appliedPlanHasRowImageRewriteExpression(entry.applied_plan);
                    self.ddl_rename_column = self.ddl_rename_column or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .rename, .column });
                    self.ddl_rename_constraint = self.ddl_rename_constraint or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .rename, .constraint });
                    self.ddl_drop_update_policy = self.ddl_drop_update_policy or appParityTokensHaveKeywordSequence(sql_tokens, &.{ .drop, .trigger });
                },
                .create_update_policy => self.ddl_create_update_policy = true,
            }
        } else if (entry.family == .adapter_noop_ddl) {
            self.adapter_noop_transaction = self.adapter_noop_transaction or std.mem.eql(u8, entry.classification_reason, "transaction_control");
            self.adapter_noop_transaction_commit = self.adapter_noop_transaction_commit or
                std.mem.eql(u8, entry.classification_reason, "transaction_control") and
                    appParityTokensStartWithKeyword(sql_tokens, .commit);
            self.adapter_noop_transaction_rollback = self.adapter_noop_transaction_rollback or
                std.mem.eql(u8, entry.classification_reason, "transaction_control") and
                    appParityTokensStartWithKeyword(sql_tokens, .rollback);
            self.adapter_noop_session = self.adapter_noop_session or std.mem.eql(u8, entry.classification_reason, "session_setting");
            self.adapter_noop_session_probe = self.adapter_noop_session_probe or
                std.mem.eql(u8, entry.classification_reason, "session_setting") and
                    (appParityTokensStartWithKeyword(sql_tokens, .reset) or appParityTokensStartWithKeyword(sql_tokens, .show));
            self.adapter_noop_schema_namespace = self.adapter_noop_schema_namespace or std.mem.eql(u8, entry.classification_reason, "schema_namespace");
            self.adapter_noop_extension = self.adapter_noop_extension or std.mem.eql(u8, entry.classification_reason, "extension");
            self.session_discard = self.session_discard or
                std.mem.eql(u8, entry.classification_reason, "session_setting") and
                    appParityTokensStartWithKeyword(sql_tokens, .discard);
        }

        self.scalar_membership = self.scalar_membership or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":in=",
            "_in=",
            ":source_in=",
            "_source_in=",
            ":array_any=",
            "_array_any=",
        });
        self.boolean_is_predicate = self.boolean_is_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .true }) or
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .false }));
        self.boolean_is_not_predicate = self.boolean_is_not_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .true }) or
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .false }));
        self.boolean_unknown_predicate = self.boolean_unknown_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .unknown }) or
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .unknown }));
        self.postfix_null_test_predicate = self.postfix_null_test_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (appParityTokensHaveIdentifier(sql_tokens, "isnull") or
                    appParityTokensHaveIdentifier(sql_tokens, "notnull"));
        self.expression_postfix_null_test_predicate = self.expression_postfix_null_test_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
                (appParityTokensHaveIdentifier(sql_tokens, "isnull") or
                    appParityTokensHaveIdentifier(sql_tokens, "notnull"));
        self.json_access_path = self.json_access_path or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":json_eq=",
            "_json_eq=",
            ":json_contains=",
            "_json_contains=",
            ":json_exists=",
            "_json_exists=",
            ":source_json_eq=",
            ":source_json_contains=",
            ":source_json_exists=",
        });
        self.array_access_path = self.array_access_path or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":array_contains=",
            "_array_contains=",
            ":array_eq=",
            "_array_eq=",
            ":source_array_contains=",
            ":source_array_eq=",
        });
        self.text_pattern = self.text_pattern or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":text_pattern=",
            "_text_pattern=",
            ":source_text_pattern=",
        });
        self.query_select_all_disambiguated_outputs = self.query_select_all_disambiguated_outputs or
            (entry.family == .query and
                sql_adapter.planHasExactBoolToken(entry.plan, "select_all=", true) and
                planHasExactStringToken(entry.plan, ":select_all_alias0=", "status_3") and
                planHasExactStringToken(entry.plan, ":select_all_expr0=", "status_2"));
        self.query_access_or_predicates = self.query_access_or_predicates or
            entry.family == .query and sql_adapter.planHasNonZeroToken(entry.plan, ":access_or=");
        self.query_array_overlap_access_or = self.query_array_overlap_access_or or
            entry.family == .query and
                appParityTokensHaveIdentifier(sql_tokens, "tags") and
                appParityTokensHaveKind(sql_tokens, .range_overlap) and
                appParityTokensHaveKeyword(sql_tokens, .array) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":access_or=");
        self.query_access_not_predicates = self.query_access_not_predicates or
            entry.family == .query and sql_adapter.planHasNonZeroToken(entry.plan, ":access_not=");
        self.read_row_lock_nowait = self.read_row_lock_nowait or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "nowait", "no_key_update_nowait", "share_nowait", "key_share_nowait" }));
        self.read_row_lock_share = self.read_row_lock_share or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "share", "share_nowait", "share_skip_locked" }));
        self.read_row_lock_key_share = self.read_row_lock_key_share or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "key_share", "key_share_nowait", "key_share_skip_locked" }));
        self.query_row_lock_no_key_update = self.query_row_lock_no_key_update or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "no_key_update", "no_key_update_nowait", "no_key_update_skip_locked" }));
        self.expression_predicate = self.expression_predicate or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":expr_pred=",
            "_expr_pred=",
            ":source_expr_pred=",
            ":expr_or=",
            "_expr_or=",
            ":source_expr_or=",
            ":expr_not=",
            "_expr_not=",
            ":source_expr_not=",
            ":expr_array=",
            "_expr_array=",
            ":source_expr_array=",
            ":having_expr=",
            ":having_any=",
            ":having_not=",
            ":filter_expr=",
            ":filter_groups=",
        });
        self.mixed_scalar_expression_or = self.mixed_scalar_expression_or or
            entry.family == .query and
                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") and
                appParityTokensHaveIdentifier(sql_tokens, "id") and
                appParityTokensHaveKeyword(sql_tokens, .@"or") and
                appParityTokensHaveFunctionCall(sql_tokens, "lower") and
                appParityTokensHaveIdentifier(sql_tokens, "email");
        self.expression_order = self.expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=") or sql_adapter.planHasNonZeroToken(entry.plan, "_order_expr=");
        self.query_order_using_operator = self.query_order_using_operator or (entry.family == .query and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.aggregate_order_using_operator = self.aggregate_order_using_operator or (entry.family == .aggregate and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.join_order_using_operator = self.join_order_using_operator or (entry.family == .join and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.lateral_order_using_operator = self.lateral_order_using_operator or (entry.family == .lateral and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.window_order_using_operator = self.window_order_using_operator or (entry.family == .window and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.update_source_order_using_operator = self.update_source_order_using_operator or (entry.family == .update_source and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_order="));
        self.delete_source_order_using_operator = self.delete_source_order_using_operator or (entry.family == .delete_source and
            appParityTokensHaveKeyword(sql_tokens, .using) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_order="));
        self.query_fixed_interval_expression = self.query_fixed_interval_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "interval") and
            appParityTokensHaveStringLiteral(sql_tokens, "1 hour") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_calendar_interval_expression = self.query_calendar_interval_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "interval") and
            appParityTokensHaveStringLiteral(sql_tokens, "1 month") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_mixed_interval_expression = self.query_mixed_interval_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "interval") and
            appParityTokensHaveStringLiteral(sql_tokens, "1 month 1 day") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_now_expression = self.query_now_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "now") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_timestamp_expression = self.query_current_timestamp_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "current_timestamp") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_timestamp_precision_expression = self.query_current_timestamp_precision_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "current_timestamp") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_date_expression = self.query_current_date_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "current_date") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_uuid_generation_expression = self.query_uuid_generation_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "gen_random_uuid") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_uuid_generate_v4_expression = self.query_uuid_generate_v4_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "uuid_generate_v4") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_substring_expression = self.query_substring_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "substring") and
            appParityTokensHaveFunctionCall(sql_tokens, "substr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_overlay_expression = self.query_overlay_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "overlay") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_translate_expression = self.query_translate_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "translate") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_split_part_expression = self.query_split_part_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "split_part") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_strpos_expression = self.query_strpos_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "strpos") and
            appParityTokensHaveFunctionCall(sql_tokens, "position") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_left_right_expression = self.query_left_right_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "left") and
            appParityTokensHaveFunctionCall(sql_tokens, "right") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_trim_variant_expression = self.query_trim_variant_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "btrim") and
            appParityTokensHaveFunctionCall(sql_tokens, "ltrim") and
            appParityTokensHaveFunctionCall(sql_tokens, "rtrim") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_replace_expression = self.query_regexp_replace_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_replace") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_substr_expression = self.query_regexp_substr_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_match_expression = self.query_regexp_match_expression or (entry.family == .query and
            appParityTokensHaveKind(sql_tokens, .regex_match) and
            appParityTokensHaveKind(sql_tokens, .regex_not_imatch) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_regexp_count_expression = self.query_regexp_count_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_instr_expression = self.query_regexp_instr_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_pad_expression = self.query_pad_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "lpad") and
            appParityTokensHaveFunctionCall(sql_tokens, "rpad") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_repeat_expression = self.query_repeat_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "repeat") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_reverse_expression = self.query_reverse_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "reverse") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_initcap_expression = self.query_initcap_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "initcap") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_text_length_expression = self.query_text_length_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "char_length") and
            appParityTokensHaveFunctionCall(sql_tokens, "character_length") and
            appParityTokensHaveFunctionCall(sql_tokens, "octet_length") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_bit_length_expression = self.query_bit_length_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "bit_length") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_md5_expression = self.query_md5_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "md5") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_concat_ws_expression = self.query_concat_ws_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "concat_ws") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nullif_expression = self.query_nullif_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "nullif") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_extremum_expression = self.query_extremum_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "greatest") and
            appParityTokensHaveFunctionCall(sql_tokens, "least") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nullable_pagination = self.query_nullable_pagination or (entry.family == .query and
            appParityTokensHaveKeywordSequence(sql_tokens, &.{ .limit, .null, .offset, .null }) and
            sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "none") and
            sql_adapter.planTokenAbsent(entry.plan, ":offset="));
        self.query_json_build_object_expression = self.query_json_build_object_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "jsonb_build_object") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_to_jsonb_expression = self.query_to_jsonb_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "to_jsonb") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_convert_from_jsonb_expression = self.query_convert_from_jsonb_expression or (entry.family == .query and
            appParityTokensHaveFunctionCallWithLiteral(sql_tokens, "convert_from", "UTF8") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_cardinality_expression = self.query_cardinality_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "cardinality") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_array_position_expression = self.query_array_position_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "array_position") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_array_positions_expression = self.query_array_positions_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "array_positions") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_array_element_transform_expression = self.query_array_element_transform_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "array_append") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_cat") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_remove") and
            appParityTokensHaveFunctionCall(sql_tokens, "array_replace") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_array_to_string_expression = self.query_array_to_string_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "array_to_string") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_string_to_array_expression = self.query_string_to_array_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "string_to_array") and
            (sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or sql_adapter.planHasNonZeroToken(entry.plan, ":expr_arr=")));
        self.query_starts_with_expression = self.query_starts_with_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "starts_with") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_ends_with_expression = self.query_ends_with_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "ends_with") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_ascii_chr_expression = self.query_ascii_chr_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "ascii") and
            appParityTokensHaveFunctionCall(sql_tokens, "chr") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_modulo_expression = self.query_modulo_expression or (entry.family == .query and
            appParityTokensHaveKind(sql_tokens, .percent) and
            appParityTokensHaveFunctionCall(sql_tokens, "mod") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_trunc_expression = self.query_date_trunc_expression or (entry.family == .query and
            appParityTokensHaveFunctionCallWithLiteral(sql_tokens, "date_trunc", "hour") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_bin_expression = self.query_date_bin_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "date_bin") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_typed_datetime_literal_expression = self.query_typed_datetime_literal_expression or (entry.family == .query and
            appParityTokensHaveIdentifier(sql_tokens, "timestamptz") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_date_part_expression = self.query_date_part_expression or (entry.family == .query and
            appParityTokensHaveFunctionCallWithLiteral(sql_tokens, "date_part", "hour") and
            appParityTokensHaveFunctionCall(sql_tokens, "extract") and
            appParityTokensHaveIdentifier(sql_tokens, "dow") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_part_epoch_expression = self.query_date_part_epoch_expression or (entry.family == .query and
            appParityTokensHaveFunctionCallWithLiteral(sql_tokens, "date_part", "epoch") and
            appParityTokensHaveFunctionCall(sql_tokens, "extract") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nested_case_fold_text_expression = self.query_nested_case_fold_text_expression or (entry.family == .query and
            appParityTokensHaveFunctionCall(sql_tokens, "lower") and
            appParityTokensHaveFunctionCall(sql_tokens, "upper") and
            appParityTokensHaveKind(sql_tokens, .pipe_concat) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.cte_stream = self.cte_stream or uses_cte_stream;

        for (entry.returning_rows) |row_json| {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
            parsed.deinit();
            self.deterministic_returning_rows += 1;
        }
        if (entry.returning_rows.len > 0) {
            switch (entry.family) {
                .insert => self.deterministic_insert_returning_rows = true,
                .update => self.deterministic_update_returning_rows = true,
                .delete => self.deterministic_delete_returning_rows = true,
                else => {},
            }
        }
        if (uses_returning_all) {
            switch (entry.family) {
                .insert => self.returning_all_insert = true,
                .update => self.returning_all_update = true,
                .delete => self.returning_all_delete = true,
                .update_source => self.returning_all_update_source = true,
                .delete_source => self.returning_all_delete_source = true,
                .update_joined_source => self.returning_all_update_joined_source = true,
                .delete_joined_source => self.returning_all_delete_joined_source = true,
                else => {},
            }
        }
        if (uses_insert_conflict) {
            self.conflict_do_update = self.conflict_do_update or (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .update }) and sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_default_update = self.conflict_default_update or (appParityTokensHaveKeyword(sql_tokens, .set) and
                appParityTokensHaveIdentifier(sql_tokens, "status") and
                appParityTokensHaveKeyword(sql_tokens, .default) and
                sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_coalesce_existing_update = self.conflict_coalesce_existing_update or
                appParityTokensHaveFunctionCall(sql_tokens, "coalesce") and
                    appParityTokensHaveIdentifierPrefix(sql_tokens, "excluded.");
            self.conflict_numeric_expression_update = self.conflict_numeric_expression_update or
                appParityTokensHaveFunctionCall(sql_tokens, "greatest") and
                    appParityTokensHaveIdentifier(sql_tokens, "amount") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.amount");
            self.conflict_case_expression_update = self.conflict_case_expression_update or
                appParityTokensHaveKeyword(sql_tokens, .case) and
                    appParityTokensHaveKeyword(sql_tokens, .when) and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.amount") and
                    appParityTokensHaveIdentifier(sql_tokens, "amount");
            self.conflict_current_timestamp_precision = self.conflict_current_timestamp_precision or
                appParityTokensHaveFunctionCall(sql_tokens, "current_timestamp");
            self.conflict_current_date_update = self.conflict_current_date_update or
                appParityTokensHaveIdentifier(sql_tokens, "current_date");
            self.conflict_uuid_generation_update = self.conflict_uuid_generation_update or
                appParityTokensHaveFunctionCall(sql_tokens, "uuid_generate_v4") or
                appParityTokensHaveFunctionCall(sql_tokens, "gen_random_uuid");
            self.conflict_text_expression_update = self.conflict_text_expression_update or
                (appParityTokensHaveIdentifier(sql_tokens, "excluded.next_status") and
                    (appParityTokensHaveFunctionCall(sql_tokens, "length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "char_length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "character_length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "octet_length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "bit_length")));
            self.conflict_octet_length_expression_update = self.conflict_octet_length_expression_update or
                appParityTokensHaveIdentifier(sql_tokens, "excluded.next_status") and
                    appParityTokensHaveFunctionCall(sql_tokens, "octet_length");
            self.conflict_bit_length_expression_update = self.conflict_bit_length_expression_update or
                appParityTokensHaveIdentifier(sql_tokens, "excluded.next_status") and
                    appParityTokensHaveFunctionCall(sql_tokens, "bit_length");
            self.conflict_regexp_replace_expression_update = self.conflict_regexp_replace_expression_update or
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_replace") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_match_expression_update = self.conflict_regexp_match_expression_update or
                (appParityTokensHaveFunctionCall(sql_tokens, "regexp_like") or
                    appParityTokensHaveFunctionCall(sql_tokens, "regexp_match")) and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_count_expression_update = self.conflict_regexp_count_expression_update or
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_instr_expression_update = self.conflict_regexp_instr_expression_update or
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_substr_expression_update = self.conflict_regexp_substr_expression_update or
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_nested_text_expression_update = self.conflict_nested_text_expression_update or
                (appParityTokensHaveIdentifier(sql_tokens, "excluded.next_status") and
                    appParityTokensHaveKind(sql_tokens, .pipe_concat) and
                    appParityTokensHaveFunctionCall(sql_tokens, "lower") and
                    (appParityTokensHaveFunctionCall(sql_tokens, "length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "char_length") or
                        appParityTokensHaveFunctionCall(sql_tokens, "character_length")));
            self.conflict_jsonb_update = self.conflict_jsonb_update or
                appParityTokensHaveIdentifierPrefix(sql_tokens, "jsonb_") or
                appParityTokensHaveFunctionCall(sql_tokens, "to_jsonb") or
                (appParityTokensHaveIdentifier(sql_tokens, "metadata") and
                    appParityTokensHaveKind(sql_tokens, .pipe_concat) and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
            self.conflict_jsonb_concat_update = self.conflict_jsonb_concat_update or
                appParityTokensHaveIdentifier(sql_tokens, "metadata") and
                    appParityTokensHaveKind(sql_tokens, .pipe_concat) and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":ops=");
            self.multi_row_conflict_do_nothing = self.multi_row_conflict_do_nothing or (uses_multi_row_insert and appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .nothing }));
            self.multi_row_conflict_do_nothing_duplicate_target = self.multi_row_conflict_do_nothing_duplicate_target or (uses_multi_row_insert and
                entry.resolver_exists == false and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .nothing }) and
                sql_adapter.writePlanHasCounts(entry.plan, 1, 0));
            self.conflict_returning_expression = self.conflict_returning_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=");
            self.conflict_do_nothing_returning_all = self.conflict_do_nothing_returning_all or (uses_returning_all and
                sql_adapter.writePlanHasCounts(entry.plan, 0, 0) and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .nothing }));
            self.conflict_guard_where = self.conflict_guard_where or uses_conflict_where;
            self.conflict_guard_where_skip = self.conflict_guard_where_skip or (uses_conflict_where and
                sql_adapter.writePlanHasCounts(entry.plan, 0, 0) and
                sql_adapter.planHasExactUsizeToken(entry.plan, ":returning_rows=", 0));
            self.conflict_interval_update = self.conflict_interval_update or
                (appParityTokensHaveIdentifier(sql_tokens, "interval") and
                    appParityTokensHaveStringLiteral(sql_tokens, "1 second"));
            self.conflict_mixed_interval_update = self.conflict_mixed_interval_update or
                (appParityTokensHaveIdentifier(sql_tokens, "interval") and
                    appParityTokensHaveStringLiteral(sql_tokens, "1 month 1 day"));
            self.conflict_date_bin_update = self.conflict_date_bin_update or
                (appParityTokensHaveFunctionCall(sql_tokens, "date_bin") and
                    appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .update, .set }) and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_typed_datetime_literal_update = self.conflict_typed_datetime_literal_update or
                (appParityTokensHaveKeywordSequence(sql_tokens, &.{ .do, .update, .set }) and
                    appParityTokensHaveIdentifier(sql_tokens, "updated_at_ns") and
                    appParityTokensHaveIdentifier(sql_tokens, "timestamptz") and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_row_assignment = self.conflict_row_assignment or
                appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
                    appParityTokensHaveIdentifier(sql_tokens, "status") and
                    appParityTokensHaveIdentifier(sql_tokens, "quantity");
            self.conflict_row_assignment_default = self.conflict_row_assignment_default or
                (appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
                    appParityTokensHaveIdentifier(sql_tokens, "status") and
                    appParityTokensHaveIdentifier(sql_tokens, "quantity") and
                    appParityTokensHaveKeyword(sql_tokens, .default));
            self.conflict_row_assignment_constructor = self.conflict_row_assignment_constructor or
                (appParityTokensHaveKeywordThenKind(sql_tokens, .set, .lparen) and
                    appParityTokensHaveIdentifier(sql_tokens, "status") and
                    appParityTokensHaveIdentifier(sql_tokens, "quantity") and
                    appParityTokensHaveFunctionCall(sql_tokens, "row"));
            self.conflict_boolean_expression_update = self.conflict_boolean_expression_update or
                appParityTokensHaveKeyword(sql_tokens, .set) and
                    appParityTokensHaveIdentifier(sql_tokens, "enabled") and
                    appParityTokensHaveIdentifier(sql_tokens, "excluded.enabled") and
                    appParityTokensHaveKeyword(sql_tokens, .@"or") and
                    appParityTokensHaveKeyword(sql_tokens, .false);
            self.schema_default_primary_named_conflict_target = self.schema_default_primary_named_conflict_target or
                appParityTokensHaveConflictConstraint(sql_tokens, "usage_records_pkey");
            self.schema_custom_primary_named_conflict_target = self.schema_custom_primary_named_conflict_target or
                (setup_summary.renamed_usage_records_primary_constraint and
                    appParityTokensHaveConflictConstraint(sql_tokens, "usage_records_id_pk"));
            self.schema_unique_conflict_target = self.schema_unique_conflict_target or (setup_summary.create_table_email_unique_constraint and
                appParityConflictTargetHasIdentifier(sql_tokens, "email"));
            self.schema_additive_unique_conflict_target = self.schema_additive_unique_conflict_target or (setup_summary.alter_add_email_unique_constraint and
                appParityConflictTargetHasIdentifier(sql_tokens, "email"));
            self.schema_partial_unique_conflict_target = self.schema_partial_unique_conflict_target or (setup_summary.partial_unique_index and
                appParityConflictTargetHasIdentifier(sql_tokens, "email") and
                appParityConflictTargetHasWhere(sql_tokens));
            self.schema_expression_unique_conflict_target = self.schema_expression_unique_conflict_target or (setup_summary.expression_unique_lower_upper and
                (appParityConflictTargetHasFunctionCall(sql_tokens, "lower") or
                    appParityConflictTargetHasFunctionCall(sql_tokens, "upper")));
            self.schema_mixed_expression_unique_conflict_target = self.schema_mixed_expression_unique_conflict_target or (setup_summary.mixed_expression_unique_tenant_lower and
                appParityConflictTargetHasIdentifier(sql_tokens, "tenant_id") and
                appParityConflictTargetHasFunctionCall(sql_tokens, "lower"));
        }
        if (entry.family == .insert and !uses_insert_conflict) {
            self.multi_row_insert = self.multi_row_insert or uses_multi_row_insert;
            self.insert_typed_datetime_literal = self.insert_typed_datetime_literal or
                appParityTokensHaveIdentifier(sql_tokens, "timestamptz");
        }
        self.point_update_expression_partial_unique_selector = self.point_update_expression_partial_unique_selector or
            appParityPointWriteHasExpressionPartialUniqueSelector(entry, sql_tokens, setup_summary, .update);
        self.point_delete_expression_partial_unique_selector = self.point_delete_expression_partial_unique_selector or
            appParityPointWriteHasExpressionPartialUniqueSelector(entry, sql_tokens, setup_summary, .delete);
        self.insert_source_cross_table_source_schema = self.insert_source_cross_table_source_schema or
            (entry.family == .insert_source and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, ":source_table=", "archived_records"));
        self.insert_source_expression_assignment = self.insert_source_expression_assignment or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":assignment_expr="));
        self.insert_source_regexp_expression_assignment = self.insert_source_regexp_expression_assignment or
            (entry.family == .insert_source and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_like") and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_instr") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":assignment_expr=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.insert_source_computed_pattern_source = self.insert_source_computed_pattern_source or
            (entry.family == .insert_source and
                uses_computed_pattern and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred="));
        self.insert_source_expression_or_source = self.insert_source_expression_or_source or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or="));
        self.insert_source_expression_not_source = self.insert_source_expression_not_source or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not="));
        self.insert_source_returning_all_expression = self.insert_source_returning_all_expression or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_all=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.insert_source_conflict_default_update = self.insert_source_conflict_default_update or
            (entry.family == .insert_source and
                appParityTokensHaveKeyword(sql_tokens, .set) and
                appParityTokensHaveIdentifier(sql_tokens, "status") and
                appParityTokensHaveKeyword(sql_tokens, .default) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_ops="));
        self.insert_source_conflict_json_set_expression = self.insert_source_conflict_json_set_expression or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_json_set_expr="));
        self.insert_source_conflict_regexp_expression = self.insert_source_conflict_regexp_expression or
            (entry.family == .insert_source and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_substr") and
                appParityTokensHaveFunctionCall(sql_tokens, "regexp_count") and
                appParityTokensHaveIdentifier(sql_tokens, "excluded.status") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_patch_expr="));
        self.insert_source_conflict_boolean_is_not_guard = self.insert_source_conflict_boolean_is_not_guard or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_where_any=") and
                appParityTokensHaveKeywordSequence(sql_tokens, &.{ .is, .not, .true }));
        self.joined_source_cross_table_source_schema = self.joined_source_cross_table_source_schema or
            ((entry.family == .update_joined_source or entry.family == .delete_joined_source) and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, ":source=", "source_records"));
        self.read_join_cross_table_source_schema = self.read_join_cross_table_source_schema or
            (entry.family == .join and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, ":right=", "customer_records"));
        self.read_lateral_cross_table_source_schema = self.read_lateral_cross_table_source_schema or
            (entry.family == .lateral and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, ":right=", "balance_records"));
        self.read_set_operation_cross_table_source_schema = self.read_set_operation_cross_table_source_schema or
            (entry.family == .read and
                appParityEntryHasCatalogSchemas(entry) and
                setOperationPlanHasRightTable(entry.plan, "archived_records"));
        self.read_set_operation_cross_table_except = self.read_set_operation_cross_table_except or
            (entry.family == .read and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, "set_operation:op=", "except") and
                setOperationPlanHasRightTable(entry.plan, "archived_records"));
        self.read_set_operation_cross_table_intersect = self.read_set_operation_cross_table_intersect or
            (entry.family == .read and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, "set_operation:op=", "intersect") and
                setOperationPlanHasRightTable(entry.plan, "archived_records"));
        self.merge_cross_table_source_schema = self.merge_cross_table_source_schema or
            (entry.family == .merge_mutation and
                appParityEntryHasCatalogSchemas(entry) and
                sql_adapter.planHasExactStringToken(entry.plan, ":source=", "archived_records"));
    }
};

test "sql adapter corpus validates fixture metadata core policy" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(alloc, .{
        .name = "missing table summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .plan = "query:table=usage_records:select=1",
    }));

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(alloc, .{
        .name = "unsupported without reason",
        .sql = "SELECT id FROM usage_records FOR SHARE",
        .family = .unsupported_read,
        .plan = "unsupported:read:requires=lock_mode",
    }));

    try validateFixtureMetadataCore(alloc, .{
        .name = "valid query",
        .sql = "SELECT id FROM usage_records WHERE tenant_id = $1",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:pred=1:select=1",
        .params = &.{.{ .string = "tenant-a" }},
    });

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(alloc, .{
        .name = "returning rows on source write",
        .sql = "INSERT INTO usage_records SELECT * FROM staged RETURNING id",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert_source:table=usage_records:source_table=staged:assignments=1:returning=1:returning_expr=0:returning_all=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }));

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(alloc, .{
        .name = "resolver hint without conflict",
        .sql = "INSERT INTO usage_records (id) VALUES ('u1')",
        .family = .insert,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=0:returning_expr=0",
        .resolver_row_json = "{\"id\":\"u1\"}",
        .resolver_version = 7,
        .resolver_exists = true,
    }));

    try validateFixtureMetadataCore(alloc, .{
        .name = "resolver hint with parsed conflict",
        .sql = "INSERT INTO usage_records (id) VALUES ('u1') ON CONFLICT (id) DO NOTHING",
        .family = .insert,
        .summary = .{ .table_name = "usage_records" },
        .plan = "insert:table=usage_records:writes=0:transforms=0:ops=1:deletes=0:returning_rows=0:returning_expr=0",
        .resolver_row_json = "{\"id\":\"u1\"}",
        .resolver_version = 7,
        .resolver_exists = true,
    });
}

test "sql adapter corpus owns ddl applied-plan fixture policy" {
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create table",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records" },
        .plan = "ddl:create_table:table=usage_records:columns=1:if_not_exists=false:replace=false",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create table if not exists",
        .sql = "CREATE TABLE IF NOT EXISTS usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records" },
        .plan = "ddl:create_table:table=usage_records:columns=1:if_not_exists=true:replace=false",
    }));
    try std.testing.expect(try corpusDdlFixtureRequiresAppliedPlan(.{
        .name = "create index",
        .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records" },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=false:if_not_exists=false",
        .applied_plan = "applied:rebuild=true:validation=true:rewrite=false:building_indexes=1:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0:work_items=2:work=rebuild/table/derived_artifacts,validate/table/constraints",
    }));
    try std.testing.expect(!try corpusDdlFixtureRequiresAppliedPlan(.{
        .name = "set search path",
        .sql = "SET search_path TO public",
        .family = .ddl,
        .summary = .{ .ddl_tag = .set_search_path },
        .plan = "ddl:session:set_search_path:namespaces=1:local=false",
    }));
}

test "sql adapter corpus placeholder coverage ignores literals and comments" {
    const alloc = std.testing.allocator;

    var contiguous = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $2");
    defer contiguous.deinit(alloc);
    try std.testing.expect(sqlParameterCoverageMatchesParsedSql(&contiguous, 2));

    var gap = try tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $3");
    defer gap.deinit(alloc);
    try std.testing.expect(!sqlParameterCoverageMatchesParsedSql(&gap, 3));

    try std.testing.expectError(error.UnsupportedSqlShape, tokenized.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE tenant_id = $1abc"));

    var ignored = try tokenized.ParsedSql.initAlloc(alloc, "SELECT '$1', $$ $2 $$, id FROM usage_records -- $3abc\nWHERE tenant_id = $1");
    defer ignored.deinit(alloc);
    try std.testing.expect(sqlParameterCoverageMatchesParsedSql(&ignored, 1));
}
