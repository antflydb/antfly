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
// Elastic License for the specific language governing permissions and limitations
// under the License.

const std = @import("std");
const db_mod = @import("../db/mod.zig");
const metadata_table_ddl = @import("../../metadata/catalog/table_ddl.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const raft_mod = @import("../../raft/mod.zig");
const storage_schema = @import("../schema.zig");
const catalog_resources = @import("../../metadata/catalog/resources.zig");
const sql_catalog_source = @import("../../sql/catalog_source.zig");
const row_execution_contract = @import("../../query/row_execution_contract.zig");
const sql_binder = @import("../../sql/binder.zig");
const sql_plan = @import("../../sql/plan.zig");
const lite_sql_source = @import("../../api/lite_sql_source.zig");
const lite_sql_value_ref = @import("../../api/lite_sql_value_ref.zig");
const table_reads = @import("../../api/table_reads.zig");
const table_writes = @import("../../api/table_writes.zig");

pub const DbSource = struct {
    db: *db_mod.DB,
    reads: table_reads.BoundTableReadSource,
    writes: table_writes.BoundTableWriteSource,

    pub fn init(db: *db_mod.DB) DbSource {
        return .{
            .db = db,
            .reads = table_reads.BoundTableReadSource.init("", 1, db, raft_mod.read_gate.noopReadableLeaseRequester()),
            .writes = table_writes.BoundTableWriteSource.init("", db),
        };
    }

    pub fn source(self: *@This()) lite_sql_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .load_stored_table = loadStoredTable,
                .load_table = loadTable,
                .apply_table_ddl_plan = applyTableDdlPlan,
                .apply_table = applyTable,
                .rebuild_secondary_index = rebuildSecondaryIndex,
                .lookup = lookup,
                .query_json = queryJson,
                .rows_query_plan = rowsQueryPlan,
                .batch_rows = batchRows,
                .build_insert_source_batch = buildInsertSourceBatch,
                .execute_read_plan = executeReadPlan,
            },
        };
    }

    fn tableReads(ptr: *anyopaque, table_name: []const u8) table_reads.TableReadSource {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.reads.table_name = table_name;
        return self.reads.source();
    }

    fn tableWrites(ptr: *anyopaque, table_name: []const u8) table_writes.TableWriteSource {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.writes.table_name = table_name;
        return self.writes.source();
    }

    fn loadStoredTable(ptr: *anyopaque, allocator: std.mem.Allocator) !?metadata_table_manager.TableRecord {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.db.getLiteSqlTableRecordAlloc(allocator);
    }

    fn loadTable(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        database_name: []const u8,
        namespace_name: []const u8,
    ) !?metadata_table_manager.TableRecord {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (try self.db.getLiteSqlTableRecordAlloc(allocator)) |table| return table;
        const schema_json = (try self.db.getSchemaJson(allocator)) orelse return null;
        defer allocator.free(schema_json);
        return try metadata_table_manager.cloneTable(allocator, .{
            .table_id = 1,
            .name = table_name,
            .database_name = database_name,
            .namespace_name = namespace_name,
            .placement_role = "data",
            .desired_replica_count = 1,
            .schema_json = schema_json,
        });
    }

    fn applyTable(ptr: *anyopaque, allocator: std.mem.Allocator, table: metadata_table_manager.TableRecord) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.db.applyLiteSqlTableRecord(allocator, table);
    }

    fn applyTableDdlPlan(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        plan_ref: lite_sql_value_ref.Ref,
        session: catalog_resources.SqlCatalogSession,
        out_result_ref: lite_sql_value_ref.OutRef,
    ) !void {
        const plan = @constCast(try plan_ref.cast(sql_binder.TableDdlLogicalPlan, .table_ddl));
        const out_result = try out_result_ref.cast(metadata_table_ddl.AppliedRelationalSqlDdlRecord, .ddl_result);
        var target = try metadata_table_ddl.relationalSqlDdlTargetForTablePlanWithSessionAlloc(allocator, plan.*, session);
        defer target.deinit(allocator);
        if (target.table_name.len == 0) return error.UnsupportedSqlShape;

        const existing_table = try loadTable(
            ptr,
            allocator,
            target.table_name,
            target.database_name,
            target.namespace_name,
        );
        defer if (existing_table) |table| metadata_table_manager.freeTable(allocator, table);
        const base_table = if (existing_table) |table| table else metadata_table_manager.TableRecord{
            .table_id = 1,
            .name = target.table_name,
            .database_name = target.database_name,
            .namespace_name = target.namespace_name,
            .placement_role = "data",
            .desired_replica_count = 1,
        };

        var applied = try metadata_table_ddl.applyTableDdlPlanToTableRecordWithSessionAlloc(
            allocator,
            &base_table,
            plan,
            session,
        );
        errdefer applied.deinit(allocator);
        try applyTable(ptr, allocator, applied.table);
        out_result.* = applied;
    }

    fn rebuildSecondaryIndex(ptr: *anyopaque, index_name: []const u8, generation: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = try self.db.rebuildRelationalSecondaryIndexInRange(index_name, generation, "", "");
    }

    fn lookup(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
    ) !?lite_sql_source.LookupResult {
        const result = (try tableReads(ptr, table_name).lookup(allocator, table_name, key, .{}, .read_index)) orelse return null;
        return .{
            .json = result.json,
            .version = result.version,
        };
    }

    fn queryJson(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        request_ref: lite_sql_value_ref.Ref,
    ) !?[]u8 {
        const request = (try request_ref.cast(db_mod.types.SearchRequest, .search_request)).*;
        const result = (try tableReads(ptr, table_name).query(allocator, table_name, request, .read_index)) orelse return null;
        return result.json;
    }

    fn rowsQueryPlan(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        schema_ref: lite_sql_value_ref.Ref,
        plan_ref: lite_sql_value_ref.Ref,
        out_result_ref: lite_sql_value_ref.OutRef,
    ) !bool {
        const schema = (try schema_ref.cast(storage_schema.TableSchema, .table_schema)).*;
        const plan = (try plan_ref.cast(db_mod.types.RelationalRowsQueryPlan, .rows_query_plan)).*;
        const out_result = try out_result_ref.cast(db_mod.types.RelationalRowsQueryResult, .rows_query_result);
        out_result.* = (try tableReads(ptr, table_name).rowsQueryPlan(allocator, table_name, schema, plan, .read_index)) orelse return false;
        return true;
    }

    fn batchRows(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        request_ref: lite_sql_value_ref.Ref,
    ) !bool {
        const request = (try request_ref.cast(db_mod.types.BatchRequest, .batch_request)).*;
        _ = (try tableWrites(ptr, table_name).batch(allocator, table_name, request)) orelse return false;
        return true;
    }

    fn buildInsertSourceBatch(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        source_table_name: []const u8,
        target_schema_ref: lite_sql_value_ref.Ref,
        source_schema_ref: lite_sql_value_ref.Ref,
        lowered_ref: lite_sql_value_ref.Ref,
        conflict_resolver_ref: ?lite_sql_value_ref.Ref,
        default_context_ref: lite_sql_value_ref.Ref,
        out_result_ref: lite_sql_value_ref.OutRef,
    ) !bool {
        const target_schema = (try target_schema_ref.cast(storage_schema.TableSchema, .table_schema)).*;
        const source_schema = (try source_schema_ref.cast(storage_schema.TableSchema, .table_schema)).*;
        const lowered = (try lowered_ref.cast(sql_plan.LoweredInsertSource, .insert_source)).*;
        const conflict_resolver = if (conflict_resolver_ref) |ref|
            (try ref.cast(row_execution_contract.UniqueSelectorResolver, .unique_selector_resolver)).*
        else
            null;
        const default_context = (try default_context_ref.cast(row_execution_contract.DefaultValueContext, .default_value_context)).*;
        const out_result = try out_result_ref.cast(row_execution_contract.OwnedRowsBatchRequest, .rows_batch_result);
        const self: *@This() = @ptrCast(@alignCast(ptr));
        out_result.* = (try table_reads.rowsLoweredInsertSourceBatchFromRoutedScansWithSchemasAndDefaultContextAlloc(
            allocator,
            tableReads(self, source_table_name),
            source_table_name,
            target_schema,
            source_schema,
            lowered,
            .read_index,
            conflict_resolver,
            default_context,
        )) orelse return false;
        return true;
    }

    fn executeReadPlan(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table_name: []const u8,
        session: catalog_resources.SqlCatalogSession,
        schema_ref: lite_sql_value_ref.Ref,
        plan_ref: lite_sql_value_ref.Ref,
        out_result_ref: lite_sql_value_ref.OutRef,
    ) !bool {
        const schema = (try schema_ref.cast(storage_schema.TableSchema, .table_schema)).*;
        const plan = (try plan_ref.cast(sql_plan.LoweredReadPlan, .read_plan)).*;
        const out_result = try out_result_ref.cast(table_reads.LoweredSqlReadPlanResult, .read_plan_result);
        const table = (try loadTable(
            ptr,
            allocator,
            table_name,
            session.currentDatabase(),
            session.primarySearchPathNamespace(),
        )) orelse return false;
        defer metadata_table_manager.freeTable(allocator, table);

        var catalog = LiteExecutionCatalog{ .table = table };
        out_result.* = (try table_reads.executeLoweredSqlReadPlanWithSessionAlloc(
            allocator,
            tableReads(ptr, table_name),
            catalog.iface(),
            session,
            table_name,
            schema,
            plan,
            .read_index,
        )) orelse return false;
        return true;
    }
};

const LiteExecutionCatalog = struct {
    table: metadata_table_manager.TableRecord,

    fn iface(self: *@This()) sql_catalog_source.SqlCatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .snapshot_alloc = snapshotAlloc,
                .free_snapshot = freeSnapshot,
            },
        };
    }

    fn snapshotAlloc(ptr: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator) !sql_catalog_source.SqlCatalogSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
        };
    }

    fn freeSnapshot(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator, snapshot: *sql_catalog_source.SqlCatalogSnapshot) void {
        snapshot.* = undefined;
    }
};
