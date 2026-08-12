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

//! Storage-owned operations required by the API-owned Lite SQL runtime.
//! Keep this contract free of concrete DB and SQL planner implementations.

const std = @import("std");
const table_record = @import("../metadata/catalog/table_record.zig");
const catalog_resources = @import("../metadata/catalog/resources.zig");
const value_ref = @import("lite_sql_value_ref.zig");

pub const LookupResult = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        load_stored_table: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!?table_record.TableRecord,
        load_table: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
        ) anyerror!?table_record.TableRecord,
        apply_table_ddl_plan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            plan: value_ref.Ref,
            session: catalog_resources.SqlCatalogSession,
            out_result: value_ref.OutRef,
        ) anyerror!void,
        apply_table: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table: table_record.TableRecord) anyerror!void,
        rebuild_secondary_index: *const fn (ptr: *anyopaque, index_name: []const u8, generation: u64) anyerror!void,
        lookup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
        ) anyerror!?LookupResult,
        query_json: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            request: value_ref.Ref,
        ) anyerror!?[]u8,
        rows_query_plan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            schema: value_ref.Ref,
            plan: value_ref.Ref,
            out_result: value_ref.OutRef,
        ) anyerror!bool,
        batch_rows: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            request: value_ref.Ref,
        ) anyerror!bool,
        build_insert_source_batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            source_table_name: []const u8,
            target_schema: value_ref.Ref,
            source_schema: value_ref.Ref,
            lowered: value_ref.Ref,
            conflict_resolver: ?value_ref.Ref,
            default_context: value_ref.Ref,
            out_result: value_ref.OutRef,
        ) anyerror!bool,
        execute_read_plan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            session: catalog_resources.SqlCatalogSession,
            schema: value_ref.Ref,
            plan: value_ref.Ref,
            out_result: value_ref.OutRef,
        ) anyerror!bool,
    };

    pub fn loadStoredTableAlloc(self: Source, alloc: std.mem.Allocator) !?table_record.TableRecord {
        return try self.vtable.load_stored_table(self.ptr, alloc);
    }

    pub fn loadTableAlloc(
        self: Source,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        database_name: []const u8,
        namespace_name: []const u8,
    ) !?table_record.TableRecord {
        return try self.vtable.load_table(self.ptr, alloc, table_name, database_name, namespace_name);
    }

    pub fn applyTable(self: Source, alloc: std.mem.Allocator, table: table_record.TableRecord) !void {
        try self.vtable.apply_table(self.ptr, alloc, table);
    }

    pub fn applyTableDdlPlan(
        self: Source,
        alloc: std.mem.Allocator,
        plan: value_ref.Ref,
        session: catalog_resources.SqlCatalogSession,
        out_result: value_ref.OutRef,
    ) !void {
        try self.vtable.apply_table_ddl_plan(self.ptr, alloc, plan, session, out_result);
    }

    pub fn rebuildSecondaryIndex(self: Source, index_name: []const u8, generation: u64) !void {
        try self.vtable.rebuild_secondary_index(self.ptr, index_name, generation);
    }

    pub fn lookupAlloc(self: Source, alloc: std.mem.Allocator, table_name: []const u8, key: []const u8) !?LookupResult {
        return try self.vtable.lookup(self.ptr, alloc, table_name, key);
    }

    pub fn queryJsonAlloc(
        self: Source,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        request: value_ref.Ref,
    ) !?[]u8 {
        return try self.vtable.query_json(self.ptr, alloc, table_name, request);
    }

    pub fn rowsQueryPlanAlloc(
        self: Source,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: value_ref.Ref,
        plan: value_ref.Ref,
        out_result: value_ref.OutRef,
    ) !bool {
        return try self.vtable.rows_query_plan(self.ptr, alloc, table_name, schema, plan, out_result);
    }

    pub fn batchRows(self: Source, alloc: std.mem.Allocator, table_name: []const u8, request: value_ref.Ref) !bool {
        return try self.vtable.batch_rows(self.ptr, alloc, table_name, request);
    }

    pub fn buildInsertSourceBatchAlloc(
        self: Source,
        alloc: std.mem.Allocator,
        source_table_name: []const u8,
        target_schema: value_ref.Ref,
        source_schema: value_ref.Ref,
        lowered: value_ref.Ref,
        conflict_resolver: ?value_ref.Ref,
        default_context: value_ref.Ref,
        out_result: value_ref.OutRef,
    ) !bool {
        return try self.vtable.build_insert_source_batch(
            self.ptr,
            alloc,
            source_table_name,
            target_schema,
            source_schema,
            lowered,
            conflict_resolver,
            default_context,
            out_result,
        );
    }

    pub fn executeReadPlanAlloc(
        self: Source,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        session: catalog_resources.SqlCatalogSession,
        schema: value_ref.Ref,
        plan: value_ref.Ref,
        out_result: value_ref.OutRef,
    ) !bool {
        return try self.vtable.execute_read_plan(self.ptr, alloc, table_name, session, schema, plan, out_result);
    }
};
