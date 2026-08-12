// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license

//! Owned, data-only result of applying a SQL DDL plan.

const std = @import("std");
const ddl_plan = @import("ddl_plan.zig");
const table_record = @import("../metadata/catalog/table_record.zig");

pub const AppliedRelationalSqlDdlRecord = struct {
    table: table_record.TableRecord,
    created_table: bool = false,
    dropped_table: bool = false,
    created_database: bool = false,
    dropped_database: bool = false,
    created_namespace: bool = false,
    renamed_namespace: bool = false,
    dropped_namespace: bool = false,
    created_tablespace: bool = false,
    renamed_tablespace: bool = false,
    dropped_tablespace: bool = false,
    created_sequence: bool = false,
    altered_sequence: bool = false,
    dropped_sequence: bool = false,
    noop: bool = false,
    requires_rebuild: bool = false,
    validation_required: bool = false,
    rewrite_required: bool = false,
    work_items: []const ddl_plan.AppliedDdlWorkItem = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        table_record.freeTable(alloc, self.table);
        if (self.work_items.len > 0) {
            for (self.work_items) |item| {
                var mutable = item;
                mutable.deinit(alloc);
            }
            alloc.free(self.work_items);
        }
        self.* = undefined;
    }
};

pub fn emptyAppliedRelationalSqlDdlRecordAlloc(alloc: std.mem.Allocator) !AppliedRelationalSqlDdlRecord {
    return .{
        .table = try table_record.cloneTable(alloc, .{
            .table_id = 0,
            .name = "",
        }),
    };
}
