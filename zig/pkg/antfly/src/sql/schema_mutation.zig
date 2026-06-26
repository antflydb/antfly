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

const binder = @import("binder.zig");
const ddl_plan = @import("ddl.zig");
const lower_expr = @import("lower_expr.zig");
const runtime_schema = @import("../storage/schema.zig");
const value_mod = @import("value.zig");

pub fn alterRelationalColumnTypeAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: ddl_plan.AlterColumnTypeOperation,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, operation.column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].generated != null) return error.UnsupportedSqlShape;
    if (operation.collation != null and !binder.relationalFieldTypeSupportsCollation(operation.field_type)) return error.UnsupportedSqlShape;
    if (!binder.relationalFieldTypeSupportsCollation(operation.field_type) and columns[index].collation != null) return error.UnsupportedSqlShape;
    const new_array_item_type = if (operation.field_type == .array) operation.array_item_type orelse return error.InvalidSqlCatalog else null;
    const new_collation = if (operation.collation) |collation| try alloc.dupe(u8, collation) else null;
    columns[index].field_type = operation.field_type;
    columns[index].array_item_type = new_array_item_type;
    if (new_collation) |collation| {
        if (columns[index].collation) |existing| alloc.free(existing);
        columns[index].collation = collation;
    }
    if (columns[index].default_value) |default_value| try value_mod.validateDefaultValueForColumnAlloc(alloc, columns[index], default_value);
    if (columns[index].on_update_value) |on_update_value| try value_mod.validateDefaultValueForColumnAlloc(alloc, columns[index], on_update_value);
    try lower_expr.validateRelationalColumnCatalog(schema.relational_columns);
    try binder.validateRelationalPeriodCatalog(schema.relational_columns, schema.periods);
    if (schema.primary_key) |primary_key| {
        try lower_expr.validatePrimaryKeyColumns(schema.relational_columns, primary_key);
        try binder.validatePrimaryKeyTemporalCatalog(schema.periods, primary_key);
        if (primary_key.name) |name| {
            if (binder.uniqueConstraintNameExists(schema.unique_constraints, name) or
                binder.foreignKeyNameExists(schema.foreign_keys, name) or
                binder.relationalCheckNameExists(schema.checks, name))
            {
                return error.InvalidSqlCatalog;
            }
        }
    }
    try lower_expr.validateUniqueConstraintCatalog(schema.relational_columns, schema.periods, schema.unique_constraints);
    try lower_expr.validateForeignKeyCatalog(schema.relational_columns, schema.periods, schema.foreign_keys);
    try lower_expr.validateRelationalCheckCatalog(schema.relational_columns, schema.checks);
}
