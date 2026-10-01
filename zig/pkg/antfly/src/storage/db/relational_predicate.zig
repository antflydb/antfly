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

//! One typed comparison contract for row filters and CHECK evaluation.
//! The caller, not the comparator, chooses UNKNOWN semantics: WHERE admits
//! only TRUE; CHECK rejects only FALSE. No JSON-number or string coercion.
const std = @import("std");
const schema = @import("../schema.zig");
const native = @import("../relational_index.zig");
const tuples = @import("relational_index_keys.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;

pub const Truth = enum {
    yes,
    no,
    unknown,

    pub fn matches(self: Truth) bool {
        return self == .yes;
    }
    pub fn satisfiesCheck(self: Truth) bool {
        return self != .no;
    }
};

pub const Condition = struct {
    column: []const u8,
    op: native.RelationalCheckOp,
    value: tuples.Value = .null,
    collation: ?[]const u8 = null,
};

pub const Plan = struct {
    tuple: tuples.TuplePlan,
    operand: []u8,
    op: native.RelationalCheckOp,
    operand_null: bool,

    /// The caller retains the schema/layout. All request data is compiled or
    /// copied here and may be released immediately after this call returns.
    pub fn init(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, condition: Condition) !Plan {
        if ((condition.op == .is_null or condition.op == .is_not_null) and condition.value != .null)
            return error.InvalidRelationalPredicate;
        var tuple = try tuples.TuplePlan.init(alloc, table, layout, &.{.{ .column = condition.column, .collation = condition.collation }});
        errdefer tuple.deinit();
        var operand = std.ArrayList(u8).empty;
        defer operand.deinit(alloc);
        const operand_null = try tuple.appendValues(alloc, &operand, &.{condition.value});
        return .{ .tuple = tuple, .operand = try operand.toOwnedSlice(alloc), .op = condition.op, .operand_null = operand_null };
    }

    pub fn deinit(self: *Plan) void {
        self.tuple.alloc.free(self.operand);
        self.tuple.deinit();
        self.* = undefined;
    }

    pub fn evaluateValue(self: *const Plan, alloc: Allocator, scratch: *std.ArrayList(u8), value: tuples.Value) !Truth {
        scratch.clearRetainingCapacity();
        const is_null = try self.tuple.appendValues(alloc, scratch, &.{value});
        return self.compare(scratch.items, is_null);
    }

    /// A column absent from an older layout is SQL NULL, not an accidentally
    /// rebound ordinal. Type changes cannot silently alter comparison rules.
    pub fn projectSource(self: *const Plan, alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout) !Source {
        return .{ .plan = self, .layout = layout, .columns = table.relational_columns, .tuple = self.tuple.projectSource(alloc, table, layout) catch |err| switch (err) {
            error.RelationalIndexColumnNotFound => null,
            else => return err,
        } };
    }

    fn compare(self: *const Plan, left: []const u8, left_null: bool) Truth {
        if (self.op == .is_null) return truth(left_null);
        if (self.op == .is_not_null) return truth(!left_null);
        if (left_null or self.operand_null) return switch (self.op) {
            .is_distinct => truth(left_null != self.operand_null),
            .is_not_distinct => truth(left_null == self.operand_null),
            else => .unknown,
        };
        const order = std.mem.order(u8, left, self.operand);
        return truth(switch (self.op) {
            .eq, .is_not_distinct => order == .eq,
            .ne, .is_distinct => order != .eq,
            .gt => order == .gt,
            .gte => order != .lt,
            .lt => order == .lt,
            .lte => order != .gt,
            .is_null, .is_not_null => unreachable,
        });
    }
};

pub const Source = struct {
    plan: *const Plan,
    tuple: ?tuples.TuplePlan,
    layout: *const codec.PhysicalLayout,
    columns: []const schema.RelationalColumn,

    pub fn deinit(self: *Source) void {
        if (self.tuple) |*tuple| tuple.deinit();
        self.* = undefined;
    }

    /// Reuses the same scratch across predicates and rows. Result bytes are
    /// transient; the source plan and row epoch remain pinned by the caller.
    pub fn evaluate(self: Source, alloc: Allocator, scratch: *std.ArrayList(u8), row: codec.OrdinalRowView) !Truth {
        if (row.layout != self.layout or row.table_schema.relational_columns.ptr != self.columns.ptr or row.table_schema.relational_columns.len != self.columns.len)
            return error.RelationalRowSchemaMismatch;
        scratch.clearRetainingCapacity();
        const has_null = if (self.tuple) |tuple| try tuple.append(alloc, scratch, row) else true;
        return self.plan.compare(scratch.items, has_null);
    }
};

fn truth(value: bool) Truth {
    return if (value) .yes else .no;
}

test "relational predicate exact integers null truth and historical absence" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer, .allows_null = true },
    };
    const table = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    const encoded = try codec.serializeOrdinal(alloc, 1, &columns, &.{.{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9007199254740993 } }}, @splat(0));
    defer alloc.free(encoded);
    const row = try codec.ordinalRowView(encoded, table, &layout);
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(alloc);
    var plan = try Plan.init(alloc, table, &layout, .{ .column = "id", .op = .gt, .value = .{ .integer = 9007199254740992 } });
    defer plan.deinit();
    var source = try plan.projectSource(alloc, table, &layout);
    defer source.deinit();
    try std.testing.expectEqual(Truth.yes, try source.evaluate(alloc, &scratch, row));
    try std.testing.expectError(error.InvalidRelationalIndexBound, Plan.init(alloc, table, &layout, .{ .column = "id", .op = .eq, .value = .{ .number = 9007199254740992 } }));
    const missing = try codec.serializeOrdinal(alloc, 1, &columns, &.{}, @splat(0));
    defer alloc.free(missing);
    const missing_row = try codec.ordinalRowView(missing, table, &layout);
    const unknown = try source.evaluate(alloc, &scratch, missing_row);
    try std.testing.expectEqual(Truth.unknown, unknown);
    try std.testing.expect(!unknown.matches());
    try std.testing.expect(unknown.satisfiesCheck());
    plan.op = .is_distinct;
    try std.testing.expectEqual(Truth.yes, try source.evaluate(alloc, &scratch, missing_row));
    plan.op = .is_not_distinct;
    try std.testing.expectEqual(Truth.no, try source.evaluate(alloc, &scratch, missing_row));
    plan.op = .is_null;
    try std.testing.expectEqual(Truth.yes, try source.evaluate(alloc, &scratch, missing_row));
    const old_columns = [_]schema.RelationalColumn{.{ .name = "old", .path = "old", .column_type = .integer }};
    const old_table = schema.TableSchema{ .version = 2, .storage_mode = .relational, .relational_columns = &old_columns };
    var old_layout = try codec.PhysicalLayout.init(alloc, old_table);
    defer old_layout.deinit();
    var historical = try plan.projectSource(alloc, old_table, &old_layout);
    defer historical.deinit();
    try std.testing.expect(historical.tuple == null);
    try std.testing.expectError(error.RelationalRowSchemaMismatch, historical.evaluate(alloc, &scratch, row));
}
