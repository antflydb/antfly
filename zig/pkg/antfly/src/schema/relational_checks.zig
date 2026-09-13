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
const wire = @import("antfly_schema_openapi");
const schema = @import("../storage/schema.zig");
const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
const predicate = @import("../storage/db/relational_predicate.zig");
const tuples = @import("../storage/db/relational_index_keys.zig");
const impl = @import("table_schema_impl.zig");
const Allocator = std.mem.Allocator;

pub fn valueFromJson(alloc: Allocator, kind: schema.RelationalColumnType, value: std.json.Value, literal: bool) !tuples.Value {
    if (value == .null) return .null;
    return switch (kind) {
        .string => if (value == .string) .{ .string = value.string } else error.InvalidBatchRequest,
        .integer => .{ .integer = if (literal and value == .string)
            std.fmt.parseInt(i64, value.string, 10) catch return error.InvalidBatchRequest
        else
            impl.documentIntegerToI64(value) orelse return error.InvalidBatchRequest },
        .number => .{ .number = impl.documentNumberToF64(value) orelse return error.InvalidBatchRequest },
        .boolean => if (value == .bool) .{ .boolean = value.bool } else error.InvalidBatchRequest,
        .datetime => .{ .datetime = impl.documentDateTimeToNs(value) orelse return error.InvalidBatchRequest },
        .blob => blk: {
            if (value != .string) return error.InvalidBatchRequest;
            const decoder = std.base64.standard.Decoder;
            const size = decoder.calcSizeForSlice(value.string) catch return error.InvalidBatchRequest;
            const decoded = try alloc.alloc(u8, size);
            errdefer alloc.free(decoded);
            decoder.decode(decoded, value.string) catch return error.InvalidBatchRequest;
            break :blk .{ .blob = decoded };
        },
        else => error.UnsupportedRelationalIndexColumn,
    };
}

fn compile(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, definition: wire.RelationalCheckConstraint) !predicate.Plan {
    const ordinal = layout.ordinalForName(table.relational_columns, definition.column) orelse return error.RelationalIndexColumnNotFound;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const value = try valueFromJson(arena.allocator(), table.relational_columns[ordinal].column_type, definition.value orelse .null, true);
    return try predicate.Plan.init(alloc, table, layout, .{
        .column = definition.column,
        .op = switch (definition.op) {
            inline else => |op| @field(@import("../storage/relational_index.zig").RelationalCheckOp, @tagName(op)),
        },
        .value = value,
        .collation = definition.collation,
    });
}

pub fn validateDefinitions(alloc: Allocator, table: schema.TableSchema, definitions: []const wire.RelationalCheckConstraint) !void {
    if (definitions.len == 0) return;
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    for (definitions) |definition| {
        var plan = try compile(alloc, table, &layout, definition);
        plan.deinit();
    }
}

/// Heap-stable compiled data owned by a public-schema epoch. The wire schema
/// supplies names; the reduced runtime schema supplies exact physical types.
pub const Set = struct {
    alloc: Allocator,
    table: schema.TableSchema,
    layout: codec.PhysicalLayout,
    definitions: []const wire.RelationalCheckConstraint,
    plans: []predicate.Plan,

    /// Takes the runtime schema only on success. Definitions remain borrowed
    /// from the same public validator and are released after this set.
    pub fn createOwned(alloc: Allocator, table: schema.TableSchema, definitions: []const wire.RelationalCheckConstraint) !*Set {
        const set = try alloc.create(Set);
        errdefer alloc.destroy(set);
        set.* = .{ .alloc = alloc, .table = table, .layout = try codec.PhysicalLayout.init(alloc, table), .definitions = definitions, .plans = undefined };
        errdefer set.layout.deinit();
        set.plans = try alloc.alloc(predicate.Plan, definitions.len);
        errdefer alloc.free(set.plans);
        var initialized: usize = 0;
        errdefer for (set.plans[0..initialized]) |*plan| plan.deinit();
        for (definitions, set.plans) |definition, *plan| {
            plan.* = try compile(alloc, table, &set.layout, definition);
            initialized += 1;
        }
        return set;
    }

    pub fn deinit(self: *Set) void {
        for (self.plans) |*plan| plan.deinit();
        self.alloc.free(self.plans);
        self.layout.deinit();
        schema.freeSchema(self.alloc, self.table);
        self.alloc.destroy(self);
    }

    pub fn firstViolationJson(self: *const Set, alloc: Allocator, value: std.json.Value) !?usize {
        if (value != .object) return error.InvalidBatchRequest;
        var scratch = std.ArrayList(u8).empty;
        defer scratch.deinit(alloc);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        for (self.definitions, self.plans, 0..) |definition, *plan, i| {
            const ordinal = plan.tuple.keys[0].ordinal;
            const scalar = try valueFromJson(arena.allocator(), self.table.relational_columns[ordinal].column_type, value.object.get(definition.column) orelse .null, false);
            if (!(try plan.evaluateValue(alloc, &scratch, scalar)).satisfiesCheck()) return i;
        }
        return null;
    }

    /// Cold validation reads selected ordinal cells only. No complete JSON
    /// materialization, reparse, or re-encoding of unrelated columns.
    pub fn firstViolationRow(self: *const Set, alloc: Allocator, row: codec.OrdinalRowView) !?usize {
        var scratch = std.ArrayList(u8).empty;
        defer scratch.deinit(alloc);
        for (self.definitions, self.plans, 0..) |definition, *plan, i| {
            const scalar: tuples.Value = scalar: {
                const ordinal = row.ordinalForName(definition.column) orelse break :scalar .null;
                const current = plan.tuple.keys[0].ordinal;
                const kind = row.table_schema.relational_columns[ordinal].column_type;
                if (kind != self.table.relational_columns[current].column_type) return error.RelationalIndexColumnTypeMismatch;
                const cell = (try row.findCell(ordinal)) orelse break :scalar .null;
                if (cell.is_null) break :scalar .null;
                break :scalar switch (kind) {
                    .string => .{ .string = cell.value.bytes_val },
                    .blob => .{ .blob = cell.value.bytes_val },
                    .integer => .{ .integer = cell.value.i64_val },
                    .number => .{ .number = cell.value.f64_val },
                    .boolean => .{ .boolean = cell.value.bool_val },
                    .datetime => .{ .datetime = cell.value.u64_val },
                    else => return error.UnsupportedRelationalIndexColumn,
                };
            };
            if (!(try plan.evaluateValue(alloc, &scratch, scalar)).satisfiesCheck()) return i;
        }
        return null;
    }
};
