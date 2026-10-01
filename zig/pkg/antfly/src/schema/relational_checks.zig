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
const expressions = @import("relational_expression.zig");
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

const Compiled = union(enum) {
    comparison: predicate.Plan,
    expression: expressions.Plan,
    fn deinit(self: *Compiled) void {
        switch (self.*) {
            inline else => |*plan| plan.deinit(),
        }
    }
};

fn compile(alloc: Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, definition: wire.RelationalCheckConstraint) !Compiled {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    if (definition.expression) |expression| {
        if (definition.column != null or definition.op != null or definition.value != null or definition.collation != null) return error.InvalidSchemaUpdateRequest;
        const json = try std.json.Stringify.valueAlloc(arena.allocator(), expression, .{});
        const value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), json, .{ .parse_numbers = false });
        return .{ .expression = try expressions.Plan.init(alloc, table, value, .boolean) };
    }
    const column = definition.column orelse return error.InvalidSchemaUpdateRequest;
    const operation = definition.op orelse return error.InvalidSchemaUpdateRequest;
    const ordinal = layout.ordinalForName(table.relational_columns, column) orelse return error.RelationalIndexColumnNotFound;
    const value = try valueFromJson(arena.allocator(), table.relational_columns[ordinal].column_type, definition.value orelse .null, true);
    return .{ .comparison = try predicate.Plan.init(alloc, table, layout, .{
        .column = column,
        .op = switch (operation) {
            inline else => |op| @field(@import("../storage/relational_index.zig").RelationalCheckOp, @tagName(op)),
        },
        .value = value,
        .collation = definition.collation,
    }) };
}

pub fn validateDefinitions(alloc: Allocator, table: schema.TableSchema, definitions: []const wire.RelationalCheckConstraint) !void {
    if (definitions.len == 0) return;
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    var nodes: usize = 0;
    var literals: usize = 0;
    for (definitions) |definition| {
        var plan = try compile(alloc, table, &layout, definition);
        defer plan.deinit();
        if (plan == .expression) {
            nodes += plan.expression.nodes.len;
            literals += plan.expression.literal_bytes;
            if (nodes > 4096 or literals > expressions.max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
        }
    }
}

/// Heap-stable compiled data owned by a public-schema epoch. The wire schema
/// supplies names; the reduced runtime schema supplies exact physical types.
pub const Set = struct {
    alloc: Allocator,
    table: schema.TableSchema,
    layout: codec.PhysicalLayout,
    definitions: []const wire.RelationalCheckConstraint,
    plans: []Compiled,
    /// Borrowed immutable schema names, deduplicated across every CHECK form.
    dependency_fields: []const []const u8 = &.{},
    identity: [32]u8 = undefined,

    /// Logical CHECK identity excludes schema epochs, column ordinals and
    /// declaration order. Operands already have canonical typed encodings.
    pub fn fingerprint(self: *const Set) [32]u8 {
        return self.identity;
    }

    fn computeFingerprint(self: *const Set, alloc: Allocator) ![32]u8 {
        const entries = try alloc.alloc([32]u8, self.plans.len);
        defer alloc.free(entries);
        for (self.definitions, self.plans, entries) |definition, plan, *entry| {
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly check definition v1");
            var size: [8]u8 = undefined;
            std.mem.writeInt(u64, &size, definition.name.len, .little);
            state.update(&size);
            state.update(definition.name);
            switch (plan) {
                .comparison => |comparison| {
                    state.update(&comparison.tuple.fingerprint);
                    state.update(&.{ @intFromEnum(comparison.op), @intFromBool(comparison.operand_null) });
                    state.update(comparison.operand);
                },
                .expression => |expression| {
                    state.update("immutable boolean expression v1");
                    state.update(&expression.fingerprint);
                },
            }
            state.final(entry);
        }
        std.mem.sort([32]u8, entries, {}, struct {
            fn less(_: void, a: [32]u8, b: [32]u8) bool {
                return std.mem.order(u8, &a, &b) == .lt;
            }
        }.less);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly check coverage v1");
        for (entries) |entry| state.update(&entry);
        var result: [32]u8 = undefined;
        state.final(&result);
        return result;
    }

    /// Takes the runtime schema only on success. Definitions remain borrowed
    /// from the same public validator and are released after this set.
    pub fn createOwned(alloc: Allocator, table: schema.TableSchema, definitions: []const wire.RelationalCheckConstraint) !*Set {
        const set = try alloc.create(Set);
        errdefer alloc.destroy(set);
        set.* = .{ .alloc = alloc, .table = table, .layout = try codec.PhysicalLayout.init(alloc, table), .definitions = definitions, .plans = undefined };
        errdefer set.layout.deinit();
        set.plans = try alloc.alloc(Compiled, definitions.len);
        errdefer alloc.free(set.plans);
        var initialized: usize = 0;
        errdefer for (set.plans[0..initialized]) |*plan| plan.deinit();
        for (definitions, set.plans) |definition, *plan| {
            plan.* = try compile(alloc, table, &set.layout, definition);
            initialized += 1;
        }
        var needed = try alloc.alloc(bool, table.relational_columns.len);
        defer alloc.free(needed);
        @memset(needed, false);
        var node_count: usize = 0;
        var literal_bytes: usize = 0;
        for (set.plans) |plan| switch (plan) {
            .comparison => |comparison| {
                needed[comparison.tuple.keys[0].ordinal] = true;
            },
            .expression => |expression| {
                node_count += expression.nodes.len;
                literal_bytes += expression.literal_bytes;
                for (expression.dependencies) |ordinal| needed[ordinal] = true;
            },
        };
        if (node_count > 4096 or literal_bytes > expressions.max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
        var field_count: usize = 0;
        for (needed) |selected| if (selected) {
            field_count += 1;
        };
        const fields = try alloc.alloc([]const u8, field_count);
        errdefer alloc.free(fields);
        var field_index: usize = 0;
        for (needed, table.relational_columns) |selected, column| if (selected) {
            fields[field_index] = column.name;
            field_index += 1;
        };
        set.dependency_fields = fields;
        set.identity = try set.computeFingerprint(alloc);
        return set;
    }

    pub fn deinit(self: *Set) void {
        for (self.plans) |*plan| plan.deinit();
        self.alloc.free(self.plans);
        self.alloc.free(self.dependency_fields);
        self.layout.deinit();
        schema.freeSchema(self.alloc, self.table);
        self.alloc.destroy(self);
    }

    pub fn firstViolationJson(self: *const Set, alloc: Allocator, value: std.json.Value) !?usize {
        if (try self.firstJson(alloc, value, false)) |failure| return failure.index;
        return null;
    }

    pub const Failure = struct { index: usize, reason: anyerror = error.RelationalCheckViolation };

    /// Invalid deterministic expression results become durable invalid-row
    /// diagnostics during activation, not indefinitely retried transient jobs.
    pub fn firstFailureJson(self: *const Set, alloc: Allocator, value: std.json.Value) !?Failure {
        return self.firstJson(alloc, value, true);
    }

    fn firstJson(self: *const Set, alloc: Allocator, value: std.json.Value, activation: bool) !?Failure {
        if (value != .object) return error.InvalidBatchRequest;
        var scratch = std.ArrayList(u8).empty;
        defer scratch.deinit(alloc);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var budget: usize = expressions.max_allocated_bytes;
        for (self.definitions, self.plans, 0..) |definition, *plan, i| {
            const accepted = switch (plan.*) {
                .comparison => |*comparison| blk: {
                    const ordinal = comparison.tuple.keys[0].ordinal;
                    const scalar = try valueFromJson(arena.allocator(), self.table.relational_columns[ordinal].column_type, value.object.get(definition.column.?) orelse .null, false);
                    break :blk (try comparison.evaluateValue(alloc, &scratch, scalar)).satisfiesCheck();
                },
                .expression => |*expression| blk: {
                    const result = expression.evaluateJsonWithBudget(arena.allocator(), value, &budget) catch |err| {
                        if (activation and isDeterministicFailure(err)) return .{ .index = i, .reason = err };
                        return err;
                    };
                    break :blk result == .null or result.boolean;
                },
            };
            if (!accepted) return .{ .index = i };
        }
        return null;
    }

    /// Cold validation reads selected ordinal cells only. No complete JSON
    /// materialization, reparse, or re-encoding of unrelated columns.
    pub fn firstViolationRow(self: *const Set, alloc: Allocator, row: codec.OrdinalRowView) !?usize {
        if (try self.firstRow(alloc, row, false)) |failure| return failure.index;
        return null;
    }

    pub fn firstFailureRow(self: *const Set, alloc: Allocator, row: codec.OrdinalRowView) !?Failure {
        return self.firstRow(alloc, row, true);
    }

    fn firstRow(self: *const Set, alloc: Allocator, row: codec.OrdinalRowView, activation: bool) !?Failure {
        var scratch = std.ArrayList(u8).empty;
        defer scratch.deinit(alloc);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var budget: usize = expressions.max_allocated_bytes;
        for (self.definitions, self.plans, 0..) |definition, *plan, i| {
            if (plan.* == .expression) {
                const result = plan.expression.evaluateRowWithBudget(arena.allocator(), row, &budget) catch |err| {
                    if (activation and isDeterministicFailure(err)) return .{ .index = i, .reason = err };
                    return err;
                };
                if (result != .null and !result.boolean) return .{ .index = i };
                continue;
            }
            const comparison = &plan.comparison;
            const scalar: tuples.Value = scalar: {
                const ordinal = row.ordinalForName(definition.column.?) orelse break :scalar .null;
                const current = comparison.tuple.keys[0].ordinal;
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
            if (!(try comparison.evaluateValue(alloc, &scratch, scalar)).satisfiesCheck()) return .{ .index = i };
        }
        return null;
    }
};

fn isDeterministicFailure(err: anyerror) bool {
    return @import("relational_expression_errors.zig").isInvalidInput(err) or err == error.RelationalIndexColumnTypeMismatch or err == error.InvalidBatchRequest;
}
