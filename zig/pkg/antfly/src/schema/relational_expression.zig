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

//! Immutable bounded scalar bytecode. Names, types, literals and operations
//! determine identity; schema epochs and physical ordinals do not.
const std = @import("std");
const schema = @import("../storage/schema.zig");
const checks = @import("relational_checks.zig");
const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
pub const Value = @import("../storage/db/relational_index_keys.zig").Value;
pub const Kind = schema.RelationalColumnType;
const Allocator = std.mem.Allocator;
pub const max_nodes = 128;
pub const max_depth = 16;
pub const max_output_bytes = 1024 * 1024;
pub const max_allocated_bytes = 4 * max_output_bytes;

const Op = enum { literal, column, add, subtract, multiply, divide, negate, concat, coalesce, lower_ascii, upper_ascii, eq, ne, gt, gte, lt, lte, is_null, is_not_null, is_distinct, is_not_distinct, @"and", @"or", not };
const Node = struct {
    op: Op,
    kind: Kind,
    children: []const u16 = &.{},
    ordinal: u32 = 0,
    column_name: []const u8 = "",
    fold_ascii: bool = false,
    literal: Value = .null,
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []const Node,
    dependencies: []const u32,
    fingerprint: [32]u8,
    result_kind: Kind,
    literal_bytes: usize,

    pub fn init(alloc: Allocator, table: schema.TableSchema, expression: std.json.Value, expected: Kind) !Plan {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var compiler: Compiler = .{ .alloc = arena.allocator(), .table = table };
        compiler.hash.update("antfly immutable scalar expression v1");
        const root = try compiler.compile(expression, 0);
        if (compiler.nodes.items[root].kind != expected) return error.InvalidRelationalExpressionType;
        var fingerprint: [32]u8 = undefined;
        compiler.hash.final(&fingerprint);
        return .{ .arena = arena, .nodes = compiler.nodes.items, .dependencies = compiler.dependencies.items, .fingerprint = fingerprint, .result_kind = expected, .literal_bytes = compiler.literal_bytes };
    }

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Results borrow input values or allocate from the caller's row arena.
    /// Coalesce is lazy, so an unselected expression cannot raise an error.
    pub fn evaluate(self: *const Plan, alloc: Allocator, values: []const Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateNode(alloc, .{ .values = values }, @intCast(self.nodes.len - 1), &budget);
    }

    pub fn evaluateJson(self: *const Plan, alloc: Allocator, document: std.json.Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateJsonWithBudget(alloc, document, &budget);
    }

    pub fn evaluateJsonWithBudget(self: *const Plan, alloc: Allocator, document: std.json.Value, budget: *usize) !Value {
        if (document != .object) return error.InvalidRelationalExpressionInput;
        return self.evaluateNode(alloc, .{ .json = document }, @intCast(self.nodes.len - 1), budget);
    }

    pub fn evaluateRow(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateRowWithBudget(alloc, row, &budget);
    }

    pub fn evaluateRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        return self.evaluateNode(alloc, .{ .row = row }, @intCast(self.nodes.len - 1), budget);
    }

    /// Caller must fence the exact immutable source layout used to compile this
    /// plan. TuplePlan establishes that proof once per scan, including history.
    pub fn evaluateBoundRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        return self.evaluateNode(alloc, .{ .bound_row = row }, @intCast(self.nodes.len - 1), budget);
    }

    const Source = union(enum) { values: []const Value, json: std.json.Value, row: codec.OrdinalRowView, bound_row: codec.OrdinalRowView };

    fn evaluateNode(self: *const Plan, alloc: Allocator, source: Source, index: u16, budget: *usize) anyerror!Value {
        const node = self.nodes[index];
        if (node.op == .literal) return node.literal;
        if (node.op == .column) {
            const value: Value = switch (source) {
                .values => |values| if (node.ordinal < values.len) values[node.ordinal] else return error.InvalidRelationalExpressionInput,
                .json => |json| blk: {
                    const input = json.object.get(node.column_name) orelse .null;
                    if (node.kind == .blob and input == .string) {
                        const size = std.base64.standard.Decoder.calcSizeForSlice(input.string) catch return error.InvalidRelationalExpressionInput;
                        if (size > budget.*) return error.RelationalExpressionBudgetExceeded;
                        budget.* -= size;
                    }
                    break :blk try checks.valueFromJson(alloc, node.kind, input, false);
                },
                .row, .bound_row => |row| blk: {
                    const ordinal = if (source == .bound_row) node.ordinal else row.ordinalForName(node.column_name) orelse break :blk .null;
                    if (ordinal >= row.table_schema.relational_columns.len) return error.RelationalIndexColumnTypeMismatch;
                    if (row.table_schema.relational_columns[ordinal].column_type != node.kind) return error.RelationalIndexColumnTypeMismatch;
                    const cell = (try row.findCell(ordinal)) orelse break :blk .null;
                    if (cell.is_null) break :blk .null;
                    break :blk switch (node.kind) {
                        .string => .{ .string = cell.value.bytes_val },
                        .blob => .{ .blob = cell.value.bytes_val },
                        .integer => .{ .integer = cell.value.i64_val },
                        .number => .{ .number = cell.value.f64_val },
                        .boolean => .{ .boolean = cell.value.bool_val },
                        .datetime => .{ .datetime = cell.value.u64_val },
                        else => return error.InvalidRelationalExpressionType,
                    };
                },
            };
            if (value != .null and !valueHasKind(value, node.kind)) return error.InvalidRelationalExpressionInput;
            if (value == .number and !std.math.isFinite(value.number)) return error.InvalidRelationalExpressionInput;
            return value;
        }
        if (node.op == .coalesce) {
            for (node.children) |child| {
                const value = try self.evaluateNode(alloc, source, child, budget);
                if (value != .null) return value;
            }
            return .null;
        }
        if (node.op == .@"and" or node.op == .@"or") {
            var unknown = false;
            for (node.children) |child| {
                const value = try self.evaluateNode(alloc, source, child, budget);
                if (value == .null) {
                    unknown = true;
                    continue;
                }
                if (value.boolean == (node.op == .@"or")) return value;
            }
            return if (unknown) .null else .{ .boolean = node.op == .@"and" };
        }
        if (node.op == .is_null or node.op == .is_not_null) {
            const value = try self.evaluateNode(alloc, source, node.children[0], budget);
            return .{ .boolean = (value == .null) == (node.op == .is_null) };
        }
        if (isComparison(node.op)) {
            const left = try self.evaluateNode(alloc, source, node.children[0], budget);
            const right = try self.evaluateNode(alloc, source, node.children[1], budget);
            if (left == .null or right == .null) return switch (node.op) {
                .is_distinct => .{ .boolean = (left == .null) != (right == .null) },
                .is_not_distinct => .{ .boolean = (left == .null) == (right == .null) },
                else => .null,
            };
            // Borrowed values need no allocation, but repeatedly comparing a
            // wide value still consumes CPU. Charge the maximum operand bytes
            // inspected against the same per-row budget as allocated outputs.
            const compared_bytes: usize = switch (left) {
                .string => |bytes| @min(bytes.len, right.string.len),
                .blob => |bytes| @min(bytes.len, right.blob.len),
                else => 0,
            };
            if (compared_bytes > budget.* / 2) return error.RelationalExpressionBudgetExceeded;
            budget.* -= compared_bytes * 2;
            const order = valueOrder(left, right, node.fold_ascii);
            return .{ .boolean = switch (node.op) {
                .eq, .is_not_distinct => order == .eq,
                .ne, .is_distinct => order != .eq,
                .gt => order == .gt,
                .gte => order != .lt,
                .lt => order == .lt,
                .lte => order != .gt,
                else => unreachable,
            } };
        }
        var operands: [32]Value = undefined;
        for (node.children, 0..) |child, i| {
            operands[i] = try self.evaluateNode(alloc, source, child, budget);
            if (operands[i] == .null) return .null;
        }
        const a = operands[0];
        switch (node.op) {
            .not => return .{ .boolean = !a.boolean },
            .negate => return switch (a) {
                .integer => |v| .{ .integer = std.math.sub(i64, 0, v) catch return error.RelationalExpressionOverflow },
                .number => |v| finite(-v),
                else => unreachable,
            },
            .add, .subtract, .multiply, .divide => {
                const b = operands[1];
                if (node.kind == .integer) {
                    const result = switch (node.op) {
                        .add => std.math.add(i64, a.integer, b.integer),
                        .subtract => std.math.sub(i64, a.integer, b.integer),
                        .multiply => std.math.mul(i64, a.integer, b.integer),
                        .divide => blk: {
                            if (b.integer == 0) return error.RelationalExpressionDivisionByZero;
                            if (a.integer == std.math.minInt(i64) and b.integer == -1) return error.RelationalExpressionOverflow;
                            break :blk @divTrunc(a.integer, b.integer);
                        },
                        else => unreachable,
                    } catch return error.RelationalExpressionOverflow;
                    return .{ .integer = result };
                }
                if (node.op == .divide and b.number == 0) return error.RelationalExpressionDivisionByZero;
                return finite(switch (node.op) {
                    .add => a.number + b.number,
                    .subtract => a.number - b.number,
                    .multiply => a.number * b.number,
                    .divide => a.number / b.number,
                    else => unreachable,
                });
            },
            .concat => {
                var size: usize = 0;
                for (operands[0..node.children.len]) |value| size = std.math.add(usize, size, value.string.len) catch return error.RelationalExpressionBudgetExceeded;
                const output = try allocateOutput(alloc, size, budget);
                var offset: usize = 0;
                for (operands[0..node.children.len]) |value| {
                    @memcpy(output[offset..][0..value.string.len], value.string);
                    offset += value.string.len;
                }
                return .{ .string = output };
            },
            .lower_ascii, .upper_ascii => {
                const output = try allocateOutput(alloc, a.string.len, budget);
                for (a.string, output) |byte, *out| out.* = if (node.op == .lower_ascii) std.ascii.toLower(byte) else std.ascii.toUpper(byte);
                return .{ .string = output };
            },
            else => unreachable,
        }
    }
};

fn allocateOutput(alloc: Allocator, size: usize, budget: *usize) ![]u8 {
    if (size > max_output_bytes or size > budget.*) return error.RelationalExpressionBudgetExceeded;
    budget.* -= size;
    return alloc.alloc(u8, size);
}

fn isComparison(op: Op) bool {
    return switch (op) {
        .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => true,
        else => false,
    };
}

fn valueOrder(a: Value, b: Value, fold_ascii: bool) std.math.Order {
    return switch (a) {
        .null => unreachable,
        .string => |left| blk: {
            if (!fold_ascii) break :blk std.mem.order(u8, left, b.string);
            for (left[0..@min(left.len, b.string.len)], b.string[0..@min(left.len, b.string.len)]) |x, y| {
                const order = std.math.order(std.ascii.toLower(x), std.ascii.toLower(y));
                if (order != .eq) break :blk order;
            }
            break :blk std.math.order(left.len, b.string.len);
        },
        .blob => |value| std.mem.order(u8, value, b.blob),
        .boolean => |value| std.math.order(@intFromBool(value), @intFromBool(b.boolean)),
        .integer => |value| std.math.order(value, b.integer),
        .number => |value| std.math.order(value, b.number),
        .datetime => |value| std.math.order(value, b.datetime),
    };
}

fn finite(value: f64) !Value {
    if (!std.math.isFinite(value)) return error.RelationalExpressionOverflow;
    return .{ .number = if (value == 0) 0 else value };
}

fn valueHasKind(value: Value, kind: Kind) bool {
    return switch (value) {
        .null => true,
        inline else => |_, tag| std.mem.eql(u8, @tagName(tag), @tagName(kind)),
    };
}

const Compiler = struct {
    alloc: Allocator,
    table: schema.TableSchema,
    nodes: std.ArrayList(Node) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    hash: std.crypto.hash.Blake3 = std.crypto.hash.Blake3.init(.{}),
    visited: usize = 0,
    literal_bytes: usize = 0,

    fn frame(self: *Compiler, bytes: []const u8) void {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, bytes.len, .little);
        self.hash.update(&size);
        self.hash.update(bytes);
    }

    fn compile(self: *Compiler, input: std.json.Value, depth: usize) anyerror!u16 {
        self.visited += 1;
        if (depth >= max_depth or self.visited > max_nodes) return error.RelationalExpressionBudgetExceeded;
        if (input != .object) return error.InvalidRelationalExpression;
        const op_value = input.object.get("op") orelse return error.InvalidRelationalExpression;
        if (op_value != .string) return error.InvalidRelationalExpression;
        const op = std.meta.stringToEnum(Op, op_value.string) orelse return error.InvalidRelationalExpression;
        var fields = input.object.iterator();
        while (fields.next()) |field| {
            const name = field.key_ptr.*;
            if (std.mem.eql(u8, name, "op")) continue;
            const allowed = switch (op) {
                .literal => std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "value"),
                .column => std.mem.eql(u8, name, "column"),
                else => std.mem.eql(u8, name, "args") or (isComparison(op) and std.mem.eql(u8, name, "collation")),
            };
            if (!allowed) return error.InvalidRelationalExpression;
        }
        self.frame(op_value.string);
        var node: Node = .{ .op = op, .kind = undefined };
        switch (op) {
            .literal => {
                const kind = input.object.get("type") orelse return error.InvalidRelationalExpression;
                if (kind != .string) return error.InvalidRelationalExpression;
                node.kind = std.meta.stringToEnum(Kind, kind.string) orelse return error.InvalidRelationalExpression;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number => {},
                    else => return error.InvalidRelationalExpressionType,
                }
                const value = input.object.get("value") orelse .null;
                if (node.kind == .blob and value == .string and value.string.len > std.base64.standard.Encoder.calcSize(max_output_bytes)) return error.RelationalExpressionBudgetExceeded;
                node.literal = checks.valueFromJson(self.alloc, node.kind, value, true) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidRelationalExpressionType,
                };
                switch (node.literal) {
                    .string => |bytes| {
                        if (bytes.len > max_output_bytes) return error.RelationalExpressionBudgetExceeded;
                        node.literal = .{ .string = try self.alloc.dupe(u8, bytes) };
                    },
                    .blob => |bytes| if (bytes.len > max_output_bytes) return error.RelationalExpressionBudgetExceeded,
                    .number => |value_number| node.literal = try finite(value_number),
                    else => {},
                }
                self.frame(@tagName(node.kind));
                self.literal_bytes += switch (node.literal) {
                    .string, .blob => |bytes| bytes.len,
                    else => 8,
                };
                if (self.literal_bytes > max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
                self.frame(@tagName(node.literal));
                var bytes: [8]u8 = undefined;
                switch (node.literal) {
                    .null => {},
                    .string, .blob => |value_bytes| self.frame(value_bytes),
                    .boolean => |boolean| self.hash.update(&.{@intFromBool(boolean)}),
                    .integer => |integer| {
                        std.mem.writeInt(i64, &bytes, integer, .little);
                        self.hash.update(&bytes);
                    },
                    .datetime => |datetime| {
                        std.mem.writeInt(u64, &bytes, datetime, .little);
                        self.hash.update(&bytes);
                    },
                    .number => |number| {
                        std.mem.writeInt(u64, &bytes, @bitCast(number), .little);
                        self.hash.update(&bytes);
                    },
                }
            },
            .column => {
                const name = input.object.get("column") orelse return error.InvalidRelationalExpression;
                if (name != .string) return error.InvalidRelationalExpression;
                node.column_name = try self.alloc.dupe(u8, name.string);
                node.ordinal = for (self.table.relational_columns, 0..) |column, ordinal| {
                    if (std.mem.eql(u8, column.name, name.string)) break @intCast(ordinal);
                } else return error.RelationalIndexColumnNotFound;
                node.kind = self.table.relational_columns[node.ordinal].column_type;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number => {},
                    else => return error.InvalidRelationalExpressionType,
                }
                if (std.mem.indexOfScalar(u32, self.dependencies.items, node.ordinal) == null) try self.dependencies.append(self.alloc, node.ordinal);
                self.frame(name.string);
                self.frame(@tagName(node.kind));
            },
            else => {
                const args = input.object.get("args") orelse return error.InvalidRelationalExpression;
                if (args != .array) return error.InvalidRelationalExpression;
                const length = args.array.items.len;
                const valid = switch (op) {
                    .negate, .lower_ascii, .upper_ascii, .not, .is_null, .is_not_null => length == 1,
                    .concat, .coalesce, .@"and", .@"or" => length >= 2 and length <= 32,
                    else => length == 2,
                };
                if (!valid) return error.InvalidRelationalExpression;
                self.hash.update(&.{@intCast(length)});
                const children = try self.alloc.alloc(u16, length);
                for (args.array.items, children) |arg, *child| child.* = try self.compile(arg, depth + 1);
                node.children = children;
                node.kind = self.nodes.items[children[0]].kind;
                for (children[1..]) |child| if (self.nodes.items[child].kind != node.kind) return error.InvalidRelationalExpressionType;
                if (isComparison(op)) {
                    if (input.object.get("collation")) |collation| {
                        if (collation != .string or node.kind != .string) return error.UnsupportedRelationalIndexCollation;
                        if (std.ascii.eqlIgnoreCase(collation.string, "ci") or std.ascii.eqlIgnoreCase(collation.string, "case_insensitive") or std.ascii.eqlIgnoreCase(collation.string, "antfly.case_insensitive")) {
                            node.fold_ascii = true;
                        } else if (!std.ascii.eqlIgnoreCase(collation.string, "C") and !std.ascii.eqlIgnoreCase(collation.string, "POSIX") and !std.ascii.eqlIgnoreCase(collation.string, "binary")) return error.UnsupportedRelationalIndexCollation;
                    }
                    self.hash.update(&.{@intFromBool(node.fold_ascii)});
                    node.kind = .boolean;
                }
                switch (op) {
                    .add, .subtract, .multiply, .divide, .negate => if (node.kind != .integer and node.kind != .number) return error.InvalidRelationalExpressionType,
                    .concat, .lower_ascii, .upper_ascii => if (node.kind != .string) return error.InvalidRelationalExpressionType,
                    .coalesce => {},
                    .@"and", .@"or", .not => if (node.kind != .boolean) return error.InvalidRelationalExpressionType,
                    .is_null, .is_not_null => node.kind = .boolean,
                    .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => {},
                    else => unreachable,
                }
            },
        }
        const index: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.alloc, node);
        return index;
    }
};

/// Immutable schema-owned defaults and generated-column dependency graph.
/// Defaults cannot reference columns. Generated columns execute in dependency
/// order, irrespective of declaration order. Callers enforce input policy.
pub const Set = struct {
    alloc: Allocator,
    table: schema.TableSchema,
    bindings: []Binding,
    order: []usize,
    read_columns: []bool,
    generated_columns: []bool,
    pub const Binding = struct { ordinal: u32, generated: bool, plan: Plan };

    pub fn createOwned(alloc: Allocator, table: schema.TableSchema, defaults: std.json.Value, generated: std.json.Value) !*Set {
        const default_entries = try entries(defaults);
        const generated_entries = try entries(generated);
        if (default_entries.len + generated_entries.len > 256) return error.RelationalExpressionBudgetExceeded;
        const bindings = try alloc.alloc(Binding, default_entries.len + generated_entries.len);
        errdefer alloc.free(bindings);
        var initialized: usize = 0;
        var total_nodes: usize = 0;
        var total_literal_bytes: usize = 0;
        errdefer for (bindings[0..initialized]) |*binding| binding.plan.deinit();
        for ([_][]const std.json.Value{ default_entries, generated_entries }, 0..) |declarations, mode| {
            for (declarations) |entry| {
                if (entry != .object or entry.object.count() != 2) return error.InvalidRelationalExpression;
                const name = entry.object.get("column") orelse return error.InvalidRelationalExpression;
                if (name != .string) return error.InvalidRelationalExpression;
                const ordinal: u32 = for (table.relational_columns, 0..) |column, i| {
                    if (std.mem.eql(u8, column.name, name.string)) break @intCast(i);
                } else return error.RelationalIndexColumnNotFound;
                for (bindings[0..initialized]) |binding| if (binding.ordinal == ordinal) return error.InvalidRelationalExpression;
                var plan = try Plan.init(alloc, table, entry.object.get("expression") orelse return error.InvalidRelationalExpression, table.relational_columns[ordinal].column_type);
                errdefer plan.deinit();
                total_nodes += plan.nodes.len;
                total_literal_bytes += plan.literal_bytes;
                if (total_nodes > 4096 or total_literal_bytes > max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
                if (mode == 0 and plan.dependencies.len != 0) return error.InvalidRelationalExpression;
                bindings[initialized] = .{ .ordinal = ordinal, .generated = mode != 0, .plan = plan };
                initialized += 1;
            }
        }
        const order = try alloc.alloc(usize, bindings.len);
        errdefer alloc.free(order);
        const read_columns = try alloc.alloc(bool, table.relational_columns.len);
        errdefer alloc.free(read_columns);
        @memset(read_columns, false);
        const generated_columns = try alloc.alloc(bool, table.relational_columns.len);
        errdefer alloc.free(generated_columns);
        @memset(generated_columns, false);
        for (bindings) |binding| {
            for (binding.plan.dependencies) |ordinal| read_columns[ordinal] = true;
            if (binding.generated) {
                read_columns[binding.ordinal] = true;
                generated_columns[binding.ordinal] = true;
            }
        }
        var states = [_]u2{0} ** 256;
        var written: usize = 0;
        for (bindings, 0..) |_, i| try visit(bindings, order, &states, &written, i);
        const set = try alloc.create(Set);
        set.* = .{ .alloc = alloc, .table = table, .bindings = bindings, .order = order, .read_columns = read_columns, .generated_columns = generated_columns };
        return set;
    }

    fn visit(bindings: []const Binding, order: []usize, states: *[256]u2, written: *usize, index: usize) anyerror!void {
        if (states[index] == 2) return;
        if (states[index] == 1) return error.RelationalGeneratedColumnCycle;
        states[index] = 1;
        for (bindings[index].plan.dependencies) |dependency| for (bindings, 0..) |binding, i| {
            if (binding.ordinal == dependency) try visit(bindings, order, states, written, i);
        };
        states[index] = 2;
        order[written.*] = index;
        written.* += 1;
    }

    pub fn deinit(self: *Set) void {
        for (self.bindings) |*binding| binding.plan.deinit();
        self.alloc.free(self.bindings);
        self.alloc.free(self.order);
        self.alloc.free(self.read_columns);
        self.alloc.free(self.generated_columns);
        schema.freeSchema(self.alloc, self.table);
        self.alloc.destroy(self);
    }

    pub const DefaultsPolicy = enum(u8) { preserve_absence, apply_to_absent };

    pub fn applyValues(self: *const Set, alloc: Allocator, values: []Value, present: []bool) !void {
        return self.applyValuesWithPolicy(alloc, values, present, .apply_to_absent);
    }

    /// Rewrite programs bind this policy durably. Ordinary restore must never
    /// call this function: it verifies historical results without computing.
    pub fn applyValuesWithPolicy(self: *const Set, alloc: Allocator, values: []Value, present: []bool, defaults: DefaultsPolicy) !void {
        if (values.len != self.table.relational_columns.len or present.len != values.len) return error.InvalidRelationalExpressionInput;
        var budget: usize = max_allocated_bytes;
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated and (present[binding.ordinal] or defaults == .preserve_absence)) continue;
            values[binding.ordinal] = try binding.plan.evaluateNode(alloc, .{ .values = values }, @intCast(binding.plan.nodes.len - 1), &budget);
            present[binding.ordinal] = true;
        }
    }

    /// Mutates a request-owned DOM without reparsing it. Generated fields are
    /// output-only: any submitted value is overwritten deterministically.
    pub fn applyJson(self: *const Set, alloc: Allocator, document: *std.json.Value) !void {
        if (document.* != .object) return error.InvalidBatchRequest;
        const values = try alloc.alloc(Value, self.table.relational_columns.len);
        defer alloc.free(values);
        const present = try alloc.alloc(bool, values.len);
        defer alloc.free(present);
        try self.readValues(alloc, document.*, values, present, true);
        try self.applyValues(alloc, values, present);
        for (self.bindings) |binding| {
            const name = self.table.relational_columns[binding.ordinal].name;
            if (!binding.generated and document.object.contains(name)) continue;
            try document.object.put(alloc, try alloc.dupe(u8, name), try valueToJson(alloc, values[binding.ordinal]));
        }
    }

    /// Restore never fills defaults or repairs forged generated values. It
    /// verifies the stored canonical logical result in dependency order.
    pub fn verifyJson(self: *const Set, alloc: Allocator, document: std.json.Value) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        if (document != .object) return error.InvalidBatchRequest;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const arena = scratch.allocator();
        const values = try arena.alloc(Value, self.table.relational_columns.len);
        const present = try arena.alloc(bool, values.len);
        try self.readValues(arena, document, values, present, false);
        try self.verifyValues(arena, values, present);
    }

    /// Cold restore verifies generated semantics directly from ordinal cells.
    /// The caller has already verified physical canonical bytes and checksum.
    /// Unrelated JSON, vector and blob payloads are never materialized.
    pub fn verifyRow(self: *const Set, alloc: Allocator, row: anytype) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const arena = scratch.allocator();
        const values = try arena.alloc(Value, self.table.relational_columns.len);
        const present = try arena.alloc(bool, values.len);
        @memset(values, .null);
        @memset(present, false);
        for (self.table.relational_columns, self.read_columns, 0..) |column, read, ordinal| {
            if (!read) continue;
            const physical = row.ordinalForName(column.name) orelse continue;
            if (row.table_schema.relational_columns[physical].column_type != column.column_type) return error.InvalidRelationalGeneratedValue;
            const cell = (try row.findCell(physical)) orelse continue;
            present[ordinal] = true;
            if (cell.is_null) continue;
            values[ordinal] = switch (column.column_type) {
                .string => .{ .string = cell.value.bytes_val },
                .blob => .{ .blob = cell.value.bytes_val },
                .integer => .{ .integer = cell.value.i64_val },
                .number => .{ .number = cell.value.f64_val },
                .boolean => .{ .boolean = cell.value.bool_val },
                .datetime => .{ .datetime = cell.value.u64_val },
                else => return error.InvalidRelationalGeneratedValue,
            };
        }
        try self.verifyValues(arena, values, present);
    }

    fn verifyValues(self: *const Set, alloc: Allocator, values: []const Value, present: []const bool) !void {
        var budget: usize = max_allocated_bytes;
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated) continue;
            if (!present[binding.ordinal]) return error.InvalidRelationalGeneratedValue;
            const expected = try binding.plan.evaluateNode(alloc, .{ .values = values }, @intCast(binding.plan.nodes.len - 1), &budget);
            if (!valuesEqual(expected, values[binding.ordinal])) return error.InvalidRelationalGeneratedValue;
        }
    }

    fn readValues(self: *const Set, alloc: Allocator, document: std.json.Value, values: []Value, present: []bool, ignore_generated: bool) !void {
        for (self.table.relational_columns, values, present, 0..) |column, *value, *exists, ordinal| {
            const input = document.object.get(column.name);
            exists.* = input != null;
            value.* = .null;
            // Wide unrelated blobs/JSON/vectors never enter expression
            // decoding. The immutable mask is compiled once per schema.
            if (!self.read_columns[ordinal] or (ignore_generated and self.generated_columns[ordinal])) continue;
            if (input) |scalar| switch (column.column_type) {
                .string, .blob, .boolean, .datetime, .integer, .number => value.* = try checks.valueFromJson(alloc, column.column_type, scalar, false),
                else => {},
            };
        }
    }
};

fn valuesEqual(a: Value, b: Value) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .string => |bytes| std.mem.eql(u8, bytes, b.string),
        .blob => |bytes| std.mem.eql(u8, bytes, b.blob),
        .integer => |value| value == b.integer,
        .number => |value| value == b.number,
        .boolean => |value| value == b.boolean,
        .datetime => |value| value == b.datetime,
    };
}

fn valueToJson(alloc: Allocator, value: Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .string => |bytes| .{ .string = try alloc.dupe(u8, bytes) },
        .blob => |bytes| blk: {
            const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
            break :blk .{ .string = std.base64.standard.Encoder.encode(encoded, bytes) };
        },
        .integer => |integer| .{ .integer = integer },
        .number => |number| .{ .float = number },
        .boolean => |boolean| .{ .bool = boolean },
        .datetime => |datetime| .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{datetime}) },
    };
}

fn entries(value: std.json.Value) ![]const std.json.Value {
    if (value == .null) return &.{};
    if (value != .array or value.array.items.len > 256) return error.InvalidRelationalExpression;
    return value.array.items;
}

/// Identity for online schema admission. Adding, removing, or changing STORED
/// generated semantics requires a row rewrite; declaration reordering does not.
pub fn generatedFingerprint(set: ?*const Set) [32]u8 {
    const Entry = struct { name: []const u8, fingerprint: [32]u8 };
    var entries_buffer: [256]Entry = undefined;
    var count: usize = 0;
    if (set) |expressions| for (expressions.bindings) |binding| {
        if (!binding.generated) continue;
        entries_buffer[count] = .{ .name = expressions.table.relational_columns[binding.ordinal].name, .fingerprint = binding.plan.fingerprint };
        count += 1;
    };
    const selected = entries_buffer[0..count];
    std.mem.sort(Entry, selected, {}, struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly stored generated columns v1");
    for (selected) |entry| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, entry.name.len, .little);
        state.update(&size);
        state.update(entry.name);
        state.update(&entry.fingerprint);
    }
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}

/// Metadata has no globally fenced proof that every owner is empty. Until a
/// distributed rewrite exists, public ALTER preserves stored-generated
/// semantics even when an unfenced row-count observation appears empty.
pub fn validateSchemaUpdate(alloc: Allocator, previous_json: []const u8, next_json: []const u8) !void {
    const previous = try generatedSchemaIdentity(alloc, previous_json);
    const next = try generatedSchemaIdentity(alloc, next_json);
    if (!std.mem.eql(u8, &previous, &next)) return error.GeneratedColumnRewriteRequired;
}

fn generatedSchemaIdentity(alloc: Allocator, json: []const u8) ![32]u8 {
    if (json.len == 0) return generatedFingerprint(null);
    const raw = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer raw.deinit();
    if (raw.value != .object) return error.InvalidSchemaUpdateRequest;
    const declarations = raw.value.object.get("generated_columns") orelse return generatedFingerprint(null);
    if (declarations == .array and declarations.array.items.len == 0) return generatedFingerprint(null);
    var parsed = try @import("table_schema_impl.zig").parseSchema(alloc, json);
    defer parsed.deinit(alloc);
    const runtime = try @import("mod.zig").deriveRelationalCheckLayout(alloc, parsed);
    errdefer schema.freeSchema(alloc, runtime);
    const set = try Set.createOwned(alloc, runtime, .null, parsed.generated_columns.?.value);
    defer set.deinit();
    return generatedFingerprint(set);
}

test "relational declarations boolean expressions three valued truth tables and lazy failures" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{ .{ .name = "a", .path = "a", .column_type = .boolean }, .{ .name = "b", .path = "b", .column_type = .boolean } } };
    const values = [_]Value{ .null, .{ .boolean = false }, .{ .boolean = true } };
    for ([_][]const u8{ "and", "or" }) |op| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\",\"args\":[{{\"op\":\"column\",\"column\":\"a\"}},{{\"op\":\"column\",\"column\":\"b\"}}]}}", .{op});
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        for (values) |a| for (values) |b| {
            const decisive = std.mem.eql(u8, op, "or");
            const expected: Value = if ((a != .null and a.boolean == decisive) or (b != .null and b.boolean == decisive)) .{ .boolean = decisive } else if (a == .null or b == .null) .null else .{ .boolean = !decisive };
            try std.testing.expectEqualDeep(expected, try plan.evaluate(alloc, &.{ a, b }));
        };
    }
    for ([_]struct { op: []const u8, left: bool }{ .{ .op = "and", .left = false }, .{ .op = "or", .left = true } }) |case| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\",\"args\":[{{\"op\":\"literal\",\"type\":\"boolean\",\"value\":{s}}},{{\"op\":\"eq\",\"args\":[{{\"op\":\"divide\",\"args\":[{{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}},{{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}}]}},{{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}}]}}]}}", .{ case.op, if (case.left) "true" else "false" });
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expectEqual(case.left, (try plan.evaluate(alloc, &.{})).boolean);
    }
    const negated = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"not","args":[{"op":"column","column":"a"}]}
    , .{});
    defer negated.deinit();
    var not_plan = try Plan.init(alloc, table, negated.value, .boolean);
    defer not_plan.deinit();
    for (values) |a| try std.testing.expectEqualDeep(if (a == .null) Value.null else Value{ .boolean = !a.boolean }, try not_plan.evaluate(alloc, &.{a}));
}

test "relational declarations expression comparisons exact integer null distinctness and normalized collation" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    for ([_]struct { json: []const u8, expected: Value }{
        .{ .json = "{\"op\":\"gt\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":9007199254740993},{\"op\":\"literal\",\"type\":\"integer\",\"value\":9007199254740992}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"eq\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .null },
        .{ .json = "{\"op\":\"is_distinct\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_not_distinct\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_null\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_not_null\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"lt\",\"args\":[{\"op\":\"literal\",\"type\":\"blob\",\"value\":\"AA==\"},{\"op\":\"literal\",\"type\":\"blob\",\"value\":\"AQ==\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"eq\",\"args\":[{\"op\":\"literal\",\"type\":\"number\",\"value\":-0.0},{\"op\":\"literal\",\"type\":\"number\",\"value\":0.0}]}", .expected = .{ .boolean = true } },
    }) |case| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, case.json, .{ .parse_numbers = false });
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expectEqualDeep(case.expected, try plan.evaluate(alloc, &.{}));
    }
    var identity: ?[32]u8 = null;
    for ([_][]const u8{ "ci", "case_insensitive", "ANTFLY.CASE_INSENSITIVE" }) |collation| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"eq\",\"collation\":\"{s}\",\"args\":[{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"AbC\"}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"abc\"}}]}}", .{collation});
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expect((try plan.evaluate(alloc, &.{})).boolean);
        if (identity) |previous| try std.testing.expectEqualSlices(u8, &previous, &plan.fingerprint);
        identity = plan.fingerprint;
    }
}

test "relational declarations comparison work shares the row byte budget even for borrowed values" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"eq","args":[{"op":"literal","type":"string","value":"0123456789"},{"op":"literal","type":"string","value":"0123456789"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(alloc, table, parsed.value, .boolean);
    defer plan.deinit();
    const empty: std.json.Value = .{ .object = .empty };
    var budget: usize = 19;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateJsonWithBudget(alloc, empty, &budget));
    budget = 20;
    try std.testing.expect((try plan.evaluateJsonWithBudget(alloc, empty, &budget)).boolean);
    try std.testing.expectEqual(@as(usize, 0), budget);
}

test "relational declarations CHECK expression dependency projection deterministic activation failure and strict writes" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var parsed = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"ratio","expression":{"op":"gte","args":[{"op":"divide","args":[{"op":"column","column":"x"},{"op":"column","column":"y"}]},{"op":"literal","type":"integer","value":0}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]},"y":{"type":["integer","null"]},"unrelated":{"type":"string"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, parsed);
    defer compiled.deinit(alloc);
    const set = compiled.checks.?;
    try std.testing.expectEqual(@as(usize, 2), set.dependency_fields.len);
    for (set.dependency_fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "unrelated"));
    for ([_]struct { json: []const u8, bad: bool }{ .{ .json = "{\"x\":10,\"y\":2}", .bad = false }, .{ .json = "{\"x\":-10,\"y\":2}", .bad = true }, .{ .json = "{\"x\":null,\"y\":2}", .bad = false }, .{ .json = "{}", .bad = false } }) |case| {
        const row = try std.json.parseFromSlice(std.json.Value, alloc, case.json, .{ .parse_numbers = false });
        defer row.deinit();
        try std.testing.expectEqual(case.bad, (try set.firstViolationJson(alloc, row.value)) != null);
    }
    const invalid = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":1,\"y\":0}", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, set.firstViolationJson(alloc, invalid.value));
    const failure = (try set.firstFailureJson(alloc, invalid.value)).?;
    try std.testing.expectEqual(@as(usize, 0), failure.index);
    try std.testing.expectEqual(error.RelationalExpressionDivisionByZero, failure.reason);
}

test "relational declarations CHECK expression rejects ambiguous shapes nonboolean results and rounded integer literals" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    for ([_][]const u8{
        "{\"name\":\"bad\",\"column\":\"x\",\"op\":\"eq\",\"expression\":{\"op\":\"literal\",\"type\":\"boolean\",\"value\":true}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"eq\",\"args\":[{\"op\":\"column\",\"column\":\"x\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":1.00000000000000001}]}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"and\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1},{\"op\":\"literal\",\"type\":\"boolean\",\"value\":true}]}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"eq\",\"collation\":\"ci\",\"args\":[{\"op\":\"column\",\"column\":\"x\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}]}}",
    }) |definition| {
        const json = try std.fmt.allocPrint(alloc, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"checks\":[{s}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"x\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{definition});
        defer alloc.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchemaUpdateRequest(alloc, json));
    }
}

fn checkExpressionAllocationFailure(alloc: Allocator) !void {
    const impl = @import("table_schema_impl.zig");
    var parsed = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"valid","expression":{"op":"or","args":[{"op":"is_null","args":[{"op":"column","column":"x"}]},{"op":"gt","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":0}]}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, parsed);
    defer compiled.deinit(alloc);
    const row = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":2}", .{});
    defer row.deinit();
    try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationJson(alloc, row.value));
}

test "relational declarations CHECK expressions release every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkExpressionAllocationFailure, .{});
}

test "relational declarations scalar expressions checked arithmetic lazy null and ordinal independent identity" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{ .{ .name = "x", .path = "x", .column_type = .integer }, .{ .name = "y", .path = "y", .column_type = .integer } } };
    const expression = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":"1"}]}
    , .{});
    defer expression.deinit();
    var plan = try Plan.init(alloc, table, expression.value, .integer);
    defer plan.deinit();
    try std.testing.expectEqual(@as(i64, 42), (try plan.evaluate(alloc, &.{ .{ .integer = 41 }, .null })).integer);
    try std.testing.expectEqual(Value.null, try plan.evaluate(alloc, &.{ .null, .null }));
    try std.testing.expectError(error.RelationalExpressionOverflow, plan.evaluate(alloc, &.{ .{ .integer = std.math.maxInt(i64) }, .null }));
    var reordered = table;
    reordered.version = 22;
    reordered.relational_columns = &.{ table.relational_columns[1], table.relational_columns[0] };
    var second = try Plan.init(alloc, reordered, expression.value, .integer);
    defer second.deinit();
    try std.testing.expectEqualSlices(u8, &plan.fingerprint, &second.fingerprint);
    const lazy = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"coalesce","args":[{"op":"literal","type":"integer","value":7},{"op":"divide","args":[{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":0}]}]}
    , .{});
    defer lazy.deinit();
    var lazy_plan = try Plan.init(alloc, table, lazy.value, .integer);
    defer lazy_plan.deinit();
    try std.testing.expectEqual(@as(i64, 7), (try lazy_plan.evaluate(alloc, &.{})).integer);
}

test "relational declarations generated graph defaults explicit null and strict restore" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var table = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"integer","value":"4"}}],"generated_columns":[{"column":"z","expression":{"op":"add","args":[{"op":"column","column":"y"},{"op":"literal","type":"integer","value":2}]}},{"column":"y","expression":{"op":"multiply","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":3}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]},"y":{"type":["integer","null"]},"z":{"type":["integer","null"]}},"required":["x","y","z"],"additionalProperties":false}}}}
    );
    defer table.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, table);
    defer compiled.deinit(alloc);
    const set = compiled.expressions.?;
    var empty = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
    defer empty.deinit();
    try set.applyJson(empty.arena.allocator(), &empty.value);
    try std.testing.expectEqual(@as(i64, 4), empty.value.object.get("x").?.integer);
    try std.testing.expectEqual(@as(i64, 12), empty.value.object.get("y").?.integer);
    try std.testing.expectEqual(@as(i64, 14), empty.value.object.get("z").?.integer);
    try set.verifyJson(alloc, empty.value);
    try empty.value.object.put(empty.arena.allocator(), "z", .{ .integer = 99 });
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyJson(alloc, empty.value));
    var explicit_null = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":null,\"y\":\"ignored\"}", .{});
    defer explicit_null.deinit();
    try set.applyJson(explicit_null.arena.allocator(), &explicit_null.value);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("x").?);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("y").?);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("z").?);
    try impl.validateDocumentJson(alloc, table, "{}");
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"x","expression":{"op":"column","column":"y"}},{"column":"y","expression":{"op":"column","column":"x"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}},"additionalProperties":false}}}}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"column","column":"x"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    ));
}

test "relational declarations scalar expression rejects mixed types unknown fields and bounded allocation" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "x", .path = "x", .column_type = .string }} };
    inline for (.{
        "{\"op\":\"now\"}",
        "{\"op\":\"column\",\"column\":\"x\",\"args\":[]}",
        "{\"op\":\"add\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1},{\"op\":\"literal\",\"type\":\"number\",\"value\":1}]}",
    }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        if (Plan.init(alloc, table, parsed.value, .integer)) |good| {
            var unexpected = good;
            unexpected.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"concat","args":[{"op":"column","column":"x"},{"op":"column","column":"x"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(alloc, table, parsed.value, .string);
    defer plan.deinit();
    const bytes = try alloc.alloc(u8, max_output_bytes);
    defer alloc.free(bytes);
    @memset(bytes, 'a');
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluate(alloc, &.{.{ .string = bytes }}));
}

test "relational declarations scalar compiler depth and node budgets are enforced before evaluation" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, owned, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}", .{});
    var value = parsed.value;
    for (0..max_depth) |_| {
        var args = std.array_list.Managed(std.json.Value).init(owned);
        try args.append(value);
        var object = std.json.ObjectMap.empty;
        try object.put(owned, "op", .{ .string = "negate" });
        try object.put(owned, "args", .{ .array = args });
        value = .{ .object = object };
    }
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, Plan.init(alloc, table, value, .integer));
    var inner_args = std.array_list.Managed(std.json.Value).init(owned);
    for (0..32) |_| try inner_args.append(parsed.value);
    var inner = std.json.ObjectMap.empty;
    try inner.put(owned, "op", .{ .string = "coalesce" });
    try inner.put(owned, "args", .{ .array = inner_args });
    var outer_args = std.array_list.Managed(std.json.Value).init(owned);
    for (0..4) |_| try outer_args.append(.{ .object = inner });
    var outer = std.json.ObjectMap.empty;
    try outer.put(owned, "op", .{ .string = "coalesce" });
    try outer.put(owned, "args", .{ .array = outer_args });
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, Plan.init(alloc, table, .{ .object = outer }, .integer));
}

test "relational declarations generated FK assignment actions cannot overwrite derived child columns" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"child","expression":{"op":"column","column":"source"}}],"foreign_keys":[{"name":"parent","child_columns":["child"],"parent_table":"parents","parent_columns":["id"],"on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"source":{"type":"integer"},"child":{"type":["integer","null"]}},"additionalProperties":false}}}}
    ;
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc, json));
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    const foreign_key = &parsed.value.object.getPtr("foreign_keys").?.array.items[0];
    try foreign_key.object.put(parsed.arena.allocator(), "on_update", .{ .string = "no_action" });
    try foreign_key.object.put(parsed.arena.allocator(), "on_delete", .{ .string = "set_null" });
    const deletion = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(deletion);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc, deletion));
}

test "relational declarations metadata generated update admission precedes catalog publication" {
    const alloc = std.testing.allocator;
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"y","expression":{"op":"column","column":"x"}},{"column":"z","expression":{"op":"column","column":"y"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"z":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const manager = @import("../metadata/table_manager.zig");
    const tables = @import("../api/tables.zig");
    const table: manager.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = json, .indexes_json = "{}" };
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const arena = parsed.arena.allocator();
    const declarations = parsed.value.object.getPtr("generated_columns").?.array.items;
    std.mem.swap(std.json.Value, &declarations[0], &declarations[1]);
    const default_value = parsed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?;
    default_value.* = .{ .string = "4" };
    const reordered = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    const updated = try tables.applySchemaUpdateRecord(alloc, &table, reordered);
    defer manager.freeTable(alloc, updated);
    const y_expression = declarations[1].object.getPtr("expression").?;
    y_expression.* = (try std.json.parseFromSlice(std.json.Value, arena, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":99}", .{})).value;
    const changed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &table, changed));
    try std.testing.expectEqualStrings(json, table.schema_json);
    _ = parsed.value.object.orderedRemove("generated_columns");
    const removed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &table, removed));
    const plain: manager.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = removed, .indexes_json = "{}" };
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &plain, json));
}

test "relational declarations omitted scalar literal value is typed NULL for generated SDKs" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    const missing = try std.json.parseFromSlice(std.json.Value, alloc, "{\"op\":\"literal\",\"type\":\"integer\"}", .{});
    defer missing.deinit();
    const explicit = try std.json.parseFromSlice(std.json.Value, alloc, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":null}", .{});
    defer explicit.deinit();
    var left = try Plan.init(alloc, table, missing.value, .integer);
    defer left.deinit();
    var right = try Plan.init(alloc, table, explicit.value, .integer);
    defer right.deinit();
    try std.testing.expectEqual(Value.null, try left.evaluate(alloc, &.{}));
    try std.testing.expectEqualSlices(u8, &left.fingerprint, &right.fingerprint);
}

test "relational declarations cold generated verification reads dependency cells only and rejects forged output" {
    const alloc = std.testing.allocator;
    var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"y","expression":{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":1}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"payload":{"type":"blob"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(alloc);
    try std.testing.expect(!validator.restore.full_root);
    const FakeRow = struct {
        table_schema: schema.TableSchema,
        reads: usize = 0,
        generated: i64 = 3,
        missing: bool = false,
        pub fn ordinalForName(self: *@This(), name: []const u8) ?usize {
            for (self.table_schema.relational_columns, 0..) |column, i| if (std.mem.eql(u8, column.name, name)) return i;
            return null;
        }
        pub fn findCell(self: *@This(), ordinal: usize) !?@import("../storage/db/algebraic/relational_row_codec.zig").Cell {
            const name = self.table_schema.relational_columns[ordinal].name;
            if (std.mem.eql(u8, name, "payload")) return error.TestUnexpectedWidePayloadRead;
            self.reads += 1;
            const generated = std.mem.eql(u8, name, "y");
            if (generated and self.missing) return null;
            return .{ .ordinal = @intCast(ordinal), .path = name, .value_type = .i64_val, .value = .{ .i64_val = if (generated) self.generated else 2 } };
        }
    };
    var row: FakeRow = .{ .table_schema = validator.execution.expressions.?.table };
    try validator.execution.expressions.?.verifyRow(alloc, &row);
    try std.testing.expectEqual(@as(usize, 2), row.reads);
    row.generated = 99;
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, validator.execution.expressions.?.verifyRow(alloc, &row));
    row.missing = true;
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, validator.execution.expressions.?.verifyRow(alloc, &row));
}
