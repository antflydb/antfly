// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bound scalar programs. Binding owns constants and resolves names, function
//! identities, column ordinals and parameter types once. Evaluation follows
//! typed instruction indices with bounded work; lazy branches preserve SQL
//! three-valued logic and do not evaluate unreachable errors.
const std = @import("std");
const ast = @import("ast.zig");
const datetime = @import("../datetime.zig");
const json_order = @import("json_order.zig");
const Allocator = std.mem.Allocator;
const Json = std.json.Value;
pub const Datum = struct {
    value: Json = .null,
    sql_null: bool = true,

    pub fn fromJson(value: Json) Datum {
        return .{ .value = value, .sql_null = value == .null };
    }
    pub fn json(value: Json) Datum {
        return .{ .value = value, .sql_null = false };
    }
};
pub const Type = struct { kind: ?ast.ColumnType = null, nullable: bool = true };
pub const Column = struct { name: []const u8, type: ast.ColumnType, nullable: bool = true };
pub const BindLimits = struct { nodes: usize = 8192, depth: usize = 64, parameters: usize = 1024 };
pub const EvalLimits = struct { steps: usize = 65_536, depth: usize = 64, output_bytes: usize = 1024 * 1024 };
pub const Function = enum { abs, lower, upper, length, octet_length, concat, coalesce, nullif, greatest, least, ceil, floor, round, sqrt, power, mod, substring, trim, ltrim, rtrim, replace, starts_with, date_part, date_trunc, to_timestamp, @"$single", @"$pattern_quantified" };

pub const Instruction = struct {
    type: Type,
    operation: union(enum) {
        literal: Json,
        column: u32,
        parameter: u32,
        unary: struct { op: ast.Scalar.Unary, operand: u32 },
        binary: struct { op: ast.Scalar.Binary, left: u32, right: u32 },
        call: struct { function: Function, args: []const u32 },
        cast: struct { operand: u32, type: ast.ColumnType },
        case_when: struct { branches: []const Branch, otherwise: ?u32 },
        in_list: struct { operand: u32, values: []const u32, negated: bool },
    },
    const Branch = struct { condition: u32, value: u32 };
};

pub const Program = struct {
    arena: std.heap.ArenaAllocator,
    instructions: []const Instruction,
    root: u32,
    output_type: Type,
    parameter_types: []const ?ast.ColumnType,
    required_columns: []const u32,

    pub fn deinit(self: *Program) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Cells use the ordinal order supplied to bind(), including unselected
    /// NULL placeholders. Borrowed scalar results remain valid while the
    /// program, cells and parameters live; computed strings use alloc.
    pub fn evaluate(self: *const Program, alloc: Allocator, cells: []const Datum, parameters: []const Json, limits: EvalLimits) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = cells, .parameters = parameters, .limits = limits };
        const result = try context.runDatum(self.root, 0);
        _ = try context.validateJson(result.value, 0);
        return result;
    }
};

pub fn bind(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, limits: BindLimits) !Program {
    return bindExpected(alloc, expression, columns, parameter_hints, null, limits);
}

/// Constraint pass for statement-wide inference. Unconstrained parameters
/// remain unknown; callers may repeat over all programs until no type changes,
/// then choose protocol defaults only for positions still unconstrained.
pub fn inferParameters(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits) !bool {
    if (limits.parameters > 1024 or parameters.len > limits.parameters) return error.SqlProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .allow_unresolved = true };
    for (columns, 0..) |column, i| try binder.names.put(binder.alloc, column.name, @intCast(i));
    @memcpy(binder.parameters[0..parameters.len], parameters);
    _ = try binder.compile(expression, expected, 0);
    if (binder.parameter_count > parameters.len) return error.InvalidSqlParameters;
    var changed = false;
    for (parameters, binder.parameters[0..parameters.len]) |*output, inferred| if (inferred) |kind| {
        if (output.* == null) changed = true;
        output.* = kind;
    };
    return changed;
}

/// Read a provisional result type without defaulting unresolved parameters or
/// NULL literals. Relation-wide inference uses this before emitting programs.
pub fn inferOutput(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []const ?ast.ColumnType) !Type {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = .{}, .allow_unresolved = true };
    if (parameters.len > binder.parameters.len) return error.SqlProgramLimitExceeded;
    @memcpy(binder.parameters[0..parameters.len], parameters);
    for (columns, 0..) |column, index| try binder.names.put(binder.alloc, column.name, @intCast(index));
    return binder.infer(expression, 0);
}

pub fn bindExpected(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits) !Program {
    if (limits.parameters > 1024 or parameter_hints.len > limits.parameters or limits.nodes == 0) return error.SqlProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits };
    for (columns, 0..) |column, i| {
        const entry = try binder.names.getOrPut(binder.alloc, column.name);
        if (entry.found_existing) return error.AmbiguousSqlColumn;
        entry.value_ptr.* = @intCast(i);
    }
    @memcpy(binder.parameters[0..parameter_hints.len], parameter_hints);
    binder.parameter_count = parameter_hints.len;
    const root = try binder.compile(expression, expected, 0);
    const output_type = binder.instructions.items[root].type;
    return .{
        .arena = arena,
        .instructions = try binder.instructions.toOwnedSlice(binder.alloc),
        .root = root,
        .output_type = output_type,
        .parameter_types = try binder.alloc.dupe(?ast.ColumnType, binder.parameters[0..binder.parameter_count]),
        .required_columns = try binder.dependencies.toOwnedSlice(binder.alloc),
    };
}

fn numeric(kind: ?ast.ColumnType) bool {
    return kind == .integer or kind == .number;
}
fn common(left: Type, right: Type) !Type {
    const kind = if (left.kind == null) right.kind else if (right.kind == null) left.kind else if (left.kind == right.kind) left.kind else if (numeric(left.kind) and numeric(right.kind)) ast.ColumnType.number else return error.SqlTypeMismatch;
    return .{ .kind = kind, .nullable = left.nullable or right.nullable };
}
fn literalType(value: ast.Value) Type {
    return .{ .kind = switch (value) {
        .null, .parameter => null,
        .boolean => .boolean,
        .integer => .integer,
        .number => .number,
        .string => .string,
    }, .nullable = value == .null or value == .parameter };
}
fn functionId(name: []const u8) !Function {
    inline for (std.meta.fields(Function)) |field| if (std.mem.eql(u8, name, field.name)) return @enumFromInt(field.value);
    if (std.mem.eql(u8, name, "char_length") or std.mem.eql(u8, name, "character_length")) return .length;
    if (std.mem.eql(u8, name, "ceiling")) return .ceil;
    if (std.mem.eql(u8, name, "substr")) return .substring;
    if (std.mem.eql(u8, name, "btrim")) return .trim;
    return error.UnsupportedSqlShape;
}
fn arity(function: Function, count: usize) !void {
    const valid = switch (function) {
        .abs, .lower, .upper, .length, .octet_length, .ceil, .floor, .round, .sqrt, .to_timestamp => count == 1,
        .nullif, .power, .mod, .starts_with, .date_part, .date_trunc, .@"$single" => count == 2,
        .@"$pattern_quantified" => count == 5,
        .substring => count == 2 or count == 3,
        .replace => count == 3,
        .trim, .ltrim, .rtrim => count == 1 or count == 2,
        .coalesce, .greatest, .least => count > 0,
        .concat => true,
    };
    if (!valid) return error.InvalidSqlParameters;
}

const Binder = struct {
    alloc: Allocator,
    columns: []const Column,
    limits: BindLimits,
    names: std.StringHashMapUnmanaged(u32) = .empty,
    inferred: std.AutoHashMapUnmanaged(*const ast.Scalar, Type) = .empty,
    instructions: std.ArrayList(Instruction) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    parameters: [1024]?ast.ColumnType = @splat(null),
    parameter_count: usize = 0,
    allow_unresolved: bool = false,

    fn infer(self: *Binder, expression: *const ast.Scalar, depth: usize) anyerror!Type {
        if (depth >= self.limits.depth) return error.SqlProgramLimitExceeded;
        if (self.inferred.get(expression)) |cached| return cached;
        if (self.inferred.count() >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const result: Type = switch (expression.*) {
            .literal => |value| if (value == .parameter) blk: {
                if (value.parameter == 0 or value.parameter > self.limits.parameters) return error.InvalidSqlParameters;
                break :blk .{ .kind = self.parameters[value.parameter - 1] };
            } else literalType(value),
            .column => |name| blk: {
                const column = self.columns[self.names.get(name) orelse return error.UnknownColumn];
                break :blk .{ .kind = column.type, .nullable = column.nullable };
            },
            .cast => |cast| .{ .kind = cast.type, .nullable = (try self.infer(cast.operand, depth + 1)).nullable },
            .unary => |unary| blk: {
                const input = try self.infer(unary.operand, depth + 1);
                switch (unary.op) {
                    .positive, .negative => {
                        if (input.kind != null and !numeric(input.kind)) return error.SqlTypeMismatch;
                        break :blk input;
                    },
                    .not, .is_true, .is_not_true, .is_false, .is_not_false => if (input.kind != null and input.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = .boolean, .nullable = unary.op == .not and input.nullable };
            },
            .binary => |binary| blk: {
                const left = try self.infer(binary.left, depth + 1);
                const right = try self.infer(binary.right, depth + 1);
                if (binary.op == .json_get or binary.op == .json_text) {
                    if (left.kind != null and left.kind != .json) return error.SqlTypeMismatch;
                    if (right.kind != null and right.kind != .string and right.kind != .integer) return error.SqlTypeMismatch;
                    break :blk .{ .kind = if (binary.op == .json_get) .json else .string };
                }
                const merged = try common(left, right);
                switch (binary.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {
                        if (merged.kind != null and !numeric(merged.kind)) return error.SqlTypeMismatch;
                        break :blk merged;
                    },
                    .concat, .like, .ilike => if (merged.kind != null and merged.kind != .string) return error.SqlTypeMismatch,
                    .@"and", .@"or" => if (merged.kind != null and merged.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = if (binary.op == .concat) .string else .boolean, .nullable = if (binary.op == .is_distinct or binary.op == .is_not_distinct) false else merged.nullable };
            },
            .call => |call| blk: {
                if (call.subquery != null or call.window != null or call.star or call.distinct or call.filter != null) return error.UnsupportedSqlShape;
                if (std.mem.eql(u8, call.name, "$validate")) {
                    if (call.args.len == 0) return error.InvalidSqlParameters;
                    for (call.args) |arg| _ = try self.infer(arg, depth + 1);
                    break :blk .{ .kind = .integer, .nullable = false };
                }
                const function = try functionId(call.name);
                try arity(function, call.args.len);
                if (function == .@"$pattern_quantified") {
                    for (call.args, 0..) |arg, index| {
                        const actual = try self.infer(arg, depth + 1);
                        const required: ast.ColumnType = if (index == 0) .string else if (index == 1) .json else .boolean;
                        if (actual.kind != null and actual.kind != required) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .boolean, .nullable = true };
                }
                var merged: Type = .{};
                switch (function) {
                    .@"$single" => merged = try self.infer(call.args[0], depth + 1),
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .sqrt, .power, .mod => for (call.args) |arg| {
                        merged = try common(merged, try self.infer(arg, depth + 1));
                    },
                    else => for (call.args) |arg| {
                        _ = try self.infer(arg, depth + 1);
                    },
                }
                if (function == .abs or function == .ceil or function == .floor or function == .round or function == .sqrt or function == .power or function == .mod) {
                    if (merged.kind != null and !numeric(merged.kind)) return error.SqlTypeMismatch;
                }
                break :blk .{ .kind = switch (function) {
                    .length, .octet_length => .integer,
                    .starts_with, .@"$pattern_quantified" => .boolean,
                    .sqrt, .power, .date_part => .number,
                    .date_trunc, .to_timestamp => .datetime,
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .mod, .@"$single" => merged.kind,
                    else => .string,
                }, .nullable = function != .concat };
            },
            .case_when => |case| blk: {
                var merged: Type = if (case.otherwise) |other| try self.infer(other, depth + 1) else .{};
                for (case.branches) |branch| {
                    const condition = try self.infer(branch.condition, depth + 1);
                    if (condition.kind != null and condition.kind != .boolean) return error.SqlTypeMismatch;
                    merged = try common(merged, try self.infer(branch.value, depth + 1));
                }
                break :blk merged;
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| merged = try common(merged, try self.infer(item, depth + 1));
                break :blk .{ .kind = .boolean, .nullable = merged.nullable };
            },
        };
        try self.inferred.put(self.alloc, expression, result);
        return result;
    }

    fn compile(self: *Binder, expression: *const ast.Scalar, expected: ?ast.ColumnType, depth: usize) anyerror!u32 {
        if (depth >= self.limits.depth or self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        var kind = try self.infer(expression, depth);
        if (kind.kind == null) kind.kind = expected;
        var instruction: Instruction = .{ .type = kind, .operation = undefined };
        instruction.operation = switch (expression.*) {
            .literal => |value| if (value == .parameter) blk: {
                const resolved = kind.kind;
                if (resolved == null and !self.allow_unresolved) return error.UnknownSqlParameterType;
                const index = value.parameter - 1;
                if (self.parameters[index]) |prior| {
                    if (resolved != null and prior != resolved.?) return error.ConflictingSqlParameterTypes;
                }
                if (resolved != null) self.parameters[index] = resolved;
                self.parameter_count = @max(self.parameter_count, value.parameter);
                break :blk .{ .parameter = index };
            } else .{ .literal = switch (value) {
                .null => .null,
                .boolean => |v| .{ .bool = v },
                .integer => |v| .{ .integer = v },
                .number => |v| .{ .float = v },
                .string => |v| .{ .string = try self.alloc.dupe(u8, v) },
                .parameter => unreachable,
            } },
            .column => |name| blk: {
                const ordinal = self.names.get(name).?;
                if (std.mem.indexOfScalar(u32, self.dependencies.items, ordinal) == null) try self.dependencies.append(self.alloc, ordinal);
                break :blk .{ .column = ordinal };
            },
            .cast => |cast| .{ .cast = .{ .operand = try self.compile(cast.operand, cast.type, depth + 1), .type = cast.type } },
            .unary => |unary| .{ .unary = .{ .op = unary.op, .operand = try self.compile(unary.operand, switch (unary.op) {
                .not, .is_true, .is_not_true, .is_false, .is_not_false => .boolean,
                .positive, .negative => kind.kind,
                else => null,
            }, depth + 1) } },
            .binary => |binary| blk: {
                if (binary.op == .json_get or binary.op == .json_text) break :blk .{ .binary = .{ .op = binary.op, .left = try self.compile(binary.left, .json, depth + 1), .right = try self.compile(binary.right, .string, depth + 1) } };
                const merged = try common(try self.infer(binary.left, depth + 1), try self.infer(binary.right, depth + 1));
                const operand_kind: ?ast.ColumnType = switch (binary.op) {
                    .@"and", .@"or" => .boolean,
                    .concat, .like, .ilike => .string,
                    .add, .subtract, .multiply, .divide, .modulo => merged.kind orelse expected,
                    else => merged.kind,
                };
                break :blk .{ .binary = .{ .op = binary.op, .left = try self.compile(binary.left, operand_kind, depth + 1), .right = try self.compile(binary.right, operand_kind, depth + 1) } };
            },
            .call => |call| blk: {
                if (std.mem.eql(u8, call.name, "$validate")) {
                    // Discarded EXISTS projections still bind names, types,
                    // functions and parameters. Their code and column reads
                    // must not become runtime dependencies of the count.
                    var scratch = std.heap.ArenaAllocator.init(self.alloc);
                    defer scratch.deinit();
                    var validator: Binder = .{
                        .alloc = scratch.allocator(),
                        .columns = self.columns,
                        .limits = self.limits,
                        .names = self.names,
                        .parameters = self.parameters,
                        .parameter_count = self.parameter_count,
                        .allow_unresolved = self.allow_unresolved,
                    };
                    for (call.args) |arg| _ = try validator.compile(arg, null, depth + 1);
                    self.parameters = validator.parameters;
                    self.parameter_count = validator.parameter_count;
                    break :blk .{ .literal = .{ .integer = 1 } };
                }
                const function = try functionId(call.name);
                const args = try self.alloc.alloc(u32, call.args.len);
                for (call.args, args, 0..) |arg, *out, i| {
                    const desired: ?ast.ColumnType = switch (function) {
                        .@"$single" => if (i == 0) kind.kind else .integer,
                        .@"$pattern_quantified" => if (i == 0) .string else if (i == 1) .json else .boolean,
                        .lower, .upper, .length, .octet_length, .trim, .ltrim, .rtrim, .replace, .starts_with => .string,
                        .substring => if (i == 0) .string else .integer,
                        .date_part, .date_trunc => if (i == 0) .string else .datetime,
                        .to_timestamp => .number,
                        .concat => null,
                        else => kind.kind,
                    };
                    const actual = try self.infer(arg, depth + 1);
                    if (desired != null and actual.kind != null and desired != actual.kind and !(desired == .datetime and actual.kind == .string) and !(numeric(desired) and numeric(actual.kind))) return error.SqlTypeMismatch;
                    out.* = try self.compile(arg, desired, depth + 1);
                }
                break :blk .{ .call = .{ .function = function, .args = args } };
            },
            .case_when => |case| blk: {
                const branches = try self.alloc.alloc(Instruction.Branch, case.branches.len);
                for (case.branches, branches) |branch, *out| out.* = .{ .condition = try self.compile(branch.condition, .boolean, depth + 1), .value = try self.compile(branch.value, kind.kind, depth + 1) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (case.otherwise) |other| try self.compile(other, kind.kind, depth + 1) else null } };
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| merged = try common(merged, try self.infer(item, depth + 1));
                const values = try self.alloc.alloc(u32, list.values.len);
                for (list.values, values) |item, *out| out.* = try self.compile(item, merged.kind, depth + 1);
                break :blk .{ .in_list = .{ .operand = try self.compile(list.operand, merged.kind, depth + 1), .values = values, .negated = list.negated } };
            },
        };
        if (self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const index: u32 = @intCast(self.instructions.items.len);
        try self.instructions.append(self.alloc, instruction);
        return index;
    }
};

const Evaluator = struct {
    program: *const Program,
    alloc: Allocator,
    cells: []const Datum,
    parameters: []const Json,
    limits: EvalLimits,
    steps: usize = 0,
    bytes: usize = 0,

    fn charge(self: *Evaluator, bytes: usize) !void {
        if (bytes > self.limits.output_bytes -| self.bytes) return error.SqlProgramLimitExceeded;
        self.bytes += bytes;
    }

    fn run(self: *Evaluator, index: u32, depth: usize) anyerror!Json {
        return (try self.runDatum(index, depth)).value;
    }

    fn runDatum(self: *Evaluator, index: u32, depth: usize) anyerror!Datum {
        if (depth >= self.limits.depth or self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
        self.steps += 1;
        const instruction = self.program.instructions[index];
        var result: Datum = switch (instruction.operation) {
            .parameter => |slot| blk: {
                if (slot >= self.parameters.len) return error.InvalidSqlParameters;
                const value = self.parameters[slot];
                if (value == .null) break :blk .{};
                break :blk Datum.json(try self.convert(value, instruction.type.kind.?));
            },
            .column => |ordinal| if (ordinal < self.cells.len) self.cells[ordinal] else return error.InvalidSqlBackendResponse,
            .cast => |cast| blk: {
                const datum = try self.runDatum(cast.operand, depth + 1);
                if (datum.sql_null) break :blk .{};
                if (cast.type == .string and self.program.instructions[cast.operand].type.kind == .json) break :blk Datum.json(.{ .string = try self.jsonText(datum.value) });
                if (datum.value == .null and cast.type == .string) break :blk Datum.json(.{ .string = "null" });
                if (datum.value == .null and cast.type != .json) return error.SqlTypeMismatch;
                break :blk Datum.json(try self.convert(datum.value, cast.type));
            },
            .unary => |unary| if (unary.op == .is_null or unary.op == .is_not_null) blk: {
                const datum = try self.runDatum(unary.operand, depth + 1);
                break :blk Datum.json(.{ .bool = datum.sql_null == (unary.op == .is_null) });
            } else Datum.fromJson(try self.runLegacy(index, depth)),
            .binary => |binary| if (binary.op == .json_get or binary.op == .json_text) blk: {
                const left = try self.runDatum(binary.left, depth + 1);
                const right = try self.runDatum(binary.right, depth + 1);
                if (left.sql_null or right.sql_null) break :blk .{};
                const value: Json = if (left.value == .object and right.value == .string)
                    left.value.object.get(right.value.string) orelse break :blk .{}
                else if (left.value == .array and right.value == .integer) selected: {
                    const items = left.value.array.items;
                    const slot = if (right.value.integer < 0) @as(i128, @intCast(items.len)) + right.value.integer else right.value.integer;
                    if (slot < 0 or slot >= items.len) break :blk .{};
                    break :selected items[@intCast(slot)];
                } else break :blk .{};
                if (binary.op == .json_get) break :blk Datum.json(value);
                if (value == .null) break :blk .{};
                break :blk Datum.json(.{ .string = if (value == .string) value.string else try self.jsonText(value) });
            } else if (binary.op == .eq or binary.op == .neq or binary.op == .lt or binary.op == .lte or binary.op == .gt or binary.op == .gte or binary.op == .is_distinct or binary.op == .is_not_distinct) blk: {
                const left = try self.runDatum(binary.left, depth + 1);
                const right = try self.runDatum(binary.right, depth + 1);
                if (binary.op == .is_distinct or binary.op == .is_not_distinct) {
                    const equal = if (left.sql_null or right.sql_null) left.sql_null and right.sql_null else (try compare(left.value, right.value)) == .eq;
                    break :blk Datum.json(.{ .bool = equal == (binary.op == .is_not_distinct) });
                }
                if (left.sql_null or right.sql_null) break :blk .{};
                break :blk Datum.json(comparison(binary.op, try compare(left.value, right.value)));
            } else Datum.fromJson(try self.runLegacy(index, depth)),
            .case_when => |case| blk: {
                for (case.branches) |branch| {
                    const condition = try self.runDatum(branch.condition, depth + 1);
                    if (!condition.sql_null) {
                        if (condition.value != .bool) return error.SqlTypeMismatch;
                        if (condition.value.bool) break :blk try self.runDatum(branch.value, depth + 1);
                    }
                }
                break :blk if (case.otherwise) |other| try self.runDatum(other, depth + 1) else .{};
            },
            .in_list => |list| blk: {
                const operand = try self.runDatum(list.operand, depth + 1);
                if (operand.sql_null) break :blk .{};
                var unknown = false;
                for (list.values) |item| {
                    const value = try self.runDatum(item, depth + 1);
                    if (value.sql_null) {
                        unknown = true;
                        continue;
                    }
                    if ((try compare(operand.value, value.value)) == .eq) break :blk Datum.json(.{ .bool = !list.negated });
                }
                break :blk if (unknown) .{} else Datum.json(.{ .bool = list.negated });
            },
            .call => |call| blk: {
                switch (call.function) {
                    .@"$single" => {
                        const count = try self.runDatum(call.args[1], depth + 1);
                        if (!count.sql_null and (count.value != .integer or count.value.integer > 1)) return error.SqlCardinalityViolation;
                        break :blk try self.runDatum(call.args[0], depth + 1);
                    },
                    .coalesce => {
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (!datum.sql_null) break :blk datum;
                        }
                        break :blk .{};
                    },
                    .nullif => {
                        const left = try self.runDatum(call.args[0], depth + 1);
                        const right = try self.runDatum(call.args[1], depth + 1);
                        break :blk if (left.sql_null or (!right.sql_null and (try compare(left.value, right.value)) == .eq)) .{} else left;
                    },
                    .greatest, .least => {
                        var best: Datum = .{};
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (datum.sql_null) continue;
                            if (best.sql_null or (try compare(datum.value, best.value)) == (if (call.function == .greatest) std.math.Order.gt else .lt)) best = datum;
                        }
                        break :blk best;
                    },
                    .concat => {
                        var output: std.ArrayList(u8) = .empty;
                        errdefer output.deinit(self.alloc);
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (datum.sql_null) continue;
                            const string = if (self.program.instructions[arg].type.kind == .json) try self.jsonText(datum.value) else try self.formatText(datum.value);
                            try self.charge(string.len);
                            try output.appendSlice(self.alloc, string);
                        }
                        break :blk Datum.json(.{ .string = try output.toOwnedSlice(self.alloc) });
                    },
                    else => break :blk Datum.fromJson(try self.invokeFunction(call.function, call.args, depth + 1)),
                }
            },
            else => Datum.fromJson(try self.runLegacy(index, depth)),
        };
        if (!result.sql_null and instruction.type.kind == .number and result.value == .integer) result.value = try finite(@floatFromInt(result.value.integer));
        return result;
    }

    fn runLegacy(self: *Evaluator, index: u32, depth: usize) anyerror!Json {
        const instruction = self.program.instructions[index];
        return switch (instruction.operation) {
            .literal => |value| value,
            .column => |ordinal| if (ordinal < self.cells.len) self.cells[ordinal].value else error.InvalidSqlBackendResponse,
            .parameter => |slot| if (slot < self.parameters.len) try self.convert(self.parameters[slot], instruction.type.kind.?) else error.InvalidSqlParameters,
            .cast => |cast| try self.convert(try self.run(cast.operand, depth + 1), cast.type),
            .unary => |unary| blk: {
                const value = try self.run(unary.operand, depth + 1);
                if (unary.op == .is_null or unary.op == .is_not_null) break :blk .{ .bool = (value == .null) == (unary.op == .is_null) };
                if (unary.op == .is_true or unary.op == .is_not_true or unary.op == .is_false or unary.op == .is_not_false) {
                    const target = unary.op == .is_true or unary.op == .is_not_true;
                    const matches = value == .bool and value.bool == target;
                    break :blk .{ .bool = matches != (unary.op == .is_not_true or unary.op == .is_not_false) };
                }
                if (value == .null) break :blk .null;
                break :blk switch (unary.op) {
                    .not => if (value == .bool) .{ .bool = !value.bool } else error.SqlTypeMismatch,
                    .positive => if (value == .integer or value == .float) value else error.SqlTypeMismatch,
                    .negative => switch (value) {
                        .integer => |v| .{ .integer = std.math.negate(v) catch return error.SqlNumericOutOfRange },
                        .float => |v| finite(-v),
                        else => error.SqlTypeMismatch,
                    },
                    else => unreachable,
                };
            },
            .binary => |binary| blk: {
                const left = try self.run(binary.left, depth + 1);
                if (binary.op == .@"and" and left == .bool and !left.bool) break :blk left;
                if (binary.op == .@"or" and left == .bool and left.bool) break :blk left;
                const right = try self.run(binary.right, depth + 1);
                if (binary.op == .@"and" or binary.op == .@"or") {
                    if ((left != .null and left != .bool) or (right != .null and right != .bool)) return error.SqlTypeMismatch;
                    const decisive = binary.op == .@"or";
                    if ((left == .bool and left.bool == decisive) or (right == .bool and right.bool == decisive)) break :blk .{ .bool = decisive };
                    break :blk if (left == .null or right == .null) .null else .{ .bool = !decisive };
                }
                if (binary.op == .is_distinct or binary.op == .is_not_distinct) {
                    const equal = if (left == .null or right == .null) left == .null and right == .null else (try compare(left, right)) == .eq;
                    break :blk .{ .bool = equal == (binary.op == .is_not_distinct) };
                }
                if (left == .null or right == .null) break :blk .null;
                break :blk switch (binary.op) {
                    .add, .subtract, .multiply, .divide, .modulo => try arithmetic(binary.op, left, right),
                    .concat => try self.concat(&.{ left, right }, false),
                    .like, .ilike => .{ .bool = try self.like(left, right, binary.op == .ilike) },
                    .eq, .neq, .lt, .lte, .gt, .gte => comparison(binary.op, try compare(left, right)),
                    else => unreachable,
                };
            },
            .call => |call| try self.invokeFunction(call.function, call.args, depth + 1),
            .case_when => |case| blk: {
                for (case.branches) |branch| {
                    const condition = try self.run(branch.condition, depth + 1);
                    if (condition == .bool and condition.bool) break :blk try self.run(branch.value, depth + 1);
                    if (condition != .bool and condition != .null) return error.SqlTypeMismatch;
                }
                break :blk if (case.otherwise) |other| try self.run(other, depth + 1) else .null;
            },
            .in_list => unreachable,
        };
    }

    fn jsonText(self: *Evaluator, value: Json) ![]const u8 {
        _ = try self.validateJson(value, 0);
        var writer: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer writer.deinit();
        // The execution allocator also carries the statement-wide hard cap.
        // Count before producing output so a large JSON subtree cannot evade
        // this scalar's independent output bound.
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try std.json.Stringify.value(value, .{}, &counter.writer);
        try self.charge(counter.count);
        std.json.Stringify.value(value, .{}, &writer.writer) catch return error.OutOfMemory;
        return writer.toOwnedSlice();
    }

    fn validateJson(self: *Evaluator, value: Json, depth: usize) error{ SqlProgramLimitExceeded, SqlTypeMismatch }!usize {
        if (depth >= self.limits.depth or self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
        self.steps += 1;
        var size: usize = 8;
        switch (value) {
            .string => |string| {
                if (!std.unicode.utf8ValidateSlice(string)) return error.SqlTypeMismatch;
                size = string.len;
            },
            .number_string => |string| size = string.len,
            .array => |array| for (array.items) |item| {
                size +|= try self.validateJson(item, depth + 1);
            },
            .object => |object| for (object.keys(), object.values()) |key, item| {
                size +|= key.len +| try self.validateJson(item, depth + 1);
            },
            else => {},
        }
        if (size > self.limits.output_bytes) return error.SqlProgramLimitExceeded;
        return size;
    }

    fn convert(self: *Evaluator, value: Json, kind: ast.ColumnType) anyerror!Json {
        if (value == .null) return .null;
        return switch (kind) {
            .integer => switch (value) {
                .integer => value,
                .float => |v| blk: {
                    const rounded = @round(v);
                    if (!std.math.isFinite(rounded) or rounded < -9223372036854775808.0 or rounded >= 9223372036854775808.0) return error.SqlNumericOutOfRange;
                    break :blk .{ .integer = @intFromFloat(rounded) };
                },
                .string, .number_string => |v| .{ .integer = std.fmt.parseInt(i64, v, 10) catch return error.SqlTypeMismatch },
                .bool => |v| .{ .integer = @intFromBool(v) },
                else => error.SqlTypeMismatch,
            },
            .number => switch (value) {
                .integer => |v| finite(@floatFromInt(v)),
                .float => |v| finite(v),
                .string, .number_string => |v| finite(std.fmt.parseFloat(f64, v) catch return error.SqlTypeMismatch),
                else => error.SqlTypeMismatch,
            },
            .boolean => switch (value) {
                .bool => value,
                .integer => |v| .{ .bool = v != 0 },
                .string => |v| if (std.ascii.eqlIgnoreCase(v, "true") or std.ascii.eqlIgnoreCase(v, "t") or std.ascii.eqlIgnoreCase(v, "yes") or std.mem.eql(u8, v, "1")) .{ .bool = true } else if (std.ascii.eqlIgnoreCase(v, "false") or std.ascii.eqlIgnoreCase(v, "f") or std.ascii.eqlIgnoreCase(v, "no") or std.mem.eql(u8, v, "0")) .{ .bool = false } else error.SqlTypeMismatch,
                else => error.SqlTypeMismatch,
            },
            .string => .{ .string = try self.formatText(value) },
            .datetime => blk: {
                if (value != .string) return error.SqlTypeMismatch;
                const ns = datetime.parseDateTimeToNs(value.string) orelse return error.InvalidSqlDateTime;
                try self.charge(30);
                break :blk .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, ns) };
            },
            .json => if (value == .string) blk: {
                try self.charge(value.string.len);
                break :blk try std.json.parseFromSliceLeaky(Json, self.alloc, value.string, .{ .parse_numbers = false, .allocate = .alloc_always });
            } else value,
        };
    }

    fn formatText(self: *Evaluator, value: Json) ![]const u8 {
        if (value == .string) return value.string;
        if (value == .number_string) return value.number_string;
        if (value == .bool) return if (value.bool) "true" else "false";
        if (value == .null) return "";
        try self.charge(64);
        return switch (value) {
            .integer => |v| try std.fmt.allocPrint(self.alloc, "{d}", .{v}),
            .float => |v| try std.fmt.allocPrint(self.alloc, "{d}", .{v}),
            else => error.SqlTypeMismatch,
        };
    }

    fn concat(self: *Evaluator, values: []const Json, ignore_null: bool) !Json {
        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(self.alloc);
        for (values) |value| {
            if (value == .null) {
                if (ignore_null) continue;
                return .null;
            }
            const text_value = try self.formatText(value);
            try self.charge(text_value.len);
            try result.appendSlice(self.alloc, text_value);
        }
        return .{ .string = try result.toOwnedSlice(self.alloc) };
    }

    fn invokeFunction(self: *Evaluator, function: Function, args: []const u32, depth: usize) anyerror!Json {
        if (function == .@"$pattern_quantified") {
            const operand = try self.run(args[0], depth + 1);
            const set = try self.run(args[1], depth + 1);
            const all = try self.run(args[2], depth + 1);
            const insensitive = try self.run(args[3], depth + 1);
            const negated = try self.run(args[4], depth + 1);
            if (all != .bool or insensitive != .bool or negated != .bool) return error.SqlTypeMismatch;
            if (set != .null and set != .array) return error.SqlTypeMismatch;
            const patterns: []const Json = if (set == .null) &.{} else set.array.items;
            if (patterns.len == 0) return .{ .bool = all.bool };
            if (operand == .null) return .null;
            var saw_null = false;
            for (patterns) |pattern| {
                if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                self.steps += 1;
                if (pattern == .null) {
                    saw_null = true;
                    continue;
                }
                const matches = (try self.like(operand, pattern, insensitive.bool)) != negated.bool;
                if (matches != all.bool) return .{ .bool = matches };
            }
            return if (saw_null) .null else .{ .bool = all.bool };
        }
        if (function == .@"$single") {
            const count = try self.run(args[1], depth + 1);
            if (count != .null and (count != .integer or count.integer > 1)) return error.SqlCardinalityViolation;
            return self.run(args[0], depth + 1);
        }
        if (function == .coalesce) {
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value != .null) return value;
            }
            return .null;
        }
        if (function == .greatest or function == .least) {
            var best: Json = .null;
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value == .null) continue;
                if (best == .null or (try compare(value, best)) == (if (function == .greatest) std.math.Order.gt else .lt)) best = value;
            }
            return best;
        }
        if (function == .concat) {
            var result: std.ArrayList(u8) = .empty;
            errdefer result.deinit(self.alloc);
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value == .null) continue;
                const text_value = try self.formatText(value);
                try self.charge(text_value.len);
                try result.appendSlice(self.alloc, text_value);
            }
            return .{ .string = try result.toOwnedSlice(self.alloc) };
        }
        var values: [3]Json = @splat(.null);
        for (args, 0..) |arg, i| values[i] = try self.run(arg, depth);
        if (function == .nullif) return if (values[0] == .null or (values[1] != .null and (try compare(values[0], values[1])) == .eq)) .null else values[0];
        for (values[0..args.len]) |value| if (value == .null) return .null;
        const first = values[0];
        switch (function) {
            .date_part, .date_trunc => {
                if (first != .string or values[1] != .string) return error.SqlTypeMismatch;
                const field = datetime.unit(first.string) orelse return error.InvalidSqlParameters;
                const ns = datetime.parseDateTimeToNs(values[1].string) orelse return error.InvalidSqlDateTime;
                if (function == .date_part) return .{ .float = datetime.part(ns, field) };
                const truncated = datetime.truncate(ns, field) orelse return error.InvalidSqlDateTime;
                try self.charge(30);
                return .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, truncated) };
            },
            .to_timestamp => {
                const seconds = try asFloat(first);
                const nanos = @round(seconds * std.time.ns_per_s);
                if (!std.math.isFinite(nanos) or nanos < 0 or nanos >= 18446744073709551616.0) return error.InvalidSqlDateTime;
                try self.charge(30);
                return .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, @intFromFloat(nanos)) };
            },
            .abs => return switch (first) {
                .integer => |v| .{ .integer = if (v < 0) std.math.negate(v) catch return error.SqlNumericOutOfRange else v },
                .float => |v| finite(@abs(v)),
                else => error.SqlTypeMismatch,
            },
            .ceil, .floor, .round => return if (first == .integer) first else if (first == .float) finite(switch (function) {
                .ceil => @ceil(first.float),
                .floor => @floor(first.float),
                else => @round(first.float),
            }) else error.SqlTypeMismatch,
            .sqrt => {
                const v = try asFloat(first);
                if (v < 0) return error.SqlNumericOutOfRange;
                return finite(@sqrt(v));
            },
            .power => return finite(std.math.pow(f64, try asFloat(first), try asFloat(values[1]))),
            .mod => return arithmetic(.modulo, first, values[1]),
            else => {},
        }
        if (first != .string) return error.SqlTypeMismatch;
        const text_value = first.string;
        return switch (function) {
            .length => .{ .integer = @intCast(std.unicode.utf8CountCodepoints(text_value) catch return error.SqlTypeMismatch) },
            .octet_length => .{ .integer = @intCast(text_value.len) },
            .lower, .upper => blk: {
                try self.charge(text_value.len);
                const output = try self.alloc.dupe(u8, text_value);
                if (function == .lower) _ = std.ascii.lowerString(output, output) else _ = std.ascii.upperString(output, output);
                break :blk .{ .string = output };
            },
            .starts_with => if (values[1] == .string) .{ .bool = std.mem.startsWith(u8, text_value, values[1].string) } else error.SqlTypeMismatch,
            .trim, .ltrim, .rtrim => blk: {
                const trim_chars = if (args.len == 1) " " else if (values[1] == .string) values[1].string else return error.SqlTypeMismatch;
                var characters: std.AutoHashMapUnmanaged(u21, void) = .empty;
                defer characters.deinit(self.alloc);
                var character_iterator = (std.unicode.Utf8View.init(trim_chars) catch return error.SqlTypeMismatch).iterator();
                while (character_iterator.nextCodepoint()) |codepoint| {
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    try characters.put(self.alloc, codepoint, {});
                }
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                var begin: usize = 0;
                var end: usize = 0;
                var leading = true;
                while (iterator.nextCodepointSlice()) |bytes| {
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    const trimmed = characters.contains(std.unicode.utf8Decode(bytes) catch return error.SqlTypeMismatch);
                    const after = @intFromPtr(bytes.ptr) - @intFromPtr(text_value.ptr) + bytes.len;
                    if (leading and trimmed) begin = after else leading = false;
                    if (!trimmed) end = after;
                }
                const start = if (function == .rtrim) 0 else begin;
                const stop = if (function == .ltrim) text_value.len else @max(start, end);
                break :blk .{ .string = text_value[start..stop] };
            },
            .substring => blk: {
                if (values[1] != .integer or (args.len == 3 and values[2] != .integer)) return error.SqlTypeMismatch;
                const start = values[1].integer;
                const length = if (args.len == 3) values[2].integer else std.math.maxInt(i64);
                if (length < 0) return error.InvalidSqlParameters;
                const end = if (args.len == 3) start +| length else std.math.maxInt(i64);
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                var position: i64 = 1;
                var begin_byte: usize = text_value.len;
                var end_byte: usize = text_value.len;
                while (iterator.nextCodepointSlice()) |bytes| : (position += 1) {
                    const offset = @intFromPtr(bytes.ptr) - @intFromPtr(text_value.ptr);
                    if (position >= @max(start, 1) and begin_byte == text_value.len) begin_byte = offset;
                    if (position >= end) {
                        end_byte = offset;
                        break;
                    }
                }
                break :blk .{ .string = text_value[@min(begin_byte, end_byte)..end_byte] };
            },
            .replace => blk: {
                if (values[1] != .string or values[2] != .string) return error.SqlTypeMismatch;
                if (values[1].string.len == 0) break :blk first;
                var output: std.ArrayList(u8) = .empty;
                errdefer output.deinit(self.alloc);
                var chunks = std.mem.splitSequence(u8, text_value, values[1].string);
                var initial = true;
                while (chunks.next()) |chunk| {
                    if (!initial) {
                        try self.charge(values[2].string.len);
                        try output.appendSlice(self.alloc, values[2].string);
                    }
                    initial = false;
                    try self.charge(chunk.len);
                    try output.appendSlice(self.alloc, chunk);
                }
                break :blk .{ .string = try output.toOwnedSlice(self.alloc) };
            },
            else => unreachable,
        };
    }

    fn like(self: *Evaluator, text_value: Json, pattern: Json, insensitive: bool) !bool {
        if (text_value != .string or pattern != .string) return error.SqlTypeMismatch;
        const text = text_value.string;
        const glob = pattern.string;
        var i: usize = 0;
        var j: usize = 0;
        var star: ?usize = null;
        var restart: usize = 0;
        while (i < text.len) {
            if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
            self.steps += 1;
            if (j < glob.len and glob[j] == '%') {
                star = j + 1;
                j += 1;
                restart = i;
                continue;
            }
            if (j < glob.len and glob[j] == '_') {
                i += std.unicode.utf8ByteSequenceLength(text[i]) catch return error.SqlTypeMismatch;
                j += 1;
                continue;
            }
            var escaped = false;
            if (j < glob.len and glob[j] == '\\') {
                j += 1;
                escaped = true;
                if (j == glob.len) return error.InvalidSqlParameters;
            }
            if (j < glob.len and (if (insensitive) std.ascii.toLower(text[i]) == std.ascii.toLower(glob[j]) else text[i] == glob[j])) {
                i += 1;
                j += 1;
                continue;
            }
            if (escaped) j -= 1;
            if (star) |next| {
                restart += std.unicode.utf8ByteSequenceLength(text[restart]) catch return error.SqlTypeMismatch;
                i = restart;
                j = next;
            } else return false;
        }
        while (j < glob.len and glob[j] == '%') : (j += 1) {}
        return j == glob.len;
    }
};

fn finite(value: f64) !Json {
    if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
    return .{ .float = value };
}
fn asFloat(value: Json) !f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| if (std.math.isFinite(v)) v else error.SqlNumericOutOfRange,
        else => error.SqlTypeMismatch,
    };
}
fn arithmetic(op: ast.Scalar.Binary, left: Json, right: Json) !Json {
    if (left == .integer and right == .integer) {
        const a = left.integer;
        const b = right.integer;
        return .{ .integer = switch (op) {
            .add => std.math.add(i64, a, b) catch return error.SqlNumericOutOfRange,
            .subtract => std.math.sub(i64, a, b) catch return error.SqlNumericOutOfRange,
            .multiply => std.math.mul(i64, a, b) catch return error.SqlNumericOutOfRange,
            .divide => if (b == 0) return error.SqlDivisionByZero else if (a == std.math.minInt(i64) and b == -1) return error.SqlNumericOutOfRange else @divTrunc(a, b),
            .modulo => if (b == 0) return error.SqlDivisionByZero else if (a == std.math.minInt(i64) and b == -1) 0 else @rem(a, b),
            else => unreachable,
        } };
    }
    const a = try asFloat(left);
    const b = try asFloat(right);
    return finite(switch (op) {
        .add => a + b,
        .subtract => a - b,
        .multiply => a * b,
        .divide => if (b == 0) return error.SqlDivisionByZero else a / b,
        .modulo => if (b == 0) return error.SqlDivisionByZero else @rem(a, b),
        else => unreachable,
    });
}
fn compareIntFloat(integer: i64, number: f64) !std.math.Order {
    if (!std.math.isFinite(number)) return error.SqlNumericOutOfRange;
    if (number >= 9223372036854775808.0) return .lt;
    if (number < -9223372036854775808.0) return .gt;
    const truncated: i64 = @intFromFloat(number);
    const order = std.math.order(integer, truncated);
    if (order != .eq) return order;
    return std.math.order(@as(f64, @floatFromInt(truncated)), number);
}
pub fn compare(left: Json, right: Json) !std.math.Order {
    if (left == .array or left == .object or left == .number_string or right == .array or right == .object or right == .number_string) {
        var budget: json_order.Budget = .{};
        return json_order.compare(left, right, &budget, 0);
    }
    if (left == .null or right == .null) return if (left == .null and right == .null) .eq else if (left == .null) .lt else .gt;
    if (left == .integer and right == .integer) return std.math.order(left.integer, right.integer);
    if (left == .integer and right == .float) return compareIntFloat(left.integer, right.float);
    if (left == .float and right == .integer) return (try compareIntFloat(right.integer, left.float)).invert();
    if (left == .float and right == .float) {
        _ = try asFloat(left);
        _ = try asFloat(right);
        return std.math.order(left.float, right.float);
    }
    if (left == .string and right == .string) return std.mem.order(u8, left.string, right.string);
    if (left == .bool and right == .bool) return std.math.order(@intFromBool(left.bool), @intFromBool(right.bool));
    var budget: json_order.Budget = .{};
    return json_order.compare(left, right, &budget, 0);
}

pub fn semanticHash(value: Json) !u64 {
    var budget: json_order.Budget = .{};
    return json_order.hash(value, &budget, 0);
}
fn comparison(op: ast.Scalar.Binary, order: std.math.Order) Json {
    return .{ .bool = switch (op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
        else => unreachable,
    } };
}

test "SQL scalar bound programs preserve lazy truth exact integers and function semantics" {
    const cases = [_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "1 + 2 * 3", .expected = "7" },
        .{ .sql = "-7 / 2", .expected = "-3" },
        .{ .sql = "NULL AND FALSE", .expected = "false" },
        .{ .sql = "NULL OR TRUE", .expected = "true" },
        .{ .sql = "NULL OR FALSE", .expected = "null" },
        .{ .sql = "FALSE AND 1 / 0 = 1", .expected = "false" },
        .{ .sql = "TRUE OR 1 / 0 = 1", .expected = "true" },
        .{ .sql = "coalesce(NULL, 7, 1 / 0)", .expected = "7" },
        .{ .sql = "CASE WHEN TRUE THEN 7 ELSE 1 / 0 END", .expected = "7" },
        .{ .sql = "CASE 2 WHEN 1 THEN 9 WHEN 2 THEN 7 END", .expected = "7" },
        .{ .sql = "9007199254740993 > 9007199254740992.0", .expected = "true" },
        .{ .sql = "NULL IS NOT DISTINCT FROM NULL", .expected = "true" },
        .{ .sql = "CAST('12' AS bigint) + 1", .expected = "13" },
        .{ .sql = "length('é🍎')", .expected = "2" },
        .{ .sql = "substring('aé🍎z', 2, 2)", .expected = "\"é🍎\"" },
        .{ .sql = "trim('éêé', 'é')", .expected = "\"ê\"" },
        .{ .sql = "ltrim('🍎x🍎', '🍎')", .expected = "\"x🍎\"" },
        .{ .sql = "rtrim('🍎x🍎', '🍎')", .expected = "\"🍎x\"" },
        .{ .sql = "trim('éé', 'é')", .expected = "\"\"" },
        .{ .sql = "replace('abcabc','b','XY')", .expected = "\"aXYcaXYc\"" },
        .{ .sql = "'héllo' LIKE 'h_llo'", .expected = "true" },
        .{ .sql = "7 BETWEEN 2 AND 9", .expected = "true" },
        .{ .sql = "7 NOT BETWEEN 2 AND 9", .expected = "false" },
        .{ .sql = "7 IN (1, 7, NULL)", .expected = "true" },
        .{ .sql = "7 NOT IN (1, NULL)", .expected = "null" },
        .{ .sql = "'hello' NOT LIKE 'z%'", .expected = "true" },
        .{ .sql = "('{\"a\":12}'::json ->> 'a')::bigint", .expected = "12" },
        .{ .sql = "'[1,2,3]'::json -> -1", .expected = "3" },
        .{ .sql = "'2024-02-29'::datetime", .expected = "\"2024-02-29T00:00:00.000000000Z\"" },
        .{ .sql = "date_part('year', '2024-02-29'::datetime)", .expected = "2024" },
        .{ .sql = "date_trunc('month', '2024-02-29T13:14:15+01:00'::datetime)", .expected = "\"2024-02-01T00:00:00.000000000Z\"" },
        .{ .sql = "to_timestamp(0)", .expected = "\"1970-01-01T00:00:00.000000000Z\"" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), value.value, .{});
        try std.testing.expectEqualStrings(case.expected, encoded);
    }
}

test "SQL scalar binding resolves ordinals and parameter types once" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "price * $1 + 2", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{.{ .name = "price", .type = .integer, .nullable = false }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.parameter_types[0].?);
    try std.testing.expectEqualSlices(u32, &.{0}, program.required_columns);
    try std.testing.expectEqual(@as(i64, 23), (try program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = 7 })}, &.{.{ .integer = 3 }}, .{})).value.integer);
    try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = std.math.maxInt(i64) })}, &.{.{ .integer = 3 }}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = 7 })}, &.{.{ .integer = 3 }}, .{ .steps = 1 }));
}

test "SQL discarded EXISTS projection binds parameters without retaining column dependencies" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "\"$validate\"(price / $1, lower(payload), CAST('bad' AS BIGINT))", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{ .{ .name = "price", .type = .integer, .nullable = false }, .{ .name = "payload", .type = .string } }, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.parameter_types[0].?);
    try std.testing.expectEqual(@as(usize, 0), program.required_columns.len);
    // No row payload is needed, and a zero divisor must never be evaluated.
    try std.testing.expectEqual(@as(i64, 1), (try program.evaluate(std.testing.allocator, &.{}, &.{.{ .integer = 0 }}, .{})).value.integer);
}

test "SQL scalar JSON null remains distinct from SQL NULL through casts and lazy branches" {
    const cases = [_]struct { sql: []const u8, sql_null: bool, expected: []const u8 }{
        .{ .sql = "CAST('null' AS json)", .sql_null = false, .expected = "null" },
        .{ .sql = "CAST(NULL AS json)", .sql_null = true, .expected = "null" },
        .{ .sql = "CAST('null' AS json) IS NULL", .sql_null = false, .expected = "false" },
        .{ .sql = "CAST('null' AS json) IS DISTINCT FROM NULL", .sql_null = false, .expected = "true" },
        .{ .sql = "coalesce(CAST('null' AS json), CAST('{}' AS json))", .sql_null = false, .expected = "null" },
        .{ .sql = "CAST(CAST('null' AS json) AS text)", .sql_null = false, .expected = "\"null\"" },
        .{ .sql = "('{\"a\":null}'::json -> 'a') IS NULL", .sql_null = false, .expected = "false" },
        .{ .sql = "('{\"a\":null}'::json ->> 'a') IS NULL", .sql_null = false, .expected = "true" },
        .{ .sql = "('{\"a\":null}'::json -> 'missing') IS NULL", .sql_null = false, .expected = "true" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(case.sql_null, value.sql_null);
        try std.testing.expectEqualStrings(case.expected, try std.json.Stringify.valueAlloc(arena.allocator(), value.value, .{}));
    }
}

test "SQL scalar parameter constraints remain unknown until shared statement context resolves them" {
    var projected = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1", .{});
    defer projected.deinit();
    var arithmetic_expression = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1 + 2", .{});
    defer arithmetic_expression.deinit();
    var parameters: [1]?ast.ColumnType = .{null};
    try std.testing.expect(!try inferParameters(std.testing.allocator, projected.expression, &.{}, &parameters, null, .{}));
    try std.testing.expectEqual(@as(?ast.ColumnType, null), parameters[0]);
    try std.testing.expect(try inferParameters(std.testing.allocator, arithmetic_expression.expression, &.{}, &parameters, null, .{}));
    var program = try bind(std.testing.allocator, projected.expression, &.{}, &parameters, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.output_type.kind.?);
    try std.testing.expectEqual(@as(i64, 3), (try program.evaluate(std.testing.allocator, &.{}, &.{.{ .integer = 3 }}, .{})).value.integer);
    var chain = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1 = $2 AND $2 > 3", .{});
    defer chain.deinit();
    var chain_parameters: [2]?ast.ColumnType = .{ null, null };
    _ = try inferParameters(std.testing.allocator, chain.expression, &.{}, &chain_parameters, .boolean, .{});
    _ = try inferParameters(std.testing.allocator, chain.expression, &.{}, &chain_parameters, .boolean, .{});
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .integer, .integer }, &chain_parameters);
}

test "SQL scalar binding and computed output clean up on allocation failures" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(alloc, "CASE WHEN price > $1 THEN concat(trim('éêé', 'é'), 'null'::json) ELSE 'none' END", .{});
            defer compiled.deinit();
            var program = try bind(alloc, compiled.expression, &.{.{ .name = "price", .type = .integer }}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{Datum.json(.{ .integer = 9 })}, &.{.{ .integer = 2 }}, .{});
            try std.testing.expectEqualStrings("ênull", result.value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL scalar bound arithmetic hot loop does not allocate per row" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "CASE WHEN price > $1 THEN price * 3 + 7 ELSE 0 END", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{.{ .name = "price", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    var no_memory = std.heap.FixedBufferAllocator.init(&.{});
    var checksum: i64 = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100000) |i| {
        const result = try program.evaluate(no_memory.allocator(), &.{Datum.json(.{ .integer = @intCast(i) })}, &.{.{ .integer = 100 }}, .{});
        checksum += result.value.integer;
    }
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(@as(i64, 15000534143), checksum);
    std.debug.print("SQL scalar hot loop: rows=100000 instructions={} allocated_bytes=0 checksum={} elapsed_ns={}\n", .{ program.instructions.len, checksum, elapsed });
}
