// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! The MERGE candidate read is bound once against a pinned target definition.
//! This is deliberately separate from publication: a candidate stream is not
//! a write plan until ordered arms and absence proofs share one native commit.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const relation_binding = @import("relation_binding.zig");
const joined_mutation = @import("joined_mutation.zig");
const scalar = @import("scalar.zig");
const decision_eval = @import("decision_eval.zig");
const DecisionProvider = @import("../functions/decisions.zig").DecisionProvider;
const Allocator = std.mem.Allocator;

pub const Candidates = struct {
    input: *const describe.BoundStatement,
    query: ast.Select,
    target: catalog.Table,
    field_ordinals: []const ?usize,
    returning: bool,
    returning_plan: ?Returning = null,
    point_plan: ?PointPlan = null,
    arms: []const BoundArm,
    parameter_types: []const ?ast.ColumnType,

    /// The source-preserving join guarantees that a NULL target identity is a
    /// source-only row. SQL UNKNOWN does not satisfy a WHEN predicate.
    pub fn selectArm(self: Candidates, alloc: Allocator, cells: []const scalar.Datum, parameters: []const std.json.Value) !?usize {
        return self.selectArmWithProvider(alloc, cells, parameters, null);
    }

    pub fn selectArmWithProvider(self: Candidates, alloc: Allocator, cells: []const scalar.Datum, parameters: []const std.json.Value, provider: ?DecisionProvider) !?usize {
        if (cells.len != self.query.columns.len or cells.len == 0) return error.InvalidSqlBackendResponse;
        const matched = !cells[0].sql_null;
        for (self.arms, 0..) |arm, index| {
            if (arm.matched != matched) continue;
            if (arm.predicate) |predicate| {
                const result = try decision_eval.evaluate(alloc, provider, &predicate, cells, parameters);
                if (result.sql_null) continue;
                if (result.value != .bool) return error.InvalidSqlBackendResponse;
                if (!result.value.bool) continue;
            }
            return index;
        }
        return null;
    }

    /// Only the selected arm's programs run. Inactive expressions may contain
    /// errors and must never be evaluated during classification.
    pub fn evaluateValues(self: Candidates, alloc: Allocator, index: usize, cells: []const scalar.Datum, parameters: []const std.json.Value) ![]const scalar.Datum {
        return self.evaluateValuesWithProvider(alloc, index, cells, parameters, null);
    }

    pub fn evaluateValuesWithProvider(self: Candidates, alloc: Allocator, index: usize, cells: []const scalar.Datum, parameters: []const std.json.Value, provider: ?DecisionProvider) ![]const scalar.Datum {
        if (index >= self.arms.len or cells.len != self.query.columns.len) return error.InvalidSqlBackendResponse;
        const values = switch (self.arms[index].action) {
            .update => |assignments| assignments,
            .insert => |assignments| assignments,
            .delete, .nothing => return &.{},
        };
        const result = try alloc.alloc(scalar.Datum, values.len);
        for (values, result) |assignment, *output| output.* = if (assignment.program) |program| try decision_eval.evaluate(alloc, provider, &program, cells, parameters) else .{};
        return result;
    }

    /// Classify the complete bounded capture before preparing any image.
    /// MERGE, unlike DELETE USING, must reject a target selected for more than
    /// one UPDATE/DELETE action rather than deduplicating or choosing a winner.
    pub fn classifyRows(self: Candidates, alloc: Allocator, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value) ![]const ?usize {
        return self.classifyRowsChecked(alloc, alloc, rows, nulls, parameters, null, .{ .row_limit = (@import("runtime.zig").Limits{}).page_rows, .byte_limit = (@import("runtime.zig").Limits{}).page_bytes });
    }

    fn classifyRowsChecked(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, backend: ?catalog.Backend, page_limits: decision_eval.PageBudget) ![]const ?usize {
        if (rows.len != nulls.len) return error.InvalidSqlBackendResponse;
        for (self.arms) |arm| if (arm.predicate) |*program| {
            if (decision_eval.hasExternal(program)) return self.classifyDecisionRows(alloc, scratch_allocator, rows, nulls, parameters, backend, page_limits);
        };
        const selected = try alloc.alloc(?usize, rows.len);
        const cells = try alloc.alloc(scalar.Datum, self.query.columns.len);
        var affected: std.StringHashMapUnmanaged(void) = .empty;
        // Arm predicates return an ordinal, not a value borrowed from their
        // evaluator. Reuse one bounded scratch arena across candidate rows
        // instead of allocating and destroying an arena for every row.
        var scratch = std.heap.ArenaAllocator.init(scratch_allocator);
        defer scratch.deinit();
        for (rows, nulls, selected) |row, flags, *slot| {
            if (backend) |active| try active.vtable.checkpoint(active.ptr);
            if (row.len != cells.len or flags.len != cells.len) return error.InvalidSqlBackendResponse;
            for (row, flags, cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
            _ = scratch.reset(.retain_capacity);
            slot.* = try self.selectArmWithProvider(scratch.allocator(), cells, parameters, if (backend) |active| active.decision_provider else null);
            if (slot.*) |index| switch (self.arms[index].action) {
                .update, .delete => {
                    if (cells[0].sql_null or cells[0].value != .string) return error.InvalidSqlBackendResponse;
                    if ((try affected.getOrPut(alloc, cells[0].value.string)).found_existing) return error.SqlMutationCardinalityViolation;
                },
                .insert, .nothing => {},
            };
        }
        return selected;
    }

    /// Arm order is SQL control flow: only unmatched rows of the appropriate
    /// matched/source-only kind are eligible for the next predicate wave.
    fn classifyDecisionRows(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, backend: ?catalog.Backend, page_limits: decision_eval.PageBudget) ![]const ?usize {
        const selected = try alloc.alloc(?usize, rows.len);
        @memset(selected, null);
        var affected: std.StringHashMapUnmanaged(void) = .empty;
        var arena = std.heap.ArenaAllocator.init(scratch_allocator);
        defer arena.deinit();
        var first: usize = 0;
        while (first < rows.len) {
            if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
            const scratch = arena.allocator();
            var budget = page_limits;
            var page_cells: std.ArrayList([]const scalar.Datum) = .empty;
            for (rows[first..], nulls[first..]) |row, flags| {
                if (backend) |active| try active.vtable.checkpoint(active.ptr);
                const input = try self.rowCells(scratch, row, flags);
                try page_cells.append(scratch, input);
                if (try budget.add(input)) break;
            }
            const cells = page_cells.items;
            const end = first + cells.len;
            for (self.arms, 0..) |arm, arm_index| {
                var eligible: std.ArrayList([]const scalar.Datum) = .empty;
                var positions: std.ArrayList(usize) = .empty;
                for (cells, first..) |row, index| {
                    if (selected[index] != null or arm.matched != !row[0].sql_null) continue;
                    try eligible.append(scratch, row);
                    try positions.append(scratch, index);
                }
                const values = if (arm.predicate) |*program|
                    try decision_eval.evaluateBatch(scratch, if (backend) |active| active.decision_provider else null, program, eligible.items, parameters)
                else
                    null;
                for (positions.items, 0..) |position, index| {
                    if (values) |predicates| {
                        if (predicates[index].sql_null) continue;
                        if (predicates[index].value != .bool) return error.InvalidSqlBackendResponse;
                        if (!predicates[index].value.bool) continue;
                    }
                    selected[position] = arm_index;
                }
            }
            for (cells, first..) |row, index| if (selected[index]) |arm_index| switch (self.arms[arm_index].action) {
                .update, .delete => {
                    if (row[0].sql_null or row[0].value != .string) return error.InvalidSqlBackendResponse;
                    if ((try affected.getOrPut(alloc, row[0].value.string)).found_existing) return error.SqlMutationCardinalityViolation;
                },
                .insert, .nothing => {},
            };
            first = end;
        }
        return selected;
    }

    fn rowCells(self: Candidates, alloc: Allocator, row: []const std.json.Value, flags: []const bool) ![]const scalar.Datum {
        if (row.len == 0 or row.len != self.query.columns.len or flags.len != row.len) return error.InvalidSqlBackendResponse;
        const cells = try alloc.alloc(scalar.Datum, row.len);
        for (row, flags, cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
        return cells;
    }

    /// Resolve assignments only for each selected arm and retain their values
    /// in the mutation arena. Pure mutation plans keep their existing hot path.
    fn decisionAssignmentValues(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, rows: []const []const std.json.Value, nulls: []const []const bool, selected: []const ?usize, parameters: []const std.json.Value, page_limits: decision_eval.PageBudget) !?[]const ?[]const scalar.Datum {
        var needed = false;
        for (self.arms) |arm| switch (arm.action) {
            .insert, .update => |assignments| for (assignments) |assignment| {
                if (assignment.program) |*program| needed = needed or decision_eval.hasExternal(program);
            },
            .delete, .nothing => {},
        };
        if (!needed) return null;
        const values = try alloc.alloc(?[]const scalar.Datum, rows.len);
        @memset(values, null);
        var arena = std.heap.ArenaAllocator.init(scratch_allocator);
        defer arena.deinit();
        for (self.arms, 0..) |arm, arm_index| {
            const assignments = switch (arm.action) {
                .insert, .update => |items| items,
                .delete, .nothing => continue,
            };
            var first: usize = 0;
            while (first < rows.len) {
                if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
                const scratch = arena.allocator();
                var cells: std.ArrayList([]const scalar.Datum) = .empty;
                var positions: std.ArrayList(usize) = .empty;
                var budget = page_limits;
                while (first < rows.len) : (first += 1) {
                    if (selected[first] == null or selected[first].? != arm_index) continue;
                    try backend.vtable.checkpoint(backend.ptr);
                    try cells.append(scratch, try self.rowCells(scratch, rows[first], nulls[first]));
                    try positions.append(scratch, first);
                    const output = try alloc.alloc(scalar.Datum, assignments.len);
                    @memset(output, .{});
                    values[first] = output;
                    if (try budget.add(cells.items[cells.items.len - 1])) {
                        first += 1;
                        break;
                    }
                }
                for (assignments, 0..) |assignment, column| {
                    const program = assignment.program orelse continue;
                    const output = try decision_eval.evaluateBatch(scratch, backend.decision_provider, &program, cells.items, parameters);
                    for (positions.items, output) |position, value| @constCast(values[position].?)[column] = try @import("operators.zig").cloneDatum(alloc, value);
                }
            }
        }
        return values;
    }

    /// Build one owned batch from the captured rows. This performs no write;
    /// the caller must retain the coordinated read-set through native commit.
    pub fn prepareMutations(self: Candidates, alloc: Allocator, backend: catalog.Backend, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize) ![]const catalog.Mutation {
        return (try self.prepareWithSourceRows(alloc, backend, rows, nulls, parameters, max_rows, max_bytes)).mutations;
    }

    pub const Prepared = struct {
        mutations: []const catalog.Mutation,
        /// Ordinal of the candidate row that selected each mutation arm.
        source_rows: []const usize,
    };

    pub fn prepareWithSourceRows(self: Candidates, alloc: Allocator, backend: catalog.Backend, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize) !Prepared {
        return self.prepareWithSourceRowsUsingScratch(alloc, alloc, backend, rows, nulls, parameters, max_rows, max_bytes);
    }

    /// Keep transient provider pages outside the owned mutation arena, so
    /// releasing a page actually returns its memory to the request budget.
    pub fn prepareWithSourceRowsUsingScratch(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize) !Prepared {
        return self.prepareWithPageLimits(alloc, scratch_allocator, backend, rows, nulls, parameters, max_rows, max_bytes, .{ .row_limit = (@import("runtime.zig").Limits{}).page_rows, .byte_limit = (@import("runtime.zig").Limits{}).page_bytes });
    }

    fn prepareWithPageLimits(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, rows: []const []const std.json.Value, nulls: []const []const bool, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize, page_limits: decision_eval.PageBudget) !Prepared {
        if (rows.len > max_rows or rows.len != nulls.len) return error.SqlResultTooLarge;
        const selections = try self.classifyRowsChecked(alloc, scratch_allocator, rows, nulls, parameters, backend, page_limits);
        const assignment_values = try self.decisionAssignmentValues(alloc, scratch_allocator, backend, rows, nulls, selections, parameters, page_limits);
        const cells = try alloc.alloc(scalar.Datum, self.query.columns.len);
        var mutations: std.ArrayList(catalog.Mutation) = .empty;
        var source_rows: std.ArrayList(usize) = .empty;
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        var retained: usize = 0;
        for (rows, nulls, selections, 0..) |row, flags, selected, source_index| {
            try backend.vtable.checkpoint(backend.ptr);
            const arm_index = selected orelse continue;
            if (self.arms[arm_index].action == .nothing) continue;
            if (row.len != cells.len or flags.len != cells.len) return error.InvalidSqlBackendResponse;
            for (row, flags, cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
            const arm = self.arms[arm_index];
            const assignments: []const BoundAssignment = switch (arm.action) {
                .update => |items| items,
                .insert => |items| items,
                .delete, .nothing => &.{},
            };
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            var old_nulls: std.ArrayList(bool) = .empty;
            const inserting = arm.action == .insert;
            const deleting = arm.action == .delete;
            if (!inserting) {
                if (cells[0].sql_null or cells[0].value != .string) return error.InvalidSqlBackendResponse;
                if (self.target.storage_mode == .document and !deleting) {
                    if (cells[3].sql_null or cells[3].value != .object) return error.InvalidSqlBackendResponse;
                    var old = cells[3].value.object.iterator();
                    while (old.next()) |member| {
                        const declared = self.target.column(member.key_ptr.*) catch null;
                        if (declared) |field| if (field.generated) continue;
                        const overwritten = for (assignments) |assignment| {
                            if (std.mem.eql(u8, assignment.column.path, member.key_ptr.*)) break true;
                        } else false;
                        if (overwritten) continue;
                        try object.put(alloc, member.key_ptr.*, member.value_ptr.*);
                        if (declared) |field| if (field.type == .json and member.value_ptr.* == .null) try json_null_fields.append(alloc, field.path);
                    }
                } else if (!deleting or self.returning) {
                    for (self.target.columns, self.field_ordinals) |field, ordinal| {
                        if (field.generated and !deleting) continue;
                        const index = ordinal orelse if (deleting) continue else return error.InvalidSqlBackendResponse;
                        if (index >= cells.len) return error.InvalidSqlBackendResponse;
                        const overwritten = for (assignments) |assignment| {
                            if (std.mem.eql(u8, assignment.column.path, field.path)) break true;
                        } else false;
                        if (overwritten) continue;
                        try object.put(alloc, field.path, cells[index].value);
                        try old_nulls.append(alloc, cells[index].sql_null);
                        if (!cells[index].sql_null and cells[index].value == .null and field.type == .json) try json_null_fields.append(alloc, field.path);
                    }
                }
            }
            var key: ?[]const u8 = if (inserting) null else cells[0].value.string;
            const values = if (assignment_values) |computed| computed[source_index] orelse &.{} else try self.evaluateValuesWithProvider(alloc, arm_index, cells, parameters, backend.decision_provider);
            for (assignments, values) |assignment, datum| {
                if (assignment.program == null) continue; // DEFAULT: native preparation fills the absent cell.
                const field = assignment.column;
                if (datum.sql_null and !field.nullable) return error.SqlNotNullViolation;
                const typed = try describe.coerce(datum.value, field.type);
                if (std.mem.eql(u8, field.name, "_id")) {
                    if (!inserting or datum.sql_null or typed != .string or typed.string.len == 0) return error.SqlRowIdentityRequired;
                    key = typed.string;
                } else {
                    try object.put(alloc, field.path, typed);
                    try old_nulls.append(alloc, datum.sql_null);
                    if (!datum.sql_null and typed == .null and field.type == .json) try json_null_fields.append(alloc, field.path);
                }
            }
            if (inserting and key == null) key = try (backend.vtable.generate_row_id orelse return error.SqlRowIdentityRequired)(backend.ptr, alloc);
            const identity = key orelse return error.InvalidSqlBackendResponse;
            if (identity.len == 0 or !std.unicode.utf8ValidateSlice(identity)) return error.SqlRowIdentityRequired;
            if ((try keys.getOrPut(alloc, identity)).found_existing) return error.DuplicateSqlRow;
            const version: u64 = if (inserting) 0 else blk: {
                if (cells[1].sql_null or cells[1].value != .string) return error.InvalidSqlBackendResponse;
                break :blk std.fmt.parseInt(u64, cells[1].value.string, 10) catch return error.InvalidSqlBackendResponse;
            };
            const digest: ?[32]u8 = if (inserting) null else blk: {
                if (cells[2].sql_null or cells[2].value != .string) return error.InvalidSqlBackendResponse;
                const hex = cells[2].value.string;
                if (hex.len == 0) break :blk null;
                if (hex.len != 64) return error.InvalidSqlBackendResponse;
                var bytes: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&bytes, hex) catch return error.InvalidSqlBackendResponse;
                break :blk bytes;
            };
            if (!inserting and self.target.storage_mode == .document and version != 0 and digest == null) return error.InvalidSqlBackendResponse;
            const previous = if (deleting and self.returning) previous: {
                const old = try alloc.create(catalog.Row);
                old.* = .{ .id = identity, .version = version, .value = .{ .object = object }, .sql_nulls = old_nulls.items };
                break :previous old;
            } else null;
            const mutation: catalog.Mutation = .{ .key = identity, .expected_version = version, .expected_content_digest = digest, .row = if (deleting) null else .{ .object = object }, .json_null_fields = json_null_fields.items, .previous = previous };
            retained = std.math.add(usize, retained, identity.len +| jsonSize(.{ .object = object })) catch return error.SqlProgramLimitExceeded;
            if (retained > max_bytes) return error.SqlProgramLimitExceeded;
            try mutations.append(alloc, mutation);
            if (self.returning) try source_rows.append(alloc, source_index);
        }
        return .{ .mutations = try mutations.toOwnedSlice(alloc), .source_rows = try source_rows.toOwnedSlice(alloc) };
    }
};

pub const PointPlan = struct {
    source_input: *const describe.BoundStatement,
    source_query: ast.Select,
    source_ordinals: []const ?usize,
    target_fields: []const ?[]const u8,
    lookup_ordinal: usize,
    target_scan: catalog.StatementScan,
    index_name: ?[]const u8 = null,
    index_columns: []const []const u8 = &.{},
    lookup_ordinals: []const usize = &.{},
    source_limit: usize = point_source_max_rows,
};

const point_source_max_rows: usize = 128;
const point_probe_batch_size: usize = 64;
const index_source_max_rows: usize = 32;
const index_fanout_max_rows: usize = 16;

fn jsonSize(value: std.json.Value) usize {
    return switch (value) {
        .string, .number_string => |text| @sizeOf(std.json.Value) +| text.len,
        .object => |object| blk: {
            var bytes: usize = @sizeOf(std.json.Value);
            for (object.keys(), object.values()) |name, item| bytes +|= name.len +| jsonSize(item);
            break :blk bytes;
        },
        .array => |array| blk: {
            var bytes: usize = @sizeOf(std.json.Value);
            for (array.items) |item| bytes +|= jsonSize(item);
            break :blk bytes;
        },
        else => @sizeOf(std.json.Value),
    };
}

pub const BoundAssignment = struct { column: catalog.Column, program: ?scalar.Program };
pub const BoundArm = struct {
    matched: bool,
    predicate: ?scalar.Program,
    action: union(enum) { update: []const BoundAssignment, delete, insert: []const BoundAssignment, nothing },
};
pub const Returning = struct {
    columns: []const describe.Column,
    programs: []const scalar.Program,
};
const Expression = struct { column: catalog.Column, value: ?*const ast.Scalar };
const UnboundArm = struct {
    matched: bool,
    predicate: ?*const ast.Scalar,
    action: union(enum) { update: []const Expression, delete, insert: []const Expression, nothing },
};

const ProjectionBuilder = struct {
    alloc: Allocator,
    projections: std.ArrayList(ast.Projection) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .empty,

    fn append(self: *@This(), name: []const u8) !void {
        if (name.len == 0) return;
        if ((try self.seen.getOrPut(self.alloc, name)).found_existing) return;
        try self.projections.append(self.alloc, .{ .field = name });
    }

    fn qualified(self: *@This(), qualifier: []const u8, name: []const u8) !void {
        try self.append(try std.fmt.allocPrint(self.alloc, "{s}\x00{s}", .{ qualifier, name }));
    }

    fn expression(self: *@This(), node: *const ast.Scalar) anyerror!void {
        switch (node.*) {
            .literal => {},
            .column => |name| try self.append(name),
            .unary => |part| try self.expression(part.operand),
            .cast => |part| try self.expression(part.operand),
            .binary => |part| {
                try self.expression(part.left);
                try self.expression(part.right);
            },
            .call => |part| {
                // Subqueries require a separate correlated mutation lowering;
                // never drop their read dependencies from the candidate plan.
                if (part.subquery != null or part.window != null) return error.UnsupportedSqlShape;
                for (part.args) |arg| try self.expression(arg);
                if (part.filter) |filter| try self.expression(filter);
            },
            .case_when => |part| {
                for (part.branches) |branch| {
                    try self.expression(branch.condition);
                    try self.expression(branch.value);
                }
                if (part.otherwise) |other| try self.expression(other);
            },
            .in_list => |part| {
                try self.expression(part.operand);
                for (part.values) |value| try self.expression(value);
            },
        }
    }
};

fn bindArms(alloc: Allocator, backend: catalog.Backend, target: catalog.Table, statement: ast.Merge, input: *const describe.BoundStatement) !struct { arms: []const BoundArm, parameters: []const ?ast.ColumnType } {
    const relation = input.relation orelse return error.InvalidSqlBackendResponse;
    if (relation.statement.columns.len != input.columns.len) return error.InvalidSqlBackendResponse;
    const columns = try alloc.alloc(scalar.Column, input.columns.len);
    for (relation.statement.columns, input.columns, columns, 0..) |projection, output, *column, index| {
        column.* = .{ .name = if (projection.field.len != 0) projection.field else try std.fmt.allocPrint(alloc, "\x00merge_null_{d}", .{index}), .type = output.type };
    }
    const unbound = try alloc.alloc(UnboundArm, statement.arms.len);
    for (statement.arms, unbound) |arm, *out| {
        out.matched = arm.matched;
        out.predicate = if (arm.predicate) |predicate| try relation_binding.lowerBoundExpression(alloc, relation.root.columns, predicate) else null;
        out.action = switch (arm.action) {
            .update => |assignments| blk: {
                const values = try alloc.alloc(Expression, assignments.len);
                for (assignments, values) |assignment, *value| {
                    const expression = if (assignment.use_default) null else assignment.expression orelse literal: {
                        const node = try alloc.create(ast.Scalar);
                        node.* = .{ .literal = assignment.value };
                        break :literal node;
                    };
                    value.* = .{ .column = try target.column(assignment.field), .value = if (expression) |node| try relation_binding.lowerBoundExpression(alloc, relation.root.columns, node) else null };
                }
                break :blk .{ .update = values };
            },
            .insert => |insert| blk: {
                const values = try alloc.alloc(Expression, insert.values.len);
                for (insert.columns, insert.values, values) |name, expression, *value| {
                    value.* = .{ .column = try target.column(name), .value = if (expression) |node| try relation_binding.lowerBoundExpression(alloc, relation.root.columns, node) else null };
                }
                break :blk .{ .insert = values };
            },
            .delete => .delete,
            .nothing => .nothing,
        };
    }
    const parameters = try alloc.dupe(?ast.ColumnType, input.parameter_types);
    for (unbound) |arm| {
        if (arm.predicate) |predicate| _ = try scalar.inferParameters(alloc, predicate, columns, parameters, .boolean, .{});
        const values: []const Expression = switch (arm.action) {
            .update => |items| items,
            .insert => |items| items,
            .delete, .nothing => &.{},
        };
        for (values) |value| if (value.value) |expression| {
            _ = try scalar.inferParameters(alloc, expression, columns, parameters, value.column.type, .{});
        };
    }
    const bound = try alloc.alloc(BoundArm, unbound.len);
    for (unbound, bound) |arm, *out| {
        out.matched = arm.matched;
        out.predicate = if (arm.predicate) |predicate| try scalar.bindExpectedWithSettings(alloc, predicate, columns, parameters, .boolean, .{}, backend.settings_view) else null;
        out.action = switch (arm.action) {
            .update, .insert => |values| blk: {
                const assignments = try alloc.alloc(BoundAssignment, values.len);
                for (values, assignments) |value, *assignment| assignment.* = .{
                    .column = value.column,
                    .program = if (value.value) |expression| try scalar.bindExpectedWithSettings(alloc, expression, columns, parameters, value.column.type, .{}, backend.settings_view) else null,
                };
                break :blk if (arm.action == .update) .{ .update = assignments } else .{ .insert = assignments };
            },
            .delete => .delete,
            .nothing => .nothing,
        };
    }
    return .{ .arms = bound, .parameters = parameters };
}

fn bindReturning(alloc: Allocator, backend: catalog.Backend, statement: ast.Merge, input: *const describe.BoundStatement, parameters: []?ast.ColumnType) !Returning {
    const relation = input.relation orelse return error.InvalidSqlBackendResponse;
    const requested = statement.returning orelse return error.InvalidSqlBackendResponse;
    const projections = try relation_binding.expandWildcards(alloc, relation.root.columns, if (requested.len == 0) &[_]ast.Projection{.{ .wildcard = true }} else requested, statement.alias orelse statement.table.table);
    const count = projections.len;
    const expressions = try alloc.alloc(*const ast.Scalar, count);
    const names = try alloc.alloc([]const u8, count);
    for (projections, expressions, names) |projection, *expression, *name| {
        const node = if (projection.expression) |value| value else blk: {
            const column = try alloc.create(ast.Scalar);
            column.* = .{ .column = if (projection.bound_column) |ordinal| relation.root.columns[ordinal].internal else projection.field };
            break :blk column;
        };
        expression.* = node;
        const field_name = if (std.mem.lastIndexOfScalar(u8, projection.field, 0)) |index| projection.field[index + 1 ..] else projection.field;
        name.* = projection.alias orelse if (projection.expression == null) field_name else "?column?";
    }
    const scalar_columns = try alloc.alloc(scalar.Column, input.columns.len);
    for (relation.statement.columns, input.columns, scalar_columns, 0..) |projection, column, *out, index| {
        out.* = .{ .name = if (projection.field.len != 0) projection.field else try std.fmt.allocPrint(alloc, "\x00merge_null_{d}", .{index}), .type = column.type };
    }
    const lowered = try alloc.alloc(*const ast.Scalar, count);
    for (expressions, projections, lowered) |expression, projection, *out| out.* = if (projection.bound_column != null) expression else try relation_binding.lowerBoundExpression(alloc, relation.root.columns, expression);
    for (lowered) |expression| _ = try scalar.inferParameters(alloc, expression, scalar_columns, parameters, null, .{});
    const programs = try alloc.alloc(scalar.Program, count);
    const columns = try alloc.alloc(describe.Column, count);
    for (lowered, names, programs, columns) |expression, name, *program, *column| {
        program.* = try scalar.bindWithSettings(alloc, expression, scalar_columns, parameters, .{}, backend.settings_view);
        column.* = .{ .name = name, .type = program.output_type.kind orelse .string, .untyped_null = program.output_type.kind == null };
    }
    return .{ .columns = columns, .programs = programs };
}

fn qualifiedColumn(alloc: Allocator, column: relation_binding.Column) ![]const u8 {
    return if (column.qualifier.len == 0) column.name else std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ column.qualifier, column.name });
}

const Equality = struct { target: relation_binding.Column, source: relation_binding.Column };

fn equalityColumn(columns: []const relation_binding.Column, name: []const u8) ?relation_binding.Column {
    for (columns) |column| {
        if (column.qualifier.len == 0) {
            if (std.mem.eql(u8, name, column.name)) return column;
        } else if (name.len == column.qualifier.len + 1 + column.name.len and
            std.mem.eql(u8, name[0..column.qualifier.len], column.qualifier) and
            name[column.qualifier.len] == 0 and
            std.mem.eql(u8, name[column.qualifier.len + 1 ..], column.name)) return column;
    }
    return null;
}

fn collectIndexEqualities(alloc: Allocator, node: *const ast.Scalar, join: anytype, pairs: *std.ArrayList(Equality)) !bool {
    if (node.* != .binary) return false;
    const binary = node.binary;
    if (binary.op == .@"and") return try collectIndexEqualities(alloc, binary.left, join, pairs) and try collectIndexEqualities(alloc, binary.right, join, pairs);
    if (binary.op != .eq or binary.left.* != .column or binary.right.* != .column or pairs.items.len >= 32) return false;
    const left_target = equalityColumn(join.left.columns, binary.left.column);
    const right_target = equalityColumn(join.left.columns, binary.right.column);
    const left_source = equalityColumn(join.right.columns, binary.left.column);
    const right_source = equalityColumn(join.right.columns, binary.right.column);
    const pair: Equality = if (left_target != null and right_source != null and right_target == null and left_source == null)
        .{ .target = left_target.?, .source = right_source.? }
    else if (right_target != null and left_source != null and left_target == null and right_source == null)
        .{ .target = right_target.?, .source = left_source.? }
    else
        return false;
    if (pair.target.type != pair.source.type or pair.target.type == .json) return false;
    try pairs.append(alloc, pair);
    return true;
}

/// A primary identity equality or a complete conjunction matching one total
/// index can use guarded probes. Other ON shapes retain the full join.
fn bindPointPlan(alloc: Allocator, backend: catalog.Backend, target: catalog.Table, statement: ast.Merge, input: *const describe.BoundStatement, parameter_types: []const ?ast.ColumnType, allow_index: bool) !?PointPlan {
    const relation = input.relation orelse return null;
    if (relation.root.operation != .join or relation.scans.len == 0 or relation.scans[0].table.id != target.id) return null;
    const join = relation.root.operation.join;
    var equalities: std.ArrayList(Equality) = .empty;
    if (!try collectIndexEqualities(alloc, statement.condition, join, &equalities) or equalities.items.len == 0) return null;
    var index_name: ?[]const u8 = null;
    var index_columns: []const []const u8 = &.{};
    var source_columns: []const relation_binding.Column = &.{};
    if (equalities.items.len == 1 and std.mem.eql(u8, equalities.items[0].target.name, "_id") and equalities.items[0].source.type == .string) {
        source_columns = try alloc.dupe(relation_binding.Column, &.{equalities.items[0].source});
    } else {
        if (!allow_index) return null;
        for (target.indexes) |index| {
            if (index.columns.len != equalities.items.len) continue;
            const matched = try alloc.alloc(relation_binding.Column, index.columns.len);
            var valid = true;
            for (index.columns, matched) |column_name, *source| {
                const pair = for (equalities.items) |candidate| {
                    if (std.mem.eql(u8, candidate.target.name, column_name)) break candidate;
                } else {
                    valid = false;
                    break;
                };
                source.* = pair.source;
                // Duplicate predicates must not stand in for an omitted key.
                var count: usize = 0;
                for (equalities.items) |candidate| if (std.mem.eql(u8, candidate.target.name, column_name)) {
                    count += 1;
                };
                if (count != 1) {
                    valid = false;
                    break;
                }
            }
            if (!valid) continue;
            index_name = index.name;
            index_columns = index.columns;
            source_columns = matched;
            break;
        }
        if (index_name == null) return null;
    }
    const source_ordinals = try alloc.alloc(?usize, relation.statement.columns.len);
    const target_fields = try alloc.alloc(?[]const u8, relation.statement.columns.len);
    const lookup_ordinals = try alloc.alloc(usize, source_columns.len);
    @memset(lookup_ordinals, std.math.maxInt(usize));
    @memset(source_ordinals, null);
    @memset(target_fields, null);
    var projections: std.ArrayList(ast.Projection) = .empty;
    for (relation.statement.columns, source_ordinals, target_fields) |projection, *source_ordinal, *target_field| {
        if (projection.expression != null) return null;
        var found = false;
        for (join.right.columns) |column| if (std.mem.eql(u8, projection.field, column.internal)) {
            source_ordinal.* = projections.items.len;
            for (source_columns, lookup_ordinals) |lookup, *ordinal| {
                if (std.mem.eql(u8, column.internal, lookup.internal)) ordinal.* = projections.items.len;
            }
            try projections.append(alloc, .{ .field = try qualifiedColumn(alloc, column) });
            found = true;
            break;
        };
        if (found) continue;
        for (join.left.columns) |column| if (std.mem.eql(u8, projection.field, column.internal)) {
            target_field.* = column.name;
            found = true;
            break;
        };
        if (!found) return null;
    }
    for (lookup_ordinals) |ordinal| if (ordinal == std.math.maxInt(usize)) return null;
    const projection_count = projections.items.len;
    // One extra row is enough to choose the full join. A bounded top-level
    // LIMIT keeps that decision from consuming a large source twice.
    const source_limit = if (index_name != null) index_source_max_rows else point_source_max_rows;
    const source_query: ast.Select = .{ .source = statement.source, .ctes = statement.ctes, .columns = try projections.toOwnedSlice(alloc), .limit = .{ .integer = @intCast(source_limit + 1) } };
    const selected: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = source_query }, .parameter_count = @intCast(parameter_types.len) };
    const source_input = try alloc.create(describe.BoundStatement);
    source_input.* = describe.bind(alloc, backend, &selected, parameter_types) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    if (source_input.columns.len != projection_count) return null;
    return .{ .source_input = source_input, .source_query = source_query, .source_ordinals = source_ordinals, .target_fields = target_fields, .lookup_ordinal = lookup_ordinals[0], .target_scan = relation.scans[0], .index_name = index_name, .index_columns = index_columns, .lookup_ordinals = lookup_ordinals, .source_limit = source_limit };
}

/// Bind the source/target candidate set without opening a row cursor. All
/// action-dependent source fields are projected; unrelated cold fields are
/// left in storage. The source-preserving join emits matches and source-only
/// rows, but never target-only rows (there is no BY SOURCE arm).
pub fn bindCandidates(alloc: Allocator, backend: catalog.Backend, target: catalog.Table, compiled: *const compiler.Compiled, parameter_types: []const ?ast.ColumnType) !Candidates {
    if (compiled.statement != .merge) return error.UnsupportedSqlExecution;
    const statement = compiled.statement.merge;
    const alias = statement.alias orelse statement.table.table;
    var projections: ProjectionBuilder = .{ .alloc = alloc };
    try projections.qualified(alias, "_id");
    for (joined_mutation.metadata_fields[0..3]) |field| try projections.qualified(alias, field);
    var needs_complete_target = statement.returning != null and statement.returning.?.len == 0;
    var needs_document = false;
    for (statement.arms) |arm| switch (arm.action) {
        .update => {
            needs_complete_target = needs_complete_target or target.storage_mode == .relational;
            needs_document = needs_document or target.storage_mode == .document;
        },
        .delete => needs_complete_target = needs_complete_target or (statement.returning != null and statement.returning.?.len == 0),
        .insert, .nothing => {},
    };
    const target_relation = try alloc.create(ast.Relation);
    target_relation.* = .{ .table = .{ .name = statement.table, .alias = statement.alias, .mutation_target = true, .mutation_document = needs_document } };
    const source = try alloc.create(ast.Relation);
    source.* = .{ .join = .{ .kind = .right, .left = target_relation, .right = statement.source, .condition = statement.condition } };
    var adapter: relation_binding.TargetResolveAdapter = .{ .backend = backend, .table = target, .name = statement.table, .cache_sources = true };
    if (needs_complete_target) for (target.columns) |field| try projections.qualified(alias, field.name);
    try projections.expression(statement.condition);
    for (statement.arms) |arm| {
        if (arm.predicate) |predicate| try projections.expression(predicate);
        switch (arm.action) {
            .update => |assignments| for (assignments, 0..) |assignment, index| {
                const field = try target.column(assignment.field);
                if (field.generated or std.mem.eql(u8, field.name, "_id")) return error.UnsupportedSqlShape;
                for (assignments[0..index]) |prior| if (std.mem.eql(u8, prior.field, field.name)) return error.DuplicateSqlColumn;
                if (assignment.expression) |expression| try projections.expression(expression);
            },
            .insert => |insert| {
                if (insert.columns.len != insert.values.len) return error.InvalidSqlParameters;
                for (insert.columns, insert.values, 0..) |name, value, index| {
                    const field = try target.column(name);
                    if (field.generated) return error.UnsupportedSqlShape;
                    for (insert.columns[0..index]) |prior| if (std.mem.eql(u8, prior, field.name)) return error.DuplicateSqlColumn;
                    if (value) |expression| try projections.expression(expression);
                }
            },
            .delete, .nothing => {},
        }
    }
    if (statement.returning) |returning| {
        var wildcard_domain: ?[]const relation_binding.Column = null;
        for (returning) |projection| {
            if (projection.wildcard) {
                if (wildcard_domain == null) {
                    const hints = try alloc.alloc(?ast.ColumnType, compiled.parameter_count);
                    @memset(hints, null);
                    @memcpy(hints[0..parameter_types.len], parameter_types);
                    wildcard_domain = try relation_binding.projectionColumns(alloc, adapter.iface(), .{ .source = source, .ctes = statement.ctes, .columns = &.{.{ .field = try std.fmt.allocPrint(alloc, "{s}\x00_id", .{alias}) }} }, hints);
                }
                const expanded = try relation_binding.expandWildcards(alloc, wildcard_domain.?, &.{projection}, alias);
                for (expanded) |entry| try projections.append(try qualifiedColumn(alloc, wildcard_domain.?[entry.bound_column.?]));
            } else if (projection.expression) |expression| try projections.expression(expression) else try projections.append(projection.field);
        }
        // An unqualified target reference resolves to the target's internal
        // name only after relation binding. Preserve that postimage slot even
        // when the syntax also selected the unqualified candidate column.
        for (target.columns) |field| if (projections.seen.contains(field.name)) {
            try projections.qualified(alias, field.name);
        };
    }
    const query: ast.Select = .{ .source = source, .ctes = statement.ctes, .columns = try projections.projections.toOwnedSlice(alloc) };
    const selected: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = query }, .parameter_count = compiled.parameter_count };
    const input = try alloc.create(describe.BoundStatement);
    input.* = try describe.bind(alloc, adapter.iface(), &selected, parameter_types);
    if (input.relation == null or input.relation.?.root.operation != .join or input.relation.?.root.operation.join.kind != .right) return error.InvalidSqlBackendResponse;
    const arms = try bindArms(alloc, backend, target, statement, input);
    const parameters = try alloc.dupe(?ast.ColumnType, arms.parameters);
    const returning_plan = if (statement.returning != null) try bindReturning(alloc, backend, statement, input, parameters) else null;
    const field_ordinals = try alloc.alloc(?usize, target.columns.len);
    for (target.columns, field_ordinals) |field, *ordinal| {
        ordinal.* = null;
        const name = try std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ alias, field.name });
        for (query.columns, 0..) |projection, index| if (std.mem.eql(u8, projection.field, name)) {
            ordinal.* = index;
            break;
        };
    }
    // Avoid a second source bind for backends that cannot coordinate point
    // captures with the statement's durable read set.
    const point_plan = if (backend.coordinated_point_reads and backend.vtable.open_statement != null) try bindPointPlan(alloc, adapter.iface(), target, statement, input, parameters, backend.coordinated_index_reads) else null;
    return .{ .input = input, .query = query, .target = target, .field_ordinals = field_ordinals, .returning = statement.returning != null, .returning_plan = returning_plan, .point_plan = point_plan, .arms = arms.arms, .parameter_types = parameters };
}

fn retainPointRow(alloc: Allocator, row: catalog.Row) !catalog.Row {
    var retained = row;
    retained.id = try alloc.dupe(u8, row.id);
    retained.value = try @import("runtime.zig").clone(alloc, row.value);
    if (row.document) |document| retained.document = try @import("runtime.zig").clone(alloc, document);
    if (row.sql_nulls) |flags| retained.sql_nulls = try alloc.dupe(bool, flags);
    return retained;
}

fn fullCandidates(context: anytype, bound: Candidates) !@import("runtime.zig").Output {
    var fallback = context;
    fallback.binding = bound.input.*;
    fallback.typed_output = true;
    fallback.limits.result_rows = context.limits.mutation_rows;
    return fallback.select(bound.query);
}

/// Small identity-key sources avoid scanning and hashing the complete target.
/// Source and target reads may use different physical captures only because
/// the backend guarantees their range proofs join one serializable read set.
fn pointCandidates(context: anytype, bound: Candidates, plan: PointPlan) !@import("runtime.zig").Output {
    const open = context.backend.vtable.open_statement orelse return error.SqlRangeTrackingRequired;
    var source_context = context;
    source_context.binding = plan.source_input.*;
    source_context.typed_output = true;
    source_context.limits.result_rows = @intCast(plan.source_limit + 1);
    source_context.limits.page_rows = @min(context.limits.page_rows, @as(u32, @intCast(plan.source_limit + 1)));
    const source = try source_context.select(plan.source_query);
    if (source.rows.len > context.limits.mutation_rows) return error.SqlResultTooLarge;
    // Above this threshold the existing coordinated hash join normally wins
    // over repeated point-capture setup, especially for small target tables.
    if (source.rows.len > plan.source_limit) {
        return fullCandidates(context, bound);
    }
    const source_nulls = source.sql_nulls orelse if (source.rows.len == 0) &.{} else return error.InvalidSqlBackendResponse;
    if (source_nulls.len != source.rows.len) return error.InvalidSqlBackendResponse;
    if (plan.index_name != null) return indexCandidates(context, bound, plan, source, source_nulls);
    var indexes: std.StringHashMapUnmanaged(usize) = .empty;
    var keys: std.ArrayList([]const u8) = .empty;
    var points: std.ArrayList(?catalog.Row) = .empty;
    for (source.rows, source_nulls) |values, nulls| {
        try context.checkpoint();
        if (plan.lookup_ordinal >= values.len or nulls.len != values.len) return error.InvalidSqlBackendResponse;
        if (nulls[plan.lookup_ordinal]) continue;
        if (values[plan.lookup_ordinal] != .string) return error.InvalidSqlBackendResponse;
        const key = values[plan.lookup_ordinal].string;
        if (key.len == 0 or indexes.contains(key)) continue;
        try indexes.put(context.arena, key, keys.items.len);
        try keys.append(context.arena, key);
        try points.append(context.arena, null);
    }
    var first: usize = 0;
    while (first < keys.items.len) {
        try context.checkpoint();
        const last = @min(first + point_probe_batch_size, keys.items.len);
        const requests = try context.arena.alloc(catalog.StatementScan, last - first);
        for (requests, keys.items[first..last]) |*request, key| {
            request.* = plan.target_scan;
            request.request.primary_key = key;
            request.request.limit = 1;
            request.request.after = null;
            request.request.primary_order = true;
            request.request.include_primary_digest = true;
        }
        const capture = try open(context.backend.ptr, context.arena, requests);
        {
            defer capture.close(capture.ptr);
            if (capture.cursors.len != requests.len) return error.InvalidSqlBackendResponse;
            for (capture.cursors, keys.items[first..last], points.items[first..last]) |cursor, key, *point| {
                try context.checkpoint();
                var page = try cursor.next(cursor.ptr, context.arena, 1);
                defer page.deinit();
                if (page.rows.len > 1 or page.after != null) return error.InvalidSqlBackendResponse;
                if (page.rows.len == 1) {
                    if (!std.mem.eql(u8, page.rows[0].id, key) or page.rows[0].expected_content_digest == null) return error.InvalidSqlBackendResponse;
                    point.* = try retainPointRow(context.arena, page.rows[0]);
                }
            }
        }
        first = last;
    }
    const rows = try context.arena.alloc([]const std.json.Value, source.rows.len);
    const flags = try context.arena.alloc([]const bool, source.rows.len);
    for (source.rows, source_nulls, rows, flags) |source_values, source_flags, *out_values, *out_flags| {
        try context.checkpoint();
        const values = try context.arena.alloc(std.json.Value, bound.query.columns.len);
        const nulls = try context.arena.alloc(bool, values.len);
        @memset(values, .null);
        @memset(nulls, true);
        for (plan.source_ordinals, values, nulls) |source_index, *value, *is_null| if (source_index) |index| {
            if (index >= source_values.len) return error.InvalidSqlBackendResponse;
            value.* = source_values[index];
            is_null.* = source_flags[index];
        };
        if (!source_flags[plan.lookup_ordinal] and source_values[plan.lookup_ordinal] == .string) {
            if (indexes.get(source_values[plan.lookup_ordinal].string)) |index| if (points.items[index]) |point| {
                for (plan.target_fields, values, nulls, bound.input.columns) |field, *value, *is_null, column| if (field) |name| {
                    const cell = try joined_mutation.cell(context.arena, point, name);
                    value.* = try describe.coerce(cell.value, column.type);
                    is_null.* = cell.sql_null;
                };
            };
        }
        out_values.* = values;
        out_flags.* = nulls;
    }
    return .{ .columns = bound.input.columns, .rows = rows, .sql_nulls = flags, .command_tag = "SELECT" };
}

fn appendIndexCandidate(alloc: Allocator, bound: Candidates, plan: PointPlan, source_values: []const std.json.Value, source_flags: []const bool, target: ?catalog.Row, rows: *std.ArrayList([]const std.json.Value), flags: *std.ArrayList([]const bool)) !void {
    const values = try alloc.alloc(std.json.Value, bound.query.columns.len);
    const nulls = try alloc.alloc(bool, values.len);
    @memset(values, .null);
    @memset(nulls, true);
    for (plan.source_ordinals, values, nulls) |source_index, *value, *is_null| if (source_index) |index| {
        if (index >= source_values.len) return error.InvalidSqlBackendResponse;
        value.* = source_values[index];
        is_null.* = source_flags[index];
    };
    if (target) |row| for (plan.target_fields, values, nulls, bound.input.columns) |field, *value, *is_null, column| if (field) |name| {
        const cell = try joined_mutation.cell(alloc, row, name);
        value.* = try describe.coerce(cell.value, column.type);
        is_null.* = cell.sql_null;
    };
    try rows.append(alloc, values);
    try flags.append(alloc, nulls);
}

/// Small typed-key sources probe one READY total index under a coordinated
/// read set. A saturated nonunique fanout returns to the one-pass join rather
/// than silently truncating candidates or retaining unbounded row images.
fn indexCandidates(context: anytype, bound: Candidates, plan: PointPlan, source: @import("runtime.zig").Output, source_nulls: []const []const bool) !@import("runtime.zig").Output {
    const open = context.backend.vtable.open_statement orelse return error.SqlRangeTrackingRequired;
    var unique: std.StringHashMapUnmanaged(usize) = .empty;
    var tuples: std.ArrayList([]const std.json.Value) = .empty;
    var matches: std.ArrayList([]const catalog.Row) = .empty;
    const source_slots = try context.arena.alloc(?usize, source.rows.len);
    for (source.rows, source_nulls, source_slots) |values, nulls, *source_slot| {
        try context.checkpoint();
        source_slot.* = null;
        if (nulls.len != values.len) return error.InvalidSqlBackendResponse;
        const tuple = try context.arena.alloc(std.json.Value, plan.lookup_ordinals.len);
        var complete = true;
        for (plan.lookup_ordinals, tuple) |ordinal, *value| {
            if (ordinal >= values.len) return error.InvalidSqlBackendResponse;
            if (nulls[ordinal]) {
                complete = false;
                break;
            }
            value.* = values[ordinal];
        }
        if (!complete) continue;
        const key = try std.json.Stringify.valueAlloc(context.arena, tuple, .{});
        const slot = try unique.getOrPut(context.arena, key);
        if (slot.found_existing) {
            source_slot.* = slot.value_ptr.*;
            continue;
        }
        slot.value_ptr.* = tuples.items.len;
        source_slot.* = tuples.items.len;
        try tuples.append(context.arena, tuple);
        try matches.append(context.arena, &.{});
    }
    var saturated = false;
    var first: usize = 0;
    while (first < tuples.items.len and !saturated) {
        try context.checkpoint();
        const last = @min(first + point_probe_batch_size, tuples.items.len);
        const requests = try context.arena.alloc(catalog.StatementScan, last - first);
        for (requests, tuples.items[first..last]) |*request, tuple| {
            request.* = plan.target_scan;
            request.request.index_equality = .{ .name = plan.index_name.?, .values = tuple };
            request.request.primary_key = null;
            request.request.after = null;
            request.request.primary_order = false;
            request.request.include_primary_digest = true;
            request.request.limit = index_fanout_max_rows + 1;
        }
        const capture = open(context.backend.ptr, context.arena, requests) catch |err| switch (err) {
            error.RelationalIndexNotReady => return fullCandidates(context, bound),
            else => return err,
        };
        {
            defer capture.close(capture.ptr);
            if (capture.cursors.len != requests.len) return error.InvalidSqlBackendResponse;
            for (capture.cursors, matches.items[first..last]) |cursor, *group| {
                try context.checkpoint();
                var page = try cursor.next(cursor.ptr, context.arena, index_fanout_max_rows + 1);
                defer page.deinit();
                if (page.rows.len > index_fanout_max_rows + 1) return error.InvalidSqlBackendResponse;
                if (page.rows.len > index_fanout_max_rows or page.after != null) {
                    saturated = true;
                    break;
                }
                const retained = try context.arena.alloc(catalog.Row, page.rows.len);
                for (page.rows, retained) |row, *copy| {
                    if (row.expected_content_digest == null) return error.InvalidSqlBackendResponse;
                    copy.* = try retainPointRow(context.arena, row);
                }
                group.* = retained;
            }
        }
        first = last;
    }
    if (saturated) {
        return fullCandidates(context, bound);
    }
    var rows: std.ArrayList([]const std.json.Value) = .empty;
    var flags: std.ArrayList([]const bool) = .empty;
    for (source.rows, source_nulls, source_slots) |source_values, source_flags, source_slot| {
        try context.checkpoint();
        var emitted = false;
        if (source_slot) |slot| {
            const group = matches.items[slot];
            for (group) |target| {
                var equal = true;
                for (plan.index_columns, plan.lookup_ordinals) |column, ordinal| {
                    const cell = try joined_mutation.cell(context.arena, target, column);
                    if (cell.sql_null or (try scalar.compare(source_values[ordinal], cell.value)) != .eq) {
                        equal = false;
                        break;
                    }
                }
                if (!equal) continue;
                if (rows.items.len >= context.limits.mutation_rows) return error.SqlResultTooLarge;
                try appendIndexCandidate(context.arena, bound, plan, source_values, source_flags, target, &rows, &flags);
                emitted = true;
            }
        }
        if (!emitted) {
            if (rows.items.len >= context.limits.mutation_rows) return error.SqlResultTooLarge;
            try appendIndexCandidate(context.arena, bound, plan, source_values, source_flags, null, &rows, &flags);
        }
    }
    return .{ .columns = bound.input.columns, .rows = rows.items, .sql_nulls = flags.items, .command_tag = "SELECT" };
}

pub fn execute(context: anytype, bound: Candidates) !@import("runtime.zig").Output {
    // The native owner must retain every source and target range proof with
    // the staged mutation. A plain autocommit batch cannot protect negative
    // match decisions, even if its row-version predicates are correct.
    if (!context.backend.atomic_statement_read_set or context.backend.vtable.open_statement == null) return error.SqlRangeTrackingRequired;
    var read = context;
    read.binding = bound.input.*;
    read.typed_output = true;
    read.limits.result_rows = context.limits.mutation_rows;
    const selected = if (context.backend.coordinated_point_reads and bound.point_plan != null)
        try pointCandidates(context, bound, bound.point_plan.?)
    else
        try read.select(bound.query);
    const flags = selected.sql_nulls orelse if (selected.rows.len == 0) &.{} else return error.InvalidSqlBackendResponse;
    if (flags.len != selected.rows.len) return error.InvalidSqlBackendResponse;
    const prepared = try bound.prepareWithPageLimits(context.arena, context.alloc, context.backend, selected.rows, flags, context.parameters, context.limits.mutation_rows, context.limits.retained_bytes, .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes });
    if (bound.returning_plan) |plan| {
        if (prepared.mutations.len > context.limits.result_rows or prepared.source_rows.len != prepared.mutations.len) return error.SqlResultTooLarge;
        const normalized = if (prepared.mutations.len == 0) prepared.mutations else blk: {
            const prepare = context.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
            break :blk try prepare(context.backend.ptr, context.arena, bound.target, prepared.mutations);
        };
        if (normalized.len != prepared.mutations.len) return error.InvalidSqlBackendResponse;
        const output_rows = try context.arena.alloc([]const std.json.Value, normalized.len);
        const output_nulls = try context.arena.alloc([]const bool, normalized.len);
        var external = false;
        for (plan.programs) |*program| external = external or decision_eval.hasExternal(program);
        const returning_cells = if (external) try context.arena.alloc([]const scalar.Datum, normalized.len) else null;
        const cells = try context.arena.alloc(scalar.Datum, bound.query.columns.len);
        for (normalized, prepared.mutations, prepared.source_rows, output_rows, output_nulls, 0..) |mutation, original, source_index, *values, *nulls, returning_index| {
            try context.checkpoint();
            if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or
                !std.meta.eql(mutation.expected_content_digest, original.expected_content_digest) or
                mutation.predicate_only != original.predicate_only or (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
            if (source_index >= selected.rows.len or selected.rows[source_index].len != cells.len or flags[source_index].len != cells.len) return error.InvalidSqlBackendResponse;
            for (selected.rows[source_index], flags[source_index], cells) |value, is_null, *cell| cell.* = .{ .value = value, .sql_null = is_null };
            if (mutation.row) |row| {
                if (row != .object) return error.InvalidSqlBackendResponse;
                cells[0] = .{ .value = .{ .string = mutation.key }, .sql_null = false };
                for (bound.target.columns, bound.field_ordinals) |field, ordinal| {
                    const index = ordinal orelse continue;
                    if (index >= cells.len) return error.InvalidSqlBackendResponse;
                    const value = row.object.get(field.path) orelse .null;
                    const json_null = for (mutation.json_null_fields) |name| {
                        if (std.mem.eql(u8, name, field.path)) break true;
                    } else false;
                    cells[index] = .{ .value = try describe.coerce(value, field.type), .sql_null = value == .null and !json_null };
                }
            }
            if (returning_cells) |all| {
                all[returning_index] = try context.arena.dupe(scalar.Datum, cells);
                continue;
            }
            const projected = try context.arena.alloc(std.json.Value, plan.programs.len);
            const projected_nulls = try context.arena.alloc(bool, plan.programs.len);
            for (plan.programs, projected, projected_nulls, plan.columns) |program, *value, *is_null, column| {
                const datum = try decision_eval.evaluate(context.arena, context.backend.decision_provider, &program, cells, context.parameters);
                value.* = try context.outputCell(datum.value, column.type);
                is_null.* = datum.sql_null;
            }
            values.* = projected;
            nulls.* = projected_nulls;
        }
        if (returning_cells) |all| {
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            var first: usize = 0;
            while (first < all.len) {
                try context.checkpoint();
                if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
                const scratch = arena.allocator();
                var budget: decision_eval.PageBudget = .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes };
                var end = first;
                while (end < all.len) {
                    const full = try budget.add(all[end]);
                    end += 1;
                    if (full) break;
                }
                const columns = try scratch.alloc([]const scalar.Datum, plan.programs.len);
                for (plan.programs, columns) |*program, *values| values.* = try decision_eval.evaluateBatch(scratch, context.backend.decision_provider, program, all[first..end], context.parameters);
                for (first..end) |index| {
                    const projected = try context.arena.alloc(std.json.Value, plan.programs.len);
                    const projected_nulls = try context.arena.alloc(bool, plan.programs.len);
                    for (columns, projected, projected_nulls, plan.columns) |values, *value, *is_null, column| {
                        const datum = values[index - first];
                        value.* = try context.outputCell(datum.value, column.type);
                        is_null.* = datum.sql_null;
                    }
                    output_rows[index] = projected;
                    output_nulls[index] = projected_nulls;
                }
                first = end;
            }
        }
        var output = try context.commitPreparedMutations(bound.target, normalized, "MERGE");
        output.columns = plan.columns;
        output.rows = output_rows;
        output.sql_nulls = output_nulls;
        return output;
    }
    return context.commitMutations(bound.target, prepared.mutations, "MERGE", null);
}
