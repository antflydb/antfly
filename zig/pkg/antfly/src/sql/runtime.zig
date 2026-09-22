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

//! Transport-independent execution over the native catalog/read/write boundary.
//! No durable state, global apply lock, or protocol-specific types live here.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const MemoryBudget = @import("memory_budget.zig");
const Json = std.json.Value;
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;

pub const Limits = struct {
    result_rows: usize = 128,
    mutation_rows: usize = 4096,
    scan_rows: usize = 100_000,
    retained_bytes: usize = 8 * 1024 * 1024,
    page_rows: u32 = 256,
    scan_pages: usize = 1024,
};
pub const Column = describe.Column;
pub const Output = struct {
    columns: []const Column = &.{},
    rows: []const []const Json = &.{},
    /// Mirrors rows when present. JSON null is encoded as a value when false;
    /// true denotes SQL NULL independently of the column's logical type.
    sql_nulls: ?[]const []const bool = null,
    rows_affected: u64 = 0,
    command_tag: []const u8,
    mutation_outcome: ?catalog.MutationOutcome = null,
    ddl_receipt: ?catalog.DdlReceipt = null,
};
pub const Result = struct {
    state: *State,
    output: Output,

    const State = struct { budget: MemoryBudget, arena: std.heap.ArenaAllocator };

    /// Includes retained result arena capacity and transient native page data.
    pub fn peakMemoryBytes(self: Result) usize {
        return self.state.budget.peak;
    }

    /// Transport adapters may normalize cells in an exclusively owned result
    /// before publishing it. SELECT cells are mutable result-arena allocations;
    /// callers must not mutate shared/published results or column metadata.
    pub fn mutableRow(self: *Result, index: usize) []Json {
        return @constCast(self.output.rows[index]);
    }

    pub fn deinit(self: *Result) void {
        const backing = self.state.budget.backing;
        self.state.arena.deinit();
        std.debug.assert(self.state.budget.live == 0);
        backing.destroy(self.state);
        self.* = undefined;
    }

    pub fn empty(alloc: std.mem.Allocator, command_tag: []const u8) !Result {
        const state = try alloc.create(State);
        state.budget = .{ .backing = alloc, .limit = (Limits{}).retained_bytes };
        state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
        var result = Result{ .state = state, .output = .{ .command_tag = "" } };
        errdefer result.deinit();
        result.output.command_tag = try state.arena.allocator().dupe(u8, command_tag);
        return result;
    }
};

pub fn execute(alloc: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits) !Result {
    if (limits.result_rows == 0 or limits.result_rows > 4096 or limits.mutation_rows == 0 or limits.mutation_rows > 4096 or
        limits.page_rows == 0 or limits.page_rows > 4096 or limits.scan_rows == 0) return error.InvalidSqlLimit;
    if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
    try backend.vtable.checkpoint(backend.ptr);
    const state = try alloc.create(Result.State);
    state.budget = .{ .backing = alloc, .limit = limits.retained_bytes };
    state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
    var result = Result{ .state = state, .output = undefined };
    errdefer result.deinit();
    const arena = state.arena.allocator();
    result.output = runBound(state.budget.allocator(), arena, backend, compiled, parameters, limits) catch |err| {
        if (err == error.OutOfMemory and state.budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    return result;
}

fn runBound(alloc: std.mem.Allocator, arena: std.mem.Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const Json, limits: Limits) !Output {
    const ddl = @import("ddl_runtime.zig");
    if (ddl.accepts(compiled.statement)) {
        const result = try ddl.execute(arena, backend, compiled.statement);
        return .{ .command_tag = result.command_tag, .mutation_outcome = result.mutation_outcome, .ddl_receipt = result.receipt };
    }
    // Describe and Execute share binding, type inference and authorization.
    // Resolve exactly once so validation and execution cannot pin different
    // catalog identities within one statement.
    const binding = try describe.bind(arena, backend, compiled, &.{});
    const context = Context{ .alloc = alloc, .arena = arena, .backend = backend, .binding = binding, .parameters = parameters, .limits = limits };
    return context.run(compiled.statement);
}

const Context = struct {
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    backend: catalog.Backend,
    binding: describe.BoundStatement,
    parameters: []const Json,
    limits: Limits,
    /// Internal relational consumers retain typed values. Decimal strings are
    /// a transport encoding, never the representation of an INSERT source.
    typed_output: bool = false,

    pub const ScanState = struct {
        opened: bool = false,
        cursor: ?catalog.Cursor = null,

        pub fn deinit(self: *ScanState) void {
            if (self.cursor) |cursor| cursor.close(cursor.ptr);
            self.cursor = null;
        }

        pub fn page(self: *ScanState, context: Context, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            if (!self.opened) {
                self.opened = true;
                if (context.backend.vtable.open_scan) |open|
                    self.cursor = try open(context.backend.ptr, context.alloc, table, request);
            }
            if (self.cursor) |cursor| return cursor.next(cursor.ptr, alloc, request.limit);
            return context.backend.vtable.scan(context.backend.ptr, alloc, table, request);
        }

        pub fn retained(self: ScanState, context: Context) bool {
            return self.cursor != null or context.backend.pinned_statement_snapshot;
        }
    };

    fn run(context: Context, input: ast.Statement) !Output {
        return switch (input) {
            .select => |statement| try context.select(statement),
            .insert => |statement| try context.insert(statement),
            .update => |statement| try context.change(statement.table, statement.predicate, statement.assignments, statement.returning),
            .delete => |statement| try context.change(statement.table, statement.predicate, null, statement.returning),
            else => return error.UnsupportedSqlExecution,
        };
    }

    pub fn checkpoint(self: Context) !void {
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }

    pub fn outputValue(self: Context, value_: Json) !Json {
        if (!self.typed_output and value_ == .integer) return .{ .string = try std.fmt.allocPrint(self.arena, "{d}", .{value_.integer}) };
        return clone(self.arena, value_);
    }

    fn value(self: Context, input: ast.Value, column: catalog.Column) !Json {
        if (input == .string and column.type == .json)
            return self.binding.json_literals.get(input.string) orelse error.InvalidSqlBackendResponse;
        if (input != .parameter) return describe.bindLiteral(self.arena, input, column.type);
        const index = input.parameter;
        if (index == 0 or index > self.parameters.len) return error.InvalidSqlParameters;
        return coerce(self.parameters[index - 1], column.type);
    }

    pub fn count(self: Context, input: ?ast.Value, default: usize) !usize {
        const node = input orelse return default;
        const parsed = try self.value(node, .{ .name = "limit", .path = "limit", .type = .integer });
        if (parsed != .integer or parsed.integer < 0) return error.InvalidSqlLimit;
        return std.math.cast(usize, parsed.integer) orelse error.InvalidSqlLimit;
    }

    const BoundPredicates = struct {
        terms: std.ArrayList(catalog.Condition) = .empty,
        primary_key: ?[]const u8 = null,
        empty: bool = false,
    };

    pub fn conditions(self: Context, table_def: catalog.Table, predicate: ?*const ast.Predicate) !BoundPredicates {
        var conditions_out: BoundPredicates = .{};
        try self.bindConditions(table_def, predicate, &conditions_out);
        return conditions_out;
    }

    fn bindConditions(self: Context, table_def: catalog.Table, predicate: ?*const ast.Predicate, output: *BoundPredicates) anyerror!void {
        const node = predicate orelse return;
        switch (node.*) {
            .comparison => |comparison| {
                const column = try table_def.column(comparison.field);
                // Row identity has a separate native key boundary; never
                // pretend it is a document property in a storage predicate.
                const bound_value = try self.value(comparison.value, column);
                // A JSON-null value is not SQL NULL. The current native
                // condition envelope cannot express that operand, so retain
                // this comparison in the already bound typed residual.
                if (bound_value == .null and column.type == .json and comparison.value == .string) return;
                // In a conjunction, comparison with SQL NULL can never make
                // WHERE true. Bind all remaining terms for diagnostics, but
                // do not scan a relation merely to rediscover UNKNOWN per row.
                if (bound_value == .null) {
                    output.empty = true;
                    return;
                }
                if (std.mem.eql(u8, column.name, "_id")) {
                    if (comparison.op != .eq) return; // Evaluated by the bound residual.
                    if (bound_value.string.len == 0) {
                        output.empty = true;
                    } else {
                        if (!std.unicode.utf8ValidateSlice(bound_value.string)) return error.SqlTypeMismatch;
                        if (output.primary_key) |previous| if (!std.mem.eql(u8, previous, bound_value.string)) {
                            output.empty = true;
                        };
                        output.primary_key = bound_value.string;
                    }
                    return;
                }
                const op: @FieldType(catalog.Condition, "op") = switch (comparison.op) {
                    inline else => |tag| @field(@FieldType(catalog.Condition, "op"), @tagName(tag)),
                };
                // Native relational predicates implement SQL three-valued
                // logic, including UNKNOWN for comparisons against NULL.
                try output.terms.append(self.arena, .{ .column = column.path, .op = op, .value = bound_value });
            },
            .is_null => |test_null| {
                const column = try table_def.column(test_null.field);
                if (std.mem.eql(u8, column.name, "_id")) {
                    if (!test_null.negated) output.empty = true;
                    return;
                }
                try output.terms.append(self.arena, .{ .column = column.path, .op = if (test_null.negated) .is_not_null else .is_null });
            },
            .conjunction => |both| {
                try self.bindConditions(table_def, both.left, output);
                try self.bindConditions(table_def, both.right, output);
            },
            // Push down only safe conjuncts. The complete bound residual is
            // evaluated before OFFSET/LIMIT/counting or mutation staging.
            .disjunction, .negation, .scalar => {},
        }
        if (output.terms.items.len > 256) return error.SqlProgramLimitExceeded;
    }

    pub fn select(self: Context, statement: ast.Select) anyerror!Output {
        if (self.binding.relation != null) return @import("relation_runtime.zig").execute(self);
        if (self.binding.aggregate != null) return @import("aggregate_runtime.zig").execute(self, statement);
        const table_def = self.binding.table orelse return self.constantSelect(statement);
        const predicates = try self.conditions(table_def, statement.predicate);
        const limit = try self.count(statement.limit, self.limits.result_rows);
        const offset = try self.count(statement.offset, 0);
        if (limit > self.limits.result_rows or offset > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
        var fields: std.ArrayList([]const u8) = .empty;
        const columns = self.binding.columns;
        if (statement.count_all) {
            // An empty native projection avoids materializing unused values.
        } else if (statement.columns.len == 0) {
            for (table_def.columns) |column| {
                try fields.append(self.arena, column.path);
            }
        } else {
            for (statement.columns) |projection| {
                if (projection.expression != null) {
                    try fields.append(self.arena, "");
                    continue;
                }
                const column = try table_def.column(projection.field);
                try fields.append(self.arena, column.path);
            }
        }
        // Native projection is a set; SQL output can repeat/alias a field.
        var native_fields: std.ArrayList([]const u8) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (fields.items) |field| {
            if (field.len == 0 or std.mem.eql(u8, field, "_id")) continue;
            const slot = try seen.getOrPut(self.arena, field);
            if (!slot.found_existing) try native_fields.append(self.arena, field);
        }
        for (self.binding.scalars.required) |ordinal| {
            const name = self.binding.scalars.columns[ordinal].name;
            if (std.mem.eql(u8, name, "_id")) continue;
            const slot = try seen.getOrPut(self.arena, name);
            if (!slot.found_existing) try native_fields.append(self.arena, name);
        }
        for (self.binding.order_keys) |key| switch (key.source) {
            .output, .expression => {},
            .column => |column| {
                if (std.mem.eql(u8, column.name, "_id")) continue;
                const slot = try seen.getOrPut(self.arena, column.path);
                if (!slot.found_existing) try native_fields.append(self.arena, column.path);
            },
        };
        var top_k: ?operators.TopK = null;
        defer if (top_k) |*operator| operator.deinit();
        if (self.binding.order_keys.len != 0 and !self.binding.primary_order and !statement.count_all and limit != 0) {
            const orders = try self.arena.alloc(operators.Order, self.binding.order_keys.len);
            for (self.binding.order_keys, orders) |key, *order| order.* = .{ .descending = key.descending, .nulls_first = key.nulls_first };
            top_k = try operators.TopK.init(self.alloc, offset + limit + @intFromBool(statement.limit == null), orders, self.limits.retained_bytes);
        }
        var rows: std.ArrayList([]const Json) = .empty;
        var null_rows: std.ArrayList([]const bool) = .empty;
        var scanned: usize = 0;
        var visited: usize = 0;
        var page_count: usize = 0;
        var retained: usize = 0;
        var after: ?[]const u8 = null;
        defer if (after) |key| self.alloc.free(key);
        var scan_state: ScanState = .{};
        defer scan_state.deinit();
        if (limit == 0) return .{ .columns = columns, .command_tag = "SELECT" };
        while (!predicates.empty) {
            try self.checkpoint();
            page_count += 1;
            if (page_count > self.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            const wanted = if (statement.count_all or top_k != null) self.limits.page_rows else @min(
                self.limits.page_rows,
                (offset -| scanned) + (limit - rows.items.len) + @as(usize, if (statement.limit == null) 1 else 0),
            );
            const page = try scan_state.page(self, page_arena.allocator(), table_def, .{
                .fields = native_fields.items,
                .primary_order = self.binding.primary_order,
                .primary_key = predicates.primary_key,
                .conditions = predicates.terms.items,
                .after = after,
                .limit = @intCast(wanted),
            });
            defer page.deinit();
            if (page.rows.len > wanted) return error.InvalidSqlBackendResponse;
            for (page.rows) |row| {
                try self.checkpoint();
                visited += 1;
                if (visited > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const expression_cells = try self.binding.scalars.cells(page_arena.allocator(), row);
                if (!try self.binding.scalars.matches(page_arena.allocator(), expression_cells, self.parameters)) continue;
                scanned += 1;
                if (statement.count_all) continue;
                if (top_k) |*operator| {
                    const values = try self.projectValues(page_arena.allocator(), row, fields.items, expression_cells);
                    const keys = try page_arena.allocator().alloc(Datum, self.binding.order_keys.len);
                    for (self.binding.order_keys, keys) |key, *out| out.* = switch (key.source) {
                        .output => |index| values[index],
                        .expression => |index| try self.binding.scalars.orders[index].?.evaluate(page_arena.allocator(), expression_cells, self.parameters, .{}),
                        .column => |column| blk: {
                            const cell = try row.cell(column.path);
                            break :blk .{ .value = try coerce(cell.value, column.type), .sql_null = cell.sql_null };
                        },
                    };
                    try operator.add(.{ .values = values, .keys = keys, .ordinal = visited });
                    continue;
                }
                if (scanned <= offset) continue;
                if (rows.items.len == limit) {
                    if (statement.limit == null) return error.SqlResultTooLarge;
                    return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
                }
                const cells = try self.arena.alloc(Json, fields.items.len);
                const nulls = try self.arena.alloc(bool, fields.items.len);
                for (fields.items, columns, cells, nulls, 0..) |field, column, *cell, *is_null, index| {
                    const program = if (index < self.binding.scalars.projections.len) self.binding.scalars.projections[index] else null;
                    const input_cell: catalog.Row.Cell = if (program) |expression| blk: {
                        const evaluated = try expression.evaluate(page_arena.allocator(), expression_cells, self.parameters, .{});
                        break :blk .{ .value = evaluated.value, .sql_null = evaluated.sql_null };
                    } else try row.cell(field);
                    const typed = try coerce(input_cell.value, column.type);
                    is_null.* = input_cell.sql_null;
                    // SQL bigint results are lossless even in JS SDKs.
                    cell.* = try self.outputValue(typed);
                    retained = std.math.add(usize, retained, jsonSize(cell.*)) catch return error.SqlProgramLimitExceeded;
                    if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                }
                try rows.append(self.arena, cells);
                try null_rows.append(self.arena, nulls);
                if (rows.items.len == limit and statement.limit != null) return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
            }
            const next = page.after orelse break;
            if (!scan_state.retained(self)) return error.SqlStatementSnapshotRequired;
            if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
            const owned_next = try self.alloc.dupe(u8, next);
            if (after) |previous| self.alloc.free(previous);
            after = owned_next;
        }
        if (top_k) |*operator| {
            const ordered = try operator.finish(self.arena);
            const remaining = ordered.len -| offset;
            if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
            for (ordered[@min(offset, ordered.len)..][0..@min(remaining, limit)]) |row| {
                try self.checkpoint();
                const cells = try self.arena.alloc(Json, row.values.len);
                const nulls = try self.arena.alloc(bool, row.values.len);
                for (row.values, cells, nulls) |value_, *cell, *is_null| {
                    cell.* = try self.outputValue(value_.value);
                    is_null.* = value_.sql_null;
                }
                try rows.append(self.arena, cells);
                try null_rows.append(self.arena, nulls);
            }
        }
        if (statement.count_all and offset == 0) {
            const cells = try self.arena.alloc(Json, 1);
            cells[0] = try self.outputValue(.{ .integer = @intCast(scanned) });
            try rows.append(self.arena, cells);
            const nulls = try self.arena.alloc(bool, 1);
            nulls[0] = false;
            try null_rows.append(self.arena, nulls);
        }
        return .{ .columns = columns, .rows = rows.items, .sql_nulls = null_rows.items, .command_tag = "SELECT" };
    }

    fn projectValues(self: Context, alloc: std.mem.Allocator, row: catalog.Row, fields: []const []const u8, expression_cells: []const Datum) ![]const Datum {
        const values = try alloc.alloc(Datum, fields.len);
        for (fields, self.binding.columns, values, 0..) |field, column, *out, index| {
            const program = if (index < self.binding.scalars.projections.len) self.binding.scalars.projections[index] else null;
            const input = if (program) |expression| try expression.evaluate(alloc, expression_cells, self.parameters, .{}) else blk: {
                const cell = try row.cell(field);
                break :blk Datum{ .value = cell.value, .sql_null = cell.sql_null };
            };
            out.* = .{ .value = try coerce(input.value, column.type), .sql_null = input.sql_null };
        }
        return values;
    }

    fn constantSelect(self: Context, statement: ast.Select) !Output {
        const limit = try self.count(statement.limit, self.limits.result_rows);
        const offset = try self.count(statement.offset, 0);
        if (limit > self.limits.result_rows or offset > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
        const columns = self.binding.columns;
        if (limit == 0 or offset != 0) return .{ .columns = columns, .command_tag = "SELECT" };
        try self.checkpoint();
        const matches = try self.binding.scalars.matches(self.arena, &.{}, self.parameters);
        if (!matches and !statement.count_all) return .{ .columns = columns, .command_tag = "SELECT" };
        const rows = try self.arena.alloc([]const Json, 1);
        const cells = try self.arena.alloc(Json, columns.len);
        const null_rows = try self.arena.alloc([]const bool, 1);
        const nulls = try self.arena.alloc(bool, columns.len);
        if (statement.count_all) {
            cells[0] = try self.outputValue(.{ .integer = @intFromBool(matches) });
            nulls[0] = false;
        } else for (self.binding.scalars.projections, cells, nulls) |optional, *cell, *is_null| {
            const program = optional orelse return error.InvalidSqlBackendResponse;
            const evaluated = try program.evaluate(self.arena, &.{}, self.parameters, .{});
            cell.* = try self.outputValue(evaluated.value);
            is_null.* = evaluated.sql_null;
        }
        rows[0] = cells;
        null_rows[0] = nulls;
        return .{ .columns = columns, .rows = rows, .sql_nulls = null_rows, .command_tag = "SELECT" };
    }

    fn insert(self: Context, statement: ast.Insert) !Output {
        if (statement.source) |source| return self.insertSelect(statement, source.*);
        const table_def = self.binding.table orelse return error.InvalidSqlBackendResponse;
        if (statement.rows.len > self.limits.mutation_rows) return error.SqlProgramLimitExceeded;
        const columns = try self.arena.alloc(catalog.Column, statement.columns.len);
        var key_index: ?usize = null;
        for (statement.columns, columns, 0..) |name, *column, i| {
            column.* = try table_def.column(name);
            if (std.mem.eql(u8, name, "_id")) key_index = i;
            for (statement.columns[0..i]) |previous| if (std.mem.eql(u8, previous, name)) return error.DuplicateColumn;
        }
        // Generated physical identities must have a native primary-key policy.
        // Until bound, explicit _id is required; never invent unstable hashes.
        const key_at = key_index orelse return error.SqlRowIdentityRequired;
        const mutations = try self.arena.alloc(catalog.Mutation, statement.rows.len);
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        var retained: usize = 0;
        for (statement.rows, mutations, 0..) |row, *mutation, row_index| {
            try self.checkpoint();
            if (row.len != columns.len) return error.InvalidSqlParameters;
            const key_datum = try self.insertValue(row[key_at], columns[key_at], row_index, key_at);
            const key = key_datum.value;
            if (key_datum.sql_null or key != .string or key.string.len == 0) return error.SqlRowIdentityRequired;
            if (!std.unicode.utf8ValidateSlice(key.string)) return error.SqlTypeMismatch;
            if ((try keys.getOrPut(self.arena, key.string)).found_existing) return error.DuplicateSqlRow;
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            for (columns, row, 0..) |column, item, i| {
                if (i == key_at) continue;
                const datum = try self.insertValue(item, column, row_index, i);
                const typed = datum.value;
                const json_null = typed == .null and !datum.sql_null;
                if (datum.sql_null and !column.nullable) return error.SqlNotNullViolation;
                if (json_null) try json_null_fields.append(self.arena, column.path);
                try putField(self.arena, &object, column.path, try clone(self.arena, typed));
            }
            const document: Json = .{ .object = object };
            retained = std.math.add(usize, retained, jsonSize(document) + key.string.len) catch return error.SqlProgramLimitExceeded;
            if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
            mutation.* = .{ .key = key.string, .expected_version = 0, .row = document, .json_null_fields = json_null_fields.items };
        }
        try self.checkpoint();
        return self.commitMutations(table_def, mutations, "INSERT", statement.returning);
    }

    fn insertSelect(self: Context, statement: ast.Insert, source: ast.Select) !Output {
        const table = self.binding.table orelse return error.InvalidSqlBackendResponse;
        const bound = self.binding.insert_source orelse return error.InvalidSqlBackendResponse;
        const target_columns = try self.arena.alloc(catalog.Column, statement.columns.len);
        for (statement.columns, target_columns) |name, *column| column.* = try table.column(name);
        var input = self;
        input.binding = bound.*;
        input.typed_output = true;
        input.limits.result_rows = self.limits.mutation_rows;
        // Materialize a bounded statement result before any mutation. This
        // also releases source cursors before writer admission and prevents
        // self-inserts from reading their own writes (Halloween problem).
        const selected = try input.select(source);
        if (selected.columns.len != statement.columns.len or selected.rows.len > self.limits.mutation_rows) return error.InvalidSqlBackendResponse;
        const flags = selected.sql_nulls orelse if (selected.rows.len == 0) &.{} else return error.InvalidSqlBackendResponse;
        if (flags.len != selected.rows.len) return error.InvalidSqlBackendResponse;
        const mutations = try self.arena.alloc(catalog.Mutation, selected.rows.len);
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        for (selected.rows, flags, mutations) |row, nulls, *mutation| {
            try self.checkpoint();
            if (row.len != statement.columns.len or nulls.len != row.len) return error.InvalidSqlBackendResponse;
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            var key: ?[]const u8 = null;
            for (target_columns, selected.columns, row, nulls) |target, source_column, value_, sql_null| {
                if (sql_null and value_ != .null) return error.InvalidSqlBackendResponse;
                if (sql_null and !target.nullable) return error.SqlNotNullViolation;
                if (!sql_null and source_column.type != target.type and !(source_column.type == .integer and target.type == .number)) return error.SqlTypeMismatch;
                const typed = try coerce(value_, target.type);
                if (std.mem.eql(u8, target.name, "_id")) {
                    if (sql_null or typed != .string or typed.string.len == 0) return error.SqlRowIdentityRequired;
                    if (!std.unicode.utf8ValidateSlice(typed.string)) return error.SqlTypeMismatch;
                    key = typed.string;
                } else {
                    if (!sql_null and typed == .null) try json_null_fields.append(self.arena, target.path);
                    // The typed source already belongs to the same bounded
                    // result arena: transfer references without JSON reparsing
                    // or a second copy of large JSON/string cells.
                    try putField(self.arena, &object, target.path, typed);
                }
            }
            const identity = key orelse return error.SqlRowIdentityRequired;
            if ((try keys.getOrPut(self.arena, identity)).found_existing) return error.DuplicateSqlRow;
            mutation.* = .{ .key = identity, .expected_version = 0, .row = .{ .object = object }, .json_null_fields = json_null_fields.items };
        }
        try self.checkpoint();
        return self.commitMutations(table, mutations, "INSERT", statement.returning);
    }

    fn insertValue(self: Context, literal: ast.Value, column: catalog.Column, row: usize, cell: usize) !@import("scalar.zig").Datum {
        if (self.binding.scalars.insert_rows.len != 0) if (self.binding.scalars.insert_rows[row][cell]) |program| {
            const result = try program.evaluate(self.arena, &.{}, self.parameters, .{});
            return .{ .value = try coerce(result.value, column.type), .sql_null = result.sql_null };
        };
        const result = try self.value(literal, column);
        return .{ .value = result, .sql_null = result == .null and !(column.type == .json and literal == .string) };
    }

    fn change(self: Context, _: ast.Name, predicate: ?*const ast.Predicate, assignments: ?[]const ast.Assignment, requested_returning: ?[]const ast.Projection) !Output {
        const returning = if (requested_returning != null) self.binding.returning_projections else null;
        const table_def = self.binding.table orelse return error.InvalidSqlBackendResponse;
        const predicates = try self.conditions(table_def, predicate);
        // Validate assignments before reading or staging any row.
        const Assignment = struct { column: catalog.Column, value: Json, sql_null: bool, program: ?@import("scalar.zig").Program = null };
        const bound_assignments = try self.arena.alloc(Assignment, if (assignments) |items| items.len else 0);
        var replaced: std.StringHashMapUnmanaged(void) = .empty;
        if (assignments) |items| for (items, bound_assignments, 0..) |item, *bound, i| {
            const column = try table_def.column(item.field);
            if (std.mem.eql(u8, column.name, "_id")) return error.UnsupportedSqlExecution;
            for (items[0..i]) |previous| if (std.mem.eql(u8, previous.field, item.field)) return error.DuplicateColumn;
            const program = if (i < self.binding.scalars.assignments.len) self.binding.scalars.assignments[i] else null;
            const typed = if (program == null) try self.value(item.value, column) else .null;
            const sql_null = typed == .null and !(column.type == .json and item.value == .string);
            if (program == null and sql_null and !column.nullable) return error.SqlNotNullViolation;
            // These constants are immutable for the entire statement. Own
            // them once, then share them across the prepared replacement rows.
            bound.* = .{ .column = column, .value = try clone(self.arena, typed), .sql_null = sql_null, .program = program };
            try replaced.put(self.arena, column.path, {});
        };
        var fields: std.ArrayList([]const u8) = .empty;
        if (assignments != null) for (table_def.columns) |column| {
            // The native row version still fences the entire row. Loading an
            // overwritten value adds no conflict protection and can dominate
            // I/O/memory for wide JSON/blob-like columns.
            if (!column.generated and !replaced.contains(column.path)) try fields.append(self.arena, column.path);
        };
        if (assignments == null and returning != null) {
            if (returning.?.len == 0) {
                for (table_def.columns) |column| try fields.append(self.arena, column.path);
            } else for (returning.?) |projection| {
                if (projection.expression == null and !std.mem.eql(u8, projection.field, "_id")) {
                    const path = (try table_def.column(projection.field)).path;
                    if (!contains(fields.items, path)) try fields.append(self.arena, path);
                }
            }
            const projection = self.binding.returning.?;
            for (projection.scalars.required) |ordinal| {
                const name = projection.scalars.columns[ordinal].name;
                if (!std.mem.eql(u8, name, "_id") and !contains(fields.items, name)) try fields.append(self.arena, name);
            }
        }
        for (self.binding.scalars.required) |ordinal| {
            const name = self.binding.scalars.columns[ordinal].name;
            if (std.mem.eql(u8, name, "_id")) continue;
            var present = false;
            for (fields.items) |field| if (std.mem.eql(u8, field, name)) {
                present = true;
                break;
            };
            if (!present) try fields.append(self.arena, name);
        }
        var mutations: std.ArrayList(catalog.Mutation) = .empty;
        var retained: usize = 0;
        var after: ?[]const u8 = null;
        var page_count: usize = 0;
        var visited: usize = 0;
        defer if (after) |key| self.alloc.free(key);
        var scan_state: ScanState = .{};
        defer scan_state.deinit();
        while (!predicates.empty) {
            try self.checkpoint();
            page_count += 1;
            if (page_count > self.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var page_arena = std.heap.ArenaAllocator.init(self.alloc);
            defer page_arena.deinit();
            const page = try scan_state.page(self, page_arena.allocator(), table_def, .{ .fields = fields.items, .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = self.limits.page_rows });
            defer page.deinit();
            if (page.rows.len > self.limits.page_rows) return error.InvalidSqlBackendResponse;
            for (page.rows) |row| {
                try self.checkpoint();
                visited += 1;
                if (visited > self.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const expression_cells = try self.binding.scalars.cells(page_arena.allocator(), row);
                if (!try self.binding.scalars.matches(page_arena.allocator(), expression_cells, self.parameters)) continue;
                if (mutations.items.len >= self.limits.mutation_rows) return error.SqlProgramLimitExceeded;
                var document: ?Json = null;
                var json_null_fields: std.ArrayList([]const u8) = .empty;
                if (assignments != null) {
                    if (row.value != .object) return error.InvalidSqlBackendResponse;
                    var copy: Json = .{ .object = .empty };
                    for (table_def.columns) |column| {
                        if (column.generated or replaced.contains(column.path)) continue;
                        if (row.value.object.get(column.path)) |old| {
                            const cell = try row.cell(column.path);
                            if (old == .null and !cell.sql_null) try json_null_fields.append(self.arena, column.path);
                            try putField(self.arena, &copy.object, column.path, try clone(self.arena, old));
                        }
                    }
                    for (bound_assignments) |bound| {
                        const assigned_value = if (bound.program) |program| blk: {
                            const evaluated = try program.evaluate(page_arena.allocator(), expression_cells, self.parameters, .{});
                            if (evaluated.sql_null and !bound.column.nullable) return error.SqlNotNullViolation;
                            if (!evaluated.sql_null and evaluated.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                            break :blk try clone(self.arena, try coerce(evaluated.value, bound.column.type));
                        } else blk: {
                            if (!bound.sql_null and bound.value == .null) try json_null_fields.append(self.arena, bound.column.path);
                            break :blk bound.value;
                        };
                        try putField(self.arena, &copy.object, bound.column.path, assigned_value);
                    }
                    document = copy;
                }
                retained = std.math.add(usize, retained, row.id.len + if (document) |doc| jsonSize(doc) else 0) catch return error.SqlProgramLimitExceeded;
                if (retained > self.limits.retained_bytes) return error.SqlProgramLimitExceeded;
                const key = try self.arena.dupe(u8, row.id);
                try mutations.append(self.arena, .{ .key = key, .expected_version = row.version, .row = document, .json_null_fields = json_null_fields.items, .previous = if (assignments == null and returning != null) .{ .id = key, .version = row.version, .value = try clone(self.arena, row.value), .sql_nulls = if (row.sql_nulls) |flags| try self.arena.dupe(bool, flags) else null } else null });
            }
            const next = page.after orelse break;
            if (!scan_state.retained(self)) return error.SqlStatementSnapshotRequired;
            if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
            const owned_next = try self.alloc.dupe(u8, next);
            if (after) |previous| self.alloc.free(previous);
            after = owned_next;
        }
        try self.checkpoint();
        // Prepared rows own their values and version fences. Do not retain a
        // read snapshot while waiting for writer/commit admission.
        scan_state.deinit();
        return self.commitMutations(table_def, mutations.items, if (assignments != null) "UPDATE" else "DELETE", returning);
    }

    fn commitMutations(self: Context, table: catalog.Table, input: []const catalog.Mutation, tag: []const u8, returning: ?[]const ast.Projection) !Output {
        var output: Output = .{ .command_tag = tag, .rows_affected = input.len };
        var prepared = input;
        if (returning != null) {
            const projections = self.binding.returning_projections orelse return error.InvalidSqlBackendResponse;
            if (input.len > self.limits.result_rows) return error.SqlResultTooLarge;
            const binding = self.binding.returning orelse return error.InvalidSqlBackendResponse;
            if (input.len != 0 and !std.mem.eql(u8, tag, "DELETE")) {
                const prepare = self.backend.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
                prepared = try prepare(self.backend.ptr, self.arena, table, input);
                if (prepared.len != input.len) return error.InvalidSqlBackendResponse;
            }
            var context = self;
            context.binding = binding.*;
            var fields: std.ArrayList([]const u8) = .empty;
            if (projections.len == 0) {
                for (table.columns) |column| try fields.append(self.arena, column.path);
            } else for (projections) |projection| try fields.append(self.arena, if (projection.expression != null) "" else (try table.column(projection.field)).path);
            const rows = try self.arena.alloc([]const Json, prepared.len);
            const flags = try self.arena.alloc([]const bool, prepared.len);
            for (prepared, input, rows, flags) |mutation, original, *cells, *nulls| {
                try self.checkpoint();
                if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
                const row = if (mutation.row) |value_| blk: {
                    if (value_ != .object) return error.InvalidSqlBackendResponse;
                    const typed_nulls = try self.arena.alloc(bool, value_.object.count());
                    for (value_.object.values(), typed_nulls) |cell_value, *flag| flag.* = cell_value == .null;
                    for (mutation.json_null_fields) |name| {
                        const index = value_.object.getIndex(name) orelse return error.InvalidSqlBackendResponse;
                        if (!typed_nulls[index] or (try table.column(name)).type != .json) return error.InvalidSqlBackendResponse;
                        typed_nulls[index] = false;
                    }
                    break :blk catalog.Row{ .id = mutation.key, .version = mutation.expected_version, .value = value_, .sql_nulls = typed_nulls };
                } else original.previous orelse return error.InvalidSqlBackendResponse;
                const expressions = try binding.scalars.cells(self.arena, row);
                const projected = try context.projectValues(self.arena, row, fields.items, expressions);
                const values = try self.arena.alloc(Json, projected.len);
                const sql_nulls = try self.arena.alloc(bool, projected.len);
                for (projected, values, sql_nulls) |value_, *cell_value, *is_null| {
                    cell_value.* = try self.outputValue(value_.value);
                    is_null.* = value_.sql_null;
                }
                cells.* = values;
                nulls.* = sql_nulls;
            }
            output.columns = binding.columns;
            output.rows = rows;
            output.sql_nulls = flags;
        }
        // Projection, quotas and normalization can fail only BEFORE commit.
        // No post-commit lookup or allocation can replace the known outcome.
        try self.checkpoint();
        output.mutation_outcome = if (prepared.len == 0) .committed else try self.backend.vtable.mutate(self.backend.ptr, self.arena, table, prepared);
        return output;
    }
};

fn coerce(raw: Json, kind: ast.ColumnType) !Json {
    return describe.coerce(raw, kind);
}

fn contains(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| if (std.mem.eql(u8, item, needle)) return true;
    return false;
}

fn fieldValue(root: Json, path: []const u8) Json {
    if (root != .object) return .null;
    // Relational column names are literal properties. A quoted "a.b" must
    // never be reinterpreted as a document-path expression.
    return root.object.get(path) orelse .null;
}

fn putField(arena: std.mem.Allocator, object: *std.json.ObjectMap, path: []const u8, value: Json) !void {
    try object.put(arena, path, value);
}

fn clone(arena: std.mem.Allocator, value: Json) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    return cloneDepth(arena, value, 0);
}

fn cloneDepth(arena: std.mem.Allocator, value: Json, depth: usize) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try arena.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try arena.dupe(u8, text) },
        .object => |object| blk: {
            var result: std.json.ObjectMap = .empty;
            for (object.keys(), object.values()) |key, item| try result.put(arena, try arena.dupe(u8, key), try cloneDepth(arena, item, depth + 1));
            break :blk .{ .object = result };
        },
        .array => |array| blk: {
            var result: std.array_list.Managed(Json) = .init(arena);
            try result.ensureTotalCapacity(array.items.len);
            for (array.items) |item| result.appendAssumeCapacity(try cloneDepth(arena, item, depth + 1));
            break :blk .{ .array = result };
        },
        else => value,
    };
}

fn jsonSize(value: Json) usize {
    return switch (value) {
        .string, .number_string => |text| @sizeOf(Json) +| text.len,
        .object => |object| blk: {
            var bytes: usize = @sizeOf(Json);
            for (object.keys(), object.values()) |key, item| bytes +|= key.len +| jsonSize(item);
            break :blk bytes;
        },
        .array => |array| blk: {
            var bytes: usize = @sizeOf(Json);
            for (array.items) |item| bytes +|= jsonSize(item);
            break :blk bytes;
        },
        else => @sizeOf(Json),
    };
}

const TestBackend = struct {
    pages: usize = 0,
    writes: usize = 0,
    row_count: usize = 2,
    cancelled: bool = false,
    ambiguous: bool = false,
    outcome: catalog.MutationOutcome = .committed,
    point_reads: usize = 0,
    primary_order: bool = false,
    statement_opens: usize = 0,
    statement_closes: usize = 0,

    fn iface(self: *TestBackend) catalog.Backend {
        return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn coordinated(self: *TestBackend) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .open_statement = openStatement } };
    }
    const Statement = struct {
        backend: *TestBackend,
        alloc: std.mem.Allocator,
        cursors: []catalog.Cursor,
        states: []State,
        const State = struct { owner: *Statement, table: catalog.Table, request: catalog.Scan, after: ?[]const u8 = null, done: bool = false };
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.done) return .{ .rows = &.{} };
            var request = state.request;
            request.limit = limit;
            request.after = state.after;
            const page = try TestBackend.scan(state.owner.backend, alloc, state.table, request);
            const next_key = if (page.after) |key| try state.owner.alloc.dupe(u8, key) else null;
            if (state.after) |key| state.owner.alloc.free(key);
            state.after = next_key;
            state.done = page.after == null;
            return page;
        }
        fn closeCursor(_: *anyopaque) void {
            @panic("statement-owned cursor closed individually");
        }
        fn close(ptr: *anyopaque) void {
            const self: *Statement = @ptrCast(@alignCast(ptr));
            self.backend.statement_closes += 1;
            for (self.states) |state| if (state.after) |key| self.alloc.free(key);
            self.alloc.free(self.states);
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };
    fn openStatement(ptr: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const backend: *TestBackend = @ptrCast(@alignCast(ptr));
        const owner = try alloc.create(Statement);
        errdefer alloc.destroy(owner);
        const cursors = try alloc.alloc(catalog.Cursor, scans.len);
        errdefer alloc.free(cursors);
        const states = try alloc.alloc(Statement.State, scans.len);
        owner.* = .{ .backend = backend, .alloc = alloc, .cursors = cursors, .states = states };
        for (scans, states, cursors) |request, *state, *cursor| {
            state.* = .{ .owner = owner, .table = request.table, .request = request.request };
            cursor.* = .{ .ptr = state, .next = Statement.next, .close = Statement.closeCursor };
        }
        backend.statement_opens += 1;
        return .{ .ptr = owner, .cursors = cursors, .close = Statement.close };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 19, .physical_name = "table:stable", .schema_version = 7, .columns = &.{.{ .name = "id", .path = "id", .type = .integer, .nullable = false }} };
    }
    fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table_def: catalog.Table, request: catalog.Scan) !catalog.Page {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(u64, 19), table_def.id);
        try std.testing.expectEqual(@as(u32, 7), table_def.schema_version);
        self.pages += 1;
        self.primary_order = request.primary_order;
        const from = if (request.primary_key orelse request.after) |key| try std.fmt.parseInt(usize, key, 10) else 0;
        if (request.primary_key != null) self.point_reads += 1;
        const count_rows = @min(if (request.primary_key != null) @as(u32, 1) else request.limit, self.row_count -| from);
        const rows = try alloc.alloc(catalog.Row, count_rows);
        for (rows, from..) |*row, index| {
            var object: std.json.ObjectMap = .empty;
            try object.put(alloc, "id", .{ .number_string = "9007199254740993" });
            row.* = .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{index}), .version = 99, .value = .{ .object = object } };
        }
        return .{ .rows = rows, .after = if (request.primary_key == null and from + count_rows < self.row_count) try std.fmt.allocPrint(alloc, "{d}", .{from + count_rows}) else null };
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table_def: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        try std.testing.expectEqual(@as(u32, 7), table_def.schema_version);
        for (mutations) |mutation| {
            try std.testing.expectEqual(@as(u64, 99), mutation.expected_version);
            if (mutation.row) |row| try std.testing.expectEqual(@as(i64, 9007199254740993), row.object.get("id").?.integer);
        }
        if (self.ambiguous) return error.AmbiguousCommit;
        return self.outcome;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *TestBackend = @ptrCast(@alignCast(ptr));
        if (self.cancelled) return error.Cancelled;
    }
};

test "SQL grouped aggregation streams pages then applies HAVING ORDER OFFSET and LIMIT" {
    var backend: TestBackend = .{ .row_count = 8 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id::bigint % 2 AS bucket, sum(_id::bigint) AS amount, count(*) AS n FROM things WHERE _id != '0' GROUP BY bucket HAVING sum(_id::bigint) > 5 ORDER BY amount DESC LIMIT 1 OFFSET 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 8), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("12", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][2].string);
}

test "SQL joins use one coordinated cut and preserve typed outer nulls across pages" {
    var backend: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id AS a, b._id AS b FROM things AS a LEFT JOIN (SELECT _id FROM things WHERE _id != '2') AS b ON a._id = b._id ORDER BY a._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[1][1].string);
    try std.testing.expectEqualStrings("2", result.output.rows[2][0].string);
    try std.testing.expect(result.output.sql_nulls.?[2][1]);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    try std.testing.expectError(error.SqlStatementSnapshotRequired, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
}

test "SQL CTE aliases feed hash joins and grouped projection without stringifying rows" {
    var backend: TestBackend = .{ .row_count = 4 };
    var compiled = try compiler.compile(std.testing.allocator, "WITH q(k) AS (SELECT _id FROM things WHERE _id != '3') SELECT a.k, count(*) AS n FROM q AS a JOIN q AS b ON a.k = b.k GROUP BY a.k ORDER BY a.k DESC", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("2", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
}

test "SQL self joins preserve duplicate matches and evaluate complete ON residuals" {
    var backend: TestBackend = .{ .row_count = 3 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id AS a, b._id AS b FROM things a JOIN things b ON a.id = b.id AND a._id < b._id ORDER BY a._id, b._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    for (result.output.rows, [_][2][]const u8{ .{ "0", "1" }, .{ "0", "2" }, .{ "1", "2" } }) |row, expected| {
        try std.testing.expectEqualStrings(expected[0], row[0].string);
        try std.testing.expectEqualStrings(expected[1], row[1].string);
    }
    try std.testing.expectEqual(@as(usize, 6), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
}

test "SQL outer joins retain rejected ON candidates and SQL null keys as unmatched" {
    const cases = [_]struct { kind: []const u8, rows: usize, left_nulls: usize, right_nulls: usize }{
        .{ .kind = "LEFT", .rows = 3, .left_nulls = 1, .right_nulls = 2 },
        .{ .kind = "RIGHT", .rows = 3, .left_nulls = 2, .right_nulls = 1 },
        .{ .kind = "FULL", .rows = 5, .left_nulls = 3, .right_nulls = 3 },
    };
    for (cases) |case| {
        var backend: TestBackend = .{ .row_count = 3 };
        const sql = try std.fmt.allocPrint(std.testing.allocator, "WITH q AS (SELECT CASE WHEN _id = '2' THEN NULL ELSE _id END AS k FROM things) SELECT a.k, b.k FROM q a {s} JOIN q b ON a.k = b.k AND a.k != '1'", .{case.kind});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        var nulls = [_]usize{ 0, 0 };
        var matches: usize = 0;
        for (result.output.sql_nulls.?) |flags| {
            for (flags, &nulls) |flag, *count| count.* += @intFromBool(flag);
            matches += @intFromBool(!flags[0] and !flags[1]);
        }
        try std.testing.expectEqual(case.left_nulls, nulls[0]);
        try std.testing.expectEqual(case.right_nulls, nulls[1]);
        try std.testing.expectEqual(@as(usize, 1), matches);
        try std.testing.expectEqual(@as(usize, 1), backend.statement_closes);
    }
}

test "SQL derived joins preserve JSON null independently of outer SQL null" {
    var backend: TestBackend = .{ .row_count = 2 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT a._id, b.j FROM things a LEFT JOIN (SELECT _id, 'null'::json AS j FROM things WHERE _id = '0') b ON a._id = b._id ORDER BY a._id", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expect(!result.output.sql_nulls.?[0][1]);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expect(result.output.rows[1][1] == .null);
}

test "SQL relation cursors close on early limit quota failure and every allocation failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            defer std.debug.assert(backend.statement_opens == backend.statement_closes);
            var result = try execute(alloc, backend.coordinated(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "WITH q AS (SELECT _id FROM things) SELECT a._id, b._id FROM q a FULL JOIN q b ON a._id = b._id LIMIT 1", .{});
    defer compiled.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
    var backend: TestBackend = .{ .row_count = 3 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.coordinated(), &compiled, &.{}, .{ .page_rows = 1, .scan_rows = 2 }));
    try std.testing.expectEqual(@as(usize, 1), backend.statement_opens);
    try std.testing.expectEqual(backend.statement_opens, backend.statement_closes);
}

test "SQL empty global aggregates produce one row and grouped empty input produces none" {
    var backend: TestBackend = .{ .row_count = 0 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(id), avg(id), min(id), bool_and(id > 0) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, true, true }, result.output.sql_nulls.?[0]);
    var grouped = try compiler.compile(std.testing.allocator, "SELECT id, count(*) FROM things GROUP BY id", .{});
    defer grouped.deinit();
    var empty = try execute(std.testing.allocator, backend.iface(), &grouped, &.{}, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.output.rows.len);
}

test "SQL tableless aggregate and HAVING preserve SQL null semantics" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*), sum(2), avg(2), max('null'::json) WHERE FALSE", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, true }, result.output.sql_nulls.?[0]);
    var accepted = try compiler.compile(std.testing.allocator, "SELECT count(*), max('null'::json) HAVING count(*) = 1", .{});
    defer accepted.deinit();
    var present = try execute(std.testing.allocator, backend.iface(), &accepted, &.{}, .{});
    defer present.deinit();
    try std.testing.expectEqualSlices(bool, &.{ false, false }, present.output.sql_nulls.?[0]);
    try std.testing.expect(present.output.rows[0][1] == .null);
}

test "SQL aggregate parameter inference and allocation failures are statement wide" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: TestBackend = .{ .row_count = 3 };
            var compiled = try compiler.compile(alloc, "SELECT $1 + 1 AS shifted, max($1) AS largest FROM things HAVING count(*) > 0", .{});
            defer compiled.deinit();
            var result = try execute(alloc, backend.iface(), &compiled, &.{.{ .integer = 4 }}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqualStrings("5", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("4", result.output.rows[0][1].string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL DISTINCT aggregates use semantic values and FILTER skips unused expressions" {
    var backend: TestBackend = .{ .row_count = 6 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(DISTINCT id), sum(DISTINCT _id::bigint % 2), count(*) FILTER (WHERE _id::bigint % 2 = 0), sum(1 / 0) FILTER (WHERE FALSE) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][2].string);
    try std.testing.expect(result.output.sql_nulls.?[0][3]);
}

test "SQL aggregate conjuncts keep native point seek and contradictory empty optimization" {
    var backend: TestBackend = .{ .row_count = 20 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT sum(id), count(*) FROM things WHERE _id = '2'", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.point_reads);
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    var empty = try compiler.compile(std.testing.allocator, "SELECT sum(id), count(*) FROM things WHERE _id = '2' AND _id = '3'", .{});
    defer empty.deinit();
    var empty_result = try execute(std.testing.allocator, backend.iface(), &empty, &.{}, .{});
    defer empty_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqualStrings("0", empty_result.output.rows[0][1].string);
}

test "SQL executor pages projection and preserves exact integers" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id AS exact, id FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), backend.pages);
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expectEqualStrings("exact", result.output.columns[0].name);
    try std.testing.expectEqualStrings("9007199254740993", result.output.rows[0][0].string);
}

test "SQL ordered scans carry required primary order to the access path" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id FROM things ORDER BY _id LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(backend.primary_order);
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
}

test "SQL result admission is not an implicit LIMIT" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlResultTooLarge, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .result_rows = 1, .page_rows = 1 }));
    var limited = try compiler.compile(std.testing.allocator, "SELECT id FROM things LIMIT 1", .{});
    defer limited.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &limited, &.{}, .{ .result_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
}

test "SQL mutation overflow never commits a partial write set" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .mutation_rows = 1, .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL mutations commit once with exact versions and never retry ambiguity" {
    var backend: TestBackend = .{ .ambiguous = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE things SET id = $1", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.AmbiguousCommit, execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .string = "9007199254740993" }}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL cancellation precedes mutation and disjunction applies a typed residual" {
    var backend: TestBackend = .{ .cancelled = true };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things WHERE id = 1 OR id = 2", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.Cancelled, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
    backend.cancelled = false;
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL expression projection and residual filtering precede offset limit and count" {
    var backend: TestBackend = .{ .row_count = 4 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id + 1 AS next, upper(_id) AS key FROM things WHERE _id = '1' OR _id = '3' LIMIT 1 OFFSET 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    try std.testing.expectEqualStrings("9007199254740994", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 4), backend.pages);
    try std.testing.expectEqualSlices(bool, &.{ false, false }, result.output.sql_nulls.?[0]);

    var counted = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things WHERE NOT (_id = '0' OR _id = '2')", .{});
    defer counted.deinit();
    var count_result = try execute(std.testing.allocator, backend.iface(), &counted, &.{}, .{ .page_rows = 1 });
    defer count_result.deinit();
    try std.testing.expectEqualStrings("2", count_result.output.rows[0][0].string);
}

test "SQL projected JSON null remains distinct from SQL NULL" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT CAST('null' AS JSON) AS j, NULL AS n FROM things LIMIT 1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.rows[0][0] == .null);
    try std.testing.expect(result.output.rows[0][1] == .null);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, result.output.sql_nulls.?[0]);
}

test "SQL tableless SELECT uses one logical row without catalog or storage access" {
    const NoLookup = struct {
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return error.UnexpectedCatalogLookup;
        }
    };
    const cases = [_]struct { sql: []const u8, expected: ?[]const u8 }{
        .{ .sql = "SELECT 2 + 3 AS sum", .expected = "5" },
        .{ .sql = "SELECT count(*) WHERE FALSE", .expected = "0" },
        .{ .sql = "SELECT count(*) WHERE TRUE", .expected = "1" },
        .{ .sql = "SELECT 1 WHERE FALSE", .expected = null },
        .{ .sql = "SELECT 1 LIMIT 0", .expected = null },
        .{ .sql = "SELECT 1 OFFSET 1", .expected = null },
    };
    var backend: TestBackend = .{};
    var iface = backend.iface();
    var vtable = iface.vtable.*;
    vtable.resolve = NoLookup.resolve;
    iface.vtable = &vtable;
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, iface, &compiled, &.{}, .{});
        defer result.deinit();
        if (case.expected) |expected| {
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings(expected, result.output.rows[0][0].string);
        } else try std.testing.expectEqual(@as(usize, 0), result.output.rows.len);
    }
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
    var undefined_column = try compiler.compile(std.testing.allocator, "SELECT 1 WHERE missing = 2", .{});
    defer undefined_column.deinit();
    try std.testing.expectError(error.UndefinedColumn, execute(std.testing.allocator, iface, &undefined_column, &.{}, .{}));
}

test "SQL expression errors never publish a partially prepared mutation" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE things SET id = id / CAST(_id AS INTEGER)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL statement never crosses fresh native page snapshots" {
    var backend: TestBackend = .{};
    var iface = backend.iface();
    iface.pinned_statement_snapshot = false;
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlStatementSnapshotRequired, execute(std.testing.allocator, iface, &compiled, &.{}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
}

test "SQL retained cursors close on completion early limit cancellation and write overflow" {
    const Provider = struct {
        const Self = @This();
        base: TestBackend = .{},
        opens: usize = 0,
        closes: usize = 0,
        cancel_after_page: bool = false,

        const Read = struct {
            alloc: std.mem.Allocator,
            provider: *Self,
            snapshot: TestBackend,
            table: catalog.Table,
            request: catalog.Scan,
            offset: usize = 0,

            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                var request = self.request;
                request.limit = limit;
                request.after = try std.fmt.allocPrint(alloc, "{d}", .{self.offset});
                const page = try TestBackend.scan(&self.snapshot, alloc, self.table, request);
                self.offset += page.rows.len;
                // Concurrent writes cannot extend a retained statement view.
                self.provider.base.row_count += 10;
                if (self.provider.cancel_after_page) self.provider.base.cancelled = true;
                return page;
            }

            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.provider.closes += 1;
                self.alloc.destroy(self);
            }
        };

        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const read = try alloc.create(Read);
            read.* = .{ .alloc = alloc, .provider = self, .snapshot = self.base, .table = table, .request = request };
            self.opens += 1;
            return .{ .ptr = read, .next = Read.next, .close = Read.close };
        }

        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try TestBackend.checkpoint(&self.base);
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedResult;
        }

        fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.opens, self.closes);
            return TestBackend.mutate(&self.base, alloc, table, mutations);
        }

        fn iface(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = TestBackend.resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    var count = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer count.deinit();
    var provider: Provider = .{};
    var result = try execute(std.testing.allocator, provider.iface(), &count, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqualStrings("2", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 1), provider.opens);
    try std.testing.expectEqual(provider.opens, provider.closes);

    var limited = try compiler.compile(std.testing.allocator, "SELECT id FROM things LIMIT 1", .{});
    defer limited.deinit();
    var small = try execute(std.testing.allocator, provider.iface(), &limited, &.{}, .{ .page_rows = 1 });
    defer small.deinit();
    try std.testing.expectEqual(@as(usize, 2), provider.closes);

    var deletion = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer deletion.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, provider.iface(), &deletion, &.{}, .{ .page_rows = 1, .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), provider.base.writes);
    try std.testing.expectEqual(@as(usize, 3), provider.closes);

    provider.base.row_count = 2;
    var committed = try execute(std.testing.allocator, provider.iface(), &deletion, &.{}, .{ .page_rows = 1 });
    defer committed.deinit();
    try std.testing.expectEqual(@as(usize, 1), provider.base.writes);
    try std.testing.expectEqual(@as(usize, 4), provider.closes);

    provider.cancel_after_page = true;
    try std.testing.expectError(error.Cancelled, execute(std.testing.allocator, provider.iface(), &count, &.{}, .{ .page_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 5), provider.closes);
}

test "SQL memory quota rejects allocations before backend work" {
    var backend: TestBackend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .retained_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.pages);
}

test "SQL page budget bounds progressing but empty backend pages" {
    const Empty = struct {
        pages: usize = 0,
        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            return .{ .rows = &.{}, .after = try std.fmt.allocPrint(alloc, "{d}", .{self.pages}) };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var fake: Empty = .{};
    const backend: catalog.Backend = .{ .ptr = &fake, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = TestBackend.resolve, .mutate = TestBackend.mutate, .scan = Empty.scan, .checkpoint = Empty.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, execute(std.testing.allocator, backend, &compiled, &.{}, .{ .scan_pages = 3 }));
    try std.testing.expectEqual(@as(usize, 3), fake.pages);
}

test "SQL quoted dotted columns remain literal properties" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var object: std.json.ObjectMap = .empty;
    try putField(arena.allocator(), &object, "a.b", .{ .integer = 7 });
    try std.testing.expectEqual(@as(i64, 7), fieldValue(.{ .object = object }, "a.b").integer);
    try std.testing.expect(object.get("a") == null);
}

test "SQL executor frees every partial allocation on failure" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator, compiled: *const compiler.Compiled) !void {
            var backend: TestBackend = .{};
            var result = try execute(alloc, backend.iface(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT id FROM things", .{});
    defer compiled.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}

test "SQL bounded JSON copy rejects nesting before exhausting the stack" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var root: Json = .null;
    for (0..70) |_| {
        var array: std.json.Array = .init(arena.allocator());
        try array.append(root);
        root = .{ .array = array };
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, clone(arena.allocator(), root));
}

test "SQL count releases pages instead of retaining the scanned relation" {
    var backend: TestBackend = .{ .row_count = 10_000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .retained_bytes = 512 * 1024 });
    defer result.deinit();
    try std.testing.expectEqualStrings("10000", result.output.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 40), backend.pages);
    try std.testing.expect(result.peakMemoryBytes() <= 512 * 1024);
}

test "SQL ORDER BY respects output aliases instead of silently using physical keys" {
    var backend: TestBackend = .{};
    var shadowed = try compiler.compile(std.testing.allocator, "SELECT id AS _id FROM things ORDER BY _id", .{});
    defer shadowed.deinit();
    var sorted = try execute(std.testing.allocator, backend.iface(), &shadowed, &.{}, .{});
    defer sorted.deinit();
    try std.testing.expectEqual(@as(usize, 2), sorted.output.rows.len);
    try std.testing.expect(!backend.primary_order);
    var aggregate = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM things ORDER BY _id", .{});
    defer aggregate.deinit();
    try std.testing.expectError(error.SqlGroupingError, execute(std.testing.allocator, backend.iface(), &aggregate, &.{}, .{}));
    var renamed = try compiler.compile(std.testing.allocator, "SELECT _id AS key FROM things ORDER BY key", .{});
    defer renamed.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &renamed, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("0", result.output.rows[0][0].string);
}

test "SQL bounded top K sorts typed expressions before offset and limits retained rows" {
    var backend: TestBackend = .{ .row_count = 100 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT CAST(_id AS INTEGER) AS n FROM things ORDER BY n DESC LIMIT 3 OFFSET 2", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 7, .retained_bytes = 256 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqualStrings("97", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("96", result.output.rows[1][0].string);
    try std.testing.expectEqualStrings("95", result.output.rows[2][0].string);
    try std.testing.expectEqual(@as(usize, 15), backend.pages);
    try std.testing.expect(result.peakMemoryBytes() <= 256 * 1024);
    try std.testing.expect(!backend.primary_order);
}

test "SQL ordering handles expressions positions and explicit null placement" {
    var backend: TestBackend = .{ .row_count = 4 };
    const statements = [_][]const u8{
        "SELECT CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END AS n FROM things ORDER BY 1 DESC NULLS LAST",
        "SELECT CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END AS n FROM things ORDER BY CASE WHEN _id = '0' THEN NULL ELSE CAST(_id AS INTEGER) END DESC NULLS LAST",
    };
    for (statements) |statement| {
        var compiled = try compiler.compile(std.testing.allocator, statement, .{});
        defer compiled.deinit();
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 4), result.output.rows.len);
        try std.testing.expectEqualStrings("3", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("1", result.output.rows[2][0].string);
        try std.testing.expect(result.output.rows[3][0] == .null);
        try std.testing.expect(result.output.sql_nulls.?[3][0]);
    }
}

test "SQL parameter inference is independent of projection order" {
    var backend: TestBackend = .{};
    for ([_][]const u8{ "SELECT $1, $1 + 1", "SELECT $1 + 1, $1" }) |statement| {
        var compiled = try compiler.compile(std.testing.allocator, statement, .{});
        defer compiled.deinit();
        var description = try describe.describe(std.testing.allocator, backend.iface(), &compiled, &.{});
        defer description.deinit();
        try std.testing.expectEqual(ast.ColumnType.integer, description.binding.parameter_types[0].?);
        var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .integer = 9007199254740993 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.integer, result.output.columns[0].type);
        try std.testing.expectEqual(ast.ColumnType.integer, result.output.columns[1].type);
    }
}

test "SQL mutation success preserves post-commit recovery outcomes" {
    var backend: TestBackend = .{ .outcome = .committed_repair_required };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM things", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(catalog.MutationOutcome.committed_repair_required, result.output.mutation_outcome.?);
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL row identity uses a point seek and contradictory keys never scan" {
    var backend: TestBackend = .{ .row_count = 100_000 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT _id, id FROM things WHERE _id = $1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend.iface(), &compiled, &.{.{ .string = "99999" }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 1), backend.point_reads);
    try std.testing.expectEqualStrings("99999", result.output.rows[0][0].string);
    for ([_][]const u8{
        "DELETE FROM things WHERE _id = 'a' AND _id = 'b'",
        "DELETE FROM things WHERE _id = NULL",
        "DELETE FROM things WHERE _id IS NULL",
        "DELETE FROM things WHERE _id = ''",
        "DELETE FROM things WHERE id = NULL",
        "DELETE FROM things WHERE id <> NULL",
    }) |sql| {
        var empty = try compiler.compile(std.testing.allocator, sql, .{});
        defer empty.deinit();
        var output = try execute(std.testing.allocator, backend.iface(), &empty, &.{}, .{});
        defer output.deinit();
        try std.testing.expectEqual(@as(u64, 0), output.output.rows_affected);
    }
    try std.testing.expectEqual(@as(usize, 1), backend.pages);
    try std.testing.expectEqual(@as(usize, 0), backend.writes);
}

test "SQL update reads only preserved columns and shares immutable assignments" {
    const Fixture = struct {
        expected: []const u8,
        reads: usize = 0,
        writes: usize = 0,

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
            try std.testing.expectEqual(catalog.Action.read_write, action);
            return .{ .id = 1, .physical_name = "wide", .schema_version = 1, .columns = &.{
                .{ .name = "keep", .path = "keep", .type = .integer },
                .{ .name = "payload", .path = "payload", .type = .string },
            } };
        }
        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            try std.testing.expectEqual(@as(usize, 1), request.fields.len);
            try std.testing.expectEqualStrings("keep", request.fields[0]);
            const rows = try alloc.alloc(catalog.Row, 64);
            for (rows, 0..) |*row, index| {
                var object: std.json.ObjectMap = .empty;
                try object.put(alloc, "keep", .{ .integer = @intCast(index) });
                row.* = .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{index}), .version = 27, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, rows: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.writes += 1;
            try std.testing.expectEqual(@as(usize, 64), rows.len);
            var shared: ?[*]const u8 = null;
            for (rows, 0..) |row, index| {
                try std.testing.expectEqual(@as(u64, 27), row.expected_version);
                const object = row.row.?.object;
                try std.testing.expectEqual(@as(usize, 2), object.count());
                try std.testing.expectEqual(@as(i64, @intCast(index)), object.get("keep").?.integer);
                const payload = object.get("payload").?.string;
                try std.testing.expectEqualStrings(self.expected, payload);
                // The constant is independently owned, but not cloned 64 times.
                try std.testing.expect(payload.ptr != self.expected.ptr);
                if (shared) |first| try std.testing.expect(first == payload.ptr);
                shared = payload.ptr;
            }
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const payload = "v" ** 2048;
    var fixture: Fixture = .{ .expected = payload };
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE wide SET payload = $1", .{});
    defer compiled.deinit();
    var result = try execute(std.testing.allocator, backend, &compiled, &.{.{ .string = payload }}, .{ .retained_bytes = 256 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 64), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.reads);
    try std.testing.expectEqual(@as(usize, 1), fixture.writes);
    try std.testing.expect(result.peakMemoryBytes() < 128 * 1024);
}

test "SQL typed mutations preserve JSON null separately from SQL NULL and filter logical values" {
    const Fixture = struct {
        expected_null_fields: []const []const u8 = &.{},
        writes: usize = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "typed", .schema_version = 1, .columns = &.{
                .{ .name = "j", .path = "j", .type = .json },
                .{ .name = "k", .path = "k", .type = .json, .nullable = false },
                .{ .name = "n", .path = "n", .type = .integer },
            } };
        }
        fn scan(_: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            var result: std.ArrayList(catalog.Row) = .empty;
            for ([_][]const u8{ "a", "b" }, 0..) |id, index| {
                if (request.primary_key) |key| if (!std.mem.eql(u8, key, id)) continue;
                var object: std.json.ObjectMap = .empty;
                try object.put(alloc, "j", .null);
                try object.put(alloc, "k", .null);
                try object.put(alloc, "n", .{ .integer = 1 });
                try result.append(alloc, .{ .id = id, .version = 1, .value = .{ .object = object }, .sql_nulls = if (index == 0) &.{ false, false, false } else &.{ true, false, false } });
            }
            return .{ .rows = result.items, .after = null };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            try std.testing.expectEqual(self.expected_null_fields.len, mutations[0].json_null_fields.len);
            for (self.expected_null_fields) |expected| {
                var found = false;
                for (mutations[0].json_null_fields) |actual| if (std.mem.eql(u8, actual, expected)) {
                    found = true;
                    break;
                };
                try std.testing.expect(found);
                try std.testing.expect(mutations[0].row.?.object.get(expected).? == .null);
            }
            self.writes += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const alloc = std.testing.allocator;
    var fixture: Fixture = .{};
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    const Case = struct { sql: []const u8, fields: []const []const u8 };
    for ([_]Case{
        .{ .sql = "INSERT INTO typed (_id,j,k,n) VALUES ('c','null','null',1)", .fields = &.{ "j", "k" } },
        .{ .sql = "INSERT INTO typed (_id,j,k,n) VALUES ('c',NULL,'null',1)", .fields = &.{"k"} },
        .{ .sql = "UPDATE typed SET n=2 WHERE _id='a'", .fields = &.{ "j", "k" } },
        .{ .sql = "UPDATE typed SET j=CAST('null' AS JSON) WHERE _id='b'", .fields = &.{ "j", "k" } },
        .{ .sql = "UPDATE typed SET j=NULL WHERE _id='a'", .fields = &.{"k"} },
    }) |case| {
        fixture.expected_null_fields = case.fields;
        var compiled = try compiler.compile(alloc, case.sql, .{});
        defer compiled.deinit();
        var result = try execute(alloc, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    }
    var invalid = try compiler.compile(alloc, "INSERT INTO typed (_id,j,k,n) VALUES ('c','null',NULL,1)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, execute(alloc, backend, &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 5), fixture.writes);
    var filtered = try compiler.compile(alloc, "SELECT _id, j FROM typed WHERE j='null'", .{});
    defer filtered.deinit();
    var selected = try execute(alloc, backend, &filtered, &.{}, .{});
    defer selected.deinit();
    try std.testing.expectEqual(@as(usize, 1), selected.output.rows.len);
    try std.testing.expectEqualStrings("a", selected.output.rows[0][0].string);
    try std.testing.expect(!selected.output.sql_nulls.?[0][1]);
}
