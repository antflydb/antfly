// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable bounded scalar bytecode. Names, types, literals and operations
//! determine identity; schema epochs and physical ordinals do not.
const std = @import("std");
const schema = @import("../storage/schema.zig");
const checks = @import("relational_checks.zig");
const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
pub const Value = @import("../sql/row_value.zig").Value;
pub const Kind = schema.RelationalColumnType;
const Allocator = std.mem.Allocator;
fn scalarJson(alloc: Allocator, kind: Kind, value: std.json.Value, literal: bool) !Value {
    return Value.fromScalar(try checks.valueFromJson(alloc, kind, value, literal));
}
const Numeric = @import("../common/sql_builtin_type.zig").Type;
const casts = @import("../sql/builtin_cast.zig");
const exact = @import("../sql/numeric_value.zig");
const binary = @import("../sql/numeric_binary.zig");
const text_kernel = @import("../sql/text_value.zig");
pub const max_nodes = 128;
pub const max_depth = 16;
pub const max_output_bytes = 1024 * 1024;
pub const max_allocated_bytes = 4 * max_output_bytes;

/// One invocation can span several immutable plans. Work/cancellation remain
/// sticky across nodes and bindings, independently of retained-byte admission.
pub const PreparationRuntime = struct {
    io: std.Io,
    uuid_clock: ?*@import("../common/uuid.zig").Version7Clock = null,
};

pub const Execution = struct {
    alloc: Allocator,
    bytes: *usize,
    numeric: exact.Context,
    preparation_runtime: ?PreparationRuntime = null,

    pub fn init(alloc: Allocator, bytes: *usize) Execution {
        return .{ .alloc = alloc, .bytes = bytes, .numeric = .{ .alloc = alloc } };
    }

    pub fn charge(self: *Execution, work: u64) !void {
        self.numeric.charge(work) catch |err| return executionFailure(err);
    }

    pub fn limit(self: *Execution) anyerror {
        return executionFailure(self.numeric.limit());
    }
};

/// One immutable row shared by multiple scalar plans. Initialize at its final
/// address; plans, source payloads and Execution must outlive this owner.
/// Cache only operands requiring preparation, never a schema-width vector.
/// Results borrow this owner and must be consumed before deinit.
pub const PreparedInput = struct {
    execution: *Execution,
    source: Plan.Source,
    roots: @import("../sql/row_value.zig").JsonRow,
    nulls: ?*@import("../sql/row_value.zig").JsonNulls,
    entries: std.StringHashMapUnmanaged(Entry),
    preparations: usize,
    /// Immutable logical-predecessor ordinal -> physical historical ordinal.
    /// NULL entries denote fields absent in that retained layout.
    ordinals: ?[]const ?u32 = null,
    const Entry = struct { kind: Kind, sql_type: ?Numeric, value: Value };

    pub fn initJson(self: *PreparedInput, execution: *Execution, document: std.json.Value, nulls: ?*@import("../sql/row_value.zig").JsonNulls) !void {
        if (document != .object) return error.InvalidRelationalExpressionInput;
        if (nulls) |owner| {
            if (owner.context != &execution.numeric or owner.admission == null or owner.admission.?.remaining != execution.bytes) return error.InvalidRelationalExpressionInput;
        }
        try self.init(execution, .{ .json = document }, nulls);
    }

    pub fn initRow(self: *PreparedInput, execution: *Execution, row: codec.OrdinalRowView) !void {
        try self.init(execution, .{ .row = row }, null);
    }

    /// Caller has certified the exact compiler layout and column identity.
    pub fn initBoundRow(self: *PreparedInput, execution: *Execution, row: codec.OrdinalRowView) !void {
        try self.init(execution, .{ .bound_row = row }, null);
    }

    pub fn initMappedRow(self: *PreparedInput, execution: *Execution, row: codec.OrdinalRowView, ordinals: []const ?u32) !void {
        try self.initBoundRow(execution, row);
        self.ordinals = ordinals;
    }

    fn init(self: *PreparedInput, execution: *Execution, source: Plan.Source, nulls: ?*@import("../sql/row_value.zig").JsonNulls) !void {
        self.* = .{ .execution = execution, .source = source, .roots = undefined, .nulls = nulls, .entries = .empty, .preparations = 0 };
        self.roots.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        errdefer self.roots.deinit();
        try self.roots.bindAdmission(execution.bytes);
    }

    pub fn deinit(self: *PreparedInput) void {
        self.roots.deinit();
        self.* = undefined;
    }

    /// Read an already resolved physical ordinal from this pinned source.
    /// Rewrite mappings need no repeated name lookup or secondary cell decoder.
    pub fn valueAt(self: *PreparedInput, execution: *Execution, ordinal: u32) !Value {
        try self.requireExecution(execution);
        if (self.source != .bound_row) return error.InvalidRelationalExpressionInput;
        const columns = self.source.bound_row.table_schema.relational_columns;
        if (ordinal >= columns.len) return error.InvalidRelationalExpressionInput;
        try execution.charge(1);
        const column = columns[ordinal];
        return Plan.readColumn(execution, .{ .prepared = self }, .{
            .op = .column,
            .kind = column.column_type,
            .sql_type = column.sql_element_type,
            .column_name = column.name,
            .ordinal = ordinal,
        });
    }

    fn requireExecution(self: *const PreparedInput, execution: *Execution) !void {
        if (execution != self.execution or self.roots.context != &execution.numeric or self.roots.admission.?.remaining != execution.bytes) return error.InvalidRelationalExpressionInput;
        try execution.charge(0);
    }

    fn mappedNode(self: *const PreparedInput, logical: Node) !?Node {
        var node = logical;
        if (node.op == .column) if (self.ordinals) |mapping| {
            if (node.ordinal >= mapping.len) return error.InvalidRelationalExpressionInput;
            node.ordinal = mapping[node.ordinal] orelse return null;
        };
        return node;
    }

    fn read(self: *PreparedInput, execution: *Execution, logical: Node) !Value {
        try self.requireExecution(execution);
        const node = (try self.mappedNode(logical)) orelse return .null;
        // Ordinary scalar cells already borrow the input. Preserve that path,
        // and preserve streamed base64 comparisons in Plan.borrowedBlob.
        switch (node.kind) {
            // Cold numeric/array cells already carry their typed encoding;
            // caching those borrowed views would add allocation without reuse.
            .numeric, .sql_array => if (self.source != .json) return Plan.readColumn(execution, self.source, node),
            .blob, .json => {},
            else => return Plan.readColumn(execution, self.source, node),
        }
        try execution.charge(node.column_name.len);
        if (self.entries.get(node.column_name)) |entry| {
            if (entry.kind != node.kind or entry.sql_type != node.sql_type) return error.RelationalIndexColumnTypeMismatch;
            return entry.value;
        }
        if (self.roots.admission) |*admission| admission.begin();
        defer if (self.roots.admission) |*admission| admission.finish();
        const value: Value = if (node.kind != .json) try Plan.readColumn(execution, self.source, node) else switch (self.source) {
            .json => |document| blk: {
                const input = document.object.get(node.column_name) orelse break :blk .null;
                if (input == .null and (self.nulls == null or !try self.nulls.?.contains(node.column_name))) break :blk .null;
                break :blk .{ .json = try self.roots.pinValue(input) };
            },
            .row, .bound_row => |row| blk: {
                const ordinal = if (self.source == .bound_row) node.ordinal else row.ordinalForName(node.column_name) orelse break :blk .null;
                if (ordinal >= row.table_schema.relational_columns.len or row.table_schema.relational_columns[ordinal].column_type != .json or row.table_schema.relational_columns[ordinal].sql_element_type != node.sql_type) return error.RelationalIndexColumnTypeMismatch;
                const cell = (try row.findCell(ordinal)) orelse break :blk .null;
                if (cell.is_null) break :blk .null;
                break :blk .{ .json = try self.roots.decodePinned(std.math.cast(u32, ordinal) orelse return error.InvalidRelationalExpressionInput, cell.value.bytes_val) };
            },
            else => return error.InvalidRelationalExpressionInput,
        };
        // Preparing a numeric/array value can consume the same invocation's
        // byte counter. Refresh headroom before allocating the cache entry.
        if (self.roots.admission) |*admission| admission.begin();
        try execution.charge(node.column_name.len);
        self.entries.put(self.roots.arena.allocator(), node.column_name, .{ .kind = node.kind, .sql_type = node.sql_type, .value = value }) catch |err| {
            if (err == error.OutOfMemory and self.roots.memory.isExhausted()) return execution.limit();
            return err;
        };
        self.preparations += 1;
        return value;
    }
};

test "relational index system shared prepared input reuses numeric operands and fences execution identity" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric }} };
    var definition = try std.json.parseFromSlice(std.json.Value, a, "{\"op\":\"column\",\"column\":\"n\"}", .{});
    defer definition.deinit();
    var left = try Plan.init(a, table, definition.value, .numeric);
    defer left.deinit();
    var right = try Plan.init(a, table, definition.value, .numeric);
    defer right.deinit();
    const Run = struct {
        fn run(backing: Allocator, first: *const Plan, second: *const Plan) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const input_json = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"n\":1.200}", .{ .parse_numbers = false });
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(region.allocator(), &bytes);
            var input: PreparedInput = undefined;
            try input.initJson(&execution, input_json, null);
            defer input.deinit();
            const one = try first.evaluatePreparedWithExecution(&execution, &input);
            const two = try second.evaluatePreparedWithExecution(&execution, &input);
            try std.testing.expectEqual(@as(usize, 1), input.preparations);
            try std.testing.expectEqual(one.numeric.ptr, two.numeric.ptr);
            try std.testing.expectEqualSlices(u8, one.numeric, two.numeric);
            const before_work = execution.numeric.remaining;
            const before_bytes = bytes;
            for (0..64) |_| {
                const reused = try second.evaluatePreparedWithExecution(&execution, &input);
                try std.testing.expectEqual(one.numeric.ptr, reused.numeric.ptr);
            }
            // Cache hits charge the VM node and key hashing, but allocate no
            // output and never rerun numeric parsing/binary preparation.
            try std.testing.expectEqual(@as(u64, 64 * 2), before_work - execution.numeric.remaining);
            try std.testing.expectEqual(before_bytes, bytes);
            try std.testing.expectEqual(@as(usize, 1), input.preparations);
            try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, input.read(&execution, .{ .op = .column, .kind = .sql_array, .sql_type = .text, .column_name = "n" }));
            var other_bytes: usize = max_allocated_bytes;
            var other = Execution.init(region.allocator(), &other_bytes);
            try std.testing.expectError(error.InvalidRelationalExpressionInput, first.evaluatePreparedWithExecution(&other, &input));
            try std.testing.expect(execution.limit() == error.RelationalExpressionBudgetExceeded);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, second.evaluatePreparedWithExecution(&execution, &input));
        }
    };
    try Run.run(a, &left, &right);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ &left, &right });
}

test "relational index system shared prepared input pins JSON null authority without reparsing" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const document = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"j\":null,\"s\":null,\"payload\":{\"n\":1.20}}", .{ .parse_numbers = false });
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(region.allocator(), &bytes);
    var nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
    nulls.initRegion(region.allocator(), bytes, &execution.numeric);
    defer nulls.deinit();
    try nulls.bindAdmission(&bytes);
    // These names represent already authenticated typed provenance.
    try nulls.import(&.{"j"});
    var input: PreparedInput = undefined;
    try input.initJson(&execution, document, &nulls);
    defer input.deinit();
    const json = try input.read(&execution, .{ .op = .column, .kind = .json, .column_name = "j" });
    try std.testing.expectEqual(std.json.Value.null, json.json.value);
    try std.testing.expectEqual(Value.null, try input.read(&execution, .{ .op = .column, .kind = .json, .column_name = "s" }));
    const payload = try input.read(&execution, .{ .op = .column, .kind = .json, .column_name = "payload" });
    const again = try input.read(&execution, .{ .op = .column, .kind = .json, .column_name = "payload" });
    try std.testing.expectEqual(payload.json, again.json);
    try std.testing.expectEqualStrings("1.20", payload.json.value.object.get("n").?.number_string);
    try std.testing.expectEqual(@as(usize, 0), input.roots.decodes);
    try std.testing.expectEqual(@as(usize, 3), input.preparations);
}

test "relational index system typed CHECK transport agrees with cold JSON null provenance" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "s", .path = "s", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var physical = try codec.PhysicalLayout.init(a, table);
    defer physical.deinit();
    const encoded = try codec.serializePreparedOrdinalDeferredHash(a, 1, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "null" } },
        .{ .ordinal = 1, .path = "s", .is_json = true, .is_null = true, .value_type = .bytes_val, .value = .{ .bytes_val = "" } },
    }, &physical);
    defer a.free(encoded);
    try codec.finalizeOrdinalMetadata(encoded, @splat(0), 0);
    const cold = try codec.ordinalRowView(encoded, table, &physical);
    const Run = struct {
        fn run(backing: Allocator, row: codec.OrdinalRowView) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const owned = region.allocator();
            const document = try std.json.parseFromSliceLeaky(std.json.Value, owned, "{\"j\":null,\"s\":null}", .{});
            // Internal VM fixture only: public JSON compilation remains
            // guarded until capability, literal identity and lowering activate.
            var plans = [_]Plan{
                .{ .arena = .init(backing), .nodes = &.{ .{ .op = .column, .kind = .json, .column_name = "j" }, .{ .op = .is_not_null, .kind = .boolean, .children = &.{0} } }, .dependencies = &.{0}, .fingerprint = @splat(0), .result_kind = .boolean, .literal_bytes = 0 },
                .{ .arena = .init(backing), .nodes = &.{ .{ .op = .column, .kind = .json, .column_name = "s", .ordinal = 1 }, .{ .op = .is_null, .kind = .boolean, .children = &.{0} } }, .dependencies = &.{1}, .fingerprint = @splat(0), .result_kind = .boolean, .literal_bytes = 0 },
            };
            defer for (&plans) |*plan| plan.deinit();
            // Borrow the pinned physical layout and plans; Set.deinit would
            // incorrectly free these independently owned fixture resources.
            const set: checks.Set = .{ .alloc = backing, .table = row.table_schema, .layout = row.layout.*, .definitions = &.{}, .plans = &plans };
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(owned, &bytes);
            var nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
            nulls.initRegion(owned, bytes, &execution.numeric);
            defer nulls.deinit();
            try nulls.bindAdmission(&bytes);
            try nulls.import(&.{"j"});
            try std.testing.expectEqual(@as(?usize, null), try set.firstViolationTypedJsonWithExecution(&execution, document, &nulls));
            try std.testing.expectEqual(@as(?checks.Set.Failure, null), try set.firstFailureTypedJsonWithExecution(&execution, document, &nulls));
            try std.testing.expectEqual(@as(?usize, null), try set.firstViolationRow(backing, row));
            try std.testing.expectEqual(@as(?usize, 0), try set.firstViolationJsonWithExecution(&execution, document));
            try nulls.import(&.{"s"});
            try std.testing.expectEqual(@as(?usize, 1), try set.firstViolationTypedJsonWithExecution(&execution, document, &nulls));
            const failure = (try set.firstFailureTypedJsonWithExecution(&execution, document, &nulls)).?;
            try std.testing.expectEqual(@as(usize, 1), failure.index);
            var other_bytes: usize = max_allocated_bytes;
            var other = Execution.init(owned, &other_bytes);
            try std.testing.expectError(error.InvalidRelationalExpressionInput, set.firstViolationTypedJsonWithExecution(&other, document, &nulls));
            var foreign: @import("../sql/row_value.zig").JsonNulls = undefined;
            foreign.initRegion(owned, bytes, &execution.numeric);
            defer foreign.deinit();
            try foreign.bindAdmission(&other_bytes);
            try std.testing.expectError(error.InvalidRelationalExpressionInput, set.firstViolationTypedJsonWithExecution(&execution, document, &foreign));
            var limited_bytes: usize = 0;
            var limited = Execution.init(owned, &limited_bytes);
            var limited_nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
            limited_nulls.initRegion(owned, 0, &limited.numeric);
            defer limited_nulls.deinit();
            try limited_nulls.bindAdmission(&limited_bytes);
            // Activation cannot persist a shared budget failure as a bad row.
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.firstFailureTypedJsonWithExecution(&limited, document, &limited_nulls));
            limited_bytes = max_allocated_bytes;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.firstViolationTypedJsonWithExecution(&limited, document, &limited_nulls));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            execution.numeric.checkpoint = Cancel.poll;
            execution.numeric.since_poll = 256;
            try std.testing.expectError(error.Canceled, set.firstFailureTypedJsonWithExecution(&execution, document, &nulls));
            execution.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, set.firstViolationTypedJsonWithExecution(&execution, document, &nulls));
        }
    };
    try Run.run(a, cold);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{cold});
}

test "relational index system shared prepared input decodes cold JSON once and keeps cancellation sticky" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_jsonb_display = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const bytes = try codec.serializePreparedOrdinalDeferredHash(a, 1, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "{\"n\":1.20}" } },
    }, &layout);
    defer a.free(bytes);
    try codec.finalizeOrdinalMetadata(bytes, @splat(0), 0);
    const row = try codec.ordinalRowView(bytes, table, &layout);
    const Run = struct {
        fn run(backing: Allocator, source: codec.OrdinalRowView) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            var remaining: usize = max_allocated_bytes;
            var execution = Execution.init(region.allocator(), &remaining);
            var input: PreparedInput = undefined;
            try input.initRow(&execution, source);
            defer input.deinit();
            const node: Node = .{ .op = .column, .kind = .json, .column_name = "j", .ordinal = 73 };
            const one = try input.read(&execution, node);
            const two = try input.read(&execution, node);
            try std.testing.expectEqual(one.json, two.json);
            try std.testing.expectEqualStrings("1.20", one.json.value.object.get("n").?.number_string);
            try std.testing.expectEqual(@as(usize, 1), input.roots.decodes);
            try std.testing.expectEqual(@as(usize, 1), input.preparations);
            var bound: PreparedInput = undefined;
            try bound.initBoundRow(&execution, source);
            defer bound.deinit();
            const resolved = try bound.valueAt(&execution, 0);
            const before = execution.numeric.remaining;
            const retained = remaining;
            try std.testing.expectEqual(resolved.json, (try bound.valueAt(&execution, 0)).json);
            try std.testing.expectEqual(@as(u64, 2), before - execution.numeric.remaining);
            try std.testing.expectEqual(retained, remaining);
            try std.testing.expectEqual(@as(usize, 1), bound.roots.decodes);
            try std.testing.expectError(error.InvalidRelationalExpressionInput, bound.valueAt(&execution, 1));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            execution.numeric.checkpoint = Cancel.poll;
            // Arm the next bounded cancellation poll; installing a callback
            // does not reset the invocation's existing sampling interval.
            execution.numeric.since_poll = 256;
            try std.testing.expectError(error.Canceled, input.read(&execution, node));
            execution.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, input.read(&execution, node));
        }
    };
    try Run.run(a, row);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{row});
}

/// Temporarily own bounded unpublished results while retaining the caller's
/// work/cancellation identity. Reconcile capacity against existing VM charges
/// rather than charging the same retained allocation twice.
pub const ExecutionScratch = struct {
    execution: *Execution,
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    saved_alloc: Allocator,
    saved_numeric_alloc: Allocator,
    initial_bytes: usize,

    pub fn init(self: *ExecutionScratch, execution: *Execution) void {
        self.* = .{
            .execution = execution,
            .memory = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true },
            .arena = undefined,
            .saved_alloc = execution.alloc,
            .saved_numeric_alloc = execution.numeric.alloc,
            .initial_bytes = execution.bytes.*,
        };
        self.arena = .init(self.memory.allocator());
        execution.alloc = self.arena.allocator();
        execution.numeric.alloc = execution.alloc;
    }

    pub fn deinit(self: *ExecutionScratch) void {
        self.arena.deinit();
        const charged = self.initial_bytes -| self.execution.bytes.*;
        self.execution.bytes.* = self.initial_bytes -| @max(charged, self.memory.footprint());
        self.execution.alloc = self.saved_alloc;
        self.execution.numeric.alloc = self.saved_numeric_alloc;
    }

    pub fn failure(self: *ExecutionScratch, err: anyerror) anyerror {
        // Borrowed comparisons can exhaust logical byte admission without
        // allocating. Keep their failure as sticky as allocator admission.
        if (err == error.RelationalExpressionBudgetExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) return self.execution.limit();
        return executionFailure(err);
    }
};

// Preserve the durable validation/transport contract. Numeric kernel errors
// must not turn deterministic bad rows into retryable activation failures.
fn executionFailure(err: anyerror) anyerror {
    return switch (err) {
        error.SqlDivisionByZero => error.RelationalExpressionDivisionByZero,
        error.InvalidSqlNumber, error.SqlNumericOutOfRange => error.RelationalExpressionOverflow,
        error.SqlInvalidTextRepresentation => error.InvalidRelationalExpressionInput,
        error.SqlProgramLimitExceeded => error.RelationalExpressionBudgetExceeded,
        else => err,
    };
}

/// Numeric scratch is an unpublished bounded region. Charge arena capacity,
/// not only result bytes: a caller arena may not reclaim out-of-order frees.
const NumericScratch = struct {
    execution: *Execution,
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator = undefined,
    saved_alloc: Allocator,
    saved_output: usize,
    saved_groups: usize,

    fn init(self: *NumericScratch, execution: *Execution) void {
        self.* = .{
            .execution = execution,
            .memory = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true },
            .saved_alloc = execution.numeric.alloc,
            .saved_output = execution.numeric.max_output_bytes,
            .saved_groups = execution.numeric.max_groups,
        };
        self.arena = .init(self.memory.allocator());
        execution.numeric.alloc = self.arena.allocator();
        execution.numeric.max_output_bytes = @min(self.saved_output, @min(max_output_bytes, execution.bytes.*));
        execution.numeric.max_groups = @min(self.saved_groups, execution.bytes.* / 2);
    }

    fn deinit(self: *NumericScratch) void {
        self.arena.deinit();
        self.execution.bytes.* -|= self.memory.footprint();
        self.execution.numeric.alloc = self.saved_alloc;
        self.execution.numeric.max_output_bytes = self.saved_output;
        self.execution.numeric.max_groups = self.saved_groups;
    }

    fn encode(self: *NumericScratch, value: exact.Value) !Value {
        const ctx = &self.execution.numeric;
        const size = try binary.encodedSize(ctx, value);
        if (size > self.execution.bytes.* -| self.memory.footprint()) return error.RelationalExpressionBudgetExceeded;
        const output = try allocateOutput(self.execution.alloc, size, self.execution.bytes);
        errdefer self.execution.alloc.free(output);
        var writer: std.Io.Writer = .fixed(output);
        try binary.encode(ctx, value, &writer);
        return .{ .numeric = output };
    }

    fn json(self: *NumericScratch, value: exact.Value) !std.json.Value {
        const text = try exact.format(&self.execution.numeric, value);
        if (text.len > self.execution.bytes.* -| self.memory.footprint()) return error.RelationalExpressionBudgetExceeded;
        const output = try allocateOutput(self.execution.alloc, text.len, self.execution.bytes);
        @memcpy(output, text);
        return if (value.kind == .finite) .{ .number_string = output } else .{ .string = output };
    }

    fn failure(self: *NumericScratch, err: anyerror) anyerror {
        if (err == error.RelationalExpressionBudgetExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) return self.execution.limit();
        return executionFailure(err);
    }
};

pub fn numericJson(execution: *Execution, input: std.json.Value) !Value {
    try execution.charge(0);
    if (input == .null) return .null;
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = @import("../sql/numeric_storage.zig").fromJson(&execution.numeric, input) catch |err| return scratch.failure(err);
    return scratch.encode(parsed.value) catch |err| return scratch.failure(err);
}

/// Bind a parsed, explicitly typed array envelope without a JSON round trip.
/// Scratch is unpublished; only canonical owned bytes escape to the row owner.
/// This adapter does not admit array expressions or ordered array keys by itself.
pub fn arrayJson(
    execution: *Execution,
    kind: @import("../sql/array_value.zig").ElementType,
    input: std.json.Value,
    modifier: ?exact.TypeModifier,
) !Value {
    try execution.charge(0);
    if (input == .null) return .null;
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const wire = @import("../sql/array_wire.zig");
    const storage = @import("../sql/array_storage.zig");
    const limits: @import("../sql/array_value.zig").Limits = .{ .bytes = execution.bytes.* };
    var decoded = wire.decodeBorrowedWithModifier(scratch.arena.allocator(), kind, input, modifier, .{
        .context = &execution.numeric,
        .values = limits,
        .wire_bytes = execution.numeric.max_output_bytes,
    }) catch |err| return scratch.failure(err);
    defer decoded.deinit();
    var prepared = storage.Prepared.init(scratch.arena.allocator(), decoded.value, .{
        .context = &execution.numeric,
        .values = limits,
        .wire_bytes = execution.numeric.max_output_bytes,
    }) catch |err| return scratch.failure(err);
    defer prepared.deinit();
    if (prepared.encoded_size > execution.bytes.* -| scratch.memory.footprint()) return execution.limit();
    const output = allocateOutput(execution.alloc, prepared.encoded_size, execution.bytes) catch |err| return scratch.failure(err);
    errdefer execution.alloc.free(output);
    prepared.writeInto(output) catch |err| return scratch.failure(err);
    return .{ .sql_array = .{ .element_type = kind, .bytes = output } };
}

/// Materialize the public ordinal envelope directly, without stringify/parse.
/// Like other row DOM adapters this is region-owned: the caller discards its
/// row region on failure. No managed-array allocator may retain a stack budget.
fn arrayJsonOutputLeaky(execution: *Execution, array: @import("../sql/row_value.zig").Array) !std.json.Value {
    try execution.charge(0);
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const limits: @import("../sql/array_value.zig").Limits = .{ .bytes = execution.bytes.* };
    const decoded = @import("../sql/array_storage.zig").decodeLeaky(scratch.arena.allocator(), array.element_type, array.bytes, .{
        .context = &execution.numeric,
        .values = limits,
        .wire_bytes = execution.numeric.max_output_bytes,
    }) catch |err| return scratch.failure(err);
    var retained: @import("../sql/memory_budget.zig") = .{
        .backing = execution.alloc,
        .limit = execution.bytes.* -| scratch.memory.footprint(),
        .monotonic = true,
    };
    defer execution.bytes.* -|= retained.footprint();
    var output = @import("../sql/array_wire.zig").toJsonLeaky(retained.allocator(), decoded.value, .{
        .context = &execution.numeric,
        .values = limits,
        .wire_bytes = execution.numeric.max_output_bytes,
    }) catch |err| return if (err == error.OutOfMemory and retained.isExhausted()) execution.limit() else scratch.failure(err);
    var work: @import("../sql/array_value.zig").Budget = .{ .shared = &execution.numeric };
    @import("../sql/json_order.zig").rehomeArrayAllocators(&output, execution.alloc, &work, 0) catch |err| return scratch.failure(err);
    return output;
}

/// Assignment and logical restore share exact parsing, work and cancellation.
/// Preservation validates the target domain without repairing logical values;
/// the physical row codec separately enforces canonical bytes.
pub fn normalizeNumericJson(execution: *Execution, input: std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !std.json.Value {
    execution.numeric.charge(0) catch |err| return executionFailure(err);
    if (input == .null) return input;
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    return normalizeNumericJsonInner(&scratch, input, modifier, preserve) catch |err| return scratch.failure(err);
}

fn normalizeNumericJsonInner(scratch: *NumericScratch, input: std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !std.json.Value {
    const ctx = &scratch.execution.numeric;
    const parsed = try @import("../sql/numeric_storage.zig").fromJson(ctx, input);
    const constrained = if (modifier) |target| try exact.applyTypeModifier(ctx, parsed.value, target) else parsed;
    if (preserve) {
        if (modifier != null and try exact.order(ctx, parsed.value, constrained.value) != .eq) return error.InvalidRelationalGeneratedValue;
        return input;
    }
    return scratch.json(constrained.value);
}

/// NUMERIC array cells borrow one reusable scratch arena and one sticky work
/// budget. No cell gets a fresh quota. Publish replacement values only after
/// complete admission, preserving dimensions, SQL NULLs and signed bounds.
pub fn normalizeNumericArrayJson(execution: *Execution, input: *std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !void {
    execution.numeric.charge(0) catch |err| return executionFailure(err);
    const wire = @import("../sql/array_wire.zig");
    // Borrowed input has its existing wire/domain bounds; only actual scratch
    // and owned output consume the preparation allocation allowance.
    const inspected = wire.inspectNumericEnvelope(input.*, .{ .context = &execution.numeric, .values = .{
        .work = @intCast(@min(execution.numeric.remaining, std.math.maxInt(usize))),
    } }) catch |err| {
        return if (err == error.SqlProgramLimitExceeded) executionFailure(execution.numeric.limit()) else err;
    };
    const alloc = execution.alloc;
    const replacement: ?[]std.json.Value = if (preserve) null else blk: {
        const size = std.math.mul(usize, inspected.values.len, @sizeOf(std.json.Value)) catch return executionFailure(execution.numeric.limit());
        if (size > execution.bytes.*) return executionFailure(execution.numeric.limit());
        const cells = try alloc.alloc(std.json.Value, inspected.values.len);
        execution.bytes.* -= size;
        break :blk cells;
    };
    var completed: usize = 0;
    errdefer if (replacement) |cells| {
        for (cells[0..completed]) |cell| if (cell == .string) alloc.free(cell.string);
        alloc.free(cells);
    };
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    for (inspected.values, inspected.nulls, 0..) |raw, flag, index| {
        execution.numeric.charge(1) catch |err| return scratch.failure(err);
        if (flag.bool) {
            if (replacement) |cells| cells[index] = .null;
        } else {
            _ = scratch.arena.reset(.retain_capacity);
            // Output admission reduces the capacity available to later cells.
            scratch.memory.limit = execution.bytes.*;
            const value = normalizeNumericJsonInner(&scratch, raw, modifier, preserve) catch |err| return scratch.failure(err);
            if (replacement) |cells| cells[index] = .{ .string = if (value == .number_string) value.number_string else value.string };
        }
        completed += 1;
    }
    if (replacement) |cells| input.object.getPtr("values").?.* = .{ .array = .fromOwnedSlice(alloc, cells) };
}

/// Already constrained canonical values are reused without decoding limbs or
/// allocating output. Logical restore may preserve equivalent display scales,
/// but never a value changed by assignment; physical restore is stricter.
fn normalizeNumericBinding(execution: *Execution, bytes: []const u8, modifier: exact.TypeModifier, preserve: bool) !Value {
    const view = binary.layout.View.openWithBudget(bytes, .{ .bytes = execution.numeric.max_input_bytes, .groups = execution.numeric.max_groups }, &execution.numeric) catch |err| return executionFailure(err);
    if (view.verifyModifier(modifier, &execution.numeric)) |_| {
        return .{ .numeric = bytes };
    } else |err| if (err != error.InvalidSqlBinaryRepresentation) return executionFailure(err);
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = binary.decodeCanonical(&execution.numeric, bytes) catch |err| return scratch.failure(err);
    const constrained = exact.applyTypeModifier(&execution.numeric, parsed.value, modifier) catch |err| return scratch.failure(err);
    if (preserve) {
        if ((exact.order(&execution.numeric, parsed.value, constrained.value) catch |err| return scratch.failure(err)) != .eq)
            return error.InvalidRelationalGeneratedValue;
        return .{ .numeric = bytes };
    }
    return scratch.encode(constrained.value) catch |err| return scratch.failure(err);
}

/// Canonical assignment streams one coefficient at a time. Already constrained
/// arrays are borrowed without allocation; coercion never builds a flat cell
/// vector or serializes JSON. Preserve mode verifies logical values, not scale.
fn normalizeNumericArrayBinding(execution: *Execution, input: Value, modifier: exact.TypeModifier, preserve: bool) !Value {
    try execution.charge(0);
    if (input == .null) return input;
    if (input != .sql_array or input.sql_array.element_type != .numeric) return error.InvalidRelationalExpressionInput;
    try modifier.validate();
    const storage = @import("../sql/array_storage.zig");
    const view = storage.validateCanonical(execution.alloc, .numeric, input.sql_array.bytes, .{
        .context = &execution.numeric,
        .wire_bytes = max_output_bytes,
    }) catch |err| return executionFailure(err);
    const constrained = for (0..view.count) |i| {
        const cell = try view.cell(i);
        if (cell.sql_null) continue;
        const number = try binary.layout.View.openAuthenticated(cell.bytes, .{});
        if (number.verifyModifier(modifier, &execution.numeric)) |_| {} else |err| {
            if (err != error.InvalidSqlBinaryRepresentation) return executionFailure(err);
            break false;
        }
    } else true;
    if (constrained) return input;

    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    var retained: @import("../sql/memory_budget.zig") = .{
        .backing = execution.alloc,
        .limit = execution.bytes.*,
        .monotonic = true,
    };
    defer execution.bytes.* -|= retained.footprint();
    const owner = retained.allocator();
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(owner);
    if (!preserve) {
        output.resize(owner, view.payload_start) catch |err| return if (retained.isExhausted()) execution.limit() else scratch.failure(err);
        var copied: usize = 0;
        while (copied < view.payload_start) {
            const end = copied + @min(view.payload_start - copied, 256);
            execution.charge(end - copied) catch |err| return scratch.failure(err);
            @memcpy(output.items[copied..end], input.sql_array.bytes[copied..end]);
            copied = end;
        }
    }
    for (0..view.count) |i| {
        const remaining = execution.bytes.* -| retained.footprint();
        if (scratch.memory.footprint() > remaining) return execution.limit();
        scratch.memory.limit = remaining;
        if (!scratch.arena.reset(.retain_capacity)) return scratch.failure(error.OutOfMemory);
        const cell = try view.cell(i);
        if (!preserve) std.mem.writeInt(u32, output.items[view.slots_start + i * 4 ..][0..4], @intCast(output.items.len - view.payload_start), .little);
        if (cell.sql_null) continue;
        var parsed = binary.decodeCanonical(&execution.numeric, cell.bytes) catch |err| return scratch.failure(err);
        defer parsed.deinit();
        var rounded = exact.applyTypeModifier(&execution.numeric, parsed.value, modifier) catch |err| return scratch.failure(err);
        defer rounded.deinit();
        if (preserve) {
            if ((exact.order(&execution.numeric, parsed.value, rounded.value) catch |err| return scratch.failure(err)) != .eq)
                return error.InvalidRelationalGeneratedValue;
            continue;
        }
        const length = binary.encodedSize(&execution.numeric, rounded.value) catch |err| return scratch.failure(err);
        if (length > max_output_bytes -| output.items.len) return execution.limit();
        retained.limit = execution.bytes.* -| scratch.memory.footprint();
        if (retained.footprint() > retained.limit) return execution.limit();
        const start = output.items.len;
        output.resize(owner, start + length) catch |err| return if (retained.isExhausted()) execution.limit() else scratch.failure(err);
        var writer: std.Io.Writer = .fixed(output.items[start..]);
        binary.encode(&execution.numeric, rounded.value, &writer) catch |err| return scratch.failure(err);
    }
    if (preserve) return input;
    std.mem.writeInt(u32, output.items[view.slots_start + @as(usize, view.count) * 4 ..][0..4], @intCast(output.items.len - view.payload_start), .little);
    retained.limit = execution.bytes.* -| scratch.memory.footprint();
    const bytes = output.toOwnedSlice(owner) catch |err| return if (retained.isExhausted()) execution.limit() else scratch.failure(err);
    return .{ .sql_array = .{ .element_type = .numeric, .bytes = bytes } };
}

fn numericJsonOutput(execution: *Execution, bytes: []const u8) !std.json.Value {
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = binary.decodeCanonical(&execution.numeric, bytes) catch |err| return scratch.failure(err);
    return scratch.json(parsed.value) catch |err| return scratch.failure(err);
}

pub const Op = enum { literal, column, add, subtract, multiply, divide, modulo, negate, concat, coalesce, lower_ascii, upper_ascii, eq, ne, gt, gte, lt, lte, is_null, is_not_null, is_distinct, is_not_distinct, @"and", @"or", not, cast, case_when, in_list, not_in_list, array, replace, md5, abs, ceil, floor, round, trunc, sign, sqrt, jsonb_typeof, jsonb_array_length, jsonb_extract_path, jsonb_extract_path_text, uuidv4, uuidv7 };

fn jsonFunction(op: Op) bool {
    return switch (op) {
        .jsonb_typeof, .jsonb_array_length, .jsonb_extract_path, .jsonb_extract_path_text => true,
        else => false,
    };
}

fn numericFunction(op: Op) ?@import("../sql/numeric_functions.zig").Function {
    return switch (op) {
        inline .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt => |tag| @field(@import("../sql/numeric_functions.zig").Function, @tagName(tag)),
        else => null,
    };
}

/// Shared structural grammar for public prechecks and typed compilation.
/// These rules grant no column/type authority; the pinned compiler owns that.
pub fn acceptsField(op: Op, name: []const u8) bool {
    if (std.mem.eql(u8, name, "op")) return true;
    return switch (op) {
        .uuidv4 => false,
        .uuidv7 => std.mem.eql(u8, name, "shift"),
        .literal => std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "value") or std.mem.eql(u8, name, "sql_type") or std.mem.eql(u8, name, "json_null"),
        .column => std.mem.eql(u8, name, "column"),
        .cast => std.mem.eql(u8, name, "args") or std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "sql_type") or std.mem.eql(u8, name, "numeric_modifier"),
        .array => std.mem.eql(u8, name, "args") or std.mem.eql(u8, name, "sql_type"),
        .add, .subtract, .multiply, .divide, .modulo, .negate, .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt => std.mem.eql(u8, name, "args") or std.mem.eql(u8, name, "sql_type"),
        else => std.mem.eql(u8, name, "args") or ((isComparison(op) or op == .in_list or op == .not_in_list) and std.mem.eql(u8, name, "collation")),
    };
}

pub fn acceptsArity(op: Op, count: usize) bool {
    return switch (op) {
        .literal, .column => false,
        .uuidv4, .uuidv7 => count == 0,
        .array => count <= 32,
        .jsonb_typeof, .jsonb_array_length => count == 1,
        .jsonb_extract_path, .jsonb_extract_path_text => count >= 2 and count <= 32,
        .negate, .lower_ascii, .upper_ascii, .not, .is_null, .is_not_null, .cast, .md5, .abs, .ceil, .floor, .sign, .sqrt => count == 1,
        .round, .trunc => count == 1 or count == 2,
        .replace => count == 3,
        .concat, .coalesce, .@"and", .@"or" => count >= 2 and count <= 32,
        .in_list, .not_in_list => count >= 2 and count <= max_nodes,
        .case_when => count >= 3 and count <= 31 and count % 2 == 1,
        else => count == 2,
    };
}
const Node = struct {
    op: Op,
    kind: Kind,
    children: []const u16 = &.{},
    ordinal: u32 = 0,
    column_name: []const u8 = "",
    fold_ascii: bool = false,
    literal: Value = .null,
    sql_type: ?Numeric = null,
    numeric_modifier: ?exact.TypeModifier = null,
    replace_pattern: ?text_kernel.Pattern = null,
    uuid_shift: ?@import("../sql/temporal_value.zig").Interval = null,
};

test "SQL UUID defaults require owned preparation preserve explicit values and never run during restore" {
    const a = std.testing.allocator;
    const sql = @import("../sql/compiler.zig");
    var compiled = try sql.compile(a, "CREATE TABLE events (id UUID NOT NULL DEFAULT uuidv7(INTERVAL '1 month'))", .{});
    defer compiled.deinit();
    const bytes = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, compiled.statement.create_table);
    defer a.free(bytes);
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, bytes);
    defer validator.deinit(a);
    const table = validator.execution.expressions.?.table;
    try std.testing.expect(table.requires_uuid_defaults);
    try std.testing.expectEqual(@as(u32, 33), schema.requiredReaderVersion(table));
    const encoded = try schema.serializeSchema(a, table);
    defer a.free(encoded);
    const restored = try schema.deserializeSchema(a, encoded);
    defer schema.freeSchema(a, restored);
    try std.testing.expect(restored.requires_uuid_defaults);
    try std.testing.expect(!(@import("../common/schema_readers.zig").ReaderCapabilities{ .semantic_ceiling = 32, .encoding_ceiling = 32 }).supports(schema.readerRequirementsForSchema(table)));
    const Input = struct {
        calls: usize = 0,
        fn random(raw: ?*anyopaque, output: []u8) std.Io.RandomSecureError!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            @memset(output, 0x5a);
        }
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = 1_706_704_496_123_456_789 };
        }
    };
    var input: Input = .{};
    var vtable = std.testing.io.vtable.*;
    vtable.randomSecure = Input.random;
    vtable.now = Input.now;
    const io: std.Io = .{ .userdata = &input, .vtable = &vtable };
    var clock: @import("../common/uuid.zig").Version7Clock = .{};
    const preparation = validator.forPreparation(.{ .io = io, .uuid_clock = &clock });
    var document = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer document.deinit();
    try std.testing.expectError(error.SqlEntropyUnavailable, validator.prepareValue(document.arena.allocator(), a, &document.value));
    try std.testing.expectEqual(@as(usize, 0), input.calls);
    try preparation.prepareValue(document.arena.allocator(), a, &document.value);
    try std.testing.expectEqual(@as(usize, 1), input.calls);
    const uuid = @import("../common/uuid.zig");
    try std.testing.expectEqual(@as(?u4, 7), uuid.version(try uuid.parse(document.value.object.get("id").?.string)));
    const saved = try a.dupe(u8, document.value.object.get("id").?.string);
    defer a.free(saved);
    try preparation.prepareValue(document.arena.allocator(), a, &document.value);
    try validator.validateValue(a, &document.value);
    try std.testing.expectEqual(@as(usize, 1), input.calls);
    try std.testing.expectEqualStrings(saved, document.value.object.get("id").?.string);
    // Rebuilding the immutable registry compiles default operations but must
    // not require or invent an entropy source for historical validation.
    var reopened = try @import("mod.zig").CompiledTableValidator.init(a, bytes);
    defer reopened.deinit(a);
    try reopened.validateValue(a, &document.value);
    try std.testing.expectEqual(@as(usize, 1), input.calls);
    var node = try std.json.parseFromSlice(std.json.Value, a, "{\"op\":\"uuidv7\"}", .{});
    defer node.deinit();
    try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, table, node.value, .string));
    for ([_]struct { expression: []const u8, version: ?u4 }{
        .{ .expression = "uuidv4()", .version = 4 },
        .{ .expression = "gen_random_uuid()", .version = 4 },
        .{ .expression = "COALESCE(NULL::uuid, uuidv7())", .version = 7 },
        .{ .expression = "uuidv7(NULL::interval)", .version = null },
        .{ .expression = "CASE WHEN false THEN uuidv7() ELSE NULL::uuid END", .version = null },
    }) |case| {
        const statement = try std.fmt.allocPrint(a, "CREATE TABLE variants (id UUID DEFAULT {s})", .{case.expression});
        defer a.free(statement);
        var declaration = try sql.compile(a, statement, .{});
        defer declaration.deinit();
        const definition = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, declaration.statement.create_table);
        defer a.free(definition);
        var owner = try @import("mod.zig").CompiledTableValidator.init(a, definition);
        defer owner.deinit(a);
        var value = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
        defer value.deinit();
        const calls = input.calls;
        try owner.forPreparation(.{ .io = io, .uuid_clock = &clock }).prepareValue(value.arena.allocator(), a, &value.value);
        const actual = value.value.object.get("id").?;
        if (case.version) |expected| {
            try std.testing.expectEqual(@as(?u4, expected), uuid.version(try uuid.parse(actual.string)));
            try std.testing.expectEqual(calls + 1, input.calls);
        } else {
            try std.testing.expect(actual == .null);
            try std.testing.expectEqual(calls, input.calls);
        }
        try owner.validateValue(a, &value.value);
    }
    var infinite = try sql.compile(a, "CREATE TABLE invalid_shift (id UUID DEFAULT uuidv7(INTERVAL 'infinity'))", .{});
    defer infinite.deinit();
    const infinite_schema = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, infinite.statement.create_table);
    defer a.free(infinite_schema);
    var infinite_owner = try @import("mod.zig").CompiledTableValidator.init(a, infinite_schema);
    defer infinite_owner.deinit(a);
    var infinite_row = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer infinite_row.deinit();
    const calls = input.calls;
    try std.testing.expectError(error.TemporalOutOfRange, infinite_owner.prepareValue(infinite_row.arena.allocator(), a, &infinite_row.value));
    try std.testing.expectEqual(calls, input.calls);
}

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []const Node,
    dependencies: []const u32,
    fingerprint: [32]u8,
    result_kind: Kind,
    result_sql_type: ?Numeric = null,
    literal_bytes: usize,
    /// Compile-time source shape: bit 0 prepares ingress operands, bit 1 cold
    /// operands. Hand-built VM fixtures derive this once at their boundary.
    prepared_sources: u8 = 0xff,

    pub fn init(alloc: Allocator, table: schema.TableSchema, expression: std.json.Value, expected: Kind) !Plan {
        return initPolicy(alloc, table, expression, expected, false);
    }

    pub fn initDefault(alloc: Allocator, table: schema.TableSchema, expression: std.json.Value, expected: Kind) !Plan {
        return initPolicy(alloc, table, expression, expected, true);
    }

    fn initPolicy(alloc: Allocator, table: schema.TableSchema, expression: std.json.Value, expected: Kind, defaults: bool) !Plan {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var bytes: usize = max_allocated_bytes;
        var execution = Execution.init(arena.allocator(), &bytes);
        var compiler: Compiler = .{ .alloc = arena.allocator(), .table = table, .execution = &execution, .allow_uuid_defaults = defaults };
        compiler.hash.update("antfly immutable scalar expression v1");
        const root = try compiler.compile(expression, 0);
        if (compiler.nodes.items[root].kind != expected) return error.InvalidRelationalExpressionType;
        var fingerprint: [32]u8 = undefined;
        compiler.hash.final(&fingerprint);
        return .{ .arena = arena, .nodes = compiler.nodes.items, .dependencies = compiler.dependencies.items, .fingerprint = fingerprint, .result_kind = expected, .result_sql_type = compiler.nodes.items[root].sql_type, .literal_bytes = compiler.literal_bytes, .prepared_sources = preparedSourceMask(compiler.nodes.items) };
    }

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Results borrow input values or allocate from the caller's row arena.
    /// Coalesce is lazy, so an unselected expression cannot raise an error.
    pub fn evaluate(self: *const Plan, alloc: Allocator, values: []const Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateWithBudget(alloc, values, &budget);
    }

    pub fn evaluateWithBudget(self: *const Plan, alloc: Allocator, values: []const Value, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateWithExecution(&execution, values);
    }

    pub fn evaluateWithExecution(self: *const Plan, execution: *Execution, values: []const Value) !Value {
        return self.evaluateNode(execution, .{ .values = values }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    pub fn evaluateJson(self: *const Plan, alloc: Allocator, document: std.json.Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateJsonWithBudget(alloc, document, &budget);
    }

    pub fn evaluateJsonWithBudget(self: *const Plan, alloc: Allocator, document: std.json.Value, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateJsonWithExecution(&execution, document);
    }

    pub fn evaluateJsonWithExecution(self: *const Plan, execution: *Execution, document: std.json.Value) !Value {
        if (document != .object) return error.InvalidRelationalExpressionInput;
        return self.evaluateRetainedSource(execution, .{ .json = document });
    }

    pub fn evaluateRow(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateRowWithBudget(alloc, row, &budget);
    }

    pub fn evaluateRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateRowWithExecution(&execution, row);
    }

    pub fn evaluateRowWithExecution(self: *const Plan, execution: *Execution, row: codec.OrdinalRowView) !Value {
        return self.evaluateRetainedSource(execution, .{ .row = row });
    }

    /// Caller must fence the exact immutable source layout used to compile this
    /// plan. TuplePlan establishes that proof once per scan, including history.
    pub fn evaluateBoundRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateBoundRowWithExecution(&execution, row);
    }

    pub fn evaluateBoundRowWithExecution(self: *const Plan, execution: *Execution, row: codec.OrdinalRowView) !Value {
        return self.evaluateRetainedSource(execution, .{ .bound_row = row });
    }

    /// Legacy entry points own their temporary prepared input. Retain any
    /// potentially decoded result in the caller's row region before retiring
    /// that input; explicit shared-owner callers can keep the borrowing API.
    fn evaluateRetainedSource(self: *const Plan, execution: *Execution, source: Source) !Value {
        const mask = if (self.prepared_sources == 0xff) preparedSourceMask(self.nodes) else self.prepared_sources;
        const selected: u8 = switch (source) {
            .json => 1,
            .row, .bound_row => 2,
            else => unreachable,
        };
        // Ordinary scalars and already encoded cold NUMERIC/arrays need no
        // temporary owner, cache, allocator accounting or per-row node scan.
        if (mask & selected == 0) {
            const value = self.evaluateNode(execution, source, @intCast(self.nodes.len - 1)) catch |err| {
                if (err == error.RelationalExpressionBudgetExceeded) return execution.limit();
                return executionFailure(err);
            };
            return retainPreparedResult(execution, value, false) catch |err| {
                if (err == error.RelationalExpressionBudgetExceeded) return execution.limit();
                return executionFailure(err);
            };
        }
        var input: PreparedInput = undefined;
        input.init(execution, source, null) catch |err| return executionFailure(err);
        defer input.deinit();
        const value = self.evaluatePreparedWithExecution(execution, &input) catch |err| {
            if (err == error.RelationalExpressionBudgetExceeded) return execution.limit();
            return executionFailure(err);
        };
        return retainPreparedResult(execution, value, input.roots.decodes != 0) catch |err| {
            if (err == error.RelationalExpressionBudgetExceeded) return execution.limit();
            return executionFailure(err);
        };
    }

    pub fn evaluatePreparedWithExecution(self: *const Plan, execution: *Execution, input: *PreparedInput) !Value {
        try input.requireExecution(execution);
        return self.evaluateNode(execution, .{ .prepared = input }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    const Source = union(enum) { values: []const Value, json: std.json.Value, row: codec.OrdinalRowView, bound_row: codec.OrdinalRowView, prepared: *PreparedInput };

    fn borrowedBlob(self: *const Plan, source: Source, index: u16) anyerror!BlobOperand {
        return borrowedBlobNode(source, self.nodes[index]);
    }

    fn borrowedBlobNode(source: Source, node: Node) anyerror!BlobOperand {
        if (node.op == .literal) return BlobOperand.fromValue(node.literal);
        std.debug.assert(node.op == .column and node.kind == .blob);
        return switch (source) {
            .prepared => |input| if (try input.mappedNode(node)) |physical| borrowedBlobNode(input.source, physical) else .{},
            .values => |values| if (node.ordinal < values.len) BlobOperand.fromValue(values[node.ordinal]) else error.InvalidRelationalExpressionInput,
            .json => |json| BlobOperand.fromJson(json.object.get(node.column_name) orelse .null),
            .row, .bound_row => |row| blk: {
                const ordinal = if (source == .bound_row) node.ordinal else row.ordinalForName(node.column_name) orelse break :blk .{};
                if (ordinal >= row.table_schema.relational_columns.len or row.table_schema.relational_columns[ordinal].column_type != .blob) return error.RelationalIndexColumnTypeMismatch;
                const cell = (try row.findCell(ordinal)) orelse break :blk .{};
                if (cell.is_null) break :blk .{};
                // AROW blob cells retain the API's base64 representation.
                // A typed expression literal/value is already decoded bytes.
                break :blk try BlobOperand.fromJson(.{ .string = cell.value.bytes_val });
            },
        };
    }

    fn evaluateArray(self: *const Plan, execution: *Execution, source: Source, node: Node) !Value {
        const arrays = @import("../sql/array_value.zig");
        const storage = @import("../sql/array_storage.zig");
        const kind = node.sql_type.?;
        if (node.children.len != 0 and self.nodes[node.children[0]].kind == .sql_array) {
            var parts: [32]?storage.layout.View = undefined;
            for (node.children, parts[0..node.children.len]) |child, *part| {
                const value = try self.evaluateNode(execution, source, child);
                part.* = if (value == .null) null else if (value == .sql_array and value.sql_array.element_type == kind) try value.sql_array.view() else return error.InvalidRelationalExpressionInput;
            }
            var memory: @import("../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true };
            defer execution.bytes.* -|= memory.footprint();
            const bytes = storage.stackCanonicalAlloc(memory.allocator(), kind, parts[0..node.children.len], .{
                .context = &execution.numeric,
                .values = .{ .bytes = execution.bytes.* },
                .wire_bytes = max_output_bytes,
            }) catch |err| return if (err == error.OutOfMemory and memory.isExhausted()) execution.limit() else executionFailure(err);
            return .{ .sql_array = .{ .element_type = kind, .bytes = bytes } };
        }
        // Scalar constructors have at most 32 arguments. Keep their descriptors
        // on the stack; only exact coefficients need bounded temporary limbs.
        var values: [32]Value = undefined;
        for (node.children, values[0..node.children.len]) |child, *value| value.* = try self.evaluateNode(execution, source, child);
        var scratch: NumericScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        var cells: [32]arrays.Element = undefined;
        var numbers: [32]exact.Value = undefined;
        for (values[0..node.children.len], 0..) |value, i| {
            cells[i] = switch (value) {
                .null => .{},
                .integer => |integer| arrays.Element.json(.{ .integer = integer }),
                .number => |number| arrays.Element.json(.{ .float = number }),
                .boolean => |boolean| arrays.Element.json(.{ .bool = boolean }),
                .string => |text| arrays.Element.json(.{ .string = text }),
                .numeric => |bytes| blk: {
                    numbers[i] = (binary.decodeCanonical(&execution.numeric, bytes) catch |err| return scratch.failure(err)).value;
                    break :blk arrays.Element.typedNumeric(&numbers[i]);
                },
                else => return error.InvalidRelationalExpressionInput,
            };
        }
        const dimensions = [_]arrays.Dimension{.{ .length = @intCast(node.children.len) }};
        var prepared = storage.Prepared.init(scratch.arena.allocator(), .{
            .element_type = kind,
            .dimensions = if (node.children.len == 0) &.{} else &dimensions,
            .elements = cells[0..node.children.len],
        }, .{ .context = &execution.numeric, .values = .{ .bytes = execution.bytes.* }, .wire_bytes = max_output_bytes }) catch |err| return scratch.failure(err);
        defer prepared.deinit();
        if (prepared.encoded_size > execution.bytes.* -| scratch.memory.footprint()) return execution.limit();
        const bytes = allocateOutput(execution.alloc, prepared.encoded_size, execution.bytes) catch |err| return scratch.failure(err);
        errdefer execution.alloc.free(bytes);
        prepared.writeInto(bytes) catch |err| return scratch.failure(err);
        return .{ .sql_array = .{ .element_type = kind, .bytes = bytes } };
    }

    fn evaluateNode(self: *const Plan, execution: *Execution, source: Source, index: u16) anyerror!Value {
        try execution.numeric.charge(1);
        const node = self.nodes[index];
        if (node.op == .uuidv4 or node.op == .uuidv7) {
            if (node.uuid_shift) |shift| if (shift.infinity() != 0) return error.TemporalOutOfRange;
            const runtime = execution.preparation_runtime orelse return error.SqlEntropyUnavailable;
            const functions = @import("../sql/uuid_functions.zig");
            const bytes = if (node.op == .uuidv4) try functions.generate4(runtime.io) else try functions.generate(runtime.io, runtime.uuid_clock orelse &functions.process_clock, node.uuid_shift);
            const formatted = @import("../common/uuid.zig").format(bytes);
            try execution.charge(formatted.len);
            const result = try allocateOutput(execution.alloc, formatted.len, execution.bytes);
            @memcpy(result, &formatted);
            return .{ .string = result };
        }
        if (node.op == .literal) return node.literal;
        if (node.op == .array) return self.evaluateArray(execution, source, node);
        if (node.op == .column) return readColumn(execution, source, node);
        return self.evaluateOperation(execution, source, index);
    }

    fn readColumn(execution: *Execution, source: Source, node: Node) anyerror!Value {
        const alloc = execution.alloc;
        const value: Value = switch (source) {
            .prepared => |input| try input.read(execution, node),
            .values => |values| if (node.ordinal < values.len) values[node.ordinal] else return error.InvalidRelationalExpressionInput,
            .json => |json| blk: {
                const input = json.object.get(node.column_name) orelse .null;
                if (node.kind == .blob) break :blk try decodeBlob(execution, try BlobOperand.fromJson(input));
                if (node.kind == .sql_array) break :blk try arrayJson(execution, node.sql_type.?, input, null);
                break :blk if (node.kind == .numeric) try numericJson(execution, input) else try scalarJson(alloc, node.kind, input, false);
            },
            .row, .bound_row => |row| blk: {
                const ordinal = if (source == .bound_row) node.ordinal else row.ordinalForName(node.column_name) orelse break :blk .null;
                if (ordinal >= row.table_schema.relational_columns.len) return error.RelationalIndexColumnTypeMismatch;
                if (row.table_schema.relational_columns[ordinal].column_type != node.kind) return error.RelationalIndexColumnTypeMismatch;
                if (node.kind == .sql_array and row.table_schema.relational_columns[ordinal].sql_element_type != node.sql_type) return error.RelationalIndexColumnTypeMismatch;
                const cell = (try row.findCell(ordinal)) orelse break :blk .null;
                if (cell.is_null) break :blk .null;
                break :blk switch (node.kind) {
                    .string => .{ .string = cell.value.bytes_val },
                    .blob => try decodeBlob(execution, try BlobOperand.fromJson(.{ .string = cell.value.bytes_val })),
                    .integer => .{ .integer = cell.value.i64_val },
                    .number => .{ .number = cell.value.f64_val },
                    .numeric => .{ .numeric = cell.value.bytes_val },
                    .sql_array => .{ .sql_array = .{ .element_type = node.sql_type.?, .bytes = cell.value.bytes_val } },
                    .boolean => .{ .boolean = cell.value.bool_val },
                    .datetime => .{ .datetime = cell.value.u64_val },
                    else => return error.InvalidRelationalExpressionType,
                };
            },
        };
        if (value != .null and !valueHasKind(value, node.kind)) return error.InvalidRelationalExpressionInput;
        if (value == .sql_array and value.sql_array.element_type != node.sql_type) return error.InvalidRelationalExpressionInput;
        if (value == .number and !std.math.isFinite(value.number)) return error.InvalidRelationalExpressionInput;
        return value;
    }

    fn evaluateOperation(self: *const Plan, execution: *Execution, source: Source, index: u16) anyerror!Value {
        const alloc = execution.alloc;
        const budget = execution.bytes;
        const node = self.nodes[index];
        if (node.op == .case_when) {
            var i: usize = 0;
            while (i + 1 < node.children.len) : (i += 2) {
                const condition = try self.evaluateNode(execution, source, node.children[i]);
                if (condition != .null and condition.boolean)
                    return self.evaluateNode(execution, source, node.children[i + 1]);
            }
            return self.evaluateNode(execution, source, node.children[node.children.len - 1]);
        }
        if (node.op == .coalesce) {
            for (node.children) |child| {
                const value = try self.evaluateNode(execution, source, child);
                if (value != .null) return value;
            }
            return .null;
        }
        if (node.op == .@"and" or node.op == .@"or") {
            var unknown = false;
            for (node.children) |child| {
                const value = try self.evaluateNode(execution, source, child);
                if (value == .null) {
                    unknown = true;
                    continue;
                }
                if (value.boolean == (node.op == .@"or")) return value;
            }
            return if (unknown) .null else .{ .boolean = node.op == .@"and" };
        }
        if (node.op == .is_null or node.op == .is_not_null) {
            const value = try self.evaluateNode(execution, source, node.children[0]);
            return .{ .boolean = (value == .null) == (node.op == .is_null) };
        }
        if (node.op == .in_list or node.op == .not_in_list) {
            // The probe is evaluated once. NULL candidates do not terminate
            // the search: a later equality wins over UNKNOWN.
            const probe = try self.evaluateNode(execution, source, node.children[0]);
            var unknown = probe == .null;
            for (node.children[1..]) |child| {
                const candidate = try self.evaluateNode(execution, source, child);
                if (probe == .null or candidate == .null) {
                    unknown = true;
                    continue;
                }
                const bytes: usize = switch (probe) {
                    .string => |value| @min(value.len, candidate.string.len),
                    .blob => |value| @min(value.len, candidate.blob.len),
                    else => 0,
                };
                if (bytes > budget.* / 2) return error.RelationalExpressionBudgetExceeded;
                budget.* -= bytes * 2;
                if (try compareValues(execution, probe, candidate, node.fold_ascii) == .eq) return .{ .boolean = node.op == .in_list };
            }
            return if (unknown) .null else .{ .boolean = node.op == .not_in_list };
        }
        if (isComparison(node.op)) {
            const l = self.nodes[node.children[0]];
            const r = self.nodes[node.children[1]];
            if (l.kind == .blob and r.kind == .blob and
                (l.op == .column or l.op == .literal) and (r.op == .column or r.op == .literal))
            {
                try execution.charge(2);
                return compareBlobs(execution, node.op, try self.borrowedBlob(source, node.children[0]), try self.borrowedBlob(source, node.children[1]));
            }
            const left = try self.evaluateNode(execution, source, node.children[0]);
            const right = try self.evaluateNode(execution, source, node.children[1]);
            if (left == .null or right == .null) return comparisonValue(node.op, left == .null, right == .null, .eq);
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
            const order = try compareValues(execution, left, right, node.fold_ascii);
            return comparisonValue(node.op, false, false, order);
        }
        if (node.op == .cast and node.kind == .sql_array) {
            const input = try self.evaluateNode(execution, source, node.children[0]);
            if (input == .null) return input;
            if (input != .sql_array or input.sql_array.element_type != node.sql_type) return error.InvalidRelationalExpressionInput;
            // Identity casts borrow the pinned canonical row. NUMERIC typmods
            // share assignment's streaming coefficient conversion and sticky
            // admission; neither path reconstructs a flat cell vector or JSON.
            if (node.numeric_modifier) |modifier| return normalizeNumericArrayBinding(execution, input, modifier, false);
            return input;
        }
        if (jsonFunction(node.op)) return self.evaluateJsonFunction(execution, source, node);
        if (node.op == .replace or node.op == .md5) {
            // Strict calls still evaluate every argument before propagating
            // NULL; a later selected argument may raise an arithmetic error.
            var args: [3]Value = undefined;
            var has_null = false;
            for (node.children, 0..) |child, i| {
                args[i] = try self.evaluateNode(execution, source, child);
                has_null = has_null or args[i] == .null;
            }
            if (has_null) return .null;
            if (node.op == .md5) {
                if (budget.* < 32) return execution.limit();
                const digest = try text_kernel.md5(args[0].string, &execution.numeric);
                const output = try allocateOutput(alloc, digest.len, budget);
                @memcpy(output, &digest);
                return .{ .string = output };
            }
            const limit = @min(max_output_bytes, budget.*);
            const result = if (node.replace_pattern) |pattern|
                try pattern.replace(alloc, args[0].string, args[2].string, limit, &execution.numeric)
            else
                try text_kernel.replace(alloc, args[0].string, args[1].string, args[2].string, limit, &execution.numeric);
            budget.* -= result.allocated_bytes;
            return .{ .string = result.bytes };
        }
        if (numericFunction(node.op)) |function| {
            // Selected arguments are evaluated before strict NULL propagation.
            var args: [2]Value = undefined;
            var has_null = false;
            for (node.children, 0..) |child, i| {
                args[i] = try self.evaluateNode(execution, source, child);
                has_null = has_null or args[i] == .null;
            }
            if (has_null) return .null;
            if (args[0] == .integer) {
                const input = args[0].integer;
                return numericCast(.{ .integer = if (input < 0) std.math.negate(input) catch return error.RelationalExpressionOverflow else input }, node.sql_type orelse .int64);
            }
            if (args[0] == .number) {
                const result = @import("../sql/numeric_functions.zig").floating(&execution.numeric, function, args[0].number) catch |err| return executionFailure(err);
                return numericCast(.{ .number = result }, node.sql_type orelse .float64);
            }
            if (function == .abs) return numericOperation(execution, node, args[0..1], self.nodes[node.children[0]]);
            if (function == .sign) {
                const view = try binary.layout.View.openWithBudget(args[0].numeric, .{}, &execution.numeric);
                const sign: exact.Value = if (view.kind == .nan) .{ .kind = .nan } else if (view.kind == .finite and view.count == 0) .{} else .{ .digits = &.{1}, .negative = view.negative or view.kind == .negative_infinity };
                var scratch: NumericScratch = undefined;
                scratch.init(execution);
                defer scratch.deinit();
                return scratch.encode(sign) catch |err| return scratch.failure(err);
            }
            var scratch: NumericScratch = undefined;
            scratch.init(execution);
            defer scratch.deinit();
            const input = numericOperand(&execution.numeric, args[0], false) catch |err| return scratch.failure(err);
            const scale: i32 = if (node.children.len == 2) std.math.cast(i32, args[1].integer) orelse return error.RelationalExpressionOverflow else 0;
            const result = @import("../sql/numeric_functions.zig").numeric(&execution.numeric, function, input, scale) catch |err| return scratch.failure(err);
            // The scratch owner pins borrowed limbs through canonical encoding.
            return scratch.encode(result.value) catch |err| return scratch.failure(err);
        }
        var operands: [32]Value = undefined;
        for (node.children, 0..) |child, i| {
            operands[i] = try self.evaluateNode(execution, source, child);
            if (operands[i] == .null) return .null;
        }
        const a = operands[0];
        if (a == .numeric or node.kind == .numeric) return numericOperation(execution, node, operands[0..node.children.len], self.nodes[node.children[0]]);
        switch (node.op) {
            .not => return .{ .boolean = !a.boolean },
            .cast => return numericCast(a, node.sql_type.?),
            .negate => return switch (a) {
                .integer => |v| numericCast(.{ .integer = std.math.sub(i64, 0, v) catch return error.RelationalExpressionOverflow }, node.sql_type orelse .int64),
                .number => |v| numericCast(.{ .number = -v }, node.sql_type orelse .float64),
                else => unreachable,
            },
            .add, .subtract, .multiply, .divide, .modulo => {
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
                        .modulo => blk: {
                            if (b.integer == 0) return error.RelationalExpressionDivisionByZero;
                            // Unlike division, minInt % -1 is exactly zero.
                            break :blk if (b.integer == -1) 0 else @rem(a.integer, b.integer);
                        },
                        else => unreachable,
                    } catch return error.RelationalExpressionOverflow;
                    return numericCast(.{ .integer = result }, node.sql_type orelse .int64);
                }
                if (node.op == .divide and b.number == 0) return error.RelationalExpressionDivisionByZero;
                const result = switch (node.op) {
                    .add => a.number + b.number,
                    .subtract => a.number - b.number,
                    .multiply => a.number * b.number,
                    .divide => a.number / b.number,
                    else => unreachable,
                };
                if (node.sql_type == .float32) {
                    // A float4 operation rounds its result once, independently
                    // of subsequent promotion to a wider expression domain.
                    const rounded = try numericCast(.{ .number = result }, .float32);
                    if (rounded.number == 0 and a.number != 0 and b.number != 0 and (node.op == .multiply or node.op == .divide)) return error.RelationalExpressionOverflow;
                    return rounded;
                }
                return finite(result);
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

    fn selectJsonPath(self: *const Plan, execution: *Execution, source: Source, node: Node) anyerror!?std.json.Value {
        const operand = self.nodes[node.children[0]];
        var value: ?std.json.Value = if (operand.op == .jsonb_extract_path) nested: {
            // A structural consumer needs the subtree, not a transient JSON
            // result header. Fuse direct path chains without cloning or
            // allocating an intermediate wrapper; keep node work identical.
            try execution.numeric.charge(1);
            break :nested try self.selectJsonPath(execution, source, operand);
        } else root: {
            const result = try self.evaluateNode(execution, source, node.children[0]);
            break :root if (result == .null) null else result.json.value;
        };
        var work: @import("../sql/json_order.zig").Budget = .{ .shared = &execution.numeric };
        // Strict selected arguments still run after SQL NULL, a NULL path
        // component or a missing child. Lazy ancestors decide selection.
        for (node.children[1..]) |child| {
            const key = try self.evaluateNode(execution, source, child);
            if (key == .null) {
                value = null;
            } else if (value) |current| {
                value = @import("../sql/json_path.zig").extract(current, key.string, &work) catch |err| return executionFailure(err);
            }
        }
        return value;
    }

    fn evaluateJsonFunction(self: *const Plan, execution: *Execution, source: Source, node: Node) !Value {
        const selected = (try self.selectJsonPath(execution, source, node)) orelse return .null;
        var work: @import("../sql/json_order.zig").Budget = .{ .shared = &execution.numeric };
        switch (node.op) {
            .jsonb_typeof => return .{ .string = switch (selected) {
                .null => "null",
                .bool => "boolean",
                .integer, .float, .number_string => "number",
                .string => "string",
                .array => "array",
                .object => "object",
            } },
            .jsonb_array_length => {
                if (selected != .array) return error.InvalidRelationalExpressionInput;
                return .{ .integer = std.math.cast(i32, selected.array.items.len) orelse return error.RelationalExpressionOverflow };
            },
            .jsonb_extract_path => {
                const Json = @import("../sql/row_value.zig").Json;
                try execution.charge(1);
                if (@sizeOf(Json) > execution.bytes.*) return execution.limit();
                execution.bytes.* -= @sizeOf(Json);
                const view = try execution.alloc.create(Json);
                view.* = .{ .value = selected };
                return .{ .json = view };
            },
            .jsonb_extract_path_text => {
                if (selected == .null) return .null;
                if (selected == .string) return .{ .string = selected.string };
                var memory: @import("../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true };
                defer execution.bytes.* -|= memory.footprint();
                const text = @import("../sql/jsonb_text.zig").format(memory.allocator(), selected, @min(max_output_bytes, execution.bytes.*), &work) catch |err|
                    return if (err == error.OutOfMemory and memory.isExhausted()) execution.limit() else executionFailure(err);
                return .{ .string = text };
            },
            else => unreachable,
        }
    }
};

test "relational index system SQL durable numeric ABS borrows positive payloads without allocation" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "9007199254740993.2500", "NaN", "Infinity", "0.00" }) |input| {
        const text = try std.fmt.allocPrint(a, "{{\"op\":\"abs\",\"args\":[{{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"{s}\"}}]}}", .{input});
        defer a.free(text);
        var definition = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer definition.deinit();
        var plan = try Plan.init(a, .{}, definition.value, .numeric);
        defer plan.deinit();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        const result = try plan.evaluate(none.allocator(), &.{});
        try std.testing.expectEqual(@intFromPtr(plan.nodes[0].literal.numeric.ptr), @intFromPtr(result.numeric.ptr));
        try std.testing.expectEqual(@as(usize, 0), none.end_index);
    }
}

test "relational index system SQL durable numeric functions validate mixed types and selected errors" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        \\{"op":"ceil","sql_type":"float32","args":[{"op":"literal","type":"number","sql_type":"float64","value":1}]}
        ,
        \\{"op":"abs","sql_type":"int16","args":[{"op":"literal","type":"integer","sql_type":"int32","value":1}]}
        ,
        "{\"op\":\"ceil\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}]}",
        "{\"op\":\"sqrt\",\"args\":[{\"op\":\"literal\",\"type\":\"number\",\"sql_type\":\"float32\",\"value\":1}]}",
        "{\"op\":\"round\",\"args\":[{\"op\":\"literal\",\"type\":\"number\",\"value\":1},{\"op\":\"literal\",\"type\":\"integer\",\"sql_type\":\"int32\",\"value\":2}]}",
        "{\"op\":\"trunc\",\"args\":[{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1\"},{\"op\":\"literal\",\"type\":\"integer\",\"sql_type\":\"int64\",\"value\":2}]}",
    }) |text| {
        var definition = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer definition.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, .{}, definition.value, .numeric));
    }
    var definition = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"round","args":[{"op":"literal","type":"numeric"},{"op":"divide","sql_type":"int32","args":[{"op":"literal","type":"integer","sql_type":"int32","value":1},{"op":"literal","type":"integer","sql_type":"int32","value":0}]}]}
    , .{});
    defer definition.deinit();
    var plan = try Plan.init(a, .{}, definition.value, .numeric);
    defer plan.deinit();
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, plan.evaluate(a, &.{}));
}

test "relational index system SQL durable numeric functions unwind faults and retain shared cancellation" {
    const a = std.testing.allocator;
    const Harness = struct {
        fn definition(alloc: Allocator, function: []const u8) !std.json.Parsed(std.json.Value) {
            const scale: []const u8 = if (std.mem.eql(u8, function, "round") or std.mem.eql(u8, function, "trunc")) ",{\"op\":\"literal\",\"type\":\"integer\",\"sql_type\":\"int32\",\"value\":2}" else "";
            const text = try std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\",\"sql_type\":\"numeric\",\"args\":[{{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"123456789.125\"}}{s}]}}", .{ function, scale });
            defer alloc.free(text);
            return std.json.parseFromSlice(std.json.Value, alloc, text, .{ .allocate = .alloc_always });
        }
        fn prepare(alloc: Allocator, expression: std.json.Value) !void {
            var plan = try Plan.init(alloc, .{}, expression, .numeric);
            defer plan.deinit();
        }
        fn evaluate(alloc: Allocator, plan: *const Plan) !void {
            const result = try plan.evaluate(alloc, &.{});
            if (result.numeric.ptr != plan.nodes[0].literal.numeric.ptr) alloc.free(result.numeric);
        }
    };
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    inline for (comptime std.meta.fieldNames(@import("../sql/numeric_functions.zig").Function)) |function| {
        var parsed = try Harness.definition(a, function);
        defer parsed.deinit();
        // Force arena growth through allocations: backing allocator resize
        // success may depend on its address/bin history, changing the number
        // of fault cuts. This also exercises every replacement-allocation cut.
        var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(no_resize.allocator(), Harness.prepare, .{parsed.value});
        var plan = try Plan.init(a, .{}, parsed.value, .numeric);
        defer plan.deinit();
        try std.testing.checkAllAllocationFailures(no_resize.allocator(), Harness.evaluate, .{&plan});
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var bytes: usize = max_allocated_bytes;
        var execution = Execution.init(arena.allocator(), &bytes);
        var observed: Control = .{};
        execution.numeric.checkpoint = Control.check;
        execution.numeric.ptr = &observed;
        _ = try plan.evaluateWithExecution(&execution, &.{});
        for (1..observed.polls + 1) |cut| {
            bytes = max_allocated_bytes;
            execution = Execution.init(arena.allocator(), &bytes);
            var control: Control = .{ .fail_at = cut };
            execution.numeric.checkpoint = Control.check;
            execution.numeric.ptr = &control;
            try std.testing.expectError(error.QueryCanceled, plan.evaluateWithExecution(&execution, &.{}));
            try std.testing.expectError(error.QueryCanceled, execution.charge(0));
        }
        bytes = max_allocated_bytes;
        execution = Execution.init(arena.allocator(), &bytes);
        execution.numeric.remaining = 0;
        try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
        try std.testing.expectError(error.RelationalExpressionBudgetExceeded, execution.charge(0));
    }
}

test "relational index system SQL durable text literal patterns compile once and retain invocation limits" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "s", .path = "s", .column_type = .string }} };
    const definition = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"replace","args":[{"op":"column","column":"s"},{"op":"literal","type":"string","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},{"op":"literal","type":"string","value":"X"}]}
    , .{});
    defer definition.deinit();
    var plan = try Plan.init(a, table, definition.value, .string);
    defer plan.deinit();
    try std.testing.expect(plan.nodes[plan.nodes.len - 1].replace_pattern != null);
    try std.testing.expect(plan.literal_bytes >= 80 * (1 + @sizeOf(usize)));
    const input: [1024]u8 = @splat('a');
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var bytes: usize = 0;
    var execution = Execution.init(none.allocator(), &bytes);
    execution.numeric.remaining = 100_000_000;
    for (0..1024) |_| {
        const result = try plan.evaluateWithExecution(&execution, &.{.{ .string = &input }});
        try std.testing.expectEqual(@intFromPtr(&input), @intFromPtr(result.string.ptr));
    }
    try std.testing.expectEqual(@as(usize, 0), none.end_index);
    execution.numeric.remaining = 10;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{.{ .string = &input }}));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, execution.charge(0));
}

test "relational index system SQL durable text evaluation preserves lazy branches strict calls and allocation faults" {
    const a = std.testing.allocator;
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const definition = try std.json.parseFromSlice(std.json.Value, alloc,
                \\{"op":"md5","args":[{"op":"replace","args":[{"op":"literal","type":"string","value":"aaaaa"},{"op":"literal","type":"string","value":"aa"},{"op":"literal","type":"string","value":"X"}]}]}
            , .{});
            defer definition.deinit();
            var plan = try Plan.init(alloc, .{}, definition.value, .string);
            defer plan.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const result = try plan.evaluate(arena.allocator(), &.{});
            var work: exact.Context = .{ .alloc = alloc };
            try std.testing.expectEqualStrings(&(try text_kernel.md5("XXa", &work)), result.string);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{});
    const definition = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"replace","args":[{"op":"literal","type":"string"},{"op":"case_when","args":[{"op":"eq","args":[{"op":"divide","args":[{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":0}]},{"op":"literal","type":"integer","value":0}]},{"op":"literal","type":"string","value":"x"},{"op":"literal","type":"string","value":""}]},{"op":"literal","type":"string","value":""}]}
    , .{});
    defer definition.deinit();
    var plan = try Plan.init(a, .{}, definition.value, .string);
    defer plan.deinit();
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, plan.evaluate(a, &.{}));
}

test "relational index system SQL durable text dynamic search and cancellation share one owner" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "s", .path = "s", .column_type = .string },
        .{ .name = "search", .path = "search", .column_type = .string },
    } };
    const definition = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"replace","args":[{"op":"column","column":"s"},{"op":"column","column":"search"},{"op":"literal","type":"string","value":"X"}]}
    , .{});
    defer definition.deinit();
    var plan = try Plan.init(a, table, definition.value, .string);
    defer plan.deinit();
    try std.testing.expect(plan.nodes[plan.nodes.len - 1].replace_pattern == null);
    var exact_bytes: [3]u8 = undefined;
    var memory = std.heap.FixedBufferAllocator.init(&exact_bytes);
    var bytes: usize = exact_bytes.len;
    var execution = Execution.init(memory.allocator(), &bytes);
    const result = try plan.evaluateWithExecution(&execution, &.{ .{ .string = "aaaaa" }, .{ .string = "aa" } });
    try std.testing.expectEqualStrings("XXa", result.string);
    try std.testing.expectEqual(@as(usize, 0), bytes);
    const Control = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.QueryCanceled;
        }
    };
    var control: Control = .{};
    bytes = 4096;
    execution = Execution.init(a, &bytes);
    execution.numeric.checkpoint = Control.check;
    execution.numeric.ptr = &control;
    const wide: [4096]u8 = @splat('a');
    try std.testing.expectError(error.QueryCanceled, plan.evaluateWithExecution(&execution, &.{ .{ .string = &wide }, .{ .string = "aaaab" } }));
    try std.testing.expectError(error.QueryCanceled, execution.charge(0));
}

fn numericOperand(ctx: *exact.Context, value: Value, real: bool) !exact.Value {
    return switch (value) {
        .numeric => |bytes| (try binary.decodeCanonical(ctx, bytes)).value,
        .integer => |integer| (try @import("../sql/numeric_storage.zig").fromJson(ctx, .{ .integer = integer })).value,
        .number => |number| (try exact.fromFloat(ctx, number, real)).value,
        else => error.InvalidRelationalExpressionInput,
    };
}

fn numericOperation(execution: *Execution, node: Node, operands: []const Value, source: Node) !Value {
    const input = operands[0];
    if (node.op == .cast and input == .numeric) if (node.numeric_modifier) |modifier|
        return normalizeNumericBinding(execution, input.numeric, modifier, false);
    if (input == .numeric and (node.op == .negate or node.op == .abs or (node.op == .cast and node.sql_type == .numeric))) {
        const View = @import("../common/sql_numeric_layout.zig").View;
        const view = try View.openWithBudget(input.numeric, .{}, &execution.numeric);
        if (node.op == .cast or view.kind == .nan or (view.kind == .finite and view.count == 0)) return input;
        if (node.op == .abs and !view.negative and view.kind != .negative_infinity) return input;
        try execution.numeric.charge(input.numeric.len);
        const bytes = try allocateOutput(execution.alloc, input.numeric.len, execution.bytes);
        @memcpy(bytes, input.numeric);
        const sign: u16 = switch (view.kind) {
            .finite => if (view.negative) 0 else 0x4000,
            .positive_infinity => 0xf000,
            .negative_infinity => 0xd000,
            .nan => unreachable,
        };
        std.mem.writeInt(u16, bytes[4..6], sign, .big);
        return .{ .numeric = bytes };
    }
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    return numericOperationInner(&scratch, node, operands, source) catch |err| return scratch.failure(err);
}

fn numericOperationInner(scratch: *NumericScratch, node: Node, operands: []const Value, source: Node) !Value {
    const ctx = &scratch.execution.numeric;
    const a = try numericOperand(ctx, operands[0], source.sql_type == .float32);
    if (node.op == .cast) {
        const target = node.sql_type.?;
        if (target == .numeric) {
            if (node.numeric_modifier) |modifier| {
                const constrained = try exact.applyTypeModifier(ctx, a, modifier);
                return scratch.encode(constrained.value);
            }
            return scratch.encode(a);
        }
        if (casts.integral(target)) {
            const integer = try exact.toInteger(i64, ctx, a);
            return numericCast(.{ .integer = integer }, target);
        }
        const text = try exact.format(ctx, a);
        return finite(if (target == .float32)
            try casts.floatValue(f32, .{ .number_string = text })
        else if (target == .float64)
            try casts.floatValue(f64, .{ .number_string = text })
        else
            return error.InvalidRelationalExpressionType);
    }
    const b = try numericOperand(ctx, operands[1], false);
    const result = try switch (node.op) {
        .add => exact.add(ctx, a, b),
        .subtract => exact.subtract(ctx, a, b),
        .multiply => exact.multiply(ctx, a, b),
        .divide => exact.divide(ctx, a, b),
        .modulo => exact.remainder(ctx, a, b),
        else => return error.InvalidRelationalExpressionType,
    };
    return scratch.encode(result.value);
}

fn numericCast(value: Value, target: Numeric) !Value {
    if (value == .null) return .null;
    if (casts.integral(target)) return .{ .integer = switch (value) {
        .integer => |v| casts.checkedInteger(v, target) catch return error.RelationalExpressionOverflow,
        .number => |v| casts.floatingInteger(v, target) catch return error.RelationalExpressionOverflow,
        else => return error.InvalidRelationalExpressionInput,
    } };
    const raw: std.json.Value = switch (value) {
        .integer => |v| .{ .integer = v },
        .number => |v| .{ .float = v },
        else => return error.InvalidRelationalExpressionInput,
    };
    return if (target == .float32)
        .{ .number = casts.floatValue(f32, raw) catch return error.RelationalExpressionOverflow }
    else if (target == .float64)
        finite(casts.floatValue(f64, raw) catch return error.RelationalExpressionOverflow)
    else
        error.InvalidRelationalExpressionType;
}

fn allocateOutput(alloc: Allocator, size: usize, budget: *usize) ![]u8 {
    if (size > max_output_bytes or size > budget.*) return error.RelationalExpressionBudgetExceeded;
    budget.* -= size;
    return alloc.alloc(u8, size);
}

fn comparisonValue(op: Op, left_null: bool, right_null: bool, order: std.math.Order) Value {
    if (left_null or right_null) return switch (op) {
        .is_distinct => .{ .boolean = left_null != right_null },
        .is_not_distinct => .{ .boolean = left_null == right_null },
        else => .null,
    };
    return .{ .boolean = switch (op) {
        .eq, .is_not_distinct => order == .eq,
        .ne, .is_distinct => order != .eq,
        .gt => order == .gt,
        .gte => order != .lt,
        .lt => order == .lt,
        .lte => order != .gt,
        else => unreachable,
    } };
}

/// Physical/API blob cells retain base64; typed operands retain decoded bytes.
/// Borrow that distinction instead of allocating a decoded row for CHECKs.
const BlobOperand = struct {
    bytes: []const u8 = &.{},
    encoded: bool = false,
    sql_null: bool = true,
    length: usize = 0,

    fn fromValue(value: Value) !BlobOperand {
        return switch (value) {
            .null => .{},
            .blob => |bytes| .{ .bytes = bytes, .length = bytes.len, .sql_null = false },
            else => error.InvalidRelationalExpressionInput,
        };
    }

    fn fromJson(value: std.json.Value) !BlobOperand {
        return switch (value) {
            .null => .{},
            .string => |bytes| .{
                .bytes = bytes,
                .encoded = true,
                .sql_null = false,
                .length = std.base64.standard.Decoder.calcSizeForSlice(bytes) catch return error.InvalidRelationalExpressionInput,
            },
            else => error.InvalidRelationalExpressionInput,
        };
    }
};

const BlobCursor = struct {
    operand: BlobOperand,
    offset: usize = 0,
    scratch: [192]u8 = undefined,

    fn next(self: *BlobCursor, execution: *Execution) ![]const u8 {
        if (self.offset == self.operand.bytes.len) return &.{};
        const end = self.offset + @min(self.operand.bytes.len - self.offset, if (self.operand.encoded) @as(usize, 256) else 192);
        const input = self.operand.bytes[self.offset..end];
        try execution.charge(input.len);
        self.offset = end;
        if (!self.operand.encoded) return input;
        const size = std.base64.standard.Decoder.calcSizeForSlice(input) catch return error.InvalidRelationalExpressionInput;
        // Padding is legal only at the end of the complete value, not a chunk.
        if (end != self.operand.bytes.len and size != self.scratch.len) return error.InvalidRelationalExpressionInput;
        std.base64.standard.Decoder.decode(self.scratch[0..size], input) catch return error.InvalidRelationalExpressionInput;
        return self.scratch[0..size];
    }
};

fn compareBlobs(execution: *Execution, op: Op, left: BlobOperand, right: BlobOperand) !Value {
    if (left.sql_null or right.sql_null) {
        for ([_]BlobOperand{ left, right }) |operand| {
            if (!operand.encoded) continue;
            var cursor: BlobCursor = .{ .operand = operand };
            while ((try cursor.next(execution)).len != 0) {}
        }
        return comparisonValue(op, left.sql_null, right.sql_null, .eq);
    }
    const compared = @min(left.length, right.length);
    if (compared > execution.bytes.* / 2) return execution.limit();
    execution.bytes.* -= compared * 2;
    if (!left.encoded and !right.encoded)
        return comparisonValue(op, false, false, try orderBytes(left.bytes, right.bytes, false, &execution.numeric));
    var l: BlobCursor = .{ .operand = left };
    var r: BlobCursor = .{ .operand = right };
    var order: std.math.Order = .eq;
    while (true) {
        const a = try l.next(execution);
        const b = try r.next(execution);
        if (a.len == 0 and b.len == 0) break;
        if (order == .eq) order = std.mem.order(u8, a, b);
        if (order != .eq) {
            if (!left.encoded) l.offset = left.bytes.len;
            if (!right.encoded) r.offset = right.bytes.len;
        }
        // Decode the complete operands even after a mismatch: an invalid
        // base64 suffix must not be hidden by NULL or an early unequal byte.
    }
    return comparisonValue(op, left.sql_null, right.sql_null, order);
}

fn decodeBlob(execution: *Execution, operand: BlobOperand) !Value {
    if (operand.sql_null) return .null;
    if (!operand.encoded) return .{ .blob = operand.bytes };
    if (operand.length > execution.bytes.*) return execution.limit();
    execution.bytes.* -= operand.length;
    const output = try execution.alloc.alloc(u8, operand.length);
    errdefer execution.alloc.free(output);
    var cursor: BlobCursor = .{ .operand = operand };
    var offset: usize = 0;
    while (true) {
        const bytes = try cursor.next(execution);
        if (bytes.len == 0) break;
        if (bytes.len > output.len - offset) return error.InvalidRelationalExpressionInput;
        @memcpy(output[offset..][0..bytes.len], bytes);
        offset += bytes.len;
    }
    if (offset != output.len) return error.InvalidRelationalExpressionInput;
    return .{ .blob = output };
}

fn isComparison(op: Op) bool {
    return switch (op) {
        .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => true,
        else => false,
    };
}

fn compareValues(execution: *Execution, a: Value, b: Value, fold_ascii: bool) !std.math.Order {
    if (a == .sql_array) {
        if (b != .sql_array or fold_ascii) return error.InvalidRelationalExpressionType;
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        return @import("../sql/array_comparison.zig").order(try a.sql_array.view(), try b.sql_array.view(), &execution.numeric, execution.bytes.*) catch |err| return scratch.failure(err);
    }
    return valueOrderWithContext(a, b, fold_ascii, &execution.numeric);
}

test "relational declarations typed array programs share PostgreSQL values across JSON pinned and cold rows" {
    const a = std.testing.allocator;
    const Fixture = struct { entries: []const struct { element_type: Numeric, binary: []const u8 } };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const r = region.allocator();
        const pg = try r.alloc(u8, entry.binary.len / 2);
        _ = try std.fmt.hexToBytes(pg, entry.binary);
        var original = try @import("../sql/array_binary.zig").decode(r, entry.element_type, pg, .{});
        defer original.deinit();
        const canonical = try @import("../sql/array_storage.zig").encodeAlloc(r, original.value, .{});
        const envelope = try @import("../sql/array_wire.zig").encodeAlloc(r, original.value, .{});
        const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_array_expressions = true, .relational_columns = &.{
            .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = entry.element_type },
        } };
        const text = try std.fmt.allocPrint(r, "{{\"op\":\"eq\",\"args\":[{{\"op\":\"column\",\"column\":\"a\"}},{{\"op\":\"literal\",\"type\":\"sql_array\",\"sql_type\":\"{s}\",\"value\":{s}}}]}}", .{ @tagName(entry.element_type), envelope });
        var plan = blk: {
            var input = try std.json.parseFromSlice(std.json.Value, a, text, .{});
            defer input.deinit();
            break :blk try Plan.init(a, table, input.value, .boolean);
        };
        defer plan.deinit();
        // Compilation owns the literal after the parsed request is released.
        const pinned: Value = .{ .sql_array = .{ .element_type = entry.element_type, .bytes = canonical } };
        try std.testing.expect((try plan.evaluate(r, &.{pinned})).boolean);
        var wrong = pinned;
        wrong.sql_array.element_type = if (entry.element_type == .int64) .int32 else .int64;
        try std.testing.expectError(error.InvalidRelationalExpressionInput, plan.evaluate(r, &.{wrong}));
        var document = try std.json.parseFromSlice(std.json.Value, r, try std.fmt.allocPrint(r, "{{\"a\":{s}}}", .{envelope}), .{});
        defer document.deinit();
        try std.testing.expect((try plan.evaluateJson(r, document.value)).boolean);
        var layout = try codec.PhysicalLayout.init(r, table);
        defer layout.deinit();
        const bytes = try codec.serializeOrdinal(r, table.version, table.relational_columns, &.{
            .{ .ordinal = 0, .path = "a", .value_type = .bytes_val, .sql_array_element_type = entry.element_type, .value = .{ .bytes_val = canonical } },
        }, @splat(0));
        const row = try codec.ordinalRowView(bytes, table, &layout);
        try std.testing.expect((try plan.evaluateRow(r, row)).boolean);
        const cast_json = try std.json.parseFromSliceLeaky(std.json.Value, r, try std.json.Stringify.valueAlloc(r, .{
            .op = "cast",
            .type = "sql_array",
            .sql_type = @tagName(entry.element_type),
            .args = .{.{ .op = "column", .column = "a" }},
        }, .{}), .{});
        var identity_cast = try Plan.init(a, table, cast_json, .sql_array);
        defer identity_cast.deinit();
        try std.testing.expectEqual(canonical.ptr, (try identity_cast.evaluate(std.testing.failing_allocator, &.{pinned})).sql_array.bytes.ptr);
        try std.testing.expectEqualSlices(u8, canonical, (try identity_cast.evaluateJson(r, document.value)).sql_array.bytes);
        try std.testing.expectEqualSlices(u8, canonical, (try identity_cast.evaluateRow(std.testing.failing_allocator, row)).sql_array.bytes);
        var allowance: usize = max_allocated_bytes;
        try std.testing.expect((try plan.evaluateBoundRowWithBudget(r, row, &allowance)).boolean);
        var wrong_columns = [_]schema.RelationalColumn{table.relational_columns[0]};
        wrong_columns[0].sql_element_type = wrong.sql_array.element_type;
        var wrong_row = row;
        wrong_row.table_schema.relational_columns = &wrong_columns;
        try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plan.evaluateRow(r, wrong_row));
        try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, identity_cast.evaluateRow(r, wrong_row));
    }
    try std.testing.expectEqual(@as(usize, 11), fixture.value.entries.len);
}

test "relational declarations typed array branches preserve exact identities and own literals under faults" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int64 },
    } };
    const probe: Value = .{ .sql_array = .{ .element_type = .int64, .bytes = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } } };
    for ([_][]const u8{
        \\{"op":"coalesce","args":[{"op":"literal","type":"sql_array","sql_type":"int64","value":null},{"op":"column","column":"a"}]}
        ,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"column","column":"a"},{"op":"literal","type":"sql_array","sql_type":"int64","value":null}]}
    }) |text| {
        var input = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer input.deinit();
        var plan = try Plan.init(a, table, input.value, .sql_array);
        defer plan.deinit();
        try std.testing.expectEqual(Numeric.int64, plan.result_sql_type.?);
        const result = try plan.evaluate(std.testing.failing_allocator, &.{probe});
        try std.testing.expectEqual(probe.sql_array.bytes.ptr, result.sql_array.bytes.ptr);
        try std.testing.expectEqual(Numeric.int64, result.sql_array.element_type);
    }
    for ([_][]const u8{
        \\{"op":"coalesce","args":[{"op":"column","column":"a"},{"op":"literal","type":"sql_array","sql_type":"int32"}]}
        ,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"column","column":"a"},{"op":"literal","type":"sql_array","sql_type":"int32"}]}
        ,
        \\{"op":"in_list","args":[{"op":"column","column":"a"},{"op":"literal","type":"sql_array","sql_type":"int32"}]}
        ,
        \\{"op":"eq","args":[{"op":"column","column":"a"},{"op":"literal","type":"sql_array","sql_type":"int32"}]}
        ,
        \\{"op":"literal","type":"sql_array","value":null}
    }) |text| {
        var input = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer input.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, table, input.value, .sql_array));
    }
    const Faults = struct {
        fn run(alloc: Allocator) !void {
            var plan = blk: {
                var input = try std.json.parseFromSlice(std.json.Value, alloc,
                    \\{"op":"coalesce","args":[{"op":"literal","type":"sql_array","sql_type":"int64"},{"op":"literal","type":"sql_array","sql_type":"int64","value":{"dimensions":[{"length":2,"lower_bound":-2}],"values":["42",null],"sql_nulls":[false,true]}}]}
                , .{});
                defer input.deinit();
                break :blk try Plan.init(alloc, .{}, input.value, .sql_array);
            };
            defer plan.deinit();
            const value = try plan.evaluate(std.testing.failing_allocator, &.{});
            const view = try value.sql_array.view();
            try std.testing.expectEqual(@as(u32, 2), view.count);
            try std.testing.expectEqual(@as(i32, -2), (try view.dimension(0)).lower);
            try std.testing.expect((try view.cell(1)).sql_null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{});
}

test "relational declarations generated NUMERIC arrays normalize dependencies and strictly verify restore" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_array_expressions = true, .requires_numeric_modifiers = true, .relational_columns = &.{
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "g", .path = "g", .column_type = .sql_array, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 3, .scale = 1 } },
    } };
    var declarations = try std.json.parseFromSlice(std.json.Value, a,
        \\[{"column":"g","expression":{"op":"column","column":"a"}}]
    , .{});
    defer declarations.deinit();
    const serialized = try schema.serializeSchema(a, table);
    defer a.free(serialized);
    const set = blk: {
        const owned = try schema.deserializeSchema(a, serialized);
        errdefer schema.freeSchema(a, owned);
        break :blk try Set.createOwned(a, owned, .null, declarations.value);
    };
    defer set.deinit();
    var document = try std.json.parseFromSlice(std.json.Value, a,
        \\{"a":{"dimensions":[{"length":3,"lower_bound":-1}],"values":["12.345",null,"-12.345"],"sql_nulls":[false,true,false]}}
    , .{});
    defer document.deinit();
    const r = document.arena.allocator();
    try set.applyJson(r, &document.value);
    const assigned = document.value.object.get("a").?.object.get("values").?.array.items;
    const generated = document.value.object.get("g").?.object.get("values").?.array.items;
    try std.testing.expectEqualStrings("12.35", assigned[0].string);
    try std.testing.expectEqualStrings("-12.35", assigned[2].string);
    try std.testing.expectEqualStrings("12.4", generated[0].string);
    try std.testing.expectEqualStrings("-12.4", generated[2].string);
    try set.verifyJson(r, document.value);
    var layout = try codec.PhysicalLayout.init(r, set.table);
    defer layout.deinit();
    var allowance: usize = max_allocated_bytes;
    var execution = Execution.init(r, &allowance);
    var cells: [2]codec.Cell = undefined;
    for (set.table.relational_columns, &cells, 0..) |column, *cell, ordinal| {
        const value = try arrayJson(&execution, .numeric, document.value.object.get(column.name).?, null);
        cell.* = .{ .ordinal = @intCast(ordinal), .path = column.name, .value_type = .bytes_val, .sql_array_element_type = .numeric, .value = .{ .bytes_val = value.sql_array.bytes } };
    }
    const bytes = try codec.serializeOrdinal(r, table.version, table.relational_columns, &cells, @splat(0));
    try set.verifyRow(r, try codec.ordinalRowView(bytes, set.table, &layout));
    // A valid coefficient in the wrong target domain must not be repaired by
    // restore, even though normal assignment could round it to the right value.
    cells[1].value.bytes_val = cells[0].value.bytes_val;
    try std.testing.expectError(error.InvalidSqlBinaryRepresentation, codec.serializeOrdinal(r, table.version, table.relational_columns, &cells, @splat(0)));
    // A canonical coefficient in the right domain still needs generated-value
    // verification: type/physical integrity alone cannot prove its derivation.
    generated[0] = .{ .string = "12.3" };
    generated[2] = .{ .string = "-12.3" };
    const wrong_value = try arrayJson(&execution, .numeric, document.value.object.get("g").?, null);
    cells[1].value.bytes_val = wrong_value.sql_array.bytes;
    const forged = try codec.serializeOrdinal(r, table.version, table.relational_columns, &cells, @splat(0));
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRow(r, try codec.ordinalRowView(forged, set.table, &layout)));
    // Exact target identity is checked even for a typed SQL NULL default.
    var wrong = try std.json.parseFromSlice(std.json.Value, a,
        \\[{"column":"g","expression":{"op":"literal","type":"sql_array","sql_type":"int64"}}]
    , .{});
    defer wrong.deinit();
    try std.testing.expectError(error.InvalidRelationalExpressionType, Set.createOwned(a, table, wrong.value, .null));
}

test "relational declarations public array defaults and direct CHECKs match PostgreSQL ordering across cold rows" {
    const a = std.testing.allocator;
    const api = @import("mod.zig");
    const Fixture = struct { entries: []const struct { element_type: Numeric, left_binary: []const u8, right_binary: []const u8, order: std.math.Order } };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_array_order_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const r = region.allocator();
        var envelopes: [2]std.json.Value = undefined;
        var canonical: [2][]u8 = undefined;
        for ([_][]const u8{ entry.left_binary, entry.right_binary }, 0..) |hex, i| {
            const pg = try r.alloc(u8, hex.len / 2);
            _ = try std.fmt.hexToBytes(pg, hex);
            var array = try @import("../sql/array_binary.zig").decode(r, entry.element_type, pg, .{});
            defer array.deinit();
            envelopes[i] = try @import("../sql/array_wire.zig").toJsonLeaky(r, array.value, .{});
            canonical[i] = try @import("../sql/array_storage.zig").encodeAlloc(r, array.value, .{});
        }
        var public = try std.json.parseFromSliceLeaky(std.json.Value, r,
            \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"a","expression":{"op":"literal","type":"sql_array","sql_type":"int64","value":null}}],"checks":[{"name":"bound","column":"a","op":"gte","value":null}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_array","x-antfly-sql-type":"int64"}},"additionalProperties":false}}}}
        , .{});
        const literal = &public.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object;
        try literal.put(r, "sql_type", .{ .string = @tagName(entry.element_type) });
        try literal.put(r, "value", envelopes[0]);
        try public.object.getPtr("checks").?.array.items[0].object.put(r, "value", envelopes[1]);
        const property = &public.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object.getPtr("a").?.object;
        try property.put(r, "x-antfly-sql-type", .{ .string = @tagName(entry.element_type) });
        var validator = try api.CompiledTableValidator.init(a, try std.json.Stringify.valueAlloc(r, public, .{}));
        defer validator.deinit(a);
        const runtime = try api.deriveRuntimeTableSchema(a, validator.schema);
        defer schema.freeSchema(a, runtime);
        try std.testing.expect(runtime.requires_array_expressions);
        try std.testing.expect(runtime.requires_public_schema);
        var document: std.json.Value = .{ .object = .empty };
        try validator.execution.expressions.?.applyJson(r, &document);
        const expected: ?usize = if (entry.order == .lt) 0 else null;
        try std.testing.expectEqual(expected, try validator.execution.checks.?.firstViolationJson(r, document));
        var layout = try codec.PhysicalLayout.init(r, runtime);
        defer layout.deinit();
        const stored = try codec.serializeOrdinal(r, runtime.version, runtime.relational_columns, &.{
            .{ .ordinal = 0, .path = "a", .value_type = .bytes_val, .sql_array_element_type = entry.element_type, .value = .{ .bytes_val = canonical[0] } },
        }, @splat(0));
        try std.testing.expectEqual(expected, try validator.execution.checks.?.firstViolationRow(r, try codec.ordinalRowView(stored, runtime, &layout)));
        try document.object.put(r, "a", .null);
        try std.testing.expectEqual(@as(?usize, null), try validator.execution.checks.?.firstViolationJson(r, document));
        try checks.validateDefinitions(r, runtime, &.{.{ .name = "nonnull", .column = "a", .op = .is_not_null }});
        var nonrelational = runtime;
        nonrelational.storage_mode = .document;
        try std.testing.expectError(error.InvalidRelationalIndexDefinition, checks.validateDefinitions(r, nonrelational, &.{.{ .name = "nonnull", .column = "a", .op = .is_not_null }}));
        try std.testing.expectError(error.UnsupportedRelationalIndexCollation, checks.validateDefinitions(r, runtime, &.{.{ .name = "bad_collation", .column = "a", .op = .is_null, .collation = "ci" }}));
    }
    try std.testing.expectEqual(@as(usize, 36), fixture.value.entries.len);
}

test "relational declarations SQL array DDL publishes precise generated domains defaults and CHECKs" {
    const a = std.testing.allocator;
    var parsed = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE array_domains (a numeric(4,2)[], g numeric(3,1)[] GENERATED ALWAYS AS (a) STORED, missing bigint[] DEFAULT NULL, CHECK (a IS NOT NULL))", .{});
    defer parsed.deinit();
    const json = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, parsed.statement.create_table);
    defer a.free(json);
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, json);
    defer validator.deinit(a);
    const runtime = try @import("mod.zig").deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, runtime);
    try std.testing.expect(runtime.requires_array_expressions);
    try std.testing.expect(runtime.requires_numeric_modifiers);
    var document = try std.json.parseFromSlice(std.json.Value, a,
        \\{"a":{"dimensions":[{"length":2,"lower_bound":-4}],"values":["12.345","-12.345"],"sql_nulls":[false,false]}}
    , .{});
    defer document.deinit();
    const r = document.arena.allocator();
    try validator.execution.expressions.?.applyJson(r, &document.value);
    try std.testing.expectEqualStrings("12.35", document.value.object.get("a").?.object.get("values").?.array.items[0].string);
    try std.testing.expectEqualStrings("12.4", document.value.object.get("g").?.object.get("values").?.array.items[0].string);
    try std.testing.expectEqual(std.json.Value.null, document.value.object.get("missing").?);
    try std.testing.expectEqual(@as(?usize, null), try validator.execution.checks.?.firstViolationJson(r, document.value));
    try validator.execution.expressions.?.verifyJson(r, document.value);
}

test "relational declarations SQL ARRAY constructors activate defaults generated columns and CHECKs" {
    const a = std.testing.allocator;
    var parsed = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE array_constructors (n integer, a integer[] DEFAULT ARRAY[1,NULL,2], g integer[] GENERATED ALWAYS AS (ARRAY[n,NULL,n+1]) STORED, matrix integer[] GENERATED ALWAYS AS (ARRAY[ARRAY[n,NULL],ARRAY[n+1,n+2]]) STORED, CHECK (g = ARRAY[n,NULL,n+1]))", .{});
    defer parsed.deinit();
    const json = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, parsed.statement.create_table);
    defer a.free(json);
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, json);
    defer validator.deinit(a);
    const runtime = try @import("mod.zig").deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, runtime);
    try std.testing.expect(runtime.requires_array_expressions);
    try std.testing.expect(runtime.requires_array_constructors);
    var document = try std.json.parseFromSlice(std.json.Value, a, "{\"n\":12}", .{});
    defer document.deinit();
    const r = document.arena.allocator();
    try validator.execution.expressions.?.applyJson(r, &document.value);
    const arrays = @import("../sql/array_value.zig");
    for ([_]struct { name: []const u8, text: []const u8 }{
        .{ .name = "a", .text = "{1,NULL,2}" },
        .{ .name = "g", .text = "{12,NULL,13}" },
        .{ .name = "matrix", .text = "{{12,NULL},{13,14}}" },
    }) |expected| {
        var decoded = try @import("../sql/array_wire.zig").decode(r, .int32, document.value.object.get(expected.name).?, .{});
        defer decoded.deinit();
        var reference = try @import("../sql/array_text.zig").decode(r, .int32, expected.text, .{});
        defer reference.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try decoded.value.compare(reference.value, &work));
    }
    try std.testing.expectEqual(@as(?usize, null), try validator.execution.checks.?.firstViolationJson(r, document.value));
    try validator.execution.expressions.?.verifyJson(r, document.value);
}

test "relational declarations typed array JSON adapter owns canonical PostgreSQL values and unwinds faults" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    const wire = @import("../sql/array_wire.zig");
    const Fixture = struct { entries: []const struct { element_type: arrays.ElementType, binary: []const u8 } };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Faults = struct {
        fn run(alloc: Allocator, kind: arrays.ElementType, input: std.json.Value, expected: []const u8) !void {
            var allowance: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &allowance);
            const result = arrayJson(&execution, kind, input, null) catch |err| {
                if (err == error.OutOfMemory) try std.testing.expect(execution.numeric.failure == null);
                return err;
            };
            defer alloc.free(result.sql_array.bytes);
            try std.testing.expectEqual(kind, result.sql_array.element_type);
            try std.testing.expectEqualSlices(u8, expected, result.sql_array.bytes);
            // Retained bytes and arena capacity are separate charges.
            try std.testing.expect(allowance <= max_allocated_bytes - result.sql_array.bytes.len);
            if (input.object.get("values").?.array.items.len != 0)
                try std.testing.expect(allowance < max_allocated_bytes - result.sql_array.bytes.len);
            try std.testing.expect(execution.numeric.remaining < 8 * 1024 * 1024);
            try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
            try std.testing.expectEqual(alloc.vtable, execution.numeric.alloc.vtable);
        }
    };
    try std.testing.expectEqual(@as(usize, 11), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const pg = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(pg);
        _ = try std.fmt.hexToBytes(pg, entry.binary);
        var original = try @import("../sql/array_binary.zig").decode(a, entry.element_type, pg, .{});
        defer original.deinit();
        const expected = try storage.encodeAlloc(a, original.value, .{});
        defer a.free(expected);
        var allowance: usize = max_allocated_bytes;
        var execution = Execution.init(a, &allowance);
        const result = blk: {
            var inputs = std.heap.ArenaAllocator.init(a);
            defer inputs.deinit();
            const envelope = try wire.toJsonLeaky(inputs.allocator(), original.value, .{});
            try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{ entry.element_type, envelope, expected });
            break :blk try arrayJson(&execution, entry.element_type, envelope, null);
        };
        defer a.free(result.sql_array.bytes);
        try std.testing.expectEqualSlices(u8, expected, result.sql_array.bytes);
        _ = try storage.validateCanonical(a, entry.element_type, result.sql_array.bytes, .{});
    }
}

test "relational declarations typed array result DOM owns PostgreSQL values and rehomes allocator lifetimes" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    const Fixture = struct { entries: []const struct { element_type: arrays.ElementType, binary: []const u8 } };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn owners(value: std.json.Value, owner: Allocator) !void {
            switch (value) {
                .array => |items| {
                    try std.testing.expectEqual(owner.ptr, items.allocator.ptr);
                    try std.testing.expectEqual(owner.vtable, items.allocator.vtable);
                    for (items.items) |item| try owners(item, owner);
                },
                .object => |object| for (object.values()) |item| try owners(item, owner),
                else => {},
            }
        }
        fn run(backing: Allocator, kind: arrays.ElementType, bytes: []const u8) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const owner = region.allocator();
            var allowance: usize = max_allocated_bytes;
            var execution = Execution.init(owner, &allowance);
            const output = blk: {
                const source = try backing.dupe(u8, bytes);
                defer backing.free(source);
                break :blk arrayJsonOutputLeaky(&execution, .{ .element_type = kind, .bytes = source }) catch |err| {
                    if (err == error.OutOfMemory) try std.testing.expect(execution.numeric.failure == null);
                    try std.testing.expectEqual(owner.ptr, execution.alloc.ptr);
                    try std.testing.expectEqual(owner.vtable, execution.numeric.alloc.vtable);
                    return err;
                };
            };
            try owners(output, owner);
            try std.testing.expect(allowance < max_allocated_bytes);
            const rebound = try arrayJson(&execution, kind, output, null);
            try std.testing.expectEqualSlices(u8, bytes, rebound.sql_array.bytes);
            try std.testing.expectEqual(owner.ptr, execution.alloc.ptr);
            try std.testing.expectEqual(owner.vtable, execution.numeric.alloc.vtable);
        }
    };
    for (fixture.value.entries) |entry| {
        const pg = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(pg);
        _ = try std.fmt.hexToBytes(pg, entry.binary);
        var original = try @import("../sql/array_binary.zig").decode(a, entry.element_type, pg, .{});
        defer original.deinit();
        const bytes = try storage.encodeAlloc(a, original.value, .{});
        defer a.free(bytes);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{ entry.element_type, bytes });
    }
}

test "relational declarations typed array result DOM admission is sticky across scratch and retained quotas" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    var object = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":[1,null,true],\"b\":\"text\"}", .{});
    defer object.deinit();
    const cells: [20]arrays.Element = @splat(arrays.Element.json(object.value));
    const bytes = try storage.encodeAlloc(a, .{ .element_type = .jsonb, .dimensions = &.{.{ .length = cells.len, .lower = -7 }}, .elements = &cells }, .{});
    defer a.free(bytes);
    var failures: usize = 0;
    var successes: usize = 0;
    for ([_]usize{ 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 }) |initial| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        var allowance = initial;
        var execution = Execution.init(region.allocator(), &allowance);
        if (arrayJsonOutputLeaky(&execution, .{ .element_type = .jsonb, .bytes = bytes })) |_| {
            successes += 1;
            try std.testing.expect(allowance < initial);
        } else |err| {
            try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
            failures += 1;
            allowance = max_allocated_bytes;
            execution.numeric.remaining = 8 * 1024 * 1024;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, execution.charge(0));
        }
    }
    try std.testing.expect(failures != 0 and successes != 0);
}

test "relational declarations typed array JSON adapter preserves sticky cancellation and byte admission" {
    const a = std.testing.allocator;
    var input = try std.json.parseFromSlice(std.json.Value, a,
        \\{"dimensions":[{"length":2,"lower_bound":-7}],"values":["9007199254740993",null],"sql_nulls":[false,true]}
    , .{});
    defer input.deinit();
    var allowance: usize = 1;
    var execution = Execution.init(a, &allowance);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, arrayJson(&execution, .int64, input.value, null));
    allowance = max_allocated_bytes;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, arrayJson(&execution, .int64, input.value, null));
    execution = Execution.init(a, &allowance);
    const Cancel = struct {
        fn poll(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    execution.numeric.checkpoint = Cancel.poll;
    try std.testing.expectError(error.Canceled, arrayJson(&execution, .int64, input.value, null));
    execution.numeric.checkpoint = null;
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.Canceled, arrayJson(&execution, .int64, .null, null));
    execution = Execution.init(a, &allowance);
    try std.testing.expect((try arrayJson(&execution, .int64, .null, null)) == .null);
}

test "relational declarations typed row values compare canonical arrays under shared scratch admission" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    var object = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":[1,null,true],\"b\":\"text\"}", .{});
    defer object.deinit();
    const elements: [50]arrays.Element = @splat(arrays.Element.json(object.value));
    const json = try storage.encodeAlloc(a, try arrays.Value.init(.jsonb, &.{.{ .length = elements.len, .lower = -9 }}, &elements, .{}), .{});
    defer a.free(json);
    const value: Value = .{ .sql_array = .{ .element_type = .jsonb, .bytes = json } };
    const Run = struct {
        fn run(alloc: Allocator, input: Value) !void {
            var allowance: usize = 64 * 1024;
            var execution = Execution.init(alloc, &allowance);
            try std.testing.expectEqual(std.math.Order.eq, try compareValues(&execution, input, input, false));
            try std.testing.expect(allowance < 64 * 1024);
            try std.testing.expect(execution.numeric.remaining < 8 * 1024 * 1024);
            try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
            try std.testing.expectEqual(alloc.vtable, execution.numeric.alloc.vtable);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{value});
    var allowance: usize = 16 * 1024;
    var execution = Execution.init(a, &allowance);
    var comparisons: usize = 0;
    while (comparisons < 128) : (comparisons += 1) {
        _ = compareValues(&execution, value, value, false) catch |err| {
            try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
            break;
        };
    }
    try std.testing.expect(comparisons > 0 and comparisons < 128);
    allowance = max_allocated_bytes;
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, compareValues(&execution, value, value, false));

    const primitive = try storage.encodeAlloc(a, try arrays.Value.init(.int64, &.{.{ .length = 2 }}, &.{
        arrays.Element.json(.{ .integer = 1 }), arrays.Element.json(.{ .integer = 2 }),
    }, .{}), .{});
    defer a.free(primitive);
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    allowance = 0;
    execution = Execution.init(denied.allocator(), &allowance);
    const integers: Value = .{ .sql_array = .{ .element_type = .int64, .bytes = primitive } };
    try std.testing.expectEqual(std.math.Order.eq, try compareValues(&execution, integers, integers, false));
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
    try std.testing.expectError(error.InvalidRelationalExpressionType, compareValues(&execution, integers, integers, true));
}

fn valueOrder(a: Value, b: Value, fold_ascii: bool) !std.math.Order {
    var context: exact.Context = .{ .alloc = std.heap.page_allocator };
    return valueOrderWithContext(a, b, fold_ascii, &context);
}

fn valueOrderWithContext(a: Value, b: Value, fold_ascii: bool, context: *exact.Context) !std.math.Order {
    return switch (a) {
        .null => unreachable,
        .string => |left| orderBytes(left, b.string, fold_ascii, context),
        .blob => |value| orderBytes(value, b.blob, false, context),
        .numeric => |bytes| blk: {
            const View = @import("../common/sql_numeric_layout.zig").View;
            const left = try View.openWithBudget(bytes, .{}, context);
            const right = try View.openWithBudget(b.numeric, .{}, context);
            break :blk try left.order(right, context);
        },
        .boolean => |value| std.math.order(@intFromBool(value), @intFromBool(b.boolean)),
        .integer => |value| std.math.order(value, b.integer),
        .number => |value| std.math.order(value, b.number),
        .datetime => |value| std.math.order(value, b.datetime),
        .sql_array => |array| @import("../sql/array_comparison.zig").order(try array.view(), try b.sql_array.view(), context, max_allocated_bytes),
        .json => |json| blk: {
            var work: @import("../sql/json_order.zig").Budget = .{ .shared = context };
            break :blk try @import("../sql/json_order.zig").compare(json.value, b.json.value, &work, 0);
        },
    };
}

/// Borrowed comparisons retain vectorized binary scans, with bounded work
/// between cancellation polls. CPU admission is shared with all row programs;
/// neither a long prefix nor ASCII collation starts a fresh comparison quota.
fn orderBytes(left: []const u8, right: []const u8, fold_ascii: bool, context: *exact.Context) !std.math.Order {
    const count = @min(left.len, right.len);
    var offset: usize = 0;
    while (offset < count) {
        const end = offset + @min(count - offset, 256);
        try context.charge(end - offset);
        const order = if (fold_ascii) blk: {
            for (left[offset..end], right[offset..end]) |x, y| {
                const compared = std.math.order(std.ascii.toLower(x), std.ascii.toLower(y));
                if (compared != .eq) break :blk compared;
            }
            break :blk std.math.Order.eq;
        } else std.mem.order(u8, left[offset..end], right[offset..end]);
        if (order != .eq) return order;
        offset = end;
    }
    return std.math.order(left.len, right.len);
}

test "relational declarations blob views decode boundaries and validate complete comparisons" {
    const a = std.testing.allocator;
    var payload: [769]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @truncate(i);
    var buffer: [1028]u8 = undefined;
    for ([_]usize{ 0, 1, 2, 3, 191, 192, 193, 384, 385, 769 }) |size| {
        const encoded = std.base64.standard.Encoder.encode(&buffer, payload[0..size]);
        const operand = try BlobOperand.fromJson(.{ .string = encoded });
        var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var allowance: usize = max_allocated_bytes;
        var execution = Execution.init(denied.allocator(), &allowance);
        const raw = try BlobOperand.fromValue(.{ .blob = payload[0..size] });
        try std.testing.expect((try compareBlobs(&execution, .eq, operand, raw)).boolean);
        try std.testing.expect((try compareBlobs(&execution, .eq, operand, operand)).boolean);
        try std.testing.expect((try compareBlobs(&execution, .eq, operand, .{})) == .null);
        try std.testing.expect((try compareBlobs(&execution, .is_distinct, operand, .{})).boolean);
        const short = try BlobOperand.fromValue(.{ .blob = payload[0 .. size / 2] });
        const expected = std.mem.order(u8, payload[0..size], payload[0 .. size / 2]);
        try std.testing.expectEqual(expected == .gt, (try compareBlobs(&execution, .gt, operand, short)).boolean);
        try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
        allowance = max_allocated_bytes;
        execution = Execution.init(a, &allowance);
        const decoded = try decodeBlob(&execution, operand);
        defer a.free(decoded.blob);
        try std.testing.expectEqualSlices(u8, payload[0..size], decoded.blob);
    }
    var malformed: [512]u8 = @splat('A');
    const different = try BlobOperand.fromValue(.{ .blob = &.{255} });
    for ([_]usize{ 254, 511 }) |position| {
        malformed[position] = if (position == 254) '=' else '!';
        const operand = try BlobOperand.fromJson(.{ .string = &malformed });
        var allowance: usize = max_allocated_bytes;
        var execution = Execution.init(a, &allowance);
        try std.testing.expectError(error.InvalidRelationalExpressionInput, compareBlobs(&execution, .ne, operand, different));
        try std.testing.expectError(error.InvalidRelationalExpressionInput, compareBlobs(&execution, .eq, operand, .{}));
        try std.testing.expectError(error.InvalidRelationalExpressionInput, decodeBlob(&execution, operand));
        malformed[position] = 'A';
    }
}

fn finite(value: f64) !Value {
    if (!std.math.isFinite(value)) return error.RelationalExpressionOverflow;
    // SQL scalar results retain signed zero. Ordered keys may normalize its
    // equality identity, but that must not erase expression result semantics.
    return .{ .number = value };
}

fn valueHasKind(value: Value, kind: Kind) bool {
    return switch (value) {
        .null => true,
        inline else => |_, tag| std.mem.eql(u8, @tagName(tag), @tagName(kind)),
    };
}

fn sameOperandType(a: Node, b: Node) bool {
    return a.kind == b.kind and (a.kind != .sql_array or a.sql_type == b.sql_type);
}

/// One collation contract for logical CHECKs, expression operands and ordered
/// scalar keys. Non-string domains never inherit a string collation implicitly.
pub fn foldAsciiCollation(kind: Kind, collation: []const u8) !bool {
    if (kind != .string) return error.UnsupportedRelationalIndexCollation;
    if (std.ascii.eqlIgnoreCase(collation, "ci") or
        std.ascii.eqlIgnoreCase(collation, "case_insensitive") or
        std.ascii.eqlIgnoreCase(collation, "antfly.case_insensitive")) return true;
    if (std.ascii.eqlIgnoreCase(collation, "C") or
        std.ascii.eqlIgnoreCase(collation, "POSIX") or
        std.ascii.eqlIgnoreCase(collation, "binary")) return false;
    return error.UnsupportedRelationalIndexCollation;
}

fn preparedSourceMask(nodes: []const Node) u8 {
    var mask: u8 = 0;
    for (nodes) |node| {
        if (node.op != .column) continue;
        mask |= switch (node.kind) {
            .numeric, .sql_array => @as(u8, 1),
            .blob, .json => @as(u8, 3),
            else => @as(u8, 0),
        };
    }
    return mask;
}

const JsonLiteral = struct { value: Value, encoded_bytes: usize, fingerprint: [32]u8 };

fn prepareJsonLiteral(execution: *Execution, value: std.json.Value) !JsonLiteral {
    const order = @import("../sql/json_order.zig");
    var work: order.Budget = .{ .shared = &execution.numeric };
    order.validateJsonbDomain(value, &work, 0) catch |err| return switch (err) {
        error.SqlTypeMismatch, error.SqlInvalidTextRepresentation, error.SqlNumericOutOfRange, error.SqlJsonUnicodeEscape, error.SqlInvalidTextEncoding => error.InvalidRelationalExpressionType,
        else => executionFailure(err),
    };
    const identity = blk: {
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        const encoded = @import("../storage/db/document_content_hash.zig").canonicalJsonbValueAlloc(execution.alloc, value, &work, max_output_bytes) catch |err| {
            if (err == error.RelationalRowResultTooLarge) return execution.limit();
            if (err == error.InvalidBatchRequest or err == error.SqlNumericOutOfRange) return error.InvalidRelationalExpressionType;
            return scratch.failure(err);
        };
        if (encoded.len > max_output_bytes) return execution.limit();
        try execution.charge(encoded.len);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly durable JSON literal display v1\x00");
        hash.update(encoded);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        break :blk .{ .fingerprint = digest, .encoded_bytes = encoded.len };
    };
    const source: @import("../sql/row_value.zig").Json = .{ .value = value };
    const retained = try retainPreparedResult(execution, .{ .json = &source }, false);
    // Plan is movable until installed in its schema epoch. Its immutable tree
    // must never embed an allocator pointing at the compiler's stack arena.
    // Plan.deinit frees the complete arena; per-container mutation/free is not
    // part of the immutable literal interface.
    order.rehomeArrayAllocators(&@constCast(retained.json).value, .failing, &work, 0) catch |err| return executionFailure(err);
    return .{ .value = retained, .encoded_bytes = identity.encoded_bytes, .fingerprint = identity.fingerprint };
}

const Compiler = struct {
    alloc: Allocator,
    execution: *Execution,
    table: schema.TableSchema,
    nodes: std.ArrayList(Node) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    hash: std.crypto.hash.Blake3 = std.crypto.hash.Blake3.init(.{}),
    visited: usize = 0,
    allow_uuid_defaults: bool = false,
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
            if (!acceptsField(op, name)) return error.InvalidRelationalExpression;
        }
        self.frame(op_value.string);
        var node: Node = .{ .op = op, .kind = undefined };
        switch (op) {
            .uuidv4, .uuidv7 => {
                if (!self.allow_uuid_defaults or !self.table.requires_uuid_defaults) return error.InvalidRelationalExpressionType;
                node.kind = .string;
                node.sql_type = .uuid;
                if (input.object.get("shift")) |shift| {
                    if (op != .uuidv7 or shift != .string or shift.string.len == 0 or shift.string.len > 128) return error.InvalidRelationalExpression;
                    const value = try @import("../sql/temporal_text.zig").parseCanonical(.interval, shift.string);
                    node.uuid_shift = value.interval;
                    var encoded: [16]u8 = undefined;
                    self.frame(try @import("../sql/temporal_value.zig").encode(value, &encoded));
                }
            },
            .literal => {
                const kind = input.object.get("type") orelse return error.InvalidRelationalExpression;
                if (kind != .string) return error.InvalidRelationalExpression;
                node.kind = std.meta.stringToEnum(Kind, kind.string) orelse return error.InvalidRelationalExpression;
                const value = input.object.get("value") orelse .null;
                const marker = input.object.get("json_null");
                if (marker) |present| {
                    if (node.kind != .json or present != .bool or !self.table.requires_json_expressions) return error.InvalidRelationalExpressionType;
                    if (present.bool and value != .null) return error.InvalidRelationalExpression;
                }
                const json_null = if (marker) |present| present.bool else false;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number, .numeric, .sql_array => {},
                    .json => if ((value != .null or json_null) and !self.table.requires_json_expressions) return error.InvalidRelationalExpressionType,
                    else => return error.InvalidRelationalExpressionType,
                }
                if (node.kind == .blob and value == .string and value.string.len > std.base64.standard.Encoder.calcSize(max_output_bytes)) return error.RelationalExpressionBudgetExceeded;
                if (node.kind == .sql_array) {
                    const identity = input.object.get("sql_type") orelse return error.InvalidRelationalExpressionType;
                    if (identity != .string) return error.InvalidRelationalExpressionType;
                    node.sql_type = std.meta.stringToEnum(Numeric, identity.string) orelse return error.InvalidRelationalExpressionType;
                }
                var json_literal: ?JsonLiteral = null;
                if (node.kind == .json and (value != .null or json_null)) json_literal = try prepareJsonLiteral(self.execution, value);
                node.literal = (if (json_literal) |literal| literal.value else if (node.kind == .sql_array) arrayJson(self.execution, node.sql_type.?, value, null) else if (node.kind == .numeric) numericJson(self.execution, value) else scalarJson(self.alloc, node.kind, value, true)) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    error.SqlProgramLimitExceeded, error.RelationalExpressionBudgetExceeded => return err,
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
                    .string, .blob, .numeric => |bytes| bytes.len,
                    .sql_array => |array| array.bytes.len,
                    .json => json_literal.?.encoded_bytes,
                    .datetime => 16,
                    else => 8,
                };
                if (self.literal_bytes > max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
                self.frame(@tagName(node.literal));
                var bytes: [8]u8 = undefined;
                switch (node.literal) {
                    .null => {},
                    .string, .blob, .numeric => |value_bytes| self.frame(value_bytes),
                    .boolean => |boolean| self.hash.update(&.{@intFromBool(boolean)}),
                    .integer => |integer| {
                        std.mem.writeInt(i64, &bytes, integer, .little);
                        self.hash.update(&bytes);
                    },
                    .datetime => |datetime| {
                        var signed: [16]u8 = undefined;
                        std.mem.writeInt(i128, &signed, datetime, .little);
                        self.hash.update(&signed);
                    },
                    .number => |number| {
                        std.mem.writeInt(u64, &bytes, @bitCast(number), .little);
                        self.hash.update(&bytes);
                    },
                    .sql_array => |array| self.frame(array.bytes),
                    // Program identity includes observable JSONB display
                    // scale, unlike the row's semantic content hash.
                    .json => self.frame(&json_literal.?.fingerprint),
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
                node.sql_type = self.table.relational_columns[node.ordinal].sql_element_type;
                if (node.kind == .numeric and node.sql_type != .numeric) return error.InvalidRelationalExpressionType;
                if (node.kind == .sql_array and node.sql_type == null) return error.InvalidRelationalExpressionType;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number, .numeric, .sql_array => {},
                    .json => if (!self.table.requires_json_expressions) return error.InvalidRelationalExpressionType,
                    else => return error.InvalidRelationalExpressionType,
                }
                if (std.mem.indexOfScalar(u32, self.dependencies.items, node.ordinal) == null) try self.dependencies.append(self.alloc, node.ordinal);
                self.frame(name.string);
                self.frame(@tagName(node.kind));
                if (self.table.relational_columns[node.ordinal].sql_element_type) |kind| {
                    self.frame("precise SQL builtin type");
                    self.frame(@tagName(kind));
                }
                if (self.table.relational_columns[node.ordinal].numeric_modifier) |modifier| {
                    self.frame("SQL NUMERIC column modifier v1");
                    var identity: [4]u8 = undefined;
                    std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
                    std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
                    self.frame(&identity);
                }
            },
            .array => {
                const identity = input.object.get("sql_type") orelse return error.InvalidRelationalExpressionType;
                if (identity != .string) return error.InvalidRelationalExpressionType;
                const kind = std.meta.stringToEnum(Numeric, identity.string) orelse return error.InvalidRelationalExpressionType;
                const args = input.object.get("args") orelse return error.InvalidRelationalExpression;
                if (args != .array or !acceptsArity(op, args.array.items.len)) return error.InvalidRelationalExpression;
                const children = try self.alloc.alloc(u16, args.array.items.len);
                for (args.array.items, children) |arg, *child| child.* = try self.compile(arg, depth + 1);
                const nested = children.len != 0 and self.nodes.items[children[0]].kind == .sql_array;
                for (children) |child| {
                    const operand = self.nodes.items[child];
                    if (nested) {
                        if (operand.kind != .sql_array or operand.sql_type != kind) return error.InvalidRelationalExpressionType;
                    } else {
                        if (operand.kind != arrayScalarKind(kind)) return error.InvalidRelationalExpressionType;
                        if ((operand.kind == .integer or operand.kind == .number or operand.kind == .numeric) and numericIdentity(operand) != kind) return error.InvalidRelationalExpressionType;
                    }
                }
                self.hash.update(&.{@intCast(children.len)});
                node.kind = .sql_array;
                node.children = children;
            },
            else => {
                const args = input.object.get("args") orelse return error.InvalidRelationalExpression;
                if (args != .array) return error.InvalidRelationalExpression;
                const length = args.array.items.len;
                if (!acceptsArity(op, length)) return error.InvalidRelationalExpression;
                self.hash.update(&.{@intCast(length)});
                const children = try self.alloc.alloc(u16, length);
                for (args.array.items, children) |arg, *child| child.* = try self.compile(arg, depth + 1);
                node.children = children;
                const operand = self.nodes.items[children[if (op == .case_when) children.len - 1 else 0]];
                node.kind = operand.kind;
                if (op == .case_when) {
                    for (children[0 .. children.len - 1], 0..) |child, i| {
                        const candidate = self.nodes.items[child];
                        if (i % 2 == 0) {
                            if (candidate.kind != .boolean) return error.InvalidRelationalExpressionType;
                        } else if (!sameOperandType(candidate, operand)) return error.InvalidRelationalExpressionType;
                    }
                } else if (jsonFunction(op)) {
                    if (!self.table.requires_json_expressions or operand.kind != .json) return error.InvalidRelationalExpressionType;
                    for (children[1..]) |child| if (self.nodes.items[child].kind != .string) return error.InvalidRelationalExpressionType;
                    node.kind = switch (op) {
                        .jsonb_extract_path => .json,
                        .jsonb_array_length => .integer,
                        else => .string,
                    };
                    if (op == .jsonb_array_length) node.sql_type = .int32;
                } else if (numericFunction(op) != null) {
                    if (operand.kind != .integer and operand.kind != .number and operand.kind != .numeric) return error.InvalidRelationalExpressionType;
                    if (operand.kind == .integer and op != .abs) return error.InvalidRelationalExpressionType;
                    if (operand.kind == .number and op != .abs and numericIdentity(operand) != .float64) return error.InvalidRelationalExpressionType;
                    if (children.len == 2 and (operand.kind != .numeric or self.nodes.items[children[1]].kind != .integer or numericIdentity(self.nodes.items[children[1]]) != .int32)) return error.InvalidRelationalExpressionType;
                    node.sql_type = numericIdentity(operand);
                } else for (children[1..]) |child| if (!sameOperandType(self.nodes.items[child], operand)) return error.InvalidRelationalExpressionType;
                if (isComparison(op) or op == .in_list or op == .not_in_list) {
                    if (input.object.get("collation")) |collation| {
                        if (collation != .string) return error.UnsupportedRelationalIndexCollation;
                        node.fold_ascii = try foldAsciiCollation(node.kind, collation.string);
                    }
                    self.hash.update(&.{@intFromBool(node.fold_ascii)});
                    node.kind = .boolean;
                }
                switch (op) {
                    .cast => {
                        if (node.kind != .integer and node.kind != .number and node.kind != .numeric and node.kind != .sql_array) return error.InvalidRelationalExpressionType;
                        const kind = input.object.get("type") orelse return error.InvalidRelationalExpression;
                        if (kind != .string) return error.InvalidRelationalExpression;
                        node.kind = std.meta.stringToEnum(Kind, kind.string) orelse return error.InvalidRelationalExpressionType;
                        if ((operand.kind == .sql_array) != (node.kind == .sql_array)) return error.InvalidRelationalExpressionType;
                        if (input.object.get("sql_type") == null) return error.InvalidRelationalExpression;
                    },
                    .add, .subtract, .multiply, .divide, .negate, .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt => if (node.kind != .integer and node.kind != .number and node.kind != .numeric) return error.InvalidRelationalExpressionType,
                    // PostgreSQL has no float4/float8 remainder operator.
                    .modulo => if (node.kind != .integer and node.kind != .numeric) return error.InvalidRelationalExpressionType,
                    .concat, .lower_ascii, .upper_ascii, .replace, .md5 => if (node.kind != .string) return error.InvalidRelationalExpressionType,
                    .coalesce, .case_when => {
                        if (node.kind == .sql_array) node.sql_type = operand.sql_type;
                        if (node.kind == .integer or node.kind == .number or node.kind == .numeric) {
                            const identity = numericIdentity(self.nodes.items[children[children.len - 1]]);
                            const same = for (children, 0..) |child, i| {
                                if (op == .case_when and i % 2 == 0 and i + 1 < children.len) continue;
                                if (numericIdentity(self.nodes.items[child]) != identity) break false;
                            } else true;
                            if (same) node.sql_type = identity;
                        }
                    },
                    .@"and", .@"or", .not => if (node.kind != .boolean) return error.InvalidRelationalExpressionType,
                    .is_null, .is_not_null => node.kind = .boolean,
                    .in_list, .not_in_list => node.kind = .boolean,
                    .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => {},
                    .jsonb_typeof, .jsonb_array_length, .jsonb_extract_path, .jsonb_extract_path_text => {},
                    else => unreachable,
                }
            },
        }
        if (input.object.get("sql_type")) |identity| {
            if (identity != .string) return error.InvalidRelationalExpressionType;
            const typed = std.meta.stringToEnum(Numeric, identity.string) orelse return error.InvalidRelationalExpressionType;
            // Function identity follows its resolved operand. Assignment or
            // width conversion is an explicit surrounding cast, not an
            // undeclared float4 overload or narrowed integer ABS operation.
            if (numericFunction(op) != null and typed != numericIdentity(self.nodes.items[node.children[0]])) return error.InvalidRelationalExpressionType;
            if (node.kind != .sql_array and (node.kind != .integer or !casts.integral(typed)) and (node.kind != .number or !casts.floating(typed)) and (node.kind != .numeric or typed != .numeric)) return error.InvalidRelationalExpressionType;
            if (node.kind == .sql_array and op == .cast and self.nodes.items[node.children[0]].sql_type != typed) return error.InvalidRelationalExpressionType;
            node.sql_type = typed;
            if (op == .literal and node.kind != .numeric and node.kind != .sql_array) node.literal = numericCast(node.literal, typed) catch return error.InvalidRelationalExpressionType;
            // Do not change fingerprints of historical unannotated programs.
            // Typed operations/casts have a distinct, immutable semantic key.
            self.frame(if (node.kind == .sql_array) "SQL array expression identity v1" else "SQL numeric expression identity v1");
            self.frame(@tagName(typed));
            if (op == .add or op == .subtract or op == .multiply or op == .divide or op == .modulo or op == .negate) {
                for (node.children) |child| if (numericIdentity(self.nodes.items[child]) != typed) return error.InvalidRelationalExpressionType;
            }
        }
        if (input.object.get("numeric_modifier")) |constraint| {
            if (node.op != .cast or (node.kind != .numeric and node.kind != .sql_array) or node.sql_type != .numeric) return error.InvalidRelationalExpressionType;
            const modifier = @import("../sql/numeric_storage.zig").modifierFromJson(constraint) catch return error.InvalidRelationalExpressionType;
            node.numeric_modifier = modifier;
            self.frame("SQL NUMERIC cast modifier v1");
            var identity: [4]u8 = undefined;
            std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
            std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
            self.frame(&identity);
        }
        if (op == .replace) {
            const search = self.nodes.items[node.children[1]];
            if (search.op == .literal and search.literal == .string) {
                const bytes = std.math.mul(usize, search.literal.string.len, @sizeOf(usize)) catch return error.RelationalExpressionBudgetExceeded;
                if (bytes > max_allocated_bytes -| self.literal_bytes) return error.RelationalExpressionBudgetExceeded;
                self.literal_bytes += bytes;
                const links = try self.alloc.alloc(usize, search.literal.string.len);
                node.replace_pattern = try text_kernel.Pattern.init(search.literal.string, links, &self.execution.numeric);
            }
        }
        const index: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.alloc, node);
        return index;
    }
};

fn arrayScalarKind(kind: Numeric) Kind {
    return switch (kind) {
        .int16, .int32, .int64 => .integer,
        .float32, .float64 => .number,
        .numeric => .numeric,
        .boolean => .boolean,
        .text, .uuid => .string,
        .jsonb => .json,
    };
}

fn numericIdentity(node: Node) Numeric {
    return node.sql_type orelse if (node.kind == .integer) .int64 else if (node.kind == .numeric) .numeric else .float64;
}

test "relational declarations exact NUMERIC constraints own literals and bound repeated row scratch" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    const Run = struct {
        fn run(alloc: Allocator) !void {
            const plan = blk: {
                var json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"minimum\":9007199254740993.25,\"exclusiveMaximum\":9007199254740993.26,\"multipleOf\":0.0001,\"enum\":[9007199254740993.2500,9007199254740993.25,\"9007199254740993.25\"]}", .{ .parse_numbers = false });
                defer json.deinit();
                break :blk (try constraints.Plan.create(alloc, json.value.object)).?;
            };
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 1), plan.enumeration.count());
            var execution: constraints.Execution = undefined;
            execution.init(alloc);
            defer execution.deinit();
            try execution.validateJson(plan, .{ .number_string = "9007199254740993.250000" });
            execution.validateJson(plan, .{ .number_string = "9007199254740993.2499" }) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidBatchRequest, err);
                return;
            };
            return error.TestExpectedError;
        }
    };
    try Run.run(a);
    try std.testing.checkAllAllocationFailures(a, Run.run, .{});

    var json = try std.json.parseFromSlice(std.json.Value, a, "{\"minimum\":1e-1000,\"maximum\":1e1000,\"multipleOf\":1e-1000}", .{ .parse_numbers = false });
    defer json.deinit();
    const plan = (try constraints.Plan.create(a, json.value.object)).?;
    defer plan.deinit();
    var execution: constraints.Execution = undefined;
    execution.init(a);
    defer execution.deinit();
    try execution.validateJson(plan, .{ .number_string = "1e-999" });
    const peak = execution.memory.peak;
    for (0..100) |_| try execution.validateJson(plan, .{ .number_string = "1e-999" });
    try std.testing.expectEqual(peak, execution.memory.peak);
    try std.testing.expectError(error.InvalidBatchRequest, execution.validateJson(plan, .{ .number_string = "1e-1001" }));
    execution.context.remaining = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, execution.validateJson(plan, .{ .integer = 1 }));
    execution.context.remaining = 10000;
    try std.testing.expectError(error.SqlProgramLimitExceeded, execution.validateJson(plan, .{ .integer = 1 }));
}

test "relational declarations exact NUMERIC constraint predicates match PostgreSQL" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    const Case = struct { schema: []const u8, value: []const u8, expected: bool };
    const Fixture = struct { reference: []const u8, entries: []const Case };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_numeric_constraints_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var definition = try std.json.parseFromSlice(std.json.Value, a, case.schema, .{ .parse_numbers = false });
        defer definition.deinit();
        const plan = (try constraints.Plan.create(a, definition.value.object)).?;
        defer plan.deinit();
        var execution: constraints.Execution = undefined;
        execution.init(a);
        defer execution.deinit();
        execution.validateJson(plan, .{ .number_string = case.value }) catch |err| {
            if (err != error.InvalidBatchRequest or case.expected) {
                std.debug.print("NUMERIC constraint: schema={s} value={s} expected={} error={s}\n", .{ case.schema, case.value, case.expected, @errorName(err) });
                return err;
            }
            continue;
        };
        try std.testing.expect(case.expected);
    }
}

test "relational declarations NUMERIC constraints borrow enum comparisons and preserve cancellation admission" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    var definition = try std.json.parseFromSlice(std.json.Value, a, "{\"minimum\":9007199254740993.25,\"enum\":[9007199254740993.25000,\"NaN\"]}", .{ .parse_numbers = false });
    defer definition.deinit();
    const plan = (try constraints.Plan.create(a, definition.value.object)).?;
    defer plan.deinit();
    var parse_context: exact.Context = .{ .alloc = a };
    var parsed = try exact.parse(&parse_context, "9007199254740993.25000000");
    defer parsed.deinit();
    var borrowed: exact.Context = .{ .alloc = std.testing.failing_allocator };
    for (0..10000) |_| try plan.validate(&borrowed, parsed.value);

    var execution: constraints.Execution = undefined;
    execution.init(a);
    defer execution.deinit();
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    execution.context.checkpoint = Cancel.check;
    try std.testing.expectError(error.Canceled, execution.validateJson(plan, .{ .integer = 1 }));
    execution.context.checkpoint = null;
    try std.testing.expectError(error.Canceled, execution.validateJson(plan, .{ .integer = 1 }));

    var limited: constraints.Execution = undefined;
    limited.init(a);
    defer limited.deinit();
    limited.memory.limit = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.validateJson(plan, .{ .integer = 1 }));
    limited.memory.limit = constraints.max_bytes;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.validateJson(plan, .{ .integer = 1 }));
    for ([_][]const u8{ "{\"multipleOf\":0}", "{\"multipleOf\":-0.001}", "{\"minimum\":\"1\"}", "{\"minimum\":1e131072}" }) |invalid| {
        var json = try std.json.parseFromSlice(std.json.Value, a, invalid, .{ .parse_numbers = false });
        defer json.deinit();
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, constraints.Plan.create(a, json.value.object));
    }
}

test "relational declarations exact NUMERIC SQL lowering and native programs match PostgreSQL" {
    const a = std.testing.allocator;
    const Case = struct {
        op: []const u8,
        left: []const u8,
        right: ?[]const u8 = null,
        expected: ?std.json.Value = null,
        @"error": ?[]const u8 = null,
    };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_exact_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const table: schema.TableSchema = .{ .storage_mode = .relational };
    var exercised: usize = 0;
    for (fixture.value.entries) |entry| {
        const op: []const u8 = if (std.mem.eql(u8, entry.op, "remainder")) "modulo" else entry.op;
        const integer_cast = std.mem.eql(u8, op, "int16") or std.mem.eql(u8, op, "int32") or std.mem.eql(u8, op, "int64");
        const ordering = std.mem.eql(u8, op, "order");
        if (!std.mem.eql(u8, op, "add") and !std.mem.eql(u8, op, "subtract") and
            !std.mem.eql(u8, op, "multiply") and !std.mem.eql(u8, op, "divide") and
            !std.mem.eql(u8, op, "modulo") and !integer_cast and !ordering) continue;
        const Literal = struct { op: []const u8 = "literal", type: []const u8 = "numeric", value: []const u8 };
        const text = if (integer_cast) try std.json.Stringify.valueAlloc(a, .{
            .op = "cast",
            .type = "integer",
            .sql_type = op,
            .args = [1]Literal{.{ .value = entry.left }},
        }, .{}) else try std.json.Stringify.valueAlloc(a, .{
            .op = if (ordering) "lt" else op,
            .args = [2]Literal{ .{ .value = entry.left }, .{ .value = entry.right.? } },
        }, .{});
        defer a.free(text);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        var plan = try Plan.init(a, table, parsed.value, if (integer_cast) .integer else if (ordering) .boolean else .numeric);
        defer plan.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const token: []const u8 = if (std.mem.eql(u8, op, "add")) "+" else if (std.mem.eql(u8, op, "subtract")) "-" else if (std.mem.eql(u8, op, "multiply")) "*" else if (std.mem.eql(u8, op, "divide")) "/" else if (ordering) "<" else "%";
        const sql = if (integer_cast)
            try std.fmt.allocPrint(owned, "CAST(CAST('{s}' AS numeric) AS {s})", .{ entry.left, if (std.mem.eql(u8, op, "int16")) "smallint" else if (std.mem.eql(u8, op, "int32")) "integer" else "bigint" })
        else
            try std.fmt.allocPrint(owned, "CAST('{s}' AS numeric) {s} CAST('{s}' AS numeric)", .{ entry.left, token, entry.right.? });
        var compiled = try @import("../sql/compiler.zig").compileScalar(owned, sql, .{});
        defer compiled.deinit();
        const lowered = try @import("../sql/schema_expression.zig").lowerColumns(owned, &.{}, compiled.expression, null);
        var sql_plan = try Plan.init(a, table, lowered.expression, plan.result_kind);
        defer sql_plan.deinit();
        for ([_]*Plan{ &plan, &sql_plan }) |candidate| {
            if (entry.@"error") |state| {
                const failure: anyerror = if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "0A000")) error.SqlFeatureNotSupported else return error.TestUnexpectedSqlstate;
                try std.testing.expectError(failure, candidate.evaluate(owned, &.{}));
            } else {
                const result = try candidate.evaluate(owned, &.{});
                if (integer_cast) {
                    const wanted = if (entry.expected.? == .integer) entry.expected.?.integer else try std.fmt.parseInt(i64, entry.expected.?.string, 10);
                    try std.testing.expectEqual(wanted, result.integer);
                } else if (ordering) {
                    var unavailable = std.heap.FixedBufferAllocator.init(&.{});
                    const borrowed = try candidate.evaluate(unavailable.allocator(), &.{});
                    try std.testing.expectEqual(entry.expected.?.integer < 0, borrowed.boolean);
                } else {
                    var context: exact.Context = .{ .alloc = arena.allocator() };
                    const value = try binary.decodeCanonical(&context, result.numeric);
                    const actual = try exact.format(&context, value.value);
                    try std.testing.expectEqualStrings(entry.expected.?.string, actual);
                }
            }
        }
        exercised += 1;
    }
    try std.testing.expectEqual(@as(usize, 537), exercised);
}

test "relational declarations NUMERIC modifier SQL lowering matches PostgreSQL and retains lazy overflow" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_numeric_typmod_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn evaluate(alloc: Allocator, sql: []const u8) !std.json.Value {
            var compiled = try @import("../sql/compiler.zig").compileScalar(alloc, sql, .{});
            defer compiled.deinit();
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(alloc, &.{}, compiled.expression, null);
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, lowered.expression, .numeric);
            defer plan.deinit();
            const result = try plan.evaluate(alloc, &.{});
            if (result == .null) return .null;
            return @import("../sql/numeric_storage.zig").jsonValueAlloc(alloc, result.numeric);
        }
    };
    var tested: usize = 0;
    for (fixture.value.entries) |case| {
        // Arrays and these functions still lack a durable VM value/opcode.
        // Their SQL runtime coverage is not durable-expression activation.
        var unsupported = false;
        for ([_][]const u8{ "ARRAY", "[]", "GREATEST", "NULLIF", "abs(", "round(" }) |token| {
            if (std.mem.indexOf(u8, case.sql, token) != null) unsupported = true;
        }
        if (unsupported) continue;
        tested += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const actual = Run.evaluate(arena.allocator(), case.sql) catch |err| {
            const state = case.@"error" orelse {
                std.debug.print("Durable NUMERIC modifier failed: {s}: {s}\n", .{ case.sql, @errorName(err) });
                return err;
            };
            try std.testing.expectEqualStrings(state, @import("../sql/errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(case.@"error" == null);
        if (case.expected) |expected| {
            try std.testing.expectEqualStrings(expected, if (actual == .number_string) actual.number_string else actual.string);
        } else try std.testing.expect(actual == .null);
    }
    try std.testing.expectEqual(@as(usize, 42), tested);
}

test "relational declarations NUMERIC modifier casts bind fingerprints and reject malformed contracts" {
    const a = std.testing.allocator;
    var first: ?[32]u8 = null;
    for ([_][]const u8{ "{\"precision\":4,\"scale\":2}", "{\"precision\":4,\"scale\":1}" }) |modifier| {
        const text = try std.fmt.allocPrint(a, "{{\"op\":\"cast\",\"type\":\"numeric\",\"sql_type\":\"numeric\",\"numeric_modifier\":{s},\"args\":[{{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1.245\"}}]}}", .{modifier});
        defer a.free(text);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        var plan = try Plan.init(a, .{ .storage_mode = .relational }, parsed.value, .numeric);
        defer plan.deinit();
        if (first) |fingerprint| try std.testing.expect(!std.mem.eql(u8, &fingerprint, &plan.fingerprint)) else first = plan.fingerprint;
    }
    for ([_][]const u8{
        "{\"op\":\"cast\",\"type\":\"integer\",\"sql_type\":\"int32\",\"numeric_modifier\":{\"precision\":4,\"scale\":2},\"args\":[{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1\"}]}",
        "{\"op\":\"cast\",\"type\":\"numeric\",\"sql_type\":\"numeric\",\"numeric_modifier\":null,\"args\":[{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1\"}]}",
    }) |text| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, .{ .storage_mode = .relational }, parsed.value, if (parsed.value.object.get("type").?.string[0] == 'i') .integer else .numeric));
    }
}

test "relational declarations NUMERIC modifier casts own rounding scratch and reuse constrained bytes" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var compiled = try @import("../sql/compiler.zig").compileScalar(alloc, "CAST(CAST(1.245 AS numeric(4,2)) AS numeric(3,1))", .{});
            defer compiled.deinit();
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(arena.allocator(), &.{}, compiled.expression, null);
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, lowered.expression, .numeric);
            defer plan.deinit();
            const value = try plan.evaluate(arena.allocator(), &.{});
            const text = try @import("../sql/numeric_storage.zig").jsonValueAlloc(arena.allocator(), value.numeric);
            try std.testing.expectEqualStrings("1.3", text.number_string);
        }
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    try Run.run(a);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"cast","type":"numeric","sql_type":"numeric","numeric_modifier":{"precision":4,"scale":2},"args":[{"op":"literal","type":"numeric","value":"1.25"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(a, .{ .storage_mode = .relational }, parsed.value, .numeric);
    defer plan.deinit();
    var unavailable = std.heap.FixedBufferAllocator.init(&.{});
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(unavailable.allocator(), &bytes);
    const literal = plan.nodes[0].literal.numeric;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const value = try plan.evaluateWithExecution(&execution, &.{});
        try std.testing.expectEqual(literal.ptr, value.numeric.ptr);
    }
    std.debug.print("NUMERIC modifier casts: rows=10000 allocated_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
    try std.testing.expectEqual(max_allocated_bytes, bytes);
    execution.numeric.remaining = 0;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
    execution = Execution.init(unavailable.allocator(), &bytes);
    execution.numeric.checkpoint = Run.canceled;
    try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&execution, &.{}));
    execution.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&execution, &.{}));
}

test "relational declarations mixed NUMERIC SQL row programs match PostgreSQL" {
    const a = std.testing.allocator;
    const columns = [_]@import("../sql/scalar.zig").Column{
        .{ .name = "n", .type = .number, .element_type = .numeric },
        .{ .name = "i", .type = .integer, .element_type = .int64 },
        .{ .name = "f", .type = .number, .element_type = .float64 },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int64 },
        .{ .name = "f", .path = "f", .column_type = .number, .sql_element_type = .float64 },
    } };
    const Case = struct { sql: []const u8, n: ?[]const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_schema_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var compiled = try @import("../sql/compiler.zig").compileScalar(owned, entry.sql, .{});
        defer compiled.deinit();
        const lowered = @import("../sql/schema_expression.zig").lowerColumns(owned, &columns, compiled.expression, null) catch |err| {
            if (entry.@"error") |state| {
                try std.testing.expectEqualStrings(state, @import("../sql/errors.zig").describe(err).code);
                continue;
            }
            return err;
        };
        const kind: Kind = if (lowered.element_type == .numeric) .numeric else switch (lowered.type) {
            .integer => .integer,
            .number => .number,
            .boolean => .boolean,
            else => unreachable,
        };
        var plan = Plan.init(a, table, lowered.expression, kind) catch |err| {
            std.debug.print("NUMERIC schema lowering failed SQL={s}\n", .{entry.sql});
            return err;
        };
        defer plan.deinit();
        var execution_bytes: usize = max_allocated_bytes;
        var execution = Execution.init(owned, &execution_bytes);
        const values = [_]Value{ if (entry.n) |n| try numericJson(&execution, .{ .string = n }) else .null, .{ .integer = 2 }, .{ .number = 1.25 } };
        if (entry.@"error") |state| {
            try std.testing.expectError(if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else return error.TestUnexpectedSqlstate, plan.evaluate(owned, &values));
            continue;
        }
        const result = try plan.evaluate(owned, &values);
        if (entry.expected) |wanted| switch (result) {
            .numeric => |bytes| {
                var context: exact.Context = .{ .alloc = owned };
                const decoded = try binary.decodeCanonical(&context, bytes);
                try std.testing.expectEqualStrings(wanted, try exact.format(&context, decoded.value));
            },
            .integer => |value| try std.testing.expectEqual(try std.fmt.parseInt(i64, wanted, 10), value),
            .number => |value| try std.testing.expectEqual(if (lowered.element_type == .float32) @as(f64, try std.fmt.parseFloat(f32, wanted)) else try std.fmt.parseFloat(f64, wanted), value),
            .boolean => |value| try std.testing.expectEqualStrings(wanted, if (value) "true" else "false"),
            else => return error.TestUnexpectedResult,
        } else try std.testing.expect(result == .null);
    }
    try std.testing.expectEqual(@as(usize, 62), fixture.value.entries.len);
}

test "relational declarations SQL NUMERIC lowering owns plans through allocation faults and compares without scratch" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            const columns = [_]@import("../sql/scalar.zig").Column{
                .{ .name = "n", .type = .number, .element_type = .numeric },
                .{ .name = "i", .type = .integer, .element_type = .int64 },
            };
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
                .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric },
                .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int64 },
            } };
            var plan = blk: {
                var temporary = std.heap.ArenaAllocator.init(alloc);
                defer temporary.deinit();
                const a = temporary.allocator();
                const text = "CASE WHEN n>i THEN coalesce(n+1.25,i) ELSE 1/(i-i) END";
                var compiled = try @import("../sql/compiler.zig").compileScalar(a, text, .{});
                defer compiled.deinit();
                const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
                break :blk try Plan.init(alloc, table, lowered.expression, .numeric);
            };
            defer plan.deinit();
            // Plan owns literals after the input JSON/SQL arena is destroyed.
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(a, &bytes);
            const source = try numericJson(&execution, .{ .string = "9007199254740993.2500" });
            const result = try plan.evaluate(a, &.{ source, .{ .integer = 2 } });
            const output = try numericJsonOutput(&execution, result.numeric);
            try std.testing.expectEqualStrings("9007199254740994.5000", output.number_string);
            if (plan.evaluate(a, &.{ .null, .{ .integer = 2 } })) |_| {
                return error.TestExpectedError;
            } else |err| switch (err) {
                error.RelationalExpressionDivisionByZero => {},
                else => return err,
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const columns = [_]@import("../sql/scalar.zig").Column{.{ .name = "n", .type = .number, .element_type = .numeric }};
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric }} };
    var compiled = try @import("../sql/compiler.zig").compileScalar(a, "n >= 0.0000", .{});
    defer compiled.deinit();
    const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
    var plan = try Plan.init(std.testing.allocator, table, lowered.expression, .boolean);
    defer plan.deinit();
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(a, &bytes);
    const value = try numericJson(&execution, .{ .string = "9007199254740993.2500" });
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| try std.testing.expect((try plan.evaluate(std.testing.failing_allocator, &.{value})).boolean);
    std.debug.print("SQL durable NUMERIC comparison: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}

test "relational declarations NUMERIC SQL defaults preserve assignment failures and reader capability" {
    const a = std.testing.allocator;
    var compiled = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE exact_defaults (n bigint DEFAULT 9007199254740993.5, f real DEFAULT 0.100000001490116119384765625, g bigint GENERATED ALWAYS AS (CAST(9007199254740993.5 AS bigint)) STORED)", .{});
    defer compiled.deinit();
    const source = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, compiled.statement.create_table);
    defer a.free(source);
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, source);
    defer validator.deinit(a);
    const expressions = validator.execution.expressions.?;
    try std.testing.expect(expressions.table.requires_exact_numeric_expressions);
    var document = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer document.deinit();
    try expressions.applyJson(document.arena.allocator(), &document.value);
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("n").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("g").?.integer);
    try std.testing.expectEqual(@as(f64, @as(f32, 0.1)), document.value.object.get("f").?.float);
    try expressions.verifyJson(a, document.value);
    var overflow = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE overflow_defaults (n smallint DEFAULT 32767.5)", .{});
    defer overflow.deinit();
    const overflow_source = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, overflow.statement.create_table);
    defer a.free(overflow_source);
    var overflow_validator = try @import("mod.zig").CompiledTableValidator.init(a, overflow_source);
    defer overflow_validator.deinit(a);
    var missing = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer missing.deinit();
    try std.testing.expectError(error.RelationalExpressionOverflow, overflow_validator.execution.expressions.?.applyJson(missing.arena.allocator(), &missing.value));
}

test "relational declarations NUMERIC schema execution owns scratch and shares sticky admission" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"multiply","args":[{"op":"literal","type":"numeric","value":"9007199254740993.0001"},{"op":"literal","type":"numeric","value":"3.000"}]}
    , .{});
    defer parsed.deinit();
    const Run = struct {
        fn run(alloc: Allocator, expression: std.json.Value) !void {
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, expression, .numeric);
            defer plan.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(arena.allocator(), &bytes);
            const result = try plan.evaluateWithExecution(&execution, &.{});
            const text = try numericJsonOutput(&execution, result.numeric);
            try std.testing.expectEqualStrings("27021597764222979.0003000", text.number_string);
            try std.testing.expect(bytes < max_allocated_bytes);
            try std.testing.expectEqual(arena.allocator().ptr, execution.numeric.alloc.ptr);
            execution.numeric.remaining = 0;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
            execution.numeric.remaining = 1000;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
            var tiny: usize = 1;
            var bounded = Execution.init(arena.allocator(), &tiny);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&bounded, &.{}));
            const Poll = struct {
                fn canceled(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            var canceled = Execution.init(arena.allocator(), &bytes);
            canceled.numeric.checkpoint = Poll.canceled;
            try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&canceled, &.{}));
            canceled.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&canceled, &.{}));
        }
    };
    try Run.run(a, parsed.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{parsed.value});
}

test "relational declarations NUMERIC defaults generated topology and strict restoration" {
    const a = std.testing.allocator;
    const source: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "sum", .path = "sum", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "negative", .path = "negative", .column_type = .numeric, .sql_element_type = .numeric },
    } };
    const encoded = try schema.serializeSchema(a, source);
    defer a.free(encoded);
    const declarations = try std.json.parseFromSlice(std.json.Value, a,
        \\{"defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"9007199254740993.0001"}}],"generated":[{"column":"negative","expression":{"op":"negate","args":[{"op":"column","column":"sum"}]}},{"column":"sum","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"literal","type":"numeric","value":"0.0002"}]}}]}
    , .{});
    defer declarations.deinit();
    const Run = struct {
        fn run(alloc: Allocator, wire: []const u8, defs: std.json.Value) !void {
            const table = try schema.deserializeSchema(alloc, wire);
            const set = Set.createOwned(alloc, table, defs.object.get("defaults").?, defs.object.get("generated").?) catch |err| {
                schema.freeSchema(alloc, table);
                return err;
            };
            defer set.deinit();
            var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
            defer document.deinit();
            try set.applyJson(document.arena.allocator(), &document.value);
            try std.testing.expectEqualStrings("9007199254740993.0001", document.value.object.get("base").?.number_string);
            try std.testing.expectEqualStrings("9007199254740993.0003", document.value.object.get("sum").?.number_string);
            try std.testing.expectEqualStrings("-9007199254740993.0003", document.value.object.get("negative").?.number_string);
            try set.verifyJson(alloc, document.value);
            // Logical comparison ignores display scale, but authenticated
            // restoration must preserve the canonical generated display.
            try document.value.object.put(document.arena.allocator(), "sum", .{ .number_string = "9007199254740993.000300" });
            try invalidGenerated(alloc, set, document.value);
            try document.value.object.put(document.arena.allocator(), "sum", .{ .number_string = "9007199254740993.0004" });
            try invalidGenerated(alloc, set, document.value);
            _ = document.value.object.swapRemove("sum");
            try invalidGenerated(alloc, set, document.value);
            var explicit_null = try std.json.parseFromSlice(std.json.Value, alloc, "{\"base\":null}", .{});
            defer explicit_null.deinit();
            try set.applyJson(explicit_null.arena.allocator(), &explicit_null.value);
            try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("sum").?);
            try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("negative").?);
            try set.verifyJson(alloc, explicit_null.value);
        }
        fn invalidGenerated(alloc: Allocator, set: *const Set, document: std.json.Value) !void {
            if (set.verifyJson(alloc, document)) |_| {
                return error.TestExpectedError;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
            }
        }
    };
    try Run.run(a, encoded, declarations.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ encoded, declarations.value });
}

test "relational declarations NUMERIC target modifiers precede dependent expressions with PostgreSQL oracle" {
    const a = std.testing.allocator;
    const source: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_numeric_modifiers = true, .relational_columns = &.{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "narrow", .path = "narrow", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 3, .scale = 1 } },
        .{ .name = "total", .path = "total", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 5, .scale = 2 } },
    } };
    const wire = try schema.serializeSchema(a, source);
    defer a.free(wire);
    const declarations = try std.json.parseFromSlice(std.json.Value, a,
        \\{"defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"1.245"}}],"generated":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"column","column":"narrow"}]}},{"column":"narrow","expression":{"op":"column","column":"base"}}]}
    , .{});
    defer declarations.deinit();
    const Entry = struct { use_default: bool = false, input: ?[]const u8 = null, expected: ?[]const ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_numeric_assignment_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn run(alloc: Allocator, encoded: []const u8, defs: std.json.Value, cases: []const Entry) !void {
            const table = try schema.deserializeSchema(alloc, encoded);
            const set = Set.createOwned(alloc, table, defs.object.get("defaults").?, defs.object.get("generated").?) catch |err| {
                schema.freeSchema(alloc, table);
                return err;
            };
            defer set.deinit();
            for (cases) |entry| {
                var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
                defer document.deinit();
                if (!entry.use_default) try document.value.object.put(document.arena.allocator(), "base", if (entry.input) |text| .{ .string = text } else .null);
                if (entry.@"error") |code| {
                    try std.testing.expectEqualStrings("22003", code);
                    if (set.applyJson(document.arena.allocator(), &document.value)) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expectEqual(error.RelationalExpressionOverflow, err);
                    }
                    try std.testing.expect(!document.value.object.contains("total"));
                    continue;
                }
                try set.applyJson(document.arena.allocator(), &document.value);
                for ([_][]const u8{ "base", "narrow", "total" }, entry.expected.?) |name, expected| {
                    const value = document.value.object.get(name).?;
                    if (expected) |text| try std.testing.expectEqualStrings(text, if (value == .number_string) value.number_string else value.string) else try std.testing.expectEqual(std.json.Value.null, value);
                }
                try set.verifyJson(alloc, document.value);
                if (entry.use_default) {
                    var verification_bytes: usize = max_allocated_bytes;
                    var verification = Execution.init(alloc, &verification_bytes);
                    try set.verifyJsonWithExecution(&verification, document.value);
                    try std.testing.expect(verification.numeric.remaining < 8 * 1024 * 1024);
                    try std.testing.expect(verification_bytes < max_allocated_bytes);
                    try std.testing.expectEqual(alloc.ptr, verification.alloc.ptr);
                    try std.testing.expectEqual(alloc.vtable, verification.alloc.vtable);
                    verification.numeric.remaining = 0;
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    verification.numeric.remaining = 8 * 1024 * 1024;
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    verification_bytes = 1;
                    verification = Execution.init(alloc, &verification_bytes);
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    // Equivalent logical display scales are not repairs.
                    try document.value.object.put(document.arena.allocator(), "base", .{ .number_string = "1.2500" });
                    try set.verifyJson(alloc, document.value);
                    // A consistent-looking generated value cannot authorize
                    // an unconstrained base assignment during restoration.
                    try document.value.object.put(document.arena.allocator(), "base", .{ .number_string = "1.245" });
                    if (set.verifyJson(alloc, document.value)) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
                    }
                }
            }
        }
    };
    try Run.run(a, wire, declarations.value, fixture.value.entries);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ wire, declarations.value, fixture.value.entries[0..1] });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ wire, declarations.value, fixture.value.entries[6..7] });
}

test "relational declarations NUMERIC JSON assignments share PostgreSQL scalar and array semantics" {
    const a = std.testing.allocator;
    const Entry = struct { op: []const u8, left: []const u8, precision: u16 = 0, scale: i16 = 0, expected: ?std.json.Value = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_exact_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var tested: usize = 0;
    for (fixture.value.entries) |entry| {
        if (!std.mem.eql(u8, entry.op, "typmod")) continue;
        tested += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .dimensions = .{ .{ .length = 2, .lower_bound = -3 }, .{ .length = 2, .lower_bound = 7 } },
            .values = .{ entry.left, null, entry.left, null },
            .sql_nulls = .{ false, true, false, true },
        }, .{});
        var document = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer document.deinit();
        const original = document.value.object.get("values").?.array.items;
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        const modifier: exact.TypeModifier = .{ .precision = entry.precision, .scale = entry.scale };
        const raw_array = try arrayJson(&execution, .numeric, document.value, null);
        const cast_text = try std.json.Stringify.valueAlloc(alloc, .{
            .op = "cast",
            .type = "sql_array",
            .sql_type = "numeric",
            .numeric_modifier = modifier,
            .args = .{.{ .op = "column", .column = "a" }},
        }, .{});
        const cast_json = try std.json.parseFromSliceLeaky(std.json.Value, alloc, cast_text, .{});
        const cast_table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
            .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .numeric },
        } };
        var cast: ?Plan = null;
        if (modifier.validate()) |_| {
            cast = try Plan.init(alloc, cast_table, cast_json, .sql_array);
        } else |_| try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(alloc, cast_table, cast_json, .sql_array));
        defer if (cast) |*plan| plan.deinit();
        if (entry.@"error") |code| {
            const expected = if (std.mem.eql(u8, code, "22023")) error.SqlInvalidParameterValue else error.RelationalExpressionOverflow;
            try std.testing.expectError(expected, normalizeNumericJson(&execution, .{ .string = entry.left }, modifier, false));
            try std.testing.expectError(expected, normalizeNumericArrayJson(&execution, &document.value, modifier, false));
            try std.testing.expectError(expected, arrayJson(&execution, .numeric, document.value, modifier));
            try std.testing.expectError(expected, normalizeNumericArrayBinding(&execution, raw_array, modifier, false));
            if (cast) |*plan| try std.testing.expectError(expected, plan.evaluate(alloc, &.{raw_array}));
            try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
            continue;
        }
        const adapted = try arrayJson(&execution, .numeric, document.value, modifier);
        const assigned = try normalizeNumericArrayBinding(&execution, raw_array, modifier, false);
        try std.testing.expectEqualSlices(u8, adapted.sql_array.bytes, assigned.sql_array.bytes);
        const converted = try cast.?.evaluate(alloc, &.{raw_array});
        try std.testing.expectEqualSlices(u8, adapted.sql_array.bytes, converted.sql_array.bytes);
        const cast_reused = try cast.?.evaluate(std.testing.failing_allocator, &.{converted});
        try std.testing.expectEqual(converted.sql_array.bytes.ptr, cast_reused.sql_array.bytes.ptr);
        const before_reuse = budget;
        const reused = try normalizeNumericArrayBinding(&execution, assigned, modifier, false);
        try std.testing.expectEqual(assigned.sql_array.bytes.ptr, reused.sql_array.bytes.ptr);
        try std.testing.expectEqual(before_reuse, budget);
        const adapted_view = try adapted.sql_array.view();
        try std.testing.expectEqual(@as(u32, 4), adapted_view.count);
        try std.testing.expectEqual(@as(i32, -3), (try adapted_view.dimension(0)).lower);
        try std.testing.expectEqual(@as(i32, 7), (try adapted_view.dimension(1)).lower);
        try std.testing.expect((try adapted_view.cell(1)).sql_null);
        try std.testing.expect((try adapted_view.cell(3)).sql_null);
        var read_context: exact.Context = .{ .alloc = alloc };
        var read_number = try binary.decodeCanonical(&read_context, (try adapted_view.cell(0)).bytes);
        defer read_number.deinit();
        const adapted_text = try exact.format(&read_context, read_number.value);
        try std.testing.expectEqualStrings(entry.expected.?.string, adapted_text);
        const scalar = try normalizeNumericJson(&execution, .{ .string = entry.left }, modifier, false);
        try std.testing.expectEqualStrings(entry.expected.?.string, if (scalar == .number_string) scalar.number_string else scalar.string);
        try normalizeNumericArrayJson(&execution, &document.value, modifier, false);
        const cells = document.value.object.get("values").?.array.items;
        try std.testing.expectEqualStrings(entry.expected.?.string, cells[0].string);
        try std.testing.expectEqualStrings(entry.expected.?.string, cells[2].string);
        try std.testing.expectEqual(std.json.Value.null, cells[1]);
        try std.testing.expectEqual(std.json.Value.null, cells[3]);
        try std.testing.expectEqual(@as(i64, -3), document.value.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
        try std.testing.expectEqual(@as(i64, 7), document.value.object.get("dimensions").?.array.items[1].object.get("lower_bound").?.integer);
        try normalizeNumericArrayJson(&execution, &document.value, modifier, true);
        try std.testing.expectEqual(cells.ptr, document.value.object.get("values").?.array.items.ptr);
    }
    try std.testing.expectEqual(@as(usize, 20), tested);
}

test "relational declarations canonical NUMERIC array assignment streams with fault and preservation safety" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    var parsing: exact.Context = .{ .alloc = a };
    var number = try exact.parse(&parsing, "12.345");
    defer number.deinit();
    var cells: [45]arrays.Element = undefined;
    for (&cells, 0..) |*cell, i| cell.* = if (i % 5 == 0) .{} else arrays.Element.typedNumeric(&number.value);
    const bytes = try storage.encodeAlloc(a, .{ .element_type = .numeric, .dimensions = &.{.{ .length = cells.len, .lower = -7 }}, .elements = &cells }, .{});
    defer a.free(bytes);
    const input: Value = .{ .sql_array = .{ .element_type = .numeric, .bytes = bytes } };
    const target: exact.TypeModifier = .{ .precision = 4, .scale = 2 };
    const Run = struct {
        fn run(alloc: Allocator, source: Value, modifier: exact.TypeModifier) !void {
            const declaration = try std.json.Stringify.valueAlloc(alloc, .{
                .op = "cast",
                .type = "sql_array",
                .sql_type = "numeric",
                .numeric_modifier = modifier,
                .args = .{.{ .op = "column", .column = "a" }},
            }, .{});
            defer alloc.free(declaration);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, declaration, .{});
            defer parsed.deinit();
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational, .relational_columns = &.{
                .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .numeric },
            } }, parsed.value, .sql_array);
            defer plan.deinit();
            var allowance: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &allowance);
            const result = plan.evaluateWithExecution(&execution, &.{source}) catch |err| {
                if (err == error.OutOfMemory) try std.testing.expect(execution.numeric.failure == null);
                try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
                try std.testing.expectEqual(alloc.vtable, execution.numeric.alloc.vtable);
                return err;
            };
            defer alloc.free(result.sql_array.bytes);
            try std.testing.expect(result.sql_array.bytes.ptr != source.sql_array.bytes.ptr);
            const view = try storage.validateCanonical(alloc, .numeric, result.sql_array.bytes, .{});
            try std.testing.expectEqual(@as(i32, -7), (try view.dimension(0)).lower);
            for (0..view.count) |i| {
                const cell = try view.cell(i);
                try std.testing.expectEqual(i % 5 == 0, cell.sql_null);
                if (!cell.sql_null) try (try binary.layout.View.open(cell.bytes, .{})).verifyStoredModifier(modifier);
            }
            var no_bytes: usize = 0;
            var no_heap = Execution.init(std.testing.failing_allocator, &no_bytes);
            const reused = try plan.evaluateWithExecution(&no_heap, &.{result});
            try std.testing.expectEqual(result.sql_array.bytes.ptr, reused.sql_array.bytes.ptr);
            try std.testing.expectEqual(@as(usize, 0), no_bytes);
            try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{ input, target });
    var allowance: usize = max_allocated_bytes;
    var execution = Execution.init(a, &allowance);
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, normalizeNumericArrayBinding(&execution, input, target, true));
    const Cancel = struct {
        calls: usize = 0,
        fn poll(ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == 4) return error.Canceled;
        }
    };
    var cancel: Cancel = .{};
    execution = Execution.init(a, &allowance);
    execution.numeric.checkpoint = Cancel.poll;
    execution.numeric.ptr = &cancel;
    try std.testing.expectError(error.Canceled, normalizeNumericArrayBinding(&execution, input, target, false));
    try std.testing.expectEqual(@as(usize, 4), cancel.calls);
    execution.numeric.checkpoint = null;
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.Canceled, normalizeNumericArrayBinding(&execution, input, target, false));
    var scaled = try exact.parse(&parsing, "12.3500");
    defer scaled.deinit();
    const same_bytes = try storage.encodeAlloc(a, .{ .element_type = .numeric, .dimensions = &.{.{ .length = 1 }}, .elements = &.{arrays.Element.typedNumeric(&scaled.value)} }, .{});
    defer a.free(same_bytes);
    execution = Execution.init(a, &allowance);
    const same: Value = .{ .sql_array = .{ .element_type = .numeric, .bytes = same_bytes } };
    try std.testing.expectEqual(same_bytes.ptr, (try normalizeNumericArrayBinding(&execution, same, target, true)).sql_array.bytes.ptr);
}

test "relational declarations canonical NUMERIC array assignment bounds scratch independently of cell count" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    const storage = @import("../sql/array_storage.zig");
    var parsing: exact.Context = .{ .alloc = a };
    var number = try exact.parse(&parsing, "123.4567");
    defer number.deinit();
    const cells = try a.alloc(arrays.Element, 10000);
    defer a.free(cells);
    @memset(cells, arrays.Element.typedNumeric(&number.value));
    const input = try storage.encodeAlloc(a, .{ .element_type = .numeric, .dimensions = &.{.{ .length = @intCast(cells.len), .lower = -9 }}, .elements = cells }, .{});
    defer a.free(input);
    var tracked = std.testing.FailingAllocator.init(a, .{});
    var allowance: usize = max_allocated_bytes;
    var execution = Execution.init(tracked.allocator(), &allowance);
    const before = execution.numeric.remaining;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    const result = try normalizeNumericArrayBinding(&execution, .{ .sql_array = .{ .element_type = .numeric, .bytes = input } }, .{ .precision = 5, .scale = 2 }, false);
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
    defer tracked.allocator().free(result.sql_array.bytes);
    try std.testing.expectEqual(tracked.allocated_bytes, max_allocated_bytes - allowance);
    try std.testing.expect(tracked.alloc_index < 100);
    try std.testing.expect(tracked.allocated_bytes < cells.len * @sizeOf(arrays.Element));
    std.debug.print("canonical NUMERIC array assignment: cells={} output_bytes={} total_allocation_bytes={} allocations={} work={} elapsed_ns={} flat_cell_vectors=0\n", .{
        cells.len, result.sql_array.bytes.len, tracked.allocated_bytes, tracked.alloc_index, before - execution.numeric.remaining, elapsed,
    });
}

test "relational declarations NUMERIC JSON array preparation is atomic bounded and cancellation aware" {
    const a = std.testing.allocator;
    const text =
        \\{"dimensions":[{"length":4,"lower_bound":-3}],"values":["1.245",null,"2.5",null],"sql_nulls":[false,true,false,true]}
    ;
    const modifier: exact.TypeModifier = .{ .precision = 4, .scale = 2 };
    const Run = struct {
        fn run(alloc: Allocator, source: []const u8, target: exact.TypeModifier, overflow: bool) !void {
            var document = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
            defer document.deinit();
            const owned = document.arena.allocator();
            const original = document.value.object.get("values").?.array.items;
            if (overflow) original[2] = .{ .string = "999.995" };
            var budget: usize = max_allocated_bytes;
            var execution = Execution.init(owned, &budget);
            if (overflow) {
                if (normalizeNumericArrayJson(&execution, &document.value, target, false)) |_| return error.TestExpectedError else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.RelationalExpressionOverflow, err);
                }
                try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
                try std.testing.expectEqualStrings("1.245", original[0].string);
            } else {
                try normalizeNumericArrayJson(&execution, &document.value, target, false);
                const cells = document.value.object.get("values").?.array.items;
                try std.testing.expectEqualStrings("1.25", cells[0].string);
                try std.testing.expectEqualStrings("2.50", cells[2].string);
                try std.testing.expect(budget < max_allocated_bytes);
            }
        }
    };
    try Run.run(a, text, modifier, false);
    try Run.run(a, text, modifier, true);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ text, modifier, false });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ text, modifier, true });
    var document = try std.json.parseFromSlice(std.json.Value, a, text, .{});
    defer document.deinit();
    const original = document.value.object.get("values").?.array.items;
    var budget: usize = max_allocated_bytes;
    var execution = Execution.init(a, &budget);
    try normalizeNumericArrayJson(&execution, &document.value, null, true);
    const used = 8 * 1024 * 1024 - execution.numeric.remaining;
    try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, normalizeNumericArrayJson(&execution, &document.value, modifier, true));
    execution = Execution.init(a, &budget);
    execution.numeric.remaining = used - 1;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericArrayJson(&execution, &document.value, null, true));
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    execution = Execution.init(a, &budget);
    execution.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, normalizeNumericArrayJson(&execution, &document.value, null, true));
    execution.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
    budget = 1;
    execution = Execution.init(a, &budget);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericArrayJson(&execution, &document.value, null, true));
    budget = max_allocated_bytes;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
}

test "relational declarations NUMERIC JSON array validation reuses bounded scratch across ten thousand cells" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const values = try alloc.alloc([]const u8, 10000);
    @memset(values, "1.25");
    const nulls = try alloc.alloc(bool, values.len);
    @memset(nulls, false);
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .dimensions = .{.{ .length = values.len, .lower_bound = -100 }},
        .values = values,
        .sql_nulls = nulls,
    }, .{});
    var document = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer document.deinit();
    var budget: usize = max_allocated_bytes;
    var execution = Execution.init(alloc, &budget);
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    try normalizeNumericArrayJson(&execution, &document.value, .{ .precision = 4, .scale = 2 }, true);
    // Retained scratch is bounded by the largest cell, not the cell count.
    try std.testing.expect(max_allocated_bytes - budget < 4096);
    std.debug.print("NUMERIC JSON array admission: cells=10000 scratch_bytes={} work={} elapsed_ns={}\n", .{ max_allocated_bytes - budget, 8 * 1024 * 1024 - execution.numeric.remaining, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
}

test "relational declarations NUMERIC constrained bindings reuse canonical bytes with sticky admission" {
    const a = std.testing.allocator;
    const bytes = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(a, .{ .string = "1.25" });
    defer a.free(bytes);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var remaining: usize = max_allocated_bytes;
    var scope = Execution.init(none.allocator(), &remaining);
    const modifier: exact.TypeModifier = .{ .precision = 4, .scale = 2 };
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const result = try normalizeNumericBinding(&scope, bytes, modifier, false);
        try std.testing.expectEqual(bytes.ptr, result.numeric.ptr);
    }
    try std.testing.expectEqual(max_allocated_bytes, remaining);
    std.debug.print("NUMERIC binding reuse: rows=10000 allocated_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    scope.numeric.remaining = 0;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope.numeric.remaining = 1000;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope = Execution.init(none.allocator(), &remaining);
    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    scope.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, normalizeNumericBinding(&scope, bytes, modifier, false));
}

test "relational declarations NUMERIC generated identity binds target and dependency modifiers and fences cold rows" {
    const a = std.testing.allocator;
    var columns = [_]schema.RelationalColumn{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "derived", .path = "derived", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "cold", .path = "cold", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_numeric_modifiers = true, .relational_columns = &columns };
    const defs = try std.json.parseFromSlice(std.json.Value, a,
        \\[{"column":"derived","expression":{"op":"column","column":"base"}}]
    , .{});
    defer defs.deinit();
    const Run = struct {
        fn identity(alloc: Allocator, source: schema.TableSchema, declarations: std.json.Value) ![32]u8 {
            const encoded = try schema.serializeSchema(alloc, source);
            defer alloc.free(encoded);
            const owned = try schema.deserializeSchema(alloc, encoded);
            const set = Set.createOwned(alloc, owned, .null, declarations) catch |err| {
                schema.freeSchema(alloc, owned);
                return err;
            };
            defer set.deinit();
            const Row = struct {
                table_schema: schema.TableSchema,
                reads: *usize,
                pub fn ordinalForName(_: @This(), name: []const u8) ?u32 {
                    return if (std.mem.eql(u8, name, "base")) 0 else 1;
                }
                pub fn findCell(self: @This(), _: u32) !?codec.Cell {
                    self.reads.* += 1;
                    return null;
                }
            };
            var historical_columns = [_]schema.RelationalColumn{ source.relational_columns[0], source.relational_columns[1] };
            historical_columns[0].numeric_modifier.?.scale += 1;
            var historical = source;
            historical.relational_columns = &historical_columns;
            var reads: usize = 0;
            try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, set.modifier_ordinals);
            try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRow(alloc, Row{ .table_schema = historical, .reads = &reads }));
            try std.testing.expectEqual(@as(usize, 0), reads);
            return generatedFingerprint(set);
        }
    };
    const original = try Run.identity(a, table, defs.value);
    columns[0].numeric_modifier.?.scale = 1;
    const dependency = try Run.identity(a, table, defs.value);
    try std.testing.expect(!std.mem.eql(u8, &original, &dependency));
    columns[0].numeric_modifier.?.scale = 2;
    columns[1].numeric_modifier.?.scale = 1;
    const target = try Run.identity(a, table, defs.value);
    try std.testing.expect(!std.mem.eql(u8, &original, &target));
    columns[1].numeric_modifier.?.scale = 2;
    columns[2].numeric_modifier.?.scale = 1;
    const unrelated = try Run.identity(a, table, defs.value);
    try std.testing.expectEqualSlices(u8, &original, &unrelated);
}

test "relational declarations SQL numeric programs retain narrow domains and checked casts" {
    const alloc = std.testing.allocator;
    const ast = @import("../sql/ast.zig");
    const columns = [_]@import("../sql/scalar.zig").Column{
        .{ .name = "n", .type = .integer, .element_type = .int16 },
        .{ .name = "i", .type = .integer, .element_type = .int32 },
        .{ .name = "b", .type = .integer, .element_type = .int64 },
        .{ .name = "f", .type = .number, .element_type = .float32 },
        .{ .name = "d", .type = .number, .element_type = .float64 },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 },
        .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int32 },
        .{ .name = "b", .path = "b", .column_type = .integer, .sql_element_type = .int64 },
        .{ .name = "f", .path = "f", .column_type = .number, .sql_element_type = .float32 },
        .{ .name = "d", .path = "d", .column_type = .number, .sql_element_type = .float64 },
    } };
    const Case = struct { sql: []const u8, n: ?i64 = 1, i: i64 = 1, d: f64 = 2.5, expected: std.json.Value = .null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Case }, alloc, @embedFile("../sql/fixtures/sql_numeric_expression_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var compiled = try @import("../sql/compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
        const expected: Kind = switch (lowered.type) {
            ast.ColumnType.integer => .integer,
            .number => .number,
            .boolean => .boolean,
            .string => .string,
            else => unreachable,
        };
        var plan = try Plan.init(alloc, table, lowered.expression, expected);
        defer plan.deinit();
        const cells = [_]Value{ if (case.n) |n| .{ .integer = n } else .null, .{ .integer = case.i }, .{ .integer = 9007199254740993 }, .{ .number = 16777216 }, .{ .number = case.d } };
        var query = try @import("../sql/scalar.zig").bind(a, compiled.expression, &columns, &.{}, .{});
        defer query.deinit();
        const Datum = @import("../sql/scalar.zig").Datum;
        const inputs = [_]Datum{ if (case.n) |n| Datum.json(.{ .integer = n }) else .{}, Datum.json(.{ .integer = case.i }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .float = 16777216 }), Datum.json(.{ .float = case.d }) };
        if (case.@"error") |state| {
            const failure = if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else return error.TestUnexpectedSqlstate;
            try std.testing.expectError(failure, plan.evaluate(alloc, &cells));
            try std.testing.expectError(if (std.mem.eql(u8, state, "22003")) error.SqlNumericOutOfRange else error.SqlDivisionByZero, query.evaluate(a, &inputs, &.{}, .{}));
        } else {
            const wanted = try scalarJson(a, expected, case.expected, true);
            try std.testing.expect(try valuesEqual(wanted, try plan.evaluate(alloc, &cells)));
            const actual = try query.evaluate(a, &inputs, &.{}, .{});
            try std.testing.expectEqual(case.expected == .null, actual.sql_null);
            try std.testing.expect(try valuesEqual(wanted, try scalarJson(a, expected, actual.value, true)));
        }
    }
}

test "relational declarations SQL numeric programs conditional allocation faults and zero scratch evaluation" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var compiled = try @import("../sql/compiler.zig").compileScalar(a, "CASE WHEN n>0 THEN CAST(n AS integer) ELSE 1/(i-i) END", .{});
            defer compiled.deinit();
            const columns = [_]@import("../sql/scalar.zig").Column{
                .{ .name = "n", .type = .integer, .element_type = .int16 },
                .{ .name = "i", .type = .integer, .element_type = .int32 },
            };
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
                .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 },
                .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int32 },
            } };
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
            var plan = try Plan.init(alloc, table, lowered.expression, .integer);
            defer plan.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            var total: i64 = 0;
            for (0..10000) |i| {
                total += (try plan.evaluate(std.testing.failing_allocator, &.{ .{ .integer = @intCast(i + 1) }, .{ .integer = 1 } })).integer;
            }
            try std.testing.expectEqual(@as(i64, 50005000), total);
            try std.testing.expectError(error.RelationalExpressionDivisionByZero, plan.evaluate(std.testing.failing_allocator, &.{ .null, .{ .integer = 1 } }));
            // Fault probes reach this point only after all preparation succeeds.
            std.debug.print("SQL durable CASE: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "relational declarations SQL membership preparation faults and zero scratch evaluation" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var compiled = try @import("../sql/compiler.zig").compileScalar(a, "(n % 3) IN (NULL,CAST(-1 AS smallint),1) AND (n>0) IS NOT UNKNOWN", .{});
            defer compiled.deinit();
            const columns = [_]@import("../sql/scalar.zig").Column{.{ .name = "n", .type = .integer, .element_type = .int16 }};
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 }} };
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
            var plan = try Plan.init(alloc, table, lowered.expression, .boolean);
            defer plan.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            for (0..10000) |_| try std.testing.expect((try plan.evaluate(std.testing.failing_allocator, &.{.{ .integer = -7 }})).boolean);
            try std.testing.expect(!(try plan.evaluate(std.testing.failing_allocator, &.{.null})).boolean);
            std.debug.print("SQL durable membership: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "relational declarations SQL numeric programs fence nonnumeric conditional generated columns" {
    const alloc = std.testing.allocator;
    var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"string","value":"ready"}}],"generated_columns":[{"column":"y","expression":{"op":"case_when","args":[{"op":"is_null","args":[{"op":"column","column":"x"}]},{"op":"literal","type":"string","value":"missing"},{"op":"column","column":"x"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["keyword","null"]},"y":{"type":"keyword"}},"required":["y"],"additionalProperties":false}}}}
    );
    defer validator.deinit(alloc);
    const expressions = validator.execution.expressions.?;
    try std.testing.expect(expressions.table.requires_typed_expressions);
    const runtime = try @import("mod.zig").deriveRuntimeTableSchema(alloc, validator.schema);
    defer schema.freeSchema(alloc, runtime);
    try std.testing.expect(runtime.requires_typed_expressions);
    var missing = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
    defer missing.deinit();
    try expressions.applyJson(missing.arena.allocator(), &missing.value);
    try std.testing.expectEqualStrings("ready", missing.value.object.get("y").?.string);
    try expressions.verifyJson(alloc, missing.value);
    var explicit = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":null}", .{});
    defer explicit.deinit();
    try expressions.applyJson(explicit.arena.allocator(), &explicit.value);
    try std.testing.expectEqualStrings("missing", explicit.value.object.get("y").?.string);
    try expressions.verifyJson(alloc, explicit.value);
    try explicit.value.object.put(explicit.arena.allocator(), "y", .{ .string = "forged" });
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, expressions.verifyJson(alloc, explicit.value));
}

test "relational declarations SQL numeric programs reject malformed conditional domains" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        \\{"op":"case_when","args":[{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":0}]}
        ,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"literal","type":"integer","value":1},{"op":"literal","type":"string","value":"bad"}]}
        ,
    }) |text| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, text, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(alloc, .{}, parsed.value, .integer));
    }
    const even = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"literal","type":"integer","value":1}]}
    , .{});
    defer even.deinit();
    try std.testing.expectError(error.InvalidRelationalExpression, Plan.init(alloc, .{}, even.value, .integer));
}

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
    modifier_ordinals: []const u32,
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
                var plan = if (mode == 0) try Plan.initDefault(alloc, table, entry.object.get("expression") orelse return error.InvalidRelationalExpression, table.relational_columns[ordinal].column_type) else try Plan.init(alloc, table, entry.object.get("expression") orelse return error.InvalidRelationalExpression, table.relational_columns[ordinal].column_type);
                errdefer plan.deinit();
                if (plan.result_kind == .sql_array and plan.result_sql_type != table.relational_columns[ordinal].sql_element_type) return error.InvalidRelationalExpressionType;
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
        var states = @as([256]u2, @splat(0));
        var written: usize = 0;
        for (bindings, 0..) |_, i| try visit(bindings, order, &states, &written, i);
        var constrained: std.ArrayList(u32) = .empty;
        defer constrained.deinit(alloc);
        for (table.relational_columns, read_columns, 0..) |column, read, ordinal| {
            if (read and column.numeric_modifier != null) try constrained.append(alloc, @intCast(ordinal));
        }
        const modifier_ordinals = try constrained.toOwnedSlice(alloc);
        errdefer alloc.free(modifier_ordinals);
        const set = try alloc.create(Set);
        set.* = .{ .alloc = alloc, .table = table, .bindings = bindings, .order = order, .read_columns = read_columns, .generated_columns = generated_columns, .modifier_ordinals = modifier_ordinals };
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
        self.alloc.free(self.modifier_ordinals);
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
        return self.applyValuesWithDefaultMask(alloc, values, present, defaults, null);
    }

    pub fn applyValuesWithDefaultMask(self: *const Set, alloc: Allocator, values: []Value, present: []bool, defaults: DefaultsPolicy, default_mask: ?[]const bool) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.applyValuesWithExecution(&execution, values, present, defaults, default_mask);
    }

    pub fn applyValuesWithExecution(self: *const Set, execution: *Execution, values: []Value, present: []bool, defaults: DefaultsPolicy, default_mask: ?[]const bool) !void {
        if (values.len != self.table.relational_columns.len or present.len != values.len or (default_mask != null and default_mask.?.len != values.len)) return error.InvalidRelationalExpressionInput;
        // Base assignments must cross the target domain before any generated
        // expression reads them. Ignore submitted generated fields and keep
        // work proportional to the immutable dependency mask.
        for (self.modifier_ordinals) |ordinal| {
            if (!present[ordinal] or self.generated_columns[ordinal]) continue;
            values[ordinal] = try self.normalizeBinding(execution, ordinal, values[ordinal]);
        }
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated and (present[binding.ordinal] or defaults == .preserve_absence or (default_mask != null and !default_mask.?[binding.ordinal]))) continue;
            values[binding.ordinal] = try self.normalizeBinding(execution, binding.ordinal, try binding.plan.evaluateWithExecution(execution, values));
            present[binding.ordinal] = true;
        }
    }

    /// Mutates a request-owned DOM without reparsing it. Generated fields are
    /// output-only: any submitted value is overwritten deterministically.
    pub fn applyJson(self: *const Set, alloc: Allocator, document: *std.json.Value) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.applyJsonWithExecution(&execution, document);
    }

    pub fn applyJsonWithExecution(self: *const Set, execution: *Execution, document: *std.json.Value) !void {
        var roots: @import("../sql/row_value.zig").JsonRow = undefined;
        roots.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        defer roots.deinit();
        try roots.bindAdmission(execution.bytes);
        var nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
        nulls.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        defer nulls.deinit();
        try nulls.bindAdmission(execution.bytes);
        try self.applyTypedJsonWithExecution(execution, document, &roots, &nulls);
        // A representation-only caller cannot discard new typed provenance.
        // Typed preparation must retain the supplied owner through encoding.
        if (nulls.fields.count() != 0) return error.InvalidRelationalExpressionInput;
    }

    pub fn applyTypedJsonWithExecution(self: *const Set, execution: *Execution, document: *std.json.Value, roots: *@import("../sql/row_value.zig").JsonRow, nulls: *@import("../sql/row_value.zig").JsonNulls) !void {
        execution.numeric.charge(0) catch |err| return executionFailure(err);
        if (roots.context != &execution.numeric or nulls.context != &execution.numeric or
            roots.admission == null or nulls.admission == null or
            roots.admission.?.remaining != execution.bytes or nulls.admission.?.remaining != execution.bytes)
            return error.InvalidRelationalExpressionInput;
        if (document.* != .object) return error.InvalidBatchRequest;
        const alloc = execution.alloc;
        const count = self.table.relational_columns.len;
        execution.numeric.charge(count) catch |err| return executionFailure(err);
        const staging_bytes = std.math.mul(usize, count, @sizeOf(Value) + @sizeOf(bool)) catch return executionFailure(execution.numeric.limit());
        if (staging_bytes > execution.bytes.*) return executionFailure(execution.numeric.limit());
        const values = try alloc.alloc(Value, self.table.relational_columns.len);
        defer alloc.free(values);
        const present = try alloc.alloc(bool, values.len);
        defer alloc.free(present);
        execution.bytes.* -= staging_bytes;
        try self.readValues(execution, document.*, values, present, true, roots, nulls);
        try self.applyValuesWithExecution(execution, values, present, .apply_to_absent, null);
        for (self.modifier_ordinals) |ordinal| {
            if (self.generated_columns[ordinal]) continue;
            const column = self.table.relational_columns[ordinal];
            if (document.object.getPtr(column.name)) |cell| switch (values[ordinal]) {
                .numeric => |bytes| cell.* = try numericJsonOutput(execution, bytes),
                .sql_array => |array| cell.* = try arrayJsonOutputLeaky(execution, array),
                .null => {},
                else => return error.InvalidRelationalExpressionInput,
            };
        }
        for (self.bindings) |binding| {
            const name = self.table.relational_columns[binding.ordinal].name;
            if (!binding.generated and document.object.contains(name)) continue;
            const value = values[binding.ordinal];
            const key = try allocateOutput(alloc, name.len, execution.bytes);
            @memcpy(key, name);
            try document.object.put(alloc, key, try boundedValueToJson(execution, value));
            if (self.table.relational_columns[binding.ordinal].column_type == .json)
                nulls.assign(name, value) catch |err| return executionFailure(err);
        }
    }

    /// Restore never fills defaults or repairs forged generated values. It
    /// verifies the stored canonical logical result in dependency order.
    pub fn verifyJson(self: *const Set, alloc: Allocator, document: std.json.Value) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.verifyJsonWithExecution(&execution, document);
    }

    pub fn verifyJsonWithExecution(self: *const Set, execution: *Execution, document: std.json.Value) !void {
        return self.verifyTypedJsonWithExecution(execution, document, &.{});
    }

    pub fn verifyTypedJsonWithExecution(self: *const Set, execution: *Execution, document: std.json.Value, json_null_fields: []const []const u8) !void {
        try execution.charge(0);
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        self.verifyJsonInner(execution, document, json_null_fields) catch |err| return scratch.failure(err);
    }

    fn verifyJsonInner(self: *const Set, execution: *Execution, document: std.json.Value, json_null_fields: []const []const u8) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        if (document != .object) return error.InvalidBatchRequest;
        try execution.charge(self.table.relational_columns.len);
        const values = try execution.alloc.alloc(Value, self.table.relational_columns.len);
        const present = try execution.alloc.alloc(bool, values.len);
        var roots: @import("../sql/row_value.zig").JsonRow = undefined;
        roots.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        defer roots.deinit();
        try roots.bindAdmission(execution.bytes);
        var nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
        nulls.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        defer nulls.deinit();
        try nulls.bindAdmission(execution.bytes);
        try nulls.import(json_null_fields);
        try self.readValues(execution, document, values, present, false, &roots, &nulls);
        // Logical transport may spell NUMERIC(p,s) with an equivalent scale.
        // First reject any coefficient changed by assignment, then normalize
        // only this unpublished typed shadow to the declared domain. The
        // document is unchanged; generated expressions are never repaired.
        // Physical verifyRow has no such allowance: its caller admitted the
        // exact canonical bytes already, including typmod display metadata.
        for (self.modifier_ordinals) |ordinal| {
            if (!present[ordinal] or values[ordinal] == .null) continue;
            const modifier = self.table.relational_columns[ordinal].numeric_modifier.?;
            if (values[ordinal] == .sql_array) {
                _ = try normalizeNumericArrayBinding(execution, values[ordinal], modifier, true);
            } else if (values[ordinal] == .numeric) {
                _ = try normalizeNumericBinding(execution, values[ordinal].numeric, modifier, true);
            } else return error.InvalidRelationalGeneratedValue;
            values[ordinal] = try self.normalizeBinding(execution, ordinal, values[ordinal]);
        }
        try self.verifyValues(execution, values, present);
    }

    /// Cold restore verifies generated semantics directly from ordinal cells.
    /// The caller has already verified physical canonical bytes and checksum.
    /// Unrelated JSON, vector and blob payloads are never materialized.
    pub fn verifyRow(self: *const Set, alloc: Allocator, row: anytype) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.verifyRowWithExecution(&execution, row);
    }

    pub fn verifyRowWithExecution(self: *const Set, execution: *Execution, row: anytype) !void {
        try execution.charge(0);
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        self.verifyRowInner(execution, row) catch |err| return scratch.failure(err);
    }

    fn verifyRowInner(self: *const Set, execution: *Execution, row: anytype) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        // One stable decoded owner spans every generated binding. Its sparse
        // cache is keyed by physical source ordinal, not by a logical ordinal
        // that may change across immutable historical schema layouts.
        var json_owner: @import("../sql/row_value.zig").JsonRow = undefined;
        json_owner.initRegion(execution.alloc, execution.bytes.*, &execution.numeric);
        defer json_owner.deinit();
        try json_owner.bindAdmission(execution.bytes);
        try execution.charge(self.table.relational_columns.len);
        const values = try execution.alloc.alloc(Value, self.table.relational_columns.len);
        const present = try execution.alloc.alloc(bool, values.len);
        @memset(values, .null);
        @memset(present, false);
        for (self.table.relational_columns, self.read_columns, 0..) |column, read, ordinal| {
            if (!read) continue;
            const physical = row.ordinalForName(column.name) orelse continue;
            if (row.table_schema.relational_columns[physical].column_type != column.column_type or
                row.table_schema.relational_columns[physical].sql_element_type != column.sql_element_type or
                !exact.TypeModifier.eql(row.table_schema.relational_columns[physical].numeric_modifier, column.numeric_modifier)) return error.InvalidRelationalGeneratedValue;
            const cell = (try row.findCell(physical)) orelse continue;
            present[ordinal] = true;
            if (cell.is_null) continue;
            values[ordinal] = switch (column.column_type) {
                .string => .{ .string = cell.value.bytes_val },
                .blob => try decodeBlob(execution, try BlobOperand.fromJson(.{ .string = cell.value.bytes_val })),
                .integer => .{ .integer = cell.value.i64_val },
                .number => .{ .number = cell.value.f64_val },
                .numeric => .{ .numeric = cell.value.bytes_val },
                .sql_array => .{ .sql_array = .{ .element_type = column.sql_element_type orelse return error.InvalidRelationalGeneratedValue, .bytes = cell.value.bytes_val } },
                .boolean => .{ .boolean = cell.value.bool_val },
                .datetime => .{ .datetime = cell.value.u64_val },
                .json => .{ .json = try json_owner.decodePinned(std.math.cast(u32, physical) orelse return error.InvalidRelationalGeneratedValue, cell.value.bytes_val) },
                else => return error.InvalidRelationalGeneratedValue,
            };
        }
        try self.verifyValues(execution, values, present);
    }

    fn verifyValues(self: *const Set, execution: *Execution, values: []const Value, present: []const bool) !void {
        for (self.modifier_ordinals) |ordinal| {
            if (!present[ordinal] or values[ordinal] == .null) continue;
            const modifier = self.table.relational_columns[ordinal].numeric_modifier.?;
            if (self.table.relational_columns[ordinal].column_type == .sql_array) {
                if (values[ordinal] != .sql_array or values[ordinal].sql_array.element_type != .numeric) return error.InvalidRelationalGeneratedValue;
                _ = try normalizeNumericArrayBinding(execution, values[ordinal], modifier, true);
            } else {
                if (values[ordinal] != .numeric) return error.InvalidRelationalGeneratedValue;
                _ = try normalizeNumericBinding(execution, values[ordinal].numeric, modifier, true);
            }
        }
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated) continue;
            if (!present[binding.ordinal]) return error.InvalidRelationalGeneratedValue;
            const expected = try self.normalizeBinding(execution, binding.ordinal, try binding.plan.evaluateWithExecution(execution, values));
            const actual = values[binding.ordinal];
            const equal = generatedDisplayEqual(execution, expected, actual) catch |err| return executionFailure(err);
            if (!equal) return error.InvalidRelationalGeneratedValue;
        }
    }

    /// Stored expressions obey the target domain before dependent expressions
    /// execute. Restore evaluates the identical conversion, never raw float8
    /// arithmetic against an already-rounded float4 physical value.
    fn normalizeBinding(self: *const Set, execution: *Execution, ordinal: usize, value: Value) !Value {
        if (value == .null) return value;
        const column = self.table.relational_columns[ordinal];
        if (column.column_type == .sql_array) {
            if (value != .sql_array or value.sql_array.element_type != column.sql_element_type) return error.InvalidRelationalExpressionInput;
            if (column.numeric_modifier) |modifier| return normalizeNumericArrayBinding(execution, value, modifier, false);
            return value;
        }
        if (column.numeric_modifier) |modifier| {
            if (value != .numeric) return error.InvalidRelationalExpressionInput;
            return normalizeNumericBinding(execution, value.numeric, modifier, false);
        }
        const kind = column.sql_element_type orelse return value;
        return switch (kind) {
            .int16, .int32, .int64 => .{ .integer = casts.checkedInteger(value.integer, kind) catch return error.InvalidRelationalExpressionInput },
            .float32 => .{ .number = casts.floatValue(f32, .{ .float = value.number }) catch return error.InvalidRelationalExpressionInput },
            .uuid => blk: {
                const uuid = @import("../common/uuid.zig");
                const canonical = uuid.format(uuid.parse(value.string) catch return error.InvalidRelationalExpressionInput);
                if (std.mem.eql(u8, value.string, &canonical)) break :blk value;
                break :blk .{ .string = try execution.alloc.dupe(u8, &canonical) };
            },
            else => value,
        };
    }

    fn readValues(self: *const Set, execution: *Execution, document: std.json.Value, values: []Value, present: []bool, ignore_generated: bool, roots: *@import("../sql/row_value.zig").JsonRow, nulls: ?*@import("../sql/row_value.zig").JsonNulls) !void {
        for (self.table.relational_columns, values, present, 0..) |column, *value, *exists, ordinal| {
            const input = document.object.get(column.name);
            exists.* = input != null;
            value.* = .null;
            // Wide unrelated blobs/JSON/vectors never enter expression
            // decoding. The immutable mask is compiled once per schema.
            if (!self.read_columns[ordinal] or (ignore_generated and self.generated_columns[ordinal])) continue;
            if (input) |scalar| switch (column.column_type) {
                .numeric => value.* = try numericJson(execution, scalar),
                .sql_array => value.* = try arrayJson(execution, column.sql_element_type orelse return error.InvalidRelationalExpressionInput, scalar, null),
                .blob => value.* = try decodeBlob(execution, try BlobOperand.fromJson(scalar)),
                .json => {
                    const present_json_null = if (scalar == .null and nulls != null)
                        nulls.?.contains(column.name) catch |err| return executionFailure(err)
                    else
                        false;
                    if (scalar != .null or present_json_null)
                        value.* = .{ .json = roots.pinValue(scalar) catch |err| return executionFailure(err) };
                },
                .string, .boolean, .datetime, .integer, .number => value.* = try scalarJson(execution.alloc, column.column_type, scalar, false),
                else => {},
            };
        }
    }
};

/// Generated restore results retain observable display, not only SQL equality.
/// Encoded NUMERIC/arrays are authenticated canonical values in one active
/// codec; comparing their display metadata never changes semantic row hashes.
fn generatedDisplayEqual(execution: *Execution, expected: Value, actual: Value) !bool {
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return false;
    if (expected == .null) return true;
    if (expected == .json) {
        var work: @import("../sql/json_order.zig").Budget = .{ .shared = &execution.numeric };
        return @import("../sql/json_order.zig").sameDisplay(expected.json.value, actual.json.value, &work, 0);
    }
    if (expected == .numeric) {
        // Validate once, retaining the same borrowed views for value and scale.
        const left = try binary.layout.View.openWithBudget(expected.numeric, .{}, &execution.numeric);
        const right = try binary.layout.View.openWithBudget(actual.numeric, .{}, &execution.numeric);
        return left.scale == right.scale and try left.order(right, &execution.numeric) == .eq;
    }
    if (expected == .sql_array) {
        if (expected.sql_array.element_type != actual.sql_array.element_type) return false;
        // Value.Array is a pinned canonical-codec capability, not producer
        // input. verifyRow's caller already checked canonicality/checksums;
        // parsed envelopes enter through the canonical owning encoder.
        // Exact bytes retain every observable scale/zero bit without decoding
        // JSONB cells again. This is not a storage-format migration identity.
        _ = try expected.sql_array.view();
        _ = try actual.sql_array.view();
        return try orderBytes(expected.sql_array.bytes, actual.sql_array.bytes, false, &execution.numeric) == .eq;
    }
    if (try compareValues(execution, expected, actual, false) != .eq) return false;
    return switch (expected) {
        .number => |number| if (number == 0) std.math.signbit(number) == std.math.signbit(actual.number) else true,
        else => true,
    };
}

test "relational index system generated restore rejects display forgeries with unchanged SQL equality" {
    const a = std.testing.allocator;
    const Helper = struct {
        fn cell(alloc: Allocator, ordinal: u32, column: schema.RelationalColumn, value: Value) !codec.Cell {
            var result: codec.Cell = .{ .ordinal = ordinal, .path = column.path, .value_type = .bytes_val, .value = .{ .bytes_val = &.{} } };
            switch (value) {
                .json => |json| {
                    var work: @import("../sql/json_order.zig").Budget = .{};
                    result.is_json = true;
                    result.value.bytes_val = try @import("../sql/jsonb_text.zig").format(alloc, json.value, max_output_bytes, &work);
                },
                .numeric => |bytes| {
                    result.is_numeric = true;
                    result.value.bytes_val = bytes;
                },
                .sql_array => |array| {
                    result.sql_array_element_type = array.element_type;
                    result.value.bytes_val = array.bytes;
                },
                .number => |number| {
                    result.value_type = .f64_val;
                    result.value = .{ .f64_val = number };
                },
                else => return error.UnexpectedDisplayFixture,
            }
            return result;
        }
        fn check(alloc: Allocator, expected: Value, forged: Value) !void {
            const kind = std.meta.stringToEnum(Kind, @tagName(std.meta.activeTag(expected))) orelse return error.UnexpectedDisplayFixture;
            const element: ?Numeric = switch (expected) {
                .numeric => .numeric,
                .number => .float64,
                .sql_array => |array| array.element_type,
                else => null,
            };
            const columns = [_]schema.RelationalColumn{
                .{ .name = "source", .path = "source", .column_type = kind, .is_json = kind == .json, .json_kind = if (kind == .json) .any else .none, .sql_element_type = element },
                .{ .name = "target", .path = "target", .column_type = kind, .is_json = kind == .json, .json_kind = if (kind == .json) .any else .none, .sql_element_type = element },
            };
            const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = kind == .json, .requires_jsonb_display = kind == .json, .requires_exact_numeric_expressions = kind == .numeric, .requires_exact_numeric_validation = kind == .numeric, .requires_array_expressions = kind == .sql_array, .requires_jsonb_array_display = kind == .sql_array and element == .jsonb, .relational_columns = &columns };
            const bytes = try schema.serializeSchema(alloc, table);
            defer alloc.free(bytes);
            const owned = try schema.deserializeSchema(alloc, bytes);
            const definition = try std.json.parseFromSlice(std.json.Value, alloc, "[{\"column\":\"target\",\"expression\":{\"op\":\"column\",\"column\":\"source\"}}]", .{});
            defer definition.deinit();
            const set = Set.createOwned(alloc, owned, .null, definition.value) catch |err| {
                schema.freeSchema(alloc, owned);
                return err;
            };
            defer set.deinit();
            var region: std.heap.ArenaAllocator = .init(alloc);
            defer region.deinit();
            var remaining: usize = max_allocated_bytes;
            var execution = Execution.init(region.allocator(), &remaining);
            try std.testing.expectEqual(std.math.Order.eq, try compareValues(&execution, expected, forged, false));
            try std.testing.expect(!try generatedDisplayEqual(&execution, expected, forged));
            const present = [_]bool{ true, true };
            try set.verifyValues(&execution, &.{ expected, expected }, &present);
            try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyValues(&execution, &.{ expected, forged }, &present));
            var layout = try codec.PhysicalLayout.init(alloc, set.table);
            defer layout.deinit();
            for ([_]bool{ false, true }) |tampered| {
                const cells = [_]codec.Cell{
                    try cell(region.allocator(), 0, set.table.relational_columns[0], expected),
                    try cell(region.allocator(), 1, set.table.relational_columns[1], if (tampered) forged else expected),
                };
                const row_bytes = try codec.serializePreparedOrdinalDeferredHash(region.allocator(), 1, set.table.relational_columns, &cells, &layout);
                try codec.finalizeOrdinalMetadata(row_bytes, @splat(0), 0);
                const row = try codec.ordinalRowView(row_bytes, set.table, &layout);
                if (tampered) try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRow(region.allocator(), row)) else try set.verifyRow(region.allocator(), row);
            }
        }
    };
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const alloc = region.allocator();
    var parsing: @import("../sql/json_order.zig").Budget = .{};
    const left_json: @import("../sql/row_value.zig").Json = .{ .value = try @import("../sql/json_order.zig").parseTextLeaky(alloc, "{\"n\":1.20}", &parsing) };
    const right_json: @import("../sql/row_value.zig").Json = .{ .value = try @import("../sql/json_order.zig").parseTextLeaky(alloc, "{\"n\":1.2}", &parsing) };
    try Helper.check(a, .{ .json = &left_json }, .{ .json = &right_json });
    var numeric_context: exact.Context = .{ .alloc = alloc };
    var left_number = try exact.parse(&numeric_context, "1.20");
    defer left_number.deinit();
    var right_number = try exact.parse(&numeric_context, "1.2");
    defer right_number.deinit();
    const left_bytes = try binary.encodeAlloc(&numeric_context, left_number.value);
    const right_bytes = try binary.encodeAlloc(&numeric_context, right_number.value);
    try Helper.check(a, .{ .numeric = left_bytes }, .{ .numeric = right_bytes });
    try Helper.check(a, .{ .number = -0.0 }, .{ .number = 0.0 });
    const arrays = @import("../sql/array_value.zig");
    for ([_]Numeric{ .jsonb, .numeric, .float64 }) |element| {
        const left_cell: arrays.Element = switch (element) {
            .jsonb => arrays.Element.json(.{ .number_string = "1.20" }),
            .numeric => arrays.Element.typedNumeric(&left_number.value),
            else => arrays.Element.json(.{ .float = -0.0 }),
        };
        const right_cell: arrays.Element = switch (element) {
            .jsonb => arrays.Element.json(.{ .number_string = "1.2" }),
            .numeric => arrays.Element.typedNumeric(&right_number.value),
            else => arrays.Element.json(.{ .float = 0.0 }),
        };
        const left_array = try @import("../sql/array_storage.zig").encodeAlloc(alloc, try arrays.Value.init(element, &.{.{ .length = 2, .lower = -3 }}, &.{ left_cell, .{} }, .{}), .{});
        const right_array = try @import("../sql/array_storage.zig").encodeAlloc(alloc, try arrays.Value.init(element, &.{.{ .length = 2, .lower = -3 }}, &.{ right_cell, .{} }, .{}), .{});
        try Helper.check(a, .{ .sql_array = .{ .element_type = element, .bytes = left_array } }, .{ .sql_array = .{ .element_type = element, .bytes = right_array } });
    }
}

test "relational index system generated logical restore normalizes typmod display aliases without repairing values" {
    const a = std.testing.allocator;
    for ([_]Kind{ .numeric, .sql_array }) |kind| {
        const columns = [_]schema.RelationalColumn{
            .{ .name = "source", .path = "source", .column_type = kind, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
            .{ .name = "target", .path = "target", .column_type = kind, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        };
        const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_exact_numeric_expressions = kind == .numeric, .requires_exact_numeric_validation = kind == .numeric, .requires_array_expressions = kind == .sql_array, .requires_numeric_modifiers = true, .relational_columns = &columns };
        const bytes = try schema.serializeSchema(a, table);
        defer a.free(bytes);
        const owned = try schema.deserializeSchema(a, bytes);
        const definition = try std.json.parseFromSlice(std.json.Value, a, "[{\"column\":\"target\",\"expression\":{\"op\":\"column\",\"column\":\"source\"}}]", .{});
        defer definition.deinit();
        const set = Set.createOwned(a, owned, .null, definition.value) catch |err| {
            schema.freeSchema(a, owned);
            return err;
        };
        defer set.deinit();
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        const input = if (kind == .numeric) "{\"source\":1.2,\"target\":1.2}" else "{\"source\":{\"dimensions\":[{\"length\":1,\"lower_bound\":-2}],\"values\":[\"1.2\"],\"sql_nulls\":[false]},\"target\":{\"dimensions\":[{\"length\":1,\"lower_bound\":-2}],\"values\":[\"1.2\"],\"sql_nulls\":[false]}}";
        const document = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), input, .{ .parse_numbers = false });
        const source = if (kind == .numeric) document.object.getPtr("source").? else &document.object.getPtr("source").?.object.getPtr("values").?.array.items[0];
        const target = if (kind == .numeric) document.object.getPtr("target").? else &document.object.getPtr("target").?.object.getPtr("values").?.array.items[0];
        try set.verifyJson(region.allocator(), document);
        try std.testing.expectEqualStrings("1.2", if (kind == .numeric) source.number_string else source.string);
        try std.testing.expectEqualStrings("1.2", if (kind == .numeric) target.number_string else target.string);
        target.* = if (kind == .numeric) .{ .number_string = "1.3" } else .{ .string = "1.3" };
        try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyJson(region.allocator(), document));
        source.* = if (kind == .numeric) .{ .number_string = "1.234" } else .{ .string = "1.234" };
        target.* = source.*;
        try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyJson(region.allocator(), document));
    }
}

test "relational index system generated array display verification streams authenticated bytes without allocations" {
    const a = std.testing.allocator;
    const arrays = @import("../sql/array_value.zig");
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const json = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"n\":1.20,\"a\":[null,true]}", .{ .parse_numbers = false });
    const cells = try region.allocator().alloc(arrays.Element, 1000);
    @memset(cells, arrays.Element.json(json));
    const encoded = try @import("../sql/array_storage.zig").encodeAlloc(region.allocator(), try arrays.Value.init(.jsonb, &.{.{ .length = 1000 }}, cells, .{}), .{});
    const value: Value = .{ .sql_array = .{ .element_type = .jsonb, .bytes = encoded } };
    const independent = try region.allocator().dupe(u8, encoded);
    const other: Value = .{ .sql_array = .{ .element_type = .jsonb, .bytes = independent } };
    try std.testing.expect(encoded.ptr != independent.ptr);
    var old_bytes: usize = max_allocated_bytes;
    var old = Execution.init(a, &old_bytes);
    const old_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..8) |_| try std.testing.expectEqual(std.math.Order.eq, try compareValues(&old, value, other, false));
    const old_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - old_start;
    var new_bytes: usize = max_allocated_bytes;
    var current = Execution.init(.failing, &new_bytes);
    const new_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..8) |_| try std.testing.expect(try generatedDisplayEqual(&current, value, other));
    const new_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - new_start;
    try std.testing.expectEqual(max_allocated_bytes, new_bytes);
    std.debug.print("generated JSONB array verification: cells=1000 iterations=8 bytes={d} old_logical_ns={d} display_ns={d} display_allocations=0\n", .{ encoded.len, old_ns, new_ns });
    const cost = 8 * 1024 * 1024 - current.numeric.remaining;
    current.numeric.remaining = cost / 8;
    try std.testing.expect(try generatedDisplayEqual(&current, value, other));
    try std.testing.expectEqual(@as(u64, 0), current.numeric.remaining);
    current.numeric.remaining = cost / 8 - 1;
    try std.testing.expectError(error.SqlProgramLimitExceeded, generatedDisplayEqual(&current, value, other));
    try std.testing.expectError(error.SqlProgramLimitExceeded, generatedDisplayEqual(&current, value, other));
}

fn valuesEqual(a: Value, b: Value) !bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .string => |bytes| std.mem.eql(u8, bytes, b.string),
        .blob => |bytes| std.mem.eql(u8, bytes, b.blob),
        .numeric, .sql_array, .json => try valueOrder(a, b, false) == .eq,
        .integer => |value| value == b.integer,
        .number => |value| value == b.number,
        .boolean => |value| value == b.boolean,
        .datetime => |value| value == b.datetime,
    };
}

/// Compile and execute a row-independent program without another evaluator or
/// type-promotion implementation. Compilation, evaluation and output share one
/// caller-owned work/byte budget. All temporary plan storage dies on return.
pub fn foldConstantJson(execution: *Execution, expression: std.json.Value) !std.json.Value {
    return foldConstantJsonImpl(execution, expression) catch |err| {
        const failure = executionFailure(err);
        if (failure == error.RelationalExpressionBudgetExceeded) execution.numeric.failure = error.SqlProgramLimitExceeded;
        return failure;
    };
}

fn foldConstantJsonImpl(execution: *Execution, expression: std.json.Value) !std.json.Value {
    try execution.numeric.charge(1);
    var memory: @import("../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    const plan: Plan = compile: {
        var compile_bytes = execution.bytes.*;
        var compilation = execution.*;
        compilation.alloc = arena.allocator();
        compilation.bytes = &compile_bytes;
        compilation.numeric.alloc = compilation.alloc;
        defer {
            const numeric_alloc = execution.numeric.alloc;
            execution.numeric = compilation.numeric;
            execution.numeric.alloc = numeric_alloc;
            // Charge real arena capacity, including failed compilation. This
            // is monotonic even when an outer arena cannot reclaim frees.
            execution.bytes.* -|= memory.footprint();
        }
        var compiler: Compiler = .{ .alloc = compilation.alloc, .table = .{ .storage_mode = .relational }, .execution = &compilation };
        compiler.hash.update("antfly immutable scalar expression v1");
        const root = compiler.compile(expression, 0) catch |err| {
            if (err == error.OutOfMemory and memory.isExhausted()) return error.RelationalExpressionBudgetExceeded;
            return err;
        };
        try compilation.numeric.charge(compiler.visited);
        // The empty binding environment rejects even a column in a lazy arm.
        std.debug.assert(compiler.dependencies.items.len == 0);
        var fingerprint: [32]u8 = undefined;
        compiler.hash.final(&fingerprint);
        break :compile .{ .arena = arena, .nodes = compiler.nodes.items, .dependencies = &.{}, .fingerprint = fingerprint, .result_kind = compiler.nodes.items[root].kind, .result_sql_type = compiler.nodes.items[root].sql_type, .literal_bytes = compiler.literal_bytes };
    };
    const result = try plan.evaluateWithExecution(execution, &.{});
    return boundedValueToJson(execution, result);
}

test "relational declarations constant folding owns output and shares sticky admission" {
    const a = std.testing.allocator;
    var literal = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"literal","type":"string","value":"survives compilation"}
    , .{});
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(a, &bytes);
    const output = try foldConstantJson(&execution, literal.value);
    defer a.free(output.string);
    literal.deinit();
    try std.testing.expectEqualStrings("survives compilation", output.string);
    try std.testing.expect(bytes < max_allocated_bytes);

    var numeric = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"cast","type":"integer","sql_type":"int64","args":[{"op":"literal","type":"numeric","value":"9007199254740993.5"}]}
    , .{});
    defer numeric.deinit();
    const Run = struct {
        fn run(alloc: Allocator, expression: std.json.Value) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var remaining: usize = max_allocated_bytes;
            var scope = Execution.init(arena.allocator(), &remaining);
            const before = scope.numeric.remaining;
            for (0..2) |_| {
                const value = try foldConstantJson(&scope, expression);
                try std.testing.expectEqual(@as(i64, 9007199254740994), value.integer);
            }
            const decimal = try foldConstantJson(&scope, expression.object.get("args").?.array.items[0]);
            try std.testing.expectEqualStrings("9007199254740993.5", decimal.number_string);
            const text = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(),
                \\{"op":"concat","args":[{"op":"literal","type":"string","value":"left"},{"op":"literal","type":"string","value":"right"}]}
            , .{});
            const joined = try foldConstantJson(&scope, text);
            try std.testing.expectEqualStrings("leftright", joined.string);
            try std.testing.expect(scope.numeric.remaining < before);
            scope.numeric.remaining = 0;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&scope, expression));
            scope.numeric.remaining = before;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&scope, expression));
        }
    };
    try Run.run(a, numeric.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{numeric.value});

    var tiny: usize = 1;
    var limited = Execution.init(a, &tiny);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&limited, numeric.value));
    tiny = max_allocated_bytes;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&limited, numeric.value));

    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    var canceled = Execution.init(a, &tiny);
    canceled.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, foldConstantJson(&canceled, numeric.value));
    canceled.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, foldConstantJson(&canceled, numeric.value));
}

fn boundedValueToJson(execution: *Execution, value: Value) !std.json.Value {
    return switch (value) {
        .json => |json| cloneJsonOutput(execution, json.*),
        .numeric => |bytes| numericJsonOutput(execution, bytes),
        .sql_array => |array| arrayJsonOutputLeaky(execution, array),
        .string => |bytes| blk: {
            const output = try allocateOutput(execution.alloc, bytes.len, execution.bytes);
            @memcpy(output, bytes);
            break :blk .{ .string = output };
        },
        .blob => |bytes| blk: {
            const output = try allocateOutput(execution.alloc, std.base64.standard.Encoder.calcSize(bytes.len), execution.bytes);
            break :blk .{ .string = std.base64.standard.Encoder.encode(output, bytes) };
        },
        .datetime => |datetime| blk: {
            var buffer: [40]u8 = undefined;
            const text = try std.fmt.bufPrint(&buffer, "{d}", .{datetime});
            const output = try allocateOutput(execution.alloc, text.len, execution.bytes);
            @memcpy(output, text);
            break :blk .{ .number_string = output };
        },
        else => valueToJson(execution.alloc, value),
    };
}

fn retainPreparedResult(execution: *Execution, value: Value, decoded: bool) !Value {
    // JSON headers themselves can belong to the temporary owner even when
    // their ingress DOM is borrowed. Always own the whole returned tree.
    if (value == .json) {
        const Json = @import("../sql/row_value.zig").Json;
        try execution.charge(1);
        if (@sizeOf(Json) > execution.bytes.*) return execution.limit();
        execution.bytes.* -= @sizeOf(Json);
        const output = try execution.alloc.create(Json);
        errdefer execution.alloc.destroy(output);
        output.* = .{ .value = try cloneJsonOutput(execution, value.json.*) };
        return .{ .json = output };
    }
    // Ordinary scalar programs still borrow cold/ingress scalar payloads and
    // allocate nothing here. Once JSON was decoded, a derived scalar may point
    // into its tree. Copy at this lifetime boundary rather than escaping it.
    if (!decoded) return value;
    return switch (value) {
        inline .string, .blob, .numeric => |bytes, tag| @unionInit(Value, @tagName(tag), try retainPreparedBytes(execution, bytes)),
        .sql_array => |array| .{ .sql_array = .{ .element_type = array.element_type, .bytes = try retainPreparedBytes(execution, array.bytes) } },
        else => value,
    };
}

fn retainPreparedBytes(execution: *Execution, bytes: []const u8) ![]const u8 {
    try execution.charge(bytes.len);
    const output = try allocateOutput(execution.alloc, bytes.len, execution.bytes);
    var offset: usize = 0;
    while (offset < bytes.len) {
        const end = offset + @min(bytes.len - offset, 256);
        try execution.numeric.pollCheckpoint(end - offset);
        @memcpy(output[offset..end], bytes[offset..end]);
        offset = end;
    }
    return output;
}

fn cloneJsonOutput(execution: *Execution, json: @import("../sql/row_value.zig").Json) !std.json.Value {
    var memory: @import("../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.*, .monotonic = true };
    defer execution.bytes.* -|= memory.footprint();
    var work: @import("../sql/json_order.zig").Budget = .{ .shared = &execution.numeric };
    var output = (json.cloneLeaky(memory.allocator(), &work) catch |err| return if (err == error.OutOfMemory and memory.isExhausted()) execution.limit() else executionFailure(err)).value;
    // Never retain the temporary admission allocator in managed JSON arrays.
    @import("../sql/json_order.zig").rehomeArrayAllocators(&output, execution.alloc, &work, 0) catch |err| return executionFailure(err);
    return output;
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
        .numeric => |bytes| @import("../sql/numeric_storage.zig").jsonValueAlloc(alloc, bytes),
        .boolean => |boolean| .{ .bool = boolean },
        .datetime => |datetime| .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{datetime}) },
        .sql_array => |array| blk: {
            var allowance: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &allowance);
            break :blk arrayJsonOutputLeaky(&execution, array);
        },
        .json => |json| blk: {
            var allowance: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &allowance);
            break :blk cloneJsonOutput(&execution, json.*);
        },
    };
}

test "relational index system legacy preparation is bound once to operand source shape" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "s", .path = "s", .column_type = .string },
        .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int64 },
        .{ .name = "b", .path = "b", .column_type = .blob },
    } };
    for ([_]struct { name: []const u8, kind: Kind, mask: u8 }{
        .{ .name = "s", .kind = .string, .mask = 0 },
        .{ .name = "n", .kind = .numeric, .mask = 1 },
        .{ .name = "a", .kind = .sql_array, .mask = 1 },
        .{ .name = "b", .kind = .blob, .mask = 3 },
    }) |case| {
        const source = try std.fmt.allocPrint(a, "{{\"op\":\"column\",\"column\":\"{s}\"}}", .{case.name});
        defer a.free(source);
        var definition = try std.json.parseFromSlice(std.json.Value, a, source, .{});
        defer definition.deinit();
        var plan = try Plan.init(a, table, definition.value, case.kind);
        defer plan.deinit();
        try std.testing.expectEqual(case.mask, plan.prepared_sources);
        if (case.kind == .string) {
            var document = try std.json.parseFromSlice(std.json.Value, a, "{\"s\":\"borrowed\"}", .{});
            defer document.deinit();
            var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(denied.allocator(), &bytes);
            const result = try plan.evaluateJsonWithExecution(&execution, document.value);
            try std.testing.expectEqual(document.value.object.get("s").?.string.ptr, result.string.ptr);
            try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
            try std.testing.expectEqual(max_allocated_bytes, bytes);
        }
    }
}

test "relational index system durable JSON functions match PostgreSQL path contracts behind capability" {
    const a = std.testing.allocator;
    const Fixture = struct { entries: []const struct { sql: []const u8, value: std.json.Value = .null, sql_null: ?bool = null, oid: ?u32 = null, @"error": ?[]const u8 = null } };
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_json_path_reference.json"), .{ .ignore_unknown_fields = true, .parse_numbers = false });
    defer fixture.deinit();
    // A test-only structural bridge feeds already typed native programs from
    // the independent SQL oracle. It does not activate public SQL lowering.
    const Native = struct {
        fn expression(alloc: Allocator, input: *const @import("../sql/ast.zig").Scalar, expected: Kind) anyerror!std.json.Value {
            var result: std.json.ObjectMap = .empty;
            switch (input.*) {
                .literal => |raw| {
                    var value: std.json.Value = switch (raw) {
                        .null => .null,
                        .string => |text| .{ .string = text },
                        else => return error.UnexpectedJsonFixture,
                    };
                    if (expected == .json and raw != .null) {
                        if (raw != .string) return error.UnexpectedJsonFixture;
                        var work: @import("../sql/json_order.zig").Budget = .{};
                        value = try @import("../sql/json_order.zig").parseTextLeaky(alloc, raw.string, &work);
                        if (value == .null) try result.put(alloc, "json_null", .{ .bool = true });
                    }
                    try result.put(alloc, "op", .{ .string = "literal" });
                    try result.put(alloc, "type", .{ .string = @tagName(expected) });
                    try result.put(alloc, "value", value);
                },
                .call => |call| {
                    const op = std.meta.stringToEnum(Op, call.name) orelse return error.UnexpectedJsonFixture;
                    if (!jsonFunction(op)) return error.UnexpectedJsonFixture;
                    var args = std.json.Array.init(alloc);
                    for (call.args, 0..) |arg, i| try args.append(try expression(alloc, arg, if (i == 0) .json else .string));
                    try result.put(alloc, "op", .{ .string = @tagName(op) });
                    try result.put(alloc, "args", .{ .array = args });
                },
                else => return error.UnexpectedJsonFixture,
            }
            return .{ .object = result };
        }
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = true };
    var checked: usize = 0;
    for (fixture.value.entries) |entry| {
        if (entry.@"error" != null) continue; // SQL signature/input errors remain the binder's contract.
        errdefer std.debug.print("native JSON oracle: {s}\n", .{entry.sql});
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        var parsed = try @import("../sql/compiler.zig").compileScalar(a, entry.sql, .{});
        defer parsed.deinit();
        const kind: Kind = switch (entry.oid.?) {
            3802 => .json,
            25 => .string,
            23 => .integer,
            else => return error.UnexpectedJsonFixture,
        };
        const definition = try Native.expression(region.allocator(), parsed.expression, kind);
        var plan = try Plan.init(a, table, definition, kind);
        defer plan.deinit();
        const actual = try plan.evaluate(region.allocator(), &.{});
        try std.testing.expectEqual(entry.sql_null orelse (entry.value == .null), actual == .null);
        const rendered = try valueToJson(region.allocator(), actual);
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(region.allocator(), entry.value, .{}), try std.json.Stringify.valueAlloc(region.allocator(), rendered, .{}));
        if (kind == .integer) try std.testing.expectEqual(@as(?Numeric, .int32), plan.result_sql_type);
        var guarded = table;
        guarded.requires_json_expressions = false;
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, guarded, definition, kind));
        checked += 1;
    }
    try std.testing.expectEqual(@as(usize, 25), checked);
}

test "relational index system durable JSON columns reuse pinned roots and fuse structural consumers" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_jsonb_display = true, .requires_json_expressions = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const bytes = try codec.serializePreparedOrdinalDeferredHash(a, 1, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "{\"k\":\"owned\",\"n\":1.20,\"a\":[null,{\"s\":\"owned\"}]}" } },
    }, &layout);
    defer a.free(bytes);
    try codec.finalizeOrdinalMetadata(bytes, @splat(0), 0);
    const cold = try codec.ordinalRowView(bytes, table, &layout);
    const Compile = struct {
        fn run(backing: Allocator, binding: schema.TableSchema, source: []const u8, kind: Kind) !Plan {
            const parsed = try std.json.parseFromSlice(std.json.Value, backing, source, .{});
            defer parsed.deinit();
            return Plan.init(backing, binding, parsed.value, kind);
        }
    };
    var text = try Compile.run(a, table, "{\"op\":\"jsonb_extract_path_text\",\"args\":[{\"op\":\"column\",\"column\":\"j\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"k\"}]}", .string);
    defer text.deinit();
    var length = try Compile.run(a, table, "{\"op\":\"jsonb_array_length\",\"args\":[{\"op\":\"jsonb_extract_path\",\"args\":[{\"op\":\"column\",\"column\":\"j\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"a\"}]}]}", .integer);
    defer length.deinit();
    var json = try Compile.run(a, table, "{\"op\":\"jsonb_extract_path\",\"args\":[{\"op\":\"column\",\"column\":\"j\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"a\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"-1\"}]}", .json);
    defer json.deinit();
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    var allowance: usize = max_allocated_bytes;
    var execution = Execution.init(region.allocator(), &allowance);
    var input: PreparedInput = undefined;
    try input.initRow(&execution, cold);
    defer input.deinit();
    const first = try text.evaluatePreparedWithExecution(&execution, &input);
    try std.testing.expectEqualStrings("owned", first.string);
    try std.testing.expectEqual(@as(i64, 2), (try length.evaluatePreparedWithExecution(&execution, &input)).integer);
    try std.testing.expectEqual(@as(usize, 1), input.preparations);
    try std.testing.expectEqual(@as(usize, 1), input.roots.decodes);
    const before = allowance;
    execution.alloc = .failing;
    execution.numeric.alloc = .failing;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const reused = try text.evaluatePreparedWithExecution(&execution, &input);
        try std.testing.expectEqual(first.string.ptr, reused.string.ptr);
        try std.testing.expectEqual(@as(i64, 2), (try length.evaluatePreparedWithExecution(&execution, &input)).integer);
    }
    std.debug.print("durable JSON pinned path: evaluations=20000 decodes={d} output_allocations=0 elapsed_ns={d}\n", .{ input.roots.decodes, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
    try std.testing.expectEqual(before, allowance);
    execution.alloc = region.allocator();
    execution.numeric.alloc = region.allocator();
    const selected = try json.evaluatePreparedWithExecution(&execution, &input);
    try std.testing.expectEqualStrings("owned", selected.json.value.object.get("s").?.string);
    // Legacy retained results must outlive the temporary decoded-root owner
    // and the physical source. Mutating the retired source proves no alias.
    const retired = try a.dupe(u8, bytes);
    defer a.free(retired);
    const source = try codec.ordinalRowView(retired, table, &layout);
    const owned_text = try text.evaluateRowWithExecution(&execution, source);
    const owned_json = try json.evaluateBoundRowWithExecution(&execution, source);
    @memset(retired, 0);
    try std.testing.expectEqualStrings("owned", owned_text.string);
    try std.testing.expectEqualStrings("owned", owned_json.json.value.object.get("s").?.string);
    var reordered = table;
    reordered.version = 99;
    reordered.relational_columns = &.{
        .{ .name = "other", .path = "other", .column_type = .string },
        table.relational_columns[0],
    };
    var same = try Compile.run(a, reordered, "{\"op\":\"jsonb_extract_path_text\",\"args\":[{\"op\":\"column\",\"column\":\"j\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"k\"}]}", .string);
    defer same.deinit();
    try std.testing.expectEqualSlices(u8, &text.fingerprint, &same.fingerprint);
    try std.testing.expectEqual(@as(u32, 1), same.dependencies[0]);
    try std.testing.expectEqualStrings("owned", (try same.evaluateRowWithExecution(&execution, cold)).string);
    const hot = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"j\":{\"k\":\"owned\",\"a\":[null,{\"s\":\"owned\"}]}}", .{});
    var hot_input: PreparedInput = undefined;
    try hot_input.initJson(&execution, hot, null);
    defer hot_input.deinit();
    const hot_text = try text.evaluatePreparedWithExecution(&execution, &hot_input);
    try std.testing.expectEqual(hot.object.get("j").?.object.get("k").?.string.ptr, hot_text.string.ptr);
    try std.testing.expectEqual(@as(i64, 2), (try length.evaluatePreparedWithExecution(&execution, &hot_input)).integer);
    try std.testing.expectEqual(@as(usize, 1), hot_input.preparations);
    try std.testing.expectEqual(@as(usize, 0), hot_input.roots.decodes);
}

test "relational index system durable JSON functions keep strict selection type and null provenance" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    const Compile = struct {
        fn run(backing: Allocator, binding: schema.TableSchema, source: []const u8, kind: Kind) !Plan {
            const parsed = try std.json.parseFromSlice(std.json.Value, backing, source, .{ .parse_numbers = false });
            defer parsed.deinit();
            return Plan.init(backing, binding, parsed.value, kind);
        }
    };
    for ([_]struct { value: []const u8, expected: []const u8 }{
        .{ .value = "null", .expected = "null" },
        .{ .value = "true", .expected = "boolean" },
        .{ .value = "1.20", .expected = "number" },
        .{ .value = "\"x\"", .expected = "string" },
        .{ .value = "[]", .expected = "array" },
        .{ .value = "{}", .expected = "object" },
    }) |case| {
        const definition = try std.fmt.allocPrint(a, "{{\"op\":\"jsonb_typeof\",\"args\":[{{\"op\":\"literal\",\"type\":\"json\",\"value\":{s}{s}}}]}}", .{ case.value, if (std.mem.eql(u8, case.value, "null")) ",\"json_null\":true" else "" });
        defer a.free(definition);
        var plan = try Compile.run(a, table, definition, .string);
        defer plan.deinit();
        try std.testing.expectEqualStrings(case.expected, (try plan.evaluate(.failing, &.{})).string);
    }
    for ([_][]const u8{ "null", "true", "1", "\"text\"", "{}" }) |scalar| {
        const definition = try std.fmt.allocPrint(a, "{{\"op\":\"jsonb_array_length\",\"args\":[{{\"op\":\"literal\",\"type\":\"json\",\"value\":{s}{s}}}]}}", .{ scalar, if (std.mem.eql(u8, scalar, "null")) ",\"json_null\":true" else "" });
        defer a.free(definition);
        var plan = try Compile.run(a, table, definition, .integer);
        defer plan.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionInput, plan.evaluate(.failing, &.{}));
    }
    for ([_][]const u8{
        "{\"op\":\"jsonb_extract_path\",\"args\":[{\"op\":\"column\",\"column\":\"j\"}]}",
        "{\"op\":\"jsonb_extract_path\",\"args\":[{\"op\":\"column\",\"column\":\"j\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}]}",
        "{\"op\":\"jsonb_extract_path\",\"args\":[{\"op\":\"literal\",\"type\":\"string\",\"value\":\"{}\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"a\"}]}",
    }, 0..) |invalid, i| {
        try std.testing.expectError(if (i == 0) error.InvalidRelationalExpression else error.InvalidRelationalExpressionType, Compile.run(a, table, invalid, .json));
    }
    // Missing/NULL path components must not suppress a later selected error.
    const failing_key = "{\"op\":\"case_when\",\"args\":[{\"op\":\"eq\",\"args\":[{\"op\":\"divide\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1},{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]},{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"a\"},{\"op\":\"literal\",\"type\":\"string\",\"value\":\"b\"}]}";
    for ([_][]const u8{ "null", "{}" }) |source| for ([_][]const u8{ "null", "\"missing\"" }) |key| {
        const definition = try std.fmt.allocPrint(a, "{{\"op\":\"jsonb_extract_path_text\",\"args\":[{{\"op\":\"literal\",\"type\":\"json\",\"value\":{s}}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":{s}}},{s}]}}", .{ source, key, failing_key });
        defer a.free(definition);
        var plan = try Compile.run(a, table, definition, .string);
        defer plan.deinit();
        try std.testing.expectError(error.RelationalExpressionDivisionByZero, plan.evaluate(.failing, &.{}));
    };
    const guarded_failure = try std.fmt.allocPrint(a, "{{\"op\":\"jsonb_extract_path_text\",\"args\":[{{\"op\":\"literal\",\"type\":\"json\",\"value\":null}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":null}},{s}]}}", .{failing_key});
    defer a.free(guarded_failure);
    const lazy = try std.fmt.allocPrint(a, "{{\"op\":\"coalesce\",\"args\":[{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"ok\"}},{s}]}}", .{guarded_failure});
    defer a.free(lazy);
    var selected = try Compile.run(a, table, lazy, .string);
    defer selected.deinit();
    try std.testing.expectEqualStrings("ok", (try selected.evaluate(.failing, &.{})).string);
    var typeof = try Compile.run(a, table, "{\"op\":\"jsonb_typeof\",\"args\":[{\"op\":\"column\",\"column\":\"j\"}]}", .string);
    defer typeof.deinit();
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    var allowance: usize = max_allocated_bytes;
    var execution = Execution.init(region.allocator(), &allowance);
    const document = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), "{\"j\":null}", .{});
    var markers: @import("../sql/row_value.zig").JsonNulls = undefined;
    markers.initRegion(region.allocator(), allowance, &execution.numeric);
    defer markers.deinit();
    try markers.bindAdmission(&allowance);
    try markers.import(&.{"j"});
    var present: PreparedInput = undefined;
    try present.initJson(&execution, document, &markers);
    defer present.deinit();
    try std.testing.expectEqualStrings("null", (try typeof.evaluatePreparedWithExecution(&execution, &present)).string);
    var sql_null: PreparedInput = undefined;
    try sql_null.initJson(&execution, document, null);
    defer sql_null.deinit();
    try std.testing.expect((try typeof.evaluatePreparedWithExecution(&execution, &sql_null)) == .null);
}

test "relational index system durable JSON formatting and retention unwind every allocation and cancellation cut" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_jsonb_display = true, .requires_json_expressions = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const wide: [1024]u8 = @splat('x');
    const raw = try std.fmt.allocPrint(a, "{{\"a\":{{\"k\":\"{s}\",\"n\":1.20}}}}", .{&wide});
    defer a.free(raw);
    const bytes = try codec.serializePreparedOrdinalDeferredHash(a, 1, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = raw } },
    }, &layout);
    defer a.free(bytes);
    try codec.finalizeOrdinalMetadata(bytes, @splat(0), 0);
    const cold = try codec.ordinalRowView(bytes, table, &layout);
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = 0,
        fn check(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const Run = struct {
        fn run(backing: Allocator, binding: schema.TableSchema, source: codec.OrdinalRowView, kind: Kind, control: *Control) !void {
            const definition = try std.fmt.allocPrint(backing, "{{\"op\":\"{s}\",\"args\":[{{\"op\":\"column\",\"column\":\"j\"}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"a\"}}]}}", .{if (kind == .json) "jsonb_extract_path" else "jsonb_extract_path_text"});
            defer backing.free(definition);
            const parsed = try std.json.parseFromSlice(std.json.Value, backing, definition, .{});
            defer parsed.deinit();
            var plan = try Plan.init(backing, binding, parsed.value, kind);
            defer plan.deinit();
            var region: std.heap.ArenaAllocator = .init(backing);
            defer region.deinit();
            var remaining: usize = max_allocated_bytes;
            var execution = Execution.init(region.allocator(), &remaining);
            execution.numeric.checkpoint = Control.check;
            execution.numeric.ptr = control;
            const value = plan.evaluateRowWithExecution(&execution, source) catch |err| {
                if (err == error.QueryCanceled) {
                    try std.testing.expectError(error.QueryCanceled, execution.charge(0));
                    try std.testing.expectError(error.QueryCanceled, plan.evaluateRowWithExecution(&execution, source));
                }
                return err;
            };
            if (kind == .json) {
                try std.testing.expectEqualStrings("1.20", value.json.value.object.get("n").?.number_string);
            } else {
                try std.testing.expect(std.mem.endsWith(u8, value.string, "\", \"n\": 1.20}"));
            }
        }
    };
    for ([_]Kind{ .json, .string }) |kind| {
        var observed: Control = .{};
        try Run.run(a, table, cold, kind, &observed);
        for (1..observed.polls + 1) |cut| {
            var canceled: Control = .{ .fail_at = cut };
            try std.testing.expectError(error.QueryCanceled, Run.run(a, table, cold, kind, &canceled));
        }
        var healthy: Control = .{};
        var stable: std.testing.FailingAllocator = .init(a, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ table, cold, kind, &healthy });
    }
}

test "relational index system durable JSON generated text shares typed preparation identity and restore verification" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_jsonb_display = true, .requires_json_expressions = true, .relational_columns = &.{
        .{ .name = "metadata", .path = "metadata", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "source_key", .path = "source_key", .column_type = .string },
    } };
    const Compile = struct {
        fn run(alloc: Allocator, binding: schema.TableSchema, key: []const u8) !*Set {
            const bytes = try schema.serializeSchema(alloc, binding);
            defer alloc.free(bytes);
            const owned = try schema.deserializeSchema(alloc, bytes);
            errdefer schema.freeSchema(alloc, owned);
            const definition = try std.fmt.allocPrint(alloc, "[{{\"column\":\"source_key\",\"expression\":{{\"op\":\"jsonb_extract_path_text\",\"args\":[{{\"op\":\"column\",\"column\":\"metadata\"}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"{s}\"}}]}}}}]", .{key});
            defer alloc.free(definition);
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, definition, .{});
            defer parsed.deinit();
            return Set.createOwned(alloc, owned, .null, parsed.value);
        }
    };
    const set = try Compile.run(a, table, "source");
    defer set.deinit();
    const changed = try Compile.run(a, table, "other");
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &generatedFingerprint(set), &generatedFingerprint(changed)));
    var moved_table = table;
    moved_table.version = 999;
    moved_table.relational_columns = &.{ table.relational_columns[1], table.relational_columns[0] };
    const moved = try Compile.run(a, moved_table, "source");
    defer moved.deinit();
    try std.testing.expectEqualSlices(u8, &generatedFingerprint(set), &generatedFingerprint(moved));
    for ([_]struct { json: []const u8, expected: ?[]const u8 }{
        .{ .json = "{\"metadata\":{\"source\":1.2300e2},\"source_key\":\"forged\"}", .expected = "123.00" },
        .{ .json = "{\"metadata\":{\"source\":true}}", .expected = "true" },
        .{ .json = "{\"metadata\":{\"source\":null}}", .expected = null },
        .{ .json = "{\"metadata\":{}}", .expected = null },
    }) |case| {
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        var document = try std.json.parseFromSliceLeaky(std.json.Value, region.allocator(), case.json, .{ .parse_numbers = false });
        var remaining: usize = max_allocated_bytes;
        var execution = Execution.init(region.allocator(), &remaining);
        var roots: @import("../sql/row_value.zig").JsonRow = undefined;
        roots.initRegion(region.allocator(), remaining, &execution.numeric);
        defer roots.deinit();
        try roots.bindAdmission(&remaining);
        var nulls: @import("../sql/row_value.zig").JsonNulls = undefined;
        nulls.initRegion(region.allocator(), remaining, &execution.numeric);
        defer nulls.deinit();
        try nulls.bindAdmission(&remaining);
        try set.applyTypedJsonWithExecution(&execution, &document, &roots, &nulls);
        if (case.expected) |text| try std.testing.expectEqualStrings(text, document.object.get("source_key").?.string) else try std.testing.expect(document.object.get("source_key").? == .null);
        const markers = try nulls.snapshot();
        try set.verifyTypedJsonWithExecution(&execution, document, markers);
        try document.object.put(region.allocator(), "source_key", .{ .string = "forged" });
        try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyTypedJsonWithExecution(&execution, document, markers));
        try std.testing.expectEqual(@as(usize, 0), roots.decodes);
    }
}

test "relational index system JSON literal ownership and display identity require explicit capability" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = true };
    const input = "{\"op\":\"literal\",\"type\":\"json\",\"value\":{\"b\":[null,[\"owned\"]],\"a\":1.20}}";
    const Compile = struct {
        fn run(backing: Allocator, binding: schema.TableSchema, source: []const u8) !Plan {
            var parsed = try std.json.parseFromSlice(std.json.Value, backing, source, .{ .parse_numbers = false });
            defer parsed.deinit();
            return Plan.init(backing, binding, parsed.value, .json);
        }
    };
    const Run = struct {
        fn run(backing: Allocator, binding: schema.TableSchema, source: []const u8) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const owned = region.allocator();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(owned, &bytes);
            const output = blk: {
                var plan = try Compile.run(backing, binding, source);
                defer plan.deinit();
                try std.testing.expectEqual(@as(u8, 0), plan.prepared_sources);
                // An explicit pinned-plan caller borrows the immutable tree
                // without allocating. Moving the Plan leaves no stack arena
                // allocator embedded in its JSON array metadata.
                const literal = try plan.evaluate(.failing, &.{});
                try std.testing.expectEqualStrings("1.20", literal.json.value.object.get("a").?.number_string);
                try std.testing.expectEqual(Allocator.failing.vtable, literal.json.value.object.get("b").?.array.allocator.vtable);
                try std.testing.expectEqual(Allocator.failing.vtable, literal.json.value.object.get("b").?.array.items[1].array.allocator.vtable);
                try std.testing.expectEqual(literal.json, (try plan.evaluate(.failing, &.{})).json);
                break :blk try plan.evaluateJsonWithExecution(&execution, .{ .object = .empty });
            };
            try std.testing.expectEqualStrings("owned", output.json.value.object.get("b").?.array.items[1].array.items[0].string);
            try std.testing.expectEqualStrings("1.20", output.json.value.object.get("a").?.number_string);
            try std.testing.expectEqual(owned.ptr, output.json.value.object.get("b").?.array.allocator.ptr);
            const nested = &@constCast(output.json).value.object.getPtr("b").?.array.items[1].array;
            try std.testing.expectEqual(owned.ptr, nested.allocator.ptr);
            try nested.ensureTotalCapacity(nested.capacity + 16);
        }
    };
    try Run.run(a, table, input);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ table, input });
    var first = try Compile.run(a, table, input);
    defer first.deinit();
    var equivalent = try Compile.run(a, table, "{\"op\":\"literal\",\"type\":\"json\",\"value\":{\"a\":120e-2,\"b\":[null,[\"owned\"]]}}");
    defer equivalent.deinit();
    try std.testing.expectEqualSlices(u8, &first.fingerprint, &equivalent.fingerprint);
    var different_display = try Compile.run(a, table, "{\"op\":\"literal\",\"type\":\"json\",\"value\":{\"a\":1.2,\"b\":[null,[\"owned\"]]}}");
    defer different_display.deinit();
    try std.testing.expect(!std.mem.eql(u8, &first.fingerprint, &different_display.fingerprint));
    try std.testing.expect(try valuesEqual(try first.evaluate(.failing, &.{}), try different_display.evaluate(.failing, &.{})));
    var sql_null = try Compile.run(a, table, "{\"op\":\"literal\",\"type\":\"json\",\"value\":null}");
    defer sql_null.deinit();
    var json_null = try Compile.run(a, table, "{\"op\":\"literal\",\"type\":\"json\",\"value\":null,\"json_null\":true}");
    defer json_null.deinit();
    var omitted = try Compile.run(a, table, "{\"op\":\"literal\",\"type\":\"json\",\"json_null\":true}");
    defer omitted.deinit();
    try std.testing.expectEqualSlices(u8, &json_null.fingerprint, &omitted.fingerprint);
    const omitted_value = try omitted.evaluate(.failing, &.{});
    try std.testing.expect(omitted_value == .json and omitted_value.json.value == .null);
    try std.testing.expectEqual(Value.null, try sql_null.evaluate(.failing, &.{}));
    const present = try json_null.evaluate(.failing, &.{});
    try std.testing.expect(present == .json and present.json.value == .null);
    try std.testing.expect(!std.mem.eql(u8, &sql_null.fingerprint, &json_null.fingerprint));
    var old = table;
    old.requires_json_expressions = false;
    try std.testing.expectError(error.InvalidRelationalExpressionType, Compile.run(a, old, input));
    try std.testing.expectError(error.InvalidRelationalExpressionType, Compile.run(a, old, "{\"op\":\"literal\",\"type\":\"json\",\"value\":null,\"json_null\":true}"));
    for ([_][]const u8{
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":{},\"json_null\":true}",
    }) |invalid| try std.testing.expectError(error.InvalidRelationalExpression, Compile.run(a, table, invalid));
    for ([_][]const u8{
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":null,\"json_null\":1}",
        "{\"op\":\"literal\",\"type\":\"integer\",\"value\":1,\"json_null\":false}",
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":1e131072}",
    }) |invalid| try std.testing.expectError(error.InvalidRelationalExpressionType, Compile.run(a, table, invalid));
    for ([_][]const u8{
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":\"\\u0000\"}",
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":{\"\\u0000\":1}}",
        "{\"op\":\"literal\",\"type\":\"json\",\"value\":[{\"k\":\"\\u0000\"}]}",
    }) |invalid| try std.testing.expectError(error.SqlUntranslatableCharacter, Compile.run(a, table, invalid));
}

test "relational index system legacy expression results survive prepared input and ingress retirement" {
    const a = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .requires_public_schema = true, .requires_jsonb_display = true, .relational_columns = &.{
        .{ .name = "j", .path = "j", .column_type = .json, .is_json = true, .json_kind = .any },
    } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const encoded = try codec.serializePreparedOrdinalDeferredHash(a, 1, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "j", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "{\"a\":[\"owned\"],\"n\":1.20}" } },
    }, &layout);
    defer a.free(encoded);
    try codec.finalizeOrdinalMetadata(encoded, @splat(0), 0);
    const row = try codec.ordinalRowView(encoded, table, &layout);
    const Run = struct {
        fn run(backing: Allocator, cold: codec.OrdinalRowView) !void {
            var result_region = std.heap.ArenaAllocator.init(backing);
            defer result_region.deinit();
            const owned = result_region.allocator();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(owned, &bytes);
            // Exercise the internal ownership boundary without admitting the
            // still-guarded public JSON column compiler grammar.
            var plan: Plan = .{ .arena = .init(backing), .nodes = &.{.{ .op = .column, .kind = .json, .column_name = "j" }}, .dependencies = &.{0}, .fingerprint = @splat(0), .result_kind = .json, .literal_bytes = 0 };
            defer plan.deinit();
            const named = try plan.evaluateRowWithExecution(&execution, cold);
            const bound = try plan.evaluateBoundRowWithExecution(&execution, cold);
            const ingress = blk: {
                var source = std.heap.ArenaAllocator.init(backing);
                defer source.deinit();
                const document = try std.json.parseFromSliceLeaky(std.json.Value, source.allocator(), "{\"j\":{\"a\":[\"owned\"],\"n\":1.20}}", .{ .parse_numbers = false });
                const result = try plan.evaluateJsonWithExecution(&execution, document);
                try std.testing.expect(result.json.value.object.get("a").?.array.items[0].string.ptr != document.object.get("j").?.object.get("a").?.array.items[0].string.ptr);
                break :blk result;
            };
            for ([_]Value{ named, bound, ingress }) |result| {
                try std.testing.expectEqualStrings("1.20", result.json.value.object.get("n").?.number_string);
                var array = &@constCast(result.json).value.object.getPtr("a").?.array;
                try std.testing.expectEqualStrings("owned", array.items[0].string);
                try std.testing.expectEqual(owned.ptr, array.allocator.ptr);
                try std.testing.expectEqual(owned.vtable, array.allocator.vtable);
                // Force allocation after the temporary owner and ingress DOM
                // retire; no embedded allocator may point at a dead owner.
                try array.ensureTotalCapacity(array.capacity + 16);
            }
            const retained_scalar = blk: {
                var source: @import("../sql/row_value.zig").JsonRow = undefined;
                source.init(backing, max_allocated_bytes, &execution.numeric);
                defer source.deinit();
                const root = try source.decodePinned(0, "\"decoded scalar\"");
                const borrowed: Value = .{ .string = root.value.string };
                const result = try retainPreparedResult(&execution, borrowed, true);
                try std.testing.expect(result.string.ptr != borrowed.string.ptr);
                break :blk result;
            };
            try std.testing.expectEqualStrings("decoded scalar", retained_scalar.string);
            const json_null: @import("../sql/row_value.zig").Json = .{ .value = .null };
            const retained_null = try retainPreparedResult(&execution, .{ .json = &json_null }, false);
            try std.testing.expect(retained_null == .json and retained_null.json.value == .null);
            try std.testing.expectEqual(Value.null, try retainPreparedResult(&execution, .null, false));
            var denied = std.testing.FailingAllocator.init(backing, .{ .fail_index = 0 });
            var allowance: usize = max_allocated_bytes;
            var scalar_execution = Execution.init(denied.allocator(), &allowance);
            const scalar = try retainPreparedResult(&scalar_execution, .{ .string = "borrowed scalar" }, false);
            try std.testing.expectEqualStrings("borrowed scalar", scalar.string);
            try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
            var tiny: usize = 0;
            var limited = Execution.init(owned, &tiny);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, retainPreparedResult(&limited, .{ .json = &json_null }, false));
            tiny = max_allocated_bytes;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateRowWithExecution(&limited, cold));
        }
    };
    try Run.run(a, row);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{row});
}

test "relational index system typed JSON values compare exactly and materialize owned bounded output" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const alloc = region.allocator();
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(alloc, &bytes);
    const output = blk: {
        var source: @import("../sql/row_value.zig").JsonRow = undefined;
        source.init(a, max_allocated_bytes, &execution.numeric);
        defer source.deinit();
        const left: Value = .{ .json = try source.decodePinned(0, "{\"n\":9007199254740993.2500,\"a\":[null,\"escaped\\ntext\"]}") };
        const right: Value = .{ .json = try source.decodePinned(1, "{\"a\":[null,\"escaped\\ntext\"],\"n\":9007199254740993.25}") };
        try std.testing.expectEqual(std.math.Order.eq, try valueOrderWithContext(left, right, false, &execution.numeric));
        try std.testing.expect(try valuesEqual(left, right));
        try std.testing.expect(!try valuesEqual(.null, .{ .json = &.{ .value = .null } }));
        break :blk try boundedValueToJson(&execution, left);
    };
    try std.testing.expectEqualStrings("9007199254740993.2500", output.object.get("n").?.number_string);
    const array = output.object.get("a").?.array;
    try std.testing.expectEqual(alloc.ptr, array.allocator.ptr);
    try std.testing.expectEqual(alloc.vtable, array.allocator.vtable);
    try std.testing.expectEqualStrings("escaped\ntext", array.items[1].string);
    try std.testing.expect(bytes < max_allocated_bytes);
    var limited_bytes: usize = 0;
    var limited = Execution.init(alloc, &limited_bytes);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, boundedValueToJson(&limited, .{ .json = &.{ .value = output } }));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, limited.charge(0));
    var cpu_bytes: usize = max_allocated_bytes;
    var cpu = Execution.init(alloc, &cpu_bytes);
    cpu.numeric.remaining = 1; // Clone succeeds; allocator rehoming exhausts work.
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, boundedValueToJson(&cpu, .{ .json = &.{ .value = .null } }));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, cpu.charge(0));
    // A tagged VM operand must not accidentally activate public JSON programs
    // before schema capability and physical SQL NULL provenance are wired.
    const definition = try std.json.parseFromSlice(std.json.Value, a, "{\"op\":\"column\",\"column\":\"doc\"}", .{});
    defer definition.deinit();
    try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "doc", .path = "doc", .column_type = .json }} }, definition.value, .json));
}

fn entries(value: std.json.Value) ![]const std.json.Value {
    if (value == .null) return &.{};
    if (value != .array or value.array.items.len > 256) return error.InvalidRelationalExpression;
    return value.array.items;
}

/// Identity for online schema admission. Adding, removing, or changing STORED
/// generated semantics requires a row rewrite; declaration reordering does not.
pub fn generatedFingerprint(set: ?*const Set) [32]u8 {
    const Entry = struct { name: []const u8, fingerprint: [32]u8, modifier: ?exact.TypeModifier };
    var entries_buffer: [256]Entry = undefined;
    var count: usize = 0;
    if (set) |expressions| for (expressions.bindings) |binding| {
        if (!binding.generated) continue;
        const column = expressions.table.relational_columns[binding.ordinal];
        entries_buffer[count] = .{ .name = column.name, .fingerprint = binding.plan.fingerprint, .modifier = column.numeric_modifier };
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
        if (entry.modifier) |modifier| {
            state.update("SQL NUMERIC generated target modifier v1\x00");
            var identity: [4]u8 = undefined;
            std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
            std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
            state.update(&identity);
        }
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

test "relational declarations column CHECKs compare large logical values without persistent key amplification" {
    const a = std.testing.allocator;
    const Case = struct { blob: enum { same, other, null }, text: enum { wide, empty, null }, accepted: bool };
    const reference = try std.json.parseFromSlice(struct { blob_bytes: usize, text_bytes: usize, entries: []const Case }, a, @embedFile("../sql/fixtures/sql_check_domain_reference.json"), .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const payload = try owned.alloc(u8, reference.value.blob_bytes);
    @memset(payload, 0);
    const encoded_blob = try owned.alloc(u8, std.base64.standard.Encoder.calcSize(payload.len));
    _ = std.base64.standard.Encoder.encode(encoded_blob, payload);
    const text = try owned.alloc(u8, reference.value.text_bytes);
    @memset(text, 'x');
    const other = try owned.dupe(u8, payload);
    other[0] = 1;
    const other_blob = try owned.alloc(u8, encoded_blob.len);
    _ = std.base64.standard.Encoder.encode(other_blob, other);
    var identity: ?[32]u8 = null;
    for ([_]bool{ false, true }) |explicit| {
        const declaration = if (explicit)
            try std.json.Stringify.valueAlloc(owned, .{ .name = "same", .expression = .{ .op = "eq", .args = .{ .{ .op = "column", .column = "b" }, .{ .op = "literal", .type = "blob", .value = encoded_blob } } } }, .{})
        else
            try std.json.Stringify.valueAlloc(owned, .{ .name = "same", .column = "b", .op = "eq", .value = encoded_blob }, .{});
        const definition = try std.fmt.allocPrint(owned, "{{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"checks\":[{s},{{\"name\":\"text\",\"column\":\"s\",\"op\":\"gt\",\"value\":\"\"}}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"b\":{{\"type\":\"blob\"}},\"s\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{declaration});
        const impl = @import("table_schema_impl.zig");
        var nullable = try std.json.parseFromSliceLeaky(std.json.Value, owned, definition, .{});
        const properties = &nullable.object.getPtr("document_schemas").?.object.getPtr("row").?.object.getPtr("schema").?.object.getPtr("properties").?.object;
        for ([_][]const u8{ "b", "s" }) |name| try properties.getPtr(name).?.object.put(owned, "nullable", .{ .bool = true });
        var parsed = try impl.parseSchema(a, try std.json.Stringify.valueAlloc(owned, nullable, .{}));
        defer parsed.deinit(a);
        var compiled = try impl.CompiledValidationPlan.init(a, parsed);
        defer compiled.deinit(a);
        const set = compiled.checks.?;
        if (identity) |previous| try std.testing.expectEqualSlices(u8, &previous, &set.fingerprint()) else identity = set.fingerprint();
        var json: std.json.Value = .{ .object = .empty };
        try json.object.put(owned, "b", .{ .string = encoded_blob });
        try json.object.put(owned, "s", .{ .string = text });
        try std.testing.expectEqual(@as(?usize, null), try set.firstViolationJson(a, json));
        var cells: [2]codec.Cell = undefined;
        const b = set.layout.ordinalForName(set.table.relational_columns, "b").?;
        const s = set.layout.ordinalForName(set.table.relational_columns, "s").?;
        cells[b] = .{ .ordinal = @intCast(b), .path = "b", .value_type = .bytes_val, .value = .{ .bytes_val = encoded_blob } };
        cells[s] = .{ .ordinal = @intCast(s), .path = "s", .value_type = .bytes_val, .value = .{ .bytes_val = text } };
        const bytes = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &cells, @splat(0));
        defer a.free(bytes);
        const row = try codec.ordinalRowView(bytes, set.table, &set.layout);
        for (reference.value.entries) |entry| {
            var document: std.json.Value = .{ .object = .empty };
            try document.object.put(owned, "b", if (entry.blob == .null) .null else .{ .string = if (entry.blob == .other) other_blob else encoded_blob });
            try document.object.put(owned, "s", if (entry.text == .null) .null else .{ .string = if (entry.text == .wide) text else "" });
            const expected: ?usize = if (entry.accepted) null else if (entry.blob == .other) 0 else 1;
            try std.testing.expectEqual(expected, try set.firstViolationJson(a, document));
            var projected = cells;
            projected[b].is_null = entry.blob == .null;
            projected[b].value = .{ .bytes_val = if (entry.blob == .other) other_blob else encoded_blob };
            projected[s].is_null = entry.text == .null;
            projected[s].value = .{ .bytes_val = if (entry.text == .wide) text else "" };
            const physical = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &projected, @splat(0));
            defer a.free(physical);
            const cold = try codec.ordinalRowView(physical, set.table, &set.layout);
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            try std.testing.expectEqual(expected, try set.firstViolationRow(failing.allocator(), cold));
            try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
        }
        var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var allowance: usize = max_allocated_bytes;
        var execution = Execution.init(denied.allocator(), &allowance);
        try std.testing.expectEqual(@as(?usize, null), try set.firstViolationRowWithExecution(&execution, row));
        try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
        try std.testing.expect(allowance < max_allocated_bytes);
        var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(a, set.table, &set.layout, &.{.{ .column = "b" }});
        defer tuple.deinit();
        var key = std.ArrayList(u8).empty;
        defer key.deinit(a);
        try std.testing.expectError(error.RelationalIndexKeyTooLarge, tuple.appendValues(a, &key, &.{.{ .blob = payload }}));
        const Cancel = struct {
            calls: usize = 0,
            fn poll(raw: ?*anyopaque) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.calls += 1;
                if (self.calls == 3) return error.Canceled;
            }
        };
        var cancel: Cancel = .{};
        allowance = max_allocated_bytes;
        execution = Execution.init(denied.allocator(), &allowance);
        execution.numeric.checkpoint = Cancel.poll;
        execution.numeric.ptr = &cancel;
        try std.testing.expectError(error.Canceled, set.firstFailureRowWithExecution(&execution, row));
        try std.testing.expectEqual(@as(usize, 3), cancel.calls);
        execution.numeric.checkpoint = null;
        try std.testing.expectError(error.Canceled, execution.charge(0));
        allowance = max_allocated_bytes;
        execution = Execution.init(denied.allocator(), &allowance);
        execution.numeric.remaining = 256;
        try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.firstFailureRowWithExecution(&execution, row));
        execution.numeric.remaining = 8 * 1024 * 1024;
        try std.testing.expectError(error.RelationalExpressionBudgetExceeded, execution.charge(0));
    }
}

test "relational declarations mixed CHECKs share JSON and ordinal row quotas with sticky activation failures" {
    const Run = struct {
        fn run(alloc: Allocator) !void {
            const impl = @import("table_schema_impl.zig");
            var table = try impl.parseSchema(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"n","op":"gte","value":0},{"name":"label","column":"s","op":"eq","value":"keep"},{"name":"small","expression":{"op":"lt","args":[{"op":"column","column":"n"},{"op":"literal","type":"numeric","value":"10"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":["number","null"],"x-antfly-sql-type":"numeric"},"s":{"type":"string"}},"additionalProperties":false}}}}
            );
            defer table.deinit(alloc);
            var compiled = try impl.CompiledValidationPlan.init(alloc, table);
            defer compiled.deinit(alloc);
            const set = compiled.checks.?;
            const json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"n\":1.25,\"s\":\"keep\"}", .{ .parse_numbers = false });
            defer json.deinit();
            const number = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, json.value.object.get("n").?);
            defer alloc.free(number);
            var cells: [2]codec.Cell = undefined;
            const n = set.layout.ordinalForName(set.table.relational_columns, "n").?;
            const s = set.layout.ordinalForName(set.table.relational_columns, "s").?;
            cells[n] = .{ .ordinal = @intCast(n), .path = "n", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = number } };
            cells[s] = .{ .ordinal = @intCast(s), .path = "s", .value_type = .bytes_val, .value = .{ .bytes_val = "keep" } };
            const bytes = try codec.serializeOrdinal(alloc, set.table.version, set.table.relational_columns, &cells, @splat(0));
            defer alloc.free(bytes);
            const row = try codec.ordinalRowView(bytes, set.table, &set.layout);
            for ([_]bool{ false, true }) |cold| {
                var budget: usize = max_allocated_bytes;
                var execution = Execution.init(alloc, &budget);
                const result = if (cold) try set.firstViolationRowWithExecution(&execution, row) else try set.firstViolationJsonWithExecution(&execution, json.value);
                try std.testing.expectEqual(@as(?usize, null), result);
                const used = 8 * 1024 * 1024 - execution.numeric.remaining;
                try std.testing.expect(used > 0 and budget < max_allocated_bytes);
                try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
                try std.testing.expectEqual(alloc.vtable, execution.alloc.vtable);
                execution = Execution.init(alloc, &budget);
                execution.numeric.remaining = used - 1;
                const limited = if (cold) set.firstViolationRowWithExecution(&execution, row) else set.firstViolationJsonWithExecution(&execution, json.value);
                if (limited) |_| return error.TestExpectedError else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
                }
                execution.numeric.remaining = 8 * 1024 * 1024;
                const activation = if (cold) set.firstFailureRowWithExecution(&execution, row) else set.firstFailureJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.RelationalExpressionBudgetExceeded, activation);
                budget = 1;
                execution = Execution.init(alloc, &budget);
                const tiny = if (cold) set.firstViolationRowWithExecution(&execution, row) else set.firstViolationJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.RelationalExpressionBudgetExceeded, tiny);
                const Cancel = struct {
                    fn poll(_: ?*anyopaque) anyerror!void {
                        return error.Canceled;
                    }
                };
                budget = max_allocated_bytes;
                execution = Execution.init(alloc, &budget);
                execution.numeric.checkpoint = Cancel.poll;
                const canceled = if (cold) set.firstFailureRowWithExecution(&execution, row) else set.firstFailureJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.Canceled, canceled);
                execution.numeric.checkpoint = null;
                try std.testing.expectError(error.Canceled, execution.charge(0));
            }
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "relational declarations borrowed CHECK byte admission remains sticky without allocations" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var table = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"equal","expression":{"op":"eq","args":[{"op":"literal","type":"string","value":"0123456789"},{"op":"literal","type":"string","value":"0123456789"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"s":{"type":"string"}},"additionalProperties":false}}}}
    );
    defer table.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, table);
    defer compiled.deinit(alloc);
    const input: std.json.Value = .{ .object = .empty };
    // Both operands are immutable plan literals. There is no runtime arena
    // allocation whose failure could accidentally establish stickiness.
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var budget: usize = 20;
    var execution = Execution.init(failing.allocator(), &budget);
    try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationJsonWithExecution(&execution, input));
    try std.testing.expectEqual(@as(usize, 0), budget);
    budget = 19;
    execution = Execution.init(failing.allocator(), &budget);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, compiled.checks.?.firstFailureJsonWithExecution(&execution, input));
    budget = max_allocated_bytes;
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, compiled.checks.?.firstViolationJsonWithExecution(&execution, input));
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
}

test "relational declarations CHECK expressions release every allocation failure" {
    // Arena remaps depend on surrounding addresses and can change allocation
    // counts between fault indexes. Exercise the allocating fallback with a
    // deterministic backing owner instead of intermittently skipping indexes.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), checkExpressionAllocationFailure, .{});
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
    const manager = @import("antfly_server_test_sources").local_test_sources.metadata_table_manager;
    const tables = @import("antfly_server_test_sources").local_test_sources.api_tables;
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

test "relational declarations public NUMERIC annotation validates scope and constrained arrays" {
    const a = std.testing.allocator;
    const public_schema = @import("mod.zig");
    for ([_][]const u8{
        "{\"type\":\"number\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"integer\",\"x-antfly-sql-type\":\"integer\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"string\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"object\",\"properties\":{\"nested\":{\"type\":\"number\",\"x-antfly-sql-type\":\"numeric\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}}}",
    }) |property| {
        const json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{s}}},\"additionalProperties\":false}}}}}}}}", .{property});
        defer a.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, public_schema.CompiledTableValidator.init(a, json));
    }
    for ([_][]const u8{
        "{\"precision\":0,\"scale\":2}",
        "{\"precision\":4,\"scale\":1001}",
        "{\"precision\":\"4\",\"scale\":2}",
        "{\"precision\":4}",
        "{\"precision\":4,\"scale\":2,\"unknown\":0}",
        "null",
    }) |modifier| {
        const json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{{\"type\":\"number\",\"x-antfly-sql-type\":\"numeric\",\"x-antfly-sql-numeric-modifier\":{s}}}}},\"additionalProperties\":false}}}}}}}}", .{modifier});
        defer a.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, public_schema.CompiledTableValidator.init(a, json));
    }
    var validator = try public_schema.CompiledTableValidator.init(a,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_array","x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    var document = try std.json.parseFromSlice(std.json.Value, a,
        \\{"a":{"dimensions":[{"length":3,"lower_bound":-7}],"values":["1.245","NaN",null],"sql_nulls":[false,false,true]}}
    , .{});
    defer document.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, validator.validateValue(a, &document.value));
    try validator.prepareValue(document.arena.allocator(), a, &document.value);
    try std.testing.expectEqualStrings("1.25", document.value.object.get("a").?.object.get("values").?.array.items[0].string);
    try std.testing.expectEqual(@as(i64, -7), document.value.object.get("a").?.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
    try validator.validateValue(a, &document.value);
    const runtime = try public_schema.deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, runtime);
    try std.testing.expect(runtime.requires_numeric_modifiers);
    try std.testing.expectEqual(@as(u16, 4), runtime.relational_columns[0].numeric_modifier.?.precision);
    try std.testing.expectEqual(@as(usize, 0), validator.restore.properties.len);
}

test "relational declarations public NUMERIC modifiers preserve PostgreSQL assignment ordering and epochs" {
    const a = std.testing.allocator;
    const Entry = struct { use_default: bool = false, input: ?[]const u8 = null, expected: ?[]const ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_numeric_assignment_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn run(alloc: Allocator, cases: []const Entry) !void {
            const public_schema = @import("mod.zig");
            var validator = try public_schema.CompiledTableValidator.init(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"1.245"}}],"generated_columns":[{"column":"narrow","expression":{"op":"column","column":"base"}},{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"column","column":"narrow"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"base":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}},"narrow":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":3,"scale":1}},"total":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":5,"scale":2}}},"additionalProperties":false}}}}
            );
            defer validator.deinit(alloc);
            const runtime = try public_schema.deriveRuntimeTableSchema(alloc, validator.schema);
            defer schema.freeSchema(alloc, runtime);
            try std.testing.expect(runtime.requires_numeric_modifiers);
            const old_catalog: @import("../storage/db/table_catalog.zig").Catalog = .{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = runtime.version, .schema_format_version = 22 };
            try std.testing.expectError(error.UnsupportedTableCapabilityVersion, old_catalog.validateForSchema(runtime));
            for (cases) |entry| {
                var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
                defer document.deinit();
                if (!entry.use_default) try document.value.object.put(document.arena.allocator(), "base", if (entry.input) |text| .{ .string = text } else .null);
                const result = validator.prepareValue(document.arena.allocator(), alloc, &document.value);
                if (entry.@"error" != null) {
                    if (result) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expect(err == error.InvalidBatchRequest or err == error.RelationalExpressionOverflow);
                    }
                    continue;
                }
                try result;
                for ([_][]const u8{ "base", "narrow", "total" }, entry.expected.?) |name, expected| {
                    const value = document.value.object.get(name).?;
                    if (expected) |text| try std.testing.expectEqualStrings(text, if (value == .number_string) value.number_string else value.string) else try std.testing.expectEqual(std.json.Value.null, value);
                }
                try validator.validateValue(alloc, &document.value);
            }
        }
    };
    try Run.run(a, fixture.value.entries);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{fixture.value.entries});
}

test "relational declarations physical restore discharges unconstrained SQL domains without materialization" {
    const a = std.testing.allocator;
    const public_schema = @import("mod.zig");
    var validator = try public_schema.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"t":{"type":"string","x-antfly-sql-type":"text"},"i":{"type":"integer","x-antfly-sql-type":"int16"},"f":{"type":"number","x-antfly-sql-type":"float32"},"n":{"type":"number","x-antfly-sql-type":"numeric"},"a":{"type":"sql_array","x-antfly-sql-type":"numeric"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    try std.testing.expect(!validator.restore.full_root);
    try std.testing.expectEqual(@as(usize, 0), validator.restore.properties.len);
    const table = try public_schema.deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, table);
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var context: exact.Context = .{ .alloc = a };
    var parsed = try exact.parse(&context, "1.25");
    defer parsed.deinit();
    const arrays = @import("../sql/array_value.zig");
    const elements = [_]arrays.Element{ arrays.Element.typedNumeric(&parsed.value), .{} };
    const array = try arrays.Value.init(.numeric, &.{.{ .length = 2, .lower = -3 }}, &elements, .{});
    const payload = try @import("../sql/array_storage.zig").encodeAlloc(a, array, .{});
    defer a.free(payload);
    const ordinal = layout.ordinalForName(table.relational_columns, "a").?;
    const cell: codec.Cell = .{ .ordinal = @intCast(ordinal), .path = "a", .value_type = .bytes_val, .sql_array_element_type = .numeric, .value = .{ .bytes_val = payload } };
    const bytes = try codec.serializeOrdinal(a, table.version, table.relational_columns, &.{cell}, @splat(0));
    defer a.free(bytes);
    try codec.validateOrdinalWithLayout(bytes, table, &layout);
    const row = try codec.ordinalRowView(bytes, table, &layout);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var budget: usize = 0;
    var execution = Execution.init(failing.allocator(), &budget);
    try validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    var constrained = try public_schema.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","minimum":1},"t":{"type":"string","x-antfly-sql-type":"text","maxLength":10}},"additionalProperties":false}}}}
    );
    defer constrained.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), constrained.restore.properties.len);
}

test "relational declarations physical restore shares generated CHECK and field constraint admission" {
    const Run = struct {
        fn run(alloc: Allocator) !void {
            var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"add","args":[{"op":"column","column":"n"},{"op":"literal","type":"numeric","value":"1"}]}}],"checks":[{"name":"positive","column":"n","op":"gte","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","minimum":1,"multipleOf":0.25},"g":{"type":"number","x-antfly-sql-type":"numeric"},"payload":{"type":"blob"}},"additionalProperties":false}}}}
            );
            defer validator.deinit(alloc);
            try std.testing.expect(!validator.restore.full_root);
            const table = validator.execution.expressions.?.table;
            var layout = try codec.PhysicalLayout.init(alloc, table);
            defer layout.deinit();
            const n = layout.ordinalForName(table.relational_columns, "n").?;
            const g = layout.ordinalForName(table.relational_columns, "g").?;
            const number = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .string = "1.25" });
            defer alloc.free(number);
            const generated = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .string = "2.25" });
            defer alloc.free(generated);
            var cells: [2]codec.Cell = .{
                .{ .ordinal = @intCast(n), .path = "n", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = number } },
                .{ .ordinal = @intCast(g), .path = "g", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = generated } },
            };
            if (n > g) std.mem.swap(codec.Cell, &cells[0], &cells[1]);
            const bytes = try codec.serializeOrdinal(alloc, table.version, table.relational_columns, &cells, @splat(0));
            defer alloc.free(bytes);
            const row = try codec.ordinalRowView(bytes, table, &layout);
            var budget: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &budget);
            try validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
            const used = 8 * 1024 * 1024 - execution.numeric.remaining;
            try std.testing.expect(used > 1 and budget < max_allocated_bytes);
            try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
            try std.testing.expectEqual(alloc.vtable, execution.alloc.vtable);
            budget = max_allocated_bytes;
            execution = Execution.init(alloc, &budget);
            execution.numeric.remaining = used - 1;
            const limited = validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
            if (limited) |_| return error.TestExpectedError else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
            }
            execution.numeric.remaining = 8 * 1024 * 1024;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            budget = 1;
            execution = Execution.init(alloc, &budget);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            budget = max_allocated_bytes;
            execution = Execution.init(alloc, &budget);
            execution.numeric.checkpoint = Cancel.poll;
            try std.testing.expectError(error.Canceled, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            execution.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            // Still reject physically canonical but logically forged output.
            const generated_index: usize = if (n > g) 0 else 1;
            cells[generated_index].value.bytes_val = number;
            const forged = try codec.serializeOrdinal(alloc, table.version, table.relational_columns, &cells, @splat(0));
            defer alloc.free(forged);
            const rejected = validator.validateRelationalRestoreFields(alloc, try codec.ordinalRowView(forged, table, &layout));
            if (rejected) |_| return error.TestExpectedError else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
            }
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "relational declarations nested blob programs agree across JSON and cold generated verification" {
    const a = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var parsed = try impl.parseSchema(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"coalesce","args":[{"op":"column","column":"b"},{"op":"literal","type":"blob","value":"eA=="}]}}],"checks":[{"name":"same","expression":{"op":"eq","args":[{"op":"coalesce","args":[{"op":"column","column":"b"},{"op":"literal","type":"blob","value":"eA=="}]},{"op":"column","column":"g"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"b":{"type":"blob","nullable":true},"g":{"type":"blob"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(a);
    var compiled = try impl.CompiledValidationPlan.init(a, parsed);
    defer compiled.deinit(a);
    const set = compiled.expressions.?;
    var layout = try codec.PhysicalLayout.init(a, set.table);
    defer layout.deinit();
    for ([_][]const u8{ "{\"b\":\"AAEC/w==\"}", "{\"b\":null}", "{}" }) |input| {
        var document = try std.json.parseFromSlice(std.json.Value, a, input, .{});
        defer document.deinit();
        try set.applyJson(document.arena.allocator(), &document.value);
        try set.verifyJson(a, document.value);
        try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationJson(a, document.value));
        var cells: [2]codec.Cell = undefined;
        for (set.table.relational_columns, &cells, 0..) |column, *cell, ordinal| {
            const value = document.value.object.get(column.name) orelse .null;
            cell.* = .{ .ordinal = @intCast(ordinal), .path = column.name, .value_type = .bytes_val, .is_null = value == .null, .value = .{ .bytes_val = if (value == .null) "" else value.string } };
        }
        const physical = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &cells, @splat(0));
        defer a.free(physical);
        const row = try codec.ordinalRowView(physical, set.table, &layout);
        try set.verifyRow(a, row);
        try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationRow(a, row));
        const g = layout.ordinalForName(set.table.relational_columns, "g").?;
        cells[g].value.bytes_val = "eQ==";
        const forged = try codec.serializeOrdinal(a, set.table.version, set.table.relational_columns, &cells, @splat(0));
        defer a.free(forged);
        const forged_row = try codec.ordinalRowView(forged, set.table, &layout);
        try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRow(a, forged_row));
        try std.testing.expectEqual(@as(?usize, 0), try compiled.checks.?.firstViolationRow(a, forged_row));
    }
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
