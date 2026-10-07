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
const Capture = @import("result_cursor.zig").Cursor;

/// Production candidates stay in their bounded typed capture. The JSON arm is
/// an adapter for scalar preparation fixtures, not an execution representation.
const RowSource = union(enum) {
    capture: *Capture,
    json: struct { rows: []const []const std.json.Value, nulls: []const []const bool },

    fn count(self: RowSource) usize {
        return switch (self) {
            .capture => |capture| capture.count(),
            .json => |json| json.rows.len,
        };
    }

    fn open(self: RowSource, width: usize) !Reader {
        switch (self) {
            .capture => |capture| {
                if (capture.width != width) return error.InvalidSqlBackendResponse;
                return .{ .source = self, .width = width, .typed = try capture.openReplayReader() };
            },
            .json => |json| {
                if (json.rows.len != json.nulls.len) return error.InvalidSqlBackendResponse;
                return .{ .source = self, .width = width };
            },
        }
    }

    const Reader = struct {
        source: RowSource,
        width: usize,
        index: usize = 0,
        typed: ?*Capture.ReplayReader = null,

        /// Provider pages retain rows across block loads; pure consumers borrow
        /// one row and own only their outputs. Neither path reconstructs JSON.
        fn next(self: *Reader, a: Allocator, own: bool) !?[]const scalar.Datum {
            if (self.index == self.source.count()) return null;
            const cells = if (self.typed) |reader|
                (try reader.next()) orelse return error.InvalidSqlBackendResponse
            else blk: {
                const json = self.source.json;
                const row = json.rows[self.index];
                const flags = json.nulls[self.index];
                if (row.len != self.width or flags.len != self.width) return error.InvalidSqlBackendResponse;
                const values = try a.alloc(scalar.Datum, row.len);
                for (row, flags, values) |value, is_null, *out| out.* = .{ .value = value, .sql_null = is_null };
                break :blk values;
            };
            self.index += 1;
            if (cells.len != self.width) return error.InvalidSqlBackendResponse;
            if (!own) return cells;
            const copied = try a.alloc(scalar.Datum, cells.len);
            for (cells, copied) |cell, *out| out.* = try @import("operators.zig").cloneDatum(a, cell);
            return copied;
        }

        fn close(self: *Reader) void {
            if (self.typed) |reader| reader.close();
        }
    };
};

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
        return self.classifyRowsChecked(alloc, alloc, .{ .json = .{ .rows = rows, .nulls = nulls } }, parameters, null, .{ .row_limit = (@import("runtime.zig").Limits{}).page_rows, .byte_limit = (@import("runtime.zig").Limits{}).page_bytes });
    }

    fn classifyRowsChecked(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, source: RowSource, parameters: []const std.json.Value, backend: ?catalog.Backend, page_limits: decision_eval.PageBudget) ![]const ?usize {
        for (self.arms) |arm| if (arm.predicate) |*program| {
            if (decision_eval.hasExternal(program)) return self.classifyDecisionRows(alloc, scratch_allocator, source, parameters, backend, page_limits);
        };
        const selected = try alloc.alloc(?usize, source.count());
        var reader = try source.open(self.query.columns.len);
        defer reader.close();
        var affected: std.StringHashMapUnmanaged(void) = .empty;
        // Arm predicates return an ordinal, not a value borrowed from their
        // evaluator. Reuse one bounded scratch arena across candidate rows
        // instead of allocating and destroying an arena for every row.
        var scratch = std.heap.ArenaAllocator.init(scratch_allocator);
        defer scratch.deinit();
        for (selected) |*slot| {
            if (backend) |active| try active.vtable.checkpoint(active.ptr);
            _ = scratch.reset(.retain_capacity);
            const cells = (try reader.next(scratch.allocator(), false)) orelse return error.InvalidSqlBackendResponse;
            slot.* = try self.selectArmWithProvider(scratch.allocator(), cells, parameters, if (backend) |active| active.decision_provider else null);
            if (slot.*) |index| switch (self.arms[index].action) {
                .update, .delete => {
                    if (cells[0].sql_null or cells[0].value != .string) return error.InvalidSqlBackendResponse;
                    const entry = try affected.getOrPut(alloc, cells[0].value.string);
                    if (entry.found_existing) return error.SqlMutationCardinalityViolation;
                    entry.key_ptr.* = try alloc.dupe(u8, cells[0].value.string);
                },
                .insert, .nothing => {},
            };
        }
        return selected;
    }

    /// Arm order is SQL control flow: only unmatched rows of the appropriate
    /// matched/source-only kind are eligible for the next predicate wave.
    fn classifyDecisionRows(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, source: RowSource, parameters: []const std.json.Value, backend: ?catalog.Backend, page_limits: decision_eval.PageBudget) ![]const ?usize {
        const selected = try alloc.alloc(?usize, source.count());
        var reader = try source.open(self.query.columns.len);
        defer reader.close();
        @memset(selected, null);
        var affected: std.StringHashMapUnmanaged(void) = .empty;
        var arena = std.heap.ArenaAllocator.init(scratch_allocator);
        defer arena.deinit();
        var first: usize = 0;
        while (first < source.count()) {
            if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
            const scratch = arena.allocator();
            var budget = page_limits;
            var page_cells: std.ArrayList([]const scalar.Datum) = .empty;
            while (try reader.next(scratch, true)) |input| {
                if (backend) |active| try active.vtable.checkpoint(active.ptr);
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
                    const entry = try affected.getOrPut(alloc, row[0].value.string);
                    if (entry.found_existing) return error.SqlMutationCardinalityViolation;
                    entry.key_ptr.* = try alloc.dupe(u8, row[0].value.string);
                },
                .insert, .nothing => {},
            };
            first = end;
        }
        return selected;
    }

    /// Resolve assignments only for each selected arm and retain their values
    /// in the mutation arena. Pure mutation plans keep their existing hot path.
    fn decisionAssignmentValues(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, source: RowSource, selected: []const ?usize, parameters: []const std.json.Value, page_limits: decision_eval.PageBudget) !?[]const ?[]const scalar.Datum {
        var needed = false;
        for (self.arms) |arm| switch (arm.action) {
            .insert, .update => |assignments| for (assignments) |assignment| {
                if (assignment.program) |*program| needed = needed or decision_eval.hasExternal(program);
            },
            .delete, .nothing => {},
        };
        if (!needed) return null;
        const values = try alloc.alloc(?[]const scalar.Datum, source.count());
        @memset(values, null);
        var arena = std.heap.ArenaAllocator.init(scratch_allocator);
        defer arena.deinit();
        for (self.arms, 0..) |arm, arm_index| {
            const assignments = switch (arm.action) {
                .insert, .update => |items| items,
                .delete, .nothing => continue,
            };
            var reader = try source.open(self.query.columns.len);
            defer reader.close();
            var first: usize = 0;
            while (first < source.count()) {
                if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
                const scratch = arena.allocator();
                var cells: std.ArrayList([]const scalar.Datum) = .empty;
                var positions: std.ArrayList(usize) = .empty;
                var budget = page_limits;
                while (first < source.count()) : (first += 1) {
                    const input = (try reader.next(scratch, false)) orelse return error.InvalidSqlBackendResponse;
                    if (selected[first] == null or selected[first].? != arm_index) continue;
                    try backend.vtable.checkpoint(backend.ptr);
                    const owned = try scratch.alloc(scalar.Datum, input.len);
                    for (input, owned) |cell, *out| out.* = try @import("operators.zig").cloneDatum(scratch, cell);
                    try cells.append(scratch, owned);
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
        return self.prepareWithPageLimits(alloc, scratch_allocator, backend, .{ .json = .{ .rows = rows, .nulls = nulls } }, parameters, max_rows, max_bytes, .{ .row_limit = (@import("runtime.zig").Limits{}).page_rows, .byte_limit = (@import("runtime.zig").Limits{}).page_bytes });
    }

    pub fn prepareCaptured(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, capture: *Capture, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize, page_limits: decision_eval.PageBudget) !Prepared {
        return self.prepareWithPageLimits(alloc, scratch_allocator, backend, .{ .capture = capture }, parameters, max_rows, max_bytes, page_limits);
    }

    fn prepareWithPageLimits(self: Candidates, alloc: Allocator, scratch_allocator: Allocator, backend: catalog.Backend, source: RowSource, parameters: []const std.json.Value, max_rows: usize, max_bytes: usize, page_limits: decision_eval.PageBudget) !Prepared {
        if (source.count() > max_rows) return error.SqlResultTooLarge;
        const selections = try self.classifyRowsChecked(alloc, scratch_allocator, source, parameters, backend, page_limits);
        const assignment_values = try self.decisionAssignmentValues(alloc, scratch_allocator, backend, source, selections, parameters, page_limits);
        var reader = try source.open(self.query.columns.len);
        defer reader.close();
        var scratch = std.heap.ArenaAllocator.init(scratch_allocator);
        defer scratch.deinit();
        var mutations: std.ArrayList(catalog.Mutation) = .empty;
        var source_rows: std.ArrayList(usize) = .empty;
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        var retained: usize = 0;
        var old_layout: ?catalog.Row.TypedLayout = null;
        if (self.returning) {
            var names: std.ArrayList([]const u8) = .empty;
            for (self.target.columns, self.field_ordinals) |field, ordinal| {
                if (ordinal != null) try names.append(alloc, field.path);
            }
            old_layout = try catalog.Row.TypedLayout.init(alloc, names.items);
        }
        for (selections, 0..) |selected, source_index| {
            _ = scratch.reset(.retain_capacity);
            const cells = (try reader.next(scratch.allocator(), false)) orelse return error.InvalidSqlBackendResponse;
            try backend.vtable.checkpoint(backend.ptr);
            const arm_index = selected orelse continue;
            if (self.arms[arm_index].action == .nothing) continue;
            const arm = self.arms[arm_index];
            const assignments: []const BoundAssignment = switch (arm.action) {
                .update => |items| items,
                .insert => |items| items,
                .delete, .nothing => &.{},
            };
            var object: std.json.ObjectMap = .empty;
            var json_null_fields: std.ArrayList([]const u8) = .empty;
            const inserting = arm.action == .insert;
            const deleting = arm.action == .delete;
            const present = if (!inserting) blk: {
                if (cells.len < 5 or cells[4].sql_null or cells[4].value != .string) return error.InvalidSqlBackendResponse;
                break :blk try joined_mutation.presenceDirectory(scratch.allocator(), cells[4].value.string);
            } else std.StringHashMapUnmanaged(void).empty;
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
                        try object.put(alloc, try alloc.dupe(u8, member.key_ptr.*), try @import("runtime.zig").clone(alloc, member.value_ptr.*));
                        if (declared) |field| if (field.type == .json and member.value_ptr.* == .null) try json_null_fields.append(alloc, field.path);
                    }
                } else if (!deleting) {
                    for (self.target.columns, self.field_ordinals) |field, ordinal| {
                        if (field.generated and !deleting) continue;
                        const index = ordinal orelse if (deleting) continue else return error.InvalidSqlBackendResponse;
                        if (index >= cells.len) return error.InvalidSqlBackendResponse;
                        const overwritten = for (assignments) |assignment| {
                            if (std.mem.eql(u8, assignment.column.path, field.path)) break true;
                        } else false;
                        if (overwritten) continue;
                        if (!present.contains(field.name)) continue;
                        try object.put(alloc, field.path, try @import("runtime.zig").encodeStorageDatum(alloc, cells[index], field, max_bytes));
                        if (!cells[index].sql_null and cells[index].value == .null and field.type == .json) try json_null_fields.append(alloc, field.path);
                    }
                }
            }
            var key: ?[]const u8 = if (inserting) null else try alloc.dupe(u8, cells[0].value.string);
            const values = if (assignment_values) |computed| computed[source_index] orelse &.{} else try self.evaluateValuesWithProvider(scratch.allocator(), arm_index, cells, parameters, backend.decision_provider);
            for (assignments, values) |assignment, datum| {
                if (assignment.program == null) continue; // DEFAULT: native preparation fills the absent cell.
                const field = assignment.column;
                if (datum.sql_null and !field.nullable) return error.SqlNotNullViolation;
                const typed = try @import("runtime.zig").encodeStorageDatum(alloc, datum, field, max_bytes);
                if (std.mem.eql(u8, field.name, "_id")) {
                    if (!inserting or datum.sql_null or typed != .string or typed.string.len == 0) return error.SqlRowIdentityRequired;
                    key = typed.string;
                } else {
                    try object.put(alloc, field.path, typed);
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
                var preimage_values: std.ArrayList(scalar.Datum) = .empty;
                var presence: std.ArrayList(bool) = .empty;
                for (self.target.columns, self.field_ordinals) |field, ordinal| {
                    const index = ordinal orelse continue;
                    if (index >= cells.len) return error.InvalidSqlBackendResponse;
                    try preimage_values.append(scratch.allocator(), cells[index]);
                    try presence.append(alloc, present.contains(field.name));
                    retained = std.math.add(usize, retained, try @import("operators.zig").datumBytes(cells[index])) catch return error.SqlProgramLimitExceeded;
                }
                if (retained > max_bytes) return error.SqlProgramLimitExceeded;
                const old = try alloc.create(catalog.Row);
                old.* = try catalog.Row.fromDatums(alloc, identity, old_layout.?, preimage_values.items);
                old.version = version;
                old.expected_content_digest = digest;
                old.typed_cells.?.presence = presence.items;
                if (!cells[3].sql_null) {
                    retained = std.math.add(usize, retained, jsonSize(cells[3].value)) catch return error.SqlProgramLimitExceeded;
                    if (retained > max_bytes) return error.SqlProgramLimitExceeded;
                    old.document = try @import("runtime.zig").clone(alloc, cells[3].value);
                }
                break :previous old;
            } else null;
            const mutation: catalog.Mutation = .{ .key = identity, .expected_version = version, .expected_content_digest = digest, .unique_absence = inserting, .row = if (deleting) null else .{ .object = object }, .json_null_fields = json_null_fields.items, .previous = previous };
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
        column.* = .{ .name = if (projection.field.len != 0) projection.field else try std.fmt.allocPrint(alloc, "\x00merge_null_{d}", .{index}), .type = output.type, .element_type = output.element_type };
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
                    const field = try target.column(assignment.field);
                    value.* = .{ .column = field, .value = if (expression) |node| try scalar.assignmentExpression(alloc, try relation_binding.lowerBoundExpression(alloc, relation.root.columns, node), .{ .kind = field.type, .element_type = field.element_type }) else null };
                }
                break :blk .{ .update = values };
            },
            .insert => |insert| blk: {
                const values = try alloc.alloc(Expression, insert.values.len);
                for (insert.columns, insert.values, values) |name, expression, *value| {
                    const field = try target.column(name);
                    value.* = .{ .column = field, .value = if (expression) |node| try scalar.assignmentExpression(alloc, try relation_binding.lowerBoundExpression(alloc, relation.root.columns, node), .{ .kind = field.type, .element_type = field.element_type }) else null };
                }
                break :blk .{ .insert = values };
            },
            .delete => .delete,
            .nothing => .nothing,
        };
    }
    const parameters = try alloc.dupe(?ast.ColumnType, input.parameter_types);
    // Constraints are shared by every arm, even though evaluation is lazy.
    // Iterate to a fixed point so an earlier predicate can use a parameter
    // whose precise array identity is supplied by a later assignment.
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (unbound) |arm| {
            if (arm.predicate) |predicate| changed = try scalar.inferParameters(alloc, predicate, columns, parameters, .boolean, .{ .invocation = backend.parameter_invocation }) or changed;
            const values: []const Expression = switch (arm.action) {
                .update => |items| items,
                .insert => |items| items,
                .delete, .nothing => &.{},
            };
            for (values) |value| if (value.value) |expression| {
                const expected: scalar.Type = .{ .kind = value.column.type, .element_type = value.column.element_type };
                changed = (if (backend.parameter_invocation) |owner|
                    try owner.infer(alloc, expression, columns, parameters, expected, .{ .assignment = true })
                else if (value.column.type == .array and parameters.len == 0)
                    try scalar.inferTypedParametersExpected(alloc, expression, columns, &.{}, expected, .{ .assignment = true })
                else
                    try scalar.inferParameters(alloc, expression, columns, parameters, value.column.type, .{ .assignment = true })) or changed;
            };
        }
        if (!changed) break;
    }
    const bound = try alloc.alloc(BoundArm, unbound.len);
    for (unbound, bound) |arm, *out| {
        out.matched = arm.matched;
        out.predicate = if (arm.predicate) |predicate| try scalar.bindExpectedWithSettings(alloc, predicate, columns, parameters, .boolean, .{ .invocation = backend.parameter_invocation }, backend.settings_view) else null;
        out.action = switch (arm.action) {
            .update, .insert => |values| blk: {
                const assignments = try alloc.alloc(BoundAssignment, values.len);
                for (values, assignments) |value, *assignment| assignment.* = .{
                    .column = value.column,
                    .program = if (value.value) |expression| bound_program: {
                        const expected: scalar.Type = .{ .kind = value.column.type, .element_type = value.column.element_type };
                        break :bound_program if (backend.parameter_invocation) |owner|
                            try scalar.bindTypedExpectedWithSettings(alloc, expression, columns, owner.descriptors, expected, .{ .invocation = owner, .assignment = true }, backend.settings_view)
                        else if (value.column.type == .array and parameters.len == 0)
                            try scalar.bindTypedExpectedWithSettings(alloc, expression, columns, &.{}, expected, .{ .assignment = true }, backend.settings_view)
                        else
                            try scalar.bindExpectedWithSettings(alloc, expression, columns, parameters, value.column.type, .{ .assignment = true }, backend.settings_view);
                    } else null,
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
        out.* = .{ .name = if (projection.field.len != 0) projection.field else try std.fmt.allocPrint(alloc, "\x00merge_null_{d}", .{index}), .type = column.type, .element_type = column.element_type };
    }
    const lowered = try alloc.alloc(*const ast.Scalar, count);
    for (expressions, projections, lowered) |expression, projection, *out| out.* = if (projection.bound_column != null) expression else try relation_binding.lowerBoundExpression(alloc, relation.root.columns, expression);
    for (lowered) |expression| _ = try scalar.inferParameters(alloc, expression, scalar_columns, parameters, null, .{ .invocation = backend.parameter_invocation });
    const programs = try alloc.alloc(scalar.Program, count);
    const columns = try alloc.alloc(describe.Column, count);
    for (lowered, names, programs, columns) |expression, name, *program, *column| {
        program.* = try scalar.bindWithSettings(alloc, expression, scalar_columns, parameters, .{ .invocation = backend.parameter_invocation }, backend.settings_view);
        column.* = .{ .name = name, .type = program.output_type.kind orelse .string, .element_type = program.output_type.element_type, .untyped_null = program.output_type.kind == null };
    }
    return .{ .columns = columns, .programs = programs };
}

fn qualifiedColumn(alloc: Allocator, column: relation_binding.Column) ![]const u8 {
    return if (column.qualifier.len == 0) column.name else std.fmt.allocPrint(alloc, "{s}\x00{s}", .{ column.qualifier, column.name });
}

test "MERGE arm domains retain array identity and shared assignment inference" {
    const Fixture = struct {
        fn resolve(_: *anyopaque, _: Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            try std.testing.expectEqualStrings("source", name.table);
            try std.testing.expectEqual(catalog.Action.read, action);
            return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "a", .path = "a", .type = .array, .element_type = .int16 },
                .{ .name = "text_values", .path = "text_values", .type = .array, .element_type = .text },
            } };
        }
        fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedCall;
        }
        fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn generate(_: *anyopaque, a: Allocator) ![]const u8 {
            return a.dupe(u8, "generated");
        }
    };
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "j", .path = "j", .type = .array, .element_type = .jsonb },
    } };
    const Case = struct { expression: []const u8, lower: ?i32 = 1, first: i64 = 1, failure: ?anyerror = null };
    for ([_]Case{
        .{ .expression = "s.a", .lower = 3, .first = 3 },
        .{ .expression = "'[-1:1]={9007199254740993,NULL,2}'", .lower = -1, .first = 9007199254740993 },
        .{ .expression = "ARRAY[1::smallint,NULL]" },
        .{ .expression = "NULL", .lower = null },
        .{ .expression = "$1", .lower = 5, .first = std.math.maxInt(i64) },
        .{ .expression = "s.text_values", .failure = error.SqlAssignmentTypeMismatch },
        .{ .expression = "ARRAY['1']", .failure = error.SqlAssignmentTypeMismatch },
        .{ .expression = "ARRAY[]", .failure = error.UnknownSqlArrayType },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const sql = try std.fmt.allocPrint(a, "MERGE INTO target t USING source s ON t._id=s.id WHEN MATCHED THEN UPDATE SET a={s} WHEN NOT MATCHED THEN INSERT (a) VALUES ({s}) RETURNING t.a,s.a,t.j", .{ case.expression, case.expression });
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var token: u8 = 0;
        const owner = try @import("parameter_binding.zig").Invocation.initLeaky(a, compiled.parameter_count);
        defer owner.deinitFrame();
        const backend: catalog.Backend = .{ .ptr = &token, .parameter_invocation = owner, .vtable = &.{ .generate_row_id = Fixture.generate, .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
        if (case.failure) |failure| {
            try std.testing.expectError(failure, bindCandidates(a, backend, target, &compiled, &.{}));
            continue;
        }
        const bound = try bindCandidates(a, backend, target, &compiled, &.{});
        const returning = bound.returning_plan.?;
        for (returning.columns, [_]@import("array_value.zig").ElementType{ .int64, .int16, .jsonb }) |column, element| {
            try std.testing.expectEqual(ast.ColumnType.array, column.type);
            try std.testing.expectEqual(element, column.element_type.?);
        }
        const source = try @import("array_text.zig").decodeLeaky(a, .int16, "[3:4]={3,NULL}", .{});
        const cells = try a.alloc(scalar.Datum, bound.query.columns.len);
        @memset(cells, .{});
        var source_found = false;
        for (bound.query.columns, cells) |projection, *cell| {
            if (std.mem.eql(u8, projection.field, "s\x00a")) {
                cell.* = scalar.Datum.typedArray(&source.value);
                source_found = true;
            }
        }
        if (std.mem.eql(u8, case.expression, "s.a")) try std.testing.expect(source_found);
        if (compiled.parameter_count != 0) {
            try std.testing.expectEqual(@as(usize, 1), owner.descriptors.len);
            try std.testing.expectEqual(@as(?ast.ColumnType, .array), owner.descriptors[0].kind);
            try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), owner.descriptors[0].element_type);
            const parameter = try @import("array_text.zig").decodeLeaky(a, .int64, "[5:6]={9223372036854775807,NULL}", .{});
            const wire = try @import("array_wire.zig").toJsonLeaky(a, parameter.value, .{});
            try owner.prepareJson(a, &.{wire}, .{});
        }
        for (bound.arms, 0..) |arm, ordinal| {
            const assignments = switch (arm.action) {
                .update, .insert => |values| values,
                else => unreachable,
            };
            try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), assignments[0].program.?.output_type.element_type);
            const values = try bound.evaluateValues(a, ordinal, cells, &.{});
            try std.testing.expectEqual(@as(usize, 1), values.len);
            if (case.lower) |lower| {
                try std.testing.expect(!values[0].sql_null);
                const array = values[0].array.?;
                try std.testing.expectEqual(lower, array.dimensions[0].lower);
                try std.testing.expectEqual(case.first, array.elements[0].value.integer);
                try std.testing.expect(array.elements[1].sql_null);
            } else {
                try std.testing.expect(values[0].sql_null);
                try std.testing.expect(values[0].array == null);
            }
        }
        // Constant/parameter arms can be prepared independently of the pending
        // lossless candidate reader. Verify the actual storage boundary, not
        // only the evaluator: a non-NULL typed array has a NULL JSON placeholder.
        if (!std.mem.eql(u8, case.expression, "s.a")) {
            const row = try a.alloc(std.json.Value, cells.len);
            const flags = try a.alloc(bool, cells.len);
            @memset(row, .null);
            @memset(flags, true);
            for ([_]bool{ true, false }) |matched| {
                row[0] = if (matched) .{ .string = "matched" } else .null;
                flags[0] = !matched;
                row[1] = .{ .string = "7" };
                flags[1] = false;
                row[2] = .{ .string = "" };
                flags[2] = false;
                row[4] = .{ .string = "" };
                flags[4] = false;
                const prepared = try bound.prepareMutations(a, backend, &.{row}, &.{flags}, &.{}, 2, 64 * 1024);
                try std.testing.expectEqual(@as(usize, 1), prepared.len);
                try std.testing.expectEqualStrings(if (matched) "matched" else "generated", prepared[0].key);
                try std.testing.expectEqual(@as(?u64, if (matched) 7 else 0), prepared[0].expected_version);
                const stored = prepared[0].row.?.object.get("a").?;
                if (case.lower) |lower| {
                    try std.testing.expect(stored != .null);
                    const decoded = try @import("array_wire.zig").decodeLeaky(a, .int64, stored, .{});
                    try std.testing.expectEqual(lower, decoded.dimensions[0].lower);
                    try std.testing.expectEqual(case.first, decoded.elements[0].value.integer);
                    try std.testing.expect(decoded.elements[1].sql_null);
                } else try std.testing.expect(stored == .null);
            }
        }
    }
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
        // An optional strategy may decline an unsupported source shape, but
        // cancellation, admission and backend failures must not become a
        // successful fallback which can publish after the request was stopped.
        error.UnsupportedSqlShape, error.UnsupportedSqlExecution => return null,
        else => return err,
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
    for (joined_mutation.metadata_fields) |field| try projections.qualified(alias, field);
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
    target_relation.* = .{ .table = .{ .name = statement.table, .alias = statement.alias, .mutation_target = true, .mutation_document = needs_document, .mutation_presence = true } };
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
    return row.cloneOwned(alloc);
}

fn fullCandidates(context: anytype, bound: Candidates) !*Capture {
    var fallback = context;
    fallback.binding = bound.input.*;
    fallback.typed_output = true;
    fallback.limits.result_rows = context.limits.mutation_rows;
    return fallback.typedQuery(bound.query);
}

/// Small identity-key sources avoid scanning and hashing the complete target.
/// Source and target reads may use different physical captures only because
/// the backend guarantees their range proofs join one serializable read set.
fn pointCandidates(context: anytype, bound: Candidates, plan: PointPlan) !?*Capture {
    const open = context.backend.vtable.open_statement orelse return error.SqlRangeTrackingRequired;
    var source_context = context;
    source_context.binding = plan.source_input.*;
    source_context.typed_output = true;
    source_context.limits.result_rows = @intCast(plan.source_limit + 1);
    source_context.limits.page_rows = @min(context.limits.page_rows, @as(u32, @intCast(plan.source_limit + 1)));
    const source = try source_context.typedQuery(plan.source_query);
    defer source.close();
    if (source.count() > context.limits.mutation_rows) return error.SqlResultTooLarge;
    // Above this threshold the existing coordinated hash join normally wins
    // over repeated point-capture setup, especially for small target tables.
    if (source.count() > plan.source_limit) return null;
    if (plan.index_name != null) return indexCandidates(context, bound, plan, source);
    var indexes: std.StringHashMapUnmanaged(usize) = .empty;
    var keys: std.ArrayList([]const u8) = .empty;
    var points: std.ArrayList(?catalog.Row) = .empty;
    const reader = try source.openReplayReader();
    defer reader.close();
    while (try reader.next()) |values| {
        try context.checkpoint();
        if (plan.lookup_ordinal >= values.len) return error.InvalidSqlBackendResponse;
        const value = values[plan.lookup_ordinal];
        if (value.sql_null) continue;
        if (value.value != .string or value.array != null) return error.InvalidSqlBackendResponse;
        if (value.value.string.len == 0 or indexes.contains(value.value.string)) continue;
        const key = try context.arena.dupe(u8, value.value.string);
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
    const result = try candidateCapture(context, bound.query.columns.len);
    errdefer result.close();
    var scratch = std.heap.ArenaAllocator.init(context.alloc);
    defer scratch.deinit();
    reader.rewind();
    while (try reader.next()) |source_values| {
        try context.checkpoint();
        _ = scratch.reset(.retain_capacity);
        const lookup = source_values[plan.lookup_ordinal];
        const target = if (!lookup.sql_null and lookup.value == .string) if (indexes.get(lookup.value.string)) |index| points.items[index] else null else null;
        try appendCandidate(scratch.allocator(), bound, plan, source_values, target, result);
    }
    return result;
}

fn candidateCapture(context: anytype, width: usize) !*Capture {
    return if (context.spill) |manager| Capture.create(context.alloc, manager, width) else Capture.createMemory(context.alloc, width, context.limits.retained_bytes / 2);
}

fn appendCandidate(alloc: Allocator, bound: Candidates, plan: PointPlan, source_values: []const scalar.Datum, target: ?catalog.Row, capture: *Capture) !void {
    const values = try alloc.alloc(scalar.Datum, bound.query.columns.len);
    @memset(values, .{});
    for (plan.source_ordinals, values) |source_index, *value| if (source_index) |index| {
        if (index >= source_values.len) return error.InvalidSqlBackendResponse;
        value.* = source_values[index];
    };
    if (target) |row| for (plan.target_fields, values, bound.input.columns) |field, *value, column| if (field) |name| {
        const cell = try joined_mutation.cell(alloc, row, name);
        value.* = try @import("document_row.zig").declaredCell(alloc, .{ .name = name, .path = name, .type = column.type, .element_type = column.element_type }, cell);
    };
    try Capture.append(capture, values);
}

/// Small typed-key sources probe one READY total index under a coordinated
/// read set. A saturated nonunique fanout returns to the one-pass join rather
/// than silently truncating candidates or retaining unbounded row images.
fn indexCandidates(context: anytype, bound: Candidates, plan: PointPlan, source: *Capture) !?*Capture {
    const open = context.backend.vtable.open_statement orelse return error.SqlRangeTrackingRequired;
    var unique: std.StringHashMapUnmanaged(usize) = .empty;
    var tuples: std.ArrayList([]const std.json.Value) = .empty;
    var matches: std.ArrayList([]const catalog.Row) = .empty;
    const source_slots = try context.arena.alloc(?usize, source.count());
    const reader = try source.openReplayReader();
    defer reader.close();
    for (source_slots) |*source_slot| {
        const values = (try reader.next()) orelse return error.InvalidSqlBackendResponse;
        try context.checkpoint();
        source_slot.* = null;
        const tuple = try context.arena.alloc(std.json.Value, plan.lookup_ordinals.len);
        var complete = true;
        for (plan.lookup_ordinals, tuple) |ordinal, *value| {
            if (ordinal >= values.len) return error.InvalidSqlBackendResponse;
            if (values[ordinal].sql_null) {
                complete = false;
                break;
            }
            if (values[ordinal].array != null) return error.UnsupportedSqlShape;
            value.* = try @import("runtime.zig").clone(context.arena, values[ordinal].value);
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
            error.RelationalIndexNotReady => return null,
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
    if (saturated) return null;
    const result = try candidateCapture(context, bound.query.columns.len);
    errdefer result.close();
    var scratch = std.heap.ArenaAllocator.init(context.alloc);
    defer scratch.deinit();
    reader.rewind();
    for (source_slots) |source_slot| {
        const source_values = (try reader.next()) orelse return error.InvalidSqlBackendResponse;
        _ = scratch.reset(.retain_capacity);
        try context.checkpoint();
        var emitted = false;
        if (source_slot) |slot| {
            const group = matches.items[slot];
            for (group) |target| {
                var equal = true;
                for (plan.index_columns, plan.lookup_ordinals) |column, ordinal| {
                    const cell = try joined_mutation.cell(context.arena, target, column);
                    if (cell.sql_null or (try scalar.compareDatums(source_values[ordinal], cell)) != .eq) {
                        equal = false;
                        break;
                    }
                }
                if (!equal) continue;
                if (result.count() >= context.limits.mutation_rows) return error.SqlResultTooLarge;
                try appendCandidate(scratch.allocator(), bound, plan, source_values, target, result);
                emitted = true;
            }
        }
        if (!emitted) {
            if (result.count() >= context.limits.mutation_rows) return error.SqlResultTooLarge;
            try appendCandidate(scratch.allocator(), bound, plan, source_values, null, result);
        }
    }
    return result;
}

pub fn execute(context: anytype, bound: Candidates) !@import("runtime.zig").Output {
    // The native owner must retain every source and target range proof with
    // the staged mutation. A plain autocommit batch cannot protect negative
    // match decisions, even if its row-version predicates are correct.
    if (!context.backend.atomic_statement_read_set or context.backend.vtable.open_statement == null) return error.SqlRangeTrackingRequired;
    const fast = if (context.backend.coordinated_point_reads and bound.point_plan != null)
        try pointCandidates(context, bound, bound.point_plan.?)
    else
        null;
    const selected = fast orelse try fullCandidates(context, bound);
    var selected_live = true;
    defer if (selected_live) selected.close();
    const prepared = try bound.prepareCaptured(context.arena, context.alloc, context.backend, selected, context.parameters, context.limits.mutation_rows, context.limits.retained_bytes, .{ .row_limit = context.limits.page_rows, .byte_limit = context.limits.page_bytes });
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
        const reader = try selected.openReplayReader();
        var reader_live = true;
        defer if (reader_live) reader.close();
        for (normalized, prepared.mutations, prepared.source_rows, output_rows, output_nulls, 0..) |mutation, original, source_index, *values, *nulls, returning_index| {
            try context.checkpoint();
            if (!std.mem.eql(u8, mutation.key, original.key) or mutation.expected_version != original.expected_version or
                !std.meta.eql(mutation.expected_content_digest, original.expected_content_digest) or
                mutation.unique_absence != original.unique_absence or mutation.conflict_guard != original.conflict_guard or
                mutation.predicate_only != original.predicate_only or (mutation.row == null) != (original.row == null)) return error.InvalidSqlBackendResponse;
            if (source_index >= selected.count() or source_index < reader.index) return error.InvalidSqlBackendResponse;
            while (reader.index < source_index) _ = (try reader.next()) orelse return error.InvalidSqlBackendResponse;
            const input = (try reader.next()) orelse return error.InvalidSqlBackendResponse;
            if (input.len != cells.len) return error.InvalidSqlBackendResponse;
            @memcpy(cells, input);
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
                    cells[index] = try @import("document_row.zig").declaredCell(context.arena, field, .{ .value = value, .sql_null = value == .null and !json_null });
                }
            }
            if (returning_cells) |all| {
                const owned = try context.arena.alloc(scalar.Datum, cells.len);
                for (cells, owned) |cell, *out| out.* = try @import("operators.zig").cloneDatum(context.arena, cell);
                all[returning_index] = owned;
                continue;
            }
            const projected = try context.arena.alloc(std.json.Value, plan.programs.len);
            const projected_nulls = try context.arena.alloc(bool, plan.programs.len);
            for (plan.programs, projected, projected_nulls, plan.columns) |program, *value, *is_null, column| {
                const datum = try decision_eval.evaluate(context.arena, context.backend.decision_provider, &program, cells, context.parameters);
                value.* = try context.outputDatum(datum, column.type, column.element_type);
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
                        value.* = try context.outputDatum(datum, column.type, column.element_type);
                        is_null.* = datum.sql_null;
                    }
                    output_rows[index] = projected;
                    output_nulls[index] = projected_nulls;
                }
                first = end;
            }
        }
        reader.close();
        reader_live = false;
        selected.close();
        selected_live = false;
        var output = try context.commitPreparedMutations(bound.target, normalized, "MERGE");
        output.columns = plan.columns;
        output.rows = output_rows;
        output.sql_nulls = output_nulls;
        return output;
    }
    selected.close();
    selected_live = false;
    return context.commitMutations(bound.target, prepared.mutations, "MERGE", null);
}
