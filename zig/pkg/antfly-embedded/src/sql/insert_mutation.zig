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

//! Command-private ordered INSERT. Source projections are pulled on demand;
//! conversion shares the ordinary INSERT assignment/default machinery.
const std = @import("std");
const runtime = @import("runtime.zig");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const Datum = @import("scalar.zig").Datum;

pub const Stream = struct {
    context: runtime.Context,
    statement: ast.Insert,
    metadata: std.heap.ArenaAllocator,
    scratch: std.heap.ArenaAllocator,
    preparation: runtime.Context.InsertPreparation,
    builder: ?runtime.Context.InsertSourceBuilder = null,
    input: ?@import("mutation_input.zig").Input = null,
    position: usize = 0,
    affected: usize = 0,
    finished: bool = false,

    pub fn create(input: runtime.Context, original: ast.Insert) !*Stream {
        if (input.backend.statement_effect_execution == null or input.backend.statement_mutations == null) return error.SqlStatementSnapshotRequired;
        const self = try input.alloc.create(Stream);
        self.* = .{ .context = input, .statement = original, .metadata = .init(input.alloc), .scratch = .init(input.alloc), .preparation = .{ .allocator = undefined } };
        self.preparation.allocator = self.metadata.allocator();
        errdefer self.close();
        if (input.binding.normalized_insert_columns) |columns| {
            self.statement.columns = columns;
            self.statement.implicit_columns = false;
        }
        const a = self.metadata.allocator();
        const table = input.binding.table orelse return error.InvalidSqlBackendResponse;
        try table.validateDocumentWriteColumns(self.statement.columns);
        if (self.statement.source != null) {
            const source = input.binding.insert_source orelse return error.InvalidSqlBackendResponse;
            const count = self.statement.columns.len + if (self.statement.conflict) |clause| clause.capture_count else @as(usize, 0);
            if (source.columns.len != count) return error.InvalidSqlBackendResponse;
            const columns = try a.alloc(catalog.Column, self.statement.columns.len);
            const provenance = try @import("domain_mutation.zig").Builder.init(a, table, false);
            for (self.statement.columns, columns, source.columns[0..columns.len]) |name, *column, from| {
                column.* = try table.column(name);
                try provenance.set(column.*, from.domain_view, false);
            }
            self.builder = .{ .columns = columns, .source = source, .domain_admission = provenance.admission() };
            var read = input;
            read.binding = source.*;
            self.input = try @import("mutation_input.zig").Input.init(read, a);
        }
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.input) |*input| input.close();
        if (self.preparation.owner) |owner| owner.close(owner.ptr);
        self.scratch.deinit();
        self.metadata.deinit();
        self.context.alloc.destroy(self);
    }

    fn finish(self: *Stream) !void {
        if (self.input) |*input| input.close();
        self.input = null;
        if (self.preparation.owner) |owner| {
            try owner.finish(owner.ptr, self.scratch.allocator());
            owner.close(owner.ptr);
            self.preparation.owner = null;
        }
        if (self.statement.values_source_rows.len != 0 and self.position != self.statement.values_source_rows.len) return error.InvalidSqlBackendResponse;
        self.finished = true;
    }

    pub fn next(self: *Stream) anyerror!?[]const Datum {
        if (self.finished) return null;
        while (true) {
            try self.context.checkpoint();
            if (!self.scratch.reset(.{ .retain_with_limit = 32 * 1024 })) return error.OutOfMemory;
            var context = self.context;
            context.arena = self.scratch.allocator();
            context.typed_output = true;
            context.limits.native_result = true;
            context.limits.page_rows = 1;
            const output = if (self.builder) |builder| source: {
                const row = try self.input.?.next(self.scratch.allocator()) orelse {
                    try self.finish();
                    return null;
                };
                if (self.position >= context.limits.mutation_rows) return error.SqlProgramLimitExceeded;
                const proposal = try context.insertSourceRow(self.statement, builder, row, self.position);
                self.position += 1;
                const table = context.binding.table.?;
                const mutations = if (self.statement.conflict) |clause|
                    try @import("conflict.zig").resolve(context, table, clause, context.binding.conflict orelse return error.InvalidSqlBackendResponse, &.{proposal}, &.{row[self.statement.columns.len..]})
                else
                    &.{proposal};
                if (mutations.len == 0) continue;
                if (self.preparation.owner == null) self.preparation.owner = try (context.backend.vtable.open_mutation_preparation orelse return error.UnsupportedSqlExecution)(context.backend.ptr, self.metadata.allocator(), table, mutations[0].key, self.statement.returning != null or table.storage_mode == .document);
                const owner = self.preparation.owner.?;
                const prepared = try owner.prepare(owner.ptr, context.arena, mutations);
                if (prepared.len != mutations.len) return error.InvalidSqlBackendResponse;
                break :source try context.commitPreparedReturning(table, prepared, "INSERT", self.statement.returning);
            } else values: {
                if (self.position == self.statement.rows.len) {
                    try self.finish();
                    return null;
                }
                const index = self.position;
                self.position += 1;
                break :values try context.insertValues(self.statement, index, index + 1, &self.preparation);
            };
            self.affected += output.rows_affected;
            if (self.statement.returning == null or output.rowCount() == 0) continue;
            const rows = output.native_rows orelse return error.InvalidSqlBackendResponse;
            if (rows.len != 1) return error.InvalidSqlBackendResponse;
            return rows[0];
        }
    }
};
