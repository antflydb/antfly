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

//! Execution-local WITH producers, tuple ownership and graph-wide admission.
//! Native SQL sessions stage writes; this owner never creates a coordinator.
const std = @import("std");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const plan = @import("statement_effects.zig");
const scope = @import("statement_cte_scope.zig");
const Replay = @import("replay_rows.zig").Replay;
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;

const Key = struct { table: u64, physical: []const u8, row: []const u8, version: u64 };
const KeyContext = struct {
    pub fn hash(_: @This(), key: Key) u64 {
        var h = std.hash.Wyhash.init(0);
        std.hash.autoHash(&h, key.table);
        std.hash.autoHash(&h, key.version);
        std.hash.autoHash(&h, key.physical.len);
        h.update(key.physical);
        h.update(key.row);
        return h.final();
    }
    pub fn eql(_: @This(), left: Key, right: Key) bool {
        return left.version == right.version and left.table == right.table and std.mem.eql(u8, left.physical, right.physical) and std.mem.eql(u8, left.row, right.row);
    }
};
const Directory = std.HashMapUnmanaged(Key, usize, KeyContext, std.hash_map.default_max_load_percentage);
const Claim = struct { version: u64, digest: ?[32]u8, retired: bool };
const Pending = std.HashMapUnmanaged(Key, Claim, KeyContext, std.hash_map.default_max_load_percentage);
const MutationStream = union(enum) {
    joined: *@import("joined_mutation.zig").Stream,
    insert: *@import("insert_mutation.zig").Stream,
    merge: *@import("merge_mutation.zig").Stream,

    fn create(input: runtime.Context, step: plan.Step) !MutationStream {
        var context = input;
        context.backend = @import("decision_eval.zig").scopedBackend(context.backend, step.binding);
        if (step.binding.joined_mutation) |bound| return .{ .joined = try @import("joined_mutation.zig").Stream.create(context, bound.*) };
        if (step.binding.merge_mutation) |bound| return .{ .merge = try @import("merge_mutation.zig").Stream.create(context, bound.*) };
        if (step.statement == .insert) return .{ .insert = try @import("insert_mutation.zig").Stream.create(context, step.statement.insert) };
        return error.UnsupportedSqlShape;
    }
    fn next(self: MutationStream) anyerror!?[]const Datum {
        return switch (self) {
            inline else => |stream| stream.next(),
        };
    }
    fn close(self: MutationStream) void {
        switch (self) {
            inline else => |stream| stream.close(),
        }
    }
    fn affected(self: MutationStream) usize {
        return switch (self) {
            inline else => |stream| stream.affected,
        };
    }
};
fn orderedStream(step: plan.Step) bool {
    return step.ordered_effects and (step.binding.joined_mutation != null or step.binding.merge_mutation != null or step.statement == .insert);
}
const Slot = struct {
    owner: *State,
    index: usize,
    status: enum { fresh, running, complete, failed } = .fresh,
    failure: ?anyerror = null,
    replay: ?*Replay = null,
    output: ?runtime.Output = null,
    stream: ?MutationStream = null,
    position: usize = 0,
    cursor: ?catalog.Cursor = null,
    scratch: ?std.heap.ArenaAllocator = null,
    page: ?catalog.Page = null,
    columns: ?catalog.ColumnPage = null,

    fn stop(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.stream) |stream| stream.close();
        self.stream = null;
        if (self.page) |page| page.deinit();
        self.page = null;
        self.columns = null;
        if (self.cursor) |cursor| cursor.close(cursor.ptr);
        self.cursor = null;
        if (self.scratch) |*scratch| scratch.deinit();
        self.scratch = null;
    }
    fn pull(raw: *anyopaque, a: A) anyerror!?[]const Datum {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const owner = self.owner;
        try owner.context.checkpoint();
        const step = owner.graph.steps[self.index];
        if (step.mutation) {
            if (orderedStream(step)) {
                const row = try owner.nextJoined(self.index) orelse return null;
                try owner.captureRow();
                return row;
            }
            try owner.mutation(self.index);
            const output = self.output.?;
            if (self.position == output.rowCount()) return null;
            const rows = output.native_rows orelse return error.InvalidSqlBackendResponse;
            const row = rows[self.position];
            self.position += 1;
            try owner.captureRow();
            return row;
        }
        if (self.cursor == null) {
            var context = owner.child(step, self.index);
            context.sink = null;
            self.cursor = try @import("relation_runtime.zig").openCursor(context);
            self.scratch = .init(owner.context.alloc);
        }
        while (true) {
            if (self.page) |page| page.deinit();
            self.page = null;
            self.columns = null;
            if (!self.scratch.?.reset(.{ .retain_with_limit = 16 * 1024 })) return error.OutOfMemory;
            const temporary = self.scratch.?.allocator();
            const fields = step.binding.relation.?.root.columns;
            const values = try a.alloc(Datum, fields.len);
            if (self.cursor.?.next_columns) |next| {
                self.columns = try next(self.cursor.?.ptr, temporary, 1);
                try self.columns.?.validate();
                if (self.columns.?.selection.len == 0) {
                    if (self.columns.?.after != null) continue;
                    return null;
                }
                if (self.columns.?.selection.len != 1) return error.InvalidSqlBackendResponse;
                for (fields, values) |field, *value| value.* = try self.columns.?.cell(a, 0, field.internal);
            } else {
                self.page = try self.cursor.?.next(self.cursor.?.ptr, temporary, 1);
                if (self.page.?.rows.len == 0) {
                    if (self.page.?.after != null) continue;
                    return null;
                }
                if (self.page.?.rows.len != 1) return error.InvalidSqlBackendResponse;
                for (fields, values) |field, *value| value.* = try self.page.?.rows[0].cell(field.internal);
            }
            try owner.captureRow();
            return values;
        }
    }
};
const State = struct {
    context: runtime.Context,
    graph: *const plan.Bound,
    slots: []Slot,
    current: usize,
    tuples: Directory = .empty,
    pending: Pending = .empty,
    mutations: usize = 0,
    scanned: usize = 0,
    pages: usize = 0,
    captured: usize = 0,
    work: usize = 0,
    prior_scalar_control: @FieldType(catalog.Backend, "scalar_control") = null,

    fn child(self: *State, step: plan.Step, ordinal: usize) runtime.Context {
        var context = self.context;
        context.binding = step.binding;
        context.backend.statement_ctes = self.graph.scope;
        context.backend.statement_cte_visibility = ordinal;
        context.backend.coordinated_point_reads = false;
        context.backend.coordinated_index_reads = false;
        context.limits.native_result = true;
        context.limits.result_rows = context.limits.scan_rows;
        context.sink = null;
        context.typed_output = true;
        return context;
    }
    fn nextJoined(self: *State, index: usize) anyerror!?[]const Datum {
        const slot = &self.slots[index];
        switch (slot.status) {
            .complete => return null,
            .failed => return slot.failure.?,
            .running => return error.SqlInvalidRecursion,
            .fresh => {},
        }
        slot.status = .running;
        const saved = self.current;
        self.current = index;
        defer self.current = saved;
        const row = self.advanceJoined(index) catch |err| {
            slot.status = .failed;
            slot.failure = err;
            return err;
        };
        slot.status = if (row == null) .complete else .fresh;
        return row;
    }
    fn advanceJoined(self: *State, index: usize) anyerror!?[]const Datum {
        const slot = &self.slots[index];
        if (slot.stream == null) slot.stream = try MutationStream.create(self.child(self.graph.steps[index], index), self.graph.steps[index]);
        return slot.stream.?.next();
    }
    fn mutation(self: *State, index: usize) !void {
        const slot = &self.slots[index];
        if (orderedStream(self.graph.steps[index])) {
            if (self.graph.scope.definitions[index].returns_rows and self.graph.steps[index].demanded) {
                const reader = try open(self, index);
                defer reader.close();
                while (try reader.next() != null) {}
            } else {
                while (try self.nextJoined(index) != null) {}
            }
            return;
        }
        switch (slot.status) {
            .complete => return,
            .failed => return slot.failure.?,
            .running => return error.SqlInvalidRecursion,
            .fresh => {},
        }
        slot.status = .running;
        const saved = self.current;
        self.current = index;
        defer self.current = saved;
        const context = self.child(self.graph.steps[index], index);
        slot.output = context.run(self.graph.steps[index].statement) catch |err| {
            slot.status = .failed;
            slot.failure = err;
            return err;
        };
        slot.status = .complete;
    }
    fn open(raw: *anyopaque, index: usize) anyerror!*Replay.Reader {
        const self: *State = @ptrCast(@alignCast(raw));
        if (index >= self.slots.len or !self.graph.scope.definitions[index].returns_rows) return error.InvalidSqlBackendResponse;
        const slot = &self.slots[index];
        if (slot.replay == null) {
            const replay = try Replay.create(self.context.alloc, self.graph.scope.definitions[index].columns.len, self.context.limits.retained_bytes / 16, self.context.spill);
            errdefer replay.deinit();
            try replay.attachProducer(.{ .ptr = slot, .pull = Slot.pull, .close = Slot.stop }, self.context.limits.scan_rows);
            slot.replay = replay;
        }
        return slot.replay.?.openReader();
    }
    fn claim(raw: *anyopaque, a: A, table: catalog.Table, input: []const catalog.Mutation, tag: []const u8) anyerror![]const catalog.Mutation {
        const self: *State = @ptrCast(@alignCast(raw));
        var selected: ?std.ArrayList(catalog.Mutation) = null;
        defer if (selected) |*values| values.deinit(a);
        for (input, 0..) |mutation_, index| {
            if (!mutation_.predicate_only) {
                if (mutation_.unique_absence and std.mem.eql(u8, tag, "INSERT")) if (self.pending.get(.{ .table = table.id, .physical = table.physical_name, .row = mutation_.key, .version = 0 })) |previous| {
                    if (!previous.retired) return error.DuplicateSqlRow;
                };
                const key: Key = .{ .table = table.id, .physical = table.physical_name, .row = mutation_.key, .version = mutation_.expected_version };
                if (self.tuples.get(key)) |owner| {
                    if (owner != self.current) {
                        if (std.mem.eql(u8, tag, "MERGE")) return error.SqlMutationCardinalityViolation;
                        if (mutation_.unique_absence) return error.UniqueConstraintViolation;
                        if (selected == null) {
                            selected = .empty;
                            try selected.?.appendSlice(a, input[0..index]);
                        }
                        continue;
                    }
                } else {
                    if (self.mutations >= self.context.limits.mutation_rows) return error.SqlProgramLimitExceeded;
                    const owned = try self.context.arena.dupe(u8, mutation_.key);
                    try self.tuples.put(self.context.alloc, .{ .table = table.id, .physical = table.physical_name, .row = owned, .version = mutation_.expected_version }, self.current);
                    self.mutations += 1;
                }
                const registered = self.tuples.getEntry(key) orelse return error.InvalidSqlBackendResponse;
                const pending_key: Key = .{ .table = table.id, .physical = table.physical_name, .row = registered.key_ptr.row, .version = 0 };
                const retired = std.mem.eql(u8, tag, "DELETE") or (std.mem.eql(u8, tag, "MERGE") and mutation_.row == null);
                if (self.pending.getPtr(pending_key)) |original| {
                    original.retired = retired;
                    if (original.digest == null and original.version == mutation_.expected_version) original.digest = mutation_.expected_content_digest;
                } else try self.pending.put(self.context.alloc, pending_key, .{ .version = mutation_.expected_version, .digest = mutation_.expected_content_digest, .retired = retired });
            }
            if (selected) |*values| try values.append(a, mutation_);
        }
        return if (selected) |*values| try values.toOwnedSlice(a) else input;
    }
    fn pendingOwner(raw: *anyopaque, table: catalog.Table, key: []const u8) ?scope.PendingTuple {
        const self: *State = @ptrCast(@alignCast(raw));
        const entry = self.pending.getEntry(.{ .table = table.id, .physical = table.physical_name, .row = key, .version = 0 }) orelse return null;
        return .{ .key = entry.key_ptr.row, .version = entry.value_ptr.version, .digest = entry.value_ptr.digest, .retired = entry.value_ptr.retired };
    }
    fn checkpoint(raw: *anyopaque) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw));
        if (self.work == self.context.limits.scan_rows *| 64) return error.SqlProgramLimitExceeded;
        self.work += 1;
    }
    fn scalarCheckpoint(raw: ?*anyopaque) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw.?));
        try checkpoint(self);
        if (self.prior_scalar_control) |control| {
            try control.checkpoint(control.ptr);
        } else try self.context.backend.vtable.checkpoint(self.context.backend.ptr);
    }
    fn scan(raw: *anyopaque, count: usize) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw));
        if (count > self.context.limits.scan_rows -| self.scanned) return error.SqlProgramLimitExceeded;
        self.scanned += count;
    }
    fn scanPage(raw: *anyopaque) anyerror!void {
        const self: *State = @ptrCast(@alignCast(raw));
        if (self.pages == self.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
        self.pages += 1;
    }
    fn captureRow(self: *State) !void {
        if (self.captured == self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
        self.captured += 1;
    }
    fn stopProducers(self: *State) void {
        // A suspended read CTE still borrows native pages from the common cut.
        // Retire every producer before closing that cut or any shared replay.
        for (self.slots) |*slot| if (slot.replay) |replay| replay.closeProducer();
    }
    fn deinit(self: *State) void {
        self.stopProducers();
        for (self.slots) |*slot| {
            if (slot.replay) |replay| replay.deinit();
            Slot.stop(slot);
        }
        self.tuples.deinit(self.context.alloc);
        self.pending.deinit(self.context.alloc);
        self.context.alloc.free(self.slots);
    }
};

pub fn execute(input: runtime.Context, graph: *const plan.Bound) anyerror!runtime.Output {
    if (!input.backend.atomic_statement_read_set or input.backend.vtable.open_statement == null) return error.SqlStatementSnapshotRequired;
    // Writable WITH is not a read-only cursor. Public SQL transports materialize
    // the statement before committing and delivering any result rows.
    if (input.sink != null) return error.UnsupportedSqlShape;
    const admission = input.backend.statement_mutations orelse return error.SqlStatementSnapshotRequired;
    const command = try admission.begin(admission.ptr, input.alloc);
    defer command.abort(command.ptr);
    if (command.bind) |bind| try bind(command.ptr, input.binding);
    try @import("decision_eval.zig").validateStatement(input.arena, input.backend.decision_provider, input.binding, input.parameters);
    var capture = try @import("mutation_capture.zig").open(input.alloc, input.backend, input.binding, input.parameters);
    defer capture.deinit();
    var state: State = .{ .context = input, .graph = graph, .slots = try input.alloc.alloc(Slot, graph.steps.len), .current = graph.steps.len };
    for (state.slots, 0..) |*slot, index| slot.* = .{ .owner = &state, .index = index };
    defer state.deinit();
    state.context.statement_capture = &capture;
    state.prior_scalar_control = input.backend.scalar_control;
    state.context.backend.scalar_control = .{ .ptr = &state, .checkpoint = State.scalarCheckpoint };
    state.context.backend.statement_effect_execution = .{ .ptr = &state, .open = State.open, .claim = State.claim, .checkpoint = State.checkpoint, .scan_rows = State.scan, .scan_page = State.scanPage, .pending_owner = State.pendingOwner };
    var root = state.child(graph.root, graph.steps.len);
    root.limits = input.limits;
    root.typed_output = input.typed_output;
    const output = if (orderedStream(graph.root))
        try collectMutation(root, graph.root)
    else
        try root.run(graph.root.statement);
    // Auxiliary mutations finish even when unused or hidden by LIMIT 0. Their
    // outputs remain private; the root alone determines the command result.
    for (graph.steps, 0..) |step, index| if (step.mutation) try state.mutation(index);
    // Caller-owned native transaction admission happens after this returns.
    // No captured read lease survives into physical publication.
    state.stopProducers();
    capture.release();
    try command.finish(command.ptr);
    return output;
}

/// Root mutations retain their command result while each volatile row advances
/// through the same private producer used by CTE consumers.
fn collectMutation(input: runtime.Context, step: plan.Step) !runtime.Output {
    var context = input;
    context.backend = @import("decision_eval.zig").scopedBackend(context.backend, context.binding);
    const stream = try MutationStream.create(context, step);
    defer stream.close();
    var rows: std.ArrayList([]const std.json.Value) = .empty;
    var nulls: std.ArrayList([]const bool) = .empty;
    var native: std.ArrayList([]const Datum) = .empty;
    while (try stream.next()) |values| {
        if (rows.items.len + native.items.len >= context.limits.result_rows) return error.SqlResultTooLarge;
        const rendered = try context.resultRow(values, context.binding.columns);
        if (context.limits.native_result) {
            try native.append(context.arena, rendered.native.?);
        } else {
            try rows.append(context.arena, rendered.json);
            try nulls.append(context.arena, rendered.nulls);
        }
    }
    return .{ .columns = context.binding.columns, .rows = rows.items, .sql_nulls = if (context.limits.native_result) null else nulls.items, .native_rows = if (context.limits.native_result) native.items else null, .rows_affected = stream.affected(), .command_tag = if (step.statement == .insert) "INSERT" else if (step.statement == .merge) "MERGE" else if (step.binding.joined_mutation.?.deleting) "DELETE" else "UPDATE", .mutation_outcome = .committed };
}
