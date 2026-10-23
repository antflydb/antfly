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

//! Verified per-row reducer contributions. A changed row replaces one leaf;
//! only its old/new groups are recomputed from the current contribution tree.
//! Extrema and exact NUMERIC never rely on subtraction or stale generations.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifact = @import("lake_index_aggregate_artifact.zig");
const tree = artifact.state_tree.tree;
const record = artifact.state_tree.record;
const keys = @import("lake_index_aggregate_keys.zig");
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const Pages = @import("../serverless/graph_segment/page_store.zig").PageStore;
const stores = @import("../serverless/artifacts/store.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Recipe = local.sql_aggregate_materialization.Recipe;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
pub fn eligible(provider: *Provider, recipe: Recipe) bool {
    return provider.fragment_rows != null and provider.resume_work == null and artifact.state_tree.eligible(recipe);
}
pub fn build(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, binding: local.serverless_segment_source_binding.Binding, recipe: Recipe, names: []const []const u8, file: usize, prior: ?Ref, budget: *@import("../serverless/build/lake_build_limits.zig").Budget, token: Token) !?[]Ref {
    if (!eligible(provider, recipe)) return null;
    var workspace: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 64 * 1024 * 1024 };
    return buildInner(workspace.allocator(), out, store, provider, binding, recipe, names, file, prior, budget, token) catch |err| {
        if (err == error.NativeAggregateArtifactTooLarge or err == error.GraphPageRecordTooLarge or err == error.GraphPageTooLarge or err == error.RelationalIndexKeyTooLarge or err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return null;
        return err;
    };
}
fn prefix(a: A, key: []const u8) ![]u8 {
    if (key.len + 12 > tree.max_key_bytes) return error.RelationalIndexKeyTooLarge;
    const result = try a.alloc(u8, key.len + 4);
    std.mem.writeInt(u32, result[0..4], @intCast(key.len), .big);
    @memcpy(result[4..], key);
    return result;
}
fn rowKey(a: A, group: []const u8, ordinal: u64) ![]u8 {
    const result = try a.alloc(u8, group.len + 8);
    @memcpy(result[0..group.len], group);
    std.mem.writeInt(u64, result[group.len..][0..8], ordinal, .big);
    return result;
}
const GroupDelta = struct {
    removed: ?[]u8 = null,
    added: ?[]u8 = null,
    fn append(self: *GroupDelta, a: A, value: []const u8, layout: *const record.Layout, remove: bool) !void {
        const target = if (remove) &self.removed else &self.added;
        const merged = if (target.*) |prior| try record.merge(a, prior, value, layout) else try a.dupe(u8, value);
        if (target.*) |prior| a.free(prior);
        target.* = merged;
    }
};
fn invertible(recipe: Recipe) bool {
    for (recipe.inputs) |input| if (!(input.spec.kind == .count or (input.spec.kind == .sum and input.spec.input_type == .integer) or input.spec.kind == .min or input.spec.kind == .max)) return false;
    return true;
}
fn hasExtrema(recipe: Recipe) bool {
    for (recipe.inputs) |input| if (input.spec.kind == .min or input.spec.kind == .max) return true;
    return false;
}
fn extremumPrefix(a: A, group: []const u8, slot: usize) ![]u8 {
    const result = try a.alloc(u8, group.len + 2);
    @memcpy(result[0..group.len], group);
    std.mem.writeInt(u16, result[group.len..][0..2], @intCast(slot), .big);
    return result;
}
fn indexExtrema(a: A, mutations: *Mutations, group: []const u8, ordinal: u64, value: record.View, recipe: Recipe, removing: bool) !void {
    for (recipe.inputs, 0..) |input, slot| {
        if (input.spec.kind != .min and input.spec.kind != .max) continue;
        const cell = try value.finish(a, slot, input.spec);
        if (cell.sql_null) continue;
        var key: std.ArrayList(u8) = .empty;
        defer key.deinit(a);
        try key.appendSlice(a, try extremumPrefix(a, group, slot));
        try local.storage_db_relational_index_keys.appendSqlScalar(a, &key, cell, false);
        var row: [8]u8 = undefined;
        std.mem.writeInt(u64, &row, ordinal, .big);
        try key.appendSlice(a, &row);
        if (key.items.len > tree.max_key_bytes) return error.RelationalIndexKeyTooLarge;
        try mutations.addRecord(key.items, if (removing) null else &row);
    }
}
fn adjust(a: A, previous: ?[]const u8, delta: GroupDelta, layout: *const record.Layout, recipe: Recipe, rows: artifact.RowState, storage: tree.Store, group: []const u8) ![]u8 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const before = if (previous) |value| try record.View.init(value, layout) else null;
    const removed = if (delta.removed) |value| try record.View.init(value, layout) else null;
    const added = if (delta.added) |value| try record.View.init(value, layout) else null;
    if (before == null and removed != null) return error.InvalidNativeAggregateArtifact;
    const states = try sa.alloc(local.sql_operators.Aggregate, layout.slots.len);
    for (states, recipe.inputs, 0..) |*state, input, slot| {
        state.* = try local.sql_operators.Aggregate.initTyped(sa, input.spec.kind, input.spec.input_type, input.spec.input_element);
        if (input.spec.kind == .min or input.spec.kind == .max) {
            const lower = try extremumPrefix(sa, group, slot);
            const upper = try keys.successor(sa, lower);
            const count = try tree.countRange(sa, storage, rows.extrema, lower, upper);
            if (count == 0) continue;
            var cursor = if (input.spec.kind == .max) try tree.Cursor.initReverse(sa, storage, rows.extrema, lower, upper) else try tree.Cursor.init(sa, storage, rows.extrema, lower, upper);
            defer cursor.deinit();
            const winner = (try cursor.next()) orelse return error.InvalidNativeAggregateArtifact;
            if (winner.value.len != 8) return error.InvalidNativeAggregateArtifact;
            var ordinal = std.mem.readInt(u64, winner.value[0..8], .big);
            if (input.spec.kind == .max) {
                // Equal scalar values retain the first row's representation,
                // including signed zero and exact NUMERIC display scale.
                if (winner.key.len < 8) return error.InvalidNativeAggregateArtifact;
                const value_prefix = winner.key[0 .. winner.key.len - 8];
                var first = try tree.Cursor.init(sa, storage, rows.extrema, value_prefix, try keys.successor(sa, value_prefix));
                defer first.deinit();
                const tied = (try first.next()) orelse return error.InvalidNativeAggregateArtifact;
                if (tied.value.len != 8) return error.InvalidNativeAggregateArtifact;
                ordinal = std.mem.readInt(u64, tied.value[0..8], .big);
            }
            const key = try rowKey(sa, group, ordinal);
            var row = try tree.Cursor.init(sa, storage, rows.rows, key, null);
            defer row.deinit();
            const contribution = (try row.next()) orelse return error.InvalidNativeAggregateArtifact;
            if (!std.mem.eql(u8, contribution.key, key)) return error.InvalidNativeAggregateArtifact;
            state.* = try (try record.View.init(contribution.value, layout)).read(sa, slot);
            if (state.count != 1) return error.InvalidNativeAggregateArtifact;
            state.count = count;
            if (state.numeric) |numeric| numeric.state.count = count;
            continue;
        }
        if (before) |value| state.* = try value.read(sa, slot);
        if (removed) |value| {
            const retired = try value.read(sa, slot);
            if (retired.count > state.count) return error.InvalidNativeAggregateArtifact;
            state.count -= retired.count;
            if (input.spec.kind == .sum) state.integer_sum = std.math.sub(i128, state.integer_sum, retired.integer_sum) catch return error.SqlNumericOutOfRange;
        }
        if (added) |value| try local.sql_aggregate_partial.merge(state, try value.read(sa, slot));
    }
    return record.encodeWithLayout(a, .{ .keys = try (before orelse (added orelse return error.InvalidNativeAggregateArtifact)).keys(sa), .aggregates = &.{}, .typed_states = states, .ordinal = 0 }, layout.*);
}
fn buildInner(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, binding: local.serverless_segment_source_binding.Binding, recipe: Recipe, names: []const []const u8, file: usize, prior: ?Ref, budget: *@import("../serverless/build/lake_build_limits.zig").Budget, token: Token) ![]Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const seed = if (prior) |ref| try artifact.rowSeed(ca, store.*, ref, recipe, token) else null;
    const selected = if (seed != null and (!hasExtrema(recipe) or seed.?.rows.extrema_version == 1)) provider.rowsForFile(file) else null;
    const old = if (selected != null) seed else null;
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: Pages = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    pages.admitPublication();
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = provider.context.io.?, .context = provider, .checkpoint = struct {
        fn check(raw: *anyopaque) !void {
            try @as(*Provider, @ptrCast(@alignCast(raw))).context.ensureActive();
        }
    }.check, .async_writes = false };
    defer manager.deinit();
    var changes = Mutations.init(a, &manager, pages.store());
    defer changes.deinit();
    changes.addition_first = true;
    changes.forest.policy = .addition_first;
    changes.private_pages.phase = store.io_phase;
    var reverse = Mutations.init(a, &manager, pages.store());
    defer reverse.deinit();
    reverse.private_pages.phase = store.io_phase;
    var extrema = Mutations.init(a, &manager, pages.store());
    defer extrema.deinit();
    extrema.addition_first = true;
    extrema.forest.policy = .addition_first;
    extrema.private_pages.phase = store.io_phase;
    const layout = try record.Layout.init(a, recipe);
    defer layout.deinit(a);
    var groups: std.StringHashMapUnmanaged(GroupDelta) = .empty;
    defer {
        var values = groups.valueIterator();
        while (values.next()) |delta| {
            if (delta.removed) |value| a.free(value);
            if (delta.added) |value| a.free(value);
        }
        groups.deinit(a);
    }
    if (old) |before| for (selected.?) |ordinal| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, ordinal, .big);
        var cursor = try tree.Cursor.init(a, pages.store(), before.rows.reverse, &key, null);
        defer cursor.deinit();
        const entry = (try cursor.next()) orelse return error.InvalidNativeAggregateArtifact;
        if (!std.mem.eql(u8, entry.key, &key) or entry.value.len < 4 or std.mem.readInt(u32, entry.value[0..4], .big) != entry.value.len - 4) return error.InvalidNativeAggregateArtifact;
        const group = try ca.dupe(u8, entry.value);
        const slot = try groups.getOrPut(a, group);
        if (!slot.found_existing) {
            slot.key_ptr.* = group;
            slot.value_ptr.* = .{};
        }
        const row_key = try rowKey(ca, group, ordinal);
        if (invertible(recipe) or hasExtrema(recipe)) {
            var rows = try tree.Cursor.init(a, pages.store(), before.rows.rows, row_key, null);
            defer rows.deinit();
            const value = (try rows.next()) orelse return error.InvalidNativeAggregateArtifact;
            if (!std.mem.eql(u8, value.key, row_key)) return error.InvalidNativeAggregateArtifact;
            if (invertible(recipe)) try slot.value_ptr.append(a, value.value, &layout, true);
            try indexExtrema(ca, &extrema, group, ordinal, try record.View.init(value.value, &layout), recipe, true);
        }
        try changes.addRecord(row_key, null);
    };
    var input = provider.*;
    input.only_plan = null;
    input.only_files = null;
    input.only_file = file;
    input.only_ordinals = if (old != null) selected else null;
    var source = try input.provider().open(a, binding);
    defer source.deinit(a);
    var seen: u64 = 0;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    while (try source.next(a)) |batch| {
        try budget.admitBatch(batch);
        for (0..batch.rowCount()) |row| {
            try token.check();
            try provider.context.ensureActive();
            _ = scratch.reset(.free_all);
            const sa = scratch.allocator();
            const ref = batch.rowRef(row);
            if (ref != .external or ref.external.row_group_ordinal != 0) return error.InvalidNativeAggregateArtifact;
            const ordinal = ref.external.row_ordinal;
            if (old != null and (seen >= selected.?.len or ordinal != selected.?[@intCast(seen)])) return error.InvalidNativeAggregateArtifact;
            const group_keys = try sa.alloc(local.sql_scalar.Datum, recipe.keys.len);
            for (recipe.keys, group_keys) |definition, *cell| {
                const vector: local.sql_execution_batch.Batch = .{ .columns = .{ .page = .{ .batch = batch, .selection = &.{row} }, .definitions = &.{definition.definition()} } };
                cell.* = try vector.cell(sa, 0, 0);
            }
            const encoded_key = try keys.encode(sa, group_keys);
            const group = try prefix(sa, encoded_key);
            const states = try sa.alloc(local.sql_operators.Aggregate, recipe.inputs.len);
            var initialized: usize = 0;
            defer for (states[0..initialized]) |*state| state.deinit();
            for (recipe.inputs, states) |definition, *state| {
                state.* = try local.sql_operators.Aggregate.initTyped(sa, definition.spec.kind, definition.spec.input_type, definition.spec.input_element);
                initialized += 1;
                const value = if (definition.column) |column| value: {
                    const vector: local.sql_execution_batch.Batch = .{ .columns = .{ .page = .{ .batch = batch, .selection = &.{row} }, .definitions = &.{column.definition()} } };
                    break :value try vector.cell(sa, 0, 0);
                } else local.sql_scalar.Datum.json(.{ .integer = 1 });
                try state.update(value);
            }
            const encoded = try record.encodeWithLayout(sa, .{ .keys = group_keys, .aggregates = &.{}, .typed_states = states, .ordinal = ordinal }, layout);
            try changes.addRecord(try rowKey(sa, group, ordinal), encoded);
            try indexExtrema(sa, &extrema, group, ordinal, try record.View.init(encoded, &layout), recipe, false);
            var reverse_key: [8]u8 = undefined;
            std.mem.writeInt(u64, &reverse_key, ordinal, .big);
            try reverse.addRecord(&reverse_key, group);
            const slot = try groups.getOrPut(a, group);
            if (!slot.found_existing) {
                slot.value_ptr.* = .{};
                slot.key_ptr.* = try ca.dupe(u8, group);
            }
            if (old == null or invertible(recipe)) try slot.value_ptr.append(a, encoded, &layout, false);
            seen += 1;
        }
    }
    if (seen != (if (old != null) selected.?.len else provider.source.inventory.files[file].row_count)) return error.InvalidNativeAggregateArtifact;
    const row_state: artifact.RowState = .{ .rows = try changes.finish(if (old) |seed_| seed_.rows.rows else null), .reverse = try reverse.finish(if (old) |seed_| seed_.rows.reverse else null), .extrema_version = 1, .extrema = try extrema.finish(if (old) |seed_| seed_.rows.extrema else null) };
    if ((if (row_state.rows) |root| root.records else 0) != provider.source.inventory.files[file].row_count or (if (row_state.reverse) |root| root.records else 0) != provider.source.inventory.files[file].row_count) return error.InvalidNativeAggregateArtifact;
    var result_changes = Mutations.init(a, &manager, pages.store());
    defer result_changes.deinit();
    result_changes.private_pages.phase = store.io_phase;
    var iterator = groups.iterator();
    while (iterator.next()) |item| {
        const group = item.key_ptr;
        try token.check();
        try provider.context.ensureActive();
        if (old == null) {
            try result_changes.addRecord(group.*[4..], item.value_ptr.added orelse return error.InvalidNativeAggregateArtifact);
            continue;
        }
        const upper = try keys.successor(a, group.*);
        defer if (upper) |key| a.free(key);
        if (invertible(recipe)) {
            if (try tree.countRange(a, pages.store(), row_state.rows, group.*, upper) == 0) {
                try result_changes.addRecord(group.*[4..], null);
                continue;
            }
            var cursor = try tree.Cursor.init(a, pages.store(), old.?.index.page, group.*[4..], null);
            defer cursor.deinit();
            const found = try cursor.next();
            const previous = if (found) |value| (if (std.mem.eql(u8, value.key, group.*[4..])) value.value else null) else null;
            const updated = try adjust(a, previous, item.value_ptr.*, &layout, recipe, row_state, pages.store(), group.*);
            defer a.free(updated);
            try result_changes.addRecord(group.*[4..], updated);
            continue;
        }
        var cursor = try tree.Cursor.init(a, pages.store(), row_state.rows, group.*, upper);
        defer cursor.deinit();
        var merged: ?[]u8 = null;
        defer if (merged) |value| a.free(value);
        while (try cursor.next()) |entry| {
            const value = if (merged) |previous| try record.merge(a, previous, entry.value, &layout) else try a.dupe(u8, entry.value);
            if (merged) |previous| a.free(previous);
            merged = value;
        }
        try result_changes.addRecord(group.*[4..], merged);
    }
    const index: artifact.state_tree.Index = .{ .domain = pages.domain, .layout = 2, .page = try result_changes.finish(if (old) |seed_| seed_.index.page else null) };
    return artifact.publishTreeWithRows(a, out, store, names, recipe, index, row_state, token);
}

test "external lake aggregate row deltas gather only changed rows and retract moved extrema nulls" {
    try aggregateRowScenario(.integer);
}
test "external lake ordered extrema retain strings and exact NUMERIC across row retractions" {
    try aggregateRowScenario(.string);
    try aggregateRowScenario(.numeric);
}
fn aggregateRowScenario(comptime mode: enum { integer, string, numeric }) !void {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-row-transitions");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(7);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, null);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    const Rows = local.storage_rowsource_types;
    const Fixture = struct {
        keys: [4][]const u8 = .{ "a", "a", "b", "b" },
        values: [4]i64 = if (mode == .numeric) .{ 9007199254740993, 9007199254740994, 9007199254740995, 9007199254740996 } else if (mode == .string) .{ 2, 10, 3, 20 } else .{ 1, 5, 10, 20 },
        text_buffers: [4][64]u8 = undefined,
        texts: [4][]const u8 = undefined,
        null_first: bool = false,
        selected: ?[]const u64 = null,
        finished: bool = false,
        visited: usize = 0,
        refs: [4]Rows.RowRef = undefined,
        columns: [2]Rows.ColumnVector = undefined,
        selected_keys: [4][]const u8 = undefined,
        selected_values: [4]i64 = undefined,
        nulls: [4]u8 = @splat(0),
        fn open(raw: *anyopaque, _: A, _: local.serverless_segment_source_binding.Binding, _: ?usize, _: Token, _: ?*const stores.IoPhase) !Rows.Source {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.selected = null;
            self.finished = false;
            return .{ .kind = .external_parquet, .ctx = self, .next_batch = next, .deinit_fn = close };
        }
        fn selectedOpen(raw: *anyopaque, alloc: A, binding: local.serverless_segment_source_binding.Binding, file: usize, selected: []const u64, token: Token, phase: ?*const stores.IoPhase) !Rows.Source {
            const result = try open(raw, alloc, binding, file, token, phase);
            @as(*@This(), @ptrCast(@alignCast(raw))).selected = selected;
            return result;
        }
        fn close(_: *anyopaque, _: A) void {}
        fn next(raw: *anyopaque, _: A) !?Rows.ColumnBatch {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.finished) return null;
            self.finished = true;
            const count = if (self.selected) |selected| selected.len else self.keys.len;
            self.nulls = @splat(0);
            for (0..count) |position| {
                const index: usize = if (self.selected) |selected| @intCast(selected[position]) else position;
                self.selected_keys[position] = self.keys[index];
                self.selected_values[position] = self.values[index];
                self.texts[position] = try std.fmt.bufPrint(&self.text_buffers[position], "{d}", .{self.values[index]});
                if (index == 0 and self.null_first) self.nulls[position] = 1;
                self.refs[position] = .{ .external = .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = "fragment", .row_group_ordinal = 0, .row_ordinal = index } };
            }
            self.columns = .{
                .{ .name = "k", .values = .{ .bytes = self.selected_keys[0..count] } },
                .{ .name = "n", .nulls = .{ .bytes = self.nulls[0..count] }, .values = if (mode == .integer) .{ .i64 = self.selected_values[0..count] } else .{ .bytes = self.texts[0..count] } },
            };
            self.visited += count;
            return .{ .snapshot = .{ .table_id = "source", .snapshot_id = "snapshot" }, .columns = &self.columns, .row_refs = self.refs[0..count] };
        }
    };
    var fixture: Fixture = .{};
    var input_files = [_]local.serverless_external_source_types.FileEntry{.{ .file_id = @constCast("fragment"), .object_uri = @constCast("file:///fragment"), .row_count = 4, .byte_len = 1, .row_groups = &.{} }};
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .scanner = undefined, .inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &input_files } };
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .fragment_rows = .{ .ptr = &fixture, .open = Fixture.open, .open_selected = Fixture.selectedOpen } };
    const value_type = if (mode == .integer) .integer else if (mode == .string) .string else .number;
    const element: ?local.sql_array_value.ElementType = if (mode == .numeric) .numeric else null;
    const column: local.sql_aggregate_materialization.Key = .{ .path = "n", .type = value_type, .element_type = element, .nullable = true };
    const all: Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = if (mode == .integer) &.{
        .{ .spec = .{ .kind = .count }, .column = null },
        .{ .spec = .{ .kind = .sum, .input_type = .integer }, .column = column },
        .{ .spec = .{ .kind = .min, .input_type = .integer }, .column = column },
        .{ .spec = .{ .kind = .max, .input_type = .integer }, .column = column },
    } else &.{
        .{ .spec = .{ .kind = .count }, .column = null },
        .{ .spec = .{ .kind = .min, .input_type = value_type, .input_element = element }, .column = column },
        .{ .spec = .{ .kind = .max, .input_type = value_type, .input_element = element }, .column = column },
    } };
    const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .algebraic, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "aggregate-rows", .column_bindings = &.{ "k", "n" } };
    for ([_]bool{ false, true }) |fixed_only| {
        if (mode != .integer and fixed_only) continue;
        fixture = .{};
        provider.changed_rows = &.{};
        var recipe = all;
        if (fixed_only) recipe.inputs = all.inputs[0..2];
        const layout = try record.Layout.init(a, recipe);
        defer layout.deinit(a);
        var previous: ?Ref = null;
        for (0..4) |step| {
            fixture.visited = 0;
            if (step != 0) provider.changed_rows = &.{if (step == 2) &.{0} else &.{1}};
            if (step == 1) {
                fixture.keys[1] = "b";
                fixture.values[1] = 99;
            }
            if (step == 2) fixture.null_first = true;
            if (step == 3) {
                fixture.keys[1] = "a";
                fixture.values[1] = -7;
            }
            var budget = try @import("../serverless/build/lake_build_limits.zig").Budget.init(.{});
            const references = (try build(a, out, &store, &provider, binding, recipe, (if (fixed_only) &.{ "count", "sum" } else if (mode == .integer) &.{ "count", "sum", "min", "max" } else &.{ "count", "min", "max" }), 0, previous, &budget, .none)).?;
            try std.testing.expectEqual(@as(usize, if (step == 0) 4 else 1), fixture.visited);
            previous = references[0];
            const seed = (try artifact.rowSeed(out, store, references[0], recipe, .none)).?;
            var reads: u64 = 0;
            var writes: u64 = 0;
            var pages: Pages = .{ .domain = domain, .artifacts = &store, .cancellation = .none, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
            var cursor = try tree.Cursor.init(a, pages.store(), seed.index.page, "", null);
            defer cursor.deinit();
            var groups: usize = 0;
            while (try cursor.next()) |entry| {
                const view = try record.View.init(entry.value, &layout);
                const group_key = (try view.keys(out))[0].value.string;
                var count: i64 = 0;
                var sum: i64 = 0;
                var minimum: ?i64 = null;
                var maximum: ?i64 = null;
                for (fixture.keys, fixture.values, 0..) |key, value, index| if (std.mem.eql(u8, key, group_key)) {
                    count += 1;
                    if (index == 0 and fixture.null_first) continue;
                    sum += value;
                    if (mode == .string) {
                        const text = try std.fmt.allocPrint(out, "{d}", .{value});
                        minimum = if (minimum) |prior| if (std.mem.lessThan(u8, text, try std.fmt.allocPrint(out, "{d}", .{prior}))) value else prior else value;
                        maximum = if (maximum) |prior| if (std.mem.lessThan(u8, try std.fmt.allocPrint(out, "{d}", .{prior}), text)) value else prior else value;
                    } else {
                        minimum = if (minimum) |prior| @min(prior, value) else value;
                        maximum = if (maximum) |prior| @max(prior, value) else value;
                    }
                };
                try std.testing.expectEqual(count, (try view.finish(out, 0, recipe.inputs[0].spec)).value.integer);
                const expected = [_]?i64{ if (minimum != null) sum else null, minimum, maximum };
                const values = if (mode == .integer) expected[0 .. recipe.inputs.len - 1] else expected[1..];
                for (values, 1..) |value, slot| {
                    const actual = try view.finish(out, slot, recipe.inputs[slot].spec);
                    if (value) |number| {
                        if (mode == .integer) {
                            try std.testing.expectEqual(number, actual.value.integer);
                        } else {
                            const text = try std.fmt.allocPrint(out, "{d}", .{number});
                            if (mode == .string) try std.testing.expectEqualStrings(text, actual.value.string) else {
                                var work: local.sql_array_value.Budget = .{};
                                const wanted = try local.sql_scalar.numericTextLeaky(out, text, &work);
                                try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(wanted, actual));
                            }
                        }
                    } else try std.testing.expect(actual.sql_null);
                }
                groups += 1;
            }
            try std.testing.expectEqual(@as(usize, 2), groups);
            var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 4, .authority = undefined, .store = store, .identity = identity, .context = .{}, .token = @splat(1), .audit_only = true, .remaining_reads = 32 * 1024 * 1024 };
            defer {
                var iterator = collector.marked.keyIterator();
                while (iterator.next()) |key| a.free(key.*);
                collector.marked.deinit(a);
            }
            try collector.markArtifact(out, references[0]);
            try std.testing.expect(collector.marked.contains(&try Pages.identity(domain, seed.rows.rows.?)));
            try std.testing.expect(collector.marked.contains(&try Pages.identity(domain, seed.rows.reverse.?)));
            if (!fixed_only) {
                try std.testing.expectEqual(@as(u8, 1), seed.rows.extrema_version);
                try std.testing.expect(collector.marked.contains(&try Pages.identity(domain, seed.rows.extrema.?)));
            }
        }
    }
}
