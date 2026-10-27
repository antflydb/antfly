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

//! One projected, delete-aware scan feeds independent bounded native builders.
//! Each replay retains at most one file. The publication producer may pair two
//! independently bounded buffers for read-ahead. Whole-source consumers stream ordinarily;
//! per-file consumers share a bounded replay without an archive-wide spool.
const std = @import("std");
const local = @import("antfly_local_sources");
const rows = local.storage_rowsource_types;
const spill = local.sql_spill;
const Provider = @import("lake_index_row_source.zig").Provider;
const Binding = local.serverless_segment_source_binding.Binding;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
/// Plan the shared projection once. Unsupported expression dependencies remain
/// safe through Provider's ordinary-scan fallback when a column is not covered.
pub fn columnsForBuild(a: A, out: A, table: local.common_topology_records.TableRecord, provider: *Provider, base: local.serverless_manifest_base_source.BaseSourceDescriptor) ![]const []const u8 {
    return columnsForFamilies(a, out, table, provider, base, .{ true, true, true, true, true });
}
pub fn columnsForFamilies(a: A, out: A, table: local.common_topology_records.TableRecord, provider: *Provider, base: local.serverless_manifest_base_source.BaseSourceDescriptor, enabled: [5]bool) ![]const []const u8 {
    const rebuild = @import("../serverless/build/lake_rebuild.zig");
    var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(a, base, provider.source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
    defer desired.deinit(a);
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer parsed.deinit(a);
    const schema = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, schema);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, schema);
    defer layout.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(a);
    const configs = try std.json.parseFromSliceLeaky(std.json.Value, ca, table.indexes_json, .{});
    var consumers: usize = 0;
    for (desired.artifacts) |artifact| {
        const family: usize = switch (artifact.kind) {
            .algebraic_segment => 0,
            .text_segment => 2,
            .sparse_segment => 3,
            .vector_segment => 4,
            else => continue,
        };
        if (!enabled[family]) continue;
        consumers += 1;
        if (artifact.kind == .text_segment) {
            const config = try std.json.parseFromSliceLeaky(std.json.Value, ca, artifact.build_spec.?.text.config_json, .{});
            const field = config.object.get("field");
            if (field == null or field.? != .string or try @import("lake_index_native_text.zig").storesSource(config)) {
                for (schema.relational_columns) |column| try names.put(a, column.name, {});
                continue;
            }
        }
        if (artifact.kind == .vector_segment or artifact.kind == .sparse_segment) if (configs.object.get(artifact.name)) |config| {
            if (@import("lake_enrichment_units.zig").configured(config, "template") != null) for (schema.relational_columns) |column| try names.put(a, column.name, {});
        };
        for (artifact.binding.column_bindings) |column| try names.put(a, column, {});
    }
    if (enabled[1]) if (try parsed.relationalIndexDefinitions(ca)) |indexes| for (indexes) |index| {
        consumers += 1;
        var tuple = try local.storage_db_relational_index_keys.TuplePlan.init(a, schema, &layout, index.keys);
        defer tuple.deinit();
        for (try tuple.columnBindings(ca)) |column| try names.put(a, column, {});
        for (try @import("lake_index_native_rows.zig").coverColumns(ca, index)) |column| try names.put(a, column, {});
        if (index.where.len != 0) {
            var predicate = try local.storage_db_relational_index_predicate.Plan.init(a, schema, &layout, index.where);
            defer predicate.deinit();
            for (predicate.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |column| try names.put(a, column, {});
        }
    };
    // Contract binds nullable/missing columns across heterogeneous Parquet files.
    const contract = try out.alloc(local.serverless_query_lake_schema.Column, schema.relational_columns.len);
    for (contract, schema.relational_columns) |*column, definition| column.* = .{ .name = try out.dupe(u8, definition.path), .kind = if (definition.sql_element_type == .numeric) "numeric" else @tagName(definition.column_type), .required = definition.required and !definition.allows_null };
    provider.schema_contract = contract;
    provider.replay_recommended = consumers >= 2;
    if (consumers == 0 or names.count() == 0) return &.{};
    const columns = try out.alloc([]const u8, names.count());
    var iterator = names.keyIterator();
    for (columns) |*column| column.* = try out.dupe(u8, iterator.next().?.*);
    std.mem.sort([]const u8, columns, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.less);
    return columns;
}
/// Sparse union owned by the publication planner. No archive-sized replay mask.
pub const Needed = struct {
    files: local.encoding_roaring.RoaringBitmap,
    pub fn deinit(self: *Needed) void {
        self.files.deinit();
    }
    pub fn contains(self: *const Needed, ordinal: usize) bool {
        return self.files.contains(std.math.cast(u32, ordinal) orelse return false);
    }
    pub fn fromMask(a: A, mask: []const bool) !Needed {
        var self: Needed = .{ .files = .init(a) };
        errdefer self.deinit();
        for (mask, 0..) |needed, ordinal| if (needed) try self.add(ordinal);
        return self;
    }
    fn add(self: *Needed, ordinal: usize) !void {
        try self.files.add(std.math.cast(u32, ordinal) orelse return error.InvalidNativeLakeFileState);
    }
    fn addPlan(self: *Needed, plan: *const @import("lake_index_native_state.zig").Plan) !void {
        var it = plan.iterator(true);
        while (try it.next()) |item| try self.add(item.ordinal);
    }
};
fn deltaMatches(provider: *Provider, source: []const u8, schema: []const u8, snapshot: []const u8) bool {
    const delta = provider.native_delta orelse return false;
    return delta.count() == provider.source.inventory.files.len and delta.prior_snapshot != null and std.mem.eql(u8, delta.prior_snapshot.?, snapshot) and
        std.mem.eql(u8, delta.source, source) and std.mem.eql(u8, delta.schema, schema);
}
/// Union changed native files so delta builders share only the necessary input.
/// Unknown/legacy producer formats conservatively request a complete replay.
pub fn changedFiles(a: A, out: A, provider: *Provider, store: @import("../serverless/artifacts/store.zig").ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const local.metadata_lake_index_catalog.FileContribution, cancellation: Cancellation) !?Needed {
    return changedFilesIndexed(a, out, provider, store, declarations, contributions, cancellation, null);
}
pub fn changedFilesIndexed(a: A, out: A, parent_provider: *Provider, store: @import("../serverless/artifacts/store.zig").ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const local.metadata_lake_index_catalog.FileContribution, cancellation: Cancellation, index: ?*@import("lake_index_contributions.zig").Index) !?Needed {
    const state = @import("lake_index_native_state.zig");
    var needed: Needed = .{ .files = .init(out) };
    var returned = false;
    defer if (!returned) needed.deinit();
    const aggregate = @import("lake_index_native_aggregates.zig");
    var old: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer old.deinit(a);
    for (contributions) |contribution| try old.put(a, aggregate.contributionKey(contribution.file, contribution.recipe, contribution.name), {});
    for (declarations) |declaration| {
        if (!parent_provider.wantsIndex(declaration.name)) continue;
        var source = parent_provider.source.*;
        var selected = parent_provider.forIndex(&source, declaration.name);
        const provider = &selected;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ca = arena.allocator();
        if (declaration.artifact.kind == .algebraic_segment) {
            if (!@import("lake_index_aggregate_artifact.zig").supportsMetadataVersion(declaration.artifact.metadata_version)) return null;
            const recipe = try @import("lake_index_aggregate_artifact.zig").loadRecipe(ca, store, declaration.artifact, cancellation);
            if (!aggregate.incrementalRecipe(recipe) or provider.source.inventory.deleted_row_groups.len != 0 or (if (provider.source.scanner.iceberg_delete_plan) |plan| plan.files.len != 0 else false)) return null;
            const fingerprint = recipe.fingerprint();
            if (provider.native_delta) |delta| if (index) |lookup| if (lookup.acceptsDelta(delta) and try lookup.reductionRoot(ca, fingerprint, declaration.name, delta.prior_count) != null) {
                var plan = try state.Plan.fromDelta(a, ca, delta);
                defer plan.deinit();
                try needed.addPlan(&plan);
                continue;
            };
            for (provider.source.inventory.files, 0..) |file, ordinal| {
                const key = aggregate.fileIdentity(provider.source, file);
                const lookup_key = aggregate.contributionKey(key, fingerprint, declaration.name);
                if (!needed.contains(ordinal) and !old.contains(lookup_key)) {
                    const missing = if (index) |lookup| missing: {
                        if (lookup.has_file_coverage) break :missing !lookup.covered_files.contains(key);
                        var scratch = std.heap.ArenaAllocator.init(a);
                        defer scratch.deinit();
                        break :missing (try lookup.lookup(scratch.allocator(), lookup_key)) == null;
                    } else true;
                    if (missing) try needed.add(ordinal);
                }
            }
            continue;
        }
        const previous: []const state.File = switch (declaration.artifact.kind) {
            .text_segment => text: {
                const native = @import("lake_index_native_text.zig");
                if (declaration.artifact.metadata_version != native.metadata_version) return null;
                const directory = try native.loadRootDirectory(ca, store, declaration.artifact, cancellation, null);
                if (directory.inventory_version == 3 and directory.checkpoint_base == null and directory.checkpoint_files == null and directory.inventory_delta_version != 0 and deltaMatches(provider, directory.binding.source_id, directory.binding.schema_fingerprint, directory.binding.snapshot_id) and directory.file_count == provider.native_delta.?.prior_count) {
                    var plan = try state.Plan.fromDelta(a, ca, provider.native_delta.?);
                    defer plan.deinit();
                    try needed.addPlan(&plan);
                    continue;
                }
                if (directory.inventory_version == 3) {
                    const records = try native.text_directory.fileRecords(ca, directory, store, null, cancellation);
                    const files = try ca.alloc(state.File, records.len);
                    for (files, records) |*file, record| file.* = record.file;
                    break :text files;
                }
                const root = try native.loadRoot(ca, store, declaration.artifact, cancellation, null);
                const files = try ca.alloc(state.File, root.file_groups.len);
                for (files, root.file_groups) |*file, group| file.* = group.file;
                break :text files;
            },
            .sparse_segment => sparse: {
                const native = @import("lake_index_native_sparse.zig");
                if (!native.supportsMetadata(declaration.artifact.metadata_version)) return null;
                const root = try native.loadRoot(ca, store, declaration.artifact, cancellation, null);
                if (root.file_directory != null and state.canUseDelta(provider, root.binding.snapshot_id)) {
                    var plan = try state.Plan.fromDelta(a, ca, provider.native_delta.?);
                    defer plan.deinit();
                    try needed.addPlan(&plan);
                    continue;
                }
                break :sparse if (root.file_directory) |directory| try state.directoryFiles(ca, store, directory, root.generation.domain, cancellation) else root.file_states;
            },
            .vector_segment => dense: {
                const native = @import("lake_index_native_dense.zig");
                if (!native.supportsMetadata(declaration.artifact.metadata_version)) return null;
                const root = try native.loadRoot(ca, store, declaration.artifact, cancellation, null);
                if (root.file_directory != null and state.canUseDelta(provider, root.binding.snapshot_id)) {
                    var plan = try state.Plan.fromDelta(a, ca, provider.native_delta.?);
                    defer plan.deinit();
                    try needed.addPlan(&plan);
                    continue;
                }
                break :dense if (root.file_directory) |directory| try state.directoryFiles(ca, store, directory, root.generation.domain, cancellation) else root.file_states;
            },
            .ordered_row_index => ordered: {
                const native = @import("lake_index_ordered_rows.zig");
                if (!native.supportsBuildMetadata(declaration.artifact.metadata_version)) return null;
                const lazy = try native.loadRootLazy(ca, store, declaration.artifact, cancellation, null);
                if (!lazy.building and deltaMatches(provider, lazy.source, declaration.binding.schema_fingerprint, lazy.snapshot)) {
                    var plan = try state.Plan.fromDelta(a, ca, provider.native_delta.?);
                    defer plan.deinit();
                    try needed.addPlan(&plan);
                    continue;
                }
                const root = try native.loadRoot(ca, store, declaration.artifact, cancellation, null);
                if (root.file_fingerprints.len != root.files.len) return null;
                const files = try ca.alloc(state.File, root.files.len);
                for (files, root.files, root.file_fingerprints) |*file, id, digest| file.* = .{ .id = id, .digest = digest };
                break :ordered files;
            },
            else => return null,
        };
        var plan = try state.Plan.init(a, ca, provider, previous);
        defer plan.deinit();
        try needed.addPlan(&plan);
    }
    returned = true;
    return needed;
}
pub const Replay = struct {
    provider: *Provider,
    columns: []const []const u8,
    arena: std.heap.ArenaAllocator,
    budget: local.sql_memory_budget,
    manager: spill.Manager = undefined,
    run: ?spill.Sequential = null,
    kinds: []rows.ColumnKind = &.{},
    range: Range = .{ .begin = .{ .row = 0, .byte = 0 }, .end = 0 },
    ready: bool = false,
    live: ?rows.Source = null,
    live_allocator: A = undefined,
    captures: usize = 0,
    read_ahead_hits: usize = 0,
    publication_sessions: usize = 0,
    streamed_batches: usize = 0,
    retained_batches: usize = 0,
    pipeline: ?*@import("lake_index_build_pipeline.zig") = null,
    /// A bounded cold-build window owns each sibling independently.
    sibling: ?*Replay = null,
    /// A speculative lane pins only the first borrowed source batch. Joining
    /// transfers its cursor and this batch together; no replay spool is built.
    primed: ?rows.ColumnBatch = null,
    primed_ready: bool = false,
    captured_file: ?usize = null,
    readers: usize = 0,
    direct: bool = false,
    decoded: [8]?CachedPage = @splat(null),
    decoded_bytes: usize = 0,
    /// Frozen sibling runs share their enclosing window quota. Reclaim idle
    /// residency from peers without waiting while holding this replay's lock.
    cache_peers: ?[]Replay = null,
    decoded_clock: u64 = 0,
    decoded_hits: usize = 0,
    decoded_loads: usize = 0,
    direct_replays: usize = 0,
    mutex: std.Io.Mutex = .init,
    max_spill_bytes: u64 = 1024 * 1024 * 1024,
    only_files: ?Needed = null,
    const Range = struct { begin: spill.Sequential.Position, end: u64 };
    const Page = local.storage_db_types.ColumnSourcePage;
    const CachedPage = struct { row: u64, following: spill.Sequential.Position, page: *Page, refs: []const rows.RowRef, used: u64 };

    pub fn init(a: A, provider: *Provider, columns: []const []const u8) Replay {
        return .{ .provider = provider, .columns = columns, .arena = .init(a), .budget = .{ .backing = a, .limit = 256 * 1024 * 1024 } };
    }
    pub fn deinit(self: *Replay) void {
        self.reset() catch unreachable;
        if (self.only_files) |*needed| needed.deinit();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
    }
    /// Discard an unleased optimization buffer before changing its admission.
    pub fn reset(self: *Replay) !void {
        if (self.readers != 0) return error.LakeReplayBusy;
        if (self.live) |source| source.deinit(self.live_allocator);
        self.live = null;
        self.primed = null;
        self.primed_ready = false;
        while (self.evictDecoded()) {}
        if (self.run) |*run| {
            run.close();
            self.manager.deinit();
            self.run = null;
        } else if (self.direct) self.manager.deinit();
        self.direct = false;
        _ = self.arena.reset(.free_all);
        self.ready = false;
        self.captured_file = null;
    }
    fn check(raw: *anyopaque) !void {
        const self: *Replay = @ptrCast(@alignCast(raw));
        try self.provider.context.ensureActive();
        if (self.pipeline) |pipeline| try pipeline.check();
    }
    fn captureCheckpoint(raw: *anyopaque) !void {
        const self: *Replay = @ptrCast(@alignCast(raw));
        if (self.provider.context.additional_checkpoint) |hook| try hook.check(hook.ptr);
        if (self.pipeline) |pipeline| try pipeline.check();
    }
    pub fn beginStream(self: *Replay, a: A, binding: Binding, file: usize, token: Cancellation) !void {
        try self.reset();
        var input = self.provider.*;
        input.replay = null;
        input.replay_consumer = null;
        input.only_file = file;
        input.only_files = null;
        input.only_plan = null;
        input.context.additional_checkpoint = .{ .ptr = self, .check = captureCheckpoint };
        self.live = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, token);
        self.live_allocator = a;
        self.ready = true;
        self.captured_file = file;
        self.captures += 1;
    }
    pub fn primeStream(self: *Replay, a: A, binding: Binding, file: usize, token: Cancellation) anyerror!bool {
        try self.beginStream(a, binding, file, token);
        self.primed = try self.live.?.next(a);
        self.primed_ready = true;
        return true;
    }
    pub fn nextLive(self: *Replay) !?rows.ColumnBatch {
        if (self.primed_ready) {
            self.primed_ready = false;
            return self.primed;
        }
        return self.live.?.next(self.live_allocator);
    }
    fn capture(self: *Replay, a: A, original: Binding, file_index: usize, cancellation: Cancellation) !void {
        if (self.ready and self.captured_file == file_index) return;
        try self.reset();
        self.budget.exhausted = false;
        self.captured_file = file_index;
        const io = self.provider.context.io orelse return error.UnsupportedSqlExecution;
        self.manager = .{ .alloc = self.budget.allocator(), .io = io, .context = self, .checkpoint = check, .max_bytes = self.max_spill_bytes };
        var shared_binding = original;
        shared_binding.column_bindings = self.columns;
        if (self.provider.fragment_rows) |fragments| if (fragments.replayable) |replayable| if (replayable(fragments.ptr, shared_binding)) {
            self.direct = true;
            self.ready = true;
            self.captures += 1;
            self.direct_replays += 1;
            return;
        };
        self.run = spill.Sequential.init(&self.manager, 512 * 1024) catch |err| {
            self.manager.deinit();
            return err;
        };
        const ca = self.arena.allocator();
        self.kinds = try ca.alloc(rows.ColumnKind, self.columns.len);
        @memset(self.kinds, .bytes);
        self.range = .{ .begin = .{ .row = 0, .byte = 0 }, .end = 0 };
        if (file_index >= self.provider.source.inventory.files.len) return error.SidecarSourceBindingMismatch;
        var input = self.provider.*;
        input.replay = null;
        input.replay_consumer = null;
        if (self.pipeline != null) input.context.additional_checkpoint = .{ .ptr = self, .check = captureCheckpoint };
        input.only_file = file_index;
        input.only_files = null;
        input.only_plan = null;
        var binding = original;
        binding.column_bindings = self.columns;
        const source = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, cancellation);
        defer source.deinit(a);
        var active: ?usize = null;
        var kinds_bound = false;
        while (try source.next(a)) |batch| {
            try cancellation.check();
            try self.provider.context.ensureActive();
            if (batch.rowCount() == 0) continue;
            const vectors = try self.budget.allocator().alloc(rows.ColumnVector, self.columns.len);
            defer self.budget.allocator().free(vectors);
            for (self.columns, self.kinds, vectors) |name, *kind, *vector| {
                const column = batch.findColumn(name) orelse return error.RowSourceColumnKindMismatch;
                vector.* = column;
                const actual = column.kind().logical();
                if (kinds_bound and kind.* != actual) return error.RowSourceColumnKindMismatch;
                kind.* = actual;
            }
            kinds_bound = true;
            var page = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer page.deinit();
            // Bind borrowed source vectors once; the spill codec gathers one
            // bounded column at a time and chooses its physical dictionary.
            var capture_batch: CaptureBatch = .{ .vectors = vectors };
            const borrowed: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &capture_batch, .read = CaptureBatch.cell, .read_physical = CaptureBatch.physical, .read_identity = CaptureBatch.identity, .count = batch.rowCount(), .width = self.columns.len } };
            var physical_arena = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer physical_arena.deinit();
            const values = try borrowed.preparePhysical(physical_arena.allocator());
            var begin: usize = 0;
            while (begin < batch.rowCount()) {
                const external = switch (batch.rowRef(begin)) {
                    .external => |ref| ref,
                    else => return error.SidecarSourceBindingMismatch,
                };
                if (!std.mem.eql(u8, external.file_id, self.provider.source.inventory.files[file_index].file_id)) return error.SidecarSourceBindingMismatch;
                const file = file_index;
                if (active == null) {
                    self.range.begin = try self.run.?.replayBoundary();
                    active = file;
                }
                var end = begin;
                while (end < batch.rowCount() and end - begin < 256) : (end += 1) {
                    const next = switch (batch.rowRef(end)) {
                        .external => |ref| ref,
                        else => return error.SidecarSourceBindingMismatch,
                    };
                    if (!std.mem.eql(u8, next.file_id, external.file_id)) break;
                }
                _ = page.reset(.retain_capacity);
                const pa = page.allocator();
                const selection = try pa.alloc(usize, end - begin);
                const ordinals = try pa.alloc(u64, selection.len);
                const file_ids = try pa.alloc(local.sql_scalar.Datum, selection.len);
                @memset(file_ids, .{ .value = .{ .integer = @intCast(file) }, .sql_null = false });
                const groups = try pa.alloc(local.sql_scalar.Datum, selection.len);
                const positions = try pa.alloc(local.sql_scalar.Datum, selection.len);
                for (selection, ordinals, groups, positions, begin..) |*selected, *ordinal, *group, *position, index| {
                    const ref = batch.rowRef(index).external;
                    selected.* = index;
                    ordinal.* = self.run.?.size + index - begin;
                    group.* = .{ .value = .{ .integer = ref.row_group_ordinal }, .sql_null = false };
                    // u64 coordinates retain their bits without decimal formatting.
                    position.* = .{ .value = .{ .integer = @bitCast(ref.row_ordinal) }, .sql_null = false };
                }
                const keys: local.sql_execution_batch.Batch = .{ .vectors = .{ .values = &.{ file_ids, groups, positions }, .count = selection.len } };
                try self.run.?.appendBatch(try values.select(pa, selection), keys, ordinals);
                begin = end;
            }
        }
        self.range.end = self.run.?.size;
        try self.run.?.seal();
        self.ready = true;
        self.captures += 1;
    }
    pub const CaptureBatch = struct {
        vectors: []const rows.ColumnVector,
        pub fn physical(raw: *anyopaque, _: A, ordinal: usize) !?local.sql_execution_batch.PhysicalColumn {
            const self: *CaptureBatch = @ptrCast(@alignCast(raw));
            const column = self.vectors[ordinal];
            const kind: local.sql_ast.ColumnType = switch (column.values) {
                .i64, .dictionary_i64 => .integer,
                .f64, .dictionary_f64 => .number,
                .bool => .boolean,
                .bytes, .dictionary_bytes, .json => .string,
                else => return null,
            };
            var vector = column;
            // JSON source columns are serialized bytes in the replay contract.
            if (vector.values == .json) vector.values = .{ .bytes = vector.values.json };
            return .{ .vector = vector, .range = .{ .count = column.rowCount() }, .kind = kind, .element = if (kind == .number) .float64 else null };
        }
        pub fn identity(raw: *anyopaque, row: usize, ordinal: usize) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const column = self.vectors[ordinal];
            const ids = switch (column.values) {
                .dictionary_i64 => |v| v.indices,
                .dictionary_f64 => |v| v.indices,
                .dictionary_bytes => |v| v.indices,
                else => return null,
            };
            return if (column.nulls.isNull(row)) 0 else @as(u64, ids[row]) + 2;
        }
        pub fn cell(raw: *anyopaque, a: A, row: usize, ordinal: usize) !local.sql_scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const column = self.vectors[ordinal];
            if (column.nulls.isNull(row)) return .{};
            return .{ .sql_null = false, .value = switch (column.values) {
                .decimal128, .encoded => try column.values.jsonAt(a, row),
                .i64 => |v| .{ .integer = v[row] },
                .dictionary_i64 => |v| .{ .integer = v.at(row) },
                .f64 => |v| .{ .float = v[row] },
                .dictionary_f64 => |v| .{ .float = v.at(row) },
                .bool => |v| .{ .bool = v[row] },
                .bytes, .json => |v| .{ .string = v[row] },
                .dictionary_bytes => |v| .{ .string = v.at(row) },
                .vector_f32 => return error.UnsupportedExternalLakeIndex,
            } };
        }
    };
    fn evictDecoded(self: *Replay) bool {
        var victim: ?usize = null;
        for (self.decoded, 0..) |entry, i| if (entry) |value| {
            if (victim == null or value.used < self.decoded[victim.?].?.used) victim = i;
        };
        const i = victim orelse return false;
        const value = self.decoded[i].?;
        self.decoded_bytes -= value.page.retainedBytes();
        self.decoded[i] = null;
        value.page.release();
        return true;
    }
    fn releaseBlock(raw: *anyopaque) void {
        @as(*spill.Sequential.OwnedBlock, @ptrCast(@alignCast(raw))).release();
    }
    fn decode(self: *Replay, reader: *spill.Sequential.Reader) !?CachedPage {
        const row = reader.offset;
        const block = try reader.nextOwned() orelse return null;
        var owns = true;
        defer if (owns) block.release();
        const a = self.budget.allocator();
        const page = try a.create(Page);
        page.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = block.count(), .owner = .{ .ptr = block, .release_fn = releaseBlock, .retained_bytes = block.retainedBytes() } };
        owns = false;
        errdefer page.release();
        const pa = page.arena.allocator();
        const refs = try pa.alloc(rows.RowRef, block.count());
        const inventory = self.provider.source.inventory;
        for (refs, 0..) |*ref, index| {
            if (block.keyWidth() != 3) return error.InvalidSqlSpill;
            const file = std.math.cast(usize, (try block.keyCell(index, 0)).value.integer) orelse return error.InvalidSqlSpill;
            if (file >= inventory.files.len) return error.InvalidSqlSpill;
            ref.* = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .identity_version = inventory.row_identity_version, .file_id = try pa.dupe(u8, inventory.files[file].file_id), .row_group_ordinal = std.math.cast(u32, (try block.keyCell(index, 1)).value.integer) orelse return error.InvalidSqlSpill, .row_ordinal = @bitCast((try block.keyCell(index, 2)).value.integer) } };
        }
        const columns = try pa.alloc(rows.ColumnVector, self.columns.len);
        for (columns, self.columns, self.kinds, 0..) |*column, name, kind, ordinal| {
            column.* = try block.columnVector(pa, ordinal, kind);
            column.name = try pa.dupe(u8, name);
        }
        page.columns = columns;
        self.decoded_loads += 1;
        return .{ .row = row, .following = reader.position(), .page = page, .refs = refs, .used = self.decoded_clock };
    }
    fn reclaimPeers(self: *Replay) !void {
        const peers = self.cache_peers orelse return;
        // Independent cursors retain the sealed run. Drop this cache lock before
        // waiting for another one, and restore it on every success/error path.
        self.mutex.unlock(self.manager.io);
        defer self.mutex.lockUncancelable(self.manager.io);
        for (peers) |*peer| {
            try self.provider.context.ensureActive();
            try peer.mutex.lock(self.manager.io);
            while (peer.evictDecoded()) {}
            peer.mutex.unlock(self.manager.io);
        }
    }
    fn readDecoded(self: *Replay, reader: *spill.Sequential.Reader) !?CachedPage {
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        self.decoded_clock +|= 1;
        for (&self.decoded) |*entry| if (entry.*) |*value| if (value.row == reader.offset) {
            try reader.seekPosition(value.following);
            value.used = self.decoded_clock;
            value.page.retain();
            self.decoded_hits += 1;
            return value.*;
        };
        const before = reader.position();
        const value = (self.decode(reader) catch |err| retry: {
            if (err != error.OutOfMemory or !self.budget.isExhausted()) return err;
            while (self.evictDecoded()) {}
            try self.reclaimPeers();
            try reader.seekPosition(before);
            break :retry try self.decode(reader);
        }) orelse return null;
        const cost = value.page.retainedBytes();
        const limit = @min(8 * 1024 * 1024, self.budget.limit / 4);
        if (cost <= limit) {
            while (self.decoded_bytes + cost > limit) if (!self.evictDecoded()) break;
            while (true) {
                for (&self.decoded) |*entry| if (entry.* == null) {
                    value.page.retain();
                    entry.* = value;
                    self.decoded_bytes += cost;
                    return value;
                };
                if (!self.evictDecoded()) break;
            }
        }
        return value;
    }
    pub fn prepare(self: *Replay, a: A, binding: Binding, file_index: usize, cancellation: Cancellation) !bool {
        const io = self.provider.context.io orelse return false;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        return self.captureAdmitted(a, binding, file_index, cancellation);
    }
    fn captureAdmitted(self: *Replay, a: A, binding: Binding, file_index: usize, cancellation: Cancellation) !bool {
        // Oversized individual files retain the ordinary streaming path. The
        // replay optimization must never make an otherwise valid scan fail.
        self.capture(a, binding, file_index, cancellation) catch |err| {
            if (err == error.SqlProgramLimitExceeded or err == error.UnsupportedExternalLakeIndex or (err == error.OutOfMemory and self.budget.isLocallyExhausted())) {
                if (self.run) |*run| {
                    run.close();
                    self.manager.deinit();
                    self.run = null;
                }
                _ = self.arena.reset(.free_all);
                self.ready = false;
                return false;
            }
            return err;
        };
        return true;
    }
    pub fn open(self: *Replay, a: A, binding: Binding, only_file: ?usize, cancellation: Cancellation) !?rows.Source {
        return self.openFor(a, binding, only_file, cancellation, null);
    }
    pub fn openFor(self: *Replay, a: A, binding: Binding, only_file: ?usize, cancellation: Cancellation, consumer: ?usize) !?rows.Source {
        return self.openForUsingIo(a, binding, only_file, cancellation, consumer, null);
    }
    pub fn openForUsingIo(self: *Replay, a: A, binding: Binding, only_file: ?usize, cancellation: Cancellation, consumer: ?usize, phase: ?*const @import("../serverless/artifacts/store.zig").IoPhase) !?rows.Source {
        try cancellation.check();
        try self.provider.context.ensureActive();
        try binding.validate();
        const inventory = self.provider.source.inventory;
        if (!std.mem.eql(u8, binding.source_id, inventory.source_id) or !std.mem.eql(u8, binding.snapshot_id, inventory.snapshot_id) or !std.mem.eql(u8, binding.schema_fingerprint, inventory.schema_fingerprint) or binding.row_ref_kind != .external) return error.SidecarSourceBindingMismatch;
        for (binding.column_bindings) |name| {
            const present = for (self.columns) |column| {
                if (std.mem.eql(u8, name, column)) break true;
            } else false;
            if (!present) return null;
        }
        const file_index = only_file orelse return null;
        if (file_index >= self.provider.source.inventory.files.len) return error.SidecarSourceBindingMismatch;
        if (consumer == null and self.captured_file != file_index) {
            var sibling = self.sibling;
            while (sibling) |candidate| : (sibling = candidate.sibling) {
                if (candidate.ready and candidate.captured_file == file_index) return candidate.openForUsingIo(a, binding, only_file, cancellation, null, phase);
            }
        }
        if (consumer) |slot| if (self.pipeline) |pipeline| {
            const chosen = (try pipeline.request(slot, file_index, cancellation)) orelse return null;
            return try chosen.openPrepared(a, binding, file_index, cancellation, slot, phase);
        };
        const io = self.provider.context.io orelse return null;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.readers != 0 and self.captured_file != file_index) return null;
        if (consumer == null) if (self.only_files) |needed| {
            if (file_index >= self.provider.source.inventory.files.len) return error.SidecarSourceBindingMismatch;
            if (!needed.contains(file_index)) return null;
        };
        if (!try self.captureAdmitted(a, binding, file_index, cancellation)) return null;
        return try self.openCursorFor(a, binding, cancellation, null, phase);
    }
    /// Producer has already captured and granted this file. Never re-enter the
    /// rendezvous or apply the serial planner's advisory changed-file mask.
    fn openPrepared(self: *Replay, a: A, binding: Binding, file_index: usize, cancellation: Cancellation, slot: usize, phase: ?*const @import("../serverless/artifacts/store.zig").IoPhase) !rows.Source {
        const io = self.provider.context.io.?;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (!self.ready or self.captured_file != file_index) return error.SidecarSourceBindingMismatch;
        return try self.openCursorFor(a, binding, cancellation, slot, phase);
    }
    fn openCursor(self: *Replay, a: A, binding: Binding, cancellation: Cancellation) !rows.Source {
        return self.openCursorFor(a, binding, cancellation, null, null);
    }
    fn openCursorFor(self: *Replay, a: A, binding: Binding, cancellation: Cancellation, slot: ?usize, phase: ?*const @import("../serverless/artifacts/store.zig").IoPhase) !rows.Source {
        const range = self.range;
        const columns = try a.dupe([]const u8, binding.column_bindings);
        errdefer a.free(columns);
        const cursor = try a.create(Cursor);
        errdefer a.destroy(cursor);
        var direct: ?rows.Source = null;
        if (self.direct) {
            const fragments = self.provider.fragment_rows.?;
            direct = try fragments.open(fragments.ptr, a, binding, self.captured_file, cancellation, phase);
        }
        cursor.* = .{ .owner = self, .slot = slot, .direct = direct, .reader = if (self.live != null or direct != null) null else try self.run.?.readerSealed(a, range.begin, range.end), .arena = .init(a), .columns = columns, .cancellation = cancellation };
        self.readers += 1;
        return .{ .kind = binding.source_kind, .ctx = cursor, .next_batch = Cursor.next, .deinit_fn = Cursor.deinit, .retain_columns = Cursor.retain };
    }
    const Cursor = struct {
        page: ?*Page = null,
        direct: ?rows.Source = null,
        owner: *Replay,
        reader: ?spill.Sequential.Reader,
        slot: ?usize = null,
        generation: u64 = 0,
        arena: std.heap.ArenaAllocator,
        columns: []const []const u8,
        cancellation: Cancellation,
        fn deinit(raw: *anyopaque, a: A) void {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            if (self.page) |page| page.release();
            if (self.direct) |source| source.deinit(a) else if (self.reader) |*reader| reader.deinit() else self.owner.pipeline.?.leaveStream(self.slot.?);
            const io = self.owner.provider.context.io.?;
            self.owner.mutex.lockUncancelable(io);
            self.owner.readers -= 1;
            self.owner.mutex.unlock(io);
            self.arena.deinit();
            a.free(self.columns);
            a.destroy(self);
        }
        fn releasePage(raw: *anyopaque) void {
            @as(*Page, @ptrCast(@alignCast(raw))).release();
        }
        const Retained = struct {
            a: A,
            owner: *Replay,
            inner: rows.ColumnOwner,
            fn release(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const replay = self.owner;
                self.inner.release();
                self.a.destroy(self);
                replay.mutex.lockUncancelable(replay.provider.context.io.?);
                replay.readers -= 1;
                replay.mutex.unlock(replay.provider.context.io.?);
            }
        };
        fn retain(raw: *anyopaque, a: A) !?rows.ColumnOwner {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            const inner = if (self.direct) |source| (try source.retainColumns(a)) orelse return null else block: {
                const page = self.page orelse return null;
                page.retain();
                break :block rows.ColumnOwner{ .ptr = page, .release_fn = releasePage, .retained_bytes = page.retainedBytes() };
            };
            errdefer inner.release();
            const held = try a.create(Retained);
            held.* = .{ .a = a, .owner = self.owner, .inner = inner };
            self.owner.mutex.lockUncancelable(self.owner.provider.context.io.?);
            self.owner.readers += 1;
            self.owner.mutex.unlock(self.owner.provider.context.io.?);
            return .{ .ptr = held, .release_fn = Retained.release, .retained_bytes = inner.retained_bytes +| @sizeOf(Retained) };
        }
        fn next(raw: *anyopaque, caller: A) !?rows.ColumnBatch {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            try self.cancellation.check();
            try self.owner.provider.context.ensureActive();
            if (self.page) |page| page.release();
            self.page = null;
            if (self.direct) |source| return source.next(caller);
            _ = self.arena.reset(.retain_capacity);
            const a = self.arena.allocator();
            if (self.reader == null) {
                const input = try self.owner.pipeline.?.readStream(self.slot.?, &self.generation, self.cancellation) orelse return null;
                const columns = try a.alloc(rows.ColumnVector, self.columns.len);
                for (columns, self.columns) |*column, name| column.* = input.findColumn(name) orelse return error.RowSourceColumnKindMismatch;
                var result = input;
                result.columns = columns;
                return result;
            }
            const decoded = try self.owner.readDecoded(&self.reader.?) orelse return null;
            self.page = decoded.page;
            const columns = try a.alloc(rows.ColumnVector, self.columns.len);
            for (self.columns, columns) |name, *column| {
                const ordinal = for (self.owner.columns, 0..) |candidate, index| {
                    if (std.mem.eql(u8, candidate, name)) break index;
                } else return error.RowSourceColumnKindMismatch;
                column.* = decoded.page.columns[ordinal];
            }
            const inventory = self.owner.provider.source.inventory;
            return .{ .snapshot = .{ .table_id = inventory.source_id, .snapshot_id = inventory.snapshot_id }, .row_refs = decoded.refs, .columns = columns };
        }
    };
};

test "external lake bounded replay releases files and falls back for oversized single files" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("bounded-lake-replay");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const values = try ca.alloc([]const u8, 2048);
    for (values, 0..) |*value, index| {
        var input: [8]u8 = undefined;
        std.mem.writeInt(u64, &input, index, .little);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&input, &digest, .{});
        value.* = try ca.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    }
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(ca, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = values }});
    for ([_][]const u8{ "a.parquet", "b.parquet" }) |name| {
        var put = try client.putObject("antfly", name, data, .{});
        put.deinit(a);
    }
    const schema = try std.fmt.allocPrint(ca,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    var external = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer external.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = external }, .{});
    defer source.deinit();
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    var replay = Replay.init(a, &provider, &.{"body"});
    defer replay.deinit();
    replay.max_spill_bytes = 160 * 1024;
    const binding: Binding = .{ .source_kind = .external_parquet, .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .schema_fingerprint = source.inventory.schema_fingerprint, .sidecar_kind = .text, .index_config_hash = "test", .column_bindings = &.{"body"}, .row_ref_kind = .external };
    var total_spill: u64 = 0;
    for (0..2) |file| {
        const rows_source = (try replay.open(a, binding, file, .none)).?;
        defer rows_source.deinit(a);
        var count: usize = 0;
        while (try rows_source.next(a)) |batch| {
            try batch.validate();
            for (0..batch.rowCount()) |index| {
                const ref = batch.rowRef(index);
                try std.testing.expectEqualStrings(source.inventory.files[file].file_id, ref.external.file_id);
                try std.testing.expectEqualStrings(values[count + index], batch.columns[0].values.bytes[index]);
            }
            count += batch.rowCount();
        }
        try std.testing.expectEqual(values.len, count);
        const decoded_before = replay.decoded_loads;
        const reused = (try replay.open(a, binding, file, .none)).?;
        var repeated: usize = 0;
        while (try reused.next(a)) |batch| {
            for (0..batch.rowCount()) |index| try std.testing.expectEqualStrings(values[repeated + index], try batch.columns[0].bytesAt(index));
            repeated += batch.rowCount();
        }
        reused.deinit(a);
        try std.testing.expectEqual(values.len, repeated);
        try std.testing.expectEqual(decoded_before, replay.decoded_loads);
        try std.testing.expect(replay.decoded_hits != 0);
        // Required decoding reclaims optional cached pages before retrying a
        // known quota denial, and restores the exact sealed block boundary.
        try std.testing.expect(replay.evictDecoded());
        const previous_limit = replay.budget.limit;
        replay.budget.limit = replay.budget.live;
        const pressure = (try replay.open(a, binding, file, .none)).?;
        const first_batch = (try pressure.next(a)).?;
        try std.testing.expectEqualStrings(values[0], try first_batch.columns[0].bytesAt(0));
        pressure.deinit(a);
        replay.budget.limit = previous_limit;
        try std.testing.expect(replay.manager.peak_bytes <= replay.max_spill_bytes);
        total_spill += replay.manager.peak_bytes;
    }
    try std.testing.expect(total_spill > replay.max_spill_bytes);
    replay.max_spill_bytes = 1;
    // Select another file to discard the successful cache before admission.
    try std.testing.expect((try replay.open(a, binding, 0, .none)) == null);
    try std.testing.expect(replay.run == null);
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
    replay.max_spill_bytes = 160 * 1024;
    replay.budget.limit = 1;
    try std.testing.expect((try replay.open(a, binding, 0, .none)) == null);
    try std.testing.expectEqual(@as(usize, 0), replay.budget.live);
    try std.testing.expectEqual(@as(usize, 0), replay.manager.files);
    replay.budget.limit = 2 * 1024 * 1024;
    const recovered = (try replay.open(a, binding, 0, .none)).?;
    _ = (try recovered.next(a)).?;
    const held = (try recovered.retainColumns(a)).?;
    recovered.deinit(a);
    try std.testing.expectError(error.LakeReplayBusy, replay.reset());
    try std.testing.expect(held.retained_bytes > 0);
    held.release();
    try replay.reset();
    // An earlier file's optional decoded pages must not consume all of a
    // later file's shared window quota. Only cache-owned leases are reclaimed.
    {
        var window_budget: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
        defer std.debug.assert(window_budget.live == 0);
        var peers = [_]Replay{ Replay.init(a, &provider, &.{"body"}), Replay.init(a, &provider, &.{"body"}) };
        defer for (&peers) |*peer| peer.deinit();
        for (&peers) |*peer| {
            peer.budget.backing = window_budget.allocator();
            peer.cache_peers = &peers;
        }
        const current = (try peers[0].open(a, binding, 0, .none)).?;
        defer current.deinit(a);
        const pa = peers[1].budget.allocator();
        const page = try pa.create(Replay.Page);
        page.* = .{ .a = pa, .arena = .init(pa), .columns = &.{}, .len = 0 };
        _ = try page.arena.allocator().alloc(u8, 1024 * 1024);
        peers[1].decoded[0] = .{ .row = 0, .following = .{ .row = 0, .byte = 0 }, .page = page, .refs = &.{}, .used = 0 };
        peers[1].decoded_bytes = page.retainedBytes();
        window_budget.limit = window_budget.live;
        const batch = (try current.next(a)).?;
        try std.testing.expectEqualStrings(values[0], try batch.columns[0].bytesAt(0));
        try std.testing.expect(peers[1].decoded[0] == null);
    }
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var failed = Replay.init(failing.allocator(), &provider, &.{"body"});
    defer failed.deinit();
    try std.testing.expectError(error.OutOfMemory, failed.open(a, binding, 0, .none));
    try std.testing.expect(!failed.budget.isLocallyExhausted());
}

test "external lake replay sparse union borrows authenticated deltas without opening file catalogs" {
    const state = @import("lake_index_native_state.zig");
    const text = @import("lake_index_native_text.zig");
    const ordered = @import("lake_index_ordered_rows.zig");
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("sparse-replay-union");
    defer directory.cleanup();
    var memory = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer memory.deinit();
    var store = memory.artifactStore();
    store.upload_scope = .{ .domain = @splat(1), .attempt = @splat(2) };
    // Valid references deliberately have no payload in the store. Planning
    // compatible generations must authenticate only the small root records.
    const missing: tree.Ref = .{ .digest = @splat(3), .bytes = 128, .height = 0, .records = 3 };
    const binding: Binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "s", .snapshot_id = "before", .schema_fingerprint = "schema", .column_bindings = &.{"body"}, .index_config_hash = "config" };
    const text_root: text.Root = .{ .domain = @splat(1), .binding = binding, .config_json = "{}", .inventory_version = 3, .inventory_delta_version = 2, .file_count = 3, .directories = .{ .files = missing } };
    var first = try store.putWithCancellation(try std.json.Stringify.valueAlloc(out, text_root, .{}), .none);
    defer first.deinit(a);
    const ordered_root: ordered.Root = .{ .fingerprint = @splat(4), .domain = @splat(1), .page = null, .source = "s", .snapshot = "before", .file_dictionary = .{ .count = 3, .slots = missing, .names = missing, .public = missing } };
    var second = try store.putWithCancellation(try std.json.Stringify.valueAlloc(out, ordered_root, .{}), .none);
    defer second.deinit(a);
    var files = [_]state.File{ .{ .id = "a", .digest = @splat(1) }, .{ .id = "b", .digest = @splat(2) }, .{ .id = "c", .digest = @splat(3) } };
    var flags = [_]bool{ false, true, false };
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.inventory = .{ .format = .parquet, .source_id = @constCast("s"), .source_uri = @constCast("s3://bucket/source"), .snapshot_id = @constCast("after"), .schema_fingerprint = @constCast("schema"), .files = &.{} };
    source.inventory.files = try out.alloc(local.serverless_external_source_types.FileEntry, files.len);
    for (source.inventory.files, files) |*file, prior| file.* = .{ .file_id = @constCast(prior.id), .object_uri = @constCast("s3://bucket/source/file"), .byte_len = 1, .row_count = 0, .row_groups = &.{} };
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .native_delta = .{ .source = "s", .schema = "schema", .prior_snapshot = "before", .prior_count = 3, .files = &files, .changed = &flags, .prior_changed = &.{}, .by_id = .empty } };
    var ordered_binding = binding;
    ordered_binding.sidecar_kind = .ordered_rows;
    const declarations = [_]local.serverless_segment_sidecar_manifest.DeclaredArtifact{
        .{ .name = "text", .binding = binding, .artifact = .{ .kind = .text_segment, .metadata_version = text.metadata_version, .artifact_id = first.artifact_id, .checksum = first.checksum, .byte_len = first.byte_len } },
        .{ .name = "ordered", .binding = ordered_binding, .artifact = .{ .kind = .ordered_row_index, .metadata_version = ordered.metadata_version, .artifact_id = second.artifact_id, .checksum = second.checksum, .byte_len = second.byte_len } },
    };
    var needed = (try changedFiles(a, a, &provider, store, &declarations, &.{}, .none)).?;
    defer needed.deinit();
    try std.testing.expectEqual(@as(usize, 1), needed.files.cardinality());
    try std.testing.expect(needed.files.contains(1));
    // Snapshot mismatch cannot turn a partial predecessor into a delta proof.
    try std.testing.expect(!deltaMatches(&provider, "s", "schema", "older"));
    try std.testing.expect(!deltaMatches(&provider, "other", "schema", "before"));
}

test "external lake replay capture keeps IEEE values and null dictionary identities through disk" {
    const a = std.testing.allocator;
    const floating = [_]f64{ std.math.nan(f64), 0, -0.0, std.math.inf(f64), -std.math.inf(f64) };
    const dictionary = [_]f64{ -0.0, std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64) };
    const vectors = [_]rows.ColumnVector{
        .{ .name = "f", .values = .{ .f64 = &floating }, .nulls = .{ .bytes = &.{ 0, 1, 0, 0, 0 } } },
        .{ .name = "f", .values = .{ .dictionary_f64 = .{ .values = &dictionary, .indices = &.{ 3, 999999, 0, 1, 2 } } }, .nulls = .{ .bytes = &.{ 0, 1, 0, 0, 0 } } },
    };
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    for (vectors) |vector| {
        var capture: Replay.CaptureBatch = .{ .vectors = &.{vector} };
        const input: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &capture, .read = Replay.CaptureBatch.cell, .read_identity = Replay.CaptureBatch.identity, .count = 5, .width = 1 } };
        var marker: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check, .async_writes = false };
        defer manager.deinit();
        var file = try spill.Sequential.init(&manager, 256);
        defer file.close();
        const keys: [5][]const local.sql_scalar.Datum = @splat(&.{});
        try file.appendBatch(input, .{ .rows = &keys }, &.{ 0, 1, 2, 3, 4 });
        var offset: u64 = 0;
        while (offset < 5) {
            const batch = try file.readBatchBorrowed(offset, 2);
            for (batch.rows) |row| {
                const index: usize = @intCast(row.ordinal);
                try std.testing.expectEqual(index == 1, row.values[0].sql_null);
                if (index != 1) try std.testing.expectEqual(@as(u64, @bitCast(floating[index])), @as(u64, @bitCast(row.values[0].value.float)));
            }
            offset = batch.following;
        }
    }
}
