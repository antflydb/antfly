// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Streaming, delete-aware input for native external index construction.
//! A source borrows one authorized snapshot. Filtered runs retain physical
//! dictionaries and vectors instead of materializing the entire table.
const std = @import("std");
const local = @import("antfly_local_sources");
const rows = local.storage_rowsource_types;
const bindings = local.serverless_segment_source_binding;
const serving = local.serverless_query_lake_serving;
const stream_api = local.serverless_query_lake_stream;
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const stores = @import("../serverless/artifacts/store.zig");
pub const Provider = struct {
    ordered_delta_plans: usize = 0,
    identity_spill_bytes: u64 = 1024 * 1024 * 1024,
    identity_working_set_bytes: usize = 64 * 1024 * 1024,
    skip_indexes: []const []const u8 = &.{},

    /// Total work belongs to a configuration, across private file checkpoints.
    resume_work: ?*ResumeWork = null,
    source: *serving.ServingSource,
    /// Borrowed authenticated prior inventory, shared by all native builders.
    previous_inventory: ?local.serverless_external_source_types.Inventory = null,
    native_delta: ?@import("lake_source_navigation.zig").Delta = null,
    enrichment_columns: bool = false,
    vector_memo: ?@import("lake_vector_enrichment.zig").Memo = null,
    progress: ?BuildProgress = null,
    io_phase: ?*const stores.IoPhase = null,
    embedding_options: ?local.inference_managed_embedder.InitOptions = null,
    context: Context,
    limits: stream_api.Limits = .{},
    expected_delete_objects: ?[32]u8 = null,
    only_file: ?usize = null,
    only_files: ?[]const bool = null,
    only_plan: ?*const @import("lake_index_native_state.zig").Plan = null,
    replay: ?*@import("lake_index_build_replay.zig").Replay = null,
    replay_recommended: bool = false,
    replay_consumer: ?usize = null,
    schema_contract: []const local.serverless_query_lake_schema.Column = &.{},
    statistics: ?Statistics = null,
    pub fn wantsIndex(self: *const Provider, name: []const u8) bool {
        for (self.skip_indexes) |skip| if (std.mem.eql(u8, skip, name)) return false;
        return true;
    }
    pub const ResumeWork = struct {
        /// Coordinator-owned append frontier; only the current immutable file
        /// changes. These private roots never serve an incomplete snapshot.
        file: ?usize = null,
        end: ?usize = null,
        positions: ?local.serverless_query_lake_inventory_catalog.Positions = null,
        navigation: ?@import("lake_index_ordered_files.zig").Public = null,
        source_cut: ?[32]u8 = null,
        aggregate: @import("../serverless/build/lake_build_limits.zig").Budget = .{ .limits = .{} },
        native_input_left: u64 = 512 * 1024 * 1024,
        native_keys_left: u64 = 256 * 1024 * 1024,
        native_output_left: u64 = 1024 * 1024 * 1024,
        private: ?struct {
            ref: local.serverless_manifest_artifact_ref.ArtifactRef,
            more: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact = &.{},
            scope: stores.UploadScope,
            store_ptr: *anyopaque,
            store_vtable: *const stores.ArtifactStore.VTable,
        } = null,
        /// A successful producer owns this exact immutable closure in its
        /// still-fenced upload attempt. This hint grants no serving authority.
        pub fn trusts(self: *const ResumeWork, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef) bool {
            const proof = self.private orelse return false;
            const scope = store.upload_scope orelse return false;
            if (store.ptr != proof.store_ptr or store.vtable != proof.store_vtable or !std.meta.eql(scope, proof.scope)) return false;
            if (sameReference(ref, proof.ref)) return true;
            for (proof.more) |declaration| if (sameReference(ref, declaration.artifact)) return true;
            return false;
        }
        fn sameReference(ref: local.serverless_manifest_artifact_ref.ArtifactRef, expected: local.serverless_manifest_artifact_ref.ArtifactRef) bool {
            if (!std.mem.eql(u8, ref.name, expected.name) or !std.mem.eql(u8, ref.artifact_id, expected.artifact_id) or !std.mem.eql(u8, ref.checksum, expected.checksum)) return false;
            var normalized = ref;
            normalized.name = expected.name;
            normalized.artifact_id = expected.artifact_id;
            normalized.checksum = expected.checksum;
            return std.meta.eql(normalized, expected);
        }
    };
    pub const Statistics = struct {
        ptr: *anyopaque,
        columns: []const []const u8,
        files: *const local.serverless_query_lake_inventory_catalog.Positions,
        needed: *const fn (*anyopaque, usize) bool,
        save: *const fn (*anyopaque, usize, []const local.sql_catalog.StatisticsSketch) anyerror!void,
    };
    pub const BuildProgress = struct {
        ptr: *anyopaque,
        save: *const fn (*anyopaque, local.serverless_segment_sidecar_manifest.DeclaredArtifact) anyerror!void,
        save_batch: ?*const fn (*anyopaque, []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) anyerror!void = null,
    };
    pub fn checkpoint(self: *Provider, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        try self.context.ensureActive();
        if (self.progress) |progress| {
            var phase: stores.IoGuard = .{};
            if (self.io_phase) |io| {
                try io.begin(io.ptr, 1);
                phase.phase = io;
            }
            defer phase.abort();
            progress.save(progress.ptr, declaration) catch |err| return phase.failed(err);
            try phase.finish();
        }
    }
    /// Builders that complete a declaration set together checkpoint it once.
    /// Per-file builders keep their individual resumable boundaries.
    pub fn checkpointBatch(self: *Provider, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        try self.context.ensureActive();
        const progress = self.progress orelse return;
        if (progress.save_batch) |save_batch| {
            var phase: stores.IoGuard = .{};
            if (self.io_phase) |io| {
                try io.begin(io.ptr, 1);
                phase.phase = io;
            }
            defer phase.abort();
            save_batch(progress.ptr, declarations) catch |err| return phase.failed(err);
            try phase.finish();
        } else {
            for (declarations) |declaration| try self.checkpoint(declaration);
        }
    }
    pub fn provider(self: *Provider) @import("../serverless/build/lake_rebuild.zig").RowSourceProvider {
        return .{ .ptr = self, .open_fn = open, .open_with_cancellation_fn = openCanceled };
    }
    fn open(raw: *anyopaque, a: A, binding: bindings.Binding) !rows.Source {
        return openCanceled(raw, a, binding, .none);
    }
    fn openCanceled(raw: *anyopaque, a: A, binding: bindings.Binding, cancellation: Cancellation) !rows.Source {
        const self: *Provider = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        try cancellation.check();
        try binding.validate();
        const inventory = self.source.inventory;
        const kind: rows.SourceKind = switch (inventory.format) {
            .parquet => .external_parquet,
            .iceberg => .external_iceberg,
            .lance => return error.UnsupportedExternalLakeIndex,
        };
        if (binding.source_kind != kind or binding.row_ref_kind != .external or
            !std.mem.eql(u8, binding.source_id, inventory.source_id) or
            !std.mem.eql(u8, binding.snapshot_id, inventory.snapshot_id) or
            !std.mem.eql(u8, binding.schema_fingerprint, inventory.schema_fingerprint)) return error.SidecarSourceBindingMismatch;
        if (self.only_files != null or self.only_plan != null) {
            if ((self.only_files != null and self.only_files.?.len != inventory.files.len) or (self.only_plan != null and self.only_plan.?.count() != inventory.files.len) or self.only_file != null) return error.InvalidNativeLakeFileState;
            const cursor = try a.create(FileCursor);
            cursor.* = .{ .provider = self.*, .binding = binding, .cancellation = cancellation, .planned = if (self.only_plan) |plan| plan.iterator(true) else null };
            return .{ .kind = kind, .ctx = cursor, .next_batch = FileCursor.next, .deinit_fn = FileCursor.deinit, .retain_columns = FileCursor.retain };
        }
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(a);
        if (self.enrichment_columns) {
            const schema = if (self.source.iceberg_schema) |selected| selected.columns else self.schema_contract;
            for (schema) |column| try names.append(a, column.name);
            if (names.items.len == 0) return error.InvalidLakeEnrichmentSchema;
        } else try names.appendSlice(a, binding.column_bindings);
        // Replay has already applied deletes. Its output projection contains
        // logical/enrichment fields; equality keys are private scan inputs.
        if (self.replay) |replay| {
            var projected = binding;
            projected.column_bindings = names.items;
            if (try replay.openFor(a, projected, self.only_file, cancellation, self.replay_consumer)) |source| return source;
        }
        if (self.source.scanner.iceberg_delete_plan) |plan| for (plan.files) |file| for (file.equality_columns) |column| {
            const present = for (names.items) |name| {
                if (std.mem.eql(u8, name, column)) break true;
            } else false;
            if (!present) try names.append(a, column);
        };
        const columns = try names.toOwnedSlice(a);
        errdefer a.free(columns);
        const guard = if (self.replay_consumer != null and self.replay != null) self.replay.?.pipeline else null;
        if (guard) |pipeline| try pipeline.source_mutex.lock(self.context.io.?);
        errdefer if (guard) |pipeline| pipeline.source_mutex.unlock(self.context.io.?);
        const state = try a.create(Cursor);
        errdefer a.destroy(state);
        state.* = .{ .stream = try stream_api.Stream.init(a, self.source, columns, &.{}, self.context, self.limits), .source_columns = columns, .expected_delete_objects = self.expected_delete_objects, .scratch = .init(a), .cancellation = cancellation, .fallback_guard = guard };
        errdefer state.stream.deinit();
        if (self.statistics) |statistics| {
            const needed = if (self.only_file) |file| statistics.needed(statistics.ptr, file) else true;
            const covered = for (statistics.columns) |name| {
                const found = for (columns) |column| {
                    if (std.mem.eql(u8, name, column)) break true;
                } else false;
                if (!found) break false;
            } else true;
            if (needed and covered and statistics.columns.len != 0 and statistics.columns.len <= 64) {
                state.statistics = try a.create(StatisticsCapture);
                state.statistics.?.* = .{ .a = a, .callback = statistics, .contract = if (self.source.iceberg_schema) |schema| schema.columns else self.schema_contract };
                if (self.only_file) |file| {
                    state.statistics.?.file = file;
                    state.statistics.?.enabled = true;
                }
            }
        }
        errdefer if (state.statistics) |statistics| a.destroy(statistics);
        if (self.only_file) |index| try state.stream.restrictFile(index);
        state.stream.schema_contract = if (self.source.iceberg_schema) |selected| selected.columns else self.schema_contract;
        return .{ .kind = kind, .ctx = state, .next_batch = Cursor.next, .deinit_fn = Cursor.deinit, .retain_columns = Cursor.retain };
    }
};
/// Open only changed files while retaining one decoded page at a time.
const FileCursor = struct {
    fn retain(raw: *anyopaque, a: A) !?rows.ColumnOwner {
        const self: *FileCursor = @ptrCast(@alignCast(raw));
        return if (self.active) |source| source.retainColumns(a) else null;
    }
    provider: Provider,
    binding: bindings.Binding,
    cancellation: Cancellation,
    file: usize = 0,
    planned: ?@import("lake_index_native_state.zig").Plan.Iterator = null,
    active: ?rows.Source = null,
    fn next(raw: *anyopaque, a: A) !?rows.ColumnBatch {
        const self: *FileCursor = @ptrCast(@alignCast(raw));
        while (true) {
            try self.cancellation.check();
            try self.provider.context.ensureActive();
            if (self.active) |source| {
                if (try source.next(a)) |batch| return batch;
                source.deinit(a);
                self.active = null;
            }
            const ordinal = if (self.planned) |*planned| blk: {
                break :blk ((try planned.next()) orelse return null).ordinal;
            } else blk: {
                const mask = self.provider.only_files.?;
                while (self.file < mask.len and !mask[self.file]) self.file += 1;
                if (self.file == mask.len) return null;
                const index = self.file;
                self.file += 1;
                break :blk index;
            };
            var input = self.provider;
            input.only_files = null;
            input.only_plan = null;
            input.only_file = ordinal;
            self.active = try Provider.openCanceled(&input, a, self.binding, self.cancellation);
        }
    }
    fn deinit(raw: *anyopaque, a: A) void {
        const self: *FileCursor = @ptrCast(@alignCast(raw));
        if (self.active) |source| source.deinit(a);
        a.destroy(self);
    }
};
const StatisticsCapture = struct {
    a: A,
    callback: Provider.Statistics,
    file: ?usize = null,
    enabled: bool = false,
    contract: []const local.serverless_query_lake_schema.Column,
    sketches: [64]local.sql_catalog.StatisticsSketch = @splat(.{}),
    complete: bool = false,
    fn add(self: *@This(), batch: rows.ColumnBatch) !void {
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var begin: usize = 0;
        while (begin < batch.rowCount()) {
            const reference = switch (batch.rowRef(begin)) {
                .external => |ref| ref,
                else => return error.SidecarSourceBindingMismatch,
            };
            const file = self.callback.files.get(reference.file_id) orelse return error.InvalidNativeLakeFileState;
            var end = begin + 1;
            while (end < batch.rowCount()) : (end += 1) {
                const next = switch (batch.rowRef(end)) {
                    .external => |ref| ref,
                    else => return error.SidecarSourceBindingMismatch,
                };
                if (!std.mem.eql(u8, next.file_id, reference.file_id)) break;
            }
            // Stream owns files in contiguous order. A complete file is
            // committed only at the next file boundary or terminal EOF.
            if (self.file == null or self.file.? != file) {
                try self.finish();
                self.file = file;
                self.complete = false;
                self.enabled = self.callback.needed(self.callback.ptr, file);
                @memset(&self.sketches, .{});
            }
            if (self.enabled) for (self.callback.columns, self.sketches[0..self.callback.columns.len]) |name, *sketch| {
                const column = batch.findColumn(name) orelse return error.RowSourceColumnKindMismatch;
                const numeric = for (self.contract) |definition| {
                    if (std.mem.eql(u8, definition.name, name)) break std.mem.eql(u8, definition.kind, "numeric") or std.mem.eql(u8, definition.kind, "number");
                } else false;
                for (begin..end) |lane| {
                    _ = scratch.reset(.retain_capacity);
                    var fixed: local.sql_numeric_value.ScaledInteger = undefined;
                    var number: local.sql_numeric_value.Value = undefined;
                    const value: local.sql_scalar.Datum = if (column.nulls.isNull(lane)) .{} else if (numeric and column.values == .decimal128) decimal: {
                        const decimal = column.values.decimal128;
                        if (decimal.scale < 0 or decimal.scale > 38) return error.InvalidNativeLakeFileState;
                        fixed = try local.sql_numeric_value.ScaledInteger.init(decimal.values[lane], @intCast(decimal.scale));
                        number = fixed.view();
                        break :decimal local.sql_scalar.Datum.typedNumeric(&number);
                    } else local.sql_scalar.Datum.fromJson(try column.values.jsonAt(scratch.allocator(), lane));
                    const width: usize = if (value.numeric != null) 16 else switch (value.value) {
                        .integer, .float => 8,
                        .bool => 1,
                        .string => |text| 16 + text.len,
                        else => 0,
                    };
                    try sketch.add(value, width);
                }
            };
            begin = end;
        }
    }
    fn finish(self: *@This()) !void {
        if (self.complete or self.file == null or !self.enabled) return;
        try self.callback.save(self.callback.ptr, self.file.?, self.sketches[0..self.callback.columns.len]);
        self.complete = true;
    }
};
const Cursor = struct {
    fn retain(raw: *anyopaque, a: A) !?rows.ColumnOwner {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.stream.page_cursor) |*cursor| return cursor.retainColumns(a);
        return null;
    }
    fallback_guard: ?*@import("lake_index_build_pipeline.zig") = null,
    statistics: ?*StatisticsCapture = null,
    stream: stream_api.Stream,
    source_columns: []const []const u8,
    scratch: std.heap.ArenaAllocator,
    cancellation: Cancellation,
    expected_delete_objects: ?[32]u8,
    batch: ?rows.ColumnBatch = null,
    keep: []bool = &.{},
    columns: []rows.ColumnVector = &.{},
    position: usize = 0,
    fn next(raw: *anyopaque, _: A) !?rows.ColumnBatch {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        try self.stream.context.ensureActive();
        while (true) {
            if (self.batch) |batch| {
                while (self.position < self.keep.len and !self.keep[self.position]) self.position += 1;
                if (self.position < self.keep.len) {
                    const begin = self.position;
                    while (self.position < self.keep.len and self.keep[self.position]) self.position += 1;
                    // Descriptors are reusable; their values continue borrowing
                    // the current Parquet page until its final live run drains.
                    const selected = try slice(self.columns, batch, begin, self.position);
                    if (self.statistics) |statistics| try statistics.add(selected);
                    return selected;
                }
            }
            self.batch = null;
            self.keep = &.{};
            self.position = 0;
            self.columns = &.{};
            if (!self.scratch.reset(.retain_capacity)) return error.OutOfMemory;
            const physical = try self.stream.next() orelse {
                if (self.statistics) |statistics| try statistics.finish();
                return null;
            };
            const batch = try nullableColumns(self.scratch.allocator(), physical, self.source_columns, self.stream.schema_contract);
            if (self.expected_delete_objects) |expected| {
                if (self.stream.source.prepared_deletes) |prepared| {
                    if (!std.mem.eql(u8, &expected, &prepared.object_versions)) return error.ExternalLakeIndexSourceChanged;
                } else if (self.stream.source.scanner.iceberg_delete_plan) |plan| {
                    if (plan.files.len != 0) return error.InvalidExternalLakeIndexCoverage;
                }
            }
            const keep = try self.scratch.allocator().alloc(bool, batch.rowCount());
            @memset(keep, true);
            try self.stream.deleteMask(self.scratch.allocator(), batch, keep);
            self.columns = try self.scratch.allocator().alloc(rows.ColumnVector, batch.columns.len);
            self.batch = batch;
            self.keep = keep;
        }
    }
    fn deinit(raw: *anyopaque, a: A) void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.statistics) |statistics| a.destroy(statistics);
        self.stream.deinit();
        if (self.fallback_guard) |pipeline| pipeline.source_mutex.unlock(pipeline.io);
        self.scratch.deinit();
        a.free(self.source_columns);
        a.destroy(self);
    }
};
/// Missing optional fields are typed NULL vectors, including files written before
/// the field existed. All consumers receive the same schema-bound column shape.
fn nullableColumns(a: A, batch: rows.ColumnBatch, names: []const []const u8, contract: []const local.serverless_query_lake_schema.Column) !rows.ColumnBatch {
    var columns: std.ArrayList(rows.ColumnVector) = .empty;
    try columns.appendSlice(a, batch.columns);
    for (names) |name| {
        if (batch.findColumn(name) != null) continue;
        const definition = for (contract) |column| {
            if (std.mem.eql(u8, column.name, name)) break column;
        } else return error.RowSourceMissingColumn;
        if (definition.required) return error.ExternalLakeSchemaMismatch;
        const n = batch.rowCount();
        const nulls = try a.alloc(u8, n);
        @memset(nulls, 1);
        const values: rows.ColumnValues = if (std.mem.eql(u8, definition.kind, "integer") or std.mem.eql(u8, definition.kind, "datetime")) value: {
            const v = try a.alloc(i64, n);
            @memset(v, 0);
            break :value .{ .i64 = v };
        } else if (std.mem.eql(u8, definition.kind, "number")) value: {
            const v = try a.alloc(f64, n);
            @memset(v, 0);
            break :value .{ .f64 = v };
        } else if (std.mem.eql(u8, definition.kind, "boolean")) value: {
            const v = try a.alloc(bool, n);
            @memset(v, false);
            break :value .{ .bool = v };
        } else value: {
            const v = try a.alloc([]const u8, n);
            @memset(v, "");
            break :value .{ .bytes = v };
        };
        try columns.append(a, .{ .name = name, .values = values, .nulls = .{ .bytes = nulls } });
    }
    var result = batch;
    result.columns = columns.items;
    return result;
}

fn slice(columns: []rows.ColumnVector, batch: rows.ColumnBatch, begin: usize, end: usize) !rows.ColumnBatch {
    if (begin > end or end > batch.rowCount()) return error.RowSourceColumnLengthMismatch;
    if (columns.len != batch.columns.len) return error.RowSourceColumnLengthMismatch;
    for (columns, batch.columns) |*column, original| {
        column.* = original;
        column.nulls = original.nulls.slice(begin, end - begin);
        column.values = switch (original.values) {
            .decimal128 => |values| .{ .decimal128 = values.slice(begin, end - begin) },
            .encoded => return error.UnsupportedSqlExecution,
            inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |dictionary, tag| @unionInit(rows.ColumnValues, @tagName(tag), .{ .values = dictionary.values, .indices = dictionary.indices[begin..end] }),
            inline else => |values, tag| @unionInit(rows.ColumnValues, @tagName(tag), values[begin..end]),
        };
    }
    return batch.identitySlice(begin, end, columns);
}

test "external lake native index input preserves dictionary ids and nulls across filtered runs" {
    const a = std.testing.allocator;
    const refs: []const rows.RowRef = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" } };
    const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "source", .snapshot_id = "snapshot" }, .row_refs = refs, .columns = &.{
        .{ .name = "label", .values = .{ .dictionary_bytes = .{ .values = &.{ "first", "second" }, .indices = &.{ 0, 1, 0, 99 } } }, .nulls = .{ .bytes = &.{ 0, 0, 0, 1 } } },
        .{ .name = "exact", .values = .{ .dictionary_i64 = .{ .values = &.{ 9007199254740993, -9007199254740993 }, .indices = &.{ 0, 1, 0, 0 } } } },
        .{ .name = "boolean", .values = .{ .bool = &.{ true, false, true, false } } },
    } };
    const columns = try a.alloc(rows.ColumnVector, batch.columns.len);
    defer a.free(columns);
    const tail = try slice(columns, batch, 2, 4);
    try tail.validate();
    try std.testing.expectEqual(@as(i64, 9007199254740993), try tail.columns[1].integerAt(0));
    try std.testing.expectEqual(@as(?u32, null), try tail.columns[0].dictionaryId(1));
    try std.testing.expect(tail.columns[0].values.dictionary_bytes.values.ptr == batch.columns[0].values.dictionary_bytes.values.ptr);
    try std.testing.expectEqualStrings("first", try tail.columns[0].bytesAt(0));
    try std.testing.expect(tail.columns[2].values.bool[0]);
    try std.testing.expect(!tail.columns[2].values.bool[1]);
}

test "external lake native index input opens one pinned real Parquet scan" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-index-source");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 9007199254740993, -9007199254740993, 3 }, .field_id = 1 }});
    defer a.free(bytes);
    var put = try client.putObject("antfly", "part.parquet", bytes, .{});
    put.deinit(a);
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{directory.path()});
    defer a.free(uri);
    const schema_json = try std.fmt.allocPrint(a, "{{\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"{s}\",\"schema_fingerprint\":\"schema\"}}}}", .{uri});
    defer a.free(schema_json);
    var owned_binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer owned_binding.deinit(a);
    var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = owned_binding }, .{});
    defer source.deinit();
    const coverage = try @import("lake_index_coverage.zig").pin(&source, .{});
    var provider_owner: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .expected_delete_objects = coverage.delete_objects };
    const binding: bindings.Binding = .{ .sidecar_kind = .algebraic, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .schema_fingerprint = source.inventory.schema_fingerprint, .index_config_hash = "config", .column_bindings = &.{"amount"} };
    var row_source = try provider_owner.provider().open(a, binding);
    defer row_source.deinit(a);
    var total: i64 = 0;
    var count: usize = 0;
    while (try row_source.next(a)) |batch| {
        try bindings.validateBatchAgainstBinding(binding, batch);
        for (0..batch.rowCount()) |index| {
            total += try batch.columns[0].integerAt(index);
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(i64, 3), total);
    {
        // Native pages can outlive their cursor without copying scalar lanes.
        var retained_source = try provider_owner.provider().open(a, binding);
        var opened = true;
        defer if (opened) retained_source.deinit(a);
        const borrowed = (try retained_source.next(a)).?;
        const owner = (try retained_source.retainColumns(a)).?;
        defer owner.release();
        var view_arena = std.heap.ArenaAllocator.init(a);
        defer view_arena.deinit();
        const stable = try @import("lake_index_owned_batch.zig").descriptors(view_arena.allocator(), borrowed);
        try std.testing.expect(stable.columns[0].values.i64.ptr == borrowed.columns[0].values.i64.ptr);
        try std.testing.expect((try retained_source.next(a)) == null);
        retained_source.deinit(a);
        opened = false;
        try stable.validate();
        try std.testing.expectEqual(@as(i64, 9007199254740993), try stable.columns[0].integerAt(0));
        try std.testing.expectEqual(@as(i64, -9007199254740993), try stable.columns[0].integerAt(1));
    }
    {
        var replay = @import("lake_index_build_replay.zig").Replay.init(a, &provider_owner, &.{"amount"});
        defer replay.deinit();
        provider_owner.replay = &replay;
        defer provider_owner.replay = null;
        var first = try provider_owner.provider().open(a, binding);
        defer first.deinit(a);
        var second_provider = provider_owner;
        second_provider.only_file = 0;
        var second = try second_provider.provider().open(a, binding);
        defer second.deinit(a);
        const x = (try first.next(a)).?;
        const y = (try second.next(a)).?;
        try std.testing.expectEqual(@as(i64, 9007199254740993), try x.columns[0].integerAt(0));
        try std.testing.expectEqual(@as(i64, -9007199254740993), try y.columns[0].integerAt(1));
        try std.testing.expectEqual(@as(i64, 9007199254740993), try x.columns[0].integerAt(0));
        try std.testing.expectEqual(@as(usize, 3), x.rowCount());
        try std.testing.expectEqual(null, try first.next(a));
        try std.testing.expectEqual(null, try second.next(a));
        // Routing regression: equality-delete inputs are internal to capture,
        // not replay's logical projection. A cached run must stay usable when
        // the raw scanner needs an auxiliary key absent from output columns.
        const DeleteFile = @typeInfo(@FieldType(@typeInfo(@FieldType(@TypeOf(source.scanner), "iceberg_delete_plan")).optional.child, "files")).pointer.child;
        const auxiliary: []u8 = @constCast("unprojected_delete_key");
        var delete_files = [_]DeleteFile{.{ .content = .equality_deletes, .file_path = @constCast("delete.parquet"), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 1, .file_sequence_number = 1, .record_count = 1, .file_size_in_bytes = 1, .equality_columns = @constCast((&[_][]u8{auxiliary})[0..]) }};
        source.scanner.iceberg_delete_plan = .{ .files = &delete_files };
        defer source.scanner.iceberg_delete_plan = null;
        const calls = replay.captures;
        var third = try second_provider.provider().open(a, binding);
        defer third.deinit(a);
        const z = (try third.next(a)).?;
        try std.testing.expectEqual(@as(usize, 1), z.columns.len);
        try std.testing.expectEqual(@as(usize, 3), z.rowCount());
        try std.testing.expectEqual(calls, replay.captures);
    }
    const verified = try @import("lake_index_coverage.zig").pin(&source, .{});
    try std.testing.expectEqual(coverage, verified);
    const changed_bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 9007199254740993, -9007199254740993, 4 }, .field_id = 1 }});
    defer a.free(changed_bytes);
    try std.testing.expectEqual(bytes.len, changed_bytes.len);
    var replacement = try client.putObject("antfly", "part.parquet", changed_bytes, .{});
    replacement.deinit(a);
    // A same-size replacement cannot inherit the pinned coverage proof.
    try std.testing.expectError(error.ExternalLakeIndexSourceChanged, @import("lake_index_coverage.zig").pin(&source, .{}));
    var changed = binding;
    changed.snapshot_id = "stale";
    try std.testing.expectError(error.SidecarSourceBindingMismatch, provider_owner.provider().open(a, changed));
}

test "external lake native input synthesizes nullable columns with their declared types" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "lake", .snapshot_id = "snapshot" }, .row_refs = &.{.{ .relational_key = "a" }}, .columns = &.{.{ .name = "present", .values = .{ .i64 = &.{7} } }} };
    const contract: []const local.serverless_query_lake_schema.Column = &.{
        .{ .name = "text", .kind = "string", .required = false },
        .{ .name = "n", .kind = "integer", .required = false },
        .{ .name = "f", .kind = "number", .required = false },
        .{ .name = "b", .kind = "boolean", .required = false },
        .{ .name = "required", .kind = "integer", .required = true },
    };
    const result = try nullableColumns(arena.allocator(), batch, &.{ "present", "text", "n", "f", "b" }, contract);
    try result.validate();
    try std.testing.expectEqual(@as(usize, 5), result.columns.len);
    for (result.columns[1..]) |column| try std.testing.expect(column.nulls.isNull(0));
    try std.testing.expectEqual(rows.ColumnKind.i64, result.findColumn("n").?.kind());
    try std.testing.expectEqual(rows.ColumnKind.f64, result.findColumn("f").?.kind());
    try std.testing.expectEqual(rows.ColumnKind.bool, result.findColumn("b").?.kind());
    try std.testing.expectError(error.ExternalLakeSchemaMismatch, nullableColumns(arena.allocator(), batch, &.{"required"}, contract));
    try std.testing.expectError(error.RowSourceMissingColumn, nullableColumns(arena.allocator(), batch, &.{"unknown"}, contract));
}
