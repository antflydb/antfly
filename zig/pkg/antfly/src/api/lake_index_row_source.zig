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
pub const Provider = struct {
    source: *serving.ServingSource,
    context: Context,
    limits: stream_api.Limits = .{},
    expected_delete_objects: ?[32]u8 = null,
    only_file: ?usize = null,
    only_files: ?[]const bool = null,
    replay: ?*@import("lake_index_build_replay.zig").Replay = null,
    schema_contract: []const local.serverless_query_lake_schema.Column = &.{},
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
        if (self.only_files) |selected| {
            if (selected.len != inventory.files.len or self.only_file != null) return error.InvalidNativeLakeFileState;
            const cursor = try a.create(FileCursor);
            cursor.* = .{ .provider = self.*, .binding = binding, .cancellation = cancellation };
            return .{ .kind = kind, .ctx = cursor, .next_batch = FileCursor.next, .deinit_fn = FileCursor.deinit };
        }
        if (self.replay) |replay| if (try replay.open(a, binding, self.only_file, cancellation)) |source| return source;
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(a);
        try names.appendSlice(a, binding.column_bindings);
        if (self.source.scanner.iceberg_delete_plan) |plan| for (plan.files) |file| for (file.equality_columns) |column| {
            const present = for (names.items) |name| {
                if (std.mem.eql(u8, name, column)) break true;
            } else false;
            if (!present) try names.append(a, column);
        };
        const columns = try names.toOwnedSlice(a);
        errdefer a.free(columns);
        const state = try a.create(Cursor);
        errdefer a.destroy(state);
        state.* = .{ .stream = try stream_api.Stream.init(a, self.source, columns, &.{}, self.context, self.limits), .source_columns = columns, .expected_delete_objects = self.expected_delete_objects, .scratch = .init(a), .cancellation = cancellation };
        errdefer state.stream.deinit();
        if (self.only_file) |index| try state.stream.restrictFile(index);
        state.stream.schema_contract = if (self.source.iceberg_schema) |selected| selected.columns else self.schema_contract;
        return .{ .kind = kind, .ctx = state, .next_batch = Cursor.next, .deinit_fn = Cursor.deinit };
    }
};
/// Open only changed files while retaining one decoded page at a time.
const FileCursor = struct {
    provider: Provider,
    binding: bindings.Binding,
    cancellation: Cancellation,
    file: usize = 0,
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
            while (self.file < self.provider.only_files.?.len and !self.provider.only_files.?[self.file]) self.file += 1;
            if (self.file == self.provider.only_files.?.len) return null;
            var input = self.provider;
            input.only_files = null;
            input.only_file = self.file;
            self.file += 1;
            self.active = try Provider.openCanceled(&input, a, self.binding, self.cancellation);
        }
    }
    fn deinit(raw: *anyopaque, a: A) void {
        const self: *FileCursor = @ptrCast(@alignCast(raw));
        if (self.active) |source| source.deinit(a);
        a.destroy(self);
    }
};
const Cursor = struct {
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
                    return try slice(self.columns, batch, begin, self.position);
                }
            }
            self.batch = null;
            self.keep = &.{};
            self.position = 0;
            self.columns = &.{};
            if (!self.scratch.reset(.retain_capacity)) return error.OutOfMemory;
            const physical = try self.stream.next() orelse return null;
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
        self.stream.deinit();
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
        if (original.nulls.bytes.len != 0) column.nulls.bytes = original.nulls.bytes[begin..end];
        column.values = switch (original.values) {
            inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |dictionary, tag| @unionInit(rows.ColumnValues, @tagName(tag), .{ .values = dictionary.values, .indices = dictionary.indices[begin..end] }),
            inline else => |values, tag| @unionInit(rows.ColumnValues, @tagName(tag), values[begin..end]),
        };
    }
    return .{ .snapshot = batch.snapshot, .row_refs = batch.row_refs[begin..end], .columns = columns };
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
