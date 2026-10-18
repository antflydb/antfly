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

//! One projected, delete-aware scan feeds independent bounded native builders.
//! Replay retains at most one file. Whole-source consumers stream ordinarily;
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
    var consumers: usize = 0;
    for (desired.artifacts) |artifact| {
        consumers += 1;
        if (artifact.kind == .text_segment) {
            const config = try std.json.parseFromSliceLeaky(std.json.Value, ca, artifact.build_spec.?.text.config_json, .{});
            const field = config.object.get("field");
            if (field == null or field.? != .string) {
                for (schema.relational_columns) |column| try names.put(a, column.name, {});
                continue;
            }
        }
        for (artifact.binding.column_bindings) |column| try names.put(a, column, {});
    }
    if (try parsed.relationalIndexDefinitions(ca)) |indexes| for (indexes) |index| {
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
    for (contract, schema.relational_columns) |*column, definition| column.* = .{ .name = try out.dupe(u8, definition.path), .kind = @tagName(definition.column_type), .required = definition.required and !definition.allows_null };
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
/// Union changed native files so delta builders share only the necessary input.
/// Unknown/legacy producer formats conservatively request a complete replay.
pub fn changedFiles(a: A, out: A, provider: *Provider, store: @import("../serverless/artifacts/store.zig").ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const local.metadata_lake_index_catalog.FileContribution, cancellation: Cancellation) !?[]const bool {
    return changedFilesIndexed(a, out, provider, store, declarations, contributions, cancellation, null);
}
pub fn changedFilesIndexed(a: A, out: A, provider: *Provider, store: @import("../serverless/artifacts/store.zig").ArtifactStore, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, contributions: []const local.metadata_lake_index_catalog.FileContribution, cancellation: Cancellation, index: ?*@import("lake_index_contributions.zig").Index) !?[]const bool {
    const state = @import("lake_index_native_state.zig");
    const needed = try out.alloc(bool, provider.source.inventory.files.len);
    @memset(needed, false);
    const aggregate = @import("lake_index_native_aggregates.zig");
    var old: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer old.deinit(a);
    for (contributions) |contribution| try old.put(a, aggregate.contributionKey(contribution.file, contribution.recipe, contribution.name), {});
    const needs_contribution_keys = for (declarations) |declaration| {
        if (declaration.artifact.kind == .algebraic_segment) break true;
    } else false;
    const file_keys = try a.alloc([32]u8, if (needs_contribution_keys) provider.source.inventory.files.len else 0);
    defer a.free(file_keys);
    for (provider.source.inventory.files[0..file_keys.len], file_keys) |file, *key| key.* = aggregate.fileIdentity(provider.source, file);
    for (declarations) |declaration| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ca = arena.allocator();
        if (declaration.artifact.kind == .algebraic_segment) {
            if (!@import("lake_index_aggregate_artifact.zig").supportsMetadataVersion(declaration.artifact.metadata_version)) return null;
            const recipe = try @import("lake_index_aggregate_artifact.zig").loadRecipe(ca, store, declaration.artifact, cancellation);
            if (!aggregate.incrementalRecipe(recipe) or provider.source.inventory.deleted_row_groups.len != 0 or (if (provider.source.scanner.iceberg_delete_plan) |plan| plan.files.len != 0 else false)) return null;
            const fingerprint = recipe.fingerprint();
            for (file_keys, needed) |key, *required| {
                const lookup_key = aggregate.contributionKey(key, fingerprint, declaration.name);
                if (!required.* and !old.contains(lookup_key)) {
                    required.* = if (index) |lookup| missing: {
                        if (lookup.has_file_coverage) break :missing !lookup.covered_files.contains(key);
                        var scratch = std.heap.ArenaAllocator.init(a);
                        defer scratch.deinit();
                        break :missing (try lookup.lookup(scratch.allocator(), lookup_key)) == null;
                    } else true;
                }
            }
            continue;
        }
        const previous: []const state.File = switch (declaration.artifact.kind) {
            .text_segment => text: {
                const native = @import("lake_index_native_text.zig");
                if (declaration.artifact.metadata_version != native.metadata_version) return null;
                const directory = try native.loadRootDirectory(ca, store, declaration.artifact, cancellation, null);
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
                    for (needed, 0..) |*file, ordinal| file.* = file.* or provider.native_delta.?.isChanged(ordinal);
                    continue;
                }
                break :sparse if (root.file_directory) |directory| try state.directoryFiles(ca, store, directory, root.generation.domain, cancellation) else root.file_states;
            },
            .vector_segment => dense: {
                const native = @import("lake_index_native_dense.zig");
                if (!native.supportsMetadata(declaration.artifact.metadata_version)) return null;
                const root = try native.loadRoot(ca, store, declaration.artifact, cancellation, null);
                if (root.file_directory != null and state.canUseDelta(provider, root.binding.snapshot_id)) {
                    for (needed, 0..) |*file, ordinal| file.* = file.* or provider.native_delta.?.isChanged(ordinal);
                    continue;
                }
                break :dense if (root.file_directory) |directory| try state.directoryFiles(ca, store, directory, root.generation.domain, cancellation) else root.file_states;
            },
            .ordered_row_index => ordered: {
                const native = @import("lake_index_ordered_rows.zig");
                if (!native.supportsBuildMetadata(declaration.artifact.metadata_version)) return null;
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
        for (needed, 0..) |*file, ordinal| file.* = file.* or plan.isChanged(ordinal);
    }
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
    captured_file: ?usize = null,
    readers: usize = 0,
    mutex: std.Io.Mutex = .init,
    max_spill_bytes: u64 = 1024 * 1024 * 1024,
    only_files: ?[]const bool = null,
    const Range = struct { begin: spill.Sequential.Position, end: u64 };
    pub fn init(a: A, provider: *Provider, columns: []const []const u8) Replay {
        return .{ .provider = provider, .columns = columns, .arena = .init(a), .budget = .{ .backing = a, .limit = 256 * 1024 * 1024 } };
    }
    pub fn deinit(self: *Replay) void {
        if (self.run) |*run| {
            run.close();
            self.manager.deinit();
        }
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
    }
    fn check(raw: *anyopaque) !void {
        const self: *Replay = @ptrCast(@alignCast(raw));
        try self.provider.context.ensureActive();
    }
    fn capture(self: *Replay, a: A, original: Binding, file_index: usize, cancellation: Cancellation) !void {
        if (self.ready and self.captured_file == file_index) return;
        if (self.readers != 0) return error.LakeReplayBusy;
        if (self.run) |*run| {
            run.close();
            self.manager.deinit();
            self.run = null;
        }
        _ = self.arena.reset(.free_all);
        self.ready = false;
        self.captured_file = file_index;
        const io = self.provider.context.io orelse return error.UnsupportedSqlExecution;
        if (self.run != null) return error.InvalidSqlSpill;
        self.manager = .{ .alloc = self.budget.allocator(), .io = io, .context = self, .checkpoint = check, .max_bytes = self.max_spill_bytes };
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
            const values: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &capture_batch, .read = CaptureBatch.cell, .read_identity = CaptureBatch.identity, .count = batch.rowCount(), .width = self.columns.len } };
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
    }
    const CaptureBatch = struct {
        vectors: []const rows.ColumnVector,
        fn identity(raw: *anyopaque, row: usize, ordinal: usize) !?u64 {
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
        fn cell(raw: *anyopaque, a: A, row: usize, ordinal: usize) !local.sql_scalar.Datum {
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
    pub fn open(self: *Replay, a: A, binding: Binding, only_file: ?usize, cancellation: Cancellation) !?rows.Source {
        for (binding.column_bindings) |name| {
            const present = for (self.columns) |column| {
                if (std.mem.eql(u8, name, column)) break true;
            } else false;
            if (!present) return null;
        }
        const file_index = only_file orelse return null;
        const io = self.provider.context.io orelse return null;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.readers != 0 and self.captured_file != file_index) return null;
        if (self.only_files) |needed| {
            if (needed.len != self.provider.source.inventory.files.len or file_index >= needed.len) return error.SidecarSourceBindingMismatch;
            if (!needed[file_index]) return null;
        }
        // Oversized individual files retain the ordinary streaming path. The
        // replay optimization must never make an otherwise valid scan fail.
        self.capture(a, binding, file_index, cancellation) catch |err| switch (err) {
            error.SqlProgramLimitExceeded => {
                if (self.run) |*run| {
                    run.close();
                    self.manager.deinit();
                    self.run = null;
                }
                _ = self.arena.reset(.free_all);
                self.ready = false;
                return null;
            },
            else => return err,
        };
        const range: Range = if (only_file) |file| blk: {
            if (file != self.captured_file) return error.SidecarSourceBindingMismatch;
            break :blk self.range;
        } else .{ .begin = .{ .row = 0, .byte = 0 }, .end = self.run.?.size };
        const cursor = try a.create(Cursor);
        errdefer a.destroy(cursor);
        cursor.* = .{ .owner = self, .reader = try self.run.?.readerSealed(a, range.begin, range.end), .arena = .init(a), .columns = binding.column_bindings, .cancellation = cancellation };
        self.readers += 1;
        return .{ .kind = binding.source_kind, .ctx = cursor, .next_batch = Cursor.next, .deinit_fn = Cursor.deinit };
    }
    const Cursor = struct {
        block: ?*spill.Sequential.OwnedBlock = null,
        owner: *Replay,
        reader: spill.Sequential.Reader,
        arena: std.heap.ArenaAllocator,
        columns: []const []const u8,
        cancellation: Cancellation,
        fn deinit(raw: *anyopaque, a: A) void {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            if (self.block) |block| block.release();
            self.reader.deinit();
            const io = self.owner.provider.context.io.?;
            self.owner.mutex.lockUncancelable(io);
            self.owner.readers -= 1;
            self.owner.mutex.unlock(io);
            self.arena.deinit();
            a.destroy(self);
        }
        fn next(raw: *anyopaque, _: A) !?rows.ColumnBatch {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            try self.cancellation.check();
            try self.owner.provider.context.ensureActive();
            if (self.block) |block| block.release();
            self.block = null;
            _ = self.arena.reset(.retain_capacity);
            const a = self.arena.allocator();
            const block = try self.reader.nextOwned() orelse return null;
            self.block = block;
            const refs = try a.alloc(rows.RowRef, block.count());
            const inventory = self.owner.provider.source.inventory;
            for (refs, 0..) |*ref, index| {
                if (block.keyWidth() != 3) return error.InvalidSqlSpill;
                const file = std.math.cast(usize, (try block.keyCell(index, 0)).value.integer) orelse return error.InvalidSqlSpill;
                if (file >= inventory.files.len) return error.InvalidSqlSpill;
                ref.* = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .identity_version = inventory.row_identity_version, .file_id = inventory.files[file].file_id, .row_group_ordinal = std.math.cast(u32, (try block.keyCell(index, 1)).value.integer) orelse return error.InvalidSqlSpill, .row_ordinal = @bitCast((try block.keyCell(index, 2)).value.integer) } };
            }
            const columns = try a.alloc(rows.ColumnVector, self.columns.len);
            for (self.columns, columns) |name, *column| {
                const ordinal = for (self.owner.columns, 0..) |candidate, index| {
                    if (std.mem.eql(u8, candidate, name)) break index;
                } else return error.RowSourceColumnKindMismatch;
                column.* = try block.columnVector(a, ordinal, self.owner.kinds[ordinal]);
                column.name = name;
            }
            return .{ .snapshot = .{ .table_id = inventory.source_id, .snapshot_id = inventory.snapshot_id }, .row_refs = refs, .columns = columns };
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
        try std.testing.expect(replay.manager.peak_bytes <= replay.max_spill_bytes);
        total_spill += replay.manager.peak_bytes;
    }
    try std.testing.expect(total_spill > replay.max_spill_bytes);
    replay.max_spill_bytes = 1;
    // Select another file to discard the successful cache before admission.
    try std.testing.expect((try replay.open(a, binding, 0, .none)) == null);
    try std.testing.expect(replay.run == null);
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
}
