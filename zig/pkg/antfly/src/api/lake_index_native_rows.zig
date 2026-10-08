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

//! Cohort construction of remote native ordered indexes. Bounded native spill
//! runs share one delete-aware physical scan and the common worker scheduler.
const std = @import("std");
const local = @import("antfly_local_sources");
const ordered = @import("lake_index_ordered_rows.zig");
const stores = @import("../serverless/artifacts/store.zig");
const state = @import("lake_index_native_state.zig");
const tuples = local.storage_db_relational_index_keys;
const predicates = local.storage_db_relational_index_predicate;
const spill = local.sql_spill;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub fn logicalName(a: A, name: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
    return std.fmt.allocPrint(a, "sql-rows:{s}", .{std.fmt.bytesToHex(&digest, .lower)});
}
pub fn fingerprint(tuple: tuples.TuplePlan, predicate: ?predicates.Plan, cover: []const []const u8) [32]u8 {
    var result = tuple.fingerprint;
    if (predicate) |plan| plan.bindFingerprint(&result);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.native-row-index.cover.v2");
    hash.update(&result);
    for (cover) |column| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, column.len, .little);
        hash.update(&length);
        hash.update(column);
    }
    hash.final(&result);
    return result;
}
const Entry = struct {
    tuple: tuples.TuplePlan,
    predicate: ?predicates.Plan,
    binding: local.serverless_segment_source_binding.Binding,
    name: []const u8,
    cover: []const []const u8,
    sort: spill.Sort,
    changed: []const bool,
    slots: []const u32,
    delta: ordered.Delta,
    fn deinit(self: *Entry) void {
        self.sort.deinit();
        if (self.predicate) |*predicate| predicate.deinit();
        self.tuple.deinit();
    }
};
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared) ![]const Declared {
    return buildIncremental(a, out, table, source, store, provider, cancellation, reusable, &.{});
}
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared, candidates: []const Declared) ![]const Declared {
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer parsed.deinit(a);
    const declarations = parsed.relational_indexes orelse return &.{};
    if (declarations.value.len == 0) return &.{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const definitions = (try parsed.relationalIndexDefinitions(ca)).?;
    const schema = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, schema);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, schema);
    defer layout.deinit();
    const io = provider.context.io orelse return error.UnsupportedSqlExecution;
    var manager: spill.Manager = .{ .alloc = a, .io = io, .context = provider, .checkpoint = check };
    defer manager.deinit();
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(io, ca, table.schema_json, table.table_id, table.name);
    const contract = try ca.alloc(local.serverless_query_lake_schema.Column, sql_table.columns.len);
    for (contract, sql_table.columns) |*column, definition| column.* = .{ .name = definition.path, .kind = @tagName(definition.type), .required = !definition.nullable };
    var input = provider.*;
    input.schema_contract = if (source.iceberg_schema) |selected| selected.columns else contract;
    var files: std.StringHashMapUnmanaged(u32) = .empty;
    defer files.deinit(a);
    for (source.inventory.files, 0..) |file, index| try files.put(a, file.file_id, std.math.cast(u32, index) orelse return error.LakeSidecarBuildLimitExceeded);
    var result: std.ArrayList(Declared) = .empty;
    errdefer result.deinit(out);
    const orders = [_]local.sql_operators.Order{.{}};
    var first: usize = 0;
    while (first < definitions.len) {
        // Reserve four retained runs per fused sort plus pending/merge files
        // within the shared 64-file manager, without enlarging its budget.
        const end = @min(first + 8, definitions.len);
        defer first = end;
        var entries: std.ArrayList(Entry) = .empty;
        defer {
            for (entries.items) |*entry| entry.deinit();
            entries.deinit(a);
        }
        var columns: std.StringHashMapUnmanaged(void) = .empty;
        defer columns.deinit(a);
        for (definitions[first..end]) |definition| {
            var tuple = try tuples.TuplePlan.init(a, schema, &layout, definition.keys);
            errdefer tuple.deinit();
            var predicate: ?predicates.Plan = if (definition.where.len != 0) try predicates.Plan.init(a, schema, &layout, definition.where) else null;
            errdefer if (predicate) |*plan| plan.deinit();
            const paths = try tuple.columnBindings(ca);
            const cover = try coverColumns(ca, definition);
            for (cover) |column| try columns.put(a, column, {});
            for (paths) |path| try columns.put(a, path, {});
            if (predicate) |plan| for (plan.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |path| try columns.put(a, path, {});
            const name = try logicalName(ca, definition.name);
            const hash = try std.fmt.allocPrint(ca, "native-ordered-rows-v5:{s}", .{std.fmt.bytesToHex(&fingerprint(tuple, predicate, cover), .lower)});
            const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .ordered_rows, .source_kind = switch (source.inventory.format) {
                .parquet => .external_parquet,
                .iceberg => .external_iceberg,
                .lance => return error.UnsupportedExternalLakeIndex,
            }, .row_ref_kind = .external, .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .schema_fingerprint = source.inventory.schema_fingerprint, .index_config_hash = hash, .column_bindings = paths };
            const previous = for (reusable) |decl| {
                if (decl.artifact.kind == .ordered_row_index and decl.artifact.metadata_version == ordered.metadata_version and std.mem.eql(u8, decl.name, name) and std.mem.eql(u8, decl.binding.index_config_hash, hash)) break decl;
            } else null;
            if (previous) |decl| {
                try result.append(out, decl);
                if (predicate) |*plan| plan.deinit();
                tuple.deinit();
                continue;
            }
            const prior: ?ordered.Root = for (candidates) |decl| {
                if (decl.artifact.kind != .ordered_row_index or decl.artifact.metadata_version != ordered.metadata_version or !std.mem.eql(u8, decl.name, name)) continue;
                var previous_binding = decl.binding;
                previous_binding.snapshot_id = binding.snapshot_id;
                if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(binding, previous_binding)) continue;
                const root = try ordered.loadRoot(ca, store.*, decl.artifact, cancellation, null);
                if (!std.mem.eql(u8, root.source, binding.source_id) or !std.mem.eql(u8, root.snapshot, decl.binding.snapshot_id) or root.file_fingerprints.len != root.files.len or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain) or !std.mem.eql(u8, &root.fingerprint, &fingerprint(tuple, predicate, cover))) continue;
                break root;
            } else null;
            const changed = try ca.alloc(bool, source.inventory.files.len);
            const slots = try ca.alloc(u32, source.inventory.files.len);
            var slot_names: std.ArrayList([]const u8) = .empty;
            var slot_hashes: std.ArrayList([32]u8) = .empty;
            var slot_map: std.StringHashMapUnmanaged(u32) = .empty;
            defer slot_map.deinit(a);
            const keep = try ca.alloc(bool, if (prior) |root| root.files.len else 0);
            @memset(keep, false);
            if (prior) |root| {
                try slot_names.appendSlice(ca, root.files);
                try slot_hashes.appendSlice(ca, root.file_fingerprints);
                for (root.files, 0..) |file, slot| try slot_map.put(a, file, @intCast(slot));
            }
            var vacant: std.ArrayList(u32) = .empty;
            if (prior) |root| for (root.files, 0..) |file, slot| {
                if (!files.contains(file)) try vacant.append(ca, @intCast(slot));
            };
            for (source.inventory.files, changed, slots) |file, *replace, *slot| {
                const digest = try state.identity(a, &input, file);
                const found = try slot_map.getOrPut(a, file.file_id);
                if (!found.found_existing) {
                    found.value_ptr.* = try claimSlot(ca, &slot_names, &slot_hashes, &vacant, file.file_id, digest);
                    replace.* = true;
                } else {
                    slot.* = found.value_ptr.*;
                    replace.* = !std.mem.eql(u8, &slot_hashes.items[slot.*], &digest);
                    if (!replace.*) keep[slot.*] = true;
                    slot_hashes.items[slot.*] = digest;
                }
                slot.* = found.value_ptr.*;
            }
            for (slot_names.items, slot_hashes.items) |file, *digest| if (!files.contains(file)) {
                digest.* = @splat(0);
            };
            try entries.append(a, .{ .tuple = tuple, .predicate = predicate, .binding = binding, .name = name, .cover = cover, .sort = spill.Sort.init(a, &manager, &orders, 512 * 1024), .changed = changed, .slots = slots, .delta = .{ .previous = prior, .keep = keep, .files = slot_names.items, .fingerprints = slot_hashes.items } });
            entries.items[entries.items.len - 1].sort.run_limit = 4;
        }
        if (entries.items.len == 0) continue;
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(a);
        var keys = columns.keyIterator();
        while (keys.next()) |path| try names.append(a, path.*);
        var binding = entries.items[0].binding;
        binding.column_bindings = names.items;
        const needed = try ca.alloc(bool, source.inventory.files.len);
        @memset(needed, false);
        for (entries.items) |entry| for (needed, entry.changed) |*need, changed| {
            need.* = need.* or changed;
        };
        input.only_files = needed;
        const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, cancellation);
        defer rows.deinit(a);
        var key: std.ArrayList(u8) = .empty;
        defer key.deinit(a);
        var condition_key: std.ArrayList(u8) = .empty;
        defer condition_key.deinit(a);
        var row_scratch = std.heap.ArenaAllocator.init(a);
        defer row_scratch.deinit();
        var ordinal: u64 = 0;
        while (try rows.next(a)) |batch| {
            try cancellation.check();
            for (entries.items) |*entry| {
                var bound = try entry.tuple.bindBatch(a, batch);
                defer bound.deinit();
                var condition_batches: std.ArrayList(tuples.BatchKeys) = .empty;
                defer {
                    for (condition_batches.items) |*condition| condition.deinit();
                    condition_batches.deinit(a);
                }
                if (entry.predicate) |plan| for (plan.conditions) |condition| {
                    var selected = try condition.tuple.bindBatch(a, batch);
                    errdefer selected.deinit();
                    try condition_batches.append(a, selected);
                };
                for (batch.row_refs, 0..) |ref, row| {
                    if (row % 128 == 0) {
                        try cancellation.check();
                        try provider.context.ensureActive();
                    }
                    var live = true;
                    if (entry.predicate) |plan| for (plan.conditions, condition_batches.items) |condition, *selected| {
                        condition_key.clearRetainingCapacity();
                        const is_null = try selected.append(a, &condition_key, row);
                        if (!condition.evaluateEncoded(condition_key.items, is_null).matches()) {
                            live = false;
                            break;
                        }
                    };
                    if (!live) continue;
                    if (ref != .external) return error.InvalidNativeLakeRowIndex;
                    const file = files.get(ref.external.file_id) orelse return error.InvalidNativeLakeRowIndex;
                    if (!entry.changed[file]) continue;
                    key.clearRetainingCapacity();
                    _ = try bound.append(a, &key, row);
                    try key.appendSlice(a, &ordered.coordinate(entry.slots[file], ref.external));
                    _ = row_scratch.reset(.retain_capacity);
                    const values = try a.alloc(local.sql_scalar.Datum, entry.cover.len);
                    defer a.free(values);
                    const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{row} };
                    for (values, entry.cover) |*value, column| {
                        const cell = try page.cell(row_scratch.allocator(), 0, column);
                        value.* = .{ .value = cell.value, .sql_null = cell.sql_null };
                    }
                    try entry.sort.add(.{ .values = values, .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key.items })}, .ordinal = ordinal });
                    ordinal = std.math.add(u64, ordinal, 1) catch return error.LakeSidecarBuildLimitExceeded;
                }
            }
        }
        for (entries.items) |*entry| {
            const root = try ordered.publishIncremental(a, out, store, &entry.sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, entry.delta);
            var owned_binding = entry.binding;
            owned_binding.source_id = try out.dupe(u8, entry.binding.source_id);
            owned_binding.snapshot_id = try out.dupe(u8, entry.binding.snapshot_id);
            owned_binding.schema_fingerprint = try out.dupe(u8, entry.binding.schema_fingerprint);
            owned_binding.index_config_hash = try out.dupe(u8, entry.binding.index_config_hash);
            const paths = try out.alloc([]const u8, entry.binding.column_bindings.len);
            for (paths, entry.binding.column_bindings) |*path, old| path.* = try out.dupe(u8, old);
            owned_binding.column_bindings = paths;
            try result.append(out, .{ .name = root.name, .artifact = root, .binding = owned_binding });
        }
    }
    return result.toOwnedSlice(out);
}
fn claimSlot(a: A, names: *std.ArrayList([]const u8), hashes: *std.ArrayList([32]u8), vacant: *std.ArrayList(u32), file: []const u8, digest: [32]u8) !u32 {
    if (vacant.pop()) |slot| {
        names.items[slot] = file;
        hashes.items[slot] = digest;
        return slot;
    }
    if (names.items.len >= 16384) return error.LakeSidecarBuildLimitExceeded;
    const slot: u32 = @intCast(names.items.len);
    try names.append(a, file);
    try hashes.append(a, digest);
    return slot;
}

test "external lake ordered slot admission remains bounded under lifetime file churn" {
    const a = std.testing.allocator;
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    var hashes: std.ArrayList([32]u8) = .empty;
    defer hashes.deinit(a);
    var vacant: std.ArrayList(u32) = .empty;
    defer vacant.deinit(a);
    try names.resize(a, 16384);
    @memset(names.items, "retained");
    try hashes.resize(a, 16384);
    @memset(hashes.items, @splat(1));
    for (0..20000) |_| {
        try vacant.append(a, 41);
        try std.testing.expectEqual(@as(u32, 41), try claimSlot(a, &names, &hashes, &vacant, "replacement", @splat(2)));
    }
    try std.testing.expectEqual(@as(usize, 16384), names.items.len);
    try std.testing.expectEqualStrings("replacement", names.items[41]);
    try std.testing.expectError(error.LakeSidecarBuildLimitExceeded, claimSlot(a, &names, &hashes, &vacant, "overflow", @splat(3)));
}

fn check(raw: *anyopaque) !void {
    const provider: *@import("lake_index_row_source.zig").Provider = @ptrCast(@alignCast(raw));
    try provider.context.ensureActive();
}

test "external lake native ordered indexes spill real Parquet and seek exact expression keys" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-row-index-parquet");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const values = try a.alloc(i64, 4096);
    defer a.free(values);
    for (values, 0..) |*value, row| value.* = 9007199254740993 + @as(i64, @intCast(values.len - row));
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = values, .field_id = 1 }});
    defer a.free(bytes);
    var object = try client.putObject("antfly", "part.parquet", bytes, .{});
    object.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}]}},{{"name":"expression_idx","keys":[{{"expression":{{"op":"negate","args":[{{"op":"column","column":"amount"}}]}},"result_type":"integer","direction":"desc"}}],"include_columns":["amount"],"where":[{{"column":"amount","op":"gt","value":0}}]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(4), 1, std.testing.io);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    const declarations = try build(a, ca, table, &source, &store, &provider, .none, &.{});
    try std.testing.expectEqual(@as(usize, 2), declarations.len);
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
    defer parsed.deinit(a);
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, runtime);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
    defer layout.deinit();
    const definitions = (try parsed.relationalIndexDefinitions(ca)).?;
    for (declarations, definitions, 0..) |declaration, definition, index| {
        var tuple = try tuples.TuplePlan.init(a, runtime, &layout, definition.keys);
        defer tuple.deinit();
        var predicate: ?predicates.Plan = if (definition.where.len == 0) null else try predicates.Plan.init(a, runtime, &layout, definition.where);
        defer if (predicate) |*plan| plan.deinit();
        const target = values[17];
        var lower: std.ArrayList(u8) = .empty;
        defer lower.deinit(a);
        _ = try tuple.appendValues(a, &lower, &.{.{ .integer = if (index == 0) target else -target }});
        const upper = try a.dupe(u8, lower.items);
        defer a.free(upper);
        upper[upper.len - 1] += 1;
        const root = try ordered.loadRoot(ca, store, declaration.artifact, .none, null);
        var reader: ordered.Reader = undefined;
        try reader.init(a, &store, root, fingerprint(tuple, predicate, try coverColumns(ca, definition)), lower.items, upper, .none);
        defer reader.deinit();
        const refs = try reader.next(ca, 64);
        try std.testing.expectEqual(@as(usize, 1), refs.len);
        try std.testing.expectEqual(@as(u64, 17), refs[0].external.row_ordinal);
        try std.testing.expect(reader.exhausted);
        var covered_reader: ordered.Reader = undefined;
        try covered_reader.init(a, &store, root, fingerprint(tuple, predicate, try coverColumns(ca, definition)), lower.items, upper, .none);
        defer covered_reader.deinit();
        const entries = try covered_reader.nextEntries(ca, 64);
        const cover = entries[0].cover orelse return error.TestUnexpectedResult;
        const payload = try @import("lake_index_aggregate_artifact.zig").readArtifact(ca, store, cover.block, .none, null);
        const block = try spill.decodeColumnarBlockInArena(ca, payload, @import("lake_index_aggregate_artifact.zig").max_block_bytes);
        try std.testing.expectEqual(@as(usize, 1), root.cover.len);
        try std.testing.expectEqualStrings("amount", root.cover[0]);
        try std.testing.expectEqual(target, (try block.cell(cover.row, 0)).value.integer);
        try std.testing.expectEqualStrings(entries[0].key, (try block.keyCell(cover.row, 0)).value.string);
    }
    const reused = try build(a, ca, table, &source, &store, &provider, .none, declarations);
    for (declarations, reused) |old, new| try std.testing.expectEqualStrings(old.artifact.artifact_id, new.artifact.artifact_id);
}

pub fn coverColumns(a: A, definition: local.storage_relational_index.RelationalIndexDefinition) ![]const []const u8 {
    var columns: std.ArrayList([]const u8) = .empty;
    for (definition.keys) |key| if (key.expression_json == null) {
        const exists = for (columns.items) |column| {
            if (std.mem.eql(u8, column, key.column)) break true;
        } else false;
        if (!exists) try columns.append(a, key.column);
    };
    for (definition.include_columns) |column| {
        const exists = for (columns.items) |old| {
            if (std.mem.eql(u8, old, column)) break true;
        } else false;
        if (!exists) try columns.append(a, column);
    }
    return columns.items;
}
