// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Native algebraic definitions bind to SQL column types, paths and NULL
//! semantics. Unsupported join/time/custom-law recipes never claim SQL reuse.
const std = @import("std");
const local = @import("antfly_local_sources");
const recipes = local.sql_aggregate_materialization;
const operators = local.sql_operators;
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const A = std.mem.Allocator;

fn field(table: local.sql_catalog.Table, config: std.json.Value, name: []const u8, group: bool) !recipes.Key {
    var path = name;
    if (config.object.get(if (group) "group_fields" else "measure_fields")) |fields| {
        if (fields != .array) return error.InvalidAlgebraicConfig;
        for (fields.array.items) |item| {
            if (item != .object) return error.InvalidAlgebraicConfig;
            const alias = item.object.get("name") orelse return error.InvalidAlgebraicConfig;
            const source_path = item.object.get("path") orelse return error.InvalidAlgebraicConfig;
            if (alias != .string or source_path != .string) return error.InvalidAlgebraicConfig;
            if (std.mem.eql(u8, alias.string, name)) {
                path = source_path.string;
                break;
            }
        }
    }
    for (table.columns) |column| if (std.mem.eql(u8, column.path, path)) return .{ .path = column.path, .type = column.type, .nullable = column.nullable };
    return error.UndefinedColumn;
}
/// One persisted materialization is one reducer; output aliases are irrelevant.
/// This deliberately shares the exact Recipe contract used by SQL binding.
pub fn recipeFor(a: A, table: local.sql_catalog.Table, config: std.json.Value, mat: std.json.Value) !?recipes.Recipe {
    if (config != .object or mat != .object) return error.InvalidAlgebraicConfig;
    for ([_][]const u8{ "join", "time", "bucket", "group_side", "measure_side", "law", "histogram_field", "range_field" }) |name| {
        if (mat.object.get(name)) |value| if (value != .null) return null;
    }
    if (mat.object.get("axes")) |axes| if (axes != .array or axes.array.items.len != 0) return null;
    const operation = mat.object.get("op") orelse return error.InvalidAlgebraicConfig;
    if (operation != .string) return error.InvalidAlgebraicConfig;
    const kind = std.meta.stringToEnum(operators.Aggregate.Kind, operation.string) orelse return null;
    if (kind == .pattern_set) return null;
    const measure = mat.object.get("measure") orelse mat.object.get("value_field");
    const input: ?recipes.Key = if (measure) |value| key: {
        if (value == .null) break :key null;
        if (value != .string or value.string.len == 0) return error.InvalidAlgebraicConfig;
        break :key try field(table, config, value.string, false);
    } else null;
    if (input == null and kind != .count) return error.InvalidAlgebraicConfig;
    const spec: operators.AggregateSpec = .{ .kind = kind, .input_type = if (input) |column| column.type else null };
    try operators.Aggregate.validate(spec.kind, spec.input_type);
    const groups = mat.object.get("group_by");
    const keys = try a.alloc(recipes.Key, if (groups) |value| count: {
        if (value != .array or value.array.items.len > 256) return error.InvalidAlgebraicConfig;
        break :count value.array.items.len;
    } else 0);
    if (groups) |value| for (keys, value.array.items) |*key, name| {
        if (name != .string) return error.InvalidAlgebraicConfig;
        key.* = try field(table, config, name.string, true);
    };
    const inputs = try a.alloc(recipes.Input, 1);
    inputs[0] = .{ .spec = spec, .column = input };
    return .{ .keys = keys, .inputs = inputs };
}

pub fn recipeIdentity(a: A, recipe: recipes.Recipe) ![]const u8 {
    return std.fmt.allocPrint(a, "native-sql-aggregate-v1:{s}", .{std.fmt.bytesToHex(&recipe.fingerprint(), .lower)});
}

/// The publication arena owns declarations; each reducer and output chunk
/// owns only its bounded transient memory. Physical input dictionaries survive.
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: @import("antfly_cancellation").CancellationToken) ![]const Declared {
    var config_arena = std.heap.ArenaAllocator.init(a);
    defer config_arena.deinit();
    const ca = config_arena.allocator();
    const definitions = try std.json.parseFromSliceLeaky(std.json.Value, ca, table.indexes_json, .{ .allocate = .alloc_always });
    if (definitions != .object) return error.InvalidTableIndexMetadata;
    const has_algebraic = for (definitions.object.values()) |config| {
        if (config == .object) if (config.object.get("type")) |kind| {
            if (kind == .string and std.mem.eql(u8, kind.string, "algebraic")) break true;
        };
    } else false;
    if (!has_algebraic) return &.{};
    const io = provider.context.io orelse return error.UnsupportedSqlExecution;
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(io, ca, table.schema_json, table.table_id, table.name);
    const contract = try ca.alloc(local.serverless_query_lake_schema.Column, sql_table.columns.len);
    for (contract, sql_table.columns) |*column, definition| column.* = .{ .name = definition.path, .kind = @tagName(definition.type), .required = !definition.nullable };
    var native_provider = provider.*;
    native_provider.schema_contract = if (source.iceberg_schema) |selected| selected.columns else contract;
    if (source.iceberg_schema) |selected| for (contract) |expected| {
        const actual = for (selected.columns) |column| {
            if (std.mem.eql(u8, column.name, expected.name)) break column;
        } else return error.ExternalLakeSchemaMismatch;
        if (!std.mem.eql(u8, actual.kind, expected.kind) or (expected.required and !actual.required)) return error.ExternalLakeSchemaMismatch;
    };
    var declarations: std.ArrayList(Declared) = .empty;
    errdefer {
        // Nested bytes are allocated in the publication arena by the caller.
        declarations.deinit(out);
    }
    var iterator = definitions.object.iterator();
    while (iterator.next()) |entry| {
        const config = entry.value_ptr.*;
        if (config != .object) return error.InvalidTableIndexMetadata;
        const index_type = config.object.get("type") orelse continue;
        if (index_type != .string or !std.mem.eql(u8, index_type.string, "algebraic")) continue;
        const mats = config.object.get("materializations") orelse continue;
        if (mats != .array) return error.InvalidAlgebraicConfig;
        for (mats.array.items) |mat| {
            try cancellation.check();
            const recipe = (try recipeFor(ca, sql_table, config, mat)) orelse continue;
            const name = mat.object.get("name") orelse return error.InvalidAlgebraicConfig;
            if (name != .string or name.string.len == 0) return error.InvalidAlgebraicConfig;
            var projected: std.ArrayList([]const u8) = .empty;
            for (recipe.keys) |key| {
                const present = for (projected.items) |path| {
                    if (std.mem.eql(u8, path, key.path)) break true;
                } else false;
                if (!present) try projected.append(out, try out.dupe(u8, key.path));
            }
            if (recipe.inputs[0].column) |column| {
                const present = for (projected.items) |path| {
                    if (std.mem.eql(u8, path, column.path)) break true;
                } else false;
                if (!present) try projected.append(out, try out.dupe(u8, column.path));
            }
            const metadata_count = recipe.keys.len == 0 and recipe.inputs[0].spec.kind == .count and recipe.inputs[0].column == null and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true);
            if (projected.items.len == 0) {
                if (sql_table.columns.len == 0) return error.UnsupportedExternalLakeIndex;
                try projected.append(out, try out.dupe(u8, sql_table.columns[0].path));
            }
            const columns = try projected.toOwnedSlice(out);
            const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .algebraic, .source_kind = switch (source.inventory.format) {
                .parquet => .external_parquet,
                .iceberg => .external_iceberg,
                else => return error.UnsupportedExternalLakeIndex,
            }, .row_ref_kind = .external, .source_id = try out.dupe(u8, source.inventory.source_id), .snapshot_id = try out.dupe(u8, source.inventory.snapshot_id), .schema_fingerprint = try out.dupe(u8, source.inventory.schema_fingerprint), .column_bindings = columns, .index_config_hash = try recipeIdentity(out, recipe) };
            const spec = recipe.inputs[0].spec;
            var checkpoint_context = provider.context;
            const Checkpoint = struct {
                fn check(raw: *anyopaque) !void {
                    const ctx: *local.serverless_query_lake_read_context.Context = @ptrCast(@alignCast(raw));
                    try ctx.ensureActive();
                }
            };
            var spill: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &checkpoint_context, .checkpoint = Checkpoint.check, .async_writes = false };
            defer spill.deinit();
            const group = try operators.Grouped.create(a, &.{spec}, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = &spill });
            defer group.deinit();
            if (recipe.keys.len == 0) try group.ensureGlobalGroup();
            if (metadata_count) {
                var stream = try local.serverless_query_lake_stream.Stream.init(a, source, columns, &.{}, provider.context, provider.limits);
                defer stream.deinit();
                stream.schema_contract = native_provider.schema_contract;
                stream.identity_only = true;
                try group.addGlobalCount((try stream.countAll()) orelse return error.UnsupportedExternalLakeIndex);
            } else {
                var rows = try native_provider.provider().open(a, binding);
                defer rows.deinit(a);
                var budget = try @import("../serverless/build/lake_build_limits.zig").Budget.init(.{});
                while (try rows.next(a)) |batch| {
                    try cancellation.check();
                    try budget.admitBatch(batch);
                    var page = std.heap.ArenaAllocator.init(a);
                    defer page.deinit();
                    const pa = page.allocator();
                    const selection = try pa.alloc(usize, batch.rowCount());
                    for (selection, 0..) |*selected, i| selected.* = i;
                    const keys = try pa.alloc(local.sql_execution_batch.Batch, recipe.keys.len);
                    for (keys, recipe.keys) |*key, definition| key.* = .{ .columns = .{ .page = .{ .batch = batch, .selection = selection }, .definitions = try pa.dupe(local.sql_scalar.Column, &.{.{ .name = definition.path, .type = definition.type, .nullable = definition.nullable }}) } };
                    const input: local.sql_execution_batch.Batch = if (recipe.inputs[0].column) |column| .{ .columns = .{ .page = .{ .batch = batch, .selection = selection }, .definitions = try pa.dupe(local.sql_scalar.Column, &.{.{ .name = column.path, .type = column.type, .nullable = column.nullable }}) } } else constant: {
                        const ids = try pa.alloc(u32, batch.rowCount());
                        @memset(ids, 0);
                        break :constant .{ .dictionary = .{ .values = &.{local.sql_scalar.Datum.fromJson(.{ .integer = 1 })}, .indices = ids } };
                    };
                    try group.addEncodedColumns(keys, &.{input}, batch.rowCount());
                }
            }
            const artifact_name = try std.fmt.allocPrint(out, "{s}.{s}", .{ entry.key_ptr.*, name.string });
            const artifact = try artifacts.publish(a, out, store, artifact_name, group, recipe, cancellation);
            try declarations.append(out, .{ .name = artifact_name, .binding = binding, .artifact = artifact });
        }
    }
    return declarations.toOwnedSlice(out);
}

test "external lake native algebraic recipes preserve aliases and SQL NULL identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: local.sql_catalog.Table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{
        .{ .name = "key", .path = "key", .type = .string },
        .{ .name = "amount", .path = "amount", .type = .integer },
    } };
    const config = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"group_fields":[{"name":"tenant","path":"key","type":"string"}],"measure_fields":[{"name":"value","path":"amount","type":"integer"}]}
    , .{});
    const mat = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"total\",\"op\":\"sum\",\"group_by\":[\"tenant\"],\"measure\":\"value\"}", .{});
    const recipe = (try recipeFor(a, table, config, mat)).?;
    try std.testing.expectEqualStrings("key", recipe.keys[0].path);
    try std.testing.expectEqualStrings("amount", recipe.inputs[0].column.?.path);
    try std.testing.expectEqual(local.sql_ast.ColumnType.integer, recipe.inputs[0].spec.input_type.?);
    const star = (try recipeFor(a, table, config, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"rows\",\"op\":\"count\"}", .{}))).?;
    const column = (try recipeFor(a, table, config, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"values\",\"op\":\"count\",\"measure\":\"value\"}", .{}))).?;
    try std.testing.expect(!star.eql(column));
    const unsupported = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"joined\",\"op\":\"sum\",\"measure\":\"value\",\"join\":\"lookup\"}", .{});
    try std.testing.expect((try recipeFor(a, table, config, unsupported)) == null);
}

test "external lake native algebraic publication reads real Parquet into exact SQL partials" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-parquet");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 9007199254740993, 9007199254740993, -1 }, .field_id = 1 }});
    defer a.free(data);
    var object = try client.putObject("antfly", "part.parquet", data, .{});
    object.deinit(a);
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"amount\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const root = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(root);
    var fs_artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, root);
    defer fs_artifacts.deinit();
    var store = fs_artifacts.artifactStore();
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema, .indexes_json = "{\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"total\",\"op\":\"sum\",\"measure\":\"amount\"},{\"name\":\"rows\",\"op\":\"count\"}]}}" };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const declarations = try build(a, output.allocator(), table, &source, &store, &provider, .none);
    try std.testing.expectEqual(@as(usize, 2), declarations.len);
    const spec: operators.AggregateSpec = .{ .kind = .sum, .input_type = .integer };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = .{ .path = "amount", .type = .integer, .nullable = true } }} };
    const cursor = (try artifacts.Reader.open(a, store, declarations[0].artifact, recipe, .none)).cursor();
    defer cursor.close(cursor.ptr);
    var page = std.heap.ArenaAllocator.init(a);
    defer page.deinit();
    const partials = (try cursor.next(cursor.ptr, page.allocator(), 32)).?;
    const group = try operators.Grouped.create(a, &.{spec}, .{});
    defer group.deinit();
    try group.importPartial(partials[0].keys, partials[0].aggregates, partials[0].ordinal);
    const result = (try group.nextResult(page.allocator())).?;
    try std.testing.expectEqual(@as(i64, 18014398509481985), result.aggregates[0].value.integer);
    try std.testing.expect((try cursor.next(cursor.ptr, page.allocator(), 32)) == null);
    const count_recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = .{ .kind = .count }, .column = null }} };
    const count_cursor = (try artifacts.Reader.open(a, store, declarations[1].artifact, count_recipe, .none)).cursor();
    defer count_cursor.close(count_cursor.ptr);
    const counts = (try count_cursor.next(count_cursor.ptr, page.allocator(), 32)).?;
    var exact = try local.sql_aggregate_partial.decode(page.allocator(), counts[0].aggregates[0], .{ .kind = .count });
    defer exact.deinit();
    try std.testing.expectEqual(@as(u64, 3), exact.count);
}
