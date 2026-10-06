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
    return buildWithReuse(a, out, table, source, store, provider, cancellation, &.{});
}
pub fn buildWithReuse(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: @import("antfly_cancellation").CancellationToken, reusable: []const Declared) ![]const Declared {
    return (try buildIncremental(a, out, table, source, store, provider, cancellation, reusable, &.{})).declarations;
}
pub const BuildResult = struct { declarations: []const Declared, contributions: []const local.metadata_lake_index_catalog.FileContribution };
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: @import("antfly_cancellation").CancellationToken, reusable: []const Declared, old_contributions: []const local.metadata_lake_index_catalog.FileContribution) !BuildResult {
    var contributions: std.ArrayList(local.metadata_lake_index_catalog.FileContribution) = .empty;
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
    if (!has_algebraic) return .{ .declarations = &.{}, .contributions = &.{} };
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
    const Request = struct { recipe: recipes.Recipe, name: []const u8 };
    var requests: std.ArrayList(Request) = .empty;
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
            const identity = try recipeIdentity(ca, recipe);
            const logical_name = try @import("lake_index_names.zig").materialization(out, entry.key_ptr.*, name.string);
            for (old_contributions) |contribution| {
                if (!std.mem.eql(u8, contribution.name, logical_name) or !std.mem.eql(u8, &contribution.recipe, &recipe.fingerprint())) continue;
                const live = for (source.inventory.files) |file| {
                    if (std.mem.eql(u8, &contribution.file, &fileIdentity(source, file))) break true;
                } else false;
                if (live) try contributions.append(out, contribution);
            }
            const previous = for (reusable) |decl| {
                if (decl.artifact.kind == .algebraic_segment and artifacts.supportsMetadataVersion(decl.artifact.metadata_version) and std.mem.eql(u8, decl.binding.index_config_hash, identity) and try @import("lake_index_names.zig").matches(ca, decl.name, entry.key_ptr.*, name.string, decl.artifact.metadata_version)) break decl;
            } else null;
            if (previous) |decl| {
                // The coordinator proves complete source/schema/credential/store
                // equivalence before offering reusable immutable roots.
                try declarations.append(out, decl);
            } else {
                try requests.append(ca, .{ .recipe = recipe, .name = logical_name });
            }
        }
    }
    const consumed = try ca.alloc(bool, requests.items.len);
    @memset(consumed, false);
    for (requests.items, 0..) |request, first| {
        if (consumed[first]) continue;
        var cohort: std.ArrayList(usize) = .empty;
        // Cap reducer width independently of the number of configured indexes.
        // Larger sets form another bounded cohort with the same semantic keys.
        for (requests.items[first..], first..) |candidate, index| {
            if (consumed[index]) continue;
            const key_recipe: recipes.Recipe = .{ .keys = request.recipe.keys, .inputs = &.{} };
            if (!key_recipe.eql(.{ .keys = candidate.recipe.keys, .inputs = &.{} }) or incrementalRecipe(request.recipe) != incrementalRecipe(candidate.recipe)) continue;
            consumed[index] = true;
            try cohort.append(ca, index);
            if (cohort.items.len == 64) break;
        }
        const inputs = try ca.alloc(recipes.Input, cohort.items.len);
        const specs = try ca.alloc(operators.AggregateSpec, cohort.items.len);
        const names = try ca.alloc([]const u8, cohort.items.len);
        for (cohort.items, inputs, specs, names) |index, *input, *spec, *name| {
            input.* = requests.items[index].recipe.inputs[0];
            spec.* = input.spec;
            name.* = requests.items[index].name;
        }
        const recipe: recipes.Recipe = .{ .keys = request.recipe.keys, .inputs = inputs };
        {
            var projected: std.ArrayList([]const u8) = .empty;
            for (recipe.keys) |key| {
                const present = for (projected.items) |path| {
                    if (std.mem.eql(u8, path, key.path)) break true;
                } else false;
                if (!present) try projected.append(out, try out.dupe(u8, key.path));
            }
            for (recipe.inputs) |input| if (input.column) |column| {
                const present = for (projected.items) |path| {
                    if (std.mem.eql(u8, path, column.path)) break true;
                } else false;
                if (!present) try projected.append(out, try out.dupe(u8, column.path));
            };
            const counts_only = for (recipe.inputs) |input| {
                if (input.spec.kind != .count or input.column != null) break false;
            } else true;
            const metadata_count = recipe.keys.len == 0 and counts_only and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true);
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
            var checkpoint_context = provider.context;
            const Checkpoint = struct {
                fn check(raw: *anyopaque) !void {
                    const ctx: *local.serverless_query_lake_read_context.Context = @ptrCast(@alignCast(raw));
                    try ctx.ensureActive();
                }
            };
            var spill: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &checkpoint_context, .checkpoint = Checkpoint.check, .async_writes = false };
            defer spill.deinit();
            const group = try operators.Grouped.create(a, specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = &spill });
            defer group.deinit();
            if (recipe.keys.len == 0) try group.ensureGlobalGroup();
            var budget = try @import("../serverless/build/lake_build_limits.zig").Budget.init(.{});
            const contribution_limit = std.math.mul(usize, source.inventory.files.len, requests.items.len) catch std.math.maxInt(usize);
            const incremental = incrementalRecipe(recipe) and contribution_limit <= 16384 and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true);
            if (incremental) {
                for (source.inventory.files, 0..) |file, file_index| {
                    try cancellation.check();
                    try provider.context.ensureActive();
                    const file_key = fileIdentity(source, file);
                    const previous = try ca.alloc(?local.metadata_lake_index_catalog.FileContribution, cohort.items.len);
                    @memset(previous, null);
                    var all_present = true;
                    for (cohort.items, 0..) |request_index, slot| {
                        const single_recipe = requests.items[request_index].recipe;
                        for (old_contributions) |contribution| {
                            if (std.mem.eql(u8, &contribution.file, &file_key) and std.mem.eql(u8, &contribution.recipe, &single_recipe.fingerprint()) and std.mem.eql(u8, contribution.name, names[slot])) {
                                previous[slot] = contribution;
                                break;
                            }
                        }
                        all_present = all_present and previous[slot] != null;
                    }
                    if (all_present) {
                        for (previous, 0..) |contribution, slot| try importContribution(a, store, contribution.?.artifact, requests.items[cohort.items[slot]].recipe, @intCast(slot), group, cancellation);
                        continue;
                    }
                    const partial = try operators.Grouped.create(a, specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = &spill });
                    defer partial.deinit();
                    if (recipe.keys.len == 0) try partial.ensureGlobalGroup();
                    native_provider.only_file = file_index;
                    if (metadata_count) {
                        var stream = try local.serverless_query_lake_stream.Stream.init(a, source, columns, &.{}, provider.context, provider.limits);
                        defer stream.deinit();
                        try stream.restrictFile(file_index);
                        stream.schema_contract = native_provider.schema_contract;
                        stream.identity_only = true;
                        try partial.addGlobalCount((try stream.countAll()) orelse return error.UnsupportedExternalLakeIndex);
                    } else try consumeCohort(a, &native_provider, binding, recipe, partial, &budget, cancellation);
                    const refs = try artifacts.publishCohort(a, out, store, names, partial, recipe, cancellation);
                    for (refs, cohort.items, 0..) |ref, request_index, slot| {
                        try importContribution(a, store, ref, requests.items[request_index].recipe, @intCast(slot), group, cancellation);
                        // Replace obsolete slots for this file/cohort, retaining only live evidence.
                        var position: usize = 0;
                        while (position < contributions.items.len) {
                            const old = contributions.items[position];
                            if (std.mem.eql(u8, &old.file, &file_key) and std.mem.eql(u8, old.name, names[slot])) _ = contributions.swapRemove(position) else position += 1;
                        }
                        try contributions.append(out, .{ .file = file_key, .recipe = requests.items[request_index].recipe.fingerprint(), .name = names[slot], .artifact = ref });
                    }
                }
                native_provider.only_file = null;
            } else if (metadata_count) {
                var stream = try local.serverless_query_lake_stream.Stream.init(a, source, columns, &.{}, provider.context, provider.limits);
                defer stream.deinit();
                stream.schema_contract = native_provider.schema_contract;
                stream.identity_only = true;
                try group.addGlobalCount((try stream.countAll()) orelse return error.UnsupportedExternalLakeIndex);
            } else try consumeCohort(a, &native_provider, binding, recipe, group, &budget, cancellation);
            const published = try artifacts.publishCohort(a, out, store, names, group, recipe, cancellation);
            for (published, names, cohort.items) |artifact, name, index| {
                var slot_binding = binding;
                slot_binding.index_config_hash = try recipeIdentity(out, requests.items[index].recipe);
                try declarations.append(out, .{ .name = name, .binding = slot_binding, .artifact = artifact });
            }
        }
    }
    return .{ .declarations = try declarations.toOwnedSlice(out), .contributions = try contributions.toOwnedSlice(out) };
}

fn incrementalRecipe(recipe: recipes.Recipe) bool {
    for (recipe.keys) |key| if (key.type == .number) return false;
    for (recipe.inputs) |input| if (input.spec.distinct or !(input.spec.kind == .count or input.spec.kind == .bool_and or input.spec.kind == .bool_or or (input.spec.kind == .sum and input.spec.input_type == .integer) or ((input.spec.kind == .min or input.spec.kind == .max) and input.spec.input_type != .number))) return false;
    return true;
}
fn fileIdentity(source: *local.serverless_query_lake_serving.ServingSource, file: local.serverless_external_source_types.FileEntry) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-file-contribution-v1");
    for ([_][]const u8{ source.inventory.source_id, source.inventory.schema_fingerprint, file.file_id, file.object_uri, file.etag, file.version_id }) |bytes| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        hash.update(&length);
        hash.update(bytes);
    }
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, file.byte_len, .little);
    hash.update(&size);
    return hash.finalResult();
}
fn importContribution(a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, recipe: recipes.Recipe, slot: u16, group: *operators.Grouped, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    const reader = try artifacts.Reader.open(a, store.*, ref, recipe, cancellation);
    defer reader.cursor().close(reader);
    try reader.setOutputSlot(slot);
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const rows = (try reader.cursor().next(reader, page.allocator(), 256)) orelse break;
        for (rows) |row| try group.importPartialMapped(row.keys, row.aggregates, row.aggregate_slots, row.ordinal);
    }
}
fn consumeCohort(a: A, provider: *@import("lake_index_row_source.zig").Provider, binding: local.serverless_segment_source_binding.Binding, recipe: recipes.Recipe, group: *operators.Grouped, budget: *@import("../serverless/build/lake_build_limits.zig").Budget, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    var rows = try provider.provider().open(a, binding);
    defer rows.deinit(a);
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
        const input_batches = try pa.alloc(local.sql_execution_batch.Batch, recipe.inputs.len);
        for (input_batches, recipe.inputs) |*input, definition| input.* = if (definition.column) |column| .{ .columns = .{ .page = .{ .batch = batch, .selection = selection }, .definitions = try pa.dupe(local.sql_scalar.Column, &.{.{ .name = column.path, .type = column.type, .nullable = column.nullable }}) } } else constant: {
            const ids = try pa.alloc(u32, batch.rowCount());
            @memset(ids, 0);
            break :constant .{ .dictionary = .{ .values = &.{local.sql_scalar.Datum.fromJson(.{ .integer = 1 })}, .indices = ids } };
        };
        try group.addEncodedColumns(keys, input_batches, batch.rowCount());
    }
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
    const built = try buildIncremental(a, output.allocator(), table, &source, &store, &provider, .none, &.{}, &.{});
    const declarations = built.declarations;
    try std.testing.expectEqual(@as(usize, 2), built.contributions.len);
    try std.testing.expectEqual(@as(usize, 2), declarations.len);
    const spec: operators.AggregateSpec = .{ .kind = .sum, .input_type = .integer };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = .{ .path = "amount", .type = .integer, .nullable = true } }} };
    const sum_reader = try artifacts.Reader.open(a, store, declarations[0].artifact, recipe, .none);
    const cursor = sum_reader.cursor();
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
    const count_reader = try artifacts.Reader.open(a, store, declarations[1].artifact, count_recipe, .none);
    const count_cursor = count_reader.cursor();
    defer count_cursor.close(count_cursor.ptr);
    try std.testing.expect(sum_reader.root.state_slot != count_reader.root.state_slot);
    try std.testing.expectEqualStrings(sum_reader.root.blocks[0].artifact.artifact_id, count_reader.root.blocks[0].artifact.artifact_id);
    var wrong_slot = sum_reader.root;
    wrong_slot.state_slot = count_reader.root.state_slot;
    const wrong_bytes = try std.json.Stringify.valueAlloc(page.allocator(), wrong_slot, .{});
    var wrong = try store.put(wrong_bytes);
    defer wrong.deinit(a);
    const wrong_reference: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .algebraic_segment, .metadata_version = artifacts.metadata_version, .name = sum_reader.root.name, .artifact_id = wrong.artifact_id, .checksum = wrong.checksum, .byte_len = wrong.byte_len };
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, artifacts.Reader.open(a, store, wrong_reference, recipe, .none));
    const counts = (try count_cursor.next(count_cursor.ptr, page.allocator(), 32)).?;
    var exact = try local.sql_aggregate_partial.decode(page.allocator(), counts[0].aggregates[0], .{ .kind = .count });
    defer exact.deinit();
    try std.testing.expectEqual(@as(u64, 3), exact.count);
    var denied = source.scanner.object_reader.client.vtable.*;
    const Denied = struct {
        fn get(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: local.storage_object_storage.GetOptions) anyerror!local.storage_object_storage.GetResult {
            return error.UnexpectedParquetRead;
        }
    };
    denied.get_object = Denied.get;
    source.scanner.object_reader.client.vtable = &denied;
    const rebuilt = try buildIncremental(a, output.allocator(), table, &source, &store, &provider, .none, &.{}, built.contributions);
    try std.testing.expectEqual(@as(usize, 2), rebuilt.declarations.len);
    try std.testing.expectEqualStrings(declarations[0].artifact.checksum, rebuilt.declarations[0].artifact.checksum);
}
