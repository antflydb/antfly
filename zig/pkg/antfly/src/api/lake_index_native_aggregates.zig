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

fn field(table: local.sql_catalog.Table, config: std.json.Value, name: []const u8, group: bool) !local.sql_catalog.Column {
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
    for (table.columns) |column| if (std.mem.eql(u8, column.path, path)) return column;
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
    const input: ?local.sql_catalog.Column = if (measure) |value| key: {
        if (value == .null) break :key null;
        if (value != .string or value.string.len == 0) return error.InvalidAlgebraicConfig;
        break :key try field(table, config, value.string, false);
    } else null;
    if (input == null and kind != .count) return error.InvalidAlgebraicConfig;
    const spec: operators.AggregateSpec = .{ .kind = kind, .input_type = if (input) |column| column.type else null, .input_element = if (input) |column| local.sql_scalar.columnType(column).element_type else null };
    try operators.Aggregate.validate(spec.kind, spec.input_type);
    const groups = mat.object.get("group_by");
    const keys = try a.alloc(recipes.Key, if (groups) |value| count: {
        if (value != .array or value.array.items.len > 256) return error.InvalidAlgebraicConfig;
        break :count value.array.items.len;
    } else 0);
    if (groups) |value| for (keys, value.array.items) |*key, name| {
        if (name != .string) return error.InvalidAlgebraicConfig;
        const column = try field(table, config, name.string, true);
        key.* = recipes.Key.fromColumn(column);
    };
    const inputs = try a.alloc(recipes.Input, 1);
    inputs[0] = .{ .spec = spec, .column = if (input) |column| recipes.Key.fromColumn(column) else null };
    return .{ .keys = keys, .inputs = inputs };
}

pub fn recipeIdentity(a: A, recipe: recipes.Recipe) ![]const u8 {
    return std.fmt.allocPrint(a, "native-sql-aggregate-v2:{s}", .{std.fmt.bytesToHex(&recipe.fingerprint(), .lower)});
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
    return buildIndexed(a, out, table, source, store, provider, cancellation, reusable, old_contributions, null);
}
pub fn buildIndexed(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: @import("antfly_cancellation").CancellationToken, reusable: []const Declared, old_contributions: []const local.metadata_lake_index_catalog.FileContribution, contribution_lookup: ?*@import("lake_index_contributions.zig").Index) !BuildResult {
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
    try source.ensureInventory(provider.context);
    try source.ensureCanonicalPlan(provider.context);
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(io, ca, table.schema_json, table.table_id, table.name);
    const contract = try ca.alloc(local.serverless_query_lake_schema.Column, sql_table.columns.len);
    for (contract, sql_table.columns) |*column, definition| column.* = .{ .name = definition.path, .kind = if (definition.element_type == .numeric) "numeric" else @tagName(definition.type), .required = !definition.nullable };
    var native_provider = provider.*;
    native_provider.schema_contract = if (source.iceberg_schema) |selected| selected.columns else contract;
    if (source.iceberg_schema) |selected| for (contract) |expected| {
        const actual = for (selected.columns) |column| {
            if (std.mem.eql(u8, column.name, expected.name)) break column;
        } else return error.ExternalLakeSchemaMismatch;
        if (!expected.accepts(actual.kind) or (expected.required and !actual.required)) return error.ExternalLakeSchemaMismatch;
    };
    var old: ContributionIndex = .{};
    defer old.deinit(a);
    for (old_contributions, 0..) |contribution, index| try old.put(a, contributionKey(contribution.file, contribution.recipe, contribution.name), index);
    var file_keys: ?[][32]u8 = null;
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
            const contributes = incrementalRecipe(recipe) and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true);
            // Whole-root reuse remains valid for recipes with no file tree
            // (floating reductions, DISTINCT, or delete-bearing snapshots).
            const previous = for (if (contribution_lookup == null or !contributes) reusable else &.{}) |decl| {
                if (decl.artifact.kind == .algebraic_segment and artifacts.supportsMetadataVersion(decl.artifact.metadata_version) and std.mem.eql(u8, decl.binding.index_config_hash, identity) and try @import("lake_index_names.zig").matches(ca, decl.name, entry.key_ptr.*, name.string, decl.artifact.metadata_version)) break decl;
            } else null;
            if (previous) |decl| {
                // The coordinator proves complete source/schema/credential/store
                // equivalence before offering reusable immutable roots.
                try declarations.append(out, decl);
                // Reusable roots have the exact source signature. Keep their
                // complete live reduction tree without reopening any payload.
                for (old_contributions) |contribution| if (std.mem.eql(u8, contribution.name, logical_name) and std.mem.eql(u8, &contribution.recipe, &recipe.fingerprint())) try contributions.append(out, contribution);
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
            var local_budget = try @import("../serverless/build/lake_build_limits.zig").Budget.init(.{});
            const budget = if (provider.resume_work) |work| &work.aggregate else &local_budget;
            const incremental = incrementalRecipe(recipe) and source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len == 0 else true);
            if (incremental and source.inventory.files.len != 0) {
                var resolver: LeafResolver = .{ .a = a, .out = out, .provider = &native_provider, .source = source, .binding = binding, .recipe = recipe, .names = names, .old = &old, .previous = old_contributions, .index = contribution_lookup, .metadata_count = metadata_count, .store = store, .spill = &spill, .budget = budget, .cancellation = cancellation, .contributions = &contributions };
                var metadata_cache = local.serverless_query_lake_serving_cache.Cache.initWithMemoryLimit(a, 2 * 1024 * 1024);
                defer metadata_cache.deinit();
                metadata_cache.decoded.max_bytes = 4 * 1024 * 1024;
                const cached: artifacts.CachedRead = .{ .cache = &metadata_cache, .scope = if (store.upload_scope) |scope| scope.domain else @splat(0), .context = provider.context };
                var reduction: Reduction = .{ .cached = cached, .a = a, .out = out, .store = store, .recipe = recipe, .specs = specs, .names = names, .old = &old, .previous = old_contributions, .index = contribution_lookup, .contributions = &contributions, .spill = &spill, .cancellation = cancellation, .resolver = &resolver };
                defer reduction.shape_keys.deinit(a);
                defer reduction.proof_memo.deinit(a);
                native_provider.only_file = null;
                const root = (try reduction.reduceAppend(&native_provider)) orelse (try reduction.reduceDelta(&native_provider)) orelse full: {
                    if (file_keys == null) {
                        file_keys = try ca.alloc([32]u8, source.inventory.files.len);
                        for (source.inventory.files, file_keys.?) |file, *key| key.* = fileIdentity(source, file);
                    }
                    const leaves = try ca.alloc(Reduction.Leaf, file_keys.?.len);
                    for (file_keys.?, leaves, 0..) |key, *leaf, index| leaf.* = .{ .key = key, .index = index };
                    var scan_order = @import("lake_index_native_state.zig").ScanOrder.init(&native_provider);
                    var pending: [8]Reduction.Leaf = undefined;
                    var count: usize = 0;
                    while (try scan_order.next()) |ordinal| {
                        pending[count] = leaves[ordinal];
                        count += 1;
                        if (count == pending.len) {
                            const resolved = try resolver.resolveBatch(pending[0..count]);
                            for (resolved) |leaf| leaves[leaf.index] = leaf;
                            out.free(resolved);
                            count = 0;
                        }
                    }
                    if (count != 0) {
                        const resolved = try resolver.resolveBatch(pending[0..count]);
                        for (resolved) |leaf| leaves[leaf.index] = leaf;
                        out.free(resolved);
                    }
                    std.mem.sort(Reduction.Leaf, leaves, {}, struct {
                        fn less(_: void, left: Reduction.Leaf, right: Reduction.Leaf) bool {
                            return std.mem.order(u8, &left.key, &right.key) == .lt;
                        }
                    }.less);
                    break :full try reduction.reduce(leaves);
                };
                for (root.refs, names, cohort.items) |artifact, name, index| {
                    var slot_binding = binding;
                    slot_binding.index_config_hash = try recipeIdentity(out, requests.items[index].recipe);
                    if (contribution_lookup) |lookup| try lookup.includeRoot(contributionKey(root.key, requests.items[index].recipe.fingerprint(), name));
                    try declarations.append(out, .{ .name = name, .binding = slot_binding, .artifact = artifact });
                }
                continue;
            } else if (metadata_count) {
                var stream = try local.serverless_query_lake_stream.Stream.init(a, source, columns, &.{}, provider.context, provider.limits);
                defer stream.deinit();
                stream.schema_contract = native_provider.schema_contract;
                stream.identity_only = true;
                try group.addGlobalCount((try stream.countAll()) orelse return error.UnsupportedExternalLakeIndex);
            } else {
                if (native_provider.replay_consumer != null) {
                    var scan = try @import("lake_index_native_state.zig").Plan.init(a, ca, &native_provider, &.{});
                    defer scan.deinit();
                    native_provider.only_plan = &scan;
                    defer native_provider.only_plan = null;
                    try consumeCohort(a, &native_provider, binding, recipe, group, budget, cancellation);
                } else try consumeCohort(a, &native_provider, binding, recipe, group, budget, cancellation);
            }
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

pub fn incrementalRecipe(recipe: recipes.Recipe) bool {
    for (recipe.keys) |key| if (key.type == .number and key.element_type != .numeric) return false;
    for (recipe.inputs) |input| {
        const spec = input.spec;
        if (spec.distinct) return false;
        const exact = spec.input_type == .number and spec.input_element == .numeric;
        if (!switch (spec.kind) {
            .count, .bool_and, .bool_or => true,
            .sum => spec.input_type == .integer or exact,
            .avg => exact,
            .min, .max => spec.input_type != .number or exact,
            else => false,
        }) return false;
    }
    return true;
}
pub fn fileIdentity(source: *local.serverless_query_lake_serving.ServingSource, file: local.serverless_external_source_types.FileEntry) [32]u8 {
    var qualified = file;
    if (source.verified_files) |files| if (files.get(file.file_id)) |verified| {
        qualified.etag = verified.etag;
        qualified.version_id = verified.version_id;
    };
    return inventoryFileIdentity(source.inventory, qualified);
}
pub fn inventoryFileIdentity(inventory: local.serverless_external_source_types.Inventory, file: local.serverless_external_source_types.FileEntry) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-file-contribution-v1");
    for ([_][]const u8{ inventory.source_id, inventory.schema_fingerprint, file.file_id, file.object_uri, file.etag, file.version_id }) |bytes| {
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
const ContributionIndex = std.AutoHashMapUnmanaged([32]u8, usize);
pub fn contributionKey(file: [32]u8, recipe: [32]u8, name: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-contribution-lookup-v1");
    hash.update(&file);
    hash.update(&recipe);
    hash.update(name);
    return hash.finalResult();
}
// A compressed binary radix tree over stable file digests. Inserting/removing
// a file leaves unrelated subtree identities unchanged. Every internal node
// is itself an ordinary exact aggregate artifact, so existing readers and GC
// retain their authenticated block format and bounded reducer semantics.
const LeafResolver = struct {
    a: A,
    out: A,
    provider: *@import("lake_index_row_source.zig").Provider,
    source: *local.serverless_query_lake_serving.ServingSource,
    binding: local.serverless_segment_source_binding.Binding,
    recipe: recipes.Recipe,
    names: []const []const u8,
    old: *const ContributionIndex,
    previous: []const local.metadata_lake_index_catalog.FileContribution,
    index: ?*@import("lake_index_contributions.zig").Index,
    metadata_count: bool,
    store: *stores.ArtifactStore,
    spill: *local.sql_spill.Manager,
    budget: *@import("../serverless/build/lake_build_limits.zig").Budget,
    cancellation: @import("antfly_cancellation").CancellationToken,
    contributions: *std.ArrayList(local.metadata_lake_index_catalog.FileContribution),
    const Job = struct {
        resolver: *LeafResolver,
        leaf: Reduction.Leaf,
        a: A,
        spill: local.sql_spill.Manager = undefined,
        group: ?*operators.Grouped = null,
        source: ?local.serverless_query_lake_serving.ServingSource = null,
        budget: *@import("../serverless/build/lake_build_limits.zig").Budget,
        fn run(self: *@This()) anyerror!void {
            const owner = self.resolver;
            self.spill = .{ .alloc = self.a, .io = owner.provider.context.io.?, .context = &owner.provider.context, .checkpoint = check, .async_writes = false };
            const specs = try self.a.alloc(operators.AggregateSpec, owner.recipe.inputs.len);
            defer self.a.free(specs);
            for (owner.recipe.inputs, specs) |input, *spec| spec.* = input.spec;
            self.group = try operators.Grouped.create(self.a, specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = &self.spill });
            if (owner.recipe.keys.len == 0) try self.group.?.ensureGlobalGroup();
            var provider = owner.provider.*;
            provider.source = &self.source.?;
            // Parallel file jobs have independent streams. A publication's
            // file rendezvous instead shares its one pinned run across families.
            provider.replay = null;
            provider.replay_consumer = null;
            provider.only_plan = null;
            provider.only_files = null;
            provider.only_file = self.leaf.index;
            try consumeCohort(self.a, &provider, owner.binding, owner.recipe, self.group.?, self.budget, owner.cancellation);
        }
        fn check(raw: *anyopaque) !void {
            const context: *local.serverless_query_lake_read_context.Context = @ptrCast(@alignCast(raw));
            try context.ensureActive();
        }
        fn deinit(self: *@This()) void {
            if (self.group) |group| group.deinit();
            self.spill.deinit();
            if (self.source) |*source| source.closeParallelView(self.resolver.source);
        }
    };
    fn present(self: *@This(), leaf: Reduction.Leaf) !bool {
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.names, 0..) |name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const key = contributionKey(leaf.key, single.fingerprint(), name);
            if (self.old.contains(key)) continue;
            if (self.index) |index| if (try index.lookup(scratch.allocator(), key) != null) continue;
            return false;
        }
        return true;
    }
    /// Only ready replay readers are shared. Capturing the scan, uploading
    /// artifacts and changing contribution ownership remain coordinator work.
    fn resolveParallel(self: *@This(), leaves: []const Reduction.Leaf) ![]const Reduction.Leaf {
        const io = self.provider.context.io.?;
        if (self.provider.replay_consumer != null) return leaves;
        if (leaves.len < 2 or leaves.len > 8 or self.metadata_count) return leaves;
        var needed: [8]bool = @splat(false);
        var count: usize = 0;
        for (leaves, 0..) |leaf, i| {
            needed[i] = leaf.refs.len == 0 and !try self.present(leaf);
            count += @intFromBool(needed[i]);
        }
        if (count < 2) return leaves;
        // Independent streams retain compressed input, decoded vectors and a
        // partial group. Concurrency must fit the publication allocator too;
        // scheduler leases account for CPU workspace, not decoder ownership.
        const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(self.a) orelse return leaves;
        const per_job = self.provider.limits.max_input_bytes +| self.provider.limits.max_decoded_bytes *| 2 +| (8 * 1024 * 1024);
        if (count > available / @max(per_job, 1)) return leaves;
        var locked: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = self.a };
        var jobs: [8]Job = undefined;
        var tasks: [8]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        var initialized: [8]bool = @splat(false);
        defer {
            for (&tasks) |*task| if (task.*) |*pending| {
                if (pending.future != null) pending.cancel(io) catch {};
            };
            for (initialized, 0..) |ready, i| if (ready) jobs[i].deinit();
        }
        for (leaves, 0..) |leaf, i| if (needed[i]) {
            jobs[i] = .{ .resolver = self, .leaf = leaf, .a = locked.allocator(), .budget = self.budget };
            // Initialize even when a task is canceled before its first call.
            jobs[i].spill = .{ .alloc = locked.allocator(), .io = io, .context = &self.provider.context, .checkpoint = Job.check, .async_writes = false };
            initialized[i] = true;
            jobs[i].source = try self.source.parallelView(locked.allocator());
            tasks[i] = local.sql_parallel_scheduler.global().submit(io, 8 * 1024 * 1024, Job.run, .{&jobs[i]});
            if (tasks[i] == null) try jobs[i].run();
        };
        // Join every worker before allocating through the coordinator arena.
        for (&tasks) |*task| if (task.*) |*pending| try pending.await(io);
        const resolved = try self.out.alloc(Reduction.Leaf, leaves.len);
        for (leaves, resolved, 0..) |leaf, *result, i| {
            result.* = if (leaf.refs.len != 0) leaf else try self.resolveWithPartial(leaf, if (needed[i]) jobs[i].group else null);
        }
        return resolved;
    }
    fn resolveBatch(self: *@This(), leaves: []const Reduction.Leaf) ![]const Reduction.Leaf {
        const prepared = try self.resolveParallel(leaves);
        if (prepared.ptr != leaves.ptr) return prepared;
        const resolved = try self.out.alloc(Reduction.Leaf, leaves.len);
        for (leaves, resolved) |leaf, *target| target.* = if (leaf.refs.len != 0) leaf else try self.resolve(leaf);
        return resolved;
    }
    fn resolve(self: *@This(), leaf: Reduction.Leaf) !Reduction.Leaf {
        return self.resolveWithPartial(leaf, null);
    }
    fn resolveWithPartial(self: *@This(), leaf: Reduction.Leaf, prepared: ?*operators.Grouped) !Reduction.Leaf {
        try self.cancellation.check();
        try self.provider.context.ensureActive();
        const refs = try self.out.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
        var all_present = true;
        var native_owned = true;
        var from_index = true;
        for (self.names, 0..) |name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const key = contributionKey(leaf.key, single.fingerprint(), name);
            if (self.old.get(key)) |old_index| {
                refs[slot] = self.previous[old_index].artifact;
                from_index = false;
            } else if (self.index) |lookup| {
                if (try lookup.lookup(self.out, key)) |value| {
                    refs[slot] = value.artifact;
                    if (value.radix == null) native_owned = false;
                } else all_present = false;
            } else all_present = false;
        }
        if (!all_present) {
            const specs = try self.a.alloc(operators.AggregateSpec, self.recipe.inputs.len);
            defer self.a.free(specs);
            for (self.recipe.inputs, specs) |input, *spec| spec.* = input.spec;
            const partial = prepared orelse try operators.Grouped.create(self.a, specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = self.spill });
            defer if (prepared == null) partial.deinit();
            if (prepared == null) {
                if (self.recipe.keys.len == 0) try partial.ensureGlobalGroup();
                self.provider.only_file = leaf.index;
                defer self.provider.only_file = null;
                const file = self.source.inventory.files[leaf.index];
                if (self.metadata_count) {
                    if (self.source.inventory.format == .iceberg) {
                        try partial.addGlobalCount(file.row_count);
                    } else {
                        // Footer discovery qualifies the immutable file version;
                        // identity-only counting never decodes a value column.
                        var stream = try local.serverless_query_lake_stream.Stream.init(self.a, self.source, &.{}, &.{}, self.provider.context, self.provider.limits);
                        defer stream.deinit();
                        stream.identity_only = true;
                        stream.schema_contract = self.provider.schema_contract;
                        try stream.restrictFile(leaf.index);
                        try partial.addGlobalCount((try stream.countAll()) orelse return error.UnsupportedExternalLakeIndex);
                    }
                } else try consumeCohort(self.a, self.provider, self.binding, self.recipe, partial, self.budget, self.cancellation);
            }
            const published = if (self.recipe.keys.len != 0) try artifacts.publishPartitioned(self.a, self.out, self.store, self.names, partial, self.recipe, self.spill, self.cancellation) else try artifacts.publishCohort(self.a, self.out, self.store, self.names, partial, self.recipe, self.cancellation);
            @memcpy(refs, published);
            self.out.free(published);
        }
        for (refs, self.names, 0..) |ref, name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            if (all_present and from_index and native_owned) try self.index.?.retain(contributionKey(leaf.key, single.fingerprint(), name)) else try self.contributions.append(self.out, .{ .file = leaf.key, .recipe = single.fingerprint(), .name = name, .artifact = ref, .radix = .{ .first = leaf.key, .last = leaf.key, .files = 1 } });
        }
        return .{ .key = leaf.key, .range = .{ .first = leaf.key, .last = leaf.key, .files = 1 }, .refs = refs, .index = leaf.index };
    }
};

const Reduction = struct {
    const Leaf = struct { key: [32]u8, range: ?local.metadata_lake_index_catalog.ReductionRange = null, refs: []const local.serverless_manifest_artifact_ref.ArtifactRef = &.{}, index: usize = 0 };
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    recipe: recipes.Recipe,
    specs: []const operators.AggregateSpec,
    names: []const []const u8,
    old: *const ContributionIndex,
    previous: []const local.metadata_lake_index_catalog.FileContribution,
    index: ?*@import("lake_index_contributions.zig").Index = null,
    contributions: *std.ArrayList(local.metadata_lake_index_catalog.FileContribution),
    spill: *local.sql_spill.Manager,
    cancellation: @import("antfly_cancellation").CancellationToken,
    resolver: ?*LeafResolver = null,
    shape_keys: std.AutoHashMapUnmanaged([2][32]u8, [32]u8) = .empty,
    proof_memo: artifacts.ProofMemo = .{},
    cached: ?artifacts.CachedRead = null,
    fn shapeKey(self: *@This(), leaves: []const Leaf) anyerror![32]u8 {
        if (leaves.len == 1) return leaves[0].key;
        const range = [2][32]u8{ leaves[0].key, leaves[leaves.len - 1].key };
        if (self.shape_keys.get(range)) |key| return key;
        const split = splitAt(leaves);
        if (split == 0) return error.InvalidLakeIndexCatalog;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-aggregate-reduction-v1");
        hash.update(&try self.shapeKey(leaves[0..split]));
        hash.update(&try self.shapeKey(leaves[split..]));
        const key = hash.finalResult();
        try self.shape_keys.put(self.a, range, key);
        return key;
    }
    fn splitAt(leaves: []const Leaf) usize {
        var bit: usize = 0;
        while (bit < 256 and bitAt(leaves[0].key, bit) == bitAt(leaves[leaves.len - 1].key, bit)) : (bit += 1) {}
        if (bit == 256) return 0;
        var split: usize = 1;
        while (split < leaves.len and !bitAt(leaves[split].key, bit)) : (split += 1) {}
        return split;
    }
    fn reduce(self: *@This(), leaves: []const Leaf) anyerror!Leaf {
        try self.cancellation.check();
        if (leaves.len == 1) return if (leaves[0].refs.len != 0) leaves[0] else try self.resolver.?.resolve(leaves[0]);
        const split = splitAt(leaves);
        if (split == 0) return error.InvalidLakeIndexCatalog;
        const key = try self.shapeKey(leaves);
        // Probe before descending. Ownership keeps all internal nodes and
        // range aliases live without rebuilding their aggregate directories.
        if (self.index) |index| {
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const retained_refs = try self.out.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
            var complete_owned = true;
            for (self.names, 0..) |name, slot| {
                const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
                const lookup_key = contributionKey(key, single.fingerprint(), name);
                const value = try index.lookup(scratch.allocator(), lookup_key);
                if (value == null or value.?.owned == null or value.?.radix == null) {
                    complete_owned = false;
                    break;
                }
                const bytes = try std.json.Stringify.valueAlloc(scratch.allocator(), value.?.artifact, .{});
                retained_refs[slot] = try std.json.parseFromSliceLeaky(local.serverless_manifest_artifact_ref.ArtifactRef, self.out, bytes, .{ .allocate = .alloc_always });
            }
            if (complete_owned) {
                for (self.names, 0..) |name, slot| {
                    const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
                    try index.retain(contributionKey(key, single.fingerprint(), name));
                }
                return .{ .key = key, .range = .{ .first = leaves[0].key, .last = leaves[leaves.len - 1].key, .files = @intCast(leaves.len) }, .refs = retained_refs };
            }
            self.out.free(retained_refs);
        }
        const prepared = if (self.resolver) |resolver| try resolver.resolveParallel(leaves) else leaves;
        defer if (prepared.ptr != leaves.ptr) self.out.free(prepared);
        const left = try self.reduce(prepared[0..split]);
        const right = try self.reduce(prepared[split..]);
        return self.join(left, right);
    }
    fn join(self: *@This(), left: Leaf, right: Leaf) anyerror!Leaf {
        const lrange = left.range orelse return error.InvalidLakeIndexCatalog;
        const rrange = right.range orelse return error.InvalidLakeIndexCatalog;
        if (std.mem.order(u8, &lrange.last, &rrange.first) != .lt) return error.InvalidLakeIndexCatalog;
        const range: local.metadata_lake_index_catalog.ReductionRange = .{ .first = lrange.first, .last = rrange.last, .files = try std.math.add(u32, lrange.files, rrange.files) };
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-aggregate-reduction-v1");
        hash.update(&left.key);
        hash.update(&right.key);
        const key = hash.finalResult();
        const refs = try self.out.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
        var complete = true;
        for (self.names, 0..) |name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const lookup_key = contributionKey(key, single.fingerprint(), name);
            if (self.old.get(lookup_key)) |old_index| refs[slot] = self.previous[old_index].artifact else if (self.index) |lookup| {
                if (try lookup.lookup(self.out, lookup_key)) |value| refs[slot] = value.artifact else complete = false;
            } else complete = false;
        }
        const aliases = try self.out.alloc(std.ArrayList([32]u8), self.names.len);
        @memset(aliases, .empty);
        defer {
            for (aliases) |*list| list.deinit(self.out);
            self.out.free(aliases);
        }
        const was_complete = complete;
        const routed = self.recipe.keys.len != 0 and (artifacts.isPartitioned(left.refs[0].metadata_version) or artifacts.isPartitioned(right.refs[0].metadata_version));
        var routed_left = left;
        var routed_right = right;
        if (routed) {
            routed_left.refs = try self.splitRefs(left.refs, 0);
            routed_right.refs = try self.splitRefs(right.refs, 0);
        }
        if (!complete and routed) {
            const published = try self.mergePartitions(routed_left, routed_right, null, aliases, 0, null);
            @memcpy(refs, published);
            self.out.free(published);
            complete = true;
        }
        if (!complete) {
            const group = try operators.Grouped.create(self.a, self.specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = self.spill });
            defer group.deinit();
            if (self.recipe.keys.len == 0) try group.ensureGlobalGroup();
            try importContributionsCached(self.a, self.store, left.refs, self.recipe, group, self.cancellation, self.cached);
            try importContributionsCached(self.a, self.store, right.refs, self.recipe, group, self.cancellation, self.cached);
            const published = if (self.recipe.keys.len != 0) try artifacts.publishPartitioned(self.a, self.out, self.store, self.names, group, self.recipe, self.spill, self.cancellation) else try artifacts.publishCohort(self.a, self.out, self.store, self.names, group, self.recipe, self.cancellation);
            @memcpy(refs, published);
            self.out.free(published);
        }
        if (was_complete and routed) {
            self.out.free(try self.mergePartitions(routed_left, routed_right, refs, aliases, 0, null));
        }
        for (self.names, refs, 0..) |name, ref, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            var owned: std.ArrayList([32]u8) = .empty;
            try owned.append(self.out, contributionKey(left.key, single.fingerprint(), name));
            try owned.append(self.out, contributionKey(right.key, single.fingerprint(), name));
            // Range aliases are recipe-local; child ownership handles all
            // descendants. Each record stays bounded independently of files.
            try owned.appendSlice(self.out, aliases[slot].items);
            try self.contributions.append(self.out, .{ .file = key, .recipe = single.fingerprint(), .name = name, .artifact = ref, .owned = try owned.toOwnedSlice(self.out), .radix = range });
        }
        return .{ .key = key, .range = range, .refs = refs };
    }
    fn firstDifference(left: [32]u8, right: [32]u8) usize {
        for (left, right, 0..) |l, r, i| if (l != r) return i * 8 + @as(usize, @clz(l ^ r));
        return 256;
    }
    fn loadNode(self: *@This(), key: [32]u8) anyerror!Leaf {
        const refs = try self.out.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
        var range: ?local.metadata_lake_index_catalog.ReductionRange = null;
        for (self.names, refs, 0..) |name, *ref, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const value = (try self.index.?.lookup(self.out, contributionKey(key, single.fingerprint(), name))) orelse return error.InvalidLakeIndexCatalog;
            const bounds = value.radix orelse return error.InvalidLakeIndexCatalog;
            if (range) |prior| if (!std.meta.eql(prior, bounds)) return error.InvalidLakeIndexCatalog;
            range = bounds;
            ref.* = value.artifact;
        }
        return .{ .key = key, .range = range, .refs = refs };
    }
    fn children(self: *@This(), node: Leaf) anyerror![2]Leaf {
        const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[0..1] };
        const value = (try self.index.?.lookup(self.out, contributionKey(node.key, single.fingerprint(), self.names[0]))) orelse return error.InvalidLakeIndexCatalog;
        const owned = value.owned orelse return error.InvalidLakeIndexCatalog;
        if (owned.len < 2) return error.InvalidLakeIndexCatalog;
        var result: [2]Leaf = undefined;
        for (&result, owned[0..2]) |*child, key| {
            const record = (try self.index.?.lookup(self.out, key)) orelse return error.InvalidLakeIndexCatalog;
            if (!std.mem.eql(u8, record.name, self.names[0]) or !std.mem.eql(u8, &record.recipe, &single.fingerprint())) return error.InvalidLakeIndexCatalog;
            child.* = try self.loadNode(record.file);
        }
        const l = result[0].range.?;
        const r = result[1].range.?;
        const n = node.range.?;
        if (!std.mem.eql(u8, &l.first, &n.first) or !std.mem.eql(u8, &r.last, &n.last) or std.mem.order(u8, &l.last, &r.first) != .lt or @as(u64, l.files) + r.files != n.files) return error.InvalidLakeIndexCatalog;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-aggregate-reduction-v1");
        hash.update(&result[0].key);
        hash.update(&result[1].key);
        if (!std.mem.eql(u8, &hash.finalResult(), &node.key)) return error.InvalidLakeIndexCatalog;
        return result;
    }
    const Mutation = struct { key: [32]u8, replacement: ?Leaf };
    fn editBatch(self: *@This(), node: ?Leaf, changes: []const Mutation, depth: usize) anyerror!?Leaf {
        try self.cancellation.check();
        if (depth > 256) return error.InvalidLakeIndexCatalog;
        if (changes.len == 0) return node;
        if (node == null) {
            const leaves = try self.out.alloc(Leaf, changes.len);
            for (changes, leaves) |change, *leaf| leaf.* = change.replacement orelse return error.InvalidLakeIndexCatalog;
            return try self.reduce(leaves);
        }
        const current = node.?;
        const range = current.range orelse return error.InvalidLakeIndexCatalog;
        if (range.files == 1 and changes.len == 1 and std.mem.eql(u8, &current.key, &changes[0].key)) return changes[0].replacement;
        const first = if (std.mem.order(u8, &range.first, &changes[0].key) == .lt) range.first else changes[0].key;
        const last = if (std.mem.order(u8, &range.last, &changes[changes.len - 1].key) == .gt) range.last else changes[changes.len - 1].key;
        const branch = firstDifference(first, last);
        if (branch == 256) return error.InvalidLakeIndexCatalog;
        var parts: [2]?Leaf = .{ null, null };
        if (firstDifference(range.first, range.last) == branch) {
            const children_ = try self.children(current);
            parts = .{ children_[0], children_[1] };
        } else parts[@intFromBool(bitAt(range.first, branch))] = current;
        var split: usize = 0;
        while (split < changes.len and !bitAt(changes[split].key, branch)) : (split += 1) {}
        const left = try self.editBatch(parts[0], changes[0..split], depth + 1);
        const right = try self.editBatch(parts[1], changes[split..], depth + 1);
        if (left == null) return right;
        if (right == null) return left;
        return try self.join(left.?, right.?);
    }
    fn reduceAppend(self: *@This(), provider: *@import("lake_index_row_source.zig").Provider) anyerror!?Leaf {
        const work = provider.resume_work orelse return null;
        const file = work.file orelse return null;
        const index = self.index orelse return error.InvalidLakeIndexCatalog;
        const end = work.end orelse file + 1;
        if (end <= file or end != provider.source.inventory.files.len) return error.InvalidLakeIndexCatalog;
        const changes = try self.out.alloc(Mutation, end - file);
        for (changes, file..) |*change, ordinal| {
            const incoming = try self.resolver.?.resolve(.{ .key = fileIdentity(provider.source, provider.source.inventory.files[ordinal]), .index = ordinal });
            change.* = .{ .key = incoming.key, .replacement = incoming };
        }
        std.mem.sort(Mutation, changes, {}, struct {
            fn less(_: void, l: Mutation, r: Mutation) bool {
                return std.mem.lessThan(u8, &l.key, &r.key);
            }
        }.less);
        if (file == 0) return try self.editBatch(null, changes, 0);
        var prior: ?[32]u8 = null;
        for (self.names, 0..) |name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const record = (try index.reductionRoot(self.out, single.fingerprint(), name, file)) orelse return error.InvalidLakeIndexCatalog;
            if (prior) |key| if (!std.mem.eql(u8, &key, &record.file)) return error.InvalidLakeIndexCatalog;
            prior = record.file;
        }
        const previous = try self.loadNode(prior.?);
        const root = (try self.editBatch(previous, changes, 0)) orelse return error.InvalidLakeIndexCatalog;
        if (root.range.?.files != end) return error.InvalidLakeIndexCatalog;
        try index.recordDeltaReduction();
        return root;
    }
    fn reduceDelta(self: *@This(), provider: *@import("lake_index_row_source.zig").Provider) anyerror!?Leaf {
        const delta = provider.native_delta orelse return null;
        const index = self.index orelse return null;
        if (!index.acceptsDelta(delta) or delta.count() != provider.source.inventory.files.len) return null;
        for (delta.prior_changed) |file| if (file.aggregate_digest == null) return null;
        var prior: ?[32]u8 = null;
        for (self.names, 0..) |name, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const record = (try index.reductionRoot(self.out, single.fingerprint(), name, delta.prior_count)) orelse return null;
            if (prior) |key| {
                if (!std.mem.eql(u8, &key, &record.file)) return error.InvalidLakeIndexCatalog;
            }
            prior = record.file;
        }
        const previous = try self.loadNode(prior orelse return null);
        var changes: std.AutoHashMapUnmanaged([32]u8, ?Leaf) = .empty;
        defer changes.deinit(self.a);
        for (delta.prior_changed) |file| try changes.put(self.a, file.aggregate_digest.?, null);
        var plan = try @import("lake_index_native_state.zig").Plan.fromDelta(self.a, self.out, delta);
        defer plan.deinit();
        var iterator = plan.iterator(true);
        var pending: [8]Leaf = undefined;
        var count: usize = 0;
        while (try iterator.next()) |item| {
            pending[count] = .{ .key = fileIdentity(provider.source, provider.source.inventory.files[item.ordinal]), .index = item.ordinal };
            count += 1;
            if (count == pending.len) {
                const resolved = try self.resolver.?.resolveBatch(pending[0..count]);
                for (resolved) |leaf| try changes.put(self.a, leaf.key, leaf);
                self.out.free(resolved);
                count = 0;
            }
        }
        if (count != 0) {
            const resolved = try self.resolver.?.resolveBatch(pending[0..count]);
            for (resolved) |leaf| try changes.put(self.a, leaf.key, leaf);
            self.out.free(resolved);
        }
        const batch = try self.out.alloc(Mutation, changes.count());
        var entries = changes.iterator();
        for (batch) |*change| {
            const entry = entries.next().?;
            change.* = .{ .key = entry.key_ptr.*, .replacement = entry.value_ptr.* };
        }
        std.mem.sort(Mutation, batch, {}, struct {
            fn less(_: void, l: Mutation, r: Mutation) bool {
                return std.mem.lessThan(u8, &l.key, &r.key);
            }
        }.less);
        const root = try self.editBatch(previous, batch, 0);
        if (root == null or root.?.range.?.files != delta.count()) return error.InvalidLakeIndexCatalog;
        try index.recordDeltaReduction();
        return root;
    }
    fn splitRefs(self: *@This(), refs: []const local.serverless_manifest_artifact_ref.ArtifactRef, depth: u8) ![]const local.serverless_manifest_artifact_ref.ArtifactRef {
        if (artifacts.isPartitioned(refs[0].metadata_version)) return refs;
        const group = try operators.Grouped.create(self.a, self.specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = self.spill });
        defer group.deinit();
        try importContributionsCached(self.a, self.store, refs, self.recipe, group, self.cancellation, self.cached);
        return artifacts.publishPartitionDirectoryAt(self.a, self.out, self.store, self.names, group, self.recipe, self.spill, self.cancellation, depth);
    }
    fn mergePartitions(self: *@This(), left: Leaf, right: Leaf, known: ?[]const local.serverless_manifest_artifact_ref.ArtifactRef, ownership: []std.ArrayList([32]u8), depth: u8, output_proofs: ?[]artifacts.StateProof) anyerror![]local.serverless_manifest_artifact_ref.ArtifactRef {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const Part = artifacts.Partition;
        const lists = try a.alloc(std.ArrayList(Part), self.names.len);
        @memset(lists, .empty);
        const lparts = try a.alloc([]const Part, self.names.len);
        const rparts = try a.alloc([]const Part, self.names.len);
        const current = try a.alloc([]const Part, self.names.len);
        for (self.names, 0..) |_, slot| {
            const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
            const ldir = try artifacts.loadPartitionDirectoryWithCache(a, self.store.*, left.refs[slot], single, self.cancellation, self.cached);
            const rdir = try artifacts.loadPartitionDirectoryWithCache(a, self.store.*, right.refs[slot], single, self.cancellation, self.cached);
            if (ldir.depth != depth or rdir.depth != depth) return error.InvalidNativeAggregateArtifact;
            lparts[slot] = ldir.parts;
            rparts[slot] = rdir.parts;
            if (known) |roots| current[slot] = (try artifacts.loadPartitionDirectoryWithCache(a, self.store.*, roots[slot], single, self.cancellation, self.cached)).parts;
        }
        for (0..artifacts.partitionFanout(depth)) |bucket| {
            const occupied_left = for (lparts[0]) |part| {
                if (part.bucket == bucket) break true;
            } else false;
            const occupied_right = for (rparts[0]) |part| {
                if (part.bucket == bucket) break true;
            } else false;
            if (!occupied_left and !occupied_right) continue;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const pa = scratch.allocator();
            const lrefs = try pa.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
            const rrefs = try pa.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
            const refs = try pa.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, self.names.len);
            const keys = try pa.alloc([32]u8, self.names.len);
            const proofs = try pa.alloc(?artifacts.StateProof, self.names.len);
            @memset(proofs, null);
            const fresh_proofs = try pa.alloc(artifacts.StateProof, self.names.len);
            var left_present = false;
            var right_present = false;
            var count: u64 = 0;
            var complete = true;
            const child_aliases = try pa.alloc(std.ArrayList([32]u8), self.names.len);
            @memset(child_aliases, .empty);
            defer for (child_aliases) |*list| list.deinit(self.out);
            const prior_records = try pa.alloc(?local.metadata_lake_index_catalog.FileContribution, self.names.len);
            @memset(prior_records, null);
            for (self.names, 0..) |name, slot| {
                const lp = for (lparts[slot]) |part| {
                    if (part.bucket == bucket) break part;
                } else null;
                const rp = for (rparts[slot]) |part| {
                    if (part.bucket == bucket) break part;
                } else null;
                if (slot == 0) {
                    left_present = lp != null;
                    right_present = rp != null;
                }
                if (left_present != (lp != null) or right_present != (rp != null)) return error.InvalidNativeAggregateArtifact;
                if (lp) |part| {
                    lrefs[slot] = part.artifact;
                    if (slot == 0) count = part.groups;
                }
                if (rp) |part| {
                    rrefs[slot] = part.artifact;
                    if (slot == 0) count += part.groups;
                }
                if (!left_present or !right_present) continue;
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                hash.update("native-group-partition-v2");
                hash.update(&.{ @intCast(bucket), depth });
                hash.update(lrefs[slot].artifact_id);
                hash.update(rrefs[slot].artifact_id);
                keys[slot] = hash.finalResult();
                const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
                const key = contributionKey(keys[slot], single.fingerprint(), name);
                if (self.old.get(key)) |old| prior_records[slot] = self.previous[old] else if (self.index) |index| prior_records[slot] = try index.lookup(pa, key);
                if (known != null) {
                    const part = for (current[slot]) |part| {
                        if (part.bucket == bucket) break part;
                    } else return error.InvalidNativeAggregateArtifact;
                    refs[slot] = part.artifact;
                    proofs[slot] = part.proof;
                    count = part.groups;
                } else if (prior_records[slot]) |record| refs[slot] = record.artifact else complete = false;
            }
            if (!left_present and !right_present) continue;
            if (left_present and right_present) {
                const nested = artifacts.isPartitioned(lrefs[0].metadata_version) or artifacts.isPartitioned(rrefs[0].metadata_version);
                const missing_ownership = for (prior_records) |record| {
                    if (record == null) break true;
                } else false;
                if (nested and (!complete or missing_ownership)) {
                    if (depth == artifacts.max_partition_depth) return error.InvalidNativeAggregateArtifact;
                    const lsplit = try self.splitRefs(lrefs, depth + 1);
                    const rsplit = try self.splitRefs(rrefs, depth + 1);
                    const merged = try self.mergePartitions(.{ .key = @splat(0), .refs = lsplit }, .{ .key = @splat(0), .refs = rsplit }, if (complete) refs else null, child_aliases, depth + 1, if (complete) null else fresh_proofs);
                    if (!complete) {
                        @memcpy(refs, merged);
                        for (proofs, fresh_proofs) |*proof, fresh| proof.* = fresh;
                    }
                    self.out.free(merged);
                } else if (!complete) {
                    const group = try operators.Grouped.create(self.a, self.specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = self.spill });
                    defer group.deinit();
                    try importContributionsCached(self.a, self.store, lrefs, self.recipe, group, self.cancellation, self.cached);
                    try importContributionsCached(self.a, self.store, rrefs, self.recipe, group, self.cancellation, self.cached);
                    const published = if (depth < artifacts.max_partition_depth and count > artifacts.partition_leaf_groups)
                        try artifacts.publishPartitionedAtProven(self.a, pa, self.store, self.names, group, self.recipe, self.spill, self.cancellation, depth + 1, fresh_proofs)
                    else
                        try artifacts.publishCohortProven(self.a, pa, self.store, self.names, group, self.recipe, self.cancellation, fresh_proofs);
                    @memcpy(refs, published);
                    for (proofs, fresh_proofs) |*proof, fresh| proof.* = fresh;
                    pa.free(published);
                }
                for (refs, keys, self.names, 0..) |ref, key, name, slot| {
                    const one: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
                    // Lookup metadata must outlive the scratch reader arena.
                    const alias = contributionKey(key, one.fingerprint(), name);
                    try ownership[slot].append(self.out, alias);
                    const held: ?[]const [32]u8 = if (prior_records[slot]) |record| record.owned else if (child_aliases[slot].items.len != 0) child_aliases[slot].items else null;
                    const bytes = try std.json.Stringify.valueAlloc(self.out, local.metadata_lake_index_catalog.FileContribution{ .file = key, .recipe = one.fingerprint(), .name = name, .artifact = ref, .owned = held }, .{});
                    defer self.out.free(bytes);
                    try self.contributions.append(self.out, try std.json.parseFromSliceLeaky(local.metadata_lake_index_catalog.FileContribution, self.out, bytes, .{ .allocate = .alloc_always }));
                }
            } else {
                @memcpy(refs, if (left_present) lrefs else rrefs);
                for (proofs, 0..) |*proof, slot| {
                    const parts = if (left_present) lparts[slot] else rparts[slot];
                    for (parts) |part| if (part.bucket == bucket) {
                        proof.* = part.proof;
                        break;
                    };
                }
            }
            for (refs, proofs, 0..) |ref, *proof, slot| {
                const single: recipes.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] };
                if (proof.* == null) proof.* = try self.proof_memo.resolve(self.a, self.store.*, ref, single, self.cancellation);
                if (slot == 0 and left_present and right_present) count = proof.*.?.groups;
                if (proof.*.?.groups != count) return error.InvalidNativeAggregateArtifact;
            }
            for (refs, lists, proofs) |ref, *list, proof| {
                // Root publication follows scratch teardown; retain nested IDs.
                var retained = ref;
                retained.artifact_id = try a.dupe(u8, ref.artifact_id);
                retained.checksum = try a.dupe(u8, ref.checksum);
                retained.name = try a.dupe(u8, ref.name);
                try list.append(a, .{ .bucket = @intCast(bucket), .artifact = retained, .groups = count, .proof = proof, .bounds = proof.?.bounds });
            }
        }
        if (known != null) return self.out.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, 0);
        return artifacts.publishPartitionRootsAtProven(self.a, self.out, self.store, self.names, self.recipe, lists, self.cancellation, depth, output_proofs);
    }
    fn bitAt(key: [32]u8, bit: usize) bool {
        return (key[bit / 8] & (@as(u8, 128) >> @as(u3, @intCast(bit % 8)))) != 0;
    }
};

fn importContributions(a: A, store: *stores.ArtifactStore, refs: []const local.serverless_manifest_artifact_ref.ArtifactRef, recipe: recipes.Recipe, group: *operators.Grouped, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    return importContributionsCached(a, store, refs, recipe, group, cancellation, null);
}
fn importContributionsCached(a: A, store: *stores.ArtifactStore, refs: []const local.serverless_manifest_artifact_ref.ArtifactRef, recipe: recipes.Recipe, group: *operators.Grouped, cancellation: @import("antfly_cancellation").CancellationToken, cached: ?artifacts.CachedRead) !void {
    var readers: std.ArrayList(*artifacts.Reader) = .empty;
    defer {
        for (readers.items) |reader| reader.cursor().close(reader);
        readers.deinit(a);
    }
    for (refs, 0..) |ref, slot| {
        const single: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] };
        const reader = try artifacts.Reader.openWithCache(a, store.*, ref, single, cancellation, cached);
        var retained = false;
        defer if (!retained) reader.cursor().close(reader);
        try reader.setOutputSlot(@intCast(slot));
        const fused = for (readers.items) |existing| {
            if (try existing.fuse(reader, @intCast(slot))) break true;
        } else false;
        if (!fused) {
            try readers.append(a, reader);
            retained = true;
        }
    }
    // A shared cohort block is decoded once for all authenticated slots, even
    // when inherited leaves originated in a different cohort composition.
    for (readers.items) |reader| while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const rows = (try reader.cursor().next(reader, page.allocator(), 256)) orelse break;
        try group.importPartialBatch(rows);
    };
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
        for (keys, recipe.keys) |*key, definition| key.* = .{ .columns = .{ .page = .{ .batch = batch, .selection = selection }, .definitions = try pa.dupe(local.sql_scalar.Column, &.{definition.definition()}) } };
        const input_batches = try pa.alloc(local.sql_execution_batch.Batch, recipe.inputs.len);
        for (input_batches, recipe.inputs) |*input, definition| input.* = if (definition.column) |column| .{ .columns = .{ .page = .{ .batch = batch, .selection = selection }, .definitions = try pa.dupe(local.sql_scalar.Column, &.{column.definition()}) } } else constant: {
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
    try std.testing.expectEqual(.int64, recipe.inputs[0].spec.input_element.?);
    // Exact SQL width is part of recipe identity, including explicit widths
    // whose reducer behavior differs from the default (REAL and NUMERIC).
    for ([_]local.sql_catalog.Column{
        .{ .name = "amount", .path = "amount", .type = .integer, .element_type = .int16 },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .float32 },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .numeric },
        .{ .name = "amount", .path = "amount", .type = .number },
    }) |typed_column| {
        const typed_table: local.sql_catalog.Table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{ table.columns[0], typed_column } };
        const typed = (try recipeFor(a, typed_table, config, mat)).?;
        var expression: local.sql_ast.Scalar = .{ .column = "amount" };
        var program = try local.sql_scalar.bind(a, &expression, &.{.{ .name = "amount", .type = typed_column.type, .element_type = typed_column.element_type }}, &.{}, .{});
        defer program.deinit();
        try std.testing.expectEqual(program.output_type.element_type, typed.inputs[0].spec.input_element);
        try std.testing.expect(!recipe.eql(typed));
    }
    const star = (try recipeFor(a, table, config, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"rows\",\"op\":\"count\"}", .{}))).?;
    const column = (try recipeFor(a, table, config, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"values\",\"op\":\"count\",\"measure\":\"value\"}", .{}))).?;
    try std.testing.expect(!star.eql(column));
    const unsupported = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"joined\",\"op\":\"sum\",\"measure\":\"value\",\"join\":\"lookup\"}", .{});
    try std.testing.expect((try recipeFor(a, table, config, unsupported)) == null);
}

test "external lake native algebraic recipes match bound SQL scalar identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const config = try std.json.parseFromSliceLeaky(std.json.Value, a, "{}", .{});
    const mat = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"name\":\"total\",\"op\":\"sum\",\"measure\":\"amount\"}", .{});
    var compiled = try local.sql_compiler.compile(a, "SELECT SUM(amount) FROM lake", .{});
    defer compiled.deinit();
    for ([_]local.sql_catalog.Column{
        .{ .name = "amount", .path = "amount", .type = .integer },
        .{ .name = "amount", .path = "amount", .type = .integer, .element_type = .int16 },
        .{ .name = "amount", .path = "amount", .type = .integer, .element_type = .int32 },
        .{ .name = "amount", .path = "amount", .type = .integer, .element_type = .int64 },
        .{ .name = "amount", .path = "amount", .type = .number },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .float32 },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .float64 },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .numeric },
    }) |column| {
        const table: local.sql_catalog.Table = .{ .id = 1, .physical_name = "lake", .schema_version = 1, .columns = &.{column} };
        const bound = try local.sql_aggregate_binding.bind(a, table, compiled.statement.select, &.{});
        const sql_recipe = (try recipes.fromBound(a, table, bound)).?;
        const published_recipe = (try recipeFor(a, table, config, mat)).?;
        try std.testing.expect(published_recipe.eql(sql_recipe));
        try std.testing.expectEqualSlices(u8, &sql_recipe.fingerprint(), &published_recipe.fingerprint());
    }
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
    const spec: operators.AggregateSpec = .{ .kind = .sum, .input_type = .integer, .input_element = .int64 };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = .{ .path = "amount", .type = .integer, .nullable = true, .element_type = .int64 } }} };
    const sum_identity = try recipeIdentity(a, recipe);
    defer a.free(sum_identity);
    const sum_declaration = for (declarations) |declaration| {
        if (std.mem.eql(u8, declaration.binding.index_config_hash, sum_identity)) break declaration;
    } else return error.InvalidNativeAggregateArtifact;
    const sum_reader = try artifacts.Reader.open(a, store, sum_declaration.artifact, recipe, .none);
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
    // A count-only Parquet publication must not decode a single value row.
    // The same file's normal aggregate path exceeds this explicit row limit.
    var count_provider = provider;
    count_provider.limits.max_examined_rows = 0;
    var count_table = table;
    count_table.indexes_json = "{\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]}}";
    const count_only = try buildIncremental(a, output.allocator(), count_table, &source, &store, &count_provider, .none, &.{}, &.{});
    try std.testing.expectEqual(@as(usize, 1), count_only.declarations.len);
    try std.testing.expectError(error.LakeRowsScanBudgetExceeded, buildIncremental(a, output.allocator(), table, &source, &store, &count_provider, .none, &.{}, &.{}));
    const count_recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = .{ .kind = .count }, .column = null }} };
    const count_identity = try recipeIdentity(a, count_recipe);
    defer a.free(count_identity);
    const count_declaration = for (declarations) |declaration| {
        if (std.mem.eql(u8, declaration.binding.index_config_hash, count_identity)) break declaration;
    } else return error.InvalidNativeAggregateArtifact;
    const count_reader = try artifacts.Reader.open(a, store, count_declaration.artifact, count_recipe, .none);
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
    const metadata_reader = try artifacts.Reader.open(a, store, count_only.declarations[0].artifact, count_recipe, .none);
    const metadata_cursor = metadata_reader.cursor();
    defer metadata_cursor.close(metadata_cursor.ptr);
    const metadata_counts = (try metadata_cursor.next(metadata_cursor.ptr, page.allocator(), 32)).?;
    var metadata_exact = try local.sql_aggregate_partial.read(page.allocator(), metadata_counts[0], 0, .{ .kind = .count });
    defer metadata_exact.deinit();
    try std.testing.expectEqual(@as(u64, 3), metadata_exact.count);
    const counts = (try count_cursor.next(count_cursor.ptr, page.allocator(), 32)).?;
    var exact = try local.sql_aggregate_partial.read(page.allocator(), counts[0], 0, .{ .kind = .count });
    defer exact.deinit();
    try std.testing.expectEqual(@as(u64, 3), exact.count);
    // Serving reads a small directory without hydrating build-only records.
    const directories = @import("lake_index_directory.zig");
    const page_records = try output.allocator().alloc(local.metadata_lake_index_catalog.FileContribution, 257);
    @memset(page_records, built.contributions[0]);
    const directory_ref = try directories.publishWithContributions(output.allocator(), &store, declarations, page_records, .none);
    const document = try directories.loadDocument(output.allocator(), store, .{ .kind = .external_base_source, .artifact_id = directory_ref.artifact_id, .checksum = directory_ref.checksum, .byte_len = directory_ref.byte_len }, .none, null);
    try std.testing.expectEqual(@as(usize, 2), document.contribution_pages.len);
    try std.testing.expectEqual(@as(usize, 0), document.file_contributions.len);
    try std.testing.expectEqual(@as(usize, 256), (try directories.loadContributionPage(output.allocator(), store, document.contribution_pages[0], .none, null)).len);
    try std.testing.expectEqual(@as(usize, 1), (try directories.loadContributionPage(output.allocator(), store, document.contribution_pages[1], .none, null)).len);
    var corrupted = document.contribution_pages[0];
    corrupted.checksum = document.contribution_pages[1].checksum;
    try std.testing.expectError(error.InvalidArtifactId, directories.loadContributionPage(output.allocator(), store, corrupted, .none, null));
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

test "external lake aggregate radix reductions reuse unchanged subtrees across append and removal" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-tree");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 1, 2, 7 }, .field_id = 1 }});
    defer a.free(data);
    for (0..16) |index| {
        const key = try std.fmt.allocPrint(a, "part-{d}.parquet", .{index});
        defer a.free(key);
        var put = try client.putObject("antfly", key, data, .{});
        put.deinit(a);
    }
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"amount\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    const root = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(root);
    var fs_artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, root);
    defer fs_artifacts.deinit();
    var store = fs_artifacts.artifactStore();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema, .indexes_json = "{\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"total\",\"op\":\"sum\",\"measure\":\"amount\"},{\"name\":\"rows\",\"op\":\"count\"},{\"name\":\"low\",\"op\":\"min\",\"measure\":\"amount\"},{\"name\":\"high\",\"op\":\"max\",\"measure\":\"amount\"}]}}" };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    var previous: BuildResult = .{ .declarations = &.{}, .contributions = &.{} };
    const Denied = struct {
        var base: local.storage_object_storage.ObjectStorage = undefined;
        var append: bool = false;
        fn get(_: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
            if (!append or !std.mem.eql(u8, key, "part-16.parquet")) return error.UnexpectedUnchangedParquetRead;
            var client_copy = base;
            client_copy.allocator = alloc;
            return client_copy.getObject(bucket, key, options);
        }
    };
    for (0..3) |phase| {
        if (phase == 1) {
            var put = try client.putObject("antfly", "part-16.parquet", data, .{});
            put.deinit(a);
        } else if (phase == 2) try client.deleteObject("antfly", "part-0.parquet", .{});
        var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        var denied = source.scanner.object_reader.client.vtable.*;
        Denied.base = source.scanner.object_reader.client;
        Denied.append = phase == 1;
        denied.get_object = Denied.get;
        if (phase != 0) source.scanner.object_reader.client.vtable = &denied;
        var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
        var replay = @import("lake_index_build_replay.zig").Replay.init(a, &provider, &.{"amount"});
        defer replay.deinit();
        provider.replay = &replay;
        if (phase != 0) {
            const changed = (try @import("lake_index_build_replay.zig").changedFiles(a, output.allocator(), &provider, store, previous.declarations, previous.contributions, .none)).?;
            replay.only_files = changed;
            const count = changed.files.cardinality();
            try std.testing.expectEqual(@as(usize, if (phase == 1) 1 else 0), count);
        }
        const built = try buildIncremental(a, output.allocator(), table, &source, &store, &provider, .none, &.{}, previous.contributions);
        // Fresh 16-file leaves execute through independent parallel streams;
        // the prior null-file replay hook could never admit this work.
        if (phase == 0) try std.testing.expectEqual(@as(usize, 0), replay.captures);
        try std.testing.expectEqual(@as(usize, 4), built.declarations.len);
        try std.testing.expectEqual((2 * source.inventory.files.len - 1) * 4, built.contributions.len);
        if (phase != 0) {
            var reused_nodes: usize = 0;
            for (built.contributions) |current| {
                const leaf = for (source.inventory.files) |file| {
                    if (std.mem.eql(u8, &current.file, &fileIdentity(&source, file))) break true;
                } else false;
                if (leaf) continue;
                for (previous.contributions) |old| if (std.mem.eql(u8, current.artifact.artifact_id, old.artifact.artifact_id)) {
                    reused_nodes += 1;
                    break;
                };
            }
            try std.testing.expect(reused_nodes != 0);
        }
        for (built.declarations, 0..) |declaration, slot| {
            const recipe = try artifacts.loadRecipe(output.allocator(), store, declaration.artifact, .none);
            const reader = try artifacts.Reader.open(a, store, declaration.artifact, recipe, .none);
            defer reader.cursor().close(reader);
            const rows = (try reader.cursor().next(reader, output.allocator(), 8)).?;
            const group = try operators.Grouped.create(a, &.{recipe.inputs[0].spec}, .{});
            defer group.deinit();
            try group.importPartialBatch(rows[0..1]);
            const result = (try group.nextResult(output.allocator())).?;
            const files: i64 = if (phase == 1) 17 else 16;
            try std.testing.expectEqual(@as(i64, switch (slot) {
                0 => files * 10,
                1 => files * 3,
                2 => 1,
                else => 7,
            }), result.aggregates[0].value.integer);
        }
        previous = built;
    }
}

test "external lake partitioned aggregate trees retain unaffected groups with lazy keyed contributions" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-group-tree");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    var amounts: [300]i64 = undefined;
    for (&amounts, 1..) |*value, ordinal| value.* = @intCast(ordinal);
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &amounts, .field_id = 1 }});
    defer a.free(data);
    for (0..16) |index| {
        const key = try std.fmt.allocPrint(a, "part-{d}.parquet", .{index});
        defer a.free(key);
        var put = try client.putObject("antfly", key, data, .{});
        put.deinit(a);
    }
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"amount\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    const root = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(root);
    var fs_artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, root);
    defer fs_artifacts.deinit();
    var store = fs_artifacts.artifactStore();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema, .indexes_json = "{\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"total\",\"group_by\":[\"amount\"],\"op\":\"sum\",\"measure\":\"amount\"},{\"name\":\"rows\",\"group_by\":[\"amount\"],\"op\":\"count\"},{\"name\":\"low\",\"group_by\":[\"amount\"],\"op\":\"min\",\"measure\":\"amount\"},{\"name\":\"high\",\"group_by\":[\"amount\"],\"op\":\"max\",\"measure\":\"amount\"}]}}" };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    var previous: BuildResult = .{ .declarations = &.{}, .contributions = &.{} };
    var previous_root: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null;
    var previous_roots: []const [32]u8 = &.{};
    const navigation_api = @import("lake_source_navigation.zig");
    var previous_navigation: ?navigation_api.Root = null;
    const Denied = struct {
        var base: local.storage_object_storage.ObjectStorage = undefined;
        var step: usize = 0;
        fn get(_: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
            const allowed = (step == 1 and std.mem.eql(u8, key, "part-16.parquet")) or (step == 3 and (std.mem.eql(u8, key, "part-17.parquet") or std.mem.eql(u8, key, "part-18.parquet")));
            if (!allowed) return error.UnexpectedUnchangedParquetRead;
            var client_copy = base;
            client_copy.allocator = alloc;
            return client_copy.getObject(bucket, key, options);
        }
    };
    for (0..4) |phase| {
        if (phase == 1) {
            const appended = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{1}, .field_id = 1 }});
            defer a.free(appended);
            var put = try client.putObject("antfly", "part-16.parquet", appended, .{});
            put.deinit(a);
        } else if (phase == 2) try client.deleteObject("antfly", "part-0.parquet", .{}) else if (phase == 3) {
            try client.deleteObject("antfly", "part-1.parquet", .{});
            try client.deleteObject("antfly", "part-2.parquet", .{});
            const appended = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{1}, .field_id = 1 }});
            defer a.free(appended);
            for ([_][]const u8{ "part-17.parquet", "part-18.parquet" }) |name| {
                var put = try client.putObject("antfly", name, appended, .{});
                put.deinit(a);
            }
        }
        var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        var denied = source.scanner.object_reader.client.vtable.*;
        Denied.base = source.scanner.object_reader.client;
        Denied.step = phase;
        denied.get_object = Denied.get;
        if (phase != 0) source.scanner.object_reader.client.vtable = &denied;
        var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
        store.upload_scope = try stores.UploadScope.forPublication(@splat(9), phase + 1, std.testing.io);
        var index: @import("lake_index_contributions.zig").Index = undefined;
        try index.init(a, store, previous_root, .none);
        defer index.deinit();
        index.counted = true;
        index.prior_roots = previous_roots;
        if (previous_navigation) |prior| index.predecessor = .{ .source = prior.source, .schema = prior.schema, .snapshot = prior.snapshot, .count = prior.count };
        var navigation = try navigation_api.build(a, output.allocator(), &store, &provider, previous_navigation, .none);
        defer navigation.deinit();
        provider.native_delta = navigation.delta;
        if (phase != 0) {
            var changed = (try @import("lake_index_build_replay.zig").changedFilesIndexed(a, output.allocator(), &provider, store, previous.declarations, &.{}, .none, &index)).?;
            defer changed.deinit();
            const count = changed.files.cardinality();
            try std.testing.expectEqual(@as(usize, if (phase == 1) 1 else if (phase == 3) 2 else 0), count);
        }
        const built = try buildIndexed(a, output.allocator(), table, &source, &store, &provider, .none, &.{}, &.{}, &index);
        try std.testing.expectEqual(@as(usize, 4), built.declarations.len);
        if (phase != 0) try std.testing.expect(index.delta_reductions > 0);
        previous_navigation = try navigation_api.load(output.allocator(), store, navigation.ref, .none, null);
        if (phase == 0) try std.testing.expect(built.contributions.len >= (2 * source.inventory.files.len - 1) * 4) else try std.testing.expect(built.contributions.len < previous_root.?.records);
        previous_root = try index.update(built.contributions);
        previous_roots = try index.rootKeys(output.allocator());
        const unchanged = try index.update(built.contributions);
        try std.testing.expect(previous_root.?.eql(unchanged.?));
        try std.testing.expect(previous_root.?.records >= (2 * source.inventory.files.len - 1) * 4);
        // A no-change refresh may read authenticated contribution pages, but
        // must not open even one aggregate/range directory or payload block.
        const MetadataOnly = struct {
            var base: stores.ArtifactStore = undefined;
            fn get(_: *anyopaque, alloc: A, id: []const u8) ![]u8 {
                var read = base;
                const bytes = try read.getAllocWithCancellationUsingAllocator(alloc, id, .none);
                if (!std.mem.startsWith(u8, bytes, "AFGPT003")) {
                    alloc.free(bytes);
                    return error.UnexpectedUnchangedAggregateRead;
                }
                return bytes;
            }
        };
        MetadataOnly.base = store;
        var metadata_vtable = store.vtable.*;
        metadata_vtable.get_alloc = MetadataOnly.get;
        metadata_vtable.get_alloc_with_cancellation = null;
        var metadata_store = store;
        metadata_store.vtable = &metadata_vtable;
        var retained_index: @import("lake_index_contributions.zig").Index = undefined;
        try retained_index.init(a, metadata_store, previous_root, .none);
        defer retained_index.deinit();
        retained_index.counted = true;
        retained_index.prior_roots = previous_roots;
        retained_index.predecessor = .{ .source = source.inventory.source_id, .schema = source.inventory.schema_fingerprint, .snapshot = source.inventory.snapshot_id, .count = @intCast(source.inventory.files.len) };
        var unchanged_delta = navigation.delta;
        unchanged_delta.prior_snapshot = source.inventory.snapshot_id;
        unchanged_delta.prior_count = @intCast(source.inventory.files.len);
        unchanged_delta.selection = null;
        unchanged_delta.files = try output.allocator().alloc(@import("lake_index_native_state.zig").File, source.inventory.files.len);
        unchanged_delta.changed = try output.allocator().alloc(bool, source.inventory.files.len);
        @memset(unchanged_delta.changed, false);
        unchanged_delta.prior_changed = &.{};
        for (unchanged_delta.files, 0..) |*file, ordinal| file.* = try navigation.delta.file(ordinal);
        provider.native_delta = unchanged_delta;
        const retained_build = try buildIndexed(a, output.allocator(), table, &source, &metadata_store, &provider, .none, &.{}, &.{}, &retained_index);
        try std.testing.expectEqual(@as(usize, 0), retained_build.contributions.len);
        try std.testing.expect(retained_index.delta_reductions > 0);
        const reads_before = retained_index.reads;
        const retained_root = (try retained_index.update(retained_build.contributions)).?;
        try std.testing.expectEqual(reads_before, retained_index.reads);
        try std.testing.expect(previous_root.?.eql(retained_root));
        for (built.declarations, retained_build.declarations) |before, after| try std.testing.expectEqualStrings(before.artifact.artifact_id, after.artifact.artifact_id);
        for (built.declarations, 0..) |declaration, slot| {
            const recipe = try artifacts.loadRecipe(output.allocator(), store, declaration.artifact, .none);
            const reader = try artifacts.Reader.open(a, store, declaration.artifact, recipe, .none);
            defer reader.cursor().close(reader);
            try std.testing.expectEqual(@as(u16, 5), declaration.artifact.metadata_version);
            if (phase == 1) {
                const before = try artifacts.loadPartitions(output.allocator(), store, previous.declarations[slot].artifact, recipe, .none);
                const after = try artifacts.loadPartitions(output.allocator(), store, declaration.artifact, recipe, .none);
                var retained: usize = 0;
                for (before) |old| for (after) |current| {
                    if (std.mem.eql(u8, old.artifact.artifact_id, current.artifact.artifact_id)) retained += 1;
                };
                try std.testing.expect(retained >= 1);
            }
            const group = try operators.Grouped.create(a, &.{recipe.inputs[0].spec}, .{});
            defer group.deinit();
            while (try reader.cursor().next(reader, output.allocator(), 8)) |rows| try group.importPartialBatch(rows);
            var count: usize = 0;
            while (try group.nextResult(output.allocator())) |result| {
                const key = result.keys[0].value.integer;
                const files: i64 = if (phase == 0) 16 else if (phase == 1) (if (key == 1) @as(i64, 17) else 16) else (if (key == 1) @as(i64, 16) else if (phase == 3) 13 else 15);
                try std.testing.expectEqual(@as(i64, switch (slot) {
                    0 => files * key,
                    1 => files,
                    else => key,
                }), result.aggregates[0].value.integer);
                count += 1;
            }
            try std.testing.expectEqual(@as(usize, 300), count);
        }
        previous = built;
    }
}

test "external lake recursive group partitions retain sibling artifacts on a single group update" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("recursive-group-delta");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @import("lake_index_publication.zig").uploadDomain(4, @splat(9)), .attempt = @splat(1) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "id", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = specs[0], .column = null }, .{ .spec = specs[1], .column = .{ .path = "amount", .type = .integer, .nullable = false } } } };
    const base = try operators.Grouped.create(a, &specs, .{});
    defer base.deinit();
    var first: i64 = -1;
    var candidate: i64 = 0;
    var count: usize = 0;
    while (count < 1400) : (candidate += 1) {
        var bytes: [9]u8 = @splat(0);
        std.mem.writeInt(u64, bytes[1..9], try local.sql_scalar.semanticHash(.{ .integer = candidate }), .little);
        var hash = std.hash.Wyhash.init(0);
        hash.update(&bytes);
        if (hash.final() % 64 != 0) continue;
        if (first < 0) first = candidate;
        try base.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = candidate })}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 1 }), local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }) });
        count += 1;
    }
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const names = &.{ "stats.rows", "stats.sum" };
    const single: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] };
    const prior = try artifacts.publishPartitioned(a, out, &store, names, base, recipe, &manager, .none);
    try std.testing.expectEqual(@as(usize, 1), manager.peak_files);
    try std.testing.expectEqual(@as(usize, 0), manager.merges);

    const delta = try operators.Grouped.create(a, &specs, .{});
    defer delta.deinit();
    try delta.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = first })}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 1 }), local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }) });
    const fresh = try artifacts.publishPartitionDirectoryAt(a, out, &store, names, delta, recipe, &manager, .none, 0);
    var old: ContributionIndex = .empty;
    defer old.deinit(a);
    var contributions: std.ArrayList(local.metadata_lake_index_catalog.FileContribution) = .empty;
    var reduction: Reduction = .{ .a = a, .out = out, .store = &store, .recipe = recipe, .specs = &specs, .names = names, .old = &old, .previous = &.{}, .contributions = &contributions, .spill = &manager, .cancellation = .none };
    defer reduction.proof_memo.deinit(a);
    var ownership = [_]std.ArrayList([32]u8){ .empty, .empty };
    defer for (&ownership) |*list| list.deinit(out);
    const merged = try reduction.mergePartitions(.{ .key = @splat(1), .refs = prior }, .{ .key = @splat(2), .refs = fresh }, null, &ownership, 0, null);
    const before_root = try artifacts.loadPartitions(out, store, prior[0], single, .none);
    const after_root = try artifacts.loadPartitions(out, store, merged[0], single, .none);
    try std.testing.expectEqual(@as(usize, 1), before_root.len);
    try std.testing.expectEqual(@as(u16, 5), before_root[0].artifact.metadata_version);
    const before = try artifacts.loadPartitions(out, store, before_root[0].artifact, single, .none);
    const after = try artifacts.loadPartitions(out, store, after_root[0].artifact, single, .none);
    var reused: usize = 0;
    for (before) |l| for (after) |r| {
        if (std.mem.eql(u8, l.artifact.artifact_id, r.artifact.artifact_id)) reused += 1;
    };
    try std.testing.expect(before.len > 1);
    try std.testing.expectEqual(before.len - 1, reused);
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = out, .table = 4, .authority = undefined, .store = store, .identity = @splat(9), .context = .{}, .token = @splat(7), .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = 16 * 1024 * 1024 };
    try collector.markArtifact(out, merged[0]);
    try std.testing.expect(collector.marked.contains(merged[0].artifact_id));
    const graph = try artifacts.partitionReferences(out, store, merged[0], .none);
    try std.testing.expect(graph.cohort != null);
    try std.testing.expect(collector.marked.contains(graph.cohort.?.artifact_id));
    for (after) |part| try std.testing.expect(collector.marked.contains(part.artifact.artifact_id));
    var index: @import("lake_index_contributions.zig").Index = undefined;
    try index.init(a, store, null, .none);
    defer index.deinit();
    index.counted = true;
    for (ownership) |list| for (list.items) |key| try index.includeRoot(key);
    _ = try index.update(contributions.items);
    for (contributions.items) |value| if (value.owned) |owned| {
        try std.testing.expect(owned.len <= 64);
    };
    const reader = try artifacts.Reader.open(a, store, merged[0], single, .none);
    defer reader.cursor().close(reader);
    const sum_reader = try artifacts.Reader.open(a, store, merged[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none);
    defer sum_reader.cursor().close(sum_reader);
    try reader.setOutputSlot(0);
    try std.testing.expect(try reader.fuse(sum_reader, 1));
    const grouped = try operators.Grouped.create(a, &specs, .{});
    defer grouped.deinit();
    while (try reader.cursor().next(reader, out, 71)) |rows| for (rows) |row| try grouped.importPartialBatch(&.{row});
    count = 0;
    while (try grouped.nextResult(out)) |row| {
        try std.testing.expectEqual(@as(i64, if (row.keys[0].value.integer == first) 2 else 1), row.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, if (row.keys[0].value.integer == first) 18014398509481986 else 9007199254740993), row.aggregates[1].value.integer);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1400), count);
}

test "external lake exact numeric incremental recipes preserve floating fallback" {
    const key: recipes.Key = .{ .path = "k", .type = .number, .nullable = true, .element_type = .numeric };
    for ([_]operators.Aggregate.Kind{ .count, .sum, .avg, .min, .max }) |kind| {
        const input: recipes.Input = .{ .spec = .{ .kind = kind, .input_type = .number, .input_element = .numeric }, .column = key };
        try std.testing.expect(incrementalRecipe(.{ .keys = &.{key}, .inputs = &.{input} }));
        var floating = input;
        floating.spec.input_element = .float64;
        if (kind != .count) try std.testing.expect(!incrementalRecipe(.{ .keys = &.{}, .inputs = &.{floating} }));
        var distinct = input;
        distinct.spec.distinct = true;
        try std.testing.expect(!incrementalRecipe(.{ .keys = &.{}, .inputs = &.{distinct} }));
    }
    var floating_key = key;
    floating_key.element_type = .float64;
    try std.testing.expect(!incrementalRecipe(.{ .keys = &.{floating_key}, .inputs = &.{.{ .spec = .{ .kind = .count }, .column = null }} }));
}

test "external lake exact numeric radix updates reuse unchanged Parquet through append and removal" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("numeric-incremental-radix");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "k", .field_id = 1, .converted_type = 0, .values = &.{ "1.0", "1.00", "2.000", "NaN", "NaN" } },
        .{ .column_id = "amount", .field_id = 2, .converted_type = 0, .values = &.{ "9007199254740993.1", "-9007199254740993.0", "0.01", "Infinity", "-Infinity" } },
    });
    defer a.free(data);
    for (0..4) |index| {
        const key = try std.fmt.allocPrint(a, "part-{d}.parquet", .{index});
        defer a.free(key);
        var uploaded = try client.putObject("antfly", key, data, .{});
        uploaded.deinit(a);
    }
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"k":{{"type":"number","x-antfly-sql-type":"numeric"}},"amount":{{"type":"number","x-antfly-sql-type":"numeric"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    const path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(4), .attempt = @splat(1) };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema, .indexes_json =
        \\{"stats":{"type":"algebraic","materializations":[{"name":"count","op":"count","group_by":["k"]},{"name":"sum","op":"sum","measure":"amount","group_by":["k"]},{"name":"avg","op":"avg","measure":"amount","group_by":["k"]},{"name":"min","op":"min","measure":"amount","group_by":["k"]},{"name":"max","op":"max","measure":"amount","group_by":["k"]}]}}
    };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const out = output.allocator();
    const Denied = struct {
        var base: local.storage_object_storage.ObjectStorage = undefined;
        var append: bool = false;
        fn get(_: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
            if (!append or !std.mem.eql(u8, key, "part-4.parquet")) return error.UnexpectedUnchangedParquetRead;
            var copy = base;
            copy.allocator = alloc;
            return copy.getObject(bucket, key, options);
        }
        fn number(alloc: A, text: []const u8) !local.sql_scalar.Datum {
            var work: local.sql_array_value.Budget = .{};
            return local.sql_scalar.numericTextLeaky(alloc, text, &work);
        }
    };
    var cumulative: @import("lake_index_row_source.zig").Provider.ResumeWork = .{};
    var previous: BuildResult = .{ .declarations = &.{}, .contributions = &.{} };
    for (0..3) |phase| {
        if (phase == 1) {
            var uploaded = try client.putObject("antfly", "part-4.parquet", data, .{});
            uploaded.deinit(a);
        } else if (phase == 2) try client.deleteObject("antfly", "part-0.parquet", .{});
        var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        var denied = source.scanner.object_reader.client.vtable.*;
        Denied.base = source.scanner.object_reader.client;
        Denied.append = phase == 1;
        denied.get_object = Denied.get;
        if (phase != 0) source.scanner.object_reader.client.vtable = &denied;
        var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .resume_work = &cumulative };
        var replay = @import("lake_index_build_replay.zig").Replay.init(a, &provider, &.{ "k", "amount" });
        defer replay.deinit();
        provider.replay = &replay;
        if (phase != 0) {
            replay.only_files = (try @import("lake_index_build_replay.zig").changedFiles(a, out, &provider, store, previous.declarations, previous.contributions, .none)).?;
            try std.testing.expectEqual(@as(usize, if (phase == 1) 1 else 0), replay.only_files.?.files.cardinality());
        }
        const built = try buildIncremental(a, out, table, &source, &store, &provider, .none, &.{}, previous.contributions);
        const files = source.inventory.files.len;
        try std.testing.expectEqual(@as(usize, 5), built.declarations.len);
        try std.testing.expect(built.contributions.len >= files * 5);
        if (phase != 0) {
            var reused_leaves: usize = 0;
            for (built.contributions) |current| {
                const leaf = for (source.inventory.files) |file| {
                    if (std.mem.eql(u8, &current.file, &fileIdentity(&source, file))) break true;
                } else false;
                if (!leaf) continue;
                for (previous.contributions) |old| if (std.mem.eql(u8, &current.file, &old.file) and std.mem.eql(u8, current.artifact.artifact_id, old.artifact.artifact_id)) {
                    reused_leaves += 1;
                    break;
                };
            }
            try std.testing.expectEqual(@as(usize, 20), reused_leaves);
        }
        for (built.declarations, 0..) |declaration, slot| {
            const recipe = try artifacts.loadRecipe(out, store, declaration.artifact, .none);
            try std.testing.expect(incrementalRecipe(recipe));
            const reader = try artifacts.Reader.open(a, store, declaration.artifact, recipe, .none);
            defer reader.cursor().close(reader);
            const group = try operators.Grouped.create(a, &.{recipe.inputs[0].spec}, .{});
            defer group.deinit();
            while (try reader.cursor().next(reader, out, 8)) |rows| try group.importPartialBatch(rows);
            var groups: usize = 0;
            while (try group.nextResult(out)) |row| {
                groups += 1;
                const nan = row.keys[0].numeric.?.kind == .nan;
                const one = !nan and (try local.sql_scalar.compareDatums(row.keys[0], try Denied.number(out, "1"))) == .eq;
                if (slot == 0) {
                    try std.testing.expectEqual(@as(i64, @intCast(files * if (one or nan) @as(usize, 2) else 1)), row.aggregates[0].value.integer);
                } else {
                    const expected = if (nan) switch (slot) {
                        3 => "-Infinity",
                        4 => "Infinity",
                        else => "NaN",
                    } else if (one) switch (slot) {
                        1 => try std.fmt.allocPrint(out, "0.{d}", .{files}),
                        2 => "0.05",
                        3 => "-9007199254740993.0",
                        else => "9007199254740993.1",
                    } else if (slot == 1) try std.fmt.allocPrint(out, "0.0{d}", .{files}) else "0.01";
                    try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(row.aggregates[0], try Denied.number(out, expected)));
                }
            }
            try std.testing.expectEqual(@as(usize, 3), groups);
        }
        previous = built;
    }
    try std.testing.expectEqual(@as(u64, 25), cumulative.aggregate.rows);
}
