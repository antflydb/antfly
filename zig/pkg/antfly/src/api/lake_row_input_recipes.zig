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

//! Index input recipes are shared by construction and relocation proof. Raw
//! row hashes never certify an uncaptured external enrichment dependency.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub const Recipe = struct {
    name: []const u8,
    /// Semantic input cohort; shared across declarations with identical inputs.
    digest: [32]u8,
    columns: []const []const u8,
    reusable: bool = true,
    /// Declaration semantics are bound separately from per-row input evidence.
    binding: ?[32]u8 = null,
    pub fn jsonStringify(self: Recipe, writer: anytype) !void {
        if (self.binding) |binding| try writer.write(.{ .name = self.name, .digest = self.digest, .columns = self.columns, .reusable = self.reusable, .binding = binding }) else try writer.write(.{ .name = self.name, .digest = self.digest, .columns = self.columns, .reusable = self.reusable });
    }
};
pub fn count(recipes: []const Recipe) usize {
    // Root admission caps declarations at 4096. Keep validation allocation-free
    // without repeatedly comparing every declaration to all its predecessors.
    if (recipes.len > local.metadata_lake_index_catalog.max_directory_artifacts) return recipes.len;
    const absent = std.math.maxInt(u16);
    var slots: [8192]u16 = @splat(absent);
    var n: usize = 0;
    for (recipes, 0..) |recipe, i| {
        var slot = std.hash.Wyhash.hash(0, &recipe.digest) % slots.len;
        while (slots[slot] != absent) : (slot = (slot + 1) % slots.len) {
            if (std.mem.eql(u8, &recipes[slots[slot]].digest, &recipe.digest)) break;
        }
        if (slots[slot] == absent) {
            slots[slot] = @intCast(i);
            n += 1;
        }
    }
    return n;
}
pub fn unique(a: A, recipes: []const Recipe) ![]Recipe {
    var seen: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer seen.deinit(a);
    var result: std.ArrayList(Recipe) = .empty;
    errdefer result.deinit(a);
    for (recipes) |recipe| {
        const entry = try seen.getOrPut(a, recipe.digest);
        if (!entry.found_existing) try result.append(a, recipe);
    }
    return result.toOwnedSlice(a);
}
/// Stable row-value domain, independent of declaration names/configurations.
/// A genuinely new input projection or schema still requires fresh evidence.
pub fn valuesRecipe(a: A, schema_json: []const u8, columns: []const []const u8) ![32]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const schema = try @import("lake_index_dependencies.zig").project(ca, schema_json, columns);
    const encoded = try std.json.Stringify.valueAlloc(ca, .{ .format = "native-lake-value-projection-v2", .schema = schema, .columns = columns }, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded, &digest, .{});
    return digest;
}
pub fn sameCohorts(a: A, left: []const Recipe, right: []const Recipe) !bool {
    var remaining: std.AutoHashMapUnmanaged([32]u8, bool) = .empty;
    defer remaining.deinit(a);
    for (left) |recipe| try remaining.put(a, recipe.digest, false);
    for (right) |recipe| {
        const entry = remaining.getPtr(recipe.digest) orelse return false;
        entry.* = true;
    }
    var seen = remaining.valueIterator();
    while (seen.next()) |value| if (!value.*) return false;
    return true;
}
pub fn configurationReusable(config: std.json.Value) bool {
    if (config != .object) return false;
    if (@import("lake_enrichment_units.zig").configured(config, "embedder") != null) return false;
    if (config.object.get("enrichments")) |value| if (value != .null and (value != .array or value.array.items.len != 0)) return false;
    if (config.object.get("source")) |value| if (value == .object and value.object.contains("artifact")) return false;
    if (config.object.get("sources")) |value| if (value == .array) for (value.array.items) |source| {
        if (source == .object and source.object.contains("artifact")) return false;
    };
    return true;
}
/// Share one eligibility decision before capturing payloads and before
/// preparing native fragments. Uncaptured enrichments/legacy builders cannot
/// consume the capture and must not pay its temporary-write cost.
pub fn nativeFragmentsEligible(configs: std.json.Value) bool {
    if (configs != .object) return false;
    for (configs.object.values()) |config| {
        if (!configurationReusable(config)) return false;
        const kind = config.object.get("type") orelse return false;
        if (kind != .string or !(std.mem.eql(u8, kind.string, "full_text") or std.mem.eql(u8, kind.string, "embeddings") or std.mem.eql(u8, kind.string, "algebraic"))) return false;
    }
    return true;
}
pub fn plan(a: A, out: A, table: local.common_topology_records.TableRecord, source: *@import("lake_index_row_source.zig").Provider, base: local.serverless_manifest_base_source.BaseSourceDescriptor) ![]const Recipe {
    var desired = try @import("../serverless/build/lake_rebuild.zig").desiredArtifactsFromResolvedExternalSourceAlloc(a, base, source.source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
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
    const input_schema = table.schema_json;
    const configs = try std.json.parseFromSliceLeaky(std.json.Value, ca, table.indexes_json, .{});
    var result: std.ArrayList(Recipe) = .empty;
    for (desired.artifacts) |artifact| {
        switch (artifact.kind) {
            .text_segment, .vector_segment, .sparse_segment => {},
            else => continue,
        }
        var columns: std.ArrayList([]const u8) = .empty;
        const config = if (configs == .object) configs.object.get(artifact.name) else null;
        var all = false;
        var reusable = if (config) |value| configurationReusable(value) else true;
        if (artifact.kind == .text_segment) {
            const text_config = try std.json.parseFromSliceLeaky(std.json.Value, ca, artifact.build_spec.?.text.config_json, .{});
            const field = text_config.object.get("field");
            all = field == null or field.? != .string or try @import("lake_index_native_text.zig").storesSource(text_config);
        }
        if (artifact.kind == .vector_segment or artifact.kind == .sparse_segment) if (config) |value| {
            all = @import("lake_enrichment_units.zig").configured(value, "template") != null;
        };
        if (all) {
            for (schema.relational_columns) |column| try columns.append(ca, column.path);
        } else {
            for (artifact.binding.column_bindings) |name| {
                const path = normalize(schema, name) orelse {
                    reusable = false;
                    continue;
                };
                try columns.append(ca, path);
            }
        }
        try result.append(out, try make(out, input_schema, artifact.name, artifact.binding.index_config_hash, columns.items, reusable));
    }
    // Native reducers can share one encoded cohort across materializations.
    // Every declaration's proof covers every input captured in that cohort.
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(source.context.io orelse return error.InvalidLogicalRowIdentity, ca, table.schema_json, table.table_id, table.name);
    const Aggregate = struct { name: []const u8, configuration: []const u8, recipe: local.sql_aggregate_materialization.Recipe, reusable: bool };
    var aggregates: std.ArrayList(Aggregate) = .empty;
    var configured = configs.object.iterator();
    while (configured.next()) |entry| {
        const config = entry.value_ptr.*;
        if (config != .object) continue;
        const kind = config.object.get("type") orelse continue;
        if (kind != .string or !std.mem.eql(u8, kind.string, "algebraic")) continue;
        const materializations = config.object.get("materializations") orelse continue;
        if (materializations != .array) return error.InvalidAlgebraicConfig;
        for (materializations.array.items) |materialization| {
            const recipe = (try @import("lake_index_native_aggregates.zig").recipeFor(ca, sql_table, config, materialization)) orelse continue;
            const name = materialization.object.get("name") orelse return error.InvalidAlgebraicConfig;
            if (name != .string) return error.InvalidAlgebraicConfig;
            try aggregates.append(ca, .{ .name = try @import("lake_index_names.zig").materialization(ca, entry.key_ptr.*, name.string), .configuration = try std.json.Stringify.valueAlloc(ca, config, .{}), .recipe = recipe, .reusable = configurationReusable(config) });
        }
    }
    for (aggregates.items) |aggregate| {
        var columns: std.ArrayList([]const u8) = .empty;
        var reusable = aggregate.reusable;
        for (aggregates.items) |candidate| {
            if (!@import("lake_index_native_aggregates.zig").compatibleInputGroup(aggregate.recipe, candidate.recipe)) continue;
            reusable = reusable and candidate.reusable;
            for (candidate.recipe.keys) |key| try columns.append(ca, key.path);
            for (candidate.recipe.inputs) |input| if (input.column) |column| try columns.append(ca, column.path);
        }
        try result.append(out, try make(out, input_schema, aggregate.name, aggregate.configuration, columns.items, reusable));
    }
    if (try parsed.relationalIndexDefinitions(ca)) |indexes| for (indexes) |index| {
        var columns: std.ArrayList([]const u8) = .empty;
        var tuple = try local.storage_db_relational_index_keys.TuplePlan.init(a, schema, &layout, index.keys);
        defer tuple.deinit();
        for (try tuple.columnBindings(ca)) |name| try columns.append(ca, normalize(schema, name) orelse return error.InvalidLogicalRowIdentity);
        for (try @import("lake_index_native_rows.zig").coverColumns(ca, index)) |name| try columns.append(ca, normalize(schema, name) orelse return error.InvalidLogicalRowIdentity);
        if (index.where.len != 0) {
            var predicate = try local.storage_db_relational_index_predicate.Plan.init(a, schema, &layout, index.where);
            defer predicate.deinit();
            for (predicate.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |name| try columns.append(ca, normalize(schema, name) orelse return error.InvalidLogicalRowIdentity);
        }
        try result.append(out, try make(out, input_schema, try @import("lake_index_native_rows.zig").logicalName(ca, index.name), try std.json.Stringify.valueAlloc(ca, index, .{}), columns.items, true));
    };
    return result.toOwnedSlice(out);
}
fn normalize(schema: local.storage_schema.TableSchema, name: []const u8) ?[]const u8 {
    for (schema.relational_columns) |column| if (std.mem.eql(u8, name, column.name) or std.mem.eql(u8, name, column.path)) return column.path;
    return null;
}
fn make(a: A, schema: []const u8, name: []const u8, configuration: []const u8, input: []const []const u8, reusable: bool) !Recipe {
    const sorted = try a.dupe([]const u8, input);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.order(u8, l, r) == .lt;
        }
    }.less);
    var columns: std.ArrayList([]const u8) = .empty;
    defer a.free(sorted);
    for (sorted, 0..) |path, i| {
        if (i != 0 and std.mem.eql(u8, path, sorted[i - 1])) continue;
        try columns.append(a, try a.dupe(u8, path));
    }
    const paths = try columns.toOwnedSlice(a);
    const contract = try @import("lake_index_dependencies.zig").project(a, schema, if (reusable) paths else null);
    defer a.free(contract);
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-input-cohort-v3", .schema = contract, .columns = paths }, .{});
    defer a.free(encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded, &digest, .{});
    const binding_bytes = try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-input-binding-v2", .cohort = digest, .name = name, .configuration = configuration, .reusable = reusable }, .{});
    defer a.free(binding_bytes);
    var binding: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(binding_bytes, &binding, .{});
    return .{ .name = try a.dupe(u8, name), .digest = digest, .columns = paths, .reusable = reusable, .binding = binding };
}

test "external lake input recipes capture selected fields and whole source independently" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","base_source":{"kind":"external","table_id":"source","format":"parquet","uri":"file:///source","schema_fingerprint":"schema"},"document_schemas":{"row":{"schema":{"type":"object","additionalProperties":false,"properties":{"body":{"type":"string","x-antfly-field":{"type":"text"}},"label":{"type":"string"},"dense":{"type":"string"},"sparse":{"type":"string"}}}}}}
    ;
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "source", .schema_json = schema, .indexes_json =
        \\{"text":{"type":"full_text","field":"body"},"all_text":{"type":"full_text"},"dense":{"type":"embeddings","external":true,"dimension":2},"sparse":{"type":"embeddings","external":true,"sparse":true}}
    };
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &.{} };
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const recipes = try plan(a, arena.allocator(), table, &provider, .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "file:///source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } });
    try std.testing.expectEqual(@as(usize, 4), recipes.len);
    for (recipes) |recipe| {
        try std.testing.expect(recipe.reusable);
        if (std.mem.eql(u8, recipe.name, "all_text")) {
            try std.testing.expectEqual(@as(usize, 4), recipe.columns.len);
        } else {
            try std.testing.expectEqual(@as(usize, 1), recipe.columns.len);
            try std.testing.expectEqualStrings(if (std.mem.eql(u8, recipe.name, "text")) "body" else recipe.name, recipe.columns[0]);
        }
    }
}

test "external lake input recipes share cohort evidence but independently bind declaration semantics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try make(a, "{}", "first", "config-one", &.{ "body", "id", "body" }, true);
    const right = try make(a, "{}", "second", "config-two", &.{ "id", "body" }, true);
    const changed = try make(a, "{}", "first", "config-three", &.{ "id", "body" }, true);
    try std.testing.expectEqualSlices(u8, &left.digest, &right.digest);
    try std.testing.expectEqualSlices(u8, &left.digest, &changed.digest);
    try std.testing.expect(!std.meta.eql(left.binding, right.binding));
    try std.testing.expect(!std.meta.eql(left.binding, changed.binding));
    try std.testing.expectEqual(@as(usize, 1), count(&.{ left, right, changed }));
    const different = try make(a, "{}", "third", "config-one", &.{"body"}, true);
    try std.testing.expectEqual(@as(usize, 2), count(&.{ left, right, different }));
    const legacy: Recipe = .{ .name = "first", .digest = left.digest, .columns = left.columns };
    const wire = try std.json.Stringify.valueAlloc(a, legacy, .{});
    try std.testing.expect(std.mem.indexOf(u8, wire, "binding") == null);
}

test "external lake input recipes decline payload capture for enrichment and legacy consumers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for ([_][]const u8{ "{}", "{\"text\":{\"type\":\"full_text\",\"field\":\"body\"}}", "{\"graph\":{\"type\":\"graph\"}}", "{\"text\":{\"type\":\"full_text\",\"enrichments\":[{}]}}" }, 0..) |bytes_, i| {
        const config = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), bytes_, .{});
        try std.testing.expectEqual(i < 2, nativeFragmentsEligible(config));
    }
}
