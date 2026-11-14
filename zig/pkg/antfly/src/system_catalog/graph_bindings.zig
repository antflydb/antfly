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

//! Resolve graph dependencies inside the metadata mutation's pinned read.
const std = @import("std");
const indexes = @import("../api/indexes.zig");
pub fn indexIdentity(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8) !?@import("cypher").catalog.IndexIdentity {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    defer parsed.deinit();
    return indexIdentityFromValue(a, parsed.value, name);
}

pub fn indexName(a: std.mem.Allocator, indexes_json: []const u8, name: []const u8) ![]const u8 {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, indexes_json, .{ .allocate = .alloc_always });
    return @import("antfly_local_sources").api_index_metadata.aliases.resolve(root, name);
}

/// Validate only relationships owned by the affected physical table. Other
/// tables cannot collide with these mappings. Call inside the catalog write
/// transaction, using the table-to-graph reverse index.
pub fn validateTableMutation(a: std.mem.Allocator, record: @import("antfly_local_sources").system_catalog_domain.Resource, table_id: u64, indexes_json: []const u8) !void {
    return validateTableMutations(a, &.{record}, table_id, indexes_json);
}
pub fn validateTableMutations(a: std.mem.Allocator, records: []const @import("antfly_local_sources").system_catalog_domain.Resource, table_id: u64, indexes_json: []const u8) !void {
    if (records.len == 0) return;
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    for (records) |record| try validateTableMutationValue(a, record, table_id, root);
}
fn validateTableMutationValue(a: std.mem.Allocator, record: @import("antfly_local_sources").system_catalog_domain.Resource, table_id: u64, root: std.json.Value) !void {
    var graph = try @import("antfly_local_sources").system_catalog_domain.graphForExecution(a, record);
    const relationships = try a.dupe(@import("cypher").catalog.Relationship, graph.relationships);
    graph.relationships = relationships;
    for (relationships) |*relationship| {
        const label = graph.label(relationship.source_label) orelse return error.InvalidCatalogRecord;
        if (label.table_id != table_id) continue;
        relationship.index = try @import("antfly_local_sources").api_index_metadata.aliases.resolve(root, relationship.index);
    }
    graph.validate() catch return error.IndexAliasGraphConflict;
}

fn indexIdentityFromValue(a: std.mem.Allocator, root: std.json.Value, name: []const u8) !?@import("cypher").catalog.IndexIdentity {
    const concrete = try @import("antfly_local_sources").api_index_metadata.aliases.resolve(root, name);
    const config = root.object.get(concrete) orelse return null;
    if (indexes.inferIndexType(concrete, config) != .graph) return null;
    const identity = (try indexes.indexRuntimeIdentity(a, concrete, config)) orelse return null;
    return .{ .incarnation = identity.incarnation, .config_hash = identity.config_hash };
}

/// A graph definition retains logical alias names. Bind a request-owned copy
/// against the same table definitions as its statement read set. Fixed index
/// mappings keep their original incarnation fences; only aliases are rebound.
pub fn resolveAliases(a: std.mem.Allocator, record: @import("antfly_local_sources").system_catalog_domain.Resource, tables: anytype) !@import("antfly_local_sources").system_catalog_domain.Resource {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const aliases = @import("antfly_local_sources").api_index_metadata.aliases;
    const graph = try domain.graphForExecution(a, record);
    var result = record;
    var source = try std.json.parseFromSliceLeaky(std.json.Value, a, record.source_json, .{ .allocate = .alloc_always });
    var changed = false;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const Catalog = struct {
        root: std.json.Value,
        identities: std.StringHashMapUnmanaged(@import("cypher").catalog.IndexIdentity) = .empty,
    };
    var catalogs: std.AutoHashMapUnmanaged(u64, Catalog) = .empty;
    for (graph.relationships, 0..) |relationship, ordinal| {
        const label = graph.label(relationship.source_label) orelse return error.InvalidCatalogRecord;
        const table = for (tables) |candidate| {
            if (candidate.id == label.table_id) break candidate;
        } else return error.CatalogGenerationChanged;
        const slot = try catalogs.getOrPut(sa, table.id);
        if (!slot.found_existing) slot.value_ptr.* = .{ .root = try std.json.parseFromSliceLeaky(std.json.Value, sa, table.index_catalog_json, .{}) };
        const catalog = slot.value_ptr;
        const concrete = try aliases.resolve(catalog.root, relationship.index);
        if (std.mem.eql(u8, concrete, relationship.index)) continue;
        if (!changed) result.graph_indexes = try a.dupe(domain.GraphIndexBinding, record.graph_indexes);
        changed = true;
        const identity = try catalog.identities.getOrPut(sa, concrete);
        if (!identity.found_existing) identity.value_ptr.* = (try indexIdentityFromValue(sa, catalog.root, concrete)) orelse return error.CatalogGenerationChanged;
        @constCast(result.graph_indexes)[ordinal].identity = identity.value_ptr.*;
        const mappings = source.object.getPtr("relationships") orelse return error.InvalidCatalogRecord;
        try mappings.array.items[ordinal].object.put(a, "index", .{ .string = try a.dupe(u8, concrete) });
    }
    if (changed) result.source_json = try std.json.Stringify.valueAlloc(a, source, .{});
    return result;
}

test "index alias SQL graph mappings bind names and incarnations per execution" {
    const local = @import("antfly_local_sources");
    const domain = local.system_catalog_domain;
    const aliases = local.api_index_metadata.aliases;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = "{\"g1\":{\"type\":\"graph\",\"_index_incarnation\":41},\"g2\":{\"type\":\"graph\",\"_index_incarnation\":42}}";
    const ready = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"complete\":true}", .{});
    const first = try aliases.put(a, base, "live", .{ .index = "g1", .idempotency_key = "1" }, ready, 1);
    const second = try aliases.put(a, first, "live", .{ .index = "g2", .expected_index = "g1", .idempotency_key = "2" }, ready, 2);
    const record: domain.Resource = .{
        .kind = .graph,
        .id = 7,
        .name = "knowledge",
        .source_json = "{\"labels\":[{\"name\":\"Person\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"KNOWS\",\"index\":\"live\",\"source_label\":\"Person\",\"target_label\":\"Person\",\"stored_type\":\"knows\"}]}",
        .graph_tables = &.{.{ .label_index = 0, .table_id = 42, .storage_name = "physical" }},
        .graph_indexes = &.{.{ .mapping_index = 0, .identity = (try indexIdentity(a, first, "live")).? }},
    };
    const Table = struct { id: u64 = 42, index_catalog_json: []const u8 };
    const old = try resolveAliases(a, record, &[_]Table{.{ .index_catalog_json = first }});
    const new = try resolveAliases(a, record, &[_]Table{.{ .index_catalog_json = second }});
    const old_graph = try domain.graphForExecution(a, old);
    const new_graph = try domain.graphForExecution(a, new);
    try std.testing.expectEqualStrings("g1", old_graph.relationships[0].index);
    try std.testing.expectEqualStrings("g2", new_graph.relationships[0].index);
    try std.testing.expectEqual(@as(u64, 41), old_graph.relationships[0].identity.?.incarnation);
    try std.testing.expectEqual(@as(u64, 42), new_graph.relationships[0].identity.?.incarnation);
    try std.testing.expectEqualStrings("live", (try domain.graphForExecution(a, record)).relationships[0].index);
    const cypher = @import("cypher");
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var compiled = try cypher.compiler.compile(a, "MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n", .{}, &diagnostic);
    defer compiled.deinit();
    const selected = try cypher.dependencies.selectedIndexes(a, compiled.query, new_graph);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expectEqualStrings("g2", selected[0]);
    const sql_catalog = local.sql_catalog;
    const graph_binding: sql_catalog.graph_relation.Binding = .{ .definition = .{ .name = .{ .table = "knowledge" }, .query = compiled.query, .parameters = &.{} }, .graph = new_graph, .catalog_json = "", .dependencies = &.{} };
    const table: sql_catalog.Table = .{ .id = 7, .physical_name = "knowledge", .schema_version = 1, .columns = &.{}, .graph = &graph_binding };
    const relation: local.sql_relation_binding.Bound = .{ .root = undefined, .scans = &.{.{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } }}, .table = table, .statement = undefined };
    const child: local.sql_describe.BoundStatement = .{ .table = null, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty, .relation = &relation };
    const parent: local.sql_describe.BoundStatement = .{ .table = null, .action = .write, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty, .insert_source = &child };
    var attribution = try local.sql_runtime.IndexAttribution.init(a, parent);
    const observer = attribution.observer();
    try observer.record(observer.ptr, "dense_search_target");
    try observer.record(observer.ptr, "dense_search_target");
    try std.testing.expectEqual(@as(usize, 2), attribution.value().?.len);
    const sql_output: local.sql_runtime.Output = .{ .command_tag = "SELECT", .resolved_indexes = selected };
    const sql_json = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, sql_output, .{ .emit_null_optional_fields = false }), .{});
    try std.testing.expectEqualStrings("g2", sql_json.object.get("resolved_indexes").?.array.items[0].string);
    const graph_output: cypher.runtime.Result = .{ .arena = undefined, .budget = undefined, .columns = &.{}, .rows = &.{}, .stages = &.{}, .stats = .{}, .graph_incarnation = 7, .graph_revision = 1, .resolved_indexes = selected };
    const graph_json = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, graph_output, .{}), .{});
    try std.testing.expectEqualStrings("g2", graph_json.object.get("resolved_indexes").?.array.items[0].string);
}

test "index alias graph binding bounds catalog allocations across shared relationship mappings" {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const cypher = @import("cypher");
    const a = std.testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(a);
    defer fixture.deinit();
    const fa = fixture.allocator();
    const padding = try fa.alloc(u8, 128 * 1024);
    @memset(padding, '\n');
    const catalog = try std.json.Stringify.valueAlloc(fa, .{
        .g1 = .{ .type = "graph", ._index_incarnation = 41 },
        .unused = .{ .type = "embeddings", .embedder = .{ .model = padding } },
        ._index_aliases = .{ .live = .{ .index = "g1" } },
    }, .{});
    const Mapping = @typeInfo(@FieldType(cypher.catalog.Definition, "relationships")).pointer.child;
    const mappings = try fa.alloc(Mapping, 64);
    const identities = try fa.alloc(domain.GraphIndexBinding, 64);
    for (mappings, identities, 0..) |*mapping, *identity, i| {
        const name = try std.fmt.allocPrint(fa, "EDGE{d}", .{i});
        mapping.* = .{ .name = name, .index = "live", .source_label = "Person", .target_label = "Person", .stored_type = name };
        identity.* = .{ .mapping_index = @intCast(i), .identity = .{ .incarnation = 1, .config_hash = 1 } };
    }
    const record: domain.Resource = .{
        .kind = .graph,
        .id = 7,
        .name = "knowledge",
        .source_json = try std.json.Stringify.valueAlloc(fa, cypher.catalog.Definition{ .labels = &.{.{ .name = "Person", .table = .{ .string = "docs" } }}, .relationships = mappings }, .{}),
        .graph_tables = &.{.{ .label_index = 0, .table_id = 42, .storage_name = "physical" }},
        .graph_indexes = identities,
    };
    var counter = std.testing.FailingAllocator.init(a, .{});
    var execution = std.heap.ArenaAllocator.init(counter.allocator());
    defer execution.deinit();
    const Table = struct { id: u64 = 42, index_catalog_json: []const u8 };
    const bound = try resolveAliases(execution.allocator(), record, &[_]Table{.{ .index_catalog_json = catalog }});
    // Escaped catalog strings require owned parse allocations. Repeating the
    // parse per mapping alone would retain over 8 MiB for this shared table.
    try std.testing.expect(counter.allocated_bytes < 4 * 1024 * 1024);
    const graph = try domain.graphForExecution(fa, bound);
    for (graph.relationships) |mapping| {
        try std.testing.expectEqualStrings("g1", mapping.index);
        try std.testing.expectEqual(@as(u64, 41), mapping.identity.?.incarnation);
    }
}

test "index alias graph admission rejects converged physical mappings and indexes graph dependencies" {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const definition =
        \\{"labels":[{"name":"D","table":"docs"}],"relationships":[{"name":"R1","index":"left","source_label":"D","target_label":"D","stored_type":"edge"},{"name":"R2","index":"right","source_label":"D","target_label":"D","stored_type":"edge"}]}
    ;
    const identity: @import("cypher").catalog.IndexIdentity = .{ .incarnation = 41, .config_hash = 99 };
    const tables = [_]domain.PhysicalTable{.{ .id = 42, .name = "docs", .graph_indexes = &.{
        .{ .name = "left", .concrete_name = "g1", .identity = identity },
        .{ .name = "right", .concrete_name = "g2", .identity = identity },
    } }};
    const converged = [_]domain.PhysicalTable{.{ .id = 42, .name = "docs", .graph_indexes = &.{
        .{ .name = "left", .concrete_name = "g2", .identity = identity },
        .{ .name = "right", .concrete_name = "g2", .identity = identity },
    } }};
    const request: domain.Mutation = .{ .kind = .graph, .action = .create, .name = "knowledge", .source_json = definition };
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidCatalogMutation, domain.plan(a, .{}, request, &converged));
    var created = try domain.plan(a, .{}, request, &tables);
    defer created.deinit(a);
    var state = try domain.MutableState.clone(a, .{ .resources = created.upserts, .next_id = created.next_id });
    defer state.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.index.graphsForTable(42).len);
    try std.testing.expectEqual(@as(usize, 0), state.index.graphsForTable(43).len);
    const record = state.index.graphsForTable(42)[0];
    const AllocationProbe = struct {
        fn run(allocator: std.mem.Allocator, original: domain.Resource) !void {
            var owned = try domain.MutableState.clone(allocator, .{ .resources = &.{original}, .next_id = 8 });
            defer owned.deinit();
            var replacement = original;
            replacement.graph_tables = &.{.{ .label_index = 0, .table_id = 43, .storage_name = "other" }};
            var upserts = [_]domain.Resource{replacement};
            var change = try owned.apply(.{ .upserts = &upserts, .removes = &.{}, .next_id = 8 });
            change.finish(&owned, false);
            try std.testing.expectEqual(@as(usize, 1), owned.index.graphsForTable(42).len);
            try std.testing.expectEqual(@as(usize, 0), owned.index.graphsForTable(43).len);
            try std.testing.expect(!owned.index.graph_users.contains(43));
        }
    };
    try std.testing.checkAllAllocationFailures(a, AllocationProbe.run, .{record});
    const Poison = struct {
        backing: std.mem.Allocator,
        fn allocate(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(_: *anyopaque, bytes: []u8, _: std.mem.Alignment, _: usize) void {
            @memset(bytes, 0xa5);
        }
    };
    var quarantine = std.heap.ArenaAllocator.init(a);
    defer quarantine.deinit();
    var poison: Poison = .{ .backing = quarantine.allocator() };
    try AllocationProbe.run(.{ .ptr = &poison, .vtable = &.{ .alloc = Poison.allocate, .resize = Poison.resize, .remap = Poison.remap, .free = Poison.free } }, record);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const before = "{\"g1\":{\"type\":\"graph\"},\"g2\":{\"type\":\"graph\"},\"_index_aliases\":{\"left\":{\"index\":\"g1\"},\"right\":{\"index\":\"g2\"}}}";
    const after = "{\"g1\":{\"type\":\"graph\"},\"g2\":{\"type\":\"graph\"},\"_index_aliases\":{\"left\":{\"index\":\"g2\"},\"right\":{\"index\":\"g2\"}}}";
    try validateTableMutation(arena.allocator(), record, 42, before);
    try validateTableMutation(arena.allocator(), record, 43, after);
    try std.testing.expectError(error.IndexAliasGraphConflict, validateTableMutation(arena.allocator(), record, 42, after));
    var dropped = try domain.plan(a, state.value, .{ .kind = .graph, .action = .drop, .name = "knowledge" }, &tables);
    defer dropped.deinit(a);
    var change = try state.apply(dropped);
    try std.testing.expectEqual(@as(usize, 0), state.index.graphsForTable(42).len);
    change.finish(&state, false);
    try std.testing.expectEqual(@as(usize, 1), state.index.graphsForTable(42).len);
    var committed = try state.apply(dropped);
    committed.finish(&state, true);
    try std.testing.expectEqual(@as(usize, 0), state.index.graphsForTable(42).len);
}
