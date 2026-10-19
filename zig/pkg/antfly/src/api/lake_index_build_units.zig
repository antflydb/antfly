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

//! Bounded independent native builders consume a shared file-first producer.
//! Bounded scan consumers, split by configuration and compatible aggregate keys.
const std = @import("std");
const local = @import("antfly_local_sources");
const builds = @import("lake_index_publication_builders.zig");
const aggregates = @import("lake_index_native_aggregates.zig");
const recipes = local.sql_aggregate_materialization;
const A = std.mem.Allocator;
const V = std.json.Value;
const Request = struct { name: []const u8, config: V, mat: V, recipe: recipes.Recipe };
fn text(a: A, value: V) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn single(a: A, table: local.common_topology_records.TableRecord, name: []const u8, config: V, kind: builds.Kind) !builds.Unit {
    var value: V = .{ .object = .empty };
    try value.object.put(a, name, config);
    var unit: builds.Unit = .{ .kind = kind, .table = table };
    unit.table.indexes_json = try text(a, value);
    return unit;
}
pub fn plan(a: A, out: A, table: local.common_topology_records.TableRecord, io: std.Io) ![]const builds.Unit {
    const configs = try std.json.parseFromSliceLeaky(V, out, table.indexes_json, .{ .allocate = .alloc_always });
    if (configs != .object) return error.InvalidTableIndexMetadata;
    var units: std.ArrayList(builds.Unit) = .empty;
    var requests: std.ArrayList(Request) = .empty;
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(io, out, table.schema_json, table.table_id, table.name);
    var it = configs.object.iterator();
    while (it.next()) |entry| {
        const config = entry.value_ptr.*;
        if (config != .object) return error.InvalidTableIndexMetadata;
        const tag = config.object.get("type") orelse continue;
        if (tag != .string) continue;
        if (std.mem.eql(u8, tag.string, "algebraic")) {
            const mats = config.object.get("materializations") orelse continue;
            if (mats != .array) return error.InvalidAlgebraicConfig;
            for (mats.array.items) |mat| if (try aggregates.recipeFor(out, sql_table, config, mat)) |recipe| {
                try requests.append(out, .{ .name = entry.key_ptr.*, .config = config, .mat = mat, .recipe = recipe });
            };
        } else if (std.mem.eql(u8, tag.string, "full_text")) {
            try units.append(out, try single(out, table, entry.key_ptr.*, config, .text));
        } else if (std.mem.eql(u8, tag.string, "embeddings")) {
            const sparse = if (config.object.get("sparse")) |v| v == .bool and v.bool else false;
            try units.append(out, try single(out, table, entry.key_ptr.*, config, if (sparse) .sparse else .dense));
        }
    }
    const consumed = try out.alloc(bool, requests.items.len);
    @memset(consumed, false);
    for (requests.items, 0..) |request, first| {
        if (consumed[first]) continue;
        const key: recipes.Recipe = .{ .keys = request.recipe.keys, .inputs = &.{} };
        var selected: V = .{ .object = .empty };
        var count: usize = 0;
        for (requests.items[first..], first..) |candidate, index| {
            if (consumed[index] or !key.eql(.{ .keys = candidate.recipe.keys, .inputs = &.{} }) or aggregates.incrementalRecipe(request.recipe) != aggregates.incrementalRecipe(candidate.recipe)) continue;
            if (!selected.object.contains(candidate.name)) {
                var config = try std.json.parseFromSliceLeaky(V, out, try text(out, candidate.config), .{ .allocate = .alloc_always });
                try config.object.put(out, "materializations", .{ .array = std.json.Array.init(out) });
                try selected.object.put(out, candidate.name, config);
            }
            try selected.object.getPtr(candidate.name).?.object.getPtr("materializations").?.array.append(candidate.mat);
            consumed[index] = true;
            count += 1;
            if (count == 64) break;
        }
        var unit: builds.Unit = .{ .kind = .aggregate, .table = table };
        unit.table.indexes_json = try text(out, selected);
        try units.append(out, unit);
    }
    const schema = try std.json.parseFromSliceLeaky(V, out, table.schema_json, .{ .allocate = .alloc_always });
    if (schema.object.get("relational_indexes")) |definitions| {
        if (definitions != .array) return error.InvalidTableIndexMetadata;
        var begin: usize = 0;
        while (begin < definitions.array.items.len) {
            const end = @min(begin + 8, definitions.array.items.len);
            var selected = try std.json.parseFromSliceLeaky(V, out, table.schema_json, .{ .allocate = .alloc_always });
            var indexes = std.json.Array.init(out);
            try indexes.appendSlice(definitions.array.items[begin..end]);
            try selected.object.put(out, "relational_indexes", .{ .array = indexes });
            var unit: builds.Unit = .{ .kind = .ordered, .table = table };
            unit.table.indexes_json = "{}";
            unit.table.schema_json = try text(out, selected);
            try units.append(out, unit);
            begin = end;
        }
    }
    return units.toOwnedSlice(out);
}
