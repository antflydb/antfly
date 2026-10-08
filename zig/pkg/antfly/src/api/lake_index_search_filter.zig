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

//! Resolve flat structured predicates once against the pinned, delete-aware
//! lake cursor. Native scorers receive public IDs before ranking or pagination;
//! no source JSON or repeated top-k hydration is needed for each candidate.
const std = @import("std");
const local = @import("antfly_local_sources");
const graph = local.storage_db_query_graph_exec;
const types = local.storage_db_types;
const A = std.mem.Allocator;
const max_filter_ids = 100_000;

pub fn resolve(a: A, table: local.sql_catalog.Table, source: *local.serverless_query_lake_serving.ServingSource, request: local.api_operation.RequestContext, req: *types.SearchRequest) !void {
    if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return;
    var include = if (req.filter_query_json.len != 0) try graph.PreparedPatternFilter.init(a, req.filter_query_json) else null;
    defer if (include) |*filter| filter.deinit();
    var exclude = if (req.exclusion_query_json.len != 0) try graph.PreparedPatternFilter.init(a, req.exclusion_query_json) else null;
    defer if (exclude) |*filter| filter.deinit();
    var fields: std.ArrayList([]const u8) = .empty;
    if (include) |filter| if (!try dependencies(a, table, filter.compiled, &fields)) return;
    if (exclude) |filter| if (!try dependencies(a, table, filter.compiled, &fields)) return;
    const cursor = try local.sql_lake_cursor.openPinned(a, table, .{ .fields = fields.items, .limit = 256 }, request, source);
    defer cursor.close(cursor.ptr);
    var ids: std.ArrayList([]const u8) = .empty;
    if (include == null) try ids.appendSlice(a, req.exclude_doc_ids);
    var existing: std.StringHashMap(void) = .init(a);
    for (req.filter_doc_ids) |id| try existing.put(id, {});
    while (true) {
        try request.ensureActive();
        var page_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer page_arena.deinit();
        const pa = page_arena.allocator();
        const page = try cursor.next_columns.?(cursor.ptr, pa, 256);
        try page.validate();
        for (0..page.selection.len) |row| {
            const id = try page.cell(pa, row, "_id");
            if (id.value != .string) return error.InvalidSqlBackendResponse;
            if ((req.filter_doc_ids_positive or req.filter_doc_ids.len != 0) and !existing.contains(id.value.string)) continue;
            var doc: std.json.Value = .{ .object = .empty };
            try doc.object.put(pa, "_id", id.value);
            for (page.batch.columns) |column| {
                const cell = try page.cell(pa, row, column.name);
                try doc.object.put(pa, column.name, cell.value);
            }
            if (include) |filter| if (!try filter.matchesJson(pa, id.value.string, doc)) continue;
            if (exclude) |filter| {
                const matched = try filter.matchesJson(pa, id.value.string, doc);
                if (include != null and matched) continue;
                if (include == null and !matched) continue;
            }
            if (ids.items.len >= max_filter_ids) return error.QueryCandidateBudgetExceeded;
            try ids.append(a, try a.dupe(u8, id.value.string));
        }
        if (page.after == null) break;
    }
    if (include != null) {
        req.filter_doc_ids = ids.items;
        req.filter_doc_ids_positive = true; // An empty set must stay match-none.
    } else {
        req.exclude_doc_ids = ids.items;
    }
    req.filter_query_json = "";
    req.exclusion_query_json = "";
}

fn dependencies(a: A, table: local.sql_catalog.Table, filter: graph.CompiledPatternFilter, fields: *std.ArrayList([]const u8)) !bool {
    switch (filter) {
        .match_all, .match_none, .doc_id => {},
        .conjuncts, .disjuncts => |items| for (items) |item| {
            if (!try dependencies(a, table, item, fields)) return false;
        },
        .bool_query => |query| {
            for ([_][]graph.CompiledPatternFilter{ query.must, query.should, query.must_not }) |items| for (items) |item| {
                if (!try dependencies(a, table, item, fields)) return false;
            };
        },
        .field_matcher => |matcher| {
            const name = switch (matcher.path) {
                .single => |value| value,
                .dotted, .json_pointer => |parts| if (parts.len == 1) parts[0] else return false,
            };
            if (std.mem.eql(u8, name, "_id")) return true;
            for (table.columns) |column| {
                if (!std.mem.eql(u8, name, column.path)) continue;
                for (fields.items) |field| if (std.mem.eql(u8, field, column.path)) return true;
                try fields.append(a, column.path);
                return true;
            }
            // A missing flat field evaluates against an absent value.
        },
    }
    return true;
}

test "external lake search filter dependencies include hidden flat fields once" {
    const a = std.testing.allocator;
    const table: local.sql_catalog.Table = .{ .id = 1, .physical_name = "hn", .schema_version = 1, .columns = &.{.{ .name = "hn_id", .path = "hn_id", .type = .integer }} };
    var filter = try graph.PreparedPatternFilter.init(a, "{\"conjuncts\":[{\"term\":{\"path\":\"/hn_id\",\"value\":42}},{\"term\":{\"hn_id\":42}}]}");
    defer filter.deinit();
    var fields: std.ArrayList([]const u8) = .empty;
    defer fields.deinit(a);
    try std.testing.expect(try dependencies(a, table, filter.compiled, &fields));
    try std.testing.expectEqual(@as(usize, 1), fields.items.len);
    try std.testing.expectEqualStrings("hn_id", fields.items[0]);
}
