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

//! Bounded disjoint-table composition. Enumerate complete matching sets before
//! exact global ordering; fail closed rather than truncate source candidates.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const V = std.json.Value;
const Response = @import("contextual_operations.zig").OwnedResponse;
pub const Executor = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!Response,
    checkpoint: *const fn (*anyopaque) anyerror!void,
};
pub fn hasSource(a: A, body: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(V, a, body, .{});
    defer parsed.deinit();
    return parsed.value == .object and parsed.value.object.contains("source");
}
const Hit = struct { value: V, table: []const u8, id: []const u8, score: f64, keys: []const V };
const Order = struct { field: []const u8, desc: bool = false };
fn compareValue(l: V, r: V) std.math.Order {
    if (l == .null) return if (r == .null) .eq else .lt;
    if (r == .null) return .gt;
    return local.sql_scalar.compare(l, r) catch .eq;
}
fn less(orders: []const Order, l: Hit, r: Hit) bool {
    for (orders, 0..) |order, i| {
        const cmp = if (std.mem.eql(u8, order.field, "_score")) std.math.order(l.score, r.score) else compareValue(l.keys[i], r.keys[i]);
        if (cmp != .eq) return if (order.desc) cmp == .gt else cmp == .lt;
    }
    const table_order = std.mem.order(u8, l.table, r.table);
    if (table_order != .eq) return table_order == .lt;
    return std.mem.lessThan(u8, l.id, r.id);
}
fn rowIdentity(a: A, keys: []const []const u8, row: V) ![]const u8 {
    if (row != .object) return error.UnsupportedQueryRequest;
    const values = try a.alloc(V, keys.len);
    for (keys, values) |key, *value| {
        value.* = row.object.get(key) orelse return error.UnsupportedQueryRequest;
        if (value.* != .integer and value.* != .string and value.* != .bool) return error.UnsupportedQueryRequest;
    }
    return std.json.Stringify.valueAlloc(a, values, .{});
}
fn integer(value: V) !usize {
    return switch (value) {
        .integer => |number| std.math.cast(usize, number) orelse error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}
pub fn execute(a: A, body: []const u8, executor: Executor) !Response {
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    return executeBudget(a, body, executor, &budget) catch |err| return if (budget.exhausted) error.QueryCandidateBudgetExceeded else err;
}
fn executeBudget(a: A, body: []const u8, executor: Executor, budget: *local.sql_memory_budget) !Response {
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const scratch = arena.allocator();
    var root = try std.json.parseFromSliceLeaky(V, scratch, body, .{ .allocate = .alloc_always });
    if (root != .object or root.object.contains("table") or root.object.contains("table_target")) return error.InvalidQueryRequest;
    const source = root.object.get("source") orelse return error.InvalidQueryRequest;
    if (source != .object or source.object.count() != 1) return error.UnsupportedQueryRequest;
    const overlay = source.object.get("overlay");
    var overlay_keys: []const []const u8 = &.{};
    var tombstone_field: []const u8 = "deleted";
    const union_value = source.object.get("union") orelse overlay_source: {
        const spec = overlay orelse return error.UnsupportedQueryRequest;
        if (spec != .object or spec.object.count() < 3 or spec.object.count() > 4) return error.InvalidQueryRequest;
        for (spec.object.keys()) |field| if (!std.mem.eql(u8, field, "base") and !std.mem.eql(u8, field, "changes") and !std.mem.eql(u8, field, "key") and !std.mem.eql(u8, field, "tombstone_field")) return error.InvalidQueryRequest;
        const keys = spec.object.get("key") orelse return error.InvalidQueryRequest;
        if (keys != .array or keys.array.items.len == 0 or keys.array.items.len > 8) return error.InvalidQueryRequest;
        const names = try scratch.alloc([]const u8, keys.array.items.len);
        for (keys.array.items, names, 0..) |key, *name, ordinal| {
            if (key != .string or key.string.len == 0 or std.mem.indexOfAny(u8, key.string, "/.~") != null) return error.InvalidQueryRequest;
            for (names[0..ordinal]) |previous| if (std.mem.eql(u8, previous, key.string)) return error.InvalidQueryRequest;
            name.* = key.string;
        }
        overlay_keys = names;
        if (spec.object.get("tombstone_field")) |field| {
            if (field != .string or field.string.len == 0 or std.mem.indexOfAny(u8, field.string, "/.~") != null) return error.InvalidQueryRequest;
            tombstone_field = field.string;
        }
        var inputs: std.ArrayList(V) = .empty;
        try inputs.append(scratch, spec.object.get("base") orelse return error.InvalidQueryRequest);
        try inputs.append(scratch, spec.object.get("changes") orelse return error.InvalidQueryRequest);
        break :overlay_source V{ .array = inputs.toManaged(scratch) };
    };
    if (union_value != .array or union_value.array.items.len < 2 or union_value.array.items.len > 16) return error.InvalidQueryRequest;
    for ([_][]const u8{ "aggregations", "hierarchy", "join", "graph_queries", "graph_searches", "analyses", "document_renderer", "search_after", "search_before", "session_id", "connection_id", "remote_snapshot" }) |name| if (root.object.contains(name)) return error.UnsupportedQueryRequest;
    const limit = if (root.object.get("limit")) |value| try integer(value) else 20;
    if (limit == 0 or limit > 4096) return error.InvalidQueryRequest;
    const offset = if (root.object.get("offset")) |value| try integer(value) else 0;
    const count = if (root.object.get("count")) |value| switch (value) {
        .bool => |b| b,
        else => return error.InvalidQueryRequest,
    } else false;
    const ranking = root.object.get("source_ranking");
    const rrf = if (ranking) |value| value == .string and std.mem.eql(u8, value.string, "rrf") else false;
    if (ranking != null and !rrf) return error.UnsupportedQueryRequest;
    var orders: std.ArrayList(Order) = .empty;
    if (root.object.get("order_by")) |value| {
        const encoded = try std.json.Stringify.valueAlloc(scratch, value, .{});
        const parsed = try std.json.parseFromSliceLeaky([]Order, scratch, encoded, .{});
        if (parsed.len == 0 or parsed.len > 8) return error.InvalidQueryRequest;
        try orders.appendSlice(scratch, parsed);
    } else try orders.append(scratch, .{ .field = "_score", .desc = true });
    for (orders.items) |order| if (std.mem.eql(u8, order.field, "_score") and !rrf) return error.UnsupportedQueryRequest;
    if (rrf and (orders.items.len != 1 or !std.mem.eql(u8, orders.items[0].field, "_score") or !orders.items[0].desc)) return error.UnsupportedQueryRequest;
    const cursor = root.object.get("source_cursor");
    const expression_bytes = try std.json.Stringify.valueAlloc(scratch, .{ .source = source, .ranking = ranking }, .{});
    _ = root.object.orderedRemove("source");
    _ = root.object.orderedRemove("source_ranking");
    _ = root.object.orderedRemove("source_cursor");
    _ = root.object.orderedRemove("offset");
    _ = root.object.orderedRemove("count");
    try root.object.put(scratch, "limit", .{ .integer = 4096 });
    const requested_fields = root.object.get("fields");
    if (overlay != null and requested_fields != null) {
        if (requested_fields.? != .array) return error.InvalidQueryRequest;
        var fields: std.ArrayList(V) = .empty;
        try fields.appendSlice(scratch, requested_fields.?.array.items);
        var added: std.ArrayList([]const u8) = .empty;
        try added.appendSlice(scratch, overlay_keys);
        try added.append(scratch, tombstone_field);
        for (added.items) |key| {
            const present = for (fields.items) |field| {
                if (field == .string and std.mem.eql(u8, field.string, key)) break true;
            } else false;
            if (!present) try fields.append(scratch, .{ .string = key });
        }
        try root.object.put(scratch, "fields", .{ .array = fields.toManaged(scratch) });
    }
    var hits: std.ArrayList(Hit) = .empty;
    var matched_total: usize = 0;
    var changes_snapshot: ?[]const u8 = null;
    var changes_identity: ?[]const u8 = null;
    var tables: std.StringHashMapUnmanaged(void) = .empty;
    var fingerprint = std.crypto.hash.Blake3.init(.{});
    fingerprint.update("composed-union-v1");
    fingerprint.update(expression_bytes);
    fingerprint.update(try std.json.Stringify.valueAlloc(scratch, root, .{}));
    for (union_value.array.items) |leaf| {
        try executor.checkpoint(executor.ptr);
        if (leaf != .object or leaf.object.count() != 1) return error.InvalidQueryRequest;
        const name = leaf.object.get("table") orelse return error.InvalidQueryRequest;
        if (name != .string or name.string.len == 0) return error.InvalidQueryRequest;
        const entry = try tables.getOrPut(scratch, name.string);
        if (entry.found_existing) return error.InvalidQueryRequest;
        var response = try executor.execute(executor.ptr, a, name.string, try std.json.Stringify.valueAlloc(scratch, root, .{}));
        if (response.status != 200) return response;
        defer response.deinit(a);
        if (response.body.len > 32 * 1024 * 1024) return error.QueryCandidateBudgetExceeded;
        const parsed = try std.json.parseFromSliceLeaky(V, scratch, response.body, .{});
        const responses = parsed.object.get("responses") orelse return error.InvalidQueryRequest;
        if (responses != .array or responses.array.items.len != 1) return error.UnsupportedQueryRequest;
        const result = responses.array.items[0];
        if (overlay != null and std.mem.eql(u8, name.string, union_value.array.items[1].object.get("table").?.string)) {
            if (result.object.get("remote_snapshot")) |token| if (token == .string) {
                changes_snapshot = token.string;
            };
            changes_identity = try std.json.Stringify.valueAlloc(scratch, result.object.get("_composed_identity") orelse .null, .{});
        }
        const result_hits = result.object.get("hits") orelse return error.UnsupportedQueryRequest;
        const items = result_hits.object.get("hits") orelse return error.UnsupportedQueryRequest;
        const total = result_hits.object.get("total") orelse return error.UnsupportedQueryRequest;
        const total_count = if (total == .integer) try integer(total) else try integer(total.object.get("value") orelse return error.UnsupportedQueryRequest);
        if (total == .object) if (total.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
        if (items != .array) return error.QueryCandidateBudgetExceeded;
        // Disjoint RRF ranks are strictly decreasing within each source. The
        // first 4096 ranks from every source prove any global window ending
        // within 4096, regardless of the archive match count. Overlays and
        // field ties still require complete inputs.
        if (items.array.items.len != total_count and (overlay != null or !rrf or items.array.items.len != 4096 or total_count < 4096)) return error.QueryCandidateBudgetExceeded;
        matched_total = try std.math.add(usize, matched_total, total_count);
        fingerprint.update(try std.json.Stringify.valueAlloc(scratch, .{ .table = name.string, .total = total_count, .identity = result.object.get("_composed_identity") orelse .null, .remote_snapshot = result.object.get("remote_snapshot") orelse .null }, .{}));
        var logical_keys: std.StringHashMapUnmanaged(void) = .empty;
        for (items.array.items, 0..) |value, rank| {
            if (overlay != null) {
                const logical = try rowIdentity(scratch, overlay_keys, value.object.get("_source") orelse return error.UnsupportedQueryRequest);
                const unique = try logical_keys.getOrPut(scratch, logical);
                if (unique.found_existing) return error.InvalidQueryRequest;
            }
            try executor.checkpoint(executor.ptr);
            const id = value.object.get("_id") orelse return error.InvalidQueryRequest;
            if (id != .string) return error.InvalidQueryRequest;
            const keys: []const V = if (value.object.get("_sort")) |sort| if (sort == .array) sort.array.items else return error.UnsupportedQueryRequest else &.{};
            if (!rrf and keys.len < orders.items.len) return error.UnsupportedQueryRequest;
            const score = if (rrf) 1.0 / (60.0 + @as(f64, @floatFromInt(rank + 1))) else 0;
            try hits.append(scratch, .{ .value = value, .table = name.string, .id = id.string, .score = score, .keys = keys });
        }
    }
    if (overlay != null) {
        const base_name = union_value.array.items[0].object.get("table").?.string;
        const changes_name = union_value.array.items[1].object.get("table").?.string;
        var hidden: std.StringHashMapUnmanaged(void) = .empty;
        // Point anti-lookups deliberately omit the user's text/filter clauses:
        // a newer nonmatching row still hides an older matching version.
        var at: usize = 0;
        while (at < hits.items.len and std.mem.eql(u8, hits.items[at].table, base_name)) {
            var clauses: std.ArrayList(V) = .empty;
            var count_keys: usize = 0;
            while (at < hits.items.len and std.mem.eql(u8, hits.items[at].table, base_name) and count_keys < 128) : (at += 1) {
                const row = hits.items[at].value.object.get("_source") orelse return error.UnsupportedQueryRequest;
                var conjuncts: std.ArrayList(V) = .empty;
                for (overlay_keys) |key| {
                    const operand = row.object.get(key) orelse return error.UnsupportedQueryRequest;
                    if (operand == .null or operand == .array or operand == .object) return error.InvalidQueryRequest;
                    const clause = try std.json.Stringify.valueAlloc(scratch, .{ .term = .{ .path = try std.fmt.allocPrint(scratch, "/{s}", .{key}), .value = operand } }, .{});
                    try conjuncts.append(scratch, try std.json.parseFromSliceLeaky(V, scratch, clause, .{}));
                }
                const clause = try std.json.Stringify.valueAlloc(scratch, .{ .conjuncts = conjuncts.items }, .{});
                try clauses.append(scratch, try std.json.parseFromSliceLeaky(V, scratch, clause, .{}));
                count_keys += 1;
            }
            const lookup = try std.json.Stringify.valueAlloc(scratch, .{ .full_text_search = .{ .match_all = .{} }, .filter_query = .{ .disjuncts = clauses.items }, .fields = overlay_keys, .limit = 4096, .remote_snapshot = changes_snapshot, .lake_read = root.object.get("lake_read") }, .{ .emit_null_optional_fields = false });
            try executor.checkpoint(executor.ptr);
            var response = try executor.execute(executor.ptr, a, changes_name, lookup);
            if (response.status != 200) return response;
            defer response.deinit(a);
            const decoded = try std.json.parseFromSliceLeaky(V, scratch, response.body, .{});
            const lookup_result = decoded.object.get("responses").?.array.items[0];
            if (changes_identity) |expected| {
                const actual = try std.json.Stringify.valueAlloc(scratch, lookup_result.object.get("_composed_identity") orelse .null, .{});
                if (!std.mem.eql(u8, expected, actual)) return error.CatalogGenerationChanged;
            }
            const result = lookup_result.object.get("hits").?;
            const items = result.object.get("hits").?.array.items;
            const total = result.object.get("total").?;
            const total_count = if (total == .integer) try integer(total) else try integer(total.object.get("value").?);
            if (total == .object) if (total.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
            if (total_count != items.len or items.len > count_keys) return error.QueryCandidateBudgetExceeded;
            for (items) |hit| {
                const key = try rowIdentity(scratch, overlay_keys, hit.object.get("_source").?);
                const entry = try hidden.getOrPut(scratch, key);
                if (entry.found_existing) return error.InvalidQueryRequest;
            }
        }
        var kept: usize = 0;
        var rank_base: usize = 0;
        var rank_changes: usize = 0;
        for (hits.items) |hit| {
            const row = hit.value.object.get("_source") orelse return error.UnsupportedQueryRequest;
            const base = std.mem.eql(u8, hit.table, base_name);
            if (base and hidden.contains(try rowIdentity(scratch, overlay_keys, row))) continue;
            if (!base) if (row.object.get(tombstone_field)) |flag| {
                if (flag != .bool and flag != .null) return error.InvalidQueryRequest;
                if (flag == .bool and flag.bool) continue;
            };
            var visible = hit;
            if (rrf) {
                const rank = if (base) &rank_base else &rank_changes;
                rank.* += 1;
                visible.score = 1.0 / (60.0 + @as(f64, @floatFromInt(rank.*)));
            }
            hits.items[kept] = visible;
            kept += 1;
        }
        hits.items.len = kept;
    }
    try executor.checkpoint(executor.ptr);
    if (!rrf) for (orders.items, 0..) |_, column| {
        var reference: ?V = null;
        for (hits.items) |hit| {
            if (hit.keys[column] == .null) continue;
            if (reference) |value| {
                _ = local.sql_scalar.compare(value, hit.keys[column]) catch return error.UnsupportedQueryRequest;
            } else reference = hit.keys[column];
        }
    };
    std.mem.sort(Hit, hits.items, orders.items, less);
    try executor.checkpoint(executor.ptr);
    for (hits.items) |hit| {
        try executor.checkpoint(executor.ptr);
        fingerprint.update(hit.table);
        fingerprint.update(try std.json.Stringify.valueAlloc(scratch, hit.value, .{}));
    }
    var digest: [32]u8 = undefined;
    fingerprint.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    var start = offset;
    if (cursor) |value| {
        if (offset != 0 or count or value != .string or value.string.len < 66 or value.string[64] != ':') return error.InvalidQueryRequest;
        if (!std.mem.eql(u8, value.string[0..64], &hex)) return error.CatalogGenerationChanged;
        start = std.fmt.parseInt(usize, value.string[65..], 10) catch return error.InvalidQueryRequest;
    }
    if (overlay == null and rrf and !count and (start > 4096 or limit > 4096 - start)) return error.QueryCandidateBudgetExceeded;
    start = @min(start, hits.items.len);
    const end = if (count) start else @min(start +| limit, hits.items.len);
    var output: std.ArrayList(V) = .empty;
    for (hits.items[start..end]) |hit| {
        try executor.checkpoint(executor.ptr);
        var value = hit.value;
        try value.object.put(scratch, "_table", .{ .string = hit.table });
        if (rrf) {
            try value.object.put(scratch, "_score", .{ .float = hit.score });
            _ = value.object.swapRemove("_index_scores");
            _ = value.object.swapRemove("_score_details");
        }
        if (overlay != null) if (requested_fields) |fields| {
            const row = value.object.getPtr("_source") orelse return error.UnsupportedQueryRequest;
            var projected: V = .{ .object = .empty };
            for (fields.array.items) |field| {
                if (field != .string) return error.InvalidQueryRequest;
                try projected.object.put(scratch, field.string, row.object.get(field.string) orelse .null);
            }
            row.* = projected;
        };
        // Leaf sort tuples are not valid global cursors.
        _ = value.object.swapRemove("_sort");
        try output.append(scratch, value);
    }
    const next: ?[]const u8 = if (!count and end < hits.items.len and !(overlay == null and rrf and end >= 4096)) try std.fmt.allocPrint(scratch, "{s}:{d}", .{ hex, end }) else null;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .status = 200, .took = 0, .hits = .{ .total = .{ .value = if (overlay == null) matched_total else hits.items.len, .relation = "exact" }, .hits = output.items }, .source_ranking = if (rrf) "rrf" else "ordered", .next_source_cursor = next }} }, .{ .emit_null_optional_fields = false });
    errdefer a.free(encoded);
    try executor.checkpoint(executor.ptr);
    return @import("contextual_operations.zig").json(encoded, false);
}

test "external lake composed union preserves equal IDs across tables and orders with deterministic provenance ties" {
    const orders = [_]Order{.{ .field = "_score", .desc = true }};
    const high: Hit = .{ .value = .null, .table = "history", .id = "1", .score = 1, .keys = &.{} };
    const low: Hit = .{ .value = .null, .table = "current", .id = "1", .score = 0.5, .keys = &.{} };
    try std.testing.expect(less(&orders, high, low));
    var tie = high;
    tie.table = "current";
    try std.testing.expect(less(&orders, tie, high));
}

const TestExecutor = struct {
    changed: bool = false,
    overlay: bool = false,
    fn checkpoint(_: *anyopaque) !void {}
    fn run(raw: *anyopaque, a: A, table: []const u8, body: []const u8) !Response {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const query = try std.json.parseFromSliceLeaky(V, scratch, body, .{});
        const data = if (std.mem.eql(u8, table, "history"))
            if (self.overlay) "[{\"_id\":\"h1\",\"_score\":2,\"_source\":{\"id\":1,\"body\":\"old match\"}},{\"_id\":\"h2\",\"_score\":1,\"_source\":{\"id\":2,\"body\":\"deleted match\"}}]" else "[{\"_id\":\"same\",\"_score\":2,\"_source\":{\"id\":1}}]"
        else if (query.object.contains("filter_query"))
            "[{\"_id\":\"c1\",\"_score\":1,\"_source\":{\"id\":1,\"body\":\"nonmatching edit\"}},{\"_id\":\"c2\",\"_score\":1,\"_source\":{\"id\":2,\"deleted\":true}}]"
        else if (self.overlay)
            "[{\"_id\":\"c4\",\"_score\":1,\"_source\":{\"id\":4,\"body\":\"new match\"}}]"
        else if (self.changed)
            "[{\"_id\":\"same\",\"_score\":1,\"_source\":{\"id\":3}}]"
        else
            "[{\"_id\":\"same\",\"_score\":1,\"_source\":{\"id\":2}}]";
        const hits = try std.json.parseFromSliceLeaky(V, scratch, data, .{});
        return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .status = 200, .took = 0, .hits = .{ .total = .{ .value = hits.array.items.len, .relation = "exact" }, .hits = hits } }} }, .{}), false);
    }
    fn executor(self: *@This()) Executor {
        return .{ .ptr = self, .execute = run, .checkpoint = checkpoint };
    }
};
test "external lake composed query paginates a global result and expires changed cuts" {
    const a = std.testing.allocator;
    var fixture: TestExecutor = .{};
    const first_body = "{\"source\":{\"union\":[{\"table\":\"history\"},{\"table\":\"current\"}]},\"source_ranking\":\"rrf\",\"full_text_search\":{\"match_all\":{}},\"fields\":[\"id\"],\"limit\":1}";
    var first = try execute(a, first_body, fixture.executor());
    defer first.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const result = (try std.json.parseFromSliceLeaky(V, scratch, first.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqual(@as(i64, 2), result.object.get("hits").?.object.get("total").?.object.get("value").?.integer);
    try std.testing.expectEqualStrings("current", result.object.get("hits").?.object.get("hits").?.array.items[0].object.get("_table").?.string);
    var request = try std.json.parseFromSliceLeaky(V, scratch, first_body, .{});
    try request.object.put(scratch, "source_cursor", result.object.get("next_source_cursor").?);
    const next_body = try std.json.Stringify.valueAlloc(scratch, request, .{});
    var next = try execute(a, next_body, fixture.executor());
    defer next.deinit(a);
    const next_result = (try std.json.parseFromSliceLeaky(V, scratch, next.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqualStrings("history", next_result.object.get("hits").?.object.get("hits").?.array.items[0].object.get("_table").?.string);
    fixture.changed = true;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(a, next_body, fixture.executor()));
}
test "external lake keyed composition hides a newer nonmatching row and tombstone before ranking" {
    const a = std.testing.allocator;
    var fixture: TestExecutor = .{ .overlay = true };
    var response = try execute(a, "{\"source\":{\"overlay\":{\"base\":{\"table\":\"history\"},\"changes\":{\"table\":\"current\"},\"key\":[\"id\"]}},\"source_ranking\":\"rrf\",\"fields\":[\"body\"],\"limit\":5}", fixture.executor());
    defer response.deinit(a);
    var parsed = try std.json.parseFromSlice(V, a, response.body, .{});
    defer parsed.deinit();
    const result = parsed.value.object.get("responses").?.array.items[0].object.get("hits").?;
    try std.testing.expectEqual(@as(i64, 1), result.object.get("total").?.object.get("value").?.integer);
    const hits = result.object.get("hits").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqualStrings("c4", hits[0].object.get("_id").?.string);
    try std.testing.expect(!hits[0].object.get("_source").?.object.contains("id"));
}

test "external lake disjoint RRF union proves a bounded window over archive-scale totals" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn execute(_: *anyopaque, a: A, _: []const u8, _: []const u8) !Response {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            var hits: std.ArrayList(V) = .empty;
            for (0..4096) |rank| {
                const item = try std.json.Stringify.valueAlloc(scratch, .{ ._id = try std.fmt.allocPrint(scratch, "{d}", .{rank}), ._source = .{ .id = rank } }, .{});
                try hits.append(scratch, try std.json.parseFromSliceLeaky(V, scratch, item, .{}));
            }
            return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .hits = .{ .total = .{ .value = 1000000, .relation = "exact" }, .hits = hits.items } }} }, .{}), false);
        }
    };
    var marker: u8 = 0;
    const a = std.testing.allocator;
    var response = try execute(a, "{\"source\":{\"union\":[{\"table\":\"history\"},{\"table\":\"current\"}]},\"source_ranking\":\"rrf\",\"limit\":2}", .{ .ptr = &marker, .execute = Fixture.execute, .checkpoint = Fixture.checkpoint });
    defer response.deinit(a);
    var decoded = try std.json.parseFromSlice(V, a, response.body, .{});
    defer decoded.deinit();
    const hits = decoded.value.object.get("responses").?.array.items[0].object.get("hits").?;
    try std.testing.expectEqual(@as(i64, 2000000), hits.object.get("total").?.object.get("value").?.integer);
    try std.testing.expectEqual(@as(usize, 2), hits.object.get("hits").?.array.items.len);
}
