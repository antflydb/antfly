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

//! Paged composition over immutable leaf cuts. Visibility resolves before
//! ranking, and continuation stores only per-source positions, never an archive.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const V = std.json.Value;
const Response = @import("contextual_operations.zig").OwnedResponse;
pub const Executor = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!Response,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    save_cursor: ?*const fn (*anyopaque, A, []const u8) anyerror![]const u8 = null,
    load_cursor: ?*const fn (*anyopaque, A, []const u8) anyerror![]const u8 = null,
};
pub fn hasSource(a: A, body: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(V, a, body, .{});
    defer parsed.deinit();
    return parsed.value == .object and parsed.value.object.contains("source");
}
const KeyKind = local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.Kind;
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
fn rowIdentity(a: A, keys: []const []const u8, row: V, kinds: []const KeyKind) ![]const u8 {
    return local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, row, kinds) catch |err| switch (err) {
        error.InvalidLakeKey => error.UnsupportedQueryRequest,
        else => err,
    };
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
    var overlay_kinds: []const KeyKind = &.{};
    var tombstone_field: []const u8 = "deleted";
    const union_value = source.object.get("union") orelse overlay_source: {
        const spec = overlay orelse return error.UnsupportedQueryRequest;
        if (spec != .object or spec.object.count() < 3 or spec.object.count() > 5) return error.InvalidQueryRequest;
        for (spec.object.keys()) |field| if (!std.mem.eql(u8, field, "base") and !std.mem.eql(u8, field, "changes") and !std.mem.eql(u8, field, "key") and !std.mem.eql(u8, field, "tombstone_field") and !std.mem.eql(u8, field, "key_types")) return error.InvalidQueryRequest;
        const keys = spec.object.get("key") orelse return error.InvalidQueryRequest;
        if (keys != .array or keys.array.items.len == 0 or keys.array.items.len > 8) return error.InvalidQueryRequest;
        const names = try scratch.alloc([]const u8, keys.array.items.len);
        for (keys.array.items, names, 0..) |key, *name, ordinal| {
            if (key != .string or key.string.len == 0 or std.mem.indexOfAny(u8, key.string, "/.~") != null) return error.InvalidQueryRequest;
            for (names[0..ordinal]) |previous| if (std.mem.eql(u8, previous, key.string)) return error.InvalidQueryRequest;
            name.* = key.string;
        }
        overlay_keys = names;
        if (spec.object.get("key_types")) |types| {
            if (types != .array or types.array.items.len != names.len) return error.InvalidQueryRequest;
            const kinds = try scratch.alloc(KeyKind, names.len);
            for (types.array.items, kinds) |value, *kind| {
                if (value != .string) return error.InvalidQueryRequest;
                kind.* = std.meta.stringToEnum(KeyKind, value.string) orelse return error.InvalidQueryRequest;
            }
            overlay_kinds = kinds;
        }
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
    const aggregations = root.object.get("aggregations");
    const graph_spec = root.object.get("graph_queries");
    if (aggregations) |specification| {
        const requests = try local.api_query.parseAggregationRequestsJson(scratch, try std.json.Stringify.valueAlloc(scratch, specification, .{}));
        defer local.api_query.freeAggregationRequests(scratch, requests);
        try validateGlobalAggregations(requests);
    }
    for ([_][]const u8{ "hierarchy", "join", "graph_searches", "analyses", "document_renderer", "search_after", "search_before", "session_id", "connection_id", "remote_snapshot" }) |name| if (root.object.contains(name)) return error.UnsupportedQueryRequest;
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
    const expression_bytes = try std.json.Stringify.valueAlloc(scratch, .{ .source = source, .ranking = ranking, .identity = root.object.get("_source_identity"), .aggregations = aggregations, .graph_queries = graph_spec, .fields = root.object.get("fields") }, .{});
    _ = root.object.orderedRemove("_source_identity");
    _ = root.object.orderedRemove("aggregations");
    _ = root.object.orderedRemove("graph_queries");
    _ = root.object.orderedRemove("source");
    _ = root.object.orderedRemove("source_ranking");
    _ = root.object.orderedRemove("source_cursor");
    _ = root.object.orderedRemove("offset");
    _ = root.object.orderedRemove("count");
    try root.object.put(scratch, "limit", .{ .integer = 4096 });
    const requested_fields = root.object.get("fields");
    if (aggregations != null) _ = root.object.orderedRemove("fields");
    if (aggregations == null and overlay != null and requested_fields != null) {
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
    if (aggregations == null and count and overlay != null) {
        var fields: std.ArrayList(V) = .empty;
        for (overlay_keys) |key| try fields.append(scratch, .{ .string = key });
        try fields.append(scratch, .{ .string = tombstone_field });
        try root.object.put(scratch, "fields", .{ .array = fields.toManaged(scratch) });
    }
    return executeStreams(a, scratch, root, union_value.array.items, overlay_keys, overlay_kinds, tombstone_field, requested_fields, orders.items, rrf, overlay != null, limit, offset, count, cursor, expression_bytes, aggregations, graph_spec, executor, budget);
}

const batch_size = 128;
const Position = struct {
    table: []const u8,
    after: []const V = &.{},
    raw_count: usize = 0,
    visible_count: usize = 0,
    total: ?usize = null,
    remote_snapshot: ?[]const u8 = null,
    identity: []const u8 = "null",
};
const Continuation = struct { version: u16 = 2, fingerprint: []const u8, sources: []const Position, total: ?usize = null, aggregations: ?V = null };
fn clone(a: A, value: anytype, comptime T: type) !T {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    return std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always });
}
/// Canonicalize object keys recursively; JSON spelling/order must not change a
/// cursor's query identity. Array order remains semantically significant.
fn canonical(a: A, value: V) !V {
    return switch (value) {
        .object => |object| object: {
            const names = try a.dupe([]const u8, object.keys());
            std.mem.sort([]const u8, names, {}, struct {
                fn lt(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.lt);
            var result: V = .{ .object = .empty };
            for (names) |name| try result.object.put(a, name, try canonical(a, object.get(name).?));
            break :object result;
        },
        .array => |array| array: {
            var result: std.ArrayList(V) = .empty;
            for (array.items) |item| try result.append(a, try canonical(a, item));
            break :array .{ .array = result.toManaged(a) };
        },
        else => value,
    };
}
const Stream = struct {
    position: Position,
    arena: std.heap.ArenaAllocator,
    position_arena: std.heap.ArenaAllocator,
    hits: []Hit = &.{},
    hidden: []bool = &.{},
    at: usize = 0,
    failed: ?Response = null,
    fn refill(self: *Stream, stable: A, response_a: A, root: V, orders: []const Order, rrf: bool, executor: Executor) !void {
        try executor.checkpoint(executor.ptr);
        _ = self.arena.reset(.free_all);
        const pa = self.arena.allocator();
        var request = try clone(pa, root, V);
        try request.object.put(pa, "limit", .{ .integer = batch_size });
        if (rrf) {
            const encoded = try std.json.Stringify.valueAlloc(pa, [_]Order{.{ .field = "_score", .desc = true }}, .{});
            try request.object.put(pa, "order_by", try std.json.parseFromSliceLeaky(V, pa, encoded, .{}));
        }
        if (self.position.after.len != 0) try request.object.put(pa, "search_after", try clone(pa, self.position.after, V));
        if (self.position.remote_snapshot) |token| try request.object.put(pa, "remote_snapshot", .{ .string = token });
        var response = try executor.execute(executor.ptr, response_a, self.position.table, try std.json.Stringify.valueAlloc(pa, request, .{}));
        if (response.status != 200) {
            self.failed = response;
            return error.ComposedLeafFailed;
        }
        defer response.deinit(response_a);
        if (response.body.len > 32 * 1024 * 1024) return error.QueryCandidateBudgetExceeded;
        const decoded = try std.json.parseFromSliceLeaky(V, pa, response.body, .{ .allocate = .alloc_always });
        const responses = decoded.object.get("responses") orelse return error.InvalidQueryRequest;
        if (responses != .array or responses.array.items.len != 1) return error.InvalidQueryRequest;
        const result = responses.array.items[0];
        const identity = try std.json.Stringify.valueAlloc(pa, result.object.get("_composed_identity") orelse .null, .{});
        if (self.position.total != null and !std.mem.eql(u8, self.position.identity, identity)) return error.CatalogGenerationChanged;
        if (self.position.total == null) self.position.identity = try stable.dupe(u8, identity);
        const remote = result.object.get("remote_snapshot");
        if (remote) |value| {
            if (value != .string) return error.InvalidQueryRequest;
            if (self.position.remote_snapshot) |expected| {
                if (!std.mem.eql(u8, expected, value.string)) return error.CatalogGenerationChanged;
            } else self.position.remote_snapshot = try stable.dupe(u8, value.string);
        } else if (self.position.remote_snapshot != null) return error.CatalogGenerationChanged;
        const result_hits = result.object.get("hits") orelse return error.InvalidQueryRequest;
        const items = result_hits.object.get("hits") orelse return error.InvalidQueryRequest;
        const total = result_hits.object.get("total") orelse return error.InvalidQueryRequest;
        const remaining = if (total == .integer) try integer(total) else try integer(total.object.get("value") orelse return error.InvalidQueryRequest);
        if (total == .object) if (total.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
        // Native text reports the full matching relation; relational ordered
        // scans report the relation remaining after the tuple. Both are exact
        // contracts on the retained cut. Normalize before checking page size.
        if (self.position.total) |expected| {
            if (remaining != expected and try std.math.add(usize, self.position.raw_count, remaining) != expected) return error.CatalogGenerationChanged;
        } else self.position.total = remaining;
        const available = self.position.total.? - self.position.raw_count;
        if (items != .array or items.array.items.len != @min(batch_size, available)) return error.QueryCandidateBudgetExceeded;
        self.hits = try pa.alloc(Hit, items.array.items.len);
        self.hidden = try pa.alloc(bool, self.hits.len);
        @memset(self.hidden, false);
        self.at = 0;
        for (items.array.items, self.hits) |value, *hit| {
            try executor.checkpoint(executor.ptr);
            const id = value.object.get("_id") orelse return error.InvalidQueryRequest;
            if (id != .string) return error.InvalidQueryRequest;
            const keys: []const V = if (value.object.get("_sort")) |sort| if (sort == .array) sort.array.items else return error.InvalidQueryRequest else &.{};
            const id_is_last_order = std.mem.eql(u8, orders[orders.len - 1].field, "_id");
            if (keys.len != orders.len + 1 and !(id_is_last_order and keys.len == orders.len)) return error.UnsupportedQueryRequest;
            if (keys[keys.len - 1] != .string or !std.mem.eql(u8, keys[keys.len - 1].string, id.string)) return error.CatalogGenerationChanged;
            hit.* = .{ .value = value, .table = self.position.table, .id = id.string, .score = 0, .keys = keys };
        }
        // Leaf ordering is authoritative. A broken/nonadvancing adapter must
        // fail rather than loop or repeat candidates across pages.
        for (self.hits, 0..) |hit, i| {
            if (i > 0) {
                var previous = self.hits[i - 1];
                var current = hit;
                if (rrf) {
                    previous.score = scoreValue(previous.keys[0]);
                    current.score = scoreValue(current.keys[0]);
                }
                if (!less(orders, previous, current)) return error.CatalogGenerationChanged;
            } else if (self.position.after.len != 0) {
                const id = self.position.after[self.position.after.len - 1];
                if (id != .string) return error.InvalidQueryRequest;
                const previous: Hit = .{ .value = .null, .table = hit.table, .id = id.string, .keys = self.position.after, .score = if (rrf) scoreValue(self.position.after[0]) else 0 };
                var current = hit;
                if (rrf) current.score = scoreValue(current.keys[0]);
                if (!less(orders, previous, current)) return error.CatalogGenerationChanged;
            }
        }
    }
    fn advance(self: *Stream, stable: A, visible: bool) !void {
        _ = stable;
        _ = self.position_arena.reset(.free_all);
        self.position.after = try clone(self.position_arena.allocator(), self.hits[self.at].keys, []const V);
        self.position.raw_count = try std.math.add(usize, self.position.raw_count, 1);
        if (visible) self.position.visible_count = try std.math.add(usize, self.position.visible_count, 1);
        self.at += 1;
    }
    fn done(self: *const Stream) bool {
        return self.position.total != null and self.position.raw_count == self.position.total.?;
    }
};
fn scoreValue(value: V) f64 {
    return switch (value) {
        .float => value.float,
        .integer => @floatFromInt(value.integer),
        else => std.math.nan(f64),
    };
}
fn maskBatch(base: *Stream, changes: *const Stream, keys: []const []const u8, kinds: []const KeyKind, root: V, response_a: A, executor: Executor) !void {
    if (base.hits.len == 0) return;
    const pa = base.arena.allocator();
    var clauses: std.ArrayList(V) = .empty;
    var identities: std.StringHashMapUnmanaged(usize) = .empty;
    for (base.hits, 0..) |hit, i| {
        const row = hit.value.object.get("_source") orelse return error.UnsupportedQueryRequest;
        const identity = try rowIdentity(pa, keys, row, kinds);
        const unique = try identities.getOrPut(pa, identity);
        if (unique.found_existing) return error.InvalidQueryRequest;
        unique.value_ptr.* = i;
        var terms: std.ArrayList(V) = .empty;
        for (keys, 0..) |key, index| {
            const key_value = try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.normalize(pa, row.object.get(key).?, if (kinds.len == 0) .scalar else kinds[index]);
            const encoded = try std.json.Stringify.valueAlloc(pa, .{ .term = .{ .path = try std.fmt.allocPrint(pa, "/{s}", .{key}), .value = key_value } }, .{});
            try terms.append(pa, try std.json.parseFromSliceLeaky(V, pa, encoded, .{}));
        }
        try clauses.append(pa, try std.json.parseFromSliceLeaky(V, pa, try std.json.Stringify.valueAlloc(pa, .{ .conjuncts = terms.items }, .{}), .{}));
    }
    const lookup = try std.json.Stringify.valueAlloc(pa, .{ .full_text_search = .{ .match_all = .{} }, .filter_query = .{ .disjuncts = clauses.items }, .fields = keys, .limit = batch_size + 1, .remote_snapshot = changes.position.remote_snapshot, .lake_read = root.object.get("lake_read") }, .{ .emit_null_optional_fields = false });
    try executor.checkpoint(executor.ptr);
    var response = try executor.execute(executor.ptr, response_a, changes.position.table, lookup);
    if (response.status != 200) {
        base.failed = response;
        return error.ComposedLeafFailed;
    }
    defer response.deinit(response_a);
    const result = (try std.json.parseFromSliceLeaky(V, pa, response.body, .{})).object.get("responses").?.array.items[0];
    if (!std.mem.eql(u8, changes.position.identity, try std.json.Stringify.valueAlloc(pa, result.object.get("_composed_identity") orelse .null, .{}))) return error.CatalogGenerationChanged;
    if (changes.position.remote_snapshot) |token| {
        const actual = result.object.get("remote_snapshot") orelse return error.CatalogGenerationChanged;
        if (actual != .string or !std.mem.eql(u8, token, actual.string)) return error.CatalogGenerationChanged;
    }
    const hits = result.object.get("hits").?;
    const items = hits.object.get("hits").?.array.items;
    const total = hits.object.get("total").?;
    const total_count = if (total == .integer) try integer(total) else try integer(total.object.get("value").?);
    if (total == .object) if (total.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
    if (items.len != total_count or items.len > base.hits.len) return error.QueryCandidateBudgetExceeded;
    for (items) |hit| {
        const key = try rowIdentity(pa, keys, hit.object.get("_source").?, kinds);
        const i = identities.get(key) orelse return error.InvalidQueryRequest;
        if (base.hidden[i]) return error.InvalidQueryRequest;
        base.hidden[i] = true;
    }
}
fn maskTombstones(stream: *Stream, field: []const u8) !void {
    for (stream.hits, stream.hidden) |hit, *hidden| {
        const row = hit.value.object.get("_source") orelse return error.UnsupportedQueryRequest;
        if (row.object.get(field)) |value| {
            if (value != .bool and value != .null) return error.InvalidQueryRequest;
            hidden.* = value == .bool and value.bool;
        }
    }
}
fn executeStreams(a: A, scratch: A, root: V, leaves: []const V, keys: []const []const u8, kinds: []const KeyKind, tombstone: []const u8, fields: ?V, orders: []const Order, rrf: bool, overlay: bool, limit: usize, offset: usize, count: bool, cursor: ?V, expression: []const u8, aggregations: ?V, graph_spec: ?V, executor: Executor, budget: *local.sql_memory_budget) !Response {
    if (aggregations == null and graph_spec == null and count and !overlay) {
        var total: usize = 0;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (leaves) |leaf| {
            try executor.checkpoint(executor.ptr);
            if (leaf != .object or leaf.object.count() != 1) return error.InvalidQueryRequest;
            const name = leaf.object.get("table") orelse return error.InvalidQueryRequest;
            if (name != .string or (try seen.getOrPut(scratch, name.string)).found_existing) return error.InvalidQueryRequest;
            var request = try clone(scratch, root, V);
            try request.object.put(scratch, "count", .{ .bool = true });
            var response = try executor.execute(executor.ptr, a, name.string, try std.json.Stringify.valueAlloc(scratch, request, .{}));
            if (response.status != 200) return response;
            defer response.deinit(a);
            var page_arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer page_arena.deinit();
            const decoded = try std.json.parseFromSliceLeaky(V, page_arena.allocator(), response.body, .{});
            const count_value = decoded.object.get("responses").?.array.items[0].object.get("hits").?.object.get("total").?;
            if (count_value == .object) if (count_value.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
            total = try std.math.add(usize, total, if (count_value == .integer) try integer(count_value) else try integer(count_value.object.get("value").?));
        }
        return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .status = 200, .took = 0, .hits = .{ .total = .{ .value = total, .relation = "exact" }, .hits = [_]V{} }, .source_ranking = if (rrf) "rrf" else "ordered" }} }, .{}), false);
    }
    var normalized = try clone(scratch, root, V);
    _ = normalized.object.orderedRemove("limit");
    _ = normalized.object.orderedRemove("timeout_ms");
    const query_identity = try std.json.Stringify.valueAlloc(scratch, try canonical(scratch, try std.json.parseFromSliceLeaky(V, scratch, try std.json.Stringify.valueAlloc(scratch, .{ .query = normalized, .expression = try std.json.parseFromSliceLeaky(V, scratch, expression, .{}) }, .{}), .{})), .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(query_identity, &digest, .{});
    const fingerprint = std.fmt.bytesToHex(digest, .lower);
    var restored: ?Continuation = null;
    if (cursor) |value| {
        if (offset != 0 or count or value != .string) return error.InvalidQueryRequest;
        const bytes = if (executor.load_cursor) |load| try load(executor.ptr, scratch, value.string) else value.string;
        restored = std.json.parseFromSliceLeaky(Continuation, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidQueryRequest;
        if (restored.?.version != 2 or restored.?.sources.len != leaves.len or !std.mem.eql(u8, restored.?.fingerprint, &fingerprint)) return error.CatalogGenerationChanged;
    }
    const streams = try scratch.alloc(Stream, leaves.len);
    var initialized: usize = 0;
    defer for (streams[0..initialized]) |*stream| {
        if (stream.failed) |*response| response.deinit(a);
        stream.position_arena.deinit();
        stream.arena.deinit();
    };
    var names: std.StringHashMapUnmanaged(void) = .empty;
    for (leaves, streams, 0..) |leaf, *stream, i| {
        if (leaf != .object or leaf.object.count() != 1) return error.InvalidQueryRequest;
        const name = leaf.object.get("table") orelse return error.InvalidQueryRequest;
        if (name != .string or name.string.len == 0) return error.InvalidQueryRequest;
        if ((try names.getOrPut(scratch, name.string)).found_existing) return error.InvalidQueryRequest;
        stream.* = .{ .position = if (restored) |state| state.sources[i] else .{ .table = name.string }, .arena = std.heap.ArenaAllocator.init(budget.allocator()), .position_arena = std.heap.ArenaAllocator.init(budget.allocator()) };
        initialized += 1;
        if (!std.mem.eql(u8, stream.position.table, name.string) or stream.position.raw_count < stream.position.visible_count or (stream.position.total != null and stream.position.raw_count > stream.position.total.?)) return error.InvalidQueryRequest;
        // Reauthorize even exhausted leaves. A previous page cannot freeze grants.
        stream.refill(scratch, a, root, orders, rrf, executor) catch |err| {
            if (stream.failed) |response| {
                stream.failed = null;
                return response;
            }
            return err;
        };
    }
    if (overlay) {
        maskBatch(&streams[0], &streams[1], keys, kinds, root, a, executor) catch |err| {
            if (streams[0].failed) |response| {
                streams[0].failed = null;
                return response;
            }
            return err;
        };
        try maskTombstones(&streams[1], tombstone);
    }
    var exact_total: ?usize = if (restored) |state| state.total else null;
    if (!overlay) {
        var total: usize = 0;
        for (streams) |stream| total = try std.math.add(usize, total, stream.position.total.?);
        exact_total = total;
    } else if (exact_total == null) {
        const complete = for (streams) |stream| {
            if (stream.position.raw_count + stream.hits.len != stream.position.total.?) break false;
        } else true;
        if (complete) {
            var total: usize = 0;
            for (streams) |stream| {
                total += stream.position.visible_count;
                for (stream.hidden) |hidden| if (!hidden) {
                    total += 1;
                };
            }
            exact_total = total;
        }
    }
    var output: std.ArrayList(V) = .empty;
    const cached_aggregations = if (restored) |state| state.aggregations else null;
    const collect_aggregations = aggregations != null and cached_aggregations == null;
    var aggregation_hits: std.ArrayList(local.storage_db_types.SearchHit) = .empty;
    var page_positions: ?[]const Position = null;
    var skipped: usize = 0;
    while (true) {
        try executor.checkpoint(executor.ptr);
        var selected: ?usize = null;
        var best: Hit = undefined;
        for (streams, 0..) |*stream, i| {
            while (true) {
                if (stream.done()) break;
                if (stream.at == stream.hits.len) {
                    stream.refill(scratch, a, root, orders, rrf, executor) catch |err| {
                        if (stream.failed) |response| {
                            stream.failed = null;
                            return response;
                        }
                        return err;
                    };
                    if (overlay) {
                        if (i == 0) maskBatch(stream, &streams[1], keys, kinds, root, a, executor) catch |err| {
                            if (stream.failed) |response| {
                                stream.failed = null;
                                return response;
                            }
                            return err;
                        } else try maskTombstones(stream, tombstone);
                    }
                }
                if (stream.hidden[stream.at]) {
                    try stream.advance(scratch, false);
                    continue;
                }
                break;
            }
            if (stream.done()) continue;
            var hit = stream.hits[stream.at];
            if (rrf) hit.score = 1.0 / (60.0 + @as(f64, @floatFromInt(stream.position.visible_count + 1)));
            if (selected != null and !rrf) for (orders, 0..) |_, column| {
                if (hit.keys[column] != .null and best.keys[column] != .null) _ = local.sql_scalar.compare(hit.keys[column], best.keys[column]) catch return error.UnsupportedQueryRequest;
            };
            if (selected == null or less(orders, hit, best)) {
                selected = i;
                best = hit;
            }
        }
        const i = selected orelse break;
        if (!count and skipped >= offset and output.items.len == limit) {
            if (!collect_aggregations) break;
            if (page_positions == null) {
                const saved = try scratch.alloc(Position, streams.len);
                for (streams, saved) |stream, *position| position.* = stream.position;
                page_positions = try clone(scratch, saved, []const Position);
            }
        }
        if (collect_aggregations) {
            if (aggregation_hits.items.len == local.storage_db_aggregations.max_aggregation_source_hits) return error.QueryCandidateBudgetExceeded;
            try aggregation_hits.append(scratch, .{ .id = try scratch.dupe(u8, best.id), .source_table = try scratch.dupe(u8, best.table), .stored_data = try std.json.Stringify.valueAlloc(scratch, best.value.object.get("_source") orelse return error.UnsupportedQueryRequest, .{}) });
        }
        if (!count and skipped >= offset and output.items.len < limit) {
            var hit = try clone(scratch, best.value, V);
            try hit.object.put(scratch, "_table", .{ .string = best.table });
            if (rrf) {
                try hit.object.put(scratch, "_score", .{ .float = best.score });
                _ = hit.object.orderedRemove("_index_scores");
                _ = hit.object.orderedRemove("_score_details");
            }
            if (overlay or aggregations != null) if (fields) |requested| {
                var projected: V = .{ .object = .empty };
                const row = hit.object.get("_source").?;
                for (requested.array.items) |field| {
                    if (field != .string) return error.InvalidQueryRequest;
                    try projected.object.put(scratch, field.string, row.object.get(field.string) orelse .null);
                }
                try hit.object.put(scratch, "_source", projected);
            };
            _ = hit.object.orderedRemove("_sort");
            try output.append(scratch, hit);
        }
        try streams[i].advance(scratch, true);
        skipped += 1;
    }
    var seen: usize = 0;
    var complete = true;
    var positions: []const Position = try scratch.alloc(Position, streams.len);
    for (streams, @constCast(positions)) |stream, *position| {
        position.* = stream.position;
        seen += stream.position.visible_count;
        complete = complete and stream.done();
    }
    if (complete) exact_total = seen;
    const computed_aggregations = if (collect_aggregations) try globalAggregations(scratch, aggregations.?, aggregation_hits.items) else cached_aggregations;
    if (page_positions) |saved| {
        positions = saved;
        complete = false;
    }
    const next: ?[]const u8 = if (!count and !complete) next: {
        // Retained pagination needs an actual immutable serving token. Native
        // mutable tables without snapshot support cannot pretend to be pinned.
        if (executor.save_cursor != null) for (positions) |position| if (position.remote_snapshot == null) return error.UnsupportedQueryRequest;
        const bytes = try std.json.Stringify.valueAlloc(scratch, Continuation{ .fingerprint = &fingerprint, .sources = positions, .total = exact_total, .aggregations = computed_aggregations }, .{});
        break :next if (executor.save_cursor) |save| try save(executor.ptr, scratch, bytes) else bytes;
    } else null;
    const graph_results: ?V = if (graph_spec) |specification| graph: {
        const graph = @import("composed_graph.zig");
        const graph_leaves = try scratch.alloc(graph.Leaf, positions.len);
        for (positions, graph_leaves) |position, *leaf| {
            const identity = try std.json.parseFromSliceLeaky(V, scratch, position.identity, .{});
            const physical = if (identity == .object) identity.object.get("physical") else null;
            leaf.* = .{ .table = position.table, .physical = if (physical) |name| name.string else position.table, .token = position.remote_snapshot orelse return error.UnsupportedQueryRequest, .identity = position.identity };
        }
        break :graph try graph.execute(scratch, specification, graph_leaves, keys, kinds, tombstone, root.object.get("lake_visibility"), output.items, complete and offset == 0, executor);
    } else null;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .status = 200, .took = 0, .hits = .{ .total = .{ .value = exact_total orelse seen, .relation = if (exact_total != null) "exact" else "gte" }, .hits = output.items }, .source_ranking = if (rrf) "rrf" else "ordered", .next_source_cursor = next, .aggregations = computed_aggregations, .graph_results = graph_results }} }, .{ .emit_null_optional_fields = false });
    errdefer a.free(encoded);
    try executor.checkpoint(executor.ptr);
    return @import("contextual_operations.zig").json(encoded, false);
}

/// Shared native collector semantics, after overlay visibility and before
/// projection/pagination. Complete source sets are admitted against the same
/// bounded aggregation and memory budgets as native queries.
fn validateGlobalAggregations(requests: []const local.storage_db_aggregations_contract.SearchAggregationRequest) !void {
    for (requests) |request| {
        // These require a pinned background corpus or an indexed join; visible
        // result rows alone cannot provide their semantics.
        if (std.mem.eql(u8, request.type, "significant_terms") or request.background_query != null or request.algebraic_join != null) return error.UnsupportedQueryRequest;
        try validateGlobalAggregations(request.aggregations);
    }
}
fn globalAggregations(a: A, specification: V, hits: []local.storage_db_types.SearchHit) !V {
    const query_api = local.api_query;
    const bytes = try std.json.Stringify.valueAlloc(a, specification, .{});
    const requests = try query_api.parseAggregationRequestsJson(a, bytes);
    defer query_api.freeAggregationRequests(a, requests);
    const results = try local.storage_db_aggregations.computeSearchAggregations(a, requests, .{ .alloc = a, .hits = hits, .total_hits = @intCast(hits.len) }, .{});
    defer local.storage_db_aggregations.deinitResults(a, results);
    var response = try query_api.encodeQueryResponses(a, "composed", .{ .aggregations_json = bytes }, .{ .aggregation_results = results }, .{ .alloc = a, .hits = &.{}, .total_hits = @intCast(hits.len) });
    defer response.deinit(a);
    const value = try std.json.parseFromSliceLeaky(V, a, response.json, .{ .allocate = .alloc_always });
    return value.object.get("responses").?.array.items[0].object.get("aggregations") orelse return error.InvalidSqlBackendResponse;
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
    compact_id_sort: bool = false,
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
        var hits = try std.json.parseFromSliceLeaky(V, scratch, data, .{});
        const full_total = hits.array.items.len;
        if (!query.object.contains("filter_query")) {
            var output: std.ArrayList(V) = .empty;
            for (hits.array.items) |hit| {
                var value = hit;
                const score = value.object.get("_score").?;
                const id = value.object.get("_id").?;
                if (query.object.get("search_after")) |after| {
                    if (self.compact_id_sort) {
                        if (std.mem.order(u8, id.string, after.array.items[0].string) != .gt) continue;
                    } else if (compareValue(score, after.array.items[0]) == .gt or (compareValue(score, after.array.items[0]) == .eq and std.mem.order(u8, id.string, after.array.items[1].string) != .gt)) continue;
                }
                try value.object.put(scratch, "_sort", if (self.compact_id_sort) try clone(scratch, [_]V{id}, V) else try clone(scratch, [_]V{ score, id }, V));
                try output.append(scratch, value);
            }
            hits = .{ .array = output.toManaged(scratch) };
        }
        return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .status = 200, .took = 0, ._composed_identity = self.changed, .remote_snapshot = table, .hits = .{ .total = .{ .value = full_total, .relation = "exact" }, .hits = hits } }} }, .{}), false);
    }
    fn executor(self: *@This()) Executor {
        return .{ .ptr = self, .execute = run, .checkpoint = checkpoint };
    }
};
test "external lake composed global aggregations precede projection and retain totals across pages" {
    const a = std.testing.allocator;
    var fixture: TestExecutor = .{};
    const body = "{\"source\":{\"union\":[{\"table\":\"history\"},{\"table\":\"current\"}]},\"source_ranking\":\"rrf\",\"fields\":[\"body\"],\"limit\":1,\"aggregations\":{\"ids\":{\"type\":\"stats\",\"field\":\"id\"}}}";
    var first = try execute(a, body, fixture.executor());
    defer first.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const result = (try std.json.parseFromSliceLeaky(V, scratch, first.body, .{})).object.get("responses").?.array.items[0];
    const stats = result.object.get("aggregations").?.object.get("ids").?;
    try std.testing.expectEqual(@as(i64, 2), stats.object.get("count").?.integer);
    try std.testing.expectEqual(@as(f64, 3), switch (stats.object.get("sum").?) {
        .integer => |n| @as(f64, @floatFromInt(n)),
        .float => |n| n,
        else => return error.TestUnexpectedResult,
    });
    var request = try std.json.parseFromSliceLeaky(V, scratch, body, .{});
    try request.object.put(scratch, "source_cursor", result.object.get("next_source_cursor").?);
    var second = try execute(a, try std.json.Stringify.valueAlloc(scratch, request, .{}), fixture.executor());
    defer second.deinit(a);
    const next = (try std.json.parseFromSliceLeaky(V, scratch, second.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(scratch, stats, .{}), try std.json.Stringify.valueAlloc(scratch, next.object.get("aggregations").?.object.get("ids").?, .{}));
    try std.testing.expectEqual(@as(i64, 2), next.object.get("hits").?.object.get("total").?.object.get("value").?.integer);
    fixture.overlay = true;
    var overlay = try execute(a, "{\"source\":{\"overlay\":{\"base\":{\"table\":\"history\"},\"changes\":{\"table\":\"current\"},\"key\":[\"id\"]}},\"source_ranking\":\"rrf\",\"limit\":1,\"aggregations\":{\"ids\":{\"type\":\"stats\",\"field\":\"id\"}}}", fixture.executor());
    defer overlay.deinit(a);
    const visible = (try std.json.parseFromSliceLeaky(V, scratch, overlay.body, .{})).object.get("responses").?.array.items[0].object.get("aggregations").?.object.get("ids").?;
    try std.testing.expectEqual(@as(i64, 1), visible.object.get("count").?.integer);
}
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
        fn execute(_: *anyopaque, a: A, _: []const u8, body: []const u8) !Response {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const query = try std.json.parseFromSliceLeaky(V, scratch, body, .{});
            const start = if (query.object.get("search_after")) |after| (try std.fmt.parseInt(usize, after.array.items[1].string, 10)) + 1 else 0;
            const maximum = try integer(query.object.get("limit").?);
            var hits: std.ArrayList(V) = .empty;
            for (start..@min(start + maximum, 1000000)) |rank| {
                const id = try std.fmt.allocPrint(scratch, "{d:0>8}", .{rank});
                const item = try std.json.Stringify.valueAlloc(scratch, .{ ._id = id, ._sort = [_]V{ .{ .float = 1.0 / @as(f64, @floatFromInt(rank + 1)) }, .{ .string = id } }, ._source = .{ .id = rank } }, .{});
                try hits.append(scratch, try std.json.parseFromSliceLeaky(V, scratch, item, .{}));
            }
            return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .hits = .{ .total = .{ .value = 1000000 - start, .relation = "exact" }, .hits = hits.items } }} }, .{}), false);
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

const ArchiveOverlayFixture = struct {
    calls: usize = 0,
    checks: usize = 0,
    cancel_after: ?usize = null,
    fn checkpoint(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.checks += 1;
        if (self.cancel_after) |maximum| if (self.checks >= maximum) return error.Cancelled;
    }
    fn run(raw: *anyopaque, a: A, table: []const u8, body: []const u8) !Response {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const pa = arena.allocator();
        const request = try std.json.parseFromSliceLeaky(V, pa, body, .{});
        var hits: std.ArrayList(V) = .empty;
        var remaining: usize = 0;
        if (std.mem.eql(u8, table, "history")) {
            const start = if (request.object.get("search_after")) |after| (try std.fmt.parseInt(usize, after.array.items[1].string[1..], 10)) + 1 else 0;
            remaining = 10003 - start;
            const maximum = try integer(request.object.get("limit").?);
            for (start..@min(start + maximum, 10003)) |rank| {
                const id = try std.fmt.allocPrint(pa, "h{d:0>8}", .{rank});
                const value = try std.json.Stringify.valueAlloc(pa, .{ ._id = id, ._sort = [_]V{ .{ .float = 1.0 / @as(f64, @floatFromInt(rank + 1)) }, .{ .string = id } }, ._source = .{ .id = rank, .body = "old matching version" } }, .{});
                try hits.append(pa, try std.json.parseFromSliceLeaky(V, pa, value, .{}));
            }
        } else if (request.object.get("filter_query")) |filter| {
            for (filter.object.get("disjuncts").?.array.items) |clause| {
                const id = clause.object.get("conjuncts").?.array.items[0].object.get("term").?.object.get("value").?.integer;
                if (id >= 5000) continue;
                const value = try std.json.Stringify.valueAlloc(pa, .{ ._id = try std.fmt.allocPrint(pa, "c{d}", .{id}), ._source = .{ .id = id, .deleted = @mod(id, 2) == 0, .body = "new nonmatching version" } }, .{});
                try hits.append(pa, try std.json.parseFromSliceLeaky(V, pa, value, .{}));
            }
            remaining = hits.items.len;
        } else if (!request.object.contains("search_after")) {
            remaining = 1;
            const value = try std.json.Stringify.valueAlloc(pa, .{ ._id = "c10004", ._sort = [_]V{ .{ .float = 1 }, .{ .string = "c10004" } }, ._source = .{ .id = 10004, .body = "new match", .deleted = false } }, .{});
            try hits.append(pa, try std.json.parseFromSliceLeaky(V, pa, value, .{}));
        }
        return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ ._composed_identity = "stable", .remote_snapshot = table, .hits = .{ .total = .{ .value = remaining, .relation = "exact" }, .hits = hits.items } }} }, .{}), false);
    }
    fn executor(self: *@This()) Executor {
        return .{ .ptr = self, .execute = run, .checkpoint = checkpoint };
    }
};
test "external lake archive overlay streams past five thousand masked candidates and continues without replay" {
    const a = std.testing.allocator;
    var fixture: ArchiveOverlayFixture = .{};
    const body = "{\"source\":{\"overlay\":{\"base\":{\"table\":\"history\"},\"changes\":{\"table\":\"current\"},\"key\":[\"id\"]}},\"source_ranking\":\"rrf\",\"limit\":3}";
    var first = try execute(a, body, fixture.executor());
    defer first.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const result = (try std.json.parseFromSliceLeaky(V, scratch, first.body, .{})).object.get("responses").?.array.items[0];
    const items = result.object.get("hits").?.object.get("hits").?.array.items;
    try std.testing.expectEqual(@as(i64, 10004), items[0].object.get("_source").?.object.get("id").?.integer);
    try std.testing.expectEqual(@as(i64, 5000), items[1].object.get("_source").?.object.get("id").?.integer);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / 61.0), items[1].object.get("_score").?.float, 0.0000001);
    try std.testing.expectEqualStrings("gte", result.object.get("hits").?.object.get("total").?.object.get("relation").?.string);
    const before = fixture.calls;
    var request = try std.json.parseFromSliceLeaky(V, scratch, body, .{});
    try request.object.put(scratch, "source_cursor", result.object.get("next_source_cursor").?);
    var next = try execute(a, try std.json.Stringify.valueAlloc(scratch, request, .{}), fixture.executor());
    defer next.deinit(a);
    const next_result = (try std.json.parseFromSliceLeaky(V, scratch, next.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqual(@as(i64, 5002), next_result.object.get("hits").?.object.get("hits").?.array.items[0].object.get("_source").?.object.get("id").?.integer);
    try std.testing.expectEqual(@as(usize, 3), fixture.calls - before);
    _ = request.object.orderedRemove("source_cursor");
    try request.object.put(scratch, "count", .{ .bool = true });
    var counted = try execute(a, try std.json.Stringify.valueAlloc(scratch, request, .{}), fixture.executor());
    defer counted.deinit(a);
    const total = (try std.json.parseFromSliceLeaky(V, scratch, counted.body, .{})).object.get("responses").?.array.items[0].object.get("hits").?.object.get("total").?;
    try std.testing.expectEqual(@as(i64, 5004), total.object.get("value").?.integer);
    try std.testing.expectEqualStrings("exact", total.object.get("relation").?.string);
    fixture.cancel_after = fixture.checks + 20;
    try std.testing.expectError(error.Cancelled, execute(a, body, fixture.executor()));
}

test "external lake overlay identities canonicalize numeric and explicit timestamp keys without rounding large integers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try std.json.parseFromSliceLeaky(V, a, "{\"id\":7,\"time\":\"2026-10-09T08:00:00-07:00\"}", .{});
    const right = try std.json.parseFromSliceLeaky(V, a, "{\"id\":7.0,\"time\":\"2026-10-09T15:00:00Z\"}", .{});
    try std.testing.expectEqualStrings(try rowIdentity(a, &.{ "id", "time" }, left, &.{ .number, .timestamp }), try rowIdentity(a, &.{ "id", "time" }, right, &.{ .number, .timestamp }));
    const precise = try std.json.parseFromSliceLeaky(V, a, "{\"id\":9007199254740993}", .{});
    const rounded = try std.json.parseFromSliceLeaky(V, a, "{\"id\":9007199254740992}", .{});
    try std.testing.expect(!std.mem.eql(u8, try rowIdentity(a, &.{"id"}, precise, &.{.number}), try rowIdentity(a, &.{"id"}, rounded, &.{.number})));
}

test "external lake composed ID ordering accepts native cursor without a duplicate tie breaker" {
    const a = std.testing.allocator;
    var fixture: TestExecutor = .{ .compact_id_sort = true };
    const body = "{\"source\":{\"union\":[{\"table\":\"history\"},{\"table\":\"current\"}]},\"order_by\":[{\"field\":\"_id\"}],\"limit\":1}";
    var first = try execute(a, body, fixture.executor());
    defer first.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const result = (try std.json.parseFromSliceLeaky(V, scratch, first.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqualStrings("current", result.object.get("hits").?.object.get("hits").?.array.items[0].object.get("_table").?.string);
    var request = try std.json.parseFromSliceLeaky(V, scratch, body, .{});
    try request.object.put(scratch, "source_cursor", result.object.get("next_source_cursor").?);
    var second = try execute(a, try std.json.Stringify.valueAlloc(scratch, request, .{}), fixture.executor());
    defer second.deinit(a);
    const next = (try std.json.parseFromSliceLeaky(V, scratch, second.body, .{})).object.get("responses").?.array.items[0];
    try std.testing.expectEqualStrings("history", next.object.get("hits").?.object.get("hits").?.array.items[0].object.get("_table").?.string);
}

test "external lake composed aggregations reject background corpus operators" {
    var fixture: TestExecutor = .{};
    try std.testing.expectError(error.UnsupportedQueryRequest, execute(std.testing.allocator, "{\"source\":{\"union\":[{\"table\":\"history\"},{\"table\":\"current\"}]},\"source_ranking\":\"rrf\",\"aggregations\":{\"significant\":{\"type\":\"significant_terms\",\"field\":\"body\"}}}", fixture.executor()));
}
