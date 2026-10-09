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

const std = @import("std");
const httpx = @import("httpx");
const types = @import("../storage/db/types.zig");
const expr = @import("expressions.zig");
const d = @import("decisions.zig");
const runtime = @import("runtime.zig");
const Json = std.json.Value;

/// Expression fields have the same visibility as public stored documents.
/// Keep this independent of the caller's final projection so hidden inputs
/// remain available without exposing storage revision metadata via _computed.
fn publicExpressionDocument(a: std.mem.Allocator, bytes: []const u8) !Json {
    var value = try std.json.parseFromSliceLeaky(Json, a, bytes, .{ .allocate = .alloc_always });
    @import("../storage/hierarchy_navigation.zig").stripPublicInternalFieldsValue(a, &value);
    return value;
}

pub fn apply(a: std.mem.Allocator, req: types.SearchRequest, result: *types.SearchResult, meta: anytype, options: anytype) !void {
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    return applyBounded(a, req, result, meta, options, budget.allocator()) catch |err| {
        if (budget.exhausted) return error.DecisionLimitExceeded;
        return err;
    };
}

fn applyBounded(a: std.mem.Allocator, req: types.SearchRequest, result: *types.SearchResult, meta: anytype, options: anytype, temporary: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(temporary);
    defer arena.deinit();
    const scratch = arena.allocator();
    var empty_registry = @import("../common/provider_registry.zig").Registry.init(scratch);
    defer empty_registry.deinit();
    const registry = options.decision_registry orelse &empty_registry;
    const raw = try std.json.parseFromSliceLeaky(Json, scratch, req.evaluation_json, .{});
    const plan = try expr.Plan.parse(scratch, raw);
    if (plan.graph_query) |name| return applyGraph(a, scratch, req, result, meta, options, raw, plan, name);
    const source_total = result.total_hits;
    const source_relation = result.total_hits_relation;
    if (plan.scope == .matches and (source_relation != .exact or result.hits.len != source_total)) return error.DecisionLimitExceeded;
    const count = @min(result.hits.len, plan.candidate_count);
    var fallback_io: ?std.Io.Threaded = null;
    defer if (fallback_io) |*io| io.deinit();
    const io = if (options.reranker_runtime) |service| service.io else if (options.backend_runtime) |backend| backend.io() orelse return error.QueryRuntimeUnavailable else blk: {
        fallback_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
        break :blk fallback_io.?.io();
    };
    var fallback_http: ?httpx.Client = null;
    defer if (fallback_http) |*http| http.deinit();
    const http = if (options.reranker_runtime) |service| &service.http else blk: {
        fallback_http = httpx.Client.initWithConfig(std.heap.smp_allocator, io, .{ .keep_alive = true, .cookies_enabled = false, .max_response_size = 1024 * 1024 });
        break :blk &fallback_http.?;
    };
    var execution: runtime.Runtime = .{ .profile_allocator = scratch, .registry = registry, .http = http, .io = io, .context = .{ .io = io, .deadline_ns = req.execution_deadline_ns, .cancellation = req.cancellation }, .source_table = options.source_table, .secret_store = options.secret_store, .antfly_provider = options.antfly_provider, .antfly_url = options.inference_api_url };
    const provider = execution.provider();
    try expr.validatePlanProviders(scratch, plan, provider);
    const documents = try scratch.alloc(Json, count);
    for (result.hits[0..count], documents) |hit, *doc| doc.* = if (hit.stored_data) |bytes| try publicExpressionDocument(scratch, bytes) else .null;
    const evaluations = try expr.evaluateBatch(scratch, plan, provider, documents);
    var indexes: std.ArrayList(usize) = .empty;
    for (evaluations, 0..) |row, i| if (row.accepted) {
        try indexes.append(scratch, i);
    };
    const accepted_documents = try scratch.alloc(Json, indexes.items.len);
    const accepted_rows = try scratch.alloc(expr.Evaluation, indexes.items.len);
    for (indexes.items, accepted_documents, accepted_rows) |index, *document, *row| {
        document.* = documents[index];
        row.* = evaluations[index];
    }
    const aggregations = try aggregate(scratch, plan, provider, accepted_documents, accepted_rows);
    if (plan.order_by) |order_by| {
        const keys = try scratch.alloc([]const Json, count);
        @memset(keys, &.{});
        for (indexes.items) |index| {
            keys[index] = try scratch.alloc(Json, order_by.array.items.len);
        }
        for (order_by.array.items, 0..) |order, column| {
            const values = try expr.evaluateExpressions(scratch, provider, order.object.get("expression").?, accepted_documents, accepted_rows);
            for (indexes.items, values) |index, value| @constCast(keys[index])[column] = value;
        }
        // Validate comparability before entering an infallible sort callback.
        for (0..order_by.array.items.len) |k| {
            var first: ?Json = null;
            for (indexes.items) |i| {
                if (keys[i][k] == .null) continue;
                if (first) |value| _ = try expr.compare(value, keys[i][k]) else first = keys[i][k];
            }
        }
        const Sort = struct {
            keys: []const []const Json,
            orders: []const Json,
            fn less(self: @This(), left: usize, right: usize) bool {
                for (self.orders, 0..) |order, k| {
                    const cmp = expr.compare(self.keys[left][k], self.keys[right][k]) catch unreachable;
                    if (cmp == .eq) continue;
                    const descending = if (order.object.get("descending")) |v| v.bool else false;
                    if (self.keys[left][k] == .null or self.keys[right][k] == .null) return cmp == .lt;
                    return cmp == if (descending) std.math.Order.gt else .lt;
                }
                return left < right;
            }
        };
        std.mem.sort(usize, indexes.items, Sort{ .keys = keys, .orders = order_by.array.items }, Sort.less);
    }
    const accepted = indexes.items.len;
    const start = @min(req.offset, accepted);
    const end = @min(start + req.limit, accepted);
    const selected = try a.alloc(types.SearchHit, end - start);
    errdefer a.free(selected);
    var initialized: usize = 0;
    errdefer for (selected[0..initialized]) |*hit| hit.deinit(a);
    for (indexes.items[start..end], selected) |index, *hit| {
        // Clone so any allocation failure leaves the original result intact.
        hit.* = try result.hits[index].clone(a);
        initialized += 1;
        if (hit.computed_json) |bytes| a.free(bytes);
        hit.computed_json = null;
        hit.computed_json = try std.json.Stringify.valueAlloc(a, evaluations[index].computed, .{});
    }
    const summary = try std.json.Stringify.valueAlloc(a, .{
        .scope = @tagName(plan.scope),
        .evaluated_rows = count,
        .accepted_rows = accepted,
        .source_total = source_total,
        .source_total_relation = @tagName(source_relation),
        .truncated = plan.scope == .candidates and (source_relation != .exact or source_total > count),
        .inference_rows = execution.rows,
        .batches = execution.batches,
        .latency_ns = execution.latency_ns,
        .provenance = execution.provenance.items,
        .cache_hits = 0,
        .usage = .{ .input_tokens = execution.input_tokens, .output_tokens = execution.output_tokens },
        .aggregations = aggregations,
    }, .{});
    for (result.hits) |*hit| hit.deinit(a);
    if (result.hits.len > 0) a.free(result.hits);
    result.hits = selected;
    result.total_hits = @intCast(accepted);
    result.total_hits_relation = .exact;
    if (meta.evaluation_json) |bytes| a.free(bytes);
    meta.evaluation_json = summary;
}
fn applyGraph(a: std.mem.Allocator, scratch: std.mem.Allocator, req: types.SearchRequest, result: *types.SearchResult, meta: anytype, options: anytype, raw: Json, plan: expr.Plan, name: []const u8) !void {
    var selected: ?*types.GraphSearchResult = null;
    for (result.graph_results) |*graph| if (std.mem.eql(u8, graph.name, name)) {
        selected = graph;
        break;
    };
    const graph = selected orelse return error.InvalidFunctionExpression;
    if (graph.aggregates.len > 0) return error.UnsupportedQueryRequest;
    if (plan.scope == .matches and graph.truncated) return error.DecisionLimitExceeded;
    const count = @min(graph.matches.len, plan.candidate_count);
    const hits = try a.alloc(types.SearchHit, count);
    var initialized: usize = 0;
    var hits_owned = false;
    errdefer if (!hits_owned) {
        for (hits[0..initialized]) |*hit| hit.deinit(a);
        a.free(hits);
    };
    for (graph.matches[0..count], hits, 0..) |match, *hit, ordinal| {
        var document = d.jsonObject();
        for (match.bindings) |binding| {
            var node = d.jsonObject();
            try d.put(scratch, &node, "key", .{ .string = binding.node.key });
            try d.put(scratch, &node, "table", if (binding.node.table) |table| .{ .string = table } else .null);
            try d.put(scratch, &node, "depth", .{ .integer = binding.node.depth });
            var payload: Json = .null;
            for (graph.hits) |stored| {
                if (!std.mem.eql(u8, stored.id, binding.node.key)) continue;
                if (!std.mem.eql(u8, stored.source_table orelse options.source_table, binding.node.table orelse options.source_table)) continue;
                if (stored.stored_data) |bytes| payload = try publicExpressionDocument(scratch, bytes);
                break;
            }
            try d.put(scratch, &node, "document", payload);
            try d.put(scratch, &document, binding.alias, node);
        }
        for (match.null_aliases) |alias| try d.put(scratch, &document, alias, .null);
        hit.* = .{ .id = try std.fmt.allocPrint(a, "{d}", .{ordinal}) };
        initialized += 1;
        hit.stored_data = try std.json.Stringify.valueAlloc(a, document, .{});
    }
    var relation: types.SearchResult = .{ .alloc = a, .hits = hits, .total_hits = @intCast(graph.matches.len), .total_hits_relation = if (graph.truncated) .gte else .exact };
    hits_owned = true;
    defer relation.deinit();
    var stage = raw;
    _ = stage.object.swapRemove("graph_query");
    const stage_json = try std.json.Stringify.valueAlloc(scratch, stage, .{});
    var stage_req = req;
    stage_req.evaluation_json = stage_json;
    for (req.graph_queries) |query| if (std.mem.eql(u8, query.name, name)) {
        if (query.query.evaluation_output_limit) |limit| stage_req.limit = @min(stage_req.limit, limit);
    };
    try applyBounded(a, stage_req, &relation, meta, options, scratch);
    const matches = try a.alloc(types.GraphPatternMatch, relation.hits.len);
    errdefer a.free(matches);
    const kept = try scratch.alloc(bool, graph.matches.len);
    @memset(kept, false);
    var ready: usize = 0;
    errdefer for (matches[0..ready]) |match| if (match.computed_json) |bytes| a.free(bytes);
    for (relation.hits, matches) |hit, *match| {
        const ordinal = try std.fmt.parseUnsigned(usize, hit.id, 10);
        match.* = graph.matches[ordinal];
        match.computed_json = try a.dupe(u8, hit.computed_json.?);
        kept[ordinal] = true;
        ready += 1;
    }
    for (graph.matches, 0..) |*match, i| {
        if (!kept[i]) match.deinit(a) else if (match.computed_json) |bytes| a.free(bytes);
    }
    if (graph.matches.len > 0) a.free(graph.matches);
    graph.matches = matches;
    graph.total_hits = relation.total_hits;
    graph.truncated = graph.truncated or (plan.scope == .candidates and count < kept.len) or relation.total_hits > matches.len + req.offset;
}

fn aggregate(a: std.mem.Allocator, plan: expr.Plan, provider: d.DecisionProvider, documents: []const Json, rows: []const expr.Evaluation) !Json {
    var output = d.jsonObject();
    const configs = plan.aggregations orelse return output;
    var it = configs.object.iterator();
    while (it.next()) |entry| {
        const config = entry.value_ptr.object;
        const kind = config.get("type").?.string;
        var sum: f64 = 0;
        var count: usize = 0;
        var buckets: std.StringArrayHashMapUnmanaged(struct { value: Json, count: usize }) = .empty;
        const values = try expr.evaluateExpressions(a, provider, config.get("expression").?, documents, rows);
        for (values) |value| {
            if (value == .null) continue;
            count += 1;
            if (std.mem.eql(u8, kind, "terms")) {
                const key = try std.json.Stringify.valueAlloc(a, value, .{});
                const bucket = try buckets.getOrPut(a, key);
                if (!bucket.found_existing) bucket.value_ptr.* = .{ .value = value, .count = 0 };
                bucket.value_ptr.count += 1;
            } else if (!std.mem.eql(u8, kind, "count")) {
                sum += switch (value) {
                    .integer => @as(f64, @floatFromInt(value.integer)),
                    .float => value.float,
                    else => return error.FunctionTypeMismatch,
                };
                if (!std.math.isFinite(sum)) return error.FunctionTypeMismatch;
            }
        }
        if (std.mem.eql(u8, kind, "terms")) {
            var array = std.json.Array.init(a);
            for (buckets.values()) |bucket| {
                var value = d.jsonObject();
                try d.put(a, &value, "key", bucket.value);
                try d.put(a, &value, "doc_count", .{ .integer = @intCast(bucket.count) });
                try array.append(value);
            }
            var result = d.jsonObject();
            try d.put(a, &result, "buckets", .{ .array = array });
            try d.put(a, &output, entry.key_ptr.*, result);
        } else {
            const value: Json = if (std.mem.eql(u8, kind, "count")) .{ .integer = @intCast(count) } else if (std.mem.eql(u8, kind, "avg")) if (count == 0) .null else .{ .float = sum / @as(f64, @floatFromInt(count)) } else .{ .float = sum };
            try d.put(a, &output, entry.key_ptr.*, value);
        }
    }
    return output;
}

test "decision functions candidate filtering sorting and aggregates reuse a named binding" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn decide(ptr: *anyopaque, alloc: std.mem.Allocator, body: []const u8, _: ?@import("antfly_inference_execution_context").RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .monotonic);
            const parsed = try std.json.parseFromSlice(Json, alloc, body, .{});
            defer parsed.deinit();
            const score: f64 = if (std.mem.eql(u8, parsed.value.object.get("input").?.string, "refund")) 0.9 else 0.1;
            return std.fmt.allocPrint(alloc, "{{\"model\":\"mock\",\"answers\":[{{\"name\":\"answer\",\"type\":\"predicate\",\"decision_method\":\"typed\",\"probability\":{d}}}],\"usage\":{{\"input_tokens\":2,\"output_tokens\":0}}}}", .{score});
        }
    };
    var fake: Fake = .{};
    var registry = @import("../common/provider_registry.zig").Registry.init(a);
    defer registry.deinit();
    try registry.registerDeciderConfig("local", .{ .provider = .antfly, .model = "mock" });
    const definition =
        \\{"scope":"candidates","candidate_count":3,"compute":{"p":{"call":"ai_probability","input":{"field":"body"},"statement":"Refund?","decider":"local"},"reused":{"ref":"p"}},"where":{"gte":[{"ref":"p"},{"literal":0.8}]},"order_by":[{"expression":{"ref":"reused"},"descending":true}],"aggregations":{"mean":{"type":"avg","expression":{"ref":"p"}}}}
    ;
    const hits = try a.alloc(types.SearchHit, 3);
    for (hits, 0..) |*hit, i| hit.* = .{ .id = try std.fmt.allocPrint(a, "{d}", .{i}), .score = 1, .stored_data = try a.dupe(u8, if (i == 0) "{\"body\":\"refund\"}" else if (i == 1) "{\"body\":\"other\"}" else "{\"body\":null}") };
    var result: types.SearchResult = .{ .alloc = a, .hits = hits, .total_hits = 50 };
    defer result.deinit();
    var meta = struct {
        evaluation_json: ?[]u8 = null,
    }{};
    defer if (meta.evaluation_json) |bytes| a.free(bytes);
    const options = struct {
        decision_registry: ?*const @import("../common/provider_registry.zig").Registry,
        reranker_runtime: ?*struct { io: std.Io, http: httpx.Client } = null,
        backend_runtime: ?*@import("../storage/background_runtime.zig").BackendRuntime = null,
        secret_store: ?*@import("../common/secrets.zig").FileStore = null,
        antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider,
        inference_api_url: ?[]const u8 = null,
        source_table: []const u8 = "docs",
    }{ .decision_registry = &registry, .antfly_provider = .{ .ptr = &fake, .embed_dense_texts = undefined, .embed_sparse_texts = undefined, .decide_json = Fake.decide } };
    const req: types.SearchRequest = .{ .evaluation_json = definition, .evaluation_limit = 3, .limit = 1 };
    try apply(a, req, &result, &meta, options);
    try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("0", result.hits[0].id);
    const computed = try std.json.parseFromSlice(Json, a, result.hits[0].computed_json.?, .{});
    defer computed.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), computed.value.object.get("reused").?.float, 0.001);
    const summary = try std.json.parseFromSlice(Json, a, meta.evaluation_json.?, .{});
    defer summary.deinit();
    try std.testing.expect(summary.value.object.get("truncated").?.bool);
    try std.testing.expectEqual(@as(i64, 3), summary.value.object.get("evaluated_rows").?.integer);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), summary.value.object.get("aggregations").?.object.get("mean").?.float, 0.001);
    var incomplete: types.SearchResult = .{ .alloc = a, .hits = &.{}, .total_hits = 10 };
    const matches = "{\"scope\":\"matches\",\"max_rows\":3,\"compute\":{\"x\":{\"literal\":1}}}";
    try std.testing.expectError(error.DecisionLimitExceeded, apply(a, .{ .evaluation_json = matches, .evaluation_limit = 3, .evaluation_matches = true }, &incomplete, &meta, options));
    const graph_hits = try a.alloc(types.SearchHit, 2);
    for (graph_hits, 0..) |*hit, i| hit.* = .{ .id = try std.fmt.allocPrint(a, "node{d}", .{i}), .stored_data = try a.dupe(u8, if (i == 0) "{\"title\":\"Public\",\"body\":\"refund\",\"_artifact_unit_fingerprint\":\"private\"}" else "{\"title\":\"Public\",\"body\":\"other\"}") };
    const graph_matches = try a.alloc(types.GraphPatternMatch, 2);
    for (graph_matches, 0..) |*match, i| {
        const bindings = try a.alloc(types.GraphPatternBinding, 1);
        bindings[0] = .{ .alias = try a.dupe(u8, "customer"), .node = .{ .key = try std.fmt.allocPrint(a, "node{d}", .{i}), .depth = 0, .distance = 0 } };
        match.* = .{ .bindings = bindings, .path = &.{} };
    }
    const graphs = try a.alloc(types.GraphSearchResult, 1);
    graphs[0] = .{ .name = try a.dupe(u8, "customers"), .matches = graph_matches, .hits = graph_hits, .total_hits = 2 };
    const base_hits = try a.alloc(types.SearchHit, 1);
    base_hits[0] = .{ .id = try a.dupe(u8, "base-offset-row") };
    var graph_result: types.SearchResult = .{ .alloc = a, .hits = base_hits, .total_hits = 9, .graph_results = graphs };
    defer graph_result.deinit();
    const graph_stage = "{\"graph_query\":\"customers\",\"scope\":\"matches\",\"max_rows\":2,\"compute\":{\"hidden\":{\"field\":\"customer.document._artifact_unit_fingerprint\"},\"p\":{\"call\":\"ai_probability\",\"input\":{\"field\":\"customer.document.body\"},\"statement\":\"Refund?\",\"decider\":\"local\"}},\"where\":{\"gte\":[{\"ref\":\"p\"},{\"literal\":0.8}]}}";
    const query = @import("../api/query_contract.zig");
    const body = try std.fmt.allocPrint(a, "{{\"limit\":2,\"graph_queries\":{{\"customers\":{{\"index\":\"graph\",\"match\":{{\"anchor\":\"customer\",\"nodes\":{{\"customer\":{{}}}},\"edges\":[]}},\"return\":{{\"bindings\":[\"customer\"],\"include_documents\":true,\"fields\":[\"title\"],\"limit\":2}}}}}},\"evaluate\":{s}}}", .{graph_stage});
    defer a.free(body);
    var request = try query.parseQueryRequest(a, null, "docs", body);
    defer request.deinit(a);
    const graph_query = request.req.graph_queries[0].query;
    try std.testing.expect(graph_query.defer_document_projection);
    try std.testing.expect(!graph_query.include_all_fields);
    const retrieval = graph_query.documentRetrievalQuery();
    try std.testing.expect(retrieval.include_all_fields);
    try std.testing.expectEqual(@as(usize, 0), retrieval.fields.len);
    request.req.limit = 1;
    try apply(a, request.req, &graph_result, &meta, options);
    try std.testing.expectEqual(@as(usize, 1), graph_result.hits.len);
    try std.testing.expectEqualStrings("base-offset-row", graph_result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 9), graph_result.total_hits);

    var encoded = try query.encodeQueryResponses(a, "docs", request.req, .{ .evaluation_json = meta.evaluation_json }, graph_result);
    defer encoded.deinit(a);
    const wire = try std.json.parseFromSlice(Json, a, encoded.json, .{});
    defer wire.deinit();
    const graph_wire = wire.value.object.get("responses").?.array.items[0].object.get("graph_results").?.object.get("customers").?;
    const document = graph_wire.object.get("rows").?.array.items[0].object.get("customer").?.object.get("document").?;
    try std.testing.expect(document.object.contains("title"));
    try std.testing.expect(!document.object.contains("body"));
    try std.testing.expect(graph_wire.object.get("computed").?.array.items[0].object.contains("p"));
    try std.testing.expect(graph_wire.object.get("computed").?.array.items[0].object.get("hidden").? == .null);
    try std.testing.expectEqual(@as(usize, 1), graphs[0].matches.len);
    try std.testing.expectEqualStrings("node0", graphs[0].matches[0].bindings[0].node.key);
    try std.testing.expect(graphs[0].matches[0].computed_json != null);

    // Direct calls in each stage form one wave, independent of row count.
    const direct_hits = try a.alloc(types.SearchHit, 4);
    for (direct_hits, 0..) |*hit, i| hit.* = .{ .id = try std.fmt.allocPrint(a, "direct{d}", .{i}), .stored_data = try a.dupe(u8, "{\"body\":\"refund\"}") };
    var direct_result: types.SearchResult = .{ .alloc = a, .hits = direct_hits, .total_hits = 4 };
    defer direct_result.deinit();
    const direct_stage =
        \\{"scope":"candidates","candidate_count":4,"compute":{"x":{"literal":1}},"where":{"gte":[{"call":"ai_probability","input":{"field":"body"},"statement":"Refund?","decider":"local"},{"literal":0.8}]},"order_by":[{"expression":{"call":"ai_probability","input":{"field":"body"},"statement":"Refund?","decider":"local"}}],"aggregations":{"literal":{"type":"avg","expression":{"call":"ai_probability","input":{"field":"body"},"statement":"Refund?","decider":"local"}}}}
    ;
    try apply(a, .{ .evaluation_json = direct_stage, .evaluation_limit = 4, .limit = 4 }, &direct_result, &meta, options);
    const direct_summary = try std.json.parseFromSlice(Json, a, meta.evaluation_json.?, .{});
    defer direct_summary.deinit();
    try std.testing.expectEqual(@as(i64, 3), direct_summary.value.object.get("batches").?.integer);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), direct_summary.value.object.get("aggregations").?.object.get("literal").?.float, 0.001);
    try std.testing.expectEqual(@as(usize, 4), direct_result.hits.len);

    var local_options = options;
    local_options.decision_registry = null;
    local_options.antfly_provider = null;
    const local_stage = "{\"scope\":\"candidates\",\"candidate_count\":1,\"compute\":{\"value\":{\"literal\":null}},\"where\":{\"is_null\":{\"ref\":\"value\"}}}";
    try apply(a, .{ .evaluation_json = local_stage, .evaluation_limit = 1, .limit = 1 }, &result, &meta, local_options);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(usize, 16), fake.calls.load(.monotonic));
}

test "decision functions expression documents hide storage revision markers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const document = try publicExpressionDocument(a,
        \\{"body":"public","_artifact_unit_fingerprint":"private","_hierarchy_unit_revision_token":"private"}
    );
    try std.testing.expect(document.object.contains("body"));
    var provider: @import("../sql/decision_eval.zig").testing.Provider = .{};
    const stage =
        \\{"scope":"candidates","candidate_count":1,"compute":{"artifact":{"field":"_artifact_unit_fingerprint"},"revision":{"field":"_hierarchy_unit_revision_token"},"public":{"field":"body"}}}
    ;
    const plan = try expr.Plan.parse(a, try std.json.parseFromSliceLeaky(Json, a, stage, .{}));
    const rows = try expr.evaluateBatch(a, plan, provider.provider(), &.{document});
    try std.testing.expect(rows[0].computed.object.get("artifact").? == .null);
    try std.testing.expect(rows[0].computed.object.get("revision").? == .null);
    try std.testing.expectEqualStrings("public", rows[0].computed.object.get("public").?.string);
}
