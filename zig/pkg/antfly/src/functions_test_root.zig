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

pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("antfly_local_sources").search_query_string;
    _ = @import("antfly_local_sources").api_public_query_string;
    // Value-only filters must reach their helpers without compiling DB query
    // fixtures merely to discover their private test dependencies.
    _ = @import("antfly_local_sources").storage_db_query_search_exec;
    _ = @import("antfly_local_sources").sql_document_row;
    _ = @import("antfly_local_sources").storage_document_mutation_revision;
    _ = @import("antfly_local_sources").storage_conditional_source;
    _ = @import("antfly_local_sources").storage_logical_source;
    _ = @import("antfly_local_sources").storage_document_projection;
    _ = @import("antfly_local_sources").functions_decisions;
    _ = @import("antfly_local_sources").functions_expressions;
    _ = @import("antfly_local_sources").functions_runtime;
    _ = @import("antfly_local_sources").functions_query_eval;
    _ = @import("antfly_local_sources").asset_producer_runtime;
}

test "decision functions public DSL preserves evaluation inputs and rejects conflicting stages" {
    const std = @import("std");
    const query = @import("antfly_local_sources").api_query_contract;
    const a = std.testing.allocator;
    var parsed = try query.parseQueryRequest(a, null, "docs",
        \\{"fields":[],"limit":2,"evaluate":{"scope":"candidates","candidate_count":20,"compute":{"null_value":{"literal":null},"p":{"call":"ai_probability","input":{"field":"private_body"},"statement":"Refund?","decider":"local"}}}}
    );
    defer parsed.deinit(a);
    try std.testing.expectEqual(@as(u32, 20), parsed.req.evaluation_limit);
    try std.testing.expectEqual(@as(u32, 2), parsed.req.limit);
    try std.testing.expect(parsed.req.include_stored);
    try std.testing.expect(parsed.req.defer_stored_projection);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const raw = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), parsed.req.evaluation_json, .{});
    try std.testing.expect(raw.object.get("compute").?.object.get("null_value").?.object.contains("literal"));
    var dense = try query.parseQueryRequest(a, null, "docs",
        \\{"embeddings":{"vector":[0.1,0.2]},"evaluate":{"scope":"candidates","candidate_count":20,"compute":{"x":{"literal":1}}}}
    );
    defer dense.deinit(a);
    try std.testing.expect(dense.req.evaluation_json.len > 0);
    try std.testing.expectError(error.UnsupportedQueryRequest, query.parseQueryRequest(a, null, "docs",
        \\{"search_after":["x"],"evaluate":{"scope":"candidates","candidate_count":20,"compute":{"x":{"literal":1}}}}
    ));
}

test "decision functions worker transport fetches hidden inputs independently of final projection" {
    const std = @import("std");
    const query = @import("antfly_local_sources").api_query_contract;
    const wire = @import("antfly_local_sources").api_local_query_contract;
    const a = std.testing.allocator;
    for ([_][]const u8{ "[]", "[\"id\"]", "[\"nested.value\"]" }) |fields| {
        const body = try std.fmt.allocPrint(a, "{{\"fields\":{s},\"evaluate\":{{\"scope\":\"candidates\",\"candidate_count\":20,\"compute\":{{\"p\":{{\"call\":\"ai_probability\",\"input\":{{\"field\":\"body\"}},\"statement\":\"Refund?\",\"decider\":\"local\"}}}}}}}}", .{fields});
        defer a.free(body);
        var original = try query.parseQueryRequest(a, null, "docs", body);
        defer original.deinit(a);
        var shard = original.req;
        shard.limit = shard.evaluation_limit;
        shard.evaluation_json = "";
        shard.evaluation_limit = 0;
        shard.evaluation_matches = false;
        const encoded = try wire.encodeQueryRequest(a, shard);
        defer a.free(encoded);
        var worker = try query.parseQueryRequest(a, null, "docs", encoded);
        defer worker.deinit(a);
        try std.testing.expect(worker.req.include_stored);
        try std.testing.expect(worker.req.include_all_fields);
        try std.testing.expectEqual(@as(u32, 20), worker.req.limit);
        try std.testing.expectEqual(@as(u32, 0), worker.req.evaluation_limit);
        try std.testing.expect(!original.req.include_all_fields);
        // Exercise both response encoders: workers retain the input document,
        // while the coordinator emits only the original caller projection.
        const types = @import("antfly_local_sources").storage_db_types;
        var hits = [_]types.SearchHit{.{
            .id = @constCast("doc"),
            .stored_data = @constCast("{\"id\":\"doc\",\"body\":\"refund\",\"nested\":{\"value\":\"kept\"}}"),
            .computed_json = @constCast("{\"p\":0.9}"),
        }};
        const result: types.SearchResult = .{ .alloc = a, .hits = &hits, .total_hits = 1 };
        var fetched = try query.encodeQueryResponses(a, "docs", worker.req, .{}, result);
        defer fetched.deinit(a);
        const fetched_json = try std.json.parseFromSlice(std.json.Value, a, fetched.json, .{});
        defer fetched_json.deinit();
        const fetched_source = fetched_json.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0].object.get("_source").?;
        try std.testing.expectEqualStrings("refund", fetched_source.object.get("body").?.string);
        var projected = try query.encodeQueryResponses(a, "docs", original.req, .{}, result);
        defer projected.deinit(a);
        const projected_json = try std.json.parseFromSlice(std.json.Value, a, projected.json, .{});
        defer projected_json.deinit();
        const hit = projected_json.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items[0];
        if (hit.object.get("_source")) |source| try std.testing.expect(!source.object.contains("body"));
        try std.testing.expectApproxEqAbs(@as(f64, 0.9), hit.object.get("_computed").?.object.get("p").?.float, 0.001);
        // Forwarding must leave the coordinator's caller projection intact.
        try std.testing.expectEqual(@as(usize, if (std.mem.eql(u8, fields, "[]")) 0 else 1), original.req.fields.len);
    }
    var ordinary = try query.parseQueryRequest(a, null, "docs", "{\"fields\":[]}");
    defer ordinary.deinit(a);
    const encoded = try wire.encodeQueryRequest(a, ordinary.req);
    defer a.free(encoded);
    var worker = try query.parseQueryRequest(a, null, "docs", encoded);
    defer worker.deinit(a);
    try std.testing.expect(!worker.req.include_stored);
}

test "decision functions graph evaluation separates collection and output windows" {
    const std = @import("std");
    const query = @import("antfly_local_sources").api_query_contract;
    const a = std.testing.allocator;
    var request = try query.parseQueryRequest(a, null, "docs",
        \\{"limit":1,"graph_queries":{"customers":{"index":"graph","match":{"anchor":"customer","nodes":{"customer":{}},"edges":[]},"return":{"bindings":["customer"],"include_documents":true,"limit":1}}},"evaluate":{"graph_query":"customers","scope":"matches","max_rows":8,"compute":{"x":{"literal":1}}}}
    );
    defer request.deinit(a);
    try std.testing.expectEqual(@as(u32, 8), request.req.graph_queries[0].query.return_limit);
    try std.testing.expectEqual(@as(?u32, 1), request.req.graph_queries[0].query.evaluation_output_limit);
    try std.testing.expect(request.req.evaluation_graph);
    try std.testing.expect(!request.req.hasHitEvaluation());
    try std.testing.expectEqual(@as(u32, 1), request.req.limit);
}

test "decision functions graph dependency views keep the original prefix" {
    const std = @import("std");
    const types = @import("antfly_local_sources").storage_db_types;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var bindings = [_]types.GraphPatternBinding{
        .{ .alias = @constCast("customer"), .node = .{ .key = @constCast("first"), .depth = 0, .distance = 0 } },
        .{ .alias = @constCast("customer"), .node = .{ .key = @constCast("second"), .depth = 0, .distance = 0 } },
    };
    var matches = [_]types.GraphPatternMatch{
        .{ .bindings = bindings[0..1], .path = &.{} },
        .{ .bindings = bindings[1..2], .path = &.{} },
    };
    var hits = [_]types.SearchHit{ .{ .id = @constCast("first") }, .{ .id = @constCast("second") } };
    const result: types.GraphSearchResult = .{ .name = @constCast("customers"), .matches = &matches, .hits = &hits, .total_hits = 2 };
    const view = try result.dependencyView(arena.allocator(), 1, "docs");
    try std.testing.expectEqual(@as(usize, 1), view.matches.len);
    try std.testing.expectEqual(@as(usize, 1), view.hits.len);
    try std.testing.expectEqualStrings("first", view.hits[0].id);
    try std.testing.expect(view.truncated);
    try std.testing.expectEqual(@as(usize, 2), result.matches.len);
}

test "decision functions vector candidate evaluation supports final offset paging" {
    const std = @import("std");
    const query = @import("antfly_local_sources").api_query_contract;
    const a = std.testing.allocator;
    var request = try query.parseQueryRequest(a, null, "docs",
        \\{"limit":1,"offset":1,"embeddings":{"vector":[0.1,0.2]},"evaluate":{"scope":"candidates","candidate_count":20,"compute":{"x":{"literal":1}}}}
    );
    defer request.deinit(a);
    try std.testing.expectEqual(@as(u32, 1), request.req.offset);
    try std.testing.expectEqual(@as(u32, 20), request.req.dense_queries[0].query.k);
    try std.testing.expectError(error.UnsupportedQueryRequest, query.parseQueryRequest(a, null, "docs",
        \\{"limit":1,"offset":1,"embeddings":{"vector":[0.1,0.2]}}
    ));
}

test "decision functions graph evaluation preserves vector hit offset restrictions" {
    const std = @import("std");
    const query = @import("antfly_local_sources").api_query_contract;
    try std.testing.expectError(error.UnsupportedQueryRequest, query.parseQueryRequest(std.testing.allocator, null, "docs",
        \\{"limit":1,"offset":1,"embeddings":{"vector":[0.1,0.2]},"graph_queries":{"customers":{"index":"graph","match":{"anchor":"customer","nodes":{"customer":{}},"edges":[]},"return":{"bindings":["customer"],"include_documents":true,"limit":1}}},"evaluate":{"graph_query":"customers","scope":"candidates","candidate_count":20,"compute":{"x":{"literal":1}}}}
    ));
}

test "api query contract conditional source projection preserves authorization-stage ownership" {
    try @import("antfly_local_sources").api_query_contract.testing.expectConditionalSourceProjection();
}

test "api query contract conditional source spans preserve strict public wire semantics" {
    try @import("antfly_local_sources").api_query_contract.testing.expectConditionalSourceWireSpans();
}

test "decision provider shared Cloudflare extraction preserves exact numbers and measured usage" {
    try @import("antfly_extracting").testCloudflareExtraction();
}

test "api query contract native scalar indexed array source receipt" {
    try @import("antfly_local_sources").api_query_contract.testing.expectNativeScalarArraySource();
}

test "api query contract native retrieval borrows the admitted scalar program" {
    const std = @import("std");
    const local = @import("antfly_local_sources");
    const query = local.api_query_contract;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(path);
    var db = try local.storage_db_mod.DB.open(a, path, .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const padding = try a.alloc(u8, 32768);
    defer a.free(padding);
    @memset(padding, 'x');
    const json = try std.fmt.allocPrint(a, "{{\"arr\":[{{\"n\":1}},{{\"n\":2}}],\"payload\":\"{s}\"}}", .{padding});
    defer a.free(json);
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = json }}, .sync_level = .full_index });
    var owned = try query.parseQueryRequest(a, null, "docs", "{\"fields\":[\"arr.1.n\"],\"select\":{\"value\":{\"case\":[{\"when\":{\"literal\":true},\"then\":{\"field\":\"payload\"}}]},\"n\":{\"field\":\"arr.1.n\"}}}");
    defer owned.deinit(a);
    try std.testing.expect(owned.req.field_projection_plan.?.get(owned.req.field_projection_json).? == owned.projection_plan.?);
    var result = try db.search(a, owned.req);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("{\"arr\":[{\"n\":2}]}", result.hits[0].stored_data.?);
    var computed = try std.json.parseFromSlice(std.json.Value, a, result.hits[0].computed_json.?, .{});
    defer computed.deinit();
    try std.testing.expectEqualStrings(padding, computed.value.object.get("value").?.string);
    try std.testing.expectEqual(@as(i64, 2), computed.value.object.get("n").?.integer);
}

test "api query contract raw JSON budgets match VM for escaped strings and wide objects" {
    const std = @import("std");
    const local = @import("antfly_local_sources");
    const sql = local.sql_scalar;
    const ast = local.sql_ast;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const expression: ast.Scalar = .{ .column = "payload" };
    var program = try sql.bind(a, &expression, &.{.{ .name = "payload", .type = .json }}, &.{}, .{ .constant_bytes = 0, .constant_steps = 0 });
    defer program.deinit();
    const Mock = struct {
        raw: []const u8,
        polls: usize = 0,
        cancel: bool = false,
        fn read(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !?[]const u8 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).raw;
        }
        fn poll(ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.polls += 1;
            if (self.cancel and self.polls >= 4) return error.Canceled;
        }
        fn compare(p: *const sql.Program, alloc: std.mem.Allocator, raw: []const u8, value: std.json.Value, steps: usize) !void {
            const normal = try p.evaluate(alloc, &.{sql.Datum.json(value)}, &.{}, .{ .steps = steps, .output_bytes = 1024 * 1024 });
            var self: @This() = .{ .raw = raw };
            const limits: sql.EvalLimits = .{ .steps = steps, .output_bytes = 1024 * 1024, .output_column_loader = .{ .context = &self, .read = read }, .checkpoint = poll, .checkpoint_context = &self };
            const result = try p.evaluateOutput(alloc, &.{}, &.{}, limits);
            try std.testing.expect(result == .raw);
            try std.testing.expect(result.raw.value.bytes.ptr == raw.ptr);
            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, alloc, result.raw.value.bytes, .{});
            try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(alloc, normal.value, .{}), try std.json.Stringify.valueAlloc(alloc, parsed, .{}));
            try std.testing.expect(self.polls > 4);
            self.polls = 0;
            self.cancel = true;
            try std.testing.expectError(error.Canceled, p.evaluateOutput(alloc, &.{}, &.{}, limits));
        }
    };
    for ([_]bool{ false, true }) |unicode| {
        var raw: std.Io.Writer.Allocating = .init(a);
        var text: std.Io.Writer.Allocating = .init(a);
        try raw.writer.writeByte('"');
        for (0..@as(usize, if (unicode) 20000 else 100000)) |_| {
            try raw.writer.writeAll(if (unicode) "\\uD83D\\uDE80" else "\\n");
            try text.writer.writeAll(if (unicode) "🚀" else "\n");
        }
        try raw.writer.writeByte('"');
        try Mock.compare(&program, a, raw.written(), .{ .string = text.written() }, 4);
    }
    var raw: std.Io.Writer.Allocating = .init(a);
    try raw.writer.writeByte('{');
    for (0..20000) |i| {
        if (i != 0) try raw.writer.writeByte(',');
        try raw.writer.print("\"k{d}\":\"\"", .{i});
    }
    try raw.writer.writeByte('}');
    try Mock.compare(&program, a, raw.written(), try std.json.parseFromSliceLeaky(std.json.Value, a, raw.written(), .{}), 20002);
    var empty: Mock = .{ .raw = "" };
    if (program.evaluateOutput(a, &.{}, &.{}, .{ .output_column_loader = .{ .context = &empty, .read = Mock.read } })) |_| return error.TestExpectedError else |_| {}
}
