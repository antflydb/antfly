// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");
pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("functions/decisions.zig");
    _ = @import("functions/expressions.zig");
    _ = @import("functions/runtime.zig");
    _ = @import("functions/query_eval.zig");
    _ = @import("asset_producer_runtime.zig");
}

test "decision functions public DSL preserves evaluation inputs and rejects conflicting stages" {
    const std = @import("std");
    const query = @import("api/query_contract.zig");
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
    const query = @import("api/query_contract.zig");
    const wire = @import("api/local_query_contract.zig");
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
        const types = @import("storage/db/types.zig");
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
    const query = @import("api/query_contract.zig");
    const a = std.testing.allocator;
    var request = try query.parseQueryRequest(a, null, "docs",
        \\{"limit":1,"graph_queries":{"customers":{"index":"graph","match":{"anchor":"customer","nodes":{"customer":{}},"edges":[]},"return":{"bindings":["customer"],"include_documents":true,"limit":1}}},"evaluate":{"graph_query":"customers","scope":"matches","max_rows":8,"compute":{"x":{"literal":1}}}}
    );
    defer request.deinit(a);
    try std.testing.expectEqual(@as(u32, 8), request.req.graph_queries[0].query.return_limit);
    try std.testing.expectEqual(@as(?u32, 1), request.req.graph_queries[0].query.evaluation_output_limit);
}

test "decision functions graph dependency views keep the original prefix" {
    const std = @import("std");
    const types = @import("storage/db/types.zig");
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
