// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const local = @import("antfly_local_sources");
const Cover = @import("../storage/native_retained_cover.zig").Owner;
test "native public retained cover merges old ranges once on a new owner" {
    const a = std.testing.allocator;
    var repository_directory = try local.common_test_directory.TestDirectory.init("public-retained-repository");
    defer repository_directory.cleanup();
    var repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
    defer repository.deinit();
    const DB = local.storage_db_db.DB;
    const cut_mod = local.storage_db_native_query_cut;
    const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
    const Range = @typeInfo(@FieldType(Cut, "cover")).pointer.child;
    const id: [64]u8 = @splat('e');
    const expiry = cut_mod.nowMs() + 300_000;
    const origins: [2]Range = .{
        .{ .group_id = 10, .namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 }, .start_key = "", .end_key = "m" },
        .{ .group_id = 20, .namespace = .{ .table_id = 7, .shard_id = 20, .range_id = 21 }, .start_key = "m" },
    };
    for (origins, 0..) |origin, index| {
        var donor_directory = try local.common_test_directory.TestDirectory.init("public-retained-donor");
        defer donor_directory.cleanup();
        var donor = try DB.open(a, donor_directory.path(), .{ .identity_namespace = origin.namespace });
        defer donor.close();
        donor.backend_runtime.query_cut_repository = repository.capability();
        try donor.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
        try donor.batch(.{ .writes = &.{.{ .key = if (index == 0) "a" else "z", .value = "{\"body\":\"original\"}" }}, .sync_level = .full_index });
        try donor.captureQueryCut(.{ .id = &id, .table_id = 7, .expires_ms = expiry, .create = true, .cover = &origins, .recipe = .{ .schema_json = "", .read_schema_json = "", .indexes_json = "{\"body_text\":{\"name\":\"body_text\",\"type\":\"full_text\",\"field\":\"body\"}}" } }, .none);
    }
    var carrier_directory = try local.common_test_directory.TestDirectory.init("public-retained-carrier");
    defer carrier_directory.cleanup();
    var carrier = try DB.open(a, carrier_directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 30, .range_id = 31 } });
    defer carrier.close();
    carrier.backend_runtime.query_cut_repository = repository.capability();
    try carrier.batch(.{ .writes = &.{.{ .key = "new", .value = "{}" }}, .sync_level = .full_index });
    const cut: Cut = .{ .id = &id, .table_id = 7, .expires_ms = expiry, .cover = &origins, .recipe = .{ .schema_json = "", .read_schema_json = "", .indexes_json = "{\"body_text\":{\"name\":\"body_text\",\"type\":\"full_text\",\"field\":\"body\"}}" } };
    var owner = try Cover.init(a, &carrier, "docs", cut, .none);
    defer owner.deinit();
    var owned = try local.api_query.parseQueryRequest(a, null, "docs", "{\"indexes\":[\"body_text\"],\"full_text_search\":{\"match\":\"original\",\"field\":\"body\"},\"limit\":1,\"order_by\":[{\"field\":\"_id\"}]}");
    defer owned.deinit(a);
    owned.req.remote_snapshot = try a.dupe(u8, "native2:test");
    var response = try owner.execute(a, owned.req);
    defer response.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, response.json, .{});
    defer parsed.deinit();
    const first = parsed.value.object.get("responses").?.array.items[0];
    const hits = first.object.get("hits").?.object.get("hits").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqualStrings("a", hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("native2:test", first.object.get("remote_snapshot").?.string);
    const sort = try std.json.Stringify.valueAlloc(a, hits[0].object.get("_sort").?, .{});
    defer a.free(sort);
    const next_json = try std.fmt.allocPrint(a, "{{\"indexes\":[\"body_text\"],\"full_text_search\":{{\"match\":\"original\",\"field\":\"body\"}},\"limit\":1,\"order_by\":[{{\"field\":\"_id\"}}],\"search_after\":{s}}}", .{sort});
    defer a.free(next_json);
    var next = try local.api_query.parseQueryRequest(a, null, "docs", next_json);
    defer next.deinit(a);
    next.req.profile = true;
    var second_response = try owner.execute(a, next.req);
    defer second_response.deinit(a);
    var second = try std.json.parseFromSlice(std.json.Value, a, second_response.json, .{});
    defer second.deinit();
    const second_hits = second.value.object.get("responses").?.array.items[0].object.get("hits").?.object.get("hits").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), second_hits.len);
    try std.testing.expectEqualStrings("z", second_hits[0].object.get("_id").?.string);
}
