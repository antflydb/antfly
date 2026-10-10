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

const std = @import("std");
const local = @import("antfly_local_sources");

test "external lake composed native cut preserves primary and vector generations across writes and restart" {
    try verifyNativeCut(false);
    try verifyNativeCut(true);
}
test "external lake native cursor repository setup retains policy and location" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-config");
    defer directory.cleanup();
    var config = try local.common_config.Config.parseFromSlice(a, "{\"lake_indexes\":{\"query_cursors\":{\"retention_ms\":900000,\"max_native_cuts\":12,\"max_native_retained_bytes\":1048576}}}");
    defer config.deinit();
    const Repository = @import("native_query_repository.zig").Repository;
    const setup = try Repository.setupJsonAlloc(a, &config, .standalone, directory.path());
    defer a.free(setup);
    var repository = try Repository.initFromSetup(a, setup, null);
    defer repository.deinit();
    try std.testing.expectEqual(@as(usize, 12), repository.capability().limits.max_cuts);
    try std.testing.expectEqual(@as(u64, 1048576), repository.capability().limits.max_bytes);
    try std.testing.expectEqual(@as(u64, 900000), @import("lake_retained_cut.zig").configuredTtl(&repository.owned_config.?));
}

test "external lake native capture releases local capacity after publication admission fails" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-admission-rollback");
    defer directory.cleanup();
    var repository_directory = try local.common_test_directory.TestDirectory.init("native-cursor-admission-artifacts");
    defer repository_directory.cleanup();
    var repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
    defer repository.deinit();
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    var db = try local.storage_db_db.DB.open(a, directory.path(), .{ .identity_namespace = namespace });
    defer db.close();
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\"}" }}, .sync_level = .full_index });
    repository.limits = .{ .max_cuts = 1, .max_bytes = 1 };
    db.backend_runtime.query_cut_repository = repository.capability();
    const id: [64]u8 = @splat('d');
    const cut = local.storage_db_native_query_cut;
    const request: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000, .create = true };
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, db.captureQueryCut(request, .none));
    const root = try cut.pathAlloc(a, directory.path(), &id);
    defer a.free(root);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openDir(std.testing.io, root, .{}));
    repository.limits.max_bytes = 64 * 1024 * 1024;
    db.backend_runtime.query_cut_repository = repository.capability();
    try db.captureQueryCut(request, .none);
    try cut.validate(a, std.testing.io, root, request, namespace, .none);
}

test "external lake portable native cursor recovers document and vector generations on a fresh owner" {
    for ([_]bool{ false, true }) |vector_store| {
        const a = std.testing.allocator;
        var repository_directory = try local.common_test_directory.TestDirectory.init("native-cursor-repository");
        defer repository_directory.cleanup();
        var repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
        var repository_open = true;
        defer if (repository_open) repository.deinit();
        var donor_directory = try local.common_test_directory.TestDirectory.init("native-cursor-donor");
        defer donor_directory.cleanup();
        const donor_pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{donor_directory.path()});
        defer a.free(donor_pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, donor_pins) catch {};
        const DB = local.storage_db_db.DB;
        const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
        var donor = try DB.open(a, donor_directory.path(), .{ .identity_namespace = namespace });
        var donor_open = true;
        defer if (donor_open) donor.close();
        donor.backend_runtime.query_cut_repository = repository.capability();
        if (vector_store) try donor.configureTableStorage(.{ .dense_embeddings = .vector_store });
        try donor.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2,\"metric\":\"l2_squared\"}" });
        try donor.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\",\"v\":[1,0]}" }}, .sync_level = .full_index });
        const id: [64]u8 = @splat('c');
        var request: local.storage_db_native_query_cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = local.storage_db_native_query_cut.nowMs() + 60_000, .create = true };
        try donor.captureQueryCut(request, .none);
        donor.close();
        donor_open = false;
        try std.Io.Dir.cwd().deleteTree(std.testing.io, donor_pins);
        var receiver_directory = try local.common_test_directory.TestDirectory.init("native-cursor-receiver");
        defer receiver_directory.cleanup();
        const receiver_pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{receiver_directory.path()});
        defer a.free(receiver_pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, receiver_pins) catch {};
        var receiver = try DB.open(a, receiver_directory.path(), .{ .identity_namespace = namespace });
        defer receiver.close();
        receiver.backend_runtime.query_cut_repository = repository.capability();
        try receiver.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"live replacement\"}" }}, .sync_level = .full_index });
        request.create = false;
        {
            var retained = try receiver.openQueryCut(request, .none);
            defer retained.close();
            const value = (try retained.get(a, "doc")).?;
            defer a.free(value);
            try std.testing.expect(std.mem.indexOf(u8, value, "original") != null);
            var result = try retained.search(a, .{ .index_name = "semantic", .dense = .{ .vector = &.{ 1, 0 }, .k = 1 }, .limit = 1 });
            defer result.deinit();
            try std.testing.expectEqualStrings("doc", result.hits[0].id);
            try std.testing.expectEqual(@as(?f32, 0), result.hits[0].distance);
        }
        try std.Io.Dir.cwd().deleteTree(std.testing.io, receiver_pins);
        repository.deinit();
        repository_open = false;
        // Reopen an empty durable destination: a cursor never recaptures live
        // data when the authoritative manifest has been lost.
        try std.Io.Dir.cwd().deleteTree(std.testing.io, repository_directory.path());
        repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
        repository_open = true;
        receiver.backend_runtime.query_cut_repository = repository.capability();
        request.timeout_ms = 1000;
        try std.testing.expectError(error.CatalogGenerationChanged, receiver.openQueryCut(request, .none));
        // Recovery errors must release the parent lock for the next request.
        try std.testing.expectError(error.CatalogGenerationChanged, receiver.openQueryCut(request, .none));
    }
}
fn verifyNativeCut(vector_store: bool) !void {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-writer");
    defer directory.cleanup();
    const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{directory.path()});
    defer a.free(pins);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
    const DB = local.storage_db_db.DB;
    var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    var opened = true;
    defer if (opened) db.close();
    if (vector_store) try db.configureTableStorage(.{ .dense_embeddings = .vector_store });
    try db.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2,\"metric\":\"l2_squared\"}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"before\",\"v\":[1,0]}" }}, .sync_level = .full_index });
    const id: [64]u8 = @splat('a');
    const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
    var cut: Cut = .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .create = true };
    var timed_out = cut;
    timed_out.timeout_ms = 0;
    try std.testing.expectError(error.DeadlineExceeded, db.captureQueryCut(timed_out, .none));
    try db.captureQueryCut(cut, .none);
    cut.create = false;
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"after\",\"v\":[0,1]}" }}, .sync_level = .full_index });
    {
        var snapshot = try db.openQueryCut(cut, .none);
        defer snapshot.close();
        if (vector_store) try std.testing.expectEqual(@as(u64, 0), snapshot.local_execution.source_vectors.load(.acquire).?.stats.inventory_updates);
        const stored = (try snapshot.get(a, "doc")).?;
        defer a.free(stored);
        try std.testing.expect(std.mem.indexOf(u8, stored, "before") != null);
        var result = try snapshot.search(a, .{ .index_name = "semantic", .dense = .{ .vector = &.{ 1, 0 }, .k = 1 }, .limit = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("doc", result.hits[0].id);
        try std.testing.expectEqual(@as(?f32, 0), result.hits[0].distance);
    }
    db.close();
    opened = false;
    var restarted = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    defer restarted.close();
    var snapshot = try restarted.openQueryCut(cut, .none);
    defer snapshot.close();
    const stored = (try snapshot.get(a, "doc")).?;
    defer a.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "before") != null);
    var missing = cut;
    const other: [64]u8 = @splat('b');
    missing.id = &other;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(missing, .none));
    var wrong = cut;
    wrong.table_id = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(wrong, .none));
    var expired = cut;
    expired.expires_ms = 0;
    try std.testing.expectError(error.CatalogGenerationChanged, restarted.openQueryCut(expired, .none));
}

test "external lake native cursor capability fences recipe incarnation scope and expiration" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-capability");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const retained = @import("native_retained_cut.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "current", .schema_json = "{}", .indexes_json = "{}" };
    const Range = @typeInfo(@FieldType(@typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child, "cover")).pointer.child;
    const cover: [1]Range = .{.{ .group_id = 10, .namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 }, .start_key = "" }};
    const token = try retained.saveWithCover(a, &store, @splat(1), std.testing.io, table, 100, @import("lake_retained_cut.zig").ttl_ms, &cover, .none);
    defer a.free(token);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const descriptor = try retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none);
    try std.testing.expectEqual(@as(usize, 64), descriptor.id.len);
    try std.testing.expectEqual(@as(u16, 3), descriptor.version);
    try std.testing.expectEqual(@as(usize, 64), descriptor.cover[0].generation_id.?.len);
    try std.testing.expectEqual(@as(usize, 1), descriptor.cover.len);
    const legacy = try retained.save(a, &store, @splat(1), std.testing.io, table, 100, .none);
    defer a.free(legacy);
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), legacy, table, 101, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(2), token, table, 101, .none));
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, @import("lake_retained_cut.zig").ttl_ms + 100, .none));
    table.table_id = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none));
    table.table_id = 7;
    table.indexes_json = "{\"changed\":{}}";
    try std.testing.expectError(error.CatalogGenerationChanged, retained.load(arena.allocator(), &store, @splat(1), token, table, 101, .none));
}

test {
    _ = @import("native_repartition_test.zig");
}

test "external lake internal query parsing retains owned native cuts across forwarding" {
    try verifyForwardedNativeCut(std.testing.allocator);
}
fn verifyForwardedNativeCut(a: std.mem.Allocator) !void {
    const api = local.api_query;
    const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
    const id: [64]u8 = @splat('a');
    const cut: Cut = .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + std.time.ms_per_hour, .create = true, .origin = .{ .table_id = 7, .shard_id = 11, .range_id = 11 } };
    const body = try std.json.Stringify.valueAlloc(a, .{ ._native_cut = cut }, .{});
    defer a.free(body);
    var request = try api.parseQueryRequest(a, null, "docs", body);
    defer request.deinit(a);
    try std.testing.expect(request.native_cut != null);
    try std.testing.expectEqualStrings(&id, request.req.native_query_cut.?.id);
    const encoded = try local.api_local_query_contract.encodeQueryRequest(a, request.req);
    defer a.free(encoded);
    var forwarded = try api.parseQueryRequest(a, null, "docs", encoded);
    defer forwarded.deinit(a);
    try std.testing.expectEqual(cut.origin.?.shard_id, forwarded.req.native_query_cut.?.origin.?.shard_id);
    try std.testing.expect(forwarded.req.native_query_cut.?.create);
    if (api.parsePublicQueryRequest(a, null, "docs", body)) |value| {
        var unexpected = value;
        unexpected.deinit(a);
        return error.TestUnexpectedResult;
    } else |err| if (err != error.InvalidQueryRequest) return err;
}

test "external lake single-range remote text continuation executes its already bound checkpoint" {
    const a = std.testing.allocator;
    var repository_directory = try local.common_test_directory.TestDirectory.init("single-range-cursor-repository");
    defer repository_directory.cleanup();
    var repository = try @import("native_query_repository.zig").Repository.init(a, null, null, .standalone, repository_directory.path());
    defer repository.deinit();
    const DB = local.storage_db_db.DB;
    const cut = local.storage_db_native_query_cut;
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 };
    const id: [64]u8 = @splat('f');
    const retained: cut.Request = .{ .id = &id, .table_id = 7, .expires_ms = cut.nowMs() + 300_000 };
    {
        var donor_directory = try local.common_test_directory.TestDirectory.init("single-range-cursor-donor");
        defer donor_directory.cleanup();
        const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{donor_directory.path()});
        defer a.free(pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
        var donor = try DB.open(a, donor_directory.path(), .{ .identity_namespace = namespace });
        defer donor.close();
        donor.backend_runtime.query_cut_repository = repository.capability();
        try donor.setSchemaJson(a, "{\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"string\"}}}}}}");
        try donor.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{}" });
        try donor.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"body\":\"original\"}" }, .{ .key = "z", .value = "{\"body\":\"original\"}" } }, .sync_level = .full_index });
        var create = retained;
        create.create = true;
        try donor.captureQueryCut(create, .none);
    }
    var carrier_directory = try local.common_test_directory.TestDirectory.init("single-range-cursor-carrier");
    defer carrier_directory.cleanup();
    const carrier_pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{carrier_directory.path()});
    defer a.free(carrier_pins);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, carrier_pins) catch {};
    var carrier = try DB.open(a, carrier_directory.path(), .{ .identity_namespace = namespace });
    defer carrier.close();
    carrier.backend_runtime.query_cut_repository = repository.capability();
    try carrier.batch(.{ .writes = &.{.{ .key = "fresh", .value = "{\"body\":\"new generation\"}" }}, .sync_level = .full_index });
    const token = "native2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const body = try std.json.Stringify.valueAlloc(a, .{
        ._native_cut = retained,
        .remote_snapshot = token,
        .full_text_index = "body_text",
        .full_text_search = .{ .match = "original", .field = "body" },
        .order_by = .{.{ .field = "_id" }},
        .search_after = .{"a"},
        .limit = 128,
    }, .{});
    defer a.free(body);
    const abi = @import("kernel_owner_abi");
    for ([_]bool{ false, true }) |profile| {
        var request_body = try std.json.parseFromSlice(std.json.Value, a, body, .{});
        defer request_body.deinit();
        try request_body.value.object.put(request_body.arena.allocator(), "profile", .{ .bool = profile });
        const bytes = try std.json.Stringify.valueAlloc(a, request_body.value, .{});
        defer a.free(bytes);
        const request: abi.LocalQueryRequest = .{ .db = &carrier, .table_name = .fromSlice("docs"), .request_json = .fromSlice(bytes), .execution_options = .{ .enabled = 1, .raw_search_result = 1 } };
        var response: abi.QueryOwnedResponse = .{};
        var failure: abi.FailureIdentity = .{};
        try std.testing.expectEqual(abi.Status.ok, @import("../storage/local_query_provider.zig").execute(&request, &response, &failure));
        defer std.heap.c_allocator.free(response.buffer.ptr.?[0..@intCast(response.buffer.len)]);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, response.buffer.ptr.?[0..@intCast(response.buffer.len)], .{});
        defer parsed.deinit();
        const result = parsed.value.object.get("responses").?.array.items[0];
        try std.testing.expectEqualStrings(token, result.object.get("remote_snapshot").?.string);
        const hits = result.object.get("hits").?.object.get("hits").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), hits.len);
        try std.testing.expectEqualStrings("z", hits[0].object.get("_id").?.string);
    }
}
