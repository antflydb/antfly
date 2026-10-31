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
    const original_vtable = store.vtable;
    var unavailable_collection = original_vtable.*;
    unavailable_collection.visit_scoped_uploads = struct {
        fn denied(_: *anyopaque, _: [32]u8, _: @import("../serverless/artifacts/store.zig").ScopedUploadVisitor, _: @import("antfly_cancellation").CancellationToken) !void {
            return error.TestCollectionUnavailable;
        }
    }.denied;
    store.vtable = &unavailable_collection;
    const retained = @import("native_retained_cut.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "current", .schema_json = "{}", .indexes_json = "{}" };
    const Range = @typeInfo(@FieldType(@typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child, "cover")).pointer.child;
    const cover: [1]Range = .{.{ .group_id = 10, .namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 }, .start_key = "" }};
    const token = try retained.saveWithCover(a, &store, @splat(1), std.testing.io, table, 100, @import("lake_retained_cut.zig").ttl_ms, &cover, .none);
    defer a.free(token);
    // Publication and replay need no listing/delete capability. Explicit
    // maintenance still surfaces a collector failure rather than hiding it.
    try std.testing.expectError(error.TestCollectionUnavailable, retained.collect(&store, 7, @splat(1), 101, .none));
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

test "external lake background cursor collection discovers a failed physical capture" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cursor-orphan-discovery");
    defer directory.cleanup();
    const repository_api = @import("native_query_repository.zig");
    var repository = try repository_api.Repository.init(a, null, null, .standalone, directory.path());
    defer repository.deinit();
    const Range = @typeInfo(@FieldType(local.storage_db_native_query_cut.Request, "cover")).pointer.child;
    const cover = [_]Range{.{ .group_id = 1, .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .start_key = "" }};
    const table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "current", .schema_json = "{}", .indexes_json = "{}" };
    const token = try repository_api.saveWithCover(a, &repository.store, std.testing.io, table, 100, 60_000, &cover, .none);
    defer a.free(token);
    const split = std.mem.lastIndexOfScalar(u8, token, ':').?;
    const artifact = token[@import("native_retained_cut.zig").prefix.len..split];
    var store = repository.store.artifactStore();
    // No physical generation is ever uploaded. The discovery witness alone
    // must let the supervised collector retain and later retire this artifact.
    const live_next = try repository.collectNext(a, null, 101, 128, .none);
    defer if (live_next) |value| a.free(value);
    const bytes = try store.getAlloc(artifact);
    store.allocator.free(bytes);
    const retired_next = try repository.collectNext(a, null, 90_102, 128, .none);
    defer if (retired_next) |value| a.free(value);
    try std.testing.expectError(error.FileNotFound, store.getAlloc(artifact));
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

test "external lake Cypher execution reads immutable native document and graph cuts" {
    inline for (.{ true, false }) |durable| {
        const a = std.testing.allocator;
        const cypher = @import("cypher");
        const DB = local.storage_db_db.DB;
        var directory = try local.common_test_directory.TestDirectory.init("cypher-native-cut");
        defer directory.cleanup();
        const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{directory.path()});
        defer a.free(pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
        var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
        defer db.close();
        try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}", .coverage_generation = 41 });
        try db.batch(.{
            .writes = &.{ .{ .key = "a", .value = "{\"id\":\"a\",\"title\":\"Alpha\"}" }, .{ .key = "b", .value = "{\"id\":\"b\",\"title\":\"Beta\"}" } },
            .graph_writes = &.{ .{ .index_name = "links", .source = "a", .target = "b", .edge_type = "cites", .edge_id = "first", .weight = 1 }, .{ .index_name = "links", .source = "a", .target = "b", .edge_type = "cites", .edge_id = "second", .weight = 2 } },
            .sync_level = .full_index,
        });
        const id: [64]u8 = @splat('c');
        const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
        const cut: Cut = .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .create = true };
        var snapshot = try db.openQueryCut(cut, .none);
        defer snapshot.close();
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"id\":\"b\",\"title\":\"Changed\"}" }}, .sync_level = .full_index });
        const Authority = struct {
            pub fn bind(_: *anyopaque, _: cypher.catalog.Graph) !void {}
            pub fn checkpoint(_: *anyopaque) !void {}
            pub fn projectRelationship(_: *anyopaque, _: std.mem.Allocator, relationship: *const cypher.value.Relationship) !?*const cypher.value.Relationship {
                return relationship;
            }
            pub fn project(_: *anyopaque, _: std.mem.Allocator, node: *const cypher.value.Node) !?*const cypher.value.Node {
                return node;
            }
        };
        var owner: u8 = 0;
        const authority: DB.CypherBackend.Authority = .{ .ptr = &owner, .bind = Authority.bind, .checkpoint = Authority.checkpoint, .project = Authority.project, .project_relationship = Authority.projectRelationship };
        const catalog_identity = (try @import("../system_catalog/graph_bindings.zig").indexIdentity(a, "{\"links\":{\"type\":\"graph\",\"_index_incarnation\":41}}", "links")).?;
        const native_identity = (try snapshot.cypherGraphIndexIdentity(a, "links")).?;
        try std.testing.expectEqual(catalog_identity.incarnation, native_identity.incarnation);
        try std.testing.expectEqual(catalog_identity.config_hash, native_identity.config_hash);
        const graph: cypher.catalog.Graph = .{ .name = "knowledge", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs", .table_id = 7 }}, .relationships = &.{.{ .name = "CITES", .index = "links", .source_label = "Document", .target_label = "Document", .stored_type = "cites", .identity = catalog_identity }} };
        var request = try cypher.request.Request.parse(a, "{\"query\":\"MATCH (a:Document {id:'a'})-[r:CITES]->(b) RETURN b.title, r.weight ORDER BY r.weight\"}", .{});
        defer request.deinit();
        var diagnostic: cypher.compiler.Diagnostic = .{};
        try std.testing.expectError(error.CypherSnapshotRequired, DB.queryCypher(a, &.{.{ .name = "docs", .db = &db }}, authority, 1, graph, request, &diagnostic));
        var result = try DB.queryCypher(a, &.{.{ .name = "docs", .db = &snapshot }}, authority, @import("antfly_platform").time.realtimeNs(), graph, request, &diagnostic);
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(json);
        try std.testing.expectEqualStrings("[[\"Beta\",1],[\"Beta\",2]]", json);

        // Exercise the production compiled-boundary read port while the caller
        // already holds both statement fences. A second capture lock would hang.
        try db.runMaintenanceUntil(db.core.nextDerivedSequence(), .{});
        const domain = local.system_catalog_domain;
        const record: domain.Resource = .{ .kind = .graph, .id = 1, .name = "knowledge", .source_json = "{\"labels\":[{\"name\":\"Document\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"CITES\",\"index\":\"links\",\"source_label\":\"Document\",\"target_label\":\"Document\",\"stored_type\":\"cites\"}]}", .graph_tables = &.{.{ .label_index = 0, .table_id = 7, .storage_name = "docs" }}, .graph_indexes = &.{.{ .mapping_index = 0, .identity = catalog_identity }} };
        const catalog_json = try std.json.Stringify.valueAlloc(a, record, .{});
        defer a.free(catalog_json);
        var statement_fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
        defer statement_fence.release();
        var capture: DB.CypherBackend = .{ .tables = &.{.{ .name = "docs", .db = &db, .capture_fence = &statement_fence }}, .authority = authority, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() };
        const materialized = try cypher.snapshot.Materialized.create(a, capture.backend(), graph, .{});
        // Mutable capture fences never export live roots as exclusion proofs.
        {
            var route_arena = std.heap.ArenaAllocator.init(a);
            defer route_arena.deinit();
            const backend = capture.backend();
            const routes = try backend.vtable.adjacency_routes.?(backend.ptr, route_arena.allocator(), "docs");
            try std.testing.expectEqual(@as(usize, 0), routes.len);
        }
        defer materialized.deinit();
        // An unrelated owner capability must never authorize mutable capture.
        var wrong_owner: DB.StatementReadFence = .{ .source = &snapshot, .primary = undefined, .replay = undefined };
        var invalid_capture: DB.CypherBackend = .{ .tables = &.{.{ .name = "docs", .db = &db, .capture_fence = &wrong_owner }}, .authority = authority, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() };
        try std.testing.expectError(error.CypherSnapshotRequired, cypher.snapshot.Materialized.create(a, invalid_capture.backend(), graph, .{}));
        var read = try DB.CypherStatementSnapshot.create(a, &db, &statement_fence, .{ .durable = durable, .indexes = if (durable) null else &.{"links"}, .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat('d'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none);
        var read_open = true;
        defer if (read_open) read.deinit();
        statement_fence.release();
        try std.testing.expectError(error.CypherSnapshotRequired, cypher.snapshot.Materialized.create(a, capture.backend(), graph, .{}));
        try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"id\":\"b\",\"title\":\"After graph capture\"}" }, .{ .key = "later", .value = "{\"id\":\"later\"}" } }, .sync_level = .full_index });
        {
            const native: *if (durable) DB.CypherStatementSnapshot.Durable else DB.CypherStatementSnapshot.Pinned = @ptrCast(@alignCast(read.ptr));
            var filtered = try native.db.scan(a, "b", "b", .{ .inclusive_from = true, .limit = 1, .include_documents = false, .filter_query_json = "{\"term\":{\"id\":\"b\"}}" });
            defer filtered.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), filtered.hashes.len);
            try std.testing.expectEqualStrings("b", filtered.hashes[0].id);
            try std.testing.expectEqual(@as(usize, 0), filtered.documents.len);
        }
        {
            var batch_arena = std.heap.ArenaAllocator.init(a);
            defer batch_arena.deinit();
            const reply = try read.invokeTyped(batch_arena.allocator(), .{ .operation = .get_many, .graph_incarnation = 1, .graph_revision = 1, .identities = &.{ .{ .table = "docs", .key = "b" }, .{ .table = "docs", .key = "missing" }, .{ .table = "docs", .key = "b" } }, .read_fields = &.{"title"} });
            try std.testing.expectEqual(@as(usize, 3), reply.values.len);
            try std.testing.expectEqualStrings("Changed", (cypher.value.Value{ .node = reply.values[0].value.node }).property("title").string);
            try std.testing.expect(reply.values[1].value == .null);
            try std.testing.expectEqualStrings("Changed", (cypher.value.Value{ .node = reply.values[2].value.node }).property("title").string);
        }
        const Channel = struct {
            fn invoke(ptr: *anyopaque, alloc: std.mem.Allocator, body: []const u8) ![]u8 {
                const port: *local.storage_statement_read_fence.GraphRead = @ptrCast(@alignCast(ptr));
                return port.invoke(alloc, body);
            }
        };
        var client: cypher.rpc.Client = .{ .channel = .{ .ptr = &read, .call = Channel.invoke }, .incarnation = 1, .revision = 1 };
        var wire_arena = std.heap.ArenaAllocator.init(a);
        defer wire_arena.deinit();
        // The real owner uses DB.CypherBackend, not the embedded pinned adapter.
        // Population metadata must survive that adapter and remain on this cut
        // after a new live document is committed.
        try std.testing.expectEqual(@as(?u64, 2), try client.backend().vtable.population_upper_bound.?(&client, wire_arena.allocator(), "docs"));
        try std.testing.expectEqual(@as(?usize, 2), try client.backend().vtable.estimate_where.?(&client, wire_arena.allocator(), "docs", &.{}, 15000));
        try std.testing.expectError(error.Forbidden, client.backend().vtable.population_upper_bound.?(&client, wire_arena.allocator(), "other"));
        const node = (try client.backend().vtable.get(&client, wire_arena.allocator(), .{ .table = "docs", .key = "b" })).?;
        try std.testing.expectEqualStrings("Changed", node.properties[1].value.string);
        try std.testing.expectError(error.Forbidden, client.backend().vtable.get(&client, wire_arena.allocator(), .{ .table = "other", .key = "b" }));
        // SQL and graph reads must consume the same document generation. The SQL
        // child independently retains the cut after its graph parent is released.
        // Exercise the actual native typed callback, with no wire fallback available.
        try std.testing.expect(read.vtable.invoke_typed != null);
        const TypedOnly = struct {
            fn forbiddenWire(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
                return error.TestUnexpectedWireCall;
            }
            fn invoke(ptr: *anyopaque, alloc: std.mem.Allocator, input: cypher.rpc.Request) !cypher.rpc.Reply {
                const native: *local.storage_statement_read_fence.GraphRead = @ptrCast(@alignCast(ptr));
                return native.invokeTyped(alloc, input);
            }
        };
        var typed_client: cypher.rpc.Client = .{ .channel = .{ .ptr = &read, .call = TypedOnly.forbiddenWire, .typed = TypedOnly.invoke }, .incarnation = 1, .revision = 1 };
        const typed_node = (try typed_client.backend().vtable.get(&typed_client, wire_arena.allocator(), .{ .table = "docs", .key = "b" })).?;
        try std.testing.expectEqualStrings("Changed", typed_node.properties[1].value.string);
        try std.testing.expectEqual(@as(?u64, 2), try typed_client.backend().vtable.population_upper_bound.?(&typed_client, wire_arena.allocator(), "docs"));
        const many = try typed_client.backend().vtable.get_many.?(&typed_client, wire_arena.allocator(), &.{ .{ .table = "docs", .key = "a" }, .{ .table = "docs", .key = "b" } });
        try std.testing.expectEqual(@as(usize, 2), many.len);
        try std.testing.expectEqualStrings("Changed", many[1].?.properties[1].value.string);
        const routing = try read.invokeTyped(wire_arena.allocator(), .{ .operation = .adjacency_routes, .graph_incarnation = 1, .graph_revision = 1, .table = "docs" });
        {
            try std.testing.expectEqual(@as(usize, 1), routing.adjacency_routes.len);
            const route = routing.adjacency_routes[0];
            try std.testing.expectEqualStrings("links", route.index);
            try std.testing.expect(route.outgoing.?.contains("a"));
            try std.testing.expect(!route.outgoing.?.contains("outside"));
            try std.testing.expect(route.incoming.?.contains("b"));
        }
        try std.testing.expectError(error.Forbidden, read.invokeTyped(wire_arena.allocator(), .{ .operation = .adjacency_routes, .graph_incarnation = 1, .graph_revision = 1, .table = "other" }));
        const edges = try typed_client.backend().vtable.expand(&typed_client, wire_arena.allocator(), graph.relationships[0], .{ .table = "docs", .key = "a" }, .out, null, 128);
        try std.testing.expectEqual(@as(usize, 2), edges.items.len);
        const bounded = try read.invokeTyped(wire_arena.allocator(), .{ .operation = .get_many, .graph_incarnation = 1, .graph_revision = 1, .identities = &.{.{ .table = "docs", .key = "b" }}, .max_work = 0 });
        try std.testing.expectEqual(cypher.rpc.BudgetFailure.work, bounded.read_failure.?);
        try std.testing.expectEqual(@as(usize, 0), bounded.values.len);
        {
            const staged_document = try std.json.parseFromSliceLeaky(std.json.Value, wire_arena.allocator(), "{\"id\":\"b\",\"title\":\"Projected\"}", .{});
            const normalized = try read.invokeTyped(wire_arena.allocator(), .{ .operation = .normalize, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .identity = .{ .table = "docs", .key = "b" }, .document = staged_document });
            const projected = try read.invokeTyped(wire_arena.allocator(), .{ .operation = .project, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .identity = .{ .table = "docs", .key = "b" }, .document = staged_document, .include_relationships = true });
            try std.testing.expectEqual(@as(usize, 1), normalized.values.len);
            try std.testing.expectEqual(@as(usize, 1), projected.values.len);
            try std.testing.expectEqualStrings("Projected", projected.values[0].value.node.properties[1].value.string);
        }
        try std.testing.expectError(error.Forbidden, typed_client.backend().vtable.get(&typed_client, wire_arena.allocator(), .{ .table = "other", .key = "b" }));
        var sql_view = try read.openSql(a, "docs", "b", "b", .{ .inclusive_from = true, .sql_document_preimage = true, .relational_query_json = "{\"fields\":[\"_id\"]}" });
        defer sql_view.deinit();
        read.deinit();
        read_open = false;
        var sql_page = try sql_view.next(a, 1);
        defer sql_page.deinit();
        try std.testing.expectEqual(@as(usize, 1), sql_page.rows.len);
        const document = sql_page.rows[0].document orelse return error.TestExpectedDocument;
        try std.testing.expectEqualStrings("Changed", document.object.get("title").?.string);
    }
}

test "external lake Cypher execution reads immutable native document and graph cuts with selective projections" {
    inline for (.{ true, false }) |durable| {
        const a = std.testing.allocator;
        const DB = local.storage_db_db.DB;
        var directory = try local.common_test_directory.TestDirectory.init("cypher-selective-cut");
        defer directory.cleanup();
        const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{directory.path()});
        defer a.free(pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
        var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .start_index_workers = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false } });
        defer db.close();
        try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}", .coverage_generation = 41 });
        try db.addIndex(.{ .name = "unused", .kind = .graph, .config_json = "{}", .coverage_generation = 42 });
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"title\":\"Alpha\"}" }}, .sync_level = .full_index });
        try db.runMaintenanceUntil(db.core.nextDerivedSequence(), .{});
        const identity = (try db.cypherGraphIndexIdentity(a, "links")).?;
        const domain = local.system_catalog_domain;
        const record: domain.Resource = .{ .kind = .graph, .id = 1, .name = "g", .source_json = "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"R\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"r\"}]}", .graph_tables = &.{.{ .label_index = 0, .table_id = 7, .storage_name = "docs" }}, .graph_indexes = &.{.{ .mapping_index = 0, .identity = identity }} };
        const catalog_json = try std.json.Stringify.valueAlloc(a, record, .{});
        defer a.free(catalog_json);
        // An unrelated unfinished projection cannot delay a primary/adjacency read.
        var checkpoint = try db.core.loadProjectionCheckpoint(a, "unused");
        checkpoint.applied_sequence = 0;
        try db.core.saveProjectionCheckpoint("unused", checkpoint);
        // Revoking a completion prefix is itself a primary commit. Advance the
        // healthy projection to that commit without repairing the unrelated one.
        var healthy = try db.core.loadProjectionCheckpoint(a, "links");
        healthy.applied_sequence = db.core.nextDerivedSequence();
        try db.core.saveProjectionCheckpoint("links", healthy);
        var fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
        defer fence.release();
        for ([_][]const []const u8{ &.{}, &.{"links"} }, 0..) |names, i| {
            var read = try DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .durable = durable, .indexes = names, .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat(@as(u8, 'e') + @as(u8, @intCast(i))), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none);
            defer read.deinit();
            if (durable) {
                const native: *DB.CypherStatementSnapshot.Durable = @ptrCast(@alignCast(read.ptr));
                try std.testing.expectEqual(names.len, native.db.core.index_manager.graph_indexes.items.len);
                try std.testing.expect(native.db.core.graphIndex("unused") == null);
                try std.testing.expect((try native.db.cypherGraphIndexIdentity(a, "links")) != null);
            } else {
                const native: *DB.CypherStatementSnapshot.Pinned = @ptrCast(@alignCast(read.ptr));
                try std.testing.expectEqual(names.len, native.db.cut.graphs.len);
                try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, pins, .{}));
            }
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const reply = try read.invokeTyped(arena.allocator(), .{ .operation = .get, .graph_incarnation = 1, .graph_revision = 1, .identity = .{ .table = "docs", .key = "a" } });
            try std.testing.expectEqual(@as(usize, 1), reply.values.len);
            const typed_json = try std.json.Stringify.valueAlloc(arena.allocator(), reply, .{});
            const wire_json = try read.invoke(arena.allocator(), "{\"operation\":\"get\",\"graph_incarnation\":1,\"graph_revision\":1,\"identity\":{\"table\":\"docs\",\"key\":\"a\"}}");
            try std.testing.expectEqualStrings(wire_json, typed_json);
            if (durable) {
                const native: *DB.CypherStatementSnapshot.Durable = @ptrCast(@alignCast(read.ptr));
                const Cut = @typeInfo(@FieldType(local.storage_db_types.SearchRequest, "native_query_cut")).optional.child;
                try std.testing.expectError(error.CatalogGenerationChanged, db.openQueryCut(@as(Cut, .{ .id = &native.input.cut_id, .table_id = 7, .expires_ms = native.input.expires_ms }), .none));
            }

            try std.testing.expectError(error.CatalogGenerationChanged, read.invokeTyped(arena.allocator(), .{ .operation = .get, .graph_incarnation = 2, .graph_revision = 1, .identity = .{ .table = "docs", .key = "a" } }));
        }
        try std.testing.expectError(error.CypherSnapshotNotReady, DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .indexes = &.{"unused"}, .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat('a'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none));
        try std.testing.expectError(error.CypherSnapshotNotReady, DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat('a'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none));
    }
}

test "external lake Cypher catalog cache invalidates on live index drop and recreation" {
    inline for (.{ true, false }) |durable| {
        const a = std.testing.allocator;
        const DB = local.storage_db_db.DB;
        var directory = try local.common_test_directory.TestDirectory.init("cypher-catalog-epochs");
        defer directory.cleanup();
        const pins = try std.fmt.allocPrint(a, "{s}.query-pins", .{directory.path()});
        defer a.free(pins);
        defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
        var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .start_index_workers = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false } });
        defer db.close();
        try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}", .coverage_generation = 41 });
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"title\":\"Alpha\"}" }}, .sync_level = .full_index });
        try db.runMaintenanceUntil(db.core.nextDerivedSequence(), .{});
        // This no-edge fixture explicitly publishes the projection frontier;
        // no asynchronous worker runs to cover metadata-only replay commits.
        var ready = try db.core.loadProjectionCheckpoint(a, "links");
        ready.applied_sequence = db.core.nextDerivedSequence();
        try db.core.saveProjectionCheckpoint("links", ready);
        const identity = (try db.cypherGraphIndexIdentity(a, "links")).?;
        const domain = local.system_catalog_domain;
        const record: domain.Resource = .{ .kind = .graph, .id = 1, .name = "g", .source_json = "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"R\",\"index\":\"links\",\"source_label\":\"D\",\"target_label\":\"D\",\"stored_type\":\"r\"}]}", .graph_tables = &.{.{ .label_index = 0, .table_id = 7, .storage_name = "docs" }}, .graph_indexes = &.{.{ .mapping_index = 0, .identity = identity }} };
        const catalog_json = try std.json.Stringify.valueAlloc(a, record, .{});
        defer a.free(catalog_json);
        var fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
        defer fence.release();
        var read = try DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .durable = durable, .indexes = if (durable) &.{} else &.{"links"}, .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat('c'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none);
        var read_open = true;
        defer if (read_open) read.deinit();
        fence.release();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const request: @import("cypher").rpc.Request = .{ .operation = .get, .graph_incarnation = 1, .graph_revision = 1, .identity = .{ .table = "docs", .key = "a" } };
        const native: *if (durable) DB.CypherStatementSnapshot.Durable else DB.CypherStatementSnapshot.Pinned = @ptrCast(@alignCast(read.ptr));
        const before = native.catalog_guards[0].validated.?.generation;
        try std.testing.expectEqual(@as(usize, 1), (try read.invokeTyped(arena.allocator(), request)).values.len);
        // A catalog commit unrelated to this graph refreshes the proof once.
        try db.addIndex(.{ .name = "unused", .kind = .graph, .config_json = "{}", .coverage_generation = 43 });
        try std.testing.expectEqual(@as(usize, 1), (try read.invokeTyped(arena.allocator(), request)).values.len);
        try std.testing.expect(native.catalog_guards[0].validated.?.generation > before);
        try std.testing.expectEqual(db.core.index_manager.writePlanGeneration(), native.catalog_guards[0].validated.?.generation);
        try std.testing.expect(try db.deleteIndex("links"));
        try std.testing.expectError(error.CatalogGenerationChanged, read.invokeTyped(arena.allocator(), request));
        // Same-name admission waits for production generation-retirement cleanup.
        if (!durable) {
            // A retired backend may still flush during close. Its namespace
            // cannot be deleted/reused until the final captured reader retires.
            try std.testing.expectError(error.IndexGenerationStillActive, db.core.index_manager.finalizeRetiredIndexStorage(db.core.store, "links"));
            read.deinit();
            read_open = false;
        }
        db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
        var cleaned = false;
        for (0..1000) |_| switch (try db.advanceGeneratedArtifactCleanupPage("links")) {
            .idle => {
                cleaned = true;
                break;
            },
            .progressed => {},
            .busy => std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {},
        };
        try std.testing.expect(cleaned);
        try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}", .coverage_generation = 42 });
        if (durable) {
            try std.testing.expectError(error.CatalogGenerationChanged, read.invokeTyped(arena.allocator(), request));
        } else {
            var replacement_fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
            defer replacement_fence.release();
            try std.testing.expectError(error.CatalogGenerationChanged, DB.CypherStatementSnapshot.create(a, &db, &replacement_fence, .{ .indexes = &.{}, .catalog_json = catalog_json, .table = "docs", .table_id = 7, .cut_id = @splat('d'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none));
        }
    }
}

test "external lake Cypher indexed bounds use the captured scalar catalog" {
    const a = std.testing.allocator;
    const DB = local.storage_db_db.DB;
    var directory = try local.common_test_directory.TestDirectory.init("cypher-scalar-catalog");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .start_index_workers = false, .start_optional_runtime_workers = false });
    defer db.close();
    try db.setSchemaJson(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"id\":1}" }, .{ .key = "b", .value = "{\"id\":2}" } } });
    for (0..128) |_| {
        if ((try db.relationalIndexBuildStatus("by_id")).state == .ready) break;
        try db.buildRelationalIndexStep("by_id", .{});
    } else return error.TestIndexNotReady;
    const domain = local.system_catalog_domain;
    const record: domain.Resource = .{ .kind = .graph, .id = 1, .name = "g", .source_json = "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[]}", .graph_tables = &.{.{ .label_index = 0, .table_id = 7, .storage_name = "docs" }} };
    const json = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(json);
    var fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
    defer fence.release();
    var read = try DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .indexes = &.{}, .schema_version = 1, .catalog_json = json, .table = "docs", .table_id = 7, .cut_id = @splat('b'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none);
    defer read.deinit();
    fence.release();
    // Publish removal without changing the schema epoch. Index bounds and row
    // reads must both keep the statement's earlier immutable definitions.
    var schema = db.core.acquireSchemaView().?;
    defer schema.release();
    var removal = try db.core.relational_indexes.prepare(schema, &.{});
    defer removal.deinit();
    {
        db.core.lockApply();
        defer db.core.unlockApply();
        try std.testing.expect(try db.core.relational_indexes.commit(&removal, &.{}));
    }
    const native: *DB.CypherStatementSnapshot.Pinned = @ptrCast(@alignCast(read.ptr));
    const session = try native.db.openRelationalReadSession(a, "", "", .{ .limit = 1, .relational_query_json = "{\"index\":\"by_id\",\"schema_version\":1,\"fields\":[\"id\"],\"lower\":{\"values\":[2]},\"upper\":{\"values\":[2]}}" });
    defer session.deinit();
    var page = try session.reader.nextPage(a, db.backend_runtime.io(), .{ .rows = 1, .output_bytes = 4096 });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqualStrings("b", page.rows[0].key);
    try std.testing.expectEqualStrings("{\"id\":2}", page.rows[0].json);
    for ([_]i64{ 1, 2, 1 }) |id| {
        try session.seekPrefix(&.{.{ .integer = id }}, null);
        var selected = try session.reader.nextPage(a, db.backend_runtime.io(), .{ .rows = 1, .output_bytes = 4096 });
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 1), selected.rows.len);
        try std.testing.expectEqualStrings(if (id == 1) "a" else "b", selected.rows[0].key);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const reply = try read.invokeTyped(arena.allocator(), .{ .operation = .get_many, .graph_incarnation = 1, .graph_revision = 1, .identities = &.{ .{ .table = "docs", .key = "b" }, .{ .table = "docs", .key = "missing" }, .{ .table = "docs", .key = "a" }, .{ .table = "docs", .key = "b" } }, .read_fields = &.{"id"} });
    try std.testing.expectEqual(@as(usize, 4), reply.values.len);
    for ([_]?i64{ 2, null, 1, 2 }, reply.values) |expected, value| {
        if (expected) |id| try std.testing.expectEqual(id, (@import("cypher").value.Value{ .node = value.value.node }).property("id").integer) else try std.testing.expect(value.value == .null);
    }
}

test "external lake Cypher execution reads immutable native document and graph cuts with owner planned sparse predicates" {
    const a = std.testing.allocator;
    const cypher = @import("cypher");
    const DB = local.storage_db_db.DB;
    var directory = try local.common_test_directory.TestDirectory.init("cypher-owner-planned-seeks");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .start_index_workers = false, .start_optional_runtime_workers = false });
    defer db.close();
    try db.setSchemaJson(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]},{"name":"by_bucket","keys":[{"column":"bucket"}]},{"name":"by_large","keys":[{"column":"large"}]},{"name":"by_bucket_code","keys":[{"column":"bucket"},{"column":"code"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"string"},"code":{"type":"string"},"bucket":{"type":"integer"},"large":{"type":"number"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    const writes = try temporary.alloc(local.storage_db_types.BatchWrite, 2000);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(temporary, "n{d}", .{i}), .value = try std.fmt.allocPrint(temporary, "{{\"id\":\"n{d}\",\"code\":\"n{d}\",\"bucket\":{d},\"large\":{d}}}", .{ i, i, i % 4, @as(u64, 9007199254740994) + i * 2 }) };
    try db.batch(.{ .writes = writes });
    for ([_][]const u8{ "by_id", "by_bucket", "by_large", "by_bucket_code" }) |name| for (0..128) |_| {
        if ((try db.relationalIndexBuildStatus(name)).state == .ready) break;
        try db.buildRelationalIndexStep(name, .{});
    } else return error.TestIndexNotReady;
    const domain = local.system_catalog_domain;
    const record: domain.Resource = .{ .kind = .graph, .id = 1, .name = "g", .source_json = "{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[]}", .graph_tables = &.{.{ .label_index = 0, .table_id = 7, .storage_name = "docs" }} };
    const json = try std.json.Stringify.valueAlloc(temporary, record, .{});
    var fence = (try db.tryStatementReadFence()) orelse return error.TestExpectedFence;
    defer fence.release();
    var read = try DB.CypherStatementSnapshot.create(a, &db, &fence, .{ .indexes = &.{}, .schema_version = 1, .catalog_json = json, .table = "docs", .table_id = 7, .cut_id = @splat('e'), .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60_000, .ttl_now_ns = @import("antfly_platform").time.realtimeNs() }, .none);
    defer read.deinit();
    fence.release();
    const Channel = struct {
        fn call(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedWireCall;
        }
        fn typed(ptr: *anyopaque, alloc: std.mem.Allocator, request: cypher.rpc.Request) !cypher.rpc.Reply {
            const port: *local.storage_statement_read_fence.GraphRead = @ptrCast(@alignCast(ptr));
            var bytes = std.Io.Writer.Discarding.init(&.{});
            try std.json.Stringify.value(request, .{}, &bytes.writer);
            try std.testing.expect(bytes.fullCount() <= 1 << 20);
            return port.invokeTyped(alloc, request);
        }
    };
    var client: cypher.rpc.Client = .{ .channel = .{ .ptr = &read, .call = Channel.call, .typed = Channel.typed }, .incarnation = 1, .revision = 1 };
    const probe_filters = &[_]cypher.rpc.Filter{.{ .property = "id", .op = .in, .domain = .string, .value = .{ .array = std.array_list.Managed(std.json.Value).init(temporary) } }};
    var probe_filter = probe_filters[0];
    try probe_filter.value.array.append(.{ .string = "n1999" });
    try probe_filter.value.array.append(.{ .string = "n1998" });
    for ([_]usize{ 1, 2 }) |maximum| {
        const reply = try read.invokeTyped(temporary, .{ .operation = .probe, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .filters = &.{probe_filter}, .limit = maximum });
        try std.testing.expect(reply.probe_supported);
        try std.testing.expectEqual(maximum == 2, reply.probe_complete);
        try std.testing.expectEqual(@as(usize, if (maximum == 2) 2 else 0), reply.values.len);
        try std.testing.expectEqual(@as(?usize, 2), reply.examined);
    }
    const exhausted = try read.invokeTyped(temporary, .{ .operation = .probe, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .filters = &.{probe_filter}, .limit = 2, .max_work = 0 });
    try std.testing.expectEqual(cypher.rpc.BudgetFailure.work, exhausted.read_failure.?);
    // Exercise the sampled planning protocol on a real immutable SQL reader,
    // then resume against the complete original domain through the RPC client.
    var samples = std.array_list.Managed(std.json.Value).init(temporary);
    for ([_][]const u8{ "n0", "n200", "n399" }) |key| try samples.append(.{ .string = key });
    const planned = try read.invokeTyped(temporary, .{ .operation = .scan, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .filters = &.{.{ .property = "id", .op = .in, .domain = .string, .plan = .{ .length = 400, .max_string_bytes = 4 }, .value = .{ .array = samples } }} });
    try std.testing.expectEqual(@as(usize, 0), planned.values.len);
    try std.testing.expectEqual(@as(?usize, 0), planned.examined);
    try std.testing.expectEqual(@as(?usize, 0), planned.seek_window.?.filter);
    const values = try temporary.alloc(cypher.value.Value, 400);
    for (values, 0..) |*value, i| value.* = .{ .string = try std.fmt.allocPrint(temporary, "n{d}", .{i}) };
    var after: ?[]const u8 = try std.json.Stringify.valueAlloc(temporary, .{ .sparse_seek = .{ .window = planned.seek_window.?, .after = planned.next.? } }, .{});
    var seen: [400]bool = @splat(false);
    var count: usize = 0;
    const backend = client.backend();
    while (after != null) {
        const page = try backend.vtable.scan_where.?(backend.ptr, temporary, "docs", &.{.{ .property = "id", .op = .in, .domain = .string, .value = .{ .list = values } }}, after, 128);
        for (page.items) |node| {
            const ordinal = try std.fmt.parseUnsigned(usize, node.identity.key[1..], 10);
            try std.testing.expect(ordinal < seen.len and !seen[ordinal]);
            seen[ordinal] = true;
            count += 1;
        }
        after = page.next;
    }
    try std.testing.expectEqual(@as(usize, 400), count);
    // A composite IN plan must resume one-row pages in mixed-radix order.
    // No independent index exists on code, so a prefix-only scan would visit
    // 1,000 rows and cannot satisfy this two-anchor contract.
    const composite = [_]cypher.backend.PropertyFilter{
        .{ .property = "bucket", .op = .in, .domain = .integer, .value = .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 } } } },
        .{ .property = "code", .op = .in, .domain = .string, .value = .{ .list = &.{ .{ .string = "n1997" }, .{ .string = "n1998" } } } },
    };
    after = null;
    var composite_rows: usize = 0;
    var composite_anchors: usize = 0;
    while (true) {
        const page = try backend.vtable.scan_where.?(backend.ptr, temporary, "docs", &composite, after, 1);
        composite_rows += page.items.len;
        composite_anchors += page.examined orelse page.items.len;
        after = page.next orelse break;
    }
    try std.testing.expectEqual(@as(usize, 2), composite_rows);
    try std.testing.expectEqual(@as(usize, 2), composite_anchors);
    // An unrelated oversized domain must not destroy the cheap complete
    // prefix domains needed by the selected composite plan.
    const long_prefix = try temporary.alloc(u8, 3000);
    @memset(long_prefix, 'x');
    const irrelevant = try temporary.alloc(cypher.value.Value, 400);
    for (irrelevant, 0..) |*value, i| value.* = .{ .string = try std.fmt.allocPrint(temporary, "{s}{d}", .{ long_prefix, i }) };
    const sampled_composite = [_]cypher.backend.PropertyFilter{
        .{ .property = "absent", .op = .in, .domain = .string, .value = .{ .list = irrelevant } },
        .{ .property = "bucket", .op = .in, .domain = .integer, .value = .{ .list = &.{ .{ .integer = 0 }, .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 3 } } } },
        composite[1],
    };
    after = null;
    composite_rows = 0;
    composite_anchors = 0;
    while (true) {
        const page = try backend.vtable.scan_where.?(backend.ptr, temporary, "docs", &sampled_composite, after, 1);
        composite_rows += page.items.len;
        composite_anchors += page.examined orelse page.items.len;
        after = page.next orelse break;
    }
    try std.testing.expectEqual(@as(usize, 2), composite_rows);
    try std.testing.expectEqual(@as(usize, 2), composite_anchors);
    const graph: cypher.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    for ([_]struct { query: []const u8, count: i64, anchors: usize }{
        .{ .query = "MATCH(n:D) WHERE n.bucket IN [0,1,2,3] AND n.id='n1999' RETURN count(*)", .count = 1, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.bucket IN [0,1,2,3] AND n.id IN ['n1999','n1998'] RETURN count(*)", .count = 2, .anchors = 2 },
        .{ .query = "MATCH(n:D) WHERE n.id IN ['n1999','n1998'] AND n.bucket IN [0,1,2,3] RETURN count(*)", .count = 2, .anchors = 2 },
        .{ .query = "MATCH(n:D) WHERE n.bucket IN [1,2] AND n.code IN ['n1997','n1998'] RETURN count(*)", .count = 2, .anchors = 2 },
        .{ .query = "MATCH(n:D) WHERE n.code IN ['n1997','n1998'] AND n.bucket IN [1,2] RETURN count(*)", .count = 2, .anchors = 2 },
        .{ .query = "MATCH(n:D) WHERE n.bucket IN [1,1,null,2] AND n.code IN ['n1997','n1998','n1997'] RETURN count(*)", .count = 2, .anchors = 2 },
        .{ .query = "MATCH(n:D) WHERE n.large=9007199254740994 RETURN count(*)", .count = 1, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.missing IN [1,2] AND n.large=9007199254740994 RETURN count(*)", .count = 0, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.large=9007199254740993 AND n.id='n1999' RETURN count(*)", .count = 0, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.id IN [] RETURN count(*)", .count = 0, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.id IN [null] RETURN count(*)", .count = 0, .anchors = 1 },
        .{ .query = "MATCH(n:D) WHERE n.missing IN [1,2] AND n.id='n1999' RETURN count(*)", .count = 0, .anchors = 1 },
        .{ .query = "UNWIND ['n1999','n1998','n1999'] AS k MATCH(n:D {id:k}) RETURN count(*)", .count = 3, .anchors = 3 },
        .{ .query = "UNWIND range(0,399) AS k WITH collect('n'+toString(k)) AS ids MATCH(n:D) WHERE n.id IN ids RETURN count(*)", .count = 400, .anchors = 400 },
    }) |case| {
        var diagnostic: cypher.compiler.Diagnostic = .{};
        var compiled = try cypher.compiler.compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = cypher.runtime.execute(a, client.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = case.anchors }) catch |err| {
            std.debug.print("owner planned query failed: {s}: {s}\n", .{ @errorName(err), case.query });
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.rows[0][0].integer);
        try std.testing.expect(result.stats.scanned_anchors <= case.anchors);
    }
}
