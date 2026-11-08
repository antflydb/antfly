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
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const admission = lifecycle.source_admission;
const A = std.mem.Allocator;

fn publication(generation: u64) local.metadata_lake_index_catalog.Publication {
    return .{ .reader_protocol = local.metadata_lake_index_catalog.native_reader_protocol, .generation = generation, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) }, .published_at_ms = 1, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } }, .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } };
}
fn owner() admission.State {
    return .{ .kind = .managed_iceberg, .owner = @splat(8), .signing_key = @splat(9), .object_generation = 7, .source = @splat(6) };
}
fn scope() lifecycle.SourceScope {
    return .{ .object_generation = 7, .desired = @splat(1), .owner = @splat(8) };
}
fn cut() admission.Cut {
    return .{ .desired = @splat(1), .store = @splat(4), .generation = 1, .reader_token = @splat(5), .state = .{ .id = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:graph:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:cccccccccccccccccccccccccccccccc", .bytes = 64 }, .wal_lsn = 10, .base_lsn = 8, .created_ms = 1000, .refresh_after_ms = 301000, .expires_ms = 721000, .cursor_ttl_ms = 300000 };
}
fn admitted(a: A) !lifecycle.State {
    var state = try (lifecycle.State{}).synchronize(a, .{ .generation = 1, .published = publication(1) });
    state = try (lifecycle.Mutation{ .configure_source = .{ .scope = scope(), .state = owner() } }).apply(a, state);
    state = try (lifecycle.Mutation{ .activate_source = scope() }).apply(a, state);
    state = try state.acquire(a, @splat(5), 1, 1000);
    return (lifecycle.Mutation{ .admit_source = .{ .scope = scope(), .epoch = 1, .source_uuid = "source", .cut = cut(), .now_ms = 1000 } }).apply(a, state);
}

test "external lake owned serving pins durable cursors through ACK replacement and collection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try admitted(a);
    try std.testing.expectEqual(@as(u64, 10), state.source_owner.?.accepted_lsn);
    try std.testing.expect(state.readers[0].expires_ms >= cut().expires_ms);
    try std.testing.expectError(error.LakeIndexReaderTokenReused, state.release(a, @splat(5)));
    state = try (lifecycle.Mutation{ .acknowledge_source = .{ .scope = scope(), .lsn = 11 } }).apply(a, state);
    try std.testing.expect(state.source_owner.?.current == null);
    try std.testing.expectError(error.CatalogGenerationChanged, (lifecycle.Mutation{ .admit_source = .{ .scope = scope(), .epoch = 1, .source_uuid = "source", .cut = cut(), .now_ms = 1001 } }).apply(a, state));
    state = try state.synchronize(a, .{ .generation = 2, .published = publication(2) });
    state = try state.beginCollection(a, @splat(7), @splat(4), 2000);
    try std.testing.expectEqual(@as(usize, 0), state.collection.?.retired.len);
    state = try state.finishCollection(a, @splat(7), 2001);
    state = try state.beginCollection(a, @splat(7), @splat(4), cut().expires_ms + lifecycle.grace_ms + 1);
    try std.testing.expectEqualSlices(u64, &.{1}, state.collection.?.retired);
}

test "external lake owned serving expires stale cuts before pruning reader sessions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try admitted(a);
    state = try state.acquire(a, @splat(7), 1, cut().expires_ms + lifecycle.grace_ms + 1);
    try std.testing.expect(state.source_owner.?.current == null);
    try std.testing.expectEqual(@as(usize, 1), state.readers.len);
    try state.validate();
    var wrong = scope();
    wrong.object_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, (lifecycle.Mutation{ .acknowledge_source = .{ .scope = wrong, .lsn = 11 } }).apply(a, state));
    const bytes = try lifecycle.encode(a, state);
    const reopened = try lifecycle.parse(a, bytes);
    try std.testing.expectEqual(@as(u16, 2), reopened.version);
    try std.testing.expectEqualSlices(u8, &owner().signing_key, &reopened.source_owner.?.signing_key);
    const legacy = try lifecycle.encode(a, .{});
    try std.testing.expect(std.mem.indexOf(u8, legacy, "source_owner") == null);
}

fn admissionAllocationFailures(backing: A) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    _ = try admitted(arena.allocator());
}
test "external lake owned serving partial metadata construction drains allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, admissionAllocationFailures, .{});
}

test "external lake owned serving WAL fencing rejects unhooked writers and preserves reads and exact retries" {
    const objects = @import("objectstore");
    const Indexed = @import("../serverless/wal/indexed_object_store.zig").IndexedObjectStore;
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var old = try Indexed.init(a, client, "db", "owned");
    defer old.deinit();
    try std.testing.expectEqual(@as(?u64, 1), try old.appendIdempotent("rows", 0, "first", "one"));
    var current = try Indexed.init(a, client, "db", "owned");
    defer current.deinit();
    var pending = try old.beginAdmission("rows", 0, "pending", "before-claim");
    defer pending.deinit();
    try current.claimNativeOwner("rows", @splat(1));
    try std.testing.expectError(error.LakeSourceOwnerRequired, pending.append());
    try std.testing.expectEqual(@as(u64, 1), try old.latestLsn("rows"));
    try std.testing.expectError(error.LakeSourceOwnerRequired, old.appendIdempotent("rows", 0, "new", "two"));
    try std.testing.expectError(error.LakeSourceOwnerRequired, old.beginAdmission("rows", 0, "first", "one"));
    try std.testing.expectEqual(@as(?u64, 1), try current.appendIdempotent("rows", 0, "first", "one"));
    try std.testing.expectEqual(@as(?u64, 2), try current.appendIdempotent("rows", 0, "new", "two"));
    try std.testing.expectError(error.LakeSourceOwnerRequired, current.claimNativeOwner("rows", @splat(2)));
}

test "external lake owned serving catalog fencing preserves committed legacy retries but rejects unhooked commits" {
    const objects = @import("objectstore");
    const catalog = local.serverless_external_source_mod.lake_catalog;
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var legacy: catalog.managed.Managed = .{ .client = client, .bucket = "db", .prefix = "catalog", .source_uri = "gs://db/catalog" };
    const schema = "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}}";
    var initial = try legacy.create(a, "initialize", schema, 1);
    defer initial.deinit(a);
    try legacy.claimNativeOwner(a, @splat(1));
    var current = legacy;
    current.native_owner = @splat(1);
    var retried = try current.create(a, "initialize", schema, 1);
    defer retried.deinit(a);
    try std.testing.expectEqualStrings(initial.metadata_location, retried.metadata_location);
    const change: catalog.types.Commit = .{ .id = "settings", .expected_metadata_location = initial.metadata_location, .body = "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"owner\":\"HN\"}}]}", .timestamp_ms = 2 };
    try std.testing.expectError(error.LakeSourceOwnerRequired, legacy.commit(a, change));
    // The rejected unhooked attempt staged a legacy intent. It must not be
    // retroactively authorized by borrowing the new owner's nonce.
    try std.testing.expectError(error.LakeSourceOwnerRequired, current.commit(a, change));
    var next = change;
    next.id = "owned-settings";
    var committed = try current.commit(a, next);
    defer committed.deinit(a);
    var replay = try current.commit(a, next);
    defer replay.deinit(a);
    try std.testing.expectEqualStrings(committed.metadata_location, replay.metadata_location);
    try std.testing.expectError(error.LakeSourceOwnerRequired, current.claimNativeOwner(a, @splat(2)));
}

test "external lake owned serving retries a remotely durable write after native acknowledgement failure" {
    const ingestion = @import("../serverless/lake_ingestion.zig");
    const configured = @import("../serverless/configured_object_store_support.zig");
    const catalog = local.serverless_external_source_mod.lake_catalog;
    const Context = catalog.types.Context;
    var directory = try local.common_test_directory.TestDirectory.init("lake-owner-unknown");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(std.testing.allocator, json);
    defer config.deinit();
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const Ack = struct {
        calls: usize = 0,
        lsn: ?u64 = null,
        fn record(raw: *anyopaque, _: u64, _: u64, _: [16]u8, lsn: ?u64, _: Context) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.lsn = lsn;
            if (self.calls == 1) return error.Canceled;
        }
    };
    var ack: Ack = .{};
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 7, .native_owner = .{ .token = @splat(1), .ptr = &ack, .table_id = 7, .object_generation = 0, .acknowledge = Ack.record } };
    var opened = try configured.openBindingObjectStoreAlloc(a, binding, options);
    defer opened.deinit();
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    _ = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    try managed.claimNativeOwner(a, @splat(1));
    var queue = try ingestion.openQueue(a, binding, options);
    defer queue.deinit();
    const store: @import("../serverless/lake_wal.zig").Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try ingestion.prefix(a, queue.prefix, binding, options), .native_owner = @splat(1) };
    var wal = try store.open(a, true);
    defer wal.deinit();
    try wal.indexed.?.claimNativeOwner(store.collectionNamespace(), @splat(1));
    const payload = "{\"batch_id\":\"first\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1}}]}";
    try std.testing.expectError(error.LakeCommitOutcomeUnknown, ingestion.accept(a, binding, options, .{}, payload));
    try std.testing.expectEqual(@as(?u64, 1), ack.lsn);
    // An exact retry repeats the native acknowledgement, never the WAL append.
    try std.testing.expectEqual(@as(u64, 1), try ingestion.accept(a, binding, options, .{}, payload));
    try std.testing.expectEqual(@as(usize, 2), ack.calls);
    try std.testing.expectEqual(@as(u64, 1), try wal.indexed.?.latestLsn(store.collectionNamespace()));
}
