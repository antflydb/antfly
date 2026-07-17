// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const cleanup = @import("seed_prefix_cleanup.zig");
const namespace_control = @import("seed_namespace_control.zig");
const object_storage = @import("../object_storage.zig");

const location = "s3://ha-bucket/instances/instance-a/ha-seeds/";
const object_prefix = "instances/instance-a/ha-seeds/";

fn requestAlloc(alloc: std.mem.Allocator) !cleanup.Request {
    const prefix_sha256 = try cleanup.sha256HexAlloc(alloc, location);
    errdefer alloc.free(prefix_sha256);
    var request = cleanup.Request{
        .version = 1,
        .kind = "DeleteHASeedPrefix",
        .operation_id = "cleanup-instance-a-7",
        .retry_token = "retry-instance-a-7",
        .instance_id = "instance-a",
        .topology_id = "topology-a",
        .topology_generation = 7,
        .location = location,
        .prefix_sha256 = prefix_sha256,
        .credentials_secret_name = "instance-a-ha-seed-store",
        .delete_all = true,
        .request_sha256 = "",
    };
    request.request_sha256 = try cleanup.requestSha256Alloc(alloc, request);
    return request;
}

fn freeRequest(alloc: std.mem.Allocator, request: cleanup.Request) void {
    alloc.free(request.prefix_sha256);
    alloc.free(request.request_sha256);
}

fn put(client: *object_storage.ObjectStorage, bucket: []const u8, key: []const u8) !void {
    var result = try client.putObject(bucket, key, "x", .{});
    result.deinit(std.testing.allocator);
}

test "storage.ha seed prefix cleanup deletes the exact instance prefix and emits a bound receipt" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-bucket");

    try put(&client, "ha-bucket", object_prefix ++ "generations/gen-a/COMPLETE.json");
    try put(&client, "ha-bucket", object_prefix ++ "generations/gen-a/files/catalog");
    try put(&client, "ha-bucket", object_prefix ++ "generations/gen-b/COMPLETE.json");
    try put(&client, "ha-bucket", object_prefix ++ "uploads/orphan.part");
    try put(&client, "ha-bucket", "instances/instance-a-other/ha-seeds/generations/keep/COMPLETE.json");

    const request = try requestAlloc(alloc);
    defer freeRequest(alloc, request);
    var result = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{
        .max_keys = 2,
        .completed_at_override = "2026-07-14T12:34:56.123456789Z",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.deleted_generations);
    try std.testing.expectEqual(@as(usize, 4), result.deleted_objects);
    var parsed = try std.json.parseFromSlice(cleanup.Receipt, alloc, result.receipt_json, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    try cleanup.validateReceipt(alloc, parsed.value, request);
    try std.testing.expect(parsed.value.complete);
    try std.testing.expect(parsed.value.prefix_empty);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.retained_objects);
    try std.testing.expectEqualStrings("2026-07-14T12:34:56.123456789Z", parsed.value.completed_at);

    var exact = try client.listObjects("ha-bucket", .{ .prefix = object_prefix, .recursive = true });
    defer exact.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), exact.entries.len);
    var sibling = try client.getObject("ha-bucket", "instances/instance-a-other/ha-seeds/generations/keep/COMPLETE.json", .{});
    defer sibling.deinit(alloc);
    try std.testing.expectEqualStrings("x", sibling.body);
}

test "storage.ha seed prefix cleanup fails closed on mutated authority and is idempotent when empty" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-bucket");

    const request = try requestAlloc(alloc);
    defer freeRequest(alloc, request);
    var wrong_digest = request;
    wrong_digest.request_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try std.testing.expectError(error.SeedPrefixCleanupRequestDigestMismatch, cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, wrong_digest, .{}));

    var wrong_prefix = request;
    wrong_prefix.location = "s3://ha-bucket/instances/instance-a/ha-seeds-extra/";
    try std.testing.expectError(error.InvalidSeedPrefixCleanupPrefix, cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = "instances/instance-a/ha-seeds-extra/",
    }, wrong_prefix, .{}));

    var not_delete_all = request;
    not_delete_all.delete_all = false;
    try std.testing.expectError(error.SeedPrefixCleanupDeleteAllRequired, cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, not_delete_all, .{}));

    var result = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{ .completed_at_override = "2026-07-14T12:34:56Z" });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), result.deleted_generations);
    try std.testing.expectEqual(@as(usize, 0), result.deleted_objects);
}

test "storage.ha seed cleanup excludes publishers and leaves a durable tombstone" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-bucket");

    const binding = namespace_control.Binding{
        .topology_id = "topology-a",
        .topology_generation = 7,
    };
    const control_store = namespace_control.Store{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    };
    var publishing = try namespace_control.acquirePublish(alloc, control_store, binding, "generation-live");
    defer publishing.deinit(alloc);

    const request = try requestAlloc(alloc);
    defer freeRequest(alloc, request);
    try std.testing.expectError(error.SeedPrefixCleanupWriterActive, cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{}));

    try namespace_control.releasePublish(alloc, control_store, binding, "generation-live", publishing);
    var result = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{ .completed_at_override = "2026-07-14T12:34:56Z" });
    defer result.deinit(alloc);

    try std.testing.expectError(error.SeedNamespaceUnavailable, namespace_control.acquirePublish(
        alloc,
        control_store,
        binding,
        "generation-after-delete",
    ));

    // A retried cleanup returns the exact receipt persisted in the tombstone.
    var retry = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{ .completed_at_override = "2026-07-15T00:00:00Z" });
    defer retry.deinit(alloc);
    try std.testing.expectEqualStrings(result.receipt_json, retry.receipt_json);
}
