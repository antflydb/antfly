// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const cleanup = @import("seed_prefix_cleanup.zig");
const namespace_control = @import("seed_namespace_control.zig");
const object_storage = @import("../object_storage.zig");

const location = "s3://ha-bucket/orgs/org-a/instances/instance-a/ha-seeds/";
const object_prefix = "orgs/org-a/instances/instance-a/ha-seeds/";

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

const VersionedTestStore = struct {
    const keys = [_][]const u8{
        object_prefix ++ "generations/gen-a/COMPLETE.json",
        object_prefix ++ "generations/gen-a/COMPLETE.json",
        object_prefix ++ "uploads/orphan.part",
    };
    const version_ids = [_][]const u8{ "version-2", "version-1", "delete-marker-1" };
    const delete_markers = [_]bool{ false, false, true };

    backing: object_storage.MemoryObjectStorage,
    deleted: [keys.len]bool = @splat(false),

    fn init(alloc: std.mem.Allocator) VersionedTestStore {
        return .{ .backing = object_storage.MemoryObjectStorage.init(alloc) };
    }

    fn client(self: *VersionedTestStore, alloc: std.mem.Allocator) object_storage.ObjectStorage {
        return .{ .allocator = alloc, .ptr = self, .vtable = &vtable };
    }

    fn deinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        self.backing.deinit();
    }

    fn bucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        return try backing.bucketExists(bucket);
    }

    fn makeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        try backing.makeBucket(bucket);
    }

    fn putObject(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8, body: []const u8, opts: object_storage.PutOptions) !object_storage.PutResult {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        backing.allocator = alloc;
        return try backing.putObject(bucket, key, body, opts);
    }

    fn getObject(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8, opts: object_storage.GetOptions) !object_storage.GetResult {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        backing.allocator = alloc;
        return try backing.getObject(bucket, key, opts);
    }

    fn getObjectAttributes(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8) !object_storage.ObjectAttributes {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        backing.allocator = alloc;
        return try backing.getObjectAttributes(bucket, key);
    }

    fn statObject(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, key: []const u8) !object_storage.ObjectMetadata {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        backing.allocator = alloc;
        return try backing.statObject(bucket, key);
    }

    fn deleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, opts: object_storage.DeleteOptions) !void {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        if (opts.version_id) |version_id| {
            for (keys, version_ids, 0..) |candidate_key, candidate_version, index| {
                if (std.mem.eql(u8, key, candidate_key) and std.mem.eql(u8, version_id, candidate_version)) {
                    if (self.deleted[index]) return error.FileNotFound;
                    self.deleted[index] = true;
                    return;
                }
            }
            return error.FileNotFound;
        }
        var backing = self.backing.client();
        try backing.deleteObject(bucket, key, opts);
    }

    fn listObjects(ptr: *anyopaque, alloc: std.mem.Allocator, bucket: []const u8, opts: object_storage.ListOptions) !object_storage.ListResult {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var backing = self.backing.client();
        backing.allocator = alloc;
        return try backing.listObjects(bucket, opts);
    }

    fn listObjectVersions(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, opts: object_storage.ListObjectVersionsOptions) !object_storage.ListObjectVersionsResult {
        const self: *VersionedTestStore = @ptrCast(@alignCast(ptr));
        var start: usize = 0;
        if (opts.key_marker) |key_marker| {
            const version_marker = opts.version_id_marker orelse return error.InvalidVersionPagination;
            var found = false;
            for (keys, version_ids, 0..) |key, version_id, index| {
                if (std.mem.eql(u8, key_marker, key) and std.mem.eql(u8, version_marker, version_id)) {
                    start = index + 1;
                    found = true;
                    break;
                }
            }
            if (!found) return error.InvalidVersionPagination;
        }

        var entries = std.ArrayListUnmanaged(object_storage.ObjectVersionEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }
        var last_index: ?usize = null;
        var index = start;
        while (index < keys.len and entries.items.len < opts.max_keys) : (index += 1) {
            if (self.deleted[index] or !std.mem.startsWith(u8, keys[index], opts.prefix)) continue;
            const key = try alloc.dupe(u8, keys[index]);
            errdefer alloc.free(key);
            const version_id = try alloc.dupe(u8, version_ids[index]);
            errdefer alloc.free(version_id);
            try entries.append(alloc, .{
                .key = key,
                .version_id = version_id,
                .is_delete_marker = delete_markers[index],
            });
            last_index = index;
        }
        var has_more = false;
        while (index < keys.len) : (index += 1) {
            if (!self.deleted[index] and std.mem.startsWith(u8, keys[index], opts.prefix)) {
                has_more = true;
                break;
            }
        }
        const marker_index = if (has_more) last_index orelse return error.InvalidVersionPagination else null;
        const owned_entries = try entries.toOwnedSlice(alloc);
        errdefer {
            for (owned_entries) |*entry| entry.deinit(alloc);
            alloc.free(owned_entries);
        }
        const next_key_marker = if (marker_index) |value| try alloc.dupe(u8, keys[value]) else null;
        errdefer if (next_key_marker) |value| alloc.free(value);
        const next_version_id_marker = if (marker_index) |value| try alloc.dupe(u8, version_ids[value]) else null;
        return .{
            .entries = owned_entries,
            .is_truncated = has_more,
            .next_key_marker = next_key_marker,
            .next_version_id_marker = next_version_id_marker,
        };
    }

    const vtable: object_storage.ObjectStorage.VTable = .{
        .deinit = deinit,
        .bucket_exists = bucketExists,
        .make_bucket = makeBucket,
        .put_object = putObject,
        .get_object = getObject,
        .get_object_attributes = getObjectAttributes,
        .stat_object = statObject,
        .delete_object = deleteObject,
        .list_objects = listObjects,
        .list_object_versions = listObjectVersions,
    };
};

test "storage.ha seed prefix cleanup purges every version and delete marker before receipt" {
    const alloc = std.testing.allocator;
    var versioned = VersionedTestStore.init(alloc);
    var client = versioned.client(alloc);
    defer client.deinit();
    try client.makeBucket("ha-bucket");

    const request = try requestAlloc(alloc);
    defer freeRequest(alloc, request);
    var result = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{
        .max_keys = 2,
        .completed_at_override = "2026-07-14T12:34:56Z",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.deleted_generations);
    try std.testing.expectEqual(@as(usize, 3), result.deleted_objects);
    try std.testing.expectEqual([_]bool{ true, true, true }, versioned.deleted);
    var remaining = try client.listObjectVersions("ha-bucket", .{ .prefix = object_prefix });
    defer remaining.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), remaining.entries.len);
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
    try put(&client, "ha-bucket", "orgs/org-a/instances/instance-a-other/ha-seeds/generations/keep/COMPLETE.json");

    const request = try requestAlloc(alloc);
    defer freeRequest(alloc, request);
    var result = try cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = object_prefix,
    }, request, .{
        .max_keys = 2,
        .require_version_purge = false,
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
    var sibling = try client.getObject("ha-bucket", "orgs/org-a/instances/instance-a-other/ha-seeds/generations/keep/COMPLETE.json", .{});
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
    wrong_prefix.location = "s3://ha-bucket/orgs/org-a/instances/instance-a/ha-seeds-extra/";
    try std.testing.expectError(error.InvalidSeedPrefixCleanupPrefix, cleanup.deleteAll(alloc, .{
        .client = &client,
        .bucket = "ha-bucket",
        .prefix = "orgs/org-a/instances/instance-a/ha-seeds-extra/",
    }, wrong_prefix, .{}));

    var nested_scope = request;
    nested_scope.location = "s3://ha-bucket/tenants/tenant-a/orgs/org-a/instances/instance-a/ha-seeds/";
    try std.testing.expectError(error.InvalidSeedPrefixCleanupPrefix, cleanup.validateRequestAuthority(alloc, nested_scope));

    var missing_separator = request;
    missing_separator.location = "s3://ha-bucket/orgs/org-a/instances/instance-a/ha-seeds";
    try std.testing.expectError(error.InvalidSeedPrefixCleanupPrefix, cleanup.validateRequestAuthority(alloc, missing_separator));

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
    }, request, .{
        .require_version_purge = false,
        .completed_at_override = "2026-07-14T12:34:56Z",
    });
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
    }, request, .{
        .require_version_purge = false,
        .completed_at_override = "2026-07-14T12:34:56Z",
    });
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

test "storage.ha seed namespace hands active authority to a newer same-topology generation" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("ha-bucket");
    const store = namespace_control.Store{ .client = &client, .bucket = "ha-bucket", .prefix = object_prefix };
    const generation_one = namespace_control.Binding{ .topology_id = "topology-a", .topology_generation = 1 };
    var first = try namespace_control.acquirePublish(alloc, store, generation_one, "initial-standby-a-1");
    try namespace_control.releasePublish(alloc, store, generation_one, "initial-standby-a-1", first);
    first.deinit(alloc);

    const generation_two = namespace_control.Binding{ .topology_id = "topology-a", .topology_generation = 2 };
    var second = try namespace_control.acquirePublish(alloc, store, generation_two, "replacement-standby-b-2");
    try namespace_control.releasePublish(alloc, store, generation_two, "replacement-standby-b-2", second);
    second.deinit(alloc);

    try std.testing.expectError(error.SeedNamespaceUnavailable, namespace_control.acquirePublish(
        alloc,
        store,
        generation_one,
        "rollback-generation",
    ));
    try std.testing.expectError(error.SeedNamespaceUnavailable, namespace_control.acquirePublish(
        alloc,
        store,
        .{ .topology_id = "topology-b", .topology_generation = 3 },
        "foreign-topology",
    ));
}
