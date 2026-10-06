// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Complete source coverage for immutable native lake index generations.
//! Provider versions are required; file sizes and snapshot labels alone cannot
//! authorize persistent index reuse. Footer discovery does not change identity.
const std = @import("std");
const local = @import("antfly_local_sources");
const Source = local.serverless_query_lake_serving.ServingSource;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const Hash = std.crypto.hash.sha2.Sha256;
fn part(hash: *Hash, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, @intCast(bytes.len), .little);
    hash.update(&length);
    hash.update(bytes);
}
fn word(hash: *Hash, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn optionalWord(hash: *Hash, value: ?u64) void {
    hash.update(&.{@intFromBool(value != null)});
    if (value) |number| word(hash, number);
}
fn versionIsStrong(etag: []const u8, version: []const u8) bool {
    return etag.len != 0 or (version.len != 0 and !std.mem.startsWith(u8, version, "object-stat:v1:") and !std.mem.startsWith(u8, version, "iceberg:"));
}
fn jsonPart(a: A, hash: *Hash, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    part(hash, bytes);
}
/// Pin all covered data objects before admitting a build. Calling again after
/// the build rejects changed versions before publication. Selection applies
/// the same proof to the freshly authorized source and falls back on mismatch.
pub const Coverage = struct { source: [32]u8, delete_objects: [32]u8 };
pub fn pin(source: *Source, context: Context) !Coverage {
    const a = source.alloc;
    var client = source.scanner.object_reader.client;
    client.allocator = a;
    // Fresh provider evidence is mandatory; bounded shared scheduling reduces
    // network latency without treating a TTL or cached bytes as authority.
    const Work = struct {
        client: local.storage_object_storage.ObjectStorage,
        uri: []const u8,
        context: Context,
        fn stat(self: @This()) anyerror!local.storage_object_storage.ObjectMetadata {
            try self.context.ensureActive();
            const location = try local.serverless_query_lake_range_io.objectLocationForUri(self.uri);
            var worker_client = self.client;
            worker_client.allocator = std.heap.page_allocator;
            return worker_client.statObject(location.bucket, location.key);
        }
    };
    var begin: usize = 0;
    while (begin < source.inventory.files.len) {
        const count = @min(8, source.inventory.files.len - begin);
        var tasks: [8]?local.sql_parallel_scheduler.Task(anyerror!local.storage_object_storage.ObjectMetadata) = @splat(null);
        var metadata_results: [8]?local.storage_object_storage.ObjectMetadata = @splat(null);
        defer {
            for (&tasks, &metadata_results) |*task, *result| if (task.*) |*future| {
                result.* = future.cancel(context.io.?) catch null;
            };
            for (&metadata_results) |*result| if (result.*) |*metadata| metadata.deinit(std.heap.page_allocator);
        }
        for (0..count) |slot| {
            const work: Work = .{ .client = client, .uri = source.inventory.files[begin + slot].object_uri, .context = context };
            if (context.io) |io| tasks[slot] = local.sql_parallel_scheduler.global().submitTransient(io, 64 * 1024, Work.stat, .{work});
            if (tasks[slot] == null) metadata_results[slot] = try work.stat();
        }
        for (tasks[0..count], 0..) |optional, slot| if (optional != null) {
            const task = &tasks[slot].?;
            const result = task.await(context.io.?);
            tasks[slot] = null;
            metadata_results[slot] = try result;
        };
        for (metadata_results[0..count], 0..) |optional, slot| {
            const metadata = optional.?;
            const index = begin + slot;
            const file = &source.inventory.files[index];
            try context.ensureActive();
            const etag = metadata.etag orelse "";
            const version = metadata.version_id orelse "";
            if (metadata.content_length != file.byte_len or !versionIsStrong(etag, version)) return error.InvalidExternalLakeIndexCoverage;
            const unresolved = (source.lazy_versions and !source.pinned_files[index]) or !versionIsStrong(file.etag, file.version_id);
            if (!unresolved and ((file.etag.len != 0 and !std.mem.eql(u8, file.etag, etag)) or
                (file.version_id.len != 0 and !std.mem.eql(u8, file.version_id, version)))) return error.ExternalLakeIndexSourceChanged;
            if (std.mem.eql(u8, file.etag, etag) and std.mem.eql(u8, file.version_id, version)) {
                if (source.lazy_versions) source.pinned_files[index] = true;
                continue;
            }
            const pinned_etag = try a.dupe(u8, etag);
            errdefer a.free(pinned_etag);
            const pinned_version = try a.dupe(u8, version);
            if (file.etag.len != 0) a.free(file.etag);
            if (file.version_id.len != 0) a.free(file.version_id);
            file.etag = pinned_etag;
            file.version_id = pinned_version;
            if (source.lazy_versions) source.pinned_files[index] = true;
        }
        begin += count;
    }
    try source.inventory.validateAlloc(a);
    var hash = Hash.init(.{});
    hash.update("native-lake-source-coverage-v1");
    try jsonPart(a, &hash, .{ .format = source.inventory.format, .source_id = source.inventory.source_id, .source_uri = source.inventory.source_uri, .snapshot_id = source.inventory.snapshot_id, .schema = source.inventory.schema_fingerprint });
    const order = try a.alloc(usize, source.inventory.files.len);
    defer a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    std.mem.sort(usize, order, source.inventory, struct {
        fn less(inventory: local.serverless_external_source_types.Inventory, left: usize, right: usize) bool {
            return std.mem.order(u8, inventory.files[left].file_id, inventory.files[right].file_id) == .lt;
        }
    }.less);
    word(&hash, order.len);
    for (order) |index| {
        const file = source.inventory.files[index];
        part(&hash, file.file_id);
        part(&hash, file.object_uri);
        part(&hash, file.etag);
        part(&hash, file.version_id);
        word(&hash, file.byte_len);
        word(&hash, file.row_count);
        optionalWord(&hash, if (file.data_sequence_number) |number| @intCast(number) else null);
        optionalWord(&hash, if (file.partition_spec_id) |number| @intCast(number) else null);
        word(&hash, file.partition_field_count);
        word(&hash, file.partition_values.len);
        for (file.partition_values) |partition| {
            part(&hash, partition.column_id);
            part(&hash, partition.string_value);
        }
    }
    // Explicit deletion vectors are part of coverage even for a reused source
    // label. Native Iceberg plans additionally pin their real object versions.
    try jsonPart(a, &hash, source.inventory.deleted_row_groups);
    var delete_versions = Hash.init(.{});
    if (source.scanner.iceberg_delete_plan) |plan| {
        for (plan.files) |file| {
            try context.ensureActive();
            try file.validate();
            const location = try local.serverless_query_lake_range_io.objectLocationForUri(file.file_path);
            var metadata = try client.statObject(location.bucket, location.key);
            defer metadata.deinit(a);
            const etag = metadata.etag orelse "";
            const version = metadata.version_id orelse "";
            if (metadata.content_length != file.file_size_in_bytes or !versionIsStrong(etag, version)) return error.InvalidExternalLakeIndexCoverage;
            try jsonPart(a, &hash, file);
            part(&hash, etag);
            part(&hash, version);
            local.serverless_query_lake_prepared_deletes.Prepared.hashObjectVersion(&delete_versions, file.file_path, etag, version);
        }
    }
    try context.ensureActive();
    return .{ .source = hash.finalResult(), .delete_objects = delete_versions.finalResult() };
}

test "external lake native index coverage rejects synthetic provider identities" {
    try std.testing.expect(!versionIsStrong("", ""));
    try std.testing.expect(!versionIsStrong("", "object-stat:v1:uri=s3://bucket/file:len=42"));
    try std.testing.expect(!versionIsStrong("", "iceberg:v1:data_seq=1:file_seq=2"));
    try std.testing.expect(versionIsStrong("etag", ""));
    try std.testing.expect(versionIsStrong("", "opaque-provider-version"));
}
