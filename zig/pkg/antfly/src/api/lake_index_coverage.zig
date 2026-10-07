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
const ObjectStat = struct {
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
pub const Coverage = struct { source: [32]u8, delete_objects: [32]u8 };
pub fn pin(source: *Source, context: Context) !Coverage {
    return pinWithInventory(source, context, null);
}
/// Hints must come from an authenticated publication in the currently authorized
/// store/credential scope. Only an explicit immutable-data contract permits reuse.
pub const FileMap = std.StringHashMapUnmanaged(*const local.serverless_external_source_types.FileEntry);
pub fn pinWithInventory(source: *Source, context: Context, previous: ?local.serverless_external_source_types.Inventory) !Coverage {
    return pinWithFileMap(source, context, previous, null);
}
pub fn pinWithFileMap(source: *Source, context: Context, previous: ?local.serverless_external_source_types.Inventory, verified_files: ?*const FileMap) !Coverage {
    const hash = try pinData(source, context, previous, verified_files);
    return finish(source, context, hash);
}
// An authenticated immutable snapshot can reuse its canonical data proof;
// delete-object evidence remains fresh for every selection.
pub const DataProof = Hash;
pub fn pinData(source: *Source, context: Context, previous: ?local.serverless_external_source_types.Inventory, verified_files: ?*const FileMap) !DataProof {
    try source.ownInventory();
    const a = source.alloc;
    if (source.immutable_objects) if (previous) |inventory| {
        var owned_files: FileMap = .empty;
        defer owned_files.deinit(a);
        if (verified_files == null) {
            try owned_files.ensureTotalCapacity(a, @intCast(inventory.files.len));
            for (inventory.files) |*file| owned_files.putAssumeCapacity(file.file_id, file);
        }
        const by_id = verified_files orelse &owned_files;
        for (source.inventory.files, 0..) |*file, index| {
            const stored = by_id.get(file.file_id) orelse continue;
            if (!std.mem.eql(u8, file.object_uri, stored.object_uri) or file.byte_len != stored.byte_len or !versionIsStrong(stored.etag, stored.version_id)) continue;
            // Never replace conflicting fresh provider evidence with a hint.
            if (versionIsStrong(file.etag, file.version_id) and
                (!std.mem.eql(u8, file.etag, stored.etag) or !std.mem.eql(u8, file.version_id, stored.version_id))) continue;
            const etag = try a.dupe(u8, stored.etag);
            errdefer a.free(etag);
            const version = try a.dupe(u8, stored.version_id);
            if (file.etag.len != 0) a.free(file.etag);
            if (file.version_id.len != 0) a.free(file.version_id);
            file.etag = etag;
            file.version_id = version;
            source.pinned_files[index] = true;
        }
    };
    var client = source.scanner.object_reader.client;
    client.allocator = a;
    // Fresh provider evidence is mandatory; bounded shared scheduling reduces
    // network latency without treating a TTL or cached bytes as authority.
    const Work = ObjectStat;
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
            const index = begin + slot;
            const file = source.inventory.files[index];
            if (source.immutable_objects and (!source.lazy_versions or source.pinned_files[index]) and versionIsStrong(file.etag, file.version_id)) continue;
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
            const metadata = optional orelse continue;
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
                if (source.pinned_files.len != 0) source.pinned_files[index] = true;
                continue;
            }
            const pinned_etag = try a.dupe(u8, etag);
            errdefer a.free(pinned_etag);
            const pinned_version = try a.dupe(u8, version);
            if (file.etag.len != 0) a.free(file.etag);
            if (file.version_id.len != 0) a.free(file.version_id);
            file.etag = pinned_etag;
            file.version_id = pinned_version;
            if (source.pinned_files.len != 0) source.pinned_files[index] = true;
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
    return hash;
}
pub fn finish(source: *Source, context: Context, data: DataProof) !Coverage {
    const a = source.alloc;
    var hash = data;
    var client = source.scanner.object_reader.client;
    client.allocator = a;
    const Work = ObjectStat;
    var delete_versions = Hash.init(.{});
    if (source.scanner.iceberg_delete_plan) |plan| {
        var first: usize = 0;
        while (first < plan.files.len) {
            const count = @min(8, plan.files.len - first);
            var tasks: [8]?local.sql_parallel_scheduler.Task(anyerror!local.storage_object_storage.ObjectMetadata) = @splat(null);
            var results: [8]?local.storage_object_storage.ObjectMetadata = @splat(null);
            defer {
                for (&tasks, &results) |*task, *result| if (task.*) |*future| {
                    result.* = future.cancel(context.io.?) catch null;
                };
                for (&results) |*result| if (result.*) |*metadata| metadata.deinit(std.heap.page_allocator);
            }
            for (0..count) |slot| {
                const file = plan.files[first + slot];
                try file.validate();
                const work: Work = .{ .client = client, .uri = file.file_path, .context = context };
                if (context.io) |io| tasks[slot] = local.sql_parallel_scheduler.global().submitTransient(io, 64 * 1024, Work.stat, .{work});
                if (tasks[slot] == null) results[slot] = try work.stat();
            }
            for (0..count) |slot| if (tasks[slot]) |*task| {
                const result = task.await(context.io.?);
                tasks[slot] = null;
                results[slot] = try result;
            };
            // Hash in manifest order, independently of completion order.
            for (0..count) |slot| {
                try context.ensureActive();
                const file = plan.files[first + slot];
                const metadata = results[slot].?;
                const etag = metadata.etag orelse "";
                const version = metadata.version_id orelse "";
                if (metadata.content_length != file.file_size_in_bytes or !versionIsStrong(etag, version)) return error.InvalidExternalLakeIndexCoverage;
                try jsonPart(a, &hash, file);
                part(&hash, etag);
                part(&hash, version);
                local.serverless_query_lake_prepared_deletes.Prepared.hashObjectVersion(&delete_versions, file.file_path, etag, version);
            }
            first += count;
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

test "external lake immutable object contract avoids data HEAD while mutable coverage revalidates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("immutable-coverage-heads");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const payload = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{1} }});
    defer a.free(payload);
    var uploaded = try client.putObject("antfly", "part.parquet", payload, .{});
    uploaded.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try Source.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const Spy = struct {
        backing: local.storage_object_storage.ObjectStorage,
        heads: std.atomic.Value(usize) = .init(0),
        gated: bool = false,
        both: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        fn stat(raw: *anyopaque, alloc: A, bucket: []const u8, key: []const u8) !local.storage_object_storage.ObjectMetadata {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const count = self.heads.fetchAdd(1, .acq_rel) + 1;
            if (self.gated) {
                if (count == 2) self.both.set(std.testing.io);
                try self.gate.wait(std.testing.io);
            }
            var backing = self.backing;
            backing.allocator = alloc;
            return backing.statObject(bucket, key);
        }
    };
    const original = source.scanner.object_reader.client;
    defer source.scanner.object_reader.client = original;
    var spy: Spy = .{ .backing = original };
    var vtable = original.vtable.*;
    vtable.stat_object = Spy.stat;
    vtable.stat_object_with_options = null;
    source.scanner.object_reader.client.ptr = &spy;
    source.scanner.object_reader.client.vtable = &vtable;
    const mutable = try pin(&source, .{ .io = std.testing.io });
    try std.testing.expectEqual(@as(usize, 1), spy.heads.load(.monotonic));
    source.immutable_objects = true;
    spy.heads.store(0, .monotonic);
    const immutable = try pin(&source, .{ .io = std.testing.io });
    try std.testing.expectEqual(@as(usize, 0), spy.heads.load(.monotonic));
    try std.testing.expectEqual(mutable, immutable);
    source.immutable_objects = false;
    _ = try pin(&source, .{ .io = std.testing.io });
    try std.testing.expectEqual(@as(usize, 1), spy.heads.load(.monotonic));

    // Delete objects always need fresh identities, even with immutable data.
    // Gate both HEADs to prove overlap, then compare with serial hashing.
    source.immutable_objects = true;
    var delete_files: [2]local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile = undefined;
    for (&delete_files, [_][]const u8{ "delete-one.parquet", "delete-two.parquet" }) |*file, key| {
        var result = try client.putObject("antfly", key, payload, .{});
        result.deinit(a);
        file.* = .{ .content = .position_deletes, .file_path = try std.fmt.allocPrint(a, "s3://antfly/{s}", .{key}), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 1, .file_sequence_number = 1, .record_count = 1, .file_size_in_bytes = payload.len };
    }
    defer for (delete_files) |file| a.free(file.file_path);
    source.scanner.iceberg_delete_plan = .{ .files = &delete_files };
    defer source.scanner.iceberg_delete_plan = null;
    spy.heads.store(0, .release);
    spy.gated = true;
    const Worker = struct {
        fn run(value: *Source) anyerror!Coverage {
            return pin(value, .{ .io = std.testing.io });
        }
    };
    var future = try std.testing.io.concurrent(Worker.run, .{&source});
    defer _ = future.cancel(std.testing.io) catch {};
    defer spy.gate.set(std.testing.io);
    try spy.both.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } });
    spy.gate.set(std.testing.io);
    const parallel = try future.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 2), spy.heads.load(.acquire));
    spy.gated = false;
    const serial = try pin(&source, .{});
    try std.testing.expectEqual(parallel, serial);
    try std.testing.expectEqual(@as(usize, 4), spy.heads.load(.acquire));
}
