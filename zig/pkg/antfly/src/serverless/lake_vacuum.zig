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

//! Expire guarded snapshots, then collect only proved native-owned unreachable
//! objects. Saved jobs survive lost commits and interrupted bounded sweeps.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("configured_object_store_support.zig");
const ingestion = @import("lake_ingestion.zig");
const pins = @import("lake_snapshot_pins.zig");
const iceberg = local.serverless_external_source_mod.iceberg_avro;
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Options = struct { operation_id: []const u8, dry_run: bool = true, exclusive_ownership: bool = false, retain_ms: u64 = 7 * 24 * 60 * 60 * 1000, keep_latest: usize = 2, max_deleted: usize = 4096 };
pub const Result = struct { expired_snapshots: usize = 0, eligible_objects: usize = 0, deleted_objects: usize = 0, retained_objects: usize = 0, complete: bool = false };
const Retired = struct { snapshot: []const u8, objects: []const []const u8 };
const Job = struct { id: []const u8, expected: []const u8, body: []const u8, timestamp_ms: i64, retired: []const Retired };
const Marker = struct { uri: []const u8, sha256: []const u8, etag: ?[]const u8, owner: []const u8 };
fn has(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| if (std.mem.eql(u8, candidate, value)) return true;
    return false;
}
fn collectSnapshot(a: A, files: catalog.row_commit.Files, snapshot: V, budget: *u64) ![]const []const u8 {
    var paths: std.StringHashMapUnmanaged(void) = .empty;
    const list_uri = try catalog.metadata.str(try catalog.metadata.get(snapshot, "manifest-list"));
    const bytes = try catalog.row_commit.readLimited(a, files, list_uri, @intCast(@min(budget.*, 16 * 1024 * 1024)));
    if (bytes.len > budget.*) return error.LakeVacuumBudgetExceeded;
    budget.* -= bytes.len;
    try paths.put(a, list_uri, {});
    const list = try iceberg.parseManifestListAlloc(a, bytes);
    for (list.entries) |entry| {
        try files.context.ensureActive();
        if (entry.manifest_length > budget.*) return error.LakeVacuumBudgetExceeded;
        const manifest_bytes = try catalog.row_commit.readLimited(a, files, entry.manifest_path, @intCast(@min(budget.*, 16 * 1024 * 1024)));
        if (manifest_bytes.len > budget.*) return error.LakeVacuumBudgetExceeded;
        budget.* -= manifest_bytes.len;
        try paths.put(a, entry.manifest_path, {});
        const manifest = try iceberg.parseDataManifestAlloc(a, manifest_bytes);
        for (manifest.entries) |file| if (file.status != .deleted) {
            if (paths.count() == 262144) return error.LakeVacuumBudgetExceeded;
            try paths.put(a, file.file_path, {});
        };
    }
    const result = try a.alloc([]const u8, paths.count());
    var iterator = paths.keyIterator();
    for (result) |*path| path.* = iterator.next().?.*;
    return result;
}
fn retainedIds(a: A, root: V, extra: []const []const u8, keep_latest: usize, before: i64) ![]const []const u8 {
    var retained: std.ArrayList([]const u8) = .empty;
    try retained.appendSlice(a, extra);
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    const refs = try catalog.metadata.get(root, "refs");
    var refs_iterator = refs.object.iterator();
    while (refs_iterator.next()) |ref| try retained.append(a, try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(ref.value_ptr.*, "snapshot-id"))}));
    // Iceberg snapshots append in commit order. Every named ref is protected
    // independently; the newest retained window additionally protects history.
    for (snapshots, 0..) |snapshot, index| {
        if (snapshots.len - index <= keep_latest or try catalog.metadata.int(try catalog.metadata.get(snapshot, "timestamp-ms")) >= before) {
            try retained.append(a, try std.fmt.allocPrint(a, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))}));
        }
    }
    return retained.items;
}
pub fn run(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, context: catalog.types.Context, request: Options, protected: []const []const u8) !Result {
    if (request.operation_id.len == 0 or request.operation_id.len > 256 or request.retain_ms < 10 * 60 * 1000 or request.keep_latest == 0 or request.keep_latest > 1024 or request.max_deleted == 0 or request.max_deleted > 4096) return error.InvalidLakeMaintenanceLimits;
    if (!request.dry_run and !request.exclusive_ownership) return error.LakeVacuumOwnershipRequired;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var queue = try ingestion.openQueue(a, binding, options);
    defer queue.deinit();
    const base = try ingestion.prefix(scratch, queue.prefix, binding, options);
    const job_key = try std.fmt.allocPrint(scratch, "{s}/maintenance/vacuum/{s}.json", .{ base, catalog.types.digestHex(request.operation_id) });
    var queue_client = queue.client;
    var saved = queue_client.getObject(queue.bucket, job_key, .{ .cancellation = catalog.types.contextCancellation(&context), .max_response_bytes = catalog.types.max_commit_bytes }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(queue_client.allocator);
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const root = try catalog.metadata.parse(scratch, current.table.metadata_json);
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    const pin_store: pins.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try pins.namespace(scratch, queue.prefix, binding, uuid), .context = context };
    var file_options = options;
    file_options.read_only = request.dry_run;
    var files = try configured.openBindingObjectStoreAlloc(a, binding, file_options);
    defer files.deinit();
    const destination: catalog.row_commit.Files = .{ .client = files.client, .bucket = files.bucket, .prefix = files.prefix, .uri = binding.source_uri, .context = context };
    var budget: u64 = 256 * 1024 * 1024;
    var job: Job = undefined;
    var result: Result = .{};
    if (saved) |value| {
        if (request.dry_run) return error.LakeMaintenanceAlreadyStarted;
        job = try std.json.parseFromSliceLeaky(Job, scratch, value.body, .{ .allocate = .alloc_always });
    } else {
        const now_ns = @import("antfly_platform").time.realtimeNs();
        const now_ms: i64 = @intCast(now_ns / std.time.ns_per_ms);
        const before = now_ms -| (std.math.cast(i64, request.retain_ms) orelse return error.InvalidLakeMaintenanceLimits);
        const keep = try retainedIds(scratch, root, protected, request.keep_latest, before);
        var retired: std.ArrayList(Retired) = .empty;
        var ids: std.ArrayList(i64) = .empty;
        for ((try catalog.metadata.get(root, "snapshots")).array.items) |snapshot| {
            const id = try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"));
            const text = try std.fmt.allocPrint(scratch, "{d}", .{id});
            if (has(keep, text) or (try pin_store.deadline(a, text)) +| pins.grace_ns >= now_ns) continue;
            try retired.append(scratch, .{ .snapshot = text, .objects = try collectSnapshot(scratch, destination, snapshot, &budget) });
            try ids.append(scratch, id);
        }
        result.expired_snapshots = ids.items.len;
        if (ids.items.len == 0) {
            result.complete = true;
            return result;
        }
        const parent = try catalog.metadata.get(root, "current-snapshot-id");
        const body = try std.json.Stringify.valueAlloc(scratch, .{ .requirements = .{.{ .type = "assert-ref-snapshot-id", .ref = "main", .@"snapshot-id" = parent }}, .updates = .{.{ .action = "remove-snapshots", .@"snapshot-ids" = ids.items }} }, .{});
        job = .{ .id = try std.fmt.allocPrint(scratch, "vacuum-{s}", .{catalog.types.digestHex(request.operation_id)}), .expected = current.table.metadata_location, .body = body, .timestamp_ms = now_ms, .retired = retired.items };
        if (!request.dry_run) {
            const bytes = try std.json.Stringify.valueAlloc(scratch, job, .{});
            if (bytes.len > catalog.types.max_commit_bytes) return error.LakeVacuumBudgetExceeded;
            var stored = try queue_client.putObject(queue.bucket, job_key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) });
            stored.deinit(queue_client.allocator);
        }
    }
    // Snapshot removal precedes retirement. Concurrent refs/commits must win a
    // catalog CAS; a failed job never deletes files or blindly rebases its intent.
    if (!request.dry_run) {
        var committed = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = job.id, .expected_metadata_location = job.expected, .body = job.body, .timestamp_ms = job.timestamp_ms } });
        defer committed.deinit(a);
    }
    // Re-read authority after resolving the intent. A newer safe publication
    // can retain old native objects, and the mark set must include its roots.
    var latest = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer latest.deinit(a);
    const live_root = try catalog.metadata.parse(scratch, latest.table.metadata_json);
    var marked: std.StringHashMapUnmanaged(void) = .empty;
    for ((try catalog.metadata.get(live_root, "snapshots")).array.items) |snapshot| {
        const id = try std.fmt.allocPrint(scratch, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"))});
        const expired = for (job.retired) |old| {
            if (std.mem.eql(u8, old.snapshot, id)) break true;
        } else false;
        if (request.dry_run and expired) continue;
        for (try collectSnapshot(scratch, destination, snapshot, &budget)) |uri| try marked.put(scratch, uri, {});
    }
    // Finish the full reader mark phase before deleting any shared object.
    // A late pin on one expired snapshot protects files shared by other expired
    // snapshots too; processing snapshots one by one would violate that rule.
    var blocked: std.StringHashMapUnmanaged(void) = .empty;
    for (job.retired) |retired| {
        const safe = !has(protected, retired.snapshot) and (request.dry_run or try pin_store.retire(a, retired.snapshot, @import("antfly_platform").time.realtimeNs()));
        if (!safe) {
            try blocked.put(scratch, retired.snapshot, {});
            for (retired.objects) |uri| try marked.put(scratch, uri, {});
        }
    }
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    result.expired_snapshots = job.retired.len;
    result.complete = true;
    for (job.retired) |retired| {
        if (blocked.contains(retired.snapshot)) {
            result.complete = false;
            continue;
        }
        for (retired.objects) |uri| {
            try context.ensureActive();
            if ((try seen.getOrPut(scratch, uri)).found_existing) continue;
            if (marked.contains(uri)) {
                result.retained_objects += 1;
                continue;
            }
            const at_limit = result.deleted_objects == request.max_deleted;
            switch (try collectOwned(scratch, destination, uri, request.dry_run or at_limit)) {
                .retained => result.retained_objects += 1,
                .eligible => {
                    result.eligible_objects += 1;
                    if (!request.dry_run) result.complete = false;
                },
                .deleted => {
                    result.eligible_objects += 1;
                    result.deleted_objects += 1;
                },
            }
        }
    }
    return result;
}

test "external lake vacuum retains refs newest snapshots recent commits and reader roots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a,
        \\{"refs":{"main":{"snapshot-id":5},"history":{"snapshot-id":1}},"snapshots":[{"snapshot-id":1,"timestamp-ms":1},{"snapshot-id":2,"timestamp-ms":2},{"snapshot-id":3,"timestamp-ms":3},{"snapshot-id":4,"timestamp-ms":100},{"snapshot-id":5,"timestamp-ms":5}]}
    , .{});
    const retained = try retainedIds(a, root, &.{"3"}, 1, 90);
    try std.testing.expect(has(retained, "1"));
    try std.testing.expect(!has(retained, "2"));
    try std.testing.expect(has(retained, "3"));
    try std.testing.expect(has(retained, "4"));
    try std.testing.expect(has(retained, "5"));
}

const Collection = enum { retained, eligible, deleted };
fn collectOwned(a: A, files: catalog.row_commit.Files, uri: []const u8, dry_run: bool) !Collection {
    var client = files.client;
    const table_prefix = try std.fmt.allocPrint(a, "{s}/", .{std.mem.trimEnd(u8, files.uri, "/")});
    if (!std.mem.startsWith(u8, uri, table_prefix) or std.mem.indexOf(u8, uri[table_prefix.len..], "..") != null) {
        return .retained;
    }
    const marker_key = try std.fmt.allocPrint(a, "{s}/.antfly-owned/{s}.json", .{ files.prefix, catalog.types.digestHex(uri) });
    var marker = client.getObject(files.bucket, marker_key, .{ .cancellation = catalog.types.contextCancellation(&files.context), .max_response_bytes = 4096 }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {
            return .retained;
        },
        else => return err,
    };
    defer marker.deinit(client.allocator);
    const proof = try std.json.parseFromSliceLeaky(Marker, a, marker.body, .{});
    if (!std.mem.eql(u8, proof.owner, "antfly-native-lake-v1") or !std.mem.eql(u8, proof.uri, uri) or proof.etag == null or proof.sha256.len != 64) return error.LakeArtifactIdentityConflict;
    if (dry_run) return .eligible;
    const key = try std.fmt.allocPrint(a, "{s}/{s}", .{ files.prefix, uri[table_prefix.len..] });
    client.deleteObject(files.bucket, key, .{ .if_match_etag = proof.etag, .cancellation = catalog.types.contextCancellation(&files.context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
    // Removal of the ownership proof is conditional as well. A retry
    // after a lost delete response observes absence and stays safe.
    try client.deleteObject(files.bucket, marker_key, .{ .if_match_etag = marker.metadata.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&files.context) });
    return .deleted;
}

test "external lake vacuum deletes only owned unchanged objects and replays absence" {
    const objectstore = @import("objectstore");
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const files: catalog.row_commit.Files = .{ .client = memory.client(), .bucket = "archive", .prefix = "hn", .uri = "s3://archive/hn" };
    const uri = try catalog.row_commit.upload(a, files, "data/owned.parquet", "native-content");
    try std.testing.expectEqual(Collection.eligible, try collectOwned(a, files, uri, true));
    try std.testing.expectEqual(Collection.deleted, try collectOwned(a, files, uri, false));
    try std.testing.expectEqual(Collection.retained, try collectOwned(a, files, uri, false));
    var client = files.client;
    var external = try client.putObject(files.bucket, "hn/data/external.parquet", "external", .{});
    external.deinit(alloc);
    try std.testing.expectEqual(Collection.retained, try collectOwned(a, files, "s3://archive/hn/data/external.parquet", false));
    const replaced_uri = try catalog.row_commit.upload(a, files, "data/replaced.parquet", "old-content");
    var changed = try client.putObject(files.bucket, "hn/data/replaced.parquet", "new-content", .{});
    changed.deinit(alloc);
    try std.testing.expectError(error.PreconditionFailed, collectOwned(a, files, replaced_uri, false));
    var remaining = try client.getObject(files.bucket, "hn/data/replaced.parquet", .{});
    defer remaining.deinit(alloc);
    try std.testing.expectEqualStrings("new-content", remaining.body);
}
