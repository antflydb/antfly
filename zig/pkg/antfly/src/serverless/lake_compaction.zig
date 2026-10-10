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

//! Native bounded compaction with exact remote intent replay.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("configured_object_store_support.zig");
const ingestion = @import("lake_ingestion.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const Attempt = struct { id: []const u8, expected: []const u8, body: []const u8, timestamp_ms: i64, result: Result = .{} };
pub const Options = struct { operation_id: []const u8, max_rows: u64 = 16384, max_bytes: u64 = 32 * 1024 * 1024, dry_run: bool = true };
pub const Result = struct { input_files: usize = 0, input_rows: u64 = 0, input_bytes: u64 = 0, output_rows: u64 = 0, scanned_rows: u64 = 0, output_files: usize = 0, committed: bool = false, complete: bool = false, conflicted: bool = false };
const Progress = struct {
    version: u16 = 2,
    operation_id: []const u8,
    parent: catalog.types.Table,
    selection: catalog.compaction.Selection,
    timestamp_ms: i64,
    file: usize = 0,
    group: u32 = 0,
    row: u64 = 0,
    outputs: []const catalog.compaction.Output = &.{},
    attempt: ?Attempt = null,
    result: Result,
};
const max_progress_bytes = 32 * 1024 * 1024;
pub fn run(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, context: catalog.types.Context, job: Options) !Result {
    if (job.operation_id.len == 0 or job.operation_id.len > 256 or job.max_rows == 0 or job.max_rows > 16384 or job.max_bytes == 0 or job.max_bytes > 32 * 1024 * 1024) return error.InvalidLakeMaintenanceLimits;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var queue = try ingestion.openQueue(a, binding, options);
    defer queue.deinit();
    const prefix = try ingestion.prefix(scratch, queue.prefix, binding, options);
    const key = try std.fmt.allocPrint(scratch, "{s}/maintenance/compact/{s}.json", .{ prefix, catalog.types.digestHex(job.operation_id) });
    var client = queue.client;
    var saved = client.getObject(queue.bucket, key, .{ .cancellation = catalog.types.contextCancellation(&context), .max_response_bytes = max_progress_bytes }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    var source_options = options;
    source_options.read_only = job.dry_run;
    var files = try configured.openBindingObjectStoreAlloc(a, binding, source_options);
    defer files.deinit();
    const destination = try ingestion.destinationFiles(scratch, files, binding.source_uri, context);
    var progress: Progress = undefined;
    if (saved) |value| {
        if (job.dry_run) return error.LakeMaintenanceAlreadyStarted;
        const document = try std.json.parseFromSliceLeaky(V, scratch, value.body, .{});
        if (document.object.get("version") == null) {
            // Earlier exact commit intents retain their original replay contract.
            const attempt = try std.json.parseFromSliceLeaky(Attempt, scratch, value.body, .{});
            var committed = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = attempt.timestamp_ms } });
            defer committed.deinit(a);
            var result = attempt.result;
            result.committed = true;
            result.complete = true;
            return result;
        }
        progress = try std.json.parseFromSliceLeaky(Progress, scratch, value.body, .{ .allocate = .alloc_always });
        if (progress.version != 2 or !std.mem.eql(u8, progress.operation_id, job.operation_id) or progress.file > progress.selection.files.len or progress.outputs.len > 65536) return error.InvalidLakeCompaction;
        if (progress.result.complete) return progress.result;
    } else {
        var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
        defer current.deinit(a);
        const selection = try catalog.compaction.selectResumable(scratch, current.table, destination);
        const result: Result = .{ .input_files = selection.files.len, .input_rows = selection.input_rows, .input_bytes = selection.input_bytes, .complete = (selection.manifests.len == 0 and !selection.rewrites_deletes) or (selection.files.len < 2 and selection.input_rows <= job.max_rows and !selection.rewrites_deletes) };
        if (job.dry_run or result.complete) return result;
        progress = .{ .operation_id = try scratch.dupe(u8, job.operation_id), .parent = .{ .metadata_location = try scratch.dupe(u8, current.table.metadata_location), .metadata_json = try scratch.dupe(u8, current.table.metadata_json) }, .selection = selection, .timestamp_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms), .result = result };
        // Fence snapshot retirement before durable admission, including a lost
        // create response or restart before the first scan turn.
        try retainParent(a, scratch, queue, binding, context, progress.parent);
        var created = client.putObject(queue.bucket, key, try encodeProgress(scratch, progress), .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => return error.LakeMaintenanceAlreadyStarted,
            else => return err,
        };
        defer created.deinit(client.allocator);
        // Initialization itself is a bounded durable turn. Reopening owns the
        // next CAS; a lost create response cannot start a different parent.
        return run(a, binding, options, context, job);
    }
    if (progress.attempt == null) {
        try retainParent(a, scratch, queue, binding, context, progress.parent);
        const root = try catalog.metadata.parse(scratch, progress.parent.metadata_json);
        const snapshot = try std.fmt.allocPrint(scratch, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(root, "current-snapshot-id"))});
        var pinned_options = options;
        pinned_options.retained_catalog_metadata = progress.parent;
        var pinned = binding;
        pinned.write_policy = .read_only;
        pinned.snapshot_mode = .{ .snapshot_id = snapshot };
        var source = try local.serverless_query_lake_serving.ServingSource.openWithContext(a, .{ .storage_mode = .relational, .external_base_source = .{ .binding = pinned, .table_id = try scratch.dupe(u8, pinned.table_id), .source_uri = try scratch.dupe(u8, pinned.source_uri), .schema_fingerprint = try scratch.dupe(u8, pinned.schema_fingerprint) } }, pinned_options.lakeOptions(), context);
        defer source.deinit();
        const schema = try catalog.row_commit.schema(scratch, root);
        const fields = (try catalog.metadata.get(schema, "fields")).array.items;
        const columns = try scratch.alloc([]const u8, fields.len);
        for (columns, fields) |*column, field| column.* = try catalog.metadata.str(try catalog.metadata.get(field, "name"));
        var live: std.ArrayList(V) = .empty;
        var decoded: usize = 0;
        var scanned: u64 = 0;
        scan: while (progress.file < progress.selection.files.len) {
            const uri = progress.selection.files[progress.file];
            const ordinal = for (source.inventory.files, 0..) |file, i| {
                if (std.mem.eql(u8, uri, file.object_uri)) break i;
            } else return error.ExternalLakeSnapshotMismatch;
            var stream = try local.serverless_query_lake_stream.Stream.init(a, &source, columns, &.{}, context, .{ .max_examined_rows = std.math.maxInt(u64), .max_decoded_bytes = 32 * 1024 * 1024, .max_input_bytes = 32 * 1024 * 1024 });
            defer stream.deinit();
            try stream.resumeFile(ordinal, progress.group, progress.row);
            while (try stream.next()) |batch| {
                const keep = try scratch.alloc(bool, batch.rowCount());
                @memset(keep, true);
                try stream.deleteMask(scratch, batch, keep);
                for (keep, batch.row_refs, 0..) |visible, reference, row| {
                    if (scanned == job.max_rows) break :scan;
                    var image: V = .{ .object = .empty };
                    var bytes: usize = 0;
                    if (visible) {
                        const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{row} };
                        for (columns, fields) |column, field| {
                            const cell = try page.cell(scratch, 0, column);
                            const cloned = try local.api_json_helpers.cloneJsonValue(scratch, try writerValue(field, cell.value));
                            bytes = try std.math.add(usize, bytes, (try std.json.Stringify.valueAlloc(scratch, cloned, .{})).len + column.len + 4);
                            try image.object.put(scratch, column, cloned);
                        }
                        if (bytes > job.max_bytes) return error.LakeWriteTooLarge;
                        if (bytes > job.max_bytes - decoded) break :scan;
                        decoded += bytes;
                        try live.append(scratch, image);
                    }
                    if (reference != .external) return error.InvalidLakeCandidateReference;
                    progress.group = reference.external.row_group_ordinal;
                    progress.row = reference.external.row_ordinal + 1;
                    scanned += 1;
                }
            }
            progress.file += 1;
            progress.group = 0;
            progress.row = 0;
        }
        progress.result.scanned_rows = try std.math.add(u64, progress.result.scanned_rows, scanned);
        if (live.items.len != 0) {
            if (progress.outputs.len == 65536) return error.LakeWriteTooLarge;
            const output = try catalog.compaction.writeOutput(scratch, progress.parent, destination, job.operation_id, progress.outputs.len, live.items);
            const appended = try scratch.alloc(catalog.compaction.Output, progress.outputs.len + 1);
            @memcpy(appended[0..progress.outputs.len], progress.outputs);
            appended[progress.outputs.len] = output;
            progress.outputs = appended;
            progress.result.output_rows += output.rows;
            progress.result.output_files = progress.outputs.len;
        }
        if (progress.file == progress.selection.files.len) {
            const prepared = try catalog.compaction.prepareOutputs(scratch, progress.parent, destination, progress.selection, progress.outputs, progress.timestamp_ms);
            progress.attempt = .{ .id = try std.fmt.allocPrint(scratch, "compact-{s}", .{catalog.types.digestHex(job.operation_id)}), .expected = progress.parent.metadata_location, .body = prepared.body, .timestamp_ms = progress.timestamp_ms, .result = progress.result };
        }
        var advanced = client.putObject(queue.bucket, key, try encodeProgress(scratch, progress), .{ .if_match_etag = saved.?.metadata.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ConditionalCheckFailed => return error.LakeMaintenanceAlreadyStarted,
            else => return err,
        };
        advanced.deinit(client.allocator);
        return if (progress.attempt != null) run(a, binding, options, context, job) else progress.result;
    }
    const attempt = progress.attempt.?;
    if (configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = attempt.timestamp_ms } })) |value| {
        var committed = value;
        committed.deinit(a);
        progress.result.committed = true;
    } else |err| switch (err) {
        // Only a definitive rejection terminates this parent. Unknown outcomes
        // retain the exact intent. A fresh job must reread and rewrite its new
        // parent, never transplant old row images into newer snapshots.
        error.LakeCommitConflict => progress.result.conflicted = true,
        else => return err,
    }

    progress.result.complete = true;
    var finished = try client.putObject(queue.bucket, key, try encodeProgress(scratch, progress), .{ .if_match_etag = saved.?.metadata.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&context) });
    finished.deinit(client.allocator);
    return progress.result;
}
fn retainParent(a: A, scratch: A, queue: local.serverless_object_store_support.OpenedObjectStore, binding: local.serverless_external_source_catalog_binding.Binding, context: catalog.types.Context, parent: catalog.types.Table) !void {
    const root = try catalog.metadata.parse(scratch, parent.metadata_json);
    const snapshot = try std.fmt.allocPrint(scratch, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(root, "current-snapshot-id"))});
    const pins = @import("lake_snapshot_pins.zig");
    const pin_store: pins.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try pins.namespace(scratch, queue.prefix, binding, try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"))), .context = context };
    _ = try pin_store.acquireFor(a, snapshot, @import("antfly_platform").time.realtimeNs(), std.time.ns_per_day);
}
fn encodeProgress(a: A, progress: Progress) ![]const u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
    if (bytes.len > max_progress_bytes) return error.LakeWriteTooLarge;
    return bytes;
}

test "external lake compaction resumable coordinator enforces per-turn admission" {
    const binding: local.serverless_external_source_catalog_binding.Binding = .{ .table_id = "t", .format = .iceberg, .source_uri = "s3://bucket/table", .schema_fingerprint = "schema" };
    try std.testing.expectError(error.InvalidLakeMaintenanceLimits, run(std.testing.allocator, binding, .{}, .{}, .{ .operation_id = "" }));
    try std.testing.expectError(error.InvalidLakeMaintenanceLimits, run(std.testing.allocator, binding, .{}, .{}, .{ .operation_id = "job", .max_rows = 0 }));
}

// The scanner exposes timestamp nanoseconds; Iceberg's writer consumes micros.
fn writerValue(field: V, value: V) !V {
    const kind = try catalog.metadata.str(try catalog.metadata.get(field, "type"));
    if (value == .integer and (std.mem.eql(u8, kind, "timestamp") or std.mem.eql(u8, kind, "timestamptz"))) {
        if (@rem(value.integer, std.time.ns_per_us) != 0) return error.LakeTimestampPrecisionLoss;
        return .{ .integer = @divExact(value.integer, std.time.ns_per_us) };
    }
    return value;
}

test "external lake compaction preserves timestamp units and signed microsecond precision" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "timestamp", "timestamptz" }) |kind| {
        var field: V = .{ .object = .empty };
        try field.object.put(a, "type", .{ .string = kind });
        for ([_]i64{ -1000001000, 0, 1000001000 }) |ns| {
            const value = try writerValue(field, .{ .integer = ns });
            try std.testing.expectEqual(ns, value.integer * std.time.ns_per_us);
        }
        try std.testing.expect((try writerValue(field, .null)) == .null);
        try std.testing.expectError(error.LakeTimestampPrecisionLoss, writerValue(field, .{ .integer = 1 }));
    }
    var field: V = .{ .object = .empty };
    try field.object.put(a, "type", .{ .string = "long" });
    try std.testing.expectEqual(@as(i64, 1000001000), (try writerValue(field, .{ .integer = 1000001000 })).integer);
}
