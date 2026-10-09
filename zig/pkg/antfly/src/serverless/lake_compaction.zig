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
pub const Result = struct { input_files: usize = 0, input_rows: u64 = 0, input_bytes: u64 = 0, output_rows: usize = 0, committed: bool = false };
pub fn run(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, context: catalog.types.Context, job: Options) !Result {
    if (job.operation_id.len == 0 or job.operation_id.len > 256) return error.InvalidLakeMaintenanceLimits;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var queue = try ingestion.openQueue(a, binding, options);
    defer queue.deinit();
    const prefix = try ingestion.prefix(scratch, queue.prefix, binding, options);
    const key = try std.fmt.allocPrint(scratch, "{s}/maintenance/compact/{s}.json", .{ prefix, catalog.types.digestHex(job.operation_id) });
    var client = queue.client;
    var saved = client.getObject(queue.bucket, key, .{ .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    if (saved) |value| {
        if (job.dry_run) return error.LakeMaintenanceAlreadyStarted;
        const attempt = try std.json.parseFromSliceLeaky(Attempt, scratch, value.body, .{});
        var result = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = attempt.timestamp_ms } });
        defer result.deinit(a);
        var replay = attempt.result;
        replay.committed = true;
        return replay;
    }
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    var source_options = options;
    source_options.read_only = job.dry_run;
    var files = try configured.openBindingObjectStoreAlloc(a, binding, source_options);
    defer files.deinit();
    const destination: catalog.row_commit.Files = .{ .client = files.client, .bucket = files.bucket, .prefix = files.prefix, .uri = binding.source_uri, .context = context };
    const selection = try catalog.compaction.select(scratch, current.table, destination, job.max_rows, job.max_bytes);
    var result: Result = .{ .input_files = selection.files.len, .input_rows = selection.input_rows, .input_bytes = selection.input_bytes };
    if (job.dry_run or (selection.manifests.len == 0 and !selection.rewrites_deletes)) return result;
    if (selection.files.len < 2 and !selection.rewrites_deletes) return result;
    // Bind the scan to exactly the selected parent, even if an external writer
    // commits while rows are decoded. Only the guarded catalog commit publishes.
    const root = try catalog.metadata.parse(scratch, current.table.metadata_json);
    var pinned = binding;
    pinned.write_policy = .read_only;
    pinned.snapshot_mode = .{ .snapshot_id = try std.fmt.allocPrint(scratch, "{d}", .{try catalog.metadata.int(try catalog.metadata.get(root, "current-snapshot-id"))}) };
    var source = try local.serverless_query_lake_serving.ServingSource.openWithContext(a, .{ .storage_mode = .relational, .external_base_source = .{ .binding = pinned, .table_id = try scratch.dupe(u8, pinned.table_id), .source_uri = try scratch.dupe(u8, pinned.source_uri), .schema_fingerprint = try scratch.dupe(u8, pinned.schema_fingerprint) } }, options.lakeOptions(), context);
    defer source.deinit();
    const schema = try catalog.row_commit.schema(scratch, root);
    const fields = (try catalog.metadata.get(schema, "fields")).array.items;
    const columns = try scratch.alloc([]const u8, fields.len);
    for (columns, fields) |*column, field| column.* = try catalog.metadata.str(try catalog.metadata.get(field, "name"));
    var live: std.ArrayList(V) = .empty;
    var decoded: usize = 0;
    for (source.inventory.files, 0..) |file, ordinal| {
        const chosen = for (selection.files) |path| {
            if (std.mem.eql(u8, path, file.object_uri)) break true;
        } else false;
        if (!chosen) continue;
        var stream = try local.serverless_query_lake_stream.Stream.init(a, &source, columns, &.{}, context, .{ .max_examined_rows = job.max_rows, .max_decoded_bytes = @intCast(job.max_bytes), .max_input_bytes = @intCast(job.max_bytes) });
        defer stream.deinit();
        try stream.restrictFile(ordinal);
        while (try stream.next()) |batch| {
            const keep = try scratch.alloc(bool, batch.rowCount());
            @memset(keep, true);
            try stream.deleteMask(scratch, batch, keep);
            for (keep, 0..) |visible, row| {
                if (!visible) continue;
                if (live.items.len >= job.max_rows) return error.LakeWriteTooLarge;
                const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{row} };
                var image: V = .{ .object = .empty };
                for (columns) |column| {
                    const cell = try page.cell(scratch, 0, column);
                    if (cell.value == .string) decoded = try std.math.add(usize, decoded, cell.value.string.len);
                    decoded = try std.math.add(usize, decoded, 32);
                    if (decoded > 32 * 1024 * 1024) return error.LakeWriteTooLarge;
                    try image.object.put(scratch, column, try local.api_json_helpers.cloneJsonValue(scratch, cell.value));
                }
                try live.append(scratch, image);
            }
        }
    }
    const timestamp: i64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const prepared = try catalog.compaction.prepare(scratch, current.table, destination, selection, live.items, timestamp);
    result.output_rows = live.items.len;
    const attempt: Attempt = .{ .result = result, .id = try std.fmt.allocPrint(scratch, "compact-{s}", .{catalog.types.digestHex(job.operation_id)}), .expected = current.table.metadata_location, .body = prepared.body, .timestamp_ms = timestamp };
    const bytes = try std.json.Stringify.valueAlloc(scratch, attempt, .{});
    var stored = client.putObject(queue.bucket, key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => return error.LakeMaintenanceAlreadyStarted,
        else => return err,
    };
    defer stored.deinit(client.allocator);
    var committed = try configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = timestamp } });
    defer committed.deinit(a);
    result.committed = true;
    return result;
}
