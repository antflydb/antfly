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

//! Durable provider-neutral CDC admission and native WAL-to-Iceberg drain.
//! Queue ownership is independent of a caller's lifetime. Source checkpoints
//! advance with the catalog commit, never with uploads or an in-memory wakeup.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("configured_object_store_support.zig");
const wal = @import("lake_wal.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Batch = catalog.row_commit.Batch;
const Binding = local.serverless_external_source_catalog_binding.Binding;
const Options = configured.BindingObjectStoreOpenOptions;
const Context = catalog.types.Context;
const Attempt = struct { id: []const u8, expected: []const u8, body: []const u8, timestamp_ms: i64 };
pub fn openQueue(a: A, binding: Binding, options: Options) !local.serverless_object_store_support.OpenedObjectStore {
    if (binding.write_policy != .iceberg_writer or binding.catalog == null) return error.ExternalLakeReadOnly;
    const config = options.node_config orelse return error.NativeArtifactStorageRequired;
    if (config.storage.artifacts.connection == null) {
        if (config.deployment_mode != .standalone and config.deployment_mode != .embedded) return error.NativeArtifactStorageRequired;
        const base = config.storage.local_base_dir orelse
            (if (config.storage.lite_path) |path| std.fs.path.dirname(path) orelse "." else return error.NativeArtifactStorageRequired);
        const root = try std.fs.path.join(a, &.{ base, "artifacts" });
        defer a.free(root);
        const uri = try std.fmt.allocPrint(a, "file://{s}", .{root});
        defer a.free(uri);
        return local.serverless_object_store_support.OpenedObjectStore.initFileUriWithOptions(a, uri, "native-lake-indexes", .{ .ensure_bucket = true });
    }
    return configured.openNativeArtifactObjectStoreAlloc(a, config, options.secret_store, false);
}
pub fn prefix(a: A, base: []const u8, binding: Binding, options: Options) ![]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ .source = binding.source_uri, .catalog = binding.catalog }, .{});
    defer a.free(identity);
    return std.fmt.allocPrint(a, "{s}{s}lake-ingestion/{d}/{d}/{s}", .{ base, if (base.len == 0) "" else "/", options.catalog_table_id, options.catalog_generation, catalog.types.digestHex(identity) });
}
pub fn coverage(a: A, table: catalog.types.Table) !u64 {
    var p = try std.json.parseFromSlice(V, a, table.metadata_json, .{});
    defer p.deinit();
    const properties = try catalog.metadata.get(p.value, "properties");
    const value = properties.object.get("antfly.wal.coverage") orelse return 0;
    return std.fmt.parseInt(u64, try catalog.metadata.str(value), 10);
}
fn matchesPrevious(previous: Batch, batch: Batch) bool {
    return std.mem.eql(u8, previous.source, batch.source) and std.mem.eql(u8, previous.epoch, batch.epoch) and batch.expected_checkpoint != null and std.mem.eql(u8, previous.checkpoint, batch.expected_checkpoint.?);
}
/// The caller must supply a stable complete transaction and opaque source
/// checkpoint. A stale/out-of-order predecessor is rejected before admission.
/// Competing sources require an explicit conflict policy; this version owns
/// one source epoch and key definition per table and rejects cross-source use.
pub fn accept(a: A, binding: Binding, options: Options, context: Context, body: []const u8) !u64 {
    if (body.len == 0 or body.len > catalog.types.max_commit_bytes) return error.LakeWriteTooLarge;
    var parsed = try std.json.parseFromSlice(Batch, a, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const batch = parsed.value;
    // Resolve current authority and validate full images before owning a batch.
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    try catalog.row_commit.validate(a, current.table.metadata_json, batch, context);
    const payload = try std.json.Stringify.valueAlloc(a, batch, .{});
    defer a.free(payload);
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    var queue_arena = std.heap.ArenaAllocator.init(a);
    defer queue_arena.deinit();
    const scratch = queue_arena.allocator();
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context };
    const last = try store.latest(scratch);
    const latest = if (last) |record| record.lsn else 0;
    const identity = try std.json.Stringify.valueAlloc(scratch, .{ batch.source, batch.epoch, batch.batch_id }, .{});
    const operation_id = &catalog.types.digestHex(identity);
    if (try store.find(scratch, operation_id, payload)) |accepted| return accepted;
    if (last) |record| {
        var previous = try std.json.parseFromSlice(Batch, a, record.payload, .{});
        defer previous.deinit();
        if (!matchesPrevious(previous.value, batch) or !keyFieldsEqual(previous.value.key_fields, batch.key_fields)) return error.LakeCheckpointConflict;
    } else {
        var metadata = try std.json.parseFromSlice(V, a, current.table.metadata_json, .{});
        defer metadata.deinit();
        const props = (try catalog.metadata.get(metadata.value, "properties")).object;
        const checkpoint = props.get("antfly.cdc.checkpoint");
        if ((checkpoint == null) != (batch.expected_checkpoint == null)) return error.LakeCheckpointConflict;
        if (checkpoint) |v| if (!std.mem.eql(u8, try catalog.metadata.str(v), batch.expected_checkpoint.?)) return error.LakeCheckpointConflict;
        if (props.get("antfly.cdc.source")) |source| {
            const key_definition = try std.json.Stringify.valueAlloc(scratch, batch.key_fields, .{});
            if (!std.mem.eql(u8, try catalog.metadata.str(source), batch.source) or
                !std.mem.eql(u8, try catalog.metadata.str(props.get("antfly.cdc.epoch").?), batch.epoch) or
                !std.mem.eql(u8, try catalog.metadata.str(props.get("antfly.cdc.key-fields").?), key_definition)) return error.LakeSourceConflict;
        }
    }
    try context.ensureActive();
    return store.append(scratch, operation_id, payload, latest, try coverage(a, current.table));
}
fn keyFieldsEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| if (!std.mem.eql(u8, l, r)) return false;
    return true;
}
/// One bounded transaction per worker pass. Exact commit intents survive a
/// restart/timeout. A confirmed conflict alone allows a new pinned attempt.
pub fn drain(a: A, binding: Binding, options: Options, context: Context) !bool {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context };
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const cut = try coverage(a, current.table);
    const record = (try store.next(scratch, cut)) orelse return false;
    var parsed = try std.json.parseFromSlice(Batch, a, record.payload, .{});
    defer parsed.deinit();
    const intent_key = try std.fmt.allocPrint(scratch, "{s}/active/{d}.json", .{ base, record.lsn });
    var client = opened.client;
    var saved = client.getObject(opened.bucket, intent_key, .{ .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    var attempt: Attempt = undefined;
    if (saved) |value| attempt = try std.json.parseFromSliceLeaky(Attempt, scratch, value.body, .{}) else {
        var source_options = options;
        source_options.read_only = false;
        var files = try configured.openBindingObjectStoreAlloc(a, binding, source_options);
        defer files.deinit();
        const timestamp: i64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        const files_uri = if (files.fs_client != null) try std.fmt.allocPrint(scratch, "object://{s}/{s}", .{ files.bucket, files.prefix }) else binding.source_uri;
        const built = try catalog.row_commit.prepare(scratch, current.table, .{ .client = files.client, .bucket = files.bucket, .prefix = files.prefix, .uri = files_uri, .context = context }, parsed.value, record.lsn, timestamp);
        attempt = .{ .id = try std.fmt.allocPrint(scratch, "wal-{d}-{s}", .{ record.lsn, catalog.types.digestHex(current.table.metadata_location) }), .expected = current.table.metadata_location, .body = built.body, .timestamp_ms = timestamp };
        const bytes = try std.json.Stringify.valueAlloc(scratch, attempt, .{});
        var stored = client.putObject(opened.bucket, intent_key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => return true,
            else => return err,
        };
        defer stored.deinit(client.allocator);
    }
    const commit: catalog.types.Commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = attempt.timestamp_ms };
    var result = configured.executeLakeCatalogAlloc(a, binding, options, context, .{ .commit = commit }) catch |err| switch (err) {
        error.LakeCommitConflict => {
            // Catalogs expose Conflict only for a definitive rejection;
            // ambiguous attempts remain OutcomeUnknown and retain this intent.
            // REST also persists original 409 proof before returning Conflict.
            var active = client.getObject(opened.bucket, intent_key, .{ .cancellation = catalog.types.contextCancellation(&context) }) catch |read_error| switch (read_error) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound => return true,
                else => return read_error,
            };
            defer active.deinit(client.allocator);
            const candidate = try std.json.parseFromSliceLeaky(Attempt, scratch, active.body, .{});
            if (std.mem.eql(u8, candidate.id, attempt.id) and std.mem.eql(u8, candidate.expected, attempt.expected) and std.mem.eql(u8, candidate.body, attempt.body)) {
                try client.deleteObject(opened.bucket, intent_key, .{ .if_match_etag = active.metadata.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&context) });
            }
            return true;
        },
        else => return err,
    };
    defer result.deinit(a);
    if (try coverage(a, result.table) != record.lsn) return error.LakeWalCoverageGap;
    return true;
}

pub const Pending = struct { lsn: u64, key_fields: []const []const u8, changes: []const Batch.Change };
/// Caller arena owns the immutable WAL cut and its normalized final images.
pub fn pending(a: A, binding: Binding, options: Options, context: Context, cut: u64) !Pending {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context };
    const suffix = try store.range(a, cut);
    var changes: std.ArrayList(Batch.Change) = .empty;
    var latest: std.StringHashMapUnmanaged(usize) = .empty;
    var keys: []const []const u8 = &.{};
    for (suffix.records) |record| {
        const batch = try std.json.parseFromSliceLeaky(Batch, a, record.payload, .{ .allocate = .alloc_always });
        if (keys.len == 0) keys = batch.key_fields else if (!keyFieldsEqual(keys, batch.key_fields)) return error.LakeSourceConflict;
        for (batch.changes) |change| {
            var values: std.ArrayList(V) = .empty;
            for (keys) |field| try values.append(a, change.row.object.get(field) orelse return error.InvalidWal);
            const identity = try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, change.row, &.{});
            const entry = try latest.getOrPut(a, identity);
            if (entry.found_existing) changes.items[entry.value_ptr.*] = change else {
                if (changes.items.len == 65536) return error.LakeOverlayTooLarge;
                entry.value_ptr.* = changes.items.len;
                try changes.append(a, change);
            }
        }
    }
    return .{ .lsn = suffix.lsn, .key_fields = keys, .changes = changes.items };
}
