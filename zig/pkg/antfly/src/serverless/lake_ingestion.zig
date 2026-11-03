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
fn attemptId(a: A, lsn: u64, table: catalog.types.Table) ![]u8 {
    // Retirement advances managed HEAD without changing metadata_location.
    // A definitively rejected intent must get a new identity at that authority.
    const authority = try std.json.Stringify.valueAlloc(a, .{ table.metadata_location, table.version, table.record_key }, .{});
    defer a.free(authority);
    return std.fmt.allocPrint(a, "wal-{d}-{s}", .{ lsn, catalog.types.digestHex(authority) });
}
fn groupAttemptId(a: A, first: u64, last: u64, table: catalog.types.Table) ![]u8 {
    const authority = try std.json.Stringify.valueAlloc(a, .{ table.metadata_location, table.version, table.record_key }, .{});
    defer a.free(authority);
    return std.fmt.allocPrint(a, "wal-{d}-{d}-{s}", .{ first, last, catalog.types.digestHex(authority) });
}
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
/// Filesystem ingestion publishes object URIs through its bucket adapter.
/// Maintenance must use the same URI namespace when reading those artifacts.
pub fn destinationFiles(a: A, files: local.serverless_object_store_support.OpenedObjectStore, source_uri: []const u8, context: Context) !catalog.row_commit.Files {
    const uri = if (files.fs_client != null) try std.fmt.allocPrint(a, "object://{s}/{s}", .{ files.bucket, files.prefix }) else source_uri;
    return .{ .client = files.client, .bucket = files.bucket, .prefix = files.prefix, .uri = uri, .context = context };
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
    const identity = try std.json.Stringify.valueAlloc(scratch, .{ batch.source, batch.epoch, batch.batch_id }, .{});
    const operation_id = &catalog.types.digestHex(identity);
    var admission = try store.beginAdmission(scratch, operation_id, payload);
    defer admission.deinit();
    if (admission.observation.accepted) |accepted| return accepted;
    // Resolve an acknowledged retry before current-schema or checkpoint tests.
    // Source evolution cannot turn a durable exact replay into a new append.
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    try catalog.row_commit.validate(a, current.table.metadata_json, batch, context);
    const last = try admission.latest(scratch);
    const latest = admission.observation.captured.snapshot.latest_lsn;
    const covered = try coverage(a, current.table);
    // Retirement may race the catalog read. A missing last body is usable only
    // when this catalog cut proves the complete accepted high watermark.
    if (last == null and latest != covered) return error.LakeCheckpointConflict;
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
    return admission.append(covered);
}
fn keyFieldsEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| if (!std.mem.eql(u8, l, r)) return false;
    return true;
}
/// Group commit bounds. One catalog snapshot may cover several contiguous WAL
/// transactions; a group never exceeds the Parquet writer's row limit.
pub const max_group_changes: usize = catalog.parquet_writer.max_rows;
pub const max_group_payload_bytes: usize = 8 * 1024 * 1024;

/// A contiguous accepted WAL suffix folded into one source transaction.
pub const Group = struct { batch: Batch, first_lsn: u64, last_lsn: u64, records: usize };

/// Fold the longest admissible prefix of `records` into one transaction.
/// Members must share source, epoch and key definition and chain their
/// checkpoints exactly. Later images of a key replace earlier ones when the
/// commit is normalized, so the result equals committing each member in order.
/// A single record is returned unchanged. Allocations belong to `a`.
pub fn group(a: A, records: []const wal.Record, max_changes: usize, max_bytes: usize) !Group {
    if (records.len == 0) return error.InvalidWal;
    const first = try std.json.parseFromSliceLeaky(Batch, a, records[0].payload, .{ .allocate = .alloc_always });
    var result: Group = .{ .batch = first, .first_lsn = records[0].lsn, .last_lsn = records[0].lsn, .records = 1 };
    var changes: std.ArrayList(Batch.Change) = .empty;
    try changes.appendSlice(a, first.changes);
    var bytes: usize = records[0].payload.len;
    var tail = first;
    for (records[1..]) |record| {
        if (record.lsn != result.last_lsn + 1) break;
        if (bytes + record.payload.len > max_bytes) break;
        const next = try std.json.parseFromSliceLeaky(Batch, a, record.payload, .{ .allocate = .alloc_always });
        if (changes.items.len + next.changes.len > max_changes) break;
        if (!matchesPrevious(tail, next) or !keyFieldsEqual(tail.key_fields, next.key_fields)) break;
        try changes.appendSlice(a, next.changes);
        bytes += record.payload.len;
        result.last_lsn = record.lsn;
        result.records += 1;
        tail = next;
    }
    if (result.records == 1) return result;
    result.batch.batch_id = try std.fmt.allocPrint(a, "wal-group-{d}-{d}", .{ result.first_lsn, result.last_lsn });
    result.batch.checkpoint = tail.checkpoint;
    result.batch.changes = changes.items;
    return result;
}

/// The WAL coverage a prepared commit body publishes. Saved intents carry it
/// in their `set-properties` update, so replay needs no extra durable field.
pub fn plannedCoverage(a: A, body: []const u8) !u64 {
    const root = try std.json.parseFromSliceLeaky(V, a, body, .{});
    for ((try catalog.metadata.get(root, "updates")).array.items) |update| {
        if (!std.mem.eql(u8, try catalog.metadata.str(try catalog.metadata.get(update, "action")), "set-properties")) continue;
        const value = (try catalog.metadata.get(update, "updates")).object.get("antfly.wal.coverage") orelse continue;
        return std.fmt.parseInt(u64, try catalog.metadata.str(value), 10);
    }
    return error.LakeWalCoverageGap;
}

/// One bounded group commit per worker pass. Exact commit intents survive a
/// restart/timeout. A confirmed conflict alone allows a new pinned attempt.
pub fn drain(a: A, binding: Binding, options: Options, context: Context) !bool {
    return drainBounded(a, binding, options, context, max_group_changes, max_group_payload_bytes);
}

pub fn drainBounded(a: A, binding: Binding, options: Options, context: Context, max_changes: usize, max_bytes: usize) !bool {
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
    // Read the bounded pending suffix once. An oversized suffix still makes
    // progress one transaction at a time.
    const records: []const wal.Record = suffix: {
        const pending_range = store.range(scratch, cut) catch |err| switch (err) {
            error.LakeOverlayTooLarge => {
                const one = (try store.next(scratch, cut)) orelse return false;
                break :suffix try scratch.dupe(wal.Record, &.{one});
            },
            else => return err,
        };
        break :suffix pending_range.records;
    };
    if (records.len == 0) return false;
    if (records[0].lsn != cut + 1) return error.LakeWalCoverageGap;
    // The intent key names the group's first LSN. A saved intent pins the
    // exact group chosen before a crash, even if more records arrived since.
    const intent_key = try std.fmt.allocPrint(scratch, "{s}/active/{d}.json", .{ base, records[0].lsn });
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
        const destination = try destinationFiles(scratch, files, binding.source_uri, context);
        const planned = try group(scratch, records, max_changes, max_bytes);
        const built = try catalog.row_commit.prepare(scratch, current.table, destination, planned.batch, planned.last_lsn, timestamp);
        const id = if (planned.records == 1) try attemptId(scratch, planned.first_lsn, current.table) else try groupAttemptId(scratch, planned.first_lsn, planned.last_lsn, current.table);
        attempt = .{ .id = id, .expected = current.table.metadata_location, .body = built.body, .timestamp_ms = timestamp };
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
                try client.compareAndDelete(opened.bucket, intent_key, active.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&context) });
            }
            return true;
        },
        else => return err,
    };
    defer result.deinit(a);
    if (try coverage(a, result.table) != try plannedCoverage(scratch, attempt.body)) return error.LakeWalCoverageGap;
    return true;
}

pub fn watermark(a: A, binding: Binding, options: Options, context: Context) !u64 {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context };
    return store.watermark(a);
}

pub const KeyIdentity = enum { legacy_json, canonical };
pub fn rowId(a: A, keys: []const []const u8, row: V, encoding: KeyIdentity) ![]u8 {
    const encoded = if (encoding == .canonical)
        try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, row, &.{})
    else legacy: {
        const values = try a.alloc(V, keys.len);
        defer a.free(values);
        for (keys, values) |key, *value| value.* = row.object.get(key) orelse return error.InvalidWal;
        break :legacy try std.json.Stringify.valueAlloc(a, values, .{});
    };
    defer a.free(encoded);
    return std.fmt.allocPrint(a, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex(encoded)});
}
pub const Pending = struct {
    key_identity: KeyIdentity = .legacy_json,
    block_digests: []const [32]u8 = &.{},
    cut_identity: ?[32]u8 = null,
    base_lsn: u64 = 0,
    lsn: u64,
    key_fields: []const []const u8,
    changes: []const Batch.Change = &.{},
    change_blocks: []const []const Batch.Change = &.{},
    pub fn count(self: @This()) usize {
        var n = self.changes.len;
        for (self.change_blocks) |block| n += block.len;
        return n;
    }
    pub fn materialized(self: @This(), a: A) !Pending {
        if (self.change_blocks.len == 0) return self;
        var result = self;
        const changes = try a.alloc(Batch.Change, self.count());
        @memcpy(changes[0..self.changes.len], self.changes);
        var n = self.changes.len;
        for (self.change_blocks) |block| {
            @memcpy(changes[n..][0..block.len], block);
            n += block.len;
        }
        result.changes = changes;
        result.change_blocks = &.{};
        result.block_digests = &.{};
        return result;
    }
    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        try stream.objectField("key_identity");
        try stream.write(self.key_identity);
        try stream.objectField("cut_identity");
        try stream.write(self.cut_identity);
        try stream.objectField("base_lsn");
        try stream.write(self.base_lsn);
        try stream.objectField("lsn");
        try stream.write(self.lsn);
        try stream.objectField("key_fields");
        try stream.write(self.key_fields);
        try stream.objectField("changes");
        try stream.beginArray();
        for (self.changes) |change| try stream.write(change);
        for (self.change_blocks) |block| for (block) |change| try stream.write(change);
        try stream.endArray();
        try stream.endObject();
    }
};
/// Caller arena owns the immutable WAL cut and its normalized final images.
pub fn pending(a: A, binding: Binding, options: Options, context: Context, cut: u64) !Pending {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context };
    const suffix = try store.range(a, cut);
    return normalizePending(a, cut, suffix.lsn, suffix.records);
}
pub fn normalizePending(a: A, cut: u64, lsn: u64, records: []const wal.Record) !Pending {
    return normalizePendingWithBase(a, cut, lsn, null, records);
}
/// Final images may borrow an immutable prior cut. Its owner must outlive this
/// result; new records are parsed only once and replace earlier key images.
pub fn normalizePendingWithBase(a: A, cut: u64, lsn: u64, base: ?Pending, records: []const wal.Record) !Pending {
    var changes: std.ArrayList(Batch.Change) = .empty;
    var latest: std.StringHashMapUnmanaged(usize) = .empty;
    var keys: []const []const u8 = if (base) |previous| previous.key_fields else &.{};
    if (base) |previous| for (previous.changes) |change| {
        const identity = try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, change.row, &.{});
        try latest.put(a, identity, changes.items.len);
        try changes.append(a, change);
    };
    for (records) |record| {
        const batch = try std.json.parseFromSliceLeaky(Batch, a, record.payload, .{ .allocate = .alloc_always });
        if (keys.len == 0) keys = batch.key_fields else if (!keyFieldsEqual(keys, batch.key_fields)) return error.LakeSourceConflict;
        for (batch.changes) |change| {
            const identity = try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, change.row, &.{});
            const entry = try latest.getOrPut(a, identity);
            if (entry.found_existing) changes.items[entry.value_ptr.*] = change else {
                if (changes.items.len == 65536) return error.LakeOverlayTooLarge;
                entry.value_ptr.* = changes.items.len;
                try changes.append(a, change);
            }
        }
    }
    return .{ .base_lsn = cut, .lsn = lsn, .key_fields = keys, .changes = changes.items };
}

test "external lake filesystem maintenance reads ingestion artifacts in the same namespace" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-artifact-namespace");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{directory.path()});
    var opened = try local.serverless_object_store_support.OpenedObjectStore.initFileUriWithOptions(alloc, uri, "archive", .{ .ensure_bucket = true });
    defer opened.deinit();
    const writer = try destinationFiles(a, opened, uri, .{});
    const artifact = try catalog.row_commit.upload(a, writer, "metadata/manifest.avro", "manifest-content");
    try std.testing.expect(std.mem.startsWith(u8, artifact, "object://archive/"));
    const maintenance = try destinationFiles(a, opened, uri, .{});
    try std.testing.expectEqualStrings("manifest-content", try catalog.row_commit.read(a, maintenance, artifact));
}

test "external lake filesystem retirement validates live roots and subsequent ingestion" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-filesystem-retirement");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{directory.path()});
    var opened = try local.serverless_object_store_support.OpenedObjectStore.initFileUriWithOptions(alloc, uri, "archive", .{ .ensure_bucket = true });
    defer opened.deinit();
    const files = try destinationFiles(a, opened, uri, .{});
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = uri };
    _ = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const row = try std.json.parseFromSliceLeaky(V, a, "{\"id\":1}", .{});
    const batch: Batch = .{ .batch_id = "first", .source = "test", .epoch = "1", .checkpoint = "1", .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
    const initial = try managed.load(a);
    const first = try catalog.row_commit.prepare(a, initial, files, batch, 1, 2);
    _ = try managed.commit(a, .{ .id = "first", .expected_metadata_location = initial.metadata_location, .body = first.body, .timestamp_ms = 2 });
    const parent = try managed.load(a);
    const orphan = try catalog.row_commit.upload(a, files, "data/orphan.parquet", "orphan");
    const retired = try managed.retire(a, .{ .id = "retire", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{orphan} });
    try std.testing.expect(retired.retirement_root != null);
    var reopened = managed;
    const current = try reopened.load(a);
    var next = batch;
    next.batch_id = "second";
    next.expected_checkpoint = "1";
    next.checkpoint = "2";
    const second = try catalog.row_commit.prepare(a, current, files, next, 2, 3);
    _ = try reopened.commit(a, .{ .id = "second", .expected_metadata_location = current.metadata_location, .body = second.body, .timestamp_ms = 3 });
    const latest = try reopened.load(a);
    const root = try catalog.metadata.parse(a, latest.metadata_json);
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    const live_uri = try catalog.metadata.str(try catalog.metadata.get(snapshots[snapshots.len - 1], "manifest-list"));
    try std.testing.expectError(error.LakeObjectStillReferenced, reopened.retire(a, .{ .id = "retire-live", .expected_metadata_location = latest.metadata_location, .expected_version = latest.version.?, .objects = &.{live_uri} }));
}

test "external lake filesystem WAL retries a rejected intent after metadata preserving retirement" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-wal-retirement-retry");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: Options = .{ .node_config = &config, .catalog_table_id = 7 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    var client = opened.client;
    var managed: catalog.managed.Managed = .{ .client = client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    _ = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const parent = try managed.load(a);
    const payload = "{\"batch_id\":\"first\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1}}]}";
    const lsn = try accept(a, binding, options, .{}, payload);
    const batch = try std.json.parseFromSliceLeaky(Batch, a, payload, .{});
    const files = try destinationFiles(a, opened, binding.source_uri, .{});
    const prepared = try catalog.row_commit.prepare(a, parent, files, batch, lsn, 2);
    const attempt: Attempt = .{ .id = try attemptId(a, lsn, parent), .expected = parent.metadata_location, .body = prepared.body, .timestamp_ms = 2 };
    const commit: catalog.types.Commit = .{ .id = attempt.id, .expected_metadata_location = attempt.expected, .body = attempt.body, .timestamp_ms = attempt.timestamp_ms };
    var queue = try openQueue(alloc, binding, options);
    defer queue.deinit();
    var queue_client = queue.client;
    const active_key = try std.fmt.allocPrint(a, "{s}/active/{d}.json", .{ try prefix(a, queue.prefix, binding, options), lsn });
    var active = try queue_client.putObject(queue.bucket, active_key, try std.json.Stringify.valueAlloc(a, attempt, .{}), .{});
    active.deinit(queue_client.allocator);
    // Seed the durable catalog intent at the crash point immediately before
    // its HEAD CAS. Retirement then wins that CAS while preserving metadata.
    const candidate = try catalog.metadata.applyAlloc(a, parent.metadata_json, parent.metadata_location, commit);
    const relative = try std.fmt.allocPrint(a, "metadata/antfly-{s}.metadata.json", .{catalog.types.digestHex(candidate)});
    const metadata_key = try std.fmt.allocPrint(a, "{s}{s}{s}", .{ opened.prefix, if (opened.prefix.len == 0) "" else "/", relative });
    var metadata_object = try client.putObject(opened.bucket, metadata_key, candidate, .{});
    metadata_object.deinit(client.allocator);
    const rejected: catalog.managed.Record = .{ .commit_id = attempt.id, .request_hash = &catalog.types.commitHash(commit), .metadata_location = try std.fmt.allocPrint(a, "{s}/{s}", .{ binding.source_uri, relative }), .metadata_key = metadata_key, .metadata_hash = &catalog.types.digestHex(candidate), .previous_record = parent.record_key, .previous_version = parent.version };
    const intent_key = try std.fmt.allocPrint(a, "{s}{s}metadata/antfly-catalog/intents/{s}.json", .{ opened.prefix, if (opened.prefix.len == 0) "" else "/", catalog.types.digestHex(attempt.id) });
    var intent = try client.putObject(opened.bucket, intent_key, try std.json.Stringify.valueAlloc(a, rejected, .{}), .{});
    intent.deinit(client.allocator);
    const orphan = try catalog.row_commit.upload(a, files, "data/orphan.parquet", "orphan");
    _ = try managed.retire(a, .{ .id = "retire", .expected_metadata_location = parent.metadata_location, .expected_version = parent.version.?, .objects = &.{orphan} });
    const retired = try managed.load(a);
    try std.testing.expectEqualStrings(parent.metadata_location, retired.metadata_location);
    try std.testing.expect(!std.mem.eql(u8, parent.version.?, retired.version.?));
    try std.testing.expectError(error.LakeCommitConflict, managed.commit(a, commit));
    // First pass resolves the saved rejection, second pass plans at the new
    // catalog version, and a third pass proves publication is complete.
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(!try drain(a, binding, options, .{}));
    const published = try managed.load(a);
    try std.testing.expectEqual(lsn, try coverage(a, published));
    try std.testing.expectEqual(catalog.types.Outcome.not_committed, try managed.resolve(a, attempt.id, &catalog.types.commitHash(commit)));
    const shared: wal.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try prefix(a, queue.prefix, binding, options) };
    const collected = try shared.prune(a, lsn, 256, false);
    try std.testing.expect(collected.complete);
    try std.testing.expect(try shared.latest(a) == null);
    // The exact original request remains a receipt read after publication and
    // complete body retirement, even though its initial checkpoint is stale.
    try std.testing.expectEqual(lsn, try accept(a, binding, options, .{}, payload));
    const invalid_reuse = "{\"batch_id\":\"first\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":\"invalid-long\"}}]}";
    try std.testing.expectError(error.WalIdempotencyConflict, accept(a, binding, options, .{}, invalid_reuse));
    const next = "{\"batch_id\":\"second\",\"source\":\"test\",\"epoch\":\"1\",\"expected_checkpoint\":\"1\",\"checkpoint\":\"2\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":2}}]}";
    try std.testing.expectEqual(lsn + 1, try accept(a, binding, options, .{}, next));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(!try drain(a, binding, options, .{}));
}

test "external lake WAL grouping stops at chain breaks gaps and limits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const records = [_]wal.Record{
        .{ .lsn = 4, .operation_id = "a", .payload = "{\"batch_id\":\"a\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"3\",\"checkpoint\":\"4\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1}}]}" },
        .{ .lsn = 5, .operation_id = "b", .payload = "{\"batch_id\":\"b\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"4\",\"checkpoint\":\"5\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":1}},{\"op\":\"upsert\",\"row\":{\"id\":2}}]}" },
        // The checkpoint chain breaks here, so this record starts a new group.
        .{ .lsn = 6, .operation_id = "c", .payload = "{\"batch_id\":\"c\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"9\",\"checkpoint\":\"6\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":3}}]}" },
    };
    const folded = try group(a, &records, 100, 1 << 20);
    try std.testing.expectEqual(@as(usize, 2), folded.records);
    try std.testing.expectEqual(@as(u64, 4), folded.first_lsn);
    try std.testing.expectEqual(@as(u64, 5), folded.last_lsn);
    try std.testing.expectEqualStrings("wal-group-4-5", folded.batch.batch_id);
    try std.testing.expectEqualStrings("3", folded.batch.expected_checkpoint.?);
    try std.testing.expectEqualStrings("5", folded.batch.checkpoint);
    try std.testing.expectEqual(@as(usize, 3), folded.batch.changes.len);
    // A single record keeps its original identity, exactly as before grouping.
    const alone = try group(a, records[2..], 100, 1 << 20);
    try std.testing.expectEqualStrings("c", alone.batch.batch_id);
    try std.testing.expectEqual(@as(usize, 1), (try group(a, &records, 2, 1 << 20)).records);
    try std.testing.expectEqual(@as(usize, 1), (try group(a, &records, 100, records[0].payload.len)).records);
    var gap = records;
    gap[1].lsn = 7;
    try std.testing.expectEqual(@as(usize, 1), (try group(a, &gap, 100, 1 << 20)).records);
}

test "external lake filesystem WAL group commit folds pending transactions into one snapshot" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-wal-group-commit");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    const binding: Binding = .{ .table_id = "events", .format = .iceberg, .source_uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "schema", .write_policy = .iceberg_writer, .catalog = .{ .type = .managed } };
    const options: Options = .{ .node_config = &config, .catalog_table_id = 9 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, binding, options);
    defer opened.deinit();
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri };
    _ = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}}", 1);
    const payloads = [_][]const u8{
        "{\"batch_id\":\"b1\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1,\"body\":\"a\"}},{\"op\":\"upsert\",\"row\":{\"id\":2,\"body\":\"b\"}}]}",
        "{\"batch_id\":\"b2\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"1\",\"checkpoint\":\"2\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":2,\"body\":\"c\"}}]}",
        "{\"batch_id\":\"b3\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"2\",\"checkpoint\":\"3\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":1}}]}",
        "{\"batch_id\":\"b4\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"3\",\"checkpoint\":\"4\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":3}}]}",
        "{\"batch_id\":\"b5\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"4\",\"checkpoint\":\"5\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":4}}]}",
    };
    for (payloads, 1..) |payload, lsn| try std.testing.expectEqual(@as(u64, lsn), try accept(a, binding, options, .{}, payload));
    // Four changes fit: b1..b3 fold into one snapshot; b4 would exceed the limit.
    try std.testing.expect(try drainBounded(a, binding, options, .{}, 4, max_group_payload_bytes));
    const first = try managed.load(a);
    try std.testing.expectEqual(@as(u64, 3), try coverage(a, first));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(!try drain(a, binding, options, .{}));
    const last = try managed.load(a);
    try std.testing.expectEqual(@as(u64, 5), try coverage(a, last));
    const root = try catalog.metadata.parse(a, last.metadata_json);
    try std.testing.expectEqualStrings("5", try catalog.metadata.str((try catalog.metadata.get(root, "properties")).object.get("antfly.cdc.checkpoint").?));
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    try std.testing.expectEqual(@as(usize, 2), snapshots.len);
    const summary = try catalog.metadata.get(snapshots[0], "summary");
    try std.testing.expectEqualStrings("wal-group-1-3", try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.batch-id")));
    try std.testing.expectEqualStrings("3", try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.wal.coverage")));
    // The folded group equals applying b1..b3 in order: one live row (id 2,
    // body c) and equality deletes for both touched keys.
    const files = try destinationFiles(a, opened, binding.source_uri, .{});
    const list = try local.serverless_external_source_mod.iceberg_avro.parseManifestListAlloc(a, try catalog.row_commit.read(a, files, try catalog.metadata.str(try catalog.metadata.get(snapshots[0], "manifest-list"))));
    try std.testing.expectEqual(@as(usize, 2), list.entries.len);
    try std.testing.expectEqual(@as(u64, 1), list.entries[0].added_rows_count);
    try std.testing.expectEqual(@as(u64, 2), list.entries[1].added_rows_count);
    // The exact original requests stay receipts after their group publishes.
    try std.testing.expectEqual(@as(u64, 2), try accept(a, binding, options, .{}, payloads[1]));
}
