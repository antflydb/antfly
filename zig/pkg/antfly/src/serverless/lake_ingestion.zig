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
    return configured.openNativeArtifactObjectStoreWithTransportAlloc(a, config, options.secret_store, false, .{ .io = options.io, .requests = options.requests });
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
pub fn accept(a: A, binding: Binding, unresolved_options: Options, context: Context, body: []const u8) !u64 {
    const options = try unresolved_options.resolveOwner(a, context);
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
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    const identity = try std.json.Stringify.valueAlloc(scratch, .{ batch.source, batch.epoch, batch.batch_id }, .{});
    const operation_id = &catalog.types.digestHex(identity);
    var admission = try store.beginAdmission(scratch, operation_id, payload);
    defer admission.deinit();
    if (admission.observation.accepted) |accepted| {
        if (options.native_owner) |owner| try owner.committed(accepted, context);
        return accepted;
    }
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
    const accepted = try admission.append(covered);
    if (options.native_owner) |owner| try owner.committed(accepted, context);
    return accepted;
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

const GroupBuilder = struct {
    result: Group,
    changes: std.ArrayList(Batch.Change) = .empty,
    bytes: usize,
    tail: Batch,

    fn init(a: A, record: wal.Record) !GroupBuilder {
        const first = try std.json.parseFromSliceLeaky(Batch, a, record.payload, .{ .allocate = .alloc_always });
        return .{ .result = .{ .batch = first, .first_lsn = record.lsn, .last_lsn = record.lsn, .records = 1 }, .bytes = record.payload.len, .tail = first };
    }
    fn full(self: GroupBuilder, max_changes: usize, max_bytes: usize) bool {
        const count = if (self.result.records == 1) self.result.batch.changes.len else self.changes.items.len;
        return count >= max_changes or self.bytes >= max_bytes;
    }
    fn add(self: *GroupBuilder, a: A, record: wal.Record, max_changes: usize, max_bytes: usize) !bool {
        if (self.full(max_changes, max_bytes) or record.lsn != self.result.last_lsn + 1) return false;
        if (record.payload.len > max_bytes - self.bytes) return false;
        const next = try std.json.parseFromSliceLeaky(Batch, a, record.payload, .{ .allocate = .alloc_always });
        const count = if (self.result.records == 1) self.result.batch.changes.len else self.changes.items.len;
        if (next.changes.len > max_changes - count) return false;
        if (!matchesPrevious(self.tail, next) or !keyFieldsEqual(self.tail.key_fields, next.key_fields)) return false;
        // Checkpoints are opaque: a valid chain can revisit its starting value.
        // Such a chain cannot be represented by one row batch (which forbids
        // equal predecessor/successor), so leave this member for the next pass.
        if (self.result.batch.expected_checkpoint) |initial| if (std.mem.eql(u8, initial, next.checkpoint)) return false;
        if (self.result.records == 1) try self.changes.appendSlice(a, self.result.batch.changes);
        try self.changes.appendSlice(a, next.changes);
        self.bytes += record.payload.len;
        self.result.last_lsn = record.lsn;
        self.result.records += 1;
        self.tail = next;
        return true;
    }
    fn finish(self: *GroupBuilder, a: A) !Group {
        if (self.result.records == 1) return self.result;
        self.result.batch.batch_id = try std.fmt.allocPrint(a, "wal-group-{d}-{d}", .{ self.result.first_lsn, self.result.last_lsn });
        self.result.batch.checkpoint = self.tail.checkpoint;
        self.result.batch.changes = self.changes.items;
        return self.result;
    }
};

/// Fold the longest admissible prefix of `records` into one transaction.
/// Members must share source, epoch and key definition and chain their
/// checkpoints exactly. Later images of a key replace earlier ones when the
/// commit is normalized, so the result equals committing each member in order.
/// A single record is returned unchanged. Allocations belong to `a`.
pub fn group(a: A, records: []const wal.Record, max_changes: usize, max_bytes: usize) !Group {
    if (records.len == 0) return error.InvalidWal;
    var builder = try GroupBuilder.init(a, records[0]);
    for (records[1..]) |record| if (!try builder.add(a, record, max_changes, max_bytes)) break;
    return builder.finish(a);
}

fn readGroup(a: A, reader: *wal.Store.Reader, max_changes: usize, max_bytes: usize) !?Group {
    const first = (try reader.next(a)) orelse return null;
    var builder = try GroupBuilder.init(a, first);
    while (!builder.full(max_changes, max_bytes)) {
        const record = (reader.nextCaptured(a) catch |err| switch (err) {
            // These rows are already owned and remain valid after the lazy WAL
            // root expires. Commit their prefix instead of retrying it forever.
            error.WalSnapshotExpired => {
                try reader.context.ensureActive();
                break;
            },
            else => return err,
        }) orelse break;
        if (!try builder.add(a, record, max_changes, max_bytes)) break;
    }
    try reader.context.ensureActive();
    return try builder.finish(a);
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
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const cut = try coverage(a, current.table);
    // Resolve saved intents before touching WAL payloads. A retry publishes
    // exactly its original group, independently of the current pending suffix.
    const intent_key = try std.fmt.allocPrint(scratch, "{s}/active/{d}.json", .{ base, cut + 1 });
    var client = opened.client;
    var saved = client.getObject(opened.bucket, intent_key, .{ .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    var attempt: Attempt = undefined;
    if (saved) |value| attempt = try std.json.parseFromSliceLeaky(Attempt, scratch, value.body, .{}) else {
        var reader = try store.reader(scratch, cut);
        defer reader.deinit();
        const planned = (try readGroup(scratch, &reader, max_changes, max_bytes)) orelse return false;
        var source_options = options;
        source_options.read_only = false;
        var files = try configured.openBindingObjectStoreAlloc(a, binding, source_options);
        defer files.deinit();
        const timestamp: i64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        const destination = try destinationFiles(scratch, files, binding.source_uri, context);
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
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    return store.watermark(a);
}

pub fn rowId(a: A, keys: []const []const u8, row: V) ![]u8 {
    const encoded = try local.serverless_external_source_mod.lake_catalog.row_commit.stable_key.identity(a, keys, row, &.{});
    defer a.free(encoded);
    return std.fmt.allocPrint(a, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex(encoded)});
}
pub const Pending = struct {
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
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
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
    // Opaque checkpoints may revisit a prior value. Both transactions are
    // admitted, but folding 5 -> 6 -> 5 must not create a forbidden no-op batch.
    const cycle = [_][]const u8{
        "{\"batch_id\":\"cycle-a\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"5\",\"checkpoint\":\"6\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":5}}]}",
        "{\"batch_id\":\"cycle-b\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"6\",\"checkpoint\":\"5\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":5}}]}",
    };
    for (cycle, 6..) |payload, lsn| try std.testing.expectEqual(@as(u64, lsn), try accept(a, binding, options, .{}, payload));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expectEqual(@as(u64, 6), try coverage(a, try managed.load(a)));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expectEqual(@as(u64, 7), try coverage(a, try managed.load(a)));
    try std.testing.expect(!try drain(a, binding, options, .{}));
    // The exact original requests stay receipts after their group publishes.
    try std.testing.expectEqual(@as(u64, 2), try accept(a, binding, options, .{}, payloads[1]));
}

test "external lake WAL group reader stops before hydrating a bounded pending suffix" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("objectstore").MemoryClient.init(alloc);
    defer memory.deinit();
    const store: wal.Store = .{ .client = memory.client(), .bucket = "archive", .prefix = "bounded-group" };
    // Seed the shared WAL's supported unpacked layout. Its large-payload
    // fixture is independent of the packed-page writer's batch-size ceiling.
    var seed = try store.open(a, true);
    defer seed.deinit();
    seed.indexed.?.packed_pages_enabled = false;
    const row = try std.json.parseFromSliceLeaky(V, a, "{\"id\":1}", .{});
    for (0..4) |i| {
        const batch: Batch = .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{i}), .source = "s", .epoch = "1", .expected_checkpoint = try std.fmt.allocPrint(a, "{d}", .{i}), .checkpoint = try std.fmt.allocPrint(a, "{d}", .{i + 1}), .key_fields = &.{"id"}, .changes = &.{.{ .op = .upsert, .row = row }} };
        _ = try seed.indexed.?.appendIdempotent(store.collectionNamespace(), 0, try std.json.Stringify.valueAlloc(a, batch, .{}), batch.batch_id);
    }
    // More than range()'s 32 MiB budget follows the group. No later payload
    // needs parsing or hydrating just to commit the first two changes.
    const large = try a.alloc(u8, 1024 * 1024);
    @memset(large, 'x');
    for (4..40) |i| _ = try seed.indexed.?.appendIdempotent(store.collectionNamespace(), 0, large, try std.fmt.allocPrint(a, "b{d}", .{i}));
    var reader = try store.reader(a, 0);
    defer reader.deinit();
    const first = (try readGroup(a, &reader, 2, max_group_payload_bytes)).?;
    try std.testing.expectEqual(@as(usize, 2), first.records);
    try std.testing.expectEqual(@as(u64, 2), first.last_lsn);
    try std.testing.expectEqual(@as(u64, 2), reader.position);
    const second = (try readGroup(a, &reader, 2, max_group_payload_bytes)).?;
    try std.testing.expectEqual(@as(u64, 3), second.first_lsn);
    try std.testing.expectEqual(@as(u64, 4), second.last_lsn);
    try std.testing.expectEqual(@as(u64, 4), reader.position);
    // A byte-bound group consumes at most one lookahead, never the full suffix.
    var byte_reader = try store.reader(a, 0);
    defer byte_reader.deinit();
    const one = (try readGroup(a, &byte_reader, max_group_changes, 1)).?;
    try std.testing.expectEqual(@as(usize, 1), one.records);
    try std.testing.expectEqual(@as(u64, 1), byte_reader.position);
}

test "external lake WAL group yields an owned prefix on root expiry without swallowing failures" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const objectstore = @import("objectstore");
    const Indexed = @import("wal/indexed_object_store.zig").IndexedObjectStore;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var faults = objectstore.ScriptedFaultClient.init(alloc, client);
    defer faults.deinit();
    const Delay = struct {
        now: u64 = std.time.ns_per_s,
        step: u64 = 0,
        faults: *objectstore.ScriptedFaultClient,
        pages: usize = 0,
        fail_after: ?usize = null,
        fn reader(self: *@This(), reader_alloc: A, owner: *Indexed, start: u64, context: Context) !wal.Store.Reader {
            return .{
                .wal = .{ .alloc = reader_alloc, .client = self.faults.client(), .bucket = try reader_alloc.dupe(u8, "db"), .prefix = try reader_alloc.dupe(u8, "probe"), .owns_client = false },
                .cursor = try owner.capturedReader(reader_alloc, "probe", try owner.captureRead("probe"), .{ .start_lsn = start, .end_lsn = 4, .max_record_bytes = 32 * 1024 * 1024 }),
                .position = start - 1,
                .context = context,
            };
        }
        fn clock(raw: ?*const anyopaque) u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return self.now;
        }
        fn page(_: *anyopaque, _: []const u8, key: []const u8) bool {
            return std.mem.indexOf(u8, key, "/pages/") != null;
        }
        fn advance(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.now += self.step;
            self.pages += 1;
            if (self.fail_after) |maximum| if (self.pages == maximum) {
                self.faults.get_filter = .{ .ptr = self, .matches = page };
                self.faults.next_get = .{ .fail_before = error.ArtifactIntegrityMismatch };
                return;
            };
            self.arm();
        }
        fn arm(self: *@This()) void {
            self.faults.get_filter = .{ .ptr = self, .matches = page };
            self.faults.next_get = .{ .complete_then_callback = .{ .ptr = self, .run = advance } };
        }
    };
    var delay: Delay = .{ .faults = &faults };
    var indexed = try Indexed.init(a, faults.client(), "db", "probe");
    defer indexed.deinit();
    indexed.managed_reads = true;
    indexed.read_clock = .{ .ptr = &delay, .now_fn = Delay.clock };
    const padding = try a.alloc(u8, 3 * 1024 * 1024);
    @memset(padding, 'x');
    for (1..5) |n| {
        const payload = try std.json.Stringify.valueAlloc(a, .{ .batch_id = try std.fmt.allocPrint(a, "b{d}", .{n}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{n}), .expected_checkpoint = if (n == 1) null else try std.fmt.allocPrint(a, "{d}", .{n - 1}), .key_fields = .{"id"}, .changes = .{.{ .op = "upsert", .row = .{ .id = n, .body = padding } }} }, .{});
        _ = try indexed.appendIdempotent("probe", 0, payload, try std.fmt.allocPrint(a, "op{d}", .{n}));
    }
    const cut = try indexed.captureRead("probe");
    var measuring = try indexed.capturedReader(a, "probe", cut, .{ .start_lsn = 1, .end_lsn = 4, .max_record_bytes = 32 * 1024 * 1024 });
    defer measuring.deinit();
    const start_gets = faults.get_attempts;
    _ = (try measuring.next(a)).?;
    const reads = faults.get_attempts - start_gets;
    try std.testing.expect(reads > 0);
    delay.step = 14 * std.time.ns_per_s / reads;
    delay.arm();
    // A one-record drain works under this provider latency.
    var single = try indexed.capturedReader(a, "probe", try indexed.captureRead("probe"), .{ .start_lsn = 1, .end_lsn = 4, .max_record_bytes = 32 * 1024 * 1024 });
    defer single.deinit();
    const single_start = delay.now;
    _ = (try single.next(a)).?;
    try std.testing.expect(delay.now - single_start < 30 * std.time.ns_per_s);
    var covered: u64 = 0;
    for (0..2) |_| {
        var adapter = try delay.reader(a, &indexed, covered + 1, .{});
        defer adapter.deinit();
        const result = (try readGroup(a, &adapter, max_group_changes, max_group_payload_bytes)).?;
        try std.testing.expectEqual(covered + 1, result.first_lsn);
        try std.testing.expectEqual(@as(usize, 2), result.records);
        covered = result.last_lsn;
    }
    try std.testing.expectEqual(@as(u64, 4), covered);
    // Root expiry is a yield only after at least one record is fully owned.
    delay.step = 40 * std.time.ns_per_s / reads;
    var empty = try delay.reader(a, &indexed, 1, .{});
    defer empty.deinit();
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, readGroup(a, &empty, max_group_changes, max_group_payload_bytes));
    // A caller deadline that coincides with root expiry must remain fatal.
    delay.step = 14 * std.time.ns_per_s / reads;
    const Deadline = struct {
        delay: *Delay,
        start: u64,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.delay.now - self.start >= 30 * std.time.ns_per_s) return error.DeadlineExceeded;
        }
    };
    var deadline: Deadline = .{ .delay = &delay, .start = delay.now };
    var canceled = try delay.reader(a, &indexed, 1, .{ .checkpoint = .{ .ptr = &deadline, .check = Deadline.check } });
    defer canceled.deinit();
    try std.testing.expectError(error.DeadlineExceeded, readGroup(a, &canceled, max_group_changes, max_group_payload_bytes));
    // Integrity/transport failures are never converted into successful prefixes.
    delay.step = 0;
    delay.pages = 0;
    delay.fail_after = reads;
    delay.arm();
    var corrupted = try delay.reader(a, &indexed, 1, .{});
    defer corrupted.deinit();
    try std.testing.expectError(error.ArtifactIntegrityMismatch, readGroup(a, &corrupted, max_group_changes, max_group_payload_bytes));
}
