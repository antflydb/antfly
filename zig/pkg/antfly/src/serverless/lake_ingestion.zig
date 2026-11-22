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
    return configured.openNativeArtifactObjectStoreWithTransportAlloc(a, config, options.secret_store, false, .{ .io = options.io, .requests = options.requests, .query_requests = options.query_requests });
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
    return propertyCoverage(try catalogProperties(p.value));
}
fn catalogProperties(metadata: V) !std.json.ObjectMap {
    const properties = try catalog.metadata.get(metadata, "properties");
    if (properties != .object) return error.InvalidLakeMetadata;
    return properties.object;
}
fn propertyCoverage(properties: std.json.ObjectMap) !u64 {
    const value = properties.get("antfly.wal.coverage") orelse return 0;
    return std.fmt.parseInt(u64, try catalog.metadata.str(value), 10);
}
fn requireSourceProperties(a: A, properties: std.json.ObjectMap, batch: Batch) !void {
    const source = properties.get("antfly.cdc.source") orelse return error.LakeSourceConflict;
    const epoch = properties.get("antfly.cdc.epoch") orelse return error.LakeSourceConflict;
    const keys = properties.get("antfly.cdc.key-fields") orelse return error.LakeSourceConflict;
    const key_definition = try std.json.Stringify.valueAlloc(a, batch.key_fields, .{});
    defer a.free(key_definition);
    if (!std.mem.eql(u8, try catalog.metadata.str(source), batch.source) or
        !std.mem.eql(u8, try catalog.metadata.str(epoch), batch.epoch) or
        !std.mem.eql(u8, try catalog.metadata.str(keys), key_definition)) return error.LakeSourceConflict;
}
fn matchesPrevious(previous: Batch, batch: Batch) bool {
    return batch.follows(previous);
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
    if (parsed.value.bootstrap) return error.InvalidSourceTransaction;
    return acceptResolvedBatch(a, binding, options, context, parsed.value);
}

/// Internal capture callers can admit a typed transaction through the same
/// durable owner, checkpoint, and idempotency checks as /lake/changes.
pub fn acceptBatch(a: A, binding: Binding, unresolved_options: Options, context: Context, batch: Batch) !u64 {
    return acceptResolvedBatch(a, binding, try unresolved_options.resolveOwner(a, context), context, batch);
}

/// Lower one complete pgoutput transaction to the writable Iceberg row
/// contract. The caller supplies a fenced source epoch and the Iceberg
/// equality key. The source checkpoint selects the retry identity. The batch
/// borrows row images and owns its derived fields in the caller’s arena.
pub fn postgresBatch(a: A, source: []const u8, epoch: []const u8, previous: ?[]const u8, checkpoint: []const u8, keys: []const []const u8, transaction: []const @import("../foreign/source.zig").ReplicationChange) !Batch {
    const batch_id = try a.dupe(u8, &catalog.types.digestHex(checkpoint));
    var changes: std.ArrayList(Batch.Change) = .empty;
    for (transaction) |change| {
        if (!std.mem.eql(u8, checkpoint, change.checkpoint)) return error.InvalidReplicationCheckpoint;
        var row = change.row orelse return error.InvalidReplicationSourceRow;
        switch (change.op) {
            .delete => try changes.append(a, .{ .op = .delete, .row = try keyImage(a, keys, row) }),
            .insert, .update => {
                if (!change.row_complete) {
                    const before = change.before orelse return error.ReplicationProjectionIncomplete;
                    if (change.op != .update or !change.before_complete or before != .object or row != .object)
                        return error.ReplicationProjectionIncomplete;
                    var complete = try before.object.clone(a);
                    var fields = row.object.iterator();
                    while (fields.next()) |field| try complete.put(a, field.key_ptr.*, field.value_ptr.*);
                    row = .{ .object = complete };
                }
                if (change.op == .update) {
                    // A missing before image cannot prove that a separately
                    // configured Iceberg equality key stayed the same.
                    const before = change.before orelse return error.ReplicationProjectionIncomplete;
                    const old_key = try keyImage(a, keys, before);
                    if (!std.mem.eql(u8, try rowId(a, keys, old_key), try rowId(a, keys, row)))
                        try changes.append(a, .{ .op = .delete, .row = old_key });
                }
                try changes.append(a, .{ .op = .upsert, .row = row });
            },
        }
        if (changes.items.len > max_group_changes) return error.LakeWriteTooLarge;
    }
    const batch: Batch = .{ .batch_id = batch_id, .source = source, .epoch = epoch, .checkpoint = checkpoint, .expected_checkpoint = previous, .key_fields = keys, .changes = changes.items };
    try batch.validate(max_group_changes);
    return batch;
}

fn keyImage(a: A, keys: []const []const u8, row: V) !V {
    if (row != .object) return error.InvalidReplicationSourceRow;
    var key: std.json.ObjectMap = .empty;
    for (keys) |name| {
        const value = row.object.get(name) orelse return error.ReplicationProjectionIncomplete;
        if (value == .null) return error.ReplicationProjectionIncomplete;
        try key.put(a, name, value);
    }
    return .{ .object = key };
}

/// The serving owner supplies qualified transport and rechecks the live
/// table incarnation and PostgreSQL cutover throughout catalog work.
pub const StreamOwner = struct {
    const Port = @import("../metadata/replication_backfill.zig").ExternalStreamDestination;

    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, A, Port.Request) anyerror!Options,
    check: *const fn (*anyopaque, Port.Request) anyerror!void,

    pub fn port(self: *StreamOwner) Port {
        return .{ .ptr = self, .apply = apply, .bootstrap_committed = bootstrapCommitted, .stage_bootstrap = stageBootstrap, .stage_snapshot_page = stageSnapshotPage, .retire_bootstrap = retireBootstrap };
    }

    const Guard = struct {
        owner: *StreamOwner,
        request: Port.Request,

        fn checkpoint(raw: *anyopaque) !void {
            const self: *Guard = @ptrCast(@alignCast(raw));
            try self.owner.check(self.owner.ptr, self.request);
        }
    };

    fn apply(raw: *anyopaque, backing: A, request: Port.Request) !Port.Outcome {
        const self: *StreamOwner = @ptrCast(@alignCast(raw));
        return self.execute(backing, request, .apply, null);
    }

    fn bootstrapCommitted(raw: *anyopaque, backing: A, request: Port.Request) !bool {
        const self: *StreamOwner = @ptrCast(@alignCast(raw));
        return (try self.execute(backing, request, .observe_bootstrap, null)) == .committed;
    }

    fn stageBootstrap(raw: *anyopaque, backing: A, request: Port.Request) !void {
        const self: *StreamOwner = @ptrCast(@alignCast(raw));
        _ = try self.execute(backing, request, .stage_bootstrap, null);
    }

    fn stageSnapshotPage(raw: *anyopaque, backing: A, request: Port.Request) ![32]u8 {
        const self: *StreamOwner = @ptrCast(@alignCast(raw));
        var digest: [32]u8 = undefined;
        _ = try self.execute(backing, request, .stage_page, &digest);
        return digest;
    }

    fn retireBootstrap(raw: *anyopaque, backing: A, request: Port.Request) !void {
        const self: *StreamOwner = @ptrCast(@alignCast(raw));
        _ = try self.execute(backing, request, .retire_bootstrap, null);
    }

    fn execute(self: *StreamOwner, backing: A, request: Port.Request, mode: enum { apply, observe_bootstrap, stage_bootstrap, stage_page, retire_bootstrap }, page_digest: ?*[32]u8) !Port.Outcome {
        if (request.table.table_id == 0 or request.table.table_id != request.cutover.table_id or request.cutover.cutover_authority_id == 0) return error.UnsupportedReplicationRoute;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var guard: Guard = .{ .owner = self, .request = request };
        const context: Context = .{ .deadline_ns = request.deadline_ns, .checkpoint = .{ .ptr = &guard, .check = Guard.checkpoint } };
        try context.ensureActive();
        var bound = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, request.table.schema_json)) orelse return error.UnsupportedReplicationRoute;
        defer bound.deinit(a);
        try bound.binding.validateSupported();
        if (bound.binding.format != .iceberg or bound.binding.write_policy != .iceberg_writer) return error.UnsupportedReplicationRoute;
        const identity = bound.binding.row_identity orelse return error.UnsupportedReplicationRoute;
        const options = try self.resolve(self.ptr, a, request);
        if (options.catalog_table_id != request.table.table_id or options.catalog_generation != request.table.object_storage_generation) return error.CatalogGenerationChanged;
        const source = try std.fmt.allocPrint(a, "postgres:{d}:{d}", .{ request.table.table_id, request.cutover.source_ordinal });
        const epoch = try std.fmt.allocPrint(a, "{d}", .{request.cutover.cutover_authority_id});
        if (mode == .retire_bootstrap) {
            const retired = request.cutover.retired_cutover_authority_id;
            if (retired == 0 or retired == request.cutover.cutover_authority_id) return error.InvalidReplicationCutoverIntent;
            var opened = try openQueue(a, bound.binding, options);
            defer opened.deinit();
            const base = try prefix(a, opened.prefix, bound.binding, options);
            const scope: BootstrapRetirement = .{ .source_ordinal = request.cutover.source_ordinal, .authority = retired };
            const retired_epoch = try std.fmt.allocPrint(a, "{d}", .{retired});
            const locator = try bootstrapLocatorKey(a, base, source, retired_epoch);
            const marker = try bootstrapRetirementKey(a, base, locator);
            const body = try std.json.Stringify.valueAlloc(a, scope, .{});
            try writeImmutableBootstrap(&opened, context, marker, body);
            if (!try sweepRetiredBootstrap(a, &opened, context, base, request.table.table_id, marker, 16)) return error.LakeWriteTooLarge;
            return .accepted;
        }
        if (mode == .observe_bootstrap) {
            if (request.checkpoint.len == 0 or !std.mem.eql(u8, request.checkpoint, request.cutover.prepared_checkpoint)) return error.InvalidReplicationCheckpoint;
            var current = try configured.executeLakeCatalogAlloc(a, bound.binding, options, context, .load);
            defer current.deinit(a);
            const properties = try catalogProperties(try catalog.metadata.parse(a, current.table.metadata_json));
            const covered = try propertyCoverage(properties);
            const marker = properties.get("antfly.cdc.bootstrap-cut") orelse {
                if (covered != 0) return .accepted;
                const outcome = try recoverPendingBootstrap(a, bound.binding, options, context, source, epoch, request.checkpoint, identity.key_columns);
                return if (outcome) .committed else .accepted;
            };
            if (!std.mem.eql(u8, try catalog.metadata.str(marker), request.checkpoint) or covered == 0) return .accepted;
            try requireSourceProperties(a, properties, .{
                .batch_id = "",
                .source = source,
                .epoch = epoch,
                .checkpoint = request.checkpoint,
                .key_fields = identity.key_columns,
                .changes = &.{},
            });
            const bootstrap = try bootstrapIdentity(a, source, epoch, request.checkpoint, identity.key_columns);
            try removeBootstrapStage(a, bound.binding, options, context, bootstrap);
            try context.ensureActive();
            return .committed;
        }
        if (mode == .stage_page or request.snapshot_chunks != null) {
            if (request.previous_checkpoint != null or request.changes.len != 0 or request.checkpoint.len == 0 or
                !std.mem.eql(u8, request.checkpoint, request.cutover.prepared_checkpoint)) return error.InvalidReplicationCheckpoint;
            const snapshot = try bootstrapIdentity(a, source, epoch, request.checkpoint, identity.key_columns);
            if (mode == .stage_page) {
                if (request.snapshot_chunks != null) return error.InvalidReplicationCheckpoint;
                const rows = request.snapshot_rows orelse return error.InvalidReplicationCheckpoint;
                page_digest.?.* = try writeBootstrapPage(a, bound.binding, options, context, snapshot, rows);
            } else {
                if (mode != .stage_bootstrap or request.snapshot_rows != null) return error.InvalidReplicationCheckpoint;
                try sealBootstrapPages(a, bound.binding, options, context, snapshot, request.snapshot_chunks.?, request.cutover.snapshot_offset);
            }
            return .accepted;
        }
        if (mode == .stage_bootstrap and request.snapshot_rows == null) return error.InvalidReplicationCheckpoint;
        const batch = if (request.snapshot_rows) |rows| blk: {
            if (request.previous_checkpoint != null or request.changes.len != 0 or
                !std.mem.eql(u8, request.checkpoint, request.cutover.prepared_checkpoint)) return error.InvalidReplicationCheckpoint;
            if (rows.len > catalog.row_commit.max_bootstrap_rows) return error.LakeWriteTooLarge;
            const changes = try a.alloc(Batch.Change, rows.len);
            for (rows, changes) |row, *change| change.* = .{ .op = .upsert, .row = row };
            var snapshot = try bootstrapIdentity(a, source, epoch, request.checkpoint, identity.key_columns);
            snapshot.changes = changes;
            try snapshot.validate(catalog.row_commit.max_bootstrap_rows);
            break :blk snapshot;
        } else blk: {
            const previous = request.previous_checkpoint orelse return error.InvalidReplicationCheckpoint;
            break :blk try postgresBatch(a, source, epoch, previous, request.checkpoint, identity.key_columns, request.changes);
        };
        if (mode == .stage_bootstrap) {
            try writeBootstrapStage(a, bound.binding, options, context, batch);
            return .accepted;
        }
        const outcome = applyBatch(a, bound.binding, options, context, batch) catch |err| switch (err) {
            error.LakeCommitOutcomeUnknown => (try observeBatch(a, bound.binding, options, context, batch)) orelse return err,
            else => return err,
        };
        if (outcome.committed and batch.bootstrap) retireBootstrapStage(a, bound.binding, options, context, batch);
        return if (outcome.committed) .committed else .accepted;
    }
};

fn bootstrapIdentity(a: A, source: []const u8, epoch: []const u8, checkpoint: []const u8, keys: []const []const u8) !Batch {
    return .{
        .batch_id = try std.fmt.allocPrint(a, "antfly-bootstrap-{s}", .{&catalog.types.digestHex(checkpoint)}),
        .source = source,
        .epoch = epoch,
        .checkpoint = checkpoint,
        .key_fields = keys,
        .changes = &.{},
        .bootstrap = true,
    };
}

// The committed cut and WAL receipt are sufficient for recovery once the
// stage is retired. Cleanup failure must not change the committed outcome.
fn retireBootstrapStage(a: A, binding: Binding, options: Options, context: Context, batch: Batch) void {
    removeBootstrapStage(a, binding, options, context, batch) catch |err|
        std.log.warn("Iceberg bootstrap stage retirement deferred table_id={d} err={s}", .{ options.catalog_table_id, @errorName(err) });
}

fn removeBootstrapStage(a: A, binding: Binding, options: Options, context: Context, batch: Batch) !void {
    try context.ensureActive();
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const locator_key = try bootstrapLocatorKey(a, base, batch.source, batch.epoch);
    if (try readBootstrapLocator(a, &opened, context, locator_key)) |checkpoint| {
        if (!std.mem.eql(u8, checkpoint, batch.checkpoint)) return error.InvalidReplicationCheckpoint;
    }
    const stage = try bootstrapStageKey(a, base, batch);
    if (!try removeBootstrapStageObjects(a, &opened, context, stage, 16)) return error.LakeWriteTooLarge;
    try removeBootstrapObject(&opened, context, locator_key);
}

fn removeBootstrapStageObjects(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, stage: []const u8, max_pages: usize) !bool {
    const manifest = try std.fmt.allocPrint(a, "{s}.manifest", .{stage});
    const pages_prefix = try std.fmt.allocPrint(a, "{s}.pages/", .{stage});
    const cancellation = catalog.types.contextCancellation(&context);
    var client = opened.client;
    // The retirement proof covers the entire exact cut. Pages omitted from
    // the sealed manifest can still exist after an interrupted capture.
    // Re-list the front of the prefix after deletion so retries need no
    // continuation token for an inventory that is being mutated.
    for (0..max_pages) |_| {
        try context.ensureActive();
        var page = try client.listObjects(opened.bucket, .{
            .prefix = pages_prefix,
            .recursive = true,
            .max_keys = 128,
            .cancellation = cancellation,
        });
        defer page.deinit(client.allocator);
        for (page.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, pages_prefix)) return error.InvalidReplicationCheckpoint;
            const suffix = entry.key[pages_prefix.len..];
            var digest: [32]u8 = undefined;
            if (suffix.len != digest.len * 2) return error.InvalidReplicationCheckpoint;
            _ = std.fmt.hexToBytes(&digest, suffix) catch return error.InvalidReplicationCheckpoint;
            try removeBootstrapObject(opened, context, entry.key);
        }
        if (page.next_continuation_token == null) {
            try removeBootstrapObject(opened, context, stage);
            try removeBootstrapObject(opened, context, manifest);
            return true;
        }
    }
    return false;
}

fn removeBootstrapObject(opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, key: []const u8) !void {
    var client = opened.client;
    const cancellation = catalog.types.contextCancellation(&context);
    var current = client.statObjectWithOptions(opened.bucket, key, .{ .cancellation = cancellation }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return,
        else => return err,
    };
    defer current.deinit(client.allocator);
    try context.ensureActive();
    client.compareAndDelete(opened.bucket, key, current.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = cancellation }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
}

fn bootstrapStageKey(a: A, base: []const u8, batch: Batch) ![]u8 {
    // WAL initialization owns its prefix and rejects pre-existing objects.
    return std.fmt.allocPrint(a, "{s}-bootstrap/{s}.json", .{ base, &std.fmt.bytesToHex(try batch.identity(a), .lower) });
}

/// The stage key remains compatible with previously staged snapshots. The
/// locator gives the retirement owner a way to find the stage even when
/// capture stopped before writing its manifest.
fn bootstrapLocatorKey(a: A, base: []const u8, source: []const u8, epoch: []const u8) ![]u8 {
    const scope = try std.json.Stringify.valueAlloc(a, .{ source, epoch }, .{});
    defer a.free(scope);
    const authority = catalog.types.digestHex(scope);
    return std.fmt.allocPrint(a, "{s}-bootstrap/authorities/{s}", .{ base, &authority });
}

const max_bootstrap_checkpoint_bytes = 1024;

fn readBootstrapLocator(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, key: []const u8) !?[]const u8 {
    try context.ensureActive();
    var client = opened.client;
    var object = client.getObject(opened.bucket, key, .{
        .max_response_bytes = max_bootstrap_checkpoint_bytes,
        .cancellation = catalog.types.contextCancellation(&context),
    }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return null,
        error.ResponseTooLarge => return error.InvalidReplicationCheckpoint,
        else => return err,
    };
    defer object.deinit(client.allocator);
    if (object.body.len == 0 or object.body.len > max_bootstrap_checkpoint_bytes) return error.InvalidReplicationCheckpoint;
    return try a.dupe(u8, object.body);
}

const BootstrapRetirement = struct {
    source_ordinal: u32,
    authority: u64,
};

fn bootstrapRetirementKey(a: A, base: []const u8, locator: []const u8) ![]u8 {
    const slash = std.mem.lastIndexOfScalar(u8, locator, '/') orelse unreachable;
    return std.fmt.allocPrint(a, "{s}-bootstrap/retired/{s}", .{ base, locator[slash + 1 ..] });
}

fn sweepRetiredBootstrap(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, base: []const u8, table_id: u64, marker: []const u8, max_pages: usize) !bool {
    try context.ensureActive();
    var client = opened.client;
    var object = client.getObject(opened.bucket, marker, .{
        .max_response_bytes = 128,
        .cancellation = catalog.types.contextCancellation(&context),
    }) catch |err| switch (err) {
        error.ResponseTooLarge => return error.InvalidReplicationCutoverIntent,
        else => return err,
    };
    defer object.deinit(client.allocator);
    const scope = std.json.parseFromSliceLeaky(BootstrapRetirement, a, object.body, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidReplicationCutoverIntent,
    };
    if (scope.authority == 0) return error.InvalidReplicationCutoverIntent;
    const source = try std.fmt.allocPrint(a, "postgres:{d}:{d}", .{ table_id, scope.source_ordinal });
    const epoch = try std.fmt.allocPrint(a, "{d}", .{scope.authority});
    const locator = try bootstrapLocatorKey(a, base, source, epoch);
    if (!std.mem.eql(u8, marker, try bootstrapRetirementKey(a, base, locator))) return error.InvalidReplicationCutoverIntent;
    if (try readBootstrapLocator(a, opened, context, locator)) |checkpoint| {
        // Row keys do not participate in the bootstrap stage identity.
        const bootstrap = try bootstrapIdentity(a, source, epoch, checkpoint, &.{});
        const stage = try bootstrapStageKey(a, base, bootstrap);
        return removeBootstrapStageObjects(a, opened, context, stage, max_pages);
    }
    return true;
}

/// Rescan retirement markers cyclically. A put that finishes after its
/// marker is swept will be collected when the durable cursor wraps.
pub fn sweepBootstrapRetirements(backing: A, binding: Binding, options: Options, context: Context) !?u64 {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    return sweepBootstrapRetirementPage(a, &opened, context, base, options.catalog_table_id);
}

fn sweepBootstrapRetirementPage(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, base: []const u8, table_id: u64) !?u64 {
    try context.ensureActive();
    const directory = try std.fmt.allocPrint(a, "{s}-bootstrap/retired/", .{base});
    const cursor_key = try std.fmt.allocPrint(a, "{s}-bootstrap/retired-cursor", .{base});
    const cancellation = catalog.types.contextCancellation(&context);
    var client = opened.client;
    var saved = client.getObject(opened.bucket, cursor_key, .{
        .max_response_bytes = directory.len + 64,
        .cancellation = cancellation,
    }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    const after = if (saved) |value| value.body else "";
    if (after.len != 0 and (!std.mem.startsWith(u8, after, directory) or after.len != directory.len + 64))
        return error.InvalidReplicationCutoverIntent;
    var page = try client.listObjects(opened.bucket, .{
        .prefix = directory,
        .start_after = if (after.len == 0) null else after,
        .max_keys = 1,
        .cancellation = cancellation,
    });
    defer page.deinit(client.allocator);
    if (page.entries.len == 0 and after.len == 0) return null;
    const next = if (page.entries.len == 0) "" else blk: {
        const key = page.entries[0].key;
        if (!std.mem.startsWith(u8, key, directory) or key.len != directory.len + 64)
            return error.InvalidReplicationCutoverIntent;
        const complete = sweepRetiredBootstrap(a, opened, context, base, table_id, key, 1) catch |err| switch (err) {
            error.InvalidReplicationCutoverIntent, error.InvalidReplicationCheckpoint => blk_complete: {
                // Keep the marker for diagnosis and revisit it on the next
                // cycle so malformed state cannot strand other scopes.
                std.log.warn("invalid Iceberg bootstrap retirement table_id={d} err={s}", .{ table_id, @errorName(err) });
                break :blk_complete true;
            },
            else => return err,
        };
        if (!complete) return 1000;
        break :blk if (page.next_continuation_token == null) "" else key;
    };
    if (!std.mem.eql(u8, next, after)) {
        try context.ensureActive();
        var updated = client.compareAndPut(opened.bucket, cursor_key, next, if (saved) |value| value.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = cancellation }) catch |err| switch (err) {
            error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => return 1000,
            else => return err,
        };
        updated.deinit(client.allocator);
    }
    return if (next.len == 0) 60_000 else 1000;
}

fn requireBootstrapStageOpen(opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, marker: []const u8) !void {
    var client = opened.client;
    var object = client.statObjectWithOptions(opened.bucket, marker, .{
        .cancellation = catalog.types.contextCancellation(&context),
    }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return,
        else => return err,
    };
    defer object.deinit(client.allocator);
    return error.ReplicationCutoverAuthorityLost;
}

fn writeBootstrapStageObject(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, base: []const u8, batch: Batch, object: []const u8, bytes: []const u8) !void {
    const key = try bootstrapLocatorKey(a, base, batch.source, batch.epoch);
    defer a.free(key);
    if (batch.checkpoint.len == 0 or batch.checkpoint.len > max_bootstrap_checkpoint_bytes) return error.InvalidReplicationCheckpoint;
    const marker = try bootstrapRetirementKey(a, base, key);
    defer a.free(marker);
    try requireBootstrapStageOpen(opened, context, marker);
    try writeImmutableBootstrap(opened, context, key, batch.checkpoint);
    try requireBootstrapStageOpen(opened, context, marker);
    try writeImmutableBootstrap(opened, context, object, bytes);
    try requireBootstrapStageOpen(opened, context, marker);
}

const max_bootstrap_pages = 1024;
// Each digest is a JSON array of 32 decimal bytes, with commas and brackets.
const max_bootstrap_manifest_bytes = 2 + max_bootstrap_pages * (32 * 4 + 2);

fn writeBootstrapPage(a: A, binding: Binding, options: Options, context: Context, batch: Batch, rows: []const V) ![32]u8 {
    if (rows.len == 0 or rows.len > max_group_changes) return error.LakeWriteTooLarge;
    const bytes = try std.json.Stringify.valueAlloc(a, rows, .{});
    if (bytes.len > catalog.types.max_commit_bytes) return error.LakeWriteTooLarge;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const stage = try bootstrapStageKey(a, base, batch);
    const page = try std.fmt.allocPrint(a, "{s}.pages/{s}", .{ stage, &std.fmt.bytesToHex(digest, .lower) });
    try writeBootstrapStageObject(a, &opened, context, base, batch, page, bytes);
    return digest;
}

fn sealBootstrapPages(a: A, binding: Binding, options: Options, context: Context, batch: Batch, pages: []const [32]u8, expected_rows: u64) !void {
    if (pages.len > max_bootstrap_pages) return error.LakeWriteTooLarge;
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const stage = try bootstrapStageKey(a, base, batch);
    const manifest = try std.fmt.allocPrint(a, "{s}.manifest", .{stage});
    var client = opened.client;
    var total_bytes: usize = 0;
    var total_rows: usize = 0;
    for (pages) |digest| {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var page = try readBootstrapPage(scratch.allocator(), &client, opened.bucket, context, stage, digest, catalog.types.max_commit_bytes - total_bytes);
        defer page.deinit(client.allocator);
        if (page.body.len > catalog.types.max_commit_bytes - total_bytes) return error.LakeWriteTooLarge;
        total_bytes += page.body.len;
        const rows = try std.json.parseFromSliceLeaky([]const V, scratch.allocator(), page.body, .{});
        if (rows.len == 0 or rows.len > max_group_changes or rows.len > catalog.row_commit.max_bootstrap_rows - total_rows)
            return error.InvalidReplicationCheckpoint;
        total_rows += rows.len;
    }
    if (total_rows != expected_rows) return error.InvalidReplicationCheckpoint;
    const bytes = try std.json.Stringify.valueAlloc(a, pages, .{});
    try writeBootstrapStageObject(a, &opened, context, base, batch, manifest, bytes);
}

fn readBootstrapPage(a: A, client: *@import("objectstore").Client, bucket: []const u8, context: Context, stage: []const u8, digest: [32]u8, remaining_bytes: usize) !@import("objectstore").GetResult {
    try context.ensureActive();
    if (remaining_bytes == 0) return error.LakeWriteTooLarge;
    const key = try std.fmt.allocPrint(a, "{s}.pages/{s}", .{ stage, &std.fmt.bytesToHex(digest, .lower) });
    defer a.free(key);
    var page = client.getObject(bucket, key, .{ .max_response_bytes = remaining_bytes, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.ResponseTooLarge => return error.LakeWriteTooLarge,
        else => return err,
    };
    errdefer page.deinit(client.allocator);
    var actual: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(page.body, &actual, .{});
    if (!std.mem.eql(u8, &digest, &actual)) return error.ArtifactIntegrityMismatch;
    return page;
}

fn readBootstrapPages(a: A, opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, stage: []const u8, expected: Batch) !?Batch {
    var client = opened.client;
    const cancellation = catalog.types.contextCancellation(&context);
    const manifest = try std.fmt.allocPrint(a, "{s}.manifest", .{stage});
    var root = client.getObject(opened.bucket, manifest, .{ .max_response_bytes = max_bootstrap_manifest_bytes, .cancellation = cancellation }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return null,
        else => return err,
    };
    defer root.deinit(client.allocator);
    const pages = try std.json.parseFromSliceLeaky([]const [32]u8, a, root.body, .{});
    if (pages.len > max_bootstrap_pages) return error.LakeWriteTooLarge;
    var changes: std.ArrayList(Batch.Change) = .empty;
    var total_bytes: usize = 0;
    for (pages) |digest| {
        var page = try readBootstrapPage(a, &client, opened.bucket, context, stage, digest, catalog.types.max_commit_bytes - total_bytes);
        defer page.deinit(client.allocator);
        if (page.body.len > catalog.types.max_commit_bytes - total_bytes) return error.LakeWriteTooLarge;
        total_bytes += page.body.len;
        const rows = try std.json.parseFromSliceLeaky([]const V, a, page.body, .{ .allocate = .alloc_always });
        if (rows.len > catalog.row_commit.max_bootstrap_rows - changes.items.len) return error.LakeWriteTooLarge;
        for (rows) |row| try changes.append(a, .{ .op = .upsert, .row = row });
    }
    var batch = expected;
    batch.changes = changes.items;
    try batch.validate(catalog.row_commit.max_bootstrap_rows);
    return batch;
}

fn writeBootstrapStage(a: A, binding: Binding, options: Options, context: Context, batch: Batch) !void {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    const key = try bootstrapStageKey(a, base, batch);
    const bytes = try std.json.Stringify.valueAlloc(a, batch, .{});
    if (bytes.len > catalog.types.max_commit_bytes) return error.LakeWriteTooLarge;
    try writeBootstrapStageObject(a, &opened, context, base, batch, key, bytes);
}

fn writeImmutableBootstrap(opened: *local.serverless_object_store_support.OpenedObjectStore, context: Context, key: []const u8, bytes: []const u8) !void {
    var client = opened.client;
    try context.ensureActive();
    var stored = client.putObject(opened.bucket, key, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => {
            var existing = client.getObject(opened.bucket, key, .{ .max_response_bytes = bytes.len, .cancellation = catalog.types.contextCancellation(&context) }) catch |read_err| switch (read_err) {
                error.ResponseTooLarge => return error.WalIdempotencyConflict,
                else => return read_err,
            };
            defer existing.deinit(client.allocator);
            if (!std.mem.eql(u8, bytes, existing.body)) return error.WalIdempotencyConflict;
            return context.ensureActive();
        },
        else => return err,
    };
    defer stored.deinit(client.allocator);
    try context.ensureActive();
}

/// Resume only the first admitted bootstrap for the requested incarnation.
fn recoverPendingBootstrap(a: A, binding: Binding, unresolved: Options, context: Context, source: []const u8, epoch: []const u8, checkpoint: []const u8, keys: []const []const u8) !bool {
    const options = try unresolved.resolveOwner(a, context);
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    const expected = try bootstrapIdentity(a, source, epoch, checkpoint, keys);
    const record = try store.next(a, 0);
    const bytes = if (record) |accepted| accepted.payload else blk: {
        var client = opened.client;
        const key = try bootstrapStageKey(a, base, expected);
        if (try readBootstrapPages(a, &opened, context, key, expected)) |assembled| {
            break :blk try std.json.Stringify.valueAlloc(a, assembled, .{});
        }
        var staged = client.getObject(opened.bucket, key, .{ .max_response_bytes = catalog.types.max_commit_bytes, .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return false,
            else => return err,
        };
        defer staged.deinit(client.allocator);
        break :blk try a.dupe(u8, staged.body);
    };
    const batch = try std.json.parseFromSliceLeaky(Batch, a, bytes, .{});
    if (!batch.bootstrap or batch.expected_checkpoint != null or
        !std.mem.eql(u8, batch.batch_id, expected.batch_id) or !std.mem.eql(u8, batch.source, source) or
        !std.mem.eql(u8, batch.epoch, epoch) or !std.mem.eql(u8, batch.checkpoint, checkpoint) or
        !keyFieldsEqual(batch.key_fields, keys)) return false;
    if (record) |accepted| {
        const observed = (try observeBatch(a, binding, options, context, batch)) orelse return error.LakeWalCoverageGap;
        if (observed.accepted_sequence != accepted.lsn) return error.LakeWalCoverageGap;
    }
    const outcome = try applyBatch(a, binding, options, context, batch);
    if (record) |accepted| if (outcome.accepted_sequence != accepted.lsn) return error.LakeWalCoverageGap;
    if (outcome.committed) try removeBootstrapStage(a, binding, options, context, batch);
    return outcome.committed;
}

pub const BatchOutcome = struct {
    accepted_sequence: u64,
    committed: bool,
};

/// Admit an exact batch and give the existing durable catalog worker one
/// bounded opportunity to commit it. A retry reconciles the original WAL
/// receipt and catalog intent. Callers may checkpoint only a committed result.
pub fn applyBatch(a: A, binding: Binding, unresolved_options: Options, context: Context, batch: Batch) !BatchOutcome {
    const options = try unresolved_options.resolveOwner(a, context);
    const sequence = try acceptResolvedBatch(a, binding, options, context, batch);
    const observed = (try observeBatch(a, binding, options, context, batch)) orelse return error.LakeWalCoverageGap;
    if (observed.accepted_sequence != sequence) return error.LakeWalCoverageGap;
    if (observed.committed) return observed;

    _ = try drain(a, binding, options, context);
    const refreshed = (try observeBatch(a, binding, options, context, batch)) orelse return error.LakeWalCoverageGap;
    if (refreshed.accepted_sequence != sequence) return error.LakeWalCoverageGap;
    return refreshed;
}

/// Resolve an exact admission from durable WAL identity and confirm whether
/// the catalog has committed its sequence. Absence never advances progress.
pub fn observeBatch(a: A, binding: Binding, unresolved_options: Options, context: Context, batch: Batch) !?BatchOutcome {
    const options = try unresolved_options.resolveOwner(a, context);
    const payload = try std.json.Stringify.valueAlloc(a, batch, .{});
    defer a.free(payload);
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    const accepted = (try observedSequence(arena.allocator(), store, batch, payload)) orelse return null;
    var current = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer current.deinit(a);
    const metadata = try catalog.metadata.parse(arena.allocator(), current.table.metadata_json);
    const properties = try catalogProperties(metadata);
    const covered = try propertyCoverage(properties);
    if (covered >= accepted) {
        try requireSourceProperties(arena.allocator(), properties, batch);
        if (covered == accepted) {
            const checkpoint = properties.get("antfly.cdc.checkpoint") orelse return error.LakeCheckpointConflict;
            if (!std.mem.eql(u8, try catalog.metadata.str(checkpoint), batch.checkpoint)) return error.LakeCheckpointConflict;
        }
    }
    try context.ensureActive();
    return .{ .accepted_sequence = accepted, .committed = covered >= accepted };
}

fn observedSequence(a: A, store: wal.Store, batch: Batch, payload: []const u8) !?u64 {
    const operation_id = &std.fmt.bytesToHex(try batch.identity(a), .lower);
    var admission = try store.beginAdmission(a, operation_id, payload);
    defer admission.deinit();
    return admission.observation.accepted;
}

fn acceptResolvedBatch(a: A, binding: Binding, options: Options, context: Context, batch: Batch) !u64 {
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
    const operation_id = &std.fmt.bytesToHex(try batch.identity(scratch), .lower);
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
        const props = try catalogProperties(metadata.value);
        const checkpoint = props.get("antfly.cdc.checkpoint");
        if ((checkpoint == null) != (batch.expected_checkpoint == null)) return error.LakeCheckpointConflict;
        if (checkpoint) |v| if (!std.mem.eql(u8, try catalog.metadata.str(v), batch.expected_checkpoint.?)) return error.LakeCheckpointConflict;
        if (props.contains("antfly.cdc.source")) try requireSourceProperties(scratch, props, batch);
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
/// Checkpoint-only records consume no row budget; bound their work separately.
pub const max_group_records: usize = 1024;

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
        return self.result.records >= max_group_records or count >= max_changes or self.bytes >= max_bytes;
    }
    fn add(self: *GroupBuilder, a: A, record: wal.Record, max_changes: usize, max_bytes: usize) !bool {
        if (self.result.batch.bootstrap or self.full(max_changes, max_bytes) or record.lsn != self.result.last_lsn + 1) return false;
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

pub const CommittedWalInterval = struct {
    previous: u64,
    through: u64,
    snapshot: i64,
    parent: ?i64,
    schema: i64,
    source: []const u8,
    epoch: []const u8,
    batch: []const u8,

    fn read(snapshot: V) !?CommittedWalInterval {
        if (snapshot != .object) return error.InvalidLakeMetadata;
        const summary = snapshot.object.get("summary") orelse return null;
        if (summary != .object) return error.LakeWalCoverageGap;
        const lower = summary.object.get("antfly.wal.previous-coverage") orelse return null;
        const previous = std.fmt.parseInt(u64, try catalog.metadata.str(lower), 10) catch return error.LakeWalCoverageGap;
        const through = std.fmt.parseInt(u64, try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.wal.coverage")), 10) catch return error.LakeWalCoverageGap;
        if (through <= previous) return error.LakeWalCoverageGap;
        return .{
            .previous = previous,
            .through = through,
            .snapshot = try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id")),
            .parent = if (snapshot.object.get("parent-snapshot-id")) |parent| try catalog.metadata.int(parent) else null,
            .schema = try catalog.metadata.int(try catalog.metadata.get(snapshot, "schema-id")),
            .source = try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.cdc.source")),
            .epoch = try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.cdc.epoch")),
            .batch = try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.batch-id")),
        };
    }
};

fn verifyCommittedWalInterval(a: A, table: catalog.types.Table, body: []const u8, previous: u64) !void {
    const intent = try std.json.parseFromSliceLeaky(V, a, body, .{});
    const planned = for ((try catalog.metadata.get(intent, "updates")).array.items) |update| {
        if (std.mem.eql(u8, try catalog.metadata.str(try catalog.metadata.get(update, "action")), "add-snapshot"))
            break try catalog.metadata.get(update, "snapshot");
    } else return error.LakeWalCoverageGap;
    // Saved intents from before interval provenance still use the original
    // committed coverage check.
    const expected = (try CommittedWalInterval.read(planned)) orelse return;
    if (expected.previous != previous) return error.LakeWalCoverageGap;
    const root = try catalog.metadata.parse(a, table.metadata_json);
    for ((try catalog.metadata.get(root, "snapshots")).array.items) |snapshot| {
        if (try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id")) != expected.snapshot) continue;
        const committed = (try CommittedWalInterval.read(snapshot)) orelse return error.LakeWalCoverageGap;
        if (committed.previous != expected.previous or committed.through != expected.through or
            committed.parent != expected.parent or committed.schema != expected.schema or
            !std.mem.eql(u8, committed.source, expected.source) or
            !std.mem.eql(u8, committed.epoch, expected.epoch) or
            !std.mem.eql(u8, committed.batch, expected.batch)) return error.LakeWalCoverageGap;
        return;
    }
    return error.LakeWalCoverageGap;
}

/// Fence snapshot retirement before publishing the durable WAL pin. A lease
/// that loses either admission race cannot become a public feed cursor.
pub fn pinCommittedWalPosition(a: A, binding: Binding, options: Options, context: Context, owner: [16]u8, position: CommittedWalPosition, expiry_seconds: u64) !void {
    const time = @import("antfly_platform").time;
    const now = time.realtimeNs();
    const expiry_ns = try std.math.mul(u64, expiry_seconds, std.time.ns_per_s);
    if (expiry_ns <= now) return error.ChangefeedLeaseExpired;
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    if (position.snapshot) |snapshot| {
        const pins = @import("lake_snapshot_pins.zig");
        const path = try pins.namespace(a, opened.prefix, binding, position.table_uuid);
        defer a.free(path);
        const id = try std.fmt.allocPrint(a, "{d}", .{snapshot});
        defer a.free(id);
        const snapshots: pins.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = path, .context = context };
        _ = try snapshots.acquireFor(a, id, now, @min(expiry_ns - now, std.time.ns_per_day));
    }
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |token| token.token else null };
    try store.pinRetention(a, owner, position.through, position.snapshot, expiry_ns / std.time.ns_per_ms);
}

pub fn retireCommittedWalPin(a: A, binding: Binding, options: Options, context: Context, owner: [16]u8, expiry_seconds: u64) !void {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |token| token.token else null };
    try store.retireRetentionPin(a, owner, expiry_seconds);
}

pub fn pinnedWalFloor(a: A, binding: Binding, options: Options, context: Context, floor: u64, now_ms: u64, anchors: *std.ArrayList([]const u8)) !u64 {
    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    return store.retainedCut(a, floor, now_ms, anchors);
}

/// Apply the catalog history window before advancing queue GC. All WAL
/// groups in the window retain their lower bound, including checkpoint-only
/// commits. Index and reader leases can hold the resulting floor back further.
pub fn retainedWalFloor(a: A, root: V, committed: u64, now_ms: u64, retain_ms: u64, keep_latest: usize) !u64 {
    const snapshots = try catalog.metadata.get(root, "snapshots");
    if (snapshots != .array) return error.InvalidLakeMetadata;
    var floor = committed;
    var positions: std.ArrayList(u64) = .empty;
    defer positions.deinit(a);
    for (snapshots.array.items) |snapshot| {
        const interval = (try CommittedWalInterval.read(snapshot)) orelse continue;
        if (interval.through > committed) return error.LakeWalCoverageGap;
        const timestamp = try catalog.metadata.int(try catalog.metadata.get(snapshot, "timestamp-ms"));
        if (timestamp < 0) return error.InvalidLakeMetadata;
        if (@as(u64, @intCast(timestamp)) >= now_ms -| retain_ms) {
            floor = @min(floor, interval.previous);
        }
        try positions.append(a, interval.previous);
    }
    // Order by WAL position so a reordered snapshot array or a compaction
    // cannot displace the source commits protected by the minimum count.
    std.mem.sortUnstable(u64, positions.items, {}, comptime std.sort.desc(u64));
    const retained = @min(positions.items.len, @max(keep_latest, 1));
    if (retained > 0) floor = @min(floor, positions.items[retained - 1]);
    return floor;
}

/// Snapshot expiration must preserve both images of every replayable WAL
/// group. In particular the predecessor can be older than the WAL window.
pub fn protectRetainedWalSnapshots(a: A, root: V, floor: u64, protected: *std.ArrayList([]const u8)) !void {
    var by_id = try indexSnapshots(a, root);
    defer by_id.deinit();
    const start = protected.items.len;
    var keep = start;
    var oldest_parent: ?i64 = null;
    var current = try mainSnapshot(root);
    for (0..by_id.count()) |_| {
        const id = current orelse break;
        const snapshot = by_id.get(id) orelse break;
        const parent = try parentSnapshot(snapshot);
        if (try CommittedWalInterval.read(snapshot)) |interval| {
            if (interval.previous < floor) break;
            try protected.append(a, try std.fmt.allocPrint(a, "{d}", .{id}));
            keep = protected.items.len;
            oldest_parent = parent;
        } else if (try compactionCoverage(snapshot)) |preserved| {
            if (preserved < floor) break;
            try protected.append(a, try std.fmt.allocPrint(a, "{d}", .{id}));
        } else {
            break;
        }
        current = parent;
    }
    // Keep the oldest group's predecessor, including when it is a physical
    // rewrite. Earlier rewrites at the same coverage are outside replay.
    protected.shrinkRetainingCapacity(keep);
    if (oldest_parent) |parent| try protected.append(a, try std.fmt.allocPrint(a, "{d}", .{parent}));
}

pub const CommittedWalPosition = struct {
    table_uuid: []const u8,
    snapshot: ?i64,
    through: u64,
};

/// Capture the bootstrap boundary from one catalog observation. Accepted
/// transactions may advance independently of this catalog position.
pub fn captureCommittedWalPosition(a: A, table: catalog.types.Table) !CommittedWalPosition {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const root = try catalog.metadata.parse(scratch, table.metadata_json);
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    const snapshot = try mainSnapshot(root);
    const through = try propertyCoverage(try catalogProperties(root));
    if (snapshot) |id| {
        var by_id = try indexSnapshots(scratch, root);
        defer by_id.deinit();
        const current = by_id.get(id) orelse return error.ChangefeedResynchronizationRequired;
        const summary = current.object.get("summary");
        const value = if (summary != null and summary.? == .object) summary.?.object.get("antfly.wal.coverage") else null;
        const upper = if (value) |raw| try std.fmt.parseInt(u64, try catalog.metadata.str(raw), 10) else 0;
        if (upper != through) return error.LakeWalCoverageGap;
    } else if (through != 0) {
        return error.LakeWalCoverageGap;
    }
    return .{ .table_uuid = try a.dupe(u8, uuid), .snapshot = snapshot, .through = through };
}

/// Walk from the current catalog head back to an acknowledged position and
/// return its next logical commit. Physical rewrites preserve WAL coverage.
pub fn nextCommittedWalInterval(a: A, table: catalog.types.Table, after: CommittedWalPosition) !?CommittedWalInterval {
    const root = try catalog.metadata.parse(a, table.metadata_json);
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    if (!std.mem.eql(u8, uuid, after.table_uuid)) return error.ChangefeedResynchronizationRequired;
    var by_id = try indexSnapshots(a, root);
    defer by_id.deinit();
    var current = try mainSnapshot(root);
    var expected = try propertyCoverage(try catalogProperties(root));
    var next: ?CommittedWalInterval = null;
    for (0..by_id.count()) |_| {
        if (current == after.snapshot) break;
        const snapshot = by_id.get(current orelse return error.ChangefeedResynchronizationRequired) orelse return error.ChangefeedResynchronizationRequired;
        if (try CommittedWalInterval.read(snapshot)) |interval| {
            if (interval.through != expected) return error.LakeWalCoverageGap;
            expected = interval.previous;
            next = interval;
        } else {
            const preserved = (try compactionCoverage(snapshot)) orelse return error.ChangefeedResynchronizationRequired;
            if (preserved != expected) return error.LakeWalCoverageGap;
        }
        current = try parentSnapshot(snapshot);
    }
    if (current != after.snapshot or expected != after.through) return error.ChangefeedResynchronizationRequired;
    if (current) |id| if (!by_id.contains(id)) return error.ChangefeedResynchronizationRequired;
    return next;
}

fn compactionCoverage(snapshot: V) !?u64 {
    const summary = snapshot.object.get("summary") orelse return null;
    if (summary != .object or !summary.object.contains("antfly.compaction")) return null;
    if (!std.mem.eql(u8, try catalog.metadata.str(try catalog.metadata.get(summary, "operation")), "replace")) return error.InvalidLakeMetadata;
    return try std.fmt.parseInt(u64, try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.wal.coverage")), 10);
}

fn parentSnapshot(snapshot: V) !?i64 {
    const value = snapshot.object.get("parent-snapshot-id") orelse return null;
    return try catalog.metadata.int(value);
}

fn mainSnapshot(root: V) !?i64 {
    const value = try catalog.metadata.get(root, "current-snapshot-id");
    if (value == .null) return null;
    const id = try catalog.metadata.int(value);
    return if (id == -1) null else id;
}

fn indexSnapshots(a: A, root: V) !std.AutoHashMap(i64, V) {
    const snapshots = try catalog.metadata.get(root, "snapshots");
    if (snapshots != .array) return error.InvalidLakeMetadata;
    var result: std.AutoHashMap(i64, V) = .init(a);
    errdefer result.deinit();
    for (snapshots.array.items) |snapshot| {
        const id = try catalog.metadata.int(try catalog.metadata.get(snapshot, "snapshot-id"));
        const entry = try result.getOrPut(id);
        if (entry.found_existing) return error.InvalidLakeMetadata;
        entry.value_ptr.* = snapshot;
    }
    return result;
}

pub const CommittedWalGroup = struct {
    interval: CommittedWalInterval,
    group: Group,
};

/// Reconstruct the WAL group for a snapshot recorded in the table's main
/// snapshot log. The caller supplies catalog-loaded metadata and owns the
/// returned group through its allocator. Expired history fails closed.
pub fn readCommittedWalGroup(a: A, binding: Binding, options: Options, context: Context, table: catalog.types.Table, snapshot_id: i64) !CommittedWalGroup {
    const root = try catalog.metadata.parse(a, table.metadata_json);
    const log = try catalog.metadata.get(root, "snapshot-log");
    if (log != .array) return error.InvalidLakeMetadata;
    for (log.array.items) |entry| {
        if (try catalog.metadata.int(try catalog.metadata.get(entry, "snapshot-id")) == snapshot_id) break;
    } else return error.LakeOverlayCoverageUnavailable;

    // An old log entry can survive a rollback. Require the requested snapshot
    // to remain on the current branch before using it as a replay position.
    var by_id = try indexSnapshots(a, root);
    defer by_id.deinit();
    var current = (try mainSnapshot(root)) orelse return error.LakeOverlayCoverageUnavailable;
    const interval = for (0..by_id.count()) |_| {
        const snapshot = by_id.get(current) orelse return error.LakeOverlayCoverageUnavailable;
        if (current == snapshot_id) break (try CommittedWalInterval.read(snapshot)) orelse return error.LakeOverlayCoverageUnavailable;
        current = try catalog.metadata.int(snapshot.object.get("parent-snapshot-id") orelse return error.LakeOverlayCoverageUnavailable);
    } else return error.LakeOverlayCoverageUnavailable;
    if (interval.through - interval.previous > max_group_records) return error.LakeWalCoverageGap;

    var opened = try openQueue(a, binding, options);
    defer opened.deinit();
    const base = try prefix(a, opened.prefix, binding, options);
    defer a.free(base);
    const store: wal.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = base, .context = context, .native_owner = if (options.native_owner) |owner| owner.token else null };
    var reader = try store.readerThrough(a, interval.previous, interval.through);
    defer reader.deinit();
    const result = (try readGroup(a, &reader, max_group_changes, max_group_payload_bytes)) orelse return error.LakeWalCoverageGap;
    if (result.last_lsn != interval.through or
        !std.mem.eql(u8, result.batch.source, interval.source) or
        !std.mem.eql(u8, result.batch.epoch, interval.epoch) or
        !std.mem.eql(u8, result.batch.batch_id, interval.batch)) return error.LakeWalCoverageGap;
    return .{ .interval = interval, .group = result };
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
    try verifyCommittedWalInterval(scratch, result.table, attempt.body, cut);
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
    // The first pass reports the original WAL admission while it resolves
    // the rejected intent. The retry commits that same admission.
    const admitted = try applyBatch(a, binding, options, .{}, batch);
    try std.testing.expectEqual(lsn, admitted.accepted_sequence);
    try std.testing.expect(!admitted.committed);
    try std.testing.expectEqual(@as(u64, 0), try coverage(a, try managed.load(a)));
    const committed = try applyBatch(a, binding, options, .{}, batch);
    try std.testing.expectEqual(lsn, committed.accepted_sequence);
    try std.testing.expect(committed.committed);
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
    const retry = try std.json.parseFromSliceLeaky(Batch, a, payloads[1], .{});
    try std.testing.expectEqual(@as(?BatchOutcome, null), try observeBatch(a, binding, options, .{}, retry));
    for (payloads, 1..) |payload, lsn| try std.testing.expectEqual(@as(u64, lsn), try accept(a, binding, options, .{}, payload));
    try std.testing.expectEqual(BatchOutcome{ .accepted_sequence = 2, .committed = false }, (try observeBatch(a, binding, options, .{}, retry)).?);
    var changed = retry;
    changed.checkpoint = "different";
    try std.testing.expectError(error.WalIdempotencyConflict, observeBatch(a, binding, options, .{}, changed));
    // Four changes fit: b1..b3 fold into one snapshot; b4 would exceed the limit.
    try std.testing.expect(try drainBounded(a, binding, options, .{}, 4, max_group_payload_bytes));
    const first = try managed.load(a);
    try std.testing.expectEqual(@as(u64, 3), try coverage(a, first));
    const first_root = try catalog.metadata.parse(a, first.metadata_json);
    const first_id = try catalog.metadata.int(try catalog.metadata.get(first_root, "current-snapshot-id"));
    const committed_group = try readCommittedWalGroup(a, binding, options, .{}, first, first_id);
    try std.testing.expectEqual(@as(u64, 1), committed_group.group.first_lsn);
    try std.testing.expectEqual(@as(u64, 3), committed_group.group.last_lsn);
    try std.testing.expectEqual(@as(usize, 3), committed_group.group.records);
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, readCommittedWalGroup(a, binding, options, .{}, first, -1));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(!try drain(a, binding, options, .{}));
    const last = try managed.load(a);
    try std.testing.expectEqual(@as(u64, 5), try coverage(a, last));
    const historical = try readCommittedWalGroup(a, binding, options, .{}, last, first_id);
    try std.testing.expectEqual(@as(u64, 0), historical.interval.previous);
    try std.testing.expectEqual(@as(u64, 3), historical.interval.through);
    try std.testing.expectEqual(first_id, historical.interval.snapshot);
    try std.testing.expectEqual(@as(?i64, null), historical.interval.parent);
    const root = try catalog.metadata.parse(a, last.metadata_json);
    const last_id = try catalog.metadata.int(try catalog.metadata.get(root, "current-snapshot-id"));
    const uuid = try catalog.metadata.str(try catalog.metadata.get(root, "table-uuid"));
    const first_position = try captureCommittedWalPosition(a, first);
    try std.testing.expectEqualStrings(uuid, first_position.table_uuid);
    try std.testing.expectEqual(@as(?i64, first_id), first_position.snapshot);
    try std.testing.expectEqual(@as(u64, 3), first_position.through);
    const current_position = try captureCommittedWalPosition(a, last);
    try std.testing.expectEqual(@as(?i64, last_id), current_position.snapshot);
    try std.testing.expectEqual(@as(u64, 5), current_position.through);
    try std.testing.expectEqual(last_id, (try nextCommittedWalInterval(a, last, first_position)).?.snapshot);
    try std.testing.expectEqual(first_id, (try nextCommittedWalInterval(a, last, .{ .table_uuid = uuid, .snapshot = null, .through = 0 })).?.snapshot);
    try std.testing.expect((try nextCommittedWalInterval(a, last, .{ .table_uuid = uuid, .snapshot = last_id, .through = 5 })) == null);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, nextCommittedWalInterval(a, last, .{ .table_uuid = uuid, .snapshot = first_id, .through = 2 }));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, nextCommittedWalInterval(a, last, .{ .table_uuid = "prior-table", .snapshot = first_id, .through = 3 }));
    try std.testing.expectEqual(@as(u64, 0), try retainedWalFloor(a, root, 5, 0, 600_000, 1));
    try std.testing.expectEqual(@as(u64, 3), try retainedWalFloor(a, root, 5, std.math.maxInt(u64), 600_000, 1));
    try std.testing.expectEqual(@as(u64, 0), try retainedWalFloor(a, root, 5, std.math.maxInt(u64), 600_000, 2));
    const successor = try readCommittedWalGroup(a, binding, options, .{}, last, last_id);
    try std.testing.expectEqual(@as(?i64, first_id), successor.interval.parent);
    try std.testing.expectEqual(historical.interval.through, successor.interval.previous);
    try std.testing.expectEqual(@as(u64, 4), successor.group.first_lsn);
    try std.testing.expectEqual(@as(u64, 5), successor.group.last_lsn);
    var detected = try local.serverless_query_lake_schema.icebergSchema(a, last.metadata_json, null);
    defer detected.deinit();
    var read_binding = binding;
    read_binding.schema_fingerprint = detected.fingerprint;
    const read_table: local.sql_catalog.Table = .{
        .id = 9,
        .physical_name = "events",
        .schema_version = 1,
        .external_base_source = .{
            .binding = read_binding,
            .table_id = @constCast(read_binding.table_id),
            .source_uri = @constCast(binding.source_uri),
            .schema_fingerprint = @constCast(binding.schema_fingerprint),
        },
        .columns = &.{
            .{ .name = "id", .path = "id", .type = .integer },
            .{ .name = "body", .path = "body", .type = .string },
        },
    };
    const changes = @import("../api/lake_committed_changes.zig");
    const initial_images = try changes.read(a, historical, last, null, read_table, .{}, options);
    try std.testing.expectEqual(@as(usize, 1), initial_images.len);
    try std.testing.expect(initial_images[0].before == null);
    try std.testing.expectEqual(@as(i64, 2), initial_images[0].after.?.object.get("id").?.integer);
    try std.testing.expectEqualStrings("c", initial_images[0].after.?.object.get("body").?.string);
    const schemas = struct {
        table: local.sql_catalog.Table,
        pub fn tableAt(self: @This(), _: A, _: i64, _: catalog.types.Table, _: local.api_operation.RequestContext) !local.sql_catalog.Table {
            return self.table;
        }
    }{ .table = read_table };
    const next_delivery = (try changes.readNext(a, binding, options, last, first_position, schemas, .{})).?;
    try std.testing.expectEqual(@as(?i64, last_id), next_delivery.position.snapshot);
    try std.testing.expectEqual(@as(u64, 5), next_delivery.position.through);
    const feed_scope: [32]u8 = @splat(7);
    const receipt_position = try changes.encodePosition(a, feed_scope, 9, next_delivery.position);
    try std.testing.expect(@import("../metadata/changefeed_control.zig").validProviderContext(receipt_position));
    const resumed_position = try changes.decodePosition(a, feed_scope, 9, receipt_position);
    try std.testing.expectEqualStrings(uuid, resumed_position.table_uuid);
    try std.testing.expectEqual(next_delivery.position.snapshot, resumed_position.snapshot);
    try std.testing.expectEqual(next_delivery.position.through, resumed_position.through);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, changes.decodePosition(a, @splat(8), 9, receipt_position));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, changes.decodePosition(a, feed_scope, 10, receipt_position));
    const next_images = next_delivery.images;
    try std.testing.expectEqual(@as(usize, 2), next_images.len);
    try std.testing.expect(next_images[0].before == null);
    try std.testing.expectEqual(@as(i64, 3), next_images[0].after.?.object.get("id").?.integer);
    var protected: std.ArrayList([]const u8) = .empty;
    try protectRetainedWalSnapshots(a, root, 3, &protected);
    try std.testing.expectEqual(@as(usize, 2), protected.items.len);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{last_id}), protected.items[0]);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{first_id}), protected.items[1]);
    var compacted = try catalog.metadata.parse(a, last.metadata_json);
    const compacted_snapshots = compacted.object.getPtr("snapshots").?;
    const rewrite_id = std.math.maxInt(i64);
    try compacted_snapshots.array.items[1].object.put(a, "parent-snapshot-id", .{ .integer = rewrite_id });
    const rewrite_json = try std.json.Stringify.valueAlloc(a, .{
        .@"snapshot-id" = rewrite_id,
        .@"parent-snapshot-id" = first_id,
        .summary = .{ .operation = "replace", .@"antfly.compaction" = "rewrite", .@"antfly.wal.coverage" = "3" },
    }, .{});
    try compacted_snapshots.array.append(try catalog.metadata.parse(a, rewrite_json));
    var compacted_table = last;
    compacted_table.metadata_json = try std.json.Stringify.valueAlloc(a, compacted, .{});
    try std.testing.expectEqual(last_id, (try nextCommittedWalInterval(a, compacted_table, first_position)).?.snapshot);
    var rewrite_protected: std.ArrayList([]const u8) = .empty;
    try protectRetainedWalSnapshots(a, compacted, 3, &rewrite_protected);
    try std.testing.expectEqual(@as(usize, 2), rewrite_protected.items.len);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{rewrite_id}), rewrite_protected.items[1]);
    // An acknowledged feed at the current logical cut must stay empty
    // when the catalog advances through a physical rewrite only.
    var rewrite_only = try catalog.metadata.parse(a, last.metadata_json);
    const rewrite_after_commit = try std.json.Stringify.valueAlloc(a, .{
        .@"snapshot-id" = rewrite_id,
        .@"parent-snapshot-id" = last_id,
        .summary = .{ .operation = "replace", .@"antfly.compaction" = "rewrite", .@"antfly.wal.coverage" = "5" },
    }, .{});
    try rewrite_only.object.getPtr("snapshots").?.array.append(try catalog.metadata.parse(a, rewrite_after_commit));
    try rewrite_only.object.put(a, "current-snapshot-id", .{ .integer = rewrite_id });
    var rewrite_only_table = last;
    rewrite_only_table.metadata_json = try std.json.Stringify.valueAlloc(a, rewrite_only, .{});
    try std.testing.expect(try changes.readNext(a, binding, options, rewrite_only_table, resumed_position, schemas, .{}) == null);

    var rolled_back = try catalog.metadata.parse(a, last.metadata_json);
    try rolled_back.object.put(a, "current-snapshot-id", .{ .integer = first_id });
    var rollback_table = last;
    rollback_table.metadata_json = try std.json.Stringify.valueAlloc(a, rolled_back, .{});
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, readCommittedWalGroup(a, binding, options, .{}, rollback_table, last_id));
    try std.testing.expectEqualStrings("5", try catalog.metadata.str((try catalog.metadata.get(root, "properties")).object.get("antfly.cdc.checkpoint").?));
    const snapshots = (try catalog.metadata.get(root, "snapshots")).array.items;
    try std.testing.expectEqual(@as(usize, 2), snapshots.len);
    const summary = try catalog.metadata.get(snapshots[0], "summary");
    try std.testing.expectEqualStrings("wal-group-1-3", try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.batch-id")));
    try std.testing.expectEqualStrings("3", try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.wal.coverage")));
    try std.testing.expectEqualStrings("0", try catalog.metadata.str(try catalog.metadata.get(summary, "antfly.wal.previous-coverage")));
    const next_summary = try catalog.metadata.get(snapshots[1], "summary");
    try std.testing.expectEqualStrings("3", try catalog.metadata.str(try catalog.metadata.get(next_summary, "antfly.wal.previous-coverage")));
    try std.testing.expectEqualStrings("5", try catalog.metadata.str(try catalog.metadata.get(next_summary, "antfly.wal.coverage")));
    const first_intent = try std.json.Stringify.valueAlloc(a, .{ .updates = .{.{ .action = "add-snapshot", .snapshot = snapshots[0] }} }, .{});
    try verifyCommittedWalInterval(a, last, first_intent, 0);
    try std.testing.expectError(error.LakeWalCoverageGap, verifyCommittedWalInterval(a, last, first_intent, 1));
    var wrong_parent = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, snapshots[0], .{}), .{});
    try wrong_parent.object.put(a, "parent-snapshot-id", .{ .integer = 1 });
    const altered_intent = try std.json.Stringify.valueAlloc(a, .{ .updates = .{.{ .action = "add-snapshot", .snapshot = wrong_parent }} }, .{});
    try std.testing.expectError(error.LakeWalCoverageGap, verifyCommittedWalInterval(a, last, altered_intent, 0));
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
    try std.testing.expectEqual(@as(u64, 2), try acceptBatch(a, binding, options, .{}, retry));
    try std.testing.expectEqual(BatchOutcome{ .accepted_sequence = 2, .committed = true }, (try observeBatch(a, binding, options, .{}, retry)).?);
    // Empty polls use the same WAL admission and catalog commit path. They
    // advance coverage without manufacturing data/delete files or losing rows.
    const polls = [_][]const u8{
        "{\"batch_id\":\"poll-1\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"5\",\"checkpoint\":\"7\",\"key_fields\":[\"id\"],\"changes\":[]}",
        "{\"batch_id\":\"poll-2\",\"source\":\"s\",\"epoch\":\"1\",\"expected_checkpoint\":\"7\",\"checkpoint\":\"8\",\"key_fields\":[\"id\"],\"changes\":[]}",
    };
    for (polls, 8..) |payload, lsn| try std.testing.expectEqual(@as(u64, lsn), try accept(a, binding, options, .{}, payload));
    try std.testing.expect(try drain(a, binding, options, .{}));
    try std.testing.expect(!try drain(a, binding, options, .{}));
    const polled = try managed.load(a);
    try std.testing.expectEqual(@as(u64, 9), try coverage(a, polled));
    const polled_root = try catalog.metadata.parse(a, polled.metadata_json);
    try std.testing.expectEqualStrings("8", try catalog.metadata.str(try catalog.metadata.get(try catalog.metadata.get(polled_root, "properties"), "antfly.cdc.checkpoint")));
    const checkpoint = try a.dupe(u8, "postgres-next");
    const before = try std.json.parseFromSliceLeaky(V, a, "{\"id\":4,\"body\":\"before\"}", .{});
    const after = try std.json.parseFromSliceLeaky(V, a, "{\"id\":6,\"body\":\"after\"}", .{});
    const update: @import("../foreign/source.zig").ReplicationChange = .{ .op = .update, .checkpoint = checkpoint, .before = before, .before_kind = .full, .row = after };
    const captured = try postgresBatch(a, "s", "1", "8", checkpoint, &.{"id"}, &.{update});
    try std.testing.expectEqual(@as(usize, 2), captured.changes.len);
    try std.testing.expectEqual(.delete, captured.changes[0].op);
    try std.testing.expectEqual(@as(i64, 4), captured.changes[0].row.object.get("id").?.integer);
    try std.testing.expectEqual(@as(usize, 1), captured.changes[0].row.object.count());
    try std.testing.expectEqual(.upsert, captured.changes[1].op);
    try std.testing.expectEqual(@as(i64, 6), captured.changes[1].row.object.get("id").?.integer);
    try std.testing.expectError(error.InvalidReplicationCheckpoint, postgresBatch(a, "s", "1", "8", "wrong", &.{"id"}, &.{update}));
    var incomplete = update;
    incomplete.row_complete = false;
    try std.testing.expectError(error.ReplicationProjectionIncomplete, postgresBatch(a, "s", "1", "8", checkpoint, &.{"id"}, &.{incomplete}));
    const committed: BatchOutcome = .{ .accepted_sequence = 10, .committed = true };
    try std.testing.expectEqual(committed, try applyBatch(a, binding, options, .{}, captured));
    try std.testing.expectEqual(committed, try applyBatch(a, binding, options, .{}, captured));
    try std.testing.expectEqual(@as(u64, 10), try watermark(a, binding, options, .{}));
    const moved = try managed.load(a);
    const moved_root = try catalog.metadata.parse(a, moved.metadata_json);
    const moved_id = try catalog.metadata.int(try catalog.metadata.get(moved_root, "current-snapshot-id"));
    const moved_group = try readCommittedWalGroup(a, binding, options, .{}, moved, moved_id);
    const moved_images = try changes.read(a, moved_group, moved, read_table, read_table, .{}, options);
    try std.testing.expectEqual(@as(usize, 2), moved_images.len);
    try std.testing.expectEqual(@as(i64, 4), moved_images[0].before.?.object.get("id").?.integer);
    try std.testing.expect(moved_images[0].after == null);
    try std.testing.expect(moved_images[1].before == null);
    try std.testing.expectEqualStrings("after", moved_images[1].after.?.object.get("body").?.string);
    const Selector = struct {
        table_id: u64 = 9,
        fields: []const []const u8 = &.{"id"},
        predicates: []const struct { field: []const u8, value: V } = &.{},
    };
    const visibility = struct {
        pub fn snapshotImageFor(_: @This(), _: A, _: @import("../metadata/changefeed_control.zig").State, _: u64, _: []const u8, row: ?V, _: local.api_operation.RequestContext) !local.api_local_table_writes.table_write_source.changefeed_projection.Image {
            return .{ .row = row };
        }
        pub fn imagesFor(_: @This(), _: A, table_id: u64, _: []const u8, before_image: ?V, after_image: ?V, _: local.api_operation.RequestContext) !@import("../metadata/native_changefeed_delivery.zig").Images {
            try std.testing.expectEqual(@as(u64, 9), table_id);
            return .{ .before = .{ .row = before_image, .readable = false }, .after = .{ .row = after_image } };
        }
    }{};
    const projected = try changes.project(a, Selector{}, .rows, moved_images, visibility, .{});
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(.insert, projected[0].change.operation);
    try std.testing.expectEqual(@as(usize, 1), projected[0].change.after.?.object.count());
    try std.testing.expectEqual(@as(i64, 6), projected[0].change.after.?.object.get("id").?.integer);
    const effects = [_]changes.Effect{
        .{ .key = moved_images[0].key, .change = .{ .operation = .delete, .before = moved_images[0].before } },
        projected[0],
    };
    const moved_position = try captureCommittedWalPosition(a, moved);
    const page_one = try changes.encodePage(a, feed_scope, 9, moved_position, &effects, 0, null, .{ .events = 1 });
    try std.testing.expectEqual(@as(u32, 1), page_one.next_ordinal);
    try std.testing.expect(!page_one.complete);
    const retry_page = try changes.encodePage(a, feed_scope, 9, moved_position, &effects, 0, page_one.next_ordinal, .{ .events = 10 });
    try std.testing.expectEqualStrings(page_one.body, retry_page.body);
    try std.testing.expectEqual(page_one.proof, retry_page.proof);
    const page_two = try changes.encodePage(a, feed_scope, 9, moved_position, &effects, page_one.next_ordinal, null, .{});
    try std.testing.expect(page_two.complete);
    try std.testing.expectEqual(@as(u32, 2), page_two.next_ordinal);
    const feed_control = @import("../metadata/changefeed_control.zig");
    const definition: feed_control.Definition = .{
        .owner = @splat(1),
        .authorization = @splat(2),
        .tables = &.{.{ .table_id = 9, .binding = @splat(3), .policy_revision = 1, .contract = .writable_iceberg_rows }},
        .projection = .rows,
    };
    const namespace = feed_control.catalogRootNamespace(definition.tables[0]);
    const authority: feed_control.Digest = @splat(2);
    var feed_state = try (feed_control.Mutation{ .create = .{ .id = @splat(1), .definition = definition, .now = 100, .expires_at = 800, .bootstrap_expires_at = 0 } }).apply(a, .{}, .{ .incarnation = authority, .sequence = 1 });
    feed_state = try (feed_control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 2 });

    const provider_context = try changes.encodePosition(a, changes.feedScope(feed_state), 9, first_position);
    feed_state = try (feed_control.Mutation{ .stage_catalog_start = provider_context }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 3 });
    feed_state = try (feed_control.Mutation{ .seal = .{ .cut = @splat(7), .provider_context = provider_context, .catalog_starts = &.{.{ .namespace = namespace, .through = first_position.through }} } }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 4 });
    const initial = feed_state.acknowledged_cursor;
    var policy = visibility;
    var group_cache: changes.GroupCache = .{};
    defer group_cache.deinit();
    var provider: changes.Provider(@TypeOf(schemas), @TypeOf(visibility)) = .{ .namespace = namespace, .binding = binding, .options = options, .metadata = last, .schemas = schemas, .policy = &policy, .group_cache = &group_cache };
    const prepared_page = try provider.prepare(a, feed_state, initial, .{ .events = 1 }, .{});
    try std.testing.expectEqual(@as(u64, 3), prepared_page.advances[0].position.sequence);
    try std.testing.expectEqual(@as(u32, 1), prepared_page.events);
    // A v3 provider position encodes the raw WAL ordinal and a visibility bit.
    try std.testing.expectEqual(@as(u32, 3), prepared_page.advances[0].position.event);
    var receipt: feed_control.Delivery = .{ .sequence = 1, .before = initial, .cursor = @splat(0), .authority = prepared_page.proof, .issued_at = 101, .advances = prepared_page.advances, .provider_context = prepared_page.provider_context };
    receipt.cursor = receipt.identity(feed_state.id, feed_state.epoch);
    feed_state = try (feed_control.Mutation{ .issue = receipt }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 5 });
    const replayed_page = try provider.replay(a, feed_state, receipt, .{ .events = 10 }, .{});
    try std.testing.expectEqualStrings(prepared_page.body, replayed_page.body);
    try std.testing.expectEqual(prepared_page.proof, replayed_page.proof);
    feed_state = try (feed_control.Mutation{ .acknowledge = receipt.cursor }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 6 });
    feed_state = try (feed_control.Mutation{ .renew_catalog = .{ .now = 200, .expires_at = 900 } }).apply(a, feed_state, .{ .incarnation = authority, .sequence = 7 });
    try std.testing.expectEqual(@as(u64, 900), feed_state.expires_at);
    try std.testing.expectEqual(feed_state.expires_at, feed_state.roots[0].renewed_through);
    try std.testing.expect(!feed_state.renewal_pending);
    const resumed_page = try provider.prepare(a, feed_state, receipt.cursor, .{}, .{});
    try std.testing.expectEqual(@as(u32, 1), resumed_page.events);
    try std.testing.expectEqual(@as(u64, 5), resumed_page.advances[0].position.sequence);
    try std.testing.expectEqual(@as(u32, 0), resumed_page.advances[0].position.event);
    const complete_position = try changes.decodePosition(a, changes.feedScope(feed_state), 9, resumed_page.provider_context);
    try std.testing.expectEqual(@as(?i64, last_id), complete_position.snapshot);

    var bootstrap_definition = definition;
    bootstrap_definition.start = .snapshot_then_changes;
    var bootstrap = try (feed_control.Mutation{ .create = .{ .id = @splat(9), .definition = bootstrap_definition, .now = 100, .expires_at = 800, .bootstrap_expires_at = 400 } }).apply(a, .{}, .{ .incarnation = authority, .sequence = 1 });
    bootstrap = try (feed_control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 2 });
    const bootstrap_context = try changes.encodePosition(a, changes.feedScope(bootstrap), 9, first_position);
    bootstrap = try (feed_control.Mutation{ .stage_catalog_start = bootstrap_context }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 3 });
    bootstrap = try (feed_control.Mutation{ .seal = .{
        .cut = @splat(7),
        .provider_context = bootstrap_context,
        .catalog_starts = &.{.{
            .namespace = namespace,
            .through = first_position.through,
            .snapshot = .{ .id = @splat(7), .digest = @splat(7), .expires_at = 400 },
        }},
    } }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 4 });
    provider.binding.row_identity = .{ .key_columns = &.{"id"} };
    const snapshot_page = try provider.prepare(a, bootstrap, bootstrap.acknowledged_cursor, .{}, .{});
    try std.testing.expectEqual(@as(u32, 1), snapshot_page.events);
    try std.testing.expect(snapshot_page.advances[0].snapshot.?.done);
    try std.testing.expect(!bootstrap.bootstrapComplete());
    var snapshot_receipt: feed_control.Delivery = .{ .sequence = 1, .before = bootstrap.acknowledged_cursor, .cursor = @splat(0), .authority = snapshot_page.proof, .issued_at = 101, .advances = snapshot_page.advances, .provider_context = snapshot_page.provider_context };
    snapshot_receipt.cursor = snapshot_receipt.identity(bootstrap.id, bootstrap.epoch);
    bootstrap = try (feed_control.Mutation{ .issue = snapshot_receipt }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 5 });
    const replayed_snapshot = try provider.replay(a, bootstrap, snapshot_receipt, .{ .events = 10 }, .{});
    try std.testing.expectEqualStrings(snapshot_page.body, replayed_snapshot.body);
    bootstrap = try (feed_control.Mutation{ .acknowledge = snapshot_receipt.cursor }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 6 });
    bootstrap = try (feed_control.Mutation{ .finish_verified_bootstrap = .{ .proof = try bootstrap.bootstrapCompletionProof() } }).apply(a, bootstrap, .{ .incarnation = authority, .sequence = 7 });
    try std.testing.expectEqual(.streaming, bootstrap.phase);
    const following = try provider.prepare(a, bootstrap, snapshot_receipt.cursor, .{}, .{});
    try std.testing.expectEqual(@as(u32, 2), following.events);
    try std.testing.expectEqual(last_id, (try changes.decodePosition(a, changes.feedScope(bootstrap), 9, following.provider_context)).snapshot.?);
    const polled_snapshots = (try catalog.metadata.get(polled_root, "snapshots")).array.items;
    const before_poll = polled_snapshots[polled_snapshots.len - 2];
    const after_poll = polled_snapshots[polled_snapshots.len - 1];
    var before_list = try local.serverless_external_source_mod.iceberg_avro.parseManifestListAlloc(alloc, try catalog.row_commit.read(a, files, try catalog.metadata.str(try catalog.metadata.get(before_poll, "manifest-list"))));
    defer before_list.deinit(alloc);
    var after_list = try local.serverless_external_source_mod.iceberg_avro.parseManifestListAlloc(alloc, try catalog.row_commit.read(a, files, try catalog.metadata.str(try catalog.metadata.get(after_poll, "manifest-list"))));
    defer after_list.deinit(alloc);
    try std.testing.expectEqual(before_list.entries.len, after_list.entries.len);
    for (before_list.entries, after_list.entries) |before_entry, after_entry| try std.testing.expectEqualStrings(before_entry.manifest_path, after_entry.manifest_path);

    // The retained successor is readable after reopening the queue and
    // compacting the expired prefix.
    var queue = try openQueue(a, binding, options);
    defer queue.deinit();
    const store: wal.Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = try prefix(a, queue.prefix, binding, options) };
    const floor = try retainedWalFloor(a, root, 5, std.math.maxInt(u64), 600_000, 1);
    const wall_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    try pinCommittedWalPosition(a, binding, options, .{}, @splat(11), first_position, wall_ms / std.time.ms_per_s + 60);
    var anchors: std.ArrayList([]const u8) = .empty;
    try std.testing.expectEqual(floor, try pinnedWalFloor(a, binding, options, .{}, 5, wall_ms, &anchors));
    try std.testing.expectEqual(@as(usize, 1), anchors.items.len);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "{d}", .{first_id}), anchors.items[0]);
    const pruned = try store.prune(a, 5, 16, false);
    try std.testing.expect(pruned.complete);
    try std.testing.expectEqual(@as(u64, 3), pruned.floor);
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, readCommittedWalGroup(a, binding, options, .{}, last, first_id));
    const retained = try readCommittedWalGroup(a, binding, options, .{}, last, last_id);
    try std.testing.expectEqual(@as(u64, 4), retained.group.first_lsn);
    try std.testing.expectEqual(@as(u64, 5), retained.group.last_lsn);
    try pinCommittedWalPosition(a, binding, options, .{}, @splat(11), current_position, wall_ms / std.time.ms_per_s + 60);
    const advanced = try store.prune(a, 5, 16, false);
    try std.testing.expectEqual(@as(u64, 5), advanced.floor);
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, readCommittedWalGroup(a, binding, options, .{}, last, last_id));
    try retireCommittedWalPin(a, binding, options, .{}, @splat(11), wall_ms / std.time.ms_per_s + 60);
    try retireCommittedWalPin(a, binding, options, .{}, @splat(11), wall_ms / std.time.ms_per_s + 60);
    try std.testing.expectError(error.WalRetentionRetired, pinCommittedWalPosition(a, binding, options, .{}, @splat(11), current_position, wall_ms / std.time.ms_per_s + 60));
    try retireCommittedWalPin(a, binding, options, .{}, @splat(12), wall_ms / std.time.ms_per_s + 60);
    try std.testing.expectError(error.WalRetentionRetired, pinCommittedWalPosition(a, binding, options, .{}, @splat(12), current_position, wall_ms / std.time.ms_per_s + 60));
    try std.testing.expectEqual(@as(u64, 8), try accept(a, binding, options, .{}, polls[0]));
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

test "external lake WAL grouping bounds checkpoint-only records independently of rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const records = try a.alloc(wal.Record, max_group_records + 1);
    for (records, 0..) |*record, i| {
        const batch: Batch = .{ .batch_id = try std.fmt.allocPrint(a, "poll-{d}", .{i}), .source = "s", .epoch = "1", .checkpoint = try std.fmt.allocPrint(a, "{d}", .{i + 1}), .expected_checkpoint = if (i == 0) null else try std.fmt.allocPrint(a, "{d}", .{i}), .key_fields = &.{"id"}, .changes = &.{} };
        record.* = .{ .lsn = i + 1, .operation_id = batch.batch_id, .payload = try std.json.Stringify.valueAlloc(a, batch, .{}) };
    }
    const folded = try group(a, records, max_group_changes, max_group_payload_bytes);
    try std.testing.expectEqual(max_group_records, folded.records);
    try std.testing.expectEqual(@as(u64, max_group_records), folded.last_lsn);
    try std.testing.expectEqual(@as(usize, 0), folded.batch.changes.len);
    try folded.batch.validate(0);
    const tail = try group(a, records[max_group_records..], max_group_changes, max_group_payload_bytes);
    try std.testing.expectEqual(@as(usize, 1), tail.records);
    try std.testing.expect(tail.batch.follows(folded.batch));
}

test "external lake filesystem WAL applies the managed PostgreSQL stream owner port" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-wal-postgres-owner");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();

    const uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()});
    const schema = try std.json.Stringify.valueAlloc(a, .{ .base_source = .{
        .kind = "external",
        .table_id = "events",
        .uri = uri,
        .format = "iceberg",
        .write_policy = "iceberg_writer",
        .catalog = .{ .type = "managed" },
        .row_identity = .{ .key_columns = .{"id"} },
    } }, .{});
    var bound = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer bound.deinit(a);
    const options: Options = .{ .node_config = &config, .catalog_table_id = 9, .catalog_generation = 4 };
    var opened = try configured.openBindingObjectStoreAlloc(alloc, bound.binding, options);
    defer opened.deinit();
    var managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = uri };
    _ = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);

    const Fixture = struct {
        options: Options,
        authority: u64 = 12,
        checks: usize = 0,
        fn resolve(raw: *anyopaque, _: A, _: StreamOwner.Port.Request) !Options {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).options;
        }
        fn check(raw: *anyopaque, request: StreamOwner.Port.Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.authority != request.cutover.cutover_authority_id) return error.ReplicationCutoverAuthorityLost;
        }
    };
    var fixture: Fixture = .{ .options = options };
    var owner: StreamOwner = .{ .ptr = &fixture, .resolve = Fixture.resolve, .check = Fixture.check };
    const port = owner.port();
    var checkpoint = [_]u8{'1'};
    const row = try std.json.parseFromSliceLeaky(V, a, "{\"id\":1}", .{});
    var request: StreamOwner.Port.Request = .{
        .table = .{ .table_id = 9, .name = "events", .schema_json = schema, .object_storage_generation = 4 },
        .cutover = .{ .table_id = 9, .source_ordinal = 2, .source_kind = "postgres", .cutover_authority_id = 12, .prepared_checkpoint = "cut" },
        .previous_checkpoint = "cut",
        .checkpoint = &checkpoint,
        .changes = &.{.{ .op = .insert, .checkpoint = &checkpoint, .row = row }},
        .deadline_ns = null,
    };
    var unbootstrapped = request;
    unbootstrapped.previous_checkpoint = null;
    try std.testing.expectError(error.InvalidReplicationCheckpoint, port.apply(port.ptr, alloc, unbootstrapped));
    var snapshot = request;
    snapshot.changes = &.{};
    snapshot.snapshot_rows = &.{};
    snapshot.previous_checkpoint = null;
    try std.testing.expectError(error.InvalidReplicationCheckpoint, port.apply(port.ptr, alloc, snapshot));
    snapshot.checkpoint = "cut";
    const initial_row = try std.json.parseFromSliceLeaky(V, a, "{\"id\":2}", .{});
    snapshot.snapshot_rows = &.{initial_row};
    var recovery = snapshot;
    recovery.snapshot_rows = null;
    try std.testing.expect(!try port.bootstrap_committed.?(port.ptr, alloc, recovery));
    const digest = try port.stage_snapshot_page.?(port.ptr, alloc, snapshot);
    try std.testing.expectEqual(digest, try port.stage_snapshot_page.?(port.ptr, alloc, snapshot));
    var interrupted = snapshot;
    interrupted.snapshot_rows = &.{row};
    const unsealed_digest = try port.stage_snapshot_page.?(port.ptr, alloc, interrupted);
    try std.testing.expect(!std.mem.eql(u8, &digest, &unsealed_digest));
    try std.testing.expect(!try port.bootstrap_committed.?(port.ptr, alloc, recovery));
    var seal = snapshot;
    seal.snapshot_rows = null;
    seal.snapshot_chunks = &.{@splat(0)};
    try std.testing.expectError(error.FileNotFound, port.stage_bootstrap.?(port.ptr, alloc, seal));
    seal.snapshot_chunks = &.{digest};
    try std.testing.expectError(error.InvalidReplicationCheckpoint, port.stage_bootstrap.?(port.ptr, alloc, seal));
    seal.cutover.snapshot_offset = 1;
    var queue = try openQueue(a, bound.binding, options);
    defer queue.deinit();
    const queue_prefix = try prefix(a, queue.prefix, bound.binding, options);
    const stage = try bootstrapStageKey(a, queue_prefix, try bootstrapIdentity(a, "postgres:9:2", "12", "cut", &.{"id"}));
    const staged_key = try std.fmt.allocPrint(a, "{s}.manifest", .{stage});
    const page_key = try std.fmt.allocPrint(a, "{s}.pages/{s}", .{ stage, &std.fmt.bytesToHex(digest, .lower) });
    var retired = recovery;
    retired.cutover.cutover_authority_id = 13;
    retired.cutover.retired_cutover_authority_id = 12;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, port.retire_bootstrap.?(port.ptr, alloc, retired));
    fixture.authority = 13;
    var successor = snapshot;
    successor.cutover.cutover_authority_id = 13;
    const successor_digest = try port.stage_snapshot_page.?(port.ptr, alloc, successor);
    const successor_stage = try bootstrapStageKey(a, queue_prefix, try bootstrapIdentity(a, "postgres:9:2", "13", "cut", &.{"id"}));
    try port.retire_bootstrap.?(port.ptr, alloc, retired);
    try port.retire_bootstrap.?(port.ptr, alloc, retired);
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, page_key));
    var successor_page = try readBootstrapPage(a, &queue.client, queue.bucket, .{}, successor_stage, successor_digest, catalog.types.max_commit_bytes);
    successor_page.deinit(queue.client.allocator);
    fixture.authority = 12;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, port.stage_snapshot_page.?(port.ptr, alloc, snapshot));
    const retired_locator = try bootstrapLocatorKey(a, queue_prefix, "postgres:9:2", "12");
    const retirement_marker = try bootstrapRetirementKey(a, queue_prefix, retired_locator);
    // Simulate a put submitted before retirement whose response was lost.
    var late_page = try queue.client.putObject(queue.bucket, page_key, "late", .{});
    late_page.deinit(queue.client.allocator);
    const other_locator = try bootstrapLocatorKey(a, queue_prefix, "postgres:9:2", "14");
    const other_marker = try bootstrapRetirementKey(a, queue_prefix, other_locator);
    try writeImmutableBootstrap(&queue, .{}, other_marker, "invalid marker");
    try std.testing.expectEqual(@as(?u64, 1000), try sweepBootstrapRetirementPage(a, &queue, .{}, queue_prefix, 9));
    {
        var reopened_queue = try openQueue(a, bound.binding, options);
        defer reopened_queue.deinit();
        try std.testing.expectEqual(@as(?u64, 60_000), try sweepBootstrapRetirementPage(a, &reopened_queue, .{}, queue_prefix, 9));
    }
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, page_key));
    try std.testing.expectEqual(@as(?u64, 1000), try sweepBootstrapRetirementPage(a, &queue, .{}, queue_prefix, 9));
    try removeBootstrapObject(&queue, .{}, retirement_marker);
    try std.testing.expectEqual(digest, try port.stage_snapshot_page.?(port.ptr, alloc, snapshot));
    try std.testing.expectEqual(unsealed_digest, try port.stage_snapshot_page.?(port.ptr, alloc, interrupted));
    try std.testing.expectError(error.LakeWriteTooLarge, readBootstrapPage(a, &queue.client, queue.bucket, .{}, stage, digest, 1));
    const bootstrap = try bootstrapIdentity(a, "postgres:9:2", "12", "cut", &.{"id"});
    const locator_key = try bootstrapLocatorKey(a, queue_prefix, bootstrap.source, bootstrap.epoch);
    try std.testing.expectEqualStrings("cut", (try readBootstrapLocator(a, &queue, .{}, locator_key)).?);
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, staged_key));
    try port.stage_bootstrap.?(port.ptr, alloc, seal);
    var staged_object = try queue.client.statObject(queue.bucket, staged_key);
    staged_object.deinit(queue.client.allocator);
    try std.testing.expectEqual(@as(u64, 0), try watermark(a, bound.binding, options, .{}));
    try std.testing.expectEqual(@as(u64, 0), try coverage(a, try managed.load(a)));
    var wrong_cut = recovery;
    wrong_cut.checkpoint = "other";
    wrong_cut.cutover.prepared_checkpoint = "other";
    try std.testing.expect(!try port.bootstrap_committed.?(port.ptr, alloc, wrong_cut));
    try std.testing.expectEqual(@as(u64, 0), try coverage(a, try managed.load(a)));
    staged_object = try queue.client.statObject(queue.bucket, staged_key);
    staged_object.deinit(queue.client.allocator);
    // Inject a listing entry that the exact-cut collector cannot own.
    const invalid_page = try std.fmt.allocPrint(a, "{s}.pages/invalid", .{stage});
    var invalid = try queue.client.putObject(queue.bucket, invalid_page, "unowned", .{});
    invalid.deinit(queue.client.allocator);
    try std.testing.expectError(error.InvalidReplicationCheckpoint, port.bootstrap_committed.?(port.ptr, alloc, recovery));
    try std.testing.expectEqual(@as(u64, 1), try coverage(a, try managed.load(a)));
    try removeBootstrapObject(&queue, .{}, invalid_page);
    var wrong_locator = try queue.client.putObject(queue.bucket, locator_key, "other", .{});
    wrong_locator.deinit(queue.client.allocator);
    try std.testing.expectError(error.InvalidReplicationCheckpoint, port.bootstrap_committed.?(port.ptr, alloc, recovery));
    var restored_locator = try queue.client.putObject(queue.bucket, locator_key, "cut", .{});
    restored_locator.deinit(queue.client.allocator);
    try std.testing.expect(try port.bootstrap_committed.?(port.ptr, alloc, recovery));
    try std.testing.expectEqual(@as(u64, 1), try coverage(a, try managed.load(a)));
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, staged_key));
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, page_key));
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, locator_key));
    const unsealed_page_key = try std.fmt.allocPrint(a, "{s}.pages/{s}", .{ stage, &std.fmt.bytesToHex(unsealed_digest, .lower) });
    try std.testing.expectError(error.FileNotFound, queue.client.statObject(queue.bucket, unsealed_page_key));
    try std.testing.expectEqual(.committed, try port.apply(port.ptr, alloc, snapshot));
    try std.testing.expect(try port.bootstrap_committed.?(port.ptr, alloc, recovery));
    try std.testing.expectEqual(.committed, try port.apply(port.ptr, alloc, request));
    try std.testing.expect(fixture.checks > 1);
    try std.testing.expectEqual(.committed, try port.apply(port.ptr, alloc, request));
    try std.testing.expectEqual(.committed, try port.apply(port.ptr, alloc, snapshot));
    request.table.object_storage_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, port.apply(port.ptr, alloc, request));
    fixture.authority += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, port.apply(port.ptr, alloc, request));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, port.bootstrap_committed.?(port.ptr, alloc, recovery));
    fixture.authority -= 1;
    try std.testing.expect(try port.bootstrap_committed.?(port.ptr, alloc, recovery));
    try std.testing.expectEqual(@as(u64, 2), try watermark(a, bound.binding, options, .{}));
}
