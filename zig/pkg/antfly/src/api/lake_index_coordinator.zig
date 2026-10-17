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

//! One bounded reconciliation attempt against authoritative table metadata.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const publication = @import("lake_index_publication.zig");
const Store = @import("lake_index_store.zig").Store;
const limits = @import("../serverless/build/lake_build_limits.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub const Authority = struct {
    ptr: *anyopaque,
    /// Returns only after a full-definition CAS commits. Ambiguous admission
    /// must return its error; callers re-read authority rather than replay it.
    replace: *const fn (*anyopaque, local.common_topology_records.TableRecord, local.common_topology_records.TableRecord) anyerror!void,
    /// Optional authoritative reread; the returned record is deeply owned by a.
    read: ?*const fn (*anyopaque, A, u64) anyerror!?local.common_topology_records.TableRecord = null,
};
pub const Options = struct {
    lease_ms: u64 = 5 * 60 * 1000,
    retry_ms: u64 = 1000,
    build_limits: limits.Limits = .{},
    embedding_options: ?local.inference_managed_embedder.InitOptions = null,
};

pub fn reconcile(a: A, io: std.Io, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, authority: Authority, context: Context, cancellation: Cancellation, clock: publication.Clock, options: Options) !void {
    try context.ensureActive();
    try cancellation.check();
    const now = try clock.now_ms(clock.ptr);
    const signature = try publication.signatureFor(a, table, source, store.identity, context);
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    if (current.value.published) |ready| {
        if (current.value.pending == null and std.meta.eql(ready.signature, signature) and try durableDirectoryAvailable(a, store, ready, cancellation)) return;
    }
    if (current.value.failure) |failure| {
        if (std.mem.eql(u8, &failure.desired, &signature.desired) and now < failure.retry_at_ms) return error.LakeIndexRetryDeferred;
    }
    const pending_bytes = try publication.beginWithLocator(a, io, table, source, store.identity, context, now, options.lease_ms, store.locator);
    defer a.free(pending_bytes);
    var pending = table;
    pending.lake_index_catalog_json = pending_bytes;
    // No upload is allowed until this exact attempt has durable authority.
    try authority.replace(authority.ptr, table, pending);

    var lease: Renewal = .{ .a = a, .io = io, .record = pending, .authority = authority, .clock = clock, .lease_ms = options.lease_ms, .parent_context = context };
    defer if (lease.owned) |bytes| a.free(bytes);
    defer if (lease.owned_read_schema) |bytes| a.free(bytes);
    var build_context = context;
    build_context.io = io;
    build_context.checkpoint = .{ .ptr = &lease, .check = Renewal.check };
    // Provider callbacks share the renewing fence, including speculative I/O.
    const old_source_context = source.scanner.shared_reader;
    if (old_source_context) |reader| reader.context = build_context;
    defer {
        if (old_source_context) |reader| reader.context = context;
    }
    if (source.context_store) |context_store| context_store.context = build_context;
    defer {
        if (source.context_store) |context_store| context_store.context = context;
    }
    var heartbeat = try io.concurrent(Renewal.run, .{&lease});
    var heartbeat_active = true;
    defer if (heartbeat_active) heartbeat.cancel(io);
    var working = try limits.WorkingSetAllocator.init(a, options.build_limits);
    const build_alloc = working.allocator();
    var artifact_reads = @import("lake_index_artifact_recovery.zig").Reads.init(store.artifactStore());
    var handle = artifact_reads.store();
    const published_bytes = publication.buildWithLeaseAndEmbedding(build_alloc, &handle, pending, source, store.identity, build_context, cancellation, clock, .{ .ptr = &lease, .snapshot = Renewal.snapshot, .save_checkpoint = Renewal.saveCheckpoint, .discard_checkpoint = Renewal.discardCheckpoint }, options.embedding_options, .{ .store = store, .table_id = table.table_id, .recipe = catalog.desiredFingerprint(table), .context = build_context }) catch |reported_error| {
        heartbeat.cancel(io);
        heartbeat_active = false;
        if (lease.terminal_error) |err| return err;
        const build_error = if (reported_error == error.LakeIndexArtifactUnavailable) artifact_reads.cause() orelse reported_error else reported_error;
        // Only derived-artifact failures invalidate saved work. A source read
        // may also return FileNotFound; its checkpoint remains resumable.
        if (reported_error == error.LakeIndexArtifactUnavailable or reported_error == error.ArtifactIntegrityMismatch) {
            try context.ensureActive();
            try cancellation.check();
            var saved = try catalog.parse(a, lease.record.lake_index_catalog_json);
            defer saved.deinit();
            if (saved.value.checkpoint != null) try Renewal.discardCheckpoint(&lease);
        }
        const failure_time = clock.now_ms(clock.ptr) catch return build_error;
        var admitted = try catalog.parse(a, lease.record.lake_index_catalog_json);
        defer admitted.deinit();
        const retry_at = std.math.add(u64, failure_time, options.retry_ms) catch std.math.maxInt(u64);
        const failed_bytes = try catalog.encode(a, try admitted.value.fail(admitted.value.pending.?.token, if (working.limit_exceeded) "LakeSidecarBuildLimitExceeded" else @errorName(build_error), retry_at));
        defer a.free(failed_bytes);
        try lease.replaceCatalog(failed_bytes);
        if (working.limit_exceeded) return error.LakeSidecarBuildLimitExceeded;
        return build_error;
    };
    defer build_alloc.free(published_bytes);
    heartbeat.cancel(io);
    heartbeat_active = false;
    try build_context.ensureActive();
    try cancellation.check();
    // Keep the final publication on the same narrowly rebased definition as
    // checkpoint and heartbeat writes; its bytes remain build-owned.
    try lease.replaceCatalog(published_bytes);
}

/// Historical metadata recovery can restore a publication whose retired
/// directory has already been collected. Its durable schema remains a build
/// obligation; do not mistake a matching definition signature for available
/// derived storage. Transient provider errors retain the existing obligation.
fn durableDirectoryAvailable(a: A, store: *Store, ready: catalog.Publication, cancellation: Cancellation) !bool {
    if (ready.namespace == null or ready.reader_protocol != catalog.native_reader_protocol) return false;
    const directory = ready.directory orelse return false;
    var handle = store.artifactStore();
    handle.allocator = a;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const document = @import("lake_index_directory.zig").loadDocument(arena.allocator(), handle, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, cancellation, null) catch |err| switch (err) {
        error.FileNotFound, error.NotFound, error.ArtifactIntegrityMismatch => return false,
        else => return err,
    };
    if (document.declarations.len != directory.count) return error.InvalidLakeIndexCatalog;
    // Definition/source signatures do not change when reader formats advance.
    // An intact obsolete directory is not a reason to skip the required build.
    for (document.declarations) |declaration| {
        if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version != @import("lake_index_native_text.zig").metadata_version) return false;
    }
    return true;
}

const Renewal = struct {
    a: A,
    io: std.Io,
    record: local.common_topology_records.TableRecord,
    authority: Authority,
    clock: publication.Clock,
    lease_ms: u64,
    parent_context: Context = .{},
    owned: ?[]u8 = null,
    owned_read_schema: ?[]u8 = null,
    mutex: std.Io.Mutex = .init,
    terminal_error: ?anyerror = null,
    expires_ms: u64 = 0,
    failed: std.atomic.Value(bool) = .init(false),
    fn snapshot(raw: *anyopaque, a: A) !local.common_topology_records.TableRecord {
        const self: *Renewal = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.terminal_error) |err| return err;
        var record = self.record;
        record.lake_index_catalog_json = try a.dupe(u8, record.lake_index_catalog_json);
        return record;
    }
    fn saveCheckpoint(raw: *anyopaque, saved: catalog.Publication) !void {
        const self: *Renewal = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.terminal_error) |err| return err;
        var state = try catalog.parse(self.a, self.record.lake_index_catalog_json);
        defer state.deinit();
        const bytes = try catalog.encode(self.a, try state.value.saveCheckpoint(saved, saved.published_at_ms));
        try self.commit(bytes);
    }
    fn discardCheckpoint(raw: *anyopaque) !void {
        const self: *Renewal = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.terminal_error) |err| return err;
        try self.parent_context.ensureActive();
        var state = try catalog.parse(self.a, self.record.lake_index_catalog_json);
        defer state.deinit();
        const attempt = state.value.pending orelse return error.LakeIndexPublicationFenceChanged;
        const now = try self.clock.now_ms(self.clock.ptr);
        const bytes = try catalog.encode(self.a, try state.value.discardCheckpoint(attempt.token, now));
        try self.commit(bytes);
    }
    // Only derived read-schema finalization may rebase this build. The recipe
    // deliberately excludes that field; every input and the exact lease/catalog
    // bytes must still match. Never replay an ambiguous write or follow a new
    // generation. A conflicting write gets at most one reread; healthy
    // checkpoints require none.
    fn replaceCatalog(self: *Renewal, bytes: []const u8) !void {
        var replacement = self.record;
        replacement.lake_index_catalog_json = bytes;
        self.authority.replace(self.authority.ptr, self.record, replacement) catch |err| {
            if (err != error.TableGenerationChanged) return err;
            const read = self.authority.read orelse return err;
            try self.parent_context.ensureActive();
            const current = (try read(self.authority.ptr, self.a, self.record.table_id)) orelse return err;
            defer @import("../metadata/table_manager.zig").freeTable(self.a, current);
            if (std.mem.eql(u8, current.read_schema_json, self.record.read_schema_json)) return err;
            var comparable = current;
            comparable.read_schema_json = self.record.read_schema_json;
            if (!@import("../metadata/table_manager.zig").tableDefinitionsEqual(comparable, self.record)) return err;
            var state = try catalog.parse(self.a, current.lake_index_catalog_json);
            defer state.deinit();
            const attempt = state.value.pending orelse return error.LakeIndexPublicationFenceChanged;
            if (try self.clock.now_ms(self.clock.ptr) >= attempt.lease_expires_at_ms)
                return error.LakeIndexPublicationFenceChanged;
            const schema = try self.a.dupe(u8, current.read_schema_json);
            errdefer self.a.free(schema);
            var rebased = current;
            rebased.lake_index_catalog_json = bytes;
            try self.parent_context.ensureActive();
            try self.authority.replace(self.authority.ptr, current, rebased);
            if (self.owned_read_schema) |old| self.a.free(old);
            self.owned_read_schema = schema;
            self.record.read_schema_json = schema;
        };
    }

    // The caller holds the lease mutex. Ambiguous metadata writes are terminal.
    fn commit(self: *Renewal, bytes: []u8) !void {
        errdefer self.a.free(bytes);
        self.replaceCatalog(bytes) catch |err| {
            self.terminal_error = err;
            self.failed.store(true, .release);
            return err;
        };
        if (self.owned) |old| self.a.free(old);
        self.owned = bytes;
        self.record.lake_index_catalog_json = bytes;
    }
    // Provider cancellation callbacks must remain free of metadata I/O.
    fn check(raw: *anyopaque) !void {
        const self: *Renewal = @ptrCast(@alignCast(raw));
        try self.parent_context.ensureActive();
        if (self.failed.load(.acquire)) return error.LakeIndexPublicationFenceChanged;
    }
    fn run(self: *Renewal) void {
        while (true) {
            self.io.sleep(.fromMilliseconds(@intCast(@max(1, self.lease_ms / 3))), .awake) catch return;
            self.pulse() catch return;
        }
    }
    fn pulse(self: *Renewal) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.terminal_error) |err| return err;
        return self.renew() catch |err| {
            // An ambiguous CAS reply is terminal; the heartbeat never replays it.
            self.terminal_error = err;
            self.failed.store(true, .release);
            return err;
        };
    }
    fn renew(self: *Renewal) !void {
        const now = try self.clock.now_ms(self.clock.ptr);
        if (self.expires_ms != 0) {
            if (now >= self.expires_ms) return error.LakeIndexPublicationFenceChanged;
            if (self.expires_ms - now > @max(1, self.lease_ms / 2)) return;
        }
        var state = try catalog.parse(self.a, self.record.lake_index_catalog_json);
        defer state.deinit();
        const attempt = state.value.pending orelse return error.LakeIndexPublicationFenceChanged;
        self.expires_ms = attempt.lease_expires_at_ms;
        if (now >= attempt.lease_expires_at_ms) return error.LakeIndexPublicationFenceChanged;
        if (attempt.lease_expires_at_ms - now > @max(1, self.lease_ms / 2)) return;
        const bytes = try catalog.encode(self.a, try state.value.renew(attempt.token, now, self.lease_ms));
        try self.commit(bytes);
        self.expires_ms = now + self.lease_ms;
    }
};

test "external lake native coordinator fences ambiguous admission and reuses durable publication" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-coordinator");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{"indexed value"} }});
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    var second = try client.putObject("antfly", "part2.parquet", data, .{});
    second.deinit(a);
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const config_json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(config_json);
    var config = try local.common_config.Config.parseFromSlice(a, config_json);
    defer config.deinit();
    var store = try Store.open(a, &config, null, false);
    defer store.deinit();
    const Mock = struct {
        table: local.common_topology_records.TableRecord,
        owned: ?[]u8 = null,
        now: u64 = 100,
        commits: usize = 0,
        lose_reply: bool = true,
        cancel_checkpoint: bool = false,
        lose_discard_reply: bool = false,
        discards: usize = 0,
        remove_source_on_admission: ?*local.storage_object_storage.ObjectStorage = null,
        fn replace(raw: *anyopaque, expected: local.common_topology_records.TableRecord, replacement: local.common_topology_records.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, expected.lake_index_catalog_json, self.table.lake_index_catalog_json)) return error.TableGenerationChanged;
            if (!(try catalog.transitionAllowed(std.testing.allocator, expected, replacement))) return error.InvalidLakeIndexCatalog;
            var before = try catalog.parse(std.testing.allocator, expected.lake_index_catalog_json);
            defer before.deinit();
            var after = try catalog.parse(std.testing.allocator, replacement.lake_index_catalog_json);
            defer after.deinit();
            const discarded = before.value.checkpoint != null and after.value.checkpoint == null and after.value.pending != null and before.value.generation == after.value.generation;
            if (discarded) {
                try std.testing.expectEqualDeep(before.value.pending, after.value.pending);
                try std.testing.expectEqualDeep(before.value.published, after.value.published);
                self.discards += 1;
            }
            const bytes = try std.testing.allocator.dupe(u8, replacement.lake_index_catalog_json);
            if (self.owned) |old| std.testing.allocator.free(old);
            self.owned = bytes;
            self.table = replacement;
            self.table.lake_index_catalog_json = bytes;
            self.commits += 1;
            if (self.remove_source_on_admission) |source_client| if (after.value.pending != null and after.value.generation == before.value.generation + 1) {
                self.remove_source_on_admission = null;
                try source_client.deleteObject("antfly", "part2.parquet", .{});
            };
            if (discarded and self.lose_discard_reply) {
                self.lose_discard_reply = false;
                return error.MetadataMutationOutcomeUnknown;
            }
            if (self.cancel_checkpoint and before.value.generation == after.value.generation) {
                var saved = try catalog.parse(std.testing.allocator, bytes);
                defer saved.deinit();
                if (saved.value.checkpoint != null) {
                    self.cancel_checkpoint = false;
                    return error.MetadataMutationOutcomeUnknown;
                }
            }
            if (self.lose_reply) {
                self.lose_reply = false;
                return error.MetadataMutationOutcomeUnknown;
            }
        }
        fn time(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.now;
        }
    };
    var mock: Mock = .{ .table = .{ .table_id = 4, .name = "lake", .schema_json = schema, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"}}" } };
    defer if (mock.owned) |bytes| a.free(bytes);
    const authority: Authority = .{ .ptr = &mock, .replace = Mock.replace };
    const clock: publication.Clock = .{ .ptr = &mock, .now_ms = Mock.time };
    const options: Options = .{ .lease_ms = 10 };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
    try std.testing.expectEqual(@as(usize, 1), mock.commits);
    try std.testing.expectError(error.LakeIndexBuildInProgress, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
    try std.testing.expectEqual(@as(usize, 1), mock.commits);
    mock.now = 110;
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    const committed_build = mock.commits;
    try std.testing.expect(committed_build > 3);
    var published = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer published.deinit();
    try std.testing.expectEqual(@as(u64, 2), published.value.published.?.generation);
    try std.testing.expect(published.value.pending == null);
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expectEqual(committed_build, mock.commits);
    const selection = @import("lake_index_selection.zig");
    var query_table: local.sql_catalog.Table = .{
        .id = mock.table.table_id,
        .physical_name = mock.table.name,
        .schema_version = 1,
        .columns = &.{},
        .external_base_source = binding,
        .external_indexes = .{ .catalog_json = mock.table.lake_index_catalog_json, .indexes_json = mock.table.indexes_json, .desired = catalog.desiredFingerprint(mock.table) },
    };
    var selected = (try selection.select(a, query_table, &source, &store, .{}, .required)).?;
    defer selected.deinit();
    try std.testing.expectEqual(@as(u64, 2), selected.publication().generation);
    query_table.external_indexes.?.desired = @splat(9);
    try std.testing.expect((try selection.select(a, query_table, &source, &store, .{}, .automatic)) == null);
    try std.testing.expectError(error.ExternalLakeIndexDefinitionChanged, selection.select(a, query_table, &source, &store, .{}, .required));
    // A new definition forces construction. Lose the reply to the first
    // complete file checkpoint, then reopen authority under another attempt.
    mock.table.indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true}}";
    mock.cancel_checkpoint = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
    var partial = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer partial.deinit();
    try std.testing.expect(partial.value.checkpoint != null);
    try std.testing.expectEqual(@as(u64, 2), partial.value.published.?.generation);
    const resumed_artifacts = store.artifactStore();
    try @import("lake_index_directory.zig").hydrateLazy(partial.arena.allocator(), resumed_artifacts, &partial.value.checkpoint.?, .none, null);
    const first_text = partial.value.checkpoint.?.declarations[0];
    const partial_root = try @import("lake_index_native_text.zig").loadRoot(partial.arena.allocator(), resumed_artifacts, first_text.artifact, .none, null);
    try std.testing.expectEqual(@as(usize, 1), partial_root.file_groups.len);
    mock.now += 10;
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    var resumed = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer resumed.deinit();
    try std.testing.expect(resumed.value.checkpoint == null);
    try std.testing.expectEqual(@as(u64, 4), resumed.value.published.?.generation);
    try @import("lake_index_directory.zig").hydrateLazy(resumed.arena.allocator(), resumed_artifacts, &resumed.value.published.?, .none, null);
    const resumed_root = try @import("lake_index_native_text.zig").loadRoot(resumed.arena.allocator(), resumed_artifacts, resumed.value.published.?.declarations[0].artifact, .none, null);
    try std.testing.expectEqual(@as(usize, 2), resumed_root.file_groups.len);
    const resumed_file = for (resumed_root.file_groups) |group| {
        if (std.mem.eql(u8, group.file.id, partial_root.file_groups[0].file.id)) break group;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(partial_root.file_groups[0].segments[0].artifact_id, resumed_file.segments[0].artifact_id);
    var resumed_writer = try @import("lake_index_native_text.zig").loadWriter(a, resumed_artifacts, resumed_root, .none, null, null);
    defer resumed_writer.deinit();
    const resumed_snapshot = resumed_writer.acquireSnapshot();
    defer resumed_snapshot.release();
    const hits = try resumed_snapshot.search(a, "body", &.{"index"}, 10);
    defer a.free(hits.hits);
    try std.testing.expectEqual(@as(u32, 2), hits.total_count);
    // Historical metadata can outlive its derived checkpoint. Recovery must
    // discard it durably, preserving serving roots and the admitted lease.
    const checkpoint_directory = partial.value.checkpoint.?.directory.?;
    const serving_catalog = try a.dupe(u8, mock.table.lake_index_catalog_json);
    defer a.free(serving_catalog);
    const stores = @import("../serverless/artifacts/store.zig");
    var checkpoint_store = store.artifactStore();
    checkpoint_store.upload_scope = try stores.uploadScopeFromArtifactId(checkpoint_directory.artifact_id);
    const checkpoint_segment = try @import("lake_index_seekable_text.zig").loadDirectory(partial.arena.allocator(), .{ .store = resumed_artifacts, .cache = null, .context = .{}, .cancellation = .none }, partial_root.file_groups[0].segments[0]);
    for ([_]enum { missing, corrupt, ambiguous, root, manifest, statistics, segment_directory, pack, ambiguous_root, source }{ .missing, .corrupt, .ambiguous, .root, .manifest, .statistics, .segment_directory, .pack, .ambiguous_root, .source }) |mode| {
        var restored = partial.value;
        const target: @import("lake_index_aggregate_artifact.zig").ChunkRef = switch (mode) {
            .root, .ambiguous_root => .{ .artifact_id = first_text.artifact.artifact_id, .checksum = first_text.artifact.checksum, .byte_len = first_text.artifact.byte_len },
            .manifest => partial_root.manifests[0],
            .segment_directory => partial_root.file_groups[0].segments[0],
            .pack => checkpoint_segment.blocks[0].retainedArtifact(),
            .statistics => stats: {
                const ref = partial_root.term_statistics orelse partial_root.statistics_deltas[0].page;
                const id = try @import("../serverless/graph_segment/page_store.zig").PageStore.identity(partial_root.domain, ref);
                break :stats .{ .artifact_id = try partial.arena.allocator().dupe(u8, &id), .checksum = try partial.arena.allocator().dupe(u8, &std.fmt.bytesToHex(&ref.digest, .lower)), .byte_len = ref.bytes };
            },
            else => .{ .artifact_id = checkpoint_directory.artifact_id, .checksum = checkpoint_directory.checksum, .byte_len = checkpoint_directory.byte_len },
        };
        const target_bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, resumed_artifacts, target, .none, null);
        defer a.free(target_bytes);
        if (mode == .corrupt) restored.checkpoint.?.directory.?.byte_len += 1 else if (mode != .source) try checkpoint_store.delete(target.artifact_id);
        checkpoint_store.upload_scope = try stores.uploadScopeFromArtifactId(checkpoint_directory.artifact_id);
        const historical_checkpoint = try catalog.encode(a, restored);
        a.free(mock.owned.?);
        mock.owned = historical_checkpoint;
        mock.table.lake_index_catalog_json = historical_checkpoint;
        mock.now += options.lease_ms;
        const prior_discards = mock.discards;
        if (mode == .ambiguous or mode == .ambiguous_root) {
            mock.lose_discard_reply = true;
            try std.testing.expectError(error.MetadataMutationOutcomeUnknown, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
            var stopped = try catalog.parse(a, mock.table.lake_index_catalog_json);
            defer stopped.deinit();
            try std.testing.expect(stopped.value.checkpoint == null);
            try std.testing.expect(stopped.value.pending != null);
            try std.testing.expectEqual(@as(u64, 2), stopped.value.published.?.generation);
            try std.testing.expectEqual(@as(u64, 4), stopped.value.generation);
            mock.now += options.lease_ms;
        }
        if (mode == .source) {
            mock.remove_source_on_admission = &client;
            try std.testing.expectError(error.FileNotFound, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
            var source_failed = try catalog.parse(a, mock.table.lake_index_catalog_json);
            defer source_failed.deinit();
            try std.testing.expect(source_failed.value.checkpoint != null);
            try std.testing.expectEqual(prior_discards, mock.discards);
            var restored_source = try client.putObject("antfly", "part2.parquet", data, .{});
            defer restored_source.deinit(a);
            mock.now += options.retry_ms;
        }
        try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
        try std.testing.expectEqual(prior_discards + @as(usize, if (mode == .source) 0 else 1), mock.discards);
        var rebuilt = try catalog.parse(a, mock.table.lake_index_catalog_json);
        defer rebuilt.deinit();
        try std.testing.expect(rebuilt.value.pending == null and rebuilt.value.checkpoint == null);
        try @import("lake_index_directory.zig").hydrateLazy(rebuilt.arena.allocator(), resumed_artifacts, &rebuilt.value.published.?, .none, null);
        const rebuilt_root = try @import("lake_index_native_text.zig").loadRoot(rebuilt.arena.allocator(), resumed_artifacts, rebuilt.value.published.?.declarations[0].artifact, .none, null);
        try std.testing.expectEqual(@as(usize, 2), rebuilt_root.file_groups.len);
        var rebuilt_writer = try @import("lake_index_native_text.zig").loadWriter(a, resumed_artifacts, rebuilt_root, .none, null, null);
        defer rebuilt_writer.deinit();
        const rebuilt_snapshot = rebuilt_writer.acquireSnapshot();
        defer rebuilt_snapshot.release();
        const rebuilt_hits = try rebuilt_snapshot.search(a, "body", &.{"index"}, 10);
        defer a.free(rebuilt_hits.hits);
        try std.testing.expectEqual(@as(u32, 2), rebuilt_hits.total_count);
        checkpoint_store.upload_scope = try stores.uploadScopeFromArtifactId(target.artifact_id);
        var repaired = try checkpoint_store.putWithCancellation(target_bytes, .none);
        defer repaired.deinit(a);
        try std.testing.expectEqualStrings(target.artifact_id, repaired.artifact_id);
    }
    a.free(mock.owned.?);
    mock.owned = try a.dupe(u8, serving_catalog);
    mock.table.lake_index_catalog_json = mock.owned.?;
    // Simulate restoring historical metadata after its derived directory was
    // retired. Matching source/schema signatures must still rebuild storage.
    var artifacts = store.artifactStore();
    try artifacts.delete(resumed.value.published.?.directory.?.artifact_id);
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expect(mock.commits > committed_build);
    var recovered = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 5), recovered.value.published.?.generation);
    try std.testing.expectEqual(published.value.namespace, recovered.value.namespace);
    try std.testing.expect(try durableDirectoryAvailable(a, &store, recovered.value.published.?, .none));
    // A complete older directory is still a refresh obligation after a
    // reader-format upgrade, even when every source signature is unchanged.
    recovered.value.published.?.reader_protocol = 33;
    const historical = try catalog.encode(a, recovered.value);
    if (mock.owned) |bytes| a.free(bytes);
    mock.owned = historical;
    mock.table.lake_index_catalog_json = historical;
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expect(mock.commits > committed_build);
    var upgraded = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer upgraded.deinit();
    try std.testing.expectEqual(catalog.native_reader_protocol, upgraded.value.published.?.reader_protocol);
    try std.testing.expectEqual(@as(u64, 6), upgraded.value.published.?.generation);
    // A text format refresh must also rebuild when the reader protocol and
    // every source/definition signature are current and the directory exists.
    var old_text = upgraded.value.published.?;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var scoped = store.artifactStore();
    scoped.upload_scope = try @import("../serverless/artifacts/store.zig").UploadScope.forPublication(publication.uploadDomainWithNamespace(mock.table.table_id, store.identity, old_text.namespace), old_text.generation, std.testing.io);
    try @import("lake_index_directory.zig").hydrateLazy(scratch.allocator(), scoped, &old_text, .none, null);
    const old_declarations = try scratch.allocator().dupe(local.serverless_segment_sidecar_manifest.DeclaredArtifact, old_text.declarations);
    for (old_declarations) |*declaration| if (declaration.artifact.kind == .text_segment) {
        declaration.artifact.metadata_version = @import("lake_index_native_text.zig").metadata_version - 1;
    };
    const old_directory = try @import("lake_index_directory.zig").publish(a, &scoped, old_declarations, .none);
    defer a.free(old_directory.artifact_id);
    defer a.free(old_directory.checksum);
    old_text.directory = old_directory;
    upgraded.value.published = old_text;
    const old_catalog = try catalog.encode(a, upgraded.value);
    if (mock.owned) |bytes| a.free(bytes);
    mock.owned = old_catalog;
    mock.table.lake_index_catalog_json = old_catalog;
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expect(mock.commits > committed_build);
    var refreshed = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer refreshed.deinit();
    try std.testing.expectEqual(@as(u64, 7), refreshed.value.published.?.generation);
    try std.testing.expect(try durableDirectoryAvailable(a, &store, refreshed.value.published.?, .none));
    // A format refresh must recover even if only a published root or one of
    // its retained packs disappeared. No checkpoint invalidation alone can
    // remove these candidates; serving authority remains until replacement.
    const healthy_catalog = try a.dupe(u8, mock.table.lake_index_catalog_json);
    defer a.free(healthy_catalog);
    try @import("lake_index_directory.zig").hydrateLazy(refreshed.arena.allocator(), artifacts, &refreshed.value.published.?, .none, null);
    const healthy_ref = refreshed.value.published.?.declarations[0].artifact;
    const healthy_root = try @import("lake_index_native_text.zig").loadRoot(refreshed.arena.allocator(), artifacts, healthy_ref, .none, null);
    const healthy_segment = try @import("lake_index_seekable_text.zig").loadDirectory(refreshed.arena.allocator(), .{ .store = artifacts, .cache = null, .context = .{}, .cancellation = .none }, healthy_root.file_groups[0].segments[0]);
    for ([_]@import("lake_index_aggregate_artifact.zig").ChunkRef{
        .{ .artifact_id = healthy_ref.artifact_id, .checksum = healthy_ref.checksum, .byte_len = healthy_ref.byte_len },
        healthy_segment.blocks[0].retainedArtifact(),
        healthy_root.file_groups[0].descriptors[0],
        healthy_root.file_groups[0].physical_manifest.?,
    }) |target| {
        const saved_bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, artifacts, target, .none, null);
        defer a.free(saved_bytes);
        try artifacts.delete(target.artifact_id);
        var forced = refreshed.value;
        forced.published.?.reader_protocol = 31;
        const forced_catalog = try catalog.encode(a, forced);
        a.free(mock.owned.?);
        mock.owned = forced_catalog;
        mock.table.lake_index_catalog_json = forced_catalog;
        // Stop after recovery commits its first healthy file. Re-admission
        // must resume that checkpoint without probing the unavailable serving
        // publication or clearing completed work a second time.
        mock.cancel_checkpoint = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
        var interrupted = try catalog.parse(a, mock.table.lake_index_catalog_json);
        defer interrupted.deinit();
        try std.testing.expect(!interrupted.value.checkpoint.?.reuse_published);
        try @import("lake_index_directory.zig").hydrateLazy(interrupted.arena.allocator(), artifacts, &interrupted.value.checkpoint.?, .none, null);
        const saved_root = try @import("lake_index_native_text.zig").loadRoot(interrupted.arena.allocator(), artifacts, interrupted.value.checkpoint.?.declarations[0].artifact, .none, null);
        try std.testing.expectEqual(@as(usize, 1), saved_root.file_groups.len);
        const prior_discards = mock.discards;
        mock.now += options.lease_ms;
        try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
        try std.testing.expectEqual(prior_discards, mock.discards);
        var repaired_publication = try catalog.parse(a, mock.table.lake_index_catalog_json);
        defer repaired_publication.deinit();
        try std.testing.expectEqual(@as(u64, 9), repaired_publication.value.published.?.generation);
        try std.testing.expect(repaired_publication.value.pending == null and repaired_publication.value.failure == null);
        try @import("lake_index_directory.zig").hydrateLazy(repaired_publication.arena.allocator(), artifacts, &repaired_publication.value.published.?, .none, null);
        const repaired_root = try @import("lake_index_native_text.zig").loadRoot(repaired_publication.arena.allocator(), artifacts, repaired_publication.value.published.?.declarations[0].artifact, .none, null);
        const repaired_file = for (repaired_root.file_groups) |group| {
            if (std.mem.eql(u8, group.file.id, saved_root.file_groups[0].file.id)) break group;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(saved_root.file_groups[0].segments[0].artifact_id, repaired_file.segments[0].artifact_id);
        var repaired_writer = try @import("lake_index_native_text.zig").loadWriter(a, artifacts, repaired_root, .none, null, null);
        defer repaired_writer.deinit();
        const repaired_snapshot = repaired_writer.acquireSnapshot();
        defer repaired_snapshot.release();
        const repaired_hits = try repaired_snapshot.search(a, "body", &.{"index"}, 10);
        defer a.free(repaired_hits.hits);
        try std.testing.expectEqual(@as(u32, 2), repaired_hits.total_count);
        // Restore the fixture to independently exercise each damaged child.
        checkpoint_store.upload_scope = try stores.uploadScopeFromArtifactId(target.artifact_id);
        var restored_artifact = try checkpoint_store.putWithCancellation(saved_bytes, .none);
        defer restored_artifact.deinit(a);
        a.free(mock.owned.?);
        mock.owned = try a.dupe(u8, healthy_catalog);
        mock.table.lake_index_catalog_json = mock.owned.?;
    }
    // Ordered checkpoints must validate covering blocks and all retained trees,
    // including those a copy-on-write update would otherwise never read.
    const original_schema = mock.table.schema_json;
    for ([_]enum { cover, forward, reverse, ties, predicates }{ .cover, .forward, .reverse, .ties, .predicates }) |mode| {
        var ordered_arena = std.heap.ArenaAllocator.init(a);
        defer ordered_arena.deinit();
        const oa = ordered_arena.allocator();
        var ordered_schema = try std.json.parseFromSliceLeaky(std.json.Value, oa, original_schema, .{});
        try ordered_schema.object.put(oa, "relational_indexes", try std.json.parseFromSliceLeaky(std.json.Value, oa, "[{\"name\":\"by_body\",\"keys\":[{\"column\":\"body\"}]}]", .{}));
        mock.table.schema_json = try std.json.Stringify.valueAlloc(oa, ordered_schema, .{});
        mock.cancel_checkpoint = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options));
        var ordered_partial = try catalog.parse(a, mock.table.lake_index_catalog_json);
        defer ordered_partial.deinit();
        try @import("lake_index_directory.zig").hydrateLazy(oa, artifacts, &ordered_partial.value.checkpoint.?, .none, null);
        const ordered = @import("lake_index_ordered_rows.zig");
        const ordered_root = try ordered.loadRoot(oa, artifacts, ordered_partial.value.checkpoint.?.declarations[0].artifact, .none, null);
        var ordered_reader: ordered.Reader = undefined;
        try ordered_reader.init(a, &artifacts, ordered_root, ordered_root.fingerprint, "", null, .none);
        defer ordered_reader.deinit();
        const saved_entries = try ordered_reader.nextEntries(oa, 16);
        try std.testing.expectEqual(@as(usize, 1), saved_entries.len);
        const saved_cover = saved_entries[0].cover.?.block;
        const target = if (mode == .cover) saved_cover.artifact_id else page: {
            const ref = switch (mode) {
                .forward => ordered_root.page.?,
                .reverse => ordered_root.reverse.?,
                .ties => ordered_root.ties.?,
                .predicates => ordered_root.predicates.?,
                .cover => unreachable,
            };
            const id = try @import("../serverless/graph_segment/page_store.zig").PageStore.identity(ordered_root.domain, ref);
            break :page try oa.dupe(u8, &id);
        };
        try artifacts.delete(target);
        const prior_discards = mock.discards;
        mock.now += options.lease_ms;
        try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
        try std.testing.expectEqual(prior_discards + 1, mock.discards);
        var ordered_resumed = try catalog.parse(a, mock.table.lake_index_catalog_json);
        defer ordered_resumed.deinit();
        try std.testing.expect(ordered_resumed.value.checkpoint == null);
        try std.testing.expect(ordered_resumed.value.pending == null and ordered_resumed.value.failure == null);
        try @import("lake_index_directory.zig").hydrateLazy(oa, artifacts, &ordered_resumed.value.published.?, .none, null);
        const final_decl = for (ordered_resumed.value.published.?.declarations) |decl| {
            if (decl.artifact.kind == .ordered_row_index) break decl;
        } else return error.TestUnexpectedResult;
        const final_root = try ordered.loadRoot(oa, artifacts, final_decl.artifact, .none, null);
        try ordered.validateReuse(a, artifacts, final_root, .{}, .none);
        var final_reader: ordered.Reader = undefined;
        try final_reader.init(a, &artifacts, final_root, final_root.fingerprint, "", null, .none);
        defer final_reader.deinit();
        const final_entries = try final_reader.nextEntries(oa, 16);
        try std.testing.expectEqual(@as(usize, 2), final_entries.len);
        for (final_entries) |entry| {
            const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, artifacts, entry.cover.?.block, .none, null);
            defer a.free(bytes);
            const block = try local.sql_spill.decodeColumnarBlockInArena(oa, bytes, @import("lake_index_aggregate_artifact.zig").max_block_bytes);
            const value = try block.cell(entry.cover.?.row, 0);
            try std.testing.expect(!value.sql_null);
            try std.testing.expectEqualStrings("indexed value", value.value.string);
        }
        if (mode == .cover) {
            // Whole-index reuse during a format refresh needs the same proof;
            // clearing a checkpoint alone cannot retire a damaged publication.
            var forced = ordered_resumed.value;
            forced.published.?.reader_protocol = 31;
            const forced_catalog = try catalog.encode(a, forced);
            a.free(mock.owned.?);
            mock.owned = forced_catalog;
            mock.table.lake_index_catalog_json = forced_catalog;
            try artifacts.delete(final_entries[0].cover.?.block.artifact_id);
            try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
            var refreshed_ordered = try catalog.parse(a, mock.table.lake_index_catalog_json);
            defer refreshed_ordered.deinit();
            try std.testing.expect(refreshed_ordered.value.checkpoint == null and refreshed_ordered.value.pending == null);
            try @import("lake_index_directory.zig").hydrateLazy(oa, artifacts, &refreshed_ordered.value.published.?, .none, null);
            const refreshed_decl = for (refreshed_ordered.value.published.?.declarations) |decl| {
                if (decl.artifact.kind == .ordered_row_index) break decl;
            } else return error.TestUnexpectedResult;
            const refreshed_root = try ordered.loadRoot(oa, artifacts, refreshed_decl.artifact, .none, null);
            try ordered.validateReuse(a, artifacts, refreshed_root, .{}, .none);
        }
        a.free(mock.owned.?);
        mock.owned = try a.dupe(u8, healthy_catalog);
        mock.table.lake_index_catalog_json = mock.owned.?;
        mock.table.schema_json = original_schema;
    }
}

test "external lake lease renewal extends a live fence and never replays an ambiguous CAS" {
    const a = std.testing.allocator;
    const signature: catalog.Signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) };
    const initial = try catalog.encode(a, try (catalog.State{}).begin(signature, @splat(1), 100, 10));
    defer a.free(initial);
    const Mock = struct {
        now: u64 = 107,
        calls: usize = 0,
        ambiguous: bool = false,
        fn time(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.now;
        }
        fn replace(raw: *anyopaque, old: local.common_topology_records.TableRecord, new: local.common_topology_records.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var before = try catalog.parse(std.testing.allocator, old.lake_index_catalog_json);
            defer before.deinit();
            var after = try catalog.parse(std.testing.allocator, new.lake_index_catalog_json);
            defer after.deinit();
            try std.testing.expectEqual(before.value.pending.?.token, after.value.pending.?.token);
            try std.testing.expect(after.value.pending.?.lease_expires_at_ms > before.value.pending.?.lease_expires_at_ms);
            self.calls += 1;
            if (self.ambiguous) return error.MetadataMutationOutcomeUnknown;
        }
    };
    var mock: Mock = .{};
    var renewal: Renewal = .{ .a = a, .io = std.testing.io, .record = .{ .table_id = 1, .name = "lake", .lake_index_catalog_json = initial }, .authority = .{ .ptr = &mock, .replace = Mock.replace }, .clock = .{ .ptr = &mock, .now_ms = Mock.time }, .lease_ms = 10 };
    defer if (renewal.owned) |bytes| a.free(bytes);
    try renewal.pulse();
    try std.testing.expectEqual(@as(u64, 117), renewal.expires_ms);
    mock.now = 114;
    const snapshot = try Renewal.snapshot(&renewal, a);
    defer a.free(snapshot.lake_index_catalog_json);
    try renewal.pulse();
    try std.testing.expectEqual(@as(u64, 124), renewal.expires_ms);
    var old_snapshot = try catalog.parse(a, snapshot.lake_index_catalog_json);
    defer old_snapshot.deinit();
    try std.testing.expectEqual(@as(u64, 117), old_snapshot.value.pending.?.lease_expires_at_ms);
    mock.now = 121;
    mock.ambiguous = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, renewal.pulse());
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, renewal.pulse());
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, Renewal.check(&renewal));
    try std.testing.expectEqual(@as(usize, 3), mock.calls);
}

test "external lake lease tolerates target read schema finalization" {
    const a = std.testing.allocator;
    const tables = @import("../metadata/table_manager.zig");
    var table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = "target", .read_schema_json = "previous" };
    const signature: catalog.Signature = .{ .desired = catalog.desiredFingerprint(table), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) };
    const initial = try catalog.encode(a, try (catalog.State{}).begin(signature, @splat(1), 100, 10));
    defer a.free(initial);
    table.lake_index_catalog_json = initial;
    const Mock = struct {
        current: local.common_topology_records.TableRecord,
        owned: ?local.common_topology_records.TableRecord = null,
        now: u64 = 107,
        calls: usize = 0,
        reads: usize = 0,
        fn time(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.now;
        }
        fn read(raw: *anyopaque, allocator: A, id: u64) !?local.common_topology_records.TableRecord {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            try std.testing.expectEqual(self.current.table_id, id);
            return try tables.cloneTable(allocator, self.current);
        }
        fn replace(raw: *anyopaque, before: local.common_topology_records.TableRecord, after: local.common_topology_records.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!tables.tableDefinitionsEqual(before, self.current)) return error.TableGenerationChanged;
            try std.testing.expectEqualStrings("finalized", after.read_schema_json);
            const owned = try tables.cloneTable(a, after);
            if (self.owned) |old| tables.freeTable(a, old);
            self.owned = owned;
            self.current = owned;
        }
    };
    var mock: Mock = .{ .current = table };
    defer if (mock.owned) |owned| tables.freeTable(a, owned);
    mock.current.read_schema_json = "finalized";
    var renewal: Renewal = .{ .a = a, .io = std.testing.io, .record = table, .authority = .{ .ptr = &mock, .replace = Mock.replace, .read = Mock.read }, .clock = .{ .ptr = &mock, .now_ms = Mock.time }, .lease_ms = 10 };
    defer if (renewal.owned) |bytes| a.free(bytes);
    defer if (renewal.owned_read_schema) |bytes| a.free(bytes);
    // Derived read schema is deliberately excluded from the publication recipe.
    try std.testing.expectEqual(signature.desired, catalog.desiredFingerprint(mock.current));
    try renewal.pulse();
    try std.testing.expectEqual(@as(usize, 2), mock.calls);
    try std.testing.expectEqual(@as(usize, 1), mock.reads);
    try std.testing.expectEqualStrings("finalized", renewal.record.read_schema_json);
    try std.testing.expectEqual(@as(u64, 117), renewal.expires_ms);
    // Once rebased, subsequent checkpoints/renewals use that authority directly.
    mock.now = 114;
    try renewal.pulse();
    try std.testing.expectEqual(@as(usize, 3), mock.calls);
    try std.testing.expectEqual(@as(usize, 1), mock.reads);
    try std.testing.expectEqual(@as(u64, 124), renewal.expires_ms);
}

test "external lake lease rebase fences recipes storage leases expiry and ambiguous writes" {
    const a = std.testing.allocator;
    const tables = @import("../metadata/table_manager.zig");
    const Case = enum { recipe, storage, catalog, lease, missing, expiry, ambiguous, ambiguous_retry };
    for (std.meta.tags(Case)) |mode| {
        errdefer std.debug.print("lake lease rebase case={s}\n", .{@tagName(mode)});
        var table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = "target", .read_schema_json = "previous" };
        const signature: catalog.Signature = .{ .desired = catalog.desiredFingerprint(table), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) };
        const initial_state = try (catalog.State{}).begin(signature, @splat(1), 100, 10);
        const initial = try catalog.encode(a, initial_state);
        defer a.free(initial);
        table.lake_index_catalog_json = initial;
        const changed = switch (mode) {
            .catalog => try catalog.encode(a, try initial_state.renew(@splat(1), 101, 20)),
            .lease => try catalog.encode(a, try initial_state.begin(signature, @splat(9), 110, 20)),
            else => null,
        };
        defer if (changed) |bytes| a.free(bytes);
        const Mock = struct {
            mode: Case,
            current: local.common_topology_records.TableRecord,
            owned: ?local.common_topology_records.TableRecord = null,
            calls: usize = 0,
            reads: usize = 0,
            now: u64 = 107,
            fn time(raw: *const anyopaque) !u64 {
                const self: *const @This() = @ptrCast(@alignCast(raw));
                return self.now;
            }
            fn read(raw: *anyopaque, allocator: A, id: u64) !?local.common_topology_records.TableRecord {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.reads += 1;
                try std.testing.expectEqual(self.current.table_id, id);
                if (self.mode == .missing) return null;
                if (self.mode == .expiry) self.now = 111;
                return try tables.cloneTable(allocator, self.current);
            }
            fn replace(raw: *anyopaque, before: local.common_topology_records.TableRecord, after: local.common_topology_records.TableRecord) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                if (!tables.tableDefinitionsEqual(before, self.current)) return error.TableGenerationChanged;
                // Model a committed write whose reply was lost, including the
                // rebase CAS itself. Neither outcome may be blindly replayed.
                if (self.mode != .ambiguous and self.mode != .ambiguous_retry) return error.TestUnexpectedResult;
                const owned = try tables.cloneTable(a, after);
                if (self.owned) |old| tables.freeTable(a, old);
                self.owned = owned;
                self.current = owned;
                return error.MetadataMutationOutcomeUnknown;
            }
        };
        var mock: Mock = .{ .mode = mode, .current = table };
        defer if (mock.owned) |owned| tables.freeTable(a, owned);
        if (mode != .ambiguous) mock.current.read_schema_json = "finalized";
        if (mode == .recipe) mock.current.schema_json = "changed target";
        if (mode == .storage) {
            mock.current.storage.engine = .object;
            mock.current.object_storage_generation = 99;
            mock.current.object_storage_identity = @splat(7);
        }
        if (changed) |bytes| mock.current.lake_index_catalog_json = bytes;
        var renewal: Renewal = .{ .a = a, .io = std.testing.io, .record = table, .authority = .{ .ptr = &mock, .replace = Mock.replace, .read = Mock.read }, .clock = .{ .ptr = &mock, .now_ms = Mock.time }, .lease_ms = 10 };
        defer if (renewal.owned) |bytes| a.free(bytes);
        defer if (renewal.owned_read_schema) |bytes| a.free(bytes);
        const expected: anyerror = switch (mode) {
            .ambiguous, .ambiguous_retry => error.MetadataMutationOutcomeUnknown,
            .expiry => error.LakeIndexPublicationFenceChanged,
            else => error.TableGenerationChanged,
        };
        try std.testing.expectError(expected, renewal.pulse());
        try std.testing.expectEqual(@as(usize, if (mode == .ambiguous_retry) 2 else 1), mock.calls);
        try std.testing.expectEqual(@as(usize, if (mode == .ambiguous) 0 else 1), mock.reads);
        try std.testing.expectError(expected, renewal.pulse());
        try std.testing.expectEqual(@as(usize, if (mode == .ambiguous_retry) 2 else 1), mock.calls);
        try std.testing.expectEqual(@as(usize, if (mode == .ambiguous) 0 else 1), mock.reads);
        try std.testing.expectError(error.LakeIndexPublicationFenceChanged, Renewal.check(&renewal));
    }
}
