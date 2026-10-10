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
    var build_context = context;
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
    var handle = store.artifactStore();
    const published_bytes = publication.buildWithLeaseAndEmbedding(build_alloc, &handle, pending, source, store.identity, build_context, cancellation, clock, .{ .ptr = &lease, .snapshot = Renewal.snapshot }, options.embedding_options, .{ .store = store, .table_id = table.table_id, .recipe = catalog.desiredFingerprint(table), .context = build_context }) catch |build_error| {
        heartbeat.cancel(io);
        heartbeat_active = false;
        const failure_time = clock.now_ms(clock.ptr) catch return build_error;
        var admitted = try catalog.parse(a, lease.record.lake_index_catalog_json);
        defer admitted.deinit();
        const retry_at = std.math.add(u64, failure_time, options.retry_ms) catch std.math.maxInt(u64);
        const failed_bytes = try catalog.encode(a, try admitted.value.fail(admitted.value.pending.?.token, if (working.limit_exceeded) "LakeSidecarBuildLimitExceeded" else @errorName(build_error), retry_at));
        defer a.free(failed_bytes);
        var failed = lease.record;
        failed.lake_index_catalog_json = failed_bytes;
        try authority.replace(authority.ptr, lease.record, failed);
        if (working.limit_exceeded) return error.LakeSidecarBuildLimitExceeded;
        return build_error;
    };
    defer build_alloc.free(published_bytes);
    heartbeat.cancel(io);
    heartbeat_active = false;
    try build_context.ensureActive();
    try cancellation.check();
    var ready = lease.record;
    ready.lake_index_catalog_json = published_bytes;
    try authority.replace(authority.ptr, lease.record, ready);
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
        errdefer self.a.free(bytes);
        var renewed = self.record;
        renewed.lake_index_catalog_json = bytes;
        try self.authority.replace(self.authority.ptr, self.record, renewed);
        if (self.owned) |old| self.a.free(old);
        self.owned = bytes;
        self.record = renewed;
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
        fn replace(raw: *anyopaque, expected: local.common_topology_records.TableRecord, replacement: local.common_topology_records.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, expected.lake_index_catalog_json, self.table.lake_index_catalog_json)) return error.TableGenerationChanged;
            if (!(try catalog.transitionAllowed(std.testing.allocator, expected, replacement))) return error.InvalidLakeIndexCatalog;
            const bytes = try std.testing.allocator.dupe(u8, replacement.lake_index_catalog_json);
            if (self.owned) |old| std.testing.allocator.free(old);
            self.owned = bytes;
            self.table = replacement;
            self.table.lake_index_catalog_json = bytes;
            self.commits += 1;
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
    try std.testing.expectEqual(@as(usize, 3), mock.commits);
    var published = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer published.deinit();
    try std.testing.expectEqual(@as(u64, 2), published.value.published.?.generation);
    try std.testing.expect(published.value.pending == null);
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expectEqual(@as(usize, 3), mock.commits);
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
    // Simulate restoring historical metadata after its derived directory was
    // retired. Matching source/schema signatures must still rebuild storage.
    var artifacts = store.artifactStore();
    try artifacts.delete(published.value.published.?.directory.?.artifact_id);
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expectEqual(@as(usize, 5), mock.commits);
    var recovered = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 3), recovered.value.published.?.generation);
    try std.testing.expectEqual(published.value.namespace, recovered.value.namespace);
    try std.testing.expect(try durableDirectoryAvailable(a, &store, recovered.value.published.?, .none));
    // A complete older directory is still a refresh obligation after a
    // reader-format upgrade, even when every source signature is unchanged.
    recovered.value.published.?.reader_protocol = 31;
    const historical = try catalog.encode(a, recovered.value);
    if (mock.owned) |bytes| a.free(bytes);
    mock.owned = historical;
    mock.table.lake_index_catalog_json = historical;
    try reconcile(a, std.testing.io, mock.table, &source, &store, authority, .{}, .none, clock, options);
    try std.testing.expectEqual(@as(usize, 7), mock.commits);
    var upgraded = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer upgraded.deinit();
    try std.testing.expectEqual(catalog.native_reader_protocol, upgraded.value.published.?.reader_protocol);
    try std.testing.expectEqual(@as(u64, 4), upgraded.value.published.?.generation);
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
    try std.testing.expectEqual(@as(usize, 9), mock.commits);
    var refreshed = try catalog.parse(a, mock.table.lake_index_catalog_json);
    defer refreshed.deinit();
    try std.testing.expectEqual(@as(u64, 5), refreshed.value.published.?.generation);
    try std.testing.expect(try durableDirectoryAvailable(a, &store, refreshed.value.published.?, .none));
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
