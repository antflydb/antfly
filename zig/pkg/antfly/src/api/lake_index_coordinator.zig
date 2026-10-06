// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
};

pub fn reconcile(a: A, io: std.Io, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, authority: Authority, context: Context, cancellation: Cancellation, clock: publication.Clock, options: Options) !void {
    try context.ensureActive();
    try cancellation.check();
    const now = try clock.now_ms(clock.ptr);
    const signature = try publication.signatureFor(a, table, source, store.identity, context);
    var current = try catalog.parse(a, table.lake_index_catalog_json);
    defer current.deinit();
    if (current.value.published) |ready| {
        if (current.value.pending == null and std.meta.eql(ready.signature, signature)) return;
    }
    if (current.value.failure) |failure| {
        if (std.mem.eql(u8, &failure.desired, &signature.desired) and now < failure.retry_at_ms) return error.LakeIndexRetryDeferred;
    }
    const pending_bytes = try publication.begin(a, io, table, source, store.identity, context, now, options.lease_ms);
    defer a.free(pending_bytes);
    var pending = table;
    pending.lake_index_catalog_json = pending_bytes;
    // No upload is allowed until this exact attempt has durable authority.
    try authority.replace(authority.ptr, table, pending);

    var working = try limits.WorkingSetAllocator.init(a, options.build_limits);
    const build_alloc = working.allocator();
    var handle = store.artifactStore();
    const published_bytes = publication.build(build_alloc, &handle, pending, source, store.identity, context, cancellation, clock) catch |build_error| {
        const failure_time = clock.now_ms(clock.ptr) catch return build_error;
        var admitted = try catalog.parse(a, pending_bytes);
        defer admitted.deinit();
        const retry_at = std.math.add(u64, failure_time, options.retry_ms) catch std.math.maxInt(u64);
        const failed_bytes = try catalog.encode(a, try admitted.value.fail(admitted.value.pending.?.token, if (working.limit_exceeded) "LakeSidecarBuildLimitExceeded" else @errorName(build_error), retry_at));
        defer a.free(failed_bytes);
        var failed = pending;
        failed.lake_index_catalog_json = failed_bytes;
        // A changed definition or replacement lease wins over this failure.
        // Never overwrite another worker's state or retry an ambiguous CAS.
        try authority.replace(authority.ptr, pending, failed);
        if (working.limit_exceeded) return error.LakeSidecarBuildLimitExceeded;
        return build_error;
    };
    defer build_alloc.free(published_bytes);
    try context.ensureActive();
    try cancellation.check();
    var ready = pending;
    ready.lake_index_catalog_json = published_bytes;
    try authority.replace(authority.ptr, pending, ready);
}

test "external lake native coordinator fences ambiguous admission and reuses durable publication" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-coordinator");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .values = &.{"indexed value"} }});
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
    try std.testing.expectError(error.ExternalLakeIndexUnavailable, selection.select(a, query_table, &source, &store, .{}, .required));
}
