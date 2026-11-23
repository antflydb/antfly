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

//! Incarnation-fenced configuration for a managed CDC controller. Provider
//! ownership remains in its durable workflow; Antfly owns CDC execution.
const std = @import("std");
const local = @import("antfly_local_sources");
const api = @import("http_server.zig");
const authorization = local.api_stored_destination_authorization;
const A = std.mem.Allocator;
const Input = struct {
    table_id: u64,
    expected_sources_hash: []const u8,
    replication_sources: std.json.Value,
};
const Table = @import("../metadata/table_manager.zig").TableRecord;
const Destination = @import("../metadata/source_control.zig").Destination;

const Outbound = struct {
    document_committed: bool = false,
    snapshot_then_changes: bool = false,
    artifact_published: bool = false,
    publication_activated: bool = false,
};

const DestinationContract = struct {
    version: u8 = 1,
    adapter: ?Destination = null,
    input: enum { row_transactions, committed_snapshots, unavailable } = .unavailable,
    accepted_by: enum { native_transaction, iceberg_ingress, none } = .none,
    committed_by: enum { native_transaction, iceberg_catalog_snapshot, external_snapshot, none } = .none,
    transaction_scope: enum { native_transaction, iceberg_table, none } = .none,
    committed_history: enum { native_rows_when_enrolled, iceberg_committed_wal, unavailable } = .unavailable,
    outbound: Outbound = .{},
};

const DestinationStatus = struct {
    contract: DestinationContract = .{},
    cdc_unsupported_reason: []const u8 = "",
};

fn destinationStatus(a: A, server: *api.ApiHttpServer, table: Table, hosted_iceberg: bool) !DestinationStatus {
    const runtime_supported = try server.source.replicationSourcesSupported();
    const runtime_reason = if (runtime_supported) "" else "The current runtime does not execute managed replication sources.";
    const feeds = server.source.changefeeds != null;
    const lake_activation = feeds and server.source.vtable.get_lake_index_lifecycle != null and
        server.source.vtable.mutate_lake_index_lifecycle != null;
    const native = if (table.storage.engine == .local) server.cfg.native_history_roots else null;
    var binding = try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json);
    defer if (binding) |*owned| owned.deinit(a);
    if (binding == null and table.storage.engine != .object) return .{
        .contract = .{
            .adapter = .native,
            .input = .row_transactions,
            .accepted_by = .native_transaction,
            .committed_by = .native_transaction,
            .transaction_scope = .native_transaction,
            .committed_history = if (native != null) .native_rows_when_enrolled else .unavailable,
            .outbound = if (native) |roots| .{
                .document_committed = feeds,
                .snapshot_then_changes = feeds and roots.bootstrap_pages != null,
                .artifact_published = feeds and roots.artifact_history != null and roots.artifact_owners != null,
            } else .{},
        },
        .cdc_unsupported_reason = runtime_reason,
    };
    const external = if (binding) |owned| owned.binding else return .{ .cdc_unsupported_reason = "The table has no qualified writable Iceberg binding." };
    if (table.storage.engine == .object and external.format == .iceberg and external.write_policy == .iceberg_writer and external.row_identity != null) {
        try external.validateSupported();
        const rows = feeds and external.catalog != null and (server.cfg.deployment_mode == .standalone or hosted_iceberg);
        return .{
            .contract = .{
                .adapter = .writable_iceberg,
                .input = .row_transactions,
                .accepted_by = .iceberg_ingress,
                .committed_by = .iceberg_catalog_snapshot,
                .transaction_scope = .iceberg_table,
                .committed_history = if (rows) .iceberg_committed_wal else .unavailable,
                .outbound = .{ .document_committed = rows, .snapshot_then_changes = rows, .publication_activated = lake_activation },
            },
            .cdc_unsupported_reason = if (!runtime_supported) runtime_reason else if (server.cfg.deployment_mode == .standalone or hosted_iceberg) "" else "The metadata runtime has no configured Iceberg writer.",
        };
    }
    const committed_snapshot = external.format == .iceberg or
        (external.format == .parquet and external.object_mutability == .immutable and
            local.serverless_query_lake_parquet_manifest.isManifest(external.source_uri));
    return .{
        .contract = .{
            .input = if (committed_snapshot) .committed_snapshots else .unavailable,
            .committed_by = if (committed_snapshot) .external_snapshot else .none,
            .outbound = .{ .publication_activated = lake_activation },
        },
        .cdc_unsupported_reason = "The table has no qualified writable Iceberg binding.",
    };
}

pub fn configure(a: A, server: *api.ApiHttpServer, physical: []const u8, bound_id: ?u64, identity: ?api.AuthenticatedIdentity, context: local.api_operation.RequestContext, body: []const u8) ![]u8 {
    if (identity) |value| {
        if (!api.permissionsAllow(value.permissions, .table, physical, .admin) or api.hasEffectiveRowFilter(value, physical)) return error.Forbidden;
    }
    if (body.len > 4 * 1024 * 1024) return error.InvalidCreateTableRequest;
    var parsed = try std.json.parseFromSlice(Input, a, body, .{});
    defer parsed.deinit();
    if (parsed.value.replication_sources != .array) return error.InvalidCreateTableRequest;
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.ReadUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != parsed.value.table_id or (bound_id != null and bound_id.? != table.table_id)) return error.TableGenerationChanged;
    const hash = sourcesHash(table.replication_sources_json);
    if (!std.mem.eql(u8, &hash, parsed.value.expected_sources_hash)) return error.TableGenerationChanged;
    const source_count = parsed.value.replication_sources.array.items.len;
    if (source_count != 0) {
        if (table.storage.engine == .object) {
            const destination = try destinationStatus(a, server, table.*, snapshot.status.managed_iceberg_sources);
            if (destination.cdc_unsupported_reason.len != 0) return error.UnsupportedOperation;
        } else if (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) |external| {
            var binding = external;
            binding.deinit(a);
            return error.UnsupportedOperation;
        }
    }
    for (parsed.value.replication_sources.array.items) |source| {
        if (source != .object) return error.InvalidCreateTableRequest;
    }
    const raw = try std.json.Stringify.valueAlloc(a, parsed.value.replication_sources, .{});
    defer a.free(raw);
    const authorizer: authorization.Authorizer = .{ .manager = server.cfg.user_manager, .auth_enabled = server.cfg.auth_enabled };
    const sealed = try authorization.sealReplicationSourcesJsonForPrincipalAlloc(a, raw, physical, api.storedDestinationPrincipal(identity), authorizer);
    defer a.free(sealed);
    if (table.storage.engine == .object) @import("../metadata/replication_backfill.zig").validateLakeSourceDefinition(a, physical, sealed, source_count, authorizer) catch |err| switch (err) {
        error.UnsupportedReplicationRoute, error.UnsupportedReplicationSource => return error.UnsupportedOperation,
        error.InvalidReplicationSourceConfig => return error.InvalidCreateTableRequest,
        else => return err,
    };
    var replacement = table.*;
    replacement.replication_sources_json = sealed;
    try server.source.replaceTableDefinition(table.*, replacement);
    const next_hash = sourcesHash(sealed);
    return std.json.Stringify.valueAlloc(a, .{ .table_id = table.table_id, .sources_hash = &next_hash, .state = "configured" }, .{});
}
pub fn sourcesHash(bytes: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return std.fmt.bytesToHex(hash, .lower);
}

const PublicationProgress = struct {
    published_generation: ?u64 = null,
    published_snapshot: ?[]const u8 = null,
    building_generation: ?u64 = null,
    failed_generation: ?u64 = null,

    fn fromCatalog(state: local.metadata_lake_index_catalog.State) PublicationProgress {
        var result: PublicationProgress = .{};
        if (state.published) |published| {
            result.published_generation = published.generation;
            result.published_snapshot = switch (published.base_source) {
                .external_iceberg, .external_parquet, .external_lance => |snapshot| snapshot.snapshot_id,
                else => null,
            };
        }
        if (state.pending) |pending| result.building_generation = pending.generation;
        if (state.failure) |failure| result.failed_generation = failure.generation;
        return result;
    }
};

pub fn status(a: A, server: *api.ApiHttpServer, physical: []const u8, bound_id: ?u64, identity: ?api.AuthenticatedIdentity, context: local.api_operation.RequestContext) ![]u8 {
    if (identity) |value| if (!api.permissionsAllow(value.permissions, .table, physical, .admin) or api.hasEffectiveRowFilter(value, physical)) return error.Forbidden;
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.ReadUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (bound_id) |id| if (id != table.table_id) return error.TableGenerationChanged;
    var sources = try std.json.parseFromSlice(std.json.Value, a, table.replication_sources_json, .{});
    defer sources.deinit();
    const hash = sourcesHash(table.replication_sources_json);
    var progress: std.ArrayList(@import("../metadata/table_manager.zig").ReplicationSourceStatusRecord) = .empty;
    defer progress.deinit(a);
    for (snapshot.replication_source_statuses) |record| if (record.table_id == table.table_id) try progress.append(a, record);
    const destination = try destinationStatus(a, server, table.*, snapshot.status.managed_iceberg_sources);
    var publication = if (table.storage.engine == .object or table.lake_index_catalog_json.len != 0 or
        destination.contract.input == .committed_snapshots)
        try local.metadata_lake_index_catalog.parse(a, table.lake_index_catalog_json)
    else
        null;
    defer if (publication) |*parsed| parsed.deinit();
    return std.json.Stringify.valueAlloc(a, .{
        .table_id = table.table_id,
        .sources_hash = &hash,
        .replication_sources = sources.value,
        .progress = progress.items,
        .destination_contract = destination.contract,
        .index_publication = if (publication) |parsed| PublicationProgress.fromCatalog(parsed.value) else null,
        .cdc_supported = destination.cdc_unsupported_reason.len == 0,
        .cdc_unsupported_reason = destination.cdc_unsupported_reason,
    }, .{});
}

test "lake SQL managed source replacement checks native definition hash and table incarnation" {
    const a = std.testing.allocator;
    const metadata = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const Source = struct {
        table: [1]metadata.TableRecord,
        replacements: usize = 0,
        fn snapshot(raw: *anyopaque, context: local.api_operation.RequestContext) !?metadata_api.AdminSnapshot {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn replace(raw: *anyopaque, expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!metadata.tableDefinitionsEqual(self.table[0], expected)) return error.TableGenerationChanged;
            const owned = try std.testing.allocator.dupe(u8, replacement.replication_sources_json);
            std.testing.allocator.free(self.table[0].replication_sources_json);
            self.table[0].replication_sources_json = owned;
            self.replacements += 1;
        }
    };
    var source: Source = .{ .table = .{.{ .table_id = 7, .name = "hn", .schema_json = "{}", .indexes_json = "{}", .replication_sources_json = try a.dupe(u8, "[]") }} };
    defer a.free(source.table[0].replication_sources_json);
    var backend = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{});
    defer backend.deinit();
    var server = api.ApiHttpServer.init(a, .{ .backend_runtime = backend.ptr(), .deployment_mode = .standalone }, .{ .ptr = &source, .vtable = &.{ .status = undefined, .linearizable_snapshot = Source.snapshot, .free_admin_snapshot = Source.free, .replace_table_definition = Source.replace } }, .{ .ptr = &source, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } }, null);
    defer server.deinit();
    const hash = sourcesHash("[]");
    const body = try std.fmt.allocPrint(a, "{{\"table_id\":7,\"expected_sources_hash\":\"{s}\",\"replication_sources\":[]}}", .{hash});
    defer a.free(body);
    try std.testing.expectError(error.TableGenerationChanged, configure(a, &server, "hn", 8, null, .{}, body));
    const standalone_status = try status(a, &server, "hn", 7, null, .{});
    defer a.free(standalone_status);
    try std.testing.expect(std.mem.indexOf(u8, standalone_status, "\"cdc_supported\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, standalone_status, "\"committed_by\":\"native_transaction\"") != null);
    server.cfg.deployment_mode = .distributed;
    const result = try configure(a, &server, "hn", 7, null, .{}, body);
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 1), source.replacements);
    const wrong = "{\"table_id\":7,\"expected_sources_hash\":\"stale\",\"replication_sources\":[]}";
    try std.testing.expectError(error.TableGenerationChanged, configure(a, &server, "hn", 7, null, .{}, wrong));
    try std.testing.expectEqual(@as(usize, 1), source.replacements);

    source.table[0].storage.engine = .object;
    source.table[0].schema_json =
        \\{"base_source":{"kind":"external","table_id":"hn","uri":"file:///lake","format":"iceberg","write_policy":"iceberg_writer","catalog":{"type":"managed"},"row_identity":{"key_columns":["id"]}}}
    ;
    const attach = try std.fmt.allocPrint(
        a,
        "{{\"table_id\":7,\"expected_sources_hash\":\"{s}\",\"replication_sources\":[{{\"type\":\"postgres\",\"dsn\":\"test-dsn\",\"postgres_table\":\"source\",\"key_template\":\"id\"}}]}}",
        .{hash},
    );
    defer a.free(attach);
    try std.testing.expectError(error.UnsupportedOperation, configure(a, &server, "hn", 7, null, .{}, attach));
    const distributed_iceberg_status = try status(a, &server, "hn", 7, null, .{});
    defer a.free(distributed_iceberg_status);
    try std.testing.expect(std.mem.indexOf(u8, distributed_iceberg_status, "\"cdc_supported\":false") != null);
    server.source.changefeeds = .{ .ptr = &source, .read_fn = undefined, .mutate_fn = undefined, .authority_fn = undefined };
    const hosted = try destinationStatus(a, &server, source.table[0], true);
    try std.testing.expect(hosted.contract.transaction_scope == .iceberg_table);
    try std.testing.expect(hosted.contract.committed_history == .iceberg_committed_wal);
    try std.testing.expect(hosted.contract.outbound.document_committed);
    try std.testing.expect(hosted.contract.outbound.snapshot_then_changes);
    server.source.changefeeds = null;
    server.cfg.deployment_mode = .standalone;
    const standalone_iceberg_status = try status(a, &server, "hn", 7, null, .{});
    defer a.free(standalone_iceberg_status);
    try std.testing.expect(std.mem.indexOf(u8, standalone_iceberg_status, "\"cdc_supported\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, standalone_iceberg_status, "\"accepted_by\":\"iceberg_ingress\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, standalone_iceberg_status, "\"committed_by\":\"iceberg_catalog_snapshot\"") != null);
    const iceberg_result = try configure(a, &server, "hn", 7, null, .{}, attach);
    defer a.free(iceberg_result);
    try std.testing.expectEqual(@as(usize, 2), source.replacements);
    source.table[0].schema_json =
        \\{"base_source":{"kind":"external","table_id":"hn","uri":"file:///lake","format":"iceberg"}}
    ;
    source.table[0].storage.engine = .local;
    const current_hash = sourcesHash(source.table[0].replication_sources_json);
    const external_attach = try std.mem.replaceOwned(u8, a, attach, &hash, &current_hash);
    defer a.free(external_attach);
    try std.testing.expectError(error.UnsupportedOperation, configure(a, &server, "hn", 7, null, .{}, external_attach));
    const failed_publication = try local.metadata_lake_index_catalog.encode(a, .{
        .generation = 4,
        .failure = .{ .generation = 4, .desired = @splat(1), .reason = "test writer unavailable", .retry_at_ms = 1 },
    });
    defer a.free(failed_publication);
    source.table[0].lake_index_catalog_json = failed_publication;
    const external_status = try status(a, &server, "hn", 7, null, .{});
    defer a.free(external_status);
    try std.testing.expect(std.mem.indexOf(u8, external_status, "\"input\":\"committed_snapshots\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, external_status, "\"cdc_supported\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, external_status, "\"failed_generation\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, external_status, "\"published_snapshot\":null") != null);
}
