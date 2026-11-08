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

//! Native serving ownership. The catalog record is durable authority; caches
//! hold immutable bytes only. No TTL or local process state admits fresh reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const server_api = @import("http_server.zig");
const configured = @import("../serverless/configured_object_store_support.zig");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
pub const admission = lifecycle.source_admission;
const catalog = local.serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;
const Table = local.common_topology_records.TableRecord;
const Request = local.api_operation.RequestContext;
const Context = local.serverless_query_lake_read_context.Context;

pub fn scope(table: Table, owner: admission.State) lifecycle.SourceScope {
    return .{ .object_generation = table.object_storage_generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table), .owner = owner.owner };
}
fn sourceIdentity(binding: local.serverless_external_source_catalog_binding.Binding) [32]u8 {
    // Schema/index changes invalidate cuts through desiredFingerprint; they
    // must not change the physical WAL namespace or the source ownership nonce.
    return @import("lake_wal_image.zig").digest(.{ .format = "native-serving-source-v1", .kind = binding.format, .uri = binding.source_uri, .table = binding.table_id, .credentials = binding.credential_ref, .catalog = binding.catalog });
}
pub fn eligible(binding: local.serverless_external_source_catalog_binding.Binding) ?admission.Kind {
    if (binding.object_mutability != .immutable) return null;
    if (binding.format == .iceberg and binding.write_policy == .iceberg_writer and binding.catalog != null and binding.catalog.?.type == .managed) return .managed_iceberg;
    if (binding.format == .parquet and binding.write_policy == .read_only and local.serverless_query_lake_parquet_manifest.isManifest(binding.source_uri)) return .sealed_manifest;
    return null;
}
fn readOwner(a: A, server: *server_api.ApiHttpServer, table_id: u64, request: Request) !@import("../metadata/lake_source_owner_view.zig").View {
    const view = @import("../metadata/lake_source_owner_view.zig");
    if (server.source.vtable.system_catalog != null) {
        const bytes: ?[]u8 = server.source.systemCatalog(a, request, .{ .lake_source_owner_read = table_id }) catch |err| switch (err) {
            error.UnsupportedOperation => null,
            else => return err,
        };
        if (bytes) |encoded| return view.parse(a, encoded);
    }
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return .{};
    const state = try authority.readState(a, table_id);
    return .{ .dropped = state.dropped, .owner = state.source_owner };
}
pub fn lookup(a: A, server: *server_api.ApiHttpServer, table: Table, request: Request) !?admission.State {
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return null;
    defer binding.deinit(a);
    const state = try readOwner(a, server, table.table_id, request);
    const owner = state.owner orelse return null;
    if (state.dropped or owner.object_generation != table.object_storage_generation or !std.mem.eql(u8, &owner.source, &sourceIdentity(binding.binding)) or eligible(binding.binding) != owner.kind) return error.CatalogGenerationChanged;
    return owner;
}
/// Writes install this even if the configuration switch is subsequently off:
/// turning off an optimization cannot un-fence a durably owned physical source.
pub fn options(backing: A, server: *server_api.ApiHttpServer, table: Table, request: Request) !configured.BindingObjectStoreOpenOptions {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var result: configured.BindingObjectStoreOpenOptions = .{ .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store, .catalog_table_id = table.table_id, .catalog_generation = table.object_storage_generation, .native_owner_resolver = .{ .ptr = server, .resolve = resolve } };
    if (try lookup(a, server, table, request)) |owner| result.native_owner = .{ .token = owner.owner, .ptr = server, .table_id = table.table_id, .object_generation = table.object_storage_generation, .acknowledge = acknowledge };
    return result;
}
fn acknowledge(raw: *anyopaque, table_id: u64, generation: u64, nonce: [16]u8, lsn: ?u64, context: Context) anyerror!void {
    const server: *server_api.ApiHttpServer = @ptrCast(@alignCast(raw));
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const request: Request = .{ .deadline_ns = context.deadline_ns, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none };
    var snapshot = (try server.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = for (snapshot.tables) |record| {
        if (record.table_id == table_id) break record;
    } else return error.CatalogGenerationChanged;
    const owner = (try lookup(a, server, table, request)) orelse return error.CatalogGenerationChanged;
    if (generation != table.object_storage_generation or !std.mem.eql(u8, &nonce, &owner.owner)) return error.CatalogGenerationChanged;
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return error.MetadataSourceUnavailable;
    if (lsn) |accepted| {
        try authority.apply(table_id, .{ .acknowledge_source = .{ .scope = scope(table, owner), .lsn = accepted } });
    } else {
        authority.apply(table_id, .{ .invalidate_source = .{ .scope = scope(table, owner), .epoch = owner.epoch } }) catch |err| {
            const observed = try authority.readState(a, table_id);
            const newer = observed.source_owner orelse return err;
            // A native invalidation observed after the committed catalog write
            // fences every earlier observation too. Never replay an ambiguous
            // mutation merely to obtain a newer epoch.
            if (observed.dropped or newer.object_generation != generation or !std.mem.eql(u8, &newer.owner, &nonce) or newer.epoch <= owner.epoch) return err;
        };
    }
    // A lost wakeup is repaired by the native inventory sweep. Eager capture
    // makes the new accepted cut ready before a successful write returns.
    @import("lake_index_text_query.zig").refreshOwnedCut(a, server, table, request) catch {
        server.notifyLakeCommit(table.name) catch {};
    };
}
pub fn activate(a: A, server: *server_api.ApiHttpServer, table: Table, request: Request) !?admission.State {
    var schema = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return null;
    defer schema.deinit(a);
    const kind = eligible(schema.binding) orelse return null;
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return null;
    var state = try authority.readState(a, table.table_id);
    if (state.source_owner == null) {
        const config = server.cfg.node_config orelse return null;
        if (!config.lake_indexes.serving_cuts.enabled or state.current == null) return null;
        var owner: admission.State = .{ .kind = kind, .owner = undefined, .signing_key = undefined, .object_generation = table.object_storage_generation, .source = sourceIdentity(schema.binding) };
        try server.embedding_provider_runtime.io.randomSecure(&owner.owner);
        try server.embedding_provider_runtime.io.randomSecure(&owner.signing_key);
        authority.apply(table.table_id, .{ .configure_source = .{ .scope = scope(table, owner), .state = owner } }) catch |err| switch (err) {
            error.CatalogGenerationChanged => {},
            else => return err,
        };
        state = try authority.readState(a, table.table_id);
    }
    const owner = (try lookup(a, server, table, request)) orelse return null;
    if (owner.enabled) return owner;
    const normalized = try request.platformDeadline();
    const context: Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    const opts = try options(a, server, table, request);
    if (kind == .managed_iceberg) {
        var opened = try configured.openBindingObjectStoreAlloc(a, schema.binding, opts);
        defer opened.deinit();
        const managed: catalog.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = schema.binding.source_uri, .context = context, .native_owner = owner.owner };
        try managed.claimNativeOwner(a, owner.owner);
        const ingestion = @import("../serverless/lake_ingestion.zig");
        var queue = try ingestion.openQueue(a, schema.binding, opts);
        defer queue.deinit();
        const prefix = try ingestion.prefix(a, queue.prefix, schema.binding, opts);
        const store: @import("../serverless/lake_wal.zig").Store = .{ .client = queue.client, .bucket = queue.bucket, .prefix = prefix, .context = context, .native_owner = owner.owner };
        var wal = try store.open(a, true);
        defer wal.deinit();
        try wal.indexed.?.claimNativeOwner(store.collectionNamespace(), owner.owner);
    }
    // Claiming fences writers, but fresh reads retain strict coordination
    // until admit_source atomically enables a fully constructed first cut.
    return owner;
}

pub fn resolve(raw: *anyopaque, backing: A, table_id: u64, generation: u64, context: Context) anyerror!?configured.NativeSourceOwner {
    const server: *server_api.ApiHttpServer = @ptrCast(@alignCast(raw));
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const request: Request = .{ .deadline_ns = context.deadline_ns, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none };
    const state = try readOwner(a, server, table_id, request);
    const owner = state.owner orelse return null;
    if (state.dropped or owner.object_generation != generation) return error.CatalogGenerationChanged;
    return .{ .token = owner.owner, .ptr = server, .table_id = table_id, .object_generation = generation, .acknowledge = acknowledge };
}
pub fn openCursor(a: A, server: *server_api.ApiHttpServer, token: []const u8, request: Request) ![]const u8 {
    const capability = @import("lake_owner_capability.zig");
    const hint = try capability.peek(.composed, token);
    const state = try readOwner(a, server, hint.table_id, request);
    const owner = state.owner orelse return error.CatalogGenerationChanged;
    if (state.dropped or owner.object_generation != hint.object_generation) return error.CatalogGenerationChanged;
    const opened = try capability.open(a, .composed, .{ .table_id = hint.table_id, .object_generation = owner.object_generation, .owner = owner.owner }, owner.signing_key, token, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    return opened.payload;
}
