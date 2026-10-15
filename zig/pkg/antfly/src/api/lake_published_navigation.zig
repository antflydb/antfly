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

//! Publication-bound cold source opening. Fresh metadata and credential scope
//! must match the authenticated plan before data manifests can be deferred.
const std = @import("std");
const local = @import("antfly_local_sources");
const nav = @import("lake_source_navigation.zig");
const Store = @import("lake_index_store.zig").Store;
const Lease = @import("lake_index_reader_lease.zig").Handle;
const Source = local.serverless_query_lake_serving.ServingSource;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    store: Store,
    lease: *Lease,
    resolver: nav.Resolver,
    fn lookup(raw: *anyopaque, a: A, id: []const u8, context: Context) !local.serverless_external_source_types.FileEntry {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        var file = try self.resolver.inventoryFile(a, id);
        errdefer file.deinit(a);
        try context.ensureActive();
        return file;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.resolver.deinit();
        self.store.deinit();
        self.lease.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};
const Factory = struct {
    store: *Store,
    publication: local.metadata_lake_index_catalog.Publication,
    lease: *Lease,
    cache: *local.serverless_query_lake_serving_cache.Cache,
    table_id: u64,
    required: bool,
    fn resolve(raw: *anyopaque, a: A, plans: Source.SnapshotPlans, header: local.serverless_external_source_types.Inventory, context: Context) !?Source.FileNavigation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.resolveVerified(a, plans, header, context) catch |err| {
            try optionalAdmission(context, self.required, err);
            return null;
        };
    }
    fn resolveVerified(self: *@This(), a: A, plans: Source.SnapshotPlans, header: local.serverless_external_source_types.Inventory, context: Context) !?Source.FileNavigation {
        const ref = self.publication.source_navigation orelse return null;
        const held_context = self.lease.readContext();
        const token: @import("antfly_cancellation").CancellationToken = if (held_context.cancellation) |value| .{ .ptr = value.ptr, .is_cancelled_fn = value.is_cancelled_fn } else .none;
        try context.ensureActive();
        try self.store.bindPublication(self.publication, token);
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = self.cache, .scope = self.store.identity, .context = held_context };
        const pin = @import("lake_index_decoded_metadata.zig").acquire(nav.Root, cached, self.store.artifactStore(), ref, token, nav.load) catch |err| switch (err) {
            error.OutOfMemory, error.SqlMemoryLimitExceeded, error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.InvalidNativeLakeFileState, error.CloudGenerationMapUnavailable => return null,
            else => return err,
        };
        defer pin.release();
        const root = pin.value.*;
        const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(self.table_id, self.store.identity, self.publication.namespace);
        if (root.coverage == null or root.coverage.?.plan == null or (!std.mem.eql(u8, &root.coverage.?.plan.?, &plans.requested) and !std.mem.eql(u8, &root.coverage.?.plan.?, &plans.current) and !std.mem.eql(u8, &root.coverage.?.plan.?, &plans.selected)) or
            !std.mem.eql(u8, &root.domain, &domain) or !std.mem.eql(u8, root.source, header.source_id) or
            !std.mem.eql(u8, root.snapshot, header.snapshot_id) or !std.mem.eql(u8, root.schema, header.schema_fingerprint)) return null;
        const owner = try a.create(Owner);
        errdefer a.destroy(owner);
        owner.a = a;
        owner.arena = .init(a);
        errdefer owner.arena.deinit();
        owner.lease = try self.lease.retain();
        errdefer owner.lease.deinit();
        owner.store = try self.store.reopenReadOnly(a);
        errdefer owner.store.deinit();
        const owned_context = owner.lease.readContext();
        owner.store.implementation.cloud_io = owned_context.io;
        try owner.store.bindPublication(self.publication, token);
        const oa = owner.arena.allocator();
        const owned_root = try std.json.parseFromSliceLeaky(nav.Root, oa, try std.json.Stringify.valueAlloc(oa, root, .{}), .{ .allocate = .alloc_always });
        var owned_reads = cached;
        owned_reads.context = owned_context;
        owner.resolver = .{ .root = owned_root, .store = owner.store.artifactStore(), .cached = owned_reads, .token = token };
        return .{ .ptr = owner, .count = root.count, .plan = root.coverage.?.plan.?, .lookup = Owner.lookup, .close = Owner.close };
    }
};
// Before navigation is adopted, an automatic SQL scan owes no native-index
// availability. Required readers retain their existing authority/error contract.
fn optionalAdmission(context: Context, required: bool, err: anyerror) !void {
    try context.ensureActive();
    if (required or err == error.OutOfMemory or err == error.MetadataMutationOutcomeUnknown) return err;
}
/// One entry point for native search and SQL. Fallback source contexts never
/// borrow the temporary native lease; adopted navigation owns an independent pin.
pub fn open(a: A, server: *@import("http_server.zig").ApiHttpServer, table_id: u64, catalog_json: []const u8, schema: local.storage_schema.TableSchema, options: anytype, context: Context, request: local.api_operation.RequestContext, reader: ?*Lease) !Source {
    const binding = (schema.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    if (binding.format != .iceberg or binding.object_mutability != .immutable or catalog_json.len == 0)
        return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    var state = try local.metadata_lake_index_catalog.parse(a, catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    if (publication.reader_protocol < 47 or publication.source_navigation == null)
        return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    var store = Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir) catch |err| {
        try optionalAdmission(context, reader != null, err);
        return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    };
    defer store.deinit();
    if (!std.mem.eql(u8, &store.identity, &publication.signature.store)) return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    store.implementation.cloud_io = context.io;
    const lease = reader orelse server.lake_reader_leases.acquireRetained(server.embedding_provider_runtime.io, authority, table_id, publication.generation, context, null) catch |err| {
        try optionalAdmission(context, false, err);
        return Source.openLazyCached(a, schema, options, context, &server.lake_read_cache);
    };
    defer if (reader == null) lease.deinit();
    var factory: Factory = .{ .store = &store, .publication = publication, .lease = lease, .cache = &server.lake_read_cache, .table_id = table_id, .required = reader != null };
    return Source.openLazyNavigated(a, schema, options, context, &server.lake_read_cache, .{ .ptr = &factory, .resolve = Factory.resolve });
}
