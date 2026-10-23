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
    fn deleteVersion(raw: *anyopaque, a: A, index: usize, file: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile, context: Context) !local.serverless_query_lake_serving.DeleteQualification.Version {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        try self.lease.readContext().ensureActive();
        return self.resolver.deleteSelector(a, index, file, context);
    }
    const Enumeration = struct {
        a: A,
        owner: *Owner,
        resolver: nav.Resolver,
        pages: @import("../serverless/graph_segment/page_store.zig").PageStore,
        store: @import("../serverless/artifacts/store.zig").ArtifactStore,
        cursor: ?@import("../serverless/graph_segment/page_tree.zig").Cursor = null,
        reads: u64 = std.math.maxInt(u64),
        writes: u64 = 0,
        seen: u32 = 0,
        full: bool = true,
        pending: ?@import("../serverless/graph_segment/page_tree.zig").Cursor.Record = null,
        fn next(raw: *anyopaque, a: A, context: Context) !?local.serverless_external_source_types.FileEntry {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.ensureActive();
            try self.owner.lease.readContext().ensureActive();
            const record = (self.pending orelse try self.cursor.?.next()) orelse {
                if (self.full and self.seen != self.owner.resolver.root.count) return error.InvalidNativeLakeFileState;
                return null;
            };
            self.pending = null;
            var file = try self.resolver.inventoryFileForScan(a, record.value);
            errdefer file.deinit(a);
            try context.ensureActive();
            const digest = local.storage_rowsource_identity.navigationDigest(self.owner.resolver.root.public_identity_version, self.owner.resolver.root.source, self.owner.resolver.root.snapshot, file.file_id);
            if (!std.mem.eql(u8, record.key, &digest) or self.seen >= self.owner.resolver.root.count) return error.InvalidNativeLakeFileState;
            self.seen += 1;
            return file;
        }
        fn hasMore(raw: *anyopaque, context: Context) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.ensureActive();
            try self.owner.lease.readContext().ensureActive();
            if (self.full) return self.seen < self.owner.resolver.root.count;
            if (self.pending == null) self.pending = try self.cursor.?.next();
            return self.pending != null;
        }
        fn split(raw: *anyopaque, a: A, index: usize, count: usize, context: Context) !Source.FileIterator {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.owner.enumerateRange(a, index, count, context);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cursor) |*cursor| cursor.deinit();
            self.resolver.deinit();
            self.a.destroy(self);
        }
    };
    fn enumerate(raw: *anyopaque, a: A, context: Context) !Source.FileIterator {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.enumerateRange(a, 0, 1, context);
    }
    fn enumerateRange(self: *Owner, a: A, index: usize, count: usize, context: Context) !Source.FileIterator {
        if (count == 0 or count > 256 or index >= count) return error.InvalidNativeLakeFileState;
        try context.ensureActive();
        const iterator = try a.create(Enumeration);
        errdefer a.destroy(iterator);
        iterator.* = .{ .a = a, .owner = self, .resolver = .{ .root = self.resolver.root, .store = self.store.artifactStore(), .cached = self.resolver.cached, .token = self.resolver.token, .page_cache_limit = 64 * 1024 }, .pages = undefined, .store = self.store.artifactStore() };
        iterator.pages = .{ .domain = self.resolver.root.domain, .artifacts = &iterator.store, .cancellation = self.resolver.token, .remaining_read_bytes = &iterator.reads, .remaining_write_bytes = &iterator.writes };
        const lower = [_]u8{@intCast(index * 256 / count)};
        const upper = [_]u8{@intCast(@min(255, (index + 1) * 256 / count))};
        iterator.full = count == 1;
        iterator.cursor = try @import("../serverless/graph_segment/page_tree.zig").Cursor.init(a, iterator.pages.store(), self.resolver.root.public, if (index == 0) "" else &lower, if (index + 1 == count) null else &upper);
        return .{ .ptr = iterator, .next = Enumeration.next, .split = Enumeration.split, .has_more = Enumeration.hasMore, .close = Enumeration.close };
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
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = self.cache, .scope = self.store.identity, .context = if (context.execution) |execution| held_context.withExecution(execution) else held_context };
        const pin = @import("lake_index_decoded_metadata.zig").acquire(nav.Root, cached, self.store.artifactStore(), ref, token, nav.load) catch |err| switch (err) {
            error.OutOfMemory, error.SqlMemoryLimitExceeded, error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.InvalidNativeLakeFileState, error.CloudGenerationMapUnavailable => return null,
            else => return err,
        };
        defer pin.release();
        const root = pin.value.*;
        const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(self.table_id, self.store.identity, self.publication.namespace);
        if (root.coverage == null or root.coverage.?.plan == null or (!std.mem.eql(u8, &root.coverage.?.plan.?, &plans.requested) and !std.mem.eql(u8, &root.coverage.?.plan.?, &plans.current) and !std.mem.eql(u8, &root.coverage.?.plan.?, &plans.selected)) or
            !std.mem.eql(u8, &root.domain, &domain) or !std.mem.eql(u8, root.source, header.source_id) or
            !std.mem.eql(u8, root.snapshot, header.snapshot_id) or !std.mem.eql(u8, root.schema, header.schema_fingerprint) or root.public_identity_version != header.row_identity_version) return null;
        if (root.delete_proof) |proof| if (!std.mem.eql(u8, &proof.source, &self.publication.signature.source)) return error.InvalidExternalLakeIndexCoverage;
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
        const delete_proof: ?local.serverless_query_lake_serving.DeleteQualification.Proof = if (root.delete_proof) |proof| proven: {
            break :proven .{ .ptr = owner, .count = proof.count, .recipe = proof.recipe, .digest = proof.digest, .source = proof.source, .data = proof.data, .lookup = Owner.deleteVersion };
        } else null;
        var statistics: std.ArrayList(local.sql_catalog.ColumnStatistics) = .empty;
        for (owned_root.costing) |column| if (column.complete) try statistics.append(oa, column.statistics);
        return .{ .statistics = statistics.items, .ptr = owner, .count = root.count, .plan = root.coverage.?.plan.?, .lookup = Owner.lookup, .close = Owner.close, .delete_proof = delete_proof, .enumerate = if (root.rows != null and root.bytes != null) Owner.enumerate else null, .estimates = if (root.rows != null and root.bytes != null) .{ .rows = root.rows.?, .bytes = root.bytes.? } else null };
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
    return openFor(a, server, table_id, catalog_json, schema, options, context, request, reader, null);
}
pub fn openFor(a: A, server: *@import("http_server.zig").ApiHttpServer, table_id: u64, catalog_json: []const u8, schema: local.storage_schema.TableSchema, options: anytype, context: Context, request: local.api_operation.RequestContext, reader: ?*Lease, execution: ?*local.common_request_admission.runnable_scheduler.Execution) !Source {
    const active = if (execution) |owner| context.withExecution(owner) else context;
    const binding = (schema.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    if (catalog_json.len == 0)
        return Source.openLazyCachedFor(a, schema, options, context, &server.lake_read_cache, execution);
    var state = try local.metadata_lake_index_catalog.parse(a, catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return Source.openLazyCachedFor(a, schema, options, context, &server.lake_read_cache, execution);
    if (publication.reader_protocol < 47 or publication.source_navigation == null)
        return Source.openLazyCachedFor(a, schema, options, context, &server.lake_read_cache, execution);
    if ((binding.format != .iceberg or binding.object_mutability != .immutable) and publication.source_navigation.?.metadata_version < 3)
        return Source.openLazyCachedFor(a, schema, options, context, &server.lake_read_cache, execution);
    var source_options: @import("antfly_local_sources").serverless_lake_host.OpenOptions = options;
    source_options.row_identity_version = binding.row_identity_version;
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return Source.openLazyCachedFor(a, schema, source_options, context, &server.lake_read_cache, execution);
    var store = Store.openNativeFor(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir, active) catch |err| {
        try optionalAdmission(context, reader != null, err);
        return Source.openLazyCachedFor(a, schema, source_options, context, &server.lake_read_cache, execution);
    };
    defer store.deinit();
    if (!std.mem.eql(u8, &store.identity, &publication.signature.store)) return Source.openLazyCachedFor(a, schema, source_options, context, &server.lake_read_cache, execution);
    store.implementation.cloud_io = context.io;
    const lease = reader orelse server.lake_reader_leases.acquireRetained(server.embedding_provider_runtime.io, authority, table_id, publication.generation, context, null) catch |err| {
        try optionalAdmission(context, false, err);
        return Source.openLazyCachedFor(a, schema, source_options, context, &server.lake_read_cache, execution);
    };
    defer if (reader == null) lease.deinit();
    var factory: Factory = .{ .store = &store, .publication = publication, .lease = lease, .cache = &server.lake_read_cache, .table_id = table_id, .required = reader != null };
    if ((binding.format != .iceberg and !local.serverless_query_lake_parquet_manifest.isManifest(binding.source_uri)) or binding.object_mutability != .immutable) {
        var source = try Source.openLazyCachedFor(a, schema, source_options, context, &server.lake_read_cache, execution);
        errdefer source.deinit();
        attachStatistics(a, &factory, &source, active) catch |err| {
            try context.ensureActive();
            if (err == error.OutOfMemory) return err;
            // Costing has no authority over required source reads.
        };
        return source;
    }
    return Source.openLazyNavigatedFor(a, schema, source_options, context, &server.lake_read_cache, .{ .ptr = &factory, .resolve = Factory.resolve }, execution);
}

/// The pin contains immutable costing data only. It performs no future reads
/// and needs no publication lease after the verified root has been copied.
fn attachStatistics(a: A, factory: *Factory, source: *Source, context: Context) !void {
    const ref = factory.publication.source_navigation orelse return;
    if (ref.metadata_version < 3) return;
    const binding = source.inventory;
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = factory.cache, .scope = factory.store.identity, .context = context };
    const token: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |value| .{ .ptr = value.ptr, .is_cancelled_fn = value.is_cancelled_fn } else .none;
    try factory.store.bindPublication(factory.publication, token);
    const pin = try @import("lake_index_decoded_metadata.zig").acquire(nav.Root, cached, factory.store.artifactStore(), ref, token, nav.load);
    var retained = false;
    defer if (!retained) pin.release();
    const root = pin.value.*;
    if (!std.mem.eql(u8, root.source, binding.source_id) or !std.mem.eql(u8, root.schema, binding.schema_fingerprint) or !std.mem.eql(u8, root.snapshot, binding.snapshot_id)) return;
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(factory.table_id, factory.store.identity, factory.publication.namespace);
    if (!std.mem.eql(u8, &root.domain, &domain)) return;
    const Owner_ = struct {
        a: A,
        pin: @TypeOf(pin),
        statistics: []local.sql_catalog.ColumnStatistics,
        fn close(raw: *anyopaque) void {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            owner.pin.release();
            owner.a.free(owner.statistics);
            owner.a.destroy(owner);
        }
    };
    const statistics = try a.alloc(local.sql_catalog.ColumnStatistics, root.costing.len);
    errdefer a.free(statistics);
    var count: usize = 0;
    for (root.costing) |column| if (column.complete) {
        statistics[count] = column.statistics;
        count += 1;
    };
    const owner = try a.create(Owner_);
    owner.* = .{ .a = a, .pin = pin, .statistics = statistics };
    source.planner_statistics = statistics[0..count];
    source.statistics_owner = .{ .ptr = owner, .close = Owner_.close };
    retained = true;
}
