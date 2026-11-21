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

const std = @import("std");
const http = @import("http_server.zig");
const control = @import("../metadata/changefeed_control.zig");
const delivery = @import("../metadata/changefeed_delivery.zig");
const native = @import("../metadata/native_changefeed_delivery.zig");
const composed_context = @import("changefeed_provider_context.zig");
const artifacts = @import("artifact_history_operations.zig");
const publications = @import("lake_publication_changefeed.zig");
const catalog = @import("antfly_local_sources").system_catalog_domain;
const roots = @import("../metadata/native_changefeed_root.zig");
const config = @import("changefeed_metadata.zig");
const local = @import("antfly_local_sources");
const clock = @import("antfly_platform").time;
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;

pub const Selection = struct {
    id: control.Id,
    after: control.Digest,
};

const IcebergTable = struct {
    record: local.common_topology_records.TableRecord,
    schema: local.storage_schema.TableSchema,
};

const IcebergSchemas = struct {
    table: IcebergTable,

    pub fn tableAt(self: IcebergSchemas, a: A, id: i64, metadata: local.serverless_external_source_mod.lake_catalog.types.Table, request: Context) !local.sql_catalog.Table {
        try request.ensureActive();
        const snapshot = try std.fmt.allocPrint(a, "{d}", .{id});
        defer a.free(snapshot);
        var detected = try local.serverless_query_lake_schema.icebergSchema(a, metadata.metadata_json, snapshot);
        defer detected.deinit();
        const columns = try a.alloc(local.sql_catalog.Column, detected.columns.len);
        for (detected.columns, columns) |source, *column| {
            const name = try a.dupe(u8, source.name);
            const numeric = std.mem.eql(u8, source.kind, "numeric");
            column.* = .{
                .name = name,
                .path = name,
                .type = if (numeric) .number else std.meta.stringToEnum(@FieldType(local.sql_catalog.Column, "type"), source.kind) orelse return error.ChangefeedCapabilityUnsupported,
                .element_type = if (numeric) .numeric else null,
                .nullable = !source.required,
            };
        }
        var binding = self.table.schema.external_base_source.?;
        binding.binding.schema_fingerprint = try a.dupe(u8, detected.fingerprint);
        return .{ .id = self.table.record.table_id, .physical_name = self.table.record.name, .schema_version = 1, .external_base_source = binding, .columns = columns };
    }
};

const IcebergScheduler = struct {
    policy: *Policy,
    native_factory: ?RoutedFactory = null,

    pub fn authorize(self: *IcebergScheduler, state: control.State, request: Context) !void {
        try self.policy.authorize(state, request);
    }

    pub fn nowSeconds(self: *IcebergScheduler, request: Context) !u64 {
        return self.policy.nowSeconds(request);
    }

    pub fn prepare(self: *IcebergScheduler, a: A, state: control.State, before: control.Digest, limits: delivery.Limits, request: Context) !delivery.Prepared {
        const positions = try state.rootsAt(a, before);
        if (positions.len == 0) return error.ChangefeedResynchronizationRequired;
        const start = state.latest_delivery % positions.len;
        const partial = if (state.phase == .streaming) for (positions) |root| {
            if (root.delivered.event != 0) break root.namespace;
        } else null else null;
        const native_pending = if (state.phase == .streaming and self.native_factory != null)
            (try composed_context.Context.decode(a, try state.contextAt(before))).native.len != 0
        else
            false;
        var heartbeat: ?delivery.Prepared = null;
        var unavailable = false;
        for (0..positions.len) |offset| {
            try request.ensureActive();
            const root = positions[(start + offset) % positions.len];
            if (root.released or (state.phase == .bootstrap and root.snapshot_delivered.done)) continue;
            if (partial) |namespace| {
                if (!std.mem.eql(u8, &root.namespace, &namespace)) continue;
            }
            if (native_pending) _ = self.native_factory.?.captured.findInstalled(root) catch continue;
            const page = self.loadPage(a, state, root.namespace, before, null, limits, request) catch |err| switch (err) {
                error.ReadUnavailable => {
                    unavailable = true;
                    continue;
                },
                else => return err,
            };
            if (page.advances.len != 0) return page;
            if (heartbeat == null) heartbeat = page;
        }
        if (unavailable or partial != null or native_pending) return error.ReadUnavailable;
        if (heartbeat != null and self.native_factory != null) return mixedHeartbeat(a, state, before);
        return heartbeat orelse error.ChangefeedBackpressure;
    }

    pub fn replay(self: *IcebergScheduler, a: A, state: control.State, receipt: control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
        if (receipt.advances.len > 1 or state.roots.len == 0) return error.ChangefeedResynchronizationRequired;
        if (receipt.advances.len == 0 and self.native_factory != null) return mixedHeartbeat(a, state, receipt.before);
        const namespace = if (receipt.advances.len == 0) state.roots[0].namespace else receipt.advances[0].namespace;
        return self.loadPage(a, state, namespace, receipt.before, receipt, limits, request);
    }

    fn loadPage(self: *IcebergScheduler, a: A, state: control.State, namespace: control.Namespace, before: control.Digest, original: ?control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
        if (self.native_factory) |factory| {
            if (factory.captured.findInstalled(state.roots[try state.rootIndex(namespace)])) |_| {
                var root = try factory.open(namespace);
                var provider: native.ProviderFor(roots.Routed, Policy) = .{ .root = &root, .policy = self.policy };
                return mixedProviderPage(a, state, before, original, .native, &provider, limits, request);
            } else |err| if (err != error.ChangefeedResynchronizationRequired) return err;
        }
        const selected = for (state.definition.?.tables) |table| {
            if (std.mem.eql(u8, &control.catalogRootNamespace(table), &namespace)) break table;
        } else return error.ChangefeedResynchronizationRequired;
        if (selected.contract != .writable_iceberg_rows) return error.ChangefeedResynchronizationRequired;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const access = try self.policy.accessFor(selected.table_id);
        const table = try resolveWritableIceberg(self.policy.api, scratch, selected, access.storage_name);
        const binding = table.schema.external_base_source.?.binding;
        const options = try self.policy.api.lakeStreamOwnerOptions(.{ .allocator = scratch, .table = table.record, .request = request });
        const configured = @import("../serverless/configured_object_store_support.zig");
        var loaded = try configured.executeLakeCatalogAlloc(scratch, binding, options, try icebergRequestContext(options, request), .load);
        defer loaded.deinit(scratch);
        const changes = @import("lake_committed_changes.zig");
        var provider: changes.Provider(IcebergSchemas, Policy) = .{
            .namespace = namespace,
            .binding = binding,
            .options = options,
            .metadata = loaded.table,
            .schemas = .{ .table = table },
            .policy = self.policy,
        };
        if (self.native_factory != null) return mixedProviderPage(a, state, before, original, .iceberg, &provider, limits, request);
        if (original) |receipt| return provider.replay(a, state, receipt, limits, request);
        return provider.prepare(a, state, before, limits, request);
    }
};

fn mixedHeartbeat(a: A, state: control.State, before: control.Digest) !delivery.Prepared {
    const saved = try state.contextAt(before);
    return .{
        .body = try std.json.Stringify.valueAlloc(a, native.Envelope{ .events = &.{}, .transaction = null }, .{}),
        .events = 0,
        .proof = control.hash(.{ .domain = "antfly:mixed-changefeed-heartbeat:v1", .consumer = state.consumerId(), .before = before, .context = saved }),
        .advances = &.{},
        .provider_context = try a.dupe(u8, saved),
    };
}

fn mixedProviderPage(a: A, state: control.State, before: control.Digest, original: ?control.Delivery, provider: composed_context.Provider, adapter: anytype, limits: delivery.Limits, request: Context) !delivery.Prepared {
    const previous = try composed_context.Context.decode(a, try state.contextAt(before));
    adapter.provider_context_override = previous.get(provider);
    defer adapter.provider_context_override = null;
    var prepared = if (original) |receipt| blk: {
        var isolated = receipt;
        const target = try composed_context.Context.decode(a, receipt.provider_context);
        isolated.provider_context = target.get(provider);
        break :blk try adapter.replay(a, state, isolated, limits, request);
    } else try adapter.prepare(a, state, before, limits, request);
    prepared.provider_context = try previous.replace(provider, prepared.provider_context).encode(a);
    return prepared;
}

fn icebergRequestContext(options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions, request: Context) !local.serverless_external_source_mod.lake_catalog.types.Context {
    const normalized = try request.platformDeadline();
    return .{
        .io = options.io,
        .deadline_ns = normalized.deadline_ns,
        .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn),
    };
}

fn executeIceberg(client: *config.Client, a: A, selection: Selection, limits: ?delivery.Limits, policy: *Policy, native_factory: ?RoutedFactory, request: Context) !Result {
    var scheduler: IcebergScheduler = .{ .policy = policy, .native_factory = native_factory };
    if (limits) |bounds| return .{ .page = try delivery.pull(client, &scheduler, a, selection.id, selection.after, bounds, request) };
    var acknowledged = try delivery.acknowledge(client, &scheduler, a, selection.id, selection.after, request);
    defer acknowledged.deinit();
    const confirmed = acknowledged.value;
    if (confirmed.phase == .bootstrap and confirmed.bootstrapComplete()) {
        try change(client, a, confirmed, .{ .finish_verified_bootstrap = .{ .proof = try confirmed.bootstrapCompletionProof() } }, request);
    }
    if (confirmed.pendingCatalogAcknowledgment() != null) {
        const owner = CatalogFeedOwner.local(policy.api);
        try settleCatalogAcknowledgment(client, a, confirmed, owner, request);
    }
    if (native_factory) |factory| {
        var refreshed = try load(client, a, confirmed.id, request);
        defer refreshed.deinit();
        const live = refreshed.value;
        if (live.epoch != confirmed.epoch or !same(live.authority, confirmed.authority)) return error.ChangefeedResynchronizationRequired;
        if (live.phase == .streaming) {
            var driver: RootPolicy(Policy) = .{ .access = policy.*, .io = policy.api.inferenceIo(), .source = factory.source, .allocator = a };
            for (factory.captured.roots) |route| {
                const item = live.roots[try live.rootIndex(route.namespace)];
                if (item.snapshot != null and !item.snapshot_released) {
                    try releaseStreamingSnapshot(client, &driver, a, live, route, request);
                    break;
                }
            }
            for (factory.captured.roots) |route| _ = try source_api.flushAcknowledged(client, &driver, a, live, route, request);
        }
    }
    return .{ .acknowledged = confirmed.acknowledged_cursor };
}

fn pinIcebergPositions(a: A, state: control.State, policy: *Policy, expires_at: u64, request: Context) !void {
    const owner = CatalogFeedOwner.local(policy.api);
    for (state.definition.?.tables) |selected| {
        if (selected.contract != .writable_iceberg_rows) continue;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const access = try policy.accessFor(selected.table_id);
        const table = try resolveWritableIceberg(policy.api, scratch, selected, access.storage_name);
        try pinIcebergPosition(scratch, state, selected, table, owner, expires_at, request);
    }
}

fn pinIcebergPosition(a: A, state: control.State, selected: control.Table, table: IcebergTable, owner: CatalogFeedOwner, expires_at: u64, request: Context) !void {
    const changes = @import("lake_committed_changes.zig");
    const encoded = if (allIceberg(state.definition.?))
        state.acknowledged_provider_context
    else
        (try composed_context.Context.decode(a, state.acknowledged_provider_context)).iceberg;
    const binding = table.schema.external_base_source.?.binding;
    const options = try owner.options_fn(owner.ptr, .{ .allocator = a, .table = table.record, .request = request });
    const position = try changes.decodePosition(a, changes.feedScope(state), selected.table_id, encoded);
    const namespace = control.catalogRootNamespace(selected);
    if (position.through != state.roots[try state.rootIndex(namespace)].acknowledged.sequence) return error.ChangefeedResynchronizationRequired;
    try @import("../serverless/lake_ingestion.zig").pinCommittedWalPosition(a, binding, options, try icebergRequestContext(options, request), state.consumerId(), position, expires_at);
}

fn settleCatalogAcknowledgment(client: *config.Client, a: A, confirmed: control.State, owner: CatalogFeedOwner, request: Context) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var parsed = try load(client, scratch, confirmed.id, request);
    defer parsed.deinit();
    const state = parsed.value;
    if (state.epoch != confirmed.epoch or !same(state.authority, confirmed.authority) or state.phase != .streaming)
        return error.ChangefeedResynchronizationRequired;
    const selected = state.pendingCatalogAcknowledgment() orelse return;
    const namespace = control.catalogRootNamespace(selected);
    const root = state.roots[try state.rootIndex(namespace)];
    var view = (try owner.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
    defer owner.source.freeAdminSnapshot(&view);
    const observed = for (view.tables) |table| {
        if (table.table_id == selected.table_id) break table;
    } else return error.ChangefeedResynchronizationRequired;
    const table = try writableIcebergFromRecord(scratch, selected, observed);
    try pinIcebergPosition(scratch, state, selected, table, owner, state.expires_at, request);
    try change(client, scratch, state, .{ .reclaimed = .{
        .namespace = namespace,
        .consumer_epoch = root.consumer_epoch,
        .previous = root.reclaimed_through,
        .next = root.acknowledged.sequence,
    } }, request);
}

/// Recheck the physical table incarnation before a retained position can be
/// used. An Iceberg UUID alone does not bind Antfly's catalog ownership.
fn resolveWritableIceberg(api: *http.ApiHttpServer, a: A, selected: control.Table, storage_name: []const u8) !IcebergTable {
    const record = (try api.source.tableRecord(a, storage_name)) orelse return error.ChangefeedResynchronizationRequired;
    return writableIcebergFromRecord(a, selected, record);
}

fn writableIcebergFromRecord(a: A, selected: control.Table, record: @FieldType(IcebergTable, "record")) !IcebergTable {
    if (record.table_id != selected.table_id or !same(control.tableBinding(record), selected.binding))
        return error.ChangefeedResynchronizationRequired;
    const parsed = try local.schema_mod.parseValidatedTableSchema(a, record.schema_json);
    const schema = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    const external = schema.external_base_source orelse return error.ChangefeedResynchronizationRequired;
    if (external.binding.format != .iceberg or external.binding.write_policy != .iceberg_writer or external.binding.catalog == null)
        return error.ChangefeedResynchronizationRequired;
    return .{ .record = record, .schema = schema };
}

const TableAccess = struct {
    id: u64,
    storage_name: []const u8,
    permission_name: []const u8,
    filter: ?[]const u8,
};

/// Borrowed credentials live for one HTTP request. Each authorization check
/// reloads the effective policy before a durable receipt or payload can escape.
const Policy = struct {
    api: *http.ApiHttpServer,
    credential: http.AuthenticatedRequest,
    tables: []const TableAccess = &.{},

    pub fn authorize(self: *Policy, state: control.State, request: Context) !void {
        try request.ensureActive();
        const definition = state.definition orelse return error.Forbidden;
        var identity = try authenticate(self.api, self.credential);
        defer identity.deinit(self.api.alloc);
        try requireOwner(identity, definition);
        if (definition.tables.len != self.tables.len) return error.ChangefeedResynchronizationRequired;
        for (definition.tables, self.tables) |table, access| {
            try request.ensureActive();
            if (table.table_id != access.id) return error.ChangefeedResynchronizationRequired;
            if (table.contract == .writable_iceberg_rows) {
                var arena = std.heap.ArenaAllocator.init(self.api.alloc);
                defer arena.deinit();
                _ = try resolveWritableIceberg(self.api, arena.allocator(), table, access.storage_name);
            } else if (table.contract == .external_snapshot_lifecycle) {
                var arena = std.heap.ArenaAllocator.init(self.api.alloc);
                defer arena.deinit();
                const record = (try self.api.source.tableRecord(arena.allocator(), access.storage_name)) orelse return error.ChangefeedResynchronizationRequired;
                if (record.table_id != table.table_id or !same(control.tableBinding(record), table.binding)) return error.ChangefeedResynchronizationRequired;
            }
            const current_name = try resolvePermissionName(self.api, self.api.alloc, access.storage_name, request);
            defer self.api.alloc.free(current_name);
            if (!std.mem.eql(u8, current_name, access.permission_name)) return error.ChangefeedResynchronizationRequired;
            if (!try http.tablePermissionCurrentlyAllowed(identity, current_name, .replicate)) return error.Forbidden;
            const filter = try http.resolveEffectiveRowFilterJson(self.api.alloc, identity, current_name);
            defer if (filter) |bytes| self.api.alloc.free(bytes);
            if (metadataEvent(definition) and filter != null) return error.Forbidden;
            if (!optionalEqual(access.filter, filter)) return error.ChangefeedResynchronizationRequired;
        }
        if (!same(definition.authorization, authorization(identity, definition.tables, self.tables)))
            return error.ChangefeedResynchronizationRequired;
        try request.ensureActive();
    }

    pub fn nowSeconds(_: *Policy, _: Context) !u64 {
        return clock.realtimeNs() / std.time.ns_per_s;
    }

    pub fn images(self: *Policy, a: A, _: control.State, header: anytype, key: []const u8, before: ?std.json.Value, after: ?std.json.Value, request: Context) !native.Images {
        return self.imagesFor(a, std.mem.readInt(u64, header.namespace[0..8], .big), key, before, after, request);
    }

    pub fn imagesFor(self: *Policy, a: A, table_id: u64, key: []const u8, before: ?std.json.Value, after: ?std.json.Value, request: Context) !native.Images {
        try request.ensureActive();
        const access = try self.accessFor(table_id);
        return .{
            .before = .{ .row = before, .readable = try visible(access.filter, a, key, before) },
            .after = .{ .row = after, .readable = try visible(access.filter, a, key, after) },
        };
    }

    pub fn snapshotImageFor(self: *Policy, a: A, _: control.State, table_id: u64, key: []const u8, row: ?std.json.Value, request: Context) !@import("antfly_local_sources").api_local_table_writes.table_write_source.changefeed_projection.Image {
        try request.ensureActive();
        return .{ .row = row, .readable = try visible((try self.accessFor(table_id)).filter, a, key, row) };
    }

    fn accessFor(self: *const Policy, id: u64) !TableAccess {
        for (self.tables) |access| if (access.id == id) return access;
        return error.ChangefeedResynchronizationRequired;
    }

    fn visible(policy_filter: ?[]const u8, a: A, key: []const u8, row: ?std.json.Value) !bool {
        const value = row orelse return false;
        const filter = policy_filter orelse return true;
        const json = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(json);
        return local.search_pattern_filter.storedDocMatchesPatternFilter(a, key, json, filter);
    }
};

fn same(left: control.Digest, right: control.Digest) bool {
    return std.crypto.timing_safe.eql(control.Digest, left, right);
}

fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |bytes| return std.mem.eql(u8, bytes, right orelse return false);
    return right == null;
}

fn authenticate(api: *http.ApiHttpServer, credential: http.AuthenticatedRequest) !http.AuthenticatedIdentity {
    return api.authenticateRequest(credential) catch |err| switch (err) {
        error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => error.Forbidden,
        else => err,
    };
}

pub fn durableOwner(identity: http.AuthenticatedIdentity) !control.Digest {
    if (identity.is_internal_service or identity.credential_principal.len == 0) return error.Forbidden;
    if (identity.trusted_changefeed_owner) |owner| return owner;
    const manager = identity.live_user_manager orelse return error.Forbidden;
    const subject = control.hash(.{
        .domain = "antfly:changefeed-owner:v1",
        .username = identity.username,
        .credential = identity.credential_principal,
        .instance = identity.user_instance_id,
    });
    if (identity.user_instance_id) |instance|
        return manager.destinationGrantMacForUserInstance(identity.credential_principal, &subject, instance);
    return manager.destinationGrantMac(identity.credential_principal, &subject);
}

fn requireOwner(identity: http.AuthenticatedIdentity, definition: control.Definition) !void {
    if (!same(definition.owner, try durableOwner(identity))) return error.Forbidden;
}

fn authorization(identity: http.AuthenticatedIdentity, tables: []const control.Table, access: []const TableAccess) control.Digest {
    if (access.len != 1) return control.hash(.{
        .domain = "antfly:changefeed-table-set-authorization:v1",
        .credential = identity.credential_principal,
        .tables = tables,
        .access = access,
    });
    const table = access[0].storage_name;
    const permission_name = access[0].permission_name;
    const filter = access[0].filter;
    if (!std.mem.eql(u8, table, permission_name)) return control.hash(.{
        .domain = "antfly:changefeed-catalog-authorization:v1",
        .credential = identity.credential_principal,
        .tables = tables,
        .permission_name = permission_name,
        .filter = filter,
    });
    return control.hash(.{
        .domain = "antfly:changefeed-authorization:v1",
        .credential = identity.credential_principal,
        .tables = tables,
        .filter = filter,
    });
}

fn resolveStorageName(api: *http.ApiHttpServer, a: A, logical: []const u8, context: Context) ![]const u8 {
    if (api.source.vtable.system_catalog == null) return a.dupe(u8, logical);
    const target = try catalog.Target.parse(logical);
    const bytes = try api.source.systemCatalog(a, context, .{ .resolve = target });
    defer a.free(bytes);
    var result = try std.json.parseFromSlice(?catalog.ResolvedTable, a, bytes, .{});
    defer result.deinit();
    const table = result.value orelse return error.Forbidden;
    if (table.native_service != null) return error.Forbidden;
    return a.dupe(u8, table.name);
}

fn resolvePermissionName(api: *http.ApiHttpServer, a: A, storage_name: []const u8, context: Context) ![]const u8 {
    if (api.source.vtable.system_catalog == null) return a.dupe(u8, storage_name);
    const bytes = try api.source.systemCatalog(a, context, .{ .resolve_many = .{ .storage_names = &.{storage_name} } });
    defer a.free(bytes);
    var result = try std.json.parseFromSlice(catalog.ResolvedMany, a, bytes, .{});
    defer result.deinit();
    if (result.value.logical_names.len != 1) return error.ChangefeedCapabilityUnsupported;
    return a.dupe(u8, result.value.logical_names[0] orelse return error.ChangefeedResynchronizationRequired);
}

pub const Result = union(enum) {
    page: delivery.Result,
    acknowledged: control.Digest,

    pub fn deinit(self: *Result) void {
        if (self.* == .page) self.page.deinit();
        self.* = undefined;
    }
};

fn hasIceberg(definition: control.Definition) bool {
    for (definition.tables) |table| if (table.contract == .writable_iceberg_rows) return true;
    return false;
}

fn nativeDefinition(a: A, definition: control.Definition) !control.Definition {
    var tables: std.ArrayList(control.Table) = .empty;
    for (definition.tables) |table| {
        if (table.contract != .writable_iceberg_rows) try tables.append(a, table);
    }
    var selected = definition;
    selected.tables = try tables.toOwnedSlice(a);
    return selected;
}

fn allIceberg(definition: control.Definition) bool {
    if (definition.tables.len == 0) return false;
    for (definition.tables) |table| if (table.contract != .writable_iceberg_rows) return false;
    return true;
}

fn authorizedIcebergTables(api: *http.ApiHttpServer, a: A, identity: http.AuthenticatedIdentity, definition: control.Definition, request: Context) ![]const TableAccess {
    var snapshot = (try api.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
    defer api.source.freeAdminSnapshot(&snapshot);
    const access = try a.alloc(TableAccess, definition.tables.len);
    for (definition.tables, access) |selected, *entry| {
        try request.ensureActive();
        const table = for (snapshot.tables) |record| {
            if (record.table_id == selected.table_id) break record;
        } else return error.ChangefeedResynchronizationRequired;
        if (!same(control.tableBinding(table), selected.binding)) return error.ChangefeedResynchronizationRequired;
        const storage_name = try a.dupe(u8, table.name);
        const permission_name = try resolvePermissionName(api, a, storage_name, request);
        if (!try http.tablePermissionCurrentlyAllowed(identity, permission_name, .replicate)) return error.Forbidden;
        entry.* = .{
            .id = selected.table_id,
            .storage_name = storage_name,
            .permission_name = permission_name,
            .filter = try http.resolveEffectiveRowFilterJson(a, identity, permission_name),
        };
    }
    return access;
}

fn authorizedTableAccess(api: *http.ApiHttpServer, a: A, identity: http.AuthenticatedIdentity, definition: control.Definition, captured: *const source_api.Captured, internal: Context) ![]const TableAccess {
    const access = try a.alloc(TableAccess, definition.tables.len);
    var root_index: usize = 0;
    for (definition.tables, access) |table, *item| {
        if (root_index >= captured.roots.len) return error.ChangefeedResynchronizationRequired;
        const route = captured.roots[root_index];
        if (route.fence.table_id != table.table_id) return error.ChangefeedResynchronizationRequired;
        const permission_name = try resolvePermissionName(api, a, route.table_name, internal);
        item.* = .{
            .id = table.table_id,
            .storage_name = route.table_name,
            .permission_name = permission_name,
            .filter = try http.resolveEffectiveRowFilterJson(a, identity, permission_name),
        };
        while (root_index < captured.roots.len and captured.roots[root_index].fence.table_id == table.table_id) : (root_index += 1) {}
    }
    return access;
}

pub const Progress = struct {
    phase: control.Phase,
    acknowledged_cursor: [@sizeOf(control.Digest) * 2]u8,
    unacknowledged_deliveries: usize,
    oldest_unacknowledged_at: ?u64,
    cursor_window: u16,
    cursor_byte_limit: u64,
    expires_at: u64,
    bootstrap_expires_at: ?u64,
};

pub fn inspect(api: *http.ApiHttpServer, backing: A, credential: http.AuthenticatedRequest, id: control.Id, public: Context) !Progress {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var parsed = try load(&client, a, id, internal);
    defer parsed.deinit();
    const state = parsed.value;
    const definition = state.definition orelse return error.Forbidden;
    try requireOwner(identity, definition);
    if (state.slot != null) return error.ChangefeedCapabilityUnsupported;
    if (state.phase != .streaming and state.phase != .bootstrap)
        return error.ChangefeedResynchronizationRequired;
    if (state.expiryDue(clock.realtimeNs() / std.time.ns_per_s))
        return error.ChangefeedLeaseExpired;
    if (hasIceberg(definition) or metadataEvent(definition)) {
        const access = try authorizedIcebergTables(api, a, identity, definition, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        try policy.authorize(state, internal);
    } else {
        const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        var captured = try source.capture(a, definition, internal);
        defer captured.deinit();
        try captured.validateInitialRoutes(state.roots);
        const access = try authorizedTableAccess(api, a, identity, definition, &captured, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        try policy.authorize(state, internal);
    }
    return .{
        .phase = state.phase,
        .acknowledged_cursor = std.fmt.bytesToHex(state.acknowledged_cursor, .lower),
        .unacknowledged_deliveries = state.cursors.len,
        .oldest_unacknowledged_at = if (state.cursors.len != 0) state.cursors[0].issued_at else null,
        .cursor_window = definition.cursor_window,
        .cursor_byte_limit = definition.cursor_byte_limit,
        .expires_at = state.expires_at,
        .bootstrap_expires_at = if (state.phase == .bootstrap) state.bootstrap_expires_at else null,
    };
}

fn renewIceberg(client: *config.Client, a: A, state: control.State, policy: *Policy, now: u64, expires_at: u64, request: Context) !u64 {
    try policy.authorize(state, request);
    if (expires_at <= state.expires_at) return state.expires_at;
    if (hasIceberg(state.definition.?)) try pinIcebergPositions(a, state, policy, expires_at, request);
    try policy.authorize(state, request);
    try change(client, a, state, .{ .renew_catalog = .{ .now = now, .expires_at = expires_at } }, request);
    return expires_at;
}

/// Extend a streaming feed to an absolute Unix timestamp. The same request
/// can resume after a lost reply while the original root leases still exist.
pub fn renew(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, id: control.Id, expires_at: u64, public: Context) !u64 {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    var parsed = try load(&client, a, id, internal);
    defer parsed.deinit();
    const definition = parsed.value.definition orelse return error.Forbidden;
    try requireOwner(identity, definition);
    if (parsed.value.slot != null or parsed.value.phase != .streaming) return error.ChangefeedConflict;
    const now = clock.realtimeNs() / std.time.ns_per_s;
    if (parsed.value.expiryDue(now)) return error.ChangefeedLeaseExpired;
    if (expires_at <= now or expires_at - now > control.max_lease_seconds) return error.InvalidChangefeed;

    if (hasIceberg(definition) or metadataEvent(definition)) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const access = try authorizedIcebergTables(api, scratch, identity, definition, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        const renewed = try renewIceberg(&client, scratch, parsed.value, &policy, now, expires_at, internal);
        if (allIceberg(definition) or metadataEvent(definition)) return renewed;
        var latest = try load(&client, scratch, id, internal);
        defer latest.deinit();
        const live = latest.value;
        if (live.epoch != parsed.value.epoch or !same(live.authority, parsed.value.authority)) return error.ChangefeedResynchronizationRequired;
        const native_source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        var routes = try native_source.capture(scratch, live.definition orelse return error.ChangefeedConflict, internal);
        defer routes.deinit();
        try routes.validateInstalledRoutes(live);
        var driver: RootPolicy(Policy) = .{ .access = policy, .io = api.inferenceIo(), .source = native_source, .allocator = scratch };
        for (routes.roots) |route| try source_api.renewRoot(&client, &driver, scratch, live, route, internal);
        return live.expires_at;
    }
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    try captured.validateInitialRoutes(parsed.value.roots);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const access = try authorizedTableAccess(api, arena.allocator(), identity, definition, &captured, internal);
    var policy: RootPolicy(Policy) = .{ .access = .{ .api = api, .credential = credential, .tables = access }, .io = api.inferenceIo(), .source = source, .allocator = a };
    try policy.access.authorize(parsed.value, internal);
    const epoch = parsed.value.epoch;
    const authority = parsed.value.authority;
    if (expires_at > parsed.value.expires_at) {
        try change(&client, a, parsed.value, .{ .renew = .{ .now = now, .expires_at = expires_at } }, internal);
        const updated = try load(&client, a, id, internal);
        parsed.deinit();
        parsed = updated;
        if (parsed.value.epoch != epoch or !same(parsed.value.authority, authority) or parsed.value.expires_at < expires_at)
            return error.ChangefeedResynchronizationRequired;
    }
    for (captured.roots) |route| try source_api.renewRoot(&client, &policy, a, parsed.value, route, internal);
    return parsed.value.expires_at;
}

/// Public cursors identify previously issued positions. Metadata maintenance
/// and owner history reads only receive a locally constructed request context.
pub fn execute(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, selection: Selection, limits: ?delivery.Limits, public: Context) !Result {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    const bytes = try client.readChangefeedState(a, selection.id, internal);
    defer a.free(bytes);
    var state = try control.parse(a, bytes);
    defer state.deinit();
    const definition = state.value.definition orelse return error.Forbidden;
    try requireOwner(identity, definition);
    var access_arena = std.heap.ArenaAllocator.init(a);
    defer access_arena.deinit();
    const scratch = access_arena.allocator();
    if (metadataEvent(definition)) {
        const access = try authorizedIcebergTables(api, scratch, identity, definition, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        var captured: ?source_api.Captured = null;
        defer if (captured) |*routes| routes.deinit();
        var artifact_port: ?artifacts.Port = null;
        if (definition.events == artifacts.artifact_event) {
            const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
            artifact_port = source.artifact_history orelse return error.ChangefeedCapabilityUnsupported;
            captured = try source.capture(scratch, definition, internal);
            try captured.?.validateInitialRoutes(state.value.roots);
        }
        var adapter: ActivationScheduler = .{ .policy = &policy, .routes = if (captured) |*routes| routes else null, .artifacts = artifact_port };
        if (limits) |bounds| return .{ .page = try delivery.pull(&client, &adapter, a, selection.id, selection.after, bounds, internal) };
        var acknowledged = try delivery.acknowledge(&client, &adapter, a, selection.id, selection.after, internal);
        defer acknowledged.deinit();
        return .{ .acknowledged = acknowledged.value.acknowledged_cursor };
    }
    if (allIceberg(definition)) {
        if (state.value.roots.len != definition.tables.len or (state.value.phase != .streaming and state.value.phase != .bootstrap))
            return error.ChangefeedCapabilityUnsupported;
        const access = try authorizedIcebergTables(api, scratch, identity, definition, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        return executeIceberg(&client, a, selection, limits, &policy, null, internal);
    }
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    if (hasIceberg(definition)) {
        const native_definition = try nativeDefinition(scratch, definition);
        var catalog_routes = try source.capturePhysical(a, native_definition, internal);
        defer catalog_routes.deinit();
        const access = try authorizedIcebergTables(api, scratch, identity, definition, internal);
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        try policy.authorize(state.value, internal);
        try stageVisibleSplits(&client, a, source, &catalog_routes, &state, selection.after, internal);
        try stageVisibleMerges(&client, a, source, &catalog_routes, &state, selection.after, internal);
        try progressMergeDrain(&client, a, source, &catalog_routes, state.value, selection.after, internal);
        try progressSplitSuccessor(&client, a, source, &catalog_routes, state.value, selection.after, internal);
        return executeIceberg(&client, a, selection, limits, &policy, .{ .source = source, .captured = &catalog_routes }, internal);
    }
    var captured = try source.capturePhysical(a, definition, internal);
    defer captured.deinit();
    if (captured.roots.len == 0) return error.ChangefeedResynchronizationRequired;
    const access = try authorizedTableAccess(api, scratch, identity, definition, &captured, internal);
    var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
    try policy.authorize(state.value, internal);
    try stageVisibleSplits(&client, a, source, &captured, &state, selection.after, internal);
    try stageVisibleMerges(&client, a, source, &captured, &state, selection.after, internal);
    try progressMergeDrain(&client, a, source, &captured, state.value, selection.after, internal);
    try progressSplitSuccessor(&client, a, source, &captured, state.value, selection.after, internal);
    var provider: native.ScheduledProvider(roots.Routed, RoutedFactory, Policy) = .{ .factory = .{ .source = source, .captured = &captured }, .policy = &policy };
    if (limits) |bounds|
        return .{ .page = try delivery.pull(&client, &provider, a, selection.id, selection.after, bounds, internal) };
    var confirmed = try delivery.acknowledge(&client, &provider, a, selection.id, selection.after, internal);
    defer confirmed.deinit();
    var driver_policy: RootPolicy(Policy) = .{ .access = policy, .io = api.inferenceIo(), .source = source, .allocator = a };
    var active = confirmed.value;
    var cutover: ?control.Parsed = null;
    defer if (cutover) |*parsed| parsed.deinit();
    if (active.phase == .bootstrap) {
        if (!active.bootstrapComplete())
            return .{ .acknowledged = active.acknowledged_cursor };
        try change(&client, a, active, .{ .finish_verified_bootstrap = .{ .proof = try active.bootstrapCompletionProof() } }, internal);
        cutover = try load(&client, a, selection.id, internal);
        active = cutover.?.value;
        if (active.phase != .streaming or active.epoch != confirmed.value.epoch or !same(active.acknowledged_cursor, selection.after))
            return error.MetadataMutationOutcomeUnknown;
    }
    for (active.roots) |item| {
        if (item.released) continue;
        const root = try captured.findInstalled(item);
        if (item.snapshot == null or item.snapshot_released) continue;
        try releaseStreamingSnapshot(&client, &driver_policy, a, active, root, internal);
        break; // Maintenance resumes any remaining releases against fresh metadata.
    }
    for (active.roots) |item| {
        if (item.released) continue;
        const root = try captured.findInstalled(item);
        _ = try source_api.flushAcknowledged(&client, &driver_policy, a, active, root, internal);
    }
    return .{ .acknowledged = confirmed.value.acknowledged_cursor };
}

pub fn stageVisibleSplits(client: *config.Client, a: A, source: source_api.Source, routes: *const source_api.Captured, parsed: *control.Parsed, cursor: control.Digest, request: Context) !void {
    if (routes.validateInstalledRoutes(parsed.value)) |_| return else |err| {
        if (err != error.ChangefeedResynchronizationRequired or parsed.value.phase != .streaming or source.split_cut == null)
            return err;
    }
    _ = try parsed.value.contextAt(cursor);
    const now = clock.realtimeNs() / std.time.ns_per_s;
    if (parsed.value.expiryDue(now)) return error.ChangefeedLeaseExpired;
    try request.ensureActive();
    var snapshot = try source.catalog.adminSnapshot();
    defer source.catalog.freeAdminSnapshot(&snapshot);
    for (0..control.max_roots) |_| {
        const state = parsed.value;
        if (routes.validateInstalledRoutes(state)) |_| return else |_| {}
        const mutation = candidate: {
            for (state.roots) |root| {
                const physical = root.physical orelse continue;
                if (root.split_handoff != null) continue;
                const table_id = std.mem.readInt(u64, root.namespace[0..8], .big);
                for (snapshot.split_transitions) |split| {
                    if (split.phase != .finalized or split.source_group_id != physical.group_id or split.table_contract.table_id != table_id) continue;
                    _ = routes.findPhysical(table_id, split.destination_group_id, split.destination_group_id) catch continue;
                    break :candidate try source.prepareSplitHandoff(a, routes, root, split, request);
                }
            }
            return error.ChangefeedResynchronizationRequired;
        };
        try change(client, a, state, mutation, request);
        var observed = try load(client, a, state.id, request);
        errdefer observed.deinit();
        const expected = mutation.stage_split_handoff;
        const index = try observed.value.rootIndex(expected.namespace);
        if (observed.value.epoch != state.epoch or observed.value.revision <= state.revision or
            !std.meta.eql(observed.value.roots[index].split_handoff, @as(?control.SplitHandoff, expected.handoff)))
            return error.MetadataMutationOutcomeUnknown;
        parsed.deinit();
        parsed.* = observed;
    }
    try routes.validateInstalledRoutes(parsed.value);
}

pub fn stageVisibleMerges(client: *config.Client, a: A, source: source_api.Source, routes: *const source_api.Captured, parsed: *control.Parsed, cursor: control.Digest, request: Context) !void {
    const state = parsed.value;
    if (state.phase != .streaming or source.merge_cut == null or state.roots.len < 2) return;
    try request.ensureActive();
    var snapshot = try source.catalog.adminSnapshot();
    defer source.catalog.freeAdminSnapshot(&snapshot);
    for (snapshot.merge_transitions) |merge| {
        if (merge.phase != .cutover_pending and merge.phase != .finalizing) continue;
        const donor = for (state.roots) |root| {
            const physical = root.physical orelse continue;
            if (physical.group_id == merge.donor_group_id and root.merge_drain == null and root.split_handoff == null and root.pending_control == null)
                break root;
        } else continue;
        _ = try state.contextAt(cursor);
        if (state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) return error.ChangefeedLeaseExpired;
        const mutation = (try source.prepareMergeDrain(a, routes, state, donor, merge, request)) orelse continue;
        try change(client, a, state, mutation, request);
        var observed = try load(client, a, state.id, request);
        errdefer observed.deinit();
        const expected = mutation.stage_merge_drain;
        const index = try observed.value.rootIndex(expected.namespace);
        if (observed.value.epoch != state.epoch or observed.value.revision <= state.revision or
            !std.meta.eql(observed.value.roots[index].merge_drain, @as(?control.MergeDrain, expected.drain)))
            return error.MetadataMutationOutcomeUnknown;
        parsed.deinit();
        parsed.* = observed;
        return;
    }
}

pub fn progressMergeDrain(client: *config.Client, a: A, source: source_api.Source, routes: *const source_api.Captured, state: control.State, cursor: control.Digest, request: Context) !void {
    if (state.phase != .streaming or !state.atDeliveryBoundary()) return;
    for (state.roots) |root| {
        const drain = root.merge_drain orelse continue;
        if (root.released) {
            if (routes.session.group_indexes.contains(drain.donor.group_id)) continue;
            _ = try routes.findPhysical(std.mem.readInt(u64, root.namespace[0..8], .big), drain.receiver.group_id, drain.receiver.range_id);
            _ = try state.contextAt(cursor);
            return change(client, a, state, .{ .prune_merge_donor = .{ .namespace = root.namespace, .transition = drain.transition } }, request);
        }
        if (!root.mergeDrained()) continue;
        _ = try state.contextAt(cursor);
        if (state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) return error.ChangefeedLeaseExpired;
        const donor = try routes.findInstalled(root);
        const command: @import("history_control_operations.zig").wire.Request = .{
            .version = 1,
            .operation = .submit,
            .command = .{ .retire = .{
                .namespace = root.storageNamespace(),
                .authority = state.authority,
                .consumer = state.consumerId(),
                .definition = state.definition.?.identity(),
                .grant = root.grant_sequence,
                .sequence = state.authority_sequence,
            } },
        };
        const result = try source.controls.execute(a, donor.fence, donor.table_name, command, request);
        if (!result.confirmed()) return error.MetadataMutationOutcomeUnknown;
        if (result.outcome.? == .rejected) return result.outcome.?.rejected.asError();
        const receipt = result.receipt.?;
        return change(client, a, state, .{ .release_merge_donor = .{
            .namespace = root.namespace,
            .transition = drain.transition,
            .receipt_term = receipt.term,
            .receipt_index = receipt.index,
        } }, request);
    }
}

pub fn progressSplitSuccessor(client: *config.Client, a: A, source: source_api.Source, routes: *const source_api.Captured, state: control.State, cursor: control.Digest, request: Context) !void {
    for (state.roots) |root| {
        const handoff = root.split_handoff orelse continue;
        if (state.phase != .streaming) continue;
        _ = try state.contextAt(cursor);
        if (state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) return error.ChangefeedLeaseExpired;
        if (root.split_consumer_epoch == null) {
            const mutation = try source.enrollSplitSuccessor(a, routes, state, root, request);
            return change(client, a, state, mutation, request);
        }
        if (!state.atDeliveryBoundary() or root.pending_control != null or handoff.cut.reserved_bytes != 0 or
            root.acknowledged.order(.{ .sequence = handoff.cut.latest }) != .eq) continue;
        return change(client, a, state, .{ .activate_split_successor = .{
            .namespace = root.namespace,
            .grant_sequence = root.split_grant_sequence.?,
        } }, request);
    }
}

const RoutedFactory = struct {
    source: source_api.Source,
    captured: *const source_api.Captured,

    pub fn open(self: RoutedFactory, namespace: control.Namespace) !roots.Routed {
        return roots.Routed.initForRoot(self.source, self.captured, namespace);
    }
};

pub const Create = struct {
    pub const Selector = struct {
        table: []const u8,
        fields: []const []const u8 = &.{},
        predicates: []const control.Predicate = &.{},
    };

    tables: []const []const u8,
    selectors: []const Selector = &.{},
    start: control.Start = .now,
    events: []const enum { document_committed, artifact_published, publication_activated } = &.{.document_committed},
    include: control.Projection = .keys_and_versions,
    lease_seconds: u32 = 3600,
};

pub const Created = struct {
    id: control.Id,
    cursor: control.Digest,
    expires_at: u64,
};

const enrollment = @import("../metadata/root_control_driver.zig");
const source_api = @import("history_roots.zig");
const metadata_api = @import("../metadata/api.zig");

fn RootPolicy(comptime Access: type) type {
    return struct {
        access: Access,
        io: std.Io,
        source: source_api.Source,
        allocator: A,
        closing: bool = false,
        renewing: bool = false,
        retirement_collected: bool = false,

        pub fn authorizeRoot(self: *@This(), state: control.State, _: control.Namespace, request: Context) !void {
            if (!self.closing) {
                if (Access != void) return self.access.authorize(state, request);
                try requireMaintenancePrincipal(request);
                if (!self.renewing or state.slot != null or state.phase != .streaming or state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) return error.Forbidden;
                return;
            }
            try request.ensureActive();
            if (state.phase != .closing and state.phase != .reset_required)
                return error.ChangefeedConflict;
            if (Access != void) {
                if (state.slot != null) return error.Forbidden;
                const access = self.access;
                var identity = try authenticate(access.api, access.credential);
                defer identity.deinit(access.api.alloc);
                try requireOwner(identity, state.definition orelse return error.Forbidden);
            } else {
                try requireMaintenancePrincipal(request);
            }
        }

        pub fn validateRoot(self: *@This(), state: control.State, namespace: control.Namespace, fence: metadata_api.CatalogRouteFence, table: []const u8, request: Context) !void {
            try self.source.validate(self.allocator, state, namespace, fence, table, request);
        }

        pub fn newAttempt(self: *@This(), _: control.State, _: control.Namespace, _: Context) !control.Id {
            var id: control.Id = undefined;
            self.io.random(&id);
            return id;
        }

        pub fn deferRetirementSettlement(self: *const @This()) bool {
            return self.closing and !self.retirement_collected;
        }

        pub fn context(_: *@This(), request: Context) !Context {
            return request;
        }
    };
}

fn load(client: *config.Client, a: A, id: control.Id, request: Context) !control.Parsed {
    const bytes = try client.readChangefeedState(a, id, request);
    defer a.free(bytes);
    return control.parse(a, bytes);
}

fn change(client: *config.Client, a: A, state: control.State, mutation: control.Mutation, request: Context) !void {
    const definition = state.definition.?;
    try client.mutateChangefeedState(a, .{
        .id = state.id,
        .authority = state.authority,
        .owner = definition.owner,
        .authorization = definition.authorization,
        .expected_revision = state.revision,
        .expected_epoch = state.epoch,
        .mutation = mutation,
    }, request);
}

fn metadataEvent(definition: control.Definition) bool {
    return definition.events == publications.activation_event or definition.events == artifacts.artifact_event;
}

fn artifactGenerations(a: A, encoded: []const u8, count: usize) ![]u8 {
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const size = decoder.calcSizeForSlice(encoded) catch return error.ChangefeedResynchronizationRequired;
    if (size != count * @sizeOf(u64)) return error.ChangefeedResynchronizationRequired;
    const bytes = try a.alloc(u8, size);
    errdefer a.free(bytes);
    decoder.decode(bytes, encoded) catch return error.ChangefeedResynchronizationRequired;
    return bytes;
}

const ActivationScheduler = struct {
    policy: *Policy,
    routes: ?*const source_api.Captured = null,
    artifacts: ?artifacts.Port = null,

    pub fn authorize(self: ActivationScheduler, state: control.State, request: Context) !void {
        try self.policy.authorize(state, request);
    }

    pub fn nowSeconds(self: ActivationScheduler, request: Context) !u64 {
        return self.policy.nowSeconds(request);
    }

    pub fn prepare(self: ActivationScheduler, a: A, state: control.State, before: control.Digest, limits: delivery.Limits, request: Context) !delivery.Prepared {
        const positions = try state.rootsAt(a, before);
        if (positions.len == 0) return error.ChangefeedResynchronizationRequired;
        const start = state.latest_delivery % positions.len;
        var heartbeat: ?delivery.Prepared = null;
        for (0..positions.len) |offset| {
            const root = positions[(start + offset) % positions.len];
            const rendered = try self.page(a, state, root, before, null, limits, request);
            if (rendered.events != 0) return rendered;
            heartbeat = rendered;
        }
        return heartbeat orelse error.ChangefeedResynchronizationRequired;
    }

    pub fn replay(self: ActivationScheduler, a: A, state: control.State, receipt: control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
        const positions = try state.rootsAt(a, receipt.before);
        if (positions.len == 0 or receipt.advances.len > 1) return error.ChangefeedResynchronizationRequired;
        const root = if (receipt.advances.len == 0) positions[0] else positions[try state.rootIndex(receipt.advances[0].namespace)];
        return self.page(a, state, root, receipt.before, receipt, limits, request);
    }

    fn page(self: ActivationScheduler, a: A, state: control.State, root: control.Root, before: control.Digest, original: ?control.Delivery, limits: delivery.Limits, request: Context) !delivery.Prepared {
        try request.ensureActive();
        if (self.routes) |routes| {
            const port = self.artifacts orelse return error.ChangefeedCapabilityUnsupported;
            const route = try routes.find(root.namespace);
            const table_id = route.fence.table_id;
            const table = for (state.definition.?.tables) |selected| {
                if (selected.table_id == table_id) break selected;
            } else return error.ChangefeedResynchronizationRequired;
            const bytes = try artifactGenerations(a, try state.contextAt(before), state.roots.len);
            defer a.free(bytes);
            const offset = (try state.rootIndex(root.namespace)) * @sizeOf(u64);
            const input: artifacts.wire.HistoryRead = .{
                .namespace = root.namespace,
                .root_generation = std.mem.readInt(u64, bytes[offset..][0..8], .big),
                .after = root.delivered.sequence,
            };
            const observation = try port.read(a, route.fence, route.table_name, input, request);
            return artifacts.render(a, state, table, input, observation, before, original, limits);
        }
        const table = for (state.definition.?.tables) |selected| {
            if (std.mem.eql(u8, &root.namespace, &control.catalogRootNamespace(selected))) break selected;
        } else return error.ChangefeedResynchronizationRequired;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const store = self.policy.api.source.lakeIndexLifecycleAuthority(request) orelse return error.ChangefeedCapabilityUnsupported;
        const lifecycle = try store.readState(scratch, table.table_id);
        if (lifecycle.dropped) return error.ChangefeedResynchronizationRequired;
        return publications.render(a, state, table, root.delivered, lifecycle, before, original, limits);
    }
};

fn createMetadataEventFeed(a: A, policy: *Policy, meta: config.Port, key: []const u8, definition: control.Definition, lease_seconds: u64, request: Context) !Created {
    try policy.authorize(.{ .definition = definition }, request);
    const api = policy.api;
    var view = (try api.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
    defer api.source.freeAdminSnapshot(&view);
    const authority = try @import("history_policy_provisioning.zig").authorityForCatalog(view.status.metadata_group_id, view.status.metadata_incarnation);
    const id: control.Id = control.hash(.{ .domain = "antfly:public-changefeed-create:v1", .owner = definition.owner, .key = key })[0..16].*;
    var client: config.Client = .{ .port = meta };
    const artifact = definition.events == artifacts.artifact_event;
    const source: ?source_api.Source = if (artifact) api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported else null;
    var captured: ?source_api.Captured = if (source) |native_source| try native_source.capture(a, definition, request) else null;
    defer if (captured) |*routes| routes.deinit();
    if (captured) |*routes| {
        if (routes.roots.len == 0) return error.ChangefeedResynchronizationRequired;
        for (routes.roots) |route| {
            if (!same(authority, try @import("history_policy_provisioning.zig").authority(route.fence))) return error.TopologyChanged;
        }
    }
    const store = if (artifact) null else api.source.lakeIndexLifecycleAuthority(request);
    if (!artifact and store == null) return error.ChangefeedCapabilityUnsupported;
    for (0..6) |_| {
        var parsed = try load(&client, a, id, request);
        defer parsed.deinit();
        const state = parsed.value;
        const now = try policy.nowSeconds(request);
        if (state.phase == .empty) {
            if (try meta.authority(a, request)) |registered| {
                if (!same(registered.incarnation, authority)) return error.ChangefeedResynchronizationRequired;
            }
            try client.mutateChangefeedState(a, .{
                .id = id,
                .authority = authority,
                .owner = definition.owner,
                .authorization = definition.authorization,
                .expected_revision = 0,
                .expected_epoch = 0,
                .mutation = .{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now + lease_seconds } },
            }, request);
            continue;
        }
        if (!same(definition.identity(), state.definition.?.identity()) or !same(authority, state.authority)) return error.ChangefeedConflict;
        try policy.authorize(state, request);
        if (state.expiryDue(now)) return error.ChangefeedLeaseExpired;
        if (state.phase == .streaming) return .{ .id = id, .cursor = state.acknowledged_cursor, .expires_at = state.expires_at };
        if (state.phase != .enrolling) return error.ChangefeedResynchronizationRequired;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        if (state.roots.len == 0) {
            const plans = if (captured) |*routes| try routes.initialPlans(scratch) else blk: {
                const plans = try scratch.alloc(control.RootPlan, definition.tables.len);
                for (definition.tables, plans) |table, *plan| plan.* = .{ .namespace = control.catalogRootNamespace(table), .ownership_epoch = 1 };
                break :blk plans;
            };
            try change(&client, a, state, .{ .plan_roots = plans }, request);
            continue;
        }
        if (captured) |*routes| {
            try routes.validateInitialRoutes(state.roots);
            if (state.latest_provider_context.len == 0) {
                const owners = source.?.artifact_owners orelse return error.ChangefeedCapabilityUnsupported;
                const bytes = try scratch.alloc(u8, routes.roots.len * @sizeOf(u64));
                for (routes.roots, 0..) |route, index| {
                    const owner = try owners.read(scratch, route.fence, route.table_name, .{
                        .version = 1,
                        .expected_table_binding = route.table_binding,
                    }, request);
                    std.mem.writeInt(u64, bytes[index * @sizeOf(u64) ..][0..8], owner.owner.root_generation, .big);
                }
                const encoder = std.base64.url_safe_no_pad.Encoder;
                const provider = encoder.encode(try scratch.alloc(u8, encoder.calcSize(bytes.len)), bytes);
                try change(&client, a, state, .{ .stage_catalog_start = provider }, request);
                continue;
            }
            const bytes = try artifactGenerations(scratch, state.latest_provider_context, state.roots.len);
            const port = source.?.artifact_history orelse return error.ChangefeedCapabilityUnsupported;
            const starts = try scratch.alloc(control.CatalogStart, routes.roots.len);
            for (routes.roots, starts, 0..) |route, *start, index| {
                const observation = try port.read(scratch, route.fence, route.table_name, .{
                    .namespace = route.namespace,
                    .root_generation = std.mem.readInt(u64, bytes[index * @sizeOf(u64) ..][0..8], .big),
                }, request);
                start.* = .{ .namespace = route.namespace, .through = observation.latest };
            }
            const cut = control.hash(.{ .domain = "antfly:artifact-publication-cut:v1", .definition = definition.identity(), .generations = state.latest_provider_context, .starts = starts });
            try change(&client, a, state, .{ .seal = .{ .cut = cut, .provider_context = state.latest_provider_context, .catalog_starts = starts } }, request);
            continue;
        }
        if (state.latest_provider_context.len == 0) {
            try change(&client, a, state, .{ .stage_catalog_start = "YQ" }, request);
            continue;
        }
        const starts = try scratch.alloc(control.CatalogStart, definition.tables.len);
        for (definition.tables, starts) |table, *position| {
            var per_table = std.heap.ArenaAllocator.init(a);
            defer per_table.deinit();
            const observed = try store.?.readState(per_table.allocator(), table.table_id);
            if (observed.dropped) return error.ChangefeedResynchronizationRequired;
            const history = observed.activation_history orelse &.{};
            position.* = .{ .namespace = control.catalogRootNamespace(table), .through = if (history.len == 0) 0 else history[history.len - 1].sequence };
        }
        const cut = control.hash(.{ .domain = "antfly:publication-activation-cut:v1", .definition = definition.identity(), .starts = starts });
        try change(&client, a, state, .{ .seal = .{ .cut = cut, .provider_context = state.latest_provider_context, .catalog_starts = starts } }, request);
    }
    return error.MetadataMutationOutcomeUnknown;
}

fn createIceberg(a: A, policy: *Policy, meta: config.Port, key: []const u8, definition: control.Definition, lease_seconds: u64, factory: ?RoutedFactory, request: Context) !Created {
    try policy.authorize(.{ .definition = definition }, request);
    const api = policy.api;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var view = (try api.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
    defer api.source.freeAdminSnapshot(&view);
    const authority = try @import("history_policy_provisioning.zig").authorityForCatalog(view.status.metadata_group_id, view.status.metadata_incarnation);
    const id: control.Id = control.hash(.{ .domain = "antfly:public-changefeed-create:v1", .owner = definition.owner, .key = key })[0..16].*;
    var plans: std.ArrayList(control.RootPlan) = .empty;
    var iceberg_count: usize = 0;
    for (definition.tables) |selected| {
        if (selected.contract != .writable_iceberg_rows) continue;
        try plans.append(scratch, .{ .namespace = control.catalogRootNamespace(selected), .ownership_epoch = 1 });
        iceberg_count += 1;
    }
    if (factory) |provider| {
        for (provider.captured.roots) |route| {
            if (!same(authority, try @import("history_policy_provisioning.zig").authority(route.fence))) return error.TopologyChanged;
        }
        try plans.appendSlice(scratch, try provider.captured.initialPlans(scratch));
    }
    std.mem.sort(control.RootPlan, plans.items, {}, struct {
        fn less(_: void, left: control.RootPlan, right: control.RootPlan) bool {
            return std.mem.order(u8, &left.namespace, &right.namespace) == .lt;
        }
    }.less);
    var client: config.Client = .{ .port = meta };
    const changes = @import("lake_committed_changes.zig");
    const ingestion = @import("../serverless/lake_ingestion.zig");
    const configured = @import("../serverless/configured_object_store_support.zig");
    for (0..@min(64, 8 + plans.items.len * 4)) |_| {
        var parsed = try load(&client, a, id, request);
        defer parsed.deinit();
        const state = parsed.value;
        const now = try policy.nowSeconds(request);
        if (state.phase == .empty) {
            if (try meta.authority(a, request)) |registered| {
                if (!same(registered.incarnation, authority)) return error.ChangefeedResynchronizationRequired;
            }
            try client.mutateChangefeedState(a, .{
                .id = id,
                .authority = authority,
                .owner = definition.owner,
                .authorization = definition.authorization,
                .expected_revision = 0,
                .expected_epoch = 0,
                .mutation = .{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now + lease_seconds, .bootstrap_expires_at = if (definition.start == .snapshot_then_changes) now + @min(lease_seconds, control.max_bootstrap_seconds) else 0 } },
            }, request);
            continue;
        }
        if (!same(definition.identity(), state.definition.?.identity()) or !same(authority, state.authority))
            return error.ChangefeedConflict;
        try policy.authorize(state, request);
        if (state.expiryDue(now)) return error.ChangefeedLeaseExpired;
        if (state.phase == .streaming or state.phase == .bootstrap)
            return .{ .id = id, .cursor = state.acknowledged_cursor, .expires_at = state.expires_at };
        if (state.phase == .reset_required and state.reset_reason == .history_expired and std.mem.allEqual(u8, &state.cut, 0)) {
            const provider = factory orelse return error.ChangefeedResynchronizationRequired;
            if (!try closeIceberg(&client, CatalogFeedOwner.local(api), a, state, request)) continue;
            var native_policy: RootPolicy(Policy) = .{ .access = policy.*, .io = api.inferenceIo(), .source = provider.source, .allocator = a, .closing = true };
            if (try retireOne(&client, &native_policy, a, state, request)) continue;
            try change(&client, a, state, .{ .restart = .{
                .definition = definition,
                .now = now,
                .expires_at = state.expires_at,
                .bootstrap_expires_at = state.bootstrap_expires_at,
            } }, request);
            continue;
        }
        if (state.phase != .enrolling) return error.ChangefeedResynchronizationRequired;
        if (state.roots.len == 0) {
            try change(&client, a, state, .{ .plan_roots = plans.items }, request);
            continue;
        }
        if (factory) |provider| {
            try provider.captured.validateInstalledRoutes(state);
            const pending = for (provider.captured.roots) |route| {
                if (state.roots[try state.rootIndex(route.namespace)].consumer_epoch == 0) break route;
            } else null;
            if (pending) |route| {
                const selected = for (definition.tables) |table| {
                    if (table.table_id == route.fence.table_id) break table;
                } else return error.ChangefeedResynchronizationRequired;
                var native_policy: RootPolicy(Policy) = .{ .access = policy.*, .io = api.inferenceIo(), .source = provider.source, .allocator = a };
                try source_api.enrollRoot(&client, &native_policy, a, state, route, selected.policy_revision, request);
                continue;
            }
        }
        const scope = changes.feedScope(state);
        const capturing = state.latest_provider_context.len == 0;
        var attempt = std.heap.ArenaAllocator.init(a);
        defer attempt.deinit();
        const turn = attempt.allocator();
        const positions = try turn.alloc(changes.TablePosition, iceberg_count);
        const starts = try turn.alloc(control.CatalogStart, iceberg_count);
        const saved_context = if (!capturing and factory != null)
            (try composed_context.Context.decode(turn, state.latest_provider_context)).iceberg
        else
            state.latest_provider_context;
        var index: usize = 0;
        for (definition.tables, policy.tables) |selected, access| {
            if (selected.contract != .writable_iceberg_rows) continue;
            const saved = &positions[index];
            const start = &starts[index];
            index += 1;
            const namespace = control.catalogRootNamespace(selected);
            try request.ensureActive();
            var scan_arena = std.heap.ArenaAllocator.init(a);
            defer scan_arena.deinit();
            const scan = scan_arena.allocator();
            const table = try resolveWritableIceberg(api, scan, selected, access.storage_name);
            const binding = table.schema.external_base_source.?.binding;
            if (definition.start == .snapshot_then_changes and binding.row_identity == null) return error.ChangefeedCapabilityUnsupported;
            const options = try api.lakeStreamOwnerOptions(.{ .allocator = scan, .table = table.record, .request = request });
            const context = try icebergRequestContext(options, request);
            var loaded = try configured.executeLakeCatalogAlloc(scan, binding, options, context, .load);
            defer loaded.deinit(scan);
            const position = if (capturing)
                try ingestion.captureCommittedWalPosition(turn, loaded.table)
            else
                try changes.decodePosition(turn, scope, selected.table_id, saved_context);
            saved.* = .{ .table_id = selected.table_id, .position = position };
            if (capturing) continue;
            _ = try ingestion.nextCommittedWalInterval(scan, loaded.table, position);
            try ingestion.pinCommittedWalPosition(scan, binding, options, context, state.consumerId(), position, state.expires_at);
            const digest = control.hash(.{ .domain = "antfly:iceberg-changefeed-root-start:v1", .scope = scope, .namespace = namespace, .position = position });
            start.* = .{
                .namespace = namespace,
                .through = position.through,
                .snapshot = if (definition.start == .snapshot_then_changes)
                    .{ .id = digest[0..16].*, .digest = digest, .expires_at = state.bootstrap_expires_at }
                else
                    null,
            };
        }
        try policy.authorize(state, request);
        if (capturing) {
            const iceberg_context = if (positions.len == 1)
                try changes.encodePosition(turn, scope, positions[0].table_id, positions[0].position)
            else
                try changes.encodeVector(turn, scope, positions);
            const provider_context = if (factory != null) try (composed_context.Context{ .iceberg = iceberg_context }).encode(turn) else iceberg_context;
            try change(&client, a, state, .{ .stage_catalog_start = provider_context }, request);
            continue;
        }
        const native_cut = if (factory) |provider| blk: {
            if (provider.captured.roots.len < 2) break :blk null;
            var native_policy: RootPolicy(Policy) = .{ .access = policy.*, .io = api.inferenceIo(), .source = provider.source, .allocator = a };
            break :blk source_api.qualifyEnrollmentCut(a, provider.source, &native_policy, provider.captured, state, request) catch |err| switch (err) {
                error.ChangefeedEnrollmentCutAdvanced => {
                    try change(&client, a, state, .{ .reset = .history_expired }, request);
                    continue;
                },
                else => return err,
            };
        } else null;
        const cut = control.hash(.{ .domain = "antfly:iceberg-changefeed-start:v2", .scope = scope, .positions = positions, .roots = state.roots, .native_cut = native_cut });
        try change(&client, a, state, .{ .seal = .{ .cut = cut, .provider_context = state.latest_provider_context, .catalog_starts = starts } }, request);
    }
    return error.MetadataMutationOutcomeUnknown;
}

/// The client-generated request key is scoped to the durable credential.
/// Repeating creation resumes the original enrollment and returns its cursor.
pub fn create(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, key: []const u8, input: Create, public: Context) !Created {
    try public.ensureActive();
    if (key.len == 0 or key.len > 128 or input.lease_seconds == 0 or input.lease_seconds > control.max_lease_seconds)
        return error.InvalidChangefeed;
    if (input.events.len != 1) return error.ChangefeedCapabilityUnsupported;
    const activation = input.events[0] == .publication_activated;
    const artifact = input.events[0] == .artifact_published;
    if ((activation or artifact) and (input.start != .now or input.selectors.len != 0)) return error.ChangefeedCapabilityUnsupported;
    if (input.tables.len == 0 or input.tables.len > control.max_tables or input.selectors.len > input.tables.len)
        return error.InvalidChangefeed;
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const caller = try durableOwner(identity);
    const meta = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };

    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const SelectionBinding = struct {
        table: control.Table,
        access: TableAccess,
        fn less(_: void, left: @This(), right: @This()) bool {
            return left.table.table_id < right.table.table_id;
        }
    };
    const selected = try scratch.alloc(SelectionBinding, input.tables.len);
    var selectors_used: usize = 0;
    for (input.tables, selected) |requested, *entry| {
        if (requested.len == 0 or requested.len > 1024) return error.InvalidChangefeed;
        if (!try http.tablePermissionCurrentlyAllowed(identity, requested, .replicate)) return error.Forbidden;
        const table_name = try resolveStorageName(api, scratch, requested, internal);
        const record = (try api.source.tableRecord(scratch, table_name)) orelse return error.Forbidden;
        const permission_name = try resolvePermissionName(api, scratch, table_name, internal);
        const requested_target = try catalog.Target.parse(requested);
        const permission_target = try catalog.Target.parse(permission_name);
        if (!std.mem.eql(u8, requested_target.database, permission_target.database) or
            !std.mem.eql(u8, requested_target.namespace, permission_target.namespace) or
            !std.mem.eql(u8, requested_target.table, permission_target.table))
            return error.ChangefeedResynchronizationRequired;
        const parsed_schema = try local.schema_mod.parseValidatedTableSchema(scratch, record.schema_json);
        const schema = try local.schema_mod.deriveRuntimeTableSchema(scratch, parsed_schema);
        const contract: control.Contract = if (activation) blk: {
            if (schema.external_base_source == null) return error.ChangefeedCapabilityUnsupported;
            break :blk .external_snapshot_lifecycle;
        } else if (schema.external_base_source) |external| blk: {
            if (artifact) return error.ChangefeedCapabilityUnsupported;
            if (external.binding.format != .iceberg or external.binding.write_policy != .iceberg_writer or external.binding.catalog == null)
                return error.ChangefeedCapabilityUnsupported;
            if (input.start == .snapshot_then_changes and external.binding.row_identity == null)
                return error.ChangefeedCapabilityUnsupported;
            break :blk .writable_iceberg_rows;
        } else blk: {
            if (record.storage.engine != .local) return error.ChangefeedCapabilityUnsupported;
            break :blk if (schema.storage_mode == .relational) .native_relational else .native_document;
        };
        var table: control.Table = .{
            .table_id = record.table_id,
            .binding = control.tableBinding(record),
            .policy_revision = 1,
            .contract = contract,
        };
        var selector_found = false;
        for (input.selectors) |selector| {
            if (!std.mem.eql(u8, selector.table, requested)) continue;
            if (selector_found or selector.fields.len > 64 or selector.predicates.len > 16)
                return error.InvalidChangefeed;
            selector_found = true;
            selectors_used += 1;
            const fields = try scratch.dupe([]const u8, selector.fields);
            const predicates = try scratch.dupe(control.Predicate, selector.predicates);
            std.mem.sort([]const u8, fields, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.lessThan(u8, left, right);
                }
            }.less);
            std.mem.sort(control.Predicate, predicates, {}, struct {
                fn less(_: void, left: control.Predicate, right: control.Predicate) bool {
                    return std.mem.lessThan(u8, left.field, right.field);
                }
            }.less);
            table.fields = fields;
            table.predicates = predicates;
        }
        entry.* = .{
            .table = table,
            .access = .{
                .id = record.table_id,
                .storage_name = table_name,
                .permission_name = permission_name,
                .filter = try http.resolveEffectiveRowFilterJson(scratch, identity, permission_name),
            },
        };
    }
    if (selectors_used != input.selectors.len) return error.InvalidChangefeed;
    std.mem.sort(SelectionBinding, selected, {}, SelectionBinding.less);
    const tables = try scratch.alloc(control.Table, selected.len);
    const access = try scratch.alloc(TableAccess, selected.len);
    for (selected, tables, access, 0..) |entry, *table, *grant, index| {
        if (index != 0 and selected[index - 1].table.table_id == entry.table.table_id) return error.InvalidChangefeed;
        table.* = entry.table;
        grant.* = entry.access;
    }
    const definition: control.Definition = .{
        .owner = caller,
        .authorization = authorization(identity, tables, access),
        .tables = tables,
        .projection = input.include,
        .events = if (activation) publications.activation_event else if (artifact) artifacts.artifact_event else 1,
        .start = input.start,
    };
    if (activation or artifact) {
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        return createMetadataEventFeed(a, &policy, meta, key, definition, input.lease_seconds, internal);
    }
    if (allIceberg(definition)) {
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        return createIceberg(a, &policy, meta, key, definition, input.lease_seconds, null, internal);
    }
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    if (input.start == .snapshot_then_changes and source.bootstrap_pages == null) return error.ChangefeedCapabilityUnsupported;
    if (hasIceberg(definition)) {
        var native_routes = try source.capture(a, try nativeDefinition(scratch, definition), internal);
        defer native_routes.deinit();
        var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
        return createIceberg(a, &policy, meta, key, definition, input.lease_seconds, .{ .source = source, .captured = &native_routes }, internal);
    }
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    if (captured.roots.len == 0) return error.ChangefeedResynchronizationRequired;
    const root = captured.roots[0];
    const history_authority = @import("history_policy_provisioning.zig").authority;
    const authority = try history_authority(root.fence);
    for (captured.roots) |route| {
        if (!same(authority, try history_authority(route.fence))) return error.TopologyChanged;
    }
    const id: control.Id = control.hash(.{ .domain = "antfly:public-changefeed-create:v1", .owner = caller, .key = key })[0..16].*;
    var client: config.Client = .{ .port = meta };
    var policy: RootPolicy(Policy) = .{
        .access = .{ .api = api, .credential = credential, .tables = access },
        .io = api.inferenceIo(),
        .source = source,
        .allocator = a,
    };
    const now = try policy.access.nowSeconds(internal);
    for (0..@min(64, 8 + captured.roots.len * 4)) |_| {
        var parsed = try load(&client, a, id, internal);
        defer parsed.deinit();
        const state = parsed.value;
        if (state.phase == .empty) {
            try policy.access.authorize(.{ .definition = definition }, internal);
            if (try meta.authority(a, internal)) |registered| {
                if (!same(registered.incarnation, authority)) return error.ChangefeedResynchronizationRequired;
            }
            try client.mutateChangefeedState(a, .{
                .id = id,
                .authority = authority,
                .owner = caller,
                .authorization = definition.authorization,
                .expected_revision = 0,
                .expected_epoch = 0,
                .mutation = .{ .create = .{ .id = id, .definition = definition, .now = now, .expires_at = now + input.lease_seconds, .bootstrap_expires_at = if (input.start == .snapshot_then_changes) now + @min(input.lease_seconds, control.max_bootstrap_seconds) else 0 } },
            }, internal);
            continue;
        }
        if (!same(definition.identity(), state.definition.?.identity()) or !same(authority, state.authority))
            return error.ChangefeedConflict;
        try policy.access.authorize(state, internal);
        if (state.expiryDue(now)) return error.ChangefeedLeaseExpired;
        if (state.phase == .streaming or state.phase == .bootstrap) {
            var active = try roots.Routed.initForRoot(source, &captured, root.namespace);
            _ = try active.validateScope(state);
            return .{ .id = id, .cursor = state.acknowledged_cursor, .expires_at = state.expires_at };
        }
        if (state.phase == .reset_required and state.reset_reason == .history_expired and std.mem.allEqual(u8, &state.cut, 0)) {
            policy.closing = true;
            const retired = try retireOne(&client, &policy, a, state, internal);
            policy.closing = false;
            if (retired) continue;
            try change(&client, a, state, .{ .restart = .{
                .definition = definition,
                .now = now,
                .expires_at = state.expires_at,
                .bootstrap_expires_at = state.bootstrap_expires_at,
            } }, internal);
            continue;
        }
        if (state.phase != .enrolling) return error.ChangefeedResynchronizationRequired;
        if (state.roots.len == 0) {
            const plans = try captured.initialPlans(scratch);
            try change(&client, a, state, .{ .plan_roots = plans }, internal);
            continue;
        }
        try captured.validateInitialRoutes(state.roots);
        const pending = for (state.roots, captured.roots) |item, route| {
            if (item.consumer_epoch == 0) break route;
        } else null;
        if (pending) |route| {
            const table = for (tables) |table| {
                if (table.table_id == route.fence.table_id) break table;
            } else return error.ChangefeedResynchronizationRequired;
            try source_api.enrollRoot(&client, &policy, a, state, route, table.policy_revision, internal);
            continue;
        }
        if (state.roots.len == 1) {
            const item = state.roots[0];
            try policy.validateRoot(state, root.namespace, root.fence, root.table_name, internal);
            const ordinary_cut = control.hash(.{
                .domain = "antfly:public-changefeed-start:v1",
                .authority = authority,
                .definition = definition.identity(),
                .namespace = item.namespace,
                .epoch = item.consumer_epoch,
                .start = item.start,
            });
            const cut = if (item.snapshot) |pin|
                control.hash(.{ .domain = "antfly:public-changefeed-bootstrap:v1", .continuation = ordinary_cut, .pin = pin })
            else
                ordinary_cut;
            try change(&client, a, state, .{ .seal = .{ .cut = cut } }, internal);
            continue;
        }
        const cut = source_api.qualifyEnrollmentCut(a, source, &policy, &captured, state, internal) catch |err| switch (err) {
            error.ChangefeedEnrollmentCutAdvanced => {
                try change(&client, a, state, .{ .reset = .history_expired }, internal);
                continue;
            },
            else => return err,
        };
        try change(&client, a, state, .{ .seal = .{ .cut = cut } }, internal);
    }
    return error.MetadataMutationOutcomeUnknown;
}

/// Close the metadata feed first, then fence each planned grant before
/// finalizing its tombstone. Repeating a DELETE resumes those same controls.
fn pendingCatalogRoot(state: control.State) ?struct { root: control.Root, table: control.Table } {
    const definition = state.definition.?;
    if (definition.events == artifacts.artifact_event) {
        for (state.roots) |root| {
            if (root.released) continue;
            const id = std.mem.readInt(u64, root.namespace[0..8], .big);
            for (definition.tables) |table| if (table.table_id == id) return .{ .root = root, .table = table };
        }
        return null;
    }
    for (definition.tables) |table| {
        if (!control.catalogBacked(table.contract)) continue;
        const index = state.rootIndex(control.catalogRootNamespace(table)) catch continue;
        const root = state.roots[index];
        if (!root.released) return .{ .root = root, .table = table };
    }
    return null;
}

pub const CatalogFeedOwner = struct {
    const owner = @import("lake_source_owner.zig");
    source: http.StatusSource,
    ptr: *anyopaque,
    options_fn: *const fn (*anyopaque, owner.OptionsInput) anyerror!owner.Options,

    fn local(api: *http.ApiHttpServer) CatalogFeedOwner {
        return .{ .source = api.source, .ptr = api, .options_fn = localOptions };
    }

    fn localOptions(ptr: *anyopaque, input: owner.OptionsInput) !owner.Options {
        const api: *http.ApiHttpServer = @ptrCast(@alignCast(ptr));
        return api.lakeStreamOwnerOptions(input);
    }
};

fn closeIceberg(client: *config.Client, catalog_owner: CatalogFeedOwner, a: A, state: control.State, request: Context) !bool {
    const pending = pendingCatalogRoot(state) orelse {
        if (state.phase == .closing and (allIceberg(state.definition.?) or metadataEvent(state.definition.?))) try change(client, a, state, .finish_close, request);
        return true;
    };
    const root = pending.root;
    const selected = pending.table;
    if (selected.contract == .writable_iceberg_rows) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var view = (try catalog_owner.source.linearizableSnapshot(request)) orelse return error.MetadataSourceUnavailable;
        defer catalog_owner.source.freeAdminSnapshot(&view);
        for (view.tables) |observed| {
            if (observed.table_id != selected.table_id or !same(control.tableBinding(observed), selected.binding)) continue;
            const table = try writableIcebergFromRecord(scratch, selected, observed);
            const options = try catalog_owner.options_fn(catalog_owner.ptr, .{ .allocator = scratch, .table = table.record, .request = request });
            const normalized = try request.platformDeadline();
            const context: local.serverless_external_source_mod.lake_catalog.types.Context = .{
                .io = options.io,
                .deadline_ns = normalized.deadline_ns,
                .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn),
            };
            try @import("../serverless/lake_ingestion.zig").retireCommittedWalPin(scratch, table.schema.external_base_source.?.binding, options, context, state.consumerId(), state.expires_at);
            break;
        }
    }
    if (root.snapshot) |snapshot| {
        if (!root.snapshot_released) {
            try change(client, a, state, .{ .release_snapshot = .{ .namespace = root.namespace, .snapshot_id = snapshot.id, .proof = snapshot.digest } }, request);
            return false;
        }
    }
    const proof = control.hash(.{ .domain = "antfly:iceberg-changefeed-retirement:v1", .owner = state.consumerId(), .namespace = root.namespace, .binding = selected.binding });
    try change(client, a, state, .{ .release_root = .{ .namespace = root.namespace, .ownership_epoch = root.ownership_epoch, .proof = proof } }, request);
    return false;
}

pub fn close(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, id: control.Id, public: Context) !void {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    for (0..control.max_roots + control.max_tables + 3) |_| {
        var parsed = try load(&client, a, id, internal);
        defer parsed.deinit();
        const state = parsed.value;
        const definition = state.definition orelse return error.Forbidden;
        try requireOwner(identity, definition);
        if (state.slot != null) return error.ChangefeedCapabilityUnsupported;
        if (state.phase == .deleted) return;
        if (state.phase != .closing) {
            try change(&client, a, state, .close, internal);
            continue;
        }

        if (allIceberg(definition) or metadataEvent(definition) or pendingCatalogRoot(state) != null) {
            if (try closeIceberg(&client, CatalogFeedOwner.local(api), a, state, internal)) return;
            continue;
        }
        const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
        var policy: RootPolicy(Policy) = .{
            .access = .{ .api = api, .credential = credential },
            .io = api.inferenceIo(),
            .source = source,
            .allocator = a,
            .closing = true,
        };
        if (!try retireOne(&client, &policy, a, state, internal)) return;
    }
    return error.MetadataMutationOutcomeUnknown;
}

/// Consume one durable cutover or cleanup step. A reset retains its
/// resynchronization reason.
pub fn maintainNativeFeed(metadata: config.Port, source: source_api.Source, io: std.Io, a: A, id: control.Id, request: Context) !void {
    return maintainNativeFeedWithSpool(metadata, source, io, a, id, null, null, request);
}

pub fn maintainNativeFeedWithSpool(metadata: config.Port, source: source_api.Source, io: std.Io, a: A, id: control.Id, spool_registry: ?@import("../metadata/pg_spool_artifacts.zig").Registry, catalog_owner: ?CatalogFeedOwner, request: Context) !void {
    var client: config.Client = .{ .port = metadata };
    var parsed = try load(&client, a, id, request);
    defer parsed.deinit();
    const state = parsed.value;
    if (state.phase == .bootstrap) {
        if (!state.bootstrapComplete() or state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) return;
        try requireMaintenancePrincipal(request);
        return change(&client, a, state, .{ .finish_verified_bootstrap = .{ .proof = try state.bootstrapCompletionProof() } }, request);
    }
    if (state.phase == .streaming) {
        if (state.renewal_pending and !state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) {
            try requireMaintenancePrincipal(request);
            var captured = try source.capturePhysical(a, state.definition.?, request);
            defer captured.deinit();
            try captured.validateInstalledRoutes(state);
            var policy: RootPolicy(void) = .{ .access = {}, .io = io, .source = source, .allocator = a, .renewing = true };
            for (state.roots) |item| {
                if (item.released) continue;
                const catalog_root = for (state.definition.?.tables) |table| {
                    if (table.contract == .writable_iceberg_rows and std.meta.eql(item.namespace, control.catalogRootNamespace(table))) break true;
                } else false;
                if (catalog_root) continue;
                const route = try captured.findInstalled(item);
                if (item.renewed_through < state.expires_at) try source_api.renewRoot(&client, &policy, a, state, route, request);
            }
            return;
        }
        if (state.pendingCatalogAcknowledgment() != null and !state.expiryDue(clock.realtimeNs() / std.time.ns_per_s)) {
            try requireMaintenancePrincipal(request);
            return settleCatalogAcknowledgment(&client, a, state, catalog_owner orelse return error.ReadUnavailable, request);
        }
        for (state.roots) |item| {
            if (item.snapshot == null or item.snapshot_released) continue;
            try requireMaintenancePrincipal(request);
            var captured = try source.capturePhysical(a, state.definition.?, request);
            defer captured.deinit();
            var policy: RootPolicy(void) = .{ .access = {}, .io = io, .source = source, .allocator = a };
            return releaseStreamingSnapshot(&client, &policy, a, state, try captured.findInstalled(item), request);
        }
        return;
    }
    if (state.phase != .closing and state.phase != .reset_required) return;
    if (state.definition) |definition| {
        if (allIceberg(definition) or metadataEvent(definition) or pendingCatalogRoot(state) != null) {
            try requireMaintenancePrincipal(request);
            _ = try closeIceberg(&client, catalog_owner orelse return error.ReadUnavailable, a, state, request);
            return;
        }
    }
    var policy: RootPolicy(void) = .{ .access = {}, .io = io, .source = source, .allocator = a, .closing = true };
    if (try retireOne(&client, &policy, a, state, request)) return;
    if (spool_registry) |storage| if (state.slot) |slot| {
        if (slot.spool.records.len != 0) {
            const cleanup = @import("../metadata/pg_spool_cleanup.zig");
            const registry: cleanup.TerminalRegistry(@TypeOf(storage)) = .{ .storage = storage };
            _ = try cleanup.cleanup(&metadata, registry, a, id, .{}, request);
        }
    };
}

fn requireMaintenancePrincipal(request: Context) !void {
    try request.ensureActive();
    const principal = request.principal orelse return error.Forbidden;
    if (principal.kind != .internal or !std.mem.eql(u8, principal.subject, "changefeed-maintenance")) return error.Forbidden;
}

fn releaseStreamingSnapshot(client: *config.Client, policy: anytype, a: A, state: control.State, selected: source_api.Root, request: Context) !void {
    const item = state.roots[try state.rootIndex(selected.namespace)];
    const pin = item.snapshot orelse return;
    if (item.snapshot_released) return;
    if (state.phase != .streaming or !item.snapshot_acknowledged.done or item.released) return error.ChangefeedConflict;
    const Access = @FieldType(@typeInfo(@TypeOf(policy)).pointer.child, "access");
    if (comptime Access != void) try policy.authorizeRoot(state, item.namespace, request);
    try policy.validateRoot(state, item.namespace, selected.fence, selected.table_name, request);
    const definition = state.definition.?;
    const table = for (definition.tables) |candidate| {
        if (candidate.table_id == selected.fence.table_id) break candidate;
    } else return error.ChangefeedResynchronizationRequired;
    const ref = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer.Reference{
        .namespace = item.namespace,
        .authority = state.authority,
        .consumer = state.consumerId(),
        .epoch = item.consumer_epoch,
        .grant = item.grant_sequence,
        .definition = definition.identity(),
    };
    const observed = try policy.source.grants.read(a, selected.fence, selected.table_name, .{
        .version = 1,
        .identity = .{ .namespace = ref.namespace, .authority = ref.authority, .consumer = ref.consumer, .definition = ref.definition, .sequence = ref.grant, .policy_revision = table.policy_revision, .include_bootstrap = true },
    }, request);
    if (observed != .active or observed.active.consumer.epoch != ref.epoch or observed.active.consumer.start != item.start)
        return error.ChangefeedResynchronizationRequired;
    if (observed.active.bootstrap) |observed_pin| {
        if (!same(observed_pin.digest, pin.digest) or observed_pin.expires_at_seconds != pin.expires_at) return error.ChangefeedResynchronizationRequired;
        const command: @import("history_control_operations.zig").wire.Request = .{
            .version = 1,
            .operation = .submit,
            .command = .{ .release_bootstrap = .{ .reference = ref, .digest = pin.digest } },
        };
        const result = try policy.source.controls.execute(a, selected.fence, selected.table_name, command, request);
        try result.validate(command);
        if (!result.confirmed()) return error.MetadataMutationOutcomeUnknown;
        if (result.outcome.? == .rejected) return result.outcome.?.rejected.asError();
    }
    try change(client, a, state, .{ .release_snapshot = .{
        .namespace = item.namespace,
        .snapshot_id = pin.id,
        .proof = control.hash(.{ .domain = "antfly:snapshot-release:v1", .feed = state.id, .epoch = state.epoch, .reference = ref, .digest = pin.digest }),
    } }, request);
}

pub fn retireOne(client: *config.Client, policy: anytype, a: A, state: control.State, request: Context) !bool {
    const definition = state.definition.?;
    for (state.roots) |root| {
        if (!root.released or root.snapshot_released) continue;
        const pin = root.snapshot orelse continue;
        if (!std.mem.eql(u8, &pin.id, &state.consumerId())) return error.ChangefeedConflict;
        try policy.authorizeRoot(state, root.namespace, request);
        try change(client, a, state, .{ .release_snapshot = .{
            .namespace = root.namespace,
            .snapshot_id = pin.id,
            .proof = control.hash(.{ .domain = "antfly:snapshot-retirement:v1", .feed = state.id, .epoch = state.epoch, .namespace = root.namespace, .pin = pin.digest }),
        } }, request);
        return true;
    }
    const item = for (state.roots) |candidate| {
        if (!candidate.released) break candidate;
    } else {
        if (state.phase == .reset_required) return false;
        // Slot garbage is collected by its artifact owner. Native history
        // retirement can run first, but the tombstone waits for both owners.
        if (state.slot) |slot| if (slot.spool.records.len != 0) return false;
        try policy.authorizeRoot(state, @splat(0), request);
        try change(client, a, state, .finish_close, request);
        return true;
    };
    try policy.authorizeRoot(state, item.namespace, request);
    const source = policy.source;
    var discovered = try source.capturePhysical(a, definition, request);
    defer discovered.deinit();
    const root = try discovered.findInstalled(item);
    if (item.pending_control) |pending| {
        if (pending.stage != .prepared) {
            const observation = @import("../metadata/root_control_observation.zig");
            const grant = try observation.request(a, state, item.namespace);
            const frozen = try std.json.parseFromSlice(metadata_api.CatalogRouteFence, a, pending.payload.fence, .{});
            defer frozen.deinit();
            try policy.validateRoot(state, item.namespace, frozen.value, pending.payload.table, request);
            if (try source.grants.read(a, frozen.value, pending.payload.table, grant, request) == .retired) {
                // Retaining the pending intent makes a lost collection reply
                // or process exit discoverable. Collection itself is safe to
                // repeat and stops at the minimum ACK of the remaining grants.
                const max_frames = 32;
                const command: @import("history_control_operations.zig").wire.Request = .{
                    .version = 1,
                    .operation = .submit,
                    .command = .{ .collect = .{ .namespace = item.storageNamespace(), .authority = state.authority, .max_frames = max_frames } },
                };
                const result = try source.controls.execute(a, frozen.value, pending.payload.table, command, request);
                try result.validate(command);
                if (!result.confirmed()) return error.MetadataMutationOutcomeUnknown;
                if (result.outcome.? == .rejected) return result.outcome.?.rejected.asError();
                if (result.outcome.?.applied.reclaimed_frames == max_frames) return true;
                policy.retirement_collected = true;
            }
            defer policy.retirement_collected = false;
            const observed = try enrollment.reconcile(client, policy, source.grants, a, state.id, item.namespace, request);
            if (observed.status == .reconciled) return true;
            if (observed.status != .unproven) return error.MetadataMutationOutcomeUnknown;
            try enrollment.prepareRetirementFence(client, policy, a, state.id, item.namespace, request);
        } else {
            const result = try enrollment.drive(client, policy, source.controls, a, state.id, item.namespace, request);
            if (result.deferred_error) |err| return err;
            if (result.status != .settled and result.status != .cancelled_prepared and result.status != .accepted)
                return error.MetadataMutationOutcomeUnknown;
        }
        return true;
    }
    const command: @import("history_control_operations.zig").wire.Request = .{
        .version = 1,
        .operation = .submit,
        .command = .{ .retire = .{
            .namespace = item.storageNamespace(),
            .authority = state.authority,
            .consumer = state.consumerId(),
            .definition = definition.identity(),
            .sequence = state.authority_sequence,
            .grant = item.grant_sequence,
        } },
    };
    try source_api.prepareRoot(client, policy, a, state.id, root, command, request);
    return true;
}
