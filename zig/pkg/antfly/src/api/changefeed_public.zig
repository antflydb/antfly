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
            const current_name = try resolvePermissionName(self.api, self.api.alloc, access.storage_name, request);
            defer self.api.alloc.free(current_name);
            if (!std.mem.eql(u8, current_name, access.permission_name)) return error.ChangefeedResynchronizationRequired;
            if (!try http.tablePermissionCurrentlyAllowed(identity, current_name, .replicate)) return error.Forbidden;
            const filter = try http.resolveEffectiveRowFilterJson(self.api.alloc, identity, current_name);
            defer if (filter) |bytes| self.api.alloc.free(bytes);
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
        try request.ensureActive();
        const access = try self.accessFor(std.mem.readInt(u64, header.namespace[0..8], .big));
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
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
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
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    try captured.validateInitialRoutes(state.roots);
    const access = try authorizedTableAccess(api, a, identity, definition, &captured, internal);
    var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
    try policy.authorize(state, internal);
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

/// Extend a streaming feed to an absolute Unix timestamp. The same request
/// can resume after a lost reply while the original root leases still exist.
pub fn renew(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, id: control.Id, expires_at: u64, public: Context) !u64 {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
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
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
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
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    try captured.validateInitialRoutes(state.value.roots);
    if (captured.roots.len == 0) return error.ChangefeedResynchronizationRequired;
    var access_arena = std.heap.ArenaAllocator.init(a);
    defer access_arena.deinit();
    const scratch = access_arena.allocator();
    const access = try authorizedTableAccess(api, scratch, identity, definition, &captured, internal);
    var policy: Policy = .{ .api = api, .credential = credential, .tables = access };
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
    for (captured.roots, active.roots) |root, item| {
        if (item.snapshot == null or item.snapshot_released) continue;
        try releaseStreamingSnapshot(&client, &driver_policy, a, active, root, internal);
        break; // Maintenance resumes any remaining releases against fresh metadata.
    }
    for (captured.roots) |root| {
        _ = try source_api.flushAcknowledged(&client, &driver_policy, a, active, root, internal);
    }
    return .{ .acknowledged = confirmed.value.acknowledged_cursor };
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
    events: []const enum { document_committed } = &.{.document_committed},
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

/// The client-generated request key is scoped to the durable credential.
/// Repeating creation resumes the original enrollment and returns its cursor.
pub fn create(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, key: []const u8, input: Create, public: Context) !Created {
    try public.ensureActive();
    if (key.len == 0 or key.len > 128 or input.lease_seconds == 0 or input.lease_seconds > control.max_lease_seconds)
        return error.InvalidChangefeed;
    if (input.events.len != 1) return error.ChangefeedCapabilityUnsupported;
    if (input.tables.len == 0 or input.tables.len > control.max_tables or input.selectors.len > input.tables.len)
        return error.InvalidChangefeed;
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const caller = try durableOwner(identity);
    const meta = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    if (input.start == .snapshot_then_changes and source.bootstrap_pages == null) return error.ChangefeedCapabilityUnsupported;
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
        if (record.storage.engine != .local) return error.ChangefeedCapabilityUnsupported;
        const parsed_schema = try local.schema_mod.parseValidatedTableSchema(scratch, record.schema_json);
        const schema = try local.schema_mod.deriveRuntimeTableSchema(scratch, parsed_schema);
        if (schema.external_base_source != null) return error.ChangefeedCapabilityUnsupported;
        var table: control.Table = .{
            .table_id = record.table_id,
            .binding = control.tableBinding(record),
            .policy_revision = 1,
            .contract = if (schema.storage_mode == .relational) .native_relational else .native_document,
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
        .start = input.start,
    };
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
pub fn close(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, id: control.Id, public: Context) !void {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    for (0..10) |_| {
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
    return maintainNativeFeedWithSpool(metadata, source, io, a, id, null, request);
}

pub fn maintainNativeFeedWithSpool(metadata: config.Port, source: source_api.Source, io: std.Io, a: A, id: control.Id, spool_registry: ?@import("../metadata/pg_spool_artifacts.zig").Registry, request: Context) !void {
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
            var captured = try source.capture(a, state.definition.?, request);
            defer captured.deinit();
            try captured.validateInitialRoutes(state.roots);
            var policy: RootPolicy(void) = .{ .access = {}, .io = io, .source = source, .allocator = a, .renewing = true };
            for (state.roots, captured.roots) |item, route| {
                if (item.renewed_through < state.expires_at) try source_api.renewRoot(&client, &policy, a, state, route, request);
            }
            return;
        }
        for (state.roots) |item| {
            if (item.snapshot == null or item.snapshot_released) continue;
            try requireMaintenancePrincipal(request);
            var captured = try source.capture(a, state.definition.?, request);
            defer captured.deinit();
            var policy: RootPolicy(void) = .{ .access = {}, .io = io, .source = source, .allocator = a };
            return releaseStreamingSnapshot(&client, &policy, a, state, try captured.find(item.namespace), request);
        }
        return;
    }
    if (state.phase != .closing and state.phase != .reset_required) return;
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
    var discovered = try source.capture(a, definition, request);
    defer discovered.deinit();
    const root = try discovered.find(item.namespace);
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
                    .command = .{ .collect = .{ .namespace = item.namespace, .authority = state.authority, .max_frames = max_frames } },
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
            .namespace = item.namespace,
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
