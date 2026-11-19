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

/// Borrowed credentials live for one HTTP request. Each authorization check
/// reloads the effective policy before a durable receipt or payload can escape.
const Policy = struct {
    api: *http.ApiHttpServer,
    credential: http.AuthenticatedRequest,
    table: []const u8,
    permission_name: []const u8,
    filter: ?[]const u8,

    pub fn authorize(self: *Policy, state: control.State, request: Context) !void {
        try request.ensureActive();
        const definition = state.definition orelse return error.Forbidden;
        var identity = try authenticate(self.api, self.credential);
        defer identity.deinit(self.api.alloc);
        try requireOwner(identity, definition);
        const current_name = try resolvePermissionName(self.api, self.api.alloc, self.table, request);
        defer self.api.alloc.free(current_name);
        if (!std.mem.eql(u8, current_name, self.permission_name)) return error.ChangefeedResynchronizationRequired;
        if (!try http.tablePermissionCurrentlyAllowed(identity, current_name, .replicate)) return error.Forbidden;
        const filter = try http.resolveEffectiveRowFilterJson(self.api.alloc, identity, self.permission_name);
        defer if (filter) |bytes| self.api.alloc.free(bytes);
        if (!optionalEqual(self.filter, filter) or !same(definition.authorization, authorization(identity, definition.tables, self.table, self.permission_name, filter)))
            return error.ChangefeedResynchronizationRequired;
        try request.ensureActive();
    }

    pub fn nowSeconds(_: *Policy, _: Context) !u64 {
        return clock.realtimeNs() / std.time.ns_per_s;
    }

    pub fn images(self: *Policy, a: A, _: control.State, _: anytype, key: []const u8, before: ?std.json.Value, after: ?std.json.Value, request: Context) !native.Images {
        try request.ensureActive();
        return .{
            .before = .{ .row = before, .readable = try self.visible(a, key, before) },
            .after = .{ .row = after, .readable = try self.visible(a, key, after) },
        };
    }

    pub fn snapshotImage(self: *Policy, a: A, _: control.State, key: []const u8, row: ?std.json.Value, request: Context) !@import("antfly_local_sources").api_local_table_writes.table_write_source.changefeed_projection.Image {
        try request.ensureActive();
        return .{ .row = row, .readable = try self.visible(a, key, row) };
    }

    fn visible(self: *Policy, a: A, key: []const u8, row: ?std.json.Value) !bool {
        const value = row orelse return false;
        const filter = self.filter orelse return true;
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
    if (identity.is_internal_service or identity.live_user_manager == null or identity.credential_principal.len == 0)
        return error.Forbidden;
    const manager = identity.live_user_manager.?;
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

fn authorization(identity: http.AuthenticatedIdentity, tables: []const control.Table, table: []const u8, permission_name: []const u8, filter: ?[]const u8) control.Digest {
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
    if (definition.tables.len != 1 or state.value.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    var root = try roots.Routed.init(source, &captured);
    const table = captured.roots[0].table_name;
    const permission_name = try resolvePermissionName(api, a, table, internal);
    defer a.free(permission_name);
    const filter = try http.resolveEffectiveRowFilterJson(a, identity, permission_name);
    defer if (filter) |value| a.free(value);
    var policy: Policy = .{ .api = api, .credential = credential, .table = table, .permission_name = permission_name, .filter = filter };
    var provider: native.ProviderFor(roots.Routed, Policy) = .{ .root = &root, .policy = &policy };
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
    try releaseStreamingSnapshot(&client, &driver_policy, a, active, captured.roots[0], internal);
    _ = try source_api.flushAcknowledged(&client, &driver_policy, a, active, captured.roots[0], internal);
    return .{ .acknowledged = confirmed.value.acknowledged_cursor };
}

pub const Create = struct {
    tables: []const []const u8,
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
        retirement_collected: bool = false,

        pub fn authorizeRoot(self: *@This(), state: control.State, _: control.Namespace, request: Context) !void {
            if (!self.closing) {
                if (Access == void) return error.Forbidden;
                return self.access.authorize(state, request);
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
    if (input.tables.len != 1 or input.events.len != 1)
        return error.ChangefeedCapabilityUnsupported;
    const requested = input.tables[0];
    if (requested.len == 0 or requested.len > 1024) return error.InvalidChangefeed;
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const caller = try durableOwner(identity);
    if (!try http.tablePermissionCurrentlyAllowed(identity, requested, .replicate)) return error.Forbidden;
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
    const filter = try http.resolveEffectiveRowFilterJson(scratch, identity, requested);
    const tables: []const control.Table = &.{.{
        .table_id = record.table_id,
        .binding = control.tableBinding(record),
        .policy_revision = 1,
        .contract = .native_document,
    }};
    const definition: control.Definition = .{
        .owner = caller,
        .authorization = authorization(identity, tables, table_name, permission_name, filter),
        .tables = tables,
        .projection = input.include,
        .start = input.start,
    };
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    if (captured.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
    const root = captured.roots[0];
    if (!std.mem.eql(u8, root.table_name, table_name)) return error.TopologyChanged;
    const authority = try @import("history_policy_provisioning.zig").authority(root.fence);
    const id: control.Id = control.hash(.{ .domain = "antfly:public-changefeed-create:v1", .owner = caller, .key = key })[0..16].*;
    var client: config.Client = .{ .port = meta };
    var policy: RootPolicy(Policy) = .{
        .access = .{ .api = api, .credential = credential, .table = root.table_name, .permission_name = permission_name, .filter = filter },
        .io = api.inferenceIo(),
        .source = source,
        .allocator = a,
    };
    const now = try policy.access.nowSeconds(internal);
    for (0..8) |_| {
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
            var active = try roots.Routed.init(source, &captured);
            _ = try active.validateScope(state);
            return .{ .id = id, .cursor = state.acknowledged_cursor, .expires_at = state.expires_at };
        }
        if (state.phase != .enrolling) return error.ChangefeedResynchronizationRequired;
        if (state.roots.len == 0) {
            try change(&client, a, state, .{ .plan_roots = &.{.{ .namespace = root.namespace, .ownership_epoch = 1 }} }, internal);
            continue;
        }
        if (state.roots.len != 1 or !std.mem.eql(u8, &state.roots[0].namespace, &root.namespace))
            return error.ChangefeedResynchronizationRequired;
        const item = state.roots[0];
        if (item.consumer_epoch != 0) {
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
        try source_api.enrollRoot(&client, &policy, a, state, root, tables[0].policy_revision, internal);
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
            .access = .{ .api = api, .credential = credential, .table = "", .permission_name = "", .filter = null },
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

fn retireOne(client: *config.Client, policy: anytype, a: A, state: control.State, request: Context) !bool {
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
