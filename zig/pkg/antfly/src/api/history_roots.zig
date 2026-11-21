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

//! Request-owned discovery for native retained-history owners. One authoritative
//! compact catalog generation binds all names, base incarnations and ranges.
//! This discovers candidate roots; successful owner admission still qualifies
//! the serving protocol. It grants neither public authorization nor a coherent
//! snapshot/distributed-completion guarantee.
const std = @import("std");
const catalog_api = @import("table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const control = @import("../metadata/changefeed_control.zig");
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Root = struct {
    namespace: control.Namespace,
    table_name: []const u8,
    table_binding: control.Digest,
    fence: metadata.CatalogRouteFence,
};
pub const Captured = struct {
    allocator: A,
    session: catalog_api.RoutingSession,
    roots: []Root,
    pub fn deinit(self: *Captured) void {
        self.allocator.free(self.roots);
        self.session.deinit();
        self.* = undefined;
    }
    pub fn find(self: *const Captured, namespace: control.Namespace) !Root {
        var low: usize = 0;
        var high = self.roots.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            switch (std.mem.order(u8, &self.roots[middle].namespace, &namespace)) {
                .lt => low = middle + 1,
                .gt => high = middle,
                .eq => return self.roots[middle],
            }
        }
        return error.ChangefeedResynchronizationRequired;
    }
    /// Caller persists this complete vector before any root dispatch. The
    /// ownership counter belongs to feed metadata, not a hash of topology.
    pub fn initialPlans(self: *const Captured, a: A) ![]control.RootPlan {
        const plans = try a.alloc(control.RootPlan, self.roots.len);
        for (self.roots, plans) |root, *plan| plan.* = .{ .namespace = root.namespace, .ownership_epoch = 1 };
        return plans;
    }

    pub fn validateInstalledRoutes(self: *const Captured, state: control.State) !void {
        const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
        var route_index: usize = 0;
        for (state.roots) |root| {
            const table_id = std.mem.readInt(u64, root.namespace[0..8], .big);
            const table = for (definition.tables) |candidate| {
                if (candidate.table_id == table_id) break candidate;
            } else return error.ChangefeedResynchronizationRequired;
            if (root.ownership_epoch != 1 or root.released) return error.ChangefeedResynchronizationRequired;
            if (table.contract == .writable_iceberg_rows) {
                if (!std.mem.eql(u8, &root.namespace, &control.catalogRootNamespace(table))) return error.ChangefeedResynchronizationRequired;
                continue;
            }
            if (route_index >= self.roots.len or !std.mem.eql(u8, &root.namespace, &self.roots[route_index].namespace))
                return error.ChangefeedResynchronizationRequired;
            route_index += 1;
        }
        if (route_index != self.roots.len) return error.ChangefeedResynchronizationRequired;
    }

    /// Initial ownership only. A replacement root needs a verified handoff
    /// before a feed can use the new route to advance its retained cursor.
    pub fn validateInitialRoutes(self: *const Captured, planned: []const control.Root) !void {
        if (planned.len != self.roots.len) return error.ChangefeedResynchronizationRequired;
        for (planned, self.roots) |root, route| {
            if (!std.mem.eql(u8, &root.namespace, &route.namespace) or root.ownership_epoch != 1 or root.released)
                return error.ChangefeedResynchronizationRequired;
        }
    }
};

test "history root discovery validates the complete installed route vector" {
    var routes = [_]Root{
        .{ .namespace = @splat(1), .table_name = "docs", .table_binding = @splat(1), .fence = undefined },
        .{ .namespace = @splat(2), .table_name = "docs", .table_binding = @splat(1), .fence = undefined },
    };
    const captured: Captured = .{ .roots = &routes, .allocator = std.testing.allocator, .session = undefined };
    var planned = [_]control.Root{
        .{ .namespace = routes[0].namespace, .ownership_epoch = 1, .grant_sequence = 1 },
        .{ .namespace = routes[1].namespace, .ownership_epoch = 1, .grant_sequence = 1 },
    };
    try captured.validateInitialRoutes(&planned);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, captured.validateInitialRoutes(planned[0..1]));
    planned[1].released = true;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, captured.validateInitialRoutes(&planned));
    planned[1].released = false;
    planned[1].ownership_epoch = 2;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, captured.validateInitialRoutes(&planned));
    planned[1].ownership_epoch = 1;
    routes[1].namespace = @splat(3);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, captured.validateInitialRoutes(&planned));
}
pub const Source = struct {
    artifact_owners: ?@import("history_owner_operations.zig").Port = null,
    artifact_history: ?@import("artifact_history_operations.zig").Port = null,
    catalog: catalog_api.CatalogSource,
    controls: @import("history_control_operations.zig").Port,
    pages: @import("history_page_operations.zig").Port,
    bootstrap_pages: ?@import("history_page_operations.zig").Bootstrap.Port = null,
    grants: @import("history_grant_operations.zig").Port,
    /// Authorization must precede capture. Returned locators borrow the owned
    /// generation, so they remain valid even after the publisher retires it.
    pub fn capture(self: Source, a: A, definition: control.Definition, context: Context) !Captured {
        try context.ensureActive();
        try definition.validate();
        var budget = self.catalog.budget(null);
        budget.deadline_ns = @min(budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }) orelse std.math.maxInt(u64), budget.nowNs() +| 5 * std.time.ns_per_s);
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.catalog, budget);
        errdefer session.deinit();
        var roots: std.ArrayList(Root) = .empty;
        errdefer roots.deinit(a);
        for (definition.tables) |expected| {
            try budget.checkpoint();
            const index = session.table_id_indexes.get(expected.table_id) orelse return error.ChangefeedResynchronizationRequired;
            const table = session.snapshot.value.tables[index];
            if (!std.mem.eql(u8, &expected.binding, &control.tableBinding(table))) return error.ChangefeedResynchronizationRequired;
            if (expected.contract == .writable_iceberg_rows) continue;
            if (table.storage.engine != .local or (expected.contract != .native_document and expected.contract != .native_relational)) return error.ChangefeedCapabilityUnsupported;
            const view = try session.tableRoutingView(table.name, budget);
            if (!view.covers_keyspace) return error.TopologyChanged;
            const ranges = session.table_range_refs.get(table.table_id) orelse return error.TopologyChanged;
            if (ranges.ranges.len > control.max_roots - roots.items.len) return error.InvalidChangefeed;
            for (ranges.ranges, 0..) |range, ordinal| {
                try budget.checkpointIndex(ordinal);
                const shard = tables.rangeDocIdentityShardId(range.*);
                const range_id = tables.rangeDocIdentityRangeId(range.*);
                if (range.group_id == 0 or shard == 0 or range_id == 0) return error.TopologyChanged;
                var namespace: control.Namespace = undefined;
                std.mem.writeInt(u64, namespace[0..8], table.table_id, .big);
                std.mem.writeInt(u64, namespace[8..16], shard, .big);
                std.mem.writeInt(u64, namespace[16..24], range_id, .big);
                const route: catalog_api.CatalogGroupRoute = .{ .group_id = range.group_id, .range_id = range_id, .identity_namespace = .{ .table_id = table.table_id, .shard_id = shard, .range_id = range_id } };
                const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = session.snapshot.value.metadata_group_id, .metadata_incarnation = session.snapshot.value.metadata_incarnation, .catalog_revision = session.snapshot.value.catalog_revision, .table_id = table.table_id, .topology_epoch = view.topology_epoch, .route = route };
                try fence.validate();
                try roots.append(a, .{ .namespace = namespace, .table_name = table.name, .table_binding = expected.binding, .fence = fence });
            }
        }
        std.mem.sort(Root, roots.items, {}, struct {
            fn less(_: void, left: Root, right: Root) bool {
                return std.mem.order(u8, &left.namespace, &right.namespace) == .lt;
            }
        }.less);
        for (roots.items, 0..) |root, index| if (index != 0 and std.mem.eql(u8, &roots.items[index - 1].namespace, &root.namespace)) return error.TopologyChanged;
        try budget.checkpoint();
        try context.ensureActive();
        return .{ .allocator = a, .session = session, .roots = try roots.toOwnedSlice(a) };
    }
    pub const RelationalFrameRead = struct {
        root_index: usize,
        position: control.Position,
        now: u64,
        max_events_per_page: u32,
        max_frame_bytes: u32 = @import("history_page_operations.zig").pages.max_source_bytes,
    };

    /// Assemble one complete participant frame and retain certificate evidence
    /// projected to the feed's authorized roots.
    pub fn readRelationalFrame(self: Source, a: A, state: control.State, captured: *const Captured, read: RelationalFrameRead, context: Context) !?@import("history_page_operations.zig").pages.Page {
        const history = @import("history_page_operations.zig");
        const frame = @import("antfly_local_sources").storage_committed_history.frame;
        try context.ensureActive();
        const definition = state.definition orelse return error.ChangefeedConflict;
        if (state.phase != .streaming or read.root_index >= state.roots.len or read.position.event != 0 or read.max_events_per_page == 0 or read.max_events_per_page > 1024)
            return error.ChangefeedCapabilityUnsupported;
        if (read.now == 0 or read.now >= state.expires_at) return error.ChangefeedLeaseExpired;
        if (read.max_frame_bytes == 0 or read.max_frame_bytes > history.pages.max_source_bytes) return error.HistoryTooLarge;
        try captured.validateInstalledRoutes(state);
        const item = state.roots[read.root_index];
        if (item.consumer_epoch == 0) return error.ChangefeedResynchronizationRequired;
        const root = try captured.find(item.namespace);
        for (definition.tables) |table| {
            if (table.table_id != root.fence.table_id) continue;
            if (table.contract != .native_relational) return error.ChangefeedCapabilityUnsupported;
            break;
        } else return error.ChangefeedResynchronizationRequired;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        const projection_count = if (captured.roots.len > 1) captured.roots.len else 0;
        if (projection_count > history.pages.max_projection_roots) return error.ChangefeedCapabilityUnsupported;
        const completion_roots = try scratch.alloc(control.Namespace, projection_count);
        for (completion_roots, captured.roots[0..projection_count]) |*namespace, selected| namespace.* = selected.namespace;
        var input: history.Request = .{ .version = 1, .request = .{
            .reference = .{
                .namespace = root.namespace,
                .authority = state.authority,
                .consumer = state.consumerId(),
                .epoch = item.consumer_epoch,
                .grant = item.grant_sequence,
                .definition = definition.identity(),
            },
            .after = .{ .sequence = read.position.sequence },
            .now_seconds = read.now,
            .max_events = read.max_events_per_page,
            .max_bytes = read.max_frame_bytes,
            .include_completion = true,
            .completion_roots = completion_roots,
        } };
        var header: ?frame.Header = null;
        var completion: @FieldType(history.pages.Page, "completion") = null;
        var completion_effects: []const ?history.pages.CompletionEffect = &.{};
        var effects_digest: control.Digest = undefined;
        var header_digest: control.Digest = undefined;
        var events: []frame.Event = &.{};
        var bytes: usize = 0;
        var latest: u64 = read.position.sequence;
        while (true) {
            try context.ensureActive();
            var page = try self.pages.read(a, root.fence, root.table_name, input, context);
            defer page.deinit();
            try history.wire.validate(page, input.request);
            if (page.observed_latest < latest) return error.ChangefeedResynchronizationRequired;
            latest = page.observed_latest;
            const current = page.header orelse {
                if (header != null) return error.ChangefeedResynchronizationRequired;
                arena.deinit();
                return null;
            };
            switch (current.transaction_scope) {
                .root_local => {},
                .coordinated => if (page.completion == null) return error.ReadUnavailable,
                .unknown => return error.ChangefeedCapabilityUnsupported,
            }
            const digest = control.hash(current);
            const projected_digest = control.hash(page.completion_effects);
            if (header == null) {
                header = current;
                completion = page.completion;
                completion_effects = try scratch.dupe(?history.pages.CompletionEffect, page.completion_effects);
                effects_digest = projected_digest;
                header.?.schema_before = try duplicateOptional(scratch, current.schema_before);
                header.?.schema_after = try duplicateOptional(scratch, current.schema_after);
                header_digest = digest;
                events = try scratch.alloc(frame.Event, page.frame_events);
                bytes = (if (current.schema_before) |value| value.len else 0) +
                    (if (current.schema_after) |value| value.len else 0);
            } else if (!std.meta.eql(digest, header_digest) or page.frame_events != events.len or !std.meta.eql(completion, page.completion) or !std.meta.eql(effects_digest, projected_digest)) {
                return error.ChangefeedResynchronizationRequired;
            }
            for (page.events, page.first_ordinal..) |event, index| {
                if (index > 0 and std.mem.order(u8, events[index - 1].key, event.key) != .lt)
                    return error.HistoryCorrupt;
                bytes += event.key.len + (if (event.before) |value| value.len else 0) +
                    (if (event.after) |value| value.len else 0) + 12;
                if (bytes > read.max_frame_bytes) return error.HistoryTooLarge;
                events[index] = .{
                    .key = try scratch.dupe(u8, event.key),
                    .before = try duplicateOptional(scratch, event.before),
                    .after = try duplicateOptional(scratch, event.after),
                };
            }
            if (page.next.event == 0) {
                _ = try frame.encodedLength(header.?, events);
                try self.validate(a, state, root.namespace, root.fence, root.table_name, context);
                return .{ .arena = arena, .header = header, .completion = completion, .completion_effects = completion_effects, .events = events, .frame_events = page.frame_events, .next = page.next, .observed_latest = latest, .caught_up = page.caught_up };
            }
            input.request.after = page.next;
        }
    }

    /// Validate an existing intent against the current base incarnation/root.
    /// A changed leader is transport discovery; a changed namespace is resync.
    pub fn validate(self: Source, a: A, state: control.State, namespace: control.Namespace, frozen: metadata.CatalogRouteFence, table_name: []const u8, context: Context) !void {
        try state.validate();
        _ = try state.rootIndex(namespace);
        var current = try self.capture(a, state.definition.?, context);
        defer current.deinit();
        const root = try current.find(namespace);
        if (!std.mem.eql(u8, root.table_name, table_name) or !std.meta.eql(root.fence.route, frozen.route) or root.fence.table_id != frozen.table_id or root.fence.metadata_group_id != frozen.metadata_group_id or !std.meta.eql(root.fence.metadata_incarnation, frozen.metadata_incarnation) or root.fence.topology_epoch != frozen.topology_epoch) return error.ChangefeedResynchronizationRequired;
    }
};

const MetadataClient = @import("changefeed_metadata.zig").Client;
const enrollment = @import("../metadata/root_control_driver.zig");

/// Two complete scans with unchanged monotonic heads prove an interval shared
/// by every enrolled root. The first scan also requires each root to have no
/// unpublished coordinated effects. An advanced head invalidates this round.
pub fn qualifyEnrollmentCut(a: A, source: Source, policy: anytype, captured: *const Captured, state: control.State, request: Context) !control.Digest {
    try state.validate();
    if (state.phase != .enrolling or state.roots.len == 0) return error.ChangefeedConflict;
    try captured.validateInstalledRoutes(state);
    const definition = state.definition.?;
    const identity = definition.identity();
    var proof = control.hash(.{ .domain = "antfly:coherent-enrollment-cut:v1", .consumer = state.consumerId(), .definition = identity });
    for (0..2) |scan| {
        for (captured.roots) |route| {
            try request.ensureActive();
            const root = state.roots[try state.rootIndex(route.namespace)];
            if (root.consumer_epoch == 0 or root.pending_control != null) return error.ChangefeedConflict;
            const table = for (definition.tables) |table| {
                if (table.table_id == route.fence.table_id) break table;
            } else return error.ChangefeedResynchronizationRequired;
            try policy.authorizeRoot(state, root.namespace, request);
            try policy.validateRoot(state, root.namespace, route.fence, route.table_name, request);
            const observation = try source.grants.read(a, route.fence, route.table_name, .{
                .version = 1,
                .identity = .{
                    .namespace = root.namespace,
                    .authority = state.authority,
                    .consumer = state.consumerId(),
                    .definition = identity,
                    .sequence = root.grant_sequence,
                    .policy_revision = table.policy_revision,
                    .include_bootstrap = root.snapshot != null,
                    .include_cut = true,
                },
            }, request);
            if (observation != .active) return error.ChangefeedResynchronizationRequired;
            const grant = observation.active;
            if (grant.consumer.epoch != root.consumer_epoch or grant.consumer.start != root.start or
                grant.consumer.acknowledged != root.start or grant.consumer.expires_at_seconds != state.expires_at)
                return error.ChangefeedResynchronizationRequired;
            if (root.snapshot) |pin| {
                const observed = grant.bootstrap orelse return error.ChangefeedResynchronizationRequired;
                if (!std.meta.eql(observed.digest, pin.digest) or observed.expires_at_seconds != pin.expires_at)
                    return error.ChangefeedResynchronizationRequired;
            }
            const latest = grant.quiescent_latest orelse return error.ReadUnavailable;
            if (latest != root.start) return error.ChangefeedEnrollmentCutAdvanced;
            if (scan == 0) proof = control.hash(.{ .proof = proof, .namespace = root.namespace, .grant = root.grant_sequence, .epoch = root.consumer_epoch, .sequence = root.start, .pin = root.snapshot });
        }
    }
    try request.ensureActive();
    return proof;
}

/// Reconcile a root to the metadata lease. An interrupted refresh can
/// resume from the current native grant and the persisted control attempt.
pub fn renewRoot(client: *MetadataClient, policy: anytype, a: A, expected: control.State, root: Root, request: Context) !void {
    for (0..4) |_| {
        const bytes = try client.readChangefeedState(a, expected.id, request);
        defer a.free(bytes);
        var parsed = try control.parse(a, bytes);
        defer parsed.deinit();
        const state = parsed.value;
        try policy.authorizeRoot(state, root.namespace, request);
        if (state.phase != .streaming or state.epoch != expected.epoch or !std.meta.eql(state.authority, expected.authority))
            return error.ChangefeedResynchronizationRequired;
        const item = state.roots[try state.rootIndex(root.namespace)];
        if (item.pending_control != null) {
            try driveRoot(client, policy, a, state, root.namespace, request);
            continue;
        }
        try policy.validateRoot(state, root.namespace, root.fence, root.table_name, request);
        const definition = state.definition orelse return error.ChangefeedResynchronizationRequired;
        const table = for (definition.tables) |candidate| {
            if (candidate.table_id == root.fence.table_id) break candidate;
        } else return error.ChangefeedResynchronizationRequired;
        const query: @import("history_grant_operations.zig").Request = .{ .version = 1, .identity = .{
            .namespace = root.namespace,
            .authority = state.authority,
            .consumer = state.consumerId(),
            .definition = definition.identity(),
            .sequence = item.grant_sequence,
            .policy_revision = table.policy_revision,
        } };
        const identity = query.identity;
        const observation = try policy.source.grants.read(a, root.fence, root.table_name, query, request);
        if (observation != .active) return error.ChangefeedResynchronizationRequired;
        const consumer = observation.active.consumer;
        if (consumer.epoch != item.consumer_epoch or consumer.start != item.start) return error.ChangefeedResynchronizationRequired;
        if (consumer.expires_at_seconds > state.expires_at) continue;
        if (consumer.expires_at_seconds >= expected.expires_at) return;
        try prepareRoot(client, policy, a, state.id, root, .{
            .version = 1,
            .operation = .submit,
            .command = .{ .renew = .{
                .reference = .{
                    .namespace = identity.namespace,
                    .authority = identity.authority,
                    .consumer = identity.consumer,
                    .definition = identity.definition,
                    .grant = identity.sequence,
                    .epoch = consumer.epoch,
                },
                .previous = consumer.expires_at_seconds,
                .next = state.expires_at,
            } },
        }, request);
    }
    return error.MetadataMutationOutcomeUnknown;
}

/// Propagate whole-frame client ACKs through receipt-qualified owner controls.
/// Partial-frame cursors can be durable without moving the native floor.
pub fn flushAcknowledged(client: *MetadataClient, policy: anytype, a: A, confirmed: control.State, root: Root, request: Context) !bool {
    const target = confirmed.roots[try confirmed.rootIndex(root.namespace)].acknowledged.sequence;
    for (0..4) |_| {
        const bytes = try client.readChangefeedState(a, confirmed.id, request);
        defer a.free(bytes);
        var parsed = try control.parse(a, bytes);
        defer parsed.deinit();
        const state = parsed.value;
        try policy.authorizeRoot(state, root.namespace, request);
        if (state.epoch != confirmed.epoch or !std.meta.eql(state.authority, confirmed.authority) or state.phase != .streaming)
            return error.ChangefeedResynchronizationRequired;
        const item = state.roots[try state.rootIndex(root.namespace)];
        const reference: @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer.Reference = .{
            .namespace = root.namespace,
            .authority = state.authority,
            .consumer = state.consumerId(),
            .epoch = item.consumer_epoch,
            .grant = item.grant_sequence,
            .definition = state.definition.?.identity(),
        };
        if (item.reclaimed_through >= target) {
            if (target == item.start) return false;
            try policy.validateRoot(state, root.namespace, root.fence, root.table_name, request);
            // Collection operates on the owner's shared ACK floor. Retrying a
            // lost reply can collect another bounded page, without changing a
            // cursor or deleting history still pinned by any consumer.
            const command: @import("history_control_operations.zig").wire.Request = .{
                .version = 1,
                .operation = .submit,
                .command = .{ .reclaim = .{ .reference = reference, .max_frames = 32 } },
            };
            const result = try policy.source.controls.execute(a, root.fence, root.table_name, command, request);
            try result.validate(command);
            if (!result.confirmed()) return error.MetadataMutationOutcomeUnknown;
            if (result.outcome.? == .rejected) return result.outcome.?.rejected.asError();
            if (result.outcome.? != .applied) return error.ChangefeedResynchronizationRequired;
            return result.outcome.?.applied.reclaimed_frames == command.command.reclaim.max_frames;
        }
        if (item.pending_control != null) {
            try driveRoot(client, policy, a, state, root.namespace, request);
            continue;
        }
        const command: @import("history_control_operations.zig").wire.Request = .{
            .version = 1,
            .operation = .submit,
            .command = .{ .acknowledge = .{
                .reference = reference,
                .previous = item.reclaimed_through,
                .next = target,
            } },
        };
        try prepareRoot(client, policy, a, state.id, root, command, request);
    }
    return error.MetadataMutationOutcomeUnknown;
}

/// Persist or drive the next step of an exact enrollment. The caller owns
/// the complete root plan and decides when its cut can be sealed.
pub fn enrollRoot(client: *MetadataClient, policy: anytype, a: A, state: control.State, root: Root, policy_revision: u64, context: Context) !void {
    const item = state.roots[try state.rootIndex(root.namespace)];
    if (state.phase != .enrolling or item.consumer_epoch != 0) return error.ChangefeedConflict;
    if (item.pending_control != null) return driveRoot(client, policy, a, state, root.namespace, context);
    const definition = state.definition orelse return error.ChangefeedConflict;
    const table = for (definition.tables) |candidate| {
        if (candidate.table_id == root.fence.table_id) break candidate;
    } else return error.ChangefeedResynchronizationRequired;
    if (table.policy_revision != policy_revision) return error.ChangefeedResynchronizationRequired;
    const native = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer;
    const grant: native.Grant = .{
        .namespace = root.namespace,
        .authority = state.authority,
        .consumer = state.consumerId(),
        .definition = definition.identity(),
        .sequence = item.grant_sequence,
        .expires_at_seconds = state.expires_at,
        .policy_revision = policy_revision,
        .require_commit_clock = table.contract == .native_relational,
    };
    const command: native.Command = if (definition.start == .snapshot_then_changes)
        .{ .capture_bootstrap = .{ .grant = grant, .expires_at_seconds = state.bootstrap_expires_at } }
    else
        .{ .enroll = grant };
    try prepareRoot(client, policy, a, state.id, root, .{ .version = 1, .operation = .submit, .command = command }, context);
}

pub fn prepareRoot(client: *MetadataClient, policy: anytype, a: A, id: control.Id, root: Root, command: @import("history_control_operations.zig").wire.Request, request: Context) !void {
    const body = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(body);
    const fence = try std.json.Stringify.valueAlloc(a, root.fence, .{});
    defer a.free(fence);
    try enrollment.prepare(client, policy, a, id, root.namespace, .{ .command = body, .fence = fence, .table = root.table_name }, request);
}

pub fn driveRoot(client: *MetadataClient, policy: anytype, a: A, state: control.State, namespace: control.Namespace, request: Context) !void {
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control.?;
    const driver_outcome = @import("../metadata/root_control_outcome.zig");
    const command = (try driver_outcome.validateRequest(a, state, namespace, pending.payload)).input.command;
    const progress = command == .renew or command == .acknowledge;
    var result = if (pending.stage == .dispatching)
        try enrollment.reconcile(client, policy, policy.source.grants, a, state.id, namespace, request)
    else
        try enrollment.drive(client, policy, policy.source.controls, a, state.id, namespace, request);
    if (progress and pending.stage == .dispatching and result.status == .unproven)
        result = try enrollment.resumeProgress(client, policy, policy.source.controls, a, state.id, namespace, pending.sequence, request);
    if ((command == .capture_bootstrap or progress) and (result.status == .accepted or result.status == .superseded))
        result = try enrollment.reconcile(client, policy, policy.source.grants, a, state.id, namespace, request);
    switch (result.status) {
        .settled => {
            const effect = result.settlement.?.effect;
            if (effect == .rejected) {
                const Rejection = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_ordered.Rejection;
                const rejection = std.enums.fromInt(Rejection, effect.rejected) orelse return error.ChangefeedCorrupt;
                return rejection.asError();
            }
        },
        .reconciled => {},
        else => return result.deferred_error orelse error.MetadataMutationOutcomeUnknown,
    }
}

fn duplicateOptional(a: A, value: ?[]const u8) !?[]const u8 {
    if (value) |bytes| return try a.dupe(u8, bytes);
    return null;
}

pub const test_support = if (@import("builtin").is_test) struct {
    const Fixture = struct {
        generation: *catalog_api.RoutingGeneration,
        captures: usize = 0,
        admin_calls: usize = 0,
        fn acquire(raw: *anyopaque, budget: catalog_api.RoutingBudget, authoritative: bool) !*catalog_api.RoutingGeneration {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try budget.checkpoint();
            try std.testing.expect(authoritative);
            self.captures += 1;
            self.generation.retain();
            return self.generation;
        }
        fn admin(raw: *anyopaque) !metadata.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.admin_calls += 1;
            return error.TestUnexpectedAdminFallback;
        }
        fn source(self: *@This()) Source {
            return .{ .catalog = .{ .ptr = self, .vtable = &.{ .admin_snapshot = admin, .free_admin_snapshot = undefined, .acquire_routing_generation = acquire } }, .controls = undefined, .pages = undefined, .grants = undefined };
        }
    };
    fn snapshot(table_records: []const tables.TableRecord, ranges: []const tables.RangeRecord) metadata.CatalogRoutingSnapshot {
        return .{ .metadata_group_id = 1, .catalog_revision = 9, .tables = @constCast(table_records), .ranges = @constCast(ranges) };
    }
    pub fn contracts() !void {
        const a = std.testing.allocator;
        const records = [_]tables.TableRecord{
            .{ .table_id = 7, .name = "zeta", .schema_json = "{}" },
            .{ .table_id = 8, .name = "alpha", .schema_json = "{}" },
        };
        const ranges = [_]tables.RangeRecord{
            .{ .table_id = 7, .group_id = 72, .range_id = 72, .start_key = "m", .end_key = null, .doc_identity_shard_id = 70, .doc_identity_range_id = 12 },
            .{ .table_id = 8, .group_id = 81, .range_id = 81, .start_key = "", .end_key = null },
            .{ .table_id = 7, .group_id = 71, .range_id = 71, .start_key = "", .end_key = "m", .doc_identity_shard_id = 70, .doc_identity_range_id = 11 },
        };
        const selected = [_]control.Table{
            .{ .table_id = 7, .binding = control.tableBinding(records[0]), .policy_revision = 1, .contract = .native_document },
            .{ .table_id = 8, .binding = control.tableBinding(records[1]), .policy_revision = 1, .contract = .native_relational },
        };
        const definition: control.Definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &selected };
        const generation = try catalog_api.RoutingGeneration.create(a, snapshot(&records, &ranges), .{});
        var fixture: Fixture = .{ .generation = generation };
        var captured = try fixture.source().capture(a, definition, .{});
        defer captured.deinit();
        // Release the publisher; the request still owns names, ranges and maps.
        generation.release();
        try std.testing.expectEqual(@as(usize, 3), captured.roots.len);
        try std.testing.expectEqualStrings("zeta", captured.roots[0].table_name);
        try std.testing.expectEqual(@as(u64, 70), captured.roots[0].fence.route.identity_namespace.shard_id);
        try std.testing.expectEqual(@as(u64, 11), captured.roots[0].fence.route.identity_namespace.range_id);
        try std.testing.expectEqual(@as(u64, 72), captured.roots[1].fence.route.group_id);
        try std.testing.expectEqualStrings("alpha", captured.roots[2].table_name);
        try std.testing.expectEqual(@as(u64, 9), captured.roots[2].fence.catalog_revision);
        try std.testing.expectEqualDeep(captured.roots[1], try captured.find(captured.roots[1].namespace));
        const plans = try captured.initialPlans(a);
        defer a.free(plans);
        try std.testing.expectEqual(@as(usize, 3), plans.len);
        try std.testing.expectEqual(@as(u64, 1), plans[0].ownership_epoch);
        try std.testing.expectEqual(@as(usize, 0), fixture.admin_calls);
        var replaced = records;
        replaced[0].table_id = 9;
        var replacement_ranges = ranges;
        for (&replacement_ranges) |*range| if (range.table_id == 7) {
            range.table_id = 9;
        };
        fixture.generation = try catalog_api.RoutingGeneration.create(a, snapshot(&replaced, &replacement_ranges), .{});
        defer fixture.generation.release();
        try std.testing.expectError(error.ChangefeedResynchronizationRequired, fixture.source().capture(a, definition, .{}));
        var gap = ranges;
        gap[0].start_key = "n";
        const gap_generation = try catalog_api.RoutingGeneration.create(a, snapshot(&records, &gap), .{});
        defer gap_generation.release();
        var gap_fixture: Fixture = .{ .generation = gap_generation };
        try std.testing.expectError(error.TopologyChanged, gap_fixture.source().capture(a, definition, .{}));
        const before = gap_fixture.captures;
        try std.testing.expectError(error.DeadlineExceeded, gap_fixture.source().capture(a, definition, .{ .deadline_ns = 1 }));
        try std.testing.expectEqual(before, gap_fixture.captures);
        try std.testing.expectEqual(@as(usize, 0), gap_fixture.admin_calls);
        var allocation_fixture: Fixture = .{ .generation = captured.session.generation.? };
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var state = try (control.Mutation{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } }).apply(arena.allocator(), .{}, .{ .incarnation = @splat(9), .sequence = 1 });
        state = try (control.Mutation{ .plan_roots = plans }).apply(arena.allocator(), state, .{ .incarnation = @splat(9), .sequence = 2 });
        const Routed = @import("../metadata/native_changefeed_root.zig").Routed;
        var adapter = try Routed.initForRoot(allocation_fixture.source(), &captured, captured.roots[1].namespace);
        try std.testing.expectEqual(selected[0].table_id, (try adapter.validateScope(state)).table_id);
        var relational = try Routed.initForRoot(allocation_fixture.source(), &captured, captured.roots[2].namespace);
        try std.testing.expectEqual(selected[1].table_id, (try relational.validateScope(state)).table_id);
        const root = captured.roots[0];
        try allocation_fixture.source().validate(a, state, root.namespace, root.fence, root.table_name, .{});
        var wrong_fence = root.fence;
        wrong_fence.metadata_incarnation = @splat(9);
        try std.testing.expectError(error.ChangefeedResynchronizationRequired, allocation_fixture.source().validate(a, state, root.namespace, wrong_fence, root.table_name, .{}));
        try enrollmentCut(a, arena.allocator(), state, &captured, allocation_fixture.source());
        try std.testing.checkAllAllocationFailures(a, captureAllocations, .{ &allocation_fixture, definition });
    }

    fn enrollmentCut(a: A, scratch: A, planned: control.State, captured: *const Captured, source: Source) !void {
        var state = planned;
        for (planned.roots, 0..) |root, ordinal| {
            state = try (control.Mutation{ .enroll = .{ .namespace = root.namespace, .ownership_epoch = 1, .grant_sequence = root.grant_sequence, .consumer_epoch = 1, .start = 5 } }).apply(scratch, state, .{ .incarnation = state.authority, .sequence = 3 + ordinal });
        }
        const grants = @import("history_grant_operations.zig");
        const GrantFixture = struct {
            calls: usize = 0,
            fail_at: ?usize = null,
            moved: bool = false,
            fn read(raw: *anyopaque, _: A, _: metadata.CatalogRouteFence, _: []const u8, request: grants.Request, _: Context) !@import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer.GrantObservation {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                try std.testing.expect(request.identity.include_cut);
                return .{ .active = .{
                    .grant = request.identity.sequence,
                    .consumer = .{ .id = request.identity.consumer, .epoch = 1, .start = 5, .acknowledged = 5, .expires_at_seconds = 1000, .policy_revision = request.identity.policy_revision },
                    .limit = 1024,
                    .quiescent_latest = if (self.calls == self.fail_at) (if (self.moved) @as(?u64, 6) else null) else 5,
                } };
            }
        };
        const Policy = struct {
            pub fn authorizeRoot(_: *@This(), _: control.State, _: control.Namespace, _: Context) !void {}
            pub fn validateRoot(_: *@This(), _: control.State, _: control.Namespace, _: metadata.CatalogRouteFence, _: []const u8, _: Context) !void {}
        };
        var policy: Policy = .{};
        var fixture: GrantFixture = .{};
        var reader = source;
        reader.grants = .{ .ptr = &fixture, .read_fn = GrantFixture.read };
        const proof = try qualifyEnrollmentCut(a, reader, &policy, captured, state, .{});
        try std.testing.expectEqual(captured.roots.len * 2, fixture.calls);
        fixture = .{};
        try std.testing.expectEqual(proof, try qualifyEnrollmentCut(a, reader, &policy, captured, state, .{}));
        fixture = .{ .fail_at = 1 };
        try std.testing.expectError(error.ReadUnavailable, qualifyEnrollmentCut(a, reader, &policy, captured, state, .{}));
        fixture = .{ .fail_at = captured.roots.len + 1, .moved = true };
        try std.testing.expectError(error.ChangefeedEnrollmentCutAdvanced, qualifyEnrollmentCut(a, reader, &policy, captured, state, .{}));
    }
    fn captureAllocations(a: A, fixture: *Fixture, definition: control.Definition) !void {
        var owned = try fixture.source().capture(a, definition, .{});
        defer owned.deinit();
    }
} else struct {};
