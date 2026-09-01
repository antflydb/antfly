// Copyright 2026 Antfly, Inc.
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

const builtin = @import("builtin");
const std = @import("std");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_server = @import("../metadata/server.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const raft_reconciler = @import("../raft/reconciler.zig");
const tables_api = @import("tables.zig");

/// One absolute monotonic budget shared by snapshot capture and all CPU-side
/// routing work that follows it. The periodic checkpoint keeps large catalog
/// scans interruptible without putting a clock read on every range.
pub const RoutingBudget = struct {
    deadline_ns: ?u64 = null,

    const checkpoint_stride: usize = 64;

    pub fn init(deadline_ns: ?u64) RoutingBudget {
        return .{ .deadline_ns = deadline_ns };
    }

    pub fn checkpoint(self: RoutingBudget) !void {
        if (self.deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
        }
    }

    pub fn checkpointIndex(self: RoutingBudget, index: usize) !void {
        if (index % checkpoint_stride == 0) try self.checkpoint();
    }
};

fn cloneGroupIdsUntil(
    alloc: std.mem.Allocator,
    source: []const u64,
    budget: RoutingBudget,
) ![]u64 {
    try budget.checkpoint();
    const ids = try alloc.alloc(u64, source.len);
    errdefer alloc.free(ids);
    for (source, ids, 0..) |group_id, *owned_group_id, index| {
        try budget.checkpointIndex(index);
        owned_group_id.* = group_id;
    }
    try budget.checkpoint();
    return ids;
}

pub const CatalogSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Snapshot slices and all transitively referenced bytes must remain
        /// valid until the matching `free_admin_snapshot` call returns.
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
        /// First-class table/range routing capability. First-party sources must
        /// override the unsupported defaults; test doubles that never route may
        /// retain them without silently falling back to an admin snapshot.
        routing_snapshot: *const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = unsupportedRoutingSnapshot,
        /// Allocation-efficient point projection used by mutation routing.
        /// It returns the same owned wire type with zero or one table.
        table_routing_snapshot: ?*const fn (ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = null,
        /// Compact projection captured after a linearizable read barrier. It
        /// is only required to confirm an eventual negative routing result.
        linearizable_routing_snapshot: ?*const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = null,
        linearizable_table_routing_snapshot: ?*const fn (ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = null,
        free_routing_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void = unsupportedFreeRoutingSnapshot,
        /// Request-scoped route capabilities may pin DB identity through this
        /// callback. When present, consumers must not re-derive identity from
        /// an admin snapshot.
        route_identity: ?*const fn (ptr: *anyopaque, table_name: []const u8, group_id: u64) anyerror!?metadata_api.CatalogIdentityNamespace = null,
        /// Request-scoped sources expose the complete authority capability so
        /// internal HTTP fanout can preserve it without re-reading metadata.
        route_fence: ?*const fn (ptr: *anyopaque, group_id: u64) anyerror!?metadata_api.CatalogRouteFence = null,
        /// Request-scoped projections can resolve directly without cloning
        /// their immutable snapshot through the ownership-oriented capture API.
        resolve_route: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query: RouteQuery, deadline_ns: ?u64) anyerror!RouteResult = null,
        /// Fresh route resolution used only to validate a previously selected
        /// capability after consistency barriers or structural admission.
        validate_route: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query: RouteQuery, deadline_ns: ?u64) anyerror!RouteResult = null,
        wait_for_routing_change: *const fn (ptr: *anyopaque, observed_token: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, probe_interval_ns: u64) anyerror!CatalogChangeWaitResult = defaultWaitForRoutingChange,
        await_route: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query: RouteQuery, deadline_ns: u64, probe_interval_ns: u64) anyerror!AwaitRouteResult = null,
        /// Production sources must fail closed when either linearizable
        /// publication validator is unavailable.
        requires_linearizable_publication_fence: bool = false,
        /// Compares a compact contract after a Raft linearizable-read barrier.
        validate_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) anyerror!bool = null,
        validate_table_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) anyerror!bool = null,
    };

    pub fn adminSnapshot(self: CatalogSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    /// Produce the complete routing capability carried by this catalog. A
    /// partial implementation is never exposed as a routing source: callers
    /// either receive capture, authoritative confirmation, and release as one
    /// capability or fail closed before capturing an eventual snapshot.
    pub fn routingSource(self: CatalogSource) !CatalogRoutingSource {
        const linearizable_snapshot = self.vtable.linearizable_routing_snapshot orelse
            return error.CatalogRoutingUnavailable;
        if (self.vtable.routing_snapshot == unsupportedRoutingSnapshot or
            self.vtable.free_routing_snapshot == unsupportedFreeRoutingSnapshot)
        {
            return error.CatalogRoutingUnavailable;
        }
        return .{
            .projection = .{
                .ptr = self.ptr,
                .snapshot = self.vtable.routing_snapshot,
                .free_snapshot = self.vtable.free_routing_snapshot,
            },
            .authority = .{
                .ptr = self.ptr,
                .linearizable_snapshot = linearizable_snapshot,
                .wait_for_change = self.vtable.wait_for_routing_change,
                .await_route = self.vtable.await_route,
            },
        };
    }

    pub fn validatePublication(self: CatalogSource, contract: metadata_api.CatalogPublicationContract) !bool {
        const validate = self.vtable.validate_publication orelse return error.CatalogPublicationFenceUnavailable;
        return try validate(self.ptr, contract);
    }

    pub fn validateTablePublication(self: CatalogSource, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const validate = self.vtable.validate_table_publication orelse return error.CatalogPublicationFenceUnavailable;
        return try validate(self.ptr, contract);
    }

    pub fn fromMetadataService(svc: *metadata_service.MetadataService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataServiceAdminSnapshot,
                .free_admin_snapshot = metadataServiceFreeAdminSnapshot,
                .routing_snapshot = metadataServiceRoutingSnapshot,
                .table_routing_snapshot = metadataServiceTableRoutingSnapshot,
                .linearizable_routing_snapshot = metadataServiceLinearizableRoutingSnapshot,
                .linearizable_table_routing_snapshot = metadataServiceLinearizableTableRoutingSnapshot,
                .free_routing_snapshot = metadataServiceFreeRoutingSnapshot,
                .wait_for_routing_change = metadataServiceWaitForRoutingChange,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataServiceValidatePublication,
                .validate_table_publication = metadataServiceValidateTablePublication,
            },
        };
    }

    pub fn fromMetadataHttpService(svc: *metadata_service.MetadataHttpService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataHttpServiceAdminSnapshot,
                .free_admin_snapshot = metadataHttpServiceFreeAdminSnapshot,
                .routing_snapshot = metadataHttpServiceRoutingSnapshot,
                .table_routing_snapshot = metadataHttpServiceTableRoutingSnapshot,
                .linearizable_routing_snapshot = metadataHttpServiceLinearizableRoutingSnapshot,
                .linearizable_table_routing_snapshot = metadataHttpServiceLinearizableTableRoutingSnapshot,
                .free_routing_snapshot = metadataHttpServiceFreeRoutingSnapshot,
                .wait_for_routing_change = metadataHttpServiceWaitForRoutingChange,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataHttpServiceValidatePublication,
                .validate_table_publication = metadataHttpServiceValidateTablePublication,
            },
        };
    }

    pub fn fromMetadataServer(srv: *metadata_server.MetadataServer) CatalogSource {
        return .{
            .ptr = srv,
            .vtable = &.{
                .admin_snapshot = metadataServerAdminSnapshot,
                .free_admin_snapshot = metadataServerFreeAdminSnapshot,
                .routing_snapshot = metadataServerRoutingSnapshot,
                .table_routing_snapshot = metadataServerTableRoutingSnapshot,
                .linearizable_routing_snapshot = metadataServerLinearizableRoutingSnapshot,
                .linearizable_table_routing_snapshot = metadataServerLinearizableTableRoutingSnapshot,
                .free_routing_snapshot = metadataServerFreeRoutingSnapshot,
                .wait_for_routing_change = metadataServerWaitForRoutingChange,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataServerValidatePublication,
                .validate_table_publication = metadataServerValidateTablePublication,
            },
        };
    }
};

/// A complete table/range routing capability. Unlike `CatalogSource`, which
/// may be used by admin-only code and fixtures, this type cannot represent an
/// eventual reader without its matching authoritative negative-confirmation
/// reader and ownership release operation.
pub const CatalogProjectionSource = struct {
    ptr: *anyopaque,
    snapshot: *const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot,
    free_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void,

    fn capture(self: @This(), deadline_ns: ?u64) !OwnedRoutingSnapshot {
        return .{ .source = self, .value = try self.snapshot(self.ptr, deadline_ns) };
    }
};

pub const CatalogRouteAuthority = struct {
    ptr: *anyopaque,
    linearizable_snapshot: *const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot,
    wait_for_change: *const fn (ptr: *anyopaque, observed_token: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, probe_interval_ns: u64) anyerror!CatalogChangeWaitResult = defaultWaitForRoutingChange,
    await_route: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query: RouteQuery, deadline_ns: u64, probe_interval_ns: u64) anyerror!AwaitRouteResult = null,
};

pub const CatalogRoutingSource = struct {
    projection: CatalogProjectionSource,
    authority: CatalogRouteAuthority,

    pub fn eventualSnapshot(self: CatalogRoutingSource, deadline_ns: ?u64) !OwnedRoutingSnapshot {
        return try self.projection.capture(deadline_ns);
    }

    pub fn linearizableSnapshot(self: CatalogRoutingSource, deadline_ns: ?u64) !OwnedRoutingSnapshot {
        return .{
            .source = self.projection,
            .value = try self.authority.linearizable_snapshot(self.authority.ptr, deadline_ns),
        };
    }

    pub fn waitForChange(
        self: CatalogRoutingSource,
        observed_token: metadata_api.CatalogRoutingChangeToken,
        deadline_ns: u64,
        probe_interval_ns: u64,
    ) !CatalogChangeWaitResult {
        return try self.authority.wait_for_change(self.authority.ptr, observed_token, deadline_ns, probe_interval_ns);
    }
};

/// An owned projection tied to the source that allocated it. Keeping the
/// release operation with the value prevents wrapper layers from pairing a
/// snapshot with a missing or different free callback.
pub const OwnedRoutingSnapshot = struct {
    source: CatalogProjectionSource,
    value: metadata_api.CatalogRoutingSnapshot,

    pub fn deinit(self: *@This()) void {
        self.source.free_snapshot(self.source.ptr, &self.value);
        self.* = undefined;
    }
};

/// Immutable request-scoped catalog capability. A session captures one
/// projection and uses it for every table touched by the request,
/// including graph targets discovered after the source table was admitted.
/// This prevents nested helpers from silently mixing catalog generations or
/// recovering storage identity from an operational admin snapshot.
pub const RoutingSession = struct {
    alloc: std.mem.Allocator,
    base: CatalogSource,
    snapshot: OwnedRoutingSnapshot,
    authoritative: bool,
    table_indexes: std.StringHashMapUnmanaged(usize) = .empty,
    table_id_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    group_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    topology_epochs: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        base: CatalogSource,
        deadline_ns: ?u64,
    ) !RoutingSession {
        const routing = try base.routingSource();
        return try initOwned(alloc, base, try routing.linearizableSnapshot(deadline_ns), true, RoutingBudget.init(deadline_ns));
    }

    /// Use the cached/eventual projection for positive routes. Misses are
    /// replaced by a linearizable projection before the session is exposed.
    pub fn initForRoute(
        alloc: std.mem.Allocator,
        base: CatalogSource,
        table_name: []const u8,
        query: RouteQuery,
        deadline_ns: ?u64,
    ) !RoutingSession {
        const budget = RoutingBudget.init(deadline_ns);
        try budget.checkpoint();
        const routing = try base.routingSource();
        var snapshot = try routing.eventualSnapshot(deadline_ns);
        const candidate = routePlanFromSnapshotWithBudget(alloc, snapshot.value, table_name, query, budget) catch |err| {
            snapshot.deinit();
            return err;
        };
        if (candidate) |plan_value| {
            var plan = plan_value;
            plan.deinit(alloc);
            return try initOwned(alloc, base, snapshot, false, budget);
        }
        snapshot.deinit();
        return try initOwned(
            alloc,
            base,
            try routing.linearizableSnapshot(deadline_ns),
            true,
            budget,
        );
    }

    fn initOwned(
        alloc: std.mem.Allocator,
        base: CatalogSource,
        snapshot_value: OwnedRoutingSnapshot,
        authoritative: bool,
        budget: RoutingBudget,
    ) !RoutingSession {
        try budget.checkpoint();
        var snapshot = snapshot_value;
        errdefer snapshot.deinit();
        var self: RoutingSession = .{
            .alloc = alloc,
            .base = base,
            .snapshot = snapshot,
            .authoritative = authoritative,
        };
        errdefer {
            self.table_indexes.deinit(alloc);
            self.table_id_indexes.deinit(alloc);
            self.group_indexes.deinit(alloc);
            self.topology_epochs.deinit(alloc);
        }
        try self.table_indexes.ensureTotalCapacity(alloc, @intCast(snapshot.value.tables.len));
        try self.table_id_indexes.ensureTotalCapacity(alloc, @intCast(snapshot.value.tables.len));
        try self.group_indexes.ensureTotalCapacity(alloc, @intCast(snapshot.value.ranges.len));
        try self.topology_epochs.ensureTotalCapacity(alloc, @intCast(snapshot.value.tables.len));
        for (snapshot.value.tables, 0..) |table, index| {
            try budget.checkpointIndex(index);
            if (self.table_indexes.contains(table.name)) return error.InvalidCatalogProjection;
            if (self.table_id_indexes.contains(table.table_id)) return error.InvalidCatalogProjection;
            self.table_indexes.putAssumeCapacity(table.name, index);
            self.table_id_indexes.putAssumeCapacity(table.table_id, index);
        }
        for (snapshot.value.ranges, 0..) |range, index| {
            try budget.checkpointIndex(index);
            if (!self.table_id_indexes.contains(range.table_id)) return error.InvalidCatalogProjection;
            if (self.group_indexes.contains(range.group_id)) return error.InvalidCatalogProjection;
            self.group_indexes.putAssumeCapacity(range.group_id, index);
        }
        // Build every table epoch in O(R log R + T). Scanning and sorting the
        // complete range set once avoids O(T*R) request setup on large
        // multi-tenant catalogs.
        const range_refs = try alloc.alloc(*const metadata_table_manager.RangeRecord, snapshot.value.ranges.len);
        defer alloc.free(range_refs);
        for (snapshot.value.ranges, range_refs, 0..) |*range, *ref, index| {
            try budget.checkpointIndex(index);
            ref.* = range;
        }
        std.sort.pdq(*const metadata_table_manager.RangeRecord, range_refs, {}, struct {
            fn lessThan(_: void, a: *const metadata_table_manager.RangeRecord, b: *const metadata_table_manager.RangeRecord) bool {
                if (a.table_id != b.table_id) return a.table_id < b.table_id;
                return switch (std.mem.order(u8, a.start_key, b.start_key)) {
                    .lt => true,
                    .gt => false,
                    // Preserve projection order for equal boundaries so the
                    // epoch exactly matches the established stable insertion
                    // ordering used by route plans and validation.
                    .eq => @intFromPtr(a) < @intFromPtr(b),
                };
            }
        }.lessThan);
        // The standard sort is not fallible, so check immediately afterward
        // and never expose a successfully routed request past its deadline.
        try budget.checkpoint();
        var first: usize = 0;
        while (first < range_refs.len) {
            try budget.checkpointIndex(first);
            var end = first + 1;
            while (end < range_refs.len and range_refs[end].table_id == range_refs[first].table_id) : (end += 1) {}
            const table_id = range_refs[first].table_id;
            const table_index = self.table_id_indexes.get(table_id) orelse return error.InvalidCatalogProjection;
            self.topology_epochs.putAssumeCapacity(
                table_id,
                try topologyEpochFromSortedRangesWithBudget(snapshot.value.tables[table_index], range_refs[first..end], budget),
            );
            first = end;
        }
        for (snapshot.value.tables, 0..) |table, index| {
            try budget.checkpointIndex(index);
            if (self.topology_epochs.contains(table.table_id)) continue;
            self.topology_epochs.putAssumeCapacity(
                table.table_id,
                topologyEpochFromSortedRanges(table, &.{}),
            );
        }
        try budget.checkpoint();
        // Ownership moved into self; keep the errdefer from releasing it.
        snapshot = undefined;
        return self;
    }

    pub fn deinit(self: *RoutingSession) void {
        self.table_indexes.deinit(self.alloc);
        self.table_id_indexes.deinit(self.alloc);
        self.group_indexes.deinit(self.alloc);
        self.topology_epochs.deinit(self.alloc);
        self.snapshot.deinit();
        self.* = undefined;
    }

    pub fn catalog(self: *RoutingSession) CatalogSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: CatalogSource.VTable = .{
        .admin_snapshot = adminSnapshot,
        .free_admin_snapshot = freeAdminSnapshot,
        .routing_snapshot = routingSnapshot,
        .linearizable_routing_snapshot = linearizableRoutingSnapshot,
        .free_routing_snapshot = freeRoutingSnapshot,
        .route_identity = routeIdentity,
        .route_fence = routeFence,
        .resolve_route = resolvePinnedRoute,
        .validate_route = resolveObservedRoute,
        .wait_for_routing_change = waitForRoutingChange,
        .requires_linearizable_publication_fence = true,
        .validate_publication = validatePublication,
        .validate_table_publication = validateTablePublication,
    };

    fn cast(ptr: *anyopaque) *RoutingSession {
        return @ptrCast(@alignCast(ptr));
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        return try cast(ptr).base.adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        cast(ptr).base.freeAdminSnapshot(snapshot);
    }

    fn routingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
        if (deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
        }
        const self = cast(ptr);
        return try cloneRoutingSnapshot(self.alloc, self.snapshot.value, deadline_ns);
    }

    fn freeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
        freeClonedRoutingSnapshot(cast(ptr).alloc, snapshot);
    }

    fn linearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
        const self = cast(ptr);
        if (self.authoritative) return try routingSnapshot(ptr, deadline_ns);
        // A request-scoped catalog is an immutable projection, not a portal
        // back into mutable metadata. Callers that need an authoritative miss
        // must restart planning with `RoutingSession.init`; silently upgrading
        // here could combine a route from revision N with a fence from N+1.
        return error.CatalogProjectionRefreshRequired;
    }

    fn routeIdentity(
        ptr: *anyopaque,
        table_name: []const u8,
        group_id: u64,
    ) !?metadata_api.CatalogIdentityNamespace {
        const self = cast(ptr);
        if (self.table_indexes.get(table_name)) |table_index| {
            if (self.group_indexes.get(group_id)) |range_index| {
                const table = self.snapshot.value.tables[table_index];
                const range = self.snapshot.value.ranges[range_index];
                if (range.table_id == table.table_id) {
                    return .{
                        .table_id = table.table_id,
                        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
                    };
                }
            }
        }
        return null;
    }

    fn routeFence(ptr: *anyopaque, group_id: u64) !?metadata_api.CatalogRouteFence {
        const self = cast(ptr);
        if (self.group_indexes.get(group_id)) |range_index| {
            const snapshot = self.snapshot.value;
            const range = snapshot.ranges[range_index];
            const table_index = self.table_id_indexes.get(range.table_id) orelse return error.InvalidCatalogProjection;
            const table = snapshot.tables[table_index];
            const range_id = metadata_table_manager.rangeDocIdentityRangeId(range);
            return .{
                .metadata_group_id = snapshot.metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .catalog_revision = snapshot.catalog_revision,
                .table_id = table.table_id,
                .topology_epoch = self.topology_epochs.get(table.table_id) orelse return error.InvalidCatalogProjection,
                .route = .{
                    .group_id = range.group_id,
                    .range_id = range_id,
                    .identity_namespace = .{
                        .table_id = table.table_id,
                        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                        .range_id = range_id,
                    },
                },
            };
        }
        return null;
    }

    fn resolvePinnedRoute(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        query: RouteQuery,
        deadline_ns: ?u64,
    ) !RouteResult {
        const self = cast(ptr);
        if (deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return .timed_out;
        }
        const budget = RoutingBudget.init(deadline_ns);
        const resolved = routePlanFromSnapshotWithBudget(alloc, self.snapshot.value, table_name, query, budget) catch |err| switch (err) {
            error.CatalogRoutingSnapshotTimeout => return .timed_out,
            else => return err,
        };
        if (resolved) |plan|
            return .{ .found = plan };
        if (self.authoritative) return .not_found;
        return error.CatalogProjectionRefreshRequired;
    }

    fn resolveObservedRoute(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        query: RouteQuery,
        deadline_ns: ?u64,
    ) !RouteResult {
        const self = cast(ptr);
        // Route consumption is deliberately eventually consistent. Recheck
        // against the latest compact projection so a receiver that has
        // observed a cutover rejects an older capability, without placing a
        // linearizable metadata barrier on every query. Miss confirmation and
        // publication/structural validation retain their separate authority
        // paths through resolveCatalogRoute and publication contracts.
        return try resolveCatalogRoute(alloc, self.base, table_name, query, deadline_ns);
    }

    fn waitForRoutingChange(
        ptr: *anyopaque,
        _: metadata_api.CatalogRoutingChangeToken,
        _: u64,
        _: u64,
    ) !CatalogChangeWaitResult {
        if (cast(ptr).authoritative) return .authoritative_absence;
        return error.CatalogProjectionRefreshRequired;
    }

    fn validatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        return try cast(ptr).base.validatePublication(contract);
    }

    fn validateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
        return try cast(ptr).base.validateTablePublication(contract);
    }
};

fn cloneRoutingSnapshot(
    alloc: std.mem.Allocator,
    source: metadata_api.CatalogRoutingSnapshot,
    deadline_ns: ?u64,
) !metadata_api.CatalogRoutingSnapshot {
    const budget = RoutingBudget.init(deadline_ns);
    try budget.checkpoint();
    const tables = try alloc.alloc(metadata_table_manager.TableRecord, source.tables.len);
    var table_count: usize = 0;
    errdefer {
        for (tables[0..table_count]) |table| metadata_table_manager.freeTable(alloc, table);
        alloc.free(tables);
    }
    for (source.tables, 0..) |table, index| {
        try budget.checkpointIndex(index);
        tables[index] = try metadata_table_manager.cloneRoutingTable(alloc, table);
        table_count = index + 1;
    }

    const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, source.ranges.len);
    var range_count: usize = 0;
    errdefer {
        for (ranges[0..range_count]) |range| metadata_table_manager.freeRange(alloc, range);
        alloc.free(ranges);
    }
    for (source.ranges, 0..) |range, index| {
        try budget.checkpointIndex(index);
        ranges[index] = try metadata_table_manager.cloneRoutingRange(alloc, range);
        range_count = index + 1;
    }
    try budget.checkpoint();
    return .{
        .metadata_group_id = source.metadata_group_id,
        .metadata_incarnation = source.metadata_incarnation,
        .catalog_revision = source.catalog_revision,
        .change_token = source.change_token,
        .tables = tables,
        .ranges = ranges,
    };
}

fn freeClonedRoutingSnapshot(
    alloc: std.mem.Allocator,
    snapshot: *metadata_api.CatalogRoutingSnapshot,
) void {
    for (snapshot.tables) |table| metadata_table_manager.freeTable(alloc, table);
    if (snapshot.tables.len > 0) alloc.free(snapshot.tables);
    for (snapshot.ranges) |range| metadata_table_manager.freeRange(alloc, range);
    if (snapshot.ranges.len > 0) alloc.free(snapshot.ranges);
    snapshot.* = undefined;
}

pub const CatalogChangeWaitResult = enum {
    changed,
    /// The bounded watch completed without proving that the caller's overall
    /// deadline expired. Re-resolve so replica selection can make progress.
    retry,
    /// The authority completed a linearizable confirmation and the catalog
    /// cursor still matches the snapshot in which the route was absent.
    authoritative_absence,
};

pub fn emptyCatalogSource() CatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = emptyAdminSnapshot,
            .free_admin_snapshot = emptyFreeAdminSnapshot,
        },
    };
}

fn unsupportedRoutingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
    return error.CatalogRoutingUnavailable;
}

fn unsupportedFreeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
    unreachable;
}

/// Compatibility path for static fixtures and remote sources that do not yet
/// expose a server-side long poll. It bounds the probe by both the caller's
/// cadence and absolute deadline; first-party local services override this
/// with their catalog publication signal.
fn defaultWaitForRoutingChange(_: *anyopaque, _: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, probe_interval_ns: u64) !CatalogChangeWaitResult {
    const now_ns = platform_time.monotonicNs();
    if (now_ns >= deadline_ns) return .retry;
    const effective_probe_ns = @max(probe_interval_ns, std.time.ns_per_ms);
    const wait_ns = @min(deadline_ns - now_ns, effective_probe_ns);
    if (wait_ns > 0) {
        const wait_ms = @max(@as(u64, 1), @divTrunc(wait_ns, std.time.ns_per_ms));
        platform_clock.Clock.real().sleepMs(wait_ms);
    }
    return .retry;
}

/// Explicit adapter for static test doubles whose fixture data is authored as
/// an admin snapshot. Wiring both capture functions declares that the fixture
/// itself is authoritative; production constructors must provide compact
/// routing callbacks directly and never use this adapter.
pub fn TestAdminRoutingAdapter(
    comptime admin_snapshot: *const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot,
    comptime free_admin_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
) type {
    if (!builtin.is_test) @compileError("TestAdminRoutingAdapter is test-only");
    return struct {
        pub fn routingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            if (deadline_ns) |deadline| {
                if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
            }
            var admin = try admin_snapshot(ptr);
            defer free_admin_snapshot(ptr, &admin);

            const tables = try std.testing.allocator.alloc(metadata_table_manager.TableRecord, admin.tables.len);
            var table_count: usize = 0;
            errdefer {
                for (tables[0..table_count]) |table| metadata_table_manager.freeTable(std.testing.allocator, table);
                std.testing.allocator.free(tables);
            }
            for (admin.tables, 0..) |table, index| {
                tables[index] = try metadata_table_manager.cloneTable(std.testing.allocator, table);
                table_count = index + 1;
            }

            const ranges = try std.testing.allocator.alloc(metadata_table_manager.RangeRecord, admin.ranges.len);
            var range_count: usize = 0;
            errdefer {
                for (ranges[0..range_count]) |range| metadata_table_manager.freeRange(std.testing.allocator, range);
                std.testing.allocator.free(ranges);
            }
            for (admin.ranges, 0..) |range, index| {
                ranges[index] = try metadata_table_manager.cloneRange(std.testing.allocator, range);
                range_count = index + 1;
            }
            return .{
                .metadata_group_id = admin.status.metadata_group_id,
                .metadata_incarnation = admin.status.metadata_incarnation,
                .change_token = .{
                    .metadata_group_id = admin.status.metadata_group_id,
                    .metadata_incarnation = admin.status.metadata_incarnation,
                },
                .tables = tables,
                .ranges = ranges,
            };
        }

        pub fn linearizableSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return try routingSnapshot(ptr, deadline_ns);
        }

        pub fn freeRoutingSnapshot(_: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
            for (snapshot.tables) |table| metadata_table_manager.freeTable(std.testing.allocator, table);
            std.testing.allocator.free(snapshot.tables);
            for (snapshot.ranges) |range| metadata_table_manager.freeRange(std.testing.allocator, range);
            std.testing.allocator.free(snapshot.ranges);
            snapshot.* = undefined;
        }
    };
}

fn emptyAdminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
    return .{
        .status = .{
            .metadata_group_id = 0,
            .metrics = .{},
        },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
}

fn emptyFreeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

pub const TableRangeRef = struct {
    group_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8,
};

pub fn resolveSingleRangeGroup(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?u64 {
    return try resolveSingleRangeGroupUntil(alloc, catalog, table_name, null);
}

pub fn resolveSingleRangeGroupUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| if (plan.groups.len == 1)
            plan.groups[0].group_id
        else
            error.UnsupportedMultiRangeTable,
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupForKey(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !?u64 {
    return try resolveGroupForKeyUntil(alloc, catalog, table_name, key, null);
}

pub fn resolveGroupForKeyUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.groups[0].group_id,
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub const RoutedGroupSnapshot = struct {
    route: ?CatalogGroupRoute,
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    catalog_revision: u64,
    topology_epoch: u64,

    pub fn fence(self: @This()) ?metadata_api.CatalogRouteFence {
        const route = self.route orelse return null;
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .catalog_revision = self.catalog_revision,
            .table_id = route.identity_namespace.table_id,
            .topology_epoch = self.topology_epoch,
            .route = route,
        };
    }
};

/// Resolve a key and compute the routing epoch from one catalog snapshot.
/// Callers can perform an external consistency barrier, acquire structural
/// read admission, and then validate the captured epoch without a torn
/// epoch/route pair.
pub fn routedGroupSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !RoutedGroupSnapshot {
    return try routedGroupSnapshotUntil(alloc, catalog, table_name, key, null);
}

pub fn routedGroupSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    deadline_ns: ?u64,
) !RoutedGroupSnapshot {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| .{
            .route = plan.groups[0],
            .metadata_group_id = plan.metadata_group_id,
            .metadata_incarnation = plan.metadata_incarnation,
            .catalog_revision = plan.catalog_revision,
            .topology_epoch = plan.topology_epoch,
        },
        .not_found => .{ .route = null, .metadata_group_id = 0, .metadata_incarnation = null, .catalog_revision = 0, .topology_epoch = 0 },
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

/// Resolve an already-selected group from the same compact projection used
/// for key routing. Distributed workers use this to retain the exact catalog
/// identity when their coordinator has selected the group before dispatch.
pub fn routedGroupIdSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    deadline_ns: ?u64,
) !RoutedGroupSnapshot {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .group = group_id }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| .{
            .route = plan.groups[0],
            .metadata_group_id = plan.metadata_group_id,
            .metadata_incarnation = plan.metadata_incarnation,
            .catalog_revision = plan.catalog_revision,
            .topology_epoch = plan.topology_epoch,
        },
        .not_found => .{ .route = null, .metadata_group_id = 0, .metadata_incarnation = null, .catalog_revision = 0, .topology_epoch = 0 },
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

/// Capture exact routes for an already-planned fanout. Request-scoped pinned
/// catalogs are preferred so nested phases retain the original decision;
/// ordinary catalogs resolve all requested groups from one compact snapshot.
pub fn routedGroupsSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_ids: []const u64,
    deadline_ns: ?u64,
) !RoutedSpanSnapshot {
    const budget = RoutingBudget.init(deadline_ns);
    try budget.checkpoint();
    if (catalog.vtable.route_fence) |route_fence| {
        const route_identity = catalog.vtable.route_identity;
        if (group_ids.len != 0 and route_identity == null) return error.CatalogRouteFenceUnsupported;
        const routes = try alloc.alloc(CatalogGroupRoute, group_ids.len);
        errdefer alloc.free(routes);
        var first: ?metadata_api.CatalogRouteFence = null;
        for (group_ids, routes, 0..) |group_id, *route, index| {
            try budget.checkpointIndex(index);
            const fence = (try route_fence(catalog.ptr, group_id)) orelse return error.TopologyChanged;
            try fence.validate();
            if (fence.route.group_id != group_id) return error.TopologyChanged;
            const identity = (route_identity.?(catalog.ptr, table_name, group_id) catch |err| switch (err) {
                // A request-scoped catalog can reject a table name that is
                // not bound to its pinned capability. Treat that exactly like
                // any other stale topology observation so callers replan.
                error.RouteIdentityNotPinned => return error.TopologyChanged,
                else => return err,
            }) orelse return error.TopologyChanged;
            if (!std.meta.eql(identity, fence.route.identity_namespace)) return error.TopologyChanged;
            if (first) |expected| {
                if (fence.metadata_group_id != expected.metadata_group_id or
                    !std.meta.eql(fence.metadata_incarnation, expected.metadata_incarnation) or
                    fence.catalog_revision != expected.catalog_revision or
                    fence.table_id != expected.table_id or
                    fence.topology_epoch != expected.topology_epoch)
                {
                    return error.TopologyChanged;
                }
            } else {
                first = fence;
            }
            route.* = fence.route;
        }
        const ids = try cloneGroupIdsUntil(alloc, group_ids, budget);
        errdefer alloc.free(ids);
        const authority = first orelse return .{
            .routes = routes,
            .group_ids = ids,
            .metadata_group_id = 0,
            .metadata_incarnation = null,
            .table_id = 0,
            .catalog_revision = 0,
            .topology_epoch = 0,
        };
        try budget.checkpoint();
        return .{
            .routes = routes,
            .group_ids = ids,
            .metadata_group_id = authority.metadata_group_id,
            .metadata_incarnation = authority.metadata_incarnation,
            .table_id = authority.table_id,
            .catalog_revision = authority.catalog_revision,
            .topology_epoch = authority.topology_epoch,
        };
    }

    var result = try resolveCatalogRoute(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    const plan = switch (result) {
        .found => |value| value,
        .not_found => return error.TopologyChanged,
        .timed_out => return error.CatalogRoutingSnapshotTimeout,
    };
    const routes = try plan.selectGroupsUntil(alloc, group_ids, budget);
    errdefer alloc.free(routes);
    const ids = try cloneGroupIdsUntil(alloc, group_ids, budget);
    errdefer alloc.free(ids);
    try budget.checkpoint();
    return .{
        .routes = routes,
        .group_ids = ids,
        .metadata_group_id = plan.metadata_group_id,
        .metadata_incarnation = plan.metadata_incarnation,
        .table_id = plan.table_id,
        .catalog_revision = plan.catalog_revision,
        .topology_epoch = plan.topology_epoch,
    };
}

pub fn resolveGroupForKeyFromRanges(
    ranges: []const *const metadata_table_manager.RangeRecord,
    key: []const u8,
) ?u64 {
    for (ranges) |range| {
        if (rangeContainsKey(range.*, key)) return range.group_id;
    }
    return null;
}

/// One authoritative, compact table projection suitable for write admission.
/// It pins key routing, storage identity, and topology epoch to the same
/// linearizable catalog revision without capturing operational admin state.
pub const AuthoritativeTableRoutingSnapshot = struct {
    snapshot: OwnedRoutingSnapshot,
    table_id: u64,
    ranges: []const *const metadata_table_manager.RangeRecord,
    topology_epoch: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.ranges);
        self.snapshot.deinit();
        self.* = undefined;
    }

    pub fn resolveRouteForKey(self: *const @This(), key: []const u8) ?CatalogGroupRoute {
        for (self.ranges) |range| {
            if (!rangeContainsKey(range.*, key)) continue;
            const range_id = metadata_table_manager.rangeDocIdentityRangeId(range.*);
            return .{
                .group_id = range.group_id,
                .range_id = range_id,
                .identity_namespace = .{
                    .table_id = self.table_id,
                    .shard_id = metadata_table_manager.rangeDocIdentityShardId(range.*),
                    .range_id = range_id,
                },
            };
        }
        return null;
    }

    pub fn coversKeyspace(self: *const @This()) bool {
        if (self.ranges.len == 0 or self.ranges[0].start_key.len != 0) return false;
        for (self.ranges[0 .. self.ranges.len - 1], self.ranges[1..]) |left, right| {
            const end_key = left.end_key orelse return false;
            if (std.mem.order(u8, left.start_key, end_key) != .lt) return false;
            if (!std.mem.eql(u8, end_key, right.start_key)) return false;
        }
        const last = self.ranges[self.ranges.len - 1];
        return last.end_key == null;
    }

    /// Materialize the complete immutable capability selected with a route.
    /// The name of this container is retained for source compatibility, but
    /// callers may obtain it from either the observed or authoritative compact
    /// projection. Safety comes from validating this fence under mutation
    /// admission at the data-Raft leader, not from making every coordinator
    /// perform a metadata quorum read.
    pub fn fenceForRoute(self: *const @This(), route: CatalogGroupRoute) metadata_api.CatalogRouteFence {
        return .{
            .metadata_group_id = self.snapshot.value.metadata_group_id,
            .metadata_incarnation = self.snapshot.value.metadata_incarnation,
            .catalog_revision = self.snapshot.value.catalog_revision,
            .table_id = self.table_id,
            .topology_epoch = self.topology_epoch,
            .route = route,
        };
    }
};

fn tableRoutingSnapshotFromOwned(
    alloc: std.mem.Allocator,
    snapshot_value: OwnedRoutingSnapshot,
    table_name: []const u8,
) !?AuthoritativeTableRoutingSnapshot {
    var snapshot = snapshot_value;
    errdefer snapshot.deinit();
    const table = findTableByName(snapshot.value.tables, table_name) orelse {
        snapshot.deinit();
        return null;
    };
    const ranges = try listTableRanges(alloc, snapshot.value.ranges, table.table_id);
    errdefer alloc.free(ranges);
    if (ranges.len == 0) {
        alloc.free(ranges);
        snapshot.deinit();
        return null;
    }
    sortRangeRefs(ranges);
    return .{
        .snapshot = snapshot,
        .table_id = table.table_id,
        .ranges = ranges,
        .topology_epoch = topologyEpochFromSortedRanges(table.*, ranges),
    };
}

/// Capture the latest locally observed compact projection for positive write
/// routing. A stale capability is harmless because the mutation leader must
/// validate it while holding structural admission before proposing it.
pub fn observedTableRoutingSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !?AuthoritativeTableRoutingSnapshot {
    const routing = try catalog.routingSource();
    return try tableRoutingSnapshotFromOwned(
        alloc,
        try routing.eventualSnapshot(deadline_ns),
        table_name,
    );
}

/// Fast positive routing with authoritative negative confirmation. This is
/// the coordinator-side half of routed write admission: normal traffic reads
/// the cached projection, while a missing table/range cannot be reported until
/// a compact linearizable snapshot confirms it.
pub fn tableRoutingSnapshotForWrite(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !?AuthoritativeTableRoutingSnapshot {
    const point_capture = catalog.vtable.table_routing_snapshot;
    const point_authority = catalog.vtable.linearizable_table_routing_snapshot;
    if ((point_capture == null) != (point_authority == null)) {
        return error.CatalogRoutingUnavailable;
    }
    if (point_capture != null and catalog.vtable.free_routing_snapshot == unsupportedFreeRoutingSnapshot) {
        return error.CatalogRoutingUnavailable;
    }
    const projection_source = CatalogProjectionSource{
        .ptr = catalog.ptr,
        .snapshot = unsupportedRoutingSnapshot,
        .free_snapshot = catalog.vtable.free_routing_snapshot,
    };
    const observed_value = if (point_capture) |capture|
        try tableRoutingSnapshotFromOwned(alloc, .{
            .source = projection_source,
            .value = try capture(catalog.ptr, table_name, deadline_ns),
        }, table_name)
    else
        try observedTableRoutingSnapshot(alloc, catalog, table_name, deadline_ns);
    if (observed_value) |value| {
        var observed = value;
        if (observed.coversKeyspace()) return observed;
        observed.deinit(alloc);
    }
    if (point_authority) |capture|
        return try tableRoutingSnapshotFromOwned(alloc, .{
            .source = projection_source,
            .value = try capture(catalog.ptr, table_name, deadline_ns),
        }, table_name);
    return try authoritativeTableRoutingSnapshot(alloc, catalog, table_name, deadline_ns);
}

pub fn authoritativeTableRoutingSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !?AuthoritativeTableRoutingSnapshot {
    const routing = try catalog.routingSource();
    return try tableRoutingSnapshotFromOwned(
        alloc,
        try routing.linearizableSnapshot(deadline_ns),
        table_name,
    );
}

/// Owns one catalog snapshot and its sorted table-range projection for the
/// lifetime of transaction routing. This keeps every key in a table pinned to
/// the same topology without taking a catalog snapshot for each operation.
pub const TransactionRoutingSnapshot = struct {
    catalog: CatalogSource,
    snapshot: metadata_api.AdminSnapshot,
    ranges: []const *const metadata_table_manager.RangeRecord,
    topology_epoch: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        metadata_admin.freeRangeRefs(alloc, self.ranges);
        self.catalog.freeAdminSnapshot(&self.snapshot);
        self.* = undefined;
    }

    pub fn resolveGroupForKey(self: *const @This(), key: []const u8) ?u64 {
        return resolveGroupForKeyFromRanges(self.ranges, key);
    }
};

/// Captures and validates the table topology once for a transaction routing
/// pass. The returned range pointers remain valid until `deinit`.
pub fn transactionRoutingSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?TransactionRoutingSnapshot {
    var snapshot = try catalog.adminSnapshot();
    errdefer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);

    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    errdefer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    sortRangeRefs(ranges);

    return .{
        .catalog = catalog,
        .snapshot = snapshot,
        .ranges = ranges,
        .topology_epoch = topologyEpochFromSortedRanges(table.*, ranges),
    };
}

/// Whether a table with this name currently exists in the catalog. Used by
/// cross-table graph hydration to fail closed (skip) rather than error when a
/// node references a dropped table.
pub fn tableExists(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !bool {
    return try tableExistsUntil(alloc, catalog, table_name, null);
}

pub fn tableExistsUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !bool {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .table, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => true,
        .not_found => false,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn topologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    return try topologyEpochUntil(alloc, catalog, table_name, null);
}

pub fn topologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !u64 {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => 0,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

fn topologyEpochForValidationUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !u64 {
    var result = try resolveCatalogRouteForValidation(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => 0,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

fn topologyEpochFromSnapshot(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
) !u64 {
    const ranges = try metadata_admin.listTableRanges(alloc, snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    return topologyEpochFromSortedRanges(table, ranges);
}

fn topologyEpochFromSortedRanges(
    table: metadata_table_manager.TableRecord,
    ranges: []const *const metadata_table_manager.RangeRecord,
) u64 {
    return topologyEpochFromSortedRangesWithBudget(table, ranges, .{}) catch unreachable;
}

fn topologyEpochFromSortedRangesWithBudget(
    table: metadata_table_manager.TableRecord,
    ranges: []const *const metadata_table_manager.RangeRecord,
    budget: RoutingBudget,
) !u64 {
    try budget.checkpoint();
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(table.name);
    hasher.update(std.mem.asBytes(&table.table_id));
    hasher.update(std.mem.asBytes(&@as(u64, @intCast(ranges.len))));
    for (ranges, 0..) |range, index| {
        try budget.checkpointIndex(index);
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(std.mem.asBytes(&range.range_id));
        const identity_shard_id = metadata_table_manager.rangeDocIdentityShardId(range.*);
        const identity_range_id = metadata_table_manager.rangeDocIdentityRangeId(range.*);
        hasher.update(std.mem.asBytes(&identity_shard_id));
        hasher.update(std.mem.asBytes(&identity_range_id));
        hasher.update(range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&[_]u8{1});
            hasher.update(end_key);
        } else {
            hasher.update(&[_]u8{0});
        }
    }
    try budget.checkpoint();
    return hasher.final();
}

test "routing topology epoch fences identity-only changes" {
    const table = metadata_table_manager.TableRecord{ .table_id = 7, .name = "docs" };
    var before = metadata_table_manager.RangeRecord{
        .group_id = 7001,
        .range_id = 11,
        .table_id = 7,
        .start_key = "",
        .doc_identity_shard_id = 7001,
        .doc_identity_range_id = 11,
    };
    var after = before;
    after.doc_identity_shard_id = 9001;
    after.doc_identity_range_id = 12;
    const before_ranges = [_]*const metadata_table_manager.RangeRecord{&before};
    const after_ranges = [_]*const metadata_table_manager.RangeRecord{&after};
    try std.testing.expect(topologyEpochFromSortedRanges(table, &before_ranges) !=
        topologyEpochFromSortedRanges(table, &after_ranges));
}

test "authoritative write routing pins keys and identity in one compact snapshot" {
    const State = struct {
        eventual_calls: usize = 0,
        point_calls: usize = 0,
        linearizable_calls: usize = 0,
        free_calls: usize = 0,

        const tables = [_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs" }};
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .table_id = 7, .group_id = 7001, .range_id = 71, .start_key = "", .end_key = "m" },
            .{ .table_id = 7, .group_id = 7002, .range_id = 72, .start_key = "m" },
        };

        fn source(self: *@This()) CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = eventualSnapshot,
                .table_routing_snapshot = tableSnapshot,
                .linearizable_routing_snapshot = linearizableSnapshot,
                .linearizable_table_routing_snapshot = linearizableTableSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForWriteRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn eventualSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.eventual_calls += 1;
            return .{
                .metadata_group_id = 3,
                .catalog_revision = 18,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn tableSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.point_calls += 1;
            return eventualSnapshot(ptr, deadline_ns);
        }

        fn linearizableSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.linearizable_calls += 1;
            return .{
                .catalog_revision = 19,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn linearizableTableSnapshot(ptr: *anyopaque, _: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return linearizableSnapshot(ptr, deadline_ns);
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.free_calls += 1;
        }
    };

    var state = State{};
    {
        var routing = (try authoritativeTableRoutingSnapshot(
            std.testing.allocator,
            state.source(),
            "docs",
            null,
        )).?;
        defer routing.deinit(std.testing.allocator);

        const left = routing.resolveRouteForKey("a").?;
        const right = routing.resolveRouteForKey("z").?;
        try std.testing.expectEqual(@as(u64, 7001), left.group_id);
        try std.testing.expectEqual(@as(u64, 71), left.identity_namespace.range_id);
        try std.testing.expectEqual(@as(u64, 7002), right.group_id);
        try std.testing.expectEqual(@as(u64, 72), right.identity_namespace.range_id);
    }
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 1), state.free_calls);
    {
        var routing = (try tableRoutingSnapshotForWrite(
            std.testing.allocator,
            state.source(),
            "docs",
            null,
        )).?;
        defer routing.deinit(std.testing.allocator);
        const route = routing.resolveRouteForKey("a").?;
        const fence = routing.fenceForRoute(route);
        try std.testing.expectEqual(@as(u64, 3), fence.metadata_group_id);
        try std.testing.expectEqual(@as(u64, 18), fence.catalog_revision);
        try std.testing.expectEqual(@as(u64, 7001), fence.route.group_id);
    }
    try std.testing.expectEqual(@as(usize, 1), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 1), state.point_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 2), state.free_calls);
}

pub fn transactionTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return 0;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);
    return try topologyEpochFromSnapshot(alloc, &snapshot, table.*);
}

pub fn validateTransactionTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);
    if (expected_epoch != 0 and (try topologyEpochFromSnapshot(alloc, &snapshot, table.*)) != expected_epoch) return error.TopologyChanged;
}

pub fn validateTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    return try validateTopologyEpochUntil(alloc, catalog, table_name, expected_epoch, null);
}

pub fn validateTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    if (expected_epoch == 0) return;
    const actual_epoch = try topologyEpochForValidationUntil(alloc, catalog, table_name, deadline_ns);
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

/// Validate an internally captured topology epoch, including the zero epoch
/// used while a table is absent. Public graph requests use zero to mean
/// "unstamped", so `validateTopologyEpoch` intentionally skips it; read
/// admission needs an exact comparison to notice a table created after an
/// absent-table snapshot was routed.
pub fn validatePinnedTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    return try validatePinnedTopologyEpochUntil(alloc, catalog, table_name, expected_epoch, null);
}

pub fn validatePinnedTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    const actual_epoch = try topologyEpochForValidationUntil(alloc, catalog, table_name, deadline_ns);
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

/// Capture the current table topology epoch only if `group_id` is one of its
/// published ranges. Internal group-local reads use this before a Raft wait so
/// an obsolete group fails fast instead of waiting on a replica that has
/// already left the table.
pub fn groupTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !u64 {
    return try groupTopologyEpochUntil(alloc, catalog, table_name, group_id, null);
}

pub fn groupTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    deadline_ns: ?u64,
) !u64 {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .group = group_id }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => error.TopologyChanged,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn validatePinnedGroupTopology(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    expected_epoch: u64,
) !void {
    return try validatePinnedGroupTopologyUntil(alloc, catalog, table_name, group_id, expected_epoch, null);
}

pub fn validatePinnedGroupTopologyUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    var result = try resolveCatalogRouteForValidation(alloc, catalog, table_name, .{ .group = group_id }, deadline_ns);
    defer result.deinit(alloc);
    const actual_epoch = switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => return error.TopologyChanged,
        .timed_out => return error.CatalogRoutingSnapshotTimeout,
    };
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

/// Transactions may not straddle a split or merge. The transition record is
/// published before range cutover, so checking it in addition to the range
/// epoch closes the prepare-to-cutover window where durable intents could
/// otherwise be left on the previous owner.
pub fn validateTransactionTopologyStable(
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    return validateTransactionTopologyStableSnapshot(&snapshot, table.*);
}

fn validateTransactionTopologyStableSnapshot(
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
) !void {
    for (snapshot.split_transitions) |transition| {
        if (!transitionPhaseActive(transition.phase)) continue;
        if (transition.table_contract.table_id == table.table_id or
            std.mem.eql(u8, transition.table_contract.table_name, table.name))
        {
            return error.TopologyChanged;
        }
        if (transition.table_contract.table_id == 0 and
            (rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.source_group_id) or
                rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.destination_group_id)))
        {
            return error.TopologyChanged;
        }
    }
    for (snapshot.merge_transitions) |transition| {
        if (!transitionPhaseActive(transition.phase)) continue;
        if (transition.table_contract.table_id == table.table_id or
            std.mem.eql(u8, transition.table_contract.table_name, table.name))
        {
            return error.TopologyChanged;
        }
        if (transition.table_contract.table_id == 0 and
            (rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.donor_group_id) or
                rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.receiver_group_id)))
        {
            return error.TopologyChanged;
        }
    }
}

fn transitionPhaseActive(phase: metadata_transition_state.TransitionPhase) bool {
    return phase != .finalized and phase != .rolled_back;
}

fn rangeGroupBelongsToTable(ranges: []const metadata_table_manager.RangeRecord, table_id: u64, group_id: u64) bool {
    for (ranges) |range| if (range.table_id == table_id and range.group_id == group_id) return true;
    return false;
}

pub fn validateDocIdentityReadyForTable(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    return validateDocIdentityReadyForTableMode(alloc, catalog, table_name, false);
}

pub fn validateDocIdentityReadyForTableStrict(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    return validateDocIdentityReadyForTableMode(alloc, catalog, table_name, true);
}

pub fn validateResolvedDocFilterContextForGroups(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_ids: []const u64,
    namespace_table_id: u64,
    namespace_shard_id: u64,
    namespace_range_id: u64,
) !void {
    _ = alloc;
    if (group_ids.len == 0) return;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    for (group_ids) |group_id| {
        const range = findRangeForTableGroup(snapshot.ranges, table.table_id, group_id) orelse return error.DocIdentityNamespaceMismatch;
        const status = findMergedGroupStatus(snapshot.merged_group_statuses, group_id) orelse return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
        if (!runtimeDocIdentityCanAcceptNamespace(status.doc_identity, range, namespace_table_id, namespace_shard_id, namespace_range_id)) {
            return error.DocIdentityNamespaceMismatch;
        }
    }
}

fn validateDocIdentityReadyForTableMode(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    require_runtime_status: bool,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    for (ranges) |range| {
        const status = findMergedGroupStatus(snapshot.merged_group_statuses, range.group_id) orelse {
            if (require_runtime_status) return error.DocIdentityNamespaceMismatch;
            continue;
        };
        if (status.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
        if (!runtimeDocIdentityMatchesRange(status.doc_identity, range.*)) return error.DocIdentityNamespaceMismatch;
    }
}

pub fn resolveGroupForKeyPinned(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_epoch: u64,
) !?u64 {
    return try resolveGroupForKeyPinnedUntil(alloc, catalog, table_name, key, expected_epoch, null);
}

pub fn resolveGroupForKeyPinnedUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| blk: {
            if (expected_epoch != 0 and plan.topology_epoch != expected_epoch) return error.TopologyChanged;
            break :blk plan.groups[0].group_id;
        },
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

fn findTableByName(
    tables: []const metadata_table_manager.TableRecord,
    table_name: []const u8,
) ?*const metadata_table_manager.TableRecord {
    for (tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findTableByNameWithBudget(
    tables: []const metadata_table_manager.TableRecord,
    table_name: []const u8,
    budget: RoutingBudget,
) !?*const metadata_table_manager.TableRecord {
    try budget.checkpoint();
    for (tables, 0..) |*table, index| {
        try budget.checkpointIndex(index);
        if (std.mem.eql(u8, table.name, table_name)) {
            try budget.checkpoint();
            return table;
        }
    }
    try budget.checkpoint();
    return null;
}

fn findTableById(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?*const metadata_table_manager.TableRecord {
    for (tables) |*table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn listTableRanges(
    alloc: std.mem.Allocator,
    catalog_ranges: []const metadata_table_manager.RangeRecord,
    table_id: u64,
) ![]const *const metadata_table_manager.RangeRecord {
    return try listTableRangesWithBudget(alloc, catalog_ranges, table_id, .{});
}

fn listTableRangesWithBudget(
    alloc: std.mem.Allocator,
    catalog_ranges: []const metadata_table_manager.RangeRecord,
    table_id: u64,
    budget: RoutingBudget,
) ![]const *const metadata_table_manager.RangeRecord {
    try budget.checkpoint();
    var count: usize = 0;
    for (catalog_ranges, 0..) |range, range_index| {
        try budget.checkpointIndex(range_index);
        if (range.table_id == table_id) count += 1;
    }
    try budget.checkpoint();
    const ranges = try alloc.alloc(*const metadata_table_manager.RangeRecord, count);
    errdefer alloc.free(ranges);
    var index: usize = 0;
    for (catalog_ranges, 0..) |*range, range_index| {
        try budget.checkpointIndex(range_index);
        if (range.table_id != table_id) continue;
        ranges[index] = range;
        index += 1;
    }
    try budget.checkpoint();
    return ranges;
}

pub const TableGroupDescriptorProjection = struct {
    table_id: u64,
    doc_identity_shard_id: u64,
    doc_identity_range_id: u64,
    schema_json: []u8,
    indexes_json: []u8,

    pub fn deinit(self: *TableGroupDescriptorProjection, alloc: std.mem.Allocator) void {
        alloc.free(self.schema_json);
        alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

/// Resolve the catalog-owned physical contract for one table group from the
/// atomically paired table/range projection. Storage owner lifecycle must not
/// depend on the much larger administrative status snapshot, whose unrelated
/// runtime projections may legitimately be busy during foreground writes.
pub fn tableGroupDescriptorProjection(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    deadline_ns: ?u64,
) !?TableGroupDescriptorProjection {
    // Catalog-wide routing intentionally strips schema and index payloads.
    // A first-party point projection is bounded to one table and therefore
    // carries the complete physical definition needed by the storage owner.
    // Sources without that capability (including rolling-upgrade peers and
    // simple fixtures) fall through to the coherent admin projection below;
    // never manufacture an owner descriptor from compact routing fields.
    if (catalog.vtable.table_routing_snapshot) |capture| {
        if (catalog.vtable.free_routing_snapshot == unsupportedFreeRoutingSnapshot)
            return error.CatalogRoutingUnavailable;
        var snapshot = try capture(catalog.ptr, table_name, deadline_ns);
        defer catalog.vtable.free_routing_snapshot(catalog.ptr, &snapshot);
        if (try descriptorProjectionFromRoutingSnapshot(alloc, snapshot, table_name, group_id)) |projection|
            return projection;
    }
    // A positive route may already be known while the eventual metadata
    // replica serving this descriptor request is still applying it. Confirm a
    // point miss through the read-index capability before consulting broader
    // lifecycle state; this keeps owner creation free of cache-timing races.
    if (catalog.vtable.linearizable_table_routing_snapshot) |capture| {
        if (catalog.vtable.free_routing_snapshot == unsupportedFreeRoutingSnapshot)
            return error.CatalogRoutingUnavailable;
        var snapshot = try capture(catalog.ptr, table_name, deadline_ns);
        defer catalog.vtable.free_routing_snapshot(catalog.ptr, &snapshot);
        if (try descriptorProjectionFromRoutingSnapshot(alloc, snapshot, table_name, group_id)) |projection|
            return projection;
    }

    // A split destination does not become an active routing range until
    // cutover, but its immutable descriptor is already captured in the
    // replicated transition contract. Consult the full lifecycle projection
    // only on this compact-routing miss; ordinary owner opens stay independent
    // of the much larger administrative/runtime status snapshot.
    var admin = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&admin);
    if (findTableByName(admin.tables, table_name)) |table| {
        for (admin.ranges) |range| {
            if (range.table_id != table.table_id or range.group_id != group_id) continue;
            return try descriptorProjectionFromValues(
                alloc,
                table.table_id,
                metadata_table_manager.rangeDocIdentityShardId(range),
                metadata_table_manager.rangeDocIdentityRangeId(range),
                table.schema_json,
                table.indexes_json,
            );
        }
    }
    for (admin.split_transitions) |transition| {
        if ((transition.source_group_id != group_id and transition.destination_group_id != group_id) or
            !std.mem.eql(u8, transition.table_contract.table_name, table_name)) continue;
        try transition.table_contract.validateForSplit();
        const identity = if (transition.destination_group_id == group_id)
            transition.table_contract.target_identity
        else
            transition.table_contract.source_identity;
        return try descriptorProjectionFromValues(
            alloc,
            transition.table_contract.table_id,
            identity.shard_id,
            identity.range_id,
            transition.table_contract.schema_json,
            transition.table_contract.indexes_json,
        );
    }
    for (admin.merge_transitions) |transition| {
        if ((transition.donor_group_id != group_id and transition.receiver_group_id != group_id) or
            !std.mem.eql(u8, transition.table_contract.table_name, table_name)) continue;
        try transition.table_contract.validateForMerge(transition.allow_doc_identity_reassignment);
        const identity = if (transition.receiver_group_id == group_id)
            transition.table_contract.target_identity
        else
            transition.table_contract.source_identity;
        return try descriptorProjectionFromValues(
            alloc,
            transition.table_contract.table_id,
            identity.shard_id,
            identity.range_id,
            transition.table_contract.schema_json,
            transition.table_contract.indexes_json,
        );
    }
    return null;
}

fn descriptorProjectionFromRoutingSnapshot(
    alloc: std.mem.Allocator,
    snapshot: metadata_api.CatalogRoutingSnapshot,
    table_name: []const u8,
    group_id: u64,
) !?TableGroupDescriptorProjection {
    const table = findTableByName(snapshot.tables, table_name) orelse return null;
    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id or range.group_id != group_id) continue;
        return try descriptorProjectionFromValues(
            alloc,
            table.table_id,
            metadata_table_manager.rangeDocIdentityShardId(range),
            metadata_table_manager.rangeDocIdentityRangeId(range),
            table.schema_json,
            table.indexes_json,
        );
    }
    return null;
}

fn descriptorProjectionFromValues(
    alloc: std.mem.Allocator,
    table_id: u64,
    doc_identity_shard_id: u64,
    doc_identity_range_id: u64,
    schema_json: []const u8,
    indexes_json: []const u8,
) !TableGroupDescriptorProjection {
    const owned_schema_json = try alloc.dupe(u8, schema_json);
    errdefer alloc.free(owned_schema_json);
    return .{
        .table_id = table_id,
        .doc_identity_shard_id = doc_identity_shard_id,
        .doc_identity_range_id = doc_identity_range_id,
        .schema_json = owned_schema_json,
        .indexes_json = try alloc.dupe(u8, indexes_json),
    };
}

pub fn resolveGroupsForSpan(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) ![]u64 {
    return try resolveGroupsForSpanUntil(alloc, catalog, table_name, from_key, to_key, null);
}

pub fn resolveGroupsForSpanUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) ![]u64 {
    const budget = RoutingBudget.init(deadline_ns);
    return switch (try resolveGroupsForSpanWithDeadline(alloc, try catalog.routingSource(), table_name, from_key, to_key, deadline_ns)) {
        .found => |plan_value| blk: {
            var plan = plan_value;
            defer plan.deinit(alloc);
            break :blk try plan.groupIdsAllocUntil(alloc, budget);
        },
        .not_found => try cloneGroupIdsUntil(alloc, &.{}, budget),
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupsForSpanPinnedUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) ![]u64 {
    const budget = RoutingBudget.init(deadline_ns);
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| blk: {
            if (expected_epoch != 0 and plan.topology_epoch != expected_epoch) return error.TopologyChanged;
            break :blk try plan.groupIdsAllocUntil(alloc, budget);
        },
        .not_found => try cloneGroupIdsUntil(alloc, &.{}, budget),
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub const CatalogIdentityNamespace = metadata_api.CatalogIdentityNamespace;
pub const CatalogGroupRoute = metadata_api.CatalogGroupRoute;

/// A complete routing decision derived from one immutable catalog projection.
/// Group selection and database identity must never be looked up separately.
pub const CatalogRoutePlan = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    catalog_revision: u64,
    table_id: u64,
    topology_epoch: u64,
    groups: []CatalogGroupRoute,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn groupIdsAlloc(self: @This(), alloc: std.mem.Allocator) ![]u64 {
        return try self.groupIdsAllocUntil(alloc, .{});
    }

    pub fn groupIdsAllocUntil(
        self: @This(),
        alloc: std.mem.Allocator,
        budget: RoutingBudget,
    ) ![]u64 {
        try budget.checkpoint();
        const ids = try alloc.alloc(u64, self.groups.len);
        errdefer alloc.free(ids);
        for (self.groups, ids, 0..) |route, *id, index| {
            try budget.checkpointIndex(index);
            id.* = route.group_id;
        }
        try budget.checkpoint();
        return ids;
    }

    /// Preserve caller order while avoiding quadratic scans for large
    /// fanouts. Tiny selections stay allocation-free beyond the result.
    pub fn selectGroupsUntil(
        self: @This(),
        alloc: std.mem.Allocator,
        group_ids: []const u64,
        budget: RoutingBudget,
    ) ![]CatalogGroupRoute {
        const linear_lookup_limit = 8;
        try budget.checkpoint();
        const selected = try alloc.alloc(CatalogGroupRoute, group_ids.len);
        errdefer alloc.free(selected);
        if (group_ids.len <= linear_lookup_limit) {
            for (group_ids, selected, 0..) |group_id, *route, requested_index| {
                try budget.checkpointIndex(requested_index);
                var found: ?CatalogGroupRoute = null;
                for (self.groups, 0..) |candidate, route_index| {
                    try budget.checkpointIndex(route_index);
                    if (candidate.group_id == group_id) {
                        found = candidate;
                        break;
                    }
                }
                route.* = found orelse return error.TopologyChanged;
            }
        } else {
            var group_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty;
            defer group_indexes.deinit(alloc);
            try group_indexes.ensureTotalCapacity(alloc, @intCast(self.groups.len));
            for (self.groups, 0..) |route, index| {
                try budget.checkpointIndex(index);
                if (group_indexes.contains(route.group_id)) return error.InvalidCatalogProjection;
                group_indexes.putAssumeCapacity(route.group_id, index);
            }
            for (group_ids, selected, 0..) |group_id, *route, index| {
                try budget.checkpointIndex(index);
                const route_index = group_indexes.get(group_id) orelse return error.TopologyChanged;
                route.* = self.groups[route_index];
            }
        }
        try budget.checkpoint();
        return selected;
    }

    pub fn group(self: @This(), group_id: u64) ?CatalogGroupRoute {
        for (self.groups) |route| if (route.group_id == group_id) return route;
        return null;
    }
};

pub const RouteQuery = union(enum) {
    /// Table existence only; a table with no published ranges is still found.
    table,
    /// Select every published range and require at least one.
    all_ranges,
    key: []const u8,
    span: struct {
        from_key: []const u8,
        to_key: []const u8,
    },
    group: u64,
};

pub const RouteResult = union(enum) {
    found: CatalogRoutePlan,
    not_found,
    timed_out,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.* == .found) self.found.deinit(alloc);
        self.* = undefined;
    }
};

pub const ResolveGroupsResult = RouteResult;

/// Result of waiting for a route to be published. A stable authoritative miss
/// at the caller's deadline is distinct from being unable to capture a
/// projection before that deadline.
pub const AwaitRouteResult = union(enum) {
    found: CatalogRoutePlan,
    publication_not_observed,
    timed_out,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.* == .found) self.found.deinit(alloc);
        self.* = undefined;
    }
};

fn resolveCatalogRoute(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !RouteResult {
    if (catalog.vtable.resolve_route) |resolve|
        return try resolve(catalog.ptr, alloc, table_name, query, deadline_ns);
    return try resolveRoute(
        alloc,
        try catalog.routingSource(),
        table_name,
        query,
        deadline_ns,
    );
}

fn resolveCatalogRouteForValidation(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !RouteResult {
    if (catalog.vtable.validate_route) |resolve|
        return try resolve(catalog.ptr, alloc, table_name, query, deadline_ns);
    return try resolveCatalogRoute(alloc, catalog, table_name, query, deadline_ns);
}

/// Resolve one routing predicate from a compact projection. Eventual
/// projections are sufficient for positive routes. Any miss is evaluated
/// again after a compact linearizable barrier before it becomes authoritative.
pub fn resolveRoute(
    alloc: std.mem.Allocator,
    routing: CatalogRoutingSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !RouteResult {
    const observed = try resolveRouteObserved(alloc, routing, table_name, query, deadline_ns);
    return switch (observed) {
        .found => |plan| .{ .found = plan },
        .not_found => .not_found,
        .timed_out => .timed_out,
    };
}

const ObservedRouteResult = union(enum) {
    found: CatalogRoutePlan,
    not_found: metadata_api.CatalogRoutingChangeToken,
    timed_out,
};

fn resolveRouteObserved(
    alloc: std.mem.Allocator,
    routing: CatalogRoutingSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !ObservedRouteResult {
    var eventual = routing.eventualSnapshot(deadline_ns) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    defer eventual.deinit();
    const budget = RoutingBudget.init(deadline_ns);
    const eventual_plan = routePlanFromSnapshotWithBudget(alloc, eventual.value, table_name, query, budget) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    if (eventual_plan) |plan| {
        return .{ .found = plan };
    }

    var authoritative = routing.linearizableSnapshot(deadline_ns) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    defer authoritative.deinit();
    const authoritative_plan = routePlanFromSnapshotWithBudget(alloc, authoritative.value, table_name, query, budget) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    if (authoritative_plan) |plan| {
        return .{ .found = plan };
    }
    return .{ .not_found = authoritative.value.change_token };
}

/// Wait until a route is published or the absolute deadline is reached. The
/// authoritative snapshot supplies the change token, so publication between
/// that snapshot and the wait cannot be missed.
pub fn awaitRoute(
    alloc: std.mem.Allocator,
    routing: CatalogRoutingSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: u64,
    probe_interval_ns: u64,
) !AwaitRouteResult {
    if (routing.authority.await_route) |await_route| {
        return try await_route(
            routing.authority.ptr,
            alloc,
            table_name,
            query,
            deadline_ns,
            probe_interval_ns,
        );
    }
    while (true) {
        if (platform_time.monotonicNs() >= deadline_ns) return .timed_out;
        var resolved = try resolveRouteObserved(alloc, routing, table_name, query, deadline_ns);
        switch (resolved) {
            .found => |plan| {
                resolved = undefined;
                return .{ .found = plan };
            },
            .timed_out => return .timed_out,
            .not_found => |observed_token| {
                switch (try routing.waitForChange(observed_token, deadline_ns, probe_interval_ns)) {
                    .changed => continue,
                    .retry => continue,
                    .authoritative_absence => return .publication_not_observed,
                }
            },
        }
    }
}

fn resolveGroupsForSpanWithDeadline(
    alloc: std.mem.Allocator,
    routing: CatalogRoutingSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) !ResolveGroupsResult {
    return try resolveRoute(alloc, routing, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
}

pub fn routePlanFromSnapshot(
    alloc: std.mem.Allocator,
    snapshot: metadata_api.CatalogRoutingSnapshot,
    table_name: []const u8,
    query: RouteQuery,
) !?CatalogRoutePlan {
    return try routePlanFromSnapshotWithBudget(alloc, snapshot, table_name, query, .{});
}

pub fn routePlanFromSnapshotUntil(
    alloc: std.mem.Allocator,
    snapshot: metadata_api.CatalogRoutingSnapshot,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !?CatalogRoutePlan {
    return try routePlanFromSnapshotWithBudget(
        alloc,
        snapshot,
        table_name,
        query,
        RoutingBudget.init(deadline_ns),
    );
}

fn routePlanFromSnapshotWithBudget(
    alloc: std.mem.Allocator,
    snapshot: metadata_api.CatalogRoutingSnapshot,
    table_name: []const u8,
    query: RouteQuery,
    budget: RoutingBudget,
) !?CatalogRoutePlan {
    try budget.checkpoint();
    const table = (try findTableByNameWithBudget(snapshot.tables, table_name, budget)) orelse return null;
    const ranges = try listTableRangesWithBudget(alloc, snapshot.ranges, table.table_id, budget);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    try budget.checkpoint();
    var groups = std.ArrayListUnmanaged(CatalogGroupRoute).empty;
    defer groups.deinit(alloc);
    for (ranges, 0..) |range, index| {
        try budget.checkpointIndex(index);
        const selected = switch (query) {
            .table => false,
            .all_ranges => true,
            .key => |key| rangeContainsKey(range.*, key),
            .span => |span| rangeOverlapsSpan(range.*, span.from_key, span.to_key),
            .group => |group_id| range.group_id == group_id,
        };
        if (!selected) continue;
        const range_id = metadata_table_manager.rangeDocIdentityRangeId(range.*);
        try groups.append(alloc, .{
            .group_id = range.group_id,
            .range_id = range_id,
            .identity_namespace = .{
                .table_id = table.table_id,
                .shard_id = metadata_table_manager.rangeDocIdentityShardId(range.*),
                .range_id = range_id,
            },
        });
    }
    const requires_route = switch (query) {
        .table => false,
        else => true,
    };
    if (groups.items.len == 0 and requires_route) {
        try budget.checkpoint();
        return null;
    }
    const topology_epoch = try topologyEpochFromSortedRangesWithBudget(table.*, ranges, budget);
    const owned_groups = try groups.toOwnedSlice(alloc);
    errdefer alloc.free(owned_groups);
    try budget.checkpoint();
    return .{
        .metadata_group_id = snapshot.metadata_group_id,
        .metadata_incarnation = snapshot.metadata_incarnation,
        .catalog_revision = snapshot.catalog_revision,
        .table_id = table.table_id,
        .topology_epoch = topology_epoch,
        .groups = owned_groups,
    };
}

test "route planning never succeeds after its absolute deadline" {
    var tables = [_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs" }};
    var ranges = [_]metadata_table_manager.RangeRecord{.{
        .table_id = 7,
        .group_id = 7001,
        .range_id = 71,
        .start_key = "",
    }};
    const snapshot = metadata_api.CatalogRoutingSnapshot{
        .metadata_group_id = 3,
        .catalog_revision = 18,
        .tables = tables[0..],
        .ranges = ranges[0..],
    };
    try std.testing.expectError(
        error.CatalogRoutingSnapshotTimeout,
        routePlanFromSnapshotUntil(
            std.testing.allocator,
            snapshot,
            "docs",
            .all_ranges,
            1,
        ),
    );
}

test "route projection preserves order and remains bounded after capture" {
    var groups: [12]CatalogGroupRoute = undefined;
    for (&groups, 0..) |*group, index| {
        const group_id: u64 = @intCast(index + 1);
        group.* = .{
            .group_id = group_id,
            .range_id = group_id + 100,
            .identity_namespace = .{
                .table_id = 7,
                .shard_id = group_id,
                .range_id = group_id + 100,
            },
        };
    }
    const plan = CatalogRoutePlan{
        .metadata_group_id = 3,
        .metadata_incarnation = null,
        .catalog_revision = 18,
        .table_id = 7,
        .topology_epoch = 19,
        .groups = groups[0..],
    };
    const requested = [_]u64{ 12, 1, 7, 2, 9, 4, 6, 3, 12 };
    const budget = RoutingBudget.init(platform_time.monotonicNs() + std.time.ns_per_s);
    const selected = try plan.selectGroupsUntil(std.testing.allocator, &requested, budget);
    defer std.testing.allocator.free(selected);
    try std.testing.expectEqual(requested.len, selected.len);
    for (requested, selected) |group_id, route| {
        try std.testing.expectEqual(group_id, route.group_id);
    }

    try std.testing.expectError(
        error.CatalogRoutingSnapshotTimeout,
        plan.groupIdsAllocUntil(std.testing.allocator, RoutingBudget.init(1)),
    );
    try std.testing.expectError(
        error.CatalogRoutingSnapshotTimeout,
        RoutedSpanSnapshot.fromPlanUntil(std.testing.allocator, plan, RoutingBudget.init(1)),
    );
}

test "pinned fanout rejects mismatched fence identity" {
    const Source = struct {
        mode: enum { valid, wrong_group, wrong_identity, unbound_table },

        fn catalog(self: *@This()) CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .route_identity = routeIdentity,
                .route_fence = routeFence,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routeIdentity(
            ptr: *anyopaque,
            table_name: []const u8,
            group_id: u64,
        ) !?metadata_api.CatalogIdentityNamespace {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.mode == .unbound_table) return error.RouteIdentityNotPinned;
            if (!std.mem.eql(u8, table_name, "docs") or group_id != 7001) return null;
            return .{
                .table_id = 7,
                .shard_id = 7001,
                .range_id = if (self.mode == .wrong_identity) 72 else 71,
            };
        }

        fn routeFence(ptr: *anyopaque, _: u64) !?metadata_api.CatalogRouteFence {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .metadata_group_id = 3,
                .catalog_revision = 18,
                .table_id = 7,
                .topology_epoch = 19,
                .route = .{
                    .group_id = if (self.mode == .wrong_group) 7002 else 7001,
                    .range_id = 71,
                    .identity_namespace = .{
                        .table_id = 7,
                        .shard_id = 7001,
                        .range_id = 71,
                    },
                },
            };
        }
    };

    var wrong_group = Source{ .mode = .wrong_group };
    try std.testing.expectError(
        error.TopologyChanged,
        routedGroupsSnapshotUntil(
            std.testing.allocator,
            wrong_group.catalog(),
            "docs",
            &.{7001},
            platform_time.monotonicNs() + std.time.ns_per_s,
        ),
    );

    var wrong_identity = Source{ .mode = .wrong_identity };
    try std.testing.expectError(
        error.TopologyChanged,
        routedGroupsSnapshotUntil(
            std.testing.allocator,
            wrong_identity.catalog(),
            "docs",
            &.{7001},
            platform_time.monotonicNs() + std.time.ns_per_s,
        ),
    );

    var unbound_table = Source{ .mode = .unbound_table };
    try std.testing.expectError(
        error.TopologyChanged,
        routedGroupsSnapshotUntil(
            std.testing.allocator,
            unbound_table.catalog(),
            "docs",
            &.{7001},
            platform_time.monotonicNs() + std.time.ns_per_s,
        ),
    );

    var valid = Source{ .mode = .valid };
    var snapshot = try routedGroupsSnapshotUntil(
        std.testing.allocator,
        valid.catalog(),
        "docs",
        &.{7001},
        platform_time.monotonicNs() + std.time.ns_per_s,
    );
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 7001), snapshot.group_ids[0]);
    try std.testing.expectEqual(@as(u64, 7001), snapshot.routes[0].group_id);
    try std.testing.expect(std.meta.eql(snapshot.routes[0].identity_namespace, .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 71,
    }));
}

pub const RoutedSpanSnapshot = struct {
    routes: []CatalogGroupRoute,
    group_ids: []u64,
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    table_id: u64,
    catalog_revision: u64,
    topology_epoch: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.routes);
        alloc.free(self.group_ids);
        self.* = undefined;
    }

    pub fn fenceAt(self: @This(), index: usize) metadata_api.CatalogRouteFence {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .catalog_revision = self.catalog_revision,
            .table_id = self.table_id,
            .topology_epoch = self.topology_epoch,
            .route = self.routes[index],
        };
    }

    fn fromPlanUntil(
        alloc: std.mem.Allocator,
        plan: CatalogRoutePlan,
        budget: RoutingBudget,
    ) !RoutedSpanSnapshot {
        try budget.checkpoint();
        const routes = try alloc.alloc(CatalogGroupRoute, plan.groups.len);
        errdefer alloc.free(routes);
        const group_ids = try alloc.alloc(u64, plan.groups.len);
        errdefer alloc.free(group_ids);
        for (plan.groups, routes, group_ids, 0..) |route, *owned_route, *group_id, index| {
            try budget.checkpointIndex(index);
            owned_route.* = route;
            group_id.* = route.group_id;
        }
        try budget.checkpoint();
        return .{
            .routes = routes,
            .group_ids = group_ids,
            .metadata_group_id = plan.metadata_group_id,
            .metadata_incarnation = plan.metadata_incarnation,
            .table_id = plan.table_id,
            .catalog_revision = plan.catalog_revision,
            .topology_epoch = plan.topology_epoch,
        };
    }
};

pub fn validateCatalogRouteFenceUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    fence: metadata_api.CatalogRouteFence,
    deadline_ns: ?u64,
) !void {
    try fence.validate();
    var result = try resolveCatalogRouteForValidation(alloc, catalog, table_name, .{ .group = fence.route.group_id }, deadline_ns);
    defer result.deinit(alloc);
    const plan = switch (result) {
        .found => |value| value,
        .not_found => return error.TopologyChanged,
        .timed_out => return error.CatalogRoutingSnapshotTimeout,
    };
    if (plan.metadata_group_id != fence.metadata_group_id or
        !std.meta.eql(plan.metadata_incarnation, fence.metadata_incarnation) or
        plan.table_id != fence.table_id or
        plan.topology_epoch != fence.topology_epoch or
        plan.groups.len != 1 or
        !std.meta.eql(plan.groups[0], fence.route))
    {
        return error.TopologyChanged;
    }
}

/// Resolve a span and compute the routing epoch from one catalog snapshot.
pub fn routedSpanSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) !RoutedSpanSnapshot {
    return try routedSpanSnapshotUntil(alloc, catalog, table_name, from_key, to_key, null);
}

pub fn routedSpanSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) !RoutedSpanSnapshot {
    const budget = RoutingBudget.init(deadline_ns);
    var result = try resolveCatalogRoute(alloc, catalog, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| try RoutedSpanSnapshot.fromPlanUntil(alloc, plan, budget),
        .not_found => blk: {
            try budget.checkpoint();
            const routes = try alloc.alloc(CatalogGroupRoute, 0);
            errdefer alloc.free(routes);
            const group_ids = try alloc.alloc(u64, 0);
            errdefer alloc.free(group_ids);
            try budget.checkpoint();
            break :blk .{
                .routes = routes,
                .group_ids = group_ids,
                .metadata_group_id = 0,
                .metadata_incarnation = null,
                .table_id = 0,
                .catalog_revision = 0,
                .topology_epoch = 0,
            };
        },
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupsForSpanEventually(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    timeout_ns: u64,
    poll_interval_ms: u64,
) !ResolveGroupsResult {
    const deadline_ns = platform_time.monotonicNs() +| timeout_ns;
    return try resolveGroupsForSpanEventuallyUntil(
        alloc,
        catalog,
        table_name,
        from_key,
        to_key,
        deadline_ns,
        poll_interval_ms,
    );
}

pub fn resolveGroupsForSpanEventuallyUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: u64,
    poll_interval_ms: u64,
) !ResolveGroupsResult {
    const routing = try catalog.routingSource();
    var result = try awaitRoute(
        alloc,
        routing,
        table_name,
        .{ .span = .{
            .from_key = from_key,
            .to_key = to_key,
        } },
        deadline_ns,
        poll_interval_ms *| std.time.ns_per_ms,
    );
    return switch (result) {
        .found => |plan| blk: {
            result = undefined;
            break :blk .{ .found = plan };
        },
        .publication_not_observed => .not_found,
        .timed_out => .timed_out,
    };
}

fn metadataServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.adminSnapshot();
}

fn metadataServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    svc.freeAdminSnapshot(snapshot);
}

fn metadataServiceRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataServiceTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataServiceLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataService, svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataServiceLinearizableTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    svc.ensureLinearizableReadWithContext(.{ .deadline_ns = deadline_ns }) catch |err| switch (err) {
        error.DeadlineExceeded,
        error.MetadataLinearizableReadTimeout,
        => return error.CatalogRoutingSnapshotTimeout,
        else => return err,
    };
    return try svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataServiceFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataServiceWaitForRoutingChange(ptr: *anyopaque, observed_token: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, _: u64) !CatalogChangeWaitResult {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    const result = svc.waitForCatalogRoutingChange(observed_token, deadline_ns, true) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .retry,
        else => return err,
    };
    return if (result.effectiveDisposition() == .unchanged) .authoritative_absence else .changed;
}

fn metadataServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.validatePublication(contract);
}

fn metadataServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.validateTablePublication(contract);
}

fn metadataHttpServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.adminSnapshot();
}

fn metadataHttpServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.freeAdminSnapshot(snapshot);
}

fn metadataHttpServiceRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataHttpServiceTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataHttpServiceLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataHttpService, svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataHttpServiceLinearizableTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.ensureLinearizableReadWithContext(.{ .deadline_ns = deadline_ns }) catch |err| switch (err) {
        error.DeadlineExceeded,
        error.MetadataLinearizableReadTimeout,
        => return error.CatalogRoutingSnapshotTimeout,
        else => return err,
    };
    return try svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataHttpServiceFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataHttpServiceWaitForRoutingChange(ptr: *anyopaque, observed_token: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, _: u64) !CatalogChangeWaitResult {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const result = svc.waitForCatalogRoutingChange(observed_token, deadline_ns, true) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .retry,
        else => return err,
    };
    return if (result.effectiveDisposition() == .unchanged) .authoritative_absence else .changed;
}

fn metadataHttpServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.validatePublication(contract);
}

fn metadataHttpServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.validateTablePublication(contract);
}

fn metadataServerAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.adminSnapshot();
}

fn metadataServerFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.freeAdminSnapshot(snapshot);
}

fn metadataServerRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataServerTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataServerLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataHttpService, srv.svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataServerLinearizableTableRoutingSnapshot(ptr: *anyopaque, table_name: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.svc.ensureLinearizableReadWithContext(.{ .deadline_ns = deadline_ns }) catch |err| switch (err) {
        error.DeadlineExceeded,
        error.MetadataLinearizableReadTimeout,
        => return error.CatalogRoutingSnapshotTimeout,
        else => return err,
    };
    return try srv.svc.catalogTableRoutingSnapshot(table_name, deadline_ns);
}

fn metadataServerFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataServerWaitForRoutingChange(ptr: *anyopaque, observed_token: metadata_api.CatalogRoutingChangeToken, deadline_ns: u64, _: u64) !CatalogChangeWaitResult {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    const result = srv.svc.waitForCatalogRoutingChange(observed_token, deadline_ns, true) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .retry,
        else => return err,
    };
    return if (result.effectiveDisposition() == .unchanged) .authoritative_absence else .changed;
}

fn metadataServerValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.validatePublication(contract);
}

fn metadataServerValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.validateTablePublication(contract);
}

fn sortRangeRefs(ranges: []const *const metadata_table_manager.RangeRecord) void {
    std.sort.insertion(*const metadata_table_manager.RangeRecord, @constCast(ranges), {}, struct {
        fn lessThan(_: void, a: *const metadata_table_manager.RangeRecord, b: *const metadata_table_manager.RangeRecord) bool {
            return std.mem.order(u8, a.start_key, b.start_key) == .lt;
        }
    }.lessThan);
}

fn rangeContainsKey(range: metadata_table_manager.RangeRecord, key: []const u8) bool {
    if (range.start_key.len > 0 and std.mem.order(u8, key, range.start_key) == .lt) return false;
    if (range.end_key) |end_key| {
        if (end_key.len > 0 and std.mem.order(u8, key, end_key) != .lt) return false;
    }
    return true;
}

fn rangeOverlapsSpan(range: metadata_table_manager.RangeRecord, from_key: []const u8, to_key: []const u8) bool {
    if (to_key.len > 0 and std.mem.order(u8, range.start_key, to_key) != .lt) return false;
    if (range.end_key) |end_key| {
        if (end_key.len > 0 and from_key.len > 0 and std.mem.order(u8, end_key, from_key) != .gt) return false;
    }
    return true;
}

test "metadata service constructors provide compact routing snapshots" {
    var svc: metadata_service.MetadataService = undefined;
    _ = try CatalogSource.fromMetadataService(&svc).routingSource();

    var http_svc: metadata_service.MetadataHttpService = undefined;
    _ = try CatalogSource.fromMetadataHttpService(&http_svc).routingSource();

    var server: metadata_server.MetadataServer = undefined;
    _ = try CatalogSource.fromMetadataServer(&server).routingSource();
}

test "catalog sources without compact routing fail closed" {
    const source = emptyCatalogSource();
    try std.testing.expectError(error.CatalogRoutingUnavailable, source.routingSource());

    const Partial = struct {
        fn routingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };
    const partial: CatalogSource = .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = emptyAdminSnapshot,
            .free_admin_snapshot = emptyFreeAdminSnapshot,
            .routing_snapshot = Partial.routingSnapshot,
            .free_routing_snapshot = Partial.freeRoutingSnapshot,
        },
    };
    try std.testing.expectError(error.CatalogRoutingUnavailable, partial.routingSource());
}

test "transaction topology fence rejects active split transitions" {
    const Source = struct {
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{.{
                    .transition_id = 9,
                    .attempt_epoch = 1,
                    .source_group_id = 7001,
                    .destination_group_id = 7002,
                    .table_contract = .{ .table_id = 7, .table_name = "docs" },
                }})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    const source: CatalogSource = .{ .ptr = undefined, .vtable = &.{
        .admin_snapshot = Source.snapshot,
        .free_admin_snapshot = Source.free,
    } };
    try std.testing.expectError(error.TopologyChanged, validateTransactionTopologyStable(source, "docs"));
}

fn findMergedGroupStatus(statuses: []const metadata_reconciler.MergedGroupStatus, group_id: u64) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn findRangeForTableGroup(
    ranges: []const metadata_table_manager.RangeRecord,
    table_id: u64,
    group_id: u64,
) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.table_id == table_id and range.group_id == group_id) return range;
    }
    return null;
}

fn runtimeDocIdentityCanAcceptNamespace(
    stats: metadata_table_manager.RuntimeDocIdentityStatusReport,
    range: metadata_table_manager.RangeRecord,
    namespace_table_id: u64,
    namespace_shard_id: u64,
    namespace_range_id: u64,
) bool {
    if (!runtimeDocIdentityHasOrdinalRows(stats)) return false;
    if (!runtimeDocIdentityMatchesRange(stats, range)) return false;
    return stats.namespace_table_id == namespace_table_id and
        stats.namespace_shard_id == namespace_shard_id and
        stats.namespace_range_id == namespace_range_id;
}

fn runtimeDocIdentityMatchesRange(
    stats: metadata_table_manager.RuntimeDocIdentityStatusReport,
    range: metadata_table_manager.RangeRecord,
) bool {
    if (!runtimeDocIdentityHasNamespace(stats)) return true;
    return stats.namespace_table_id == range.table_id and
        stats.namespace_shard_id == metadata_table_manager.rangeDocIdentityShardId(range) and
        stats.namespace_range_id == metadata_table_manager.rangeDocIdentityRangeId(range);
}

fn runtimeDocIdentityHasNamespace(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0;
}

fn runtimeDocIdentityHasOrdinalRows(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0;
}

test "catalog source resolves a single-range table group" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                    .linearizable_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).linearizableSnapshot,
                    .free_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const group_id = (try resolveSingleRangeGroup(std.testing.allocator, FakeCatalog.iface(), "docs")).?;
    try std.testing.expectEqual(@as(u64, 7001), group_id);
}

test "catalog source resolves groups by key and span" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                    .linearizable_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).linearizableSnapshot,
                    .free_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try std.testing.expectEqual(@as(u64, 7001), (try resolveGroupForKey(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:a")).?);
    try std.testing.expectEqual(@as(u64, 7002), (try resolveGroupForKey(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:z")).?);

    const groups = try resolveGroupsForSpan(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:b", "doc:z");
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 7001), groups[0]);
    try std.testing.expectEqual(@as(u64, 7002), groups[1]);
}

test "span routing uses compact catalog snapshot when available" {
    const TestState = struct {
        freed: bool = false,
        routing_calls: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = routingSnapshot,
                    .linearizable_routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.routing_calls += 1;
            return .{
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.freed = true;
        }
    };

    var state = TestState{};
    var found = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "docs",
        "",
        "",
        null,
    );
    defer found.deinit(std.testing.allocator);
    switch (found) {
        .found => |plan| {
            try std.testing.expectEqual(@as(u64, 7), plan.table_id);
            try std.testing.expectEqual(@as(usize, 1), plan.groups.len);
            try std.testing.expectEqual(@as(u64, 7001), plan.groups[0].group_id);
            try std.testing.expectEqual(CatalogIdentityNamespace{
                .table_id = 7,
                .shard_id = 7001,
                .range_id = 7001,
            }, plan.groups[0].identity_namespace);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(state.freed);
    try std.testing.expectEqual(@as(usize, 1), state.routing_calls);

    const not_found = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "missing",
        "",
        "",
        null,
    );
    try std.testing.expectEqual(ResolveGroupsResult.not_found, not_found);
}

test "descriptor projection never sources physical config from compact routing records" {
    const State = struct { admin_calls: usize = 0, routing_calls: usize = 0 };
    const FakeCatalog = struct {
        const full_tables = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = "{\"type\":\"object\"}",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }};
        const compact_tables = [_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs" }};
        const ranges = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .range_id = 71,
            .start_key = "",
            .end_key = null,
            .doc_identity_shard_id = 17,
            .doc_identity_range_id = 71,
        }};

        fn iface(state: *State) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = routingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.routing_calls += 1;
            return .{
                .tables = @constCast(compact_tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.admin_calls += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(full_tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var state: State = .{};
    var projection = (try tableGroupDescriptorProjection(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        "docs",
        7001,
        null,
    )).?;
    defer projection.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), state.admin_calls);
    try std.testing.expectEqual(@as(usize, 0), state.routing_calls);
    try std.testing.expectEqualStrings("{\"type\":\"object\"}", projection.schema_json);
    try std.testing.expectEqualStrings(
        "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        projection.indexes_json,
    );
    try std.testing.expectEqual(@as(u64, 17), projection.doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 71), projection.doc_identity_range_id);
}

test "descriptor projection resolves a staged split destination from its transition contract" {
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .schema_json = "{\"type\":\"object\"}", .indexes_json = "{}", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
        };
        const split_transitions = [_]metadata_transition_state.SplitTransitionRecord{.{
            .transition_id = 99,
            .attempt_epoch = 1,
            .source_group_id = 7001,
            .destination_group_id = 7002,
            .table_contract = .{
                .table_id = 7,
                .table_name = "docs",
                .schema_json = "{\"type\":\"object\"}",
                .indexes_json = "{}",
                .source_identity = .{ .shard_id = 71, .range_id = 72 },
                .target_identity = .{ .shard_id = 71, .range_id = 72 },
            },
        }};

        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = routingSnapshot,
                    .linearizable_routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
                },
            };
        }

        fn routingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return .{
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast(split_transitions[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var projection = (try tableGroupDescriptorProjection(
        std.testing.allocator,
        FakeCatalog.iface(),
        "docs",
        7002,
        null,
    )).?;
    defer projection.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 7), projection.table_id);
    try std.testing.expectEqual(@as(u64, 71), projection.doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 72), projection.doc_identity_range_id);
    try std.testing.expectEqualStrings("{\"type\":\"object\"}", projection.schema_json);
    try std.testing.expectEqualStrings("{}", projection.indexes_json);
}

test "routing session pins every table without consulting admin state" {
    const TestState = struct {
        admin_calls: usize = 0,
        linearizable_calls: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs" },
            .{ .table_id = 8, .name = "authors" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .range_id = 71, .table_id = 7, .start_key = "", .doc_identity_shard_id = 17, .doc_identity_range_id = 71 },
            .{ .group_id = 8001, .range_id = 81, .table_id = 8, .start_key = "", .doc_identity_shard_id = 18, .doc_identity_range_id = 81 },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = linearizableRoutingSnapshot,
                .linearizable_routing_snapshot = linearizableRoutingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.admin_calls += 1;
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn linearizableRoutingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .metadata_group_id = 1,
                .catalog_revision = 9,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }
    };

    var state = TestState{};
    var session = try RoutingSession.init(std.testing.allocator, FakeCatalog.iface(&state), null);
    defer session.deinit();
    const source = session.catalog();
    const identity = (try source.vtable.route_identity.?(source.ptr, "authors", 8001)).?;
    try std.testing.expectEqual(@as(u64, 8), identity.table_id);
    try std.testing.expectEqual(@as(u64, 18), identity.shard_id);
    const fence = (try source.vtable.route_fence.?(source.ptr, 8001)).?;
    try std.testing.expectEqual(@as(u64, 8), fence.table_id);
    try std.testing.expectEqual(@as(u64, 81), fence.route.identity_namespace.range_id);
    try std.testing.expectEqual(@as(usize, 0), state.admin_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
}

test "routing session validates a pinned selection against current topology" {
    const TestState = struct {
        eventual_calls: usize = 0,
        linearizable_calls: usize = 0,
        publish_current: bool = false,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs" }};
        const eventual_ranges = [_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .range_id = 71, .table_id = 7, .start_key = "" }};
        const current_ranges = [_]metadata_table_manager.RangeRecord{.{ .group_id = 7002, .range_id = 72, .table_id = 7, .start_key = "" }};

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = eventualSnapshot,
                .linearizable_routing_snapshot = linearizableSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn eventualSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.eventual_calls += 1;
            return .{
                .metadata_group_id = 1,
                .catalog_revision = if (state.publish_current) 10 else 9,
                .tables = @constCast(tables[0..]),
                .ranges = if (state.publish_current)
                    @constCast(current_ranges[0..])
                else
                    @constCast(eventual_ranges[0..]),
            };
        }

        fn linearizableSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .metadata_group_id = 1,
                .catalog_revision = 10,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(current_ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };

    var state = TestState{};
    var session = try RoutingSession.initForRoute(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        "docs",
        .all_ranges,
        null,
    );
    defer session.deinit();
    const source = session.catalog();
    var pinned = try routedSpanSnapshot(std.testing.allocator, source, "docs", "", "");
    defer pinned.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u64, &.{7001}, pinned.group_ids);
    try std.testing.expect((try source.vtable.route_identity.?(source.ptr, "docs", 7002)) == null);
    try std.testing.expect((try source.vtable.route_fence.?(source.ptr, 7002)) == null);
    try std.testing.expectError(
        error.CatalogProjectionRefreshRequired,
        resolveCatalogRoute(std.testing.allocator, source, "docs", .{ .group = 7002 }, null),
    );
    try std.testing.expectEqual(@as(usize, 0), state.linearizable_calls);
    state.publish_current = true;
    try std.testing.expectError(
        error.TopologyChanged,
        validatePinnedTopologyEpoch(std.testing.allocator, source, "docs", pinned.topology_epoch),
    );
    try std.testing.expectEqual(@as(usize, 2), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 0), state.linearizable_calls);
}

test "span routing confirms eventual misses with a linearizable compact snapshot" {
    const TestState = struct {
        eventual_calls: usize = 0,
        linearizable_calls: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 11, .name = "new-table", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11001, .table_id = 11, .range_id = 17, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = linearizableRoutingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.eventual_calls += 1;
            return .{ .catalog_revision = 4, .tables = &.{}, .ranges = &.{} };
        }

        fn linearizableRoutingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .catalog_revision = 5,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }
    };

    var state = TestState{};
    var result = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "new-table",
        "",
        "",
        null,
    );
    defer result.deinit(std.testing.allocator);
    switch (result) {
        .found => |plan| {
            try std.testing.expectEqual(@as(u64, 5), plan.catalog_revision);
            try std.testing.expectEqual(@as(u64, 11001), plan.groups[0].group_id);
            try std.testing.expectEqual(@as(u64, 17), plan.groups[0].identity_namespace.range_id);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 2), state.frees);
}

test "route resolver confirms a table-present range miss linearly" {
    const TestState = struct {
        eventual_calls: usize = 0,
        linearizable_calls: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 12, .name = "docs", .placement_role = "data" },
        };
        const eventual_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 12001, .table_id = 12, .start_key = "", .end_key = "doc:m" },
        };
        const authoritative_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 12001, .table_id = 12, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 12002, .table_id = 12, .range_id = 22, .start_key = "doc:m", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = linearizableRoutingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.eventual_calls += 1;
            return .{
                .catalog_revision = 8,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(eventual_ranges[0..]),
            };
        }

        fn linearizableRoutingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .catalog_revision = 9,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(authoritative_ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }
    };

    var state = TestState{};
    try std.testing.expectEqual(
        @as(?u64, 12002),
        try resolveGroupForKeyUntil(
            std.testing.allocator,
            FakeCatalog.iface(&state),
            "docs",
            "doc:z",
            platform_time.monotonicNs() + std.time.ns_per_s,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 2), state.frees);
}

test "eventual span routing distinguishes snapshot timeout" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = routingSnapshot,
                    .linearizable_routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(_: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            try std.testing.expect(deadline_ns != null);
            return error.CatalogRoutingSnapshotTimeout;
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };

    const result = try resolveGroupsForSpanEventually(
        std.testing.allocator,
        FakeCatalog.iface(),
        "docs",
        "",
        "",
        std.time.ns_per_s,
        1,
    );
    try std.testing.expectEqual(ResolveGroupsResult.timed_out, result);
}

test "await route observes delayed publication without a polling sleep" {
    const State = struct {
        published: bool = false,
        token: u64 = 1,
        waits: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 21, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 21001, .table_id = 21, .start_key = "", .end_key = null },
        };

        fn iface(state: *State) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = routingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
                .wait_for_routing_change = waitForChange,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *State = @ptrCast(@alignCast(ptr));
            return .{
                .catalog_revision = state.token,
                .change_token = .{ .revision = state.token },
                .tables = if (state.published) @constCast(tables[0..]) else &.{},
                .ranges = if (state.published) @constCast(ranges[0..]) else &.{},
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }

        fn waitForChange(ptr: *anyopaque, observed: metadata_api.CatalogRoutingChangeToken, _: u64, _: u64) !CatalogChangeWaitResult {
            const state: *State = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(state.token, observed.revision);
            state.waits += 1;
            state.published = true;
            state.token += 1;
            return .changed;
        }
    };

    var state = State{};
    var result = try awaitRoute(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "docs",
        .all_ranges,
        platform_time.monotonicNs() + std.time.ns_per_s,
        50 * std.time.ns_per_ms,
    );
    defer result.deinit(std.testing.allocator);
    switch (result) {
        .found => |plan| try std.testing.expectEqual(@as(u64, 21001), plan.groups[0].group_id),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), state.waits);
    try std.testing.expectEqual(@as(usize, 3), state.frees);
}

test "await route distinguishes persistent absence from capture timeout" {
    const State = struct { waits: usize = 0 };
    const FakeCatalog = struct {
        fn iface(state: *State) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = routingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
                .wait_for_routing_change = waitForChange,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return .{ .tables = &.{}, .ranges = &.{} };
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}

        fn waitForChange(ptr: *anyopaque, _: metadata_api.CatalogRoutingChangeToken, _: u64, _: u64) !CatalogChangeWaitResult {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.waits += 1;
            return .authoritative_absence;
        }
    };

    var state = State{};
    const result = try awaitRoute(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "missing",
        .all_ranges,
        platform_time.monotonicNs() + std.time.ns_per_s,
        std.time.ns_per_ms,
    );
    try std.testing.expectEqual(AwaitRouteResult.publication_not_observed, result);
    try std.testing.expectEqual(@as(usize, 1), state.waits);
}

test "await route reports an expired pre-capture deadline as timed out" {
    const State = struct { snapshots: usize = 0 };
    const FakeCatalog = struct {
        fn iface(state: *State) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = routingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.snapshots += 1;
            return .{ .tables = &.{}, .ranges = &.{} };
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };

    var state = State{};
    const result = try awaitRoute(
        std.testing.allocator,
        try FakeCatalog.iface(&state).routingSource(),
        "missing",
        .all_ranges,
        platform_time.monotonicNs() -| 1,
        std.time.ns_per_ms,
    );
    try std.testing.expectEqual(AwaitRouteResult.timed_out, result);
    try std.testing.expectEqual(@as(usize, 0), state.snapshots);
}

test "catalog doc identity readiness checks table range health" {
    const alloc = std.testing.allocator;

    const TestState = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus = &.{},
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
            .{ .table_id = 8, .name = "other", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
            .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(state.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var missing_statuses = TestState{};
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&missing_statuses), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&missing_statuses), "docs"));

    const healthy = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
        .{ .group_id = 8001, .doc_identity = .{ .rebuild_required = true } },
    };
    var healthy_state = TestState{ .statuses = healthy[0..] };
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&healthy_state), "docs");
    try validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&healthy_state), "docs");
    try validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&healthy_state), "docs", &.{7001}, 7, 7001, 7001);
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&healthy_state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001));

    const mixed_version = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var mixed_state = TestState{ .statuses = mixed_version[0..] };
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&mixed_state), "docs");
    try validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&mixed_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&mixed_state), "docs", &.{7001}, 7, 7001, 7001));

    const rebuild_required = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001 },
        .{ .group_id = 7002, .doc_identity = .{ .rebuild_required = true } },
    };
    var rebuild_state = TestState{ .statuses = rebuild_required[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&rebuild_state), "docs"));

    const namespace_conflict = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity_namespace_conflict = true },
        .{ .group_id = 7002 },
    };
    var conflict_state = TestState{ .statuses = namespace_conflict[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&conflict_state), "docs"));

    const reassignment_active = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity_reassignment_active = true },
        .{ .group_id = 7002 },
    };
    var reassignment_state = TestState{ .statuses = reassignment_active[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&reassignment_state), "docs"));

    const stale_namespace = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
    };
    var stale_state = TestState{ .statuses = stale_namespace[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&stale_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&stale_state), "docs", &.{7002}, 7, 7001, 7001));

    const empty_stale_namespace = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
    };
    var empty_stale_state = TestState{ .statuses = empty_stale_namespace[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&empty_stale_state), "docs"));
}

test "catalog resolved filter validation accepts preserved split identity domains" {
    const TestState = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus = &.{},
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .range_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{
                .group_id = 7002,
                .range_id = 7002,
                .table_id = 7,
                .start_key = "doc:m",
                .end_key = null,
                .doc_identity_shard_id = 7001,
                .doc_identity_range_id = 7001,
            },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(state.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var missing_state = TestState{};
    try validateDocIdentityReadyForTable(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs", &.{7002}, 7, 7001, 7001));

    const old_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
    };
    var old_state = TestState{ .statuses = old_statuses[0..] };
    try validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&old_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&old_state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001));

    const stale_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var stale_state = TestState{ .statuses = stale_statuses[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(std.testing.allocator, FakeCatalog.iface(&stale_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&stale_state), "docs", &.{7002}, 7, 7001, 7001));

    const statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
    };
    var state = TestState{ .statuses = statuses[0..] };
    try validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&state), "docs");
    try validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001);
}
