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
};
pub const Source = struct {
    catalog: catalog_api.CatalogSource,
    controls: @import("history_control_operations.zig").Port,
    pages: @import("history_page_operations.zig").Port,
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
    /// Assemble a single immutable root-local transaction from authenticated
    /// retained pages. The current grant and private cursor fence every read.
    pub fn readRelationalFrame(self: Source, a: A, state: control.State, position: control.Position, now: u64, max_events_per_page: u32, context: Context) !?@import("history_page_operations.zig").pages.Page {
        const history = @import("history_page_operations.zig");
        const frame = @import("antfly_local_sources").storage_committed_history.frame;
        try context.ensureActive();
        const definition = state.definition orelse return error.ChangefeedConflict;
        if (state.phase != .streaming or definition.tables.len != 1 or definition.tables[0].contract != .native_relational or
            state.roots.len != 1 or position.event != 0 or max_events_per_page == 0 or max_events_per_page > 1024)
            return error.ChangefeedCapabilityUnsupported;
        if (now == 0 or now >= state.expires_at) return error.ChangefeedLeaseExpired;
        const item = state.roots[0];
        if (item.released or item.consumer_epoch == 0) return error.ChangefeedResynchronizationRequired;
        var captured = try self.capture(a, definition, context);
        defer captured.deinit();
        if (captured.roots.len != 1) return error.ChangefeedResynchronizationRequired;
        const root = try captured.find(item.namespace);
        var input: history.Request = .{ .version = 1, .request = .{
            .reference = .{
                .namespace = root.namespace,
                .authority = state.authority,
                .consumer = state.consumerId(),
                .epoch = item.consumer_epoch,
                .grant = item.grant_sequence,
                .definition = definition.identity(),
            },
            .after = .{ .sequence = position.sequence },
            .now_seconds = now,
            .max_events = max_events_per_page,
            .max_bytes = history.pages.max_source_bytes,
        } };
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        var header: ?frame.Header = null;
        var header_digest: control.Digest = undefined;
        var events: []frame.Event = &.{};
        var bytes: usize = 0;
        var latest: u64 = position.sequence;
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
            if (current.transaction_scope != .root_local) return error.ChangefeedCapabilityUnsupported;
            const digest = control.hash(current);
            if (header == null) {
                header = current;
                header.?.schema_before = try duplicateOptional(scratch, current.schema_before);
                header.?.schema_after = try duplicateOptional(scratch, current.schema_after);
                header_digest = digest;
                events = try scratch.alloc(frame.Event, page.frame_events);
                bytes = (if (current.schema_before) |value| value.len else 0) +
                    (if (current.schema_after) |value| value.len else 0);
            } else if (!std.meta.eql(digest, header_digest) or page.frame_events != events.len) {
                return error.ChangefeedResynchronizationRequired;
            }
            for (page.events, page.first_ordinal..) |event, index| {
                if (index > 0 and std.mem.order(u8, events[index - 1].key, event.key) != .lt)
                    return error.HistoryCorrupt;
                bytes += event.key.len + (if (event.before) |value| value.len else 0) +
                    (if (event.after) |value| value.len else 0) + 12;
                if (bytes > history.pages.max_source_bytes) return error.HistoryTooLarge;
                events[index] = .{
                    .key = try scratch.dupe(u8, event.key),
                    .before = try duplicateOptional(scratch, event.before),
                    .after = try duplicateOptional(scratch, event.after),
                };
            }
            if (page.next.event == 0) {
                _ = try frame.encodedLength(header.?, events);
                try self.validate(a, state, root.namespace, root.fence, root.table_name, context);
                return .{ .arena = arena, .header = header, .events = events, .frame_events = page.frame_events, .next = page.next, .observed_latest = latest, .caught_up = page.caught_up };
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
        const root = captured.roots[0];
        try allocation_fixture.source().validate(a, state, root.namespace, root.fence, root.table_name, .{});
        var wrong_fence = root.fence;
        wrong_fence.metadata_incarnation = @splat(9);
        try std.testing.expectError(error.ChangefeedResynchronizationRequired, allocation_fixture.source().validate(a, state, root.namespace, wrong_fence, root.table_name, .{}));
        try std.testing.checkAllAllocationFailures(a, captureAllocations, .{ &allocation_fixture, definition });
    }
    fn captureAllocations(a: A, fixture: *Fixture, definition: control.Definition) !void {
        var owned = try fixture.source().capture(a, definition, .{});
        defer owned.deinit();
    }
} else struct {};
