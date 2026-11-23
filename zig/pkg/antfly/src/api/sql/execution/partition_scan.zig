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

//! Expand immutable logical families before capturing the ordinary coordinated
//! statement. Each physical cursor is borrowed from that one statement owner;
//! concatenation never claims global index/order evidence from a single leaf.
const std = @import("std");
const catalog = @import("antfly_local_sources").sql_catalog;
const A = std.mem.Allocator;
pub const max_physical_scans = 64;

pub const Group = struct { first: usize, count: usize, family: bool };
pub const Expansion = struct {
    allocator: A,
    physical: []catalog.StatementScan,
    groups: []Group,
    parents: []catalog.Table = &.{},

    pub fn deinit(self: *Expansion) void {
        self.allocator.free(self.physical);
        self.allocator.free(self.groups);
        self.allocator.free(self.parents);
        self.* = undefined;
    }
};

/// No native handle is opened until the entire fanout has passed its shape and
/// budget checks. In particular, an oversized family cannot acquire a prefix
/// of its members under independent visibility cuts.
const partitions = @import("antfly_local_sources").system_catalog_table_partitions;

const PruningRange = struct {
    lower: i128 = std.math.minInt(i128),
    upper: i128 = std.math.maxInt(i128),

    fn bind(table: catalog.Table, descriptor: partitions.Descriptor, scan: catalog.Scan) PruningRange {
        var result: PruningRange = .{};
        const key = table.column(descriptor.key_name) catch return result;
        for (scan.conditions) |term| {
            if (!std.mem.eql(u8, term.column, key.path) and !std.mem.eql(u8, term.column, key.name)) continue;
            const datum = term.datum();
            // Unsupported coercions remain in the full SQL residual.
            const point: i128 = if (datum.temporal()) |value| switch (value.*) {
                .timestamp_tz => |timestamp| if (timestamp.infinity() == 0) timestamp.micros else continue,
                else => continue,
            } else partitions.timestampKey(term.value) catch continue;
            switch (term.op) {
                .eq => {
                    result.lower = @max(result.lower, point);
                    result.upper = @min(result.upper, point + 1);
                },
                .gt => result.lower = @max(result.lower, point + 1),
                .gte => result.lower = @max(result.lower, point),
                .lt => result.upper = @min(result.upper, point),
                .lte => result.upper = @min(result.upper, point + 1),
                else => {},
            }
        }
        return result;
    }

    fn bound(value: partitions.Bound) i128 {
        return switch (value) {
            .minvalue => std.math.minInt(i128),
            .maxvalue => std.math.maxInt(i128),
            .value => |point| point,
        };
    }

    fn mayMatch(self: PruningRange, member: partitions.Range) bool {
        return @max(self.lower, bound(member.lower)) < @min(self.upper, bound(member.upper));
    }
};

pub fn expand(a: A, requests: []const catalog.StatementScan) !Expansion {
    if (requests.len > max_physical_scans) return error.SqlProgramLimitExceeded;
    var physical: std.ArrayList(catalog.StatementScan) = .empty;
    errdefer physical.deinit(a);
    const groups = try a.alloc(Group, requests.len);
    errdefer a.free(groups);
    const parents = try a.alloc(catalog.Table, requests.len);
    errdefer a.free(parents);
    for (requests, groups, parents) |request, *group, *parent| {
        group.* = .{ .first = physical.items.len, .count = 0, .family = request.table.partition_family != null };
        if (request.table.partition_family) |family| {
            try family.descriptor.validate();
            try family.parent_guard.validate();
            const descriptor_digest = try family.descriptor.digest();
            const policy_digest = try family.descriptor.policy.digest();
            if (family.descriptor.parent.table_id != request.table.id or
                family.descriptor.parent.schema_version != request.table.schema_version or
                family.parent_guard.owner.table_id != request.table.id or
                family.parent_guard.owner.schema_version != request.table.schema_version or
                family.parent_guard.hasDefinitionIntent() or
                family.leaves.len != family.descriptor.members.len)
                return error.PartitionCatalogChanged;
            const pruning = PruningRange.bind(request.table, family.descriptor, request.request);
            if (request.request.index_equality != null or request.request.index_range != null)
                return error.InvalidSqlBackendResponse;
            parent.* = request.table;
            parent.partition_family = null;
            for (family.leaves, family.descriptor.members) |leaf, member| {
                const route = leaf.partition_route orelse return error.PartitionCatalogChanged;
                if (leaf.partition_family != null or leaf.partition_parent == null or leaf.partition_guard == null or
                    leaf.id != member.relation.table_id or leaf.schema_version != member.relation.schema_version or
                    route.parent_id != request.table.id or route.child_id != leaf.id or
                    route.child_schema_version != leaf.schema_version or
                    leaf.partition_parent.?.id != request.table.id or
                    leaf.partition_parent.?.schema_version != request.table.schema_version or
                    route.parent_revision != family.descriptor.revision or
                    route.membership_generation != member.membership_generation or
                    !std.mem.eql(u8, &route.parent_descriptor, &descriptor_digest) or
                    !std.mem.eql(u8, &route.policy, &policy_digest) or
                    !std.meta.eql(route.inherited, try @import("antfly_local_sources").system_catalog_table_partitions.inheritanceDigest(member.inherited)))
                    return error.PartitionCatalogChanged;
                try route.validate();
                if (!pruning.mayMatch(member.bounds)) continue;
                if (physical.items.len == max_physical_scans) return error.SqlProgramLimitExceeded;
                var scan = request.request;
                // An ordered prefix is safe only after a provider attests the
                // complete source order. The concatenation below never does.
                scan.ordered_window = null;
                var bounded_leaf = leaf;
                bounded_leaf.partition_parent = parent;
                try physical.append(a, .{ .table = bounded_leaf, .request = scan });
            }
        } else {
            if (physical.items.len == max_physical_scans) return error.SqlProgramLimitExceeded;
            try physical.append(a, request);
        }
        group.count = physical.items.len - group.first;
    }
    return .{ .allocator = a, .physical = try physical.toOwnedSlice(a), .groups = groups, .parents = parents };
}

const FamilyCursor = struct {
    children: []const catalog.Cursor,
    current: usize = 0,
    page_sequence: u64 = 0,
    closed: bool = false,

    fn next(raw: *anyopaque, a: A, limit: u32) anyerror!catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.closed) return error.InvalidSqlBackendResponse;
        if (limit == 0) return error.InvalidArgument;
        while (self.current < self.children.len) {
            var page = try self.children[self.current].next(self.children[self.current].ptr, a, limit);
            if (page.rows.len > limit) {
                page.deinit();
                return error.InvalidSqlBackendResponse;
            }
            if (page.after == null) self.current += 1;
            if (page.rows.len == 0 and self.current < self.children.len) {
                page.deinit();
                continue;
            }
            errdefer page.deinit();
            if (self.current < self.children.len) {
                self.page_sequence = std.math.add(u64, self.page_sequence, 1) catch return error.SqlProgramLimitExceeded;
                // Each emitted page owns a unique internal continuation.
                // Repeating a static marker makes bulk mutation pagination
                // appear stalled; cursor-owned mutable bytes break retained
                // earlier pages. Neither may be used as a leaf physical key.
                if (page.owned_arena == null) page.owned_arena = .init(a);
                const token = try page.owned_arena.?.allocator().alloc(u8, 8);
                std.mem.writeInt(u64, token[0..8], self.page_sequence, .big);
                page.after = token;
            }
            return page;
        }
        return .{ .rows = &.{} };
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // Native views and overlays belong to Owner.physical, exactly once.
        self.closed = true;
    }
};

const Owner = struct {
    allocator: A,
    physical: ?catalog.StatementRead,
    expansion: Expansion,
    families: []FamilyCursor,
    cursors: []catalog.Cursor,

    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.allocator;
        for (self.families) |*family| family.closed = true;
        if (self.physical) |physical| physical.close(physical.ptr);
        a.free(self.cursors);
        a.free(self.families);
        self.expansion.deinit();
        a.destroy(self);
    }
};

/// Consumes expansion and the physical statement on both success and failure.
/// Null is permitted only for an entirely empty set of physical sources.
pub fn compose(a: A, expansion: Expansion, physical: ?catalog.StatementRead) !catalog.StatementRead {
    var owned = expansion;
    errdefer owned.deinit();
    errdefer if (physical) |read| read.close(read.ptr);
    if ((physical == null) != (owned.physical.len == 0)) return error.InvalidSqlBackendResponse;
    if (physical) |read| if (read.cursors.len != owned.physical.len) return error.InvalidSqlBackendResponse;
    const self = try a.create(Owner);
    errdefer a.destroy(self);
    const cursors = try a.alloc(catalog.Cursor, owned.groups.len);
    errdefer a.free(cursors);
    const families = try a.alloc(FamilyCursor, owned.groups.len);
    errdefer a.free(families);
    for (owned.groups, cursors, families) |group, *cursor, *family| {
        const children: []const catalog.Cursor = if (physical) |read| read.cursors[group.first..][0..group.count] else &.{};
        family.* = .{ .children = children };
        cursor.* = if (!group.family) children[0] else .{ .ptr = family, .next = FamilyCursor.next, .close = FamilyCursor.close, .order_satisfied = false };
    }
    self.* = .{ .allocator = a, .physical = physical, .expansion = owned, .families = families, .cursors = cursors };
    return .{ .ptr = self, .cursors = cursors, .close = Owner.close };
}

test "SQL partition parent routing family cursor preserves duplicate native keys and later leaf exhaustion" {
    const Fake = struct {
        rows: []const catalog.Row,
        emitted: bool = false,
        fn next(raw: *anyopaque, _: A, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.emitted) return .{ .rows = &.{} };
            self.emitted = true;
            return .{ .rows = self.rows };
        }
        fn close(_: *anyopaque) void {}
        fn cursor(self: *@This()) catalog.Cursor {
            return .{ .ptr = self, .next = next, .close = close };
        }
    };
    const left_rows = [_]catalog.Row{.{ .id = "same", .version = 1, .origin_table_id = 11, .value = .null }};
    const right_rows = [_]catalog.Row{.{ .id = "same", .version = 2, .origin_table_id = 12, .value = .null }};
    var empty: Fake = .{ .rows = &.{} };
    var left: Fake = .{ .rows = &left_rows };
    var right: Fake = .{ .rows = &right_rows };
    var third: Fake = .{ .rows = &right_rows };
    const children = [_]catalog.Cursor{ empty.cursor(), left.cursor(), right.cursor(), third.cursor() };
    var family: FamilyCursor = .{ .children = &children };
    var first = try FamilyCursor.next(&family, std.testing.allocator, 8);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 1), first.rows.len);
    try std.testing.expectEqual(@as(u64, 11), first.rows[0].origin_table_id);
    try std.testing.expect(first.after != null);
    var second = try FamilyCursor.next(&family, std.testing.allocator, 8);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.rows.len);
    try std.testing.expectEqualStrings(first.rows[0].id, second.rows[0].id);
    try std.testing.expectEqual(@as(u64, 12), second.rows[0].origin_table_id);
    try std.testing.expect(second.after != null);
    try std.testing.expect(!std.mem.eql(u8, first.after.?, second.after.?));
    try std.testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, first.after.?[0..8], .big));
    try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, second.after.?[0..8], .big));
    var last = try FamilyCursor.next(&family, std.testing.allocator, 8);
    defer last.deinit();
    try std.testing.expectEqual(@as(usize, 1), last.rows.len);
    try std.testing.expect(last.after == null);
    var final = try FamilyCursor.next(&family, std.testing.allocator, 8);
    defer final.deinit();
    try std.testing.expectEqual(@as(usize, 0), final.rows.len);
}

test "partition family composition owns one captured physical statement under allocation failure" {
    const Probe = struct {
        closed: usize = 0,
        fn next(_: *anyopaque, _: A, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn borrowedClose(_: *anyopaque) void {}
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
        }
        fn run(a: A) !void {
            var probe: @This() = .{};
            const physical = try a.alloc(catalog.StatementScan, 1);
            const groups = a.alloc(Group, 1) catch |err| {
                a.free(physical);
                return err;
            };
            groups[0] = .{ .first = 0, .count = 1, .family = true };
            const child = [_]catalog.Cursor{.{ .ptr = &probe, .next = next, .close = borrowedClose, .order_satisfied = true }};
            const captured: catalog.StatementRead = .{ .ptr = &probe, .cursors = &child, .close = close };
            const read = compose(a, .{ .allocator = a, .physical = physical, .groups = groups }, captured) catch |err| {
                try std.testing.expectEqual(@as(usize, 1), probe.closed);
                return err;
            };
            try std.testing.expectEqual(@as(usize, 0), probe.closed);
            try std.testing.expect(!read.cursors[0].order_satisfied);
            read.cursors[0].close(read.cursors[0].ptr);
            try std.testing.expectEqual(@as(usize, 0), probe.closed);
            read.close(read.ptr);
            try std.testing.expectEqual(@as(usize, 1), probe.closed);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "partition family expansion verifies complete route and caps combined statement fanout" {
    const a = std.testing.allocator;
    const Core = @import("antfly_local_sources").system_catalog_table_partitions;
    const Domain = @import("antfly_local_sources").system_catalog_domain;
    const guard: Domain.RelationMutationGuard = .{ .target = .{ .name = "parent" }, .logical_table = "parent", .incarnation = @splat(1), .owner = .{ .table_id = 1, .schema_version = 1, .schema_digest = @splat(2), .kind = .table } };
    const members = [_]Core.Member{
        .{ .relation = .{ .table_id = 2, .schema_version = 1, .definition = @splat(3) }, .bounds = .{ .lower = .minvalue, .upper = .{ .value = 0 } }, .membership_generation = 1 },
        .{ .relation = .{ .table_id = 3, .schema_version = 2, .definition = @splat(4) }, .bounds = .{ .lower = .{ .value = 0 }, .upper = .maxvalue }, .membership_generation = 2 },
    };
    const descriptor: Core.Descriptor = .{ .parent = .{ .table_id = 1, .schema_version = 1, .definition = @splat(2) }, .revision = 2, .key_attribute = 7, .members = &members };
    const parent_authority: catalog.Table = .{ .id = 1, .physical_name = "table:1", .schema_version = 1, .columns = &.{} };
    var leaves = [_]catalog.Table{
        .{ .id = 2, .physical_name = "table:2", .schema_version = 1, .columns = &.{}, .partition_parent = &parent_authority, .partition_guard = &guard, .partition_route = try Core.routeFor(descriptor, 2) },
        .{ .id = 3, .physical_name = "table:3", .schema_version = 2, .columns = &.{}, .partition_parent = &parent_authority, .partition_guard = &guard, .partition_route = try Core.routeFor(descriptor, 3) },
    };
    const family: catalog.PartitionFamily = .{ .descriptor = descriptor, .parent_guard = guard, .leaves = &leaves };
    var parent = parent_authority;
    parent.partition_family = &family;
    const request: catalog.StatementScan = .{ .table = parent, .request = .{ .fields = &.{"created_at"}, .limit = 8, .ordered_window = 5, .primary_key = "same" } };
    var expanded = try expand(a, &.{request});
    defer expanded.deinit();
    try std.testing.expectEqual(@as(usize, 2), expanded.physical.len);
    for (expanded.physical) |scan| {
        try std.testing.expect(scan.request.ordered_window == null);
        try std.testing.expectEqualStrings("same", scan.request.primary_key.?);
    }
    // A signed route for another schema/policy is not usable even when its
    // physical table ID happens to match this immutable declaration.
    leaves[0].partition_route.?.child_schema_version += 1;
    try std.testing.expectError(error.PartitionCatalogChanged, expand(a, &.{request}));
    leaves[0].partition_route = try Core.routeFor(descriptor, 2);
    leaves[0].partition_route.?.policy[0] ^= 1;
    try std.testing.expectError(error.PartitionCatalogChanged, expand(a, &.{request}));
    leaves[0].partition_route = try Core.routeFor(descriptor, 2);
    const requests: [33]catalog.StatementScan = @splat(request);
    var maximum = try expand(a, requests[0..32]);
    defer maximum.deinit();
    try std.testing.expectEqual(@as(usize, max_physical_scans), maximum.physical.len);
    try std.testing.expectError(error.SqlProgramLimitExceeded, expand(a, &requests));
}
