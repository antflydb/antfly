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

//! Request-owned schema plans, shared by column selection and primary rows.
//! Active block/overlay handles pin plans; admission cannot retire their views.
const std = @import("std");
const registry = @import("schema_registry.zig");
const admission = @import("schema_cache_admission.zig").Admission;
const graph = @import("query/graph_exec.zig");
const projection = @import("query/relational_projection.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const types = @import("types.zig");
const Allocator = std.mem.Allocator;

pub const Filter = union(enum) {
    match_all,
    match_none,
    doc_id: []const []const u8,
    field_matcher: Matcher,
    conjuncts: []Filter,
    disjuncts: []Filter,
    bool_query: struct { must: []Filter, should: []Filter, must_not: []Filter, min_should: usize },

    pub const Matcher = struct {
        ordinal: ?u32,
        remaining: ?graph.CompiledPatternFilter.FieldPath,
        predicate: *const graph.CompiledPatternFilter.FieldPredicate,
    };

    fn bindMany(alloc: Allocator, source: []const graph.CompiledPatternFilter, bindings: *const std.StringHashMapUnmanaged(?u32)) Allocator.Error![]Filter {
        const result = try alloc.alloc(Filter, source.len);
        for (source, result) |*item, *out| out.* = try bind(alloc, item, bindings);
        return result;
    }

    fn bind(alloc: Allocator, source: *const graph.CompiledPatternFilter, bindings: *const std.StringHashMapUnmanaged(?u32)) Allocator.Error!Filter {
        return switch (source.*) {
            .match_all => .match_all,
            .match_none => .match_none,
            .doc_id => |ids| .{ .doc_id = ids },
            .field_matcher => |*matcher| blk: {
                var tail: ?graph.CompiledPatternFilter.FieldPath = null;
                const root = switch (matcher.path) {
                    .single => |name| name,
                    .dotted => |parts| parts: {
                        if (parts.len > 1) tail = .{ .dotted = parts[1..] };
                        break :parts parts[0];
                    },
                    .json_pointer => |parts| parts: {
                        if (parts.len > 1) tail = .{ .json_pointer = parts[1..] };
                        break :parts parts[0];
                    },
                };
                break :blk .{ .field_matcher = .{ .ordinal = bindings.get(root).?, .remaining = tail, .predicate = &matcher.predicate } };
            },
            .conjuncts => |items| .{ .conjuncts = try bindMany(alloc, items, bindings) },
            .disjuncts => |items| .{ .disjuncts = try bindMany(alloc, items, bindings) },
            .bool_query => |query| .{ .bool_query = .{
                .must = try bindMany(alloc, query.must, bindings),
                .should = try bindMany(alloc, query.should, bindings),
                .must_not = try bindMany(alloc, query.must_not, bindings),
                .min_should = query.min_should,
            } },
        };
    }
};

pub const Plan = struct {
    alloc: Allocator,
    view: registry.SchemaView,
    arena: std.heap.ArenaAllocator,
    primary_filter: ?graph.PreparedOrdinalPatternFilter,
    filter: ?Filter,
    projected: ?projection.Plan,
    references: usize = 1,
    touched: u64 = 0,

    /// The caller transfers its view only on success.
    fn create(alloc: Allocator, view: registry.SchemaView, source: ?*const graph.PreparedPatternFilter, opts: types.ScanOptions) !*Plan {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var primary_filter = if (source) |value| try graph.PreparedOrdinalPatternFilter.init(alloc, value, view.tableSchema().*, view.physicalLayout()) else null;
        errdefer if (primary_filter) |*value| value.deinit();
        const filter = if (source) |value| try Filter.bind(arena.allocator(), &value.compiled, &primary_filter.?.ordinals) else null;
        var projected = if (opts.include_documents and projection.Plan.supports(opts.fields, opts.include_all_fields))
            try projection.Plan.init(alloc, view.tableSchema().*, view.physicalLayout(), opts.fields)
        else
            null;
        errdefer if (projected) |*value| value.deinit();
        const result = try alloc.create(Plan);
        result.* = .{ .alloc = alloc, .view = view, .arena = arena, .primary_filter = primary_filter, .filter = filter, .projected = projected };
        return result;
    }

    pub fn release(self: *Plan) void {
        self.references -= 1;
        if (self.references != 0) return;
        if (self.projected) |*value| value.deinit();
        if (self.primary_filter) |*value| value.deinit();
        self.arena.deinit();
        self.view.release();
        self.alloc.destroy(self);
    }

    pub fn matches(self: *const Plan, alloc: Allocator, key: []const u8, row: codec.OrdinalRowView) !bool {
        const filter = if (self.primary_filter) |*value| value else return true;
        return (try filter.matches(alloc, key, row)) orelse blk: {
            var logical = try row.materializeRootAlloc(alloc);
            defer logical.deinit(alloc);
            break :blk try filter.source.compiled.matches(alloc, key, logical.root);
        };
    }
};

pub const Cache = struct {
    alloc: Allocator,
    source: ?*const graph.PreparedPatternFilter,
    opts: types.ScanOptions,
    entries: [32]?*Plan = @splat(null),
    frequency: admission = .{},
    clock: u64 = 0,

    pub fn deinit(self: *Cache) void {
        for (self.entries) |entry| if (entry) |plan| plan.release();
    }

    pub fn get(self: *Cache, db: anytype, version: u32) !*Plan {
        self.frequency.record(version);
        self.clock +|= 1;
        var victim: ?usize = null;
        for (self.entries, 0..) |entry, i| {
            if (entry) |plan| {
                if (plan.view.version() == version) {
                    plan.references += 1;
                    plan.touched = self.clock;
                    if (self.opts.columnar_stats) |stats| stats.scan_plan_hits += 1;
                    return plan;
                }
                if (plan.references != 1) continue;
                if (victim == null or (self.entries[victim.?] != null and
                    (self.frequency.frequency(versionOf(plan)) < self.frequency.frequency(versionOf(self.entries[victim.?].?)) or
                        (self.frequency.frequency(versionOf(plan)) == self.frequency.frequency(versionOf(self.entries[victim.?].?)) and plan.touched < self.entries[victim.?].?.touched)))) victim = i;
            } else victim = i;
        }
        var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
        errdefer view.release();
        const plan = try Plan.create(self.alloc, view, self.source, self.opts);
        plan.touched = self.clock;
        if (self.opts.columnar_stats) |stats| stats.scan_plans_built += 1;
        if (victim) |index| {
            if (self.entries[index]) |old| {
                if (!self.frequency.admits(version, versionOf(old))) return plan;
                old.release();
            }
            self.entries[index] = plan;
            plan.references += 1;
        }
        return plan;
    }

    fn versionOf(plan: *const Plan) u32 {
        return plan.view.version();
    }
};

/// Execution ownership is independent of cache admission. A stream retains
/// its current epoch even if a scan-resistant cache rejects that epoch. The
/// returned plan is borrowed until the next get/use/deinit on this cursor.
pub const Cursor = struct {
    active: ?*Plan = null,

    pub fn deinit(self: *Cursor) void {
        if (self.active) |plan| plan.release();
        self.* = .{};
    }

    pub fn use(self: *Cursor, plan: *Plan) void {
        if (self.active == plan) return;
        plan.references += 1;
        if (self.active) |old| old.release();
        self.active = plan;
    }

    pub fn get(self: *Cursor, cache: *Cache, db: anytype, version: u32) !*Plan {
        if (self.active) |plan| if (plan.view.version() == version) {
            if (cache.opts.columnar_stats) |stats| stats.scan_plan_hits += 1;
            return plan;
        };
        // Acquire before releasing so failure leaves the cursor usable.
        const next = try cache.get(db, version);
        if (self.active) |old| old.release();
        self.active = next;
        return next;
    }
};

const TestCore = struct {
    alloc: Allocator,
    pub fn acquireSchemaVersionView(self: *TestCore, version: u32) !?registry.SchemaView {
        const schema = @import("../schema.zig");
        const named = schema.RelationalColumn{ .name = "user-name", .path = "user-name", .column_type = .string };
        const nested = schema.RelationalColumn{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any };
        const columns = if (version % 2 == 0) [_]schema.RelationalColumn{ nested, named } else [_]schema.RelationalColumn{ named, nested };
        return .{ .epoch = try registry.Epoch.createCloned(self.alloc, .{ .version = version, .storage_mode = .relational, .relational_columns = &columns }) };
    }
};

fn testPlanAllocation(alloc: Allocator) !void {
    var db = struct { core: TestCore }{ .core = .{ .alloc = alloc } };
    var source = try graph.PreparedPatternFilter.init(alloc, "{\"conjuncts\":[{\"term\":{\"payload.id\":2}},{\"exists\":{\"field\":\"missing\"}}]}");
    defer source.deinit();
    var cache = Cache{ .alloc = alloc, .source = &source, .opts = .{ .include_documents = true, .fields = &.{ "user-name", "payload.id" } } };
    defer cache.deinit();
    for ([_]u32{ 1, 2, 1 }) |version| {
        const plan = try cache.get(&db, version);
        defer plan.release();
        try std.testing.expectEqual(@as(?u32, if (version == 1) 1 else 0), plan.filter.?.conjuncts[0].field_matcher.ordinal);
        try std.testing.expectEqual(@as(?u32, null), plan.filter.?.conjuncts[1].field_matcher.ordinal);
        try std.testing.expectEqualStrings("id", plan.filter.?.conjuncts[0].field_matcher.remaining.?.dotted[0]);
        try std.testing.expectEqual(@as(u32, if (version == 1) 0 else 1), plan.projected.?.base.ordinals[0]);
    }
}

test "column scan plans bind epochs once and release every allocation failure" {
    try testPlanAllocation(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testPlanAllocation, .{});
}

test "column scan plans bound residency without evicting pinned blocks" {
    const alloc = std.testing.allocator;
    var db = struct { core: TestCore }{ .core = .{ .alloc = alloc } };
    var stats: types.ColumnarScanStats = .{};
    var cache = Cache{ .alloc = alloc, .source = null, .opts = .{ .columnar_stats = &stats } };
    defer cache.deinit();
    var pins: [32]*Plan = undefined;
    var initialized: usize = 0;
    defer for (pins[0..initialized]) |plan| plan.release();
    for (&pins, 0..) |*pin, i| {
        pin.* = try cache.get(&db, @intCast(i + 1));
        initialized += 1;
    }
    for (33..100) |version| {
        const transient = try cache.get(&db, @intCast(version));
        transient.release();
    }
    const first = try cache.get(&db, 1);
    defer first.release();
    try std.testing.expect(first == pins[0]);
    try std.testing.expectEqual(@as(u64, 99), stats.scan_plans_built);
    try std.testing.expectEqual(@as(u64, 1), stats.scan_plan_hits);
    for (cache.entries, 0..) |entry, i| try std.testing.expect(entry.? == pins[31 - i]);
}

test "column scan cursor retains rejected epochs and shares pinned execution plans" {
    const alloc = std.testing.allocator;
    var db = struct { core: TestCore }{ .core = .{ .alloc = alloc } };
    var stats: types.ColumnarScanStats = .{};
    var cache = Cache{ .alloc = alloc, .source = null, .opts = .{ .columnar_stats = &stats } };
    defer cache.deinit();
    // All residents are much hotter than the trailing historical epoch.
    for (0..40) |_| for (1..33) |version| {
        const plan = try cache.get(&db, @intCast(version));
        plan.release();
    };
    stats = .{};
    var column: Cursor = .{};
    defer column.deinit();
    const transient = try column.get(&cache, &db, 33);
    for (cache.entries) |entry| try std.testing.expect(entry.? != transient);
    var primary: Cursor = .{};
    defer primary.deinit();
    for (0..256) |_| {
        primary.use(transient);
        try std.testing.expectEqual(transient, try primary.get(&cache, &db, 33));
        // Interleaved dirty rows use another epoch without losing the base
        // execution plan, even though it never entered the resident cache.
        _ = try primary.get(&cache, &db, 1);
    }
    try std.testing.expectEqual(@as(u64, 1), stats.scan_plans_built);
    try std.testing.expectEqual(transient, try column.get(&cache, &db, 33));
}

fn testCursorAllocation(alloc: Allocator) !void {
    var db = struct { core: TestCore }{ .core = .{ .alloc = alloc } };
    var cache = Cache{ .alloc = alloc, .source = null, .opts = .{} };
    defer cache.deinit();
    var cursor: Cursor = .{};
    defer cursor.deinit();
    const first = try cursor.get(&cache, &db, 1);
    try std.testing.expectEqual(first, try cursor.get(&cache, &db, 1));
    const second = cursor.get(&cache, &db, 2) catch |err| {
        try std.testing.expectEqual(first, cursor.active.?);
        return err;
    };
    cursor.use(first);
    try std.testing.expectEqual(first, cursor.active.?);
    cursor.use(second);
}

test "column scan cursor releases plans on every allocation failure" {
    try testCursorAllocation(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testCursorAllocation, .{});
}

test "column scan cursor historical churn allocation benchmark" {
    var bytes: [2]usize = undefined;
    for (0..2) |mode| {
        var measured = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const alloc = measured.allocator();
        {
            var db = struct { core: TestCore }{ .core = .{ .alloc = alloc } };
            var stats: types.ColumnarScanStats = .{};
            var cache = Cache{ .alloc = alloc, .source = null, .opts = .{ .columnar_stats = &stats } };
            defer cache.deinit();
            for (0..40) |_| for (1..33) |version| {
                const plan = try cache.get(&db, @intCast(version));
                plan.release();
            };
            stats = .{};
            const before = measured.allocated_bytes;
            var cursor: Cursor = .{};
            defer cursor.deinit();
            for (0..32) |_| {
                if (mode == 0) {
                    const plan = try cache.get(&db, 33);
                    plan.release();
                } else _ = try cursor.get(&cache, &db, 33);
            }
            bytes[mode] = measured.allocated_bytes - before;
            try std.testing.expectEqual(@as(u64, if (mode == 0) 32 else 1), stats.scan_plans_built);
        }
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
    }
    try std.testing.expectEqual(bytes[0], bytes[1] * 32);
    std.debug.print("\nhistorical plan churn: per-row/cursor builds=32/1, allocated bytes={d}/{d}\n", .{ bytes[0], bytes[1] });
}
