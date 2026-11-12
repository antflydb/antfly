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

//! Shared typed result assembly for retained dataset SQL and JSON reads.
//! A caller supplies one complete metadata cut and a bounded partition reader.
const std = @import("std");
const local = @import("antfly_local_sources");
const dataset = local.retained_dataset_types;
const series = local.storage_retained_series;
const codec = local.storage_retained_series_archive;
const owner_model = @import("../metadata/retained_dataset_owners.zig");
const topology = @import("../metadata/topology_protocol.zig");
const operation = local.api_operation;
const callback_abi = local.runtime_callback_abi;
const A = std.mem.Allocator;
const MemoryBudget = @import("antfly_platform").allocator.BoundedAllocator;

pub const QueryRequest = struct {
    start_ns: []const u8,
    end_ns: []const u8,
    max_rows: u32 = 4096,
    after: ?[]const u8 = null,
    dimensions: ?[]const std.json.Value = null,
};
pub const Request = struct {
    start_ns: []const u8,
    end_ns: []const u8,
    max_rows: u32 = 4096,
    after: ?dataset.RowKey = null,
    dimensions_json: ?[]const u8 = null,
    page: bool = false,
};

pub fn canonicalDimensions(a: A, definition: dataset.Definition, values: []const std.json.Value) ![]const u8 {
    const capture = local.storage_retained_series_capture;
    const Frozen = struct { dimensions: []const struct { name: []const u8, type: capture.DimensionType } };
    const schema = try std.json.parseFromSliceLeaky(Frozen, a, definition.schema_json, .{ .ignore_unknown_fields = true });
    if (values.len != schema.dimensions.len) return error.InvalidDatasetCoverage;
    const normalized = try a.alloc(std.json.Value, values.len);
    for (normalized, values, schema.dimensions) |*out, value, column| out.* = capture.normalizeDimension(a, column.type, value) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDatasetCoverage,
    };
    const result = try std.json.Stringify.valueAlloc(a, normalized, .{});
    if (result.len > 4096) return error.InvalidDatasetCoverage;
    return result;
}

pub fn parseAfter(raw: []const u8) !dataset.RowKey {
    if (raw.len != @sizeOf(dataset.RowKey)) return error.InvalidDatasetCoverage;
    for (raw) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDatasetCoverage;
    return raw[0..@sizeOf(dataset.RowKey)].*;
}
pub const NativeQuery = struct {
    version: u8 = 2,
    observe_only: bool = false,
    status_only: bool = false,
    clock_ns: i128 = 0,
    scope: owner_model.ArchiveScope,
    start_ns: []const u8,
    end_ns: []const u8,
    limit: u32 = 4096,
    after: ?dataset.RowKey = null,
    dimensions_json: ?[]const u8 = null,
    allow_more: bool = false,
    pub fn jsonStringify(self: NativeQuery, writer: anytype) !void {
        if (!self.status_only) return dataset.writeFieldsOmitting(self, .{}, .{"status_only"}, writer);
        try dataset.writeFields(self, .{}, writer);
    }
    pub fn validate(self: NativeQuery, group: u64) !void {
        if ((self.version != 2 and self.version != 3) or (self.status_only and self.version != 3) or (self.observe_only and self.status_only)) return error.InvalidRetainedOwnerPlan;
        try (owner_model.Read{ .owner = self.scope }).validate();
        if (self.scope.group != group or self.start_ns.len > 40 or self.end_ns.len > 40 or self.limit == 0 or self.limit > max_input_rows) return error.InvalidRetainedOwnerPlan;
        const range: dataset.Range = .{ .start_ns = self.start_ns, .end_ns = self.end_ns };
        try range.validate();
        if (self.dimensions_json) |dimensions| if (dimensions.len > 4096) return error.InvalidDatasetCoverage;
        if (self.after) |key| {
            _ = try parseAfter(&key);
            const timestamp = try dataset.rowKeyTimestamp(key);
            if (timestamp < try range.start() or timestamp >= try range.end()) return error.InvalidDatasetCoverage;
        }
    }
};
pub const Observation = struct {
    root: u128,
    source_sequence: u64,
    cursor: series.Cursor,
    admitted_ns: i128,
    pub fn retentionBoundary(spec: series.Spec, clock: i128) !i128 {
        return std.math.add(i128, try spec.bucket(try std.math.sub(i128, clock, spec.retention_ns)), spec.retention_ns);
    }
    pub fn from(root: u128, progress: series.Progress, spec: series.Spec) !Observation {
        if (progress.revoked) return error.RetainedSeriesSourceRevoked;
        if (!progress.ready) return error.RetainedSeriesNotReady;
        return .{ .root = root, .source_sequence = progress.source_sequence, .admitted_ns = try retentionBoundary(spec, progress.admitted_ns), .cursor = .{ .definition = spec.definition, .partition = spec.source_partition, .incarnation = spec.source_incarnation, .sequence = progress.feed_latest } };
    }
};
pub const Capture = struct { table: []const u8, scope: owner_model.ArchiveScope, observation: Observation };
pub const NativeReply = struct {
    rows: []const series.Bucket,
    root_incarnation: []const u8,
    cursor: series.Cursor,
    next: ?dataset.RowKey = null,
    admitted_ns: i128 = 0,
    observation: ?Observation = null,
    progress: ?series.Progress = null,
    pub fn jsonStringify(self: NativeReply, writer: anytype) !void {
        if (self.progress == null) return dataset.writeFieldsOmitting(self, .{}, .{"progress"}, writer);
        try dataset.writeFields(self, .{}, writer);
    }
};
pub const NativeReplay = struct {
    version: u8 = 3,
    scope: owner_model.ArchiveScope,
    root_incarnation: []const u8,
    cursor: series.Cursor,
    limit: u16 = 8,
    clock_ns: i128 = 0,
    pub fn validate(self: NativeReplay, group: u64) !void {
        if (self.version != 2 and self.version != 3) return error.InvalidRetainedOwnerPlan;
        try (owner_model.Read{ .owner = self.scope }).validate();
        if (self.scope.group != group or self.root_incarnation.len == 0 or self.root_incarnation.len > 40 or self.limit == 0 or self.limit > 32) return error.InvalidRetainedOwnerPlan;
        if ((std.fmt.parseInt(u128, self.root_incarnation, 10) catch return error.InvalidRetainedOwnerPlan) == 0) return error.InvalidRetainedOwnerPlan;
    }
};
pub const NativePort = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, []const u8, u64, NativeQuery, operation.RequestContext) anyerror![]u8,
    poll_fn: *const fn (*anyopaque, A, []const u8, u64, NativeReplay, operation.RequestContext) anyerror![]u8,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct {
        read_fn: *const fn (*anyopaque, A, []const u8, u64, NativeQuery, operation.RequestContext) anyerror![]u8,
        poll_fn: *const fn (*anyopaque, A, []const u8, u64, NativeReplay, operation.RequestContext) anyerror![]u8,
    };
    const BoundaryAbi = callback_abi.Boundary(VTable);
    pub fn readRows(self: NativePort, a: A, table: []const u8, group: u64, request: NativeQuery, context: operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate(group);
        return BoundaryAbi.call("read_fn", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, table, group, request, context });
    }
    pub fn poll(self: NativePort, a: A, table: []const u8, group: u64, request: NativeReplay, context: operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate(group);
        return BoundaryAbi.call("poll_fn", self.boundary_dispatch, self.poll_fn, .{ self.ptr, a, table, group, request, context });
    }
};
pub const Snapshot = struct {
    catalog_revision: u64 = 0,
    resource: local.system_catalog_domain.Resource,
    plan: owner_model.Plan,
    owners: []const owner_model.Owner,
    source_table: ?@import("../metadata/table_manager.zig").TableRecord = null,
};
pub const Authority = enum { native, archive };
pub const max_result_bytes = 32 * 1024 * 1024;
pub const max_input_rows = 128 * 1024;

pub const Result = struct {
    budget: ?*MemoryBudget = null,
    arena: std.heap.ArenaAllocator,
    rows: []const series.Bucket,
    history: dataset.InitialHistory,
    input_rows: usize = 0,
    captures: []const Capture = &.{},
    next: ?dataset.RowKey = null,
    fn allocationFailure(self: Result, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.budget != null and self.budget.?.denied) error.DatasetPageBudgetExceeded else err;
    }
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        if (self.budget) |budget| {
            std.debug.assert(budget.live == 0);
            budget.backing.destroy(budget);
        }
        self.* = undefined;
    }
};

/// Readers implement `readPartition(a, owner, authority, range, context)`.
/// A read failure aborts the complete query. The selected archive authority
/// cannot fall back to an older native copy after metadata has handed it off.
pub fn read(a: A, snapshot: Snapshot, input: Request, context: operation.RequestContext, reader: anytype) !Result {
    return readLimited(a, snapshot, input, context, reader, max_result_bytes);
}
fn readLimited(a: A, snapshot: Snapshot, input: Request, context: operation.RequestContext, reader: anytype, memory_limit: usize) !Result {
    const budget = try a.create(MemoryBudget);
    errdefer a.destroy(budget);
    budget.* = .{ .backing = a, .limit = memory_limit, .total_limit = memory_limit };
    var result = readBudgeted(budget.allocator(), snapshot, input, context, reader) catch |err| {
        std.debug.assert(budget.live == 0);
        return if (err == error.OutOfMemory and budget.denied) error.DatasetPageBudgetExceeded else err;
    };
    result.budget = budget;
    return result;
}
fn readBudgeted(a: A, snapshot: Snapshot, input: Request, context: operation.RequestContext, reader: anytype) !Result {
    const range: dataset.Range = .{ .start_ns = input.start_ns, .end_ns = input.end_ns };
    try range.validate();
    if (input.max_rows == 0 or input.max_rows > max_input_rows) return error.DatasetPageBudgetExceeded;
    try snapshot.plan.validate();
    if (snapshot.plan.phase != .active and snapshot.plan.phase != .closing and snapshot.plan.phase != .closed) return error.RetainedSeriesNotReady;
    if (!snapshot.plan.registration_complete or snapshot.owners.len != snapshot.plan.registered) return error.InvalidRetainedOwnerPlan;
    var descriptor = try local.system_catalog_retained_datasets.parse(a, snapshot.resource.source_json);
    defer descriptor.deinit();
    if (descriptor.value.phase == .retiring) return error.RetainedOwnerPlanChanged;
    if (descriptor.value.phase != .active and descriptor.value.phase != .closing and descriptor.value.phase != .closed) return error.RetainedSeriesNotReady;
    const definition = descriptor.value.definition;
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    if (snapshot.resource.kind != .dataset or snapshot.resource.id != snapshot.plan.resource or definition.generation != snapshot.plan.generation or
        !std.mem.eql(u8, &definition.id, &snapshot.plan.dataset) or !std.mem.eql(u8, &try definition.fingerprint(a), &snapshot.plan.definition)) return error.RetainedOwnerPlanChanged;
    var membership: topology.RangeMembershipAccumulator = .{};
    var prior_group: u64 = 0;
    for (snapshot.owners) |owner| {
        try owner.validate();
        if (owner.group <= prior_group or owner.incarnation != snapshot.plan.generation or !std.mem.eql(u8, &owner.plan, &snapshot.plan.id)) return error.InvalidRetainedOwnerPlan;
        try membership.add(owner.group);
        prior_group = owner.group;
    }
    if (!std.mem.eql(u8, &membership.finish(snapshot.plan.source_table).digest, &snapshot.plan.membership)) return error.RetainedOwnerPlanChanged;

    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    var positions: std.AutoHashMapUnmanaged(dataset.RowKey, usize) = .empty;
    defer positions.deinit(a);
    var rows: std.ArrayList(series.Bucket) = .empty;
    var row_bytes: usize = 0;
    var reducers = std.ArrayList([]series.MetricReducer).empty;
    var visited_rows: usize = 0;
    var more = false;
    var safe_through: ?dataset.RowKey = null;
    const start = try range.start();
    const end = try range.end();
    const width: i128 = summary.interval_ns;
    const offset = @mod(try std.fmt.parseInt(i128, summary.origin_ns, 10), width);
    if (@mod(start, width) != offset or @mod(end, width) != offset) return error.RetainedSeriesResolutionMismatch;
    for (snapshot.owners) |owner| {
        try context.ensureActive();
        const authority: Authority = if (owner.independent_receipt != null) .archive else switch (owner.mode) {
            .ready, .closed => .native,
            else => return error.RetainedSeriesNotReady,
        };
        var part = try reader.readPartition(a, owner, authority, range, context);
        defer part.deinit();
        if (part.rows.len > max_input_rows - visited_rows) return error.DatasetPageBudgetExceeded;
        visited_rows += part.rows.len;
        more = more or part.next != null;
        if (part.next) |key| {
            _ = try parseAfter(&key);
            const timestamp = try dataset.rowKeyTimestamp(key);
            if (timestamp < start or timestamp >= end) return error.RetainedSeriesCorrupt;
            if (input.after) |after| if (std.mem.order(u8, &key, &after) != .gt) return error.RetainedSeriesCorrupt;
            if (safe_through == null or std.mem.order(u8, &key, &safe_through.?) == .lt) safe_through = key;
        }
        var previous: ?dataset.RowKey = null;
        for (part.rows) |row| {
            if (row.start_ns < start or row.start_ns >= end or @mod(row.start_ns, width) != offset or
                row.rows <= 0 or row.dimensions.len > 4096 or row.metrics.len != summary.measures.len) return error.RetainedSeriesCorrupt;
            const key = codec.key(row);
            if (input.after) |after| if (std.mem.order(u8, &key, &after) != .gt) continue;
            if (input.dimensions_json) |dimensions| if (!std.mem.eql(u8, dimensions, row.dimensions)) continue;
            if (previous) |last| if (std.mem.order(u8, &last, &key) != .lt) return error.RetainedSeriesCorrupt;
            previous = key;
            if (positions.get(key)) |index| {
                const merged = &rows.items[index];
                if (!std.mem.eql(u8, merged.dimensions, row.dimensions)) return error.RetainedSeriesCorrupt;
                merged.rows = std.math.add(i64, merged.rows, row.rows) catch return error.RetainedSeriesNumericOverflow;
                for (reducers.items[index], row.metrics) |*target, metric| try target.add(scratch, metric);
            } else {
                if (rows.items.len >= (if (input.page) @as(usize, max_input_rows) else input.max_rows)) return error.DatasetPageBudgetExceeded;
                const cost = @sizeOf(series.Bucket) + row.dimensions.len + series.metricBytes(row.metrics);
                if (cost > max_result_bytes - row_bytes) return error.DatasetPageBudgetExceeded;
                row_bytes += cost;
                try positions.put(a, key, rows.items.len);
                const metrics = try scratch.alloc(series.MetricReducer, row.metrics.len);
                for (metrics, row.metrics) |*target, metric| target.* = try series.MetricReducer.init(scratch, metric);
                try reducers.append(scratch, metrics);
                try rows.append(scratch, .{ .start_ns = row.start_ns, .dimensions = try scratch.dupe(u8, row.dimensions), .rows = row.rows, .metrics = &.{} });
            }
        }
    }
    row_bytes = 0;
    for (rows.items, reducers.items) |*row, metrics| {
        row.metrics = try scratch.alloc(series.Metric, metrics.len);
        for (row.metrics, metrics) |*target, metric| target.* = try metric.finish(scratch);
        const cost = @sizeOf(series.Bucket) + row.dimensions.len + series.metricBytes(row.metrics);
        if (cost > max_result_bytes - row_bytes) return error.DatasetPageBudgetExceeded;
        row_bytes += cost;
    }
    std.mem.sort(series.Bucket, rows.items, {}, struct {
        fn before(_: void, left: series.Bucket, right: series.Bucket) bool {
            return std.mem.order(u8, &codec.key(left), &codec.key(right)) == .lt;
        }
    }.before);
    if (input.page) if (safe_through) |fence| {
        while (rows.items.len > 0 and std.mem.order(u8, &codec.key(rows.items[rows.items.len - 1]), &fence) == .gt) rows.items.len -= 1;
    };
    var next_key: ?dataset.RowKey = if (input.page and rows.items.len == 0) safe_through else null;
    if (input.page and rows.items.len != 0 and (rows.items.len > input.max_rows or more)) {
        rows.items.len = @min(rows.items.len, input.max_rows);
        next_key = codec.key(rows.items[rows.items.len - 1]);
    }
    try context.ensureActive();
    return .{ .arena = arena, .rows = rows.items, .history = summary.initial_history, .input_rows = visited_rows, .next = next_key };
}

/// Capture authority after the caller has authorized the qualified dataset.
pub fn fetchReadCut(server: anytype, a: A, target: local.system_catalog_domain.Target, input: operation.RequestContext) !Snapshot {
    var context = input;
    context.setting_admin = true;
    const bytes = try server.source.systemCatalog(a, context, .{ .retained_dataset_read_cut = target });
    defer a.free(bytes);
    if (bytes.len > 8 * 1024 * 1024) return error.DatasetPageBudgetExceeded;
    const snapshot = try std.json.parseFromSliceLeaky(Snapshot, a, bytes, .{ .allocate = .alloc_always });
    var descriptor = try local.system_catalog_retained_datasets.parse(a, snapshot.resource.source_json);
    defer descriptor.deinit();
    // A detached owner journal can retain its last closed plan. The current
    // resource phase must revoke serving before any archive I/O or replay.
    if (descriptor.value.phase == .retiring) return error.RetainedOwnerPlanChanged;
    if (descriptor.value.phase != .active and descriptor.value.phase != .closing and descriptor.value.phase != .closed) return error.RetainedSeriesNotReady;
    return snapshot;
}

pub fn readResolved(server: anytype, snapshot: Snapshot, definition: dataset.Definition, input: Request, request_context: operation.RequestContext) !Result {
    return readResolvedOwned(server, snapshot, definition, input, request_context, false);
}

/// Complete one certified finite prefix, including short/empty coverage pages.
/// All pages share one deadline and live working-memory/input-work ceiling.
pub fn readResolvedPrefix(server: anytype, snapshot: Snapshot, definition: dataset.Definition, input: Request, request_context: operation.RequestContext) !Result {
    return readResolvedOwned(server, snapshot, definition, input, request_context, true);
}

fn readResolvedOwned(server: anytype, snapshot: Snapshot, definition: dataset.Definition, input: Request, request_context: operation.RequestContext, prefix: bool) !Result {
    // A result owns its arena independently of the HTTP/SQL request arena.
    // Use the reclaiming server allocator so completed partitions release
    // actual memory and admission measures peak usage, not cumulative I/O.
    const backing = server.alloc;
    const budget = try backing.create(MemoryBudget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = max_result_bytes };
    var context = try request_context.platformDeadline();
    context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s);
    const PageReader = struct {
        server: @TypeOf(server),
        snapshot: Snapshot,
        definition: dataset.Definition,
        pub fn page(self: @This(), a: A, request: Request, ctx: operation.RequestContext) !Result {
            return readResolvedBudgeted(self.server, a, self.snapshot, self.definition, request, ctx);
        }
    };
    var result = (if (prefix)
        readPrefixBudgeted(budget.allocator(), input, context, PageReader{ .server = server, .snapshot = snapshot, .definition = definition })
    else
        readResolvedBudgeted(server, budget.allocator(), snapshot, definition, input, context)) catch |err| {
        std.debug.assert(budget.live == 0);
        return if (err == error.OutOfMemory and budget.denied) error.DatasetPageBudgetExceeded else err;
    };
    result.budget = budget;
    return result;
}
fn readPrefixBudgeted(a: A, input: Request, context: operation.RequestContext, reader: anytype) !Result {
    if (!input.page or input.max_rows == 0 or input.max_rows > max_input_rows) return error.DatasetPageBudgetExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var rows: std.ArrayList(series.Bucket) = .empty;
    var captures: std.ArrayList(Capture) = .empty;
    var positions: std.AutoHashMapUnmanaged(owner_model.ArchiveScope, usize) = .empty;
    defer positions.deinit(a);
    var request = input;
    var history: ?dataset.InitialHistory = null;
    var row_bytes: usize = 0;
    var input_rows: usize = 0;
    var next: ?dataset.RowKey = null;
    var previous = input.after;
    var pages: usize = 0;
    var expected_captures: ?usize = null;
    while (rows.items.len < input.max_rows) {
        try context.ensureActive();
        if (pages == 8192) return error.DatasetPageBudgetExceeded;
        pages += 1;
        request.max_rows = input.max_rows - @as(u32, @intCast(rows.items.len));
        var part = try reader.page(a, request, context);
        defer part.deinit();
        if (part.rows.len > request.max_rows or part.input_rows > max_input_rows - input_rows) return error.DatasetPageBudgetExceeded;
        input_rows += part.input_rows;
        if (history) |expected| {
            if (expected != part.history) return error.RetainedOwnerPlanChanged;
        } else history = part.history;
        if (expected_captures) |expected| {
            if (part.captures.len != expected) return error.RetainedReadRestartRequired;
        } else expected_captures = part.captures.len;
        for (part.captures) |capture| {
            const slot = try positions.getOrPut(a, capture.scope);
            if (slot.found_existing) {
                const old = captures.items[slot.value_ptr.*];
                // Never join prefixes observed at different native cuts.
                if (!std.mem.eql(u8, old.table, capture.table) or !std.meta.eql(old.observation, capture.observation)) return error.RetainedReadRestartRequired;
            } else {
                if (captures.items.len == 4096) return error.SqlProgramLimitExceeded;
                slot.value_ptr.* = captures.items.len;
                var copy = capture;
                copy.table = try owned.dupe(u8, capture.table);
                try captures.append(owned, copy);
            }
        }
        if (captures.items.len != expected_captures.?) return error.RetainedReadRestartRequired;
        for (part.rows) |row| {
            const key = codec.key(row);
            if (previous) |last| if (std.mem.order(u8, &key, &last) != .gt) return error.RetainedSeriesCorrupt;
            const cost = @sizeOf(series.Bucket) + row.dimensions.len + series.metricBytes(row.metrics);
            if (cost > max_result_bytes - row_bytes) return error.DatasetPageBudgetExceeded;
            row_bytes += cost;
            try rows.append(owned, .{ .start_ns = row.start_ns, .dimensions = try owned.dupe(u8, row.dimensions), .rows = row.rows, .metrics = try series.ownMetrics(owned, row.metrics) });
            previous = key;
        }
        next = part.next;
        if (next) |key| {
            if (request.after) |last| if (std.mem.order(u8, &key, &last) != .gt) return error.RetainedSeriesCorrupt;
            if (previous) |last| if (std.mem.order(u8, &key, &last) == .lt) return error.RetainedSeriesCorrupt;
            request.after = key;
            previous = key;
        } else break;
    }
    return .{ .arena = arena, .rows = rows.items, .history = history orelse return error.RetainedSeriesCorrupt, .input_rows = input_rows, .captures = captures.items, .next = next };
}

fn readResolvedBudgeted(server: anytype, a: A, snapshot: Snapshot, definition: dataset.Definition, input: Request, request_context: operation.RequestContext) !Result {
    var context = try request_context.platformDeadline();
    const now = @import("antfly_platform").time.monotonicNs();
    context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), now +| 30 * std.time.ns_per_s);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var descriptor = try local.system_catalog_retained_datasets.parse(scratch, snapshot.resource.source_json);
    defer descriptor.deinit();
    if (!std.mem.eql(u8, &try descriptor.value.definition.fingerprint(scratch), &try definition.fingerprint(scratch))) return error.RetainedOwnerPlanChanged;
    const Archive = @import("../serverless/datasets/configured_archive.zig").Opened;
    var archive: ?Archive = null;
    defer if (archive) |*opened| opened.deinit();
    if (snapshot.plan.independent != 0) {
        const config = server.cfg.node_config orelse return error.InvalidDatasetBinding;
        archive = try Archive.init(scratch, config, server.cfg.secret_store, descriptor.value, false);
    }
    const Reader = struct {
        server: @TypeOf(server),
        snapshot: Snapshot,
        definition: dataset.Definition,
        archive: ?*const Archive,
        limit: u32,
        input: Request,
        captures: *std.ArrayList(Capture),
        capture_alloc: A,
        const Part = @import("../serverless/datasets/summary_read.zig").Result;
        pub fn readPartition(self: @This(), alloc: A, owner: owner_model.Owner, authority: Authority, range: dataset.Range, ctx: operation.RequestContext) !Part {
            if (authority == .native) {
                const source = self.snapshot.source_table orelse return error.RetainedOwnerPlanChanged;
                var part_arena = std.heap.ArenaAllocator.init(alloc);
                errdefer part_arena.deinit();
                const temp = part_arena.allocator();
                const scope: owner_model.ArchiveScope = .{ .resource = self.snapshot.plan.resource, .generation = self.snapshot.plan.generation, .plan = self.snapshot.plan.id, .group = owner.group, .revision = owner.revision };
                const raw = try self.server.readRetainedOwnerRows(temp, source.name, owner.group, .{ .scope = scope, .start_ns = range.start_ns, .end_ns = range.end_ns, .limit = self.limit, .after = self.input.after, .dimensions_json = self.input.dimensions_json, .allow_more = self.input.page }, ctx);
                const decoded = try std.json.parseFromSliceLeaky(NativeReply, temp, raw, .{});
                if (decoded.observation) |observed| try self.captures.append(self.capture_alloc, .{ .table = source.name, .scope = scope, .observation = observed });
                return .{ .arena = part_arena, .rows = decoded.rows, .next = decoded.next };
            }
            const opened = self.archive orelse return error.InvalidDatasetBinding;
            const io = self.server.sharedApiIo() orelse return error.BackendRuntimeUnavailable;
            const closed = owner.cut orelse return error.InvalidRetainedOwnerPlan;
            const cut: @import("../serverless/datasets/closure_manifest.zig").Cut = .{ .closure = closed.closure, .sequence = closed.sequence, .complete_from_ns = try std.fmt.parseInt(i128, closed.complete_from_ns, 10), .admitted_ns = try std.fmt.parseInt(i128, closed.admitted_ns, 10) };
            var part_arena = std.heap.ArenaAllocator.init(alloc);
            defer part_arena.deinit();
            const spec = try @import("retained_owner_binding.zig").specification(part_arena.allocator(), self.definition, owner.partition, owner.incarnation);
            const binding = opened.location();
            const Resolver = struct {
                value: @TypeOf(binding),
                pub fn location(resolver: @This(), id: dataset.Id) !@TypeOf(binding) {
                    if (!std.mem.eql(u8, &resolver.value.id, &id)) return error.DatasetLocationUnavailable;
                    return resolver.value;
                }
            };
            return @import("../serverless/datasets/summary_read.zig").readClosed(alloc, opened.storeWithAllocator(alloc, io, &ctx), self.definition, spec, range, .{ .rows = self.limit, .after = self.input.after, .dimensions_json = self.input.dimensions_json, .page = self.input.page }, cut, owner.independent_receipt orelse return error.DatasetDependencyPending, Resolver{ .value = binding });
        }
    };
    if (input.max_rows == 0 or input.max_rows > max_input_rows) return error.DatasetPageBudgetExceeded;
    var captures = std.ArrayList(Capture).empty;
    var result = try readBudgeted(a, snapshot, input, context, Reader{ .server = server, .snapshot = snapshot, .definition = definition, .archive = if (archive) |*opened| opened else null, .limit = if (input.page) @intCast(@min(input.max_rows + 1, @max(@as(usize, 1), max_input_rows / @max(@as(usize, 1), snapshot.owners.len)))) else input.max_rows, .input = input, .captures = &captures, .capture_alloc = scratch });
    errdefer result.deinit();
    const owned = result.arena.allocator();
    const output = owned.dupe(Capture, captures.items) catch |err| return result.allocationFailure(err);
    for (output) |*capture| capture.table = owned.dupe(u8, capture.table) catch |err| return result.allocationFailure(err);
    result.captures = output;
    return result;
}

pub fn observeNative(server: anytype, a: A, table: []const u8, scope: owner_model.ArchiveScope, clock_ns: i128, context: operation.RequestContext) !NativeReply {
    const raw = try server.readRetainedOwnerRows(a, table, scope.group, .{ .scope = scope, .start_ns = "0", .end_ns = "1", .limit = 1, .observe_only = true, .clock_ns = clock_ns }, context);
    const reply = try std.json.parseFromSliceLeaky(NativeReply, a, raw, .{});
    if (reply.observation == null) return error.RetainedReadRestartRequired;
    return reply;
}

pub fn certify(server: anytype, a: A, captures: []const Capture, context: operation.RequestContext) !bool {
    var unique: std.AutoHashMapUnmanaged(owner_model.ArchiveScope, Capture) = .empty;
    defer unique.deinit(a);
    for (captures) |capture| {
        const slot = try unique.getOrPut(a, capture.scope);
        if (slot.found_existing) {
            if (!std.meta.eql(slot.value_ptr.observation, capture.observation)) return false;
        } else slot.value_ptr.* = capture;
    }
    var values = unique.valueIterator();
    while (values.next()) |capture| {
        try context.ensureActive();
        const now = try observeNative(server, a, capture.table, capture.scope, 0, context);
        if (!std.meta.eql(capture.observation, now.observation.?)) return false;
    }
    return true;
}

pub fn queryJson(server: anytype, a: A, target: local.system_catalog_domain.Target, input: QueryRequest, context: operation.RequestContext) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const snapshot = try fetchReadCut(server, scratch, target, context);
    var descriptor = try local.system_catalog_retained_datasets.parse(scratch, snapshot.resource.source_json);
    defer descriptor.deinit();
    const definition = descriptor.value.definition;
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
    if (input.max_rows == 0 or input.max_rows > 4096) return error.DatasetPageBudgetExceeded;
    if (input.after) |raw| {
        const range: dataset.Range = .{ .start_ns = input.start_ns, .end_ns = input.end_ns };
        const timestamp = try dataset.rowKeyTimestamp(try parseAfter(raw));
        if (timestamp < try range.start() or timestamp >= try range.end()) return error.InvalidDatasetCoverage;
    }
    const request: Request = .{ .start_ns = input.start_ns, .end_ns = input.end_ns, .max_rows = input.max_rows, .page = true, .after = if (input.after) |after| try parseAfter(after) else null, .dimensions_json = if (input.dimensions) |values| try canonicalDimensions(scratch, definition, values) else null };
    var result = try readResolved(server, snapshot, definition, request, context);
    defer result.deinit();
    const Row = struct { bucket_start_ns: []const u8, dimensions: []const std.json.Value, values: []const std.json.Value };
    const output = try scratch.alloc(Row, result.rows.len);
    for (result.rows, output) |row, *out| {
        const dimensions = try std.json.parseFromSliceLeaky([]std.json.Value, scratch, row.dimensions, .{ .parse_numbers = false });
        if (dimensions.len != summary.dimensions.len) return error.RetainedSeriesCorrupt;
        const values = try scratch.alloc(std.json.Value, summary.measures.len);
        for (values, summary.measures, row.metrics) |*cell, measure, metric| cell.* = switch (try metric.finalize(measure.law)) {
            .null => .null,
            .integer => |value| .{ .string = try std.fmt.allocPrint(scratch, "{d}", .{value}) },
            .number => |value| .{ .float = value },
        };
        out.* = .{ .bucket_start_ns = try std.fmt.allocPrint(scratch, "{d}", .{row.start_ns}), .dimensions = dimensions, .values = values };
    }
    return std.json.Stringify.valueAlloc(a, .{ .initial_history = result.history, .dimensions = summary.dimensions, .measures = summary.measures, .rows = output, .next = if (result.next) |key| try scratch.dupe(u8, &key) else null }, .{});
}

test "retained owner native controls merge archived and native average states without fallback" {
    const a = std.testing.allocator;
    const cluster = "11111111111111111111111111111111".*;
    const lineage = try local.system_catalog_retained_datasets.Source.fromTable(a, .{ .table_id = @as(u64, 7), .name = "raw", .schema_json = "" }, cluster);
    var schema: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const definition: dataset.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(2), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = lineage.identity(), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "avg_v", .law = .avg, .field = "v" }} } };
    const descriptor: local.system_catalog_retained_datasets.Descriptor = .{ .definition = definition, .source = lineage, .phase = .closing, .catalog_location = @splat(3), .catalog_storage_identity = @splat(4) };
    const json = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(json);
    var membership: topology.RangeMembershipAccumulator = .{};
    try membership.add(11);
    try membership.add(12);
    var plan: owner_model.Plan = .{ .id = @splat(0), .resource = 8, .dataset = definition.id, .generation = 1, .definition = try definition.fingerprint(a), .source = lineage.identity(), .source_table = 7, .topology_generation = 1, .membership = membership.finish(7).digest, .phase = .closing, .registration_after = 12, .registration_complete = true, .registered = 2, .registration_membership = membership, .ready = 2, .stopped = 1, .independent = 1 };
    plan.id = plan.identity();
    const snapshot: Snapshot = .{ .resource = .{ .id = 8, .kind = .dataset, .name = "stats", .source_json = json, .retained = descriptor.identity() }, .plan = plan, .owners = &.{
        .{ .plan = plan.id, .group = 11, .partition = @splat(5), .incarnation = 1, .revision = 2, .mode = .ready, .native_receipt = @splat(7) },
        .{ .plan = plan.id, .group = 12, .partition = @splat(6), .incarnation = 1, .revision = 4, .mode = .closed, .native_receipt = @splat(8), .independent_receipt = @splat(9), .cut = .{ .closure = @splat(10), .sequence = 1, .complete_from_ns = "0", .admitted_ns = "100" } },
    } };
    const Reader = struct {
        archive_available: bool = true,
        native_calls: usize = 0,
        archived_calls: usize = 0,
        pub fn readPartition(self: *@This(), alloc: A, owner: owner_model.Owner, authority: Authority, _: dataset.Range, _: operation.RequestContext) !@import("../serverless/datasets/summary_read.zig").Result {
            if (owner.group == 12) {
                try std.testing.expectEqual(Authority.archive, authority);
                self.archived_calls += 1;
                if (!self.archive_available) return error.DatasetLocationUnavailable;
            } else {
                try std.testing.expectEqual(Authority.native, authority);
                self.native_calls += 1;
            }
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const scratch = arena.allocator();
            const rows = try scratch.alloc(series.Bucket, 1);
            const metric = try scratch.alloc(series.Metric, 1);
            metric[0] = if (owner.group == 11) .{ .count = 1, .sum = 10 } else .{ .count = 3, .sum = 3 };
            rows[0] = .{ .start_ns = 0, .dimensions = "[]", .rows = metric[0].count, .metrics = metric };
            return .{ .arena = arena, .rows = rows };
        }
    };
    var reader: Reader = .{};
    const range: Request = .{ .start_ns = "0", .end_ns = "10" };
    var result = try read(a, snapshot, range, .{}, &reader);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(?f64, 3.25), result.rows[0].metrics[0].value(.avg));
    try std.testing.expect(result.budget.?.peak <= max_result_bytes);
    var bounded_reader: Reader = .{};
    try std.testing.expectError(error.DatasetPageBudgetExceeded, readLimited(a, snapshot, range, .{}, &bounded_reader, 128));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, read(failing.allocator(), snapshot, range, .{}, &bounded_reader));
    reader.archive_available = false;
    try std.testing.expectError(error.DatasetLocationUnavailable, read(a, snapshot, range, .{}, &reader));
    try std.testing.expectEqual(@as(usize, 2), reader.native_calls);
    try std.testing.expectEqual(@as(usize, 2), reader.archived_calls);
    var retired_descriptor = descriptor;
    retired_descriptor.phase = .retiring;
    const retired_json = try std.json.Stringify.valueAlloc(a, retired_descriptor, .{});
    defer a.free(retired_json);
    var retired_snapshot = snapshot;
    retired_snapshot.resource.source_json = retired_json;
    try std.testing.expectError(error.RetainedOwnerPlanChanged, read(a, retired_snapshot, range, .{}, &reader));
    try std.testing.expectEqual(@as(usize, 2), reader.archived_calls);
}

test "retained owner native controls page only through the common owner frontier" {
    const a = std.testing.allocator;
    const cluster = "11111111111111111111111111111111".*;
    const lineage = try local.system_catalog_retained_datasets.Source.fromTable(a, .{ .table_id = @as(u64, 7), .name = "raw", .schema_json = "" }, cluster);
    var schema: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const definition: dataset.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(2), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = lineage.identity(), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "avg_v", .law = .avg, .field = "v" }} } };
    const descriptor: local.system_catalog_retained_datasets.Descriptor = .{ .definition = definition, .source = lineage, .phase = .closing, .catalog_location = @splat(3), .catalog_storage_identity = @splat(4) };
    const json = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(json);
    var membership: topology.RangeMembershipAccumulator = .{};
    try membership.add(11);
    try membership.add(12);
    var plan: owner_model.Plan = .{ .id = @splat(0), .resource = 8, .dataset = definition.id, .generation = 1, .definition = try definition.fingerprint(a), .source = lineage.identity(), .source_table = 7, .topology_generation = 1, .membership = membership.finish(7).digest, .phase = .closing, .registration_after = 12, .registration_complete = true, .registered = 2, .registration_membership = membership, .ready = 2, .stopped = 1, .independent = 1 };
    plan.id = plan.identity();
    const snapshot: Snapshot = .{ .resource = .{ .id = 8, .kind = .dataset, .name = "stats", .source_json = json, .retained = descriptor.identity() }, .plan = plan, .owners = &.{
        .{ .plan = plan.id, .group = 11, .partition = @splat(5), .incarnation = 1, .revision = 2, .mode = .ready, .native_receipt = @splat(7) },
        .{ .plan = plan.id, .group = 12, .partition = @splat(6), .incarnation = 1, .revision = 4, .mode = .closed, .native_receipt = @splat(8), .independent_receipt = @splat(9), .cut = .{ .closure = @splat(10), .sequence = 1, .complete_from_ns = "0", .admitted_ns = "100" } },
    } };
    const Reader = struct {
        pub fn readPartition(_: @This(), alloc: A, owner: owner_model.Owner, _: Authority, _: dataset.Range, _: operation.RequestContext) !@import("../serverless/datasets/summary_read.zig").Result {
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const scratch = arena.allocator();
            const all = try scratch.alloc(series.Bucket, 3);
            const metrics = try scratch.alloc(series.Metric, 1);
            metrics[0] = .{ .count = 1, .sum = 1 };
            for (all, [_][]const u8{ "[1]", "[2]", "[3]" }) |*row, dim| row.* = .{ .start_ns = 0, .dimensions = dim, .rows = 1, .metrics = metrics };
            std.mem.sort(series.Bucket, all, {}, struct {
                fn less(_: void, l: series.Bucket, r: series.Bucket) bool {
                    return std.mem.order(u8, &codec.key(l), &codec.key(r)) == .lt;
                }
            }.less);
            const rows = if (owner.group == 11) all[0..2] else all[1..3];
            return .{ .arena = arena, .rows = rows, .next = codec.key(rows[1]) };
        }
    };
    var page = try read(a, snapshot, .{ .start_ns = "0", .end_ns = "10", .max_rows = 3, .page = true }, .{}, Reader{});
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(i64, 2), page.rows[1].rows);
    try std.testing.expectEqual(codec.key(page.rows[1]), page.next.?);
}

test "retained owner native controls certify duplicate scans once and reject mixed owner observations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const observed: Observation = .{ .root = 7, .source_sequence = 10, .cursor = .{ .definition = @splat(2), .partition = @splat(3), .incarnation = 1, .sequence = 5 }, .admitted_ns = 1000 };
    const capture: Capture = .{ .table = "raw", .scope = .{ .resource = 41, .generation = 1, .plan = @splat(4), .group = 11, .revision = 1 }, .observation = observed };
    const Server = struct {
        value: Observation,
        calls: usize = 0,
        pub fn readRetainedOwnerRows(self: *@This(), alloc: A, _: []const u8, _: u64, input: NativeQuery, _: operation.RequestContext) ![]u8 {
            try std.testing.expect(input.observe_only);
            self.calls += 1;
            return std.json.Stringify.valueAlloc(alloc, NativeReply{ .root_incarnation = "7", .cursor = self.value.cursor, .rows = &.{}, .observation = self.value }, .{});
        }
    };
    var server: Server = .{ .value = observed };
    try std.testing.expect(try certify(&server, a, &.{ capture, capture }, .{}));
    try std.testing.expectEqual(@as(usize, 1), server.calls);
    server.value.source_sequence += 1;
    try std.testing.expect(!try certify(&server, a, &.{capture}, .{}));
    var stale = capture;
    stale.observation.root += 1;
    const before = server.calls;
    try std.testing.expect(!try certify(&server, a, &.{ capture, stale }, .{}));
    try std.testing.expectEqual(before, server.calls);
}

test "retained owner native controls read certification ignores clock changes within an expiry bucket" {
    const spec: series.Spec = .{ .id = @splat(1), .definition = @splat(2), .generation = 1, .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .retention_ns = 1000, .laws = &.{.count} };
    var progress: series.Progress = .{ .definition = spec.definition, .source_incarnation = 1, .source_sequence = 5, .admitted_ns = 1000, .complete_from_ns = 0, .ready = true };
    const first = try Observation.from(7, progress, spec);
    progress.admitted_ns = 1001;
    try std.testing.expectEqualDeep(first, try Observation.from(7, progress, spec));
    progress.admitted_ns = 1060;
    try std.testing.expect(!std.meta.eql(first, try Observation.from(7, progress, spec)));
    progress.admitted_ns = 1000;
    progress.source_sequence += 1;
    try std.testing.expect(!std.meta.eql(first, try Observation.from(7, progress, spec)));
}

test "retained owner native controls exact fanin stays inside one working allocation ceiling" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const lineage = try local.system_catalog_retained_datasets.Source.fromTable(temp, .{ .table_id = 7, .name = "raw", .schema_json = "" }, "11111111111111111111111111111111".*);
    var schema: dataset.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    const definition: dataset.Definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(2), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = lineage.identity(), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "sum_v", .law = .sum, .field = "v" }} } };
    const descriptor: local.system_catalog_retained_datasets.Descriptor = .{ .definition = definition, .source = lineage, .phase = .active, .catalog_location = @splat(3), .catalog_storage_identity = @splat(4) };
    const json = try std.json.Stringify.valueAlloc(temp, descriptor, .{});
    var membership: topology.RangeMembershipAccumulator = .{};
    for (1..97) |group| try membership.add(group);
    var plan: owner_model.Plan = .{ .id = @splat(0), .resource = 8, .dataset = definition.id, .generation = 1, .definition = try definition.fingerprint(temp), .source = lineage.identity(), .source_table = 7, .topology_generation = 1, .membership = membership.finish(7).digest, .phase = .active, .registration_after = 96, .registration_complete = true, .registered = 96, .registration_membership = membership, .ready = 96 };
    plan.id = plan.identity();
    const owners = try temp.alloc(owner_model.Owner, 96);
    for (owners, 1..) |*owner, group| owner.* = .{ .plan = plan.id, .group = group, .partition = @splat(5), .incarnation = 1, .mode = .ready, .native_receipt = @splat(6) };
    const snapshot: Snapshot = .{ .resource = .{ .id = 8, .kind = .dataset, .name = "stats", .source_json = json, .retained = descriptor.identity() }, .plan = plan, .owners = owners };
    const Reader = struct {
        rows: [3]series.Bucket,
        pub fn readPartition(self: *@This(), alloc: A, owner: owner_model.Owner, _: Authority, _: dataset.Range, _: operation.RequestContext) !@import("../serverless/datasets/summary_read.zig").Result {
            const index = (owner.group - 1) % 3;
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = self.rows[index..][0..1] };
        }
    };
    const metrics = try temp.alloc(series.Metric, 3);
    for (metrics, [_]f64{ 0x1p1023, 0x1p-1074, -0x1p1023 }) |*metric, value| metric.* = .{ .count = 1, .exact_sum = try (try series.ExactSum.input(value)).encode(temp) };
    var reader: Reader = .{ .rows = undefined };
    for (&reader.rows, 0..) |*row, index| row.* = .{ .start_ns = 0, .dimensions = "[]", .rows = 1, .metrics = metrics[index..][0..1] };
    var result = try readLimited(a, snapshot, .{ .start_ns = "0", .end_ns = "10" }, .{}, &reader, 64 * 1024);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(f64, 0x1p-1069), (try result.rows[0].metrics[0].finalize(.sum)).number);
    try std.testing.expect(result.budget.?.total <= 64 * 1024);

    const Server = struct {
        alloc: A,
        rows: [3]series.Bucket,
        definition: dataset.Id,
        cfg: struct { node_config: ?*const local.common_config.Config = null, secret_store: ?*local.common_secrets.FileStore = null } = .{},
        pub fn sharedApiIo(_: *@This()) ?std.Io {
            return std.testing.io;
        }
        pub fn readRetainedOwnerRows(self: *@This(), alloc: A, _: []const u8, group: u64, _: NativeQuery, _: operation.RequestContext) ![]u8 {
            const index = (group - 1) % 3;
            return std.json.Stringify.valueAlloc(alloc, NativeReply{ .rows = self.rows[index..][0..1], .root_incarnation = "1", .cursor = .{ .definition = self.definition, .partition = @splat(5), .incarnation = 1, .sequence = 0 } }, .{});
        }
    };
    var server: Server = .{ .alloc = a, .rows = reader.rows, .definition = plan.definition };
    var serving = snapshot;
    serving.source_table = .{ .table_id = 7, .name = "raw" };
    var resolved = try readResolved(&server, serving, definition, .{ .start_ns = "0", .end_ns = "10" }, .{});
    defer resolved.deinit();
    try std.testing.expectEqual(@as(f64, 0x1p-1069), (try resolved.rows[0].metrics[0].finalize(.sum)).number);
    try std.testing.expect(resolved.budget.?.peak <= max_result_bytes);
    // Cumulative partition work exceeds peak usage because temporary buffers
    // return to the reclaiming backing allocator after each owner.
    try std.testing.expect(resolved.budget.?.total > resolved.budget.?.peak);
}

const PrefixTestReader = struct {
    total: usize = 200_000,
    chunk: usize = 256,
    calls: usize = 0,
    consumed: usize = 0,
    empty_first: bool = true,
    native: bool = false,
    change_cut: bool = false,
    omit_capture: bool = false,
    stalled: bool = false,
    work_per_page: ?usize = null,
    pub fn page(self: *@This(), a: A, request: Request, ctx: operation.RequestContext) !Result {
        try ctx.ensureActive();
        if (!request.page) return error.UnexpectedCompleteRead;
        self.calls += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const temp = arena.allocator();
        var captures: []Capture = &.{};
        if (self.native and !(self.omit_capture and self.calls > 1)) {
            captures = try temp.alloc(Capture, 1);
            const sequence: u64 = if (self.change_cut and self.calls > 1) 4 else 3;
            captures[0] = .{ .table = try temp.dupe(u8, "source"), .scope = .{ .resource = 1, .generation = 1, .plan = @splat(1), .group = 11, .revision = 1 }, .observation = .{ .root = 1, .source_sequence = sequence, .admitted_ns = 0, .cursor = .{ .definition = @splat(2), .partition = @splat(3), .incarnation = 1, .sequence = sequence } } };
        }
        if (self.empty_first and self.calls == 1) return .{ .arena = arena, .rows = &.{}, .history = .retained_source_rows, .captures = captures, .next = dataset.rowKey(0, @splat(255)) };
        if (self.stalled) return .{ .arena = arena, .rows = &.{}, .history = .retained_source_rows, .captures = captures, .next = request.after };
        const first: usize = if (request.after) |key| @intCast(try dataset.rowKeyTimestamp(key)) else 0;
        const count = @min(@min(self.chunk, request.max_rows), self.total - first);
        const rows = try temp.alloc(series.Bucket, count);
        for (rows, 0..) |*row, i| {
            const metrics = try temp.alloc(series.Metric, 1);
            metrics[0] = .{ .count = 1 };
            row.* = .{ .start_ns = @intCast(first + i + 1), .dimensions = try temp.dupe(u8, "[]"), .rows = 1, .metrics = metrics };
        }
        self.consumed += count;
        return .{ .arena = arena, .rows = rows, .history = .retained_source_rows, .captures = captures, .input_rows = self.work_per_page orelse count, .next = if (first + count < self.total) codec.key(rows[rows.len - 1]) else null };
    }
};

test "retained owner native controls SQL prefix traverses short empty pages without a complete scan" {
    const a = std.testing.allocator;
    var memory: MemoryBudget = .{ .backing = a, .limit = max_result_bytes };
    defer std.debug.assert(memory.live == 0);
    var reader: PrefixTestReader = .{};
    var result = try readPrefixBudgeted(memory.allocator(), .{ .start_ns = "0", .end_ns = "200001", .max_rows = 10001, .page = true }, .{}, &reader);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 10001), result.rows.len);
    try std.testing.expectEqual(@as(usize, 10001), reader.consumed);
    try std.testing.expectEqual(@as(usize, 41), reader.calls);
    try std.testing.expectEqual(@as(i128, 10001), result.rows[10000].start_ns);
    try std.testing.expectEqual(@as(i64, 1), result.rows[10000].metrics[0].count);
    try std.testing.expectEqualStrings("[]", result.rows[10000].dimensions);
    try std.testing.expect(result.next != null);
    try std.testing.expect(memory.peak < max_result_bytes);
}

test "retained owner native controls SQL prefix preserves one native cut and rejects missing proofs" {
    const a = std.testing.allocator;
    const request: Request = .{ .start_ns = "0", .end_ns = "200001", .max_rows = 1000, .page = true };
    var reader: PrefixTestReader = .{ .native = true };
    var result = try readPrefixBudgeted(a, request, .{}, &reader);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.captures.len);
    try std.testing.expectEqualStrings("source", result.captures[0].table);
    var changed: PrefixTestReader = .{ .native = true, .change_cut = true };
    try std.testing.expectError(error.RetainedReadRestartRequired, readPrefixBudgeted(a, request, .{}, &changed));
    var missing: PrefixTestReader = .{ .native = true, .omit_capture = true };
    try std.testing.expectError(error.RetainedReadRestartRequired, readPrefixBudgeted(a, request, .{}, &missing));
    var work: PrefixTestReader = .{ .work_per_page = max_input_rows };
    try std.testing.expectError(error.DatasetPageBudgetExceeded, readPrefixBudgeted(a, request, .{}, &work));
    var stalled: PrefixTestReader = .{ .stalled = true };
    try std.testing.expectError(error.RetainedSeriesCorrupt, readPrefixBudgeted(a, request, .{}, &stalled));
    var limited: MemoryBudget = .{ .backing = a, .limit = 512 };
    var bounded: PrefixTestReader = .{};
    try std.testing.expectError(error.OutOfMemory, readPrefixBudgeted(limited.allocator(), request, .{}, &bounded));
    try std.testing.expect(limited.denied);
    try std.testing.expectEqual(@as(usize, 0), limited.live);
}
