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
    scope: owner_model.ArchiveScope,
    start_ns: []const u8,
    end_ns: []const u8,
    limit: u32 = 4096,
    after: ?dataset.RowKey = null,
    dimensions_json: ?[]const u8 = null,
    allow_more: bool = false,
    pub fn validate(self: NativeQuery, group: u64) !void {
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
pub const NativeReply = struct { rows: []const series.Bucket, root_incarnation: []const u8, cursor: series.Cursor, next: ?dataset.RowKey = null };
pub const NativeReplay = struct {
    scope: owner_model.ArchiveScope,
    root_incarnation: []const u8,
    cursor: series.Cursor,
    limit: u16 = 8,
    pub fn validate(self: NativeReplay, group: u64) !void {
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
    resource: local.system_catalog_domain.Resource,
    plan: owner_model.Plan,
    owners: []const owner_model.Owner,
    source_table: ?@import("../metadata/table_manager.zig").TableRecord = null,
};
pub const Authority = enum { native, archive };
pub const max_result_bytes = 32 * 1024 * 1024;
pub const max_input_rows = 128 * 1024;

pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    rows: []const series.Bucket,
    history: dataset.InitialHistory,
    next: ?dataset.RowKey = null,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Readers implement `readPartition(a, owner, authority, range, context)`.
/// A read failure aborts the complete query. The selected archive authority
/// cannot fall back to an older native copy after metadata has handed it off.
pub fn read(a: A, snapshot: Snapshot, input: Request, context: operation.RequestContext, reader: anytype) !Result {
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
                for (merged.metrics, row.metrics) |*target, metric| target.* = try target.merge(metric);
            } else {
                if (rows.items.len >= (if (input.page) @as(usize, max_input_rows) else input.max_rows)) return error.DatasetPageBudgetExceeded;
                const cost = @sizeOf(series.Bucket) + row.dimensions.len + row.metrics.len * @sizeOf(series.Metric);
                if (cost > max_result_bytes - row_bytes) return error.DatasetPageBudgetExceeded;
                row_bytes += cost;
                try positions.put(a, key, rows.items.len);
                try rows.append(scratch, .{ .start_ns = row.start_ns, .dimensions = try scratch.dupe(u8, row.dimensions), .rows = row.rows, .metrics = try scratch.dupe(series.Metric, row.metrics) });
            }
        }
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
    return .{ .arena = arena, .rows = rows.items, .history = summary.initial_history, .next = next_key };
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

pub fn readResolved(server: anytype, a: A, snapshot: Snapshot, definition: dataset.Definition, input: Request, request_context: operation.RequestContext) !Result {
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
            return @import("../serverless/datasets/summary_read.zig").readClosed(alloc, opened.store(io, &ctx), self.definition, spec, range, .{ .rows = self.limit, .after = self.input.after, .dimensions_json = self.input.dimensions_json, .page = self.input.page }, cut, owner.independent_receipt orelse return error.DatasetDependencyPending, Resolver{ .value = binding });
        }
    };
    if (input.max_rows == 0 or input.max_rows > max_input_rows) return error.DatasetPageBudgetExceeded;
    return read(a, snapshot, input, context, Reader{ .server = server, .snapshot = snapshot, .definition = definition, .archive = if (archive) |*opened| opened else null, .limit = if (input.page) @intCast(@min(input.max_rows + 1, @max(@as(usize, 1), max_input_rows / @max(@as(usize, 1), snapshot.owners.len)))) else input.max_rows, .input = input });
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
    var result = try readResolved(server, a, snapshot, definition, request, context);
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
