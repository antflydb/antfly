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
const A = std.mem.Allocator;

pub const Request = struct {
    start_ns: []const u8,
    end_ns: []const u8,
    max_rows: u32 = 4096,
};
pub const NativeQuery = struct {
    scope: owner_model.ArchiveScope,
    start_ns: []const u8,
    end_ns: []const u8,
    limit: u16 = 4096,
    pub fn validate(self: NativeQuery, group: u64) !void {
        try (owner_model.Read{ .owner = self.scope }).validate();
        if (self.scope.group != group or self.start_ns.len > 40 or self.end_ns.len > 40 or self.limit == 0 or self.limit > 4096) return error.InvalidRetainedOwnerPlan;
        try (dataset.Range{ .start_ns = self.start_ns, .end_ns = self.end_ns }).validate();
    }
};
pub const NativeReply = struct { rows: []const series.Bucket };
pub const NativePort = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, []const u8, u64, NativeQuery, operation.RequestContext) anyerror![]u8,
    pub fn readRows(self: NativePort, a: A, table: []const u8, group: u64, request: NativeQuery, context: operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate(group);
        return self.read_fn(self.ptr, a, table, group, request, context);
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
    if (input.max_rows == 0 or input.max_rows > 4096) return error.DatasetPageBudgetExceeded;
    try snapshot.plan.validate();
    if (snapshot.plan.phase != .active and snapshot.plan.phase != .closing and snapshot.plan.phase != .closed) return error.RetainedSeriesNotReady;
    if (!snapshot.plan.registration_complete or snapshot.owners.len != snapshot.plan.registered) return error.InvalidRetainedOwnerPlan;
    var descriptor = try local.system_catalog_retained_datasets.parse(a, snapshot.resource.source_json);
    defer descriptor.deinit();
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
        var previous: ?dataset.RowKey = null;
        for (part.rows) |row| {
            if (row.start_ns < start or row.start_ns >= end or @mod(row.start_ns, width) != offset or
                row.rows <= 0 or row.dimensions.len > 4096 or row.metrics.len != summary.measures.len) return error.RetainedSeriesCorrupt;
            const key = codec.key(row);
            if (previous) |last| if (std.mem.order(u8, &last, &key) != .lt) return error.RetainedSeriesCorrupt;
            previous = key;
            if (positions.get(key)) |index| {
                const merged = &rows.items[index];
                if (!std.mem.eql(u8, merged.dimensions, row.dimensions)) return error.RetainedSeriesCorrupt;
                merged.rows = std.math.add(i64, merged.rows, row.rows) catch return error.RetainedSeriesNumericOverflow;
                for (merged.metrics, row.metrics) |*target, metric| target.* = try target.merge(metric);
            } else {
                if (rows.items.len >= input.max_rows) return error.DatasetPageBudgetExceeded;
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
            if (left.start_ns != right.start_ns) return left.start_ns < right.start_ns;
            return std.mem.order(u8, left.dimensions, right.dimensions) == .lt;
        }
    }.before);
    try context.ensureActive();
    return .{ .arena = arena, .rows = rows.items, .history = summary.initial_history };
}

/// Serve one already-authorized qualified dataset range. Raw table authority
/// stays private and is resolved solely from the dataset's metadata cut.
pub fn queryJson(server: anytype, a: A, target: local.system_catalog_domain.Target, input: Request, request_context: operation.RequestContext) ![]u8 {
    var context = request_context;
    context.setting_admin = true;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const bytes = try server.source.systemCatalog(scratch, context, .{ .retained_dataset_read_cut = target });
    if (bytes.len > 8 * 1024 * 1024) return error.DatasetPageBudgetExceeded;
    const snapshot = try std.json.parseFromSliceLeaky(Snapshot, scratch, bytes, .{});
    var descriptor = try local.system_catalog_retained_datasets.parse(scratch, snapshot.resource.source_json);
    defer descriptor.deinit();
    const definition = descriptor.value.definition;
    const summary = definition.summary orelse return error.InvalidDatasetDefinition;
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
        limit: u16,
        const Part = @import("../serverless/datasets/summary_read.zig").Result;
        pub fn readPartition(self: @This(), alloc: A, owner: owner_model.Owner, authority: Authority, range: dataset.Range, ctx: operation.RequestContext) !Part {
            if (authority == .native) {
                const source = self.snapshot.source_table orelse return error.RetainedOwnerPlanChanged;
                var part_arena = std.heap.ArenaAllocator.init(alloc);
                errdefer part_arena.deinit();
                const temp = part_arena.allocator();
                const scope: owner_model.ArchiveScope = .{ .resource = self.snapshot.plan.resource, .generation = self.snapshot.plan.generation, .plan = self.snapshot.plan.id, .group = owner.group, .revision = owner.revision };
                const raw = try self.server.readRetainedOwnerRows(temp, source.name, owner.group, .{ .scope = scope, .start_ns = range.start_ns, .end_ns = range.end_ns, .limit = self.limit }, ctx);
                const decoded = try std.json.parseFromSliceLeaky(NativeReply, temp, raw, .{});
                return .{ .arena = part_arena, .rows = decoded.rows };
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
            return @import("../serverless/datasets/summary_read.zig").readClosed(alloc, opened.store(io, &ctx), self.definition, spec, range, .{ .rows = self.limit }, cut, owner.independent_receipt orelse return error.DatasetDependencyPending, Resolver{ .value = binding });
        }
    };
    if (input.max_rows == 0 or input.max_rows > 4096) return error.DatasetPageBudgetExceeded;
    var result = try read(a, snapshot, input, context, Reader{ .server = server, .snapshot = snapshot, .definition = definition, .archive = if (archive) |*opened| opened else null, .limit = @intCast(input.max_rows) });
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
    return std.json.Stringify.valueAlloc(a, .{ .initial_history = result.history, .dimensions = summary.dimensions, .measures = summary.measures, .rows = output }, .{});
}
