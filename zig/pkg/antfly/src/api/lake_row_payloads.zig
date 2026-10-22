// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Private typed payload capture during stable-key verification. Payloads have
//! physical coordinates until the complete verified logical maps exist.
const std = @import("std");
const local = @import("antfly_local_sources");
const rows = local.storage_rowsource_types;
const spill = local.sql_spill;
const tree = @import("../serverless/graph_segment/page_tree.zig");
const stores = @import("../serverless/artifacts/store.zig");
const PrivatePages = @import("../serverless/graph_segment/page_store.zig").PrivatePages;
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const A = std.mem.Allocator;
pub const Spec = struct { kind: rows.ColumnKind, precision: i32 = 0, scale: i32 = 0 };
pub fn vectors(a: A, batch: rows.ColumnBatch, columns: []const []const u8, specs: []Spec, bound: bool) ![]rows.ColumnVector {
    const result = try a.alloc(rows.ColumnVector, columns.len);
    for (columns, result, specs) |name, *vector, *spec| {
        vector.* = batch.findColumn(name) orelse return error.UnsupportedExternalLakeIndex;
        const kind: rows.ColumnKind = if (vector.values == .decimal128) .decimal128 else vector.kind().logical();
        const current: Spec = if (vector.values == .decimal128) .{ .kind = kind, .precision = vector.values.decimal128.precision, .scale = vector.values.decimal128.scale } else .{ .kind = kind };
        if (bound) {
            if (spec.kind != current.kind or spec.scale != current.scale) return error.UnsupportedExternalLakeIndex;
            spec.precision = @max(spec.precision, current.precision);
        } else spec.* = current;
        if (kind == .vector_f32 or kind == .encoded) return error.UnsupportedExternalLakeIndex;
        if (vector.values == .decimal128) {
            const values = try a.alloc([]const u8, batch.rowCount());
            for (values, 0..) |*value, row| {
                const bytes = try a.alloc(u8, 16);
                std.mem.writeInt(i128, bytes[0..16], if (vector.nulls.isNull(row)) 0 else vector.values.decimal128.values[row], .big);
                value.* = bytes;
            }
            vector.values = .{ .bytes = values };
        }
    }
    return result;
}
pub fn selectedBytes(batch: rows.ColumnBatch, selection: []const usize) !usize {
    try batch.validate();
    for (selection) |row| if (row >= batch.rowCount()) return error.InvalidSqlBackendResponse;
    var bytes: usize = 0;
    for (batch.columns) |column| {
        bytes +|= column.name.len +| @sizeOf(rows.ColumnVector);
        for (selection) |row| {
            bytes +|= 1 + @sizeOf(rows.RowRef);
            if (column.nulls.isNull(row)) continue;
            bytes +|= switch (column.values) {
                .bytes, .json => |v| v[row].len +| @sizeOf([]const u8),
                .dictionary_bytes => |v| v.at(row).len +| @sizeOf([]const u8),
                .decimal128 => @sizeOf(i128),
                .i64, .f64, .dictionary_i64, .dictionary_f64 => 8,
                .bool => 1,
                .vector_f32 => |v| v[row].len *| @sizeOf(f32) +| @sizeOf([]const f32),
                .encoded => return error.UnsupportedSqlExecution,
            };
        }
    }
    return bytes;
}

pub const Capture = struct {
    const workspace_bytes = 64 * 1024 * 1024;
    a: A,
    budget: local.sql_memory_budget,
    manager: spill.Manager = undefined,
    payloads: spill.JoinPayloads = undefined,
    routes: PrivatePages = undefined,
    edits: ?Mutations = null,
    route_store: tree.Store = undefined,
    route_root: ?tree.Ref = null,
    columns: []const []const u8 = &.{},
    specs: []Spec = &.{},
    arena: std.heap.ArenaAllocator = undefined,
    context: local.serverless_query_lake_read_context.Context,
    bound: bool = false,
    complete: bool = false,
    rows: u64 = 0,
    initialized: bool = false,
    pub fn start(a: A, provider: *@import("lake_index_row_source.zig").Provider, store: *stores.ArtifactStore, columns: []const []const u8) !?*Capture {
        if (columns.len == 0 or provider.identity_spill_bytes < 1024 * 1024) return null;
        const scheduler = local.sql_parallel_scheduler.global();
        if (!scheduler.reservePull(0, workspace_bytes)) return null;
        const self = a.create(Capture) catch |err| {
            scheduler.releasePull(0, workspace_bytes);
            return err;
        };
        self.* = .{ .a = a, .budget = .{ .backing = a, .limit = workspace_bytes }, .context = provider.context };
        const ca = self.budget.allocator();
        self.arena = .init(ca);
        self.manager = .{ .alloc = ca, .io = provider.context.io.?, .context = self, .checkpoint = check, .max_bytes = provider.identity_spill_bytes / 2, .async_writes = false };
        var owned = true;
        defer if (owned) self.deinit();
        self.initialize(store, columns) catch |err| {
            if (self.optionalFailure(err)) return null;
            return err;
        };
        owned = false;
        return self;
    }
    fn initialize(self: *Capture, store: *stores.ArtifactStore, columns: []const []const u8) !void {
        const ca = self.budget.allocator();
        const out = self.arena.allocator();
        self.columns = try out.alloc([]const u8, columns.len);
        for (@constCast(self.columns), columns) |*name, source| name.* = try out.dupe(u8, source);
        self.specs = try out.alloc(Spec, columns.len);
        const scope = store.upload_scope.?;
        self.routes = .{ .manager = &self.manager, .domain = scope.domain, .attempt = scope.attempt, .phase = store.io_phase };
        self.payloads = try spill.JoinPayloads.init(&self.manager);
        self.initialized = true;
        self.route_store = try PrivatePages.beginRun(&self.routes);
        self.edits = Mutations.init(ca, &self.manager, self.route_store);
        self.edits.?.private_pages.phase = store.io_phase;
    }
    fn check(raw: *anyopaque) !void {
        try @as(*Capture, @ptrCast(@alignCast(raw))).context.ensureActive();
    }
    pub fn deinit(self: *Capture) void {
        if (self.edits) |*edits| edits.deinit();
        if (self.initialized) {
            self.payloads.close();
            self.routes.deinit();
        }
        self.arena.deinit();
        self.manager.deinit();
        std.debug.assert(self.budget.live == 0);
        local.sql_parallel_scheduler.global().releasePull(0, workspace_bytes);
        self.a.destroy(self);
    }
    /// Allocation/quota/type decline only drops this optional optimization.
    pub fn optionalFailure(self: *Capture, err: anyerror) bool {
        return err == error.SqlProgramLimitExceeded or err == error.UnsupportedExternalLakeIndex or (err == error.OutOfMemory and self.budget.isLocallyExhausted());
    }
    pub fn add(self: *Capture, batch: rows.ColumnBatch, slots: *const std.StringHashMapUnmanaged(u32), store: *stores.ArtifactStore) !void {
        try check(self);
        const a = self.budget.allocator();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ca = arena.allocator();
        const bound_vectors = try vectors(ca, batch, self.columns, self.specs, self.bound);
        self.bound = true;
        var adapter: @import("lake_index_build_replay.zig").Replay.CaptureBatch = .{ .vectors = bound_vectors };
        const Reader = @import("lake_index_build_replay.zig").Replay.CaptureBatch;
        const borrowed: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &adapter, .read = Reader.cell, .read_physical = Reader.physical, .read_identity = Reader.identity, .count = batch.rowCount(), .width = self.columns.len } };
        var begin: usize = 0;
        while (begin < batch.rowCount()) {
            var selection: [spill.JoinPayloads.page_rows]usize = undefined;
            var take = @min(selection.len, batch.rowCount() - begin);
            for (&selection, begin..) |*row, i| row.* = i;
            while (take > 1 and try selectedBytes(batch, selection[0..take]) > 8 * 1024 * 1024) take = @max(1, take / 2);
            var phase = try store.beginIo(1);
            defer phase.abort();
            const offset = self.payloads.appendBatch(try borrowed.select(ca, selection[0..take])) catch |err| return phase.failed(err);
            try phase.finish();
            for (selection[0..take], 0..) |row, ordinal| {
                const ref = batch.rowRef(row);
                if (ref != .external) return error.InvalidLogicalRowIdentity;
                const coordinate: @import("lake_row_identity.zig").logical.Coordinate = .{ .file = slots.get(ref.external.file_id) orelse return error.InvalidLogicalRowIdentity, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal };
                var value: [24]u8 = undefined;
                std.mem.writeInt(u64, value[0..8], offset, .big);
                std.mem.writeInt(u64, value[8..16], ordinal, .big);
                std.mem.writeInt(u64, value[16..24], try selectedBytes(batch, &.{row}), .big);
                const key = coordinate.encode();
                try self.edits.?.addRecord(&key, &value);
                self.rows += 1;
            }
            begin += take;
        }
    }
    pub fn finish(self: *Capture, expected_rows: u64) !void {
        if (self.rows != expected_rows) return error.InvalidLogicalRowIdentity;
        self.route_root = try self.edits.?.finish(null);
        self.edits.?.deinit();
        self.edits = null;
        self.routes.phase = null;
        self.complete = true;
    }
};
