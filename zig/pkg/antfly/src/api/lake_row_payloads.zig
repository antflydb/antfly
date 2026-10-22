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
//! IDs until complete key/value verification authorizes their adoption.
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
/// Validate a producer batch once and size each typed row in one column pass.
/// Selections thereafter inspect only their own bounded positions.
pub const Sizer = struct {
    a: A,
    base: usize = 0,
    sizes: []usize,
    pub fn bind(a: A, batch: rows.ColumnBatch) !Sizer {
        try batch.validate();
        const sizes = try a.alloc(usize, batch.rowCount());
        errdefer a.free(sizes);
        @memset(sizes, 0);
        var base: usize = 0;
        for (batch.columns) |column| {
            base +|= column.name.len +| @sizeOf(rows.ColumnVector);
            for (sizes, 0..) |*size, row| {
                size.* +|= 1 + @sizeOf(rows.RowRef);
                if (column.nulls.isNull(row)) continue;
                size.* +|= switch (column.values) {
                    .bytes, .json => |v| v[row].len +| @sizeOf([]const u8),
                    .dictionary_bytes => |v| v.at(row).len +| @sizeOf([]const u8),
                    .decimal128 => @sizeOf(i128),
                    .i64, .f64, .dictionary_i64, .dictionary_f64 => 8,
                    .bool => 1,
                    .vector_f32 => |v| v[row].len *| @sizeOf(f32) +| @sizeOf([]const f32),
                    .encoded => return error.UnsupportedExternalLakeIndex,
                };
            }
        }
        return .{ .a = a, .base = base, .sizes = sizes };
    }
    pub fn deinit(self: Sizer) void {
        self.a.free(self.sizes);
    }
    pub fn selected(self: Sizer, selection: []const usize) !usize {
        var bytes = self.base;
        for (selection) |row| {
            if (row >= self.sizes.len) return error.InvalidSqlBackendResponse;
            bytes +|= self.sizes[row];
        }
        return bytes;
    }
};
pub fn selectedBytes(batch: rows.ColumnBatch, selection: []const usize) !usize {
    // One-shot callers size a page; repeated row/tile callers retain a Sizer.
    var bytes: usize = 0;
    try batch.validate();
    for (selection) |row| if (row >= batch.rowCount()) return error.InvalidSqlBackendResponse;
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

/// Bounded external clustering rewrites typed payloads into their consumption
/// order. Indirect sorting is deliberately disabled: sorting only offsets would
/// leave every logical window reopening unrelated physical producer pages.
pub const Cluster = struct {
    sort: spill.Sort,
    width: usize,
    count: u64 = 0,
    pub fn init(a: A, manager: *spill.Manager, width: usize) Cluster {
        var sort = spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024);
        sort.allow_indirect = false;
        sort.parallel_runs = false;
        sort.encoded_bytes = true;
        sort.shared_runs = true;
        return .{ .sort = sort, .width = width };
    }
    pub fn deinit(self: *Cluster) void {
        self.sort.deinit();
    }
    pub fn add(self: *Cluster, a: A, source: []const rows.ColumnVector, keys: []const []const u8, selected: []const usize, sizing: Sizer) !void {
        const Datum = local.sql_scalar.Datum;
        if (keys.len != selected.len) return error.InvalidLogicalRowIdentity;
        const sizes = try a.alloc(i64, selected.len);
        for (sizes, selected) |*size, row| size.* = std.math.cast(i64, try sizing.selected(&.{row})) orelse return error.SqlProgramLimitExceeded;
        const Reader = @import("lake_index_build_replay.zig").Replay.CaptureBatch;
        var adapter: Reader = .{ .vectors = source };
        const input: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &adapter, .read = Reader.cell, .read_physical = Reader.physical, .read_identity = Reader.identity, .count = sizing.sizes.len, .width = source.len } };
        const Sized = struct {
            input: local.sql_execution_batch.Batch,
            sizes: []const i64,
            fn cell(raw: *anyopaque, alloc: A, row: usize, column: usize) !Datum {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                if (column == self_.input.width()) return .{ .value = .{ .integer = self_.sizes[row] }, .sql_null = false };
                return self_.input.cell(alloc, row, column);
            }
            fn physical(raw: *anyopaque, alloc: A, column: usize) !?local.sql_execution_batch.PhysicalColumn {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                if (column == self_.input.width()) return .{ .vector = .{ .name = "_capture_bytes", .values = .{ .i64 = self_.sizes } }, .range = .{ .count = self_.sizes.len }, .kind = .integer };
                return self_.input.physicalColumn(alloc, column);
            }
        };
        var sized: Sized = .{ .input = try input.select(a, selected), .sizes = sizes };
        const values: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &sized, .read = Sized.cell, .read_physical = Sized.physical, .count = selected.len, .width = source.len + 1 } };
        const datums = try a.alloc(Datum, keys.len);
        const order = try a.alloc([]const Datum, keys.len);
        const ordinals = try a.alloc(u64, keys.len);
        for (keys, datums, order, ordinals, 0..) |key, *datum, *slice, *ordinal, i| {
            datum.* = .{ .value = .{ .string = key }, .sql_null = false };
            slice.* = datum[0..1];
            ordinal.* = self.count + i;
        }
        try self.sort.addBatch(values, order, ordinals);
        self.count += selected.len;
    }
    pub fn finish(self: *Cluster, payloads: *spill.JoinPayloads, edits: *Mutations) !void {
        const View = struct {
            leases: []const spill.Sort.RowLease,
            fn cell(raw: *anyopaque, _: A, row: usize, column: usize) !local.sql_scalar.Datum {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return self_.leases[row].cell(column);
            }
        };
        var pending: ?spill.Sort.RowLease = null;
        defer if (pending) |lease| lease.release();
        var previous: [32]u8 = undefined;
        var previous_len: usize = 0;
        while (true) {
            var leases: [spill.JoinPayloads.page_rows]spill.Sort.RowLease = undefined;
            var count: usize = 0;
            defer for (leases[0..count]) |lease| lease.release();
            var bytes: usize = 0;
            while (count < leases.len) {
                const lease = if (pending) |value| value else (try self.sort.nextLeased()) orelse break;
                pending = null;
                errdefer lease.release();
                const size = try lease.cell(self.width);
                if (size.sql_null or size.value != .integer or size.value.integer < 0) return error.InvalidSqlSpill;
                const cost: usize = @intCast(size.value.integer);
                if (count != 0 and cost > 8 * 1024 * 1024 -| bytes) {
                    pending = lease;
                    break;
                }
                leases[count] = lease;
                count += 1;
                bytes +|= cost;
            }
            if (count == 0) break;
            var view: View = .{ .leases = leases[0..count] };
            const batch: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &view, .read = View.cell, .count = count, .width = self.width } };
            const offset = try payloads.appendBatch(batch);
            for (leases[0..count], 0..) |lease, row| {
                const key = try lease.keyCell(0);
                if (key.sql_null or key.value != .string or key.value.string.len > previous.len) return error.InvalidLogicalRowIdentity;
                if (previous_len != 0 and std.mem.order(u8, previous[0..previous_len], key.value.string) != .lt) return error.InvalidLogicalRowIdentity;
                previous_len = key.value.string.len;
                @memcpy(previous[0..previous_len], key.value.string);
                const size = try lease.cell(self.width);
                var value: [24]u8 = undefined;
                std.mem.writeInt(u64, value[0..8], offset, .big);
                std.mem.writeInt(u64, value[8..16], row, .big);
                std.mem.writeInt(u64, value[16..24], @intCast(size.value.integer), .big);
                try edits.addRecord(key.value.string, &value);
            }
        }
    }
};

pub const Capture = struct {
    const workspace_bytes = 64 * 1024 * 1024;
    a: A,
    budget: local.sql_memory_budget,
    manager: spill.Manager = undefined,
    payloads: spill.JoinPayloads = undefined,
    cluster: ?Cluster = null,
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
        self.cluster = Cluster.init(ca, &self.manager, columns.len);
        self.route_store = try PrivatePages.beginRun(&self.routes);
        self.edits = Mutations.init(ca, &self.manager, self.route_store);
        self.edits.?.private_pages.phase = store.io_phase;
    }
    fn check(raw: *anyopaque) !void {
        try @as(*Capture, @ptrCast(@alignCast(raw))).context.ensureActive();
    }
    pub fn deinit(self: *Capture) void {
        if (self.cluster) |*cluster| cluster.deinit();
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
    pub fn add(self: *Capture, batch: rows.ColumnBatch, ids: []const [32]u8, store: *stores.ArtifactStore) !void {
        try check(self);
        const a = self.budget.allocator();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ca = arena.allocator();
        if (ids.len != batch.rowCount()) return error.InvalidLogicalRowIdentity;
        const sizing = try Sizer.bind(ca, batch);
        defer sizing.deinit();
        const bound_vectors = try vectors(ca, batch, self.columns, self.specs, self.bound);
        self.bound = true;
        const keys = try ca.alloc([]const u8, ids.len);
        const selected = try ca.alloc(usize, ids.len);
        for (ids, keys, selected, 0..) |*id, *key, *row, i| {
            key.* = id;
            row.* = i;
        }
        var phase = try store.beginIo(1);
        defer phase.abort();
        self.cluster.?.add(ca, bound_vectors, keys, selected, sizing) catch |err| return phase.failed(err);
        try phase.finish();
        self.rows += ids.len;
    }

    pub fn finish(self: *Capture, expected_rows: u64) !void {
        if (self.rows != expected_rows) return error.InvalidLogicalRowIdentity;
        try self.cluster.?.finish(&self.payloads, &self.edits.?);
        self.cluster.?.deinit();
        self.cluster = null;
        self.route_root = try self.edits.?.finish(null);
        if ((if (self.route_root) |root| root.records else 0) != expected_rows) return error.InvalidLogicalRowIdentity;
        self.edits.?.deinit();
        self.edits = null;
        self.routes.phase = null;
        self.complete = true;
    }
};

test "external lake payload sizing validates dictionary once and retains linear row costs" {
    const a = std.testing.allocator;
    var indices: [4096]u32 = @splat(0);
    const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "s", .snapshot_id = "cut" }, .external_span = .{ .first = .{ .source_id = "s", .snapshot_id = "cut", .file_id = "part", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = indices.len }, .columns = &.{.{ .name = "body", .values = .{ .dictionary_bytes = .{ .values = &.{"value"}, .indices = &indices } } }} };
    const sizing = try Sizer.bind(a, batch);
    defer sizing.deinit();
    for ([_][]const usize{ &.{0}, &.{ 4095, 7, 7, 1024 }, &.{} }) |selection| try std.testing.expectEqual(try selectedBytes(batch, selection), try sizing.selected(selection));
    try std.testing.expectError(error.InvalidSqlBackendResponse, sizing.selected(&.{4096}));
    // The retained costs no longer borrow producer validity or dictionary IDs.
    // A whole-batch revalidation here would observe this unrelated bad index.
    indices[4095] = 99;
    const first = try sizing.selected(&.{0});
    for (0..4096) |row| try std.testing.expectEqual(first, try sizing.selected(&.{row}));
    try std.testing.expectError(error.RowSourceDictionaryIndexOutOfBounds, Sizer.bind(a, batch));
}

test "external lake clustered payloads retain exact decimal and raw JSON through allocation failures" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("clustered-payload-ownership");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(37), 1, std.testing.io);
    const Run = struct {
        fn run(alloc: A, artifacts_: *stores.ArtifactStore) !void {
            var provider: @import("lake_index_row_source.zig").Provider = .{ .source = undefined, .context = .{ .io = std.testing.io } };
            const capture = (try Capture.start(alloc, &provider, artifacts_, &.{ "exact", "json" })).?;
            defer capture.deinit();
            const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "source", .snapshot_id = "snapshot" }, .external_span = .{ .first = .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = "part", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = 3 }, .columns = &.{
                .{ .name = "exact", .values = .{ .decimal128 = .{ .precision = 38, .scale = 3, .values = &.{ 9007199254740993123, -9007199254740993123, 0 } } } },
                .{ .name = "json", .values = .{ .json = &.{ "{\"n\":9007199254740993123}", "[null,1.2300]", "null" } } },
            } };
            var ids: [3][32]u8 = @splat(@splat(0));
            ids[0][0] = 3;
            ids[1][0] = 1;
            ids[2][0] = 2;
            try capture.add(batch, &ids, artifacts_);
            try capture.finish(3);
            try std.testing.expectEqual(rows.ColumnKind.decimal128, capture.specs[0].kind);
            try std.testing.expectEqual(rows.ColumnKind.json, capture.specs[1].kind);
            var cursor = try tree.Cursor.init(alloc, capture.route_store, capture.route_root, "", null);
            defer cursor.deinit();
            for ([_]usize{ 1, 2, 0 }) |physical| {
                const entry = (try cursor.next()).?;
                try std.testing.expectEqualSlices(u8, &ids[physical], entry.key);
                const offset = std.mem.readInt(u64, entry.value[0..8], .big);
                const row: usize = @intCast(std.mem.readInt(u64, entry.value[8..16], .big));
                const block = try capture.payloads.read(offset);
                defer block.release();
                const decimal = try block.cell(row, 0);
                try std.testing.expectEqual(batch.columns[0].values.decimal128.values[physical], std.mem.readInt(i128, decimal.value.string[0..16], .big));
                const json = try block.cell(row, 1);
                try std.testing.expectEqualStrings(batch.columns[1].values.json[physical], json.value.string);
            }
            try std.testing.expect((try cursor.next()) == null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Run.run, .{&store});
}
