// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded typed proof lanes. A source batch remains borrowed until every
//! child drains; only the coordinator mutates the global uniqueness census.
const std = @import("std");
const local = @import("antfly_local_sources");
const logical = local.storage_rowsource_identity.logical;
const fingerprints = @import("lake_row_fingerprints.zig");
const Recipe = @import("lake_row_input_recipes.zig").Recipe;
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
const Scheduler = local.sql_parallel_scheduler;
const Token = @import("antfly_cancellation").CancellationToken;
pub const Result = struct {
    a: A,
    ids: []Digest,
    inputs: []Digest,
    cohorts: usize,
    pub fn deinit(self: Result) void {
        self.a.free(self.inputs);
        self.a.free(self.ids);
    }
    pub fn row(self: Result, ordinal: usize) []const Digest {
        return self.inputs[ordinal * self.cohorts ..][0..self.cohorts];
    }
};
const Job = struct {
    a: A,
    tuple: local.storage_db_relational_index_keys.TuplePlan,
    namespace: logical.Namespace,
    batch: local.storage_rowsource_types.ColumnBatch,
    recipes: []const Recipe,
    token: Token,
    context: local.serverless_query_lake_read_context.Context,
    begin: usize,
    end: usize,
    output: *const Result,
    fn run(self: *Job) anyerror!void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var bound = try self.tuple.bindExternalBatch(a, self.batch);
        defer bound.deinit();
        const plans = try a.alloc(fingerprints.Plan, self.recipes.len);
        var initialized: usize = 0;
        defer for (plans[0..initialized]) |*plan| plan.deinit();
        for (plans, self.recipes) |*plan, recipe| {
            plan.* = try fingerprints.Plan.bind(a, recipe.digest, self.batch, recipe.columns);
            initialized += 1;
        }
        var kernels = try fingerprints.Kernels.init(a, plans);
        defer kernels.deinit();
        const tiles = try a.alloc([64]Digest, plans.len);
        var key: std.ArrayList(u8) = .empty;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var begin = self.begin;
        while (begin < self.end) {
            try self.token.check();
            try self.context.ensureActive();
            const end = @min(begin + 64, self.end);
            _ = scratch.reset(.retain_capacity);
            try kernels.fill(scratch.allocator(), begin, end - begin, tiles);
            for (begin..end) |row| {
                key.clearRetainingCapacity();
                if (try bound.append(a, &key, row)) return error.NullLogicalRowKey;
                self.output.ids[row] = try logical.keyId(self.namespace, key.items);
                for (tiles, self.output.inputs[row * self.recipes.len ..][0..self.recipes.len]) |tile, *value| value.* = tile[row - begin];
            }
            begin = end;
        }
    }
};

pub fn compute(a: A, tuple: local.storage_db_relational_index_keys.TuplePlan, namespace: logical.Namespace, batch: local.storage_rowsource_types.ColumnBatch, recipes: []const Recipe, token: Token, context: local.serverless_query_lake_read_context.Context, phase: ?*const stores.IoPhase) !Result {
    const ids = try a.alloc(Digest, batch.rowCount());
    errdefer a.free(ids);
    const inputs = try a.alloc(Digest, try std.math.mul(usize, batch.rowCount(), recipes.len));
    errdefer a.free(inputs);
    const result: Result = .{ .a = a, .ids = ids, .inputs = inputs, .cohorts = recipes.len };
    var locked: Scheduler.LockedAllocator = .{ .backing = a };
    var jobs: [4]Job = undefined;
    var tasks: [4]?Scheduler.Task(anyerror!void) = @splat(null);
    // Opaque decoders may have mutable caches. Physical typed vectors and
    // dictionaries are immutable for the batch lease and can be shared.
    const typed = for (batch.columns) |column| {
        if (column.values == .encoded) break false;
    } else true;
    const width: usize = if (phase != null and phase.?.scheduler != null and phase.?.park != null and phase.?.resume_cpu != null and typed)
        @max(1, @min(4, @min(phase.?.scheduler.?.max_workers, batch.rowCount() / 64)))
    else
        1;
    const io = if (phase) |p| p.io else context.io;
    defer if (io) |active| {
        for (tasks) |task| if (task != null) {
            phase.?.park.?(phase.?.ptr);
            break;
        };
        for (&tasks) |*task| if (task.*) |*held| {
            _ = held.cancel(active) catch {};
        };
    };
    var failure: ?anyerror = null;
    for (jobs[0..width], 0..) |*job, lane| {
        job.* = .{ .a = locked.allocator(), .tuple = tuple, .namespace = namespace, .batch = batch, .recipes = recipes, .token = token, .context = context, .begin = batch.rowCount() * lane / width, .end = batch.rowCount() * (lane + 1) / width, .output = &result };
        if (width > 1 and lane + 1 < width) tasks[lane] = phase.?.scheduler.?.submitWithIoReserve(io.?, 2 * 1024 * 1024, Job.run, .{job});
        if (tasks[lane] == null) job.run() catch |err| {
            failure = err;
            break;
        };
    }
    if (width > 1) {
        phase.?.park.?(phase.?.ptr);
        for (&tasks) |*task| if (task.*) |*held| {
            held.await(io.?) catch |err| {
                if (failure == null) failure = err;
            };
            task.* = null;
        };
        try phase.?.resume_cpu.?(phase.?.ptr);
    }
    if (failure) |err| return err;
    return result;
}

test "external lake parallel typed verification preserves proof bytes errors and admission" {
    const a = std.testing.allocator;
    const schema: local.storage_schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "body", .path = "body", .column_type = .string },
    } };
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, schema);
    defer layout.deinit();
    var tuple = try local.storage_db_relational_index_keys.TuplePlan.init(a, schema, &layout, &.{.{ .column = "id" }});
    defer tuple.deinit();
    var ids: [256]i64 = undefined;
    var refs: [256]local.storage_rowsource_types.RowRef = undefined;
    for (&ids, &refs, 0..) |*id, *ref, row| {
        id.* = 9007199254740993 + @as(i64, @intCast(row));
        ref.* = .{ .relational_key = "row" };
    }
    var columns = [_]local.storage_rowsource_types.ColumnVector{
        .{ .name = "id", .values = .{ .i64 = &ids } },
        .{ .name = "body", .values = .{ .bytes = &(@as([256][]const u8, @splat("same text"))) } },
    };
    const batch: local.storage_rowsource_types.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &columns };
    const recipes = [_]Recipe{ .{ .name = "all", .digest = @splat(3), .columns = &.{ "id", "body" } }, .{ .name = "body", .digest = @splat(4), .columns = &.{"body"} } };
    const namespace: logical.Namespace = .{ .incarnation = @splat(1), .adapter = .stable_key, .recipe = tuple.fingerprint };
    const expected = try compute(a, tuple, namespace, batch, &recipes, .none, .{}, null);
    defer expected.deinit();
    for ([_]usize{ 1, 4 }) |workers| {
        var scheduler: Scheduler.Scheduler = .{ .max_workers = workers, .max_io_workers = 1 };
        var execution: Scheduler.Execution = .{ .scheduler = &scheduler, .bytes = 1 };
        try execution.enter(std.testing.io);
        var phase: stores.ExecutionIo = .{ .execution = &execution, .io = std.testing.io };
        const actual = try compute(a, tuple, namespace, batch, &recipes, .none, .{ .io = std.testing.io }, phase.capability());
        defer actual.deinit();
        try std.testing.expectEqualSlices(Digest, expected.ids, actual.ids);
        try std.testing.expectEqualSlices(Digest, expected.inputs, actual.inputs);
        var nulls: [256]u8 = @splat(0);
        nulls[130] = 1;
        columns[0].nulls = .{ .bytes = &nulls };
        try std.testing.expectError(error.NullLogicalRowKey, compute(a, tuple, namespace, batch, &recipes, .none, .{ .io = std.testing.io }, phase.capability()));
        columns[0].nulls = .{};
        try std.testing.expect(execution.active);
        execution.pause();
        try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
        try std.testing.expectEqual(workers > 1, scheduler.peak_workers > 1);
    }
}
