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

//! Bounded independent native builders consume a shared file-first producer.
const std = @import("std");
const local = @import("antfly_local_sources");
const replay_api = @import("lake_index_build_replay.zig");
const Pipeline = @import("lake_index_build_pipeline.zig");
const aggregates = @import("lake_index_native_aggregates.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Contribution = local.metadata_lake_index_catalog.FileContribution;
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Kind = enum { aggregate, ordered, text, sparse, dense };
pub const Result = struct {
    native: aggregates.BuildResult = .{ .declarations = &.{}, .contributions = &.{} },
    ordered: []const Declared = &.{},
    text: []const Declared = &.{},
    sparse: []const Declared = &.{},
    dense: []const Declared = &.{},
};
pub const Args = struct {
    a: A,
    out: A,
    table: local.common_topology_records.TableRecord,
    store: *stores.ArtifactStore,
    provider: *Provider,
    base: local.serverless_manifest_base_source.BaseSourceDescriptor,
    reusable: []const Declared,
    candidates: []const Declared,
    contributions: []const Contribution,
    index: *@import("lake_index_contributions.zig").Index,
    token: Token,
    scheduler: ?*local.sql_parallel_scheduler.Scheduler = null,
};
const Job = struct {
    args: Args,
    kind: Kind,
    slot: usize,
    provider: Provider,
    source: local.serverless_query_lake_serving.ServingSource,
    store: stores.ArtifactStore,
    pipeline: *Pipeline,
    start: *std.Io.Event,
    admitted: *bool,
    execution: local.sql_parallel_scheduler.Execution,
    result: Result = .{},
    fn check(raw: *anyopaque) !void {
        const self: *Job = @ptrCast(@alignCast(raw));
        try self.args.token.check();
        try self.args.provider.context.ensureActive();
        try self.pipeline.check();
    }
    fn run(self: *Job) anyerror!void {
        defer self.pipeline.complete(self.slot);
        defer self.execution.pause();
        try self.start.wait(self.pipeline.io);
        if (!self.admitted.*) return;
        self.executeAdmitted() catch |err| {
            self.pipeline.stop(err);
            return err;
        };
    }
    fn executeAdmitted(self: *Job) !void {
        self.execution.checkpoint = .{ .ptr = self, .check = check };
        try self.execution.enter(self.pipeline.io);
        try self.execute();
    }
    fn execute(self: *Job) !void {
        self.provider.context.additional_checkpoint = .{ .ptr = self, .check = check };
        self.result = try executeFamily(self.args, self.kind, &self.provider, &self.store);
    }
};
fn executeFamily(args: Args, kind: Kind, provider: *Provider, store: *stores.ArtifactStore) !Result {
    var result: Result = .{};
    const source = provider.source;
    switch (kind) {
        .aggregate => {
            result.native = try aggregates.buildIndexed(args.a, args.out, args.table, source, store, provider, args.token, args.reusable, args.contributions, args.index);
            try provider.checkpointBatch(result.native.declarations);
        },
        .ordered => {
            result.ordered = try @import("lake_index_native_rows.zig").buildIncremental(args.a, args.out, args.table, source, store, provider, args.token, args.reusable, args.candidates);
            for (result.ordered) |declaration| try provider.checkpoint(declaration);
        },
        .text => {
            result.text = try @import("lake_index_native_text.zig").buildIncremental(args.a, args.out, args.table, source, args.base, store, provider, args.token, args.reusable, args.candidates);
            for (result.text) |declaration| try provider.checkpoint(declaration);
        },
        .sparse => result.sparse = try @import("lake_index_native_sparse.zig").buildIncremental(args.a, args.out, args.table, source, args.base, store, provider, args.token, args.reusable, args.candidates),
        .dense => result.dense = try @import("lake_index_native_dense.zig").buildIncremental(args.a, args.out, args.table, source, args.base, store, provider, args.token, args.reusable, args.candidates),
    }
    return result;
}
pub const Unit = struct { kind: Kind, table: local.common_topology_records.TableRecord };
fn allowance(kind: Kind) usize {
    return switch (kind) {
        .aggregate, .ordered => 8 * 1024 * 1024,
        .text => 64 * 1024 * 1024,
        .sparse, .dense => 256 * 1024 * 1024,
    };
}
fn append(comptime T: type, a: A, left: []const T, right: []const T) ![]const T {
    if (right.len == 0) return left;
    if (left.len == 0) return right;
    const joined = try std.mem.concat(a, T, &.{ left, right });
    a.free(left);
    a.free(right);
    return joined;
}
fn merge(a: A, result: *Result, part: Result) !void {
    result.native.declarations = try append(Declared, a, result.native.declarations, part.native.declarations);
    result.native.contributions = try append(Contribution, a, result.native.contributions, part.native.contributions);
    result.ordered = try append(Declared, a, result.ordered, part.ordered);
    result.text = try append(Declared, a, result.text, part.text);
    result.sparse = try append(Declared, a, result.sparse, part.sparse);
    result.dense = try append(Declared, a, result.dense, part.dense);
}
fn projectionForUnits(args: Args, out: A, units: []const Unit) ![]const []const u8 {
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(args.a);
    for (units) |unit| {
        var enabled: [5]bool = @splat(false);
        enabled[@backingInt(unit.kind)] = true;
        var provider = args.provider.*;
        for (try replay_api.columnsForFamilies(args.a, out, unit.table, &provider, args.base, enabled)) |name| try names.put(args.a, name, {});
    }
    const columns = try out.alloc([]const u8, names.count());
    var keys = names.keyIterator();
    for (columns) |*column| column.* = keys.next().?.*;
    return columns;
}
/// Configuration units share files even within the same family. Admission
/// bounds workers, projections and reducer memory independently of index count.
pub fn build(args: Args) !?Result {
    _ = args.provider.replay orelse return null;
    _ = args.provider.context.io orelse return null;
    var arena = std.heap.ArenaAllocator.init(args.a);
    defer arena.deinit();
    const units = try @import("lake_index_build_units.zig").plan(args.a, arena.allocator(), args.table, args.provider.context.io.?);
    if (units.len < 2) return null;
    const pending = try arena.allocator().alloc(bool, units.len);
    @memset(pending, true);
    var remaining = units.len;
    var completed = false;
    var result: Result = .{};
    while (remaining >= 2) {
        var cohort: [Pipeline.width]Unit = undefined;
        var indexes: [Pipeline.width]usize = undefined;
        var size: usize = 0;
        for (units, pending, 0..) |unit, active, index| if (active) {
            cohort[size] = unit;
            indexes[size] = index;
            size += 1;
            if (size == cohort.len) break;
        };
        while (size >= 2) {
            if (try buildCohort(args, cohort[0..size])) |part| {
                try merge(args.out, &result, part);
                for (indexes[0..size]) |index| pending[index] = false;
                remaining -= size;
                completed = true;
                break;
            }
            var heaviest: usize = 0;
            for (cohort[0..size], 0..) |unit, slot| if (allowance(unit.kind) >= allowance(cohort[heaviest].kind)) {
                heaviest = slot;
            };
            size -= 1;
            cohort[heaviest] = cohort[size];
            indexes[heaviest] = indexes[size];
        }
        if (size < 2) break;
    }
    if (!completed) return null;
    for (units, pending) |unit, active| if (active) {
        const replay = args.provider.replay.?;
        const columns = try projectionForUnits(args, arena.allocator(), &.{unit});
        try replay.reset();
        const original = replay.columns;
        replay.columns = columns;
        defer {
            replay.reset() catch unreachable;
            replay.columns = original;
        }
        var single = args;
        single.table = unit.table;
        try merge(args.out, &result, try executeFamily(single, unit.kind, args.provider, args.store));
    };
    return result;
}
fn buildCohort(args_: Args, units: []const Unit) !?Result {
    const replay = args_.provider.replay.?;
    const io = args_.provider.context.io.?;
    const scheduler = args_.scheduler orelse local.sql_parallel_scheduler.global();
    // Reducer/input retention is admitted by the statement working set below.
    // Share the scheduler's CPU execution allowance across this cohort instead
    // of charging every lane the maximum and starving the producer's I/O lane.
    const lanes = @min(units.len, scheduler.max_workers);
    if (lanes < 2) return null;
    const worker_bytes = scheduler.cpuWorkspaceBudget(lanes * 8 * 1024 * 1024) / lanes;
    if (worker_bytes < 1024 * 1024) return null;
    // One input/decode workspace is shared under the source mutex, including
    // speculative reads. Scale the replay cache to actual remaining headroom.
    const decoded = std.math.mul(usize, args_.provider.limits.max_decoded_bytes, 2) catch return null;
    var workspace = std.math.add(usize, args_.provider.limits.max_input_bytes, decoded) catch return null;
    for (units) |unit| workspace = std.math.add(usize, workspace, allowance(unit.kind)) catch return null;
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(args_.a) orelse return null;
    if (workspace >= available or available - workspace < 1024 * 1024) return null;
    const frame_limit = std.math.mul(usize, args_.provider.limits.max_decoded_bytes, 2) catch return null;
    const queue_bytes = std.math.mul(usize, frame_limit, 2) catch return null;
    const buffered = queue_bytes < available - workspace;
    if (buffered) workspace += queue_bytes;
    // A second private cursor pins one first batch, its compressed input and
    // decoder state. Admission covers its entire lifetime, including uploads
    // by consumers of the current file. No speculative archive spool is used.
    const scan_workspace = std.math.add(usize, args_.provider.limits.max_input_bytes, decoded) catch return null;
    const stream_read_ahead = scan_workspace < available - workspace;
    if (stream_read_ahead) workspace += scan_workspace;
    const replay_limit = @min(replay.budget.limit, available - workspace);
    const retained_bytes = std.math.add(usize, workspace, replay_limit) catch return null;
    if (!scheduler.reservePull(units.len, retained_bytes)) return null;
    defer scheduler.releasePull(units.len, retained_bytes);
    var projection_arena = std.heap.ArenaAllocator.init(args_.a);
    defer projection_arena.deinit();
    const projection = try projectionForUnits(args_, projection_arena.allocator(), units);
    try replay.reset();
    const original_columns = replay.columns;
    replay.columns = projection;
    defer replay.columns = original_columns;
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = args_.a };
    var outputs: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = args_.out };
    var args = args_;
    args.a = allocations.allocator();
    args.out = outputs.allocator();
    // Replay/index owners outlive these wrappers. Restore their allocators after
    // every worker and borrowed reader has joined, including failed admission.
    const arena_parent = replay.arena.child_allocator;
    const budget_parent = replay.budget.backing;
    const limit_parent = replay.budget.limit;
    replay.budget.limit = replay_limit;
    const index_io = args.index.io;
    args.index.io = io;
    const index_parent = args.index.a;
    const seed_parent = args.index.seed_arena.child_allocator;
    const index_store_parent = args.index.store.allocator;
    replay.arena.child_allocator = args.a;
    replay.budget.backing = args.a;
    args.index.a = args.a;
    args.index.seed_arena.child_allocator = args.a;
    args.index.store.allocator = args.a;
    defer {
        // Retained replay buffers cannot outlive this allocator admission.
        replay.reset() catch unreachable;
        replay.arena.child_allocator = arena_parent;
        replay.budget.backing = budget_parent;
        replay.budget.limit = limit_parent;
        args.index.io = index_io;
        args.index.a = index_parent;
        args.index.seed_arena.child_allocator = seed_parent;
        args.index.store.allocator = index_store_parent;
    }
    try args.provider.source.ensureInventory(args.provider.context);
    try args.provider.source.ensureCanonicalPlan(args.provider.context);
    // Capture lanes own their mutable reader/version state, just like consumers.
    // Parent authority stays pinned and quiescent until every task has joined.
    var capture_source = try args.provider.source.parallelView(args.a);
    defer capture_source.closeParallelView(args.provider.source);
    var capture_provider = args.provider.*;
    capture_provider.source = &capture_source;
    var ahead_source = try args.provider.source.parallelView(args.a);
    defer ahead_source.closeParallelView(args.provider.source);
    var ahead_provider = args.provider.*;
    ahead_provider.source = &ahead_source;
    const replay_provider = replay.provider;
    replay.provider = &capture_provider;
    defer replay.provider = replay_provider;
    var ahead = replay_api.Replay.init(args.a, &ahead_provider, replay.columns);
    defer ahead.deinit();
    // Two buffers share the original replay memory and disk admission. A large
    // file can always fall back to the existing ordinary streaming source.
    const disk_limit = replay.max_spill_bytes;
    defer replay.max_spill_bytes = disk_limit;
    replay.max_spill_bytes = disk_limit / 2;
    replay.budget.limit = replay_limit / 2;
    ahead.budget.limit = replay_limit / 2;
    ahead.max_spill_bytes = disk_limit / 2;
    var pipeline: Pipeline = .{ .replay = replay, .spare = &ahead, .io = io, .scheduler = args.scheduler, .streaming = true, .stream_read_ahead = stream_read_ahead, .buffered = buffered, .frame_limit = frame_limit };
    defer pipeline.deinit();
    defer replay.reset() catch unreachable;
    ahead.pipeline = &pipeline;
    defer replay.captures += ahead.captures;
    defer replay.read_ahead_hits += pipeline.prefetched;
    defer replay.streamed_batches += pipeline.stream_batches;
    defer replay.retained_batches += pipeline.retained_batches;
    replay.pipeline = &pipeline;
    defer replay.pipeline = null;
    var start: std.Io.Event = .unset;
    var admitted = false;
    var jobs: [Pipeline.width]Job = undefined;
    var initialized: [Pipeline.width]bool = @splat(false);
    defer for (initialized, 0..) |ready, slot| if (ready) jobs[slot].source.closeParallelView(args_.provider.source);
    var tasks: [Pipeline.width]?std.Io.Future(anyerror!void) = @splat(null);
    defer {
        pipeline.stop(error.Canceled);
        start.set(io);
        for (&tasks) |*task| if (task.*) |*running| {
            running.cancel(io) catch {};
            task.* = null;
        };
    }
    for (units, 0..) |unit, slot| {
        var provider = args.provider.*;
        provider.replay_consumer = slot;
        var store = args.store.*;
        store.allocator = args.a;
        var job_args = args;
        job_args.table = unit.table;
        jobs[slot] = .{ .args = job_args, .kind = unit.kind, .slot = slot, .provider = provider, .source = try args.provider.source.parallelView(args.a), .store = store, .pipeline = &pipeline, .start = &start, .admitted = &admitted, .execution = .{ .scheduler = scheduler, .bytes = worker_bytes } };
        initialized[slot] = true;
        jobs[slot].provider.source = &jobs[slot].source;
        pipeline.activate(slot);
        pipeline.executions[slot] = &jobs[slot].execution;
        // Reducers are bounded above by the statement working-set admission.
        // CPU admission is acquired only while executing, never while waiting
        // for the start barrier or a source file/batch.
        tasks[slot] = io.concurrent(Job.run, .{&jobs[slot]}) catch return null;
    }
    admitted = true;
    start.set(io);
    try pipeline.drive(args.a, args.token);
    for (&tasks) |*task| if (task.*) |*running| {
        const result = running.await(io);
        task.* = null;
        try result;
    };
    var result: Result = .{};
    for (units, 0..) |_, slot| try merge(args.out, &result, jobs[slot].result);
    return result;
}

test "external lake file-first publication shares one decode across five native families" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-file-fanout");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 1, 2, 3, 4 }, .field_id = 1 }}, &.{
        .{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = &.{ "alpha", "beta", "alpha", "gamma" } },
        .{ .column_id = "dense", .field_id = 3, .converted_type = 0, .values = &.{ "[1,0]", "[0,1]", "[1,0]", "[0,1]" } },
        .{ .column_id = "sparse", .field_id = 4, .converted_type = 0, .values = &.{ "{\"1\":2}", "{\"2\":3}", "{\"1\":2}", "{\"2\":3}" } },
    });
    defer a.free(bytes);
    for (0..3) |ordinal| {
        const name = try std.fmt.allocPrint(a, "part-{d}.parquet", .{ordinal});
        defer a.free(name);
        var uploaded = try client.putObject("antfly", name, bytes, .{});
        uploaded.deinit(a);
    }
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}],"include_columns":["body"]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}},"body":{{"type":"string"}},"dense":{{"type":"string"}},"sparse":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(path);
    var artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, path);
    defer artifacts.deinit();
    var store = artifacts.artifactStore();
    store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(1) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const navigation_api = @import("lake_source_navigation.zig");
    var navigation = try navigation_api.build(a, out, &store, &provider, null, .none);
    defer navigation.deinit();
    provider.native_delta = navigation.delta;
    const inventory_bytes = try navigation_api.encodeInventory(a, &provider, navigation.root);
    defer a.free(inventory_bytes);
    var inventory = try store.putWithCancellation(inventory_bytes, .none);
    defer inventory.deinit(a);
    const base = try binding.binding.toManifestBaseSource(source.inventory.snapshot_id, inventory.artifact_id);
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema_json, .indexes_json =
        \\{"dense":{"type":"embeddings","external":true,"dimension":2},"sparse":{"type":"embeddings","external":true,"sparse":true},"words":{"type":"full_text","field":"body"},"words2":{"type":"full_text","field":"body"},"stats":{"type":"algebraic","materializations":[{"name":"total","op":"sum","measure":"amount"},{"name":"rows","op":"count"},{"name":"by_body","op":"count","group_by":["body"]}]}}
    };
    const columns = try replay_api.columnsForBuild(a, out, table, &provider, base);
    const light = try replay_api.columnsForFamilies(a, out, table, &provider, base, .{ true, true, true, false, false });
    try std.testing.expectEqual(@as(usize, 2), light.len);
    try std.testing.expectEqualStrings("amount", light[0]);
    try std.testing.expectEqualStrings("body", light[1]);
    var replay = replay_api.Replay.init(a, &provider, columns);
    defer replay.deinit();
    provider.replay = &replay;
    var index: @import("lake_index_contributions.zig").Index = undefined;
    try index.init(a, store, null, .none);
    defer index.deinit();
    var args: Args = .{ .a = a, .out = out, .table = table, .store = &store, .provider = &provider, .base = base, .reusable = &.{}, .candidates = &.{}, .contributions = &.{}, .index = &index, .token = .none };
    // Partial admission must release the first waiting worker before serial
    // fallback; no source capture or checkpoint may have started yet.
    var saturated: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1 };
    args.scheduler = &saturated;
    try std.testing.expect((try build(args)) == null);
    try std.testing.expectEqual(@as(usize, 0), saturated.workers);
    try std.testing.expectEqual(@as(usize, 0), saturated.bytes);
    try std.testing.expectEqual(@as(usize, 0), replay.captures);
    args.scheduler = null;
    var bounded = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = 1024 * 1024 });
    args.a = bounded.allocator();
    try std.testing.expect((try build(args)) == null);
    try std.testing.expectEqual(@as(usize, 0), bounded.live_bytes);
    try std.testing.expect(!bounded.limit_exceeded);
    args.a = a;
    var streaming_scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_bytes = 128 * 1024 * 1024 };
    args.scheduler = &streaming_scheduler;
    const result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(streaming_scheduler.peak_io_workers > 0);
    try std.testing.expect(replay.read_ahead_hits > 0);
    try std.testing.expectEqual(@as(usize, 0), streaming_scheduler.workers);
    // Denied speculative I/O admission preserves one scan per required file.
    var denied_ahead: local.sql_parallel_scheduler.Scheduler = .{ .max_io_workers = 0, .max_bytes = 128 * 1024 * 1024 };
    args.scheduler = &denied_ahead;
    const before_denied = replay.captures;
    const hits_before_denied = replay.read_ahead_hits;
    _ = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), replay.captures - before_denied);
    try std.testing.expectEqual(hits_before_denied, replay.read_ahead_hits);
    args.scheduler = null;
    try std.testing.expectEqual(@as(usize, 6), replay.captures);
    try std.testing.expect(replay.streamed_batches != 0);
    try std.testing.expect(replay.retained_batches != 0);
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
    try std.testing.expectEqual(@as(usize, 3), result.native.declarations.len);
    try std.testing.expectEqual(@as(usize, 1), result.ordered.len);
    try std.testing.expectEqual(@as(usize, 2), result.text.len);
    try std.testing.expectEqual(@as(usize, 1), result.sparse.len);
    try std.testing.expectEqual(@as(usize, 1), result.dense.len);
    const ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, result.ordered[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 12), ordered.page.?.records);
    const text = try @import("lake_index_native_text.zig").loadRootDirectory(out, store, result.text[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u32, 12), text.collection.?.documents);
    // CPU scheduling queues admitted consumers without repeating completed
    // families. All admitted consumers share files through two CPU worker slots.
    var pairs: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 2 };
    args.scheduler = &pairs;
    const prior_captures = replay.captures;
    const paired = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(replay.captures - prior_captures <= 15);
    try std.testing.expectEqual(@as(usize, 0), pairs.workers);
    try std.testing.expectEqual(@as(usize, 1), paired.ordered.len);
    try std.testing.expectEqual(@as(usize, 1), paired.dense.len);
    args.scheduler = null;
    // At 128 MiB, lightweight families still share a small admitted replay.
    var modest = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = 128 * 1024 * 1024 });
    {
        var limited_replay = replay_api.Replay.init(modest.allocator(), &provider, columns);
        defer limited_replay.deinit();
        var limited_index: @import("lake_index_contributions.zig").Index = undefined;
        try limited_index.init(modest.allocator(), store, null, .none);
        defer limited_index.deinit();
        provider.replay = &limited_replay;
        defer provider.replay = &replay;
        args.a = modest.allocator();
        args.index = &limited_index;
        defer {
            args.a = a;
            args.index = &index;
        }
        const limited = (try build(args)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(usize, 1), limited.ordered.len);
        try std.testing.expect(limited_replay.captures < 21);
    }
    try std.testing.expectEqual(@as(usize, 0), modest.live_bytes);
    // At 224 MiB these three light consumers admit the second source cursor
    // while retaining borrowed single-batch backpressure (no two-frame queue).
    var borrowed_budget = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = 224 * 1024 * 1024 });
    {
        var borrowed_provider = provider;
        var borrowed_replay = replay_api.Replay.init(borrowed_budget.allocator(), &borrowed_provider, columns);
        defer borrowed_replay.deinit();
        borrowed_provider.replay = &borrowed_replay;
        var borrowed_index: @import("lake_index_contributions.zig").Index = undefined;
        try borrowed_index.init(borrowed_budget.allocator(), store, null, .none);
        defer borrowed_index.deinit();
        var borrowed_args = args;
        borrowed_args.a = borrowed_budget.allocator();
        borrowed_args.provider = &borrowed_provider;
        borrowed_args.index = &borrowed_index;
        borrowed_args.table.indexes_json = "{\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"total\",\"op\":\"sum\",\"measure\":\"amount\"},{\"name\":\"rows\",\"op\":\"count\"},{\"name\":\"by_body\",\"op\":\"count\",\"group_by\":[\"body\"]}]}}";
        var borrowed_scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_bytes = 128 * 1024 * 1024 };
        borrowed_args.scheduler = &borrowed_scheduler;
        const borrowed = (try build(borrowed_args)) orelse return error.TestUnexpectedResult;
        try std.testing.expect(borrowed_replay.read_ahead_hits > 0);
        try std.testing.expectEqual(@as(usize, 3), borrowed_replay.captures);
        const borrowed_ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, borrowed.ordered[0].artifact, .none, null);
        try std.testing.expectEqual(@as(u64, 12), borrowed_ordered.page.?.records);
        try std.testing.expectEqual(@as(usize, 0), borrowed_scheduler.workers);
    }
    try std.testing.expectEqual(@as(usize, 0), borrowed_budget.live_bytes);
    try std.testing.expect(!borrowed_budget.limit_exceeded);
    // A file larger than replay admission streams ordinarily. Every consumer
    // still receives all rows and no reader pins the failed temporary replay.
    replay.max_spill_bytes = 1;
    replay.ready = false;
    const fallback = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
    const fallback_ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, fallback.ordered[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 12), fallback_ordered.page.?.records);
    const fallback_text = try @import("lake_index_native_text.zig").loadRootDirectory(out, store, fallback.text[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u32, 12), fallback_text.collection.?.documents);
    // The full eight-configuration cohort retains file-first sharing and the
    // default node scheduler still admits the next-file I/O lane.
    var eight = table;
    const extra = ",\"words3\":{\"type\":\"full_text\",\"field\":\"body\"}}";
    eight.indexes_json = try std.mem.concat(out, u8, &.{ table.indexes_json[0 .. table.indexes_json.len - 1], extra });
    var default_scheduler: local.sql_parallel_scheduler.Scheduler = .{};
    args.table = eight;
    args.scheduler = &default_scheduler;
    const captures_before_eight = replay.captures;
    const hits_before_eight = replay.read_ahead_hits;
    const eight_result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), eight_result.text.len);
    try std.testing.expect(default_scheduler.peak_cpu_workers <= default_scheduler.max_workers);
    try std.testing.expect(default_scheduler.peak_io_workers > 0);
    try std.testing.expect(replay.read_ahead_hits > hits_before_eight);
    try std.testing.expectEqual(@as(usize, 3), replay.captures - captures_before_eight);
    try std.testing.expectEqual(@as(usize, 0), default_scheduler.bytes);
    // More configurations than CPU lanes share the same file rendezvous. Use
    // the production shared scheduler and serving cache (parallel column decode),
    // rather than an isolated scheduler which could hide producer starvation.
    replay.max_spill_bytes = 1024 * 1024 * 1024;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    try source.attachCache(&cache, binding.binding, .{ .io = std.testing.io });
    defer {
        source.scanner.shared_reader.?.drain(true);
        a.destroy(source.scanner.shared_reader.?);
        source.scanner.shared_reader = null;
    }
    var nine = eight;
    nine.indexes_json = try std.mem.concat(out, u8, &.{ eight.indexes_json[0 .. eight.indexes_json.len - 1], ",\"words4\":{\"type\":\"full_text\",\"field\":\"body\"}}" });
    args.table = nine;
    args.scheduler = null;
    const captures_before_nine = replay.captures;
    const nine_result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), nine_result.text.len);
    try std.testing.expectEqual(@as(usize, 3), replay.captures - captures_before_nine);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().workers);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().bytes);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().pull_workers);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().pull_bytes);
    try std.testing.expect(local.sql_parallel_scheduler.global().peak_cpu_workers <= local.sql_parallel_scheduler.global().max_workers);
}
