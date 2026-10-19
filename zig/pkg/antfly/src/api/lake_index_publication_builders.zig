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
const Kind = enum { aggregate, ordered, text, sparse, dense };
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
    parent_hook: @FieldType(local.serverless_query_lake_read_context.Context, "additional_checkpoint"),
    result: Result = .{},
    fn check(raw: *anyopaque) !void {
        const self: *Job = @ptrCast(@alignCast(raw));
        if (self.parent_hook) |hook| try hook.check(hook.ptr);
        try self.pipeline.check();
    }
    fn run(self: *Job) anyerror!void {
        defer self.pipeline.complete(self.slot);
        try self.start.wait(self.pipeline.io);
        if (!self.admitted.*) return;
        self.execute() catch |err| {
            self.pipeline.stop(err);
            return err;
        };
    }
    fn execute(self: *Job) !void {
        self.provider.context.additional_checkpoint = .{ .ptr = self, .check = check };
        const args = self.args;
        const source = self.provider.source;
        switch (self.kind) {
            .aggregate => {
                self.result.native = try aggregates.buildIndexed(args.a, args.out, args.table, source, &self.store, &self.provider, args.token, args.reusable, args.contributions, args.index);
                try self.provider.checkpointBatch(self.result.native.declarations);
            },
            .ordered => {
                self.result.ordered = try @import("lake_index_native_rows.zig").buildIncremental(args.a, args.out, args.table, source, &self.store, &self.provider, args.token, args.reusable, args.candidates);
                for (self.result.ordered) |declaration| try self.provider.checkpoint(declaration);
            },
            .text => {
                self.result.text = try @import("lake_index_native_text.zig").buildIncremental(args.a, args.out, args.table, source, args.base, &self.store, &self.provider, args.token, args.reusable, args.candidates);
                for (self.result.text) |declaration| try self.provider.checkpoint(declaration);
            },
            .sparse => self.result.sparse = try @import("lake_index_native_sparse.zig").buildIncremental(args.a, args.out, args.table, source, args.base, &self.store, &self.provider, args.token, args.reusable, args.candidates),
            .dense => self.result.dense = try @import("lake_index_native_dense.zig").buildIncremental(args.a, args.out, args.table, source, args.base, &self.store, &self.provider, args.token, args.reusable, args.candidates),
        }
    }
};
/// Admission is all-or-nothing before starting work. Saturation retains the
/// serial builder path; it cannot strand a rendezvous behind an inline task.
pub fn build(args_: Args) !?Result {
    const replay = args_.provider.replay orelse return null;
    const io = args_.provider.context.io orelse return null;
    var enabled: [Pipeline.width]bool = @splat(false);
    const rebuild = @import("../serverless/build/lake_rebuild.zig");
    var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(args_.a, args_.base, args_.provider.source.inventory, .{ .table_name = args_.table.name, .schema_json = args_.table.schema_json, .indexes_json = args_.table.indexes_json });
    defer desired.deinit(args_.a);
    for (desired.artifacts) |artifact| switch (artifact.kind) {
        .algebraic_segment => enabled[@backingInt(Kind.aggregate)] = true,
        .text_segment => enabled[@backingInt(Kind.text)] = true,
        .sparse_segment => enabled[@backingInt(Kind.sparse)] = true,
        .vector_segment => enabled[@backingInt(Kind.dense)] = true,
        else => {},
    };
    var parsed = try local.schema_mod.parseValidatedTableSchema(args_.a, args_.table.schema_json);
    defer parsed.deinit(args_.a);
    var planning = std.heap.ArenaAllocator.init(args_.a);
    defer planning.deinit();
    if (try parsed.relationalIndexDefinitions(planning.allocator())) |definitions| {
        enabled[@backingInt(Kind.ordered)] = definitions.len != 0;
    }
    var count: usize = 0;
    for (enabled) |active| count += @intFromBool(active);
    if (count < 2) return null;
    // Concurrent construction must fit the same publication working set as
    // serial construction. Decline before mutating checkpoints when headroom
    // cannot cover replay plus conservative per-family transient allowances.
    var workspace: usize = replay.budget.limit;
    for (enabled, 0..) |active, slot| if (active) {
        workspace += switch (@as(Kind, @fromBackingInt(@intCast(slot)))) {
            .aggregate, .ordered => 8 * 1024 * 1024,
            .text => 64 * 1024 * 1024,
            .sparse, .dense => 256 * 1024 * 1024,
        };
    };
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(args_.a) orelse return null;
    if (workspace > available) return null;
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = args_.a };
    var outputs: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = args_.out };
    var args = args_;
    args.a = allocations.allocator();
    args.out = outputs.allocator();
    // Replay/index owners outlive these wrappers. Restore their allocators after
    // every worker and borrowed reader has joined, including failed admission.
    const arena_parent = replay.arena.child_allocator;
    const budget_parent = replay.budget.backing;
    const index_parent = args.index.a;
    const seed_parent = args.index.seed_arena.child_allocator;
    const index_store_parent = args.index.store.allocator;
    replay.arena.child_allocator = args.a;
    replay.budget.backing = args.a;
    args.index.a = args.a;
    args.index.seed_arena.child_allocator = args.a;
    args.index.store.allocator = args.a;
    defer {
        replay.arena.child_allocator = arena_parent;
        replay.budget.backing = budget_parent;
        args.index.a = index_parent;
        args.index.seed_arena.child_allocator = seed_parent;
        args.index.store.allocator = index_store_parent;
    }
    var pipeline: Pipeline = .{ .replay = replay, .io = io };
    replay.pipeline = &pipeline;
    defer replay.pipeline = null;
    var start: std.Io.Event = .unset;
    var admitted = false;
    var jobs: [Pipeline.width]Job = undefined;
    var initialized: [Pipeline.width]bool = @splat(false);
    defer for (initialized, 0..) |ready, slot| if (ready) jobs[slot].source.closeParallelView(args_.provider.source);
    var tasks: [Pipeline.width]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
    defer {
        pipeline.stop(error.Canceled);
        start.set(io);
        for (&tasks) |*task| if (task.*) |*running| if (running.future != null) {
            running.cancel(io) catch {};
        };
    }
    try args.provider.source.ensureInventory(args.provider.context);
    try args.provider.source.ensureCanonicalPlan(args.provider.context);
    for (enabled, 0..) |active, slot| if (active) {
        var provider = args.provider.*;
        provider.replay_consumer = slot;
        var store = args.store.*;
        store.allocator = args.a;
        jobs[slot] = .{ .args = args, .kind = @fromBackingInt(@intCast(slot)), .slot = slot, .provider = provider, .source = try args.provider.source.parallelView(args.a), .store = store, .pipeline = &pipeline, .start = &start, .admitted = &admitted, .parent_hook = provider.context.additional_checkpoint };
        initialized[slot] = true;
        jobs[slot].provider.source = &jobs[slot].source;
        pipeline.activate(slot);
        tasks[slot] = (args.scheduler orelse local.sql_parallel_scheduler.global()).submit(io, 8 * 1024 * 1024, Job.run, .{&jobs[slot]});
        if (tasks[slot] == null) return null;
    };
    admitted = true;
    start.set(io);
    try pipeline.drive(args.a, args.token);
    for (&tasks) |*task| if (task.*) |*running| try running.await(io);
    var result: Result = .{};
    for (enabled, 0..) |active, slot| if (active) {
        switch (jobs[slot].kind) {
            .aggregate => result.native = jobs[slot].result.native,
            .ordered => result.ordered = jobs[slot].result.ordered,
            .text => result.text = jobs[slot].result.text,
            .sparse => result.sparse = jobs[slot].result.sparse,
            .dense => result.dense = jobs[slot].result.dense,
        }
    };
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
        \\{"dense":{"type":"embeddings","external":true,"dimension":2},"sparse":{"type":"embeddings","external":true,"sparse":true},"words":{"type":"full_text","field":"body"},"stats":{"type":"algebraic","materializations":[{"name":"total","op":"sum","measure":"amount"},{"name":"rows","op":"count"}]}}
    };
    const columns = try replay_api.columnsForBuild(a, out, table, &provider, base);
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
    const result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), replay.captures);
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
    try std.testing.expectEqual(@as(usize, 2), result.native.declarations.len);
    try std.testing.expectEqual(@as(usize, 1), result.ordered.len);
    try std.testing.expectEqual(@as(usize, 1), result.text.len);
    try std.testing.expectEqual(@as(usize, 1), result.sparse.len);
    try std.testing.expectEqual(@as(usize, 1), result.dense.len);
    const ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, result.ordered[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 12), ordered.page.?.records);
    const text = try @import("lake_index_native_text.zig").loadRootDirectory(out, store, result.text[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u32, 12), text.collection.?.documents);
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
}
