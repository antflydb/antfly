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
const cloneMetadata = @import("lake_index_metadata_ownership.zig").clone;
pub const Kind = enum { aggregate, ordered, text, sparse, dense };
pub const Result = struct {
    /// Complete attempt-private contribution directory; installed only when
    /// all file frontiers finish and the publication owner adopts the result.
    contribution_root: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
    contribution_roots: []const [32]u8 = &.{},
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
    unindexed_aggregates: bool = false,
};
/// One capability per serial worker execution. Retained-state reservation
/// outlives both CPU and I/O phases; no shared backend stores this pointer.
const PublicationIo = struct {
    execution: *local.sql_parallel_scheduler.Execution,
    io: std.Io,
    pending: ?local.sql_parallel_scheduler.Execution.Io = null,
    bridge: stores.IoPhase = undefined,
    fn capability(self: *PublicationIo) ?*const stores.IoPhase {
        // Disabling I/O lanes retains the established required-inline fallback.
        if (self.execution.scheduler.max_io_workers == 0) return null;
        self.bridge = .{ .io = self.io, .ptr = self, .max_parallel = @min(8, self.execution.scheduler.max_io_workers), .scheduler = self.execution.scheduler, .begin = begin, .finish = finish, .abort = abort };
        return &self.bridge;
    }
    fn begin(raw: *anyopaque, count: usize) !void {
        const self: *PublicationIo = @ptrCast(@alignCast(raw));
        std.debug.assert(self.pending == null);
        self.pending = try self.execution.beginIo(self.io, count);
    }
    fn finish(raw: *anyopaque) !void {
        const self: *PublicationIo = @ptrCast(@alignCast(raw));
        var pending = self.pending.?;
        self.pending = null;
        try pending.finish();
    }
    fn abort(raw: *anyopaque) void {
        const self: *PublicationIo = @ptrCast(@alignCast(raw));
        if (self.pending) |*pending| pending.abort();
        self.pending = null;
    }
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
    publication_io: PublicationIo = undefined,
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
        self.publication_io = .{ .execution = &self.execution, .io = self.pipeline.io };
        self.store.io_phase = self.publication_io.capability();
        self.provider.io_phase = self.store.io_phase;
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
            result.native = try aggregates.buildIndexed(args.a, args.out, args.table, source, store, provider, args.token, args.reusable, args.contributions, if (args.unindexed_aggregates) null else args.index);
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
pub const Unit = struct { kind: Kind, table: local.common_topology_records.TableRecord, incremental: bool = true };
fn allowance(kind: Kind) usize {
    return switch (kind) {
        .aggregate => 22 * 1024 * 1024,
        .ordered => 8 * 1024 * 1024,
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
    if (part.contribution_root) |root| {
        if (result.contribution_root != null) return error.InvalidLakeIndexCatalog;
        result.contribution_root = root;
        result.contribution_roots = part.contribution_roots;
    }
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
    const scheduler = args.scheduler orelse local.sql_parallel_scheduler.global();
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(args.a) orelse return null;
    var desired = args.provider.limits.max_input_bytes +| args.provider.limits.max_decoded_bytes *| 2;
    for (units) |unit| desired +|= allowance(unit.kind);
    const pending = try arena.allocator().alloc(bool, units.len);
    @memset(pending, true);
    var remaining = units.len;
    var completed = false;
    var result: Result = .{};
    if (units.len > Pipeline.width or desired +| (1024 * 1024) > @min(available, scheduler.max_pull_bytes)) {
        try args.provider.source.ensureInventory(args.provider.context);
        const source = args.provider.source;
        const deletes = source.inventory.deleted_row_groups.len != 0 or (if (source.scanner.iceberg_delete_plan) |plan| plan.files.len != 0 else false);
        var eligible: std.ArrayList(Unit) = .empty;
        var positions: std.ArrayList(usize) = .empty;
        for (units, 0..) |unit, index| if (unit.incremental and !(unit.kind == .aggregate and deletes)) {
            try eligible.append(arena.allocator(), unit);
            try positions.append(arena.allocator(), index);
        };
        // Independent fallbacks must not force compatible text/vector/ordered
        // builders to repeat the archive scan for every retained-state cohort.
        if (eligible.items.len >= 2) if (try buildFileMajor(args, eligible.items)) |part| {
            try merge(args.out, &result, part);
            for (positions.items) |index| pending[index] = false;
            remaining -= positions.items.len;
            completed = true;
        };
    }
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
const FileCheckpoint = struct { arena: ?std.heap.ArenaAllocator = null, result: Result = .{}, work: Provider.ResumeWork = .{} };
const FileJob = struct {
    args: Args,
    unit: Unit,
    state: *FileCheckpoint,
    failure: *std.atomic.Value(u16),
    provider: Provider,
    source: local.serverless_query_lake_serving.ServingSource,
    store: stores.ArtifactStore,
    execution: local.sql_parallel_scheduler.Execution,
    publication_io: PublicationIo = undefined,
    fn check(raw: *anyopaque) !void {
        const self: *FileJob = @ptrCast(@alignCast(raw));
        const code = self.failure.load(.acquire);
        if (code != 0) return @errorFromInt(code);
        try self.args.token.check();
        try self.args.provider.context.ensureActive();
    }
    fn run(self: *FileJob, io: std.Io) anyerror!void {
        return self.execute(io) catch |err| {
            _ = self.failure.cmpxchgStrong(0, @intFromError(err), .release, .monotonic);
            return err;
        };
    }
    fn execute(self: *FileJob, io: std.Io) anyerror!void {
        self.provider.context.additional_checkpoint = .{ .ptr = self, .check = check };
        self.execution.checkpoint = .{ .ptr = self, .check = check };
        defer self.execution.pause();
        try self.execution.enter(io);
        self.publication_io = .{ .execution = &self.execution, .io = io };
        self.store.io_phase = self.publication_io.capability();
        self.provider.io_phase = self.store.io_phase;
        var next = std.heap.ArenaAllocator.init(self.args.a);
        defer next.deinit();
        var stage = self.args;
        stage.out = next.allocator();
        stage.table = self.unit.table;
        stage.reusable = &.{};
        stage.contributions = &.{};
        stage.candidates = switch (self.unit.kind) {
            .aggregate => &.{},
            .ordered => self.state.result.ordered,
            .text => self.state.result.text,
            .sparse => self.state.result.sparse,
            .dense => self.state.result.dense,
        };
        const borrowed = try executeFamily(stage, self.unit.kind, &self.provider, &self.store);
        // Reused refs can borrow the old checkpoint. Own them before either
        // producer arena retires, without retaining every historical prefix.
        var owned = std.heap.ArenaAllocator.init(self.args.a);
        errdefer owned.deinit();
        const result = try cloneMetadata(Result, owned.allocator(), borrowed);
        if (self.state.arena) |*previous| previous.deinit();
        self.state.arena = owned;
        self.state.result = result;
        const declarations = switch (self.unit.kind) {
            .text => result.text,
            .ordered => result.ordered,
            .sparse => result.sparse,
            .dense => result.dense,
            .aggregate => result.native.declarations,
        };
        if (declarations.len != 0) if (self.store.upload_scope) |scope| {
            self.state.work.private = .{ .ref = declarations[0].artifact, .more = declarations[1..], .scope = scope, .store_ptr = self.store.ptr, .store_vtable = self.store.vtable };
        };
    }
};

/// Cold publications whose configurations cannot reside together advance
/// private immutable checkpoints over adaptive windows of up to 64 files.
/// Each family stays open through its admitted window and releases working
/// state after checkpointing.
/// At most two projected windows share the existing replay memory/disk quota.
/// No prefix checkpoint is exposed to recovery or serving as a complete root.
fn buildFileMajor(args: Args, units: []const Unit) !?Result {
    if (args.reusable.len != 0 or args.candidates.len != 0 or args.contributions.len != 0 or args.index.root != null) return null;
    for (units) |unit| if (!unit.incremental) return null;
    const parent = args.provider.source;
    try parent.ensureInventory(args.provider.context);
    for (units) |unit| if (unit.kind == .aggregate) {
        if (parent.inventory.deleted_row_groups.len != 0 or (if (parent.scanner.iceberg_delete_plan) |plan| plan.files.len != 0 else false)) return null;
    };
    if (parent.inventory.files.len < 2) return null;
    const scheduler = args.scheduler orelse local.sql_parallel_scheduler.global();
    const io = args.provider.context.io.?;
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(args.a) orelse return null;
    var maximum: usize = 0;
    var all: usize = 0;
    for (units) |unit| {
        maximum = @max(maximum, allowance(unit.kind));
        all +|= allowance(unit.kind);
    }
    const lanes: usize = @min(8, units.len, scheduler.max_workers, scheduler.max_pull_workers);
    if (lanes == 0) return null;
    const worker_bytes = scheduler.cpuWorkspaceBudget(lanes * 8 * 1024 * 1024) / lanes;
    if (worker_bytes < 1024 * 1024) return null;
    const Window = @import("lake_index_build_window.zig");
    const replay = args.provider.replay.?;
    const replay_bytes = @min(replay.budget.limit, 16 * 1024 * 1024);
    const scan_workspace = args.provider.limits.max_input_bytes +| args.provider.limits.max_decoded_bytes *| 2;
    const required_fixed = scan_workspace +| replay_bytes +| (2 * @sizeOf(Window));
    const retained = scheduler.reservePullUpTo(lanes, required_fixed +| maximum, @min(available, required_fixed +| all +| scan_workspace)) orelse return null;
    const ahead = retained - required_fixed - maximum >= scan_workspace;
    const fixed = required_fixed + if (ahead) scan_workspace else 0;
    defer scheduler.releasePull(lanes, retained);
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = args.a };
    const a = allocations.allocator();
    try replay.reset();
    const original_columns = replay.columns;
    const original_budget = replay.budget.limit;
    const original_backing = replay.budget.backing;
    const original_arena = replay.arena.child_allocator;
    replay.budget.backing = a;
    replay.arena.child_allocator = a;
    defer {
        replay.reset() catch unreachable;
        replay.columns = original_columns;
        replay.budget.limit = original_budget;
        replay.budget.backing = original_backing;
        replay.arena.child_allocator = original_arena;
    }
    var control = std.heap.ArenaAllocator.init(a);
    defer control.deinit();
    replay.columns = try projectionForUnits(args, control.allocator(), units);
    replay.budget.limit = replay_bytes;
    var private_index: @import("lake_index_contributions.zig").Index = undefined;
    try private_index.init(a, args.store.*, null, args.token);
    defer private_index.deinit();
    private_index.counted = true;
    private_index.io = io;
    var prior_roots: []local.metadata_lake_index_catalog.Digest = &.{};
    defer a.free(prior_roots);
    var positions: local.serverless_query_lake_inventory_catalog.Positions = .{ .catalog = parent.catalog() };
    defer positions.deinit(a);
    if (positions.catalog == null) for (parent.inventory.files, 0..) |entry, ordinal| try positions.put(a, entry.file_id, ordinal);
    var public: @import("lake_index_ordered_files.zig").Public = .{ .identity_version = parent.inventory.row_identity_version, .root = null, .count = 0, .source = parent.inventory.source_id, .snapshot = parent.inventory.snapshot_id };
    var navigation_reads: u64 = 512 * 1024 * 1024;
    var navigation_writes: u64 = 512 * 1024 * 1024;
    var navigation_pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = args.store.upload_scope.?.domain, .attempt = args.store.upload_scope.?.attempt, .artifacts = args.store, .cancellation = args.token, .remaining_read_bytes = &navigation_reads, .remaining_write_bytes = &navigation_writes, .streaming = true };
    const states = try a.alloc(FileCheckpoint, units.len);
    defer a.free(states);
    @memset(states, .{});
    defer for (states) |*state| if (state.arena) |*arena| arena.deinit();
    const capture: local.serverless_segment_source_binding.Binding = .{
        .source_kind = switch (parent.inventory.format) {
            .parquet => .external_parquet,
            .iceberg => .external_iceberg,
            .lance => return null,
        },
        .row_ref_kind = .external,
        .source_id = parent.inventory.source_id,
        .snapshot_id = parent.inventory.snapshot_id,
        .schema_fingerprint = parent.inventory.schema_fingerprint,
        .sidecar_kind = .text,
        .index_config_hash = "file-major-replay",
        .column_bindings = replay.columns,
    };
    const windows = try a.alloc(Window, 2);
    defer a.free(windows);
    var window_count: usize = 0;
    defer for (windows[0..window_count]) |*window| window.deinit(replay);
    for (0..@as(usize, if (ahead) 2 else 1)) |index| {
        try windows[index].init(a, args.provider, capture, replay_bytes / (if (ahead) @as(usize, 2) else 1), replay.max_spill_bytes / (if (ahead) @as(u64, 2) else 1));
        window_count += 1;
    }
    var read_ahead: ?local.sql_parallel_scheduler.Task(anyerror!bool) = null;
    defer if (read_ahead) |*task| {
        _ = task.cancel(io) catch false;
    };
    var file: usize = 0;
    var turn: usize = 0;
    var session_width: usize = Window.initial_width;
    while (file < parent.inventory.files.len) {
        try args.token.check();
        try args.provider.context.ensureActive();
        const requested_end = @min(file + session_width, parent.inventory.files.len);
        const window = &windows[turn % window_count];
        const prepared = if (read_ahead) |*task| loaded: {
            const result = task.await(io);
            read_ahead = null;
            replay.read_ahead_hits += 1;
            break :loaded try result;
        } else try window.capture(file, requested_end, args.token);
        window.report(replay);
        const end = window.end;
        replay.publication_sessions += 1;
        session_width = window.nextWidth(requested_end - file);
        if (ahead and end < parent.inventory.files.len) {
            const next = &windows[(turn + 1) % window_count];
            read_ahead = scheduler.submitIoTransient(io, 8 * 1024 * 1024, Window.capture, .{ next, end, @min(end + session_width, parent.inventory.files.len), args.token });
        }
        private_index.roots.clearRetainingCapacity();
        var nav_arena = std.heap.ArenaAllocator.init(a);
        defer nav_arena.deinit();
        // No consumer observes an intermediate root within this admitted
        // window. Rewrite every touched navigation subtree once per window.
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        const mutations = try nav_arena.allocator().alloc(tree.Mutation, end - file);
        const keys = try nav_arena.allocator().alloc([32]u8, mutations.len);
        for (parent.inventory.files[file..end], mutations, keys) |input, *mutation, *key| {
            key.* = local.storage_rowsource_identity.navigationDigest(public.identity_version, public.source, public.snapshot, input.file_id);
            mutation.* = .{ .key = key, .value = input.file_id };
        }
        std.mem.sort(tree.Mutation, mutations, {}, struct {
            fn less(_: void, left: tree.Mutation, right: tree.Mutation) bool {
                return std.mem.lessThan(u8, left.key, right.key);
            }
        }.less);
        public.root = try tree.apply(nav_arena.allocator(), navigation_pages.store(), public.root, mutations);
        public.count += @intCast(mutations.len);
        var view = try parent.parallelView(a);
        defer view.closeParallelView(parent);
        view.inventory.files = parent.inventory.files[0..end];
        view.scanner.inventory = view.inventory;
        view.selected_catalog_view = true;
        view.local_plan = null;
        view.plan_identity = null;
        // Without a replay each worker owns its own scanner. The retained
        // reservation covers one decode workspace, so stream them serially.
        const wave_lanes: usize = if (prepared) lanes else 1;
        var begin: usize = 0;
        while (begin < units.len) {
            const headroom = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(a) orelse return error.LakeSidecarBuildBudgetExceeded;
            const capacity = @min(retained - fixed, headroom -| fixed);
            var count: usize = 0;
            var bytes: usize = 0;
            while (begin + count < units.len and count < wave_lanes) {
                const wanted = allowance(units[begin + count].kind);
                if (wanted > capacity -| bytes) break;
                bytes += wanted;
                count += 1;
            }
            if (count == 0) return error.LakeSidecarBuildBudgetExceeded;
            var jobs: [8]FileJob = undefined;
            var failure = std.atomic.Value(u16).init(0);
            var initialized: usize = 0;
            defer for (jobs[0..initialized]) |*job| job.source.closeParallelView(&view);
            var tasks: [8]?std.Io.Future(anyerror!void) = @splat(null);
            defer for (&tasks) |*task| if (task.*) |*pending| {
                pending.cancel(io) catch {};
            };
            for (0..count) |slot| {
                const index = begin + slot;
                var provider = args.provider.*;
                provider.native_delta = null;
                provider.previous_inventory = null;
                provider.replay_consumer = null;
                provider.only_plan = null;
                provider.only_files = null;
                provider.only_file = null;
                provider.progress = null;
                states[index].work.file = file;
                states[index].work.end = end;
                states[index].work.positions = positions;
                states[index].work.navigation = public;
                states[index].work.source_cut = if (end == parent.inventory.files.len) (if (args.provider.native_delta) |delta| delta.source_cut else null) else null;
                provider.resume_work = &states[index].work;
                provider.replay = if (prepared) window.replay() else null;
                var job_args = args;
                job_args.a = a;
                job_args.index = &private_index;
                job_args.unindexed_aggregates = false;
                var store = args.store.*;
                store.allocator = a;
                jobs[slot] = .{ .args = job_args, .unit = units[index], .state = &states[index], .failure = &failure, .provider = provider, .source = try view.parallelView(a), .store = store, .execution = .{ .scheduler = scheduler, .bytes = worker_bytes } };
                initialized += 1;
                jobs[slot].provider.source = &jobs[slot].source;
                tasks[slot] = io.concurrent(FileJob.run, .{ &jobs[slot], io }) catch fallback: {
                    try jobs[slot].run(io);
                    break :fallback null;
                };
            }
            for (&tasks) |*task| if (task.*) |*pending| {
                const finished = pending.await(io);
                task.* = null;
                try finished;
            };
            begin += count;
        }
        var metadata = std.heap.ArenaAllocator.init(a);
        defer metadata.deinit();
        var changes: std.ArrayList(Contribution) = .empty;
        for (states) |state| try changes.appendSlice(metadata.allocator(), state.result.native.contributions);
        private_index.root = try private_index.update(changes.items);
        const next_roots = try private_index.rootKeys(a);
        a.free(prior_roots);
        prior_roots = next_roots;
        private_index.prior_roots = prior_roots;
        private_index.reduction_seeds.clearRetainingCapacity();
        _ = private_index.seed_arena.reset(.free_all);
        private_index.seeds_loaded = false;
        for (&window.buffers) |*buffer| try buffer.reset();
        file = end;
        turn += 1;
    }
    var result: Result = .{ .contribution_root = private_index.root, .contribution_roots = try args.out.dupe([32]u8, prior_roots) };
    for (states) |state| {
        // Transfer only the final immutable metadata to the publication owner.
        // Old/new metadata overlaps only within the admitted worker wave.
        var completed = state.result;
        completed.native.contributions = &.{};
        const owned_result = try cloneMetadata(Result, args.out, completed);
        try merge(args.out, &result, owned_result);
    }
    try args.provider.checkpointBatch(result.native.declarations);
    for (result.ordered) |declaration| try args.provider.checkpoint(declaration);
    for (result.text) |declaration| try args.provider.checkpoint(declaration);
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
    const scan_workspace = std.math.add(usize, args_.provider.limits.max_input_bytes, decoded) catch return null;
    const desired = @min(available, workspace +| queue_bytes +| scan_workspace +| replay.budget.limit);
    var retained_bytes = scheduler.reservePullUpTo(units.len, workspace + 1024 * 1024, desired) orelse return null;
    defer scheduler.releasePull(units.len, retained_bytes);
    const buffered = queue_bytes < retained_bytes - workspace;
    if (buffered) workspace += queue_bytes;
    // A second private cursor pins one first batch, its compressed input and
    // decoder state. Admission covers its entire lifetime, including uploads
    // by consumers of the current file. No speculative archive spool is used.
    const stream_read_ahead = scan_workspace < retained_bytes - workspace;
    if (stream_read_ahead) workspace += scan_workspace;
    const replay_limit = @min(replay.budget.limit, retained_bytes - workspace);
    const used = workspace + replay_limit;
    scheduler.releasePull(0, retained_bytes - used);
    retained_bytes = used;
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

    // Fresh canonical/serving groups must flow through the local producer
    // handoff. Reject a provider GET of any group written by this build.
    const ManifestProbe = struct {
        var active: *@This() = undefined;
        backend: stores.ArtifactStore,
        mutex: std.atomic.Mutex = .unlocked,
        ids: [64][32]u8 = undefined,
        count: usize = 0,
        fn put(_: *anyopaque, alloc: A, scope: stores.UploadScope, data: []const u8, token: Token) !stores.ArtifactMetadata {
            const self = active;
            var backend = self.backend;
            backend.allocator = alloc;
            const metadata = try backend.putScoped(scope, data, token);
            if (std.mem.startsWith(u8, data, "{\"file\":")) {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                defer self.mutex.unlock();
                std.debug.assert(self.count < self.ids.len);
                self.ids[self.count] = @import("lake_index_term_directory.zig").identity(metadata.artifact_id);
                self.count += 1;
            }
            return metadata;
        }
        fn read(_: *anyopaque, alloc: A, id: []const u8, maximum: usize, token: Token) ![]u8 {
            const self = active;
            const key = @import("lake_index_term_directory.zig").identity(id);
            {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                defer self.mutex.unlock();
                for (self.ids[0..self.count]) |fresh| if (std.mem.eql(u8, &key, &fresh)) return error.InjectedReadFailure;
            }
            const data = try self.backend.getAllocWithCancellationUsingAllocator(alloc, id, token);
            errdefer alloc.free(data);
            if (data.len > maximum) return error.ArtifactTooLarge;
            return data;
        }
    };
    var manifest_probe: ManifestProbe = .{ .backend = store };
    ManifestProbe.active = &manifest_probe;
    const original_vtable = store.vtable;
    var manifest_vtable = store.vtable.*;
    manifest_vtable.put_scoped = ManifestProbe.put;
    manifest_vtable.get_bounded_alloc_with_cancellation = ManifestProbe.read;
    store.vtable = &manifest_vtable;
    defer store.vtable = original_vtable;
    const result = (try build(args)) orelse return error.TestUnexpectedResult;
    store.vtable = original_vtable;
    try std.testing.expect(manifest_probe.count >= 6);

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
    // At 248 MiB, including the new 8 MiB state sort, these consumers admit
    // the second source cursor while retaining single-batch backpressure.
    var borrowed_budget = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = 248 * 1024 * 1024 });
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
    // Shared retention pressure must trim optional buffers before splitting a
    // file rendezvous. This previously opened the same three files six times.
    var constrained: local.sql_parallel_scheduler.Scheduler = .{ .max_pull_bytes = 1200 * 1024 * 1024 };
    args.scheduler = &constrained;
    const captures_before_constrained = replay.captures;
    const constrained_result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), constrained_result.text.len);
    try std.testing.expectEqual(@as(usize, 3), replay.captures - captures_before_constrained);
    try std.testing.expectEqual(@as(usize, 0), constrained.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), constrained.pull_workers);
    var file_major: local.sql_parallel_scheduler.Scheduler = .{ .max_pull_bytes = 500 * 1024 * 1024 };
    args.scheduler = &file_major;
    const captures_before_major = replay.captures;
    const Progress = struct {
        replay: *replay_api.Replay,
        complete_at: usize,
        calls: usize = 0,
        fn save(raw: *anyopaque, _: Declared) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.complete_at, self.replay.captures);
            self.calls += 1;
        }
    };
    var progress: Progress = .{ .replay = &replay, .complete_at = captures_before_major + 3 };
    provider.progress = .{ .ptr = &progress, .save = Progress.save };
    defer provider.progress = null;
    const major = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 4), major.text.len);
    try std.testing.expectEqual(@as(usize, 1), major.dense.len);
    try std.testing.expectEqual(@as(usize, 1), major.sparse.len);
    try std.testing.expectEqual(@as(usize, 1), major.ordered.len);
    try std.testing.expectEqual(@as(usize, 3), major.native.declarations.len);
    try std.testing.expectEqual(@as(usize, 3), replay.captures - captures_before_major);
    try std.testing.expectEqual(@as(usize, 8), progress.calls);
    try std.testing.expect(major.contribution_root != null);
    try std.testing.expectEqual(@as(usize, 0), major.native.contributions.len);
    try std.testing.expectEqual(@as(usize, 3), major.contribution_roots.len);
    const held_ref = major.text[0].artifact;
    const private_work: Provider.ResumeWork = .{ .private = .{ .ref = held_ref, .scope = store.upload_scope.?, .store_ptr = store.ptr, .store_vtable = store.vtable } };
    try std.testing.expect(private_work.trusts(store, held_ref));
    var changed_ref = held_ref;
    changed_ref.metadata_version += 1;
    try std.testing.expect(!private_work.trusts(store, changed_ref));
    changed_ref = held_ref;
    changed_ref.byte_len += 1;
    try std.testing.expect(!private_work.trusts(store, changed_ref));
    changed_ref = held_ref;
    changed_ref.name = "another-index";
    try std.testing.expect(!private_work.trusts(store, changed_ref));
    var changed_store = store;
    changed_store.upload_scope.?.attempt[15] ^= 1;
    try std.testing.expect(!private_work.trusts(changed_store, held_ref));
    changed_store = store;
    var other_backend: u8 = 0;
    changed_store.ptr = &other_backend;
    try std.testing.expect(!private_work.trusts(changed_store, held_ref));
    const another_vtable = store.vtable.*;
    changed_store = store;
    changed_store.vtable = &another_vtable;
    try std.testing.expect(!private_work.trusts(changed_store, held_ref));
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_workers);
    const major_ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, major.ordered[0].artifact, .none, null);
    try std.testing.expect(!major_ordered.building);
    try std.testing.expectEqual(@as(u64, 12), major_ordered.page.?.records);
    const dense = try @import("lake_index_native_dense.zig").loadRoot(out, store, major.dense[0].artifact, .none, null);
    var dense_rows: u64 = 0;
    try std.testing.expectEqual(@as(usize, 0), dense.file_states.len);
    try std.testing.expectEqual(@as(u64, 3), dense.file_directory.?.records);
    for (try @import("lake_index_native_state.zig").directoryFiles(out, store, dense.file_directory.?, dense.generation.domain, .none)) |file| dense_rows += file.count;
    try std.testing.expectEqual(@as(u64, 12), dense_rows);
    const sparse = try @import("lake_index_native_sparse.zig").loadRoot(out, store, major.sparse[0].artifact, .none, null);
    var sparse_rows: u64 = 0;
    try std.testing.expectEqual(@as(usize, 0), sparse.file_states.len);
    try std.testing.expectEqual(@as(u64, 3), sparse.file_directory.?.records);
    for (try @import("lake_index_native_state.zig").directoryFiles(out, store, sparse.file_directory.?, sparse.generation.domain, .none)) |file| sparse_rows += file.count;
    try std.testing.expectEqual(@as(u64, 12), sparse_rows);
    const artifact_api = @import("lake_index_aggregate_artifact.zig");
    for (major.native.declarations) |declaration| {
        const recipe = try artifact_api.loadRecipe(out, store, declaration.artifact, .none);
        const reader = try artifact_api.Reader.open(a, store, declaration.artifact, recipe, .none);
        defer reader.cursor().close(reader);
        const group = try local.sql_operators.Grouped.create(a, &.{recipe.inputs[0].spec}, .{});
        defer group.deinit();
        while (try reader.cursor().next(reader, out, 8)) |rows| try group.importPartialBatch(rows);
        var total: i64 = 0;
        while (try group.nextResult(out)) |row| total += row.aggregates[0].value.integer;
        try std.testing.expectEqual(@as(i64, if (recipe.inputs[0].spec.kind == .sum) 30 else 12), total);
    }
    // A declined per-file replay preserves the ordinary streaming path and
    // keeps a single scan workspace under the same constrained reservation.
    provider.progress = null;
    const spill_limit = replay.max_spill_bytes;
    replay.max_spill_bytes = 1;
    const streamed = (try build(args)) orelse return error.TestUnexpectedResult;
    replay.max_spill_bytes = spill_limit;
    provider.progress = .{ .ptr = &progress, .save = Progress.save };
    try std.testing.expectEqual(major.text.len, streamed.text.len);
    try std.testing.expectEqual(major.native.declarations.len, streamed.native.declarations.len);
    const streamed_rows = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, streamed.ordered[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 12), streamed_rows.page.?.records);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.workers);
    var canceled = std.atomic.Value(bool).init(true);
    args.token = Token.fromAtomic(&canceled);
    const captures_before_cancel = replay.captures;
    try std.testing.expectError(error.Canceled, build(args));
    try std.testing.expectEqual(captures_before_cancel, replay.captures);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_workers);
    const Interrupt = struct {
        replay: *replay_api.Replay,
        after: usize,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.replay.captures >= self.after) return error.Canceled;
        }
    };
    var interrupt: Interrupt = .{ .replay = &replay, .after = replay.captures + 2 };
    args.token = .none;
    progress.calls = 0;
    provider.context.additional_checkpoint = .{ .ptr = &interrupt, .check = Interrupt.check };
    defer provider.context.additional_checkpoint = null;
    try std.testing.expectError(error.Canceled, build(args));
    try std.testing.expectEqual(@as(usize, 0), progress.calls);
    try std.testing.expect(replay.run == null);
    try std.testing.expectEqual(@as(usize, 0), replay.readers);
    try std.testing.expectEqual(@as(usize, 0), replay.budget.live);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.workers);

    // Two adaptive sessions cover nine files instead of three fixed windows. The
    // next window is captured on the scheduler while current consumers run.
    provider.context.additional_checkpoint = null;
    provider.progress = null;
    for (3..9) |ordinal| {
        const name = try std.fmt.allocPrint(a, "part-{d}.parquet", .{ordinal});
        defer a.free(name);
        var put = try client.putObject("antfly", name, bytes, .{});
        put.deinit(a);
    }
    var expanded = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer expanded.deinit();
    provider.source = &expanded;
    defer provider.source = &source;
    provider.native_delta = null;
    var expanded_navigation = try navigation_api.build(a, out, &store, &provider, null, .none);
    defer expanded_navigation.deinit();
    provider.native_delta = expanded_navigation.delta;
    defer provider.native_delta = navigation.delta;
    const expanded_inventory = try navigation_api.encodeInventory(out, &provider, expanded_navigation.root);
    var expanded_ref = try store.putWithCancellation(expanded_inventory, .none);
    defer expanded_ref.deinit(a);
    args.base = try binding.binding.toManifestBaseSource(expanded.inventory.snapshot_id, expanded_ref.artifact_id);
    const before_windows = replay.captures;
    const sessions_before_windows = replay.publication_sessions;
    const ahead_before_windows = replay.read_ahead_hits;
    const windows_result = (try build(args)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 9), replay.captures - before_windows);
    try std.testing.expectEqual(@as(usize, 2), replay.publication_sessions - sessions_before_windows);
    try std.testing.expect(replay.read_ahead_hits > ahead_before_windows);
    const window_ordered = try @import("lake_index_ordered_rows.zig").loadRootLazy(out, store, windows_result.ordered[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 36), window_ordered.page.?.records);
    try std.testing.expectEqual(@as(u32, 0), window_ordered.pending_files);
    const window_dense = try @import("lake_index_native_dense.zig").loadRoot(out, store, windows_result.dense[0].artifact, .none, null);
    const window_sparse = try @import("lake_index_native_sparse.zig").loadRoot(out, store, windows_result.sparse[0].artifact, .none, null);
    try std.testing.expectEqual(@as(u64, 9), window_dense.file_directory.?.records);
    try std.testing.expectEqual(@as(u64, 9), window_sparse.file_directory.?.records);
    var windows_rows: u64 = 0;
    for (try @import("lake_index_native_state.zig").directoryFiles(out, store, window_sparse.file_directory.?, window_sparse.generation.domain, .none)) |file| windows_rows += file.count;
    try std.testing.expectEqual(@as(u64, 36), windows_rows);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.workers);
    // One non-incremental AVG must leave the nine compatible configurations
    // on their shared file-major pass. Only AVG needs its separate full pass.
    const original_table = args.table;
    var mixed_table = original_table;
    mixed_table.indexes_json = try std.mem.concat(out, u8, &.{ original_table.indexes_json[0 .. original_table.indexes_json.len - 1], ",\"averages\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"avg_amount\",\"op\":\"avg\",\"measure\":\"amount\"}]}}" });
    args.table = mixed_table;
    const before_mixed = replay.captures;
    const mixed = (try build(args)) orelse return error.TestUnexpectedResult;
    args.table = original_table;
    try std.testing.expectEqual(@as(usize, 9), replay.captures - before_mixed);
    try std.testing.expectEqual(@as(usize, 4), mixed.native.declarations.len);
    try std.testing.expectEqual(windows_result.text.len, mixed.text.len);
    try std.testing.expect(mixed.contribution_root != null);
    // Interrupt current consumers after scheduling the next capture. Both
    // windows and all reservations must retire before the failed build returns.
    interrupt.after = replay.captures + 4;
    provider.context.additional_checkpoint = .{ .ptr = &interrupt, .check = Interrupt.check };
    try std.testing.expectError(error.Canceled, build(args));
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), file_major.pull_workers);
    try std.testing.expectEqual(@as(usize, 0), file_major.workers);
    try std.testing.expectEqual(@as(usize, 0), file_major.io_workers);
}

test "external lake publication checkpoints park CPU and unwind failed durable saves" {
    const Checkpoint = struct {
        execution: *local.sql_parallel_scheduler.Execution,
        io: std.Io,
        fail: bool = false,
        fn save(raw: *anyopaque, _: Declared) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.execution.active);
            try std.testing.expectEqual(@as(usize, 1), self.execution.scheduler.io_workers);
            var competing: local.sql_parallel_scheduler.Execution = .{ .scheduler = self.execution.scheduler, .bytes = 32 };
            try competing.enter(self.io);
            competing.pause();
            if (self.fail) return error.InjectedUploadFailure;
        }
    };
    const io = std.testing.io;
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = 2, .max_bytes = 128, .max_io_bytes = 64 };
    try std.testing.expect(scheduler.reservePull(1, 4096));
    defer scheduler.releasePull(1, 4096);
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = &scheduler, .bytes = 32 };
    defer execution.pause();
    try execution.enter(io);
    var phase: PublicationIo = .{ .execution = &execution, .io = io };
    var checkpoint: Checkpoint = .{ .execution = &execution, .io = io };
    var provider: Provider = .{ .source = undefined, .context = .{}, .io_phase = phase.capability(), .progress = .{ .ptr = &checkpoint, .save = Checkpoint.save } };
    const declaration: Declared = .{
        .name = "count",
        .binding = .{ .sidecar_kind = .algebraic, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "config" },
        .artifact = .{ .kind = .algebraic_segment, .artifact_id = "test", .byte_len = 0, .checksum = "test" },
    };
    try provider.checkpoint(declaration);
    try std.testing.expect(execution.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    checkpoint.fail = true;
    try std.testing.expectError(error.InjectedUploadFailure, provider.checkpointBatch(&.{declaration}));
    try std.testing.expect(execution.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 4096), scheduler.pull_bytes);
    scheduler.max_io_workers = 0;
    try std.testing.expect(phase.capability() == null);
}

test "external lake publication adapters batch reverse statistics and text directories" {
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    const ordered = @import("lake_index_ordered_rows.zig");
    const corpus = @import("lake_index_native_text.zig");
    const directory = @import("lake_index_text_directory.zig");
    const statistics = @import("lake_index_text_statistics.zig");
    const Probe = struct {
        var active: *@This() = undefined;
        phase: PublicationIo,
        original: stores.ArtifactStore,
        peak_slots: usize = 0,
        reads: usize = 0,
        manifest_reads: usize = 0,
        track_packs: bool = false,
        fail_pack: bool = false,
        read_failure: bool = false,
        canceled: bool = false,
        cancel_resume: bool = false,
        active_puts: std.atomic.Value(usize) = .init(0),
        peak_puts: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
        fn put(_: *anyopaque, alloc: A, scope: stores.UploadScope, bytes: []const u8, token: Token) !stores.ArtifactMetadata {
            const self = active;
            try std.testing.expect(!self.phase.execution.active);
            const current = self.active_puts.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.active_puts.fetchSub(1, .acq_rel);
            if (self.track_packs) {
                _ = self.peak_puts.fetchMax(current, .acq_rel);
                try std.testing.io.sleep(.fromMilliseconds(3), .awake);
                if (self.fail_pack and bytes.len != 0 and bytes[0] == 3) return error.InjectedUploadFailure;
            }
            var backend = self.original;
            backend.allocator = alloc;
            return backend.putScoped(scope, bytes, token);
        }
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.phase.execution.active);
            self.peak_slots = @max(self.peak_slots, count);
            try PublicationIo.begin(&self.phase, count);
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancel_resume) self.canceled = true;
            try PublicationIo.finish(&self.phase);
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            PublicationIo.abort(&self.phase);
        }
        fn read(_: *anyopaque, a: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self = active;
            try std.testing.expect(!self.phase.execution.active);
            try std.testing.expectEqual(@as(usize, 1), self.phase.execution.scheduler.io_workers);
            self.reads += 1;
            var competing: local.sql_parallel_scheduler.Execution = .{ .scheduler = self.phase.execution.scheduler, .bytes = 32 };
            try competing.enter(std.testing.io);
            competing.pause();
            return self.original.getRangeAllocWithCancellationUsingAllocator(a, id, offset, length, token);
        }
        fn bounded(_: *anyopaque, a: A, id: []const u8, maximum: usize, token: Token) ![]u8 {
            const self = active;
            try std.testing.expect(!self.phase.execution.active);
            try std.testing.expectEqual(@as(usize, 1), self.phase.execution.scheduler.io_workers);
            self.manifest_reads += 1;
            var competing: local.sql_parallel_scheduler.Execution = .{ .scheduler = self.phase.execution.scheduler, .bytes = 32 };
            try competing.enter(std.testing.io);
            competing.pause();
            if (self.read_failure) return error.InjectedReadFailure;
            const bytes = try self.original.getAllocWithCancellationUsingAllocator(a, id, token);
            errdefer a.free(bytes);
            if (bytes.len > maximum) return error.ArtifactTooLarge;
            return bytes;
        }
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("publication-transport-adapters");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var original = fs.artifactStore();
    original.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = 3, .max_bytes = 128 };
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = &scheduler, .bytes = 32 };
    defer execution.pause();
    try execution.enter(std.testing.io);
    var probe: Probe = .{ .phase = .{ .execution = &execution, .io = std.testing.io }, .original = original };
    Probe.active = &probe;
    execution.checkpoint = .{ .ptr = &probe, .check = Probe.check };
    const phase: stores.IoPhase = .{ .io = std.testing.io, .ptr = &probe, .max_parallel = 3, .begin = Probe.begin, .finish = Probe.finish, .abort = Probe.abort };
    var vtable = original.vtable.*;
    vtable.get_range_alloc_with_cancellation = Probe.read;
    vtable.put_scoped = Probe.put;
    vtable.get_bounded_alloc_with_cancellation = Probe.bounded;
    var store = original;
    store.vtable = &vtable;
    store.io_phase = &phase;
    {
        const reverse = try ordered.ReverseBuilder.create(a, &store, .none);
        defer reverse.deinit();
        for (0..6000) |ordinal| {
            var key: [24]u8 = @splat(0);
            std.mem.writeInt(u64, key[0..8], 6000 - ordinal, .big);
            std.mem.writeInt(u64, key[16..24], ordinal, .big);
            try reverse.add(&key);
        }
        const root = (try reverse.finish()).?;
        try std.testing.expectEqual(@as(u64, 6000), root.records);
        try std.testing.expect(probe.peak_slots > 1 and probe.peak_slots <= 3);
        var cursor = try tree.Cursor.init(a, reverse.pages.store(), root, "", null);
        defer cursor.deinit();
        var ordinal: u64 = 0;
        while (try cursor.next()) |record| : (ordinal += 1) try std.testing.expectEqual(ordinal, std.mem.readInt(u64, record.key[8..16], .big));
        try std.testing.expectEqual(@as(u64, 6000), ordinal);
        try std.testing.expect(probe.reads > 0);
    }
    {
        probe.peak_slots = 0;
        var writer = local.segment.SegmentWriter.init(a);
        defer writer.deinit();
        var text = local.section_inverted.InvertedIndexBuilder.init(a, .{});
        defer text.deinit();
        try writer.addStoredDoc("one", "{}");
        for (0..4096) |ordinal| {
            var buffer: [128]u8 = undefined;
            const term = try std.fmt.bufPrint(&buffer, "term-{d:0>4}-long-enough-to-span-multiple-statistics-pages", .{ordinal});
            try text.addDocumentSingle(0, term, 1, 4096);
        }
        const inverted = try text.build();
        defer a.free(inverted);
        try writer.addSection(try writer.addField("body"), .inverted_text, inverted);
        const bytes = try writer.build();
        defer a.free(bytes);
        const summary = (try statistics.publishSegment(a, &store, bytes, .none)).?;
        try std.testing.expectEqual(@as(u64, 4096), summary.records);
        try std.testing.expect(probe.peak_slots > 1 and probe.peak_slots <= 3);
    }
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const out = arena.allocator();
        const groups = try out.alloc(corpus.FileGroup, 256);
        const refs = try out.alloc(@import("lake_index_aggregate_artifact.zig").ChunkRef, groups.len);
        const records = try out.alloc(directory.FileRecord, groups.len);
        for (groups, refs, records, 0..) |*group, *ref, *record, ordinal| {
            group.* = .{ .file = .{ .id = try std.fmt.allocPrint(out, "file-{d}", .{ordinal}), .digest = @splat(1) }, .segments = &.{} };
            ref.* = try corpus.publishFileManifest(a, out, &store, group.*, .none);
            record.* = .{ .file = group.file, .manifest = ref.* };
        }
        probe.peak_slots = 0;
        const roots = try directory.publish(a, &store, groups, refs, &.{}, &.{}, .{}, .none, std.testing.io);
        try std.testing.expect(probe.peak_slots > 1 and probe.peak_slots <= 3);
        // Reuse the decode arena after every group. Both consumers observe
        // exactly one manifest GET, and the directory retains no borrowed data.
        var publisher: directory.Publisher = undefined;
        try publisher.init(a, &store, .none, std.testing.io);
        defer publisher.deinit();
        var decoded_group = std.heap.ArenaAllocator.init(a);
        defer decoded_group.deinit();
        probe.manifest_reads = 0;
        var consumed: usize = 0;
        for (refs) |ref| {
            _ = decoded_group.reset(.free_all);
            const group = try corpus.loadFileManifestPublication(decoded_group.allocator(), store, ref, original.upload_scope.?.domain, .none);
            consumed += group.file.id.len;
            try publisher.add(group, ref, false);
        }
        _ = decoded_group.reset(.free_all);
        const shared = try publisher.finish(.{});
        try std.testing.expect(consumed > 0);
        try std.testing.expectEqual(@as(usize, 256), probe.manifest_reads);
        try std.testing.expect(roots.files.?.eql(shared.files.?));
        // Paged source spans use the same explicit read admission, with CPU
        // restored between each verified object and its JSON decode.
        const spans = try out.alloc(corpus.source_spans.Span, 65);
        for (spans, 0..) |*span, ordinal| span.* = .{ .file = groups[0].file, .physical = refs[0], .file_rows = 1, .file_lower = 0, .pack_lower = @intCast(ordinal), .count = 1 };
        const span_pages = try corpus.source_spans.publish(out, &store, spans, .none);
        const paged_manifest = try corpus.publishFileManifest(a, out, &store, .{ .file = groups[0].file, .segments = refs[0..1], .source_pages = span_pages, .source_count = 65 }, .none);
        const before_spans = probe.manifest_reads;
        const paged_group = try corpus.loadFileManifestPublication(out, store, paged_manifest, original.upload_scope.?.domain, .none);
        try std.testing.expectEqual(@as(usize, 65), paged_group.sources.len);
        try std.testing.expectEqual(before_spans + 1 + span_pages.len, probe.manifest_reads);
        probe.read_failure = true;
        try std.testing.expectError(error.InjectedReadFailure, corpus.loadFileManifestPublication(out, store, refs[0], original.upload_scope.?.domain, .none));
        probe.read_failure = false;
        try std.testing.expect(execution.active);
        probe.cancel_resume = true;
        try std.testing.expectError(error.Canceled, corpus.loadFileManifestPublication(out, store, refs[0], original.upload_scope.?.domain, .none));
        probe.cancel_resume = false;
        probe.canceled = false;
        try std.testing.expect(!execution.active);
        try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
        try execution.enter(std.testing.io);
        probe.peak_slots = 0;
        const streamed = try directory.publishManifests(a, &store, refs, &.{}, .{}, original.upload_scope.?.domain, .none, std.testing.io);
        try std.testing.expect(roots.files.?.eql(streamed.files.?));
        try std.testing.expect(probe.peak_slots > 1 and probe.peak_slots <= 3);
        probe.peak_slots = 0;
        const checkpoints = (try directory.publishFileRecords(a, &store, records, &.{}, null, .none, std.testing.io)).?;
        try std.testing.expect(roots.files.?.eql(checkpoints));
        try std.testing.expect(probe.peak_slots > 1 and probe.peak_slots <= 3);
        probe.reads = 0;
        const updated = (try directory.publishFileRecords(a, &store, &.{}, records[0..1], checkpoints, .none, std.testing.io)).?;
        try std.testing.expectEqual(@as(u64, 255), updated.records);
        try std.testing.expect(probe.reads > 0);
    }
    {
        const seekable = @import("lake_index_seekable_text.zig");
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const out = arena.allocator();
        const bytes = try a.alloc(u8, 13 * 1024 * 1024 - 7);
        defer a.free(bytes);
        for (bytes, 0..) |*byte, ordinal| byte.* = @intCast(ordinal / (1024 * 1024));
        probe.track_packs = true;
        const parallel = try seekable.publish(a, out, &store, bytes, .none);
        try std.testing.expect(probe.peak_puts.load(.acquire) > 1 and probe.peak_puts.load(.acquire) <= 3);
        const serial = try seekable.publish(a, out, &original, bytes, .none);
        try std.testing.expectEqualStrings(serial.artifact_id, parallel.artifact_id);
        const data = try seekable.load(a, .{ .store = original, .cache = null, .context = .{}, .cancellation = .none }, parallel);
        var source = data.native;
        defer source.close();
        var sample: [16]u8 = undefined;
        for (1..13) |boundary| {
            const offset = boundary * 1024 * 1024 - 8;
            try source.readInto(offset, &sample);
            try std.testing.expectEqualSlices(u8, bytes[offset..][0..sample.len], &sample);
        }
        probe.fail_pack = true;
        try std.testing.expectError(error.InjectedUploadFailure, seekable.publish(a, out, &store, bytes, .none));
        probe.fail_pack = false;
        probe.track_packs = false;
        try std.testing.expectEqual(@as(usize, 0), probe.active_puts.load(.acquire));
        std.debug.print("packed text publication: packs=13 peak_uploads={} shared_manifest_reads=256\n", .{probe.peak_puts.load(.acquire)});
    }
    try std.testing.expect(execution.active);
    try std.testing.expect(probe.phase.pending == null);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
}
