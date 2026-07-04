// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const antfly = @import("antfly-zig");

const db_mod = antfly.db;
const replay_stream_mod = db_mod.replay_stream;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;
const schema_api_mod = antfly.table_schema;
const schema_mod = antfly.schema;

const Workload = enum {
    documents,
    relational_rows,
    explicit_full_text,
    explicit_dense,
};

const PrimaryKind = enum {
    lsm,
    lsm_memory,
    mem,
    lmdb,
};

const MutationMode = enum {
    overwrite,
    transform,
    delete,
    membership_change,
    identity_rewrite,
};

const OverwriteShape = enum {
    changing,
    unchanged,
};

const QueryShape = enum {
    range,
    equality,
    ordered_pagination,
};

const MatrixPreset = enum {
    smoke,
    full,
};

const MatrixWriteScenario = enum {
    current,
    insert,
    overwrite_changed,
    overwrite_unchanged,
    delete,
    membership_change,
    identity_rewrite,
};

const RelationalIndexMode = enum {
    none,
    single_column,
    ordered_tuple,
    partial_single_column,
    partial_ordered_tuple,
};

const RelationalConstraintProbe = enum {
    none,
    unique,
    foreign_key,
};

const Config = struct {
    workload: Workload = .explicit_full_text,
    primary: PrimaryKind = .lsm,
    relational_index_mode: RelationalIndexMode = .single_column,
    relational_constraint_probe: RelationalConstraintProbe = .none,
    docs: usize = 10_000,
    overwrite_passes: usize = 1,
    body_repeat: usize = 1,
    dims: usize = 384,
    batch_size: usize = 500,
    query_repeats: usize = 0,
    query_limit: usize = 100,
    query_total_mode: db_mod.types.RelationalRowsQueryRequest.TotalMode = .exact,
    query_shape: QueryShape = .range,
    predicate_min_amount: usize = 250,
    seed: u64 = 42,
    bulk_session: bool = false,
    sync_level: db_mod.types.SyncLevel = .write,
    mutation_mode: MutationMode = .overwrite,
    overwrite_shape: OverwriteShape = .changing,
    matrix: bool = false,
    matrix_preset: MatrixPreset = .smoke,
    matrix_docs: [4]usize = .{ 0, 0, 0, 0 },
    matrix_docs_len: usize = 0,
    matrix_fail_on_regression: bool = false,
    matrix_max_ordered_vs_single_writes: f64 = 1.0,
    matrix_max_ordered_vs_single_write_bytes: f64 = 1.25,
    matrix_max_ordered_vs_single_stage_time: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_total_time: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_predicate: f64 = 1.25,
    matrix_max_ordered_vs_single_stage_allocations: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_stage_allocated_bytes: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_query_allocations: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_iterator_seeks: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_residual_rechecks: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_store_mutations: f64 = std.math.inf(f64),
    matrix_max_ordered_vs_single_relational_mutations: f64 = std.math.inf(f64),
    matrix_all_query_shapes: bool = true,
    matrix_write_scenario: MatrixWriteScenario = .current,
    matrix_all_write_scenarios: bool = false,
    matrix_all_constraint_probes: bool = false,
};

const ReplayStats = struct {
    last_sequence: u64 = 0,
    entries: usize = 0,
    payload_bytes: usize = 0,
};

const AllocationStats = struct {
    allocation_events: usize = 0,
    free_events: usize = 0,
    allocated_bytes: usize = 0,
    freed_bytes: usize = 0,
    live_bytes: usize = 0,
    peak_live_bytes: usize = 0,
};

const QueryStats = struct {
    lookup_repeats: usize = 0,
    lookup_hits: usize = 0,
    lookup_ns: u64 = 0,
    predicate_repeats: usize = 0,
    predicate_hits: usize = 0,
    predicate_ns: u64 = 0,
    predicate_exact_totals: usize = 0,
    index_entries_scanned: u64 = 0,
    candidate_rows: u64 = 0,
    candidate_gate_limit: u64 = 0,
    candidate_gate_observed: u64 = 0,
    iterator_seeks: u64 = 0,
    hydrated_rows: u64 = 0,
    residual_rechecks: u64 = 0,
    covering_payload_rows: u64 = 0,
    projected_rows: u64 = 0,
    ordered_tuple_plan_queries: u64 = 0,
    ordered_tuple_max_key_count: u32 = 0,
    ordered_tuple_max_equality_prefix_len: u32 = 0,
    ordered_tuple_max_filter_predicates: u32 = 0,
    ordered_tuple_max_proven_predicates: u32 = 0,
    ordered_tuple_max_residual_predicates: u32 = 0,
    ordered_tuple_range_plan_queries: u64 = 0,
    ordered_tuple_prefix_scan_queries: u64 = 0,
    unknown_access_queries: u64 = 0,
    base_scan_queries: u64 = 0,
    resolved_doc_set_queries: u64 = 0,
    ordered_tuple_doc_set_queries: u64 = 0,
    ordered_tuple_stream_queries: u64 = 0,
    ordered_tuple_candidate_gate_fallbacks: u64 = 0,
    ordered_tuple_materialization_cap_fallbacks: u64 = 0,
    ordered_tuple_exact_paged_total_fallbacks: u64 = 0,
    ordered_tuple_ordering_not_covered_fallbacks: u64 = 0,
    ordered_tuple_index_not_ready_fallbacks: u64 = 0,
    ordered_tuple_stale_generation_fallbacks: u64 = 0,
    ordered_tuple_predicate_not_proven_fallbacks: u64 = 0,
    ordered_tuple_no_usable_bounds_fallbacks: u64 = 0,
    allocations: AllocationStats = .{},
};

const Summary = struct {
    requested_writes: usize = 0,
    requested_deletes: usize = 0,
    requested_transforms: usize = 0,
    batches: usize = 0,
    stage_ns: u64 = 0,
    finish_ns: u64 = 0,
    total_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    profile: db_mod.BatchProfile = .{},
    replay: ReplayStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
    query: QueryStats = .{},
    stage_allocations: AllocationStats = .{},
};

const MatrixComparison = struct {
    docs: usize,
    write_scenario: MatrixWriteScenario,
    constraint_probe: RelationalConstraintProbe,
    mutation_mode: MutationMode,
    overwrite_shape: OverwriteShape,
    query_shape: QueryShape,
    predicate_min_amount: usize,
    selectivity: []const u8,
    total_mode: db_mod.types.RelationalRowsQueryRequest.TotalMode,
    ordered_vs_no_index_writes: f64,
    ordered_vs_single_writes: f64,
    ordered_vs_no_index_write_bytes: f64,
    ordered_vs_single_write_bytes: f64,
    ordered_vs_no_index_stage_time: f64,
    ordered_vs_single_stage_time: f64,
    ordered_vs_no_index_total_time: f64,
    ordered_vs_single_total_time: f64,
    ordered_vs_no_index_store_mutations: f64,
    ordered_vs_single_store_mutations: f64,
    ordered_vs_no_index_relational_mutations: f64,
    ordered_vs_single_relational_mutations: f64,
    ordered_vs_no_index_stage_allocations: f64,
    ordered_vs_single_stage_allocations: f64,
    ordered_vs_no_index_stage_allocated_bytes: f64,
    ordered_vs_single_stage_allocated_bytes: f64,
    ordered_vs_no_index_query_allocations: f64,
    ordered_vs_single_query_allocations: f64,
    ordered_vs_no_index_iterator_seeks: f64,
    ordered_vs_single_iterator_seeks: f64,
    ordered_vs_no_index_residual_rechecks: f64,
    ordered_vs_single_residual_rechecks: f64,
    ordered_vs_no_index_predicate: f64,
    ordered_vs_single_predicate: f64,
    ordered_tuple_base_scan_queries: u64,
    ordered_tuple_resolved_doc_set_queries: u64,
    ordered_tuple_doc_set_queries: u64,
    ordered_tuple_stream_queries: u64,
    ordered_tuple_access_missing_flag: bool,
    ordered_tuple_fallback_flag: bool,
};

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    stats: AllocationStats = .{},

    pub fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    pub fn reset(self: *CountingAllocator) void {
        self.stats = .{};
    }

    pub fn snapshot(self: *const CountingAllocator) AllocationStats {
        return self.stats;
    }

    pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn recordAlloc(self: *CountingAllocator, len: usize) void {
        self.stats.allocation_events += 1;
        self.stats.allocated_bytes += len;
        self.stats.live_bytes += len;
        self.stats.peak_live_bytes = @max(self.stats.peak_live_bytes, self.stats.live_bytes);
    }

    fn recordFree(self: *CountingAllocator, len: usize) void {
        self.stats.free_events += 1;
        self.stats.freed_bytes += len;
        self.stats.live_bytes -|= len;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.recordAlloc(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.recordFree(memory.len);
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

pub fn main(init: std.process.Init) !void {
    var counting = CountingAllocator.init(std.heap.c_allocator);
    const alloc = counting.allocator();
    const cfg = try parseArgs(init.minimal.args);

    if (cfg.matrix) {
        try runMatrix(alloc, &counting, cfg);
        return;
    }

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const summary = try runBench(alloc, &counting, path, cfg);
    printSummary(cfg, summary);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--workload")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.workload = parseWorkload(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--primary")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.primary = parsePrimary(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--relational-index-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.relational_index_mode = parseRelationalIndexMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--constraint-probe")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.relational_constraint_probe = parseRelationalConstraintProbe(raw) orelse return error.InvalidArgument;
            cfg.matrix_all_constraint_probes = false;
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--overwrite-passes")) {
            cfg.overwrite_passes = try parseNextUsize(&args, "--overwrite-passes");
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, "--body-repeat");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--query-repeats")) {
            cfg.query_repeats = try parseNextUsize(&args, "--query-repeats");
        } else if (std.mem.eql(u8, arg, "--query-limit")) {
            cfg.query_limit = try parseNextUsize(&args, "--query-limit");
        } else if (std.mem.eql(u8, arg, "--query-total-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.query_total_mode = parseQueryTotalMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--query-shape")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.query_shape = parseQueryShape(raw) orelse return error.InvalidArgument;
            cfg.matrix_all_query_shapes = false;
        } else if (std.mem.eql(u8, arg, "--predicate-min-amount")) {
            cfg.predicate_min_amount = try parseNextUsize(&args, "--predicate-min-amount");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--bulk-session")) {
            cfg.bulk_session = true;
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--mutation-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.mutation_mode = parseMutationMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--overwrite-shape")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.overwrite_shape = parseOverwriteShape(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--matrix")) {
            cfg.matrix = true;
        } else if (std.mem.eql(u8, arg, "--matrix-preset")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.matrix_preset = parseMatrixPreset(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--matrix-docs")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.matrix_docs_len = try parseMatrixDocs(raw, &cfg.matrix_docs);
        } else if (std.mem.eql(u8, arg, "--matrix-fail-on-regression")) {
            cfg.matrix_fail_on_regression = true;
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-writes")) {
            cfg.matrix_max_ordered_vs_single_writes = try parseNextF64(&args, "--matrix-max-ordered-vs-single-writes");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-write-bytes")) {
            cfg.matrix_max_ordered_vs_single_write_bytes = try parseNextF64(&args, "--matrix-max-ordered-vs-single-write-bytes");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-stage-time")) {
            cfg.matrix_max_ordered_vs_single_stage_time = try parseNextF64(&args, "--matrix-max-ordered-vs-single-stage-time");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-total-time")) {
            cfg.matrix_max_ordered_vs_single_total_time = try parseNextF64(&args, "--matrix-max-ordered-vs-single-total-time");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-predicate")) {
            cfg.matrix_max_ordered_vs_single_predicate = try parseNextF64(&args, "--matrix-max-ordered-vs-single-predicate");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-stage-allocations")) {
            cfg.matrix_max_ordered_vs_single_stage_allocations = try parseNextF64(&args, "--matrix-max-ordered-vs-single-stage-allocations");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-stage-allocated-bytes")) {
            cfg.matrix_max_ordered_vs_single_stage_allocated_bytes = try parseNextF64(&args, "--matrix-max-ordered-vs-single-stage-allocated-bytes");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-query-allocations")) {
            cfg.matrix_max_ordered_vs_single_query_allocations = try parseNextF64(&args, "--matrix-max-ordered-vs-single-query-allocations");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-iterator-seeks")) {
            cfg.matrix_max_ordered_vs_single_iterator_seeks = try parseNextF64(&args, "--matrix-max-ordered-vs-single-iterator-seeks");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-residual-rechecks")) {
            cfg.matrix_max_ordered_vs_single_residual_rechecks = try parseNextF64(&args, "--matrix-max-ordered-vs-single-residual-rechecks");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-store-mutations")) {
            cfg.matrix_max_ordered_vs_single_store_mutations = try parseNextF64(&args, "--matrix-max-ordered-vs-single-store-mutations");
        } else if (std.mem.eql(u8, arg, "--matrix-max-ordered-vs-single-relational-mutations")) {
            cfg.matrix_max_ordered_vs_single_relational_mutations = try parseNextF64(&args, "--matrix-max-ordered-vs-single-relational-mutations");
        } else if (std.mem.eql(u8, arg, "--matrix-write-scenario")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.matrix_write_scenario = parseMatrixWriteScenario(raw) orelse return error.InvalidArgument;
            cfg.matrix_all_write_scenarios = false;
        } else if (std.mem.eql(u8, arg, "--matrix-write-scenarios")) {
            const raw = args.next() orelse return error.InvalidArgument;
            if (std.mem.eql(u8, raw, "all")) {
                cfg.matrix_all_write_scenarios = true;
                cfg.matrix_write_scenario = .current;
            } else {
                cfg.matrix_write_scenario = parseMatrixWriteScenario(raw) orelse return error.InvalidArgument;
                cfg.matrix_all_write_scenarios = false;
            }
        } else if (std.mem.eql(u8, arg, "--matrix-constraint-probes")) {
            const raw = args.next() orelse return error.InvalidArgument;
            if (std.mem.eql(u8, raw, "all")) {
                cfg.matrix_all_constraint_probes = true;
                cfg.relational_constraint_probe = .none;
            } else {
                cfg.relational_constraint_probe = parseRelationalConstraintProbe(raw) orelse return error.InvalidArgument;
                cfg.matrix_all_constraint_probes = false;
            }
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.overwrite_passes == 0 or cfg.body_repeat == 0 or cfg.batch_size == 0 or cfg.dims == 0) {
        return error.InvalidArgument;
    }
    if (cfg.query_limit == 0 or cfg.query_limit > std.math.maxInt(u32)) return error.InvalidArgument;
    if (cfg.matrix_max_ordered_vs_single_writes <= 0 or
        cfg.matrix_max_ordered_vs_single_write_bytes <= 0 or
        cfg.matrix_max_ordered_vs_single_stage_time <= 0 or
        cfg.matrix_max_ordered_vs_single_total_time <= 0 or
        cfg.matrix_max_ordered_vs_single_predicate <= 0 or
        cfg.matrix_max_ordered_vs_single_stage_allocations <= 0 or
        cfg.matrix_max_ordered_vs_single_stage_allocated_bytes <= 0 or
        cfg.matrix_max_ordered_vs_single_query_allocations <= 0 or
        cfg.matrix_max_ordered_vs_single_iterator_seeks <= 0 or
        cfg.matrix_max_ordered_vs_single_residual_rechecks <= 0 or
        cfg.matrix_max_ordered_vs_single_store_mutations <= 0 or
        cfg.matrix_max_ordered_vs_single_relational_mutations <= 0)
    {
        return error.InvalidArgument;
    }
    if (cfg.mutation_mode == .transform and cfg.workload == .explicit_dense) {
        std.debug.print("--mutation-mode transform is not supported for explicit_dense\n", .{});
        return error.InvalidArgument;
    }
    if (cfg.mutation_mode == .transform and cfg.workload == .relational_rows) {
        std.debug.print("--mutation-mode transform is not supported for relational_rows\n", .{});
        return error.InvalidArgument;
    }
    if (cfg.mutation_mode == .identity_rewrite and cfg.workload != .relational_rows) {
        std.debug.print("--mutation-mode identity-rewrite is only supported for relational_rows\n", .{});
        return error.InvalidArgument;
    }
    if (cfg.workload != .relational_rows and cfg.relational_index_mode != .single_column) {
        std.debug.print("--relational-index-mode is only supported for --workload relational_rows\n", .{});
        return error.InvalidArgument;
    }
    if (!cfg.matrix and cfg.workload != .relational_rows and cfg.relational_constraint_probe != .none) {
        std.debug.print("--constraint-probe is only supported for --workload relational_rows\n", .{});
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseWorkload(raw: []const u8) ?Workload {
    if (std.mem.eql(u8, raw, "documents")) return .documents;
    if (std.mem.eql(u8, raw, "relational_rows")) return .relational_rows;
    if (std.mem.eql(u8, raw, "explicit_full_text")) return .explicit_full_text;
    if (std.mem.eql(u8, raw, "explicit_dense")) return .explicit_dense;
    return null;
}

fn parsePrimary(raw: []const u8) ?PrimaryKind {
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    if (std.mem.eql(u8, raw, "lsm_memory")) return .lsm_memory;
    if (std.mem.eql(u8, raw, "mem")) return .mem;
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    return null;
}

fn parseMutationMode(raw: []const u8) ?MutationMode {
    if (std.mem.eql(u8, raw, "overwrite")) return .overwrite;
    if (std.mem.eql(u8, raw, "transform")) return .transform;
    if (std.mem.eql(u8, raw, "delete")) return .delete;
    if (std.mem.eql(u8, raw, "membership-change")) return .membership_change;
    if (std.mem.eql(u8, raw, "identity-rewrite")) return .identity_rewrite;
    return null;
}

fn parseOverwriteShape(raw: []const u8) ?OverwriteShape {
    if (std.mem.eql(u8, raw, "changing")) return .changing;
    if (std.mem.eql(u8, raw, "unchanged")) return .unchanged;
    return null;
}

fn parseMatrixPreset(raw: []const u8) ?MatrixPreset {
    if (std.mem.eql(u8, raw, "smoke")) return .smoke;
    if (std.mem.eql(u8, raw, "full")) return .full;
    return null;
}

fn parseMatrixWriteScenario(raw: []const u8) ?MatrixWriteScenario {
    if (std.mem.eql(u8, raw, "current")) return .current;
    if (std.mem.eql(u8, raw, "insert")) return .insert;
    if (std.mem.eql(u8, raw, "overwrite-changed")) return .overwrite_changed;
    if (std.mem.eql(u8, raw, "overwrite-unchanged")) return .overwrite_unchanged;
    if (std.mem.eql(u8, raw, "delete")) return .delete;
    if (std.mem.eql(u8, raw, "membership-change")) return .membership_change;
    if (std.mem.eql(u8, raw, "identity-rewrite")) return .identity_rewrite;
    return null;
}

fn parseRelationalIndexMode(raw: []const u8) ?RelationalIndexMode {
    if (std.mem.eql(u8, raw, "none")) return .none;
    if (std.mem.eql(u8, raw, "single-column")) return .single_column;
    if (std.mem.eql(u8, raw, "ordered-tuple")) return .ordered_tuple;
    if (std.mem.eql(u8, raw, "partial-single-column")) return .partial_single_column;
    if (std.mem.eql(u8, raw, "partial-ordered-tuple")) return .partial_ordered_tuple;
    return null;
}

fn parseRelationalConstraintProbe(raw: []const u8) ?RelationalConstraintProbe {
    if (std.mem.eql(u8, raw, "none")) return .none;
    if (std.mem.eql(u8, raw, "unique")) return .unique;
    if (std.mem.eql(u8, raw, "foreign-key")) return .foreign_key;
    if (std.mem.eql(u8, raw, "foreign_key")) return .foreign_key;
    return null;
}

fn parseQueryTotalMode(raw: []const u8) ?db_mod.types.RelationalRowsQueryRequest.TotalMode {
    if (std.mem.eql(u8, raw, "exact")) return .exact;
    if (std.mem.eql(u8, raw, "bounded")) return .bounded;
    if (std.mem.eql(u8, raw, "none")) return .none;
    return null;
}

fn parseQueryShape(raw: []const u8) ?QueryShape {
    if (std.mem.eql(u8, raw, "range")) return .range;
    if (std.mem.eql(u8, raw, "equality")) return .equality;
    if (std.mem.eql(u8, raw, "ordered-pagination")) return .ordered_pagination;
    return null;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn parseNextF64(args: *std.process.Args.Iterator, flag: []const u8) !f64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseFloat(f64, raw);
}

fn parseMatrixDocs(raw: []const u8, out: *[4]usize) !usize {
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        if (part.len == 0 or count == out.len) return error.InvalidArgument;
        const docs = try std.fmt.parseInt(usize, part, 10);
        if (docs == 0) return error.InvalidArgument;
        out[count] = docs;
        count += 1;
    }
    if (count == 0) return error.InvalidArgument;
    return count;
}

fn runMatrix(alloc: std.mem.Allocator, counting: *CountingAllocator, base_cfg: Config) !void {
    var doc_counts_buf = base_cfg.matrix_docs;
    var doc_counts_len = base_cfg.matrix_docs_len;
    if (doc_counts_len == 0) {
        switch (base_cfg.matrix_preset) {
            .smoke => {
                doc_counts_buf = .{ 10_000, 0, 0, 0 };
                doc_counts_len = 1;
            },
            .full => {
                doc_counts_buf = .{ 10_000, 100_000, 1_000_000, 0 };
                doc_counts_len = 3;
            },
        }
    }

    const predicate_thresholds = [_]usize{ 250, 900 };
    const all_query_shapes = [_]QueryShape{ .range, .equality, .ordered_pagination };
    const selected_query_shapes = [_]QueryShape{base_cfg.query_shape};
    const query_shapes = if (base_cfg.matrix_all_query_shapes) all_query_shapes[0..] else selected_query_shapes[0..];
    const all_write_scenarios = [_]MatrixWriteScenario{ .insert, .overwrite_changed, .overwrite_unchanged, .delete, .membership_change, .identity_rewrite };
    const selected_write_scenarios = [_]MatrixWriteScenario{base_cfg.matrix_write_scenario};
    const write_scenarios = if (base_cfg.matrix_all_write_scenarios) all_write_scenarios[0..] else selected_write_scenarios[0..];
    const all_constraint_probes = [_]RelationalConstraintProbe{ .none, .unique, .foreign_key };
    const selected_constraint_probes = [_]RelationalConstraintProbe{base_cfg.relational_constraint_probe};
    const constraint_probes = if (base_cfg.matrix_all_constraint_probes) all_constraint_probes[0..] else selected_constraint_probes[0..];
    const relational_modes = [_]RelationalIndexMode{ .none, .single_column, .ordered_tuple, .partial_single_column, .partial_ordered_tuple };
    const total_modes = [_]db_mod.types.RelationalRowsQueryRequest.TotalMode{ .none, .bounded, .exact };
    var regression_failures: usize = 0;

    std.debug.print(
        "batch_bench_matrix_start preset={s} docs={s} write_scenarios={s} constraint_probes={s} overwrite_passes={d} overwrite_shape={s} query_shapes={s} batch_size={d} query_repeats={d} query_limit={d} primary={s} sync={s} fail_on_regression={any} max_ordered_vs_single_writes={d:.3} max_ordered_vs_single_write_bytes={d:.3} max_ordered_vs_single_stage_time={d:.3} max_ordered_vs_single_total_time={d:.3} max_ordered_vs_single_predicate={d:.3} max_ordered_vs_single_stage_allocations={d:.3} max_ordered_vs_single_stage_allocated_bytes={d:.3} max_ordered_vs_single_query_allocations={d:.3} max_ordered_vs_single_iterator_seeks={d:.3} max_ordered_vs_single_residual_rechecks={d:.3} max_ordered_vs_single_store_mutations={d:.3} max_ordered_vs_single_relational_mutations={d:.3}\n",
        .{
            @tagName(base_cfg.matrix_preset),
            if (base_cfg.matrix_docs_len == 0) @tagName(base_cfg.matrix_preset) else "custom",
            if (base_cfg.matrix_all_write_scenarios) "insert,overwrite_changed,overwrite_unchanged,delete,membership_change,identity_rewrite" else @tagName(base_cfg.matrix_write_scenario),
            if (base_cfg.matrix_all_constraint_probes) "none,unique,foreign_key" else @tagName(base_cfg.relational_constraint_probe),
            base_cfg.overwrite_passes,
            @tagName(base_cfg.overwrite_shape),
            if (base_cfg.matrix_all_query_shapes) "range,equality,ordered_pagination" else @tagName(base_cfg.query_shape),
            base_cfg.batch_size,
            if (base_cfg.query_repeats == 0) @as(usize, 3) else base_cfg.query_repeats,
            base_cfg.query_limit,
            @tagName(base_cfg.primary),
            db_mod.types.publicSyncLevelText(base_cfg.sync_level),
            base_cfg.matrix_fail_on_regression,
            base_cfg.matrix_max_ordered_vs_single_writes,
            base_cfg.matrix_max_ordered_vs_single_write_bytes,
            base_cfg.matrix_max_ordered_vs_single_stage_time,
            base_cfg.matrix_max_ordered_vs_single_total_time,
            base_cfg.matrix_max_ordered_vs_single_predicate,
            base_cfg.matrix_max_ordered_vs_single_stage_allocations,
            base_cfg.matrix_max_ordered_vs_single_stage_allocated_bytes,
            base_cfg.matrix_max_ordered_vs_single_query_allocations,
            base_cfg.matrix_max_ordered_vs_single_iterator_seeks,
            base_cfg.matrix_max_ordered_vs_single_residual_rechecks,
            base_cfg.matrix_max_ordered_vs_single_store_mutations,
            base_cfg.matrix_max_ordered_vs_single_relational_mutations,
        },
    );

    for (doc_counts_buf[0..doc_counts_len]) |docs| {
        for (write_scenarios, 0..) |write_scenario, write_scenario_idx| {
            const scenario_base_cfg = matrixConfigForWriteScenario(base_cfg, write_scenario);
            for (constraint_probes, 0..) |constraint_probe, constraint_probe_idx| {
                for (query_shapes, 0..) |query_shape, query_shape_idx| {
                    for (predicate_thresholds) |predicate_min_amount| {
                        var relational_summaries: [relational_modes.len][total_modes.len]Summary = undefined;
                        var relational_seen: [relational_modes.len][total_modes.len]bool = .{.{false} ** total_modes.len} ** relational_modes.len;

                        for (relational_modes, 0..) |mode, mode_idx| {
                            for (total_modes, 0..) |total_mode, total_idx| {
                                var cfg = scenario_base_cfg;
                                cfg.matrix = false;
                                cfg.workload = .relational_rows;
                                cfg.relational_index_mode = mode;
                                cfg.relational_constraint_probe = constraint_probe;
                                cfg.docs = docs;
                                cfg.query_repeats = if (base_cfg.query_repeats == 0) 3 else base_cfg.query_repeats;
                                cfg.query_shape = query_shape;
                                cfg.query_total_mode = total_mode;
                                cfg.predicate_min_amount = predicate_min_amount;
                                cfg.batch_size = @min(base_cfg.batch_size, docs);

                                var path_buf: [256]u8 = undefined;
                                const path = matrixTempPath(&path_buf, "rel", docs, predicate_min_amount, write_scenario_idx * constraint_probes.len * query_shapes.len * relational_modes.len * total_modes.len + constraint_probe_idx * query_shapes.len * relational_modes.len * total_modes.len + query_shape_idx * relational_modes.len * total_modes.len + mode_idx * total_modes.len + total_idx);
                                defer cleanupTempDir(path);
                                const summary = try runBench(alloc, counting, path, cfg);
                                relational_summaries[mode_idx][total_idx] = summary;
                                relational_seen[mode_idx][total_idx] = true;
                                printMatrixResult(cfg, selectivityLabel(predicate_min_amount), summary);
                            }
                        }

                        for (total_modes, 0..) |total_mode, total_idx| {
                            if (relational_seen[0][total_idx] and relational_seen[1][total_idx] and relational_seen[2][total_idx]) {
                                const comparison = matrixRelationalComparison(
                                    docs,
                                    write_scenario,
                                    constraint_probe,
                                    scenario_base_cfg.mutation_mode,
                                    scenario_base_cfg.overwrite_shape,
                                    query_shape,
                                    predicate_min_amount,
                                    selectivityLabel(predicate_min_amount),
                                    total_mode,
                                    relational_summaries[0][total_idx],
                                    relational_summaries[1][total_idx],
                                    relational_summaries[2][total_idx],
                                );
                                printMatrixRelationalComparison(comparison);
                                if (matrixComparisonFailsGuardrail(base_cfg, comparison)) {
                                    regression_failures += 1;
                                    printMatrixRegressionFailure(base_cfg, comparison);
                                }
                            }
                        }

                        if (constraint_probe == .none and write_scenario != .identity_rewrite) {
                            var document_cfg = scenario_base_cfg;
                            document_cfg.matrix = false;
                            document_cfg.workload = .documents;
                            document_cfg.relational_index_mode = .single_column;
                            document_cfg.relational_constraint_probe = .none;
                            document_cfg.docs = docs;
                            document_cfg.query_repeats = if (base_cfg.query_repeats == 0) 3 else base_cfg.query_repeats;
                            document_cfg.query_shape = query_shape;
                            document_cfg.query_total_mode = .none;
                            document_cfg.predicate_min_amount = predicate_min_amount;
                            document_cfg.batch_size = @min(base_cfg.batch_size, docs);

                            var document_path_buf: [256]u8 = undefined;
                            const document_path = matrixTempPath(&document_path_buf, "doc", docs, predicate_min_amount, write_scenario_idx * query_shapes.len + query_shape_idx);
                            defer cleanupTempDir(document_path);
                            const document_summary = try runBench(alloc, counting, document_path, document_cfg);
                            printMatrixResult(document_cfg, selectivityLabel(predicate_min_amount), document_summary);
                        } else if (constraint_probe == .none and write_scenario == .identity_rewrite) {
                            std.debug.print(
                                "batch_bench_matrix_skip write_scenario={s} workload=documents reason=document_identity_rewrite_not_supported docs={d} query_shape={s} selectivity={s}\n",
                                .{
                                    @tagName(write_scenario),
                                    docs,
                                    @tagName(query_shape),
                                    selectivityLabel(predicate_min_amount),
                                },
                            );
                        }
                    }
                }
            }
        }
    }
    std.debug.print("batch_bench_matrix_end regression_failures={d}\n", .{regression_failures});
    if (base_cfg.matrix_fail_on_regression and regression_failures != 0) return error.BenchmarkRegression;
}

fn matrixConfigForWriteScenario(base_cfg: Config, scenario: MatrixWriteScenario) Config {
    var cfg = base_cfg;
    cfg.matrix_write_scenario = scenario;
    cfg.matrix_all_write_scenarios = false;
    switch (scenario) {
        .current => {},
        .insert => {
            cfg.mutation_mode = .overwrite;
            cfg.overwrite_shape = .changing;
            cfg.overwrite_passes = 1;
        },
        .overwrite_changed => {
            cfg.mutation_mode = .overwrite;
            cfg.overwrite_shape = .changing;
            cfg.overwrite_passes = @max(base_cfg.overwrite_passes, 2);
        },
        .overwrite_unchanged => {
            cfg.mutation_mode = .overwrite;
            cfg.overwrite_shape = .unchanged;
            cfg.overwrite_passes = @max(base_cfg.overwrite_passes, 2);
        },
        .delete => {
            cfg.mutation_mode = .delete;
            cfg.overwrite_shape = .changing;
            cfg.overwrite_passes = 1;
        },
        .membership_change => {
            cfg.mutation_mode = .membership_change;
            cfg.overwrite_shape = .changing;
            cfg.overwrite_passes = 1;
        },
        .identity_rewrite => {
            cfg.mutation_mode = .identity_rewrite;
            cfg.overwrite_shape = .changing;
            cfg.overwrite_passes = 1;
            cfg.workload = .relational_rows;
        },
    }
    return cfg;
}

fn runBench(alloc: std.mem.Allocator, counting: *CountingAllocator, path: []const u8, cfg: Config) !Summary {
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var opts = openOptions(cfg);
    opts.resource_manager = &resource_manager;

    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    try configureIndexes(alloc, &db, cfg);

    var total_profile = db_mod.BatchProfile{};
    var max_batch_ns: u64 = 0;
    var batches: usize = 0;
    const requested_writes = switch (cfg.mutation_mode) {
        .overwrite => cfg.docs * cfg.overwrite_passes,
        .transform => cfg.docs,
        .delete => cfg.docs,
        .membership_change => cfg.docs * 2,
        .identity_rewrite => cfg.docs,
    };
    const requested_deletes = if (cfg.mutation_mode == .delete) cfg.docs else 0;
    const requested_transforms = switch (cfg.mutation_mode) {
        .overwrite, .delete, .membership_change, .identity_rewrite => 0,
        .transform => cfg.docs * (cfg.overwrite_passes - 1),
    };

    var empty_vector: [0]f32 = .{};
    const vector_buf = if (cfg.workload == .explicit_dense) try alloc.alloc(f32, cfg.dims) else empty_vector[0..];
    defer if (cfg.workload == .explicit_dense) alloc.free(vector_buf);

    if (cfg.bulk_session) try db.beginBulkIngestSession();
    errdefer if (cfg.bulk_session) db.abortBulkIngestSession();

    counting.reset();
    const stage_start_ns = nowNs();
    var start_doc: usize = 0;
    const stage_passes: usize = if (cfg.mutation_mode == .delete or cfg.mutation_mode == .membership_change or cfg.mutation_mode == .identity_rewrite) 2 else cfg.overwrite_passes;
    for (0..stage_passes) |pass_idx| {
        start_doc = 0;
        while (start_doc < cfg.docs) : (start_doc += cfg.batch_size) {
            const end_doc = @min(start_doc + cfg.batch_size, cfg.docs);
            var profile = db_mod.BatchProfile{};
            switch (cfg.mutation_mode) {
                .overwrite => {
                    const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                    defer freeWrites(alloc, writes);
                    try db.batchProfiled(.{
                        .writes = writes,
                        .sync_level = cfg.sync_level,
                    }, &profile);
                },
                .transform => {
                    if (pass_idx == 0) {
                        const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                        defer freeWrites(alloc, writes);
                        try db.batchProfiled(.{
                            .writes = writes,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    } else {
                        const transforms = try buildTransforms(alloc, cfg, pass_idx, start_doc, end_doc);
                        defer freeTransforms(alloc, transforms);
                        try db.batchProfiled(.{
                            .transforms = transforms,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    }
                },
                .delete => {
                    if (pass_idx == 0) {
                        const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                        defer freeWrites(alloc, writes);
                        try db.batchProfiled(.{
                            .writes = writes,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    } else {
                        const deletes = try buildDeletes(alloc, start_doc, end_doc);
                        defer freeDeletes(alloc, deletes);
                        try db.batchProfiled(.{
                            .deletes = deletes,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    }
                },
                .membership_change => {
                    const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                    defer freeWrites(alloc, writes);
                    try db.batchProfiled(.{
                        .writes = writes,
                        .sync_level = cfg.sync_level,
                    }, &profile);
                },
                .identity_rewrite => {
                    if (pass_idx == 0) {
                        const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                        defer freeWrites(alloc, writes);
                        try db.batchProfiled(.{
                            .writes = writes,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    } else {
                        const rewrites = try buildIdentityRewrites(alloc, cfg, pass_idx, start_doc, end_doc);
                        defer freeIdentityRewrites(alloc, rewrites);
                        try db.batchProfiled(.{
                            .relational_identity_rewrites = rewrites,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    }
                },
            }
            addBatchProfile(&total_profile, profile);
            max_batch_ns = @max(max_batch_ns, profile.total_ns);
            batches += 1;
        }
    }
    const stage_ns = elapsedSince(stage_start_ns);
    const stage_allocations = counting.snapshot();

    var finish_ns: u64 = 0;
    if (cfg.bulk_session) {
        const finish_start_ns = nowNs();
        try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
        finish_ns = elapsedSince(finish_start_ns);
    }
    var query = QueryStats{};
    if (cfg.query_repeats > 0) {
        counting.reset();
        query = try runQueryProbes(alloc, &db, cfg);
        query.allocations = counting.snapshot();
    }

    return .{
        .requested_writes = requested_writes,
        .requested_deletes = requested_deletes,
        .requested_transforms = requested_transforms,
        .batches = batches,
        .stage_ns = stage_ns,
        .finish_ns = finish_ns,
        .total_ns = stage_ns + finish_ns,
        .max_batch_ns = max_batch_ns,
        .profile = total_profile,
        .replay = try snapshotReplayStats(alloc, &db),
        .async_indexing = db.snapshotAsyncIndexingStats(),
        .query = query,
        .stage_allocations = stage_allocations,
    };
}

fn openOptions(cfg: Config) db_mod.OpenOptions {
    var opts: db_mod.OpenOptions = .{
        .start_index_workers = false,
    };
    switch (cfg.primary) {
        .lsm => {},
        .lsm_memory => opts.primary_backend = .{ .lsm_memory = .{} },
        .mem => opts.primary_backend = .{ .mem = .{} },
        .lmdb => opts.primary_backend = .lmdb,
    }
    return opts;
}

fn configureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    switch (cfg.workload) {
        .documents => return,
        .relational_rows => {
            const schema_json = try relationalSchemaJsonAlloc(alloc, cfg.relational_index_mode, cfg.relational_constraint_probe);
            defer alloc.free(schema_json);
            var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
            defer parsed_schema.deinit(alloc);
            const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
            defer schema_mod.freeSchema(alloc, runtime_schema);
            try db.setSchema(runtime_schema);
        },
        .explicit_full_text => try db.addIndex(.{
            .name = "ft_idx",
            .kind = .full_text,
            .config_json = "{\"field\":\"body\"}",
        }),
        .explicit_dense => {
            const cfg_json = try std.fmt.allocPrint(
                alloc,
                "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}",
                .{cfg.dims},
            );
            defer alloc.free(cfg_json);
            try db.addIndex(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = cfg_json,
            });
        },
    }
}

fn buildWrites(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
    vector_buf: []f32,
) ![]db_mod.types.BatchWrite {
    const writes = try alloc.alloc(db_mod.types.BatchWrite, end_doc - start_doc);
    errdefer {
        for (writes) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        alloc.free(writes);
    }
    for (writes, start_doc..) |*write, doc_idx| {
        write.* = try makeBatchWrite(alloc, cfg, doc_idx, pass_idx, vector_buf);
    }
    return writes;
}

fn buildTransforms(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
) ![]db_mod.types.DocumentTransform {
    const transforms = try alloc.alloc(db_mod.types.DocumentTransform, end_doc - start_doc);
    errdefer {
        for (transforms) |transform| freeTransform(alloc, transform);
        alloc.free(transforms);
    }
    for (transforms, start_doc..) |*transform, doc_idx| {
        transform.* = try makeDocumentTransform(alloc, cfg, doc_idx, pass_idx);
    }
    return transforms;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    alloc.free(writes);
}

fn buildDeletes(
    alloc: std.mem.Allocator,
    start_doc: usize,
    end_doc: usize,
) ![][]const u8 {
    const deletes = try alloc.alloc([]const u8, end_doc - start_doc);
    errdefer {
        for (deletes) |key| alloc.free(@constCast(key));
        alloc.free(deletes);
    }
    for (deletes, start_doc..) |*key, doc_idx| {
        key.* = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    }
    return deletes;
}

fn buildIdentityRewrites(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
) ![]db_mod.types.RelationalIdentityRewrite {
    const rewrites = try alloc.alloc(db_mod.types.RelationalIdentityRewrite, end_doc - start_doc);
    var filled: usize = 0;
    errdefer {
        for (rewrites[0..filled]) |rewrite| freeIdentityRewrite(alloc, rewrite);
        alloc.free(rewrites);
    }
    for (rewrites, start_doc..) |*rewrite, doc_idx| {
        rewrite.* = try makeRelationalIdentityRewrite(alloc, cfg, doc_idx, pass_idx);
        filled += 1;
    }
    return rewrites;
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(@constCast(key));
    alloc.free(deletes);
}

fn freeIdentityRewrites(alloc: std.mem.Allocator, rewrites: []db_mod.types.RelationalIdentityRewrite) void {
    for (rewrites) |rewrite| freeIdentityRewrite(alloc, rewrite);
    alloc.free(rewrites);
}

fn freeIdentityRewrite(alloc: std.mem.Allocator, rewrite: db_mod.types.RelationalIdentityRewrite) void {
    alloc.free(@constCast(rewrite.old_key));
    alloc.free(@constCast(rewrite.new_key));
    alloc.free(@constCast(rewrite.value));
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    for (transforms) |transform| freeTransform(alloc, transform);
    alloc.free(transforms);
}

fn freeTransform(alloc: std.mem.Allocator, transform: db_mod.types.DocumentTransform) void {
    alloc.free(transform.key);
    for (transform.operations) |op| {
        if (op.value_json) |value_json| alloc.free(value_json);
    }
    alloc.free(transform.operations);
}

fn makeBatchWrite(
    alloc: std.mem.Allocator,
    cfg: Config,
    doc_idx: usize,
    pass_idx: usize,
    vector_buf: []f32,
) !db_mod.types.BatchWrite {
    const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    errdefer alloc.free(key);
    const value_pass_idx: usize = switch (cfg.mutation_mode) {
        .membership_change => pass_idx,
        else => switch (cfg.overwrite_shape) {
            .changing => pass_idx,
            .unchanged => 0,
        },
    };
    const value = switch (cfg.workload) {
        .documents, .relational_rows, .explicit_full_text => try encodeDocumentJsonAlloc(alloc, doc_idx, value_pass_idx, cfg),
        .explicit_dense => try encodeVectorDocJsonAlloc(alloc, doc_idx, cfg, vector_buf),
    };
    return .{
        .key = key,
        .value = value,
    };
}

fn makeRelationalIdentityRewrite(
    alloc: std.mem.Allocator,
    cfg: Config,
    doc_idx: usize,
    pass_idx: usize,
) !db_mod.types.RelationalIdentityRewrite {
    const old_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    errdefer alloc.free(old_key);
    const new_key = try std.fmt.allocPrint(alloc, "doc-rewritten:{d:0>8}", .{doc_idx});
    errdefer alloc.free(new_key);
    const value = try encodeIdentityRewriteDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg);
    return .{
        .old_key = old_key,
        .new_key = new_key,
        .value = value,
    };
}

fn makeDocumentTransform(
    alloc: std.mem.Allocator,
    cfg: Config,
    doc_idx: usize,
    pass_idx: usize,
) !db_mod.types.DocumentTransform {
    const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    errdefer alloc.free(key);

    const title_value = try std.fmt.allocPrint(alloc, "\"doc-{d}-pass-{d}\"", .{ doc_idx, pass_idx });
    errdefer alloc.free(title_value);

    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg);
    defer alloc.free(body);
    const body_value = try std.fmt.allocPrint(alloc, "\"{s}\"", .{body});
    errdefer alloc.free(body_value);

    const operations = try alloc.alloc(db_mod.types.TransformOp, 2);
    operations[0] = .{
        .op = .set,
        .path = "title",
        .value_json = title_value,
    };
    operations[1] = .{
        .op = .set,
        .path = "body",
        .value_json = body_value,
    };

    return .{
        .key = key,
        .operations = operations,
    };
}

fn encodeDocumentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg);
    defer alloc.free(body);
    const status = statusForDocPass(cfg, doc_idx, pass_idx);
    const amount_pass_idx: usize = if (cfg.mutation_mode == .membership_change) 0 else pass_idx;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"id\":\"doc:{d:0>8}\",\"title\":\"doc-{d}\",\"status\":\"{s}\",\"amount\":{d},\"created_at\":{d},\"body\":\"{s}\"}}",
        .{ doc_idx, doc_idx, status, amountForDoc(doc_idx, amount_pass_idx), createdAtForDoc(doc_idx), body },
    );
}

fn encodeIdentityRewriteDocumentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg);
    defer alloc.free(body);
    const status = statusForDocPass(cfg, doc_idx, pass_idx);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"id\":\"doc-rewritten:{d:0>8}\",\"title\":\"doc-{d}\",\"status\":\"{s}\",\"amount\":{d},\"created_at\":{d},\"body\":\"{s}\"}}",
        .{ doc_idx, doc_idx, status, amountForDoc(doc_idx, pass_idx), createdAtForDoc(doc_idx), body },
    );
}

fn encodeVectorDocJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, cfg: Config, vector: []f32) ![]u8 {
    var norm_sq: f32 = 0;
    for (vector, 0..) |*slot, dim_idx| {
        const noise = deterministicNoise(cfg.seed, doc_idx, dim_idx);
        const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
        slot.* = cluster + noise;
        norm_sq += slot.* * slot.*;
    }
    const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
    for (vector) |*slot| slot.* *= inv_norm;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc-{d}\",\"embedding\":{f}}}",
        .{ doc_idx, std.json.fmt(vector, .{}) },
    );
}

fn generatedBodyTextAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
    const topic = switch (doc_idx % 8) {
        0 => "alpha",
        1 => "beta",
        2 => "gamma",
        3 => "delta",
        4 => "epsilon",
        5 => "zeta",
        6 => "eta",
        else => "theta",
    };
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);
    const prefix = try std.fmt.allocPrint(alloc, "document {d} pass {d} topic {s} dims {d}", .{ doc_idx, pass_idx, topic, cfg.dims });
    defer alloc.free(prefix);
    try buf.appendSlice(alloc, prefix);
    for (0..cfg.body_repeat) |repeat_idx| {
        const segment = try std.fmt.allocPrint(alloc, " repeated context {s} token {d}", .{ topic, repeat_idx });
        defer alloc.free(segment);
        try buf.appendSlice(alloc, segment);
    }
    const suffix = try std.fmt.allocPrint(alloc, " tail {d}", .{(doc_idx + pass_idx) % 97});
    defer alloc.free(suffix);
    try buf.appendSlice(alloc, suffix);
    return try buf.toOwnedSlice(alloc);
}

fn deterministicNoise(seed: u64, doc_idx: usize, dim_idx: usize) f32 {
    var x = seed ^
        (@as(u64, @intCast(doc_idx + 1)) *% 0x9E3779B97F4A7C15) ^
        (@as(u64, @intCast(dim_idx + 1)) *% 0xC2B2AE3D27D4EB4F);
    x ^= x >> 33;
    x *%= 0xFF51AFD7ED558CCD;
    x ^= x >> 33;
    x *%= 0xC4CEB9FE1A85EC53;
    x ^= x >> 33;
    const scaled = @as(f32, @floatFromInt(x & 1023)) / 1024.0;
    return scaled * 0.01;
}

fn runQueryProbes(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !QueryStats {
    var stats = QueryStats{
        .lookup_repeats = cfg.query_repeats,
        .predicate_repeats = cfg.query_repeats,
    };

    var key_buf: [32]u8 = undefined;
    const lookup_start = nowNs();
    for (0..cfg.query_repeats) |idx| {
        const key = std.fmt.bufPrint(&key_buf, "doc:{d:0>8}", .{idx % cfg.docs}) catch unreachable;
        if (try db.lookup(alloc, key, .{})) |lookup_result| {
            var result = lookup_result;
            stats.lookup_hits += 1;
            result.deinit(alloc);
        }
    }
    stats.lookup_ns = elapsedSince(lookup_start);

    const predicate_start = nowNs();
    for (0..cfg.query_repeats) |_| {
        switch (cfg.workload) {
            .relational_rows => {
                const runtime_schema = db.core.schema orelse return error.InvalidArgument;
                var amount_buf: [32]u8 = undefined;
                const amount_json = try std.fmt.bufPrint(
                    &amount_buf,
                    "{d}",
                    .{if (cfg.query_shape == .equality) equalityProbeAmount(cfg.docs) else cfg.predicate_min_amount},
                );
                const range_predicates = [_]schema_mod.RelationalCheck{
                    .{ .name = "", .field = "status", .op = .eq, .value_json = "\"open\"" },
                    .{ .name = "", .field = "amount", .op = .gt, .value_json = amount_json },
                };
                const equality_predicates = [_]schema_mod.RelationalCheck{
                    .{ .name = "", .field = "status", .op = .eq, .value_json = "\"open\"" },
                    .{ .name = "", .field = "amount", .op = .eq, .value_json = amount_json },
                };
                const ordered_predicates = [_]schema_mod.RelationalCheck{
                    .{ .name = "", .field = "status", .op = .eq, .value_json = "\"open\"" },
                };
                const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
                    .field = "amount",
                    .direction = .asc,
                }};
                const select = [_][]const u8{ "id", "amount" };
                const predicates = switch (cfg.query_shape) {
                    .range => range_predicates[0..],
                    .equality => equality_predicates[0..],
                    .ordered_pagination => ordered_predicates[0..],
                };
                const orders = if (cfg.query_shape == .ordered_pagination) order_by[0..] else &[_]db_mod.types.RelationalRowsQueryOrder{};
                var result = try db.queryRelationalRows(alloc, runtime_schema, .{
                    .predicates = predicates[0..],
                    .select = select[0..],
                    .select_all = false,
                    .order_by = orders,
                    .limit = @intCast(cfg.query_limit),
                    .total_mode = cfg.query_total_mode,
                });
                stats.predicate_hits += result.rows.len;
                if (result.total_exact) stats.predicate_exact_totals += 1;
                stats.index_entries_scanned += result.profile.index_entries_scanned;
                stats.candidate_rows += result.profile.candidate_rows;
                stats.candidate_gate_limit = @max(stats.candidate_gate_limit, result.profile.candidate_gate_limit);
                stats.candidate_gate_observed = @max(stats.candidate_gate_observed, result.profile.candidate_gate_observed);
                stats.iterator_seeks += result.profile.iterator_seeks;
                stats.hydrated_rows += result.profile.hydrated_rows;
                stats.residual_rechecks += result.profile.residual_rechecks;
                stats.covering_payload_rows += result.profile.covering_payload_rows;
                stats.projected_rows += result.profile.projected_rows;
                if (result.profile.ordered_tuple_plan_selected) {
                    stats.ordered_tuple_plan_queries += 1;
                    stats.ordered_tuple_max_key_count = @max(stats.ordered_tuple_max_key_count, result.profile.ordered_tuple_key_count);
                    stats.ordered_tuple_max_equality_prefix_len = @max(stats.ordered_tuple_max_equality_prefix_len, result.profile.ordered_tuple_equality_prefix_len);
                    stats.ordered_tuple_max_filter_predicates = @max(stats.ordered_tuple_max_filter_predicates, result.profile.ordered_tuple_filter_predicates);
                    stats.ordered_tuple_max_proven_predicates = @max(stats.ordered_tuple_max_proven_predicates, result.profile.ordered_tuple_proven_predicates);
                    stats.ordered_tuple_max_residual_predicates = @max(stats.ordered_tuple_max_residual_predicates, result.profile.ordered_tuple_residual_predicates);
                    if (result.profile.ordered_tuple_range_key_index != std.math.maxInt(u32)) stats.ordered_tuple_range_plan_queries += 1;
                    if (result.profile.ordered_tuple_prefix_scan) stats.ordered_tuple_prefix_scan_queries += 1;
                }
                switch (result.profile.access_method) {
                    .unknown => stats.unknown_access_queries += 1,
                    .base_scan => stats.base_scan_queries += 1,
                    .resolved_doc_set => stats.resolved_doc_set_queries += 1,
                    .ordered_tuple_doc_set => stats.ordered_tuple_doc_set_queries += 1,
                    .ordered_tuple_stream => stats.ordered_tuple_stream_queries += 1,
                }
                switch (result.profile.fallback_reason) {
                    .none => {},
                    .ordered_tuple_candidate_gate => stats.ordered_tuple_candidate_gate_fallbacks += 1,
                    .ordered_tuple_materialization_cap => stats.ordered_tuple_materialization_cap_fallbacks += 1,
                    .ordered_tuple_skipped_for_exact_paged_total => stats.ordered_tuple_exact_paged_total_fallbacks += 1,
                    .ordered_tuple_ordering_not_covered,
                    .ordered_tuple_order_field_not_covered,
                    .ordered_tuple_order_direction_not_covered,
                    .ordered_tuple_order_nulls_not_covered,
                    .ordered_tuple_order_collation_not_covered,
                    .ordered_tuple_collation_not_supported,
                    => stats.ordered_tuple_ordering_not_covered_fallbacks += 1,
                    .ordered_tuple_index_not_ready => stats.ordered_tuple_index_not_ready_fallbacks += 1,
                    .ordered_tuple_stale_generation => stats.ordered_tuple_stale_generation_fallbacks += 1,
                    .ordered_tuple_access_method_mismatch => stats.ordered_tuple_stale_generation_fallbacks += 1,
                    .ordered_tuple_predicate_not_proven => stats.ordered_tuple_predicate_not_proven_fallbacks += 1,
                    .ordered_tuple_no_usable_bounds => stats.ordered_tuple_no_usable_bounds_fallbacks += 1,
                }
                result.deinit(alloc);
            },
            .documents, .explicit_full_text => {
                const amount = if (cfg.query_shape == .equality) equalityProbeAmount(cfg.docs) else cfg.predicate_min_amount;
                const filter_query_json = switch (cfg.query_shape) {
                    .range, .ordered_pagination => try std.fmt.allocPrint(
                        alloc,
                        "{{\"bool\":{{\"must\":[{{\"term\":{{\"status\":\"open\"}}}},{{\"numeric_range\":{{\"field\":\"amount\",\"min\":{d}}}}}]}}}}",
                        .{amount},
                    ),
                    .equality => try std.fmt.allocPrint(
                        alloc,
                        "{{\"bool\":{{\"must\":[{{\"term\":{{\"status\":\"open\"}}}},{{\"numeric_range\":{{\"field\":\"amount\",\"min\":{d},\"max\":{d},\"inclusive_min\":true,\"inclusive_max\":true}}}}]}}}}",
                        .{ amount, amount },
                    ),
                };
                defer alloc.free(filter_query_json);
                var result = try db.search(alloc, .{
                    .query = .{ .match_all = {} },
                    .filter_query_json = filter_query_json,
                    .limit = @intCast(cfg.query_limit),
                    .include_stored = false,
                });
                stats.predicate_hits += result.hits.len;
                result.deinit();
            },
            .explicit_dense => {
                var result = try db.search(alloc, .{
                    .query = .{ .match_all = {} },
                    .limit = @intCast(cfg.query_limit),
                    .include_stored = false,
                });
                stats.predicate_hits += result.hits.len;
                result.deinit();
            },
        }
    }
    stats.predicate_ns = elapsedSince(predicate_start);

    return stats;
}

fn statusForDoc(doc_idx: usize) []const u8 {
    return switch (doc_idx % 4) {
        0, 1 => "open",
        2 => "pending",
        else => "closed",
    };
}

fn statusForDocPass(cfg: Config, doc_idx: usize, pass_idx: usize) []const u8 {
    if (cfg.mutation_mode == .membership_change) return if (pass_idx == 0) "closed" else "open";
    return statusForDoc(doc_idx);
}

fn amountForDoc(doc_idx: usize, pass_idx: usize) usize {
    return (doc_idx * 17 + pass_idx * 31) % 1000;
}

fn equalityProbeAmount(docs: usize) usize {
    return amountForDoc(if (docs > 20) 20 else 0, 0);
}

fn createdAtForDoc(doc_idx: usize) usize {
    return 1_700_000_000 + doc_idx;
}

fn relationalSchemaJsonAlloc(alloc: std.mem.Allocator, mode: RelationalIndexMode, constraint_probe: RelationalConstraintProbe) ![]u8 {
    const base = relationalBaseSchemaJson(mode);
    if (constraint_probe == .none) return try alloc.dupe(u8, base);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ base[0 .. base.len - 1], relationalConstraintProbeSchemaSuffix(constraint_probe) });
}

fn relationalConstraintProbeSchemaSuffix(probe: RelationalConstraintProbe) []const u8 {
    return switch (probe) {
        .none => "}",
        .unique => ",\"unique_constraints\":[{\"name\":\"row_title_key\",\"columns\":[\"title\"]}]}",
        .foreign_key => ",\"foreign_keys\":[{\"name\":\"row_self_fkey\",\"columns\":[\"id\"],\"references\":{\"table\":\"row\",\"columns\":[\"_id\"]},\"on_delete\":\"restrict\",\"on_update\":\"restrict\"}]}",
    };
}

fn relationalBaseSchemaJson(mode: RelationalIndexMode) []const u8 {
    return switch (mode) {
        .none =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-index":false},"title":{"type":"text","x-antfly-index":false},"status":{"type":"keyword","x-antfly-index":false},"amount":{"type":"numeric","x-antfly-index":false},"created_at":{"type":"numeric","x-antfly-index":false},"body":{"type":"text","x-antfly-index":false}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        .single_column =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-index":false},"title":{"type":"text","x-antfly-index":false},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric","x-antfly-index":false},"body":{"type":"text","x-antfly-index":false}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        .ordered_tuple =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-index":false},"title":{"type":"text","x-antfly-index":false},"status":{"type":"keyword","x-antfly-index-name":"status_amount_idx","x-antfly-index-keys":[{"column":"status"},{"column":"amount"}],"x-antfly-index-include":["id","amount"]},"amount":{"type":"numeric","x-antfly-index":false},"created_at":{"type":"numeric","x-antfly-index":false},"body":{"type":"text","x-antfly-index":false}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        .partial_single_column =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-index":false},"title":{"type":"text","x-antfly-index":false},"status":{"type":"keyword","x-antfly-index":false},"amount":{"type":"numeric","x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"open"}]}},"created_at":{"type":"numeric","x-antfly-index":false},"body":{"type":"text","x-antfly-index":false}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        .partial_ordered_tuple =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-index":false},"title":{"type":"text","x-antfly-index":false},"status":{"type":"keyword","x-antfly-index-name":"status_amount_partial_idx","x-antfly-index-keys":[{"column":"status"},{"column":"amount"}],"x-antfly-index-include":["id","amount"],"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"open"}]}},"amount":{"type":"numeric","x-antfly-index":false},"created_at":{"type":"numeric","x-antfly-index":false},"body":{"type":"text","x-antfly-index":false}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
    };
}

fn snapshotReplayStats(alloc: std.mem.Allocator, db: *db_mod.DB) !ReplayStats {
    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    var payload_bytes: usize = 0;
    for (entries) |entry| payload_bytes += entry.payload.len;
    return .{
        .last_sequence = if (entries.len == 0) 0 else entries[entries.len - 1].sequence,
        .entries = entries.len,
        .payload_bytes = payload_bytes,
    };
}

fn addBatchProfile(total: *db_mod.BatchProfile, delta: db_mod.BatchProfile) void {
    total.total_ns += delta.total_ns;
    total.resolve_transforms_ns += delta.resolve_transforms_ns;
    total.merge_effective_req_ns += delta.merge_effective_req_ns;
    total.predicates_ns += delta.predicates_ns;
    total.validate_range_ns += delta.validate_range_ns;
    total.extract_writes_ns += delta.extract_writes_ns;
    total.delete_artifacts_ns += delta.delete_artifacts_ns;
    total.precompute_generated_ns += delta.precompute_generated_ns;
    total.identity_capacity_check_ns += delta.identity_capacity_check_ns;
    total.identity_metadata_ns += delta.identity_metadata_ns;
    total.identity_metadata_writes += delta.identity_metadata_writes;
    total.relational_row_value_ns += delta.relational_row_value_ns;
    total.relational_prepare_upsert_ns += delta.relational_prepare_upsert_ns;
    total.relational_prepare_delete_ns += delta.relational_prepare_delete_ns;
    total.relational_prepare_identity_rewrite_ns += delta.relational_prepare_identity_rewrite_ns;
    total.relational_row_upserts += delta.relational_row_upserts;
    total.relational_row_deletes += delta.relational_row_deletes;
    total.relational_identity_rewrites += delta.relational_identity_rewrites;
    total.relational_store_writes += delta.relational_store_writes;
    total.relational_store_deletes += delta.relational_store_deletes;
    total.relational_store_write_bytes += delta.relational_store_write_bytes;
    total.relational_store_delete_key_bytes += delta.relational_store_delete_key_bytes;
    total.identity_upsert_keys += delta.identity_upsert_keys;
    total.identity_delete_keys += delta.identity_delete_keys;
    total.store_write_ns += delta.store_write_ns;
    total.store_write_count += delta.store_write_count;
    total.store_delete_count += delta.store_delete_count;
    total.store_write_bytes += delta.store_write_bytes;
    total.store_delete_key_bytes += delta.store_delete_key_bytes;
    total.split_delta_ns += delta.split_delta_ns;
    total.build_derived_ns += delta.build_derived_ns;
    total.apply_shadow_ns += delta.apply_shadow_ns;
    total.collect_sync_targets_ns += delta.collect_sync_targets_ns;
    total.append_replay_journal_ns += delta.append_replay_journal_ns;
    total.wait_sync_ns += delta.wait_sync_ns;
    total.backlog_pressure_ns += delta.backlog_pressure_ns;
    total.executor_notify_ns += delta.executor_notify_ns;
    total.derived_apply_ns += delta.derived_apply_ns;
    total.sync_wait_ns += delta.sync_wait_ns;
    total.full_text_apply_ns += delta.full_text_apply_ns;
    total.dense_apply_ns += delta.dense_apply_ns;
    total.dense_delete_ns += delta.dense_delete_ns;
    total.dense_doc_index_ns += delta.dense_doc_index_ns;
    total.dense_embedding_apply_ns += delta.dense_embedding_apply_ns;
    total.sparse_apply_ns += delta.sparse_apply_ns;
    total.graph_apply_ns += delta.graph_apply_ns;
    total.index_sync_ns += delta.index_sync_ns;
    total.applied_sequence_save_ns += delta.applied_sequence_save_ns;
    total.replay_journal_truncate_ns += delta.replay_journal_truncate_ns;
    total.notify_enrichment_ns += delta.notify_enrichment_ns;
    total.hbc_insert_calls += delta.hbc_insert_calls;
    total.hbc_grouped_items += delta.hbc_grouped_items;
    total.hbc_grouped_fallback_items += delta.hbc_grouped_fallback_items;
    total.hbc_insert_find_leaf_ns += delta.hbc_insert_find_leaf_ns;
    total.hbc_insert_mutate_leaf_ns += delta.hbc_insert_mutate_leaf_ns;
    total.hbc_insert_commit_ns += delta.hbc_insert_commit_ns;
    total.hbc_refresh_quantized_ns += delta.hbc_refresh_quantized_ns;
}

fn storeMutationEntries(profile: db_mod.BatchProfile) u64 {
    return profile.store_write_count +| profile.store_delete_count;
}

fn relationalStoreMutationEntries(profile: db_mod.BatchProfile) u64 {
    return profile.relational_store_writes +| profile.relational_store_deletes;
}

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "batch_bench workload={s} mutation_mode={s} overwrite_shape={s} query_shape={s} primary={s} relational_index_mode={s} constraint_probe={s} docs={d} overwrite_passes={d} body_repeat={d} dims={d} batch_size={d} query_limit={d} query_total_mode={s} predicate_min_amount={d} bulk_session={any} sync={s} requested_writes={d} requested_deletes={d} requested_transforms={d} batches={d} stage_ms={d:.3} finish_ms={d:.3} total_ms={d:.3} max_batch_ms={d:.3}\n",
        .{
            @tagName(cfg.workload),
            @tagName(cfg.mutation_mode),
            @tagName(cfg.overwrite_shape),
            @tagName(cfg.query_shape),
            @tagName(cfg.primary),
            @tagName(cfg.relational_index_mode),
            @tagName(cfg.relational_constraint_probe),
            cfg.docs,
            cfg.overwrite_passes,
            cfg.body_repeat,
            cfg.dims,
            cfg.batch_size,
            cfg.query_limit,
            @tagName(cfg.query_total_mode),
            cfg.predicate_min_amount,
            cfg.bulk_session,
            db_mod.types.publicSyncLevelText(cfg.sync_level),
            summary.requested_writes,
            summary.requested_deletes,
            summary.requested_transforms,
            summary.batches,
            nsToMsFloat(summary.stage_ns),
            nsToMsFloat(summary.finish_ns),
            nsToMsFloat(summary.total_ns),
            nsToMsFloat(summary.max_batch_ns),
        },
    );
    std.debug.print(
        "batch_bench_profile resolve_ms={d:.3} merge_req_ms={d:.3} identity_capacity_ms={d:.3} identity_metadata_ms={d:.3} identity_metadata_writes={d} store_write_ms={d:.3} store_writes={d} store_deletes={d} store_mutations={d} store_write_bytes={d} store_delete_key_bytes={d} stage_alloc_events={d} stage_alloc_bytes={d} stage_peak_live_bytes={d} build_derived_ms={d:.3} append_replay_journal_ms={d:.3} full_text_apply_ms={d:.3} dense_apply_ms={d:.3}\n",
        .{
            nsToMsFloat(summary.profile.resolve_transforms_ns),
            nsToMsFloat(summary.profile.merge_effective_req_ns),
            nsToMsFloat(summary.profile.identity_capacity_check_ns),
            nsToMsFloat(summary.profile.identity_metadata_ns),
            summary.profile.identity_metadata_writes,
            nsToMsFloat(summary.profile.store_write_ns),
            summary.profile.store_write_count,
            summary.profile.store_delete_count,
            storeMutationEntries(summary.profile),
            summary.profile.store_write_bytes,
            summary.profile.store_delete_key_bytes,
            summary.stage_allocations.allocation_events,
            summary.stage_allocations.allocated_bytes,
            summary.stage_allocations.peak_live_bytes,
            nsToMsFloat(summary.profile.build_derived_ns),
            nsToMsFloat(summary.profile.append_replay_journal_ns),
            nsToMsFloat(summary.profile.full_text_apply_ns),
            nsToMsFloat(summary.profile.dense_apply_ns),
        },
    );
    if (cfg.workload == .relational_rows) {
        std.debug.print(
            "batch_bench_relational row_value_ms={d:.3} prepare_upsert_ms={d:.3} prepare_delete_ms={d:.3} prepare_identity_rewrite_ms={d:.3} row_upserts={d} row_deletes={d} identity_rewrites={d} relational_store_writes={d} relational_store_deletes={d} relational_store_mutations={d} relational_store_write_bytes={d} relational_store_delete_key_bytes={d}\n",
            .{
                nsToMsFloat(summary.profile.relational_row_value_ns),
                nsToMsFloat(summary.profile.relational_prepare_upsert_ns),
                nsToMsFloat(summary.profile.relational_prepare_delete_ns),
                nsToMsFloat(summary.profile.relational_prepare_identity_rewrite_ns),
                summary.profile.relational_row_upserts,
                summary.profile.relational_row_deletes,
                summary.profile.relational_identity_rewrites,
                summary.profile.relational_store_writes,
                summary.profile.relational_store_deletes,
                relationalStoreMutationEntries(summary.profile),
                summary.profile.relational_store_write_bytes,
                summary.profile.relational_store_delete_key_bytes,
            },
        );
    }
    std.debug.print(
        "batch_bench_replay seq={d} entries={d} payload_bytes={d}\n",
        .{ summary.replay.last_sequence, summary.replay.entries, summary.replay.payload_bytes },
    );
    if (cfg.query_repeats > 0) {
        std.debug.print(
            "batch_bench_query lookup_repeats={d} lookup_hits={d} lookup_ms={d:.3} predicate_repeats={d} predicate_hits={d} predicate_exact_totals={d} predicate_ms={d:.3} query_alloc_events={d} query_alloc_bytes={d} query_peak_live_bytes={d} index_entries_scanned={d} candidate_rows={d} candidate_gate_limit={d} candidate_gate_observed={d} iterator_seeks={d} hydrated_rows={d} residual_rechecks={d} covering_payload_rows={d} projected_rows={d} unknown_access_queries={d} base_scan_queries={d} resolved_doc_set_queries={d} ordered_tuple_doc_set_queries={d} ordered_tuple_stream_queries={d} ordered_tuple_candidate_gate_fallbacks={d} ordered_tuple_materialization_cap_fallbacks={d} ordered_tuple_exact_paged_total_fallbacks={d} ordered_tuple_ordering_not_covered_fallbacks={d} ordered_tuple_index_not_ready_fallbacks={d} ordered_tuple_stale_generation_fallbacks={d} ordered_tuple_predicate_not_proven_fallbacks={d} ordered_tuple_no_usable_bounds_fallbacks={d}\n",
            .{
                summary.query.lookup_repeats,
                summary.query.lookup_hits,
                nsToMsFloat(summary.query.lookup_ns),
                summary.query.predicate_repeats,
                summary.query.predicate_hits,
                summary.query.predicate_exact_totals,
                nsToMsFloat(summary.query.predicate_ns),
                summary.query.allocations.allocation_events,
                summary.query.allocations.allocated_bytes,
                summary.query.allocations.peak_live_bytes,
                summary.query.index_entries_scanned,
                summary.query.candidate_rows,
                summary.query.candidate_gate_limit,
                summary.query.candidate_gate_observed,
                summary.query.iterator_seeks,
                summary.query.hydrated_rows,
                summary.query.residual_rechecks,
                summary.query.covering_payload_rows,
                summary.query.projected_rows,
                summary.query.unknown_access_queries,
                summary.query.base_scan_queries,
                summary.query.resolved_doc_set_queries,
                summary.query.ordered_tuple_doc_set_queries,
                summary.query.ordered_tuple_stream_queries,
                summary.query.ordered_tuple_candidate_gate_fallbacks,
                summary.query.ordered_tuple_materialization_cap_fallbacks,
                summary.query.ordered_tuple_exact_paged_total_fallbacks,
                summary.query.ordered_tuple_ordering_not_covered_fallbacks,
                summary.query.ordered_tuple_index_not_ready_fallbacks,
                summary.query.ordered_tuple_stale_generation_fallbacks,
                summary.query.ordered_tuple_predicate_not_proven_fallbacks,
                summary.query.ordered_tuple_no_usable_bounds_fallbacks,
            },
        );
        std.debug.print(
            "batch_bench_query_plan ordered_tuple_plan_queries={d} ordered_tuple_max_key_count={d} ordered_tuple_max_equality_prefix_len={d} ordered_tuple_max_filter_predicates={d} ordered_tuple_max_proven_predicates={d} ordered_tuple_max_residual_predicates={d} ordered_tuple_range_plan_queries={d} ordered_tuple_prefix_scan_queries={d}\n",
            .{
                summary.query.ordered_tuple_plan_queries,
                summary.query.ordered_tuple_max_key_count,
                summary.query.ordered_tuple_max_equality_prefix_len,
                summary.query.ordered_tuple_max_filter_predicates,
                summary.query.ordered_tuple_max_proven_predicates,
                summary.query.ordered_tuple_max_residual_predicates,
                summary.query.ordered_tuple_range_plan_queries,
                summary.query.ordered_tuple_prefix_scan_queries,
            },
        );
    }
    std.debug.print(
        "batch_bench_bulk_coalescing active={any} staged_keys={d} stage_batches={d} stage_writes={d} stage_deletes={d} stage_transforms={d} flush_calls={d} flushed_keys={d}\n",
        .{
            summary.async_indexing.bulk_coalescing.active_session,
            summary.async_indexing.bulk_coalescing.staged_keys,
            summary.async_indexing.bulk_coalescing.stage_batches,
            summary.async_indexing.bulk_coalescing.stage_writes,
            summary.async_indexing.bulk_coalescing.stage_deletes,
            summary.async_indexing.bulk_coalescing.stage_transforms,
            summary.async_indexing.bulk_coalescing.flush_calls,
            summary.async_indexing.bulk_coalescing.flushed_keys,
        },
    );
}

fn printMatrixResult(cfg: Config, selectivity: []const u8, summary: Summary) void {
    std.debug.print(
        "batch_bench_matrix workload={s} write_scenario={s} constraint_probe={s} mutation_mode={s} overwrite_shape={s} query_shape={s} relational_index_mode={s} docs={d} requested_writes={d} requested_deletes={d} selectivity={s} predicate_min_amount={d} total_mode={s} stage_ms={d:.3} total_ms={d:.3} store_write_ms={d:.3} store_writes={d} store_deletes={d} store_mutations={d} store_write_bytes={d} store_delete_key_bytes={d} stage_alloc_events={d} stage_alloc_bytes={d} stage_peak_live_bytes={d} relational_store_writes={d} relational_store_deletes={d} relational_store_mutations={d} relational_store_write_bytes={d} relational_store_delete_key_bytes={d}",
        .{
            @tagName(cfg.workload),
            @tagName(cfg.matrix_write_scenario),
            @tagName(cfg.relational_constraint_probe),
            @tagName(cfg.mutation_mode),
            @tagName(cfg.overwrite_shape),
            @tagName(cfg.query_shape),
            @tagName(cfg.relational_index_mode),
            cfg.docs,
            summary.requested_writes,
            summary.requested_deletes,
            selectivity,
            cfg.predicate_min_amount,
            @tagName(cfg.query_total_mode),
            nsToMsFloat(summary.stage_ns),
            nsToMsFloat(summary.total_ns),
            nsToMsFloat(summary.profile.store_write_ns),
            summary.profile.store_write_count,
            summary.profile.store_delete_count,
            storeMutationEntries(summary.profile),
            summary.profile.store_write_bytes,
            summary.profile.store_delete_key_bytes,
            summary.stage_allocations.allocation_events,
            summary.stage_allocations.allocated_bytes,
            summary.stage_allocations.peak_live_bytes,
            summary.profile.relational_store_writes,
            summary.profile.relational_store_deletes,
            relationalStoreMutationEntries(summary.profile),
            summary.profile.relational_store_write_bytes,
            summary.profile.relational_store_delete_key_bytes,
        },
    );
    std.debug.print(
        " predicate_ms={d:.3} predicate_hits={d} exact_totals={d} query_alloc_events={d} query_alloc_bytes={d} query_peak_live_bytes={d} index_entries_scanned={d} candidate_rows={d} candidate_gate_limit={d} candidate_gate_observed={d} iterator_seeks={d} hydrated_rows={d} residual_rechecks={d} covering_payload_rows={d} projected_rows={d} unknown_access_queries={d} base_scan_queries={d} resolved_doc_set_queries={d} ordered_tuple_doc_set_queries={d} ordered_tuple_stream_queries={d} candidate_gate_fallbacks={d} materialization_cap_fallbacks={d} exact_paged_total_fallbacks={d} ordering_not_covered_fallbacks={d} index_not_ready_fallbacks={d} stale_generation_fallbacks={d} predicate_not_proven_fallbacks={d} no_usable_bounds_fallbacks={d}\n",
        .{
            nsToMsFloat(summary.query.predicate_ns),
            summary.query.predicate_hits,
            summary.query.predicate_exact_totals,
            summary.query.allocations.allocation_events,
            summary.query.allocations.allocated_bytes,
            summary.query.allocations.peak_live_bytes,
            summary.query.index_entries_scanned,
            summary.query.candidate_rows,
            summary.query.candidate_gate_limit,
            summary.query.candidate_gate_observed,
            summary.query.iterator_seeks,
            summary.query.hydrated_rows,
            summary.query.residual_rechecks,
            summary.query.covering_payload_rows,
            summary.query.projected_rows,
            summary.query.unknown_access_queries,
            summary.query.base_scan_queries,
            summary.query.resolved_doc_set_queries,
            summary.query.ordered_tuple_doc_set_queries,
            summary.query.ordered_tuple_stream_queries,
            summary.query.ordered_tuple_candidate_gate_fallbacks,
            summary.query.ordered_tuple_materialization_cap_fallbacks,
            summary.query.ordered_tuple_exact_paged_total_fallbacks,
            summary.query.ordered_tuple_ordering_not_covered_fallbacks,
            summary.query.ordered_tuple_index_not_ready_fallbacks,
            summary.query.ordered_tuple_stale_generation_fallbacks,
            summary.query.ordered_tuple_predicate_not_proven_fallbacks,
            summary.query.ordered_tuple_no_usable_bounds_fallbacks,
        },
    );
    std.debug.print(
        "batch_bench_matrix_plan workload={s} write_scenario={s} constraint_probe={s} query_shape={s} relational_index_mode={s} docs={d} selectivity={s} total_mode={s} ordered_tuple_plan_queries={d} ordered_tuple_max_key_count={d} ordered_tuple_max_equality_prefix_len={d} ordered_tuple_max_filter_predicates={d} ordered_tuple_max_proven_predicates={d} ordered_tuple_max_residual_predicates={d} ordered_tuple_range_plan_queries={d} ordered_tuple_prefix_scan_queries={d}\n",
        .{
            @tagName(cfg.workload),
            @tagName(cfg.matrix_write_scenario),
            @tagName(cfg.relational_constraint_probe),
            @tagName(cfg.query_shape),
            @tagName(cfg.relational_index_mode),
            cfg.docs,
            selectivity,
            @tagName(cfg.query_total_mode),
            summary.query.ordered_tuple_plan_queries,
            summary.query.ordered_tuple_max_key_count,
            summary.query.ordered_tuple_max_equality_prefix_len,
            summary.query.ordered_tuple_max_filter_predicates,
            summary.query.ordered_tuple_max_proven_predicates,
            summary.query.ordered_tuple_max_residual_predicates,
            summary.query.ordered_tuple_range_plan_queries,
            summary.query.ordered_tuple_prefix_scan_queries,
        },
    );
}

fn matrixRelationalComparison(
    docs: usize,
    write_scenario: MatrixWriteScenario,
    constraint_probe: RelationalConstraintProbe,
    mutation_mode: MutationMode,
    overwrite_shape: OverwriteShape,
    query_shape: QueryShape,
    predicate_min_amount: usize,
    selectivity: []const u8,
    total_mode: db_mod.types.RelationalRowsQueryRequest.TotalMode,
    no_index: Summary,
    single_column: Summary,
    ordered_tuple: Summary,
) MatrixComparison {
    const ordered_vs_no_index_writes = ratio(ordered_tuple.profile.relational_store_writes, no_index.profile.relational_store_writes);
    const ordered_vs_single_writes = ratio(ordered_tuple.profile.relational_store_writes, single_column.profile.relational_store_writes);
    const ordered_vs_no_index_write_bytes = ratio(ordered_tuple.profile.relational_store_write_bytes, no_index.profile.relational_store_write_bytes);
    const ordered_vs_single_write_bytes = ratio(ordered_tuple.profile.relational_store_write_bytes, single_column.profile.relational_store_write_bytes);
    const ordered_vs_no_index_stage_time = ratio(ordered_tuple.stage_ns, no_index.stage_ns);
    const ordered_vs_single_stage_time = ratio(ordered_tuple.stage_ns, single_column.stage_ns);
    const ordered_vs_no_index_total_time = ratio(ordered_tuple.total_ns, no_index.total_ns);
    const ordered_vs_single_total_time = ratio(ordered_tuple.total_ns, single_column.total_ns);
    const ordered_vs_no_index_store_mutations = ratio(storeMutationEntries(ordered_tuple.profile), storeMutationEntries(no_index.profile));
    const ordered_vs_single_store_mutations = ratio(storeMutationEntries(ordered_tuple.profile), storeMutationEntries(single_column.profile));
    const ordered_vs_no_index_relational_mutations = ratio(relationalStoreMutationEntries(ordered_tuple.profile), relationalStoreMutationEntries(no_index.profile));
    const ordered_vs_single_relational_mutations = ratio(relationalStoreMutationEntries(ordered_tuple.profile), relationalStoreMutationEntries(single_column.profile));
    const ordered_vs_no_index_stage_allocations = ratio(ordered_tuple.stage_allocations.allocation_events, no_index.stage_allocations.allocation_events);
    const ordered_vs_single_stage_allocations = ratio(ordered_tuple.stage_allocations.allocation_events, single_column.stage_allocations.allocation_events);
    const ordered_vs_no_index_stage_allocated_bytes = ratio(ordered_tuple.stage_allocations.allocated_bytes, no_index.stage_allocations.allocated_bytes);
    const ordered_vs_single_stage_allocated_bytes = ratio(ordered_tuple.stage_allocations.allocated_bytes, single_column.stage_allocations.allocated_bytes);
    const ordered_vs_no_index_query_allocations = ratio(ordered_tuple.query.allocations.allocation_events, no_index.query.allocations.allocation_events);
    const ordered_vs_single_query_allocations = ratio(ordered_tuple.query.allocations.allocation_events, single_column.query.allocations.allocation_events);
    const ordered_vs_no_index_iterator_seeks = ratio(ordered_tuple.query.iterator_seeks, no_index.query.iterator_seeks);
    const ordered_vs_single_iterator_seeks = ratio(ordered_tuple.query.iterator_seeks, single_column.query.iterator_seeks);
    const ordered_vs_no_index_residual_rechecks = ratio(ordered_tuple.query.residual_rechecks, no_index.query.residual_rechecks);
    const ordered_vs_single_residual_rechecks = ratio(ordered_tuple.query.residual_rechecks, single_column.query.residual_rechecks);
    const ordered_vs_single_predicate = ratio(ordered_tuple.query.predicate_ns, single_column.query.predicate_ns);
    const ordered_vs_no_index_predicate = ratio(ordered_tuple.query.predicate_ns, no_index.query.predicate_ns);
    const ordered_tuple_flagged = ordered_tuple.query.ordered_tuple_candidate_gate_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_materialization_cap_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_exact_paged_total_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_ordering_not_covered_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_index_not_ready_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_stale_generation_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_predicate_not_proven_fallbacks != 0 or
        ordered_tuple.query.ordered_tuple_no_usable_bounds_fallbacks != 0;
    const ordered_tuple_access_missing = ordered_tuple.query.predicate_repeats != 0 and
        ordered_tuple.query.ordered_tuple_doc_set_queries == 0 and
        ordered_tuple.query.ordered_tuple_stream_queries == 0;
    return .{
        .docs = docs,
        .write_scenario = write_scenario,
        .constraint_probe = constraint_probe,
        .mutation_mode = mutation_mode,
        .overwrite_shape = overwrite_shape,
        .query_shape = query_shape,
        .predicate_min_amount = predicate_min_amount,
        .selectivity = selectivity,
        .total_mode = total_mode,
        .ordered_vs_no_index_writes = ordered_vs_no_index_writes,
        .ordered_vs_single_writes = ordered_vs_single_writes,
        .ordered_vs_no_index_write_bytes = ordered_vs_no_index_write_bytes,
        .ordered_vs_single_write_bytes = ordered_vs_single_write_bytes,
        .ordered_vs_no_index_stage_time = ordered_vs_no_index_stage_time,
        .ordered_vs_single_stage_time = ordered_vs_single_stage_time,
        .ordered_vs_no_index_total_time = ordered_vs_no_index_total_time,
        .ordered_vs_single_total_time = ordered_vs_single_total_time,
        .ordered_vs_no_index_store_mutations = ordered_vs_no_index_store_mutations,
        .ordered_vs_single_store_mutations = ordered_vs_single_store_mutations,
        .ordered_vs_no_index_relational_mutations = ordered_vs_no_index_relational_mutations,
        .ordered_vs_single_relational_mutations = ordered_vs_single_relational_mutations,
        .ordered_vs_no_index_stage_allocations = ordered_vs_no_index_stage_allocations,
        .ordered_vs_single_stage_allocations = ordered_vs_single_stage_allocations,
        .ordered_vs_no_index_stage_allocated_bytes = ordered_vs_no_index_stage_allocated_bytes,
        .ordered_vs_single_stage_allocated_bytes = ordered_vs_single_stage_allocated_bytes,
        .ordered_vs_no_index_query_allocations = ordered_vs_no_index_query_allocations,
        .ordered_vs_single_query_allocations = ordered_vs_single_query_allocations,
        .ordered_vs_no_index_iterator_seeks = ordered_vs_no_index_iterator_seeks,
        .ordered_vs_single_iterator_seeks = ordered_vs_single_iterator_seeks,
        .ordered_vs_no_index_residual_rechecks = ordered_vs_no_index_residual_rechecks,
        .ordered_vs_single_residual_rechecks = ordered_vs_single_residual_rechecks,
        .ordered_vs_no_index_predicate = ordered_vs_no_index_predicate,
        .ordered_vs_single_predicate = ordered_vs_single_predicate,
        .ordered_tuple_base_scan_queries = ordered_tuple.query.base_scan_queries,
        .ordered_tuple_resolved_doc_set_queries = ordered_tuple.query.resolved_doc_set_queries,
        .ordered_tuple_doc_set_queries = ordered_tuple.query.ordered_tuple_doc_set_queries,
        .ordered_tuple_stream_queries = ordered_tuple.query.ordered_tuple_stream_queries,
        .ordered_tuple_access_missing_flag = ordered_tuple_access_missing,
        .ordered_tuple_fallback_flag = ordered_tuple_flagged,
    };
}

fn printMatrixRelationalComparison(comparison: MatrixComparison) void {
    std.debug.print(
        "batch_bench_matrix_compare docs={d} write_scenario={s} constraint_probe={s} mutation_mode={s} overwrite_shape={s} query_shape={s} selectivity={s} predicate_min_amount={d} total_mode={s}",
        .{
            comparison.docs,
            @tagName(comparison.write_scenario),
            @tagName(comparison.constraint_probe),
            @tagName(comparison.mutation_mode),
            @tagName(comparison.overwrite_shape),
            @tagName(comparison.query_shape),
            comparison.selectivity,
            comparison.predicate_min_amount,
            @tagName(comparison.total_mode),
        },
    );
    std.debug.print(
        " ordered_vs_no_index_writes={d:.3} ordered_vs_single_writes={d:.3} ordered_vs_no_index_write_bytes={d:.3} ordered_vs_single_write_bytes={d:.3} ordered_vs_no_index_stage_time={d:.3} ordered_vs_single_stage_time={d:.3} ordered_vs_no_index_total_time={d:.3} ordered_vs_single_total_time={d:.3} ordered_vs_no_index_store_mutations={d:.3} ordered_vs_single_store_mutations={d:.3} ordered_vs_no_index_relational_mutations={d:.3} ordered_vs_single_relational_mutations={d:.3} ordered_vs_no_index_stage_allocations={d:.3} ordered_vs_single_stage_allocations={d:.3} ordered_vs_no_index_stage_allocated_bytes={d:.3} ordered_vs_single_stage_allocated_bytes={d:.3} ordered_vs_no_index_query_allocations={d:.3} ordered_vs_single_query_allocations={d:.3} ordered_vs_no_index_iterator_seeks={d:.3} ordered_vs_single_iterator_seeks={d:.3} ordered_vs_no_index_residual_rechecks={d:.3} ordered_vs_single_residual_rechecks={d:.3} ordered_vs_no_index_predicate={d:.3} ordered_vs_single_predicate={d:.3} ordered_tuple_base_scan_queries={d} ordered_tuple_resolved_doc_set_queries={d} ordered_tuple_doc_set_queries={d} ordered_tuple_stream_queries={d} ordered_tuple_access_missing_flag={any} ordered_tuple_fallback_flag={any}\n",
        .{
            comparison.ordered_vs_no_index_writes,
            comparison.ordered_vs_single_writes,
            comparison.ordered_vs_no_index_write_bytes,
            comparison.ordered_vs_single_write_bytes,
            comparison.ordered_vs_no_index_stage_time,
            comparison.ordered_vs_single_stage_time,
            comparison.ordered_vs_no_index_total_time,
            comparison.ordered_vs_single_total_time,
            comparison.ordered_vs_no_index_store_mutations,
            comparison.ordered_vs_single_store_mutations,
            comparison.ordered_vs_no_index_relational_mutations,
            comparison.ordered_vs_single_relational_mutations,
            comparison.ordered_vs_no_index_stage_allocations,
            comparison.ordered_vs_single_stage_allocations,
            comparison.ordered_vs_no_index_stage_allocated_bytes,
            comparison.ordered_vs_single_stage_allocated_bytes,
            comparison.ordered_vs_no_index_query_allocations,
            comparison.ordered_vs_single_query_allocations,
            comparison.ordered_vs_no_index_iterator_seeks,
            comparison.ordered_vs_single_iterator_seeks,
            comparison.ordered_vs_no_index_residual_rechecks,
            comparison.ordered_vs_single_residual_rechecks,
            comparison.ordered_vs_no_index_predicate,
            comparison.ordered_vs_single_predicate,
            comparison.ordered_tuple_base_scan_queries,
            comparison.ordered_tuple_resolved_doc_set_queries,
            comparison.ordered_tuple_doc_set_queries,
            comparison.ordered_tuple_stream_queries,
            comparison.ordered_tuple_access_missing_flag,
            comparison.ordered_tuple_fallback_flag,
        },
    );
}

fn matrixComparisonFailsGuardrail(cfg: Config, comparison: MatrixComparison) bool {
    if (comparison.ordered_tuple_access_missing_flag) return true;
    if (comparison.ordered_tuple_fallback_flag) return true;
    if (comparison.ordered_vs_single_writes > cfg.matrix_max_ordered_vs_single_writes) return true;
    if (comparison.ordered_vs_single_write_bytes > cfg.matrix_max_ordered_vs_single_write_bytes) return true;
    if (comparison.ordered_vs_single_stage_time > cfg.matrix_max_ordered_vs_single_stage_time) return true;
    if (comparison.ordered_vs_single_total_time > cfg.matrix_max_ordered_vs_single_total_time) return true;
    if (comparison.ordered_vs_single_store_mutations > cfg.matrix_max_ordered_vs_single_store_mutations) return true;
    if (comparison.ordered_vs_single_relational_mutations > cfg.matrix_max_ordered_vs_single_relational_mutations) return true;
    if (comparison.ordered_vs_single_stage_allocations > cfg.matrix_max_ordered_vs_single_stage_allocations) return true;
    if (comparison.ordered_vs_single_stage_allocated_bytes > cfg.matrix_max_ordered_vs_single_stage_allocated_bytes) return true;
    if (comparison.ordered_vs_single_query_allocations > cfg.matrix_max_ordered_vs_single_query_allocations) return true;
    if (comparison.ordered_vs_single_iterator_seeks > cfg.matrix_max_ordered_vs_single_iterator_seeks) return true;
    if (comparison.ordered_vs_single_residual_rechecks > cfg.matrix_max_ordered_vs_single_residual_rechecks) return true;
    if (comparison.ordered_vs_single_predicate > cfg.matrix_max_ordered_vs_single_predicate) return true;
    return false;
}

fn printMatrixRegressionFailure(cfg: Config, comparison: MatrixComparison) void {
    std.debug.print(
        "batch_bench_matrix_regression docs={d} write_scenario={s} constraint_probe={s} mutation_mode={s} overwrite_shape={s} query_shape={s} selectivity={s} predicate_min_amount={d} total_mode={s} ordered_tuple_access_missing_flag={any} ordered_tuple_fallback_flag={any}",
        .{
            comparison.docs,
            @tagName(comparison.write_scenario),
            @tagName(comparison.constraint_probe),
            @tagName(comparison.mutation_mode),
            @tagName(comparison.overwrite_shape),
            @tagName(comparison.query_shape),
            comparison.selectivity,
            comparison.predicate_min_amount,
            @tagName(comparison.total_mode),
            comparison.ordered_tuple_access_missing_flag,
            comparison.ordered_tuple_fallback_flag,
        },
    );
    std.debug.print(
        " ordered_vs_single_writes={d:.3} max_ordered_vs_single_writes={d:.3} ordered_vs_single_write_bytes={d:.3} max_ordered_vs_single_write_bytes={d:.3} ordered_vs_single_stage_time={d:.3} max_ordered_vs_single_stage_time={d:.3} ordered_vs_single_total_time={d:.3} max_ordered_vs_single_total_time={d:.3} ordered_vs_single_store_mutations={d:.3} max_ordered_vs_single_store_mutations={d:.3} ordered_vs_single_relational_mutations={d:.3} max_ordered_vs_single_relational_mutations={d:.3} ordered_vs_single_stage_allocations={d:.3} max_ordered_vs_single_stage_allocations={d:.3} ordered_vs_single_stage_allocated_bytes={d:.3} max_ordered_vs_single_stage_allocated_bytes={d:.3} ordered_vs_single_query_allocations={d:.3} max_ordered_vs_single_query_allocations={d:.3} ordered_vs_single_iterator_seeks={d:.3} max_ordered_vs_single_iterator_seeks={d:.3} ordered_vs_single_residual_rechecks={d:.3} max_ordered_vs_single_residual_rechecks={d:.3} ordered_vs_single_predicate={d:.3} max_ordered_vs_single_predicate={d:.3}\n",
        .{
            comparison.ordered_vs_single_writes,
            cfg.matrix_max_ordered_vs_single_writes,
            comparison.ordered_vs_single_write_bytes,
            cfg.matrix_max_ordered_vs_single_write_bytes,
            comparison.ordered_vs_single_stage_time,
            cfg.matrix_max_ordered_vs_single_stage_time,
            comparison.ordered_vs_single_total_time,
            cfg.matrix_max_ordered_vs_single_total_time,
            comparison.ordered_vs_single_store_mutations,
            cfg.matrix_max_ordered_vs_single_store_mutations,
            comparison.ordered_vs_single_relational_mutations,
            cfg.matrix_max_ordered_vs_single_relational_mutations,
            comparison.ordered_vs_single_stage_allocations,
            cfg.matrix_max_ordered_vs_single_stage_allocations,
            comparison.ordered_vs_single_stage_allocated_bytes,
            cfg.matrix_max_ordered_vs_single_stage_allocated_bytes,
            comparison.ordered_vs_single_query_allocations,
            cfg.matrix_max_ordered_vs_single_query_allocations,
            comparison.ordered_vs_single_iterator_seeks,
            cfg.matrix_max_ordered_vs_single_iterator_seeks,
            comparison.ordered_vs_single_residual_rechecks,
            cfg.matrix_max_ordered_vs_single_residual_rechecks,
            comparison.ordered_vs_single_predicate,
            cfg.matrix_max_ordered_vs_single_predicate,
        },
    );
}

fn ratio(numerator: anytype, denominator: anytype) f64 {
    if (denominator == 0) return if (numerator == 0) 1.0 else std.math.inf(f64);
    return @as(f64, @floatFromInt(numerator)) / @as(f64, @floatFromInt(denominator));
}

fn selectivityLabel(predicate_min_amount: usize) []const u8 {
    return if (predicate_min_amount >= 900) "low" else "high";
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn tempPath(buf: []u8) []u8 {
    return std.fmt.bufPrint(buf, "/tmp/antfly-batch-bench-{d}", .{platform_time.monotonicNs()}) catch unreachable;
}

fn matrixTempPath(buf: []u8, kind: []const u8, docs: usize, predicate_min_amount: usize, ordinal: usize) []u8 {
    return std.fmt.bufPrint(
        buf,
        "/tmp/antfly-batch-bench-matrix-{s}-{d}-{d}-{d}-{d}",
        .{ kind, docs, predicate_min_amount, ordinal, platform_time.monotonicNs() },
    ) catch unreachable;
}

fn cleanupTempDir(path: []const u8) void {
    _ = path;
}
