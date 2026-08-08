// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const antfly = @import("antfly-zig");

const docstore_mod = antfly.docstore;
const relational_row_codec = antfly.relational_row_codec;
const relational_store = antfly.relational_store;
const schema_mod = antfly.schema;

const jsonl_schema_version: u32 = 1;

const Config = struct {
    docs: usize = 10_000,
    batch_size: usize = 1_000,
    samples: usize = 3,
};

const IndexShape = enum {
    scalar_nullable,
    ordered_nullable,
    mixed_multi,
};

const RowShape = enum {
    open_null,
    open_10,
    open_20,
    closed_20,
};

const OperationCase = struct {
    name: []const u8,
    old: ?RowShape,
    new: ?RowShape,
};

const operation_cases = [_]OperationCase{
    .{ .name = "insert_null", .old = null, .new = .open_null },
    .{ .name = "insert_value", .old = null, .new = .open_10 },
    .{ .name = "overwrite_unchanged", .old = .open_10, .new = .open_10 },
    .{ .name = "overwrite_amount", .old = .open_10, .new = .open_20 },
    .{ .name = "overwrite_status", .old = .open_20, .new = .closed_20 },
    .{ .name = "value_to_null", .old = .open_10, .new = .open_null },
    .{ .name = "null_to_value", .old = .open_null, .new = .open_10 },
    .{ .name = "delete_value", .old = .open_10, .new = null },
    .{ .name = "delete_null", .old = .open_null, .new = null },
};

const columns = [_]schema_mod.RelationalColumn{
    .{ .name = "status", .path = "status", .field_type = .keyword },
    .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = true },
};
const amount_key = [_]schema_mod.RelationalIndexKey{.{ .column = "amount", .nulls = .first }};
const scalar_indexes = [_]schema_mod.RelationalIndex{.{
    .name = "amount_scalar_idx",
    .owner_kind = .relational_column,
    .owner_name = "amount",
    .access_method = .scalar_column,
    .columns = &.{"amount"},
    .lifecycle = .ready,
    .generation = 5,
    .schema_fingerprint = "secondary-index-v1:amount_scalar_idx",
}};
const ordered_indexes = [_]schema_mod.RelationalIndex{.{
    .name = "amount_ordered_idx",
    .owner_kind = .relational_column,
    .owner_name = "amount",
    .access_method = .ordered_tuple,
    .columns = &.{"amount"},
    .keys = amount_key[0..],
    .lifecycle = .ready,
    .generation = 7,
    .schema_fingerprint = "secondary-index-v1:amount_ordered_idx",
    .generation_record = .{ .generation = 7, .lifecycle = .ready },
}};
const mixed_indexes = [_]schema_mod.RelationalIndex{
    .{
        .name = "status_scalar_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .scalar_column,
        .columns = &.{"status"},
        .lifecycle = .ready,
        .generation = 9,
        .schema_fingerprint = "secondary-index-v1:status_scalar_idx",
    },
    .{
        .name = "amount_ordered_idx",
        .owner_kind = .relational_column,
        .owner_name = "amount",
        .access_method = .ordered_tuple,
        .columns = &.{"amount"},
        .keys = amount_key[0..],
        .lifecycle = .ready,
        .generation = 10,
        .schema_fingerprint = "secondary-index-v1:amount_ordered_idx",
        .generation_record = .{ .generation = 10, .lifecycle = .ready },
    },
};

const AllocationStats = struct {
    events: u64 = 0,
    bytes: u64 = 0,
    peak_live_bytes: u64 = 0,
};

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocation_events: u64 = 0,
    allocated_bytes: u64 = 0,
    live_bytes: u64 = 0,
    peak_live_bytes: u64 = 0,
    measurement_live_baseline: u64 = 0,

    fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn beginMeasurement(self: *CountingAllocator) void {
        self.allocation_events = 0;
        self.allocated_bytes = 0;
        self.measurement_live_baseline = self.live_bytes;
        self.peak_live_bytes = self.live_bytes;
    }

    fn snapshot(self: *const CountingAllocator) AllocationStats {
        return .{
            .events = self.allocation_events,
            .bytes = self.allocated_bytes,
            .peak_live_bytes = self.peak_live_bytes - self.measurement_live_baseline,
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocation_events += 1;
        self.allocated_bytes += @intCast(len);
        self.live_bytes += @intCast(len);
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) {
            const delta = new_len - memory.len;
            self.allocation_events += 1;
            self.allocated_bytes += @intCast(delta);
            self.live_bytes += @intCast(delta);
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        } else {
            self.live_bytes -= @intCast(memory.len - new_len);
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            const delta = new_len - memory.len;
            self.allocation_events += 1;
            self.allocated_bytes += @intCast(delta);
            self.live_bytes += @intCast(delta);
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        } else {
            self.live_bytes -= @intCast(memory.len - new_len);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.live_bytes -= @intCast(memory.len);
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

const Rows = struct {
    open_null: []u8,
    open_10: []u8,
    open_20: []u8,
    closed_20: []u8,

    fn init(alloc: std.mem.Allocator) !Rows {
        return .{
            .open_null = try relational_row_codec.serialize(alloc, &.{
                .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
            }),
            .open_10 = try relational_row_codec.serialize(alloc, &.{
                .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
                .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10 } },
            }),
            .open_20 = try relational_row_codec.serialize(alloc, &.{
                .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
                .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20 } },
            }),
            .closed_20 = try relational_row_codec.serialize(alloc, &.{
                .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "closed" } },
                .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20 } },
            }),
        };
    }

    fn deinit(self: Rows, alloc: std.mem.Allocator) void {
        alloc.free(self.open_null);
        alloc.free(self.open_10);
        alloc.free(self.open_20);
        alloc.free(self.closed_20);
    }

    fn get(self: Rows, shape: RowShape) []const u8 {
        return switch (shape) {
            .open_null => self.open_null,
            .open_10 => self.open_10,
            .open_20 => self.open_20,
            .closed_20 => self.closed_20,
        };
    }
};

const MutationStats = struct {
    writes: u64 = 0,
    deletes: u64 = 0,
    write_bytes: u64 = 0,
    delete_key_bytes: u64 = 0,
};

const CaseResult = struct {
    elapsed_ns: u64,
    allocations: AllocationStats,
    mutations: MutationStats,
    scalar_entries: usize,
    ordered_entries: usize,
    cleanup_verified: bool,
};

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init.minimal.args);
    const shapes = [_]IndexShape{ .scalar_nullable, .ordered_nullable, .mixed_multi };
    var counting = CountingAllocator.init(std.heap.c_allocator);
    const alloc = counting.allocator();

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"event\":\"relational_index_maintenance_config\",\"schema_version\":{d},\"docs\":{d},\"batch_size\":{d},\"samples\":{d},\"index_shapes\":[\"scalar_nullable\",\"ordered_nullable\",\"mixed_multi\"],\"operations\":[\"insert_null\",\"insert_value\",\"overwrite_unchanged\",\"overwrite_amount\",\"overwrite_status\",\"value_to_null\",\"null_to_value\",\"delete_value\",\"delete_null\"]}}\n",
        .{ jsonl_schema_version, cfg.docs, cfg.batch_size, cfg.samples },
    );

    for (shapes) |shape| {
        for (operation_cases) |operation| {
            for (0..cfg.samples) |sample| {
                const result = try runMutationCase(alloc, &counting, cfg, shape, operation);
                try printMutationResult(out, cfg, shape, operation, sample, result);
            }
        }
        for (0..cfg.samples) |sample| {
            const result = try runRebuildCase(alloc, &counting, cfg, shape);
            try printRebuildResult(out, cfg, shape, sample, result);
        }
    }
    try out.flush();
}

fn policyForShape(shape: IndexShape) relational_store.ColumnIndexPolicy {
    return switch (shape) {
        .scalar_nullable => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], scalar_indexes[0..]),
        .ordered_nullable => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], ordered_indexes[0..]),
        .mixed_multi => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], mixed_indexes[0..]),
    };
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args);
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.samples == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator) !usize {
    return try std.fmt.parseUnsigned(usize, args.next() orelse return error.InvalidArgument, 10);
}

fn runMutationCase(
    alloc: std.mem.Allocator,
    counting: *CountingAllocator,
    cfg: Config,
    shape: IndexShape,
    operation: OperationCase,
) !CaseResult {
    var backend = antfly.mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();
    const rows = try Rows.init(alloc);
    defer rows.deinit(alloc);
    const doc_keys = try makeDocKeys(alloc, cfg.docs);
    defer freeDocKeys(alloc, doc_keys);
    const policy = policyForShape(shape);

    if (operation.old) |old| _ = try applyRows(alloc, &store, doc_keys, cfg.batch_size, rows.get(old), policy);

    counting.beginMeasurement();
    const started = antfly.platform_time.monotonicNs();
    const mutations = if (operation.new) |new|
        try applyRows(alloc, &store, doc_keys, cfg.batch_size, rows.get(new), policy)
    else
        try deleteRows(alloc, &store, doc_keys, cfg.batch_size, policy);
    const elapsed_ns = antfly.platform_time.monotonicNs() - started;
    const allocations = counting.snapshot();

    const counts = try verifyIndexEntries(alloc, &store, shape);
    const expected_scalar = expectedScalarEntries(shape, operation.new, cfg.docs);
    const expected_ordered = expectedOrderedEntries(shape, operation.new, cfg.docs);
    const cleanup_verified = counts.scalar == expected_scalar and counts.ordered == expected_ordered;
    if (!cleanup_verified) {
        std.debug.print(
            "relational index maintenance cleanup failed shape={s} operation={s} scalar={d}/{d} ordered={d}/{d}\n",
            .{ @tagName(shape), operation.name, counts.scalar, expected_scalar, counts.ordered, expected_ordered },
        );
        return error.BenchmarkContractViolation;
    }

    return .{
        .elapsed_ns = elapsed_ns,
        .allocations = allocations,
        .mutations = mutations,
        .scalar_entries = counts.scalar,
        .ordered_entries = counts.ordered,
        .cleanup_verified = true,
    };
}

fn runRebuildCase(
    alloc: std.mem.Allocator,
    counting: *CountingAllocator,
    cfg: Config,
    shape: IndexShape,
) !CaseResult {
    var backend = antfly.mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();
    const rows = try Rows.init(alloc);
    defer rows.deinit(alloc);
    const doc_keys = try makeDocKeys(alloc, cfg.docs);
    defer freeDocKeys(alloc, doc_keys);
    _ = try applyRows(alloc, &store, doc_keys, cfg.batch_size, rows.open_10, relational_store.ColumnIndexPolicy.empty());

    counting.beginMeasurement();
    const started = antfly.platform_time.monotonicNs();
    var mutations = MutationStats{};
    var scalar_rebuild_indexes = scalar_indexes;
    scalar_rebuild_indexes[0].lifecycle = .building;
    var ordered_rebuild_indexes = ordered_indexes;
    ordered_rebuild_indexes[0].lifecycle = .building;
    ordered_rebuild_indexes[0].generation_record.?.lifecycle = .building;
    var mixed_rebuild_indexes = mixed_indexes;
    mixed_rebuild_indexes[0].lifecycle = .building;
    mixed_rebuild_indexes[1].lifecycle = .building;
    mixed_rebuild_indexes[1].generation_record.?.lifecycle = .building;
    const policy = switch (shape) {
        .scalar_nullable => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], scalar_rebuild_indexes[0..]),
        .ordered_nullable => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], ordered_rebuild_indexes[0..]),
        .mixed_multi => relational_store.ColumnIndexPolicy.fromSchemaParts(columns[0..], mixed_rebuild_indexes[0..]),
    };
    switch (shape) {
        .scalar_nullable => try accumulateRebuild(&mutations, try relational_store.rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
            alloc,
            &store,
            "amount_scalar_idx",
            5,
            "row:",
            "row:\xff",
            policy,
        )),
        .ordered_nullable => try accumulateRebuild(&mutations, try relational_store.rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
            alloc,
            &store,
            "amount_ordered_idx",
            7,
            "row:",
            "row:\xff",
            policy,
        )),
        .mixed_multi => {
            try accumulateRebuild(&mutations, try relational_store.rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
                alloc,
                &store,
                "status_scalar_idx",
                9,
                "row:",
                "row:\xff",
                policy,
            ));
            try accumulateRebuild(&mutations, try relational_store.rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
                alloc,
                &store,
                "amount_ordered_idx",
                10,
                "row:",
                "row:\xff",
                policy,
            ));
        },
    }
    const elapsed_ns = antfly.platform_time.monotonicNs() - started;
    const allocations = counting.snapshot();
    const counts = try verifyIndexEntries(alloc, &store, shape);
    const expected_scalar = expectedScalarEntries(shape, .open_10, cfg.docs);
    const expected_ordered = expectedOrderedEntries(shape, .open_10, cfg.docs);
    const cleanup_verified = counts.scalar == expected_scalar and counts.ordered == expected_ordered;
    if (!cleanup_verified) return error.BenchmarkContractViolation;
    return .{
        .elapsed_ns = elapsed_ns,
        .allocations = allocations,
        .mutations = mutations,
        .scalar_entries = counts.scalar,
        .ordered_entries = counts.ordered,
        .cleanup_verified = true,
    };
}

fn accumulateRebuild(out: *MutationStats, report: relational_store.SecondaryIndexRebuildReport) !void {
    out.writes += report.written_entries;
    out.deletes += report.deleted_entries;
}

fn makeDocKeys(alloc: std.mem.Allocator, docs: usize) ![][]u8 {
    const keys = try alloc.alloc([]u8, docs);
    errdefer alloc.free(keys);
    var initialized: usize = 0;
    errdefer for (keys[0..initialized]) |key| alloc.free(key);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(alloc, "row:{d:0>9}", .{i});
        initialized += 1;
    }
    return keys;
}

fn freeDocKeys(alloc: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn applyRows(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    doc_keys: []const []const u8,
    batch_size: usize,
    row: []const u8,
    policy: relational_store.ColumnIndexPolicy,
) !MutationStats {
    var stats = MutationStats{};
    var start: usize = 0;
    while (start < doc_keys.len) : (start += batch_size) {
        const end = @min(start + batch_size, doc_keys.len);
        var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer writes.deinit(alloc);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        var owned_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_keys.items) |key| alloc.free(key);
            owned_keys.deinit(alloc);
        }
        var owned_values = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_values.items) |value| alloc.free(value);
            owned_values.deinit(alloc);
        }
        for (doc_keys[start..end]) |doc_key| {
            try relational_store.appendUpsertWithColumnIndexPolicy(
                alloc,
                store,
                &writes,
                &deletes,
                &owned_keys,
                &owned_values,
                doc_key,
                row,
                policy,
            );
        }
        recordMutations(&stats, writes.items, deletes.items);
        try store.putBatch(writes.items, deletes.items);
    }
    return stats;
}

fn deleteRows(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    doc_keys: []const []const u8,
    batch_size: usize,
    policy: relational_store.ColumnIndexPolicy,
) !MutationStats {
    var stats = MutationStats{};
    var start: usize = 0;
    while (start < doc_keys.len) : (start += batch_size) {
        const end = @min(start + batch_size, doc_keys.len);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        var owned_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_keys.items) |key| alloc.free(key);
            owned_keys.deinit(alloc);
        }
        for (doc_keys[start..end]) |doc_key| {
            try relational_store.appendDeleteWithColumnIndexPolicy(alloc, store, &deletes, &owned_keys, doc_key, policy);
        }
        recordMutations(&stats, &.{}, deletes.items);
        try store.putBatch(&.{}, deletes.items);
    }
    return stats;
}

fn recordMutations(stats: *MutationStats, writes: []const docstore_mod.KVPair, deletes: []const []const u8) void {
    stats.writes += @intCast(writes.len);
    stats.deletes += @intCast(deletes.len);
    for (writes) |write| stats.write_bytes += @intCast(write.key.len + write.value.len);
    for (deletes) |key| stats.delete_key_bytes += @intCast(key.len);
}

const IndexEntryCounts = struct { scalar: usize = 0, ordered: usize = 0 };

fn verifyIndexEntries(alloc: std.mem.Allocator, store: *docstore_mod.DocStore, shape: IndexShape) !IndexEntryCounts {
    var out = IndexEntryCounts{};
    if (shape == .scalar_nullable) {
        const values = try relational_store.scanColumnAlloc(alloc, store, "amount", "row:", "row:\xff");
        defer relational_store.freeColumnValues(alloc, values);
        out.scalar = values.len;
    } else if (shape == .mixed_multi) {
        const values = try relational_store.scanColumnAlloc(alloc, store, "status", "row:", "row:\xff");
        defer relational_store.freeColumnValues(alloc, values);
        out.scalar = values.len;
    }
    if (shape == .ordered_nullable or shape == .mixed_multi) {
        const keys = try relational_store.scanOrderedTupleDocKeysAlloc(alloc, store, "amount_ordered_idx", "", "row:", "row:\xff");
        defer relational_store.freeDocKeys(alloc, keys);
        out.ordered = keys.len;
    }
    return out;
}

fn expectedScalarEntries(shape: IndexShape, new: ?RowShape, docs: usize) usize {
    const row = new orelse return 0;
    return switch (shape) {
        .scalar_nullable => if (row == .open_null) 0 else docs,
        .ordered_nullable => 0,
        .mixed_multi => docs,
    };
}

fn expectedOrderedEntries(shape: IndexShape, new: ?RowShape, docs: usize) usize {
    _ = new orelse return 0;
    return switch (shape) {
        .scalar_nullable => 0,
        .ordered_nullable, .mixed_multi => docs,
    };
}

fn printMutationResult(
    writer: anytype,
    cfg: Config,
    shape: IndexShape,
    operation: OperationCase,
    sample: usize,
    result: CaseResult,
) !void {
    const mutations = result.mutations.writes + result.mutations.deletes;
    try writer.print(
        "{{\"event\":\"relational_index_maintenance\",\"schema_version\":{d},\"index_shape\":\"{s}\",\"operation\":\"{s}\",\"sample\":{d},\"docs\":{d},\"elapsed_ns\":{d},\"ns_per_row\":{d:.2},\"allocation_events\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"staged_writes\":{d},\"staged_deletes\":{d},\"staged_mutations\":{d},\"staged_write_bytes\":{d},\"staged_delete_key_bytes\":{d},\"mutations_per_row\":{d:.3},\"scalar_entries_after\":{d},\"ordered_entries_after\":{d},\"cleanup_verified\":{}}}\n",
        .{
            jsonl_schema_version,
            @tagName(shape),
            operation.name,
            sample,
            cfg.docs,
            result.elapsed_ns,
            @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, @floatFromInt(cfg.docs)),
            result.allocations.events,
            result.allocations.bytes,
            result.allocations.peak_live_bytes,
            result.mutations.writes,
            result.mutations.deletes,
            mutations,
            result.mutations.write_bytes,
            result.mutations.delete_key_bytes,
            @as(f64, @floatFromInt(mutations)) / @as(f64, @floatFromInt(cfg.docs)),
            result.scalar_entries,
            result.ordered_entries,
            result.cleanup_verified,
        },
    );
}

fn printRebuildResult(writer: anytype, cfg: Config, shape: IndexShape, sample: usize, result: CaseResult) !void {
    try writer.print(
        "{{\"event\":\"relational_index_rebuild\",\"schema_version\":{d},\"index_shape\":\"{s}\",\"sample\":{d},\"docs\":{d},\"elapsed_ns\":{d},\"ns_per_row\":{d:.2},\"allocation_events\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"written_entries\":{d},\"deleted_entries\":{d},\"scalar_entries_after\":{d},\"ordered_entries_after\":{d},\"cleanup_verified\":{}}}\n",
        .{
            jsonl_schema_version,
            @tagName(shape),
            sample,
            cfg.docs,
            result.elapsed_ns,
            @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, @floatFromInt(cfg.docs)),
            result.allocations.events,
            result.allocations.bytes,
            result.allocations.peak_live_bytes,
            result.mutations.writes,
            result.mutations.deletes,
            result.scalar_entries,
            result.ordered_entries,
            result.cleanup_verified,
        },
    );
}
