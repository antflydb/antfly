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

const Operation = enum { insert, overwrite };
const SchemaShape = enum { no_derived_artifact, generated_only };
const Path = enum { generic_baseline, base_row_fast_path };

const Config = struct {
    docs: usize = 10_000,
    batch_size: usize = 1_000,
    samples: usize = 3,
};

const AllocationStats = struct {
    events: u64 = 0,
    bytes: u64 = 0,
};

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocation_events: u64 = 0,
    allocated_bytes: u64 = 0,

    fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn snapshot(self: *const CountingAllocator) AllocationStats {
        return .{ .events = self.allocation_events, .bytes = self.allocated_bytes };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocation_events += 1;
        self.allocated_bytes += @intCast(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) {
            self.allocation_events += 1;
            self.allocated_bytes += @intCast(new_len - memory.len);
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            self.allocation_events += 1;
            self.allocated_bytes += @intCast(new_len - memory.len);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

const Result = struct {
    elapsed_ns: u64,
    allocations: AllocationStats,
    staged_writes: u64,
    staged_write_bytes: u64,
    base_row_fast_path_upserts: u64,
    generic_upserts: u64,
    authoritative_row_lookups: u64,
    row_decodes: u64,
};

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init.minimal.args);
    var counting = CountingAllocator.init(std.heap.c_allocator);
    const alloc = counting.allocator();
    const operations = [_]Operation{ .insert, .overwrite };
    const shapes = [_]SchemaShape{ .no_derived_artifact, .generated_only };
    const paths = [_]Path{ .generic_baseline, .base_row_fast_path };

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "{{\"event\":\"relational_write_fast_path_config\",\"schema_version\":1,\"docs\":{d},\"batch_size\":{d},\"samples\":{d},\"schema_shapes\":[\"no_derived_artifact\",\"generated_only\"],\"operations\":[\"insert\",\"overwrite\"],\"paths\":[\"generic_baseline\",\"base_row_fast_path\"]}}\n",
        .{ cfg.docs, cfg.batch_size, cfg.samples },
    );

    for (shapes) |shape| {
        for (operations) |operation| {
            for (paths) |path| {
                for (0..cfg.samples) |sample| {
                    const result = try runCase(alloc, &counting, cfg, shape, operation, path);
                    try out.print(
                        "{{\"event\":\"relational_write_fast_path\",\"schema_version\":1,\"schema_shape\":\"{s}\",\"operation\":\"{s}\",\"path\":\"{s}\",\"sample\":{d},\"docs\":{d},\"elapsed_ns\":{d},\"ns_per_row\":{d:.2},\"allocation_events\":{d},\"allocated_bytes\":{d},\"staged_writes\":{d},\"staged_write_bytes\":{d},\"writes_per_row\":{d:.3},\"base_row_fast_path_upserts\":{d},\"generic_upserts\":{d},\"authoritative_row_lookups\":{d},\"row_decodes\":{d}}}\n",
                        .{
                            @tagName(shape),
                            @tagName(operation),
                            @tagName(path),
                            sample,
                            cfg.docs,
                            result.elapsed_ns,
                            @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, @floatFromInt(cfg.docs)),
                            result.allocations.events,
                            result.allocations.bytes,
                            result.staged_writes,
                            result.staged_write_bytes,
                            @as(f64, @floatFromInt(result.staged_writes)) / @as(f64, @floatFromInt(cfg.docs)),
                            result.base_row_fast_path_upserts,
                            result.generic_upserts,
                            result.authoritative_row_lookups,
                            result.row_decodes,
                        },
                    );
                }
            }
        }
    }
    try out.flush();
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

fn runCase(
    alloc: std.mem.Allocator,
    counting: *CountingAllocator,
    cfg: Config,
    shape: SchemaShape,
    operation: Operation,
    path: Path,
) !Result {
    var backend = antfly.mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "id", .value_type = .bytes_val, .value = .{ .bytes_val = "row" } },
        .{ .path = "title", .value_type = .bytes_val, .value = .{ .bytes_val = "Hello" } },
        .{ .path = "title_lc", .value_type = .bytes_val, .value = .{ .bytes_val = "hello" } },
    });
    defer alloc.free(row);
    const doc_keys = try alloc.alloc([]u8, cfg.docs);
    defer {
        for (doc_keys) |key| alloc.free(key);
        alloc.free(doc_keys);
    }
    for (doc_keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(alloc, "row:{d}", .{i});

    if (operation == .overwrite) try stageParticipantRows(alloc, &store, doc_keys, row, cfg.batch_size, shape, true, null);

    const allocations_before = counting.snapshot();
    const start_ns = antfly.platform_time.monotonicNs();
    var staged_writes: u64 = 0;
    var staged_write_bytes: u64 = 0;
    var stats = relational_store.WriteParticipantStats{};
    switch (path) {
        .generic_baseline => try stageParticipantRows(
            alloc,
            &store,
            doc_keys,
            row,
            cfg.batch_size,
            shape,
            false,
            .{ .stats = &stats, .staged_writes = &staged_writes, .staged_write_bytes = &staged_write_bytes },
        ),
        .base_row_fast_path => try stageParticipantRows(
            alloc,
            &store,
            doc_keys,
            row,
            cfg.batch_size,
            shape,
            true,
            .{ .stats = &stats, .staged_writes = &staged_writes, .staged_write_bytes = &staged_write_bytes },
        ),
    }
    const elapsed_ns = antfly.platform_time.monotonicNs() - start_ns;
    const allocations_after = counting.snapshot();
    return .{
        .elapsed_ns = elapsed_ns,
        .allocations = .{
            .events = allocations_after.events - allocations_before.events,
            .bytes = allocations_after.bytes - allocations_before.bytes,
        },
        .staged_writes = staged_writes,
        .staged_write_bytes = staged_write_bytes,
        .base_row_fast_path_upserts = stats.base_row_fast_path_upserts,
        .generic_upserts = stats.generic_upserts,
        .authoritative_row_lookups = stats.authoritative_row_lookups,
        .row_decodes = stats.row_decodes,
    };
}

const FastPathOutput = struct {
    stats: *relational_store.WriteParticipantStats,
    staged_writes: *u64,
    staged_write_bytes: *u64,
};

fn stageParticipantRows(
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,
    doc_keys: []const []const u8,
    row: []const u8,
    batch_size: usize,
    shape: SchemaShape,
    fast_path_enabled: bool,
    output: ?FastPathOutput,
) !void {
    const base_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .indexed = false },
        .{ .name = "title", .path = "title", .field_type = .keyword, .indexed = false },
    };
    const generated_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .indexed = false },
        .{ .name = "title", .path = "title", .field_type = .keyword, .indexed = false },
        .{ .name = "title_lc", .path = "title_lc", .field_type = .keyword, .indexed = false, .generated = .{ .op = .lower, .field = "title" } },
    };
    const columns = if (shape == .generated_only) generated_columns[0..] else base_columns[0..];

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
        var participant = relational_store.WriteParticipant.initWithColumnIndexPolicy(
            alloc,
            store,
            &writes,
            &deletes,
            &owned_keys,
            &owned_values,
            relational_store.ColumnIndexPolicy.empty(),
        );
        participant.configurePeriods(&.{}, columns);
        participant.configureBaseRowFastPathEnabled(fast_path_enabled);
        for (doc_keys[start..end]) |doc_key| try participant.prepareUpsert("row", doc_key, row, null);
        try participant.closePreparedIntents();
        if (output) |out| {
            const delta = participant.statsSnapshot();
            out.stats.base_row_fast_path_upserts += delta.base_row_fast_path_upserts;
            out.stats.generic_upserts += delta.generic_upserts;
            out.stats.authoritative_row_lookups += delta.authoritative_row_lookups;
            out.stats.row_decodes += delta.row_decodes;
            recordStaged(writes.items, out.staged_writes, out.staged_write_bytes);
        }
        try store.putBatch(writes.items, deletes.items);
    }
}

fn recordStaged(writes: []const docstore_mod.KVPair, count: *u64, bytes: *u64) void {
    count.* += @intCast(writes.len);
    for (writes) |write| bytes.* += @intCast(write.key.len + write.value.len);
}
