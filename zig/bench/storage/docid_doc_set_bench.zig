// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const bench_root = @import("docid_doc_set_bench_root");
const doc_set = bench_root.doc_set;
const roaring = bench_root.roaring;

const Config = struct {
    samples: usize = 1,
    repeats: usize = 32,
    small_cardinality: usize = 32,
    medium_cardinality: usize = 1024,
    large_cardinality: usize = 16_384,
    dense_stride: u64 = 1,
    sparse_stride: u64 = 97,
    max_doc_key_cardinality: usize = 1024,
};

const Scenario = struct {
    name: []const u8,
    cardinality: usize,
    stride: u64,
};

const Result = struct {
    representation: []const u8,
    operation: []const u8,
    scenario: []const u8,
    cardinality: usize,
    stride: u64,
    repeats: usize,
    ops: usize,
    ns: u64,
    result_cardinality: usize,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.page_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);
    const scenarios = [_]Scenario{
        .{ .name = "small_dense", .cardinality = cfg.small_cardinality, .stride = cfg.dense_stride },
        .{ .name = "small_sparse", .cardinality = cfg.small_cardinality, .stride = cfg.sparse_stride },
        .{ .name = "medium_dense", .cardinality = cfg.medium_cardinality, .stride = cfg.dense_stride },
        .{ .name = "medium_sparse", .cardinality = cfg.medium_cardinality, .stride = cfg.sparse_stride },
        .{ .name = "large_dense", .cardinality = cfg.large_cardinality, .stride = cfg.dense_stride },
        .{ .name = "large_sparse", .cardinality = cfg.large_cardinality, .stride = cfg.sparse_stride },
    };

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "docid doc-set bench samples={d} repeats={d} small={d} medium={d} large={d} dense_stride={d} sparse_stride={d} max_doc_key_cardinality={d}\n",
        .{
            cfg.samples,
            cfg.repeats,
            cfg.small_cardinality,
            cfg.medium_cardinality,
            cfg.large_cardinality,
            cfg.dense_stride,
            cfg.sparse_stride,
            cfg.max_doc_key_cardinality,
        },
    );

    for (0..cfg.samples) |sample| {
        for (scenarios) |scenario| {
            try runScenario(alloc, out, sample, cfg, scenario);
            try stdout_writer.flush();
        }
    }
    try stdout_writer.flush();
}

fn runScenario(
    alloc: std.mem.Allocator,
    out: anytype,
    sample: usize,
    cfg: Config,
    scenario: Scenario,
) !void {
    const left = try makeOrdinals(alloc, scenario.cardinality, 0, scenario.stride);
    defer alloc.free(left);
    const right = try makeOrdinals(alloc, scenario.cardinality, scenario.cardinality / 2, scenario.stride);
    defer alloc.free(right);

    try printResult(out, sample, try benchOrdinalArrayOp(alloc, "ordinal_u32_sorted", "union", scenario, cfg.repeats, left, right));
    try printResult(out, sample, try benchOrdinalArrayOp(alloc, "ordinal_u32_sorted", "intersect", scenario, cfg.repeats, left, right));
    try printResult(out, sample, try benchOrdinalArrayOp(alloc, "ordinal_u32_sorted", "difference", scenario, cfg.repeats, left, right));

    var left_bitmap = roaring.RoaringBitmap.init(alloc);
    defer left_bitmap.deinit();
    try left_bitmap.addSortedAscending(left);
    var right_bitmap = roaring.RoaringBitmap.init(alloc);
    defer right_bitmap.deinit();
    try right_bitmap.addSortedAscending(right);
    try printResult(out, sample, try benchRoaringOp(alloc, "roaring_bitmap_direct", "union", scenario, cfg.repeats, &left_bitmap, &right_bitmap));
    try printResult(out, sample, try benchRoaringOp(alloc, "roaring_bitmap_direct", "intersect", scenario, cfg.repeats, &left_bitmap, &right_bitmap));
    try printResult(out, sample, try benchRoaringOp(alloc, "roaring_bitmap_direct", "difference", scenario, cfg.repeats, &left_bitmap, &right_bitmap));

    var left_doc_set = try doc_set.fromOrdinalsAlloc(alloc, left);
    defer left_doc_set.deinit(alloc);
    var right_doc_set = try doc_set.fromOrdinalsAlloc(alloc, right);
    defer right_doc_set.deinit(alloc);
    try printResult(out, sample, try benchDocSetOp(alloc, "ordinal_doc_set", "union", scenario, cfg.repeats, &left_doc_set, &right_doc_set));
    try printResult(out, sample, try benchDocSetOp(alloc, "ordinal_doc_set", "intersect", scenario, cfg.repeats, &left_doc_set, &right_doc_set));
    try printResult(out, sample, try benchDocSetOp(alloc, "ordinal_doc_set", "difference", scenario, cfg.repeats, &left_doc_set, &right_doc_set));

    const left_ids = try makeSparseIds(alloc, scenario.cardinality, 0, scenario.stride);
    defer alloc.free(left_ids);
    const right_ids = try makeSparseIds(alloc, scenario.cardinality, scenario.cardinality / 2, scenario.stride);
    defer alloc.free(right_ids);
    try printResult(out, sample, try benchSparseIdOp(alloc, "sparse_u64_sorted", "union", scenario, cfg.repeats, left_ids, right_ids));
    try printResult(out, sample, try benchSparseIdOp(alloc, "sparse_u64_sorted", "intersect", scenario, cfg.repeats, left_ids, right_ids));
    try printResult(out, sample, try benchSparseIdOp(alloc, "sparse_u64_sorted", "difference", scenario, cfg.repeats, left_ids, right_ids));

    if (scenario.cardinality <= cfg.max_doc_key_cardinality) {
        const left_keys = try makeDocKeys(alloc, scenario.cardinality, 0, scenario.stride);
        defer freeDocKeys(alloc, left_keys);
        const right_keys = try makeDocKeys(alloc, scenario.cardinality, scenario.cardinality / 2, scenario.stride);
        defer freeDocKeys(alloc, right_keys);
        var left_key_set = try doc_set.cloneDocKeysAlloc(alloc, left_keys);
        defer left_key_set.deinit(alloc);
        var right_key_set = try doc_set.cloneDocKeysAlloc(alloc, right_keys);
        defer right_key_set.deinit(alloc);
        try printResult(out, sample, try benchDocSetOp(alloc, "public_doc_key_set", "union", scenario, cfg.repeats, &left_key_set, &right_key_set));
        try printResult(out, sample, try benchDocSetOp(alloc, "public_doc_key_set", "intersect", scenario, cfg.repeats, &left_key_set, &right_key_set));
        try printResult(out, sample, try benchDocSetOp(alloc, "public_doc_key_set", "difference", scenario, cfg.repeats, &left_key_set, &right_key_set));
    }
}

fn benchOrdinalArrayOp(
    alloc: std.mem.Allocator,
    representation: []const u8,
    operation: []const u8,
    scenario: Scenario,
    repeats: usize,
    left: []const doc_set.DocOrdinal,
    right: []const doc_set.DocOrdinal,
) !Result {
    var result_cardinality: usize = 0;
    const start = nanotime();
    for (0..repeats) |_| {
        const result = if (std.mem.eql(u8, operation, "union"))
            try unionOrdinalsAlloc(alloc, left, right)
        else if (std.mem.eql(u8, operation, "intersect"))
            try intersectOrdinalsAlloc(alloc, left, right)
        else if (std.mem.eql(u8, operation, "difference"))
            try differenceOrdinalsAlloc(alloc, left, right)
        else
            return error.InvalidArgument;
        defer alloc.free(result);
        result_cardinality = result.len;
        std.mem.doNotOptimizeAway(result_cardinality);
    }
    return .{
        .representation = representation,
        .operation = operation,
        .scenario = scenario.name,
        .cardinality = scenario.cardinality,
        .stride = scenario.stride,
        .repeats = repeats,
        .ops = repeats,
        .ns = nanotime() - start,
        .result_cardinality = result_cardinality,
    };
}

fn benchRoaringOp(
    alloc: std.mem.Allocator,
    representation: []const u8,
    operation: []const u8,
    scenario: Scenario,
    repeats: usize,
    left: *const roaring.RoaringBitmap,
    right: *const roaring.RoaringBitmap,
) !Result {
    var result_cardinality: usize = 0;
    const start = nanotime();
    for (0..repeats) |_| {
        var result = try left.clone(alloc);
        defer result.deinit();
        if (std.mem.eql(u8, operation, "union")) {
            try result.orWith(right);
        } else if (std.mem.eql(u8, operation, "intersect")) {
            result.andWith(right);
        } else if (std.mem.eql(u8, operation, "difference")) {
            result.andNotWith(right);
        } else {
            return error.InvalidArgument;
        }
        result_cardinality = result.cardinality();
        std.mem.doNotOptimizeAway(result_cardinality);
    }
    return .{
        .representation = representation,
        .operation = operation,
        .scenario = scenario.name,
        .cardinality = scenario.cardinality,
        .stride = scenario.stride,
        .repeats = repeats,
        .ops = repeats,
        .ns = nanotime() - start,
        .result_cardinality = result_cardinality,
    };
}

fn benchDocSetOp(
    alloc: std.mem.Allocator,
    representation: []const u8,
    operation: []const u8,
    scenario: Scenario,
    repeats: usize,
    left: *const doc_set.ResolvedDocSet,
    right: *const doc_set.ResolvedDocSet,
) !Result {
    var result_cardinality: usize = 0;
    const start = nanotime();
    for (0..repeats) |_| {
        var result = if (std.mem.eql(u8, operation, "union"))
            (try doc_set.unionAlloc(alloc, left, right)).?
        else if (std.mem.eql(u8, operation, "intersect"))
            (try doc_set.intersectAlloc(alloc, left, right)).?
        else if (std.mem.eql(u8, operation, "difference"))
            (try doc_set.differenceAlloc(alloc, left, right)).?
        else
            return error.InvalidArgument;
        defer result.deinit(alloc);
        result_cardinality = result.estimatedCardinality() orelse 0;
        std.mem.doNotOptimizeAway(result_cardinality);
    }
    return .{
        .representation = representation,
        .operation = operation,
        .scenario = scenario.name,
        .cardinality = scenario.cardinality,
        .stride = scenario.stride,
        .repeats = repeats,
        .ops = repeats,
        .ns = nanotime() - start,
        .result_cardinality = result_cardinality,
    };
}

fn benchSparseIdOp(
    alloc: std.mem.Allocator,
    representation: []const u8,
    operation: []const u8,
    scenario: Scenario,
    repeats: usize,
    left: []const u64,
    right: []const u64,
) !Result {
    var result_cardinality: usize = 0;
    const start = nanotime();
    for (0..repeats) |_| {
        const result = if (std.mem.eql(u8, operation, "union"))
            try unionSparseIdsAlloc(alloc, left, right)
        else if (std.mem.eql(u8, operation, "intersect"))
            try intersectSparseIdsAlloc(alloc, left, right)
        else if (std.mem.eql(u8, operation, "difference"))
            try differenceSparseIdsAlloc(alloc, left, right)
        else
            return error.InvalidArgument;
        defer alloc.free(result);
        result_cardinality = result.len;
        std.mem.doNotOptimizeAway(result_cardinality);
    }
    return .{
        .representation = representation,
        .operation = operation,
        .scenario = scenario.name,
        .cardinality = scenario.cardinality,
        .stride = scenario.stride,
        .repeats = repeats,
        .ops = repeats,
        .ns = nanotime() - start,
        .result_cardinality = result_cardinality,
    };
}

fn unionOrdinalsAlloc(
    alloc: std.mem.Allocator,
    left: []const doc_set.DocOrdinal,
    right: []const doc_set.DocOrdinal,
) ![]doc_set.DocOrdinal {
    var out = try std.ArrayListUnmanaged(doc_set.DocOrdinal).initCapacity(alloc, left.len + right.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len or j < right.len) {
        if (j >= right.len or (i < left.len and left[i] < right[j])) {
            out.appendAssumeCapacity(left[i]);
            i += 1;
        } else if (i >= left.len or right[j] < left[i]) {
            out.appendAssumeCapacity(right[j]);
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn intersectOrdinalsAlloc(
    alloc: std.mem.Allocator,
    left: []const doc_set.DocOrdinal,
    right: []const doc_set.DocOrdinal,
) ![]doc_set.DocOrdinal {
    var out = try std.ArrayListUnmanaged(doc_set.DocOrdinal).initCapacity(alloc, @min(left.len, right.len));
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len and j < right.len) {
        if (left[i] < right[j]) {
            i += 1;
        } else if (right[j] < left[i]) {
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn differenceOrdinalsAlloc(
    alloc: std.mem.Allocator,
    left: []const doc_set.DocOrdinal,
    right: []const doc_set.DocOrdinal,
) ![]doc_set.DocOrdinal {
    var out = try std.ArrayListUnmanaged(doc_set.DocOrdinal).initCapacity(alloc, left.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len) {
        while (j < right.len and right[j] < left[i]) j += 1;
        if (j >= right.len or left[i] != right[j]) out.appendAssumeCapacity(left[i]);
        i += 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn unionSparseIdsAlloc(alloc: std.mem.Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = try std.ArrayListUnmanaged(u64).initCapacity(alloc, left.len + right.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len or j < right.len) {
        if (j >= right.len or (i < left.len and left[i] < right[j])) {
            out.appendAssumeCapacity(left[i]);
            i += 1;
        } else if (i >= left.len or right[j] < left[i]) {
            out.appendAssumeCapacity(right[j]);
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn intersectSparseIdsAlloc(alloc: std.mem.Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = try std.ArrayListUnmanaged(u64).initCapacity(alloc, @min(left.len, right.len));
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len and j < right.len) {
        if (left[i] < right[j]) {
            i += 1;
        } else if (right[j] < left[i]) {
            j += 1;
        } else {
            out.appendAssumeCapacity(left[i]);
            i += 1;
            j += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn differenceSparseIdsAlloc(alloc: std.mem.Allocator, left: []const u64, right: []const u64) ![]u64 {
    var out = try std.ArrayListUnmanaged(u64).initCapacity(alloc, left.len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    var j: usize = 0;
    while (i < left.len) {
        while (j < right.len and right[j] < left[i]) j += 1;
        if (j >= right.len or left[i] != right[j]) out.appendAssumeCapacity(left[i]);
        i += 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn makeOrdinals(alloc: std.mem.Allocator, count: usize, offset: usize, stride: u64) ![]doc_set.DocOrdinal {
    const out = try alloc.alloc(doc_set.DocOrdinal, count);
    errdefer alloc.free(out);
    for (out, 0..) |*slot, i| {
        const value = (@as(u64, @intCast(i + offset)) * stride);
        if (value > std.math.maxInt(doc_set.DocOrdinal)) return error.InvalidArgument;
        slot.* = @intCast(value);
    }
    return out;
}

fn makeSparseIds(alloc: std.mem.Allocator, count: usize, offset: usize, stride: u64) ![]u64 {
    const out = try alloc.alloc(u64, count);
    errdefer alloc.free(out);
    for (out, 0..) |*slot, i| {
        slot.* = 10_000_000_000 + (@as(u64, @intCast(i + offset)) * stride);
    }
    return out;
}

fn makeDocKeys(alloc: std.mem.Allocator, count: usize, offset: usize, stride: u64) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(out);
    }
    for (out, 0..) |*slot, i| {
        const id = 10_000_000_000 + (@as(u64, @intCast(i + offset)) * stride);
        slot.* = try std.fmt.allocPrint(alloc, "doc:{d:0>12}", .{id});
        initialized += 1;
    }
    return out;
}

fn freeDocKeys(alloc: std.mem.Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    alloc.free(keys);
}

fn printResult(writer: anytype, sample: usize, result: Result) !void {
    const secs = @as(f64, @floatFromInt(result.ns)) / 1e9;
    const ops_per_sec = if (result.ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(result.ops)) / secs;
    try writer.print(
        "{{\"sample\":{d},\"representation\":\"{s}\",\"operation\":\"{s}\",\"scenario\":\"{s}\",\"cardinality\":{d},\"stride\":{d},\"repeats\":{d},\"ops\":{d},\"ns\":{d},\"ops_per_sec\":{d:.2},\"result_cardinality\":{d}}}\n",
        .{
            sample,
            result.representation,
            result.operation,
            result.scenario,
            result.cardinality,
            result.stride,
            result.repeats,
            result.ops,
            result.ns,
            ops_per_sec,
            result.result_cardinality,
        },
    );
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--small")) {
            cfg.small_cardinality = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--medium")) {
            cfg.medium_cardinality = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--large")) {
            cfg.large_cardinality = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--dense-stride")) {
            cfg.dense_stride = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--sparse-stride")) {
            cfg.sparse_stride = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-doc-key-cardinality")) {
            cfg.max_doc_key_cardinality = try parseNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(u64, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn nanotime() u64 {
    return bench_root.platform_time.monotonicNs();
}
