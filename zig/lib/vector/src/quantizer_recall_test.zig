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
const proto = @import("proto.zig");
const quantizer_mod = @import("quantizer.zig");
const vec = @import("vector.zig");

const OwnedVectorSet = struct {
    dims: usize,
    count: usize,
    data: []f32,

    fn deinit(self: *OwnedVectorSet, alloc: std.mem.Allocator) void {
        alloc.free(self.data);
        self.* = undefined;
    }

    fn asSet(self: *const OwnedVectorSet) vec.Set {
        return .{
            .dims = self.dims,
            .count = self.count,
            .data = self.data,
        };
    }
};

const SplitDataset = struct {
    data: vec.Set,
    queries: vec.Set,
};

const MetricStats = struct {
    euclidean: f64,
    inner_product: f64,
    cosine: f64,
};

const RecallCase = struct {
    dataset: []const u8,
    randomize: bool = false,
    top_k: usize = 10,
    count: usize,
    tolerance: f64 = 0.500001,
    expected: MetricStats,
};

const parity_cases = [_]RecallCase{
    .{ .dataset = "images-512d-10k.gob", .count = 1000, .expected = .{ .euclidean = 70.00, .inner_product = 70.00, .cosine = 69.50 } },
    .{ .dataset = "images-512d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 85.00, .inner_product = 85.00, .cosine = 85.00 } },
    .{ .dataset = "random-20d-1k.gob", .count = 1000, .expected = .{ .euclidean = 88.00, .inner_product = 93.50, .cosine = 89.00 } },
    .{ .dataset = "random-20d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 88.50, .inner_product = 89.00, .cosine = 88.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .count = 1000, .expected = .{ .euclidean = 76.00, .inner_product = 75.00, .cosine = 70.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 87.50, .inner_product = 87.00, .cosine = 85.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .count = 1000, .expected = .{ .euclidean = 81.50, .inner_product = 81.50, .cosine = 81.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 85.00, .inner_product = 85.00, .cosine = 85.00 } },
};

test "RaBitQuantizer recall matches Go fixture subset" {
    for (parity_cases) |case| {
        const dataset_path = try convertedDatasetPathAlloc(std.testing.allocator, case.dataset);
        defer std.testing.allocator.free(dataset_path);

        std.Io.Dir.cwd().access(std.testing.io, dataset_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.SkipZigTest,
            else => return err,
        };

        const actual = try runQuantizerCase(dataset_path, case);

        try std.testing.expectApproxEqAbs(case.expected.euclidean, actual.euclidean, case.tolerance);
        try std.testing.expectApproxEqAbs(case.expected.inner_product, actual.inner_product, case.tolerance);
        try std.testing.expectApproxEqAbs(case.expected.cosine, actual.cosine, case.tolerance);
    }
}

fn convertedDatasetPathAlloc(alloc: std.mem.Allocator, gob_name: []const u8) ![]u8 {
    const file_dir = std.fs.path.dirname(@src().file) orelse return error.Unexpected;
    const root = try std.fs.path.resolve(alloc, &.{ file_dir, "..", "..", ".." });
    defer alloc.free(root);

    const converted = if (std.mem.endsWith(u8, gob_name, ".gob"))
        try std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name[0 .. gob_name.len - 4]})
    else
        try std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name});
    defer alloc.free(converted);

    return std.fs.path.join(alloc, &.{ root, "testdata", "vectorsets", converted });
}

fn runQuantizerCase(dataset_path: []const u8, case: RecallCase) !MetricStats {
    const alloc = std.testing.allocator;

    var loaded = try loadVectorSet(alloc, dataset_path);
    defer loaded.deinit(alloc);

    var working = try cloneSet(alloc, loaded.asSet());
    defer working.deinit(alloc);

    const split = try splitDataset(working.asSet(), case.count);
    if (case.randomize) {
        try applyRandomTransformInPlace(alloc, split.data, 42);
        try applyRandomTransformInPlace(alloc, split.queries, 42);
    }

    return .{
        .euclidean = 100.0 * try calculateQuantizerRecallMetric(split, case.top_k, .l2_squared),
        .inner_product = 100.0 * try calculateQuantizerRecallMetric(split, case.top_k, .inner_product),
        .cosine = 100.0 * try calculateQuantizerRecallMetric(split, case.top_k, .cosine),
    };
}

fn calculateQuantizerRecallMetric(
    split: SplitDataset,
    top_k: usize,
    metric: vec.DistanceMetric,
) !f64 {
    const alloc = std.testing.allocator;

    var data_owned = try cloneSet(alloc, split.data);
    defer data_owned.deinit(alloc);
    var query_owned = try cloneSet(alloc, split.queries);
    defer query_owned.deinit(alloc);

    if (metric == .cosine) {
        normalizeSetInPlace(data_owned.asSet());
        normalizeSetInPlace(query_owned.asSet());
    }

    const data = data_owned.asSet();
    const queries = query_owned.asSet();
    const centroid = try computeCentroid(alloc, data);
    defer alloc.free(centroid);

    var quantizer = try quantizer_mod.RaBitQuantizer.init(alloc, data.dims, 42, metric);
    defer quantizer.deinit();

    var quantized = try quantizer.quantize(centroid, data.data, data.count);
    defer quantized.deinit(alloc);

    var scratch = try quantizer_mod.RaBitQuantizer.EstimateScratch.init(alloc, data.dims);
    defer scratch.deinit(alloc);

    const estimated = try alloc.alloc(f32, data.count);
    defer alloc.free(estimated);
    const error_bounds = try alloc.alloc(f32, data.count);
    defer alloc.free(error_bounds);
    const prediction = try alloc.alloc(usize, data.count);
    defer alloc.free(prediction);

    var recall_sum: f64 = 0;
    for (0..queries.count) |query_idx| {
        const query = queries.atConst(query_idx);
        try quantizer.estimateDistancesWithScratch(&quantized, query, estimated, error_bounds, &scratch);
        for (prediction, 0..) |*slot, i| slot.* = i;
        std.mem.sort(usize, prediction, DistanceCtx{ .distances = estimated }, DistanceCtx.lessThan);

        const truth = try calculateTruth(alloc, top_k, metric, query, data);
        defer alloc.free(truth);
        recall_sum += calculateRecall(prediction[0..top_k], truth);
    }

    return recall_sum / @as(f64, @floatFromInt(queries.count));
}

fn loadVectorSet(alloc: std.mem.Allocator, path: []const u8) !OwnedVectorSet {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(1 << 30));
    defer alloc.free(bytes);

    var decoded = try proto.VectorSet.decode(alloc, bytes);
    errdefer decoded.deinit(alloc);

    const dims: usize = @intCast(decoded.dims);
    const count: usize = @intCast(decoded.count);
    if (dims == 0 or count == 0) return error.EmptyDataset;
    if (decoded.data.len != dims * count) return error.MalformedDataset;

    return .{
        .dims = dims,
        .count = count,
        .data = decoded.data,
    };
}

fn cloneSet(alloc: std.mem.Allocator, set: vec.Set) !OwnedVectorSet {
    return .{
        .dims = set.dims,
        .count = set.count,
        .data = try alloc.dupe(f32, set.data),
    };
}

fn splitDataset(set: vec.Set, count: usize) !SplitDataset {
    if (count == 0 or count > set.count) return error.InvalidCount;
    const data_count = (count * 98) / 100;
    if (data_count == 0 or data_count >= count) return error.InvalidCount;
    const query_count = count - data_count;

    return .{
        .data = .{
            .dims = set.dims,
            .count = data_count,
            .data = set.data[0 .. data_count * set.dims],
        },
        .queries = .{
            .dims = set.dims,
            .count = query_count,
            .data = set.data[data_count * set.dims .. count * set.dims],
        },
    };
}

fn normalizeSetInPlace(set: vec.Set) void {
    for (0..set.count) |i| {
        _ = vec.normalize(set.at(i));
    }
}

fn applyRandomTransformInPlace(
    alloc: std.mem.Allocator,
    set: vec.Set,
    seed: u64,
) !void {
    var transformer = try vec.RandomOrthogonalTransformer.init(alloc, .givens, set.dims, seed);
    defer transformer.deinit();

    const scratch = try alloc.alloc(f32, set.dims);
    defer alloc.free(scratch);

    for (0..set.count) |i| {
        const target = set.at(i);
        _ = transformer.transform(target, scratch);
        @memcpy(target, scratch);
    }
}

fn calculateTruth(
    alloc: std.mem.Allocator,
    top_k: usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    data: vec.Set,
) ![]usize {
    if (top_k == 0 or top_k > data.count) return error.InvalidTopK;

    const distances = try alloc.alloc(f32, data.count);
    defer alloc.free(distances);
    const offsets = try alloc.alloc(usize, data.count);
    defer alloc.free(offsets);

    for (0..data.count) |i| {
        distances[i] = vec.distance(query, data.atConst(i), metric);
        offsets[i] = i;
    }

    std.mem.sort(usize, offsets, DistanceCtx{ .distances = distances }, DistanceCtx.lessThan);
    return try alloc.dupe(usize, offsets[0..top_k]);
}

fn calculateRecall(prediction: []const usize, truth: []const usize) f64 {
    var hits: usize = 0;
    for (truth) |item| {
        for (prediction) |candidate| {
            if (candidate == item) {
                hits += 1;
                break;
            }
        }
    }
    return @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(truth.len));
}

fn computeCentroid(
    alloc: std.mem.Allocator,
    data: vec.Set,
) ![]f32 {
    const centroid = try alloc.alloc(f32, data.dims);
    @memset(centroid, 0);
    for (0..data.count) |i| {
        vec.add(centroid, data.atConst(i));
    }
    vec.scale(1.0 / @as(f32, @floatFromInt(data.count)), centroid);
    return centroid;
}

const DistanceCtx = struct {
    distances: []const f32,

    fn lessThan(ctx: DistanceCtx, lhs: usize, rhs: usize) bool {
        if (ctx.distances[lhs] != ctx.distances[rhs]) return ctx.distances[lhs] < ctx.distances[rhs];
        return lhs < rhs;
    }
};
