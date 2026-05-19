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

pub const proto = antfly.proto;
pub const quantizer_mod = antfly.quantizer;
pub const vec = antfly.vector;

pub const OwnedVectorSet = struct {
    dims: usize,
    count: usize,
    data: []f32,

    pub fn deinit(self: *OwnedVectorSet, alloc: std.mem.Allocator) void {
        alloc.free(self.data);
        self.* = undefined;
    }

    pub fn asSet(self: *const OwnedVectorSet) vec.Set {
        return .{
            .dims = self.dims,
            .count = self.count,
            .data = self.data,
        };
    }
};

pub const SplitDataset = struct {
    data: vec.Set,
    queries: vec.Set,
};

pub const MetricStats = struct {
    euclidean: f64,
    inner_product: f64,
    cosine: f64,
};

pub fn loadVectorSet(io: std.Io, alloc: std.mem.Allocator, path: []const u8) !OwnedVectorSet {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 30));
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

pub fn cloneSet(alloc: std.mem.Allocator, set: vec.Set) !OwnedVectorSet {
    return .{
        .dims = set.dims,
        .count = set.count,
        .data = try alloc.dupe(f32, set.data),
    };
}

pub fn splitDataset(set: vec.Set, count: usize) !SplitDataset {
    if (count == 0 or count > set.count) return error.InvalidCount;
    const data_count = (count * 98) / 100;
    if (data_count == 0 or data_count >= count) return error.InvalidCount;
    const query_count = count - data_count;
    const dims = set.dims;
    return .{
        .data = .{
            .dims = dims,
            .count = data_count,
            .data = set.data[0 .. data_count * dims],
        },
        .queries = .{
            .dims = dims,
            .count = query_count,
            .data = set.data[data_count * dims .. count * dims],
        },
    };
}

pub fn normalizeSetInPlace(set: vec.Set) void {
    for (0..set.count) |i| {
        _ = vec.normalize(set.at(i));
    }
}

pub fn applyRandomTransformInPlace(
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

pub fn calculateTruth(
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

    std.mem.sort(usize, offsets, DistanceSortCtx{
        .distances = distances,
    }, DistanceSortCtx.lessThan);

    return try alloc.dupe(usize, offsets[0..top_k]);
}

pub fn calculateRecall(prediction: []const usize, truth: []const usize) f64 {
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

pub fn metricLabel(metric: vec.DistanceMetric) []const u8 {
    return switch (metric) {
        .l2_squared => "Euclidean",
        .inner_product => "InnerProduct",
        .cosine => "Cosine",
    };
}

pub fn metricFileLabel(metric: vec.DistanceMetric) []const u8 {
    return switch (metric) {
        .l2_squared => "l2_squared",
        .inner_product => "inner_product",
        .cosine => "cosine",
    };
}

pub fn computeCentroid(
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

pub fn distanceMetricResults(
    comptime T: type,
    euclidean: T,
    inner_product: T,
    cosine: T,
    metric: vec.DistanceMetric,
) T {
    return switch (metric) {
        .l2_squared => euclidean,
        .inner_product => inner_product,
        .cosine => cosine,
    };
}

const DistanceSortCtx = struct {
    distances: []const f32,

    fn lessThan(ctx: DistanceSortCtx, lhs: usize, rhs: usize) bool {
        if (ctx.distances[lhs] != ctx.distances[rhs]) {
            return ctx.distances[lhs] < ctx.distances[rhs];
        }
        return lhs < rhs;
    }
};
