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
const proto = @import("antfly_vector").proto;
const vec = @import("antfly_vector").vector;
const hbc = @import("storage/hbc_adapter.zig");
const lsm_backend = @import("storage/lsm_backend/mod.zig");

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

const TruthTable = struct {
    top_k: usize,
    query_count: usize,
    ids: []const usize,

    fn row(self: TruthTable, query_idx: usize) []const usize {
        std.debug.assert(query_idx < self.query_count);
        const start = query_idx * self.top_k;
        return self.ids[start .. start + self.top_k];
    }
};

const TruthCache = struct {
    const Entry = struct {
        dataset: []const u8,
        count: usize,
        top_k: usize,
        metric: vec.DistanceMetric,
        query_count: usize,
        ids: []usize,
    };

    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn init(alloc: std.mem.Allocator) TruthCache {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *TruthCache) void {
        for (self.entries.items) |entry| self.alloc.free(entry.ids);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn getOrCompute(
        self: *TruthCache,
        dataset: []const u8,
        count: usize,
        top_k: usize,
        metric: vec.DistanceMetric,
        split: SplitDataset,
    ) !TruthTable {
        for (self.entries.items) |entry| {
            if (entry.count == count and
                entry.top_k == top_k and
                entry.metric == metric and
                std.mem.eql(u8, entry.dataset, dataset))
            {
                return .{
                    .top_k = entry.top_k,
                    .query_count = entry.query_count,
                    .ids = entry.ids,
                };
            }
        }

        const total_ids = try std.math.mul(usize, split.queries.count, top_k);
        const ids = try self.alloc.alloc(usize, total_ids);
        errdefer self.alloc.free(ids);
        for (0..split.queries.count) |query_idx| {
            const start = query_idx * top_k;
            try calculateTruthInto(ids[start .. start + top_k], metric, split.queries.atConst(query_idx), split.data);
        }

        try self.entries.append(self.alloc, .{
            .dataset = dataset,
            .count = count,
            .top_k = top_k,
            .metric = metric,
            .query_count = split.queries.count,
            .ids = ids,
        });

        return .{
            .top_k = top_k,
            .query_count = split.queries.count,
            .ids = ids,
        };
    }
};

// Uses the same dataset coverage as lib/vectorindex/index_test.go on the Go
// side, but pins the current Zig HBC hilbert-seeded bulk-build recall baselines.
const parity_cases = [_]RecallCase{
    .{ .dataset = "images-512d-10k.gob", .count = 1000, .expected = .{ .euclidean = 95.00, .inner_product = 95.00, .cosine = 90.50 } },
    .{ .dataset = "images-512d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 100.00, .cosine = 99.50 } },
    .{ .dataset = "random-20d-1k.gob", .count = 1000, .expected = .{ .euclidean = 98.50, .inner_product = 99.50, .cosine = 97.50 } },
    .{ .dataset = "random-20d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 97.50, .inner_product = 96.50, .cosine = 97.00 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .count = 1000, .expected = .{ .euclidean = 97.50, .inner_product = 86.50, .cosine = 91.00 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 99.50, .cosine = 99.00 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .count = 1000, .expected = .{ .euclidean = 94.00, .inner_product = 93.00, .cosine = 93.00 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 100.00, .cosine = 100.00 } },
    .{ .dataset = "laionclip-768d-1k.gob", .count = 1000, .expected = .{ .euclidean = 96.00, .inner_product = 96.50, .cosine = 89.50 } },
    .{ .dataset = "laionclip-768d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 98.50, .inner_product = 98.50, .cosine = 97.00 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .count = 1000, .expected = .{ .euclidean = 92.00, .inner_product = 92.00, .cosine = 80.00 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 99.50, .cosine = 96.50 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .count = 10_000, .tolerance = 1.5, .expected = .{ .euclidean = 82.20, .inner_product = 83.65, .cosine = 72.85 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .randomize = true, .count = 10_000, .tolerance = 1.5, .expected = .{ .euclidean = 84.65, .inner_product = 86.20, .cosine = 84.30 } },
    .{ .dataset = "wikiarticles-768d-10k.gob", .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 100.00, .cosine = 98.50 } },
    .{ .dataset = "wikiarticles-768d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 99.50, .cosine = 99.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 99.50, .cosine = 98.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 100.00, .cosine = 99.00 } },
};

test "HBC recall covers Go fixture sets" {
    var truth_cache = TruthCache.init(std.testing.allocator);
    defer truth_cache.deinit();

    var failed = false;
    for (parity_cases) |case| {
        const dataset_path = try convertedDatasetPathAlloc(std.testing.allocator, case.dataset);
        defer std.testing.allocator.free(dataset_path);

        std.Io.Dir.cwd().access(std.testing.io, dataset_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.SkipZigTest,
            else => return err,
        };

        const actual = try runHBCCase(dataset_path, case, null, &truth_cache);

        failed = !expectMetric(case, "euclidean", case.expected.euclidean, actual.euclidean) or failed;
        failed = !expectMetric(case, "inner_product", case.expected.inner_product, actual.inner_product) or failed;
        failed = !expectMetric(case, "cosine", case.expected.cosine, actual.cosine) or failed;
    }

    try std.testing.expect(!failed);
}

test "HBC recall recursive bulk build covers worst fixture" {
    var truth_cache = TruthCache.init(std.testing.allocator);
    defer truth_cache.deinit();

    const case: RecallCase = .{
        .dataset = "laiongemini-512d-10k.gob",
        .count = 10_000,
        .tolerance = 1.5,
        .expected = .{ .euclidean = 84.95, .inner_product = 85.95, .cosine = 80.85 },
    };
    const dataset_path = try convertedDatasetPathAlloc(std.testing.allocator, case.dataset);
    defer std.testing.allocator.free(dataset_path);

    std.Io.Dir.cwd().access(std.testing.io, dataset_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };

    const actual = try runHBCCase(dataset_path, case, .recursive, &truth_cache);
    var failed = false;
    failed = !expectMetric(case, "euclidean", case.expected.euclidean, actual.euclidean) or failed;
    failed = !expectMetric(case, "inner_product", case.expected.inner_product, actual.inner_product) or failed;
    failed = !expectMetric(case, "cosine", case.expected.cosine, actual.cosine) or failed;
    try std.testing.expect(!failed);
}

fn expectMetric(case: RecallCase, metric_name: []const u8, expected: f64, actual: f64) bool {
    if (std.math.approxEqAbs(f64, expected, actual, case.tolerance)) return true;
    std.debug.print(
        "HBC recall mismatch dataset={s} randomize={} metric={s}: expected {d:.2}, actual {d:.2}, tolerance {d:.2}\n",
        .{ case.dataset, case.randomize, metric_name, expected, actual, case.tolerance },
    );
    return false;
}

fn convertedDatasetPathAlloc(alloc: std.mem.Allocator, gob_name: []const u8) ![]u8 {
    const converted = if (std.mem.endsWith(u8, gob_name, ".gob"))
        try std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name[0 .. gob_name.len - 4]})
    else
        try std.fmt.allocPrint(alloc, "{s}.pbvec", .{gob_name});
    defer alloc.free(converted);

    return std.fs.path.join(alloc, &.{ "testdata", "vectorsets", converted });
}

fn runHBCCase(dataset_path: []const u8, case: RecallCase, bulk_algo: ?hbc.BulkBuildAlgo, truth_cache: *TruthCache) !MetricStats {
    const alloc = std.testing.allocator;

    var loaded = try loadVectorSet(alloc, dataset_path);
    defer loaded.deinit(alloc);

    const split = try splitDataset(loaded.asSet(), case.count);

    return .{
        .euclidean = 100.0 * try calculateHBCRecallMetric(case.dataset, case.count, split, case.top_k, case.randomize, .l2_squared, bulk_algo, truth_cache),
        .inner_product = 100.0 * try calculateHBCRecallMetric(case.dataset, case.count, split, case.top_k, case.randomize, .inner_product, bulk_algo, truth_cache),
        .cosine = 100.0 * try calculateHBCRecallMetric(case.dataset, case.count, split, case.top_k, case.randomize, .cosine, bulk_algo, truth_cache),
    };
}

fn calculateHBCRecallMetric(
    dataset: []const u8,
    count: usize,
    split: SplitDataset,
    top_k: usize,
    randomize: bool,
    metric: vec.DistanceMetric,
    bulk_algo: ?hbc.BulkBuildAlgo,
    truth_cache: *TruthCache,
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

    var memory_storage = lsm_backend.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    var idx = try hbc.HBCIndex.openWithLsmStorage(alloc, "/hbc-recall", .{
        .storage_backend = .lsm,
        .dims = @intCast(data_owned.dims),
        .metric = metric,
        .split_algo = .kmeans,
        .branching_factor = 7 * 24,
        .leaf_size = 7 * 24,
        .search_width = 3 * 7 * 24,
        .epsilon = 1.6,
        .use_quantization = true,
        .rerank_policy = .always,
        .quantizer_seed = 42,
        .use_random_ortho_trans = randomize,
        .no_sync = true,
        .max_cached_nodes = 10_000,
        .max_cached_vectors = 10_000,
    }, memory_storage.storage());
    defer idx.close();

    const data = data_owned.asSet();
    const queries = query_owned.asSet();
    const items = try alloc.alloc(hbc.BatchInsertItem, data.count);
    defer {
        for (items) |item| alloc.free(item.metadata);
        alloc.free(items);
    }
    for (0..data.count) |i| {
        items[i] = .{
            .vector_id = @intCast(i + 1),
            .vector = data.atConst(i),
            .metadata = try std.fmt.allocPrint(alloc, "data_{d}", .{i + 1}),
        };
    }
    if (bulk_algo) |algo| {
        try idx.bulkBuildWithMetadataOptions(items, .{ .algo = algo });
    } else {
        try idx.bulkBuildWithMetadata(items);
    }

    const truth_table = try truth_cache.getOrCompute(dataset, count, top_k, metric, .{
        .data = data,
        .queries = queries,
    });

    var recall_sum: f64 = 0;
    for (0..queries.count) |query_idx| {
        const query = queries.atConst(query_idx);
        var results = try idx.searchWithRequest(.{
            .query = query,
            .k = top_k,
            .load_metadata = false,
        });
        defer results.deinit();

        const hits = results.getHits();
        const prediction = try alloc.alloc(usize, @min(top_k, hits.len));
        defer alloc.free(prediction);
        for (prediction, 0..) |*slot, i| {
            slot.* = @intCast(hits[i].vector_id - 1);
        }

        recall_sum += calculateRecall(prediction, truth_table.row(query_idx));
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

    return .{
        .data = .{
            .dims = set.dims,
            .count = data_count,
            .data = set.data[0 .. data_count * set.dims],
        },
        .queries = .{
            .dims = set.dims,
            .count = count - data_count,
            .data = set.data[data_count * set.dims .. count * set.dims],
        },
    };
}

fn normalizeSetInPlace(set: vec.Set) void {
    for (0..set.count) |i| {
        _ = vec.normalize(set.at(i));
    }
}

fn calculateTruthInto(
    truth: []usize,
    metric: vec.DistanceMetric,
    query: []const f32,
    data: vec.Set,
) !void {
    const top_k = truth.len;
    if (top_k == 0 or top_k > data.count) return error.InvalidTopK;

    var best_distances_buf: [128]f32 = undefined;
    if (top_k > best_distances_buf.len) return error.InvalidTopK;
    const best_distances = best_distances_buf[0..top_k];

    @memset(truth, std.math.maxInt(usize));
    @memset(best_distances, std.math.inf(f32));

    for (0..data.count) |i| {
        const distance = vec.distance(query, data.atConst(i), metric);
        var pos: usize = 0;
        while (pos < top_k) : (pos += 1) {
            if (!betterTruthCandidate(distance, i, best_distances[pos], truth[pos])) continue;

            var shift = top_k - 1;
            while (shift > pos) : (shift -= 1) {
                best_distances[shift] = best_distances[shift - 1];
                truth[shift] = truth[shift - 1];
            }
            best_distances[pos] = distance;
            truth[pos] = i;
            break;
        }
    }
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

fn betterTruthCandidate(candidate_distance: f32, candidate_offset: usize, current_distance: f32, current_offset: usize) bool {
    if (candidate_distance != current_distance) return candidate_distance < current_distance;
    return candidate_offset < current_offset;
}
