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
// limitations under the License.

const std = @import("std");
const antfly = @import("antfly-zig");
const hdc = antfly.hdc;

const Config = struct {
    input_dimensions: usize = 768,
    hdc_dimensions: usize = 10_000,
    projection_iterations: usize = 20,
    structural_iterations: usize = 1_000,
    associations: usize = 8,
    recall_vectors: usize = 256,
    recall_queries: usize = 20,
    recall_k: usize = 10,
    recall_candidates: usize = 40,
    semantic_weight: f32 = 8,
    seed: u64 = 13,
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();
    const config = try parseArgs(init.minimal.args);

    const input_dimensions = std.math.cast(u32, config.input_dimensions) orelse return error.InvalidArgument;
    const output_dimensions = std.math.cast(u32, config.hdc_dimensions) orelse return error.InvalidArgument;
    const projection = hdc.Projection{
        .input_dimensions = input_dimensions,
        .output_dimensions = output_dimensions,
        .seed = config.seed,
    };
    try projection.validate();

    const encoder = try hdc.Encoder.init(.{
        .dimensions = output_dimensions,
        .atomic_seed = config.seed,
    });

    const embedding = try alloc.alloc(f32, config.input_dimensions);
    defer alloc.free(embedding);
    var rng = std.Random.DefaultPrng.init(config.seed);
    const random = rng.random();
    for (embedding) |*component| component.* = random.float(f32) * 2 - 1;

    const projected = try alloc.alloc(f32, config.hdc_dimensions);
    defer alloc.free(projected);
    const structural = try alloc.alloc(f32, config.hdc_dimensions);
    defer alloc.free(structural);
    const scratch = try alloc.alloc(f32, config.hdc_dimensions);
    defer alloc.free(scratch);

    try projection.project(embedding, projected);
    std.mem.doNotOptimizeAway(projected.ptr);

    const projection_start = antfly.platform_time.monotonicNs();
    for (0..config.projection_iterations) |_| {
        try projection.project(embedding, projected);
        std.mem.doNotOptimizeAway(projected.ptr);
    }
    const projection_elapsed = antfly.platform_time.monotonicNs() - projection_start;

    const structural_start = antfly.platform_time.monotonicNs();
    for (0..config.structural_iterations) |iteration| {
        @memset(structural, 0);
        for (0..config.associations) |association| {
            var path_buf: [32]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "field_{d}", .{association});
            var value_buf: [64]u8 = undefined;
            const value = try std.fmt.bufPrint(&value_buf, "value_{d}_{d}", .{ association, iteration % 31 });
            try encoder.addAssociation(
                structural,
                scratch,
                path,
                .{ .kind = .string, .bytes = value },
            );
        }
        std.mem.doNotOptimizeAway(structural.ptr);
    }
    const structural_elapsed = antfly.platform_time.monotonicNs() - structural_start;

    const checksum_start = antfly.platform_time.monotonicNs();
    const projection_checksum = projection.checksum();
    std.mem.doNotOptimizeAway(&projection_checksum);
    const checksum_elapsed = antfly.platform_time.monotonicNs() - checksum_start;

    const projection_calls: f64 = @floatFromInt(config.projection_iterations);
    const structural_calls: f64 = @floatFromInt(config.structural_iterations);
    const association_calls: f64 = @floatFromInt(config.structural_iterations * config.associations);
    const projection_ns_per_call = @as(f64, @floatFromInt(projection_elapsed)) / projection_calls;
    const structural_ns_per_document = @as(f64, @floatFromInt(structural_elapsed)) / structural_calls;
    const structural_ns_per_association = @as(f64, @floatFromInt(structural_elapsed)) / association_calls;

    std.debug.print(
        "hdc_bench input_dims={d} hdc_dims={d} projection_iterations={d} structural_iterations={d} associations={d}\n",
        .{
            config.input_dimensions,
            config.hdc_dimensions,
            config.projection_iterations,
            config.structural_iterations,
            config.associations,
        },
    );
    std.debug.print(
        "projection avg_ms={d:.3} coordinates_per_second={d:.0} checksum_ms={d:.3}\n",
        .{
            projection_ns_per_call / std.time.ns_per_ms,
            @as(f64, @floatFromInt(config.input_dimensions * config.hdc_dimensions)) /
                (projection_ns_per_call / std.time.ns_per_s),
            @as(f64, @floatFromInt(checksum_elapsed)) / std.time.ns_per_ms,
        },
    );
    std.debug.print(
        "structural avg_document_us={d:.3} avg_association_us={d:.3}\n",
        .{
            structural_ns_per_document / std.time.ns_per_us,
            structural_ns_per_association / std.time.ns_per_us,
        },
    );
    std.debug.print(
        "resources authoritative_bytes={d} projection_working_bytes={d} logical_matrix_bytes={d}\n",
        .{
            try encoder.identity.authoritativeBytesPerVector(),
            try projection.workingBytes(),
            try projection.matrixBytes(),
        },
    );
    try runRecallBenchmark(alloc, config, projection, encoder, random);
}

const RankedDistance = struct {
    index: usize,
    distance: f32,
};

fn runRecallBenchmark(
    alloc: std.mem.Allocator,
    config: Config,
    projection: hdc.Projection,
    encoder: hdc.Encoder,
    random: std.Random,
) !void {
    const dims = config.hdc_dimensions;
    const query_count = @min(config.recall_queries, config.recall_vectors);
    const k = @min(config.recall_k, config.recall_vectors);
    const candidate_count = @min(config.recall_candidates, config.recall_vectors);
    const source = try alloc.alloc(f32, config.input_dimensions);
    defer alloc.free(source);
    const vectors = try alloc.alloc(f32, config.recall_vectors * dims);
    defer alloc.free(vectors);
    const queries = try alloc.alloc(f32, query_count * dims);
    defer alloc.free(queries);
    const projected = try alloc.alloc(f32, dims);
    defer alloc.free(projected);
    const structural = try alloc.alloc(f32, dims);
    defer alloc.free(structural);
    const scratch = try alloc.alloc(f32, dims);
    defer alloc.free(scratch);
    const centroid = try alloc.alloc(f32, dims);
    defer alloc.free(centroid);
    @memset(centroid, 0);

    const encode_start = antfly.platform_time.monotonicNs();
    for (0..config.recall_vectors) |document_index| {
        for (source) |*component| component.* = random.float(f32) * 2 - 1;
        try projection.project(source, projected);
        if (document_index < query_count) {
            const query = queries[document_index * dims ..][0..dims];
            @memcpy(query, projected);
            _ = antfly.vector.normalize(query);
        }

        @memset(structural, 0);
        for (0..config.associations) |association| {
            var path_buf: [32]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "field_{d}", .{association});
            var value_buf: [64]u8 = undefined;
            const value = try std.fmt.bufPrint(
                &value_buf,
                "cohort_{d}_{d}",
                .{ association, (document_index + association * 17) % 31 },
            );
            try encoder.addAssociation(
                structural,
                scratch,
                path,
                .{ .kind = .string, .bytes = value },
            );
        }
        const destination = vectors[document_index * dims ..][0..dims];
        for (destination, structural, projected) |*out, structured, semantic| {
            out.* = structured + config.semantic_weight * semantic;
        }
        _ = antfly.vector.normalize(destination);
        for (centroid, destination) |*mean, value| mean.* += value;
    }
    antfly.vector.scale(1.0 / @as(f32, @floatFromInt(config.recall_vectors)), centroid);
    const encode_elapsed = antfly.platform_time.monotonicNs() - encode_start;

    var quantizer = try antfly.quantizer.RaBitQuantizer.init(
        alloc,
        dims,
        config.seed,
        .cosine,
    );
    defer quantizer.deinit();
    const quantize_start = antfly.platform_time.monotonicNs();
    var quantized = try quantizer.quantize(centroid, vectors, config.recall_vectors);
    defer quantized.deinit(alloc);
    const quantize_elapsed = antfly.platform_time.monotonicNs() - quantize_start;

    const exact_ranked = try alloc.alloc(RankedDistance, config.recall_vectors);
    defer alloc.free(exact_ranked);
    const approximate_ranked = try alloc.alloc(RankedDistance, config.recall_vectors);
    defer alloc.free(approximate_ranked);
    const reranked = try alloc.alloc(RankedDistance, candidate_count);
    defer alloc.free(reranked);
    const approximate_distances = try alloc.alloc(f32, config.recall_vectors);
    defer alloc.free(approximate_distances);
    const error_bounds = try alloc.alloc(f32, config.recall_vectors);
    defer alloc.free(error_bounds);
    var estimate_scratch = try antfly.quantizer.RaBitQuantizer.EstimateScratch.init(alloc, dims);
    defer estimate_scratch.deinit(alloc);

    var exact_elapsed: u64 = 0;
    var approximate_elapsed: u64 = 0;
    var rerank_elapsed: u64 = 0;
    var approximate_recalled: usize = 0;
    var reranked_recalled: usize = 0;
    for (0..query_count) |query_index| {
        const query = queries[query_index * dims ..][0..dims];
        const exact_start = antfly.platform_time.monotonicNs();
        for (0..config.recall_vectors) |document_index| {
            const document = vectors[document_index * dims ..][0..dims];
            exact_ranked[document_index] = .{
                .index = document_index,
                .distance = 1 - antfly.vector.dot(query, document),
            };
        }
        std.mem.sort(RankedDistance, exact_ranked, {}, rankedDistanceLessThan);
        exact_elapsed += antfly.platform_time.monotonicNs() - exact_start;

        const approximate_start = antfly.platform_time.monotonicNs();
        try quantizer.estimateDistancesWithScratch(
            &quantized,
            query,
            approximate_distances,
            error_bounds,
            &estimate_scratch,
        );
        for (approximate_distances, 0..) |distance, document_index| {
            approximate_ranked[document_index] = .{
                .index = document_index,
                .distance = distance,
            };
        }
        std.mem.sort(RankedDistance, approximate_ranked, {}, rankedDistanceLessThan);
        approximate_elapsed += antfly.platform_time.monotonicNs() - approximate_start;

        for (approximate_ranked[0..k]) |candidate| {
            for (exact_ranked[0..k]) |truth| {
                if (candidate.index == truth.index) {
                    approximate_recalled += 1;
                    break;
                }
            }
        }

        const rerank_start = antfly.platform_time.monotonicNs();
        for (approximate_ranked[0..candidate_count], 0..) |candidate, candidate_index| {
            const document = vectors[candidate.index * dims ..][0..dims];
            reranked[candidate_index] = .{
                .index = candidate.index,
                .distance = 1 - antfly.vector.dot(query, document),
            };
        }
        std.mem.sort(RankedDistance, reranked, {}, rankedDistanceLessThan);
        rerank_elapsed += antfly.platform_time.monotonicNs() - rerank_start;
        for (reranked[0..k]) |candidate| {
            for (exact_ranked[0..k]) |truth| {
                if (candidate.index == truth.index) {
                    reranked_recalled += 1;
                    break;
                }
            }
        }
    }

    const quantized_bytes =
        quantized.centroid.len * @sizeOf(f32) +
        quantized.codes.data.len * @sizeOf(u64) +
        quantized.code_counts.len * @sizeOf(u32) +
        quantized.centroid_distances.len * @sizeOf(f32) +
        quantized.quantized_dot_products.len * @sizeOf(f32) +
        quantized.centroid_dot_products.len * @sizeOf(f32);
    std.debug.print(
        "rabitq vectors={d} queries={d} k={d} candidates={d} recall_at_k={d:.4} reranked_recall_at_k={d:.4} encode_ms={d:.3} quantize_ms={d:.3} exact_query_ms={d:.3} approximate_query_ms={d:.3} rerank_query_ms={d:.3}\n",
        .{
            config.recall_vectors,
            query_count,
            k,
            candidate_count,
            @as(f64, @floatFromInt(approximate_recalled)) / @as(f64, @floatFromInt(query_count * k)),
            @as(f64, @floatFromInt(reranked_recalled)) / @as(f64, @floatFromInt(query_count * k)),
            @as(f64, @floatFromInt(encode_elapsed)) / std.time.ns_per_ms,
            @as(f64, @floatFromInt(quantize_elapsed)) / std.time.ns_per_ms,
            @as(f64, @floatFromInt(exact_elapsed)) / @as(f64, @floatFromInt(query_count)) / std.time.ns_per_ms,
            @as(f64, @floatFromInt(approximate_elapsed)) / @as(f64, @floatFromInt(query_count)) / std.time.ns_per_ms,
            @as(f64, @floatFromInt(rerank_elapsed)) / @as(f64, @floatFromInt(query_count)) / std.time.ns_per_ms,
        },
    );
    std.debug.print(
        "rabitq resources exact_vector_bytes={d} quantized_set_bytes={d} compression_ratio={d:.3}\n",
        .{
            vectors.len * @sizeOf(f32),
            quantized_bytes,
            @as(f64, @floatFromInt(vectors.len * @sizeOf(f32))) /
                @as(f64, @floatFromInt(quantized_bytes)),
        },
    );
}

fn rankedDistanceLessThan(_: void, left: RankedDistance, right: RankedDistance) bool {
    if (left.distance == right.distance) return left.index < right.index;
    return left.distance < right.distance;
}

fn parseArgs(args_in: std.process.Args) !Config {
    var config = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |argument| {
        if (std.mem.eql(u8, argument, "--input-dims")) {
            config.input_dimensions = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--hdc-dims")) {
            config.hdc_dimensions = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--projection-iterations")) {
            config.projection_iterations = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--structural-iterations")) {
            config.structural_iterations = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--associations")) {
            config.associations = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--recall-vectors")) {
            config.recall_vectors = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--recall-queries")) {
            config.recall_queries = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--recall-k")) {
            config.recall_k = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--recall-candidates")) {
            config.recall_candidates = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, argument, "--semantic-weight")) {
            const raw = args.next() orelse return error.InvalidArgument;
            config.semantic_weight = try std.fmt.parseFloat(f32, raw);
        } else if (std.mem.eql(u8, argument, "--seed")) {
            const raw = args.next() orelse return error.InvalidArgument;
            config.seed = try std.fmt.parseInt(u64, raw, 10);
        } else {
            return error.InvalidArgument;
        }
    }
    if (config.input_dimensions == 0 or
        config.hdc_dimensions == 0 or
        config.projection_iterations == 0 or
        config.structural_iterations == 0 or
        config.associations == 0 or
        config.recall_vectors == 0 or
        config.recall_queries == 0 or
        config.recall_k == 0 or
        config.recall_candidates < config.recall_k or
        !std.math.isFinite(config.semantic_weight) or
        config.semantic_weight < 0)
    {
        return error.InvalidArgument;
    }
    return config;
}

fn parseNextUsize(args: *std.process.Args.Iterator) !usize {
    const raw = args.next() orelse return error.InvalidArgument;
    return try std.fmt.parseInt(usize, raw, 10);
}
