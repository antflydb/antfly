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
    structured_fusion_weight: f32 = 0.25,
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
    try runControlledQualityExperiment(alloc, config, projection);
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

const quality_topics: usize = 12;
const quality_regions: usize = 6;
const quality_features: usize = 4;

const QualityDocument = struct {
    topic: usize,
    region: usize,
    feature: usize,
};

const QualityQueryKind = enum {
    hard_match,
    soft_fallback,
};

const QualityQuery = struct {
    topic: usize,
    region: usize,
    feature: usize,
    kind: QualityQueryKind,
};

const RankedSimilarity = struct {
    index: usize,
    similarity: f32,
};

const QualityMetrics = struct {
    queries: usize = 0,
    top1: usize = 0,
    hit10: usize = 0,
    reciprocal_rank_sum: f64 = 0,
    score_ns: u64 = 0,

    fn observe(self: *QualityMetrics, rank: ?usize, elapsed_ns: u64) void {
        self.queries += 1;
        self.score_ns += elapsed_ns;
        if (rank) |value| {
            if (value == 0) self.top1 += 1;
            if (value < 10) self.hit10 += 1;
            self.reciprocal_rank_sum += 1.0 / @as(f64, @floatFromInt(value + 1));
        }
    }

    fn print(self: QualityMetrics, label: []const u8, kind: QualityQueryKind) void {
        const denominator: f64 = @floatFromInt(self.queries);
        std.debug.print(
            "quality kind={s} method={s} queries={d} top1={d:.4} mrr={d:.4} hit10={d:.4} score_ms_per_query={d:.4}\n",
            .{
                @tagName(kind),
                label,
                self.queries,
                @as(f64, @floatFromInt(self.top1)) / denominator,
                self.reciprocal_rank_sum / denominator,
                @as(f64, @floatFromInt(self.hit10)) / denominator,
                @as(f64, @floatFromInt(self.score_ns)) / denominator / std.time.ns_per_ms,
            },
        );
    }
};

const QualityMethodMetrics = struct {
    embedding: QualityMetrics = .{},
    embedding_exact_filter: QualityMetrics = .{},
    embedding_structured_fusion: QualityMetrics = .{},
    semantic_hdc: QualityMetrics = .{},
    complete_hdc_text: QualityMetrics = .{},
    complete_hdc_structured: QualityMetrics = .{},

    fn print(self: QualityMethodMetrics, kind: QualityQueryKind) void {
        self.embedding.print("embedding", kind);
        self.embedding_exact_filter.print("embedding_exact_filter", kind);
        self.embedding_structured_fusion.print("embedding_structured_fusion", kind);
        self.semantic_hdc.print("semantic_hdc", kind);
        self.complete_hdc_text.print("complete_hdc_text", kind);
        self.complete_hdc_structured.print("complete_hdc_structured", kind);
    }
};

fn runControlledQualityExperiment(
    alloc: std.mem.Allocator,
    config: Config,
    projection: hdc.Projection,
) !void {
    const max_documents = quality_topics * quality_regions * quality_features;
    const documents = try alloc.alloc(QualityDocument, max_documents);
    defer alloc.free(documents);
    const semantic_vectors = try alloc.alloc(f32, max_documents * config.input_dimensions);
    defer alloc.free(semantic_vectors);
    const semantic_hypervectors = try alloc.alloc(f32, max_documents * config.hdc_dimensions);
    defer alloc.free(semantic_hypervectors);
    const complete_hypervectors = try alloc.alloc(f32, max_documents * config.hdc_dimensions);
    defer alloc.free(complete_hypervectors);

    const topic_centers = try alloc.alloc(f32, quality_topics * config.input_dimensions);
    defer alloc.free(topic_centers);
    const group_count = quality_topics / 3;
    const group_centers = try alloc.alloc(f32, group_count * config.input_dimensions);
    defer alloc.free(group_centers);
    var center_rng = std.Random.DefaultPrng.init(config.seed ^ 0x7175_616c_6974_7921);
    for (0..quality_topics) |topic| {
        fillUnitRandom(center_rng.random(), topic_centers[topic * config.input_dimensions ..][0..config.input_dimensions]);
    }
    for (0..group_count) |group| {
        fillUnitRandom(center_rng.random(), group_centers[group * config.input_dimensions ..][0..config.input_dimensions]);
    }

    const config_json = try std.fmt.allocPrint(
        alloc,
        "{{\"dimensions\":{d},\"seed\":{d},\"projection_seed\":{d},\"semantic_weight\":{d},\"structural_paths\":[\"feature\",\"region\"]}}",
        .{ config.hdc_dimensions, config.seed, config.seed, config.semantic_weight },
    );
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
    defer parsed_config.deinit();
    var user_config = try hdc.UserConfig.parseValue(alloc, parsed_config.value);
    defer user_config.deinit();

    const projected = try alloc.alloc(f32, config.hdc_dimensions);
    defer alloc.free(projected);
    const noise = try alloc.alloc(f32, config.input_dimensions);
    defer alloc.free(noise);

    var document_count: usize = 0;
    for (0..quality_topics) |topic| {
        for (0..quality_regions) |region| {
            for (0..quality_features) |feature| {
                if (qualityCombinationMissing(topic, region, feature)) continue;
                documents[document_count] = .{
                    .topic = topic,
                    .region = region,
                    .feature = feature,
                };
                const semantic = semantic_vectors[document_count * config.input_dimensions ..][0..config.input_dimensions];
                var document_rng = std.Random.DefaultPrng.init(qualityCombinationSeed(config.seed, topic, region, feature));
                fillUnitRandom(document_rng.random(), noise);
                combineSemanticVector(
                    semantic,
                    topic_centers[topic * config.input_dimensions ..][0..config.input_dimensions],
                    group_centers[(topic / 3) * config.input_dimensions ..][0..config.input_dimensions],
                    noise,
                );

                try projection.project(semantic, projected);
                const semantic_hv = semantic_hypervectors[document_count * config.hdc_dimensions ..][0..config.hdc_dimensions];
                @memcpy(semantic_hv, projected);
                _ = antfly.vector.normalize(semantic_hv);

                var document_json_buffer: [96]u8 = undefined;
                const document_json = try std.fmt.bufPrint(
                    &document_json_buffer,
                    "{{\"region\":\"region_{d}\",\"feature\":\"feature_{d}\"}}",
                    .{ region, feature },
                );
                const complete = try hdc.composeJsonDocument(alloc, user_config, document_json, projected);
                defer alloc.free(complete);
                const complete_hv = complete_hypervectors[document_count * config.hdc_dimensions ..][0..config.hdc_dimensions];
                @memcpy(complete_hv, complete);
                _ = antfly.vector.normalize(complete_hv);
                document_count += 1;
            }
        }
    }

    const ranked = try alloc.alloc(RankedSimilarity, document_count);
    defer alloc.free(ranked);
    const query_semantic = try alloc.alloc(f32, config.input_dimensions);
    defer alloc.free(query_semantic);
    const query_projected = try alloc.alloc(f32, config.hdc_dimensions);
    defer alloc.free(query_projected);
    const query_noise = try alloc.alloc(f32, config.input_dimensions);
    defer alloc.free(query_noise);

    var hard_metrics = QualityMethodMetrics{};
    var soft_metrics = QualityMethodMetrics{};
    var hard_queries: usize = 0;
    var soft_queries: usize = 0;

    for (0..quality_topics) |topic| {
        for (0..quality_regions) |region| {
            for (0..quality_features) |feature| {
                const kind: QualityQueryKind = if (qualityCombinationMissing(topic, region, feature))
                    .soft_fallback
                else
                    .hard_match;
                const query = QualityQuery{
                    .topic = topic,
                    .region = region,
                    .feature = feature,
                    .kind = kind,
                };
                var query_rng = std.Random.DefaultPrng.init(
                    qualityCombinationSeed(config.seed ^ 0x5155_4552_5921, topic, region, feature),
                );
                fillUnitRandom(query_rng.random(), query_noise);
                combineSemanticVector(
                    query_semantic,
                    topic_centers[topic * config.input_dimensions ..][0..config.input_dimensions],
                    group_centers[(topic / 3) * config.input_dimensions ..][0..config.input_dimensions],
                    query_noise,
                );
                try projection.project(query_semantic, query_projected);
                _ = antfly.vector.normalize(query_projected);

                var associations_buffer: [96]u8 = undefined;
                const associations_json = try std.fmt.bufPrint(
                    &associations_buffer,
                    "{{\"region\":\"region_{d}\",\"feature\":\"feature_{d}\"}}",
                    .{ region, feature },
                );
                const structured_query = try hdc.composeJsonQuery(
                    alloc,
                    user_config,
                    associations_json,
                    query_projected,
                );
                defer alloc.free(structured_query);
                _ = antfly.vector.normalize(structured_query);

                const metrics = if (kind == .hard_match) &hard_metrics else &soft_metrics;
                if (kind == .hard_match) hard_queries += 1 else soft_queries += 1;
                try evaluateQualityMethod(
                    &metrics.embedding,
                    ranked,
                    documents[0..document_count],
                    semantic_vectors[0 .. document_count * config.input_dimensions],
                    query_semantic,
                    query,
                    false,
                );
                try evaluateQualityMethod(
                    &metrics.embedding_exact_filter,
                    ranked,
                    documents[0..document_count],
                    semantic_vectors[0 .. document_count * config.input_dimensions],
                    query_semantic,
                    query,
                    true,
                );
                try evaluateStructuredFusion(
                    &metrics.embedding_structured_fusion,
                    ranked,
                    documents[0..document_count],
                    semantic_vectors[0 .. document_count * config.input_dimensions],
                    query_semantic,
                    query,
                    config.structured_fusion_weight,
                );
                try evaluateQualityMethod(
                    &metrics.semantic_hdc,
                    ranked,
                    documents[0..document_count],
                    semantic_hypervectors[0 .. document_count * config.hdc_dimensions],
                    query_projected,
                    query,
                    false,
                );
                try evaluateQualityMethod(
                    &metrics.complete_hdc_text,
                    ranked,
                    documents[0..document_count],
                    complete_hypervectors[0 .. document_count * config.hdc_dimensions],
                    query_projected,
                    query,
                    false,
                );
                try evaluateQualityMethod(
                    &metrics.complete_hdc_structured,
                    ranked,
                    documents[0..document_count],
                    complete_hypervectors[0 .. document_count * config.hdc_dimensions],
                    structured_query,
                    query,
                    false,
                );
            }
        }
    }

    std.debug.print(
        "quality_workload documents={d} hard_queries={d} soft_fallback_queries={d} topics={d} regions={d} features={d} semantic_weight={d:.3} structured_fusion_weight={d:.3}\n",
        .{
            document_count,
            hard_queries,
            soft_queries,
            quality_topics,
            quality_regions,
            quality_features,
            config.semantic_weight,
            config.structured_fusion_weight,
        },
    );
    hard_metrics.print(.hard_match);
    soft_metrics.print(.soft_fallback);
    std.debug.print(
        "quality_note hard_match treats region and feature as exact constraints; soft_fallback asks for an absent combination and grades same-topic/same-region alternatives\n",
        .{},
    );
    std.debug.print(
        "quality_note each relevant seed owns one exact graph answer, so hard_match top1 is also controlled graph-answer accuracy; graph traversal itself is not timed here\n",
        .{},
    );
}

fn evaluateQualityMethod(
    metrics: *QualityMetrics,
    ranked: []RankedSimilarity,
    documents: []const QualityDocument,
    vectors: []const f32,
    query_vector: []const f32,
    query: QualityQuery,
    exact_filter: bool,
) !void {
    if (documents.len == 0 or vectors.len % documents.len != 0) return error.InvalidArgument;
    const dimensions = vectors.len / documents.len;
    if (query_vector.len != dimensions) return error.InvalidArgument;

    const started_at = antfly.platform_time.monotonicNs();
    var candidate_count: usize = 0;
    for (documents, 0..) |document, document_index| {
        if (exact_filter and
            (document.region != query.region or document.feature != query.feature))
        {
            continue;
        }
        ranked[candidate_count] = .{
            .index = document_index,
            .similarity = antfly.vector.dot(
                query_vector,
                vectors[document_index * dimensions ..][0..dimensions],
            ),
        };
        candidate_count += 1;
    }
    std.mem.sort(RankedSimilarity, ranked[0..candidate_count], {}, rankedSimilarityGreaterThan);
    const elapsed = antfly.platform_time.monotonicNs() - started_at;

    var first_relevant_rank: ?usize = null;
    for (ranked[0..candidate_count], 0..) |candidate, rank| {
        if (qualityDocumentRelevant(documents[candidate.index], query)) {
            first_relevant_rank = rank;
            break;
        }
    }
    metrics.observe(first_relevant_rank, elapsed);
}

fn evaluateStructuredFusion(
    metrics: *QualityMetrics,
    ranked: []RankedSimilarity,
    documents: []const QualityDocument,
    vectors: []const f32,
    query_vector: []const f32,
    query: QualityQuery,
    structured_fusion_weight: f32,
) !void {
    if (documents.len == 0 or vectors.len % documents.len != 0) return error.InvalidArgument;
    const dimensions = vectors.len / documents.len;
    if (query_vector.len != dimensions) return error.InvalidArgument;

    const started_at = antfly.platform_time.monotonicNs();
    for (documents, 0..) |document, document_index| {
        const association_matches: u2 =
            @as(u2, @intFromBool(document.region == query.region)) +
            @as(u2, @intFromBool(document.feature == query.feature));
        ranked[document_index] = .{
            .index = document_index,
            .similarity = antfly.vector.dot(
                query_vector,
                vectors[document_index * dimensions ..][0..dimensions],
            ) + structured_fusion_weight * @as(f32, @floatFromInt(association_matches)),
        };
    }
    std.mem.sort(RankedSimilarity, ranked[0..documents.len], {}, rankedSimilarityGreaterThan);
    const elapsed = antfly.platform_time.monotonicNs() - started_at;

    var first_relevant_rank: ?usize = null;
    for (ranked[0..documents.len], 0..) |candidate, rank| {
        if (qualityDocumentRelevant(documents[candidate.index], query)) {
            first_relevant_rank = rank;
            break;
        }
    }
    metrics.observe(first_relevant_rank, elapsed);
}

fn qualityDocumentRelevant(document: QualityDocument, query: QualityQuery) bool {
    return switch (query.kind) {
        .hard_match => document.topic == query.topic and
            document.region == query.region and
            document.feature == query.feature,
        .soft_fallback => document.topic == query.topic and document.region == query.region,
    };
}

fn qualityCombinationMissing(topic: usize, region: usize, feature: usize) bool {
    return (topic * 17 + region * 7 + feature * 3) % 11 == 0;
}

fn qualityCombinationSeed(seed: u64, topic: usize, region: usize, feature: usize) u64 {
    return seed ^
        (@as(u64, @intCast(topic)) *% 0x9e37_79b9_7f4a_7c15) ^
        (@as(u64, @intCast(region)) *% 0xbf58_476d_1ce4_e5b9) ^
        (@as(u64, @intCast(feature)) *% 0x94d0_49bb_1331_11eb);
}

fn fillUnitRandom(random: std.Random, vector: []f32) void {
    for (vector) |*component| component.* = random.float(f32) * 2 - 1;
    _ = antfly.vector.normalize(vector);
}

fn combineSemanticVector(
    out: []f32,
    topic: []const f32,
    group: []const f32,
    noise: []const f32,
) void {
    std.debug.assert(out.len == topic.len and out.len == group.len and out.len == noise.len);
    for (out, topic, group, noise) |*component, topic_value, group_value, noise_value| {
        component.* = 0.9 * topic_value + 0.35 * group_value + 0.15 * noise_value;
    }
    _ = antfly.vector.normalize(out);
}

fn rankedSimilarityGreaterThan(_: void, left: RankedSimilarity, right: RankedSimilarity) bool {
    if (left.similarity == right.similarity) return left.index < right.index;
    return left.similarity > right.similarity;
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
        } else if (std.mem.eql(u8, argument, "--structured-fusion-weight")) {
            const raw = args.next() orelse return error.InvalidArgument;
            config.structured_fusion_weight = try std.fmt.parseFloat(f32, raw);
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
        config.semantic_weight <= 0 or
        !std.math.isFinite(config.structured_fusion_weight) or
        config.structured_fusion_weight < 0)
    {
        return error.InvalidArgument;
    }
    return config;
}

fn parseNextUsize(args: *std.process.Args.Iterator) !usize {
    const raw = args.next() orelse return error.InvalidArgument;
    return try std.fmt.parseInt(usize, raw, 10);
}
