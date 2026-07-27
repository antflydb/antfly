// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const antfly = @import("antfly-zig");
const hdc = antfly.hdc;

const fixture_magic = "AFHDCW01";
const fixture_version: u32 = 1;
const fixture_header_bytes: usize = 32;
const qrel_bytes: usize = 12;
const top_k: usize = 10;

const Config = struct {
    fixture_path: []const u8,
    hdc_dimensions: usize = 10_000,
    semantic_weights: []const f32 = &.{ 4, 8, 12, 16, 24, 32 },
    fusion_weight: f32 = 0.25,
    seed: u64 = 13,
    baseline_only: bool = false,
    ann_semantic_weight: ?f32 = null,
    ann_candidates: []const usize = &.{},
};

const Fixture = struct {
    dimensions: usize,
    max_tokens: usize,
    product_count: usize,
    query_count: usize,
    product_embeddings: []const f32,
    query_embeddings: []const f32,
    product_ids: []const u32,
    product_classes: []const u32,
    product_categories: []const u32,
    query_ids: []const u32,
    query_classes: []const u32,
    qrels: []Qrel,
};

const Qrel = struct {
    query_index: u32,
    product_index: u32,
    gain: u8,
};

const Ranked = struct {
    index: usize,
    score: f32,
};

const Split = enum {
    validation,
    holdout,
    all,
};

const Metrics = struct {
    queries: usize = 0,
    ndcg_sum: f64 = 0,
    mrr10_sum: f64 = 0,
    recall10_sum: f64 = 0,
    top1_relevant: usize = 0,
    graph_answer_top1: usize = 0,
    score_ns: u64 = 0,

    fn add(self: *Metrics, other: Metrics) void {
        self.queries += other.queries;
        self.ndcg_sum += other.ndcg_sum;
        self.mrr10_sum += other.mrr10_sum;
        self.recall10_sum += other.recall10_sum;
        self.top1_relevant += other.top1_relevant;
        self.graph_answer_top1 += other.graph_answer_top1;
        self.score_ns += other.score_ns;
    }

    fn print(self: Metrics, method: []const u8, split: Split, dimensions: usize) void {
        if (self.queries == 0) return;
        const denominator: f64 = @floatFromInt(self.queries);
        std.debug.print(
            "wands method={s} split={s} queries={d} dimensions={d} ndcg10={d:.4} mrr10={d:.4} recall10={d:.4} top1_relevant={d:.4} graph_answer_top1={d:.4} score_ms_per_query={d:.4}\n",
            .{
                method,
                @tagName(split),
                self.queries,
                dimensions,
                self.ndcg_sum / denominator,
                self.mrr10_sum / denominator,
                self.recall10_sum / denominator,
                @as(f64, @floatFromInt(self.top1_relevant)) / denominator,
                @as(f64, @floatFromInt(self.graph_answer_top1)) / denominator,
                @as(f64, @floatFromInt(self.score_ns)) / denominator / std.time.ns_per_ms,
            },
        );
    }
};

const Method = enum {
    embedding,
    embedding_exact_filter,
    embedding_structured_fusion,
    semantic_hdc,
    complete_hdc_text,
    complete_hdc_structured,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.page_allocator;
    const config = try parseArgs(alloc, init.minimal.args);
    defer alloc.free(config.semantic_weights);
    defer alloc.free(config.ann_candidates);
    if (!config.baseline_only) {
        if (config.ann_semantic_weight) |ann_weight| {
            var found = false;
            for (config.semantic_weights) |semantic_weight| {
                found = found or
                    @as(u32, @bitCast(ann_weight)) == @as(u32, @bitCast(semantic_weight));
            }
            if (!found) return error.AnnWeightNotEvaluated;
        }
    }

    const raw = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        config.fixture_path,
        alloc,
        .limited(512 * 1024 * 1024),
    );
    defer alloc.free(raw);
    var fixture = try parseFixture(alloc, raw);
    defer alloc.free(fixture.qrels);

    std.debug.print(
        "wands_fixture products={d} queries={d} qrels={d} embedding_dimensions={d} max_tokens={d} hdc_dimensions={d} seed={d} fusion_weight={d:.3}\n",
        .{
            fixture.product_count,
            fixture.query_count,
            fixture.qrels.len,
            fixture.dimensions,
            fixture.max_tokens,
            config.hdc_dimensions,
            config.seed,
            config.fusion_weight,
        },
    );

    std.mem.sort(Qrel, fixture.qrels, {}, qrelLessThan);
    const qrel_offsets = try buildQrelOffsets(alloc, fixture.query_count, fixture.qrels);
    defer alloc.free(qrel_offsets);

    try evaluateMethod(
        alloc,
        fixture,
        qrel_offsets,
        .embedding,
        fixture.product_embeddings,
        fixture.query_embeddings,
        fixture.dimensions,
        null,
        config.fusion_weight,
        "embedding",
    );
    try evaluateMethod(
        alloc,
        fixture,
        qrel_offsets,
        .embedding_exact_filter,
        fixture.product_embeddings,
        fixture.query_embeddings,
        fixture.dimensions,
        null,
        config.fusion_weight,
        "embedding_exact_filter",
    );
    try evaluateMethod(
        alloc,
        fixture,
        qrel_offsets,
        .embedding_structured_fusion,
        fixture.product_embeddings,
        fixture.query_embeddings,
        fixture.dimensions,
        null,
        config.fusion_weight,
        "embedding_structured_fusion",
    );
    if (config.ann_semantic_weight != null) {
        try evaluateAnnMethod(
            alloc,
            fixture,
            qrel_offsets,
            .embedding_structured_fusion,
            fixture.product_embeddings,
            fixture.query_embeddings,
            fixture.dimensions,
            null,
            config.fusion_weight,
            config.ann_candidates,
            config.seed,
            true,
            "embedding_structured_fusion",
        );
        try evaluateAnnMethod(
            alloc,
            fixture,
            qrel_offsets,
            .embedding_structured_fusion,
            fixture.product_embeddings,
            fixture.query_embeddings,
            fixture.dimensions,
            null,
            config.fusion_weight,
            config.ann_candidates,
            config.seed,
            false,
            "embedding_structured_fusion_post_candidates",
        );
    }
    if (config.baseline_only) return;

    const projected_products = try alloc.alloc(
        f32,
        try std.math.mul(usize, fixture.product_count, config.hdc_dimensions),
    );
    defer alloc.free(projected_products);
    const projected_queries = try alloc.alloc(
        f32,
        try std.math.mul(usize, fixture.query_count, config.hdc_dimensions),
    );
    defer alloc.free(projected_queries);

    const projection = hdc.Projection{
        .input_dimensions = @intCast(fixture.dimensions),
        .output_dimensions = @intCast(config.hdc_dimensions),
        .seed = config.seed,
    };
    try projection.validate();
    const projection_started = antfly.platform_time.monotonicNs();
    for (0..fixture.product_count) |index| {
        try projection.project(
            fixture.product_embeddings[index * fixture.dimensions ..][0..fixture.dimensions],
            projected_products[index * config.hdc_dimensions ..][0..config.hdc_dimensions],
        );
    }
    for (0..fixture.query_count) |index| {
        try projection.project(
            fixture.query_embeddings[index * fixture.dimensions ..][0..fixture.dimensions],
            projected_queries[index * config.hdc_dimensions ..][0..config.hdc_dimensions],
        );
    }
    const projection_elapsed = antfly.platform_time.monotonicNs() - projection_started;
    std.debug.print(
        "wands_projection vectors={d} dimensions={d} total_ms={d:.3} ms_per_vector={d:.4} working_bytes={d} logical_matrix_bytes={d}\n",
        .{
            fixture.product_count + fixture.query_count,
            config.hdc_dimensions,
            @as(f64, @floatFromInt(projection_elapsed)) / std.time.ns_per_ms,
            @as(f64, @floatFromInt(projection_elapsed)) /
                @as(f64, @floatFromInt(fixture.product_count + fixture.query_count)) /
                std.time.ns_per_ms,
            try projection.workingBytes(),
            try projection.matrixBytes(),
        },
    );

    try evaluateMethod(
        alloc,
        fixture,
        qrel_offsets,
        .semantic_hdc,
        projected_products,
        projected_queries,
        config.hdc_dimensions,
        null,
        config.fusion_weight,
        "semantic_hdc",
    );

    const class_count = classCount(fixture);
    const structural_classes = try alloc.alloc(
        f32,
        try std.math.mul(usize, class_count, config.hdc_dimensions),
    );
    defer alloc.free(structural_classes);
    try buildStructuralClasses(
        alloc,
        structural_classes,
        class_count,
        config.hdc_dimensions,
        config.seed,
    );

    const complete_products = try alloc.alloc(f32, projected_products.len);
    defer alloc.free(complete_products);
    for (config.semantic_weights) |semantic_weight| {
        const compose_started = antfly.platform_time.monotonicNs();
        for (0..fixture.product_count) |product_index| {
            const projected = projected_products[product_index * config.hdc_dimensions ..][0..config.hdc_dimensions];
            const class_id = fixture.product_classes[product_index];
            const structured = structural_classes[@as(usize, class_id) * config.hdc_dimensions ..][0..config.hdc_dimensions];
            const complete = complete_products[product_index * config.hdc_dimensions ..][0..config.hdc_dimensions];
            for (complete, structured, projected) |*out, structural_coordinate, semantic_coordinate| {
                out.* = structural_coordinate + semantic_weight * semantic_coordinate;
            }
            _ = antfly.vector.normalize(complete);
        }
        const compose_elapsed = antfly.platform_time.monotonicNs() - compose_started;
        std.debug.print(
            "wands_compose semantic_weight={d:.3} vectors={d} total_ms={d:.3} ms_per_vector={d:.4} authoritative_bytes={d}\n",
            .{
                semantic_weight,
                fixture.product_count,
                @as(f64, @floatFromInt(compose_elapsed)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(compose_elapsed)) /
                    @as(f64, @floatFromInt(fixture.product_count)) /
                    std.time.ns_per_ms,
                complete_products.len * @sizeOf(f32),
            },
        );

        var label_buffer: [96]u8 = undefined;
        const text_label = try std.fmt.bufPrint(
            &label_buffer,
            "complete_hdc_text_w{d}",
            .{semantic_weight},
        );
        try evaluateMethod(
            alloc,
            fixture,
            qrel_offsets,
            .complete_hdc_text,
            complete_products,
            projected_queries,
            config.hdc_dimensions,
            null,
            config.fusion_weight,
            text_label,
        );

        var structured_label_buffer: [96]u8 = undefined;
        const structured_label = try std.fmt.bufPrint(
            &structured_label_buffer,
            "complete_hdc_structured_w{d}",
            .{semantic_weight},
        );
        try evaluateMethod(
            alloc,
            fixture,
            qrel_offsets,
            .complete_hdc_structured,
            complete_products,
            projected_queries,
            config.hdc_dimensions,
            .{
                .vectors = structural_classes,
                .count = class_count,
                .semantic_weight = semantic_weight,
            },
            config.fusion_weight,
            structured_label,
        );
        if (config.ann_semantic_weight) |ann_weight| {
            if (@as(u32, @bitCast(ann_weight)) == @as(u32, @bitCast(semantic_weight))) {
                try evaluateAnnMethod(
                    alloc,
                    fixture,
                    qrel_offsets,
                    .complete_hdc_structured,
                    complete_products,
                    projected_queries,
                    config.hdc_dimensions,
                    .{
                        .vectors = structural_classes,
                        .count = class_count,
                        .semantic_weight = semantic_weight,
                    },
                    config.fusion_weight,
                    config.ann_candidates,
                    config.seed,
                    true,
                    structured_label,
                );
            }
        }
    }
}

const StructuredQueries = struct {
    vectors: []const f32,
    count: usize,
    semantic_weight: f32,
};

const AnnMetrics = struct {
    quality: Metrics = .{},
    exact_top10_recall_sum: f64 = 0,
    approximate_ns: u64 = 0,
    rerank_ns: u64 = 0,

    fn add(self: *AnnMetrics, other: AnnMetrics) void {
        self.quality.add(other.quality);
        self.exact_top10_recall_sum += other.exact_top10_recall_sum;
        self.approximate_ns += other.approximate_ns;
        self.rerank_ns += other.rerank_ns;
    }

    fn print(
        self: AnnMetrics,
        method: []const u8,
        split: Split,
        dimensions: usize,
        candidates: usize,
    ) void {
        if (self.quality.queries == 0) return;
        const denominator: f64 = @floatFromInt(self.quality.queries);
        std.debug.print(
            "wands_ann method={s} split={s} queries={d} dimensions={d} candidates={d} ndcg10={d:.4} mrr10={d:.4} recall10={d:.4} top1_relevant={d:.4} graph_answer_top1={d:.4} exact_top10_recall={d:.4} approximate_ms_per_query={d:.4} rerank_ms_per_query={d:.4}\n",
            .{
                method,
                @tagName(split),
                self.quality.queries,
                dimensions,
                candidates,
                self.quality.ndcg_sum / denominator,
                self.quality.mrr10_sum / denominator,
                self.quality.recall10_sum / denominator,
                @as(f64, @floatFromInt(self.quality.top1_relevant)) / denominator,
                @as(f64, @floatFromInt(self.quality.graph_answer_top1)) / denominator,
                self.exact_top10_recall_sum / denominator,
                @as(f64, @floatFromInt(self.approximate_ns)) / denominator / std.time.ns_per_ms,
                @as(f64, @floatFromInt(self.rerank_ns)) / denominator / std.time.ns_per_ms,
            },
        );
    }
};

fn evaluateAnnMethod(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    qrel_offsets: []const usize,
    method: Method,
    product_vectors: []const f32,
    query_vectors: []const f32,
    dimensions: usize,
    structured_queries: ?StructuredQueries,
    fusion_weight: f32,
    candidate_budgets: []const usize,
    seed: u64,
    apply_structure_during_candidate_selection: bool,
    label: []const u8,
) !void {
    if (candidate_budgets.len == 0 or
        product_vectors.len != fixture.product_count * dimensions or
        query_vectors.len != fixture.query_count * dimensions)
    {
        return error.InvalidArgument;
    }
    const maximum_candidates = candidate_budgets[candidate_budgets.len - 1];
    if (maximum_candidates < top_k or maximum_candidates > fixture.product_count) {
        return error.InvalidArgument;
    }

    const centroid = try alloc.alloc(f32, dimensions);
    defer alloc.free(centroid);
    @memset(centroid, 0);
    for (0..fixture.product_count) |product_index| {
        const vector = product_vectors[product_index * dimensions ..][0..dimensions];
        for (centroid, vector) |*mean, value| mean.* += value;
    }
    antfly.vector.scale(1.0 / @as(f32, @floatFromInt(fixture.product_count)), centroid);

    var quantizer = try antfly.quantizer.RaBitQuantizer.init(
        alloc,
        dimensions,
        seed,
        .cosine,
    );
    defer quantizer.deinit();
    const quantize_started = antfly.platform_time.monotonicNs();
    var quantized = try quantizer.quantize(centroid, product_vectors, fixture.product_count);
    defer quantized.deinit(alloc);
    const quantize_elapsed = antfly.platform_time.monotonicNs() - quantize_started;

    const approximate_distances = try alloc.alloc(f32, fixture.product_count);
    defer alloc.free(approximate_distances);
    const error_bounds = try alloc.alloc(f32, fixture.product_count);
    defer alloc.free(error_bounds);
    var estimate_scratch = try antfly.quantizer.RaBitQuantizer.EstimateScratch.init(alloc, dimensions);
    defer estimate_scratch.deinit(alloc);
    const approximate_ranked = try alloc.alloc(Ranked, fixture.product_count);
    defer alloc.free(approximate_ranked);
    const reranked = try alloc.alloc(Ranked, maximum_candidates);
    defer alloc.free(reranked);
    const gains = try alloc.alloc(u8, fixture.product_count);
    defer alloc.free(gains);
    const query_scratch = try alloc.alloc(f32, dimensions);
    defer alloc.free(query_scratch);
    const validation = try alloc.alloc(AnnMetrics, candidate_budgets.len);
    defer alloc.free(validation);
    @memset(validation, .{});
    const holdout = try alloc.alloc(AnnMetrics, candidate_budgets.len);
    defer alloc.free(holdout);
    @memset(holdout, .{});

    for (0..fixture.query_count) |query_index| {
        @memset(gains, 0);
        const qrels = fixture.qrels[qrel_offsets[query_index]..qrel_offsets[query_index + 1]];
        var relevant_count: usize = 0;
        for (qrels) |qrel| {
            gains[qrel.product_index] = @max(gains[qrel.product_index], qrel.gain);
            if (qrel.gain > 0) relevant_count += 1;
        }
        if (relevant_count == 0) continue;

        const raw_query = query_vectors[query_index * dimensions ..][0..dimensions];
        const query = switch (method) {
            .complete_hdc_structured => blk: {
                const structured = structured_queries orelse return error.InvalidArgument;
                const class_id = fixture.query_classes[query_index];
                if (class_id >= structured.count) return error.InvalidFixture;
                const association = structured.vectors[@as(usize, class_id) * dimensions ..][0..dimensions];
                for (query_scratch, association, raw_query) |*out, structural_coordinate, semantic_coordinate| {
                    out.* = structural_coordinate + structured.semantic_weight * semantic_coordinate;
                }
                _ = antfly.vector.normalize(query_scratch);
                break :blk query_scratch;
            },
            else => raw_query,
        };

        var exact_top: [top_k]Ranked = undefined;
        var exact_count: usize = 0;
        for (0..fixture.product_count) |product_index| {
            const score = exactScore(
                method,
                fixture,
                query_index,
                product_index,
                query,
                product_vectors,
                dimensions,
                fusion_weight,
            );
            insertTopK(&exact_top, &exact_count, .{ .index = product_index, .score = score });
        }

        const approximate_started = antfly.platform_time.monotonicNs();
        try quantizer.estimateDistancesWithScratch(
            &quantized,
            query,
            approximate_distances,
            error_bounds,
            &estimate_scratch,
        );
        for (approximate_distances, 0..) |distance, product_index| {
            var score = 1 - distance;
            if (apply_structure_during_candidate_selection and
                method == .embedding_structured_fusion and
                fixture.product_classes[product_index] == fixture.query_classes[query_index])
            {
                score += fusion_weight;
            }
            approximate_ranked[product_index] = .{ .index = product_index, .score = score };
        }
        std.mem.sort(Ranked, approximate_ranked, {}, rankedGreaterThanContext);
        const approximate_elapsed = antfly.platform_time.monotonicNs() - approximate_started;

        for (candidate_budgets, 0..) |candidate_budget, budget_index| {
            const rerank_started = antfly.platform_time.monotonicNs();
            for (approximate_ranked[0..candidate_budget], 0..) |candidate, candidate_index| {
                reranked[candidate_index] = .{
                    .index = candidate.index,
                    .score = exactScore(
                        method,
                        fixture,
                        query_index,
                        candidate.index,
                        query,
                        product_vectors,
                        dimensions,
                        fusion_weight,
                    ),
                };
            }
            std.mem.sort(Ranked, reranked[0..candidate_budget], {}, rankedGreaterThanContext);
            const rerank_elapsed = antfly.platform_time.monotonicNs() - rerank_started;

            const quality = observeQuery(
                fixture,
                qrels,
                gains,
                reranked[0..top_k],
                relevant_count,
                approximate_elapsed + rerank_elapsed,
            );
            var exact_recalled: usize = 0;
            for (reranked[0..top_k]) |candidate| {
                for (exact_top[0..exact_count]) |truth| {
                    if (candidate.index == truth.index) {
                        exact_recalled += 1;
                        break;
                    }
                }
            }
            const observed = AnnMetrics{
                .quality = quality,
                .exact_top10_recall_sum = @as(f64, @floatFromInt(exact_recalled)) / @as(f64, top_k),
                .approximate_ns = approximate_elapsed,
                .rerank_ns = rerank_elapsed,
            };
            if (isValidationQuery(fixture.query_ids[query_index])) {
                validation[budget_index].add(observed);
            } else {
                holdout[budget_index].add(observed);
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
        "wands_ann_index method={s} dimensions={d} vectors={d} quantize_ms={d:.3} authoritative_bytes={d} quantized_bytes={d} compression_ratio={d:.3}\n",
        .{
            label,
            dimensions,
            fixture.product_count,
            @as(f64, @floatFromInt(quantize_elapsed)) / std.time.ns_per_ms,
            product_vectors.len * @sizeOf(f32),
            quantized_bytes,
            @as(f64, @floatFromInt(product_vectors.len * @sizeOf(f32))) /
                @as(f64, @floatFromInt(quantized_bytes)),
        },
    );
    for (candidate_budgets, 0..) |candidate_budget, budget_index| {
        validation[budget_index].print(label, .validation, dimensions, candidate_budget);
        holdout[budget_index].print(label, .holdout, dimensions, candidate_budget);
        var all = validation[budget_index];
        all.add(holdout[budget_index]);
        all.print(label, .all, dimensions, candidate_budget);
    }
}

fn exactScore(
    method: Method,
    fixture: Fixture,
    query_index: usize,
    product_index: usize,
    query: []const f32,
    product_vectors: []const f32,
    dimensions: usize,
    fusion_weight: f32,
) f32 {
    var score = antfly.vector.dot(
        query,
        product_vectors[product_index * dimensions ..][0..dimensions],
    );
    if (method == .embedding_structured_fusion and
        fixture.product_classes[product_index] == fixture.query_classes[query_index])
    {
        score += fusion_weight;
    }
    return score;
}

fn rankedGreaterThanContext(_: void, left: Ranked, right: Ranked) bool {
    return rankedGreaterThan(left, right);
}

fn evaluateMethod(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    qrel_offsets: []const usize,
    method: Method,
    product_vectors: []const f32,
    query_vectors: []const f32,
    dimensions: usize,
    structured_queries: ?StructuredQueries,
    fusion_weight: f32,
    label: []const u8,
) !void {
    if (product_vectors.len != fixture.product_count * dimensions or
        query_vectors.len != fixture.query_count * dimensions)
    {
        return error.InvalidFixture;
    }

    const gains = try alloc.alloc(u8, fixture.product_count);
    defer alloc.free(gains);
    const query_scratch = try alloc.alloc(f32, dimensions);
    defer alloc.free(query_scratch);

    var validation = Metrics{};
    var test_metrics = Metrics{};
    for (0..fixture.query_count) |query_index| {
        @memset(gains, 0);
        const qrels = fixture.qrels[qrel_offsets[query_index]..qrel_offsets[query_index + 1]];
        var relevant_count: usize = 0;
        for (qrels) |qrel| {
            gains[qrel.product_index] = @max(gains[qrel.product_index], qrel.gain);
            if (qrel.gain > 0) relevant_count += 1;
        }
        if (relevant_count == 0) continue;

        const raw_query = query_vectors[query_index * dimensions ..][0..dimensions];
        const query = switch (method) {
            .complete_hdc_structured => blk: {
                const structured = structured_queries orelse return error.InvalidArgument;
                const class_id = fixture.query_classes[query_index];
                if (class_id >= structured.count) return error.InvalidFixture;
                const association = structured.vectors[@as(usize, class_id) * dimensions ..][0..dimensions];
                for (query_scratch, association, raw_query) |*out, structural_coordinate, semantic_coordinate| {
                    out.* = structural_coordinate + structured.semantic_weight * semantic_coordinate;
                }
                _ = antfly.vector.normalize(query_scratch);
                break :blk query_scratch;
            },
            .complete_hdc_text => blk: {
                @memcpy(query_scratch, raw_query);
                _ = antfly.vector.normalize(query_scratch);
                break :blk query_scratch;
            },
            else => raw_query,
        };

        const started = antfly.platform_time.monotonicNs();
        var ranked: [top_k]Ranked = undefined;
        var ranked_count: usize = 0;
        for (0..fixture.product_count) |product_index| {
            if (method == .embedding_exact_filter and
                fixture.product_classes[product_index] != fixture.query_classes[query_index])
            {
                continue;
            }
            var score = antfly.vector.dot(
                query,
                product_vectors[product_index * dimensions ..][0..dimensions],
            );
            if (method == .embedding_structured_fusion and
                fixture.product_classes[product_index] == fixture.query_classes[query_index])
            {
                score += fusion_weight;
            }
            insertTopK(&ranked, &ranked_count, .{ .index = product_index, .score = score });
        }
        const elapsed = antfly.platform_time.monotonicNs() - started;
        const observed = observeQuery(
            fixture,
            qrels,
            gains,
            ranked[0..ranked_count],
            relevant_count,
            elapsed,
        );
        if (isValidationQuery(fixture.query_ids[query_index])) {
            validation.add(observed);
        } else {
            test_metrics.add(observed);
        }
    }
    validation.print(label, .validation, dimensions);
    test_metrics.print(label, .holdout, dimensions);
    var all = validation;
    all.add(test_metrics);
    all.print(label, .all, dimensions);
}

fn observeQuery(
    fixture: Fixture,
    qrels: []const Qrel,
    gains: []const u8,
    ranked: []const Ranked,
    relevant_count: usize,
    elapsed: u64,
) Metrics {
    var metrics = Metrics{ .queries = 1, .score_ns = elapsed };
    var dcg: f64 = 0;
    var recalled: usize = 0;
    for (ranked, 0..) |candidate, rank| {
        const gain = gains[candidate.index];
        if (gain > 0) {
            recalled += 1;
            if (rank == 0) metrics.top1_relevant = 1;
            if (metrics.mrr10_sum == 0) {
                metrics.mrr10_sum = 1.0 / @as(f64, @floatFromInt(rank + 1));
            }
        }
        dcg += @as(f64, @floatFromInt(gain)) / @log2(@as(f64, @floatFromInt(rank + 2)));
    }
    metrics.recall10_sum =
        @as(f64, @floatFromInt(recalled)) / @as(f64, @floatFromInt(relevant_count));
    metrics.ndcg_sum = dcg / idealDcg10(qrels);

    if (ranked.len > 0) {
        const answer_category = fixture.product_categories[ranked[0].index];
        for (qrels) |qrel| {
            if (qrel.gain > 0 and fixture.product_categories[qrel.product_index] == answer_category) {
                metrics.graph_answer_top1 = 1;
                break;
            }
        }
    }
    return metrics;
}

fn idealDcg10(qrels: []const Qrel) f64 {
    var exact_count: usize = 0;
    var partial_count: usize = 0;
    for (qrels) |qrel| switch (qrel.gain) {
        2 => exact_count += 1,
        1 => partial_count += 1,
        else => {},
    };
    var dcg: f64 = 0;
    for (0..top_k) |rank| {
        const gain: usize = if (rank < exact_count)
            2
        else if (rank < exact_count + partial_count)
            1
        else
            0;
        dcg += @as(f64, @floatFromInt(gain)) / @log2(@as(f64, @floatFromInt(rank + 2)));
    }
    return dcg;
}

fn insertTopK(ranked: *[top_k]Ranked, count: *usize, candidate: Ranked) void {
    if (count.* == top_k and !rankedGreaterThan(candidate, ranked[top_k - 1])) return;
    var position = @min(count.*, top_k - 1);
    if (count.* < top_k) count.* += 1;
    while (position > 0 and rankedGreaterThan(candidate, ranked[position - 1])) : (position -= 1) {
        ranked[position] = ranked[position - 1];
    }
    ranked[position] = candidate;
}

fn rankedGreaterThan(left: Ranked, right: Ranked) bool {
    if (left.score == right.score) return left.index < right.index;
    return left.score > right.score;
}

fn isValidationQuery(query_id: u32) bool {
    return query_id % 5 == 0;
}

fn buildStructuralClasses(
    alloc: std.mem.Allocator,
    vectors: []f32,
    count: usize,
    dimensions: usize,
    seed: u64,
) !void {
    if (vectors.len != count * dimensions) return error.InvalidArgument;
    const encoder = try hdc.Encoder.init(.{
        .dimensions = @intCast(dimensions),
        .atomic_seed = seed,
    });
    const scratch = try alloc.alloc(f32, dimensions);
    defer alloc.free(scratch);
    for (0..count) |class_id| {
        const vector = vectors[class_id * dimensions ..][0..dimensions];
        @memset(vector, 0);
        var value_buffer: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buffer, "{d}", .{class_id});
        try encoder.addAssociation(
            vector,
            scratch,
            "product_class",
            .{ .kind = .string, .bytes = value },
        );
    }
}

fn classCount(fixture: Fixture) usize {
    var maximum: u32 = 0;
    for (fixture.product_classes) |value| maximum = @max(maximum, value);
    for (fixture.query_classes) |value| maximum = @max(maximum, value);
    return @as(usize, maximum) + 1;
}

fn buildQrelOffsets(
    alloc: std.mem.Allocator,
    query_count: usize,
    qrels: []const Qrel,
) ![]usize {
    const offsets = try alloc.alloc(usize, query_count + 1);
    @memset(offsets, 0);
    for (qrels) |qrel| {
        if (qrel.query_index >= query_count) return error.InvalidFixture;
        offsets[@as(usize, qrel.query_index) + 1] += 1;
    }
    for (1..offsets.len) |index| offsets[index] += offsets[index - 1];
    return offsets;
}

fn qrelLessThan(_: void, left: Qrel, right: Qrel) bool {
    if (left.query_index != right.query_index) return left.query_index < right.query_index;
    return left.product_index < right.product_index;
}

fn parseFixture(alloc: std.mem.Allocator, raw: []const u8) !Fixture {
    if (raw.len < fixture_header_bytes or !std.mem.eql(u8, raw[0..8], fixture_magic)) {
        return error.InvalidFixture;
    }
    const version = std.mem.readInt(u32, raw[8..12], .little);
    if (version != fixture_version) return error.UnsupportedFixtureVersion;
    const dimensions: usize = std.mem.readInt(u32, raw[12..16], .little);
    const product_count: usize = std.mem.readInt(u32, raw[16..20], .little);
    const query_count: usize = std.mem.readInt(u32, raw[20..24], .little);
    const qrel_count: usize = std.mem.readInt(u32, raw[24..28], .little);
    const max_tokens: usize = std.mem.readInt(u32, raw[28..32], .little);
    if (dimensions == 0 or product_count == 0 or query_count == 0 or qrel_count == 0) {
        return error.InvalidFixture;
    }

    var offset: usize = fixture_header_bytes;
    const product_embeddings = try takeAlignedSlice(f32, raw, &offset, product_count * dimensions);
    const query_embeddings = try takeAlignedSlice(f32, raw, &offset, query_count * dimensions);
    const product_ids = try takeAlignedSlice(u32, raw, &offset, product_count);
    const product_classes = try takeAlignedSlice(u32, raw, &offset, product_count);
    const product_categories = try takeAlignedSlice(u32, raw, &offset, product_count);
    const query_ids = try takeAlignedSlice(u32, raw, &offset, query_count);
    const query_classes = try takeAlignedSlice(u32, raw, &offset, query_count);

    const qrel_data_bytes = try std.math.mul(usize, qrel_count, qrel_bytes);
    if (offset > raw.len or qrel_data_bytes > raw.len - offset or offset + qrel_data_bytes != raw.len) {
        return error.InvalidFixture;
    }
    const qrels = try alloc.alloc(Qrel, qrel_count);
    errdefer alloc.free(qrels);
    for (qrels, 0..) |*qrel, index| {
        const record = raw[offset + index * qrel_bytes ..][0..qrel_bytes];
        qrel.* = .{
            .query_index = std.mem.readInt(u32, record[0..4], .little),
            .product_index = std.mem.readInt(u32, record[4..8], .little),
            .gain = record[8],
        };
        if (qrel.query_index >= query_count or
            qrel.product_index >= product_count or
            qrel.gain > 2)
        {
            return error.InvalidFixture;
        }
    }

    return .{
        .dimensions = dimensions,
        .max_tokens = max_tokens,
        .product_count = product_count,
        .query_count = query_count,
        .product_embeddings = product_embeddings,
        .query_embeddings = query_embeddings,
        .product_ids = product_ids,
        .product_classes = product_classes,
        .product_categories = product_categories,
        .query_ids = query_ids,
        .query_classes = query_classes,
        .qrels = qrels,
    };
}

fn takeAlignedSlice(
    comptime T: type,
    raw: []const u8,
    offset: *usize,
    count: usize,
) ![]const T {
    const byte_count = try std.math.mul(usize, count, @sizeOf(T));
    if (offset.* > raw.len or byte_count > raw.len - offset.*) return error.InvalidFixture;
    const bytes = raw[offset.* .. offset.* + byte_count];
    if (@intFromPtr(bytes.ptr) % @alignOf(T) != 0) return error.InvalidFixtureAlignment;
    const aligned: []align(@alignOf(T)) const u8 = @alignCast(bytes);
    offset.* += byte_count;
    return std.mem.bytesAsSlice(T, aligned);
}

fn parseArgs(alloc: std.mem.Allocator, args_in: std.process.Args) !Config {
    var fixture_path: ?[]const u8 = null;
    var hdc_dimensions: usize = 10_000;
    var seed: u64 = 13;
    var fusion_weight: f32 = 0.25;
    var baseline_only = false;
    var semantic_weights = std.ArrayListUnmanaged(f32).empty;
    errdefer semantic_weights.deinit(alloc);
    var ann_semantic_weight: ?f32 = null;
    var ann_candidates = std.ArrayListUnmanaged(usize).empty;
    errdefer ann_candidates.deinit(alloc);

    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |argument| {
        if (std.mem.eql(u8, argument, "--fixture")) {
            fixture_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, argument, "--hdc-dims")) {
            hdc_dimensions = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, argument, "--semantic-weights")) {
            var values = std.mem.splitScalar(u8, args.next() orelse return error.InvalidArgument, ',');
            while (values.next()) |value| {
                const parsed = try std.fmt.parseFloat(f32, value);
                if (!std.math.isFinite(parsed) or parsed <= 0) return error.InvalidArgument;
                try semantic_weights.append(alloc, parsed);
            }
        } else if (std.mem.eql(u8, argument, "--fusion-weight")) {
            fusion_weight = try std.fmt.parseFloat(f32, args.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, argument, "--seed")) {
            seed = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, argument, "--baseline-only")) {
            baseline_only = true;
        } else if (std.mem.eql(u8, argument, "--ann-semantic-weight")) {
            const parsed = try std.fmt.parseFloat(f32, args.next() orelse return error.InvalidArgument);
            if (!std.math.isFinite(parsed) or parsed <= 0) return error.InvalidArgument;
            ann_semantic_weight = parsed;
        } else if (std.mem.eql(u8, argument, "--ann-candidates")) {
            var values = std.mem.splitScalar(u8, args.next() orelse return error.InvalidArgument, ',');
            while (values.next()) |value| {
                const parsed = try std.fmt.parseInt(usize, value, 10);
                if (parsed < top_k) return error.InvalidArgument;
                try ann_candidates.append(alloc, parsed);
            }
        } else {
            return error.InvalidArgument;
        }
    }
    if (fixture_path == null or
        hdc_dimensions == 0 or
        !std.math.isFinite(fusion_weight) or
        fusion_weight < 0)
    {
        return error.InvalidArgument;
    }
    if (semantic_weights.items.len == 0) {
        try semantic_weights.appendSlice(alloc, &.{ 4, 8, 12, 16, 24, 32 });
    }
    if (ann_semantic_weight != null and ann_candidates.items.len == 0) {
        try ann_candidates.appendSlice(alloc, &.{ 50, 100, 200 });
    }
    std.mem.sort(usize, ann_candidates.items, {}, std.sort.asc(usize));
    return .{
        .fixture_path = fixture_path.?,
        .hdc_dimensions = hdc_dimensions,
        .semantic_weights = try semantic_weights.toOwnedSlice(alloc),
        .fusion_weight = fusion_weight,
        .seed = seed,
        .baseline_only = baseline_only,
        .ann_semantic_weight = ann_semantic_weight,
        .ann_candidates = try ann_candidates.toOwnedSlice(alloc),
    };
}

test "top-k insertion is descending and deterministic on ties" {
    var ranked: [top_k]Ranked = undefined;
    var count: usize = 0;
    insertTopK(&ranked, &count, .{ .index = 7, .score = 0.5 });
    insertTopK(&ranked, &count, .{ .index = 3, .score = 0.5 });
    insertTopK(&ranked, &count, .{ .index = 9, .score = 0.7 });
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(usize, 9), ranked[0].index);
    try std.testing.expectEqual(@as(usize, 3), ranked[1].index);
    try std.testing.expectEqual(@as(usize, 7), ranked[2].index);
}

test "fixture parser validates and exposes the binary layout" {
    var raw: [72]u8 align(4) = [_]u8{0} ** 72;
    @memcpy(raw[0..8], fixture_magic);
    std.mem.writeInt(u32, raw[8..12], fixture_version, .little);
    std.mem.writeInt(u32, raw[12..16], 1, .little);
    std.mem.writeInt(u32, raw[16..20], 1, .little);
    std.mem.writeInt(u32, raw[20..24], 1, .little);
    std.mem.writeInt(u32, raw[24..28], 1, .little);
    std.mem.writeInt(u32, raw[28..32], 32, .little);
    std.mem.writeInt(u32, raw[32..36], @bitCast(@as(f32, 0.25)), .little);
    std.mem.writeInt(u32, raw[36..40], @bitCast(@as(f32, 0.75)), .little);
    std.mem.writeInt(u32, raw[40..44], 17, .little);
    std.mem.writeInt(u32, raw[44..48], 2, .little);
    std.mem.writeInt(u32, raw[48..52], 4, .little);
    std.mem.writeInt(u32, raw[52..56], 23, .little);
    std.mem.writeInt(u32, raw[56..60], 2, .little);
    std.mem.writeInt(u32, raw[60..64], 0, .little);
    std.mem.writeInt(u32, raw[64..68], 0, .little);
    raw[68] = 2;

    const fixture = try parseFixture(std.testing.allocator, &raw);
    defer std.testing.allocator.free(fixture.qrels);
    try std.testing.expectEqual(@as(usize, 1), fixture.dimensions);
    try std.testing.expectEqual(@as(usize, 32), fixture.max_tokens);
    try std.testing.expectEqual(@as(f32, 0.25), fixture.product_embeddings[0]);
    try std.testing.expectEqual(@as(f32, 0.75), fixture.query_embeddings[0]);
    try std.testing.expectEqual(@as(u32, 17), fixture.product_ids[0]);
    try std.testing.expectEqual(@as(u32, 23), fixture.query_ids[0]);
    try std.testing.expectEqual(@as(u8, 2), fixture.qrels[0].gain);
}
