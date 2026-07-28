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
const association_fixture_magic = "AFHDCAS1";
const association_fixture_version: u32 = 1;
const association_fixture_header_bytes: usize = 32;
const top_k: usize = 10;

const Config = struct {
    fixture_path: []const u8,
    hdc_dimensions: usize = 10_000,
    semantic_weights: []const f32 = &.{ 4, 8, 12, 16, 24, 32 },
    fusion_weight: f32 = 0.25,
    seed: u64 = 13,
    ann_seed: u64 = 13,
    baseline_only: bool = false,
    ann_semantic_weight: ?f32 = null,
    ann_candidates: []const usize = &.{},
    associations_path: ?[]const u8 = null,
    compositional_only: bool = false,
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
    associations: ?Associations = null,
    compositional_only: bool = false,
};

const Associations = struct {
    field_count: usize,
    product_offsets: []const u32,
    product_pairs: []const u32,
    query_offsets: []const u32,
    query_pairs: []const u32,

    fn product(self: Associations, index: usize) []const u32 {
        const start = @as(usize, self.product_offsets[index]) * 2;
        const end = @as(usize, self.product_offsets[index + 1]) * 2;
        return self.product_pairs[start..end];
    }

    fn query(self: Associations, index: usize) []const u32 {
        const start = @as(usize, self.query_offsets[index]) * 2;
        const end = @as(usize, self.query_offsets[index + 1]) * 2;
        return self.query_pairs[start..end];
    }
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
    const associations_raw = if (config.associations_path) |path|
        try std.Io.Dir.cwd().readFileAlloc(
            init.io,
            path,
            alloc,
            .limited(64 * 1024 * 1024),
        )
    else
        &.{};
    defer if (config.associations_path != null) alloc.free(associations_raw);
    if (config.associations_path != null) {
        fixture.associations = try parseAssociationsFixture(
            associations_raw,
            fixture.product_count,
            fixture.query_count,
        );
    }
    if (config.compositional_only and fixture.associations == null) {
        return error.CompositionalAssociationsRequired;
    }
    fixture.compositional_only = config.compositional_only;

    std.debug.print(
        "wands_fixture products={d} queries={d} qrels={d} embedding_dimensions={d} max_tokens={d} hdc_dimensions={d} seed={d} ann_seed={d} fusion_weight={d:.3} association_fields={d} product_associations={d} query_associations={d} compositional_only={any}\n",
        .{
            fixture.product_count,
            fixture.query_count,
            fixture.qrels.len,
            fixture.dimensions,
            fixture.max_tokens,
            config.hdc_dimensions,
            config.seed,
            config.ann_seed,
            config.fusion_weight,
            if (fixture.associations) |associations| associations.field_count else 0,
            if (fixture.associations) |associations| associations.product_pairs.len / 2 else 0,
            if (fixture.associations) |associations| associations.query_pairs.len / 2 else 0,
            config.compositional_only,
        },
    );

    std.mem.sort(Qrel, fixture.qrels, {}, qrelLessThan);
    const qrel_offsets = try buildQrelOffsets(alloc, fixture.query_count, fixture.qrels);
    defer alloc.free(qrel_offsets);
    const ann_quality_count = try std.math.mul(
        usize,
        fixture.query_count,
        config.ann_candidates.len,
    );
    const fusion_ann_quality = try alloc.alloc(PerQueryQuality, ann_quality_count);
    defer alloc.free(fusion_ann_quality);
    const fusion_candidate_quality = try alloc.alloc(PerQueryQuality, ann_quality_count);
    defer alloc.free(fusion_candidate_quality);
    const hdc_rabitq_quality = try alloc.alloc(PerQueryQuality, ann_quality_count);
    defer alloc.free(hdc_rabitq_quality);
    const hdc_bipolar_quality = try alloc.alloc(PerQueryQuality, ann_quality_count);
    defer alloc.free(hdc_bipolar_quality);
    for (fusion_ann_quality) |*quality| quality.* = .{};
    for (fusion_candidate_quality) |*quality| quality.* = .{};
    for (hdc_rabitq_quality) |*quality| quality.* = .{};
    for (hdc_bipolar_quality) |*quality| quality.* = .{};

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
            config.ann_seed,
            true,
            "embedding_structured_fusion",
            fusion_candidate_quality,
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
            config.ann_seed,
            false,
            "embedding_structured_fusion_post_candidates",
            fusion_ann_quality,
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

    const structural_products: []f32 = if (fixture.associations != null)
        try alloc.alloc(
            f32,
            try std.math.mul(usize, fixture.product_count, config.hdc_dimensions),
        )
    else
        &.{};
    defer if (fixture.associations != null) alloc.free(structural_products);
    const structural_queries: []f32 = if (fixture.associations != null)
        try alloc.alloc(
            f32,
            try std.math.mul(usize, fixture.query_count, config.hdc_dimensions),
        )
    else
        &.{};
    defer if (fixture.associations != null) alloc.free(structural_queries);
    if (fixture.associations != null) {
        const structural_started = antfly.platform_time.monotonicNs();
        try buildCompositionalStructuralVectors(
            alloc,
            fixture,
            config.hdc_dimensions,
            config.seed,
            structural_products,
            structural_queries,
        );
        const structural_elapsed = antfly.platform_time.monotonicNs() - structural_started;
        std.debug.print(
            "wands_compositional_structure products={d} queries={d} dimensions={d} total_ms={d:.3} bytes={d}\n",
            .{
                fixture.product_count,
                fixture.query_count,
                config.hdc_dimensions,
                @as(f64, @floatFromInt(structural_elapsed)) / std.time.ns_per_ms,
                (structural_products.len + structural_queries.len) * @sizeOf(f32),
            },
        );
        if (config.ann_candidates.len > 0) {
            try evaluateCandidateLocalPackedInteraction(
                alloc,
                fixture,
                qrel_offsets,
                structural_products,
                structural_queries,
                config.hdc_dimensions,
                config.fusion_weight,
                config.ann_candidates,
                config.ann_seed,
                config.seed,
            );
        }
    }

    const complete_products = try alloc.alloc(f32, projected_products.len);
    defer alloc.free(complete_products);
    for (config.semantic_weights) |semantic_weight| {
        const compose_started = antfly.platform_time.monotonicNs();
        for (0..fixture.product_count) |product_index| {
            const projected = projected_products[product_index * config.hdc_dimensions ..][0..config.hdc_dimensions];
            const structured = if (fixture.associations != null)
                structural_products[product_index * config.hdc_dimensions ..][0..config.hdc_dimensions]
            else blk: {
                const class_id = fixture.product_classes[product_index];
                break :blk structural_classes[@as(usize, class_id) * config.hdc_dimensions ..][0..config.hdc_dimensions];
            };
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
        const structured_query_vectors = StructuredQueries{
            .vectors = if (fixture.associations != null)
                structural_queries
            else
                structural_classes,
            .count = if (fixture.associations != null)
                fixture.query_count
            else
                class_count,
            .semantic_weight = semantic_weight,
            .by_query = fixture.associations != null,
        };
        try evaluateMethod(
            alloc,
            fixture,
            qrel_offsets,
            .complete_hdc_structured,
            complete_products,
            projected_queries,
            config.hdc_dimensions,
            structured_query_vectors,
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
                    structured_query_vectors,
                    config.fusion_weight,
                    config.ann_candidates,
                    config.ann_seed,
                    true,
                    structured_label,
                    hdc_rabitq_quality,
                );
                var bipolar_label_buffer: [128]u8 = undefined;
                const bipolar_label = try std.fmt.bufPrint(
                    &bipolar_label_buffer,
                    "{s}_packed_bipolar",
                    .{structured_label},
                );
                try evaluatePackedBipolarMethod(
                    alloc,
                    fixture,
                    qrel_offsets,
                    complete_products,
                    projected_queries,
                    config.hdc_dimensions,
                    structured_query_vectors,
                    config.ann_candidates,
                    bipolar_label,
                    hdc_bipolar_quality,
                );
                for (config.ann_candidates, 0..) |candidate_budget, budget_index| {
                    const offset = budget_index * fixture.query_count;
                    const fusion_quality =
                        fusion_ann_quality[offset..][0..fixture.query_count];
                    const candidate_fusion_quality =
                        fusion_candidate_quality[offset..][0..fixture.query_count];
                    const rabitq_quality =
                        hdc_rabitq_quality[offset..][0..fixture.query_count];
                    const bipolar_quality =
                        hdc_bipolar_quality[offset..][0..fixture.query_count];
                    inline for (.{ PairedMetric.ndcg10, PairedMetric.graph_answer_top1 }) |metric| {
                        try printPairedBootstrap(
                            alloc,
                            fixture,
                            candidate_budget,
                            structured_label,
                            "embedding_structured_fusion_post_candidates",
                            rabitq_quality,
                            fusion_quality,
                            metric,
                            config.seed ^ config.ann_seed ^
                                @as(u64, @intCast(candidate_budget)) ^
                                @as(u64, @intFromEnum(metric)),
                        );
                        try printPairedBootstrap(
                            alloc,
                            fixture,
                            candidate_budget,
                            bipolar_label,
                            "embedding_structured_fusion_post_candidates",
                            bipolar_quality,
                            fusion_quality,
                            metric,
                            config.seed ^ config.ann_seed ^
                                @as(u64, @intCast(candidate_budget)) ^
                                @as(u64, @intFromEnum(metric)) ^ 0xb170_1a2,
                        );
                        try printPairedBootstrap(
                            alloc,
                            fixture,
                            candidate_budget,
                            structured_label,
                            "embedding_structured_fusion",
                            rabitq_quality,
                            candidate_fusion_quality,
                            metric,
                            config.seed ^ config.ann_seed ^
                                @as(u64, @intCast(candidate_budget)) ^
                                @as(u64, @intFromEnum(metric)) ^ 0xca11_d1da,
                        );
                        try printPairedBootstrap(
                            alloc,
                            fixture,
                            candidate_budget,
                            bipolar_label,
                            "embedding_structured_fusion",
                            bipolar_quality,
                            candidate_fusion_quality,
                            metric,
                            config.seed ^ config.ann_seed ^
                                @as(u64, @intCast(candidate_budget)) ^
                                @as(u64, @intFromEnum(metric)) ^ 0xca11_b170,
                        );
                    }
                }
            }
        }
    }
}

const StructuredQueries = struct {
    vectors: []const f32,
    count: usize,
    semantic_weight: f32,
    by_query: bool = false,
};

fn structuredQueryVector(
    fixture: Fixture,
    structured: StructuredQueries,
    query_index: usize,
    dimensions: usize,
) ![]const f32 {
    const vector_index = if (structured.by_query)
        query_index
    else
        @as(usize, fixture.query_classes[query_index]);
    if (vector_index >= structured.count) return error.InvalidFixture;
    return structured.vectors[vector_index * dimensions ..][0..dimensions];
}

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

const PerQueryQuality = struct {
    ndcg10: f64 = std.math.nan(f64),
    graph_answer_top1: f64 = std.math.nan(f64),
};

const CandidateFeature = struct {
    product_index: usize,
    gain: u8,
    semantic: f32,
    structured_count: f32,
    structured_normalized: f32,
    hdc_similarity: f32,
};

const CandidateRerankerWeights = struct {
    semantic: f64 = 0,
    structured: f64 = 0,
    hdc: f64 = 0,

    fn score(self: CandidateRerankerWeights, feature: CandidateFeature) f64 {
        return self.semantic * feature.semantic +
            self.structured * feature.structured_normalized +
            self.hdc * feature.hdc_similarity;
    }
};

const CandidateRerankerKind = enum {
    transparent,
    learned,
    learned_hdc,
};

/// Evaluates packed bipolar HDC only after the ordinary embedding path has
/// generated candidates. A pairwise linear ranker is trained on validation
/// queries twice: once on transparent semantic/structured features, and once
/// with the packed-HDC similarity feature. Holdout comparisons therefore ask
/// whether HDC adds signal beyond both fixed hybrid fusion and a learned
/// reranker without allowing HDC to become the primary candidate index.
fn evaluateCandidateLocalPackedInteraction(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    qrel_offsets: []const usize,
    structural_products: []const f32,
    structural_queries: []const f32,
    hdc_dimensions: usize,
    fusion_weight: f32,
    candidate_budgets: []const usize,
    ann_seed: u64,
    experiment_seed: u64,
) !void {
    if (candidate_budgets.len == 0 or
        structural_products.len != fixture.product_count * hdc_dimensions or
        structural_queries.len != fixture.query_count * hdc_dimensions)
    {
        return error.InvalidArgument;
    }
    const maximum_candidates = candidate_budgets[candidate_budgets.len - 1];
    if (maximum_candidates < top_k or maximum_candidates > fixture.product_count) {
        return error.InvalidArgument;
    }

    const words_per_vector = packedWordCount(hdc_dimensions);
    const packed_products = try alloc.alloc(
        u64,
        try std.math.mul(usize, fixture.product_count, words_per_vector),
    );
    defer alloc.free(packed_products);
    const pack_started = antfly.platform_time.monotonicNs();
    for (0..fixture.product_count) |product_index| {
        try packBipolarSigns(
            structural_products[product_index * hdc_dimensions ..][0..hdc_dimensions],
            packed_products[product_index * words_per_vector ..][0..words_per_vector],
        );
    }
    const pack_elapsed = antfly.platform_time.monotonicNs() - pack_started;

    const centroid = try alloc.alloc(f32, fixture.dimensions);
    defer alloc.free(centroid);
    @memset(centroid, 0);
    for (0..fixture.product_count) |product_index| {
        const vector =
            fixture.product_embeddings[product_index * fixture.dimensions ..][0..fixture.dimensions];
        for (centroid, vector) |*mean, value| mean.* += value;
    }
    antfly.vector.scale(1.0 / @as(f32, @floatFromInt(fixture.product_count)), centroid);

    var quantizer = try antfly.quantizer.RaBitQuantizer.init(
        alloc,
        fixture.dimensions,
        ann_seed,
        .cosine,
    );
    defer quantizer.deinit();
    var quantized = try quantizer.quantize(
        centroid,
        fixture.product_embeddings,
        fixture.product_count,
    );
    defer quantized.deinit(alloc);

    const feature_count = try std.math.mul(
        usize,
        fixture.query_count,
        maximum_candidates,
    );
    const features = try alloc.alloc(CandidateFeature, feature_count);
    defer alloc.free(features);
    const included_queries = try alloc.alloc(bool, fixture.query_count);
    defer alloc.free(included_queries);
    @memset(included_queries, false);
    const candidate_generation_ns = try alloc.alloc(u64, fixture.query_count);
    defer alloc.free(candidate_generation_ns);
    @memset(candidate_generation_ns, 0);
    const hdc_feature_ns = try alloc.alloc(u64, fixture.query_count);
    defer alloc.free(hdc_feature_ns);
    @memset(hdc_feature_ns, 0);

    const approximate_distances = try alloc.alloc(f32, fixture.product_count);
    defer alloc.free(approximate_distances);
    const error_bounds = try alloc.alloc(f32, fixture.product_count);
    defer alloc.free(error_bounds);
    var estimate_scratch =
        try antfly.quantizer.RaBitQuantizer.EstimateScratch.init(alloc, fixture.dimensions);
    defer estimate_scratch.deinit(alloc);
    const approximate_ranked = try alloc.alloc(Ranked, fixture.product_count);
    defer alloc.free(approximate_ranked);
    const gains = try alloc.alloc(u8, fixture.product_count);
    defer alloc.free(gains);
    const packed_query = try alloc.alloc(u64, words_per_vector);
    defer alloc.free(packed_query);

    for (0..fixture.query_count) |query_index| {
        if (!queryIncluded(fixture, query_index)) continue;
        @memset(gains, 0);
        const qrels = fixture.qrels[qrel_offsets[query_index]..qrel_offsets[query_index + 1]];
        var relevant_count: usize = 0;
        for (qrels) |qrel| {
            gains[qrel.product_index] = @max(gains[qrel.product_index], qrel.gain);
            if (qrel.gain > 0) relevant_count += 1;
        }
        if (relevant_count == 0) continue;
        included_queries[query_index] = true;

        const query_embedding =
            fixture.query_embeddings[query_index * fixture.dimensions ..][0..fixture.dimensions];
        const started = antfly.platform_time.monotonicNs();
        try quantizer.estimateDistancesWithScratch(
            &quantized,
            query_embedding,
            approximate_distances,
            error_bounds,
            &estimate_scratch,
        );
        for (approximate_distances, 0..) |distance, product_index| {
            approximate_ranked[product_index] = .{
                .index = product_index,
                .score = 1 - distance,
            };
        }
        std.mem.sort(Ranked, approximate_ranked, {}, rankedGreaterThanContext);
        candidate_generation_ns[query_index] =
            antfly.platform_time.monotonicNs() - started;

        const query_structural =
            structural_queries[query_index * hdc_dimensions ..][0..hdc_dimensions];
        const hdc_started = antfly.platform_time.monotonicNs();
        try packBipolarSigns(query_structural, packed_query);
        const output =
            features[query_index * maximum_candidates ..][0..maximum_candidates];
        for (approximate_ranked[0..maximum_candidates], output) |candidate, *feature| {
            const packed_product =
                packed_products[candidate.index * words_per_vector ..][0..words_per_vector];
            const hamming: f32 = @floatFromInt(hammingDistance(packed_query, packed_product));
            feature.* = .{
                .product_index = candidate.index,
                .gain = gains[candidate.index],
                .semantic = 0,
                .structured_count = 0,
                .structured_normalized = 0,
                .hdc_similarity = 1 - 2 * hamming / @as(f32, @floatFromInt(hdc_dimensions)),
            };
        }
        hdc_feature_ns[query_index] =
            antfly.platform_time.monotonicNs() - hdc_started;

        const association_count =
            1 + (fixture.associations orelse return error.InvalidArgument).query(query_index).len / 2;
        for (approximate_ranked[0..maximum_candidates], output) |candidate, *feature| {
            const product_embedding =
                fixture.product_embeddings[candidate.index * fixture.dimensions ..][0..fixture.dimensions];
            const match_count = structuredMatchCount(fixture, query_index, candidate.index);
            feature.semantic = antfly.vector.dot(query_embedding, product_embedding);
            feature.structured_count = @floatFromInt(match_count);
            feature.structured_normalized = @as(f32, @floatFromInt(match_count)) /
                @as(f32, @floatFromInt(association_count));
        }
    }

    const learned = trainCandidateReranker(
        fixture,
        features,
        included_queries,
        maximum_candidates,
        false,
    );
    const learned_hdc = trainCandidateReranker(
        fixture,
        features,
        included_queries,
        maximum_candidates,
        true,
    );
    std.debug.print(
        "wands_candidate_reranker_training split=validation candidates={d} epochs=12 objective=pairwise_logistic semantic_weight={d:.6} structured_weight={d:.6} hdc_weight={d:.6} learned_hdc_semantic_weight={d:.6} learned_hdc_structured_weight={d:.6} learned_hdc_hdc_weight={d:.6}\n",
        .{
            maximum_candidates,
            learned.semantic,
            learned.structured,
            learned.hdc,
            learned_hdc.semantic,
            learned_hdc.structured,
            learned_hdc.hdc,
        },
    );
    var hdc_feature_total_ns: u64 = 0;
    var included_query_count: usize = 0;
    for (included_queries, hdc_feature_ns) |included, elapsed| {
        if (!included) continue;
        included_query_count += 1;
        hdc_feature_total_ns += elapsed;
    }
    std.debug.print(
        "wands_candidate_hdc_storage dimensions={d} vectors={d} words_per_vector={d} candidates={d} pack_ms={d:.3} packed_bytes={d} feature_ms_per_query={d:.4} role=candidate_rerank_feature mapping=sign_ge_zero_le_u64_v1\n",
        .{
            hdc_dimensions,
            fixture.product_count,
            words_per_vector,
            maximum_candidates,
            @as(f64, @floatFromInt(pack_elapsed)) / std.time.ns_per_ms,
            packed_products.len * @sizeOf(u64),
            @as(f64, @floatFromInt(hdc_feature_total_ns)) /
                @as(f64, @floatFromInt(included_query_count)) /
                std.time.ns_per_ms,
        },
    );

    const quality_count =
        try std.math.mul(usize, fixture.query_count, candidate_budgets.len);
    const transparent_quality = try alloc.alloc(PerQueryQuality, quality_count);
    defer alloc.free(transparent_quality);
    const learned_quality = try alloc.alloc(PerQueryQuality, quality_count);
    defer alloc.free(learned_quality);
    const learned_hdc_quality = try alloc.alloc(PerQueryQuality, quality_count);
    defer alloc.free(learned_hdc_quality);
    for (transparent_quality) |*quality| quality.* = .{};
    for (learned_quality) |*quality| quality.* = .{};
    for (learned_hdc_quality) |*quality| quality.* = .{};

    try evaluateCandidateReranker(
        alloc,
        fixture,
        qrel_offsets,
        features,
        included_queries,
        candidate_generation_ns,
        maximum_candidates,
        candidate_budgets,
        .transparent,
        .{ .semantic = 1, .structured = fusion_weight },
        transparent_quality,
        "embedding_structured_fusion_candidate_rerank",
    );
    try evaluateCandidateReranker(
        alloc,
        fixture,
        qrel_offsets,
        features,
        included_queries,
        candidate_generation_ns,
        maximum_candidates,
        candidate_budgets,
        .learned,
        learned,
        learned_quality,
        "learned_candidate_reranker",
    );
    try evaluateCandidateReranker(
        alloc,
        fixture,
        qrel_offsets,
        features,
        included_queries,
        candidate_generation_ns,
        maximum_candidates,
        candidate_budgets,
        .learned_hdc,
        learned_hdc,
        learned_hdc_quality,
        "learned_candidate_reranker_plus_packed_hdc",
    );

    for (candidate_budgets, 0..) |candidate_budget, budget_index| {
        const offset = budget_index * fixture.query_count;
        inline for (.{ PairedMetric.ndcg10, PairedMetric.graph_answer_top1 }) |metric| {
            try printPairedBootstrap(
                alloc,
                fixture,
                candidate_budget,
                "learned_candidate_reranker",
                "embedding_structured_fusion_candidate_rerank",
                learned_quality[offset..][0..fixture.query_count],
                transparent_quality[offset..][0..fixture.query_count],
                metric,
                experiment_seed ^ ann_seed ^ @as(u64, @intCast(candidate_budget)) ^
                    @as(u64, @intFromEnum(metric)) ^ 0x1ea2_0ed,
            );
            try printPairedBootstrap(
                alloc,
                fixture,
                candidate_budget,
                "learned_candidate_reranker_plus_packed_hdc",
                "learned_candidate_reranker",
                learned_hdc_quality[offset..][0..fixture.query_count],
                learned_quality[offset..][0..fixture.query_count],
                metric,
                experiment_seed ^ ann_seed ^ @as(u64, @intCast(candidate_budget)) ^
                    @as(u64, @intFromEnum(metric)) ^ 0xb170_1a2,
            );
        }
    }
}

fn trainCandidateReranker(
    fixture: Fixture,
    features: []const CandidateFeature,
    included_queries: []const bool,
    candidates_per_query: usize,
    include_hdc: bool,
) CandidateRerankerWeights {
    var weights = CandidateRerankerWeights{};
    const epochs: usize = 12;
    const learning_rate: f64 = 0.025;
    const regularization: f64 = 0.0001;
    for (0..epochs) |epoch| {
        const epoch_rate = learning_rate / @sqrt(@as(f64, @floatFromInt(epoch + 1)));
        for (included_queries, 0..) |included, query_index| {
            if (!included or !isValidationQuery(fixture.query_ids[query_index])) continue;
            const query_features =
                features[query_index * candidates_per_query ..][0..candidates_per_query];
            for (query_features) |higher| {
                if (higher.gain == 0) continue;
                var compared: usize = 0;
                for (query_features) |lower| {
                    if (lower.gain >= higher.gain) continue;
                    const semantic_delta: f64 = higher.semantic - lower.semantic;
                    const structured_delta: f64 =
                        higher.structured_normalized - lower.structured_normalized;
                    const hdc_delta: f64 = if (include_hdc)
                        higher.hdc_similarity - lower.hdc_similarity
                    else
                        0;
                    const margin = std.math.clamp(
                        weights.semantic * semantic_delta +
                            weights.structured * structured_delta +
                            weights.hdc * hdc_delta,
                        -20,
                        20,
                    );
                    const gradient = 1.0 / (1.0 + @exp(margin));
                    weights.semantic += epoch_rate *
                        (gradient * semantic_delta - regularization * weights.semantic);
                    weights.structured += epoch_rate *
                        (gradient * structured_delta - regularization * weights.structured);
                    if (include_hdc) {
                        weights.hdc += epoch_rate *
                            (gradient * hdc_delta - regularization * weights.hdc);
                    }
                    compared += 1;
                    if (compared == 32) break;
                }
            }
        }
    }
    return weights;
}

fn evaluateCandidateReranker(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    qrel_offsets: []const usize,
    features: []const CandidateFeature,
    included_queries: []const bool,
    candidate_generation_ns: []const u64,
    candidates_per_query: usize,
    candidate_budgets: []const usize,
    kind: CandidateRerankerKind,
    weights: CandidateRerankerWeights,
    per_query_quality: []PerQueryQuality,
    label: []const u8,
) !void {
    const validation = try alloc.alloc(Metrics, candidate_budgets.len);
    defer alloc.free(validation);
    @memset(validation, .{});
    const holdout = try alloc.alloc(Metrics, candidate_budgets.len);
    defer alloc.free(holdout);
    @memset(holdout, .{});
    const gains = try alloc.alloc(u8, fixture.product_count);
    defer alloc.free(gains);
    const ranked = try alloc.alloc(Ranked, candidates_per_query);
    defer alloc.free(ranked);

    for (included_queries, 0..) |included, query_index| {
        if (!included) continue;
        @memset(gains, 0);
        const qrels = fixture.qrels[qrel_offsets[query_index]..qrel_offsets[query_index + 1]];
        var relevant_count: usize = 0;
        for (qrels) |qrel| {
            gains[qrel.product_index] = @max(gains[qrel.product_index], qrel.gain);
            if (qrel.gain > 0) relevant_count += 1;
        }
        if (relevant_count == 0) continue;
        const query_features =
            features[query_index * candidates_per_query ..][0..candidates_per_query];
        for (candidate_budgets, 0..) |candidate_budget, budget_index| {
            const rerank_started = antfly.platform_time.monotonicNs();
            for (query_features[0..candidate_budget], 0..) |feature, rank_index| {
                const score = switch (kind) {
                    .transparent => weights.semantic * feature.semantic +
                        weights.structured * feature.structured_count,
                    .learned, .learned_hdc => weights.score(feature),
                };
                ranked[rank_index] = .{
                    .index = feature.product_index,
                    .score = @floatCast(score),
                };
            }
            std.mem.sort(
                Ranked,
                ranked[0..candidate_budget],
                {},
                rankedGreaterThanContext,
            );
            const rerank_elapsed =
                antfly.platform_time.monotonicNs() - rerank_started;
            const observed = observeQuery(
                fixture,
                qrels,
                gains,
                ranked[0..top_k],
                relevant_count,
                candidate_generation_ns[query_index] + rerank_elapsed,
            );
            per_query_quality[budget_index * fixture.query_count + query_index] = .{
                .ndcg10 = observed.ndcg_sum,
                .graph_answer_top1 = @floatFromInt(observed.graph_answer_top1),
            };
            if (isValidationQuery(fixture.query_ids[query_index])) {
                validation[budget_index].add(observed);
            } else {
                holdout[budget_index].add(observed);
            }
        }
    }
    for (candidate_budgets, 0..) |candidate_budget, budget_index| {
        var method_buffer: [160]u8 = undefined;
        const method = try std.fmt.bufPrint(
            &method_buffer,
            "{s}_candidates_{d}",
            .{ label, candidate_budget },
        );
        validation[budget_index].print(method, .validation, fixture.dimensions);
        holdout[budget_index].print(method, .holdout, fixture.dimensions);
        var all = validation[budget_index];
        all.add(holdout[budget_index]);
        all.print(method, .all, fixture.dimensions);
    }
}

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
    per_query_quality: ?[]PerQueryQuality,
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
    if (per_query_quality) |quality| {
        if (quality.len != fixture.query_count * candidate_budgets.len) {
            return error.InvalidArgument;
        }
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
        if (!queryIncluded(fixture, query_index)) continue;
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
                const association = try structuredQueryVector(
                    fixture,
                    structured,
                    query_index,
                    dimensions,
                );
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
                method == .embedding_structured_fusion)
            {
                score += fusion_weight * @as(
                    f32,
                    @floatFromInt(structuredMatchCount(fixture, query_index, product_index)),
                );
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
            if (per_query_quality) |per_query| {
                per_query[budget_index * fixture.query_count + query_index] = .{
                    .ndcg10 = quality.ndcg_sum,
                    .graph_answer_top1 = @floatFromInt(quality.graph_answer_top1),
                };
            }
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

/// Evaluates a one-bit sign sketch as candidate-selection state while retaining
/// the complete f32 vectors as the authoritative exact-reranking state.
fn evaluatePackedBipolarMethod(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    qrel_offsets: []const usize,
    product_vectors: []const f32,
    query_vectors: []const f32,
    dimensions: usize,
    structured_queries: StructuredQueries,
    candidate_budgets: []const usize,
    label: []const u8,
    per_query_quality: ?[]PerQueryQuality,
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
    if (per_query_quality) |quality| {
        if (quality.len != fixture.query_count * candidate_budgets.len) {
            return error.InvalidArgument;
        }
    }

    const words_per_vector = packedWordCount(dimensions);
    const packed_products = try alloc.alloc(
        u64,
        try std.math.mul(usize, fixture.product_count, words_per_vector),
    );
    defer alloc.free(packed_products);
    const pack_started = antfly.platform_time.monotonicNs();
    for (0..fixture.product_count) |product_index| {
        try packBipolarSigns(
            product_vectors[product_index * dimensions ..][0..dimensions],
            packed_products[product_index * words_per_vector ..][0..words_per_vector],
        );
    }
    const pack_elapsed = antfly.platform_time.monotonicNs() - pack_started;

    const approximate_ranked = try alloc.alloc(Ranked, fixture.product_count);
    defer alloc.free(approximate_ranked);
    const reranked = try alloc.alloc(Ranked, maximum_candidates);
    defer alloc.free(reranked);
    const gains = try alloc.alloc(u8, fixture.product_count);
    defer alloc.free(gains);
    const query_scratch = try alloc.alloc(f32, dimensions);
    defer alloc.free(query_scratch);
    const packed_query = try alloc.alloc(u64, words_per_vector);
    defer alloc.free(packed_query);
    const validation = try alloc.alloc(AnnMetrics, candidate_budgets.len);
    defer alloc.free(validation);
    @memset(validation, .{});
    const holdout = try alloc.alloc(AnnMetrics, candidate_budgets.len);
    defer alloc.free(holdout);
    @memset(holdout, .{});

    for (0..fixture.query_count) |query_index| {
        if (!queryIncluded(fixture, query_index)) continue;
        @memset(gains, 0);
        const qrels = fixture.qrels[qrel_offsets[query_index]..qrel_offsets[query_index + 1]];
        var relevant_count: usize = 0;
        for (qrels) |qrel| {
            gains[qrel.product_index] = @max(gains[qrel.product_index], qrel.gain);
            if (qrel.gain > 0) relevant_count += 1;
        }
        if (relevant_count == 0) continue;

        const raw_query = query_vectors[query_index * dimensions ..][0..dimensions];
        const association = try structuredQueryVector(
            fixture,
            structured_queries,
            query_index,
            dimensions,
        );
        for (query_scratch, association, raw_query) |*out, structural_coordinate, semantic_coordinate| {
            out.* = structural_coordinate +
                structured_queries.semantic_weight * semantic_coordinate;
        }
        _ = antfly.vector.normalize(query_scratch);

        var exact_top: [top_k]Ranked = undefined;
        var exact_count: usize = 0;
        for (0..fixture.product_count) |product_index| {
            const score = antfly.vector.dot(
                query_scratch,
                product_vectors[product_index * dimensions ..][0..dimensions],
            );
            insertTopK(&exact_top, &exact_count, .{ .index = product_index, .score = score });
        }

        const approximate_started = antfly.platform_time.monotonicNs();
        try packBipolarSigns(query_scratch, packed_query);
        for (0..fixture.product_count) |product_index| {
            const packed_product =
                packed_products[product_index * words_per_vector ..][0..words_per_vector];
            approximate_ranked[product_index] = .{
                .index = product_index,
                .score = -@as(f32, @floatFromInt(hammingDistance(packed_query, packed_product))),
            };
        }
        std.mem.sort(Ranked, approximate_ranked, {}, rankedGreaterThanContext);
        const approximate_elapsed = antfly.platform_time.monotonicNs() - approximate_started;

        for (candidate_budgets, 0..) |candidate_budget, budget_index| {
            const rerank_started = antfly.platform_time.monotonicNs();
            for (approximate_ranked[0..candidate_budget], 0..) |candidate, candidate_index| {
                reranked[candidate_index] = .{
                    .index = candidate.index,
                    .score = antfly.vector.dot(
                        query_scratch,
                        product_vectors[candidate.index * dimensions ..][0..dimensions],
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
            if (per_query_quality) |per_query| {
                per_query[budget_index * fixture.query_count + query_index] = .{
                    .ndcg10 = quality.ndcg_sum,
                    .graph_answer_top1 = @floatFromInt(quality.graph_answer_top1),
                };
            }
            if (isValidationQuery(fixture.query_ids[query_index])) {
                validation[budget_index].add(observed);
            } else {
                holdout[budget_index].add(observed);
            }
        }
    }

    const authoritative_bytes = product_vectors.len * @sizeOf(f32);
    const packed_bytes = packed_products.len * @sizeOf(u64);
    std.debug.print(
        "wands_bipolar_index method={s} dimensions={d} vectors={d} words_per_vector={d} pack_ms={d:.3} authoritative_bytes={d} packed_bytes={d} compression_ratio={d:.3} mapping=sign_ge_zero_le_u64_v1\n",
        .{
            label,
            dimensions,
            fixture.product_count,
            words_per_vector,
            @as(f64, @floatFromInt(pack_elapsed)) / std.time.ns_per_ms,
            authoritative_bytes,
            packed_bytes,
            @as(f64, @floatFromInt(authoritative_bytes)) /
                @as(f64, @floatFromInt(packed_bytes)),
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

const PairedMetric = enum {
    ndcg10,
    graph_answer_top1,
};

fn printPairedBootstrap(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    candidate_budget: usize,
    method: []const u8,
    baseline: []const u8,
    method_quality: []const PerQueryQuality,
    baseline_quality: []const PerQueryQuality,
    metric: PairedMetric,
    seed: u64,
) !void {
    if (method_quality.len != fixture.query_count or
        baseline_quality.len != fixture.query_count)
    {
        return error.InvalidArgument;
    }
    const deltas = try alloc.alloc(f64, fixture.query_count);
    defer alloc.free(deltas);
    var delta_count: usize = 0;
    for (method_quality, baseline_quality, 0..) |method_query, baseline_query, query_index| {
        if (isValidationQuery(fixture.query_ids[query_index])) continue;
        const method_value = switch (metric) {
            .ndcg10 => method_query.ndcg10,
            .graph_answer_top1 => method_query.graph_answer_top1,
        };
        const baseline_value = switch (metric) {
            .ndcg10 => baseline_query.ndcg10,
            .graph_answer_top1 => baseline_query.graph_answer_top1,
        };
        if (!std.math.isFinite(method_value) or !std.math.isFinite(baseline_value)) continue;
        deltas[delta_count] = method_value - baseline_value;
        delta_count += 1;
    }
    if (delta_count == 0) return error.InvalidArgument;

    var mean_delta: f64 = 0;
    for (deltas[0..delta_count]) |delta| mean_delta += delta;
    mean_delta /= @floatFromInt(delta_count);

    const bootstrap_samples: usize = 10_000;
    const sample_means = try alloc.alloc(f64, bootstrap_samples);
    defer alloc.free(sample_means);
    var random_state = seed;
    for (sample_means) |*sample_mean| {
        var total: f64 = 0;
        for (0..delta_count) |_| {
            const sampled_index = bootstrapNext(&random_state) % delta_count;
            total += deltas[sampled_index];
        }
        sample_mean.* = total / @as(f64, @floatFromInt(delta_count));
    }
    std.mem.sort(f64, sample_means, {}, std.sort.asc(f64));
    const ci_low = sample_means[@divFloor(bootstrap_samples * 25, 1000)];
    const ci_high = sample_means[@divFloor(bootstrap_samples * 975, 1000) - 1];
    std.debug.print(
        "wands_paired method={s} baseline={s} split=holdout candidates={d} queries={d} metric={s} mean_delta={d:.6} ci95_low={d:.6} ci95_high={d:.6} significant_advantage={any} bootstrap_samples={d}\n",
        .{
            method,
            baseline,
            candidate_budget,
            delta_count,
            @tagName(metric),
            mean_delta,
            ci_low,
            ci_high,
            ci_low > 0,
            bootstrap_samples,
        },
    );
}

fn bootstrapNext(state: *u64) u64 {
    state.* +%= 0x9e3779b97f4a7c15;
    var value = state.*;
    value = (value ^ (value >> 30)) *% 0xbf58476d1ce4e5b9;
    value = (value ^ (value >> 27)) *% 0x94d049bb133111eb;
    return value ^ (value >> 31);
}

fn packedWordCount(dimensions: usize) usize {
    std.debug.assert(dimensions > 0);
    return 1 + (dimensions - 1) / 64;
}

/// Packs non-negative coordinates as one and negative coordinates as zero.
/// Coordinates are ordered low-coordinate-first in little-endian u64 words.
fn packBipolarSigns(vector: []const f32, sign_words: []u64) !void {
    if (vector.len == 0 or sign_words.len != packedWordCount(vector.len)) {
        return error.InvalidArgument;
    }
    @memset(sign_words, 0);
    for (vector, 0..) |coordinate, index| {
        if (!std.math.isFinite(coordinate)) return error.NonFiniteHypervector;
        if (coordinate >= 0) {
            sign_words[index / 64] |= @as(u64, 1) << @intCast(index % 64);
        }
    }
}

fn hammingDistance(left: []const u64, right: []const u64) usize {
    std.debug.assert(left.len == right.len);
    var distance: usize = 0;
    for (left, right) |left_word, right_word| {
        distance += @popCount(left_word ^ right_word);
    }
    return distance;
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
    if (method == .embedding_structured_fusion) {
        score += fusion_weight * @as(
            f32,
            @floatFromInt(structuredMatchCount(fixture, query_index, product_index)),
        );
    }
    return score;
}

fn queryIncluded(fixture: Fixture, query_index: usize) bool {
    if (!fixture.compositional_only) return true;
    const associations = fixture.associations orelse return false;
    return associations.query(query_index).len > 0;
}

fn structuredMatchCount(
    fixture: Fixture,
    query_index: usize,
    product_index: usize,
) usize {
    var matches: usize = @intFromBool(
        fixture.product_classes[product_index] == fixture.query_classes[query_index],
    );
    const associations = fixture.associations orelse return matches;
    const product = associations.product(product_index);
    const query = associations.query(query_index);
    var query_offset: usize = 0;
    while (query_offset < query.len) : (query_offset += 2) {
        var product_offset: usize = 0;
        while (product_offset < product.len) : (product_offset += 2) {
            if (query[query_offset] == product[product_offset] and
                query[query_offset + 1] == product[product_offset + 1])
            {
                matches += 1;
                break;
            }
        }
    }
    return matches;
}

fn allStructuredAssociationsMatch(
    fixture: Fixture,
    query_index: usize,
    product_index: usize,
) bool {
    if (fixture.product_classes[product_index] != fixture.query_classes[query_index]) {
        return false;
    }
    const associations = fixture.associations orelse return true;
    return structuredMatchCount(fixture, query_index, product_index) ==
        1 + associations.query(query_index).len / 2;
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
        if (!queryIncluded(fixture, query_index)) continue;
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
                const association = try structuredQueryVector(
                    fixture,
                    structured,
                    query_index,
                    dimensions,
                );
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
                !allStructuredAssociationsMatch(fixture, query_index, product_index))
            {
                continue;
            }
            var score = antfly.vector.dot(
                query,
                product_vectors[product_index * dimensions ..][0..dimensions],
            );
            if (method == .embedding_structured_fusion) {
                score += fusion_weight * @as(
                    f32,
                    @floatFromInt(structuredMatchCount(fixture, query_index, product_index)),
                );
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

fn buildCompositionalStructuralVectors(
    alloc: std.mem.Allocator,
    fixture: Fixture,
    dimensions: usize,
    seed: u64,
    product_vectors: []f32,
    query_vectors: []f32,
) !void {
    const associations = fixture.associations orelse return error.InvalidArgument;
    if (product_vectors.len != fixture.product_count * dimensions or
        query_vectors.len != fixture.query_count * dimensions)
    {
        return error.InvalidArgument;
    }
    const encoder = try hdc.Encoder.init(.{
        .dimensions = @intCast(dimensions),
        .atomic_seed = seed,
    });
    const scratch = try alloc.alloc(f32, dimensions);
    defer alloc.free(scratch);

    for (0..fixture.product_count) |product_index| {
        try buildCompositionalStructuralVector(
            encoder,
            scratch,
            product_vectors[product_index * dimensions ..][0..dimensions],
            fixture.product_classes[product_index],
            associations.product(product_index),
        );
    }
    for (0..fixture.query_count) |query_index| {
        try buildCompositionalStructuralVector(
            encoder,
            scratch,
            query_vectors[query_index * dimensions ..][0..dimensions],
            fixture.query_classes[query_index],
            associations.query(query_index),
        );
    }
}

fn buildCompositionalStructuralVector(
    encoder: hdc.Encoder,
    scratch: []f32,
    vector: []f32,
    class_id: u32,
    association_pairs: []const u32,
) !void {
    if (association_pairs.len % 2 != 0) return error.InvalidFixture;
    @memset(vector, 0);
    var class_value_buffer: [32]u8 = undefined;
    const class_value = try std.fmt.bufPrint(&class_value_buffer, "{d}", .{class_id});
    try encoder.addAssociation(
        vector,
        scratch,
        "product_class",
        .{ .kind = .string, .bytes = class_value },
    );

    var offset: usize = 0;
    while (offset < association_pairs.len) : (offset += 2) {
        var path_buffer: [32]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &path_buffer,
            "attribute_{d}",
            .{association_pairs[offset]},
        );
        var value_buffer: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(
            &value_buffer,
            "{d}",
            .{association_pairs[offset + 1]},
        );
        try encoder.addAssociation(
            vector,
            scratch,
            path,
            .{ .kind = .string, .bytes = value },
        );
    }
    const scale = 1.0 / @sqrt(@as(
        f32,
        @floatFromInt(1 + association_pairs.len / 2),
    ));
    antfly.vector.scale(scale, vector);
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

fn parseAssociationsFixture(
    raw: []const u8,
    expected_products: usize,
    expected_queries: usize,
) !Associations {
    if (raw.len < association_fixture_header_bytes or
        !std.mem.eql(u8, raw[0..8], association_fixture_magic))
    {
        return error.InvalidAssociationsFixture;
    }
    const version = std.mem.readInt(u32, raw[8..12], .little);
    if (version != association_fixture_version) {
        return error.UnsupportedAssociationsFixtureVersion;
    }
    const product_count: usize = std.mem.readInt(u32, raw[12..16], .little);
    const query_count: usize = std.mem.readInt(u32, raw[16..20], .little);
    const field_count: usize = std.mem.readInt(u32, raw[20..24], .little);
    const product_association_count: usize = std.mem.readInt(u32, raw[24..28], .little);
    const query_association_count: usize = std.mem.readInt(u32, raw[28..32], .little);
    if (product_count != expected_products or
        query_count != expected_queries or
        field_count == 0)
    {
        return error.InvalidAssociationsFixture;
    }

    var offset: usize = association_fixture_header_bytes;
    const product_offsets = try takeAlignedSlice(u32, raw, &offset, product_count + 1);
    const product_pairs = try takeAlignedSlice(
        u32,
        raw,
        &offset,
        try std.math.mul(usize, product_association_count, 2),
    );
    const query_offsets = try takeAlignedSlice(u32, raw, &offset, query_count + 1);
    const query_pairs = try takeAlignedSlice(
        u32,
        raw,
        &offset,
        try std.math.mul(usize, query_association_count, 2),
    );
    if (offset != raw.len or
        product_offsets[0] != 0 or
        query_offsets[0] != 0 or
        product_offsets[product_count] != product_association_count or
        query_offsets[query_count] != query_association_count)
    {
        return error.InvalidAssociationsFixture;
    }
    try validateAssociationRows(product_offsets, product_pairs, field_count);
    try validateAssociationRows(query_offsets, query_pairs, field_count);
    return .{
        .field_count = field_count,
        .product_offsets = product_offsets,
        .product_pairs = product_pairs,
        .query_offsets = query_offsets,
        .query_pairs = query_pairs,
    };
}

fn validateAssociationRows(
    offsets: []const u32,
    pairs: []const u32,
    field_count: usize,
) !void {
    for (offsets[1..], offsets[0 .. offsets.len - 1]) |end, start| {
        if (end < start or @as(usize, end) * 2 > pairs.len) {
            return error.InvalidAssociationsFixture;
        }
    }
    var offset: usize = 0;
    while (offset < pairs.len) : (offset += 2) {
        if (pairs[offset] >= field_count) return error.InvalidAssociationsFixture;
    }
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
    var ann_seed: u64 = 13;
    var fusion_weight: f32 = 0.25;
    var baseline_only = false;
    var associations_path: ?[]const u8 = null;
    var compositional_only = false;
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
        } else if (std.mem.eql(u8, argument, "--ann-seed")) {
            ann_seed = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, argument, "--associations")) {
            associations_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, argument, "--compositional-only")) {
            compositional_only = true;
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
        fusion_weight < 0 or
        (compositional_only and associations_path == null))
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
        .ann_seed = ann_seed,
        .baseline_only = baseline_only,
        .ann_semantic_weight = ann_semantic_weight,
        .ann_candidates = try ann_candidates.toOwnedSlice(alloc),
        .associations_path = associations_path,
        .compositional_only = compositional_only,
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

test "bipolar signs pack low-coordinate-first and preserve hamming similarity" {
    var left_words: [2]u64 = undefined;
    var right_words: [2]u64 = undefined;
    var left = [_]f32{-1} ** 65;
    left[0] = 1;
    left[2] = 0;
    left[64] = 1;
    var right = left;
    right[0] = -1;
    right[64] = -1;

    try packBipolarSigns(&left, &left_words);
    try packBipolarSigns(&right, &right_words);

    try std.testing.expectEqual(@as(usize, 2), packedWordCount(left.len));
    try std.testing.expectEqual(@as(u64, 0b101), left_words[0]);
    try std.testing.expectEqual(@as(u64, 1), left_words[1]);
    try std.testing.expectEqual(@as(usize, 2), hammingDistance(&left_words, &right_words));
}

test "bipolar sign packing rejects non-finite coordinates" {
    var sign_words: [1]u64 = undefined;
    try std.testing.expectError(
        error.NonFiniteHypervector,
        packBipolarSigns(&.{ 1, std.math.nan(f32) }, &sign_words),
    );
}

test "candidate reranker learns held-out HDC feature only when enabled" {
    const fixture = Fixture{
        .dimensions = 1,
        .max_tokens = 1,
        .product_count = 2,
        .query_count = 1,
        .product_embeddings = &.{ 0, 0 },
        .query_embeddings = &.{0},
        .product_ids = &.{ 1, 2 },
        .product_classes = &.{ 0, 0 },
        .product_categories = &.{ 0, 0 },
        .query_ids = &.{5},
        .query_classes = &.{0},
        .qrels = &.{},
    };
    const features = [_]CandidateFeature{
        .{
            .product_index = 0,
            .gain = 2,
            .semantic = 0,
            .structured_count = 0,
            .structured_normalized = 0,
            .hdc_similarity = 1,
        },
        .{
            .product_index = 1,
            .gain = 0,
            .semantic = 0,
            .structured_count = 0,
            .structured_normalized = 0,
            .hdc_similarity = -1,
        },
    };
    const without_hdc = trainCandidateReranker(
        fixture,
        &features,
        &.{true},
        features.len,
        false,
    );
    const with_hdc = trainCandidateReranker(
        fixture,
        &features,
        &.{true},
        features.len,
        true,
    );
    try std.testing.expectEqual(@as(f64, 0), without_hdc.hdc);
    try std.testing.expect(with_hdc.hdc > 0);
}

test "association fixture exposes compositional rows and exact matching" {
    var raw: [64]u8 align(4) = [_]u8{0} ** 64;
    @memcpy(raw[0..8], association_fixture_magic);
    std.mem.writeInt(u32, raw[8..12], association_fixture_version, .little);
    std.mem.writeInt(u32, raw[12..16], 1, .little);
    std.mem.writeInt(u32, raw[16..20], 1, .little);
    std.mem.writeInt(u32, raw[20..24], 1, .little);
    std.mem.writeInt(u32, raw[24..28], 1, .little);
    std.mem.writeInt(u32, raw[28..32], 1, .little);
    std.mem.writeInt(u32, raw[32..36], 0, .little);
    std.mem.writeInt(u32, raw[36..40], 1, .little);
    std.mem.writeInt(u32, raw[40..44], 0, .little);
    std.mem.writeInt(u32, raw[44..48], 7, .little);
    std.mem.writeInt(u32, raw[48..52], 0, .little);
    std.mem.writeInt(u32, raw[52..56], 1, .little);
    std.mem.writeInt(u32, raw[56..60], 0, .little);
    std.mem.writeInt(u32, raw[60..64], 7, .little);

    const associations = try parseAssociationsFixture(&raw, 1, 1);
    try std.testing.expectEqualSlices(u32, &.{ 0, 7 }, associations.product(0));
    try std.testing.expectEqualSlices(u32, &.{ 0, 7 }, associations.query(0));

    const fixture = Fixture{
        .dimensions = 1,
        .max_tokens = 1,
        .product_count = 1,
        .query_count = 1,
        .product_embeddings = &.{0},
        .query_embeddings = &.{0},
        .product_ids = &.{1},
        .product_classes = &.{2},
        .product_categories = &.{3},
        .query_ids = &.{4},
        .query_classes = &.{2},
        .qrels = &.{},
        .associations = associations,
        .compositional_only = true,
    };
    try std.testing.expect(queryIncluded(fixture, 0));
    try std.testing.expectEqual(@as(usize, 2), structuredMatchCount(fixture, 0, 0));
    try std.testing.expect(allStructuredAssociationsMatch(fixture, 0, 0));
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
