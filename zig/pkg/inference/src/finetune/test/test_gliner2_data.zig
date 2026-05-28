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
const gliner2_data = @import("termite_finetune_data").gliner2_data;

test {
    std.testing.refAllDecls(gliner2_data);
}

test "GLiNER2 smoke NER fixture has stable stats and span shape" {
    const allocator = std.testing.allocator;
    const entity_types = [_][]const u8{ "person", "organization", "location" };

    var loaded = try gliner2_data.loadExamples(allocator, "testdata/gliner2_ner_smoke.jsonl", null);
    defer loaded.deinit();

    const stats = try gliner2_data.computeStats(allocator, loaded.examples);
    try std.testing.expectEqual(@as(usize, 3), stats.num_examples);
    try std.testing.expectApproxEqAbs(@as(f64, 27.0), stats.avg_text_chars, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), stats.avg_entities, 0.01);
    try std.testing.expectEqual(@as(usize, 3), stats.unique_labels);

    const coverage = gliner2_data.computeTargetCoverageStats(loaded.examples, &entity_types);
    try std.testing.expectEqual(@as(usize, 3), coverage.num_samples);
    try std.testing.expectEqual(@as(usize, 9), coverage.total_entities);
    try std.testing.expectEqual(@as(usize, 9), coverage.target_entities);
    try std.testing.expectEqual(@as(usize, 3), coverage.samples_with_target);
    try std.testing.expectEqual(@as(usize, 0), coverage.samples_without_target);

    const label_vocab = try gliner2_data.buildLabelVocab(allocator, loaded.examples, &entity_types);
    defer {
        for (label_vocab) |label| allocator.free(label);
        allocator.free(label_vocab);
    }
    try std.testing.expectEqual(@as(usize, 3), label_vocab.len);
    try std.testing.expectEqualStrings("location", label_vocab[0]);
    try std.testing.expectEqualStrings("organization", label_vocab[1]);
    try std.testing.expectEqualStrings("person", label_vocab[2]);

    const batch_shape = try gliner2_data.buildSimpleBatchShapeSummary(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
        4,
    );
    try std.testing.expectEqual(@as(usize, 3), batch_shape.batch_size);
    try std.testing.expectEqual(@as(usize, 256), batch_shape.max_length);
    try std.testing.expectEqual(@as(usize, 3), batch_shape.num_entity_types);
    try std.testing.expectEqual(@as(usize, 244), batch_shape.max_words_per_sample);
    try std.testing.expectEqual(@as(usize, 1952), batch_shape.max_spans);
    try std.testing.expectEqual(@as(usize, 51), batch_shape.valid_spans);
    try std.testing.expectEqual(@as(usize, 8), batch_shape.positive_labels);

    const span_targets = try gliner2_data.summarizeSpanTargetsForExamples(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
    );
    try std.testing.expectEqual(@as(usize, 3), span_targets.num_examples);
    try std.testing.expectEqual(@as(usize, 51), span_targets.valid_spans);
    try std.testing.expectEqual(@as(usize, 8), span_targets.positive_labels);
}

test "GLiNER2 dataset readiness exposes non-toy gate failures" {
    const allocator = std.testing.allocator;
    const entity_types = [_][]const u8{ "person", "organization", "location" };

    var loaded = try gliner2_data.loadExamples(allocator, "testdata/gliner2_ner_smoke.jsonl", null);
    defer loaded.deinit();

    var smoke = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
        4,
        .{
            .min_examples = 3,
            .min_total_entities = 9,
            .min_unique_labels = 3,
            .min_target_entities = 9,
            .min_target_coverage_ratio = 1.0,
            .require_all_examples_with_target = true,
            .min_positive_span_labels = 8,
        },
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &smoke);
    try std.testing.expect(smoke.passed);
    try std.testing.expectEqual(@as(usize, 0), smoke.failed_reasons.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), smoke.target_coverage_ratio, 0.000001);
    try std.testing.expectEqual(@as(usize, 8), smoke.span_targets.positive_labels);

    var small_preview = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
        1,
        .{
            .min_examples = 3,
            .min_total_entities = 9,
            .min_unique_labels = 3,
            .min_target_entities = 9,
            .min_target_coverage_ratio = 1.0,
            .require_all_examples_with_target = true,
            .min_positive_span_labels = 8,
        },
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &small_preview);
    try std.testing.expect(small_preview.passed);
    try std.testing.expectEqual(@as(usize, 1), small_preview.batch_shape.batch_size);
    try std.testing.expectEqual(@as(usize, 8), small_preview.span_targets.positive_labels);

    var non_toy = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
        4,
        .{
            .min_examples = 100,
            .min_total_entities = 100,
            .min_unique_labels = 3,
            .min_target_entities = 100,
            .min_target_coverage_ratio = 1.0,
            .require_all_examples_with_target = true,
            .min_positive_span_labels = 100,
        },
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &non_toy);
    try std.testing.expect(!non_toy.passed);
    try std.testing.expect(containsReason(non_toy.failed_reasons, "min_examples"));
    try std.testing.expect(containsReason(non_toy.failed_reasons, "min_total_entities"));
    try std.testing.expect(containsReason(non_toy.failed_reasons, "min_target_entities"));
    try std.testing.expect(containsReason(non_toy.failed_reasons, "min_positive_span_labels"));
}

test "GLiNER2 label class capacity rejects collapsed label mappings" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadExamples(allocator, "testdata/gliner2_ner_smoke.jsonl", null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 3), try gliner2_data.validateLabelClassCapacity(allocator, loaded.examples, 4));
    try std.testing.expectError(error.TooManyEntityTypes, gliner2_data.validateLabelClassCapacity(allocator, loaded.examples, 3));
    try std.testing.expectError(error.InvalidNumClasses, gliner2_data.validateLabelClassCapacity(allocator, loaded.examples, 1));
}

fn containsReason(reasons: []const []const u8, needle: []const u8) bool {
    for (reasons) |reason| {
        if (std.mem.eql(u8, reason, needle)) return true;
    }
    return false;
}
