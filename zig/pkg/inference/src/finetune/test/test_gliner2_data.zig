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
const platform = @import("antfly_platform");
const gliner2_data = @import("inference_finetune_data").gliner2_data;

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
    try std.testing.expectEqual(@as(usize, 9), batch_shape.positive_labels);

    const span_targets = try gliner2_data.summarizeSpanTargetsForExamples(
        allocator,
        loaded.examples,
        &entity_types,
        256,
        8,
    );
    try std.testing.expectEqual(@as(usize, 3), span_targets.num_examples);
    try std.testing.expectEqual(@as(usize, 51), span_targets.valid_spans);
    try std.testing.expectEqual(@as(usize, 9), span_targets.positive_labels);
}

test "simple example encoded length matches the full batch attention mask" {
    const allocator = std.testing.allocator;
    const entity_types = [_][]const u8{ "person", "organization", "location" };
    var loaded = try gliner2_data.loadExamples(allocator, "testdata/gliner2_ner_smoke.jsonl", null);
    defer loaded.deinit();
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    tokenizer.use_gliner2_hf_prompt = true;

    const measured = try gliner2_data.measureSimpleExampleEncodedLength(
        allocator,
        &tokenizer,
        loaded.examples[0],
        &entity_types,
        128,
    );
    var batch = try gliner2_data.buildSimpleBatch(allocator, &tokenizer, loaded.examples[0..1], &entity_types, 128, 4, 1);
    defer batch.deinit();
    try std.testing.expectEqual(activeTokenCount(batch.attention_mask), measured);
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
            .min_positive_span_labels = 9,
        },
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &smoke);
    try std.testing.expect(smoke.passed);
    try std.testing.expectEqual(@as(usize, 0), smoke.failed_reasons.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), smoke.target_coverage_ratio, 0.000001);
    try std.testing.expectEqual(@as(usize, 9), smoke.span_targets.positive_labels);

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
            .min_positive_span_labels = 9,
        },
    );
    defer gliner2_data.freeDatasetReadinessSummary(allocator, &small_preview);
    try std.testing.expect(small_preview.passed);
    try std.testing.expectEqual(@as(usize, 1), small_preview.batch_shape.batch_size);
    try std.testing.expectEqual(@as(usize, 9), small_preview.span_targets.positive_labels);

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

test "GLiNER2 upstream all-task fixture exposes extractive span labels" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadExamples(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();

    const stats = try gliner2_data.computeStats(allocator, loaded.examples);
    try std.testing.expectEqual(@as(usize, 4), stats.num_examples);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), stats.avg_entities, 0.01);
    try std.testing.expectEqual(@as(usize, 10), stats.unique_labels);

    try std.testing.expectEqual(@as(usize, 3), loaded.examples[0].entities.len);
    try std.testing.expectEqual(@as(usize, 0), loaded.examples[1].entities.len);
    try std.testing.expectEqual(@as(usize, 4), loaded.examples[2].entities.len);
    try std.testing.expectEqual(@as(usize, 3), loaded.examples[3].entities.len);

    const label_vocab = try gliner2_data.buildLabelVocab(allocator, loaded.examples, null);
    defer {
        for (label_vocab) |label| allocator.free(label);
        allocator.free(label_vocab);
    }

    try std.testing.expect(containsLabel(label_vocab, "person"));
    try std.testing.expect(containsLabel(label_vocab, "product.name"));
    try std.testing.expect(containsLabel(label_vocab, "product.status"));
    try std.testing.expect(containsLabel(label_vocab, "founded.head"));
    try std.testing.expect(containsLabel(label_vocab, "founded.tail"));
}

test "GLiNER2 upstream all-task fixture preserves task-level labels" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 4), loaded.records.len);
    const stats = gliner2_data.computeUpstreamTaskStats(loaded.records);
    try std.testing.expectEqual(@as(usize, 4), stats.num_records);
    try std.testing.expectEqual(@as(usize, 1), stats.entity_tasks);
    try std.testing.expectEqual(@as(usize, 2), stats.classification_tasks);
    try std.testing.expectEqual(@as(usize, 1), stats.json_structure_tasks);
    try std.testing.expectEqual(@as(usize, 1), stats.relation_tasks);
    try std.testing.expectEqual(@as(usize, 4), stats.non_entity_task_annotations);
    try std.testing.expectEqual(@as(usize, 5), stats.classification_label_count);
    try std.testing.expectEqual(@as(usize, 2), stats.classification_true_label_count);
    try std.testing.expectEqual(@as(usize, 10), stats.span_field_annotations);

    const entities = loaded.records[0].tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.entities, entities.kind);
    try std.testing.expectEqualStrings("entities", entities.name);
    try std.testing.expectEqual(@as(usize, 3), entities.fields.len);
    try std.testing.expectEqualStrings("person", entities.fields[0].name);

    const cls = loaded.records[1].tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.classifications, cls.kind);
    try std.testing.expectEqualStrings("sentiment", cls.name);
    try std.testing.expectEqual(@as(usize, 2), cls.labels.len);
    try std.testing.expectEqualStrings("negative", cls.true_labels[0]);

    const structure = loaded.records[2].tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.json_structures, structure.kind);
    try std.testing.expectEqualStrings("product", structure.name);
    try std.testing.expectEqual(@as(usize, 4), structure.fields.len);
    try std.testing.expectEqualStrings("status", structure.fields[3].name);
    try std.testing.expectEqualStrings("available", structure.fields[3].value);
    try std.testing.expectEqual(@as(usize, 4), structure.fields[3].target_word_start.?);
    try std.testing.expectEqual(@as(usize, 4), structure.fields[3].target_word_end.?);
    try std.testing.expectEqual(@as(usize, 9), loaded.records[2].prefix_tokens.len);

    const relation = loaded.records[3].tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.relations, relation.kind);
    try std.testing.expectEqualStrings("founded", relation.name);
    try std.testing.expectEqual(@as(usize, 3), relation.fields.len);
    try std.testing.expectEqualStrings("head", relation.fields[0].name);
    try std.testing.expect(relation.fields[0].start != null);
}

test "grouped structures mirror upstream dedup empty and relation completeness rules" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_structure_edge_smoke.jsonl", null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 1), loaded.records.len);
    const tasks = loaded.records[0].tasks;
    try std.testing.expectEqual(@as(usize, 3), tasks.len);

    const item = tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.json_structures, item.kind);
    try std.testing.expectEqualStrings("item", item.name);
    try std.testing.expectEqual(@as(usize, 2), item.count);
    try std.testing.expectEqual(@as(usize, 2), item.schema_fields.len);
    try std.testing.expectEqualStrings("name", item.schema_fields[0]);
    try std.testing.expectEqualStrings("note", item.schema_fields[1]);
    try std.testing.expectEqual(@as(usize, 2), item.fields.len);
    try std.testing.expectEqual(@as(usize, 0), item.fields[0].instance);
    try std.testing.expectEqual(@as(usize, 1), item.fields[1].instance);

    const empty = tasks[1];
    try std.testing.expectEqualStrings("empty", empty.name);
    try std.testing.expectEqual(@as(usize, 0), empty.count);
    try std.testing.expectEqual(@as(usize, 2), empty.schema_fields.len);
    try std.testing.expectEqualStrings("x", empty.schema_fields[0]);
    try std.testing.expectEqualStrings("y", empty.schema_fields[1]);
    try std.testing.expectEqual(@as(usize, 0), empty.fields.len);

    const relation = tasks[2];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.relations, relation.kind);
    try std.testing.expectEqual(@as(usize, 2), relation.count);
    try std.testing.expectEqual(@as(usize, 2), relation.schema_fields.len);
    try std.testing.expectEqualStrings("head", relation.schema_fields[0]);
    try std.testing.expectEqualStrings("tail", relation.schema_fields[1]);
    try std.testing.expectEqual(@as(usize, 4), relation.fields.len);
    for (relation.fields) |field| try std.testing.expect(!std.mem.eql(u8, field.name, "extra"));
}

test "choice prefixes preserve raw occurrences and all matching selected positions" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_choice_structure_edge_smoke.jsonl", null);
    defer loaded.deinit();

    const record = loaded.records[0];
    try std.testing.expectEqual(@as(usize, 18), record.prefix_tokens.len);
    try std.testing.expectEqual(@as(usize, 2), record.tasks.len);
    const item = record.tasks[0];
    try std.testing.expectEqual(@as(usize, 1), item.count);
    try std.testing.expectEqual(@as(usize, 3), item.fields.len);
    try std.testing.expectEqualStrings("tag", item.fields[0].name);
    try std.testing.expectEqual(@as(usize, 0), item.fields[0].start.?);
    for (item.fields[1..]) |field| {
        try std.testing.expectEqualStrings("status", field.name);
        try std.testing.expect(field.target_word_start != null);
    }
    try std.testing.expectEqual(@as(usize, 4), item.fields[1].target_word_start.?);
    try std.testing.expectEqual(@as(usize, 13), item.fields[2].target_word_start.?);

    const empty = record.tasks[1];
    try std.testing.expectEqual(@as(usize, 0), empty.count);
    try std.testing.expectEqual(@as(usize, 0), empty.fields.len);
}

test "legacy entity rows become one upstream entities task" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_ner_smoke.jsonl", null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 1), loaded.records[0].tasks.len);
    const task = loaded.records[0].tasks[0];
    try std.testing.expectEqual(gliner2_data.UpstreamTaskKind.entities, task.kind);
    try std.testing.expectEqualStrings("entities", task.name);
    try std.testing.expectEqual(@as(usize, 1), task.count);
    try std.testing.expectEqual(@as(usize, 3), task.fields.len);
}

test "legacy entity rows reject Unicode annotations instead of using ASCII matching" {
    const allocator = std.testing.allocator;
    const data =
        \\{"text":"ÉCOLE","entities":[{"text":"école","label":"place","start":0,"end":6}]}
        \\
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "legacy-unicode.jsonl", .data = data });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/legacy-unicode.jsonl", .{tmp.sub_path});
    defer allocator.free(path);

    try std.testing.expectError(
        error.UnsupportedLegacyUnicodeAnnotation,
        gliner2_data.loadExamples(allocator, path, null),
    );
    try std.testing.expectError(
        error.UnsupportedLegacyUnicodeAnnotation,
        gliner2_data.loadTrainingRecords(allocator, path, null),
    );
}

test "entity schemas preserve empty labels as negative supervision" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_negative_entity_schema_smoke.jsonl", null);
    defer loaded.deinit();

    for (loaded.records) |record| {
        try std.testing.expectEqual(@as(usize, 1), record.tasks.len);
        try std.testing.expectEqual(@as(usize, 2), record.tasks[0].schema_fields.len);
    }
    try std.testing.expectEqual(@as(usize, 1), loaded.records[0].tasks[0].fields.len);
    try std.testing.expectEqual(@as(usize, 0), loaded.records[1].tasks[0].fields.len);

    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 2), labels.len);
    try std.testing.expectEqualStrings("person", labels[0]);
    try std.testing.expectEqualStrings("organization", labels[1]);

    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, loaded.records, labels, 256, 2, 2);
    defer batch.deinit();
    for (batch.entity_type_kind) |kind| try std.testing.expectEqual(@as(i32, 2), kind);
    const organization = labelIndex(labels, "organization").?;
    var organization_positives: usize = 0;
    for (0..batch.batch_size * batch.max_spans) |span| {
        if (batch.span_labels[span * labels.len + organization] > 0) organization_positives += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), organization_positives);
}

test "upstream annotations require token-sequence matches" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.AnnotationNotFound,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_substring_annotation_smoke.jsonl", null),
    );
}

test "classification schemas fail closed on invalid labels and duplicate tasks" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.UnknownClassificationTrueLabel,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_invalid_classification_smoke.jsonl", null),
    );
    try std.testing.expectError(
        error.DuplicateClassificationTaskName,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_duplicate_classification_smoke.jsonl", null),
    );
}

test "upstream schema conditioning metadata is parsed and encoded" {
    const allocator = std.testing.allocator;
    const data =
        \\{"input":"Alice built Acme.","output":{"entities":{"person":["Alice"]},"entity_descriptions":{"person":"a human"},"json_structures":[{"product":{"name":"Acme"}}],"json_descriptions":{"product":{"name":"a company"}},"classifications":[{"task":"sentiment","labels":["positive","negative"],"true_label":["positive"],"multi_label":true,"prompt":"Choose a sentiment.","examples":[["Great.","positive"],["Bad.","negative"]],"label_descriptions":{"positive":"favorable","negative":"unfavorable"}}]}}
        \\
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "conditioned.jsonl", .data = data });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/conditioned.jsonl", .{tmp.sub_path});
    defer allocator.free(path);

    var loaded_examples = try gliner2_data.loadExamples(allocator, path, null);
    defer loaded_examples.deinit();
    try std.testing.expectEqual(@as(usize, 1), loaded_examples.examples.len);

    var loaded = try gliner2_data.loadTrainingRecords(allocator, path, null);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 3), loaded.records[0].tasks.len);
    const json_task = loaded.records[0].tasks[0];
    const entity_task = loaded.records[0].tasks[1];
    const classification_task = loaded.records[0].tasks[2];
    try std.testing.expectEqualStrings("name", json_task.label_descriptions[0].label);
    try std.testing.expectEqualStrings("a company", json_task.label_descriptions[0].description);
    try std.testing.expectEqualStrings("person", entity_task.label_descriptions[0].label);
    try std.testing.expectEqualStrings("a human", entity_task.label_descriptions[0].description);
    try std.testing.expectEqualStrings("Choose a sentiment.", classification_task.prompt.?);
    try std.testing.expect(classification_task.multi_label);
    try std.testing.expectEqual(@as(usize, 2), classification_task.label_descriptions.len);
    try std.testing.expectEqual(@as(usize, 2), classification_task.examples.len);
    try std.testing.expectEqualStrings("Great.", classification_task.examples[0].input);
    try std.testing.expectEqualStrings("positive", classification_task.examples[0].output);

    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, loaded.records, labels, 1024, 2, 1);
    defer batch.deinit();
    try std.testing.expectEqual(@as(i32, 2), batch.schema_special_counts[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.schema_special_counts[1]);
    try std.testing.expectEqual(@as(i32, 3), batch.schema_special_counts[2]);
    var description_tokens: usize = 0;
    var example_tokens: usize = 0;
    var output_tokens: usize = 0;
    for (batch.input_ids) |token_id| {
        if (token_id == tokenizer.description_token_id) description_tokens += 1;
        if (token_id == tokenizer.example_token_id) example_tokens += 1;
        if (token_id == tokenizer.output_token_id) output_tokens += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), description_tokens);
    try std.testing.expectEqual(@as(usize, 2), example_tokens);
    try std.testing.expectEqual(@as(usize, 2), output_tokens);
}

test "malformed upstream schema conditioning metadata fails closed in both loaders" {
    const allocator = std.testing.allocator;
    const cases = [_]struct { data: []const u8, expected: anyerror }{
        .{
            .data =
            \\{"input":"Alice.","output":{"entities":{"person":["Alice"]},"entity_descriptions":[]}}
            \\
            ,
            .expected = error.InvalidEntityDescriptions,
        },
        .{
            .data =
            \\{"input":"Acme.","output":{"json_structures":[{"product":{"name":"Acme"}}],"json_descriptions":{"product":"company"}}}
            \\
            ,
            .expected = error.InvalidJsonDescriptions,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"prompt":1}]}}
            \\
            ,
            .expected = error.InvalidClassificationPrompt,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"examples":[["Bad."]]}]}}
            \\
            ,
            .expected = error.InvalidClassificationExamples,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"label_descriptions":{"positive":1}}]}}
            \\
            ,
            .expected = error.InvalidClassificationLabelDescriptions,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"label_descriptions":{"negative":"unfavorable"}}]}}
            \\
            ,
            .expected = error.UnknownLabelDescription,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"examples":[["Bad.","negative"]]}]}}
            \\
            ,
            .expected = error.UnknownClassificationExampleLabel,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":["positive"],"true_label":["positive"],"multi_label":"yes"}]}}
            \\
            ,
            .expected = error.InvalidClassificationMultiLabel,
        },
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/invalid-conditioning.jsonl", .{tmp.sub_path});
    defer allocator.free(path);
    for (cases) |case| {
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "invalid-conditioning.jsonl", .data = case.data });
        try std.testing.expectError(case.expected, gliner2_data.loadExamples(allocator, path, null));
        try std.testing.expectError(case.expected, gliner2_data.loadTrainingRecords(allocator, path, null));
    }
}

test "schema identifiers reject surrounding ASCII whitespace before target construction" {
    const allocator = std.testing.allocator;
    const cases = [_]struct { data: []const u8, expected: anyerror }{
        .{
            .data =
            \\{"input":"Alice.","output":{"entities":{" person":["Alice"]}}}
            \\
            ,
            .expected = error.InvalidEntitySchema,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment ","labels":["positive"],"true_label":["positive"]}]}}
            \\
            ,
            .expected = error.InvalidClassificationSchema,
        },
        .{
            .data =
            \\{"input":"Good.","output":{"classifications":[{"task":"sentiment","labels":[" positive"],"true_label":[" positive"]}]}}
            \\
            ,
            .expected = error.InvalidClassificationSchema,
        },
        .{
            .data =
            \\{"input":"Acme.","output":{"json_structures":[{" product":{"name":"Acme"}}]}}
            \\
            ,
            .expected = error.InvalidStructureSchema,
        },
        .{
            .data =
            \\{"input":"Acme.","output":{"json_structures":[{"product":{"name ":"Acme"}}]}}
            \\
            ,
            .expected = error.InvalidStructureSchema,
        },
        .{
            .data =
            \\{"input":"The shoe is red.","output":{"json_structures":[{"product":{"color":{"value":"red ","choices":["red ","blue"]}}}]}}
            \\
            ,
            .expected = error.InvalidChoiceSchema,
        },
        .{
            .data =
            \\{"text":"Alice.","entities":[{"text":"Alice","label":"person ","start":0,"end":5}]}
            \\
            ,
            .expected = error.InvalidEntitySchema,
        },
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/whitespace.jsonl", .{tmp.sub_path});
    defer allocator.free(path);
    for (cases) |case| {
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "whitespace.jsonl", .data = case.data });
        try std.testing.expectError(case.expected, gliner2_data.loadExamples(allocator, path, null));
        try std.testing.expectError(case.expected, gliner2_data.loadTrainingRecords(allocator, path, null));
    }
}

test "choice schemas reject selections outside the declared choices" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.ChoiceValueNotInChoices,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_invalid_choice_smoke.jsonl", null),
    );
}

test "upstream records reject empty tasks and malformed structures" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.NoGliner2Tasks,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_empty_task_smoke.jsonl", null),
    );
    try std.testing.expectError(
        error.InvalidStructureOccurrence,
        gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_invalid_structure_smoke.jsonl", null),
    );
}

test "GLiNER2 upstream all-task label vocab includes classification and structured labels" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();

    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }

    const sentiment_positive = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "sentiment", "positive");
    defer allocator.free(sentiment_positive);
    const sentiment_negative = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "sentiment", "negative");
    defer allocator.free(sentiment_negative);
    const priority_urgent = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "priority", "urgent");
    defer allocator.free(priority_urgent);
    const product_name = try gliner2_data.upstreamTaskFieldKey(allocator, .json_structures, "product", "name");
    defer allocator.free(product_name);
    const product_price = try gliner2_data.upstreamTaskFieldKey(allocator, .json_structures, "product", "price");
    defer allocator.free(product_price);
    const product_color = try gliner2_data.upstreamTaskFieldKey(allocator, .json_structures, "product", "color");
    defer allocator.free(product_color);
    const product_status = try gliner2_data.upstreamTaskFieldKey(allocator, .json_structures, "product", "status");
    defer allocator.free(product_status);
    const founded_head = try gliner2_data.upstreamTaskFieldKey(allocator, .relations, "founded", "head");
    defer allocator.free(founded_head);
    const founded_tail = try gliner2_data.upstreamTaskFieldKey(allocator, .relations, "founded", "tail");
    defer allocator.free(founded_tail);

    try std.testing.expect(containsLabel(labels, "person"));
    try std.testing.expect(containsLabel(labels, sentiment_negative));
    try std.testing.expect(containsLabel(labels, priority_urgent));
    try std.testing.expect(containsLabel(labels, product_name));
    try std.testing.expect(containsLabel(labels, product_status));
    try std.testing.expect(containsLabel(labels, founded_head));
    try std.testing.expect(containsLabel(labels, founded_tail));
    try std.testing.expectEqualStrings("person", labels[0]);
    try std.testing.expectEqualStrings("organization", labels[1]);
    try std.testing.expectEqualStrings("location", labels[2]);
    try std.testing.expectEqualStrings(sentiment_positive, labels[3]);
    try std.testing.expectEqualStrings(sentiment_negative, labels[4]);
    try std.testing.expectEqualStrings(product_name, labels[8]);
    try std.testing.expectEqualStrings(product_price, labels[9]);
    try std.testing.expectEqualStrings(product_color, labels[10]);
    try std.testing.expectEqualStrings(product_status, labels[11]);
}

test "GLiNER2 upstream task batch carries schema markers and structured span labels" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();

    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }

    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, loaded.records, labels, 256, 4, loaded.records.len);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 4), batch.batch_size);
    try std.testing.expectEqual(@as(usize, labels.len), batch.num_entity_types);
    try std.testing.expectEqual(@as(i32, 1), batch.schema_counts[0]);
    try std.testing.expectEqual(@as(i32, 2), batch.schema_counts[1]);
    try std.testing.expectEqual(@as(i32, 1), batch.schema_counts[2]);
    try std.testing.expectEqual(@as(i32, 1), batch.schema_counts[3]);
    try std.testing.expectEqual(gliner2_data.upstreamTaskTypeId(.entities), batch.task_type_ids[0]);
    try std.testing.expectEqual(gliner2_data.upstreamTaskTypeId(.classifications), batch.task_type_ids[batch.max_schemas]);
    try std.testing.expect(batch.schema_special_counts[0] > 0);
    try std.testing.expect(batch.schema_special_positions[0] >= 0);

    const sentiment_negative = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "sentiment", "negative");
    defer allocator.free(sentiment_negative);
    const priority_urgent = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "priority", "urgent");
    defer allocator.free(priority_urgent);
    const product_name = try gliner2_data.upstreamTaskFieldKey(allocator, .json_structures, "product", "name");
    defer allocator.free(product_name);
    const negative_idx = labelIndex(labels, sentiment_negative).?;
    const urgent_idx = labelIndex(labels, priority_urgent).?;
    const product_name_idx = labelIndex(labels, product_name).?;
    try std.testing.expect(batch.e_token_positions[1 * labels.len + negative_idx] >= 0);
    try std.testing.expect(batch.e_token_positions[1 * labels.len + urgent_idx] >= 0);
    try std.testing.expect(batch.e_token_positions[2 * labels.len + product_name_idx] >= 0);
    try std.testing.expectEqual(@as(i32, 2), batch.entity_type_kind[2 * labels.len + product_name_idx]);
}

test "upstream record encoded length matches the full batch attention mask" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    const record = loaded.records[1];
    const measured = try gliner2_data.measureUpstreamRecordEncodedLength(allocator, &tokenizer, record, 256);
    const slots = try gliner2_data.maxUpstreamRecordLabelSlots(allocator, loaded.records[1..2]);
    var batch = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
        allocator,
        &tokenizer,
        loaded.records[1..2],
        slots,
        256,
        4,
        1,
    );
    defer batch.deinit();
    try std.testing.expectEqual(activeTokenCount(batch.attention_mask), measured);
}

test "GLiNER2 upstream batches use bounded per-sample schema slots" {
    const allocator = std.testing.allocator;
    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_all_task_smoke.jsonl", null);
    defer loaded.deinit();

    const dataset_labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (dataset_labels) |label| allocator.free(label);
        allocator.free(dataset_labels);
    }
    const entity_labels = try gliner2_data.buildUpstreamEntityLabelVocab(allocator, loaded.records, null);
    defer {
        for (entity_labels) |label| allocator.free(label);
        allocator.free(entity_labels);
    }
    const slots = try gliner2_data.maxUpstreamRecordLabelSlots(allocator, loaded.records);
    try std.testing.expect(slots < dataset_labels.len);
    try std.testing.expectEqual(@as(usize, 3), entity_labels.len);

    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
        allocator,
        &tokenizer,
        loaded.records,
        slots,
        256,
        4,
        loaded.records.len,
    );
    defer batch.deinit();

    try std.testing.expectEqual(slots, batch.num_entity_types);
    for (0..batch.batch_size) |sample_idx| {
        const local = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records[sample_idx .. sample_idx + 1], null);
        defer {
            for (local) |label| allocator.free(label);
            allocator.free(local);
        }
        for (0..local.len) |slot| try std.testing.expect(batch.e_token_positions[sample_idx * slots + slot] >= 0);
        for (local.len..slots) |slot| {
            try std.testing.expectEqual(@as(i32, -1), batch.e_token_positions[sample_idx * slots + slot]);
            try std.testing.expectEqual(@as(i32, 0), batch.entity_type_kind[sample_idx * slots + slot]);
        }
    }
    try std.testing.expectError(
        error.TooManySchemaSlots,
        gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
            allocator,
            &tokenizer,
            loaded.records,
            slots - 1,
            256,
            4,
            loaded.records.len,
        ),
    );
}

test "heterogeneous local slots keep raw entity positive counts aligned" {
    const allocator = std.testing.allocator;
    const first_fields = [_]gliner2_data.UpstreamField{
        .{ .name = "person", .value = "Alice" },
        .{ .name = "person", .value = "Bob" },
    };
    const second_fields = [_]gliner2_data.UpstreamField{
        .{ .name = "organization", .value = "Acme" },
        .{ .name = "person", .value = "Carol" },
    };
    const first_tasks = [_]gliner2_data.UpstreamTask{.{
        .kind = .entities,
        .name = "entities",
        .schema_fields = &.{"person"},
        .fields = &first_fields,
    }};
    const second_tasks = [_]gliner2_data.UpstreamTask{.{
        .kind = .entities,
        .name = "entities",
        .schema_fields = &.{ "organization", "person" },
        .fields = &second_fields,
    }};
    const records = [_]gliner2_data.UpstreamRecord{
        .{ .text = "Alice met Bob", .tasks = &first_tasks },
        .{ .text = "Carol joined Acme", .tasks = &second_tasks },
    };
    // Record-local slot 0 means "person" in the first record and
    // "organization" in the second. Manifest counts follow raw label order.
    const manifest_labels = [_][]const u8{ "organization", "person", "unused" };
    const counts = try gliner2_data.countUpstreamEntityLabelPositives(allocator, &records, &manifest_labels);
    defer allocator.free(counts);

    try std.testing.expectEqualSlices(u64, &.{ 1, 3, 0 }, counts);
}

test "classification schemas keep duplicate label names in distinct contextual slots" {
    const allocator = std.testing.allocator;
    const first = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "approved",
        .labels = &.{ "yes", "no" },
        .true_labels = &.{"yes"},
    };
    const second = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "verified",
        .labels = &.{ "yes", "no" },
        .true_labels = &.{"no"},
    };
    const tasks = [_]gliner2_data.UpstreamTask{ first, second };
    const records = [_]gliner2_data.UpstreamRecord{.{ .text = "The request was reviewed.", .tasks = &tasks }};
    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, &records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 4), labels.len);
    const approved_yes_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "approved", "yes");
    defer allocator.free(approved_yes_key);
    const verified_yes_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "verified", "yes");
    defer allocator.free(verified_yes_key);
    const approved_yes = labelIndex(labels, approved_yes_key).?;
    const verified_yes = labelIndex(labels, verified_yes_key).?;
    try std.testing.expect(approved_yes != verified_yes);

    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &records, labels, 256, 2, 1);
    defer batch.deinit();
    const approved_pos = batch.e_token_positions[approved_yes];
    const verified_pos = batch.e_token_positions[verified_yes];
    try std.testing.expect(approved_pos >= 0);
    try std.testing.expect(verified_pos >= 0);
    try std.testing.expect(approved_pos != verified_pos);
}

test "task field identities are collision-free across kinds and dotted names" {
    const allocator = std.testing.allocator;
    const json_fields = [_]gliner2_data.UpstreamField{.{ .name = "value", .value = "yes" }};
    const dotted_a_fields = [_]gliner2_data.UpstreamField{.{ .name = "c", .value = "yes" }};
    const dotted_b_fields = [_]gliner2_data.UpstreamField{.{ .name = "b.c", .value = "yes" }};
    const tasks = [_]gliner2_data.UpstreamTask{
        .{ .kind = .classifications, .name = "shared", .labels = &.{"value"} },
        .{ .kind = .json_structures, .name = "shared", .fields = &json_fields, .count = 1 },
        .{ .kind = .relations, .name = "a.b", .fields = &dotted_a_fields, .count = 1 },
        .{ .kind = .relations, .name = "a", .fields = &dotted_b_fields, .count = 1 },
    };
    const records = [_]gliner2_data.UpstreamRecord{.{ .text = "yes", .tasks = &tasks }};
    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, &records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 4), labels.len);
    for (labels, 0..) |label, idx| {
        for (labels[0..idx]) |previous| try std.testing.expect(!std.mem.eql(u8, label, previous));
    }
}

test "task supervision fails closed when schema rows exceed span rows" {
    const allocator = std.testing.allocator;
    const first = gliner2_data.UpstreamTask{ .kind = .classifications, .name = "a", .labels = &.{"yes"} };
    const second = gliner2_data.UpstreamTask{ .kind = .classifications, .name = "b", .labels = &.{"no"} };
    const tasks = [_]gliner2_data.UpstreamTask{ first, second };
    const records = [_]gliner2_data.UpstreamRecord{.{ .text = "x", .tasks = &tasks }};
    const first_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "a", "yes");
    defer allocator.free(first_key);
    const second_key = try gliner2_data.upstreamTaskFieldKey(allocator, .classifications, "b", "no");
    defer allocator.free(second_key);
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    try std.testing.expectError(
        error.TooManySchemaTasks,
        gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &records, &.{ first_key, second_key }, 1, 1, 1),
    );
}

test "GLiNER2 negative fixture is explicit and unresolved annotations fail" {
    const allocator = std.testing.allocator;

    var loaded = try gliner2_data.loadTrainingRecords(allocator, "testdata/gliner2_negative_smoke.jsonl", null);
    defer loaded.deinit();
    const stats = gliner2_data.computeUpstreamTaskStats(loaded.records);
    try std.testing.expectEqual(@as(usize, 3), loaded.records.len);
    try std.testing.expectEqual(@as(usize, 3), stats.classification_tasks);
    try std.testing.expectEqual(@as(usize, 0), stats.classification_true_label_count);
    try std.testing.expectEqual(@as(usize, 0), stats.span_field_annotations);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "invalid.jsonl",
        .data =
        \\{"input":"Mary visited London.","output":{"entities":{"person":["John"]}}}
        \\
        ,
    });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/invalid.jsonl", .{tmp.sub_path});
    defer allocator.free(path);
    try std.testing.expectError(
        error.AnnotationNotFound,
        gliner2_data.loadTrainingRecords(allocator, path, null),
    );
}

test "GLiNER2 upstream data labels every mention and appends terminal punctuation" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "repeated.jsonl",
        .data =
        \\{"input":"Café met café","output":{"entities":{"place":["café"]}}}
        \\
        ,
    });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/repeated.jsonl", .{tmp.sub_path});
    defer allocator.free(path);

    var loaded = try gliner2_data.loadTrainingRecords(allocator, path, null);
    defer loaded.deinit();
    const fields = loaded.records[0].tasks[0].fields;
    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqual(@as(?usize, 0), fields[0].start);
    try std.testing.expectEqual(@as(?usize, 10), fields[1].start);

    const labels = try gliner2_data.buildUpstreamTaskLabelVocab(allocator, loaded.records, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, loaded.records, labels, 64, 1, 1);
    defer batch.deinit();

    try std.testing.expectEqual(@as(i32, 4), batch.text_word_counts[0]);
    var positives: usize = 0;
    for (batch.span_labels) |label| if (label > 0.0) {
        positives += 1;
    };
    try std.testing.expectEqual(@as(usize, 2), positives);
}

test "GLiNER2 upstream entity decoding preserves punctuation-aware boundaries" {
    const allocator = std.testing.allocator;
    const text = "Alice, joined Acme.";
    const entity_types = [_][]const u8{ "person", "organization" };
    const task = gliner2_data.UpstreamTask{
        .kind = .entities,
        .name = "entities",
        .schema_fields = &entity_types,
        .count = 1,
    };
    const record = gliner2_data.UpstreamRecord{ .text = text, .tasks = &.{task} };
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    var batch = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
        allocator,
        &tokenizer,
        &.{record},
        entity_types.len,
        128,
        2,
        1,
    );
    defer batch.deinit();

    try std.testing.expect(batch.uses_upstream_word_splitting);
    try std.testing.expectEqual(@as(i32, 5), batch.text_word_counts[0]);

    const span_scores = try allocator.alloc(f32, batch.max_spans * batch.num_entity_types);
    defer allocator.free(span_scores);
    @memset(span_scores, 0.0);
    const acme_span_idx = 3 * 2;
    span_scores[acme_span_idx * batch.num_entity_types + 1] = 0.9;

    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    const predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &batch,
        &examples,
        &entity_types,
        span_scores,
        0.5,
    );
    defer allocator.free(predictions);

    try std.testing.expectEqual(@as(usize, 1), predictions.len);
    try std.testing.expectEqualStrings("organization", predictions[0].label);
    try std.testing.expectEqualStrings("Acme", predictions[0].text);
    try std.testing.expectEqual(@as(usize, 14), predictions[0].start);
    try std.testing.expectEqual(@as(usize, 18), predictions[0].end);
}

test "GLiNER2 upstream preprocessing matches Python 3.12 Unicode word categories" {
    const allocator = std.testing.allocator;
    const task = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{"positive"},
    };
    const key = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, task.labels[0]);
    defer allocator.free(key);
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    try std.testing.expectEqualStrings("15.0.0", gliner2_data.upstream_unicode_version);
    const cases = [_]struct { text: []const u8, words: i32 }{
        .{ .text = "École", .words = 2 }, // word + appended period
        .{ .text = "école—paris", .words = 4 },
        .{ .text = "hello 😀", .words = 3 },
        .{ .text = "Москва 東京", .words = 3 },
        .{ .text = "e\u{0301}", .words = 3 }, // combining mark is `\S`, not `\w`
        .{ .text = "a\u{00a0}b.", .words = 3 },
    };
    for (cases) |case| {
        const record = gliner2_data.UpstreamRecord{ .text = case.text, .tasks = &.{task} };
        var batch = try gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &.{record}, &.{key}, 256, 1, 1);
        defer batch.deinit();
        try std.testing.expectEqual(case.words, batch.text_word_counts[0]);
    }
}

test "GLiNER2 upstream Unicode splitter preserves byte boundaries and special branches" {
    const allocator = std.testing.allocator;
    const text = "HTTPS://Exämple.com/a\u{00a0}USER+tag@Example.COM42 @Name @é foo-bar foo--bar.";
    const expected = [_][]const u8{
        "HTTPS://Exämple.com/a",
        "USER+tag@Example.COM",
        "42",
        "@Name",
        "@",
        "é",
        "foo-bar",
        "foo",
        "-",
        "-",
        "bar",
        ".",
    };
    const boundaries = try gliner2_data.upstreamWordBoundariesAlloc(allocator, text);
    defer allocator.free(boundaries);
    try std.testing.expectEqual(expected.len, boundaries.len);
    for (expected, boundaries) |token, boundary| {
        try std.testing.expectEqualStrings(token, text[boundary[0]..boundary[1]]);
    }

    const ignore_case = "@ı @ſ a@ı.ſſ HTTPſ://example.com";
    const ignore_case_expected = [_][]const u8{ "@ı", "@ſ", "a@ı.ſſ", "HTTPſ://example.com" };
    const ignore_case_boundaries = try gliner2_data.upstreamWordBoundariesAlloc(allocator, ignore_case);
    defer allocator.free(ignore_case_boundaries);
    try std.testing.expectEqual(ignore_case_expected.len, ignore_case_boundaries.len);
    for (ignore_case_expected, ignore_case_boundaries) |token, boundary| {
        try std.testing.expectEqualStrings(token, ignore_case[boundary[0]..boundary[1]]);
    }

    const mixed = "école—東京 😀";
    const mixed_boundaries = try gliner2_data.upstreamWordBoundariesAlloc(allocator, mixed);
    defer allocator.free(mixed_boundaries);
    try std.testing.expectEqualSlices([2]usize, &.{ .{ 0, 6 }, .{ 6, 9 }, .{ 9, 15 }, .{ 16, 20 } }, mixed_boundaries);
}

test "GLiNER2 upstream annotations use Unicode lowercase matching" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "unicode.jsonl",
        .data =
        \\{"input":"ÉCOLE met Москва","output":{"entities":{"place":["école"],"city":["москва"]}}}
        \\
        ,
    });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/unicode.jsonl", .{tmp.sub_path});
    defer allocator.free(path);

    var loaded = try gliner2_data.loadTrainingRecords(allocator, path, null);
    defer loaded.deinit();
    const fields = loaded.records[0].tasks[0].fields;
    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqual(@as(?usize, 0), fields[0].start);
    try std.testing.expectEqual(@as(?usize, 6), fields[0].end);
    try std.testing.expectEqual(@as(?usize, 11), fields[1].start);
    try std.testing.expectEqual(@as(?usize, 23), fields[1].end);
}

test "GLiNER2 upstream preprocessing rejects invalid UTF-8 and lowercase expansion" {
    const allocator = std.testing.allocator;
    const task = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{"positive"},
    };
    const key = try gliner2_data.upstreamTaskFieldKey(allocator, task.kind, task.name, task.labels[0]);
    defer allocator.free(key);
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    const invalid = [_]u8{ 0xc3, 0x28 };
    const cases = [_]struct { text: []const u8, expected: anyerror }{
        .{ .text = &invalid, .expected = error.InvalidUtf8 },
        .{ .text = "İstanbul", .expected = error.UnsupportedUnicodeLowerExpansion },
    };
    for (cases) |case| {
        const record = gliner2_data.UpstreamRecord{ .text = case.text, .tasks = &.{task} };
        try std.testing.expectError(
            case.expected,
            gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &.{record}, &.{key}, 64, 1, 1),
        );
    }
}

test "GLiNER2 upstream preprocessing rejects unsupported Unicode in schema conditioning" {
    const allocator = std.testing.allocator;
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    const tasks = [_]gliner2_data.UpstreamTask{
        .{ .kind = .classifications, .name = "sentiment😀", .labels = &.{"positive"} },
        .{ .kind = .classifications, .name = "sentiment", .labels = &.{"positive"}, .prompt = "choose 😀" },
        .{ .kind = .classifications, .name = "sentiment", .labels = &.{"positive😀"} },
        .{
            .kind = .classifications,
            .name = "sentiment",
            .labels = &.{"positive"},
            .label_descriptions = &.{.{ .label = "positive", .description = "good 😀" }},
        },
        .{
            .kind = .classifications,
            .name = "sentiment",
            .labels = &.{"positive"},
            .examples = &.{.{ .input = "great 😀", .output = "positive" }},
        },
        .{ .kind = .entities, .name = "entities", .schema_fields = &.{"person😀"} },
    };
    for (tasks) |task| {
        const record = gliner2_data.UpstreamRecord{ .text = "fine.", .tasks = &.{task} };
        try std.testing.expectError(
            error.UnsupportedUnicodePreprocessing,
            gliner2_data.buildUpstreamTaskBatchWithLocalSlots(allocator, &tokenizer, &.{record}, 4, 128, 1, 1),
        );
    }
}

test "GLiNER2 pinned normalizer invariant subset fails closed" {
    const accepted = [_][]const u8{ "Москва", "東京", "😀", "é", "Москва 東京 😀" };
    for (accepted) |text| try gliner2_data.validatePinnedFastinoNormalizerInvariant(text);

    const rejected = [_][]const u8{
        "①",
        "Ａ",
        "ﬁ",
        "e\u{0301}",
        "a\tb",
        " a",
        "a ",
        "a  b",
        "👩‍💻",
    };
    for (rejected) |text| {
        try std.testing.expectError(
            error.UnsupportedTokenizerNormalization,
            gliner2_data.validatePinnedFastinoNormalizerInvariant(text),
        );
    }
    try std.testing.expectError(
        error.InvalidUtf8,
        gliner2_data.validatePinnedFastinoNormalizerInvariant(&.{ 0xc3, 0x28 }),
    );
}

test "GLiNER2 pinned tokenizer rejects normalization-changing document text" {
    const allocator = std.testing.allocator;
    const model_dir = platform.env.getenv("TERMITE_GLINER2_REAL_MODEL_DIR") orelse return error.SkipZigTest;
    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, model_dir);
    defer tokenizer.deinit(allocator);

    try tokenizer.validateNormalizerInvariant("Москва 東京 😀");
    for ([_][]const u8{ "①", "Ａ", "ﬁ" }) |text| {
        try std.testing.expectError(error.UnsupportedTokenizerNormalization, tokenizer.validateNormalizerInvariant(text));
    }

    const task = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{"positive"},
    };
    const supported = gliner2_data.UpstreamRecord{ .text = "Москва 東京 😀.", .tasks = &.{task} };
    var batch = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(allocator, &tokenizer, &.{supported}, 1, 128, 1, 1);
    batch.deinit();

    for ([_][]const u8{ "①", "Ａ", "ﬁ" }) |text| {
        const record = gliner2_data.UpstreamRecord{ .text = text, .tasks = &.{task} };
        try std.testing.expectError(
            error.UnsupportedTokenizerNormalization,
            gliner2_data.buildUpstreamTaskBatchWithLocalSlots(allocator, &tokenizer, &.{record}, 1, 128, 1, 1),
        );
    }
}

test "GLiNER2 upstream batch rejects truncated inputs and unrepresentable spans" {
    const allocator = std.testing.allocator;
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    const classification = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{ "positive", "negative" },
    };
    const wide_schema = gliner2_data.UpstreamRecord{
        .text = "fine.",
        .tasks = &.{classification},
    };
    try std.testing.expectError(
        error.SequenceTooLong,
        gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &.{wide_schema}, &.{ "positive", "negative" }, 4, 1, 1),
    );

    const long_text = gliner2_data.UpstreamRecord{
        .text = "one two three four",
        .tasks = &.{},
    };
    try std.testing.expectError(
        error.SequenceTooLong,
        gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &.{long_text}, &.{}, 4, 1, 1),
    );

    const field = gliner2_data.UpstreamField{
        .name = "place",
        .value = "New York",
        .start = 0,
        .end = 8,
    };
    const entity_task = gliner2_data.UpstreamTask{
        .kind = .entities,
        .name = "entities",
        .fields = &.{field},
        .count = 1,
    };
    const wide_mention = gliner2_data.UpstreamRecord{
        .text = "New York.",
        .tasks = &.{entity_task},
    };
    try std.testing.expectError(
        error.AnnotationOutsideBatch,
        gliner2_data.buildUpstreamTaskBatch(allocator, &tokenizer, &.{wide_mention}, &.{"place"}, 128, 1, 1),
    );
}

test "GLiNER2 encoded-length preflight matches full simple and upstream batches" {
    const allocator = std.testing.allocator;
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);

    const simple = gliner2_data.Example{ .text = "Alice joined Acme.", .entities = &.{} };
    const entity_types = [_][]const u8{ "person", "organization" };
    var simple_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        &tokenizer,
        &.{simple},
        &entity_types,
        64,
        4,
        1,
    );
    defer simple_batch.deinit();
    try std.testing.expectEqual(
        activeTokenCount(simple_batch.attention_mask),
        try gliner2_data.measureSimpleExampleEncodedLength(allocator, &tokenizer, simple, &entity_types, 64),
    );

    const classification = gliner2_data.UpstreamTask{
        .kind = .classifications,
        .name = "sentiment",
        .labels = &.{ "positive", "negative" },
        .prompt = "choose a label",
        .label_descriptions = &.{.{ .label = "positive", .description = "favorable" }},
        .examples = &.{.{ .input = "great", .output = "positive" }},
    };
    const upstream = gliner2_data.UpstreamRecord{ .text = "fine.", .tasks = &.{classification} };
    var upstream_batch = try gliner2_data.buildUpstreamTaskBatchWithLocalSlots(
        allocator,
        &tokenizer,
        &.{upstream},
        2,
        256,
        4,
        1,
    );
    defer upstream_batch.deinit();
    try std.testing.expectEqual(
        activeTokenCount(upstream_batch.attention_mask),
        try gliner2_data.measureUpstreamRecordEncodedLength(allocator, &tokenizer, upstream, 256),
    );
}

test "GLiNER2 fitted simple length preserves words with many schema labels" {
    const allocator = std.testing.allocator;
    var tokenizer = try gliner2_data.Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    const entity_types = [_][]const u8{
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    };
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..100) |_| try text.appendSlice(allocator, "word ");
    const example = gliner2_data.Example{ .text = text.items, .entities = &.{} };

    const fitted_length = try gliner2_data.measureSimpleExampleRequiredLength(
        allocator,
        &tokenizer,
        example,
        &entity_types,
        256,
    );
    try std.testing.expect(fitted_length < 256);
    var full = try gliner2_data.buildSimpleBatch(allocator, &tokenizer, &.{example}, &entity_types, 256, 4, 1);
    defer full.deinit();
    var fitted = try gliner2_data.buildSimpleBatch(allocator, &tokenizer, &.{example}, &entity_types, fitted_length, 4, 1);
    defer fitted.deinit();
    var full_max_word: i32 = 0;
    for (full.words_mask) |word| full_max_word = @max(full_max_word, word);
    var fitted_max_word: i32 = 0;
    for (fitted.words_mask) |word| fitted_max_word = @max(fitted_max_word, word);
    try std.testing.expectEqual(@as(i32, 100), full_max_word);
    try std.testing.expectEqual(full_max_word, fitted_max_word);
    try std.testing.expectEqual(activeTokenCount(full.attention_mask), activeTokenCount(fitted.attention_mask));
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

fn activeTokenCount(attention_mask: []const i32) usize {
    var count: usize = 0;
    for (attention_mask) |active| count += @intFromBool(active != 0);
    return count;
}

fn containsLabel(labels: []const []const u8, needle: []const u8) bool {
    for (labels) |label| {
        if (std.mem.eql(u8, label, needle)) return true;
    }
    return false;
}

fn labelIndex(labels: []const []const u8, needle: []const u8) ?usize {
    for (labels, 0..) |label, idx| {
        if (std.mem.eql(u8, label, needle)) return idx;
    }
    return null;
}
