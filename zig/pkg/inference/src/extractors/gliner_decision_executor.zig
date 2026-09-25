// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GLiNER2.5-Decide classification execution for extraction schema v2.
const std = @import("std");
const wire = @import("extraction_v2.zig");
const gliner = @import("../pipelines/gliner.zig");
const presentation = @import("../pipelines/gliner_boundary_pipeline.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

/// Validate every item before any model invocation. This also keeps the
/// classification-only checkpoint from silently serving unrelated heads.
pub fn preflight(request: *const wire.Request) !void {
    if (request.items.len == 0) return error.InvalidDecisionRequest;
    for (request.items) |item| {
        try item.options.validateNativeLimits(.{});
        try presentation.validateOptions(item.options.native(.{}));
        const s = item.compiled.schema;
        if (s.classifications.len == 0 or s.entities.len != 0 or s.entity_attributes.len != 0 or
            s.relations.len != 0 or s.structures.len != 0 or s.joint_ie != null)
            return error.UnsupportedDecisionSchema;
        if (item.options.long_document.mode != .reject) return error.UnsupportedDecisionWindowing;
        for (s.classifications) |classification| {
            if (classification.hypothesis_template != null) return error.UnsupportedDecisionHypothesisTemplate;
            if (classification.task.labels.len < 2) return error.InvalidDecisionTask;
        }
    }
}

pub fn execute(
    allocator: std.mem.Allocator,
    pipeline: *gliner.GlinerPipeline,
    request: *const wire.Request,
    max_response_bytes: usize,
    control: ?Control,
) ![]u8 {
    try preflight(request);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const inputs = try a.alloc(gliner.DecisionRequest, request.items.len);
    for (request.items, inputs) |item, *input| {
        const classes = item.compiled.schema.classifications;
        const tasks = try a.alloc(gliner.DecisionTask, classes.len);
        for (classes, tasks) |classification, *task| {
            const labels = try a.alloc(gliner.DecisionLabel, classification.task.labels.len);
            for (classification.task.labels, labels) |label, *out| {
                out.* = .{ .name = label };
                for (classification.label_definitions) |definition| {
                    if (std.mem.eql(u8, label, definition.name)) {
                        out.description = definition.description orelse "";
                        break;
                    }
                }
            }
            const examples = try a.alloc(gliner.DecisionExample, classification.examples.len);
            for (classification.examples, examples) |example, *out| out.* = .{ .input = example.input, .output = example.label };
            task.* = .{
                .name = classification.task.name,
                .prompt = classification.prompt orelse "",
                .labels = labels,
                .examples = examples,
                .example_mode = if (examples.len > 0) .both else .descriptions,
                .multi_label = classification.mode == .multi,
                .activation = switch (classification.activation) {
                    .auto => .auto,
                    .sigmoid => .sigmoid,
                    .softmax => .softmax,
                },
                .temperature = @floatCast(classification.task.temperature),
                .threshold = @floatCast(classification.task.threshold),
                .top_k = classification.top_k,
            };
        }
        input.* = .{ .text = item.text, .tasks = tasks };
    }

    if (control) |c| try c.check();
    pipeline.execution_control = control;
    const results = try pipeline.decideBatch(inputs);
    defer {
        for (results) |*result| result.deinit(pipeline.allocator);
        pipeline.allocator.free(results);
    }
    if (results.len != request.items.len) return error.InvalidDecisionOutput;
    var writer = wire.ResponseWriter.init(allocator, max_response_bytes, request.items.len);
    defer writer.deinit();
    try writer.begin(request.model);
    var prompt_tokens: usize = 0;
    for (request.items, results) |item, result| {
        if (control) |c| try c.check();
        const classes = item.compiled.schema.classifications;
        if (result.task_ranges.len != classes.len) return error.InvalidDecisionOutput;
        const rows = try a.alloc([]const f64, classes.len);
        for (result.task_ranges, rows, classes, 0..) |range, *row, class, index| {
            if (range.task_index != index or range.end < range.start or range.end > result.raw_logits.len or
                range.end - range.start != class.task.labels.len) return error.InvalidDecisionOutput;
            const scores = try a.alloc(f64, class.task.labels.len);
            for (scores, result.raw_logits[range.start..range.end]) |*score, logit| score.* = logit;
            row.* = scores;
        }
        var options = item.options.native(.{});
        options.control = control;
        var selected = try presentation.presentClassifications(allocator, &item.compiled, rows, 1, options);
        defer selected.deinit();
        try writer.append(item, .{ .classifications = selected.classifications, .classification_solver = selected.diagnostics });
        prompt_tokens = try std.math.add(usize, prompt_tokens, result.prompt_tokens);
    }
    return writer.finish(prompt_tokens);
}

test "Decide V2 rejects unsupported heads before model execution" {
    const allocator = std.testing.allocator;
    var valid = try wire.parseJson(allocator,
        \\{"schema_version":2,"model":"fastino/GLiNER2.5-Decide","schema":{"classifications":[{"name":"intent","labels":["sales","support"]}]},"inputs":[{"content":"Please help"}]}
    , .{});
    defer valid.deinit();
    try preflight(&valid);

    var invalid = try wire.parseJson(allocator,
        \\{"schema_version":2,"model":"fastino/GLiNER2.5-Decide","schema":{"entities":["person"],"classifications":[{"name":"intent","labels":["sales","support"]}]},"inputs":[{"content":"Please help"}]}
    , .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UnsupportedDecisionSchema, preflight(&invalid));
}
