// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const factory = @import("../architectures/session_factory.zig");
const manifest_mod = @import("../models/manifest.zig");
const hf = @import("inference_hf_tokenizer");
const gliner = @import("gliner.zig");
const wire = @import("../extractors/extraction_v2.zig");
const executor = @import("../extractors/gliner_decision_executor.zig");

const fixture_bytes = @embedFile("testdata/decide_oracle.json");
const Fixture = struct {
    cases: []const struct {
        name: []const u8,
        text: []const u8,
        input_ids: []const i64,
        l_marker_positions: []const i64,
        tasks: std.json.Value,
    },
};

test "Decide published checkpoint matches upstream tokenization and logits" {
    const path = platform.env.getenv("ANTFLY_GLINER_DECIDE_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Fixture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    var manifest = try manifest_mod.loadFromDir(a, path);
    defer manifest.deinit();
    try std.testing.expect(manifest.gliner_classification_head == .label_marker_mlp);
    try std.testing.expectEqual(@as(u32, 1024), manifest.hidden_size);
    const tokenizer_bytes = try c_file.readFile(a, manifest.tokenizer_json_path orelse return error.NoTokenizerFound);
    defer a.free(tokenizer_bytes);
    const tokenizer = try hf.HfTokenizer.loadFromBytesWithOptions(a, tokenizer_bytes, .{ .strict_unigram_normalizer = true });
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const selected = platform.env.getenv("ANTFLY_GLINER_DECIDE_BACKEND") orelse "native";
    const session = if (std.mem.eql(u8, selected, "cuda"))
        try factory.createCudaSession(a, path)
    else if (std.mem.eql(u8, selected, "native"))
        try factory.createNativeSession(a, path)
    else
        return error.InvalidDecisionTestBackend;
    defer session.close();
    var pipeline = gliner.GlinerPipeline{
        .allocator = a,
        .session = session,
        .tok = tok,
        .config = .{
            .model_type = manifest.gliner_model_type,
            .classification_head = .label_marker_mlp,
            .max_length = manifest.max_position_embeddings,
            .token_p = manifest.gliner_token_p,
            .token_l = manifest.gliner_token_l,
            .token_sep_struct = manifest.gliner_token_sep_struct,
            .token_sep_text = manifest.gliner_token_sep_text,
        },
    };
    const names = [_][2][]const u8{ .{ "sentiment", "issue" }, .{ "intent", "urgency" } };
    for (parsed.value.cases, 0..) |case, case_index| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var tasks: [2]gliner.DecisionTask = undefined;
        for (names[case_index], 0..) |task_name, task_index| {
            const expected = case.tasks.object.get(task_name) orelse return error.InvalidDecisionFixture;
            const labels_json = expected.object.get("labels") orelse return error.InvalidDecisionFixture;
            const labels = try scratch.alloc(gliner.DecisionLabel, labels_json.array.items.len);
            for (labels_json.array.items, labels) |label, *out| out.* = .{ .name = label.string };
            tasks[task_index] = .{ .name = task_name, .labels = labels };
        }
        const request = gliner.DecisionRequest{ .text = case.text, .tasks = &tasks };
        var prepared = try pipeline.prepareDecisionBatch(&.{request});
        defer prepared.deinit();
        try std.testing.expectEqual(@as(usize, 1), prepared.rows.len);
        try std.testing.expectEqualSlices(i64, case.input_ids, prepared.rows[0].input_ids);
        try std.testing.expectEqualSlices(i64, case.l_marker_positions, prepared.rows[0].marker_positions);
        const result = try pipeline.decidePrepared(&prepared);
        defer {
            for (result) |*row| row.deinit(a);
            a.free(result);
        }
        var flat: usize = 0;
        for (tasks) |task| {
            const expected = case.tasks.object.get(task.name).?;
            const logits = expected.object.get("logits").?.object;
            for (task.labels) |label| {
                const reference = logits.get(label.name).?;
                const want: f32 = @floatCast(reference.float);
                try std.testing.expectApproxEqAbs(want, result[0].raw_logits[flat], 0.02);
                flat += 1;
            }
        }
    }

    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"fastino/GLiNER2.5-Decide","schema":{"classifications":[{"name":"intent","mode":"single","labels":["refund","technical_support","sales"]},{"name":"urgency","mode":"single","labels":["low","medium","high"]}]},"inputs":[{"content":"Please refund the duplicate charge. I do not need technical help."}]}
    , .{});
    defer request.deinit();
    const response = try executor.execute(a, &pipeline, &request, 1024 * 1024, null);
    defer a.free(response);
    var json = try std.json.parseFromSlice(std.json.Value, a, response, .{});
    defer json.deinit();
    const classes = json.value.object.get("data").?.array.items[0].object.get("classifications").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), classes.len);
    try std.testing.expectEqualStrings("intent", classes[0].object.get("name").?.string);
    try std.testing.expectEqualStrings("refund", classes[0].object.get("label").?.string);
    try std.testing.expectEqualStrings("urgency", classes[1].object.get("name").?.string);
    try std.testing.expectEqualStrings("low", classes[1].object.get("label").?.string);
}
