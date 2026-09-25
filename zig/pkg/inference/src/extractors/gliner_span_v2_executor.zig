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

//! schema_version:2 classification for legacy span (`SpanExtractor`) GLiNER2
//! checkpoints such as fastino/GLiNER2.5-Decide.
//!
//! Upstream gliner2 shares one SchemaTransformer between the span and boundary
//! architectures, so the boundary processor produces the exact model-facing
//! prompt. Only the head differs: the span model scores each `[L]` marker's
//! raw encoder state with `classifier` (Linear H→2H, ReLU, Linear 2H→1) at
//! model temperature 1. Activation, thresholds, top_k and constraints are the
//! shared boundary presentation.

const std = @import("std");
const wire = @import("extraction_v2.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const boundary_executor = @import("gliner_boundary_executor.zig");
const compute = @import("../ops/ops.zig");
const deberta_mod = @import("../models/deberta.zig");
const deberta_arch = @import("../architectures/deberta.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const CT = compute.CT;

pub const Options = struct {
    max_response_bytes: usize = 64 * 1024 * 1024,
    /// `max_sequence_tokens` is the hard per-sequence ceiling. It defaults to
    /// DeBERTa-v3's 512 pretraining positions, the same cap the legacy span
    /// route applies; beyond it the Metal encoder leaves flash attention and
    /// materializes full score matrices. The server passes the model's
    /// max_position_embeddings.
    processor: processor.Options = .{ .max_sequence_tokens = 512, .max_batch_tokens = 512 },
    /// Split classification tasks across sequences so each sequence (its task
    /// prompts plus the full text) stays within this many tokens, repeating
    /// the text rather than truncating it. When some task's prompt plus the
    /// text alone already exceeds it no split can reach it, so the item runs
    /// as one sequence, still bounded by `processor.max_sequence_tokens`.
    /// Zero disables splitting (upstream behavior).
    max_prompt_tokens: usize = 512,
    /// Each split sequence re-encodes the full text; bound that amplification.
    max_sequences_per_item: usize = 8,
    /// Encoded tokens across every sequence of the request.
    max_request_tokens: usize = 64 * 1024,
    pipeline: pipeline.Options = .{},
    control: ?Control = null,
    failure: ?*wire.FailureContext = null,
};

fn progress(options: Options, index: ?usize, stage: []const u8) !void {
    if (options.control) |control| try control.check();
    if (options.failure) |failure| failure.* = .{ .input_index = index, .stage = stage };
}

/// Classification is the only span task on this route; entity, relation and
/// structure extraction for span checkpoints stay on schema_version 1.
pub fn preflight(request: *const wire.Request, options: Options) !void {
    try boundary_executor.preflight(request, .{ .control = options.control, .failure = options.failure, .max_response_bytes = options.max_response_bytes, .pipeline = options.pipeline });
    for (request.items, 0..) |item, index| {
        try progress(options, index, "preflight");
        const schema = item.compiled.schema;
        if (schema.classifications.len == 0) return error.UnsupportedGlinerSpanV2Task;
        if (schema.entities.len != 0 or schema.entity_attributes.len != 0 or schema.structures.len != 0 or
            schema.relations.len != 0 or schema.joint_ie != null) return error.UnsupportedGlinerSpanV2Task;
        if (item.options.long_document.mode == .window) return error.UnsupportedGlinerSpanLongDocument;
    }
}

/// Stage timings; profiling synchronizes the backend between stages.
pub const Profile = struct {
    encoder: deberta_arch.EncoderProfile = .{},
    encoder_ns: u64 = 0,
    head_ns: u64 = 0,
    readback_ns: u64 = 0,
};

fn nowNs() u64 {
    return @import("antfly_platform").time.monotonicNs();
}

/// Classifier logits for every `[L]` marker of one prepared sample, grouped by
/// classification task in schema order. Rows are owned by `allocator`.
pub fn classificationLogits(
    cb: *const compute.ComputeBackend,
    allocator: Allocator,
    config: deberta_mod.Config,
    sample: processor.Sample,
    task_label_counts: []const usize,
) ![][]f64 {
    return classificationLogitsProfiled(cb, allocator, config, sample, task_label_counts, null);
}

pub fn classificationLogitsProfiled(
    cb: *const compute.ComputeBackend,
    allocator: Allocator,
    config: deberta_mod.Config,
    sample: processor.Sample,
    task_label_counts: []const usize,
    profile: ?*Profile,
) ![][]f64 {
    var stage_start = if (profile != null) nowNs() else 0;
    const seq_len = sample.input_ids.len;
    const H: usize = config.hidden_size;
    const labels = sample.classification_labels;
    if (seq_len == 0 or labels.len == 0) return error.InvalidExtractionInput;

    const attention_mask = try allocator.alloc(i64, seq_len);
    defer allocator.free(attention_mask);
    @memset(attention_mask, 1);
    // Same encoder and head mirror policy as the session's legacy span route.
    cb.preferEagerQuantMirrors(true);
    const hidden = try deberta_arch.forwardCtProfiled(cb, allocator, config, sample.input_ids, attention_mask, 1, seq_len, deberta_mod.glinerPrefersWeightMirrors(config), if (profile) |p| &p.encoder else null);
    defer cb.free(hidden);
    if (profile) |p| {
        try cb.evalTensor(hidden);
        const now = nowNs();
        p.encoder_ns += now - stage_start;
        stage_start = now;
    }

    const positions = try allocator.alloc(u32, labels.len);
    defer allocator.free(positions);
    for (labels, positions) |label, *position| {
        if (label.marker_index >= seq_len) return error.InvalidExtractionInput;
        position.* = @intCast(label.marker_index);
    }
    // One Metal frame for the whole head instead of a synchronous command
    // buffer per op; the readback below happens after it completes.
    var head_frame_active = false;
    if (cb.kind() == .metal and !cb.decoderRuntimeHasActiveFrame()) {
        head_frame_active = cb.decoderRuntimeBeginFrame() catch false;
    }
    errdefer if (head_frame_active) cb.decoderRuntimeCancelFrame() catch {};
    const marker_states = (try cb.takeRows(hidden, positions, labels.len, H)) orelse gather: {
        const ids = try allocator.alloc(i64, labels.len);
        defer allocator.free(ids);
        for (positions, ids) |position, *id| id.* = position;
        break :gather try cb.embeddingLookup(hidden, ids, labels.len, H);
    };
    defer cb.free(marker_states);

    const w0 = try cb.getWeight("classifier.0.weight");
    defer cb.free(w0);
    const b0 = try cb.getWeight("classifier.0.bias");
    defer cb.free(b0);
    const first = if (try cb.linearRelu(marker_states, w0, b0, labels.len, H, 2 * H)) |fused|
        fused
    else blk: {
        const linear = try cb.linear(marker_states, w0, b0, labels.len, H, 2 * H);
        defer cb.free(linear);
        break :blk try cb.relu(linear);
    };
    defer cb.free(first);
    const w2 = try cb.getWeight("classifier.2.weight");
    defer cb.free(w2);
    const b2 = try cb.getWeight("classifier.2.bias");
    defer cb.free(b2);
    const logits_ct = try cb.linear(first, w2, b2, labels.len, 2 * H, 1);
    defer cb.free(logits_ct);
    if (head_frame_active) {
        head_frame_active = false;
        try cb.decoderRuntimeSubmitAndWaitFrame();
    }
    if (profile) |p| {
        try cb.evalTensor(logits_ct);
        const now = nowNs();
        p.head_ns += now - stage_start;
        stage_start = now;
    }
    const logits = try cb.toFloat32(logits_ct, allocator);
    defer allocator.free(logits);
    if (profile) |p| p.readback_ns += nowNs() - stage_start;
    if (logits.len != labels.len) return error.InvalidExtractionOutput;

    const rows = try allocator.alloc([]f64, task_label_counts.len);
    var built: usize = 0;
    errdefer {
        for (rows[0..built]) |row| allocator.free(row);
        allocator.free(rows);
    }
    for (task_label_counts, rows) |count, *row| {
        row.* = try allocator.alloc(f64, count);
        @memset(row.*, std.math.nan(f64));
        built += 1;
    }
    for (labels, logits) |label, logit| {
        if (label.schema_index >= rows.len or label.label_index >= rows[label.schema_index].len) return error.InvalidExtractionOutput;
        rows[label.schema_index][label.label_index] = logit;
    }
    // presentClassifications rejects non-finite scores, so a label the
    // processor did not emit cannot silently become a decision.
    return rows;
}

fn freeRows(allocator: Allocator, rows: [][]f64) void {
    for (rows) |row| allocator.free(row);
    allocator.free(rows);
}

pub const TaskRange = struct { start: usize, end: usize };

/// Greedy contiguous split of `task_count` tasks: each range grows while its
/// full prompt length (reported by `prompt_len` for tasks [start, end)) stays
/// within `budget`. If a single task cannot fit, splitting only multiplies
/// encoder work without reaching the budget, so all tasks share one range.
pub fn planTaskRanges(
    allocator: Allocator,
    task_count: usize,
    budget: usize,
    context: anytype,
    comptime prompt_len: fn (@TypeOf(context), usize, usize) anyerror!usize,
) ![]TaskRange {
    var ranges = std.ArrayListUnmanaged(TaskRange).empty;
    errdefer ranges.deinit(allocator);
    if (task_count == 0) return ranges.toOwnedSlice(allocator);
    if (budget == 0 or try prompt_len(context, 0, task_count) <= budget) {
        try ranges.append(allocator, .{ .start = 0, .end = task_count });
        return ranges.toOwnedSlice(allocator);
    }
    var start: usize = 0;
    while (start < task_count) {
        if (try prompt_len(context, start, start + 1) > budget) {
            ranges.clearRetainingCapacity();
            try ranges.append(allocator, .{ .start = 0, .end = task_count });
            return ranges.toOwnedSlice(allocator);
        }
        var end = start + 1;
        while (end < task_count and try prompt_len(context, start, end + 1) <= budget) end += 1;
        try ranges.append(allocator, .{ .start = start, .end = end });
        start = end;
    }
    return ranges.toOwnedSlice(allocator);
}

/// Borrowed view of `compiled` restricted to classification tasks [start, end).
/// Never deinit the returned value; it aliases the original arena.
fn classificationView(compiled: *const schema_mod.CompiledSchema, range: TaskRange) schema_mod.CompiledSchema {
    var view = compiled.*;
    view.schema.classifications = compiled.schema.classifications[range.start..range.end];
    return view;
}

/// Prompt lengths for contiguous task ranges without re-tokenizing the text
/// per candidate. The processor encodes every group fragment independently of
/// the text and joins groups with one `[SEP_STRUCT]`, so a range's length is
/// the sum of its groups, plus separators, plus the (range-independent) text
/// part. Group costs are measured against an empty text; the text part once.
const PromptLengths = struct {
    /// Length of task i alone with an empty text.
    alone_empty: []usize,
    /// Empty-text part (`[SEP_TEXT]` plus the synthetic period).
    empty_text: usize,
    /// Length of task 0 with the real text.
    first_with_text: usize,

    fn init(allocator: Allocator, tokenizer: Tokenizer, item: *const wire.Item, options: processor.Options) !PromptLengths {
        const count = item.compiled.schema.classifications.len;
        const alone = try allocator.alloc(usize, count);
        errdefer allocator.free(alone);
        for (alone, 0..) |*length, i| length.* = try preparedLength(allocator, tokenizer, item, "", .{ .start = i, .end = i + 1 }, options);
        const empty_text = if (count >= 2) blk: {
            const pair = try preparedLength(allocator, tokenizer, item, "", .{ .start = 0, .end = 2 }, options);
            // pair = (alone0 - E) + (alone1 - E) + 1 + E
            break :blk std.math.sub(usize, alone[0] + alone[1] + 1, pair) catch return error.InvalidExtractionOutput;
        } else 0;
        return .{
            .alone_empty = alone,
            .empty_text = empty_text,
            .first_with_text = try preparedLength(allocator, tokenizer, item, item.text, .{ .start = 0, .end = 1 }, options),
        };
    }

    fn deinit(self: *PromptLengths, allocator: Allocator) void {
        allocator.free(self.alone_empty);
        self.* = undefined;
    }

    fn measure(self: *const PromptLengths, start: usize, end: usize) anyerror!usize {
        const n = end - start;
        var total = self.first_with_text + (n - 1) - self.alone_empty[0];
        for (self.alone_empty[start..end]) |length| total += length;
        return total - (n - 1) * self.empty_text;
    }
};

fn preparedLength(allocator: Allocator, tokenizer: Tokenizer, item: *const wire.Item, text: []const u8, range: TaskRange, options: processor.Options) !usize {
    const view = classificationView(&item.compiled, range);
    var prepared = try processor.prepare(allocator, tokenizer, &.{.{ .text = text, .schema = &view }}, options);
    defer prepared.deinit();
    return prepared.samples[0].input_ids.len;
}

/// Tokenized sequences of one item, one per contiguous task range.
pub const PreparedItem = struct {
    ranges: []TaskRange,
    batches: []processor.PreparedBatch,
    prompt_tokens: usize,

    fn deinit(self: *PreparedItem, allocator: Allocator) void {
        for (self.batches) |*batch| batch.deinit();
        allocator.free(self.batches);
        allocator.free(self.ranges);
        self.* = undefined;
    }
};

/// Tokenizes and splits one item. Needs no backend or model lock.
pub fn prepareItem(allocator: Allocator, tokenizer: Tokenizer, item: *const wire.Item, processor_options: processor.Options, options: Options) !PreparedItem {
    const classifications = item.compiled.schema.classifications;
    var lengths = try PromptLengths.init(allocator, tokenizer, item, processor_options);
    defer lengths.deinit(allocator);
    const ranges = try planTaskRanges(allocator, classifications.len, options.max_prompt_tokens, &lengths, PromptLengths.measure);
    errdefer allocator.free(ranges);
    if (ranges.len > options.max_sequences_per_item) return error.ExtractionSchemaLimitExceeded;
    const batches = try allocator.alloc(processor.PreparedBatch, ranges.len);
    var prepared: usize = 0;
    errdefer {
        for (batches[0..prepared]) |*batch| batch.deinit();
        allocator.free(batches);
    }
    var prompt_tokens: usize = 0;
    for (ranges, batches) |range, *batch| {
        const view = classificationView(&item.compiled, range);
        // Enforces processor_options.max_sequence_tokens on every sequence.
        batch.* = try processor.prepare(allocator, tokenizer, &.{.{ .text = item.text, .schema = &view }}, processor_options);
        prepared += 1;
        prompt_tokens = try std.math.add(usize, prompt_tokens, batch.samples[0].input_ids.len);
    }
    return .{ .ranges = ranges, .batches = batches, .prompt_tokens = prompt_tokens };
}

pub const ItemLogits = struct {
    rows: [][]f64,
    prompt_tokens: usize,
    sequences: usize,

    pub fn deinit(self: *ItemLogits, allocator: Allocator) void {
        freeRows(allocator, self.rows);
        self.* = undefined;
    }
};

/// Classifier logits for every task of a prepared item, grouped in schema order.
pub fn preparedItemLogits(
    cb: *const compute.ComputeBackend,
    allocator: Allocator,
    config: deberta_mod.Config,
    item: *const wire.Item,
    prepared_item: *const PreparedItem,
    profile: ?*Profile,
) !ItemLogits {
    const classifications = item.compiled.schema.classifications;
    const rows = try allocator.alloc([]f64, classifications.len);
    var built: usize = 0;
    errdefer {
        for (rows[0..built]) |row| allocator.free(row);
        allocator.free(rows);
    }
    for (prepared_item.ranges, prepared_item.batches) |range, batch| {
        const counts = try allocator.alloc(usize, range.end - range.start);
        defer allocator.free(counts);
        for (classifications[range.start..range.end], counts) |classification, *count| count.* = classification.task.labels.len;
        const chunk_rows = try classificationLogitsProfiled(cb, allocator, config, batch.samples[0], counts, profile);
        defer allocator.free(chunk_rows);
        for (chunk_rows) |row| {
            rows[built] = row;
            built += 1;
        }
    }
    if (built != rows.len) return error.InvalidExtractionOutput;
    return .{ .rows = rows, .prompt_tokens = prepared_item.prompt_tokens, .sequences = prepared_item.ranges.len };
}

/// Classifier logits for every task of `item`, grouped in schema order, using
/// as many sequences as `options.max_prompt_tokens` requires.
pub fn itemClassificationLogits(
    cb: *const compute.ComputeBackend,
    allocator: Allocator,
    config: deberta_mod.Config,
    tokenizer: Tokenizer,
    item: *const wire.Item,
    options: Options,
    profile: ?*Profile,
) !ItemLogits {
    var prepared_item = try prepareItem(allocator, tokenizer, item, item.options.preprocessing(options.processor), options);
    defer prepared_item.deinit(allocator);
    return preparedItemLogits(cb, allocator, config, item, &prepared_item, profile);
}

/// Conservative device scratch for one sequence of `tokens` on `config`:
/// hidden-sized activations, the FFN intermediate, disentangled-attention
/// score/c2p/p2c matrices (as if materialized), relative-position tables and
/// the classifier head. Sequences run serially, so this bounds a request.
pub fn deviceScratchUpperBound(config: deberta_mod.Config, tokens: usize) !usize {
    const h: usize = config.hidden_size;
    const i: usize = config.intermediate_size;
    const heads: usize = config.num_attention_heads;
    const buckets: usize = config.position_buckets;
    const f = @sizeOf(f32);
    var total: usize = 0;
    total = try std.math.add(usize, total, try std.math.mul(usize, 8 * f, try std.math.mul(usize, tokens, h)));
    total = try std.math.add(usize, total, try std.math.mul(usize, 2 * f, try std.math.mul(usize, tokens, i)));
    total = try std.math.add(usize, total, try std.math.mul(usize, 3 * f * heads, try std.math.mul(usize, tokens, tokens)));
    total = try std.math.add(usize, total, try std.math.mul(usize, 4 * f, try std.math.mul(usize, buckets, h)));
    total = try std.math.add(usize, total, try std.math.mul(usize, 2 * f, try std.math.mul(usize, tokens, h)));
    // Allocator rounding and transient copies.
    return std.math.mul(usize, total, 2);
}

/// Longest planned sequence, for executor-contract token validation and
/// device admission.
pub fn maxPlannedSequenceTokens(request_plan: *const Plan) usize {
    var longest: usize = 0;
    for (request_plan.items) |item| for (item.batches) |batch| {
        longest = @max(longest, batch.samples[0].input_ids.len);
    };
    return longest;
}

test "gliner span v2 device scratch bound covers a large 512-token sequence" {
    const large = deberta_mod.Config{ .hidden_size = 1024, .num_hidden_layers = 24, .num_attention_heads = 16, .intermediate_size = 4096 };
    const bytes = try deviceScratchUpperBound(large, 512);
    // Materialized 16-head 512x512 scores alone are 16 MiB per matrix.
    try std.testing.expect(bytes >= 3 * 16 * 512 * 512 * 4);
    try std.testing.expect(bytes < 512 * 1024 * 1024);
}

/// Tokenized, split request. Built before the model lock is taken.
pub const Plan = struct {
    items: []PreparedItem,
    prompt_tokens: usize,

    pub fn deinit(self: *Plan, allocator: Allocator) void {
        for (self.items) |*item| item.deinit(allocator);
        allocator.free(self.items);
        self.* = undefined;
    }
};

/// Preflight, tokenization and splitting for the whole request, including
/// the request-wide encoded-token budget. Uses only the tokenizer.
pub fn plan(allocator: Allocator, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !Plan {
    try preflight(request, options);
    const items = try allocator.alloc(PreparedItem, request.items.len);
    var prepared: usize = 0;
    errdefer {
        for (items[0..prepared]) |*item| item.deinit(allocator);
        allocator.free(items);
    }
    var prompt_tokens: usize = 0;
    for (request.items, items, 0..) |*item, *out, index| {
        try progress(options, index, "tokenizing");
        var processor_options = item.options.preprocessing(options.processor);
        processor_options.control = options.control;
        out.* = try prepareItem(allocator, tokenizer, item, processor_options, options);
        prepared += 1;
        prompt_tokens = std.math.add(usize, prompt_tokens, out.prompt_tokens) catch return error.ExtractionRequestLimitExceeded;
        if (prompt_tokens > options.max_request_tokens) return error.ExtractionRequestLimitExceeded;
    }
    return .{ .items = items, .prompt_tokens = prompt_tokens };
}

/// Runs a plan built by `plan` for the same request. The caller owns the
/// managed backend, model lock and admission for the complete execution.
pub fn executePlanned(cb: *const compute.ComputeBackend, allocator: Allocator, config: deberta_mod.Config, request: *const wire.Request, request_plan: *const Plan, options: Options) ![]u8 {
    if (cb.kind() != .native and cb.kind() != .metal) return error.UnsupportedExtractionBackend;
    if (request_plan.items.len != request.items.len) return error.InvalidExtractionInput;
    var writer = wire.ResponseWriter.init(allocator, options.max_response_bytes, request.items.len);
    defer writer.deinit();
    try writer.begin(request.model);
    var remaining_values = options.pipeline.max_output_values;
    for (request.items, request_plan.items, 0..) |*item, *prepared_item, index| {
        if (remaining_values == 0) return error.ExtractionOutputLimitExceeded;
        try progress(options, index, "encoder");
        var logits = try preparedItemLogits(cb, allocator, config, item, prepared_item, null);
        defer logits.deinit(allocator);
        const const_rows = try allocator.alloc([]const f64, logits.rows.len);
        defer allocator.free(const_rows);
        for (logits.rows, const_rows) |row, *out| out.* = row;

        try progress(options, index, "decoding");
        var pipeline_options = item.options.native(options.pipeline);
        pipeline_options.control = options.control;
        pipeline_options.max_output_values = remaining_values;
        var presented = try pipeline.presentClassifications(allocator, &item.compiled, const_rows, 1.0, pipeline_options);
        defer presented.deinit();
        if (presented.output_values > remaining_values) return error.ExtractionOutputLimitExceeded;
        remaining_values -= presented.output_values;

        try progress(options, index, "serializing");
        try writer.append(item.*, .{ .classifications = presented.classifications, .classification_solver = presented.diagnostics });
    }
    try progress(options, null, "serializing");
    return writer.finish(request_plan.prompt_tokens);
}

pub fn execute(cb: *const compute.ComputeBackend, allocator: Allocator, config: deberta_mod.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    var request_plan = try plan(allocator, tokenizer, request, options);
    defer request_plan.deinit(allocator);
    return executePlanned(cb, allocator, config, request, &request_plan, options);
}

test "gliner span v2 task split is greedy, contiguous and never splits an unreachable budget" {
    const a = std.testing.allocator;
    // Prompt length = text + sum of task costs.
    const Costs = struct {
        text: usize,
        tasks: []const usize,
        fn len(self: *const @This(), start: usize, end: usize) anyerror!usize {
            var total = self.text;
            for (self.tasks[start..end]) |cost| total += cost;
            return total;
        }
    };
    const fits = Costs{ .text = 100, .tasks = &.{ 50, 50, 50 } };
    const one = try planTaskRanges(a, 3, 512, &fits, Costs.len);
    defer a.free(one);
    try std.testing.expectEqualSlices(TaskRange, &.{.{ .start = 0, .end = 3 }}, one);

    const split = Costs{ .text = 300, .tasks = &.{ 100, 100, 150, 200, 50 } };
    const ranges = try planTaskRanges(a, 5, 512, &split, Costs.len);
    defer a.free(ranges);
    // 400, 500 | 450 | 500 | 350: tasks 3 and 4 together would be 550.
    try std.testing.expectEqualSlices(TaskRange, &.{ .{ .start = 0, .end = 2 }, .{ .start = 2, .end = 3 }, .{ .start = 3, .end = 4 }, .{ .start = 4, .end = 5 } }, ranges);

    // Task 3 alone (300 + 400) exceeds the budget: one sequence, not four.
    const unreachable_budget = Costs{ .text = 300, .tasks = &.{ 100, 100, 150, 400, 50 } };
    const single = try planTaskRanges(a, 5, 512, &unreachable_budget, Costs.len);
    defer a.free(single);
    try std.testing.expectEqualSlices(TaskRange, &.{.{ .start = 0, .end = 5 }}, single);

    const disabled = try planTaskRanges(a, 5, 0, &split, Costs.len);
    defer a.free(disabled);
    try std.testing.expectEqualSlices(TaskRange, &.{.{ .start = 0, .end = 5 }}, disabled);
}

test "gliner span v2 preflight accepts classification and rejects span tasks" {
    const a = std.testing.allocator;
    var ok = try wire.parseJson(a,
        \\{"schema_version":2,"model":"decide","schema":{"classifications":[{"name":"intent","labels":["a","b"]}]},"inputs":[{"content":"x"}]}
    , .{});
    defer ok.deinit();
    try preflight(&ok, .{});
    var mixed = try wire.parseJson(a,
        \\{"schema_version":2,"model":"decide","schema":{"entities":["person"],"classifications":[{"name":"intent","labels":["a","b"]}]},"inputs":[{"content":"x"}]}
    , .{});
    defer mixed.deinit();
    try std.testing.expectError(error.UnsupportedGlinerSpanV2Task, preflight(&mixed, .{}));
}

fn jsonNumber(value: std.json.Value) f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => std.math.nan(f64),
    };
}

fn thresholdMargin(logits: []const std.json.Value, threshold: f64) f64 {
    const cut = @log(threshold / (1 - threshold));
    var margin = std.math.inf(f64);
    for (logits) |value| margin = @min(margin, @abs(jsonNumber(value) - cut));
    return margin;
}

fn topTwoMargin(logits: []const std.json.Value) f64 {
    var first = -std.math.inf(f64);
    var second = -std.math.inf(f64);
    for (logits) |value| {
        const logit = jsonNumber(value);
        if (logit > first) {
            second = first;
            first = logit;
        } else if (logit > second) second = logit;
    }
    return first - second;
}

// Parity against testdata/gliner25/decide/cases.json, captured from upstream
// gliner2==2.0.0 by scripts/gliner25/decide_oracle.py. Checks the exact
// model-facing ids, every classifier logit, and the presented decisions.
fn testDecideParity(directory: []const u8, metal: bool, logit_tolerance: f64) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const bytes = try fixtures.fixtureBytes(a, "decide/cases.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer fixture.deinit();

    const factory = @import("../architectures/session_factory.zig");
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    try std.testing.expectEqual(@as(u32, 1024), config.hidden_size);
    try std.testing.expectEqual(deberta_mod.GlinerCountLayer.count_lstm, config.gliner_count_layer);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();

    // Accelerator sessions require process isolation for uninterruptible work.
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = Control{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 600 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const cb = &managed.backend;

    var max_logit_error: f64 = 0;
    for (fixture.value.object.get("classification").?.array.items) |case_value| {
        const case = case_value.object;
        const name = case.get("name").?.string;
        const raw = try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "fastino/GLiNER2.5-Decide",
            .schema = case.get("v2_schema").?,
            .inputs = .{.{ .content = case.get("text").?.string }},
        }, .{});
        defer a.free(raw);
        var request = try wire.parseJson(a, raw, .{});
        defer request.deinit();
        const item = request.items[0];

        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, (Options{}).processor);
        defer prepared.deinit();
        const sample = prepared.samples[0];
        const expected_ids = case.get("input_ids").?.array.items;
        errdefer std.debug.print("case {s}: ids differ\n", .{name});
        try std.testing.expectEqual(expected_ids.len, sample.input_ids.len);
        for (expected_ids, sample.input_ids) |expected, actual| try std.testing.expectEqual(expected.integer, actual);

        const classifications = item.compiled.schema.classifications;
        const counts = try a.alloc(usize, classifications.len);
        defer a.free(counts);
        for (classifications, counts) |classification, *count| count.* = classification.task.labels.len;
        const rows = try classificationLogits(cb, a, config, sample, counts);
        defer freeRows(a, rows);
        const expected_rows = case.get("classifier_logits").?.array.items;
        try std.testing.expectEqual(expected_rows.len, rows.len);
        for (expected_rows, rows) |expected_row, row| {
            try std.testing.expectEqual(expected_row.array.items.len, row.len);
            for (expected_row.array.items, row) |expected, actual| {
                const want: f64 = switch (expected) {
                    .float => |f| f,
                    .integer => |i| @floatFromInt(i),
                    else => return error.InvalidFixture,
                };
                max_logit_error = @max(max_logit_error, @abs(want - actual));
            }
        }

        const const_rows = try a.alloc([]const f64, rows.len);
        defer a.free(const_rows);
        for (rows, const_rows) |row, *out| out.* = row;
        var presented = try pipeline.presentClassifications(a, &item.compiled, const_rows, 1.0, item.options.native(.{}));
        defer presented.deinit();
        const expected_result = case.get("result").?.object;
        try std.testing.expectEqual(expected_result.count(), presented.classifications.len);
        for (presented.classifications, expected_rows, item.compiled.schema.classifications) |classification, expected_row, task| {
            // A reference decision margin inside the logit tolerance is a
            // genuine tie at this precision (e.g. Q8_0): the top-2 gap for a
            // single-label task, the distance from the threshold for a
            // multi-label one. Either outcome is acceptable there.
            const margin = if (task.mode == .multi)
                thresholdMargin(expected_row.array.items, task.task.threshold)
            else
                topTwoMargin(expected_row.array.items);
            if (margin < 2 * logit_tolerance) {
                std.debug.print("case {s} task {s}: near-tie (margin {d:.4}), decision not compared\n", .{ name, classification.name, margin });
                continue;
            }
            const expected = expected_result.get(classification.name).?;
            const expected_labels: []const std.json.Value = switch (expected) {
                .array => |list| list.items,
                .object => &.{expected},
                else => return error.InvalidFixture,
            };
            errdefer std.debug.print("case {s} task {s}: decision differs\n", .{ name, classification.name });
            try std.testing.expectEqual(expected_labels.len, classification.labels.len);
            for (expected_labels, classification.labels) |want, got| {
                try std.testing.expectEqualStrings(want.object.get("label").?.string, got.label);
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(want.object.get("confidence").?.float)), got.confidence, @as(f32, @floatCast(logit_tolerance)));
            }
        }
    }
    std.debug.print("GLiNER2.5-Decide {s} max classifier logit error {e}\n", .{ if (metal) "metal" else "native", max_logit_error });
    try std.testing.expect(max_logit_error <= logit_tolerance);
}

test "gliner span v2 GLiNER2.5-Decide native parity" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    try testDecideParity(directory, false, 2e-3);
}

test "gliner span v2 GLiNER2.5-Decide Metal parity" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    try testDecideParity(directory, true, 5e-3);
}

// Span head + CountLSTM v1 parity on upstream's exact entity inputs, which
// isolates the head from the legacy entity pipeline's prompt construction.
test "gliner span v2 GLiNER2.5-Decide CountLSTM v1 entity head parity" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const gliner_head = @import("../architectures/gliner_head.zig");
    const session = try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    var managed = try factory.getManagedComputeBackend(session, a, null, null);
    defer managed.deinit();
    const cb = &managed.backend;
    // "My name is Clara and I live in Berkeley, California." with
    // ["person", "location"], from gliner2==2.0.0 SchemaTransformer.
    const ids = [_]i64{ 287, 128003, 6967, 287, 128005, 604, 128005, 1250, 1263, 1263, 128002, 312, 601, 269, 66753, 452, 263, 584, 685, 267, 507, 108030, 366, 28225, 323 };
    const words = [_]i64{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 9, 10, 11, 12 };
    const num_words = 12;
    const max_width = 8;
    var spans: [num_words * max_width * 2]i64 = undefined;
    for (0..num_words) |w| for (0..max_width) |k| {
        const valid = w + k < num_words;
        spans[(w * max_width + k) * 2] = if (valid) @intCast(w) else 0;
        spans[(w * max_width + k) * 2 + 1] = if (valid) @intCast(w + k) else 0;
    };
    var mask: [ids.len]i64 = undefined;
    @memset(&mask, 1);
    const hidden = try deberta_arch.forwardCt(cb, a, config, &ids, &mask, 1, ids.len, true);
    defer cb.free(hidden);
    const head = try gliner_head.forwardCtWithLabelMarkers(cb, a, hidden, &ids, &words, &spans, 1, ids.len, config.hidden_size, .{
        .classification = config.classification_token_id,
        .entity = config.entity_token_id,
        .relation = config.relation_token_id,
        .count_layer = .count_lstm,
    });
    defer cb.free(head.logits);
    const logits = try cb.toFloat32(head.logits, a);
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 2), head.num_labels);
    const Expect = struct { word: usize, label: usize, score: f32 };
    // Upstream width-1 span probabilities.
    const expected = [_]Expect{ .{ .word = 3, .label = 0, .score = 0.9999 }, .{ .word = 8, .label = 1, .score = 0.9812 }, .{ .word = 10, .label = 1, .score = 0.8735 }, .{ .word = 3, .label = 1, .score = 0 }, .{ .word = 8, .label = 0, .score = 0 } };
    for (expected) |e| {
        const logit = logits[(e.word * max_width) * 2 + e.label];
        const probability = 1.0 / (1.0 + @exp(-logit));
        std.debug.print("word {d} label {d}: zig {d:.4} upstream {d:.4}\n", .{ e.word, e.label, probability, e.score });
        try std.testing.expectApproxEqAbs(e.score, probability, 1e-3);
    }
}

test "gliner span v2 GLiNER2.5-Decide Q8_0 bundle Metal parity" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_Q8_BUNDLE_DIR") orelse return error.SkipZigTest;
    // Q8_0 encoder weights: decisions must match exactly; probabilities and
    // logits carry quantization error.
    try testDecideParity(directory, true, 5e-2);
}

// Warm Metal latency breakdown: ANTFLY_GLINER25_DECIDE_BENCH=1 plus the model dir.
test "gliner span v2 GLiNER2.5-Decide Metal latency profile" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const env = @import("antfly_platform").env;
    const directory = env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    if (!env.getenvBool("ANTFLY_GLINER25_DECIDE_BENCH")) return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const backend_name = env.getenv("ANTFLY_GLINER25_DECIDE_BENCH_BACKEND") orelse "metal";
    const metal = std.mem.eql(u8, backend_name, "metal");
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = Control{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = nowNs() + 900 * std.time.ns_per_s };

    const raw =
        \\{"schema_version":2,"model":"decide","schema":{"classifications":[
        \\{"name":"intent","labels":["maintenance","room_change","checkout","billing","complaint","amenity_request"]},
        \\{"name":"priority","labels":["low","normal","high","urgent"]},
        \\{"name":"needs_human","labels":["yes","no"]},
        \\{"name":"topics","labels":["hvac","billing","housekeeping","noise","safety"],"multi_label":true,"threshold":0.4}]},
        \\"inputs":[{"content":"Guest in room 1408 says the AC has been out since yesterday and they want to move tonight or leave. They also asked for the incidentals hold to be released."}]}
    ;
    const tiny =
        \\{"schema_version":2,"model":"decide","schema":{"classifications":[{"name":"answer","labels":["yes","no"]}]},"inputs":[{"content":"ok thanks"}]}
    ;
    var request = try wire.parseJson(a, if (env.getenvBool("ANTFLY_GLINER25_DECIDE_BENCH_TINY")) tiny else raw, .{});
    defer request.deinit();
    const item = request.items[0];
    const counts = try a.alloc(usize, item.compiled.schema.classifications.len);
    defer a.free(counts);
    for (item.compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;

    const token_count = count: {
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, (Options{}).processor);
        defer prepared.deinit();
        break :count prepared.samples[0].input_ids.len;
    };
    const iterations = env.getenvUsize("ANTFLY_GLINER25_DECIDE_BENCH_ITERS") orelse 20;
    for (0..2) |phase| {
        const profiled = phase == 0;
        var profile = Profile{};
        var prepare_ns: u64 = 0;
        var total_ns: u64 = 0;
        var min_ns: u64 = std.math.maxInt(u64);
        for (0..iterations + 2) |iteration| {
            const start = nowNs();
            var managed = try factory.getManagedComputeBackend(session, a, null, control);
            defer managed.deinit();
            // Measurement only: without execution control the encoder skips
            // its per-2-layer cancellation submit-and-wait.
            if (env.getenvBool("ANTFLY_GLINER25_DECIDE_BENCH_NO_CONTROL_SYNC")) managed.backend.execution_control = null;
            var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, (Options{}).processor);
            defer prepared.deinit();
            const prepared_at = nowNs();
            var sample_profile = Profile{};
            const rows = try classificationLogitsProfiled(&managed.backend, a, config, prepared.samples[0], counts, if (profiled) &sample_profile else null);
            freeRows(a, rows);
            const elapsed = nowNs() - start;
            if (iteration < 2) continue; // warmup
            prepare_ns += prepared_at - start;
            total_ns += elapsed;
            min_ns = @min(min_ns, elapsed);
            profile.encoder_ns += sample_profile.encoder_ns;
            profile.head_ns += sample_profile.head_ns;
            profile.readback_ns += sample_profile.readback_ns;
            profile.encoder.add(sample_profile.encoder);
        }
        const n: f64 = @floatFromInt(iterations);
        const ms = struct {
            fn f(ns: u64, count: f64) f64 {
                return @as(f64, @floatFromInt(ns)) / count / 1e6;
            }
        }.f;
        std.debug.print("decide {s} {s} tokens={d}: mean {d:.2}ms min {d:.2}ms prepare {d:.2}ms encoder {d:.2}ms head {d:.2}ms readback {d:.2}ms\n", .{
            backend_name,                          if (profiled) "profiled" else "unsynced",
            token_count,                           ms(total_ns, n),
            @as(f64, @floatFromInt(min_ns)) / 1e6, ms(prepare_ns, n),
            ms(profile.encoder_ns, n),             ms(profile.head_ns, n),
            ms(profile.readback_ns, n),
        });
        if (profiled) {
            const e = profile.encoder;
            std.debug.print("  encoder host: embeddings {d:.2} relpos {d:.2} layers {d:.2} (qkv {d:.2} relqk {d:.2} attn {d:.2} attn_out {d:.2} ffn_in {d:.2} ffn_out {d:.2} ln {d:.2})\n", .{
                ms(e.embeddings_ns, n), ms(e.relative_position_ns, n), ms(e.layer_total_ns, n), ms(e.qkv_ns, n), ms(e.relative_qk_ns, n), ms(e.attention_ns, n), ms(e.attention_output_ns, n), ms(e.ffn_intermediate_ns, n), ms(e.ffn_output_ns, n), ms(e.layernorm_residual_ns, n),
            });
        }
    }
}

test "gliner span v2 GLiNER2.5-Decide splits an over-budget prompt across sequences" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const session = try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    var managed = try factory.getManagedComputeBackend(session, a, null, null);
    defer managed.deinit();

    const raw =
        \\{"schema_version":2,"model":"decide","schema":{"classifications":[
        \\{"name":"intent","labels":["order_status","refund_request","cancel_subscription","update_payment","login_problem","shipping_delay","bug_report","speak_to_human","other"]},
        \\{"name":"priority","labels":["low","normal","high","urgent"]},
        \\{"name":"sentiment","labels":["positive","negative","mixed","neutral"]},
        \\{"name":"needs_human","labels":["yes","no"]},
        \\{"name":"channel","labels":["email","chat","phone","social_media"]},
        \\{"name":"product","labels":["streaming","music","bundle"],"label_definitions":{"streaming":{"description":"The video streaming subscription"},"music":{"description":"The music subscription"},"bundle":{"description":"The combined streaming and music bundle"}}},
        \\{"name":"topics","labels":["billing","outage","account","content","device","privacy"],"multi_label":true,"threshold":0.4},
        \\{"name":"churn_risk","labels":["low","medium","high"],"prompt":"How likely is this customer to cancel within a month?"}]},
        \\"inputs":[{"content":"Hello, I have been a customer for six years and I am writing because my subscription renewed on April 15 for 5,400 yen even though the service had been down for most of the previous week. I tried to log in several times from my television and my phone, and each time the app showed an error saying the account could not be verified. I contacted support by chat twice and was told that an engineer would look into it, but nobody ever followed up. On top of that I noticed a second charge on my card for a music add-on that I never ordered. I would like both charges refunded, and I want someone to explain why my account was locked in the first place. If this cannot be resolved this week I will cancel everything and move to a different provider, which I would honestly regret because the catalog has always been excellent. Please call me back rather than sending another automated email, because the last three emails I received did not address any of my questions and one of them even asked me to reset a password I had already reset. A week later nothing has changed. The television app still says my account cannot be verified, the second charge is still on my statement, and the chat agent I spoke to this morning told me that refunds for outages are only issued as account credit, which is not acceptable to me because I intend to leave if this continues. I have attached screenshots of both charges and of the error message, and I have already reset my password twice as instructed."}]}
    ;
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    const item = &request.items[0];
    const processor_options = item.options.preprocessing((Options{}).processor);

    // The additive length model must agree with real tokenization for every
    // contiguous task range.
    var lengths = try PromptLengths.init(a, tokenizer.tokenizer(), item, processor_options);
    defer lengths.deinit(a);
    const task_count = item.compiled.schema.classifications.len;
    for (0..task_count) |start| for (start + 1..task_count + 1) |end| {
        const actual = try preparedLength(a, tokenizer.tokenizer(), item, item.text, .{ .start = start, .end = end }, processor_options);
        try std.testing.expectEqual(actual, try lengths.measure(start, end));
    };

    var whole = try itemClassificationLogits(&managed.backend, a, config, tokenizer.tokenizer(), item, .{ .max_prompt_tokens = 0 }, null);
    defer whole.deinit(a);
    const budget: usize = 400;
    var split = try itemClassificationLogits(&managed.backend, a, config, tokenizer.tokenizer(), item, .{ .max_prompt_tokens = budget }, null);
    defer split.deinit(a);
    std.debug.print("decide split: whole {d} tokens in {d} sequence, split {d} tokens in {d} sequences\n", .{ whole.prompt_tokens, whole.sequences, split.prompt_tokens, split.sequences });
    try std.testing.expect(whole.prompt_tokens > budget);
    try std.testing.expect(split.sequences > 1);
    try std.testing.expectEqual(whole.rows.len, split.rows.len);

    var disagreements: usize = 0;
    for (item.compiled.schema.classifications, whole.rows, split.rows) |classification, w, s| {
        try std.testing.expectEqual(w.len, s.len);
        for (s) |logit| try std.testing.expect(std.math.isFinite(logit));
        const best_w = std.mem.indexOfMax(f64, w);
        const best_s = std.mem.indexOfMax(f64, s);
        if (best_w != best_s) {
            disagreements += 1;
            std.debug.print("  task {s}: whole={s} split={s}\n", .{ classification.task.name, classification.task.labels[best_w], classification.task.labels[best_s] });
        }
    }
    std.debug.print("decide split: {d}/{d} top-label disagreements vs unsplit\n", .{ disagreements, whole.rows.len });
    try std.testing.expectEqual(@as(usize, 0), disagreements);
}

test "gliner span v2 GLiNER2.5-Decide execute serves the wire response and enforces limits" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const session = try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    var managed = try factory.getManagedComputeBackend(session, a, null, null);
    defer managed.deinit();

    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"decide","schema":{"classifications":[
        \\{"name":"intent","labels":["maintenance","room_change","checkout","billing","complaint","amenity_request"]},
        \\{"name":"topics","labels":["hvac","billing","housekeeping","noise","safety"],"multi_label":true,"threshold":0.4}]},
        \\"inputs":[{"content":"Guest in room 1408 says the AC has been out since yesterday and they want to move tonight or leave. They also asked for the incidentals hold to be released."}]}
    , .{});
    defer request.deinit();
    const response = try execute(&managed.backend, a, config, tokenizer.tokenizer(), &request, .{});
    defer a.free(response);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, response, .{});
    defer parsed.deinit();
    const decisions = parsed.value.object.get("data").?.array.items[0].object.get("classifications").?.array.items;
    try std.testing.expectEqualStrings("intent", decisions[0].object.get("name").?.string);
    try std.testing.expectEqualStrings("room_change", decisions[0].object.get("label").?.string);
    try std.testing.expectEqualStrings("topics", decisions[1].object.get("name").?.string);
    try std.testing.expectEqualStrings("hvac", decisions[1].object.get("label").?.string);
    try std.testing.expect(parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer > 0);

    // One sequence over the 512-token ceiling cannot run and cannot be split.
    var long_text = std.ArrayListUnmanaged(u8).empty;
    defer long_text.deinit(a);
    try long_text.appendSlice(a, "{\"schema_version\":2,\"model\":\"decide\",\"schema\":{\"classifications\":[{\"name\":\"answer\",\"labels\":[\"yes\",\"no\"]}]},\"inputs\":[{\"content\":\"");
    for (0..700) |_| try long_text.appendSlice(a, "word ");
    try long_text.appendSlice(a, "\"}]}");
    var too_long = try wire.parseJson(a, long_text.items, .{});
    defer too_long.deinit();
    try expectExecuteError(error.BoundarySequenceLimitExceeded, execute(&managed.backend, a, config, tokenizer.tokenizer(), &too_long, .{}));

    // Splitting beyond the per-item sequence budget is rejected before
    // encoding. A budget equal to the longer single-task prompt forces two
    // sequences (both tasks together are longer still).
    var lengths = try PromptLengths.init(a, tokenizer.tokenizer(), &request.items[0], request.items[0].options.preprocessing((Options{}).processor));
    defer lengths.deinit(a);
    const budget = @max(try lengths.measure(0, 1), try lengths.measure(1, 2));
    try std.testing.expect(try lengths.measure(0, 2) > budget);
    try expectExecuteError(error.ExtractionSchemaLimitExceeded, execute(&managed.backend, a, config, tokenizer.tokenizer(), &request, .{ .max_prompt_tokens = budget, .max_sequences_per_item = 1 }));
}

fn expectExecuteError(expected: anyerror, result: anyerror![]u8) !void {
    if (result) |unexpected| {
        std.testing.allocator.free(unexpected);
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expectEqual(expected, err);
}
