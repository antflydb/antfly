// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! GLiDE decision execution (Fastino `glide/model.py`, `decide_batch`).
//!
//! Every (state, question) pair is one prompt (glide_prompt.zig). All prompts
//! of a request are rendered and tokenized before any execution permit is
//! taken, sorted by length, and packed into right-padded micro-batches under
//! a padded-token budget with the reference rule `(count + 1) * len <= budget`
//! (rows ascending, so `len` is the longest row of the batch). One forward per
//! micro-batch returns 255 readout logits per row; a softmax over the first
//! `n` (the question's option count) scaled by the checkpoint temperature is
//! the option distribution. Nothing is generated and there is no KV cache.

const std = @import("std");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Tensor = @import("../backends/tensor.zig").Tensor;
const sessions = @import("../backends/session.zig");
const Session = sessions.Session;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const glide = @import("../models/glide.zig");
pub const prompt = @import("glide_prompt.zig");

/// Padded tokens per forward micro-batch (reference DEFAULT_TOKEN_BUDGET).
pub const default_token_budget: usize = 16384;
/// Rows per forward; bounds the readout batch independently of the budget.
pub const max_batch_rows: usize = 128;

/// Opt-in per-phase wall-clock trace (`ANTFLY_GLIDE_TRACE=1`).
pub const trace = glide.trace;

pub const Options = struct {
    token_budget: usize = default_token_budget,
    /// Per-prompt token ceiling from the resolved executor contract.
    max_input_tokens: ?usize = null,
};

pub const Result = struct {
    /// Option distributions, item-major then question order: entry
    /// `i * questions.len + q` has `questions[q].options.len` probabilities.
    probabilities: []const []const f64,
    /// Prompt tokens per item (special tokens included, after truncation).
    item_tokens: []const usize,
    prompt_tokens: usize,
    execution_chunks: usize = 0,
    padded_tokens: usize = 0,
};

/// Upper bound on a rendered prompt's bytes (before Python rstrip).
pub fn promptBytes(codes: []const []const u8, state: []const u8, question: prompt.Question) usize {
    var bytes = prompt.prompt_prefix.len + prompt.prompt_suffix.len + prompt.body_tail.len + 32 + state.len + question.instructions.len;
    for (question.options, 0..) |option, i| bytes += codes[@min(i, codes.len - 1)].len + option.key.len + option.description.len + 6;
    return bytes;
}

const Prepared = struct { ids: []i32 };

/// Allocations of the result belong to `a` (normally a request arena).
/// `scratch` backs tensors and model temporaries.
pub fn execute(
    a: std.mem.Allocator,
    scratch: std.mem.Allocator,
    session: Session,
    tok: Tokenizer,
    config: *const glide.DecisionConfig,
    states: []const []const u8,
    questions: []const prompt.Question,
    control: ?Control,
    options: Options,
) !Result {
    if (states.len == 0 or questions.len == 0) return error.InvalidGlideRequest;
    for (questions) |question| if (question.options.len == 0 or question.options.len > glide.max_options or question.options.len > config.codes.len) return error.InvalidGlideQuestion;
    const total = std.math.mul(usize, states.len, questions.len) catch return error.ExtractionRequestLimitExceeded;

    // Host residency for every prompt's ids (each token is at least one
    // byte of rendered text) plus one rendered prompt at a time.
    var retained: usize = total * (@sizeOf(Prepared) + @sizeOf(usize));
    var largest_text: usize = 0;
    for (states) |state| for (questions) |question| {
        const bytes = promptBytes(config.codes, state, question);
        largest_text = @max(largest_text, bytes);
        retained +|= @min(bytes + 2, config.max_len) * @sizeOf(i32);
    };
    var permit = try session.admitHostPreprocess(retained +| largest_text *| 6);
    defer permit.deinit();
    trace.mark("pipeline host admission");

    // Render and tokenize everything before any execution permit.
    const prepared = try a.alloc(Prepared, total);
    defer a.free(prepared);
    var prepared_count: usize = 0;
    defer for (prepared[0..prepared_count]) |entry| a.free(entry.ids);
    const item_tokens = try a.alloc(usize, states.len);
    @memset(item_tokens, 0);
    var prompt_tokens: usize = 0;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(scratch);
    for (states, 0..) |state, item| for (questions) |question| {
        if (control) |active| try active.check();
        text.clearRetainingCapacity();
        try prompt.renderInto(scratch, &text, config.codes, state, question);
        const ids = try prompt.encode(a, tok, config, text.items);
        prepared[prepared_count] = .{ .ids = ids };
        prepared_count += 1;
        if (options.max_input_tokens) |limit| if (ids.len > limit) return error.InferenceInputTokensExceeded;
        item_tokens[item] += ids.len;
        prompt_tokens += ids.len;
    };
    text.clearAndFree(scratch);
    trace.mark("render+tokenize");

    // Stable ascending length order, then budgeted micro-batches.
    const order = try a.alloc(usize, total);
    defer a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    std.sort.pdq(usize, order, @as([]const Prepared, prepared), struct {
        fn less(rows: []const Prepared, l: usize, r: usize) bool {
            return if (rows[l].ids.len == rows[r].ids.len) l < r else rows[l].ids.len < rows[r].ids.len;
        }
    }.less);

    const lengths = try a.alloc(usize, total);
    defer a.free(lengths);
    for (lengths, order) |*len, row| len.* = prepared[row].ids.len;

    const probabilities = try a.alloc([]const f64, total);
    @memset(probabilities, &.{});
    errdefer {
        for (probabilities) |values| if (values.len != 0) a.free(values);
        a.free(probabilities);
    }
    const pad: i64 = blk: {
        const special = tok.specialTokens();
        break :blk if (special.pad_id >= 0) special.pad_id else 0;
    };
    var chunks: usize = 0;
    var padded: usize = 0;
    var start: usize = 0;
    while (start < total) {
        if (control) |active| try active.check();
        const end = prompt.microBatchEnd(lengths, start, options.token_budget, max_batch_rows);
        trace.mark("chunk plan");
        var admitted = try admitChunk(session, prepared, order[start..end], retained);
        defer admitted.permit.deinit();
        trace.mark("chunk admission");
        const rows = order[start..][0..admitted.count];
        try runChunk(a, scratch, &admitted.permit, config, prepared, rows, questions, pad, probabilities, control);
        chunks += 1;
        padded += rows.len * prepared[rows[rows.len - 1]].ids.len;
        start += admitted.count;
    }
    if (control) |active| try active.check();
    return .{
        .probabilities = probabilities,
        .item_tokens = item_tokens,
        .prompt_tokens = prompt_tokens,
        .execution_chunks = chunks,
        .padded_tokens = padded,
    };
}

const AdmittedChunk = struct { count: usize, permit: sessions.RunPermit };

fn chunkPlan(session: Session, prepared: []const Prepared, rows: []const usize, retained: usize) !sessions.RunRequest {
    const batch: i64 = @intCast(rows.len);
    const sequence: i64 = @intCast(prepared[rows[rows.len - 1]].ids.len);
    var request = try session.planShapes(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ batch, sequence } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ batch, sequence } },
        .{ .name = "readout_index", .dtype = .i64, .shape = &.{batch} },
    }, rows.len);
    // Tensor constructors copy the padded inputs; the preprocessing permit
    // already owns the prepared ids and accumulated results.
    request.host_preprocess_bytes = try std.math.add(usize, retained, request.input_bytes);
    request.pre_admitted_host_bytes = retained;
    return request;
}

/// Admit the largest prefix of `rows` (ascending length) that fits; halve on
/// capacity errors before any forward has started.
fn admitChunk(session: Session, prepared: []const Prepared, rows: []const usize, retained: usize) !AdmittedChunk {
    var count = rows.len;
    while (count > 1 and !try session.fitsRun(try chunkPlan(session, prepared, rows[0..count], retained))) count = (count + 1) / 2;
    while (true) {
        const permit = session.admit(try chunkPlan(session, prepared, rows[0..count], retained)) catch |err| {
            if ((err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) or count == 1) return err;
            count = (count + 1) / 2;
            continue;
        };
        return .{ .count = count, .permit = permit };
    }
}

fn runChunk(
    a: std.mem.Allocator,
    scratch: std.mem.Allocator,
    permit: *sessions.RunPermit,
    config: *const glide.DecisionConfig,
    prepared: []const Prepared,
    rows: []const usize,
    questions: []const prompt.Question,
    pad: i64,
    probabilities: [][]const f64,
    control: ?Control,
) !void {
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    const chunk = arena.allocator();
    const batch = rows.len;
    const seq = prepared[rows[batch - 1]].ids.len;
    const ids = try chunk.alloc(i64, batch * seq);
    @memset(ids, pad);
    const mask = try chunk.alloc(i64, ids.len);
    @memset(mask, 0);
    const readout = try chunk.alloc(i64, batch);
    for (rows, 0..) |row, b| {
        const source = prepared[row].ids;
        for (ids[b * seq ..][0..source.len], source) |*dest, id| dest.* = id;
        @memset(mask[b * seq ..][0..source.len], 1);
        readout[b] = @intCast(try config.readoutIndex(source.len));
    }
    var inputs: [3]Tensor = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |*input| input.deinit();
    inputs[0] = try Tensor.initInt64(chunk, "input_ids", &.{ @intCast(batch), @intCast(seq) }, ids);
    initialized += 1;
    inputs[1] = try Tensor.initInt64(chunk, "attention_mask", &.{ @intCast(batch), @intCast(seq) }, mask);
    initialized += 1;
    inputs[2] = try Tensor.initInt64(chunk, "readout_index", &.{@intCast(batch)}, readout);
    initialized += 1;
    trace.mark("batch tensors");
    const outputs = try permit.runWithControl(&inputs, scratch, control);
    trace.mark("session run");
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != 1 or outputs[0].dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    if (logits.len != batch * glide.max_options) return error.UnexpectedOutputShape;
    for (rows, 0..) |row, b| {
        const question = questions[row % questions.len];
        const n = question.options.len;
        const values = logits[b * glide.max_options ..][0..glide.max_options];
        for (values[0..n]) |v| if (!std.math.isFinite(v)) return error.InvalidGlideOutput;
        const out = try a.alloc(f64, n);
        errdefer a.free(out);
        try glide.optionProbabilities(values, n, config.temperature, out);
        probabilities[row] = out;
    }
    trace.mark("softmax");
}
