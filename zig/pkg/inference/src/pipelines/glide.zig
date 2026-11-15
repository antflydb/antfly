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
    /// Per-prompt token ceiling from the resolved executor contract. As for
    /// Laya, it bounds the model's input sequence: the rendered prompt
    /// (input, instructions and options), after encoder truncation.
    max_input_tokens: ?usize = null,
    /// Native forwards allocate their activations from the caller's bounded
    /// heap; micro-batches whose planned workspace exceeds this are split.
    max_workspace_bytes: ?usize = null,
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

/// Rendered and tokenized prompts, length-ordered for micro-batching. The
/// prepared ids and ordering live in `scratch` (a freeing allocator, so
/// tokenizer temporaries are returned as each prompt is encoded); the host
/// preprocessing permit stays held until `deinit`.
pub const Plan = struct {
    scratch: std.mem.Allocator,
    permit: sessions.RunPermit,
    prepared: []Prepared,
    prepared_count: usize = 0,
    order: []usize,
    lengths: []usize,
    /// Owned by the caller's result allocator `a`.
    item_tokens: []usize,
    prompt_tokens: usize = 0,
    retained: usize,
    /// Right-padding token id for micro-batches.
    pad: i64,

    pub fn deinit(self: *Plan) void {
        for (self.prepared[0..self.prepared_count]) |entry| self.scratch.free(entry.ids);
        self.scratch.free(self.prepared);
        self.scratch.free(self.order);
        self.scratch.free(self.lengths);
        self.permit.deinit();
        self.* = undefined;
    }
};

/// Host bytes that stay resident from tokenization until the response:
/// every prompt's ids (each token is at least one byte of rendered text),
/// the ordering arrays, and each prompt's option distribution.
pub fn retainedHostBytes(config: *const glide.DecisionConfig, states: []const []const u8, questions: []const prompt.Question) usize {
    const total = states.len *| questions.len;
    var retained: usize = total *| (@sizeOf(Prepared) + 2 * @sizeOf(usize) + @sizeOf([]const f64));
    retained +|= states.len *| @sizeOf(usize);
    for (states) |state| for (questions) |question| {
        retained +|= @min(promptBytes(config.codes, state, question) + 2, config.max_len) *| @sizeOf(i32);
        retained +|= question.options.len *| @sizeOf(f64);
    };
    return retained;
}

/// Allocations of the result belong to `a` (normally a request arena).
/// `scratch` backs prompts, tensors and model temporaries.
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
    var plan = try prepare(a, scratch, session, tok, config, states, questions, control, options);
    defer plan.deinit();
    return run(a, scratch, session, &plan, config, questions, control, options);
}

/// Render and tokenize every prompt under a host preprocessing permit. This
/// needs no execution lock, so callers prepare before serializing on the
/// model.
pub fn prepare(
    a: std.mem.Allocator,
    scratch: std.mem.Allocator,
    session: Session,
    tok: Tokenizer,
    config: *const glide.DecisionConfig,
    states: []const []const u8,
    questions: []const prompt.Question,
    control: ?Control,
    options: Options,
) !Plan {
    if (states.len == 0 or questions.len == 0) return error.InvalidGlideRequest;
    for (questions) |question| if (question.options.len == 0 or question.options.len > glide.max_options or question.options.len > config.codes.len) return error.InvalidGlideQuestion;
    const total = std.math.mul(usize, states.len, questions.len) catch return error.ExtractionRequestLimitExceeded;

    // Retained host bytes plus one rendered prompt and its tokenizer
    // temporaries at a time.
    const retained = retainedHostBytes(config, states, questions);
    var largest_text: usize = 0;
    for (states) |state| for (questions) |question| {
        largest_text = @max(largest_text, promptBytes(config.codes, state, question));
    };
    var permit = try session.admitHostPreprocess(retained +| largest_text *| 6);
    errdefer permit.deinit();
    trace.mark("pipeline host admission");

    const prepared = try scratch.alloc(Prepared, total);
    errdefer scratch.free(prepared);
    const order = try scratch.alloc(usize, total);
    errdefer scratch.free(order);
    const lengths = try scratch.alloc(usize, total);
    errdefer scratch.free(lengths);
    const item_tokens = try a.alloc(usize, states.len);
    errdefer a.free(item_tokens);
    @memset(item_tokens, 0);
    var plan = Plan{
        .scratch = scratch,
        .permit = permit,
        .prepared = prepared,
        .order = order,
        .lengths = lengths,
        .item_tokens = item_tokens,
        .retained = retained,
        .pad = blk: {
            const special = tok.specialTokens();
            break :blk if (special.pad_id >= 0) special.pad_id else 0;
        },
    };
    errdefer for (plan.prepared[0..plan.prepared_count]) |entry| scratch.free(entry.ids);

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(scratch);
    for (states, 0..) |state, item| for (questions) |question| {
        if (control) |active| try active.check();
        text.clearRetainingCapacity();
        try prompt.renderInto(scratch, &text, config.codes, state, question);
        const ids = try prompt.encode(scratch, tok, config, text.items);
        plan.prepared[plan.prepared_count] = .{ .ids = ids };
        plan.prepared_count += 1;
        if (options.max_input_tokens) |limit| if (ids.len > limit) return error.InferenceInputTokensExceeded;
        item_tokens[item] += ids.len;
        plan.prompt_tokens += ids.len;
    };
    text.clearAndFree(scratch);
    trace.mark("render+tokenize");

    // Stable ascending length order for budgeted micro-batches.
    for (order, 0..) |*index, i| index.* = i;
    std.sort.pdq(usize, order, @as([]const Prepared, prepared), struct {
        fn less(rows: []const Prepared, l: usize, r: usize) bool {
            return if (rows[l].ids.len == rows[r].ids.len) l < r else rows[l].ids.len < rows[r].ids.len;
        }
    }.less);
    for (lengths, order) |*len, row| len.* = prepared[row].ids.len;
    return plan;
}

/// Run a prepared plan's micro-batches. Callers serialize this on the model.
pub fn run(
    a: std.mem.Allocator,
    scratch: std.mem.Allocator,
    session: Session,
    plan: *const Plan,
    config: *const glide.DecisionConfig,
    questions: []const prompt.Question,
    control: ?Control,
    options: Options,
) !Result {
    const prepared = plan.prepared[0..plan.prepared_count];
    const order = plan.order;
    const lengths = plan.lengths;
    const retained = plan.retained;
    const total = prepared.len;

    const probabilities = try a.alloc([]const f64, total);
    @memset(probabilities, &.{});
    errdefer {
        for (probabilities) |values| if (values.len != 0) a.free(values);
        a.free(probabilities);
    }
    var chunks: usize = 0;
    var padded: usize = 0;
    var start: usize = 0;
    while (start < total) {
        if (control) |active| try active.check();
        const end = prompt.microBatchEnd(lengths, start, options.token_budget, max_batch_rows);
        trace.mark("chunk plan");
        var admitted = try admitChunk(session, prepared, order[start..end], retained, options.max_workspace_bytes);
        defer admitted.permit.deinit();
        trace.mark("chunk admission");
        const rows = order[start..][0..admitted.count];
        try runChunk(a, scratch, &admitted.permit, config, prepared, rows, questions, plan.pad, probabilities, control);
        chunks += 1;
        padded += rows.len * prepared[rows[rows.len - 1]].ids.len;
        start += admitted.count;
    }
    if (control) |active| try active.check();
    return .{
        .probabilities = probabilities,
        .item_tokens = plan.item_tokens,
        .prompt_tokens = plan.prompt_tokens,
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
/// capacity errors before any forward has started. With a workspace limit
/// (native forwards allocate activations on the caller's bounded heap), the
/// planned workspace of every micro-batch must also stay under it.
fn admitChunk(session: Session, prepared: []const Prepared, rows: []const usize, retained: usize, max_workspace_bytes: ?usize) !AdmittedChunk {
    var count = rows.len;
    while (true) {
        const request = try chunkPlan(session, prepared, rows[0..count], retained);
        const over_workspace = if (max_workspace_bytes) |limit| request.workspace_bytes > limit else false;
        if (!over_workspace and try session.fitsRun(request)) break;
        if (count == 1) {
            if (over_workspace) return error.ResourceLimitExceeded;
            break;
        }
        count = (count + 1) / 2;
    }
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
