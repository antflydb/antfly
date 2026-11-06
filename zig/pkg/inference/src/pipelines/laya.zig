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

const std = @import("std");
const platform = @import("antfly_platform");
const model = @import("../models/laya.zig");
const tree = @import("laya_tree.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Session = @import("../backends/session.zig").Session;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Question = struct {
    name: []const u8,
    kind: model.QuestionType,
    instruction: []const u8,
    labels: []const []const u8,
    descriptions: []const []const u8,
};
pub const Task = struct { text: []const u8, question: Question };
pub const Sequence = struct { ids: []i64, markers: []i64 };
pub const Decision = struct {
    name: []const u8,
    kind: model.QuestionType,
    label: []const u8,
    labels: []const []const u8,
    probabilities: []f32,
    confidence: f32,
    expected_value: ?f32 = null,
    true_probability: ?f32 = null,
    /// Null for checkpoints without an action head (`format: opendecider`).
    act_probability: ?f32,
};
pub const Result = struct { decisions: []Decision, prompt_tokens: usize, execution_chunks: usize = 0, padded_tokens: usize = 0 };

pub fn encodeClean(a: std.mem.Allocator, tok: Tokenizer, text: []const u8, mask: []const u8) ![]i32 {
    if (mask.len == 0) return error.InvalidLayaTokenizer;
    const clean = try std.mem.replaceOwned(u8, a, text, mask, " ");
    defer a.free(clean);
    return tok.encode(a, clean);
}

/// Upstream option text; every option token run is `[MASK]` plus at most 48 tokens.
pub const max_option_tokens = 48;

/// Question text and option tokens with upstream's shared `head_max_len`
/// budget. `options` are untruncated; `per` caps each `[MASK]`+option run
/// and `head_len` caps the question text when all options share the budget.
pub const QuestionTokens = struct {
    head: []i32,
    options: [][]i32,
    per: usize,
    options_len: usize,
    head_len: usize,
    /// Most option tokens after each `[MASK]`; OpenDecider keeps every one.
    option_cap: usize = max_option_tokens,

    pub fn deinit(self: QuestionTokens, a: std.mem.Allocator) void {
        for (self.options) |ids| a.free(ids);
        a.free(self.options);
        a.free(self.head);
    }

    /// Option run length under the shared budget, including its `[MASK]` marker.
    pub fn optionRun(self: QuestionTokens, i: usize) usize {
        return @min(1 + @min(self.options[i].len, self.option_cap), self.per);
    }
};

pub fn questionTokens(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, q: Question) !QuestionTokens {
    if (q.labels.len < 2 or q.labels.len > cfg.maxOptions() or q.labels.len != q.descriptions.len) return error.InvalidLayaQuestion;
    const mask = cfg.mask_token[0..cfg.mask_token_len];
    if (cfg.format == .opendecider) return openDeciderTokens(a, tok, mask, q);
    const head_text = try std.fmt.allocPrint(a, "{s} question: {s}", .{ @tagName(q.kind), q.instruction });
    defer a.free(head_text);
    const head = try encodeClean(a, tok, head_text, mask);
    errdefer a.free(head);
    const options = try a.alloc([]i32, q.labels.len);
    errdefer a.free(options);
    var initialized: usize = 0;
    errdefer for (options[0..initialized]) |ids| a.free(ids);
    var options_len: usize = 0;
    for (q.labels, q.descriptions, 0..) |label, desc, i| {
        const text = switch (q.kind) {
            .choice => if (desc.len == 0) try std.fmt.allocPrint(a, " {s}", .{label}) else try std.fmt.allocPrint(a, " {s}: {s}", .{ label, desc }),
            .score => try std.fmt.allocPrint(a, " level {d}: {s}", .{ i, if (desc.len == 0) label else desc }),
            .noul => try std.fmt.allocPrint(a, " {s}: {s}", .{ label, if (desc.len > 0) desc else if (i == 0) "no, the statement does not hold" else "yes, the statement holds" }),
        };
        defer a.free(text);
        options[i] = try encodeClean(a, tok, text, mask);
        initialized += 1;
        options_len += 1 + @min(options[i].len, max_option_tokens);
    }
    const per: usize = if (options_len + 16 > cfg.head_max_len) @max(4, (cfg.head_max_len - 16) / options.len) else max_option_tokens + 1;
    options_len = 0;
    for (options) |ids| options_len += @min(1 + @min(ids.len, max_option_tokens), per);
    const budget = cfg.head_max_len -| options_len;
    return .{ .head = head, .options = options, .per = per, .options_len = options_len, .head_len = @min(head.len, @max(8, budget)) };
}

/// OpenDecider-nano's question and option text (opendecider/prompt.py,
/// `nano_ids`), with no shared budget and no cap on option length. Yes/no
/// options are named "yes" and "no", described "Yes" and "No" by default.
fn openDeciderTokens(a: std.mem.Allocator, tok: Tokenizer, mask: []const u8, q: Question) !QuestionTokens {
    const head_text = try std.fmt.allocPrint(a, "question: {s}", .{q.instruction});
    defer a.free(head_text);
    const head = try encodeClean(a, tok, head_text, mask);
    errdefer a.free(head);
    const options = try a.alloc([]i32, q.labels.len);
    errdefer a.free(options);
    var initialized: usize = 0;
    errdefer for (options[0..initialized]) |ids| a.free(ids);
    var options_len: usize = 0;
    for (q.labels, q.descriptions, 0..) |label, desc, i| {
        const name: []const u8 = if (q.kind == .noul) (if (i == 1) "yes" else "no") else label;
        const shown: []const u8 = if (desc.len > 0) desc else if (q.kind == .noul) (if (i == 1) "Yes" else "No") else "";
        const text = if (shown.len == 0 or std.mem.eql(u8, shown, name)) try std.fmt.allocPrint(a, " {s}", .{name}) else try std.fmt.allocPrint(a, " {s}: {s}", .{ name, shown });
        defer a.free(text);
        options[i] = try encodeClean(a, tok, text, mask);
        initialized += 1;
        options_len += 1 + options[i].len;
    }
    return .{ .head = head, .options = options, .per = std.math.maxInt(usize), .options_len = options_len, .head_len = head.len, .option_cap = std.math.maxInt(usize) };
}

/// Allocations belong to the caller's request arena. State overflow is rejected,
/// unlike upstream's silent truncation; question/option formatting matches it.
pub fn prepare(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, task: Task) !Sequence {
    const q = task.question;
    if (q.labels.len > model.max_options) return error.InvalidLayaQuestion;
    const special = tok.specialTokens();
    const mask = cfg.mask_token[0..cfg.mask_token_len];
    const tokens = try questionTokens(a, tok, cfg, q);
    defer tokens.deinit(a);
    const head = tokens.head;
    const options = tokens.options;
    const options_len = tokens.options_len;
    const head_len = tokens.head_len;
    const state_text = if (cfg.format == .opendecider) try std.fmt.allocPrint(a, "input: {s}", .{task.text}) else try a.dupe(u8, task.text);
    defer a.free(state_text);
    const encoded = try encodeClean(a, tok, state_text, mask);
    defer a.free(encoded);
    const state = if (cfg.truncate_state) encoded[0..@min(encoded.len, cfg.max_len -| (4 + head_len + options_len))] else encoded;
    const total = 4 + head_len + options_len + state.len;
    if (total > cfg.max_len) return error.ExtractionTextLimitExceeded;
    const ids = try a.alloc(i64, total);
    errdefer a.free(ids);
    const markers = try a.alloc(i64, options.len);
    var pos: usize = 0;
    ids[pos] = special.cls_id;
    pos += 1;
    for (head[0..head_len]) |id| {
        ids[pos] = id;
        pos += 1;
    }
    ids[pos] = special.sep_id;
    pos += 1;
    // OpenDecider lists "yes" before "no"; markers stay in label order.
    const reversed = cfg.format == .opendecider and q.kind == .noul;
    for (0..options.len) |k| {
        const i = if (reversed) options.len - 1 - k else k;
        markers[i] = @intCast(pos);
        ids[pos] = special.mask_id;
        pos += 1;
        for (options[i][0 .. tokens.optionRun(i) - 1]) |id| {
            ids[pos] = id;
            pos += 1;
        }
    }
    ids[pos] = special.sep_id;
    pos += 1;
    for (state) |id| {
        ids[pos] = id;
        pos += 1;
    }
    ids[pos] = special.sep_id;
    return .{ .ids = ids, .markers = markers };
}

pub fn decode(a: std.mem.Allocator, cfg: model.Config, q: Question, logits: []const f32, action: []const f32) !Decision {
    if (logits.len != q.labels.len or action.len != cfg.n_act) return error.UnexpectedOutputShape;
    const probabilities = try a.alloc(f32, logits.len);
    errdefer a.free(probabilities);
    try softmax(logits, cfg.scale(q.kind, logits.len), probabilities);
    if (cfg.n_act == 0) return finalize(q, probabilities, null);
    var acts: [33]f32 = undefined;
    try softmax(action, 1, acts[0..action.len]);
    return finalize(q, probabilities, acts[0]);
}

/// Build a `Decision` from an already-computed probability distribution
/// (`probabilities`, which the result owns). Used both by `decode`, from a
/// softmax, and by two-stage choice's merged distribution
/// (`mergeTwoStage`), which never runs its own softmax over the full label set.
pub fn finalize(q: Question, probabilities: []f32, act_probability: ?f32) Decision {
    var winner: usize = 0;
    var entropy: f32 = 0;
    var expected: f32 = 0;
    for (probabilities, 0..) |p, i| {
        if (p > probabilities[winner]) winner = i;
        entropy -= p * @log(@max(p, 1e-12));
        expected += @as(f32, @floatFromInt(i)) * p;
    }
    return .{
        .name = q.name,
        .kind = q.kind,
        .label = q.labels[winner],
        .labels = q.labels,
        .probabilities = probabilities,
        .confidence = if (q.kind == .noul) @max(probabilities[1], 1 - probabilities[1]) else std.math.clamp(1 - entropy / @log(@as(f32, @floatFromInt(probabilities.len))), 0, 1),
        .expected_value = if (q.kind == .score) expected else null,
        .true_probability = if (q.kind == .noul) probabilities[1] else null,
        .act_probability = act_probability,
    };
}
fn softmax(logits: []const f32, scale: f32, out: []f32) !void {
    if (logits.len == 0 or logits.len != out.len) return error.UnexpectedOutputShape;
    var max: f32 = -std.math.inf(f32);
    for (logits) |z| {
        if (!std.math.isFinite(z)) return error.InvalidLayaOutput;
        max = @max(max, z);
    }
    var total: f32 = 0;
    for (logits, out) |z, *p| {
        p.* = @exp((z - max) / scale);
        total += p.*;
    }
    for (out) |*p| p.* /= total;
}

pub fn execute(a: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, control: ?Control) !Result {
    return executeWithTokenLimit(a, session, tok, cfg, tasks, control, null);
}

pub fn executeWithTokenLimit(a: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, control: ?Control, max_input_tokens: ?usize) !Result {
    return executeWithScratch(a, a, session, tok, cfg, tasks, control, max_input_tokens);
}

/// Keep model temporaries on a freeing allocator. A request arena retains every
/// intermediate across encoder layers even after ComputeBackend.free, which can
/// exhaust the bounded serving heap on the released 28-layer CPU checkpoint.
pub fn executeWithScratch(a: std.mem.Allocator, scratch: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, control: ?Control, max_input_tokens: ?usize) !Result {
    return executeWithScratchLimit(a, scratch, session, tok, cfg, tasks, control, max_input_tokens, null);
}

/// Bound each unpacked forward's workspace when its scratch allocator has a tighter
/// ceiling than the session's process-wide admission limits.
pub fn executeWithScratchLimit(a: std.mem.Allocator, scratch: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, control: ?Control, max_input_tokens: ?usize, max_workspace_bytes: ?usize) !Result {
    if (tasks.len == 0 or tasks.len > 512) return error.ExtractionRequestLimitExceeded;
    if (cfg.packing.enabled()) return executePacked(a, scratch, session, tok, cfg, tasks, control, max_input_tokens);
    var permit = try session.admitHostPreprocess(tasks.len * cfg.max_len * 64);
    defer permit.deinit();
    const sequences = try a.alloc(Sequence, tasks.len);
    defer a.free(sequences);
    var prepared_count: usize = 0;
    defer for (sequences[0..prepared_count]) |sequence| {
        a.free(sequence.ids);
        a.free(sequence.markers);
    };
    var tokens: usize = 0;
    // Validate and tokenize every task before acquiring any execution permit.
    for (tasks, sequences) |task, *prepared| {
        if (control) |active| try active.check();
        prepared.* = try prepare(a, tok, cfg, task);
        prepared_count += 1;
        if (max_input_tokens) |limit| if (prepared.ids.len > limit) return error.InferenceInputTokensExceeded;
        tokens += prepared.ids.len;
    }
    // Group similar lengths so a chunk pads to its own longest input rather
    // than the request's.
    const bucketing = if (session.backend() == .cuda)
        platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_OPTIMIZATIONS", true) and platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_BUCKETING", true)
    else
        platform.env.getenvBoolDefault("ANTFLY_LAYA_BUCKETING", true);
    const order = if (bucketing) try bucketOrder(a, sequences) else null;
    defer if (order) |indices| a.free(indices);
    const ordered_tasks = if (order != null) try a.alloc(Task, tasks.len) else null;
    defer if (ordered_tasks) |value| a.free(value);
    const ordered_sequences = if (order != null) try a.alloc(Sequence, sequences.len) else null;
    defer if (ordered_sequences) |value| a.free(value);
    if (order) |indices| for (indices, 0..) |original, i| {
        ordered_tasks.?[i] = tasks[original];
        ordered_sequences.?[i] = sequences[original];
    };
    const execution_tasks = ordered_tasks orelse tasks;
    const execution_sequences = ordered_sequences orelse sequences;
    const decisions = try a.alloc(Decision, tasks.len);
    var finished: usize = 0;
    errdefer {
        for (decisions[0..finished]) |decision| a.free(decision.probabilities);
        a.free(decisions);
    }
    var execution_chunks: usize = 0;
    var padded_tokens: usize = 0;
    const retained = tasks.len * cfg.max_len * 64;
    while (finished < tasks.len) {
        if (control) |active| try active.check();
        var end = tasks.len;
        if (order != null) {
            end = finished + 1;
            while (end < tasks.len and lengthBucket(execution_sequences[end]) == lengthBucket(execution_sequences[finished])) : (end += 1) {}
        }
        const remaining = execution_sequences[finished..end];
        var admitted = try admitChunk(session, remaining, retained, max_workspace_bytes);
        defer admitted.permit.deinit();
        try executeChunk(a, scratch, &admitted.permit, &permit, cfg, execution_tasks[finished..][0..admitted.count], remaining[0..admitted.count], tok.specialTokens().pad_id, decisions[finished..][0..admitted.count], control);
        execution_chunks += 1;
        padded_tokens += admitted.count * chunkShape(remaining[0..admitted.count]).sequence;
        finished += admitted.count;
    }
    if (control) |active| try active.check();
    const result = if (order) |indices| blk: {
        const restored = try a.alloc(Decision, decisions.len);
        for (indices, decisions) |original, decision| restored[original] = decision;
        a.free(decisions);
        break :blk restored;
    } else decisions;
    return .{ .decisions = result, .prompt_tokens = tokens, .execution_chunks = execution_chunks, .padded_tokens = padded_tokens };
}

/// One packed row (or a group of rows for one state that overflowed a single
/// row) plus the absolute task index of each of its questions.
const Planned = struct { row: tree.Row, members: []const usize, text: []const u8 };

/// Tree-packed execution: every task that shares a state text shares one
/// trunk encoding (see pipelines/laya_tree.zig). Rows are built and validated
/// for the whole request before any model work is admitted.
fn executePacked(a: std.mem.Allocator, scratch: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, control: ?Control, max_input_tokens: ?usize) !Result {
    var permit = try session.admitHostPreprocess(tasks.len * cfg.max_len * 64);
    defer permit.deinit();
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    const plan = arena.allocator();
    // Group tasks by state text in first-appearance order.
    var groups: std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(usize)) = .empty;
    for (tasks, 0..) |task, i| {
        const entry = try groups.getOrPut(plan, task.text);
        if (!entry.found_existing) entry.value_ptr.* = .empty;
        try entry.value_ptr.append(plan, i);
    }
    var rows: std.ArrayListUnmanaged(Planned) = .empty;
    var tokens: usize = 0;
    for (groups.keys(), groups.values()) |text, members| {
        if (control) |active| try active.check();
        const questions = try plan.alloc(Question, members.items.len);
        for (questions, members.items) |*q, index| q.* = tasks[index].question;
        for (try tree.build(plan, tok, cfg, text, questions, null)) |row| {
            if (max_input_tokens) |limit| if (row.ids.len > limit) return error.InferenceInputTokensExceeded;
            tokens += row.ids.len;
            try rows.append(plan, .{ .row = row, .members = members.items, .text = text });
        }
    }
    const done = try plan.alloc(bool, tasks.len);
    @memset(done, false);
    const decisions = try a.alloc(Decision, tasks.len);
    var decoded: usize = 0;
    errdefer {
        for (decisions, done) |decision, filled| if (filled) a.free(decision.probabilities);
        a.free(decisions);
    }
    // Several rows, possibly from different states, share one session call
    // when their combined physical length fits the row budget (models/laya
    // LAYA.md, "Segment attention"). Segment attention already keeps cost
    // proportional to visible keys, so a batched call does the same work as
    // separate calls, minus their per-call overhead. Greedy in request
    // order: add the next row while it still fits, then flush.
    const batching = platform.env.getenvBoolDefault("ANTFLY_LAYA_PACKED_BATCH", true);
    const batch_limit = cfg.packing.max_packed_len;
    var start: usize = 0;
    var execution_chunks: usize = 0;
    while (start < rows.items.len) {
        if (control) |active| try active.check();
        var end = start + 1;
        var used = rows.items[start].row.ids.len;
        if (batching) while (end < rows.items.len) : (end += 1) {
            const next = rows.items[end].row.ids.len;
            if (used + next > batch_limit) break;
            used += next;
        };
        try runPackedBatch(a, plan, scratch, session, tok, cfg, tasks, rows.items[start..end], decisions, done, &decoded, control, &tokens, &permit);
        execution_chunks += 1;
        start = end;
    }
    if (decoded != tasks.len) return error.UnexpectedOutputShape;
    if (control) |active| try active.check();
    return .{ .decisions = decisions, .prompt_tokens = tokens, .execution_chunks = execution_chunks };
}

/// Run one session call over `batch`, a run of `Planned` rows (one call
/// already, or several coalesced into one physical row). `plan` is an arena
/// scoped to the whole request; `scratch` backs the model's temporaries.
fn runPackedBatch(a: std.mem.Allocator, plan: std.mem.Allocator, scratch: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, tasks: []const Task, batch: []const Planned, decisions: []Decision, done: []bool, decoded: *usize, control: ?Control, tokens: *usize, preprocessing: ?*const @import("../backends/session.zig").RunPermit) !void {
    // `plan` is the request arena; a coalesced row's memory is reclaimed
    // with everything else when `executePacked` tears it down.
    var row: tree.Row = undefined;
    var owners: []const usize = &.{};
    if (batch.len == 1) {
        row = batch[0].row;
    } else {
        const sub_rows = try plan.alloc(tree.Row, batch.len);
        for (sub_rows, batch) |*dst, planned| dst.* = planned.row;
        const merged = try tree.coalesce(plan, sub_rows);
        row = merged.row;
        owners = merged.owners;
    }
    const questions = row.questions();
    var execution: @import("../backends/session.zig").RunPermit = .{ .session = session, .lease = null };
    defer execution.deinit();
    var source = @import("../backends/session.zig").PreparedSource{};
    defer source.deinit();
    const outputs = try runPackedRow(&execution, &source, preprocessing, session, scratch, row, questions, control);
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    const acts = outputs[1].asFloat32();
    if (logits.len != questions * row.width or acts.len != questions * cfg.n_act) return error.UnexpectedOutputShape;
    for (row.question_index, 0..) |local, qi| {
        const owner = if (owners.len > 0) owners[qi] else 0;
        const index = batch[owner].members[local];
        const q = tasks[index].question;
        var decision = try decode(a, cfg, q, logits[qi * row.width ..][0..q.labels.len], acts[qi * cfg.n_act ..][0..cfg.n_act]);
        // Two-stage choice (LAYA.md, step 2b): refine many-option choices by
        // comparing the stage-1 finalists jointly off the same state.
        if (cfg.packing.mode == .candidate and cfg.packing.two_stage.enabled() and q.kind == .choice and q.labels.len > cfg.packing.two_stage.top_k)
            decision = try refineTwoStage(a, plan, scratch, session, tok, cfg, batch[owner].text, q, decision, control, tokens, preprocessing);
        decisions[index] = decision;
        done[index] = true;
        decoded.* += 1;
    }
}

fn runPackedRow(execution: *@import("../backends/session.zig").RunPermit, source: *@import("../backends/session.zig").PreparedSource, preprocessing: ?*const @import("../backends/session.zig").RunPermit, session: Session, scratch: std.mem.Allocator, row: tree.Row, questions: usize, control: ?Control) ![]Tensor {
    const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
    const n: i64 = @intCast(row.ids.len);
    const q: i64 = @intCast(questions);
    const signature = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, @intCast(row.parents.len) } },
        .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ q, @intCast(row.width) } },
        .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ q, 1 } },
    };
    var request = try session.planShapes(&signature, 1);
    request.host_preprocess_bytes = request.input_bytes;
    execution.* = try session.admitShapeRequest(.{ .host_shapes = &signature }, request, scratch, control);
    var inputs: [7]Tensor = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |*input| input.deinit();
    // Success and every error/cancel end Model/lane and all header borrows
    // BEFORE these helper-local inputs die. Source funding remains caller-owned
    // through output/decode cleanup; ColdCall retains copied values/pins only.
    defer source.finishInputLifetime();
    for ([_][]const u8{ "input_ids", "position_ids", "token_segment", "segment_parent", "token_qtype", "marker_pos", "anchor_pos" }, [_][]const i64{ row.ids, row.positions, row.segments, row.parents, row.kinds, row.markers, row.anchors }, 0..) |name, values, i| {
        const shape: [2]i64 = switch (i) {
            3 => .{ 1, @intCast(row.parents.len) },
            5 => .{ q, @intCast(row.width) },
            6 => .{ q, 1 },
            else => .{ 1, n },
        };
        inputs[i] = try Tensor.initInt64(scratch, name, &shape, values);
        initialized += 1;
    }
    if (try source.initOwningSynchronousHostTaskPermitBeforeExecutionInto(execution, preprocessing, &inputs, scratch, control)) {
        try source.enter();
        const outputs = try source.runHost(&inputs);
        errdefer {
            for (outputs) |*output| output.deinit();
            scratch.free(outputs);
        }
        _ = try source.finishSynchronousHostTaskWorkspaceAfterUnlock();
        return outputs;
    }
    return execution.runWithControl(&inputs, scratch, control);
}

/// Indices of the highest-probability options, ascending, capped at `top_k`
/// and (once at least 2 are kept) at cumulative mass `mass_cutoff` (0
/// disables the cutoff). Used only by two-stage choice (roadmap 2b).
fn selectFinalists(a: std.mem.Allocator, probabilities: []const f32, top_k: usize, mass_cutoff: f32) ![]usize {
    const order = try a.alloc(usize, probabilities.len);
    defer a.free(order);
    for (order, 0..) |*o, i| o.* = i;
    const Ctx = struct {
        fn more(probs: []const f32, l: usize, r: usize) bool {
            return probs[l] > probs[r];
        }
    };
    std.mem.sort(usize, order, probabilities, Ctx.more);
    var count: usize = 0;
    var mass: f32 = 0;
    const cap = @min(top_k, order.len);
    while (count < cap) {
        if (mass_cutoff > 0 and count >= 2 and mass >= mass_cutoff) break;
        mass += probabilities[order[count]];
        count += 1;
    }
    const out = try a.dupe(usize, order[0..count]);
    std.mem.sort(usize, out, {}, std.sort.asc(usize));
    return out;
}

/// Tokens of a row outside its trunk(s).
fn branchTokens(row: tree.Row) usize {
    var count: usize = 0;
    for (row.kinds) |kind| count += @intFromBool(kind != tree.trunk_kind);
    return count;
}

/// Two-stage choice (LAYA.md, roadmap 2b). Stage 1 (`stage1`, already
/// decoded) scored every option in its own candidate branch; this packs the
/// surviving finalists into one joint branch off the same trunk (so they can
/// compare each other) and blends its distribution back in: each finalist's
/// probability becomes the stage-1 mass captured by the shortlist times its
/// stage-2 share of that mass, and every other option keeps its stage-1
/// probability. The mix therefore still sums to 1. Always consumes `stage1`.
/// `prompt_tokens` counts each input token once, so this adds only the joint
/// branch's tokens to `*tokens`: the trunk is the same state stage 1 already
/// counted, whether or not it is re-encoded here.
fn refineTwoStage(a: std.mem.Allocator, plan: std.mem.Allocator, scratch: std.mem.Allocator, session: Session, tok: Tokenizer, cfg: model.Config, text: []const u8, q: Question, stage1: Decision, control: ?Control, tokens: *usize, preprocessing: ?*const @import("../backends/session.zig").RunPermit) !Decision {
    errdefer a.free(stage1.probabilities);
    const finalists = try selectFinalists(plan, stage1.probabilities, cfg.packing.two_stage.top_k, cfg.packing.two_stage.mass_cutoff);
    std.debug.assert(finalists.len >= 2);
    const labels = try plan.alloc([]const u8, finalists.len);
    const descriptions = try plan.alloc([]const u8, finalists.len);
    for (finalists, labels, descriptions) |idx, *l, *d| {
        l.* = q.labels[idx];
        d.* = q.descriptions[idx];
    }
    const shortlist = Question{ .name = q.name, .kind = q.kind, .instruction = q.instruction, .labels = labels, .descriptions = descriptions };
    const rows = try tree.build(plan, tok, cfg, text, &.{shortlist}, .question);
    if (rows.len != 1) return error.UnexpectedOutputShape;
    const row = rows[0];
    tokens.* += branchTokens(row);
    var execution: @import("../backends/session.zig").RunPermit = .{ .session = session, .lease = null };
    defer execution.deinit();
    var source = @import("../backends/session.zig").PreparedSource{};
    defer source.deinit();
    const outputs = try runPackedRow(&execution, &source, preprocessing, session, scratch, row, 1, control);
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    const acts = outputs[1].asFloat32();
    if (logits.len != row.width or acts.len != cfg.n_act) return error.UnexpectedOutputShape;
    const stage2 = try decode(scratch, cfg, shortlist, logits[0..labels.len], acts[0..cfg.n_act]);
    defer scratch.free(stage2.probabilities);
    var mass: f32 = 0;
    for (finalists) |idx| mass += stage1.probabilities[idx];
    const merged = try a.dupe(f32, stage1.probabilities);
    errdefer a.free(merged);
    for (finalists, 0..) |idx, j| merged[idx] = mass * stage2.probabilities[j];
    a.free(stage1.probabilities);
    return finalize(q, merged, stage1.act_probability);
}

fn lengthBucket(sequence: Sequence) usize {
    return (sequence.ids.len + 63) / 64;
}

/// Stable grouping is worthwhile only when it saves at least 20% of padding.
fn bucketOrder(a: std.mem.Allocator, sequences: []const Sequence) !?[]usize {
    if (sequences.len < 8) return null;
    const order = try a.alloc(usize, sequences.len);
    var keep = false;
    defer if (!keep) a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    const Less = struct {
        fn less(rows: []const Sequence, lhs: usize, rhs: usize) bool {
            const left = lengthBucket(rows[lhs]);
            const right = lengthBucket(rows[rhs]);
            return if (left == right) lhs < rhs else left < right;
        }
    };
    std.mem.sort(usize, order, sequences, Less.less);
    var baseline: usize = 0;
    var begin: usize = 0;
    while (begin < sequences.len) {
        const end = @min(begin + 128, sequences.len);
        baseline += (end - begin) * chunkShape(sequences[begin..end]).sequence;
        begin = end;
    }
    var grouped: usize = 0;
    begin = 0;
    while (begin < order.len) {
        var end = begin;
        var maximum: usize = 0;
        while (end < order.len and end - begin < 128 and lengthBucket(sequences[order[end]]) == lengthBucket(sequences[order[begin]])) : (end += 1) {
            maximum = @max(maximum, sequences[order[end]].ids.len);
        }
        grouped += maximum * (end - begin);
        begin = end;
    }
    if (grouped * 5 > baseline * 4) return null;
    keep = true;
    return order;
}

const ChunkShape = struct { sequence: usize = 0, options: usize = 0 };
const AdmittedChunk = struct { count: usize, permit: @import("../backends/session.zig").RunPermit };

fn admitChunk(session: Session, sequences: []const Sequence, retained: usize, max_workspace_bytes: ?usize) !AdmittedChunk {
    var count = if (session.backend() == .cuda or max_workspace_bytes != null) try selectChunk(session, sequences, retained, max_workspace_bytes) else sequences.len;
    while (true) {
        const request = try chunkPlan(session, sequences[0..count], retained);
        const permit = session.admit(request) catch |err| {
            // fitsRun checks permanent limits. HTTP preprocessing and other
            // live leases can leave less room at admission time. Both capacity
            // errors are safe to retry here, before any forward has started.
            if ((err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) or count == 1 or (session.backend() != .cuda and max_workspace_bytes == null)) return err;
            count = (count + 1) / 2;
            continue;
        };
        return .{ .count = count, .permit = permit };
    }
}

fn chunkShape(sequences: []const Sequence) ChunkShape {
    var shape: ChunkShape = .{};
    for (sequences) |sequence| {
        shape.sequence = @max(shape.sequence, sequence.ids.len);
        shape.options = @max(shape.options, sequence.markers.len);
    }
    return shape;
}

fn chunkPlan(session: Session, sequences: []const Sequence, retained: usize) !@import("../backends/session.zig").RunRequest {
    const shape = chunkShape(sequences);
    const batch: i64 = @intCast(sequences.len);
    const sequence: i64 = @intCast(shape.sequence);
    const options: i64 = @intCast(shape.options);
    var request = try session.planShapes(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ batch, sequence } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ batch, sequence } },
        .{ .name = "qtype", .dtype = .i64, .shape = &.{ batch, 1 } },
        .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ batch, options } },
    }, sequences.len);
    // Tensor constructors copy input buffers. Charge both copies, while the
    // preprocessing lease already owns prepared sequences and accumulated results.
    request.host_preprocess_bytes = try std.math.add(usize, retained, request.input_bytes);
    request.pre_admitted_host_bytes = retained;
    return request;
}

fn selectChunk(session: Session, sequences: []const Sequence, retained: usize, max_workspace_bytes: ?usize) !usize {
    var count: usize = 0;
    while (count < @min(128, sequences.len)) {
        const request = try chunkPlan(session, sequences[0 .. count + 1], retained);
        if (max_workspace_bytes) |limit| if (request.workspace_bytes > limit) break;
        if (!try session.fitsRun(request)) break;
        count += 1;
    }
    if (count == 0) return error.ResourceLimitExceeded;
    return count;
}

fn runUnpackedChunkSource(source: *@import("../backends/session.zig").PreparedSource, execution: *@import("../backends/session.zig").RunPermit, preprocessing: ?*const @import("../backends/session.zig").RunPermit, inputs: []const Tensor, scratch: std.mem.Allocator, control: ?Control) ![]Tensor {
    if (!try source.initOwningSynchronousHostTaskPermitBeforeExecutionInto(execution, preprocessing, inputs, scratch, control))
        return execution.runWithControl(inputs, scratch, control);
    try source.enter();
    return source.runHost(inputs);
}

fn executeChunk(a: std.mem.Allocator, scratch: std.mem.Allocator, execution: *@import("../backends/session.zig").RunPermit, preprocessing: ?*const @import("../backends/session.zig").RunPermit, cfg: model.Config, tasks: []const Task, sequences: []const Sequence, pad: i32, decisions: []Decision, control: ?Control) !void {
    // The synchronous Source is declared before ALL copied input buffers.
    // Output/input physical death precedes its original-floor retirement.
    var source = @import("../backends/session.zig").PreparedSource{};
    defer source.deinit();
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    const chunk = arena.allocator();
    const shape = chunkShape(sequences);
    const seq = shape.sequence;
    const count = shape.options;
    const ids = try chunk.alloc(i64, tasks.len * seq);
    @memset(ids, pad);
    const mask = try chunk.alloc(i64, ids.len);
    @memset(mask, 0);
    const kinds = try chunk.alloc(i64, tasks.len);
    const markers = try chunk.alloc(i64, tasks.len * count);
    @memset(markers, -1);
    for (sequences, tasks, 0..) |prepared, task, i| {
        @memcpy(ids[i * seq ..][0..prepared.ids.len], prepared.ids);
        @memset(mask[i * seq ..][0..prepared.ids.len], 1);
        @memcpy(markers[i * count ..][0..prepared.markers.len], prepared.markers);
        kinds[i] = @backingInt(task.question.kind);
    }
    var inputs: [4]Tensor = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |*input| input.deinit();
    inputs[0] = try Tensor.initInt64(chunk, "input_ids", &.{ @intCast(tasks.len), @intCast(seq) }, ids);
    initialized += 1;
    inputs[1] = try Tensor.initInt64(chunk, "attention_mask", &.{ @intCast(tasks.len), @intCast(seq) }, mask);
    initialized += 1;
    inputs[2] = try Tensor.initInt64(chunk, "qtype", &.{ @intCast(tasks.len), 1 }, kinds);
    initialized += 1;
    inputs[3] = try Tensor.initInt64(chunk, "marker_pos", &.{ @intCast(tasks.len), @intCast(count) }, markers);
    initialized += 1;
    if (try execution.runLayaDecisionsWithControl(&inputs, scratch, control)) |outputs| {
        defer {
            for (outputs) |*output| output.deinit();
            scratch.free(outputs);
        }
        if (outputs.len != 1 or outputs[0].dtype != .f32) return error.UnexpectedOutputShape;
        const result_values = outputs[0].asFloat32();
        const width = count + 6;
        if (result_values.len != tasks.len * width) return error.UnexpectedOutputShape;
        var decoded: usize = 0;
        errdefer for (decisions[0..decoded]) |decision| a.free(decision.probabilities);
        for (tasks, decisions, 0..) |task, *decision, i| {
            const row = result_values[i * width ..][0..width];
            const q = task.question;
            if (row[count + 5] != 0 or !std.math.isFinite(row[count]) or row[count] < 0 or row[count] >= @as(f32, @floatFromInt(q.labels.len))) return error.InvalidLayaOutput;
            const winner: usize = @intFromFloat(row[count]);
            decision.* = .{
                .name = q.name,
                .kind = q.kind,
                .label = q.labels[winner],
                .labels = q.labels,
                .probabilities = try a.dupe(f32, row[0..q.labels.len]),
                .confidence = row[count + 1],
                .expected_value = if (q.kind == .score) row[count + 2] else null,
                .true_probability = if (q.kind == .noul) row[count + 3] else null,
                .act_probability = row[count + 4],
            };
            decoded += 1;
        }
        return;
    }
    defer source.finishInputLifetime(); // unlock on error BEFORE input frees
    const outputs = try runUnpackedChunkSource(&source, execution, preprocessing, &inputs, scratch, control);
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != @as(usize, if (cfg.n_act == 0) 1 else 2)) return error.UnexpectedOutputShape;
    for (outputs) |output| if (output.dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    const acts: []const f32 = if (cfg.n_act == 0) &.{} else outputs[1].asFloat32();
    if (logits.len != tasks.len * count or acts.len != tasks.len * cfg.n_act) return error.UnexpectedOutputShape;
    var decoded: usize = 0;
    errdefer for (decisions[0..decoded]) |decision| a.free(decision.probabilities);
    for (tasks, decisions, 0..) |task, *decision, i| {
        decision.* = try decode(a, cfg, task.question, logits[i * count ..][0..task.question.labels.len], acts[i * cfg.n_act ..][0..cfg.n_act]);
        decoded += 1;
    }
}

test "laya decision decoding preserves ordinal expectation and boolean probability" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = Question{ .name = "urgency", .kind = .score, .instruction = "urgency?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } };
    const d = try decode(arena.allocator(), .{}, q, &.{ 0, 0, 0 }, &.{ 0, 0 });
    try std.testing.expectApproxEqAbs(@as(f32, 1), d.expected_value.?, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), d.confidence, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), d.act_probability.?, 1e-6);
    const b = try decode(arena.allocator(), .{}, .{ .name = "needed", .kind = .noul, .instruction = "needed?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } }, &.{ 0, @log(@as(f32, 3)) }, &.{ 0, 0 });
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), b.true_probability.?, 1e-6);
    try std.testing.expectEqualStrings("true", b.label);
}

test "laya CUDA chunk planning uses padded shape retained memory and 128 task ceiling" {
    const sessions = @import("../backends/session.zig");
    const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
    const memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .cuda;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn geometry(_: *anyopaque, inputs: sessions.ShapeInputs, batch: usize) !?sessions.RunGeometry {
            const seq: usize = @intCast(inputs.get(0).shape[1]);
            return .{ .sequence = seq, .output_bytes = batch * 16, .workspace_bytes = batch * seq * 4096 };
        }
    };
    var marker: u8 = 0;
    var controller: memory.AdmissionController = .{};
    var session = Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = Probe.info, .backend = Probe.backend, .close = undefined, .runGeometry = Probe.geometry } };
    var ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var markers = [_]i64{ 0, 1 };
    var sequences = @as([512]Sequence, @splat(.{ .ids = ids[0..2], .markers = &markers }));
    try std.testing.expectEqual(@as(usize, 128), try selectChunk(session, &sequences, 1024, null));
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .backend_limit_bytes = 2 * 2 * 4096 }, .static_workspace_bytes = 0, .check_live_memory = false };
    sequences[2].ids = &ids;
    try std.testing.expectEqual(@as(usize, 2), try selectChunk(session, &sequences, 1024, null));
    const plan = try chunkPlan(session, sequences[0..3], 1024);
    try std.testing.expectEqual(@as(usize, 8), plan.sequence);
    try std.testing.expectEqual(@as(usize, 1024), plan.pre_admitted_host_bytes);
    session.run_admission.?.limits.backend_limit_bytes = 4096;
    try std.testing.expectError(error.ResourceLimitExceeded, selectChunk(session, &sequences, 1024, null));
    session.run_admission.?.limits.backend_limit_bytes = 1024 * 1024;
    session.run_admission.?.limits.host_limit_bytes = 1023;
    try std.testing.expectError(error.ResourceLimitExceeded, selectChunk(session, &sequences, 1024, null));

    // Permanent limits fit four rows, but an outer HTTP scratch lease leaves
    // room for only two. Admission must shrink before executing any model work.
    session.run_admission.?.limits.host_limit_bytes = 1024 * 1024;
    session.run_admission.?.limits.scratch_limit_bytes = 40_000;
    sequences[2].ids = ids[0..2];
    try std.testing.expectEqual(@as(usize, 4), try selectChunk(session, sequences[0..4], 0, null));
    {
        var outer = try session.admitHostPreprocess(18_000);
        defer outer.deinit();
        var admitted = try admitChunk(session, sequences[0..4], 0, null);
        defer admitted.permit.deinit();
        try std.testing.expectEqual(@as(usize, 2), admitted.count);
    }
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    {
        var outer = try session.admitHostPreprocess(39_500);
        defer outer.deinit();
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitChunk(session, sequences[0..4], 0, null));
    }
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "laya native chunk planning respects a bounded request workspace" {
    const sessions = @import("../backends/session.zig");
    const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
    const Probe = struct {
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn geometry(_: *anyopaque, inputs: sessions.ShapeInputs, batch: usize) !?sessions.RunGeometry {
            const sequence: usize = @intCast(inputs.get(0).shape[1]);
            return .{ .sequence = sequence, .output_bytes = batch * 16, .workspace_bytes = batch * sequence * 4096 };
        }
    };
    var marker: u8 = 0;
    const session = Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = Probe.info, .backend = Probe.backend, .close = undefined, .runGeometry = Probe.geometry } };
    var ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var markers = [_]i64{ 0, 1 };
    var sequences = @as([192]Sequence, @splat(.{ .ids = ids[0..2], .markers = &markers }));
    {
        var admitted = try admitChunk(session, &sequences, 1024, 32 * 1024);
        defer admitted.permit.deinit();
        try std.testing.expectEqual(@as(usize, 4), admitted.count);
    }
    // Padding to a longer question changes the admitted batch before execution.
    sequences[2].ids = &ids;
    try std.testing.expectEqual(@as(usize, 2), try selectChunk(session, &sequences, 1024, 32 * 1024));
    try std.testing.expectError(error.ResourceLimitExceeded, admitChunk(session, &sequences, 1024, 4096));
}

test "laya decoding rejects nonfinite logits without leaking probabilities" {
    const q = Question{ .name = "choice", .kind = .choice, .instruction = "choose", .labels = &.{ "a", "b" }, .descriptions = &.{ "", "" } };
    try std.testing.expectError(error.InvalidLayaOutput, decode(std.testing.allocator, .{}, q, &.{ 0, std.math.nan(f32) }, &.{ 0, 0 }));
    try std.testing.expectError(error.InvalidLayaOutput, decode(std.testing.allocator, .{}, q, &.{ 0, 0 }, &.{ 0, std.math.inf(f32) }));
}

test "laya length buckets are stable and require meaningful padding savings" {
    const a = std.testing.allocator;
    var ids = @as([149]i64, @splat(0));
    var markers = [_]i64{ 0, 1 };
    var sequences: [8]Sequence = undefined;
    for (&sequences, [_]usize{ 61, 100, 55, 79, 149, 80, 81, 143 }) |*sequence, len|
        sequence.* = .{ .ids = ids[0..len], .markers = &markers };
    const order = (try bucketOrder(a, &sequences)).?;
    defer a.free(order);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 1, 3, 5, 6, 4, 7 }, order);
    try std.testing.expect(try bucketOrder(a, sequences[0..7]) == null);
    for (&sequences) |*sequence| sequence.ids = ids[0..61];
    try std.testing.expect(try bucketOrder(a, &sequences) == null);
}

fn freeLayaBankTestResult(a: std.mem.Allocator, result: Result) void {
    for (result.decisions) |decision| a.free(decision.probabilities);
    a.free(result.decisions);
}
fn expectLayaBankTestResult(expected: Result, actual: Result) !void {
    try std.testing.expectEqual(expected.prompt_tokens, actual.prompt_tokens);
    try std.testing.expectEqual(expected.execution_chunks, actual.execution_chunks);
    try std.testing.expectEqual(expected.padded_tokens, actual.padded_tokens);
    try std.testing.expectEqual(expected.decisions.len, actual.decisions.len);
    for (expected.decisions, actual.decisions) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try std.testing.expectEqualStrings(want.label, got.label);
        try std.testing.expectEqual(want.kind, got.kind);
        try std.testing.expectEqualSlices(f32, want.probabilities, got.probabilities);
        try std.testing.expectEqual(want.confidence, got.confidence);
        try std.testing.expectEqual(want.expected_value, got.expected_value);
        try std.testing.expectEqual(want.true_probability, got.true_probability);
        try std.testing.expectEqual(want.act_probability, got.act_probability);
    }
}
fn layaOriginalPermitBankPipelineCase(a: std.mem.Allocator, path: []const u8, tok: Tokenizer, cfg: model.Config, tasks: []const Task, reference: Result, bank_bytes: usize, extra: usize, accepted: bool) !void {
    const Memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const factory = @import("../architectures/session_factory.zig");
    var session = try factory.createNativeSession(a, path);
    var live = true;
    defer if (live) session.close();
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    const domain = try Domain.create();
    defer domain.release();
    var pin = domain.pin();
    defer pin.release();
    session.model_lifetime.?.admission_domain = pin.retain();
    const retained = tasks.len * cfg.max_len * 64;
    const sequences = try a.alloc(Sequence, tasks.len);
    defer a.free(sequences);
    var initialized: usize = 0;
    defer for (sequences[0..initialized]) |sequence| {
        a.free(sequence.ids);
        a.free(sequence.markers);
    };
    for (tasks, sequences) |task, *sequence| {
        sequence.* = try prepare(a, tok, cfg, task);
        initialized += 1;
    }
    const request = try chunkPlan(session, sequences, retained);
    var policy = @import("../backends/session.zig").RunAdmission{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const floor = (try policy.planRequest(request, session.outputInfo())).required;
    const old_peak = try floor.merge(.{ .host_scratch_bytes = retained });
    policy.limits.host_limit_bytes = try std.math.add(usize, old_peak.host_scratch_bytes, extra);
    session.run_admission = policy;
    const Probe = struct {
        gate: *std.atomic.Mutex,
        bank_bytes: usize,
        calls: usize = 0,
        releases: usize = 0,
        records: [4]Memory.AdmissionAmounts = undefined,
        violation: bool = false,
        cancel_on_bank: bool = false,
        cancel: bool = false,
        fn observe(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
        }
        fn reserve(raw: *anyopaque, amounts: Memory.AdmissionAmounts) Memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (self.calls >= self.records.len) return error.ResourceLimitExceeded;
            self.records[self.calls] = amounts;
            self.calls += 1;
            if (self.cancel_on_bank and self.calls == 3 and std.meta.eql(amounts, Memory.AdmissionAmounts{ .host_scratch_bytes = self.bank_bytes })) self.cancel = true;
            return self.calls;
        }
        fn retain(raw: *anyopaque, _: usize, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.releases += 1;
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancel) return error.Cancelled;
        }
    };
    var probe = Probe{ .gate = session.execution_gate.?, .bank_bytes = bank_bytes };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const control = Control{ .native_call_policy = .in_process, .ptr = &probe, .check_fn = Probe.check };
    for (0..2) |_| {
        probe.calls = 0;
        probe.releases = 0;
        const actual = try executeWithScratchLimit(a, a, session, tok, cfg, tasks, control, null, null);
        defer freeLayaBankTestResult(a, actual);
        try expectLayaBankTestResult(reference, actual);
        try std.testing.expectEqual(@as(usize, if (accepted) 3 else 2), probe.calls);
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{ .host_scratch_bytes = retained }, probe.records[0]);
        try std.testing.expectEqualDeep(floor, probe.records[1]);
        if (accepted) try std.testing.expectEqualDeep(Memory.AdmissionAmounts{ .host_scratch_bytes = bank_bytes }, probe.records[2]);
        try std.testing.expectEqual(probe.calls, probe.releases);
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
        try std.testing.expect(!probe.violation and scheduler.head == null);
    }
    if (accepted) {
        probe.calls = 0;
        probe.releases = 0;
        probe.cancel_on_bank = true;
        const attempted = executeWithScratchLimit(a, a, session, tok, cfg, tasks, control, null, null);
        if (attempted) |unexpected| {
            freeLayaBankTestResult(a, unexpected);
            try std.testing.expect(false);
        } else |err| try std.testing.expectEqual(error.Cancelled, err);
        try std.testing.expectEqual(@as(usize, 3), probe.calls);
        try std.testing.expectEqual(probe.calls, probe.releases);
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
        try std.testing.expect(!probe.violation and scheduler.head == null);
        probe.cancel = false;
        probe.cancel_on_bank = false;
        probe.calls = 0;
        probe.releases = 0;
        const recovered = try executeWithScratchLimit(a, a, session, tok, cfg, tasks, control, null, null);
        defer freeLayaBankTestResult(a, recovered);
        try expectLayaBankTestResult(reference, recovered);
        try std.testing.expectEqual(@as(usize, 3), probe.calls);
        try std.testing.expectEqual(probe.calls, probe.releases);
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
    }
    session.close();
    live = false;
    try std.testing.expect(!probe.violation);
}

test "actual Laya Q8 task pipeline adopts original chunk once and supplements only exact MAX" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const synthetic = @import("../util/laya_synthetic.zig");
    const factory = @import("../architectures/session_factory.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    try synthetic.writeModelWith(a, std.testing.io, path, null, ",\"weight_quantization\":\"q8_0\"", 128, 717);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const question = Question{ .name = "first", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch" }, .descriptions = &.{ "look it up", "fetch it" } };
    var other = question;
    other.name = "second";
    const tasks = [_]Task{
        .{ .text = "find the invoice and check whether payment is late", .question = question },
        .{ .text = "find the invoice and check whether payment is late", .question = other },
    };
    var raw = try factory.createNativeSession(a, path);
    var raw_live = true;
    defer if (raw_live) raw.close();
    const cfg = factory.getLayaConfig(raw).?;
    const bytes = try @import("../util/c_file.zig").readFileFromDir(a, path, "config.json");
    defer a.free(bytes);
    const encoder = try @import("../architectures/modern_bert_config.zig").parseConfig(a, bytes);
    const bank = (try model.unpackedMatrixScratchGeometry(cfg, encoder, 2, 1)).?;
    const reference = try executeWithScratchLimit(a, a, raw, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
    defer freeLayaBankTestResult(a, reference);
    raw.close();
    raw_live = false;
    try layaOriginalPermitBankPipelineCase(a, path, tok, cfg, &tasks, reference, bank.bytes, bank.bytes, true);
    try layaOriginalPermitBankPipelineCase(a, path, tok, cfg, &tasks, reference, bank.bytes, bank.bytes - 1, false);
    try layaOriginalPermitBankPipelineCase(a, path, tok, cfg, &tasks, reference, bank.bytes, 0, false);
}

fn layaOutputOwnerLifetimeCase(comptime packed_mode: bool, comptime outcome: enum { success, invalid_output, decode_oom, cancel_after_decode }, path: []const u8, tok: Tokenizer, tasks: []const Task, reference: Result) !void {
    const a = std.testing.allocator;
    const Memory = @import("../runtime/tier/memory.zig");
    const factory = @import("../architectures/session_factory.zig");
    var session = try factory.createNativeSession(a, path);
    defer session.close();
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var pin = domain.pin();
    defer pin.release();
    session.model_lifetime.?.admission_domain = pin.retain();
    session.run_admission = .{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const cfg = factory.getLayaConfig(session).?;
    try std.testing.expectEqual(packed_mode, cfg.packing.enabled());
    if (outcome == .invalid_output) {
        // Real complete checkpoint + actual weight replacement, before run.
        // Nonfinite scorer output reaches the original decode error branch.
        const architecture = @import("../execution/architecture_session.zig");
        const data = architecture.nativeWeightPreparationModelData(session) orelse return error.TestExpectedCanonicalNative;
        const name = "model.scorer.3.bias";
        const original = data.resident_weights.getPtr(name) orelse return error.MissingWeight;
        var replacement = try Tensor.initFloat32(a, name, original.tensor.shape, &.{std.math.nan(f32)});
        architecture.replaceBlasResidentWeight(session, name, .{ .tensor = replacement }) catch |err| {
            replacement.deinit();
            return err;
        };
    }
    const Probe = struct {
        gate: *std.atomic.Mutex,
        probability_bytes: usize,
        calls: usize = 0,
        releases: usize = 0,
        primary_live: bool = false,
        after_primary: bool = false,
        observe_output_death: bool = false,
        decode_attempts: usize = 0,
        output_frees_before_primary_close: usize = 0,
        cancel: bool = false,
        violation: bool = false,
        fn outside(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
        }
        fn reserve(raw: *anyopaque, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.outside();
            self.calls += 1;
            // The task's first token is preprocessing, second is its SAME
            // original numerical/output floor; optional MAX is token three.
            if (self.calls == 2) {
                self.primary_live = true;
                self.after_primary = true;
            }
            return self.calls;
        }
        fn retain(raw: *anyopaque, _: usize, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.outside();
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.outside();
            if (token == 2) {
                if (self.decode_attempts != 0 and self.output_frees_before_primary_close == 0) self.violation = true;
                self.primary_live = false;
                self.observe_output_death = false;
            }
            self.releases += 1;
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancel) return error.Cancelled;
        }
        fn allocResult(raw: *anyopaque, bytes: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // All task planning/Decision[] allocations precede primary admit.
            // Numerical tensors use the DISTINCT scratch allocator below.
            // The result allocator's label-count F32 allocation after primary
            // is the actual original decode() probability allocation.
            if (self.after_primary and bytes == self.probability_bytes) {
                self.decode_attempts += 1;
                self.observe_output_death = true;
                if (!self.primary_live) self.violation = true;
                if (outcome == .decode_oom) return null;
                if (outcome == .cancel_after_decode) self.cancel = true;
            }
            return a.rawAlloc(bytes, alignment, ra);
        }
        fn allocScratch(_: *anyopaque, bytes: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return a.rawAlloc(bytes, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn freeResult(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            a.rawFree(bytes, alignment, ra);
        }
        fn freeScratch(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.observe_output_death) {
                if (!self.primary_live) self.violation = true;
                self.output_frees_before_primary_close += 1;
            }
            a.rawFree(bytes, alignment, ra);
        }
        fn resultAllocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocResult, .resize = resize, .remap = remap, .free = freeResult } };
        }
        fn scratchAllocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocScratch, .resize = resize, .remap = remap, .free = freeScratch } };
        }
    };
    var probe = Probe{ .gate = session.execution_gate.?, .probability_bytes = tasks[0].question.labels.len * @sizeOf(f32) };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const result_allocator = probe.resultAllocator();
    const attempted = executeWithScratchLimit(result_allocator, probe.scratchAllocator(), session, tok, cfg, tasks, .{ .native_call_policy = .in_process, .ptr = &probe, .check_fn = Probe.check }, null, null);
    if (attempted) |actual| {
        defer freeLayaBankTestResult(result_allocator, actual);
        try std.testing.expect(outcome == .success);
        try expectLayaBankTestResult(reference, actual);
    } else |err| {
        const expected: anyerror = switch (outcome) {
            .success => return err,
            .invalid_output => error.InvalidLayaOutput,
            .decode_oom => error.OutOfMemory,
            .cancel_after_decode => error.Cancelled,
        };
        try std.testing.expectEqual(expected, err);
    }
    try std.testing.expect(probe.decode_attempts > 0 and probe.output_frees_before_primary_close > 0);
    try std.testing.expect(!probe.primary_live and !probe.violation and scheduler.head == null);
    try std.testing.expectEqual(@as(usize, if (packed_mode) 2 else 3), probe.calls);
    try std.testing.expectEqual(probe.calls, probe.releases);
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "actual Laya raw output tensors keep original caller owners through decode error cancellation and OOM" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const synthetic = @import("../util/laya_synthetic.zig");
    const factory = @import("../architectures/session_factory.zig");
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const question = Question{ .name = "first", .kind = .choice, .instruction = "choose a tool", .labels = &.{ "search", "fetch" }, .descriptions = &.{ "look it up", "fetch it" } };
    const tasks = [_]Task{.{ .text = "find the invoice and check payment", .question = question }};
    inline for (.{ false, true }) |packed_mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(path);
        try synthetic.writeModelWith(a, std.testing.io, path, if (packed_mode) "{\"mode\":\"question\",\"max_packed_len\":512}" else null, ",\"weight_quantization\":\"q8_0\"", 128, 151);
        var raw = try factory.createNativeSession(a, path);
        defer raw.close();
        const cfg = factory.getLayaConfig(raw).?;
        const reference = try executeWithScratchLimit(a, a, raw, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
        defer freeLayaBankTestResult(a, reference);
        inline for (.{ .success, .invalid_output, .decode_oom, .cancel_after_decode }) |outcome|
            try layaOutputOwnerLifetimeCase(packed_mode, outcome, path, tok, &tasks, reference);
    }
}

fn packedLayaTaskBankCase(refine: bool, deny_first: bool) !void {
    const phases: usize = if (refine) 2 else 1;
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const synthetic = @import("../util/laya_synthetic.zig");
    const factory = @import("../architectures/session_factory.zig");
    const architecture = @import("../execution/architecture_session.zig");
    const Memory = @import("../runtime/tier/memory.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    try synthetic.writeModelWith(a, std.testing.io, path, if (refine) "{\"mode\":\"candidate\",\"max_packed_len\":512,\"two_stage\":{\"top_k\":2}}" else "{\"mode\":\"question\",\"max_packed_len\":512}", ",\"weight_quantization\":\"q8_0\"", 128, 152);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const question = Question{ .name = "first", .kind = .choice, .instruction = "choose a tool", .labels = if (refine) &.{ "search", "fetch", "open", "close" } else &.{ "search", "fetch" }, .descriptions = if (refine) &.{ "look it up", "fetch it", "open it", "close it" } else &.{ "look it up", "fetch it" } };
    const tasks = [_]Task{.{ .text = "find the invoice and check whether payment is late", .question = question }};
    var raw = try factory.createNativeSession(a, path);
    var raw_live = true;
    defer if (raw_live) raw.close();
    architecture.setLayaTrunkCachePrecision(raw, .f32);
    architecture.setLayaTrunkCacheLimit(raw, 1 << 20, 1);
    const cfg = factory.getLayaConfig(raw).?;
    const reference = try executeWithScratchLimit(a, a, raw, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
    defer freeLayaBankTestResult(a, reference);
    raw.close();
    raw_live = false;
    const config_bytes = try @import("../util/c_file.zig").readFileFromDir(a, path, "config.json");
    defer a.free(config_bytes);
    const encoder = try @import("../architectures/modern_bert_config.zig").parseConfig(a, config_bytes);
    const geometry = (try model.selectedMatrixScratchGeometry(cfg, encoder, .{ .encoder = true, .head = true })).?;
    const rows = try tree.build(a, tok, cfg, tasks[0].text, &.{question}, null);
    defer {
        for (rows) |selected| selected.deinit(a);
        a.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    const selected_trunk = @import("../architectures/laya_packed.zig").cacheKeyForRow(encoder, cfg, rows[0]).?;
    const auxiliary = try @import("../architectures/laya_packed.zig").auxiliaryGeometry(encoder.num_hidden_layers + cfg.head_layers, encoder.hidden_size, selected_trunk.tokens);
    const warm_supplement = try std.math.add(usize, geometry.bytes, auxiliary.bytes);
    const CacheModule = @import("../architectures/laya_trunk_cache.zig");
    var empty_cache = CacheModule.Cache.init(a, 1 << 20);
    defer empty_cache.deinit();
    var empty_snapshot = try empty_cache.snapshotCold(selected_trunk.key);
    defer empty_snapshot.finishOutsideExecution(&empty_cache, .{ .outside_execution = true }) catch @panic("cold geometry snapshot unwind");
    const cold_entries = try CacheModule.coldListGeometry(&empty_snapshot, .entries, 1);
    const cold_pending = try CacheModule.coldListGeometry(&empty_snapshot, .pending, 1);
    const cold_entry = try CacheModule.coldFillGeometry(selected_trunk.tokens, encoder.num_hidden_layers + cfg.head_layers, encoder.hidden_size, .f32);
    const cold_supplement = try std.math.add(usize, warm_supplement, try std.math.add(usize, cold_entries.bytes + cold_pending.bytes, cold_entry.metadata_bytes));
    var session = try factory.createNativeSession(a, path);
    var session_live = true;
    defer if (session_live) session.close();
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var pin = domain.pin();
    defer pin.release();
    session.model_lifetime.?.admission_domain = pin.retain();
    session.run_admission = .{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 << 20 }, .static_workspace_bytes = 0, .check_live_memory = false };
    try architecture.configureSharedCacheAdmissionForSession(session, a, pin.controller(), .cpu, session.run_admission.?.limits, .{});
    architecture.setLayaTrunkCachePrecision(session, .f32);
    architecture.setLayaTrunkCacheLimit(session, 1 << 20, 1);
    const Probe = struct {
        gate: *std.atomic.Mutex,
        bank_bytes: usize,
        supplement_bytes: usize,
        cold_supplement_bytes: usize,
        deny_cold_once: bool = false,
        calls: usize = 0,
        releases: usize = 0,
        kv_reservations: usize = 0,
        matrix_allocations: usize = 0,
        matrix_deaths: usize = 0,
        matrix_failures: usize = 0,
        cancel_on_bank: bool = false,
        fail_bank: bool = false,
        deny_bank: bool = false,
        denials: usize = 0,
        check_matrix_outside: bool = true,
        awaiting_bank: bool = false,
        cancelled: bool = false,
        failed_bank: bool = false,
        violation: bool = false,
        fn outside(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
        }
        fn reserve(raw_ctx: *anyopaque, amounts: Memory.AdmissionAmounts) Memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
            self.calls += 1;
            if (self.deny_cold_once and amounts.host_scratch_bytes == self.cold_supplement_bytes and amounts.host_kv_bytes == 0) {
                self.deny_cold_once = false;
                self.denials += 1;
                return error.ResourceLimitExceeded;
            }
            if (amounts.host_kv_bytes != 0) self.kv_reservations += 1;
            if (amounts.host_scratch_bytes == self.supplement_bytes and amounts.host_kv_bytes == 0) {
                // Every previous numerical bank must already be physically
                // dead before another refinement's supplier is reached.
                if (self.matrix_allocations - self.matrix_failures != self.matrix_deaths) self.violation = true;
                if (self.deny_bank) {
                    self.denials += 1;
                    return error.ResourceLimitExceeded;
                }
                self.awaiting_bank = true;
                if (self.cancel_on_bank) self.cancelled = true;
            }
            return self.calls;
        }
        fn retain(raw_ctx: *anyopaque, _: usize, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
        }
        fn release(raw_ctx: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
            self.releases += 1;
        }
        fn check(raw_ctx: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn alloc(raw_ctx: *anyopaque, bytes: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (bytes == self.bank_bytes) {
                if (self.check_matrix_outside) self.outside(); // accepted MAX is never allocated in math
                self.matrix_allocations += 1;
                if (self.fail_bank and self.awaiting_bank) {
                    self.failed_bank = true;
                    self.matrix_failures += 1;
                    self.awaiting_bank = false;
                    return null;
                }
                self.awaiting_bank = false;
            }
            return a.rawAlloc(bytes, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw_ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            if (bytes.len == self.bank_bytes) {
                if (self.check_matrix_outside) self.outside();
                self.matrix_deaths += 1;
            }
            a.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
    };
    var probe = Probe{ .gate = session.execution_gate.?, .bank_bytes = geometry.bytes, .supplement_bytes = warm_supplement, .cold_supplement_bytes = cold_supplement, .deny_cold_once = deny_first, .check_matrix_outside = !deny_first };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const control = Control{ .native_call_policy = .in_process, .ptr = &probe, .check_fn = Probe.check };
    const first = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    defer freeLayaBankTestResult(a, first);
    try expectLayaBankTestResult(reference, first);
    const cold_stats = architecture.layaTrunkCacheStats(session).?;
    try std.testing.expectEqual(@as(usize, 1), cold_stats.entries);
    try std.testing.expectEqual(@as(u64, 1), cold_stats.misses);
    try std.testing.expectEqual(@as(usize, 1), probe.kv_reservations);
    if (deny_first) {
        try std.testing.expect(!probe.deny_cold_once and probe.denials == 1);
        try std.testing.expect(probe.matrix_allocations != 0); // original numerical scratch route remains selected
    } else try std.testing.expectEqual(phases, probe.matrix_allocations);
    try std.testing.expectEqual(probe.matrix_allocations, probe.matrix_deaths);
    const initial_matrix_allocations = probe.matrix_allocations;
    probe.check_matrix_outside = true;
    const retained = pin.controller().snapshot();
    try std.testing.expect(retained.host_kv_bytes > 0 and retained.host_scratch_bytes > 0);
    const calls_before_warm = probe.calls;
    const warm = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    defer freeLayaBankTestResult(a, warm);
    try expectLayaBankTestResult(reference, warm);
    try std.testing.expectEqual(1 + 2 * phases, probe.calls - calls_before_warm); // preprocessing plus SAME original primary and exact bank per original numerical phase
    try std.testing.expectEqual(@as(usize, 1), probe.kv_reservations);
    try std.testing.expectEqual(initial_matrix_allocations + phases, probe.matrix_allocations);
    try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
    try std.testing.expectEqual(cold_stats.hits + phases, architecture.layaTrunkCacheStats(session).?.hits);
    probe.cancel_on_bank = true;
    const cancelled = executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    if (cancelled) |unexpected| {
        freeLayaBankTestResult(a, unexpected);
        try std.testing.expect(false);
    } else |err| try std.testing.expectEqual(error.Cancelled, err);
    try std.testing.expect(probe.cancelled and scheduler.head == null);
    try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
    probe.cancel_on_bank = false;
    probe.cancelled = false;
    probe.fail_bank = true;
    const failed = executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    if (failed) |unexpected| {
        freeLayaBankTestResult(a, unexpected);
        try std.testing.expect(false);
    } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
    try std.testing.expect(probe.failed_bank and scheduler.head == null);
    try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
    probe.fail_bank = false;
    const recovered = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    defer freeLayaBankTestResult(a, recovered);
    try expectLayaBankTestResult(reference, recovered);
    try std.testing.expectEqual(@as(usize, 1), probe.kv_reservations);
    try std.testing.expect(!probe.violation and scheduler.head == null);
    if (!refine) {
        try packedLayaRotationCapacityCase(session, cfg, rows[0], a, false);
        try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
        const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
        const row = rows[0];
        const n: i64 = @intCast(row.ids.len);
        const q: i64 = @intCast(row.questions());
        const signature = [_]TensorInfo{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, n } },
            .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, n } },
            .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, n } },
            .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, @intCast(row.parents.len) } },
            .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, n } },
            .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ q, @intCast(row.width) } },
            .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ q, 1 } },
        };
        // Reconstruct the SAME original request, without subtracting any
        // estimator component. Cache/primary/preprocess/bank really overlap.
        var request = try session.planShapes(&signature, 1);
        request.host_preprocess_bytes = request.input_bytes;
        const policy = session.run_admission.?;
        const primary = (try policy.planRequest(request, session.outputInfo())).required;
        const preprocessing = Memory.AdmissionAmounts{ .host_scratch_bytes = tasks.len * cfg.max_len * 64 };
        const original_live = try (try retained.merge(primary)).merge(preprocessing);
        const accepted_live = try original_live.merge(.{ .host_scratch_bytes = warm_supplement });
        const exact = accepted_live.hostTotalBytes();
        const previous_limit = session.run_admission.?.limits.host_limit_bytes;
        defer session.run_admission.?.limits.host_limit_bytes = previous_limit;
        session.run_admission.?.limits.host_limit_bytes = exact;
        const calls_exact = probe.calls;
        const allocs_exact = probe.matrix_allocations;
        const exact_result = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
        defer freeLayaBankTestResult(a, exact_result);
        try expectLayaBankTestResult(reference, exact_result);
        try std.testing.expectEqual(@as(usize, 3), probe.calls - calls_exact);
        try std.testing.expectEqual(allocs_exact + 1, probe.matrix_allocations);
        try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
        // At one byte below the exact bank overlap, and at the original
        // request's own full overlap, the optional bank declines pre-math.
        // The old supported route keeps its actual original floor and output.
        probe.check_matrix_outside = false;
        for ([_]usize{ exact - 1, original_live.hostTotalBytes() }) |limit| {
            session.run_admission.?.limits.host_limit_bytes = limit;
            const before_calls = probe.calls;
            const legacy = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
            defer freeLayaBankTestResult(a, legacy);
            try expectLayaBankTestResult(reference, legacy);
            try std.testing.expectEqual(@as(usize, 2), probe.calls - before_calls);
            try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
        }
        probe.check_matrix_outside = true;
        session.run_admission.?.limits.host_limit_bytes = previous_limit;
    }
    // A real optional supplementary denial leaves the original permit and
    // original scratch/cache/math route available. No prepared-bank claim is
    // made for those unchanged legacy numerical allocations.
    probe.deny_bank = true;
    probe.check_matrix_outside = false;
    const denied = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &tasks, control, null, null);
    defer freeLayaBankTestResult(a, denied);
    try expectLayaBankTestResult(reference, denied);
    try std.testing.expectEqual(phases + @intFromBool(deny_first), probe.denials);
    try std.testing.expectEqualDeep(retained, pin.controller().snapshot());
    try std.testing.expect(!probe.violation and scheduler.head == null);
    if (!refine) {
        probe.deny_bank = false;
        probe.check_matrix_outside = true;
        const combined = [_]Task{ tasks[0], .{ .text = "find the contract and check whether delivery is late", .question = question } };
        var raw_split = try factory.createNativeSession(a, path);
        defer raw_split.close();
        architecture.setLayaTrunkCachePrecision(raw_split, .f32);
        architecture.setLayaTrunkCacheLimit(raw_split, 1 << 20, 1);
        // Original cold coalesced routing has no cached trunk and therefore
        // uses the full row, with zero invented Entry births.
        const coalesced = try executeWithScratchLimit(a, a, raw_split, tok, cfg, &combined, control, null, null);
        defer freeLayaBankTestResult(a, coalesced);
        try std.testing.expectEqual(@as(usize, 0), architecture.layaTrunkCacheStats(raw_split).?.entries);
        const prewarm = try executeWithScratchLimit(a, a, raw_split, tok, cfg, &tasks, control, null, null);
        defer freeLayaBankTestResult(a, prewarm);
        const split_reference = try executeWithScratchLimit(a, a, raw_split, tok, cfg, &combined, control, null, null);
        defer freeLayaBankTestResult(a, split_reference);
        try std.testing.expectEqual(@as(usize, 2), architecture.layaTrunkCacheStats(raw_split).?.entries);
        const allocations_before_split = probe.matrix_allocations;
        const births_before_split = probe.kv_reservations;
        const split_result = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &combined, control, null, null);
        defer freeLayaBankTestResult(a, split_result);
        try expectLayaBankTestResult(split_reference, split_result);
        try std.testing.expectEqual(allocations_before_split + 1, probe.matrix_allocations); // SAME serial MAX for both trees
        try std.testing.expectEqual(births_before_split + 1, probe.kv_reservations);
        try std.testing.expectEqual(@as(usize, 2), architecture.layaTrunkCacheStats(session).?.entries);
        const split_warm = try executeWithScratchLimit(a, probe.allocator(), session, tok, cfg, &combined, control, null, null);
        defer freeLayaBankTestResult(a, split_warm);
        try expectLayaBankTestResult(split_reference, split_warm);
        try std.testing.expectEqual(allocations_before_split + 2, probe.matrix_allocations);
        try std.testing.expectEqual(births_before_split + 1, probe.kv_reservations);
        try std.testing.expect(!probe.violation and scheduler.head == null);
        const pending_rows = try tree.build(a, tok, cfg, "find the receipt and check whether refund is late", &.{question}, null);
        defer {
            for (pending_rows) |pending_row| pending_row.deinit(a);
            a.free(pending_rows);
        }
        try std.testing.expectEqual(@as(usize, 1), pending_rows.len);
        try packedLayaRotationCapacityCase(session, cfg, pending_rows[0], a, true);
        try std.testing.expect(!probe.violation and scheduler.head == null);
    }
    session.close();
    session_live = false;
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
    try std.testing.expectEqual(probe.calls - probe.denials, probe.releases);
}

test "actual packed Laya task uses SAME permit MAX and trunk Entry cold warm cancellation and OOM" {
    try packedLayaTaskBankCase(false, false);
}

test "actual candidate Laya refinement retires first numerical bank while original output floor remains live" {
    try packedLayaTaskBankCase(true, false);
}

fn packedLayaTaskScratchOomCase(allocator: std.mem.Allocator, path: []const u8, tok: Tokenizer, cfg: model.Config, tasks: []const Task, reference: Result) !void {
    const a = std.testing.allocator;
    const architecture = @import("../execution/architecture_session.zig");
    const Memory = @import("../runtime/tier/memory.zig");
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var session = try @import("../architectures/session_factory.zig").createNativeSession(a, path);
    // All request objects and registered B callbacks drain before this failing
    // allocator's context leaves scope, including every failed constructor.
    defer {
        session.close();
        std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot()) catch @panic("packed task all-OOM retained quota after physical close");
    }
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    session.model_lifetime.?.admission_domain = pin.retain();
    session.run_admission = .{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 << 20 }, .static_workspace_bytes = 0, .check_live_memory = false };
    try architecture.configureSharedCacheAdmissionForSession(session, a, pin.controller(), .cpu, session.run_admission.?.limits, .{});
    architecture.setLayaTrunkCachePrecision(session, .f32);
    architecture.setLayaTrunkCacheLimit(session, 1 << 20, 1);
    const result = try executeWithScratchLimit(allocator, allocator, session, tok, cfg, tasks, .{ .native_call_policy = .in_process }, null, null);
    defer freeLayaBankTestResult(allocator, result);
    try expectLayaBankTestResult(reference, result);
    try std.testing.expect(scheduler.head == null);
}

test "actual packed Laya Source input control matrix auxiliary output and decode unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const synthetic = @import("../util/laya_synthetic.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    try synthetic.writeModelWith(a, std.testing.io, path, "{\"mode\":\"question\",\"max_packed_len\":512}", ",\"weight_quantization\":\"q8_0\"", 128, 153);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const question = Question{ .name = "first", .kind = .choice, .instruction = "choose a tool", .labels = &.{ "search", "fetch" }, .descriptions = &.{ "look it up", "fetch it" } };
    const tasks = [_]Task{.{ .text = "find the invoice and check whether payment is late", .question = question }};
    var raw = try @import("../architectures/session_factory.zig").createNativeSession(a, path);
    defer raw.close();
    @import("../execution/architecture_session.zig").setLayaTrunkCachePrecision(raw, .f32);
    @import("../execution/architecture_session.zig").setLayaTrunkCacheLimit(raw, 1 << 20, 1);
    const cfg = @import("../architectures/session_factory.zig").getLayaConfig(raw).?;
    const reference = try executeWithScratchLimit(a, a, raw, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
    defer freeLayaBankTestResult(a, reference);
    try packedLayaTaskScratchOomCase(a, path, tok, cfg, &tasks, reference);
    try platform.allocator.checkAllAllocationFailures(a, packedLayaTaskScratchOomCase, .{ path, tok, cfg, @as([]const Task, &tasks), reference });
}

fn packedLayaRotationCapacityCase(session: Session, cfg: model.Config, row: tree.Row, allocator: std.mem.Allocator, pending_peer: bool) !void {
    const source_module = @import("../backends/session.zig");
    const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
    const sink = @import("../execution/retirement_sink.zig");
    const n: i64 = @intCast(row.ids.len);
    const q: i64 = @intCast(row.questions());
    const signature = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, @intCast(row.parents.len) } },
        .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ q, @intCast(row.width) } },
        .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ q, 1 } },
    };
    var preprocessing = try session.admitHostPreprocess(cfg.max_len * 64);
    defer preprocessing.deinit();
    var request = try session.planShapes(&signature, 1);
    request.host_preprocess_bytes = request.input_bytes;
    var execution = try session.admitShapeRequest(.{ .host_shapes = &signature }, request, allocator, .{ .native_call_policy = .in_process });
    defer execution.deinit();
    var source = source_module.PreparedSource{}; // final address BEFORE inputs/outputs
    defer source.deinit();
    var inputs: [7]Tensor = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |*input| input.deinit();
    defer source.finishInputLifetime();
    for (signature, [_][]const i64{ row.ids, row.positions, row.segments, row.parents, row.kinds, row.markers, row.anchors }, 0..) |info, values, index| {
        inputs[index] = try Tensor.initInt64(allocator, info.name, info.shape, values);
        initialized += 1;
    }
    const Cache = @import("../architectures/laya_trunk_cache.zig").Cache;
    const Entry = @import("../architectures/laya_trunk_cache.zig").Entry;
    const Pending = struct {
        cache: *Cache,
        entry: *Entry,
        gate: *std.atomic.Mutex,
        scheduler: *@import("../runtime/embedding_scheduler.zig").Scheduler,
        checks: usize = 0,
        published: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.published) return;
            if (!self.gate.tryLock()) return;
            defer self.gate.unlock();
            if (self.scheduler.head != null) return;
            self.checks += 1;
            if (self.checks <= 20) return; // prove the old eight-retry decline was unsafe
            // A real peer can enter Model and publish while cold Source waits:
            // this is the actual pending Entry, not a fake hit/selector.
            self.cache.publish(self.entry);
            self.cache.release(self.entry);
            self.published = true;
        }
    };
    var setup_sink = try sink.Ref.create();
    defer setup_sink.release();
    var setup_guard = try setup_sink.enter();
    defer setup_guard.finish();
    var setup_epoch = try setup_guard.retainEpoch();
    defer setup_epoch.release();
    var pending: ?Pending = null;
    defer if (pending) |*peer| if (!peer.published) peer.cache.abandon(peer.entry);
    if (pending_peer) {
        const adapter = @import("../execution/architecture_session.zig");
        var described = (try adapter.collectNativeMatrixScratchCallOutsideExecution(session, &inputs, allocator, .{ .boundary = &setup_epoch })).?;
        const cache = described.workload.?.cache;
        const encoder_cfg = described.workload.?.config;
        try described.finishOutsideExecution(.{ .boundary = &setup_epoch });
        const selected = @import("../architectures/laya_packed.zig").cacheKeyForRow(encoder_cfg, cfg, row).?;
        const entry = switch (try cache.lookup(selected.key, selected.tokens, encoder_cfg.num_hidden_layers + cfg.head_layers, encoder_cfg.hidden_size, true)) {
            .fill => |value| value,
            else => return error.TestUnexpectedResult,
        };
        @memset(entry.host16, 0);
        @memset(entry.host32, 0);
        pending = .{ .cache = cache, .entry = entry, .gate = session.execution_gate.?, .scheduler = session.cohort_scheduler.? };
    }
    const selected_control = Control{ .native_call_policy = .in_process, .ptr = if (pending) |*peer| peer else null, .check_fn = if (pending != null) Pending.check else null };
    try std.testing.expect(try source.initOwningSynchronousHostTaskPermitBeforeExecutionInto(&execution, &preprocessing, &inputs, allocator, selected_control));
    if (pending) |peer| try std.testing.expect(peer.published and peer.checks > 20);
    try source.enter();
    const outputs = try source.runHost(&inputs);
    defer {
        for (outputs) |*output| output.deinit();
        allocator.free(outputs);
    }
    if (pending_peer) {
        try std.testing.expect(try source.finishSynchronousHostTaskWorkspaceAfterUnlock());
        for (outputs) |output| for (output.asFloat32()) |value| try std.testing.expect(std.math.isFinite(value));
        return;
    }
    var original_a = try source.epoch().?.retain();
    defer original_a.release();
    const original_amounts = source.permit.?.lease.?.amounts;
    const before = session.run_admission.?.controller.snapshot();
    const actual_sink = source.cleanup.retained_sink.?;
    var guards: [64]sink.Guard = undefined;
    var count: usize = 0;
    defer for (guards[0..count]) |*guard| guard.finish();
    var exhausted = false;
    while (count < guards.len) {
        guards[count] = actual_sink.enter() catch |err| {
            try std.testing.expectEqual(error.RetirementEpochCapacityExceeded, err);
            exhausted = true;
            break;
        };
        count += 1;
    }
    try std.testing.expect(exhausted);
    try std.testing.expect(!try source.finishSynchronousHostTaskWorkspaceAfterUnlock());
    try std.testing.expect(!try original_a.isSealed());
    try std.testing.expect(source.epoch().?.record == original_a.record and source.epoch().?.nonce == original_a.nonce);
    try std.testing.expectEqualDeep(original_amounts, source.permit.?.lease.?.amounts);
    try std.testing.expectEqualDeep(before, session.run_admission.?.controller.snapshot());
    try std.testing.expect(source.native_matrix_scratch == null and source.native_matrix_call == null and source.native_matrix_supplement == null);
    // Outputs remain readable and owned; no replay/early quota credit occurs.
    try std.testing.expectEqual(@as(usize, 2), outputs.len);
    for (outputs) |output| for (output.asFloat32()) |value| try std.testing.expect(std.math.isFinite(value));
}

test "actual cold first packed Laya numeric MAX denial preserves same cache Entry with KV suppliers outside Model" {
    try packedLayaTaskBankCase(false, true);
}

test "actual full-row Laya no-cache optional bank denial uses empty capability with no zero supplier" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const synthetic = @import("../util/laya_synthetic.zig");
    const factory = @import("../architectures/session_factory.zig");
    const architecture = @import("../execution/architecture_session.zig");
    const Memory = @import("../runtime/tier/memory.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    try synthetic.writeModelWith(a, std.testing.io, path, "{\"mode\":\"question\",\"max_packed_len\":512,\"trunk_sees\":\"questions\"}", ",\"weight_quantization\":\"q8_0\"", 128, 154);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const question = Question{ .name = "first", .kind = .choice, .instruction = "choose a tool", .labels = &.{ "search", "fetch" }, .descriptions = &.{ "look it up", "fetch it" } };
    const tasks = [_]Task{.{ .text = "find the invoice and check whether payment is late", .question = question }};
    var raw = try factory.createNativeSession(a, path);
    defer raw.close();
    const cfg = factory.getLayaConfig(raw).?;
    const reference = try executeWithScratchLimit(a, a, raw, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
    defer freeLayaBankTestResult(a, reference);
    const config_bytes = try @import("../util/c_file.zig").readFileFromDir(a, path, "config.json");
    defer a.free(config_bytes);
    const encoder = try @import("../architectures/modern_bert_config.zig").parseConfig(a, config_bytes);
    const geometry = (try model.selectedMatrixScratchGeometry(cfg, encoder, .{ .encoder = true, .head = true })).?;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var session = try factory.createNativeSession(a, path);
    var live = true;
    defer if (live) session.close();
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    session.model_lifetime.?.admission_domain = pin.retain();
    session.run_admission = .{ .controller = pin.controller(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 << 20 }, .static_workspace_bytes = 0, .check_live_memory = false };
    try architecture.configureSharedCacheAdmissionForSession(session, a, pin.controller(), .cpu, session.run_admission.?.limits, .{});
    architecture.setLayaTrunkCachePrecision(session, .f32);
    architecture.setLayaTrunkCacheLimit(session, 1 << 20, 1);
    const Probe = struct {
        gate: *std.atomic.Mutex,
        bank_bytes: usize,
        reservations: usize = 0,
        denials: usize = 0,
        releases: usize = 0,
        zero_calls: usize = 0,
        kv_calls: usize = 0,
        violation: bool = false,
        fn outside(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
        }
        fn reserve(raw_ctx: *anyopaque, amounts: Memory.AdmissionAmounts) Memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
            if (std.meta.eql(amounts, Memory.AdmissionAmounts{})) self.zero_calls += 1;
            if (amounts.host_kv_bytes != 0) self.kv_calls += 1;
            if (std.meta.eql(amounts, Memory.AdmissionAmounts{ .host_scratch_bytes = self.bank_bytes })) {
                self.denials += 1;
                return error.ResourceLimitExceeded;
            }
            self.reservations += 1;
            return self.reservations;
        }
        fn retain(raw_ctx: *anyopaque, _: usize, _: Memory.AdmissionAmounts) Memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
        }
        fn release(raw_ctx: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            self.outside();
            self.releases += 1;
        }
    };
    var probe = Probe{ .gate = session.execution_gate.?, .bank_bytes = geometry.bytes };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    for (0..2) |iteration| {
        const result = try executeWithScratchLimit(a, a, session, tok, cfg, &tasks, .{ .native_call_policy = .in_process }, null, null);
        defer freeLayaBankTestResult(a, result);
        try expectLayaBankTestResult(reference, result);
        try std.testing.expectEqual(iteration + 1, probe.denials);
        try std.testing.expectEqual(@as(usize, 0), architecture.layaTrunkCacheStats(session).?.entries);
        try std.testing.expectEqual(@as(usize, 0), probe.zero_calls);
        try std.testing.expectEqual(@as(usize, 0), probe.kv_calls);
        try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
        try std.testing.expect(!probe.violation and scheduler.head == null);
    }
    session.close();
    live = false;
    try std.testing.expectEqual(probe.reservations, probe.releases);
    try std.testing.expectEqualDeep(Memory.AdmissionAmounts{}, pin.controller().snapshot());
}
