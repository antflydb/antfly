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
    act_probability: f32,
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

    pub fn deinit(self: QuestionTokens, a: std.mem.Allocator) void {
        for (self.options) |ids| a.free(ids);
        a.free(self.options);
        a.free(self.head);
    }

    /// Option run length under the shared budget, including its `[MASK]` marker.
    pub fn optionRun(self: QuestionTokens, i: usize) usize {
        return @min(1 + @min(self.options[i].len, max_option_tokens), self.per);
    }
};

pub fn questionTokens(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, q: Question) !QuestionTokens {
    if (q.labels.len < 2 or q.labels.len > cfg.maxOptions() or q.labels.len != q.descriptions.len) return error.InvalidLayaQuestion;
    const mask = cfg.mask_token[0..cfg.mask_token_len];
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
    const state = try encodeClean(a, tok, task.text, mask);
    defer a.free(state);
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
    for (options, 0..) |option, i| {
        markers[i] = @intCast(pos);
        ids[pos] = special.mask_id;
        pos += 1;
        for (option[0 .. tokens.optionRun(i) - 1]) |id| {
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
    var winner: usize = 0;
    var entropy: f32 = 0;
    var expected: f32 = 0;
    for (probabilities, 0..) |p, i| {
        if (p > probabilities[winner]) winner = i;
        entropy -= p * @log(@max(p, 1e-12));
        expected += @as(f32, @floatFromInt(i)) * p;
    }
    var acts: [33]f32 = undefined;
    try softmax(action, 1, acts[0..action.len]);
    return .{
        .name = q.name,
        .kind = q.kind,
        .label = q.labels[winner],
        .labels = q.labels,
        .probabilities = probabilities,
        .confidence = if (q.kind == .noul) @max(probabilities[1], 1 - probabilities[1]) else std.math.clamp(1 - entropy / @log(@as(f32, @floatFromInt(logits.len))), 0, 1),
        .expected_value = if (q.kind == .score) expected else null,
        .true_probability = if (q.kind == .noul) probabilities[1] else null,
        .act_probability = acts[0],
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
    const order = if (session.backend() == .cuda and platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_OPTIMIZATIONS", true) and platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_BUCKETING", true))
        try bucketOrder(a, sequences)
    else
        null;
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
        var admitted = try admitChunk(session, remaining, retained);
        defer admitted.permit.deinit();
        try executeChunk(a, scratch, &admitted.permit, cfg, execution_tasks[finished..][0..admitted.count], remaining[0..admitted.count], tok.specialTokens().pad_id, decisions[finished..][0..admitted.count], control);
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
    const Planned = struct { row: tree.Row, members: []const usize };
    var rows: std.ArrayListUnmanaged(Planned) = .empty;
    var tokens: usize = 0;
    for (groups.keys(), groups.values()) |text, members| {
        if (control) |active| try active.check();
        const questions = try plan.alloc(Question, members.items.len);
        for (questions, members.items) |*q, index| q.* = tasks[index].question;
        for (try tree.build(plan, tok, cfg, text, questions)) |row| {
            if (max_input_tokens) |limit| if (row.ids.len > limit) return error.InferenceInputTokensExceeded;
            tokens += row.ids.len;
            try rows.append(plan, .{ .row = row, .members = members.items });
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
    for (rows.items) |planned| {
        if (control) |active| try active.check();
        const row = planned.row;
        const questions = row.questions();
        const outputs = try runPackedRow(session, scratch, row, questions, control);
        defer {
            for (outputs) |*output| output.deinit();
            scratch.free(outputs);
        }
        if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32) return error.UnexpectedOutputShape;
        const logits = outputs[0].asFloat32();
        const acts = outputs[1].asFloat32();
        if (logits.len != questions * row.width or acts.len != questions * cfg.n_act) return error.UnexpectedOutputShape;
        for (row.question_index, 0..) |local, qi| {
            const index = planned.members[local];
            const q = tasks[index].question;
            decisions[index] = try decode(a, cfg, q, logits[qi * row.width ..][0..q.labels.len], acts[qi * cfg.n_act ..][0..cfg.n_act]);
            done[index] = true;
            decoded += 1;
        }
    }
    if (decoded != tasks.len) return error.UnexpectedOutputShape;
    if (control) |active| try active.check();
    return .{ .decisions = decisions, .prompt_tokens = tokens, .execution_chunks = rows.items.len };
}

fn runPackedRow(session: Session, scratch: std.mem.Allocator, row: tree.Row, questions: usize, control: ?Control) ![]Tensor {
    const n: i64 = @intCast(row.ids.len);
    const q: i64 = @intCast(questions);
    var request = try session.planShapes(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, @intCast(row.parents.len) } },
        .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, n } },
        .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ q, @intCast(row.width) } },
        .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ q, 1 } },
    }, 1);
    request.host_preprocess_bytes = request.input_bytes;
    var execution = try session.admit(request);
    defer execution.deinit();
    var inputs: [7]Tensor = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |*input| input.deinit();
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
    return execution.runWithControl(&inputs, scratch, control);
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

fn admitChunk(session: Session, sequences: []const Sequence, retained: usize) !AdmittedChunk {
    var count = if (session.backend() == .cuda) try selectChunk(session, sequences, retained) else sequences.len;
    while (true) {
        const request = try chunkPlan(session, sequences[0..count], retained);
        const permit = session.admit(request) catch |err| {
            // fitsRun checks permanent limits. HTTP preprocessing and other
            // live leases can leave less room at admission time. Both capacity
            // errors are safe to retry here, before any forward has started.
            if ((err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) or count == 1 or session.backend() != .cuda) return err;
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

fn selectChunk(session: Session, sequences: []const Sequence, retained: usize) !usize {
    var count: usize = 0;
    while (count < @min(128, sequences.len)) {
        if (!try session.fitsRun(try chunkPlan(session, sequences[0 .. count + 1], retained))) break;
        count += 1;
    }
    if (count == 0) return error.ResourceLimitExceeded;
    return count;
}

fn executeChunk(a: std.mem.Allocator, scratch: std.mem.Allocator, execution: *@import("../backends/session.zig").RunPermit, cfg: model.Config, tasks: []const Task, sequences: []const Sequence, pad: i32, decisions: []Decision, control: ?Control) !void {
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
        kinds[i] = @intFromEnum(task.question.kind);
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
    const outputs = try execution.runWithControl(&inputs, scratch, control);
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    const acts = outputs[1].asFloat32();
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
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), d.act_probability, 1e-6);
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
    var sequences = [_]Sequence{.{ .ids = ids[0..2], .markers = &markers }} ** 512;
    try std.testing.expectEqual(@as(usize, 128), try selectChunk(session, &sequences, 1024));
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .backend_limit_bytes = 2 * 2 * 4096 }, .static_workspace_bytes = 0, .check_live_memory = false };
    sequences[2].ids = &ids;
    try std.testing.expectEqual(@as(usize, 2), try selectChunk(session, &sequences, 1024));
    const plan = try chunkPlan(session, sequences[0..3], 1024);
    try std.testing.expectEqual(@as(usize, 8), plan.sequence);
    try std.testing.expectEqual(@as(usize, 1024), plan.pre_admitted_host_bytes);
    session.run_admission.?.limits.backend_limit_bytes = 4096;
    try std.testing.expectError(error.ResourceLimitExceeded, selectChunk(session, &sequences, 1024));
    session.run_admission.?.limits.backend_limit_bytes = 1024 * 1024;
    session.run_admission.?.limits.host_limit_bytes = 1023;
    try std.testing.expectError(error.ResourceLimitExceeded, selectChunk(session, &sequences, 1024));

    // Permanent limits fit four rows, but an outer HTTP scratch lease leaves
    // room for only two. Admission must shrink before executing any model work.
    session.run_admission.?.limits.host_limit_bytes = 1024 * 1024;
    session.run_admission.?.limits.scratch_limit_bytes = 40_000;
    sequences[2].ids = ids[0..2];
    try std.testing.expectEqual(@as(usize, 4), try selectChunk(session, sequences[0..4], 0));
    {
        var outer = try session.admitHostPreprocess(18_000);
        defer outer.deinit();
        var admitted = try admitChunk(session, sequences[0..4], 0);
        defer admitted.permit.deinit();
        try std.testing.expectEqual(@as(usize, 2), admitted.count);
    }
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    {
        var outer = try session.admitHostPreprocess(39_500);
        defer outer.deinit();
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitChunk(session, sequences[0..4], 0));
    }
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "laya decoding rejects nonfinite logits without leaking probabilities" {
    const q = Question{ .name = "choice", .kind = .choice, .instruction = "choose", .labels = &.{ "a", "b" }, .descriptions = &.{ "", "" } };
    try std.testing.expectError(error.InvalidLayaOutput, decode(std.testing.allocator, .{}, q, &.{ 0, std.math.nan(f32) }, &.{ 0, 0 }));
    try std.testing.expectError(error.InvalidLayaOutput, decode(std.testing.allocator, .{}, q, &.{ 0, 0 }, &.{ 0, std.math.inf(f32) }));
}

test "laya length buckets are stable and require meaningful padding savings" {
    const a = std.testing.allocator;
    var ids = [_]i64{0} ** 149;
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
