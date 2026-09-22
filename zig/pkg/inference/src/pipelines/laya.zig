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
const model = @import("../models/laya.zig");
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
pub const Result = struct { decisions: []Decision, prompt_tokens: usize };

fn encodeClean(a: std.mem.Allocator, tok: Tokenizer, text: []const u8, mask: []const u8) ![]i32 {
    if (mask.len == 0) return error.InvalidLayaTokenizer;
    const clean = try std.mem.replaceOwned(u8, a, text, mask, " ");
    defer a.free(clean);
    return tok.encode(a, clean);
}

/// Allocations belong to the caller's request arena. State overflow is rejected,
/// unlike upstream's silent truncation; question/option formatting matches it.
pub fn prepare(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, task: Task) !Sequence {
    const q = task.question;
    if (q.labels.len < 2 or q.labels.len > 20 or q.labels.len != q.descriptions.len) return error.InvalidLayaQuestion;
    const special = tok.specialTokens();
    const mask = cfg.mask_token[0..cfg.mask_token_len];
    const head_text = try std.fmt.allocPrint(a, "{s} question: {s}", .{ @tagName(q.kind), q.instruction });
    defer a.free(head_text);
    const head = try encodeClean(a, tok, head_text, mask);
    defer a.free(head);
    const options = try a.alloc([]i32, q.labels.len);
    defer a.free(options);
    var initialized: usize = 0;
    defer for (options[0..initialized]) |ids| a.free(ids);
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
        options_len += 1 + @min(options[i].len, 48);
    }
    const per: usize = if (options_len + 16 > cfg.head_max_len) @max(4, (cfg.head_max_len - 16) / options.len) else 49;
    options_len = 0;
    for (options) |ids| options_len += @min(1 + @min(ids.len, 48), per);
    const budget = cfg.head_max_len -| options_len;
    const head_len = @min(head.len, @max(8, budget));
    const state = try encodeClean(a, tok, task.text, mask);
    defer a.free(state);
    const total = 4 + head_len + options_len + state.len;
    if (total > cfg.max_len) return error.ExtractionTextLimitExceeded;
    const ids = try a.alloc(i64, total);
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
        for (option[0..@min(@min(option.len, 48), per - 1)]) |id| {
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
    var permit = try session.admitHostPreprocess(tasks.len * cfg.max_len * 64);
    defer permit.deinit();
    const sequences = try a.alloc(Sequence, tasks.len);
    var seq: usize = 0;
    var count: usize = 0;
    var tokens: usize = 0;
    // Prepare the whole batch before running any model work.
    for (tasks, sequences) |task, *prepared| {
        if (control) |active| try active.check();
        prepared.* = try prepare(a, tok, cfg, task);
        if (max_input_tokens) |limit| if (prepared.ids.len > limit) return error.InferenceInputTokensExceeded;
        seq = @max(seq, prepared.ids.len);
        count = @max(count, prepared.markers.len);
        tokens += prepared.ids.len;
    }
    const ids = try a.alloc(i64, tasks.len * seq);
    @memset(ids, tok.specialTokens().pad_id);
    const mask = try a.alloc(i64, ids.len);
    @memset(mask, 0);
    const kinds = try a.alloc(i64, tasks.len);
    const markers = try a.alloc(i64, tasks.len * count);
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
    inputs[0] = try Tensor.initInt64(a, "input_ids", &.{ @intCast(tasks.len), @intCast(seq) }, ids);
    initialized += 1;
    inputs[1] = try Tensor.initInt64(a, "attention_mask", &.{ @intCast(tasks.len), @intCast(seq) }, mask);
    initialized += 1;
    inputs[2] = try Tensor.initInt64(a, "qtype", &.{ @intCast(tasks.len), 1 }, kinds);
    initialized += 1;
    inputs[3] = try Tensor.initInt64(a, "marker_pos", &.{ @intCast(tasks.len), @intCast(count) }, markers);
    initialized += 1;
    if (try session.runLayaDecisionsWithControl(&inputs, scratch, control)) |outputs| {
        defer {
            for (outputs) |*output| output.deinit();
            scratch.free(outputs);
        }
        if (outputs.len != 1 or outputs[0].dtype != .f32) return error.UnexpectedOutputShape;
        const result_values = outputs[0].asFloat32();
        const width = count + 6;
        if (result_values.len != tasks.len * width) return error.UnexpectedOutputShape;
        const decisions = try a.alloc(Decision, tasks.len);
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
        }
        return .{ .decisions = decisions, .prompt_tokens = tokens };
    }
    const outputs = try session.runWithControl(&inputs, scratch, control);
    defer {
        for (outputs) |*output| output.deinit();
        scratch.free(outputs);
    }
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32) return error.UnexpectedOutputShape;
    const logits = outputs[0].asFloat32();
    const acts = outputs[1].asFloat32();
    if (logits.len != tasks.len * count or acts.len != tasks.len * cfg.n_act) return error.UnexpectedOutputShape;
    const decisions = try a.alloc(Decision, tasks.len);
    for (tasks, decisions, 0..) |task, *decision, i| decision.* = try decode(a, cfg, task.question, logits[i * count ..][0..task.question.labels.len], acts[i * cfg.n_act ..][0..cfg.n_act]);
    if (control) |active| try active.check();
    return .{ .decisions = decisions, .prompt_tokens = tokens };
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
