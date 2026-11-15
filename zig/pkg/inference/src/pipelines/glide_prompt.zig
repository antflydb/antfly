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

//! GLiDE question prompts and token ids (Fastino `glide/prompt.py` and the
//! encoder wrap in `glide/encoder.py`, prompt format
//! `quick-empty-think-medium-v1`).
//!
//! Every question becomes one prompt:
//!
//!     <|im_start|>user
//!     Context:
//!     <state>
//!
//!     Question: <instructions>
//!
//!     Options:
//!     (A) key: description
//!     ...
//!
//!     Think it through briefly, then answer with the option letter only.<|im_end|>
//!     <|im_start|>assistant
//!     <think>
//!
//!     </think>
//!
//!     Answer: (
//!
//! The chat-template wrapper is fixed: the checkpoint's template renders it
//! with `enable_thinking=True, reasoning_effort="medium"`. It is not rendered
//! at runtime (the template's own default effort injects a system turn). Both
//! released checkpoints render the same text: 0.4B through its
//! `render_tokenizer/`, 0.8B through its own template.
//!
//! Token ids: decoder checkpoints encode the text without special tokens.
//! Encoder checkpoints encode it with the root (ModernBERT) tokenizer, keep
//! the first `trunc_head` and the last `max_len - 2 - trunc_head` ids of an
//! over-long prompt and wrap the result as `[CLS] ids [SEP]`.

const std = @import("std");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const glide = @import("../models/glide.zig");

pub const Kind = enum { choice, score, noul };

/// One prompt option: its key (the answer label) and the description shown.
/// A description equal to the key renders as `(A) key`.
pub const Option = struct { key: []const u8, description: []const u8 };

pub const Question = struct {
    name: []const u8,
    kind: Kind,
    /// Rendered instruction text (`as_text` already applied).
    instructions: []const u8,
    options: []const Option,
};

/// One state with its questions; every question is one prompt.
pub const Item = struct {
    /// Rendered state text (non-string states are JSON-encoded).
    state: []const u8,
    questions: []const Question,
};

pub const prompt_prefix = "<|im_start|>user\n";
pub const prompt_suffix = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\nAnswer: (";
pub const body_tail = "\n\nThink it through briefly, then answer with the option letter only.";

/// Noul options, true FIRST (the reverse of the typed-decision label order).
pub const noul_true_default = "Yes";
pub const noul_false_default = "No";

/// Full prompt text for one question. `codes` are the checkpoint's option codes.
pub fn render(a: std.mem.Allocator, codes: []const []const u8, state: []const u8, question: Question) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try renderInto(a, &out, codes, state, question);
    return out.toOwnedSlice(a);
}

pub fn renderInto(a: std.mem.Allocator, out: *std.ArrayList(u8), codes: []const []const u8, state: []const u8, question: Question) !void {
    if (question.options.len == 0 or question.options.len > codes.len or question.options.len > glide.max_options) return error.InvalidGlideQuestion;
    try out.appendSlice(a, prompt_prefix);
    const body_start = out.items.len;
    try out.appendSlice(a, "Context:\n");
    try out.appendSlice(a, state);
    try out.appendSlice(a, "\n\nQuestion: ");
    try out.appendSlice(a, question.instructions);
    try out.appendSlice(a, "\n\nOptions:\n");
    for (question.options, 0..) |option, i| {
        if (i != 0) try out.append(a, '\n');
        try out.append(a, '(');
        try out.appendSlice(a, codes[i]);
        try out.appendSlice(a, ") ");
        try out.appendSlice(a, option.key);
        if (!std.mem.eql(u8, option.description, option.key)) {
            try out.appendSlice(a, ": ");
            try out.appendSlice(a, option.description);
        }
    }
    // Python `str.rstrip()` on the body, before the fixed tail.
    out.items.len = body_start + pythonRstripLen(out.items[body_start..]);
    try out.appendSlice(a, body_tail);
    try out.appendSlice(a, prompt_suffix);
}

/// Length of `text` after Python's `str.rstrip()` (Unicode whitespace).
pub fn pythonRstripLen(text: []const u8) usize {
    var end = text.len;
    while (end > 0) {
        var start = end - 1;
        while (start > 0 and (text[start] & 0xC0) == 0x80 and end - start < 4) start -= 1;
        const cp = std.unicode.utf8Decode(text[start..end]) catch return end;
        if (!pythonIsSpace(cp)) return end;
        end = start;
    }
    return 0;
}

fn pythonIsSpace(cp: u21) bool {
    return switch (cp) {
        0x09...0x0D, 0x1C...0x20, 0x85, 0xA0, 0x1680, 0x2000...0x200A, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000 => true,
        else => false,
    };
}

// ------------------------------------------------------------------ tokens

pub const EncodeError = error{GlidePromptTooLong};

/// Prompt token ids fed to the backbone (special tokens included).
/// Decoder prompts longer than `max_len - 8` tokens are rejected (the
/// reference's `len + 8 > max_len` request error); encoder prompts are
/// head/tail truncated to `max_len` like the reference.
pub fn encode(a: std.mem.Allocator, tok: Tokenizer, config: *const glide.DecisionConfig, text: []const u8) ![]i32 {
    const raw = try tok.encode(a, text);
    switch (config.backbone) {
        .decoder => {
            if (raw.len + 8 > config.max_len) {
                a.free(raw);
                return error.GlidePromptTooLong;
            }
            return raw;
        },
        .encoder => {
            defer a.free(raw);
            const special = tok.specialTokens();
            if (special.cls_id < 0 or special.sep_id < 0) return error.InvalidGlideTokenizer;
            const budget = config.max_len - 2;
            const keep = @min(raw.len, budget);
            const ids = try a.alloc(i32, keep + 2);
            ids[0] = special.cls_id;
            if (raw.len <= budget) {
                @memcpy(ids[1..][0..raw.len], raw);
            } else {
                const head = config.trunc_head;
                const tail = budget - head;
                @memcpy(ids[1..][0..head], raw[0..head]);
                @memcpy(ids[1 + head ..][0..tail], raw[raw.len - tail ..]);
            }
            ids[keep + 1] = special.sep_id;
            return ids;
        },
    }
}

// ------------------------------------------------------------------ micro-batches

/// End (exclusive) of the micro-batch starting at `start` over prompt
/// lengths sorted ascending: the reference rule adds row `j` while
/// `(j - start + 1) * lengths[j] <= budget`; a prompt longer than the budget
/// runs alone. `max_rows` additionally caps the batch.
pub fn microBatchEnd(lengths: []const usize, start: usize, budget: usize, max_rows: usize) usize {
    var end = start + 1;
    while (end < lengths.len and end - start < max_rows and (end - start + 1) * lengths[end] <= budget) end += 1;
    return end;
}

// ------------------------------------------------------------------ public decision contract

/// Public typed-decision request -> GLiDE questions (shared by every input):
///   choice    choices[{value, description}] in order; a missing description
///             renders `(A) value`.
///   predicate noul: `(A) true: Yes`, `(B) false: No`; answers p(true).
///   score     levels[{label, description}] in order as keys "0".."n-1"; the
///             level text is `label: description` when both are given and
///             differ, else `label`; a label equal to its index ("0", "1",
///             ...) shows only the description.
/// `request` is `antfly_decisions.Request` (borrowed into `a`).
pub fn questionsFromPublic(a: std.mem.Allocator, request: anytype) ![]Question {
    const questions = try a.alloc(Question, request.inner.questions.len);
    for (questions, request.inner.questions, request.policies) |*dest, question, policy| {
        const kind: Kind = switch (question.kind) {
            .choice => .choice,
            .score => .score,
            .noul => .noul,
        };
        const options = try a.alloc(Option, question.labels.len);
        switch (kind) {
            .choice => for (options, question.labels, question.descriptions) |*option, label, description| {
                option.* = .{ .key = label, .description = description };
            },
            .noul => {
                if (options.len != 2) return error.InvalidGlideQuestion;
                options[0] = .{ .key = "true", .description = noul_true_default };
                options[1] = .{ .key = "false", .description = noul_false_default };
            },
            .score => for (options, question.labels, question.descriptions, 0..) |*option, key, description, i| {
                const label: []const u8 = if (i < policy.level_labels.len) policy.level_labels[i] else description;
                // A label that only repeats the level index adds nothing.
                option.* = .{
                    .key = key,
                    .description = if (std.mem.eql(u8, label, description) or std.mem.eql(u8, label, key)) description else try std.fmt.allocPrint(a, "{s}: {s}", .{ label, description }),
                };
            },
        }
        if (options.len > glide.max_options) return error.InvalidGlideQuestion;
        dest.* = .{ .name = question.name, .kind = kind, .instructions = question.instructions, .options = options };
    }
    return questions;
}

/// Typed-decision envelope for `antfly_decisions.trainedResponse` (the
/// `decisions` row shape shared with Laya): one row per item, one decision per
/// question with its full distribution keyed by option key, which is exactly
/// the typed label ("true"/"false", "0".."n-1", or the choice value).
/// `probabilities` is item-major (see pipelines/glide.zig Result).
pub fn decisionsJson(a: std.mem.Allocator, questions: []const Question, item_count: usize, probabilities: []const []const f64, prompt_tokens: usize) ![]u8 {
    if (probabilities.len != item_count * questions.len) return error.InvalidDecideOutput;
    const V = std.json.Value;
    var data: std.array_list.Managed(V) = .init(a);
    for (0..item_count) |item| {
        var decisions: std.array_list.Managed(V) = .init(a);
        for (questions, 0..) |question, q| {
            const values = probabilities[item * questions.len + q];
            if (values.len != question.options.len) return error.InvalidDecideOutput;
            var rows: std.array_list.Managed(V) = .init(a);
            for (question.options, values) |option, p| {
                var row: std.json.ObjectMap = .empty;
                try row.put(a, "label", .{ .string = option.key });
                try row.put(a, "probability", .{ .float = p });
                try rows.append(.{ .object = row });
            }
            var decision: std.json.ObjectMap = .empty;
            try decision.put(a, "name", .{ .string = question.name });
            try decision.put(a, "probabilities", .{ .array = rows });
            try decisions.append(.{ .object = decision });
        }
        var entry: std.json.ObjectMap = .empty;
        try entry.put(a, "decisions", .{ .array = decisions });
        try data.append(.{ .object = entry });
    }
    var usage: std.json.ObjectMap = .empty;
    try usage.put(a, "prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
    try usage.put(a, "completion_tokens", .{ .integer = 0 });
    var root: std.json.ObjectMap = .empty;
    try root.put(a, "data", .{ .array = data });
    try root.put(a, "usage", .{ .object = usage });
    return std.json.Stringify.valueAlloc(a, V{ .object = root }, .{});
}

// ------------------------------------------------------------------ Fastino "systemone" requests

/// Parse one Fastino request `{"state": ..., "questions": {key: question}}`
/// exactly like `glide/prompt.py` (`options_for`, `as_text`, state JSON).
/// Multimodal (image) states are rejected. Strings borrow from `value`.
pub fn parseSystemOne(a: std.mem.Allocator, value: std.json.Value) !Item {
    if (value != .object) return error.InvalidGlideRequest;
    const questions_value = value.object.get("questions") orelse return error.InvalidGlideRequest;
    if (questions_value != .object) return error.InvalidGlideRequest;
    const state_value: std.json.Value = value.object.get("state") orelse .null;
    if (state_value == .array) for (state_value.array.items) |part| {
        if (part == .object) if (part.object.get("type")) |t| if (t == .string) {
            for ([_][]const u8{ "image_url", "image", "input_image" }) |image| if (std.mem.eql(u8, t.string, image)) return error.UnsupportedGlideImages;
        };
    };
    const state = switch (state_value) {
        .string => |s| s,
        else => try jsonDumps(a, state_value),
    };
    const questions = try a.alloc(Question, questions_value.object.count());
    for (questions_value.object.keys(), questions_value.object.values(), questions) |name, raw, *dest| {
        if (raw != .object) return error.InvalidGlideRequest;
        const type_value = raw.object.get("type") orelse return error.InvalidGlideRequest;
        if (type_value != .string) return error.InvalidGlideRequest;
        const kind = std.meta.stringToEnum(Kind, type_value.string) orelse return error.InvalidGlideRequest;
        const instructions = try asText(a, raw.object.get("instructions") orelse std.json.Value{ .string = "" });
        const options = try optionsFor(a, kind, raw.object.get("criteria") orelse .null);
        if (options.len > glide.max_options) return error.InvalidGlideQuestion;
        dest.* = .{ .name = name, .kind = kind, .instructions = instructions, .options = options };
    }
    return .{ .state = state, .questions = questions };
}

fn truthy(v: std.json.Value) bool {
    return switch (v) {
        .null => false,
        .bool => |b| b,
        .integer => |i| i != 0,
        .float => |f| f != 0,
        .number_string => |s| !std.mem.eql(u8, s, "0"),
        .string => |s| s.len != 0,
        .array => |arr| arr.items.len != 0,
        .object => |obj| obj.count() != 0,
    };
}

/// Python `str(x)` for JSON scalars; containers would use Python repr.
fn pythonStr(a: std.mem.Allocator, v: std.json.Value) ![]const u8 {
    return switch (v) {
        .string => |s| s,
        .null => "None",
        .bool => |b| if (b) "True" else "False",
        .integer => |i| try std.fmt.allocPrint(a, "{d}", .{i}),
        .number_string => |s| s,
        .float => |f| try pythonFloatRepr(a, f),
        .array, .object => error.UnsupportedGlideCriteria,
    };
}

fn optionsFor(a: std.mem.Allocator, kind: Kind, criteria: std.json.Value) ![]Option {
    switch (kind) {
        .noul => {
            const options = try a.alloc(Option, 2);
            const c: ?std.json.ObjectMap = if (criteria == .object) criteria.object else null;
            const yes = if (c) |m| m.get("true") else null;
            const no = if (c) |m| m.get("false") else null;
            options[0] = .{ .key = "true", .description = if (yes != null and truthy(yes.?)) try pythonStr(a, yes.?) else noul_true_default };
            options[1] = .{ .key = "false", .description = if (no != null and truthy(no.?)) try pythonStr(a, no.?) else noul_false_default };
            return options;
        },
        .score => {
            var levels: std.ArrayList(std.json.Value) = .empty;
            switch (criteria) {
                .array => |arr| try levels.appendSlice(a, arr.items),
                .object => |obj| {
                    const Keyed = struct { key: f64, value: std.json.Value };
                    const keyed = try a.alloc(Keyed, obj.count());
                    for (obj.keys(), obj.values(), keyed) |key, value, *dest| dest.* = .{
                        .key = std.fmt.parseFloat(f64, std.mem.trim(u8, key, " \t\r\n")) catch return error.InvalidGlideCriteria,
                        .value = value,
                    };
                    std.sort.insertion(Keyed, keyed, {}, struct {
                        fn less(_: void, l: Keyed, r: Keyed) bool {
                            return l.key < r.key;
                        }
                    }.less);
                    for (keyed) |entry| try levels.append(a, entry.value);
                },
                .null => return error.InvalidGlideCriteria,
                else => return error.InvalidGlideCriteria,
            }
            if (levels.items.len == 0) return error.InvalidGlideCriteria;
            const options = try a.alloc(Option, levels.items.len);
            for (options, levels.items, 0..) |*option, level, i| option.* = .{
                .key = try std.fmt.allocPrint(a, "{d}", .{i}),
                .description = try pythonStr(a, level),
            };
            return options;
        },
        .choice => {
            if (criteria != .object or criteria.object.count() == 0) return error.InvalidGlideCriteria;
            const options = try a.alloc(Option, criteria.object.count());
            for (criteria.object.keys(), criteria.object.values(), options) |key, value, *option| {
                option.* = .{ .key = key, .description = switch (value) {
                    .string => |s| if (s.len != 0) s else key,
                    .null => key,
                    else => try jsonDumps(a, value),
                } };
            }
            return options;
        },
    }
}

/// `as_text`: strings as-is; objects as `key: value` lines (non-string values
/// JSON-encoded); anything else JSON-encoded.
pub fn asText(a: std.mem.Allocator, v: std.json.Value) ![]const u8 {
    switch (v) {
        .string => |s| return s,
        .object => |obj| {
            var out: std.ArrayList(u8) = .empty;
            for (obj.keys(), obj.values(), 0..) |key, value, i| {
                if (i != 0) try out.append(a, '\n');
                try out.appendSlice(a, key);
                try out.appendSlice(a, ": ");
                if (value == .string) try out.appendSlice(a, value.string) else try writeJson(a, &out, value);
            }
            return out.toOwnedSlice(a);
        },
        else => return jsonDumps(a, v),
    }
}

/// Python `json.dumps(v, ensure_ascii=False)` with default separators.
pub fn jsonDumps(a: std.mem.Allocator, v: std.json.Value) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try writeJson(a, &out, v);
    return out.toOwnedSlice(a);
}

fn writeJson(a: std.mem.Allocator, out: *std.ArrayList(u8), v: std.json.Value) !void {
    switch (v) {
        .null => try out.appendSlice(a, "null"),
        .bool => |b| try out.appendSlice(a, if (b) "true" else "false"),
        .integer => |i| try out.print(a, "{d}", .{i}),
        .number_string => |s| try out.appendSlice(a, s),
        .float => |f| {
            const repr = try pythonFloatRepr(a, f);
            defer a.free(repr);
            try out.appendSlice(a, repr);
        },
        .string => |s| try writeJsonString(a, out, s),
        .array => |arr| {
            try out.append(a, '[');
            for (arr.items, 0..) |item, i| {
                if (i != 0) try out.appendSlice(a, ", ");
                try writeJson(a, out, item);
            }
            try out.append(a, ']');
        },
        .object => |obj| {
            try out.append(a, '{');
            for (obj.keys(), obj.values(), 0..) |key, value, i| {
                if (i != 0) try out.appendSlice(a, ", ");
                try writeJsonString(a, out, key);
                try out.appendSlice(a, ": ");
                try writeJson(a, out, value);
            }
            try out.append(a, '}');
        },
    }
}

fn writeJsonString(a: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try out.append(a, '"');
    for (s) |c| switch (c) {
        '"' => try out.appendSlice(a, "\\\""),
        '\\' => try out.appendSlice(a, "\\\\"),
        '\n' => try out.appendSlice(a, "\\n"),
        '\r' => try out.appendSlice(a, "\\r"),
        '\t' => try out.appendSlice(a, "\\t"),
        0x08 => try out.appendSlice(a, "\\b"),
        0x0C => try out.appendSlice(a, "\\f"),
        0x00...0x07, 0x0B, 0x0E...0x1F => try out.print(a, "\\u{x:0>4}", .{c}),
        else => try out.append(a, c),
    };
    try out.append(a, '"');
}

/// Python `repr(float)`: shortest round-trip digits; scientific notation
/// when the decimal exponent is < -4 or >= 16.
pub fn pythonFloatRepr(a: std.mem.Allocator, f: f64) ![]u8 {
    if (std.math.isNan(f)) return a.dupe(u8, "NaN");
    if (std.math.isInf(f)) return a.dupe(u8, if (f > 0) "Infinity" else "-Infinity");
    var buf: [64]u8 = undefined;
    const sci = try std.fmt.bufPrint(&buf, "{e}", .{f});
    // `sci` is "[-]d[.ddd]e[-]x".
    const e_at = std.mem.indexOfScalar(u8, sci, 'e') orelse return error.InvalidFloat;
    const negative = sci[0] == '-';
    const mantissa = sci[@intFromBool(negative)..e_at];
    const exponent = try std.fmt.parseInt(i32, sci[e_at + 1 ..], 10);
    var digits_buf: [32]u8 = undefined;
    var nd: usize = 0;
    for (mantissa) |c| if (c != '.') {
        digits_buf[nd] = c;
        nd += 1;
    };
    while (nd > 1 and digits_buf[nd - 1] == '0') nd -= 1;
    const digits = digits_buf[0..nd];
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    if (negative) try out.append(a, '-');
    if (digits.len == 1 and digits[0] == '0') {
        try out.appendSlice(a, "0.0");
        return out.toOwnedSlice(a);
    }
    if (exponent < -4 or exponent >= 16) {
        try out.append(a, digits[0]);
        if (digits.len > 1) {
            try out.append(a, '.');
            try out.appendSlice(a, digits[1..]);
        }
        try out.print(a, "e{c}{d:0>2}", .{ @as(u8, if (exponent < 0) '-' else '+'), @abs(exponent) });
    } else if (exponent < 0) {
        try out.appendSlice(a, "0.");
        for (0..@intCast(-exponent - 1)) |_| try out.append(a, '0');
        try out.appendSlice(a, digits);
    } else {
        const int_len: usize = @as(usize, @intCast(exponent)) + 1;
        if (digits.len <= int_len) {
            try out.appendSlice(a, digits);
            for (0..int_len - digits.len) |_| try out.append(a, '0');
            try out.appendSlice(a, ".0");
        } else {
            try out.appendSlice(a, digits[0..int_len]);
            try out.append(a, '.');
            try out.appendSlice(a, digits[int_len..]);
        }
    }
    return out.toOwnedSlice(a);
}

// ------------------------------------------------------------------ tests

const test_codes = [_][]const u8{ "A", "B", "C", "D", "E", "F" };

test "GLiDE prompt renders options, key-only lines and trailing-whitespace rstrip" {
    const a = std.testing.allocator;
    const text = try render(a, &test_codes, "{\"ticket\": \"x\"}", .{
        .name = "intent",
        .kind = .choice,
        .instructions = "What?",
        .options = &.{ .{ .key = "refund", .description = "Get money back" }, .{ .key = "other", .description = "other  \n" } },
    });
    defer a.free(text);
    try std.testing.expectEqualStrings(
        "<|im_start|>user\nContext:\n{\"ticket\": \"x\"}\n\nQuestion: What?\n\nOptions:\n(A) refund: Get money back\n(B) other: other\n\n" ++
            "Think it through briefly, then answer with the option letter only.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\nAnswer: (",
        text,
    );
    const key_only = try render(a, &test_codes, "s", .{ .name = "q", .kind = .choice, .instructions = "?", .options = &.{ .{ .key = "a", .description = "a" }, .{ .key = "b", .description = "B\u{3000}" } } });
    defer a.free(key_only);
    try std.testing.expect(std.mem.indexOf(u8, key_only, "(A) a\n(B) b: B\n\nThink") != null);
    try std.testing.expectEqual(@as(usize, 0), pythonRstripLen(" \t\u{a0}\u{2029}"));
    try std.testing.expectEqual(@as(usize, 3), pythonRstripLen("x\u{e9}\u{85} "));
}

test "GLiDE systemone parsing matches prompt.py options_for, as_text and JSON states" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json =
        \\{"state": {"order": {"id": 4821, "items": ["lamp", "desk"], "ok": true, "x": null, "p": 0.5, "big": 1e20, "s": "é\n\u0001"}},
        \\ "questions": {"u": {"type": "noul", "instructions": "Esc?"},
        \\   "r": {"type": "noul", "instructions": {"goal": "risk", "n": [1, 2]}, "criteria": {"true": "Yes, risk", "false": ""}},
        \\   "s": {"type": "score", "instructions": "Sev?", "criteria": {"10": "high", "2": "low", "3.5": 7}},
        \\   "c": {"type": "choice", "instructions": "Pick", "criteria": {"a": "A", "b": "", "c": {"description": "C", "examples": ["x"]}}}}}
    ;
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{});
    const item = try parseSystemOne(a, parsed);
    try std.testing.expectEqualStrings("{\"order\": {\"id\": 4821, \"items\": [\"lamp\", \"desk\"], \"ok\": true, \"x\": null, \"p\": 0.5, \"big\": 1e+20, \"s\": \"\u{e9}\\n\\u0001\"}}", item.state);
    try std.testing.expectEqual(@as(usize, 4), item.questions.len);
    const u = item.questions[0];
    try std.testing.expectEqualStrings("true", u.options[0].key);
    try std.testing.expectEqualStrings("Yes", u.options[0].description);
    try std.testing.expectEqualStrings("No", u.options[1].description);
    const r = item.questions[1];
    try std.testing.expectEqualStrings("goal: risk\nn: [1, 2]", r.instructions);
    try std.testing.expectEqualStrings("Yes, risk", r.options[0].description);
    try std.testing.expectEqualStrings("No", r.options[1].description);
    const s = item.questions[2];
    try std.testing.expectEqualStrings("low", s.options[0].description);
    try std.testing.expectEqualStrings("7", s.options[1].description);
    try std.testing.expectEqualStrings("high", s.options[2].description);
    try std.testing.expectEqualStrings("2", s.options[2].key);
    const c = item.questions[3];
    try std.testing.expectEqualStrings("b", c.options[1].description);
    try std.testing.expectEqualStrings("{\"description\": \"C\", \"examples\": [\"x\"]}", c.options[2].description);
}

test "GLiDE Python float repr" {
    const a = std.testing.allocator;
    for ([_]struct { f64, []const u8 }{
        .{ 1.0, "1.0" },          .{ 0.1, "0.1" },                 .{ 1e-4, "0.0001" },           .{ 1e-5, "1e-05" },
        .{ 1e16, "1e+16" },       .{ 1e15, "1000000000000000.0" }, .{ -2.5, "-2.5" },             .{ 123.456, "123.456" },
        .{ 1.5e300, "1.5e+300" }, .{ 0.0, "0.0" },                 .{ 12345678.9, "12345678.9" },
    }) |case| {
        const got = try pythonFloatRepr(a, case[0]);
        defer a.free(got);
        try std.testing.expectEqualStrings(case[1], got);
    }
}

test "GLiDE public decision contract maps choice, predicate and score questions" {
    const decisions = @import("antfly_decisions");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try decisions.parse(a,
        \\{"model":"glide","input":"Where is my package?","questions":[
        \\ {"name":"intent","type":"choice","instructions":"What does the customer want?","choices":[{"value":"refund","description":"Get money back"},{"value":"other"}]},
        \\ {"name":"urgent","type":"predicate","instructions":"Escalate?"},
        \\ {"name":"priority","type":"score","instructions":"Priority?","levels":[{"label":"low","description":"no impact"},{"label":"high"}]},
        \\ {"name":"urgency","type":"score","instructions":"Urgency?","levels":[{"label":"0","description":"Routine"},{"label":"1","description":"Soon"}]}]}
    );
    const questions = try questionsFromPublic(a, request);
    try std.testing.expectEqual(Kind.choice, questions[0].kind);
    try std.testing.expectEqualStrings("other", questions[0].options[1].description);
    try std.testing.expectEqual(Kind.noul, questions[1].kind);
    try std.testing.expectEqualStrings("true", questions[1].options[0].key);
    try std.testing.expectEqualStrings("Yes", questions[1].options[0].description);
    try std.testing.expectEqualStrings("false", questions[1].options[1].key);
    try std.testing.expectEqualStrings("0", questions[2].options[0].key);
    try std.testing.expectEqualStrings("low: no impact", questions[2].options[0].description);
    try std.testing.expectEqualStrings("high", questions[2].options[1].description);
    try std.testing.expectEqualStrings("Routine", questions[3].options[0].description);
    const text = try render(a, &test_codes, request.items[0].input, questions[2]);
    try std.testing.expect(std.mem.indexOf(u8, text, "(A) 0: low: no impact\n(B) 1: high\n\nThink") != null);
    const choice = try render(a, &test_codes, request.items[0].input, questions[0]);
    try std.testing.expect(std.mem.indexOf(u8, choice, "(A) refund: Get money back\n(B) other\n\nThink") != null);
}

test "GLiDE answers present through the public typed-decision response" {
    const decisions = @import("antfly_decisions");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try decisions.parse(a,
        \\{"model":"glide","inputs":[{"id":"x","input":"one"},{"input":"two"}],"questions":[
        \\ {"name":"intent","type":"choice","instructions":"?","choices":[{"value":"refund"},{"value":"other"}]},
        \\ {"name":"urgent","type":"predicate","instructions":"?"},
        \\ {"name":"priority","type":"score","instructions":"?","levels":[{"label":"low"},{"label":"mid"},{"label":"high"}]}]}
    );
    const questions = try questionsFromPublic(a, request);
    // Option order: choice as given, noul true first, score levels ascending.
    const probabilities = [_][]const f64{
        &.{ 0.7, 0.3 }, &.{ 0.8, 0.2 },   &.{ 0.1, 0.2, 0.7 },
        &.{ 0.4, 0.6 }, &.{ 0.25, 0.75 }, &.{ 0.5, 0.5, 0.0 },
    };
    const json = try decisionsJson(a, questions, 2, &probabilities, 42);
    const response = try decisions.trainedResponse(a, request, json, .laya);
    const value = try std.json.parseFromSliceLeaky(std.json.Value, a, response, .{});
    try std.testing.expectEqual(@as(i64, 42), value.object.get("usage").?.object.get("input_tokens").?.integer);
    const rows = value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    const first = rows[0].object.get("answers").?.array.items;
    try std.testing.expectEqualStrings("refund", first[0].object.get("choice").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), first[1].object.get("probability").?.float, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1.6), first[2].object.get("score").?.float, 1e-12);
    try std.testing.expectEqualStrings("high", first[2].object.get("probabilities").?.array.items[2].object.get("label").?.string);
    const second = rows[1].object.get("answers").?.array.items;
    try std.testing.expectEqualStrings("other", second[0].object.get("choice").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), second[1].object.get("probability").?.float, 1e-12);
}

test "GLiDE micro-batches follow the reference padded-token budget" {
    const lengths = [_]usize{ 68, 87, 88, 106, 122, 128, 263, 287, 729, 743, 753, 2078, 2088, 5129, 20000 };
    var ends: [lengths.len]usize = undefined;
    var count: usize = 0;
    var start: usize = 0;
    while (start < lengths.len) : (count += 1) {
        ends[count] = microBatchEnd(&lengths, start, 16384, 128);
        start = ends[count];
    }
    // 11 * 753 fits but 12 * 2078 does not; 3 * 5129 fits; 20000 runs alone.
    try std.testing.expectEqualSlices(usize, &.{ 11, 14, 15 }, ends[0..count]);
    try std.testing.expectEqual(@as(usize, 2), microBatchEnd(&lengths, 0, 16384, 2));
}
