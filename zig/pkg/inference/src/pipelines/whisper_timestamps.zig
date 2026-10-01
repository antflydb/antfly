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

//! Whisper timestamp-token handling.
//!
//! When the `<|notimestamps|>` prompt token is omitted, Whisper brackets each
//! phrase with `<|t|>` tokens whose id encodes an offset in 20 ms steps from
//! the window start. This module holds the decode-time logit rules that keep
//! those tokens well formed (the same rules as the reference implementation's
//! timestamp logits processor) and the post-decode parser that turns a token
//! stream into timed phrase segments and estimated word spans.

const std = @import("std");
const ops = @import("../ops/ops.zig");

/// Seconds per timestamp step.
pub const step_ms: u64 = 20;

pub const Rules = struct {
    timestamp_begin: i32,
    eot: i32,
    no_timestamps: i32,
    /// Largest timestamp index the first token may take (`<|1.00|>` = 50).
    max_initial_timestamp_index: usize = 50,

    pub fn isTimestamp(self: Rules, token: i32) bool {
        return token >= self.timestamp_begin;
    }
};

const neg_inf = -std.math.inf(f32);

/// Apply the timestamp constraints in place. `generated` holds the tokens
/// produced so far after the prompt (timestamps included).
pub fn applyRules(rules: Rules, logits: []f32, generated: []const i32) void {
    const begin: usize = @intCast(rules.timestamp_begin);
    if (begin >= logits.len) return;
    // The prompt already decided timestamps are on.
    if (rules.no_timestamps >= 0 and @as(usize, @intCast(rules.no_timestamps)) < logits.len)
        logits[@intCast(rules.no_timestamps)] = neg_inf;

    const eot: usize = @intCast(rules.eot);
    if (generated.len == 0) {
        // First token is a timestamp no later than the initial cap.
        suppressRange(logits, 0, begin, eot);
        const cap = @min(logits.len, begin + rules.max_initial_timestamp_index + 1);
        suppressRange(logits, cap, logits.len, eot);
        return;
    }

    // Reference semantics: a lone first timestamp counts as "penultimate was
    // a timestamp" so text follows the opening stamp; `text <|t|>` must be
    // closed by a second stamp; `<|a|><|b|>` must be followed by text.
    const last_is_ts = rules.isTimestamp(generated[generated.len - 1]);
    const penultimate_is_ts = generated.len < 2 or rules.isTimestamp(generated[generated.len - 2]);
    if (last_is_ts) {
        if (penultimate_is_ts) {
            suppressRange(logits, begin, logits.len, eot);
        } else {
            suppressRange(logits, 0, begin, eot);
        }
    }

    // Timestamps never go backwards within a window.
    var last_ts: ?i32 = null;
    var i = generated.len;
    while (i > 0) {
        i -= 1;
        if (rules.isTimestamp(generated[i])) {
            last_ts = generated[i];
            break;
        }
    }
    if (last_ts) |ts| {
        // Right after an opening stamp the closing one may repeat it (an
        // empty phrase); once text or a closed pair follows, the next stamp
        // must be strictly later. Mirrors the reference logits processor.
        const min_ts: usize = @intCast(ts);
        const first_allowed = if (last_is_ts and !penultimate_is_ts) min_ts else min_ts + 1;
        suppressRange(logits, begin, @min(first_allowed, logits.len), eot);
    }

    // If the timestamp mass beats the best text token, only a timestamp may
    // be chosen. Computed on log-softmax so both sides are comparable.
    var max_logit: f32 = neg_inf;
    for (logits) |v| if (v > max_logit) {
        max_logit = v;
    };
    if (max_logit == neg_inf) return;
    var ts_sum: f64 = 0;
    var text_max: f32 = neg_inf;
    for (logits, 0..) |v, idx| {
        if (v == neg_inf) continue;
        if (idx >= begin) {
            ts_sum += @exp(@as(f64, v - max_logit));
        } else if (v > text_max) {
            text_max = v;
        }
    }
    if (ts_sum <= 0) return;
    const ts_logprob: f32 = @floatCast(@log(ts_sum));
    if (ts_logprob > text_max - max_logit) suppressRange(logits, 0, begin, eot);
}

/// Range form of `applyRules` for backends that pick the token on the
/// device: everything except the timestamp-mass rule, which needs the
/// logits and is decided from the returned statistics in `chooseFromStats`.
pub const RuleWindow = struct {
    text_allowed: bool,
    /// Allowed timestamp ids are `[ts_min, ts_max)`.
    ts_min: usize,
    ts_max: usize,
};

pub fn ruleWindow(rules: Rules, generated: []const i32, vocab: usize) RuleWindow {
    const begin: usize = @intCast(rules.timestamp_begin);
    if (begin >= vocab) return .{ .text_allowed = true, .ts_min = vocab, .ts_max = vocab };
    var window = RuleWindow{ .text_allowed = true, .ts_min = begin, .ts_max = vocab };
    if (generated.len == 0) {
        window.text_allowed = false;
        window.ts_max = @min(vocab, begin + rules.max_initial_timestamp_index + 1);
        return window;
    }
    const last_is_ts = rules.isTimestamp(generated[generated.len - 1]);
    const penultimate_is_ts = generated.len < 2 or rules.isTimestamp(generated[generated.len - 2]);
    if (last_is_ts) {
        if (penultimate_is_ts) {
            window.ts_max = window.ts_min;
        } else {
            window.text_allowed = false;
        }
    }
    var last_ts: ?i32 = null;
    var i = generated.len;
    while (i > 0) {
        i -= 1;
        if (rules.isTimestamp(generated[i])) {
            last_ts = generated[i];
            break;
        }
    }
    if (last_ts) |ts| {
        const min_ts: usize = @intCast(ts);
        const first_allowed = if (last_is_ts and !penultimate_is_ts) min_ts else min_ts + 1;
        window.ts_min = @max(window.ts_min, @min(first_allowed, vocab));
    }
    if (window.ts_max < window.ts_min) window.ts_max = window.ts_min;
    return window;
}

/// Layout of the sixteen statistics the device kernel (and `hostLogitsStats`)
/// produce. Ids are stored as u32 bit patterns; `no_candidate` marks an
/// empty category.
pub const stats_best_value = 0;
pub const stats_best_id = 1;
pub const stats_text_value = 2;
pub const stats_text_id = 3;
pub const stats_ts_value = 4;
pub const stats_ts_id = 5;
pub const stats_raw_value = 6;
pub const stats_raw_id = 7;
pub const stats_all_max = 8;
pub const stats_all_sum = 9;
pub const stats_ts_max = 10;
pub const stats_ts_sum = 11;
pub const stats_raw_max = 12;
pub const stats_raw_sum = 13;
pub const stats_probe = 14;
/// In device-choice mode (`whisper_logits_mode_choose`) position 14 holds
/// the chosen token's id bits and 15 its log-probability instead.
pub const stats_choice_token = 14;
pub const stats_choice_logprob = 15;
/// Raw logit of `eot`, which the mass rule never removes.
pub const stats_eot = 15;
pub const no_candidate: u32 = 0xffff_ffff;

pub fn statsId(stats: *const ops.WhisperLogitsStatsRaw, index: usize) ?u32 {
    const id: u32 = @bitCast(stats[index]);
    return if (id == no_candidate) null else id;
}

fn lseFromPair(max_value: f32, sum: f32) f32 {
    if (sum <= 0) return neg_inf;
    return max_value + @log(sum);
}

pub fn statsLogSumExp(stats: *const ops.WhisperLogitsStatsRaw, max_index: usize, sum_index: usize) f32 {
    return lseFromPair(stats[max_index], stats[sum_index]);
}

pub const Choice = struct {
    token: u32,
    logprob: f64,
};

/// The token `applyRules` + argmax would pick, decided from the device
/// statistics: the timestamp-mass rule forces a timestamp when the
/// log-sum-exp of the allowed timestamps beats the best text logit. As in
/// `applyRules`, `eot` survives the rule, so it competes with the best
/// timestamp when `eot_allowed` (not on the explicit suppress list).
pub fn chooseFromStats(stats: *const ops.WhisperLogitsStatsRaw, timestamps_on: bool, eot: u32, eot_allowed: bool) ?Choice {
    const ts_id = statsId(stats, stats_ts_id);
    const text_id = statsId(stats, stats_text_id);
    if (timestamps_on and ts_id != null) {
        const lse_ts = statsLogSumExp(stats, stats_ts_max, stats_ts_sum);
        if (text_id == null or lse_ts > stats[stats_text_value]) {
            const eot_logit = stats[stats_eot];
            if (eot_allowed and eot_logit != neg_inf) {
                // lse over timestamps plus the surviving eot.
                const hi = @max(lse_ts, eot_logit);
                const lse = hi + @log(@exp(lse_ts - hi) + @exp(eot_logit - hi));
                if (eot_logit >= stats[stats_ts_value]) {
                    // Ties go to the lower id, and eot precedes every timestamp.
                    return .{ .token = eot, .logprob = @as(f64, eot_logit) - @as(f64, lse) };
                }
                return .{ .token = ts_id.?, .logprob = @as(f64, stats[stats_ts_value]) - @as(f64, lse) };
            }
            return .{ .token = ts_id.?, .logprob = @as(f64, stats[stats_ts_value]) - @as(f64, lse_ts) };
        }
    }
    const best_id = statsId(stats, stats_best_id) orelse return null;
    const lse_all = statsLogSumExp(stats, stats_all_max, stats_all_sum);
    return .{ .token = best_id, .logprob = @as(f64, stats[stats_best_value]) - @as(f64, lse_all) };
}

/// The grammar state the device choice kernel keeps: the window for the
/// next step followed by the token history it was derived from, in the
/// shape `ruleWindow` sees it. `advanceGrammar` is the host mirror of the
/// kernel's update.
pub fn grammarState(rules: Rules, generated: []const i32, vocab: usize, rules_active: bool) ops.WhisperGrammarState {
    var state: ops.WhisperGrammarState = [_]u32{0} ** 8;
    if (!rules_active) {
        state[0] = 1;
        state[1] = @intCast(vocab);
        state[2] = @intCast(vocab);
        return state;
    }
    const window = ruleWindow(rules, generated, vocab);
    state[0] = @intFromBool(window.text_allowed);
    state[1] = @intCast(window.ts_min);
    state[2] = @intCast(window.ts_max);
    // With fewer than two tokens the penultimate counts as a timestamp, so
    // an empty history reads as "last was a timestamp" for the next update.
    state[3] = @intFromBool(generated.len == 0 or rules.isTimestamp(generated[generated.len - 1]));
    state[4] = @intFromBool(generated.len < 2 or rules.isTimestamp(generated[generated.len - 2]));
    var i = generated.len;
    while (i > 0) {
        i -= 1;
        if (rules.isTimestamp(generated[i])) {
            state[5] = 1;
            state[6] = @intCast(generated[i]);
            break;
        }
    }
    if (generated.len > 0) state[7] = @intCast(generated[generated.len - 1]);
    return state;
}

/// Fold `token` into the grammar state the way the device kernel does.
pub fn advanceGrammar(state: *ops.WhisperGrammarState, token: u32, ts_begin: u32, vocab: u32) void {
    const is_ts = token >= ts_begin;
    const penult_is_ts = state[3];
    const last_is_ts: u32 = @intFromBool(is_ts);
    var has_last_ts = state[5];
    var last_ts = state[6];
    if (is_ts) {
        has_last_ts = 1;
        last_ts = token;
    }
    var text_allowed: u32 = 1;
    var ts_min: u32 = ts_begin;
    var ts_max: u32 = vocab;
    if (last_is_ts != 0) {
        if (penult_is_ts != 0) ts_max = ts_min else text_allowed = 0;
    }
    if (has_last_ts != 0) {
        const first_allowed = if (last_is_ts != 0 and penult_is_ts == 0) last_ts else last_ts + 1;
        ts_min = @max(ts_min, @min(first_allowed, vocab));
    }
    if (ts_max < ts_min) ts_max = ts_min;
    state.* = .{ text_allowed, ts_min, ts_max, last_is_ts, penult_is_ts, has_last_ts, last_ts, token };
}

/// Probability of the probed token under the raw (unconstrained) logits.
pub fn statsProbeProbability(stats: *const ops.WhisperLogitsStatsRaw) f32 {
    const lse_raw = statsLogSumExp(stats, stats_raw_max, stats_raw_sum);
    if (lse_raw == neg_inf) return 0;
    return @exp(stats[stats_probe] - lse_raw);
}

/// Reference implementation of the device statistics kernel, used to check
/// its contract against `applyRules` and as documentation of the layout.
pub fn hostLogitsStats(logits: []const f32, params: ops.WhisperLogitsParams, suppress: []const i32) ops.WhisperLogitsStatsRaw {
    var out: ops.WhisperLogitsStatsRaw = [_]f32{0} ** 16;
    var best: ?u32 = null;
    var text: ?u32 = null;
    var ts: ?u32 = null;
    var raw: ?u32 = null;
    var all_m: f32 = neg_inf;
    var all_s: f64 = 0;
    var ts_m: f32 = neg_inf;
    var ts_s: f64 = 0;
    var raw_m: f32 = neg_inf;
    var raw_s: f64 = 0;
    const n = @min(logits.len, params.out_dim);
    for (logits[0..n], 0..) |v, idx| {
        const i: u32 = @intCast(idx);
        if (raw == null or v > logits[raw.?]) raw = i;
        pushLse(&raw_m, &raw_s, v);
        const is_ts = i >= params.ts_begin;
        var allowed = i == params.eot or (if (is_ts) (i >= params.ts_min and i < params.ts_max) else params.text_allowed != 0);
        if (allowed) for (suppress) |t| {
            if (t >= 0 and @as(u32, @intCast(t)) == i) allowed = false;
        };
        if (!allowed) continue;
        if (best == null or v > logits[best.?]) best = i;
        pushLse(&all_m, &all_s, v);
        if (is_ts) {
            if (ts == null or v > logits[ts.?]) ts = i;
            pushLse(&ts_m, &ts_s, v);
        } else if (text == null or v > logits[text.?]) text = i;
    }
    putBest(&out, stats_best_value, logits, best);
    putBest(&out, stats_text_value, logits, text);
    putBest(&out, stats_ts_value, logits, ts);
    putBest(&out, stats_raw_value, logits, raw);
    out[stats_all_max] = all_m;
    out[stats_all_sum] = @floatCast(all_s);
    out[stats_ts_max] = ts_m;
    out[stats_ts_sum] = @floatCast(ts_s);
    out[stats_raw_max] = raw_m;
    out[stats_raw_sum] = @floatCast(raw_s);
    out[stats_probe] = if (params.probe_id < n) logits[params.probe_id] else 0;
    out[stats_eot] = if (params.eot < n) logits[params.eot] else neg_inf;
    return out;
}

fn pushLse(m: *f32, s: *f64, v: f32) void {
    if (v > m.*) {
        s.* = s.* * @exp(@as(f64, m.* - v)) + 1;
        m.* = v;
    } else {
        s.* += @exp(@as(f64, v - m.*));
    }
}

fn putBest(out: *ops.WhisperLogitsStatsRaw, value_index: usize, logits: []const f32, id: ?u32) void {
    out[value_index] = if (id) |i| logits[i] else neg_inf;
    out[value_index + 1] = @bitCast(id orelse no_candidate);
}

fn suppressRange(logits: []f32, start: usize, end: usize, keep: usize) void {
    var i = start;
    while (i < end) : (i += 1) {
        if (i == keep) continue;
        logits[i] = neg_inf;
    }
}

pub fn suppressTokens(logits: []f32, tokens: []const i32) void {
    for (tokens) |token| {
        if (token < 0) continue;
        const idx: usize = @intCast(token);
        if (idx < logits.len) logits[idx] = neg_inf;
    }
}

pub const TokenSegment = struct {
    /// Token index range `[start, end)` into the generated token slice,
    /// covering text tokens only.
    token_start: usize,
    token_end: usize,
    start_ms: u64,
    end_ms: u64,
};

/// Split generated tokens into timestamped phrase segments. Text without a
/// closing timestamp (the model hit EOT or the length cap) is closed at
/// `window_ms`. Tokens before the first timestamp are attached to a segment
/// starting at zero.
pub fn parseSegments(
    allocator: std.mem.Allocator,
    rules: Rules,
    tokens: []const i32,
    window_ms: u64,
) ![]TokenSegment {
    var out = std.ArrayListUnmanaged(TokenSegment).empty;
    errdefer out.deinit(allocator);
    var open: ?TokenSegment = null;
    for (tokens, 0..) |token, index| {
        if (token == rules.eot) break;
        if (rules.isTimestamp(token)) {
            const ms = @as(u64, @intCast(token - rules.timestamp_begin)) * step_ms;
            if (open) |*segment| {
                if (segment.token_end > segment.token_start) {
                    segment.end_ms = @max(ms, segment.start_ms);
                    try out.append(allocator, segment.*);
                }
                open = null;
            }
            open = .{ .token_start = index + 1, .token_end = index + 1, .start_ms = ms, .end_ms = ms };
            continue;
        }
        if (open == null) open = .{ .token_start = index, .token_end = index, .start_ms = 0, .end_ms = 0 };
        open.?.token_end = index + 1;
    }
    if (open) |segment| if (segment.token_end > segment.token_start) {
        var closed = segment;
        closed.end_ms = @max(window_ms, closed.start_ms);
        try out.append(allocator, closed);
    };
    return out.toOwnedSlice(allocator);
}

pub const Word = struct {
    word: []const u8,
    start_ms: u64,
    end_ms: u64,
};

/// Estimate word spans inside a timed phrase by distributing its duration in
/// proportion to word length. Whisper only times phrases; exact word timing
/// needs cross-attention alignment, which the fused attention op does not
/// expose. `words` borrow from `text`.
pub fn splitWords(
    allocator: std.mem.Allocator,
    text: []const u8,
    start_ms: u64,
    end_ms: u64,
) ![]Word {
    var out = std.ArrayListUnmanaged(Word).empty;
    errdefer out.deinit(allocator);
    var total_weight: usize = 0;
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (it.next()) |word| total_weight += word.len;
    if (total_weight == 0) return out.toOwnedSlice(allocator);
    const span = end_ms -| start_ms;
    var consumed: usize = 0;
    var cursor_ms = start_ms;
    it = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (it.next()) |word| {
        consumed += word.len;
        const word_end = start_ms + (span * consumed) / total_weight;
        try out.append(allocator, .{ .word = word, .start_ms = cursor_ms, .end_ms = @max(word_end, cursor_ms) });
        cursor_ms = word_end;
    }
    return out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const test_rules = Rules{ .timestamp_begin = 100, .eot = 3, .no_timestamps = 99, .max_initial_timestamp_index = 5 };

fn logitsWith(values: []f32, fill: f32) void {
    @memset(values, fill);
}

test "timestamp rules force an initial timestamp within the cap" {
    var logits: [120]f32 = undefined;
    logitsWith(&logits, 0);
    applyRules(test_rules, &logits, &.{});
    try std.testing.expectEqual(neg_inf, logits[10]);
    try std.testing.expectEqual(neg_inf, logits[99]);
    try std.testing.expectEqual(@as(f32, 0), logits[100]);
    try std.testing.expectEqual(@as(f32, 0), logits[105]);
    try std.testing.expectEqual(neg_inf, logits[106]);
    try std.testing.expectEqual(@as(f32, 0), logits[3]); // EOT stays available
}

test "timestamp rules alternate text and closing stamps and never go backwards" {
    var logits: [120]f32 = undefined;
    // Opening stamp alone: text must follow.
    logitsWith(&logits, 0);
    logits[7] = 8;
    applyRules(test_rules, &logits, &.{102});
    try std.testing.expectEqual(@as(f32, 8), logits[7]);
    try std.testing.expectEqual(neg_inf, logits[102]);
    try std.testing.expectEqual(neg_inf, logits[110]);
    // `<|102|> text`: text may continue; a stamp must be later than 102.
    // (A confident text token keeps the timestamp-mass rule from firing.)
    logitsWith(&logits, 0);
    logits[7] = 8;
    applyRules(test_rules, &logits, &.{ 102, 7 });
    try std.testing.expectEqual(@as(f32, 8), logits[7]);
    try std.testing.expectEqual(neg_inf, logits[102]);
    try std.testing.expectEqual(@as(f32, 0), logits[103]);
    // `<|102|> text <|104|>`: the pair must be closed by another stamp.
    logitsWith(&logits, 0);
    applyRules(test_rules, &logits, &.{ 102, 7, 104 });
    try std.testing.expectEqual(neg_inf, logits[7]);
    try std.testing.expectEqual(@as(f32, 0), logits[3]); // EOT may end the window
    try std.testing.expectEqual(@as(f32, 0), logits[104]);
    // Closed pair `<|102|> text <|104|><|104|>`: text must follow.
    logitsWith(&logits, 0);
    applyRules(test_rules, &logits, &.{ 102, 7, 104, 104 });
    try std.testing.expectEqual(@as(f32, 0), logits[7]);
    try std.testing.expectEqual(neg_inf, logits[104]);
    try std.testing.expectEqual(neg_inf, logits[110]);
}

test "timestamp mass overrides a weak text token" {
    var logits: [120]f32 = undefined;
    logitsWith(&logits, -20);
    // Inside an open phrase text is allowed, but timestamps collectively dominate.
    logits[7] = 1.0;
    for (100..120) |i| logits[i] = 0.9;
    applyRules(test_rules, &logits, &.{ 102, 7 });
    try std.testing.expectEqual(neg_inf, logits[7]);
    try std.testing.expectEqual(@as(f32, 0.9), logits[110]);
}

test "parse segments pairs timestamps with text and closes an open tail" {
    const allocator = std.testing.allocator;
    // <|0.00|> a b <|1.00|> <|1.00|> c <|EOT|>
    const tokens = [_]i32{ 100, 7, 8, 150, 150, 9, 3, 11 };
    const segments = try parseSegments(allocator, test_rules, &tokens, 30_000);
    defer allocator.free(segments);
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    try std.testing.expectEqual(@as(usize, 1), segments[0].token_start);
    try std.testing.expectEqual(@as(usize, 3), segments[0].token_end);
    try std.testing.expectEqual(@as(u64, 0), segments[0].start_ms);
    try std.testing.expectEqual(@as(u64, 1000), segments[0].end_ms);
    try std.testing.expectEqual(@as(u64, 1000), segments[1].start_ms);
    try std.testing.expectEqual(@as(u64, 30_000), segments[1].end_ms);
    try std.testing.expectEqual(@as(usize, 5), segments[1].token_start);
    try std.testing.expectEqual(@as(usize, 6), segments[1].token_end);

    // Text before any timestamp and empty pairs.
    const bare = [_]i32{ 7, 8, 120, 120, 121 };
    const bare_segments = try parseSegments(allocator, test_rules, &bare, 5_000);
    defer allocator.free(bare_segments);
    try std.testing.expectEqual(@as(usize, 1), bare_segments.len);
    try std.testing.expectEqual(@as(u64, 0), bare_segments[0].start_ms);
    try std.testing.expectEqual(@as(u64, 400), bare_segments[0].end_ms);
}

test "split words distributes a phrase by word length" {
    const allocator = std.testing.allocator;
    const words = try splitWords(allocator, "  the quick fox ", 1000, 2000);
    defer allocator.free(words);
    try std.testing.expectEqual(@as(usize, 3), words.len);
    try std.testing.expectEqualStrings("the", words[0].word);
    try std.testing.expectEqual(@as(u64, 1000), words[0].start_ms);
    // Weights 3, 5, 3 of 11 over a 1000 ms phrase.
    try std.testing.expectEqual(@as(u64, 1272), words[0].end_ms);
    try std.testing.expectEqual(@as(u64, 1272), words[1].start_ms);
    try std.testing.expectEqual(@as(u64, 1727), words[1].end_ms);
    try std.testing.expectEqual(@as(u64, 2000), words[2].end_ms);
    const none = try splitWords(allocator, "   ", 0, 10);
    defer allocator.free(none);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

test "device token choice matches applyRules and argmax" {
    const vocab: usize = 64;
    const rules = Rules{ .timestamp_begin = 48, .eot = 47, .no_timestamps = 46, .max_initial_timestamp_index = 6 };
    const suppress_base = [_]i32{ 3, 9 };
    var prng = std.Random.DefaultPrng.init(0x5151);
    const random = prng.random();
    const histories = [_][]const i32{
        &.{},
        &.{50},
        &.{ 50, 12 },
        &.{ 50, 12, 52 },
        &.{ 50, 12, 52, 53 },
        &.{ 50, 12, 52, 53, 7, 8 },
    };
    var trial: usize = 0;
    while (trial < 40) : (trial += 1) {
        var logits: [64]f32 = undefined;
        for (&logits) |*v| v.* = random.float(f32) * 12 - 6;
        // Occasionally make timestamps dominate so the mass rule fires.
        if (trial % 3 == 0) for (logits[48..]) |*v| {
            v.* += 5;
        };
        for (histories) |generated| {
            // Reference: host suppression, rules, argmax, log-probability.
            var scored = logits;
            var suppress = std.ArrayListUnmanaged(i32).empty;
            defer suppress.deinit(std.testing.allocator);
            try suppress.appendSlice(std.testing.allocator, &suppress_base);
            try suppress.append(std.testing.allocator, rules.no_timestamps);
            suppressTokens(&scored, suppress.items);
            applyRules(rules, &scored, generated);
            var ref_best: usize = 0;
            var ref_val: f32 = neg_inf;
            for (scored, 0..) |v, i| if (v > ref_val) {
                ref_val = v;
                ref_best = i;
            };
            var ref_lse_max: f32 = neg_inf;
            for (scored) |v| if (v > ref_lse_max) {
                ref_lse_max = v;
            };
            var ref_sum: f64 = 0;
            for (scored) |v| if (v != neg_inf) {
                ref_sum += @exp(@as(f64, v - ref_lse_max));
            };
            const ref_logprob = @as(f64, scored[ref_best]) - (@as(f64, ref_lse_max) + @log(ref_sum));

            const window = ruleWindow(rules, generated, vocab);
            const params = ops.WhisperLogitsParams{
                .out_dim = @intCast(vocab),
                .suppress_count = @intCast(suppress.items.len),
                .ts_begin = @intCast(rules.timestamp_begin),
                .text_allowed = @intFromBool(window.text_allowed),
                .ts_min = @intCast(window.ts_min),
                .ts_max = @intCast(window.ts_max),
                .eot = @intCast(rules.eot),
                .probe_id = 45,
            };
            const stats = hostLogitsStats(&logits, params, suppress.items);
            const choice = chooseFromStats(&stats, true, @intCast(rules.eot), true).?;
            try std.testing.expectEqual(ref_best, @as(usize, choice.token));
            try std.testing.expectApproxEqAbs(ref_logprob, choice.logprob, 1e-3);
            // Raw statistics ignore every constraint.
            var raw_best: usize = 0;
            for (logits, 0..) |v, i| if (v > logits[raw_best]) {
                raw_best = i;
            };
            try std.testing.expectEqual(@as(u32, @intCast(raw_best)), statsId(&stats, stats_raw_id).?);
            var raw_max: f32 = neg_inf;
            for (logits) |v| if (v > raw_max) {
                raw_max = v;
            };
            var raw_sum: f64 = 0;
            for (logits) |v| raw_sum += @exp(@as(f64, v - raw_max));
            const expected_probe = @exp(@as(f64, logits[45]) - (@as(f64, raw_max) + @log(raw_sum)));
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected_probe)), statsProbeProbability(&stats), 1e-4);
        }
    }
}

test "rule window with timestamps disabled allows everything" {
    const rules = Rules{ .timestamp_begin = 1000, .eot = 47, .no_timestamps = 46 };
    const window = ruleWindow(rules, &.{ 1, 2 }, 64);
    try std.testing.expect(window.text_allowed);
    try std.testing.expectEqual(@as(usize, 64), window.ts_min);
    try std.testing.expectEqual(@as(usize, 64), window.ts_max);
}

test "device grammar state follows ruleWindow token by token" {
    const rules = Rules{ .timestamp_begin = 48, .eot = 47, .no_timestamps = 46, .max_initial_timestamp_index = 6 };
    const vocab: usize = 64;
    const sequences = [_][]const i32{
        &.{ 48, 5, 6, 50, 50, 7, 52 },
        &.{ 49, 1, 2, 3, 51, 51, 51, 55, 4, 55 },
        &.{ 5, 6, 7 },
        &.{ 48, 48, 49, 49 },
    };
    for (sequences) |seq| {
        // Seed from every prefix length, then let the mirror advance.
        for (0..seq.len) |seed_len| {
            var state = grammarState(rules, seq[0..seed_len], vocab, true);
            var len = seed_len;
            while (len < seq.len) : (len += 1) {
                const expected = ruleWindow(rules, seq[0..len], vocab);
                try std.testing.expectEqual(@intFromBool(expected.text_allowed), state[0]);
                try std.testing.expectEqual(@as(u32, @intCast(expected.ts_min)), state[1]);
                try std.testing.expectEqual(@as(u32, @intCast(expected.ts_max)), state[2]);
                advanceGrammar(&state, @intCast(seq[len]), 48, @intCast(vocab));
            }
            const final = ruleWindow(rules, seq, vocab);
            try std.testing.expectEqual(@intFromBool(final.text_allowed), state[0]);
            try std.testing.expectEqual(@as(u32, @intCast(final.ts_min)), state[1]);
            try std.testing.expectEqual(@as(u32, @intCast(final.ts_max)), state[2]);
        }
    }
    // Rules off: everything but timestamps, which do not exist.
    const off = grammarState(rules, &.{ 1, 2 }, vocab, false);
    try std.testing.expectEqual(@as(u32, 1), off[0]);
    try std.testing.expectEqual(@as(u32, 64), off[1]);
    try std.testing.expectEqual(@as(u32, 64), off[2]);
}
