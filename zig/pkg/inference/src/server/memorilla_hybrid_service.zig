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

//! Hybrid research memory (docs/plans/memorilla-copy17-native.md sections
//! 8.4-8.7): the released reader's 16 soft rows (general recall) composed
//! with the copy17 candidate's 16-row exact record, read by the frozen
//! decoder, over ONE resident encoder. Production composition of
//! `scripts/memorilla/qwen_hybrid_eval.py --record-gate --answer-canonicalize`
//! (merged prompt, soft rows first, full record rows):
//!
//! 1. soft half: the stock `qwen3_encoder_f32_v1` encoder pools every document
//!    and the question (last token, L2, FP16 -> BF16 channel), and the
//!    released reader (`released_memory_module_path`, e.g. stage3) turns them
//!    into 16 rows;
//! 2. record half: the same encoder, with the hybrid bundle's encoder LoRA
//!    applied to its q/v projections at run time, produces the per-token
//!    `tokens-dual` features; the research reader, copy head, contiguous
//!    runs, snapping and word completion produce the 16 record ids, their
//!    spans and the source map;
//! 3. record gate (plan 7.4): open when one copied span snapped every slot at
//!    cosine >= 0.85. Open: system = helpful prompt + " " + the candidate's
//!    exact-answer prompt, 32 rows = soft then record embedding rows. Closed:
//!    the helpful prompt with the 16 soft rows alone;
//! 4. greedy decode, then (gate open) answer canonicalization against the
//!    record text, the frame-echo answer guard and citations of the record's
//!    copied spans evaluated on the published answer.
//!
//! Same lease, budget and fence discipline as `memorilla_research_service`.
//! Nothing here is a qualified release.
const std = @import("std");
const platform = @import("antfly_platform");
const tok = @import("inference_tokenizer");
const common = @import("memorilla_service.zig");
const research_service = @import("memorilla_research_service.zig");
const manager_mod = @import("model_manager.zig");
const selection = @import("../registry/model_selection.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const encoder_lora = @import("../models/memorilla_encoder_lora.zig");
const encoder = @import("../pipelines/memorilla_encoder.zig");
const memory_pipeline = @import("../pipelines/memorilla.zig");
const token_features = @import("../pipelines/memorilla_token_features.zig");
const copy = @import("../pipelines/memorilla_copy.zig");
const span_window = @import("../pipelines/memorilla_span.zig");
const gpt = @import("../architectures/gpt.zig");
const c_file = @import("../util/c_file.zig");
const grounding = @import("../pipelines/memorilla_grounding.zig");
const decode = @import("../pipelines/memorilla_generation.zig");
const generation = @import("../pipelines/generation.zig");
const factory = @import("../architectures/session_factory.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const execution = @import("../ops/execution_scope.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const PhysicalBudgetScope = if (@import("build_options").enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute.MemorillaDeviceBudgetScope else struct {
    fn deinit(_: *@This()) void {}
};
const A = std.mem.Allocator;

pub const Profile = common.Profile;
/// `encoder` is the stock managed encoder (both passes), `decoder` the
/// managed decoder.
pub const Paths = common.Paths;
pub const Options = common.Options;
pub const Stage = common.Stage;
pub const encoder_profile: qualified.Profile = .qwen3_encoder_f32_v1;
pub const decoder_profile: qualified.Profile = .qwen3_decoder_f32_kv_f32_v1;

/// Rows per reader (the decoder's trained memory frame).
pub const slots: usize = 16;
/// Gate open: soft rows then record rows.
pub const hybrid_rows: usize = 2 * slots;
/// The released reader's prompt (upstream `HELPFUL_PROMPT`).
pub const helpful_prompt: []const u8 = "You are a helpful assistant.";
/// Plan 7.4: a copied span is trusted when every slot snapped at least this.
pub const record_gate_cosine: f32 = 0.85;
/// `canonicalize_answer`'s minimum squashed tail length.
pub const tail_min_chars: usize = 3;
/// Pooled vectors of documents longer than the stock encoder's pooled limit
/// come from sentence-greedy chunks of at most this many tokens (EOS
/// included).
pub const chunk_max_tokens: usize = 128;
/// Upper bound on the per-token record batch (rows x padded length). The
/// evaluator has none; depot cases use at most 219 positions and squad at
/// most 1525. This keeps one request's encoder activations bounded.
pub const max_record_positions: usize = 16384;

pub const RecordGate = enum { open, closed };
pub const AnswerGuard = research_service.AnswerGuard;
pub const Citation = copy.Citation;
pub const Citations = copy.Citations;
pub const unknown_answer = research_service.unknown_answer;

pub const OwnedResult = struct {
    /// Owns `record_text` and `raw_text`.
    allocator: A,
    generation: generation.GenerationResult,
    answer_guard: AnswerGuard,
    record_gate: RecordGate,
    /// The published text was re-spelled from the record.
    canonicalized: bool,
    /// Rows the decoder read: 32 (gate open) or 16 (closed).
    memory_tokens: usize,
    encoder_execution_identity: [64]u8,
    decoder_execution_identity: [64]u8,
    /// Research reader weights (record half).
    module_sha256: [64]u8,
    /// Released reader weights (soft half).
    soft_module_sha256: [64]u8,
    candidate_sha256: [64]u8,
    /// Run-time encoder LoRA applied to the record pass.
    adapter_sha256: [64]u8,
    document_count: usize,
    /// Pooled vectors read by the soft half (documents, or their chunks).
    soft_rows: usize,
    question_tokens: usize,
    /// Copied spans of the record with their document byte ranges; empty
    /// when the gate is closed.
    citations: copy.Citations,
    /// Diagnostics: the decoded record (`hybrid_record_text`), the trimmed
    /// decoder answer before canonicalization (`answer_raw`) and the smallest
    /// snap cosine over the record slots.
    record_text: []u8,
    raw_text: []u8,
    min_snap_cosine: f32,
    /// Span-window bundles: the decoded span, its evidence window and the
    /// gate margin (null for copy17 records and for `span_soft_only`).
    span: ?SpanDiagnostics = null,
    /// Span-window bundles: the predicted span and the evidence window as
    /// regions of the caller's documents (`copy.buildCitations` of each, with
    /// no answer), whether or not the gate opened; null for copy17 records
    /// and for `span_soft_only`. `unavailable` as for `citations`.
    span_regions: ?copy.Citations = null,
    window_regions: ?copy.Citations = null,
    /// Span-window bundles: stage timings (null for copy17 records).
    timings: ?SpanTimings = null,
    /// `span_soft_only`: no span pass ran; the decoder read the soft rows
    /// alone under the released prompt.
    soft_only: bool = false,
    response_lease: memory.AdmissionLease,

    pub fn deinit(self: *OwnedResult) void {
        self.allocator.free(self.record_text);
        self.allocator.free(self.raw_text);
        if (self.span) |diagnostics| {
            if (diagnostics.gate_profile) |name| self.allocator.free(name);
        }
        if (self.span_regions) |*regions| regions.deinit();
        if (self.window_regions) |*regions| regions.deinit();
        self.generation.deinit();
        self.citations.deinit();
        self.response_lease.release();
        self.* = undefined;
    }
};

/// Span-window record diagnostics (`qwen_span_copy.span_record_memory`).
pub const SpanDiagnostics = struct {
    /// Inclusive span in the concatenated copy ids.
    start: usize,
    end: usize,
    /// Window `[lo, hi)` in the copy ids (the record rows before padding).
    window_lo: usize,
    window_hi: usize,
    score: f64,
    null_score: f64,
    /// `score - null_score - threshold`; the gate opens at `margin >=
    /// gate_offset`.
    margin: f64,
    gate_policy: span_window.GatePolicy,
    /// The offset the margin was compared with (-inf for `always`).
    gate_offset: f64 = 0,
    /// Routed or declared profile name (owned by the result), null for
    /// `always` and `learned` without a declared profile.
    gate_profile: ?[]u8 = null,
    /// Cosine of the question to the routed centroid (routed only).
    gate_similarity: f32 = 0,
    /// The routed question fell below its profile's floor and took the
    /// `learned` offset.
    gate_fallback: bool = false,
    /// Span-head load and forward time (every pass the request ran).
    head_ms: f64,
    /// Read mode of a cascade bundle (plan 8.32), null without a cascade.
    read_mode: ?span_window.ReadMode = null,
    /// The pass whose record the request used (null without a cascade).
    read_pass: ?span_window.ReadPass = null,
    /// The cascade's widening threshold (null without a cascade).
    cascade_threshold: ?f64 = null,
    /// Margins of the narrow and wide passes that ran (null: not run).
    narrow_margin: ?f64 = null,
    wide_margin: ?f64 = null,
    /// Why a cascade read set was skipped (its preflight error name; the
    /// request kept or took the other read), null when both were usable.
    narrow_unavailable: ?[]const u8 = null,
    wide_unavailable: ?[]const u8 = null,
    /// Leading candidate documents read (the span pass, citations and copy
    /// sources cover exactly these).
    read_documents: usize = 0,
    /// Copy ids of the read set (`read_tokens` of the evaluator's records).
    read_tokens: usize = 0,
    /// Encoder tokens the read budget summed (0 without a budget).
    read_budget_tokens: usize = 0,
    /// Evidence window rows (`span_window_tokens` or the cascade's).
    window_rows: usize = 0,
};

/// Stage timings of a span-bundle request in milliseconds: the
/// `memorilla_span_timing` stages (each measured from the previous mark, so
/// a stage also carries the model-handle and verification work done since).
/// `total_ms` covers the whole service call (`generate`), including
/// preflight, admission and the stages not listed.
pub const SpanTimings = struct {
    /// Pooled (stock encoder) document and question vectors.
    pooled_ms: f64 = 0,
    /// The first span pass's per-token encoder features (with LoRA).
    span_features_ms: f64 = 0,
    /// The first pass's span head (after the decoder handle is acquired).
    span_head_ms: f64 = 0,
    /// The cascade's wide pass, when it ran.
    wide_features_ms: f64 = 0,
    wide_head_ms: f64 = 0,
    /// Read plan, evidence window, path checks and the window's embedding rows.
    window_ms: f64 = 0,
    /// The released reader's 16 soft rows.
    soft_rows_ms: f64 = 0,
    /// Prefill and greedy decoding.
    generate_ms: f64 = 0,
    total_ms: f64 = 0,

    fn add(self: *SpanTimings, stage: []const u8, ms: f64) void {
        const named = .{
            .{ "pooled", "pooled_ms" },
            .{ "span_features", "span_features_ms" },
            .{ "span_head", "span_head_ms" },
            .{ "wide_features", "wide_features_ms" },
            .{ "wide_head", "wide_head_ms" },
            .{ "soft_rows", "soft_rows_ms" },
            .{ "generate", "generate_ms" },
        };
        inline for (named) |entry| {
            if (std.mem.eql(u8, stage, entry[0])) {
                @field(self, entry[1]) += ms;
                return;
            }
        }
        self.window_ms += ms;
    }
};

// --- policy helpers (pure, unit tested) ---------------------------------------

/// `qwen_hybrid_eval.system_prompt_text("merged")`: helpful then exact.
pub fn mergedSystemPrompt(a: A, exact_prompt: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ helpful_prompt, " ", exact_prompt });
}

/// `record_gate_open`: true when at least one copied span (record slots
/// `begin..end` before word completion) has every slot's top-1 snap cosine
/// at or above `threshold`. The evaluator compares the cosines it recorded,
/// rounded to four decimals; so does this.
pub fn recordGateOpen(spans: []const copy.Span, cosines: []const f32, threshold: f32) !bool {
    if (!(threshold >= 0 and threshold <= 1)) return error.InvalidRecordGate;
    for (spans) |span| {
        if (!(span.begin < span.end and span.end <= cosines.len)) return error.InvalidCopySpan;
        var lowest: f32 = std.math.inf(f32);
        for (cosines[span.begin..span.end]) |cosine| lowest = @min(lowest, round4(cosine));
        if (lowest >= threshold) return true;
    }
    return false;
}

fn round4(value: f32) f32 {
    const scaled: f64 = @as(f64, value) * 10000.0;
    return @floatCast(@round(scaled) / 10000.0);
}

/// Python `str.strip()` (Unicode whitespace), as the evaluator's `generate`
/// applies to the decoded answer. Invalid UTF-8 is returned unstripped.
pub fn pythonStrip(text: []const u8) []const u8 {
    const view = std.unicode.Utf8View.init(text) catch return text;
    var it = view.iterator();
    var offset: usize = 0;
    var first: ?usize = null;
    var last: usize = 0;
    while (it.nextCodepoint()) |cp| {
        const n = std.unicode.utf8CodepointSequenceLength(cp) catch unreachable;
        if (!qualified.pythonWhitespace(cp)) {
            if (first == null) first = offset;
            last = offset + n;
        }
        offset += n;
    }
    return if (first) |start| text[start..last] else text[text.len..];
}

fn isRecordWordByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '-';
}

/// `squash`: lower case (ASCII), without hyphens or whitespace.
fn squash(a: A, text: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    const view = std.unicode.Utf8View.init(text) catch {
        for (text) |byte| if (byte != '-' and !std.ascii.isWhitespace(byte)) try out.append(a, std.ascii.toLower(byte));
        return out.toOwnedSlice(a);
    };
    var it = view.iterator();
    while (it.nextCodepointSlice()) |piece| {
        const cp = std.unicode.utf8Decode(piece) catch unreachable;
        if (cp == '-' or qualified.pythonWhitespace(cp)) continue;
        if (piece.len == 1) try out.append(a, std.ascii.toLower(piece[0])) else try out.appendSlice(a, piece);
    }
    return out.toOwnedSlice(a);
}

fn codepoints(text: []const u8) usize {
    return std.unicode.utf8CountCodepoints(text) catch text.len;
}

/// `canonicalize_answer` (`qwen_hybrid_eval.py`, TAIL_MIN_CHARS = 3): re-spell a
/// decoded answer from the record it was read from.
///
/// (a) An answer occurring in `record_text` case-insensitively and aligned to
/// record word boundaries (letters, digits, `-`) takes the record's casing.
/// (b) Otherwise an answer that is the tail of one to three consecutive record
/// words (`[A-Za-z0-9][A-Za-z0-9-]*`, hyphens and whitespace ignored) becomes
/// that word sequence joined by single spaces; the squashed tail needs at
/// least three characters, and a tail starting inside a word is accepted only
/// for an answer carrying a digit. UNKNOWN and empty answers pass through.
/// The result is the stripped answer when nothing applies. Case folding is
/// ASCII-only (the evaluator uses Unicode `str.lower`).
pub fn canonicalizeAnswer(a: A, answer: []const u8, record_text: []const u8) ![]u8 {
    const raw = pythonStrip(answer);
    if (raw.len == 0 or std.mem.eql(u8, raw, unknown_answer)) return a.dupe(u8, raw);
    if (raw.len <= record_text.len) {
        var start: usize = 0;
        while (start + raw.len <= record_text.len) : (start += 1) {
            if (!std.ascii.eqlIgnoreCase(record_text[start..][0..raw.len], raw)) continue;
            const end = start + raw.len;
            const before_ok = start == 0 or !isRecordWordByte(record_text[start - 1]);
            const after_ok = end == record_text.len or !isRecordWordByte(record_text[end]);
            if (before_ok and after_ok) return a.dupe(u8, record_text[start..end]);
        }
    }
    const target = try squash(a, raw);
    defer a.free(target);
    if (codepoints(target) < tail_min_chars) return a.dupe(u8, raw);
    var words: std.ArrayListUnmanaged([]const u8) = .empty;
    defer words.deinit(a);
    var i: usize = 0;
    while (i < record_text.len) {
        if (!std.ascii.isAlphanumeric(record_text[i])) {
            i += 1;
            continue;
        }
        var j = i + 1;
        while (j < record_text.len and isRecordWordByte(record_text[j])) j += 1;
        try words.append(a, record_text[i..j]);
        i = j;
    }
    var has_digit = false;
    for (raw) |byte| has_digit = has_digit or std.ascii.isDigit(byte);
    var joined: std.ArrayListUnmanaged(u8) = .empty;
    defer joined.deinit(a);
    for ([_]usize{ 1, 2, 3 }) |size| {
        if (words.items.len < size) break;
        for (0..words.items.len - size + 1) |index| {
            const sequence = words.items[index..][0..size];
            // Squashed words concatenate: word k starts at the summed lengths
            // of the squashed words before it.
            joined.clearRetainingCapacity();
            var starts: [3]usize = undefined;
            for (sequence, 0..) |word, k| {
                starts[k] = joined.items.len;
                for (word) |byte| if (byte != '-') try joined.append(a, std.ascii.toLower(byte));
            }
            if (!std.mem.endsWith(u8, joined.items, target)) continue;
            const tail_start = joined.items.len - target.len;
            const aligned = std.mem.indexOfScalar(usize, starts[0..size], tail_start) != null;
            if (has_digit or aligned) return std.mem.join(a, " ", sequence);
        }
    }
    return a.dupe(u8, raw);
}

/// Hybrid answer guard. General answers can be sentences, so only the
/// frame-echo rules of the research guard (plan 7.1) apply, to the
/// whitespace-trimmed text: the literal UNKNOWN is accepted; empty text, any
/// control character (including newline and tab), the `<|memory|>` marker or
/// the frame literal `Memory` as a standalone word is rejected. There is no
/// length, word-count or character-set limit.
pub fn guardAnswer(text: []const u8) AnswerGuard {
    const trimmed = std.mem.trim(u8, text, &std.ascii.whitespace);
    if (std.mem.eql(u8, trimmed, unknown_answer)) return .accepted;
    if (trimmed.len == 0) return .rejected;
    if (std.mem.indexOf(u8, trimmed, decode.marker) != null) return .rejected;
    for (trimmed) |byte| if (std.ascii.isControl(byte)) return .rejected;
    var it = std.mem.tokenizeAny(u8, trimmed, &std.ascii.whitespace);
    while (it.next()) |word| if (std.mem.eql(u8, word, "Memory")) return .rejected;
    return .accepted;
}

/// Appends `piece` decoded leniently: valid UTF-8 is copied, an invalid or
/// truncated sequence becomes one U+FFFD (Python `bytes.decode(errors="replace")`
/// on a token piece).
fn appendLossy(a: A, out: *std.ArrayListUnmanaged(u8), piece: []const u8) !void {
    var i: usize = 0;
    while (i < piece.len) {
        const length = std.unicode.utf8ByteSequenceLength(piece[i]) catch {
            try out.appendSlice(a, "\u{fffd}");
            i += 1;
            continue;
        };
        var continued: usize = 0;
        while (continued + 1 < length and i + 1 + continued < piece.len and piece[i + 1 + continued] & 0xc0 == 0x80) continued += 1;
        if (continued + 1 == length and std.unicode.utf8ValidateSlice(piece[i..][0..length])) {
            try out.appendSlice(a, piece[i..][0..length]);
        } else {
            try out.appendSlice(a, "\u{fffd}");
        }
        i += 1 + continued;
    }
}

/// `"".join(dec_tok.decode([i]) for i in ids)`: the record text the
/// evaluator canonicalizes against, decoded token by token.
pub fn recordText(a: A, tokenizer: tok.Tokenizer, ids: []const i32) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    for (ids) |id| {
        const piece = try tokenizer.decode(a, &.{id});
        defer a.free(piece);
        try appendLossy(a, &out, piece);
    }
    return out.toOwnedSlice(a);
}

// --- pooled chunking ----------------------------------------------------------

const Range = struct { start: usize, end: usize };

fn tokenCount(a: A, tokenizer: tok.Tokenizer, text: []const u8) !usize {
    const ids = try tokenizer.encode(a, text);
    defer a.free(ids);
    // The pooled pass appends EOS.
    return ids.len + 1;
}

fn trimRange(text: []const u8, range: Range) ?Range {
    var start = range.start;
    var end = range.end;
    while (start < end and std.ascii.isWhitespace(text[start])) start += 1;
    while (end > start and std.ascii.isWhitespace(text[end - 1])) end -= 1;
    return if (start < end) .{ .start = start, .end = end } else null;
}

/// Greedily merges consecutive `units` (each fitting alone) into the longest
/// runs whose covering text fits `max_tokens`.
fn packRanges(a: A, tokenizer: tok.Tokenizer, text: []const u8, units: []const Range, max_tokens: usize, out: *std.ArrayListUnmanaged(Range)) !void {
    var current: ?Range = null;
    for (units) |unit| {
        if (current) |held| {
            const candidate = Range{ .start = held.start, .end = unit.end };
            if (try tokenCount(a, tokenizer, text[candidate.start..candidate.end]) <= max_tokens) {
                current = candidate;
                continue;
            }
            try out.append(a, held);
        }
        current = unit;
    }
    if (current) |held| try out.append(a, held);
}

/// Splits one oversized sentence into word groups, and an oversized word into
/// pieces of at most `max_tokens - 1` bytes on UTF-8 boundaries (a byte-level
/// BPE token covers at least one byte).
fn splitUnit(a: A, tokenizer: tok.Tokenizer, text: []const u8, unit: Range, max_tokens: usize, out: *std.ArrayListUnmanaged(Range)) !void {
    var words: std.ArrayListUnmanaged(Range) = .empty;
    defer words.deinit(a);
    var i = unit.start;
    while (i < unit.end) {
        while (i < unit.end and std.ascii.isWhitespace(text[i])) i += 1;
        if (i == unit.end) break;
        var j = i;
        while (j < unit.end and !std.ascii.isWhitespace(text[j])) j += 1;
        if (try tokenCount(a, tokenizer, text[i..j]) <= max_tokens) {
            try words.append(a, .{ .start = i, .end = j });
        } else {
            var piece_start = i;
            while (piece_start < j) {
                var piece_end = @min(j, piece_start + max_tokens - 1);
                while (piece_end > piece_start + 1 and piece_end < j and text[piece_end] & 0xc0 == 0x80) piece_end -= 1;
                try words.append(a, .{ .start = piece_start, .end = piece_end });
                piece_start = piece_end;
            }
        }
        i = j;
    }
    try packRanges(a, tokenizer, text, words.items, max_tokens, out);
}

/// Sentence-greedy chunks of `text` whose pooled encodings (EOS included) fit
/// `max_tokens`: sentences end after `.`, `!` or `?` followed by whitespace,
/// or at a newline; consecutive sentences are merged while they fit, an
/// oversized sentence is split at whitespace (and an oversized word at
/// UTF-8 boundaries). Chunks are trimmed slices of `text`, in order.
pub fn chunkDocument(a: A, tokenizer: tok.Tokenizer, text: []const u8, max_tokens: usize) ![]Range {
    if (max_tokens < 2) return error.MemorillaTextLimitExceeded;
    var units: std.ArrayListUnmanaged(Range) = .empty;
    defer units.deinit(a);
    var start: usize = 0;
    for (text, 0..) |byte, i| {
        const boundary = byte == '\n' or ((byte == '.' or byte == '!' or byte == '?') and (i + 1 == text.len or std.ascii.isWhitespace(text[i + 1])));
        if (!boundary) continue;
        if (trimRange(text, .{ .start = start, .end = i + 1 })) |sentence| {
            if (try tokenCount(a, tokenizer, text[sentence.start..sentence.end]) <= max_tokens) try units.append(a, sentence) else try splitUnit(a, tokenizer, text, sentence, max_tokens, &units);
        }
        start = i + 1;
    }
    if (trimRange(text, .{ .start = start, .end = text.len })) |sentence| {
        if (try tokenCount(a, tokenizer, text[sentence.start..sentence.end]) <= max_tokens) try units.append(a, sentence) else try splitUnit(a, tokenizer, text, sentence, max_tokens, &units);
    }
    var chunks: std.ArrayListUnmanaged(Range) = .empty;
    errdefer chunks.deinit(a);
    try packRanges(a, tokenizer, text, units.items, max_tokens, &chunks);
    return chunks.toOwnedSlice(a);
}

// --- validation and preflight -------------------------------------------------

/// Pre-admission validation shared with the HTTP handler. Only policy and
/// shape; no model, reader or GPU state is consulted. The identity is the
/// stock encoder's: both passes run on it.
pub fn validateInputs(profile: Profile, question: []const u8, documents: []const []const u8, identity: []const u8, max_new_tokens: usize) !void {
    try (platform.inference_settings.Memorilla{ .enabled = true, .profiles = &.{profile} }).validate();
    if (profile.reader_kind != .hybrid_research or profile.released_memory_module_path == null) return error.UnsupportedMemorillaProfile;
    if (question.len > grounding.max_document_bytes) return error.MemorillaTextLimitExceeded;
    _ = try qualified.canonicalQuestion(question);
    if (!std.mem.eql(u8, identity, &qualified.executionIdentity(encoder_profile))) return error.IncompatibleMemorillaProducer;
    if (max_new_tokens == 0 or max_new_tokens > profile.max_new_tokens) return error.MemorillaGenerationLimitExceeded;
    if (documents.len == 0) return error.EmptyMemorillaCollection;
    if (documents.len > profile.max_documents) return error.MemorillaCollectionBudgetExceeded;
    try grounding.validateSources(documents, documents.len);
}

fn requireLimits(limits: memory.Limits) !void {
    if (limits.host_limit_bytes == 0 or limits.backend_limit_bytes == 0 or limits.combined_limit_bytes == 0 or
        limits.kv_limit_bytes == 0 or limits.scratch_limit_bytes == 0) return error.MemorillaFiniteBudgetRequired;
}

fn notify(options: Options, stage: Stage, manager: *manager_mod.ModelManager) !void {
    if (options.stage) |callback| try callback(options.context, stage, manager);
}

fn add(x: usize, y: usize) !usize {
    return std.math.add(usize, x, y) catch error.MemorillaResourceOverflow;
}

fn mul(x: usize, y: usize) !usize {
    return std.math.mul(usize, x, y) catch error.MemorillaResourceOverflow;
}

/// Elementwise maximum: one admission that covers either decoder plan.
pub fn maxAmounts(x: memory.AdmissionAmounts, y: memory.AdmissionAmounts) memory.AdmissionAmounts {
    var out: memory.AdmissionAmounts = .{};
    inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |name| @field(out, name) = @max(@field(x, name), @field(y, name));
    return out;
}

/// Encoder-side facts computed with the stock encoder's tokenizer before any
/// model loads, so limits fail early and admission is exact.
const Preflight = struct {
    a: A,
    question_tokens: usize,
    /// Pooled texts (slices of the caller's documents): one per document, or
    /// its chunks when it exceeds the pooled limit.
    soft_texts: [][]const u8,
    /// Selected per-token rows of the record pass (`2 * sum(n_i + 1)`).
    record_rows: usize,

    fn deinit(self: *Preflight) void {
        self.a.free(self.soft_texts);
    }
};

fn preflightEncoder(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, canonical: []const u8, documents: []const []const u8, control: Control) !Preflight {
    try control.check();
    var assets = try common.acquireGenerationAssets(a, io, manager, selected, encoder_profile, control);
    defer assets.deinit();
    const tokenizer = assets.tokenizer();
    // The pooled question, exactly as the released preflight counts it.
    const question_tokens = blk: {
        var tokens = try tokenizer.encodeForModel(a, canonical, qualified.max_text_tokens + 1);
        defer tokens.deinit();
        if (tokens.ids.len != tokens.attention_mask.len) return error.InvalidMemorillaTokens;
        var active: usize = 0;
        var last: i32 = -1;
        for (tokens.ids, tokens.attention_mask) |id, used| {
            if (used != 0 and used != 1) return error.InvalidMemorillaTokens;
            if (used == 1) {
                active += 1;
                last = id;
            }
        }
        if (active > qualified.max_text_tokens) return error.MemorillaTextLimitExceeded;
        if (active == 0 or last != token_features.eos_id) return error.InvalidMemorillaTokens;
        break :blk active;
    };
    var soft: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer soft.deinit(a);
    for (documents) |document| {
        try control.check();
        if (try tokenCount(a, tokenizer, document) <= qualified.max_text_tokens) {
            try soft.append(a, document);
            continue;
        }
        const chunks = try chunkDocument(a, tokenizer, document, chunk_max_tokens);
        defer a.free(chunks);
        for (chunks) |chunk| try soft.append(a, document[chunk.start..chunk.end]);
    }
    if (soft.items.len == 0) return error.EmptyMemorillaCollection;
    if (soft.items.len > qualified.max_documents) return error.MemorillaCollectionBudgetExceeded;
    // The record batch: bounds, rows and the copy-source map are recomputed
    // by the encoder pass and must agree.
    var batch = try token_features.prepareBounded(a, tokenizer, documents, canonical, token_features.hybrid_max_padded_length);
    defer batch.deinit();
    if (try mul(batch.batch, batch.seq_len) > max_record_positions) return error.MemorillaEncoderBoundExceeded;
    const record_rows = try mul(try token_features.selectedRowCount(batch.counts), 2);
    try selected.verify(a, io);
    try control.check();
    return .{ .a = a, .question_tokens = question_tokens, .soft_texts = try soft.toOwnedSlice(a), .record_rows = record_rows };
}

// --- service --------------------------------------------------------------------

pub fn generate(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const u8, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, request_limits: platform.resource_policy.InferenceRequestLimits, control: Control, options_in: Options) !OwnedResult {
    var options = options_in;
    options.request_limits = request_limits;
    const started = std.Io.Timestamp.now(io, .awake);
    var result = try generateImpl(a, io, manager, profile, paths, question, documents, identity, max_new_tokens, limits, control, options);
    errdefer result.deinit();
    try control.check();
    if (result.timings) |*timings| timings.total_ms = elapsedSince(io, started);
    return result;
}

fn generateImpl(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, question: []const u8, documents: []const []const u8, identity: []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control, options: Options) !OwnedResult {
    try control.check();
    try validateInputs(profile, question, documents, identity, max_new_tokens);
    try common.validateRequestLimits(options.request_limits, documents.len, max_new_tokens);
    try requireLimits(limits);
    try qualified.requireCudaEnvironment();
    if (paths.encoder.publication == null or paths.decoder.publication == null) return error.MemorillaManagedProducerRequired;
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    const encoder_control = paths.encoder.bind(control);
    const decoder_control = paths.decoder.bind(control);
    const canonical = try qualified.canonicalQuestion(question);
    const released_directory = profile.released_memory_module_path orelse return error.UnsupportedMemorillaProfile;
    // A span-window bundle (plan 8.11) replaces the copy17 record half.
    if (try readSpanProtocol(platform.allocator.reclaimingScratch(a), profile.memory_module_path)) |span_protocol| {
        if (options.span_soft_only)
            return generateSoftOnlyImpl(a, io, manager, profile, paths, canonical, documents, max_new_tokens, limits, control, options, span_protocol, released_directory);
        return generateSpanImpl(a, io, manager, profile, paths, canonical, documents, max_new_tokens, limits, control, options, span_protocol, released_directory);
    }
    // Read modes (and the soft-only comparison) are span-bundle features.
    if (options.span_read_mode != null or options.span_soft_only) return error.UnsupportedMemorillaReadMode;
    // Stored soft-half vectors are a span-bundle feature.
    if (options.stored_documents != null or options.stored_question != null) return error.UnsupportedMemorillaProfile;

    var response_lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 32 * 1024 * 1024 });
    var transfer_response = false;
    defer if (!transfer_response) response_lease.release();
    const backing = platform.allocator.reclaimingScratch(a);

    // Candidate pins read before any model loads: the run-time adapters and
    // the exact-answer prompt of the hybrid bundle.
    var adapter = try encoder_lora.loadBundle(backing, profile.memory_module_path, encoder_lora.qwen3_embedding_4b);
    defer adapter.deinit();
    const exact_prompt = try research_service.bundleSystemPrompt(backing, profile.memory_module_path);
    defer backing.free(exact_prompt);
    const merged_prompt = try mergedSystemPrompt(backing, exact_prompt);
    defer backing.free(merged_prompt);

    var preflight = try preflightEncoder(backing, io, manager, paths.encoder, canonical, documents, encoder_control);
    defer preflight.deinit();
    try common.validateInputTokenLimit(options.request_limits, preflight.question_tokens);

    var assets = try common.acquireGenerationAssets(backing, io, manager, paths.decoder, decoder_profile, decoder_control);
    defer assets.deinit();
    const config = try assets.decoderConfig(backing, paths.decoder.path);
    const bounded = decode.Limits{ .max_new_tokens = profile.max_new_tokens };
    const template = assets.chatTemplate() orelse return error.MemorillaChatTemplateRequired;
    // Both prompts are prepared up front; the gate picks one after the record.
    var open_text = try decode.prepareText(backing, assets.tokenizer(), template, config, canonical, .{ .system_prompt = merged_prompt, .memory_rows = hybrid_rows }, bounded, decoder_control);
    defer open_text.deinit();
    var closed_text = try decode.prepareText(backing, assets.tokenizer(), template, config, canonical, .{ .system_prompt = helpful_prompt, .memory_rows = slots }, bounded, decoder_control);
    defer closed_text.deinit();
    try common.validateInputTokenLimit(options.request_limits, @max(open_text.input_ids.len, closed_text.input_ids.len));
    if (options.prepared_text) |callback| try callback(options.context, &open_text);
    const open_plan = try decode.ResourcePlan.create(config, .cuda, open_text.input_ids.len, open_text.rendered.len, hybrid_rows, max_new_tokens, bounded);
    const closed_plan = try decode.ResourcePlan.create(config, .cuda, closed_text.input_ids.len, closed_text.rendered.len, slots, max_new_tokens, bounded);

    // Admission: the decoder plan covering either prompt plus the copy/snap
    // transients, the released reader over the pooled rows and the research
    // reader over the per-token rows (plus the uploaded 32 memory rows).
    const soft_request = memory.AdmissionAmounts{ .host_scratch_bytes = 16 * 1024 * 1024, .backend_scratch_bytes = try add(128 * 1024 * 1024, try mul(qualified.max_documents, qualified.width * 16 * @sizeOf(f32))) };
    const record_request = memory.AdmissionAmounts{
        .host_scratch_bytes = try add(16 * 1024 * 1024, try mul(preflight.record_rows, qualified.width * @sizeOf(f32))),
        .backend_scratch_bytes = try add(try add(128 * 1024 * 1024, try mul(preflight.record_rows, research_service.reader_scratch_per_row)), try mul(hybrid_rows, config.hidden_size * @sizeOf(f32))),
    };
    const copy_request = memory.AdmissionAmounts{ .backend_scratch_bytes = research_service.decoder_copy_scratch_bytes };
    const open_decoder = try (try memory.AdmissionAmounts.fromEstimate(open_plan.decoder).merge(.fromEstimate(open_plan.host))).merge(copy_request);
    const closed_decoder = try (try memory.AdmissionAmounts.fromEstimate(closed_plan.decoder).merge(.fromEstimate(closed_plan.host))).merge(copy_request);
    const decoder_request = maxAmounts(open_decoder, closed_decoder);
    const request_amounts = try (try decoder_request.merge(soft_request)).merge(record_request);
    var request_lease = try manager.acquireRunResourceAmounts(.gpu, limits, request_amounts);
    defer request_lease.release();
    var decoder_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, decoder_request));
    var soft_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, soft_request));
    var record_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, record_request));
    // Reserve the 32-row plan; a closed gate swaps it for the 16-row plan in
    // place (the reservation's address identifies the decoder owner).
    var reservation = try open_plan.reserve(&decoder_budget);
    defer reservation.deinit();
    const copy_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = research_service.decoder_copy_scratch_bytes, .scratch_tier = .backend };
    try decoder_budget.reserveEstimate(copy_estimate);
    defer decoder_budget.releaseEstimate(copy_estimate);
    inline for (.{ .{ &soft_budget, soft_request }, .{ &record_budget, record_request } }) |entry| {
        const backend_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = entry[1].backend_scratch_bytes, .scratch_tier = .backend };
        try entry[0].reserveEstimate(backend_estimate);
        const host_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = entry[1].host_scratch_bytes, .scratch_tier = .host };
        entry[0].reserveEstimate(host_estimate) catch |err| {
            entry[0].releaseEstimate(backend_estimate);
            return err;
        };
    }
    defer inline for (.{ .{ &soft_budget, soft_request }, .{ &record_budget, record_request } }) |entry| {
        entry[0].releaseEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = entry[1].host_scratch_bytes, .scratch_tier = .host });
        entry[0].releaseEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = entry[1].backend_scratch_bytes, .scratch_tier = .backend });
    };
    try notify(options, .prepared, manager);

    // One resident encoder serves both passes.
    var encoder_handle = try common.acquireResidentModel(manager, paths.encoder.path, encoder_control);
    defer encoder_handle.release();
    const encoder_model = encoder_handle.get();
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try notify(options, .encoder_loaded, manager);
    // Soft pass: pooled vectors without adapters.
    var pooled = try encoder.encode(backing, &encoder_handle, preflight.soft_texts, canonical, limits, encoder_control);
    defer pooled.deinit();
    if (!std.mem.eql(u8, pooled.question_text, canonical)) return error.NonCanonicalMemorillaQuestion;
    try encoder.requireProducer(pooled.producer, paths.encoder.publication.?, encoder_profile);
    if (pooled.document_count != preflight.soft_texts.len or pooled.question_tokens != preflight.question_tokens) return error.InvalidMemorillaTokens;
    // Record pass: per-token features with the pinned adapters at run time.
    var record_features = try encoder.encodeHybridTokenFeatures(backing, &encoder_handle, documents, canonical, &adapter, limits, encoder_control);
    defer record_features.deinit();
    try encoder.requireProducer(record_features.producer, paths.encoder.publication.?, encoder_profile);
    if (record_features.adapter_sha256 == null or !std.mem.eql(u8, &record_features.adapter_sha256.?, &adapter.sha256)) return error.MemorillaCandidateMismatch;
    if (record_features.features.rows != preflight.record_rows) return error.MemorillaCollectionBudgetExceeded;
    try notify(options, .question_encoded, manager);
    try control.check();
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);

    var decoder_handle = try common.acquireResidentModel(manager, paths.decoder.path, decoder_control);
    defer decoder_handle.release();
    const decoder_model = decoder_handle.get();
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try notify(options, .decoder_loaded, manager);
    var native_guard = try control.enterUninterruptible(.uninterruptible);
    defer native_guard.deinit();

    // Soft half: the released reader, borrowed and retired before the record
    // half (the two reader caches are never held together).
    const soft_rows, const soft_module_sha256 = blk: {
        var soft_handle = try decoder_model.memorilla_reader_cache.acquire(decoder_model.allocator, io, manager, released_directory, limits, control);
        defer soft_handle.deinit();
        var soft_clean = false;
        defer if (!soft_clean) soft_handle.invalidate();
        const reader = soft_handle.owner();
        try reader.bundle.config.validate(true);
        if (!reader.bundle.release or reader.bundle.config.num_memories != slots or reader.bundle.config.output_dim != config.hidden_size) return error.UnsupportedMemorillaProfile;
        const rows = pooled.document_count;
        const mask = try backing.alloc(bool, rows);
        defer backing.free(mask);
        @memset(mask, false);
        var soft_backend = try reader.computeBackendWithBudget(&soft_budget);
        soft_backend.execution_control = control;
        soft_backend.execution_budget = &soft_budget;
        var soft_scope = try execution.Scope.acquire(control, .{ .mutex = &reader.execution_mutex });
        defer soft_scope.deinit();
        var soft_physical: PhysicalBudgetScope = if (comptime @import("build_options").enable_cuda)
            try reader.compute.beginMemorillaDeviceBudget(soft_request.backendTotalBytes())
        else
            return error.CudaNotEnabled;
        defer soft_physical.deinit();
        try soft_scope.bind(soft_backend);
        const values = inner: {
            defer {
                reader.releaseRequestResources() catch @panic("Memorilla released reader request cleanup failed; worker restart required");
                soft_clean = true;
            }
            const docs = try soft_backend.fromFloat32Shape(pooled.documents, &.{ @intCast(rows), @intCast(qualified.width) });
            defer soft_backend.free(docs);
            const query = try soft_backend.fromFloat32Shape(pooled.question, &.{ 1, @intCast(qualified.width) });
            defer soft_backend.free(query);
            try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
            try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            const memories = try memory_pipeline.forward(&soft_backend, backing, reader.bundle.config, .{ .documents = docs, .question = query, .padding_mask = mask, .batch = 1, .num_documents = rows, .max_document_rows = qualified.max_documents }, null);
            defer soft_backend.free(memories);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            break :inner try soft_backend.toFloat32(memories, backing);
        };
        errdefer backing.free(values);
        if (values.len != slots * config.hidden_size) return error.InvalidMemorillaMemoryRows;
        try soft_handle.verify(control);
        break :blk .{ values, reader.bundle.weights_sha256 };
    };
    defer backing.free(soft_rows);
    try control.check();

    // Record half: the research reader of the hybrid bundle.
    var reader_handle = try decoder_model.memorilla_research_reader_cache.acquire(decoder_model.allocator, io, manager, profile.memory_module_path, limits, control);
    defer reader_handle.deinit();
    var reader_request_clean = false;
    defer if (!reader_request_clean) reader_handle.invalidate();
    const reader = reader_handle.owner();
    if (reader.bundle.release or reader.bundle.config.num_memories != slots or reader.bundle.config.output_dim != config.hidden_size) return error.UnsupportedMemorillaProfile;
    if (!std.mem.eql(u8, reader.system_prompt, exact_prompt)) return error.ModelArtifactsChanging;
    // One candidate: the adapters were verified against this very manifest,
    // and the reader names the same candidate.json.
    if (!std.mem.eql(u8, &reader.bundle.manifest_sha256, &adapter.manifest_sha256)) return error.ModelArtifactsChanging;
    if (!std.mem.eql(u8, &reader.candidate_sha256, &adapter.candidate_sha256)) return error.MemorillaCandidateMismatch;
    try notify(options, .reader_loaded, manager);
    const module_sha256 = reader.bundle.weights_sha256;
    const candidate_sha256 = reader.candidate_sha256;

    var reader_backend = try reader.computeBackendWithBudget(&record_budget);
    reader_backend.execution_control = control;
    reader_backend.execution_budget = &record_budget;
    var reader_scope = try execution.Scope.acquire(control, .{ .mutex = &reader.execution_mutex });
    defer reader_scope.deinit();
    var reader_physical: PhysicalBudgetScope = if (comptime @import("build_options").enable_cuda)
        try reader.compute.beginMemorillaDeviceBudget(record_request.backendTotalBytes())
    else
        return error.CudaNotEnabled;
    defer reader_physical.deinit();
    try reader_scope.bind(reader_backend);
    var completed_spans: []copy.CompletedSpan = &.{};
    defer backing.free(completed_spans);
    var gate: RecordGate = .closed;
    var record_text: []u8 = &.{};
    defer backing.free(record_text);
    var min_snap_cosine: f32 = 0;
    const output = blk: {
        defer {
            reader.releaseRequestResources() catch @panic("Memorilla research reader request cleanup failed; worker restart required");
            reader_request_clean = true;
        }
        const rows = record_features.features.rows;
        const mask = try backing.alloc(bool, rows);
        defer backing.free(mask);
        @memset(mask, false);
        try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
        try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
        try factory.requireMemorillaCudaExecution(&reader_backend);
        const memories = blk2: {
            const docs = try reader_backend.fromFloat32Shape(record_features.features.docs, &.{ @intCast(rows), @intCast(qualified.width) });
            defer reader_backend.free(docs);
            const query = try reader_backend.fromFloat32Shape(record_features.features.query, &.{ 1, @intCast(qualified.width) });
            defer reader_backend.free(query);
            const memories_ct = try memory_pipeline.forward(&reader_backend, backing, reader.bundle.config, .{ .documents = docs, .question = query, .padding_mask = mask, .batch = 1, .num_documents = rows, .max_document_rows = preflight.record_rows }, null);
            defer reader_backend.free(memories_ct);
            break :blk2 try reader_backend.toFloat32(memories_ct, backing);
        };
        defer backing.free(memories);
        try factory.requireMemorillaCudaExecution(&reader_backend);
        try control.check();
        var decoder_backend = try factory.getCoordinatedCudaComputeBackend(decoder_model.session, &decoder_budget, @intFromPtr(&reservation));
        defer decoder_backend.deinit();
        var snapped = blk3: {
            decoder_backend.execution_control = decoder_control;
            decoder_backend.execution_budget = &decoder_budget;
            var decoder_scope = try execution.Scope.acquire(control, .{ .mutex = decoder_model.nativeGenerationMutex(), .lane = &decoder_model.native_reader_execution_lane });
            defer decoder_scope.deinit();
            try decoder_scope.bind(decoder_backend);
            try factory.requireMemorillaCudaExecution(&decoder_backend);
            const result = try research_service.copyAndSnap(backing, reader, &decoder_backend, config, assets.tokenizer(), memories, record_features.features.copy_ids, control);
            errdefer {
                var owned = result;
                owned.deinit();
            }
            try factory.requireMemorillaCudaExecution(&decoder_backend);
            break :blk3 result;
        };
        defer snapped.deinit();
        completed_spans = snapped.spans;
        snapped.spans = &.{};
        min_snap_cosine = std.math.inf(f32);
        for (snapped.cosines) |cosine| min_snap_cosine = @min(min_snap_cosine, cosine);
        gate = if (try recordGateOpen(snapped.copy_spans, snapped.cosines, record_gate_cosine)) .open else .closed;
        record_text = try recordText(backing, assets.tokenizer(), snapped.completed_ids);
        try control.check();
        // Compose the memory rows on the host: soft rows first, then (gate
        // open) the record's embedding rows; one F32 upload for injection.
        const memory_rows: usize = if (gate == .open) hybrid_rows else slots;
        const composed = try backing.alloc(f32, memory_rows * config.hidden_size);
        defer backing.free(composed);
        @memcpy(composed[0 .. slots * config.hidden_size], soft_rows);
        if (gate == .open) @memcpy(composed[slots * config.hidden_size ..], snapped.rows);
        if (gate == .closed) {
            reservation.deinit();
            reservation = try closed_plan.reserve(&decoder_budget);
        }
        const text = if (gate == .open) &open_text else &closed_text;
        const rows_ct = try reader_backend.fromFloat32Shape(composed, &.{ @intCast(memory_rows), @intCast(config.hidden_size) });
        defer reader_backend.free(rows_ct);
        var decoder = try decode.Decoder.fromQualifiedSession(decoder_model.session, &decoder_backend, assets.tokenizer(), decoder_model.nativeGenerationMutex());
        decoder.execution_lane = &decoder_model.native_reader_execution_lane;
        break :blk try decode.generate(a, decoder, text, .{ .backend = &reader_backend, .tensor = rows_ct }, .{ .max_new_tokens = max_new_tokens, .teacher_forced_ids = options.teacher_forced_ids, .trace = options.trace, .ignore_eos = options.ignore_eos }, &reservation, bounded, decoder_control);
    };
    var result = output;
    errdefer result.deinit();

    // Publish: the stripped decoder answer, re-spelled from the record when
    // the decoder read it, then the frame-echo guard and the citations.
    const raw_text = try a.dupe(u8, pythonStrip(result.text));
    errdefer a.free(raw_text);
    const published = if (gate == .open)
        try canonicalizeAnswer(result.allocator, raw_text, record_text)
    else
        try result.allocator.dupe(u8, raw_text);
    result.allocator.free(result.text);
    result.text = published;
    const answer_guard = guardAnswer(result.text);
    if (answer_guard == .rejected) try research_service.abstainGeneration(&result);
    const canonicalized = answer_guard == .accepted and !std.mem.eql(u8, result.text, raw_text);
    var citations = if (gate == .open)
        try copy.buildCitations(a, completed_spans, record_features.features.sources, documents, research_service.citedAnswer(result.text, answer_guard))
    else
        copy.Citations.none(a, false);
    errdefer citations.deinit();
    const owned_record_text = try a.dupe(u8, record_text);
    errdefer a.free(owned_record_text);
    if (comptime @import("build_options").enable_cuda) {
        if (options.reader_physical_resources) |record| {
            const snapshot = reader.compute.ctx.device_allocations.snapshot();
            try record(options.context, .{ .baseline = reader_physical.baseline, .held_transient_bytes = record_request.backendTotalBytes(), .limit = snapshot.limit, .live = snapshot.live, .lifetime_peak = snapshot.peak });
        }
    }
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    try reader_handle.verify(control);
    try control.check();
    try notify(options, .decoded, manager);
    try control.check();
    if (record_features.question_tokens == 0) return error.InvalidMemorillaTokens;
    transfer_response = true;
    return .{
        .allocator = a,
        .generation = result,
        .answer_guard = answer_guard,
        .record_gate = gate,
        .canonicalized = canonicalized,
        .memory_tokens = if (gate == .open) hybrid_rows else slots,
        .encoder_execution_identity = qualified.executionIdentity(encoder_profile),
        .decoder_execution_identity = qualified.executionIdentity(decoder_profile),
        .module_sha256 = module_sha256,
        .soft_module_sha256 = soft_module_sha256,
        .candidate_sha256 = candidate_sha256,
        .adapter_sha256 = adapter.sha256,
        .document_count = documents.len,
        .soft_rows = pooled.document_count,
        .question_tokens = preflight.question_tokens,
        .citations = citations,
        .record_text = owned_record_text,
        .raw_text = raw_text,
        .min_snap_cosine = min_snap_cosine,
        .response_lease = response_lease,
    };
}

// --- span-window record half (plan 8.11/8.12) ------------------------------------

/// Debug-level stage timing of the span path (`memorilla_span_timing`).
const SpanTimer = struct {
    io: std.Io,
    last: std.Io.Timestamp,
    /// The marks so far, by stage (`SpanTimings.add`).
    timings: SpanTimings = .{},

    fn start(io: std.Io) SpanTimer {
        return .{ .io = io, .last = std.Io.Timestamp.now(io, .awake) };
    }

    fn mark(self: *SpanTimer, name: []const u8) void {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const nanoseconds = self.last.durationTo(now).nanoseconds;
        self.last = now;
        const ms = @as(f64, @floatFromInt(@max(nanoseconds, 0))) / 1_000_000.0;
        self.timings.add(name, ms);
        std.log.info("memorilla_span_timing stage={s} ms={d:.1}", .{ name, ms });
    }
};

/// `ANTFLY_MEMORILLA_SPAN_HEAD_HOST=1`: run the span head on the host (F32,
/// `memorilla_span.forward`) instead of the device-resident head.
pub const span_head_host_env = "ANTFLY_MEMORILLA_SPAN_HEAD_HOST";

fn spanHeadOnHost() bool {
    return platform.env.getenvBool(span_head_host_env);
}

/// The span fields of the bundle's pinned `protocol.json` (sha256 and size
/// checked against `manifest.json`), or null for a copy17 bundle.
pub fn readSpanProtocol(a: A, directory: []const u8) !?span_window.Protocol {
    const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
    defer a.free(manifest_path);
    const manifest_bytes = try c_file.readFileMax(a, manifest_path, 1024 * 1024);
    defer a.free(manifest_bytes);
    var manifest = std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{}) catch return error.InvalidMemorillaManifest;
    defer manifest.deinit();
    const protocol_path = try std.fs.path.join(a, &.{ directory, "protocol.json" });
    defer a.free(protocol_path);
    const protocol_bytes = try c_file.readFileMax(a, protocol_path, 256 * 1024);
    defer a.free(protocol_bytes);
    try @import("../models/memorilla.zig").verifyFile(manifest.value, "protocol.json", protocol_bytes);
    return span_window.parseProtocol(a, protocol_bytes);
}

/// The soft half's reader inputs: BF16-valued F32 rows, `qualified.width` wide.
const SoftInputs = struct {
    documents: []const f32,
    document_count: usize,
    question: []const f32,
};

/// Encoder tokens (no special tokens, `tokenizer(text,
/// add_special_tokens=False)`) of the candidate documents, counted on first
/// use: the read-budget rule inspects only the documents it reads plus the
/// first one it refuses.
const DocumentTokens = struct {
    a: A,
    tokenizer: tok.Tokenizer,
    documents: []const []const u8,

    fn count(self: *const DocumentTokens, index: usize) anyerror!usize {
        if (index >= self.documents.len) return error.InvalidMemorillaDocumentCount;
        const ids = try self.tokenizer.encode(self.a, self.documents[index]);
        defer self.a.free(ids);
        return ids.len;
    }
};

/// One read set a request may take: its plan and the span rows (copy ids)
/// its record pass selects.
const ReadOption = struct { plan: span_window.ReadPlan, span_rows: usize };

/// Preflight failures of one cascade read set that leave the request
/// answerable from the other read (study F: a record error is margin -inf
/// for the narrow read and "keep narrow" for the wide one).
fn recoverableRead(err: anyerror) bool {
    return err == error.MemorillaEncoderBoundExceeded or err == error.MemorillaQuestionPrefixMismatch;
}

const SpanPreflight = struct {
    a: A,
    question_tokens: usize,
    soft_texts: [][]const u8,
    /// The narrow read of a cascade request (without a cascade: the one read
    /// of every document) and the wide read, when they passed preflight and
    /// the mode can take them.
    narrow: ?ReadOption = null,
    wide: ?ReadOption = null,
    /// A cascade read set skipped at preflight (`recoverableRead`).
    narrow_error: ?anyerror = null,
    wide_error: ?anyerror = null,
    /// Admitted: the most span rows and the widest window of the usable reads.
    span_rows: usize = 0,
    window_rows: usize = 0,

    fn deinit(self: *SpanPreflight) void {
        self.a.free(self.soft_texts);
    }

    fn option(self: *const SpanPreflight, pass: span_window.ReadPass) ?ReadOption {
        return switch (pass) {
            .narrow => self.narrow,
            .wide => self.wide,
        };
    }
};

/// The span batch of `plan`'s read set, bounded as the encoder pass bounds
/// it: its copy-id count.
fn prepareRead(a: A, tokenizer: tok.Tokenizer, documents: []const []const u8, canonical: []const u8, plan: span_window.ReadPlan, control: Control) !ReadOption {
    try control.check();
    var batch = try token_features.prepareSpanBounded(a, tokenizer, documents[0..plan.documents], canonical, token_features.hybrid_max_padded_length);
    defer batch.deinit();
    if (try mul(batch.batch, batch.seq_len) > max_record_positions) return error.MemorillaEncoderBoundExceeded;
    return .{ .plan = plan, .span_rows = batch.copy_ids.len };
}

/// Encoder-side preflight of a span request. `documents` are the request's
/// documents: the span read set itself for a bundle without a cascade
/// (`mode` null), the ranked candidate list with one. A cascade request
/// prepares the reads its mode can take (`narrow`: the narrow one, `wide`:
/// the wide one, `auto`: both); a read that fails a recoverable bound is
/// skipped while the mode's first pass stays possible, and admission covers
/// the larger. `pool_documents`: the soft half pools `documents` (no stored
/// chunk vectors). `reads`: false prepares no span read at all (the
/// soft-only request), only the question and the pooled texts.
fn preflightSpan(a: A, io: std.Io, manager: *manager_mod.ModelManager, selected: selection.ResolvedModel, canonical: []const u8, documents: []const []const u8, protocol: *const span_window.Protocol, mode: ?span_window.ReadMode, pool_documents: bool, reads: bool, control: Control) !SpanPreflight {
    try control.check();
    var assets = try common.acquireGenerationAssets(a, io, manager, selected, encoder_profile, control);
    defer assets.deinit();
    const tokenizer = assets.tokenizer();
    const question_tokens = blk: {
        var tokens = try tokenizer.encodeForModel(a, canonical, qualified.max_text_tokens + 1);
        defer tokens.deinit();
        if (tokens.ids.len != tokens.attention_mask.len) return error.InvalidMemorillaTokens;
        var active: usize = 0;
        var last: i32 = -1;
        for (tokens.ids, tokens.attention_mask) |id, used| {
            if (used != 0 and used != 1) return error.InvalidMemorillaTokens;
            if (used == 1) {
                active += 1;
                last = id;
            }
        }
        if (active > qualified.max_text_tokens) return error.MemorillaTextLimitExceeded;
        if (active == 0 or last != token_features.eos_id) return error.InvalidMemorillaTokens;
        break :blk active;
    };
    var soft: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer soft.deinit(a);
    if (pool_documents) {
        for (documents) |document| {
            try control.check();
            if (try tokenCount(a, tokenizer, document) <= qualified.max_text_tokens) {
                try soft.append(a, document);
                continue;
            }
            const chunks = try chunkDocument(a, tokenizer, document, chunk_max_tokens);
            defer a.free(chunks);
            for (chunks) |chunk| try soft.append(a, document[chunk.start..chunk.end]);
        }
        if (soft.items.len == 0) return error.EmptyMemorillaCollection;
        if (soft.items.len > qualified.max_documents) return error.MemorillaCollectionBudgetExceeded;
    }
    var out = SpanPreflight{ .a = a, .question_tokens = question_tokens, .soft_texts = &.{} };
    if (!reads) {
        if (mode != null) return error.UnsupportedMemorillaReadMode;
    } else if (mode) |m| {
        const cascade = protocol.read_cascade orelse return error.UnsupportedMemorillaReadMode;
        if (m != .wide) {
            const plan = try span_window.narrowPlan(protocol, documents.len);
            if (prepareRead(a, tokenizer, documents, canonical, plan, control)) |option| out.narrow = option else |err| {
                if (!recoverableRead(err)) return err;
                out.narrow_error = err;
            }
        }
        if (m != .narrow) {
            const tokens = DocumentTokens{ .a = a, .tokenizer = tokenizer, .documents = documents };
            const plan = try span_window.widePlan(cascade, &tokens, DocumentTokens.count, documents.len);
            // A wide read of the narrow documents selects the same rows.
            const same = if (out.narrow) |narrow| narrow.plan.documents == plan.documents else false;
            if (same) {
                out.wide = .{ .plan = plan, .span_rows = out.narrow.?.span_rows };
            } else if (prepareRead(a, tokenizer, documents, canonical, plan, control)) |option| out.wide = option else |err| {
                if (!recoverableRead(err)) return err;
                out.wide_error = err;
            }
        }
        if (span_window.firstPass(m, out.narrow != null, out.wide != null) == null)
            return (if (m == .wide) out.wide_error else out.narrow_error) orelse error.MemorillaCollectionBudgetExceeded;
    } else {
        out.narrow = try prepareRead(a, tokenizer, documents, canonical, try span_window.wholePlan(protocol, documents.len), control);
    }
    for ([_]?ReadOption{ out.narrow, out.wide }) |entry| if (entry) |option| {
        out.span_rows = @max(out.span_rows, option.span_rows);
        out.window_rows = @max(out.window_rows, option.plan.window_tokens);
    };
    try selected.verify(a, io);
    try control.check();
    out.soft_texts = try soft.toOwnedSlice(a);
    return out;
}

/// The pinned candidate every span pass of one request must read.
const AdapterPins = struct { adapter: [64]u8, candidate: [64]u8 };

/// The span features of one read set: the stock encoder with the bundle's
/// encoder LoRA (verified on a miss and cached per encoder) at run time.
fn spanPassFeatures(a: A, io: std.Io, manager: *manager_mod.ModelManager, encoder_handle: *const manager_mod.ModelHandle, profile: Profile, paths: Paths, documents: []const []const u8, canonical: []const u8, span_rows: usize, prefix: ?encoder.SpanPrefix, limits: memory.Limits, control: Control, encoder_control: Control, pins: *?AdapterPins) !encoder.SpanTokenFeatures {
    const encoder_model = encoder_handle.get();
    var adapter_handle = try encoder_model.memorilla_adapter_cache.acquire(encoder_model.allocator, a, io, manager, profile.memory_module_path, encoder_lora.qwen3_embedding_4b, limits, control);
    const seen = AdapterPins{ .adapter = adapter_handle.adapter().sha256, .candidate = adapter_handle.adapter().candidate_sha256 };
    var features = blk: {
        defer adapter_handle.deinit();
        break :blk try encoder.encodeSpanTokenFeatures(a, encoder_handle, documents, canonical, adapter_handle.adapter(), prefix, limits, encoder_control);
    };
    errdefer features.deinit();
    try encoder.requireProducer(features.producer, paths.encoder.publication.?, encoder_profile);
    if (!std.mem.eql(u8, &features.adapter_sha256, &seen.adapter)) return error.MemorillaCandidateMismatch;
    if (features.rows() != span_rows) return error.MemorillaCollectionBudgetExceeded;
    // Both passes of a cascade request read one candidate.
    if (pins.*) |known| {
        if (!std.meta.eql(known, seen)) return error.ModelArtifactsChanging;
    } else pins.* = seen;
    return features;
}

/// The span head over one pass's features: its best span. The head runs on
/// the device (weights resident per decoder); `ANTFLY_MEMORILLA_SPAN_HEAD_HOST=1`
/// selects the host F32 head (A/B parity runs).
fn spanPassHead(a: A, io: std.Io, manager: *manager_mod.ModelManager, decoder_model: anytype, profile: Profile, protocol: span_window.Protocol, features: *const encoder.SpanTokenFeatures, bounds: []const span_window.Bound, limits: memory.Limits, control: Control, head_sha256: *[64]u8) !span_window.Span {
    if (spanHeadOnHost()) {
        var head_handle = try decoder_model.memorilla_span_head_cache.acquire(decoder_model.allocator, a, io, manager, profile.memory_module_path, protocol, limits, control);
        defer head_handle.deinit();
        head_sha256.* = head_handle.head().sha256;
        var logits = try span_window.forward(a, io, head_handle.head(), features.features, features.counts);
        defer logits.deinit();
        return span_window.bestSpan(logits.start, logits.end, bounds, protocol.max_span_tokens);
    }
    var head_handle = try decoder_model.memorilla_span_head_device_cache.acquire(decoder_model.allocator, io, manager, profile.memory_module_path, limits, control);
    defer head_handle.deinit();
    const owner = head_handle.owner();
    head_sha256.* = owner.sha256;
    var logits = owner.forward(a, protocol, features.features, features.counts) catch |err| {
        head_handle.invalidate();
        return err;
    };
    defer logits.deinit();
    return span_window.bestSpan(logits.start, logits.end, bounds, protocol.max_span_tokens);
}

/// One span pass of a request: its read plan, features, document bounds,
/// best span and gate margin.
const SpanRead = struct {
    plan: span_window.ReadPlan,
    features: encoder.SpanTokenFeatures,
    bounds: []span_window.Bound,
    best: span_window.Span,
    margin: f64,

    fn deinit(self: *SpanRead, a: A) void {
        a.free(self.bounds);
        self.features.deinit();
    }
};

fn elapsedSince(io: std.Io, started: std.Io.Timestamp) f64 {
    const nanoseconds = started.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds;
    return if (nanoseconds <= 0) 0 else @as(f64, @floatFromInt(nanoseconds)) / 1_000_000.0;
}

/// `ANTFLY_MEMORILLA_SPAN_CASCADE_REUSE=0`: a cascade's wide pass encodes
/// every document it reads instead of reusing the narrow pass's rows of the
/// leading documents (research A/B; outputs as `wide` mode, bit for bit).
pub const span_cascade_reuse_env = "ANTFLY_MEMORILLA_SPAN_CASCADE_REUSE";

fn cascadeReuse() bool {
    return platform.env.getenvBoolDefault(span_cascade_reuse_env, true);
}

/// Runs `option`'s span features into `slot` (owned there from the moment
/// they exist, so every later error path releases their lease).
fn startSpanRead(a: A, io: std.Io, manager: *manager_mod.ModelManager, encoder_handle: *const manager_mod.ModelHandle, profile: Profile, paths: Paths, documents: []const []const u8, canonical: []const u8, option: ReadOption, prefix: ?encoder.SpanPrefix, limits: memory.Limits, control: Control, encoder_control: Control, pins: *?AdapterPins, slot: *?SpanRead) !void {
    var features = try spanPassFeatures(a, io, manager, encoder_handle, profile, paths, documents[0..option.plan.documents], canonical, option.span_rows, prefix, limits, control, encoder_control, pins);
    const bounds = span_window.docBounds(a, features.counts) catch |err| {
        features.deinit();
        return err;
    };
    slot.* = .{ .plan = option.plan, .features = features, .bounds = bounds, .best = undefined, .margin = undefined };
}

/// The span head over `read`'s features: its best span and gate margin.
/// Returns the head's milliseconds.
fn finishSpanRead(a: A, io: std.Io, manager: *manager_mod.ModelManager, decoder_model: anytype, profile: Profile, protocol: span_window.Protocol, limits: memory.Limits, control: Control, head_sha256: *[64]u8, read: *SpanRead) !f64 {
    const started = std.Io.Timestamp.now(io, .awake);
    read.best = try spanPassHead(a, io, manager, decoder_model, profile, protocol, &read.features, read.bounds, limits, control, head_sha256);
    const ms = elapsedSince(io, started);
    read.margin = read.best.score - read.best.null_score - protocol.gate_threshold;
    return ms;
}

fn generateSpanImpl(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, canonical: []const u8, documents: []const []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control, options: Options, protocol: span_window.Protocol, released_directory: []const u8) !OwnedResult {
    const encoder_control = paths.encoder.bind(control);
    const decoder_control = paths.decoder.bind(control);
    const policy = options.span_gate_policy orelse protocol.gate_policy;
    const declared = options.span_gate_profile;
    // Read policy (plan 8.32): with `span_read_cascade`, `documents` is the
    // ranked candidate list and the read mode (default `auto`, the cascade)
    // picks the read sets; no question is routed for its read. Without a
    // cascade every document is read under `span_window_tokens`, as before.
    const mode = try protocol.readMode(options.span_read_mode);
    const cascade = mode != null;

    var response_lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 32 * 1024 * 1024 });
    var transfer_response = false;
    defer if (!transfer_response) response_lease.release();
    const backing = platform.allocator.reclaimingScratch(a);

    const exact_prompt = try research_service.bundleSystemPrompt(backing, profile.memory_module_path);
    defer backing.free(exact_prompt);
    const merged_prompt = try mergedSystemPrompt(backing, exact_prompt);
    defer backing.free(merged_prompt);
    // The service's prompts (merged when open, helpful when closed, the
    // canonical question), or the benchmark's (`hybrid-bench`).
    const open_system: []const u8 = if (options.span_bench_prompt) |bench| bench.system else merged_prompt;
    const closed_system: []const u8 = if (options.span_bench_prompt) |bench| bench.system else helpful_prompt;
    const bench_user: ?[]u8 = if (options.span_bench_prompt) |bench| try std.mem.concat(backing, u8, &.{ canonical, bench.user_suffix }) else null;
    defer if (bench_user) |text| backing.free(text);
    const user_text: []const u8 = if (bench_user) |text| pythonStrip(text) else canonical;

    var preflight = try preflightSpan(backing, io, manager, paths.encoder, canonical, documents, &protocol, mode, !(cascade and options.stored_documents != null), true, encoder_control);
    defer preflight.deinit();
    try common.validateInputTokenLimit(options.request_limits, preflight.question_tokens);
    // Admitted for the widest read the request can take (worst case wide);
    // a narrower chosen window swaps in its own prompt and plan.
    const admitted_window_rows = preflight.window_rows;
    const admitted_open_rows = slots + admitted_window_rows;

    var assets = try common.acquireGenerationAssets(backing, io, manager, paths.decoder, decoder_profile, decoder_control);
    defer assets.deinit();
    const config = try assets.decoderConfig(backing, paths.decoder.path);
    var bounded = decode.Limits{ .max_new_tokens = profile.max_new_tokens };
    bounded.max_memory_rows = @max(bounded.max_memory_rows, slots + protocol.maxWindowTokens());
    const template = assets.chatTemplate() orelse return error.MemorillaChatTemplateRequired;
    var open_text = try decode.prepareText(backing, assets.tokenizer(), template, config, user_text, .{ .system_prompt = open_system, .memory_rows = admitted_open_rows }, bounded, decoder_control);
    defer open_text.deinit();
    var closed_text = try decode.prepareText(backing, assets.tokenizer(), template, config, user_text, .{ .system_prompt = closed_system, .memory_rows = slots }, bounded, decoder_control);
    defer closed_text.deinit();
    try common.validateInputTokenLimit(options.request_limits, @max(open_text.input_ids.len, closed_text.input_ids.len));
    if (options.prepared_text) |callback| try callback(options.context, &open_text);
    const open_plan = try decode.ResourcePlan.create(config, .cuda, open_text.input_ids.len, open_text.rendered.len, admitted_open_rows, max_new_tokens, bounded);
    const closed_plan = try decode.ResourcePlan.create(config, .cuda, closed_text.input_ids.len, closed_text.rendered.len, slots, max_new_tokens, bounded);

    // Admission: either decoder plan plus the embedding-lookup transients;
    // the released reader over the pooled rows plus the composed memory rows
    // it uploads; the host span head (weights, features, forward scratch).
    // The span passes of a cascade request run one after the other, so the
    // larger read bounds the head scratch; each pass's features carry their
    // own encoder lease.
    const soft_request = memory.AdmissionAmounts{
        .host_scratch_bytes = try add(16 * 1024 * 1024, try mul(admitted_open_rows, config.hidden_size * @sizeOf(f32))),
        .backend_scratch_bytes = try add(try add(128 * 1024 * 1024, try mul(qualified.max_documents, qualified.width * 16 * @sizeOf(f32))), try mul(admitted_open_rows, config.hidden_size * @sizeOf(f32))),
    };
    // The head's weights are admitted by the decoder's span-head cache.
    const span_host = try add(span_window.Head.forwardBytes(protocol, preflight.span_rows), try add(16 * 1024 * 1024, try mul(try mul(preflight.span_rows, span_window.feature_width), @sizeOf(f32))));
    const span_request = memory.AdmissionAmounts{ .host_scratch_bytes = span_host };
    const copy_request = memory.AdmissionAmounts{ .backend_scratch_bytes = research_service.decoder_copy_scratch_bytes };
    const open_decoder = try (try memory.AdmissionAmounts.fromEstimate(open_plan.decoder).merge(.fromEstimate(open_plan.host))).merge(copy_request);
    const closed_decoder = try (try memory.AdmissionAmounts.fromEstimate(closed_plan.decoder).merge(.fromEstimate(closed_plan.host))).merge(copy_request);
    const decoder_request = maxAmounts(open_decoder, closed_decoder);
    const request_amounts = try (try decoder_request.merge(soft_request)).merge(span_request);
    var request_lease = try manager.acquireRunResourceAmounts(.gpu, limits, request_amounts);
    defer request_lease.release();
    var decoder_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, decoder_request));
    var soft_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, soft_request));
    var reservation = try open_plan.reserve(&decoder_budget);
    defer reservation.deinit();
    const copy_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = research_service.decoder_copy_scratch_bytes, .scratch_tier = .backend };
    try decoder_budget.reserveEstimate(copy_estimate);
    defer decoder_budget.releaseEstimate(copy_estimate);
    const soft_backend_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = soft_request.backend_scratch_bytes, .scratch_tier = .backend };
    try soft_budget.reserveEstimate(soft_backend_estimate);
    defer soft_budget.releaseEstimate(soft_backend_estimate);
    const soft_host_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = soft_request.host_scratch_bytes, .scratch_tier = .host };
    try soft_budget.reserveEstimate(soft_host_estimate);
    defer soft_budget.releaseEstimate(soft_host_estimate);
    try notify(options, .prepared, manager);

    var encoder_handle = try common.acquireResidentModel(manager, paths.encoder.path, encoder_control);
    defer encoder_handle.release();
    const encoder_model = encoder_handle.get();
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try notify(options, .encoder_loaded, manager);
    var timer = SpanTimer.start(io);
    // Soft half inputs: the collection's stored chunk vectors (and the
    // question's stored vector) when the caller supplies them, as the native
    // service reads them; otherwise pooled encodings of the span documents.
    const stored_documents: ?[]f32 = if (options.stored_documents) |rows| try common.storedRows(backing, rows, qualified.max_documents) else null;
    defer if (stored_documents) |values| backing.free(values);
    const stored_question: ?[]f32 = if (stored_documents != null) if (options.stored_question) |vector| try common.storedRows(backing, &.{vector}, 1) else null else null;
    defer if (stored_question) |values| backing.free(values);
    var encoded: ?encoder.Features = if (stored_documents == null)
        try encoder.encode(backing, &encoder_handle, preflight.soft_texts, canonical, limits, encoder_control)
    else if (stored_question == null)
        try encoder.encodeQuestion(backing, &encoder_handle, canonical, limits, encoder_control)
    else
        null;
    defer if (encoded) |*features| features.deinit();
    timer.mark("pooled");
    factory.logMemorillaOpProfile(encoder_model.session, "pooled");
    if (encoded) |*features| {
        if (!std.mem.eql(u8, features.question_text, canonical)) return error.NonCanonicalMemorillaQuestion;
        try encoder.requireProducer(features.producer, paths.encoder.publication.?, encoder_profile);
        if (features.question_tokens != preflight.question_tokens) return error.InvalidMemorillaTokens;
        if (stored_documents == null and features.document_count != preflight.soft_texts.len) return error.InvalidMemorillaTokens;
    }
    const pooled = SoftInputs{
        .documents = if (stored_documents) |values| values else encoded.?.documents,
        .document_count = if (options.stored_documents) |rows| rows.len else encoded.?.document_count,
        .question = if (stored_question) |values| values else encoded.?.question,
    };
    // The gate's question vector (routed policy or a declared profile only).
    const gate_question: []const f32 = pooled.question;

    // Span passes: the first read (the narrow one, or without a cascade the
    // whole read), then for `auto` below the threshold the wide read, whose
    // record replaces the narrow one (study F `replace@t`).
    const first_pass: span_window.ReadPass = if (mode) |m|
        span_window.firstPass(m, preflight.narrow != null, preflight.wide != null) orelse return error.MemorillaCollectionBudgetExceeded
    else
        .narrow;
    var reads: [2]?SpanRead = .{ null, null };
    defer for (&reads) |*slot| if (slot.*) |*read| read.deinit(backing);
    var pins: ?AdapterPins = null;
    var head_sha256: [64]u8 = undefined;
    var head_ms: f64 = 0;
    const first_option = preflight.option(first_pass).?;
    try startSpanRead(backing, io, manager, &encoder_handle, profile, paths, documents, canonical, first_option, null, limits, control, encoder_control, &pins, &reads[@backingInt(first_pass)]);
    timer.mark("span_features");
    factory.logMemorillaOpProfile(encoder_model.session, "span_features");
    try notify(options, .question_encoded, manager);
    try control.check();

    // The decoder pin also pins its span-head cache.
    var decoder_handle = try common.acquireResidentModel(manager, paths.decoder.path, decoder_control);
    defer decoder_handle.release();
    const decoder_model = decoder_handle.get();
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try notify(options, .decoder_loaded, manager);

    head_ms += try finishSpanRead(backing, io, manager, decoder_model, profile, protocol, limits, control, &head_sha256, &reads[@backingInt(first_pass)].?);
    timer.mark("span_head");
    const narrow_index = @backingInt(span_window.ReadPass.narrow);
    const wide_index = @backingInt(span_window.ReadPass.wide);
    if (mode) |m| if (first_pass == .narrow and span_window.widens(protocol.read_cascade.?, m, reads[narrow_index].?.margin, preflight.wide != null)) {
        // The wide read begins with the narrow read's documents: their rows
        // are reused, and only the rest is encoded.
        const narrow = &reads[narrow_index].?;
        const prefix: ?encoder.SpanPrefix = if (options.span_cascade_reuse orelse cascadeReuse()) .{
            .documents = @min(narrow.plan.documents, preflight.wide.?.plan.documents),
            .copy_ids = narrow.features.copy_ids,
            .features = narrow.features.features,
        } else null;
        try startSpanRead(backing, io, manager, &encoder_handle, profile, paths, documents, canonical, preflight.wide.?, prefix, limits, control, encoder_control, &pins, &reads[wide_index]);
        timer.mark("wide_features");
        factory.logMemorillaOpProfile(encoder_model.session, "wide_features");
        head_ms += try finishSpanRead(backing, io, manager, decoder_model, profile, protocol, limits, control, &head_sha256, &reads[wide_index].?);
        timer.mark("wide_head");
    };
    // The record used: the wide one whenever it ran.
    const chosen_pass: span_window.ReadPass = if (reads[wide_index] != null) .wide else .narrow;
    const chosen = &reads[@backingInt(chosen_pass)].?;
    const features = &chosen.features;
    const best = chosen.best;
    const margin = chosen.margin;
    const bounds = chosen.bounds;
    const read_plan = chosen.plan;
    const read_documents = documents[0..read_plan.documents];
    const window_rows = read_plan.window_tokens;
    const open_rows = slots + window_rows;
    if (open_rows != admitted_open_rows) {
        // The chosen read needs a narrower window than admitted: its own
        // prompt and plan.
        var narrowed = try decode.prepareText(backing, assets.tokenizer(), template, config, user_text, .{ .system_prompt = open_system, .memory_rows = open_rows }, bounded, decoder_control);
        errdefer narrowed.deinit();
        const narrowed_plan = try decode.ResourcePlan.create(config, .cuda, narrowed.input_ids.len, narrowed.rendered.len, open_rows, max_new_tokens, bounded);
        reservation.deinit();
        reservation = try narrowed_plan.reserve(&decoder_budget);
        open_text.deinit();
        open_text = narrowed;
    }
    timer.mark("read_plan");
    // The gate (unchanged by the read policy): the routes table is read only
    // when the routed policy or a declared profile needs it; the question
    // routes on its pooled (stock encoder) vector.
    var routes: ?span_window.Routes = if (policy == .routed or declared != null) try span_window.Routes.loadBundle(backing, profile.memory_module_path, protocol) else null;
    defer if (routes) |*table| table.deinit();
    // A wide record of a cascade with `wide_gate_offset` is gated at that
    // offset; every other record exactly as before (`gateProtocol`).
    const gate_protocol = span_window.gateProtocol(protocol, if (cascade) chosen_pass else null);
    const decision = try span_window.decideGate(policy, gate_protocol, if (routes) |*table| table else null, gate_question, margin, declared);
    const gate: RecordGate = if (decision.open) .open else .closed;
    const gate_profile: ?[]u8 = if (decision.profile) |i| try a.dupe(u8, routes.?.names[i]) else null;
    errdefer if (gate_profile) |name| a.free(name);
    const continues = try span_window.continuationFlags(backing, assets.tokenizer(), features.copy_ids);
    defer backing.free(continues);
    const window = try span_window.evidenceWindow(continues, best.start, best.end, bounds, window_rows, protocol.left_fraction);
    const pad = try assets.tokenizer().encode(backing, " ");
    defer backing.free(pad);
    if (pad.len != 1 or pad[0] != copy.default_pad_id) return error.InvalidMemorillaTokenizer;
    const record_ids = try span_window.windowIds(backing, features.copy_ids, window, window_rows, pad[0]);
    defer backing.free(record_ids);
    const record_text = try recordText(backing, assets.tokenizer(), record_ids);
    defer backing.free(record_text);
    // The predicted span's text: the decoder's speculative answer draft
    // (gate open only; verified losslessly by greedy decoding).
    const span_draft: ?[]u8 = if (gate == .open and best.start <= best.end and best.end < features.copy_ids.len) draft: {
        const ids = try backing.alloc(i32, best.end + 1 - best.start);
        defer backing.free(ids);
        for (features.copy_ids[best.start .. best.end + 1], ids) |id, *slot| slot.* = @intCast(id);
        break :draft try recordText(backing, assets.tokenizer(), ids);
    } else null;
    defer if (span_draft) |text| backing.free(text);
    if (span_draft) |text| std.log.debug("memorilla_span_draft text=\"{s}\"", .{text});
    timer.mark("window");
    try control.check();
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    timer.mark("verify_paths");

    var native_guard = try control.enterUninterruptible(.uninterruptible);
    defer native_guard.deinit();

    // The released reader produces the soft rows and stays borrowed through
    // generation: its backend carries the composed memory rows (the decoder
    // compute must hold only resident weights when generation starts).
    var soft_handle = try decoder_model.memorilla_reader_cache.acquire(decoder_model.allocator, io, manager, released_directory, limits, control);
    defer soft_handle.deinit();
    var soft_clean = false;
    defer if (!soft_clean) soft_handle.invalidate();
    const reader = soft_handle.owner();
    try reader.bundle.config.validate(true);
    if (!reader.bundle.release or reader.bundle.config.num_memories != slots or reader.bundle.config.output_dim != config.hidden_size) return error.UnsupportedMemorillaProfile;
    const soft_module_sha256 = reader.bundle.weights_sha256;
    var soft_backend = try reader.computeBackendWithBudget(&soft_budget);
    soft_backend.execution_control = control;
    soft_backend.execution_budget = &soft_budget;
    var soft_scope = try execution.Scope.acquire(control, .{ .mutex = &reader.execution_mutex });
    defer soft_scope.deinit();
    var soft_physical: PhysicalBudgetScope = if (comptime @import("build_options").enable_cuda)
        try reader.compute.beginMemorillaDeviceBudget(soft_request.backendTotalBytes())
    else
        return error.CudaNotEnabled;
    defer soft_physical.deinit();
    try soft_scope.bind(soft_backend);
    const output = blk: {
        defer {
            reader.releaseRequestResources() catch @panic("Memorilla released reader request cleanup failed; worker restart required");
            soft_clean = true;
        }
        const soft_rows = inner: {
            const rows = pooled.document_count;
            const mask = try backing.alloc(bool, rows);
            defer backing.free(mask);
            @memset(mask, false);
            const docs = try soft_backend.fromFloat32Shape(pooled.documents, &.{ @intCast(rows), @intCast(qualified.width) });
            defer soft_backend.free(docs);
            const query = try soft_backend.fromFloat32Shape(pooled.question, &.{ 1, @intCast(qualified.width) });
            defer soft_backend.free(query);
            try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
            try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            const memories = try memory_pipeline.forward(&soft_backend, backing, reader.bundle.config, .{ .documents = docs, .question = query, .padding_mask = mask, .batch = 1, .num_documents = rows, .max_document_rows = qualified.max_documents }, null);
            defer soft_backend.free(memories);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            break :inner try soft_backend.toFloat32(memories, backing);
        };
        defer backing.free(soft_rows);
        if (soft_rows.len != slots * config.hidden_size) return error.InvalidMemorillaMemoryRows;
        timer.mark("soft_rows");
        try control.check();
        var decoder_backend = try factory.getCoordinatedCudaComputeBackend(decoder_model.session, &decoder_budget, @intFromPtr(&reservation));
        defer decoder_backend.deinit();
        // The window's decoder input-embedding rows (gate open only).
        const window_values: []f32 = if (gate == .closed) &.{} else rows: {
            decoder_backend.execution_control = decoder_control;
            decoder_backend.execution_budget = &decoder_budget;
            var decoder_scope = try execution.Scope.acquire(control, .{ .mutex = decoder_model.nativeGenerationMutex(), .lane = &decoder_model.native_reader_execution_lane });
            defer decoder_scope.deinit();
            try decoder_scope.bind(decoder_backend);
            try factory.requireMemorillaCudaExecution(&decoder_backend);
            const wide = try backing.alloc(i64, window_rows);
            defer backing.free(wide);
            for (record_ids, wide) |id, *w| {
                if (id < 0 or id >= config.vocab_size) return error.InvalidMemorillaTokens;
                w.* = id;
            }
            const weight = try gpt.getEmbeddingWeight(&decoder_backend, config);
            defer decoder_backend.free(weight);
            const ct = try decoder_backend.embeddingLookup(weight, wide, window_rows, config.hidden_size);
            defer decoder_backend.free(ct);
            const values = try decoder_backend.toFloat32(ct, backing);
            if (values.len != window_rows * config.hidden_size) {
                backing.free(values);
                return error.UnexpectedEmbeddingTable;
            }
            try factory.requireMemorillaCudaExecution(&decoder_backend);
            break :rows values;
        };
        defer if (gate == .open) backing.free(window_values);
        const memory_rows: usize = if (gate == .open) open_rows else slots;
        const composed = try backing.alloc(f32, memory_rows * config.hidden_size);
        defer backing.free(composed);
        @memcpy(composed[0 .. slots * config.hidden_size], soft_rows);
        if (gate == .open) @memcpy(composed[slots * config.hidden_size ..], window_values);
        if (gate == .closed) {
            reservation.deinit();
            reservation = try closed_plan.reserve(&decoder_budget);
        }
        const text = if (gate == .open) &open_text else &closed_text;
        const rows_ct = try soft_backend.fromFloat32Shape(composed, &.{ @intCast(memory_rows), @intCast(config.hidden_size) });
        defer soft_backend.free(rows_ct);
        timer.mark("window_rows");
        var decoder = try decode.Decoder.fromQualifiedSession(decoder_model.session, &decoder_backend, assets.tokenizer(), decoder_model.nativeGenerationMutex());
        decoder.execution_lane = &decoder_model.native_reader_execution_lane;
        defer timer.mark("generate");
        break :blk try decode.generate(a, decoder, text, .{ .backend = &soft_backend, .tensor = rows_ct }, .{ .max_new_tokens = max_new_tokens, .teacher_forced_ids = options.teacher_forced_ids, .trace = options.trace, .ignore_eos = options.ignore_eos, .draft_text = span_draft, .draft_context = if (gate == .open) record_text else null }, &reservation, bounded, decoder_control);
    };
    var result = output;
    errdefer result.deinit();
    try soft_handle.verify(control);
    factory.logMemorillaOpProfile(decoder_model.session, "generate");

    const raw_text = try a.dupe(u8, pythonStrip(result.text));
    errdefer a.free(raw_text);
    const published = if (gate == .open)
        try canonicalizeAnswer(result.allocator, raw_text, record_text)
    else
        try result.allocator.dupe(u8, raw_text);
    result.allocator.free(result.text);
    result.text = published;
    const answer_guard = guardAnswer(result.text);
    if (answer_guard == .rejected) try research_service.abstainGeneration(&result);
    const canonicalized = answer_guard == .accepted and !std.mem.eql(u8, result.text, raw_text);
    var citations = if (gate == .open)
        try copy.buildCitations(a, &.{span_window.windowSpan(window)}, features.sources, read_documents, research_service.citedAnswer(result.text, answer_guard))
    else
        copy.Citations.none(a, false);
    errdefer citations.deinit();
    // Diagnostics: the span and the window as regions of the caller's
    // documents, whether or not the decoder read the window.
    var span_regions = try copy.buildCitations(a, &.{spanRegion(best)}, features.sources, read_documents, null);
    errdefer span_regions.deinit();
    var window_regions = try copy.buildCitations(a, &.{span_window.windowSpan(window)}, features.sources, read_documents, null);
    errdefer window_regions.deinit();
    const owned_record_text = try a.dupe(u8, record_text);
    errdefer a.free(owned_record_text);
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    try control.check();
    try notify(options, .decoded, manager);
    try control.check();
    transfer_response = true;
    return .{
        .allocator = a,
        .generation = result,
        .answer_guard = answer_guard,
        .record_gate = gate,
        .canonicalized = canonicalized,
        .memory_tokens = if (gate == .open) open_rows else slots,
        .encoder_execution_identity = qualified.executionIdentity(encoder_profile),
        .decoder_execution_identity = qualified.executionIdentity(decoder_profile),
        .module_sha256 = head_sha256,
        .soft_module_sha256 = soft_module_sha256,
        .candidate_sha256 = pins.?.candidate,
        .adapter_sha256 = pins.?.adapter,
        .document_count = documents.len,
        .soft_rows = pooled.document_count,
        .question_tokens = preflight.question_tokens,
        .citations = citations,
        .record_text = owned_record_text,
        .raw_text = raw_text,
        .min_snap_cosine = 1,
        .span = .{
            .start = best.start,
            .end = best.end,
            .window_lo = window.lo,
            .window_hi = window.hi,
            .score = best.score,
            .null_score = best.null_score,
            .margin = margin,
            .gate_policy = policy,
            .gate_offset = decision.offset,
            .gate_profile = gate_profile,
            .gate_similarity = decision.similarity,
            .gate_fallback = decision.fallback,
            .head_ms = head_ms,
            .read_mode = mode,
            .read_pass = if (cascade) chosen_pass else null,
            .cascade_threshold = if (cascade) protocol.read_cascade.?.threshold else null,
            .narrow_margin = if (cascade) if (reads[narrow_index]) |read| read.margin else null else null,
            .wide_margin = if (reads[wide_index]) |read| read.margin else null,
            .narrow_unavailable = if (preflight.narrow_error) |err| @errorName(err) else null,
            .wide_unavailable = if (preflight.wide_error) |err| @errorName(err) else null,
            .read_documents = read_plan.documents,
            .read_tokens = features.copy_ids.len,
            .read_budget_tokens = read_plan.budget_tokens,
            .window_rows = window_rows,
        },
        .span_regions = span_regions,
        .window_regions = window_regions,
        .timings = timer.timings,
        .response_lease = response_lease,
    };
}

/// The predicted inclusive span as one citable region (`copy.buildCitations`
/// input), in copy-id positions like the window.
pub fn spanRegion(best: span_window.Span) copy.CompletedSpan {
    const last = @max(best.end + 1, best.start);
    return .{
        .span = .{ .begin = 0, .end = last - best.start, .start = best.start },
        .first = best.start,
        .last = last,
        .slot_begin = 0,
        .slot_end = last - best.start,
    };
}

/// The bundle's pinned digests a soft-only request reports in place of the
/// span pass's (`files["span-head.safetensors"]`, `source.candidate_sha256`,
/// `files["encoder-lora.safetensors"]` of `manifest.json`); the caches verify
/// the same pins whenever a span pass loads them.
const BundlePins = struct { head: [64]u8, candidate: [64]u8, adapter: [64]u8 };

fn bundlePins(a: A, directory: []const u8) !BundlePins {
    const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
    defer a.free(manifest_path);
    const manifest_bytes = try c_file.readFileMax(a, manifest_path, 1024 * 1024);
    defer a.free(manifest_bytes);
    var manifest = std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{}) catch return error.InvalidMemorillaManifest;
    defer manifest.deinit();
    const Pin = struct {
        fn hex(value: ?std.json.Value) ![64]u8 {
            const text = value orelse return error.InvalidMemorillaManifest;
            if (text != .string or text.string.len != 64) return error.InvalidMemorillaManifest;
            for (text.string) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return error.InvalidMemorillaManifest;
            return text.string[0..64].*;
        }
        fn file(root: std.json.Value, name: []const u8) ![64]u8 {
            if (root != .object) return error.InvalidMemorillaManifest;
            const files = root.object.get("files") orelse return error.InvalidMemorillaManifest;
            if (files != .object) return error.InvalidMemorillaManifest;
            const entry = files.object.get(name) orelse return error.InvalidMemorillaManifest;
            if (entry != .object) return error.InvalidMemorillaManifest;
            return hex(entry.object.get("sha256"));
        }
    };
    const source = if (manifest.value == .object) manifest.value.object.get("source") else null;
    const candidate = if (source) |value| (if (value == .object) value.object.get("candidate_sha256") else null) else null;
    return .{
        .head = try Pin.file(manifest.value, "span-head.safetensors"),
        .candidate = try Pin.hex(candidate),
        .adapter = try Pin.file(manifest.value, "encoder-lora.safetensors"),
    };
}

/// `span_soft_only` (HTTP `read_mode: none`) on a span bundle: no span pass.
/// The decoder reads the 16 soft rows alone under the released helpful
/// prompt (or the benchmark prompt), which is `generateSpanImpl`'s
/// closed-gate path step for step: the same preflight of the pooled texts,
/// prompt bound, pooled vectors, released reader and greedy decoding without
/// a draft. It is stock Memorilla over the same pooled vectors, for
/// side-by-side comparison with the hybrid. Only the closed plan is
/// admitted; nothing of the record half (adapters, span head) is touched.
fn generateSoftOnlyImpl(a: A, io: std.Io, manager: *manager_mod.ModelManager, profile: Profile, paths: Paths, canonical: []const u8, documents: []const []const u8, max_new_tokens: usize, limits: memory.Limits, control: Control, options: Options, protocol: span_window.Protocol, released_directory: []const u8) !OwnedResult {
    // Exclusive with a read mode; stored soft-half vectors are a cascade
    // research input this comparison does not take.
    if (options.span_read_mode != null or options.stored_documents != null or options.stored_question != null) return error.UnsupportedMemorillaReadMode;
    const encoder_control = paths.encoder.bind(control);
    const decoder_control = paths.decoder.bind(control);

    var response_lease = try manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = 32 * 1024 * 1024 });
    var transfer_response = false;
    defer if (!transfer_response) response_lease.release();
    const backing = platform.allocator.reclaimingScratch(a);
    const pins = try bundlePins(backing, profile.memory_module_path);

    const closed_system: []const u8 = if (options.span_bench_prompt) |bench| bench.system else helpful_prompt;
    const bench_user: ?[]u8 = if (options.span_bench_prompt) |bench| try std.mem.concat(backing, u8, &.{ canonical, bench.user_suffix }) else null;
    defer if (bench_user) |text| backing.free(text);
    const user_text: []const u8 = if (bench_user) |text| pythonStrip(text) else canonical;

    var preflight = try preflightSpan(backing, io, manager, paths.encoder, canonical, documents, &protocol, null, true, false, encoder_control);
    defer preflight.deinit();
    try common.validateInputTokenLimit(options.request_limits, preflight.question_tokens);

    var assets = try common.acquireGenerationAssets(backing, io, manager, paths.decoder, decoder_profile, decoder_control);
    defer assets.deinit();
    const config = try assets.decoderConfig(backing, paths.decoder.path);
    // Bounded as the span path bounds it, so the closed prompt and plan are
    // prepared exactly as on a closed gate.
    var bounded = decode.Limits{ .max_new_tokens = profile.max_new_tokens };
    bounded.max_memory_rows = @max(bounded.max_memory_rows, slots + protocol.maxWindowTokens());
    const template = assets.chatTemplate() orelse return error.MemorillaChatTemplateRequired;
    var closed_text = try decode.prepareText(backing, assets.tokenizer(), template, config, user_text, .{ .system_prompt = closed_system, .memory_rows = slots }, bounded, decoder_control);
    defer closed_text.deinit();
    try common.validateInputTokenLimit(options.request_limits, closed_text.input_ids.len);
    if (options.prepared_text) |callback| try callback(options.context, &closed_text);
    const closed_plan = try decode.ResourcePlan.create(config, .cuda, closed_text.input_ids.len, closed_text.rendered.len, slots, max_new_tokens, bounded);

    // Admission: the closed decoder plan; the released reader over the
    // pooled rows plus the 16 memory rows it uploads.
    const soft_request = memory.AdmissionAmounts{
        .host_scratch_bytes = try add(16 * 1024 * 1024, try mul(slots, config.hidden_size * @sizeOf(f32))),
        .backend_scratch_bytes = try add(try add(128 * 1024 * 1024, try mul(qualified.max_documents, qualified.width * 16 * @sizeOf(f32))), try mul(slots, config.hidden_size * @sizeOf(f32))),
    };
    const decoder_request = try memory.AdmissionAmounts.fromEstimate(closed_plan.decoder).merge(.fromEstimate(closed_plan.host));
    const request_amounts = try decoder_request.merge(soft_request);
    var request_lease = try manager.acquireRunResourceAmounts(.gpu, limits, request_amounts);
    defer request_lease.release();
    var decoder_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, decoder_request));
    var soft_budget = memory.RunBudget.init(try common.requestBudgetLimits(limits, soft_request));
    var reservation = try closed_plan.reserve(&decoder_budget);
    defer reservation.deinit();
    const soft_backend_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = soft_request.backend_scratch_bytes, .scratch_tier = .backend };
    try soft_budget.reserveEstimate(soft_backend_estimate);
    defer soft_budget.releaseEstimate(soft_backend_estimate);
    const soft_host_estimate = memory.Estimate{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = soft_request.host_scratch_bytes, .scratch_tier = .host };
    try soft_budget.reserveEstimate(soft_host_estimate);
    defer soft_budget.releaseEstimate(soft_host_estimate);
    try notify(options, .prepared, manager);

    var encoder_handle = try common.acquireResidentModel(manager, paths.encoder.path, encoder_control);
    defer encoder_handle.release();
    const encoder_model = encoder_handle.get();
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try notify(options, .encoder_loaded, manager);
    var timer = SpanTimer.start(io);
    var pooled = try encoder.encode(backing, &encoder_handle, preflight.soft_texts, canonical, limits, encoder_control);
    defer pooled.deinit();
    timer.mark("pooled");
    if (!std.mem.eql(u8, pooled.question_text, canonical)) return error.NonCanonicalMemorillaQuestion;
    try encoder.requireProducer(pooled.producer, paths.encoder.publication.?, encoder_profile);
    if (pooled.question_tokens != preflight.question_tokens or pooled.document_count != preflight.soft_texts.len) return error.InvalidMemorillaTokens;
    try notify(options, .question_encoded, manager);
    try control.check();

    var decoder_handle = try common.acquireResidentModel(manager, paths.decoder.path, decoder_control);
    defer decoder_handle.release();
    const decoder_model = decoder_handle.get();
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try notify(options, .decoder_loaded, manager);
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);

    var native_guard = try control.enterUninterruptible(.uninterruptible);
    defer native_guard.deinit();

    // The released reader, borrowed through generation as on the span path.
    var soft_handle = try decoder_model.memorilla_reader_cache.acquire(decoder_model.allocator, io, manager, released_directory, limits, control);
    defer soft_handle.deinit();
    var soft_clean = false;
    defer if (!soft_clean) soft_handle.invalidate();
    const reader = soft_handle.owner();
    try reader.bundle.config.validate(true);
    if (!reader.bundle.release or reader.bundle.config.num_memories != slots or reader.bundle.config.output_dim != config.hidden_size) return error.UnsupportedMemorillaProfile;
    const soft_module_sha256 = reader.bundle.weights_sha256;
    var soft_backend = try reader.computeBackendWithBudget(&soft_budget);
    soft_backend.execution_control = control;
    soft_backend.execution_budget = &soft_budget;
    var soft_scope = try execution.Scope.acquire(control, .{ .mutex = &reader.execution_mutex });
    defer soft_scope.deinit();
    var soft_physical: PhysicalBudgetScope = if (comptime @import("build_options").enable_cuda)
        try reader.compute.beginMemorillaDeviceBudget(soft_request.backendTotalBytes())
    else
        return error.CudaNotEnabled;
    defer soft_physical.deinit();
    try soft_scope.bind(soft_backend);
    const output = blk: {
        defer {
            reader.releaseRequestResources() catch @panic("Memorilla released reader request cleanup failed; worker restart required");
            soft_clean = true;
        }
        const soft_rows = inner: {
            const rows = pooled.document_count;
            const mask = try backing.alloc(bool, rows);
            defer backing.free(mask);
            @memset(mask, false);
            const docs = try soft_backend.fromFloat32Shape(pooled.documents, &.{ @intCast(rows), @intCast(qualified.width) });
            defer soft_backend.free(docs);
            const query = try soft_backend.fromFloat32Shape(pooled.question, &.{ 1, @intCast(qualified.width) });
            defer soft_backend.free(query);
            try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
            try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            const memories = try memory_pipeline.forward(&soft_backend, backing, reader.bundle.config, .{ .documents = docs, .question = query, .padding_mask = mask, .batch = 1, .num_documents = rows, .max_document_rows = qualified.max_documents }, null);
            defer soft_backend.free(memories);
            try factory.requireMemorillaCudaExecution(&soft_backend);
            break :inner try soft_backend.toFloat32(memories, backing);
        };
        defer backing.free(soft_rows);
        if (soft_rows.len != slots * config.hidden_size) return error.InvalidMemorillaMemoryRows;
        timer.mark("soft_rows");
        try control.check();
        var decoder_backend = try factory.getCoordinatedCudaComputeBackend(decoder_model.session, &decoder_budget, @intFromPtr(&reservation));
        defer decoder_backend.deinit();
        const rows_ct = try soft_backend.fromFloat32Shape(soft_rows, &.{ @intCast(slots), @intCast(config.hidden_size) });
        defer soft_backend.free(rows_ct);
        var decoder = try decode.Decoder.fromQualifiedSession(decoder_model.session, &decoder_backend, assets.tokenizer(), decoder_model.nativeGenerationMutex());
        decoder.execution_lane = &decoder_model.native_reader_execution_lane;
        defer timer.mark("generate");
        break :blk try decode.generate(a, decoder, &closed_text, .{ .backend = &soft_backend, .tensor = rows_ct }, .{ .max_new_tokens = max_new_tokens, .teacher_forced_ids = options.teacher_forced_ids, .trace = options.trace, .ignore_eos = options.ignore_eos }, &reservation, bounded, decoder_control);
    };
    var result = output;
    errdefer result.deinit();
    try soft_handle.verify(control);
    factory.logMemorillaOpProfile(decoder_model.session, "generate");

    // Published as a closed gate publishes: the stripped answer, no
    // canonicalization, the frame-echo guard, no citations.
    const raw_text = try a.dupe(u8, pythonStrip(result.text));
    errdefer a.free(raw_text);
    const published = try result.allocator.dupe(u8, raw_text);
    result.allocator.free(result.text);
    result.text = published;
    const answer_guard = guardAnswer(result.text);
    if (answer_guard == .rejected) try research_service.abstainGeneration(&result);
    var citations = copy.Citations.none(a, false);
    errdefer citations.deinit();
    try encoder_model.verifyMemorillaIdentityWithControl(encoder_control);
    try decoder_model.verifyMemorillaIdentityWithControl(decoder_control);
    try paths.encoder.verify(a, io);
    try paths.decoder.verify(a, io);
    try control.check();
    try notify(options, .decoded, manager);
    try control.check();
    transfer_response = true;
    return .{
        .allocator = a,
        .generation = result,
        .answer_guard = answer_guard,
        .record_gate = .closed,
        .canonicalized = false,
        .memory_tokens = slots,
        .encoder_execution_identity = qualified.executionIdentity(encoder_profile),
        .decoder_execution_identity = qualified.executionIdentity(decoder_profile),
        .module_sha256 = pins.head,
        .soft_module_sha256 = soft_module_sha256,
        .candidate_sha256 = pins.candidate,
        .adapter_sha256 = pins.adapter,
        .document_count = documents.len,
        .soft_rows = pooled.document_count,
        .question_tokens = preflight.question_tokens,
        .citations = citations,
        .record_text = &.{},
        .raw_text = raw_text,
        .min_snap_cosine = 1,
        .timings = timer.timings,
        .soft_only = true,
        .response_lease = response_lease,
    };
}

// --- tests ----------------------------------------------------------------------

test "Memorilla hybrid canonicalization mirrors the evaluator cases" {
    const a = std.testing.allocator;
    const cases = [_][3][]const u8{
        // (a) case flips take the record's casing.
        .{ "WZ3-003545", " code Wz3-003545 at Dociru Depot", "Wz3-003545" },
        .{ "hidehi hokifu", " key holder Hidehi Hokifu at Kopene Depot ", "Hidehi Hokifu" },
        // (b) prefix drops grow back to the record word (hyphen ignored).
        .{ "3005601", " code Te3-005601 at Zorutu ", "Te3-005601" },
        .{ "4672", " code Xd4-004672 at Kuruli Depot ", "Xd4-004672" },
        .{ "001618", " code Pp7-001618 at Soceso Depot ", "Pp7-001618" },
        // A whole-word match is left as the record spells it, never extended.
        .{ "Hokifu", " key holder Hidehi Hokifu at Kopene Depot ", "Hokifu" },
        // Bare digits never grow into a record word (three-character tail);
        // the shortest real prefix drop still completes.
        .{ "4", " code holder4 10 and 50 centavos ", "4" },
        .{ "0", " code UNKNOWN at 12-10 with under 2 ", "0" },
        .{ "10", " code UNKNOWN at 12-10 with under 2 ", "10" },
        .{ "672", " code Xd4-004672 at Kuruli Depot ", "Xd4-004672" },
        // Multi-word tails complete to the word sequence.
        .{ "Fihunu at Kopene", " key holder Faketa Fihunu at Kopene Depot ", "Fihunu at Kopene" },
        .{ "Tuloco", " key holder Komode Tuloco ", "Tuloco" },
        // Free-form words never grow into longer words.
        .{ "yes", " code holder Hayes at 1 or 0 ", "yes" },
        .{ "France", " key holder region in France ", "France" },
        // No relation to the record: unchanged (whitespace stripped).
        .{ "JESAVO DEGEBE", " key holder Jesavo Degebo at Gihera ", "JESAVO DEGEBE" },
        .{ " Rome ", " code holder at 1060s ", "Rome" },
        .{ "UNKNOWN", " code Te3-005601 ", "UNKNOWN" },
        .{ "", " code Te3-005601 ", "" },
        .{ "-", " code Te3-005601 ", "-" },
    };
    for (cases) |case| {
        const got = try canonicalizeAnswer(a, case[0], case[1]);
        defer a.free(got);
        std.testing.expectEqualStrings(case[2], got) catch |err| {
            std.debug.print("canonicalize({s}, {s})\n", .{ case[0], case[1] });
            return err;
        };
    }
}

test "Memorilla hybrid canonicalization handles multi-word tails and Unicode whitespace" {
    const a = std.testing.allocator;
    // A digit-carrying tail may start inside a word and spans up to three words.
    const coded = try canonicalizeAnswer(a, "9-009369 at Pipofo", " code Kg9-009369 at Pipofo Depot ");
    defer a.free(coded);
    try std.testing.expectEqualStrings("Kg9-009369 at Pipofo", coded);
    // Without a digit the tail must start at a word start.
    const named = try canonicalizeAnswer(a, "ode Tuloco", " key holder Komode Tuloco ");
    defer a.free(named);
    try std.testing.expectEqualStrings("ode Tuloco", named);
    // Python strip removes Unicode whitespace around the answer.
    const spaced = try canonicalizeAnswer(a, "\u{a0}Kg9-009369\u{3000}", " code Kg9-009369 ");
    defer a.free(spaced);
    try std.testing.expectEqualStrings("Kg9-009369", spaced);
    try std.testing.expectEqualStrings("x y", pythonStrip("\u{2003}x y\u{85}"));
    try std.testing.expectEqualStrings("", pythonStrip(" \t\n"));
}

test "Memorilla hybrid record gate trusts a clean copied span" {
    var cosines: [16]f32 = @splat(1.0);
    // No copied span: the soft rows go alone.
    try std.testing.expect(!try recordGateOpen(&.{}, &cosines, record_gate_cosine));
    // A clean span opens the gate.
    try std.testing.expect(try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &cosines, record_gate_cosine));
    // A fence-sitting slot inside the only span closes it.
    var fence = cosines;
    fence[4] = 0.7;
    try std.testing.expect(!try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &fence, record_gate_cosine));
    // Another clean span is enough.
    try std.testing.expect(try recordGateOpen(&.{ .{ .begin = 2, .end = 6, .start = 0 }, .{ .begin = 8, .end = 10, .start = 20 } }, &fence, record_gate_cosine));
    // Low cosines outside the spans (generated label slots) do not matter.
    var noisy: [16]f32 = @splat(0.3);
    noisy[2] = 0.85;
    noisy[3] = 0.9;
    noisy[4] = 1.0;
    noisy[5] = 0.86;
    try std.testing.expect(try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &noisy, record_gate_cosine));
    try std.testing.expect(!try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &noisy, 0.9));
    // The evaluator compares four-decimal cosines: 0.84996 counts as 0.85.
    noisy[5] = 0.84996;
    try std.testing.expect(try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &noisy, record_gate_cosine));
    noisy[5] = 0.8494;
    try std.testing.expect(!try recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &noisy, record_gate_cosine));
    try std.testing.expectError(error.InvalidCopySpan, recordGateOpen(&.{.{ .begin = 10, .end = 17, .start = 0 }}, &cosines, record_gate_cosine));
    try std.testing.expectError(error.InvalidRecordGate, recordGateOpen(&.{.{ .begin = 2, .end = 6, .start = 0 }}, &cosines, 1.5));
}

test "Memorilla hybrid guard rejects only frame echo" {
    // General answers: sentences, punctuation and length are fine.
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("The Normans were in Normandy in the 10th and 11th centuries, according to the passage."));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("Denmark, Iceland and Norway"));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("\"Rollo\" (c. 860 - c. 930)"));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("Kg9-009369"));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer(" UNKNOWN\n"));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("Memoryville"));
    try std.testing.expectEqual(AnswerGuard.accepted, guardAnswer("Zürich"));
    // Frame echo.
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer("Memory key holder with the brass key for Zehisi Depot  \nMemory"));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer("Zehisi Memory"));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer("<|memory|>Alice"));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer("First line\nsecond line"));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer("tab\tseparated"));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer(""));
    try std.testing.expectEqual(AnswerGuard.rejected, guardAnswer(" \t\n"));
}

test "Memorilla hybrid merged prompt and lenient record pieces" {
    const a = std.testing.allocator;
    const merged = try mergedSystemPrompt(a, "Answer using the supplied memory.");
    defer a.free(merged);
    try std.testing.expectEqualStrings("You are a helpful assistant. Answer using the supplied memory.", merged);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    defer out.deinit(a);
    try appendLossy(a, &out, " Z\xc3\xbcrich");
    try appendLossy(a, &out, "\xe2\x80");
    try appendLossy(a, &out, "\x80x");
    try std.testing.expectEqualStrings(" Zürich\u{fffd}\u{fffd}x", out.items);
}

test "Memorilla hybrid admission covers either decoder plan" {
    const open = memory.AdmissionAmounts{ .host_scratch_bytes = 10, .backend_kv_bytes = 30, .backend_scratch_bytes = 5 };
    const closed = memory.AdmissionAmounts{ .host_scratch_bytes = 12, .backend_kv_bytes = 20, .backend_scratch_bytes = 5 };
    const both = maxAmounts(open, closed);
    try std.testing.expectEqual(@as(usize, 12), both.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 30), both.backend_kv_bytes);
    try std.testing.expectEqual(@as(usize, 5), both.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), both.host_weight_bytes);
}

test "Memorilla hybrid validation rejects kind identity bounds and texts before models" {
    const research_kind = Profile{ .name = "copy17", .encoder = "encoder", .decoder = "decoder", .memory_module_path = "bundle", .reader_kind = .encoder_lora_research };
    var p = research_kind;
    p.reader_kind = .hybrid_research;
    p.released_memory_module_path = "stage3";
    const identity = qualified.executionIdentity(encoder_profile);
    const docs = [_][]const u8{ "Alice Smith holds the brass key for Mozeba Depot.", "The Normans gave their name to Normandy." };
    try validateInputs(p, "\xc2\xa0question\xc2\xa0", &docs, &identity, 48);
    try std.testing.expectError(error.UnsupportedMemorillaProfile, validateInputs(research_kind, "q", &docs, &identity, 24));
    try std.testing.expectError(error.EmptyMemorillaCollection, validateInputs(p, "q", &.{}, &identity, 1));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, validateInputs(p, "q", &docs, "other", 1));
    // The research encoder identity is not the stock encoder that serves both passes.
    try std.testing.expectError(error.IncompatibleMemorillaProducer, validateInputs(p, "q", &docs, &qualified.executionIdentity(.qwen3_encoder_lora_research_f32_v1), 1));
    try std.testing.expectError(error.MemorillaGenerationLimitExceeded, validateInputs(p, "q", &docs, &identity, 129));
    try std.testing.expectError(error.InvalidMemorillaSourceText, validateInputs(p, "q", &.{" \n"}, &identity, 1));
    const long: [8193]u8 = @splat('a');
    try std.testing.expectError(error.MemorillaSourceTextLimitExceeded, validateInputs(p, "q", &.{&long}, &identity, 1));
    try std.testing.expectError(error.MemorillaTextLimitExceeded, validateInputs(p, &long, &docs, &identity, 1));
    var small = p;
    small.max_documents = 1;
    try std.testing.expectError(error.MemorillaCollectionBudgetExceeded, validateInputs(small, "q", &docs, &identity, 1));
    var missing = p;
    missing.released_memory_module_path = null;
    try std.testing.expectError(error.InvalidMemorillaConfig, validateInputs(missing, "q", &docs, &identity, 1));
}

test "Memorilla hybrid cascade skips a read only past a per-request bound" {
    // A read set past the encoder bound (or whose question prefix changes its
    // tail ids) leaves the other read; resource and model errors fail the
    // request as before.
    try std.testing.expect(recoverableRead(error.MemorillaEncoderBoundExceeded));
    try std.testing.expect(recoverableRead(error.MemorillaQuestionPrefixMismatch));
    for ([_]anyerror{ error.OutOfMemory, error.ResourceLimitExceeded, error.InvalidMemorillaTokens, error.MemorillaCollectionBudgetExceeded, error.Timeout }) |err|
        try std.testing.expect(!recoverableRead(err));
}

test "Memorilla hybrid span bundles are detected from the pinned protocol" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..] });
    defer a.free(dir);
    const digest = @import("../models/memorilla.zig").digest;
    const Write = struct {
        fn bundle(allocator: A, dir_io: std.Io, d: std.Io.Dir, protocol: []const u8, pinned: []const u8) !void {
            try d.writeFile(dir_io, .{ .sub_path = "protocol.json", .data = protocol });
            var manifest: std.ArrayList(u8) = .empty;
            defer manifest.deinit(allocator);
            try manifest.print(allocator, "{{\"files\":{{\"protocol.json\":{{\"sha256\":\"{s}\",\"size_bytes\":{d}}}}}}}", .{ &digest(pinned), pinned.len });
            try d.writeFile(dir_io, .{ .sub_path = "manifest.json", .data = manifest.items });
        }
    };
    const copy17 = "{\"system_prompt\":\"x\"}";
    try Write.bundle(a, io, temp.dir, copy17, copy17);
    try std.testing.expect((try readSpanProtocol(a, dir)) == null);
    const span_json =
        \\{"record_kind":"span-window","document_question_prefix":"question-passage-v1","span_head":{"width":5120,"hidden":1024,"layers":2,"heads":8,"max_positions":2048,"max_documents":4},"span_window_tokens":32,"span_window_left_fraction":0.65,"span_max_tokens":30,"span_gate_threshold":-3.8,"span_gate_policy":"learned"}
    ;
    try Write.bundle(a, io, temp.dir, span_json, span_json);
    const protocol = (try readSpanProtocol(a, dir)).?;
    try std.testing.expectEqual(span_window.GatePolicy.learned, protocol.gate_policy);
    try std.testing.expectEqual(@as(usize, 32), protocol.window_tokens);
    // A protocol that is not the pinned one is refused.
    try Write.bundle(a, io, temp.dir, span_json, copy17);
    try std.testing.expectError(error.MemorillaArtifactMismatch, readSpanProtocol(a, dir));
    // The read cascade (plan 8.32) is bundle data; the per-route read budget
    // it replaced is refused.
    const cascade_json =
        \\{"record_kind":"span-window","document_question_prefix":"question-passage-v1","span_head":{"width":5120,"hidden":1024,"layers":2,"heads":8,"max_positions":2048,"max_documents":4},"span_window_tokens":32,"span_window_left_fraction":0.65,"span_max_tokens":30,"span_gate_threshold":-3.8,"span_gate_policy":"learned","span_read_cascade":{"threshold":-4.0,"read_tokens":512,"window_tokens":64}}
    ;
    try Write.bundle(a, io, temp.dir, cascade_json, cascade_json);
    const cascade = (try readSpanProtocol(a, dir)).?;
    try std.testing.expectEqual(span_window.ReadCascade{ .threshold = -4, .read_tokens = 512, .window_tokens = 64 }, cascade.read_cascade.?);
    try std.testing.expectEqual(@as(?span_window.ReadMode, .auto), try cascade.readMode(null));
    try std.testing.expectEqual(@as(?span_window.ReadMode, null), try protocol.readMode(null));
    try std.testing.expectError(error.UnsupportedMemorillaReadMode, protocol.readMode(.wide));
    const routed_json =
        \\{"record_kind":"span-window","document_question_prefix":"question-passage-v1","span_head":{"width":5120,"hidden":1024,"layers":2,"heads":8,"max_positions":2048,"max_documents":4},"span_window_tokens":32,"span_window_left_fraction":0.65,"span_max_tokens":30,"span_gate_threshold":-3.8,"span_gate_routes":{"profiles":["triviaqa"]},"span_read_routes":{"triviaqa":{"read_tokens":512,"window_tokens":64}}}
    ;
    try Write.bundle(a, io, temp.dir, routed_json, routed_json);
    try std.testing.expectError(error.UnsupportedMemorillaProfile, readSpanProtocol(a, dir));
}

test "Memorilla hybrid span timings group the timer stages" {
    var timings = SpanTimings{};
    for ([_]struct { []const u8, f64 }{
        .{ "pooled", 30 },   .{ "span_features", 60 }, .{ "span_head", 5 },  .{ "wide_features", 100 },
        .{ "wide_head", 6 }, .{ "read_plan", 0.5 },    .{ "window", 0.25 },  .{ "verify_paths", 0.25 },
        .{ "soft_rows", 4 }, .{ "window_rows", 1 },    .{ "generate", 200 }, .{ "generate", 10 },
    }) |entry| timings.add(entry[0], entry[1]);
    try std.testing.expectEqual(@as(f64, 30), timings.pooled_ms);
    try std.testing.expectEqual(@as(f64, 60), timings.span_features_ms);
    try std.testing.expectEqual(@as(f64, 5), timings.span_head_ms);
    try std.testing.expectEqual(@as(f64, 100), timings.wide_features_ms);
    try std.testing.expectEqual(@as(f64, 6), timings.wide_head_ms);
    // Read plan, window, path checks and the window rows.
    try std.testing.expectEqual(@as(f64, 2), timings.window_ms);
    try std.testing.expectEqual(@as(f64, 4), timings.soft_rows_ms);
    try std.testing.expectEqual(@as(f64, 210), timings.generate_ms);
    try std.testing.expectEqual(@as(f64, 0), timings.total_ms);
}

test "Memorilla hybrid span and window regions map copy positions to document bytes" {
    const a = std.testing.allocator;
    const documents = [_][]const u8{ "Site Alpha.", "Gate code Kq9-0012 opens the yard." };
    // Copy positions: 0..2 cover document 0, 3..9 document 1 (one range per piece).
    const sources = [_]copy.SourceRange{
        .{ .document = 0, .start = 0, .end = 4 },   .{ .document = 0, .start = 4, .end = 10 },  .{ .document = 0, .start = 10, .end = 11 },
        .{ .document = 1, .start = 0, .end = 4 },   .{ .document = 1, .start = 4, .end = 9 },   .{ .document = 1, .start = 9, .end = 14 },
        .{ .document = 1, .start = 14, .end = 18 }, .{ .document = 1, .start = 18, .end = 24 }, .{ .document = 1, .start = 24, .end = 28 },
        .{ .document = 1, .start = 28, .end = 34 },
    };
    // The inclusive span 5..6 is " Kq9-" + "0012" (bytes 9..18, trimmed to 10..18).
    const region = spanRegion(.{ .start = 5, .end = 6, .score = 1, .null_score = 0 });
    try std.testing.expectEqual(@as(usize, 5), region.first);
    try std.testing.expectEqual(@as(usize, 7), region.last);
    var span = try copy.buildCitations(a, &.{region}, &sources, &documents, null);
    defer span.deinit();
    try std.testing.expect(!span.unavailable);
    try std.testing.expectEqual(@as(usize, 1), span.items.len);
    try std.testing.expectEqual(@as(usize, 1), span.items[0].document_index);
    try std.testing.expectEqualStrings("Kq9-0012", span.items[0].text);
    try std.testing.expectEqual(@as(usize, 10), span.items[0].start);
    try std.testing.expectEqual(@as(usize, 18), span.items[0].end);
    try std.testing.expect(!span.items[0].supports_answer);
    var window = try copy.buildCitations(a, &.{span_window.windowSpan(.{ .lo = 3, .hi = 10 })}, &sources, &documents, null);
    defer window.deinit();
    try std.testing.expectEqual(@as(usize, 1), window.items.len);
    try std.testing.expectEqualStrings(documents[1], window.items[0].text);
    // A one-token span; an unmappable source map is reported, not an error.
    const single = spanRegion(.{ .start = 2, .end = 2, .score = 1, .null_score = 0 });
    try std.testing.expectEqual(@as(usize, 3), single.last);
    var missing = try copy.buildCitations(a, &.{single}, null, &documents, null);
    defer missing.deinit();
    try std.testing.expect(missing.unavailable);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "Memorilla hybrid soft-only requests report the bundle's pinned digests" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..] });
    defer a.free(dir);
    const head: [64]u8 = @splat('1');
    const candidate: [64]u8 = @splat('2');
    const adapter: [64]u8 = @splat('3');
    const manifest = try std.fmt.allocPrint(a, "{{\"source\":{{\"candidate_sha256\":\"{s}\"}},\"files\":{{\"span-head.safetensors\":{{\"sha256\":\"{s}\",\"size_bytes\":1}},\"encoder-lora.safetensors\":{{\"sha256\":\"{s}\",\"size_bytes\":1}}}}}}", .{ &candidate, &head, &adapter });
    defer a.free(manifest);
    try temp.dir.writeFile(io, .{ .sub_path = "manifest.json", .data = manifest });
    const pins = try bundlePins(a, dir);
    try std.testing.expectEqualStrings(&head, &pins.head);
    try std.testing.expectEqualStrings(&candidate, &pins.candidate);
    try std.testing.expectEqualStrings(&adapter, &pins.adapter);
    // A bundle without a span head (or a malformed digest) is refused.
    try temp.dir.writeFile(io, .{ .sub_path = "manifest.json", .data = "{\"source\":{\"candidate_sha256\":\"xyz\"},\"files\":{}}" });
    try std.testing.expectError(error.InvalidMemorillaManifest, bundlePins(a, dir));
}

test "Memorilla hybrid service cancellation precedes component resolution and model loading" {
    try std.testing.expectError(error.Timeout, generate(std.testing.allocator, std.testing.io, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, .{}, .{ .deadline_ns = 0 }, .{}));
}

/// Word-level stand-in tokenizer for the chunker: every whitespace-separated
/// word (punctuation attached) is one token.
const WordCountTokenizer = struct {
    var sentinel: u8 = 0;
    fn handle() tok.Tokenizer {
        return .{ .ptr = &sentinel, .vtable = &.{ .encode = encode, .encodeInto = append, .encodeForModel = unused, .encodeGeneration = unusedGeneration, .decode = decodeIds, .specialTokens = specials, .vocabSize = size, .deinit = deinit } };
    }
    fn encode(_: *anyopaque, a: A, text: []const u8) anyerror![]i32 {
        var count: usize = 0;
        var it = std.mem.tokenizeAny(u8, text, &std.ascii.whitespace);
        while (it.next()) |_| count += 1;
        const ids = try a.alloc(i32, count);
        @memset(ids, 7);
        return ids;
    }
    fn append(raw: *anyopaque, a: A, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(raw, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }
    fn decodeIds(_: *anyopaque, a: A, _: []const i32) anyerror![]u8 {
        return a.dupe(u8, "");
    }
    fn unused(_: *anyopaque, _: A, _: []const u8, _: usize) anyerror!tok.EncodeResult {
        return error.NotUsed;
    }
    fn unusedGeneration(_: *anyopaque, _: A, _: []const u8, _: usize, _: bool) anyerror!tok.EncodeResult {
        return error.NotUsed;
    }
    fn specials(_: *anyopaque) tok.SpecialTokens {
        return .{};
    }
    fn size(_: *anyopaque) usize {
        return 16;
    }
    fn deinit(_: *anyopaque) void {}
};

test "Memorilla hybrid chunker packs sentences greedily within the token bound" {
    const a = std.testing.allocator;
    const tokenizer = WordCountTokenizer.handle();
    // Bound 5 = four words plus EOS.
    const text = "One two three. Four five.\nSix seven eight nine ten eleven. Twelve!";
    const chunks = try chunkDocument(a, tokenizer, text, 5);
    defer a.free(chunks);
    var got: std.ArrayListUnmanaged([]const u8) = .empty;
    defer got.deinit(a);
    for (chunks) |chunk| try got.append(a, text[chunk.start..chunk.end]);
    const expected = [_][]const u8{ "One two three.", "Four five.", "Six seven eight nine", "ten eleven. Twelve!" };
    try std.testing.expectEqual(expected.len, got.items.len);
    for (expected, got.items) |want, have| try std.testing.expectEqualStrings(want, have);
    // Every chunk fits the bound.
    for (got.items) |chunk| try std.testing.expect(try tokenCount(a, tokenizer, chunk) <= 5);
    const one = try chunkDocument(a, tokenizer, "  short text  ", 5);
    defer a.free(one);
    try std.testing.expectEqual(@as(usize, 1), one.len);
    try std.testing.expectEqualStrings("short text", "  short text  "[one[0].start..one[0].end]);
    try std.testing.expectError(error.MemorillaTextLimitExceeded, chunkDocument(a, tokenizer, "x", 1));
}
