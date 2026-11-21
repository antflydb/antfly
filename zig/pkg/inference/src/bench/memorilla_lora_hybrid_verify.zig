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

//! Drives the managed hybrid service (`memorilla_hybrid_service`) through a
//! real `ModelManager` with ONE resident encoder (the stock managed
//! `qwen3_encoder_f32_v1`, run-time encoder LoRA on the record pass), the
//! decoder, the released reader and the hybrid research reader, over the
//! Python hybrid evaluator's rows (`qwen_hybrid_eval.py --record-gate
//! --answer-canonicalize`, `hybrid-<set>.jsonl`). Rows carry the question,
//! gold, Python answer and gate but not the documents, so each set also names
//! a case file (`{"cases":[{name, documents, question, answer}]}`).
//!
//! Per set it reports agreement with the Python answer and gate, Zig
//! correctness (exact for depot sets; SQuAD EM/F1 for QA sets), record text
//! agreement when both gates opened, citation checks (every citation equals
//! its document bytes; supporting citations) and latency. Plan section 8.7.
const std = @import("std");
const inference = @import("inference_internal");
const options_mod = @import("build_options");
const service = inference.server.memorilla_hybrid_service;
const common = inference.server.memorilla_service;
const manager_mod = inference.server.model_manager;
const qualified = inference.models.memorilla_qualification;
const memory = inference.runtime.tier.memory;
const Control = inference.InferenceExecutionControl;
const file = inference.util.c_file;
const A = std.mem.Allocator;

const Kind = enum { exact, qa };

const SetArg = struct {
    name: []const u8,
    kind: Kind,
    rows: []const u8,
    cases: []const u8,
};

const Args = struct {
    encoder: []const u8 = "",
    decoder: []const u8 = "",
    bundle: []const u8 = "",
    released_bundle: []const u8 = "",
    sets: []const SetArg = &.{},
    output: []const u8 = "",
    max_cases: usize = 0,
    budget_gib: usize = 112,
    max_new_tokens: usize = 48,
    /// Span-window bundles: override the bundle's gate policy.
    span_gate: ?inference.pipelines.memorilla_span.GatePolicy = null,
    span_gate_profile: ?[]const u8 = null,
    /// `--span-read narrow|wide|auto`: the read mode of a cascade bundle
    /// (`span_read_cascade`, plan 8.32); unset takes the bundle default.
    span_read: ?inference.pipelines.memorilla_span.ReadMode = null,
    /// `--span-variants v1,v2,...` (`narrow`, `wide`, `auto`, `autofull` =
    /// auto with the wide pass encoding every document): every case runs each
    /// variant back to back, the order rotated by one per case, so variants
    /// share the process, its warm caches and the same moments of the run.
    variants: []const Variant = &.{},
    /// `--set-prompt name:prompt.json`: the benchmark prompt and new-token
    /// limit (`max_new_tokens` in the file) of one set.
    set_prompts: []const SetPrompt = &.{},
    /// `--admission-events 1`: after every request, emit what stays admitted.
    admission_events: bool = false,
    /// `--plain-set name:rows.jsonl`: plain-text timing rows (RAG baselines).
    plain_sets: []const PlainSetArg = &.{},
    /// `--soft-set name:cases.json:vectors.f32[:questions.f32]`: native
    /// Memorilla (released reader over stored document vectors, no text)
    /// timing rows; with a questions file the stored question vectors replace
    /// the question pass.
    soft_sets: []const SoftSetArg = &.{},
    /// `--stored set:cases.json:vectors.f32[:questions.f32]`: the hybrid set
    /// `set` reads these stored chunk (and question) vectors for its soft
    /// half instead of pooling its span documents.
    stored: []const SoftSetArg = &.{},
    /// `--bench-prompt prompt.json` (`{"system": ..., "suffix": ...}`): every
    /// hybrid set uses this benchmark prompt (`qwen_hybrid_eval.py
    /// paper-run --arms hybrid-bench`) instead of the service's own.
    bench_prompt: ?common.BenchPrompt = null,
    /// `--pause-file PATH`: before each hybrid case, wait (untimed) while
    /// PATH exists, so a runner can hold cases while the GPU is shared.
    pause_file: []const u8 = "",
};

fn waitWhilePaused(io: std.Io, path: []const u8) !f64 {
    if (path.len == 0) return 0;
    var waited: f64 = 0;
    while (if (std.Io.Dir.cwd().access(io, path, .{})) |_| true else |_| false) {
        try io.sleep(.fromMilliseconds(250), .awake);
        waited += 250;
    }
    return waited;
}

fn unixMs(io: std.Io) i64 {
    return @intCast(@divTrunc(std.Io.Timestamp.now(io, .real).nanoseconds, 1_000_000));
}

const BenchPromptFile = struct { system: []const u8, suffix: []const u8 = "", max_new_tokens: usize = 0 };

const ReadMode = inference.pipelines.memorilla_span.ReadMode;

/// One request configuration of `--span-variants`.
const Variant = struct { name: []const u8, mode: ?ReadMode, reuse: ?bool = null };

fn parseVariants(a: A, value: []const u8) ![]const Variant {
    var out: std.ArrayList(Variant) = .empty;
    var it = std.mem.splitScalar(u8, value, ',');
    while (it.next()) |name| {
        const variant: Variant = if (std.mem.eql(u8, name, "autofull"))
            .{ .name = name, .mode = .auto, .reuse = false }
        else
            .{ .name = name, .mode = std.meta.stringToEnum(ReadMode, name) orelse return error.InvalidArguments };
        for (out.items) |seen| if (std.mem.eql(u8, seen.name, name)) return error.InvalidArguments;
        try out.append(a, variant);
    }
    if (out.items.len == 0) return error.InvalidArguments;
    return out.toOwnedSlice(a);
}

const SetPrompt = struct { name: []const u8, prompt: common.BenchPrompt, max_new_tokens: usize };

fn readSetPrompt(a: A, value: []const u8) !SetPrompt {
    const colon = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidArguments;
    if (colon == 0 or colon + 1 >= value.len) return error.InvalidArguments;
    const bytes = try file.readFileMax(a, value[colon + 1 ..], 1024 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(BenchPromptFile, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    // Kept for the process lifetime.
    if (parsed.value.system.len == 0 or parsed.value.max_new_tokens > 128) return error.InvalidArguments;
    return .{ .name = value[0..colon], .prompt = .{ .system = parsed.value.system, .user_suffix = parsed.value.suffix }, .max_new_tokens = parsed.value.max_new_tokens };
}

fn readBenchPrompt(a: A, path: []const u8) !common.BenchPrompt {
    const bytes = try file.readFileMax(a, path, 1024 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(BenchPromptFile, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    // Kept for the process lifetime.
    if (parsed.value.system.len == 0) return error.InvalidArguments;
    return .{ .system = parsed.value.system, .user_suffix = parsed.value.suffix };
}

const SoftSetArg = struct { name: []const u8, cases: []const u8, vectors: []const u8, questions: ?[]const u8 = null };

/// `name:cases.json:vectors.f32[:questions.f32]`.
fn parseSoftSet(value: []const u8) !SoftSetArg {
    var parts = std.mem.splitScalar(u8, value, ':');
    const name = parts.next() orelse return error.InvalidArguments;
    const cases = parts.next() orelse return error.InvalidArguments;
    const vectors = parts.next() orelse return error.InvalidArguments;
    const questions = parts.next();
    if (name.len == 0 or cases.len == 0 or vectors.len == 0 or parts.next() != null) return error.InvalidArguments;
    if (questions) |path| if (path.len == 0) return error.InvalidArguments;
    return .{ .name = name, .cases = cases, .vectors = vectors, .questions = questions };
}

/// Stored vectors of a soft set: per-case document rows at `offset` in the
/// vector file and (optional) one question row per case, in case order.
const SoftCaseFile = struct { cases: []const SoftCase };

const StoredVectors = struct {
    parsed: std.json.Parsed(SoftCaseFile),
    vectors: []f32,
    questions: ?[]f32,

    fn load(a: A, set: SoftSetArg) !StoredVectors {
        const width = qualified.width;
        const case_bytes = try file.readFileMax(a, set.cases, 64 * 1024 * 1024);
        defer a.free(case_bytes);
        var parsed = try std.json.parseFromSlice(SoftCaseFile, a, case_bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        errdefer parsed.deinit();
        const vectors = try readVectors(a, set.vectors);
        errdefer a.free(vectors);
        const questions: ?[]f32 = if (set.questions) |path| try readVectors(a, path) else null;
        errdefer if (questions) |values| a.free(values);
        if (questions) |values| if (values.len != parsed.value.cases.len * width) return error.InvalidArguments;
        for (parsed.value.cases) |case| {
            if (case.documents == 0 or (case.offset + case.documents) * width > vectors.len) return error.InvalidArguments;
        }
        return .{ .parsed = parsed, .vectors = vectors, .questions = questions };
    }

    fn readVectors(a: A, path: []const u8) ![]f32 {
        const bytes = try file.readFileMax(a, path, 2 * 1024 * 1024 * 1024);
        defer a.free(bytes);
        if (bytes.len == 0 or bytes.len % (qualified.width * @sizeOf(f32)) != 0) return error.InvalidArguments;
        const values = try a.alloc(f32, bytes.len / @sizeOf(f32));
        @memcpy(std.mem.sliceAsBytes(values), bytes);
        return values;
    }

    fn deinit(self: *StoredVectors, a: A) void {
        self.parsed.deinit();
        a.free(self.vectors);
        if (self.questions) |values| a.free(values);
    }

    fn find(self: *const StoredVectors, name: []const u8) ?usize {
        for (self.parsed.value.cases, 0..) |case, i| if (std.mem.eql(u8, case.name, name)) return i;
        return null;
    }

    /// Row slices of case `index` (caller frees the outer slice).
    fn documents(self: *const StoredVectors, a: A, index: usize) ![]const []const f32 {
        const width = qualified.width;
        const case = self.parsed.value.cases[index];
        const rows = try a.alloc([]const f32, case.documents);
        for (rows, 0..) |*row, i| row.* = self.vectors[(case.offset + i) * width ..][0..width];
        return rows;
    }

    fn question(self: *const StoredVectors, index: usize) ?[]const f32 {
        const values = self.questions orelse return null;
        return values[index * qualified.width ..][0..qualified.width];
    }
};

/// One native Memorilla case: `documents` stored vectors (FP16-exact F32,
/// unit norm, `qualified.width` wide) at `offset` vectors into the set's
/// little-endian F32 vector file.
const SoftCase = struct {
    name: []const u8,
    question: []const u8,
    documents: usize,
    offset: usize,
};

const PlainSetArg = struct { name: []const u8, rows: []const u8 };

/// One plain-text row: an already-rendered system and user turn (for example
/// the paper's RAG-k baseline prompt) and its generation budget.
const PlainRow = struct {
    name: []const u8,
    system: []const u8,
    user: []const u8,
    max_new_tokens: usize = 0,
};

/// `--set name:kind:rows.jsonl:cases.json` (kind `exact` or `qa`).
fn parseSet(value: []const u8) !SetArg {
    var it = std.mem.splitScalar(u8, value, ':');
    const name = it.next() orelse return error.InvalidArguments;
    const kind_text = it.next() orelse return error.InvalidArguments;
    const rows = it.next() orelse return error.InvalidArguments;
    const cases = it.next() orelse return error.InvalidArguments;
    if (it.next() != null or name.len == 0 or rows.len == 0 or cases.len == 0) return error.InvalidArguments;
    const kind = std.meta.stringToEnum(Kind, kind_text) orelse return error.InvalidArguments;
    return .{ .name = name, .kind = kind, .rows = rows, .cases = cases };
}

fn parse(a: A, init: std.process.Init) !Args {
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    var result = Args{};
    var sets: std.ArrayList(SetArg) = .empty;
    var plain_sets: std.ArrayList(PlainSetArg) = .empty;
    var soft_sets: std.ArrayList(SoftSetArg) = .empty;
    var stored: std.ArrayList(SoftSetArg) = .empty;
    var set_prompts: std.ArrayList(SetPrompt) = .empty;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const flag = args[i];
        i += 1;
        const value = try a.dupe(u8, args[i]);
        if (std.mem.eql(u8, flag, "--encoder")) {
            result.encoder = value;
        } else if (std.mem.eql(u8, flag, "--decoder")) {
            result.decoder = value;
        } else if (std.mem.eql(u8, flag, "--bundle")) {
            result.bundle = value;
        } else if (std.mem.eql(u8, flag, "--released-bundle")) {
            result.released_bundle = value;
        } else if (std.mem.eql(u8, flag, "--set")) {
            try sets.append(a, try parseSet(value));
        } else if (std.mem.eql(u8, flag, "--output")) {
            result.output = value;
        } else if (std.mem.eql(u8, flag, "--max-cases")) {
            result.max_cases = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--budget-gib")) {
            result.budget_gib = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--max-new-tokens")) {
            result.max_new_tokens = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, flag, "--span-gate")) {
            result.span_gate = std.meta.stringToEnum(inference.pipelines.memorilla_span.GatePolicy, value) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, flag, "--span-gate-profile")) {
            result.span_gate_profile = value;
        } else if (std.mem.eql(u8, flag, "--span-variants")) {
            result.variants = try parseVariants(a, value);
        } else if (std.mem.eql(u8, flag, "--admission-events")) {
            result.admission_events = std.mem.eql(u8, value, "1");
        } else if (std.mem.eql(u8, flag, "--set-prompt")) {
            try set_prompts.append(a, try readSetPrompt(a, value));
        } else if (std.mem.eql(u8, flag, "--span-read")) {
            result.span_read = std.meta.stringToEnum(inference.pipelines.memorilla_span.ReadMode, value) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, flag, "--plain-set")) {
            const colon = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidArguments;
            if (colon == 0 or colon + 1 >= value.len) return error.InvalidArguments;
            try plain_sets.append(a, .{ .name = value[0..colon], .rows = value[colon + 1 ..] });
        } else if (std.mem.eql(u8, flag, "--soft-set")) {
            try soft_sets.append(a, try parseSoftSet(value));
        } else if (std.mem.eql(u8, flag, "--stored")) {
            try stored.append(a, try parseSoftSet(value));
        } else if (std.mem.eql(u8, flag, "--pause-file")) {
            result.pause_file = value;
        } else if (std.mem.eql(u8, flag, "--bench-prompt")) {
            result.bench_prompt = try readBenchPrompt(a, value);
        } else return error.UnknownArgument;
    }
    result.sets = try sets.toOwnedSlice(a);
    result.plain_sets = try plain_sets.toOwnedSlice(a);
    result.soft_sets = try soft_sets.toOwnedSlice(a);
    result.stored = try stored.toOwnedSlice(a);
    result.set_prompts = try set_prompts.toOwnedSlice(a);
    if (result.variants.len > 0 and result.span_read != null) return error.InvalidArguments;
    if (result.encoder.len == 0 or result.decoder.len == 0 or result.bundle.len == 0 or result.released_bundle.len == 0 or
        (result.sets.len == 0 and result.plain_sets.len == 0 and result.soft_sets.len == 0) or
        result.budget_gib == 0 or result.budget_gib > 112 or result.max_new_tokens == 0 or result.max_new_tokens > 128) return error.InvalidArguments;
    return result;
}

const Case = struct {
    name: []const u8,
    documents: []const []const u8,
    question: []const u8,
    answer: []const u8 = "",
};

const CaseFile = struct { cases: []const Case };

/// One Python evaluator row (`hybrid-<set>.jsonl`).
const PythonRow = struct {
    name: []const u8,
    question: []const u8,
    gold: []const u8,
    answer: []const u8,
    record_gate: ?bool = null,
    canonicalized: bool = false,
    memory_rows: usize = 0,
    hybrid_record_text: ?[]const u8 = null,
    /// Span-window rows: `[[start, end + 1, 0]]` in the copy ids.
    spans: ?[]const [3]i64 = null,
    @"error": ?[]const u8 = null,
};

// --- SQuAD metrics (qwen_hybrid_eval.normalize_answer / squad_exact / squad_f1) ---

/// Python `string.punctuation`.
fn isPunctuation(byte: u8) bool {
    return (byte >= '!' and byte <= '/') or (byte >= ':' and byte <= '@') or (byte >= '[' and byte <= '`') or (byte >= '{' and byte <= '~');
}

fn isArticle(word: []const u8) bool {
    return std.mem.eql(u8, word, "a") or std.mem.eql(u8, word, "an") or std.mem.eql(u8, word, "the");
}

/// Lower case (ASCII), drop ASCII punctuation, drop the articles a/an/the,
/// collapse whitespace. Returns the normalized words.
fn normalizedWords(a: A, text: []const u8) !std.ArrayList([]const u8) {
    var cleaned: std.ArrayList(u8) = .empty;
    defer cleaned.deinit(a);
    for (text) |byte| {
        if (isPunctuation(byte)) continue;
        try cleaned.append(a, std.ascii.toLower(byte));
    }
    var words: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (words.items) |word| a.free(word);
        words.deinit(a);
    }
    var it = std.mem.tokenizeAny(u8, cleaned.items, &std.ascii.whitespace);
    while (it.next()) |word| {
        if (isArticle(word)) continue;
        try words.append(a, try a.dupe(u8, word));
    }
    return words;
}

fn freeWords(a: A, words: *std.ArrayList([]const u8)) void {
    for (words.items) |word| a.free(word);
    words.deinit(a);
}

fn isAbstain(a: A, text: []const u8) !bool {
    var words = try normalizedWords(a, text);
    defer freeWords(a, &words);
    const joined = try std.mem.join(a, " ", words.items);
    defer a.free(joined);
    for ([_][]const u8{ "", "unknown", "unanswerable", "no answer" }) |marker| if (std.mem.eql(u8, joined, marker)) return true;
    return false;
}

const QaScore = struct { em: f64, f1: f64 };

fn squadScore(a: A, prediction: []const u8, gold: []const u8) !QaScore {
    if (try isAbstain(a, gold)) {
        const value: f64 = if (try isAbstain(a, prediction)) 1 else 0;
        return .{ .em = value, .f1 = value };
    }
    var predicted = try normalizedWords(a, prediction);
    defer freeWords(a, &predicted);
    var tokens = try normalizedWords(a, gold);
    defer freeWords(a, &tokens);
    var em: f64 = 0;
    if (predicted.items.len == tokens.items.len) {
        em = 1;
        for (predicted.items, tokens.items) |x, y| if (!std.mem.eql(u8, x, y)) {
            em = 0;
            break;
        };
    }
    if (predicted.items.len == 0 or tokens.items.len == 0) return .{ .em = em, .f1 = 0 };
    // Multiset intersection size.
    const used = try a.alloc(bool, tokens.items.len);
    defer a.free(used);
    @memset(used, false);
    var common_count: usize = 0;
    for (predicted.items) |word| {
        for (tokens.items, used) |token, *taken| {
            if (!taken.* and std.mem.eql(u8, word, token)) {
                taken.* = true;
                common_count += 1;
                break;
            }
        }
    }
    if (common_count == 0) return .{ .em = em, .f1 = 0 };
    const precision = @as(f64, @floatFromInt(common_count)) / @as(f64, @floatFromInt(predicted.items.len));
    const recall = @as(f64, @floatFromInt(common_count)) / @as(f64, @floatFromInt(tokens.items.len));
    return .{ .em = em, .f1 = 2 * precision * recall / (precision + recall) };
}

// --- reporting ------------------------------------------------------------------

fn emit(a: A, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}

fn elapsedMs(io: std.Io, started: std.Io.Timestamp) f64 {
    const finished = std.Io.Timestamp.now(io, .awake);
    const nanoseconds = started.durationTo(finished).nanoseconds;
    return if (nanoseconds <= 0) 0 else @as(f64, @floatFromInt(nanoseconds)) / 1_000_000.0;
}

/// Milliseconds from the request start to each service stage (-1: not
/// reached), via `Options.stage`.
const StageClock = struct {
    io: std.Io,
    started: std.Io.Timestamp,
    prepared: f64 = -1,
    encoder_loaded: f64 = -1,
    question_encoded: f64 = -1,
    decoder_loaded: f64 = -1,
    reader_loaded: f64 = -1,
    decoded: f64 = -1,

    fn record(context: ?*anyopaque, stage: common.Stage, _: *manager_mod.ModelManager) anyerror!void {
        const self: *StageClock = @ptrCast(@alignCast(context.?));
        const now = elapsedMs(self.io, self.started);
        switch (stage) {
            inline else => |tag| @field(self, @tagName(tag)) = now,
        }
    }
};

/// Count of citations that do not reproduce their document bytes.
fn citationMismatches(documents: []const []const u8, citations: []const service.Citation) usize {
    var mismatches: usize = 0;
    for (citations) |citation| {
        const valid = citation.document_index < documents.len and citation.start < citation.end and
            citation.end <= documents[citation.document_index].len and
            std.mem.eql(u8, documents[citation.document_index][citation.start..citation.end], citation.text);
        mismatches += @intFromBool(!valid);
    }
    return mismatches;
}

fn percentile(values: []f64, fraction: f64) f64 {
    if (values.len == 0) return 0;
    std.mem.sort(f64, values, {}, std.sort.asc(f64));
    const index: usize = @intFromFloat(@floor(fraction * @as(f64, @floatFromInt(values.len - 1)) + 0.5));
    return values[@min(index, values.len - 1)];
}

const SetSummary = struct {
    set: []const u8,
    kind: []const u8,
    cases: usize = 0,
    errors: usize = 0,
    /// Zig published text == Python `answer`.
    answer_matches_python: usize = 0,
    /// Zig gate == Python `record_gate`.
    gate_agreement: usize = 0,
    gate_open_zig: usize = 0,
    gate_open_python: usize = 0,
    /// Both gates open and the decoded record texts are equal.
    record_text_matches: usize = 0,
    both_gates_open: usize = 0,
    canonicalized_zig: usize = 0,
    canonicalized_python: usize = 0,
    /// Span-window bundles: Zig span == Python span, out of `span_rows`.
    span_matches: usize = 0,
    span_rows: usize = 0,
    /// Mean host span-head load + forward time.
    mean_head_ms: f64 = 0,
    guard_rejections: usize = 0,
    /// exact sets: answer == gold.
    zig_exact: usize = 0,
    python_exact: usize = 0,
    /// exact sets: gold UNKNOWN cases and how many were answered UNKNOWN.
    gold_missing: usize = 0,
    zig_abstained_on_missing: usize = 0,
    /// qa sets: mean SQuAD EM / F1.
    zig_em: f64 = 0,
    zig_f1: f64 = 0,
    python_em: f64 = 0,
    python_f1: f64 = 0,
    citations: usize = 0,
    citation_mismatches: usize = 0,
    citations_unavailable_cases: usize = 0,
    supported_cases: usize = 0,
    /// Correct (exact sets) or EM=1 (qa sets) non-UNKNOWN answers with a
    /// supporting citation, out of `correct_answered_gate_open`.
    correct_with_support: usize = 0,
    correct_answered_gate_open: usize = 0,
    admission_refunded: usize = 0,
    mean_ms: f64 = 0,
    p50_ms: f64 = 0,
    p90_ms: f64 = 0,
};

const Summary = struct {
    event: []const u8 = "complete",
    sets: []const SetSummary,
    cold_ms: f64,
    passed: bool,
};

fn readJsonl(a: A, path: []const u8) !std.ArrayList(std.json.Parsed(PythonRow)) {
    const bytes = try file.readFileMax(a, path, 256 * 1024 * 1024);
    defer a.free(bytes);
    var rows: std.ArrayList(std.json.Parsed(PythonRow)) = .empty;
    errdefer {
        for (rows.items) |*row| row.deinit();
        rows.deinit(a);
    }
    // Split on "\n" only (U+2028 inside JSON strings is not a line break).
    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        try rows.append(a, try std.json.parseFromSlice(PythonRow, a, line, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }));
    }
    return rows;
}

pub fn main(init: std.process.Init) !void {
    if (comptime !options_mod.enable_cuda) return error.CudaNotEnabled;
    const a = std.heap.c_allocator;
    const io = init.io;
    const args = try parse(a, init);
    const cap = try std.math.mul(usize, args.budget_gib, 1024 * 1024 * 1024);
    const limits = memory.Limits{ .host_limit_bytes = cap, .backend_limit_bytes = cap, .combined_limit_bytes = cap, .kv_limit_bytes = 2 * 1024 * 1024 * 1024, .scratch_limit_bytes = 16 * 1024 * 1024 * 1024 };

    const selection = inference.model_selection;
    const encoder_path = try std.Io.Dir.cwd().realPathFileAlloc(io, args.encoder, a);
    const selected_encoder = try selection.fromDirectory(a, io, encoder_path);
    defer a.free(selected_encoder.path);
    const decoder_path = try std.Io.Dir.cwd().realPathFileAlloc(io, args.decoder, a);
    const selected_decoder = try selection.fromDirectory(a, io, decoder_path);
    defer a.free(selected_decoder.path);
    const bundle_path = try std.Io.Dir.cwd().realPathFileAlloc(io, args.bundle, a);
    defer a.free(bundle_path);
    const released_path = try std.Io.Dir.cwd().realPathFileAlloc(io, args.released_bundle, a);
    defer a.free(released_path);
    const paths = common.Paths{ .encoder = selected_encoder, .decoder = selected_decoder };
    const profile = common.Profile{
        .name = "hybrid-research",
        .encoder = selected_encoder.path,
        .decoder = selected_decoder.path,
        .memory_module_path = bundle_path,
        .max_new_tokens = 128,
        .reader_kind = .hybrid_research,
        .released_memory_module_path = released_path,
    };
    const identity = qualified.executionIdentity(service.encoder_profile);

    var sessions = inference.backends.SessionManager.initWithIo(a, io);
    sessions.preferred_backends = &.{.cuda};
    sessions.required_backend = .cuda;
    sessions.native_call_policy = .in_process;
    var models = manager_mod.ModelManager.init(a, sessions);
    defer models.deinit();
    models.configureServingPolicy(.{});
    models.configureAdmissionLimits(limits);
    models.configureProcessMemoryLimit(cap, .explicit);
    models.configureModelCache(600_000, 4);
    try models.ensureResourceOwnerReady();

    try emit(a, io, .{
        .event = "ready",
        .scope = "actual-managed-memorilla-hybrid-service",
        .sets = args.sets.len,
        .encoder_publication = selected_encoder.publication != null,
        .decoder_publication = selected_decoder.publication != null,
        .operator_cap_bytes = cap,
        .encoder_execution_identity = identity[0..],
        .max_new_tokens = args.max_new_tokens,
    });

    var summaries: std.ArrayList(SetSummary) = .empty;
    var retained: ?memory.AdmissionAmounts = null;
    var cold_ms: f64 = 0;
    var first_case = true;
    var passed = true;
    for (args.sets) |set| {
        var rows = try readJsonl(a, set.rows);
        defer {
            for (rows.items) |*row| row.deinit();
            rows.deinit(a);
        }
        var stored: ?StoredVectors = for (args.stored) |entry| {
            if (std.mem.eql(u8, entry.name, set.name)) break try StoredVectors.load(a, entry);
        } else null;
        defer if (stored) |*vectors| vectors.deinit(a);
        const case_bytes = try file.readFileMax(a, set.cases, 256 * 1024 * 1024);
        defer a.free(case_bytes);
        var case_file = try std.json.parseFromSlice(CaseFile, a, case_bytes, .{ .ignore_unknown_fields = true });
        defer case_file.deinit();
        var by_name: std.StringHashMapUnmanaged(*const Case) = .empty;
        defer by_name.deinit(a);
        for (case_file.value.cases) |*case| try by_name.put(a, case.name, case);

        var summary = SetSummary{ .set = set.name, .kind = @tagName(set.kind) };
        const set_prompt_entry: ?SetPrompt = for (args.set_prompts) |entry| {
            if (std.mem.eql(u8, entry.name, set.name)) break entry;
        } else null;
        const set_prompt: ?common.BenchPrompt = if (set_prompt_entry) |entry| entry.prompt else args.bench_prompt;
        const set_max_new_tokens: usize = if (set_prompt_entry) |entry| (if (entry.max_new_tokens > 0) entry.max_new_tokens else args.max_new_tokens) else args.max_new_tokens;
        const single = [_]Variant{.{ .name = if (args.span_read) |mode| @tagName(mode) else "", .mode = args.span_read }};
        const variants: []const Variant = if (args.variants.len > 0) args.variants else &single;
        var latencies: std.ArrayList(f64) = .empty;
        defer latencies.deinit(a);
        var em_total: f64 = 0;
        var f1_total: f64 = 0;
        var python_em_total: f64 = 0;
        var python_f1_total: f64 = 0;
        var head_ms_total: f64 = 0;
        const limit = if (args.max_cases == 0) rows.items.len else @min(args.max_cases, rows.items.len);
        for (rows.items[0..limit], 0..) |*parsed_row, row_index| {
            const row = parsed_row.value;
            if (row.@"error" != null) continue;
            const case = by_name.get(row.name) orelse {
                summary.cases += 1;
                summary.errors += 1;
                try emit(a, io, .{ .event = "case_error", .set = set.name, .name = row.name, .@"error" = "CaseNotFound" });
                continue;
            };
            const stored_index: ?usize = if (stored) |*vectors| vectors.find(row.name) orelse {
                summary.cases += 1;
                summary.errors += 1;
                try emit(a, io, .{ .event = "case_error", .set = set.name, .name = row.name, .@"error" = "StoredVectorsNotFound" });
                continue;
            } else null;
            const stored_documents: ?[]const []const f32 = if (stored_index) |index| try stored.?.documents(a, index) else null;
            defer if (stored_documents) |rows_| a.free(rows_);
            const stored_question: ?[]const f32 = if (stored_index) |index| stored.?.question(index) else null;
            for (0..variants.len) |step| {
                const variant = variants[(row_index + step) % variants.len];
                summary.cases += 1;
                const paused_ms = try waitWhilePaused(io, args.pause_file);
                const wall_start_ms = unixMs(io);
                const started = std.Io.Timestamp.now(io, .awake);
                var clock = StageClock{ .io = io, .started = started };
                const control = Control{ .io = io, .native_call_policy = .in_process };
                var result = service.generate(a, io, &models, profile, paths, case.question, case.documents, &identity, set_max_new_tokens, limits, .{}, control, .{ .span_gate_policy = args.span_gate, .span_gate_profile = args.span_gate_profile, .span_read_mode = variant.mode, .span_cascade_reuse = variant.reuse, .context = &clock, .stage = StageClock.record, .stored_documents = stored_documents, .stored_question = stored_question, .span_bench_prompt = set_prompt }) catch |err| {
                    summary.errors += 1;
                    try emit(a, io, .{ .event = "case_error", .set = set.name, .name = row.name, .variant = variant.name, .@"error" = @errorName(err) });
                    continue;
                };
                var released = false;
                defer if (!released) result.deinit();
                const total_ms = elapsedMs(io, started);
                if (first_case) {
                    cold_ms = total_ms;
                    first_case = false;
                } else try latencies.append(a, total_ms);
                const answer = std.mem.trim(u8, result.generation.text, " \t\r\n");
                const python_answer = std.mem.trim(u8, row.answer, " \t\r\n");
                const gate_open = result.record_gate == .open;
                const python_gate = row.record_gate orelse (row.memory_rows == 32);
                summary.answer_matches_python += @intFromBool(std.mem.eql(u8, answer, python_answer));
                summary.gate_agreement += @intFromBool(gate_open == python_gate);
                summary.gate_open_zig += @intFromBool(gate_open);
                summary.gate_open_python += @intFromBool(python_gate);
                summary.canonicalized_zig += @intFromBool(result.canonicalized);
                summary.canonicalized_python += @intFromBool(row.canonicalized);
                summary.guard_rejections += @intFromBool(result.answer_guard == .rejected);
                var span_matches: ?bool = null;
                if (result.span) |diagnostics| {
                    head_ms_total += diagnostics.head_ms;
                    if (row.spans) |spans| if (spans.len == 1) {
                        summary.span_rows += 1;
                        span_matches = spans[0][0] == @as(i64, @intCast(diagnostics.start)) and spans[0][1] == @as(i64, @intCast(diagnostics.end + 1));
                        summary.span_matches += @intFromBool(span_matches.?);
                    };
                }
                var record_matches: ?bool = null;
                if (gate_open and python_gate) {
                    summary.both_gates_open += 1;
                    if (row.hybrid_record_text) |text| {
                        record_matches = std.mem.eql(u8, text, result.record_text);
                        summary.record_text_matches += @intFromBool(record_matches.?);
                    }
                }
                var correct = false;
                var score: QaScore = .{ .em = 0, .f1 = 0 };
                var python_score: QaScore = .{ .em = 0, .f1 = 0 };
                switch (set.kind) {
                    .exact => {
                        correct = std.mem.eql(u8, answer, row.gold);
                        summary.zig_exact += @intFromBool(correct);
                        summary.python_exact += @intFromBool(std.mem.eql(u8, python_answer, row.gold));
                        if (std.mem.eql(u8, row.gold, service.unknown_answer)) {
                            summary.gold_missing += 1;
                            summary.zig_abstained_on_missing += @intFromBool(std.mem.eql(u8, answer, service.unknown_answer));
                        }
                    },
                    .qa => {
                        score = try squadScore(a, answer, row.gold);
                        python_score = try squadScore(a, python_answer, row.gold);
                        em_total += score.em;
                        f1_total += score.f1;
                        python_em_total += python_score.em;
                        python_f1_total += python_score.f1;
                        correct = score.em == 1;
                    },
                }
                const citations = result.citations.items;
                const mismatches = citationMismatches(case.documents, citations);
                var supported = false;
                for (citations) |citation| supported = supported or citation.supports_answer;
                const answered = !std.mem.eql(u8, answer, service.unknown_answer);
                summary.citations += citations.len;
                summary.citation_mismatches += mismatches;
                summary.citations_unavailable_cases += @intFromBool(result.citations.unavailable);
                summary.supported_cases += @intFromBool(supported);
                if (correct and answered and gate_open) {
                    summary.correct_answered_gate_open += 1;
                    summary.correct_with_support += @intFromBool(supported);
                }
                try emit(a, io, .{
                    .event = "case",
                    .set = set.name,
                    .name = row.name,
                    .variant = variant.name,
                    .documents = case.documents.len,
                    .soft_rows = result.soft_rows,
                    .answer = answer,
                    .answer_raw = result.raw_text,
                    .python_answer = python_answer,
                    .gold = row.gold,
                    .answer_matches_python = std.mem.eql(u8, answer, python_answer),
                    .correct = correct,
                    .em = score.em,
                    .f1 = score.f1,
                    .python_em = python_score.em,
                    .python_f1 = python_score.f1,
                    .record_gate = @tagName(result.record_gate),
                    .python_record_gate = python_gate,
                    .record_text = result.record_text,
                    .python_record_text = row.hybrid_record_text,
                    .record_text_matches = record_matches,
                    .min_snap_cosine = result.min_snap_cosine,
                    .span = result.span,
                    .python_spans = row.spans,
                    .span_matches = span_matches,
                    .canonicalized = result.canonicalized,
                    .python_canonicalized = row.canonicalized,
                    .answer_guard = @tagName(result.answer_guard),
                    .finish_reason = result.generation.finish_reason,
                    .memory_tokens = result.memory_tokens,
                    .citations = citations,
                    .citations_unavailable = result.citations.unavailable,
                    .citation_mismatches = mismatches,
                    .answer_supported = supported,
                    .question_tokens = result.question_tokens,
                    .prompt_tokens = result.generation.prompt_tokens,
                    .completion_tokens = result.generation.tokens_used,
                    .total_ms = total_ms,
                    .wall_start_ms = wall_start_ms,
                    .wall_end_ms = unixMs(io),
                    .paused_ms = paused_ms,
                    .stages_ms = .{
                        .prepared = clock.prepared,
                        .encoder_loaded = clock.encoder_loaded,
                        .question_encoded = clock.question_encoded,
                        .decoder_loaded = clock.decoder_loaded,
                        .reader_loaded = clock.reader_loaded,
                        .decoded = clock.decoded,
                    },
                });
                // Every request must retire its own admission; what remains is the
                // resident encoder, decoder and both readers.
                result.deinit();
                released = true;
                const now = models.currentAdmissionAmounts();
                if (retained) |expected| {
                    summary.admission_refunded += @intFromBool(std.meta.eql(expected, now));
                } else {
                    retained = now;
                    summary.admission_refunded += 1;
                }
                // What stays admitted after this request retired its own
                // admission (the resident models, readers and caches).
                if (args.admission_events) try emit(a, io, .{ .event = "admission", .set = set.name, .name = row.name, .variant = variant.name, .refunded = std.meta.eql(retained.?, now), .amounts = now });
            }
        }
        const scored = summary.cases - summary.errors;
        if (scored > 0) summary.mean_head_ms = head_ms_total / @as(f64, @floatFromInt(scored));
        if (set.kind == .qa and scored > 0) {
            const n: f64 = @floatFromInt(scored);
            summary.zig_em = em_total / n;
            summary.zig_f1 = f1_total / n;
            summary.python_em = python_em_total / n;
            summary.python_f1 = python_f1_total / n;
        }
        if (latencies.items.len > 0) {
            var sum: f64 = 0;
            for (latencies.items) |value| sum += value;
            summary.mean_ms = sum / @as(f64, @floatFromInt(latencies.items.len));
            summary.p50_ms = percentile(latencies.items, 0.5);
            summary.p90_ms = percentile(latencies.items, 0.9);
        }
        passed = passed and summary.errors == 0 and summary.admission_refunded == scored and summary.citation_mismatches == 0;
        try emit(a, io, summary);
        try summaries.append(a, summary);
    }
    for (args.plain_sets) |set| try runPlainSet(a, io, &models, selected_decoder, set, args, limits);
    for (args.soft_sets) |set| try runSoftSet(a, io, &models, paths, profile, released_path, set, args, limits, &identity);
    try emit(a, io, .{ .event = "retained", .amounts = retained });
    const summary = Summary{ .sets = summaries.items, .cold_ms = cold_ms, .passed = passed };
    try emit(a, io, summary);
    if (args.output.len > 0) {
        const json = try std.json.Stringify.valueAlloc(a, summary, .{ .whitespace = .indent_2 });
        defer a.free(json);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = args.output, .data = json });
    }
    if (!passed) return error.HybridServiceVerificationFailed;
}

/// Times plain-text generation on the managed decoder
/// (`source_decoder_service`, no memory rows) for each row of a plain set:
/// the RAG-k baseline on the same runtime as the hybrid.
fn runPlainSet(a: A, io: std.Io, models: *manager_mod.ModelManager, decoder: inference.model_selection.ResolvedModel, set: PlainSetArg, args: Args, limits: memory.Limits) !void {
    const plain = inference.server.source_decoder_service;
    const bytes = try file.readFileMax(a, set.rows, 256 * 1024 * 1024);
    defer a.free(bytes);
    var latencies: std.ArrayList(f64) = .empty;
    defer latencies.deinit(a);
    var prompt_tokens: std.ArrayList(f64) = .empty;
    defer prompt_tokens.deinit(a);
    var errors: usize = 0;
    var seen: usize = 0;
    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        if (args.max_cases != 0 and seen >= args.max_cases) break;
        seen += 1;
        var parsed = try std.json.parseFromSlice(PlainRow, a, line, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        defer parsed.deinit();
        const row = parsed.value;
        const budget = if (row.max_new_tokens == 0) args.max_new_tokens else @min(row.max_new_tokens, 128);
        const started = std.Io.Timestamp.now(io, .awake);
        const control = Control{ .io = io, .native_call_policy = .in_process };
        // The user turn (question + retrieved chunks) is the speculative
        // draft context (research mode only; lossless greedy verification).
        var result = plain.generateWithOptions(a, io, models, decoder, row.system, row.user, budget, limits, .{}, control, .{ .draft_context = row.user }) catch |err| {
            errors += 1;
            try emit(a, io, .{ .event = "plain_case_error", .set = set.name, .name = row.name, .@"error" = @errorName(err) });
            continue;
        };
        defer result.deinit();
        const total_ms = elapsedMs(io, started);
        // The first row of each set includes cold loads; it is reported but
        // kept out of the latency summary.
        if (seen > 1) {
            try latencies.append(a, total_ms);
            try prompt_tokens.append(a, @floatFromInt(result.generation.prompt_tokens));
        }
        try emit(a, io, .{
            .event = "plain_case",
            .set = set.name,
            .name = row.name,
            .answer = result.generation.text,
            .finish_reason = result.generation.finish_reason,
            .prompt_tokens = result.generation.prompt_tokens,
            .completion_tokens = result.generation.tokens_used,
            .total_ms = total_ms,
        });
    }
    try emit(a, io, .{
        .event = "plain_set",
        .set = set.name,
        .cases = seen,
        .errors = errors,
        .p50_ms = percentile(latencies.items, 0.5),
        .p90_ms = percentile(latencies.items, 0.9),
        .median_prompt_tokens = percentile(prompt_tokens.items, 0.5),
    });
}

/// Times native Memorilla (`memorilla_service`, the released reader over the
/// case's stored document vectors, no document text): the RAG-0 arm on the
/// same runtime. The first case includes the reader's cold load and is kept
/// out of the summary.
fn runSoftSet(a: A, io: std.Io, models: *manager_mod.ModelManager, paths: common.Paths, hybrid: common.Profile, released_path: []const u8, set: SoftSetArg, args: Args, limits: memory.Limits, identity: []const u8) !void {
    var stored = try StoredVectors.load(a, set);
    defer stored.deinit(a);
    const profile = common.Profile{
        .name = "released",
        .encoder = hybrid.encoder,
        .decoder = hybrid.decoder,
        .memory_module_path = released_path,
        .max_new_tokens = 128,
    };
    var latencies: std.ArrayList(f64) = .empty;
    defer latencies.deinit(a);
    var errors: usize = 0;
    var seen: usize = 0;
    for (stored.parsed.value.cases, 0..) |case, index| {
        if (args.max_cases != 0 and seen >= args.max_cases) break;
        seen += 1;
        const documents = try stored.documents(a, index);
        defer a.free(documents);
        const started = std.Io.Timestamp.now(io, .awake);
        var clock = StageClock{ .io = io, .started = started };
        const control = Control{ .io = io, .native_call_policy = .in_process };
        var result = common.generateWithOptions(a, io, models, profile, paths, case.question, documents, identity, args.max_new_tokens, limits, control, .{ .context = &clock, .stage = StageClock.record, .stored_question = stored.question(index) }) catch |err| {
            errors += 1;
            try emit(a, io, .{ .event = "soft_case_error", .set = set.name, .name = case.name, .@"error" = @errorName(err) });
            continue;
        };
        defer result.deinit();
        const total_ms = elapsedMs(io, started);
        if (seen > 1) try latencies.append(a, total_ms);
        try emit(a, io, .{
            .event = "soft_case",
            .set = set.name,
            .name = case.name,
            .documents = case.documents,
            .answer = result.generation.text,
            .finish_reason = result.generation.finish_reason,
            .prompt_tokens = result.generation.prompt_tokens,
            .completion_tokens = result.generation.tokens_used,
            .total_ms = total_ms,
            .stages_ms = .{
                .prepared = clock.prepared,
                .encoder_loaded = clock.encoder_loaded,
                .question_encoded = clock.question_encoded,
                .decoder_loaded = clock.decoder_loaded,
                .reader_loaded = clock.reader_loaded,
                .decoded = clock.decoded,
            },
        });
    }
    try emit(a, io, .{
        .event = "soft_set",
        .set = set.name,
        .cases = seen,
        .errors = errors,
        .p50_ms = percentile(latencies.items, 0.5),
        .p90_ms = percentile(latencies.items, 0.9),
    });
}

test "memorilla lora hybrid verify parses sets and scores SQuAD answers" {
    const a = std.testing.allocator;
    const set = try parseSet("squad:qa:rows.jsonl:cases.json");
    try std.testing.expectEqualStrings("squad", set.name);
    try std.testing.expect(set.kind == .qa);
    try std.testing.expectError(error.InvalidArguments, parseSet("squad:fuzzy:rows:cases"));
    try std.testing.expectError(error.InvalidArguments, parseSet("squad:qa:rows"));
    const exact = try squadScore(a, "The 10th and 11th centuries.", "10th and 11th centuries");
    try std.testing.expectEqual(@as(f64, 1), exact.em);
    try std.testing.expectEqual(@as(f64, 1), exact.f1);
    const partial = try squadScore(a, "10th", "10th century");
    try std.testing.expectEqual(@as(f64, 0), partial.em);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0 / 3.0), partial.f1, 1e-12);
    const abstain = try squadScore(a, "UNKNOWN", "");
    try std.testing.expectEqual(@as(f64, 1), abstain.em);
    const wrong = try squadScore(a, "France", "");
    try std.testing.expectEqual(@as(f64, 0), wrong.f1);
    // Repeated tokens count once per gold occurrence.
    const repeated = try squadScore(a, "the the cat cat", "cat");
    try std.testing.expectApproxEqAbs(@as(f64, 2 * 0.5 * 1.0 / 1.5), repeated.f1, 1e-12);
}

test "memorilla lora hybrid verify counts citations that do not reproduce their documents" {
    var good = "Kq9".*;
    var bad = "Kq8".*;
    const documents = [_][]const u8{"Code Kq9 opens it."};
    const citations = [_]service.Citation{
        .{ .document_index = 0, .start = 5, .end = 8, .text = &good, .slots = .{ 0, 2 }, .supports_answer = true, .match = .exact },
        .{ .document_index = 0, .start = 5, .end = 8, .text = &bad, .slots = .{ 0, 2 }, .supports_answer = false, .match = .none },
    };
    try std.testing.expectEqual(@as(usize, 1), citationMismatches(&documents, &citations));
    var values = [_]f64{ 5, 1, 3, 2, 4 };
    try std.testing.expectEqual(@as(f64, 3), percentile(&values, 0.5));
}
