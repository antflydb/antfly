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

//! Memorilla copy17 `tokens-dual` encoder features (pure host F32).
//!
//! Mirrors `scripts/memorilla/encoder_lora.py` (`prefixed_documents`,
//! `self_context_tokens`, `self_context_states`, `document_features_dual`,
//! `feature_channel`) and the batch built by `encoder_lora_eval.py`:
//!
//! 1. every document gets one leading space unless it already starts with one;
//! 2. each document is encoded twice (`D + D`); the second copy's ids must equal
//!    `encode(D)` (else `encode(" " + D)`, else error) and are the copy sources;
//! 3. rows `[D1+D1, ..., Dn+Dn, question]`, each followed by EOS 151643, right
//!    padded with 151643 to a common length `<= 128`;
//! 4. from both returned state tensors (final norm output and HF
//!    `hidden_states[24]`) keep each document's last `n_i + 1` active rows and
//!    the question row whole;
//! 5. rows are L2-normalized in F32 (eps 1e-12) and sent through the stored
//!    feature channel F32 -> FP16 (RNE) -> BF16 (RNE) -> F32; `docs` is the
//!    final-layer rows followed by the layer-24 rows (`[2M, 2560]`), `query`
//!    the question's last active final-layer row.
//!
//! Alongside the copy sources it records a source map: for every copy id the
//! caller document it came from and its byte range in the caller's original
//! text (`sourceMap`), so copied spans can be cited.
const std = @import("std");
const tok = @import("inference_tokenizer");
const qualified = @import("../models/memorilla_qualification.zig");
const encoder = @import("memorilla_encoder.zig");
const copy = @import("memorilla_copy.zig");
const span_window = @import("memorilla_span.zig");
const A = std.mem.Allocator;

pub const width: usize = qualified.width;
/// `<|endoftext|>`: appended to every row by the HF post-processor and used as
/// the pad id (mask 0).
pub const eos_id: i32 = 151643;
pub const pad_id: i32 = eos_id;
/// The candidate was trained and evaluated under this encoder bound.
pub const max_padded_length: usize = 128;
/// The hybrid evaluator's padded batch bound (`qwen_hybrid_eval.py
/// --encoder-max-length 1024`): general passages exceed 128 tokens under
/// self-context doubling.
pub const hybrid_max_padded_length: usize = 1024;
/// HF `hidden_states[k]` index of the intermediate layer (0-based layer 23).
pub const tap_layer: usize = 24;
pub const normalize_eps: f32 = 1e-12;

/// `prefixed_documents`: one leading space unless the text already has one.
pub fn prefixDocument(a: A, text: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, text, " ")) return a.dupe(u8, text);
    const out = try a.alloc(u8, text.len + 1);
    out[0] = ' ';
    @memcpy(out[1..], text);
    return out;
}

fn encodeIds(a: A, tokenizer: tok.Tokenizer, text: []const u8) ![]i32 {
    const ids = try tokenizer.encode(a, text);
    errdefer a.free(ids);
    for (ids) |id| if (id < 0 or id == eos_id) return error.InvalidMemorillaTokens;
    return ids;
}

fn endsWith(ids: []const i32, tail: []const i32) bool {
    return tail.len <= ids.len and std.mem.eql(i32, ids[ids.len - tail.len ..], tail);
}

/// One document's causal self-context: the doubled text, how many trailing
/// ids belong to the second copy, and those ids (the copy sources).
pub const SelfContextDocument = struct {
    doubled: []u8,
    single_ids: []i32,
    fallback: bool,
};

/// `self_context_tokens`. `document` is normally already space prefixed; a
/// text without the prefix is repeated after one space, exactly as in Python.
pub fn selfContextDocument(a: A, tokenizer: tok.Tokenizer, document: []const u8) !SelfContextDocument {
    if (std.mem.trim(u8, document, &std.ascii.whitespace).len == 0) return error.BlankMemorillaDocument;
    const separator: []const u8 = if (std.mem.startsWith(u8, document, " ")) "" else " ";
    const doubled = try std.mem.concat(a, u8, &.{ document, separator, document });
    errdefer a.free(doubled);
    const doubled_ids = try encodeIds(a, tokenizer, doubled);
    defer a.free(doubled_ids);

    var single = try encodeIds(a, tokenizer, document);
    var fallback = false;
    if (single.len == 0 or !endsWith(doubled_ids, single)) {
        a.free(single);
        const spaced = try std.mem.concat(a, u8, &.{ " ", document });
        defer a.free(spaced);
        single = try encodeIds(a, tokenizer, spaced);
        fallback = true;
    }
    errdefer a.free(single);
    if (single.len == 0 or !endsWith(doubled_ids, single)) return error.MemorillaSelfContextMismatch;
    return .{ .doubled = doubled, .single_ids = single, .fallback = fallback };
}

/// `self_context_documents` over a case: doubled texts, kept counts `n_i` and
/// the concatenated copy sources.
pub const SelfContext = struct {
    a: A,
    texts: [][]u8,
    counts: []usize,
    copy_ids: []u32,
    fallbacks: usize,
    /// Per document: its copy ids came from `encode(" " + D)`.
    fallback_flags: []bool,

    pub fn deinit(self: *SelfContext) void {
        for (self.texts) |text| self.a.free(text);
        self.a.free(self.texts);
        self.a.free(self.counts);
        self.a.free(self.copy_ids);
        self.a.free(self.fallback_flags);
        self.* = undefined;
    }
};

pub fn selfContextTokens(a: A, tokenizer: tok.Tokenizer, documents: []const []const u8) !SelfContext {
    if (documents.len == 0 or documents.len > qualified.max_documents) return error.InvalidMemorillaDocumentCount;
    const texts = try a.alloc([]u8, documents.len);
    var built: usize = 0;
    errdefer {
        for (texts[0..built]) |text| a.free(text);
        a.free(texts);
    }
    const counts = try a.alloc(usize, documents.len);
    errdefer a.free(counts);
    const fallback_flags = try a.alloc(bool, documents.len);
    errdefer a.free(fallback_flags);
    var copy_ids = std.ArrayListUnmanaged(u32).empty;
    errdefer copy_ids.deinit(a);
    var fallbacks: usize = 0;
    for (documents, texts, counts, fallback_flags) |document, *text, *count, *flag| {
        const context = try selfContextDocument(a, tokenizer, document);
        defer a.free(context.single_ids);
        text.* = context.doubled;
        built += 1;
        count.* = context.single_ids.len;
        flag.* = context.fallback;
        fallbacks += @intFromBool(context.fallback);
        try copy_ids.ensureUnusedCapacity(a, context.single_ids.len);
        for (context.single_ids) |id| copy_ids.appendAssumeCapacity(@intCast(id));
    }
    return .{
        .a = a,
        .texts = texts,
        .counts = counts,
        .copy_ids = try copy_ids.toOwnedSlice(a),
        .fallbacks = fallbacks,
        .fallback_flags = fallback_flags,
    };
}

/// Maps one document's copy ids to byte ranges of the caller's `original`
/// text. The ids are the encoding of `leading` spaces followed by `original`
/// (the space prefix, plus one more on the `encode(" " + D)` fallback).
/// Byte-level BPE is lossless, so the ids' token pieces must concatenate to
/// exactly that text; each piece's cumulative byte range, shifted back by
/// `leading` and clamped at 0, is its range in `original` (a piece covering
/// only added spaces gets the empty range at the document start). Returns
/// false, leaving `out` partly written, when the pieces do not reproduce the
/// text byte for byte (for example a normalizer rewrote it) or a piece cannot
/// be decoded; allocation failure is an error.
pub fn mapDocumentSources(a: A, tokenizer: tok.Tokenizer, document: usize, original: []const u8, leading: usize, ids: []const u32, out: []copy.SourceRange) !bool {
    std.debug.assert(out.len == ids.len);
    const total = leading + original.len;
    if (document > std.math.maxInt(u32) or total > std.math.maxInt(u32)) return false;
    var cursor: usize = 0;
    for (ids, out) |id, *range| {
        if (id > std.math.maxInt(i32)) return false;
        const piece = tokenizer.tokenBytes(a, @intCast(id)) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return false,
        };
        defer a.free(piece);
        if (piece.len > total - cursor) return false;
        for (piece, cursor..) |byte, at| {
            const expected: u8 = if (at < leading) ' ' else original[at - leading];
            if (byte != expected) return false;
        }
        const start = @max(cursor, leading) - leading;
        cursor += piece.len;
        const end = @max(cursor, leading) - leading;
        range.* = .{ .document = @intCast(document), .start = @intCast(start), .end = @intCast(end) };
    }
    return cursor == total;
}

/// The source map of `context`: one `SourceRange` per copy id, in copy-id
/// order. `originals` are the caller's texts and `encoded` the texts handed
/// to `selfContextTokens` (each must end with its original; the bytes before
/// it are the added space prefix). Returns null when any document's pieces
/// do not reproduce its encoded text; citations are then unavailable while
/// the request itself is unaffected.
pub fn sourceMap(a: A, tokenizer: tok.Tokenizer, originals: []const []const u8, encoded: []const []const u8, context: *const SelfContext) !?[]copy.SourceRange {
    if (originals.len != encoded.len or originals.len != context.counts.len) return error.InvalidMemorillaDocumentCount;
    const ranges = try a.alloc(copy.SourceRange, context.copy_ids.len);
    errdefer a.free(ranges);
    var offset: usize = 0;
    for (originals, encoded, context.counts, context.fallback_flags, 0..) |original, text, count, fallback, index| {
        if (!std.mem.endsWith(u8, text, original)) {
            a.free(ranges);
            return null;
        }
        const leading = text.len - original.len + @intFromBool(fallback);
        if (count > context.copy_ids.len - offset) return error.MemorillaSelfContextMismatch;
        const ids = context.copy_ids[offset..][0..count];
        if (!try mapDocumentSources(a, tokenizer, index, original, leading, ids, ranges[offset..][0..count])) {
            a.free(ranges);
            return null;
        }
        offset += count;
    }
    if (offset != context.copy_ids.len) return error.MemorillaSelfContextMismatch;
    return ranges;
}

/// The encoder batch `[D1+D1, ..., Dn+Dn, question]`, EOS appended and right
/// padded. `input_ids` is row-major `[batch, seq_len]` i64 as the `.gpt`
/// session run expects; `active[row]` counts the row's real tokens including
/// its EOS. `counts` and `copy_ids` are the documents' self-context values.
pub const Batch = struct {
    a: A,
    input_ids: []i64,
    batch: usize,
    seq_len: usize,
    active: []usize,
    counts: []usize,
    copy_ids: []u32,
    fallbacks: usize,
    /// Source map parallel to `copy_ids` (`sourceMap`); null when the copy
    /// sources could not be mapped back to the caller's texts.
    sources: ?[]copy.SourceRange = null,

    pub fn documentCount(self: *const Batch) usize {
        return self.batch - 1;
    }

    pub fn deinit(self: *Batch) void {
        self.a.free(self.input_ids);
        self.a.free(self.active);
        self.a.free(self.counts);
        self.a.free(self.copy_ids);
        if (self.sources) |sources| self.a.free(sources);
        self.* = undefined;
    }
};

/// Tokenizes `context.texts ++ [question]` into one right-padded batch. The
/// question gets no prefix and no instruction. `context` stays owned by the
/// caller; its counts and copy ids are copied into the batch.
pub fn assembleBatch(a: A, tokenizer: tok.Tokenizer, context: *const SelfContext, question: []const u8) !Batch {
    return assembleBatchBounded(a, tokenizer, context, question, max_padded_length);
}

/// `assembleBatch` with an explicit padded-row bound (`<= bound` active
/// tokens per row, EOS included).
pub fn assembleBatchBounded(a: A, tokenizer: tok.Tokenizer, context: *const SelfContext, question: []const u8, bound: usize) !Batch {
    if (bound == 0 or bound > hybrid_max_padded_length) return error.MemorillaEncoderBoundExceeded;
    const batch = context.texts.len + 1;
    const rows = try a.alloc([]i32, batch);
    var encoded: usize = 0;
    defer {
        for (rows[0..encoded]) |row| a.free(row);
        a.free(rows);
    }
    const active = try a.alloc(usize, batch);
    errdefer a.free(active);
    var seq_len: usize = 0;
    for (0..batch) |row| {
        const text = if (row < context.texts.len) context.texts[row] else question;
        rows[row] = try encodeIds(a, tokenizer, text);
        encoded += 1;
        active[row] = rows[row].len + 1;
        if (active[row] > bound) return error.MemorillaEncoderBoundExceeded;
        seq_len = @max(seq_len, active[row]);
    }
    if (std.mem.trim(u8, question, &std.ascii.whitespace).len == 0) return error.BlankMemorillaQuestion;
    const input_ids = try a.alloc(i64, batch * seq_len);
    errdefer a.free(input_ids);
    @memset(input_ids, pad_id);
    for (rows, 0..) |row, index| {
        const target = input_ids[index * seq_len ..][0..seq_len];
        for (row, target[0..row.len]) |id, *out| out.* = id;
        target[row.len] = eos_id;
    }
    for (context.counts, active[0..context.texts.len]) |count, row_active| {
        if (count + 1 > row_active) return error.MemorillaSelfContextMismatch;
    }
    const counts = try a.dupe(usize, context.counts);
    errdefer a.free(counts);
    const copy_ids = try a.dupe(u32, context.copy_ids);
    return .{
        .a = a,
        .input_ids = input_ids,
        .batch = batch,
        .seq_len = seq_len,
        .active = active,
        .counts = counts,
        .copy_ids = copy_ids,
        .fallbacks = context.fallbacks,
    };
}

/// Prefix, self-context and batch assembly in one step (section 1.2 steps
/// 1-3), plus the source map of the copy ids into `documents`.
pub fn prepare(a: A, tokenizer: tok.Tokenizer, documents: []const []const u8, question: []const u8) !Batch {
    return prepareBounded(a, tokenizer, documents, question, max_padded_length);
}

/// `prepare` with an explicit padded-row bound (`assembleBatchBounded`).
pub fn prepareBounded(a: A, tokenizer: tok.Tokenizer, documents: []const []const u8, question: []const u8, bound: usize) !Batch {
    if (documents.len == 0 or documents.len > qualified.max_documents) return error.InvalidMemorillaDocumentCount;
    const prefixed = try a.alloc([]u8, documents.len);
    var built: usize = 0;
    defer {
        for (prefixed[0..built]) |text| a.free(text);
        a.free(prefixed);
    }
    for (documents, prefixed) |document, *text| {
        text.* = try prefixDocument(a, document);
        built += 1;
    }
    var context = try selfContextTokens(a, tokenizer, prefixed);
    defer context.deinit();
    const sources = try sourceMap(a, tokenizer, documents, prefixed, &context);
    errdefer if (sources) |owned| a.free(owned);
    var batch = try assembleBatchBounded(a, tokenizer, &context, question, bound);
    batch.sources = sources;
    return batch;
}

/// The span copy half's batch (`qwen_span_copy.span_features`): the same
/// prefix, self-context and source map as `prepareBounded`, with every
/// doubled document encoded after the question prefix
/// (`memorilla_span.questionPrefix`); the kept tail ids are verified
/// unchanged. The question row is still appended (its states are unused).
pub fn prepareSpanBounded(a: A, tokenizer: tok.Tokenizer, documents: []const []const u8, question: []const u8, bound: usize) !Batch {
    if (documents.len == 0 or documents.len > qualified.max_documents) return error.InvalidMemorillaDocumentCount;
    const prefixed = try a.alloc([]u8, documents.len);
    var built: usize = 0;
    defer {
        for (prefixed[0..built]) |text| a.free(text);
        a.free(prefixed);
    }
    for (documents, prefixed) |document, *text| {
        text.* = try prefixDocument(a, document);
        built += 1;
    }
    var context = try selfContextTokens(a, tokenizer, prefixed);
    defer context.deinit();
    const sources = try sourceMap(a, tokenizer, documents, prefixed, &context);
    errdefer if (sources) |owned| a.free(owned);
    const prefix = try span_window.questionPrefix(a, question);
    defer a.free(prefix);
    const conditioned = try span_window.prefixDoubledTexts(a, tokenizer, context.texts, context.counts, prefix);
    for (context.texts, conditioned) |*text, joined| {
        a.free(text.*);
        text.* = joined;
    }
    a.free(conditioned);
    var batch = try assembleBatchBounded(a, tokenizer, &context, question, bound);
    batch.sources = sources;
    return batch;
}

/// Number of selected document rows `M = sum(n_i + 1)`.
pub fn selectedRowCount(counts: []const usize) !usize {
    var total: usize = 0;
    for (counts) |count| total = try std.math.add(usize, total, try std.math.add(usize, count, 1));
    return total;
}

fn checkStates(states: []const f32, batch: usize, seq_len: usize, row_width: usize, active: []const usize) !void {
    if (batch < 2 or seq_len == 0 or row_width == 0 or active.len != batch) return error.InvalidMemorillaStates;
    const expected = try std.math.mul(usize, try std.math.mul(usize, batch, seq_len), row_width);
    if (states.len != expected) return error.InvalidMemorillaStates;
    for (active) |length| if (length == 0 or length > seq_len) return error.InvalidMemorillaStates;
}

/// `self_context_states` for the documents of one `[batch, seq_len, row_width]`
/// host tensor: rows `0..batch-1` are documents, the last row is the question
/// (not selected here). Returns `[M, row_width]` with each document's last
/// `counts[i] + 1` active rows in order (second copy, then its EOS state).
pub fn selectRows(
    a: A,
    states: []const f32,
    batch: usize,
    seq_len: usize,
    row_width: usize,
    active: []const usize,
    counts: []const usize,
) ![]f32 {
    try checkStates(states, batch, seq_len, row_width, active);
    if (counts.len != batch - 1) return error.InvalidMemorillaStates;
    const rows = try selectedRowCount(counts);
    const out = try a.alloc(f32, try std.math.mul(usize, rows, row_width));
    errdefer a.free(out);
    var cursor: usize = 0;
    for (counts, active[0 .. batch - 1], 0..) |count, length, index| {
        const keep = count + 1;
        if (keep > length) return error.InvalidMemorillaStates;
        const first = length - keep;
        const source = states[(index * seq_len + first) * row_width ..][0 .. keep * row_width];
        @memcpy(out[cursor * row_width ..][0 .. keep * row_width], source);
        cursor += keep;
    }
    std.debug.assert(cursor == rows);
    return out;
}

/// The question's last active row (its EOS state), a view into `states`.
pub fn questionLastRow(states: []const f32, batch: usize, seq_len: usize, row_width: usize, active: []const usize) ![]const f32 {
    try checkStates(states, batch, seq_len, row_width, active);
    const row = batch - 1;
    return states[(row * seq_len + active[row] - 1) * row_width ..][0..row_width];
}

/// `F.normalize(x, p=2, dim=-1)` in F32: `x / max(||x||, 1e-12)`. A zero or
/// non-finite row is an encoder failure, as in Python.
pub fn normalizeRow(row: []f32) !void {
    var sum: f64 = 0;
    for (row) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
        sum += @as(f64, value) * value;
    }
    const norm: f32 = @floatCast(@sqrt(sum));
    if (norm == 0) return error.ZeroMemorillaFeature;
    const denominator = @max(norm, normalize_eps);
    for (row) |*value| value.* /= denominator;
}

/// `feature_channel`: F32 -> FP16 (RNE, subnormals kept) -> BF16 (RNE) -> F32.
pub fn featureChannel(row: []f32) !void {
    for (row) |*value| value.* = (try encoder.storedValue(value.*)).memory;
}

pub fn normalizeAndChannel(row: []f32) !void {
    try normalizeRow(row);
    try featureChannel(row);
}

fn normalizeRows(values: []f32, row_width: usize) !void {
    var offset: usize = 0;
    while (offset < values.len) : (offset += row_width) try normalizeAndChannel(values[offset..][0..row_width]);
}

/// Reader input: `docs` `[rows, width]` = final-layer rows then layer-24 rows
/// (`rows = 2M`), `query` `[width]`, the copy sources and per-document counts.
pub const Features = struct {
    a: A,
    docs: []f32,
    rows: usize,
    query: []f32,
    copy_ids: []u32,
    counts: []usize,
    /// Source map parallel to `copy_ids`, or null (see `Batch.sources`).
    sources: ?[]copy.SourceRange = null,

    pub fn deinit(self: *Features) void {
        self.a.free(self.docs);
        self.a.free(self.query);
        self.a.free(self.copy_ids);
        self.a.free(self.counts);
        if (self.sources) |sources| self.a.free(sources);
        self.* = undefined;
    }
};

/// `document_features_dual` over the two host tensors returned for `batch`:
/// `final` is `last_hidden_state`, `tap` is `hidden_states_24`, both
/// `[batch.batch, batch.seq_len, width]` F32.
pub fn features(a: A, final: []const f32, tap: []const f32, batch: *const Batch) !Features {
    return featuresWithWidth(a, final, tap, batch, width);
}

pub fn featuresWithWidth(a: A, final: []const f32, tap: []const f32, batch: *const Batch, row_width: usize) !Features {
    if (final.len != tap.len) return error.InvalidMemorillaStates;
    const final_rows = try selectRows(a, final, batch.batch, batch.seq_len, row_width, batch.active, batch.counts);
    defer a.free(final_rows);
    const tap_rows = try selectRows(a, tap, batch.batch, batch.seq_len, row_width, batch.active, batch.counts);
    defer a.free(tap_rows);
    const docs = try a.alloc(f32, final_rows.len + tap_rows.len);
    errdefer a.free(docs);
    @memcpy(docs[0..final_rows.len], final_rows);
    @memcpy(docs[final_rows.len..], tap_rows);
    try normalizeRows(docs, row_width);

    const query = try a.dupe(f32, try questionLastRow(final, batch.batch, batch.seq_len, row_width, batch.active));
    errdefer a.free(query);
    try normalizeAndChannel(query);

    const copy_ids = try a.dupe(u32, batch.copy_ids);
    errdefer a.free(copy_ids);
    const counts = try a.dupe(usize, batch.counts);
    errdefer a.free(counts);
    const sources: ?[]copy.SourceRange = if (batch.sources) |map| try a.dupe(copy.SourceRange, map) else null;
    return .{
        .a = a,
        .docs = docs,
        .rows = docs.len / row_width,
        .query = query,
        .copy_ids = copy_ids,
        .counts = counts,
        .sources = sources,
    };
}

// --- tests ---

/// Word-level stand-in for the Qwen tokenizer: a word preceded by a space maps
/// to `hash(word)`, a sentence-initial word (no space before it) to
/// `hash(word) + 100_000`, mirroring "Mujapo" -> M/uj/apo versus " Muj"/apo.
const WordTokenizer = struct {
    var sentinel: u8 = 0;
    fn hash(word: []const u8) i32 {
        var value: u32 = 7;
        for (word) |byte| value = value *% 31 +% byte;
        return @intCast(value % 50_000 + 1);
    }
    fn encode(_: *anyopaque, a: A, text: []const u8) ![]i32 {
        var ids = std.ArrayListUnmanaged(i32).empty;
        errdefer ids.deinit(a);
        var start: usize = 0;
        var index: usize = 0;
        while (index <= text.len) : (index += 1) {
            if (index == text.len or text[index] == ' ') {
                if (index > start) {
                    const initial = start == 0;
                    try ids.append(a, hash(text[start..index]) + @as(i32, if (initial) 100_000 else 0));
                }
                start = index + 1;
            }
        }
        return ids.toOwnedSlice(a);
    }
    fn append(raw: *anyopaque, a: A, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        const ids = try encode(raw, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }
    fn decode(_: *anyopaque, a: A, _: []const i32) ![]u8 {
        return a.dupe(u8, "");
    }
    fn unused(_: *anyopaque, _: A, _: []const u8, _: usize) !tok.EncodeResult {
        return error.NotUsed;
    }
    fn unusedGeneration(_: *anyopaque, _: A, _: []const u8, _: usize, _: bool) !tok.EncodeResult {
        return error.NotUsed;
    }
    fn specials(_: *anyopaque) tok.SpecialTokens {
        return .{};
    }
    fn size(_: *anyopaque) usize {
        return 200_000;
    }
    fn deinit(_: *anyopaque) void {}
    fn handle() tok.Tokenizer {
        return .{ .ptr = &sentinel, .vtable = &.{ .encode = encode, .encodeInto = append, .encodeForModel = unused, .encodeGeneration = unusedGeneration, .decode = decode, .specialTokens = specials, .vocabSize = size, .deinit = deinit } };
    }
};

/// Every id is its position: a doubled text never ends with its own ids.
const PositionTokenizer = struct {
    var sentinel: u8 = 0;
    fn encode(_: *anyopaque, a: A, text: []const u8) ![]i32 {
        const out = try a.alloc(i32, text.len);
        for (out, 0..) |*id, index| id.* = @intCast(index);
        return out;
    }
    fn handle() tok.Tokenizer {
        return .{ .ptr = &sentinel, .vtable = &.{ .encode = encode, .encodeInto = WordTokenizer.append, .encodeForModel = WordTokenizer.unused, .encodeGeneration = WordTokenizer.unusedGeneration, .decode = WordTokenizer.decode, .specialTokens = WordTokenizer.specials, .vocabSize = WordTokenizer.size, .deinit = WordTokenizer.deinit } };
    }
};

/// Lossless byte-level stand-in for the Qwen BPE: text splits into units of
/// one optional leading space plus a run of non-space bytes (a space before
/// another space, or a trailing space, is its own unit). ASCII units are
/// interned as ids whose piece is the unit's bytes; a sentence-initial unit
/// (text start, no leading space) gets a distinct id with the same bytes, so
/// an unprefixed document takes the self-context fallback as with Qwen.
/// Non-ASCII units become one token per byte, splitting multibyte UTF-8
/// characters across pieces. `lowercase` imitates a normalizer that rewrites
/// bytes, which the source map must detect.
const PieceTokenizer = struct {
    a: A,
    units: std.ArrayListUnmanaged(Unit) = .empty,
    lowercase: bool = false,

    const Unit = struct { bytes: []u8, initial: bool };
    const byte_base: i32 = 1_000_000;

    fn deinitSelf(self: *PieceTokenizer) void {
        for (self.units.items) |unit| self.a.free(unit.bytes);
        self.units.deinit(self.a);
    }

    fn intern(self: *PieceTokenizer, bytes: []const u8, initial: bool) !i32 {
        for (self.units.items, 0..) |unit, index| {
            if (unit.initial == initial and std.mem.eql(u8, unit.bytes, bytes)) return @intCast(index);
        }
        const owned = try self.a.dupe(u8, bytes);
        errdefer self.a.free(owned);
        try self.units.append(self.a, .{ .bytes = owned, .initial = initial });
        return @intCast(self.units.items.len - 1);
    }

    fn encode(raw: *anyopaque, a: A, text: []const u8) ![]i32 {
        const self: *PieceTokenizer = @ptrCast(@alignCast(raw));
        var ids = std.ArrayListUnmanaged(i32).empty;
        errdefer ids.deinit(a);
        var index: usize = 0;
        while (index < text.len) {
            var end = index;
            if (text[end] == ' ') end += 1;
            while (end < text.len and text[end] != ' ') end += 1;
            const unit = text[index..end];
            const ascii = for (unit) |byte| {
                if (byte >= 0x80) break false;
            } else true;
            if (ascii) {
                try ids.append(a, try self.intern(unit, index == 0 and unit[0] != ' '));
            } else {
                for (unit) |byte| try ids.append(a, byte_base + byte);
            }
            index = end;
        }
        return ids.toOwnedSlice(a);
    }

    fn append(raw: *anyopaque, a: A, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        const ids = try encode(raw, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }

    fn decode(raw: *anyopaque, a: A, ids: []const i32) ![]u8 {
        const self: *PieceTokenizer = @ptrCast(@alignCast(raw));
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(a);
        for (ids) |id| {
            if (id >= byte_base) {
                try out.append(a, @intCast(id - byte_base));
            } else {
                if (id < 0 or @as(usize, @intCast(id)) >= self.units.items.len) return error.UnknownToken;
                try out.appendSlice(a, self.units.items[@intCast(id)].bytes);
            }
        }
        if (self.lowercase) for (out.items) |*byte| {
            byte.* = std.ascii.toLower(byte.*);
        };
        return out.toOwnedSlice(a);
    }

    fn size(_: *anyopaque) usize {
        return 2_000_000;
    }

    fn handle(self: *PieceTokenizer) tok.Tokenizer {
        return .{ .ptr = self, .vtable = &.{ .encode = encode, .encodeInto = append, .encodeForModel = WordTokenizer.unused, .encodeGeneration = WordTokenizer.unusedGeneration, .decode = decode, .specialTokens = WordTokenizer.specials, .vocabSize = size, .deinit = WordTokenizer.deinit } };
    }
};

fn expectRanges(expected: []const [3]u32, actual: []const copy.SourceRange) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, x| {
        try std.testing.expectEqual(e[0], x.document);
        try std.testing.expectEqual(e[1], x.start);
        try std.testing.expectEqual(e[2], x.end);
    }
}

/// Each document's ranges tile its original text in order: concatenating
/// the cited bytes of its copy ids reproduces the text exactly.
fn expectTiles(documents: []const []const u8, sources: []const copy.SourceRange) !void {
    var cursor: usize = 0;
    var document: usize = 0;
    for (sources) |range| {
        if (range.document != document) {
            try std.testing.expectEqual(documents[document].len, cursor);
            try std.testing.expectEqual(document + 1, range.document);
            document = range.document;
            cursor = 0;
        }
        try std.testing.expectEqual(cursor, range.start);
        cursor = range.end;
    }
    try std.testing.expectEqual(documents.len - 1, document);
    try std.testing.expectEqual(documents[document].len, cursor);
}

test "memorilla token features source map round-trips prefixed, spaced and multibyte documents" {
    const a = std.testing.allocator;
    var pieces = PieceTokenizer{ .a = a };
    defer pieces.deinitSelf();
    const tokenizer = pieces.handle();
    // D0 gets the space prefix; D1 already has it and splits "ë" into bytes.
    const documents = [_][]const u8{ "Mujapo Depot code QX7.", " Zo\xc3\xab holds it" };
    var batch = try prepare(a, tokenizer, &documents, "Which code?");
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 0), batch.fallbacks);
    const sources = batch.sources orelse return error.TestExpectedSourceMap;
    try std.testing.expectEqual(batch.copy_ids.len, sources.len);
    try expectRanges(&.{
        // " Mujapo" covers the added space and starts at byte 0.
        .{ 0, 0, 6 }, .{ 0, 6, 12 }, .{ 0, 12, 17 }, .{ 0, 17, 22 },
        // " Zoë" as five byte pieces; "ë" = C3 AB spans two of them.
        .{ 1, 0, 1 }, .{ 1, 1, 2 },  .{ 1, 2, 3 },   .{ 1, 3, 4 },
        .{ 1, 4, 5 }, .{ 1, 5, 11 }, .{ 1, 11, 14 },
    }, sources);
    try expectTiles(&documents, sources);
    try std.testing.expectEqualStrings("\xc3", documents[1][sources[7].start..sources[7].end]);
    // Every piece outside the added prefix is exactly its cited bytes.
    for (batch.copy_ids, sources, 0..) |id, range, index| {
        const piece = try tokenizer.tokenBytes(a, @intCast(id));
        defer a.free(piece);
        const cited = documents[range.document][range.start..range.end];
        if (index == 0) {
            try std.testing.expectEqualStrings(" Mujapo", piece);
            try std.testing.expectEqualStrings("Mujapo", cited);
        } else try std.testing.expectEqualStrings(piece, cited);
    }

    // The reader features carry the same map.
    const row_width: usize = 2;
    const states = try a.alloc(f32, batch.batch * batch.seq_len * row_width);
    defer a.free(states);
    @memset(states, 1);
    var result = try featuresWithWidth(a, states, states, &batch, row_width);
    defer result.deinit();
    try expectRanges(&.{ .{ 0, 0, 6 }, .{ 0, 6, 12 } }, result.sources.?[0..2]);
    try std.testing.expectEqual(sources.len, result.sources.?.len);
}

test "memorilla token features source map follows the self-context fallback" {
    const a = std.testing.allocator;
    var pieces = PieceTokenizer{ .a = a };
    defer pieces.deinitSelf();
    const tokenizer = pieces.handle();
    // Handed over unprefixed, "Mujapo Depot" re-segments mid-text and its copy
    // ids come from encode(" " + D): one leading space more than the text.
    const documents = [_][]const u8{ "Mujapo Depot", " Site code" };
    var context = try selfContextTokens(a, tokenizer, &documents);
    defer context.deinit();
    try std.testing.expectEqualSlices(bool, &.{ true, false }, context.fallback_flags);
    const sources = (try sourceMap(a, tokenizer, &documents, &documents, &context)) orelse return error.TestExpectedSourceMap;
    defer a.free(sources);
    try expectRanges(&.{ .{ 0, 0, 6 }, .{ 0, 6, 12 }, .{ 1, 0, 5 }, .{ 1, 5, 10 } }, sources);
    try expectTiles(&documents, sources);

    // Prefix plus fallback: two added spaces, the first piece is all prefix.
    const ids = try tokenizer.encode(a, "  ab");
    defer a.free(ids);
    var wide: [2]u32 = undefined;
    for (ids, &wide) |id, *w| w.* = @intCast(id);
    var out: [2]copy.SourceRange = undefined;
    try std.testing.expect(try mapDocumentSources(a, tokenizer, 3, "ab", 2, &wide, &out));
    try expectRanges(&.{ .{ 3, 0, 0 }, .{ 3, 0, 2 } }, &out);
    // Pieces that do not tile the text exactly are rejected.
    try std.testing.expect(!try mapDocumentSources(a, tokenizer, 3, "ab", 1, &wide, &out));
    try std.testing.expect(!try mapDocumentSources(a, tokenizer, 3, "abc", 2, &wide, &out));
    try std.testing.expect(!try mapDocumentSources(a, tokenizer, 3, "ab", 2, wide[0..1], out[0..1]));
    try std.testing.expect(!try mapDocumentSources(a, tokenizer, 3, "xb", 2, &wide, &out));
    // An encoded text that does not end with its original has no map.
    try std.testing.expect((try sourceMap(a, tokenizer, &.{ "Mujapo Depot", "other" }, &documents, &context)) == null);
}

test "memorilla token features source map fails closed when pieces rewrite the text" {
    const a = std.testing.allocator;
    var pieces = PieceTokenizer{ .a = a, .lowercase = true };
    defer pieces.deinitSelf();
    // A normalizer-like rewrite: the request still prepares, without a map.
    var batch = try prepare(a, pieces.handle(), &.{ "Mujapo Depot", "code QX7" }, "Which code?");
    defer batch.deinit();
    try std.testing.expect(batch.sources == null);
    try std.testing.expectEqual(@as(usize, 4), batch.copy_ids.len);
    // Lowercase text survives the rewrite unchanged and maps.
    var lower = try prepare(a, pieces.handle(), &.{"mujapo depot"}, "q");
    defer lower.deinit();
    try std.testing.expect(lower.sources != null);
    // The word stand-in decodes every id to "", which never tiles a text.
    var words = try prepare(a, WordTokenizer.handle(), &.{"a b"}, "q");
    defer words.deinit();
    try std.testing.expect(words.sources == null);
}

test "memorilla token features prefix each document with one space" {
    const a = std.testing.allocator;
    const prefixed = try prefixDocument(a, "Antfly stores versions.");
    defer a.free(prefixed);
    try std.testing.expectEqualStrings(" Antfly stores versions.", prefixed);
    const kept = try prefixDocument(a, " already prefixed");
    defer a.free(kept);
    try std.testing.expectEqualStrings(" already prefixed", kept);
    const empty = try prefixDocument(a, "");
    defer a.free(empty);
    try std.testing.expectEqualStrings(" ", empty);
}

test "memorilla token features self-context doubles documents with the fallback rule" {
    const a = std.testing.allocator;
    const tokenizer = WordTokenizer.handle();
    const muj = WordTokenizer.hash("Mujapo");
    const depot = WordTokenizer.hash("Depot");

    // Prefixed text: the doubled text ends with encode(D) directly.
    const prefixed = try selfContextDocument(a, tokenizer, " Mujapo Depot");
    defer a.free(prefixed.doubled);
    defer a.free(prefixed.single_ids);
    try std.testing.expectEqualStrings(" Mujapo Depot Mujapo Depot", prefixed.doubled);
    try std.testing.expectEqualSlices(i32, &.{ muj, depot }, prefixed.single_ids);
    try std.testing.expect(!prefixed.fallback);

    // Sentence-initial word re-segments mid-text: fall back to encode(" " + D).
    const raw = try selfContextDocument(a, tokenizer, "Mujapo Depot");
    defer a.free(raw.doubled);
    defer a.free(raw.single_ids);
    try std.testing.expectEqualStrings("Mujapo Depot Mujapo Depot", raw.doubled);
    try std.testing.expectEqualSlices(i32, &.{ muj, depot }, raw.single_ids);
    try std.testing.expect(raw.fallback);

    try std.testing.expectError(error.MemorillaSelfContextMismatch, selfContextDocument(a, PositionTokenizer.handle(), " abc"));
    try std.testing.expectError(error.BlankMemorillaDocument, selfContextDocument(a, tokenizer, "   "));

    var context = try selfContextTokens(a, tokenizer, &.{ " Mujapo Depot", "Site code 7" });
    defer context.deinit();
    try std.testing.expectEqual(@as(usize, 2), context.texts.len);
    try std.testing.expectEqualSlices(usize, &.{ 2, 3 }, context.counts);
    try std.testing.expectEqual(@as(usize, 1), context.fallbacks);
    const expected_copy = [_]u32{ @intCast(muj), @intCast(depot), @intCast(WordTokenizer.hash("Site")), @intCast(WordTokenizer.hash("code")), @intCast(WordTokenizer.hash("7")) };
    try std.testing.expectEqualSlices(u32, &expected_copy, context.copy_ids);
    try std.testing.expectError(error.InvalidMemorillaDocumentCount, selfContextTokens(a, tokenizer, &.{}));
}

test "memorilla token features batch appends EOS, right pads and bounds the length" {
    const a = std.testing.allocator;
    const tokenizer = WordTokenizer.handle();
    var batch = try prepare(a, tokenizer, &.{ "a b", " c" }, "q r");
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), batch.batch);
    try std.testing.expectEqual(@as(usize, 2), batch.documentCount());
    try std.testing.expectEqual(@as(usize, 5), batch.seq_len);
    try std.testing.expectEqualSlices(usize, &.{ 5, 3, 3 }, batch.active);
    try std.testing.expectEqualSlices(usize, &.{ 2, 1 }, batch.counts);
    try std.testing.expectEqual(@as(usize, 0), batch.fallbacks);
    const ha = WordTokenizer.hash("a");
    const hb = WordTokenizer.hash("b");
    const hc = WordTokenizer.hash("c");
    const hq = WordTokenizer.hash("q") + 100_000;
    const hr = WordTokenizer.hash("r");
    const expected = [_]i64{
        ha, hb, ha,     hb,     eos_id,
        hc, hc, eos_id, pad_id, pad_id,
        hq, hr, eos_id, pad_id, pad_id,
    };
    try std.testing.expectEqualSlices(i64, &expected, batch.input_ids);
    try std.testing.expectEqualSlices(u32, &.{ @intCast(ha), @intCast(hb), @intCast(hc) }, batch.copy_ids);

    // 64 words doubled -> 128 ids + EOS = 129 > 128.
    var long_buf: [64 * 4]u8 = undefined;
    var long_len: usize = 0;
    for (0..64) |index| long_len += (try std.fmt.bufPrint(long_buf[long_len..], " w{d}", .{index})).len;
    const long = long_buf[0..long_len];
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, prepare(a, tokenizer, &.{long}, "q"));
    // 63 words doubled -> 126 ids + EOS = 127 fits.
    var fits = try prepare(a, tokenizer, &.{long[0 .. long.len - " w63".len]}, "q");
    defer fits.deinit();
    try std.testing.expectEqual(@as(usize, 127), fits.seq_len);
    try std.testing.expectError(error.BlankMemorillaQuestion, prepare(a, tokenizer, &.{"a"}, " "));
    // The hybrid bound admits the 129-token row and still rejects 1025.
    var wide = try prepareBounded(a, tokenizer, &.{long}, "q", hybrid_max_padded_length);
    defer wide.deinit();
    try std.testing.expectEqual(@as(usize, 129), wide.seq_len);
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, prepareBounded(a, tokenizer, &.{long}, "q", 128));
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, prepareBounded(a, tokenizer, &.{"a"}, "q", hybrid_max_padded_length + 1));
    var huge_buf: [512 * 5]u8 = undefined;
    var huge_len: usize = 0;
    for (0..512) |index| huge_len += (try std.fmt.bufPrint(huge_buf[huge_len..], " w{d}", .{index})).len;
    // 512 words doubled -> 1024 ids + EOS = 1025 > 1024.
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, prepareBounded(a, tokenizer, &.{huge_buf[0..huge_len]}, "q", hybrid_max_padded_length));
}

test "memorilla token features span batch prefixes every doubled document with the question" {
    const a = std.testing.allocator;
    const tokenizer = WordTokenizer.handle();
    var plain = try prepareBounded(a, tokenizer, &.{ "a b", " c" }, "q  r", hybrid_max_padded_length);
    defer plain.deinit();
    var prefixed = try prepareSpanBounded(a, tokenizer, &.{ "a b", " c" }, "q  r", hybrid_max_padded_length);
    defer prefixed.deinit();
    // Same copy sources, counts and source map; only the document rows grow.
    try std.testing.expectEqualSlices(u32, plain.copy_ids, prefixed.copy_ids);
    try std.testing.expectEqualSlices(usize, plain.counts, prefixed.counts);
    try std.testing.expectEqual(plain.batch, prefixed.batch);
    // The word tokenizer has no offsets, so neither batch carries a source map.
    try std.testing.expectEqual(plain.sources == null, prefixed.sources == null);
    if (plain.sources) |sources| {
        try std.testing.expectEqual(sources.len, prefixed.sources.?.len);
        for (sources, prefixed.sources.?) |x, y| try std.testing.expect(std.meta.eql(x, y));
    }
    // "Question: q r\nPassage:" adds the words "Question:" (initial), "q",
    // "r\nPassage:" in front of each doubled document.
    const head = [_]i64{ WordTokenizer.hash("Question:") + 100_000, WordTokenizer.hash("q"), WordTokenizer.hash("r\nPassage:") };
    for (0..2) |row| {
        const p_row = prefixed.input_ids[row * prefixed.seq_len ..][0..prefixed.active[row]];
        const q_row = plain.input_ids[row * plain.seq_len ..][0..plain.active[row]];
        try std.testing.expectEqual(q_row.len + head.len, p_row.len);
        try std.testing.expectEqualSlices(i64, &head, p_row[0..head.len]);
        try std.testing.expectEqualSlices(i64, q_row, p_row[head.len..]);
    }
}

test "memorilla token features select the last n_i + 1 rows per document and the question EOS row" {
    const a = std.testing.allocator;
    const batch: usize = 3;
    const seq_len: usize = 5;
    const row_width: usize = 2;
    var states: [batch * seq_len * row_width]f32 = undefined;
    for (0..batch) |b| for (0..seq_len) |t| {
        const value: f32 = @floatFromInt(b * 100 + t);
        states[(b * seq_len + t) * row_width] = value;
        states[(b * seq_len + t) * row_width + 1] = -value;
    };
    const active = [_]usize{ 5, 3, 3 };
    const counts = [_]usize{ 1, 1 };
    const rows = try selectRows(a, &states, batch, seq_len, row_width, &active, &counts);
    defer a.free(rows);
    try std.testing.expectEqualSlices(f32, &.{ 3, -3, 4, -4, 101, -101, 102, -102 }, rows);
    const question = try questionLastRow(&states, batch, seq_len, row_width, &active);
    try std.testing.expectEqualSlices(f32, &.{ 202, -202 }, question);
    try std.testing.expectError(error.InvalidMemorillaStates, selectRows(a, &states, batch, seq_len, row_width, &active, &.{ 5, 1 }));
    try std.testing.expectError(error.InvalidMemorillaStates, selectRows(a, &states, batch, seq_len, row_width, &.{ 5, 3, 6 }, &counts));
    try std.testing.expectError(error.InvalidMemorillaStates, selectRows(a, states[0 .. states.len - 1], batch, seq_len, row_width, &active, &counts));
}

test "memorilla token features normalize in F32 and round through FP16 then BF16" {
    var row = [_]f32{ 3, 4, 0, 0 };
    try normalizeRow(&row);
    try std.testing.expectEqualSlices(f32, &.{ 0.6, 0.8, 0, 0 }, &row);
    var zero = [_]f32{ 0, 0 };
    try std.testing.expectError(error.ZeroMemorillaFeature, normalizeRow(&zero));
    var bad = [_]f32{ 1, std.math.nan(f32) };
    try std.testing.expectError(error.NonFiniteMemorillaFeature, normalizeRow(&bad));

    // FP16 ties to even: 1 + 2^-11 sits between 1 and 1 + 2^-10 -> 1.
    var tie_down = [_]f32{1.0 + 0.00048828125};
    try featureChannel(&tie_down);
    try std.testing.expectEqual(@as(f32, 1.0), tie_down[0]);
    // 1 + 3 * 2^-11 rounds up to 1 + 2^-9 in FP16 (even mantissa), then BF16
    // (7 mantissa bits) drops 2^-9 (below half of 2^-7) -> 1.
    try std.testing.expectEqual(@as(f16, 1.001953125), (try encoder.storedValue(1.0 + 3 * 0.00048828125)).fp16);
    var up_then_down = [_]f32{1.0 + 3 * 0.00048828125};
    try featureChannel(&up_then_down);
    try std.testing.expectEqual(@as(f32, 1.0), up_then_down[0]);
    // BF16 ties to even: 1 + 2^-8 (exact in FP16) -> 1; 1 + 3 * 2^-8 -> 1 + 2^-6.
    var bf_tie = [_]f32{ 1.0 + 0.00390625, 1.0 + 3 * 0.00390625 };
    try featureChannel(&bf_tie);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 1.015625 }, &bf_tie);
    // FP16 subnormals survive both roundings.
    var tiny = [_]f32{std.math.pow(f32, 2, -20)};
    try featureChannel(&tiny);
    try std.testing.expectEqual(std.math.pow(f32, 2, -20), tiny[0]);
    // Both halves of a normalized vector pass through the same channel.
    var pair = [_]f32{ 0.6, 0.8 };
    try featureChannel(&pair);
    try std.testing.expectEqual(@as(f32, 0.6015625), pair[0]);
    try std.testing.expectEqual(@as(f32, 0.80078125), pair[1]);
}

test "memorilla token features build docs as final rows then tap rows and the final-layer query" {
    const a = std.testing.allocator;
    const tokenizer = WordTokenizer.handle();
    var batch = try prepare(a, tokenizer, &.{ "a b", " c" }, "q r");
    defer batch.deinit();
    const row_width: usize = 2;
    const total = batch.batch * batch.seq_len * row_width;
    const final = try a.alloc(f32, total);
    defer a.free(final);
    const tap = try a.alloc(f32, total);
    defer a.free(tap);
    for (0..batch.batch) |b| for (0..batch.seq_len) |t| {
        const base: f32 = @floatFromInt(b * 100 + t + 1);
        final[(b * batch.seq_len + t) * row_width] = base;
        final[(b * batch.seq_len + t) * row_width + 1] = 0;
        tap[(b * batch.seq_len + t) * row_width] = 0;
        tap[(b * batch.seq_len + t) * row_width + 1] = -base;
    };
    var result = try featuresWithWidth(a, final, tap, &batch, row_width);
    defer result.deinit();
    // M = (2 + 1) + (1 + 1) = 5 rows per layer.
    try std.testing.expectEqual(@as(usize, 10), result.rows);
    try std.testing.expectEqual(@as(usize, 20), result.docs.len);
    for (result.docs[0..10], 0..) |value, index| try std.testing.expectEqual(@as(f32, if (index % 2 == 0) 1 else 0), value);
    for (result.docs[10..], 0..) |value, index| try std.testing.expectEqual(@as(f32, if (index % 2 == 0) 0 else -1), value);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0 }, result.query);
    try std.testing.expectEqualSlices(u32, batch.copy_ids, result.copy_ids);
    try std.testing.expectEqualSlices(usize, &.{ 2, 1 }, result.counts);
    try std.testing.expectError(error.InvalidMemorillaStates, featuresWithWidth(a, final, tap[1..], &batch, row_width));
}

test "memorilla token features agree with the Qwen3 encoder tokenizer" {
    const platform = @import("antfly_platform");
    const directory = platform.env.getenv("ANTFLY_MEMORILLA_ENCODER_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    defer a.free(bytes);
    const hf = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer hf.deinitSelf();
    const tokenizer = hf.tokenizer();

    // scripts/memorilla/fixtures/encoder-tokenizer.json "short_document": the
    // pipeline appends 151643; `encode` itself matches add_special_tokens=False.
    const short = try tokenizer.encode(a, "Antfly stores immutable source versions.");
    defer a.free(short);
    try std.testing.expectEqualSlices(i32, &.{ 17117, 21642, 10533, 51572, 2530, 10795, 13 }, short);

    // A prefixed record starts with one leading-space token and its doubled
    // text ends with its own ids (no fallback).
    const context = try selfContextDocument(a, tokenizer, " At Mujapo Depot the owner code is QX7.");
    defer a.free(context.doubled);
    defer a.free(context.single_ids);
    try std.testing.expect(!context.fallback);
    const first = try tokenizer.decodeToken(a, context.single_ids[0], false);
    defer a.free(first);
    try std.testing.expectEqualStrings(" At", first);
    const second = try tokenizer.decodeToken(a, context.single_ids[1], false);
    defer a.free(second);
    try std.testing.expect(std.mem.startsWith(u8, second, " "));

    // The sentence-initial segmentation differs ("Mujapo" -> M/uj/apo), so an
    // unprefixed document takes the fallback and still yields the spaced ids.
    const raw = try selfContextDocument(a, tokenizer, "Mujapo Depot owner code QX7.");
    defer a.free(raw.doubled);
    defer a.free(raw.single_ids);
    const spaced = try tokenizer.encode(a, " Mujapo Depot owner code QX7.");
    defer a.free(spaced);
    try std.testing.expectEqualSlices(i32, spaced, raw.single_ids);

    var batch = try prepare(a, tokenizer, &.{ "At Mujapo Depot the owner code is QX7.", " Site Rendal Yard, holder Ana." }, "Which code belongs to Mujapo Depot?");
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 3), batch.batch);
    try std.testing.expect(batch.seq_len <= max_padded_length);
    for (batch.active, 0..) |length, row| {
        try std.testing.expectEqual(@as(i64, eos_id), batch.input_ids[row * batch.seq_len + length - 1]);
        if (row < 2) try std.testing.expect(batch.counts[row] + 1 <= length);
    }
    try std.testing.expectEqual(batch.counts[0] + batch.counts[1], batch.copy_ids.len);

    // Source map: the real byte-level pieces tile every caller text, with
    // multibyte characters and punctuation, and the first piece of a prefixed
    // document starts at byte 0.
    const texts = [_][]const u8{ "At Mujapo Depot the owner code is QX7.", " Site Rendal Yard, holder Zo\u{eb} \u{c5}ngstr\u{f6}m.", "Code Kq9-0012 opens the gate \u{2014} gate 3." };
    var mapped = try prepare(a, tokenizer, &texts, "Which code belongs to Mujapo Depot?");
    defer mapped.deinit();
    const sources = mapped.sources orelse return error.TestExpectedSourceMap;
    try std.testing.expectEqual(mapped.copy_ids.len, sources.len);
    try expectTiles(&texts, sources);
    try std.testing.expectEqual(@as(u32, 0), sources[0].start);
    try std.testing.expectEqualStrings("At", texts[0][sources[0].start..sources[0].end]);
    // NFC composes a decomposed accent, so the pieces no longer reproduce the
    // supplied bytes: no map, the batch itself is unaffected.
    var decomposed = try prepare(a, tokenizer, &.{"Zoe\u{301} holds the key."}, "Who holds the key?");
    defer decomposed.deinit();
    try std.testing.expect(decomposed.sources == null);
    try std.testing.expect(decomposed.copy_ids.len > 0);
}

/// One case of an `encoder_lora_eval.py` reference receipt (only the fields
/// the citation replay needs).
const ReferenceCase = struct {
    name: []const u8,
    documents: []const []const u8,
    question: []const u8,
    answer: []const u8,
    expected_answer: []const u8,
    copy_ids: []const i64,
    spans: []const [3]usize,
    snapped_ids: []const i64,
    completed_ids: []const i64,
};

fn narrowIds(a: A, ids: []const i64) ![]i32 {
    const out = try a.alloc(i32, ids.len);
    for (ids, out) |id, *o| o.* = @intCast(id);
    return out;
}

test "memorilla token features cite the Python reference cases with the Qwen3 tokenizer" {
    // CPU replay of the serving citation pipeline over reference receipts:
    // the real tokenizer's source map, the Python copy-head spans and snapped
    // ids, word completion (must reproduce the reference record) and
    // citations checked against the case documents. Set
    // ANTFLY_MEMORILLA_LORA_REFERENCE_DIRS to ':'-separated lora-ref-* dirs.
    const platform = @import("antfly_platform");
    const directory = platform.env.getenv("ANTFLY_MEMORILLA_ENCODER_MODEL_DIR") orelse return error.SkipZigTest;
    const references = platform.env.getenv("ANTFLY_MEMORILLA_LORA_REFERENCE_DIRS") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const c_file = @import("../util/c_file.zig");
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const bytes = try c_file.readFile(a, path);
    defer a.free(bytes);
    const hf = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer hf.deinitSelf();
    const tokenizer = hf.tokenizer();

    var dirs = std.mem.tokenizeScalar(u8, references, ':');
    while (dirs.next()) |reference_dir| {
        const receipt_path = try std.fs.path.join(a, &.{ reference_dir, "reference.json" });
        defer a.free(receipt_path);
        const receipt = try c_file.readFile(a, receipt_path);
        defer a.free(receipt);
        var parsed = try std.json.parseFromSlice(struct { cases: []const ReferenceCase }, a, receipt, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        var unmapped: usize = 0;
        var citation_count: usize = 0;
        var supported_cases: usize = 0;
        var correct_answered: usize = 0;
        var correct_supported: usize = 0;
        for (parsed.value.cases, 0..) |case, case_index| {
            var batch = try prepare(a, tokenizer, case.documents, case.question);
            defer batch.deinit();
            try std.testing.expectEqual(case.copy_ids.len, batch.copy_ids.len);
            for (case.copy_ids, batch.copy_ids) |expected, actual| try std.testing.expectEqual(expected, @as(i64, actual));
            const sources = batch.sources orelse {
                unmapped += 1;
                continue;
            };
            try expectTiles(case.documents, sources);
            const source_ids = try narrowIds(a, case.copy_ids);
            defer a.free(source_ids);
            const snapped = try narrowIds(a, case.snapped_ids);
            defer a.free(snapped);
            const spans = try a.alloc(copy.Span, case.spans.len);
            defer a.free(spans);
            for (case.spans, spans) |raw, *span| span.* = .{ .begin = raw[0], .end = raw[1], .start = raw[2] };
            var completed = try copy.completeCopiedWords(a, tokenizer, snapped, spans, source_ids, copy.default_pad_id);
            defer completed.deinit();
            for (case.completed_ids, completed.ids) |expected, actual| try std.testing.expectEqual(expected, @as(i64, actual));
            const answer = std.mem.trim(u8, case.answer, &std.ascii.whitespace);
            const published: ?[]const u8 = if (std.mem.eql(u8, answer, "UNKNOWN")) null else answer;
            var citations = try copy.buildCitations(a, completed.spans, sources, case.documents, published);
            defer citations.deinit();
            try std.testing.expect(!citations.unavailable);
            var supported = false;
            for (citations.items) |citation| {
                try std.testing.expectEqualStrings(case.documents[citation.document_index][citation.start..citation.end], citation.text);
                supported = supported or citation.supports_answer;
            }
            if (case_index == 0) for (citations.items) |citation| {
                std.debug.print("  {s} answer={s}: document_index={d} start={d} end={d} text=\"{s}\" slots=[{d},{d}) supports_answer={} match={s}\n", .{ case.name, answer, citation.document_index, citation.start, citation.end, citation.text, citation.slots[0], citation.slots[1], citation.supports_answer, @tagName(citation.match) });
            };
            citation_count += citations.items.len;
            supported_cases += @intFromBool(supported);
            const correct = published != null and std.mem.eql(u8, answer, case.expected_answer);
            correct_answered += @intFromBool(correct);
            correct_supported += @intFromBool(correct and supported);
        }
        std.debug.print("citation replay {s}: cases={d} unmapped={d} citations={d} supported_cases={d} correct_answered={d} correct_with_support={d}\n", .{ std.fs.path.basename(reference_dir), parsed.value.cases.len, unmapped, citation_count, supported_cases, correct_answered, correct_supported });
        try std.testing.expectEqual(@as(usize, 0), unmapped);
    }
}
