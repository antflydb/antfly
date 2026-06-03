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

// GLiNER2 span classification head using abstract ComputeBackend ops.
//
// Implements SpanMarkerV0 (span representation) and CountLSTMv2 (label projection),
// then scores spans against labels via matmul.
//
// Input: encoder hidden states [batch, seq_len, H], words_mask, span_idx
// Output: logits [batch, num_words, max_width, num_labels]

const std = @import("std");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

pub const ForwardResult = struct {
    logits: []f32, // [batch * num_words * max_width * num_labels]
    num_words: usize,
    max_width: usize,
    num_labels: usize,
};

pub const ForwardCtResult = struct {
    logits: CT, // shape [total_spans, num_labels] -- empty CT when num_labels = 0
    num_words: usize,
    max_width: usize,
    num_labels: usize,
};

pub const LabelMarkerTokens = struct {
    classification: i64 = 0,
    entity: i64,
    relation: i64 = 0,

    pub fn fromEntityToken(entity_token_id: i64) LabelMarkerTokens {
        return .{ .entity = entity_token_id };
    }
};

pub const ForwardProfile = struct {
    materialize_hidden_ns: u64 = 0,
    extract_words_ns: u64 = 0,
    extract_labels_ns: u64 = 0,
    span_info_ns: u64 = 0,
    span_marker_ns: u64 = 0,
    span_word_to_ct_ns: u64 = 0,
    span_start_end_mlp_ns: u64 = 0,
    span_start_end_first_linear_ns: u64 = 0,
    span_start_end_relu_ns: u64 = 0,
    span_start_end_second_linear_ns: u64 = 0,
    span_gather_concat_relu_ns: u64 = 0,
    span_out_project_ns: u64 = 0,
    span_out_project_first_linear_ns: u64 = 0,
    span_out_project_relu_ns: u64 = 0,
    span_out_project_second_linear_ns: u64 = 0,
    label_projection_ns: u64 = 0,
    logits_ns: u64 = 0,

    pub fn add(self: *ForwardProfile, other: ForwardProfile) void {
        self.materialize_hidden_ns += other.materialize_hidden_ns;
        self.extract_words_ns += other.extract_words_ns;
        self.extract_labels_ns += other.extract_labels_ns;
        self.span_info_ns += other.span_info_ns;
        self.span_marker_ns += other.span_marker_ns;
        self.span_word_to_ct_ns += other.span_word_to_ct_ns;
        self.span_start_end_mlp_ns += other.span_start_end_mlp_ns;
        self.span_start_end_first_linear_ns += other.span_start_end_first_linear_ns;
        self.span_start_end_relu_ns += other.span_start_end_relu_ns;
        self.span_start_end_second_linear_ns += other.span_start_end_second_linear_ns;
        self.span_gather_concat_relu_ns += other.span_gather_concat_relu_ns;
        self.span_out_project_ns += other.span_out_project_ns;
        self.span_out_project_first_linear_ns += other.span_out_project_first_linear_ns;
        self.span_out_project_relu_ns += other.span_out_project_relu_ns;
        self.span_out_project_second_linear_ns += other.span_out_project_second_linear_ns;
        self.label_projection_ns += other.label_projection_ns;
        self.logits_ns += other.logits_ns;
    }
};

fn monotonicNowNs() u64 {
    // wasm-freestanding has no posix clock; profiling is best-effort there.
    if (@import("builtin").target.cpu.arch.isWasm()) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn profileStart(profile: ?*ForwardProfile) u64 {
    return if (profile != null) monotonicNowNs() else 0;
}

fn profileElapsed(start_ns: u64) u64 {
    if (start_ns == 0) return 0;
    return monotonicNowNs() - start_ns;
}

/// Run the full GLiNER2 forward pass.  Takes the encoder hidden state
/// as a CT (the output of `deberta_arch.forwardCt`) and returns logits
/// as a CT as well, so the encoder/head boundary stays on the backend
/// without a toFloat32 + fromFloat32Shape round-trip.  The internal
/// f32-only helpers (extractWord/Label embeddings) materialise the
/// hidden tensor once via `cb.toFloat32` -- on native that's a
/// memcpy + free, on Metal/MLX one device->host transfer instead of
/// the prior device->host->device->host pattern.
pub fn forwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    entity_token_id: i64,
) !ForwardCtResult {
    return forwardCtProfiledWithLabelMarkers(cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, hidden_size, LabelMarkerTokens.fromEntityToken(entity_token_id), null);
}

pub fn forwardCtWithLabelMarkers(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    label_markers: LabelMarkerTokens,
) !ForwardCtResult {
    return forwardCtProfiledWithLabelMarkers(cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, hidden_size, label_markers, null);
}

pub fn forwardCtProfiled(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    entity_token_id: i64,
    profile: ?*ForwardProfile,
) !ForwardCtResult {
    return forwardCtProfiledWithLabelMarkers(cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, hidden_size, LabelMarkerTokens.fromEntityToken(entity_token_id), profile);
}

pub fn forwardCtProfiledWithLabelMarkers(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    label_markers: LabelMarkerTokens,
    profile: ?*ForwardProfile,
) !ForwardCtResult {
    const H: usize = hidden_size;

    const num_words = countWords(words_mask);
    const label_positions = try collectLabelPositions(allocator, input_ids, seq_len, label_markers);
    defer allocator.free(label_positions);
    const num_labels = label_positions.len;

    const span_info = try getSpanInfo(allocator, span_idx, batch, num_words);
    const num_spans = span_info.num_spans;
    const max_width = span_info.max_width;
    defer allocator.free(span_info.start_indices);
    defer allocator.free(span_info.end_indices);

    const total_spans = batch * num_spans;
    if (num_labels == 0 or total_spans == 0) {
        const empty_shape = [_]i32{ 0, 0 };
        return .{
            .logits = try cb.fromFloat32Shape(&.{}, &empty_shape),
            .num_words = num_words,
            .max_width = max_width,
            .num_labels = num_labels,
        };
    }

    if (try cb.glinerWordEmbeddings(hidden, words_mask, batch, seq_len, H, num_words)) |word_ct| {
        defer cb.free(word_ct);
        if (try cb.takeRows(hidden, label_positions, num_labels, H)) |label_hidden_ct| {
            defer cb.free(label_hidden_ct);

            var timer = profileStart(profile);
            const span_ct = try spanMarkerForwardFromWordCt(cb, allocator, word_ct, span_info.start_indices, span_info.end_indices, batch, num_words, num_spans, H, profile);
            defer cb.free(span_ct);
            if (profile) |p| p.span_marker_ns += profileElapsed(timer);

            timer = profileStart(profile);
            const label_ct = try countLstmForwardFromCt(cb, allocator, label_hidden_ct, num_labels, H);
            defer cb.free(label_ct);
            if (profile) |p| p.label_projection_ns += profileElapsed(timer);

            timer = profileStart(profile);
            const logits_ct = try cb.linearNoBias(span_ct, label_ct, total_spans, H, num_labels);
            if (profile) |p| p.logits_ns += profileElapsed(timer);
            return .{
                .logits = logits_ct,
                .num_words = num_words,
                .max_width = max_width,
                .num_labels = num_labels,
            };
        }
    }

    // Materialise the encoder output once for the f32-only helpers.
    var timer = profileStart(profile);
    const hidden_f32 = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_f32);
    if (profile) |p| p.materialize_hidden_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const word_result = try extractWordEmbeddings(allocator, hidden_f32, words_mask, batch, seq_len, H);
    defer allocator.free(word_result.embeddings);
    if (profile) |p| p.extract_words_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const label_result = try extractLabelEmbeddings(allocator, hidden_f32, input_ids, batch, seq_len, H, label_markers);
    defer allocator.free(label_result.embeddings);
    if (profile) |p| p.extract_labels_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const span_ct = try spanMarkerForwardCt(cb, allocator, word_result.embeddings, span_info.start_indices, span_info.end_indices, batch, num_words, num_spans, H, profile);
    defer cb.free(span_ct);
    if (profile) |p| p.span_marker_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const label_ct = try countLstmForwardCt(cb, allocator, label_result.embeddings, num_labels, H);
    defer cb.free(label_ct);
    if (profile) |p| p.label_projection_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const logits_ct = try cb.linearNoBias(span_ct, label_ct, total_spans, H, num_labels);
    if (profile) |p| p.logits_ns += profileElapsed(timer);
    return .{
        .logits = logits_ct,
        .num_words = num_words,
        .max_width = max_width,
        .num_labels = num_labels,
    };
}

fn countWords(words_mask: []const i64) usize {
    var max_word_id: i64 = 0;
    for (words_mask) |v| {
        if (v > max_word_id) max_word_id = v;
    }
    return @intCast(max_word_id);
}

fn collectLabelPositions(allocator: std.mem.Allocator, input_ids: []const i64, seq_len: usize, label_markers: LabelMarkerTokens) ![]u32 {
    var label_positions = std.ArrayListUnmanaged(u32).empty;
    defer label_positions.deinit(allocator);
    for (0..@min(seq_len, input_ids.len)) |t| {
        if (isGlinerLabelMarkerToken(input_ids[t], label_markers)) {
            try label_positions.append(allocator, @intCast(t));
        }
    }
    return try label_positions.toOwnedSlice(allocator);
}

/// f32-in/f32-out variant of `forwardCt` for callers that don't have a
/// CT hidden state.  Performs the boundary fromFloat32Shape +
/// toFloat32 conversion on the caller's behalf.
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden_f32: []const f32,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    entity_token_id: i64,
) !ForwardResult {
    return forwardWithLabelMarkers(cb, allocator, hidden_f32, input_ids, words_mask, span_idx, batch, seq_len, hidden_size, LabelMarkerTokens.fromEntityToken(entity_token_id));
}

pub fn forwardWithLabelMarkers(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden_f32: []const f32,
    input_ids: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: usize,
    seq_len: usize,
    hidden_size: u32,
    label_markers: LabelMarkerTokens,
) !ForwardResult {
    const total = batch * seq_len;
    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    const hidden = try cb.fromFloat32Shape(hidden_f32, &shape);
    defer cb.free(hidden);
    const ct_result = try forwardCtWithLabelMarkers(cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, hidden_size, label_markers);
    defer cb.free(ct_result.logits);
    const logits_f32 = if (ct_result.num_labels == 0)
        try allocator.alloc(f32, 0)
    else
        try cb.toFloat32(ct_result.logits, allocator);
    return .{
        .logits = logits_f32,
        .num_words = ct_result.num_words,
        .max_width = ct_result.max_width,
        .num_labels = ct_result.num_labels,
    };
}

const WordEmbResult = struct {
    embeddings: []f32, // [batch * num_words, H]
    num_words: usize,
};

/// Extract word embeddings by averaging token embeddings per word using words_mask.
/// words_mask: [batch, seq_len] with values 0 (non-word) or word_id (1-indexed).
fn extractWordEmbeddings(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    words_mask: []const i64,
    batch: usize,
    seq_len: usize,
    H: usize,
) !WordEmbResult {
    // Find max word ID to determine num_words
    var max_word_id: i64 = 0;
    for (words_mask) |v| {
        if (v > max_word_id) max_word_id = v;
    }
    const num_words: usize = @intCast(max_word_id);

    const output = try allocator.alloc(f32, batch * num_words * H);
    @memset(output, 0.0);
    const counts = try allocator.alloc(f32, batch * num_words);
    defer allocator.free(counts);
    @memset(counts, 0.0);

    for (0..batch) |b| {
        for (0..seq_len) |t| {
            const word_id = words_mask[b * seq_len + t];
            if (word_id <= 0) continue;
            const w: usize = @intCast(word_id - 1); // 0-indexed
            const count_off = b * num_words + w;
            if (counts[count_off] != 0.0) continue;
            const src_off = (b * seq_len + t) * H;
            const dst_off = (b * num_words + w) * H;
            const dst = output[dst_off..][0..H];
            const src = hidden[src_off..][0..H];
            @memcpy(dst, src);
            counts[count_off] = 1.0;
        }
    }

    return .{ .embeddings = output, .num_words = num_words };
}

const LabelEmbResult = struct {
    embeddings: []f32, // [num_labels, H]
    num_labels: usize,
};

/// Extract label embeddings from positions where input_ids correspond to entity tokens.
/// GLiNER uses special [E] tokens (id >= 128000 typically) to mark labels in the input.
/// We use a simpler heuristic: labels are tokens after the last [SEP] or in positions
/// where words_mask == 0 and the token is not padding.
///
/// Actually, GLiNER prepends label tokens with special markers. The ONNX model
/// receives them as part of input_ids with words_mask == 0 for label positions.
/// We extract the hidden states at positions where words_mask == 0 and
/// attention_mask == 1 and the token appears to be a label boundary.
///
/// For simplicity: we take the first token of each label span (consecutive words_mask==0
/// tokens between word tokens) as the label embedding.
fn extractLabelEmbeddings(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    H: usize,
    label_markers: LabelMarkerTokens,
) !LabelEmbResult {
    _ = batch;
    // GLiNER2 uses [E]/[C]/[R] tokens to mark label positions in the input.
    // Extract hidden states at those positions.

    var label_positions = std.ArrayListUnmanaged(usize).empty;
    defer label_positions.deinit(allocator);

    // Only look at first batch item (labels are same across batch)
    for (0..seq_len) |t| {
        if (isGlinerLabelMarkerToken(input_ids[t], label_markers)) {
            try label_positions.append(allocator, t);
        }
    }

    const num_labels = label_positions.items.len;
    if (num_labels == 0) {
        // Fallback: no labels found, return empty
        return .{ .embeddings = try allocator.alloc(f32, 0), .num_labels = 0 };
    }

    const output = try allocator.alloc(f32, num_labels * H);
    for (label_positions.items, 0..) |pos, i| {
        @memcpy(output[i * H ..][0..H], hidden[pos * H ..][0..H]);
    }

    return .{ .embeddings = output, .num_labels = num_labels };
}

fn isGlinerLabelMarkerToken(token_id: i64, label_markers: LabelMarkerTokens) bool {
    return token_id == label_markers.entity or
        (label_markers.classification != 0 and token_id == label_markers.classification) or
        (label_markers.relation != 0 and token_id == label_markers.relation);
}

const SpanInfo = struct {
    num_spans: usize,
    max_width: usize,
    start_indices: []u32,
    end_indices: []u32,
};

/// Parse span_idx tensor to get start/end indices for each span.
/// span_idx: [batch, num_spans, 2] flattened to [batch * num_spans * 2]
fn getSpanInfo(
    allocator: std.mem.Allocator,
    span_idx: []const i64,
    batch: usize,
    num_words: usize,
) !SpanInfo {
    // span_idx is [batch, num_spans, 2] where each entry is (start_word, end_word)
    // Total elements = batch * num_spans * 2
    const total_elements = span_idx.len;
    const num_spans = total_elements / (batch * 2);
    const max_width = if (num_words > 0) num_spans / num_words else 8;

    const starts = try allocator.alloc(u32, batch * num_spans);
    const ends = try allocator.alloc(u32, batch * num_spans);

    for (0..batch) |b| {
        for (0..num_spans) |s| {
            const idx = (b * num_spans + s) * 2;
            starts[b * num_spans + s] = @intCast(@max(span_idx[idx], 0));
            ends[b * num_spans + s] = @intCast(@max(span_idx[idx + 1], 0));
        }
    }

    return .{
        .num_spans = num_spans,
        .max_width = max_width,
        .start_indices = starts,
        .end_indices = ends,
    };
}

/// SpanMarkerV0 forward pass.
/// Three 2-layer MLPs: project_start, project_end (H→4H→H), out_project (2H→4H→H).
/// Gathers start/end word embeddings by span_idx, concatenates, projects.
fn spanMarkerForwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    word_embs: []const f32, // [batch * num_words, H]
    start_indices: []const u32,
    end_indices: []const u32,
    batch: usize,
    num_words: usize,
    num_spans: usize,
    H: usize,
    profile: ?*ForwardProfile,
) !CT {
    const total_words = batch * num_words;

    // Project word embeddings through start and end MLPs
    const timer = profileStart(profile);
    const word_shape = [_]i32{ @intCast(total_words), @intCast(H) };
    const word_ct = try cb.fromFloat32Shape(word_embs[0 .. total_words * H], &word_shape);
    defer cb.free(word_ct);
    if (profile) |p| p.span_word_to_ct_ns += profileElapsed(timer);

    return spanMarkerForwardFromWordCt(cb, allocator, word_ct, start_indices, end_indices, batch, num_words, num_spans, H, profile);
}

fn spanMarkerForwardFromWordCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    word_ct: CT, // [batch * num_words, H]
    start_indices: []const u32,
    end_indices: []const u32,
    batch: usize,
    num_words: usize,
    num_spans: usize,
    H: usize,
    profile: ?*ForwardProfile,
) !CT {
    const total_words = batch * num_words;
    const total_spans = batch * num_spans;

    var timer = profileStart(profile);
    const span_projections = try mlp2SharedInputPair(
        cb,
        word_ct,
        total_words,
        H,
        4 * H,
        H,
        "span_rep.span_rep_layer.project_start",
        "span_rep.span_rep_layer.project_end",
        profile,
    );
    const start_proj = span_projections.first;
    const end_proj = span_projections.second;
    defer cb.free(start_proj);
    defer cb.free(end_proj);
    if (profile) |p| p.span_start_end_mlp_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const start_ids = try allocator.alloc(i64, total_spans);
    defer allocator.free(start_ids);
    const end_ids = try allocator.alloc(i64, total_spans);
    defer allocator.free(end_ids);

    for (0..batch) |b| {
        for (0..num_spans) |s| {
            const span_flat = b * num_spans + s;
            const si: usize = @min(start_indices[span_flat], @as(u32, @intCast(num_words - 1)));
            const ei: usize = @min(end_indices[span_flat], @as(u32, @intCast(num_words - 1)));
            start_ids[span_flat] = @intCast(b * num_words + si);
            end_ids[span_flat] = @intCast(b * num_words + ei);
        }
    }

    const gathered_start = try cb.embeddingLookup(start_proj, start_ids, total_spans, H);
    defer cb.free(gathered_start);
    const gathered_end = try cb.embeddingLookup(end_proj, end_ids, total_spans, H);
    defer cb.free(gathered_end);

    const concat_ct = try cb.concat(gathered_start, gathered_end, total_spans, H, H);
    defer cb.free(concat_ct);

    // ReLU on concatenated [start, end] before out_project (matches ONNX graph)
    const concat_relu = try cb.relu(concat_ct);
    defer cb.free(concat_relu);
    if (profile) |p| p.span_gather_concat_relu_ns += profileElapsed(timer);

    // out_project MLP: 2H → 4H → H
    timer = profileStart(profile);
    const out = try mlp2(cb, concat_relu, total_spans, H * 2, 4 * H, H, "span_rep.span_rep_layer.out_project", profile);
    if (profile) |p| p.span_out_project_ns += profileElapsed(timer);
    return out;
}

fn spanMarkerForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    word_embs: []const f32,
    start_indices: []const u32,
    end_indices: []const u32,
    batch: usize,
    num_words: usize,
    num_spans: usize,
    H: usize,
) ![]f32 {
    const out = try spanMarkerForwardCt(cb, allocator, word_embs, start_indices, end_indices, batch, num_words, num_spans, H, null);
    defer cb.free(out);
    return try cb.toFloat32(out, allocator);
}

/// 2-layer MLP: Linear(in→hidden, ReLU, Linear(hidden→out))
/// Weight naming: "{prefix}.0.weight/bias" for first layer, "{prefix}.3.weight/bias" for second
/// (indices 0 and 3 because PyTorch Sequential has: 0=Linear, 1=Dropout, 2=ReLU, 3=Linear)
fn mlp2(
    cb: *const ComputeBackend,
    input: CT,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    prefix: []const u8,
    profile: ?*ForwardProfile,
) !CT {
    var buf: [256]u8 = undefined;

    // First layer
    const w1_name = std.fmt.bufPrint(&buf, "{s}.0.weight", .{prefix}) catch return error.NameTooLong;
    const w1 = try cb.getWeight(w1_name);
    defer cb.free(w1);
    const b1_name = std.fmt.bufPrint(&buf, "{s}.0.bias", .{prefix}) catch return error.NameTooLong;
    const b1 = try cb.getWeight(b1_name);
    defer cb.free(b1);

    // Second layer
    var buf3: [256]u8 = undefined;
    const w2_name = std.fmt.bufPrint(&buf3, "{s}.3.weight", .{prefix}) catch return error.NameTooLong;
    const w2 = try cb.getWeight(w2_name);
    defer cb.free(w2);
    const b2_name = std.fmt.bufPrint(&buf3, "{s}.3.bias", .{prefix}) catch return error.NameTooLong;
    const b2 = try cb.getWeight(b2_name);
    defer cb.free(b2);

    var timer = profileStart(profile);
    if (try cb.denseMlp2(&.{
        .input = input,
        .first_weight = w1,
        .first_bias = b1,
        .second_weight = w2,
        .second_bias = b2,
        .rows = rows,
        .in_dim = in_dim,
        .hidden_dim = hidden_dim,
        .out_dim = out_dim,
        .activation = .relu,
    })) |fused| {
        if (profile) |p| p.span_out_project_first_linear_ns += profileElapsed(timer);
        return fused;
    }

    timer = profileStart(profile);
    const h1_relu = if (try cb.linearRelu(input, w1, b1, rows, in_dim, hidden_dim)) |fused|
        fused
    else blk: {
        const h1 = try cb.linear(input, w1, b1, rows, in_dim, hidden_dim);
        defer cb.free(h1);
        break :blk try cb.relu(h1);
    };
    defer cb.free(h1_relu);
    if (profile) |p| p.span_out_project_first_linear_ns += profileElapsed(timer);

    if (profile) |p| p.span_out_project_relu_ns += 0;

    timer = profileStart(profile);
    const out = try cb.linear(h1_relu, w2, b2, rows, hidden_dim, out_dim);
    if (profile) |p| p.span_out_project_second_linear_ns += profileElapsed(timer);
    return out;
}

fn mlp2SharedInputPair(
    cb: *const ComputeBackend,
    input: CT,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    out_dim: usize,
    prefix_a: []const u8,
    prefix_b: []const u8,
    profile: ?*ForwardProfile,
) !ops.LinearPairResult {
    var buf_a: [256]u8 = undefined;
    var buf_b: [256]u8 = undefined;

    const w1_a_name = std.fmt.bufPrint(&buf_a, "{s}.0.weight", .{prefix_a}) catch return error.NameTooLong;
    const w1_a = try cb.getWeight(w1_a_name);
    defer cb.free(w1_a);
    const b1_a_name = std.fmt.bufPrint(&buf_a, "{s}.0.bias", .{prefix_a}) catch return error.NameTooLong;
    const b1_a = try cb.getWeight(b1_a_name);
    defer cb.free(b1_a);

    const w1_b_name = std.fmt.bufPrint(&buf_b, "{s}.0.weight", .{prefix_b}) catch return error.NameTooLong;
    const w1_b = try cb.getWeight(w1_b_name);
    defer cb.free(w1_b);
    const b1_b_name = std.fmt.bufPrint(&buf_b, "{s}.0.bias", .{prefix_b}) catch return error.NameTooLong;
    const b1_b = try cb.getWeight(b1_b_name);
    defer cb.free(b1_b);

    var timer = profileStart(profile);
    const first_layers = try cb.linearPair(input, w1_a, b1_a, w1_b, b1_b, rows, in_dim, hidden_dim);
    defer cb.free(first_layers.first);
    defer cb.free(first_layers.second);
    if (profile) |p| p.span_start_end_first_linear_ns += profileElapsed(timer);

    timer = profileStart(profile);
    const first_relu = try cb.relu(first_layers.first);
    defer cb.free(first_relu);
    const second_relu = try cb.relu(first_layers.second);
    defer cb.free(second_relu);
    if (profile) |p| p.span_start_end_relu_ns += profileElapsed(timer);

    const w2_a_name = std.fmt.bufPrint(&buf_a, "{s}.3.weight", .{prefix_a}) catch return error.NameTooLong;
    const w2_a = try cb.getWeight(w2_a_name);
    defer cb.free(w2_a);
    const b2_a_name = std.fmt.bufPrint(&buf_a, "{s}.3.bias", .{prefix_a}) catch return error.NameTooLong;
    const b2_a = try cb.getWeight(b2_a_name);
    defer cb.free(b2_a);

    const w2_b_name = std.fmt.bufPrint(&buf_b, "{s}.3.weight", .{prefix_b}) catch return error.NameTooLong;
    const w2_b = try cb.getWeight(w2_b_name);
    defer cb.free(w2_b);
    const b2_b_name = std.fmt.bufPrint(&buf_b, "{s}.3.bias", .{prefix_b}) catch return error.NameTooLong;
    const b2_b = try cb.getWeight(b2_b_name);
    defer cb.free(b2_b);

    timer = profileStart(profile);
    const first = try cb.linear(first_relu, w2_a, b2_a, rows, hidden_dim, out_dim);
    errdefer cb.free(first);
    const second = try cb.linear(second_relu, w2_b, b2_b, rows, hidden_dim, out_dim);
    if (profile) |p| p.span_start_end_second_linear_ns += profileElapsed(timer);
    return .{ .first = first, .second = second };
}

/// CountLSTMv2 forward pass (count=1 at inference time).
/// GRU single step + DownscaledTransformer.
fn countLstmForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label_embs: []const f32, // [num_labels, H]
    num_labels: usize,
    H: usize,
) ![]f32 {
    const result = try countLstmForwardCt(cb, allocator, label_embs, num_labels, H);
    defer cb.free(result);
    return try cb.toFloat32(result, allocator);
}

fn countLstmForwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label_embs: []const f32, // [num_labels, H]
    num_labels: usize,
    H: usize,
) !CT {
    if (num_labels == 0) {
        const empty_shape = [_]i32{ 0, @intCast(H) };
        return cb.fromFloat32Shape(&.{}, &empty_shape);
    }

    // 1. Get position embedding for count=0 (first row)
    var buf: [256]u8 = undefined;
    const pos_name = std.fmt.bufPrint(&buf, "count_embed.pos_embedding.weight", .{}) catch return error.NameTooLong;
    const pos_w = try cb.getWeight(pos_name);
    defer cb.free(pos_w);
    const pos_data = try cb.toFloat32(pos_w, allocator);
    defer allocator.free(pos_data);

    // pos = pos_data[0..H] broadcast to [num_labels, H]
    const pos_broadcast = try allocator.alloc(f32, num_labels * H);
    defer allocator.free(pos_broadcast);
    for (0..num_labels) |i| {
        @memcpy(pos_broadcast[i * H ..][0..H], pos_data[0..H]);
    }

    // 2. GRU single step: h_0 = label_embs, x = pos_broadcast
    const gru_out = try gruStep(cb, allocator, label_embs, pos_broadcast, num_labels, H);
    defer allocator.free(gru_out);

    // 3. Skip connection: combined = gru_out + label_embs
    const combined = try allocator.alloc(f32, num_labels * H);
    for (0..num_labels * H) |i| {
        combined[i] = gru_out[i] + label_embs[i];
    }

    // 4. DownscaledTransformer
    const result = try downscaledTransformer(cb, allocator, combined, num_labels, H);
    allocator.free(combined);
    return result;
}

fn countLstmForwardFromCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label_ct: CT,
    num_labels: usize,
    H: usize,
) !CT {
    if (num_labels == 0) {
        const empty_shape = [_]i32{ 0, @intCast(H) };
        return cb.fromFloat32Shape(&.{}, &empty_shape);
    }

    if (try cb.glinerLabelGruCombined(label_ct, num_labels, H)) |combined_ct| {
        defer cb.free(combined_ct);
        return downscaledTransformerCt(cb, allocator, combined_ct, num_labels, H);
    }

    const label_embs = try cb.toFloat32(label_ct, allocator);
    defer allocator.free(label_embs);
    return countLstmForwardCt(cb, allocator, label_embs, num_labels, H);
}

/// GRU single step.
/// h_0: [N, H], x: [N, H]
/// Returns h_1: [N, H]
fn gruStep(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    h_0: []const f32,
    x: []const f32,
    N: usize,
    H: usize,
) ![]f32 {
    // Load GRU weights
    const w_ih = try cb.getWeight("count_embed.gru.weight_ih_l0"); // [3H, H]
    defer cb.free(w_ih);
    const w_hh = try cb.getWeight("count_embed.gru.weight_hh_l0"); // [3H, H]
    defer cb.free(w_hh);
    const b_ih = try cb.getWeight("count_embed.gru.bias_ih_l0"); // [3H]
    defer cb.free(b_ih);
    const b_hh = try cb.getWeight("count_embed.gru.bias_hh_l0"); // [3H]
    defer cb.free(b_hh);

    // gi = x @ W_ih^T + b_ih : [N, 3H]
    const x_shape = [_]i32{ @intCast(N), @intCast(H) };
    const x_ct = try cb.fromFloat32Shape(x[0 .. N * H], &x_shape);
    defer cb.free(x_ct);
    const gi = try cb.linear(x_ct, w_ih, b_ih, N, H, 3 * H);
    defer cb.free(gi);
    const gi_data = try cb.toFloat32(gi, allocator);
    defer allocator.free(gi_data);

    // gh = h_0 @ W_hh^T + b_hh : [N, 3H]
    const h_shape = [_]i32{ @intCast(N), @intCast(H) };
    const h_ct = try cb.fromFloat32Shape(h_0[0 .. N * H], &h_shape);
    defer cb.free(h_ct);
    const gh = try cb.linear(h_ct, w_hh, b_hh, N, H, 3 * H);
    defer cb.free(gh);
    const gh_data = try cb.toFloat32(gh, allocator);
    defer allocator.free(gh_data);

    // Split gates and compute h_1
    const h_1 = try allocator.alloc(f32, N * H);
    for (0..N) |i| {
        const gi_off = i * 3 * H;
        const gh_off = i * 3 * H;

        for (0..H) |d| {
            // r = sigmoid(gi_r + gh_r)
            const r = sigmoid_f32(gi_data[gi_off + d] + gh_data[gh_off + d]);
            // z = sigmoid(gi_z + gh_z)
            const z = sigmoid_f32(gi_data[gi_off + H + d] + gh_data[gh_off + H + d]);
            // n = tanh(gi_n + r * gh_n)
            const n = std.math.tanh(gi_data[gi_off + 2 * H + d] + r * gh_data[gh_off + 2 * H + d]);
            // h_1 = (1 - z) * n + z * h_0
            h_1[i * H + d] = (1.0 - z) * n + z * h_0[i * H + d];
        }
    }

    return h_1;
}

fn sigmoid_f32(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}

/// DownscaledTransformer: projects from H→128, runs 2-layer transformer, projects back.
/// out_projector has skip connection: concat([transformer_out, combined]) → MLP(896→768→768→768)
fn downscaledTransformer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    combined: []const f32, // [N, H=768]
    N: usize,
    H: usize,
) !CT {
    const combined_shape = [_]i32{ @intCast(N), @intCast(H) };
    const combined_ct = try cb.fromFloat32Shape(combined[0 .. N * H], &combined_shape);
    defer cb.free(combined_ct);
    return downscaledTransformerCt(cb, allocator, combined_ct, N, H);
}

fn downscaledTransformerCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    combined_ct: CT, // [N, H=768]
    N: usize,
    H: usize,
) !CT {
    const D: usize = 128; // downscaled dim
    const D_FFN: usize = 256;

    // in_projector: [N, H] → [N, D]
    const in_w = try cb.getWeight("count_embed.transformer.in_projector.weight");
    defer cb.free(in_w);
    const in_b = try cb.getWeight("count_embed.transformer.in_projector.bias");
    defer cb.free(in_b);
    const projected = try cb.linear(combined_ct, in_w, in_b, N, H, D);

    // 2-layer transformer encoder
    var hidden = projected;
    for (0..2) |layer| {
        const new_hidden = try miniTransformerLayer(cb, allocator, hidden, N, D, D_FFN, layer);
        cb.free(hidden);
        hidden = new_hidden;
    }

    // out_projector with skip: concat([transformer_out, combined]) → MLP
    // transformer_out: [N, 128], combined: [N, 768] → concatenated: [N, 896]
    const concat_dim = D + H; // 896
    const cat_ct = try cb.concat(hidden, combined_ct, N, D, H);
    defer cb.free(cat_ct);
    cb.free(hidden);

    // 3-layer MLP: 896→768, ReLU, 768→768, ReLU, 768→768
    const result = try outProjectorMlp(cb, allocator, cat_ct, N, concat_dim, H);
    return result;
}

/// Mini transformer encoder layer (128-dim, 4 heads, 256 FFN, post-norm style).
fn miniTransformerLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    N: usize,
    D: usize,
    D_FFN: usize,
    layer: usize,
) !CT {
    return miniTransformerLayerCpu(cb, allocator, hidden, N, D, D_FFN, layer);
}

/// CPU-based mini transformer layer (N is small, so this is fast).
fn miniTransformerLayerCpu(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    N: usize,
    D: usize,
    D_FFN: usize,
    layer: usize,
) !CT {
    // We'll use linear/layerNorm/relu ops through the backend but handle
    // attention score computation via toFloat32 since we need to split Q,K,V.
    var buf: [256]u8 = undefined;

    // Self-attention
    const in_proj_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.self_attn.in_proj_weight", .{layer}) catch return error.NameTooLong;
    const in_proj_w = try cb.getWeight(in_proj_w_name);
    defer cb.free(in_proj_w);
    const in_proj_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.self_attn.in_proj_bias", .{layer}) catch return error.NameTooLong;
    const in_proj_b = try cb.getWeight(in_proj_b_name);
    defer cb.free(in_proj_b);

    const qkv = try cb.linear(hidden, in_proj_w, in_proj_b, N, D, 3 * D);
    defer cb.free(qkv);

    // Use scaledDotProductAttention by splitting QKV
    // Q = qkv[:, 0:D], K = qkv[:, D:2D], V = qkv[:, 2D:3D]
    // Use the backend's `splitLastDim3` op rather than round-tripping the
    // QKV tensor through f32.  On native compute it operates directly on
    // the underlying f32 view (zero extra alloc beyond the three split
    // buffers); on Metal/MLX the split stays device-resident.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const split = try cb.splitLastDim3(alloc, qkv, N, D);
    const q_ct = split.first;
    defer cb.free(q_ct);
    const k_ct = split.second;
    defer cb.free(k_ct);
    const v_ct = split.third;
    defer cb.free(v_ct);

    const num_heads: usize = 4;
    const head_dim = D / num_heads; // 32
    const attn_out = if (try cb.scaledDotProductAttentionFull(q_ct, k_ct, v_ct, null, 1, N, num_heads, head_dim)) |full_attn|
        full_attn
    else blk: {
        // All labels attend to each other. Backends without a no-mask fast
        // path still use the existing masked attention implementation.
        const mask = try allocator.alloc(i64, N);
        defer allocator.free(mask);
        @memset(mask, 1);
        break :blk try cb.scaledDotProductAttention(q_ct, k_ct, v_ct, mask, null, 1, N, num_heads, head_dim);
    };
    defer cb.free(attn_out);

    // out_proj
    const out_proj_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.self_attn.out_proj.weight", .{layer}) catch return error.NameTooLong;
    const out_proj_w = try cb.getWeight(out_proj_w_name);
    defer cb.free(out_proj_w);
    const out_proj_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.self_attn.out_proj.bias", .{layer}) catch return error.NameTooLong;
    const out_proj_b = try cb.getWeight(out_proj_b_name);
    defer cb.free(out_proj_b);
    const attn_proj = try cb.linear(attn_out, out_proj_w, out_proj_b, N, D, D);
    defer cb.free(attn_proj);

    // Residual + norm1 (post-norm)
    const res1 = try cb.add(attn_proj, hidden);
    defer cb.free(res1);

    const norm1_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.norm1.weight", .{layer}) catch return error.NameTooLong;
    const norm1_w = try cb.getWeight(norm1_w_name);
    defer cb.free(norm1_w);
    const norm1_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.norm1.bias", .{layer}) catch return error.NameTooLong;
    const norm1_b = try cb.getWeight(norm1_b_name);
    defer cb.free(norm1_b);
    const normed1 = try cb.layerNorm(res1, norm1_w, norm1_b, D, 1e-5);

    // FFN: linear1 → relu → linear2
    const ffn1_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.linear1.weight", .{layer}) catch return error.NameTooLong;
    const ffn1_w = try cb.getWeight(ffn1_w_name);
    defer cb.free(ffn1_w);
    const ffn1_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.linear1.bias", .{layer}) catch return error.NameTooLong;
    const ffn1_b = try cb.getWeight(ffn1_b_name);
    defer cb.free(ffn1_b);
    const ffn1 = try cb.linear(normed1, ffn1_w, ffn1_b, N, D, D_FFN);
    defer cb.free(ffn1);

    const ffn1_relu = try cb.relu(ffn1);
    defer cb.free(ffn1_relu);

    const ffn2_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.linear2.weight", .{layer}) catch return error.NameTooLong;
    const ffn2_w = try cb.getWeight(ffn2_w_name);
    defer cb.free(ffn2_w);
    const ffn2_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.linear2.bias", .{layer}) catch return error.NameTooLong;
    const ffn2_b = try cb.getWeight(ffn2_b_name);
    defer cb.free(ffn2_b);
    const ffn2 = try cb.linear(ffn1_relu, ffn2_w, ffn2_b, N, D_FFN, D);
    defer cb.free(ffn2);

    // Residual + norm2 (post-norm)
    const res2 = try cb.add(ffn2, normed1);
    cb.free(normed1);
    defer cb.free(res2);

    const norm2_w_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.norm2.weight", .{layer}) catch return error.NameTooLong;
    const norm2_w = try cb.getWeight(norm2_w_name);
    defer cb.free(norm2_w);
    const norm2_b_name = std.fmt.bufPrint(&buf, "count_embed.transformer.transformer.layers.{d}.norm2.bias", .{layer}) catch return error.NameTooLong;
    const norm2_b = try cb.getWeight(norm2_b_name);
    defer cb.free(norm2_b);
    return try cb.layerNorm(res2, norm2_w, norm2_b, D, 1e-5);
}

/// out_projector 3-layer MLP: 896→768 (ReLU), 768→768 (ReLU), 768→768
fn outProjectorMlp(
    cb: *const ComputeBackend,
    _: std.mem.Allocator,
    input: CT,
    N: usize,
    in_dim: usize,
    H: usize,
) !CT {
    // Layer 0: 896 → 768
    const w0 = try cb.getWeight("count_embed.transformer.out_projector.0.weight");
    defer cb.free(w0);
    const b0 = try cb.getWeight("count_embed.transformer.out_projector.0.bias");
    defer cb.free(b0);
    const h0_relu = if (try cb.linearRelu(input, w0, b0, N, in_dim, H)) |fused|
        fused
    else blk: {
        const h0 = try cb.linear(input, w0, b0, N, in_dim, H);
        defer cb.free(h0);
        break :blk try cb.relu(h0);
    };
    defer cb.free(h0_relu);

    // Layer 2: 768 → 768
    const w2 = try cb.getWeight("count_embed.transformer.out_projector.2.weight");
    defer cb.free(w2);
    const b2 = try cb.getWeight("count_embed.transformer.out_projector.2.bias");
    defer cb.free(b2);
    const h2_relu = if (try cb.linearRelu(h0_relu, w2, b2, N, H, H)) |fused|
        fused
    else blk: {
        const h2 = try cb.linear(h0_relu, w2, b2, N, H, H);
        defer cb.free(h2);
        break :blk try cb.relu(h2);
    };
    defer cb.free(h2_relu);

    // Layer 4: 768 → 768
    const w4 = try cb.getWeight("count_embed.transformer.out_projector.4.weight");
    defer cb.free(w4);
    const b4 = try cb.getWeight("count_embed.transformer.out_projector.4.bias");
    defer cb.free(b4);
    return cb.linear(h2_relu, w4, b4, N, H, H);
}

test "extractWordEmbeddings uses first sub-token embedding per word" {
    const allocator = std.testing.allocator;
    const hidden = [_]f32{
        1, 2,
        3, 4,
        5, 6,
        7, 8,
    };
    const words_mask = [_]i64{ 1, 1, 2, 0 };

    const result = try extractWordEmbeddings(allocator, &hidden, &words_mask, 1, 4, 2);
    defer allocator.free(result.embeddings);

    try std.testing.expectEqual(@as(usize, 2), result.num_words);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6 }, result.embeddings);
}

test "extractLabelEmbeddings uses entity marker positions" {
    const allocator = std.testing.allocator;
    const hidden = [_]f32{
        1, 10,
        2, 20,
        3, 30,
        4, 40,
    };
    const input_ids = [_]i64{ 7, 99, 5, 99 };

    const result = try extractLabelEmbeddings(allocator, &hidden, &input_ids, 1, 4, 2, .{ .entity = 99 });
    defer allocator.free(result.embeddings);

    try std.testing.expectEqual(@as(usize, 2), result.num_labels);
    try std.testing.expectEqualSlices(f32, &.{ 2, 20, 4, 40 }, result.embeddings);
}

test "extractLabelEmbeddings accepts GLiNER classification and relation markers" {
    const allocator = std.testing.allocator;
    const hidden = [_]f32{
        1, 10,
        2, 20,
        3, 30,
        4, 40,
        5, 50,
    };
    const input_ids = [_]i64{ 52, 7, 51, 8, 53 };

    const result = try extractLabelEmbeddings(allocator, &hidden, &input_ids, 1, 5, 2, .{
        .classification = 52,
        .entity = 51,
        .relation = 53,
    });
    defer allocator.free(result.embeddings);

    try std.testing.expectEqual(@as(usize, 3), result.num_labels);
    try std.testing.expectEqualSlices(f32, &.{ 1, 10, 3, 30, 5, 50 }, result.embeddings);
}

test "collectLabelPositions mirrors first-batch label extraction" {
    const allocator = std.testing.allocator;
    const input_ids = [_]i64{
        7,  99, 5,  99,
        99, 8,  99, 9,
    };

    const positions = try collectLabelPositions(allocator, &input_ids, 4, .{ .entity = 99 });
    defer allocator.free(positions);

    try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, positions);
}

test "getSpanInfo derives width from span_idx layout" {
    const allocator = std.testing.allocator;
    const span_idx = [_]i64{
        0, 0,
        0, 1,
        1, 1,
        1, 2,
        2, 2,
        2, 0,
    };

    const info = try getSpanInfo(allocator, &span_idx, 1, 3);
    defer allocator.free(info.start_indices);
    defer allocator.free(info.end_indices);

    try std.testing.expectEqual(@as(usize, 6), info.num_spans);
    try std.testing.expectEqual(@as(usize, 2), info.max_width);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 1, 1, 2, 2 }, info.start_indices);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 1, 2, 2, 0 }, info.end_indices);
}
