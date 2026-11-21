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

//! Managed encoder-to-memory bridge. Authentication/source closure belongs to
//! its caller; producer metadata is an identity check, never an access grant.
const std = @import("std");
const manager = @import("../server/model_manager.zig");
const factory = @import("../architectures/session_factory.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const catalog = @import("../registry/memorilla_catalog.zig");
const token_features = @import("memorilla_token_features.zig");
const span_mod = @import("memorilla_span.zig");
const source_map = @import("memorilla_copy.zig");
const encoder_lora = @import("../models/memorilla_encoder_lora.zig");
const session_mod = @import("../backends/session.zig");
const Tensor = @import("../backends/backends.zig").Tensor;
const c_file = @import("../util/c_file.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const memory = @import("../runtime/root.zig").tier.memory;
const A = std.mem.Allocator;
pub const width = qualified.width;
/// Request text ceiling shared by every document and the question (bytes).
pub const max_text_bytes: usize = 8192;

pub const v1_profile: qualified.Profile = .qwen3_encoder_f32_v1;
pub const research_profile: qualified.Profile = .qwen3_encoder_lora_research_f32_v1;
/// Research candidates ship their lock next to the weights (<= 1 MiB).
pub const research_lock_file = "memorilla_lock.json";
pub const research_lock_max_bytes: usize = 1024 * 1024;

pub const Producer = struct {
    /// The actual qualification tag that produced these features.
    profile: qualified.Profile,
    publication: [32]u8,
    identity: [64]u8,
    /// Lowercase sha256 hex of the profile's source lock: v1 pins the embedded
    /// `memorilla_v1_base_models.json`; the research tag pins the candidate
    /// directory's `memorilla_lock.json` bytes.
    source_lock_sha256: [64]u8,
    /// Lowercase sha256 hex of the canonical manifest for `profile`.
    recipe_sha256: [64]u8,
};

/// sha256 hex of the embedded v1 base-model lock.
pub fn v1SourceLockSha256() [64]u8 {
    var source: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(qualified.lock_bytes, &source, .{});
    return std.fmt.bytesToHex(source, .lower);
}

/// sha256 hex of `<model_dir>/memorilla_lock.json`. The loaded model exposes no
/// parsed lock, so the research producer pin is computed from its directory.
pub fn researchSourceLockSha256(a: A, model_dir: []const u8) ![64]u8 {
    const path = try std.fs.path.join(a, &.{ model_dir, research_lock_file });
    defer a.free(path);
    const bytes = c_file.readFileMax(a, path, research_lock_max_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.MemorillaLockNotFound,
        else => return err,
    };
    defer a.free(bytes);
    if (bytes.len == 0) return error.InvalidMemorillaLock;
    var source: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &source, .{});
    return std.fmt.bytesToHex(source, .lower);
}

/// Canonical manifest digest for an encoder profile.
pub fn recipeSha256(profile: qualified.Profile) ![64]u8 {
    const source: []const u8 = switch (profile) {
        v1_profile => catalog.encoder_manifest_sha256,
        research_profile => catalog.research_encoder_manifest_sha256,
        else => return error.IncompatibleMemorillaProducer,
    };
    if (source.len != 64) return error.IncompatibleMemorillaProducer;
    var out: [64]u8 = undefined;
    @memcpy(&out, source);
    return out;
}

fn isLowerHex(text: []const u8) bool {
    for (text) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    return true;
}

/// Stored-vector identity checks do not replace caller authorization or source
/// version closure. The caller supplies the exact expected publication pin and
/// the profile it is serving. The v1 lock is embedded and verified here; the
/// research lock lives in the candidate directory, so this only requires a
/// well-formed digest and `requireProducerSourceLock` pins the exact value.
pub fn requireProducer(producer: Producer, expected_publication: [32]u8, expected: qualified.Profile) !void {
    if (producer.profile != expected or
        !std.mem.eql(u8, &producer.publication, &expected_publication) or
        !std.mem.eql(u8, &producer.identity, &qualified.executionIdentity(expected)) or
        !std.mem.eql(u8, &producer.recipe_sha256, &(try recipeSha256(expected))))
        return error.IncompatibleMemorillaProducer;
    if (expected == v1_profile) {
        if (!std.mem.eql(u8, &producer.source_lock_sha256, &v1SourceLockSha256())) return error.IncompatibleMemorillaProducer;
    } else if (!isLowerHex(&producer.source_lock_sha256)) return error.IncompatibleMemorillaProducer;
}

/// Pin a research producer to one candidate directory's lock digest
/// (`researchSourceLockSha256`).
pub fn requireProducerSourceLock(producer: Producer, expected_source_lock_sha256: [64]u8) !void {
    if (!std.mem.eql(u8, &producer.source_lock_sha256, &expected_source_lock_sha256)) return error.IncompatibleMemorillaProducer;
}

pub const Features = struct {
    allocator: A,
    document_count: usize,
    question_tokens: usize,
    documents_fp16: []f16,
    question_fp16: []f16,
    /// Exact BF16-valued features widened to F32 for the memory compute API.
    documents: []f32,
    question: []f32,
    question_text: []u8,
    document_sha256: [][32]u8,
    producer: Producer,
    lease: ?memory.AdmissionLease,

    pub fn deinit(self: *Features) void {
        self.allocator.free(self.documents_fp16);
        self.allocator.free(self.question_fp16);
        self.allocator.free(self.documents);
        self.allocator.free(self.question);
        self.allocator.free(self.question_text);
        self.allocator.free(self.document_sha256);
        if (self.lease) |*lease| lease.release();
        self.* = undefined;
    }
};

pub const canonicalQuestion = qualified.canonicalQuestion;

pub const StoredValue = struct { fp16: f16, memory: f32 };
pub fn storedValue(value: f32) !StoredValue {
    if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
    const half: f16 = @floatCast(value);
    if (!std.math.isFinite(half)) return error.NonFiniteMemorillaFeature;
    const widened: f32 = @floatCast(half);
    const bits: u32 = @bitCast(widened);
    const rounded = bits +% (0x7fff + ((bits >> 16) & 1));
    const bf16: u32 = (rounded >> 16) << 16;
    return .{ .fp16 = half, .memory = @bitCast(bf16) };
}

fn copyFeature(source: []const f32, stored: []f16, destination: []f32) !void {
    if (source.len != width or stored.len != width or destination.len != width) return error.InvalidMemorillaFeatureWidth;
    var norm: f64 = 0;
    for (source) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
        norm += @as(f64, value) * value;
    }
    if (@abs(@sqrt(norm) - 1) > 1e-4) return error.UnnormalizedMemorillaFeature;
    for (source, stored, destination) |value, *half, *memory_value| {
        const converted = try storedValue(value);
        half.* = converted.fp16;
        memory_value.* = converted.memory;
    }
}
pub const convertFeature = copyFeature;

const TokenBudget = struct {
    total: usize = 0,
    question_text: []const u8,
    question_tokens: usize = 0,
    fn observe(raw: *anyopaque, text: []const u8, ids: []const i32, mask: []const i32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (ids.len != mask.len) return error.InvalidMemorillaTokens;
        var active: usize = 0;
        var last: i32 = -1;
        for (ids, mask) |id, used| {
            if (used != 0 and used != 1) return error.InvalidMemorillaTokens;
            if (used == 1) {
                active += 1;
                last = id;
            }
        }
        if (active == 0 or active > qualified.max_text_tokens or last != 151643) return error.MemorillaTextLimitExceeded;
        self.total = try std.math.add(usize, self.total, active);
        if (self.total > 16384) return error.MemorillaTextLimitExceeded;
        if (std.mem.eql(u8, text, self.question_text)) {
            if (self.question_tokens != 0 and self.question_tokens != active) return error.InvalidMemorillaTokens;
            self.question_tokens = active;
        }
    }
};

/// The caller retains its handle and authorization closure. This call adds an
/// independent lifetime pin and keeps the complete output admitted until deinit.
pub fn encode(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control) !Features {
    return encodeImpl(a, handle, documents, question, limits, control, false);
}

/// Query execution over supplied stored features encodes only the question.
/// Its empty documents arrays are intentional and never encode an empty corpus.
pub fn encodeQuestion(a: A, handle: *const manager.ModelHandle, question: []const u8, limits: memory.Limits, control: Control) !Features {
    return encodeImpl(a, handle, &.{}, question, limits, control, true);
}

fn encodeImpl(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control, question_only: bool) !Features {
    try control.check();
    if (documents.len == 0 and !question_only) return error.EmptyMemorillaCollection;
    if (documents.len > qualified.max_documents or question.len > 8192) return error.MemorillaTextLimitExceeded;
    const canonical = try canonicalQuestion(question);
    if (canonical.len == 0) return error.EmptyMemorillaQuestion;
    for (documents) |text| {
        if (text.len > 8192) return error.MemorillaTextLimitExceeded;
        _ = try std.unicode.Utf8View.init(text);
    }
    const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
    var pin = handle.duplicate();
    defer pin.release();
    const model = pin.get();
    try qualified.requireRecipe(v1_profile, model.manifest);
    try factory.requireMemorillaProfile(model.session, v1_profile);
    const publication = model.manifest.managed_publication orelse return error.MemorillaManagedProducerRequired;
    const elements = try std.math.mul(usize, documents.len + 1, width);
    const owned_bytes = try std.math.add(usize, try std.math.mul(usize, elements, 10), canonical.len + documents.len * 32 + (documents.len + 1) * @sizeOf([]const u8));
    var lease = try pin.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = owned_bytes });
    var owns_lease = true;
    defer if (owns_lease) lease.release();
    try control.check();
    var assets = try model.acquireEmbeddingAssetLeaseWithControl(false, control);
    defer assets.release();
    var pipeline = blk: {
        try model.lockEmbeddingAssetsWithControl(control);
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(true, false, control);
        break :blk model.embeddingPipelineLocked(backing);
    };
    try model.verifyEmbeddingIdentityWithControl(control);
    const identity = model.embedding_identity orelse return error.InvalidEmbeddingIdentity;
    pipeline.execution_control = control;
    pipeline.execution_lane = &model.embedding_execution_lane;
    const lane = try model.embedding_execution_lane.acquire(control);
    defer lane.release();
    // This owner explicitly admits all retained full-width F32 output rows.
    // Keep bounded physical batches without the Gemma-only retained-row floor.
    pipeline.execution_lane_held = true;
    pipeline.config.max_length = qualified.max_text_tokens;
    pipeline.config.batch_size = 4;
    pipeline.config.strict_max_length = true;
    pipeline.config.reject_truncation = true;
    // Research-only (`researchFastPrefill`): pad each batch to its longest
    // text instead of the 256-token limit. Causal attention and last-token
    // pooling leave active rows unaffected; GEMM row counts change.
    if (qualified.researchFastPrefill()) pipeline.config.trim_padding_to_batch_max = true;
    var budget = TokenBudget{ .question_text = canonical };
    pipeline.tokenization_observer = .{ .ptr = &budget, .observe = TokenBudget.observe };
    const texts = try backing.alloc([]const u8, documents.len + 1);
    defer backing.free(texts);
    @memcpy(texts[0..documents.len], documents);
    texts[documents.len] = canonical;
    const vectors = try pipeline.embed(texts);
    defer {
        for (vectors) |vector| backing.free(vector);
        backing.free(vectors);
    }
    try control.check();
    try model.verifyEmbeddingIdentityWithControl(control);
    try factory.requireMemorillaProfile(model.session, v1_profile);
    if (vectors.len != texts.len) return error.InvalidMemorillaFeatureWidth;
    if (budget.question_tokens == 0) return error.InvalidMemorillaTokens;
    const documents_fp16 = try backing.alloc(f16, documents.len * width);
    errdefer backing.free(documents_fp16);
    const question_fp16 = try backing.alloc(f16, width);
    errdefer backing.free(question_fp16);
    const document_features = try backing.alloc(f32, documents.len * width);
    errdefer backing.free(document_features);
    const question_features = try backing.alloc(f32, width);
    errdefer backing.free(question_features);
    const question_text = try backing.dupe(u8, canonical);
    errdefer backing.free(question_text);
    const document_sha256 = try backing.alloc([32]u8, documents.len);
    errdefer backing.free(document_sha256);
    for (documents, vectors[0..documents.len], 0..) |text, vector, i| {
        try control.check();
        try copyFeature(vector, documents_fp16[i * width ..][0..width], document_features[i * width ..][0..width]);
        std.crypto.hash.sha2.Sha256.hash(text, &document_sha256[i], .{});
    }
    try copyFeature(vectors[documents.len], question_fp16, question_features);
    const recipe_hash = try recipeSha256(v1_profile);
    try control.check();
    owns_lease = false;
    return .{ .allocator = backing, .document_count = documents.len, .question_tokens = budget.question_tokens, .documents_fp16 = documents_fp16, .question_fp16 = question_fp16, .documents = document_features, .question = question_features, .question_text = question_text, .document_sha256 = document_sha256, .producer = .{ .profile = v1_profile, .publication = publication, .identity = identity, .source_lock_sha256 = v1SourceLockSha256(), .recipe_sha256 = recipe_hash }, .lease = lease };
}

// --- per-token (copy17 `tokens-dual`) encoder entry ---

/// Reader input for the research profile. Owns `features` and keeps the
/// complete host footprint admitted until deinit.
pub const TokenFeatures = struct {
    allocator: A,
    features: token_features.Features,
    /// Active length of the question row (its ids plus EOS).
    question_tokens: usize,
    producer: Producer,
    /// sha256 of the run-time encoder LoRA applied to this pass (hybrid
    /// profile on the stock encoder); null for the merged research encoder.
    adapter_sha256: ?[64]u8 = null,
    lease: memory.AdmissionLease,

    pub fn deinit(self: *TokenFeatures) void {
        self.features.deinit();
        self.lease.release();
        self.* = undefined;
    }
};

const row_bytes: usize = width * @sizeOf(f32);
const padded_length: usize = token_features.max_padded_length;

/// Host bytes retained or transiently touched by `encodeTokenFeatures` for
/// `documents` documents whose texts (plus the question) total `text_bytes`:
/// both host output tensors at the padded ceiling, the `[2M, width]` feature
/// rows plus the row-selection temporaries, the query, copy ids, the i64 ids
/// and mask, the per-row i32 encodings, bookkeeping slices, the prefixed
/// and doubled text copies, and the citation source map (one SourceRange per
/// copy id, held by both the batch and the features).
pub fn tokenFeatureHostBytes(documents: usize, text_bytes: usize) !usize {
    return tokenFeatureHostBytesBounded(documents, text_bytes, padded_length);
}

/// `tokenFeatureHostBytes` at an explicit padded-row bound.
pub fn tokenFeatureHostBytesBounded(documents: usize, text_bytes: usize, bound: usize) !usize {
    if (documents == 0) return error.EmptyMemorillaCollection;
    const rows = try std.math.add(usize, documents, 1);
    const positions = try std.math.mul(usize, rows, bound);
    const document_positions = try std.math.mul(usize, documents, bound);
    var total = try std.math.mul(usize, positions, row_bytes * 2);
    total = try std.math.add(usize, total, try std.math.mul(usize, document_positions, row_bytes * 4));
    total = try std.math.add(usize, total, row_bytes);
    total = try std.math.add(usize, total, try std.math.mul(usize, document_positions, 2 * @sizeOf(u32)));
    total = try std.math.add(usize, total, try std.math.mul(usize, document_positions, 2 * @sizeOf(source_map.SourceRange)));
    total = try std.math.add(usize, total, try std.math.mul(usize, positions, 2 * @sizeOf(i64) + @sizeOf(i32)));
    total = try std.math.add(usize, total, try std.math.mul(usize, rows, 4 * @sizeOf(usize) + @sizeOf([]const u8)));
    total = try std.math.add(usize, total, try std.math.mul(usize, text_bytes, 3));
    return std.math.add(usize, total, try std.math.mul(usize, documents, 2));
}

/// Explicit admission for one tapped encoder run of `batch` rows padded to
/// `seq_len`: i64 ids and mask plus the tap scalar as inputs, two F32
/// `[batch, seq_len, width]` host outputs, and a conservative activation
/// workspace of eight F32 row-widths per padded position at the 128 ceiling.
/// The session's own admission turns `workspace_bytes` into the backend
/// scratch that `archRunAdmitted` requires for a Memorilla encoder.
pub fn tokenFeatureRunRequest(batch: usize, seq_len: usize) !session_mod.RunRequest {
    return tokenFeatureRunRequestBounded(batch, seq_len, padded_length, null);
}

/// Run-time adapter footprint of one tapped run: the packed F32 adapters
/// (host input and device copies) and the per-position low-rank and
/// projection-width temporaries of one adapted projection.
pub const RunAdapter = struct { bytes: usize, rank: usize, projection_width: usize };

/// `tokenFeatureRunRequest` at an explicit padded-row bound, plus the
/// adapter inputs and their device copies and temporaries when a run-time
/// encoder LoRA rides on the run.
pub fn tokenFeatureRunRequestBounded(batch: usize, seq_len: usize, bound: usize, adapter: ?RunAdapter) !session_mod.RunRequest {
    if (batch < 2) return error.InvalidMemorillaDocumentCount;
    return runRequestAmounts(batch, seq_len, bound, adapter);
}

/// `tokenFeatureRunRequestBounded` for one span sub-batch: any positive row
/// count (the question row is not part of a sub-batch).
pub fn spanSubBatchRunRequest(rows: usize, seq_len: usize, bound: usize, adapter: ?RunAdapter) !session_mod.RunRequest {
    if (rows == 0) return error.InvalidMemorillaDocumentCount;
    return runRequestAmounts(rows, seq_len, bound, adapter);
}

/// Device bytes one padded position of a tapped Qwen3-Embedding run can hold
/// at a layer's peak, in F32 row widths: the block keeps its attention tensors
/// (Q/K/V, normed and rotated copies, attention output, BF16 staging) until
/// it returns, the research FFN adds three BF16 `[rows, 9728]` intermediates
/// (5.7 row widths), and best-fit reuse of larger cached blocks wastes up to
/// a block. Measured on the two D1 span batches that exhausted the old bound
/// (plan 8.28): 15 x 443 had 21.8 row widths per position live when a 1.9-wide
/// FFN intermediate failed, 25 x 536 had 20.1 when a 1.0-wide state failed.
pub const token_feature_peak_row_widths: usize = 32;

fn runRequestAmounts(batch: usize, seq_len: usize, bound: usize, adapter: ?RunAdapter) !session_mod.RunRequest {
    if (bound == 0 or bound > token_features.hybrid_max_padded_length or seq_len == 0 or seq_len > bound) return error.MemorillaEncoderBoundExceeded;
    const positions = try std.math.mul(usize, batch, seq_len);
    var input_bytes = try std.math.add(usize, try std.math.mul(usize, positions, 2 * @sizeOf(i64)), @sizeOf(i64));
    var workspace = try std.math.mul(usize, try std.math.mul(usize, batch, bound), row_bytes * 8);
    if (adapter) |run| {
        // The hybrid passes (run-time adapter, padded bound 1024): eight row
        // widths at the bound covers short rows, but rows padded past a
        // quarter of the bound need the per-position peak of the actual
        // geometry, or the run exhausts its device budget mid-layer.
        workspace = @max(workspace, try std.math.mul(usize, positions, row_bytes * token_feature_peak_row_widths));
        input_bytes = try std.math.add(usize, input_bytes, try std.math.add(usize, run.bytes, @sizeOf(f32)));
        // Device adapters, then x A^T, B (x A^T), its scaled copy and the sum.
        const per_position = try std.math.add(usize, run.rank, try std.math.mul(usize, run.projection_width, 3));
        workspace = try std.math.add(usize, workspace, try std.math.add(usize, run.bytes, try std.math.mul(usize, positions, per_position * @sizeOf(f32))));
    }
    return .{
        .batch = batch,
        .sequence = seq_len,
        .input_bytes = input_bytes,
        .output_bytes = try std.math.mul(usize, positions, row_bytes * 2),
        .workspace_bytes = workspace,
    };
}

/// Per-token `tokens-dual` features for the research encoder profile. The
/// caller retains its handle and authorization closure and passes the already
/// canonical question (`canonicalQuestion`); documents are encoded verbatim
/// after the reference's space prefix. Same guards as `encode`: profile tag,
/// managed publication, embedding identity before and after, embedding asset
/// lease and execution lane, with the forward admitted through the session.
pub fn encodeTokenFeatures(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control) !TokenFeatures {
    return encodeTokenFeaturesImpl(a, handle, documents, question, limits, control, .{});
}

/// The hybrid profile's record pass: the same `tokens-dual` features, run on
/// the stock `qwen3_encoder_f32_v1` encoder with the bundle's encoder LoRA
/// applied to the q/v projections at run time (resident weights untouched)
/// and the hybrid evaluator's 1024-token padded-row bound. The producer is
/// the stock encoder's; `adapter_sha256` records the adapters applied.
pub fn encodeHybridTokenFeatures(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, adapter: *const encoder_lora.Adapter, limits: memory.Limits, control: Control) !TokenFeatures {
    return encodeTokenFeaturesImpl(a, handle, documents, question, limits, control, .{ .profile = v1_profile, .bound = token_features.hybrid_max_padded_length, .adapter = adapter });
}

/// The span copy half's record pass (`qwen_span_copy.span_features`):
/// per-token rows `[n, 2 * width]` (final then layer 24, each L2-normalized,
/// FP16-stored) aligned to the copy ids, from the stock encoder with the
/// bundle's run-time LoRA over the question-prefixed self-context documents.
pub const SpanTokenFeatures = struct {
    allocator: A,
    features: []f32,
    copy_ids: []u32,
    counts: []usize,
    /// Source map parallel to `copy_ids`, or null (see `token_features.Batch`).
    sources: ?[]source_map.SourceRange,
    /// Longest padded encoder row of the pass (prefix included).
    seq_len: usize,
    producer: Producer,
    adapter_sha256: [64]u8,
    lease: memory.AdmissionLease,

    pub fn rows(self: *const SpanTokenFeatures) usize {
        return self.copy_ids.len;
    }

    pub fn deinit(self: *SpanTokenFeatures) void {
        self.allocator.free(self.features);
        self.allocator.free(self.copy_ids);
        self.allocator.free(self.counts);
        if (self.sources) |sources| self.allocator.free(sources);
        self.lease.release();
        self.* = undefined;
    }
};

/// Extra host bytes of the span pass over `tokenFeatureHostBytesBounded`:
/// the question prefix copied in front of every doubled document (and its
/// re-encoding for the tail check).
pub fn spanFeatureExtraBytes(documents: usize, question_bytes: usize) !usize {
    const prefix = try std.math.add(usize, question_bytes, 64);
    return std.math.mul(usize, documents, try std.math.mul(usize, prefix, 3));
}

/// Span pass batching (plan 8.28). A read set of at least
/// `span_subbatch_default_min_documents` documents runs in length-sorted
/// sub-batches (`spanSubBatches`) instead of one batch padded to its longest
/// document, when the grouping finds more than one group; every document
/// stays its own row with no cross-document attention and the unused question
/// row is not encoded. Smaller read sets (R2) and single-group read sets keep
/// the one padded batch, bit-identical to before. Sub-batching R2 measured
/// -3 to -19 ms per request and changes numerics, so it stays off there.
/// `ANTFLY_MEMORILLA_SPAN_SUBBATCH=0` forces one padded batch everywhere.
pub const span_subbatch_env = "ANTFLY_MEMORILLA_SPAN_SUBBATCH";
/// Research override of the document threshold
/// (`ANTFLY_MEMORILLA_SPAN_SUBBATCH_MIN_DOCUMENTS`, at least 1).
pub const span_subbatch_min_documents_env = "ANTFLY_MEMORILLA_SPAN_SUBBATCH_MIN_DOCUMENTS";
pub const span_subbatch_default_min_documents: usize = 3;
/// Fixed cost of one extra encoder run in the grouping objective, in padded
/// positions (`ANTFLY_MEMORILLA_SPAN_SUBBATCH_RUN_POSITIONS`).
pub const span_subbatch_run_positions_env = "ANTFLY_MEMORILLA_SPAN_SUBBATCH_RUN_POSITIONS";
pub const span_subbatch_default_run_positions: usize = 256;

pub const SpanBatching = struct { enabled: bool = true, min_documents: usize = span_subbatch_default_min_documents, run_positions: usize = span_subbatch_default_run_positions };

fn spanBatchingFromEnv() SpanBatching {
    const env = @import("antfly_platform").env;
    return .{
        .enabled = env.getenvBoolDefault(span_subbatch_env, true),
        .min_documents = @max(env.getenvUsize(span_subbatch_min_documents_env) orelse span_subbatch_default_min_documents, 1),
        .run_positions = env.getenvUsize(span_subbatch_run_positions_env) orelse span_subbatch_default_run_positions,
    };
}

/// The grouping a span pass over rows of padded `lengths` (document order)
/// takes: null for one padded batch (sub-batching off, too few documents, or
/// a single group), else the row order (ascending length, ties by document)
/// and each group's end in it.
pub const SpanGroups = struct {
    order: []usize,
    ends: []usize,

    pub fn deinit(self: *SpanGroups, a: A) void {
        a.free(self.order);
        a.free(self.ends);
    }
};

pub fn spanGroups(a: A, lengths: []const usize, batching: SpanBatching) !?SpanGroups {
    if (!batching.enabled or lengths.len < batching.min_documents or lengths.len == 0) return null;
    const order = try a.alloc(usize, lengths.len);
    errdefer a.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    const Less = struct {
        fn less(active: []const usize, x: usize, y: usize) bool {
            return active[x] < active[y] or (active[x] == active[y] and x < y);
        }
    };
    std.mem.sort(usize, order, lengths, Less.less);
    const sorted = try a.alloc(usize, lengths.len);
    defer a.free(sorted);
    for (order, sorted) |row, *length| length.* = lengths[row];
    const ends = try spanSubBatches(a, sorted, batching.run_positions);
    if (ends.len < 2) {
        a.free(ends);
        a.free(order);
        return null;
    }
    return .{ .order = order, .ends = ends };
}

/// Splits rows sorted by ascending length (`sorted_lengths`) into contiguous
/// groups minimizing `sum(rows_g * longest_g) + run_positions * groups` (each
/// group pads to its own longest row). Returns each group's end index.
pub fn spanSubBatches(a: A, sorted_lengths: []const usize, run_positions: usize) ![]usize {
    const n = sorted_lengths.len;
    if (n == 0) return error.EmptyMemorillaCollection;
    for (sorted_lengths[1..], sorted_lengths[0 .. n - 1]) |next, previous| if (next < previous) return error.InvalidMemorillaStates;
    const best = try a.alloc(usize, n + 1);
    defer a.free(best);
    const cut = try a.alloc(usize, n + 1);
    defer a.free(cut);
    best[0] = 0;
    for (1..n + 1) |i| {
        best[i] = std.math.maxInt(usize);
        for (0..i) |j| {
            const cost = best[j] + (i - j) * sorted_lengths[i - 1] + run_positions;
            // Ties keep fewer, larger groups (the earliest cut).
            if (cost < best[i]) {
                best[i] = cost;
                cut[i] = j;
            }
        }
    }
    var ends: std.ArrayListUnmanaged(usize) = .empty;
    errdefer ends.deinit(a);
    var i = n;
    while (i > 0) : (i = cut[i]) try ends.append(a, i);
    std.mem.reverse(usize, ends.items);
    return ends.toOwnedSlice(a);
}

/// Rows of a read set's leading documents that an earlier span pass of the
/// same request already computed (a cascade's narrow read inside its wide
/// read, plan 8.32): that pass's copy ids and feature rows, of which the
/// first `documents` documents' rows are reused. Each document is its own
/// encoder row, so its rows do not depend on the other documents read.
pub const SpanPrefix = struct { documents: usize, copy_ids: []const u32, features: []const f32 };

/// The feature rows of `prefix` that `batch` reuses, after checking that its
/// leading documents tokenized to the same copy ids.
fn reusedRows(batch: *const token_features.Batch, prefix: SpanPrefix) ![]const f32 {
    if (prefix.documents == 0 or prefix.documents > batch.counts.len) return error.InvalidMemorillaDocumentCount;
    var rows: usize = 0;
    for (batch.counts[0..prefix.documents]) |count| rows += count;
    if (rows > prefix.copy_ids.len or rows > batch.copy_ids.len or prefix.features.len < rows * 2 * width or
        !std.mem.eql(u32, prefix.copy_ids[0..rows], batch.copy_ids[0..rows])) return error.MemorillaCandidateMismatch;
    return prefix.features[0 .. rows * 2 * width];
}

/// The groups of a span pass whose first `reused` documents' rows come from
/// an earlier pass: the remaining rows as `spanGroups` groups them (any
/// count; one group in document order when it declines or batching is off),
/// in document indices. Empty when every document is reused.
pub fn spanGroupsAfter(a: A, lengths: []const usize, reused: usize, batching: SpanBatching) !SpanGroups {
    if (reused > lengths.len) return error.InvalidMemorillaDocumentCount;
    const rest = lengths[reused..];
    if (batching.enabled and rest.len > 0) {
        if (try spanGroups(a, rest, .{ .min_documents = 1, .run_positions = batching.run_positions })) |grouped| {
            for (grouped.order) |*row| row.* += reused;
            return grouped;
        }
    }
    const order = try a.alloc(usize, rest.len);
    errdefer a.free(order);
    for (order, reused..) |*slot, row| slot.* = row;
    const ends = try a.alloc(usize, if (rest.len == 0) 0 else 1);
    if (rest.len > 0) ends[0] = rest.len;
    return .{ .order = order, .ends = ends };
}

/// `prefix`: reuse an earlier pass's rows of the leading documents (only the
/// remaining documents are encoded, `spanGroupsAfter`); null encodes every
/// document as before.
pub fn encodeSpanTokenFeatures(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, adapter: *const encoder_lora.Adapter, prefix: ?SpanPrefix, limits: memory.Limits, control: Control) !SpanTokenFeatures {
    try control.check();
    if (documents.len == 0) return error.EmptyMemorillaCollection;
    if (documents.len > qualified.max_documents or question.len > max_text_bytes) return error.MemorillaTextLimitExceeded;
    const canonical = try canonicalQuestion(question);
    if (canonical.len == 0) return error.EmptyMemorillaQuestion;
    if (!std.mem.eql(u8, canonical, question)) return error.NonCanonicalMemorillaQuestion;
    var text_bytes: usize = question.len;
    for (documents) |text| {
        if (text.len > max_text_bytes) return error.MemorillaTextLimitExceeded;
        _ = try std.unicode.Utf8View.init(text);
        if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0) return error.BlankMemorillaDocument;
        text_bytes += text.len;
    }
    const bound = token_features.hybrid_max_padded_length;
    const host_bytes = try std.math.add(usize, try std.math.add(usize, try tokenFeatureHostBytesBounded(documents.len, text_bytes, bound), adapter.bytes()), try spanFeatureExtraBytes(documents.len, question.len));
    const profile = v1_profile;
    const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
    var pin = handle.duplicate();
    defer pin.release();
    const model = pin.get();
    try qualified.requireRecipe(profile, model.manifest);
    try factory.requireMemorillaProfile(model.session, profile);
    const publication = model.manifest.managed_publication orelse return error.MemorillaManagedProducerRequired;
    var lease = try pin.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = host_bytes });
    var owns_lease = true;
    defer if (owns_lease) lease.release();
    try control.check();
    var assets = try model.acquireEmbeddingAssetLeaseWithControl(false, control);
    defer assets.release();
    {
        try model.lockEmbeddingAssetsWithControl(control);
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(true, false, control);
    }
    try model.verifyEmbeddingIdentityWithControl(control);
    const identity = model.embedding_identity orelse return error.InvalidEmbeddingIdentity;
    const tokenizer = model.getTokenizer();
    const lane = try model.embedding_execution_lane.acquire(control);
    defer lane.release();
    var batch = try token_features.prepareSpanBounded(backing, tokenizer, documents, question, bound);
    defer batch.deinit();
    try control.check();
    if (adapter.layers == 0 or adapter.values.len % adapter.layers != 0) return error.InvalidMemorillaEncoderLora;
    const run_adapter = RunAdapter{ .bytes = adapter.bytes(), .rank = adapter.rank, .projection_width = encoder_lora.qwen3_embedding_4b.q_out };
    if (batch.counts.len == 0 or batch.counts.len + 1 != batch.batch) return error.InvalidMemorillaDocumentCount;
    const batching = spanBatchingFromEnv();
    const features = if (prefix) |reused| blk: {
        const rows = try reusedRows(&batch, reused);
        var after = try spanGroupsAfter(backing, batch.active[0..batch.counts.len], reused.documents, batching);
        defer after.deinit(backing);
        break :blk try spanFeaturesSubBatched(backing, model, &batch, adapter, run_adapter, bound, &after, reused.documents, rows, control);
    } else blk: {
        var groups = try spanGroups(backing, batch.active[0..batch.counts.len], batching);
        defer if (groups) |*g| g.deinit(backing);
        break :blk if (groups) |*g|
            try spanFeaturesSubBatched(backing, model, &batch, adapter, run_adapter, bound, g, 0, &.{}, control)
        else
            try spanFeaturesOneBatch(backing, model, &batch, adapter, run_adapter, bound, control);
    };
    errdefer backing.free(features);
    try control.check();
    try model.verifyEmbeddingIdentityWithControl(control);
    try factory.requireMemorillaProfile(model.session, profile);
    const copy_ids = try backing.dupe(u32, batch.copy_ids);
    errdefer backing.free(copy_ids);
    const counts = try backing.dupe(usize, batch.counts);
    errdefer backing.free(counts);
    const sources: ?[]source_map.SourceRange = if (batch.sources) |map| try backing.dupe(source_map.SourceRange, map) else null;
    errdefer if (sources) |owned| backing.free(owned);
    const recipe_hash = try recipeSha256(profile);
    try control.check();
    owns_lease = false;
    return .{
        .allocator = backing,
        .features = features,
        .copy_ids = copy_ids,
        .counts = counts,
        .sources = sources,
        .seq_len = batch.seq_len,
        .producer = .{ .profile = profile, .publication = publication, .identity = identity, .source_lock_sha256 = v1SourceLockSha256(), .recipe_sha256 = recipe_hash },
        .adapter_sha256 = adapter.sha256,
        .lease = lease,
    };
}

/// The span pass as one tapped run over the whole padded batch (documents
/// and the question row) -> `spanFeatures` rows in document order.
fn spanFeaturesOneBatch(backing: A, model: anytype, batch: *const token_features.Batch, adapter: *const encoder_lora.Adapter, run_adapter: RunAdapter, bound: usize, control: Control) ![]f32 {
    const shape = [_]i64{ @intCast(batch.batch), @intCast(batch.seq_len) };
    var ids = try Tensor.initInt64(backing, "input_ids", &shape, batch.input_ids);
    defer ids.deinit();
    const mask_values = try backing.alloc(i64, batch.input_ids.len);
    defer backing.free(mask_values);
    for (0..batch.batch) |row| {
        for (0..batch.seq_len) |position| mask_values[row * batch.seq_len + position] = if (position < batch.active[row]) 1 else 0;
    }
    var mask = try Tensor.initInt64(backing, "attention_mask", &shape, mask_values);
    defer mask.deinit();
    var outputs = try spanRun(backing, model, &ids, &mask, adapter, try tokenFeatureRunRequestBounded(batch.batch, batch.seq_len, bound, run_adapter), control);
    defer outputs.deinit(backing);
    return span_mod.spanFeatures(backing, outputs.final(), outputs.tap(), batch.batch, batch.seq_len, width, batch.active, batch.counts, 1);
}

/// The two tapped host outputs of one span run.
const SpanRunOutputs = struct {
    tensors: []Tensor,

    fn final(self: *const SpanRunOutputs) []const f32 {
        return self.tensors[0].asFloat32();
    }

    fn tap(self: *const SpanRunOutputs) []const f32 {
        return self.tensors[1].asFloat32();
    }

    fn deinit(self: *SpanRunOutputs, backing: A) void {
        for (self.tensors) |*output| output.deinit();
        backing.free(self.tensors);
    }
};

/// One tapped encoder run with the run-time LoRA (final and layer-24 states).
fn spanRun(backing: A, model: anytype, ids: *const Tensor, mask: *const Tensor, adapter: *const encoder_lora.Adapter, request: session_mod.RunRequest, control: Control) !SpanRunOutputs {
    var tap = try Tensor.initInt64(backing, factory.gpt_hidden_states_tap_input, &.{1}, &.{@intCast(token_features.tap_layer)});
    defer tap.deinit();
    var lora_values = try Tensor.initFloat32(backing, factory.gpt_encoder_lora_input, &.{ @intCast(adapter.layers), @intCast(adapter.values.len / adapter.layers) }, adapter.values);
    defer lora_values.deinit();
    var lora_scale = try Tensor.initFloat32(backing, factory.gpt_encoder_lora_scale_input, &.{1}, &.{adapter.scale});
    defer lora_scale.deinit();
    var permit = try model.session.admit(request);
    defer permit.deinit();
    try control.check();
    const outputs = try permit.runWithControl(&.{ ids.*, mask.*, tap, lora_values, lora_scale }, backing, control);
    errdefer {
        for (outputs) |*output| output.deinit();
        backing.free(outputs);
    }
    try control.check();
    try model.verifyEmbeddingIdentityWithControl(control);
    try factory.requireMemorillaProfile(model.session, v1_profile);
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32 or
        !std.mem.eql(u8, outputs[1].name, factory.gptHiddenStatesTapName(token_features.tap_layer)))
        return error.UnexpectedMemorillaEncoderOutputs;
    return .{ .tensors = outputs };
}

/// A span group pads to its longest row: the last one in a length-sorted
/// group, anywhere in the document-order group after reused rows.
pub fn groupPaddedLength(active: []const usize, rows: []const usize) usize {
    var longest: usize = 0;
    for (rows) |row| longest = @max(longest, active[row]);
    return longest;
}

/// The span pass in length-sorted sub-batches (`spanSubBatches`): each group
/// of documents is its own tapped run padded to its longest row; the
/// question row is not encoded. Rows return in document order. The first
/// `reused_documents` documents' rows are `reused` (no run); `groups` covers
/// the others.
fn spanFeaturesSubBatched(backing: A, model: anytype, batch: *const token_features.Batch, adapter: *const encoder_lora.Adapter, run_adapter: RunAdapter, bound: usize, groups: *const SpanGroups, reused_documents: usize, reused: []const f32, control: Control) ![]f32 {
    const documents = batch.counts.len;
    if (documents == 0 or documents + 1 != batch.batch or groups.order.len + reused_documents != documents) return error.InvalidMemorillaDocumentCount;
    const order = groups.order;
    const ends = groups.ends;
    // Each document's first feature row in document order.
    const offsets = try backing.alloc(usize, documents);
    defer backing.free(offsets);
    var total: usize = 0;
    for (batch.counts, offsets) |count, *offset| {
        offset.* = total;
        total += count;
    }
    const row_width = 2 * width;
    const features = try backing.alloc(f32, total * row_width);
    errdefer backing.free(features);
    const reused_rows = if (reused_documents == 0) 0 else offsets[reused_documents - 1] + batch.counts[reused_documents - 1];
    if (reused.len != reused_rows * row_width) return error.InvalidMemorillaStates;
    @memcpy(features[0..reused.len], reused);
    for (groups.order) |row| if (row < reused_documents) return error.InvalidMemorillaDocumentCount;
    var begin: usize = 0;
    for (ends) |end| {
        defer begin = end;
        const rows = order[begin..end];
        const seq_len = groupPaddedLength(batch.active, rows);
        const ids_values = try backing.alloc(i64, rows.len * seq_len);
        defer backing.free(ids_values);
        const mask_values = try backing.alloc(i64, rows.len * seq_len);
        defer backing.free(mask_values);
        const active = try backing.alloc(usize, rows.len);
        defer backing.free(active);
        const counts = try backing.alloc(usize, rows.len);
        defer backing.free(counts);
        for (rows, 0..) |row, j| {
            @memcpy(ids_values[j * seq_len ..][0..seq_len], batch.input_ids[row * batch.seq_len ..][0..seq_len]);
            for (0..seq_len) |position| mask_values[j * seq_len + position] = if (position < batch.active[row]) 1 else 0;
            active[j] = batch.active[row];
            counts[j] = batch.counts[row];
        }
        const shape = [_]i64{ @intCast(rows.len), @intCast(seq_len) };
        var ids = try Tensor.initInt64(backing, "input_ids", &shape, ids_values);
        defer ids.deinit();
        var mask = try Tensor.initInt64(backing, "attention_mask", &shape, mask_values);
        defer mask.deinit();
        try control.check();
        var outputs = try spanRun(backing, model, &ids, &mask, adapter, try spanSubBatchRunRequest(rows.len, seq_len, bound, run_adapter), control);
        defer outputs.deinit(backing);
        const group = try span_mod.spanFeatures(backing, outputs.final(), outputs.tap(), rows.len, seq_len, width, active, counts, 1);
        defer backing.free(group);
        var cursor: usize = 0;
        for (rows, counts) |row, count| {
            @memcpy(features[offsets[row] * row_width ..][0 .. count * row_width], group[cursor * row_width ..][0 .. count * row_width]);
            cursor += count;
        }
    }
    return features;
}

const TokenOptions = struct {
    profile: qualified.Profile = research_profile,
    bound: usize = token_features.max_padded_length,
    adapter: ?*const encoder_lora.Adapter = null,
};

fn encodeTokenFeaturesImpl(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control, options: TokenOptions) !TokenFeatures {
    try control.check();
    if (documents.len == 0) return error.EmptyMemorillaCollection;
    if (documents.len > qualified.max_documents or question.len > max_text_bytes) return error.MemorillaTextLimitExceeded;
    const canonical = try canonicalQuestion(question);
    if (canonical.len == 0) return error.EmptyMemorillaQuestion;
    if (!std.mem.eql(u8, canonical, question)) return error.NonCanonicalMemorillaQuestion;
    var text_bytes: usize = question.len;
    for (documents) |text| {
        if (text.len > max_text_bytes) return error.MemorillaTextLimitExceeded;
        _ = try std.unicode.Utf8View.init(text);
        if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0) return error.BlankMemorillaDocument;
        text_bytes += text.len;
    }
    // The adapter tensors are copied into one run input.
    const adapter_bytes: usize = if (options.adapter) |adapter| adapter.bytes() else 0;
    const host_bytes = try std.math.add(usize, try tokenFeatureHostBytesBounded(documents.len, text_bytes, options.bound), adapter_bytes);
    const profile = options.profile;
    if ((profile == v1_profile) != (options.adapter != null)) return error.IncompatibleMemorillaProducer;
    const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
    var pin = handle.duplicate();
    defer pin.release();
    const model = pin.get();
    try qualified.requireRecipe(profile, model.manifest);
    try factory.requireMemorillaProfile(model.session, profile);
    const publication = model.manifest.managed_publication orelse return error.MemorillaManagedProducerRequired;
    var lease = try pin.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = host_bytes });
    var owns_lease = true;
    defer if (owns_lease) lease.release();
    try control.check();
    var assets = try model.acquireEmbeddingAssetLeaseWithControl(false, control);
    defer assets.release();
    {
        try model.lockEmbeddingAssetsWithControl(control);
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(true, false, control);
    }
    try model.verifyEmbeddingIdentityWithControl(control);
    const identity = model.embedding_identity orelse return error.InvalidEmbeddingIdentity;
    const tokenizer = model.getTokenizer();
    const lane = try model.embedding_execution_lane.acquire(control);
    defer lane.release();
    var batch = try token_features.prepareBounded(backing, tokenizer, documents, question, options.bound);
    defer batch.deinit();
    try control.check();
    const shape = [_]i64{ @intCast(batch.batch), @intCast(batch.seq_len) };
    var ids = try Tensor.initInt64(backing, "input_ids", &shape, batch.input_ids);
    defer ids.deinit();
    const mask_values = try backing.alloc(i64, batch.input_ids.len);
    defer backing.free(mask_values);
    for (0..batch.batch) |row| {
        for (0..batch.seq_len) |position| mask_values[row * batch.seq_len + position] = if (position < batch.active[row]) 1 else 0;
    }
    var mask = try Tensor.initInt64(backing, "attention_mask", &shape, mask_values);
    defer mask.deinit();
    var tap = try Tensor.initInt64(backing, factory.gpt_hidden_states_tap_input, &.{1}, &.{@intCast(token_features.tap_layer)});
    defer tap.deinit();
    var lora_values: ?Tensor = null;
    defer if (lora_values) |*tensor| tensor.deinit();
    var lora_scale: ?Tensor = null;
    defer if (lora_scale) |*tensor| tensor.deinit();
    var run_adapter: ?RunAdapter = null;
    if (options.adapter) |adapter| {
        if (adapter.layers == 0 or adapter.values.len % adapter.layers != 0) return error.InvalidMemorillaEncoderLora;
        lora_values = try Tensor.initFloat32(backing, factory.gpt_encoder_lora_input, &.{ @intCast(adapter.layers), @intCast(adapter.values.len / adapter.layers) }, adapter.values);
        lora_scale = try Tensor.initFloat32(backing, factory.gpt_encoder_lora_scale_input, &.{1}, &.{adapter.scale});
        run_adapter = .{ .bytes = adapter.bytes(), .rank = adapter.rank, .projection_width = encoder_lora.qwen3_embedding_4b.q_out };
    }
    var permit = try model.session.admit(try tokenFeatureRunRequestBounded(batch.batch, batch.seq_len, options.bound, run_adapter));
    defer permit.deinit();
    try control.check();
    const outputs = if (options.adapter != null)
        try permit.runWithControl(&.{ ids, mask, tap, lora_values.?, lora_scale.? }, backing, control)
    else
        try permit.runWithControl(&.{ ids, mask, tap }, backing, control);
    defer {
        for (outputs) |*output| output.deinit();
        backing.free(outputs);
    }
    try control.check();
    try model.verifyEmbeddingIdentityWithControl(control);
    try factory.requireMemorillaProfile(model.session, profile);
    if (outputs.len != 2 or outputs[0].dtype != .f32 or outputs[1].dtype != .f32 or
        !std.mem.eql(u8, outputs[1].name, factory.gptHiddenStatesTapName(token_features.tap_layer)))
        return error.UnexpectedMemorillaEncoderOutputs;
    var features = try token_features.features(backing, outputs[0].asFloat32(), outputs[1].asFloat32(), &batch);
    errdefer features.deinit();
    const source_lock = if (profile == v1_profile) v1SourceLockSha256() else try researchSourceLockSha256(backing, model.model_dir);
    const recipe_hash = try recipeSha256(profile);
    try control.check();
    owns_lease = false;
    return .{
        .allocator = backing,
        .features = features,
        .question_tokens = batch.active[batch.batch - 1],
        .producer = .{ .profile = profile, .publication = publication, .identity = identity, .source_lock_sha256 = source_lock, .recipe_sha256 = recipe_hash },
        .adapter_sha256 = if (options.adapter) |adapter| adapter.sha256 else null,
        .lease = lease,
    };
}

test "Memorilla question rendering matches Python strip including Unicode boundaries" {
    try std.testing.expectEqualStrings("María", try canonicalQuestion("\x1c\t\u{3000}María\u{a0}\u{202f}"));
    try std.testing.expectError(error.EmptyMemorillaQuestion, canonicalQuestion("\u{2007}\r\n"));
    try std.testing.expectEqualStrings("x\u{200b}", try canonicalQuestion(" x\u{200b} "));
    try std.testing.expectError(error.InvalidUtf8, canonicalQuestion("\xff"));
    try std.testing.expectError(error.InvalidMemorillaQuestion, canonicalQuestion("Use <|memory|> directly"));
}

test "Memorilla storage casts FP16 before BF16 with ties to even" {
    const result = try storedValue(1.00390625);
    try std.testing.expectEqual(@as(f32, 1), result.memory);
    try std.testing.expectEqual(@as(f32, 1.00390625), @as(f32, @floatCast(result.fp16)));
    try std.testing.expectEqual(@as(f32, 1.015625), (try storedValue(1.01171875)).memory);
    try std.testing.expectError(error.NonFiniteMemorillaFeature, storedValue(std.math.nan(f32)));
}

test "Memorilla encoder rejects cancellation empty collection and injected question before touching a handle" {
    var untouched: manager.ModelHandle = undefined;
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, encode(std.testing.allocator, &untouched, &.{"doc"}, "question", .{}, .{ .check_fn = Cancel.check }));
    try std.testing.expectError(error.EmptyMemorillaCollection, encode(std.testing.allocator, &untouched, &.{}, "question", .{}, .{}));
    try std.testing.expectError(error.InvalidMemorillaQuestion, encodeQuestion(std.testing.allocator, &untouched, "<|memory|>", .{}, .{}));
}

test "Memorilla token-feature encoder rejects deadline collection and text limits before touching a handle" {
    var untouched: manager.ModelHandle = undefined;
    const a = std.testing.allocator;
    try std.testing.expectError(error.Timeout, encodeTokenFeatures(a, &untouched, &.{"doc"}, "question", .{}, .{ .deadline_ns = 0 }));
    try std.testing.expectError(error.EmptyMemorillaCollection, encodeTokenFeatures(a, &untouched, &.{}, "question", .{}, .{}));
    const many: [qualified.max_documents + 1][]const u8 = @splat("doc");
    try std.testing.expectError(error.MemorillaTextLimitExceeded, encodeTokenFeatures(a, &untouched, &many, "question", .{}, .{}));
    const long = try a.alloc(u8, max_text_bytes + 1);
    defer a.free(long);
    @memset(long, 'x');
    try std.testing.expectError(error.MemorillaTextLimitExceeded, encodeTokenFeatures(a, &untouched, &.{long}, "question", .{}, .{}));
    try std.testing.expectError(error.MemorillaTextLimitExceeded, encodeTokenFeatures(a, &untouched, &.{"doc"}, long, .{}, .{}));
    try std.testing.expectError(error.BlankMemorillaDocument, encodeTokenFeatures(a, &untouched, &.{" \t\n"}, "question", .{}, .{}));
    try std.testing.expectError(error.InvalidUtf8, encodeTokenFeatures(a, &untouched, &.{"\xff"}, "question", .{}, .{}));
    try std.testing.expectError(error.EmptyMemorillaQuestion, encodeTokenFeatures(a, &untouched, &.{"doc"}, " \u{3000}", .{}, .{}));
    try std.testing.expectError(error.NonCanonicalMemorillaQuestion, encodeTokenFeatures(a, &untouched, &.{"doc"}, " question ", .{}, .{}));
    try std.testing.expectError(error.InvalidMemorillaQuestion, encodeTokenFeatures(a, &untouched, &.{"doc"}, "<|memory|>", .{}, .{}));
}

test "Memorilla token-feature lease sizing covers outputs features inputs and text" {
    // rows = 2, positions = 256, document positions = 128, row = 10240 bytes:
    // 256*20480 + 128*40960 + 10240 + 128*8 + 256*20 + 2*48 + 3*10 + 2.
    try std.testing.expectEqual(@as(usize, 10_505_344), try tokenFeatureHostBytes(1, 10));
    try std.testing.expect(try tokenFeatureHostBytes(64, 0) > try tokenFeatureHostBytes(63, 8192 * 65));
    try std.testing.expectError(error.EmptyMemorillaCollection, tokenFeatureHostBytes(0, 0));
    try std.testing.expectError(error.Overflow, tokenFeatureHostBytes(std.math.maxInt(usize), 0));
    const request = try tokenFeatureRunRequest(3, 50);
    try std.testing.expectEqual(@as(usize, 3), request.batch);
    try std.testing.expectEqual(@as(usize, 50), request.sequence);
    try std.testing.expectEqual(@as(usize, 150 * 16 + 8), request.input_bytes);
    try std.testing.expectEqual(@as(?usize, 150 * 2560 * 4 * 2), request.output_bytes);
    try std.testing.expectEqual(@as(usize, 3 * 128 * 2560 * 4 * 8), request.workspace_bytes);
    try std.testing.expect(!request.workspace_planned);
    try std.testing.expectError(error.InvalidMemorillaDocumentCount, tokenFeatureRunRequest(1, 8));
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, tokenFeatureRunRequest(2, 129));
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, tokenFeatureRunRequest(2, 0));
    // The hybrid bound with run-time adapters: inputs carry the packed
    // adapters and scale, the workspace their device copies and temporaries.
    const adapter = RunAdapter{ .bytes = 11_796_480, .rank = 8, .projection_width = 4096 };
    const hybrid = try tokenFeatureRunRequestBounded(3, 300, token_features.hybrid_max_padded_length, adapter);
    try std.testing.expectEqual(@as(usize, 900 * 16 + 8 + 11_796_480 + 4), hybrid.input_bytes);
    // 3 x 300 rows pass a quarter of the bound: 32 row widths per position
    // (28,800) exceed 8 at the bound (24,576).
    try std.testing.expectEqual(@as(usize, 900 * 2560 * 4 * token_feature_peak_row_widths + 11_796_480 + 900 * (8 + 3 * 4096) * 4), hybrid.workspace_bytes);
    const short = try tokenFeatureRunRequestBounded(3, 200, token_features.hybrid_max_padded_length, adapter);
    try std.testing.expectEqual(@as(usize, 3 * 1024 * 2560 * 4 * 8 + 11_796_480 + 600 * (8 + 3 * 4096) * 4), short.workspace_bytes);
    // Rows padded past a quarter of the bound take the per-position peak: the
    // 15 x 443 D1 span batch that exhausted 8 row widths at the bound
    // (narrativeqa-validation-00318-119, plan 8.28).
    const long = try tokenFeatureRunRequestBounded(15, 443, token_features.hybrid_max_padded_length, adapter);
    try std.testing.expectEqual(@as(usize, 15 * 443 * 2560 * 4 * token_feature_peak_row_widths + 11_796_480 + 6645 * (8 + 3 * 4096) * 4), long.workspace_bytes);
    try std.testing.expect(long.workspace_bytes > 15 * 1024 * 2560 * 4 * 8 + 11_796_480 + 6645 * (8 + 3 * 4096) * 4);
    const sub = try spanSubBatchRunRequest(1, 536, token_features.hybrid_max_padded_length, adapter);
    try std.testing.expectEqual(@as(usize, 536 * 2560 * 4 * token_feature_peak_row_widths + 11_796_480 + 536 * (8 + 3 * 4096) * 4), sub.workspace_bytes);
    try std.testing.expectEqual(@as(?usize, 900 * 2560 * 4 * 2), hybrid.output_bytes);
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, tokenFeatureRunRequestBounded(3, 1025, token_features.hybrid_max_padded_length, adapter));
    try std.testing.expectError(error.MemorillaEncoderBoundExceeded, tokenFeatureRunRequestBounded(3, 8, token_features.hybrid_max_padded_length + 1, null));
    try std.testing.expect(try tokenFeatureHostBytesBounded(1, 10, 1024) > try tokenFeatureHostBytes(1, 10));
}

test "Memorilla producer check is profile specific" {
    const publication: [32]u8 = @splat(7);
    const v1 = Producer{ .profile = v1_profile, .publication = publication, .identity = qualified.executionIdentity(v1_profile), .source_lock_sha256 = v1SourceLockSha256(), .recipe_sha256 = try recipeSha256(v1_profile) };
    try requireProducer(v1, publication, v1_profile);
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(v1, @splat(8), v1_profile));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(v1, publication, .qwen3_decoder_f32_kv_f32_v1));
    var stale = v1;
    stale.source_lock_sha256[0] = if (stale.source_lock_sha256[0] == '0') '1' else '0';
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(stale, publication, v1_profile));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, recipeSha256(.qwen3_decoder_f32_kv_f32_v1));
    const lock: [64]u8 = @splat('a');
    const candidate = Producer{ .profile = research_profile, .publication = publication, .identity = qualified.executionIdentity(research_profile), .source_lock_sha256 = lock, .recipe_sha256 = try recipeSha256(research_profile) };
    try requireProducer(candidate, publication, research_profile);
    try requireProducerSourceLock(candidate, lock);
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducerSourceLock(candidate, @splat('b')));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(candidate, publication, v1_profile));
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(v1, publication, research_profile));
    try std.testing.expect(!std.mem.eql(u8, &candidate.recipe_sha256, &v1.recipe_sha256));
    try std.testing.expect(!std.mem.eql(u8, &candidate.identity, &v1.identity));
    var upper = candidate;
    upper.source_lock_sha256[3] = 'A';
    try std.testing.expectError(error.IncompatibleMemorillaProducer, requireProducer(upper, publication, research_profile));
}

test "Memorilla span sub-batches minimize padded positions plus run overhead" {
    const a = std.testing.allocator;
    // One long document among short ones: 4 x 100 + 256 = 656 padded in one
    // run vs 3 x 10 + 256 + 100 + 256 = 642 in two.
    const two = try spanSubBatches(a, &.{ 10, 10, 10, 100 }, 256);
    defer a.free(two);
    try std.testing.expectEqualSlices(usize, &.{ 3, 4 }, two);
    // A costlier run keeps one batch; similar lengths never split.
    const one = try spanSubBatches(a, &.{ 10, 10, 10, 100 }, 1000);
    defer a.free(one);
    try std.testing.expectEqualSlices(usize, &.{4}, one);
    const even = try spanSubBatches(a, &.{ 50, 52, 52, 60 }, 64);
    defer a.free(even);
    try std.testing.expectEqualSlices(usize, &.{4}, even);
    // Free runs: every distinct length its own group.
    const free = try spanSubBatches(a, &.{ 5, 5, 9 }, 0);
    defer a.free(free);
    try std.testing.expectEqual(@as(usize, 3), free[free.len - 1]);
    var padded: usize = 0;
    var start: usize = 0;
    const lengths = [_]usize{ 5, 5, 9 };
    for (free) |end| {
        padded += (end - start) * lengths[end - 1];
        start = end;
    }
    try std.testing.expectEqual(@as(usize, 19), padded);
    try std.testing.expectError(error.InvalidMemorillaStates, spanSubBatches(a, &.{ 9, 5 }, 0));
    try std.testing.expectError(error.EmptyMemorillaCollection, spanSubBatches(a, &.{}, 0));
}

test "Memorilla span pass sub-batches only read sets of three or more documents that split" {
    const a = std.testing.allocator;
    const on = SpanBatching{};
    // R2 keeps the one padded batch, however uneven (bit-identical default).
    try std.testing.expect((try spanGroups(a, &.{ 600, 40 }, on)) == null);
    // Three rows that do not split keep it too.
    try std.testing.expect((try spanGroups(a, &.{ 50, 52, 60 }, on)) == null);
    // Uneven read sets split; rows are ordered by length, ties by document.
    var groups = (try spanGroups(a, &.{ 100, 10, 10, 10 }, on)).?;
    defer groups.deinit(a);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 3, 0 }, groups.order);
    try std.testing.expectEqualSlices(usize, &.{ 3, 4 }, groups.ends);
    // The switch and the research threshold.
    try std.testing.expect((try spanGroups(a, &.{ 100, 10, 10, 10 }, .{ .enabled = false })) == null);
    var r2 = (try spanGroups(a, &.{ 600, 40 }, .{ .min_documents = 2 })).?;
    defer r2.deinit(a);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, r2.order);
    try std.testing.expect((try spanGroups(a, &.{}, .{ .min_documents = 1 })) == null);
}

test "Memorilla span pass after reused leading documents groups only the rest" {
    const a = std.testing.allocator;
    const on = SpanBatching{};
    // The narrow read's two documents reused: the uneven rest splits by length.
    var rest = try spanGroupsAfter(a, &.{ 70, 80, 100, 10, 10, 10 }, 2, on);
    defer rest.deinit(a);
    try std.testing.expectEqualSlices(usize, &.{ 3, 4, 5, 2 }, rest.order);
    try std.testing.expectEqualSlices(usize, &.{ 3, 4 }, rest.ends);
    // One remaining document, or rows that do not split: one group.
    var one = try spanGroupsAfter(a, &.{ 70, 80, 600 }, 2, on);
    defer one.deinit(a);
    try std.testing.expectEqualSlices(usize, &.{2}, one.order);
    try std.testing.expectEqualSlices(usize, &.{1}, one.ends);
    var even = try spanGroupsAfter(a, &.{ 70, 80, 60, 52, 50 }, 2, on);
    defer even.deinit(a);
    // Document order, not length order: the group pads to its longest row
    // wherever it sits (`spanFeaturesSubBatched`).
    try std.testing.expectEqualSlices(usize, &.{ 2, 3, 4 }, even.order);
    try std.testing.expectEqualSlices(usize, &.{3}, even.ends);
    // Batching off: the rest as one group in document order.
    var off = try spanGroupsAfter(a, &.{ 70, 80, 100, 10, 10, 10 }, 2, .{ .enabled = false });
    defer off.deinit(a);
    try std.testing.expectEqualSlices(usize, &.{ 2, 3, 4, 5 }, off.order);
    try std.testing.expectEqualSlices(usize, &.{4}, off.ends);
    // Everything reused: nothing to run.
    var none = try spanGroupsAfter(a, &.{ 70, 80 }, 2, on);
    defer none.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), none.order.len);
    try std.testing.expectEqual(@as(usize, 0), none.ends.len);
    try std.testing.expectError(error.InvalidMemorillaDocumentCount, spanGroupsAfter(a, &.{70}, 2, on));
    // Every group pads to its longest row, wherever it sits (a group padded
    // to its last row truncated the longer rows before it).
    const lengths = [_]usize{ 70, 80, 60, 52, 50 };
    try std.testing.expectEqual(@as(usize, 60), groupPaddedLength(&lengths, even.order));
    try std.testing.expectEqual(@as(usize, 100), groupPaddedLength(&.{ 70, 80, 100, 10, 10, 10 }, rest.order[3..4]));
    try std.testing.expectEqual(@as(usize, 10), groupPaddedLength(&.{ 70, 80, 100, 10, 10, 10 }, rest.order[0..3]));
}
