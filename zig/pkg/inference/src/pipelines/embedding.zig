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

// Embedding pipeline: tokenize -> encode -> pool -> normalize.
//
// Accepts a batch of text strings and returns [batch][hidden_dim] f32 embeddings.
// Uses the unified Tokenizer interface (HuggingFace or SentencePiece) and any
// backend Session (ONNX, native).

const CleanupContext = @import("../execution/cleanup_context.zig").Context;
const sequence_pool = @import("../execution/sequence_pool.zig");
const text_inputs = @import("../execution/text_inputs.zig");
const compute_provider = @import("../backends/compute_provider.zig");
const std = @import("std");
const build_options = @import("build_options");
const antfly_image = @import("antfly_image");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const linalg = @import("inference_linalg");
const tokenizer_mod = @import("inference_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const EncodeResult = tokenizer_mod.EncodeResult;
const Tensor = backends.Tensor;
const session_mod = @import("../backends/session.zig");
const ops_mod = @import("../ops/ops.zig");
const image = @import("image.zig");
const audio = @import("audio.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gemma2_content = @import("embedding_gemma2_content.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const resident_ops = @import("../graph/resident_ops.zig");
const embedding_trace = @import("../embedding_trace.zig");
const qwen_batch = @import("qwen_embedding_batch.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const memory_admission = @import("../runtime/tier/memory.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

const qwen3_embedding_resident_override_level = 4;

/// Owns the exact codec-slab admission deltas requested by image preprocessing.
/// Slabs grow geometrically between waves, so retaining the deltas makes the
/// process-wide reservation equal the largest physical slab without charging
/// every request the configured 128 MiB safety ceiling.
const PreprocessScratchAdmission = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    permits: std.ArrayListUnmanaged(session_mod.RunPermit) = .empty,

    fn descriptor(self: *@This()) image.ScratchAdmission {
        return .{ .context = self, .grow = growOpaque };
    }

    fn growOpaque(context: *anyopaque, current_bytes: usize, target_bytes: usize) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (target_bytes < current_bytes) return error.InvalidBatchPreprocessOptions;
        const additional = target_bytes - current_bytes;
        if (additional == 0) return;
        var permit = try self.session.admitHostPreprocess(additional);
        errdefer permit.deinit();
        try self.permits.append(self.allocator, permit);
    }

    pub fn deinit(self: *@This()) void {
        for (self.permits.items) |*permit| permit.deinit();
        self.permits.deinit(self.allocator);
        self.permits = .empty;
    }
};

const TextInputTensorSet = text_inputs.Set;

pub const PoolingStrategy = enum {
    mean,
    cls,
    max,
    last,
};

pub const ImagePreprocessProfile = enum {
    default,
    clip,
};

pub const EmbeddingConfig = struct {
    normalize: bool = true,
    reject_truncation: bool = false,
    pooling: PoolingStrategy = .mean,
    max_length: usize = 512,
    batch_size: usize = 32,
    /// When true, resident encoder/projection paths fail instead of silently
    /// falling back to host-session projection.
    resident_projection_required: bool = false,
    /// Optional text prefix applied before tokenization. Jina v5 text models
    /// default to document embeddings by encoding "Document: " + text.
    text_prefix: []const u8 = "",
    /// Trim padded text batches to the longest active sequence in the batch
    /// before encoder execution. Useful for decoder-style embedders where
    /// padding to the architectural context window is prohibitively expensive.
    trim_padding_to_batch_max: bool = false,
    /// Enable the direct resident Qwen3/Jina embedding encoder. This is set
    /// from Jina/Qwen3 embedding manifests, not merely from the backbone family.
    resident_qwen3_embedding: bool = false,
    /// Guarantee exactly one trailing EOS token on every encoded sequence.
    /// Last-token-pooling embedders (Qwen3-Embedding, Jina v5) read the EOS
    /// position; a missing EOS silently corrupts the embedding. The guard is
    /// idempotent: sequences already ending in EOS (tokenizers with a
    /// TemplateProcessing post-processor) are left untouched, so old and new
    /// tokenizer.json snapshots produce identical ids.
    ensure_trailing_eos_id: ?i32 = null,
    /// Infer resident stages from the session contract by default. Explicit
    /// false retains the host reference route for qualification or callers.
    resident_text_encoder: ?bool = null,
    /// Bidirectional EmbeddingGemma 2 uses its own resident encoder contract.
    resident_embedding_gemma2: bool = false,
    /// Keep single-image CUDA soft tokens resident through the encoder.
    /// Disabled only by the explicit host-path qualification control.
    resident_image_handoff: bool = true,
    /// Authoritative manifest input restrictions apply before media decoding,
    /// including native mixed-content and direct CLI pipeline entrypoints.
    allow_image: bool = true,
    allow_video: bool = true,
    allow_audio: bool = true,
    /// Reject context overflow instead of allowing tokenizer truncation.
    strict_max_length: bool = false,
    /// EmbeddingGemma 2 trims before normalization, including device readback.
    embedding_gemma2_dimensions: usize = 768,
    /// For CLIP/SigLIP multimodal models: image size for vision encoder.
    image_size: u32 = 224,
    /// Model-selected image preprocessing contract.
    image_preprocess_profile: ImagePreprocessProfile = .default,
    /// BackendRuntime-owned executor used for bounded image decode work.
    preprocess_io: ?std.Io = null,
    /// For CLAP audio models: mel spectrogram configuration.
    audio_config: audio.AudioConfig = audio.CLAP_CONFIG,
    /// Aggregate audio working-set budget shared by retained decoded PCM,
    /// CLAP preprocessing, and duplicated backend input tensors. The field
    /// retains its decode-specific name for source compatibility.
    max_audio_decode_working_bytes: usize = audio.default_decode_working_bytes,
    audio_prepare_ahead: bool = true,
    audio_prepared_batches: bool = true,
    gemma4_audio_batching: bool = true,
};

/// CLAP preprocessing handles clips sequentially, but its peak includes the
/// resampled 60-second window, centered-STFT padding, mel buffers, and FFT
/// scratch while the batch feature buffer remains resident. This conservative
/// bound is shared by every batch item rather than multiplied by batch size.
const clap_per_clip_preprocess_working_bytes: usize = 32 * 1024 * 1024;

const ClapBatchFeatureWorkingSet = struct {
    elements: usize,
    reserve_bytes: usize,
};

fn clapBatchFeatureWorkingSet(
    batch: usize,
    channels: usize,
    frames: usize,
    mels: usize,
) !ClapBatchFeatureWorkingSet {
    const batch_channels = std.math.mul(usize, batch, channels) catch return error.AudioTooLarge;
    const batch_frames = std.math.mul(usize, batch_channels, frames) catch return error.AudioTooLarge;
    const elements = std.math.mul(usize, batch_frames, mels) catch return error.AudioTooLarge;
    const feature_bytes = std.math.mul(usize, elements, @sizeOf(f32)) catch return error.AudioTooLarge;

    // clapFeaturesFromPcm's temporary output coexists with all_mels while each
    // clip is copied into the aggregate batch buffer.
    const preprocessing_reserve = std.math.add(usize, feature_bytes, clap_per_clip_preprocess_working_bytes) catch
        return error.AudioTooLarge;

    // Tensor.initFloat32 and Tensor.initBool duplicate their inputs, so both
    // the original and backend-owned copies coexist until session execution.
    const duplicated_feature_bytes = std.math.mul(usize, feature_bytes, 2) catch return error.AudioTooLarge;
    const duplicated_flags_bytes = std.math.mul(usize, batch, 2) catch return error.AudioTooLarge;
    const tensor_reserve = std.math.add(usize, duplicated_feature_bytes, duplicated_flags_bytes) catch
        return error.AudioTooLarge;

    return .{ .elements = elements, .reserve_bytes = @max(preprocessing_reserve, tensor_reserve) };
}

fn ensureClapWorkingSet(retained_pcm_bytes: usize, feature_reserve_bytes: usize, max_working_bytes: usize) !void {
    const peak = std.math.add(usize, retained_pcm_bytes, feature_reserve_bytes) catch return error.AudioTooLarge;
    if (peak > max_working_bytes) return error.AudioTooLarge;
}

pub const ClapBatchPlan = struct {
    channels: usize,
    frames: usize,
    mels: usize,
    feature_elements: usize,
    feature_reserve_bytes: usize,
    fusion_enabled: bool,
    force_short_singleton: bool,
};

pub fn clapPreprocessingPolicy(session: backends.Session, logical_batch: usize) @import("audio_preparation.zig").ClapPreprocessingPolicy {
    const fusion_enabled = if (session_factory.getClapConfig(session)) |cfg| cfg.audio_config.enable_fusion else false;
    return @import("audio_preparation.zig").ClapPreprocessingPolicy.init(fusion_enabled, logical_batch);
}

pub fn clapBatchPlan(session: backends.Session, batch: usize) !ClapBatchPlan {
    return clapBatchPlanWithPolicy(session, batch, clapPreprocessingPolicy(session, batch));
}

/// Reserve physical-batch storage while preserving the caller's feature policy.
pub fn clapBatchPlanWithPolicy(session: backends.Session, batch: usize, policy: @import("audio_preparation.zig").ClapPreprocessingPolicy) !ClapBatchPlan {
    const channels = policy.channels;
    const default_frames = audio.CLAP_CONFIG.chunk_length_s * audio.CLAP_CONFIG.sample_rate / audio.CLAP_CONFIG.hop_length + 1;
    const frames = clapInputFeatureFrames(session, default_frames);
    const mels = audio.CLAP_CONFIG.n_mels;
    const working = try clapBatchFeatureWorkingSet(batch, channels, frames, mels);

    return .{
        .channels = channels,
        .frames = frames,
        .mels = mels,
        .feature_elements = working.elements,
        .feature_reserve_bytes = working.reserve_bytes,
        .fusion_enabled = policy.fusion_enabled,
        .force_short_singleton = policy.force_short_singleton,
    };
}

pub const ResidentProjectionModality = enum { text, image, audio };
pub const ResidentProjectionOutcome = enum { success, fallback };

pub const ResidentProjectionStats = struct {
    text_success: u64 = 0,
    text_fallback: u64 = 0,
    image_success: u64 = 0,
    image_fallback: u64 = 0,
    audio_success: u64 = 0,
    audio_fallback: u64 = 0,

    pub fn add(self: *ResidentProjectionStats, other: ResidentProjectionStats) void {
        self.text_success += other.text_success;
        self.text_fallback += other.text_fallback;
        self.image_success += other.image_success;
        self.image_fallback += other.image_fallback;
        self.audio_success += other.audio_success;
        self.audio_fallback += other.audio_fallback;
    }
};

pub const AtomicResidentProjectionStats = struct {
    const AtomicU64 = std.atomic.Value(u64);

    text_success: AtomicU64 = .init(0),
    text_fallback: AtomicU64 = .init(0),
    image_success: AtomicU64 = .init(0),
    image_fallback: AtomicU64 = .init(0),
    audio_success: AtomicU64 = .init(0),
    audio_fallback: AtomicU64 = .init(0),

    pub fn record(
        self: *AtomicResidentProjectionStats,
        modality: ResidentProjectionModality,
        outcome: ResidentProjectionOutcome,
    ) void {
        switch (modality) {
            .text => switch (outcome) {
                .success => _ = self.text_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.text_fallback.fetchAdd(1, .monotonic),
            },
            .image => switch (outcome) {
                .success => _ = self.image_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.image_fallback.fetchAdd(1, .monotonic),
            },
            .audio => switch (outcome) {
                .success => _ = self.audio_success.fetchAdd(1, .monotonic),
                .fallback => _ = self.audio_fallback.fetchAdd(1, .monotonic),
            },
        }
    }

    pub fn snapshot(self: *const AtomicResidentProjectionStats) ResidentProjectionStats {
        return .{
            .text_success = self.text_success.load(.monotonic),
            .text_fallback = self.text_fallback.load(.monotonic),
            .image_success = self.image_success.load(.monotonic),
            .image_fallback = self.image_fallback.load(.monotonic),
            .audio_success = self.audio_success.load(.monotonic),
            .audio_fallback = self.audio_fallback.load(.monotonic),
        };
    }
};

pub const EncodedAudioClip = @import("audio_preparation.zig").Clip;

/// Ordered parts of one embedding input. The legacy content-part batch API
/// continues to produce one embedding per part.
pub const EmbeddingContentPart = union(enum) {
    text: []const u8,
    image: []const u8,
    audio: EncodedAudioClip,
    video: []const u8,
};

pub const EmbeddingContentInput = struct {
    content: []const EmbeddingContentPart,
};

fn resetIndexedCallState(last_input_tokens: *usize, errors: []?anyerror, item_count: usize) !void {
    if (errors.len != item_count) return error.InvalidInputShape;
    last_input_tokens.* = 0;
    @memset(errors, null);
}

test "indexed embedding calls clear prior errors and token usage" {
    var tokens: usize = 41;
    var errors = [_]?anyerror{ error.InvalidImage, error.InvalidAudioInput };
    try resetIndexedCallState(&tokens, &errors, 2);
    try std.testing.expectEqual(@as(usize, 0), tokens);
    try std.testing.expectEqualSlices(?anyerror, &.{ null, null }, &errors);
    try std.testing.expectError(error.InvalidInputShape, resetIndexedCallState(&tokens, errors[0..1], 2));
}

pub const EmbeddingPipeline = struct {
    batch_observation: ?*@import("batch_execution.zig").Observation = null,
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: EmbeddingConfig,
    /// Optional vision encoder session for CLIP/SigLIP multimodal embedding.
    vision_session: ?backends.Session = null,
    /// Optional audio encoder session for CLAP multimodal embedding.
    audio_session: ?backends.Session = null,
    /// Optional projection session for text embeddings.
    text_projection: ?backends.Session = null,
    /// Optional projection session for image embeddings.
    visual_projection: ?backends.Session = null,
    /// Optional projection session (e.g. audio_projection.onnx for CLIPCLAP).
    audio_projection: ?backends.Session = null,
    /// Optional caller-owned resident path counters for benchmark/service use.
    resident_projection_stats: ?*AtomicResidentProjectionStats = null,
    /// Optional caller-owned guard for backend state shared by pipelines made
    /// from the same loaded model. Tokenization and tensor preparation remain
    /// parallel; only device/session execution is serialized.
    execution_lock: ?*std.atomic.Mutex = null,
    execution_gate_held: bool = false,
    gemma_workspace: ?*@import("../backends/admitted_allocator.zig").AdmittedAllocator = null,
    execution_lane: ?*@import("../execution_control.zig").ExecutionLane = null,
    execution_lane_held: bool = false,
    execution_lane_from_lock: ?@import("../execution_control.zig").ExecutionLane.Lease = null,
    direct_cohort_waiter: @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter = .{},
    direct_cohort_owned: bool = false,
    interactive: bool = false,
    trace: ?*embedding_trace.Trace = null,
    /// Request lifetime propagated from ingress through model execution.
    execution_control: ?InferenceExecutionControl = null,
    /// Print phase timings for CLI/debug callers. TERMITE_EMBED_TIMING still
    /// enables the same logs for server and legacy workflows.
    print_timing: bool = false,
    /// Snapshot captured from the backend that executed the most recent
    /// resident encoder request. This is intentionally request-scoped because
    /// architecture sessions create and own their resident backend per run.
    last_resident_backend_timing: ?ops_mod.BackendDebugTimingSnapshot = null,
    audio_timings: ?*@import("embedding_gemma2.zig").MediaTimings = null,
    /// Expanded tokens successfully encoded by the most recent public call.
    last_input_tokens: usize = 0,

    fn usesResidentTextOutputs(self: *const EmbeddingPipeline) bool {
        return self.text_projection != null or self.residentTextEncoderEnabled();
    }

    pub fn residentTextEncoderEnabled(self: *const EmbeddingPipeline) bool {
        return self.config.resident_text_encoder orelse self.session.model_contract.resident_text_embeddings;
    }

    pub fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        tok: Tokenizer,
        config: EmbeddingConfig,
    ) EmbeddingPipeline {
        return .{
            .allocator = allocator,
            .session = session,
            .tok = tok,
            .config = config,
        };
    }

    pub fn initMultimodal(
        allocator: std.mem.Allocator,
        text_session: backends.Session,
        vision_session: backends.Session,
        tok: Tokenizer,
        config: EmbeddingConfig,
    ) EmbeddingPipeline {
        return .{
            .allocator = allocator,
            .session = text_session,
            .tok = tok,
            .config = config,
            .vision_session = vision_session,
        };
    }

    /// Cold eligibility only; a decline means no encoder was attempted.
    /// Unknown lane/workspace geometries keep their separately-owned adapter.
    pub fn canEmbedOwned(self: *const EmbeddingPipeline) bool {
        // A published owning entry needs the primary retaining Model before
        // resolving any provider, architecture policy or public callback.
        if (self.session.model_lifetime == null or @import("../execution/architecture_session.zig").hasInvalidPublishedModelHeader(self.session)) return false;
        if ((self.execution_lane != null and !self.usesLateWorkspaceLaneProtocol()) or self.execution_lane_held or self.execution_gate_held or self.gemma_workspace != null) return false;
        // Classify the retained Model through its typed provider, not an
        // optional config getter hidden by a copied/forwarded public table.
        // Losing that getter must never authorize the generic owned route.
        if (self.usesGemmaWorkspaceLaneProtocol() and !self.gemmaOwnedTextEligible()) return false;
        const source = self.textEncodingSession();
        const policy = source.run_admission orelse return false;
        const model = source.model_lifetime orelse return false;
        const domain = model.admission_domain orelse return false;
        if (domain.controller() != policy.controller) return false;
        _ = self.ownedTextOutputWidth() catch return false;
        return true;
    }

    /// Final vectors own exact original-run credit across bounded cohorts.
    /// Explicit caller cleanup governs this synchronous source teardown; no
    /// raw result escapes and no source is replayed after export failure.
    pub fn embedOwned(self: *EmbeddingPipeline, texts: []const []const u8, consumer_cleanup: CleanupContext) !session_mod.OwnedHostVectors {
        if (!consumer_cleanup.outside_execution or consumer_cleanup.boundary != null or consumer_cleanup.operation != null) return error.OwnedOutputRequiresOutsideEntry;
        if (!self.canEmbedOwned()) return error.UnsupportedOwnedEmbeddingEntry;
        if (consumer_cleanup.boundary) |current| if (try current.isSealed()) return error.RetirementEpochSealed;
        var scoped = self.*;
        scoped.allocator = @import("embedding_gemma2.zig").workspaceBackingAllocator(self.session.backend());
        scoped.last_input_tokens = 0;
        defer {
            self.last_input_tokens = scoped.last_input_tokens;
            self.last_resident_backend_timing = scoped.last_resident_backend_timing;
        }
        if (texts.len == 0) return .{ .owner = null };
        if (scoped.execution_control) |control| try control.update(.tokenizing, 0, @intCast(texts.len));
        if (scoped.gemmaOwnedTextEligible()) return scoped.embedGemma2TextsOwned(texts);
        if (scoped.boundedQwenBatchingEnabled())
            return scoped.embedBoundedQwenResult(texts, true, consumer_cleanup);
        if (textSessionBatchPlan(scoped.textEncodingSession(), texts.len)) |plan|
            return scoped.embedWithBatchPlanOwned(texts, plan, consumer_cleanup);
        return scoped.embedAdaptiveOwned(texts, consumer_cleanup);
    }

    fn embedWithBatchPlanOwned(self: *EmbeddingPipeline, texts: []const []const u8, plan: TextSessionBatchPlan, consumer_cleanup: CleanupContext) !session_mod.OwnedHostVectors {
        if (plan.batch_size == 0) return error.InvalidInputShape;
        var output = session_mod.OwnedHostVectors{ .owner = null };
        errdefer output.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker();
        var offset: usize = 0;
        while (offset < texts.len) {
            if (self.execution_control) |control| try control.check();
            const real_count = @min(plan.batch_size, texts.len - offset);
            if (real_count != plan.batch_size and !plan.pad_final_batch) return error.InvalidInputShape;
            var cohort = try self.embedDirectResult(texts[offset..][0..real_count], plan.batch_size, true, consumer_cleanup, null);
            defer cohort.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker();
            if (cohort.len() != real_count) return error.UnexpectedOutputShape;
            try output.appendMove(&cohort);
            offset += real_count;
        }
        return output;
    }
    /// SAME ResourceLimit-only bisection and midpoint as the raw adapter.
    /// Joining original container roots replaces a temporary pointer-array
    /// allocation; arithmetic schedules for every selected cohort are unchanged.
    fn embedAdaptiveOwned(self: *EmbeddingPipeline, texts: []const []const u8, consumer_cleanup: CleanupContext) anyerror!session_mod.OwnedHostVectors {
        return self.embedDirectResult(texts, texts.len, true, consumer_cleanup, null) catch |err| {
            if (err != error.ResourceLimitExceeded or texts.len <= 1) return err;
            const midpoint = (texts.len + 1) / 2;
            var left = try self.embedAdaptiveOwned(texts[0..midpoint], consumer_cleanup);
            errdefer left.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker();
            if (left.len() != midpoint) return error.UnexpectedOutputShape;
            var right = try self.embedAdaptiveOwned(texts[midpoint..], consumer_cleanup);
            defer right.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker();
            if (right.len() != texts.len - midpoint) return error.UnexpectedOutputShape;
            try left.appendMove(&right);
            return left;
        };
    }

    /// Embed a batch of texts, returning [batch_size][hidden_dim] embeddings.
    /// Caller owns the returned slices and must free them with the allocator.
    pub fn embed(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        // Release the model lane between bounded cohorts. A backlog must not
        // own the entire model working set for one arbitrarily large call.
        if (self.execution_lane != null and !self.execution_lane_held and texts.len > 4) {
            if (self.execution_control) |control| try control.check();
            // Earlier cohorts retain their results while later cohorts run.
            // Keep that output storage admitted for the complete request.
            const row_bytes = try std.math.add(usize, @sizeOf([]f32), try std.math.mul(usize, self.config.embedding_gemma2_dimensions, @sizeOf(f32)));
            var retained = try self.session.admitHostPreprocess(try std.math.mul(usize, texts.len, row_bytes));
            defer retained.deinit();
            const output = try self.allocator.alloc([]f32, texts.len);
            errdefer self.allocator.free(output);
            var completed: usize = 0;
            var tokens: usize = 0;
            errdefer for (output[0..completed]) |row| self.allocator.free(row);
            while (completed < texts.len) {
                const count = @min(@as(usize, 4), texts.len - completed);
                const rows = try self.embedCohort(texts[completed..][0..count]);
                @memcpy(output[completed..][0..count], rows);
                self.allocator.free(rows);
                tokens +|= self.last_input_tokens;
                completed += count;
            }
            self.last_input_tokens = tokens;
            return output;
        }
        return self.embedCohort(texts);
    }

    fn embedCohort(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        // Cancellation precedes model inspection and selecting its lane order.
        if (self.execution_control) |control| try control.check();
        try self.validateQwenWorkspaceAdmission();
        // All text routes for one actual dense Qwen model obey the SAME
        // ready/model -> numerical lane order, independent of batching flags.
        const lane = if (self.usesQwenWorkspaceLaneProtocol()) null else try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        self.last_input_tokens = 0;
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null and texts.len != 0) {
            // Request arenas retain logical frees. Encoder temporaries need a
            // reclaiming owner with an admitted physical allocation ceiling.
            const limit = 512 * 1024 * 1024;
            // The ceiling is a guard, not live residency. Charging all 512 MiB
            // rejects even one short CPU row under the default scratch budget.
            // Admit physical allocations in chunks before touching backing RAM.
            var admitted = @import("../backends/admitted_allocator.zig").AdmittedAllocator{
                .backing = @import("embedding_gemma2.zig").workspaceBackingAllocator(self.session.backend()),
                .session = self.session,
            };
            defer admitted.deinit();
            var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = admitted.allocator(), .limit = limit };
            defer std.debug.assert(bounded.live == 0);
            var scoped = self.*;
            scoped.allocator = bounded.allocator();
            scoped.gemma_workspace = &admitted;
            // Length buckets retain the admitted physical allocation ceiling.
            const temporary = (if (self.residentTextEncoderEnabled() and self.config.normalize and self.config.pooling == .mean and self.text_projection == null and (self.session.backend() == .native or self.session.backend() == .metal)) scoped.embedGemma2Texts(texts) else scoped.embedWithBatchPlan(texts, .{ .batch_size = 1, .pad_final_batch = false })) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else if (err == error.OutOfMemory) admitted.admission_error orelse err else err;
            defer {
                for (temporary) |vector| scoped.allocator.free(vector);
                scoped.allocator.free(temporary);
            }
            var output_permit = try self.session.admitHostPreprocess(try std.math.mul(usize, temporary.len, 768 * @sizeOf(f32) * 2 + @sizeOf([]f32)));
            defer output_permit.deinit();
            const output = try self.allocator.alloc([]f32, temporary.len);
            var count: usize = 0;
            errdefer {
                for (output[0..count]) |vector| self.allocator.free(vector);
                self.allocator.free(output);
            }
            for (temporary, output) |source, *dest| {
                dest.* = try self.allocator.dupe(f32, source);
                count += 1;
            }
            return output;
        }
        return self.embedUnscoped(texts);
    }

    /// Tokenize once, retain real lengths, and execute bounded length buckets.
    /// The outer Gemma2 owner accounts all tokenizer/packing storage physically.
    fn embedGemma2Texts(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        return self.embedGemma2TextsWithTaskOutput(texts, null);
    }
    fn embedGemma2TextsWithTaskOutput(self: *EmbeddingPipeline, texts: []const []const u8, task_output: ?*GemmaTaskHostOutput) ![][]f32 {
        const a = self.allocator;
        const max_len = textSequenceLengthForInputs(self.textEncodingSession().inputInfo(), self.config.max_length);
        const encoded = try a.alloc(EncodeResult, texts.len);
        defer a.free(encoded);
        var initialized: usize = 0;
        defer for (encoded[0..initialized]) |*row| row.deinit();
        const lengths = try a.alloc(usize, texts.len);
        defer a.free(lengths);
        const started = if (self.trace != null) embedding_trace.now() else 0;
        for (texts, 0..) |text, i| {
            if (self.execution_control) |control| try control.check();
            const rendered = try std.fmt.allocPrint(a, "{s}{s}", .{ self.config.text_prefix, text });
            defer a.free(rendered);
            encoded[i] = try self.tok.encodeForModel(a, rendered, max_len + 1);
            initialized += 1;
            lengths[i] = activeTokenLength(encoded[i].attention_mask);
            if (lengths[i] > max_len) return error.EmbeddingInputTooLong;
        }
        if (self.trace) |trace| trace.tokenize_ns += embedding_trace.now() -| started;
        const order = try qwen_batch.order(a, lengths);
        defer a.free(order);
        std.mem.sort(usize, order, lengths, struct {
            fn less(rows: []const usize, lhs: usize, rhs: usize) bool {
                return if (rows[lhs] == rows[rhs]) lhs < rhs else rows[lhs] < rows[rhs];
            }
        }.less);
        const vectors = try a.alloc([]f32, texts.len);
        errdefer a.free(vectors);
        var cursor: usize = 0;
        errdefer for (order[0..cursor]) |i| a.free(vectors[i]);
        while (cursor < order.len) {
            var sorted_lengths: [4]usize = undefined;
            const candidates = @min(@as(usize, 4), order.len - cursor);
            for (order[cursor..][0..candidates], sorted_lengths[0..candidates]) |i, *length| length.* = lengths[i];
            const compact_policy = @import("../architectures/embedding_gemma2.zig").compactRowsEnabled(self.session.backend() == .metal);
            var count = @import("embedding_gemma2.zig").preparedCohortCountForBackend(sorted_lengths[0..candidates], candidates, compact_policy);
            while (true) {
                const seq = lengths[order[cursor + count - 1]];
                var cohort_rows: usize = 0;
                for (sorted_lengths[0..count]) |length| cohort_rows += length;
                const request = session_mod.RunRequest{ .batch = if (compact_policy) 1 else count, .sequence = if (compact_policy) cohort_rows else seq, .input_bytes = count * seq * 24, .host_preprocess_bytes = count * seq * 32, .workspace_bytes = try @import("embedding_gemma2.zig").textEncoderWorkspace(if (compact_policy) cohort_rows else count * seq), .output_bytes = count * self.config.embedding_gemma2_dimensions * @sizeOf(f32) };
                if (count > 1 and !try self.session.fitsRun(request)) {
                    count -= 1;
                    continue;
                }
                const ids = try a.alloc(i64, count * seq);
                defer a.free(ids);
                const mask = try a.alloc(i64, count * seq);
                defer a.free(mask);
                @memset(ids, 0);
                @memset(mask, 0);
                for (order[cursor..][0..count], 0..) |i, row| {
                    for (encoded[i].ids[0..lengths[i]], encoded[i].attention_mask[0..lengths[i]], 0..) |id, active, col| {
                        ids[row * seq + col] = id;
                        mask[row * seq + col] = active;
                    }
                }
                var denied_before_dispatch = false;
                const result = self.embedTokenizedResult(ids, mask, count, seq, false, .{}, task_output, &denied_before_dispatch) catch |err| {
                    if (denied_before_dispatch and count > 1 and (err == error.ResourceLimitExceeded or err == error.MemoryBudgetExceeded or err == error.ResourceTemporarilyUnavailable)) {
                        count = (count + 1) / 2;
                        continue;
                    }
                    return err;
                };
                defer a.free(result);
                for (order[cursor..][0..count], result) |i, vector| vectors[i] = vector;
                if (self.trace) |trace| trace.shape(sorted_lengths[0..count], count, seq);
                if (self.batch_observation) |observation| observation.record(count);
                cursor += count;
                break;
            }
        }
        return vectors;
    }

    fn embedUnscoped(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        if (self.execution_control) |control| try control.update(.tokenizing, 0, @intCast(texts.len));
        if (texts.len == 0) return try self.allocator.alloc([]f32, 0);
        const text_session = self.textEncodingSession();
        if (textSessionBatchPlan(text_session, texts.len)) |plan| {
            return try self.embedWithBatchPlan(texts, plan);
        }

        if (self.boundedQwenBatchingEnabled()) return self.embedBoundedQwen(texts);

        if (self.cudaDocumentBatchingEnabled()) return @import("text_batch_admission.zig").run(self.allocator, self, texts, runDirectTextBatch, false);
        return embedTextBatchAdaptive(self.allocator, self, texts, runDirectTextBatch);
    }

    /// One result per ordered content group; text parts concatenate without
    /// inserted spaces. Media soft tokens join the same bidirectional encoder.
    pub fn embedContent(self: *EmbeddingPipeline, inputs: []const EmbeddingContentInput) ![][]f32 {
        if (!self.config.resident_embedding_gemma2) return error.OrderedEmbeddingContentNotSupported;
        var text_only = true;
        for (inputs) |item| {
            if (item.content.len == 0) return error.EmptyEmbeddingInput;
            for (item.content) |part| if (part != .text) {
                if ((part == .image and !self.config.allow_image) or
                    (part == .audio and !self.config.allow_audio) or
                    (part == .video and !self.config.allow_video)) return error.UnsupportedInput;
                text_only = false;
            };
        }
        if (inputs.len == 0) {
            self.last_input_tokens = 0;
            return self.allocator.alloc([]f32, 0);
        }
        if (text_only) {
            const texts = try self.allocator.alloc([]const u8, inputs.len);
            defer self.allocator.free(texts);
            var filled: usize = 0;
            defer for (texts[0..filled]) |text| self.allocator.free(text);
            for (inputs, 0..) |item, index| {
                var joined = std.ArrayListUnmanaged(u8).empty;
                errdefer joined.deinit(self.allocator);
                for (item.content) |part| try joined.appendSlice(self.allocator, part.text);
                texts[index] = try joined.toOwnedSlice(self.allocator);
                filled += 1;
            }
            return self.embed(texts);
        }
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        self.last_input_tokens = 0;
        const embeddings = try self.allocator.alloc([]f32, inputs.len);
        errdefer self.allocator.free(embeddings);
        var filled: usize = 0;
        errdefer for (embeddings[0..filled]) |embedding| self.allocator.free(embedding);
        for (inputs, 0..) |item, index| {
            embeddings[index] = try self.embedContentItem(item);
            filled += 1;
        }
        return embeddings;
    }

    fn embedContentItem(self: *EmbeddingPipeline, input: EmbeddingContentInput) anyerror![]f32 {
        const allocator = self.allocator;
        for (input.content) |part| if (part == .video) {
            if (input.content.len != 1) return error.InvalidEmbeddingGroup;
            if (!self.config.allow_video) return error.UnsupportedInput;
            const grouped = @import("embedding_gemma2.zig");
            const result = grouped.embed(allocator, self.session, self.tok, .{ .content = &.{.{ .video = part.video }} }, self.gemmaStageOptions(.{ .max_tokens = self.config.max_length }), self.execution_lock, self.execution_control) catch |err| return switch (err) {
                error.EmbeddingInputTooLong => error.EmbeddingSequenceTooLong,
                else => err,
            };
            self.last_input_tokens +|= result.input_tokens;
            return result.vector;
        };
        if (self.session.backend() == .cuda and input.content.len == 1 and input.content[0] == .text) {
            // Flat mixed batches also pass their text rows through this
            // dispatcher. Keep their prefix and length-bounded text admission.
            const vectors = try self.embedUnscoped(&.{input.content[0].text});
            defer allocator.free(vectors);
            return vectors[0];
        }
        if (self.session.backend() == .cuda and input.content.len == 1 and input.content[0] == .audio) {
            const clip = input.content[0].audio;
            const grouped = @import("embedding_gemma2.zig");
            // Pure audio, like pure image, has no task prefix.
            const result = grouped.embed(allocator, self.session, self.tok, .{
                .content = &.{.{ .audio = .{ .bytes = clip.bytes, .decode_options = clip.decode_options } }},
            }, self.gemmaStageOptions(.{ .max_tokens = self.config.max_length, .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes }), self.execution_lock, self.execution_control) catch |err| return switch (err) {
                error.EmbeddingInputTooLong => error.EmbeddingSequenceTooLong,
                else => err,
            };
            self.last_input_tokens +|= result.input_tokens;
            return result.vector;
        }
        if (self.config.resident_image_handoff and self.session.backend() == .cuda and
            input.content.len == 1 and input.content[0] == .image)
        {
            // Pure media has no task prefix. Mixed groups retain the ordered
            // frontend, including its manual markers and text-prefix policy.
            const grouped = @import("embedding_gemma2.zig");
            const result = grouped.embed(allocator, self.session, self.tok, .{
                .content = &.{.{ .image = input.content[0].image }},
            }, self.gemmaStageOptions(.{ .max_tokens = self.config.max_length }), self.execution_lock, self.execution_control) catch |err| return switch (err) {
                error.EmbeddingInputTooLong => error.EmbeddingSequenceTooLong,
                else => err,
            };
            self.last_input_tokens +|= result.input_tokens;
            return result.vector;
        }
        const parts = try allocator.alloc(gemma2_content.Part, input.content.len);
        defer allocator.free(parts);
        for (input.content, 0..) |part, index| parts[index] = switch (part) {
            .text => |text| .{ .text = text },
            .image => |bytes| .{ .image = bytes },
            .video => return error.InvalidEmbeddingGroup,
            .audio => |clip| .{ .audio = .{ .bytes = clip.bytes, .decode_options = clip.decode_options } },
        };
        // Reserve preprocessing independently from the eventual expanded
        // encoder sequence. The tower outputs determine its exact length.
        const input_bytes = std.math.mul(usize, self.config.max_length, 512 * @sizeOf(f32)) catch return error.ResourceLimitExceeded;
        const preprocessing_bytes = @max(128 * 1024 * 1024, self.config.max_audio_decode_working_bytes);
        var preprocess_permit = try self.session.admitHostPreprocess(std.math.add(usize, input_bytes, preprocessing_bytes) catch return error.ResourceLimitExceeded);
        defer preprocess_permit.deinit();
        if (comptime build_options.enable_cuda) {
            if (self.session.backend() == .cuda) {
                const Stage = @import("../execution/architecture_session.zig").StagedExecution;
                var permit = session_mod.RunPermit{ .session = self.session, .lease = null };
                defer permit.deinit();
                var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
                var stage = Stage{
                    .execution_lane = self.execution_lane,
                    .execution_lane_interactive = self.interactive,
                    .session = self.session,
                    .gate = self.execution_lock,
                    .waiter = &waiter,
                    .cb = undefined,
                    .control = self.execution_control orelse .{},
                    .active = false,
                    .permit = &permit,
                    .idle_backend = 0,
                    .scheduling_enabled = false,
                };
                defer stage.deinit();
                try stage.prepareBeforeExecution();
                // Keep the existing mixed-content envelope and renderer. The
                // runtime supplies cold admission/cache pressure outside lanes.
                const request: session_mod.RunRequest = .{ .batch = 1, .sequence = self.config.max_length, .workspace_bytes = 1536 * 1024 * 1024, .output_bytes = 768 * 4 };
                try stage.admitInitialCuda(allocator, request, null);
                try stage.enterInitial(allocator, self.execution_control);
                const backend = stage.backend();
                var prepared = try gemma2_content.prepareResident(backend, allocator, self.tok, self.config.text_prefix, parts, self.config.max_length, self.config.max_audio_decode_working_bytes, self.execution_control);
                defer prepared.deinit();
                const cfg = session_factory.getEmbeddingGemma2Config(self.session) orelse return error.UnsupportedEmbeddingGemma2Backend;
                if (prepared.resident_embeddings) |*resident_input| try stage.prepareCudaInputHandoff(resident_input);
                // Exact expanded input state may cross the quota phase only
                // after its real owner has certified/snapshotted independence.
                var expanded = request;
                expanded.sequence = prepared.sequence;
                expanded.preprocessing_complete = true;
                try stage.retain(expanded);
                const result = try stage.runResidentEmbeddingEncoder(allocator, cfg.resident orelse return error.UnsupportedEmbeddingGemma2Backend, prepared.resident_embeddings.?, prepared.attention_mask, 1, prepared.sequence, 768, false);
                self.last_input_tokens +|= prepared.sequence;
                return result;
            }
        }
        var prepared = prepare: {
            try self.lockExecution();
            defer self.unlockExecution();
            var managed = try compute_provider.acquireWithControl(self.session, allocator, self.execution_control);
            defer managed.deinit();
            break :prepare try gemma2_content.prepare(&managed.backend, allocator, self.tok, self.config.text_prefix, parts, self.config.max_length, self.config.max_audio_decode_working_bytes, self.execution_control);
        };
        defer prepared.deinit();
        const shape = [_]i64{ 1, @intCast(prepared.sequence) };
        var ids = try Tensor.initInt64(allocator, "input_ids", &shape, prepared.input_ids);
        defer ids.deinit();
        var mask = try Tensor.initInt64(allocator, "attention_mask", &shape, prepared.attention_mask);
        defer mask.deinit();
        var values = try Tensor.initFloat32(allocator, "inputs_embeds", &.{ 1, @intCast(prepared.sequence), 512 }, prepared.input_embeddings);
        defer values.deinit();
        const mask_i32 = try allocator.alloc(i32, prepared.sequence);
        defer allocator.free(mask_i32);
        for (prepared.attention_mask, 0..) |value, index| mask_i32[index] = @intCast(value);
        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        var source: session_mod.PreparedSource = .{};
        defer source.deinit();
        var projection_execution: ?session_mod.RunPermit = null;
        defer if (projection_execution) |*owned| owned.deinitWithCleanup(.{ .boundary = source.epoch() });
        const inputs = [_]Tensor{ ids, mask, values };
        try source.initBeforeExecutionInto(self.session, self.session, self.execution_lock, .{ .host = &inputs }, try session_mod.RunRequest.fromTensors(&inputs), null, if (self.usesResidentTextOutputs()) .resident_consumer else .host, allocator, self.execution_control);
        defer source.finishInputLifetime();
        var projection_shell = if (self.text_projection) |projection| try projection.prepareResidentRetirement() else null;
        defer if (projection_shell) |*shell| shell.destroyUnused();
        if (self.usesGemmaWorkspaceLaneProtocol() or self.usesNativeWeightLaneProtocol()) try source.bindExecutionLaneBeforeExecution(self.execution_lane, self.interactive);
        try source.enter();
        const late_lane = if (self.usesQwenWorkspaceLaneProtocol()) try self.enterNumericalExecutionLane() else null;
        var late_lane_live = true;
        defer if (late_lane_live) self.leaveExecutionLane(late_lane);
        const permit = try source.beginExecutionDispatch();
        self.execution_gate_held = true;
        defer {
            late_lane_live = false;
            self.leaveExecutionLane(late_lane);
            source.leaveExecution();
            self.execution_gate_held = false;
        }
        const embeddings = try self.embedPreparedTextInputs(&inputs, mask_i32, prepared.input_ids, 1, prepared.sequence, permit, &projection_shell, .{ .boundary = source.epoch() }, &projection_execution, .{ .prepared = &source }, null);
        defer allocator.free(embeddings);
        if (embeddings.len != 1) {
            for (embeddings) |embedding| allocator.free(embedding);
            return error.InvalidEmbeddingOutput;
        }
        self.last_input_tokens +|= prepared.sequence;
        return embeddings[0];
    }

    fn embedSeparateParts(self: *EmbeddingPipeline, parts: []const EmbeddingContentPart) ![][]f32 {
        const inputs = try self.allocator.alloc(EmbeddingContentInput, parts.len);
        defer self.allocator.free(inputs);
        for (inputs, 0..) |*input, index| input.* = .{ .content = parts[index .. index + 1] };
        return self.embedContent(inputs);
    }

    fn embedSeparatePartsIndexed(self: *EmbeddingPipeline, parts: []const EmbeddingContentPart, errors: []?anyerror) ![][]f32 {
        try resetIndexedCallState(&self.last_input_tokens, errors, parts.len);
        const vectors = try self.allocator.alloc([]f32, parts.len);
        errdefer self.allocator.free(vectors);
        var filled: usize = 0;
        errdefer for (vectors[0..filled]) |vector| self.allocator.free(vector);
        for (parts, 0..) |part, index| {
            vectors[index] = self.embedContentItem(.{ .content = &.{part} }) catch |err| failed: {
                if (self.execution_control) |control| try control.check();
                if (err == error.OutOfMemory or err == error.CudaOutOfMemory or err == error.ResourceLimitExceeded) return err;
                errors[index] = err;
                break :failed try self.allocator.alloc(f32, 0);
            };
            filled += 1;
        }
        return vectors;
    }

    fn runDirectTextBatch(ctx: *anyopaque, texts: []const []const u8) anyerror![][]f32 {
        const self: *EmbeddingPipeline = @ptrCast(@alignCast(ctx));
        return self.embedDirect(texts, texts.len);
    }

    fn usesGemmaWorkspaceLaneProtocol(self: *const EmbeddingPipeline) bool {
        return @import("../execution/architecture_session.zig").gemmaWorkspaceLanePolicy(self.session) orelse false;
    }
    fn usesNativeWeightLaneProtocol(self: *const EmbeddingPipeline) bool {
        return self.textEncodingSession().nativeWeightPreparationReceiver();
    }
    fn usesLateWorkspaceLaneProtocol(self: *const EmbeddingPipeline) bool {
        return self.usesQwenWorkspaceLaneProtocol() or self.usesGemmaWorkspaceLaneProtocol() or self.usesNativeWeightLaneProtocol();
    }
    fn gemmaStageOptions(self: *const EmbeddingPipeline, options: @import("embedding_gemma2.zig").Options) @import("embedding_gemma2.zig").Options {
        var result = options;
        if (self.usesGemmaWorkspaceLaneProtocol()) {
            result.execution_lane = self.execution_lane;
            result.execution_lane_interactive = self.interactive;
        }
        return result;
    }
    fn gemmaOwnedTextEligible(self: *const EmbeddingPipeline) bool {
        const backend = @import("../execution/architecture_session.zig").publishedModelBackend(self.session) orelse return false;
        const selected = switch (backend) {
            .native, .metal => true,
            .cuda => if (@import("../execution/architecture_session.zig").gemmaCudaOwnedTextWidth(self.session)) |width| self.config.embedding_gemma2_dimensions <= width else false,
            else => false,
        };
        return selected and self.usesGemmaWorkspaceLaneProtocol() and !self.config.resident_embedding_gemma2 and
            @import("../architectures/embedding_gemma2.zig").validDimension(self.config.embedding_gemma2_dimensions) and
            // Canonical CUDA already returns its original normalized 768-F32
            // head for either pipeline option. normalize=false only suppresses
            // the later prefix L2; it changes no encoder/source/kernel path.
            self.residentTextEncoderEnabled() and (self.config.normalize or backend == .cuda) and self.config.pooling == .mean and
            self.text_projection == null;
    }
    fn embedGemma2TextsOwned(self: *EmbeddingPipeline, texts: []const []const u8) !session_mod.OwnedHostVectors {
        if (!self.gemmaOwnedTextEligible() or self.execution_gate_held or self.execution_lane_held) return error.UnsupportedOwnedEmbeddingEntry;
        var producer: ?session_mod.HostOutputProducer = null;
        defer if (producer) |*value| {
            value.abortWithCleanup(.{ .outside_execution = true }) catch platform.inference_process_supervisor.restartWorker();
            value.finishOutsideExecution();
        };
        var task_output: GemmaTaskHostOutput = .{ .producer = &producer, .rows = texts.len, .width = self.config.embedding_gemma2_dimensions };
        const windowed = self.execution_lane != null and texts.len > 4;
        // SAME public raw whole-call retained-row floor, independent of the
        // ADDITIONAL named final bank on the first actual original Source.
        const row_bytes = try std.math.add(usize, @sizeOf([]f32), try std.math.mul(usize, self.config.embedding_gemma2_dimensions, @sizeOf(f32)));
        var retained = if (windowed) try self.session.admitHostPreprocess(try std.math.mul(usize, texts.len, row_bytes)) else null;
        defer if (retained) |*permit| permit.deinit();
        var cursor: usize = 0;
        var tokens: usize = 0;
        while (cursor < texts.len) {
            if (self.execution_control) |control| try control.check();
            const count = if (windowed) @min(@as(usize, 4), texts.len - cursor) else texts.len;
            // SAME original public window boundary, allocator and length-bucket
            // body. Do not sort or compact across another raw window's rows.
            {
                var admitted = @import("../backends/admitted_allocator.zig").AdmittedAllocator{
                    .backing = @import("embedding_gemma2.zig").workspaceBackingAllocator(self.session.backend()),
                    .session = self.session,
                };
                defer admitted.deinit();
                var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = admitted.allocator(), .limit = 512 * 1024 * 1024 };
                defer std.debug.assert(bounded.live == 0);
                var scoped = self.*;
                scoped.allocator = bounded.allocator();
                scoped.gemma_workspace = &admitted;
                scoped.last_input_tokens = 0;
                const vectors = (if (self.session.backend() == .cuda)
                    scoped.embedWithBatchPlanWithTaskOutput(texts[cursor..][0..count], .{ .batch_size = 1, .pad_final_batch = false }, &task_output)
                else
                    scoped.embedGemma2TextsWithTaskOutput(texts[cursor..][0..count], &task_output)) catch |err| {
                    return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else if (err == error.OutOfMemory) admitted.admission_error orelse err else err;
                };
                defer freeEmbeddingSlices(scoped.allocator, vectors);
                if (vectors.len != count) return error.UnexpectedOutputShape;
                const output = if (producer) |*value| value else return error.MissingHostOutputEscrow;
                for (vectors, 0..) |row, index| try output.writeRow(cursor + index, row);
                tokens +|= scoped.last_input_tokens;
                self.last_resident_backend_timing = scoped.last_resident_backend_timing;
            }
            cursor += count;
        }
        self.last_input_tokens = tokens;
        const output = if (producer) |*value| value else return error.MissingHostOutputEscrow;
        return output.publish();
    }

    fn usesQwenWorkspaceLaneProtocol(self: *const EmbeddingPipeline) bool {
        const session = self.textEncodingSession();
        if (@import("../execution/architecture_session.zig").qwenWorkspaceLanePolicy(session)) |actual| return actual;
        const cfg = session_factory.getGptConfig(session) orelse return false;
        return qwenWorkspaceLaneModel(session.backend(), cfg);
    }

    /// Public decoder metadata describes geometry; it cannot certify a Model
    /// domain or extend the lifetime of a charged SDK cache. Unknown raw
    /// sessions keep their original callback contract only when unadmitted.
    fn validateQwenWorkspaceAdmission(self: *const EmbeddingPipeline) !void {
        const session = self.textEncodingSession();
        const admission = session.run_admission orelse return;
        if (!self.usesQwenWorkspaceLaneProtocol()) return;
        if (!@import("../execution/architecture_session.zig").acceptsMetalWorkspaceSession(session))
            return error.MissingWorkspaceAdmissionDomain;
        // Actual canonical owner is now certified. Its immutable domain must
        // own THIS controller; an unrelated controller cannot fund late Roots.
        const pin = session.model_lifetime.?.admission_domain orelse return error.MissingWorkspaceAdmissionDomain;
        if (pin.controller() != admission.controller) return error.MissingWorkspaceAdmissionDomain;
    }

    fn boundedQwenBatchingEnabled(self: *const EmbeddingPipeline) bool {
        if (!@import("../execution/architecture_session.zig").acceptsMetalWorkspaceSession(self.textEncodingSession())) return false;
        if (!metal_runtime.appleSimdgroupFeatureEnabled("TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING", true) or
            self.text_projection != null) return false;
        const cfg = session_factory.getGptConfig(self.textEncodingSession()) orelse return false;
        return residentQwen3EmbeddingEligible(self.textEncodingSession(), cfg, self.config) and
            self.tok.vtable.encodeForModelUnpadded != null and
            !hasFixedTextSequenceLength(self.textEncodingSession().inputInfo());
    }

    fn embedBoundedQwen(self: *EmbeddingPipeline, texts: []const []const u8) ![][]f32 {
        return self.embedBoundedQwenResult(texts, false, .{});
    }

    fn embedBoundedQwenResult(self: *EmbeddingPipeline, texts: []const []const u8, comptime owned: bool, consumer_cleanup: CleanupContext) anyerror!(if (owned) session_mod.OwnedHostVectors else [][]f32) {
        if (owned and (!consumer_cleanup.outside_execution or consumer_cleanup.boundary != null or consumer_cleanup.operation != null)) return error.OwnedOutputRequiresOutsideEntry;
        const tokenize_started = if (self.trace != null) embedding_trace.now() else 0;
        const alloc = self.allocator;
        const session = self.textEncodingSession();
        const max_len = textSequenceLengthForInputs(session.inputInfo(), self.config.max_length);
        const Row = struct { encoded: EncodeResult, permit: session_mod.RunPermit };
        // Completed chunk vectors remain live while later chunks execute.
        // Keep their full batch storage admitted until pipeline return.
        const result_row_bytes = try std.math.mul(usize, self.configOutputWidth(), @sizeOf(f32));
        const host_floor = try std.math.mul(usize, texts.len, try std.math.add(usize, @sizeOf(Row) + 4 * @sizeOf(usize) + @sizeOf([]f32), result_row_bytes));
        var metadata_permit = if (owned)
            try session.admitHostPreprocessWithOutput(host_floor, texts.len, self.configOutputWidth())
        else
            try session.admitHostPreprocess(host_floor);
        defer metadata_permit.deinit();
        var producer: ?session_mod.HostOutputProducer = if (owned) try metadata_permit.prepareDeclaredHostOutputOutsideExecution() else null;
        defer if (producer) |*value| {
            value.abortWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker();
            value.finishOutsideExecution();
        };
        const rows = try alloc.alloc(Row, texts.len);
        defer alloc.free(rows);
        var initialized: usize = 0;
        defer for (rows[0..initialized]) |*row| {
            row.encoded.deinit();
            row.permit.deinit();
        };
        const lengths = try alloc.alloc(usize, texts.len);
        defer alloc.free(lengths);
        var input_tokens: usize = 0;
        for (texts, 0..) |text, index| {
            if (self.execution_control) |control| try control.update(.tokenizing, @intCast(index), @intCast(texts.len));
            const rendered_bytes = try std.math.add(usize, self.config.text_prefix.len, text.len);
            // A fixed allocator bounds every BPE temporary, including raw IDs
            // before truncation. Retain only actual rows after the slab drains.
            const slab_bytes = @max(@as(usize, 64 * 1024), try std.math.mul(usize, try std.math.add(usize, rendered_bytes, 1), 128));
            var permit = try session.admitHostPreprocess(try std.math.add(usize, slab_bytes, try std.math.add(usize, rendered_bytes, try std.math.mul(usize, @min(max_len, try std.math.add(usize, rendered_bytes, 2)), 8))));
            errdefer permit.deinit();
            const row_encoded: EncodeResult = blk: {
                const slab = try alloc.alloc(u8, slab_bytes);
                defer alloc.free(slab);
                var bounded = std.heap.FixedBufferAllocator.init(slab);
                const rendered = if (self.config.text_prefix.len > 0) try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.config.text_prefix, text }) else text;
                defer if (self.config.text_prefix.len > 0) alloc.free(rendered);
                var encoded = self.tok.encodeForModelUnpadded(bounded.allocator(), rendered, max_len) catch |err| switch (err) {
                    error.OutOfMemory => return error.ResourceLimitExceeded,
                    else => return err,
                };
                defer encoded.deinit();
                if (self.config.ensure_trailing_eos_id) |eos| ensureTrailingEos(&encoded, eos);
                const length = activeTokenLength(encoded.attention_mask);
                const ids = try alloc.dupe(i32, encoded.ids[0..length]);
                errdefer alloc.free(ids);
                const mask = try alloc.dupe(i32, encoded.attention_mask[0..length]);
                errdefer alloc.free(mask);
                break :blk .{ .ids = ids, .attention_mask = mask, .allocator = alloc };
            };
            errdefer {
                alloc.free(row_encoded.ids);
                alloc.free(row_encoded.attention_mask);
            }
            if (permit.lease) |*lease| try lease.retain(.{ .host_scratch_bytes = row_encoded.ids.len * 8 });
            rows[index] = .{ .encoded = row_encoded, .permit = permit };
            initialized += 1;
            lengths[index] = row_encoded.ids.len;
            for (row_encoded.attention_mask) |valid| if (valid != 0) {
                input_tokens +|= 1;
            };
        }
        const indices = try qwen_batch.order(alloc, lengths);
        defer alloc.free(indices);
        if (self.trace) |trace| trace.tokenize_ns += embedding_trace.now() -| tokenize_started;
        const vectors = if (!owned) try alloc.alloc([]f32, texts.len) else &.{};
        errdefer if (!owned) alloc.free(vectors);
        var completed: usize = 0;
        errdefer if (!owned) {
            for (indices[0..completed]) |index| alloc.free(vectors[index]);
        };
        var cursor: usize = 0;
        while (cursor < indices.len) {
            if (self.execution_control) |control| try control.check();
            var count = qwen_batch.chunkLength(lengths, indices[cursor..], self.config.batch_size);
            while (true) {
                var pressure: ?memory_admission.AdmissionPressure = null;
                var admission_denied = false;
                const result = self.runBoundedQwenChunk(Row, rows, indices[cursor..][0..count], if (producer) |*value| try value.heldAdmission() else null, &pressure, &admission_denied) catch |err| {
                    const splittable = admission_denied and (err == error.ResourceLimitExceeded or
                        (err == error.ResourceTemporarilyUnavailable and pressure != null and std.meta.activeTag(pressure.?) != .external_budget));
                    if (!splittable or count == 1) return err;
                    count = (count + 1) / 2;
                    continue;
                };
                defer alloc.free(result);
                if (result.len != count) {
                    for (result) |vector| alloc.free(vector);
                    return error.UnexpectedOutputShape;
                }
                if (owned) {
                    // Original indices, SAME length-sorted numerical chunks.
                    // No raw row survives this handoff; the original protected
                    // raw-result bank still covers every source/copy overlap.
                    defer for (result) |vector| alloc.free(vector);
                    for (indices[cursor..][0..count], result) |index, vector| try producer.?.writeRow(index, vector);
                } else for (indices[cursor..][0..count], result) |index, vector| vectors[index] = vector;
                completed += count;
                cursor += count;
                if (self.batch_observation) |observation| observation.record(count);
                break;
            }
        }
        self.last_input_tokens = input_tokens;
        if (owned) return producer.?.publish();
        return vectors;
    }

    fn qwenWorkspaceHostDeclaration(cold_capacity: usize, fresh_count: usize, receiving_count: usize, selected_metadata: usize, cpu_bytes: usize) !usize {
        const roles = @import("../backends/metal_workspace_roles.zig");
        const ownership = @import("../backends/metal_workspace_ownership.zig");
        const final_capacity = try std.math.add(usize, cold_capacity, fresh_count);
        const cold_arrays = try std.math.mul(usize, 2, try roles.Snapshot.requestedMetadataBytes(cold_capacity));
        const final_arrays = try roles.Snapshot.requestedMetadataBytes(final_capacity);
        const roots = try std.math.mul(usize, try std.math.add(usize, cold_capacity, final_capacity), @sizeOf(*ownership.Root));
        const bank_all = try ownership.UseBank.requestedMetadataBytes(receiving_count);
        // Receiving Fences are in the shared persistent metadata charge;
        // Entry arrays are synchronous and remain in the original permit.
        const bank_entries = bank_all - try std.math.mul(usize, receiving_count, @sizeOf(@import("../backends/metal_workspace_fence.zig").Fence));
        var bytes = try std.math.add(usize, try ownership.Phase.requestedPersistentMetadataBytes(fresh_count, receiving_count), bank_entries);
        inline for (.{ cold_arrays, final_arrays, roots, selected_metadata, cpu_bytes }) |part| bytes = try std.math.add(usize, bytes, part);
        return bytes;
    }

    /// First-cold family publication. This is a pre-numerical transaction:
    /// every supplier/metadata operation precedes the actual cohort/Model/lane,
    /// then the original family preparer creates the SAME scratch/token banks
    /// through their individually funded Root slots. Its published immutable
    /// formats are inspected only after all numerical/provider owners leave.
    /// A caller must rebuild its cold selected plan after this transaction.
    fn prepareBoundedQwenWorkspaceFamily(
        self: *EmbeddingPipeline,
        request: session_mod.RunRequest,
        cleanup: CleanupContext,
        pressure: *?memory_admission.AdmissionPressure,
        selected: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter,
        held_output: ?session_mod.HostOutputProducer.HeldAdmission,
    ) anyerror!void {
        if (comptime !build_options.enable_metal) return error.UnsupportedResidentTextEncoder;
        if (self.execution_gate_held or self.execution_lane_held) return error.WorkspaceRequiresColdPreparation;
        const session = self.textEncodingSession();
        if (session.run_admission == null) return error.MissingRunAdmission;
        const epoch = cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
        const cfg = session_factory.getGptConfig(session) orelse return error.UnsupportedResidentTextEncoder;
        const architecture = @import("../execution/architecture_session.zig");
        const roles = @import("../backends/metal_workspace_roles.zig");
        const ownership = @import("../backends/metal_workspace_ownership.zig");
        const Metal = @import("../ops/metal_compute.zig").MetalCompute;
        const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
        // Constructor-only preparation retains the original complete request
        // floor and cannot call beginRequest or inspect unobserved credit.
        var constructor_permit = try session.admitWithPressure(request, pressure);
        defer constructor_permit.deinit();
        if (!try architecture.prepareQwenWorkspaceProviderOutsideExecution(session, &constructor_permit, self.execution_control))
            return error.UnsupportedResidentTextEncoder;
        var described = (try architecture.describeQwenWorkspaceAuxiliaryOutsideExecution(session, 0, self.execution_control)) orelse
            return error.UnsupportedResidentTextEncoder;
        defer described.deinitOutsideExecution();
        // Release before acquiring the SAME original floor plus disjoint
        // declarations; no double original peak and no supplier under a lane.
        constructor_permit.deinit();
        const old_count = (try described.census.physical.capturedCoverage()).witnessed_roots;
        const host_metadata = try qwenWorkspaceHostDeclaration(described.census.physical.entries.len, described.auxiliary.plan.fresh_slots, old_count, 0, 0);
        var declarations: [2]@import("../execution/prepared_storage_admission.zig").Requirement = undefined;
        var declaration_count: usize = 0;
        if (described.auxiliary.plan.fresh_bytes != 0) {
            declarations[declaration_count] = .{ .backend = .metal, .class = .workspace, .bytes = described.auxiliary.plan.fresh_bytes };
            declaration_count += 1;
        }
        if (host_metadata != 0) {
            declarations[declaration_count] = .{ .backend = .native, .class = .host_workspace, .bytes = host_metadata };
            declaration_count += 1;
        }
        var permit = try session.admitDeclaredStorageWithHeldHostOutputWithPressure(request, declarations[0..declaration_count], held_output, pressure);
        defer permit.deinit();
        var phase = try ownership.Phase.prepareFromPermitOutsideExecution(session, &permit, &described.census.target, described.auxiliary.plan.fresh_slots, described.auxiliary.plan.fresh_bytes, old_count, self.execution_control);
        defer phase.finishOutsideExecution();
        const roots = try process.alloc(*ownership.Root, described.census.physical.entries.len);
        defer process.free(roots);
        const existing = try described.census.physical.copyRoots(roots);
        var bank = try ownership.UseBank.prepareExact(existing.len);
        defer bank.deinitOutsideExecution();
        try phase.fundReceivingBankOutsideExecution(&bank);
        try bank.bindOutsideExecution(existing, epoch);
        var validation = try roles.Snapshot.prepare(described.census.physical.entries.len);
        defer validation.deinitOutsideExecution();
        const final_capacity = try std.math.add(usize, described.census.physical.entries.len, described.auxiliary.plan.fresh_slots);
        var final = try roles.Snapshot.prepare(final_capacity);
        defer final.deinitOutsideExecution();
        const final_roots = try process.alloc(*ownership.Root, final_capacity);
        defer process.free(final_roots);
        // These final-address loans are synchronous only; no late node points
        // into the caller or this stack. Each bank node owns its own pure cell.
        var receiving: ownership.ReceiverUses = .{ .existing = &bank, .allocations = &phase };
        var current = cleanup;
        var context: @import("../execution/invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &receiving };
        const gate = session.execution_gate orelse self.execution_lock;
        try session.acquireSelectedCohort(self.execution_control orelse .{}, selected);
        defer session.releaseSelectedCohortGate(selected);
        const extra_gate = if (self.execution_lock != gate) self.execution_lock else null;
        if (extra_gate) |extra| {
            if (self.execution_control) |control| try control.lock(extra) else platform.sync.lockYielding(extra);
        }
        defer if (extra_gate) |extra| extra.unlock();
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        self.execution_gate_held = true;
        defer self.execution_gate_held = false;
        var managed = try architecture.constructMetalWorkspaceReceiver(session, self.allocator, self.execution_control);
        defer managed.deinitWithCleanup(current);
        const cb = &managed.backend;
        if (!try cb.bindCleanupEpoch(epoch)) return error.BackendOperationCleanupUnavailable;
        current.operation = cb.operationToken();
        context.cleanup = current;
        var binding = try Metal.beginPreparedAuxiliaryWorkspaceInvocation(cb, &context, &described.census.physical, &validation, &described.auxiliary);
        // Family preparation has the same checked scope lifecycle, with no
        // mathematical work or retry. Close before propagating its outcome.
        const body: anyerror!bool = body: {
            @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
            const prepared = cb.decoderRuntimePrepareOrReuseTextPrefill(self.allocator, cfg, cfg.num_hidden_layers) catch |err| break :body err;
            if (!prepared.prepared) break :body false;
            // GGUF logical registration deliberately leaves quantized SDK kinds
            // lazy. Publish the real SAME representations during this funded
            // cold family scope, before the outside selected-plan capture.
            break :body Metal.prepareOwnedQwenWorkspaceCatalog(cb, cfg, cfg.num_hidden_layers, self.execution_control) catch |err| break :body err;
        };
        const closed = binding.closeReported(&final, final_roots) catch platform.inference_process_supervisor.restartWorker();
        receiving.allocations = null;
        const prepared = try body; // real SDK errors retain their original cause
        try closed.check();
        if (!prepared) return error.PreparedWorkspaceFamilyUnavailable;
    }

    fn runBoundedQwenChunk(self: *EmbeddingPipeline, comptime Row: type, rows: []const Row, indices: []const usize, held_output: ?session_mod.HostOutputProducer.HeldAdmission, pressure: *?memory_admission.AdmissionPressure, admission_denied: *bool) anyerror![][]f32 {
        if (comptime !build_options.enable_metal) return error.UnsupportedResidentTextEncoder;
        if (self.execution_gate_held or self.execution_lane_held) return error.WorkspaceRequiresColdPreparation;
        try self.validateQwenWorkspaceAdmission();
        const session = self.textEncodingSession();
        const cfg = session_factory.getGptConfig(session) orelse return error.UnsupportedResidentTextEncoder;
        if (!@import("../execution/architecture_session.zig").acceptsMetalWorkspaceSession(session)) return error.UnsupportedWorkspaceTarget;
        const architecture = @import("../execution/architecture_session.zig");
        const role_types = @import("../backends/metal_workspace_roles.zig");
        const ownership = @import("../backends/metal_workspace_ownership.zig");
        const Metal = @import("../ops/metal_compute.zig").MetalCompute;
        const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
        var cleanup_owner = try @import("../execution/cleanup_owner.zig").Owner.beginResident(session);
        defer cleanup_owner.finishAfterUnlock();
        const cleanup: CleanupContext = .{ .boundary = cleanup_owner.epoch() };
        const epoch = cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
        var sequence: usize = 1;
        for (indices) |index| sequence = @max(sequence, rows[index].encoded.ids.len);
        if (indices.len == 1 and sequence == 1) sequence = 2;
        const total = try std.math.mul(usize, indices.len, sequence);
        const original_workspace = if (session.run_admission) |admission|
            try admission.qwenTransientWorkspaceBytes(total)
        else
            try std.math.add(usize, 16 * 1024 * 1024, try std.math.mul(usize, total, self.configOutputWidth() * 16));
        // Preserve the COMPLETE original opaque/private/static/mandatory/IO
        // floor. Exact fresh SDK declarations are additive; no old fungible
        // byte estimate or snapshot is spent as replacement credit.
        const request: session_mod.RunRequest = .{
            .batch = indices.len,
            .sequence = sequence,
            .input_bytes = try std.math.mul(usize, total, 12),
            .output_bytes = try std.math.mul(usize, indices.len, self.configOutputWidth() * @sizeOf(f32)),
            .workspace_bytes = original_workspace,
        };
        var waiter: @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter = .{};
        const gate = session.execution_gate orelse self.execution_lock;
        const lock_started = if (self.trace != null) embedding_trace.now() else 0;
        try session.selectReadyCohort(gate, self.execution_control orelse .{}, &waiter);
        defer session.leaveSelectedCohort(&waiter);
        if (session.run_admission == null)
            return self.runUnadmittedBoundedQwenChunk(Row, rows, indices, sequence, request, cleanup, &waiter);
        var layers: [256]ops_mod.DecoderRuntimeLayerSpec = undefined;
        const actual_layers = try decoder_gated_runtime.fillDenseQwen3LayerSpecs(cfg, cfg.num_hidden_layers, &layers);
        const frame_request: ops_mod.DecoderRuntimePrefillFramePlanRequest = .{
            .contract = .qwen3_dense_text_prefill,
            .layer_count = actual_layers.len,
            .rows = total,
            .batch = indices.len,
            .seq_len = sequence,
            .hidden_size = cfg.hidden_size,
            .vocab_size = cfg.vocab_size,
            .num_attention_heads = cfg.num_attention_heads,
            .global_head_dim = 0,
            .ple_hidden_size = 0,
            .final_norm_slot = decoder_gated_runtime.finalNormSlot(cfg.num_hidden_layers),
            .final_lm_head_slot = 0,
            .include_tail = false,
            .layers = actual_layers,
        };
        const last = self.config.pooling == .last;
        if (!last and self.config.pooling != .mean) return error.UnsupportedWorkspaceTail;
        const normalize_in_frame = last and self.config.normalize and !Metal.workspaceFrameNormalizeDisabled();
        const tail_request: Metal.WorkspaceTailRequest = .{
            .batch = indices.len,
            .sequence = sequence,
            .width = cfg.hidden_size,
            .take_rows = last,
            .normalize_rows = normalize_in_frame,
            .pool_sequence = if (!last) sequence else if (self.config.normalize and !normalize_in_frame) 1 else null,
        };
        var family_prepared = false;
        var stale_attempts: usize = 0;
        while (true) {
            if (self.execution_control) |control| try control.check();
            var cold = (architecture.describeQwenWorkspaceOutsideExecution(session, &frame_request, tail_request, self.execution_control) catch |err| {
                if (err != error.QueueFull) return err;
                const control = self.execution_control orelse return err;
                if (control.deadline_ns == null) return err;
                try control.check();
                std.Thread.yield() catch {};
                continue;
            }) orelse {
                if (family_prepared) return error.PreparedWorkspaceFamilyUnavailable;
                // This original SDK preparation is BEFORE any numerical work;
                // afterwards the actual selected formats are replanned cold.
                try self.prepareBoundedQwenWorkspaceFamily(request, cleanup, pressure, &waiter, held_output);
                family_prepared = true;
                continue;
            };
            defer cold.deinitOutsideExecution();
            const fresh_count = try std.math.add(usize, cold.reference.fresh_slots, try std.math.add(usize, try std.math.add(usize, cold.roles.fresh_slots, cold.auxiliary.plan.fresh_slots), try std.math.add(usize, cold.tail.allocations.fresh_slots, cold.tail.staging.fresh_slots)));
            const fresh_bytes = try std.math.add(usize, cold.reference.fresh_bytes, try std.math.add(usize, try std.math.add(usize, cold.roles.fresh_bytes, cold.auxiliary.plan.fresh_bytes), try std.math.add(usize, cold.tail.allocations.fresh_bytes, cold.tail.staging.fresh_bytes)));
            const old_count = (try cold.census.physical.capturedCoverage()).witnessed_roots;
            const host_metadata = try qwenWorkspaceHostDeclaration(cold.census.physical.entries.len, fresh_count, old_count, try cold.metadata.requestedMetadataBytes(), try std.math.add(usize, cold.tail.host_cpu_bytes, cold.reference.host_cpu_bytes));
            var declarations: [2]@import("../execution/prepared_storage_admission.zig").Requirement = undefined;
            var declaration_count: usize = 0;
            if (fresh_bytes != 0) {
                declarations[declaration_count] = .{ .backend = .metal, .class = .workspace, .bytes = fresh_bytes };
                declaration_count += 1;
            }
            if (host_metadata != 0) {
                declarations[declaration_count] = .{ .backend = .native, .class = .host_workspace, .bytes = host_metadata };
                declaration_count += 1;
            }
            var permit = session.admitDeclaredStorageWithHeldHostOutputWithPressure(request, declarations[0..declaration_count], held_output, pressure) catch |err| {
                admission_denied.* = true;
                return err;
            };
            defer permit.deinit();
            var phase = try ownership.Phase.prepareFromPermitOutsideExecutionWithHostCpu(session, &permit, &cold.census.target, fresh_count, fresh_bytes, old_count, cold.reference.host_cpu_bytes, self.execution_control);
            defer phase.finishOutsideExecution();
            const root_storage = try process.alloc(*ownership.Root, cold.census.physical.entries.len);
            defer process.free(root_storage);
            const old_roots = try cold.census.physical.copyRoots(root_storage);
            var bank = try ownership.UseBank.prepareExact(old_roots.len);
            defer bank.deinitOutsideExecution();
            try phase.fundReceivingBankOutsideExecution(&bank);
            try bank.bindOutsideExecution(old_roots, epoch);
            var validation = try role_types.Snapshot.prepare(cold.census.physical.entries.len);
            defer validation.deinitOutsideExecution();
            const final_capacity = try std.math.add(usize, cold.census.physical.entries.len, fresh_count);
            var final = try role_types.Snapshot.prepare(final_capacity);
            defer final.deinitOutsideExecution();
            const final_roots = try process.alloc(*ownership.Root, final_capacity);
            defer process.free(final_roots);
            var uses: ownership.ReceiverUses = .{ .existing = &bank, .allocations = &phase };
            var current = cleanup;
            var context: @import("../execution/invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &uses };
            // All supplier and metadata work above runs with no actual Model,
            // provider, external gate or execution lane. Only this block owns
            // numerical state. A stale cold plan retries after ALL defers leave.
            const vectors = numerical: {
                try session.acquireSelectedCohort(self.execution_control orelse .{}, &waiter);
                defer session.releaseSelectedCohortGate(&waiter);
                const extra_gate = if (self.execution_lock != gate) self.execution_lock else null;
                if (extra_gate) |extra| {
                    if (self.execution_control) |control| try control.lock(extra) else platform.sync.lockYielding(extra);
                }
                defer if (extra_gate) |extra| extra.unlock();
                const lane = try self.enterExecutionLane();
                defer self.leaveExecutionLane(lane);
                self.execution_gate_held = true;
                defer self.execution_gate_held = false;
                var managed = try architecture.constructMetalWorkspaceReceiver(session, self.allocator, self.execution_control);
                defer managed.deinitWithCleanup(current);
                const cb = &managed.backend;
                if (!try cb.bindCleanupEpoch(epoch)) return error.BackendOperationCleanupUnavailable;
                current.operation = cb.operationToken();
                context.cleanup = current;
                if (!self.boundedQwenBatchingEnabled()) return error.WorkspacePolicyChanged;
                var binding = Metal.beginPreparedWorkspaceInvocation(cb, &context, &cold.census.physical, &validation, &cold.roles, &cold.auxiliary, &cold.tail, &cold.reference, &cold.metadata, &frame_request, &final, final_roots) catch |err| switch (err) {
                    error.WorkspaceRoleChanged, error.WorkspaceTailChanged, error.WorkspaceReferenceProgramChanged, error.WorkspaceTargetChanged, error.WorkspaceCensusChanged, error.WorkspaceRequestChanged => break :numerical null,
                    else => return err,
                };
                // Capture the original body outcome before closing ONCE. Its
                // inner SDK/CT defers have completed; no body error can bypass
                // checked cleanup or authorize replay through an outer retry.
                const body: anyerror![][]f32 = body: {
                    @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
                    if (self.trace) |trace| trace.execution_lock_ns += embedding_trace.now() -| lock_started;
                    const produced = self.embedBoundedQwenPacked(Row, rows, indices, sequence, current, cb) catch |err| break :body err;
                    break :body produced orelse error.PreparedWorkspaceKernelUnavailable;
                };
                const closed = binding.closeReported(&final, final_roots) catch platform.inference_process_supervisor.restartWorker();
                uses.allocations = null;
                const result = body catch |err| {
                    // Optional callbacks report null for allocation decline;
                    // use its certified cause, preserving real SDK/math errors.
                    if (err == error.PreparedWorkspaceKernelUnavailable) try closed.check();
                    return err;
                };
                errdefer freeEmbeddingSlices(self.allocator, result);
                try closed.check();
                break :numerical result;
            };
            if (vectors) |result| return result;
            stale_attempts += 1;
            if (stale_attempts == 8) return error.WorkspaceRoleChanged;
            // No numerical callback ran on this path. Loop teardown executes
            // outside every gate before rebuilding the immutable declarations.
        }
    }

    /// No admission means no new modern Root or quota credit. It still borrows
    /// exact old-root cells under its genuine resident B, so an ordinary peer
    /// cannot consume prior charged roots with an unbound cleanup context.
    fn runUnadmittedBoundedQwenChunk(self: *EmbeddingPipeline, comptime Row: type, rows: []const Row, indices: []const usize, sequence: usize, request: session_mod.RunRequest, cleanup: CleanupContext, selected: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter) anyerror![][]f32 {
        if (comptime !build_options.enable_metal) return error.UnsupportedResidentTextEncoder;
        const architecture = @import("../execution/architecture_session.zig");
        const roles = @import("../backends/metal_workspace_roles.zig");
        const ownership = @import("../backends/metal_workspace_ownership.zig");
        const Metal = @import("../ops/metal_compute.zig").MetalCompute;
        const process = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
        const session = self.textEncodingSession();
        var constructor_permit = try session.admit(request);
        defer constructor_permit.deinit();
        _ = try architecture.prepareQwenWorkspaceProviderOutsideExecution(session, &constructor_permit, self.execution_control);
        var census = (try architecture.snapshotQwenWorkspaceOutsideExecution(session, self.execution_control)) orelse return error.WorkspaceTargetChanged;
        defer census.deinitOutsideExecution();
        const root_storage = try process.alloc(*ownership.Root, census.physical.entries.len);
        defer process.free(root_storage);
        const roots = try census.physical.copyRoots(root_storage);
        var bank = try ownership.UseBank.prepareExact(roots.len);
        defer bank.deinitOutsideExecution();
        try bank.bindOutsideExecution(roots, cleanup.boundary.?);
        var validation = try roles.Snapshot.prepare(census.physical.entries.len);
        defer validation.deinitOutsideExecution();
        var final = try roles.Snapshot.prepare(census.physical.entries.len);
        defer final.deinitOutsideExecution();
        var uses: ownership.ReceiverUses = .{ .existing = &bank };
        var current = cleanup;
        var context: @import("../execution/invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &uses };
        const gate = session.execution_gate orelse self.execution_lock;
        try session.acquireSelectedCohort(self.execution_control orelse .{}, selected);
        defer session.releaseSelectedCohortGate(selected);
        const extra_gate = if (self.execution_lock != gate) self.execution_lock else null;
        if (extra_gate) |extra| {
            if (self.execution_control) |control| try control.lock(extra) else platform.sync.lockYielding(extra);
        }
        defer if (extra_gate) |extra| extra.unlock();
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        self.execution_gate_held = true;
        defer self.execution_gate_held = false;
        var managed = try architecture.constructMetalWorkspaceReceiver(session, self.allocator, self.execution_control);
        defer managed.deinitWithCleanup(current);
        const cb = &managed.backend;
        if (!try cb.bindCleanupEpoch(cleanup.boundary.?)) return error.BackendOperationCleanupUnavailable;
        current.operation = cb.operationToken();
        context.cleanup = current;
        // Preserve the exact original Main expression, including unadmitted
        // maxInt when there is no lease. This raw byte guard is NOT Root credit.
        const original_workspace_bytes = if (constructor_permit.lease) |lease| lease.amounts.backend_scratch_bytes else std.math.maxInt(usize);
        var binding = try Metal.bindUnadmittedBoundedWorkspaceInvocation(cb, &context, @intFromPtr(census.target.target.metal.runtime), &census.physical, &validation, original_workspace_bytes);
        const body: anyerror![][]f32 = body: {
            @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current) catch |err| break :body err;
            const result = self.embedBoundedQwenPacked(Row, rows, indices, sequence, current, cb) catch |err| break :body err;
            break :body result orelse error.UnsupportedResidentTextEncoder;
        };
        const closed = Metal.finishExistingWorkspaceInvocationReported(&binding, &final, root_storage) catch platform.inference_process_supervisor.restartWorker();
        const result = body catch |err| {
            if (err == error.UnsupportedResidentTextEncoder) try closed.check();
            return err;
        };
        errdefer freeEmbeddingSlices(self.allocator, result);
        try closed.check();
        return result;
    }

    fn embedBoundedQwenPacked(self: *EmbeddingPipeline, comptime Row: type, rows: []const Row, indices: []const usize, seq_len: usize, cleanup: CleanupContext, receiving: ?*const ops_mod.ComputeBackend) !?[][]f32 {
        const alloc = self.allocator;
        const total = try std.math.mul(usize, indices.len, seq_len);
        const ids = try alloc.alloc(i64, total);
        defer alloc.free(ids);
        const mask = try alloc.alloc(i32, total);
        defer alloc.free(mask);
        @memset(ids, self.tok.specialTokens().pad_id);
        @memset(mask, 0);
        for (indices, 0..) |index, row| for (rows[index].encoded.ids, rows[index].encoded.attention_mask, 0..) |id, active, col| {
            ids[row * seq_len + col] = id;
            mask[row * seq_len + col] = active;
        };
        if (receiving) |cb| {
            const cfg = session_factory.getGptConfig(self.textEncodingSession()) orelse return error.UnsupportedResidentTextEncoder;
            return self.embedTextResidentQwen3OnBackend(cb, cfg, mask, ids, indices.len, seq_len, &cleanup);
        }
        return self.tryEmbedTextResidentQwen3(mask, ids, indices.len, seq_len, cleanup, null, null);
    }

    fn configOutputWidth(self: *const EmbeddingPipeline) usize {
        const cfg = session_factory.getGptConfig(self.textEncodingSession()) orelse unreachable;
        return cfg.hidden_size;
    }

    /// Execute one backend batch. `texts` contains only caller-provided rows;
    /// any required static-batch padding is added after tokenization by copying
    /// the final encoded row. This keeps fixed-batch compatibility from
    /// multiplying tokenizer and prefix-allocation work on short tail batches.
    fn embedDirect(self: *EmbeddingPipeline, texts: []const []const u8, execution_batch: usize) ![][]f32 {
        return self.embedDirectResult(texts, execution_batch, false, .{}, null);
    }

    fn embedDirectResult(
        self: *EmbeddingPipeline,
        texts: []const []const u8,
        execution_batch: usize,
        comptime owned: bool,
        consumer_cleanup: CleanupContext,
        task_output: ?*GemmaTaskHostOutput,
    ) anyerror!(if (owned) session_mod.OwnedHostVectors else [][]f32) {
        if (self.execution_control) |control| try control.check();
        try self.validateQwenWorkspaceAdmission();
        if (texts.len == 0 or execution_batch < texts.len) return error.InvalidInputShape;
        const output_width = if (owned) try self.ownedTextOutputWidth() else 0;

        const text_session = self.textEncodingSession();
        const alloc = self.allocator;
        const input_info = text_session.inputInfo();
        const max_len = textSequenceLengthForInputs(input_info, self.config.max_length);
        const fixed_len = hasFixedTextSequenceLength(input_info);
        const batch = execution_batch;
        const max_preprocess_tokens = std.math.mul(usize, batch, max_len) catch
            return error.ResourceLimitExceeded;
        var preprocess_permit = text_session.admitHostPreprocess(
            std.math.mul(usize, max_preprocess_tokens, 32) catch
                return error.ResourceLimitExceeded,
        ) catch |err| return self.textAdmissionError(err, batch);
        defer preprocess_permit.deinit();

        const encoded = try alloc.alloc(EncodeResult, texts.len);
        defer alloc.free(encoded);
        var encoded_count: usize = 0;
        defer {
            for (encoded[0..encoded_count]) |*result| result.deinit();
        }

        const tokenize_started = if (self.trace != null) embedding_trace.now() else 0;
        var trace_lengths: [32]usize = @splat(0);
        var effective_len: usize = if (self.config.trim_padding_to_batch_max and !fixed_len) 1 else max_len;
        for (texts, 0..) |text, i| {
            if (self.execution_control) |control| try control.update(.tokenizing, @intCast(i), @intCast(texts.len));
            if (self.config.resident_embedding_gemma2) try gemma2_content.validateTextOnly(text);
            const token_text = if (self.config.text_prefix.len > 0)
                try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.config.text_prefix, text })
            else
                text;
            defer if (self.config.text_prefix.len > 0) alloc.free(token_text);

            // One extra token distinguishes an exact-limit input from a
            // truncated input, including the tokenizer's special tokens.
            const encode_len = if (self.config.strict_max_length or self.config.reject_truncation)
                std.math.add(usize, max_len, 1) catch return error.ResourceLimitExceeded
            else
                max_len;
            encoded[i] = try self.tok.encodeForModel(alloc, token_text, encode_len);
            encoded_count += 1;
            if ((self.config.strict_max_length or self.config.reject_truncation) and activeTokenLength(encoded[i].attention_mask) > max_len)
                return if (self.config.resident_embedding_gemma2) error.EmbeddingSequenceTooLong else error.EmbeddingInputTooLong;
            if (self.config.ensure_trailing_eos_id) |eos_id| {
                ensureTrailingEos(&encoded[i], eos_id);
            }
            if (self.trace != null and i < trace_lengths.len) trace_lengths[i] = activeTokenLength(encoded[i].attention_mask);
            if (self.config.trim_padding_to_batch_max and !fixed_len) {
                effective_len = @max(effective_len, activeTokenLength(encoded[i].attention_mask));
            }
        }
        if (self.execution_control) |control| try control.update(.tokenizing, @intCast(texts.len), @intCast(texts.len));

        if (self.trace) |trace| {
            trace.tokenize_ns += embedding_trace.now() -| tokenize_started;
            if (texts.len <= trace_lengths.len) trace.shape(trace_lengths[0..texts.len], batch, effective_len);
        }

        // The tokenizer/preprocessing lease above covers the conservative
        // maximum-context host allocation. Once the real longest row is known,
        // admit backend workspace at the actual padded sequence length. This
        // preserves fail-closed memory accounting without forcing short BGE-M3
        // batches to reserve 8K-token attention/FFN scratch.
        const text_plan = try text_inputs.Plan.init(input_info, &.{ @intCast(batch), @intCast(effective_len) }, .{ .attention_heads = text_session.model_contract.text_attention_heads });
        if (self.execution_control) |control| try control.check();
        // The runtime source owns its exact FIFO/model lane and whole-consumer B.
        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        for (0..8) |_| {
            if (self.execution_control) |control| try control.check();
            var source: session_mod.PreparedSource = .{};
            var typed_source_ready = false;
            defer if (owned) {
                if (typed_source_ready) source.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker() else source.deinitOutsideExecution();
            } else source.deinit();
            var projection_execution: ?session_mod.RunPermit = null;
            defer if (projection_execution) |*permit| permit.deinitWithCleanup(.{ .boundary = source.epoch() });
            const signature = text_plan.signature();
            const task_geometry: ?session_mod.RunPermit.HostOutputGeometry = if (task_output) |task|
                .{ .rows = task.rows, .width = task.width, .held = if (task.producer.*) |*producer| try producer.heldAdmission() else null }
            else
                null;
            self.prepareTextSourceWithTaskOutput(&source, text_session, .{ .host_shapes = signature.slice() }, .{
                .batch = batch,
                .sequence = effective_len,
                .input_bytes = text_plan.input_bytes,
                .host_preprocess_bytes = text_plan.temporary_bytes,
                .output_bytes = if (owned) try session_mod.PreparedSource.hostOutputRequestBytes(texts.len, output_width) else null,
            }, task_geometry, owned) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and source.canRetryBeforeExecution()) continue;
                return err;
            };
            if (task_geometry != null and task_geometry.?.held == null) task_output.?.producer.* = try source.takeTaskHostOutputBeforeExecution();
            if (owned) {
                try source.prepareHostOutputsBeforeExecution(texts.len, output_width);
                typed_source_ready = true;
            }
            if (self.execution_control) |control| try control.check();

            const all_ids = try alloc.alloc(i32, batch * effective_len);
            defer alloc.free(all_ids);
            const all_mask = try alloc.alloc(i32, batch * effective_len);
            defer alloc.free(all_mask);

            for (encoded, 0..) |result, i| {
                @memcpy(all_ids[i * effective_len .. (i + 1) * effective_len], result.ids[0..effective_len]);
                @memcpy(all_mask[i * effective_len .. (i + 1) * effective_len], result.attention_mask[0..effective_len]);
            }
            for (texts.len..batch) |i| {
                @memcpy(all_ids[i * effective_len .. (i + 1) * effective_len], all_ids[(texts.len - 1) * effective_len .. texts.len * effective_len]);
                @memcpy(all_mask[i * effective_len .. (i + 1) * effective_len], all_mask[(texts.len - 1) * effective_len .. texts.len * effective_len]);
            }

            // Convert i32 token IDs to i64 (ONNX graphs expect int64 tensors)
            const ids_i64 = try alloc.alloc(i64, batch * effective_len);
            defer alloc.free(ids_i64);
            const mask_i64 = try alloc.alloc(i64, batch * effective_len);
            defer alloc.free(mask_i64);

            for (0..batch * effective_len) |j| {
                ids_i64[j] = @intCast(all_ids[j]);
                mask_i64[j] = @intCast(all_mask[j]);
            }

            // Build input tensors
            const shape = [_]i64{ @intCast(batch), @intCast(effective_len) };
            var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
            defer input_ids_tensor.deinit();
            var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
            defer attention_mask_tensor.deinit();

            var input_set = try text_plan.materialize(alloc, input_ids_tensor, attention_mask_tensor, .{});
            defer input_set.deinit();
            defer source.finishInputLifetime();
            source.bindInputsBeforeExecution(.{ .host = input_set.slice() }, if (self.usesResidentTextOutputs()) .resident_consumer else .host) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and source.canRetryBeforeExecution()) continue;
                return err;
            };
            var projection_shell = if (self.text_projection) |projection| try projection.prepareResidentRetirement() else null;
            defer if (projection_shell) |*shell| shell.destroyUnused();
            if (self.usesGemmaWorkspaceLaneProtocol() or self.usesNativeWeightLaneProtocol()) try source.bindExecutionLaneBeforeExecution(self.execution_lane, self.interactive);
            try source.enter();
            const qwen_lane = if (self.usesQwenWorkspaceLaneProtocol()) try self.enterNumericalExecutionLane() else null;
            var qwen_lane_live = true;
            defer if (qwen_lane_live) self.leaveExecutionLane(qwen_lane);
            const active_permit = try source.beginExecutionDispatch();
            self.execution_gate_held = true;
            defer {
                // Release the late lane BEFORE the selected Model gate. Errors
                // before this defer still use the fallback lane owner above.
                qwen_lane_live = false;
                self.leaveExecutionLane(qwen_lane);
                source.leaveExecution();
                self.execution_gate_held = false;
            }
            var pre_entry_refusal = false;
            const vectors = self.embedPreparedTextInputs(
                input_set.slice(),
                all_mask,
                ids_i64,
                batch,
                effective_len,
                active_permit,
                &projection_shell,
                .{ .boundary = source.epoch() },
                &projection_execution,
                .{ .prepared = &source },
                &pre_entry_refusal,
            ) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and pre_entry_refusal) continue;
                return err;
            };
            for (all_mask[0 .. texts.len * effective_len]) |valid| if (valid != 0) {
                self.last_input_tokens +|= 1;
            };
            if (self.batch_observation) |observation| observation.record(texts.len);
            if (owned) {
                defer freeEmbeddingSlices(alloc, vectors);
                if (vectors.len != execution_batch) return error.UnexpectedOutputShape;
                // Preserve padded execution shape; only real caller rows escape.
                return source.publishHostOutputs(vectors[0..texts.len]);
            }
            return vectors;
        }
        return error.ResourceTemporarilyUnavailable;
    }

    /// Benchmark-facing encoder contract without tokenization or model load.
    pub fn embedTokenized(self: *EmbeddingPipeline, input_ids: []const i64, attention_mask: []const i64, batch: usize, seq_len: usize) ![][]f32 {
        return self.embedTokenizedResult(input_ids, attention_mask, batch, seq_len, false, .{}, null, null);
    }

    /// One exact tokenized cohort, with original-run-funded final backing.
    /// The caller supplies no enclosing task execution lane. Native/Metal/CUDA
    /// numerical and projection choices are the SAME existing shared body.
    /// No unmanaged raw fallback or encoder replay is performed by this API.
    pub fn embedTokenizedOwned(self: *EmbeddingPipeline, input_ids: []const i64, attention_mask: []const i64, batch: usize, seq_len: usize, consumer_cleanup: CleanupContext) !session_mod.OwnedHostVectors {
        // Final and temporary output allocations must really reclaim. A
        // request Arena's logical free cannot justify retiring its run quota.
        if (!consumer_cleanup.outside_execution or consumer_cleanup.boundary != null or consumer_cleanup.operation != null) return error.OwnedOutputRequiresOutsideEntry;
        if (self.gemma_workspace != null) return error.OwnedOutputRequiresColdWorkspaceOwner;
        var scoped = self.*;
        scoped.allocator = @import("embedding_gemma2.zig").workspaceBackingAllocator(self.session.backend());
        defer {
            self.last_resident_backend_timing = scoped.last_resident_backend_timing;
            self.last_input_tokens = scoped.last_input_tokens;
        }
        return scoped.embedTokenizedResult(input_ids, attention_mask, batch, seq_len, true, consumer_cleanup, null, null);
    }

    fn ownedTextOutputWidth(self: *const EmbeddingPipeline) !usize {
        if (self.session.backend() == .cuda and self.gemmaOwnedTextEligible()) return self.config.embedding_gemma2_dimensions;
        if (self.text_projection == null and self.residentTextEncoderEnabled() and self.config.normalize and self.config.pooling == .mean and session_factory.getEmbeddingGemma2Config(self.session) != null)
            return self.config.embedding_gemma2_dimensions;
        const final_session = self.text_projection orelse self.textEncodingSession();
        return @import("../execution/architecture_session.zig").pooledEmbeddingOutputWidth(final_session) orelse error.UnsupportedOwnedOutputGeometry;
    }

    const GemmaTaskHostOutput = struct {
        producer: *?session_mod.HostOutputProducer,
        rows: usize,
        width: usize,
    };

    fn embedTokenizedResult(
        self: *EmbeddingPipeline,
        input_ids: []const i64,
        attention_mask: []const i64,
        batch: usize,
        seq_len: usize,
        comptime owned: bool,
        consumer_cleanup: CleanupContext,
        task_output: ?*GemmaTaskHostOutput,
        denied_before_dispatch: ?*bool,
    ) anyerror!(if (owned) session_mod.OwnedHostVectors else [][]f32) {
        var dispatch_started = false;
        if (denied_before_dispatch) |flag| flag.* = false;
        errdefer if (denied_before_dispatch) |flag| {
            // The outer private caller additionally filters actual resource
            // errors. No error name can undo a started numerical dispatch.
            flag.* = !dispatch_started;
        };
        if (self.execution_control) |control| try control.check();
        try self.validateQwenWorkspaceAdmission();
        // An enclosing task lane is not outside-execution authority. The
        // admitted owned entry must cold-prepare its original escrow first.
        // Existing raw entry keeps its original lane/fairness behavior.
        if (owned and (!consumer_cleanup.outside_execution or consumer_cleanup.boundary != null or consumer_cleanup.operation != null)) return error.OwnedOutputRequiresOutsideEntry;
        if (owned) if (consumer_cleanup.boundary) |current| {
            if (try current.isSealed()) return error.RetirementEpochSealed;
        };
        if (owned and ((self.execution_lane != null and !self.usesQwenWorkspaceLaneProtocol() and !self.usesNativeWeightLaneProtocol()) or self.execution_lane_held)) return error.OwnedOutputRequiresColdLaneEntry;
        const output_width = if (owned) try self.ownedTextOutputWidth() else 0;
        const lane = if (owned or self.usesQwenWorkspaceLaneProtocol()) null else try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (batch == 0 or seq_len == 0) return error.InvalidInputShape;
        if (self.config.strict_max_length and seq_len > self.config.max_length)
            return error.EmbeddingSequenceTooLong;
        self.last_resident_backend_timing = null;
        const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidInputShape;
        if (input_ids.len != total or attention_mask.len != total) return error.InvalidInputShape;

        const alloc = self.allocator;
        const text_session = self.textEncodingSession();
        const shape = [_]i64{ @intCast(batch), @intCast(seq_len) };
        if (self.execution_gate_held and text_session.preparesInvocation() and text_session.run_admission != null)
            return error.PreparedAdmissionRequiresReadyTurn;
        var preprocess_permit: ?session_mod.RunPermit = if (!self.execution_gate_held)
            try text_session.admitHostPreprocess(try std.math.mul(usize, total, 32))
        else
            null;
        defer if (preprocess_permit) |*permit| permit.deinit();
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, input_ids);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, attention_mask);
        defer attention_mask_tensor.deinit();
        const text_plan = try text_inputs.Plan.init(text_session.inputInfo(), &shape, .{ .attention_heads = text_session.model_contract.text_attention_heads });

        const mask_i32 = try alloc.alloc(i32, total);
        defer alloc.free(mask_i32);
        for (attention_mask, 0..) |value, index| {
            mask_i32[index] = std.math.cast(i32, value) orelse return error.InvalidInputShape;
        }
        if (self.execution_control) |control| try control.check();
        const compact_policy = @import("../architectures/embedding_gemma2.zig").compactRowsEnabled(text_session.backend() == .metal) and
            self.residentTextEncoderEnabled() and self.config.normalize and self.config.pooling == .mean and self.text_projection == null and session_factory.getEmbeddingGemma2Config(self.session) != null;
        var active_rows: usize = 0;
        for (attention_mask) |value| {
            if (compact_policy and value != 0 and value != 1) return error.InvalidAttentionMask;
            active_rows += @intFromBool(value != 0);
        }
        if (compact_policy and (active_rows == 0 or active_rows > 8192)) return error.EmbeddingInputTooLong;
        // The runtime source owns its exact FIFO/model lane and whole-consumer B.
        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        for (0..8) |_| {
            dispatch_started = false; // every retry above is privately proven pre-entry
            if (self.execution_control) |control| try control.check();
            var source: session_mod.PreparedSource = .{};
            var typed_source_ready = false;
            defer if (owned) {
                // Before successful cold producer commitment this synchronous
                // path has acquired NO task/model/provider lane. No foreign
                // numerical consumer has been entered by this function yet.
                if (typed_source_ready) source.deinitWithCleanup(consumer_cleanup) catch platform.inference_process_supervisor.restartWorker() else source.deinitOutsideExecution();
            } else source.deinit();
            var projection_execution: ?session_mod.RunPermit = null;
            defer if (projection_execution) |*permit| permit.deinitWithCleanup(.{ .boundary = source.epoch() });
            const signature = text_plan.signature();
            const task_geometry: ?session_mod.RunPermit.HostOutputGeometry = if (task_output) |task|
                .{ .rows = task.rows, .width = task.width, .held = if (task.producer.*) |*producer| try producer.heldAdmission() else null }
            else
                null;
            self.prepareTextSourceWithTaskOutput(&source, text_session, .{ .host_shapes = signature.slice() }, .{
                .batch = if (compact_policy) 1 else batch,
                .sequence = if (compact_policy) active_rows else seq_len,
                .output_bytes = if (owned)
                    @max(if (compact_policy) batch * self.config.embedding_gemma2_dimensions * @sizeOf(f32) else 0, try session_mod.PreparedSource.hostOutputRequestBytes(batch, output_width))
                else if (compact_policy) batch * self.config.embedding_gemma2_dimensions * @sizeOf(f32) else null,
                .input_bytes = text_plan.input_bytes,
                .host_preprocess_bytes = if (preprocess_permit != null) text_plan.temporary_bytes else try std.math.add(usize, try std.math.mul(usize, total, 32), text_plan.temporary_bytes),
                .workspace_bytes = if (session_factory.getEmbeddingGemma2Config(self.session) != null)
                    try @import("embedding_gemma2.zig").textEncoderWorkspace(if (compact_policy) active_rows else total)
                else
                    0,
            }, task_geometry, owned) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and source.canRetryBeforeExecution()) continue;
                return err;
            };
            if (task_geometry != null and task_geometry.?.held == null) task_output.?.producer.* = try source.takeTaskHostOutputBeforeExecution();
            if (owned) {
                try source.prepareHostOutputsBeforeExecution(batch, output_width);
                typed_source_ready = true;
            }
            if (self.execution_control) |control| try control.check();
            var input_set = try text_plan.materialize(alloc, input_ids_tensor, attention_mask_tensor, .{});
            defer input_set.deinit();
            defer source.finishInputLifetime();
            source.bindInputsBeforeExecution(.{ .host = input_set.slice() }, if (self.usesResidentTextOutputs()) .resident_consumer else .host) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and source.canRetryBeforeExecution()) continue;
                return err;
            };
            var projection_shell = if (self.text_projection) |projection| try projection.prepareResidentRetirement() else null;
            defer if (projection_shell) |*shell| shell.destroyUnused();
            if (self.usesGemmaWorkspaceLaneProtocol() or self.usesNativeWeightLaneProtocol()) try source.bindExecutionLaneBeforeExecution(self.execution_lane, self.interactive);
            try source.enter();
            const qwen_lane = if (self.usesQwenWorkspaceLaneProtocol()) try self.enterNumericalExecutionLane() else null;
            var qwen_lane_live = true;
            defer if (qwen_lane_live) self.leaveExecutionLane(qwen_lane);
            const active_permit = try source.beginExecutionDispatch();
            dispatch_started = true;
            self.execution_gate_held = true;
            defer {
                // Release the late lane BEFORE the selected Model gate. Errors
                // before this defer still use the fallback lane owner above.
                qwen_lane_live = false;
                self.leaveExecutionLane(qwen_lane);
                source.leaveExecution();
                self.execution_gate_held = false;
            }
            var pre_entry_refusal = false;
            const vectors = self.embedPreparedTextInputs(input_set.slice(), mask_i32, input_ids, batch, seq_len, active_permit, &projection_shell, .{ .boundary = source.epoch() }, &projection_execution, .{ .prepared = &source }, &pre_entry_refusal) catch |err| {
                if (err == error.WorkspaceReceiptChangedBeforeExecution and pre_entry_refusal) continue;
                return err;
            };
            if (owned) {
                // Reclaim every temporary row BEFORE source cleanup retires its
                // original run. The returned backing is independently owned.
                defer freeEmbeddingSlices(alloc, vectors);
                return source.publishHostOutputs(vectors);
            }
            return vectors;
        }
        return error.ResourceTemporarilyUnavailable;
    }

    /// Returns backend telemetry for the most recent successful resident
    /// encoder request. Consumers must read this before starting another run.
    pub fn lastResidentBackendTiming(self: *const EmbeddingPipeline) ?ops_mod.BackendDebugTimingSnapshot {
        return self.last_resident_backend_timing;
    }

    fn cudaDocumentBatchingEnabled(self: *const EmbeddingPipeline) bool {
        return self.config.resident_embedding_gemma2 and self.session.backend() == .cuda and
            self.session.executionCapabilities().text_batch_padded_tokens > 0;
    }

    /// Only a refusal before dispatch may be retried with fewer rows. Never
    /// classify a backend/kernel failure as evidence that a smaller run fits.
    fn textAdmissionError(self: *const EmbeddingPipeline, err: anyerror, batch: usize) anyerror {
        if (batch > 1 and self.cudaDocumentBatchingEnabled() and
            (err == error.ResourceLimitExceeded or err == error.ResourceTemporarilyUnavailable or err == error.MemoryBudgetExceeded))
            return error.EmbeddingBatchAdmissionDenied;
        return err;
    }

    fn textSourcePlacement(self: *const EmbeddingPipeline, session: session_mod.Session) session_mod.PreparedSource.Placement {
        if (self.usesResidentTextOutputs()) return .resident_consumer;
        const cfg = session_factory.getGptConfig(session) orelse return .host;
        return if (residentQwen3EmbeddingEligible(session, cfg, self.config)) .resident_transient else .host;
    }

    fn prepareTextSourceWithTaskOutput(self: *EmbeddingPipeline, source: *session_mod.PreparedSource, session: session_mod.Session, shapes: session_mod.ShapeInputs, request: session_mod.RunRequest, task_output: ?session_mod.RunPermit.HostOutputGeometry, comptime owned: bool) !void {
        // Metal prepayment is for the SAME direct preparedTexts constructor
        // below. Other raw resident/projection modes preserve their original
        // owner/dispatch contract; Native's existing prepaid routes are intact.
        const direct_metal = session.backend() == .metal and self.residentTextEncoderEnabled() and self.config.normalize and self.config.pooling == .mean and self.text_projection == null;
        const direct_cuda_owned = (owned or task_output != null) and session.backend() == .cuda and self.gemmaOwnedTextEligible();
        // Only the original protected HOST class becomes allocator capacity;
        // backend scratch/KV/weights and final-bank credit stay independent.
        const workspace = if ((session.backend() == .native or direct_metal or direct_cuda_owned) and !session.preparesInvocation()) self.gemma_workspace else null;
        const cfg = session_factory.getEmbeddingGemma2Config(session);
        if (workspace != null and cfg == null) return error.InvalidAdmissionLeaseReduction;
        if (cfg) |gemma| {
            var planned = request;
            if (session.backend() == .cuda and self.residentTextEncoderEnabled() and self.config.pooling == .mean and self.text_projection == null) {
                if (@import("../execution/architecture_session.zig").gemmaCudaOwnedTextWidth(session)) |width| {
                    if (!@import("../architectures/embedding_gemma2.zig").validDimension(self.config.embedding_gemma2_dimensions) or self.config.embedding_gemma2_dimensions > width) return error.InvalidEmbeddingDimensions;
                    // Original full-width encoder/F32 readback demand remains
                    // independent of the additionally declared task bank.
                    const rows = try std.math.mul(usize, request.batch, width);
                    planned.output_bytes = @max(planned.output_bytes orelse 0, try std.math.mul(usize, rows, @sizeOf(f32)));
                }
            }
            if (workspace != null and session.backend() == .native) {
                // Preserve the Native prepaid encoder floor independently of
                // the final pooled/export backing and original input bytes.
                const rows = std.math.mul(usize, request.batch, request.sequence) catch return error.ResourceLimitExceeded;
                planned.output_bytes = @max(request.output_bytes orelse 0, std.math.mul(usize, rows, gemma.embedding_dim * @sizeOf(f32)) catch return error.ResourceLimitExceeded);
                planned.workspace_bytes = @max(request.workspace_bytes, try @import("embedding_gemma2.zig").textEncoderWorkspace(rows));
            }
            if (workspace != null and session.backend() == .metal) {
                const constructor = try @import("../backends/compute_factory.zig").metalConstructionLayout();
                const metadata = try @import("../backends/admitted_allocator.zig").AdmittedAllocator.constructionHostBytes(constructor.bytes, constructor.alignment);
                planned.host_preprocess_bytes = try std.math.add(usize, planned.host_preprocess_bytes, metadata);
                const rms = try @import("../backends/compute_factory.zig").metalRmsMetadataLayout(try @import("../architectures/embedding_gemma2.zig").textRmsMetadataEntries(gemma.num_hidden_layers));
                // One additional real map allocation, not another RunScope
                // node or a guessed per-layer byte allowance. Same constructor
                // recipe bulk-reserves its registered capacity before math.
                const rms_bytes = try std.math.add(usize, rms.bytes, @import("../backends/admitted_allocator.zig").AdmittedAllocator.allocationOverhead(rms.alignment));
                planned.host_preprocess_bytes = try std.math.add(usize, planned.host_preprocess_bytes, rms_bytes);
                const projections = try @import("../architectures/embedding_gemma2.zig").textProjectionMetadata(gemma);
                const projection_bytes = try @import("../backends/compute_factory.zig").metalProjectionMetadataHostBytes(projections.linear_entries, &projections.zero_widths);
                planned.host_preprocess_bytes = try std.math.add(usize, planned.host_preprocess_bytes, projection_bytes);
                const weights = try @import("../architectures/embedding_gemma2.zig").textWeightMetadata(gemma);
                planned.host_preprocess_bytes = try std.math.add(usize, planned.host_preprocess_bytes, try @import("../backends/compute_factory.zig").metalWeightMetadataHostBytes(weights));
            }
            // One actual selected numerical plane for prepaid and ordinary
            // sources. Compact active rows differ from padded host inputs;
            // their original input/preprocessing byte floors remain intact.
            const token_shape = [_]i64{ @intCast(planned.batch), @intCast(planned.sequence) };
            const numerical_inputs = [_]backends.TensorInfo{
                .{ .name = "input_ids", .dtype = .i64, .shape = &token_shape },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &token_shape },
            };
            return source.initShapesBeforeExecutionWithTaskOutputInto(session, self.session, self.execution_lock, .{ .shapes = &numerical_inputs }, planned, workspace, self.textSourcePlacement(session), self.allocator, self.execution_control, task_output) catch |err| return self.textAdmissionError(err, request.batch);
        }
        return source.initShapesBeforeExecutionWithTaskOutputInto(session, self.session, self.execution_lock, shapes, request, null, self.textSourcePlacement(session), self.allocator, self.execution_control, task_output) catch |err| return self.textAdmissionError(err, request.batch);
    }

    fn embedPreparedTextInputs(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        all_mask: []const i32,
        ids_i64: []const i64,
        batch: usize,
        effective_len: usize,
        run_permit: *session_mod.RunPermit,
        supplied_projection_shell: ?*?@import("../execution/resource_retirement.zig").PreparedCharge,
        cleanup: CleanupContext,
        projection_execution: *?session_mod.RunPermit,
        source_cohort: ?TextSourceCohort,
        pre_entry_refusal: ?*bool,
    ) ![][]f32 {
        if (self.usesResidentTextOutputs() and cleanup.boundary == null) return error.MissingCurrentCleanupEpoch;
        const alloc = self.allocator;
        var own_projection_shell: ?@import("../execution/resource_retirement.zig").PreparedCharge = null;
        defer if (own_projection_shell) |*shell| shell.destroyUnused();
        if (!self.execution_gate_held) {
            if (self.text_projection != null or self.residentTextEncoderEnabled()) try run_permit.prepareResidentOutputRetirement();
            if (self.text_projection) |projection| own_projection_shell = try projection.prepareResidentRetirement();
        } else if (self.text_projection != null or self.residentTextEncoderEnabled()) try run_permit.validateResidentOutputRetirement();
        const projection_shell = supplied_projection_shell orelse &own_projection_shell;
        const lock_started = if (self.trace != null) embedding_trace.now() else 0;
        if (self.execution_control) |control| try control.update(.executing, 0, 0);
        var cohort_waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
        const own_cohort = !self.execution_gate_held;
        if (own_cohort) try self.session.enterExecutionCohort(self.execution_lock, self.execution_control orelse .{}, &cohort_waiter);
        defer if (own_cohort) self.session.leaveExecutionCohort(self.execution_lock, &cohort_waiter);
        const execute_started = if (self.trace != null) embedding_trace.now() else 0;
        if (self.trace) |trace| trace.execution_lock_ns += execute_started -| lock_started;
        defer if (self.trace) |trace| {
            trace.execute_pool_normalize_ns += embedding_trace.now() -| execute_started;
        };
        if (self.execution_control) |control| try control.check();

        if (self.residentTextEncoderEnabled() and (self.session.backend() == .metal or self.session.backend() == .native) and self.config.normalize and self.config.pooling == .mean and self.text_projection == null) {
            if (session_factory.getEmbeddingGemma2Config(self.session)) |cfg| {
                const dimensions = self.config.embedding_gemma2_dimensions;
                if (!@import("../architectures/embedding_gemma2.zig").validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
                var managed = if (cleanup.boundary != null)
                    try compute_provider.acquireWithCleanup(self.session, alloc, self.execution_control, cleanup)
                else
                    try compute_provider.acquire(self.session, alloc, null, self.execution_control);
                defer {
                    if (cleanup.boundary != null) managed.deinitWithCleanup(cleanup) else managed.deinit();
                }
                // This direct encoder consumes the SAME Source dispatch as
                // the permit route below. Borrow its final-address cache loan
                // on the actual fresh Native header for this synchronous call.
                // The callback parks the real Model/lane before cold funding.
                var native_invocation = @import("../execution/invocation_context.zig").Context{
                    .cleanup = cleanup,
                    .native_weights = run_permit.native_weights,
                };
                const native_source = if (comptime build_options.enable_native)
                    managed.backend.vtable == &@import("../ops/native_compute.zig").vtable_impl and run_permit.session.nativePanelPreparationReceiver()
                else
                    false;
                if (native_source) {
                    if (native_invocation.native_weights == null or cleanup.boundary == null) return error.ForeignNativeWeightModel;
                    if (managed.backend.call_context != null) return error.NativeWeightPreparationReentered;
                    managed.backend.call_context = &native_invocation;
                }
                // Clear the actual header before provider destruction; neither
                // returned CPU rows nor a durable backend can retain Source.
                defer if (native_source) {
                    managed.backend.call_context = null;
                };
                const prepaid_source = if (source_cohort) |cohort| switch (cohort) {
                    .prepared => |owner| if (owner.shape_scope) |scope| scope.owner != null else false,
                    .legacy => false,
                } else false;
                if (comptime build_options.enable_metal) {
                    if (managed.backend.kind() == .metal and prepaid_source) {
                        var metadata_cleanup = cleanup;
                        const metadata_epoch = metadata_cleanup.boundary orelse return error.MissingWorkspaceCleanupEpoch;
                        if (!try managed.backend.bindCleanupEpoch(metadata_epoch)) return error.BackendOperationCleanupUnavailable;
                        metadata_cleanup.operation = managed.backend.operationToken();
                        try @import("../ops/metal_compute.zig").MetalCompute.prepareRmsMetadata(&managed.backend, try @import("../architectures/embedding_gemma2.zig").textRmsMetadataEntries(cfg.num_hidden_layers), metadata_cleanup);
                        const projections = try @import("../architectures/embedding_gemma2.zig").textProjectionMetadata(cfg);
                        try @import("../ops/metal_compute.zig").MetalCompute.prepareProjectionMetadata(&managed.backend, .{ .linear_entries = projections.linear_entries, .zero_widths = &projections.zero_widths }, metadata_cleanup);
                        const weights = try @import("../architectures/embedding_gemma2.zig").textWeightMetadata(cfg);
                        try @import("../ops/metal_compute.zig").MetalCompute.prepareWeightMetadata(&managed.backend, .{
                            .entries = weights.entries,
                            .name_bytes = weights.name_bytes,
                            .shape_axes = weights.shape_axes,
                            .vector_entries = weights.vector_entries,
                            .vector_elements = weights.vector_elements,
                            .slice_views = weights.slice_views,
                        }, metadata_cleanup);
                    }
                }
                const mask = try alloc.alloc(i64, all_mask.len);
                defer alloc.free(mask);
                for (mask, all_mask) |*dest, source| dest.* = source;
                const embeddings = if (@import("../architectures/embedding_gemma2.zig").compactRowsEnabled(managed.backend.kind() == .metal) or batch * effective_len <= 8192)
                    try @import("embedding_gemma2.zig").preparedTexts(&managed.backend, alloc, cfg, ids_i64, mask, batch, effective_len, dimensions)
                else blk: {
                    const vectors = try alloc.alloc([]f32, batch);
                    var initialized: usize = 0;
                    errdefer {
                        for (vectors[0..initialized]) |vector| alloc.free(vector);
                        alloc.free(vectors);
                    }
                    for (vectors, 0..) |*vector, index| {
                        if (self.execution_control) |control| try control.check();
                        vector.* = try @import("embedding_gemma2.zig").preparedText(&managed.backend, alloc, cfg, ids_i64[index * effective_len ..][0..effective_len], mask[index * effective_len ..][0..effective_len], dimensions);
                        initialized += 1;
                    }
                    break :blk vectors;
                };
                self.last_resident_backend_timing = managed.backend.debugTimingSnapshot();
                self.recordResidentProjection(.text, .success, "text.embeddinggemma2.pool_normalize.resident", batch, null);
                return embeddings;
            }
        }

        if (self.text_projection) |proj| {
            if (try self.tryEmbedTextResidentProjection(
                inputs,
                all_mask,
                batch,
                effective_len,
                proj,
                run_permit,
                projection_shell,
                cleanup,
                projection_execution,
                source_cohort,
            )) |resident_embeddings| {
                return resident_embeddings;
            }
        } else if (self.residentTextEncoderEnabled()) {
            if (try self.tryEmbedTextResidentEncoder(inputs, all_mask, batch, effective_len, run_permit, cleanup)) |resident_embeddings| {
                return resident_embeddings;
            }
        } else if (try self.tryEmbedTextResidentQwen3(all_mask, ids_i64, batch, effective_len, cleanup, if (source_cohort) |selected| switch (selected) {
            .prepared => |owner| try owner.existingWorkspaceReceipt(),
            .legacy => null,
        } else null, pre_entry_refusal)) |resident_embeddings| {
            return resident_embeddings;
        }

        // Run inference
        const encoder_start = embedTimingStart(self.print_timing);
        var outputs = try run_permit.runWithControl(inputs, alloc, self.execution_control);
        logEmbedTiming("text.encoder", batch, encoder_start);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        // Host execution has also completed: source prepared state and its
        // actual lane end before any downstream supplier/admission callback.
        if (self.text_projection != null) {
            run_permit.retireExecutionWithCleanup(cleanup);
            try self.releaseTextSourceCohort(source_cohort);
            outputs = if (source_cohort) |cohort| switch (cohort) {
                .prepared => |owner| try owner.attachSynchronousHost(outputs),
                .legacy => try run_permit.attachHostOutputs(outputs, alloc),
            } else try run_permit.attachHostOutputs(outputs, alloc);
            if (source_cohort == null or std.meta.activeTag(source_cohort.?) == .legacy) run_permit.deinitWithCleanup(cleanup);
        }
        return self.finishTextHostOutputs(outputs, all_mask, batch, effective_len);
    }

    /// Host rows are already owned by this allocator. Preserve full-width
    /// values, including each backend's original source normalization policy.
    /// A reduced prefix gets only the pipeline/model normalization it requests.
    fn finishGemmaTextDimensions(self: *EmbeddingPipeline, rows: [][]f32) !void {
        if (self.text_projection != null) return;
        const cfg = session_factory.getEmbeddingGemma2Config(self.session) orelse return;
        const width = self.config.embedding_gemma2_dimensions;
        if (!@import("../architectures/embedding_gemma2.zig").validDimension(width) or width > cfg.embedding_dim) return error.InvalidEmbeddingDimensions;
        for (rows) |*row| {
            if (row.len == width) continue; // Already selected/truncated: never normalize twice.
            if (row.len < width) return error.UnexpectedOutputShape;
            const prefix = try self.allocator.dupe(f32, row.*[0..width]);
            if (self.config.normalize or self.config.resident_embedding_gemma2) linalg.l2Normalize(prefix, width);
            self.allocator.free(row.*);
            row.* = prefix;
        }
    }

    fn finishTextHostOutputs(self: *EmbeddingPipeline, outputs: []Tensor, all_mask: []const i32, batch: usize, effective_len: usize) ![][]f32 {
        const alloc = self.allocator;
        if (outputs.len == 0) return error.NoOutputTensors;

        if (self.text_projection) |proj| {
            if (projectionInputDim(proj)) |expected_dim| {
                if (selectProjectedOutput(outputs, expected_dim, batch)) |selection| {
                    logEmbedSelectedTensor(self.print_timing, "text.project_input", selection.tensor);
                    const projected_embeddings = if (selection.use_cls_pool)
                        try self.pool3D(selection.tensor, all_mask, batch, effective_len, false)
                    else
                        try self.extract2D(selection.tensor, batch, false);
                    errdefer freeEmbeddingSlices(alloc, projected_embeddings);
                    try self.projectEmbeddings(projected_embeddings, proj);
                    return projected_embeddings;
                }
            }
        }

        // Get the first output tensor (last_hidden_state for encoder models)
        const output = &outputs[0];
        const output_shape = output.shape;

        const embeddings = switch (output_shape.len) {
            3 => try self.pool3D(output, all_mask, batch, effective_len, self.text_projection == null),
            2 => if (output_shape[0] == (try std.math.mul(usize, batch, effective_len)) and effective_len > 1) blk: {
                // Imported graphs may flatten token rows. They still require
                // the requested pooling, rather than silently taking row zero.
                var token_output = output.borrowedView(output.name);
                const token_shape = [_]i64{ @intCast(batch), @intCast(effective_len), output_shape[1] };
                token_output.shape = &token_shape;
                break :blk try self.pool3D(&token_output, all_mask, batch, effective_len, self.text_projection == null);
            } else try self.extract2D(output, batch, self.text_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, embeddings);

        if (self.text_projection) |proj| {
            try self.projectEmbeddings(embeddings, proj);
        }

        try self.finishGemmaTextDimensions(embeddings);
        if (self.config.resident_embedding_gemma2)
            for (embeddings) |embedding| try validateEmbeddingGemma2Vector(embedding);

        return embeddings;
    }

    /// Metadata-only source selection shared by cold physical custody owners.
    pub fn textEncodingSession(self: *const EmbeddingPipeline) backends.Session {
        if (self.vision_session) |vs| {
            if (session_factory.getClapConfig(self.session) != null and
                session_factory.getClipConfig(vs) != null and
                sessionHasInput(vs, "input_ids"))
            {
                return vs;
            }
        }
        return self.session;
    }

    fn embedWithBatchPlan(
        self: *EmbeddingPipeline,
        texts: []const []const u8,
        plan: TextSessionBatchPlan,
    ) anyerror![][]f32 {
        return self.embedWithBatchPlanWithTaskOutput(texts, plan, null);
    }
    fn embedWithBatchPlanWithTaskOutput(
        self: *EmbeddingPipeline,
        texts: []const []const u8,
        plan: TextSessionBatchPlan,
        task_output: ?*GemmaTaskHostOutput,
    ) anyerror![][]f32 {
        if (plan.batch_size == 0) return error.InvalidInputShape;
        const embeddings = try self.allocator.alloc([]f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |embedding| self.allocator.free(embedding);
            self.allocator.free(embeddings);
        }

        var offset: usize = 0;
        while (offset < texts.len) {
            if (self.execution_control) |control| try control.check();
            const real_count = @min(plan.batch_size, texts.len - offset);
            if (real_count != plan.batch_size and !plan.pad_final_batch) return error.InvalidInputShape;
            const batch_embeddings = try self.embedDirectResult(
                texts[offset .. offset + real_count],
                plan.batch_size,
                false,
                .{},
                task_output,
            );
            if (batch_embeddings.len != plan.batch_size) {
                freeEmbeddingSlices(self.allocator, batch_embeddings);
                return error.UnexpectedOutputShape;
            }

            for (batch_embeddings[0..real_count], 0..) |embedding, index| {
                embeddings[offset + index] = embedding;
            }
            for (batch_embeddings[real_count..]) |embedding| self.allocator.free(embedding);
            self.allocator.free(batch_embeddings);
            initialized += real_count;
            offset += real_count;
        }

        return embeddings;
    }

    fn activeTokenLength(mask: []const i32) usize {
        var last_active: usize = 0;
        var found = false;
        for (mask, 0..) |value, idx| {
            if (value > 0) {
                last_active = idx;
                found = true;
            }
        }
        return if (found) last_active + 1 else 1;
    }

    /// Guarantee the encoded sequence's last active token is `eos_id`.
    /// Appends into padding when room exists; when the sequence fills its
    /// buffer, the final token is overwritten (HF truncates the sequence
    /// before the post-processor appends EOS, so EOS always survives).
    /// Idempotent when the tokenizer already appended EOS.
    fn ensureTrailingEos(encoded: *EncodeResult, eos_id: i32) void {
        const mask = encoded.attention_mask;
        if (mask.len == 0) return;
        var last_active: ?usize = null;
        for (mask, 0..) |value, idx| {
            if (value > 0) last_active = idx;
        }
        if (last_active) |last| {
            if (encoded.ids[last] == eos_id) return;
            if (last + 1 < mask.len) {
                encoded.ids[last + 1] = eos_id;
                encoded.attention_mask[last + 1] = 1;
            } else {
                encoded.ids[last] = eos_id;
            }
        } else {
            // Empty input: the embedding of an empty string is the EOS row.
            encoded.ids[0] = eos_id;
            encoded.attention_mask[0] = 1;
        }
    }

    /// Pool 3D output [batch, seq, hidden] -> [batch][hidden]
    fn pool3D(self: *EmbeddingPipeline, output: *const Tensor, mask: []const i32, batch: usize, seq_len: usize, normalize: bool) ![][]f32 {
        const hidden: usize = @intCast(output.shape[2]);
        const data = output.asFloat32();

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            @memset(emb, 0.0);

            switch (self.config.pooling) {
                .mean => {
                    var count: f32 = 0.0;
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) {
                            const offset = (b * seq_len + s) * hidden;
                            for (0..hidden) |h| {
                                emb[h] += data[offset + h];
                            }
                            count += 1.0;
                        }
                    }
                    if (count > 0.0) {
                        for (0..hidden) |h| emb[h] /= count;
                    }
                },
                .cls => {
                    const offset = b * seq_len * hidden;
                    @memcpy(emb, data[offset .. offset + hidden]);
                },
                .max => {
                    @memset(emb, -std.math.inf(f32));
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) {
                            const offset = (b * seq_len + s) * hidden;
                            for (0..hidden) |h| {
                                if (data[offset + h] > emb[h]) emb[h] = data[offset + h];
                            }
                        }
                    }
                },
                .last => {
                    var last_idx: usize = 0;
                    for (0..seq_len) |s| {
                        if (mask[b * seq_len + s] > 0) last_idx = s;
                    }
                    const offset = (b * seq_len + last_idx) * hidden;
                    @memcpy(emb, data[offset .. offset + hidden]);
                },
            }

            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }

            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }

    /// Extract 2D output [batch, hidden] -> [batch][hidden]
    fn extract2D(self: *EmbeddingPipeline, output: *const Tensor, batch: usize, normalize: bool) ![][]f32 {
        if (output.shape.len != 2 or output.shape[1] <= 0) return error.UnexpectedOutputShape;
        const hidden: usize = @intCast(output.shape[1]);
        const data = output.asFloat32();
        const rows = if (hidden == 0) 0 else data.len / hidden;
        if (hidden == 0 or rows < batch or rows * hidden != data.len) {
            std.log.warn("extract2D shape/data mismatch shape={any} batch={d} hidden={d} rows={d} data_len={d}", .{
                output.shape,
                batch,
                hidden,
                rows,
                data.len,
            });
            return error.ShapeMismatch;
        }

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            @memcpy(emb, data[b * hidden .. (b + 1) * hidden]);

            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }

            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }

    /// Embed a batch of images (raw JPEG/PNG bytes), returning [batch][projection_dim] embeddings.
    /// Requires a vision_session (CLIP/SigLIP model).
    pub fn embedImages(self: *EmbeddingPipeline, images: []const []const u8) anyerror![][]f32 {
        if (images.len > 0 and !self.config.allow_image) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        return (try self.embedImagesReported(images)).vectors;
    }

    pub const ImageBatchResult = struct {
        vectors: [][]f32,
        execution: @import("batch_execution.zig").Execution,
    };

    pub fn embedImagesReported(self: *EmbeddingPipeline, images: []const []const u8) anyerror!ImageBatchResult {
        if (images.len > 0 and !self.config.allow_image) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            const result = try @import("embedding_gemma2.zig").embedImages(self.allocator, self.session, self.tok, images, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .dimensions = self.config.embedding_gemma2_dimensions }), self.execution_lock, self.execution_control, null);
            return .{ .vectors = result.vectors, .execution = result.execution };
        }
        if (images.len == 0) return .{ .vectors = try self.allocator.alloc([]f32, 0), .execution = .serial };
        if (self.config.resident_embedding_gemma2) {
            const parts = try self.allocator.alloc(EmbeddingContentPart, images.len);
            defer self.allocator.free(parts);
            for (images, 0..) |bytes, index| parts[index] = .{ .image = bytes };
            return .{ .vectors = try self.embedSeparateParts(parts), .execution = .serial };
        }
        // The batch primitive acquires the execution gate only after bounded
        // preprocessing. Fallback reuses that primitive for each item, so
        // taking the gate here would both serialize preprocessing and attempt
        // to re-enter the non-reentrant backend lock.
        const vectors = self.embedImagesBatch(images) catch |err| {
            if (images.len > 1 and shouldFallbackBatchedImageError(err)) {
                return .{ .vectors = try self.embedImagesIndividually(images), .execution = .fallback };
            }
            return err;
        };
        return .{ .vectors = vectors, .execution = if (images.len > 1) .native_batch else .serial };
    }

    fn imageWorkControl(self: *EmbeddingPipeline) antfly_image.work_control.Control {
        return if (self.execution_control) |*control| control.imageWorkControl() else .{};
    }

    /// Native broker entry point: invalid media occupies an indexed error slot
    /// and never enters the encoder. Healthy rows retain their original order.
    pub fn embedImagesIndexed(self: *EmbeddingPipeline, images: []const []const u8, errors: []?anyerror) !ImageBatchResult {
        if (images.len > 0 and !self.config.allow_image) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (errors.len != images.len) return error.InvalidInputShape;
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            const result = try @import("embedding_gemma2.zig").embedImages(self.allocator, self.session, self.tok, images, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .dimensions = self.config.embedding_gemma2_dimensions }), self.execution_lock, self.execution_control, errors);
            return .{ .vectors = result.vectors, .execution = result.execution };
        }
        if (images.len == 0) return .{ .vectors = try self.allocator.alloc([]f32, 0), .execution = .serial };
        if (self.config.resident_embedding_gemma2) {
            const parts = try self.allocator.alloc(EmbeddingContentPart, images.len);
            defer self.allocator.free(parts);
            for (images, 0..) |bytes, index| parts[index] = .{ .image = bytes };
            return .{ .vectors = try self.embedSeparatePartsIndexed(parts, errors), .execution = .serial };
        }
        const vectors = self.embedImagesBatchIndexed(images, errors) catch |err| {
            if (images.len <= 1 or !shouldFallbackBatchedImageError(err)) return err;
            const outputs = try self.allocator.alloc([]f32, images.len);
            var initialized: usize = 0;
            errdefer {
                for (outputs[0..initialized]) |vector| self.allocator.free(vector);
                self.allocator.free(outputs);
            }
            for (images, 0..) |_, i| {
                const single = try self.embedImagesBatchIndexed(images[i..][0..1], errors[i..][0..1]);
                outputs[i] = single[0];
                self.allocator.free(single);
                initialized += 1;
            }
            return .{ .vectors = outputs, .execution = .fallback };
        };
        var valid: usize = 0;
        for (errors) |err| if (err == null) {
            valid += 1;
        };
        return .{ .vectors = vectors, .execution = if (valid > 1) .native_batch else .serial };
    }

    /// Size an image wave before allocating normalized tensors. Permanent
    /// session limits are monotone in this shape; current pressure is left to
    /// the real reservation, never converted into singleton retry storms.
    pub fn imageBatchPrefix(self: *EmbeddingPipeline, media_bytes: []const usize) !usize {
        const vs = self.vision_session orelse self.session;
        const row_bytes = try std.math.mul(usize, try std.math.mul(usize, self.config.image_size, self.config.image_size), 3 * @sizeOf(f32));
        var retained: usize = 0;
        for (media_bytes, 0..) |bytes, i| {
            retained = try std.math.add(usize, retained, bytes);
            if (!try vs.fitsRun(.{ .batch = i + 1, .input_bytes = try std.math.mul(usize, i + 1, row_bytes), .host_preprocess_bytes = retained })) return i;
            if (self.visual_projection) |projection| {
                if (projectionInputDim(projection)) |dim| {
                    const packed_bytes = try std.math.mul(usize, try std.math.mul(usize, i + 1, dim), @sizeOf(f32));
                    if (!try projection.fitsRun(.{ .batch = i + 1, .sequence = 1, .input_bytes = packed_bytes, .host_preprocess_bytes = packed_bytes })) return i;
                }
            }
        }
        return media_bytes.len;
    }

    fn embedImagesBatch(self: *EmbeddingPipeline, images: []const []const u8) anyerror![][]f32 {
        return self.embedImagesBatchIndexed(images, null);
    }

    fn embedImagesBatchIndexed(self: *EmbeddingPipeline, images: []const []const u8, item_errors: ?[]?anyerror) anyerror![][]f32 {
        if (self.execution_control) |control| try control.check();
        const vs = self.vision_session orelse if (sessionHasInput(self.session, "pixel_values")) self.session else return error.NoVisionSession;

        const alloc = self.allocator;
        const img_size = self.config.image_size;
        const batch = images.len;
        const pixel_elements = std.math.mul(
            usize,
            std.math.mul(
                usize,
                std.math.mul(usize, batch, 3) catch
                    return error.ResourceLimitExceeded,
                img_size,
            ) catch return error.ResourceLimitExceeded,
            img_size,
        ) catch return error.ResourceLimitExceeded;
        const pixel_bytes = std.math.mul(usize, pixel_elements, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        var encoded_bytes: usize = 0;
        for (images) |encoded| {
            encoded_bytes = std.math.add(usize, encoded_bytes, encoded.len) catch
                return error.ResourceLimitExceeded;
        }
        var scratch_admission = PreprocessScratchAdmission{ .allocator = alloc, .session = vs };
        defer scratch_admission.deinit();
        const preprocess_options = image.BatchPreprocessOptions{
            .io = self.config.preprocess_io,
            .scratch_admission = scratch_admission.descriptor(),
            .control = self.imageWorkControl(),
            .item_errors = item_errors,
        };
        const preprocess_resident_bytes = std.math.add(usize, encoded_bytes, pixel_bytes) catch
            return error.ResourceLimitExceeded;
        // Caller-retained media and the normalized tensor have a stable lease.
        // Codec slabs acquire exact, independently releasable deltas as they grow.
        var preprocess_permit = try vs.admitHostPreprocess(preprocess_resident_bytes);
        defer preprocess_permit.deinit();

        // Preprocess all images to [batch, 3, H, W]
        const preprocess_start = embedTimingStart(self.print_timing);
        const pixel_values = try alloc.alloc(f32, pixel_elements);
        var pixel_values_owned = true;
        defer if (pixel_values_owned) alloc.free(pixel_values);
        switch (self.config.image_preprocess_profile) {
            .default => try image.preprocessBatchIntoBounded(
                pixel_values,
                images,
                img_size,
                image.IMAGENET_MEAN,
                image.IMAGENET_STD,
                .bilinear,
                preprocess_options,
            ),
            .clip => try image.preprocessClipBatchIntoBounded(
                pixel_values,
                images,
                img_size,
                image.IMAGENET_MEAN,
                image.IMAGENET_STD,
                preprocess_options,
            ),
        }
        logEmbedTiming("image.preprocess", batch, preprocess_start);
        scratch_admission.deinit();

        var valid_count = batch;
        if (item_errors) |errors| {
            valid_count = 0;
            const row_len = pixel_elements / batch;
            for (errors, 0..) |err, index| {
                if (err != null) continue;
                if (valid_count != index) std.mem.copyForwards(f32, pixel_values[valid_count * row_len ..][0..row_len], pixel_values[index * row_len ..][0..row_len]);
                valid_count += 1;
            }
            if (valid_count == 0) {
                const empty = try alloc.alloc([]f32, batch);
                @memset(empty, &.{});
                return empty;
            }
        }

        // Build input tensor
        const sz: i64 = @intCast(img_size);
        const pv_shape = [_]i64{ @intCast(valid_count), 3, sz, sz };
        var pv_tensor = try Tensor.initFloat32Owned(alloc, "pixel_values", &pv_shape, pixel_values);
        pixel_values_owned = false;
        defer pv_tensor.deinit();
        var input_tensor = pv_tensor.borrowedView("pixel_values");
        input_tensor.data = pv_tensor.data[0 .. valid_count * 3 * img_size * img_size * @sizeOf(f32)];

        const retained_host_bytes = std.math.add(usize, encoded_bytes, pixel_bytes) catch
            return error.ResourceLimitExceeded;
        // Codec scratch is gone once preprocessing returns. Retain only the
        // encoded sources and adopted normalized tensor while queued for this
        // session; the run request below credits exactly these still-live
        // bytes and reserves the remaining host/backend peak.
        try preprocess_permit.retainHostBytes(retained_host_bytes);

        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        var source_stage: session_mod.PreparedSource = .{};
        defer source_stage.deinit();
        try source_stage.initBeforeExecutionInto(vs, self.session, self.execution_lock, .{ .host = &.{input_tensor} }, .{
            .batch = valid_count,
            .sequence = 1,
            .input_bytes = pixel_bytes,
            .host_preprocess_bytes = encoded_bytes,
            .pre_admitted_host_bytes = retained_host_bytes,
        }, null, if (self.visual_projection != null) .resident_consumer else .host, alloc, self.execution_control);
        try source_stage.enter();

        if (self.visual_projection) |proj| {
            const resident = try self.tryEmbedResidentProjection(
                &.{input_tensor},
                proj,
                .image,
                valid_count,
                "image.encoder.resident",
                "image.projection.resident",
                &source_stage,
            );
            if (resident) |embeddings| return self.expandIndexedImageVectors(embeddings, item_errors);
        }

        // Run vision encoder
        const encoder_start = embedTimingStart(self.print_timing);
        const outputs = try source_stage.runHost(&.{input_tensor});
        logEmbedTiming("image.encoder", batch, encoder_start);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        const embeddings = self.imageEmbeddingsFromOutputs(outputs, valid_count) catch |err| {
            return err;
        };
        return self.expandIndexedImageVectors(embeddings, item_errors);
    }

    fn expandIndexedImageVectors(self: *EmbeddingPipeline, vectors: [][]f32, errors: ?[]?anyerror) ![][]f32 {
        const indexed = errors orelse return vectors;
        errdefer freeEmbeddingSlices(self.allocator, vectors);
        const expanded = try self.allocator.alloc([]f32, indexed.len);
        var cursor: usize = 0;
        for (indexed, expanded) |err, *output| {
            if (err != null) {
                output.* = &.{};
                continue;
            }
            output.* = vectors[cursor];
            cursor += 1;
        }
        self.allocator.free(vectors);
        return expanded;
    }

    /// Embed renderer-owned decoded rasters without an encode/decode round
    /// trip. Buffers remain borrowed for this synchronous call and are copied
    /// only into the model's normalized f32 input tensor.
    pub fn embedBorrowedRasters(
        self: *EmbeddingPipeline,
        rasters: []const antfly_image.BorrowedRasterAttachment,
    ) anyerror![][]f32 {
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        return (try self.embedBorrowedRastersReported(rasters)).vectors;
    }

    pub fn embedBorrowedRastersReported(self: *EmbeddingPipeline, rasters: []const antfly_image.BorrowedRasterAttachment) anyerror!ImageBatchResult {
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (rasters.len == 0) return .{ .vectors = try self.allocator.alloc([]f32, 0), .execution = .serial };
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            const result = try @import("embedding_gemma2.zig").embedRasters(self.allocator, self.session, self.tok, rasters, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .dimensions = self.config.embedding_gemma2_dimensions }), self.execution_lock, self.execution_control);
            return .{ .vectors = result.vectors, .execution = result.execution };
        }
        const vectors = self.embedBorrowedRastersBatch(rasters) catch |err| {
            if (rasters.len > 1 and shouldFallbackBatchedImageError(err)) {
                const embeddings = try self.allocator.alloc([]f32, rasters.len);
                var initialized: usize = 0;
                errdefer {
                    for (embeddings[0..initialized]) |embedding| self.allocator.free(embedding);
                    self.allocator.free(embeddings);
                }
                for (rasters, 0..) |_, index| {
                    const single = try self.embedBorrowedRastersBatch(rasters[index .. index + 1]);
                    defer self.allocator.free(single);
                    embeddings[index] = single[0];
                    initialized += 1;
                }
                return .{ .vectors = embeddings, .execution = .fallback };
            }
            return err;
        };
        return .{ .vectors = vectors, .execution = if (rasters.len > 1) .native_batch else .serial };
    }

    fn embedBorrowedRastersBatch(
        self: *EmbeddingPipeline,
        rasters: []const antfly_image.BorrowedRasterAttachment,
    ) anyerror![][]f32 {
        if (self.execution_control) |control| try control.check();
        const vs = self.vision_session orelse if (sessionHasInput(self.session, "pixel_values")) self.session else return error.NoVisionSession;
        const alloc = self.allocator;
        const img_size = self.config.image_size;
        const batch = rasters.len;
        const pixel_elements = std.math.mul(
            usize,
            std.math.mul(
                usize,
                std.math.mul(usize, batch, 3) catch return error.ResourceLimitExceeded,
                img_size,
            ) catch return error.ResourceLimitExceeded,
            img_size,
        ) catch return error.ResourceLimitExceeded;
        const pixel_bytes = std.math.mul(usize, pixel_elements, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        var raster_bytes: usize = 0;
        for (rasters) |raster| {
            try raster.validate();
            raster_bytes = std.math.add(usize, raster_bytes, raster.bytes.len) catch
                return error.ResourceLimitExceeded;
        }
        var scratch_admission = PreprocessScratchAdmission{ .allocator = alloc, .session = vs };
        defer scratch_admission.deinit();
        const preprocess_options = image.BatchPreprocessOptions{
            .io = self.config.preprocess_io,
            .scratch_admission = scratch_admission.descriptor(),
            .control = self.imageWorkControl(),
        };
        const preprocess_resident_bytes = std.math.add(usize, raster_bytes, pixel_bytes) catch
            return error.ResourceLimitExceeded;
        var preprocess_permit = try vs.admitHostPreprocess(preprocess_resident_bytes);
        defer preprocess_permit.deinit();

        const pixel_values = try alloc.alloc(f32, pixel_elements);
        var pixel_values_owned = true;
        defer if (pixel_values_owned) alloc.free(pixel_values);
        const preprocess_start = embedTimingStart(self.print_timing);
        switch (self.config.image_preprocess_profile) {
            .default => try image.preprocessBorrowedRasterBatchIntoWithOptions(
                alloc,
                pixel_values,
                rasters,
                img_size,
                image.IMAGENET_MEAN,
                image.IMAGENET_STD,
                .bilinear,
                preprocess_options,
            ),
            .clip => try image.preprocessClipBorrowedRasterBatchIntoWithOptions(
                alloc,
                pixel_values,
                rasters,
                img_size,
                image.IMAGENET_MEAN,
                image.IMAGENET_STD,
                preprocess_options,
            ),
        }
        logEmbedTiming("image.preprocess.raster", batch, preprocess_start);
        scratch_admission.deinit();

        const sz: i64 = @intCast(img_size);
        const pv_shape = [_]i64{ @intCast(batch), 3, sz, sz };
        var pv_tensor = try Tensor.initFloat32Owned(alloc, "pixel_values", &pv_shape, pixel_values);
        pixel_values_owned = false;
        defer pv_tensor.deinit();

        const retained_host_bytes = std.math.add(usize, raster_bytes, pixel_bytes) catch
            return error.ResourceLimitExceeded;
        try preprocess_permit.retainHostBytes(retained_host_bytes);

        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        var source_stage: session_mod.PreparedSource = .{};
        defer source_stage.deinit();
        try source_stage.initBeforeExecutionInto(vs, self.session, self.execution_lock, .{ .host = &.{pv_tensor} }, .{
            .batch = batch,
            .sequence = 1,
            .input_bytes = pixel_bytes,
            .host_preprocess_bytes = raster_bytes,
            .pre_admitted_host_bytes = retained_host_bytes,
        }, null, if (self.visual_projection != null) .resident_consumer else .host, alloc, self.execution_control);
        try source_stage.enter();

        if (self.visual_projection) |proj| {
            const resident = try self.tryEmbedResidentProjection(
                &.{pv_tensor},
                proj,
                .image,
                batch,
                "image.encoder.resident",
                "image.projection.resident",
                &source_stage,
            );
            if (resident) |embeddings| return embeddings;
        }

        const encoder_start = embedTimingStart(self.print_timing);
        const outputs = try source_stage.runHost(&.{pv_tensor});
        logEmbedTiming("image.encoder", batch, encoder_start);
        defer {
            for (outputs) |*output| output.deinit();
            alloc.free(outputs);
        }
        return try self.imageEmbeddingsFromOutputs(outputs, batch);
    }

    fn imageEmbeddingsFromOutputs(self: *EmbeddingPipeline, outputs: []Tensor, batch: usize) ![][]f32 {
        const alloc = self.allocator;
        if (outputs.len == 0) return error.NoOutputTensors;
        logEmbedTensorShapes(self.print_timing, "image.outputs", outputs);

        if (self.visual_projection) |proj| {
            if (projectionInputDim(proj)) |expected_dim| {
                if (selectProjectedOutput(outputs, expected_dim, batch)) |selection| {
                    logEmbedSelectedTensor(self.print_timing, "image.project_input", selection.tensor);
                    const projected_embeddings = if (selection.use_cls_pool)
                        try self.extractVision3D(selection.tensor, batch, false)
                    else
                        try self.extract2D(selection.tensor, batch, false);
                    errdefer freeEmbeddingSlices(alloc, projected_embeddings);
                    try self.projectEmbeddings(projected_embeddings, proj);
                    return projected_embeddings;
                }
            }
        }

        if (!tensorHasBatchRows(&outputs[0], batch)) return error.ShapeMismatch;
        const extracted = switch (outputs[0].shape.len) {
            3 => try self.extractVision3D(&outputs[0], batch, self.visual_projection == null),
            2 => try self.extract2D(&outputs[0], batch, self.visual_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, extracted);

        if (self.visual_projection) |proj| {
            try self.projectEmbeddings(extracted, proj);
        }

        return extracted;
    }

    fn embedImagesIndividually(self: *EmbeddingPipeline, images: []const []const u8) anyerror![][]f32 {
        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, images.len);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |emb| alloc.free(emb);
            alloc.free(embeddings);
        }
        for (images, 0..) |img, i| {
            const single = try self.embedImagesBatch(&.{img});
            defer alloc.free(single);
            embeddings[i] = single[0];
            initialized += 1;
        }
        return embeddings;
    }

    /// Embed a batch of supported encoded audio clips, returning [batch][embed_dim] embeddings.
    /// Requires an audio_session (CLAP model).
    pub fn embedAudio(self: *EmbeddingPipeline, audio_clips: []const []const u8) ![][]f32 {
        if (audio_clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            const clips = try self.allocator.alloc(EncodedAudioClip, audio_clips.len);
            defer self.allocator.free(clips);
            for (clips, audio_clips) |*clip, bytes| clip.* = .{ .bytes = bytes };
            return self.embedEncodedAudio(clips);
        }
        if (audio_clips.len > 0 and !self.config.resident_embedding_gemma2) {
            const audio_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
            const feature_plan = try clapBatchPlan(audio_session, audio_clips.len);
            try ensureClapWorkingSet(0, feature_plan.feature_reserve_bytes, self.config.max_audio_decode_working_bytes);
        }
        const alloc = self.allocator;
        const clips = try alloc.alloc(EncodedAudioClip, audio_clips.len);
        defer alloc.free(clips);
        for (audio_clips, 0..) |clip_bytes, i| {
            clips[i] = .{ .bytes = clip_bytes };
        }
        return self.embedEncodedAudio(clips);
    }

    /// Embed a batch of supported encoded audio clips, allowing per-clip
    /// decode hints such as MIME type when byte sniffing is ambiguous.
    pub fn embedEncodedAudio(
        self: *EmbeddingPipeline,
        audio_clips: []const EncodedAudioClip,
    ) ![][]f32 {
        if (audio_clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            if (self.config.gemma4_audio_batching) {
                const grouped = @import("embedding_gemma2.zig");
                const result = try grouped.embedAudios(self.allocator, self.session, self.tok, audio_clips, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes, .dimensions = self.config.embedding_gemma2_dimensions, .prepare_ahead = self.config.audio_prepare_ahead, .timings = self.audio_timings }), self.execution_lock, self.execution_control, null);
                self.allocator.free(result.input_tokens);
                return result.vectors;
            }
            return self.embedGemma2AudioSerial(audio_clips, null);
        }
        if (audio_clips.len == 0) return try self.allocator.alloc([]f32, 0);
        if (self.config.resident_embedding_gemma2) {
            const parts = try self.allocator.alloc(EmbeddingContentPart, audio_clips.len);
            defer self.allocator.free(parts);
            for (audio_clips, 0..) |clip, index| parts[index] = .{ .audio = clip };
            return self.embedSeparateParts(parts);
        }
        const prepared_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        if (self.config.audio_prepared_batches and (prepared_session.backend() == .metal or prepared_session.backend() == .cuda)) return (try self.embedClapAudioPrepared(audio_clips, null)).vectors;

        const audio_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        const feature_plan = try clapBatchPlan(audio_session, audio_clips.len);
        try ensureClapWorkingSet(0, feature_plan.feature_reserve_bytes, self.config.max_audio_decode_working_bytes);

        const alloc = self.allocator;
        const decoded = try alloc.alloc(audio.Audio, audio_clips.len);
        var initialized: usize = 0;
        defer {
            for (decoded[0..initialized]) |*clip| clip.deinit();
            alloc.free(decoded);
        }

        var pcm_inputs = try alloc.alloc(audio.PcmAudio, audio_clips.len);
        defer alloc.free(pcm_inputs);

        const decode_start = embedTimingStart(self.print_timing);
        var remaining_decode_bytes = self.config.max_audio_decode_working_bytes - feature_plan.feature_reserve_bytes;
        for (audio_clips, 0..) |clip, i| {
            decoded[i] = try audio.decodeBounded(
                alloc,
                clip.bytes,
                clip.decode_options,
                remaining_decode_bytes,
            );
            initialized += 1;
            const retained_bytes = std.math.mul(usize, decoded[i].samples.len, @sizeOf(f32)) catch
                return error.AudioTooLarge;
            if (retained_bytes > remaining_decode_bytes) return error.AudioTooLarge;
            remaining_decode_bytes -= retained_bytes;
            pcm_inputs[i] = .{
                .samples = decoded[i].samples,
                .sample_rate = decoded[i].sample_rate,
            };
        }
        logEmbedTiming("audio.decode", audio_clips.len, decode_start);

        return self.embedAudioPcm(pcm_inputs);
    }

    /// Cross-caller entry point. Decode failures stay indexed; retained PCM and
    /// feature scratch share one budget. Flush a window before decoding the
    /// next clip when it does not fit, without retrying a failed model forward.
    pub fn embedEncodedAudioIndexed(self: *EmbeddingPipeline, clips: []const EncodedAudioClip, errors: []?anyerror) !ImageBatchResult {
        if (clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (errors.len != clips.len) return error.InvalidInputShape;
        if (self.config.resident_embedding_gemma2) {
            const parts = try self.allocator.alloc(EmbeddingContentPart, clips.len);
            defer self.allocator.free(parts);
            for (clips, 0..) |clip, index| parts[index] = .{ .audio = clip };
            return .{ .vectors = try self.embedSeparatePartsIndexed(parts, errors), .execution = .serial };
        }
        if (clips.len != errors.len) return error.InvalidInputShape;
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) {
            if (self.config.gemma4_audio_batching) {
                const result = try @import("embedding_gemma2.zig").embedAudios(self.allocator, self.session, self.tok, clips, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes, .dimensions = self.config.embedding_gemma2_dimensions, .prepare_ahead = self.config.audio_prepare_ahead, .timings = self.audio_timings }), self.execution_lock, self.execution_control, errors);
                self.allocator.free(result.input_tokens);
                return .{ .vectors = result.vectors, .execution = result.execution };
            }
            return .{ .vectors = try self.embedGemma2AudioSerial(clips, errors), .execution = .serial };
        }
        @memset(errors, null);
        const prepared_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        if (self.config.audio_prepared_batches and (prepared_session.backend() == .metal or prepared_session.backend() == .cuda)) return self.embedClapAudioPrepared(clips, errors);
        const alloc = self.allocator;
        const vectors = try alloc.alloc([]f32, clips.len);
        @memset(vectors, &.{});
        errdefer {
            for (vectors) |row| alloc.free(row);
            alloc.free(vectors);
        }
        const session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        const decoded = try alloc.alloc(audio.Audio, clips.len);
        defer alloc.free(decoded);
        var retained: usize = 0;
        defer for (decoded[0..retained]) |*clip| clip.deinit();
        const pcm = try alloc.alloc(audio.PcmAudio, clips.len);
        defer alloc.free(pcm);
        const indexes = try alloc.alloc(usize, clips.len);
        defer alloc.free(indexes);
        var cursor: usize = 0;
        var observation = @import("batch_execution.zig").Observation{};
        var fallback = false;
        while (cursor < clips.len) {
            var pcm_bytes: usize = 0;
            while (cursor < clips.len) {
                if (self.execution_control) |control| try control.check();
                const plan = try clapBatchPlan(session, retained + 1);
                const budget = self.config.max_audio_decode_working_bytes;
                const fits = plan.feature_reserve_bytes < budget and pcm_bytes < budget - plan.feature_reserve_bytes and
                    try session.fitsRun(.{ .batch = retained + 1, .input_bytes = plan.feature_elements * @sizeOf(f32), .host_preprocess_bytes = pcm_bytes });
                if (!fits) {
                    if (retained > 0) break;
                    errors[cursor] = error.AudioTooLarge;
                    cursor += 1;
                    continue;
                }
                decoded[retained] = audio.decodeBounded(alloc, clips[cursor].bytes, clips[cursor].decode_options, budget - plan.feature_reserve_bytes - pcm_bytes) catch |err| {
                    if (err == error.OutOfMemory or err == error.Canceled or err == error.DeadlineExceeded) return err;
                    if (err == error.AudioTooLarge and retained > 0) break;
                    errors[cursor] = err;
                    cursor += 1;
                    continue;
                };
                const clip = decoded[retained];
                const next_pcm_bytes = pcm_bytes + clip.samples.len * @sizeOf(f32);
                const decoded_fits = session.fitsRun(.{ .batch = retained + 1, .input_bytes = plan.feature_elements * @sizeOf(f32), .host_preprocess_bytes = next_pcm_bytes }) catch |err| {
                    decoded[retained].deinit();
                    return err;
                };
                if (!decoded_fits) {
                    decoded[retained].deinit();
                    if (retained > 0) break;
                    errors[cursor] = error.ResourceLimitExceeded;
                    cursor += 1;
                    continue;
                }
                pcm[retained] = .{ .samples = clip.samples, .sample_rate = clip.sample_rate };
                indexes[retained] = cursor;
                retained += 1;
                pcm_bytes = next_pcm_bytes;
                cursor += 1;
            }
            if (retained == 0) continue;
            const batch = try self.embedAudioPcmReported(pcm[0..retained]);
            defer alloc.free(batch.vectors);
            if (batch.vectors.len != retained) {
                for (batch.vectors) |row| alloc.free(row);
                return error.UnexpectedOutputShape;
            }
            for (batch.vectors, indexes[0..retained]) |row, index| vectors[index] = row;
            observation.record(retained);
            fallback = fallback or batch.execution == .fallback;
            for (decoded[0..retained]) |*clip| clip.deinit();
            retained = 0;
        }
        return .{ .vectors = vectors, .execution = if (fallback) .fallback else observation.execution(observation.native_items + observation.serial_items) };
    }

    fn embedClapAudioPrepared(self: *EmbeddingPipeline, clips: []const EncodedAudioClip, errors: ?[]?anyerror) anyerror!ImageBatchResult {
        const prep = @import("audio_preparation.zig");
        const session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        const out = self.allocator;
        const vectors = try out.alloc([]f32, clips.len);
        @memset(vectors, &.{});
        errdefer {
            for (vectors) |vector| out.free(vector);
            out.free(vectors);
        }
        if (errors) |items| {
            if (items.len != clips.len) return error.InvalidInputShape;
            @memset(items, null);
        }
        if (clips.len == 0) return .{ .vectors = vectors, .execution = .serial };
        var slots: [2]prep.Wave = undefined;
        var live: [2]bool = @splat(false);
        defer for (&slots, live) |*slot, exists| if (exists) slot.deinit();
        const control = self.execution_control orelse InferenceExecutionControl{};
        const budget = self.config.max_audio_decode_working_bytes;
        const preprocessing = clapPreprocessingPolicy(session, clips.len);
        var current: usize = 0;
        var offset: usize = 0;
        const first = @min(prep.max_batch, clips.len);
        slots[0] = prep.Wave.initClap(session, clips[0..first], if (errors) |e| e[0..first] else null, control, budget, preprocessing);
        live[0] = true;
        slots[0].run();
        var observation = @import("batch_execution.zig").Observation{};
        var fallback = false;
        while (offset < clips.len) {
            try control.check();
            const slot = &slots[current];
            slot.join();
            if (slot.failure) |err| return err;
            if (self.audio_timings) |t| t.preprocess_ns += slot.prepare_ns;
            const next = 1 - current;
            const next_offset = offset + slot.consumed;
            const next_count = @min(prep.max_batch, clips.len - next_offset);
            if (next_count > 0) {
                slots[next] = prep.Wave.initClap(session, clips[next_offset..][0..next_count], if (errors) |e| e[next_offset..][0..next_count] else null, control, budget, preprocessing);
                live[next] = true;
                if (self.config.audio_prepare_ahead) {
                    const spawned = slots[next].start();
                    if (self.audio_timings) |t| t.preparation_jobs += @intFromBool(spawned);
                }
            }
            if (self.audio_timings) |t| t.max_prepared_audio = @max(t.max_prepared_audio, slot.clips.len + if (self.config.audio_prepare_ahead) next_count else 0);
            if (slot.retained > 0) {
                var scoped = self.*;
                scoped.allocator = slot.allocator();
                scoped.execution_gate_held = false;
                const plan = try slot.clapPlan(slot.retained);
                const request = @import("../backends/session.zig").RunRequest{ .batch = slot.retained, .sequence = plan.frames, .input_bytes = slot.mels.len * 4, .pre_admitted_host_bytes = slot.mels.len * 4, .host_preprocess_bytes = plan.feature_reserve_bytes };
                var permit = session.admit(request) catch |err| blk: {
                    if (next_count == 0 or !self.config.audio_prepare_ahead or !prep.pressure(err)) return err;
                    slots[next].deinit();
                    slots[next].failure = err;
                    break :blk try session.admit(request);
                };
                defer permit.deinit();
                const begun = platform.time.monotonicNs();
                const result = scoped.embedClapPreparedFeatures(session, plan, slot.retained, slot.mels, slot.flags[0..slot.retained], &permit) catch |err| blk: {
                    if (err != error.BatchedAudioOutputCollapsed or slot.retained == 1) return err;
                    permit.deinit();
                    // Preserve singleton fusion/layout when an exported graph
                    // collapses its batch. The feature producer stays bounded.
                    scoped.config.audio_prepared_batches = false;
                    const rows = try scoped.allocator.alloc([]f32, slot.retained);
                    var copied: usize = 0;
                    errdefer {
                        for (rows[0..copied]) |row| scoped.allocator.free(row);
                        scoped.allocator.free(rows);
                    }
                    for (slot.indexes[0..slot.retained], rows) |index, *row| {
                        const clip = slot.clips[index];
                        const one = if (clip.pcm) |pcm| try scoped.embedAudioPcm(&.{pcm}) else try scoped.embedEncodedAudio(&.{clip});
                        row.* = one[0];
                        scoped.allocator.free(one);
                        copied += 1;
                    }
                    fallback = true;
                    break :blk rows;
                };
                defer {
                    for (result) |row| scoped.allocator.free(row);
                    scoped.allocator.free(result);
                }
                if (result.len != slot.retained) return error.InvalidEmbeddingResultCount;
                self.last_resident_backend_timing = scoped.last_resident_backend_timing;
                if (self.audio_timings) |t| {
                    t.audio_ns += platform.time.monotonicNs() - begun;
                    t.resident_cohorts += 1;
                }
                for (slot.indexes[0..slot.retained], result) |index, row| vectors[offset + index] = try out.dupe(f32, row);
                observation.record(slot.retained);
            }
            slot.deinit();
            live[current] = false;
            offset = next_offset;
            if (next_count > 0) {
                if (!self.config.audio_prepare_ahead) slots[next].run();
                slots[next].join();
                if (slots[next].failure) |err| if (prep.pressure(err)) {
                    slots[next].deinit();
                    slots[next] = prep.Wave.initClap(session, clips[next_offset..][0..next_count], if (errors) |e| e[next_offset..][0..next_count] else null, control, budget, preprocessing);
                    if (self.audio_timings) |t| t.preparation_backpressure_retries += 1;
                    slots[next].run();
                };
                current = next;
            }
        }
        try control.check();
        return .{ .vectors = vectors, .execution = if (fallback) .fallback else observation.execution(observation.native_items + observation.serial_items) };
    }

    fn embedGemma2AudioSerial(self: *EmbeddingPipeline, clips: []const EncodedAudioClip, item_errors: ?[]?anyerror) ![][]f32 {
        const grouped = @import("embedding_gemma2.zig");
        const out = try self.allocator.alloc([]f32, clips.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |vector| self.allocator.free(vector);
            self.allocator.free(out);
        }
        for (clips, 0..) |clip, index| {
            if (item_errors) |errors| errors[index] = null;
            const part: grouped.Part = if (clip.pcm) |pcm| .{ .pcm = pcm } else .{ .audio = .{ .bytes = clip.bytes, .decode_options = clip.decode_options } };
            const result = grouped.embed(self.allocator, self.session, self.tok, .{ .content = &.{part} }, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes, .dimensions = self.config.embedding_gemma2_dimensions }), self.execution_lock, self.execution_control) catch |err| {
                if (item_errors) |errors| {
                    if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled or err == error.Timeout or err == error.DeadlineExceeded) return err;
                    errors[index] = err;
                    out[index] = try self.allocator.alloc(f32, 0);
                    initialized += 1;
                    continue;
                }
                return err;
            };
            out[index] = result.vector;
            initialized += 1;
        }
        return out;
    }

    /// Embed a batch of interleaved PCM audio clips, explicitly downmixing to
    /// mono before CLAP preprocessing.
    pub fn embedAudioInterleavedPcm(
        self: *EmbeddingPipeline,
        audio_clips: []const audio.PcmAudioInterleaved,
    ) ![][]f32 {
        if (audio_clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) return self.embedGemma2Pcm(audio_clips);
        if (audio_clips.len == 0) return try self.allocator.alloc([]f32, 0);

        const audio_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        const feature_plan = try clapBatchPlan(audio_session, audio_clips.len);
        try ensureClapWorkingSet(0, feature_plan.feature_reserve_bytes, self.config.max_audio_decode_working_bytes);
        var retained_mono_bytes: usize = 0;
        for (audio_clips) |clip| {
            if (clip.channels == 0 or clip.samples.len % clip.channels != 0) return error.UnsupportedAudioFormat;
            const mono_samples = clip.samples.len / clip.channels;
            const mono_bytes = std.math.mul(usize, mono_samples, @sizeOf(f32)) catch return error.AudioTooLarge;
            retained_mono_bytes = std.math.add(usize, retained_mono_bytes, mono_bytes) catch return error.AudioTooLarge;
        }
        try ensureClapWorkingSet(
            retained_mono_bytes,
            feature_plan.feature_reserve_bytes,
            self.config.max_audio_decode_working_bytes,
        );

        const alloc = self.allocator;
        var mono_storage = try alloc.alloc([]f32, audio_clips.len);
        defer alloc.free(mono_storage);

        var mono_inputs = try alloc.alloc(audio.PcmAudio, audio_clips.len);
        defer alloc.free(mono_inputs);

        var initialized: usize = 0;
        errdefer {
            for (mono_storage[0..initialized]) |samples| alloc.free(samples);
        }

        for (audio_clips, 0..) |clip, i| {
            mono_storage[i] = try audio.downmixToMono(alloc, clip.samples, clip.channels);
            initialized += 1;
            mono_inputs[i] = .{
                .samples = mono_storage[i],
                .sample_rate = clip.sample_rate,
            };
        }
        defer {
            for (mono_storage[0..initialized]) |samples| alloc.free(samples);
        }

        return self.embedAudioPcm(mono_inputs);
    }

    /// Embed a batch of PCM audio clips, returning [batch][embed_dim] embeddings.
    /// Requires an audio_session (CLAP model).
    pub fn embedAudioPcm(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) anyerror![][]f32 {
        if (audio_clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        return (try self.embedAudioPcmReported(audio_clips)).vectors;
    }

    pub fn embedAudioPcmReported(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) anyerror!ImageBatchResult {
        if (audio_clips.len > 0 and !self.config.allow_audio) return error.UnsupportedInput;
        const lane = try self.enterExecutionLane();
        defer self.leaveExecutionLane(lane);
        if (!self.config.resident_embedding_gemma2 and session_factory.getEmbeddingGemma2Config(self.session) != null) return self.embedGemma2PcmReported(audio_clips);
        if (audio_clips.len == 0) return .{ .vectors = try self.allocator.alloc([]f32, 0), .execution = .serial };
        const prepared_session = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;
        if (self.config.audio_prepared_batches and (prepared_session.backend() == .metal or prepared_session.backend() == .cuda)) {
            const clips = try self.allocator.alloc(EncodedAudioClip, audio_clips.len);
            defer self.allocator.free(clips);
            for (clips, audio_clips) |*dst, pcm| dst.* = .{ .pcm = pcm };
            return self.embedClapAudioPrepared(clips, null);
        }
        const vectors = self.embedAudioPcmBatch(audio_clips) catch |err| {
            if (audio_clips.len > 1 and err == error.BatchedAudioOutputCollapsed) {
                return .{ .vectors = try self.embedAudioPcmIndividually(audio_clips), .execution = .fallback };
            }
            return err;
        };
        return .{ .vectors = vectors, .execution = if (audio_clips.len > 1) .native_batch else .serial };
    }

    fn embedGemma2Pcm(self: *EmbeddingPipeline, clips: anytype) ![][]f32 {
        return (try self.embedGemma2PcmReported(clips)).vectors;
    }

    fn embedGemma2PcmReported(self: *EmbeddingPipeline, clips: anytype) !ImageBatchResult {
        if (clips.len > 128) return error.ResourceLimitExceeded;
        var bytes: usize = 0;
        for (clips) |clip| {
            const channels = if (@hasField(@TypeOf(clip), "channels")) clip.channels else 1;
            if (channels == 0 or channels > 8 or clip.sample_rate == 0 or clip.sample_rate > 192000 or clip.samples.len == 0 or clip.samples.len % channels != 0) return error.UnsupportedAudioFormat;
            if (clip.samples.len / channels > @as(usize, clip.sample_rate) * 30) return error.AudioTooLong;
            bytes = std.math.add(usize, bytes, std.math.mul(usize, clip.samples.len, 4) catch return error.AudioTooLarge) catch return error.AudioTooLarge;
            for (clip.samples) |sample| if (!std.math.isFinite(sample)) return error.UnsupportedAudioFormat;
        }
        if (bytes > 64 * 1024 * 1024) return error.AudioTooLarge;
        if (self.config.gemma4_audio_batching and (self.session.backend() == .metal or self.session.backend() == .cuda)) {
            // Original PCM admission/conversion is host-only. No Model or
            // numerical lane is held while its supplier runs; the existing
            // grouped Stage below owns both for actual projection/encoding.
            var host = try self.session.admitHostPreprocess(bytes);
            defer host.deinit();
            const prepared = try self.allocator.alloc(EncodedAudioClip, clips.len);
            defer self.allocator.free(prepared);
            var mono = std.ArrayList([]f32).empty;
            defer {
                for (mono.items) |samples| self.allocator.free(samples);
                mono.deinit(self.allocator);
            }
            for (prepared, clips) |*dst, clip| {
                if (self.execution_control) |control| try control.check();
                const channels = if (@hasField(@TypeOf(clip), "channels")) clip.channels else 1;
                var samples: []const f32 = clip.samples;
                if (channels > 1) {
                    const downmixed = try self.allocator.alloc(f32, samples.len / channels);
                    mono.append(self.allocator, downmixed) catch |err| {
                        self.allocator.free(downmixed);
                        return err;
                    };
                    for (downmixed, 0..) |*value, frame| {
                        var sum: f32 = 0;
                        for (samples[frame * channels ..][0..channels]) |sample| sum += sample;
                        value.* = sum / @as(f32, @floatFromInt(channels));
                    }
                    samples = downmixed;
                }
                dst.* = .{ .pcm = .{ .samples = samples, .sample_rate = clip.sample_rate } };
            }
            const result = try @import("embedding_gemma2.zig").embedAudios(self.allocator, self.session, self.tok, prepared, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes, .dimensions = self.config.embedding_gemma2_dimensions, .prepare_ahead = self.config.audio_prepare_ahead, .timings = self.audio_timings }), self.execution_lock, self.execution_control, null);
            self.allocator.free(result.input_tokens);
            return .{ .vectors = result.vectors, .execution = result.execution };
        }
        var permit = try self.session.admit(.{ .batch = 1, .sequence = 1, .host_preprocess_bytes = 64 * 1024 * 1024 });
        defer permit.deinit();
        const output = try self.allocator.alloc([]f32, clips.len);
        var initialized: usize = 0;
        errdefer {
            for (output[0..initialized]) |vector| self.allocator.free(vector);
            self.allocator.free(output);
        }
        for (clips, output) |clip, *vector| {
            if (self.execution_control) |control| try control.check();
            const channels: u16 = if (@hasField(@TypeOf(clip), "channels")) @intCast(clip.channels) else 1;
            // IEEE F32 WAV preserves PCM values exactly and shares the same
            // bounded decoder, downmix, resample and HF processor as HTTP.
            const encoded = try audio.wav.encodeInterleaved(std.heap.smp_allocator, clip.samples, channels, .{ .audio_format = 3, .sample_rate = clip.sample_rate, .bits_per_sample = 32 });
            defer std.heap.smp_allocator.free(encoded);
            const result = try @import("embedding_gemma2.zig").embed(self.allocator, self.session, self.tok, .{ .content = &.{.{ .audio = .{ .bytes = encoded } }} }, self.gemmaStageOptions(.{ .max_tokens = @min(self.config.max_length, 8192), .max_audio_decode_working_bytes = self.config.max_audio_decode_working_bytes, .dimensions = self.config.embedding_gemma2_dimensions }), self.execution_lock, self.execution_control);
            vector.* = result.vector;
            initialized += 1;
        }
        return .{ .vectors = output, .execution = .serial };
    }

    fn enterExecutionLane(self: *EmbeddingPipeline) !?@import("../execution_control.zig").ExecutionLane.Lease {
        if (self.execution_lane == null) return null;
        if (self.usesGemmaWorkspaceLaneProtocol() or self.usesNativeWeightLaneProtocol()) return null;
        return self.enterNumericalExecutionLane();
    }
    fn enterNumericalExecutionLane(self: *EmbeddingPipeline) !?@import("../execution_control.zig").ExecutionLane.Lease {
        if (self.execution_lane_held) return null;
        const lane = self.execution_lane orelse return null;
        const lease = try lane.acquireWithPriority(self.execution_control orelse .{}, self.interactive);
        self.execution_lane_held = true;
        return lease;
    }

    fn leaveExecutionLane(self: *EmbeddingPipeline, lease: ?@import("../execution_control.zig").ExecutionLane.Lease) void {
        if (lease) |owned| {
            self.execution_lane_held = false;
            owned.release();
        }
    }

    fn lockExecution(self: *EmbeddingPipeline) !void {
        return self.lockExecutionWithLanePolicy(self.usesGemmaWorkspaceLaneProtocol());
    }
    fn lockExecutionWithLanePolicy(self: *EmbeddingPipeline, gemma_late_lane: bool) !void {
        if (self.execution_lane_from_lock != null or self.direct_cohort_owned) return error.InvalidStageCohort;
        if (self.execution_lock) |mutex| {
            if (self.execution_control) |control|
                try control.lock(mutex)
            else
                platform.sync.lockYielding(mutex);
        } else if (gemma_late_lane and self.execution_lane != null) {
            // A lane-only direct prepare still needs the actual selected Model
            // owner first. This is the real Session cohort, not a guessed alias
            // for an absent injected native mutex.
            if (self.session.execution_gate == null and self.session.cohort_scheduler == null) {
                // Explicit raw/unadmitted factory contract has no Model
                // cohort. Preserve its original caller-supplied lane only;
                // absence of a gate cannot mint admitted Model authority.
                if (self.session.run_admission != null) return error.MissingExecutionCohortScheduler;
            } else {
                if (self.session.execution_gate == null or self.session.cohort_scheduler == null) return error.MissingExecutionCohortScheduler;
                try self.session.enterExecutionCohort(self.session.execution_gate, self.execution_control orelse .{}, &self.direct_cohort_waiter);
                self.direct_cohort_owned = true;
            }
        }
        errdefer self.unlockExecution();
        if (self.execution_lane != null and gemma_late_lane) self.execution_lane_from_lock = try self.enterNumericalExecutionLane();
    }

    fn unlockExecution(self: *EmbeddingPipeline) void {
        if (self.execution_lane_from_lock) |lease| self.leaveExecutionLane(lease);
        self.execution_lane_from_lock = null;
        if (self.direct_cohort_owned) {
            self.direct_cohort_owned = false;
            self.session.leaveExecutionCohort(self.session.execution_gate, &self.direct_cohort_waiter);
        } else if (self.execution_lock) |mutex| mutex.unlock();
    }

    fn embedAudioPcmBatch(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) ![][]f32 {
        const as = self.audio_session orelse if (sessionHasInput(self.session, "input_features")) self.session else return error.NoAudioSession;

        const alloc = self.allocator;
        const batch = audio_clips.len;
        const feature_plan = try clapBatchPlan(as, batch);
        try ensureClapWorkingSet(0, feature_plan.feature_reserve_bytes, self.config.max_audio_decode_working_bytes);

        // Process each audio clip to official CLAP input features and concatenate.
        var retained_pcm_bytes: usize = 0;
        for (audio_clips) |clip| {
            const clip_bytes = std.math.mul(usize, clip.samples.len, @sizeOf(f32)) catch return error.AudioTooLarge;
            retained_pcm_bytes = std.math.add(usize, retained_pcm_bytes, clip_bytes) catch return error.AudioTooLarge;
        }
        try ensureClapWorkingSet(
            retained_pcm_bytes,
            feature_plan.feature_reserve_bytes,
            self.config.max_audio_decode_working_bytes,
        );

        var is_longer = try alloc.alloc(u8, batch);
        defer alloc.free(is_longer);
        const feature_bytes = std.math.mul(usize, feature_plan.feature_elements, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        var run_permit = try as.admit(.{
            .batch = batch,
            .sequence = feature_plan.frames,
            .input_bytes = feature_bytes,
            .host_preprocess_bytes = std.math.add(
                usize,
                retained_pcm_bytes,
                feature_plan.feature_reserve_bytes,
            ) catch return error.ResourceLimitExceeded,
        });
        defer run_permit.deinit();
        const all_mels = try alloc.alloc(f32, feature_plan.feature_elements);
        @memset(all_mels, 0.0);
        defer alloc.free(all_mels);

        const features_start = embedTimingStart(self.print_timing);
        for (0..batch) |b| {
            var features = try audio.clapFeaturesFromPcm(
                alloc,
                audio_clips[b].samples,
                audio_clips[b].sample_rate,
                feature_plan.channels,
            );
            defer features.deinit();

            const per_clip_elements = feature_plan.feature_elements / batch;
            const dst = all_mels[b * per_clip_elements ..][0..per_clip_elements];
            const src_frames = @min(features.time_frames, feature_plan.frames);
            const src_channels = @min(features.channels, feature_plan.channels);
            const src_plane = features.time_frames * features.mel_bins;
            const dst_plane = feature_plan.frames * feature_plan.mels;
            for (0..src_channels) |ch| {
                for (0..src_frames) |frame| {
                    const src_off = ch * src_plane + frame * features.mel_bins;
                    const dst_off = ch * dst_plane + frame * feature_plan.mels;
                    @memcpy(dst[dst_off..][0..feature_plan.mels], features.data[src_off..][0..feature_plan.mels]);
                }
            }
            is_longer[b] = if (features.is_longer) 1 else 0;
        }
        logEmbedTiming("audio.features", batch, features_start);

        if (feature_plan.fusion_enabled) {
            var any_long = false;
            for (is_longer) |flag| {
                if (flag != 0) {
                    any_long = true;
                    break;
                }
            }
            if (!any_long and is_longer.len == 1) {
                // Some exported CLAP graphs expect the fusion branch to be
                // exercised for single short clips. Do not fake a long item for
                // multi-item batches: the fusion path contains concrete
                // singleton-batch reshapes in common ONNX exports.
                is_longer[0] = 1;
            }
        }

        return self.embedClapPreparedFeatures(as, feature_plan, batch, all_mels, is_longer, &run_permit);
    }

    fn embedClapPreparedFeatures(self: *EmbeddingPipeline, as: backends.Session, feature_plan: ClapBatchPlan, batch: usize, all_mels: []const f32, is_longer: []const u8, permit: *@import("../backends/session.zig").RunPermit) ![][]f32 {
        const alloc = self.allocator;
        // Build input tensor: [batch, channels, n_frames, n_mels]
        const mel_shape = [_]i64{ @intCast(batch), @intCast(feature_plan.channels), @intCast(feature_plan.frames), @intCast(feature_plan.mels) };
        var mel_tensor = try Tensor.initFloat32(alloc, "input_features", &mel_shape, all_mels);
        defer mel_tensor.deinit();
        const longer_shape = [_]i64{ @intCast(batch), 1 };
        var is_longer_tensor = try Tensor.initBool(alloc, "is_longer", &longer_shape, is_longer);
        defer is_longer_tensor.deinit();

        // Run audio encoder
        const run_inputs = if (sessionHasInput(as, "is_longer"))
            &[_]Tensor{ mel_tensor, is_longer_tensor }
        else
            &[_]Tensor{mel_tensor};

        if (self.execution_gate_held) return error.PreparedAdmissionRequiresReadyTurn;
        var source_stage: session_mod.PreparedSource = .{};
        defer source_stage.deinit();
        try source_stage.initBeforeExecutionInto(as, self.session, self.execution_lock, .{ .host = run_inputs }, .{}, permit, if (self.audio_projection != null) .resident_consumer else .host, alloc, self.execution_control);
        try source_stage.enter();

        if (self.audio_projection) |proj| {
            if (try self.tryEmbedResidentProjection(
                run_inputs,
                proj,
                .audio,
                batch,
                "audio.encoder.resident",
                "audio.projection.resident",
                &source_stage,
            )) |embeddings| return embeddings;
        }

        const encoder_start = embedTimingStart(self.print_timing);
        const outputs = try source_stage.runHost(run_inputs);
        logEmbedTiming("audio.encoder", batch, encoder_start);
        logEmbedTensorShapes(self.print_timing, "audio.outputs", outputs);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        return self.audioEmbeddingsFromOutputs(outputs, batch);
    }

    fn audioEmbeddingsFromOutputs(self: *EmbeddingPipeline, outputs: []Tensor, batch: usize) ![][]f32 {
        const alloc = self.allocator;
        if (outputs.len == 0) return error.NoOutputTensors;

        if (self.audio_projection) |proj| {
            if (projectionInputDim(proj)) |expected_dim| {
                if (selectProjectedOutput(outputs, expected_dim, batch)) |selection| {
                    const projected_embeddings = if (selection.use_cls_pool)
                        try self.extractVision3D(selection.tensor, batch, false)
                    else
                        try self.extract2D(selection.tensor, batch, false);
                    errdefer freeEmbeddingSlices(alloc, projected_embeddings);
                    try self.projectEmbeddings(projected_embeddings, proj);
                    return projected_embeddings;
                }
            }
        }

        if (batch > 1 and !outputsContainBatchRows(outputs, batch)) {
            logEmbedFallback(self.print_timing, "audio.individual", batch);
            return error.BatchedAudioOutputCollapsed;
        }

        const embeddings = switch (outputs[0].shape.len) {
            3 => try self.extractVision3D(&outputs[0], batch, self.audio_projection == null),
            2 => try self.extract2D(&outputs[0], batch, self.audio_projection == null),
            else => return error.UnexpectedOutputShape,
        };
        errdefer freeEmbeddingSlices(alloc, embeddings);

        // Apply audio projection if present (CLIPCLAP: CLAP→CLIP space)
        if (self.audio_projection) |proj| {
            try self.projectEmbeddings(embeddings, proj);
        }

        return embeddings;
    }

    fn embedAudioPcmIndividually(self: *EmbeddingPipeline, audio_clips: []const audio.PcmAudio) anyerror![][]f32 {
        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, audio_clips.len);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |embedding| alloc.free(embedding);
            alloc.free(embeddings);
        }

        for (audio_clips, 0..) |_, i| {
            // Each batch cold-prepares before acquiring its numerical lock.
            const one = try self.embedAudioPcmBatch(audio_clips[i .. i + 1]);
            defer alloc.free(one);
            if (one.len != 1) return error.UnexpectedOutputShape;
            embeddings[i] = one[0];
            initialized += 1;
        }

        return embeddings;
    }

    pub fn deinit(self: *EmbeddingPipeline) void {
        self.session.close();
        if (self.vision_session) |vs| vs.close();
        if (self.audio_session) |as_| as_.close();
        if (self.text_projection) |tp| tp.close();
        if (self.visual_projection) |vp| vp.close();
        if (self.audio_projection) |ap| ap.close();
        self.tok.deinitTokenizer();
    }

    fn projectEmbeddings(self: *EmbeddingPipeline, embeddings: [][]f32, proj: backends.Session) !void {
        const alloc = self.allocator;
        if (embeddings.len == 0) return;

        const batch = embeddings.len;
        const in_dim = embeddings[0].len;
        for (embeddings) |embedding| {
            if (embedding.len != in_dim) return error.ShapeMismatch;
        }
        const input_elements = std.math.mul(usize, batch, in_dim) catch
            return error.ResourceLimitExceeded;
        const input_bytes = std.math.mul(usize, input_elements, @sizeOf(f32)) catch
            return error.ResourceLimitExceeded;
        if (self.execution_gate_held) return error.ProjectionAdmissionRequiresStageBoundary;
        if (self.execution_control) |control| try control.check();
        var packed_permit = try proj.admitHostPreprocess(try std.math.mul(usize, input_bytes, 2));
        defer packed_permit.deinit();
        const packed_embeddings = try alloc.alloc(f32, input_elements);
        defer alloc.free(packed_embeddings);
        for (embeddings, 0..) |embedding, b| {
            @memcpy(packed_embeddings[b * in_dim ..][0..in_dim], embedding);
        }

        const proj_shape = [_]i64{ @intCast(batch), @intCast(in_dim) };
        var proj_input = try Tensor.initFloat32(alloc, "input", &proj_shape, packed_embeddings);
        defer proj_input.deinit();

        var destination: session_mod.PreparedSource = .{};
        defer destination.deinit();
        try destination.initBeforeExecutionInto(proj, proj, self.execution_lock, .{ .host = &.{proj_input} }, .{
            .batch = batch,
            .sequence = 1,
            .input_bytes = input_bytes,
            .host_preprocess_bytes = input_bytes,
            .pre_admitted_host_bytes = try std.math.mul(usize, input_bytes, 2),
        }, null, .host, alloc, self.execution_control);
        try destination.enter();
        const projection_start = embedTimingStart(self.print_timing);
        var proj_outputs = try destination.runHost(&.{proj_input});
        logEmbedTiming("projection", batch, projection_start);
        defer {
            for (proj_outputs) |*o| o.deinit();
            alloc.free(proj_outputs);
        }

        if (proj_outputs.len == 0) return error.NoOutputTensors;

        const proj_data = proj_outputs[0].asFloat32();
        if (proj_outputs[0].shape.len == 0) return error.UnexpectedOutputShape;
        const proj_dim: usize = @intCast(proj_outputs[0].shape[proj_outputs[0].shape.len - 1]);
        if (proj_dim == 0 or proj_data.len != batch * proj_dim) return error.ShapeMismatch;

        for (0..batch) |b| {
            if (proj_dim != in_dim) {
                const projected_embedding = try alloc.alloc(f32, proj_dim);
                alloc.free(embeddings[b]);
                embeddings[b] = projected_embedding;
            }
            @memcpy(embeddings[b], proj_data[b * proj_dim ..][0..proj_dim]);
            if (self.config.normalize) {
                linalg.l2Normalize(embeddings[b], embeddings[b].len);
            }
        }
    }

    const ResidentPooled = sequence_pool.Value;

    fn recordResidentProjection(
        self: *EmbeddingPipeline,
        modality: ResidentProjectionModality,
        outcome: ResidentProjectionOutcome,
        phase: []const u8,
        count: usize,
        reason: ?[]const u8,
    ) void {
        if (self.resident_projection_stats) |stats| {
            stats.record(modality, outcome);
        }

        if (outcome == .success) {
            logEmbedResidentSuccess(modality, phase, count);
        } else {
            logEmbedResidentFallback(modality, phase, count, reason orelse "unknown");
        }
    }

    fn residentProjectionFallback(
        self: *EmbeddingPipeline,
        modality: ResidentProjectionModality,
        phase: []const u8,
        count: usize,
        reason: []const u8,
    ) !?[][]f32 {
        self.recordResidentProjection(modality, .fallback, phase, count, reason);
        if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
        return null;
    }

    fn residentProjectionRequired(self: *const EmbeddingPipeline) bool {
        return self.config.resident_projection_required or embedResidentFailClosedEnabled();
    }

    fn tryEmbedTextResidentQwen3(
        self: *EmbeddingPipeline,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        cleanup: CleanupContext,
        receipt: ?*@import("../execution/existing_workspace_receipt.zig").Owner,
        pre_entry_refusal: ?*bool,
    ) !?[][]f32 {
        // An opaque/manual raw API owns its original host callback contract.
        // No provider cast, new Root, storage credit, or inferred B is granted.
        // The exact Arch raw host service checks no-tag before numerical work.
        if (!@import("../execution/architecture_session.zig").acceptsMetalWorkspaceSession(self.session)) {
            try self.validateQwenWorkspaceAdmission();
            return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "uncertified_workspace_owner");
        }
        const cfg = session_factory.getGptConfig(self.session) orelse {
            if (self.residentProjectionRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "not_gpt_session");
            }
            return null;
        };
        if (!residentQwen3EmbeddingEligible(self.session, cfg, self.config)) {
            if (self.residentProjectionRequired()) {
                return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "ineligible");
            }
            return null;
        }

        return self.embedTextResidentQwen3(cfg, mask, input_ids, batch, seq_len, cleanup, receipt, pre_entry_refusal);
    }

    fn embedTextResidentQwen3(
        self: *EmbeddingPipeline,
        cfg: gpt_arch.Config,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        cleanup: CleanupContext,
        receipt: ?*@import("../execution/existing_workspace_receipt.zig").Owner,
        pre_entry_refusal: ?*bool,
    ) !?[][]f32 {
        if (comptime !build_options.enable_metal) return null;
        const architecture = @import("../execution/architecture_session.zig");
        const actual_canonical_provider = architecture.acceptsMetalWorkspaceSession(self.session);
        var compute = if (actual_canonical_provider)
            try architecture.constructMetalWorkspaceReceiver(self.session, self.allocator, self.execution_control)
        else
            try compute_provider.acquireWithControl(self.session, self.allocator, self.execution_control);
        var current = cleanup;
        defer compute.deinitWithCleanup(current);
        const cb = &compute.backend;
        if (cb.kind() != .metal) {
            return self.residentProjectionFallback(.text, "text.encoder.qwen3.resident", batch, "not_metal_backend");
        }

        const canonical = if (comptime build_options.enable_metal)
            @import("../ops/metal_compute.zig").MetalCompute.acceptsEmbeddingStage(cb)
        else
            false;
        var scoped: ?*const CleanupContext = null;
        if (canonical) {
            const boundary = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (!try cb.bindCleanupEpoch(boundary)) return error.BackendOperationCleanupUnavailable;
            current.operation = cb.operationToken();
            _ = try current.operationFor(cb);
            scoped = &current;
        }

        const Metal = @import("../ops/metal_compute.zig").MetalCompute;
        var context: @import("../execution/invocation_context.zig").Context = .{ .cleanup = current };
        var binding: ?Metal.WorkspaceInvocationBinding = null;
        var binding_live = false;
        defer if (binding_live) {
            const owner = receipt.?;
            Metal.finishExistingWorkspaceInvocation(&binding.?, &owner.final_owned, owner.roots) catch platform.inference_process_supervisor.restartWorker();
        };
        if (actual_canonical_provider) {
            if (receipt) |owner| {
                context.workspace_uses = try owner.receiverUses();
                binding = Metal.bindExistingWorkspaceInvocation(cb, &context, owner.runtime_address, &owner.expected, &owner.validation) catch |err| {
                    if (err == error.WorkspaceCensusChanged or err == error.WorkspaceCensusCapacityChanged or err == error.ForeignWorkspaceEndpoint) {
                        if (pre_entry_refusal) |proof| proof.* = true;
                        return error.WorkspaceReceiptChangedBeforeExecution;
                    }
                    return err;
                };
                binding_live = true;
            } else Metal.validateNoWorkspaceInvocation(cb) catch |err| {
                if (err == error.UnpreparedWorkspaceUses) {
                    if (pre_entry_refusal) |proof| proof.* = true;
                    return error.WorkspaceReceiptChangedBeforeExecution;
                }
                return err;
            };
            // Constructor-only path: original request setup occurs exactly
            // ONCE, after receiving identity/census/B are attested.
            try @import("../backends/compute_factory.zig").beginRequestWithCleanup(self.session, cb, current);
        }
        const result = try self.embedTextResidentQwen3OnBackend(cb, cfg, mask, input_ids, batch, seq_len, scoped);
        errdefer if (result) |vectors| freeEmbeddingSlices(self.allocator, vectors);
        if (binding_live) {
            binding_live = false;
            const owner = receipt.?;
            try Metal.finishExistingWorkspaceInvocation(&binding.?, &owner.final_owned, owner.roots);
        }
        return result;
    }

    fn snapshotResidentBackendTiming(self: *EmbeddingPipeline, cb: *const ops_mod.ComputeBackend, scoped: ?*const CleanupContext) void {
        const operation = if (scoped) |current| current.operation else null;
        self.last_resident_backend_timing = if (operation) |token|
            @import("../execution/metadata_sample.zig").snapshotOwned(cb, token)
        else
            null;
    }

    /// SAME numerical body for an already-attested final receiving header.
    /// Its caller owns the B/use-bank binding through encoder AND pooling.
    fn embedTextResidentQwen3OnBackend(
        self: *EmbeddingPipeline,
        cb: *const ops_mod.ComputeBackend,
        cfg: gpt_arch.Config,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        scoped: ?*const CleanupContext,
    ) anyerror!?[][]f32 {
        if (try self.tryEmbedTextResidentQwen3Graph(cb, cfg, mask, input_ids, batch, seq_len, scoped)) |graph_embeddings| {
            self.snapshotResidentBackendTiming(cb, scoped);
            return graph_embeddings;
        }

        // The cold selected declaration already committed this route. A null
        // optional callback after graph work cannot trigger eager replay.
        if (declaredWorkspaceSelected(cb)) return error.PreparedWorkspaceKernelUnavailable;

        const prepare = try cb.decoderRuntimePrepareOrReuseFamily(
            self.allocator,
            cfg,
            0,
            cfg.num_hidden_layers,
        );
        if (!prepare.prepared) {
            return self.residentProjectionFallback(.text, "text.prepare.qwen3.resident", batch, "prepare_failed");
        }

        var overrides = decoder_gated_runtime.buildOverridesWithLevel(
            cfg,
            cfg.num_hidden_layers,
            qwen3_embedding_resident_override_level,
        );
        overrides.cleanup = scoped;
        const encoder_start = embedTimingStart(self.print_timing);
        const hidden = try gpt_arch.hiddenForwardResidentWithOverrides(
            cb,
            self.allocator,
            cfg,
            input_ids,
            batch,
            seq_len,
            null,
            overrides,
        );
        logEmbedTiming("text.encoder.qwen3.resident", batch, encoder_start);

        // A borrowed stack vector needs no fallible metadata allocation after
        // the actual producer. The original backend owns the one hidden CT.
        defer if (scoped) |cleanup_scope| cb.freeWithCleanup(hidden, cleanup_scope.*) else cb.free(hidden);
        var output_storage = [_]ops_mod.CT{hidden};
        var encoder_outputs = session_mod.ResidentOutputs{
            .outputs = &output_storage,
            .backend = cb,
            .allocator = self.allocator,
        };

        const embeddings = try self.finishProducedTextRowsWithCleanup(&encoder_outputs, mask, batch, seq_len, self.config.normalize, "text.pool.qwen3.resident", "text.encoder.qwen3.resident", scoped);
        self.snapshotResidentBackendTiming(cb, scoped);
        return embeddings;
    }

    fn tryEmbedTextResidentQwen3Graph(
        self: *EmbeddingPipeline,
        cb: *const ops_mod.ComputeBackend,
        cfg: gpt_arch.Config,
        mask: []const i32,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        cleanup: ?*const CleanupContext,
    ) !?[][]f32 {
        const selected_rows = if (self.config.pooling == .last)
            gpt_arch.activeFinalRowIndices(i32, self.allocator, mask, batch, seq_len) catch |err| switch (err) {
                error.InvalidRerankerAttentionMask, error.InvalidRerankerInputShape => return null,
                else => return err,
            }
        else
            null;
        defer if (selected_rows) |rows| self.allocator.free(rows);
        const disable_frame_normalize = @import("../ops/metal_compute.zig").MetalCompute.workspaceFrameNormalizeDisabled();
        const normalize_in_frame = self.config.normalize and selected_rows != null and
            self.boundedQwenBatchingEnabled() and !disable_frame_normalize;
        const encoder_start = embedTimingStart(self.print_timing);
        const output = (try gpt_arch.tryDenseQwen3Prefill(cb, self.allocator, cfg, input_ids, mask, batch, seq_len, .{
            .cleanup = cleanup,
            .output_rows = selected_rows,
            .normalize_selected_rows = normalize_in_frame,
            .profile = embedTimingEnabled(self.print_timing),
        })) orelse return null;
        defer if (cleanup) |current_scope| cb.freeWithCleanup(output, current_scope.*) else cb.free(output);
        logEmbedTiming("text.encoder.qwen3.graph", batch, encoder_start);
        if (embedTimingEnabled(self.print_timing)) {
            const stats = cb.debugTimingSnapshot().provider;
            std.debug.print("qwen3_embedding_metal_timing: batch={d} seq={d} hd128_dispatches={d} stages={any}\n", .{
                batch, seq_len, stats.metal_dense_causal_hd128_dispatches, stats.metal_stage_timing,
            });
        }
        var output_storage = [_]ops_mod.CT{output};
        var encoder_outputs = session_mod.ResidentOutputs{
            .outputs = &output_storage,
            .backend = cb,
            .allocator = self.allocator,
        };

        const embeddings = try self.finishProducedTextRowsWithCleanup(&encoder_outputs, mask, batch, seq_len, self.config.normalize and !normalize_in_frame, "text.pool.qwen3.graph", "text.encoder.qwen3.graph", cleanup);
        return embeddings;
    }

    /// Null is reserved for pre-producer eligibility. A completed hidden CT
    /// survives pooling refusal and is consumed exactly once by the host path.
    fn finishProducedTextRows(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool, phase: []const u8, success_phase: []const u8) ![][]f32 {
        return self.finishProducedTextRowsWithCleanup(outputs, mask, batch, sequence, normalize, phase, success_phase, null);
    }
    fn finishProducedTextRowsWithCleanup(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool, phase: []const u8, success_phase: []const u8, cleanup: ?*const CleanupContext) ![][]f32 {
        const rows = self.finishResidentTextRowsWithCleanup(outputs, mask, batch, sequence, normalize, cleanup) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => return self.finishProducedTextHost(outputs, mask, batch, sequence, normalize, phase, @errorName(err)),
            else => return err,
        };
        self.recordResidentProjection(.text, .success, success_phase, batch, null);
        return rows;
    }
    fn declaredWorkspaceSelected(cb: *const ops_mod.ComputeBackend) bool {
        const context = cb.call_context orelse return false;
        const uses = context.workspace_uses orelse return false;
        return uses.allocations != null;
    }
    fn finishProducedTextHost(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool, phase: []const u8, reason: []const u8) ![][]f32 {
        if (declaredWorkspaceSelected(outputs.backend)) return error.PreparedWorkspaceKernelUnavailable;
        self.recordResidentProjection(.text, .fallback, phase, batch, reason);
        if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
        const host = try outputs.materializeFloat32(self.allocator, &.{}, outputs.backend.operationToken());
        defer {
            for (host) |*value| value.deinit();
            self.allocator.free(host);
        }
        // Selected graph rows may already be normalized in the actual frame.
        // This borrowed task policy changes no ownership and is never deinited.
        var remaining = self.*;
        remaining.config.normalize = normalize;
        return remaining.finishTextHostOutputs(host, mask, batch, sequence);
    }

    const TextSourceCohort = union(enum) {
        prepared: *session_mod.PreparedSource,
        legacy: *@import("../runtime/embedding_scheduler.zig").Scheduler.Waiter,
    };

    fn releaseTextSourceCohort(self: *EmbeddingPipeline, source: ?TextSourceCohort) !void {
        const selected = source orelse return error.ProjectionAdmissionRequiresStageBoundary;
        const waiter = switch (selected) {
            .prepared => |owner| {
                try owner.releaseForConsumer();
                self.execution_gate_held = false;
                return;
            },
            .legacy => |value| value,
        };
        // A missing/acquired-false waiter is not authority over an ambient gate.
        if (waiter.gate == null and waiter.owner == null and self.execution_lock == null) {
            self.execution_gate_held = false;
            return;
        }
        if (!waiter.acquired or waiter.gate != self.execution_lock) return error.UnselectedExecutionCohort;
        self.session.leaveSelectedCohort(waiter);
        self.execution_gate_held = false;
    }

    fn finishPooledTextProjection(self: *EmbeddingPipeline, pooled: *const ResidentPooled, mask: []const i32, batch: usize, seq_len: usize, reason: []const u8) ![][]f32 {
        self.recordResidentProjection(.text, .fallback, "text.projection.resident", batch, reason);
        if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
        // Borrow only the pooled value. Its original physical owner stays live
        // across this checked host export and the downstream task-owned head.
        var values = [_]ops_mod.CT{pooled.value};
        var borrowed = session_mod.ResidentOutputs{ .outputs = &values, .backend = pooled.backend, .allocator = self.allocator };
        const host = try borrowed.materializeFloat32(self.allocator, &.{}, pooled.backend.operationToken());
        defer {
            for (host) |*value| value.deinit();
            self.allocator.free(host);
        }
        return self.finishTextHostOutputs(host, mask, batch, seq_len);
    }

    fn tryEmbedTextResidentProjection(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
        proj: backends.Session,
        permit: *session_mod.RunPermit,
        projection_shell: *?@import("../execution/resource_retirement.zig").PreparedCharge,
        cleanup: CleanupContext,
        projection_execution: *?session_mod.RunPermit,
        source_cohort: ?TextSourceCohort,
    ) !?[][]f32 {
        const expected_dim = projectionInputDim(proj) orelse
            return self.residentProjectionFallback(
                .text,
                "text.projection.resident",
                batch,
                "unknown_projection_input_dim",
            );
        try permit.validateResidentOutputRetirement();
        if (proj.run_admission != null and projection_shell.* == null) return error.UnpreparedOutputRetirement;
        const encoder_start = embedTimingStart(self.print_timing);
        const pending_outputs = (try permit.runResidentWithControl(inputs, self.allocator, self.execution_control)) orelse
            return self.residentProjectionFallback(.text, "text.encoder.resident", batch, "unsupported");
        logEmbedTiming("text.encoder.resident", batch, encoder_start);
        var pending: ?session_mod.ResidentOutputs = pending_outputs;
        errdefer if (pending) |*outputs| outputs.deinitWithBoundary(cleanup.boundary);
        var consumer: session_mod.PreparedSource.SynchronousResident = if (source_cohort) |cohort| switch (cohort) {
            .prepared => |owner| try owner.adoptSynchronousResident(&pending),
            .legacy => legacy: {
                try permit.attachResidentOutputs(&pending.?);
                const moved = pending.?;
                pending = null;
                break :legacy .{ .outputs = moved, .source = null };
            },
        } else legacy: {
            try permit.attachResidentOutputs(&pending.?);
            const moved = pending.?;
            pending = null;
            break :legacy .{ .outputs = moved, .source = null };
        };
        defer consumer.close(cleanup);
        const encoder_outputs = &consumer.outputs;
        if (encoder_outputs.outputs.len == 0) return error.NoOutputTensors;

        // Promotion validates the COMPLETE original output set, exact model,
        // concrete backend/table, live operation and current B before a derived
        // pooled CT/backend can escape. Decline retains the original source.
        const source_ready = try encoder_outputs.prepareResidentConsumer(.{ .boundary = cleanup.boundary, .operation = encoder_outputs.backend.operationToken() });
        if (!source_ready) {
            self.recordResidentProjection(.text, .fallback, "text.encoder.resident", batch, "source_not_detachable");
            if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
            const host = try encoder_outputs.materializeFloat32(self.allocator, &.{}, encoder_outputs.backend.operationToken());
            defer {
                for (host) |*value| value.deinit();
                self.allocator.free(host);
            }
            consumer.close(cleanup);
            permit.retireExecutionWithCleanup(cleanup);
            try self.releaseTextSourceCohort(source_cohort);
            if (source_cohort == null or std.meta.activeTag(source_cohort.?) == .legacy) permit.deinitWithCleanup(cleanup);
            return try self.finishTextHostOutputs(host, mask, batch, seq_len);
        }
        // The original Engine/provider has retired; the payload/model/quota
        // moved intact. Keep B alive through the destination, release only the
        // caller's exact selected model cohort, and never replay the encoder.
        permit.retireExecutionWithCleanup(cleanup);
        try self.releaseTextSourceCohort(source_cohort);
        if (source_cohort == null or std.meta.activeTag(source_cohort.?) == .legacy) permit.deinitWithCleanup(cleanup);
        var pooled = self.residentPoolTextOutput(encoder_outputs, mask, batch, seq_len) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling, error.UnsupportedPrimitiveOp, error.UnsupportedOperation, error.UnsupportedShape => {
                self.recordResidentProjection(.text, .fallback, "text.pool.resident", batch, @errorName(err));
                if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
                const host = try encoder_outputs.materializeFloat32(self.allocator, &.{}, encoder_outputs.backend.operationToken());
                defer {
                    for (host) |*value| value.deinit();
                    self.allocator.free(host);
                }
                return try self.finishTextHostOutputs(host, mask, batch, seq_len);
            },
            else => return err,
        };
        defer pooled.deinit();

        const resident_shape = [_]i64{ @intCast(batch), @intCast(expected_dim) };
        const resident_input = [_]session_mod.ResidentInput{.{
            .value = pooled.value,
            .backend = pooled.backend,
            .estimated_bytes = std.math.mul(
                usize,
                std.math.mul(usize, batch, expected_dim) catch
                    return error.ResourceLimitExceeded,
                @sizeOf(f32),
            ) catch return error.ResourceLimitExceeded,
            .shape = &resident_shape,
        }};
        const projection_start = embedTimingStart(self.print_timing);
        std.debug.assert(projection_execution.* == null);
        projection_execution.* = try proj.admitResidentInputsBeforeExecutionWithRetirement(&resident_input, projection_shell, self.allocator, self.execution_control);
        const projection_permit = &projection_execution.*.?;
        // Cold physical bank funding must precede the destination's actual
        // provider operation; execution may only adopt already-funded storage.
        try projection_permit.preparePhysicalStorage(self.allocator);
        try projection_permit.bindPhysicalCleanupEpoch(cleanup.boundary orelse return error.MissingCurrentCleanupEpoch);
        var proj_outputs = (projection_permit.runResidentInputsWithControl(&resident_input, self.allocator, self.execution_control) catch |err| switch (err) {
            error.UnsupportedResidentInputBackend => {
                projection_permit.deinitWithCleanup(cleanup);
                projection_execution.* = null;
                return try self.finishPooledTextProjection(&pooled, mask, batch, seq_len, @errorName(err));
            },
            else => return err,
        }) orelse {
            projection_permit.deinitWithCleanup(cleanup);
            projection_execution.* = null;
            return try self.finishPooledTextProjection(&pooled, mask, batch, seq_len, "unsupported");
        };
        logEmbedTiming("text.projection.resident", batch, projection_start);
        defer proj_outputs.deinitWithBoundary(cleanup.boundary);

        const embeddings = try self.resident2DToEmbeddings(&proj_outputs, batch);
        self.recordResidentProjection(.text, .success, "text.projection.resident", batch, null);
        return embeddings;
    }

    fn tryEmbedTextResidentEncoder(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
        permit: *session_mod.RunPermit,
        cleanup: CleanupContext,
    ) !?[][]f32 {
        const encoder_start = embedTimingStart(self.print_timing);
        if (self.config.pooling == .mean) {
            if (try permit.runResidentTextEmbeddingWithControl(inputs, .{
                .pooling = .mean,
                .normalize = self.config.normalize,
            }, self.allocator, self.execution_control)) |ready_output_value| {
                var ready_outputs = ready_output_value;
                defer ready_outputs.deinitWithBoundary(cleanup.boundary);
                if (ready_outputs.outputs.len == 0) return error.NoOutputTensors;
                const gemma_width: ?usize = if (self.session.backend() == .cuda and self.text_projection == null and @import("../execution/architecture_session.zig").gemmaCudaOwnedTextWidth(self.session) != null)
                    self.config.embedding_gemma2_dimensions
                else
                    null;
                const embeddings = try self.resident2DToEmbeddingsWithWidth(&ready_outputs, batch, false, gemma_width);
                self.last_resident_backend_timing = @import("../execution/metadata_sample.zig").snapshot(ready_outputs.backend);
                logEmbedTiming("text.encoder.pool_normalize.resident", batch, encoder_start);
                self.recordResidentProjection(.text, .success, "text.encoder.pool_normalize.resident", batch, null);
                return embeddings;
            }
        }
        if (self.config.resident_embedding_gemma2 and self.session.backend() == .cuda)
            return error.UnsupportedEmbeddingGemma2Resident;
        var encoder_outputs = (try permit.runResidentWithControl(inputs, self.allocator, self.execution_control)) orelse
            return self.residentProjectionFallback(.text, "text.encoder.resident", batch, "unsupported");
        logEmbedTiming("text.encoder.resident", batch, encoder_start);
        defer encoder_outputs.deinitWithBoundary(cleanup.boundary);
        if (encoder_outputs.outputs.len == 0) return error.NoOutputTensors;

        const embeddings = self.finishResidentTextRows(&encoder_outputs, mask, batch, seq_len, self.config.normalize) catch |err| switch (err) {
            error.UnsupportedResidentTextPooling,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedOperation,
            error.UnsupportedShape,
            => {
                // The encoder has committed. A pooling capability decline
                // consumes this same hidden result; null is only pre-producer.
                self.recordResidentProjection(.text, .fallback, "text.pool.resident", batch, @errorName(err));
                if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
                const host = try encoder_outputs.materializeFloat32(self.allocator, &.{}, encoder_outputs.backend.operationToken());
                defer {
                    for (host) |*tensor| tensor.deinit();
                    self.allocator.free(host);
                }
                return try self.finishTextHostOutputs(host, mask, batch, seq_len);
            },
            else => return err,
        };
        errdefer freeEmbeddingSlices(self.allocator, embeddings);
        try self.finishGemmaTextDimensions(embeddings);
        self.last_resident_backend_timing = @import("../execution/metadata_sample.zig").snapshot(encoder_outputs.backend);
        self.recordResidentProjection(.text, .success, "text.encoder.resident", batch, null);
        return embeddings;
    }

    fn finishMediaHostOutputs(self: *EmbeddingPipeline, outputs: []Tensor, batch: usize, modality: ResidentProjectionModality) ![][]f32 {
        return switch (modality) {
            .image => self.imageEmbeddingsFromOutputs(outputs, batch),
            .audio => self.audioEmbeddingsFromOutputs(outputs, batch),
            .text => error.InvalidMediaProjection,
        };
    }

    fn finishMediaSelectedHost(self: *EmbeddingPipeline, selected: *const ResidentPooled, batch: usize, modality: ResidentProjectionModality, projection_phase: []const u8, reason: []const u8) ![][]f32 {
        self.recordResidentProjection(modality, .fallback, projection_phase, batch, reason);
        if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
        var values = [_]ops_mod.CT{selected.value};
        var borrowed = session_mod.ResidentOutputs{ .outputs = &values, .backend = selected.backend, .allocator = self.allocator };
        const host = try borrowed.materializeFloat32(self.allocator, &.{}, selected.backend.operationToken());
        defer {
            for (host) |*tensor| tensor.deinit();
            self.allocator.free(host);
        }
        return self.finishMediaHostOutputs(host, batch, modality);
    }

    fn tryEmbedResidentProjection(
        self: *EmbeddingPipeline,
        inputs: []const Tensor,
        proj: backends.Session,
        modality: ResidentProjectionModality,
        batch: usize,
        encoder_phase: []const u8,
        projection_phase: []const u8,
        source: *session_mod.PreparedSource,
    ) !?[][]f32 {
        const expected_dim = projectionInputDim(proj) orelse
            return self.residentProjectionFallback(modality, projection_phase, batch, "unknown_projection_input_dim");
        const encoder_start = embedTimingStart(self.print_timing);
        var encoder_outputs = try source.runResident(.{ .host = inputs });
        if (encoder_outputs == null) return self.residentProjectionFallback(modality, encoder_phase, batch, "unsupported");
        logEmbedTiming(encoder_phase, batch, encoder_start);
        defer if (encoder_outputs) |*outputs| outputs.deinitWithBoundary(source.epoch());
        if (encoder_outputs.?.outputs.len == 0) return error.NoOutputTensors;
        // Complete source ownership is resolved before selecting a derived CT.
        if (!try source.handoffResident(&encoder_outputs.?)) {
            self.recordResidentProjection(modality, .fallback, encoder_phase, batch, "source_not_detachable");
            if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
            const host = try source.materializeResident(&encoder_outputs);
            defer {
                for (host) |*tensor| tensor.deinit();
                self.allocator.free(host);
            }
            return try self.finishMediaHostOutputs(host, batch, modality);
        }
        var selected = self.selectResidentProjectedInput(&encoder_outputs.?, expected_dim, batch) catch |err| switch (err) {
            error.UnsupportedResidentProjectionInput, error.UnsupportedPrimitiveOp, error.UnsupportedOperation, error.UnsupportedShape => {
                self.recordResidentProjection(modality, .fallback, encoder_phase, batch, @errorName(err));
                if (self.residentProjectionRequired()) return error.ResidentEmbeddingFallback;
                const host = try source.materializeResident(&encoder_outputs);
                defer {
                    for (host) |*tensor| tensor.deinit();
                    self.allocator.free(host);
                }
                return try self.finishMediaHostOutputs(host, batch, modality);
            },
            else => return err,
        };
        defer selected.deinit();
        const resident_shape = [_]i64{ @intCast(batch), @intCast(expected_dim) };
        const resident_input = [_]session_mod.ResidentInput{.{
            .value = selected.value,
            .backend = selected.backend,
            .estimated_bytes = try std.math.mul(usize, try std.math.mul(usize, batch, expected_dim), @sizeOf(f32)),
            .shape = &resident_shape,
        }};
        var destination: session_mod.PreparedSource = .{};
        defer destination.deinit();
        try destination.initBeforeExecutionInto(proj, proj, self.execution_lock, .{ .resident = &resident_input }, .{ .batch = batch, .sequence = 1, .input_bytes = resident_input[0].estimated_bytes }, null, .resident_consumer, self.allocator, self.execution_control);
        try destination.enter();
        const projection_start = embedTimingStart(self.print_timing);
        var proj_outputs = (destination.runResident(.{ .resident = &resident_input }) catch |err| switch (err) {
            error.UnsupportedResidentInputBackend => {
                destination.deinit();
                return try self.finishMediaSelectedHost(&selected, batch, modality, projection_phase, @errorName(err));
            },
            else => return err,
        }) orelse {
            destination.deinit();
            return try self.finishMediaSelectedHost(&selected, batch, modality, projection_phase, "unsupported");
        };
        defer proj_outputs.deinitWithBoundary(destination.epoch());
        logEmbedTiming(projection_phase, batch, projection_start);
        const embeddings = try self.resident2DToEmbeddings(&proj_outputs, batch);
        self.recordResidentProjection(modality, .success, projection_phase, batch, null);
        return embeddings;
    }

    fn selectResidentProjectedInput(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        expected_dim: usize,
        expected_batch: usize,
    ) !ResidentPooled {
        return @import("../execution/backend_operation.zig").callOpaque(outputs.backend.*, outputs.backend.operationToken(), selectResidentProjectedInputImpl, .{ self, outputs, expected_dim, expected_batch });
    }

    fn selectResidentProjectedInputImpl(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        expected_dim: usize,
        expected_batch: usize,
    ) !ResidentPooled {
        _ = self;
        for (outputs.outputs) |output| {
            const shape = try outputs.backend.tensorShape(output, outputs.allocator);
            defer outputs.allocator.free(shape);
            if (shape.len == 2 and
                shape[0] == @as(i64, @intCast(expected_batch)) and
                shape[1] == @as(i64, @intCast(expected_dim)))
            {
                return .{ .value = output, .backend = outputs.backend, .owns_value = false };
            }
        }
        for (outputs.outputs) |output| {
            const shape = try outputs.backend.tensorShape(output, outputs.allocator);
            defer outputs.allocator.free(shape);
            if (shape.len == 3 and
                shape[0] == @as(i64, @intCast(expected_batch)) and
                shape[2] == @as(i64, @intCast(expected_dim)))
            {
                if (shape[1] <= 0) return error.ShapeMismatch;
                return try residentClsPool(outputs.allocator, outputs.backend, output, expected_batch, @intCast(shape[1]), expected_dim);
            }
        }
        return error.UnsupportedResidentProjectionInput;
    }

    fn finishResidentTextRows(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool) ![][]f32 {
        return self.finishResidentTextRowsWithCleanup(outputs, mask, batch, sequence, normalize, null);
    }
    fn finishResidentTextRowsWithCleanup(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool, cleanup: ?*const CleanupContext) ![][]f32 {
        return @import("../execution/backend_operation.zig").callOpaque(outputs.backend.*, if (cleanup) |current| current.operation else outputs.backend.operationToken(), finishResidentTextRowsImpl, .{ self, outputs, mask, batch, sequence, normalize, cleanup });
    }

    fn finishResidentTextRowsImpl(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, mask: []const i32, batch: usize, sequence: usize, normalize: bool, cleanup: ?*const CleanupContext) ![][]f32 {
        if (outputs.outputs.len == 0) return error.NoOutputTensors;
        const rows = try sequence_pool.finishHostRowsWithCleanup(self.allocator, outputs.backend, outputs.outputs[0], mask, batch, sequence, switch (self.config.pooling) {
            .mean => .mean,
            .cls => .cls,
            .max => .max,
            .last => .last,
        }, normalize, cleanup);
        errdefer freeEmbeddingSlices(self.allocator, rows);
        if (self.config.resident_embedding_gemma2) for (rows) |row| try validateEmbeddingGemma2Vector(row);
        return rows;
    }

    fn residentPoolTextOutput(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
    ) !ResidentPooled {
        return @import("../execution/backend_operation.zig").callOpaque(outputs.backend.*, outputs.backend.operationToken(), residentPoolTextOutputImpl, .{ self, outputs, mask, batch, seq_len });
    }

    fn residentPoolTextOutputImpl(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        mask: []const i32,
        batch: usize,
        seq_len: usize,
    ) !ResidentPooled {
        if (outputs.outputs.len == 0) return error.NoOutputTensors;
        return sequence_pool.poolOutput(outputs.allocator, outputs.backend, outputs.outputs[0], mask, batch, seq_len, switch (self.config.pooling) {
            .mean => .mean,
            .cls => .cls,
            .max => .max,
            .last => .last,
        });
    }

    const residentMaskedMeanPool = sequence_pool.maskedMean;

    const residentClsPool = sequence_pool.cls;

    const residentLastTokenPool = sequence_pool.last;

    fn resident2DToEmbeddings(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        batch: usize,
    ) ![][]f32 {
        return self.resident2DToEmbeddingsWithNormalization(outputs, batch, self.config.normalize);
    }

    fn resident2DToEmbeddingsWithNormalization(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        batch: usize,
        normalize: bool,
    ) ![][]f32 {
        return self.resident2DToEmbeddingsWithWidth(outputs, batch, normalize, null);
    }
    fn resident2DToEmbeddingsWithWidth(self: *EmbeddingPipeline, outputs: *session_mod.ResidentOutputs, batch: usize, normalize: bool, width: ?usize) ![][]f32 {
        return @import("../execution/backend_operation.zig").callOpaque(outputs.backend.*, outputs.backend.operationToken(), resident2DToEmbeddingsWithNormalizationImpl, .{ self, outputs, batch, normalize, width });
    }

    fn resident2DToEmbeddingsWithNormalizationImpl(
        self: *EmbeddingPipeline,
        outputs: *session_mod.ResidentOutputs,
        batch: usize,
        normalize: bool,
        requested_width: ?usize,
    ) ![][]f32 {
        if (outputs.outputs.len == 0) return error.NoOutputTensors;
        const shape = try outputs.backend.tensorShape(outputs.outputs[0], self.allocator);
        defer self.allocator.free(shape);
        if (shape.len == 0) return error.UnexpectedOutputShape;

        const proj_dim: usize = @intCast(shape[shape.len - 1]);
        if (proj_dim == 0) return error.ShapeMismatch;
        const output_width = requested_width orelse proj_dim;
        if (output_width == 0 or output_width > proj_dim) return error.ShapeMismatch;
        const resident_output = if (normalize)
            try resident_ops.l2NormalizeLastDim(self.allocator, outputs.backend, outputs.outputs[0], shape)
        else
            outputs.outputs[0];
        defer if (normalize) outputs.backend.free(resident_output);

        const data = try outputs.backend.toFloat32(resident_output, self.allocator);
        defer self.allocator.free(data);
        if (data.len != batch * proj_dim) return error.ShapeMismatch;

        const embeddings = try self.allocator.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |embedding| self.allocator.free(embedding);
            self.allocator.free(embeddings);
        }

        for (0..batch) |b| {
            const embedding = try self.allocator.alloc(f32, output_width);
            @memcpy(embedding, data[b * proj_dim ..][0..output_width]);
            // Default full-width output is byte-identical. Reduced Gemma
            // dimensions use the existing host normalizer on the F32 prefix.
            if (output_width != proj_dim and (self.config.normalize or self.config.resident_embedding_gemma2)) linalg.l2Normalize(embedding, output_width);
            embeddings[b] = embedding;
            initialized += 1;
            if (self.config.resident_embedding_gemma2) try validateEmbeddingGemma2Vector(embedding);
        }

        return embeddings;
    }

    fn extractVision3D(self: *EmbeddingPipeline, output: *const Tensor, batch: usize, normalize: bool) ![][]f32 {
        const hidden: usize = @intCast(output.shape[2]);
        const seq_len: usize = @intCast(output.shape[1]);
        const data = output.asFloat32();

        const alloc = self.allocator;
        const embeddings = try alloc.alloc([]f32, batch);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |e| alloc.free(e);
            alloc.free(embeddings);
        }

        for (0..batch) |b| {
            const emb = try alloc.alloc(f32, hidden);
            const offset = b * seq_len * hidden;
            @memcpy(emb, data[offset .. offset + hidden]);
            if (normalize and self.config.normalize) {
                linalg.l2Normalize(emb, hidden);
            }
            embeddings[b] = emb;
            initialized += 1;
        }

        return embeddings;
    }
};

fn validateEmbeddingGemma2Vector(values: []const f32) !void {
    var squares: f64 = 0;
    for (values) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteEmbeddingOutput;
        squares += @as(f64, value) * value;
    }
    if (squares == 0) return error.ZeroEmbeddingOutput;
}

test "EmbeddingGemma2 rejects nonfinite and zero output vectors" {
    try validateEmbeddingGemma2Vector(&.{ 0.6, 0.8 });
    try std.testing.expectError(error.ZeroEmbeddingOutput, validateEmbeddingGemma2Vector(&.{ 0, 0 }));
    try std.testing.expectError(error.NonFiniteEmbeddingOutput, validateEmbeddingGemma2Vector(&.{std.math.nan(f32)}));
    try std.testing.expectError(error.NonFiniteEmbeddingOutput, validateEmbeddingGemma2Vector(&.{std.math.inf(f32)}));
}

fn textInputTensorSet(
    alloc: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    input_ids_tensor: Tensor,
    attention_mask_tensor: Tensor,
    shape: []const i64,
) !TextInputTensorSet {
    return text_inputs.build(alloc, input_info, input_ids_tensor, attention_mask_tensor, shape, .{});
}

fn expectTextInputNames(input_info: []const backends.TensorInfo, expected: []const []const u8) !void {
    const alloc = std.testing.allocator;
    const shape = [_]i64{ 1, 2 };
    var input_ids = try Tensor.initInt64(alloc, "input_ids", &shape, &.{ 101, 102 });
    defer input_ids.deinit();
    var attention_mask = try Tensor.initInt64(alloc, "attention_mask", &shape, &.{ 1, 1 });
    defer attention_mask.deinit();

    var set = try textInputTensorSet(alloc, input_info, input_ids, attention_mask, &shape);
    defer set.deinit();

    const inputs = set.slice();
    try std.testing.expectEqual(expected.len, inputs.len);
    for (expected, 0..) |name, i| {
        try std.testing.expectEqualStrings(name, inputs[i].name);
    }
}

fn textSequenceLengthForInputs(input_info: []const backends.TensorInfo, default_len: usize) usize {
    for (input_info) |info| {
        if (!std.mem.eql(u8, info.name, "input_ids")) continue;
        if (info.shape.len < 2 or info.shape[1] <= 0) return default_len;
        return @intCast(info.shape[1]);
    }
    return default_len;
}

fn hasFixedTextSequenceLength(input_info: []const backends.TensorInfo) bool {
    for (input_info) |info| {
        if (!std.mem.eql(u8, info.name, "input_ids")) continue;
        return info.shape.len >= 2 and info.shape[1] > 0;
    }
    return false;
}

test "clap aggregate feature working set is checked before allocation" {
    const frames = audio.CLAP_CONFIG.chunk_length_s * audio.CLAP_CONFIG.sample_rate / audio.CLAP_CONFIG.hop_length + 1;
    const mels = audio.CLAP_CONFIG.n_mels;
    const one = try clapBatchFeatureWorkingSet(1, 1, frames, mels);
    try std.testing.expectEqual(frames * mels, one.elements);
    try ensureClapWorkingSet(0, one.reserve_bytes, audio.default_decode_working_bytes);

    const feature_bytes_per_clip = frames * mels * @sizeOf(f32);
    const oversized_batch = audio.default_decode_working_bytes / (2 * feature_bytes_per_clip) + 1;
    const oversized = try clapBatchFeatureWorkingSet(oversized_batch, 1, frames, mels);
    try std.testing.expectError(
        error.AudioTooLarge,
        ensureClapWorkingSet(0, oversized.reserve_bytes, audio.default_decode_working_bytes),
    );
    try std.testing.expectError(
        error.AudioTooLarge,
        ensureClapWorkingSet(audio.default_decode_working_bytes, one.reserve_bytes, audio.default_decode_working_bytes),
    );
    try std.testing.expectError(
        error.AudioTooLarge,
        clapBatchFeatureWorkingSet(std.math.maxInt(usize), 2, frames, mels),
    );
}

test "embedding text inputs follow declared model arity" {
    try expectTextInputNames(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
    }, &.{"input_ids"});

    try expectTextInputNames(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
    }, &.{ "input_ids", "attention_mask" });

    try expectTextInputNames(&.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "token_type_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
    }, &.{ "input_ids", "attention_mask", "token_type_ids" });
}

test "embedding text length follows fixed input_ids sequence dimension" {
    const input_info = [_]backends.TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, 77 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, 77 } },
    };
    try std.testing.expect(hasFixedTextSequenceLength(&input_info));
    try std.testing.expectEqual(@as(usize, 77), textSequenceLengthForInputs(&input_info, 512));
}

test "embedding text plans backend and fixed-shape batches" {
    const dynamic = fakeTextSession(.native, &.{ -1, 77 });
    try std.testing.expect(textSessionBatchPlan(dynamic, 2) == null);

    const fixed_one = fakeTextSession(.native, &.{ 1, 77 });
    try std.testing.expect(textSessionBatchPlan(fixed_one, 1) == null);
    try std.testing.expectEqual(
        TextSessionBatchPlan{ .batch_size = 1, .pad_final_batch = true },
        textSessionBatchPlan(fixed_one, 2).?,
    );

    const fixed_two = fakeTextSession(.native, &.{ 2, 77 });
    try std.testing.expectEqual(
        TextSessionBatchPlan{ .batch_size = 2, .pad_final_batch = true },
        textSessionBatchPlan(fixed_two, 1).?,
    );
    try std.testing.expect(textSessionBatchPlan(fixed_two, 2) == null);
    try std.testing.expectEqual(
        TextSessionBatchPlan{ .batch_size = 2, .pad_final_batch = true },
        textSessionBatchPlan(fixed_two, 3).?,
    );

    const dynamic_info = [_]backends.TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, 77 } },
    };
    try std.testing.expect(textSessionBatchPlanForRuntime(&dynamic_info, .native, true, 3) == null);
    try std.testing.expect(textSessionBatchPlanForRuntime(&dynamic_info, .metal, true, 3) == null);
}

fn sessionHasInput(session: backends.Session, name: []const u8) bool {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return true;
    }
    return false;
}

pub const TextSessionBatchPlan = struct {
    batch_size: usize,
    pad_final_batch: bool,
};

/// Return an execution plan only when the requested batch cannot be sent to
/// the session directly. Fixed-shape models are processed at their declared
/// batch size, padding only the final partial chunk. External ONNX sessions
/// and non-native imported graphs retain the conservative single-row path;
/// imported native graphs preserve runtime batch dimensions and execute whole.
pub fn textSessionBatchPlan(session: backends.Session, requested_batch: usize) ?TextSessionBatchPlan {
    return textSessionBatchPlanForRuntime(
        session.inputInfo(),
        session.backend(),
        backends.imported_onnx_session.sharedBackendContext(session) != null,
        requested_batch,
    );
}

fn textSessionBatchPlanForRuntime(
    input_info: []const backends.TensorInfo,
    backend: backends.BackendType,
    imported: bool,
    requested_batch: usize,
) ?TextSessionBatchPlan {
    if (requested_batch == 0) return null;
    for (input_info) |info| {
        if (!std.mem.eql(u8, info.name, "input_ids")) continue;
        if (info.shape.len == 0) break;
        const declared_batch = info.shape[0];
        if (declared_batch > 0) {
            const batch_size: usize = @intCast(declared_batch);
            if (batch_size == requested_batch) return null;
            return .{ .batch_size = batch_size, .pad_final_batch = true };
        }
        break;
    }
    if (requested_batch <= 1) return null;
    if (imported and backend != .native and backend != .metal) {
        return .{ .batch_size = 1, .pad_final_batch = false };
    }
    return null;
}

fn shouldFallbackBatchedImageError(err: anyerror) bool {
    return switch (err) {
        error.ShapeMismatch,
        error.UnexpectedOutputShape,
        error.UnsupportedShape,
        error.InvalidInputShape,
        error.InvalidTensorShape,
        => true,
        else => false,
    };
}

fn tensorHasBatchRows(tensor: *const Tensor, expected_batch: usize) bool {
    if (tensor.shape.len < 2 or tensor.shape[0] <= 0) return true;
    return @as(usize, @intCast(tensor.shape[0])) >= expected_batch;
}

fn outputsContainBatchRows(outputs: []const Tensor, expected_batch: usize) bool {
    for (outputs) |*output| {
        if (output.shape.len >= 2 and tensorHasBatchRows(output, expected_batch)) return true;
    }
    return false;
}

fn fakeTextSession(comptime backend_type: backends.BackendType, comptime input_shape: []const i64) backends.Session {
    const VTable = struct {
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.TestUnexpectedResult;
        }

        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "input_ids", .dtype = .i64, .shape = input_shape }};
        }

        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }

        fn backend(_: *anyopaque) backends.BackendType {
            return backend_type;
        }

        fn close(_: *anyopaque) void {}
    };
    const state = struct {
        var value: u8 = 0;
    };
    return .{
        .ptr = @ptrCast(&state.value),
        .vtable = &.{
            .run = VTable.run,
            .inputInfo = VTable.inputInfo,
            .outputInfo = VTable.outputInfo,
            .backend = VTable.backend,
            .close = VTable.close,
        },
    };
}

fn embedTimingEnabled(explicit: bool) bool {
    if (explicit) return true;
    return platform.env.getenvBoolDefault("TERMITE_EMBED_TIMING", false);
}

fn embedTimingNowNs() u128 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

fn embedTimingStart(explicit: bool) u128 {
    if (!embedTimingEnabled(explicit)) return 0;
    return embedTimingNowNs();
}

fn logEmbedTiming(phase: []const u8, count: usize, start_ns: u128) void {
    if (start_ns == 0) return;
    const now = embedTimingNowNs();
    const elapsed_us = if (now > start_ns) @divTrunc(now - start_ns, 1000) else 0;
    std.log.info("antfly inference embed timing phase={s} count={d} elapsed_us={d}", .{ phase, count, elapsed_us });
}

fn logEmbedFallback(explicit: bool, phase: []const u8, count: usize) void {
    if (!embedTimingEnabled(explicit)) return;
    std.log.info("antfly inference embed fallback phase={s} count={d}", .{ phase, count });
}

fn logEmbedResidentSuccess(modality: ResidentProjectionModality, phase: []const u8, count: usize) void {
    if (!embedTimingEnabled(false) and !embedResidentFailClosedEnabled()) return;
    std.log.info("antfly inference embed resident outcome=success modality={s} phase={s} count={d}", .{
        @tagName(modality),
        phase,
        count,
    });
}

fn logEmbedResidentFallback(modality: ResidentProjectionModality, phase: []const u8, count: usize, reason: []const u8) void {
    if (!embedTimingEnabled(false) and !embedResidentFailClosedEnabled()) return;
    std.log.warn("antfly inference embed resident outcome=fallback modality={s} phase={s} count={d} reason={s}", .{
        @tagName(modality),
        phase,
        count,
        reason,
    });
}

fn embedResidentFailClosedEnabled() bool {
    if (platform.env.getenvSlice("TERMITE_EMBED_RESIDENT_FAIL_CLOSED")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    if (platform.env.getenvSlice("TERMITE_EMBED_RESIDENT_REQUIRED")) |value| {
        if (envFlagEnabled(value)) return true;
    }
    return false;
}

fn residentQwen3EmbeddingEligible(session: backends.Session, cfg: gpt_arch.Config, embedding_config: EmbeddingConfig) bool {
    return residentQwen3EmbeddingEligibleForBackend(session.backend(), cfg, embedding_config);
}

fn qwenWorkspaceLaneModel(backend: backends.BackendType, cfg: gpt_arch.Config) bool {
    return backend == .metal and gpt_arch.denseQwen3PrefillEligible(cfg);
}

fn residentQwen3EmbeddingEligibleForBackend(backend: backends.BackendType, cfg: gpt_arch.Config, embedding_config: EmbeddingConfig) bool {
    if (!embedding_config.resident_qwen3_embedding) return false;
    if (backend != .metal) return false;
    return gpt_arch.denseQwen3PrefillEligible(cfg);
}

fn envFlagEnabled(value: []const u8) bool {
    return value.len > 0 and
        !std.mem.eql(u8, value, "0") and
        !std.ascii.eqlIgnoreCase(value, "false") and
        !std.ascii.eqlIgnoreCase(value, "off") and
        !std.ascii.eqlIgnoreCase(value, "no");
}

fn logEmbedTensorShapes(explicit: bool, label: []const u8, tensors: []const Tensor) void {
    if (!embedTimingEnabled(explicit)) return;
    for (tensors, 0..) |tensor, i| {
        std.log.info("antfly inference embed tensor label={s} index={d} name={s} shape={any} data_len={d}", .{
            label,
            i,
            tensor.name,
            tensor.shape,
            tensor.asFloat32().len,
        });
    }
}

fn logEmbedSelectedTensor(explicit: bool, label: []const u8, tensor: *const Tensor) void {
    if (!embedTimingEnabled(explicit)) return;
    std.log.info("antfly inference embed selected label={s} name={s} shape={any} data_len={d}", .{
        label,
        tensor.name,
        tensor.shape,
        tensor.asFloat32().len,
    });
}

fn clapInputFeatureFrames(session: backends.Session, default_frames: usize) usize {
    for (session.inputInfo()) |info| {
        if (!std.mem.eql(u8, info.name, "input_features")) continue;
        if (info.shape.len >= 3 and info.shape[2] > 0) {
            return @max(default_frames, @as(usize, @intCast(info.shape[2])));
        }
    }

    for (session.outputInfo()) |info| {
        if (info.shape.len < 2 or info.shape[1] <= 0) continue;
        const dim = @as(usize, @intCast(info.shape[1]));
        if (dim > default_frames and dim <= 4096 and dim % 32 == 0) {
            return dim;
        }
    }

    return default_frames;
}

fn freeEmbeddingSlices(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |emb| allocator.free(emb);
    allocator.free(embeddings);
}

const TextBatchRunner = *const fn (
    ctx: *anyopaque,
    texts: []const []const u8,
) anyerror![][]f32;

/// Preserve the requested batch on the fast path, but split an over-budget
/// dynamic batch until it fits. A single-row resource failure is returned
/// unchanged so a model that cannot fit at all still fails closed.
fn embedTextBatchAdaptive(
    allocator: std.mem.Allocator,
    ctx: *anyopaque,
    texts: []const []const u8,
    run_batch: TextBatchRunner,
) anyerror![][]f32 {
    return @import("text_batch_admission.zig").run(allocator, ctx, texts, run_batch, true);
}

test "embedding text batch adaptively splits resource-limited requests" {
    const allocator = std.testing.allocator;
    const Probe = struct {
        allocator: std.mem.Allocator,
        max_batch: usize,
        calls: usize = 0,
        largest_success: usize = 0,

        fn run(ctx: *anyopaque, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (texts.len > self.max_batch) return error.ResourceLimitExceeded;
            self.largest_success = @max(self.largest_success, texts.len);

            const vectors = try self.allocator.alloc([]f32, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (vectors[0..initialized]) |vector| self.allocator.free(vector);
                self.allocator.free(vectors);
            }
            for (texts, 0..) |text, index| {
                vectors[index] = try self.allocator.dupe(f32, &.{@floatFromInt(text.len)});
                initialized += 1;
            }
            return vectors;
        }
    };

    var probe = Probe{ .allocator = allocator, .max_batch = 1 };
    const texts = [_][]const u8{ "a", "bb", "ccc", "dddd" };
    const vectors = try embedTextBatchAdaptive(allocator, &probe, &texts, Probe.run);
    defer freeEmbeddingSlices(allocator, vectors);

    try std.testing.expectEqual(@as(usize, 4), vectors.len);
    try std.testing.expectEqualSlices(f32, &.{1}, vectors[0]);
    try std.testing.expectEqualSlices(f32, &.{2}, vectors[1]);
    try std.testing.expectEqualSlices(f32, &.{3}, vectors[2]);
    try std.testing.expectEqualSlices(f32, &.{4}, vectors[3]);
    try std.testing.expectEqual(@as(usize, 1), probe.largest_success);
    try std.testing.expectEqual(@as(usize, 7), probe.calls);
}

test "embedding text batch preserves a single-row resource limit" {
    const Probe = struct {
        fn run(_: *anyopaque, _: []const []const u8) ![][]f32 {
            return error.ResourceLimitExceeded;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        embedTextBatchAdaptive(
            std.testing.allocator,
            &context,
            &.{"too large"},
            Probe.run,
        ),
    );
}

const ProjectionSelection = struct {
    tensor: *const Tensor,
    use_cls_pool: bool,
};

fn projectionInputDim(session: backends.Session) ?usize {
    const inputs = session.inputInfo();
    if (inputs.len == 0) return null;
    const shape = inputs[0].shape;
    if (shape.len == 0) return null;
    const dim = shape[shape.len - 1];
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn selectProjectedOutput(outputs: []Tensor, expected_dim: usize, expected_batch: usize) ?ProjectionSelection {
    for (outputs) |*output| {
        if (output.shape.len == 2 and output.shape[1] > 0 and @as(usize, @intCast(output.shape[1])) == expected_dim and tensorHasBatchRows(output, expected_batch)) {
            return .{ .tensor = output, .use_cls_pool = false };
        }
    }
    for (outputs) |*output| {
        if (output.shape.len == 3 and output.shape[2] > 0 and @as(usize, @intCast(output.shape[2])) == expected_dim and tensorHasBatchRows(output, expected_batch)) {
            return .{ .tensor = output, .use_cls_pool = true };
        }
    }
    return null;
}

test "embedding execution gate owns the supplied mutex" {
    var gate: std.atomic.Mutex = .unlocked;
    var pipeline = EmbeddingPipeline{
        .allocator = undefined,
        .session = undefined,
        .tok = undefined,
        .config = .{},
        .execution_lock = &gate,
    };

    try pipeline.lockExecution();
    try std.testing.expect(!gate.tryLock());
    pipeline.unlockExecution();

    try std.testing.expect(gate.tryLock());
    gate.unlock();
}

test "projectEmbeddings runs projection as a single batch" {
    const allocator = std.testing.allocator;

    const first = try allocator.dupe(f32, &.{ 1.0, 2.0 });
    errdefer allocator.free(first);
    const second = try allocator.dupe(f32, &.{ 3.0, 4.0 });
    errdefer allocator.free(second);
    const third = try allocator.dupe(f32, &.{ 5.0, 6.0 });
    errdefer allocator.free(third);

    var embeddings = try allocator.alloc([]f32, 3);
    defer allocator.free(embeddings);
    embeddings[0] = first;
    embeddings[1] = second;
    embeddings[2] = third;
    defer {
        for (embeddings) |embedding| allocator.free(embedding);
    }

    var fake = FakeProjectionSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = false },
    };

    try pipeline.projectEmbeddings(embeddings, fake.session());

    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(@as(usize, 3), fake.last_batch);
    try std.testing.expectEqual(@as(usize, 2), fake.last_dim);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0, 7.0 }, embeddings[1]);
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 6.0, 11.0 }, embeddings[2]);
}

test "resident fail closed env flag parser accepts explicit opt in only" {
    try std.testing.expect(envFlagEnabled("1"));
    try std.testing.expect(envFlagEnabled("true"));
    try std.testing.expect(envFlagEnabled("yes"));
    try std.testing.expect(!envFlagEnabled(""));
    try std.testing.expect(!envFlagEnabled("0"));
    try std.testing.expect(!envFlagEnabled("false"));
    try std.testing.expect(!envFlagEnabled("off"));
    try std.testing.expect(!envFlagEnabled("no"));
}

test "resident projection stats records success fallback and fail closed" {
    var stats = AtomicResidentProjectionStats{};
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .resident_projection_required = true },
        .resident_projection_stats = &stats,
    };

    pipeline.recordResidentProjection(.text, .success, "text.projection.resident", 2, null);
    var snapshot = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.text_success);
    try std.testing.expectEqual(@as(u64, 0), snapshot.text_fallback);

    try std.testing.expectError(
        error.ResidentEmbeddingFallback,
        pipeline.residentProjectionFallback(.image, "image.encoder.resident", 2, "unsupported"),
    );
    snapshot = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 1), snapshot.image_fallback);
}

test "resident masked mean pooling uses backend primitives" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer weight_store.deinitOwned();
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const input = try cb.fromFloat32Shape(&values, &.{ 2, 3, 2 });
    defer cb.free(input);

    const mask = [_]i32{ 1, 1, 0, 1, 0, 0 };
    const pooled = try EmbeddingPipeline.residentMaskedMeanPool(allocator, &cb, input, &mask, 2, 3, 2);
    defer pooled.deinit();

    const shape = try cb.tensorShape(pooled.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(pooled.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 2.0, 3.0, 7.0, 8.0 }, host);
}

test "resident text pooling handles flattened batch sequence hidden states" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer weight_store.deinitOwned();
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 6, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .pooling = .last, .normalize = false },
    };

    const mask = [_]i32{ 1, 1, 0, 1, 0, 0 };
    const pooled = try pipeline.residentPoolTextOutput(&resident_outputs, &mask, 2, 3);
    defer pooled.deinit();

    const shape = try cb.tensorShape(pooled.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(pooled.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0, 7.0, 8.0 }, host);
}

test "pool3D last uses final non-padding token and normalizes" {
    const allocator = std.testing.allocator;

    const shape = [_]i64{ 2, 3, 2 };
    const values = [_]f32{
        1.0, 0.0,
        3.0, 4.0,
        9.0, 9.0,
        0.0, 2.0,
        8.0, 8.0,
        0.0, 5.0,
    };
    var output = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, &values);
    defer output.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .pooling = .last, .normalize = true },
    };

    const mask = [_]i32{ 1, 1, 0, 1, 0, 1 };
    const embeddings = try pipeline.pool3D(&output, &mask, 2, 3, true);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectApproxEqAbs(@as(f32, 0.6), embeddings[0][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), embeddings[0][1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), embeddings[1][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), embeddings[1][1], 1e-6);
}

test "active token length trims trailing padding but keeps at least one token" {
    try std.testing.expectEqual(@as(usize, 3), EmbeddingPipeline.activeTokenLength(&.{ 1, 1, 1, 0, 0 }));
    try std.testing.expectEqual(@as(usize, 4), EmbeddingPipeline.activeTokenLength(&.{ 1, 0, 0, 1, 0 }));
    try std.testing.expectEqual(@as(usize, 1), EmbeddingPipeline.activeTokenLength(&.{ 0, 0, 0 }));
}

test "resident qwen3 embedding eligibility accepts dense metal qwen3 only" {
    var cfg = gpt_arch.Config{
        .family = .qwen3,
        .norm_type = .rms_norm,
        .position_encoding = .rope,
        .activation = .silu,
        .hidden_size = 1024,
        .num_hidden_layers = 28,
        .num_attention_heads = 16,
        .num_key_value_heads = 8,
        .intermediate_size = 3072,
    };

    const embedding_cfg = EmbeddingConfig{
        .pooling = .last,
        .text_prefix = "Document: ",
        .trim_padding_to_batch_max = true,
        .resident_qwen3_embedding = true,
    };

    try std.testing.expect(residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, .{}));
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.native, cfg, embedding_cfg));

    cfg.family = .qwen3_5;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.family = .qwen3;

    cfg.num_local_experts = 4;
    cfg.num_experts_per_tok = 2;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.num_local_experts = 0;
    cfg.num_experts_per_tok = 0;

    cfg.image_token_index = 151655;
    cfg.mm_tokens_per_image = 256;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.image_token_index = -1;
    cfg.mm_tokens_per_image = 0;

    cfg.ple_hidden_size = 1024;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
    cfg.ple_hidden_size = 0;

    cfg.sliding_window = 4096;
    try std.testing.expect(!residentQwen3EmbeddingEligibleForBackend(.metal, cfg, embedding_cfg));
}

test "resident qwen3 embedding requires a live guard before backend preparation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const Probe = struct {
        armed: bool = false,
        disarms: usize = 0,
        fn backend(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.armed = true;
            return 1;
        }
        fn disarm(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.armed);
            self.armed = false;
            self.disarms += 1;
        }
    };
    var probe = Probe{};
    var vtable: backends.Session.VTable = undefined;
    vtable.backend = Probe.backend;
    vtable.interruption = null;
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = .{ .ptr = &probe, .vtable = &vtable },
        .tok = undefined,
        .config = .{ .resident_qwen3_embedding = true },
        .execution_control = .{ .deadline_ns = 0 },
    };
    const cfg = gpt_arch.Config{ .hidden_size = 4, .num_hidden_layers = 1, .num_attention_heads = 1, .intermediate_size = 8 };
    try std.testing.expectError(error.Timeout, pipeline.embedTextResidentQwen3(cfg, &.{1}, &.{1}, 1, 1, .{ .outside_execution = true }, null, null));
    pipeline.execution_control = .{};
    try std.testing.expectError(error.ProcessIsolationRequired, pipeline.embedTextResidentQwen3(cfg, &.{1}, &.{1}, 1, 1, .{ .outside_execution = true }, null, null));
    pipeline.execution_control = .{ .hard_cancellation = .{ .ptr = &probe, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm } };
    // Even backend construction failure must release the pipeline's guard.
    try std.testing.expectError(error.UnsupportedSessionComputeBackend, pipeline.embedTextResidentQwen3(cfg, &.{1}, &.{1}, 1, 1, .{ .outside_execution = true }, null, null));
    try std.testing.expectEqual(@as(usize, 1), probe.disarms);
    try std.testing.expect(!probe.armed);
}

test "resident projected input selection supports 3d cls pooling" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer weight_store.deinitOwned();
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const values = [_]f32{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 2, 3, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = false },
    };

    const selected = try pipeline.selectResidentProjectedInput(&resident_outputs, 2, 2);
    defer selected.deinit();

    const shape = try cb.tensorShape(selected.value, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, shape);

    const host = try cb.toFloat32(selected.value, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 7.0, 8.0 }, host);
}

test "resident 2d embedding extraction normalizes before host readback" {
    const allocator = std.testing.allocator;
    const native_mod = @import("../ops/native_compute.zig");

    var weight_store = native_mod.WeightStore{
        .allocator = allocator,
        .resident_weights = .empty,
        .lazy_weights = .empty,
    };
    defer weight_store.deinitOwned();
    var compute = native_mod.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    const values = [_]f32{ 3.0, 4.0, 0.0, 0.0 };
    const output_ct = try cb.fromFloat32Shape(&values, &.{ 2, 2 });
    const output_storage = try allocator.alloc(ops_mod.CT, 1);
    output_storage[0] = output_ct;
    var resident_outputs = session_mod.ResidentOutputs{
        .outputs = output_storage,
        .backend = &cb,
        .allocator = allocator,
    };
    defer resident_outputs.deinit();

    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .normalize = true },
    };

    const embeddings = try pipeline.resident2DToEmbeddings(&resident_outputs, 2);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectApproxEqAbs(@as(f32, 0.6), embeddings[0][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), embeddings[0][1], 1e-6);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 0.0 }, embeddings[1]);
}

test "projected output selection prefers pooled 2D encoder output" {
    const allocator = std.testing.allocator;

    const hidden_data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var hidden = try Tensor.initFloat32(allocator, "last_hidden_state", &.{ 1, 2, 2 }, &hidden_data);
    defer hidden.deinit();
    const pooled_data = [_]f32{ 5.0, 6.0 };
    var pooled = try Tensor.initFloat32(allocator, "pooler_output", &.{ 1, 2 }, &pooled_data);
    defer pooled.deinit();

    var outputs = [_]Tensor{ hidden, pooled };
    const selection = selectProjectedOutput(outputs[0..], 2, 1) orelse return error.TestExpectedEqual;

    try std.testing.expect(!selection.use_cls_pool);
    try std.testing.expectEqualStrings("pooler_output", selection.tensor.name);
}

test "embedImages uses one vision session run for an image batch" {
    const allocator = std.testing.allocator;

    var fake = FakeVisionBatchSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false, .image_size = 2 },
        .vision_session = fake.session(),
    };

    const images = [_][]const u8{ red_png_2x2[0..], red_png_2x2[0..] };
    const result = try pipeline.embedImagesReported(&images);
    try std.testing.expectEqual(.native_batch, result.execution);
    const embeddings = result.vectors;
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.last_batch);
    try std.testing.expectEqual(@as(usize, 2), embeddings.len);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 1.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[1]);
}

test "indexed image embedding compacts healthy rows into one backend run" {
    const alloc = std.testing.allocator;
    var fake = FakeVisionBatchSession{};
    var pipeline = EmbeddingPipeline{ .allocator = alloc, .session = fake.session(), .tok = undefined, .vision_session = fake.session(), .config = .{ .normalize = false, .image_size = 2 } };
    var errors: [3]?anyerror = undefined;
    const result = try pipeline.embedImagesIndexed(&.{ &red_png_2x2, "broken", &red_png_2x2 }, &errors);
    defer freeEmbeddingSlices(alloc, result.vectors);
    try std.testing.expectEqual(.native_batch, result.execution);
    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.last_batch);
    try std.testing.expect(errors[0] == null and errors[2] == null);
    try std.testing.expectEqual(error.ImageDecodeFailed, errors[1].?);
    try std.testing.expectEqual(@as(usize, 0), result.vectors[1].len);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, result.vectors[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, result.vectors[2]);
    const empty = try pipeline.embedImagesIndexed(&.{"broken"}, errors[0..1]);
    defer freeEmbeddingSlices(alloc, empty.vectors);
    try std.testing.expectEqual(@as(usize, 1), fake.run_count);
    try std.testing.expectEqual(error.ImageDecodeFailed, errors[0].?);
}

test "image batch planning respects permanent workspace limits without reserving" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = FakeVisionBatchSession{};
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 700 }, .static_workspace_bytes = 1, .check_live_memory = false };
    var pipeline = EmbeddingPipeline{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .vision_session = session, .config = .{ .image_size = 2 } };
    const single = try session.fitsRun(.{ .batch = 1, .input_bytes = 48, .host_preprocess_bytes = 64 });
    try std.testing.expect(single);
    try std.testing.expectEqual(@as(usize, 1), try pipeline.imageBatchPrefix(&.{ 64, 64, 64 }));
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 0), fake.run_count);
}

test "embedImages falls back to per-image runs when batched image shape collapses" {
    const allocator = std.testing.allocator;

    var fake = FakeCollapsingVisionSession{};
    var execution_gate: std.atomic.Mutex = .unlocked;
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false, .image_size = 2 },
        .execution_lock = &execution_gate,
        .vision_session = fake.session(),
    };

    const images = [_][]const u8{ red_png_2x2[0..], red_png_2x2[0..] };
    const result = try pipeline.embedImagesReported(&images);
    try std.testing.expectEqual(.fallback, result.execution);
    const embeddings = result.vectors;
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectEqual(@as(usize, 3), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.collapsed_batch_attempts);
    try std.testing.expectEqual(@as(usize, 2), embeddings.len);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[1]);
    try std.testing.expect(execution_gate.tryLock());
    execution_gate.unlock();
}

test "image embedding broker fuses concurrent callers and transfers owned vectors" {
    const microbatch = @import("../server/executor_microbatch.zig");
    const shared = std.heap.smp_allocator;
    var broker = microbatch.Broker.init(std.testing.allocator);
    defer broker.deinit();
    const Executor = struct {
        vision: FakeVisionBatchSession = .{},

        fn run(ptr: *anyopaque, items: []const microbatch.ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.execute(items) catch |err| {
                for (items) |item| item.slot.fail(err);
            };
        }

        fn execute(self: *@This(), items: []const microbatch.ExecuteItem) !void {
            const images = try shared.alloc([]const u8, items.len);
            defer shared.free(images);
            for (items, images) |item, *encoded| encoded.* = item.payloadAs([]const u8).*;
            var pipeline = EmbeddingPipeline{
                .allocator = shared,
                .session = self.vision.session(),
                .tok = undefined,
                .config = .{ .normalize = false, .image_size = 2 },
                .vision_session = self.vision.session(),
            };
            const batch = try pipeline.embedImagesReported(images);
            defer shared.free(batch.vectors);
            for (items, batch.vectors) |item, vector| item.slot.setValue([]f32, vector, switch (batch.execution) {
                .native_batch => .native_batch,
                .serial => .serial,
                .fallback => .fallback,
            });
        }
    };
    const Submit = struct {
        broker: *microbatch.Broker,
        executor: *Executor,
        result: ?microbatch.ItemResult([]f32) = null,
        err: ?anyerror = null,

        fn run(self: *@This()) std.Io.Cancelable!void {
            const payload: []const u8 = &red_png_2x2;
            const results = self.broker.submitBatchControlled([]const u8, []f32, std.testing.io, shared, .{ .model = "clip", .generation = 1, .task = .embed, .transform = "encoded-image", .resource_class = .cpu }, .{ .mode = .native, .preferred_items = 2, .max_items = 2, .max_wait_us = 500_000 }, &.{.{ .bytes = payload.len, .pixels = 4 }}, &.{.{}}, null, .{}, &.{payload}, self.executor, Executor.run) catch |err| {
                self.err = err;
                return;
            };
            self.result = results[0];
            shared.free(results);
        }
        pub fn deinit(self: *@This()) void {
            if (self.result) |result| switch (result.result) {
                .value => |vector| shared.free(vector),
                .item_error => {},
            };
        }
    };
    var executor = Executor{};
    var first = Submit{ .broker = &broker, .executor = &executor };
    defer first.deinit();
    var second = Submit{ .broker = &broker, .executor = &executor };
    defer second.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(std.testing.io);
    try group.concurrent(std.testing.io, Submit.run, .{&first});
    try group.concurrent(std.testing.io, Submit.run, .{&second});
    try group.await(std.testing.io);
    if (first.err) |err| return err;
    if (second.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), executor.vision.run_count);
    try std.testing.expectEqual(@as(usize, 2), executor.vision.last_batch);
    try std.testing.expectEqual(first.result.?.execution_id, second.result.?.execution_id);
    try std.testing.expectEqual(.native_batch, first.result.?.execution);
    const first_vector = first.result.?.result.value;
    const second_vector = second.result.?.result.value;
    try std.testing.expect(first_vector.ptr != second_vector.ptr);
    try std.testing.expectEqual(@as(f32, 1), first_vector[0] + second_vector[0]);
}

test "borrowed image embeddings report native singleton and fallback execution" {
    const allocator = std.testing.allocator;
    var fake = FakeVisionBatchSession{};
    var pipeline = EmbeddingPipeline{ .allocator = allocator, .session = fake.session(), .tok = undefined, .config = .{ .normalize = false, .image_size = 2 }, .vision_session = fake.session() };
    const pixels = z17RepeatArray([_]u8{ 255, 0, 0, 255 }, 4);
    const rasters = @as([2]antfly_image.BorrowedRasterAttachment, @splat(.{ .bytes = &pixels, .width = 2, .height = 2, .stride_bytes = 8 }));
    const native = try pipeline.embedBorrowedRastersReported(&rasters);
    defer freeEmbeddingSlices(allocator, native.vectors);
    try std.testing.expectEqual(.native_batch, native.execution);
    const singleton = try pipeline.embedBorrowedRastersReported(rasters[0..1]);
    defer freeEmbeddingSlices(allocator, singleton.vectors);
    try std.testing.expectEqual(.serial, singleton.execution);
    var collapsing = FakeCollapsingVisionSession{};
    pipeline.session = collapsing.session();
    pipeline.vision_session = collapsing.session();
    const fallback = try pipeline.embedBorrowedRastersReported(&rasters);
    defer freeEmbeddingSlices(allocator, fallback.vectors);
    try std.testing.expectEqual(.fallback, fallback.execution);
    try std.testing.expectEqual(@as(usize, 3), collapsing.run_count);
}

test "embedAudioPcm falls back under the execution gate without re-entry" {
    const allocator = std.testing.allocator;

    var fake = FakeCollapsingAudioSession{};
    var execution_gate: std.atomic.Mutex = .unlocked;
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false },
        .execution_lock = &execution_gate,
        .audio_session = fake.session(),
    };

    const samples = @as([1024]f32, @splat(0.0));
    const clips = [_]audio.PcmAudio{
        .{ .samples = &samples, .sample_rate = audio.CLAP_CONFIG.sample_rate },
        .{ .samples = &samples, .sample_rate = audio.CLAP_CONFIG.sample_rate },
    };
    const embeddings = try pipeline.embedAudioPcm(&clips);
    defer freeEmbeddingSlices(allocator, embeddings);

    try std.testing.expectEqual(@as(usize, 3), fake.run_count);
    try std.testing.expectEqual(@as(usize, 2), fake.collapsed_batch_attempts);
    try std.testing.expectEqual(@as(usize, 2), embeddings.len);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, embeddings[1]);
    try std.testing.expect(execution_gate.tryLock());
    execution_gate.unlock();
}

test "audio microbatch isolates corrupt clips and bounds decoded windows" {
    const allocator = std.testing.allocator;
    var fake = FakeCollapsingAudioSession{};
    var pipeline = EmbeddingPipeline{
        .allocator = allocator,
        .session = fake.session(),
        .audio_session = fake.session(),
        .tok = undefined,
        .config = .{ .normalize = false },
    };
    const samples = @as([1024]f32, @splat(0));
    const wav = try audio.wav.encodeMono(allocator, &samples, .{ .sample_rate = audio.CLAP_CONFIG.sample_rate, .audio_format = 1, .bits_per_sample = 16 });
    defer allocator.free(wav);
    const clips = [_]EncodedAudioClip{ .{ .bytes = wav }, .{ .bytes = "invalid audio" }, .{ .bytes = wav } };
    var errors: [3]?anyerror = undefined;
    const plan = try clapBatchPlan(fake.session(), 1);
    pipeline.config.max_audio_decode_working_bytes = plan.feature_reserve_bytes + 2 * samples.len * @sizeOf(f32);
    const result = try pipeline.embedEncodedAudioIndexed(&clips, &errors);
    defer freeEmbeddingSlices(allocator, result.vectors);
    try std.testing.expect(errors[0] == null);
    try std.testing.expect(errors[1] != null);
    try std.testing.expect(errors[2] == null);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, result.vectors[0]);
    try std.testing.expectEqual(@as(usize, 0), result.vectors[1].len);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, result.vectors[2]);
    try std.testing.expectEqual(@as(usize, 0), fake.collapsed_batch_attempts);
    try std.testing.expectEqual(@as(usize, 2), fake.run_count);
    try std.testing.expectEqual(.fallback, result.execution);
}

test "text-only embedding capability rejects media before decoder or backend access" {
    const AdmissionProbe = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return error.Cancelled;
        }
    };
    var admission: AdmissionProbe = .{};
    var lane: @import("../execution_control.zig").ExecutionLane = .{};
    var pipeline: EmbeddingPipeline = .{
        .allocator = std.testing.allocator,
        // Unsupported media must not inspect either backend or tokenizer.
        .session = undefined,
        .tok = undefined,
        .config = .{ .allow_image = false, .allow_audio = false, .resident_embedding_gemma2 = true },
        .execution_lane = &lane,
        .execution_control = .{ .ptr = &admission, .check_fn = AdmissionProbe.check },
        .last_input_tokens = 7,
    };
    const malformed = [_][]const u8{"this must never reach a decoder"};
    var errors = [_]?anyerror{null};
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedImages(&malformed));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedImagesReported(&malformed));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedImagesIndexed(&malformed, &errors));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedAudio(&malformed));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedEncodedAudio(&.{.{ .bytes = malformed[0] }}));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedEncodedAudioIndexed(&.{.{ .bytes = malformed[0] }}, &errors));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedContent(&.{.{ .content = &.{.{ .image = malformed[0] }} }}));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedContent(&.{.{ .content = &.{.{ .audio = .{ .bytes = malformed[0] } }} }}));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedAudioPcm(&.{.{ .samples = &.{0}, .sample_rate = 16000 }}));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedAudioPcmReported(&.{.{ .samples = &.{0}, .sample_rate = 16000 }}));
    try std.testing.expectError(error.UnsupportedInput, pipeline.embedAudioInterleavedPcm(&.{.{ .samples = &.{0}, .sample_rate = 16000, .channels = 1 }}));
    try std.testing.expectEqual(@as(usize, 0), admission.calls);
    try std.testing.expectEqual(@as(usize, 7), pipeline.last_input_tokens);
    try std.testing.expect(!lane.owned);
    // A supported request must still honor cancellation at admission.
    pipeline.config.allow_image = true;
    try std.testing.expectError(error.Cancelled, pipeline.embedImagesReported(&malformed));
    try std.testing.expectEqual(@as(usize, 1), admission.calls);
    try std.testing.expectError(error.Cancelled, pipeline.embedContent(&.{.{ .content = &.{.{ .text = "supported" }} }}));
    try std.testing.expectEqual(@as(usize, 2), admission.calls);
    try std.testing.expectEqual(@as(usize, 7), pipeline.last_input_tokens);
    try std.testing.expect(!lane.owned);
}

test "selectProjectedOutput skips collapsed pooled output for image batch" {
    const allocator = std.testing.allocator;

    var outputs = try allocator.alloc(Tensor, 2);
    defer allocator.free(outputs);
    outputs[0] = try Tensor.initFloat32(allocator, "pooled", &.{ 1, 2 }, &.{ 9.0, 9.0 });
    defer outputs[0].deinit();
    outputs[1] = try Tensor.initFloat32(allocator, "hidden", &.{ 2, 3, 2 }, &.{
        1.0,  2.0,
        3.0,  4.0,
        5.0,  6.0,
        7.0,  8.0,
        9.0,  10.0,
        11.0, 12.0,
    });
    defer outputs[1].deinit();

    const selection = selectProjectedOutput(outputs, 2, 2) orelse return error.TestUnexpectedResult;
    try std.testing.expect(selection.use_cls_pool);
    try std.testing.expectEqualStrings("hidden", selection.tensor.name);
}

const red_png_2x2 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
    0x08, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xd4, 0x9a, 0x73, 0x00, 0x00, 0x00,
    0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x4f, 0x25, 0xc4, 0xd6, 0x00, 0x00, 0x00, 0x10, 0x49, 0x44,
    0x41, 0x54, 0x78, 0x9c, 0x63, 0xfc, 0xc3, 0x00, 0x02, 0x2c, 0x60, 0x92,
    0x01, 0x00, 0x0d, 0x04, 0x01, 0x02, 0xbf, 0x50, 0x15, 0xb3, 0x00, 0x00,
    0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
};

const FakeVisionBatchSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,

    fn session(self: *FakeVisionBatchSession) backends.Session {
        return fakeVisionSession(self, run);
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeVisionBatchSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        const batch: usize = @intCast(inputs[0].shape[0]);
        self.run_count += 1;
        self.last_batch = batch;

        const data = try allocator.alloc(f32, batch * 2);
        defer allocator.free(data);
        for (0..batch) |b| {
            data[b * 2 + 0] = @floatFromInt(b);
            data[b * 2 + 1] = @floatFromInt(b + 1);
        }

        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "image_embeds", &.{ @intCast(batch), 2 }, data);
        return out;
    }
};

const FakeCollapsingVisionSession = struct {
    run_count: usize = 0,
    collapsed_batch_attempts: usize = 0,

    fn session(self: *FakeCollapsingVisionSession) backends.Session {
        return fakeVisionSession(self, run);
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeCollapsingVisionSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        const requested_batch: usize = @intCast(inputs[0].shape[0]);
        self.run_count += 1;
        if (requested_batch > 1) self.collapsed_batch_attempts = requested_batch;

        const actual_batch: usize = if (requested_batch > 1) 1 else requested_batch;
        const data = [_]f32{ 1.0, 2.0 };
        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "image_embeds", &.{ @intCast(actual_batch), 2 }, data[0 .. actual_batch * 2]);
        return out;
    }
};

const FakeCollapsingAudioSession = struct {
    run_count: usize = 0,
    collapsed_batch_attempts: usize = 0,

    fn session(self: *FakeCollapsingAudioSession) backends.Session {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = run,
                .inputInfo = inputInfo,
                .outputInfo = outputInfo,
                .backend = backend,
                .close = close,
            },
        };
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeCollapsingAudioSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        const requested_batch: usize = @intCast(inputs[0].shape[0]);
        self.run_count += 1;
        if (requested_batch > 1) self.collapsed_batch_attempts = requested_batch;

        const actual_batch: usize = if (requested_batch > 1) 1 else requested_batch;
        const data = [_]f32{ 1.0, 2.0 };
        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "audio_embeds", &.{ @intCast(actual_batch), 2 }, data[0 .. actual_batch * 2]);
        return out;
    }

    fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, 1, 1001, 64 } }};
    }

    fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "audio_embeds", .dtype = .f32, .shape = &.{ -1, 2 } }};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};

fn fakeVisionSession(ptr: anytype, comptime runFn: anytype) backends.Session {
    const VTable = struct {
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, 2, 2 } }};
        }

        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "image_embeds", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }

        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }

        fn close(_: *anyopaque) void {}
    };
    return .{
        .ptr = @ptrCast(ptr),
        .vtable = &.{
            .run = runFn,
            .inputInfo = VTable.inputInfo,
            .outputInfo = VTable.outputInfo,
            .backend = VTable.backend,
            .close = VTable.close,
        },
    };
}

const FakeProjectionSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,
    last_dim: usize = 0,

    fn session(self: *FakeProjectionSession) backends.Session {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = run,
                .inputInfo = inputInfo,
                .outputInfo = outputInfo,
                .backend = backend,
                .close = close,
            },
        };
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeProjectionSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), inputs.len);
        try std.testing.expectEqual(@as(usize, 2), inputs[0].shape.len);
        const batch: usize = @intCast(inputs[0].shape[0]);
        const dim: usize = @intCast(inputs[0].shape[1]);
        self.run_count += 1;
        self.last_batch = batch;
        self.last_dim = dim;

        const input = inputs[0].asFloat32();
        const projected = try allocator.alloc(f32, batch * 3);
        defer allocator.free(projected);
        for (0..batch) |b| {
            const row = input[b * dim ..][0..dim];
            projected[b * 3 + 0] = row[0];
            projected[b * 3 + 1] = row[1];
            projected[b * 3 + 2] = row[0] + row[1];
        }

        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "projected", &.{ @intCast(batch), 3 }, projected);
        return out;
    }

    fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "input", .dtype = .f32, .shape = &.{ -1, 2 } }};
    }

    fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "output", .dtype = .f32, .shape = &.{ -1, 3 } }};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};

fn z17RepeatArray(comptime array: anytype, comptime repetitions: usize) [array.len * repetitions]@TypeOf(array[0]) {
    var result: [array.len * repetitions]@TypeOf(array[0]) = undefined;
    for (0..repetitions) |i| @memcpy(result[i * array.len ..][0..array.len], &array);
    return result;
}

test "CLAP prepared accelerator waves preserve indexed errors and collapsed-batch fallback" {
    const a = std.testing.allocator;
    var fake = FakeCollapsingAudioSession{};
    var vtable = fake.session().vtable.*;
    const Mock = struct {
        fn backend(_: *anyopaque) backends.BackendType {
            return .metal;
        }
    };
    vtable.backend = Mock.backend;
    const session = backends.Session{ .ptr = &fake, .vtable = &vtable };
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = session, .audio_session = session, .tok = undefined, .config = .{ .normalize = false } };
    const samples: [1024]f32 = @splat(0);
    const wav = try audio.wav.encodeMono(a, &samples, .{ .sample_rate = audio.CLAP_CONFIG.sample_rate, .audio_format = 1, .bits_per_sample = 16 });
    defer a.free(wav);
    var clips: [10]EncodedAudioClip = undefined;
    for (&clips) |*clip| clip.* = .{ .bytes = wav };
    clips[1].bytes = "invalid audio";
    var errors: [10]?anyerror = undefined;
    const result = try pipeline.embedEncodedAudioIndexed(&clips, &errors);
    defer freeEmbeddingSlices(a, result.vectors);
    try std.testing.expectEqual(.fallback, result.execution);
    try std.testing.expect(errors[1] != null);
    try std.testing.expectEqual(@as(usize, 10), result.vectors.len);
    for (result.vectors, errors, 0..) |row, failure, index| {
        if (index == 1) {
            try std.testing.expectEqual(@as(usize, 0), row.len);
        } else {
            try std.testing.expect(failure == null);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, row);
        }
    }
    try std.testing.expect(fake.collapsed_batch_attempts > 1);
    // Two attempted batch forwards plus nine valid singleton fallbacks.
    try std.testing.expectEqual(@as(usize, 11), fake.run_count);
}

test "embedding workspace producer never owns the actual model gate on denial" {
    const Probe = struct {
        gate: *std.atomic.Mutex,
        calls: usize = 0,
        gate_owned: bool = false,
        fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const free = self.gate.tryLock();
            self.gate_owned = self.gate_owned or !free;
            if (free) self.gate.unlock();
            return error.ResourceTemporarilyUnavailable;
        }
        fn retain(_: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    var probe = Probe{ .gate = &gate };
    var controller = memory_admission.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer controller.configureResourceBudget(null) catch unreachable;
    var session = fakeTextSession(.wasm, &.{ -1, -1 });
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    session.execution_gate = &gate;
    session.cohort_scheduler = &scheduler;
    var pipeline = EmbeddingPipeline{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{}, .execution_lock = &gate };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(!probe.gate_owned);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
}

test "embedding workspace producer holds one ready turn and supports actual gate and controller reentry" {
    const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
    const Probe = struct {
        gate: *std.atomic.Mutex,
        scheduler: *Scheduler,
        controller: *memory_admission.AdmissionController,
        calls: usize = 0,
        releases: usize = 0,
        metadata_reentries: usize = 0,
        gate_owned: bool = false,
        ready_owned: bool = false,
        scheduler_locked: bool = false,
        fn observe(self: *@This()) void {
            const free = self.gate.tryLock();
            self.gate_owned = self.gate_owned or !free;
            if (free) self.gate.unlock();
            _ = self.controller.snapshot();
            self.metadata_reentries += 1;
        }
        fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.observe();
            if (self.calls == 1) return 1; // Independently admitted host copies.
            if (self.scheduler.mutex.tryLock()) {
                if (self.scheduler.head) |head| self.ready_owned = head.ready and !head.acquired;
                self.scheduler.mutex.unlock();
            } else self.scheduler_locked = true;
            return error.ResourceTemporarilyUnavailable;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.releases += 1;
        }
    };
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = Scheduler{};
    var controller = memory_admission.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .gate = &gate, .scheduler = &scheduler, .controller = &controller };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer controller.configureResourceBudget(null) catch unreachable;
    var session = fakeTextSession(.wasm, &.{ -1, -1 });
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    session.execution_gate = &gate;
    session.cohort_scheduler = &scheduler;
    var pipeline = EmbeddingPipeline{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{}, .execution_lock = &gate };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expect(probe.metadata_reentries >= 3);
    try std.testing.expect(probe.ready_owned and !probe.gate_owned and !probe.scheduler_locked);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
}

test "embedding cohorts let a query run before the bulk call finishes" {
    const Lane = @import("../execution_control.zig").ExecutionLane;
    const Probe = struct {
        lane: *Lane,
        started: std.Io.Event = .unset,
        batches: [4]usize = @splat(0),
        runs: usize = 0,

        fn encode(_: *anyopaque, alloc: std.mem.Allocator, text: []const u8, max_len: usize) !EncodeResult {
            const ids = try alloc.alloc(i32, max_len);
            errdefer alloc.free(ids);
            const mask = try alloc.alloc(i32, max_len);
            @memset(ids, 0);
            @memset(mask, 0);
            ids[0] = @intCast(text.len);
            mask[0] = 1;
            return .{ .ids = ids, .attention_mask = mask, .allocator = alloc };
        }
        fn special(_: *anyopaque) tokenizer_mod.SpecialTokens {
            return .{};
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            };
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "sentence_embedding", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn run(raw: *anyopaque, inputs: []const Tensor, alloc: std.mem.Allocator) ![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const batch: usize = @intCast(inputs[0].shape[0]);
            self.batches[self.runs] = batch;
            self.runs += 1;
            if (self.runs == 1) {
                self.started.set(std.testing.io);
                const until = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
                while (true) {
                    platform.sync.lockYielding(&self.lane.mutex);
                    const queued = self.lane.head != null;
                    self.lane.mutex.unlock();
                    if (queued) break;
                    if (platform.time.monotonicNs() >= until) return error.Timeout;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                }
            }
            const seq: usize = @intCast(inputs[0].shape[1]);
            const values = try alloc.alloc(f32, batch * 2);
            defer alloc.free(values);
            for (0..batch) |i| {
                values[i * 2] = @floatFromInt(inputs[0].asInt64()[i * seq]);
                values[i * 2 + 1] = 1;
            }
            const outputs = try alloc.alloc(Tensor, 1);
            errdefer alloc.free(outputs);
            outputs[0] = try Tensor.initFloat32(alloc, "sentence_embedding", &.{ @intCast(batch), 2 }, values);
            return outputs;
        }
        fn runControlled(raw: *anyopaque, inputs: []const Tensor, alloc: std.mem.Allocator, control: InferenceExecutionControl) ![]Tensor {
            try control.check();
            return run(raw, inputs, alloc);
        }
        fn query(probe: *@This(), base: EmbeddingPipeline) !void {
            try probe.started.wait(std.testing.io);
            var pipeline = base;
            pipeline.interactive = true;
            const rows = try pipeline.embed(&.{"query"});
            defer freeEmbeddingSlices(std.testing.allocator, rows);
            try std.testing.expectEqual(@as(f32, 5), rows[0][0]);
        }
    };
    var lane = Lane{};
    var probe = Probe{ .lane = &lane };
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = .{ .ptr = &probe, .vtable = &.{ .run = Probe.run, .runWithControl = Probe.runControlled, .inputInfo = Probe.inputInfo, .outputInfo = Probe.outputInfo, .backend = Probe.backend, .close = Probe.close } },
        .tok = .{ .ptr = &probe, .vtable = &.{ .encode = undefined, .encodeInto = undefined, .encodeForModel = Probe.encode, .encodeGeneration = undefined, .decode = undefined, .specialTokens = Probe.special, .vocabSize = undefined, .deinit = undefined } },
        .config = .{ .normalize = false, .max_length = 8, .batch_size = 4 },
        .execution_lane = &lane,
        .execution_control = .{ .io = std.testing.io },
    };
    var query = try std.testing.io.concurrent(Probe.query, .{ &probe, pipeline });
    defer query.cancel(std.testing.io) catch {};
    const rows = try pipeline.embed(&.{ "a", "bb", "ccc", "dddd", "eeeee", "ffffff", "ggggggg", "hhhhhhhh", "iiiiiiiii" });
    defer freeEmbeddingSlices(std.testing.allocator, rows);
    try query.await(std.testing.io);
    try std.testing.expectEqualSlices(usize, &.{ 4, 1, 4, 1 }, &probe.batches);
    for (rows, 1..) |row, i| try std.testing.expectEqual(@as(f32, @floatFromInt(i)), row[0]);
    try std.testing.expectEqual(@as(usize, 9), pipeline.last_input_tokens);
}

test "embeddinggemma2 CUDA flat audio and mixed batch dispatch fit production admission" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const model = @import("antfly_platform").env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const tokenizer_path = try std.fs.path.join(a, &.{ model, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const hf = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer hf.deinitSelf();
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var session = try session_factory.createCudaSession(a, model);
    defer session.close();
    const MiB = 1024 * 1024;
    const limits = memory.Limits{ .host_limit_bytes = 1536 * MiB, .backend_limit_bytes = 4608 * MiB, .combined_limit_bytes = 6144 * MiB, .scratch_limit_bytes = 768 * MiB };
    var weights = try controller.tryAcquire(.gpu, limits, .{ .backend_weight_bytes = 3195 * MiB }, false);
    defer weights.release();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = limits, .static_workspace_bytes = 512 * MiB, .model_profile = .{ .hidden_size = 512, .embedding_gemma2_cuda = true }, .check_live_memory = false };
    var pipeline = EmbeddingPipeline.init(a, session, hf.tokenizer(), .{
        .resident_embedding_gemma2 = true,
        .max_length = 8192,
        .strict_max_length = true,
        .trim_padding_to_batch_max = true,
        .text_prefix = try @import("../architectures/embedding_gemma2.zig").taskPrefix("CLUSTERING"),
    });
    // This offline GPU test deliberately runs without a serving supervisor.
    pipeline.execution_control = .{ .native_call_policy = .in_process };
    const samples: [16000]f32 = @splat(0.25);
    const wav = try audio.wav.encodeMono(a, &samples, .{ .audio_format = 1, .sample_rate = 16000, .bits_per_sample = 16 });
    defer a.free(wav);
    const text = "Group this short audio clip.";
    const expected_text = try pipeline.embed(&.{text});
    defer freeEmbeddingSlices(a, expected_text);
    const expected_audio = try @import("embedding_gemma2.zig").embed(a, session, hf.tokenizer(), .{ .content = &.{.{ .audio = .{ .bytes = wav } }} }, .{}, null, pipeline.execution_control);
    defer a.free(expected_audio.vector);

    // Exercise the public flat-audio entrypoint, not just its budget helper.
    const flat = try pipeline.embedEncodedAudio(&.{.{ .bytes = wav, .decode_options = .{ .mime_hint = "audio/wav" } }});
    defer freeEmbeddingSlices(a, flat);
    try std.testing.expectEqual(@as(usize, 1), flat.len);
    try std.testing.expectEqual(@as(usize, 768), flat[0].len);
    for (expected_audio.vector, flat[0]) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);

    // Flat mixed HTTP inputs reach embedContent with one part per row. Both
    // the text row and audio row must avoid the legacy 1536 MiB reservation.
    const mixed = try pipeline.embedContent(&.{ .{ .content = &.{.{ .text = text }} }, .{ .content = &.{.{ .audio = .{ .bytes = wav, .decode_options = .{ .mime_hint = "audio/wav" } } }} } });
    defer freeEmbeddingSlices(a, mixed);
    try std.testing.expectEqual(@as(usize, 2), mixed.len);
    for ([_][]const f32{ expected_text[0], expected_audio.vector }, mixed) |expected, actual| {
        try std.testing.expectEqual(@as(usize, 768), actual.len);
        for (expected, actual) |x, y| try std.testing.expectApproxEqAbs(x, y, 1e-5);
    }
    try std.testing.expect(pipeline.last_input_tokens > expected_audio.input_tokens);
}

test "embedding stages default to the session resident contract and preserve explicit host control" {
    var marker: u8 = 0;
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = .{ .ptr = &marker, .vtable = undefined, .model_contract = .{ .resident_text_embeddings = true } },
        .tok = undefined,
        .config = .{},
    };
    try std.testing.expect(pipeline.residentTextEncoderEnabled());
    pipeline.config.resident_text_encoder = false;
    try std.testing.expect(!pipeline.residentTextEncoderEnabled());
    pipeline.config.resident_text_encoder = null;
    pipeline.session.model_contract.resident_text_embeddings = false;
    try std.testing.expect(!pipeline.residentTextEncoderEnabled());
    pipeline.config.resident_text_encoder = true;
    try std.testing.expect(pipeline.residentTextEncoderEnabled());
}

test "host flattened token outputs honor every requested pooling mode" {
    const a = std.testing.allocator;
    const Probe = struct {
        fn run(_: *anyopaque, _: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
            const outputs = try allocator.alloc(Tensor, 1);
            errdefer allocator.free(outputs);
            const nan = std.math.nan(f32);
            outputs[0] = try Tensor.initFloat32(allocator, "hidden", &.{ 6, 2 }, &.{ 1, 6, 3, 4, nan, nan, 7, 8, 11, 12, 9, 10 });
            return outputs;
        }
    };
    var session = fakeTextSession(.native, &.{ 2, 3 });
    var vtable = session.vtable.*;
    vtable.run = Probe.run;
    session.vtable = &vtable;
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = session, .tok = undefined, .config = .{ .normalize = false, .resident_text_encoder = false } };
    const modes = [_]PoolingStrategy{ .mean, .cls, .max, .last };
    const expected = [_][4]f32{ .{ 2, 5, 10, 11 }, .{ 1, 6, 7, 8 }, .{ 3, 6, 11, 12 }, .{ 3, 4, 9, 10 } };
    for (modes, expected) |mode, values| {
        pipeline.config.pooling = mode;
        const output = try pipeline.embedTokenized(&.{ 101, 10, 0, 101, 20, 30 }, &.{ 1, 1, 0, 0, 1, 1 }, 2, 3);
        defer freeEmbeddingSlices(a, output);
        try std.testing.expectEqual(@as(usize, 2), output.len);
        try std.testing.expectEqualSlices(f32, values[0..2], output[0]);
        try std.testing.expectEqualSlices(f32, values[2..4], output[1]);
    }
}

test "actual imported text pipeline prepares bank tokens outside model and provider gates" {
    if (comptime build_options.enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const imported = @import("../backends/imported_onnx_session.zig");
    const sink_module = @import("../execution/retirement_sink.zig");
    const Probe = struct {
        gate: *std.atomic.Mutex,
        controller: *memory_admission.AdmissionController,
        scheduler: *@import("../runtime/embedding_scheduler.zig").Scheduler,
        ready_phase_calls: usize = 0,
        invalid_ready_phase: bool = false,
        tokens: [64]memory_admission.AdmissionAmounts = @splat(.{}),
        next: usize = 1,
        context_refs: usize = 1,
        observed: usize = 0,
        under_gate: bool = false,
        provider_gate: ?*std.atomic.Mutex = null,
        under_provider_gate: bool = false,
        fn observe(self: *@This()) void {
            const available = self.gate.tryLock();
            self.under_gate = self.under_gate or !available;
            if (available) self.gate.unlock();
            if (self.provider_gate) |provider| {
                const available_provider = provider.tryLock();
                self.under_provider_gate = self.under_provider_gate or !available_provider;
                if (available_provider) provider.unlock();
            }
            _ = self.controller.snapshot();
            if (self.scheduler.mutex.tryLock()) {
                if (self.scheduler.head) |head| {
                    self.ready_phase_calls += 1;
                    self.invalid_ready_phase = self.invalid_ready_phase or !head.ready or head.acquired;
                }
                self.scheduler.mutex.unlock();
            } else self.invalid_ready_phase = true;
            self.observed += 1;
        }
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.context_refs += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.context_refs -= 1;
        }
        fn reserve(raw: *anyopaque, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (self.next == self.tokens.len) return error.ResourceLimitExceeded;
            const token = self.next;
            self.next += 1;
            self.tokens[token] = amounts;
            return token;
        }
        fn retain(raw: *anyopaque, token: usize, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) return error.ResourceLimitExceeded;
            self.tokens[token] = amounts;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) @panic("unknown actual pipeline token");
            self.tokens[token] = .{};
        }
        fn chargedBanks(pool: *@import("../execution/physical_buffers.zig").Pool) usize {
            var count: usize = 0;
            for (pool.frames.items) |frame| for (frame.storages) |maybe| if (maybe) |lease| {
                if (lease.admission != null) count += 1;
            };
            return count;
        }
        fn closeOutsideExecution(session: session_mod.Session) void {
            const fatal = platform.inference_process_supervisor.restartWorker;
            var sink = sink_module.Ref.create() catch fatal();
            defer sink.release();
            var guard = sink.enter() catch fatal();
            defer guard.finish();
            var epoch = guard.retainEpoch() catch fatal();
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const ids = try builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, 2 }));
    try graph.markOutput(try builder.neg(try builder.convertDtype(ids, .f32)));
    const bytes = try @import("onnx_graph").exportGraph(a, &graph, .{});
    defer a.free(bytes);
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    try directory.dir.writeFile(std.testing.io, .{ .sub_path = "text-budget.onnx", .data = bytes });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "text-budget.onnx" });
    defer a.free(path);
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    var controller = memory_admission.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .gate = &gate, .controller = &controller, .scheduler = &scheduler };
    try controller.configureResourceBudget(.{ .context = &probe, .retain_context = Probe.retainContext, .release_context = Probe.releaseContext, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var configured = true;
    defer if (configured) controller.configureResourceBudget(null) catch @panic("actual pipeline quota survived cleanup");
    var session = try imported.createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .partitioned });
    const implementation_for_provider: *imported.ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    const provider_owner = implementation_for_provider.shared_backend_ctx.retain();
    defer {
        probe.provider_gate = null;
        provider_owner.release();
    }
    probe.provider_gate = &provider_owner.operations.gate.mutex;
    var opened = true;
    defer if (opened) Probe.closeOutsideExecution(session);
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .combined_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    session.execution_gate = &gate;
    session.cohort_scheduler = &scheduler;
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = session, .tok = undefined, .config = .{ .normalize = false, .max_length = 2 }, .execution_lock = &gate };
    for ([_][2]i64{ .{ 1, 2 }, .{ 3, 4 } }) |values| {
        const outputs = try pipeline.embedTokenized(&values, &.{ 1, 1 }, 1, 2);
        defer freeEmbeddingSlices(a, outputs);
        const expected = [_]f32{ -@as(f32, @floatFromInt(values[0])), -@as(f32, @floatFromInt(values[1])) };
        try std.testing.expectEqual(@as(usize, 1), outputs.len);
        try std.testing.expectEqualSlices(f32, &expected, outputs[0]);
        const implementation: *imported.ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
        const pool = implementation.executable.runtime.physical_pool orelse return error.MissingActualTextPhysicalPool;
        var charged = Probe.chargedBanks(pool);
        if (pool.variant_cache) |cache| {
            for (cache.entries.items) |variant| charged += Probe.chargedBanks(variant.pool);
        }
        try std.testing.expect(charged > 0);
    }
    Probe.closeOutsideExecution(session);
    opened = false;
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
    for (probe.tokens) |amounts| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amounts);
    try controller.configureResourceBudget(null);
    configured = false;
    try std.testing.expectEqual(@as(usize, 1), probe.context_refs);
    try std.testing.expect(probe.observed > 4 and !probe.under_gate and !probe.under_provider_gate);
    try std.testing.expect(probe.ready_phase_calls > 0 and !probe.invalid_ready_phase);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
}

test "actual imported text pipeline evicts banks outside model and provider gates" {
    if (comptime build_options.enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ml = @import("ml");
    const imported = @import("../backends/imported_onnx_session.zig");
    const sink_module = @import("../execution/retirement_sink.zig");
    const Probe = struct {
        gate: *std.atomic.Mutex,
        controller: *memory_admission.AdmissionController,
        scheduler: *@import("../runtime/embedding_scheduler.zig").Scheduler,
        ready_phase_calls: usize = 0,
        invalid_ready_phase: bool = false,
        tokens: [64]memory_admission.AdmissionAmounts = @splat(.{}),
        next: usize = 1,
        context_refs: usize = 1,
        observed: usize = 0,
        under_gate: bool = false,
        provider_gate: ?*std.atomic.Mutex = null,
        under_provider_gate: bool = false,
        fn observe(self: *@This()) void {
            const available = self.gate.tryLock();
            self.under_gate = self.under_gate or !available;
            if (available) self.gate.unlock();
            if (self.provider_gate) |provider| {
                const available_provider = provider.tryLock();
                self.under_provider_gate = self.under_provider_gate or !available_provider;
                if (available_provider) provider.unlock();
            }
            _ = self.controller.snapshot();
            if (self.scheduler.mutex.tryLock()) {
                if (self.scheduler.head) |head| {
                    self.ready_phase_calls += 1;
                    self.invalid_ready_phase = self.invalid_ready_phase or !head.ready or head.acquired;
                }
                self.scheduler.mutex.unlock();
            } else self.invalid_ready_phase = true;
            self.observed += 1;
        }
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.context_refs += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.context_refs -= 1;
        }
        fn reserve(raw: *anyopaque, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (self.next == self.tokens.len) return error.ResourceLimitExceeded;
            const token = self.next;
            self.next += 1;
            self.tokens[token] = amounts;
            return token;
        }
        fn retain(raw: *anyopaque, token: usize, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) return error.ResourceLimitExceeded;
            self.tokens[token] = amounts;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) @panic("unknown actual pipeline token");
            self.tokens[token] = .{};
        }
        fn chargedBanks(pool: *@import("../execution/physical_buffers.zig").Pool) usize {
            var count: usize = 0;
            for (pool.frames.items) |frame| for (frame.storages) |maybe| if (maybe) |lease| {
                if (lease.admission != null) count += 1;
            };
            return count;
        }
        fn closeOutsideExecution(session: session_mod.Session) void {
            const fatal = platform.inference_process_supervisor.restartWorker;
            var sink = sink_module.Ref.create() catch fatal();
            defer sink.release();
            var guard = sink.enter() catch fatal();
            defer guard.finish();
            var epoch = guard.retainEpoch() catch fatal();
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const ids = try builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ -1, -1 }));
    try graph.markOutput(try builder.neg(try builder.convertDtype(ids, .f32)));
    const bytes = try @import("onnx_graph").exportGraph(a, &graph, .{});
    defer a.free(bytes);
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    try directory.dir.writeFile(std.testing.io, .{ .sub_path = "text-budget.onnx", .data = bytes });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "text-budget.onnx" });
    defer a.free(path);
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    var controller = memory_admission.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .gate = &gate, .controller = &controller, .scheduler = &scheduler };
    try controller.configureResourceBudget(.{ .context = &probe, .retain_context = Probe.retainContext, .release_context = Probe.releaseContext, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var configured = true;
    defer if (configured) controller.configureResourceBudget(null) catch @panic("actual pipeline quota survived cleanup");
    var session = try imported.createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .partitioned });
    const implementation_for_provider: *imported.ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    const provider_owner = implementation_for_provider.shared_backend_ctx.retain();
    defer {
        probe.provider_gate = null;
        provider_owner.release();
    }
    probe.provider_gate = &provider_owner.operations.gate.mutex;
    var opened = true;
    defer if (opened) Probe.closeOutsideExecution(session);
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .combined_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    session.execution_gate = &gate;
    session.cohort_scheduler = &scheduler;
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = session, .tok = undefined, .config = .{ .normalize = false, .max_length = 8 }, .execution_lock = &gate };
    // More than the four-entry specialization capacity: old charged child
    // programs must retire through explicit preflight cleanup before the next
    // admission, without any callback holding specialization/model exclusion.
    const mask: [8]i64 = @splat(1);
    for (2..8) |width| {
        var values: [8]i64 = undefined;
        var expected: [8]f32 = undefined;
        for (values[0..width], expected[0..width], 0..) |*value, *want, index| {
            value.* = @intCast(width * 10 + index);
            want.* = -@as(f32, @floatFromInt(value.*));
        }
        const outputs = try pipeline.embedTokenized(values[0..width], mask[0..width], 1, width);
        defer freeEmbeddingSlices(a, outputs);
        try std.testing.expectEqual(@as(usize, 1), outputs.len);
        try std.testing.expectEqualSlices(f32, expected[0..width], outputs[0]);
        const implementation: *imported.ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
        var charged: usize = 0;
        for (implementation.specializations.entries.items) |entry| {
            if (entry.session.executable.runtime.physical_pool) |pool| {
                charged += Probe.chargedBanks(pool);
                if (pool.variant_cache) |cache| {
                    for (cache.entries.items) |variant| charged += Probe.chargedBanks(variant.pool);
                }
            }
        }
        try std.testing.expect(charged > 0);
        if (width == 7) try std.testing.expect(implementation.specializations.stats.evictions > 0);
    }
    Probe.closeOutsideExecution(session);
    opened = false;
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
    for (probe.tokens) |amounts| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amounts);
    try controller.configureResourceBudget(null);
    configured = false;
    try std.testing.expectEqual(@as(usize, 1), probe.context_refs);
    try std.testing.expect(probe.observed > 4 and !probe.under_gate and !probe.under_provider_gate);
    try std.testing.expect(probe.ready_phase_calls > 0 and !probe.invalid_ready_phase);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
}

test "raw text projection refusal preserves current B and retires quota after actual Native provider and outer gate cleanup" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
    const operation = @import("../execution/backend_operation.zig");
    const first = try Shared.init(a, .native, null);
    defer first.release();
    const second = try Shared.init(a, .native, null);
    defer second.release();
    const Probe = struct {
        backends: [2]ops_mod.ComputeBackend,
        gate: *std.atomic.Mutex,
        bounded_source_closes: usize = 0,
        raw_source_closes: usize = 0,
        callbacks: usize = 0,
        source_calls: usize = 0,
        projection_calls: usize = 0,
        failure: ?anyerror = null,
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.UnexpectedHostForward;
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn sourceInputs(_: *anyopaque) []const backends.TensorInfo {
            return &.{ .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 2 } }, .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 1, 2 } } };
        }
        fn sourceOutputs(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "hidden", .dtype = .f32, .shape = &.{ 1, 2, 2 } }};
        }
        fn projectionInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "embedding", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn rawClose(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.raw_source_closes += 1;
        }
        fn accepts(outputs: *const session_mod.ResidentOutputs) bool {
            return outputs.deinit_backend_owner == rawClose;
        }
        fn boundedClose(raw: *anyopaque, _: std.mem.Allocator, cleanup: CleanupContext) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (cleanup.boundary == null or cleanup.outside_execution) self.failure = error.MissingActualTextCleanupEpoch;
            self.bounded_source_closes += 1;
        }
        const cleanup_services = session_mod.ResidentOutputs.BackendOwnerCleanupServices{ .accepts = accepts, .deinit = boundedClose };
        fn resident(raw: *anyopaque, _: []const Tensor, allocator: std.mem.Allocator) !?session_mod.ResidentOutputs {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.gate.tryLock()) {
                self.gate.unlock();
                return error.TextForwardMissingActualGate;
            }
            self.source_calls += 1;
            if (self.source_calls != 1) return error.ConsumedSourceReplayed;
            const backend = &self.backends[0];
            var scope = try operation.enter(backend.*, null, null, .managed);
            defer scope.deinitChecked() catch @panic("raw text source operation cleanup failed");
            const vector = try allocator.alloc(ops_mod.CT, 1);
            errdefer allocator.free(vector);
            vector[0] = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 2, 2 });
            errdefer backend.free(vector[0]);
            try scope.finish();
            return .{ .outputs = vector, .backend = backend, .allocator = allocator, .backend_owner = self, .deinit_backend_owner = rawClose, .backend_owner_cleanup_services = &cleanup_services };
        }
        fn residentControlled(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, _: InferenceExecutionControl) !?session_mod.ResidentOutputs {
            return resident(raw, inputs, allocator);
        }
        fn project(raw: *anyopaque, _: []const session_mod.ResidentInput, _: std.mem.Allocator) !?session_mod.ResidentOutputs {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.projection_calls += 1;
            try self.reenter();
            return error.ProjectionRefused;
        }
        fn projectControlled(raw: *anyopaque, inputs: []const session_mod.ResidentInput, allocator: std.mem.Allocator, _: InferenceExecutionControl) !?session_mod.ResidentOutputs {
            return project(raw, inputs, allocator);
        }
        fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reenter() catch |err| {
                self.failure = err;
            };
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            self.reenter() catch |err| {
                self.failure = err;
            };
        }
        fn reenter(self: *@This()) !void {
            if (!self.gate.tryLock()) return error.ProjectionRefundUnderOuterGate;
            self.gate.unlock();
            for (&self.backends) |*backend| {
                var scope = try operation.enter(backend.*, null, null, .managed);
                defer scope.deinitChecked() catch @panic("projection quota reentry cleanup failed");
                const input = try backend.fromFloat32(&.{ 2, 5 });
                defer backend.free(input);
                const doubled = try backend.add(input, input);
                defer backend.free(doubled);
                const values = try backend.toFloat32(doubled, std.testing.allocator);
                defer std.testing.allocator.free(values);
                try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
                try scope.finish();
            }
        }
        const source_table = backends.Session.VTable{ .run = run, .runResident = resident, .runResidentWithControl = residentControlled, .inputInfo = sourceInputs, .outputInfo = sourceOutputs, .backend = kind, .close = close };
        const projection_table = backends.Session.VTable{ .run = run, .runResidentInputs = project, .runResidentInputsWithControl = projectControlled, .inputInfo = projectionInfo, .outputInfo = projectionInfo, .backend = kind, .close = close };
    };
    var gate: std.atomic.Mutex = .unlocked;
    var probe = Probe{ .gate = &gate, .backends = .{ first.computeBackend(), second.computeBackend() } };
    var controller = memory_admission.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const source = backends.Session{ .ptr = &probe, .vtable = &Probe.source_table };
    const projection = backends.Session{ .ptr = &probe, .vtable = &Probe.projection_table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = source, .text_projection = projection, .tok = undefined, .execution_lock = &gate, .config = .{ .normalize = false, .max_length = 2, .pooling = .mean } };
    try std.testing.expectError(error.ProjectionRefused, pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2));
    try std.testing.expectEqual(@as(usize, 1), probe.source_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.projection_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.bounded_source_closes);
    try std.testing.expectEqual(@as(usize, 0), probe.raw_source_closes);
    try std.testing.expect(probe.callbacks > 0);
    if (probe.failure) |err| return err;
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
}

fn checkPreparedTextProjectionBoundary(backend_kind: backends.BackendType) !void {
    const a = std.testing.allocator;
    const ml = @import("ml");
    const imported = @import("../backends/imported_onnx_session.zig");
    const Probe = struct {
        source_counter: ?*std.atomic.Value(u64),
        retired_source_executions: u64 = 0,
        gate: *std.atomic.Mutex,
        providers: []const *std.atomic.Mutex,
        source_backend: ops_mod.ComputeBackend,
        cancel_on_destination: bool,
        cancelled: bool = false,
        live: usize = 0,
        tokens: [128]memory_admission.AdmissionAmounts = @splat(.{}),
        next_token: usize = 1,
        source_token: usize = 0,
        source_host_bytes: usize = 0,
        source_funded_at_destination: bool = false,
        callbacks: usize = 0,
        callback_phase: enum { reserve, retain, release } = .reserve,
        failure: ?anyerror = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn sourceExecutions(self: *const @This()) u64 {
            return if (self.source_counter) |counter| counter.load(.acquire) else self.retired_source_executions;
        }
        fn retireWrapperMetadata(self: *@This()) void {
            self.retired_source_executions = self.sourceExecutions();
            self.source_counter = null;
        }
        fn observe(self: *@This()) void {
            self.callbacks += 1;
            if (!self.gate.tryLock()) self.failure = error.SourceModelGateDuringSupplier else self.gate.unlock();
            for (self.providers, 0..) |gate, provider_index| {
                if (!gate.tryLock()) {
                    if (self.failure == null) {
                        std.debug.print("TEXT_STAGE_SUPPLIER_PROVIDER_HELD phase={s} provider={d} source_executions={d} callbacks={d}\n", .{ @tagName(self.callback_phase), provider_index, self.sourceExecutions(), self.callbacks });
                        std.debug.dumpCurrentStackTrace(.{ .first_address = @returnAddress(), .allow_unsafe_unwind = false });
                    }
                    self.failure = error.SourceOrDestinationProviderDuringSupplier;
                } else gate.unlock();
            }
            if (self.source_backend.operationToken() != null) {
                self.failure = error.SourceOperationDuringSupplier;
            }
        }
        fn reserve(raw: *anyopaque, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .reserve;
            self.observe();
            if (self.next_token == self.tokens.len) return error.ResourceLimitExceeded;
            const token = self.next_token;
            self.next_token += 1;
            if (self.sourceExecutions() != 0) {
                if (self.source_token == 0 or self.tokens[self.source_token].host_scratch_bytes < self.source_host_bytes or self.source_host_bytes < 2 * @sizeOf(f32) or self.tokens[self.source_token].hostTotalBytes() + self.tokens[self.source_token].backendTotalBytes() == 0) self.failure = error.SourceHostQuotaLostBeforeDestination;
                self.source_funded_at_destination = true;
                if (self.cancel_on_destination) self.cancelled = true;
            }
            self.tokens[token] = amounts;
            self.live += 1;
            return token;
        }
        fn retain(raw: *anyopaque, token: usize, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .retain;
            self.observe();
            self.tokens[token] = amounts;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .release;
            self.observe();
            self.tokens[token] = .{};
            self.live -= 1;
        }
        fn close(session: backends.Session) void {
            const Sink = @import("../execution/retirement_sink.zig");
            var sink = Sink.Ref.create() catch @panic("test cleanup sink");
            defer sink.release();
            var guard = sink.enter() catch @panic("test cleanup guard");
            defer guard.finish();
            var epoch = guard.retainEpoch() catch @panic("test cleanup epoch");
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    // 0: actual resident success; 1: unsupported rank after actual prepared
    // execution; 2: cancellation first observed at destination admission.
    for (0..3) |mode| {
        // Pin the actual provider owners independently of Imported wrappers.
        // These defers run AFTER both session-close defers, including errors.
        const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
        var source_provider: ?*Shared = null;
        defer if (source_provider) |owner| owner.release();
        var projection_provider: ?*Shared = null;
        defer if (projection_provider) |owner| owner.release();
        var source_graph = ml.graph.Graph.init(a);
        defer source_graph.deinit();
        var source_builder = ml.graph.Builder.init(&source_graph);
        const ids = try source_builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, 2 }));
        const negative = try source_builder.neg(try source_builder.convertDtype(ids, .f32));
        try source_graph.markOutput(if (mode == 1) try source_builder.reshape(negative, ml.graph.Shape.init(.f32, &.{ 1, 1, 1, 2 })) else negative);
        const source_bytes = try @import("onnx_graph").exportGraph(a, &source_graph, .{});
        defer a.free(source_bytes);
        var projection_graph = ml.graph.Graph.init(a);
        defer projection_graph.deinit();
        var projection_builder = ml.graph.Builder.init(&projection_graph);
        const value = try projection_builder.parameter("input", ml.graph.Shape.init(.f32, &.{ 1, 2 }));
        try projection_graph.markOutput(try projection_builder.add(value, value));
        const projection_bytes = try @import("onnx_graph").exportGraph(a, &projection_graph, .{});
        defer a.free(projection_bytes);
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        try directory.dir.writeFile(std.testing.io, .{ .sub_path = "source.onnx", .data = source_bytes });
        try directory.dir.writeFile(std.testing.io, .{ .sub_path = "projection.onnx", .data = projection_bytes });
        const source_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "source.onnx" });
        defer a.free(source_path);
        const projection_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "projection.onnx" });
        defer a.free(projection_path);
        var source = try imported.createSessionWithOptions(a, source_path, backend_kind, .{ .graph_runtime_strategy = .partitioned });
        var source_open = true;
        defer if (source_open) Probe.close(source);
        var projection = try imported.createSessionWithOptions(a, projection_path, backend_kind, .{ .graph_runtime_strategy = .partitioned });
        var projection_open = true;
        defer if (projection_open) Probe.close(projection);
        const source_impl: *imported.ImportedOnnxSession = @ptrCast(@alignCast(source.ptr));
        const projection_impl: *imported.ImportedOnnxSession = @ptrCast(@alignCast(projection.ptr));
        source_provider = source_impl.shared_backend_ctx.retain();
        projection_provider = projection_impl.shared_backend_ctx.retain();
        var gate: std.atomic.Mutex = .unlocked;
        var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
        source.execution_gate = &gate;
        source.cohort_scheduler = &scheduler;
        var probe = Probe{ .source_counter = &source_impl.execution_count, .gate = &gate, .providers = &.{ &source_impl.shared_backend_ctx.operations.gate.mutex, &projection_impl.shared_backend_ctx.operations.gate.mutex }, .source_backend = source_impl.cb, .cancel_on_destination = mode == 2 };
        // Imported execution-count storage belongs to the wrapper; backend
        // header is copied by value and its exact context is independently pinned.
        defer probe.retireWrapperMetadata();
        const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
        defer domain.release();
        const controller = &domain.controller;
        try std.testing.expect(source.model_lifetime.?.admission_domain == null and projection.model_lifetime.?.admission_domain == null);
        source.model_lifetime.?.admission_domain = domain.pin();
        projection.model_lifetime.?.admission_domain = domain.pin();
        try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        const admission = session_mod.RunAdmission{ .controller = controller, .backend_class = if (backend_kind == .native) .cpu else .gpu, .limits = .{ .host_limit_bytes = 8 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
        source.run_admission = admission;
        projection.run_admission = admission;
        var pipeline = EmbeddingPipeline{ .allocator = a, .session = source, .text_projection = projection, .execution_lock = &gate, .tok = undefined, .execution_control = .{ .ptr = &probe, .check_fn = Probe.check, .native_call_policy = .in_process }, .config = .{ .normalize = false, .max_length = 2, .pooling = .mean, .resident_projection_required = mode == 0 and backend_kind == .native } };
        {
            var cleanup = try @import("../execution/cleanup_owner.zig").Owner.beginResident(source);
            defer cleanup.finishAfterUnlock();
            var projection_execution: ?session_mod.RunPermit = null;
            defer if (projection_execution) |*owned| owned.deinitWithCleanup(.{ .boundary = cleanup.epoch() });
            var ids_tensor = try Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 1, 2 });
            defer ids_tensor.deinit();
            var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
            try source.selectReadyCohort(&gate, pipeline.execution_control.?, &waiter);
            defer source.leaveSelectedCohort(&waiter);
            var permit = try source.admitInputs(&.{ids_tensor}, a, pipeline.execution_control);
            defer permit.deinitWithCleanup(.{ .boundary = cleanup.epoch() });
            try permit.prepareResidentOutputRetirement();
            try permit.preparePhysicalStorage(a);
            try permit.bindPhysicalCleanupEpoch(cleanup.epoch().?);
            // Capture the ACTUAL source permit token, not a heuristic largest
            // reservation or unrelated preprocessing/cached bank charge.
            probe.source_token = permit.lease.?.external_lease orelse if (permit.lease.?.shared_external) |shared| shared.token else return error.MissingSourceExternalQuotaToken;
            probe.source_host_bytes = permit.lease.?.amounts.host_scratch_bytes;
            var projection_shell = try projection.prepareResidentRetirement();
            defer if (projection_shell) |*shell| shell.destroyUnused();
            try source.acquireSelectedCohort(pipeline.execution_control.?, &waiter);
            pipeline.execution_gate_held = true;
            defer {
                source.releaseSelectedCohortGate(&waiter);
                pipeline.execution_gate_held = false;
            }
            const result = pipeline.tryEmbedTextResidentProjection(&.{ids_tensor}, &.{ 1, 1 }, 1, 2, projection, &permit, &projection_shell, .{ .boundary = cleanup.epoch() }, &projection_execution, .{ .legacy = &waiter });
            if (mode == 0) {
                const outputs = (try result) orelse return error.SourceNotAttempted;
                defer freeEmbeddingSlices(a, outputs);
                try std.testing.expectEqual(@as(usize, 1), outputs.len);
                try std.testing.expectEqualSlices(f32, &.{ -2, -4 }, outputs[0]);
            } else try std.testing.expectError(if (mode == 1) error.UnexpectedOutputShape else error.Cancelled, result);
        }
        try std.testing.expectEqual(@as(u64, 1), imported.executionStats(source).?.executions);
        try std.testing.expectEqual(@as(u64, if (mode == 0) 1 else 0), imported.executionStats(projection).?.executions);
        try std.testing.expect(!pipeline.execution_gate_held and scheduler.head == null);
        // Stop reading wrapper-owned counter metadata before actual retirement;
        // copied backend and provider gates remain live and checked through B.
        probe.retireWrapperMetadata();
        Probe.close(source);
        source_open = false;
        Probe.close(projection);
        projection_open = false;
        try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, probe.tokens[probe.source_token]);
        try std.testing.expect(probe.callbacks > 0);
        if (mode != 1) try std.testing.expect(probe.source_funded_at_destination);
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(@as(usize, 0), probe.live);
        try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
    }
}

test "actual prepared Native text projection releases source lanes preserves output and never replays consumed source" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    try checkPreparedTextProjectionBoundary(.native);
}

test "actual prepared Metal nonpromotable text source retains exact host quota through destination admission and cancellation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try checkPreparedTextProjectionBoundary(.metal);
}

fn checkPreparedMediaSourceBoundary(backend_kind: backends.BackendType, modality: ResidentProjectionModality) !void {
    const a = std.testing.allocator;
    const ml = @import("ml");
    const imported = @import("../backends/imported_onnx_session.zig");
    const Probe = struct {
        source_counter: ?*std.atomic.Value(u64),
        retired_source_executions: u64 = 0,
        gate: *std.atomic.Mutex,
        providers: []const *std.atomic.Mutex,
        source_backend: ops_mod.ComputeBackend,
        cancel_on_destination: bool,
        cancelled: bool = false,
        live: usize = 0,
        tokens: [128]memory_admission.AdmissionAmounts = @splat(.{}),
        next_token: usize = 1,
        source_token: usize = 0,
        source_host_bytes: usize = 0,
        source_funded_at_destination: bool = false,
        callbacks: usize = 0,
        callback_phase: enum { reserve, retain, release } = .reserve,
        failure: ?anyerror = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn sourceExecutions(self: *const @This()) u64 {
            return if (self.source_counter) |counter| counter.load(.acquire) else self.retired_source_executions;
        }
        fn retireWrapperMetadata(self: *@This()) void {
            self.retired_source_executions = self.sourceExecutions();
            self.source_counter = null;
        }
        fn observe(self: *@This()) void {
            self.callbacks += 1;
            if (!self.gate.tryLock()) self.failure = error.SourceModelGateDuringSupplier else self.gate.unlock();
            for (self.providers, 0..) |gate, provider_index| {
                if (!gate.tryLock()) {
                    if (self.failure == null) {
                        std.debug.print("TEXT_STAGE_SUPPLIER_PROVIDER_HELD phase={s} provider={d} source_executions={d} callbacks={d}\n", .{ @tagName(self.callback_phase), provider_index, self.sourceExecutions(), self.callbacks });
                        std.debug.dumpCurrentStackTrace(.{ .first_address = @returnAddress(), .allow_unsafe_unwind = false });
                    }
                    self.failure = error.SourceOrDestinationProviderDuringSupplier;
                } else gate.unlock();
            }
            if (self.source_backend.operationToken() != null) {
                self.failure = error.SourceOperationDuringSupplier;
            }
        }
        fn reserve(raw: *anyopaque, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .reserve;
            self.observe();
            if (self.next_token == self.tokens.len) return error.ResourceLimitExceeded;
            const token = self.next_token;
            self.next_token += 1;
            if (self.sourceExecutions() != 0) {
                if (self.source_token == 0 or self.tokens[self.source_token].host_scratch_bytes < self.source_host_bytes or self.source_host_bytes < 2 * @sizeOf(f32) or self.tokens[self.source_token].hostTotalBytes() + self.tokens[self.source_token].backendTotalBytes() == 0) self.failure = error.SourceHostQuotaLostBeforeDestination;
                self.source_funded_at_destination = true;
                if (self.cancel_on_destination) self.cancelled = true;
            }
            self.tokens[token] = amounts;
            self.live += 1;
            return token;
        }
        fn retain(raw: *anyopaque, token: usize, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .retain;
            self.observe();
            self.tokens[token] = amounts;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callback_phase = .release;
            self.observe();
            self.tokens[token] = .{};
            self.live -= 1;
        }
        fn close(session: backends.Session) void {
            const Sink = @import("../execution/retirement_sink.zig");
            var sink = Sink.Ref.create() catch @panic("test cleanup sink");
            defer sink.release();
            var guard = sink.enter() catch @panic("test cleanup guard");
            defer guard.finish();
            var epoch = guard.retainEpoch() catch @panic("test cleanup epoch");
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    // 0: actual resident success; 1: unsupported rank after actual prepared
    // execution; 2: cancellation first observed at destination admission.
    for (0..3) |mode| {
        // Pin the actual provider owners independently of Imported wrappers.
        // These defers run AFTER both session-close defers, including errors.
        const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
        var source_provider: ?*Shared = null;
        defer if (source_provider) |owner| owner.release();
        var projection_provider: ?*Shared = null;
        defer if (projection_provider) |owner| owner.release();
        var source_graph = ml.graph.Graph.init(a);
        defer source_graph.deinit();
        var source_builder = ml.graph.Builder.init(&source_graph);
        const ids = try source_builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, 2 }));
        const negative = try source_builder.neg(try source_builder.convertDtype(ids, .f32));
        try source_graph.markOutput(if (mode == 1) try source_builder.reshape(negative, ml.graph.Shape.init(.f32, &.{ 1, 1, 1, 2 })) else negative);
        const source_bytes = try @import("onnx_graph").exportGraph(a, &source_graph, .{});
        defer a.free(source_bytes);
        var projection_graph = ml.graph.Graph.init(a);
        defer projection_graph.deinit();
        var projection_builder = ml.graph.Builder.init(&projection_graph);
        const value = try projection_builder.parameter("input", ml.graph.Shape.init(.f32, &.{ 1, 2 }));
        try projection_graph.markOutput(try projection_builder.add(value, value));
        const projection_bytes = try @import("onnx_graph").exportGraph(a, &projection_graph, .{});
        defer a.free(projection_bytes);
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        try directory.dir.writeFile(std.testing.io, .{ .sub_path = "source.onnx", .data = source_bytes });
        try directory.dir.writeFile(std.testing.io, .{ .sub_path = "projection.onnx", .data = projection_bytes });
        const source_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "source.onnx" });
        defer a.free(source_path);
        const projection_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "projection.onnx" });
        defer a.free(projection_path);
        var source = try imported.createSessionWithOptions(a, source_path, backend_kind, .{ .graph_runtime_strategy = .partitioned });
        var source_open = true;
        defer if (source_open) Probe.close(source);
        var projection = try imported.createSessionWithOptions(a, projection_path, backend_kind, .{ .graph_runtime_strategy = .partitioned });
        var projection_open = true;
        defer if (projection_open) Probe.close(projection);
        const source_impl: *imported.ImportedOnnxSession = @ptrCast(@alignCast(source.ptr));
        const projection_impl: *imported.ImportedOnnxSession = @ptrCast(@alignCast(projection.ptr));
        source_provider = source_impl.shared_backend_ctx.retain();
        projection_provider = projection_impl.shared_backend_ctx.retain();
        var gate: std.atomic.Mutex = .unlocked;
        var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
        source.execution_gate = &gate;
        source.cohort_scheduler = &scheduler;
        var probe = Probe{ .source_counter = &source_impl.execution_count, .gate = &gate, .providers = &.{ &source_impl.shared_backend_ctx.operations.gate.mutex, &projection_impl.shared_backend_ctx.operations.gate.mutex }, .source_backend = source_impl.cb, .cancel_on_destination = mode == 2 };
        defer probe.retireWrapperMetadata();
        const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
        defer domain.release();
        const controller = &domain.controller;
        try std.testing.expect(source.model_lifetime.?.admission_domain == null and projection.model_lifetime.?.admission_domain == null);
        source.model_lifetime.?.admission_domain = domain.pin();
        projection.model_lifetime.?.admission_domain = domain.pin();
        try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        const admission = session_mod.RunAdmission{ .controller = controller, .backend_class = if (backend_kind == .native) .cpu else .gpu, .limits = .{ .host_limit_bytes = 8 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
        source.run_admission = admission;
        projection.run_admission = admission;
        var pipeline = EmbeddingPipeline{ .allocator = a, .session = source, .visual_projection = if (modality == .image) projection else null, .audio_projection = if (modality == .audio) projection else null, .execution_lock = &gate, .tok = undefined, .execution_control = .{ .ptr = &probe, .check_fn = Probe.check, .native_call_policy = .in_process }, .config = .{ .normalize = false, .max_length = 2, .pooling = .mean, .resident_projection_required = mode == 0 and backend_kind == .native } };
        {
            var ids_tensor = try Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 1, 2 });
            defer ids_tensor.deinit();
            var source_stage: session_mod.PreparedSource = .{};
            defer source_stage.deinit();
            try source_stage.initBeforeExecutionInto(pipeline.session, pipeline.session, &gate, .{ .host = &.{ids_tensor} }, .{ .batch = 1, .sequence = 2, .input_bytes = 16 }, null, .resident_consumer, a, pipeline.execution_control);
            try std.testing.expect(scheduler.head == &source_stage.waiter);
            probe.source_token = source_stage.permit.?.lease.?.external_lease orelse if (source_stage.permit.?.lease.?.shared_external) |shared| shared.token else return error.MissingSourceExternalQuotaToken;
            probe.source_host_bytes = source_stage.permit.?.lease.?.amounts.host_scratch_bytes;
            try source_stage.enter();
            const result = pipeline.tryEmbedResidentProjection(&.{ids_tensor}, projection, modality, 1, "media.encoder.resident", "media.projection.resident", &source_stage);
            if (mode == 0) {
                const outputs = (try result) orelse return error.SourceNotAttempted;
                defer freeEmbeddingSlices(a, outputs);
                try std.testing.expectEqual(@as(usize, 1), outputs.len);
                try std.testing.expectEqualSlices(f32, &.{ -2, -4 }, outputs[0]);
            } else try std.testing.expectError(if (mode == 1) error.UnexpectedOutputShape else error.Cancelled, result);
            // Both committed success and post-producer refusal consume the
            // one actual source; retrying it cannot replay numerical work.
            try std.testing.expectError(error.SourceInvocationAlreadyExecuted, source_stage.runHost(&.{ids_tensor}));
        }
        try std.testing.expectEqual(@as(u64, 1), imported.executionStats(source).?.executions);
        try std.testing.expectEqual(@as(u64, if (mode == 0) 1 else 0), imported.executionStats(projection).?.executions);
        try std.testing.expect(!pipeline.execution_gate_held and scheduler.head == null);
        probe.retireWrapperMetadata();
        Probe.close(source);
        source_open = false;
        Probe.close(projection);
        projection_open = false;
        try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, probe.tokens[probe.source_token]);
        try std.testing.expect(probe.callbacks > 0);
        if (mode != 1) try std.testing.expect(probe.source_funded_at_destination);
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(@as(usize, 0), probe.live);
        try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, controller.snapshot());
    }
}

test "actual Native media source owner image and audio handoff keep quota lanes and exactly once producer" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    for ([_]ResidentProjectionModality{ .image, .audio }) |modality| try checkPreparedMediaSourceBoundary(.native, modality);
}
test "actual Metal media source owner image and audio nonpromotable fallback keep host quota and cancellation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    for ([_]ResidentProjectionModality{ .image, .audio }) |modality| try checkPreparedMediaSourceBoundary(.metal, modality);
}

test "nonprojection text pooling capability decline consumes actual Native hidden once and strict mode never replays" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
    const shared = try Shared.init(a, .native, null);
    defer shared.release();
    const Producer = struct {
        backend: ops_mod.ComputeBackend,
        calls: usize = 0,
        fn host(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.ConsumedEncoderReplayed;
        }
        fn resident(raw: *anyopaque, _: []const Tensor, allocator: std.mem.Allocator) !?session_mod.ResidentOutputs {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls != 1) return error.ConsumedEncoderReplayed;
            const outputs = try allocator.alloc(ops_mod.CT, 1);
            errdefer allocator.free(outputs);
            outputs[0] = try self.backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 2, 2 });
            return .{ .outputs = outputs, .backend = &self.backend, .allocator = allocator };
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        const table = backends.Session.VTable{ .run = host, .runResident = resident, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close };
    };
    for ([_]bool{ false, true }) |decline| {
        for ([_]bool{ false, true }) |strict| {
            var partial = shared.stableBackend().vtable.*;
            // Deliberately untrusted partial provider: this test grants NO
            // physical-owner/promotion authority to the copied table. It only
            // exercises unsupported pooling after real Native CT production.
            partial.whereSelect = null;
            partial.operation_services = null;
            var producer = Producer{ .backend = shared.computeBackend() };
            if (decline) producer.backend.vtable = &partial;
            const session = backends.Session{ .ptr = &producer, .vtable = &Producer.table };
            var pipeline = EmbeddingPipeline{ .allocator = a, .session = session, .tok = undefined, .config = .{ .pooling = .max, .normalize = false, .resident_projection_required = strict } };
            var cleanup = try @import("../execution/cleanup_owner.zig").Owner.beginResident(session);
            defer cleanup.finishAfterUnlock();
            var permit = try session.admit(.{});
            defer permit.deinitWithCleanup(.{ .boundary = cleanup.epoch() });
            const result = pipeline.tryEmbedTextResidentEncoder(&.{}, &.{ 1, 1 }, 1, 2, &permit, .{ .boundary = cleanup.epoch() });
            if (strict and decline) {
                try std.testing.expectError(error.ResidentEmbeddingFallback, result);
            } else {
                const rows = (try result) orelse return error.CommittedEncoderReturnedNotAttempted;
                defer freeEmbeddingSlices(a, rows);
                try std.testing.expectEqual(@as(usize, 1), rows.len);
                try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, rows[0]);
            }
            try std.testing.expectEqual(@as(usize, 1), producer.calls);
        }
    }
}

fn qwenSameResultAllocationCase(a: std.mem.Allocator, strict: bool) !void {
    const Native = @import("../ops/native_compute.zig");
    var weights = Native.WeightStore{ .allocator = std.testing.allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    var compute = Native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    var table = Native.vtable_impl;
    table.whereSelect = null;
    table.operation_services = null;
    const cb = ops_mod.ComputeBackend{ .ptr = &compute, .vtable = &table };
    const Producer = struct {
        calls: usize = 0,
        fn run(self: *@This(), backend: *const ops_mod.ComputeBackend) !ops_mod.CT {
            self.calls += 1;
            if (self.calls != 1) return error.ConsumedEncoderReplayed;
            return backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 2, 2 });
        }
    };
    var producer = Producer{};
    const hidden = try producer.run(&cb);
    defer cb.free(hidden);
    var storage = [_]ops_mod.CT{hidden};
    var output = session_mod.ResidentOutputs{ .outputs = &storage, .backend = &cb, .allocator = a };
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = undefined, .tok = undefined, .config = .{ .pooling = .max, .normalize = true, .resident_projection_required = strict } };
    if (strict) {
        try std.testing.expectError(error.ResidentEmbeddingFallback, pipeline.finishProducedTextRows(&output, &.{ 1, 1 }, 1, 2, true, "text.pool.qwen3.resident", "text.encoder.qwen3.resident"));
    } else {
        const rows = try pipeline.finishProducedTextRows(&output, &.{ 1, 1 }, 1, 2, true, "text.pool.qwen3.resident", "text.encoder.qwen3.resident");
        defer freeEmbeddingSlices(a, rows);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        for ([_]f32{ 0.6, 0.8 }, rows[0]) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
    }
    try std.testing.expectEqual(@as(usize, 1), producer.calls);
}

test "Qwen committed hidden pooling refusal consumes actual Native output once and survives all allocation failures" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    try qwenSameResultAllocationCase(std.testing.allocator, false);
    try qwenSameResultAllocationCase(std.testing.allocator, true);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, qwenSameResultAllocationCase, .{false});
}

test "Qwen selected graph row host fallback preserves completed normalization boundary" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Native = @import("../ops/native_compute.zig");
    var weights = Native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    var compute = Native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var normalized = [_]f32{ 17, 23, 31 };
    linalg.l2Normalize(&normalized, normalized.len);
    const hidden = try cb.fromFloat32Shape(&normalized, &.{ 1, 3 });
    defer cb.free(hidden);
    var storage = [_]ops_mod.CT{hidden};
    var output = session_mod.ResidentOutputs{ .outputs = &storage, .backend = &cb, .allocator = a };
    var pipeline = EmbeddingPipeline{ .allocator = a, .session = undefined, .tok = undefined, .config = .{ .pooling = .last, .normalize = true } };
    const rows = try pipeline.finishProducedTextHost(&output, &.{ 1, 1, 0, 0 }, 1, 4, false, "text.pool.qwen3.graph", "unsupported_pooling");
    defer freeEmbeddingSlices(a, rows);
    try std.testing.expectEqualSlices(f32, &normalized, rows[0]);
    try std.testing.expect(pipeline.config.normalize);
}

test "Qwen lane model protocol is independent of adaptive and fixed batch policy" {
    var cfg = gpt_arch.Config{
        .family = .qwen3,
        .norm_type = .rms_norm,
        .position_encoding = .rope,
        .activation = .silu,
        .hidden_size = 64,
        .num_hidden_layers = 2,
        .num_attention_heads = 2,
        .num_key_value_heads = 1,
        .intermediate_size = 128,
    };
    try std.testing.expect(qwenWorkspaceLaneModel(.metal, cfg));
    try std.testing.expect(!qwenWorkspaceLaneModel(.native, cfg));
    // Capability comes from the actual model, not a task batching/env flag.
    // The source helper has no EmbeddingConfig argument to accidentally vary.
    cfg.family = .qwen3_5;
    try std.testing.expect(!qwenWorkspaceLaneModel(.metal, cfg));
    cfg.family = .qwen3;
    cfg.num_local_experts = 4;
    cfg.num_experts_per_tok = 2;
    try std.testing.expect(!qwenWorkspaceLaneModel(.metal, cfg));
}

test "Qwen injected numerical lane cannot invert selected source and legacy chunk in either order" {
    if (comptime build_options.enable_wasm) return error.SkipZigTest;
    const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
    const Lane = @import("../execution_control.zig").ExecutionLane;
    const Probe = struct {
        gate: *std.atomic.Mutex,
        lane: *Lane,
        failure: ?anyerror = null,
        callbacks: usize = 0,
        fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reenter();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reenter();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reenter();
        }
        fn reenter(self: *@This()) void {
            self.callbacks += 1;
            if (!self.gate.tryLock()) {
                self.failure = error.SelectedQwenSupplierStillOwnsModel;
                return;
            }
            self.gate.unlock();
            const lease = self.lane.acquire(.{ .deadline_ns = platform.time.monotonicNs() + 2 * std.time.ns_per_s }) catch |err| {
                self.failure = err;
                return;
            };
            lease.release();
        }
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.UnexpectedQwenSchedulingNumerics;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
        const table = backends.Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close };
    };
    const Worker = struct {
        session: backends.Session,
        lane: *Lane,
        source: bool,
        control: InferenceExecutionControl,
        waiter: Scheduler.Waiter = .{},
        failure: ?anyerror = null,
        done: std.atomic.Value(bool) = .init(false),
        fn execute(self: *@This()) !void {
            if (self.source) {
                try self.session.selectReadyCohort(self.session.execution_gate, self.control, &self.waiter);
                errdefer self.session.leaveSelectedCohort(&self.waiter);
                try self.session.acquireSelectedCohort(self.control, &self.waiter);
            } else try self.session.enterExecutionCohort(self.session.execution_gate, self.control, &self.waiter);
            defer self.session.leaveExecutionCohort(self.session.execution_gate, &self.waiter);
            // EXACT order used by119's source and68a's chunk: the real model
            // cohort is acquired before the injected numerical lane.
            const lane = try self.lane.acquire(self.control);
            defer lane.release();
        }
        fn start(self: *@This()) void {
            self.execute() catch |err| {
                self.failure = err;
            };
            self.done.store(true, .release);
        }
    };
    for ([_]bool{ true, false }) |source_first| {
        var gate: std.atomic.Mutex = .unlocked;
        var lane: Lane = .{};
        var scheduler: Scheduler = .{};
        var probe = Probe{ .gate = &gate, .lane = &lane };
        const session = backends.Session{ .ptr = &probe, .vtable = &Probe.table, .execution_gate = &gate, .cohort_scheduler = &scheduler };
        const control = InferenceExecutionControl{ .deadline_ns = platform.time.monotonicNs() + 2 * std.time.ns_per_s };
        var head: Scheduler.Waiter = .{};
        var head_live = true;
        if (source_first) {
            try session.selectReadyCohort(&gate, control, &head);
        } else try session.enterExecutionCohort(&gate, control, &head);
        var head_lane: ?Lane.Lease = if (source_first) null else try lane.acquire(control);
        var worker = Worker{ .session = session, .lane = &lane, .source = !source_first, .control = control };
        var spawned = false;
        errdefer if (!spawned) {
            if (head_lane) |owned| owned.release();
            if (head_live) session.leaveSelectedCohort(&head);
        };
        const thread = try std.Thread.spawn(.{}, Worker.start, .{&worker});
        spawned = true;
        defer {
            // Even a failed assertion releases the original owner before join.
            if (head_lane) |owned| owned.release();
            if (head_live) session.leaveSelectedCohort(&head);
            thread.join();
        }
        while (true) {
            // Selected cold sources have not acquired the model yet. Query
            // the exact queued successor under the actual publication lock;
            // hasPeer intentionally requires acquired numerical ownership.
            while (!scheduler.mutex.tryLock()) {
                try control.check();
                std.Thread.yield() catch {};
            }
            const peer_queued = scheduler.head == &head and head.next == &worker.waiter;
            scheduler.mutex.unlock();
            if (peer_queued) break;
            try control.check();
            if (worker.done.load(.acquire)) return worker.failure orelse error.QwenPeerSkippedCohort;
            std.Thread.yield() catch {};
        }
        if (source_first) {
            // The peer has joined the ACTUAL FIFO but must own no lane. This
            // real supplier reenters both physical owners while head is cold.
            var controller: memory_admission.AdmissionController = .{};
            defer controller.deinit();
            try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
            var lease = try controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 64 }, .{ .backend_scratch_bytes = 64 }, false);
            lease.release();
            try std.testing.expect(probe.failure == null);
            try std.testing.expectEqual(@as(usize, 2), probe.callbacks);
            try session.acquireSelectedCohort(control, &head);
            head_lane = try lane.acquire(control);
        }
        try std.testing.expect(!worker.done.load(.acquire));
        head_lane.?.release();
        head_lane = null;
        session.leaveSelectedCohort(&head);
        head_live = false;
        while (!worker.done.load(.acquire)) {
            try control.check();
            std.Thread.yield() catch {};
        }
        if (worker.failure) |err| return err;
    }
}

// Real, complete, hash-pinned HF fixtures; only QA assets are supplied by env.
const DenseQwen68Qa = struct {
    const a = std.testing.allocator;
    const FilePin = struct { name: []const u8, sha: []const u8 };
    const Pins = struct { hidden: usize, files: [5]FilePin };
    const fixtures = [_]Pins{
        .{ .hidden = 64, .files = .{ .{ .name = "config.json", .sha = "db1e66565df4b1bbab35305c3d3f2ce15f46ec090f348cbb064d9c4894f0f923" }, .{ .name = "model.safetensors", .sha = "20043bf1993e7279bdf5a86f399abb726c623bf6a73dd4811210aea684e91a3e" }, .{ .name = "model_manifest.json", .sha = "3d54e3eff4551555de6afa1d58ed6b6147867a7d3f5ae9c6206cd3d118de6bad" }, .{ .name = "tokenizer.json", .sha = "fdd36f2129669781a8342a38c2b57dcde7f3abdd20aec517d89404f66395ca9e" }, .{ .name = "tokenizer_config.json", .sha = "0124b7e26d88568ac8c06f2267057273093c50a1ac864ce6bfa56bb765860956" } } },
        .{ .hidden = 128, .files = .{ .{ .name = "config.json", .sha = "d40ef381f49e75b71fc1b7ebd43dd08d6f124c8a87d9b31758ca458b66d1afd3" }, .{ .name = "model.safetensors", .sha = "67c03f5612fda7cf027911ce4aee666ac72e17bf8f6b72e8e3e16d23b0c6d43c" }, .{ .name = "model_manifest.json", .sha = "3d54e3eff4551555de6afa1d58ed6b6147867a7d3f5ae9c6206cd3d118de6bad" }, .{ .name = "tokenizer.json", .sha = "fdd36f2129669781a8342a38c2b57dcde7f3abdd20aec517d89404f66395ca9e" }, .{ .name = "tokenizer_config.json", .sha = "0124b7e26d88568ac8c06f2267057273093c50a1ac864ce6bfa56bb765860956" } } },
    };
    fn hashFile(root: []const u8, file: []const u8) ![64]u8 {
        const path = try std.fs.path.join(a, &.{ root, file });
        defer a.free(path);
        const bytes = try @import("../util/c_file.zig").readFile(a, path);
        defer a.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return std.fmt.bytesToHex(digest, .lower);
    }
    fn fixture() !struct { path: []const u8, hidden: usize } {
        const path = platform.env.getenv("ANTFLY_UNIFIED_DENSE_QWEN68_FIXTURE") orelse return error.SkipZigTest;
        const config_hash = try hashFile(path, "config.json");
        for (fixtures) |pin| {
            if (!std.mem.eql(u8, &config_hash, pin.files[0].sha)) continue;
            for (pin.files) |file| try std.testing.expectEqualStrings(file.sha, &(try hashFile(path, file.name)));
            return .{ .path = path, .hidden = pin.hidden };
        }
        return error.UnknownDenseQwen68Fixture;
    }
    fn tokenizer(path: []const u8) !*@import("inference_hf_tokenizer").HfTokenizer {
        const filename = try std.fs.path.join(a, &.{ path, "tokenizer.json" });
        defer a.free(filename);
        const bytes = try @import("../util/c_file.zig").readFile(a, filename);
        defer a.free(bytes);
        return @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    }
    const control = InferenceExecutionControl{ .native_call_policy = .in_process };
    /// Publish the actual shared provider/runtime through its normal cold
    /// constructor, without running model arithmetic or warming hidden states.
    fn publishMetalProvider(session: session_mod.Session) !void {
        session_factory.attachIo(session, std.testing.io);
        var backend = try compute_provider.acquireWithControl(session, a, control);
        backend.deinit(); // real outside entry, before run admission/numerical gates
        const target = (try session.coldComputeTarget(control)) orelse return error.ExpectedActualMetalProvider;
        if (target != .metal) return error.ExpectedActualMetalProvider;
    }
    fn publishEmbeddingContract(session: *session_mod.Session, manifest: *const @import("../models/manifest.zig").ModelManifest) !void {
        // SAME loaded-runtime resolver: explicit artifact semantics, never a
        // fixture-only hidden-width guess or a broadened production contract.
        session.model_contract = @import("../models/session_contract.zig").resolve(session.model_contract, manifest, session.backend());
        try std.testing.expect(session.model_contract.embedding.?.family == .last_token_decoder);
        try std.testing.expect(session.model_contract.embedding.?.pooling == .last);
        try std.testing.expect(session.model_contract.embedding.?.normalize);
    }
    fn reportDifferences(label: []const u8, batch: usize, sequence: usize, row: usize, expected: []const f32, actual: []const f32) void {
        var maximum: f64 = 0;
        var squared: f64 = 0;
        for (expected, actual) |x, y| {
            const diff = @abs(@as(f64, x) - @as(f64, y));
            maximum = @max(maximum, diff);
            squared += diff * diff;
        }
        std.debug.print("dense-qwen68 parity={s} B={d} S={d} row={d} max_abs={d:.9} rms={d:.9}\n", .{ label, batch, sequence, row, maximum, @sqrt(squared / @as(f64, @floatFromInt(expected.len))) });
    }
    fn config(resident: bool, pooling: PoolingStrategy) EmbeddingConfig {
        return .{ .normalize = true, .pooling = pooling, .max_length = 32, .batch_size = 2, .resident_qwen3_embedding = resident, .resident_text_encoder = false, .trim_padding_to_batch_max = true, .ensure_trailing_eos_id = 63 };
    }
    fn inputs(batch: usize, sequence: usize, ids: *[32]i64, mask: *[32]i64) void {
        for (0..batch * sequence) |i| {
            ids[i] = if ((i + 1) % sequence == 0) 63 else @intCast(2 + (i * 7) % 13);
            mask[i] = 1;
        }
    }
    fn compare(expected: []const f32, actual: []const f32, hidden: usize) !void {
        try std.testing.expectEqual(hidden, expected.len);
        try std.testing.expectEqual(hidden, actual.len);
        var norm: f64 = 0;
        for (expected, actual) |reference, value| {
            try std.testing.expect(std.math.isFinite(reference) and std.math.isFinite(value));
            try std.testing.expectApproxEqAbs(reference, value, 3e-4);
            norm += @as(f64, value) * value;
        }
        try std.testing.expectApproxEqAbs(@as(f64, 1), norm, 1e-4);
    }
    fn graphExecutions(pipeline: *const EmbeddingPipeline) u64 {
        const timing = pipeline.lastResidentBackendTiming() orelse return 0;
        return timing.provider.prefill_frame_execute_successes;
    }
    fn close(session: session_mod.Session) void {
        const sink_mod = @import("../execution/retirement_sink.zig");
        var sink = sink_mod.Ref.create() catch @panic("dense Qwen QA close sink");
        defer sink.release();
        var guard = sink.enter() catch @panic("dense Qwen QA close guard");
        defer guard.finish();
        var epoch = guard.retainEpoch() catch @panic("dense Qwen QA close epoch");
        defer epoch.release();
        session.closeWithBoundary(&epoch);
    }
    const Probe = struct {
        model_gate: ?*std.atomic.Mutex = null,
        provider_gate: ?*std.atomic.Mutex = null,
        foreign_gate: ?*std.atomic.Mutex = null,
        tokens: [1024]memory_admission.AdmissionAmounts = @splat(.{}),
        next: usize = 1,
        calls: usize = 0,
        releases: usize = 0,
        references: usize = 1,
        violation: bool = false,
        fn observe(self: *@This()) void {
            for ([_]?*std.atomic.Mutex{ self.model_gate, self.provider_gate, self.foreign_gate }) |maybe| if (maybe) |gate| {
                const available = gate.tryLock();
                if (available) gate.unlock() else self.violation = true;
            };
            self.calls += 1;
        }
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.references += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.references -= 1;
        }
        fn reserve(raw: *anyopaque, amount: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (self.next == self.tokens.len) return error.ResourceLimitExceeded;
            const index = self.next;
            self.next += 1;
            self.tokens[index] = amount;
            return index;
        }
        fn retain(raw: *anyopaque, token: usize, amount: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) return error.ResourceLimitExceeded;
            self.tokens[token] = amount;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            if (token == 0 or token >= self.next) @panic("dense Qwen QA unknown token");
            self.tokens[token] = .{};
            self.releases += 1;
        }
    };
};

test "dense Qwen68 complete HF Native Metal eager resident normalized output and warm parity" {
    if (comptime !build_options.enable_native or !build_options.enable_metal) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    try std.testing.expect(manifest.isLastTokenDecoderEmbedder());
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    var native = try session_factory.createNativeSession(Q.a, fixture.path);
    defer Q.close(native);
    try Q.publishEmbeddingContract(&native, &manifest);
    var metal = try session_factory.createMetalSession(Q.a, fixture.path);
    defer Q.close(metal);
    try Q.publishEmbeddingContract(&metal, &manifest);
    try Q.publishMetalProvider(metal);
    const cfg = session_factory.getGptConfig(metal) orelse return error.ExpectedActualQwenConfiguration;
    try std.testing.expect(cfg.family == .qwen3 and gpt_arch.denseQwen3PrefillEligible(cfg));
    var reference = EmbeddingPipeline.init(Q.a, native, hf.tokenizer(), Q.config(false, .last));
    var eager = EmbeddingPipeline.init(Q.a, metal, hf.tokenizer(), Q.config(false, .last));
    var resident = EmbeddingPipeline.init(Q.a, metal, hf.tokenizer(), Q.config(true, .last));
    reference.execution_control = Q.control;
    eager.execution_control = Q.control;
    resident.execution_control = Q.control;
    var graph_dispatches: u64 = 0;
    var ids: [32]i64 = undefined;
    var mask: [32]i64 = undefined;
    // Actual B1/B2 and a shape change exercise reused and replaced plans.
    for ([_][2]usize{ .{ 1, 16 }, .{ 1, 16 }, .{ 2, 16 }, .{ 2, 16 }, .{ 1, 7 }, .{ 1, 16 } }) |shape| {
        const batch = shape[0];
        const sequence = shape[1];
        Q.inputs(batch, sequence, &ids, &mask);
        const count = batch * sequence;
        const expected = try reference.embedTokenized(ids[0..count], mask[0..count], batch, sequence);
        defer freeEmbeddingSlices(Q.a, expected);
        const ordinary = try eager.embedTokenized(ids[0..count], mask[0..count], batch, sequence);
        defer freeEmbeddingSlices(Q.a, ordinary);
        const actual = try resident.embedTokenized(ids[0..count], mask[0..count], batch, sequence);
        defer freeEmbeddingSlices(Q.a, actual);
        try std.testing.expectEqual(batch, actual.len);
        std.debug.print("dense-qwen68 snapshot_available={} actual_frame_successes={d} B={d} S={d}\n", .{ resident.lastResidentBackendTiming() != null, Q.graphExecutions(&resident), batch, sequence });
        for (0..batch) |row| {
            Q.reportDifferences("Native/eagerMetal", batch, sequence, row, expected[row], ordinary[row]);
            Q.reportDifferences("Native/residentMetal", batch, sequence, row, expected[row], actual[row]);
            Q.reportDifferences("eagerMetal/residentMetal", batch, sequence, row, ordinary[row], actual[row]);
            try Q.compare(expected[row], ordinary[row], fixture.hidden);
            try Q.compare(expected[row], actual[row], fixture.hidden);
        }
        graph_dispatches += Q.graphExecutions(&resident);
    }
    // Real tokenizer and normal public text entry, not a fake tokenizer.
    const texts = [_][]const u8{ "hello world alpha beta", "tiny query document again" };
    const expected_text = try reference.embed(&texts);
    defer freeEmbeddingSlices(Q.a, expected_text);
    const actual_text = try resident.embed(&texts);
    defer freeEmbeddingSlices(Q.a, actual_text);
    for (expected_text, actual_text) |expected, actual| try Q.compare(expected, actual, fixture.hidden);
    graph_dispatches += Q.graphExecutions(&resident);
    // Mean remains a real supported pooling fallback; no geometry is forced.
    reference.config.pooling = .mean;
    resident.config.pooling = .mean;
    Q.inputs(1, 16, &ids, &mask);
    const expected_mean = try reference.embedTokenized(ids[0..16], mask[0..16], 1, 16);
    defer freeEmbeddingSlices(Q.a, expected_mean);
    const actual_mean = try resident.embedTokenized(ids[0..16], mask[0..16], 1, 16);
    defer freeEmbeddingSlices(Q.a, actual_mean);
    try Q.compare(expected_mean[0], actual_mean[0], fixture.hidden);
    std.debug.print("dense-qwen68 hidden={d} actual_graph_success_evidence={d} (zero is not fallback proof; inspect snapshot availability)\n", .{ fixture.hidden, graph_dispatches });
    if (platform.env.getenvBoolDefault("ANTFLY_UNIFIED_QWEN68_REQUIRE_GRAPH", false)) try std.testing.expect(graph_dispatches > 0);
}

test "dense Qwen68 actual admitted pipeline cap refusal retained outputs and foreign B cleanup" {
    if (comptime !build_options.enable_native or !build_options.enable_metal) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    try std.testing.expect(manifest.isLastTokenDecoderEmbedder());
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    var native = try session_factory.createNativeSession(Q.a, fixture.path);
    defer Q.close(native);
    try Q.publishEmbeddingContract(&native, &manifest);
    var reference = EmbeddingPipeline.init(Q.a, native, hf.tokenizer(), Q.config(false, .last));
    reference.execution_control = Q.control;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe = Q.Probe{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = Q.Probe.retainContext, .release_context = Q.Probe.releaseContext, .try_reserve = Q.Probe.reserve, .retain = Q.Probe.retain, .release = Q.Probe.release });
    var configured = true;
    defer if (configured) domain.controller.configureResourceBudget(null) catch @panic("dense Qwen QA live quota");
    var metal = try session_factory.createMetalSession(Q.a, fixture.path);
    var metal_live = true;
    defer if (metal_live) {
        probe.model_gate = null;
        probe.provider_gate = null;
        Q.close(metal);
    };
    try Q.publishMetalProvider(metal);
    try Q.publishEmbeddingContract(&metal, &manifest);
    try metal.installOwnedExecutionGate(Q.a);
    probe.model_gate = metal.execution_gate;
    const model = metal.model_lifetime orelse return error.ExpectedActualModelLifetime;
    try std.testing.expect(model.admission_domain == null);
    model.admission_domain = domain.pin();
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    metal.cohort_scheduler = &scheduler;
    const cold = (try metal.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    probe.provider_gate = switch (cold) {
        .metal => |target| &target.exclusion.mutex,
        else => return error.ExpectedActualMetalProvider,
    };
    const MiB = 1024 * 1024;
    metal.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 512 * MiB, .combined_limit_bytes = 768 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    var pipeline = EmbeddingPipeline.init(Q.a, metal, hf.tokenizer(), Q.config(true, .last));
    pipeline.execution_lock = metal.execution_gate;
    pipeline.execution_control = Q.control;
    var ids: [32]i64 = undefined;
    var mask: [32]i64 = undefined;
    Q.inputs(2, 16, &ids, &mask);
    const expected = try reference.embedTokenized(&ids, &mask, 2, 16);
    defer freeEmbeddingSlices(Q.a, expected);
    // Cold and warm calls use the same actual managed provider and admission.
    for (0..3) |_| {
        var outputs = try pipeline.embedTokenizedOwned(&ids, &mask, 2, 16, .{ .outside_execution = true });
        defer outputs.deinitOutsideExecution();
        try std.testing.expectEqual(@as(usize, 2), outputs.len());
        for (0..2) |row| try Q.compare(expected[row], outputs.values(row), fixture.hidden);
    }
    const before_denial = domain.controller.snapshot();
    const original = pipeline.session.run_admission.?;
    pipeline.session.run_admission.?.limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 1, .combined_limit_bytes = 1 };
    if (pipeline.embedTokenizedOwned(&ids, &mask, 2, 16, .{ .outside_execution = true })) |value| {
        var unexpected = value;
        unexpected.deinitOutsideExecution();
        return error.ExpectedActualAdmissionDenial;
    } else |err| try std.testing.expect(err == error.ResourceLimitExceeded);
    pipeline.session.run_admission = original;
    try std.testing.expectEqualDeep(before_denial, domain.controller.snapshot());
    var held = try pipeline.embedTokenizedOwned(&ids, &mask, 2, 16, .{ .outside_execution = true });
    defer held.deinitOutsideExecution();
    // Borrowed diagnostic pointers end before actual Model/provider close.
    probe.model_gate = null;
    probe.provider_gate = null;
    Q.close(metal);
    metal_live = false;
    for (0..2) |row| try Q.compare(expected[row], held.values(row), fixture.hidden);
    try std.testing.expect(domain.controller.snapshot().hostTotalBytes() > 0);
    const sink_mod = @import("../execution/retirement_sink.zig");
    var sink = try sink_mod.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var foreign_gate: std.atomic.Mutex = .unlocked;
    const releases_before_b = probe.releases;
    {
        try std.testing.expect(foreign_gate.tryLock());
        probe.foreign_gate = &foreign_gate;
        // Every error/failed assertion unlocks before guard.finish callbacks.
        defer {
            foreign_gate.unlock();
            probe.foreign_gate = null;
        }
        try held.deinitWithCleanup(.{ .boundary = &epoch });
        try std.testing.expectEqual(releases_before_b, probe.releases);
    }
    guard.finish();
    try std.testing.expect(probe.releases > releases_before_b);
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
    for (probe.tokens) |amount| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amount);
    try std.testing.expect(!probe.violation and probe.calls > 4);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
    try domain.controller.configureResourceBudget(null);
    configured = false;
    try std.testing.expectEqual(@as(usize, 1), probe.references);
}

test "dense Qwen68 default Q8 resident baseline exports exact ordered normalized F32 vectors" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    // This is a separate production-default prepared-weight policy oracle.
    // The original Native/F32 comparison retains its independent strict gate.
    if (platform.env.getenv("TERMITE_METAL_Q8_RUNTIME_STAGING_MAX_MB")) |value| {
        const actual = std.fmt.parseUnsigned(usize, value, 10) catch return error.SkipZigTest;
        if (actual != 32) return error.SkipZigTest;
    }
    const fixture = try Q.fixture();
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    try std.testing.expect(manifest.isLastTokenDecoderEmbedder());
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    var metal = try session_factory.createMetalSession(Q.a, fixture.path);
    defer Q.close(metal);
    try Q.publishEmbeddingContract(&metal, &manifest);
    try Q.publishMetalProvider(metal);
    const cfg = session_factory.getGptConfig(metal) orelse return error.ExpectedActualQwenConfiguration;
    try std.testing.expect(cfg.family == .qwen3 and gpt_arch.denseQwen3PrefillEligible(cfg));
    var pipeline = EmbeddingPipeline.init(Q.a, metal, hf.tokenizer(), Q.config(true, .last));
    pipeline.execution_control = Q.control;
    var flat: std.ArrayListUnmanaged(f32) = .empty;
    defer flat.deinit(Q.a);
    var metadata: std.Io.Writer.Allocating = .init(Q.a);
    defer metadata.deinit();
    const config_hash = try Q.hashFile(fixture.path, "config.json");
    const weights_hash = try Q.hashFile(fixture.path, "model.safetensors");
    try metadata.writer.print("{{\"schema\":\"antfly.dense_qwen68.default_q8.v1\",\"encoding\":\"f32_little_endian\",\"q8_staging_max_mb\":32,\"fixture_config_sha256\":\"{s}\",\"fixture_weights_sha256\":\"{s}\",\"hidden\":{d},\"cases\":[", .{ config_hash, weights_hash, fixture.hidden });
    const Export = struct {
        fn append(writer: *std.Io.Writer, destination: *std.ArrayListUnmanaged(f32), ordinal: usize, kind: []const u8, pooling: []const u8, batch: usize, sequence: usize, hidden: usize, rows: [][]f32) !void {
            try std.testing.expectEqual(batch, rows.len);
            const offset = destination.items.len;
            for (rows) |row| {
                try std.testing.expectEqual(hidden, row.len);
                var norm: f64 = 0;
                for (row) |value| {
                    try std.testing.expect(std.math.isFinite(value));
                    norm += @as(f64, value) * value;
                }
                try std.testing.expectApproxEqAbs(@as(f64, 1), norm, 1e-4);
                try destination.appendSlice(Q.a, row); // preserve exact returned F32 bits/order
            }
            if (ordinal != 0) try writer.writeAll(",");
            try writer.print("{{\"ordinal\":{d},\"kind\":\"{s}\",\"pooling\":\"{s}\",\"shape\":[{d},{d}],\"input_sequence\":", .{ ordinal, kind, pooling, batch, hidden });
            if (sequence == 0) try writer.writeAll("null") else try writer.print("{d}", .{sequence});
            try writer.print(",\"offset_elements\":{d},\"elements\":{d}}}", .{ offset, destination.items.len - offset });
        }
    };
    var ids: [32]i64 = undefined;
    var mask: [32]i64 = undefined;
    var ordinal: usize = 0;
    // Exactly the original fixture's sequence/order; repeats preserve a real
    // warm baseline, and B2/shape changes exercise the established schedules.
    for ([_][2]usize{ .{ 1, 16 }, .{ 1, 16 }, .{ 2, 16 }, .{ 2, 16 }, .{ 1, 7 }, .{ 1, 16 } }) |shape| {
        const batch = shape[0];
        const sequence = shape[1];
        Q.inputs(batch, sequence, &ids, &mask);
        const count = batch * sequence;
        const rows = try pipeline.embedTokenized(ids[0..count], mask[0..count], batch, sequence);
        defer freeEmbeddingSlices(Q.a, rows);
        try Export.append(&metadata.writer, &flat, ordinal, "tokenized", "last", batch, sequence, fixture.hidden, rows);
        ordinal += 1;
    }
    const texts = [_][]const u8{ "hello world alpha beta", "tiny query document again" };
    {
        const rows = try pipeline.embed(&texts);
        defer freeEmbeddingSlices(Q.a, rows);
        try Export.append(&metadata.writer, &flat, ordinal, "text", "last", texts.len, 0, fixture.hidden, rows);
        ordinal += 1;
    }
    pipeline.config.pooling = .mean;
    Q.inputs(1, 16, &ids, &mask);
    {
        const rows = try pipeline.embedTokenized(ids[0..16], mask[0..16], 1, 16);
        defer freeEmbeddingSlices(Q.a, rows);
        try Export.append(&metadata.writer, &flat, ordinal, "tokenized", "mean", 1, 16, fixture.hidden, rows);
        ordinal += 1;
    }
    try std.testing.expectEqual(@as(usize, 8), ordinal);
    try metadata.writer.print("],\"elements\":{d}}}\n", .{flat.items.len});
    if (platform.env.getenv("ANTFLY_UNIFIED_DENSE_QWEN68_DEFAULT_OUTPUT")) |path| {
        if (path.len == 0) return error.InvalidDefaultQwenOutputPath;
        // The explicit test runner chooses an ignored QA asset path; no file
        // is emitted by ordinary production inference or without this opt-in.
        try @import("../util/c_file.zig").writeF32LeFile(Q.a, std.testing.io, path, flat.items);
        const sidecar = try std.fmt.allocPrint(Q.a, "{s}.json", .{path});
        defer Q.a.free(sidecar);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = sidecar, .data = metadata.written() });
    }
}

test "Qwen opaque raw callback cannot authorize replay by returning the pre-entry error name" {
    const Probe = struct {
        calls: usize = 0,
        const shape = [_]i64{ -1, -1 };
        const inputs = [_]backends.TensorInfo{
            .{ .name = "input_ids", .dtype = .i64, .shape = &shape },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &shape },
        };
        fn run(raw: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1; // an actual opaque callback already ran
            return error.WorkspaceReceiptChangedBeforeExecution;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &inputs;
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
        fn interruption(_: *anyopaque) @import("../execution_control.zig").Interruption {
            return .cooperative;
        }
        const table: backends.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close, .interruption = interruption };
    };
    var probe: Probe = .{};
    const cfg = gpt_arch.Config{ .family = .qwen3, .norm_type = .rms_norm, .activation = .silu, .position_encoding = .rope, .hidden_size = 128, .intermediate_size = 256, .num_hidden_layers = 1, .num_attention_heads = 1, .num_key_value_heads = 1, .attention_head_dim = 128, .vocab_size = 64 };
    try std.testing.expect(gpt_arch.denseQwen3PrefillEligible(cfg));
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    const session: backends.Session = .{ .ptr = &probe, .vtable = &Probe.table, .execution_gate = &gate, .cohort_scheduler = &scheduler, .model_contract = .{ .decoder_config = &cfg } };
    var pipeline: EmbeddingPipeline = .{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{ .resident_qwen3_embedding = false, .resident_text_encoder = false }, .execution_lock = &gate };
    try std.testing.expectError(error.WorkspaceReceiptChangedBeforeExecution, pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(scheduler.head == null);
}

test "Qwen manual raw callback stays uncharged and admitted missing domain refuses before dispatch" {
    const Probe = struct {
        calls: usize = 0,
        const shape = [_]i64{ -1, -1 };
        const inputs = [_]backends.TensorInfo{
            .{ .name = "input_ids", .dtype = .i64, .shape = &shape },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &shape },
        };
        fn run(raw: *anyopaque, _: []const Tensor, a: std.mem.Allocator) anyerror![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            var tensor = try Tensor.initFloat32(a, "sentence_embedding", &.{ 1, 2 }, &.{ 3, 4 });
            errdefer tensor.deinit();
            const result = try a.alloc(Tensor, 1);
            result[0] = tensor;
            return result;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &inputs;
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
        fn interruption(_: *anyopaque) @import("../execution_control.zig").Interruption {
            return .cooperative;
        }
        const table: backends.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close, .interruption = interruption };
    };
    var probe: Probe = .{};
    const cfg = gpt_arch.Config{ .family = .qwen3, .norm_type = .rms_norm, .activation = .silu, .position_encoding = .rope, .hidden_size = 128, .intermediate_size = 256, .num_hidden_layers = 1, .num_attention_heads = 1, .num_key_value_heads = 1, .attention_head_dim = 128, .vocab_size = 64 };
    try std.testing.expect(gpt_arch.denseQwen3PrefillEligible(cfg));
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    const session: backends.Session = .{ .ptr = &probe, .vtable = &Probe.table, .execution_gate = &gate, .cohort_scheduler = &scheduler, .model_contract = .{ .decoder_config = &cfg } };
    var pipeline: EmbeddingPipeline = .{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{ .normalize = false }, .execution_lock = &gate };
    // No tokenizer/kernel/context is fabricated to make an opaque callback a
    // modern workspace producer. Its public raw host contract is unchanged.
    try std.testing.expect(!pipeline.boundedQwenBatchingEnabled());
    const result = try pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2);
    defer freeEmbeddingSlices(std.testing.allocator, result);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, result[0]);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var controller: memory_admission.AdmissionController = .{};
    pipeline.session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const denied = pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2);
    // Preserve the original expectation if it regresses, without leaking an
    // unexpected successful owning result and masking the routing failure.
    defer if (denied) |unexpected| freeEmbeddingSlices(std.testing.allocator, unexpected) else |_| {};
    try std.testing.expectError(error.MissingWorkspaceAdmissionDomain, denied);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectEqual(memory_admission.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(scheduler.head == null);
}

test "Qwen Source planning callback error name cannot authorize pre-entry replay" {
    const Probe = struct {
        geometry_calls: usize = 0,
        numerical_calls: usize = 0,
        const shape = [_]i64{ -1, -1 };
        const inputs = [_]backends.TensorInfo{
            .{ .name = "input_ids", .dtype = .i64, .shape = &shape },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &shape },
        };
        fn geometry(raw: *anyopaque, _: session_mod.ShapeInputs, _: usize) anyerror!?session_mod.RunGeometry {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.geometry_calls += 1; // unknown callback may have side effects
            return error.WorkspaceReceiptChangedBeforeExecution;
        }
        fn run(raw: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.numerical_calls += 1;
            return error.UnexpectedNumericalDispatch;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &inputs;
        }
        fn kind(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn interruption(_: *anyopaque) @import("../execution_control.zig").Interruption {
            return .cooperative;
        }
        const table: backends.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close, .interruption = interruption, .runGeometry = geometry };
    };
    var probe: Probe = .{};
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    const session: backends.Session = .{ .ptr = &probe, .vtable = &Probe.table, .execution_gate = &gate, .cohort_scheduler = &scheduler };
    var pipeline: EmbeddingPipeline = .{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{ .resident_qwen3_embedding = false, .resident_text_encoder = false }, .execution_lock = &gate };
    try std.testing.expectError(error.WorkspaceReceiptChangedBeforeExecution, pipeline.embedTokenized(&.{ 1, 2 }, &.{ 1, 1 }, 1, 2));
    try std.testing.expectEqual(@as(usize, 1), probe.geometry_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.numerical_calls);
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(scheduler.head == null);
}

// Append to pipelines/embedding.zig after full68 producer/caller composition.
// Reuses the actual QA77c artifact, tokenizer, policy and public pipeline.
const ExistingMetalHostAudit134 = struct {
    actual: session_mod.Session,
    eager_calls: usize = 0,
    raw_calls: usize = 0,
    array_address: usize = 0,
    data_address: usize = 0,
    saw_receipt: bool = false,
    fail_after_math: bool = false,
    fn from(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn accept(value: *const session_mod.Session.VTable) bool {
        return value == &table;
    }
    fn layout(_: *anyopaque) !?@import("../execution/recurrent_state_admission.zig").Layout {
        return null;
    }
    fn eager(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator, control: ?InferenceExecutionControl, context: *const @import("../execution/invocation_context.zig").Context, receipt: ?*@import("../execution/existing_workspace_receipt.zig").Owner) ![]Tensor {
        const self = from(raw);
        self.eager_calls += 1;
        self.saw_receipt = receipt != null and context.workspace_uses != null;
        const output = try self.actual.vtable.eager_host_services.?.run(self.actual.ptr, inputs, a, control, context, receipt);
        self.array_address = @intFromPtr(output.ptr);
        self.data_address = @intFromPtr(output[0].data.ptr);
        if (self.fail_after_math) {
            // A public valid forwarding callback already completed real math.
            // Returning the internal error NAME must not replay this body.
            free(a, output);
            return error.WorkspaceReceiptChangedBeforeExecution;
        }
        return output;
    }
    fn run(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator) ![]Tensor {
        const self = from(raw);
        self.raw_calls += 1;
        return self.actual.vtable.run(self.actual.ptr, inputs, a);
    }
    fn controlled(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator, control: InferenceExecutionControl) ![]Tensor {
        const self = from(raw);
        self.raw_calls += 1;
        return self.actual.vtable.runWithControl(self.actual.ptr, inputs, a, control);
    }
    fn input(raw: *anyopaque) []const backends.TensorInfo {
        return from(raw).actual.inputInfo();
    }
    fn outputInfo(raw: *anyopaque) []const backends.TensorInfo {
        return from(raw).actual.outputInfo();
    }
    fn backend(raw: *anyopaque) backends.BackendType {
        return from(raw).actual.backend();
    }
    fn geometry(raw: *anyopaque, inputs: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
        const self = from(raw);
        return if (self.actual.vtable.runGeometry) |f| try f(self.actual.ptr, inputs, batch) else null;
    }
    fn close(_: *anyopaque) void {
        @panic("borrowed audit header is not an owner");
    }
    const services: session_mod.EagerHostRunServices = .{ .accepts = accept, .recurrent_layout = layout, .run = eager };
    const table: session_mod.Session.VTable = .{ .run = run, .runWithControl = controlled, .inputInfo = input, .outputInfo = outputInfo, .backend = backend, .runGeometry = geometry, .close = close, .eager_host_services = &services };
    fn view(self: *@This()) session_mod.Session {
        var value = self.actual;
        value.ptr = self;
        value.vtable = &table;
        return value;
    }
    fn free(a: std.mem.Allocator, tensors: []Tensor) void {
        for (tensors) |*tensor| tensor.deinit();
        a.free(tensors);
    }
    fn compare(expected: []const Tensor, actual: []const Tensor) !void {
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |x, y| {
            try std.testing.expectEqualSlices(i64, x.shape, y.shape);
            try std.testing.expectEqual(x.dtype, y.dtype);
            for (x.asFloat32(), y.asFloat32()) |left, right| {
                try std.testing.expect(std.math.isFinite(left) and std.math.isFinite(right));
                try std.testing.expectApproxEqAbs(left, right, 1e-4);
            }
        }
    }
    fn roots(session: session_mod.Session) !usize {
        var census = (try @import("../execution/architecture_session.zig").snapshotMetalWorkspaceOutsideExecution(session, DenseQwen68Qa.control)) orelse return 0;
        defer census.deinitOutsideExecution();
        var count: usize = 0;
        for (census.physical.entries[0..census.physical.count]) |entry| if (entry.root != null) {
            count += 1;
        };
        return count;
    }
};

test "Metal actual modern shared roots support zero-credit generic host and explicit Source original outputs before raw tokenized and owned reuse" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    // Strict same-Metal F32 oracle. Default production-Q8 baseline/current
    // vectors remain the independent77e campaign; this test changes no env.
    const q8 = platform.env.getenv("TERMITE_METAL_Q8_RUNTIME_STAGING_MAX_MB") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, q8, "0")) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    if (fixture.hidden != 128) return error.SkipZigTest; // actual supported77c head128 graph fixture
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe: Q.Probe = .{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = Q.Probe.retainContext, .release_context = Q.Probe.releaseContext, .try_reserve = Q.Probe.reserve, .retain = Q.Probe.retain, .release = Q.Probe.release });
    defer domain.controller.configureResourceBudget(null) catch @panic("generic receiving fixture live quota");
    var metal = try session_factory.createMetalSession(Q.a, fixture.path);
    var metal_live = true;
    defer if (metal_live) {
        probe.model_gate = null;
        probe.provider_gate = null;
        Q.close(metal);
    };
    try Q.publishMetalProvider(metal);
    try Q.publishEmbeddingContract(&metal, &manifest);
    try metal.installOwnedExecutionGate(Q.a);
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    metal.cohort_scheduler = &scheduler;
    probe.model_gate = metal.execution_gate;
    const target = (try metal.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    probe.provider_gate = &target.metal.exclusion.mutex;
    metal.model_lifetime.?.admission_domain = domain.pin();
    const MiB = 1024 * 1024;
    metal.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 2048 * MiB, .combined_limit_bytes = 2560 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    var ids: [32]i64 = undefined;
    var mask: [32]i64 = undefined;
    Q.inputs(2, 16, &ids, &mask);
    var token_tensor = try Tensor.initInt64(Q.a, "input_ids", &.{ 2, 16 }, &ids);
    defer token_tensor.deinit();
    var mask_tensor = try Tensor.initInt64(Q.a, "attention_mask", &.{ 2, 16 }, &mask);
    defer mask_tensor.deinit();
    const inputs = [_]Tensor{ token_tensor, mask_tensor };
    try std.testing.expectEqual(@as(usize, 0), try ExistingMetalHostAudit134.roots(metal));
    var pipeline = EmbeddingPipeline.init(Q.a, metal, hf.tokenizer(), Q.config(true, .last));
    pipeline.execution_lock = metal.execution_gate;
    pipeline.execution_control = Q.control;
    // Thirteen real one-piece words + the HF tokenizer's CLS/SEP + the
    // existing trailing-EOS adapter give the SAME natural B2/S16 geometry.
    // Only host tokenization precedes the FIRST public numerical request.
    const texts = [_][]const u8{
        "hello world alpha beta again tiny query document one two three token15 token16",
        "token16 token15 three two one document query tiny again beta alpha world hello",
    };
    for (texts) |text| {
        var encoded = try pipeline.tok.encodeForModelUnpadded(Q.a, text, pipeline.config.max_length);
        defer encoded.deinit();
        EmbeddingPipeline.ensureTrailingEos(&encoded, 63);
        try std.testing.expectEqual(@as(usize, 16), EmbeddingPipeline.activeTokenLength(encoded.attention_mask));
        try std.testing.expectEqual(@as(i32, 63), encoded.ids[15]);
    }
    {
        // Actual fresh-storage producer: neither an old-only tokenized owner
        // nor a generic reference may populate caches before this call.
        const producer = try pipeline.embed(&texts);
        defer freeEmbeddingSlices(Q.a, producer);
        try std.testing.expectEqual(@as(usize, 32), pipeline.last_input_tokens);
        try std.testing.expectEqual(@as(usize, 2), producer.len);
        for (producer) |row| try Q.compare(row, row, fixture.hidden);
        try std.testing.expect((try ExistingMetalHostAudit134.roots(metal)) > 0);
        const warm_text = try pipeline.embed(&texts);
        defer freeEmbeddingSlices(Q.a, warm_text);
        for (producer, warm_text) |expected, actual| try Q.compare(expected, actual, fixture.hidden);
    }
    const root_count = try ExistingMetalHostAudit134.roots(metal);
    try std.testing.expect(root_count > 0); // actual tagged SDK roots survive producer output release
    // The generic reference is deliberately AFTER the actual producer. Its
    // existing F32 tensor/shape/numerical gate remains the consumer oracle.
    const reference = try metal.runWithControl(&inputs, Q.a, Q.control);
    defer ExistingMetalHostAudit134.free(Q.a, reference);
    const original_quota = domain.controller.snapshot();
    var audit: ExistingMetalHostAudit134 = .{ .actual = metal };
    const forwarded = audit.view(); // actual canonical provider, different public ptr/table
    const hosted = try forwarded.runWithControl(&inputs, Q.a, Q.control);
    var hosted_live = true;
    defer if (hosted_live) ExistingMetalHostAudit134.free(Q.a, hosted);
    try std.testing.expect(audit.saw_receipt and audit.eager_calls == 1 and audit.raw_calls == 0);
    try std.testing.expectEqual(audit.array_address, @intFromPtr(hosted.ptr));
    try std.testing.expectEqual(audit.data_address, @intFromPtr(hosted[0].data.ptr));
    try ExistingMetalHostAudit134.compare(reference, hosted);
    try std.testing.expect(scheduler.head == null);
    try std.testing.expect((try ExistingMetalHostAudit134.roots(metal)) > 0);
    {
        var source: session_mod.PreparedSource = .{};
        defer source.deinit();
        try source.initBeforeExecutionInto(metal, metal, metal.execution_gate, .{ .host = &inputs }, try metal.planRun(&inputs, null), null, .host, Q.a, Q.control);
        try std.testing.expectError(error.SourceInvocationNotExecuting, source.existingWorkspaceReceipt());
        try source.enter();
        const receipt = (try source.existingWorkspaceReceipt()) orelse return error.MissingActualExistingWorkspaceReceipt;
        try std.testing.expect(receipt.receiver.allocations == null and receipt.bank.count > 0);
        try std.testing.expect(receipt.bank.epoch.?.record == source.epoch().?.record and receipt.bank.epoch.?.nonce == source.epoch().?.nonce);
        const output = try source.runHost(&inputs);
        defer ExistingMetalHostAudit134.free(Q.a, output);
        try ExistingMetalHostAudit134.compare(reference, output);
        try std.testing.expectError(error.SourceInvocationNotExecuting, source.existingWorkspaceReceipt());
    }
    // Same original encoder/normalization policy through both public owners.
    const raw = try pipeline.embedTokenized(&ids, &mask, 2, 16);
    defer freeEmbeddingSlices(Q.a, raw);
    var owned = try pipeline.embedTokenizedOwned(&ids, &mask, 2, 16, .{ .outside_execution = true });
    defer owned.deinitOutsideExecution();
    for (raw, 0..) |row, index| try Q.compare(row, owned.values(index), fixture.hidden);
    const before_cancel = domain.controller.snapshot();
    const Cancel = struct {
        fn now(_: ?*anyopaque) bool {
            return true;
        }
    };
    var canceled_control = Q.control;
    canceled_control.cancellation = .{ .is_cancelled_fn = Cancel.now };
    try std.testing.expectError(error.Cancelled, forwarded.runWithControl(&inputs, Q.a, canceled_control));
    try std.testing.expectEqualDeep(before_cancel, domain.controller.snapshot());
    var denied = forwarded;
    denied.run_admission.?.limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 1, .combined_limit_bytes = 1 };
    try std.testing.expectError(error.ResourceLimitExceeded, denied.runWithControl(&inputs, Q.a, Q.control));
    try std.testing.expectEqualDeep(before_cancel, domain.controller.snapshot());
    {
        const before_error = domain.controller.snapshot();
        var fault_audit: ExistingMetalHostAudit134 = .{ .actual = metal, .fail_after_math = true };
        const fault_forwarded = fault_audit.view();
        try std.testing.expectError(error.WorkspaceReceiptChangedBeforeExecution, fault_forwarded.runWithControl(&inputs, Q.a, Q.control));
        try std.testing.expectEqual(@as(usize, 1), fault_audit.eager_calls);
        try std.testing.expectEqual(@as(usize, 0), fault_audit.raw_calls);
        try std.testing.expect(fault_audit.saw_receipt);
        try std.testing.expectEqualDeep(before_error, domain.controller.snapshot());
        try std.testing.expect(scheduler.head == null);
    }
    try std.testing.expect(!probe.violation);
    std.debug.print("existing-root134 tagged_roots={d} before_backend={d} after_backend={d} original_host_pointer=true\n", .{ root_count, original_quota.backend_scratch_bytes, before_cancel.backend_scratch_bytes });
    // Hosted original data is independent of actual Model/provider lifetime.
    owned.deinitOutsideExecution();
    probe.model_gate = null;
    probe.provider_gate = null;
    Q.close(metal);
    metal_live = false;
    try std.testing.expectEqual(audit.data_address, @intFromPtr(hosted[0].data.ptr));
    try ExistingMetalHostAudit134.compare(reference, hosted);
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().backend_scratch_bytes);
    ExistingMetalHostAudit134.free(Q.a, hosted);
    hosted_live = false;
    try std.testing.expect(!probe.violation);
}

test "Metal bounded public Qwen original task escrow preserves sorted rows after source close and foreign B" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const policy = platform.env.getenv("TERMITE_METAL_Q8_RUNTIME_STAGING_MAX_MB") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, policy, "0")) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    if (fixture.hidden != 128) return error.SkipZigTest;
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    const tokenizer = try Q.tokenizer(fixture.path);
    defer tokenizer.deinitSelf();
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe: Q.Probe = .{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = Q.Probe.retainContext, .release_context = Q.Probe.releaseContext, .try_reserve = Q.Probe.reserve, .retain = Q.Probe.retain, .release = Q.Probe.release });
    defer domain.controller.configureResourceBudget(null) catch @panic("Qwen task escrow live quota");
    var model = try session_factory.createMetalSession(Q.a, fixture.path);
    var model_live = true;
    defer if (model_live) {
        probe.model_gate = null;
        probe.provider_gate = null;
        Q.close(model);
    };
    try Q.publishMetalProvider(model);
    try Q.publishEmbeddingContract(&model, &manifest);
    try model.installOwnedExecutionGate(Q.a);
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    model.cohort_scheduler = &scheduler;
    model.model_lifetime.?.admission_domain = domain.pin();
    const MiB = 1024 * 1024;
    model.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 2048 * MiB, .combined_limit_bytes = 2560 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    probe.model_gate = model.execution_gate;
    const target = (try model.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    probe.provider_gate = &target.metal.exclusion.mutex;
    var pipeline = EmbeddingPipeline.init(Q.a, model, tokenizer.tokenizer(), Q.config(true, .last));
    pipeline.execution_lock = model.execution_gate;
    pipeline.execution_control = Q.control;
    try std.testing.expect(pipeline.boundedQwenBatchingEnabled() and pipeline.canEmbedOwned());
    const texts = [_][]const u8{
        "hello world alpha beta again tiny query document one two three token15 token16",
        "hello world alpha beta",
        "token16 token15 three two one document query tiny again beta alpha world hello",
        "beta alpha world hello",
        "hello world alpha beta again tiny query document one two three token15 token16",
    };
    // FIRST inference is the SAME public bounded producer; no eager or
    // tokenized reference can prepopulate legacy caches to fake ownership.
    var owned = try pipeline.embedOwned(&texts, .{ .outside_execution = true });
    defer owned.deinitOutsideExecution();
    const usage = pipeline.last_input_tokens;
    try std.testing.expect(usage > 0 and owned.len() == texts.len);
    try std.testing.expect((try ExistingMetalHostAudit134.roots(model)) > 0);
    const raw = try pipeline.embed(&texts);
    defer freeEmbeddingSlices(Q.a, raw);
    try std.testing.expectEqual(usage, pipeline.last_input_tokens);
    for (raw, 0..) |row, index| try Q.compare(row, owned.values(index), fixture.hidden);
    try Q.compare(owned.values(0), owned.values(4), fixture.hidden);
    try std.testing.expect(scheduler.head == null and !probe.violation);
    var denied = pipeline;
    denied.session.run_admission.?.limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 1, .combined_limit_bytes = 1 };
    const before_denied = domain.controller.snapshot();
    try std.testing.expectError(error.ResourceLimitExceeded, denied.embedOwned(&texts, .{ .outside_execution = true }));
    try std.testing.expectEqualDeep(before_denied, domain.controller.snapshot());
    var taken = try owned.take(2);
    defer taken.deinitOutsideExecution();
    // Final rows contain no numerical owner or Model lifetime. Source closes
    // before a separate foreign consumer releases either container or row.
    probe.model_gate = null;
    probe.provider_gate = null;
    Q.close(model);
    model_live = false;
    const host_bytes = try session_mod.HostOutputProducer.requiredBytes(texts.len, fixture.hidden);
    const deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
    while (!std.meta.eql(domain.controller.snapshot(), memory_admission.AdmissionAmounts{ .host_scratch_bytes = host_bytes })) {
        _ = @import("../execution/resource_retirement.zig").poll();
        if (platform.time.monotonicNs() >= deadline) return error.QwenTaskSourceDidNotRetire;
        std.Thread.yield() catch {};
    }
    try Q.compare(raw[2], taken.values(), fixture.hidden);
    const Sink = @import("../execution/retirement_sink.zig");
    var sink = try Sink.Ref.create();
    defer sink.release();
    var foreign = try sink.enter();
    defer foreign.finish();
    var epoch = try foreign.retainEpoch();
    defer epoch.release();
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    var held = true;
    defer if (held) gate.unlock();
    probe.foreign_gate = &gate;
    const releases = probe.releases;
    try owned.deinitWithCleanup(.{ .boundary = &epoch });
    try taken.deinitWithCleanup(.{ .boundary = &epoch });
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqual(releases, probe.releases);
    try std.testing.expectEqual(host_bytes, domain.controller.snapshot().host_scratch_bytes);
    gate.unlock();
    held = false;
    foreign.finish();
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(!probe.violation);
}

test "Metal committed workspace allocation denial closes once and healthy cold and warm family scopes recover" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    const architecture = @import("../execution/architecture_session.zig");
    const ownership = @import("../backends/metal_workspace_ownership.zig");
    const roles = @import("../backends/metal_workspace_roles.zig");
    const Metal = @import("../ops/metal_compute.zig").MetalCompute;
    const Owner = @import("../execution/cleanup_owner.zig").Owner;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe: Q.Probe = .{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = Q.Probe.retainContext, .release_context = Q.Probe.releaseContext, .try_reserve = Q.Probe.reserve, .retain = Q.Probe.retain, .release = Q.Probe.release });
    defer domain.controller.configureResourceBudget(null) catch @panic("close94 live quota");
    var session = try session_factory.createMetalSession(Q.a, fixture.path);
    var session_live = true;
    defer if (session_live) {
        probe.model_gate = null;
        probe.provider_gate = null;
        Q.close(session);
    };
    try Q.publishMetalProvider(session);
    try session.installOwnedExecutionGate(Q.a);
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    session.cohort_scheduler = &scheduler;
    session.model_lifetime.?.admission_domain = domain.pin();
    probe.model_gate = session.execution_gate;
    const target = (try session.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    probe.provider_gate = &target.metal.exclusion.mutex;
    const MiB = 1024 * 1024;
    session.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 2048 * MiB, .combined_limit_bytes = 2560 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    const cfg = session_factory.getGptConfig(session) orelse return error.MissingDecoderGeometry;
    const request: session_mod.RunRequest = .{ .batch = 1, .sequence = 1, .input_bytes = 16, .output_bytes = fixture.hidden * 4 };
    // Real C guard refusal BEFORE SDK/take, healthy original preparation,
    // then the SAME refusal and healthy reuse on the warmed real provider.
    for ([_]bool{ true, false, true, false }) |deny| {
        var cleanup = try Owner.beginResident(session);
        defer cleanup.finishAfterUnlock();
        const epoch = cleanup.epoch() orelse return error.MissingCurrentCleanupEpoch;
        var described = (try architecture.describeQwenWorkspaceAuxiliaryOutsideExecution(session, 0, Q.control)) orelse return error.MissingWorkspaceDescription;
        defer described.deinitOutsideExecution();
        const old_count = (try described.census.physical.capturedCoverage()).witnessed_roots;
        const count = described.auxiliary.plan.fresh_slots;
        const bytes = described.auxiliary.plan.fresh_bytes;
        const host = try EmbeddingPipeline.qwenWorkspaceHostDeclaration(described.census.physical.entries.len, count, old_count, 0, 0);
        var declarations: [2]@import("../execution/prepared_storage_admission.zig").Requirement = undefined;
        var declaration_count: usize = 0;
        if (bytes != 0) {
            declarations[declaration_count] = .{ .backend = .metal, .class = .workspace, .bytes = bytes };
            declaration_count += 1;
        }
        if (host != 0) {
            declarations[declaration_count] = .{ .backend = .native, .class = .host_workspace, .bytes = host };
            declaration_count += 1;
        }
        var pressure: ?memory_admission.AdmissionPressure = null;
        var permit = try session.admitDeclaredStorageWithPressure(request, declarations[0..declaration_count], &pressure);
        defer permit.deinit();
        var phase = try ownership.Phase.prepareFromPermitOutsideExecution(session, &permit, &described.census.target, count, bytes, old_count, Q.control);
        defer phase.finishOutsideExecution();
        const root_storage = try Q.a.alloc(*ownership.Root, described.census.physical.entries.len);
        defer Q.a.free(root_storage);
        const old_roots = try described.census.physical.copyRoots(root_storage);
        var bank = try ownership.UseBank.prepareExact(old_roots.len);
        defer bank.deinitOutsideExecution();
        try phase.fundReceivingBankOutsideExecution(&bank);
        try bank.bindOutsideExecution(old_roots, epoch);
        var validation = try roles.Snapshot.prepare(described.census.physical.entries.len);
        defer validation.deinitOutsideExecution();
        const final_capacity = try std.math.add(usize, described.census.physical.entries.len, count);
        var final = try roles.Snapshot.prepare(final_capacity);
        defer final.deinitOutsideExecution();
        const final_roots = try Q.a.alloc(*ownership.Root, final_capacity);
        defer Q.a.free(final_roots);
        var uses: ownership.ReceiverUses = .{ .existing = &bank, .allocations = &phase };
        var current = CleanupContext{ .boundary = epoch };
        var context: @import("../execution/invocation_context.zig").Context = .{ .cleanup = current, .workspace_uses = &uses };
        var waiter: @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter = .{};
        try session.selectReadyCohort(session.execution_gate, Q.control, &waiter);
        defer session.leaveSelectedCohort(&waiter);
        {
            try session.acquireSelectedCohort(Q.control, &waiter);
            defer session.releaseSelectedCohortGate(&waiter);
            var managed = try architecture.constructMetalWorkspaceReceiver(session, Q.a, Q.control);
            defer managed.deinitWithCleanup(current);
            const cb = &managed.backend;
            const compute: *Metal = @ptrCast(@alignCast(cb.ptr));
            const runtime = compute.provider_impl.raw_decode_runtime orelse return error.ExpectedActualMetalRuntime;
            try std.testing.expect(try cb.bindCleanupEpoch(epoch));
            current.operation = cb.operationToken();
            context.cleanup = current;
            var binding = try Metal.beginPreparedAuxiliaryWorkspaceInvocation(cb, &context, &described.census.physical, &validation, &described.auxiliary);
            var active = true;
            defer if (active) {
                _ = binding.closeReported(&final, final_roots) catch platform.inference_process_supervisor.restartWorker();
            };
            try @import("../backends/compute_factory.zig").beginRequestWithCleanup(session, cb, current);
            const before = domain.controller.snapshot();
            const before_sdk = metal_runtime.runtimeMemorySnapshot(runtime).scratch_bytes;
            if (deny) {
                // Deliberately request MORE than the actual scope's complete
                // declaration. Neither C guard nor Phase may invent credit.
                const elements = phase.envelope.?.reservedBytes() / 4 + 1;
                const shape = [_]i32{ 1, @intCast(elements) };
                // Fresh allocation bypasses a warm reuse-pool hit, without
                // bypassing the SAME allocator guard or typed Phase service.
                const attempted = metal_runtime.MetalTensor.deviceAllocateFreshWithAllocator(Q.a, @ptrCast(runtime), elements * 4, .private, &shape);
                defer if (attempted) |value| {
                    var tensor = value;
                    tensor.deinitWithCleanup(current);
                } else |_| {};
                try std.testing.expectError(error.MetalBufferAllocFailed, attempted);
                try std.testing.expectEqual(@as(usize, 0), phase.cursor);
                try std.testing.expect(phase.failure == null); // actual pre-take C budget denial
                try std.testing.expectEqual(before_sdk, metal_runtime.runtimeMemorySnapshot(runtime).scratch_bytes);
                active = false; // a close, including a failing close, is never retried
                const closed = try binding.closeReported(&final, final_roots);
                uses.allocations = null;
                try std.testing.expect(closed == .allocation_denied);
                try std.testing.expectError(error.ResourceLimitExceeded, closed.check());
                try std.testing.expectEqualDeep(before, domain.controller.snapshot());
            } else {
                const prepared = try cb.decoderRuntimePrepareOrReuseTextPrefill(Q.a, cfg, cfg.num_hidden_layers);
                active = false; // a close, including a failing close, is never retried
                const closed = try binding.closeReported(&final, final_roots);
                uses.allocations = null;
                try std.testing.expect(prepared.prepared);
                try std.testing.expect(closed == .closed);
                try closed.check();
            }
            try std.testing.expect(cb.call_context == null and compute.workspace_invocation_context == null);
            try std.testing.expect(phase.current == null and uses.bound_operation == null);
        }
        try std.testing.expect(scheduler.head == &waiter and !waiter.acquired);
        try std.testing.expect(!probe.violation);
    }
    try std.testing.expect(scheduler.head == null);
    try std.testing.expect(domain.controller.snapshot().backend_scratch_bytes > 0); // actual prepared scratch retained
    probe.model_gate = null;
    probe.provider_gate = null;
    Q.close(session);
    session_live = false;
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(!probe.violation);
}

test "Gemma public text original source task escrow preserves four-row windows compact cohorts and later B" {
    const model_path = platform.env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fs.path.join(a, &.{ model_path, "tokenizer.json" });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    defer a.free(bytes);
    const hf = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer hf.deinitSelf();
    const Lane = @import("../execution_control.zig").ExecutionLane;
    const Probe = struct {
        model_gate: ?*std.atomic.Mutex = null,
        lane: *Lane,
        violation: std.atomic.Value(bool) = .init(false),
        next: std.atomic.Value(usize) = .init(1),
        fn observe(self: *@This()) void {
            if (self.model_gate) |gate| {
                if (gate.tryLock()) gate.unlock() else self.violation.store(true, .release);
            }
            if (!self.lane.mutex.tryLock()) {
                self.violation.store(true, .release);
                return;
            }
            const held = self.lane.owned;
            self.lane.mutex.unlock();
            if (held) self.violation.store(true, .release);
        }
        fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            return self.next.fetchAdd(1, .monotonic);
        }
        fn retain(raw: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn close(session: session_mod.Session) void {
            var sink = @import("../execution/retirement_sink.zig").Ref.create() catch @panic("Gemma task close sink");
            defer sink.release();
            var guard = sink.enter() catch @panic("Gemma task close guard");
            defer guard.finish();
            var epoch = guard.retainEpoch() catch @panic("Gemma task close epoch");
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    const texts = [_][]const u8{
        "A longer document about a red bus, a quiet street, trees and a child holding a blue umbrella.",
        "A red bus.",
        "The complete document describes several trees beside the quiet street and a blue umbrella.",
        "A quiet street.",
        "A tiny blue umbrella.",
        "Trees surround a bus on a quiet street while children wait and a pedestrian carries an umbrella.",
        "A red bus.",
    };
    const control: InferenceExecutionControl = .{ .native_call_policy = .in_process };
    for ([_]backends.BackendType{ .native, .metal, .cuda }) |kind| {
        if (kind == .native and !build_options.enable_native) continue;
        if (kind == .metal and !build_options.enable_metal) continue;
        if (kind == .cuda and !build_options.enable_cuda) continue;
        var lane: Lane = .{};
        var probe: Probe = .{ .lane = &lane };
        const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
        defer domain.release();
        try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        defer domain.controller.configureResourceBudget(null) catch @panic("Gemma task live budget");
        var session = switch (kind) {
            .native => try session_factory.createNativeSession(a, model_path),
            .metal => try session_factory.createMetalSession(a, model_path),
            .cuda => session_factory.createCudaSession(a, model_path) catch |err| switch (err) {
                error.CudaUnavailable, error.NoCudaDevices => continue,
                else => return err,
            },
            else => unreachable,
        };
        var live = true;
        defer if (live) {
            probe.model_gate = null;
            Probe.close(session);
        };
        session_factory.attachIo(session, std.testing.io);
        try session.installOwnedExecutionGate(a);
        var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
        session.cohort_scheduler = &scheduler;
        try std.testing.expect(session.model_lifetime.?.admission_domain == null);
        session.model_lifetime.?.admission_domain = domain.pin();
        const GiB: usize = 1024 * 1024 * 1024;
        session.run_admission = .{ .controller = &domain.controller, .backend_class = if (kind == .native) .cpu else .gpu, .limits = .{ .host_limit_bytes = 8 * GiB, .backend_limit_bytes = 4 * GiB }, .static_workspace_bytes = 0, .check_live_memory = false };
        if (kind == .native) {
            // Match the production published Model/cache Domain before any
            // forward. The first call below must cold-publish typed entries.
            try @import("../execution/architecture_session.zig").configureSharedCacheAdmissionForSession(session, a, &domain.controller, .cpu, session.run_admission.?.limits, .{});
            const data = @import("../execution/architecture_session.zig").nativePanelPreparationModelData(session) orelse return error.MissingCanonicalNativeModel;
            try std.testing.expect(data.prefetch_initialized);
            try std.testing.expectEqual(@as(usize, 0), data.embeddinggemma2_f32_cache.count());
            try std.testing.expectEqual(@as(usize, 0), data.embeddinggemma2_canonical_panels.count());
        }
        probe.model_gate = session.execution_gate;
        var pipeline = EmbeddingPipeline.init(a, session, hf.tokenizer(), .{ .resident_text_encoder = true, .normalize = true, .pooling = .mean, .max_length = 128, .strict_max_length = true, .trim_padding_to_batch_max = true, .embedding_gemma2_dimensions = 768, .text_prefix = try @import("../architectures/embedding_gemma2.zig").taskPrefix("CLUSTERING") });
        pipeline.execution_control = control;
        pipeline.execution_lock = session.execution_gate;
        pipeline.execution_lane = &lane;
        try std.testing.expect(pipeline.gemmaOwnedTextEligible() and pipeline.canEmbedOwned());
        if (kind == .cuda) {
            // A provider/config hint cannot authorize a foreign output callback.
            var foreign_table = session.vtable.*;
            var foreign = pipeline;
            foreign.session.vtable = &foreign_table;
            try std.testing.expect(!foreign.canEmbedOwned());
            var projected = pipeline;
            projected.text_projection = session;
            try std.testing.expect(!projected.canEmbedOwned());
            var other_pooling = pipeline;
            other_pooling.config.pooling = .cls;
            try std.testing.expect(!other_pooling.canEmbedOwned());
            var invalid_width = pipeline;
            invalid_width.config.embedding_gemma2_dimensions = 129;
            try std.testing.expect(!invalid_width.canEmbedOwned());
        }
        var owned_trace: embedding_trace.Trace = .{ .allocator = a, .io = std.testing.io, .directory = "", .id = 0, .started_ns = 0, .path = "", .model = "", .inputs = &texts, .task_type = "CLUSTERING", .instruction = null };
        pipeline.trace = &owned_trace;
        // FIRST forward is actual public text owned with the same real lane;
        // no tokenized/reference warmup or alternate encoder is introduced.
        var owned = try pipeline.embedOwned(&texts, .{ .outside_execution = true });
        defer owned.deinitOutsideExecution();
        if (kind == .native) {
            const data = @import("../execution/architecture_session.zig").nativePanelPreparationModelData(session) orelse return error.MissingCanonicalNativeModel;
            if (!@import("../backends/native.zig").useBlas()) {
                try std.testing.expect(data.embeddinggemma2_canonical_panels.count() > 0);
                try std.testing.expect(data.canonical_panel_prepared_map_bytes > 0);
                var entries = data.embeddinggemma2_canonical_panels.valueIterator();
                while (entries.next()) |entry| try std.testing.expect(entry.prepared_physical_bytes > 0);
            }
            try std.testing.expect(!probe.violation.load(.acquire) and !lane.owned and scheduler.head == null);
        }
        var no_normalize_held: ?session_mod.OwnedHostVectors = null;
        defer if (no_normalize_held) |*value| value.deinitOutsideExecution();
        var no_normalize_expected: [128]f32 = undefined;
        try std.testing.expectEqual(@as(usize, texts.len), owned.len());
        var raw_trace: embedding_trace.Trace = .{ .allocator = a, .io = std.testing.io, .directory = "", .id = 0, .started_ns = 0, .path = "", .model = "", .inputs = &texts, .task_type = "CLUSTERING", .instruction = null };
        pipeline.trace = &raw_trace;
        const raw = try pipeline.embed(&texts);
        defer freeEmbeddingSlices(a, raw);
        try std.testing.expect(owned_trace.shape_count > 0);
        try std.testing.expectEqual(owned_trace.shape_count, raw_trace.shape_count);
        try std.testing.expectEqualDeep(owned_trace.shapes[0..owned_trace.shape_count], raw_trace.shapes[0..raw_trace.shape_count]);
        for (raw, 0..) |row, index| {
            try std.testing.expectEqual(@as(usize, 768), row.len);
            if (kind == .cuda) {
                try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(row), std.mem.sliceAsBytes(owned.values(index)));
            } else for (row, owned.values(index)) |x, y| try std.testing.expectApproxEqAbs(x, y, 1e-5);
        }
        for (owned.values(1), owned.values(6)) |x, y| try std.testing.expectApproxEqAbs(x, y, 1e-5);
        {
            var no_normalize = pipeline;
            no_normalize.trace = null;
            no_normalize.config.normalize = false;
            const original = try no_normalize.embed(texts[0..1]);
            defer freeEmbeddingSlices(a, original);
            try std.testing.expectEqual(@as(usize, 768), original[0].len);
            no_normalize.config.embedding_gemma2_dimensions = 128;
            const prefix = try no_normalize.embed(texts[0..1]);
            defer freeEmbeddingSlices(a, prefix);
            try std.testing.expectEqual(@as(usize, 128), prefix[0].len);
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(original[0][0..128]), std.mem.sliceAsBytes(prefix[0]));
            if (kind == .cuda) {
                const Observation = @import("batch_execution.zig").Observation;
                const before_false = domain.controller.snapshot();
                var no_sources: Observation = .{};
                var foreign_table = session.vtable.*;
                var missing = no_normalize;
                missing.session.vtable = &foreign_table;
                missing.batch_observation = &no_sources;
                try std.testing.expect(!missing.canEmbedOwned());
                try std.testing.expectError(error.UnsupportedOwnedEmbeddingEntry, missing.embedOwned(texts[0..1], .{ .outside_execution = true }));
                try std.testing.expectEqual(@as(usize, 0), no_sources.serial_items);
                try std.testing.expectEqualDeep(before_false, domain.controller.snapshot());
                // Genuine canonical table, but no retained Model proof. The
                // receiver must refuse before any source/opaque owner access.
                missing = no_normalize;
                missing.session.model_lifetime = null;
                missing.batch_observation = &no_sources;
                try std.testing.expect(!missing.canEmbedOwned());
                try std.testing.expectError(error.UnsupportedOwnedEmbeddingEntry, missing.embedOwned(texts[0..1], .{ .outside_execution = true }));
                try std.testing.expectEqual(@as(usize, 0), no_sources.serial_items);
                try std.testing.expectEqualDeep(before_false, domain.controller.snapshot());
                var bank_below = no_normalize;
                bank_below.batch_observation = &no_sources;
                bank_below.session.run_admission.?.limits.host_limit_bytes = (try session_mod.HostOutputProducer.requiredBytes(1, 128)) - 1;
                try std.testing.expectError(error.ResourceLimitExceeded, bank_below.embedOwned(texts[0..1], .{ .outside_execution = true }));
                try std.testing.expectEqual(@as(usize, 0), no_sources.serial_items);
                const denied_deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
                while (!std.meta.eql(before_false, domain.controller.snapshot())) {
                    _ = @import("../execution/resource_retirement.zig").poll();
                    if (platform.time.monotonicNs() >= denied_deadline) return error.GemmaCudaUnnormalizedDeniedBankDidNotRetire;
                    std.Thread.yield() catch {};
                }
                for ([_]usize{ 128, 256, 512, 768 }) |width| {
                    no_normalize.config.embedding_gemma2_dimensions = width;
                    try std.testing.expect(no_normalize.canEmbedOwned());
                    try std.testing.expectEqual(width, try no_normalize.ownedTextOutputWidth());
                    var actual = try no_normalize.embedOwned(texts[0..1], .{ .outside_execution = true });
                    var actual_live = true;
                    defer if (actual_live) actual.deinitOutsideExecution();
                    const direct = try no_normalize.embed(texts[0..1]);
                    defer freeEmbeddingSlices(a, direct);
                    try std.testing.expectEqual(@as(usize, 1), actual.len());
                    try std.testing.expectEqual(width, actual.values(0).len);
                    // Independent original full-width source prefix, with NO
                    // additional L2 for the false option; exact F32 bytes.
                    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(original[0][0..width]), std.mem.sliceAsBytes(actual.values(0)));
                    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(direct[0]), std.mem.sliceAsBytes(actual.values(0)));
                    if (width == 128) {
                        @memcpy(&no_normalize_expected, actual.values(0));
                        no_normalize_held = actual;
                        actual_live = false;
                    }
                }
            } else try std.testing.expect(!no_normalize.canEmbedOwned());
        }
        if (kind == .cuda) {
            const full_tokens = pipeline.last_input_tokens;
            for ([_]usize{ 128, 256, 512 }) |width| {
                var reduced = pipeline;
                reduced.trace = null;
                reduced.config.embedding_gemma2_dimensions = width;
                try std.testing.expect(reduced.canEmbedOwned());
                var reduced_owned = try reduced.embedOwned(&texts, .{ .outside_execution = true });
                defer reduced_owned.deinitOutsideExecution();
                try std.testing.expectEqual(full_tokens, reduced.last_input_tokens);
                const reduced_raw = try reduced.embed(&texts);
                defer freeEmbeddingSlices(a, reduced_raw);
                try std.testing.expectEqual(full_tokens, reduced.last_input_tokens);
                for (raw, reduced_raw, 0..) |full, row, index| {
                    var expected: [768]f32 = undefined;
                    @memcpy(expected[0..width], full[0..width]);
                    linalg.l2Normalize(expected[0..width], width);
                    try std.testing.expectEqual(width, row.len);
                    try std.testing.expectEqual(width, reduced_owned.values(index).len);
                    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected[0..width]), std.mem.sliceAsBytes(row));
                    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(row), std.mem.sliceAsBytes(reduced_owned.values(index)));
                }
            }
        }
        try std.testing.expect(!probe.violation.load(.acquire) and !lane.owned and scheduler.head == null);
        var cancel_trace: embedding_trace.Trace = .{ .allocator = a, .io = std.testing.io, .directory = "", .id = 0, .started_ns = 0, .path = "", .model = "", .inputs = &texts, .task_type = "CLUSTERING", .instruction = null };
        const Observation = @import("batch_execution.zig").Observation;
        var committed_cancel: Observation = .{};
        const CancelAfterCommittedCohort = struct {
            trace: *embedding_trace.Trace,
            forwards: ?*Observation = null,
            fn check(cancel_context: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(cancel_context.?));
                if (self.forwards) |forwards| {
                    if (forwards.serial_items != 0) return error.Cancelled;
                } else if (self.trace.shape_count != 0) return error.Cancelled;
            }
        };
        var cancel_probe: CancelAfterCommittedCohort = .{ .trace = &cancel_trace, .forwards = if (kind == .cuda) &committed_cancel else null };
        var cancel_control = control;
        cancel_control.ptr = &cancel_probe;
        cancel_control.check_fn = CancelAfterCommittedCohort.check;
        var cancelled = pipeline;
        cancelled.execution_control = cancel_control;
        cancelled.trace = &cancel_trace;
        if (kind == .cuda) cancelled.batch_observation = &committed_cancel;
        const before_cancel = domain.controller.snapshot();
        try std.testing.expectError(error.Cancelled, cancelled.embedOwned(&texts, .{ .outside_execution = true }));
        if (kind == .cuda) try std.testing.expectEqual(@as(usize, 1), committed_cancel.serial_items) else try std.testing.expect(cancel_trace.shape_count > 0);
        const cancel_deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
        while (!std.meta.eql(before_cancel, domain.controller.snapshot())) {
            _ = @import("../execution/resource_retirement.zig").poll();
            if (platform.time.monotonicNs() >= cancel_deadline) return error.GemmaCancelledHostBankDidNotRetire;
            std.Thread.yield() catch {};
        }
        try std.testing.expect(!lane.owned and scheduler.head == null and !probe.violation.load(.acquire));
        if (kind == .cuda) {
            var false_committed: Observation = .{};
            var false_probe: CancelAfterCommittedCohort = .{ .trace = &cancel_trace, .forwards = &false_committed };
            var false_control = control;
            false_control.ptr = &false_probe;
            false_control.check_fn = CancelAfterCommittedCohort.check;
            var false_cancelled = pipeline;
            false_cancelled.config.normalize = false;
            false_cancelled.config.embedding_gemma2_dimensions = 128;
            false_cancelled.trace = null;
            false_cancelled.execution_control = false_control;
            false_cancelled.batch_observation = &false_committed;
            try std.testing.expectError(error.Cancelled, false_cancelled.embedOwned(&texts, .{ .outside_execution = true }));
            try std.testing.expectEqual(@as(usize, 1), false_committed.serial_items);
            const false_deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
            while (!std.meta.eql(before_cancel, domain.controller.snapshot())) {
                _ = @import("../execution/resource_retirement.zig").poll();
                if (platform.time.monotonicNs() >= false_deadline) return error.GemmaCudaUnnormalizedCancelledBankDidNotRetire;
                std.Thread.yield() catch {};
            }
            try std.testing.expect(!lane.owned and scheduler.head == null and !probe.violation.load(.acquire));
            // Fault after an actual first Source completed, not after merely
            // tokenizing. Existing final owners must unwind without replay.
            var committed_oom: Observation = .{};
            const AllocationFailureAfterSource = struct {
                forwards: *Observation,
                fn check(raw_context: ?*anyopaque) !void {
                    const fault: *@This() = @ptrCast(@alignCast(raw_context.?));
                    if (fault.forwards.serial_items != 0) return error.OutOfMemory;
                }
            };
            var fault: AllocationFailureAfterSource = .{ .forwards = &committed_oom };
            var oom_control = control;
            oom_control.ptr = &fault;
            oom_control.check_fn = AllocationFailureAfterSource.check;
            var failing = pipeline;
            failing.trace = null;
            failing.batch_observation = &committed_oom;
            failing.execution_control = oom_control;
            const before_oom = domain.controller.snapshot();
            try std.testing.expectError(error.OutOfMemory, failing.embedOwned(&texts, .{ .outside_execution = true }));
            try std.testing.expectEqual(@as(usize, 1), committed_oom.serial_items);
            const oom_deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
            while (!std.meta.eql(before_oom, domain.controller.snapshot())) {
                _ = @import("../execution/resource_retirement.zig").poll();
                if (platform.time.monotonicNs() >= oom_deadline) return error.GemmaCudaFailedHostBankDidNotRetire;
                std.Thread.yield() catch {};
            }
            try std.testing.expect(!lane.owned and scheduler.head == null and !probe.violation.load(.acquire));
            // Same actual post-Source failure path for the newly admitted
            // false option. This is explicit control fault injection, not an
            // exhaustive GPU allocator failure claim.
            committed_oom = .{};
            failing.config.normalize = false;
            failing.config.embedding_gemma2_dimensions = 128;
            try std.testing.expectError(error.OutOfMemory, failing.embedOwned(&texts, .{ .outside_execution = true }));
            try std.testing.expectEqual(@as(usize, 1), committed_oom.serial_items);
            const false_oom_deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
            while (!std.meta.eql(before_oom, domain.controller.snapshot())) {
                _ = @import("../execution/resource_retirement.zig").poll();
                if (platform.time.monotonicNs() >= false_oom_deadline) return error.GemmaCudaUnnormalizedFailedBankDidNotRetire;
                std.Thread.yield() catch {};
            }
            try std.testing.expect(!lane.owned and scheduler.head == null and !probe.violation.load(.acquire));
            var bank_below = pipeline;
            bank_below.trace = null;
            bank_below.session.run_admission.?.limits.host_limit_bytes = (try session_mod.HostOutputProducer.requiredBytes(texts.len, 768)) - 1;
            try std.testing.expectError(error.ResourceLimitExceeded, bank_below.embedOwned(&texts, .{ .outside_execution = true }));
        }
        var denied = pipeline;
        denied.trace = null;
        denied.session.run_admission.?.limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 1, .combined_limit_bytes = 1 };
        try std.testing.expectError(error.ResourceLimitExceeded, denied.embedOwned(&texts, .{ .outside_execution = true }));
        probe.model_gate = null;
        Probe.close(session);
        live = false;
        const required = try std.math.add(usize, try session_mod.HostOutputProducer.requiredBytes(texts.len, 768), if (no_normalize_held != null) try session_mod.HostOutputProducer.requiredBytes(1, 128) else 0);
        const deadline = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
        while (!std.meta.eql(domain.controller.snapshot(), memory_admission.AdmissionAmounts{ .host_scratch_bytes = required })) {
            _ = @import("../execution/resource_retirement.zig").poll();
            if (platform.time.monotonicNs() >= deadline) return error.GemmaTextSourceDidNotRetire;
            std.Thread.yield() catch {};
        }
        for (raw[0], owned.values(0)) |x, y| try std.testing.expectApproxEqAbs(x, y, 1e-5);
        var sink = try @import("../execution/retirement_sink.zig").Ref.create();
        defer sink.release();
        var foreign = try sink.enter();
        defer foreign.finish();
        var epoch = try foreign.retainEpoch();
        defer epoch.release();
        const foreign_lane = try lane.acquire(control);
        var foreign_held = true;
        defer if (foreign_held) foreign_lane.release();
        try owned.deinitWithCleanup(.{ .boundary = &epoch });
        if (no_normalize_held) |*value| {
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&no_normalize_expected), std.mem.sliceAsBytes(value.values(0)));
            try value.deinitWithCleanup(.{ .boundary = &epoch });
            no_normalize_held = null;
        }
        _ = @import("../execution/resource_retirement.zig").poll();
        try std.testing.expectEqual(required, domain.controller.snapshot().host_scratch_bytes);
        foreign_lane.release();
        foreign_held = false;
        foreign.finish();
        _ = @import("../execution/resource_retirement.zig").poll();
        try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
        try std.testing.expect(!probe.violation.load(.acquire));
    }
}

const GemmaBucketRetryProbe10 = struct {
    calls: usize = 0,
    encodes: usize = 0,
    reserves: usize = 0,
    failure: ?anyerror = null,
    deny_first: bool = false,
    fn encode(raw: *anyopaque, a: std.mem.Allocator, text: []const u8, length: usize) !EncodeResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.encodes += 1;
        const ids = try a.alloc(i32, length);
        errdefer a.free(ids);
        const mask = try a.alloc(i32, length);
        @memset(ids, 0);
        @memset(mask, 0);
        ids[0] = @intCast(text.len);
        mask[0] = 1;
        return .{ .ids = ids, .attention_mask = mask, .allocator = a };
    }
    fn special(_: *anyopaque) tokenizer_mod.SpecialTokens {
        return .{};
    }
    fn input(_: *anyopaque) []const backends.TensorInfo {
        return &.{ .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } } };
    }
    fn output(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "sentence_embedding", .dtype = .f32, .shape = &.{ -1, 2 } }};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    fn run(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator) ![]Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1; // observable actual dispatch before named failure
        if (self.failure) |err| return err;
        const batch: usize = @intCast(inputs[0].shape[0]);
        const sequence: usize = @intCast(inputs[0].shape[1]);
        const data = try a.alloc(f32, batch * 2);
        defer a.free(data);
        for (0..batch) |index| {
            data[index * 2] = @floatFromInt(inputs[0].asInt64()[index * sequence]);
            data[index * 2 + 1] = 1;
        }
        const result = try a.alloc(Tensor, 1);
        errdefer a.free(result);
        result[0] = try Tensor.initFloat32(a, "sentence_embedding", &.{ @intCast(batch), 2 }, data);
        return result;
    }
    fn controlled(raw: *anyopaque, inputs: []const Tensor, callback_allocator: std.mem.Allocator, callback_control: InferenceExecutionControl) ![]Tensor {
        try callback_control.check();
        return run(raw, inputs, callback_allocator);
    }
    fn reserve(raw: *anyopaque, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.reserves += 1;
        if (self.deny_first and self.reserves == 1) return error.ResourceTemporarilyUnavailable;
        return self.reserves;
    }
    fn retain(_: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {}
    fn release(_: *anyopaque, _: usize) void {}
    const table: session_mod.Session.VTable = .{ .run = run, .runWithControl = controlled, .inputInfo = input, .outputInfo = output, .backend = backend, .close = close };
    fn pipeline(self: *@This()) EmbeddingPipeline {
        return .{ .allocator = std.testing.allocator, .session = .{ .ptr = self, .vtable = &table }, .tok = .{ .ptr = self, .vtable = &.{ .encode = undefined, .encodeInto = undefined, .encodeForModel = encode, .encodeGeneration = undefined, .decode = undefined, .specialTokens = special, .vocabSize = undefined, .deinit = undefined } }, .config = .{ .normalize = false, .resident_text_encoder = false, .max_length = 8, .embedding_gemma2_dimensions = 2 }, .execution_control = .{ .native_call_policy = .in_process } };
    }
};

test "Gemma shared buckets propagate post-dispatch resource names once without numerical replay" {
    for ([_]anyerror{ error.ResourceLimitExceeded, error.MemoryBudgetExceeded, error.ResourceTemporarilyUnavailable }) |failure| {
        var probe: GemmaBucketRetryProbe10 = .{ .failure = failure };
        var pipeline = probe.pipeline();
        try std.testing.expectError(failure, pipeline.embedGemma2Texts(&.{ "a", "bb" }));
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectEqual(@as(usize, 2), probe.encodes);
    }
}

test "Gemma shared buckets bisect only actual cold admission denial without tokenization replay" {
    var probe: GemmaBucketRetryProbe10 = .{ .deny_first = true };
    var controller: memory_admission.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = GemmaBucketRetryProbe10.reserve, .retain = GemmaBucketRetryProbe10.retain, .release = GemmaBucketRetryProbe10.release });
    defer controller.configureResourceBudget(null) catch @panic("Gemma retry live quota");
    var pipeline = probe.pipeline();
    pipeline.session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const vectors = try pipeline.embedGemma2Texts(&.{ "a", "bb" });
    defer freeEmbeddingSlices(std.testing.allocator, vectors);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expectEqual(@as(usize, 2), probe.encodes);
    try std.testing.expectEqualSlices(f32, &.{ 1, 1 }, vectors[0]);
    try std.testing.expectEqualSlices(f32, &.{ 2, 1 }, vectors[1]);
}

test "Gemma direct preparation lane-only and injected-lock paths retain actual Model then lane custody" {
    const Probe = struct {
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.UnexpectedNumericalDispatch;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        const table: session_mod.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    var marker: u8 = 0;
    var gate: std.atomic.Mutex = .unlocked;
    var native_mutex: std.atomic.Mutex = .unlocked;
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    var lane: @import("../execution_control.zig").ExecutionLane = .{};
    const session: session_mod.Session = .{ .ptr = &marker, .vtable = &Probe.table, .execution_gate = &gate, .cohort_scheduler = &scheduler };
    var pipeline: EmbeddingPipeline = .{ .allocator = std.testing.allocator, .session = session, .tok = undefined, .config = .{}, .execution_lane = &lane };
    // Exercise the same private bookkeeping with immutable classification
    // supplied explicitly; separate actual-Model policy test attests selection.
    try pipeline.lockExecutionWithLanePolicy(true);
    try std.testing.expect(pipeline.direct_cohort_owned and lane.owned);
    try std.testing.expect(!gate.tryLock());
    pipeline.unlockExecution();
    try std.testing.expect(!lane.owned and !pipeline.execution_lane_held and !pipeline.direct_cohort_owned);
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
    pipeline.execution_lock = &native_mutex;
    try pipeline.lockExecutionWithLanePolicy(true);
    try std.testing.expect(!pipeline.direct_cohort_owned and lane.owned);
    try std.testing.expect(!native_mutex.tryLock());
    pipeline.unlockExecution();
    try std.testing.expect(!lane.owned and !pipeline.execution_lane_held);
    try std.testing.expect(native_mutex.tryLock());
    native_mutex.unlock();
    pipeline.execution_lock = null;
    pipeline.session.execution_gate = null;
    pipeline.session.cohort_scheduler = null;
    try pipeline.lockExecutionWithLanePolicy(true);
    try std.testing.expect(lane.owned and !pipeline.direct_cohort_owned);
    pipeline.unlockExecution();
    try std.testing.expect(!lane.owned and !pipeline.execution_lane_held);
    var controller: @import("../runtime/tier/memory.zig").AdmissionController = .{};
    defer controller.deinit();
    pipeline.session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    try std.testing.expectError(error.MissingExecutionCohortScheduler, pipeline.lockExecutionWithLanePolicy(true));
    try std.testing.expect(!lane.owned and !pipeline.direct_cohort_owned and pipeline.execution_lane_from_lock == null);
}

test "dense F32 public Main remains bitwise available under reference quantized flag with actual admitted roots" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!platform.env.getenvBool("TERMITE_METAL_REFERENCE_RUNTIME_QUANT_LINEAR")) return error.SkipZigTest;
    const q8 = platform.env.getenv("TERMITE_METAL_Q8_RUNTIME_STAGING_MAX_MB") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, q8, "0")) return error.SkipZigTest;
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    if (fixture.hidden != 128) return error.SkipZigTest;
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    const texts = [_][]const u8{
        "hello world alpha beta again tiny query document one two three token15 token16",
        "token16 token15 three two one document query tiny again beta alpha world hello",
    };
    // Independent original unadmitted Main control, SAME external flag/policy.
    // Its provider never warms the admitted model's slots or Root census.
    var raw_model = try session_factory.createMetalSession(Q.a, fixture.path);
    var raw_live = true;
    defer if (raw_live) Q.close(raw_model);
    try Q.publishMetalProvider(raw_model);
    try Q.publishEmbeddingContract(&raw_model, &manifest);
    var original = EmbeddingPipeline.init(Q.a, raw_model, hf.tokenizer(), Q.config(true, .last));
    original.execution_control = Q.control;
    const expected = try original.embed(&texts);
    defer freeEmbeddingSlices(Q.a, expected);
    try std.testing.expectEqual(@as(usize, 32), original.last_input_tokens);
    Q.close(raw_model);
    raw_live = false;

    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var probe: Q.Probe = .{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = Q.Probe.retainContext, .release_context = Q.Probe.releaseContext, .try_reserve = Q.Probe.reserve, .retain = Q.Probe.retain, .release = Q.Probe.release });
    var configured = true;
    defer if (configured) domain.controller.configureResourceBudget(null) catch @panic("reference-neutral Main live quota");
    var model = try session_factory.createMetalSession(Q.a, fixture.path);
    var model_live = true;
    defer if (model_live) {
        probe.model_gate = null;
        probe.provider_gate = null;
        Q.close(model);
    };
    try Q.publishMetalProvider(model);
    try Q.publishEmbeddingContract(&model, &manifest);
    try model.installOwnedExecutionGate(Q.a);
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    model.cohort_scheduler = &scheduler;
    model.model_lifetime.?.admission_domain = domain.pin();
    probe.model_gate = model.execution_gate;
    const cold = (try model.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    if (cold != .metal) return error.ExpectedActualMetalProvider;
    probe.provider_gate = &cold.metal.exclusion.mutex;
    const MiB = 1024 * 1024;
    model.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 2048 * MiB, .combined_limit_bytes = 2560 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    var pipeline = EmbeddingPipeline.init(Q.a, model, hf.tokenizer(), Q.config(true, .last));
    pipeline.execution_lock = model.execution_gate;
    pipeline.execution_control = Q.control;
    try std.testing.expectEqual(@as(usize, 0), try ExistingMetalHostAudit134.roots(model));
    for (0..2) |_| {
        // FIRST actual inference is public Main; no tokenized warmup/manual
        // phase/forced kernel or altered numerical threshold is introduced.
        var actual = try pipeline.embedOwned(&texts, .{ .outside_execution = true });
        defer actual.deinitOutsideExecution();
        try std.testing.expectEqual(@as(usize, 32), pipeline.last_input_tokens);
        try std.testing.expectEqual(expected.len, actual.len());
        for (expected, 0..) |row, index| {
            try std.testing.expectEqualSlices(f32, row, actual.values(index));
            try Q.compare(row, actual.values(index), fixture.hidden);
        }
        try std.testing.expect(Q.graphExecutions(&pipeline) > 0);
        try std.testing.expect((try ExistingMetalHostAudit134.roots(model)) > 0);
        try std.testing.expect(!probe.violation and scheduler.head == null and scheduler.tail == null);
    }
    probe.model_gate = null;
    probe.provider_gate = null;
    Q.close(model);
    model_live = false;
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!std.meta.eql(domain.controller.snapshot(), memory_admission.AdmissionAmounts{})) {
        _ = @import("../execution/resource_retirement.zig").poll();
        if (platform.time.monotonicNs() >= deadline) return error.ReferenceNeutralMainDidNotRetire;
        std.Thread.yield() catch {};
    }
    for (probe.tokens) |amount| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amount);
    try std.testing.expect(!probe.violation and probe.calls > 0);
    try domain.controller.configureResourceBudget(null);
    configured = false;
    try std.testing.expectEqual(@as(usize, 1), probe.references);
}

const ReferenceMainAudit100 = struct {
    probe: DenseQwen68Qa.Probe = .{},
    /// A real reference matrix is concurrently materialized with the output.
    /// This discriminates the mixed CPU/backend numerical run declaration
    /// from the small host-only preprocess and final-response declarations.
    reference_matrix_bytes: usize,
    host_reserve_cap: ?usize = null,
    reference_request_host_bytes: usize = 0,
    denied_host_bytes: usize = 0,
    cancel_reference: bool = false,
    cancelled: bool = false,
    denied_calls: usize = 0,
    cancelled_calls: usize = 0,

    fn referenceRequest(self: *const @This(), amount: memory_admission.AdmissionAmounts) bool {
        return amount.backend_scratch_bytes != 0 and amount.host_scratch_bytes >= self.reference_matrix_bytes;
    }
    fn retainContext(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return DenseQwen68Qa.Probe.retainContext(&self.probe);
    }
    fn releaseContext(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        DenseQwen68Qa.Probe.releaseContext(&self.probe);
    }
    fn reserve(raw: *anyopaque, amount: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.referenceRequest(amount)) {
            self.reference_request_host_bytes = @max(self.reference_request_host_bytes, amount.host_scratch_bytes);
            if (self.host_reserve_cap) |cap| if (amount.host_scratch_bytes > cap) {
                self.probe.observe();
                self.denied_calls += 1;
                self.denied_host_bytes = amount.host_scratch_bytes;
                return error.ResourceLimitExceeded;
            };
        }
        const token = try DenseQwen68Qa.Probe.reserve(&self.probe, amount);
        if (self.cancel_reference and self.referenceRequest(amount)) {
            self.cancelled = true;
            self.cancelled_calls += 1;
        }
        return token;
    }
    fn retain(raw: *anyopaque, token: usize, amount: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try DenseQwen68Qa.Probe.retain(&self.probe, token, amount);
    }
    fn release(raw: *anyopaque, token: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        DenseQwen68Qa.Probe.release(&self.probe, token);
    }
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw orelse return error.MissingReferenceAudit));
        if (self.cancelled) return error.Cancelled;
    }
    fn encoders(session: session_mod.Session) !u64 {
        const target = (try session.coldComputeTarget(DenseQwen68Qa.control)) orelse return error.ExpectedActualMetalProvider;
        if (target != .metal) return error.ExpectedActualMetalProvider;
        var ticket = try target.metal.exclusion.prepare();
        defer ticket.release() catch unreachable;
        try ticket.begin();
        defer ticket.end() catch unreachable;
        return @import("../backends/metal_runtime.zig").runtimeMemorySnapshot(@ptrCast(@alignCast(target.metal.runtime))).compute_encoder_count;
    }
    fn expectNewTokensRetired(self: *const @This(), start: usize) !void {
        for (self.probe.tokens[start..self.probe.next]) |amount| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amount);
    }
};

test "reference quantized public Main retains original arithmetic with admitted CPU SDK lifetimes and foreign B" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!platform.env.getenvBool("TERMITE_METAL_REFERENCE_RUNTIME_QUANT_LINEAR")) return error.SkipZigTest;
    if (platform.env.getenv("TERMITE_METAL_Q8_RUNTIME_STAGING_MAX_MB")) |policy| {
        if (!std.mem.eql(u8, policy, "32")) return error.SkipZigTest;
    }
    const Q = DenseQwen68Qa;
    const fixture = try Q.fixture();
    if (fixture.hidden != 128) return error.SkipZigTest;
    var manifest = try @import("../models/manifest.zig").loadFromDir(Q.a, fixture.path);
    defer manifest.deinit();
    const hf = try Q.tokenizer(fixture.path);
    defer hf.deinitSelf();
    const texts = [_][]const u8{
        "hello world alpha beta again tiny query document one two three token15 token16",
        "token16 token15 three two one document query tiny again beta alpha world hello",
    };
    // The control is the original unadmitted public Main, under the SAME
    // external reference flag and quantization policy, on an independent model.
    var raw_model = try session_factory.createMetalSession(Q.a, fixture.path);
    var raw_live = true;
    defer if (raw_live) Q.close(raw_model);
    try Q.publishMetalProvider(raw_model);
    try Q.publishEmbeddingContract(&raw_model, &manifest);
    var original = EmbeddingPipeline.init(Q.a, raw_model, hf.tokenizer(), Q.config(true, .last));
    original.execution_control = Q.control;
    const expected = try original.embed(&texts);
    defer freeEmbeddingSlices(Q.a, expected);
    try std.testing.expectEqual(@as(usize, 32), original.last_input_tokens);
    const expected_single = try original.embed(texts[0..1]);
    defer freeEmbeddingSlices(Q.a, expected_single);
    const cfg = session_factory.getGptConfig(raw_model) orelse return error.ExpectedActualQwenConfiguration;
    const intermediate_size = cfg.intermediate_size;
    Q.close(raw_model);
    raw_live = false;

    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var audit: ReferenceMainAudit100 = .{ .reference_matrix_bytes = try std.math.mul(usize, try std.math.mul(usize, fixture.hidden, intermediate_size), @sizeOf(f32)) };
    try domain.controller.configureResourceBudget(.{ .context = &audit, .retain_context = ReferenceMainAudit100.retainContext, .release_context = ReferenceMainAudit100.releaseContext, .try_reserve = ReferenceMainAudit100.reserve, .retain = ReferenceMainAudit100.retain, .release = ReferenceMainAudit100.release });
    var configured = true;
    defer if (configured) domain.controller.configureResourceBudget(null) catch @panic("reference Main live quota");
    var model = try session_factory.createMetalSession(Q.a, fixture.path);
    var model_live = true;
    defer if (model_live) {
        audit.probe.model_gate = null;
        audit.probe.provider_gate = null;
        Q.close(model);
    };
    try Q.publishMetalProvider(model);
    try Q.publishEmbeddingContract(&model, &manifest);
    try model.installOwnedExecutionGate(Q.a);
    var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
    model.cohort_scheduler = &scheduler;
    model.model_lifetime.?.admission_domain = domain.pin();
    audit.probe.model_gate = model.execution_gate;
    const target = (try model.coldComputeTarget(Q.control)) orelse return error.ExpectedActualMetalProvider;
    if (target != .metal) return error.ExpectedActualMetalProvider;
    audit.probe.provider_gate = &target.metal.exclusion.mutex;
    const MiB = 1024 * 1024;
    model.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 512 * MiB, .backend_limit_bytes = 2048 * MiB, .combined_limit_bytes = 2560 * MiB }, .static_workspace_bytes = 0, .check_live_memory = false };
    var pipeline = EmbeddingPipeline.init(Q.a, model, hf.tokenizer(), Q.config(true, .last));
    pipeline.execution_lock = model.execution_gate;
    pipeline.execution_control = Q.control;
    try std.testing.expectEqual(@as(usize, 0), try ExistingMetalHostAudit134.roots(model));
    for (0..2) |_| {
        // First admitted inference is the real public producer. There is no
        // tokenized warmup, manual Phase, retagging, or alternate math route.
        var actual = try pipeline.embedOwned(&texts, .{ .outside_execution = true });
        defer actual.deinitOutsideExecution();
        for (expected, 0..) |row, index| {
            try std.testing.expectEqualSlices(f32, row, actual.values(index));
            try Q.compare(row, actual.values(index), fixture.hidden);
        }
        try std.testing.expectEqual(@as(usize, 32), pipeline.last_input_tokens);
        try std.testing.expect(Q.graphExecutions(&pipeline) > 0);
        try std.testing.expect((try ExistingMetalHostAudit134.roots(model)) > 0);
        try std.testing.expect(!audit.probe.violation and scheduler.head == null and scheduler.tail == null);
    }
    // Select a single-row public call, so genuine admission denial cannot
    // be hidden by the existing multi-row bisection policy. Calibration uses
    // the actual cold supplier request, not a guessed workspace allowance.
    var exact_host_request: usize = 0;
    for (0..3) |iteration| {
        audit.reference_request_host_bytes = 0;
        var calibrated = try pipeline.embedOwned(texts[0..1], .{ .outside_execution = true });
        defer calibrated.deinitOutsideExecution();
        try std.testing.expectEqualSlices(f32, expected_single[0], calibrated.values(0));
        try std.testing.expect(audit.reference_request_host_bytes >= audit.reference_matrix_bytes);
        if (iteration == 2) try std.testing.expectEqual(exact_host_request, audit.reference_request_host_bytes);
        exact_host_request = audit.reference_request_host_bytes;
    }
    {
        audit.host_reserve_cap = exact_host_request;
        defer audit.host_reserve_cap = null;
        var exact = try pipeline.embedOwned(texts[0..1], .{ .outside_execution = true });
        defer exact.deinitOutsideExecution();
        try std.testing.expectEqualSlices(f32, expected_single[0], exact.values(0));
    }
    const before_denied = try ReferenceMainAudit100.encoders(model);
    const denied_start = audit.probe.next;
    {
        audit.host_reserve_cap = exact_host_request - 1;
        defer audit.host_reserve_cap = null;
        try std.testing.expectError(error.ResourceLimitExceeded, pipeline.embedOwned(texts[0..1], .{ .outside_execution = true }));
    }
    try std.testing.expect(audit.denied_calls > 0);
    try std.testing.expectEqual(exact_host_request, audit.denied_host_bytes);
    try std.testing.expectEqual(before_denied, try ReferenceMainAudit100.encoders(model));
    try audit.expectNewTokensRetired(denied_start);
    const cancelled_start = audit.probe.next;
    var cancelled = pipeline;
    cancelled.execution_control = .{ .native_call_policy = .in_process, .ptr = &audit, .check_fn = ReferenceMainAudit100.check };
    {
        audit.cancel_reference = true;
        defer audit.cancel_reference = false;
        try std.testing.expectError(error.Cancelled, cancelled.embedOwned(texts[0..1], .{ .outside_execution = true }));
    }
    audit.cancelled = false;
    try std.testing.expect(audit.cancelled_calls > 0);
    try std.testing.expectEqual(before_denied, try ReferenceMainAudit100.encoders(model));
    try audit.expectNewTokensRetired(cancelled_start);
    var held_output = try pipeline.embedOwned(&texts, .{ .outside_execution = true });
    defer held_output.deinitOutsideExecution();
    for (expected, 0..) |row, index| try std.testing.expectEqualSlices(f32, row, held_output.values(index));
    // Numerical/model roots die before the output moves to an unrelated B.
    audit.probe.model_gate = null;
    audit.probe.provider_gate = null;
    Q.close(model);
    model_live = false;
    const host_bytes = try session_mod.HostOutputProducer.requiredBytes(texts.len, fixture.hidden);
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!std.meta.eql(domain.controller.snapshot(), memory_admission.AdmissionAmounts{ .host_scratch_bytes = host_bytes })) {
        _ = @import("../execution/resource_retirement.zig").poll();
        if (platform.time.monotonicNs() >= deadline) return error.ReferenceMainSourceDidNotRetire;
        std.Thread.yield() catch {};
    }
    for (expected, 0..) |row, index| try std.testing.expectEqualSlices(f32, row, held_output.values(index));
    const Sink = @import("../execution/retirement_sink.zig");
    var sink = try Sink.Ref.create();
    defer sink.release();
    var foreign = try sink.enter();
    defer foreign.finish();
    var epoch = try foreign.retainEpoch();
    defer epoch.release();
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    var gate_held = true;
    audit.probe.foreign_gate = &gate;
    defer {
        audit.probe.foreign_gate = null;
        if (gate_held) gate.unlock();
    }
    const releases = audit.probe.releases;
    try held_output.deinitWithCleanup(.{ .boundary = &epoch });
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqual(releases, audit.probe.releases);
    try std.testing.expectEqual(host_bytes, domain.controller.snapshot().host_scratch_bytes);
    gate.unlock();
    gate_held = false;
    audit.probe.foreign_gate = null;
    foreign.finish();
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
    for (audit.probe.tokens) |amount| try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, amount);
    try std.testing.expect(!audit.probe.violation and scheduler.head == null and scheduler.tail == null);
    try domain.controller.configureResourceBudget(null);
    configured = false;
    try std.testing.expectEqual(@as(usize, 1), audit.probe.references);
}

test "read pool microbatch CUDA admission marks only pre-dispatch cohort refusals" {
    const memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.TestUnexpectedForward;
        }
        fn inputs(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            };
        }
        fn outputs(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{.{ .name = "sentence_embedding", .dtype = .f32, .shape = &.{ -1, 768 } }};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .cuda;
        }
        fn capabilities(_: *anyopaque) session_mod.ExecutionCapabilities {
            return .{ .text_batch_rows = 4, .text_batch_tokens = 8192, .text_batch_padded_tokens = 8192 };
        }
        fn close(_: *anyopaque) void {}
        fn admit(pipeline: *EmbeddingPipeline, batch: usize) !void {
            // Exercise the CUDA branch directly. The generic native branch
            // references the full architecture dispatch table and is unrelated
            // to this pre-dispatch refusal contract.
            var permit = pipeline.session.admit(.{ .batch = batch, .sequence = 2, .input_bytes = batch * 48 }) catch |err|
                return pipeline.textAdmissionError(err, batch);
            defer permit.deinit();
        }
    };
    var state: u8 = 0;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var pipeline = EmbeddingPipeline{
        .allocator = std.testing.allocator,
        .session = .{
            .ptr = &state,
            .vtable = &.{ .run = Probe.run, .inputInfo = Probe.inputs, .outputInfo = Probe.outputs, .backend = Probe.backend, .close = Probe.close, .executionCapabilities = Probe.capabilities },
            .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .scratch_limit_bytes = 1 }, .static_workspace_bytes = 0, .check_live_memory = false },
        },
        .tok = undefined, // Admission-only test; tokenization and forward are unreachable.
        .config = .{ .resident_embedding_gemma2 = true },
    };
    try std.testing.expectError(error.EmbeddingBatchAdmissionDenied, Probe.admit(&pipeline, 2));
    try std.testing.expectError(error.ResourceLimitExceeded, Probe.admit(&pipeline, 1));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "Native Gemma fresh public Session and normalize false Source prepare caches outside real Model and lane" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    if (@import("../backends/native.zig").useBlas()) return error.SkipZigTest;
    const model_path = platform.env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const tokenizer_path = try std.fs.path.join(a, &.{ model_path, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.deinitSelf();
    const Lane = @import("../execution_control.zig").ExecutionLane;
    const Probe = struct {
        model: ?*std.atomic.Mutex = null,
        lane: *Lane,
        violation: bool = false,
        calls: usize = 0,
        primary_expected: ?memory_admission.AdmissionAmounts = null,
        primary_calls: usize = 0,
        primary_cookie: ?usize = null,
        primary_releases: usize = 0,
        arms: usize = 0,
        disarms: usize = 0,
        fail_arm: bool = false,
        cancel_preflight: bool = false,
        cancel_after_reserve: bool = false,
        cancel_at_calls: usize = 0,
        cancel_source: ?*session_mod.PreparedSource = null,
        cancellations: usize = 0,
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) anyerror!u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.arms += 1;
            if (self.fail_arm) return error.TestGuardArmFailed;
            return @intCast(self.arms);
        }
        fn disarm(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.disarms += 1;
        }
        fn guarded(self: *@This()) InferenceExecutionControl {
            return .{ .ptr = self, .check_fn = @This().control, .hard_cancellation = .{ .ptr = self, .arm_fn = @This().arm, .disarm_fn = @This().disarm } };
        }
        fn control(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancel_preflight or (self.cancel_after_reserve and self.calls >= self.cancel_at_calls)) return error.Cancelled;
            if (self.cancel_source) |source| if (source.native_weight_paused) {
                self.cancellations += 1;
                return error.Cancelled;
            };
        }
        fn check(self: *@This()) void {
            if (self.model) |gate| {
                if (gate.tryLock()) gate.unlock() else self.violation = true;
            }
            if (self.lane.mutex.tryLock()) {
                if (self.lane.owned) self.violation = true;
                self.lane.mutex.unlock();
            } else self.violation = true;
        }
        fn reserve(raw: *anyopaque, amounts: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            self.calls += 1;
            if (self.primary_expected) |expected| if (std.meta.eql(expected, amounts)) {
                self.primary_calls += 1;
                self.primary_cookie = self.calls;
            };
            return self.calls;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory_admission.AdmissionAmounts) memory_admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, cookie: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            if (self.primary_cookie == cookie) self.primary_releases += 1;
        }
        fn close(session: session_mod.Session) void {
            var sink = @import("../execution/retirement_sink.zig").Ref.create() catch @panic("Native Source close sink");
            defer sink.release();
            var guard = sink.enter() catch @panic("Native Source close guard");
            defer guard.finish();
            var epoch = guard.retainEpoch() catch @panic("Native Source close epoch");
            defer epoch.release();
            session.closeWithBoundary(&epoch);
        }
    };
    const control: InferenceExecutionControl = .{ .native_call_policy = .in_process };
    const texts = [_][]const u8{"A red bus on a quiet street."};
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 2, 100 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", &.{ 1, 2 }, &.{ 1, 1 });
    defer mask.deinit();
    const inputs = [_]Tensor{ ids, mask };
    // Each entry has its OWN new managed provider and independent raw model.
    // No staged/owned call primes the cache before any entry being witnessed.
    for (0..9) |entry| {
        const raw = try session_factory.createNativeSession(a, model_path);
        defer raw.close();
        session_factory.attachIo(raw, std.testing.io);
        var lane: Lane = .{};
        var probe: Probe = .{ .lane = &lane };
        const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
        defer domain.release();
        try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        defer domain.controller.configureResourceBudget(null) catch @panic("Native Source live budget");
        var session = try session_factory.createNativeSession(a, model_path);
        var live = true;
        defer if (live) {
            probe.model = null;
            Probe.close(session);
        };
        session_factory.attachIo(session, std.testing.io);
        try session.installOwnedExecutionGate(a);
        var scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{};
        session.cohort_scheduler = &scheduler;
        session.model_lifetime.?.admission_domain = domain.pin();
        const GiB: usize = 1024 * 1024 * 1024;
        session.run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 8 * GiB, .backend_limit_bytes = GiB }, .static_workspace_bytes = 0, .check_live_memory = false };
        try @import("../execution/architecture_session.zig").configureSharedCacheAdmissionForSession(session, a, &domain.controller, .cpu, session.run_admission.?.limits, .{});
        probe.model = session.execution_gate;
        if (entry == 8) {
            // This actual public call is FIRST on this managed provider.
            // Cache metadata births are independent host_weight acquisitions;
            // identify primary by its exact copied six-class declaration.
            const old_request = try session_mod.RunRequest.fromTensors(&inputs);
            const old_floor = try session.run_admission.?.estimateRequest(old_request, session.outputInfo());
            var resolved = try session.planRun(&inputs, null);
            resolved.pre_admitted_host_bytes = old_request.pre_admitted_host_bytes;
            const source_floor = try session.run_admission.?.estimateRequest(resolved, session.outputInfo());
            inline for (@typeInfo(memory_admission.AdmissionAmounts).@"struct".field_names) |field| try std.testing.expect(@field(source_floor, field) >= @field(old_floor, field));
            probe.primary_expected = source_floor;
            var expected = (try raw.runResident(&inputs, a)) orelse return error.ExpectedOriginalResidentOutput;
            defer expected.deinit();
            const expected_values = try expected.backend.toFloat32(expected.outputs[0], a);
            defer a.free(expected_values);
            var actual = (try session.runResident(&inputs, a)) orelse return error.ExpectedPublicResidentOutput;
            var actual_live = true;
            defer if (actual_live) actual.deinit();
            try std.testing.expectEqual(@as(usize, 1), probe.primary_calls);
            try std.testing.expect(actual.resource_lease != null);
            try std.testing.expectEqualDeep(source_floor, actual.resource_lease.?.amounts);
            try std.testing.expectEqual(probe.primary_cookie, actual.resource_lease.?.external_lease);
            try std.testing.expectEqual(@as(usize, 0), probe.primary_releases);
            try std.testing.expect(actual.backend.call_context == null and actual.backend.embedding_stage == null and actual.backend.native_weight_cache == null and actual.backend.execution_control == null);
            probe.model = null;
            Probe.close(session);
            live = false;
            try std.testing.expectEqual(@as(usize, 0), probe.primary_releases);
            const values = try actual.backend.toFloat32(actual.outputs[0], a);
            defer a.free(values);
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected_values), std.mem.sliceAsBytes(values));
            actual.deinit();
            actual_live = false;
            _ = @import("../execution/resource_retirement.zig").poll();
            try std.testing.expectEqual(@as(usize, 1), probe.primary_releases);
            try std.testing.expectEqualDeep(memory_admission.AdmissionAmounts{}, domain.controller.snapshot());
        } else if (entry == 1) {
            var raw_pipeline = EmbeddingPipeline.init(a, raw, tokenizer.tokenizer(), .{ .normalize = false, .resident_text_encoder = true, .max_length = 128, .embedding_gemma2_dimensions = 768 });
            raw_pipeline.execution_control = control;
            const expected = try raw_pipeline.embed(&texts);
            defer freeEmbeddingSlices(a, expected);
            var pipeline = EmbeddingPipeline.init(a, session, tokenizer.tokenizer(), raw_pipeline.config);
            pipeline.execution_control = control;
            pipeline.execution_lock = session.execution_gate;
            pipeline.execution_lane = &lane;
            const actual = try pipeline.embed(&texts); // FIRST actual managed inference
            defer freeEmbeddingSlices(a, actual);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |x, y| try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(x), std.mem.sliceAsBytes(y));
        } else if (entry == 0 or entry == 3) {
            const expected = if (entry == 0) try raw.runWithControl(&inputs, a, control) else try raw.run(&inputs, a);
            defer {
                for (expected) |*value| value.deinit();
                a.free(expected);
            }
            const actual = if (entry == 0) try session.runWithControl(&inputs, a, probe.guarded()) else try session.run(&inputs, a); // FIRST, no Stage loan
            defer {
                for (actual) |*value| value.deinit();
                a.free(actual);
            }
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |x, y| {
                try std.testing.expectEqualSlices(i64, x.shape, y.shape);
                try std.testing.expectEqualSlices(u8, x.data, y.data);
            }
            if (entry == 0) {
                try std.testing.expectEqual(@as(usize, 1), probe.arms);
                try std.testing.expectEqual(@as(usize, 1), probe.disarms);
            }
        } else if (entry == 6 or entry == 7) {
            const before = probe.calls;
            if (entry == 6) {
                try std.testing.expectError(error.ProcessIsolationRequired, session.runWithControl(&inputs, a, .{}));
                try std.testing.expectEqual(before, probe.calls);
                probe.fail_arm = true;
                try std.testing.expectError(error.TestGuardArmFailed, session.runWithControl(&inputs, a, probe.guarded()));
                try std.testing.expectEqual(before, probe.calls);
                try std.testing.expectEqual(@as(usize, 1), probe.arms);
                try std.testing.expectEqual(@as(usize, 0), probe.disarms);
                probe.fail_arm = false;
                probe.cancel_preflight = true;
                try std.testing.expectError(error.Cancelled, session.runWithControl(&inputs, a, probe.guarded()));
                try std.testing.expectEqual(before, probe.calls);
                try std.testing.expectEqual(@as(usize, 1), probe.arms); // check before arm
            } else {
                probe.cancel_after_reserve = true;
                probe.cancel_at_calls = before + 1;
                const attempted = session.runWithControl(&inputs, a, probe.guarded());
                defer if (attempted) |values| {
                    for (values) |*value| value.deinit();
                    a.free(values);
                } else |_| {};
                try std.testing.expectError(error.Cancelled, attempted);
                try std.testing.expect(probe.calls > before);
                try std.testing.expectEqual(@as(usize, 1), probe.arms);
                try std.testing.expectEqual(@as(usize, 1), probe.disarms);
            }
        } else if (entry == 5) {
            var source: session_mod.PreparedSource = .{};
            defer source.deinitOutsideExecution();
            try source.initBeforeExecutionInto(session, session, session.execution_gate, .{ .host = &inputs }, try session.planRun(&inputs, null), null, .resident_transient, a, control);
            const data = @import("../execution/architecture_session.zig").nativePanelPreparationModelData(session) orelse return error.ExpectedNativePanelReceiver;
            const original_allocator = data.allocator;
            var failing = std.testing.FailingAllocator.init(original_allocator, .{ .fail_index = 0 });
            data.allocator = failing.allocator(); // actual cold cache staging only
            defer data.allocator = original_allocator;
            try source.bindExecutionLaneBeforeExecution(&lane, false);
            try source.enter();
            const attempted = source.runHost(&inputs);
            defer if (attempted) |values| {
                for (values) |*value| value.deinit();
                a.free(values);
            } else |_| {};
            try std.testing.expectError(error.OutOfMemory, attempted);
            try std.testing.expect(failing.has_induced_failure);
            try std.testing.expect(source.native_control == null);
            source.deinitOutsideExecution();
        } else if (entry == 4) {
            var source: session_mod.PreparedSource = .{};
            defer source.deinitOutsideExecution();
            const cancelling: InferenceExecutionControl = .{ .ptr = &probe, .check_fn = Probe.control, .native_call_policy = .in_process };
            try source.initBeforeExecutionInto(session, session, session.execution_gate, .{ .host = &inputs }, try session.planRun(&inputs, null), null, .resident_transient, a, cancelling);
            probe.cancel_source = &source;
            defer probe.cancel_source = null;
            try source.bindExecutionLaneBeforeExecution(&lane, false);
            try source.enter();
            const attempted = source.runHost(&inputs);
            defer if (attempted) |values| {
                for (values) |*value| value.deinit();
                a.free(values);
            } else |_| {};
            try std.testing.expectError(error.Cancelled, attempted);
            try std.testing.expectEqual(@as(usize, 1), probe.cancellations);
            try std.testing.expect(source.native_control == null);
            if (source.permit) |permit| try std.testing.expect(permit.native_weights == null);
            source.deinitOutsideExecution(); // restored Model/lane must now unwind
        } else {
            var expected = (try raw.runResident(&inputs, a)) orelse return error.ExpectedOriginalResidentOutput;
            defer expected.deinit();
            const expected_values = try expected.backend.toFloat32(expected.outputs[0], a);
            defer a.free(expected_values);
            var source: session_mod.PreparedSource = .{};
            defer source.deinitOutsideExecution();
            try source.initBeforeExecutionInto(session, session, session.execution_gate, .{ .host = &inputs }, try session.planRun(&inputs, null), null, .resident_consumer, a, null);
            try source.bindExecutionLaneBeforeExecution(&lane, false);
            try source.enter();
            var actual = (try source.runResident(.{ .host = &inputs })) orelse return error.ExpectedSourceResidentOutput;
            defer actual.deinit();
            try std.testing.expect(actual.backend.call_context == null and actual.backend.embedding_stage == null);
            try std.testing.expect(actual.backend.native_weight_cache == null);
            try std.testing.expect(try source.handoffResident(&actual));
            source.deinitOutsideExecution();
            probe.model = null;
            Probe.close(session);
            live = false; // actual outputs retain their original model/backend
            const actual_values = try actual.backend.toFloat32(actual.outputs[0], a);
            defer a.free(actual_values);
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected_values), std.mem.sliceAsBytes(actual_values));
        }
        try std.testing.expect((entry == 6 or probe.calls != 0) and !probe.violation);
        try std.testing.expect(!lane.owned and scheduler.head == null);
    }
}
